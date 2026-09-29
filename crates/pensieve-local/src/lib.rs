//! Local-engine library backing the **`pensieve`** CLI's `mcp` / `serve` / `setup` /
//! `sync` commands — the single-binary context engine for local machines.
//!
//! Zero infra: an embedded **SQLite catalog** + a **local-filesystem object
//! store** + the in-process columnar engine. The `pensieve` CLI exposes:
//!
//!   - `pensieve mcp`   — serve the Model Context Protocol over **stdio** (what a
//!     coding agent spawns): durable graph-aware **memory** *and* live data/graph.
//!   - `pensieve serve` — serve the **same web interface** + HTTP API the hosted
//!     server runs (query/KQL/SQL, catalog, graph, ingest, MCP over HTTP) on a
//!     local port, zero-auth.
//!   - `pensieve setup <agent>` — wire a coding agent to `pensieve mcp` in one command.
//!   - `pensieve sync` — sync memory bidirectionally with a control plane.
//!
//! Data lives under `~/.pensieve` (override with `PENSIEVE_HOME` / `PENSIEVE_LOCAL_DB` /
//! `PENSIEVE_LOCAL_DATA`): `catalog.db` (metadata + memory graph) and `data/`
//! (columnar extents). For `mcp`, **stdout is the protocol channel** — logs go
//! to stderr.

#![forbid(unsafe_code)]

pub mod agent_sources;
pub mod brain_registry;
mod cc_pipeline;
mod cc_sync;
mod cc_writeback;
mod cli_config_heal;
mod cred_store;
mod datasource_catalog;
pub mod node;
pub mod server_service;
mod setup;
mod source_watchers;
pub mod sqlite_queue;
mod sync;
mod vault_sync;
mod watcher_settings;
pub mod watcher_status;
pub mod worker;

#[cfg(test)]
mod server_service_unit_tests;
#[cfg(test)]
mod worker_unit_tests;

#[cfg(test)]
mod cc_pipeline_unit_tests;

#[cfg(test)]
mod cc_sync_unit_tests;

#[cfg(test)]
mod vault_sync_unit_tests;

#[cfg(test)]
mod cc_writeback_unit_tests;

use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};

use anyhow::{Context, Result};
use pensieve_catalog_sqlite::SqliteCatalog;
use pensieve_core::catalog::Catalog;
use pensieve_core::credentials::CredentialStore;
use pensieve_core::crypto::Crypto;
use pensieve_core::segment_format::SegmentFormat;
use pensieve_datasources::admin::AdminState as DataSourceAdminState;
use pensieve_datasources::catalog_trait::DataSourceCatalog;
use pensieve_datasources::registry::DataSourceRegistry;
use pensieve_datasources::runner::{DataSourceTickDeps, GraphRegisterFn, RowSink};
use pensieve_datasources::scheduler::DataSourceScheduler;
use pensieve_datasources::secrets::EnvSecretStore;
use pensieve_format_tlm::TelemetryFormat;
use pensieve_ingest_core::events::IngestEvents;
use pensieve_ingest_core::{StagingBuffer, StagingConfig, WritePath};
use pensieve_ingest_otlp::self_export::{SelfTraceCtx, SelfTraceExporter};
use pensieve_ingest_rest::IngestState;
use pensieve_mcp::{serve_stdio, McpState, ServerInfo, ToolDispatch};
use pensieve_server::agent::local::{
    FileEnabledSkillsStore, FileEnginePreferenceStore, NullCredentialStore,
};
use pensieve_server::agent::{
    AgentState, ConsumerPublisher, ConsumerSink, LocalConsumerPublisher, SharedToolCtx,
};
use pensieve_server::auth::{
    require_role_middleware, AuthBackend, AuthLayerState, EnvAuthBackend, Role, SessionAuthBackend,
};
use pensieve_server::catalog_handler::SchemaCache;
use pensieve_server::QueryState;
use pensieve_storage::{build_object_store, StorageConfig};
use tracing::{info, warn};

/// Set up the tracing subscriber for `pensieve serve` — includes the fmt layer
/// AND a `tracing_opentelemetry` layer that routes `pensieve_telemetry`-target
/// spans to the in-process self-trace exporter.
///
/// Returns the handle for wiring the exporter to storage once the catalog
/// is ready. Call this BEFORE `run_serve` and pass the returned handle to
/// `run_serve`.
///
/// If the global subscriber is already installed (e.g. in a test harness),
/// this is a no-op and returns an unwired handle.
pub fn setup_serve_tracing() -> Arc<OnceLock<SelfTraceCtx>> {
    let exporter = SelfTraceExporter::unwired();
    let handle = exporter.handle();
    use opentelemetry::trace::TracerProvider as _;
    let tp = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
        .build();
    let tracer = tp.tracer("pensieve-local");
    let otel_layer = tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_filter(
            tracing_subscriber::filter::Targets::new()
                .with_target("pensieve_telemetry", tracing::Level::INFO),
        );
    use tracing_subscriber::prelude::*;
    let _ = tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(std::io::stderr)
                .with_target(false)
                .with_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                        tracing_subscriber::EnvFilter::new("info,sqlx=warn,hyper=warn")
                    }),
                ),
        )
        .with(otel_layer)
        .try_init();
    handle
}

/// Resolved on-disk locations for the local engine.
struct Paths {
    catalog_db: String,
    data_root: String,
}

fn resolve_paths() -> Paths {
    let home = pensieve_core::config::home_or(".").display().to_string();
    let catalog_db =
        std::env::var("PENSIEVE_LOCAL_DB").unwrap_or_else(|_| format!("{home}/catalog.db"));
    let data_root = std::env::var("PENSIEVE_LOCAL_DATA").unwrap_or_else(|_| format!("{home}/data"));
    Paths {
        catalog_db,
        data_root,
    }
}

/// The shared local engine: embedded catalog + local-filesystem columnar store.
#[derive(Clone)]
struct Engine {
    /// Concrete handle — for sync watermarks (`sync_state`) beyond the trait.
    sqlite: Arc<SqliteCatalog>,
    catalog: Arc<dyn Catalog>,
    format: Arc<dyn SegmentFormat>,
}

async fn open_engine(paths: &Paths) -> Result<Engine> {
    if let Some(parent) = std::path::Path::new(&paths.catalog_db).parent() {
        std::fs::create_dir_all(parent).ok();
    }
    std::fs::create_dir_all(&paths.data_root)
        .with_context(|| format!("creating data root {}", paths.data_root))?;

    let sqlite = Arc::new(
        SqliteCatalog::connect(&paths.catalog_db)
            .await
            .map_err(|e| anyhow::anyhow!("opening catalog at {}: {e}", paths.catalog_db))?,
    );
    let catalog: Arc<dyn Catalog> = sqlite.clone();
    info!(catalog = %paths.catalog_db, "embedded catalog ready");

    let store = build_object_store(&StorageConfig::Local {
        root: paths.data_root.clone(),
    })
    .context("building local object store")?;
    // S2.1: per-extent format dispatch (TLM + Parquet readers). Local writes use
    // PENSIEVE_WRITE_FORMAT (default "tlm"); both formats stay readable so a local
    // store can mix them.
    let tlm_fmt: Arc<dyn SegmentFormat> =
        Arc::new(TelemetryFormat::new(store.clone(), "pensieve-local"));
    let parquet_fmt: Arc<dyn SegmentFormat> = Arc::new(
        pensieve_format_parquet::ParquetFormat::new(store, "pensieve-local"),
    );
    let format: Arc<dyn SegmentFormat> =
        if std::env::var("PENSIEVE_WRITE_FORMAT").as_deref() == Ok("parquet") {
            Arc::new(pensieve_core::segment_format::FormatRegistry::new(
                parquet_fmt,
                vec![tlm_fmt],
            ))
        } else {
            Arc::new(pensieve_core::segment_format::FormatRegistry::new(
                tlm_fmt,
                vec![parquet_fmt],
            ))
        };
    info!(data = %paths.data_root, "local object store ready (TLM + Parquet readers)");

    Ok(Engine {
        sqlite,
        catalog,
        format,
    })
}

/// Forwards consumer activity to a running `pensieve serve` over HTTP. Used by the
/// standalone `pensieve mcp` (stdio) process, which has no access to the serve's
/// in-process bus, so the agent driving it still shows in the live overlay.
/// Best-effort + fire-and-forget; with no serve up the POST just fails silently.
struct RemoteConsumerPublisher {
    endpoint: String,
    token: String,
    client: reqwest::Client,
}

impl ConsumerPublisher for RemoteConsumerPublisher {
    fn tenant(&self) -> pensieve_core::tenant::TenantId {
        pensieve_core::tenant::DEFAULT_TENANT
    }
    fn publish(&self, activity: pensieve_ingest_core::ConsumerActivity) {
        let url = format!("{}/v1/consumers/emit", self.endpoint.trim_end_matches('/'));
        let token = self.token.clone();
        let client = self.client.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let _ = client
                    .post(url)
                    .bearer_auth(token)
                    .json(&activity)
                    .timeout(std::time::Duration::from_secs(2))
                    .send()
                    .await;
            });
        }
    }
}

/// Build a forwarder to a running serve from `${PENSIEVE_HOME}/config.json`, if it
/// exists with an endpoint + token. `None` ⇒ `pensieve mcp` runs standalone (no
/// overlay forwarding), exactly as before.
fn remote_consumer_sink() -> Option<ConsumerSink> {
    let home = pensieve_core::config::home()?.display().to_string();
    let raw = std::fs::read_to_string(format!("{home}/config.json")).ok()?;
    let cfg: serde_json::Value = serde_json::from_str(&raw).ok()?;
    let endpoint = cfg.get("endpoint")?.as_str()?.trim().to_string();
    let token = cfg.get("token")?.as_str()?.trim().to_string();
    if endpoint.is_empty() || token.is_empty() {
        return None;
    }
    Some(std::sync::Arc::new(RemoteConsumerPublisher {
        endpoint,
        token,
        client: reqwest::Client::new(),
    }))
}

fn mcp_state(engine: &Engine, memory: Option<pensieve_memory::MemoryQueue>) -> McpState {
    // No Postgres pool in local mode — recall/save run over the engine.
    let shared = SharedToolCtx {
        realm_scope: Default::default(),
        // Forward to a running serve so stdio agents appear in the live overlay.
        consumer_sink: remote_consumer_sink(),
        federation: None,
        catalog: engine.catalog.clone(),
        format: engine.format.clone(),
        pool: None,
        memory,
        hitl: None,
        memory_settings_path: None,
    };
    McpState {
        dispatch: ToolDispatch::new(shared),
        builder: None,
        server_info: ServerInfo {
            name: "pensieve".into(),
            version: env!("CARGO_PKG_VERSION").into(),
        },
    }
}

/// A running async memory ingest queue: the submit/barrier handle, the worker
/// task, and the trigger that tells the worker to drain + stop.
struct LocalMemoryQueue {
    queue: pensieve_memory::MemoryQueue,
    worker: tokio::task::JoinHandle<()>,
    stop: tokio::sync::oneshot::Sender<()>,
}

impl LocalMemoryQueue {
    /// Flush queued memories and stop the worker. Called when the transport
    /// is done (stdin EOF / HTTP shutdown) so no queued memory is lost.
    async fn shutdown(self) {
        if !self.queue.drain(std::time::Duration::from_secs(15)).await {
            warn!("memory queue drain timed out; the worker's shutdown pass retries");
        }
        let _ = self.stop.send(());
        // The worker's shutdown arm drains anything still buffered.
        let _ = self.worker.await;
    }
}

/// Spawn the async memory ingest worker over the local engine. Returns `None`
/// (synchronous memory writes) when `PENSIEVE_MEMORY_ASYNC=0` or the embedding
/// backend cannot be built. Local default: in-memory queue tier — crash loss
/// window is bounded by the flush linger; durable opt-in via
/// `PENSIEVE_MEMORY_QUEUE_DURABLE=1` (persists pending saves to the catalog).
async fn spawn_local_memory_queue(engine: &Engine) -> Option<LocalMemoryQueue> {
    let disabled = std::env::var("PENSIEVE_MEMORY_ASYNC")
        .map(|v| v == "0" || v.eq_ignore_ascii_case("false"))
        .unwrap_or(false);
    if disabled {
        info!("PENSIEVE_MEMORY_ASYNC=0 — memory writes are synchronous");
        return None;
    }
    let embed = match pensieve_memory::shared_embedding().await {
        Ok(e) => e,
        Err(e) => {
            warn!(error = %e, "embedding backend unavailable; memory writes stay synchronous");
            return None;
        }
    };
    let cfg = pensieve_memory::MemoryIngestConfig::from_env(false);
    let (stop, stop_rx) = tokio::sync::oneshot::channel::<()>();
    // Local/embedded mode has no cluster node registration (no `nodes` row to
    // reuse) — a fresh id is fine here: the embedded SQLite catalog's
    // `background_tasks.claimed_by` carries no foreign key to `nodes`, unlike
    // the Postgres catalog used by the server binary.
    let (queue, worker) = pensieve_memory::spawn_memory_queue(
        engine.catalog.clone(),
        engine.format.clone(),
        embed,
        cfg,
        pensieve_core::types::NodeId::new(),
        async move {
            let _ = stop_rx.await;
        },
    );
    info!("async memory ingest queue started (batched embeds + group commits)");
    Some(LocalMemoryQueue {
        queue,
        worker,
        stop,
    })
}

/// `pensieve mcp` — serve the Model Context Protocol over stdio.
///
/// The caller (the `pensieve` binary) must route tracing to **stderr** — stdout is
/// the JSON-RPC protocol channel.
pub async fn run_mcp() -> Result<()> {
    pensieve_server::agent::identity::set_source("mcp-stdio");
    let engine = open_engine(&resolve_paths()).await?;
    // Opportunistic Claude Code file-memory sync: a session is starting, so
    // pick up any memory files that changed since the last one. Detached —
    // never delays the protocol handshake. Kill switch: PENSIEVE_CC_SYNC_ON_MCP=0.
    if env_flag("PENSIEVE_CC_SYNC_ON_MCP", true) && env_flag("PENSIEVE_CC_FILE_SYNC", true) {
        let eng = engine.clone();
        tokio::spawn(async move {
            if let Err(e) = run_cc_phase(&eng, None, &SyncOptions::default()).await {
                tracing::debug!("mcp-startup cc-sync: {e}");
            }
        });
    }
    let memq = spawn_local_memory_queue(&engine).await;
    let state = mcp_state(&engine, memq.as_ref().map(|m| m.queue.clone()));
    info!("serving MCP over stdio (memory + data + graph); stdin/stdout is the protocol channel");
    let served = serve_stdio(state).await;
    // stdin EOF (client disconnected): land queued memories before exiting —
    // this is what makes async saves safe in a short-lived stdio process.
    if let Some(memq) = memq {
        memq.shutdown().await;
    }
    served.context("stdio MCP loop")?;
    Ok(())
}

/// Assemble the complete local-mode axum router from already-opened engine
/// components.
///
/// Exposed for integration tests and for embedding the local server in other
/// contexts — the returned `Router` is the exact same application that
/// `run_serve` binds and serves. `run_serve` calls this function and then
/// performs the socket-binding, signal handling, and background-worker
/// lifecycle that are not part of router construction.
///
/// Also returns the `AgentState` so the caller can start optional background
/// workers (e.g. the `PENSIEVE_CC_WATCH` watcher) that hold a reference to it.
/// `watcher_status` is passed in by the caller (created before `SqliteDataSourceCatalog`
/// so it can be injected into the catalog for `list_watchers()`).
#[allow(clippy::too_many_arguments)]
pub fn build_local_app(
    catalog: Arc<dyn pensieve_core::catalog::Catalog>,
    format: Arc<dyn pensieve_core::segment_format::SegmentFormat>,
    backend: Arc<dyn AuthBackend>,
    memory: Option<pensieve_memory::MemoryQueue>,
    local_dreaming: Option<Arc<pensieve_server::agent::dreaming_local::LocalDreamingStore>>,
    mcp_url: Option<String>,
    // Optional credential store — if None, falls back to NullCredentialStore.
    cred_store: Option<Arc<dyn CredentialStore>>,
    // Optional data source admin state (catalog + registry). When provided the
    // /v1/data-sources routes are mounted behind Role::Write.
    ds_admin: Option<(Arc<dyn DataSourceCatalog>, Arc<DataSourceRegistry>)>,
    // In-process watcher status — created before SqliteDataSourceCatalog and
    // injected into it so list_watchers() returns cc-sync heartbeats.
    watcher_status: watcher_status::LocalWatcherStatus,
    // Located `git` binary — mounts the brain repos surface (`/v1/brain` +
    // `/git/<name>.git`). `None` ⇒ management API reports git_available:false
    // and repo-touching endpoints answer 503.
    brain_git: Option<Arc<pensieve_brain::gitbin::GitBin>>,
) -> (axum::Router, AgentState, pensieve_server::brain::BrainState) {
    let schema_cache = Arc::new(SchemaCache::from_env());
    let query_state = QueryState {
        federation: None,
        catalog: catalog.clone(),
        format: format.clone(),
        schema_cache: schema_cache.clone(),
        node_id: None,
        pg_pool: None, // local: no Postgres — pool-only surfaces degrade gracefully
        layout_cache: std::sync::Arc::new(pensieve_server::graph_layout_cache::LayoutCache::new()),
    };
    // Engine preference + enabled skills persist to JSON under ~/.pensieve so
    // Settings → Agent engine works locally and survives restarts. Engine
    // auth auto-detects env vars / ~/.claude/.credentials.json — the Postgres
    // credential store stays a control-plane feature.
    let pensieve_home = pensieve_core::config::home_or(".").display().to_string();
    let resolved_cred_store: Arc<dyn CredentialStore> = cred_store
        .clone()
        .unwrap_or_else(|| Arc::new(NullCredentialStore));
    // Live consumer-activity bus — fed by the memory tool paths, subscribed by
    // the /v1/consumers/live WebSocket that drives the graph explorer overlay.
    let consumer_events = pensieve_ingest_core::ConsumerEvents::new(256);
    let agent_state = AgentState {
        catalog: catalog.clone(),
        format: format.clone(),
        pool: None, // local: run/session history not persisted; memory runs over the engine
        engines: Arc::new(FileEnginePreferenceStore::new(std::format!(
            "{pensieve_home}/agent-engine.json"
        ))),
        credentials: resolved_cred_store,
        tenant: pensieve_core::tenant::DEFAULT_TENANT,
        skills: Arc::new(FileEnabledSkillsStore::new(std::format!(
            "{pensieve_home}/agent-skills.json"
        ))),
        // Loopback to this serve's own MCP endpoint so the ClaudeCli engine can
        // reach the local memory + data tools during dreaming/ask. `None` keeps
        // MCP wiring disabled (adk engines query the engine directly).
        mcp_url,
        memory: memory.clone(),
        // Degraded local-mode dreaming: inline execution + in-memory ring + SQLite.
        local_dreaming,
        // Local memory settings persist to a JSON file under ${PENSIEVE_HOME}.
        memory_settings_path: Some(std::path::PathBuf::from(std::format!(
            "{pensieve_home}/memory-settings.json"
        ))),
        consumer_events: Some(consumer_events.clone()),
    };
    let ingest_events = IngestEvents::new(256);
    // Group-commit by default. Without it every capture POST is its own
    // snapshot, and a laptop running coding-agent hooks grows tens of
    // thousands of catalog rows. `PENSIEVE_STAGING_DISABLED=1` keeps the
    // one-extent-per-request path for tests that depend on it.
    let write_path = if std::env::var("PENSIEVE_STAGING_DISABLED").ok().as_deref() == Some("1") {
        WritePath::new(catalog.clone(), format.clone()).with_events(ingest_events.clone())
    } else {
        let staging =
            StagingBuffer::new(catalog.clone(), format.clone(), StagingConfig::from_env());
        let timer = staging.clone();
        tokio::spawn(timer.run_timer(std::future::pending()));
        tracing::info!("local ingest staging: group-commit enabled");
        WritePath::with_staging(catalog.clone(), format.clone(), staging)
            .with_events(ingest_events.clone())
    };
    let ingest_state = IngestState {
        catalog: catalog.clone(),
        write_path,
    };
    let read_mw = || {
        axum::middleware::from_fn_with_state(
            AuthLayerState {
                backend: backend.clone(),
                required: Role::Read,
            },
            require_role_middleware,
        )
    };
    let write_mw = || {
        axum::middleware::from_fn_with_state(
            AuthLayerState {
                backend: backend.clone(),
                required: Role::Write,
            },
            require_role_middleware,
        )
    };

    let admin_mw = || {
        axum::middleware::from_fn_with_state(
            AuthLayerState {
                backend: backend.clone(),
                required: Role::Admin,
            },
            require_role_middleware,
        )
    };

    // The same web interface + full API the hosted server serves, over the
    // embedded catalog. Read surfaces (query/catalog/graph/agent/memory/MCP)
    // require Role::Read; ingest + dashboard/cleanup writes require
    // Role::Write; user admin requires Role::Admin; login/web/health are open.
    // Control-plane-only surfaces (data sources, credentials, OAuth, saved-view
    // writes) are NOT mounted — /v1/capabilities tells clients so, and the
    // SPA fallback 404s unknown /v1/* paths instead of serving HTML.
    // (agent_state is cloned: the PENSIEVE_CC_WATCH watcher in run_serve keeps one.)
    // Build a second QueryState for the live router sharing the same schema_cache.
    let query_state_for_live = QueryState {
        federation: None,
        catalog: catalog.clone(),
        format: format.clone(),
        schema_cache,
        node_id: None,
        pg_pool: None,
        layout_cache: std::sync::Arc::new(pensieve_server::graph_layout_cache::LayoutCache::new()),
    };
    // Build McpState from the same catalog + format the rest of the app uses.
    let mcp = McpState {
        dispatch: ToolDispatch::new(SharedToolCtx {
            realm_scope: Default::default(),
            consumer_sink: Some(std::sync::Arc::new(LocalConsumerPublisher {
                events: consumer_events.clone(),
                tenant: pensieve_core::tenant::DEFAULT_TENANT,
            })),
            federation: None,
            catalog: catalog.clone(),
            format: format.clone(),
            pool: None,
            memory: memory.clone(),
            hitl: None,
            memory_settings_path: agent_state.memory_settings_path.clone(),
        }),
        // Local mode has no per-request Principal — realm scoping is a
        // deployed-server concern.
        builder: None,
        server_info: ServerInfo {
            name: "pensieve".into(),
            version: env!("CARGO_PKG_VERSION").into(),
        },
    };
    let read_router = pensieve_server::router_with_agent(query_state, agent_state.clone())
        .merge(pensieve_mcp::router(mcp))
        .merge(pensieve_server::capabilities::router(
            pensieve_server::capabilities::Capabilities::LOCAL,
        ))
        // POST /v1/consumers/emit — separate pensieve mcp (stdio) processes forward
        // their consumer activity here so they show in the live overlay.
        .merge(
            pensieve_server::discover::consumers_live::consumers_emit_router(Some(
                consumer_events.clone(),
            )),
        )
        .layer(read_mw());
    let ingest_router = pensieve_ingest_rest::router(ingest_state).layer(write_mw());
    // Dashboards + table cleanup write over the Catalog trait — fully
    // supported by the embedded SQLite catalog (the web UI needs them).
    let local_write_router = pensieve_server::dashboards_write_router(catalog.clone())
        .merge(pensieve_server::cleanup_write_router(catalog.clone()))
        .merge(pensieve_server::compact_write_router(catalog.clone()))
        .layer(write_mw());
    // Keep the per-tenant quota cache (S2.6) fresh so the admission limiter and
    // the /v1/admin/tenant-quotas endpoint work in local mode too. No-op when the
    // tenant_quotas table is empty (the single-tenant default).
    let _quota_refresh_handle = pensieve_server::quota_cache::spawn_refresh(catalog.clone());
    let admin_users_router =
        pensieve_server::admin_handler::admin_users_router(catalog.clone()).layer(admin_mw());
    let session_router =
        pensieve_server::auth_handler::auth_session_router(catalog.clone()).layer(read_mw());

    // Live-tail WebSocket — mounted WITHOUT auth middleware; the session
    // authenticates via its first message (browsers can't send WS headers).
    // Live consumers WebSocket — same auth-by-first-message pattern as the
    // live-tail router. Backfills recent memory spans from the local `otel`
    // self-trace DB (matches the frontend's OPS_DB).
    let consumers_router = pensieve_server::discover::consumers_live::consumers_live_router(
        query_state_for_live.clone(),
        backend.clone(),
        Some(consumer_events),
        "otel".to_string(),
    );
    let live_router = pensieve_server::discover::live::explore_live_router(
        query_state_for_live,
        backend.clone(),
        Some(ingest_events),
    );
    // /v1/workers stub (empty registry) so the dreaming UI's NodesStrip renders
    // its empty state. Behind read auth like the rest of the API surface.
    let workers_router = pensieve_server::local_workers_router().layer(read_mw());
    // In-process watcher registry serving /v1/data-sources/watchers.
    // Omit this when the full datasource admin router is mounted — that router
    // already exposes /v1/data-sources/watchers (via SqliteDataSourceCatalog
    // which has watcher_status injected) and the two would conflict.
    let watchers_router = if ds_admin.is_none() {
        Some(watcher_status.router().layer(read_mw()))
    } else {
        None
    };
    // Settings route always mounted — lets the UI toggle cc-sync regardless of
    // whether the full ds_admin router is present.
    let watcher_settings_router = watcher_status.settings_router().layer(read_mw());

    // Credentials CRUD — only when a real credential store was supplied.
    let creds_router = cred_store.map(|store| {
        pensieve_server::credentials_handler::router(
            pensieve_server::credentials_handler::CredentialsState { store },
        )
        .layer(write_mw())
    });

    // Data source admin CRUD — only when a catalog + registry were supplied.
    let ds_router = ds_admin.map(|(ds_catalog, ds_registry)| {
        pensieve_server::datasource_admin_router(DataSourceAdminState {
            catalog: ds_catalog,
            registry: ds_registry,
        })
        .layer(write_mw())
    });

    // Brain repos: /v1/brain management (read-mounted; mutating handlers gate
    // Write/Admin in-handler) + /git/<name>.git smart HTTP with Basic auth.
    let brain_state = pensieve_server::brain::BrainState::new(
        Arc::new(brain_registry::LocalBrainRegistry::new(std::format!(
            "{pensieve_home}/brains.json"
        ))),
        brain_git,
        std::path::PathBuf::from(std::format!("{pensieve_home}/brain")),
        agent_state.clone(),
    );
    let brain_mgmt_router =
        pensieve_server::brain::routes::brain_router(brain_state.clone()).layer(read_mw());
    let brain_git_router = pensieve_server::brain::git_http::git_http_router(brain_state.clone())
        .layer(axum::middleware::from_fn_with_state(
            AuthLayerState {
                backend: backend.clone(),
                required: Role::Read,
            },
            pensieve_server::auth::require_git_auth_middleware,
        ));

    let mut app = read_router
        .merge(ingest_router)
        .merge(local_write_router)
        .merge(admin_users_router)
        .merge(session_router)
        .merge(workers_router)
        .merge(pensieve_server::auth_handler::auth_login_router(
            catalog.clone(),
        ))
        .merge(pensieve_server::health_router())
        .merge(live_router)
        .merge(consumers_router)
        .merge(pensieve_server::web_ui::router())
        .merge(watcher_settings_router)
        .merge(brain_mgmt_router)
        .merge(brain_git_router);
    if let Some(r) = watchers_router {
        app = app.merge(r);
    }
    if let Some(r) = creds_router {
        app = app.merge(r);
    }
    if let Some(r) = ds_router {
        app = app.merge(r);
    }
    let app = pensieve_server::with_permissive_cors(app);
    (app, agent_state, brain_state)
}

/// `pensieve serve` — serve the web UI + full HTTP API on `addr`, over the embedded
/// catalog (zero infra).
///
/// `self_trace_handle` is the value returned by `setup_serve_tracing()`. When
/// `Some`, the self-trace exporter is wired to the catalog write path so that
/// internal `pensieve_telemetry` spans land in the `otel_traces` table.
pub async fn run_serve(
    addr: SocketAddr,
    self_trace_handle: Option<Arc<OnceLock<SelfTraceCtx>>>,
) -> Result<()> {
    pensieve_server::agent::identity::set_source("local-serve");
    let engine = open_engine(&resolve_paths()).await?;

    // Wire self-trace exporter now that the catalog + write path are ready.
    // This makes internal pensieve_telemetry spans land in otel_traces immediately.
    // Pre-create the table so the Traces page never 404s on an empty database.
    if let Some(handle) = &self_trace_handle {
        let wp = WritePath::new(engine.catalog.clone(), engine.format.clone());
        let _ = handle.set(SelfTraceCtx {
            catalog: engine.catalog.clone(),
            write_path: wp,
            database: "otel".into(),
        });
    }
    pensieve_ingest_otlp::ensure_traces_table(&engine.catalog, "otel").await;

    let memq = spawn_local_memory_queue(&engine).await;
    let memory = memq.as_ref().map(|m| m.queue.clone());

    // The web UI requires a sign-in. Seed a local user (default `admin`/`admin`,
    // override with PENSIEVE_LOCAL_USER / PENSIEVE_LOCAL_PASSWORD) and authenticate via
    // session tokens stored in the embedded catalog — same machinery as the
    // server, over SQLite.
    let user = std::env::var("PENSIEVE_LOCAL_USER").unwrap_or_else(|_| "admin".into());
    let password = std::env::var("PENSIEVE_LOCAL_PASSWORD").unwrap_or_else(|_| "admin".into());
    if engine.catalog.count_users().await.unwrap_or(0) == 0 {
        let phc = pensieve_server::auth::passwords::hash_password(&password)
            .map_err(|e| anyhow::anyhow!("hashing local password: {e}"))?;
        engine
            .catalog
            .create_user(&user, &phc, "admin")
            .await
            .context("seeding local user")?;
        info!(username = %user, "seeded local web-UI user");
    }
    let backend: Arc<dyn AuthBackend> = Arc::new(SessionAuthBackend::new(
        engine.catalog.clone(),
        EnvAuthBackend::from_env(),
        true,
    ));

    // Self-heal the CLI connection: the CLI, MCP, and every coding-agent
    // capture hook read ~/.pensieve/config.json, and a stale token there (e.g. an
    // expired browser session token from `pensieve connect`) silently 401s them
    // all. Validate it against this serve's auth backend and mint a durable
    // replacement when needed. Best-effort — never blocks startup.
    if env_flag("PENSIEVE_LOCAL_HEAL_CONFIG", true) {
        let cfg_path = std::path::PathBuf::from(home_dir()).join(".pensieve/config.json");
        match cli_config_heal::heal_cli_config(&cfg_path, addr, &backend, &engine.catalog).await {
            Ok(cli_config_heal::HealOutcome::TokenMinted) => {
                info!(config = %cfg_path.display(), "minted durable CLI token (stored token was missing or stale)");
            }
            Ok(cli_config_heal::HealOutcome::EndpointRepaired) => {
                info!(config = %cfg_path.display(), "repaired CLI endpoint");
            }
            Ok(cli_config_heal::HealOutcome::ForeignEndpoint) => {
                info!(config = %cfg_path.display(), "CLI config points at another server — left untouched");
            }
            Ok(cli_config_heal::HealOutcome::TokenValid) => {}
            Err(e) => warn!(error = %e, "couldn't self-heal CLI config"),
        }
    }

    // Degraded local-mode dreaming state: in-memory ring hydrated from the
    // embedded SQLite catalog. Inline runs + the dreaming HTTP handlers read it.
    let local_dreaming = Some(
        pensieve_server::agent::dreaming_local::LocalDreamingStore::new(engine.catalog.clone())
            .await,
    );
    // Loopback URL for this serve's own MCP endpoint, so the ClaudeCli engine
    // can reach the local memory/data tools during a dreaming run. When bound
    // to all interfaces (0.0.0.0 / ::), dial 127.0.0.1 instead — the unspecified
    // address isn't connectable as a destination.
    let mcp_host = if addr.ip().is_unspecified() {
        format!("127.0.0.1:{}", addr.port())
    } else {
        addr.to_string()
    };
    let mcp_url = Some(format!("http://{mcp_host}/mcp/v1"));

    // Local serve always seeds a user, so the auth backend is "enabled" and the
    // MCP endpoint requires a bearer. The dreaming ClaudeCli path reads
    // PENSIEVE_INTERNAL_BEARER for that header. If a static admin token is
    // configured via PENSIEVE_AUTH_TOKENS and PENSIEVE_INTERNAL_BEARER isn't already
    // set, derive it so dreaming-over-ClaudeCli works out of the box.
    if std::env::var("PENSIEVE_INTERNAL_BEARER").is_err() {
        if let Some(tok) = first_admin_token() {
            // SAFETY: set before any worker thread reads it; single-threaded here.
            std::env::set_var("PENSIEVE_INTERNAL_BEARER", tok);
        }
    }

    // ── Credentials: AES-256-GCM key from env or auto-generated file ──────────
    let pensieve_home = pensieve_core::config::home_or(".").display().to_string();
    let secret_key_path = format!("{pensieve_home}/secret.key");
    let crypto = Arc::new(
        Crypto::from_env_or_file(&secret_key_path)
            .context("loading or generating local secret key")?,
    );
    let pool = engine.sqlite.pool().clone();
    let cred_store: Arc<dyn CredentialStore> =
        Arc::new(cred_store::SqliteCredentialStore::new(pool.clone(), crypto));

    // ── In-process watcher status (created before ds_catalog so it can be
    // injected for list_watchers()) ─────────────────────────────────────────
    let watcher_status = {
        let ws = watcher_status::LocalWatcherStatus::default();
        let settings = watcher_settings::WatcherSettings::load().await;
        ws.set_cc_sync_enabled(settings.cc_sync_enabled);
        ws
    };

    // ── Data Sources: catalog + connector registry ──────────────────────────
    let ds_catalog: Arc<dyn DataSourceCatalog> = Arc::new(
        datasource_catalog::SqliteDataSourceCatalog::new(pool.clone())
            .with_watcher_status(watcher_status.clone()),
    );
    let ds_control = Arc::new(datasource_catalog::SqliteDataSourceControl::new(
        pool.clone(),
    ));

    let mut conn_reg = DataSourceRegistry::new();
    // Register all supported connectors (same set as the hosted server).
    use pensieve_datasources::prometheus::PromDataSource;
    conn_reg.register(Arc::new(PromDataSource));
    conn_reg.register(Arc::new(
        pensieve_datasources::postgres::PgIntrospectDataSource,
    ));
    conn_reg.register(Arc::new(pensieve_datasources::s3::S3DataSource));
    conn_reg.register(Arc::new(pensieve_datasources::gitlab::GitlabDataSource));
    conn_reg.register(Arc::new(
        pensieve_datasources::bitbucket::BitbucketDataSource,
    ));
    conn_reg.register(Arc::new(pensieve_datasources::github::GithubDataSource));
    conn_reg.register(Arc::new(pensieve_datasources::notion::NotionDataSource));
    conn_reg.register(Arc::new(
        pensieve_datasources::googledrive::GdriveDataSource,
    ));
    conn_reg.register(Arc::new(pensieve_datasources::gmail::GmailDataSource));
    conn_reg.register(Arc::new(pensieve_datasources::slack::SlackDataSource));
    conn_reg.register(Arc::new(pensieve_datasources::jira::JiraDataSource));
    conn_reg.register(Arc::new(
        pensieve_datasources::confluence::ConfluenceDataSource,
    ));
    conn_reg.register(Arc::new(pensieve_datasources::msfabric::MsFabricDataSource));
    conn_reg.register(Arc::new(pensieve_datasources::obsidian::ObsidianDataSource));
    let conn_registry = Arc::new(conn_reg);

    // ── RowSink: auto-create + evolve schema, then ingest ───────────────────
    let catalog_for_sink = engine.catalog.clone();
    let format_for_sink = engine.format.clone();
    let conn_sink: RowSink = Arc::new(
        move |db: String, tbl: String, rows: Vec<serde_json::Value>, idem: Option<String>| {
            let catalog = catalog_for_sink.clone();
            let write_path =
                pensieve_ingest_core::WritePath::new(catalog.clone(), format_for_sink.clone());
            Box::pin(async move {
                let table = pensieve_ingest_core::ensure_table(catalog.as_ref(), &db, &tbl)
                    .await
                    .map_err(|e| anyhow::anyhow!("ensure_table: {e}"))?;
                let table = pensieve_ingest_core::evolve_schema_for_records(
                    catalog.as_ref(),
                    &db,
                    table,
                    &rows,
                )
                .await
                .map_err(|e| anyhow::anyhow!("evolve_schema: {e}"))?;
                let batches =
                    pensieve_datasources::arrow_coerce::rows_to_batches(&table.schema, rows)
                        .map_err(|e| anyhow::anyhow!("arrow coerce: {e}"))?;
                write_path
                    .ingest_with_idempotency(&db, &table, batches, idem.as_deref())
                    .await
                    .map_err(|e| anyhow::anyhow!("ingest: {e}"))?;
                Ok(())
            })
        },
    );

    // ── GraphRegisterFn: create property graph binding after ingest ─────────
    let catalog_for_graph = engine.catalog.clone();
    let graph_register: GraphRegisterFn =
        Arc::new(move |db: String, hint: pensieve_datasources::GraphHint| {
            let catalog = catalog_for_graph.clone();
            Box::pin(async move {
                let spec = pensieve_core::catalog::GraphSpec::with_defaults(
                    hint.node_table.clone(),
                    hint.edge_table.clone(),
                );
                match catalog.create_graph(&db, &hint.graph_name, spec).await {
                    Ok(_) => {}
                    Err(e) => {
                        let msg = e.to_string();
                        if !msg.contains("already exists")
                            && !msg.contains("duplicate")
                            && !msg.contains("23505")
                        {
                            tracing::warn!(db=%db, graph=%hint.graph_name, "graph register: {e}");
                        }
                    }
                }
                Ok(())
            })
        });

    let tick_deps = DataSourceTickDeps {
        control: ds_control.clone(),
        registry: conn_registry.clone(),
        sink: conn_sink,
        graph_register,
        secrets: Arc::new(EnvSecretStore),
        credentials: cred_store.clone(),
        oauth: None, // OAuth requires Postgres
        artifacts: None,
        catalog: Some(engine.catalog.clone()),
    };

    // Brain repos need the git binary; absent ⇒ the surface degrades to 503s.
    let brain_git = pensieve_brain::gitbin::GitBin::detect().await.map(Arc::new);

    let (app, agent_state, brain_state) = build_local_app(
        engine.catalog.clone(),
        engine.format.clone(),
        backend,
        memory,
        local_dreaming.clone(),
        mcp_url,
        Some(cred_store),
        Some((ds_catalog.clone(), conn_registry.clone())),
        watcher_status.clone(),
        brain_git,
    );

    // ── Data source scheduler: periodic tick + trigger enqueue ──────────────
    {
        let sched = DataSourceScheduler::new(ds_catalog.clone());
        tokio::spawn(async move {
            sched
                .run(async {
                    let _ = tokio::signal::ctrl_c().await;
                })
                .await;
        });
        info!("local data source scheduler running");
    }

    // ── Fabric worker: claims + executes data_source_sync, plus (S1.5) the
    //    index_build + embed_backfill jobs that activate local-mode ANN + BM25
    //    retrieval — every server retrieval capability except the global
    //    centroid tree, which is server-only by design. ────────────────────────
    {
        let worker_id = uuid::Uuid::new_v4();
        let queue = Arc::new(sqlite_queue::SqliteQueue::new(
            pool.clone(),
            worker_id,
            vec!["data_source".to_string()],
            120, // 2-minute lease
        ));
        let mut exec_reg = pensieve_jobs::ExecutorRegistry::new();
        exec_reg.register(Arc::new(
            pensieve_jobs::datasource_sync::DataSourceSyncExecutor::new(tick_deps),
        ));

        // Register the sidecar-building executors + start the activation
        // scheduler. Gated on an object store (always present for the local TLM
        // format); the scheduler enqueues into the same SQLite `jobs` table this
        // worker drains.
        let mut indexing_kinds = "data_source_sync";
        if let Some(store) = engine.format.object_store() {
            let mut builders: std::collections::HashMap<
                pensieve_core::index_sidecar::SidecarKind,
                Arc<dyn pensieve_core::index_sidecar::SidecarBuilder>,
            > = std::collections::HashMap::new();
            builders.insert(
                pensieve_core::index_sidecar::SidecarKind::IvfRabitq,
                Arc::new(pensieve_index_vector::IvfRabitqBuilder::new()),
            );
            builders.insert(
                pensieve_core::index_sidecar::SidecarKind::TantivyFts,
                Arc::new(pensieve_index_fts::TantivyFtsBuilder::new()),
            );
            exec_reg.register(Arc::new(
                pensieve_jobs::index_build::IndexBuildExecutor::new(
                    engine.catalog.clone(),
                    engine.format.clone(),
                    store,
                    builders,
                ),
            ));
            // The embed_backfill executor needs the process embedding backend.
            // If unavailable (provider feature off) only ANN/FTS over already-
            // embedded data activate; embeddings stay as ingested.
            match pensieve_memory::shared_embedding().await {
                Ok(embedder) => {
                    exec_reg.register(Arc::new(
                        pensieve_jobs::embed_backfill::EmbedBackfillExecutor::new(
                            engine.catalog.clone(),
                            engine.format.clone(),
                            embedder,
                        ),
                    ));
                    indexing_kinds = "data_source_sync + index_build + embed_backfill";
                }
                Err(e) => {
                    warn!(error = %e, "embedding backend unavailable; local embed_backfill disabled");
                    indexing_kinds = "data_source_sync + index_build";
                }
            }

            // Activation scheduler: enqueue missing index_build / embed_backfill
            // jobs over the SQLite jobs table. Same scan logic the server runs;
            // stops on Ctrl-C like the other local loops.
            let scheduler = pensieve_compaction::IndexScheduler::with_enqueuer(
                engine.catalog.clone(),
                Arc::new(sqlite_queue::SqliteEnqueuer::new(pool.clone())),
            );
            let (sched_shutdown, sched_rx) = tokio::sync::broadcast::channel::<()>(1);
            tokio::spawn(async move {
                let _ = tokio::signal::ctrl_c().await;
                let _ = sched_shutdown.send(());
            });
            tokio::spawn(scheduler.run(sched_rx));

            // Graph-snapshot activation scheduler (S3.2): the same refresh loop
            // the server runs, so local-mode deep-subgraph queries also serve
            // from a persistent CSR snapshot once a graph's edges change.
            let graph_snap = pensieve_server::graph_snapshot_sched::GraphSnapshotScheduler::new(
                engine.catalog.clone(),
                engine.format.clone(),
            );
            let (gsnap_shutdown, gsnap_rx) = tokio::sync::broadcast::channel::<()>(1);
            tokio::spawn(async move {
                let _ = tokio::signal::ctrl_c().await;
                let _ = gsnap_shutdown.send(());
            });
            tokio::spawn(graph_snap.run(gsnap_rx));
        }

        let runner = pensieve_jobs::JobRunner::new(queue, exec_reg, 120);
        tokio::spawn(async move {
            runner
                .run(async {
                    let _ = tokio::signal::ctrl_c().await;
                })
                .await;
        });
        info!(%worker_id, kinds = indexing_kinds, "local fabric worker running");
    }

    // Optional in-process dreaming scheduler — only fires when dreaming is
    // enabled in ${PENSIEVE_HOME}/memory-settings.json (OFF by default). Runs inline
    // in this process; no worker fabric.
    if let Some(store) = local_dreaming {
        let scheduler = pensieve_server::agent::dreaming::LocalDreamingScheduler::new(
            agent_state.clone(),
            store,
        );
        tokio::spawn(async move {
            scheduler
                .run(async {
                    let _ = tokio::signal::ctrl_c().await;
                })
                .await;
        });
    }

    // In-process brain-export scheduler — exports each brain on its own
    // interval (per-brain `export_interval_secs`; 0 = manual only). Shares
    // the app's BrainState so exports serialize against pushes.
    {
        let scheduler = pensieve_server::brain::scheduler::LocalBrainScheduler::new(brain_state);
        tokio::spawn(async move {
            scheduler
                .run(async {
                    let _ = tokio::signal::ctrl_c().await;
                })
                .await;
        });
    }

    // In-process compaction. Local mode previously never compacted (the
    // scheduler was hardcoded to Postgres), so the `extents` table grew
    // unbounded and every scan paid to enumerate thousands of tiny segments.
    // The worker + scheduler now cooperate via the catalog task queue over
    // SQLite; each stops on Ctrl-C like the dreaming scheduler above. Thresholds
    // are tunable via PENSIEVE_COMPACTION_* (same knobs as server mode).
    {
        use pensieve_compaction::{CompactionScheduler, CompactionWorker};
        let mut worker = CompactionWorker::new(
            engine.catalog.clone(),
            engine.format.clone(),
            pensieve_core::types::NodeId::new(),
        );
        let mut scheduler = CompactionScheduler::new(engine.catalog.clone());
        if let Some(ms) = std::env::var("PENSIEVE_COMPACTION_IDLE_SLEEP_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
        {
            worker.idle_sleep = std::time::Duration::from_millis(ms);
        }
        if let Some(s) = std::env::var("PENSIEVE_COMPACTION_POLL_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
        {
            scheduler.poll_interval = std::time::Duration::from_secs(s);
        }
        if let Some(n) = std::env::var("PENSIEVE_COMPACTION_MIN_EXTENTS")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
        {
            scheduler.min_extents_to_compact = n;
        }
        tokio::spawn(worker.run(async {
            let _ = tokio::signal::ctrl_c().await;
        }));
        tokio::spawn(scheduler.run(async {
            let _ = tokio::signal::ctrl_c().await;
        }));
        info!("local compaction worker + scheduler running");
    }

    // Snapshot + manifest + finished-task rows are append-only unless something
    // deletes them. Queries don't read superseded snapshots, so reclaim them.
    {
        let sqlite = engine.sqlite.clone();
        tokio::spawn(async move {
            loop {
                match sqlite
                    .gc_history(chrono::Duration::minutes(10), 2_000)
                    .await
                {
                    Ok(stats) => {
                        let n =
                            stats.snapshots_deleted + stats.manifests_deleted + stats.tasks_deleted;
                        if n > 0 {
                            info!(
                                snapshots = stats.snapshots_deleted,
                                manifests = stats.manifests_deleted,
                                tasks = stats.tasks_deleted,
                                "catalog gc reclaimed superseded snapshot history"
                            );
                        }
                        let drained = stats.snapshots_deleted < 2_000
                            && stats.manifests_deleted < 2_000
                            && stats.tasks_deleted < 2_000;
                        if drained {
                            // Retry while the freelist is still large. A busy
                            // pool makes VACUUM fail, and the next ticks delete
                            // nothing, so gating this on snapshots_deleted
                            // would leave the file bloated forever.
                            match sqlite.reclaim_free_pages().await {
                                Ok(true) => info!("catalog vacuum reclaimed free pages"),
                                Ok(false) => {}
                                Err(e) => {
                                    tracing::warn!(error = %e, "catalog vacuum skipped")
                                }
                            }
                            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                        } else {
                            tokio::task::yield_now().await;
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "catalog gc failed");
                        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                    }
                }
            }
        });
        info!("local catalog gc running (snapshot history + finished tasks)");
    }

    // Vault watchers: continuous sync loops for continuous-drive data sources
    // (obsidian). Reconciles enabled rows ↔ running notify loops every 15s,
    // so create/pause/resume/delete in the UI take effect without a restart.
    {
        let eng = engine.clone();
        let status = watcher_status.clone();
        let control = ds_control.clone();
        let pool_w = pool.clone();
        tokio::spawn(async move {
            source_watchers::run_manager(eng, status, control, pool_w).await;
        });
        info!("vault watcher manager running (continuous data sources)");
    }

    // Resident watcher: keep Claude Code file memory synced while the local
    // server runs. On by default — disable via the UI (Settings) or by setting
    // PENSIEVE_CC_WATCH=0 as a host-level kill switch.
    if env_flag("PENSIEVE_CC_WATCH", true) {
        let eng = engine.clone();
        let agent = agent_state.clone();
        let poll = env_secs("PENSIEVE_CC_SYNC_POLL_SECS", 30);
        let status = watcher_status.clone();
        tokio::spawn(async move {
            let opts = SyncOptions::default();
            let (node_host, node_id, identity) = watcher_status::node_identity();
            let config = serde_json::json!({
                "poll_secs": poll.as_secs(),
                "root": "~/.claude/projects",
            });
            let started_at = chrono::Utc::now().to_rfc3339();
            let mut last_scan: Option<serde_json::Value> = None;
            loop {
                if status.cc_sync_enabled() {
                    match run_cc_phase(&eng, Some(&agent), &opts).await {
                        Ok(scan) => last_scan = Some(scan),
                        Err(e) => tracing::warn!("cc-sync watcher: {e}"),
                    }
                    status.upsert(watcher_status::LocalWatcher {
                        id: "cc-sync".into(),
                        kind: "cc_sync".into(),
                        node_host: node_host.clone(),
                        node_id: node_id.clone(),
                        identity: identity.clone(),
                        config: config.clone(),
                        started_at: started_at.clone(),
                        last_heartbeat_at: chrono::Utc::now().to_rfc3339(),
                        last_scan: last_scan.clone(),
                    });
                } else {
                    // Disabled — remove from the visible list so the UI shows
                    // the empty state with the Enable button.
                    status.remove("cc-sync");
                    last_scan = None;
                }
                tokio::time::sleep(poll).await;
            }
        });
        let enabled = watcher_status.cc_sync_enabled();
        if enabled {
            info!(
                "cc-sync watcher running (every {:?}; toggle in UI or PENSIEVE_CC_WATCH=0 to disable)",
                poll
            );
        } else {
            info!("cc-sync watcher spawned but disabled via settings (enable in UI)");
        }
    }

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding {addr}"))?;
    info!(%addr, "pensieve serving the web UI + full HTTP API (local, SQLite)");
    info!("  web UI:   http://{addr}/   (sign in: {user} / {password})");
    info!("  ingest:   POST http://{addr}/v1/ingest   (X-Database / X-Table headers)");
    info!("  MCP:      http://{addr}/mcp/v1");
    if password == "admin" {
        warn!(
            "using the default local password 'admin' — set PENSIEVE_LOCAL_PASSWORD to change it"
        );
    }
    // `into_make_service_with_connect_info` so handlers can read the peer addr
    // (the live-consumers overlay records the connecting agent's ip/pid).
    let served = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async {
        let _ = tokio::signal::ctrl_c().await;
        info!("shutdown signal received");
    })
    .await
    .context("http server");
    // Land queued memories before exiting (Ctrl-C path).
    if let Some(memq) = memq {
        memq.shutdown().await;
    }
    served?;
    Ok(())
}

/// Options for `pensieve sync`.
#[derive(Debug, Clone, Default)]
pub struct SyncOptions {
    /// Keep running, re-syncing on an interval (`PENSIEVE_CC_SYNC_POLL_SECS`,
    /// default 30s).
    pub watch: bool,
    /// Plan + audit-log Claude Code file changes without writing them
    /// (memory ingestion into the local engine still runs).
    pub dry_run: bool,
    /// Only the local Claude Code file phase.
    pub cc_only: bool,
    /// Only the control-plane push/pull.
    pub cloud_only: bool,
    /// Limit the file phase to one project path.
    pub project: Option<std::path::PathBuf>,
}

fn env_flag(key: &str, default: bool) -> bool {
    std::env::var(key).map_or(default, |v| v != "0")
}

/// First `admin`-role token from `PENSIEVE_AUTH_TOKENS` (`tok:role,…`), if any.
/// Used to derive `PENSIEVE_INTERNAL_BEARER` so the dreaming ClaudeCli path can
/// reach the auth-protected loopback MCP endpoint.
fn first_admin_token() -> Option<String> {
    let raw = std::env::var("PENSIEVE_AUTH_TOKENS").ok()?;
    raw.split(',').find_map(|pair| {
        let (tok, role) = pair.trim().split_once(':')?;
        (role.trim() == "admin" && !tok.trim().is_empty()).then(|| tok.trim().to_string())
    })
}

fn env_secs(key: &str, default: u64) -> std::time::Duration {
    std::time::Duration::from_secs(
        std::env::var(key)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default),
    )
}

fn home_dir() -> String {
    std::env::var("HOME").unwrap_or_else(|_| ".".to_string())
}

/// `~/.claude` (override: `PENSIEVE_CC_HOME`).
fn claude_home() -> std::path::PathBuf {
    std::env::var("PENSIEVE_CC_HOME")
        .unwrap_or_else(|_| format!("{}/.claude", home_dir()))
        .into()
}

/// Assemble the file-phase options from env + CLI options.
fn cc_pipeline_options(opts: &SyncOptions) -> cc_pipeline::CcPipelineOptions {
    let cc = claude_home();
    let pensieve_home = pensieve_core::config::home_or(".").display().to_string();
    cc_pipeline::CcPipelineOptions {
        sync: cc_sync::CcSyncOptions {
            projects_dir: cc.join("projects"),
            claude_json: Some(format!("{}/.claude.json", home_dir()).into()),
            project: opts.project.clone(),
        },
        curate: env_flag("PENSIEVE_CC_CURATE", true),
        promote_cfg: pensieve_server::agent::cc_curate::CurationConfig::from_env(),
        llm_cfg: pensieve_server::agent::cc_curate::LlmCurationConfig::from_env(),
        writeback: cc_writeback::WritebackConfig {
            dry_run: opts.dry_run,
            quiet_window: env_secs("PENSIEVE_CC_QUIET_WINDOW", 300),
            lock_ttl: env_secs("PENSIEVE_CC_LOCK_TTL", 300),
            sessions_dir: Some(cc.join("sessions")),
            audit_log: Some(format!("{pensieve_home}/cc-curation.log").into()),
        },
    }
}

/// One Claude Code file-phase pass: ingest memory files → curate → apply.
///
/// Returns the sync-phase rollup in the watcher `last_scan` shape
/// (`CcSyncReport::last_scan_value`) so the `PENSIEVE_CC_WATCH` loop can feed
/// `/v1/data-sources/watchers`; other callers ignore it.
async fn run_cc_phase(
    engine: &Engine,
    agent: Option<&AgentState>,
    opts: &SyncOptions,
) -> Result<serde_json::Value> {
    let pass_start = std::time::Instant::now();
    let wall_start = chrono::Utc::now();
    let embed = pensieve_memory::shared_embedding()
        .await
        .map_err(|e| anyhow::anyhow!("embedding backend: {e}"))?;
    let writer =
        pensieve_memory::MemoryWriter::new(engine.catalog.clone(), engine.format.clone(), embed);
    let popts = cc_pipeline_options(opts);
    let report = cc_pipeline::run_pass(engine, &writer, agent, &popts).await?;
    let (mut up, mut skip, mut arch) = (0, 0, 0);
    for p in &report.sync.projects {
        up += p.upserted + p.user_edited;
        skip += p.skipped;
        arch += p.archived;
    }
    let (mut written, mut farch) = (0, 0);
    for c in &report.curated {
        written += c.applied.written;
        farch += c.applied.archived;
        tracing::debug!(
            realm = %c.realm,
            promoted = c.outcome.promoted,
            refreshed = c.outcome.refreshed,
            merged = c.outcome.merged,
            index_entries = c.outcome.index_entries,
            written = c.applied.written,
            archived = c.applied.archived,
            "cc-sync curated project"
        );
    }
    eprintln!(
        "cc-sync ok — {} project(s): {up} ingested, {skip} unchanged, {arch} archived in store; \
         {written} file(s) written, {farch} file(s) archived{}",
        report.sync.projects.len(),
        if opts.dry_run { " (dry run)" } else { "" },
    );
    write_cc_sync_health(true, None);
    Ok(report.sync.last_scan_value(
        u64::try_from(pass_start.elapsed().as_millis()).unwrap_or(u64::MAX),
        wall_start,
    ))
}

/// Path to the cc-sync health marker `pensieve status` reads. Fixed under
/// `$HOME/.pensieve`, not `PENSIEVE_HOME`-relative, matching the hook-side
/// `capture-health.json` convention so both processes agree on the path.
fn cc_sync_health_path() -> Option<std::path::PathBuf> {
    std::env::var_os("HOME").map(|h| {
        std::path::PathBuf::from(h)
            .join(".pensieve")
            .join("cc-sync-health.json")
    })
}

/// Record the outcome of a cc-sync pass so `pensieve status` can report sync
/// freshness instead of it being invisible between session-boundary hook
/// runs (which run this detached, fire-and-forget). Best-effort: a failure
/// to write the marker never fails the sync itself.
fn write_cc_sync_health(ok: bool, detail: Option<&str>) {
    let Some(path) = cc_sync_health_path() else {
        return;
    };
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let body = serde_json::json!({
        "ts": chrono::Utc::now().to_rfc3339(),
        "status": if ok { "ok" } else { "error" },
        "detail": detail,
    });
    let _ = std::fs::write(&path, body.to_string());
}

/// `pensieve sync` — sync memory with Claude Code's file memory (always, when
/// `~/.claude` exists) and with a control plane (when `PENSIEVE_CLOUD_URL` is
/// set). `--watch` keeps the loop running.
pub async fn run_sync(opts: SyncOptions) -> Result<()> {
    let engine = open_engine(&resolve_paths()).await?;
    let poll = env_secs("PENSIEVE_CC_SYNC_POLL_SECS", 30);
    loop {
        // ── Phase 1: Claude Code memory files (fails open). ──
        if !opts.cloud_only && env_flag("PENSIEVE_CC_FILE_SYNC", true) {
            if let Err(e) = run_cc_phase(&engine, None, &opts).await {
                eprintln!("cc-sync failed (continuing): {e}");
                write_cc_sync_health(false, Some(&e.to_string()));
            }
        }
        // ── Phase 2: control plane (only when configured). ──
        if !opts.cc_only {
            match std::env::var("PENSIEVE_CLOUD_URL") {
                Ok(cloud_url) if !cloud_url.is_empty() => {
                    let cfg = sync::SyncConfig {
                        cloud_url,
                        token: std::env::var("PENSIEVE_CLOUD_TOKEN")
                            .ok()
                            .filter(|s| !s.is_empty()),
                        realm: std::env::var("PENSIEVE_SYNC_REALM")
                            .ok()
                            .filter(|s| !s.is_empty()),
                        now: chrono::Utc::now().to_rfc3339(),
                    };
                    sync::run(&engine, cfg).await?;
                }
                _ if opts.cloud_only => {
                    return Err(anyhow::anyhow!(
                        "set PENSIEVE_CLOUD_URL (and usually PENSIEVE_CLOUD_TOKEN) to sync to a control plane"
                    ));
                }
                _ => {}
            }
        }
        if !opts.watch {
            return Ok(());
        }
        tokio::select! {
            () = tokio::time::sleep(poll) => {}
            _ = tokio::signal::ctrl_c() => return Ok(()),
        }
    }
}

/// `pensieve setup <agent>` — wire a coding agent to `pensieve mcp` over stdio.
pub fn run_setup(agent: &str, print: bool) -> Result<()> {
    setup::run(agent, print)
}

/// Print the resolved local paths (diagnostics).
pub fn print_info() {
    let paths = resolve_paths();
    eprintln!("pensieve — local single-binary context engine");
    eprintln!("  catalog : {}", paths.catalog_db);
    eprintln!("  data    : {}", paths.data_root);
    eprintln!("  mcp     : pensieve mcp           (stdio MCP; memory + data + graph)");
    eprintln!("  serve   : pensieve serve         (web UI + HTTP API + ingest, zero-auth)");
    eprintln!("  setup   : pensieve setup <agent> (wire claude-code/cursor/windsurf to mcp)");
    eprintln!(
        "  sync    : pensieve sync          (Claude Code file memory + PENSIEVE_CLOUD_URL push/pull)"
    );
    eprintln!("            --watch --dry-run --cc-only --cloud-only --project <path>");
    eprintln!(
        "  worker  : pensieve worker install|uninstall|status (background sync as an OS user service)"
    );
}
