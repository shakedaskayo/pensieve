//! The main pensieve binary.
//!
//! Phase A wiring: loads config from env, connects to Postgres + MinIO,
//! registers this node in the catalog, starts the HTTP server with ingest +
//! query routes mounted.
//!
//! Future slices add: gRPC server, background compaction worker, TTL GC,
//! Kafka consumers, file-drop watchers, OTLP receivers, WAL + staging
//! buffers, proper config file support, graceful shutdown.

use anyhow::{Context, Result};
use clap::Parser;
use pensieve_catalog::PostgresCatalog;
use pensieve_compaction::{
    ArtifactRetentionWorker, CompactionScheduler, CompactionWorker, PhysicalDeleteWorker,
    RetentionSweeper,
};
use pensieve_core::catalog::GraphSpec;
use pensieve_core::catalog::{Catalog, NodeInfo, NodeRole};
use pensieve_core::segment_format::SegmentFormat;
use pensieve_datasources::prometheus::PromDataSource;
use pensieve_datasources::registry::DataSourceRegistry;
use pensieve_datasources::scheduler::DataSourceScheduler;
use pensieve_datasources::secrets::EnvSecretStore;
use pensieve_datasources::PgDataSourceCatalog;
use pensieve_format_tlm::TelemetryFormat;
use pensieve_ingest_core::{
    ensure_table, events::IngestEvents, evolve_schema_for_records, spawn_idempotency_cleanup,
    CommitCoordinator, CoordinatorConfig, StagingBuffer, StagingConfig, WritePath,
};
use pensieve_ingest_filedrop::{FiledropConfig, FiledropWatcher};
use pensieve_ingest_kafka::{KafkaConsumerConfig, KafkaConsumerWorker};
use pensieve_ingest_otlp::traces::OtlpTraceService;
use pensieve_ingest_otlp::OtlpLogsService;
use pensieve_ingest_rest::IngestState;
use pensieve_server::auth::{
    require_role_middleware, AuthBackend, AuthLayerState, EnvAuthBackend, Role,
};
use pensieve_server::{DataSourceAdminState, QueryState};
use pensieve_storage::{build_object_store, config_from_env};
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::LogsServiceServer as OtlpLogsServer;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::signal;
use tracing::{error, info, warn};

#[derive(Debug, Parser)]
#[command(
    name = "pensieve",
    about = "pensieve engine — unified data platform (pre-alpha)"
)]
struct Cli {
    /// Postgres catalog URL. Falls back to PENSIEVE_CATALOG_URL env var.
    #[arg(
        long,
        env = "PENSIEVE_CATALOG_URL",
        default_value = "postgres://pensieve:pensieve_dev@localhost:5433/pensieve"
    )]
    catalog_url: String,

    /// HTTP listen address.
    #[arg(long, env = "PENSIEVE_HTTP_ADDR", default_value = "0.0.0.0:8080")]
    http_addr: SocketAddr,

    /// gRPC (Arrow Flight) listen address. Set to "off" to disable.
    #[arg(long, env = "PENSIEVE_GRPC_ADDR", default_value = "0.0.0.0:9090")]
    grpc_addr: String,

    /// OTLP gRPC listen address (standard port 4317). Set to "off" to disable.
    #[arg(long, env = "PENSIEVE_OTLP_ADDR", default_value = "off")]
    otlp_addr: String,

    /// Target database for OTLP-received logs.
    #[arg(long, env = "PENSIEVE_OTLP_DATABASE", default_value = "default")]
    otlp_database: String,

    /// Object-store path prefix.
    #[arg(long, env = "PENSIEVE_PATH_PREFIX", default_value = "pensieve")]
    path_prefix: String,
}

/// Which background components a node runs, selected by `PENSIEVE_ROLE` (S2.4/S2.6
/// role split). The HTTP API (query + ingest) is always served; only background
/// work is gated, so stateless roles can be horizontally scaled.
struct RoleComponents {
    /// Run the staged-ingest committer loop (the PG lease still elects one).
    run_committer: bool,
    /// Run heavy background jobs (compaction worker + scheduler).
    run_jobs: bool,
}

/// Map `PENSIEVE_ROLE` → components. `all_in_one` (default / unknown) runs
/// everything (single-node, unchanged). `query`/`ingest`/`edge` are stateless
/// HTTP nodes; `committer` runs the commit lease; `worker`/`compaction` run jobs.
fn role_components(role: &str) -> RoleComponents {
    match role.trim().to_ascii_lowercase().as_str() {
        "query" | "ingest" | "edge" => RoleComponents {
            run_committer: false,
            run_jobs: false,
        },
        "committer" => RoleComponents {
            run_committer: true,
            run_jobs: false,
        },
        "worker" | "compaction" => RoleComponents {
            run_committer: false,
            run_jobs: true,
        },
        _ => RoleComponents {
            run_committer: true,
            run_jobs: true,
        },
    }
}

// Worker threads are 8 MiB. tokio-macros 2.7 has no `thread_stack_size`
// argument on `#[tokio::main]`; the iterative ingest scanner is the real
// guard, and this stack is the backstop for other recursive walks.
fn main() -> Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .thread_stack_size(8 * 1024 * 1024)
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(async_main())
}

async fn async_main() -> Result<()> {
    use opentelemetry::trace::TracerProvider as _;
    use tracing_subscriber::layer::SubscriberExt as _;
    use tracing_subscriber::util::SubscriberInitExt as _;
    use tracing_subscriber::Layer as _;

    // The env filter is attached to the fmt layer ONLY: a restrictive
    // RUST_LOG (e.g. `warn`) must not silence self-tracing, whose otel layer
    // carries its own `pensieve_telemetry` Targets filter below.
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        tracing_subscriber::EnvFilter::new("info,sqlx=warn,hyper=warn,h2=warn")
    });

    // Self-tracing: spans with target `pensieve_telemetry` are exported into our
    // own otel_traces table. The exporter starts unwired (drops batches) and
    // is connected to storage further down, once the write path exists.
    // (BatchSpanProcessor needs the Tokio runtime — we're inside #[tokio::main].)
    let self_exporter = pensieve_ingest_otlp::self_export::SelfTraceExporter::unwired();
    let self_trace_handle = self_exporter.handle();
    let tracer_provider = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_batch_exporter(self_exporter, opentelemetry_sdk::runtime::Tokio)
        .build();
    let tracer = tracer_provider.tracer("pensieve-server");
    let otel_layer = tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_filter(
            tracing_subscriber::filter::Targets::new()
                .with_target("pensieve_telemetry", tracing::Level::INFO),
        );

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(true)
                .with_filter(env_filter),
        )
        .with(otel_layer)
        .init();

    // Keep the provider (and its batch processor) alive for the process
    // lifetime. We intentionally do NOT call shutdown on exit — spans in the
    // last flush window (~5s) are lost on shutdown, which is acceptable for
    // self-tracing.
    let _tracer_provider_guard = tracer_provider;

    // Install the Prometheus recorder. Must happen before any metrics macro
    // fires — so, very first thing in main.
    let _metrics_handle = pensieve_server::metrics::install();

    let cli = Cli::parse();
    info!(catalog_url = %cli.catalog_url, http_addr = %cli.http_addr, "pensieve starting");

    // 1. Catalog.
    let pg_catalog = Arc::new(
        PostgresCatalog::connect(&cli.catalog_url)
            .await
            .with_context(|| format!("connecting to catalog at {}", cli.catalog_url))?,
    );
    // Hang on to the raw pg pool — the agent surface persists `agent_runs`
    // rows via it — before we erase the concrete type behind `dyn Catalog`.
    let pg_pool = pg_catalog.pool().clone();
    let catalog: Arc<dyn Catalog> = pg_catalog.clone();
    info!("catalog connected; migrations applied");

    // 1a. Seed the admin user from env if requested and no users exist yet.
    //     Both PENSIEVE_ADMIN_USER and PENSIEVE_ADMIN_PASSWORD must be set; if only
    //     one is set we skip seeding and warn instead of failing.
    {
        let admin_user = std::env::var("PENSIEVE_ADMIN_USER").ok();
        let admin_pw = std::env::var("PENSIEVE_ADMIN_PASSWORD").ok();
        match (admin_user, admin_pw) {
            (Some(user), Some(pw)) => {
                let user_count = catalog
                    .count_users()
                    .await
                    .context("counting users for admin seed check")?;
                if user_count == 0 {
                    let phc = pensieve_server::auth::passwords::hash_password(&pw)
                        .map_err(|e| anyhow::anyhow!("hashing admin password: {e}"))?;
                    catalog
                        .create_user(&user, &phc, "admin")
                        .await
                        .context("seeding admin user")?;
                    info!(username = %user, "seeded admin user");
                } else {
                    info!("PENSIEVE_ADMIN_USER set but users already exist — skipping seed");
                }
            }
            (Some(_), None) => {
                warn!(
                    "PENSIEVE_ADMIN_USER is set but PENSIEVE_ADMIN_PASSWORD is not — skipping admin seed"
                );
            }
            (None, Some(_)) => {
                warn!(
                    "PENSIEVE_ADMIN_PASSWORD is set but PENSIEVE_ADMIN_USER is not — skipping admin seed"
                );
            }
            (None, None) => {
                // Neither set — no seeding requested, this is fine.
            }
        }
    }

    // 2. Object store + format.
    let storage_config = config_from_env();
    let store = build_object_store(&storage_config).context("building object store")?;
    info!("object store ready");
    // Self-hosted single-tenant deployments write under DEFAULT_TENANT —
    // every extent path becomes `<prefix>/<tenant_id>/extents/<id>.pensieve`,
    // mirroring what cloud workspaces will do once Slice 2 plumbs per-tenant
    // formats through the write path. For self-hosted users this means a
    // one-time path-layout shift: pre-Slice-0 extents stay at their
    // catalog-stored `object_path` (read by exact match), new extents get
    // the tenant segment.
    // S2.1: per-extent format dispatch. Both TLM and Parquet are registered as
    // readers (a Parquet object starts `PAR1`, a TLM object `PENSIEVE…`, so reads
    // pick the decoder by magic); new extents use the format named by
    // PENSIEVE_WRITE_FORMAT (default "tlm" — flip to "parquet" once baked in, and
    // compaction migrates old extents organically). Old TLM extents stay
    // readable forever either way.
    let tenant = pensieve_core::tenant::DEFAULT_TENANT;
    let tlm_fmt: Arc<dyn SegmentFormat> = Arc::new(TelemetryFormat::with_tenant(
        store.clone(),
        cli.path_prefix.clone(),
        tenant,
    ));
    let parquet_fmt: Arc<dyn SegmentFormat> =
        Arc::new(pensieve_format_parquet::ParquetFormat::with_tenant(
            store.clone(),
            cli.path_prefix.clone(),
            tenant,
        ));
    let write_format = std::env::var("PENSIEVE_WRITE_FORMAT").unwrap_or_else(|_| "tlm".into());
    let format: Arc<dyn SegmentFormat> = match write_format.as_str() {
        "parquet" => Arc::new(pensieve_core::segment_format::FormatRegistry::new(
            parquet_fmt,
            vec![tlm_fmt],
        )),
        _ => Arc::new(pensieve_core::segment_format::FormatRegistry::new(
            tlm_fmt,
            vec![parquet_fmt],
        )),
    };
    info!(write_format = %write_format, "segment format registry: TLM + Parquet readers");

    // 3. Register this node in the catalog.
    let lease = catalog
        .register_node(NodeInfo {
            role: NodeRole::AllInOne,
            endpoint: cli.http_addr.to_string(),
            capabilities: serde_json::json!({
                "version": env!("CARGO_PKG_VERSION"),
                "features": ["http", "sql", "ingest"],
            }),
        })
        .await
        .context("registering node in catalog")?;
    info!(node_id = %lease.node_id, "node registered");

    // 4. Set up the shutdown broadcast channel now so background tasks
    //    (ingest staging, compaction, retention, gc) can subscribe.
    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(8);

    //    Build the HTTP router by merging ingest + query + health + metrics.
    //    Health + metrics stay unauthenticated; ingest + query get the auth
    //    middleware (bypassed at runtime when PENSIEVE_AUTH_TOKENS is empty).
    //
    //    Backend selection order:
    //    1. PENSIEVE_AUTH_BACKEND=db  → DbAuthBackend (cloud-auth feature, cloud only)
    //    2. PENSIEVE_AUTH_BACKEND=supabase → SupabaseAuthBackend (Supabase JWTs +
    //       JIT user provisioning, wrapping SessionAuthBackend for opaque tokens)
    //    3. PENSIEVE_AUTH_BACKEND=session OR users_exist → SessionAuthBackend
    //       (session tokens + env static tokens in one backend)
    //    4. Otherwise → EnvAuthBackend (static PENSIEVE_AUTH_TOKENS only)
    let users_exist = catalog
        .count_users()
        .await
        .context("counting users for backend selection")?
        > 0;

    let backend: Arc<dyn AuthBackend> = match std::env::var("PENSIEVE_AUTH_BACKEND").ok().as_deref() {
        #[cfg(feature = "cloud-auth")]
        Some("db") => {
            use pensieve_server::auth::DbAuthBackend;
            info!("auth: using db backend (api_tokens table)");
            Arc::new(DbAuthBackend::new(pg_pool.clone()))
        }
        #[cfg(not(feature = "cloud-auth"))]
        Some("db") => {
            warn!(
                "PENSIEVE_AUTH_BACKEND=db requested but the binary was compiled without \
                 the `cloud-auth` feature; falling back to SessionAuthBackend."
            );
            Arc::new(pensieve_server::auth::SessionAuthBackend::new(
                catalog.clone(),
                EnvAuthBackend::from_env(),
                users_exist,
            ))
        }
        Some("supabase") => {
            use pensieve_server::auth::{SupabaseAuthBackend, SupabaseAuthConfig};
            let config = SupabaseAuthConfig::from_env().context(
                "PENSIEVE_AUTH_BACKEND=supabase requires PENSIEVE_SUPABASE_URL \
                 (e.g. https://<ref>.supabase.co)",
            )?;
            info!(url = %config.url, "auth: using supabase backend (JWT + JIT users)");
            let inner = pensieve_server::auth::SessionAuthBackend::new(
                catalog.clone(),
                EnvAuthBackend::from_env(),
                users_exist,
            );
            Arc::new(SupabaseAuthBackend::new(config, catalog.clone(), inner))
        }
        Some("session") => {
            info!("auth: using session backend (PENSIEVE_AUTH_BACKEND=session)");
            Arc::new(pensieve_server::auth::SessionAuthBackend::new(
                catalog.clone(),
                EnvAuthBackend::from_env(),
                users_exist,
            ))
        }
        Some(other) if !other.is_empty() => {
            warn!("PENSIEVE_AUTH_BACKEND={other:?} unrecognized; using SessionAuthBackend.");
            Arc::new(pensieve_server::auth::SessionAuthBackend::new(
                catalog.clone(),
                EnvAuthBackend::from_env(),
                users_exist,
            ))
        }
        _ if users_exist => {
            info!("auth: users exist — using session backend");
            Arc::new(pensieve_server::auth::SessionAuthBackend::new(
                catalog.clone(),
                EnvAuthBackend::from_env(),
                users_exist,
            ))
        }
        _ => Arc::new(EnvAuthBackend::from_env()),
    };
    // Optionally wrap with OIDC validation. When PENSIEVE_OIDC_ISSUERS is set,
    // JWTs are validated against the listed issuers; non-JWT tokens fall
    // through to the inner backend. When unset, OIDC is disabled and the
    // inner backend handles all auth.
    let backend: Arc<dyn AuthBackend> = match pensieve_server::auth::OidcConfig::from_env() {
        Some(cfg) => {
            tracing::info!(
                issuers = ?cfg.issuers,
                audience = %cfg.audience,
                "OIDC auth enabled"
            );
            Arc::new(pensieve_server::auth::OidcAuthBackend::new(cfg, backend))
        }
        None => backend,
    };

    if backend.enabled() {
        info!("auth: bearer-token protection enabled on /v1/ingest (write) + /v1/query (read)");
    } else {
        info!("auth: disabled (set PENSIEVE_AUTH_TOKENS to enable)");
    }

    // Staging buffer (group-commit) drives ingest throughput. Can be
    // disabled by setting PENSIEVE_STAGING_DISABLED=1 for tests that want
    // one-extent-per-request semantics.
    let use_staging = std::env::var("PENSIEVE_STAGING_DISABLED")
        .map(|v| v != "1" && v != "true")
        .unwrap_or(true);
    // S2.4/S2.6 role split: PENSIEVE_ROLE selects which background work this node
    // runs. `all_in_one` (default) runs everything (single-node, unchanged).
    // `query`/`ingest`/`edge` are stateless HTTP nodes (no committer, no
    // compaction) and so HPA-safe; `committer` runs the commit lease;
    // `worker`/`compaction` run the heavy background jobs.
    let role = std::env::var("PENSIEVE_ROLE").unwrap_or_else(|_| "all_in_one".to_string());
    let rc = role_components(&role);
    info!(role = %role, run_committer = rc.run_committer, run_jobs = rc.run_jobs, "node role");

    // S2.2: staged ingest ("S3 is the WAL"). Routers stage the written extent
    // and ack; the async committer commits it. Opt-in via PENSIEVE_INGEST_MODE=staged
    // (default = synchronous read-your-writes). Staged mode bypasses the
    // group-commit StagingBuffer — the committer is the batching layer.
    let staged_ingest = std::env::var("PENSIEVE_INGEST_MODE").as_deref() == Ok("staged");
    let ingest_events = IngestEvents::new(256);
    // Live consumer-activity bus — fed by the memory tool paths, subscribed by
    // the /v1/consumers/live WebSocket that drives the graph explorer overlay.
    let consumer_events = pensieve_ingest_core::ConsumerEvents::new(256);
    let write_path: WritePath = if staged_ingest {
        info!("ingest mode: staged (router + async committer)");
        // Every staged node stages + acks; only committer-eligible roles run the
        // committer loop (the PG-advisory-lock lease still elects a single one).
        if rc.run_committer {
            let committer = pensieve_ingest_core::committer::Committer::new(
                catalog.clone(),
                pensieve_core::tenant::DEFAULT_TENANT,
            );
            let committer_rx = shutdown_tx.subscribe();
            tokio::spawn(committer.run(committer_rx));
        }
        WritePath::new(catalog.clone(), format.clone())
            .with_staged_mode()
            .with_events(ingest_events.clone())
    } else if use_staging {
        // Start the commit coordinator so flushes get group-commit squared.
        let coordinator = CommitCoordinator::spawn(catalog.clone(), CoordinatorConfig::from_env());
        let staging =
            StagingBuffer::new(catalog.clone(), format.clone(), StagingConfig::from_env())
                .with_coordinator(coordinator);
        // Background timer flushes age-expired buffers.
        let staging_rx = shutdown_tx.subscribe();
        let staging_for_timer = staging.clone();
        tokio::spawn(staging_for_timer.run_timer(async move {
            let mut rx = staging_rx;
            let _ = rx.recv().await;
        }));
        info!("ingest staging: group-commit enabled");
        WritePath::with_staging(catalog.clone(), format.clone(), staging)
            .with_events(ingest_events.clone())
    } else {
        info!("ingest staging: disabled (PENSIEVE_STAGING_DISABLED=1)");
        WritePath::new(catalog.clone(), format.clone()).with_events(ingest_events.clone())
    };
    // Connect self-tracing to storage (drops silently before this point).
    // `PENSIEVE_SELF_TRACE=off` (or 0/false) leaves the exporter unwired so the
    // server's own spans are never written to otel_traces — operators who don't
    // want self-traces consuming their storage, and deterministic tests that
    // assert exact extent/object counts, both set this.
    let self_trace_enabled = std::env::var("PENSIEVE_SELF_TRACE")
        .map(|v| !matches!(v.as_str(), "off" | "0" | "false"))
        .unwrap_or(true);
    if self_trace_enabled {
        let _ = self_trace_handle.set(pensieve_ingest_otlp::self_export::SelfTraceCtx {
            catalog: catalog.clone(),
            write_path: write_path.clone(),
            database: cli.otlp_database.clone(),
        });
        // Pre-create otel_traces so the Traces page never shows a 404 on fresh
        // install while waiting for the first self-trace batch to flush.
        pensieve_ingest_otlp::ensure_traces_table(&catalog, &cli.otlp_database).await;
    } else {
        info!("self-tracing: disabled (PENSIEVE_SELF_TRACE=off)");
    }
    let ingest_router = pensieve_ingest_rest::router(IngestState {
        catalog: catalog.clone(),
        write_path: write_path.clone(),
    })
    // Realm-scoped tokens have no realm control over raw ingest (a write could
    // inject memory_nodes rows with any realm) — fail closed.
    .layer(axum::middleware::from_fn(
        pensieve_server::realm_token_guard_middleware,
    ))
    // Database scope check runs after auth middleware injects Principal.
    .layer(axum::middleware::from_fn(
        pensieve_server::database_scope_middleware,
    ))
    .layer(axum::middleware::from_fn_with_state(
        AuthLayerState {
            backend: backend.clone(),
            required: Role::Write,
        },
        require_role_middleware,
    ));
    // System-wide encrypted credentials store + engine-preference store.
    // Built here (not later) because AgentState and the data source runner both
    // need them — see /v1/agent/* and /v1/data sources/*. Key loaded from
    // PENSIEVE_SECRET_KEY (sha256-stretched if shorter than 32 bytes).
    let crypto = std::sync::Arc::new(
        pensieve_core::crypto::Crypto::from_env()
            .context("loading credentials encryption key (PENSIEVE_SECRET_KEY)")?,
    );
    let cred_store = std::sync::Arc::new(pensieve_catalog::PgCredentialStore::new(
        pg_catalog.pool().clone(),
        crypto.clone(),
    ));

    // Live-proxy runtime for federated tables (Microsoft Fabric, …). Remote
    // queries run on the external platform's compute — the guardrails bound
    // each one. Tunable via env; defaults are conservative.
    let federation = pensieve_federation::runtime_from(cred_store.clone());
    let engine_store = std::sync::Arc::new(
        pensieve_server::agent::engine::PgEnginePreferenceStore::new(pg_pool.clone()),
    );

    let skills_store = std::sync::Arc::new(pensieve_server::agent::skills::PgEnabledSkillsStore::new(
        pg_pool.clone(),
    ));
    // Loopback URL of our own MCP endpoint, handed to the Claude CLI engine so
    // the agent can query the user's data via `--mcp-config`. Defaults to the
    // HTTP bind address (mapping a wildcard host to loopback); override with
    // PENSIEVE_AGENT_MCP_URL=<url>, or PENSIEVE_AGENT_MCP_URL=off to disable.
    let mcp_url = match std::env::var("PENSIEVE_AGENT_MCP_URL").ok().as_deref() {
        Some("off") => None,
        Some(url) if !url.is_empty() => Some(url.to_string()),
        _ => {
            let ip = if cli.http_addr.ip().is_unspecified() {
                "127.0.0.1".to_string()
            } else {
                cli.http_addr.ip().to_string()
            };
            Some(format!("http://{}:{}/mcp/v1", ip, cli.http_addr.port()))
        }
    };
    if let Some(url) = &mcp_url {
        info!(url = %url, "agent: Claude CLI engine will reach data tools via MCP");
    }

    // Async memory ingest queue — memory write tools ack immediately and a
    // background worker lands batched embeds + group commits. Durable by
    // default on the server: pending saves ride the catalog's background-task
    // store and replay after a crash. PENSIEVE_MEMORY_ASYNC=0 restores fully
    // synchronous writes.
    pensieve_server::agent::identity::set_source("server");
    let memory_async = std::env::var("PENSIEVE_MEMORY_ASYNC")
        .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
        .unwrap_or(true);
    let memory_queue = if memory_async {
        match pensieve_memory::shared_embedding().await {
            Ok(embed) => {
                let cfg = pensieve_memory::MemoryIngestConfig::from_env(true);
                let mut mem_rx = shutdown_tx.subscribe();
                let (q, handle) = pensieve_memory::spawn_memory_queue(
                    catalog.clone(),
                    format.clone(),
                    embed,
                    cfg,
                    lease.node_id,
                    async move {
                        let _ = mem_rx.recv().await;
                    },
                );
                info!("async memory ingest queue started (durable, batched)");
                Some((q, handle))
            }
            Err(e) => {
                warn!(error = %e, "embedding backend unavailable; memory writes stay synchronous");
                None
            }
        }
    } else {
        info!("PENSIEVE_MEMORY_ASYNC=0 — memory writes are synchronous");
        None
    };
    let memory = memory_queue.as_ref().map(|(q, _)| q.clone());

    let agent_state = pensieve_server::agent::AgentState {
        catalog: catalog.clone(),
        format: format.clone(),
        pool: Some(pg_pool.clone()),
        engines: engine_store.clone(),
        credentials: cred_store.clone(),
        tenant: pensieve_core::tenant::DEFAULT_TENANT,
        skills: skills_store,
        mcp_url,
        memory: memory.clone(),
        // Server mode: dreaming persists to Postgres + the worker fabric, not
        // the degraded local store. Settings live in the `memory_settings` row.
        local_dreaming: None,
        memory_settings_path: None,
        consumer_events: Some(consumer_events.clone()),
    };
    // Build the SchemaCache once and share it across the query router, live
    // router, and flight router via Arc::clone — they all serve the same node
    // and benefit from a shared schema-doc TTL window.
    let schema_cache = std::sync::Arc::new(pensieve_server::catalog_handler::SchemaCache::from_env());
    let query_router = pensieve_server::router_with_agent(
        QueryState {
            federation: Some(federation.clone()),
            catalog: catalog.clone(),
            format: format.clone(),
            schema_cache: schema_cache.clone(),
            node_id: Some(lease.node_id),
            pg_pool: Some(std::sync::Arc::new(pg_pool.clone())),
            layout_cache: std::sync::Arc::new(pensieve_server::graph_layout_cache::LayoutCache::new()),
        },
        agent_state.clone(),
    )
    .merge(pensieve_server::artifacts_handler::artifacts_router(
        catalog.clone(),
        store.clone(),
    ))
    .merge(
        pensieve_server::discover::consumers_live::consumers_emit_router(Some(consumer_events.clone())),
    )
    .layer(axum::middleware::from_fn_with_state(
        AuthLayerState {
            backend: backend.clone(),
            required: Role::Read,
        },
        require_role_middleware,
    ));

    // Build MCP state from the same SharedToolCtx the inline /v1/agent endpoint uses.
    let mcp_shared = pensieve_server::agent::SharedToolCtx {
        realm_scope: Default::default(),
        consumer_sink: Some(std::sync::Arc::new(
            pensieve_server::agent::LocalConsumerPublisher {
                events: consumer_events.clone(),
                tenant: pensieve_core::tenant::DEFAULT_TENANT,
            },
        )),
        federation: Some(federation.clone()),
        catalog: catalog.clone(),
        format: format.clone(),
        pool: Some(pg_pool.clone()),
        memory: memory.clone(),
        hitl: None,
        memory_settings_path: None,
    };
    // Read-only data source access over MCP: lets MCP-driven agents (notably
    // Claude CLI dreaming runs) fill memory gaps from configured sources.
    // Budget here is generous server-lifetime hygiene; per-run budgets are
    // enforced on the adk path and by wall-clock on the CLI path.
    let mcp_data_source_ctx = pensieve_server::agent::datasource_tools::DataSourceToolCtx {
        pool: Some(pg_pool.clone()),
        credentials: cred_store.clone(),
        tenant: pensieve_core::tenant::DEFAULT_TENANT,
        budget: Arc::new(
            pensieve_server::agent::datasource_tools::DataSourceReadBudget::new(10_000, u64::MAX),
        ),
    };
    // Per-request rebuild ingredients for realm-restricted tokens (server mode
    // only). Cloned before the base dispatch consumes the originals.
    let mcp_builder = pensieve_mcp::DispatchBuilder {
        shared: mcp_shared.clone(),
        artifact_store: Some(store.clone()),
        datasource_ctx: Some(mcp_data_source_ctx.clone()),
    };
    let mcp_state = pensieve_mcp::McpState {
        dispatch: pensieve_mcp::ToolDispatch::new(mcp_shared)
            .with_artifact_store(store.clone())
            .with_datasource_tools(mcp_data_source_ctx),
        builder: Some(mcp_builder),
        server_info: pensieve_mcp::ServerInfo {
            name: "pensieve".into(),
            version: env!("CARGO_PKG_VERSION").into(),
        },
    };
    let mcp_router = pensieve_mcp::router(mcp_state)
        // Fail closed: MCP tools (execute_sql etc.) address databases
        // internally — same policy as /v1/agent/* and /flight/*.
        .layer(axum::middleware::from_fn(
            pensieve_server::scoped_token_guard_middleware,
        ))
        .layer(axum::middleware::from_fn_with_state(
            AuthLayerState {
                backend: backend.clone(),
                required: Role::Read,
            },
            require_role_middleware,
        ));
    // DataSource registry + row-sink.
    let mut conn_reg = DataSourceRegistry::new();
    conn_reg.register(Arc::new(PromDataSource));
    conn_reg.register(Arc::new(pensieve_datasources::postgres::PgIntrospectDataSource));
    conn_reg.register(Arc::new(pensieve_datasources::s3::S3DataSource));
    conn_reg.register(Arc::new(pensieve_datasources::gitlab::GitlabDataSource));
    conn_reg.register(Arc::new(pensieve_datasources::bitbucket::BitbucketDataSource));
    // GitHub registers unconditionally (metadata + repo graph). The deep code
    // graph inside it is feature-gated; the data source itself is always present.
    conn_reg.register(Arc::new(pensieve_datasources::github::GithubDataSource));
    // OAuth2 SaaS data sources (token via the connect flow → encrypted credential).
    conn_reg.register(Arc::new(pensieve_datasources::notion::NotionDataSource));
    conn_reg.register(Arc::new(pensieve_datasources::googledrive::GdriveDataSource));
    conn_reg.register(Arc::new(pensieve_datasources::gmail::GmailDataSource));
    conn_reg.register(Arc::new(pensieve_datasources::slack::SlackDataSource));
    conn_reg.register(Arc::new(pensieve_datasources::jira::JiraDataSource));
    conn_reg.register(Arc::new(pensieve_datasources::confluence::ConfluenceDataSource));
    conn_reg.register(Arc::new(pensieve_datasources::msfabric::MsFabricDataSource));
    let conn_registry = Arc::new(conn_reg);

    // RowSink: bridges data source JSON rows → arrow coercion → WritePath.
    //
    // The sink auto-creates the target table (ensure_table) and promotes
    // unknown JSON properties to columns (evolve_schema_for_records) before
    // coercing + ingesting. Both helpers are no-ops on the happy path when the
    // table already exists with the right shape, so the legacy Prometheus path
    // is unaffected.
    let conn_sink: pensieve_datasources::runner::RowSink = {
        let catalog_for_sink = catalog.clone();
        let write_path_for_sink = write_path.clone();
        Arc::new(
            move |db: String, tbl: String, rows: Vec<serde_json::Value>, idem: Option<String>| {
                let catalog = catalog_for_sink.clone();
                let write_path = write_path_for_sink.clone();
                Box::pin(async move {
                    // Auto-create the database + table if they don't exist yet
                    // (idempotent — single SQL lookup on the happy path).
                    let table = ensure_table(catalog.as_ref(), &db, &tbl)
                        .await
                        .map_err(|e| anyhow::anyhow!("ensure_table: {e}"))?;
                    // Promote unknown JSON property names to nullable Utf8
                    // columns (capped at 32 new columns per request).
                    let table = evolve_schema_for_records(catalog.as_ref(), &db, table, &rows)
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
        )
    };

    // GraphRegisterFn: registers (or idempotently re-registers) a
    // property-graph binding in the catalog after multi-table ingest.
    let graph_register: pensieve_datasources::runner::GraphRegisterFn = {
        let catalog_for_graph = catalog.clone();
        Arc::new(move |db: String, hint: pensieve_datasources::GraphHint| {
            let catalog = catalog_for_graph.clone();
            Box::pin(async move {
                let spec =
                    GraphSpec::with_defaults(hint.node_table.clone(), hint.edge_table.clone());
                match catalog.create_graph(&db, &hint.graph_name, spec).await {
                    Ok(_) => {}
                    Err(e) => {
                        // Swallow "already exists" — the graph was registered
                        // on a previous tick; nothing to do.
                        let msg = e.to_string();
                        if msg.contains("already exists")
                            || msg.contains("duplicate")
                            || msg.contains("23505")
                        {
                            // idempotent — ignore
                        } else {
                            return Err(anyhow::anyhow!("create_graph: {msg}"));
                        }
                    }
                }
                Ok(())
            })
        })
    };

    let health_router = pensieve_server::health_router();
    let metrics_router = pensieve_server::metrics::router();
    let pg_ds_catalog: Arc<dyn pensieve_datasources::catalog_trait::DataSourceCatalog> =
        Arc::new(PgDataSourceCatalog::from_pg_catalog(&pg_catalog));
    let datasource_admin_router = pensieve_server::datasource_admin_router(DataSourceAdminState {
        catalog: pg_ds_catalog.clone(),
        registry: conn_registry.clone(),
    })
    .layer(axum::middleware::from_fn_with_state(
        AuthLayerState {
            backend: backend.clone(),
            required: Role::Write,
        },
        require_role_middleware,
    ));

    // Credentials router — reuses the cred_store built above (alongside the
    // AgentState wiring). Kept here so the router-mounting block stays
    // logically together.
    let credentials_router = pensieve_server::credentials_handler::router(
        pensieve_server::credentials_handler::CredentialsState {
            store: cred_store.clone(),
        },
    )
    .layer(axum::middleware::from_fn_with_state(
        AuthLayerState {
            backend: backend.clone(),
            required: Role::Write,
        },
        require_role_middleware,
    ));

    // OAuth2 connect flow — start/poll are authenticated (Role::Write); the
    // callback the identity provider redirects to is unauthenticated (it carries
    // no bearer) and trusts only the single-use `state` token. Client apps come
    // from operator env (PENSIEVE_OAUTH_<PROVIDER>_CLIENT_ID/_SECRET) or per-tenant
    // bring-your-own creds; tokens land in the encrypted credentials store.
    let oauth_redirect_base = std::env::var("PENSIEVE_OAUTH_REDIRECT_BASE")
        .unwrap_or_else(|_| "http://localhost:8080".to_string());
    let oauth_ui_return_base =
        std::env::var("PENSIEVE_OAUTH_UI_RETURN_BASE").unwrap_or_else(|_| oauth_redirect_base.clone());
    let oauth_state = pensieve_server::OAuthState::new(
        pg_pool.clone(),
        cred_store.clone(),
        crypto.clone(),
        oauth_redirect_base,
        oauth_ui_return_base,
    );
    let oauth_authed_router = pensieve_server::oauth_authed_router(oauth_state.clone()).layer(
        axum::middleware::from_fn_with_state(
            AuthLayerState {
                backend: backend.clone(),
                required: Role::Write,
            },
            require_role_middleware,
        ),
    );
    let oauth_callback_router = pensieve_server::oauth_callback_router(oauth_state);

    // GitHub data source — repos picker endpoint (Role::Write, behind auth).
    let github_repos_router = {
        let secrets: std::sync::Arc<dyn pensieve_datasources::secrets::SecretStore> =
            Arc::new(pensieve_datasources::secrets::EnvSecretStore);
        pensieve_datasources::github::github_repos_router(secrets).layer(
            axum::middleware::from_fn_with_state(
                AuthLayerState {
                    backend: backend.clone(),
                    required: Role::Write,
                },
                require_role_middleware,
            ),
        )
    };
    let dashboards_write_router = pensieve_server::dashboards_write_router(catalog.clone()).layer(
        axum::middleware::from_fn_with_state(
            AuthLayerState {
                backend: backend.clone(),
                required: Role::Write,
            },
            require_role_middleware,
        ),
    );
    // Saved-views CRUD (write side) — list endpoint is on the read router.
    let discover_views_write_router = pensieve_server::discover_views_write_router(
        std::sync::Arc::new(pg_pool.clone()),
    )
    .layer(axum::middleware::from_fn_with_state(
        AuthLayerState {
            backend: backend.clone(),
            required: Role::Write,
        },
        require_role_middleware,
    ));
    let cleanup_write_router = pensieve_server::cleanup_write_router(catalog.clone()).layer(
        axum::middleware::from_fn_with_state(
            AuthLayerState {
                backend: backend.clone(),
                required: Role::Write,
            },
            require_role_middleware,
        ),
    );
    let compact_write_router = pensieve_server::compact_write_router(catalog.clone()).layer(
        axum::middleware::from_fn_with_state(
            AuthLayerState {
                backend: backend.clone(),
                required: Role::Write,
            },
            require_role_middleware,
        ),
    );
    // Feature discovery — everything is on in server mode (Role::Read).
    let capabilities_router = pensieve_server::capabilities::router(
        pensieve_server::capabilities::Capabilities::SERVER,
    )
    .layer(axum::middleware::from_fn_with_state(
        AuthLayerState {
            backend: backend.clone(),
            required: Role::Read,
        },
        require_role_middleware,
    ));
    // Auth routes: login is unauthenticated; me/logout are authenticated.
    let auth_login_router = pensieve_server::auth_handler::auth_login_router(catalog.clone());
    let auth_session_router = pensieve_server::auth_handler::auth_session_router(catalog.clone())
        .layer(axum::middleware::from_fn_with_state(
            AuthLayerState {
                backend: backend.clone(),
                required: Role::Read,
            },
            require_role_middleware,
        ));
    // Admin user management (/v1/admin/users) — gated at Role::Admin.
    let admin_users_router = pensieve_server::admin_handler::admin_users_router(catalog.clone()).layer(
        axum::middleware::from_fn_with_state(
            AuthLayerState {
                backend: backend.clone(),
                required: Role::Admin,
            },
            require_role_middleware,
        ),
    );
    // Worker/job fabric — the distributed context-engine control plane.
    // Worker-facing endpoints authenticate with worker tokens (kyw_…) via
    // their own middleware; the operator surface rides the regular bearer
    // middleware at Role::Write.
    let fabric_store = std::sync::Arc::new(pensieve_catalog::PgFabricStore::new(pg_pool.clone()));
    let fabric_state = pensieve_server::fabric_handler::FabricState::new(fabric_store.clone(), None);
    let fabric_worker_router = pensieve_server::fabric_handler::worker_router(fabric_state.clone());
    let fabric_admin_router = pensieve_server::fabric_handler::admin_router(fabric_state.clone())
        .layer(axum::middleware::from_fn_with_state(
            AuthLayerState {
                backend: backend.clone(),
                required: Role::Write,
            },
            require_role_middleware,
        ));
    // Live-tail WebSocket — mounted WITHOUT auth middleware; the session
    // authenticates via its first message (browsers can't send WS headers).
    let live_router = pensieve_server::discover::live::explore_live_router(
        QueryState {
            federation: Some(federation.clone()),
            catalog: catalog.clone(),
            format: format.clone(),
            schema_cache: schema_cache.clone(),
            node_id: Some(lease.node_id),
            pg_pool: Some(std::sync::Arc::new(pg_pool.clone())),
            layout_cache: std::sync::Arc::new(pensieve_server::graph_layout_cache::LayoutCache::new()),
        },
        backend.clone(),
        Some(ingest_events),
    );
    // Live consumers WebSocket — backfills recent memory spans from the OTLP
    // self-trace database (PENSIEVE_OTLP_DATABASE), then tails the consumer bus.
    let consumers_router = pensieve_server::discover::consumers_live::consumers_live_router(
        QueryState {
            federation: Some(federation.clone()),
            catalog: catalog.clone(),
            format: format.clone(),
            schema_cache: schema_cache.clone(),
            node_id: Some(lease.node_id),
            pg_pool: Some(std::sync::Arc::new(pg_pool.clone())),
            layout_cache: std::sync::Arc::new(pensieve_server::graph_layout_cache::LayoutCache::new()),
        },
        backend.clone(),
        Some(consumer_events),
        cli.otlp_database.clone(),
    );
    // Brain repos — /v1/brain management + /git/<name>.git smart HTTP. The
    // bare repos live on a mounted volume (PENSIEVE_BRAIN_DIR); no git binary or
    // unwritable dir ⇒ the surface stays mounted but answers 503 / reports
    // git_available:false.
    let brain_state = {
        let brain_dir = std::path::PathBuf::from(
            std::env::var("PENSIEVE_BRAIN_DIR").unwrap_or_else(|_| "/var/lib/pensieve/brain".to_string()),
        );
        let mut brain_git = pensieve_brain::gitbin::GitBin::detect().await.map(std::sync::Arc::new);
        if brain_git.is_some() {
            if let Err(e) = std::fs::create_dir_all(&brain_dir) {
                tracing::warn!(dir = %brain_dir.display(), error = %e,
                    "PENSIEVE_BRAIN_DIR not writable — brain repos disabled");
                brain_git = None;
            }
        }
        pensieve_server::brain::BrainState::new(
            std::sync::Arc::new(pensieve_server::brain::pg_registry::PgBrainRegistry::new(
                pg_pool.clone(),
                pensieve_core::tenant::DEFAULT_TENANT,
            )),
            brain_git,
            brain_dir,
            agent_state.clone(),
        )
    };
    let brain_mgmt_router = pensieve_server::brain::routes::brain_router(brain_state.clone()).layer(
        axum::middleware::from_fn_with_state(
            AuthLayerState {
                backend: backend.clone(),
                required: Role::Read,
            },
            require_role_middleware,
        ),
    );
    let brain_git_router =
        pensieve_server::brain::git_http::git_http_router(brain_state.clone()).layer(
            axum::middleware::from_fn_with_state(
                AuthLayerState {
                    backend: backend.clone(),
                    required: Role::Read,
                },
                pensieve_server::auth::require_git_auth_middleware,
            ),
        );

    let app = ingest_router
        .merge(query_router)
        .merge(mcp_router)
        .merge(capabilities_router)
        .merge(admin_users_router)
        .merge(dashboards_write_router)
        .merge(discover_views_write_router)
        .merge(cleanup_write_router)
        .merge(compact_write_router)
        .merge(health_router)
        .merge(metrics_router)
        .merge(datasource_admin_router)
        .merge(credentials_router)
        .merge(oauth_authed_router)
        .merge(oauth_callback_router)
        .merge(auth_login_router)
        .merge(auth_session_router)
        .merge(fabric_worker_router)
        .merge(fabric_admin_router)
        .merge(live_router)
        .merge(consumers_router)
        .merge(brain_mgmt_router)
        .merge(brain_git_router);

    let app = app.merge(github_repos_router);
    #[cfg(feature = "web-ui")]
    let app = app.merge(pensieve_server::web_ui::router());
    // Re-assert the SPA fallback on the final app, un-layered: merging
    // auth-layered routers can leave the inherited fallback wrapped by the
    // auth middleware, which would 401 the login page and every asset the
    // moment auth is enabled (supabase/session backends).
    #[cfg(feature = "web-ui")]
    let app = app.fallback(pensieve_server::web_ui::serve_spa_fallback);

    // Expose Flight over gRPC-web at /flight/* so browsers can query via Arrow Flight.
    // Auth is enforced the same way as /v1/* (Bearer token, Role::Read required).
    #[cfg(feature = "web-ui")]
    let app = {
        let flight_router = pensieve_server::flight_web_router(pensieve_server::QueryState {
            federation: Some(federation.clone()),
            catalog: catalog.clone(),
            format: format.clone(),
            schema_cache: schema_cache.clone(),
            node_id: Some(lease.node_id),
            pg_pool: Some(std::sync::Arc::new(pg_pool.clone())),
            layout_cache: std::sync::Arc::new(pensieve_server::graph_layout_cache::LayoutCache::new()),
        })
        // Fail closed: database-scoped tokens cannot use Flight (tickets
        // address databases internally, bypassing per-handler scope
        // checks). Layered before auth so it runs AFTER auth populates
        // the Principal extension (axum layers run outermost-last-added).
        .layer(axum::middleware::from_fn(
            pensieve_server::scoped_token_guard_middleware,
        ))
        // Realm-scoped tokens likewise cannot use Flight (no realm model).
        .layer(axum::middleware::from_fn(
            pensieve_server::realm_token_guard_middleware,
        ))
        .layer(axum::middleware::from_fn_with_state(
            AuthLayerState {
                backend: backend.clone(),
                required: pensieve_server::auth::Role::Read,
            },
            pensieve_server::auth::require_role_middleware,
        ));
        app.merge(flight_router)
    };

    // 5. Spawn background workers. Each has an independent shutdown watch so
    //    a panic in one worker doesn't starve the others or the HTTP server.
    let mut worker = CompactionWorker::new(catalog.clone(), format.clone(), lease.node_id);
    let mut scheduler = CompactionScheduler::new(catalog.clone());
    // Allow env overrides for aggressive testing.
    if let Ok(ms) = std::env::var("PENSIEVE_COMPACTION_IDLE_SLEEP_MS")
        .and_then(|v| v.parse::<u64>().map_err(|_| std::env::VarError::NotPresent))
    {
        worker.idle_sleep = std::time::Duration::from_millis(ms);
    }
    if let Ok(s) = std::env::var("PENSIEVE_COMPACTION_POLL_SECS")
        .and_then(|v| v.parse::<u64>().map_err(|_| std::env::VarError::NotPresent))
    {
        scheduler.poll_interval = std::time::Duration::from_secs(s);
    }
    if let Ok(n) = std::env::var("PENSIEVE_COMPACTION_MIN_EXTENTS")
        .and_then(|v| v.parse::<i64>().map_err(|_| std::env::VarError::NotPresent))
    {
        scheduler.min_extents_to_compact = n;
    }
    // Compaction is heavy background work — only on job-running roles
    // (all_in_one / worker). Stateless query/ingest nodes skip it (HPA-safe).
    let (worker_handle, scheduler_handle) = if rc.run_jobs {
        let worker_rx = shutdown_tx.subscribe();
        let scheduler_rx = shutdown_tx.subscribe();
        (
            Some(tokio::spawn(worker.run(async move {
                let mut rx = worker_rx;
                let _ = rx.recv().await;
            }))),
            Some(tokio::spawn(scheduler.run(async move {
                let mut rx = scheduler_rx;
                let _ = rx.recv().await;
            }))),
        )
    } else {
        (None, None)
    };

    // Index-build activation scheduler: enqueues ivf_rabitq build jobs for
    // un-indexed vector columns so ANN sidecars actually get created. Job
    // production is background work — gated on run_jobs (off for query/committer).
    let index_scheduler_handle = if rc.run_jobs {
        let index_scheduler = pensieve_compaction::IndexScheduler::new(catalog.clone());
        Some(tokio::spawn(index_scheduler.run(shutdown_tx.subscribe())))
    } else {
        None
    };

    // Graph-snapshot activation scheduler: refreshes persistent CSR topology
    // snapshots (S3.2) after a graph's edges change, so deep-subgraph queries on
    // large graphs serve from the snapshot instead of a per-hop scan. Query nodes
    // read snapshots but never build them — gated on run_jobs.
    let graph_snapshot_scheduler_handle = if rc.run_jobs {
        let graph_snapshot_scheduler =
            pensieve_server::graph_snapshot_sched::GraphSnapshotScheduler::new(
                catalog.clone(),
                format.clone(),
            );
        Some(tokio::spawn(
            graph_snapshot_scheduler.run(shutdown_tx.subscribe()),
        ))
    } else {
        None
    };

    // Per-tenant quota cache refresh (S2.6): keeps the in-RAM tenant_quotas
    // snapshot fresh for the admission limiter. NOT gated on run_jobs — the
    // limiter runs in the query/agent HTTP handlers on every serving node, so the
    // cache must be populated there too. Cheap + read-only (one catalog list per
    // PENSIEVE_QUOTA_REFRESH_SECS); a no-op when the table is empty (the default).
    let _quota_refresh_handle = pensieve_server::quota_cache::spawn_refresh(catalog.clone());

    // Data-lifecycle workers (retention soft-delete, physical-delete GC, artifact
    // GC) are background jobs — gated on run_jobs. Stateless query nodes and the
    // committer never sweep; only all_in_one / worker roles do. GC is the one way
    // this architecture can lose data, so it stays on a job-running role only.
    let retention_handle = if rc.run_jobs {
        let mut retention = RetentionSweeper::new(catalog.clone());
        if let Ok(s) = std::env::var("PENSIEVE_RETENTION_POLL_SECS")
            .and_then(|v| v.parse::<u64>().map_err(|_| std::env::VarError::NotPresent))
        {
            retention.poll_interval = std::time::Duration::from_secs(s);
        }
        let retention_rx = shutdown_tx.subscribe();
        Some(tokio::spawn(retention.run(async move {
            let mut rx = retention_rx;
            let _ = rx.recv().await;
        })))
    } else {
        None
    };

    // Physical-delete worker (remove bytes after grace).
    let gc_handle = if rc.run_jobs {
        let mut gc = PhysicalDeleteWorker::new(catalog.clone(), store.clone());
        if let Ok(s) = std::env::var("PENSIEVE_PHYSICAL_GC_POLL_SECS")
            .and_then(|v| v.parse::<u64>().map_err(|_| std::env::VarError::NotPresent))
        {
            gc.poll_interval = std::time::Duration::from_secs(s);
        }
        if let Ok(s) = std::env::var("PENSIEVE_PHYSICAL_GC_GRACE_SECS")
            .and_then(|v| v.parse::<i64>().map_err(|_| std::env::VarError::NotPresent))
        {
            gc.grace_period = chrono::Duration::seconds(s);
        }
        let gc_rx = shutdown_tx.subscribe();
        Some(tokio::spawn(gc.run(async move {
            let mut rx = gc_rx;
            let _ = rx.recv().await;
        })))
    } else {
        None
    };

    // Artifact-retention worker — sweeps expired object-store artifacts (CI job
    // logs, contributed files, fs-watch snapshots) that live outside the
    // columnar extents, on the same soft-delete + grace pattern.
    let artifact_gc_handle = if rc.run_jobs {
        let mut artifact_gc = ArtifactRetentionWorker::new(catalog.clone(), store.clone());
        if let Ok(s) = std::env::var("PENSIEVE_ARTIFACT_GC_POLL_SECS")
            .and_then(|v| v.parse::<u64>().map_err(|_| std::env::VarError::NotPresent))
        {
            artifact_gc.poll_interval = std::time::Duration::from_secs(s);
        }
        if let Ok(s) = std::env::var("PENSIEVE_ARTIFACT_GC_GRACE_SECS")
            .and_then(|v| v.parse::<i64>().map_err(|_| std::env::VarError::NotPresent))
        {
            artifact_gc.grace_period = chrono::Duration::seconds(s);
        }
        let artifact_gc_rx = shutdown_tx.subscribe();
        Some(tokio::spawn(artifact_gc.run(async move {
            let mut rx = artifact_gc_rx;
            let _ = rx.recv().await;
        })))
    } else {
        None
    };

    // Catch-all artifact-graph sync — materializes graph nodes for artifacts
    // that have no producer-graph node (object-store blobs, contributed files,
    // fs-watch snapshots). Wired here (not inside ArtifactRetentionWorker)
    // because that worker lives in pensieve-compaction and holds no SegmentFormat
    // handle; this startup site already has `catalog` + `format` in scope, so
    // wiring here is the least-invasive correct seam. Runs an immediate startup
    // backfill, then re-syncs on the artifact-GC cadence. Postgres-only: a safe
    // no-op (Ok(0)) under `pensieve local` (sqlite has no artifacts catalog).
    // Background sync/index work — gated on run_jobs.
    let artifact_graph_handle = if rc.run_jobs {
        let artifact_graph_poll = std::env::var("PENSIEVE_ARTIFACT_GC_POLL_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(std::time::Duration::from_secs)
            .unwrap_or_else(|| std::time::Duration::from_secs(300));
        let artifact_graph_catalog = catalog.clone();
        let artifact_graph_format = format.clone();
        // Object-store handle for the content indexer: artifact blobs are fetched,
        // chunked + embedded into `artifacts.artifact_chunks` on the same cadence.
        let artifact_graph_store = store.clone();
        let mut artifact_graph_rx = shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            // Startup backfill for all tenants.
            if let Err(e) =
                pensieve_server::agent::artifact_graph_sync::sync_artifact_nodes_all_tenants(
                    artifact_graph_catalog.clone(),
                    artifact_graph_format.clone(),
                    Some(artifact_graph_store.clone()),
                )
                .await
            {
                warn!(error = %e, "artifact-graph backfill failed");
            }
            loop {
                tokio::select! {
                    biased;
                    _ = artifact_graph_rx.recv() => return,
                    _ = tokio::time::sleep(artifact_graph_poll) => {
                        if let Err(e) = pensieve_server::agent::artifact_graph_sync::sync_artifact_nodes_all_tenants(
                            artifact_graph_catalog.clone(),
                            artifact_graph_format.clone(),
                            Some(artifact_graph_store.clone()),
                        )
                        .await
                        {
                            warn!(error = %e, "artifact-graph sync failed");
                        }
                    }
                }
            }
        }))
    } else {
        None
    };

    // Memory consolidation ("dreaming") pipeline — periodically distills new
    // conversation-firehose activity into durable summary memories and records
    // each run in `memory_pipeline_runs`. On by default; set
    // PENSIEVE_MEMORY_CONSOLIDATION=0 to disable.
    let memory_consolidator_handle = if rc.run_jobs
        && std::env::var("PENSIEVE_MEMORY_CONSOLIDATION")
            .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
            .unwrap_or(true)
    {
        let mut consolidator = pensieve_server::agent::MemoryConsolidator::new(
            pensieve_server::agent::SharedToolCtx {
                realm_scope: Default::default(),
                consumer_sink: None,
                federation: Some(federation.clone()),
                catalog: catalog.clone(),
                format: format.clone(),
                pool: Some(pg_pool.clone()),
                memory: memory.clone(),
                hitl: None,
                memory_settings_path: None,
            },
            pg_pool.clone(),
            pensieve_core::tenant::DEFAULT_TENANT,
        )
        // Reuse the configured agent engine for LLM extraction + conflict
        // resolution; falls back to deterministic summaries when the engine is
        // unset or is the claude_cli kind (which can't run through adk-rust).
        .with_engine(agent_state.clone());
        if let Ok(s) = std::env::var("PENSIEVE_MEMORY_CONSOLIDATION_POLL_SECS")
            .and_then(|v| v.parse::<u64>().map_err(|_| std::env::VarError::NotPresent))
        {
            consolidator.poll_interval = std::time::Duration::from_secs(s);
        }
        let cons_rx = shutdown_tx.subscribe();
        info!("memory consolidation pipeline enabled");
        Some(tokio::spawn(consolidator.run(async move {
            let mut rx = cons_rx;
            let _ = rx.recv().await;
        })))
    } else {
        None
    };

    // CI failure-correlation ("dreaming") pipeline — scans the github_job_logs
    // failure signal for recurring failures and writes durable incident
    // memories. On by default; set PENSIEVE_CI_CORRELATE=0 to disable.
    let ci_correlate_handle = if rc.run_jobs
        && std::env::var("PENSIEVE_CI_CORRELATE")
            .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
            .unwrap_or(true)
    {
        let mut correlator = pensieve_server::agent::CiCorrelator::new(
            pensieve_server::agent::SharedToolCtx {
                realm_scope: Default::default(),
                consumer_sink: None,
                federation: Some(federation.clone()),
                catalog: catalog.clone(),
                format: format.clone(),
                pool: Some(pg_pool.clone()),
                memory: memory.clone(),
                hitl: None,
                memory_settings_path: None,
            },
            pg_pool.clone(),
            pensieve_core::tenant::DEFAULT_TENANT,
        );
        if let Ok(s) = std::env::var("PENSIEVE_CI_CORRELATE_POLL_SECS")
            .and_then(|v| v.parse::<u64>().map_err(|_| std::env::VarError::NotPresent))
        {
            correlator.poll_interval = std::time::Duration::from_secs(s);
        }
        let ci_rx = shutdown_tx.subscribe();
        info!("ci-correlate pipeline enabled");
        Some(tokio::spawn(correlator.run(async move {
            let mut rx = ci_rx;
            let _ = rx.recv().await;
        })))
    } else {
        None
    };

    // File-candidate promotion ("dreaming") pipeline — resolves contributed /
    // scraped candidate File nodes to their live upstream repo File nodes and
    // stitches them with cross-graph SAME_AS edges. On by default; set
    // PENSIEVE_FILE_PROMOTE=0 to disable.
    let file_promote_handle = if rc.run_jobs
        && std::env::var("PENSIEVE_FILE_PROMOTE")
            .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
            .unwrap_or(true)
    {
        let mut promoter = pensieve_server::agent::FilePromoter::new(
            pensieve_server::agent::SharedToolCtx {
                realm_scope: Default::default(),
                consumer_sink: None,
                federation: Some(federation.clone()),
                catalog: catalog.clone(),
                format: format.clone(),
                pool: Some(pg_pool.clone()),
                memory: memory.clone(),
                hitl: None,
                memory_settings_path: None,
            },
            pg_pool.clone(),
            pensieve_core::tenant::DEFAULT_TENANT,
        );
        if let Ok(s) = std::env::var("PENSIEVE_FILE_PROMOTE_POLL_SECS")
            .and_then(|v| v.parse::<u64>().map_err(|_| std::env::VarError::NotPresent))
        {
            promoter.poll_interval = std::time::Duration::from_secs(s);
        }
        let fp_rx = shutdown_tx.subscribe();
        info!("file-promote pipeline enabled");
        Some(tokio::spawn(promoter.run(async move {
            let mut rx = fp_rx;
            let _ = rx.recv().await;
        })))
    } else {
        None
    };

    // File-drop watcher — polls an object-store prefix for NDJSON files.
    // Disabled by default; set PENSIEVE_FILEDROP_ENABLED=1 to turn on.
    let filedrop_handle = if std::env::var("PENSIEVE_FILEDROP_ENABLED")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
    {
        let fd_config = FiledropConfig::from_env();
        let mut watcher = FiledropWatcher::new(
            catalog.clone(),
            store.clone(),
            write_path.clone(),
            fd_config.clone(),
        );
        // Register in the watcher registry and heartbeat per scan via the
        // scan hook. Best-effort: a registry failure must not prevent the
        // watcher from running — warn and run unregistered.
        let (host, node_id, user) = pensieve_datasources::watchers::node_identity();
        match pensieve_datasources::watchers::WatcherRegistry::register(
            pg_catalog.pool(),
            "filedrop",
            &host,
            &node_id,
            &user,
            serde_json::json!({
                "prefixes": fd_config.prefixes,
                "poll_secs": fd_config.poll_interval.as_secs(),
                "delete_after_ingest": fd_config.delete_after_ingest,
            }),
        )
        .await
        {
            Ok(reg) => {
                watcher = watcher.with_scan_hook(std::sync::Arc::new(move |scan| {
                    // Capture timestamp synchronously so it reflects scan completion,
                    // not whenever the executor picks up the task.
                    let at = chrono::Utc::now();
                    let reg = reg.clone();
                    // one-shot UPDATE; completes fast, no retry
                    tokio::spawn(async move {
                        // FiledropScan → ScanStats: add registry-layer fields (at, detail)
                        reg.heartbeat(Some(&pensieve_datasources::watchers::ScanStats {
                            seen: scan.seen,
                            processed: scan.processed,
                            errors: scan.errors,
                            duration_ms: scan.duration_ms,
                            at,
                            detail: None,
                        }))
                        .await;
                    });
                }));
            }
            Err(e) => {
                warn!(error = %e, "watcher registry unavailable; filedrop runs unregistered");
            }
        }
        let filedrop_rx = shutdown_tx.subscribe();
        info!("file-drop watcher enabled");
        Some(tokio::spawn(watcher.run(async move {
            let mut rx = filedrop_rx;
            let _ = rx.recv().await;
        })))
    } else {
        None
    };

    // Kafka consumer — on when PENSIEVE_KAFKA_ENABLED=1 and PENSIEVE_KAFKA_TOPICS is non-empty.
    let kafka_handle = match KafkaConsumerConfig::from_env() {
        Some(config) => {
            let worker = KafkaConsumerWorker::new(catalog.clone(), write_path.clone(), config);
            let kafka_rx = shutdown_tx.subscribe();
            info!("kafka consumer enabled");
            Some(tokio::spawn(worker.run(async move {
                let mut rx = kafka_rx;
                let _ = rx.recv().await;
            })))
        }
        None => None,
    };

    // DataSource scheduler — enqueues datasource_sync jobs on the worker fabric.
    // Job production is background work — gated on run_jobs.
    let conn_sched_handle = if rc.run_jobs {
        let conn_sched = DataSourceScheduler::new(pg_ds_catalog.clone());
        let conn_sched_rx = shutdown_tx.subscribe();
        Some(tokio::spawn(conn_sched.run(async move {
            let mut rx = conn_sched_rx;
            let _ = rx.recv().await;
        })))
    } else {
        None
    };

    // Embedded fabric worker — the server's in-process compute identity. It
    // registers in the workers table (visible in GET /v1/workers next to any
    // remote daemons) and runs N job-runner loops claiming fabric jobs.
    // On job-running roles, run N fabric runner loops; on stateless query /
    // committer roles, register the worker identity with zero capacity so it
    // claims no jobs (the `for _ in 0..0` below spawns no runners). Keeping the
    // registration preserves embedded_worker_id for the shutdown-offline call
    // and the DreamingExecutor.
    let n_fabric_workers = if rc.run_jobs {
        std::env::var("PENSIEVE_FABRIC_WORKERS")
            .or_else(|_| std::env::var("PENSIEVE_DATA_SOURCE_WORKERS"))
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(4)
    } else {
        0
    };
    // Shared object-store artifact capability for data sources that persist
    // full-file blobs (e.g. GitHub Actions job logs) — threaded into the fabric
    // data source executor via `tick_deps` below.
    let artifact_store: std::sync::Arc<dyn pensieve_datasources::artifacts::ArtifactStore> =
        std::sync::Arc::new(pensieve_datasources::artifacts::ObjectArtifactStore::new(
            store.clone(),
            pg_catalog.clone(),
        ));
    let fabric_lease_secs: i64 = std::env::var("PENSIEVE_FABRIC_LEASE_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300);
    let mut embedded_caps: Vec<String> = ["data_source", "dreaming", "llm"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    // Brain exports need the git binary + the brain volume on this worker.
    if brain_state.git.is_some() {
        embedded_caps.push("brain".to_string());
    }
    // Advertise the Claude CLI when the binary is present — dreaming jobs
    // running on the ClaudeCli engine require it.
    if pensieve_server::agent::engine::claude_cli::locate_binary().is_some() {
        embedded_caps.push("claude-cli".to_string());
    }
    let embedded_host = hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .unwrap_or_else(|| "localhost".to_string());
    let embedded_worker_id = fabric_store
        .upsert_embedded_worker(
            pensieve_core::tenant::DEFAULT_TENANT,
            &pensieve_core::fabric::WorkerRegistration {
                name: format!("embedded@{embedded_host}"),
                kind: pensieve_core::fabric::WorkerKind::Embedded,
                hostname: Some(embedded_host.clone()),
                capabilities: embedded_caps.clone(),
                labels: serde_json::json!({}),
                sources: serde_json::json!({}),
                max_concurrent: n_fabric_workers as i32,
                version: Some(env!("CARGO_PKG_VERSION").to_string()),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("registering embedded worker: {e}"))?;

    let tick_deps = pensieve_datasources::runner::DataSourceTickDeps {
        control: Arc::new(pensieve_datasources::runner::PgDataSourceControl::new(
            pg_pool.clone(),
        )),
        registry: conn_registry.clone(),
        sink: conn_sink.clone(),
        graph_register: graph_register.clone(),
        secrets: Arc::new(EnvSecretStore),
        credentials: cred_store.clone(),
        oauth: Some(pensieve_datasources::oauth::OAuthRuntime {
            pool: pg_pool.clone(),
            crypto: crypto.clone(),
        }),
        artifacts: Some(artifact_store.clone()),
        catalog: Some(catalog.clone()),
    };
    let mut exec_registry = pensieve_jobs::ExecutorRegistry::new();
    exec_registry.register(Arc::new(
        pensieve_jobs::datasource_sync::DataSourceSyncExecutor::new(tick_deps),
    ));
    exec_registry.register(Arc::new(DreamingExecutor {
        state: agent_state.clone(),
        worker_id: embedded_worker_id,
    }));
    exec_registry.register(Arc::new(BrainExportExecutor {
        state: brain_state.clone(),
    }));
    // Index-sidecar build jobs: S1.1 ivf_rabitq ANN + S1.4 tantivy_fts BM25.
    // A job for an unregistered kind fails terminally with a clear
    // "no builder registered" error.
    let mut sidecar_builders: std::collections::HashMap<
        pensieve_core::index_sidecar::SidecarKind,
        Arc<dyn pensieve_core::index_sidecar::SidecarBuilder>,
    > = std::collections::HashMap::new();
    sidecar_builders.insert(
        pensieve_core::index_sidecar::SidecarKind::IvfRabitq,
        Arc::new(pensieve_index_vector::IvfRabitqBuilder::new()),
    );
    sidecar_builders.insert(
        pensieve_core::index_sidecar::SidecarKind::TantivyFts,
        Arc::new(pensieve_index_fts::TantivyFtsBuilder::new()),
    );
    exec_registry.register(Arc::new(pensieve_jobs::index_build::IndexBuildExecutor::new(
        catalog.clone(),
        format.clone(),
        store.clone(),
        sidecar_builders,
    )));
    // Async embedding backfill (S1.5): fills a configured text column's
    // embeddings into a vector column by rewriting extents, with a content-hash
    // cache. Injects the process-wide embedding backend (the same `OnceCell`
    // memory uses). The executor picks its embedder by asserting
    // `embedder.id() == payload.model_id` — single-model for now; a per-model
    // registry is a later step (the scheduler that enqueues these jobs, out of
    // S1.5 scope, sets `model_id` from the same backend's id). If no backend is
    // available (provider feature off), the executor is simply not registered;
    // an enqueued `embed_backfill` job then fails terminally with the fabric's
    // standard "no executor for kind" error.
    match pensieve_memory::shared_embedding().await {
        Ok(embedder) => {
            exec_registry.register(Arc::new(
                pensieve_jobs::embed_backfill::EmbedBackfillExecutor::new(
                    catalog.clone(),
                    format.clone(),
                    embedder,
                ),
            ));
        }
        Err(e) => {
            warn!(error = %e, "embedding backend unavailable; embed_backfill executor not registered");
        }
    }
    // S1.3 global ANN centroid tree: the ann_maintain executor (re)builds the
    // table-wide tree from per-extent IVF sidecars. The IndexScheduler enqueues
    // ann_maintain when the sidecar-bearing extent set changes (server mode).
    exec_registry.register(Arc::new(pensieve_jobs::ann_maintain::AnnMaintainExecutor::new(
        catalog.clone(),
        store.clone(),
    )));
    let mut fabric_runner_handles = Vec::with_capacity(n_fabric_workers);
    for _ in 0..n_fabric_workers {
        let queue = Arc::new(pensieve_jobs::PgQueue::new(
            fabric_store.clone(),
            embedded_worker_id,
            None, // all-tenant: the embedded worker serves the whole deployment
            embedded_caps.clone(),
            fabric_lease_secs,
        ));
        let runner = pensieve_jobs::JobRunner::new(queue, exec_registry.clone(), fabric_lease_secs);
        let runner_rx = shutdown_tx.subscribe();
        fabric_runner_handles.push(tokio::spawn(async move {
            let mut rx = runner_rx;
            runner
                .run(async move {
                    let _ = rx.recv().await;
                })
                .await;
        }));
    }

    // Fabric housekeeping: embedded-worker heartbeat + stale sweep (requeue
    // expired leases, fail exhausted ones, offline silent workers).
    let fabric_sweep_secs: u64 = std::env::var("PENSIEVE_FABRIC_SWEEP_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30);
    let fabric_offline_secs: i64 = std::env::var("PENSIEVE_FABRIC_OFFLINE_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(90);
    let fabric_housekeeping = {
        let store = fabric_store.clone();
        let mut rx = shutdown_tx.subscribe();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(fabric_sweep_secs));
            loop {
                tokio::select! {
                    biased;
                    _ = rx.recv() => return,
                    _ = tick.tick() => {
                        if let Err(e) = store
                            .touch_heartbeat(embedded_worker_id, &pensieve_core::fabric::Heartbeat::default())
                            .await
                        {
                            tracing::warn!(error = %e, "embedded worker heartbeat failed");
                        }
                        match store.sweep_stale(fabric_offline_secs).await {
                            Ok((0, 0, 0)) => {}
                            Ok((requeued, failed, offlined)) => tracing::info!(
                                requeued, failed, offlined, "fabric sweep"
                            ),
                            Err(e) => tracing::warn!(error = %e, "fabric sweep failed"),
                        }
                    }
                }
            }
        })
    };
    info!(
        workers = n_fabric_workers,
        worker_id = %embedded_worker_id,
        "data source scheduler + embedded fabric worker started"
    );

    // Dreaming scheduler — enqueues agentic memory-housekeeping jobs on the
    // fabric when enabled in memory settings (OFF by default). Job production is
    // background work — gated on run_jobs.
    let dreaming_sched_handle = if rc.run_jobs {
        let dreaming_sched = pensieve_server::agent::dreaming::DreamingScheduler::new(
            agent_state.clone(),
            fabric_store.clone(),
        );
        let dreaming_sched_rx = shutdown_tx.subscribe();
        Some(tokio::spawn(dreaming_sched.run(async move {
            let mut rx = dreaming_sched_rx;
            let _ = rx.recv().await;
        })))
    } else {
        None
    };

    // Brain export scheduler — enqueues brain_export fabric jobs per due
    // brain (interval per brain config). Background — run_jobs.
    let brain_sched_handle = if rc.run_jobs {
        let brain_sched = pensieve_server::brain::scheduler::BrainScheduler::new(
            brain_state.clone(),
            fabric_store.clone(),
        );
        let brain_sched_rx = shutdown_tx.subscribe();
        Some(tokio::spawn(brain_sched.run(async move {
            let mut rx = brain_sched_rx;
            let _ = rx.recv().await;
        })))
    } else {
        None
    };

    // Idempotency ledger cleanup — runs every hour, deletes entries older
    // than 25 hours (1-hour grace beyond the 24-hour TTL). Background — run_jobs.
    let idem_cleanup_handle = if rc.run_jobs {
        let idem_rx = shutdown_tx.subscribe();
        Some(spawn_idempotency_cleanup(catalog.clone(), async move {
            let mut rx = idem_rx;
            let _ = rx.recv().await;
        }))
    } else {
        None
    };

    if rc.run_jobs {
        info!(
            "background workers started (compaction, retention, physical-gc, idempotency-cleanup)"
        );
    } else {
        info!(role = %role, "background jobs skipped on this role (run_jobs=false)");
    }

    // 6. Arrow Flight gRPC server (optional — set PENSIEVE_GRPC_ADDR=off).
    let grpc_handle = if cli.grpc_addr.eq_ignore_ascii_case("off") {
        info!("grpc: disabled (PENSIEVE_GRPC_ADDR=off)");
        None
    } else {
        let grpc_addr: SocketAddr = cli
            .grpc_addr
            .parse()
            .with_context(|| format!("parsing grpc_addr {}", cli.grpc_addr))?;
        let flight_svc = pensieve_server::flight::flight_server(pensieve_server::flight::FlightState {
            catalog: catalog.clone(),
            format: format.clone(),
            node_id: Some(lease.node_id),
        });
        let mut grpc_rx = shutdown_tx.subscribe();
        info!(addr = %grpc_addr, "grpc/flight server listening");
        Some(tokio::spawn(async move {
            let res = tonic::transport::Server::builder()
                .add_service(flight_svc)
                .serve_with_shutdown(grpc_addr, async move {
                    let _ = grpc_rx.recv().await;
                })
                .await;
            if let Err(e) = res {
                error!(error = %e, "grpc server terminated");
            }
        }))
    };

    // 6.5 OTLP gRPC server (port 4317 conventionally).
    let otlp_handle = if cli.otlp_addr.eq_ignore_ascii_case("off") {
        info!("otlp: disabled (PENSIEVE_OTLP_ADDR=off)");
        None
    } else {
        let otlp_addr: SocketAddr = cli
            .otlp_addr
            .parse()
            .with_context(|| format!("parsing otlp_addr {}", cli.otlp_addr))?;
        let otlp_svc = OtlpLogsServer::new(OtlpLogsService::new(
            catalog.clone(),
            write_path.clone(),
            cli.otlp_database.clone(),
        ));
        let otlp_trace_svc = OtlpTraceService::new(
            catalog.clone(),
            write_path.clone(),
            cli.otlp_database.clone(),
        )
        .into_server();
        let mut otlp_rx = shutdown_tx.subscribe();
        info!(addr = %otlp_addr, database = %cli.otlp_database, "otlp gRPC server listening");
        Some(tokio::spawn(async move {
            let res = tonic::transport::Server::builder()
                .add_service(otlp_svc)
                .add_service(otlp_trace_svc)
                .serve_with_shutdown(otlp_addr, async move {
                    let _ = otlp_rx.recv().await;
                })
                .await;
            if let Err(e) = res {
                error!(error = %e, "otlp server terminated");
            }
        }))
    };

    // 7. HTTP server.
    //    Apply dev CORS to the outermost router so browsers on a separate
    //    origin (e.g. `localhost:5173`) can reach the API. Production
    //    deploys should replace this with a config-driven allow-list.
    let app = pensieve_server::with_configured_cors(app);
    let listener = tokio::net::TcpListener::bind(cli.http_addr)
        .await
        .with_context(|| format!("binding {}", cli.http_addr))?;
    info!(addr = %cli.http_addr, "http server listening");
    let shutdown = shutdown_signal();
    // connect-info make-service so handlers can read the peer addr (the
    // live-consumers overlay records the connecting agent's ip).
    let serve = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown);
    if let Err(e) = serve.await {
        error!(error = %e, "http server terminated with error");
    }

    // Land queued memory writes first — explicit saves must commit before the
    // process winds down (the worker's shutdown pass also drains, but this is
    // the earlier, bounded hook).
    if let Some((q, _)) = &memory_queue {
        info!("draining memory ingest queue");
        let _ = q.drain(std::time::Duration::from_secs(30)).await;
        info!("memory ingest queue drained");
    }

    // Explicitly drain the staging buffer now that HTTP is no longer accepting
    // new ingest requests. This ensures partially-flushed batches are committed
    // before we wind down background workers. The staging timer also drains on
    // shutdown_tx, but that fires later — this is the earlier, safer hook.
    info!("draining ingest staging buffer");
    write_path.drain_staging().await;
    info!("staging buffer drained");

    // Broadcast the wind-down signal BEFORE joining any task handles. The
    // grpc/otlp/filedrop/kafka servers and the background workers all run until
    // they observe this signal (serve_with_shutdown / shutdown_tx.subscribe()),
    // so awaiting their handles first would deadlock — the task never returns
    // because the signal that ends it hasn't been sent yet. In production this
    // manifested as SIGTERM hanging until the k8s grace-period SIGKILL.
    let _ = shutdown_tx.send(());

    if let Some(h) = grpc_handle {
        let _ = h.await;
    }
    if let Some(h) = otlp_handle {
        let _ = h.await;
    }
    if let Some(h) = filedrop_handle {
        let _ = h.await;
    }
    if let Some(h) = memory_consolidator_handle {
        let _ = h.await;
    }
    if let Some(h) = ci_correlate_handle {
        let _ = h.await;
    }
    if let Some(h) = file_promote_handle {
        let _ = h.await;
    }
    if let Some(h) = kafka_handle {
        let _ = h.await;
    }

    // Workers were already signaled above; join their handles. Background-job
    // handles are None on stateless query / committer roles (run_jobs=false).
    if let Some(h) = worker_handle {
        let _ = h.await;
    }
    if let Some(h) = index_scheduler_handle {
        let _ = h.await;
    }
    if let Some(h) = graph_snapshot_scheduler_handle {
        let _ = h.await;
    }
    if let Some(h) = scheduler_handle {
        let _ = h.await;
    }
    if let Some(h) = retention_handle {
        let _ = h.await;
    }
    if let Some(h) = gc_handle {
        let _ = h.await;
    }
    if let Some(h) = artifact_gc_handle {
        let _ = h.await;
    }
    if let Some(h) = artifact_graph_handle {
        let _ = h.await;
    }
    if let Some(h) = conn_sched_handle {
        let _ = h.await;
    }
    if let Some(h) = idem_cleanup_handle {
        let _ = h.await;
    }
    if let Some((_, h)) = memory_queue {
        let _ = h.await;
    }
    for h in fabric_runner_handles {
        let _ = h.await;
    }
    let _ = fabric_housekeeping.await;
    if let Some(h) = dreaming_sched_handle {
        let _ = h.await;
    }
    if let Some(h) = brain_sched_handle {
        let _ = h.await;
    }
    // Mark the embedded worker offline so discovery doesn't show a ghost.
    if let Err(e) = fabric_store
        .set_worker_status(embedded_worker_id, pensieve_core::fabric::WorkerStatus::Offline)
        .await
    {
        error!(error = %e, "failed to offline embedded worker on shutdown");
    }

    // 6. Best-effort cleanup — deregister the node.
    if let Err(e) = catalog.deregister_node(lease).await {
        error!(error = %e, "failed to deregister node on shutdown");
    } else {
        info!("node deregistered");
    }

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.ok();
    };
    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut s) = signal::unix::signal(signal::unix::SignalKind::terminate()) {
            s.recv().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!("ctrl-c received; shutting down"),
        _ = terminate => info!("SIGTERM received; shutting down"),
    }
}

/// Embedded-worker executor for `dreaming` jobs: bridges the fabric's
/// [`pensieve_jobs::JobExecutor`] contract onto
/// [`pensieve_server::agent::dreaming::run_dreaming`], which owns the agent run
/// and all persistence (run row, session, trace). Lives here because pensieve-bin
/// is the one place that holds both the fabric runtime and the AgentState.
struct DreamingExecutor {
    state: pensieve_server::agent::AgentState,
    worker_id: uuid::Uuid,
}

#[async_trait::async_trait]
impl pensieve_jobs::JobExecutor for DreamingExecutor {
    fn kind(&self) -> &'static str {
        pensieve_core::fabric::JOB_DREAMING
    }

    async fn run(
        &self,
        ctx: &pensieve_jobs::JobCtx,
        job: &pensieve_core::fabric::ClaimedJob,
    ) -> Result<serde_json::Value, pensieve_jobs::JobError> {
        let mut req: pensieve_server::agent::dreaming::DreamingRequest =
            serde_json::from_value(job.payload.clone())
                .map_err(|e| pensieve_jobs::JobError::Config(format!("dreaming payload: {e}")))?;
        req.job_id = Some(job.id);
        req.worker_id = Some(self.worker_id);

        // Bridge the executor's live snapshots onto the job's progress JSONB.
        let sink = ctx.progress.clone();
        let progress: pensieve_server::agent::dreaming::ProgressFn =
            std::sync::Arc::new(move |snapshot| {
                let sink = sink.clone();
                Box::pin(async move { sink.push(snapshot).await })
            });

        let (run_id, outcome) =
            pensieve_server::agent::dreaming::run_dreaming(&self.state, progress, req)
                .await
                // LLM runs are not retried (max_attempts=1) — any failure here
                // is terminal for the job; the run row carries the detail.
                .map_err(|e| pensieve_jobs::JobError::Permanent(e.to_string()))?;
        let _ = ctx.queue.link_dreaming_run(job.id, run_id).await;
        Ok(serde_json::json!({
            "run_id": run_id,
            "stats": outcome,
        }))
    }
}

/// Fabric executor for `brain_export` jobs: one export pass for the named
/// brain on this worker. Requires the git binary and the brain volume
/// (`PENSIEVE_BRAIN_DIR`) — jobs land here only on workers advertising the
/// `brain` capability; a missing repo dir fails as Config so the operator
/// sees a clear signal instead of a silent re-init on the wrong replica.
struct BrainExportExecutor {
    state: pensieve_server::brain::BrainState,
}

#[async_trait::async_trait]
impl pensieve_jobs::JobExecutor for BrainExportExecutor {
    fn kind(&self) -> &'static str {
        pensieve_core::fabric::JOB_BRAIN_EXPORT
    }

    async fn run(
        &self,
        ctx: &pensieve_jobs::JobCtx,
        job: &pensieve_core::fabric::ClaimedJob,
    ) -> Result<serde_json::Value, pensieve_jobs::JobError> {
        let name = job
            .payload
            .get("brain")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| pensieve_jobs::JobError::Permanent("payload missing `brain`".into()))?;
        if self.state.git.is_none() {
            return Err(pensieve_jobs::JobError::Config(
                "git binary not found on this worker".into(),
            ));
        }
        let rec = self
            .state
            .registry
            .get(name)
            .await
            .map_err(|e| pensieve_jobs::JobError::Transient(e.to_string()))?
            .ok_or_else(|| {
                pensieve_jobs::JobError::Permanent(format!("brain `{name}` not found"))
            })?;
        if !self.state.repo_dir(name).exists() {
            return Err(pensieve_jobs::JobError::Config(format!(
                "brain repo dir missing on this worker (PENSIEVE_BRAIN_DIR volume?): {}",
                self.state.repo_dir(name).display()
            )));
        }
        ctx.progress
            .push(serde_json::json!({ "current_phase": "export", "brain": name }))
            .await;
        pensieve_server::brain::routes::run_export_now(&self.state, &rec.config)
            .await
            .map_err(pensieve_jobs::JobError::Transient)
    }
}

#[cfg(test)]
mod role_tests {
    use super::role_components;

    #[test]
    fn role_components_map_correctly() {
        // Default / unknown → all_in_one (everything).
        for r in ["all_in_one", "", "bogus", "ALL_IN_ONE"] {
            let c = role_components(r);
            assert!(c.run_committer && c.run_jobs, "{r} should run everything");
        }
        // Stateless HTTP nodes: no committer, no jobs (HPA-safe).
        for r in ["query", "ingest", "edge", "Query", " edge "] {
            let c = role_components(r);
            assert!(!c.run_committer && !c.run_jobs, "{r} should be stateless");
        }
        // Committer node: commit lease only.
        let c = role_components("committer");
        assert!(c.run_committer && !c.run_jobs);
        // Worker node: jobs only.
        let w = role_components("worker");
        assert!(!w.run_committer && w.run_jobs);
    }
}
