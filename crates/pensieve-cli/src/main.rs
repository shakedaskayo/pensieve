//! Pensieve CLI — admin + client mode.
//!
//! Client subcommands (talk to a running pensieve server):
//!   connect  <url> [--token TOKEN]   save server URL + bearer token
//!   status                            show config + probe /health
//!   query    "<question>" [--json]    stream /v1/agent/ask to stdout
//!   recall   "<text>" [--realm R]     semantic memory recall via MCP
//!   distill  [--realm R]              stdin transcript → durable memories
//!   ingest   push --table T           stdin NDJSON → POST /v1/ingest
//!   install-skill [--target DIR]      write SKILL.md for coding agents
//!   install-plugin [--target DIR]     install the pensieve-memory Claude Code plugin
//!
//! Admin subcommands (talk directly to Postgres):
//!   create-database <name>
//!   create-table    --db <name> --name <name> --schema <spec>
//!   list-tables     --db <name>
//!   alter-table     --db <name> --table <name> --add-column <spec>
//!   create-graph    --db <name> --name <name> --nodes <tbl> --edges <tbl>
//!   list-graphs     --db <name>
//!   drop-graph      --db <name> --name <name>
//!   version

mod client;
mod brain;
mod datasource;
mod deploy;
mod plugin;
mod scrape;
mod update;
mod users;
mod ux;
use client::{
    delete_json, effective_config, get_json, load_config, probe_auth, probe_health, save_config,
    stream_agent_ask, write_skill_file, ClientConfig,
};

use anyhow::{anyhow, Context, Result};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use clap::{Parser, Subcommand};
use pensieve_catalog::PostgresCatalog;
use pensieve_core::catalog::{Catalog, TableConfig};
use std::net::SocketAddr;
use std::sync::Arc;

const SKILL_TEMPLATE: &str = include_str!("skill_template.md");
/// `pensieve-deploy` skill — production deployment runbook for coding agents.
const DEPLOY_SKILL: &str = include_str!("../../../integrations/claude-code/pensieve-deploy/SKILL.md");

#[derive(Debug, Parser)]
#[command(
    name = "pensieve",
    about = "Pensieve CLI — client queries + admin operations",
    styles = clap::builder::Styles::styled()
)]
struct Cli {
    /// Postgres connection URL (admin subcommands only).
    #[arg(
        long,
        env = "PENSIEVE_CATALOG_URL",
        default_value = "postgres://pensieve:pensieve_dev@localhost:5433/pensieve"
    )]
    catalog_url: String,

    /// Disable colored/styled output (also respects NO_COLOR and non-TTY stdout).
    #[arg(long, global = true)]
    no_color: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    // ── client subcommands ────────────────────────────────────────────
    /// Save a connection to a pensieve server.
    Connect {
        /// Server base URL, e.g. http://localhost:8080
        url: String,
        /// Bearer token (omit to authenticate via /v1/auth/login interactively later).
        #[arg(long)]
        token: Option<String>,
    },
    /// Show the saved connection + probe the server's /health.
    Status,
    /// Compact a table's small extents (merge them) so scans stay fast as data
    /// accumulates. Talks to a running server; its in-process worker does the
    /// merge. With no `--table`, compacts every table in scope.
    Compact {
        /// Table to compact, e.g. claude_code_events.
        #[arg(long)]
        table: Option<String>,
        /// Database — defaults to `default` with --table, else every database.
        #[arg(long)]
        database: Option<String>,
        /// Max extents merged per task (memory bound). Default 32.
        #[arg(long)]
        max_merge: Option<usize>,
        /// Block until the live extent count stops dropping (or ~2 min).
        #[arg(long)]
        wait: bool,
    },
    /// Stream an agent answer to stdout.
    Query {
        /// The question, e.g. "How many 500s in the last hour?"
        question: String,
        /// Emit the raw SSE event stream as JSONL instead of plain text.
        #[arg(long)]
        json: bool,
        /// Resume a specific conversation session by id.
        #[arg(long)]
        session: Option<String>,
        /// Resume the most recent session (the last one `query` used).
        #[arg(long = "continue")]
        continue_session: bool,
    },
    /// Inspect or manage agent conversation sessions.
    Sessions {
        #[command(subcommand)]
        op: SessionsOp,
    },
    /// Recursively scrape a file/folder into pensieve's candidate file graph
    /// (deterministic structure; the local path is the source pointer).
    Scrape(scrape::ScrapeArgs),
    /// Watch a path and contribute files as they change (foreground).
    Watch(scrape::ScrapeArgs),
    /// Manage users — list/create/passwd/set-role/delete (admin token required).
    User {
        #[command(subcommand)]
        op: users::UsersOp,
    },
    /// Install the Pensieve skill so coding agents (Claude Code, Cursor, …)
    /// can discover and use this CLI.
    InstallSkill {
        /// Target directory. Default: `$HOME/.pensieve/skills/<skill>`.
        #[arg(long)]
        target: Option<std::path::PathBuf>,
        /// Also symlink into `$HOME/.claude/skills/<skill>` if that dir exists.
        #[arg(long)]
        also_link_claude: bool,
        /// Which skill(s): the pensieve CLI skill, the production-deployment
        /// skill, or both.
        #[arg(long, value_enum, default_value = "pensieve")]
        which: SkillWhich,
    },
    /// Manage data sources — add a GitHub/GitLab/Bitbucket repo, list, pause,
    /// resume, trigger, remove. See `pensieve datasource --help`.
    #[command(name = "datasource")]
    DataSource {
        #[command(subcommand)]
        op: datasource::Op,
    },
    /// Publish memory as Git-clonable Obsidian vaults ("brains") — create,
    /// list, export, clone. See `pensieve brain --help`.
    Brain {
        #[command(subcommand)]
        op: brain::Op,
    },
    /// Git credential helper for brain clones (used via
    /// `credential.helper=!pensieve git-credential`; not for interactive use).
    #[command(name = "git-credential", hide = true)]
    GitCredential {
        /// The credential action git invokes: get | store | erase.
        action: Option<String>,
    },
    /// Deploy pensieve to production (AWS Fargate + S3 + Supabase) or run a
    /// Supabase-backed local test drive. See `pensieve deploy --help`.
    Deploy {
        #[command(subcommand)]
        op: deploy::Op,
    },
    /// Inspect ingestion runs — `status` snapshots, `tail` follows, `push`
    /// streams NDJSON from stdin into a table.
    Ingest {
        #[command(subcommand)]
        op: datasource::IngestOp,
    },
    /// Recall durable memories from Pensieve (semantic search via the MCP
    /// `recall_memory` tool). Used by the pensieve-memory plugin to inject context.
    Recall {
        /// What to recall.
        query: String,
        /// Restrict to a realm (plus `global`). Defaults to all realms.
        #[arg(long)]
        realm: Option<String>,
        /// Max memories to return.
        #[arg(long, default_value_t = 8)]
        limit: usize,
        /// Emit the raw MCP structured result as JSON instead of a ranked list.
        #[arg(long)]
        json: bool,
    },
    /// Save a durable memory to Pensieve (recallable later via `pensieve recall`).
    Remember {
        /// The memory content — a self-contained, durable fact/decision/preference.
        content: String,
        /// Type: fact | decision | preference | learning | procedure (default: fact).
        #[arg(long = "type")]
        memory_type: Option<String>,
        /// Realm (namespace). Defaults to the server's default realm.
        #[arg(long)]
        realm: Option<String>,
        /// Importance 0.0–1.0 (higher surfaces first).
        #[arg(long)]
        importance: Option<f32>,
        /// Stable upsert key (e.g. `architecture/auth`) — re-saving updates in place.
        #[arg(long = "topic-key")]
        topic_key: Option<String>,
    },
    /// Create/update a virtual graph entity (a service/repo/table/person/concept)
    /// and wire it to memories + existing graph nodes.
    Entity {
        /// Entity name, e.g. "payments service".
        name: String,
        /// Kind: service | repo | table | person | file | config | concept.
        #[arg(long)]
        kind: Option<String>,
        /// Realm (namespace).
        #[arg(long)]
        realm: Option<String>,
        /// Property `key=value` (repeatable).
        #[arg(long = "prop")]
        prop: Vec<String>,
        /// Link `node_id[|namespace[|rel]]` (repeatable) — e.g.
        /// `repo:owner/name|github|LIVES_IN` or `memory:<uuid>||DOCUMENTED_BY`.
        #[arg(long)]
        link: Vec<String>,
        /// Icon from the gallery (e.g. github, datadog, kubernetes, service,
        /// database, person). Omit to auto-derive from type/kind/vendor.
        #[arg(long)]
        icon: Option<String>,
        /// Classification type `provider::resource` (e.g. kubernetes::pod,
        /// aws::ec2::instance, github::repository, datadog::monitor).
        #[arg(long = "type")]
        r#type: Option<String>,
    },
    /// Distill a session transcript (stdin) into durable memories via the
    /// pensieve agent. Used by the pensieve-memory plugin at session end.
    Distill {
        /// Originating Claude Code session id (recorded for provenance).
        #[arg(long)]
        session: Option<String>,
        /// Memory realm to save under. Defaults to `default`.
        #[arg(long)]
        realm: Option<String>,
    },
    /// Install the pensieve-memory Claude Code plugin (hooks + MCP + commands)
    /// into `~/.claude/skills/pensieve-memory`.
    InstallPlugin {
        /// Target plugin directory. Default: `$HOME/.claude/skills/pensieve-memory`.
        #[arg(long)]
        target: Option<std::path::PathBuf>,
        /// Overwrite an existing install without warning.
        #[arg(long)]
        force: bool,
    },

    // ── local engine (zero-infra: embedded SQLite + local files) ──────
    /// Serve the Model Context Protocol over stdio — what a coding agent spawns.
    /// Full context-engine toolset over an embedded local catalog (no infra).
    Mcp,
    /// Serve the web UI + HTTP API locally (query/catalog/graph/ingest/MCP), zero
    /// infra. Browse the graph + ingest on demand. Sign in: admin / admin.
    Serve {
        /// Listen address.
        #[arg(long, env = "PENSIEVE_LOCAL_HTTP_ADDR", default_value = "127.0.0.1:7777")]
        addr: SocketAddr,
    },
    /// Wire a coding agent to `pensieve mcp` over stdio (claude-code | cursor |
    /// windsurf | …). One-liner onboarding; `setup list` shows the supported set.
    Setup {
        /// Agent key (e.g. claude-code, cursor, windsurf), or `list`.
        agent: String,
        /// Print the config instead of writing it.
        #[arg(long)]
        print: bool,
    },
    /// Sync memory with Claude Code's file memory (~/.claude/projects/*/memory,
    /// always) and bidirectionally with a control plane (when PENSIEVE_CLOUD_URL is
    /// set). The file phase ingests + embeds Claude Code memory files, promotes
    /// high-value pensieve memories back as native files, and curates MEMORY.md.
    Sync {
        /// Keep running, re-syncing on an interval (PENSIEVE_CC_SYNC_POLL_SECS,
        /// default 30s).
        #[arg(long)]
        watch: bool,
        /// Plan + audit-log Claude Code file changes without writing them
        /// (ingestion into the local store still runs).
        #[arg(long)]
        dry_run: bool,
        /// Only the local Claude Code file phase (skip the control plane).
        #[arg(long)]
        cc_only: bool,
        /// Only the control-plane push/pull (skip Claude Code files).
        #[arg(long)]
        cloud_only: bool,
        /// Limit the file phase to one project path.
        #[arg(long)]
        project: Option<std::path::PathBuf>,
    },
    /// Manage the optional background sync worker — an OS user service
    /// (launchd on macOS, systemd --user on Linux) running `pensieve sync --watch`
    /// so memory stays synced with no terminal or session open.
    Worker {
        #[command(subcommand)]
        action: WorkerAction,
    },
    /// Manage the local server as an OS user service (launchd on macOS,
    /// systemd --user on Linux): starts at login, restarts on crash —
    /// `pensieve serve` that stays up. See `pensieve service --help`.
    Service {
        #[command(subcommand)]
        action: ServiceAction,
    },

    // ── admin subcommands ─────────────────────────────────────────────
    /// Create a new database (namespace) — admin, talks to Postgres directly.
    CreateDatabase {
        name: String,
        /// Succeed (and print the existing id) if the database already exists,
        /// instead of erroring. Makes bootstrap scripts idempotent — the server
        /// itself pre-creates `default` on boot.
        #[arg(long)]
        if_not_exists: bool,
    },
    /// Create a new table — admin.
    CreateTable {
        #[arg(long)]
        db: String,
        #[arg(long)]
        name: String,
        /// Schema spec: "col:type,col:type,...". Types: int, long, real, bool, string, timestamp, dynamic.
        #[arg(long)]
        schema: String,
        /// Optional retention in days.
        #[arg(long)]
        retention_days: Option<u32>,
    },
    /// List tables in a database — admin.
    ListTables {
        #[arg(long)]
        db: String,
    },
    /// Configure auto-embedding for a table: a background job embeds the text
    /// in `--source-column` into the vector `--embedding-column`. Rows can be
    /// ingested with the embedding column left NULL; the engine fills it.
    ConfigureEmbed {
        #[arg(long)]
        db: String,
        #[arg(long)]
        table: String,
        /// Utf8 text column whose contents are embedded.
        #[arg(long)]
        source_column: String,
        /// FixedSizeList<Float32, dim> column the embeddings are written into.
        #[arg(long)]
        embedding_column: String,
        /// Embedding-backend id, e.g. "fastembed/bge-small-en-v1.5".
        #[arg(long)]
        model: String,
        /// Embedding dimension (must match the embedding column's list size).
        #[arg(long)]
        dim: u32,
        /// Disable auto-embedding (config kept but the scheduler skips it).
        #[arg(long)]
        disable: bool,
    },
    /// Add a nullable column to an existing table — admin.
    AlterTable {
        #[arg(long)]
        db: String,
        #[arg(long)]
        table: String,
        /// Spec: `name:type`. Types: bool, int, long, real, string, timestamp, dynamic.
        #[arg(long)]
        add_column: String,
    },
    /// Print the CLI version.
    Version,
    /// Self-update to the latest GitHub release (binary + embedded web UI),
    /// then restart the local server so the new UI is live immediately.
    Update {
        /// Only check whether a newer release exists; don't install.
        #[arg(long)]
        check: bool,
        /// Install a specific release tag (e.g. v0.0.3) instead of the latest.
        #[arg(long)]
        version: Option<String>,
        /// Reinstall even if this version is already current.
        #[arg(long)]
        force: bool,
        /// Don't restart a running local `pensieve serve` after updating.
        #[arg(long)]
        no_restart: bool,
    },
    /// Register a property-graph — admin.
    CreateGraph {
        #[arg(long)]
        db: String,
        #[arg(long)]
        name: String,
        #[arg(long)]
        nodes: String,
        #[arg(long)]
        edges: String,
        #[arg(long, default_value = "id")]
        id_col: String,
        #[arg(long, default_value = "labels")]
        label_col: String,
        #[arg(long, default_value = "src")]
        src_col: String,
        #[arg(long, default_value = "dst")]
        dst_col: String,
        #[arg(long, default_value = "type")]
        type_col: String,
        #[arg(long)]
        realm_col: Option<String>,
    },
    /// List registered graphs in a database — admin.
    ListGraphs {
        #[arg(long)]
        db: String,
    },
    /// Drop a graph registration — admin.
    DropGraph {
        #[arg(long)]
        db: String,
        #[arg(long)]
        name: String,
    },
}

#[derive(Debug, Subcommand)]
enum WorkerAction {
    /// Install + start the background sync worker (user service).
    Install {
        /// Poll interval in seconds (default 30; sets PENSIEVE_CC_SYNC_POLL_SECS).
        #[arg(long)]
        interval: Option<u64>,
        /// Only the Claude Code file phase.
        #[arg(long)]
        cc_only: bool,
        /// Only the control-plane push/pull.
        #[arg(long)]
        cloud_only: bool,
    },
    /// Stop + remove the background sync worker.
    Uninstall,
    /// Show whether the worker is installed/running and where it logs.
    Status,
    /// Run a fabric node daemon: register with the control plane, sync local
    /// sources, and pull jobs this node accepts (low-impact by default).
    Run {
        /// Control-plane URL (or PENSIEVE_SERVER_URL).
        #[arg(long, env = "PENSIEVE_SERVER_URL")]
        server: String,
        /// Worker token from `pensieve worker create` (or PENSIEVE_WORKER_TOKEN).
        #[arg(long, env = "PENSIEVE_WORKER_TOKEN")]
        token: String,
        /// Job kinds to accept (comma-separated). Default: source_sync only.
        #[arg(long, value_delimiter = ',', default_value = "source_sync")]
        accept: Vec<String>,
        #[arg(long, default_value_t = 1)]
        max_concurrent: usize,
        /// Friendly node name (defaults to node@<hostname>).
        #[arg(long)]
        name: Option<String>,
    },
    /// Mint a worker identity + token on the control plane (admin).
    Create {
        #[arg(long)]
        name: String,
        /// Capabilities to advertise (comma-separated). Default: `sources`
        /// (this node owns local coding-agent sources). The running daemon also
        /// advertises a per-agent `source:<kind>` for each detected agent.
        #[arg(long, value_delimiter = ',', default_value = "sources")]
        capabilities: Vec<String>,
    },
    /// List registered workers (node discovery).
    List,
    /// Revoke a worker's token and mark it offline.
    Revoke {
        /// Worker id (uuid).
        id: String,
    },
}

#[derive(Debug, Subcommand)]
enum ServiceAction {
    /// Install + start the server service (web UI + API + workers).
    Install {
        /// Listen address.
        #[arg(long, default_value = "127.0.0.1:7777")]
        addr: String,
        /// Static admin token (PENSIEVE_AUTH_TOKENS=<token>:admin in the service
        /// env). Omit for the auth-disabled local default.
        #[arg(long)]
        token: Option<String>,
    },
    /// Stop + remove the server service.
    Uninstall,
    /// Show whether the server service is installed/running and where it logs.
    Status,
}

#[derive(Debug, Subcommand)]
enum SessionsOp {
    /// List recent conversation sessions.
    List,
    /// Show a session's metadata + rolling summary.
    Show { id: String },
    /// Print a session's turns in order.
    Turns { id: String },
    /// Delete a session and all of its turns.
    Delete { id: String },
}

// Worker threads are 8 MiB. tokio-macros 2.7 has no `thread_stack_size`
// argument on `#[tokio::main]`; the iterative ingest scanner is the real
// guard, and this stack is the backstop for other recursive walks.
fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .thread_stack_size(8 * 1024 * 1024)
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(async_main())
}

async fn async_main() {
    let cli = Cli::parse();
    ux::theme::init(cli.no_color);
    if let Err(err) = run(cli).await {
        ux::error::print_error(&err);
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<()> {
    // `pensieve serve` sets up a richer subscriber that includes the OTel self-trace
    // layer; all other subcommands use a plain fmt subscriber.
    let self_trace_handle = if matches!(cli.command, Command::Serve { .. }) {
        Some(pensieve_local::setup_serve_tracing())
    } else {
        // Logs go to STDERR so command output (and the `pensieve mcp` stdio
        // protocol channel) stays clean on stdout.
        tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                    tracing_subscriber::EnvFilter::new("info,sqlx=warn,hyper=warn")
                }),
            )
            .with_target(false)
            .try_init()
            .ok();
        None
    };

    match cli.command {
        // ── client subcommands ────────────────────────────────────────
        Command::Connect { url, token } => cmd_connect(url, token).await,
        Command::Status => {
            let result = cmd_status().await;
            update::maybe_notify_update().await;
            result
        }
        Command::Compact {
            table,
            database,
            max_merge,
            wait,
        } => cmd_compact(table, database, max_merge, wait).await,
        Command::Query {
            question,
            json,
            session,
            continue_session,
        } => cmd_query(question, json, session, continue_session).await,
        Command::Sessions { op } => cmd_sessions(op).await,
        Command::Scrape(args) => scrape::scrape(args).await,
        Command::Watch(args) => scrape::watch(args).await,
        Command::User { op } => users::run(op).await,
        Command::InstallSkill {
            target,
            also_link_claude,
            which,
        } => cmd_install_skill(target, also_link_claude, which).await,
        Command::DataSource { op } => datasource::run(op).await,
        Command::Brain { op } => brain::run(op).await,
        Command::GitCredential { action } => brain::run_git_credential(action).await,
        Command::Deploy { op } => deploy::run(op).await,
        Command::Ingest { op } => datasource::run_ingest(op).await,
        Command::Recall {
            query,
            realm,
            limit,
            json,
        } => plugin::recall(query, realm, limit, json).await,
        Command::Remember {
            content,
            memory_type,
            realm,
            importance,
            topic_key,
        } => plugin::remember(content, memory_type, realm, importance, topic_key).await,
        Command::Entity {
            name,
            kind,
            realm,
            prop,
            link,
            icon,
            r#type,
        } => plugin::entity(name, kind, realm, prop, link, icon, r#type).await,
        Command::Distill { session, realm } => plugin::distill(session, realm).await,
        Command::InstallPlugin { target, force } => plugin::install_plugin(target, force).await,
        // Local engine — delegate to the pensieve-local library (one `pensieve` binary).
        Command::Mcp => pensieve_local::run_mcp().await,
        Command::Serve { addr } => {
            // Fire-and-forget staleness nudge; serve runs until killed so it
            // prints (to stderr) once the throttled check resolves.
            tokio::spawn(update::maybe_notify_update());
            pensieve_local::run_serve(addr, self_trace_handle).await
        }
        Command::Setup { agent, print } => pensieve_local::run_setup(&agent, print),
        Command::Sync {
            watch,
            dry_run,
            cc_only,
            cloud_only,
            project,
        } => {
            pensieve_local::run_sync(pensieve_local::SyncOptions {
                watch,
                dry_run,
                cc_only,
                cloud_only,
                project,
            })
            .await
        }
        Command::Worker { action } => match action {
            WorkerAction::Install {
                interval,
                cc_only,
                cloud_only,
            } => {
                pensieve_local::worker::install(&pensieve_local::worker::WorkerOptions {
                    interval_secs: interval,
                    cc_only,
                    cloud_only,
                    pensieve_home: None, // resolved by install()
                })
            }
            WorkerAction::Uninstall => pensieve_local::worker::uninstall(),
            WorkerAction::Status => pensieve_local::worker::status(),
            WorkerAction::Run {
                server,
                token,
                accept,
                max_concurrent,
                name,
            } => {
                pensieve_local::node::run_node(pensieve_local::node::NodeConfig {
                    server_url: server,
                    token,
                    accept,
                    max_concurrent,
                    name,
                })
                .await
            }
            WorkerAction::Create { name, capabilities } => {
                let cfg = client::effective_config()?;
                let resp = reqwest::Client::new()
                    .post(format!("{}/v1/workers", cfg.endpoint.trim_end_matches('/')))
                    .bearer_auth(cfg.token.clone().unwrap_or_default())
                    .json(&serde_json::json!({ "name": name, "capabilities": capabilities }))
                    .send()
                    .await?;
                let status = resp.status();
                let body: serde_json::Value = resp.json().await.unwrap_or_default();
                if !status.is_success() {
                    anyhow::bail!("create failed ({status}): {body}");
                }
                println!("worker_id: {}", body["worker_id"].as_str().unwrap_or("?"));
                println!("token:     {}", body["token"].as_str().unwrap_or("?"));
                println!();
                println!("Shown once — store it now. Start the node with:");
                println!(
                    "  pensieve worker run --server {} --token <token>",
                    cfg.endpoint
                );
                Ok(())
            }
            WorkerAction::List => {
                let cfg = client::effective_config()?;
                let resp = reqwest::Client::new()
                    .get(format!("{}/v1/workers", cfg.endpoint.trim_end_matches('/')))
                    .bearer_auth(cfg.token.clone().unwrap_or_default())
                    .send()
                    .await?;
                let body: serde_json::Value = resp.json().await.unwrap_or_default();
                let items = body["items"].as_array().cloned().unwrap_or_default();
                if items.is_empty() {
                    println!("no workers registered");
                    return Ok(());
                }
                for w in items {
                    println!(
                        "{}  {:<26} {:<9} {:<8} caps={} presence={} heartbeat={}",
                        w["id"].as_str().unwrap_or("?"),
                        w["name"].as_str().unwrap_or("?"),
                        w["kind"].as_str().unwrap_or("?"),
                        w["status"].as_str().unwrap_or("?"),
                        w["capabilities"].as_array().map(|a| a.len()).unwrap_or(0),
                        w["presence"].as_array().map(|a| a.len()).unwrap_or(0),
                        w["last_heartbeat"].as_str().unwrap_or("never"),
                    );
                }
                Ok(())
            }
            WorkerAction::Revoke { id } => {
                let cfg = client::effective_config()?;
                let resp = reqwest::Client::new()
                    .delete(format!(
                        "{}/v1/workers/{}",
                        cfg.endpoint.trim_end_matches('/'),
                        id
                    ))
                    .bearer_auth(cfg.token.clone().unwrap_or_default())
                    .send()
                    .await?;
                if resp.status().is_success() {
                    println!("revoked {id}");
                    Ok(())
                } else {
                    anyhow::bail!("revoke failed ({})", resp.status())
                }
            }
        },
        Command::Service { action } => match action {
            ServiceAction::Install { addr, token } => {
                pensieve_local::server_service::install(&pensieve_local::server_service::ServerOptions {
                    addr: addr.clone(),
                    token: token.clone(),
                    pensieve_home: None,
                    secret_key: None,
                })?;
                // Keep the CLI pointed at the service we just installed: the
                // plist/unit carries this token, so config.json must match or
                // every CLI call and capture hook 401s silently.
                if let Err(e) =
                    client::persist_local_connection(&format!("http://{addr}"), token.as_deref())
                {
                    eprintln!("warning: couldn't sync ~/.pensieve/config.json: {e}");
                }
                Ok(())
            }
            ServiceAction::Uninstall => pensieve_local::server_service::uninstall(),
            ServiceAction::Status => pensieve_local::server_service::status(),
        },

        // ── admin subcommands ─────────────────────────────────────────
        Command::Version => {
            println!("pensieve {}", env!("CARGO_PKG_VERSION"));
            update::maybe_notify_update().await;
            Ok(())
        }
        Command::Update {
            check,
            version,
            force,
            no_restart,
        } => update::run(check, version, force, no_restart).await,
        Command::CreateDatabase {
            name,
            if_not_exists,
        } => {
            let catalog = connect_catalog(&cli.catalog_url).await?;
            if if_not_exists {
                if let Some(id) = catalog
                    .lookup_database(&name)
                    .await
                    .with_context(|| format!("looking up database {name}"))?
                {
                    println!("database {name} already exists ({id})");
                    return Ok(());
                }
            }
            let id = catalog
                .create_database(&name)
                .await
                .with_context(|| format!("creating database {name}"))?;
            println!("created database {name} ({id})");
            Ok(())
        }
        Command::CreateTable {
            db,
            name,
            schema,
            retention_days,
        } => {
            let catalog = connect_catalog(&cli.catalog_url).await?;
            let db_id = find_database_id(&catalog, &db).await?;
            let parsed_schema = parse_schema_spec(&schema)
                .with_context(|| format!("parsing schema spec: {schema}"))?;
            let config = TableConfig {
                retention_days,
                ..Default::default()
            };
            let id = catalog
                .create_table(db_id, &name, Arc::new(parsed_schema), config)
                .await
                .with_context(|| format!("creating table {db}.{name}"))?;
            println!("created table {db}.{name} ({id})");
            Ok(())
        }
        Command::ConfigureEmbed {
            db,
            table,
            source_column,
            embedding_column,
            model,
            dim,
            disable,
        } => {
            let catalog = connect_catalog(&cli.catalog_url).await?;
            let tref = catalog
                .lookup_table(&db, &table)
                .await
                .with_context(|| format!("looking up table {db}.{table}"))?;
            // Validate the columns exist and have the right shape before saving.
            let fields = tref.schema.fields();
            let src_ok = fields.iter().any(|f| {
                f.name() == &source_column && matches!(f.data_type(), arrow_schema::DataType::Utf8)
            });
            if !src_ok {
                anyhow::bail!("source column '{source_column}' must be a Utf8 (string) column");
            }
            let emb_ok = fields.iter().any(|f| {
                f.name() == &embedding_column
                    && matches!(f.data_type(),
                        arrow_schema::DataType::FixedSizeList(inner, n)
                        if *n == dim as i32 && matches!(inner.data_type(), arrow_schema::DataType::Float32))
            });
            if !emb_ok {
                anyhow::bail!(
                    "embedding column '{embedding_column}' must be FixedSizeList<Float32, {dim}>"
                );
            }
            catalog
                .set_table_embed_config(
                    pensieve_core::tenant::DEFAULT_TENANT,
                    &pensieve_core::catalog::TableEmbedConfig {
                        table_id: tref.id,
                        source_column: source_column.clone(),
                        embedding_column: embedding_column.clone(),
                        model_id: model.clone(),
                        dim: dim as u16,
                        auto_embed: !disable,
                    },
                )
                .await
                .with_context(|| format!("setting embed config for {db}.{table}"))?;
            println!(
                "configured auto-embed for {db}.{table}: {source_column} -> {embedding_column} \
                 via {model} (dim {dim}, auto_embed={})",
                !disable
            );
            Ok(())
        }
        Command::AlterTable {
            db,
            table,
            add_column,
        } => {
            let catalog = connect_catalog(&cli.catalog_url).await?;
            let t = catalog.lookup_table(&db, &table).await?;
            let (name, ty) = add_column
                .split_once(':')
                .ok_or_else(|| anyhow!("--add-column must be name:type; got '{add_column}'"))?;
            let new_schema = catalog
                .alter_table_add_column(t.id, name.trim(), ty.trim())
                .await?;
            println!(
                "altered {db}.{table}: added column {name}:{ty} (schema_snapshot={new_schema})"
            );
            Ok(())
        }
        Command::ListTables { db } => {
            let catalog = connect_catalog(&cli.catalog_url).await?;
            let tables = catalog.list_tables_in_database(&db).await?;
            if tables.is_empty() {
                println!("(no tables in database {db})");
            } else {
                for t in tables {
                    let cols: Vec<String> = t
                        .schema
                        .fields()
                        .iter()
                        .map(|f| format!("{}:{:?}", f.name(), f.data_type()))
                        .collect();
                    println!("{}  [{}]", t.name, cols.join(", "));
                }
            }
            Ok(())
        }
        Command::CreateGraph {
            db,
            name,
            nodes,
            edges,
            id_col,
            label_col,
            src_col,
            dst_col,
            type_col,
            realm_col,
        } => {
            if name == "schema" {
                anyhow::bail!(
                    "'schema' is reserved for the synthetic schema-graph; choose another name"
                );
            }
            let cat = connect_catalog(&cli.catalog_url).await?;
            let spec = pensieve_core::catalog::GraphSpec {
                node_table: nodes,
                edge_table: edges,
                id_col,
                label_col,
                src_col,
                dst_col,
                type_col,
                realm_col,
            };
            let reg = cat.create_graph(&db, &name, spec).await?;
            println!(
                "registered graph '{}' in db '{}' (nodes={}, edges={})",
                reg.name, db, reg.node_table, reg.edge_table
            );
            Ok(())
        }
        Command::ListGraphs { db } => {
            let cat = connect_catalog(&cli.catalog_url).await?;
            let graphs = cat.list_graphs(&db).await?;
            if graphs.is_empty() {
                println!("(no graphs registered in '{db}')");
            } else {
                for g in graphs {
                    println!(
                        "{}\tnodes={}\tedges={}\trealm={}",
                        g.name,
                        g.node_table,
                        g.edge_table,
                        g.realm_col.as_deref().unwrap_or("-")
                    );
                }
            }
            Ok(())
        }
        Command::DropGraph { db, name } => {
            let cat = connect_catalog(&cli.catalog_url).await?;
            if cat.drop_graph(&db, &name).await? {
                println!("dropped graph '{name}' from '{db}'");
            } else {
                println!("no graph '{name}' in '{db}'");
            }
            Ok(())
        }
    }
}

// ── client subcommand implementations ────────────────────────────────────────

async fn cmd_compact(
    table: Option<String>,
    database: Option<String>,
    max_merge: Option<usize>,
    wait: bool,
) -> Result<()> {
    let cfg = client::effective_config()?;
    let body = serde_json::json!({
        "table": table,
        "database": database,
        "max_merge": max_merge,
        "wait": wait,
    });
    if wait {
        println!("Compacting… (waiting for the server's worker to drain the queue)");
    }
    let v = client::post_json(&cfg, "/v1/admin/compact", body).await?;
    println!("{}", serde_json::to_string_pretty(&v)?);
    Ok(())
}

async fn cmd_connect(url: String, token: Option<String>) -> Result<()> {
    let url = url.trim_end_matches('/').to_string();
    // Preserve any existing last_session_id across reconnects.
    let cfg = ClientConfig {
        endpoint: url.clone(),
        token,
        ..load_config().unwrap_or_default()
    };
    save_config(&cfg)?;
    println!("Saved connection to {url}");
    let path = client::config_path()?;
    println!("Config: {}", path.display());
    Ok(())
}

async fn cmd_status() -> Result<()> {
    match load_config() {
        Ok(cfg) => {
            println!("Endpoint:  {}", cfg.endpoint);
            let token_line = if cfg.token.is_some() {
                ux::theme::success(&format!("{} configured", ux::theme::CHECK))
            } else {
                ux::theme::muted(&format!("{} not set", ux::theme::CROSS))
            };
            println!("Token:     {token_line}");
            // Use effective_config for probes so PENSIEVE_SERVER_URL/PENSIEVE_TOKEN env
            // overrides are honoured; fall back to the on-disk config if it fails.
            let probe_cfg = effective_config().unwrap_or_else(|_| cfg.clone());
            match probe_health(&probe_cfg).await {
                Ok(body) => println!(
                    "Health:    {}",
                    ux::theme::success(&format!("{} {}", ux::theme::CHECK, body.trim()))
                ),
                Err(e) => println!(
                    "Health:    {}",
                    ux::theme::error(&format!("{} error — {e}", ux::theme::CROSS))
                ),
            }
            match probe_auth(&probe_cfg).await {
                Ok(true) => println!(
                    "Auth:      {}",
                    ux::theme::success(&format!("{} ok (token accepted)", ux::theme::CHECK))
                ),
                Ok(false) => println!(
                    "Auth:      {}",
                    ux::theme::warn(&format!(
                        "{} TOKEN REJECTED — the server does not accept the configured token.\n           Fix: re-run the installer, or `pensieve service install --addr <addr> --token <tok>`,\n           or `pensieve connect {} --token <tok>` with the server's real token.",
                        ux::theme::CROSS,
                        probe_cfg.endpoint
                    ))
                ),
                Err(e) => println!(
                    "Auth:      {}",
                    ux::theme::error(&format!("{} probe error — {e}", ux::theme::CROSS))
                ),
            }
            // Hook-side capture health (written by the pensieve-memory plugin hooks).
            if let Ok(dir) = client::config_dir() {
                let p = dir.join("capture-health.json");
                if let Ok(raw) = std::fs::read_to_string(&p) {
                    let v: serde_json::Value = serde_json::from_str(&raw).unwrap_or_default();
                    println!(
                        "Capture:   {}",
                        ux::theme::error(&format!(
                            "{} LAST INGEST FAILED at {} — {}",
                            ux::theme::CROSS,
                            v["ts"].as_str().unwrap_or("?"),
                            v["detail"].as_str().unwrap_or("unknown error"),
                        ))
                    );
                } else {
                    println!(
                        "Capture:   {}",
                        ux::theme::success(&format!(
                            "{} ok (no recorded hook failures)",
                            ux::theme::CHECK
                        ))
                    );
                }

                // cc-sync freshness (written by `run_cc_phase` on every pass —
                // hook-triggered, worker-driven, or manual `pensieve sync`).
                let sp = dir.join("cc-sync-health.json");
                if let Ok(raw) = std::fs::read_to_string(&sp) {
                    let v: serde_json::Value = serde_json::from_str(&raw).unwrap_or_default();
                    let ts = v["ts"].as_str().unwrap_or("?");
                    if v["status"].as_str() == Some("ok") {
                        println!("Sync:      last cc-sync ok at {ts}");
                    } else {
                        println!(
                            "Sync:      LAST CC-SYNC FAILED at {ts} — {}",
                            v["detail"].as_str().unwrap_or("unknown error"),
                        );
                    }
                } else {
                    println!(
                        "Sync:      no recorded cc-sync yet (runs automatically at Claude Code \
                         session start/end, or via `pensieve sync`)"
                    );
                }
            }

            // Background worker (`pensieve worker install`) — sync otherwise only
            // runs at session-boundary hooks or manual `pensieve sync` calls.
            let w = pensieve_local::worker::probe();
            match w.installed {
                Some(true) if w.running => {
                    println!("Worker:    installed, running (continuous `pensieve sync --watch`)");
                }
                Some(true) => println!(
                    "Worker:    installed but not running — see ~/.pensieve/logs/worker.log, or \
                     `pensieve worker install` again"
                ),
                Some(false) => println!(
                    "Worker:    not installed — sync only runs at session start/end \
                     (`pensieve worker install` for continuous sync)"
                ),
                None => {}
            }
        }
        Err(_) => {
            println!(
                "{}",
                ux::theme::muted("No config found. Run `pensieve connect <url>` first.")
            );
        }
    }
    Ok(())
}

async fn cmd_query(
    question: String,
    json: bool,
    session: Option<String>,
    continue_session: bool,
) -> Result<()> {
    let cfg = effective_config()?;
    // Resolve the session to resume: explicit --session wins, else --continue
    // reuses the last-used session from the persisted config.
    let resolved_session = match session {
        Some(s) => Some(s),
        None if continue_session => load_config().ok().and_then(|c| c.last_session_id),
        None => None,
    };

    let mut had_error = false;
    let mut new_session_id: Option<String> = None;
    stream_agent_ask(
        &cfg,
        &question,
        resolved_session.as_deref(),
        |event, data| {
            if event == "session" {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(data) {
                    if let Some(s) = v.get("session_id").and_then(|s| s.as_str()) {
                        new_session_id = Some(s.to_string());
                    }
                }
            }
            if json {
                println!("{}", serde_json::json!({ "event": event, "data": data }));
                return;
            }
            match event {
                "answer_delta" | "answer_final" => {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(data) {
                        if let Some(t) = v.get("text").and_then(|t| t.as_str()) {
                            print!("{}", t);
                            use std::io::Write;
                            let _ = std::io::stdout().flush();
                        }
                    }
                }
                "run_error" => {
                    had_error = true;
                    eprintln!("\n[error] {}", data);
                }
                "run_finished" => {
                    println!();
                }
                _ => {}
            }
        },
    )
    .await?;

    // Remember the session id so a later `--continue` can resume it.
    if let Some(sid) = new_session_id {
        remember_session(&sid);
    }
    if had_error {
        std::process::exit(1);
    }
    Ok(())
}

/// Persist the last-used session id (best-effort).
fn remember_session(session_id: &str) {
    let mut cfg = load_config().unwrap_or_default();
    if cfg.last_session_id.as_deref() != Some(session_id) {
        cfg.last_session_id = Some(session_id.to_string());
        let _ = save_config(&cfg);
    }
}

async fn cmd_sessions(op: SessionsOp) -> Result<()> {
    let cfg = effective_config()?;
    match op {
        SessionsOp::List => {
            let v = get_json(&cfg, "/v1/agent/sessions").await?;
            let empty = vec![];
            let sessions = v
                .get("sessions")
                .and_then(|s| s.as_array())
                .unwrap_or(&empty);
            if sessions.is_empty() {
                println!("(no sessions)");
            } else {
                for s in sessions {
                    let id = s.get("session_id").and_then(|x| x.as_str()).unwrap_or("?");
                    let turns = s.get("turn_count").and_then(|x| x.as_i64()).unwrap_or(0);
                    let last = s.get("last_active").and_then(|x| x.as_str()).unwrap_or("");
                    let title = s.get("title").and_then(|x| x.as_str()).unwrap_or("");
                    let src = s.get("source").and_then(|x| x.as_str()).unwrap_or("");
                    println!("{id}\tturns={turns}\tsource={src}\tlast_active={last}\t{title}");
                }
            }
        }
        SessionsOp::Show { id } => {
            let v = get_json(&cfg, &format!("/v1/agent/sessions/{id}")).await?;
            println!("{}", serde_json::to_string_pretty(&v)?);
        }
        SessionsOp::Turns { id } => {
            let v = get_json(&cfg, &format!("/v1/agent/sessions/{id}/turns")).await?;
            println!("{}", serde_json::to_string_pretty(&v)?);
        }
        SessionsOp::Delete { id } => {
            let v = delete_json(&cfg, &format!("/v1/agent/sessions/{id}")).await?;
            println!("{}", serde_json::to_string_pretty(&v)?);
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum SkillWhich {
    /// The `pensieve` CLI skill (recall/remember/query).
    Pensieve,
    /// The `pensieve-deploy` production-deployment skill.
    Deploy,
    /// Both skills.
    All,
}

async fn cmd_install_skill(
    target: Option<std::path::PathBuf>,
    also_link_claude: bool,
    which: SkillWhich,
) -> Result<()> {
    let skills: &[(&str, &str)] = match which {
        SkillWhich::Pensieve => &[("pensieve", SKILL_TEMPLATE)],
        SkillWhich::Deploy => &[("pensieve-deploy", DEPLOY_SKILL)],
        SkillWhich::All => &[("pensieve", SKILL_TEMPLATE), ("pensieve-deploy", DEPLOY_SKILL)],
    };
    if target.is_some() && skills.len() > 1 {
        anyhow::bail!("--target only works with a single skill (drop --which all)");
    }
    for (slug, body) in skills {
        install_one_skill(slug, body, target.clone(), also_link_claude)?;
    }
    Ok(())
}

fn install_one_skill(
    slug: &str,
    body: &str,
    target: Option<std::path::PathBuf>,
    also_link_claude: bool,
) -> Result<()> {
    let dir = match target {
        Some(p) => {
            std::fs::create_dir_all(&p).with_context(|| format!("mkdir {}", p.display()))?;
            p
        }
        None => {
            let d = client::config_dir()?.join("skills").join(slug);
            std::fs::create_dir_all(&d).with_context(|| format!("mkdir {}", d.display()))?;
            d
        }
    };
    let path = write_skill_file(&dir, body)?;
    println!("Wrote {}", path.display());

    if also_link_claude {
        #[cfg(unix)]
        {
            if let Some(home) = std::env::var_os("HOME") {
                let claude_skills = std::path::PathBuf::from(home)
                    .join(".claude")
                    .join("skills");
                if claude_skills.is_dir() {
                    let link = claude_skills.join(slug);
                    let _ = std::fs::remove_file(&link);
                    let _ = std::fs::remove_dir_all(&link);
                    std::os::unix::fs::symlink(&dir, &link).with_context(|| {
                        format!("symlink {} -> {}", link.display(), dir.display())
                    })?;
                    println!("Linked {} -> {}", link.display(), dir.display());
                } else {
                    println!(
                        "(skipped) {} doesn't exist; pass --also-link-claude later",
                        claude_skills.display()
                    );
                }
            }
        }
        #[cfg(not(unix))]
        {
            eprintln!(
                "note: --also-link-claude only works on Unix; copy the file manually on Windows"
            );
        }
    }
    Ok(())
}

// ── admin helpers ─────────────────────────────────────────────────────────────

async fn connect_catalog(url: &str) -> Result<Arc<dyn Catalog>> {
    let c = PostgresCatalog::connect(url)
        .await
        .with_context(|| format!("connecting to catalog {url}"))?;
    Ok(Arc::new(c))
}

async fn find_database_id(
    catalog: &Arc<dyn Catalog>,
    name: &str,
) -> Result<pensieve_core::types::DatabaseId> {
    // We don't have a `lookup_database` method yet. Workaround: try to
    // create-then-read by creating; if duplicate, look it up via a direct
    // pg query. For phase A we just try to create the database and if it
    // already exists, we query an arbitrary existing table to resolve the
    // database id — which won't work if there are no tables yet.
    //
    // Clean fix: add a `lookup_database` to the trait. For now, cheat via
    // the pool on PostgresCatalog. We downcast the Arc via Any trick.
    //
    // Simpler still: the CLI always runs against Postgres in phase A, so we
    // just run a direct query here. Accept the layering violation as a
    // phase-A expedient; proper fix = add `lookup_database` to the Catalog
    // trait (tracked as follow-up).
    let _ = catalog;
    let pool = sqlx::PgPool::connect(
        std::env::var("PENSIEVE_CATALOG_URL")
            .ok()
            .as_deref()
            .unwrap_or("postgres://pensieve:pensieve_dev@localhost:5433/pensieve"),
    )
    .await?;
    let row: Option<(uuid::Uuid,)> = sqlx::query_as("SELECT id FROM databases WHERE name = $1")
        .bind(name)
        .fetch_optional(&pool)
        .await?;
    let id = row
        .ok_or_else(|| anyhow!("database '{}' not found — create it first", name))?
        .0;
    Ok(pensieve_core::types::DatabaseId::from_uuid(id))
}

fn parse_schema_spec(spec: &str) -> Result<Schema> {
    let mut fields = Vec::new();
    for col in spec.split(',') {
        let col = col.trim();
        if col.is_empty() {
            continue;
        }
        let (name, ty) = col
            .split_once(':')
            .ok_or_else(|| anyhow!("column spec missing ':' — got '{col}'"))?;
        let name = name.trim();
        let ty = ty.trim();
        if name.is_empty() {
            return Err(anyhow!("empty column name in '{col}'"));
        }
        // Vector columns are non-nullable because null-vector ingest isn't
        // supported yet (the coercion path rejects serde_json::Value::Null).
        // All other columns default to nullable=true to match existing seed
        // scripts and the catalog's historical behaviour.
        let (data_type, nullable) = match ty {
            "bool" => (DataType::Boolean, true),
            "int" => (DataType::Int32, true),
            "long" => (DataType::Int64, true),
            "real" => (DataType::Float64, true),
            "string" => (DataType::Utf8, true),
            "timestamp" => (DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            "dynamic" => (DataType::Binary, true),
            other if other.starts_with("vector(") && other.ends_with(')') => {
                let inner = &other[7..other.len() - 1];
                let dim: i32 = inner.trim().parse().map_err(|_| {
                    anyhow!("vector(N): N must be a positive integer, got '{inner}'")
                })?;
                if dim <= 0 {
                    return Err(anyhow!("vector(N): N must be > 0, got {dim}"));
                }
                (
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Float32, false)),
                        dim,
                    ),
                    false,
                )
            }
            other => return Err(anyhow!("unsupported column type: {other}")),
        };
        fields.push(Field::new(name, data_type, nullable));
    }
    if fields.is_empty() {
        return Err(anyhow!("schema spec produced no fields"));
    }
    Ok(Schema::new(fields))
}
