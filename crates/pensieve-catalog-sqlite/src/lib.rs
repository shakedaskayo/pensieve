//! Embedded SQLite-backed catalog implementation.
//!
//! Implements [`pensieve_core::catalog::Catalog`] against a local SQLite database —
//! the storage backend for **local single-binary mode** (`pensieve local`), where
//! there is no Postgres/MinIO/HTTP layer. The metadata model mirrors the
//! Postgres catalog one-for-one (table → snapshot → manifest → extent), so the
//! engine, ingest, compaction, and memory layers run unchanged on top of it.
//!
//! # Dialect differences from the Postgres catalog
//!
//! * **UUIDs** are stored as BLOBs (sqlx's native `Uuid` encoding); ids are
//!   generated in Rust (`Uuid::new_v4()`) rather than via `RETURNING`.
//! * **Timestamps** are stored as RFC3339 TEXT (sqlx's native `DateTime<Utc>`
//!   encoding) — fixed-width, so lexicographic order is chronological.
//! * **JSON** columns (`column_stats`, `config`, `arrow_schema`, …) are stored
//!   as TEXT. There is no JSONB; **extent pruning is evaluated in Rust** in
//!   [`SqliteCatalog::list_extents_in_tenant`] instead of in SQL (no GIST/GIN).
//! * **No pgvector** — memory embeddings live in the columnar `memory_nodes`
//!   table (served by the engine), and the native columnar ANN prune
//!   (centroid+radius) is applied here as a Rust post-filter, exactly as the
//!   Postgres catalog does.
//! * **CAS commit** is the same optimistic compare-and-set; SQLite's
//!   file-level write serialization plus a `UNIQUE(table_id, sequence_number)`
//!   constraint provide the conflict detection.

#![forbid(unsafe_code)]

mod snapshot;

pub use snapshot::SqliteSnapshotTxn;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use pensieve_core::catalog::{
    cosine_distance_lower_bound, BackgroundTask, Catalog, CleanupResult, ColumnInfo, ColumnPrune,
    Dashboard, DashboardPanel, DashboardUpdate, DashboardWithPanels, ExtentManifest,
    GraphRegistration, GraphSpec, IngestLedgerEntry, LiveNode, NodeInfo, NodeLease, NodeRole,
    PrunePredicate, RefreshClaim, SnapshotTxn, TableConfig, TableEmbedConfig, TableRef,
    TokenPrincipal, User,
};
use pensieve_core::errors::{CatalogError, Result};
use pensieve_core::index_sidecar::{IndexSidecarDescriptor, SidecarKind};
use pensieve_core::tenant::{TenantId, DEFAULT_TENANT};
use pensieve_core::types::{DatabaseId, ExtentId, NodeId, SchemaSnapshotId, SnapshotId, TableId};
use serde_json::Value as Json;
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow, SqliteSynchronous,
};
use sqlx::{Row, Sqlite, SqlitePool, Transaction};
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

/// Rows dropped by [`SqliteCatalog::gc_history`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CatalogGcStats {
    pub snapshots_deleted: u64,
    pub manifests_deleted: u64,
    pub tasks_deleted: u64,
}

/// The embedded SQLite-backed catalog. Holds a connection pool; cloneable and
/// thread-safe.
#[derive(Debug, Clone)]
pub struct SqliteCatalog {
    pool: SqlitePool,
}

/// `BEGIN IMMEDIATE` so a writer waits on the SQLite lock instead of failing
/// with `SQLITE_BUSY_SNAPSHOT`. `busy_timeout` covers that wait; a deferred
/// transaction that upgrades to a write does not, and the local process has
/// several background writers (ingest, compaction, schedulers).
pub(crate) async fn begin_immediate(
    pool: &SqlitePool,
) -> sqlx::Result<Transaction<'static, Sqlite>> {
    let conn = pool.acquire().await?;
    Transaction::begin(conn, Some(std::borrow::Cow::Borrowed("BEGIN IMMEDIATE"))).await
}

impl SqliteCatalog {
    /// Open (creating if missing) a file-backed catalog at `path` and run the
    /// embedded schema. WAL mode + a busy timeout make concurrent in-process
    /// readers/writers behave under the single-binary workload.
    pub async fn connect(path: &str) -> Result<Self> {
        let opts = SqliteConnectOptions::from_str(&format!("sqlite://{path}"))
            .map_err(ce)?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            // WAL + NORMAL is the durable setting SQLite recommends for
            // concurrent readers. FULL fsyncs every commit and turns a
            // burst of small ingests into a disk-bound stall.
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(std::time::Duration::from_secs(10))
            .foreign_keys(false)
            .pragma("cache_size", "-65536")
            .pragma("temp_store", "MEMORY")
            .pragma("mmap_size", "268435456")
            .pragma("wal_autocheckpoint", "1000");
        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(opts)
            .await
            .map_err(ce)?;
        Self::from_pool(pool).await
    }

    /// Open an in-memory catalog (one shared connection). For tests and
    /// ephemeral local sessions.
    pub async fn connect_in_memory() -> Result<Self> {
        let opts = SqliteConnectOptions::from_str("sqlite::memory:")
            .map_err(ce)?
            .create_if_missing(true)
            .foreign_keys(false);
        // One connection so the in-memory database persists for the pool's life.
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(opts)
            .await
            .map_err(ce)?;
        Self::from_pool(pool).await
    }

    async fn from_pool(pool: SqlitePool) -> Result<Self> {
        // Additive upgrades for catalogs created before these columns existed,
        // applied *before* SCHEMA so its partial index can build. Failures are
        // "duplicate column name" (already upgraded) or "no such table"
        // (fresh DB — SCHEMA creates the full shape) — both safe to ignore.
        for ddl in [
            "ALTER TABLE users ADD COLUMN external_id TEXT",
            "ALTER TABLE users ADD COLUMN auth_provider TEXT",
        ] {
            let _ = sqlx::query(ddl).execute(&pool).await;
        }
        sqlx::raw_sql(SCHEMA)
            .execute(&pool)
            .await
            .map_err(|e| CatalogError::Sql(format!("schema init failed: {e}")))?;
        Ok(Self { pool })
    }

    /// Borrow the underlying pool (for the local-mode wiring and tests).
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Drop snapshot history the engine no longer reads.
    ///
    /// Every ingest inserts a snapshot and a manifest, and nothing else
    /// deletes them. Queries list live extents (`deleted_at IS NULL`), and a
    /// commit only needs the table's *current* snapshot row to compute the
    /// next sequence number. Left alone, a laptop capture stream grows the
    /// catalog without bound (observed: ~92k snapshots for 26 live extents,
    /// and a 234 MiB file that was mostly free pages).
    ///
    /// `min_age` keeps recently superseded snapshots so an in-flight commit
    /// can still read the parent it captured. `batch` caps each statement so
    /// one tick does not hold the write lock across the whole history.
    pub async fn gc_history(
        &self,
        min_age: chrono::Duration,
        batch: i64,
    ) -> Result<CatalogGcStats> {
        let cutoff = Utc::now() - min_age;
        let mut stats = CatalogGcStats::default();

        let manifests = sqlx::query(
            "DELETE FROM manifests WHERE snapshot_id IN (
                 SELECT id FROM snapshots
                 WHERE created_at < ?
                   AND id NOT IN (
                       SELECT current_snapshot_id FROM tables
                       WHERE current_snapshot_id IS NOT NULL
                   )
                 LIMIT ?
             )",
        )
        .bind(cutoff)
        .bind(batch)
        .execute(&self.pool)
        .await
        .map_err(ce)?;
        stats.manifests_deleted += manifests.rows_affected();

        let snapshots = sqlx::query(
            "DELETE FROM snapshots WHERE id IN (
                 SELECT id FROM snapshots
                 WHERE created_at < ?
                   AND id NOT IN (
                       SELECT current_snapshot_id FROM tables
                       WHERE current_snapshot_id IS NOT NULL
                   )
                 LIMIT ?
             )",
        )
        .bind(cutoff)
        .bind(batch)
        .execute(&self.pool)
        .await
        .map_err(ce)?;
        stats.snapshots_deleted += snapshots.rows_affected();

        // Manifests whose snapshot row is already gone (a previous tick
        // deleted the snapshot first, or a crash between the two deletes).
        let orphans = sqlx::query(
            "DELETE FROM manifests WHERE rowid IN (
                 SELECT m.rowid FROM manifests m
                 LEFT JOIN snapshots s ON s.id = m.snapshot_id
                 WHERE s.id IS NULL
                 LIMIT ?
             )",
        )
        .bind(batch)
        .execute(&self.pool)
        .await
        .map_err(ce)?;
        stats.manifests_deleted += orphans.rows_affected();

        let tasks = sqlx::query(
            "DELETE FROM background_tasks WHERE id IN (
                 SELECT id FROM background_tasks
                 WHERE status = 'done' AND updated_at < ?
                 LIMIT ?
             )",
        )
        .bind(cutoff)
        .bind(batch)
        .execute(&self.pool)
        .await
        .map_err(ce)?;
        stats.tasks_deleted += tasks.rows_affected();

        Ok(stats)
    }

    /// Rewrite the file when free pages dominate it. Best-effort: VACUUM
    /// needs a moment with no other writer, and a busy catalog skips it.
    /// Freelist pages are reused either way, so skipping does not leak.
    pub async fn reclaim_free_pages(&self) -> Result<bool> {
        let free: i64 = sqlx::query_scalar("PRAGMA freelist_count")
            .fetch_one(&self.pool)
            .await
            .map_err(ce)?;
        let pages: i64 = sqlx::query_scalar("PRAGMA page_count")
            .fetch_one(&self.pool)
            .await
            .map_err(ce)?;
        if free < 10_000 || pages == 0 || free * 2 < pages {
            return Ok(false);
        }
        sqlx::query("VACUUM")
            .execute(&self.pool)
            .await
            .map_err(ce)?;
        Ok(true)
    }

    /// Read a sync watermark / state value by key (e.g. memory push/pull
    /// watermarks). `None` if unset. Backs bidirectional memory sync.
    pub async fn get_sync_state(&self, key: &str) -> Result<Option<String>, CatalogError> {
        sqlx::query_scalar("SELECT watermark FROM sync_state WHERE key = ?")
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(ce)
    }

    /// Upsert a sync watermark / state value by key.
    pub async fn set_sync_state(&self, key: &str, value: &str) -> Result<(), CatalogError> {
        sqlx::query(
            "INSERT INTO sync_state (key, watermark) VALUES (?, ?) \
             ON CONFLICT (key) DO UPDATE SET watermark = excluded.watermark",
        )
        .bind(key)
        .bind(value)
        .execute(&self.pool)
        .await
        .map_err(ce)?;
        Ok(())
    }

    // ── local-mode dreaming run persistence (degraded mode) ──────────────────

    /// Upsert one dreaming run record. `run_json` is the Run JSON the HTTP
    /// layer serves; `trace_json` is the agent_runs conversation trace. Called
    /// both on insert (status `running`) and on finalize (terminal status), so
    /// it is an UPSERT keyed on the run id. Local-mode only.
    pub async fn upsert_dreaming_run(
        &self,
        run_id: Uuid,
        agent_run_id: Option<Uuid>,
        started_at: &str,
        run_json: &str,
        trace_json: &str,
    ) -> Result<(), CatalogError> {
        sqlx::query(
            "INSERT INTO local_dreaming_runs (id, agent_run_id, started_at, run_json, trace_json) \
             VALUES (?, ?, ?, ?, ?) \
             ON CONFLICT (id) DO UPDATE SET \
                agent_run_id = excluded.agent_run_id, \
                run_json = excluded.run_json, \
                trace_json = excluded.trace_json",
        )
        .bind(run_id)
        .bind(agent_run_id)
        .bind(started_at)
        .bind(run_json)
        .bind(trace_json)
        .execute(&self.pool)
        .await
        .map_err(ce)?;
        Ok(())
    }

    /// Load the most recent `limit` dreaming run records, newest first. Returns
    /// `(run_json, trace_json)` strings — the caller deserializes. Local-mode
    /// only.
    pub async fn recent_dreaming_runs(
        &self,
        limit: i64,
    ) -> Result<Vec<(String, String)>, CatalogError> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT run_json, trace_json FROM local_dreaming_runs \
             ORDER BY started_at DESC LIMIT ?",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(ce)?;
        Ok(rows)
    }
}

/// The embedded schema. Mirrors the Postgres migrations' columns; types are
/// SQLite-flavored (BLOB ids, TEXT json/timestamps). `IF NOT EXISTS` so reopen
/// is idempotent.
const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS databases (
    id          BLOB PRIMARY KEY,
    tenant_id   BLOB NOT NULL,
    name        TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    UNIQUE (tenant_id, name)
);

CREATE TABLE IF NOT EXISTS tables (
    id                  BLOB PRIMARY KEY,
    tenant_id           BLOB NOT NULL,
    database_id         BLOB NOT NULL,
    name                TEXT NOT NULL,
    current_snapshot_id BLOB,
    schema_snapshot_id  BLOB,
    config              TEXT NOT NULL DEFAULT '{}',
    created_at          TEXT NOT NULL,
    UNIQUE (tenant_id, database_id, name)
);

CREATE TABLE IF NOT EXISTS schema_snapshots (
    id           BLOB PRIMARY KEY,
    tenant_id    BLOB NOT NULL,
    table_id     BLOB NOT NULL,
    arrow_schema TEXT NOT NULL,
    created_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshots (
    id                 BLOB PRIMARY KEY,
    tenant_id          BLOB NOT NULL,
    table_id           BLOB NOT NULL,
    parent_id          BLOB,
    sequence_number    INTEGER NOT NULL,
    schema_snapshot_id BLOB NOT NULL,
    summary            TEXT NOT NULL DEFAULT '{}',
    created_at         TEXT NOT NULL,
    UNIQUE (table_id, sequence_number)
);

CREATE TABLE IF NOT EXISTS manifests (
    id           BLOB PRIMARY KEY,
    tenant_id    BLOB NOT NULL,
    snapshot_id  BLOB NOT NULL,
    kind         TEXT NOT NULL,
    extent_count INTEGER NOT NULL,
    byte_size    INTEGER NOT NULL,
    created_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS extents (
    id                 BLOB PRIMARY KEY,
    tenant_id          BLOB NOT NULL,
    table_id           BLOB NOT NULL,
    manifest_id        BLOB NOT NULL,
    schema_snapshot_id BLOB NOT NULL,
    object_path        TEXT NOT NULL,
    byte_size          INTEGER NOT NULL,
    row_count          INTEGER NOT NULL,
    min_timestamp      TEXT,
    max_timestamp      TEXT,
    column_stats       TEXT NOT NULL DEFAULT '{}',
    present_paths      TEXT NOT NULL DEFAULT '[]',
    compaction_gen     INTEGER NOT NULL DEFAULT 0,
    created_at         TEXT NOT NULL,
    deleted_at         TEXT
);
CREATE INDEX IF NOT EXISTS idx_extents_table ON extents (table_id, deleted_at);

-- Index sidecars (mirrors Postgres migration 028). Foreign keys are OFF on
-- this pool, so there is no ON DELETE CASCADE — delete_extent_rows and
-- cleanup_soft_deleted_extents delete these rows explicitly.
CREATE TABLE IF NOT EXISTS extent_indexes (
    id                 BLOB PRIMARY KEY,
    tenant_id          BLOB NOT NULL,
    table_id           BLOB NOT NULL,
    extent_id          BLOB NOT NULL,
    column_name        TEXT NOT NULL,
    kind               TEXT NOT NULL,
    object_path        TEXT NOT NULL,
    byte_size          INTEGER NOT NULL,
    params             TEXT NOT NULL DEFAULT '{}',
    embedding_model_id TEXT,
    created_at         TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS extent_indexes_uniq
    ON extent_indexes (extent_id, column_name, kind, COALESCE(embedding_model_id, ''));
CREATE INDEX IF NOT EXISTS extent_indexes_lookup
    ON extent_indexes (table_id, column_name, kind);

-- Global ANN centroid tree (S1.3). Local mode stores it for parity/tests but
-- never enqueues the maintenance job — server mode is where fan-out matters.
CREATE TABLE IF NOT EXISTS ann_tree (
    id                 BLOB PRIMARY KEY,
    tenant_id          BLOB NOT NULL,
    table_id           BLOB NOT NULL,
    column_name        TEXT NOT NULL,
    embedding_model_id TEXT,
    generation         INTEGER NOT NULL DEFAULT 0,
    extent_fingerprint TEXT NOT NULL,
    object_path        TEXT NOT NULL,
    byte_size          INTEGER NOT NULL,
    params             TEXT NOT NULL DEFAULT '{}',
    created_at         TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS ann_tree_uniq
    ON ann_tree (table_id, column_name, COALESCE(embedding_model_id, ''));

-- Per-tenant resource quotas (S2.6; mirrors Postgres migration 032). Local mode
-- is typically single-tenant, but the table exists for parity + tests so the
-- quota cache resolves identically across catalogs.
CREATE TABLE IF NOT EXISTS tenant_quotas (
    tenant_id            BLOB PRIMARY KEY,
    max_query_concurrent INTEGER,
    max_agent_concurrent INTEGER,
    updated_at           TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS background_tasks (
    id               BLOB PRIMARY KEY,
    tenant_id        BLOB NOT NULL,
    kind             TEXT NOT NULL,
    table_id         BLOB,
    payload          TEXT NOT NULL DEFAULT '{}',
    priority         INTEGER NOT NULL DEFAULT 0,
    status           TEXT NOT NULL DEFAULT 'pending',
    attempt          INTEGER NOT NULL DEFAULT 0,
    max_attempts     INTEGER NOT NULL DEFAULT 5,
    claim_expires_at TEXT,
    claimed_by       BLOB,
    last_error       TEXT,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tasks_claim ON background_tasks (kind, status, priority);

CREATE TABLE IF NOT EXISTS nodes (
    node_id        BLOB PRIMARY KEY,
    role           TEXT NOT NULL,
    endpoint       TEXT NOT NULL,
    capabilities   TEXT NOT NULL DEFAULT '{}',
    lease_id       BLOB NOT NULL,
    expires_at     TEXT NOT NULL,
    last_heartbeat TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dashboards (
    id                       BLOB PRIMARY KEY,
    tenant_id                BLOB NOT NULL,
    name                     TEXT NOT NULL,
    description              TEXT,
    time_range_preset        TEXT NOT NULL DEFAULT 'last_1h',
    refresh_interval_seconds INTEGER,
    created_at               TEXT NOT NULL,
    updated_at               TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dashboard_panels (
    id            BLOB PRIMARY KEY,
    tenant_id     BLOB NOT NULL,
    dashboard_id  BLOB NOT NULL,
    title         TEXT NOT NULL,
    panel_type    TEXT NOT NULL,
    query         TEXT,
    database_name TEXT,
    config        TEXT NOT NULL DEFAULT '{}',
    grid_x        INTEGER NOT NULL DEFAULT 0,
    grid_y        INTEGER NOT NULL DEFAULT 0,
    grid_w        INTEGER NOT NULL DEFAULT 6,
    grid_h        INTEGER NOT NULL DEFAULT 6,
    display_order INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_panels_dashboard ON dashboard_panels (dashboard_id);

CREATE TABLE IF NOT EXISTS graph_registrations (
    id          BLOB PRIMARY KEY,
    tenant_id   BLOB NOT NULL,
    database    TEXT NOT NULL,
    name        TEXT NOT NULL,
    node_table  TEXT NOT NULL,
    edge_table  TEXT NOT NULL,
    id_col      TEXT NOT NULL,
    label_col   TEXT NOT NULL,
    src_col     TEXT NOT NULL,
    dst_col     TEXT NOT NULL,
    type_col    TEXT NOT NULL,
    realm_col   TEXT,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    UNIQUE (tenant_id, database, name)
);

CREATE TABLE IF NOT EXISTS users (
    id            BLOB PRIMARY KEY,
    tenant_id     BLOB NOT NULL,
    username      TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    role          TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL,
    external_id   TEXT,
    auth_provider TEXT,
    UNIQUE (tenant_id, username)
);

CREATE UNIQUE INDEX IF NOT EXISTS users_external_identity_idx
    ON users (tenant_id, auth_provider, external_id)
    WHERE auth_provider IS NOT NULL AND external_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS api_tokens (
    token_hash   BLOB PRIMARY KEY,
    tenant_id    BLOB NOT NULL,
    scopes       TEXT NOT NULL,
    subject      TEXT,
    kind         TEXT NOT NULL DEFAULT 'api',
    expires_at   TEXT,
    revoked      INTEGER NOT NULL DEFAULT 0,
    session_id   BLOB,
    last_used_at TEXT,
    created_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ingest_ledger (
    tenant_id     BLOB NOT NULL,
    key           TEXT NOT NULL,
    table_id      BLOB NOT NULL,
    snapshot_id   BLOB NOT NULL,
    rows_ingested INTEGER NOT NULL,
    bytes_written INTEGER NOT NULL,
    applied_at    TEXT NOT NULL,
    expires_at    TEXT,
    PRIMARY KEY (tenant_id, key)
);

CREATE TABLE IF NOT EXISTS sync_state (
    key       TEXT PRIMARY KEY,
    watermark TEXT NOT NULL
);

-- Local-mode durable record of dreaming runs (degraded mode, no Postgres).
-- One row per finished run: `run_json` is the same Run shape the HTTP layer
-- serves (memory_pipeline_runs equivalent), `trace_json` is the agent_runs
-- conversation trace the UI drills into via /v1/agent/runs/:agent_run_id.
CREATE TABLE IF NOT EXISTS local_dreaming_runs (
    id           BLOB PRIMARY KEY,
    agent_run_id BLOB,
    started_at   TEXT NOT NULL,
    run_json     TEXT NOT NULL,
    trace_json   TEXT NOT NULL DEFAULT '[]'
);
CREATE INDEX IF NOT EXISTS idx_local_dreaming_started ON local_dreaming_runs (started_at);

-- Credentials store (local mode — mirrors Postgres schema; enc_value is
-- nonce(12) || ciphertext AES-256-GCM stored as a BLOB).
CREATE TABLE IF NOT EXISTS credentials (
    id          TEXT NOT NULL PRIMARY KEY,
    tenant_id   TEXT NOT NULL,
    label       TEXT NOT NULL,
    kind        TEXT NOT NULL,
    enc_value   BLOB NOT NULL,
    metadata    TEXT NOT NULL DEFAULT '{}',
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    UNIQUE (tenant_id, label)
);
CREATE INDEX IF NOT EXISTS idx_credentials_tenant ON credentials (tenant_id);

-- Data sources (local mode — mirrors Postgres connectors/data_sources schema).
CREATE TABLE IF NOT EXISTS data_sources (
    id                  TEXT NOT NULL PRIMARY KEY,
    tenant_id           TEXT NOT NULL,
    name                TEXT NOT NULL,
    type                TEXT NOT NULL,
    target_database     TEXT NOT NULL,
    target_table        TEXT NOT NULL DEFAULT '',
    config_json         TEXT NOT NULL DEFAULT '{}',
    schedule_ms         INTEGER NOT NULL,
    drive_model         TEXT NOT NULL DEFAULT 'periodic',
    enabled             INTEGER NOT NULL DEFAULT 1,
    disabled_reason     TEXT,
    last_run_at         TEXT,
    last_success_at     TEXT,
    last_error          TEXT,
    last_rows_ingested  INTEGER,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    UNIQUE (tenant_id, name)
);
CREATE INDEX IF NOT EXISTS idx_data_sources_tenant ON data_sources (tenant_id);
CREATE INDEX IF NOT EXISTS idx_data_sources_enabled ON data_sources (drive_model, enabled)
    WHERE enabled = 1;

-- Cursor state per data source (checkpoint for incremental sync).
CREATE TABLE IF NOT EXISTS data_source_cursors (
    data_source_id TEXT NOT NULL PRIMARY KEY REFERENCES data_sources(id) ON DELETE CASCADE,
    cursor_json    TEXT,
    updated_at     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS jobs (
    id               TEXT PRIMARY KEY,
    tenant_id        TEXT NOT NULL,
    kind             TEXT NOT NULL,
    payload          TEXT NOT NULL DEFAULT '{}',
    status           TEXT NOT NULL DEFAULT 'pending',
    priority         INTEGER NOT NULL DEFAULT 0,
    req_capabilities TEXT NOT NULL DEFAULT '[]',
    claimed_by       TEXT,
    claim_expires_at TEXT,
    attempt          INTEGER NOT NULL DEFAULT 0,
    max_attempts     INTEGER NOT NULL DEFAULT 3,
    progress         TEXT NOT NULL DEFAULT '{}',
    result           TEXT,
    last_error       TEXT,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS jobs_pending_idx ON jobs (kind, priority DESC, created_at)
    WHERE status = 'pending';
CREATE UNIQUE INDEX IF NOT EXISTS jobs_data_source_sync_uniq
    ON jobs (json_extract(payload, '$.data_source_id'), json_extract(payload, '$.scheduled_for'))
    WHERE kind = 'data_source_sync' AND status IN ('pending', 'claimed', 'running');

CREATE TABLE IF NOT EXISTS workers (
    id             TEXT PRIMARY KEY,
    tenant_id      TEXT NOT NULL,
    name           TEXT NOT NULL,
    hostname       TEXT,
    kind           TEXT NOT NULL DEFAULT 'embedded',
    capabilities   TEXT NOT NULL DEFAULT '[]',
    status         TEXT NOT NULL DEFAULT 'online',
    max_concurrent INTEGER NOT NULL DEFAULT 1,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL,
    UNIQUE (tenant_id, name)
);

-- Async embedding backfill pipeline (S1.5; mirrors Postgres migration 029).
-- Content-addressed embedding cache: the f32 vector is stored as raw
-- little-endian bytes (dim*4) in a BLOB. Ids are BLOBs to match the
-- extents/extent_indexes convention on this pool.
CREATE TABLE IF NOT EXISTS embedding_cache (
    tenant_id    BLOB NOT NULL,
    content_hash TEXT NOT NULL,
    model_id     TEXT NOT NULL,
    dim          INTEGER NOT NULL,
    embedding    BLOB NOT NULL,
    created_at   TEXT NOT NULL,
    PRIMARY KEY (tenant_id, content_hash, model_id)
);

-- Per-table "embed THIS text column INTO THAT vector column with THIS model".
CREATE TABLE IF NOT EXISTS table_embed_config (
    tenant_id        BLOB NOT NULL,
    table_id         BLOB NOT NULL,
    source_column    TEXT NOT NULL,
    embedding_column TEXT NOT NULL,
    model_id         TEXT NOT NULL,
    dim              INTEGER NOT NULL,
    auto_embed       INTEGER NOT NULL DEFAULT 1,
    updated_at       TEXT NOT NULL,
    PRIMARY KEY (tenant_id, table_id, embedding_column)
);
CREATE INDEX IF NOT EXISTS table_embed_config_table_idx
    ON table_embed_config (tenant_id, table_id);
"#;

#[async_trait]
impl Catalog for SqliteCatalog {
    fn as_ref_any(&self) -> &(dyn std::any::Any + Send + Sync) {
        self
    }

    // ------------------------- databases & tables -------------------------

    async fn create_database_in_tenant(&self, tenant: TenantId, name: &str) -> Result<DatabaseId> {
        let id = Uuid::new_v4();
        sqlx::query("INSERT INTO databases (id, tenant_id, name, created_at) VALUES (?,?,?,?)")
            .bind(id)
            .bind(tenant.as_uuid())
            .bind(name)
            .bind(Utc::now())
            .execute(&self.pool)
            .await
            .map_err(ce)?;
        Ok(DatabaseId::from_uuid(id))
    }

    async fn lookup_database_in_tenant(
        &self,
        tenant: TenantId,
        name: &str,
    ) -> Result<Option<DatabaseId>> {
        let id: Option<Uuid> =
            sqlx::query_scalar("SELECT id FROM databases WHERE tenant_id = ? AND name = ?")
                .bind(tenant.as_uuid())
                .bind(name)
                .fetch_optional(&self.pool)
                .await
                .map_err(ce)?;
        Ok(id.map(DatabaseId::from_uuid))
    }

    async fn create_table_in_tenant(
        &self,
        tenant: TenantId,
        database_id: DatabaseId,
        name: &str,
        schema: Arc<arrow_schema::Schema>,
        config: TableConfig,
    ) -> Result<TableId> {
        let schema_json = schema_to_json(&schema)?.to_string();
        let config_json =
            serde_json::to_string(&config).map_err(|e| CatalogError::Sql(e.to_string()))?;

        let mut tx = begin_immediate(&self.pool).await.map_err(ce)?;

        // 0. Verify the database belongs to this tenant.
        let db_tenant: Option<Uuid> =
            sqlx::query_scalar("SELECT tenant_id FROM databases WHERE id = ?")
                .bind(database_id.as_uuid())
                .fetch_optional(&mut *tx)
                .await
                .map_err(ce)?;
        let db_tenant = db_tenant
            .ok_or_else(|| CatalogError::Sql(format!("database {database_id} not found")))?;
        if db_tenant != tenant.as_uuid() {
            return Err(CatalogError::Sql(format!(
                "database {database_id} does not belong to tenant {tenant}"
            ))
            .into());
        }

        // 1. Insert the table with NULL snapshot pointers (bootstrap).
        let table_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO tables (id, tenant_id, database_id, name, config, created_at)
             VALUES (?,?,?,?,?,?)",
        )
        .bind(table_id)
        .bind(tenant.as_uuid())
        .bind(database_id.as_uuid())
        .bind(name)
        .bind(&config_json)
        .bind(Utc::now())
        .execute(&mut *tx)
        .await
        .map_err(ce)?;

        // 2. Insert the initial schema snapshot.
        let schema_snap_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO schema_snapshots (id, tenant_id, table_id, arrow_schema, created_at)
             VALUES (?,?,?,?,?)",
        )
        .bind(schema_snap_id)
        .bind(tenant.as_uuid())
        .bind(table_id)
        .bind(&schema_json)
        .bind(Utc::now())
        .execute(&mut *tx)
        .await
        .map_err(ce)?;

        // 3. Insert snapshot #0 (empty).
        let snap_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO snapshots
                (id, tenant_id, table_id, parent_id, sequence_number, schema_snapshot_id, summary, created_at)
             VALUES (?,?,?,NULL,0,?,?,?)",
        )
        .bind(snap_id)
        .bind(tenant.as_uuid())
        .bind(table_id)
        .bind(schema_snap_id)
        .bind(serde_json::json!({ "operation": "bootstrap" }).to_string())
        .bind(Utc::now())
        .execute(&mut *tx)
        .await
        .map_err(ce)?;

        // 4. Point the table at the initial snapshot + schema.
        sqlx::query(
            "UPDATE tables SET current_snapshot_id = ?, schema_snapshot_id = ? WHERE id = ?",
        )
        .bind(snap_id)
        .bind(schema_snap_id)
        .bind(table_id)
        .execute(&mut *tx)
        .await
        .map_err(ce)?;

        tx.commit().await.map_err(ce)?;
        Ok(TableId::from_uuid(table_id))
    }

    async fn lookup_table_in_tenant(
        &self,
        tenant: TenantId,
        database: &str,
        name: &str,
    ) -> Result<TableRef> {
        let row = sqlx::query(
            "SELECT t.id AS id, t.database_id AS database_id, t.current_snapshot_id AS current_snapshot_id,
                    t.schema_snapshot_id AS schema_snapshot_id, ss.arrow_schema AS arrow_schema, t.config AS config
             FROM tables t
             JOIN databases d ON d.id = t.database_id AND d.tenant_id = ?
             LEFT JOIN schema_snapshots ss ON ss.id = t.schema_snapshot_id
             WHERE t.tenant_id = ? AND d.name = ? AND t.name = ?",
        )
        .bind(tenant.as_uuid())
        .bind(tenant.as_uuid())
        .bind(database)
        .bind(name)
        .fetch_optional(&self.pool)
        .await
        .map_err(ce)?
        .ok_or_else(|| CatalogError::TableNotFound {
            database: database.to_owned(),
            name: name.to_owned(),
        })?;

        let current_snapshot_id: Option<Uuid> = row.try_get("current_snapshot_id").map_err(ce)?;
        let schema_snapshot_id: Option<Uuid> = row.try_get("schema_snapshot_id").map_err(ce)?;
        let schema_str: String = row.try_get("arrow_schema").map_err(ce)?;
        let config_str: String = row.try_get("config").map_err(ce)?;

        let schema = json_to_schema(&parse_json(&schema_str)?)?;
        let config: TableConfig = serde_json::from_str(&config_str).unwrap_or_default();
        let current_snapshot_id = current_snapshot_id.ok_or_else(|| {
            CatalogError::Sql("table has no current snapshot (bootstrap row)".into())
        })?;
        let schema_snapshot_id = schema_snapshot_id
            .ok_or_else(|| CatalogError::Sql("table has no schema snapshot".into()))?;

        Ok(TableRef {
            id: TableId::from_uuid(row.try_get::<Uuid, _>("id").map_err(ce)?),
            database_id: DatabaseId::from_uuid(row.try_get::<Uuid, _>("database_id").map_err(ce)?),
            name: name.to_owned(),
            current_snapshot_id: SnapshotId::from_uuid(current_snapshot_id),
            schema_snapshot_id: SchemaSnapshotId::from_uuid(schema_snapshot_id),
            schema,
            config,
        })
    }

    async fn list_tables_in_database_in_tenant(
        &self,
        tenant: TenantId,
        database: &str,
    ) -> Result<Vec<TableRef>> {
        let rows = sqlx::query(
            "SELECT t.id AS id, t.database_id AS database_id, t.name AS name,
                    t.current_snapshot_id AS current_snapshot_id, t.schema_snapshot_id AS schema_snapshot_id,
                    ss.arrow_schema AS arrow_schema, t.config AS config
             FROM tables t
             JOIN databases d ON d.id = t.database_id AND d.tenant_id = ?
             LEFT JOIN schema_snapshots ss ON ss.id = t.schema_snapshot_id
             WHERE t.tenant_id = ? AND d.name = ?
             ORDER BY t.name",
        )
        .bind(tenant.as_uuid())
        .bind(tenant.as_uuid())
        .bind(database)
        .fetch_all(&self.pool)
        .await
        .map_err(ce)?;

        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            let current_snapshot_id: Option<Uuid> =
                row.try_get("current_snapshot_id").map_err(ce)?;
            let schema_snapshot_id: Option<Uuid> = row.try_get("schema_snapshot_id").map_err(ce)?;
            let schema_str: Option<String> = row.try_get("arrow_schema").map_err(ce)?;
            let config_str: String = row.try_get("config").map_err(ce)?;

            let (Some(schema_str), Some(current_snapshot_id), Some(schema_snapshot_id)) =
                (schema_str, current_snapshot_id, schema_snapshot_id)
            else {
                continue; // bootstrap glitch window
            };
            let schema = json_to_schema(&parse_json(&schema_str)?)?;
            let config: TableConfig = serde_json::from_str(&config_str).unwrap_or_default();
            out.push(TableRef {
                id: TableId::from_uuid(row.try_get::<Uuid, _>("id").map_err(ce)?),
                database_id: DatabaseId::from_uuid(
                    row.try_get::<Uuid, _>("database_id").map_err(ce)?,
                ),
                name: row.try_get::<String, _>("name").map_err(ce)?,
                current_snapshot_id: SnapshotId::from_uuid(current_snapshot_id),
                schema_snapshot_id: SchemaSnapshotId::from_uuid(schema_snapshot_id),
                schema,
                config,
            });
        }
        Ok(out)
    }

    async fn drop_table_in_tenant(
        &self,
        tenant: TenantId,
        database: &str,
        name: &str,
    ) -> Result<bool> {
        // SQLite schema declares no FK cascades — delete children explicitly.
        let id: Option<Uuid> = sqlx::query_scalar(
            "SELECT t.id FROM tables t
             JOIN databases d ON d.id = t.database_id AND d.tenant_id = ?
             WHERE t.tenant_id = ? AND d.name = ? AND t.name = ?",
        )
        .bind(tenant.as_uuid())
        .bind(tenant.as_uuid())
        .bind(database)
        .bind(name)
        .fetch_optional(&self.pool)
        .await
        .map_err(ce)?;
        let Some(id) = id else { return Ok(false) };

        let mut tx = begin_immediate(&self.pool).await.map_err(ce)?;
        sqlx::query("DELETE FROM extents WHERE table_id = ?")
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(ce)?;
        sqlx::query(
            "DELETE FROM manifests WHERE snapshot_id IN (SELECT id FROM snapshots WHERE table_id = ?)",
        )
        .bind(id)
        .execute(&mut *tx)
        .await
        .map_err(ce)?;
        sqlx::query("DELETE FROM snapshots WHERE table_id = ?")
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(ce)?;
        sqlx::query("DELETE FROM schema_snapshots WHERE table_id = ?")
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(ce)?;
        sqlx::query("DELETE FROM tables WHERE id = ?")
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(ce)?;
        tx.commit().await.map_err(ce)?;
        Ok(true)
    }

    // ------------------------- schema-listing (UI) -------------------------

    async fn list_databases_in_tenant(
        &self,
        tenant: TenantId,
    ) -> Result<Vec<String>, CatalogError> {
        let rows: Vec<String> =
            sqlx::query_scalar("SELECT name FROM databases WHERE tenant_id = ? ORDER BY name")
                .bind(tenant.as_uuid())
                .fetch_all(&self.pool)
                .await
                .map_err(ce)?;
        Ok(rows)
    }

    async fn list_tables_in_tenant(
        &self,
        tenant: TenantId,
        database: &str,
    ) -> Result<Vec<String>, CatalogError> {
        let rows: Vec<String> = sqlx::query_scalar(
            "SELECT t.name FROM tables t
             JOIN databases d ON d.id = t.database_id AND d.tenant_id = ?
             WHERE t.tenant_id = ? AND d.name = ?
             ORDER BY t.name",
        )
        .bind(tenant.as_uuid())
        .bind(tenant.as_uuid())
        .bind(database)
        .fetch_all(&self.pool)
        .await
        .map_err(ce)?;
        Ok(rows)
    }

    async fn get_table_columns_in_tenant(
        &self,
        tenant: TenantId,
        database: &str,
        table: &str,
    ) -> Result<Vec<ColumnInfo>, CatalogError> {
        let db_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM databases WHERE tenant_id = ? AND name = ?)",
        )
        .bind(tenant.as_uuid())
        .bind(database)
        .fetch_one(&self.pool)
        .await
        .map_err(ce)?;
        if !db_exists {
            return Err(CatalogError::DatabaseNotFound(database.to_string()));
        }

        let schema_str: Option<String> = sqlx::query_scalar(
            "SELECT ss.arrow_schema
             FROM tables t
             JOIN databases d ON d.id = t.database_id AND d.tenant_id = ?
             JOIN schema_snapshots ss ON ss.id = t.schema_snapshot_id
             WHERE t.tenant_id = ? AND d.name = ? AND t.name = ?",
        )
        .bind(tenant.as_uuid())
        .bind(tenant.as_uuid())
        .bind(database)
        .bind(table)
        .fetch_optional(&self.pool)
        .await
        .map_err(ce)?;

        let schema_str = schema_str.ok_or_else(|| CatalogError::TableNotFound {
            database: database.to_owned(),
            name: table.to_owned(),
        })?;
        let schema_json = parse_json(&schema_str)?;
        let fields = schema_json
            .get("fields")
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                CatalogError::Sql("malformed arrow_schema: missing fields array".into())
            })?;

        let mut columns = Vec::with_capacity(fields.len());
        for f in fields {
            let name = f
                .get("name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| CatalogError::Sql("field missing name".into()))?
                .to_owned();
            let col_type = f
                .get("type")
                .and_then(|v| v.as_str())
                .ok_or_else(|| CatalogError::Sql("field missing type".into()))?
                .to_owned();
            let nullable = f.get("nullable").and_then(|v| v.as_bool()).unwrap_or(true);
            columns.push(ColumnInfo {
                name,
                r#type: col_type,
                nullable,
            });
        }
        Ok(columns)
    }

    async fn alter_table_add_column(
        &self,
        table_id: TableId,
        column_name: &str,
        column_type: &str,
    ) -> Result<SchemaSnapshotId> {
        let _ = string_to_arrow_type(column_type)?; // reject garbage early

        let mut tx = begin_immediate(&self.pool).await.map_err(ce)?;

        let row = sqlx::query(
            "SELECT t.tenant_id AS tenant_id, t.schema_snapshot_id AS schema_snapshot_id,
                    ss.arrow_schema AS arrow_schema
             FROM tables t
             JOIN schema_snapshots ss ON ss.id = t.schema_snapshot_id
             WHERE t.id = ?",
        )
        .bind(table_id.as_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(ce)?
        .ok_or_else(|| CatalogError::Sql(format!("table {table_id} not found")))?;

        let tenant_id: Uuid = row.try_get("tenant_id").map_err(ce)?;
        let current_schema_snapshot_id: Uuid = row.try_get("schema_snapshot_id").map_err(ce)?;
        let schema_str: String = row.try_get("arrow_schema").map_err(ce)?;
        let mut schema_json = parse_json(&schema_str)?;

        if let Some(fields) = schema_json.get("fields").and_then(|v| v.as_array()) {
            if fields
                .iter()
                .any(|f| f.get("name").and_then(|n| n.as_str()) == Some(column_name))
            {
                return Err(
                    CatalogError::Sql(format!("column {column_name} already exists")).into(),
                );
            }
        }
        if let Some(arr) = schema_json.get_mut("fields").and_then(|v| v.as_array_mut()) {
            arr.push(serde_json::json!({
                "name": column_name,
                "type": column_type,
                "nullable": true,
            }));
        } else {
            return Err(CatalogError::Sql("existing schema has no `fields` array".into()).into());
        }

        let new_schema_snap_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO schema_snapshots (id, tenant_id, table_id, arrow_schema, created_at)
             VALUES (?,?,?,?,?)",
        )
        .bind(new_schema_snap_id)
        .bind(tenant_id)
        .bind(table_id.as_uuid())
        .bind(schema_json.to_string())
        .bind(Utc::now())
        .execute(&mut *tx)
        .await
        .map_err(ce)?;

        let swapped = sqlx::query(
            "UPDATE tables SET schema_snapshot_id = ? WHERE id = ? AND schema_snapshot_id = ?",
        )
        .bind(new_schema_snap_id)
        .bind(table_id.as_uuid())
        .bind(current_schema_snapshot_id)
        .execute(&mut *tx)
        .await
        .map_err(ce)?;
        if swapped.rows_affected() != 1 {
            return Err(CatalogError::Conflict.into());
        }

        tx.commit().await.map_err(ce)?;
        Ok(SchemaSnapshotId::from_uuid(new_schema_snap_id))
    }

    // ------------------------- snapshot transactions -------------------------

    async fn begin_snapshot_in_tenant(
        &self,
        tenant: TenantId,
        table_id: TableId,
    ) -> Result<Box<dyn SnapshotTxn>> {
        let row = sqlx::query(
            "SELECT current_snapshot_id, schema_snapshot_id
             FROM tables WHERE tenant_id = ? AND id = ?",
        )
        .bind(tenant.as_uuid())
        .bind(table_id.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(ce)?
        .ok_or_else(|| {
            CatalogError::Sql(format!(
                "table {table_id} not found in tenant {tenant} for begin_snapshot"
            ))
        })?;

        let parent: Uuid = row
            .try_get::<Option<Uuid>, _>("current_snapshot_id")
            .map_err(ce)?
            .ok_or_else(|| CatalogError::Sql("table has no current_snapshot_id".into()))?;
        let schema_snap: Uuid = row
            .try_get::<Option<Uuid>, _>("schema_snapshot_id")
            .map_err(ce)?
            .ok_or_else(|| CatalogError::Sql("table has no schema_snapshot_id".into()))?;

        Ok(Box::new(SqliteSnapshotTxn::new(
            self.pool.clone(),
            tenant,
            table_id,
            SnapshotId::from_uuid(parent),
            SchemaSnapshotId::from_uuid(schema_snap),
        )))
    }

    // ------------------------- planner support -------------------------

    async fn list_extents_in_tenant(
        &self,
        tenant: TenantId,
        table_id: TableId,
        _snapshot: SnapshotId,
        prune: &PrunePredicate,
    ) -> Result<Vec<ExtentManifest>> {
        // Read all live extents; apply every prune predicate in Rust (no
        // JSONB/GIST/GIN in SQLite). Matches the Postgres catalog's results.
        let rows = sqlx::query(
            "SELECT id, table_id, schema_snapshot_id, object_path, byte_size, row_count,
                    min_timestamp, max_timestamp, column_stats, present_paths,
                    compaction_gen, created_at
             FROM extents
             WHERE tenant_id = ? AND table_id = ? AND deleted_at IS NULL",
        )
        .bind(tenant.as_uuid())
        .bind(table_id.as_uuid())
        .fetch_all(&self.pool)
        .await
        .map_err(ce)?;

        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            let column_stats_str: String = row.try_get("column_stats").map_err(ce)?;
            let present_paths_str: String = row.try_get("present_paths").map_err(ce)?;
            out.push(ExtentManifest {
                id: ExtentId::from_uuid(row.try_get::<Uuid, _>("id").map_err(ce)?),
                table_id: TableId::from_uuid(row.try_get::<Uuid, _>("table_id").map_err(ce)?),
                schema_snapshot_id: SchemaSnapshotId::from_uuid(
                    row.try_get::<Uuid, _>("schema_snapshot_id").map_err(ce)?,
                ),
                object_path: row.try_get("object_path").map_err(ce)?,
                byte_size: row.try_get::<i64, _>("byte_size").map_err(ce)? as u64,
                row_count: row.try_get::<i64, _>("row_count").map_err(ce)? as u64,
                min_timestamp: row.try_get("min_timestamp").map_err(ce)?,
                max_timestamp: row.try_get("max_timestamp").map_err(ce)?,
                column_stats: parse_json(&column_stats_str).unwrap_or(Json::Null),
                present_paths: serde_json::from_str(&present_paths_str).unwrap_or_default(),
                compaction_gen: row.try_get::<i64, _>("compaction_gen").map_err(ce)? as u32,
                created_at: row.try_get("created_at").map_err(ce)?,
            });
        }

        // --- Rust-side pruning (conservative: never a false negative) ---
        if let Some(tr) = &prune.time_range {
            out.retain(|m| match (m.min_timestamp, m.max_timestamp) {
                (Some(min), Some(max)) => max >= tr.start_inclusive && min < tr.end_exclusive,
                _ => true, // unknown bounds → keep
            });
        }
        if !prune.required_paths.is_empty() {
            out.retain(|m| {
                prune
                    .required_paths
                    .iter()
                    .all(|p| m.present_paths.contains(p))
            });
        }
        for (col, pred) in &prune.column_predicates {
            match pred {
                ColumnPrune::Equals(v) => {
                    out.retain(|m| match distinct_array(&m.column_stats, col) {
                        Some(arr) => arr.iter().any(|x| x == v),
                        None => true,
                    })
                }
                ColumnPrune::InSet(vs) => {
                    out.retain(|m| match distinct_array(&m.column_stats, col) {
                        Some(arr) => vs.iter().any(|v| arr.iter().any(|x| x == v)),
                        None => true,
                    })
                }
                ColumnPrune::Between { low, high } => {
                    out.retain(|m| match distinct_array(&m.column_stats, col) {
                        Some(arr) => arr.iter().any(|x| in_range(x, low, high)),
                        None => true,
                    });
                }
                ColumnPrune::ContainsTokens(tokens) => {
                    out.retain(|m| match token_array(&m.column_stats, col) {
                        Some(toks) => tokens.iter().all(|t| toks.iter().any(|x| x == t)),
                        None => true,
                    });
                }
                ColumnPrune::VectorDistance { query, threshold } => {
                    out.retain(|m| keep_extent_by_vector(&m.column_stats, col, query, *threshold));
                }
            }
        }

        // ORDER BY min_timestamp DESC NULLS LAST.
        out.sort_by(|a, b| match (b.min_timestamp, a.min_timestamp) {
            (Some(bt), Some(at)) => bt.cmp(&at),
            (Some(_), None) => std::cmp::Ordering::Greater,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (None, None) => std::cmp::Ordering::Equal,
        });
        Ok(out)
    }

    // ------------------------- index sidecars -------------------------

    async fn register_index_sidecar(
        &self,
        tenant: TenantId,
        desc: &IndexSidecarDescriptor,
    ) -> Result<()> {
        // Dialect-safe upsert (no expression conflict targets): UPDATE the
        // logical key first; INSERT when nothing matched. Single-process
        // local mode makes the update/insert window benign.
        let updated = sqlx::query(
            "UPDATE extent_indexes
             SET object_path = ?, byte_size = ?, params = ?, created_at = ?
             WHERE tenant_id = ? AND extent_id = ? AND column_name = ? AND kind = ?
               AND COALESCE(embedding_model_id, '') = COALESCE(?, '')",
        )
        .bind(&desc.object_path)
        .bind(desc.byte_size as i64)
        .bind(desc.params.to_string())
        .bind(desc.created_at)
        .bind(tenant.as_uuid())
        .bind(desc.extent_id.as_uuid())
        .bind(&desc.column)
        .bind(desc.kind.as_str())
        .bind(desc.embedding_model_id.as_deref())
        .execute(&self.pool)
        .await
        .map_err(ce)?;
        if updated.rows_affected() > 0 {
            return Ok(());
        }
        sqlx::query(
            "INSERT INTO extent_indexes
                (id, tenant_id, table_id, extent_id, column_name, kind,
                 object_path, byte_size, params, embedding_model_id, created_at)
             VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        )
        .bind(desc.id)
        .bind(tenant.as_uuid())
        .bind(desc.table_id.as_uuid())
        .bind(desc.extent_id.as_uuid())
        .bind(&desc.column)
        .bind(desc.kind.as_str())
        .bind(&desc.object_path)
        .bind(desc.byte_size as i64)
        .bind(desc.params.to_string())
        .bind(desc.embedding_model_id.as_deref())
        .bind(desc.created_at)
        .execute(&self.pool)
        .await
        .map_err(ce)?;
        Ok(())
    }

    async fn list_index_sidecars(
        &self,
        tenant: TenantId,
        table_id: TableId,
        extent_ids: &[ExtentId],
        kind: Option<SidecarKind>,
    ) -> Result<Vec<IndexSidecarDescriptor>> {
        if extent_ids.is_empty() {
            return Ok(Vec::new());
        }
        let placeholders = vec!["?"; extent_ids.len()].join(",");
        let mut sql = format!(
            "SELECT id, table_id, extent_id, column_name, kind, object_path,
                    byte_size, params, embedding_model_id, created_at
             FROM extent_indexes
             WHERE tenant_id = ? AND table_id = ? AND extent_id IN ({placeholders})"
        );
        if kind.is_some() {
            sql.push_str(" AND kind = ?");
        }
        sql.push_str(" ORDER BY extent_id, column_name, kind");

        let mut q = sqlx::query(&sql)
            .bind(tenant.as_uuid())
            .bind(table_id.as_uuid());
        for e in extent_ids {
            q = q.bind(*e.as_uuid());
        }
        if let Some(k) = kind {
            q = q.bind(k.as_str());
        }
        let rows = q.fetch_all(&self.pool).await.map_err(ce)?;
        rows.iter().map(row_to_sidecar).collect()
    }

    async fn delete_index_sidecars_for_extents(
        &self,
        tenant: TenantId,
        extent_ids: &[ExtentId],
    ) -> Result<Vec<String>> {
        if extent_ids.is_empty() {
            return Ok(Vec::new());
        }
        let placeholders = vec!["?"; extent_ids.len()].join(",");
        let sql = format!(
            "DELETE FROM extent_indexes
             WHERE tenant_id = ? AND extent_id IN ({placeholders})
             RETURNING object_path"
        );
        let mut q = sqlx::query_scalar::<_, String>(&sql).bind(tenant.as_uuid());
        for e in extent_ids {
            q = q.bind(*e.as_uuid());
        }
        let paths = q.fetch_all(&self.pool).await.map_err(ce)?;
        Ok(paths)
    }

    // ----------------------- global ANN centroid tree -----------------------

    async fn upsert_ann_tree(
        &self,
        tenant: TenantId,
        desc: &pensieve_core::index_sidecar::AnnTreeDescriptor,
    ) -> Result<()> {
        // UPDATE-then-INSERT upsert on (table, column, model) — single-process
        // local mode makes the window benign (same pattern as sidecars).
        let updated = sqlx::query(
            "UPDATE ann_tree
             SET generation = ?, extent_fingerprint = ?, object_path = ?,
                 byte_size = ?, params = ?, created_at = ?
             WHERE tenant_id = ? AND table_id = ? AND column_name = ?
               AND COALESCE(embedding_model_id, '') = COALESCE(?, '')",
        )
        .bind(desc.generation)
        .bind(&desc.extent_fingerprint)
        .bind(&desc.object_path)
        .bind(desc.byte_size as i64)
        .bind(desc.params.to_string())
        .bind(desc.created_at)
        .bind(tenant.as_uuid())
        .bind(desc.table_id.as_uuid())
        .bind(&desc.column)
        .bind(desc.embedding_model_id.as_deref())
        .execute(&self.pool)
        .await
        .map_err(ce)?;
        if updated.rows_affected() > 0 {
            return Ok(());
        }
        sqlx::query(
            "INSERT INTO ann_tree
                (id, tenant_id, table_id, column_name, embedding_model_id,
                 generation, extent_fingerprint, object_path, byte_size, params, created_at)
             VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        )
        .bind(desc.id)
        .bind(tenant.as_uuid())
        .bind(desc.table_id.as_uuid())
        .bind(&desc.column)
        .bind(desc.embedding_model_id.as_deref())
        .bind(desc.generation)
        .bind(&desc.extent_fingerprint)
        .bind(&desc.object_path)
        .bind(desc.byte_size as i64)
        .bind(desc.params.to_string())
        .bind(desc.created_at)
        .execute(&self.pool)
        .await
        .map_err(ce)?;
        Ok(())
    }

    async fn get_ann_tree(
        &self,
        tenant: TenantId,
        table_id: TableId,
        column: &str,
        embedding_model_id: Option<&str>,
    ) -> Result<Option<pensieve_core::index_sidecar::AnnTreeDescriptor>> {
        let row = sqlx::query(
            "SELECT id, table_id, column_name, embedding_model_id, generation,
                    extent_fingerprint, object_path, byte_size, params, created_at
             FROM ann_tree
             WHERE tenant_id = ? AND table_id = ? AND column_name = ?
               AND COALESCE(embedding_model_id, '') = COALESCE(?, '')",
        )
        .bind(tenant.as_uuid())
        .bind(table_id.as_uuid())
        .bind(column)
        .bind(embedding_model_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(ce)?;
        row.as_ref().map(row_to_ann_tree).transpose()
    }

    async fn delete_ann_tree(
        &self,
        tenant: TenantId,
        table_id: TableId,
        column: &str,
    ) -> Result<Vec<String>> {
        let paths = sqlx::query_scalar::<_, String>(
            "DELETE FROM ann_tree
             WHERE tenant_id = ? AND table_id = ? AND column_name = ?
             RETURNING object_path",
        )
        .bind(tenant.as_uuid())
        .bind(table_id.as_uuid())
        .bind(column)
        .fetch_all(&self.pool)
        .await
        .map_err(ce)?;
        Ok(paths)
    }

    // ------------------------- per-tenant quotas (S2.6) -------------------------

    async fn upsert_tenant_quota(&self, quota: &pensieve_core::catalog::TenantQuota) -> Result<()> {
        let now = Utc::now();
        let updated = sqlx::query(
            "UPDATE tenant_quotas
             SET max_query_concurrent = ?, max_agent_concurrent = ?, updated_at = ?
             WHERE tenant_id = ?",
        )
        .bind(quota.max_query_concurrent.map(|v| v as i64))
        .bind(quota.max_agent_concurrent.map(|v| v as i64))
        .bind(now)
        .bind(quota.tenant.as_uuid())
        .execute(&self.pool)
        .await
        .map_err(ce)?;
        if updated.rows_affected() > 0 {
            return Ok(());
        }
        sqlx::query(
            "INSERT INTO tenant_quotas
                (tenant_id, max_query_concurrent, max_agent_concurrent, updated_at)
             VALUES (?,?,?,?)",
        )
        .bind(quota.tenant.as_uuid())
        .bind(quota.max_query_concurrent.map(|v| v as i64))
        .bind(quota.max_agent_concurrent.map(|v| v as i64))
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(ce)?;
        Ok(())
    }

    async fn get_tenant_quota(
        &self,
        tenant: TenantId,
    ) -> Result<Option<pensieve_core::catalog::TenantQuota>> {
        let row: Option<(Option<i64>, Option<i64>, DateTime<Utc>)> = sqlx::query_as(
            "SELECT max_query_concurrent, max_agent_concurrent, updated_at
             FROM tenant_quotas WHERE tenant_id = ?",
        )
        .bind(tenant.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(ce)?;
        Ok(
            row.map(|(q, a, updated_at)| pensieve_core::catalog::TenantQuota {
                tenant,
                max_query_concurrent: q.map(|v| v.max(0) as u32),
                max_agent_concurrent: a.map(|v| v.max(0) as u32),
                updated_at,
            }),
        )
    }

    async fn list_tenant_quotas(&self) -> Result<Vec<pensieve_core::catalog::TenantQuota>> {
        let rows: Vec<(Uuid, Option<i64>, Option<i64>, DateTime<Utc>)> = sqlx::query_as(
            "SELECT tenant_id, max_query_concurrent, max_agent_concurrent, updated_at
             FROM tenant_quotas",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(ce)?;
        Ok(rows
            .into_iter()
            .map(
                |(t, q, a, updated_at)| pensieve_core::catalog::TenantQuota {
                    tenant: TenantId::from_uuid(t),
                    max_query_concurrent: q.map(|v| v.max(0) as u32),
                    max_agent_concurrent: a.map(|v| v.max(0) as u32),
                    updated_at,
                },
            )
            .collect())
    }

    // ------------------------- garbage collection -------------------------

    async fn gc_candidates(&self, before: DateTime<Utc>) -> Result<Vec<ExtentId>> {
        let ids: Vec<Uuid> = sqlx::query_scalar(
            "SELECT id FROM extents WHERE deleted_at IS NOT NULL AND deleted_at < ?",
        )
        .bind(before)
        .fetch_all(&self.pool)
        .await
        .map_err(ce)?;
        Ok(ids.into_iter().map(ExtentId::from_uuid).collect())
    }

    async fn compaction_candidates(
        &self,
        small_bytes: i64,
        min_extents: i64,
        max_merge: i64,
    ) -> Result<Vec<(TableId, Vec<ExtentId>)>> {
        use sqlx::Row as _;
        // SQLite (bundled ≥3.46) supports window functions. Per table: the
        // smallest `max_merge` live extents under `small_bytes`, only for tables
        // with ≥ `min_extents` such extents. Tenant-scoped like every query here.
        let rows = sqlx::query(
            "WITH small AS (
                SELECT table_id, id, byte_size,
                       row_number() OVER (PARTITION BY table_id ORDER BY byte_size) AS rnk,
                       count(*)      OVER (PARTITION BY table_id)                   AS total
                FROM extents
                WHERE deleted_at IS NULL
                  AND tenant_id = ?
                  AND byte_size < ?
             )
             SELECT table_id, id
             FROM small
             WHERE total >= ? AND rnk <= ?
             ORDER BY table_id, byte_size",
        )
        .bind(DEFAULT_TENANT.as_uuid())
        .bind(small_bytes)
        .bind(min_extents)
        .bind(max_merge)
        .fetch_all(&self.pool)
        .await
        .map_err(ce)?;

        let mut by_table: std::collections::BTreeMap<Uuid, Vec<ExtentId>> =
            std::collections::BTreeMap::new();
        for row in rows {
            let tid: Uuid = row.try_get("table_id").map_err(ce)?;
            let eid: Uuid = row.try_get("id").map_err(ce)?;
            by_table
                .entry(tid)
                .or_default()
                .push(ExtentId::from_uuid(eid));
        }
        Ok(by_table
            .into_iter()
            .filter(|(_, v)| v.len() >= 2)
            .map(|(tid, eids)| (TableId::from_uuid(tid), eids))
            .collect())
    }

    async fn delete_extent_rows(&self, extents: &[ExtentId]) -> Result<()> {
        if extents.is_empty() {
            return Ok(());
        }
        let placeholders = vec!["?"; extents.len()].join(",");
        // Foreign keys are off on this pool, so the extent_indexes rows
        // (Postgres: ON DELETE CASCADE) are deleted explicitly first.
        let sidecar_sql = format!("DELETE FROM extent_indexes WHERE extent_id IN ({placeholders})");
        let mut sq = sqlx::query(&sidecar_sql);
        for e in extents {
            sq = sq.bind(*e.as_uuid());
        }
        sq.execute(&self.pool).await.map_err(ce)?;
        let sql = format!("DELETE FROM extents WHERE id IN ({placeholders})");
        let mut q = sqlx::query(&sql);
        for e in extents {
            q = q.bind(*e.as_uuid());
        }
        q.execute(&self.pool).await.map_err(ce)?;
        Ok(())
    }

    async fn cleanup_soft_deleted_extents_in_tenant(
        &self,
        tenant: TenantId,
        database: &str,
        table: &str,
        before: DateTime<Utc>,
    ) -> Result<CleanupResult> {
        // Resolve table id.
        let table_id: Option<Uuid> = sqlx::query_scalar(
            "SELECT t.id FROM tables t
             JOIN databases d ON d.id = t.database_id AND d.tenant_id = ?
             WHERE t.tenant_id = ? AND d.name = ? AND t.name = ?",
        )
        .bind(tenant.as_uuid())
        .bind(tenant.as_uuid())
        .bind(database)
        .bind(table)
        .fetch_optional(&self.pool)
        .await
        .map_err(ce)?;
        let Some(table_id) = table_id else {
            return Ok(CleanupResult {
                extents_deleted: 0,
                rows_freed: 0,
                bytes_freed: 0,
            });
        };

        let agg: Option<(i64, i64, i64)> = sqlx::query_as(
            "SELECT COUNT(*), COALESCE(SUM(row_count),0), COALESCE(SUM(byte_size),0)
             FROM extents
             WHERE tenant_id = ? AND table_id = ? AND deleted_at IS NOT NULL AND deleted_at < ?",
        )
        .bind(tenant.as_uuid())
        .bind(table_id)
        .bind(before)
        .fetch_optional(&self.pool)
        .await
        .map_err(ce)?;
        let (count, rows, bytes) = agg.unwrap_or((0, 0, 0));

        // No FK cascade on this pool: drop the sidecar rows of the extents
        // about to be hard-deleted.
        sqlx::query(
            "DELETE FROM extent_indexes WHERE extent_id IN (
                 SELECT id FROM extents
                 WHERE tenant_id = ? AND table_id = ? AND deleted_at IS NOT NULL AND deleted_at < ?
             )",
        )
        .bind(tenant.as_uuid())
        .bind(table_id)
        .bind(before)
        .execute(&self.pool)
        .await
        .map_err(ce)?;

        sqlx::query(
            "DELETE FROM extents
             WHERE tenant_id = ? AND table_id = ? AND deleted_at IS NOT NULL AND deleted_at < ?",
        )
        .bind(tenant.as_uuid())
        .bind(table_id)
        .bind(before)
        .execute(&self.pool)
        .await
        .map_err(ce)?;

        Ok(CleanupResult {
            extents_deleted: count as u64,
            rows_freed: rows as u64,
            bytes_freed: bytes as u64,
        })
    }

    // ------------------------- node identity & heartbeat -------------------------

    async fn register_node(&self, info: NodeInfo) -> Result<NodeLease> {
        let node_id = NodeId::new();
        let lease_id = Uuid::new_v4();
        let now = Utc::now();
        let expires_at = now + chrono::Duration::seconds(30);
        sqlx::query(
            "INSERT INTO nodes (node_id, role, endpoint, capabilities, lease_id, expires_at, last_heartbeat)
             VALUES (?,?,?,?,?,?,?)",
        )
        .bind(node_id.as_uuid())
        .bind(role_to_str(info.role))
        .bind(&info.endpoint)
        .bind(info.capabilities.to_string())
        .bind(lease_id)
        .bind(expires_at)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(ce)?;
        Ok(NodeLease {
            node_id,
            lease_id,
            expires_at,
        })
    }

    async fn heartbeat(&self, lease: &NodeLease) -> Result<()> {
        let now = Utc::now();
        let expires_at = now + chrono::Duration::seconds(30);
        sqlx::query(
            "UPDATE nodes SET last_heartbeat = ?, expires_at = ? WHERE node_id = ? AND lease_id = ?",
        )
        .bind(now)
        .bind(expires_at)
        .bind(lease.node_id.as_uuid())
        .bind(lease.lease_id)
        .execute(&self.pool)
        .await
        .map_err(ce)?;
        Ok(())
    }

    async fn deregister_node(&self, lease: NodeLease) -> Result<()> {
        sqlx::query("DELETE FROM nodes WHERE node_id = ? AND lease_id = ?")
            .bind(lease.node_id.as_uuid())
            .bind(lease.lease_id)
            .execute(&self.pool)
            .await
            .map_err(ce)?;
        Ok(())
    }

    async fn list_live_nodes(&self, max_stale_secs: u32) -> Result<Vec<LiveNode>> {
        let cutoff = Utc::now() - chrono::Duration::seconds(i64::from(max_stale_secs));
        let rows = sqlx::query(
            "SELECT node_id, role, endpoint, last_heartbeat
             FROM nodes WHERE last_heartbeat >= ?",
        )
        .bind(cutoff)
        .fetch_all(&self.pool)
        .await
        .map_err(ce)?;
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            out.push(LiveNode {
                node_id: NodeId::from_uuid(row.try_get::<Uuid, _>("node_id").map_err(ce)?),
                role: str_to_role(&row.try_get::<String, _>("role").map_err(ce)?),
                endpoint: row.try_get("endpoint").map_err(ce)?,
                last_heartbeat: row.try_get("last_heartbeat").map_err(ce)?,
            });
        }
        Ok(out)
    }

    // ------------------------- background task queue -------------------------

    async fn submit_task(
        &self,
        kind: &str,
        table_id: Option<TableId>,
        payload: serde_json::Value,
        priority: i32,
    ) -> Result<uuid::Uuid> {
        let id = Uuid::new_v4();
        let now = Utc::now();
        sqlx::query(
            "INSERT INTO background_tasks
                (id, tenant_id, kind, table_id, payload, priority, status, attempt, max_attempts, created_at, updated_at)
             VALUES (?,?,?,?,?,?, 'pending', 0, 5, ?, ?)",
        )
        .bind(id)
        .bind(DEFAULT_TENANT.as_uuid())
        .bind(kind)
        .bind(table_id.map(|t| *t.as_uuid()))
        .bind(payload.to_string())
        .bind(priority)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(ce)?;
        Ok(id)
    }

    async fn claim_task(
        &self,
        kind: &str,
        node_id: NodeId,
        lease: chrono::Duration,
    ) -> Result<Option<BackgroundTask>> {
        let now = Utc::now();
        let mut tx = begin_immediate(&self.pool).await.map_err(ce)?;
        // Pick the next runnable task: pending, or a running task whose claim expired.
        let row = sqlx::query(
            "SELECT id, kind, table_id, payload, priority, attempt, max_attempts
             FROM background_tasks
             WHERE kind = ? AND status IN ('pending','running')
               AND (claim_expires_at IS NULL OR claim_expires_at < ?)
             ORDER BY priority DESC, created_at ASC
             LIMIT 1",
        )
        .bind(kind)
        .bind(now)
        .fetch_optional(&mut *tx)
        .await
        .map_err(ce)?;

        let Some(row) = row else {
            tx.commit().await.map_err(ce)?;
            return Ok(None);
        };

        let id: Uuid = row.try_get("id").map_err(ce)?;
        let attempt: i64 = row.try_get("attempt").map_err(ce)?;
        let new_attempt = attempt + 1;
        let claim_expires_at = now + lease;

        let claimed = sqlx::query(
            "UPDATE background_tasks
             SET status = 'running', attempt = ?, claim_expires_at = ?, claimed_by = ?, updated_at = ?
             WHERE id = ? AND status IN ('pending','running')",
        )
        .bind(new_attempt)
        .bind(claim_expires_at)
        .bind(node_id.as_uuid())
        .bind(now)
        .bind(id)
        .execute(&mut *tx)
        .await
        .map_err(ce)?;
        tx.commit().await.map_err(ce)?;

        if claimed.rows_affected() != 1 {
            return Ok(None);
        }

        let payload_str: String = row.try_get("payload").map_err(ce)?;
        Ok(Some(BackgroundTask {
            id,
            kind: row.try_get("kind").map_err(ce)?,
            table_id: row
                .try_get::<Option<Uuid>, _>("table_id")
                .map_err(ce)?
                .map(TableId::from_uuid),
            payload: parse_json(&payload_str).unwrap_or(Json::Null),
            priority: row.try_get::<i64, _>("priority").map_err(ce)? as i32,
            attempt: new_attempt as i32,
            max_attempts: row.try_get::<i64, _>("max_attempts").map_err(ce)? as i32,
            claim_expires_at,
        }))
    }

    async fn complete_task(&self, task_id: uuid::Uuid) -> Result<()> {
        sqlx::query("UPDATE background_tasks SET status = 'done', updated_at = ? WHERE id = ?")
            .bind(Utc::now())
            .bind(task_id)
            .execute(&self.pool)
            .await
            .map_err(ce)?;
        Ok(())
    }

    async fn fail_task(&self, task_id: uuid::Uuid, error: &str) -> Result<()> {
        // Requeue if attempt < max_attempts, else mark failed.
        sqlx::query(
            "UPDATE background_tasks
             SET status = CASE WHEN attempt < max_attempts THEN 'pending' ELSE 'failed' END,
                 claim_expires_at = NULL,
                 last_error = ?,
                 updated_at = ?
             WHERE id = ?",
        )
        .bind(error)
        .bind(Utc::now())
        .bind(task_id)
        .execute(&self.pool)
        .await
        .map_err(ce)?;
        Ok(())
    }

    // ------------------------- dashboards -------------------------

    async fn create_dashboard_in_tenant(
        &self,
        tenant: TenantId,
        name: &str,
        description: Option<&str>,
    ) -> Result<Dashboard, CatalogError> {
        let id = Uuid::new_v4();
        let now = Utc::now();
        sqlx::query(
            "INSERT INTO dashboards (id, tenant_id, name, description, time_range_preset, refresh_interval_seconds, created_at, updated_at)
             VALUES (?,?,?,?, 'last_1h', NULL, ?, ?)",
        )
        .bind(id)
        .bind(tenant.as_uuid())
        .bind(name)
        .bind(description)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(ce)?;
        Ok(Dashboard {
            id,
            name: name.to_owned(),
            description: description.map(ToOwned::to_owned),
            time_range_preset: "last_1h".to_owned(),
            refresh_interval_seconds: None,
            created_at: now,
            updated_at: now,
        })
    }

    async fn list_dashboards_in_tenant(
        &self,
        tenant: TenantId,
    ) -> Result<Vec<Dashboard>, CatalogError> {
        let rows = sqlx::query(
            "SELECT id, name, description, time_range_preset, refresh_interval_seconds, created_at, updated_at
             FROM dashboards WHERE tenant_id = ? ORDER BY name",
        )
        .bind(tenant.as_uuid())
        .fetch_all(&self.pool)
        .await
        .map_err(ce)?;
        rows.iter().map(row_to_dashboard).collect()
    }

    async fn get_dashboard_in_tenant(
        &self,
        tenant: TenantId,
        id: uuid::Uuid,
    ) -> Result<Option<DashboardWithPanels>, CatalogError> {
        let maybe = sqlx::query(
            "SELECT id, name, description, time_range_preset, refresh_interval_seconds, created_at, updated_at
             FROM dashboards WHERE tenant_id = ? AND id = ?",
        )
        .bind(tenant.as_uuid())
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(ce)?;
        let Some(row) = maybe else { return Ok(None) };
        let dashboard = row_to_dashboard(&row)?;

        let panel_rows = sqlx::query(
            "SELECT id, dashboard_id, title, panel_type, query, database_name, config,
                    grid_x, grid_y, grid_w, grid_h, display_order
             FROM dashboard_panels WHERE tenant_id = ? AND dashboard_id = ?
             ORDER BY display_order",
        )
        .bind(tenant.as_uuid())
        .bind(id)
        .fetch_all(&self.pool)
        .await
        .map_err(ce)?;
        let panels = panel_rows
            .iter()
            .map(row_to_panel)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Some(DashboardWithPanels { dashboard, panels }))
    }

    async fn update_dashboard_in_tenant(
        &self,
        tenant: TenantId,
        id: uuid::Uuid,
        patch: DashboardUpdate,
    ) -> Result<Dashboard, CatalogError> {
        let mut tx = begin_immediate(&self.pool).await.map_err(ce)?;
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM dashboards WHERE tenant_id = ? AND id = ?)",
        )
        .bind(tenant.as_uuid())
        .bind(id)
        .fetch_one(&mut *tx)
        .await
        .map_err(ce)?;
        if !exists {
            return Err(CatalogError::Sql(format!("dashboard {id} not found")));
        }

        if let Some(name) = &patch.name {
            sqlx::query("UPDATE dashboards SET name = ? WHERE tenant_id = ? AND id = ?")
                .bind(name)
                .bind(tenant.as_uuid())
                .bind(id)
                .execute(&mut *tx)
                .await
                .map_err(ce)?;
        }
        if let Some(description) = &patch.description {
            sqlx::query("UPDATE dashboards SET description = ? WHERE tenant_id = ? AND id = ?")
                .bind(description.as_deref())
                .bind(tenant.as_uuid())
                .bind(id)
                .execute(&mut *tx)
                .await
                .map_err(ce)?;
        }
        if let Some(preset) = &patch.time_range_preset {
            sqlx::query(
                "UPDATE dashboards SET time_range_preset = ? WHERE tenant_id = ? AND id = ?",
            )
            .bind(preset)
            .bind(tenant.as_uuid())
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(ce)?;
        }
        if let Some(refresh) = &patch.refresh_interval_seconds {
            sqlx::query(
                "UPDATE dashboards SET refresh_interval_seconds = ? WHERE tenant_id = ? AND id = ?",
            )
            .bind(*refresh)
            .bind(tenant.as_uuid())
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(ce)?;
        }
        sqlx::query("UPDATE dashboards SET updated_at = ? WHERE tenant_id = ? AND id = ?")
            .bind(Utc::now())
            .bind(tenant.as_uuid())
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(ce)?;

        if let Some(panels) = &patch.panels {
            sqlx::query("DELETE FROM dashboard_panels WHERE tenant_id = ? AND dashboard_id = ?")
                .bind(tenant.as_uuid())
                .bind(id)
                .execute(&mut *tx)
                .await
                .map_err(ce)?;
            for panel in panels {
                let panel_id = panel.id.unwrap_or_else(Uuid::new_v4);
                sqlx::query(
                    "INSERT INTO dashboard_panels
                        (id, tenant_id, dashboard_id, title, panel_type, query, database_name,
                         config, grid_x, grid_y, grid_w, grid_h, display_order)
                     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                )
                .bind(panel_id)
                .bind(tenant.as_uuid())
                .bind(id)
                .bind(&panel.title)
                .bind(&panel.panel_type)
                .bind(panel.query.as_deref())
                .bind(panel.database_name.as_deref())
                .bind(panel.config.to_string())
                .bind(panel.grid_x)
                .bind(panel.grid_y)
                .bind(panel.grid_w)
                .bind(panel.grid_h)
                .bind(panel.display_order)
                .execute(&mut *tx)
                .await
                .map_err(ce)?;
            }
        }

        let row = sqlx::query(
            "SELECT id, name, description, time_range_preset, refresh_interval_seconds, created_at, updated_at
             FROM dashboards WHERE tenant_id = ? AND id = ?",
        )
        .bind(tenant.as_uuid())
        .bind(id)
        .fetch_one(&mut *tx)
        .await
        .map_err(ce)?;
        let dashboard = row_to_dashboard(&row)?;
        tx.commit().await.map_err(ce)?;
        Ok(dashboard)
    }

    async fn delete_dashboard_in_tenant(
        &self,
        tenant: TenantId,
        id: uuid::Uuid,
    ) -> Result<bool, CatalogError> {
        let mut tx = begin_immediate(&self.pool).await.map_err(ce)?;
        sqlx::query("DELETE FROM dashboard_panels WHERE tenant_id = ? AND dashboard_id = ?")
            .bind(tenant.as_uuid())
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(ce)?;
        let res = sqlx::query("DELETE FROM dashboards WHERE tenant_id = ? AND id = ?")
            .bind(tenant.as_uuid())
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(ce)?;
        tx.commit().await.map_err(ce)?;
        Ok(res.rows_affected() > 0)
    }

    // ------------------------- graphs -------------------------

    async fn create_graph_in_tenant(
        &self,
        tenant: TenantId,
        database: &str,
        name: &str,
        spec: GraphSpec,
    ) -> Result<GraphRegistration, CatalogError> {
        let id = Uuid::new_v4();
        let now = Utc::now();
        sqlx::query(
            "INSERT INTO graph_registrations
                (id, tenant_id, database, name, node_table, edge_table,
                 id_col, label_col, src_col, dst_col, type_col, realm_col, created_at, updated_at)
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        )
        .bind(id)
        .bind(tenant.as_uuid())
        .bind(database)
        .bind(name)
        .bind(&spec.node_table)
        .bind(&spec.edge_table)
        .bind(&spec.id_col)
        .bind(&spec.label_col)
        .bind(&spec.src_col)
        .bind(&spec.dst_col)
        .bind(&spec.type_col)
        .bind(spec.realm_col.as_deref())
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(ce)?;
        Ok(GraphRegistration {
            id,
            database: database.to_owned(),
            name: name.to_owned(),
            node_table: spec.node_table,
            edge_table: spec.edge_table,
            id_col: spec.id_col,
            label_col: spec.label_col,
            src_col: spec.src_col,
            dst_col: spec.dst_col,
            type_col: spec.type_col,
            realm_col: spec.realm_col,
            created_at: now,
            updated_at: now,
        })
    }

    async fn list_graphs_in_tenant(
        &self,
        tenant: TenantId,
        database: &str,
    ) -> Result<Vec<GraphRegistration>, CatalogError> {
        let rows = sqlx::query(
            "SELECT id, database, name, node_table, edge_table,
                    id_col, label_col, src_col, dst_col, type_col, realm_col, created_at, updated_at
             FROM graph_registrations WHERE tenant_id = ? AND database = ? ORDER BY name",
        )
        .bind(tenant.as_uuid())
        .bind(database)
        .fetch_all(&self.pool)
        .await
        .map_err(ce)?;
        rows.iter().map(row_to_graph).collect()
    }

    async fn get_graph_in_tenant(
        &self,
        tenant: TenantId,
        database: &str,
        name: &str,
    ) -> Result<Option<GraphRegistration>, CatalogError> {
        let maybe = sqlx::query(
            "SELECT id, database, name, node_table, edge_table,
                    id_col, label_col, src_col, dst_col, type_col, realm_col, created_at, updated_at
             FROM graph_registrations WHERE tenant_id = ? AND database = ? AND name = ?",
        )
        .bind(tenant.as_uuid())
        .bind(database)
        .bind(name)
        .fetch_optional(&self.pool)
        .await
        .map_err(ce)?;
        match maybe {
            Some(row) => Ok(Some(row_to_graph(&row)?)),
            None => Ok(None),
        }
    }

    async fn drop_graph_in_tenant(
        &self,
        tenant: TenantId,
        database: &str,
        name: &str,
    ) -> Result<bool, CatalogError> {
        let res = sqlx::query(
            "DELETE FROM graph_registrations WHERE tenant_id = ? AND database = ? AND name = ?",
        )
        .bind(tenant.as_uuid())
        .bind(database)
        .bind(name)
        .execute(&self.pool)
        .await
        .map_err(ce)?;
        Ok(res.rows_affected() > 0)
    }

    // ------------------------- auth: users -------------------------

    async fn create_user_in_tenant(
        &self,
        tenant: TenantId,
        username: &str,
        password_hash: &str,
        role: &str,
    ) -> Result<User, CatalogError> {
        let id = Uuid::new_v4();
        let now = Utc::now();
        sqlx::query(
            "INSERT INTO users (id, tenant_id, username, password_hash, role, created_at, updated_at)
             VALUES (?,?,?,?,?,?,?)",
        )
        .bind(id)
        .bind(tenant.as_uuid())
        .bind(username)
        .bind(password_hash)
        .bind(role)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(ce)?;
        Ok(User {
            id,
            username: username.to_owned(),
            role: role.to_owned(),
            created_at: now,
            updated_at: now,
        })
    }

    async fn get_user_with_hash_in_tenant(
        &self,
        tenant: TenantId,
        username: &str,
    ) -> Result<Option<(User, String)>, CatalogError> {
        let maybe = sqlx::query(
            "SELECT id, username, role, password_hash, created_at, updated_at
             FROM users WHERE tenant_id = ? AND username = ?",
        )
        .bind(tenant.as_uuid())
        .bind(username)
        .fetch_optional(&self.pool)
        .await
        .map_err(ce)?;
        match maybe {
            Some(row) => {
                let user = row_to_user(&row)?;
                let hash: String = row.try_get("password_hash").map_err(ce)?;
                Ok(Some((user, hash)))
            }
            None => Ok(None),
        }
    }

    async fn count_users_in_tenant(&self, tenant: TenantId) -> Result<u64, CatalogError> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users WHERE tenant_id = ?")
            .bind(tenant.as_uuid())
            .fetch_one(&self.pool)
            .await
            .map_err(ce)?;
        Ok(count as u64)
    }

    async fn list_users_in_tenant(&self, tenant: TenantId) -> Result<Vec<User>, CatalogError> {
        let rows = sqlx::query(
            "SELECT id, username, role, created_at, updated_at
             FROM users WHERE tenant_id = ? ORDER BY username",
        )
        .bind(tenant.as_uuid())
        .fetch_all(&self.pool)
        .await
        .map_err(ce)?;
        rows.iter().map(row_to_user).collect()
    }

    async fn set_user_password_in_tenant(
        &self,
        tenant: TenantId,
        username: &str,
        password_hash: &str,
    ) -> Result<bool, CatalogError> {
        let res = sqlx::query(
            "UPDATE users SET password_hash = ?, updated_at = ? WHERE tenant_id = ? AND username = ?",
        )
        .bind(password_hash)
        .bind(Utc::now())
        .bind(tenant.as_uuid())
        .bind(username)
        .execute(&self.pool)
        .await
        .map_err(ce)?;
        Ok(res.rows_affected() > 0)
    }

    async fn set_user_role_in_tenant(
        &self,
        tenant: TenantId,
        username: &str,
        role: &str,
    ) -> Result<bool, CatalogError> {
        let res = sqlx::query(
            "UPDATE users SET role = ?, updated_at = ? WHERE tenant_id = ? AND username = ?",
        )
        .bind(role)
        .bind(Utc::now())
        .bind(tenant.as_uuid())
        .bind(username)
        .execute(&self.pool)
        .await
        .map_err(ce)?;
        Ok(res.rows_affected() > 0)
    }

    async fn delete_user_in_tenant(
        &self,
        tenant: TenantId,
        username: &str,
    ) -> Result<bool, CatalogError> {
        let res = sqlx::query("DELETE FROM users WHERE tenant_id = ? AND username = ?")
            .bind(tenant.as_uuid())
            .bind(username)
            .execute(&self.pool)
            .await
            .map_err(ce)?;
        Ok(res.rows_affected() > 0)
    }

    // -------------- auth: external identities (JIT provisioning) --------------

    async fn upsert_external_user_in_tenant(
        &self,
        tenant: TenantId,
        provider: &str,
        external_id: &str,
        username: &str,
        role: &str,
    ) -> Result<User, CatalogError> {
        let id = Uuid::new_v4();
        let now = Utc::now();
        // '!external' is deliberately not a valid PHC string: password
        // verification can never succeed for JIT-provisioned users.
        sqlx::query(
            "INSERT INTO users (id, tenant_id, username, password_hash, role, created_at, updated_at, auth_provider, external_id)
             VALUES (?,?,?,'!external',?,?,?,?,?)
             ON CONFLICT (tenant_id, auth_provider, external_id)
               WHERE auth_provider IS NOT NULL AND external_id IS NOT NULL
             DO UPDATE SET username = excluded.username,
                           role = excluded.role,
                           updated_at = excluded.updated_at",
        )
        .bind(id)
        .bind(tenant.as_uuid())
        .bind(username)
        .bind(role)
        .bind(now)
        .bind(now)
        .bind(provider)
        .bind(external_id)
        .execute(&self.pool)
        .await
        .map_err(ce)?;
        // Read back: covers both the freshly-created and refreshed paths.
        let row = sqlx::query(
            "SELECT id, username, role, created_at, updated_at
             FROM users WHERE tenant_id = ? AND auth_provider = ? AND external_id = ?",
        )
        .bind(tenant.as_uuid())
        .bind(provider)
        .bind(external_id)
        .fetch_one(&self.pool)
        .await
        .map_err(ce)?;
        row_to_user(&row)
    }

    // ------------------------- auth: api tokens -------------------------

    async fn insert_api_token_in_tenant(
        &self,
        tenant: TenantId,
        token_hash: &[u8],
        scopes: &str,
        subject: Option<&str>,
        kind: &str,
        expires_at: Option<DateTime<Utc>>,
    ) -> Result<(), CatalogError> {
        sqlx::query(
            "INSERT INTO api_tokens (token_hash, tenant_id, scopes, subject, kind, expires_at, revoked, created_at)
             VALUES (?,?,?,?,?,?,0,?)",
        )
        .bind(token_hash)
        .bind(tenant.as_uuid())
        .bind(scopes)
        .bind(subject)
        .bind(kind)
        .bind(expires_at)
        .bind(Utc::now())
        .execute(&self.pool)
        .await
        .map_err(ce)?;
        Ok(())
    }

    async fn lookup_api_token(
        &self,
        token_hash: &[u8],
    ) -> Result<Option<TokenPrincipal>, CatalogError> {
        let now = Utc::now();
        let maybe = sqlx::query(
            "SELECT tenant_id, scopes, subject FROM api_tokens
             WHERE token_hash = ? AND revoked = 0 AND kind != 'refresh'
               AND (expires_at IS NULL OR expires_at > ?)",
        )
        .bind(token_hash)
        .bind(now)
        .fetch_optional(&self.pool)
        .await
        .map_err(ce)?;
        let Some(row) = maybe else { return Ok(None) };

        // Fire-and-forget last_used_at bump.
        let _ = sqlx::query("UPDATE api_tokens SET last_used_at = ? WHERE token_hash = ?")
            .bind(now)
            .bind(token_hash)
            .execute(&self.pool)
            .await;

        Ok(Some(TokenPrincipal {
            tenant: TenantId::from_uuid(row.try_get::<Uuid, _>("tenant_id").map_err(ce)?),
            role: row.try_get("scopes").map_err(ce)?,
            subject: row.try_get("subject").map_err(ce)?,
        }))
    }

    async fn revoke_api_token(&self, token_hash: &[u8]) -> Result<bool, CatalogError> {
        let res =
            sqlx::query("UPDATE api_tokens SET revoked = 1 WHERE token_hash = ? AND revoked = 0")
                .bind(token_hash)
                .execute(&self.pool)
                .await
                .map_err(ce)?;
        Ok(res.rows_affected() > 0)
    }

    async fn list_api_tokens_in_tenant(
        &self,
        tenant: TenantId,
        kind: &str,
    ) -> Result<Vec<pensieve_core::catalog::ApiTokenInfo>, CatalogError> {
        let rows = sqlx::query(
            "SELECT token_hash, scopes, subject, kind, created_at, last_used_at,
                    expires_at, revoked
             FROM api_tokens
             WHERE tenant_id = ? AND kind = ?
             ORDER BY created_at DESC",
        )
        .bind(tenant.as_uuid())
        .bind(kind)
        .fetch_all(&self.pool)
        .await
        .map_err(ce)?;
        rows.iter()
            .map(|row| {
                Ok(pensieve_core::catalog::ApiTokenInfo {
                    token_hash: row.try_get("token_hash").map_err(ce)?,
                    role: row.try_get("scopes").map_err(ce)?,
                    subject: row.try_get("subject").map_err(ce)?,
                    kind: row.try_get("kind").map_err(ce)?,
                    created_at: row.try_get("created_at").map_err(ce)?,
                    last_used_at: row.try_get("last_used_at").map_err(ce)?,
                    expires_at: row.try_get("expires_at").map_err(ce)?,
                    revoked: row.try_get::<i64, _>("revoked").map_err(ce)? != 0,
                })
            })
            .collect()
    }

    async fn insert_session_token(
        &self,
        token_hash: &[u8],
        scopes: &str,
        subject: Option<&str>,
        kind: &str,
        expires_at: DateTime<Utc>,
        session_id: uuid::Uuid,
    ) -> Result<(), CatalogError> {
        sqlx::query(
            "INSERT INTO api_tokens (token_hash, tenant_id, scopes, subject, kind, expires_at, revoked, session_id, created_at)
             VALUES (?,?,?,?,?,?,0,?,?)",
        )
        .bind(token_hash)
        .bind(DEFAULT_TENANT.as_uuid())
        .bind(scopes)
        .bind(subject)
        .bind(kind)
        .bind(expires_at)
        .bind(session_id)
        .bind(Utc::now())
        .execute(&self.pool)
        .await
        .map_err(ce)?;
        Ok(())
    }

    async fn lookup_refresh_token(
        &self,
        token_hash: &[u8],
    ) -> Result<Option<RefreshClaim>, CatalogError> {
        let now = Utc::now();
        let maybe = sqlx::query(
            "SELECT tenant_id, scopes, subject, session_id FROM api_tokens
             WHERE token_hash = ? AND revoked = 0 AND kind = 'refresh'
               AND (expires_at IS NULL OR expires_at > ?)",
        )
        .bind(token_hash)
        .bind(now)
        .fetch_optional(&self.pool)
        .await
        .map_err(ce)?;
        let Some(row) = maybe else { return Ok(None) };
        let session_id: Option<Uuid> = row.try_get("session_id").map_err(ce)?;
        Ok(Some(RefreshClaim {
            tenant: TenantId::from_uuid(row.try_get::<Uuid, _>("tenant_id").map_err(ce)?),
            role: row.try_get("scopes").map_err(ce)?,
            subject: row.try_get("subject").map_err(ce)?,
            session_id: session_id.unwrap_or_else(Uuid::nil),
        }))
    }

    async fn revoke_session_by_token(&self, token_hash: &[u8]) -> Result<u64, CatalogError> {
        let session_id: Option<Uuid> =
            sqlx::query_scalar("SELECT session_id FROM api_tokens WHERE token_hash = ?")
                .bind(token_hash)
                .fetch_optional(&self.pool)
                .await
                .map_err(ce)?
                .flatten();
        let res = if let Some(sid) = session_id {
            sqlx::query("UPDATE api_tokens SET revoked = 1 WHERE session_id = ? AND revoked = 0")
                .bind(sid)
                .execute(&self.pool)
                .await
                .map_err(ce)?
        } else {
            sqlx::query("UPDATE api_tokens SET revoked = 1 WHERE token_hash = ? AND revoked = 0")
                .bind(token_hash)
                .execute(&self.pool)
                .await
                .map_err(ce)?
        };
        Ok(res.rows_affected())
    }

    // ------------------------- ingest idempotency ledger -------------------------

    async fn lookup_idempotency_in_tenant(
        &self,
        tenant: TenantId,
        key: &str,
    ) -> Result<Option<IngestLedgerEntry>> {
        let now = Utc::now();
        let maybe = sqlx::query(
            "SELECT table_id, snapshot_id, rows_ingested, bytes_written, applied_at
             FROM ingest_ledger
             WHERE tenant_id = ? AND key = ? AND (expires_at IS NULL OR expires_at > ?)",
        )
        .bind(tenant.as_uuid())
        .bind(key)
        .bind(now)
        .fetch_optional(&self.pool)
        .await
        .map_err(ce)?;
        match maybe {
            Some(row) => Ok(Some(row_to_ledger(&row)?)),
            None => Ok(None),
        }
    }

    async fn record_idempotency_in_tenant(
        &self,
        tenant: TenantId,
        key: &str,
        entry: IngestLedgerEntry,
        ttl: chrono::Duration,
    ) -> Result<Option<IngestLedgerEntry>> {
        let expires_at = entry.applied_at + ttl;
        let res = sqlx::query(
            "INSERT INTO ingest_ledger
                (tenant_id, key, table_id, snapshot_id, rows_ingested, bytes_written, applied_at, expires_at)
             VALUES (?,?,?,?,?,?,?,?)
             ON CONFLICT (tenant_id, key) DO NOTHING",
        )
        .bind(tenant.as_uuid())
        .bind(key)
        .bind(entry.table_id.as_uuid())
        .bind(entry.snapshot_id.as_uuid())
        .bind(entry.rows_ingested as i64)
        .bind(entry.bytes_written as i64)
        .bind(entry.applied_at)
        .bind(expires_at)
        .execute(&self.pool)
        .await
        .map_err(ce)?;
        if res.rows_affected() == 0 {
            // A concurrent writer raced and won.
            Ok(None)
        } else {
            Ok(Some(entry))
        }
    }

    // --- embedding backfill (S1.5) ---

    async fn set_table_embed_config(&self, tenant: TenantId, cfg: &TableEmbedConfig) -> Result<()> {
        sqlx::query(
            "INSERT INTO table_embed_config
                (tenant_id, table_id, source_column, embedding_column, model_id, dim,
                 auto_embed, updated_at)
             VALUES (?,?,?,?,?,?,?,?)
             ON CONFLICT (tenant_id, table_id, embedding_column)
             DO UPDATE SET source_column = excluded.source_column,
                           model_id      = excluded.model_id,
                           dim           = excluded.dim,
                           auto_embed    = excluded.auto_embed,
                           updated_at    = excluded.updated_at",
        )
        .bind(tenant.as_uuid())
        .bind(cfg.table_id.as_uuid())
        .bind(&cfg.source_column)
        .bind(&cfg.embedding_column)
        .bind(&cfg.model_id)
        .bind(cfg.dim as i64)
        .bind(cfg.auto_embed as i64)
        .bind(Utc::now())
        .execute(&self.pool)
        .await
        .map_err(ce)?;
        Ok(())
    }

    async fn list_table_embed_configs(
        &self,
        tenant: TenantId,
        table_id: TableId,
    ) -> Result<Vec<TableEmbedConfig>> {
        let rows = sqlx::query(
            "SELECT table_id, source_column, embedding_column, model_id, dim, auto_embed
             FROM table_embed_config
             WHERE tenant_id = ? AND table_id = ?
             ORDER BY embedding_column",
        )
        .bind(tenant.as_uuid())
        .bind(table_id.as_uuid())
        .fetch_all(&self.pool)
        .await
        .map_err(ce)?;
        rows.iter()
            .map(|r| {
                Ok(TableEmbedConfig {
                    table_id: TableId::from_uuid(r.try_get::<Uuid, _>("table_id").map_err(ce)?),
                    source_column: r.try_get("source_column").map_err(ce)?,
                    embedding_column: r.try_get("embedding_column").map_err(ce)?,
                    model_id: r.try_get("model_id").map_err(ce)?,
                    dim: r.try_get::<i64, _>("dim").map_err(ce)? as u16,
                    auto_embed: r.try_get::<i64, _>("auto_embed").map_err(ce)? != 0,
                })
            })
            .collect()
    }

    async fn get_cached_embeddings(
        &self,
        tenant: TenantId,
        model_id: &str,
        hashes: &[String],
    ) -> Result<std::collections::HashMap<String, Vec<f32>>> {
        if hashes.is_empty() {
            return Ok(std::collections::HashMap::new());
        }
        let placeholders = vec!["?"; hashes.len()].join(",");
        let sql = format!(
            "SELECT content_hash, embedding
             FROM embedding_cache
             WHERE tenant_id = ? AND model_id = ? AND content_hash IN ({placeholders})"
        );
        let mut q = sqlx::query(&sql).bind(tenant.as_uuid()).bind(model_id);
        for h in hashes {
            q = q.bind(h);
        }
        let rows = q.fetch_all(&self.pool).await.map_err(ce)?;
        let mut out = std::collections::HashMap::with_capacity(rows.len());
        for r in &rows {
            let h: String = r.try_get("content_hash").map_err(ce)?;
            let bytes: Vec<u8> = r.try_get("embedding").map_err(ce)?;
            if let Some(v) = pensieve_core::catalog::embedding_from_le_bytes(&bytes) {
                out.insert(h, v);
            }
        }
        Ok(out)
    }

    async fn put_cached_embeddings(
        &self,
        tenant: TenantId,
        model_id: &str,
        dim: u16,
        entries: &[(String, Vec<f32>)],
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let now = Utc::now();
        let mut tx = begin_immediate(&self.pool).await.map_err(ce)?;
        for (hash, vec) in entries {
            let bytes = pensieve_core::catalog::embedding_to_le_bytes(vec);
            sqlx::query(
                "INSERT INTO embedding_cache
                    (tenant_id, content_hash, model_id, dim, embedding, created_at)
                 VALUES (?,?,?,?,?,?)
                 ON CONFLICT (tenant_id, content_hash, model_id) DO NOTHING",
            )
            .bind(tenant.as_uuid())
            .bind(hash)
            .bind(model_id)
            .bind(dim as i64)
            .bind(bytes)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(ce)?;
        }
        tx.commit().await.map_err(ce)?;
        Ok(())
    }
}

// --------------------------------------------------------------------------
// Row mappers
// --------------------------------------------------------------------------

fn row_to_dashboard(row: &SqliteRow) -> Result<Dashboard, CatalogError> {
    Ok(Dashboard {
        id: row.try_get::<Uuid, _>("id").map_err(ce)?,
        name: row.try_get("name").map_err(ce)?,
        description: row.try_get("description").map_err(ce)?,
        time_range_preset: row.try_get("time_range_preset").map_err(ce)?,
        refresh_interval_seconds: row.try_get("refresh_interval_seconds").map_err(ce)?,
        created_at: row.try_get("created_at").map_err(ce)?,
        updated_at: row.try_get("updated_at").map_err(ce)?,
    })
}

fn row_to_panel(row: &SqliteRow) -> Result<DashboardPanel, CatalogError> {
    let config_str: String = row.try_get("config").map_err(ce)?;
    Ok(DashboardPanel {
        id: row.try_get::<Uuid, _>("id").map_err(ce)?,
        dashboard_id: row.try_get::<Uuid, _>("dashboard_id").map_err(ce)?,
        title: row.try_get("title").map_err(ce)?,
        panel_type: row.try_get("panel_type").map_err(ce)?,
        query: row.try_get("query").map_err(ce)?,
        database_name: row.try_get("database_name").map_err(ce)?,
        config: parse_json(&config_str).unwrap_or(Json::Null),
        grid_x: row.try_get::<i64, _>("grid_x").map_err(ce)? as i32,
        grid_y: row.try_get::<i64, _>("grid_y").map_err(ce)? as i32,
        grid_w: row.try_get::<i64, _>("grid_w").map_err(ce)? as i32,
        grid_h: row.try_get::<i64, _>("grid_h").map_err(ce)? as i32,
        display_order: row.try_get::<i64, _>("display_order").map_err(ce)? as i32,
    })
}

fn row_to_user(row: &SqliteRow) -> Result<User, CatalogError> {
    Ok(User {
        id: row.try_get::<Uuid, _>("id").map_err(ce)?,
        username: row.try_get("username").map_err(ce)?,
        role: row.try_get("role").map_err(ce)?,
        created_at: row.try_get("created_at").map_err(ce)?,
        updated_at: row.try_get("updated_at").map_err(ce)?,
    })
}

fn row_to_graph(row: &SqliteRow) -> Result<GraphRegistration, CatalogError> {
    Ok(GraphRegistration {
        id: row.try_get::<Uuid, _>("id").map_err(ce)?,
        database: row.try_get("database").map_err(ce)?,
        name: row.try_get("name").map_err(ce)?,
        node_table: row.try_get("node_table").map_err(ce)?,
        edge_table: row.try_get("edge_table").map_err(ce)?,
        id_col: row.try_get("id_col").map_err(ce)?,
        label_col: row.try_get("label_col").map_err(ce)?,
        src_col: row.try_get("src_col").map_err(ce)?,
        dst_col: row.try_get("dst_col").map_err(ce)?,
        type_col: row.try_get("type_col").map_err(ce)?,
        realm_col: row.try_get("realm_col").map_err(ce)?,
        created_at: row.try_get("created_at").map_err(ce)?,
        updated_at: row.try_get("updated_at").map_err(ce)?,
    })
}

fn row_to_ledger(row: &SqliteRow) -> Result<IngestLedgerEntry, CatalogError> {
    Ok(IngestLedgerEntry {
        table_id: TableId::from_uuid(row.try_get::<Uuid, _>("table_id").map_err(ce)?),
        snapshot_id: SnapshotId::from_uuid(row.try_get::<Uuid, _>("snapshot_id").map_err(ce)?),
        rows_ingested: row.try_get::<i64, _>("rows_ingested").map_err(ce)? as u64,
        bytes_written: row.try_get::<i64, _>("bytes_written").map_err(ce)? as u64,
        applied_at: row.try_get("applied_at").map_err(ce)?,
    })
}

// --------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------

fn ce(e: sqlx::Error) -> CatalogError {
    CatalogError::Sql(e.to_string())
}

fn parse_json(s: &str) -> Result<Json, CatalogError> {
    serde_json::from_str(s).map_err(|e| CatalogError::Sql(format!("invalid stored json: {e}")))
}

fn row_to_sidecar(row: &SqliteRow) -> Result<IndexSidecarDescriptor> {
    let kind_str: String = row.try_get("kind").map_err(ce)?;
    let kind: SidecarKind = kind_str.parse()?;
    let params_str: String = row.try_get("params").map_err(ce)?;
    Ok(IndexSidecarDescriptor {
        id: row.try_get("id").map_err(ce)?,
        extent_id: ExtentId::from_uuid(row.try_get("extent_id").map_err(ce)?),
        table_id: TableId::from_uuid(row.try_get("table_id").map_err(ce)?),
        column: row.try_get("column_name").map_err(ce)?,
        kind,
        object_path: row.try_get("object_path").map_err(ce)?,
        byte_size: row.try_get::<i64, _>("byte_size").map_err(ce)? as u64,
        params: parse_json(&params_str).unwrap_or(Json::Null),
        embedding_model_id: row.try_get("embedding_model_id").map_err(ce)?,
        created_at: row.try_get("created_at").map_err(ce)?,
    })
}

fn row_to_ann_tree(row: &SqliteRow) -> Result<pensieve_core::index_sidecar::AnnTreeDescriptor> {
    let params_str: String = row.try_get("params").map_err(ce)?;
    Ok(pensieve_core::index_sidecar::AnnTreeDescriptor {
        id: row.try_get("id").map_err(ce)?,
        table_id: TableId::from_uuid(row.try_get("table_id").map_err(ce)?),
        column: row.try_get("column_name").map_err(ce)?,
        embedding_model_id: row.try_get("embedding_model_id").map_err(ce)?,
        generation: row.try_get("generation").map_err(ce)?,
        extent_fingerprint: row.try_get("extent_fingerprint").map_err(ce)?,
        object_path: row.try_get("object_path").map_err(ce)?,
        byte_size: row.try_get::<i64, _>("byte_size").map_err(ce)? as u64,
        params: parse_json(&params_str).unwrap_or(Json::Null),
        created_at: row.try_get("created_at").map_err(ce)?,
    })
}

fn role_to_str(role: NodeRole) -> &'static str {
    match role {
        NodeRole::AllInOne => "all_in_one",
        NodeRole::Ingest => "ingest",
        NodeRole::Query => "query",
        NodeRole::Compaction => "compaction",
    }
}

fn str_to_role(s: &str) -> NodeRole {
    match s {
        "ingest" => NodeRole::Ingest,
        "query" => NodeRole::Query,
        "compaction" => NodeRole::Compaction,
        _ => NodeRole::AllInOne,
    }
}

/// `column_stats.{col}.distinct` as a JSON array, or `None` (→ keep extent)
/// when the stat is missing, null, or not an array.
fn distinct_array<'a>(stats: &'a Json, col: &str) -> Option<&'a Vec<Json>> {
    stats
        .get(col)
        .and_then(|c| c.get("distinct"))
        .and_then(Json::as_array)
}

/// `column_stats.{col}.tokens` as a JSON string array, or `None` (→ keep).
fn token_array<'a>(stats: &'a Json, col: &str) -> Option<&'a Vec<Json>> {
    stats
        .get(col)
        .and_then(|c| c.get("tokens"))
        .and_then(Json::as_array)
}

/// `low <= x <= high` with numeric or string comparison; incomparable values
/// are treated as in-range (keep the extent — no false negatives).
fn in_range(x: &Json, low: &Json, high: &Json) -> bool {
    match (x.as_f64(), low.as_f64(), high.as_f64()) {
        (Some(xv), Some(lo), Some(hi)) => xv >= lo && xv <= hi,
        _ => match (x.as_str(), low.as_str(), high.as_str()) {
            (Some(xs), Some(ls), Some(hs)) => xs >= ls && xs <= hs,
            _ => true,
        },
    }
}

/// Keep an extent for a `VectorDistance` prune iff its centroid+radius cosine
/// lower bound is below `threshold`. Missing/malformed `vec` stat ⇒ keep.
fn keep_extent_by_vector(column_stats: &Json, col: &str, query: &[f32], threshold: f64) -> bool {
    let Some(vec_stat) = column_stats.get(col).and_then(|c| c.get("vec")) else {
        return true;
    };
    let centroid: Vec<f64> = match vec_stat.get("centroid").and_then(Json::as_array) {
        Some(arr) => arr.iter().filter_map(Json::as_f64).collect(),
        None => return true,
    };
    let radius = match vec_stat.get("radius").and_then(Json::as_f64) {
        Some(r) => r,
        None => return true,
    };
    match cosine_distance_lower_bound(query, &centroid, radius) {
        Some(lb) => lb < threshold,
        None => true,
    }
}

// --- Arrow schema ⇆ JSON (DB-agnostic; identical shape to the PG catalog) ---

fn schema_to_json(schema: &arrow_schema::Schema) -> Result<Json, CatalogError> {
    let fields: Vec<Json> = schema
        .fields()
        .iter()
        .map(|f| {
            serde_json::json!({
                "name":     f.name(),
                "type":     arrow_type_to_string(f.data_type()),
                "nullable": f.is_nullable(),
            })
        })
        .collect();
    Ok(serde_json::json!({ "fields": fields }))
}

fn json_to_schema(json: &Json) -> Result<Arc<arrow_schema::Schema>, CatalogError> {
    let fields = json
        .get("fields")
        .and_then(Json::as_array)
        .ok_or_else(|| CatalogError::Sql("malformed schema json: missing fields".into()))?;
    let mut out = Vec::with_capacity(fields.len());
    for f in fields {
        let name = f
            .get("name")
            .and_then(Json::as_str)
            .ok_or_else(|| CatalogError::Sql("field missing name".into()))?;
        let ty = f
            .get("type")
            .and_then(Json::as_str)
            .ok_or_else(|| CatalogError::Sql("field missing type".into()))?;
        let nullable = f.get("nullable").and_then(Json::as_bool).unwrap_or(true);
        out.push(arrow_schema::Field::new(
            name,
            string_to_arrow_type(ty)?,
            nullable,
        ));
    }
    Ok(Arc::new(arrow_schema::Schema::new(out)))
}

fn arrow_type_to_string(ty: &arrow_schema::DataType) -> String {
    use arrow_schema::{DataType, TimeUnit};
    match ty {
        DataType::Boolean => "bool".into(),
        DataType::Int32 => "int".into(),
        DataType::Int64 => "long".into(),
        DataType::Float64 => "real".into(),
        DataType::Utf8 | DataType::LargeUtf8 => "string".into(),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => "timestamp".into(),
        DataType::Binary | DataType::LargeBinary => "dynamic".into(),
        DataType::FixedSizeList(inner, dim) if matches!(inner.data_type(), DataType::Float32) => {
            format!("vector({dim})")
        }
        other => format!("arrow:{other:?}"),
    }
}

fn string_to_arrow_type(s: &str) -> Result<arrow_schema::DataType, CatalogError> {
    use arrow_schema::{DataType, Field, TimeUnit};
    Ok(match s {
        "bool" => DataType::Boolean,
        "int" => DataType::Int32,
        "long" => DataType::Int64,
        "real" => DataType::Float64,
        "string" => DataType::Utf8,
        "timestamp" => DataType::Timestamp(TimeUnit::Nanosecond, None),
        "dynamic" => DataType::Binary,
        other if other.starts_with("vector(") && other.ends_with(')') => {
            let inner = &other[7..other.len() - 1];
            let dim: i32 = inner.trim().parse().map_err(|_| {
                CatalogError::Sql(format!(
                    "vector(N): N must be a positive integer, got '{inner}'"
                ))
            })?;
            if dim <= 0 {
                return Err(CatalogError::Sql(format!(
                    "vector(N): N must be > 0, got {dim}"
                )));
            }
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), dim)
        }
        other => {
            return Err(CatalogError::Sql(format!(
                "unsupported column type in schema: {other}"
            )))
        }
    })
}

#[cfg(test)]
mod tests;
