//! Per-table staging buffer with group commit.
//!
//! Every `POST /v1/ingest` previously produced its own extent + Postgres
//! CAS commit. Under contention this made the commit serialization point
//! our bottleneck. This module fixes that by accumulating concurrent
//! requests into batched flushes:
//!
//! - Requests append their `RecordBatch` to the per-table buffer and
//!   block on a `oneshot` until the batched flush completes.
//! - When the buffer crosses a threshold (rows OR age), one task runs the
//!   flush: write a single extent containing all staged batches, commit
//!   one new snapshot, then wake every queued caller with the same
//!   snapshot id.
//! - Durability contract is unchanged: the caller's `ack` still means
//!   "extent written to object storage + snapshot committed in catalog."
//!   Just with far less commit work per row.
//!
//! # Threshold tuning
//!
//! Defaults are conservative for the laptop bench. Real production numbers
//! live in per-table config:
//!   - `flush_max_rows` — flush when buffer reaches N rows (default 50 000).
//!   - `flush_max_age` — flush if the oldest request has been waiting more
//!     than this (default 200 ms).
//!   - `flush_max_bytes` — safety cap (default 64 MiB).
//!
//! # Shutdown
//!
//! `StagingBuffer::drain_all` flushes every partially-full buffer. The
//! main binary calls it on SIGTERM before exit.

use arrow_array::RecordBatch;
use chrono::Utc;
use pensieve_core::catalog::{
    Catalog, ExtentManifest, IngestLedgerEntry, SnapshotSummary, TableRef,
};
use pensieve_core::errors::{CatalogError, Error, Result};
use pensieve_core::segment_format::{ExtentWriteResult, SegmentFormat};
use pensieve_core::types::{SnapshotId, TableId};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{oneshot, Mutex};
use tracing::{debug, info, instrument, warn};

/// Per-table staging state.
struct TableStaging {
    /// Immutable reference — the schema + snapshot pointer captured at
    /// the time of the first queued request. Later requests targeting the
    /// same table may observe a later snapshot; that's fine, we re-lookup
    /// on flush.
    schema: Arc<arrow_schema::Schema>,
    table_name: String,
    batches: Vec<RecordBatch>,
    total_rows: u64,
    total_bytes: u64,
    first_queued_at: Instant,
    /// Callers waiting for the next flush. Each flush wakes them all with
    /// the same `FlushOutcome`.
    waiters: Vec<oneshot::Sender<FlushOutcome>>,
}

/// Result delivered to every queued caller on a successful flush.
#[derive(Debug, Clone)]
pub struct FlushOutcome {
    pub snapshot_id: SnapshotId,
    pub extent_id: pensieve_core::types::ExtentId,
    pub byte_size: u64,
    pub row_count: u64,
}

/// Configuration for the buffer's flush triggers.
#[derive(Debug, Clone)]
pub struct StagingConfig {
    pub flush_max_rows: u64,
    pub flush_max_bytes: u64,
    pub flush_max_age: Duration,
    /// Max wall clock a request will wait before giving up if the background
    /// timer never fires (e.g. dead flusher). 10× `flush_max_age` is a safe default.
    pub request_timeout: Duration,
}

impl Default for StagingConfig {
    fn default() -> Self {
        // Tuned for low-latency web workloads. The win of group-commit comes
        // from amortizing the per-extent fixed cost (object-store PUT + PG
        // CAS, ~25-35ms on loopback) over many incoming requests. We want
        // flushes to fire either when we've got enough to amortize
        // (flush_max_rows) or when the oldest waiter is hurting
        // (flush_max_age).
        //
        // Workload shape to remember:
        //   tiny (≤100 rows/req) + high concurrency (100+) → batching wins big
        //   medium (500 rows/req) + low concurrency (≤16)  → batching ≈ neutral
        //   large (5000+ rows/req) + low concurrency       → batching hurts
        //
        // These defaults assume a tiny-to-medium web workload.
        Self {
            flush_max_rows: 8_000,
            flush_max_bytes: 16 * 1024 * 1024,
            flush_max_age: Duration::from_millis(50),
            request_timeout: Duration::from_secs(30),
        }
    }
}

impl StagingConfig {
    pub fn from_env() -> Self {
        let mut d = Self::default();
        if let Ok(v) = std::env::var("PENSIEVE_FLUSH_MAX_ROWS")
            .and_then(|v| v.parse::<u64>().map_err(|_| std::env::VarError::NotPresent))
        {
            d.flush_max_rows = v;
        }
        if let Ok(v) = std::env::var("PENSIEVE_FLUSH_MAX_BYTES")
            .and_then(|v| v.parse::<u64>().map_err(|_| std::env::VarError::NotPresent))
        {
            d.flush_max_bytes = v;
        }
        if let Ok(v) = std::env::var("PENSIEVE_FLUSH_MAX_AGE_MS")
            .and_then(|v| v.parse::<u64>().map_err(|_| std::env::VarError::NotPresent))
        {
            d.flush_max_age = Duration::from_millis(v);
        }
        d
    }
}

/// The per-engine staging buffer.
///
/// Cloneable handle — internally wraps an `Arc<Mutex<…>>`.
#[derive(Clone)]
pub struct StagingBuffer {
    inner: Arc<Mutex<HashMap<TableId, TableStaging>>>,
    catalog: Arc<dyn Catalog>,
    format: Arc<dyn SegmentFormat>,
    config: StagingConfig,
    /// Optional commit coordinator. When present, flushes hand their
    /// extent manifests to the coordinator for group commit (many
    /// extents → one snapshot). When absent, flushes commit directly.
    coordinator: Option<crate::CommitCoordinator>,
}

impl StagingBuffer {
    pub fn new(
        catalog: Arc<dyn Catalog>,
        format: Arc<dyn SegmentFormat>,
        config: StagingConfig,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            catalog,
            format,
            config,
            coordinator: None,
        }
    }

    /// Attach a commit coordinator so flushes are bundled into shared snapshots.
    pub fn with_coordinator(mut self, coord: crate::CommitCoordinator) -> Self {
        self.coordinator = Some(coord);
        self
    }

    /// Submit a batch and block until the next flush commits it.
    ///
    /// The returned `FlushOutcome::snapshot_id` is shared across every
    /// caller whose batch landed in the same flush. That is the correct
    /// semantics: the snapshot is the atom of visibility.
    #[instrument(skip(self, schema, batches), fields(table = %table.name, rows = batches.iter().map(|b| b.num_rows()).sum::<usize>()))]
    pub async fn submit(
        &self,
        table: &TableRef,
        schema: Arc<arrow_schema::Schema>,
        batches: Vec<RecordBatch>,
    ) -> Result<FlushOutcome> {
        if batches.is_empty() {
            // Nothing to flush; fabricate a no-op outcome pointing at the
            // caller's current snapshot.
            return Ok(FlushOutcome {
                snapshot_id: table.current_snapshot_id,
                extent_id: pensieve_core::types::ExtentId::new(),
                byte_size: 0,
                row_count: 0,
            });
        }
        let rx = self.enqueue(table, schema, batches).await;
        let timeout = self.config.request_timeout;
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(outcome)) => Ok(outcome),
            Ok(Err(_canceled)) => Err(Error::Internal("staging buffer dropped waiter".into())),
            Err(_elapsed) => Err(Error::Internal(format!(
                "staging buffer flush did not complete within {timeout:?}"
            ))),
        }
    }

    async fn enqueue(
        &self,
        table: &TableRef,
        schema: Arc<arrow_schema::Schema>,
        batches: Vec<RecordBatch>,
    ) -> oneshot::Receiver<FlushOutcome> {
        let (tx, rx) = oneshot::channel();
        let mut should_flush = false;
        let (added_rows, added_bytes) = sum_batches(&batches);

        // If a pending buffer exists with a *different* schema (ALTER TABLE
        // happened between requests), force-flush that first so the new
        // schema gets its own buffer. Otherwise we'd mix V1 + V2 batches
        // into one extent and the format writer would reject on schema
        // mismatch at the first V2 append.
        let needs_schema_flush = {
            let guard = self.inner.lock().await;
            guard
                .get(&table.id)
                .map(|e| e.schema.as_ref() != schema.as_ref())
                .unwrap_or(false)
        };
        if needs_schema_flush {
            let buf = self.clone();
            let tid = table.id;
            // Drive it inline so the new entry we're about to create
            // observes the post-flush empty state.
            if let Err(e) = buf.flush_table(tid, FlushReason::SchemaChange).await {
                warn!(error = %e, "schema-change pre-flush failed; staging will attempt again on next request");
            }
        }

        {
            let mut guard = self.inner.lock().await;
            let entry = guard.entry(table.id).or_insert_with(|| TableStaging {
                schema: schema.clone(),
                table_name: table.name.clone(),
                batches: Vec::new(),
                total_rows: 0,
                total_bytes: 0,
                first_queued_at: Instant::now(),
                waiters: Vec::new(),
            });
            // Keep schema ref fresh (post-schema-change path leaves entry empty).
            entry.schema = schema.clone();
            entry.batches.extend(batches);
            entry.total_rows += added_rows;
            entry.total_bytes += added_bytes;
            entry.waiters.push(tx);

            if entry.total_rows >= self.config.flush_max_rows
                || entry.total_bytes >= self.config.flush_max_bytes
            {
                should_flush = true;
            }
        }

        if should_flush {
            // Kick a flush; don't await (caller's rx will fire when done).
            let buf = self.clone();
            let table_id = table.id;
            tokio::spawn(async move {
                if let Err(e) = buf.flush_table(table_id, FlushReason::Threshold).await {
                    warn!(error = %e, "buffered flush failed");
                }
            });
        }
        rx
    }

    /// Background ticker — flushes any table whose oldest waiter is too old.
    ///
    /// Runs every ~10 ms, cheap lock + iter.
    pub async fn run_timer(self, shutdown: impl std::future::Future<Output = ()>) {
        info!(
            max_age_ms = self.config.flush_max_age.as_millis() as u64,
            "staging buffer timer starting"
        );
        tokio::pin!(shutdown);
        let mut tick = tokio::time::interval(Duration::from_millis(10));
        loop {
            tokio::select! {
                biased;
                () = &mut shutdown => {
                    info!("staging buffer timer shutting down; draining");
                    self.drain_all().await;
                    return;
                }
                _ = tick.tick() => {
                    self.flush_if_expired().await;
                }
            }
        }
    }

    async fn flush_if_expired(&self) {
        let cutoff = Instant::now() - self.config.flush_max_age;
        let mut to_flush: Vec<TableId> = Vec::new();
        {
            let guard = self.inner.lock().await;
            for (tid, entry) in guard.iter() {
                if !entry.batches.is_empty() && entry.first_queued_at <= cutoff {
                    to_flush.push(*tid);
                }
            }
        }
        // Spawn each flush so the timer doesn't block on one table's MinIO
        // upload while another table is also waiting. Pipelining flushes is
        // what gives us concurrency across tables and across generations of
        // the same table.
        for tid in to_flush {
            let buf = self.clone();
            tokio::spawn(async move {
                if let Err(e) = buf.flush_table(tid, FlushReason::Age).await {
                    warn!(table_id = %tid, error = %e, "age-triggered flush failed");
                }
            });
        }
    }

    /// Drain every partially-full buffer. Called on shutdown.
    pub async fn drain_all(&self) {
        let table_ids: Vec<TableId> = {
            let guard = self.inner.lock().await;
            guard
                .iter()
                .filter(|(_, s)| !s.batches.is_empty())
                .map(|(tid, _)| *tid)
                .collect()
        };
        for tid in table_ids {
            if let Err(e) = self.flush_table(tid, FlushReason::Shutdown).await {
                warn!(table_id = %tid, error = %e, "shutdown flush failed");
            }
        }
    }

    /// Snapshot the staged state for a table, write an extent, commit the
    /// snapshot, wake every waiter.
    #[instrument(skip(self), fields(reason = ?reason))]
    async fn flush_table(&self, table_id: TableId, reason: FlushReason) -> Result<()> {
        // 1. Atomically take the staged batches + waiters.
        let staging = {
            let mut guard = self.inner.lock().await;
            let Some(entry) = guard.get_mut(&table_id) else {
                return Ok(());
            };
            if entry.batches.is_empty() {
                return Ok(());
            }
            let schema = entry.schema.clone();
            let table_name = entry.table_name.clone();
            let batches = std::mem::take(&mut entry.batches);
            let waiters = std::mem::take(&mut entry.waiters);
            let rows = std::mem::replace(&mut entry.total_rows, 0);
            let bytes = std::mem::replace(&mut entry.total_bytes, 0);
            entry.first_queued_at = Instant::now();
            DrainedStaging {
                schema,
                table_name,
                batches,
                waiters,
                rows,
                _bytes: bytes,
            }
        };

        let start = Instant::now();
        // 2. Re-lookup the table so we have the current snapshot pointer at
        //    flush time (not when the first request arrived).
        let table_ref = match lookup_by_id(&*self.catalog, table_id).await? {
            Some(t) => t,
            None => {
                // Table removed between queue and flush — fail all waiters.
                for w in staging.waiters {
                    let _ = w.send_err(Error::Internal(format!(
                        "table {table_id} disappeared before flush"
                    )));
                }
                return Ok(());
            }
        };

        // 3. Write extent.
        let target_bytes = table_ref
            .config
            .extent_target_bytes
            .unwrap_or(1024 * 1024 * 1024);
        let mut writer = self
            .format
            .start_extent(staging.schema.clone(), target_bytes)
            .await?;
        for b in staging.batches {
            writer.append(b).await?;
        }
        let result: ExtentWriteResult = writer.finish().await?;

        // 4. Commit — either via the shared coordinator (group-commit
        //    across concurrent flushes) or directly.
        let manifest = extent_write_result_to_manifest(&table_ref, &result);
        let snapshot_id = if let Some(coord) = &self.coordinator {
            let table_label = staging.table_name.clone();
            let rows = staging.rows;
            let bytes = result.byte_size;
            coord
                .submit(move |responder| crate::commit_coordinator::CommitJob {
                    table_id: table_ref.id,
                    manifest: manifest.clone(),
                    rows,
                    bytes,
                    table_label,
                    responder,
                })
                .await?
        } else {
            commit_with_retry(
                &*self.catalog,
                table_ref.id,
                &manifest,
                staging.rows,
                &staging.table_name,
            )
            .await?
        };

        // 5. Wake every waiter with the shared outcome.
        let outcome = FlushOutcome {
            snapshot_id,
            extent_id: result.extent_id,
            byte_size: result.byte_size,
            row_count: result.row_count,
        };
        let n_waiters = staging.waiters.len();
        for w in staging.waiters {
            // If the receiver was dropped (client disconnect), ignore.
            let _ = w.send(outcome.clone());
        }

        let dur = start.elapsed();
        debug!(
            table = %staging.table_name,
            waiters = n_waiters,
            rows = staging.rows,
            bytes = result.byte_size,
            seconds = dur.as_secs_f64(),
            reason = ?reason,
            "batched flush committed"
        );

        ::metrics::counter!(
            "pensieve_staging_flushes_total",
            "table" => staging.table_name.clone(),
            "reason" => format!("{reason:?}").to_lowercase()
        )
        .increment(1);
        ::metrics::histogram!(
            "pensieve_staging_flush_waiters",
            "table" => staging.table_name.clone()
        )
        .record(n_waiters as f64);
        ::metrics::histogram!(
            "pensieve_staging_flush_duration_seconds",
            "table" => staging.table_name
        )
        .record(dur.as_secs_f64());

        Ok(())
    }
}

// ------------------------------------------------------------------

#[derive(Debug)]
enum FlushReason {
    Threshold,
    Age,
    Shutdown,
    SchemaChange,
}

struct DrainedStaging {
    schema: Arc<arrow_schema::Schema>,
    table_name: String,
    batches: Vec<RecordBatch>,
    waiters: Vec<oneshot::Sender<FlushOutcome>>,
    rows: u64,
    _bytes: u64,
}

fn sum_batches(batches: &[RecordBatch]) -> (u64, u64) {
    let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
    let bytes: u64 = batches
        .iter()
        .map(|b| {
            b.columns()
                .iter()
                .map(|c| c.get_array_memory_size() as u64)
                .sum::<u64>()
        })
        .sum();
    (rows, bytes)
}

async fn lookup_by_id(catalog: &dyn Catalog, table_id: TableId) -> Result<Option<TableRef>> {
    // We don't have a direct lookup-by-id; walk known databases. This is
    // acceptable here because lookup happens once per flush (not per
    // request) — amortized cost is low.
    //
    // Future: add `Catalog::lookup_table_by_id`. For now we reach around via
    // the Postgres pool like the compaction scheduler does.
    use std::any::Any;
    let any_ref: &dyn Any = catalog.as_ref_any();
    if let Some(pg) = any_ref.downcast_ref::<pensieve_catalog::PostgresCatalog>() {
        let row = sqlx::query_as::<_, (uuid::Uuid, String)>(
            "SELECT d.id, d.name
             FROM tables t JOIN databases d ON d.id = t.database_id
             WHERE t.id = $1",
        )
        .bind(table_id.as_uuid())
        .fetch_optional(pg.pool())
        .await
        .map_err(|e| Error::Catalog(CatalogError::Sql(e.to_string())))?;
        let Some((_, db_name)) = row else {
            return Ok(None);
        };
        let tables = catalog.list_tables_in_database(&db_name).await?;
        for t in tables {
            if t.id == table_id {
                return Ok(Some(t));
            }
        }
        return Ok(None);
    }
    // Local mode (and any non-Postgres catalog) has no pool to query. A
    // flush happens once per batch, and a single-binary catalog only has a
    // handful of databases, so a trait walk is cheap enough.
    let dbs = catalog.list_databases().await.map_err(Error::Catalog)?;
    for db_name in dbs {
        let tables = catalog.list_tables_in_database(&db_name).await?;
        for t in tables {
            if t.id == table_id {
                return Ok(Some(t));
            }
        }
    }
    Ok(None)
}

fn extent_write_result_to_manifest(table: &TableRef, r: &ExtentWriteResult) -> ExtentManifest {
    ExtentManifest {
        id: r.extent_id,
        table_id: table.id,
        schema_snapshot_id: table.schema_snapshot_id,
        object_path: r.object_path.clone(),
        byte_size: r.byte_size,
        row_count: r.row_count,
        min_timestamp: r.min_timestamp_nanos.map(nanos_to_utc),
        max_timestamp: r.max_timestamp_nanos.map(nanos_to_utc),
        column_stats: r.column_stats.clone(),
        present_paths: r.present_paths.clone(),
        compaction_gen: 0,
        created_at: Utc::now(),
    }
}

fn nanos_to_utc(n: i64) -> chrono::DateTime<Utc> {
    let secs = n.div_euclid(1_000_000_000);
    let nsec = n.rem_euclid(1_000_000_000) as u32;
    chrono::DateTime::<Utc>::from_timestamp(secs, nsec).unwrap_or_else(Utc::now)
}

async fn commit_with_retry(
    catalog: &dyn Catalog,
    table_id: TableId,
    manifest: &ExtentManifest,
    rows: u64,
    table_label: &str,
) -> Result<SnapshotId> {
    const MAX: u32 = 20;
    for attempt in 1..=MAX {
        let mut txn = catalog.begin_snapshot(table_id).await?;
        txn.add_extent(manifest.clone()).await?;
        match txn
            .commit(SnapshotSummary {
                rows_added: rows as i64,
                rows_removed: 0,
                bytes_added: manifest.byte_size as i64,
                bytes_removed: 0,
                operation: "ingest".to_string(),
            })
            .await
        {
            Ok(id) => return Ok(id),
            Err(Error::Catalog(CatalogError::Conflict)) => {
                ::metrics::counter!(
                    "pensieve_catalog_cas_conflicts_total",
                    "table" => table_label.to_string()
                )
                .increment(1);
                let base_ms = (2u64.saturating_pow(attempt.min(8))).min(200);
                let jitter_ms = fastrand::u64(..base_ms.max(1));
                tokio::time::sleep(Duration::from_millis(base_ms + jitter_ms)).await;
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    Err(Error::Catalog(CatalogError::Conflict))
}

/// Unused but kept for API symmetry if we later add idempotency short-circuit
/// at the staging layer.
pub fn _staging_ledger_hint(_: &IngestLedgerEntry) {}

// ------------------------------------------------------------------
// oneshot::Sender extension: send an error-shaped marker
// ------------------------------------------------------------------

trait OneshotSenderExt {
    fn send_err(self, e: Error);
}

impl OneshotSenderExt for oneshot::Sender<FlushOutcome> {
    fn send_err(self, _e: Error) {
        // We can't actually send an error through `oneshot<FlushOutcome>`.
        // Dropping the sender causes the receiver to get `Canceled`, which
        // our caller treats as an internal error. That's enough signalling
        // without plumbing a custom channel type for the rare case.
        drop(self);
    }
}
