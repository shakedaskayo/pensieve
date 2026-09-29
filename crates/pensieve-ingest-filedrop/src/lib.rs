//! Object-store file-drop ingest.
//!
//! Polls a configured bucket/prefix on an interval. For each object found:
//!   1. Route by path convention `{prefix}/{database}/{table}/*.ndjson` to
//!      the target (database, table).
//!   2. Compute a SHA256 of the object contents; use `filedrop:{sha}` as
//!      the idempotency key in the existing `ingest_ledger`. Re-processing
//!      the same content is a no-op.
//!   3. Parse the content per extension (NDJSON for MVP; CSV + Parquet in
//!      follow-on work).
//!   4. Route through the same `WritePath` the REST ingest uses, so
//!      staging / commit-coordinator / metrics all fire unchanged.
//!
//! We do **not** delete processed files by default — the idempotency ledger
//! makes re-polling a no-op and keeping the raw files preserves a
//! replayable audit trail. Configurable via `FiledropConfig::delete_after_ingest`.

#![forbid(unsafe_code)]

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use futures::StreamExt;
use object_store::path::Path;
use object_store::ObjectStore;
use pensieve_core::catalog::Catalog;
use pensieve_core::errors::{Error, Result};
use pensieve_ingest_core::{ensure_table, evolve_schema_for_records, WritePath};
use sha2::{Digest, Sha256};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, instrument, warn};

#[derive(Debug, Clone)]
pub struct FiledropConfig {
    /// One or more object-store prefixes to watch. Each is scanned per tick
    /// in the order given. Files under any of these prefixes still must
    /// match the `{prefix}/{database}/{table}/...` path convention.
    ///
    /// Default: `["ingest"]`, preserving the original single-prefix
    /// behavior. Multiple prefixes let one pensieve instance host watchers for
    /// many independent pipelines without spawning N separate watcher tasks.
    pub prefixes: Vec<String>,
    /// How often to scan each prefix. The same interval applies to all.
    pub poll_interval: Duration,
    /// If true, delete object after successful ingest. If false (default),
    /// leave in place — the idempotency ledger handles re-scans.
    pub delete_after_ingest: bool,
    /// If true, missing target tables are auto-created on first file with
    /// the engine's default schema (`at, label, body, props`). New
    /// properties in subsequent files extend the schema. Defaults to true.
    pub auto_create: bool,
    /// If true, scan each NDJSON file for new top-level keys and `ALTER
    /// TABLE ADD COLUMN` for any that aren't already part of the schema.
    /// Bounded by `MAX_NEW_COLUMNS_PER_REQUEST`. Defaults to true.
    pub schema_evolve: bool,
}

impl Default for FiledropConfig {
    fn default() -> Self {
        Self {
            prefixes: vec!["ingest".to_string()],
            poll_interval: Duration::from_secs(5),
            delete_after_ingest: false,
            auto_create: true,
            schema_evolve: true,
        }
    }
}

impl FiledropConfig {
    pub fn from_env() -> Self {
        let mut d = Self::default();
        // PENSIEVE_FILEDROP_PREFIXES wins if set (comma-separated). Otherwise
        // PENSIEVE_FILEDROP_PREFIX (legacy single-prefix) wins.
        if let Ok(v) = std::env::var("PENSIEVE_FILEDROP_PREFIXES") {
            let prefixes: Vec<String> = v
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if !prefixes.is_empty() {
                d.prefixes = prefixes;
            }
        } else if let Ok(v) = std::env::var("PENSIEVE_FILEDROP_PREFIX") {
            if !v.is_empty() {
                d.prefixes = vec![v];
            }
        }
        if let Ok(v) = std::env::var("PENSIEVE_FILEDROP_POLL_SECS")
            .and_then(|v| v.parse::<u64>().map_err(|_| std::env::VarError::NotPresent))
        {
            d.poll_interval = Duration::from_secs(v);
        }
        if let Ok(v) = std::env::var("PENSIEVE_FILEDROP_DELETE_AFTER_INGEST") {
            d.delete_after_ingest = v == "1" || v.eq_ignore_ascii_case("true");
        }
        if let Ok(v) = std::env::var("PENSIEVE_FILEDROP_AUTO_CREATE") {
            d.auto_create = !(v == "0" || v.eq_ignore_ascii_case("false"));
        }
        if let Ok(v) = std::env::var("PENSIEVE_FILEDROP_SCHEMA_EVOLVE") {
            d.schema_evolve = !(v == "0" || v.eq_ignore_ascii_case("false"));
        }
        d
    }
}

/// Outcome of one poll cycle, handed to the optional scan hook.
#[derive(Debug, Clone, Default)]
pub struct FiledropScan {
    /// Objects listed across all configured prefixes.
    pub seen: u64,
    /// Objects newly ingested (idempotency-ledger replays don't count).
    pub processed: u64,
    /// Per-object processing failures (a failed list aborts the tick and is
    /// reported by `run()` as `errors: 1` instead).
    pub errors: u64,
    /// Wall-clock duration of the whole tick.
    pub duration_ms: u64,
}

/// Callback invoked after every poll cycle with that cycle's [`FiledropScan`].
///
/// This is how the binary bridges the watcher into the watcher registry
/// (heartbeats) without this crate depending on `pensieve-datasources`. Runs on
/// the watcher's task inside the tokio runtime, so implementations may
/// `tokio::spawn`.
pub type ScanHook = std::sync::Arc<dyn Fn(FiledropScan) + Send + Sync>;

/// The file-drop watcher. Cloneable; `run` consumes self.
#[derive(Clone)]
pub struct FiledropWatcher {
    catalog: Arc<dyn Catalog>,
    store: Arc<dyn ObjectStore>,
    write_path: WritePath,
    config: FiledropConfig,
    scan_hook: Option<ScanHook>,
}

impl FiledropWatcher {
    pub fn new(
        catalog: Arc<dyn Catalog>,
        store: Arc<dyn ObjectStore>,
        write_path: WritePath,
        config: FiledropConfig,
    ) -> Self {
        Self {
            catalog,
            store,
            write_path,
            config,
            scan_hook: None,
        }
    }

    /// Install a hook that fires after every poll cycle with that cycle's
    /// [`FiledropScan`]. Used by the binary to heartbeat the watcher registry.
    #[must_use]
    pub fn with_scan_hook(mut self, hook: ScanHook) -> Self {
        self.scan_hook = Some(hook);
        self
    }

    pub async fn run(self, shutdown: impl Future<Output = ()>) {
        info!(
            prefixes = ?self.config.prefixes,
            poll_secs = self.config.poll_interval.as_secs(),
            auto_create = self.config.auto_create,
            schema_evolve = self.config.schema_evolve,
            "filedrop watcher starting"
        );
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                biased;
                () = &mut shutdown => {
                    info!("filedrop watcher shutting down");
                    return;
                }
                _ = tokio::time::sleep(self.config.poll_interval) => {
                    match self.tick().await {
                        Ok(scan) => {
                            if let Some(hook) = &self.scan_hook {
                                hook(scan);
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "filedrop tick failed");
                            if let Some(hook) = &self.scan_hook {
                                hook(FiledropScan { errors: 1, ..Default::default() });
                            }
                        }
                    }
                }
            }
        }
    }

    /// One poll cycle: scan every configured prefix and process what's there.
    /// Returns the cycle's counters; `run()` hands them to the scan hook.
    /// Public so integration tests can drive a single deterministic cycle.
    #[instrument(skip(self))]
    pub async fn tick(&self) -> Result<FiledropScan> {
        // Walk each configured prefix in turn. We don't parallelize here on
        // purpose: a single watcher should be I/O-bound on the object store
        // anyway, and serial scanning gives deterministic per-prefix logs.
        let started = std::time::Instant::now();
        let mut total_seen = 0u64;
        let mut total_processed = 0u64;
        let mut total_errors = 0u64;
        for prefix in &self.config.prefixes {
            let prefix_path = Path::from(prefix.clone());
            let mut stream = self.store.list(Some(&prefix_path));
            let mut seen = 0u64;
            let mut processed = 0u64;
            let mut errors = 0u64;
            while let Some(obj) = stream.next().await {
                let obj = obj.map_err(|e| Error::Internal(format!("list {prefix}: {e}")))?;
                seen += 1;
                match self.process_one(prefix, &obj.location).await {
                    Ok(true) => processed += 1,
                    Ok(false) => {} // replayed from ledger; not counted as new work
                    Err(e) => {
                        errors += 1;
                        warn!(path = %obj.location, prefix = %prefix, error = %e, "filedrop: failed to process file");
                    }
                }
            }
            total_seen += seen;
            total_processed += processed;
            total_errors += errors;
            if seen > 0 {
                debug!(prefix = %prefix, seen, processed, errors, "filedrop scan (per-prefix)");
            }
        }
        if total_seen > 0 {
            debug!(
                seen = total_seen,
                processed = total_processed,
                errors = total_errors,
                "filedrop scan complete"
            );
        }
        Ok(FiledropScan {
            seen: total_seen,
            processed: total_processed,
            errors: total_errors,
            duration_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        })
    }

    /// Returns `Ok(true)` if the file was newly ingested, `Ok(false)` if it
    /// was an idempotency-ledger replay.
    async fn process_one(&self, prefix: &str, path: &Path) -> Result<bool> {
        // 1. Parse database + table from the path. Convention:
        //    `{prefix}/{database}/{table}/{filename}`
        let (database, table, filename) = match split_path(prefix, path) {
            Some(x) => x,
            None => {
                debug!(path = %path, prefix = %prefix, "filedrop: skipping (does not match prefix/database/table/file)");
                return Ok(false);
            }
        };

        // 2. Download bytes + SHA256.
        let get = self
            .store
            .get(path)
            .await
            .map_err(|e| Error::Internal(format!("get {path}: {e}")))?;
        let bytes = get
            .bytes()
            .await
            .map_err(|e| Error::Internal(format!("read {path}: {e}")))?;
        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        let digest = hasher.finalize();
        let sha_hex = hex_encode(&digest);
        let idem_key = format!("filedrop:{sha_hex}");

        // 3. Resolve the target table — auto-create on first file if
        //    enabled, else strict lookup.
        let table_ref = if self.config.auto_create {
            ensure_table(&*self.catalog, &database, &table).await?
        } else {
            self.catalog.lookup_table(&database, &table).await?
        };

        // 4. Parse by extension.
        let ext = filename
            .rsplit('.')
            .next()
            .unwrap_or("")
            .to_ascii_lowercase();
        let table_ref = if matches!(ext.as_str(), "ndjson" | "jsonl" | "json")
            && self.config.schema_evolve
        {
            // Pre-scan for new top-level keys so the schema is up-to-date
            // before parse_ndjson runs (which would otherwise drop them).
            match parse_records_for_inspection(&bytes) {
                Ok(records) => {
                    match evolve_schema_for_records(&*self.catalog, &database, table_ref, &records)
                        .await
                    {
                        Ok(t) => t,
                        Err(e) => {
                            warn!(error = %e, "schema_evolve failed; continuing with current schema");
                            // Re-look up to be safe.
                            self.catalog.lookup_table(&database, &table).await?
                        }
                    }
                }
                Err(_) => table_ref,
            }
        } else {
            table_ref
        };
        let batches: Vec<RecordBatch> = match ext.as_str() {
            "ndjson" | "jsonl" | "json" => parse_ndjson(&bytes, &table_ref.schema)?,
            other => {
                return Err(Error::Internal(format!(
                    "filedrop: unsupported extension `.{other}` (MVP supports ndjson/jsonl/json)"
                )));
            }
        };
        if batches.is_empty() {
            // Still record the ledger entry so we don't re-download an
            // empty file on every poll.
            let _ = self
                .catalog
                .record_idempotency(
                    &idem_key,
                    pensieve_core::catalog::IngestLedgerEntry {
                        table_id: table_ref.id,
                        snapshot_id: table_ref.current_snapshot_id,
                        rows_ingested: 0,
                        bytes_written: 0,
                        applied_at: chrono::Utc::now(),
                    },
                    chrono::Duration::hours(24 * 30),
                )
                .await?;
            return Ok(false);
        }

        // 5. Ingest via WritePath with the SHA-keyed idempotency header.
        //    If the ledger already has this SHA, write_path returns
        //    `replayed: true` without writing another extent.
        let ack = self
            .write_path
            .ingest_with_idempotency(&database, &table_ref, batches, Some(&idem_key))
            .await?;

        ::metrics::counter!("pensieve_filedrop_objects_processed_total",
            "replayed" => ack.replayed.to_string())
        .increment(1);
        ::metrics::counter!("pensieve_filedrop_rows_total", "table" => table.clone())
            .increment(ack.rows_ingested);

        if self.config.delete_after_ingest && !ack.replayed {
            if let Err(e) = self.store.delete(path).await {
                warn!(path = %path, error = %e, "filedrop: delete after ingest failed");
            }
        }

        info!(
            path = %path,
            database = %database,
            table = %table,
            rows = ack.rows_ingested,
            replayed = ack.replayed,
            "filedrop: ingest complete"
        );
        Ok(!ack.replayed)
    }
}

/// `{prefix}/{database}/{table}/{filename}` → `(database, table, filename)`.
fn split_path(prefix: &str, path: &Path) -> Option<(String, String, String)> {
    let s = path.as_ref();
    let prefix = prefix.trim_end_matches('/');
    let s = s.strip_prefix(prefix)?.trim_start_matches('/');
    let parts: Vec<&str> = s.split('/').collect();
    if parts.len() < 3 {
        return None;
    }
    let database = parts[0].to_owned();
    let table = parts[1].to_owned();
    let filename = parts[2..].join("/"); // allow nested subdirs, preserve
    if database.is_empty() || table.is_empty() || filename.is_empty() {
        return None;
    }
    Some((database, table, filename))
}

/// Parse an NDJSON body into `RecordBatch`es according to the table's schema.
/// Delegates to the shared `pensieve_ingest_core::parse_ndjson`, which handles
/// primitive columns via arrow-json and adds `FixedSizeList<Float32>`
/// (vector-column) support — the same path REST and Kafka ingest use.
fn parse_ndjson(bytes: &[u8], schema: &Arc<Schema>) -> Result<Vec<RecordBatch>> {
    pensieve_ingest_core::parse_ndjson(bytes, schema.clone())
        .map_err(|e| Error::Internal(format!("filedrop ndjson: {e}")))
}

/// Cheap NDJSON pre-scan that yields the same `serde_json::Value`s the
/// schema-evolve helper expects. Mirrors the helper in pensieve-ingest-rest;
/// kept duplicated for now to avoid a new shared utility crate over a
/// 12-line function. If a third frontend grows this, hoist it.
fn parse_records_for_inspection(
    bytes: &[u8],
) -> std::result::Result<Vec<serde_json::Value>, String> {
    pensieve_ingest_core::records_for_schema_evolve(bytes)
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0xf) as usize] as char);
    }
    out
}
