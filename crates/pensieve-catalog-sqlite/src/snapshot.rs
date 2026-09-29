//! `SqliteSnapshotTxn` — the SQLite analogue of `PgSnapshotTxn`.
//!
//! Accumulates add/remove-extent operations in memory, then commits them in a
//! single short SQLite transaction. The final step is the same optimistic
//! compare-and-set on `tables.current_snapshot_id` the Postgres catalog uses:
//!
//! ```sql
//! UPDATE tables SET current_snapshot_id = ?new
//!   WHERE id = ?table AND current_snapshot_id = ?parent;
//! ```
//!
//! Zero rows affected ⇒ another writer advanced the snapshot since
//! `begin_snapshot` ⇒ [`CatalogError::Conflict`]. SQLite also serialises
//! writers at the file level, and the `UNIQUE(table_id, sequence_number)`
//! constraint catches a racing sibling before the CAS — both surface as
//! `Conflict`, matching the Postgres semantics.

use async_trait::async_trait;
use chrono::Utc;
use pensieve_core::catalog::{ExtentManifest, SnapshotSummary, SnapshotTxn};
use pensieve_core::errors::{CatalogError, Error, Result};
use pensieve_core::tenant::TenantId;
use pensieve_core::types::{ExtentId, SchemaSnapshotId, SnapshotId, TableId};
use sqlx::error::ErrorKind;
use sqlx::SqlitePool;
use uuid::Uuid;

/// In-memory accumulation of snapshot operations prior to commit.
#[derive(Debug)]
pub struct SqliteSnapshotTxn {
    pool: SqlitePool,
    tenant: TenantId,
    table_id: TableId,
    parent_snapshot_id: SnapshotId,
    schema_snapshot_id: SchemaSnapshotId,
    added: Vec<ExtentManifest>,
    removed: Vec<ExtentId>,
}

impl SqliteSnapshotTxn {
    pub fn new(
        pool: SqlitePool,
        tenant: TenantId,
        table_id: TableId,
        parent_snapshot_id: SnapshotId,
        schema_snapshot_id: SchemaSnapshotId,
    ) -> Self {
        Self {
            pool,
            tenant,
            table_id,
            parent_snapshot_id,
            schema_snapshot_id,
            added: Vec::new(),
            removed: Vec::new(),
        }
    }
}

#[async_trait]
impl SnapshotTxn for SqliteSnapshotTxn {
    fn parent_snapshot_id(&self) -> SnapshotId {
        self.parent_snapshot_id
    }

    async fn add_extent(&mut self, manifest: ExtentManifest) -> Result<()> {
        self.added.push(manifest);
        Ok(())
    }

    async fn remove_extents(&mut self, extents: &[ExtentId]) -> Result<()> {
        self.removed.extend_from_slice(extents);
        Ok(())
    }

    async fn commit(self: Box<Self>, summary: SnapshotSummary) -> Result<SnapshotId> {
        let Self {
            pool,
            tenant,
            table_id,
            parent_snapshot_id,
            schema_snapshot_id,
            added,
            removed,
        } = *self;

        let summary_json = serde_json::json!({
            "operation":      summary.operation,
            "rows_added":     summary.rows_added,
            "rows_removed":   summary.rows_removed,
            "bytes_added":    summary.bytes_added,
            "bytes_removed":  summary.bytes_removed,
        })
        .to_string();

        let mut tx = super::begin_immediate(&pool).await.map_err(ce)?;

        // Compute the new sequence number from the parent's sequence + 1.
        let parent_seq: i64 =
            sqlx::query_scalar("SELECT sequence_number FROM snapshots WHERE id = ?")
                .bind(*parent_snapshot_id.as_uuid())
                .fetch_optional(&mut *tx)
                .await
                .map_err(ce)?
                .ok_or_else(|| {
                    CatalogError::Sql(format!("parent snapshot {parent_snapshot_id} not found"))
                })?;
        let new_seq = parent_seq + 1;

        // 1. Insert the new snapshot row. The UNIQUE(table_id, sequence_number)
        //    constraint catches a racing sibling that computed the same seq.
        let new_snap_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO snapshots
                (id, tenant_id, table_id, parent_id, sequence_number, schema_snapshot_id, summary, created_at)
             VALUES (?,?,?,?,?,?,?,?)",
        )
        .bind(new_snap_id)
        .bind(tenant.as_uuid())
        .bind(*table_id.as_uuid())
        .bind(*parent_snapshot_id.as_uuid())
        .bind(new_seq)
        .bind(*schema_snapshot_id.as_uuid())
        .bind(&summary_json)
        .bind(Utc::now())
        .execute(&mut *tx)
        .await
        .map_err(sql_err_or_conflict)?;

        // 2. Insert a manifest row for this snapshot (kind=data).
        let manifest_id = Uuid::new_v4();
        let bytes_added: i64 = added.iter().map(|e| e.byte_size as i64).sum();
        sqlx::query(
            "INSERT INTO manifests
                (id, tenant_id, snapshot_id, kind, extent_count, byte_size, created_at)
             VALUES (?,?,?,'data',?,?,?)",
        )
        .bind(manifest_id)
        .bind(tenant.as_uuid())
        .bind(new_snap_id)
        .bind(added.len() as i64)
        .bind(bytes_added)
        .bind(Utc::now())
        .execute(&mut *tx)
        .await
        .map_err(ce)?;

        // 3. Insert the added extents.
        for e in &added {
            sqlx::query(
                "INSERT INTO extents (
                    id, tenant_id, table_id, manifest_id, schema_snapshot_id, object_path, byte_size,
                    row_count, min_timestamp, max_timestamp, column_stats, present_paths,
                    compaction_gen, created_at, deleted_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL)",
            )
            .bind(*e.id.as_uuid())
            .bind(tenant.as_uuid())
            .bind(*e.table_id.as_uuid())
            .bind(manifest_id)
            .bind(*e.schema_snapshot_id.as_uuid())
            .bind(&e.object_path)
            .bind(e.byte_size as i64)
            .bind(e.row_count as i64)
            .bind(e.min_timestamp)
            .bind(e.max_timestamp)
            .bind(e.column_stats.to_string())
            .bind(serde_json::to_string(&e.present_paths).unwrap_or_else(|_| "[]".into()))
            .bind(e.compaction_gen as i64)
            .bind(e.created_at)
            .execute(&mut *tx)
            .await
            .map_err(ce)?;
        }

        // 4. Soft-delete the removed extents (compaction + retention path).
        if !removed.is_empty() {
            let now = Utc::now();
            let placeholders = vec!["?"; removed.len()].join(",");
            let sql = format!(
                "UPDATE extents SET deleted_at = ? \
                 WHERE tenant_id = ? AND deleted_at IS NULL AND id IN ({placeholders})"
            );
            let mut q = sqlx::query(&sql).bind(now).bind(tenant.as_uuid());
            for id in &removed {
                q = q.bind(*id.as_uuid());
            }
            q.execute(&mut *tx).await.map_err(ce)?;
        }

        // 5. The critical CAS. Zero rows ⇒ someone advanced the snapshot.
        let swapped = sqlx::query(
            "UPDATE tables SET current_snapshot_id = ? \
             WHERE tenant_id = ? AND id = ? AND current_snapshot_id = ?",
        )
        .bind(new_snap_id)
        .bind(tenant.as_uuid())
        .bind(*table_id.as_uuid())
        .bind(*parent_snapshot_id.as_uuid())
        .execute(&mut *tx)
        .await
        .map_err(ce)?;

        if swapped.rows_affected() != 1 {
            // Rollback is implicit when `tx` drops without commit.
            return Err(CatalogError::Conflict.into());
        }

        tx.commit().await.map_err(ce)?;
        Ok(SnapshotId::from_uuid(new_snap_id))
    }

    async fn rollback(self: Box<Self>) -> Result<()> {
        // No SQLite tx is open until commit; drop is a no-op.
        Ok(())
    }
}

/// True for a transient SQLite lock (`SQLITE_BUSY` / `SQLITE_BUSY_SNAPSHOT`).
/// WAL + `busy_timeout` handles plain BUSY, but BUSY_SNAPSHOT (a write-write
/// serialization conflict under concurrent in-process writers — the local
/// engine has several background pollers) is NOT covered by busy_timeout and
/// surfaces here. It is genuinely retryable, so we map it to `Conflict` and let
/// the ingest commit-retry loop re-run the transaction.
fn is_transient_lock(e: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(db) = e {
        let m = db.message();
        return m.contains("database is locked") || m.contains("database table is locked");
    }
    false
}

fn ce(e: sqlx::Error) -> CatalogError {
    if is_transient_lock(&e) {
        return CatalogError::Conflict;
    }
    CatalogError::Sql(e.to_string())
}

/// Unique-constraint violations on the snapshots table are conflicts — another
/// writer committed a child snapshot with the same sequence number in the
/// window between our `SELECT` and our `INSERT`. A transient lock is likewise
/// a retryable conflict.
fn sql_err_or_conflict(e: sqlx::Error) -> Error {
    if let sqlx::Error::Database(db_err) = &e {
        if db_err.kind() == ErrorKind::UniqueViolation {
            return Error::Catalog(CatalogError::Conflict);
        }
    }
    if is_transient_lock(&e) {
        return Error::Catalog(CatalogError::Conflict);
    }
    Error::Catalog(CatalogError::Sql(e.to_string()))
}
