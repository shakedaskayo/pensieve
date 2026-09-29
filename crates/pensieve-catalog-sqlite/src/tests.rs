//! In-memory round-trip tests for the SQLite catalog. These exercise the
//! Iceberg-style metadata path (db → table → snapshot → extent), Rust-side
//! extent pruning, schema evolution, graphs, tasks, users, tokens, dashboards,
//! and the idempotency ledger — no Postgres, no object store.

use super::*;
use arrow_schema::{DataType, Field, Schema};
use pensieve_core::catalog::{
    ColumnPrune, ExtentManifest, GraphSpec, IngestLedgerEntry, NodeInfo, PrunePredicate,
    SnapshotSummary, TableConfig,
};
use std::collections::HashMap;

fn sample_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("level", DataType::Utf8, true),
        Field::new("msg", DataType::Utf8, true),
    ]))
}

fn extent_for(tref: &TableRef, distinct_levels: &[&str]) -> ExtentManifest {
    let levels: Vec<Json> = distinct_levels
        .iter()
        .map(|s| Json::String((*s).into()))
        .collect();
    ExtentManifest {
        id: ExtentId::new(),
        table_id: tref.id,
        schema_snapshot_id: tref.schema_snapshot_id,
        object_path: format!("ext/{}.tlm", Uuid::new_v4()),
        byte_size: 4096,
        row_count: 100,
        min_timestamp: Some(Utc::now() - chrono::Duration::minutes(10)),
        max_timestamp: Some(Utc::now()),
        column_stats: serde_json::json!({
            "level": { "distinct": levels },
            "embedding": { "vec": { "centroid": [1.0, 0.0, 0.0], "radius": 0.0 } }
        }),
        present_paths: vec!["ts".into(), "level".into(), "msg".into()],
        compaction_gen: 0,
        created_at: Utc::now(),
    }
}

async fn fresh() -> SqliteCatalog {
    SqliteCatalog::connect_in_memory()
        .await
        .expect("open in-memory catalog")
}

#[tokio::test]
async fn full_table_snapshot_extent_round_trip() {
    let cat = fresh().await;

    // create db + table
    let db = cat.create_database("default").await.unwrap();
    let table_id = cat
        .create_table(db, "logs", sample_schema(), TableConfig::default())
        .await
        .unwrap();

    // lookup resolves schema + pointers
    let tref = cat.lookup_table("default", "logs").await.unwrap();
    assert_eq!(tref.id, table_id);
    assert_eq!(tref.schema.fields().len(), 3);

    // begin snapshot, add two extents, commit
    let mut txn = cat.begin_snapshot(table_id).await.unwrap();
    txn.add_extent(extent_for(&tref, &["info", "warn"]))
        .await
        .unwrap();
    txn.add_extent(extent_for(&tref, &["error"])).await.unwrap();
    let new_snap = txn
        .commit(SnapshotSummary {
            operation: "ingest".into(),
            rows_added: 200,
            ..Default::default()
        })
        .await
        .unwrap();

    // table now points at the new snapshot
    let tref = cat.lookup_table("default", "logs").await.unwrap();
    assert_eq!(tref.current_snapshot_id, new_snap);

    // list all live extents (no prune)
    let all = cat
        .list_extents(
            table_id,
            tref.current_snapshot_id,
            &PrunePredicate::default(),
        )
        .await
        .unwrap();
    assert_eq!(all.len(), 2, "both extents visible");

    // newest-first ordering by min_timestamp
    assert!(all[0].min_timestamp >= all[1].min_timestamp);
}

#[tokio::test]
async fn compaction_candidates_picks_small_extents() {
    // Exercises the SQLite window-function query behind
    // `Catalog::compaction_candidates` (the seam that lets compaction run on the
    // local SQLite catalog, not just Postgres).
    let cat = fresh().await;
    let db = cat.create_database("default").await.unwrap();
    let table_id = cat
        .create_table(db, "logs", sample_schema(), TableConfig::default())
        .await
        .unwrap();
    let tref = cat.lookup_table("default", "logs").await.unwrap();

    // Five small (4096-byte) live extents.
    let mut txn = cat.begin_snapshot(table_id).await.unwrap();
    for _ in 0..5 {
        txn.add_extent(extent_for(&tref, &["info"])).await.unwrap();
    }
    txn.commit(SnapshotSummary {
        operation: "ingest".into(),
        rows_added: 500,
        ..Default::default()
    })
    .await
    .unwrap();

    // ≥3 small extents → the table qualifies; capped to the smallest max_merge=4.
    let groups = cat.compaction_candidates(1_000_000, 3, 4).await.unwrap();
    assert_eq!(groups.len(), 1, "one table qualifies");
    let (tid, eids) = &groups[0];
    assert_eq!(*tid, table_id);
    assert_eq!(eids.len(), 4, "capped at max_merge");

    // Threshold above the extent count → no candidates.
    assert!(cat
        .compaction_candidates(1_000_000, 10, 4)
        .await
        .unwrap()
        .is_empty());
    // Byte ceiling below the extent size → no "small" extents qualify.
    assert!(cat
        .compaction_candidates(1_000, 3, 4)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn equals_prune_drops_non_matching_extent() {
    let cat = fresh().await;
    let db = cat.create_database("default").await.unwrap();
    let table_id = cat
        .create_table(db, "logs", sample_schema(), TableConfig::default())
        .await
        .unwrap();
    let tref = cat.lookup_table("default", "logs").await.unwrap();

    let mut txn = cat.begin_snapshot(table_id).await.unwrap();
    txn.add_extent(extent_for(&tref, &["info", "warn"]))
        .await
        .unwrap();
    txn.add_extent(extent_for(&tref, &["error"])).await.unwrap();
    txn.commit(SnapshotSummary {
        operation: "ingest".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let tref = cat.lookup_table("default", "logs").await.unwrap();

    // Equals("error") should keep only the extent whose distinct set has it.
    let mut preds = HashMap::new();
    preds.insert(
        "level".to_string(),
        ColumnPrune::Equals(Json::String("error".into())),
    );
    let pruned = cat
        .list_extents(
            table_id,
            tref.current_snapshot_id,
            &PrunePredicate {
                column_predicates: preds,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(pruned.len(), 1, "only the error extent survives");

    // A value present in neither distinct set drops both.
    let mut preds = HashMap::new();
    preds.insert(
        "level".to_string(),
        ColumnPrune::Equals(Json::String("trace".into())),
    );
    let none = cat
        .list_extents(
            table_id,
            tref.current_snapshot_id,
            &PrunePredicate {
                column_predicates: preds,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(none.len(), 0, "no extent has level=trace");
}

#[tokio::test]
async fn vector_distance_prune_uses_centroid_radius() {
    let cat = fresh().await;
    let db = cat.create_database("default").await.unwrap();
    let table_id = cat
        .create_table(db, "mem", sample_schema(), TableConfig::default())
        .await
        .unwrap();
    let tref = cat.lookup_table("default", "mem").await.unwrap();

    let mut txn = cat.begin_snapshot(table_id).await.unwrap();
    txn.add_extent(extent_for(&tref, &["info"])).await.unwrap(); // centroid = [1,0,0], radius 0
    txn.commit(SnapshotSummary::default()).await.unwrap();
    let tref = cat.lookup_table("default", "mem").await.unwrap();

    // Query == centroid → lower bound 0 < threshold ⇒ kept.
    let mut preds = HashMap::new();
    preds.insert(
        "embedding".to_string(),
        ColumnPrune::VectorDistance {
            query: vec![1.0, 0.0, 0.0],
            threshold: 0.5,
        },
    );
    let kept = cat
        .list_extents(
            table_id,
            tref.current_snapshot_id,
            &PrunePredicate {
                column_predicates: preds,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(kept.len(), 1);

    // Opposite query → lower bound 2.0, threshold tiny ⇒ pruned.
    let mut preds = HashMap::new();
    preds.insert(
        "embedding".to_string(),
        ColumnPrune::VectorDistance {
            query: vec![-1.0, 0.0, 0.0],
            threshold: 0.01,
        },
    );
    let dropped = cat
        .list_extents(
            table_id,
            tref.current_snapshot_id,
            &PrunePredicate {
                column_predicates: preds,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(dropped.len(), 0, "far extent pruned by ANN bound");
}

#[tokio::test]
async fn alter_table_add_column_evolves_schema() {
    let cat = fresh().await;
    let db = cat.create_database("default").await.unwrap();
    let table_id = cat
        .create_table(db, "logs", sample_schema(), TableConfig::default())
        .await
        .unwrap();

    cat.alter_table_add_column(table_id, "trace_id", "string")
        .await
        .unwrap();

    let cols = cat.get_table_columns("default", "logs").await.unwrap();
    assert_eq!(cols.len(), 4);
    assert!(cols
        .iter()
        .any(|c| c.name == "trace_id" && c.r#type == "string"));

    // Duplicate add is rejected.
    let dup = cat
        .alter_table_add_column(table_id, "trace_id", "string")
        .await;
    assert!(dup.is_err(), "duplicate column rejected");
}

#[tokio::test]
async fn graphs_round_trip() {
    let cat = fresh().await;
    cat.create_database("memory").await.unwrap();
    let reg = cat
        .create_graph(
            "memory",
            "mem_graph",
            GraphSpec::with_defaults("memory_nodes", "memory_edges"),
        )
        .await
        .unwrap();
    assert_eq!(reg.name, "mem_graph");

    let got = cat
        .get_graph("memory", "mem_graph")
        .await
        .unwrap()
        .expect("graph present");
    assert_eq!(got.node_table, "memory_nodes");
    assert_eq!(got.edge_table, "memory_edges");

    let listed = cat.list_graphs("memory").await.unwrap();
    assert_eq!(listed.len(), 1);

    assert!(cat.drop_graph("memory", "mem_graph").await.unwrap());
    assert!(cat
        .get_graph("memory", "mem_graph")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn users_round_trip() {
    let cat = fresh().await;
    assert_eq!(cat.count_users().await.unwrap(), 0);
    cat.create_user("admin", "hash123", "admin").await.unwrap();
    assert_eq!(cat.count_users().await.unwrap(), 1);

    let (user, hash) = cat
        .get_user_with_hash("admin")
        .await
        .unwrap()
        .expect("user exists");
    assert_eq!(user.role, "admin");
    assert_eq!(hash, "hash123");

    assert!(cat.set_user_role("admin", "viewer").await.unwrap());
    let (user, _) = cat.get_user_with_hash("admin").await.unwrap().unwrap();
    assert_eq!(user.role, "viewer");

    assert!(cat.delete_user("admin").await.unwrap());
    assert_eq!(cat.count_users().await.unwrap(), 0);
}

#[tokio::test]
async fn api_token_lookup_and_revoke() {
    let cat = fresh().await;
    let hash = b"token-hash-bytes";
    cat.insert_api_token(hash, "admin", Some("svc"), "api", None)
        .await
        .unwrap();

    let principal = cat
        .lookup_api_token(hash)
        .await
        .unwrap()
        .expect("token resolves");
    assert_eq!(principal.role, "admin");
    assert_eq!(principal.subject.as_deref(), Some("svc"));

    assert!(cat.revoke_api_token(hash).await.unwrap());
    assert!(
        cat.lookup_api_token(hash).await.unwrap().is_none(),
        "revoked token gone"
    );
}

#[tokio::test]
async fn idempotency_ledger_is_write_once() {
    let cat = fresh().await;
    let db = cat.create_database("default").await.unwrap();
    let table_id = cat
        .create_table(db, "logs", sample_schema(), TableConfig::default())
        .await
        .unwrap();
    let tref = cat.lookup_table("default", "logs").await.unwrap();

    let entry = IngestLedgerEntry {
        table_id,
        snapshot_id: tref.current_snapshot_id,
        rows_ingested: 10,
        bytes_written: 1024,
        applied_at: Utc::now(),
    };
    let first = cat
        .record_idempotency("k1", entry.clone(), chrono::Duration::hours(1))
        .await
        .unwrap();
    assert!(first.is_some(), "first record stored");

    // Second record under same key is a no-op (raced/duplicate).
    let second = cat
        .record_idempotency("k1", entry, chrono::Duration::hours(1))
        .await
        .unwrap();
    assert!(second.is_none(), "duplicate key ignored");

    let looked = cat
        .lookup_idempotency("k1")
        .await
        .unwrap()
        .expect("entry present");
    assert_eq!(looked.rows_ingested, 10);
}

#[tokio::test]
async fn background_tasks_claim_complete() {
    let cat = fresh().await;
    let node = NodeId::new();
    cat.submit_task("compaction", None, serde_json::json!({"x": 1}), 5)
        .await
        .unwrap();

    let claimed = cat
        .claim_task("compaction", node, chrono::Duration::minutes(5))
        .await
        .unwrap()
        .expect("a task is claimable");
    assert_eq!(claimed.kind, "compaction");
    assert_eq!(claimed.attempt, 1);

    // Nothing else to claim now.
    let none = cat
        .claim_task("compaction", node, chrono::Duration::minutes(5))
        .await
        .unwrap();
    assert!(
        none.is_none(),
        "claimed task not re-handed-out before lease expiry"
    );

    cat.complete_task(claimed.id).await.unwrap();
}

#[tokio::test]
async fn node_register_heartbeat_list() {
    let cat = fresh().await;
    let lease = cat
        .register_node(NodeInfo {
            role: NodeRole::AllInOne,
            endpoint: "127.0.0.1:50051".into(),
            capabilities: serde_json::json!({}),
        })
        .await
        .unwrap();
    cat.heartbeat(&lease).await.unwrap();
    let live = cat.list_live_nodes(60).await.unwrap();
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].endpoint, "127.0.0.1:50051");
    cat.deregister_node(lease).await.unwrap();
    assert_eq!(cat.list_live_nodes(60).await.unwrap().len(), 0);
}

#[tokio::test]
async fn dashboards_round_trip() {
    let cat = fresh().await;
    let dash = cat
        .create_dashboard("Ops", Some("ops board"))
        .await
        .unwrap();
    let with_panels = cat
        .get_dashboard(dash.id)
        .await
        .unwrap()
        .expect("dashboard present");
    assert_eq!(with_panels.dashboard.name, "Ops");
    assert_eq!(with_panels.panels.len(), 0);

    let listed = cat.list_dashboards().await.unwrap();
    assert_eq!(listed.len(), 1);

    assert!(cat.delete_dashboard(dash.id).await.unwrap());
    assert!(cat.get_dashboard(dash.id).await.unwrap().is_none());
}

// ---------------------------------------------------------------------------
// Index sidecars (extent_indexes) — parallel of pensieve-catalog's
// extent_indexes_it.rs, run against the embedded SQLite catalog.
// ---------------------------------------------------------------------------

fn sidecar_desc(
    tref: &TableRef,
    extent_id: ExtentId,
    column: &str,
    kind: pensieve_core::index_sidecar::SidecarKind,
    model: Option<&str>,
) -> pensieve_core::index_sidecar::IndexSidecarDescriptor {
    pensieve_core::index_sidecar::IndexSidecarDescriptor {
        id: Uuid::new_v4(),
        extent_id,
        table_id: tref.id,
        column: column.to_string(),
        kind,
        object_path: format!(
            "{DEFAULT_TENANT}/indexes/{extent_id}/{column}.{}",
            kind.as_str()
        ),
        byte_size: 2048,
        params: serde_json::json!({ "nlist": 16 }),
        embedding_model_id: model.map(str::to_string),
        created_at: Utc::now(),
    }
}

#[tokio::test]
async fn index_sidecars_register_list_reregister_delete() {
    use pensieve_core::index_sidecar::SidecarKind;

    let cat = fresh().await;
    let db = cat.create_database("default").await.unwrap();
    cat.create_table(db, "logs", sample_schema(), TableConfig::default())
        .await
        .unwrap();
    let tref = cat.lookup_table("default", "logs").await.unwrap();

    // Two committed extents.
    let m1 = extent_for(&tref, &["info"]);
    let m2 = extent_for(&tref, &["warn"]);
    let (e1, e2) = (m1.id, m2.id);
    for m in [m1, m2] {
        let mut txn = cat.begin_snapshot(tref.id).await.unwrap();
        txn.add_extent(m).await.unwrap();
        txn.commit(SnapshotSummary {
            operation: "ingest".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    }

    // Register: ANN (with model) + FTS (no model) on e1, FTS on e2.
    let ann = sidecar_desc(&tref, e1, "embedding", SidecarKind::IvfRabitq, Some("m1"));
    let fts1 = sidecar_desc(&tref, e1, "msg", SidecarKind::TantivyFts, None);
    let fts2 = sidecar_desc(&tref, e2, "msg", SidecarKind::TantivyFts, None);
    for d in [&ann, &fts1, &fts2] {
        cat.register_index_sidecar(DEFAULT_TENANT, d).await.unwrap();
    }

    // List + kind filter.
    let all = cat
        .list_index_sidecars(DEFAULT_TENANT, tref.id, &[e1, e2], None)
        .await
        .unwrap();
    assert_eq!(all.len(), 3);
    let fts_only = cat
        .list_index_sidecars(
            DEFAULT_TENANT,
            tref.id,
            &[e1, e2],
            Some(SidecarKind::TantivyFts),
        )
        .await
        .unwrap();
    assert_eq!(fts_only.len(), 2);
    assert!(cat
        .list_index_sidecars(DEFAULT_TENANT, tref.id, &[], None)
        .await
        .unwrap()
        .is_empty());

    // Idempotent re-register: upsert in place (NULL model key included).
    let mut fts1b = sidecar_desc(&tref, e1, "msg", SidecarKind::TantivyFts, None);
    fts1b.byte_size = 4242;
    fts1b.params = serde_json::json!({ "tokenizer": "en_stem" });
    cat.register_index_sidecar(DEFAULT_TENANT, &fts1b)
        .await
        .unwrap();
    let all = cat
        .list_index_sidecars(DEFAULT_TENANT, tref.id, &[e1, e2], None)
        .await
        .unwrap();
    assert_eq!(all.len(), 3, "re-register must upsert, not duplicate");
    let updated = all
        .iter()
        .find(|d| d.extent_id == e1 && d.kind == SidecarKind::TantivyFts)
        .unwrap();
    assert_eq!(updated.byte_size, 4242);
    assert_eq!(
        updated.params,
        serde_json::json!({ "tokenizer": "en_stem" })
    );
    assert_eq!(
        updated.embedding_model_id, None,
        "model id round-trips as NULL"
    );

    // A second model on the same column+kind coexists.
    let ann2 = sidecar_desc(&tref, e1, "embedding", SidecarKind::IvfRabitq, Some("m2"));
    cat.register_index_sidecar(DEFAULT_TENANT, &ann2)
        .await
        .unwrap();
    assert_eq!(
        cat.list_index_sidecars(DEFAULT_TENANT, tref.id, &[e1], None)
            .await
            .unwrap()
            .len(),
        3
    );

    // delete_for_extents returns e1's paths and leaves e2 intact.
    let mut paths = cat
        .delete_index_sidecars_for_extents(DEFAULT_TENANT, &[e1])
        .await
        .unwrap();
    paths.sort();
    let mut expected = vec![
        ann.object_path.clone(),
        fts1.object_path.clone(),
        ann2.object_path.clone(),
    ];
    expected.sort();
    assert_eq!(paths, expected);
    let left = cat
        .list_index_sidecars(DEFAULT_TENANT, tref.id, &[e1, e2], None)
        .await
        .unwrap();
    assert_eq!(left.len(), 1);
    assert_eq!(left[0].extent_id, e2);

    // Hard-deleting the extent rows removes the remaining sidecar rows too
    // (no FK cascade in SQLite — delete_extent_rows does it explicitly).
    cat.delete_extent_rows(&[e2]).await.unwrap();
    assert!(cat
        .list_index_sidecars(DEFAULT_TENANT, tref.id, &[e1, e2], None)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn tenant_quota_round_trip_and_upsert_in_place() {
    let cat = fresh().await;
    let t = DEFAULT_TENANT;

    // Default: no row ⇒ None / empty (the env-global limits apply).
    assert!(cat.get_tenant_quota(t).await.unwrap().is_none());
    assert!(cat.list_tenant_quotas().await.unwrap().is_empty());

    // Insert.
    cat.upsert_tenant_quota(&pensieve_core::catalog::TenantQuota {
        tenant: t,
        max_query_concurrent: Some(5),
        max_agent_concurrent: None,
        updated_at: chrono::Utc::now(),
    })
    .await
    .unwrap();
    let got = cat
        .get_tenant_quota(t)
        .await
        .unwrap()
        .expect("quota present");
    assert_eq!(got.max_query_concurrent, Some(5));
    assert_eq!(got.max_agent_concurrent, None);
    assert_eq!(cat.list_tenant_quotas().await.unwrap().len(), 1);

    // Upsert again updates IN PLACE (no duplicate PK row).
    cat.upsert_tenant_quota(&pensieve_core::catalog::TenantQuota {
        tenant: t,
        max_query_concurrent: Some(10),
        max_agent_concurrent: Some(2),
        updated_at: chrono::Utc::now(),
    })
    .await
    .unwrap();
    let got = cat.get_tenant_quota(t).await.unwrap().unwrap();
    assert_eq!(got.max_query_concurrent, Some(10));
    assert_eq!(got.max_agent_concurrent, Some(2));
    assert_eq!(
        cat.list_tenant_quotas().await.unwrap().len(),
        1,
        "upsert must update in place, not insert a second row"
    );
}

#[tokio::test]
async fn gc_history_keeps_the_current_snapshot_and_live_extents() {
    let cat = fresh().await;
    let db = cat.create_database("default").await.unwrap();
    let table_id = cat
        .create_table(db, "logs", sample_schema(), TableConfig::default())
        .await
        .unwrap();
    let tref = cat.lookup_table("default", "logs").await.unwrap();
    for _ in 0..3 {
        let mut txn = cat.begin_snapshot(table_id).await.unwrap();
        txn.add_extent(extent_for(&tref, &["info"])).await.unwrap();
        txn.commit(SnapshotSummary {
            operation: "ingest".into(),
            rows_added: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    }

    // Negative age: every snapshot written in this test is old enough.
    let stats = cat
        .gc_history(chrono::Duration::seconds(-5), 100)
        .await
        .unwrap();
    // bootstrap + 3 commits = 4 snapshots; the current one stays.
    // Bootstrap has no manifest row. The current commit's manifest stays,
    // so the two superseded commits are the ones reclaimed.
    assert_eq!(stats.snapshots_deleted, 3);
    assert_eq!(stats.manifests_deleted, 2);

    let tref = cat.lookup_table("default", "logs").await.unwrap();
    let live = cat
        .list_extents(
            table_id,
            tref.current_snapshot_id,
            &PrunePredicate::default(),
        )
        .await
        .unwrap();
    assert_eq!(live.len(), 3, "live extents survive snapshot gc");

    // The kept snapshot is a valid parent for the next commit.
    let mut txn = cat.begin_snapshot(table_id).await.unwrap();
    txn.add_extent(extent_for(&tref, &["warn"])).await.unwrap();
    txn.commit(SnapshotSummary {
        operation: "ingest".into(),
        rows_added: 1,
        ..Default::default()
    })
    .await
    .unwrap();
}
