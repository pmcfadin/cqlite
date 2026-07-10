//! Roborev job 1623 (issue #2207): access-path label FIDELITY when a multi-key
//! point route DEDUPS/PRUNES down to a SINGLE surviving key.
//!
//! The access-path label is the router's decision (the ROUTE VARIANT
//! `detect_route` chose), NOT the surviving-key count. A full-PK `IN (...)` list
//! is a `MultiPartitionPointRead` route; it must report `multi_partition_lookup`
//! EVEN when the list dedups (or token-prunes) to a single surviving key. The old
//! code derived the label from `key_bytes.len()` AFTER dedup/pruning, so a
//! duplicate-collapsing `IN (3, 3)` was mislabeled `streaming_partition_lookup` —
//! this test is the RED case that pins the corrected convention.
//!
//! Its OWN test binary (separate process) so the process-global in-memory meter
//! provider is never shared with the sibling point-read metrics binaries (that
//! harness's documented cumulative-aggregation contamination hazard). Gated behind
//! `observability-testing` like its siblings.
//!
//! ```text
//! cargo test -p cqlite-flight --features observability-testing --test point_read_dedup_metrics_test
//! ```

#![cfg(feature = "observability-testing")]

use std::collections::HashMap;

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Ticket;
use futures::StreamExt;
use tonic::Request;

use cqlite_core::observability::{catalog, testing};
use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use cqlite_flight::service::CqliteFlightService;

const KS: &str = "point_read_dedup_metrics_ks";
const TBL: &str = "items";
const DDL: &str =
    "CREATE TABLE point_read_dedup_metrics_ks.items (id int PRIMARY KEY, name text, score int)";

fn simple_schema() -> TableSchema {
    let col = |name: &str, ty: &str, nullable: bool| Column {
        name: name.into(),
        data_type: ty.into(),
        nullable,
        default: None,
        is_static: false,
    };
    TableSchema {
        keyspace: KS.into(),
        table: TBL.into(),
        partition_keys: vec![KeyColumn {
            name: "id".into(),
            data_type: "int".into(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            col("id", "int", false),
            col("name", "text", true),
            col("score", "int", true),
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn write_row(id: i32, name: &str, score: i32, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("id", Value::Integer(id)),
        None,
        vec![
            CellOperation::Write {
                column: "name".into(),
                value: Value::Text(name.into()),
            },
            CellOperation::Write {
                column: "score".into(),
                value: Value::Integer(score),
            },
        ],
        ts,
        None,
    )
}

fn build_fixture() -> (tempfile::TempDir, std::path::PathBuf) {
    let schema = simple_schema();
    let temp = tempfile::TempDir::new().unwrap();
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema);
    let mut engine = WriteEngine::new(config).expect("engine");
    for i in 1..=12 {
        engine
            .write(write_row(i, &format!("n{i}"), i * 10, 100))
            .expect("write");
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(engine.flush()).expect("flush").expect("info");
    (temp, data_dir)
}

/// A ticket carrying a full-PK `IN` list (`id IN (...)`) → the multi-key point
/// route. Passing a DUPLICATE-heavy list here (`IN (3, 3)`) is the point: the
/// router dedups it to ONE surviving key, but the ROUTE VARIANT is still
/// multi-partition.
fn pk_in_ticket(ids: &[i32]) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "keyspace": KS,
        "table": TBL,
        "ddl": DDL,
        "filter": {"type": "In", "column": "id", "values": ids},
    }))
    .unwrap()
}

#[test]
fn in_list_that_dedups_to_one_key_still_reports_multi_partition_lookup() {
    let mc = testing::metrics_capture();
    let (_temp, data_dir) = build_fixture();
    let svc = CqliteFlightService::new(data_dir, 4);

    mc.reset();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // `IN (3, 3)` — a full-PK IN list whose two entries DEDUP to a single
        // surviving key. The label must reflect the multi-partition ROUTE VARIANT,
        // not the one surviving key (the red case: old code labeled this
        // `streaming_partition_lookup` off `key_bytes.len() == 1`).
        let resp = svc
            .do_get(Request::new(Ticket::new(pk_in_ticket(&[3, 3]))))
            .await
            .expect("do_get");
        let mut stream = resp.into_inner();
        let mut msgs = 0usize;
        while let Some(item) = stream.next().await {
            item.expect("stream item ok");
            msgs += 1;
        }
        assert!(msgs > 0, "do_get must yield at least the schema message");
    });

    let metrics = mc.flush_and_collect();
    let scanned = metrics
        .find(catalog::QUERY_ROWS_SCANNED)
        .expect("cqlite.query.rows_scanned must be emitted by the dedup point read");

    // Every access_path attribute must be `multi_partition_lookup` (the router's
    // route-variant decision) — NEVER the single-key `streaming_partition_lookup`
    // just because the IN list collapsed to one surviving key.
    let mut saw_point_label = false;
    for p in &scanned.points {
        let ap = p
            .attributes
            .iter()
            .find(|(k, _)| k == catalog::attr::ACCESS_PATH)
            .map(|(_, v)| v.as_str());
        assert_eq!(
            ap,
            Some("multi_partition_lookup"),
            "an IN list that dedups to one surviving key must STILL report \
             multi_partition_lookup (route variant), got {ap:?}"
        );
        saw_point_label = true;
    }
    assert!(
        saw_point_label,
        "the dedup point read must emit at least one rows_scanned point carrying the label"
    );
}
