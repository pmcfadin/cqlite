//! Finding 3 (roborev job 1616, issue #2207): access-path label FIDELITY for a
//! MULTI-key point read.
//!
//! A full-PK `IN (...)` `do_get` whose keys survive token filtering with >1 key
//! must report the `multi_partition_lookup` access-path label on
//! `cqlite.query.rows_scanned` (core `AccessPath::MultiPartitionLookup`) — NOT
//! the single-key `streaming_partition_lookup` constant the point path used to
//! hard-code for EVERY route. This label is the field-run evidence of WHICH
//! targeted path ran, so it must reflect the route shape. The single-key case
//! (`streaming_partition_lookup`) is asserted by the sibling
//! `point_read_metrics_test.rs`.
//!
//! Its OWN test binary (separate process) so the process-global in-memory meter
//! provider is never shared with `point_read_metrics_test.rs` /
//! `metrics_capture_test.rs` (that harness's documented cumulative-aggregation
//! contamination hazard). Gated behind `observability-testing` like its siblings.
//!
//! ```text
//! cargo test -p cqlite-flight --features observability-testing --test point_read_multi_metrics_test
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

const KS: &str = "point_read_multi_metrics_ks";
const TBL: &str = "items";
const DDL: &str =
    "CREATE TABLE point_read_multi_metrics_ks.items (id int PRIMARY KEY, name text, score int)";

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
/// route.
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
fn multi_key_point_read_reports_multi_partition_lookup_access_path() {
    let mc = testing::metrics_capture();
    let (_temp, data_dir) = build_fixture();
    let svc = CqliteFlightService::new(data_dir, 4);

    mc.reset();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // Two DISTINCT keys, both present and (no token range on the ticket) both
        // surviving token filtering → a >1-key point route.
        let resp = svc
            .do_get(Request::new(Ticket::new(pk_in_ticket(&[3, 5]))))
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
        .expect("cqlite.query.rows_scanned must be emitted by the multi-key point read");

    // Every access_path attribute on the multi-key read's rows_scanned points must
    // be the `multi_partition_lookup` label (never the single-key
    // `streaming_partition_lookup`, never `full_scan`).
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
            "a >1-key IN point read must report multi_partition_lookup (got {ap:?})"
        );
        saw_point_label = true;
    }
    assert!(
        saw_point_label,
        "the multi-key point read must emit at least one rows_scanned point carrying the label"
    );
}
