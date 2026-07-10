//! Observability proof for the partition point-read path (issue #2207, Stage 4.3).
//!
//! A full-PK-equality `do_get` must report the `streaming_partition_lookup`
//! access-path label on `cqlite.query.rows_scanned` (core
//! `AccessPath::StreamingPartitionLookup`), distinguishing the point path from
//! the scan path's `full_scan` — the signal the field harness reads to confirm
//! the pushdown did I/O-level work. On `main` the same PK query reports
//! `full_scan` (no point path), so this label assertion fails before the change.
//!
//! Its OWN test binary (separate process) so the process-global in-memory meter
//! provider is never shared with `metrics_capture_test.rs` (that harness's
//! documented contamination hazard). Gated behind `observability-testing` like
//! its sibling.
//!
//! ```text
//! cargo test -p cqlite-flight --features observability-testing --test point_read_metrics_test
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

const KS: &str = "point_read_metrics_ks";
const TBL: &str = "items";
const DDL: &str =
    "CREATE TABLE point_read_metrics_ks.items (id int PRIMARY KEY, name text, score int)";

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

/// A ticket carrying a full-PK equality (`id = <id>`) → the point route.
fn pk_eq_ticket(id: i32) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "keyspace": KS,
        "table": TBL,
        "ddl": DDL,
        "filter": {"type": "Compare", "column": "id", "op": "Equal", "value": id},
    }))
    .unwrap()
}

#[test]
fn point_read_do_get_reports_streaming_partition_lookup_access_path() {
    let mc = testing::metrics_capture();
    let (_temp, data_dir) = build_fixture();
    let svc = CqliteFlightService::new(data_dir, 4);

    mc.reset();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let resp = svc
            .do_get(Request::new(Ticket::new(pk_eq_ticket(3))))
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
        .expect("cqlite.query.rows_scanned must be emitted by the point read");

    // Every access_path attribute on the point read's rows_scanned points must be
    // the bounded `streaming_partition_lookup` label (never `full_scan`).
    let mut saw_point_label = false;
    for p in &scanned.points {
        let ap = p
            .attributes
            .iter()
            .find(|(k, _)| k == catalog::attr::ACCESS_PATH)
            .map(|(_, v)| v.as_str());
        assert_eq!(
            ap,
            Some("streaming_partition_lookup"),
            "a full-PK-equality do_get must report the point-read access path (got {ap:?})"
        );
        saw_point_label = true;
    }
    assert!(
        saw_point_label,
        "the point read must emit at least one rows_scanned point carrying the access-path label"
    );
}
