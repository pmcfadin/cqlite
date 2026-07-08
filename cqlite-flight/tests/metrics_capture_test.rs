//! Issue #2162 — OTel-level assertions through the shared `observability-testing`
//! capture harness (`cqlite_core::observability::testing`).
//!
//! A SEPARATE integration-test binary/process (roborev finding, matching the
//! #2163 precedent for `cqlite-core/tests/observability_correctness.rs`): the
//! capture harness installs a PROCESS-GLOBAL in-memory meter provider on first
//! use, so sharing it with `cqlite-flight`'s parallel `cargo test --lib`
//! unit-test binary would risk cross-test metric contamination/flake. Building
//! its own SSTable fixtures directly via `cqlite_core::storage::write_engine`
//! (mirroring `do_get_transport_test.rs`) rather than the crate's `testutil`,
//! which is `#[cfg(test)]`-gated to the crate's own unit-test build and so is
//! not visible to an external integration-test binary.
//!
//! The feature-independent `StreamProbe`/`ScanProgress` seam tests in
//! `cqlite-flight/src/streaming_tests.rs` carry the always-compiled
//! (feature-off-safe) wiring evidence; THIS file additionally reads back the
//! actual emitted OTel series to prove:
//!
//! * `cqlite.rpc.phase.duration` records a bounded `merge_setup` phase sample
//!   over a completed `do_get` (Stage 2), every phase value is in the closed
//!   set, and no phase sample carries an unbounded attribute (Stage 2/4),
//! * `cqlite.rpc.rows` and `cqlite.query.rows_scanned` are emitted, carrying
//!   only their bounded attribute keys (Stage 1/3/4).
//!
//! Run with:
//! ```text
//! cargo test -p cqlite-flight --features observability-testing --test metrics_capture_test
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

const KS: &str = "metrics_capture_ks";
const TBL: &str = "items";
const DDL: &str =
    "CREATE TABLE metrics_capture_ks.items (id int PRIMARY KEY, name text, score int)";

/// The bounded attribute keys any #2162 metric may carry — the invariant the
/// no-unbounded-attribute scenario asserts (Stage 4.1).
const BOUNDED_KEYS: &[&str] = &[
    catalog::attr::RPC_METHOD,
    catalog::attr::RPC_PHASE,
    catalog::attr::RPC_STATUS,
    catalog::attr::ACCESS_PATH,
    catalog::attr::SSTABLE_FORMAT,
];

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

/// Flush a 12-row single-SSTable fixture, large enough (batch_size = 4) to
/// exercise several record batches.
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

/// The on-the-wire ticket is JSON (the `#[non_exhaustive]` `FlightTicket` is
/// only constructible inside the crate); build the same bytes the connector
/// would send.
fn ticket_bytes() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "keyspace": KS,
        "table": TBL,
        "ddl": DDL,
    }))
    .unwrap()
}

/// Assert every attribute key on a collected metric point is in the bounded set
/// (never a ticket, partition key, token, or query string).
fn assert_bounded_attrs(attrs: &[(String, String)], metric: &str) {
    for (k, _) in attrs {
        assert!(
            BOUNDED_KEYS.contains(&k.as_str()),
            "metric {metric} carries unbounded attribute key {k:?}"
        );
    }
}

/// Run a full `do_get` over a multi-row fixture and read back the emitted
/// metrics, asserting the #2162 incremental + phase series and the bounded-
/// attribute invariant.
#[test]
fn do_get_emits_bounded_phase_and_incremental_metrics() {
    // Install the in-memory meter provider BEFORE any metric is recorded in this
    // process (this integration test binary owns its own process, so no other
    // crate test can have bound the global meter first).
    let mc = testing::metrics_capture();

    let (_temp, data_dir) = build_fixture();
    let svc = CqliteFlightService::new(data_dir, 4); // batch_size 4 → 3 batches

    mc.reset();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let resp = svc
            .do_get(Request::new(Ticket::new(ticket_bytes())))
            .await
            .expect("do_get");
        // Drain the whole stream so the merge completes and every phase +
        // incremental counter is flushed.
        let mut stream = resp.into_inner();
        let mut msgs = 0usize;
        while let Some(item) = stream.next().await {
            item.expect("stream item ok");
            msgs += 1;
        }
        assert!(msgs > 0, "do_get must yield at least the schema message");
    });

    let metrics = mc.flush_and_collect();

    // --- Stage 2: a bounded merge_setup phase sample is recorded ---------------
    let phase = metrics
        .find(catalog::RPC_PHASE_DURATION)
        .expect("cqlite.rpc.phase.duration must be recorded over a completed do_get");
    assert_eq!(
        metrics.unit(catalog::RPC_PHASE_DURATION),
        Some(catalog::unit::SECONDS)
    );
    let merge_setup_samples = phase
        .points
        .iter()
        .filter(|p| {
            p.attributes
                .iter()
                .any(|(k, v)| k == catalog::attr::RPC_PHASE && v == "merge_setup")
        })
        .count();
    assert!(
        merge_setup_samples >= 1,
        "a merge_setup-tagged phase sample must exist (the #2157 stall localizer)"
    );

    // Every phase value is one of the closed set; no ticket/key/query attribute.
    for p in &phase.points {
        let phase_val = p
            .attributes
            .iter()
            .find(|(k, _)| k == catalog::attr::RPC_PHASE)
            .map(|(_, v)| v.as_str());
        assert!(
            matches!(phase_val, Some("resolve" | "merge_setup" | "stream")),
            "phase value must be in the closed set, got {phase_val:?}"
        );
        assert_bounded_attrs(&p.attributes, catalog::RPC_PHASE_DURATION);
    }

    // --- Stage 1: rpc.rows emitted, bounded attrs -----------------------------
    assert!(
        metrics.counter_sum(catalog::RPC_ROWS) >= 12.0,
        "cqlite.rpc.rows must have accumulated the streamed rows, got {}",
        metrics.counter_sum(catalog::RPC_ROWS)
    );
    if let Some(rpc_rows) = metrics.find(catalog::RPC_ROWS) {
        for p in &rpc_rows.points {
            assert_bounded_attrs(&p.attributes, catalog::RPC_ROWS);
        }
    }

    // --- Stage 3: query.rows_scanned emitted, only the access_path attr --------
    let scanned = metrics
        .find(catalog::QUERY_ROWS_SCANNED)
        .expect("cqlite.query.rows_scanned must be emitted by the merge scan");
    assert!(
        metrics.counter_sum(catalog::QUERY_ROWS_SCANNED) >= 12.0,
        "rows_scanned must reflect the examined rows"
    );
    for p in &scanned.points {
        assert_bounded_attrs(&p.attributes, catalog::QUERY_ROWS_SCANNED);
        let ap = p
            .attributes
            .iter()
            .find(|(k, _)| k == catalog::attr::ACCESS_PATH)
            .map(|(_, v)| v.as_str());
        assert_eq!(
            ap,
            Some("full_scan"),
            "the Flight merge scan reports the bounded full_scan access path"
        );
    }

    // --- Stage 4/roborev: read.rows / read.partitions are format-agnostic ------
    // (the k-way merge reconciles across possibly mixed-format input SSTables
    // before this counter's grain, so no attributes are attached — see
    // catalog::READ_ROWS's doc comment).
    if let Some(read_rows) = metrics.find(catalog::READ_ROWS) {
        for p in &read_rows.points {
            assert!(
                p.attributes.is_empty(),
                "the Flight merge-scan cqlite.read.rows emission must carry no \
                 attributes (format-agnostic contract), got {:?}",
                p.attributes
            );
        }
    }
    if let Some(read_partitions) = metrics.find(catalog::READ_PARTITIONS) {
        for p in &read_partitions.points {
            assert!(
                p.attributes.is_empty(),
                "the Flight merge-scan cqlite.read.partitions emission must carry \
                 no attributes (format-agnostic contract), got {:?}",
                p.attributes
            );
        }
    }
}
