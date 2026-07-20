//! Issue #2684 — cold-start invariant (#2385) for `cqlite.flight.tables_discovered`.
//!
//! The saturation sampler's table-discovery walk is readdir-only: it must NEVER
//! open, stat-for-generation, or parse an SSTable. `cqlite.sstable.index_parses_total`
//! is incremented at exactly one site (a full Index.db parse), so N sampler ticks
//! over a POPULATED data-dir must show a zero delta on that counter — and the
//! `tables_discovered` gauge must reflect the genuine on-disk table count.
//!
//! Uses the shared `observability-testing` capture harness (a process-global
//! in-memory meter provider), so this lives in its OWN integration-test binary
//! (matching the `metrics_capture_test.rs` precedent).
//!
//! Run with:
//! ```text
//! cargo test -p cqlite-flight --features observability-testing --test issue_2684_tables_discovered_coldstart_test
//! ```

#![cfg(feature = "observability-testing")]

use std::time::Duration;

use cqlite_core::observability::{catalog, testing};
use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;

const KS: &str = "coldstart_ks";
const TBL: &str = "items";

fn schema() -> TableSchema {
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
        columns: vec![col("id", "int", false), col("name", "text", true)],
        comments: std::collections::HashMap::new(),
        dropped_columns: std::collections::HashMap::new(),
    }
}

fn write_row(id: i32, name: &str) -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("id", Value::Integer(id)),
        None,
        vec![CellOperation::Write {
            column: "name".into(),
            value: Value::text(name),
        }],
        100,
        None,
    )
}

/// Flush a populated single-SSTable fixture (a real Index.db present) and return
/// its data-dir.
fn build_populated_fixture() -> (tempfile::TempDir, std::path::PathBuf) {
    let temp = tempfile::TempDir::new().unwrap();
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema());
    let mut engine = WriteEngine::new(config).expect("engine");
    for i in 1..=32 {
        engine.write(write_row(i, &format!("n{i}"))).expect("write");
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(engine.flush()).expect("flush").expect("info");
    (temp, data_dir)
}

#[test]
fn sampler_ticks_produce_zero_index_parse_delta_and_count_tables() {
    // Install the in-memory meter provider BEFORE any metric is recorded.
    let mc = testing::metrics_capture();

    let (_temp, data_dir) = build_populated_fixture();

    mc.reset();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // Run several sampler ticks over the populated dir with NO query activity.
        // A tiny interval keeps the test fast; shutdown resolves after a bounded
        // number of ticks have certainly occurred.
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let handle = tokio::spawn(cqlite_flight::saturation::run_sampler(
            Duration::from_millis(5),
            data_dir.clone(),
            async move {
                let _ = rx.await;
            },
        ));
        // Let the sampler tick many times, then stop it.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let _ = tx.send(());
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("sampler resolves after shutdown")
            .expect("sampler task ok");
    });

    let metrics = mc.flush_and_collect();

    // COLD-START INVARIANT (#2385): the discovery walk opened/parsed nothing, so
    // the full-Index.db-parse counter shows zero delta from the sampling.
    let index_parses = metrics.counter_sum(catalog::INDEX_PARSES_TOTAL);
    assert_eq!(
        index_parses, 0.0,
        "the readdir-only discovery walk must produce zero index_parses delta \
         (cold-start invariant #2385); got {index_parses}"
    );

    // And the gauge is emitted with the genuine table count (one table dir).
    assert!(
        metrics.contains(catalog::FLIGHT_TABLES_DISCOVERED),
        "the sampler must emit cqlite.flight.tables_discovered"
    );
    assert_eq!(
        metrics.unit(catalog::FLIGHT_TABLES_DISCOVERED),
        Some("{entry}"),
        "tables_discovered carries the {{entry}} unit"
    );
    // DELTA temporality: the last-collected gauge value is the current count (1).
    let last = metrics
        .find(catalog::FLIGHT_TABLES_DISCOVERED)
        .and_then(|m| m.points.last())
        .map(|p| p.value)
        .expect("a tables_discovered data point");
    assert_eq!(
        last, 1.0,
        "the populated fixture has exactly one genuine table dir"
    );
}
