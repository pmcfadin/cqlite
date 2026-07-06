//! Issue #1578 (Epic D / D2): a GROUP-BY-free aggregate must fold the scan stream
//! into an O(1) accumulator, NOT buffer the whole table.
//!
//! This is the memory guard. It uses the cfg-gated aggregate-buffering probe
//! (`cqlite_core::query::agg_stream_probe`) to observe the number of rows the
//! aggregate held resident in its input buffer — the no-heuristics way to prove
//! O(1): observe the *work*, not just the (identical) `COUNT(*)` answer.
//!
//! On `main`/pre-D2 `COUNT(*)` over N rows buffered all N (`buffered_rows() == N`),
//! so this guard flips red as the fixture grows. After D2 the fold buffers ZERO
//! rows regardless of table size, so `buffered_rows()` stays `0` and is FLAT
//! between a small and a large fixture. A companion `SELECT *` (which genuinely
//! materializes) proves the probe is actually wired.
//!
//! The counter getter + reset live behind the `work-counters` feature; the counter
//! is a shared process-global, so every test here serializes on a shared mutex.
//!
//! Run:
//!   cargo test --package cqlite-core \
//!     --features write-support,cli-helpers,state_machine,work-counters \
//!     --test issue_1578_aggregate_o1_memory

#![cfg(all(
    feature = "write-support",
    feature = "cli-helpers",
    feature = "state_machine",
    feature = "work-counters",
    not(feature = "tombstones")
))]

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::agg_stream_probe;
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use cqlite_core::{Config, Database};
use serial_test::serial;
use tempfile::TempDir;

const KS: &str = "agg_mem_ks";
const TBL: &str = "rows";

fn schema_cql() -> String {
    format!("CREATE TABLE {KS}.{TBL} (\n  id int PRIMARY KEY,\n  v int\n);\n")
}

fn write_mutation(id: i32, ts: i64) -> Mutation {
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![CellOperation::Write {
        column: "v".to_string(),
        value: Value::Integer(id * 2),
    }];
    Mutation::new(TableId::new(KS, TBL), pk, None, ops, ts, None)
}

async fn open_with_n_rows(n: i32) -> (Database, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    let schema_path = temp_dir.path().join("schema.cql");
    std::fs::write(&schema_path, schema_cql()).expect("write schema file");

    {
        let data_dir = data_dir.clone();
        let wal_dir = wal_dir.clone();
        tokio::task::spawn_blocking(move || {
            use cqlite_core::schema::parse_cql_schema;
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio runtime");
            let schema = parse_cql_schema(&schema_cql()).expect("parse schema");
            let config = WriteEngineConfig::new(data_dir, wal_dir, schema);
            let mut engine = WriteEngine::new(config).expect("engine");
            for id in 0..n {
                engine.write(write_mutation(id, 100 + id as i64)).expect("write");
            }
            rt.block_on(engine.flush())
                .expect("flush")
                .expect("must produce an SSTable");
            rt.block_on(engine.close()).expect("close");
        })
        .await
        .expect("fixture build task");
    }

    let result = ingest(IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir,
        version_hint: None,
        core_config: Config::default(),
        table_directory_filter: None,
    })
    .await
    .expect("ingest fixture");
    (result.database, temp_dir)
}

async fn count_star_buffered_rows(db: &Database) -> u64 {
    agg_stream_probe::reset();
    let result = db
        .execute(&format!("SELECT COUNT(*) FROM {KS}.{TBL}"))
        .await
        .expect("COUNT(*) executes");
    assert_eq!(result.rows.len(), 1, "COUNT(*) yields one row");
    agg_stream_probe::buffered_rows()
}

/// COUNT(*) peak resident rows is O(1): flat between a small and a large fixture,
/// and near-zero — NOT proportional to row count.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn count_star_buffers_o1_rows() {
    let (small_db, _s) = open_with_n_rows(50).await;
    let (large_db, _l) = open_with_n_rows(2000).await;

    let small_buffered = count_star_buffered_rows(&small_db).await;
    let large_buffered = count_star_buffered_rows(&large_db).await;

    // Flat across a 40x size difference — the discriminating property. On `main`
    // this is 50 vs 2000 (proportional to rows); after D2 it is 0 vs 0.
    assert_eq!(
        small_buffered, large_buffered,
        "Issue #1578: COUNT(*) buffered rows must be FLAT across fixture sizes \
         (small={small_buffered}, large={large_buffered}); a size-proportional \
         count means the whole table was materialized before aggregating"
    );
    // And O(1) in absolute terms (the fold holds only the accumulator).
    assert!(
        large_buffered <= 1,
        "Issue #1578: COUNT(*) over 2000 rows must buffer O(1) rows, got {large_buffered}"
    );
}

/// Sanity: the probe is genuinely wired — a materializing `SELECT *` DOES buffer
/// its rows, so a zero from the aggregate path above is a real O(1) result, not a
/// dead counter.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn probe_is_wired_via_group_by_aggregate() {
    let (db, _tmp) = open_with_n_rows(300).await;

    // A GROUP BY aggregate still materializes (E5 territory), so it buffers its
    // input — proving the probe increments on the buffered path.
    agg_stream_probe::reset();
    let _ = db
        .execute(&format!("SELECT v, COUNT(*) FROM {KS}.{TBL} GROUP BY v"))
        .await
        .expect("GROUP BY executes");
    let grouped_buffered = agg_stream_probe::buffered_rows();
    assert!(
        grouped_buffered >= 300,
        "Issue #1578: a buffered GROUP BY over 300 rows must record >= 300 buffered \
         rows (got {grouped_buffered}); if this is 0 the probe is not wired and the \
         O(1) assertion above is vacuous"
    );
}
