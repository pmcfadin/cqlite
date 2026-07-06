//! Issue #1578 (Epic D / D2): the `max_result_rows` row-count valve is demoted to
//! a safety valve — a query with an EXPLICIT `LIMIT` is exempt (the user bounded
//! the result themselves), so a big-but-legal `SELECT ... LIMIT N` returns rows
//! instead of erroring with the "result set too large" cliff.
//!
//! Rather than build a 1.5M-row fixture, this lowers `max_result_rows` to 2 via
//! config (the documented "lower the constant via a test-visible mechanism") and
//! proves:
//!   * `SELECT * ... LIMIT 4` over 4 rows RETURNS 4 rows (exempt) — RED on `main`,
//!     which trips the valve at 4 > 2 even with an explicit LIMIT.
//!   * `SELECT * ...` (no LIMIT) over 4 rows STILL errors — the valve remains a
//!     genuine safety net for unbounded materialization.
//!
//! Run:
//!   cargo test --package cqlite-core \
//!     --features write-support,cli-helpers,state_machine \
//!     --test issue_1578_limit_exempts_max_results

#![cfg(all(
    feature = "write-support",
    feature = "cli-helpers",
    feature = "state_machine",
    not(feature = "tombstones")
))]

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use cqlite_core::{Config, Database};
use tempfile::TempDir;

const KS: &str = "limit_ks";
const TBL: &str = "rows";

fn schema_cql() -> String {
    format!("CREATE TABLE {KS}.{TBL} (\n  id int PRIMARY KEY,\n  v int\n);\n")
}

fn write_mutation(id: i32, ts: i64) -> Mutation {
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![CellOperation::Write {
        column: "v".to_string(),
        value: Value::Integer(id),
    }];
    Mutation::new(TableId::new(KS, TBL), pk, None, ops, ts, None)
}

/// Open with `max_result_rows` lowered so a tiny fixture exercises the valve.
async fn open_with_row_cap(n_rows: i32, max_result_rows: u64) -> (Database, TempDir) {
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
            for id in 0..n_rows {
                engine
                    .write(write_mutation(id, 100 + id as i64))
                    .expect("write");
            }
            rt.block_on(engine.flush())
                .expect("flush")
                .expect("must produce an SSTable");
            rt.block_on(engine.close()).expect("close");
        })
        .await
        .expect("fixture build task");
    }

    let mut core_config = Config::default();
    core_config.query.max_result_rows = max_result_rows;

    let result = ingest(IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir,
        version_hint: None,
        core_config,
        table_directory_filter: None,
    })
    .await
    .expect("ingest fixture");
    (result.database, temp_dir)
}

/// A query with an EXPLICIT LIMIT is exempt from the row-count valve.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_limit_exempts_row_count_valve() {
    let (db, _tmp) = open_with_row_cap(4, 2).await;

    let result = db
        .execute(&format!("SELECT * FROM {KS}.{TBL} LIMIT 4"))
        .await
        .expect(
            "Issue #1578: an explicit LIMIT must exempt the query from the \
             max_result_rows safety valve — LIMIT 4 over 4 rows must return rows, \
             not error with 'result set too large'",
        );
    assert_eq!(
        result.rows.len(),
        4,
        "LIMIT 4 over a 4-row table returns all 4 rows"
    );
}

/// Without a LIMIT, the valve is still a genuine safety net.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_limit_still_trips_row_count_valve() {
    let (db, _tmp) = open_with_row_cap(4, 2).await;

    let err = db
        .execute(&format!("SELECT * FROM {KS}.{TBL}"))
        .await
        .err()
        .expect(
            "Issue #1578: an UNBOUNDED SELECT over 4 rows with max_result_rows=2 \
             must still trip the safety valve",
        );
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("too large") || msg.contains("limit"),
        "expected a result-too-large error, got: {err}"
    );
}
