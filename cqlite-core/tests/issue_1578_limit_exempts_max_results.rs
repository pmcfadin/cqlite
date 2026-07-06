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
//!   * `SELECT * ... LIMIT 4` with a tiny `max_result_bytes` STILL errors with
//!     `Error::ResultTooLarge` — the row-count valve is exempted for an explicit
//!     LIMIT, but the BYTE budget (the primary guard, issue #1582) is NOT. This
//!     pins the exact contract the demotion leans on: LIMIT bypasses the
//!     secondary row-count valve only, never the byte ceiling.
//!
//! ## Roborev follow-up: pinning the demotion at NON-TRIVIAL scale
//!
//! The tests above use a 4-row fixture, which cannot by itself distinguish "the
//! row-count valve is exempted" from "4 rows happens to fit under any reasonable
//! byte budget regardless". Two more tests repeat the same shape over a
//! few-thousand-row fixture (the existing write-engine generator, same pattern as
//! `issue_1578_aggregate_o1_memory.rs`'s `open_with_n_rows`):
//!
//!   * `large_limit_returns_all_rows_at_scale` — a huge `LIMIT` (far above the row
//!     count) with a tiny `max_result_rows` (10) but a GENEROUS `max_result_bytes`
//!     succeeds and returns EXACTLY the fixture's row count — proving a huge LIMIT
//!     at scale is not silently truncated by the (exempted) row-count valve, and
//!     the returned count is bounded by the actual matching rows (no over-return).
//!   * `tight_byte_budget_still_trips_at_scale` — the SAME huge `LIMIT` over the
//!     SAME fixture, but with the byte budget dropped to a few bytes, still trips
//!     `Error::ResultTooLarge` — proving the byte guard (the PRIMARY guard, issue
//!     #1582) stays live at non-trivial row counts, not merely at 4 rows.
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
use cqlite_core::{Config, Database, Error};
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
    open_with_caps(n_rows, max_result_rows, None).await
}

/// Open with both `max_result_rows` and (optionally) `max_result_bytes` lowered,
/// so a tiny fixture can exercise either guard independently.
async fn open_with_caps(
    n_rows: i32,
    max_result_rows: u64,
    max_result_bytes: Option<u64>,
) -> (Database, TempDir) {
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
    if let Some(bytes) = max_result_bytes {
        core_config.query.max_result_bytes = bytes;
    }

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

/// The LIMIT exemption is SCOPED to the row-count valve only: the byte budget
/// (the primary guard, issue #1582) still fires `Error::ResultTooLarge` even on
/// a LIMIT-bounded query. This is the exact contract the row-count demotion
/// leans on — LIMIT does not blanket-exempt a query from EVERY materialization
/// guard, only the crude row-count one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_limit_does_not_exempt_byte_budget() {
    // Row-count valve is generous (100) so only the byte budget can trip;
    // max_result_bytes is set far below what 4 rows require.
    let (db, _tmp) = open_with_caps(4, 100, Some(4)).await;

    let err = db
        .execute(&format!("SELECT * FROM {KS}.{TBL} LIMIT 4"))
        .await
        .err()
        .expect(
            "Issue #1578: an explicit LIMIT exempts the ROW-COUNT valve only — \
             the byte budget must still trip ResultTooLarge",
        );
    match err {
        Error::ResultTooLarge { budget_bytes, .. } => {
            assert_eq!(budget_bytes, 4, "byte budget must be the one we configured");
        }
        other => panic!("expected Error::ResultTooLarge, got: {other:?}"),
    }
}

/// Row count for the at-scale companion tests: a "few thousand" rows, per the
/// roborev ask — large enough that a size-proportional row-count valve or a
/// silently truncated LIMIT would be unmistakable, small enough to build fast in
/// a test.
const SCALE_ROWS: i32 = 3000;

/// Roborev follow-up: a HUGE `LIMIT` (far above `SCALE_ROWS`) with a stingy
/// `max_result_rows` but a GENEROUS `max_result_bytes` succeeds and returns
/// EXACTLY the fixture's row count at non-trivial scale — the row-count valve's
/// exemption is not an artifact of the earlier 4-row fixture, and the returned
/// count is bounded by the actual matching rows (no truncation, no over-return).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_limit_returns_all_rows_at_scale() {
    // max_result_rows=10 would trip immediately without the LIMIT exemption
    // (SCALE_ROWS far exceeds it); max_result_bytes is generous (64 MiB) so only
    // the exemption itself is under test, not an incidentally-tight byte budget.
    let (db, _tmp) = open_with_caps(SCALE_ROWS, 10, Some(64 * 1024 * 1024)).await;

    let result = db
        .execute(&format!("SELECT * FROM {KS}.{TBL} LIMIT 1500000"))
        .await
        .expect(
            "Issue #1578: a huge LIMIT over a few-thousand-row table with a \
             generous byte budget must succeed despite a stingy max_result_rows \
             — the row-count valve is exempted for an explicit LIMIT at scale, \
             not just for a 4-row fixture",
        );
    assert_eq!(
        result.rows.len(),
        SCALE_ROWS as usize,
        "LIMIT 1_500_000 over a {SCALE_ROWS}-row table must return EXACTLY \
         {SCALE_ROWS} rows — bounded by the actual matching rows, neither \
         truncated nor duplicated"
    );
}

/// Roborev follow-up: the SAME huge-LIMIT query over the SAME
/// non-trivial-scale fixture, but with the byte budget dropped to a few bytes,
/// STILL trips `Error::ResultTooLarge` — the byte guard (issue #1582's primary
/// guard) remains a live safety net at scale, not merely at 4 rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tight_byte_budget_still_trips_at_scale() {
    // max_result_rows is generous (far above SCALE_ROWS) so only the byte
    // budget can trip; max_result_bytes is a handful of bytes, far below what
    // SCALE_ROWS worth of (id, v) int pairs need.
    let (db, _tmp) = open_with_caps(SCALE_ROWS, 1_000_000, Some(16)).await;

    let err = db
        .execute(&format!("SELECT * FROM {KS}.{TBL} LIMIT 1500000"))
        .await
        .err()
        .expect(
            "Issue #1578: a huge LIMIT over a few-thousand-row table with a \
             tiny max_result_bytes must still trip the byte guard — LIMIT \
             exempts only the row-count valve, never the byte ceiling, at any \
             scale",
        );
    match err {
        Error::ResultTooLarge {
            budget_bytes, rows, ..
        } => {
            assert_eq!(
                budget_bytes, 16,
                "byte budget must be the one we configured"
            );
            assert!(
                rows > 0,
                "the byte guard must report a non-trivial row count at trip time \
                 (got {rows}), confirming it evaluated the actual scaled result"
            );
        }
        other => panic!("expected Error::ResultTooLarge, got: {other:?}"),
    }
}
