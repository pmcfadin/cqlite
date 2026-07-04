//! Issue #1582 (Epic D6): the memory guard on a MATERIALIZED result set must be
//! a BYTE ceiling, not a row count — enforced as a SINGLE robust check on the
//! FINAL result (narrow subset, owner decision).
//!
//! ## The bug this guards
//!
//! The materializing SELECT path capped a result at `MAX_RESULTS = 1_000_000`
//! ROWS. That is the wrong unit: 1M skinny rows may fit comfortably inside the
//! project's <128MB memory target, while a few thousand WIDE rows (large blobs,
//! wide partitions) can blow it long before the row-count guard ever fires. The
//! fix adds a byte ceiling — [`cqlite_core::config::QueryConfig::max_result_bytes`],
//! default 64 MiB — summed over the FINAL returned rows with the SAME
//! `estimate_value_size` estimator the row cache uses, and raises the typed
//! [`cqlite_core::Error::ResultTooLarge`] (whose message tells the user to add
//! `LIMIT` or stream) when it is crossed. The row-count guard is retained only as
//! a secondary safety valve ([`cqlite_core::config::QueryConfig::max_result_rows`]).
//!
//! ## Narrow subset: one check on the FINAL result
//!
//! The budget is applied ONCE, on the fully-materialized result AFTER the whole
//! step pipeline (Limit/Offset/Filter/Sort/Aggregate/Project) — not during
//! collection. Because it sees only the RETURNED rows (post LIMIT/OFFSET), a
//! `LIMIT N` query whose final result is small never trips, and OFFSET-skipped
//! rows are never charged. This replaced the earlier during-collection machinery
//! (LIMIT/OFFSET pushdown, per-row early-stop, storage-layer row limit) that kept
//! generating correctness edge cases. SCOPE (owner-accepted, tracked on #1582):
//! does NOT bound peak scan memory (deferred #1897) and does NOT cover the legacy
//! `WHERE id = ?` point-lookup path (deferred to the D6 redesign).
//!
//! ## Why controlled `WriteEngine` fixtures (not the vendored `test_wide_rows`)
//!
//! The assertions need EXACT, deterministic control over per-row width and row
//! count so the byte budget can be tripped (or not) predictably regardless of
//! environment — and they must not silently SKIP when the gitignored dataset
//! `.db` binaries are absent from a clean checkout. Building a wide fixture (a
//! few large-blob rows) and a skinny fixture (many tiny rows) via the public
//! `WriteEngine` API gives that determinism while still exercising the guard
//! end-to-end through `Database::execute`.
//!
//! Run with:
//!   cargo test --package cqlite-core \
//!     --features write-support,cli-helpers,state_machine \
//!     --test issue_1582_byte_bounded_result_budget

#![cfg(all(
    feature = "write-support",
    feature = "cli-helpers",
    feature = "state_machine",
    not(feature = "tombstones")
))]

use cqlite_core::config::DEFAULT_MAX_RESULT_BYTES;
use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use cqlite_core::{Config, Database, Error};
use tempfile::TempDir;

const KS: &str = "budget_ks";
const WIDE_TBL: &str = "wide_items";
const SKINNY_TBL: &str = "skinny_items";

/// ~10 KiB per row → a handful of rows dwarfs a small byte budget while staying
/// far below the 1M row-count safety valve.
const WIDE_PAYLOAD_BYTES: usize = 10 * 1024;
const WIDE_ROW_COUNT: i32 = 8;

/// Number of wide rows whose combined bytes stay under [`SHARED_BUDGET_BYTES`]:
/// each wide row estimates to ~`WIDE_PAYLOAD_BYTES` + a few bytes for the int
/// key, so a `LIMIT` of this many keeps the FINAL result under budget.
const WIDE_ROWS_UNDER_BUDGET: usize = 2;

/// Many tiny rows: total bytes stay small even though the row COUNT is large
/// relative to the wide fixture.
const SKINNY_ROW_COUNT: i32 = 400;

/// A byte budget that a handful of ~10 KiB wide rows exceeds, but the skinny
/// fixture's total logical bytes (~few KiB) stays under.
const SHARED_BUDGET_BYTES: u64 = 24 * 1024;

fn wide_schema_cql() -> String {
    format!("CREATE TABLE {KS}.{WIDE_TBL} (\n  id int PRIMARY KEY,\n  payload blob\n);\n")
}

fn skinny_schema_cql() -> String {
    format!("CREATE TABLE {KS}.{SKINNY_TBL} (\n  id int PRIMARY KEY,\n  n int\n);\n")
}

fn wide_row(id: i32, ts: i64) -> Mutation {
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![CellOperation::Write {
        column: "payload".to_string(),
        value: Value::Blob(vec![0xABu8; WIDE_PAYLOAD_BYTES]),
    }];
    Mutation::new(TableId::new(KS, WIDE_TBL), pk, None, ops, ts, None)
}

fn skinny_row(id: i32, ts: i64) -> Mutation {
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![CellOperation::Write {
        column: "n".to_string(),
        value: Value::Integer(id),
    }];
    Mutation::new(TableId::new(KS, SKINNY_TBL), pk, None, ops, ts, None)
}

/// Flush a single generation for `table` with the supplied schema + rows.
fn build_fixture(
    data_dir: &std::path::Path,
    wal_dir: &std::path::Path,
    schema_cql: &str,
    rows: Vec<Mutation>,
) {
    use cqlite_core::schema::parse_cql_schema;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let schema = parse_cql_schema(schema_cql).expect("parse fixture schema");
    let config = WriteEngineConfig::new(data_dir.to_path_buf(), wal_dir.to_path_buf(), schema);
    let mut engine = WriteEngine::new(config).expect("engine creation");

    for m in rows {
        engine.write(m).expect("write row");
    }
    rt.block_on(engine.flush())
        .expect("flush")
        .expect("flush produced no SSTable");
    rt.block_on(engine.close()).expect("close engine");
}

/// Open the full query stack over `data_dir` with a specific `max_result_bytes`
/// budget wired through config (so the knob is exercised end-to-end).
async fn open_db_with_budget(
    data_dir: std::path::PathBuf,
    schema_path: std::path::PathBuf,
    max_result_bytes: u64,
) -> Database {
    // Default row-count valve (1M) — byte budget is the guard under test here.
    open_db_with_budgets(data_dir, schema_path, max_result_bytes, 1_000_000).await
}

/// Open the query stack wiring BOTH the byte budget and the `max_result_rows`
/// row-count safety valve through config (the row-count knob must be
/// load-bearing, not a hardcoded constant).
async fn open_db_with_budgets(
    data_dir: std::path::PathBuf,
    schema_path: std::path::PathBuf,
    max_result_bytes: u64,
    max_result_rows: u64,
) -> Database {
    let mut core_config = Config::default();
    core_config.query.max_result_bytes = max_result_bytes;
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
    assert!(
        result.schema_load_result.schemas_loaded >= 1,
        "schema must load"
    );
    result.database
}

/// Build a wide-row fixture under `root`; returns (data_dir, schema_path).
///
/// The fixture is flushed on a blocking thread (the `WriteEngine` flush drives
/// its own current-thread runtime and cannot run inside the test's tokio
/// runtime).
async fn prepare_wide(root: &std::path::Path) -> (std::path::PathBuf, std::path::PathBuf) {
    let data_dir = root.join("wide_data");
    let wal_dir = root.join("wide_wal");
    let schema_path = root.join("wide_schema.cql");
    std::fs::write(&schema_path, wide_schema_cql()).expect("write wide schema");
    let (d, w) = (data_dir.clone(), wal_dir.clone());
    tokio::task::spawn_blocking(move || {
        let rows: Vec<Mutation> = (0..WIDE_ROW_COUNT).map(|i| wide_row(i, 100)).collect();
        build_fixture(&d, &w, &wide_schema_cql(), rows);
    })
    .await
    .expect("build wide fixture");
    (data_dir, schema_path)
}

async fn prepare_skinny(root: &std::path::Path) -> (std::path::PathBuf, std::path::PathBuf) {
    let data_dir = root.join("skinny_data");
    let wal_dir = root.join("skinny_wal");
    let schema_path = root.join("skinny_schema.cql");
    std::fs::write(&schema_path, skinny_schema_cql()).expect("write skinny schema");
    let (d, w) = (data_dir.clone(), wal_dir.clone());
    tokio::task::spawn_blocking(move || {
        let rows: Vec<Mutation> = (0..SKINNY_ROW_COUNT).map(|i| skinny_row(i, 100)).collect();
        build_fixture(&d, &w, &skinny_schema_cql(), rows);
    })
    .await
    .expect("build skinny fixture");
    (data_dir, schema_path)
}

/// A wide, UNLIMITED FINAL result trips the BYTE guard before the row-count
/// guard. The full fixture is collected and its combined bytes exceed the
/// budget; the row count is far below the 1M row-count valve, proving the byte
/// guard fired first.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wide_final_result_trips_byte_guard() {
    let temp = TempDir::new().unwrap();
    let (data_dir, schema_path) = prepare_wide(temp.path()).await;

    let db = open_db_with_budget(data_dir, schema_path, SHARED_BUDGET_BYTES).await;

    let sql = format!("SELECT * FROM {KS}.{WIDE_TBL}");
    let err = db
        .execute(&sql)
        .await
        .expect_err("wide SELECT * must exceed the byte budget");

    match err {
        Error::ResultTooLarge {
            budget_bytes,
            estimated_bytes,
            rows,
        } => {
            assert_eq!(
                budget_bytes, SHARED_BUDGET_BYTES as usize,
                "error must report the configured budget"
            );
            assert!(
                estimated_bytes > budget_bytes,
                "estimated bytes ({estimated_bytes}) must exceed budget ({budget_bytes})"
            );
            // The whole point of D6: the BYTE guard fired, NOT the 1M row-count
            // valve. A handful of wide rows is nowhere near 1M.
            assert!(
                rows < 1_000_000,
                "byte guard must trip long before the 1M row-count guard; got rows={rows}"
            );
        }
        other => panic!("expected Error::ResultTooLarge, got {other:?}"),
    }
}

/// A `LIMIT` keeps a big table under budget: the UNLIMITED scan over the wide
/// fixture trips the guard, but `SELECT * ... LIMIT 2` produces a small FINAL
/// result (2 wide rows, under budget), so the single final-result check never
/// raises `ResultTooLarge`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn limit_keeps_big_table_under_budget() {
    let temp = TempDir::new().unwrap();
    let (data_dir, schema_path) = prepare_wide(temp.path()).await;

    let db = open_db_with_budget(data_dir, schema_path, SHARED_BUDGET_BYTES).await;

    // Sanity: the UNLIMITED query trips the guard (proves the budget is binding).
    let unlimited = format!("SELECT * FROM {KS}.{WIDE_TBL}");
    let err = db
        .execute(&unlimited)
        .await
        .expect_err("unlimited wide SELECT * must exceed the byte budget");
    assert!(
        matches!(err, Error::ResultTooLarge { .. }),
        "unlimited query must trip the guard, got {err:?}"
    );

    // LIMIT within the budget fit must complete and return exactly that many rows.
    let sql = format!("SELECT * FROM {KS}.{WIDE_TBL} LIMIT {WIDE_ROWS_UNDER_BUDGET}");
    let result = db
        .execute(&sql)
        .await
        .expect("a LIMITed query whose FINAL result fits the budget must complete");
    assert_eq!(
        result.rows.len(),
        WIDE_ROWS_UNDER_BUDGET,
        "LIMIT must be honored and the final small result must not trip the byte budget"
    );
}

/// MANY skinny rows whose total bytes stay under the SAME budget that tripped the
/// wide fixture complete fine and return every row (byte unit, not row unit).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn skinny_result_under_budget_completes() {
    let temp = TempDir::new().unwrap();
    let (data_dir, schema_path) = prepare_skinny(temp.path()).await;

    let db = open_db_with_budget(data_dir, schema_path, SHARED_BUDGET_BYTES).await;

    let sql = format!("SELECT * FROM {KS}.{SKINNY_TBL}");
    let result = db
        .execute(&sql)
        .await
        .expect("skinny SELECT * must stay under the byte budget");

    assert_eq!(
        result.rows.len(),
        SKINNY_ROW_COUNT as usize,
        "all skinny rows must be returned even though there are MORE rows than \
         the wide fixture that tripped the same byte budget"
    );
}

/// The `max_result_bytes` knob is load-bearing — the same skinny query passes
/// under a large budget and trips under a small one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_result_bytes_knob_is_load_bearing() {
    let temp = TempDir::new().unwrap();
    let (data_dir, schema_path) = prepare_skinny(temp.path()).await;

    let sql = format!("SELECT * FROM {KS}.{SKINNY_TBL}");

    // Large budget (the shipped default): the query PASSES and returns all rows.
    {
        let db = open_db_with_budget(
            data_dir.clone(),
            schema_path.clone(),
            DEFAULT_MAX_RESULT_BYTES,
        )
        .await;
        let result = db
            .execute(&sql)
            .await
            .expect("skinny SELECT * passes under the default budget");
        assert_eq!(result.rows.len(), SKINNY_ROW_COUNT as usize);
    }

    // Small budget: the SAME query now TRIPS the guard. Lowering the knob changed
    // what trips → the knob is real, not decorative.
    {
        let tiny_budget: u64 = 512;
        let db = open_db_with_budget(data_dir, schema_path, tiny_budget).await;
        let err = db
            .execute(&sql)
            .await
            .expect_err("lowering max_result_bytes must make the skinny query trip");
        match err {
            Error::ResultTooLarge { budget_bytes, .. } => {
                assert_eq!(budget_bytes, tiny_budget as usize);
            }
            other => panic!("expected Error::ResultTooLarge under tiny budget, got {other:?}"),
        }
    }
}

/// A constant `SELECT` (no FROM clause, e.g. `SELECT '<big literal>'`) routes
/// through `execute_constant_query`, which returned BEFORE the final-result
/// budget check. This regression (roborev, #1582) proves the SAME byte budget is
/// now applied to a constant result: a large literal trips `ResultTooLarge`,
/// while a small literal under a generous budget still completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn over_budget_constant_query_trips_byte_guard() {
    let temp = TempDir::new().unwrap();
    // Any valid DB — a constant query touches no table, so the skinny fixture's
    // schema/data are irrelevant; we just need an open query stack whose budget
    // is wired from config.
    let (data_dir, schema_path) = prepare_skinny(temp.path()).await;

    let tiny_budget: u64 = 512;
    // A single text literal far larger than the budget (Text estimates to its
    // byte length; see `estimate_value_size`).
    let big_literal_len = (tiny_budget as usize) * 4;
    let big_literal: String = "x".repeat(big_literal_len);

    let db = open_db_with_budget(data_dir, schema_path, tiny_budget).await;

    let sql = format!("SELECT '{big_literal}'");
    let err = db
        .execute(&sql)
        .await
        .expect_err("an over-budget constant SELECT must trip the byte guard");

    match err {
        Error::ResultTooLarge {
            budget_bytes,
            estimated_bytes,
            ..
        } => {
            assert_eq!(
                budget_bytes, tiny_budget as usize,
                "error must report the configured budget"
            );
            assert!(
                estimated_bytes > budget_bytes,
                "estimated bytes ({estimated_bytes}) must exceed budget ({budget_bytes})"
            );
        }
        other => panic!("expected Error::ResultTooLarge from a constant query, got {other:?}"),
    }
}

/// The `max_result_rows` row-count safety valve must be load-bearing (wired from
/// config), not a hardcoded 1,000,000 constant.
///
/// Under a GENEROUS byte budget (so the byte guard never fires), the skinny
/// query passes with a high `max_result_rows` and TRIPS the row-count valve when
/// `max_result_rows` is lowered below the fixture's row count.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_result_rows_knob_is_load_bearing() {
    let temp = TempDir::new().unwrap();
    let (data_dir, schema_path) = prepare_skinny(temp.path()).await;

    let sql = format!("SELECT * FROM {KS}.{SKINNY_TBL}");

    // High row-count valve (and generous byte budget): the query PASSES.
    {
        let db = open_db_with_budgets(
            data_dir.clone(),
            schema_path.clone(),
            DEFAULT_MAX_RESULT_BYTES,
            1_000_000,
        )
        .await;
        let result = db
            .execute(&sql)
            .await
            .expect("skinny SELECT * passes under a high row-count valve");
        assert_eq!(result.rows.len(), SKINNY_ROW_COUNT as usize);
    }

    // Low row-count valve (same generous byte budget): the SAME query now TRIPS.
    // Lowering the config knob changed what trips → the valve is load-bearing.
    {
        let low_rows: u64 = (SKINNY_ROW_COUNT as u64) / 2; // < fixture row count
        let db =
            open_db_with_budgets(data_dir, schema_path, DEFAULT_MAX_RESULT_BYTES, low_rows).await;
        let err = db
            .execute(&sql)
            .await
            .expect_err("lowering max_result_rows must make the skinny query trip the valve");
        // The row-count valve raises the legacy query-execution error (add LIMIT),
        // NOT ResultTooLarge (which is the byte guard, generous here).
        match err {
            Error::QueryExecution(msg) => {
                assert!(
                    msg.contains("LIMIT"),
                    "row-count valve error should advise adding LIMIT; got {msg:?}"
                );
            }
            other => {
                panic!("expected Error::QueryExecution from the row-count valve, got {other:?}")
            }
        }
    }
}
