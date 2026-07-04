//! Issue #1582 (Epic D6): the memory guard on a MATERIALIZED result set must be
//! a BYTE ceiling, not a row count.
//!
//! ## The bug this guards
//!
//! The materializing SELECT path capped a result at `MAX_RESULTS = 1_000_000`
//! ROWS. That is the wrong unit: 1M skinny rows may fit comfortably inside the
//! project's <128MB memory target, while a few thousand WIDE rows (large blobs,
//! wide partitions) can blow it long before the row-count guard ever fires. The
//! fix adds a byte ceiling — [`cqlite_core::config::QueryConfig::max_result_bytes`],
//! default 64 MiB — accumulated with the SAME `estimate_row_size` estimator the
//! row cache uses, and raises the typed [`cqlite_core::Error::ResultTooLarge`]
//! (whose message tells the user to add `LIMIT` or stream) when it is crossed.
//! The row-count guard is retained only as a secondary safety valve.
//!
//! ## Why controlled `WriteEngine` fixtures (not the vendored `test_wide_rows`)
//!
//! The three assertions need EXACT, deterministic control over per-row width and
//! row count so the byte budget can be tripped (or not) predictably regardless
//! of environment — and they must not silently SKIP when the gitignored dataset
//! `.db` binaries are absent from a clean checkout. Building a wide fixture (a
//! few large-blob rows) and a skinny fixture (many tiny rows) via the public
//! `WriteEngine` API gives that determinism while still exercising the guard
//! end-to-end through `Database::execute`.
//!
//! ## What the three tests assert (all FAIL on pre-fix `main`, which has no
//! byte guard and no `max_result_bytes` knob)
//!
//!   1. `wide_rows_trip_byte_guard_before_row_count_guard` — a handful of wide
//!      rows trips the BYTE guard; the collected row count is far below the 1M
//!      row-count valve, proving the byte guard fired first.
//!   2. `skinny_rows_under_byte_budget_complete` — MANY skinny rows whose TOTAL
//!      bytes stay under the SAME budget that tripped the wide fixture complete
//!      fine and return every row (byte unit, not row unit).
//!   3. `max_result_bytes_knob_is_load_bearing` — the same skinny query passes
//!      under a large budget and trips under a small budget: lowering the knob
//!      changes what trips, so it is a real behavioral knob, not decoration.
//!   4. `limited_query_over_wide_table_completes_under_budget` (FINDING 2) — a
//!      LIMITed query over the wide fixture (whose UNLIMITED scan trips the guard)
//!      completes and returns the limited rows: collection early-stops after
//!      predicate evaluation, so the budget never bites on matching rows beyond
//!      the LIMIT.
//!   5. `limit_with_non_pk_predicate_returns_all_matches_not_first_n_raw`
//!      (roborev FINDING 1) — a `WHERE non_pk = ? LIMIT N` returns ALL N matching
//!      rows, not the matches among the first N RAW rows: the LIMIT is NOT pushed
//!      into the predicate-unaware `storage.scan`.
//!   6. `limit_zero_targeted_returns_empty_never_result_too_large`
//!      (roborev FINDING 2) — `WHERE pk = ? LIMIT 0` over a wide table under a
//!      sub-row budget returns empty, never `ResultTooLarge`: LIMIT 0
//!      short-circuits before any scan/lookup/push/budget work.
//!   7. `limit_offset_skips_wide_rows_uncharged_against_budget`
//!      (roborev FINDING B) — `LIMIT 2 OFFSET N` over the wide fixture, where the
//!      skipped OFFSET rows would blow the budget but the 2 returned rows fit,
//!      SUCCEEDS: the offset rows are skipped uncharged, so only the returned
//!      rows are budgeted.
//!   8. `max_result_rows_knob_is_load_bearing` (roborev FINDING A) — lowering
//!      `max_result_rows` under a generous byte budget makes the skinny query
//!      trip the row-count safety valve; a high value passes. The knob is real,
//!      not a hardcoded constant.
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
/// Non-PK-filter fixture for FINDING 1 (LIMIT must not be pushed into storage
/// ahead of executor-side predicate evaluation).
const FILTERED_TBL: &str = "filtered_items";

/// ~10 KiB per row → a handful of rows dwarfs a small byte budget while staying
/// far below the 1M row-count safety valve.
const WIDE_PAYLOAD_BYTES: usize = 10 * 1024;
const WIDE_ROW_COUNT: i32 = 8;

/// Rows the byte guard admits before it trips at [`SHARED_BUDGET_BYTES`]: each
/// wide row estimates to ~`WIDE_PAYLOAD_BYTES` + a few bytes for the int key, so
/// `floor(budget / row_bytes)` rows fit before the running estimate crosses the
/// budget. Used as the LIMIT that must still complete under budget (FINDING 2).
const WIDE_ROWS_UNDER_BUDGET: usize = 2;

/// Many tiny rows: total bytes stay small even though the row COUNT is large
/// relative to the wide fixture.
const SKINNY_ROW_COUNT: i32 = 400;

/// FINDING 1 fixture: total rows, and the group value a small, spread-out subset
/// carries. Only every 20th row matches `MATCH_GRP` (10 rows), so the matching
/// rows are scattered throughout token order — NOT confined to the first
/// `MATCH_COUNT` rows a buggy storage-pushed LIMIT would return.
const FILTERED_ROW_COUNT: i32 = 200;
const MATCH_GRP: i32 = 7;
const NON_MATCH_GRP: i32 = 0;
const MATCH_COUNT: usize = 10;

/// Wide fixture with a UUID `id` PK (roborev legacy-path finding). A UUID `WHERE
/// id = <uuid>` point lookup satisfies `is_simple_id_lookup` (≤ 8 tokens,
/// `WHERE id =`) and is routed through the LEGACY `QueryExecutor`: because the
/// value is a `Uuid` (not an `Integer`), `condition_to_row_key` falls through to
/// `value_to_row_key`, which produces the exact 16-byte partition key the real
/// SSTable stores — so the legacy `storage.get` actually returns the wide row
/// (unlike an int `id`, which the legacy path maps to a synthetic `user_key_N`
/// that never matches a real SSTable partition key).
const WIDE_UUID_TBL: &str = "wide_uuid_items";
/// The partition looked up by the legacy-path tests (`0x11` repeated 16×). Its
/// canonical UUID literal (below) is what the CQL `WHERE id = ...` clause carries.
const LOOKUP_UUID: [u8; 16] = [0x11u8; 16];
const LOOKUP_UUID_LITERAL: &str = "11111111-1111-1111-1111-111111111111";

fn wide_schema_cql() -> String {
    format!("CREATE TABLE {KS}.{WIDE_TBL} (\n  id int PRIMARY KEY,\n  payload blob\n);\n")
}

fn wide_uuid_schema_cql() -> String {
    format!("CREATE TABLE {KS}.{WIDE_UUID_TBL} (\n  id uuid PRIMARY KEY,\n  payload blob\n);\n")
}

fn wide_uuid_row(id_bytes: [u8; 16], ts: i64) -> Mutation {
    let pk = PartitionKey::single("id", Value::Uuid(id_bytes));
    let ops = vec![CellOperation::Write {
        column: "payload".to_string(),
        value: Value::Blob(vec![0xABu8; WIDE_PAYLOAD_BYTES]),
    }];
    Mutation::new(TableId::new(KS, WIDE_UUID_TBL), pk, None, ops, ts, None)
}

fn skinny_schema_cql() -> String {
    format!("CREATE TABLE {KS}.{SKINNY_TBL} (\n  id int PRIMARY KEY,\n  n int\n);\n")
}

fn filtered_schema_cql() -> String {
    format!("CREATE TABLE {KS}.{FILTERED_TBL} (\n  id int PRIMARY KEY,\n  grp int\n);\n")
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

fn filtered_row(id: i32, ts: i64) -> Mutation {
    let pk = PartitionKey::single("id", Value::Integer(id));
    // Every 20th row matches MATCH_GRP; the rest carry NON_MATCH_GRP.
    let grp = if id % 20 == 0 {
        MATCH_GRP
    } else {
        NON_MATCH_GRP
    };
    let ops = vec![CellOperation::Write {
        column: "grp".to_string(),
        value: Value::Integer(grp),
    }];
    Mutation::new(TableId::new(KS, FILTERED_TBL), pk, None, ops, ts, None)
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
/// row-count safety valve through config (roborev FINDING A: the row-count knob
/// must be load-bearing, not a hardcoded constant).
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

/// Build a wide UUID-PK fixture: a handful of distinct-UUID single-row
/// partitions, one keyed by [`LOOKUP_UUID`]. Each row's ~10 KiB payload dwarfs
/// [`SUB_ROW_BUDGET_BYTES`].
async fn prepare_wide_uuid(root: &std::path::Path) -> (std::path::PathBuf, std::path::PathBuf) {
    let data_dir = root.join("wide_uuid_data");
    let wal_dir = root.join("wide_uuid_wal");
    let schema_path = root.join("wide_uuid_schema.cql");
    std::fs::write(&schema_path, wide_uuid_schema_cql()).expect("write wide uuid schema");
    let (d, w) = (data_dir.clone(), wal_dir.clone());
    tokio::task::spawn_blocking(move || {
        let rows: Vec<Mutation> = (0..WIDE_ROW_COUNT)
            .map(|i| {
                let uuid = if i == 0 {
                    LOOKUP_UUID
                } else {
                    [(0x20u8 + i as u8); 16]
                };
                wide_uuid_row(uuid, 100)
            })
            .collect();
        build_fixture(&d, &w, &wide_uuid_schema_cql(), rows);
    })
    .await
    .expect("build wide uuid fixture");
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

async fn prepare_filtered(root: &std::path::Path) -> (std::path::PathBuf, std::path::PathBuf) {
    let data_dir = root.join("filtered_data");
    let wal_dir = root.join("filtered_wal");
    let schema_path = root.join("filtered_schema.cql");
    std::fs::write(&schema_path, filtered_schema_cql()).expect("write filtered schema");
    let (d, w) = (data_dir.clone(), wal_dir.clone());
    tokio::task::spawn_blocking(move || {
        let rows: Vec<Mutation> = (0..FILTERED_ROW_COUNT)
            .map(|i| filtered_row(i, 100))
            .collect();
        build_fixture(&d, &w, &filtered_schema_cql(), rows);
    })
    .await
    .expect("build filtered fixture");
    (data_dir, schema_path)
}

/// A byte budget that a handful of ~10 KiB wide rows exceeds, but the skinny
/// fixture's total logical bytes (~few KiB) stays under.
const SHARED_BUDGET_BYTES: u64 = 24 * 1024;

/// A byte budget SMALLER than a single wide row (~10 KiB): a single-partition
/// lookup that materializes one wide row would trip the guard under it — used to
/// prove the FINDING 2 `LIMIT 0` short-circuit fires BEFORE any push/byte-check.
const SUB_ROW_BUDGET_BYTES: u64 = 512;

/// Test 1: a few WIDE rows trip the BYTE guard before the row-count guard.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wide_rows_trip_byte_guard_before_row_count_guard() {
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
            // The guard fails fast: it trips after collecting only the few rows
            // that fill the budget (~`WIDE_ROWS_UNDER_BUDGET` + 1), a small handful
            // strictly below the full fixture — not after collecting all of it.
            assert!(
                rows <= WIDE_ROWS_UNDER_BUDGET + 1,
                "byte guard should fail fast a row past the budget fit \
                 ({WIDE_ROWS_UNDER_BUDGET}); got rows={rows}"
            );
        }
        other => panic!("expected Error::ResultTooLarge, got {other:?}"),
    }
}

/// FINDING 2: a LIMITed query over the same wide fixture — whose UNLIMITED scan
/// trips the byte guard (see `wide_rows_trip_byte_guard_before_row_count_guard`)
/// — must complete and return the limited rows, WITHOUT the budget biting on
/// matching rows beyond the LIMIT.
///
/// This is the red→green for FINDING 2: before the fix the LIMIT was applied by a
/// SEPARATE post-scan step, so the executor enforced the byte budget while
/// collecting EVERY matching row and raised `ResultTooLarge` at ~row 3 — before
/// the LIMIT step ever ran. Propagating the LIMIT bound into the scan's `limit`
/// AND early-stopping collection at `offset + count` means only the limited rows
/// are collected, so the budget never bites on rows beyond the LIMIT.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn limited_query_over_wide_table_completes_under_budget() {
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
        .expect("a LIMITed query that fits the budget must complete, not trip ResultTooLarge");
    assert_eq!(
        result.rows.len(),
        WIDE_ROWS_UNDER_BUDGET,
        "LIMIT must be honored (early-stop before the byte budget bites)"
    );
}

/// FINDING 1 (roborev HIGH): a LIMIT must NOT be pushed into storage ahead of
/// executor-side predicate evaluation. The optimizer folds `WHERE grp = ?` into
/// the `SSTableScan` step's predicates WITHOUT a residual `Filter`, but
/// `storage.scan` is not predicate-aware. If the query-wide `LIMIT N` bound is
/// pushed into `storage.scan`, storage returns only the first `N` RAW (unfiltered)
/// rows in token order; the matching rows scattered further along the scan are
/// never seen, so the executor filters `N` raw rows down to far fewer than `N`
/// matches → WRONG RESULTS.
///
/// This fixture has `MATCH_COUNT` matching rows spread across `FILTERED_ROW_COUNT`
/// total rows. `SELECT * WHERE grp = MATCH_GRP LIMIT MATCH_COUNT` must return all
/// `MATCH_COUNT` matches. Pre-fix (storage-pushed LIMIT) this returns only the
/// matches that happen to fall within the first `MATCH_COUNT` token-ordered rows
/// (≈ `MATCH_COUNT² / FILTERED_ROW_COUNT` ≈ 0–1) → RED. Post-fix the storage limit
/// is withheld whenever executor-side predicates exist and the early-stop counts
/// only rows that PASSED the predicate → all `MATCH_COUNT` matches → GREEN.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn limit_with_non_pk_predicate_returns_all_matches_not_first_n_raw() {
    let temp = TempDir::new().unwrap();
    let (data_dir, schema_path) = prepare_filtered(temp.path()).await;

    // Generous budget: FINDING 1 is about correctness, not the byte guard.
    let db = open_db_with_budget(data_dir, schema_path, DEFAULT_MAX_RESULT_BYTES).await;

    let sql =
        format!("SELECT * FROM {KS}.{FILTERED_TBL} WHERE grp = {MATCH_GRP} LIMIT {MATCH_COUNT}");
    let result = db
        .execute(&sql)
        .await
        .expect("filtered LIMIT query must succeed");

    assert_eq!(
        result.rows.len(),
        MATCH_COUNT,
        "LIMIT must return ALL matching rows, not the matches among the first N \
         RAW rows a storage-pushed limit would return (FINDING 1)"
    );
}

/// FINDING 2 (roborev MEDIUM): `WHERE pk = ? LIMIT 0` over a wide table must
/// return an empty result, never `ResultTooLarge`. The budget here is SMALLER than
/// a single wide row, so pre-fix the targeted lookup materialized the matching row
/// and byte-checked it before the LIMIT-0 bound was honored → `ResultTooLarge`.
/// Post-fix the `collect_bound == Some(0)` short-circuit returns empty before any
/// scan/lookup/push/budget work.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn limit_zero_targeted_returns_empty_never_result_too_large() {
    let temp = TempDir::new().unwrap();
    let (data_dir, schema_path) = prepare_wide(temp.path()).await;

    let db = open_db_with_budget(data_dir, schema_path, SUB_ROW_BUDGET_BYTES).await;

    // Sanity: the same single-partition lookup with a NON-zero LIMIT trips the
    // guard (one wide row exceeds the sub-row budget) — proving the budget is
    // binding and that only the LIMIT-0 short-circuit makes this query empty rather
    // than error. (A bare `WHERE id = 0` with no LIMIT is routed to the legacy
    // executor by a word-count heuristic in `QueryEngine::execute`; keeping a LIMIT
    // clause here routes through the optimizer path under test.)
    let point = format!("SELECT * FROM {KS}.{WIDE_TBL} WHERE id = 0 LIMIT 5");
    let err = db
        .execute(&point)
        .await
        .expect_err("a single wide row must exceed the sub-row byte budget");
    assert!(
        matches!(err, Error::ResultTooLarge { .. }),
        "point lookup of one wide row must trip the guard, got {err:?}"
    );

    let sql = format!("SELECT * FROM {KS}.{WIDE_TBL} WHERE id = 0 LIMIT 0");
    let result = db
        .execute(&sql)
        .await
        .expect("LIMIT 0 must short-circuit to an empty result, never ResultTooLarge");
    assert!(
        result.rows.is_empty(),
        "LIMIT 0 must return zero rows; got {}",
        result.rows.len()
    );
}

/// Test 2: MANY skinny rows whose total bytes stay under the SAME budget that
/// tripped the wide fixture complete fine and return every row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn skinny_rows_under_byte_budget_complete() {
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

/// Test 3: the `max_result_bytes` knob is load-bearing — the same skinny query
/// passes under a large budget and trips under a small one.
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

/// FINDING B (roborev MEDIUM): `LIMIT ... OFFSET ...` must NOT charge the skipped
/// OFFSET rows against the byte budget. A query whose FINAL result is small must
/// SUCCEED even when the skipped rows are wide enough that charging them would
/// exceed the budget.
///
/// The wide fixture has `WIDE_ROW_COUNT` rows of ~`WIDE_PAYLOAD_BYTES` each.
/// `LIMIT WIDE_ROWS_UNDER_BUDGET OFFSET WIDE_OFFSET` under `SHARED_BUDGET_BYTES`:
/// the `WIDE_OFFSET` skipped rows total ~40 KiB (well over the ~24 KiB budget),
/// but the 2 returned rows total ~20 KiB (under budget). Pre-fix the scan
/// collected `offset + count` matching rows and charged EACH, tripping the byte
/// guard at ~row 3 → RED (`ResultTooLarge`). Post-fix the offset rows are skipped
/// uncharged (never pushed, never budgeted) and only the 2 returned rows are
/// budgeted → GREEN.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn limit_offset_skips_wide_rows_uncharged_against_budget() {
    // Skip 4 wide rows (~40 KiB, over budget), return 2 (~20 KiB, under budget).
    const WIDE_OFFSET: usize = 4;
    // Sanity on the fixture sizing: offset + limit must fit inside the fixture.
    assert!(WIDE_OFFSET + WIDE_ROWS_UNDER_BUDGET <= WIDE_ROW_COUNT as usize);

    let temp = TempDir::new().unwrap();
    let (data_dir, schema_path) = prepare_wide(temp.path()).await;

    let db = open_db_with_budget(data_dir, schema_path, SHARED_BUDGET_BYTES).await;

    let sql = format!(
        "SELECT * FROM {KS}.{WIDE_TBL} LIMIT {WIDE_ROWS_UNDER_BUDGET} OFFSET {WIDE_OFFSET}"
    );
    let result = db.execute(&sql).await.expect(
        "LIMIT/OFFSET must skip the wide OFFSET rows uncharged and budget only the \
         returned rows, not trip ResultTooLarge",
    );
    assert_eq!(
        result.rows.len(),
        WIDE_ROWS_UNDER_BUDGET,
        "must return exactly the LIMIT rows after skipping the OFFSET rows"
    );
}

/// roborev FINDING (Medium): a WIDE-partition POINT LOOKUP that satisfies
/// `is_simple_id_lookup` (`WHERE id = <value>` with ≤ 8 whitespace tokens) is
/// routed through the LEGACY `QueryExecutor`, which — pre-fix — did NOT enforce
/// the D6 byte budget. So a single wide row materialized unbounded and bypassed
/// the guard entirely. Post-fix the budget is enforced POST-materialization on the
/// legacy `QueryResult` at BOTH legacy return points (plan-cache hit and
/// fall-through), reusing the SAME estimator/enforcement as the optimizer path.
///
/// `SUB_ROW_BUDGET_BYTES` (512 B) is smaller than one ~10 KiB wide row, so the
/// point lookup of that one row must trip `ResultTooLarge`. The SQL is exactly 8
/// whitespace tokens and contains `WHERE id =`, so it trips `is_simple_id_lookup`
/// and exercises the legacy path under test (NOT the optimizer path).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wide_point_lookup_via_legacy_path_trips_byte_budget() {
    let temp = TempDir::new().unwrap();
    let (data_dir, schema_path) = prepare_wide_uuid(temp.path()).await;

    let db = open_db_with_budget(data_dir, schema_path, SUB_ROW_BUDGET_BYTES).await;

    // 8 whitespace tokens + "WHERE id =" → is_simple_id_lookup == true → legacy path.
    let sql = format!("SELECT * FROM {KS}.{WIDE_UUID_TBL} WHERE id = {LOOKUP_UUID_LITERAL}");
    assert_eq!(
        sql.split_whitespace().count(),
        8,
        "test SQL must be <= 8 tokens to trip is_simple_id_lookup"
    );
    assert!(sql.contains("WHERE id ="));

    let err = db
        .execute(&sql)
        .await
        .expect_err("wide point lookup must exceed the sub-row byte budget on the legacy path");
    match err {
        Error::ResultTooLarge {
            budget_bytes,
            estimated_bytes,
            rows,
        } => {
            assert_eq!(
                budget_bytes, SUB_ROW_BUDGET_BYTES as usize,
                "error must report the configured budget"
            );
            assert!(
                estimated_bytes > budget_bytes,
                "estimated bytes ({estimated_bytes}) must exceed budget ({budget_bytes})"
            );
            assert!(
                rows >= 1,
                "the materialized wide row must be counted; got rows={rows}"
            );
        }
        other => panic!("expected Error::ResultTooLarge on the legacy path, got {other:?}"),
    }
}

/// Control for the legacy-path budget: the SAME point lookup under a GENEROUS
/// budget succeeds and returns the row — the guard is not over-firing on normal
/// point reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wide_point_lookup_via_legacy_path_succeeds_under_generous_budget() {
    let temp = TempDir::new().unwrap();
    let (data_dir, schema_path) = prepare_wide_uuid(temp.path()).await;

    let db = open_db_with_budget(data_dir, schema_path, DEFAULT_MAX_RESULT_BYTES).await;

    let sql = format!("SELECT * FROM {KS}.{WIDE_UUID_TBL} WHERE id = {LOOKUP_UUID_LITERAL}");
    let result = db
        .execute(&sql)
        .await
        .expect("point lookup under a generous budget must succeed");
    assert_eq!(
        result.rows.len(),
        1,
        "point lookup on id=0 must return exactly the one wide row"
    );
}

/// Knob test on the legacy path: lowering `max_result_bytes` flips the SAME
/// point-lookup query from Ok → `ResultTooLarge` — the byte budget is load-bearing
/// on the legacy point-lookup path too, not just the optimizer path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_result_bytes_knob_is_load_bearing_on_legacy_point_lookup() {
    let temp = TempDir::new().unwrap();
    let (data_dir, schema_path) = prepare_wide_uuid(temp.path()).await;

    let sql = format!("SELECT * FROM {KS}.{WIDE_UUID_TBL} WHERE id = {LOOKUP_UUID_LITERAL}");

    // Generous budget: PASSES.
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
            .expect("point lookup passes under the default budget");
        assert_eq!(result.rows.len(), 1);
    }

    // Sub-row budget: the SAME query now TRIPS.
    {
        let db = open_db_with_budget(data_dir, schema_path, SUB_ROW_BUDGET_BYTES).await;
        let err = db
            .execute(&sql)
            .await
            .expect_err("lowering max_result_bytes must make the point lookup trip");
        match err {
            Error::ResultTooLarge { budget_bytes, .. } => {
                assert_eq!(budget_bytes, SUB_ROW_BUDGET_BYTES as usize);
            }
            other => panic!("expected Error::ResultTooLarge under sub-row budget, got {other:?}"),
        }
    }
}

/// FINDING A (roborev MEDIUM): the `max_result_rows` row-count safety valve must
/// be load-bearing (wired from config), not a hardcoded 1,000,000 constant.
///
/// Under a GENEROUS byte budget (so the byte guard never fires), the skinny
/// query passes with a high `max_result_rows` and TRIPS the row-count valve when
/// `max_result_rows` is lowered below the fixture's row count. Lowering the knob
/// changes what trips → it is a real behavioral knob. Pre-fix the guard used a
/// hardcoded `MAX_RESULTS = 1_000_000`, so lowering the config value had NO
/// effect and the query still passed → RED.
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
