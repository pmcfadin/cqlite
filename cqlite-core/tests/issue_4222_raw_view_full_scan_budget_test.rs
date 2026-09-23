//! Issue #4222 — raw SSTable view, bounded full-scan row producer.
//!
//! Spec: "A full scan is bounded by the existing result-byte budget, not
//! table size" — a no-predicate query against a `_raw_sstable_data` view with
//! a small configured `max_result_bytes` must return the existing
//! budget-exceeded error rather than materializing the whole corpus.
//!
//! Fixture discipline (#3220/#3121, roborev finding, issue #4222):
//! `resurrection_gc_positive`'s `Data.db` is NOT git-committed — only its
//! JSONL/`.txt`/`.crc32` sidecars are — so it depends entirely on
//! `fetch-datasets.sh` populating an out-of-tree `CQLITE_DATASETS_ROOT`.
//! This lane SKIPs cleanly whenever no candidate root carries the table's
//! real bytes; it does NOT replicate issue #3121's two-level SKIP/PANIC
//! rule (see `issue_4222_raw_view_point_read_test.rs`'s module doc for why:
//! `scripts/agent-gate.sh` UNCONDITIONALLY exports `CQLITE_DATASETS_ROOT`
//! for every test run, so "is the env var set" can never signal "a real
//! fetch happened" here). `CQLITE_REQUIRE_FIXTURES=1` turns even the
//! clean-skip case into a hard failure. See
//! `issue_4222_raw_view_point_read_test.rs`'s identical helper for the
//! reference implementation this duplicates (cross-file sharing isn't
//! available to `#[path]`-included test support modules).

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

#[path = "support/datasets_root.rs"]
mod datasets_root;

use cqlite_core::{ingestion::ingest, ingestion::IngestionConfig, Config, Database, Error};
use datasets_root::{describe_search, schema_path, sstables_root_for_table};

const KEYSPACE: &str = "test_tomb";
const TABLE: &str = "resurrection_gc_positive";

fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

/// See `issue_4222_raw_view_point_read_test.rs::test_tomb_root_or_skip` —
/// identical plain-skip rule, duplicated here.
fn test_tomb_root_or_skip(table: &str) -> Option<std::path::PathBuf> {
    if let Some(root) = sstables_root_for_table(KEYSPACE, table) {
        return Some(root);
    }
    if require_fixtures_strict() {
        panic!(
            "CQLITE_REQUIRE_FIXTURES=1 but '{KEYSPACE}.{table}' was not found under any \
             candidate root — fetch the corpus first \
             (bash test-data/scripts/fetch-datasets.sh): {}",
            describe_search(KEYSPACE, table)
        );
    }
    eprintln!(
        "SKIP: '{KEYSPACE}.{table}' (fetch-only fixture) was not found under any candidate \
         root — {}",
        describe_search(KEYSPACE, table)
    );
    None
}

async fn open_db_with_byte_budget(max_result_bytes: u64) -> Option<Database> {
    open_db_with_budgets(max_result_bytes, None).await
}

/// `max_result_rows` is left at its default when `None` — used by the
/// LIMIT-exemption test below, which needs a SMALL row-count valve to
/// prove an explicit LIMIT waives it (roborev finding, issue #4222 —
/// round 8).
async fn open_db_with_budgets(
    max_result_bytes: u64,
    max_result_rows: Option<u64>,
) -> Option<Database> {
    let root = test_tomb_root_or_skip(TABLE)?;
    let schema = schema_path("tombstone-parity.cql")
        .expect("committed schema tombstone-parity.cql must be readable (#3148)");
    let mut core_config = Config::default();
    core_config.query.max_result_bytes = max_result_bytes;
    if let Some(rows) = max_result_rows {
        core_config.query.max_result_rows = rows;
    }
    core_config
        .validate()
        .expect("a max_result_bytes/max_result_rows budget must be a VALID configuration");
    let cfg = IngestionConfig {
        schema_paths: vec![schema],
        data_dir: root,
        version_hint: None,
        core_config,
        table_directory_filter: Some(format!("/{KEYSPACE}/")),
    };
    Some(
        ingest(cfg)
            .await
            .expect("ingestion of the fixture")
            .database,
    )
}

/// THE RED CASE: a no-predicate `_raw_sstable_data` query (the full-scan
/// producer, spec's "bounded full scan" requirement) under a byte-budget too
/// small for even one row must return `Error::ResultTooLarge`, never
/// materialize the whole corpus first and fail after the fact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_scan_over_budget_returns_result_too_large() {
    let Some(db) = open_db_with_byte_budget(1).await else {
        return;
    };
    let query = format!("SELECT * FROM {KEYSPACE}.{TABLE}_raw_sstable_data");
    let outcome = db.execute(&query).await;
    match outcome {
        Err(Error::ResultTooLarge { budget_bytes, .. }) => {
            assert_eq!(
                budget_bytes, 1,
                "the reported budget must be the CONFIGURED one"
            );
        }
        Err(other) => panic!(
            "expected Error::ResultTooLarge under a 1-byte budget, got a DIFFERENT error: {other}"
        ),
        Ok(result) => panic!(
            "REGRESSION (issue #4222): a 1-byte max_result_bytes budget was IGNORED by the raw \
             view's full-scan producer — it returned {} rows instead of failing closed",
            result.rows.len()
        ),
    }
}

/// Sanity: a generous budget over the SAME no-predicate query returns real,
/// non-empty rows spanning BOTH generations (never a vacuous empty pass).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_scan_under_budget_returns_every_physical_row() {
    let Some(db) = open_db_with_byte_budget(64 * 1024 * 1024).await else {
        return;
    };
    let query = format!("SELECT * FROM {KEYSPACE}.{TABLE}_raw_sstable_data");
    let result = db
        .execute(&query)
        .await
        .expect("a generous budget must not trip the guard");
    // gen-1: 5 (pk=1) + 3 (pk=2) live rows. gen-2: 2 (pk=1 row/cell
    // tombstones) + 1 (pk=2 partition tombstone) = 11 total physical rows,
    // matching the point-read test's per-key counts summed across both keys.
    assert_eq!(
        result.rows.len(),
        11,
        "the unbounded full scan must surface EVERY physical row across BOTH generations, \
         unreconciled"
    );
}

/// Roborev finding (issue #4222, round 8): every existing LIMIT/OFFSET
/// assertion (`issue_4222_raw_view_point_read_test.rs::limit_and_offset_are_honored`)
/// uses `WHERE pk = ?`, which always routes to the POINT producer — the
/// full-scan producer's own `stop_after`/`ControlFlow::Break` early-stop
/// path (`scan.rs`) was never exercised by any test. A no-predicate
/// `LIMIT 2` against the known 11-row corpus is the direct regression test
/// for that gap: it must return EXACTLY 2, never the whole corpus.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_scan_limit_stops_early_without_a_predicate() {
    let Some(db) = open_db_with_byte_budget(64 * 1024 * 1024).await else {
        return;
    };
    let query = format!("SELECT * FROM {KEYSPACE}.{TABLE}_raw_sstable_data LIMIT 2");
    let result = db
        .execute(&query)
        .await
        .expect("a no-predicate LIMIT query must succeed");
    assert_eq!(
        result.rows.len(),
        2,
        "a no-predicate 'LIMIT 2' must return EXACTLY 2 rows via the full-scan producer's \
         own stop_after/ControlFlow::Break path, never the whole 11-row corpus"
    );
}

/// Roborev finding (issue #4222, round 8): an explicit LIMIT must exempt
/// the ROW-COUNT valve (`max_result_rows`, issue #1578's convention) on the
/// FULL-SCAN path too, not just the point-key path
/// (`limit_and_offset_are_honored`'s existing coverage). `max_result_rows`
/// is configured well below the known 11-row corpus, but an explicit
/// `LIMIT 5` (< 11, > max_result_rows) must still succeed and return
/// exactly 5 — never `Error::ResultTooLarge`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_scan_limit_exempts_the_row_count_valve() {
    let Some(db) = open_db_with_budgets(64 * 1024 * 1024, Some(2)).await else {
        return;
    };
    let query = format!("SELECT * FROM {KEYSPACE}.{TABLE}_raw_sstable_data LIMIT 5");
    let result = db.execute(&query).await.expect(
        "an explicit LIMIT must exempt the row-count valve on the full-scan path too — a \
         REGRESSION here would wrongly trip Error::ResultTooLarge under a LIMIT smaller than \
         the corpus but larger than max_result_rows",
    );
    assert_eq!(result.rows.len(), 5);
}
