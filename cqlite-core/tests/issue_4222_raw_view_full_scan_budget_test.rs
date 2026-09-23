//! Issue #4222 — raw SSTable view, bounded full-scan row producer.
//!
//! Spec: "A full scan is bounded by the existing result-byte budget, not
//! table size" — a no-predicate query against a `_raw_sstable_data` view with
//! a small configured `max_result_bytes` must return the existing
//! budget-exceeded error rather than materializing the whole corpus.
//!
//! Fixture discipline (#3220/#3121, roborev finding, issue #4222):
//! `resurrection_gc_positive`'s `Data.db` is NOT git-committed — only its
//! JSONL/`.txt`/`.crc32` sidecars are — so it is always absent from a bare
//! checkout and depends on `fetch-datasets.sh` populating an out-of-tree
//! `CQLITE_DATASETS_ROOT`. This lane SKIPs cleanly when no fetched root is
//! configured (the checkout alone can never carry this fixture), and PANICs
//! only when a fetched root IS configured but still lacks the table (a
//! dropped/renamed fixture — a real regression, not a legitimate skip).
//! `CQLITE_REQUIRE_FIXTURES=1` turns even the clean-skip case into a hard
//! failure. See `issue_4222_raw_view_point_read_test.rs`'s identical helper
//! for the reference implementation this duplicates (cross-file sharing
//! isn't available to `#[path]`-included test support modules).

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

#[path = "support/datasets_root.rs"]
mod datasets_root;

use cqlite_core::{ingestion::ingest, ingestion::IngestionConfig, Config, Database, Error};
use datasets_root::{
    describe_search, fetched_root_is_configured, schema_path, sstables_root_for_table,
};

const KEYSPACE: &str = "test_tomb";
const TABLE: &str = "resurrection_gc_positive";

fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

/// See `issue_4222_raw_view_point_read_test.rs::test_tomb_root_or_skip` —
/// identical two-level SKIP/PANIC rule, duplicated here.
fn test_tomb_root_or_skip(table: &str) -> Option<std::path::PathBuf> {
    if let Some(root) = sstables_root_for_table(KEYSPACE, table) {
        return Some(root);
    }
    if fetched_root_is_configured() {
        panic!(
            "a fetched CQLITE_DATASETS_ROOT is configured but does not carry \
             '{KEYSPACE}.{table}' — a renamed/regenerated/dropped fixture must FAIL here, \
             not silently skip (issue #3121's rule, applied here): {}",
            describe_search(KEYSPACE, table)
        );
    }
    if require_fixtures_strict() {
        panic!(
            "CQLITE_REQUIRE_FIXTURES=1 but no fetched CQLITE_DATASETS_ROOT is configured and \
             the checkout alone never carries '{KEYSPACE}.{table}' — fetch the corpus first \
             (bash test-data/scripts/fetch-datasets.sh)"
        );
    }
    eprintln!(
        "SKIP: no fetched CQLITE_DATASETS_ROOT configured, and the checkout's own committed \
         corpus never carries '{KEYSPACE}.{table}' (fetch-only fixture) — {}",
        describe_search(KEYSPACE, table)
    );
    None
}

async fn open_db_with_byte_budget(max_result_bytes: u64) -> Option<Database> {
    let root = test_tomb_root_or_skip(TABLE)?;
    let schema = schema_path("tombstone-parity.cql")
        .expect("committed schema tombstone-parity.cql must be readable (#3148)");
    let mut core_config = Config::default();
    core_config.query.max_result_bytes = max_result_bytes;
    core_config
        .validate()
        .expect("a max_result_bytes budget must be a VALID configuration");
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
