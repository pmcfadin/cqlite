//! Bounded-memory guard for the CLI query cutover (issue #1581, Epic D5),
//! dhat-gated (mirrors the cqlite-core #1046/#790 allocation-guard pattern).
//!
//! **The bug**: the CLI query path materialized the WHOLE result before applying
//! LIMIT (`Database::execute` scans + materializes every matching row, then
//! truncates). A `SELECT * FROM huge_table LIMIT 10` therefore held the whole
//! table resident. **The fix** routes the CLI through
//! `commands::collect_query_result` → `Database::execute_streaming`, whose
//! producer early-stops the scan once the SQL `LIMIT` is satisfied, so only the
//! returned rows are ever materialized.
//!
//! **This test** measures the dhat `total_blocks` allocation delta of the actual
//! CLI collector for a `LIMIT 1` query vs a full scan of the same multi-row
//! table. With the streaming cutover the `LIMIT 1` scan allocates a small
//! fraction of the full scan (it stops after one row). Against the pre-change
//! `execute()`-based collector the two are ~equal (LIMIT is applied only AFTER
//! full materialization), so this assertion FAILS — the red-first proof of the
//! bug. Allocation counts are deterministic (unlike wall-clock/RSS), so this
//! guard is stable and not load-sensitive; a 99-row fixture is sufficient because
//! the guard compares 1-row vs whole-table allocation, not absolute bytes.
//!
//! Run:
//! ```text
//! env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!   cargo test --package cqlite-cli --features dhat-heap \
//!   --test issue_1581_query_stream_memory --profile dev
//! ```

#![cfg(feature = "dhat-heap")]

use std::path::PathBuf;

use cqlite_cli::commands::collect_query_result;
use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::Database;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

/// The full scan must allocate at least this many times more blocks than the
/// `LIMIT 1` scan. On the streaming cutover the ratio is large (full scan
/// materializes ~99 rows, `LIMIT 1` materializes ~1). On the pre-change
/// `execute()` collector the ratio is ~1 (both materialize the whole table), so
/// this bound FAILS — the red-first signal.
const MIN_FULL_TO_LIMIT_ALLOC_RATIO: f64 = 3.0;

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

fn keyspace_has_data(keyspace: &str) -> bool {
    let Some(root) = datasets_root() else {
        return false;
    };
    let dir = root.join("sstables").join(keyspace);
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return false;
    };
    for entry in entries.flatten() {
        if let Ok(files) = std::fs::read_dir(entry.path()) {
            for file in files.flatten() {
                if file
                    .file_name()
                    .to_str()
                    .is_some_and(|n| n.ends_with("-Data.db"))
                {
                    return true;
                }
            }
        }
    }
    false
}

async fn setup_db(schema_file: &str, keyspace: &str) -> Database {
    let root = datasets_root().expect("CQLITE_DATASETS_ROOT must be set");
    let schema_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("test-data")
        .join("schemas")
        .join(schema_file);
    let config = IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir: root.join("sstables"),
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(format!("/{keyspace}/")),
    };
    ingest(config)
        .await
        .expect("ingest must succeed with fixtures present")
        .database
}

/// Allocation blocks consumed by one `collect_query_result` call.
async fn collect_alloc_blocks(db: &Database, query: &str, limit: Option<usize>) -> u64 {
    let before = dhat::HeapStats::get().total_blocks;
    let result = collect_query_result(db, query, limit)
        .await
        .expect("collect_query_result must succeed");
    // Keep the result alive across the measurement so its allocations count.
    let _rows = result.rows.len();
    let after = dhat::HeapStats::get().total_blocks;
    after.saturating_sub(before)
}

#[tokio::test]
async fn limit_query_is_bounded_not_full_table_scan() {
    if !keyspace_has_data("test_basic") {
        eprintln!("Skipping: no test_basic Data.db fixtures (run fetch-datasets.sh)");
        return;
    }

    let _profiler = dhat::Profiler::builder().testing().build();

    let db = setup_db("basic-types.cql", "test_basic").await;
    // 99-row fixture — enough that whole-table vs one-row allocation differ clearly.
    let table = "test_basic.compression_test_table";

    // Sanity: the fixture really has many rows (never let the guard pass vacuously
    // on an empty/1-row table where LIMIT 1 == full scan trivially).
    let full_rows = collect_query_result(&db, &format!("SELECT * FROM {table}"), None)
        .await
        .expect("full scan must succeed")
        .rows
        .len();
    assert!(
        full_rows >= 20,
        "fixture problem: {table} has only {full_rows} rows; need a multi-row table to \
         distinguish bounded LIMIT from a full scan"
    );

    // Warm caches so the measured deltas reflect steady-state scan allocation.
    let _ = collect_alloc_blocks(&db, &format!("SELECT * FROM {table} LIMIT 1"), None).await;
    let _ = collect_alloc_blocks(&db, &format!("SELECT * FROM {table}"), None).await;

    let limit1 = collect_alloc_blocks(&db, &format!("SELECT * FROM {table} LIMIT 1"), None).await;
    let full = collect_alloc_blocks(&db, &format!("SELECT * FROM {table}"), None).await;

    eprintln!("issue #1581 bounded-memory: LIMIT 1 = {limit1} blocks, full scan = {full} blocks");
    assert!(
        limit1 > 0 && full > 0,
        "measurement failed: limit1={limit1} full={full}"
    );

    let ratio = full as f64 / limit1 as f64;
    assert!(
        ratio >= MIN_FULL_TO_LIMIT_ALLOC_RATIO,
        "Issue #1581 REGRESSION: `SELECT * LIMIT 1` allocated {limit1} blocks vs {full} for the \
         full scan (ratio {ratio:.2} < {MIN_FULL_TO_LIMIT_ALLOC_RATIO}). The CLI query path is \
         materializing the whole table before applying LIMIT instead of early-stopping via \
         `execute_streaming` (collect_query_result)."
    );
}
