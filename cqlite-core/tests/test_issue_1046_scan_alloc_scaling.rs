//! Allocation-scaling regression guard for Issue #1046 (dhat-gated).
//!
//! **Mandate**: "Buffers should be reused, do not allocate as we iterate."
//! Per-row work on the read/scan hot path must not allocate an amount that
//! scales with `rows × schema-columns`.
//!
//! **The fixed bug**: `parse_row_data_with_offset_impl` (the per-row decode in
//! the `V5CompressedLegacy` parser) rebuilt a `HashMap<String, &Column>` on
//! EVERY parsed row purely to look the schema column up by name — one `String`
//! clone per schema column PLUS one HashMap allocation, per row. A dhat heap
//! profile of a full `test_basic.simple_table` scan (18 regular columns)
//! showed this as the single largest allocation-count call-site, growing
//! linearly with the number of rows scanned. The fix replaces the per-row map
//! with an allocation-free linear `iter().find()` over `schema.columns`
//! (byte-for-byte equivalent lookup), removing ~1 HashMap + ~18 String
//! allocations per row.
//!
//! **This test** measures dhat allocation COUNTS for a full steady-state scan of
//! `test_basic.simple_table` (18 regular columns) and asserts the
//! allocations-PER-ROW stays below a bound that the pre-fix code (which added
//! ~1 HashMap + ~18 String clones per row) comfortably exceeded. It measures a
//! SECOND, warmed scan so one-time setup (db open, schema parse, lazy reader
//! init) is excluded from the delta — the figure is the per-row decode +
//! materialization cost only. Per-row allocation is the precise quantity the
//! mandate targets: "do not allocate as we iterate." A regression that
//! reintroduces per-row, per-column allocation pushes this number up in
//! proportion to schema width and trips the assert. It is fail-closed: a real
//! `assert!`, gated only on the `dhat-heap` feature being available.
//!
//! Proof the guard is non-vacuous, measured on `test_basic.simple_table`
//! (1000 rows, 18 regular columns) via `--features cli-helpers,dhat-heap`:
//! WITHOUT the fix (per-row HashMap restored) the scan allocates 102.32
//! allocs/row and the assert FAILS; WITH the fix it allocates 82.32 allocs/row
//! and the assert PASSES. The fix removes ~20 allocations/row (1 HashMap + 18
//! name `String` clones + the map's first bucket grow), and the 92.0 ceiling
//! sits ~10 allocs/row on either side of both figures.
//!
//! Run via:
//! ```text
//! env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!   cargo test --package cqlite-core --features cli-helpers,dhat-heap \
//!   --test test_issue_1046_scan_alloc_scaling --profile bench
//! ```
//!
//! **Requirements**: `CQLITE_DATASETS_ROOT`, real Data.db files.

#![cfg(all(
    feature = "state_machine",
    feature = "cli-helpers",
    feature = "dhat-heap"
))]

use std::path::PathBuf;

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::Database;

// The dhat allocator must be the global allocator to observe every allocation.
// This test binary is separate from all others, so installing it here does not
// affect normal builds or other test binaries.
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

/// Allocations-per-row ceiling for a full steady-state scan. Measured figures
/// (see module docs): pre-fix 102.32/row, post-fix 82.32/row — the per-row
/// HashMap build + ~18 name clones contributed ~20/row of pure overhead. The
/// 92.0 ceiling sits ~10 allocs/row below the pre-fix figure (so the old code
/// fails with margin) and ~10 above the post-fix figure (so the guard tolerates
/// small legitimate decode churn without flaking). A regression that
/// reintroduces per-row, per-column allocation pushes the figure back above 92
/// and trips this assert.
const MAX_ALLOCS_PER_ROW: f64 = 92.0;

fn get_datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

fn get_schemas_dir() -> Option<PathBuf> {
    if let Some(datasets_root) = get_datasets_root() {
        let schemas_dir = datasets_root.parent()?.join("schemas");
        if schemas_dir.exists() {
            return Some(schemas_dir);
        }
    }
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let schemas_dir = manifest_dir.parent()?.join("test-data").join("schemas");
    schemas_dir.exists().then_some(schemas_dir)
}

fn data_files_present(keyspace: &str) -> bool {
    let Some(root) = get_datasets_root() else {
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

async fn setup_db(schema_file: &str, keyspace: &str) -> Result<Database, String> {
    let datasets_root =
        get_datasets_root().ok_or_else(|| "CQLITE_DATASETS_ROOT not set".to_string())?;
    let schemas_dir = get_schemas_dir().ok_or_else(|| "schemas dir not found".to_string())?;
    let schema_path = schemas_dir.join(schema_file);
    if !schema_path.exists() {
        return Err(format!("schema not found at {:?}", schema_path));
    }

    let config = IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir: datasets_root.join("sstables"),
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(format!("/{}/", keyspace)),
    };
    let result = ingest(config)
        .await
        .map_err(|e| format!("ingestion failed: {}", e))?;
    Ok(result.database)
}

/// Run a full scan and return `(rows_seen, allocations_during_scan)` measured as
/// the dhat `total_blocks` delta across the query. Counting the delta (not the
/// absolute total) excludes profiler/setup allocations that happened before the
/// scan, so the value is purely this scan's own allocation count.
async fn full_scan_alloc_count(db: &Database, qualified: &str) -> (usize, u64) {
    let before = dhat::HeapStats::get().total_blocks;
    let sql = format!("SELECT * FROM {qualified}");
    let result = db.execute(&sql).await.expect("scan query should succeed");
    let after = dhat::HeapStats::get().total_blocks;
    (result.rows.len(), after.saturating_sub(before))
}

#[tokio::test]
async fn test_scan_allocations_do_not_scale_per_row() {
    if !data_files_present("test_basic") {
        eprintln!("Skipping: no Data.db files (run fetch-datasets.sh)");
        return;
    }

    // Start the profiler before the workload so all allocation is attributed.
    let _profiler = dhat::Profiler::builder().testing().build();

    // Fail-closed once fixtures are present: the only legitimate skip is the
    // genuine absence of the external dataset, which `data_files_present` above
    // already handles. With Data.db files on disk, any setup/schema/ingestion
    // failure must FAIL the test so a broken setup cannot let the allocation
    // guard pass vacuously.
    let db = setup_db("basic-types.cql", "test_basic")
        .await
        .expect("setup_db must succeed when Data.db fixtures are present");

    // `test_basic.simple_table` is a wide-schema (18 regular column) UUID-keyed
    // table; the per-row decode dominates a full scan's allocation count. The
    // first scan warms any lazily-built reader/cache state so it is not charged
    // to the measured (second) scan — that delta is steady-state per-row decode
    // + row-materialization cost only.
    let qualified = "test_basic.simple_table";
    let (warm_rows, _) = full_scan_alloc_count(&db, qualified).await;
    let (rows, allocs) = full_scan_alloc_count(&db, qualified).await;

    assert!(
        rows > 0 && rows == warm_rows,
        "fixture problem: warm={warm_rows} measured={rows} rows (need a stable non-empty scan)"
    );

    let per_row = allocs as f64 / rows as f64;
    eprintln!(
        "Issue #1046 scan alloc scaling: {rows} rows -> {allocs} allocs, \
         {per_row:.2} allocs/row (ceiling {MAX_ALLOCS_PER_ROW})"
    );

    assert!(
        per_row <= MAX_ALLOCS_PER_ROW,
        "Issue #1046: per-row scan allocations regressed to {per_row:.2}/row \
         (> {MAX_ALLOCS_PER_ROW} ceiling). The read/scan hot path is allocating \
         per row in proportion to schema width — likely a per-row HashMap/clone \
         reintroduced in row decode (see parse_row_data_with_offset_impl). Hoist \
         it to a reused/borrowing lookup."
    );
}
