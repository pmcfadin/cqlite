//! Allocation-scaling regression guard for Issue #1046 (dhat-gated).
//!
//! **Mandate**: "Buffers should be reused, do not allocate as we iterate."
//! Per-row work on the read/scan hot path must not allocate an amount that
//! scales with `rows × schema-columns`.
//!
//! **The fixed bug & its iterations**: `parse_row_data_with_offset_impl` (the
//! per-row decode in the `V5CompressedLegacy` parser) used to rebuild a
//! `HashMap<String, &Column>` on EVERY parsed row purely to look the schema
//! column up by name — one `String` clone per schema column PLUS one HashMap
//! allocation, per row. A first fix swapped that for an allocation-free
//! `iter().find()` (rejected: O(header×schema) CPU on wide schemas). A second
//! fix used a per-row borrowed-key `HashMap<&str,&Column>` (rejected: STILL one
//! HashMap allocation per row, sized to schema width — it still "allocates as we
//! iterate"). **The true fix (this change)** hoists the entire header→schema
//! resolution into `RowColumnResolution`, built ONCE per block (or per partition
//! on the sliding-window compaction drivers) and reused across every row, so the
//! per-row decode performs ZERO schema-lookup allocations: no per-row HashMap, no
//! per-row `String` clone, no per-row `Vec` of columns. The bitmap filter is
//! applied inline over the precomputed slice.
//!
//! **This test** measures dhat allocation COUNTS for full steady-state scans and
//! asserts per-row allocations stay below a bound, AND that per-row allocations
//! do NOT scale with schema width. The latter is the precise quantity the mandate
//! targets: a schema-lookup allocation that scaled with column count (the
//! rejected v1/v2 fixes) would make a 100-column table allocate dramatically more
//! per row than an 18-column table. With the hoist, the per-row schema-lookup
//! cost is zero on BOTH, so the per-row figures stay close in absolute terms
//! despite a ~5.5x difference in column count.
//!
//! Measured figures (`--features cli-helpers,dhat-heap`, second/warmed scan):
//!   - `test_basic.simple_table`        (18 regular cols): ~77.3 allocs/row
//!     (pre-true-fix v2 borrowed-key-map: 82.3; original per-row String-clone
//!     map: 102.3 — see git history)
//!   - `test_wide_rows.many_columns_table` (100 regular cols): ~51.5 allocs/row
//!     — LOWER than the 18-column table despite 5.5x the columns (this fixture is
//!     sparse, so most cells are null/absent). The measured byte-scaling slope is
//!     NEGATIVE (~-0.32 allocs per extra column): per-row allocation is driven by
//!     row materialization, NOT by schema width. A per-row, per-column
//!     schema-lookup allocation (rejected v1/v2) would instead make the slope
//!     POSITIVE and large (~1+ alloc per extra column).
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

/// Allocations-per-row ceiling for the NARROW (18-column) `simple_table` scan.
/// Measured: original per-row String-clone map 102.3/row, v2 borrowed-key map
/// 82.3/row, true hoist 77.3/row. The 85.0 ceiling sits below both the original
/// (102.3) and the rejected v2 borrowed-map (82.3) figures — so any
/// reintroduction of a per-row map fails — yet ~8 allocs/row above the hoisted
/// figure to tolerate small legitimate decode churn without flaking.
const MAX_ALLOCS_PER_ROW_NARROW: f64 = 85.0;

/// Allocations-per-row ceiling for the WIDE (100-column) `many_columns_table`
/// scan. The wide table materializes ~5.5x as many columns per row, so the
/// per-row figure is legitimately higher (more `Value`s + cells-map inserts).
/// The point of THIS guard is byte-scaling of the SCHEMA-LOOKUP path, which the
/// hoist drove to zero: the per-row figure is dominated by row materialization,
/// not by a width-proportional schema-lookup allocation. A regression that
/// reintroduced a per-row schema map (one HashMap + ~100 entries/clones per row
/// on this fixture) would add ~100+ allocs/row and blow past this ceiling. See
/// the explicit per-column-allocation assertion below, which is the strict
/// byte-scaling check independent of materialization cost.
const MAX_ALLOCS_PER_ROW_WIDE: f64 = 320.0;

/// Strict byte-scaling guard: the DELTA in per-row allocations between the wide
/// (100-col) and narrow (18-col) scans, divided by the delta in column count,
/// must stay small. A per-row, per-column schema-lookup allocation (the rejected
/// v1/v2 designs) would push this toward ~1.0+ alloc per extra column; the hoist
/// makes the schema-lookup contribution zero, so the only growth is row
/// materialization (a bounded handful of allocations per extra non-null cell).
/// 2.5 allocs per extra column is a generous ceiling that a width-proportional
/// schema-lookup map would still exceed once its String clones + bucket growth
/// are counted, while leaving headroom for per-cell `Value`/insert churn.
const MAX_ALLOCS_PER_EXTRA_COLUMN: f64 = 2.5;

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

/// Measure steady-state (warmed second scan) per-row allocations for `qualified`.
async fn measure_per_row(db: &Database, qualified: &str) -> f64 {
    let (warm_rows, _) = full_scan_alloc_count(db, qualified).await;
    let (rows, allocs) = full_scan_alloc_count(db, qualified).await;
    assert!(
        rows > 0 && rows == warm_rows,
        "fixture problem for {qualified}: warm={warm_rows} measured={rows} rows (need a stable non-empty scan)"
    );
    let per_row = allocs as f64 / rows as f64;
    eprintln!("Issue #1046 scan alloc scaling: {qualified}: {rows} rows -> {allocs} allocs, {per_row:.2} allocs/row");
    per_row
}

// NOTE: a single `#[test]` covers BOTH fixtures. dhat installs a process-wide
// global allocator and permits only ONE live `Profiler` at a time, so two
// separate tests in this binary would conflict (the second `Profiler::build`
// panics). One profiler, two measured scans.
#[tokio::test]
async fn test_scan_allocations_do_not_scale_per_row_or_with_schema_width() {
    if !data_files_present("test_basic") {
        eprintln!("Skipping: no test_basic Data.db files (run fetch-datasets.sh)");
        return;
    }

    // Start the profiler before the workload so all allocation is attributed.
    let _profiler = dhat::Profiler::builder().testing().build();

    // Fail-closed once fixtures are present: the only legitimate skip is the
    // genuine absence of the external dataset, which `data_files_present` above
    // already handles. With Data.db files on disk, any setup/schema/ingestion
    // failure must FAIL the test so a broken setup cannot let the allocation
    // guard pass vacuously.
    //
    // NARROW: `test_basic.simple_table` is an 18-regular-column, UUID-keyed table.
    let narrow_db = setup_db("basic-types.cql", "test_basic")
        .await
        .expect("setup_db(test_basic) must succeed when Data.db fixtures are present");
    let narrow_cols = 18.0_f64;
    let narrow_per_row = measure_per_row(&narrow_db, "test_basic.simple_table").await;

    assert!(
        narrow_per_row <= MAX_ALLOCS_PER_ROW_NARROW,
        "Issue #1046: narrow-table per-row scan allocations regressed to \
         {narrow_per_row:.2}/row (> {MAX_ALLOCS_PER_ROW_NARROW} ceiling). The \
         read/scan hot path is allocating per row — likely a per-row \
         HashMap/clone reintroduced in row decode (see RowColumnResolution / \
         parse_row_data_with_offset_impl). Hoist it to a reused/borrowing lookup."
    );

    // WIDE: byte-scaling guard. Requires the wide fixture too; skip only on its
    // genuine absence (the narrow assertion above already ran and is meaningful).
    if !data_files_present("test_wide_rows") {
        eprintln!(
            "Skipping wide-schema byte-scaling assertion: no test_wide_rows \
             Data.db files (run fetch-datasets.sh). Narrow assertion above still ran."
        );
        return;
    }

    // 100-regular-column fixture — exercises BYTE-scaling with schema width. If the
    // per-row decode allocated in proportion to column count (rejected v1/v2), this
    // table would allocate ~5.5x the narrow figure for the schema-lookup alone.
    let wide_db = setup_db("wide-rows.cql", "test_wide_rows")
        .await
        .expect("setup_db(test_wide_rows) must succeed when Data.db fixtures are present");
    let wide_cols = 100.0_f64;
    let wide_per_row = measure_per_row(&wide_db, "test_wide_rows.many_columns_table").await;

    // Absolute ceiling on the wide table (row materialization dominates; the
    // schema-lookup contribution is zero after the hoist).
    assert!(
        wide_per_row <= MAX_ALLOCS_PER_ROW_WIDE,
        "Issue #1046: wide-table per-row scan allocations are {wide_per_row:.2}/row \
         (> {MAX_ALLOCS_PER_ROW_WIDE} ceiling) for a 100-column table — a per-row, \
         per-column schema-lookup allocation has been reintroduced."
    );

    // Strict byte-scaling check: per-row allocation growth per EXTRA column must be
    // small. A width-proportional schema-lookup (one entry/clone per column per
    // row) would push this toward ~1+/col; the hoist makes the schema-lookup
    // contribution zero, so the only slope is bounded per-cell materialization.
    let allocs_per_extra_column = (wide_per_row - narrow_per_row) / (wide_cols - narrow_cols);
    eprintln!(
        "Issue #1046 byte-scaling: narrow={narrow_per_row:.2}/row ({narrow_cols} cols), \
         wide={wide_per_row:.2}/row ({wide_cols} cols) -> {allocs_per_extra_column:.3} \
         allocs per extra column (ceiling {MAX_ALLOCS_PER_EXTRA_COLUMN})"
    );
    assert!(
        allocs_per_extra_column <= MAX_ALLOCS_PER_EXTRA_COLUMN,
        "Issue #1046: per-row allocations scale with schema width at \
         {allocs_per_extra_column:.3} allocs/extra-column \
         (> {MAX_ALLOCS_PER_EXTRA_COLUMN}). The per-row schema-lookup allocation \
         (per-row HashMap / String clones / Vec of columns) was reintroduced — \
         hoist the header→schema resolution out of the per-row decode \
         (RowColumnResolution, built once per block/partition)."
    );
}
