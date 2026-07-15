//! Row-assembly allocations-per-row AND per-cell budget guard (issue #2075;
//! dhat-gated). This is the MEASUREMENT harness only — it does NOT implement any
//! smallvec/RowCells optimization (that is #1645 item 2, explicitly out of scope).
//!
//! **What it pins.** The full-scan public surface
//! `Database::execute("SELECT * FROM ks.table")` drives the real decode →
//! `RowCells` (`Vec<(Arc<str>, Value)>`, `cqlite-core/src/types.rs`) assembly →
//! `QueryRow` path. This guard measures the dhat allocation COUNT of that path and
//! pins TWO absolute budgets per fixture shape: `allocs_per_row =
//! scan_alloc_delta / rows` and `allocs_per_cell = scan_alloc_delta /
//! total_cells`, where `total_cells` is the sum over the result of each row's
//! materialized cell count (`QueryRow::values.len()`). dhat allocation counts are
//! deterministic and machine-independent, so they are a hard, load-deterministic
//! regression net.
//!
//! **Why this is NOT a duplicate of `test_issue_1046_scan_alloc_scaling.rs`.**
//! #1046 is a schema-WIDTH-SCALING guard: it asserts per-row allocations do not
//! grow *in proportion to column count* (the delta per extra column stays small),
//! catching a reintroduced per-row, per-column schema-lookup map. It has NO
//! per-cell metric and pins no absolute per-cell budget. #2075 pins ABSOLUTE
//! per-row AND per-cell budgets (the per-cell metric #1046 lacks) across a
//! wide-row shape AND a text-heavy shape, so the H2-promised allocs/row +
//! allocs/cell budgets exist and #1645 item 2's smallvec-RowCells win is
//! measurable and gateable. The two guards are complementary: #1046 watches the
//! *slope* vs schema width, #2075 watches the *absolute level* per row and per
//! cell.
//!
//! **Two shapes.**
//!   - WIDE-ROW: `test_wide_rows.many_columns_table` (100 regular cols, UUID PK) —
//!     stresses schema width / number of materialized cells per row.
//!   - TEXT-HEAVY: `test_wide_rows.document_versions` (TEXT title/content/
//!     change_summary + `SET<TEXT>` tags + `MAP<TEXT,TEXT>` metadata) — stresses
//!     per-cell String/collection materialization, the RowCells assembly cost.
//!
//! **Non-vacuous by construction.** Once a fixture's binaries are present the
//! guard asserts `rows > 0` AND `total_cells > 0` AND `scan_alloc_delta > 0`
//! before checking any ceiling — a present fixture that scans empty or allocates
//! nothing is a setup/measurement FAILURE, never a passing "0 ≤ budget". Only a
//! genuinely absent dataset skip-registers (early return + actionable eprintln).
//!
//! ## Run via:
//! ```text
//! env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!   cargo test --package cqlite-core --features cli-helpers,dhat-heap,arrow \
//!   --test issue_2075_row_assembly_alloc_budget -- --test-threads=1
//! ```
//! (`--test-threads=1` is mandatory: `dhat::Profiler` installs a process-global
//! allocator and permits only one live profiler per process.)

#![cfg(all(
    feature = "dhat-heap",
    feature = "cli-helpers",
    feature = "state_machine"
))]

// The dhat allocator must be the global allocator to observe every allocation.
// This test binary is separate from all others, so installing it here does not
// affect normal builds or other test binaries.
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[path = "../benches/fixtures/mod.rs"]
mod fixtures;

use fixtures::ReadFixture;

// -- Ceilings -------------------------------------------------------------------
//
// Baselines measured on 2026-07-15 on this branch's `main` base
// (`cargo test --features cli-helpers,dhat-heap,arrow`, warmed second scan):
//
//   test_wide_rows.many_columns_table  (WIDE-ROW):
//     50 rows, 400 cells -> 1103 allocs; 22.06 allocs/row, 2.76 allocs/cell
//   test_wide_rows.document_versions   (TEXT-HEAVY):
//     50 rows, 550 cells -> 2454 allocs; 49.08 allocs/row, 4.46 allocs/cell
//
// Ceilings sit at ~1.3–1.5x the measured value: tight enough that #1645 item 2
// (smallvec RowCells) shows a measurable win here, loose enough to absorb small
// legitimate decode churn without flaking.

/// WIDE-ROW allocations-per-row ceiling (`many_columns_table`).
/// Measured 22.06/row on 2026-07-15 main; ceiling 32.0 = ~1.45x headroom.
const MAX_ALLOCS_PER_ROW_WIDE: f64 = 32.0;

/// WIDE-ROW allocations-per-cell ceiling (`many_columns_table`).
/// Measured 2.76/cell on 2026-07-15 main; ceiling 4.0 = ~1.45x headroom.
const MAX_ALLOCS_PER_CELL_WIDE: f64 = 4.0;

/// TEXT-HEAVY allocations-per-row ceiling (`document_versions`).
/// Measured 49.08/row on 2026-07-15 main; ceiling 68.0 = ~1.39x headroom.
const MAX_ALLOCS_PER_ROW_TEXT: f64 = 68.0;

/// TEXT-HEAVY allocations-per-cell ceiling (`document_versions`).
/// Measured 4.46/cell on 2026-07-15 main; ceiling 6.5 = ~1.46x headroom.
const MAX_ALLOCS_PER_CELL_TEXT: f64 = 6.5;

/// One measured full-scan of `qualified`: warm once (so first-touch lazy statics /
/// caches land in the pre-measurement baseline), then measure the dhat
/// `total_blocks` delta across a second `execute`. Returns
/// `(rows, total_cells, scan_alloc_delta)` where `total_cells` sums each row's
/// materialized cell count (`QueryRow::values.len()`).
fn measure_scan(
    rt: &tokio::runtime::Runtime,
    db: &cqlite_core::Database,
    qualified: &str,
) -> (usize, usize, u64) {
    let sql = format!("SELECT * FROM {qualified}");
    // Warm.
    let _ = rt
        .block_on(db.execute(&sql))
        .expect("warm fixture scan must succeed when Data.db is present");
    // Measure the steady-state scan.
    let before = dhat::HeapStats::get().total_blocks;
    let result = rt
        .block_on(db.execute(&sql))
        .expect("fixture scan must succeed when Data.db is present");
    let after = dhat::HeapStats::get().total_blocks;
    let rows = result.rows.len();
    let total_cells: usize = result.rows.iter().map(|r| r.values.len()).sum();
    (rows, total_cells, after.saturating_sub(before))
}

/// Assert a single shape's per-row and per-cell budgets (fail-closed + non-vacuous).
fn assert_shape(
    rt: &tokio::runtime::Runtime,
    db: &cqlite_core::Database,
    qualified: &str,
    shape: &str,
    max_per_row: f64,
    max_per_cell: f64,
) {
    let (rows, total_cells, allocs) = measure_scan(rt, db, qualified);

    // Non-vacuity: a present fixture MUST yield rows, cells, and allocations.
    assert!(
        rows > 0,
        "issue #2075 ({shape}): fixture {qualified} returned 0 rows — a present \
         fixture that scans empty is a setup failure, not a passing 0-alloc budget"
    );
    assert!(
        total_cells > 0,
        "issue #2075 ({shape}): fixture {qualified} materialized 0 cells over \
         {rows} rows — refusing a vacuous per-cell pass"
    );
    assert!(
        allocs > 0,
        "issue #2075 ({shape}): scan of {qualified} observed 0 allocations over \
         {rows} rows / {total_cells} cells — the decode→RowCells assembly did not \
         execute; refusing a vacuous pass"
    );

    let per_row = allocs as f64 / rows as f64;
    let per_cell = allocs as f64 / total_cells as f64;
    eprintln!(
        "issue #2075 row-assembly alloc budget [{shape}]: {qualified}: \
         {rows} rows, {total_cells} cells -> {allocs} allocs; \
         {per_row:.2} allocs/row (ceiling {max_per_row}), \
         {per_cell:.2} allocs/cell (ceiling {max_per_cell})"
    );

    assert!(
        per_row <= max_per_row,
        "issue #2075 ({shape}): decode→RowCells row-assembly per-row allocations \
         regressed to {per_row:.2}/row (> {max_per_row} ceiling) for {qualified}. \
         The RowCells (Vec<(Arc<str>, Value)>) assembly path is allocating more per \
         row. This harness exists to measure #1645 item 2 (smallvec RowCells); a \
         regression here erases that headroom."
    );
    assert!(
        per_cell <= max_per_cell,
        "issue #2075 ({shape}): decode→RowCells row-assembly per-cell allocations \
         regressed to {per_cell:.2}/cell (> {max_per_cell} ceiling) for \
         {qualified}. Each materialized cell in RowCells (Vec<(Arc<str>, Value)>) \
         is allocating more. This harness exists to measure #1645 item 2 (smallvec \
         RowCells); a regression here erases that headroom."
    );
}

// A single `#[test]` covers BOTH shapes: dhat installs a process-wide global
// allocator and permits only ONE live `Profiler` at a time, so two separate tests
// in this binary would conflict (the second `Profiler::build` panics). One
// profiler, two measured scans. `#[serial]` keeps the process-global dhat profiler
// exclusive against any sibling dhat test. A plain `#[test]` (NOT `#[tokio::test]`)
// — `open_read_db` builds its own tokio runtime for ingest, and nesting a
// `block_on` inside `#[tokio::test]`'s runtime panics.
#[test]
#[serial_test::serial]
fn row_assembly_per_row_and_per_cell_allocations_within_budget() {
    let wide = ReadFixture::MANY_COLUMNS;
    let text = ReadFixture::DOCUMENT_VERSIONS;

    let wide_present = fixtures::fixture_present(&wide);
    let text_present = fixtures::fixture_present(&text);

    if !wide_present && !text_present {
        eprintln!(
            "Skipping issue #2075 row-assembly alloc budget: neither {} nor {} \
             present (run test-data/scripts/fetch-datasets.sh + set \
             CQLITE_DATASETS_ROOT).",
            wide.qualified(),
            text.qualified()
        );
        return;
    }

    // Open the fixtures BEFORE the profiler so ingest/open allocation is not
    // attributed to the row-assembly budget. Only a genuinely absent dataset is a
    // legitimate skip; a present fixture that fails to open must FAIL (open panics).
    let wide_db = wide_present.then(|| fixtures::open_read_db(&wide));
    let text_db = text_present.then(|| fixtures::open_read_db(&text));

    // One runtime drives every async `execute`.
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");

    // Start the single process-global profiler around the measured workload.
    let _profiler = dhat::Profiler::builder().testing().build();

    if let Some(db) = &wide_db {
        assert_shape(
            &rt,
            &db.db,
            &wide.qualified(),
            "WIDE-ROW",
            MAX_ALLOCS_PER_ROW_WIDE,
            MAX_ALLOCS_PER_CELL_WIDE,
        );
    } else {
        eprintln!(
            "issue #2075: skipping WIDE-ROW shape ({} absent); TEXT-HEAVY shape \
             below still ran.",
            wide.qualified()
        );
    }

    if let Some(db) = &text_db {
        assert_shape(
            &rt,
            &db.db,
            &text.qualified(),
            "TEXT-HEAVY",
            MAX_ALLOCS_PER_ROW_TEXT,
            MAX_ALLOCS_PER_CELL_TEXT,
        );
    } else {
        eprintln!(
            "issue #2075: skipping TEXT-HEAVY shape ({} absent); WIDE-ROW shape \
             above still ran.",
            text.qualified()
        );
    }
}
