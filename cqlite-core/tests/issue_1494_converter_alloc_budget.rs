//! CQL→Arrow converter allocation-budget guard (issue #1494, AD5; dhat-gated).
//!
//! The export/Flight data plane funnels every exported cell through
//! `cqlite_core::export::arrow_convert::rows_to_record_batch`. This guard pins
//! today's (post-#1495, PR #2312) per-row allocation COUNT for that converter as
//! a load-deterministic regression net — the hard, machine-independent signal the
//! spec designates for the export path (a wall-clock throughput number flakes
//! under load; an allocation count does not). AE1–AE5 tighten the bound; this
//! change lands it PASSING as a baseline lock ("do not regress below today").
//!
//! **Reuses the epic-H machinery** (the `#[global_allocator] dhat::Alloc` +
//! `HeapStats::total_blocks` delta pattern of
//! `tests/test_issue_1046_scan_alloc_scaling.rs` / `tests/memory_budget.rs`) —
//! it does not duplicate it.
//!
//! **Non-vacuous by construction**: it asserts the fixture yields ≥ 1 row AND
//! that the converter observed > 0 allocations before checking the bound, so a
//! run that measured nothing (empty dataset, converter no-op'd) FAILS rather than
//! passing at "0 ≤ budget". A genuinely absent dataset skip-registers.
//!
//! Run via (the `memory-budget` gate component runs this):
//! ```text
//! env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!   cargo test --package cqlite-core --features cli-helpers,dhat-heap,arrow \
//!   --test issue_1494_converter_alloc_budget -- --test-threads=1
//! ```

#![cfg(all(feature = "dhat-heap", feature = "cli-helpers", feature = "arrow"))]

use cqlite_core::export::rows_to_record_batch;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[path = "../benches/fixtures/mod.rs"]
mod fixtures;

/// Per-row allocation ceiling for converting a TYPE_HEAVY scan result
/// (`test_collections.collection_table`, 500 rows × 7 columns) to an Arrow
/// `RecordBatch`. Measured on post-#1495 (PR #2312) `main` at **~0.91 allocs/row**
/// (453 allocations / 500 rows; the accessor-once win landed the per-cell lookup
/// at zero — see `benches/README.md`). dhat allocation counts are deterministic
/// and machine-independent, so the ceiling can sit tight at 3.0/row: ~3.3× the
/// measured figure (small legitimate decode churn absorbed) while any per-cell
/// lookup/clone reintroduction — which would add ~1 alloc per column per row
/// (~7/row here) — fails closed. AE1–AE5 ratchet this DOWN; tighten when they land.
const MAX_ALLOCS_PER_ROW_TYPE_HEAVY: f64 = 3.0;

/// Measure the dhat `total_blocks` delta across ONE `rows_to_record_batch`
/// conversion of the given columns/rows (a warmed second call excludes one-time
/// interning/setup churn), returning `(rows, allocations)`.
fn converter_alloc_count(
    columns: &[cqlite_core::query::ColumnInfo],
    rows: &[cqlite_core::query::QueryRow],
) -> (usize, u64) {
    // Warm once (drops any first-touch lazy statics into the pre-measurement
    // baseline), then measure the steady-state conversion.
    let _ = rows_to_record_batch(columns, rows).expect("warm conversion");
    let before = dhat::HeapStats::get().total_blocks;
    let batch = rows_to_record_batch(columns, rows).expect("conversion must succeed");
    let after = dhat::HeapStats::get().total_blocks;
    assert_eq!(
        batch.num_rows(),
        rows.len(),
        "converter dropped rows: {} in, {} out",
        rows.len(),
        batch.num_rows()
    );
    (rows.len(), after.saturating_sub(before))
}

// NOTE: a plain `#[test]` (NOT `#[tokio::test]`). `open_read_db` builds its own
// tokio runtime for the one-shot ingest, and the SELECT below uses a separate
// runtime — nesting a `block_on` inside `#[tokio::test]`'s runtime panics
// ("Cannot start a runtime from within a runtime"). `#[serial]` keeps the
// process-global dhat profiler exclusive if a sibling dhat test is ever added.
#[test]
#[serial_test::serial]
fn converter_per_row_allocations_within_budget() {
    let fx = fixtures::ReadFixture::TYPE_HEAVY;
    if !fixtures::fixture_present(&fx) {
        eprintln!(
            "Skipping issue #1494 converter alloc budget: {} absent \
             (run fetch-datasets.sh + set CQLITE_DATASETS_ROOT)",
            fx.qualified()
        );
        return;
    }

    // Fail-closed once fixtures are present: any setup/scan failure must FAIL,
    // never let the budget pass vacuously. Open + scan BEFORE the profiler so
    // ingest/open allocation is not attributed to the converter budget.
    let loaded = fixtures::open_read_db(&fx);
    let sql = format!("SELECT * FROM {}", fx.qualified());
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let result = rt
        .block_on(loaded.db.execute(&sql))
        .expect("fixture scan must succeed when Data.db is present");

    // Start the profiler only around the conversion workload.
    let _profiler = dhat::Profiler::builder().testing().build();

    assert!(
        !result.rows.is_empty(),
        "issue #1494: fixture {} returned 0 rows — a present fixture that scans \
         empty is a setup failure, not a passing 0-alloc budget",
        fx.qualified()
    );

    let (rows, allocs) = converter_alloc_count(&result.metadata.columns, &result.rows);

    // Non-vacuity: the converter must have allocated something. A "0 allocs"
    // reading means the conversion did not run / was optimized away — that is a
    // measurement failure, never a passing "0 ≤ budget".
    assert!(
        allocs > 0,
        "issue #1494: converter observed 0 allocations over {rows} rows — the \
         conversion did not execute; refusing a vacuous pass"
    );

    let per_row = allocs as f64 / rows as f64;
    eprintln!(
        "issue #1494 converter alloc budget: {} rows ({} columns) -> {allocs} allocs, \
         {per_row:.2} allocs/row (ceiling {MAX_ALLOCS_PER_ROW_TYPE_HEAVY})",
        rows,
        result.metadata.columns.len()
    );

    assert!(
        per_row <= MAX_ALLOCS_PER_ROW_TYPE_HEAVY,
        "issue #1494: CQL→Arrow converter per-row allocations regressed to \
         {per_row:.2}/row (> {MAX_ALLOCS_PER_ROW_TYPE_HEAVY} ceiling). The export \
         data plane (rows_to_record_batch / arrow_columnar) is allocating more per \
         cell — a per-cell lookup/clone was likely reintroduced (see issue #1495 \
         accessor-once). AE1–AE5 own tightening this bound."
    );
}
