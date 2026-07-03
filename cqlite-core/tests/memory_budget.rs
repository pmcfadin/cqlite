//! dhat allocation / peak-heap budget lane for the read path (issue #1565, Epic A / A4).
//!
//! This target compiles ONLY under the opt-in `dhat-heap` feature (see the
//! file-wide `#![cfg]` below), so the default `core-tests` run never installs
//! the dhat global allocator or executes these budgets. It is additive
//! measurement machinery: it drives the real public query path
//! (`cqlite_core::Database::execute`) over the vendored real SSTable fixtures
//! and pins today's measured allocation totals / peak heap as regression nets
//! (ratchet targets for Epic E to tighten).
//!
//! Run it:
//!
//! ```text
//! cargo test --package cqlite-core --features cli-helpers,dhat-heap \
//!     --test memory_budget -- --test-threads=1
//! ```
//!
//! `dhat::Profiler` is a process-global singleton (two live profilers panic),
//! so every test builds/drops its own profiler AND is `#[serial_test::serial]`;
//! the gate additionally passes `--test-threads=1`.
//!
//! Requires BOTH `dhat-heap` (the global allocator + `HeapStats`) and
//! `cli-helpers` (the `benches/fixtures/mod.rs` real-SSTable loader below uses
//! `open_read_db`, which is itself `#[cfg(feature = "cli-helpers")]`). Gating on
//! both means `--features dhat-heap` alone compiles an empty target instead of
//! failing to build (roborev #1565); the gate always passes `cli-helpers,dhat-heap`.
#![cfg(all(feature = "dhat-heap", feature = "cli-helpers"))]

// The dhat allocator must be the global allocator to observe every allocation.
// It is confined to this one test binary — default builds/tests never link it.
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[path = "../benches/fixtures/mod.rs"]
mod fixtures;

/// The CLAUDE.md project memory budget: <128 MiB for large files.
const PROJECT_HEAP_BUDGET_BYTES: u64 = 128 * 1024 * 1024;

/// Fixed repeat count for the full-scan allocation workload. SIMPLE has 999
/// rows; repeating the scan 10x inside the profiled region gives ~10k-row
/// allocation volume (the issue's "10k-row intent") deterministically, using
/// real data rather than a synthetic 10k-row table.
const FULL_SCAN_ITERATIONS: usize = 10;

/// Full-scan total-bytes regression net for the SELECT loop ONLY (fixture copy +
/// ingest are excluded — the profiler starts after `open_read_db`). Measured
/// 207,016,624 bytes on `main` today (SIMPLE, 10 iterations); ceiling = measured
/// + ~22% slack for allocator/toolchain variance. Epic E (E2/E3) ratchets this
/// DOWN toward the streaming target as the read path stops materializing whole
/// result sets — tighten this constant when E2/E3 land, do not just leave slack.
const CEILING_TOTAL_BYTES: u64 = 252_000_000;

/// Materializing-read peak-heap regression net for the query's working set ONLY
/// (fixture copy + ingest excluded — profiler starts after `open_read_db`).
/// Measured 4,579,884 bytes on `main` today (TYPE_HEAVY); ceiling = measured +
/// ~31% slack (peak varies more than totals). Epic E ratchets this DOWN as peak
/// working-set shrinks; it also sits far below the 128 MiB project budget
/// asserted alongside it.
const CEILING_PEAK_BYTES: u64 = 6_000_000;

/// Total bytes allocated during a repeated full-table `SELECT *` over the
/// largest simple real fixture must stay within the pinned ceiling.
#[test]
#[serial_test::serial]
fn select_full_scan_alloc_budget() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let fx = fixtures::ReadFixture::SIMPLE;
    let sql = format!("SELECT * FROM {}", fx.qualified());

    // Open the fixture (copy + schema ingest + db open) BEFORE starting the
    // profiler so those one-time setup allocations are NOT attributed to the
    // budget — this must be a *read-path* regression net, not a fixture/ingest
    // one (roborev #1565). Only the repeated SELECT loop below is measured.
    let loaded = fixtures::open_read_db(&fx);

    let _profiler = dhat::Profiler::builder().testing().build();

    for i in 0..FULL_SCAN_ITERATIONS {
        let result = rt
            .block_on(loaded.db.execute(&sql))
            .expect("full-scan query");
        if i == 0 {
            assert!(
                !result.rows.is_empty(),
                "zero rows from {} — fixtures not fetched? \
                 Run: bash test-data/scripts/fetch-datasets.sh",
                fx.qualified()
            );
        }
    }

    let stats = dhat::HeapStats::get();
    println!(
        "select_full_scan_alloc_budget: total_bytes={} total_blocks={} max_bytes={} max_blocks={} (iters={})",
        stats.total_bytes, stats.total_blocks, stats.max_bytes, stats.max_blocks, FULL_SCAN_ITERATIONS
    );

    assert!(
        stats.total_bytes <= CEILING_TOTAL_BYTES,
        "full-scan total bytes {} exceeded ceiling {}",
        stats.total_bytes,
        CEILING_TOTAL_BYTES
    );
}

/// Peak heap bytes for a materializing type-heavy `SELECT *` must stay within
/// the pinned ceiling AND under the project's 128 MiB budget.
#[test]
#[serial_test::serial]
fn materialized_select_byte_ceiling() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let fx = fixtures::ReadFixture::TYPE_HEAVY;
    let sql = format!("SELECT * FROM {}", fx.qualified());

    // Open the fixture BEFORE the profiler so the measured peak reflects only the
    // query's working set, not one-time fixture-copy/ingest allocations
    // (roborev #1565).
    let loaded = fixtures::open_read_db(&fx);

    let _profiler = dhat::Profiler::builder().testing().build();

    let result = rt
        .block_on(loaded.db.execute(&sql))
        .expect("type-heavy query");
    assert!(
        !result.rows.is_empty(),
        "zero rows from {} — fixtures not fetched? \
         Run: bash test-data/scripts/fetch-datasets.sh",
        fx.qualified()
    );

    let stats = dhat::HeapStats::get();
    println!(
        "materialized_select_byte_ceiling: total_bytes={} total_blocks={} max_bytes={} max_blocks={}",
        stats.total_bytes, stats.total_blocks, stats.max_bytes, stats.max_blocks
    );

    assert!(
        stats.max_bytes as u64 <= CEILING_PEAK_BYTES,
        "peak heap {} exceeded pinned ceiling {}",
        stats.max_bytes,
        CEILING_PEAK_BYTES
    );
    assert!(
        stats.max_bytes as u64 <= PROJECT_HEAP_BUDGET_BYTES,
        "peak heap {} exceeded 128 MiB project budget",
        stats.max_bytes
    );
}
