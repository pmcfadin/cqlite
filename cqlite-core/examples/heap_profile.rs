//! Heap-profiling harness for the read path (docs/profiling.md).
//!
//! Runs the same fixture workloads as the `read` criterion bench (full scan of
//! `test_basic.simple_table`, type-heavy scan of
//! `test_collections.collection_table`) under the [dhat] heap profiler and
//! reports allocation totals plus peak heap against the project's <128 MiB
//! memory budget (CLAUDE.md "Memory target").
//!
//! Two artifacts are written to `target/profiling/`:
//!
//! - `dhat-heap.json` — full allocation profile. Open it in the dhat viewer:
//!   <https://nnethercote.github.io/dh_view/dh_view.html> to see allocation
//!   hot spots by call stack (which code allocates the most / churns the most).
//! - `heap-summary.json` — compact machine-readable summary consumed by
//!   `scripts/profile_report.py` for the recursive-improvement report.
//!
//! Run via the orchestrator (preferred):
//!
//! ```text
//! ./scripts/profile.sh heap
//! ```
//!
//! or directly:
//!
//! ```text
//! cargo run --package cqlite-core --example heap_profile \
//!     --features cli-helpers,dhat-heap --profile bench
//! ```
//!
//! Without `dhat-heap` the workload still runs (useful as a smoke test) but no
//! allocation data is collected. The `bench` profile matters: the workspace
//! `release` profile strips symbols, which destroys dhat's backtraces.

// The dhat allocator must be the global allocator to observe every allocation.
// Only installed under the opt-in `dhat-heap` feature so normal builds and
// other examples are unaffected.
#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[cfg(feature = "cli-helpers")]
#[path = "../benches/fixtures/mod.rs"]
mod fixtures;

#[cfg(not(feature = "cli-helpers"))]
fn main() {
    eprintln!(
        "heap_profile requires the cli-helpers feature:\n  \
         cargo run -p cqlite-core --example heap_profile \
         --features cli-helpers,dhat-heap --profile bench"
    );
    std::process::exit(2);
}

#[cfg(feature = "cli-helpers")]
fn main() {
    use std::path::PathBuf;

    /// Memory budget from CLAUDE.md: <128 MiB for large files.
    const HEAP_BUDGET_BYTES: u64 = 128 * 1024 * 1024;

    let out_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/profiling");
    std::fs::create_dir_all(&out_dir).expect("create target/profiling");

    // Start the profiler before any workload allocation so setup cost (db
    // open, schema parse) is attributed too — those paths count against the
    // memory budget exactly like steady-state reads.
    #[cfg(feature = "dhat-heap")]
    let profiler = dhat::Profiler::builder()
        .file_name(out_dir.join("dhat-heap.json"))
        .build();

    let workloads = [
        ("read/full_scan", fixtures::ReadFixture::SIMPLE),
        ("read/type_heavy", fixtures::ReadFixture::TYPE_HEAVY),
    ];

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    for (name, fx) in &workloads {
        let loaded = fixtures::open_read_db(fx);
        let sql = format!("SELECT * FROM {}", fx.qualified());
        let result = rt
            .block_on(loaded.db.execute(&sql))
            .unwrap_or_else(|e| panic!("{name}: query failed: {e}"));
        assert!(
            !result.rows.is_empty(),
            "{name}: zero rows from {} — fixtures not fetched? \
             Run: bash test-data/scripts/fetch-datasets.sh",
            fx.qualified()
        );
        println!("{name}: scanned {} rows", result.rows.len());
    }

    #[cfg(feature = "dhat-heap")]
    {
        let stats = dhat::HeapStats::get();
        let within_budget = (stats.max_bytes as u64) <= HEAP_BUDGET_BYTES;

        // Compact summary for scripts/profile_report.py. serde_json is a
        // regular cqlite-core dependency, so it is available to examples.
        let summary = serde_json::json!({
            "workloads": workloads.iter().map(|(n, _)| n).collect::<Vec<_>>(),
            "total_allocations": stats.total_blocks,
            "total_bytes_allocated": stats.total_bytes,
            "peak_blocks": stats.max_blocks,
            "peak_bytes": stats.max_bytes,
            "heap_budget_bytes": HEAP_BUDGET_BYTES,
            "within_budget": within_budget,
        });
        let summary_path = out_dir.join("heap-summary.json");
        std::fs::write(
            &summary_path,
            serde_json::to_string_pretty(&summary).expect("serialize heap summary"),
        )
        .expect("write heap-summary.json");

        println!();
        println!("heap profile (dhat):");
        println!("  total allocations : {}", stats.total_blocks);
        println!("  total bytes       : {}", stats.total_bytes);
        println!(
            "  peak heap         : {} bytes ({:.1} MiB)",
            stats.max_bytes,
            stats.max_bytes as f64 / (1024.0 * 1024.0)
        );
        println!(
            "  budget (<128 MiB) : {}",
            if within_budget { "PASS" } else { "FAIL" }
        );
        println!("  summary           : {}", summary_path.display());
        println!(
            "  full profile      : {} (view at https://nnethercote.github.io/dh_view/dh_view.html)",
            out_dir.join("dhat-heap.json").display()
        );

        // Drop writes dhat-heap.json; do it before the budget verdict exits.
        drop(profiler);

        if !within_budget {
            std::process::exit(1);
        }
    }

    #[cfg(not(feature = "dhat-heap"))]
    println!(
        "\nworkloads completed (no allocation data: rebuild with --features dhat-heap \
         to collect a heap profile)"
    );
}
