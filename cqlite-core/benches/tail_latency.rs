//! Mixed-load tail-latency harness bench (Issue #1563, Epic A / A2).
//!
//! A `harness = false` custom-main bench (not Criterion): Criterion reports a
//! single median per bench, but this harness must emit **percentiles**
//! (p50/p99/p999) and derived **intra-run ratios**, which Criterion's model does
//! not express. The measurement core lives in the shared `tail_latency/mod.rs`
//! module so the `tail_latency_harness` integration test can exercise the same
//! code (see that module's docs for what it measures and why).
//!
//! Run it (needs the read fixture loader):
//! ```bash
//! env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!   cargo bench -p cqlite-core --features cli-helpers --bench tail_latency
//! ```
//! It prints the harness JSON to stdout and appends one record PER metric to the
//! unified history ledger (`target/profiling/history.jsonl`, gitignored) via the
//! shared `bench_ledger` module (Issue #1566, Epic A / A5). Under default features
//! (no `cli-helpers`) it prints a note and exits 0 without measuring.

#[path = "bench_ledger/mod.rs"]
mod bench_ledger;

#[path = "fixtures/mod.rs"]
mod fixtures;

#[path = "tail_latency/mod.rs"]
mod harness;

fn main() {
    #[cfg(feature = "cli-helpers")]
    {
        match harness::run(fixtures::ReadFixture::SIMPLE) {
            Some(report) => {
                // Machine-readable JSON to stdout.
                println!("{}", report.to_json());
                // Persist one record PER metric to the unified (gitignored) ledger.
                // Best-effort: a ledger write must never fail a measurement run.
                if let Err(e) =
                    bench_ledger::append_metrics("tail_latency", &report.ledger_metrics())
                {
                    eprintln!(
                        "tail_latency: could not append unified ledger {}: {e}",
                        bench_ledger::ledger_path().display()
                    );
                }
            }
            None => {
                eprintln!(
                    "tail_latency: fixture absent — no measurement. \
                     Fetch datasets: bash test-data/scripts/fetch-datasets.sh"
                );
            }
        }
    }

    #[cfg(not(feature = "cli-helpers"))]
    {
        eprintln!(
            "tail_latency: requires --features cli-helpers; nothing to measure. \
             Run: cargo bench -p cqlite-core --features cli-helpers --bench tail_latency"
        );
    }
}
