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
//! It prints the harness JSON to stdout and appends one record to the history
//! ledger (`benches/tail-latency-history.jsonl`, gitignored). Under default
//! features (no `cli-helpers`) it prints a note and exits 0 without measuring.

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
                // Persist one record to the (gitignored) history ledger.
                let ledger = harness::default_ledger_path();
                if let Err(e) = harness::append_ledger(&ledger, &report) {
                    eprintln!(
                        "tail_latency: could not append ledger {}: {e}",
                        ledger.display()
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
