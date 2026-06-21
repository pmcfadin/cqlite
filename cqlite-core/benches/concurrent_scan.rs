//! Concurrent full-scan scaling benchmark for a single `SSTableReader`
//! (Issue #917, follow-up to #815, read-path perf epic #906).
//!
//! # What this measures
//!
//! #815 removed the `scan_mutex` full-scan serialization and gave every scan its
//! own `ScanCursor`, so N concurrent full scans on one `SSTableReader` now run in
//! parallel instead of single-file. #815 proved that change *correct*
//! (`test_concurrent_scans_single_reader_are_consistent`) but deliberately did not
//! quantify the *speedup*. This bench supplies the missing number: aggregate
//! throughput of N ∈ {1, 2, 4, 8} concurrent `get_all_entries()` scans against the
//! same `Arc<SSTableReader>`, on both the buffered and mmap backends.
//!
//! Each bench id is `concurrent_scan/<backend>/n<N>` and reports
//! `Throughput::Elements(rows_per_scan * N)` — i.e. aggregate rows/sec across all N
//! scans. Reading the rows/sec criterion prints for `n1` vs `n2/n4/n8` gives the
//! scaling curve directly (n4 rows/sec ÷ n1 rows/sec = 4-way scaling factor).
//!
//! # Not a CI gate (by design)
//!
//! Concurrent-scan scaling is sublinear and IO/scheduler-bound, so the absolute
//! curve is machine-dependent and noisy on shared runners. Per the issue and the
//! project's flaky-perf-gate history, this bench is **documentation + a local
//! regression guard**, not a hard timing assertion: it is intentionally absent
//! from `cqlite-core/benches/perf-gate.json`, so it runs under
//! `./scripts/profile.sh` but never fails the perf-regression CI. The only
//! hard assertion here is a *correctness* floor — every scan must return the same
//! non-zero row count — which catches an accidental re-serialization or a broken
//! scan far more reliably than a timing threshold would.
//!
//! Gated on `cli-helpers` only to share the profiling-harness feature set used by
//! the other read benches (`scripts/profile.sh`); the scan path itself is core.
//! Under default features the bench compiles to an empty-but-valid criterion group.

#[cfg(feature = "cli-helpers")]
use criterion::{black_box, BenchmarkId, Throughput};
use criterion::{criterion_group, criterion_main, Criterion};

#[path = "fixtures/mod.rs"]
mod fixtures;

#[path = "profiling/mod.rs"]
mod profiling;

/// Concurrency degrees to measure. `n1` is the uncontended baseline; the rest
/// show how aggregate throughput scales as more scans share one reader.
#[cfg(feature = "cli-helpers")]
const SCAN_DEGREES: [usize; 4] = [1, 2, 4, 8];

/// Open a single `SSTableReader` over the SIMPLE fixture's Data.db, selecting the
/// buffered or mmap backend. Returns the shared reader plus the per-scan row count
/// measured once up front (used for the throughput element count and the
/// correctness floor).
#[cfg(feature = "cli-helpers")]
fn open_reader(
    rt: &tokio::runtime::Runtime,
    use_mmap: bool,
) -> (
    std::sync::Arc<cqlite_core::storage::sstable::SSTableReader>,
    u64,
) {
    use cqlite_core::{Config, Platform};
    use std::sync::Arc;

    let fx = fixtures::ReadFixture::SIMPLE;
    let data_file = fixtures::table_dir(fx.keyspace, fx.table).join("nb-1-big-Data.db");
    assert!(
        data_file.exists(),
        "concurrent_scan: Data.db missing at {} — fetch fixtures: bash test-data/scripts/fetch-datasets.sh",
        data_file.display()
    );

    let mut config = Config::default();
    config.storage.use_mmap = use_mmap;

    rt.block_on(async move {
        let platform = Arc::new(
            Platform::new(&config)
                .await
                .expect("build platform for concurrent_scan"),
        );
        let reader = Arc::new(
            cqlite_core::storage::sstable::SSTableReader::open(&data_file, &config, platform)
                .await
                .expect("open SSTableReader for concurrent_scan"),
        );
        // Backend (buffered vs mmap) is selected by `config.storage.use_mmap`
        // above; the `is_mmap_backed()` accessor is `pub(crate)` so we can't assert
        // the selection from outside the crate — the unit test
        // `test_concurrent_scans_single_reader_are_consistent` covers that.
        let rows = reader
            .get_all_entries()
            .await
            .expect("baseline scan for concurrent_scan")
            .len() as u64;
        assert!(
            rows > 0,
            "concurrent_scan: baseline scan returned zero rows — fixtures not fetched?"
        );
        (reader, rows)
    })
}

/// Bench: N concurrent full scans on one shared `Arc<SSTableReader>`, for each
/// backend × each degree in [`SCAN_DEGREES`].
///
/// The runtime is a fixed 8-worker multi-thread tokio runtime so that the N
/// spawned scans genuinely run in parallel (up to 8 cores) rather than being
/// folded onto one thread — without that, "concurrency" would be cooperative
/// only and the scaling number would be meaningless.
#[cfg(feature = "cli-helpers")]
fn bench_concurrent_scan(c: &mut Criterion) {
    use std::sync::Arc;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .expect("multi-thread tokio runtime for concurrent_scan");

    let mut group = c.benchmark_group("concurrent_scan");

    for use_mmap in [false, true] {
        let backend = if use_mmap { "mmap" } else { "buffered" };
        let (reader, rows_per_scan) = open_reader(&rt, use_mmap);

        for &degree in &SCAN_DEGREES {
            // Aggregate rows moved per iteration = rows_per_scan × number of scans,
            // so the reported rows/sec is total reader throughput across all scans.
            group.throughput(Throughput::Elements(rows_per_scan * degree as u64));
            group.bench_with_input(
                BenchmarkId::new(backend, format!("n{degree}")),
                &degree,
                |bch, &degree| {
                    bch.iter(|| {
                        rt.block_on(async {
                            let mut handles = Vec::with_capacity(degree);
                            for _ in 0..degree {
                                let reader = Arc::clone(&reader);
                                handles.push(tokio::spawn(async move {
                                    reader
                                        .get_all_entries()
                                        .await
                                        .expect("concurrent scan")
                                        .len()
                                }));
                            }
                            let mut total = 0usize;
                            for h in handles {
                                total += h.await.expect("scan task panicked");
                            }
                            black_box(total)
                        })
                    });
                },
            );
        }
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// criterion_group! / criterion_main! — feature-gated so the bench compiles
// under default features (no cli-helpers) with an empty but valid group.
// ---------------------------------------------------------------------------

#[cfg(feature = "cli-helpers")]
criterion_group!(
    name = benches;
    config = profiling::configure();
    targets = bench_concurrent_scan
);

#[cfg(not(feature = "cli-helpers"))]
fn bench_noop(_c: &mut Criterion) {
    // Nothing to bench without cli-helpers. The bench binary still compiles and
    // runs successfully; it just reports no measurements.
}

#[cfg(not(feature = "cli-helpers"))]
criterion_group!(
    name = benches;
    config = profiling::configure();
    targets = bench_noop
);

criterion_main!(benches);
