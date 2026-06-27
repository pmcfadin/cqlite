//! Read-while-write tail-latency guard bench (Issue #1143).
//!
//! # What this measures
//!
//! Issue #1143 is a read-path **p99 regression under concurrent write load**: with
//! ~6 concurrent full-scan readers running alongside ~2 sustained writers, the
//! reader-side tail (p99) latency blows up even though the isolated mean scan
//! latency is fine. The root cause is allocator/bandwidth contention — the
//! pre-fix scan path stitched the ENTIRE Data.db into one growing `Vec<u8>` per
//! scan, so N concurrent multi-MB buffers thrash the global allocator alongside
//! the writers' allocation.
//!
//! `concurrent_scan` (the sibling bench) measures *aggregate read throughput* with
//! NO writers and reports a *mean*; it is structurally blind to this regression.
//! This bench closes that blind spot: it runs `READERS` full-scan reader tasks
//! concurrently with `WRITERS` sustained-ingest writer tasks and reports the
//! **reader-side p99** scan latency — a tail statistic, which is the metric that
//! actually moved in #1143.
//!
//! The corpus the readers scan is STATIC (the vendored SIMPLE fixture copied into
//! a temp dir); the writers ingest into a SEPARATE temp `WriteEngine`, so the
//! readers and writers contend only on the process-global allocator / memory
//! bandwidth / scheduler — exactly the contention surface #1143 is about — and
//! never on the same files.
//!
//! # Not a CI gate (by design)
//!
//! Tail (p99) statistics over a fixed sample are runner-noisy: p99 is dominated by
//! the worst few samples, which on a shared CI runner are scheduler/allocator
//! outliers unrelated to the code under test. So, exactly like `concurrent_scan`,
//! this bench is **advisory** — it is deliberately ABSENT from
//! `cqlite-core/benches/perf-gate.json`, so it runs under `./scripts/profile.sh`
//! and as a local regression guard but never fails the perf-regression CI. The
//! only hard assertions here are *correctness* floors (every scan returns the
//! same non-zero row count; writers actually ingested), which catch a broken scan
//! or a wedged writer far more reliably than a timing threshold would.
//!
//! Gated on `cli-helpers` + `write-support` (the read fixture loader needs the
//! former, the `WriteEngine` the latter). Under default features the bench
//! compiles to an empty-but-valid criterion group.

use criterion::{criterion_group, criterion_main, Criterion};

#[path = "fixtures/mod.rs"]
mod fixtures;

#[path = "profiling/mod.rs"]
mod profiling;

/// Number of concurrent full-scan reader tasks. Mirrors the failing #1143
/// workload (~6 readers).
#[cfg(all(feature = "cli-helpers", feature = "write-support"))]
const READERS: usize = 6;

/// Number of concurrent sustained-ingest writer tasks. Mirrors the failing #1143
/// workload (~2 writers).
#[cfg(all(feature = "cli-helpers", feature = "write-support"))]
const WRITERS: usize = 2;

/// Full scans each reader task performs per measured iteration. The reported p99
/// is computed across all `READERS * SCANS_PER_READER` scan latencies of one
/// iteration, so this also sets the tail sample size.
#[cfg(all(feature = "cli-helpers", feature = "write-support"))]
const SCANS_PER_READER: usize = 8;

#[cfg(all(feature = "cli-helpers", feature = "write-support"))]
fn bench_read_while_write(c: &mut Criterion) {
    use criterion::black_box;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    let fx = fixtures::ReadFixture::SIMPLE;
    let loaded = Arc::new(fixtures::open_read_db(&fx));
    let sql = Arc::new(format!("SELECT * FROM {}", fx.qualified()));

    // Multi-thread runtime so readers + writers genuinely run in parallel (this
    // is a contention bench — a single-threaded runtime would serialise away the
    // very contention we want to measure).
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads((READERS + WRITERS + 1).max(8))
        .enable_all()
        .build()
        .expect("multi-thread tokio runtime for read_while_write");

    // Correctness floor: one isolated scan must return a non-zero, fixed row
    // count. Captured once up front, asserted against every scan below.
    let expected_rows = rt.block_on(async {
        let res = loaded
            .db
            .execute(&sql)
            .await
            .expect("read_while_write baseline scan");
        let n = res.rows.len();
        assert!(
            n > 0,
            "read_while_write: baseline scan returned zero rows — fixtures not fetched? \
             (bash test-data/scripts/fetch-datasets.sh)"
        );
        n
    });

    let mut group = c.benchmark_group("read_while_write");
    // Tail latencies are not comparable across machines and the writer
    // backpressure makes a wall-clock-per-element throughput meaningless, so we
    // report raw per-iteration time and read the p99 from the printed line below.
    group.sample_size(10);

    group.bench_function(format!("readers{READERS}_writers{WRITERS}"), |bch| {
        bch.iter_custom(|iters| {
            rt.block_on(async {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    // Writers run for the whole reader window; `stop` halts
                    // them once the readers finish so a slow writer cannot
                    // pin the iteration open.
                    let stop = Arc::new(AtomicBool::new(false));

                    // Spawn sustained-ingest writers, each on its own engine
                    // + temp dir (WAL off → pure CPU/memtable allocation, the
                    // contention surface, no fsync I/O noise).
                    let mut writer_handles = Vec::with_capacity(WRITERS);
                    for _ in 0..WRITERS {
                        let stop = Arc::clone(&stop);
                        writer_handles.push(tokio::task::spawn_blocking(move || {
                            use rand::Rng;
                            let tmp = tempfile::TempDir::new()
                                .expect("temp dir for read_while_write writer");
                            let mut engine =
                                fixtures::open_write_engine_wal_off(tmp.path(), usize::MAX);
                            let mut rng = fixtures::seeded_rng();
                            let mut written = 0u64;
                            while !stop.load(Ordering::Relaxed) {
                                let id = uuid::Uuid::from_u128(rng.gen());
                                let age: i32 = rng.gen_range(0..100);
                                let stmt = format!(
                                    "INSERT INTO test_basic.simple_table \
                                         (id, name, age, active) \
                                         VALUES ({id}, 'rww-row', {age}, true)"
                                );
                                engine.execute(&stmt).expect("read_while_write ingest");
                                written += 1;
                            }
                            written
                        }));
                    }

                    // Spawn the readers; each loops `SCANS_PER_READER` full
                    // scans, recording per-scan latency.
                    let mut reader_handles = Vec::with_capacity(READERS);
                    for _ in 0..READERS {
                        let loaded = Arc::clone(&loaded);
                        let sql = Arc::clone(&sql);
                        reader_handles.push(tokio::spawn(async move {
                            let mut samples = Vec::with_capacity(SCANS_PER_READER);
                            for _ in 0..SCANS_PER_READER {
                                let t0 = Instant::now();
                                let res = loaded
                                    .db
                                    .execute(&sql)
                                    .await
                                    .expect("read_while_write scan");
                                samples.push((t0.elapsed(), res.rows.len()));
                            }
                            samples
                        }));
                    }

                    // Join readers, collect every scan latency.
                    let mut latencies: Vec<Duration> =
                        Vec::with_capacity(READERS * SCANS_PER_READER);
                    for h in reader_handles {
                        let samples = h.await.expect("reader task panicked");
                        for (dur, rows) in samples {
                            assert_eq!(
                                rows, expected_rows,
                                "read_while_write: scan row count drifted under write load \
                                     (got {rows}, expected {expected_rows}) — a broken scan"
                            );
                            latencies.push(dur);
                        }
                    }

                    // Readers done: stop writers and confirm they ingested.
                    stop.store(true, Ordering::Relaxed);
                    let mut total_written = 0u64;
                    for h in writer_handles {
                        total_written += h.await.expect("writer task panicked");
                    }
                    assert!(
                        total_written > 0,
                        "read_while_write: writers ingested nothing — wedged writer path"
                    );

                    // Report the reader-side p99 of THIS iteration. Criterion
                    // measures the per-iteration wall time (the sum over the
                    // window), but the tail line we print is the signal #1143
                    // cares about; it surfaces in the bench stdout for the
                    // local regression guard.
                    let p99 = percentile(&mut latencies, 99.0);
                    let p50 = percentile(&mut latencies, 50.0);
                    eprintln!(
                        "read_while_write: readers={READERS} writers={WRITERS} \
                             scans={} writes={total_written} p50={p50:?} p99={p99:?}",
                        latencies.len()
                    );

                    total += latencies.iter().sum::<Duration>();
                    black_box(p99);
                }
                total
            })
        });
    });

    group.finish();
}

/// Nearest-rank percentile of a latency sample. Sorts `samples` in place and
/// returns the `pct`-th percentile duration. Returns `Duration::ZERO` for an
/// empty sample (cannot happen given the correctness floor, but keeps the helper
/// total).
#[cfg(all(feature = "cli-helpers", feature = "write-support"))]
fn percentile(samples: &mut [std::time::Duration], pct: f64) -> std::time::Duration {
    if samples.is_empty() {
        return std::time::Duration::ZERO;
    }
    samples.sort_unstable();
    // Nearest-rank: rank = ceil(pct/100 * N), clamped to [1, N], 1-indexed.
    let n = samples.len() as f64;
    let rank = ((pct / 100.0) * n).ceil().max(1.0) as usize;
    let idx = rank.min(samples.len()) - 1;
    samples[idx]
}

// ---------------------------------------------------------------------------
// criterion_group! / criterion_main! — feature-gated so the bench compiles
// under default features (no cli-helpers / write-support) with an empty group.
// ---------------------------------------------------------------------------

#[cfg(all(feature = "cli-helpers", feature = "write-support"))]
criterion_group!(
    name = benches;
    config = profiling::configure();
    targets = bench_read_while_write
);

#[cfg(not(all(feature = "cli-helpers", feature = "write-support")))]
fn bench_noop(_c: &mut Criterion) {
    // Nothing to bench without cli-helpers + write-support. The bench binary
    // still compiles and runs successfully; it just reports no measurements.
}

#[cfg(not(all(feature = "cli-helpers", feature = "write-support")))]
criterion_group!(
    name = benches;
    config = profiling::configure();
    targets = bench_noop
);

criterion_main!(benches);
