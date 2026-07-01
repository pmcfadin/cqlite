//! Observability zero-overhead-when-disabled benchmark (epic #1031, issue #1043).
//!
//! # What this proves
//!
//! The observability contract (see `crate::observability`) is that
//! **instrumentation call sites are identical whether or not the `observability`
//! feature is enabled, and when the feature is off — OR on but export is
//! disabled — the cost is negligible.** This bench measures that empirically.
//!
//! It runs a representative read/scan flow (and a write/merge flow when
//! `write-support` is on) that already carries the production `#[tracing::instrument]`
//! spans and catalog metric calls. The SAME bench source runs under two builds:
//!
//! 1. the DEFAULT build (`observability` OFF — every helper is a compile-time no-op);
//! 2. `--features observability` WITH EXPORT DISABLED — the OTel crates are linked
//!    and the helper bodies execute, but `init` is never called, so there is no
//!    global meter/tracer provider and no exporter. Metric helpers fall through the
//!    global no-op meter and the `tracing` spans have no OTel layer attached.
//!
//! Because a single `cargo bench` process compiles exactly one feature set, the
//! two builds cannot run in one process. The comparison is therefore performed
//! across two invocations by `scripts/ci/observability_overhead.sh`, which runs
//! this bench under both builds and asserts the median deltas stay within
//! [`OVERHEAD_THRESHOLD_PCT`]. This file's only job is to be a stable, identical
//! workload in both builds; it never reads the threshold itself.
//!
//! # Threshold
//!
//! [`OVERHEAD_THRESHOLD_PCT`] = 2.0%. The "disabled" arm should be within ~2% of
//! the default arm. The bench uses a fixed deterministic workload and the
//! comparison script re-measures both arms on the SAME runner (like the existing
//! perf-regression gate), so the delta is immune to cross-machine variance. The
//! threshold is deliberately generous relative to the true cost (the helpers are
//! `#[inline]` and branch on a `const` feature predicate) to keep the CI signal
//! non-flaky on shared runners.
//!
//! Bench group/IDs (consumed by the comparison script):
//! - `observability_overhead/read_scan`
//! - `observability_overhead/write_merge`  (only when `write-support` is enabled)

use criterion::{criterion_group, criterion_main, Criterion};

#[path = "fixtures/mod.rs"]
mod fixtures;

#[path = "profiling/mod.rs"]
mod profiling;

/// Maximum tolerated median overhead of the export-disabled `observability`
/// build over the default build, in percent. Documented here as the single
/// source of truth; the comparison script reads this exact value.
///
/// Kept as a `pub const` (not just a doc number) so a future Rust-side check or
/// test can reference it without drifting from the script.
pub const OVERHEAD_THRESHOLD_PCT: f64 = 2.0;

// ---------------------------------------------------------------------------
// read/scan workload — identical source under both builds
// ---------------------------------------------------------------------------

/// Representative read flow: a full scan of `test_basic.simple_table` through the
/// public query API, which crosses every instrumented read boundary
/// (`query.execute`, the storage-open spans, the per-SSTable read spans, and the
/// `cqlite.read.*` / `cqlite.query.*` catalog metrics). Whether those spans/metrics
/// do any work depends solely on the build, which is exactly what we are measuring.
#[cfg(feature = "cli-helpers")]
fn bench_read_scan(c: &mut Criterion) {
    use criterion::{black_box, Throughput};

    let fx = fixtures::ReadFixture::SIMPLE;
    let loaded = fixtures::open_read_db(&fx);
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let sql = format!("SELECT * FROM {}", fx.qualified());

    let setup = rt
        .block_on(loaded.db.execute(&sql))
        .expect("overhead read setup query");
    let row_count = setup.rows.len() as u64;
    assert!(
        row_count > 0,
        "observability_overhead read_scan: scan of {} returned zero rows — fixtures not fetched?",
        fx.qualified()
    );

    let mut group = c.benchmark_group("observability_overhead");
    group.throughput(Throughput::Elements(row_count));
    group.bench_function("read_scan", |b| {
        b.iter(|| {
            let res = rt
                .block_on(loaded.db.execute(black_box(&sql)))
                .expect("overhead read scan");
            black_box(res.rows.len())
        });
    });
    group.finish();
}

// ---------------------------------------------------------------------------
// write/merge workload — identical source under both builds (write-support only)
// ---------------------------------------------------------------------------

/// Representative write/merge flow: ingest a fixed batch of rows into a fresh
/// `WriteEngine` and flush them to an SSTable. WAL durability is disabled here
/// to keep this strict overhead gate focused on CPU/memtable/flush/writer
/// instrumentation; per-row fsync latency is runner-I/O noise and is tracked by
/// the advisory WAL-on write bench instead. The flush exercises the SSTable
/// writer (the "merge"-shaped output path) without needing a multi-SSTable
/// compaction setup, keeping the bench deterministic and fast. Mirrors the
/// proven `write/flush` bench pattern.
#[cfg(feature = "write-support")]
fn bench_write_merge(c: &mut Criterion) {
    use criterion::{black_box, BatchSize, Throughput};
    use rand::Rng;

    /// Fixed number of rows ingested + flushed per iteration. Deterministic via
    /// the shared seeded RNG.
    const ROWS: u64 = 256;

    fn fill_engine(engine: &mut cqlite_core::storage::write_engine::WriteEngine) {
        let mut rng = fixtures::seeded_rng();
        for _ in 0..ROWS {
            let id = uuid::Uuid::from_u128(rng.gen());
            let age: i32 = rng.gen_range(0..100);
            let salary: i64 = rng.gen_range(30_000..200_000);
            let stmt = format!(
                "INSERT INTO test_basic.simple_table \
                 (id, name, age, salary, active) \
                 VALUES ({id}, 'overhead-row', {age}, {salary}, true)"
            );
            engine.execute(&stmt).expect("overhead write row");
        }
    }

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime for write overhead bench");

    let mut group = c.benchmark_group("observability_overhead");
    group.throughput(Throughput::Elements(ROWS));
    group.bench_function("write_merge", |b| {
        b.iter_batched(
            // SETUP (untimed): fresh temp dir + engine.
            || {
                let tmp = tempfile::TempDir::new().expect("temp dir for write overhead bench");
                // usize::MAX flush threshold so ingest never auto-flushes; we
                // flush explicitly in the routine.
                let engine = fixtures::open_write_engine_wal_off(tmp.path(), usize::MAX);
                (tmp, engine)
            },
            // ROUTINE (timed): ingest ROWS rows then flush to an SSTable.
            |(_tmp, mut engine)| {
                fill_engine(&mut engine);
                let result = rt.block_on(engine.flush()).expect("overhead flush");
                assert!(
                    result.is_some(),
                    "write_merge overhead: flush produced no SSTable"
                );
                black_box(result)
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

// ---------------------------------------------------------------------------
// criterion wiring — feature-gated so the binary always compiles
// ---------------------------------------------------------------------------

#[cfg(all(feature = "cli-helpers", feature = "write-support"))]
criterion_group!(
    name = benches;
    config = profiling::configure();
    targets = bench_read_scan, bench_write_merge
);

#[cfg(all(feature = "cli-helpers", not(feature = "write-support")))]
criterion_group!(
    name = benches;
    config = profiling::configure();
    targets = bench_read_scan
);

#[cfg(all(not(feature = "cli-helpers"), feature = "write-support"))]
criterion_group!(
    name = benches;
    config = profiling::configure();
    targets = bench_write_merge
);

#[cfg(not(any(feature = "cli-helpers", feature = "write-support")))]
fn bench_noop(_c: &mut Criterion) {
    // Without cli-helpers (read) or write-support (write) there is no workload to
    // run; the binary still compiles and runs so the bench harness stays valid.
}

#[cfg(not(any(feature = "cli-helpers", feature = "write-support")))]
criterion_group!(
    name = benches;
    config = profiling::configure();
    targets = bench_noop
);

criterion_main!(benches);
