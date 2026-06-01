//! Write micro-benchmarks for cqlite-core (Issue #539, #574, Epic #541 Phase 1).
//!
//! These benches measure the three hot paths in the write engine:
//!
//! - `write/ingest_wal_on` — sustained mutation ingest through the public
//!   `WriteEngine::execute` API with WAL enabled (`Durability::SyncEachWrite`).
//!   Every row triggers `wal.append()` + `wal.sync()` (fsync), so this bench
//!   is dominated by I/O latency on real disks.  It is tracked as **advisory**
//!   in the perf gate — fsync variance on CI runners makes it unsuitable for
//!   strict pass/fail.
//!
//! - `write/ingest_wal_off` — identical 256-row ingest loop but built with
//!   [`Durability::Disabled`] (Issue #547/#574).  The measured path performs no
//!   `wal.append()` or `wal.sync()` — it is pure CPU + memtable cost.  This
//!   bench is tracked as **strictly gated** in the perf gate; because there is
//!   no fsync it is stable enough to catch genuine throughput regressions.
//!
//! - `write/flush` — memtable → SSTable flush latency.  A pre-filled memtable
//!   is flushed once per iteration; throughput is reported in MB/s relative to
//!   the pre-flush memtable byte size.
//!
//! All benches are deterministic: all input is generated from
//! [`fixtures::seeded_rng`] so key/value selection is byte-for-byte identical
//! across runs and machines.  Each iteration uses a fresh [`tempfile::TempDir`]
//! so iterations are independent and cannot share state through the WAL or data
//! files.
//!
//! ## Running
//!
//! ```text
//! # default features (compiles as no-op group):
//! cargo bench -p cqlite-core --bench write -- --test
//!
//! # with write-support:
//! cargo bench -p cqlite-core --features write-support --bench write -- --test
//! cargo bench -p cqlite-core --features write-support --bench write
//! ```

use criterion::{criterion_group, criterion_main, Criterion};

#[cfg(feature = "write-support")]
use criterion::{black_box, Throughput};

#[path = "fixtures/mod.rs"]
mod fixtures;

/// Number of rows inserted per ingest iteration.
#[cfg(feature = "write-support")]
const INGEST_ROWS: u64 = 256;

/// Number of rows inserted into the memtable before each flush iteration.
#[cfg(feature = "write-support")]
const FLUSH_ROWS: u64 = 1_000;

/// `write/ingest_wal_on` — sustained mutation ingest (WAL enabled).
///
/// Each iteration creates a fresh engine in a temp dir (setup, untimed) and
/// then inserts [`INGEST_ROWS`] seeded rows via `engine.execute` (routine,
/// timed).  The flush threshold is `usize::MAX` so no mid-batch auto-flush
/// perturbs the timing.
///
/// Throughput is reported as writes/second (`Throughput::Elements`).
///
/// This bench uses `Durability::SyncEachWrite` (the default): every row causes
/// `wal.append()` + `wal.sync()` (fsync), so the result is dominated by
/// disk-I/O latency.  It is tracked as **advisory** in the perf gate — fsync
/// variance on shared CI runners makes it unsuitable for strict pass/fail.
/// Use `write/ingest_wal_off` for the CPU-bound, strictly-gated measurement.
#[cfg(feature = "write-support")]
fn bench_ingest(c: &mut Criterion) {
    use rand::Rng;

    let mut group = c.benchmark_group("write");
    group.throughput(Throughput::Elements(INGEST_ROWS));

    group.bench_function("ingest_wal_on", |b| {
        b.iter_batched(
            // SETUP (untimed): fresh temp dir + engine with huge flush threshold
            // so no auto-flush fires mid-batch, plus the seeded RNG (its ChaCha
            // key schedule is kept out of the timed path).
            || {
                let tmp = tempfile::TempDir::new().expect("create temp dir for ingest bench");
                let engine = fixtures::open_write_engine(tmp.path(), usize::MAX);
                let rng = fixtures::seeded_rng();
                (tmp, engine, rng)
            },
            // ROUTINE (timed): insert INGEST_ROWS seeded rows.
            |(_tmp, mut engine, mut rng)| {
                for _ in 0..INGEST_ROWS {
                    let id = uuid::Uuid::from_u128(rng.gen());
                    let age: i32 = rng.gen_range(0..100);
                    let stmt = format!(
                        "INSERT INTO test_basic.simple_table \
                         (id, name, age, active) \
                         VALUES ({id}, 'bench-row', {age}, true)"
                    );
                    engine.execute(&stmt).expect("ingest seeded row");
                }
                // Assert that rows actually landed so a broken engine fails
                // loudly rather than measuring nothing.
                let n = engine.memtable_row_count();
                assert!(
                    n > 0,
                    "ingest_wal_on: memtable is empty after {INGEST_ROWS} inserts"
                );
                black_box(n)
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

/// `write/ingest_wal_off` — sustained mutation ingest (WAL disabled, Issue #574).
///
/// Identical to [`bench_ingest`] in every respect except the engine is built
/// with [`Durability::Disabled`]: `wal.append()` and `wal.sync()` are skipped
/// for every row.  The measured loop is pure CPU + memtable cost with no fsync
/// I/O on the critical path.
///
/// **This bench is strictly gated** in the CI perf regression gate: because
/// there is no fsync the measurement is stable across runs and a genuine
/// throughput regression will be caught reliably.  `write/ingest_wal_on`
/// complements it as the durability/disk probe (advisory, never fails CI on
/// its own).
///
/// Throughput is reported as writes/second (`Throughput::Elements`).
#[cfg(feature = "write-support")]
fn bench_ingest_wal_off(c: &mut Criterion) {
    use rand::Rng;

    let mut group = c.benchmark_group("write");
    group.throughput(Throughput::Elements(INGEST_ROWS));

    group.bench_function("ingest_wal_off", |b| {
        b.iter_batched(
            // SETUP (untimed): fresh temp dir + engine with Durability::Disabled
            // and huge flush threshold so no auto-flush fires mid-batch.
            || {
                let tmp =
                    tempfile::TempDir::new().expect("create temp dir for ingest_wal_off bench");
                let engine = fixtures::open_write_engine_wal_off(tmp.path(), usize::MAX);
                let rng = fixtures::seeded_rng();
                (tmp, engine, rng)
            },
            // ROUTINE (timed): insert INGEST_ROWS seeded rows — no wal.append()
            // or wal.sync() on this path; pure memtable cost.
            |(_tmp, mut engine, mut rng)| {
                for _ in 0..INGEST_ROWS {
                    let id = uuid::Uuid::from_u128(rng.gen());
                    let age: i32 = rng.gen_range(0..100);
                    let stmt = format!(
                        "INSERT INTO test_basic.simple_table \
                         (id, name, age, active) \
                         VALUES ({id}, 'bench-row', {age}, true)"
                    );
                    engine.execute(&stmt).expect("ingest seeded row (wal off)");
                }
                // Assert that rows actually landed so a broken engine fails
                // loudly rather than measuring nothing.
                let n = engine.memtable_row_count();
                assert!(
                    n > 0,
                    "ingest_wal_off: memtable is empty after {INGEST_ROWS} inserts"
                );
                black_box(n)
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

/// `write/flush` — memtable → SSTable flush latency.
///
/// Each iteration pre-fills a fresh engine with [`FLUSH_ROWS`] seeded rows
/// (setup, untimed), then flushes once via `rt.block_on(engine.flush())`
/// (routine, timed).  The flush must return `Ok(Some(_))` (i.e. actually
/// produce an SSTable); if it returns `None` the bench panics so a regression
/// is immediately visible.
///
/// Throughput is reported as bytes/second (`Throughput::Bytes`) using the
/// memtable byte size captured after setup so the report shows MB/s.
#[cfg(feature = "write-support")]
fn bench_flush(c: &mut Criterion) {
    use rand::Rng;

    // Helper: fill `engine` with FLUSH_ROWS seeded rows and return the
    // memtable byte size.
    fn fill_engine(engine: &mut cqlite_core::storage::write_engine::WriteEngine) -> usize {
        let mut rng = fixtures::seeded_rng();
        for _ in 0..FLUSH_ROWS {
            let id = uuid::Uuid::from_u128(rng.gen());
            let age: i32 = rng.gen_range(0..100);
            let salary: i64 = rng.gen_range(30_000..200_000);
            let stmt = format!(
                "INSERT INTO test_basic.simple_table \
                 (id, name, age, salary, active) \
                 VALUES ({id}, 'flush-row', {age}, {salary}, true)"
            );
            engine.execute(&stmt).expect("fill engine row");
        }
        engine.memtable_size()
    }

    // Measure the representative memtable byte size once so Criterion can use
    // it as a static throughput label.  (Criterion's throughput is set on the
    // group before the loop and doesn't change per-iteration.)
    let size_probe = {
        let tmp = tempfile::TempDir::new().expect("probe temp dir");
        let mut engine = fixtures::open_write_engine(tmp.path(), usize::MAX);
        let sz = fill_engine(&mut engine);
        assert!(
            sz > 0,
            "flush bench: memtable_size() returned 0 after {FLUSH_ROWS} inserts"
        );
        sz
    };

    // Shared Tokio runtime for `block_on(engine.flush())`.
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime for flush bench");

    let mut group = c.benchmark_group("write");
    group.throughput(Throughput::Bytes(size_probe as u64));

    group.bench_function("flush", |b| {
        b.iter_batched(
            // SETUP (untimed): fresh engine pre-filled with FLUSH_ROWS rows.
            || {
                let tmp = tempfile::TempDir::new().expect("create temp dir for flush bench");
                // Use usize::MAX flush threshold so the engine will not
                // auto-flush during setup; we trigger the flush manually in
                // the routine.
                let mut engine = fixtures::open_write_engine(tmp.path(), usize::MAX);
                let sz = fill_engine(&mut engine);
                assert!(
                    sz > 0,
                    "flush bench setup: memtable empty after {FLUSH_ROWS} rows"
                );
                (tmp, engine)
            },
            // ROUTINE (timed): flush once and assert an SSTable was produced.
            |(_tmp, mut engine)| {
                let result = rt.block_on(engine.flush()).expect("flush must not error");
                assert!(
                    result.is_some(),
                    "flush returned None — nothing was written to disk; \
                     check that FLUSH_ROWS rows are above the flush threshold"
                );
                black_box(result)
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

// ── criterion_group! / criterion_main! ──────────────────────────────────────
//
// Four variants mirror fixtures_smoke.rs so the bench file compiles under
// every feature combination without dead-code noise.

#[cfg(feature = "write-support")]
criterion_group!(benches, bench_ingest, bench_ingest_wal_off, bench_flush);

#[cfg(not(feature = "write-support"))]
fn bench_noop(_c: &mut Criterion) {
    // write-support is disabled; these benches are no-ops.
    // Enable with: --features write-support
}

#[cfg(not(feature = "write-support"))]
criterion_group!(benches, bench_noop);

criterion_main!(benches);
