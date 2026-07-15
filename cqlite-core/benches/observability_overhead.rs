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
//! # Subscriber-on variant (issue #1703, epic #1686 AI3; faithful install #2172a)
//!
//! A real CLI user runs with a fmt subscriber installed at INFO — the previously
//! unmeasured default posture. To measure it faithfully this bench installs a
//! **process-global** fmt subscriber (INFO, writing to `io::sink`) EXACTLY ONCE
//! (see [`ensure_global_subscriber`]) with a per-call [`ToggleFilter`] gated by an
//! atomic [`SUBSCRIBER_ON`] toggle. The `*_subscriber_on` variants flip the toggle
//! ON for their measurement; the baseline arms (`read_scan`, `write_merge`) leave
//! it OFF.
//!
//! Why global, not thread-local: the earlier `*_subscriber_on` arms installed the
//! subscriber via `tracing::subscriber::with_default`, which is THREAD-LOCAL — it
//! observed only spans emitted on the bench's own thread. The read scan crosses
//! `spawn_blocking` / blocking-pool threads, whose spans were therefore NOT
//! counted, so the recorded subscriber-on number under-counted the true default
//! posture. A single process-global default reaches every thread, so spans on
//! `spawn_blocking` threads are now counted too.
//!
//! The baseline arms therefore run with the global subscriber INSTALLED but the
//! toggle OFF (not truly subscriber-less). This does NOT affect the cross-build
//! feature-overhead comparison: the toggled-off global has the identical structure
//! in both the default and `observability` builds, so it cancels out. After the
//! #1703 uniform DEBUG demotion the write/compaction spans are DEBUG, so the INFO
//! filter drops them and the subscriber-on number stays close to baseline. The
//! comparison script records it **advisory-first** (prints + warns, never fails).
//!
//! Bench group/IDs (consumed by the comparison script):
//! - `observability_overhead/read_scan`
//! - `observability_overhead/read_scan_subscriber_on`
//! - `observability_overhead/write_merge`  (only when `write-support` is enabled)
//! - `observability_overhead/write_merge_subscriber_on`  (write-support only)

use criterion::{criterion_group, criterion_main, Criterion};
#[cfg(any(feature = "cli-helpers", feature = "write-support"))]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(any(feature = "cli-helpers", feature = "write-support"))]
use std::sync::Once;

/// Process-global on/off toggle for the INFO subscriber installed by
/// [`ensure_global_subscriber`]. The `*_subscriber_on` bench variants flip this
/// ON for the duration of their measurement (via [`SubscriberOnGuard`]); every
/// baseline arm leaves it OFF. Because the subscriber is a single PROCESS-GLOBAL
/// default it reaches spans emitted on `spawn_blocking` / blocking-pool threads
/// too — which the old thread-local `with_default` install silently missed,
/// under-counting the true default posture (issue #2172a).
#[cfg(any(feature = "cli-helpers", feature = "write-support"))]
static SUBSCRIBER_ON: AtomicBool = AtomicBool::new(false);

/// Guards the one-time install of the process-global subscriber.
#[cfg(any(feature = "cli-helpers", feature = "write-support"))]
static INSTALL_SUBSCRIBER: Once = Once::new();

/// A per-callsite `tracing` filter whose verdict is re-evaluated on EVERY call so
/// the runtime [`SUBSCRIBER_ON`] toggle is honored. Returning
/// `Interest::sometimes()` from `callsite_enabled` is load-bearing: `always()` /
/// `never()` would let `tracing` cache the first verdict per callsite and freeze
/// the toggle.
#[cfg(any(feature = "cli-helpers", feature = "write-support"))]
struct ToggleFilter;

#[cfg(any(feature = "cli-helpers", feature = "write-support"))]
impl<S> tracing_subscriber::layer::Filter<S> for ToggleFilter {
    fn enabled(
        &self,
        meta: &tracing::Metadata<'_>,
        _cx: &tracing_subscriber::layer::Context<'_, S>,
    ) -> bool {
        SUBSCRIBER_ON.load(Ordering::Relaxed) && *meta.level() <= tracing::Level::INFO
    }

    fn callsite_enabled(
        &self,
        _meta: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        // NEVER always()/never(): those cache the first verdict per callsite and
        // would defeat the runtime toggle. `sometimes()` forces per-call
        // `enabled()` re-evaluation.
        tracing::subscriber::Interest::sometimes()
    }
}

/// Install the process-global fmt subscriber (INFO, writing to `io::sink`) EXACTLY
/// once, gated by [`ToggleFilter`]. Called at the top of each bench fn so the
/// global is in place before any measurement regardless of which bench runs
/// first. The baseline arms run with [`SUBSCRIBER_ON`] == false, so this global is
/// a structurally-identical no-op in both the default and `observability` builds —
/// the cross-build feature-overhead comparison is unaffected.
#[cfg(any(feature = "cli-helpers", feature = "write-support"))]
fn ensure_global_subscriber() {
    use tracing_subscriber::prelude::*;
    INSTALL_SUBSCRIBER.call_once(|| {
        let fmt_layer = tracing_subscriber::fmt::layer()
            .with_writer(std::io::sink)
            .with_filter(ToggleFilter);
        tracing_subscriber::registry().with(fmt_layer).init();
    });
}

/// RAII guard that flips [`SUBSCRIBER_ON`] on for its lifetime and resets it on
/// drop — so a panic inside the measured closure still clears the toggle and
/// cannot leak the subscriber-on posture into a later baseline arm.
#[cfg(any(feature = "cli-helpers", feature = "write-support"))]
struct SubscriberOnGuard;

#[cfg(any(feature = "cli-helpers", feature = "write-support"))]
impl SubscriberOnGuard {
    fn activate() -> Self {
        SUBSCRIBER_ON.store(true, Ordering::Relaxed);
        SubscriberOnGuard
    }
}

#[cfg(any(feature = "cli-helpers", feature = "write-support"))]
impl Drop for SubscriberOnGuard {
    fn drop(&mut self) {
        SUBSCRIBER_ON.store(false, Ordering::Relaxed);
    }
}

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

    // Install the process-global subscriber before any measurement (idempotent).
    ensure_global_subscriber();

    const REPEATS: usize = 8;

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
    group.throughput(Throughput::Elements(row_count * REPEATS as u64));
    group.bench_function("read_scan", |b| {
        b.iter(|| {
            let mut rows = 0usize;
            for _ in 0..REPEATS {
                let res = rt
                    .block_on(loaded.db.execute(black_box(&sql)))
                    .expect("overhead read scan");
                rows += res.rows.len();
            }
            black_box(rows)
        });
    });

    // Subscriber-on variant (issue #1703 / #2172a): identical work, but with the
    // process-global INFO subscriber toggled ON for the whole measurement — the
    // CLI's default posture, now faithfully counting spans on `spawn_blocking`
    // threads too. Advisory number recorded by the comparison script.
    group.bench_function("read_scan_subscriber_on", |b| {
        let _on = SubscriberOnGuard::activate();
        b.iter(|| {
            let mut rows = 0usize;
            for _ in 0..REPEATS {
                let res = rt
                    .block_on(loaded.db.execute(black_box(&sql)))
                    .expect("overhead read scan (subscriber on)");
                rows += res.rows.len();
            }
            black_box(rows)
        });
    });
    group.finish();
}

// ---------------------------------------------------------------------------
// write/merge workload — identical source under both builds (write-support only)
// ---------------------------------------------------------------------------

/// Representative write flow: ingest a fixed batch of rows into a fresh
/// `WriteEngine`. WAL durability and SSTable flush are disabled here to keep
/// this strict overhead gate focused on CPU/memtable instrumentation; fsync and
/// Data.db file output are runner-I/O noise and are tracked by the write benches
/// instead. The benchmark ID remains `write_merge` for CI baseline continuity.
#[cfg(feature = "write-support")]
fn bench_write_merge(c: &mut Criterion) {
    use criterion::{black_box, BatchSize, Throughput};
    use rand::Rng;

    // Install the process-global subscriber before any measurement (idempotent).
    ensure_global_subscriber();

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
            // ROUTINE (timed): ingest ROWS rows into the memtable.
            |(_tmp, mut engine)| {
                fill_engine(&mut engine);
                assert_eq!(
                    engine.memtable_row_count(),
                    ROWS as usize,
                    "write_merge overhead: every row must reach the memtable"
                );
                black_box(engine.memtable_row_count())
            },
            BatchSize::SmallInput,
        );
    });

    // Subscriber-on variant (issue #1703 / #2172a): identical ingest, but with the
    // process-global INFO subscriber toggled ON for the whole measurement — the
    // CLI's default posture (per-mutation write.mutation / wal.* / memtable.insert
    // spans are DEBUG post-#1703, so the INFO filter drops them). Advisory number.
    group.bench_function("write_merge_subscriber_on", |b| {
        let _on = SubscriberOnGuard::activate();
        b.iter_batched(
            || {
                let tmp = tempfile::TempDir::new()
                    .expect("temp dir for write overhead bench (subscriber on)");
                let engine = fixtures::open_write_engine_wal_off(tmp.path(), usize::MAX);
                (tmp, engine)
            },
            |(_tmp, mut engine)| {
                fill_engine(&mut engine);
                assert_eq!(
                    engine.memtable_row_count(),
                    ROWS as usize,
                    "write_merge overhead (subscriber on): every row must reach the memtable"
                );
                black_box(engine.memtable_row_count())
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
