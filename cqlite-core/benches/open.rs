//! Cold-open + per-reader-memory benchmarks for cqlite-core (Issue #1566, Epic A / A5).
//!
//! The read audit (`docs/reports/read-path-performance-audit-2026-07-01.md` §Epic A)
//! has no gauge for `SSTableReader::open` cost (loading Statistics / Summary /
//! CompressionInfo and, for BTI, the `Partitions.db` trie root) nor for the
//! per-reader memory footprint. Epic G3's bounded-`Index.db` mode needs the RSS
//! baseline; this bench establishes both.
//!
//! # Benches
//!
//! - `open/cold_big` — a fresh `SSTableReader::open` on the BIG multi-chunk
//!   `test_basic.simple_table` (`nb`) fixture: a genuine COLD open (component load),
//!   not a warm reuse — every iteration opens the file afresh.
//! - `open/cold_bti` — the same over the BTI `test_da.simple_table` (`da`) fixture,
//!   which additionally loads the trie root. **Optional**: skip-registers (no group)
//!   when the `test_da` corpus is absent.
//! - `mem/open_n_readers` — opens `N` readers over the BIG fixture and records the
//!   process RSS after, so G3 has a before/after per-reader memory gauge.
//!
//! # Honesty (parity-is-truth)
//!
//! A fixture that is entirely absent is skip-registered (no group, gate reports
//! SKIP). A fixture that is PRESENT but yields an unusable open panics at setup
//! rather than recording a misleading measurement (same guard family as A1/A2).
//!
//! # Ledger
//!
//! Beyond the criterion medians (which `scripts/profile_report.py` folds into the
//! unified `target/profiling/history.jsonl`), this bench appends its own measured
//! cold-open medians and the per-reader memory metric — which criterion's timing
//! model cannot express — directly through the shared `bench_ledger` module. The
//! append is best-effort: a ledger failure logs and never fails the bench.
//!
//! Gated on `cli-helpers` only to keep parity with the other read benches' feature
//! surface (the open path itself needs no query engine); under default features the
//! bench compiles to an empty-but-valid criterion group.

use criterion::{criterion_group, criterion_main, Criterion};

#[path = "bench_ledger/mod.rs"]
mod bench_ledger;

#[path = "fixtures/mod.rs"]
mod fixtures;

#[path = "profiling/mod.rs"]
mod profiling;

// ---------------------------------------------------------------------------
// cli-helpers benches
// ---------------------------------------------------------------------------

/// Readers opened for the memory footprint gauge. Enough that per-reader
/// component memory (Summary/Statistics/CompressionInfo/BTI root) dominates
/// fixed process overhead, without a long bench.
#[cfg(feature = "cli-helpers")]
const N_READERS: usize = 16;

/// Cold-open samples for the manual median appended to the ledger. Small — one
/// open is milliseconds — so the ledger append stays cheap alongside criterion.
#[cfg(feature = "cli-helpers")]
const MEDIAN_SAMPLES: usize = 30;

/// Locate the `*-Data.db` file inside a present fixture's table directory.
/// Returns `None` when absent (caller skip-registers) and panics only on a
/// genuinely broken directory it could not read.
#[cfg(feature = "cli-helpers")]
fn data_db_path(fx: &fixtures::ReadFixture) -> Option<std::path::PathBuf> {
    if !fixtures::fixture_present(fx) {
        return None;
    }
    let dir = fixtures::table_dir(fx.keyspace, fx.table);
    let entry = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("cannot read fixture dir {}: {e}", dir.display()))
        .filter_map(|e| e.ok())
        .find(|e| e.file_name().to_string_lossy().ends_with("-Data.db"));
    entry.map(|e| e.path())
}

/// Median of a sample (nearest-rank p50 by sort; 0 for empty). Local to avoid a
/// cross-bench dependency on the tail harness's percentile helper.
#[cfg(feature = "cli-helpers")]
fn median_ns(mut samples: Vec<u128>) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    samples.sort_unstable();
    samples[samples.len() / 2] as f64
}

/// Open one reader from `path`, panicking on error (present-but-broken guard).
#[cfg(feature = "cli-helpers")]
fn open_reader_blocking(
    rt: &tokio::runtime::Runtime,
    path: &std::path::Path,
    config: &cqlite_core::Config,
    platform: &std::sync::Arc<cqlite_core::Platform>,
) -> cqlite_core::storage::sstable::reader::SSTableReader {
    rt.block_on(cqlite_core::storage::sstable::reader::SSTableReader::open(
        path,
        config,
        platform.clone(),
    ))
    .unwrap_or_else(|e| {
        panic!(
            "open: fixture {} present but a fresh SSTableReader::open failed: {e} — \
             a broken/mismatched component would make this a misleading measurement",
            path.display()
        )
    })
}

/// Sample the process RSS in bytes: `/proc/self/statm` (current RSS, Linux) or
/// `getrusage` `ru_maxrss` (peak RSS in bytes, macOS). `None` elsewhere so the
/// caller records nothing rather than a bogus value. Best-effort.
#[cfg(feature = "cli-helpers")]
fn rss_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
        let resident_pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
        // SAFETY: `sysconf(_SC_PAGESIZE)` is a pure read of a system constant.
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if page <= 0 {
            return None;
        }
        Some(resident_pages.saturating_mul(page as u64))
    }
    #[cfg(target_os = "macos")]
    {
        // SAFETY: getrusage writes a fully-initialized rusage into the zeroed struct.
        let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
        let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
        if rc != 0 {
            return None;
        }
        // On macOS ru_maxrss is bytes (Linux would be KiB, handled above).
        Some(usage.ru_maxrss.max(0) as u64)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
}

/// Bench a genuine COLD open of `fx`'s `Data.db` and append its measured median to
/// the ledger. Skip-registers (no group) when the fixture is absent; panics at
/// setup when present-but-broken.
#[cfg(feature = "cli-helpers")]
fn bench_cold_open(
    c: &mut Criterion,
    fx: fixtures::ReadFixture,
    bench_name: &str,
    ledger_metric: &str,
    ledger: &mut Vec<(String, f64, String)>,
) {
    let Some(path) = data_db_path(&fx) else {
        eprintln!(
            "open/{bench_name}: fixture {} not present — skipping (skip-register)",
            fx.qualified()
        );
        return;
    };
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let config = cqlite_core::Config::default();
    let platform = std::sync::Arc::new(
        rt.block_on(cqlite_core::Platform::new(&config))
            .expect("platform init"),
    );

    // Setup guard: one fresh open must succeed (present-but-broken → panic).
    let reader = open_reader_blocking(&rt, &path, &config, &platform);
    std::hint::black_box(reader.header().generation);
    drop(reader);

    // Manual median (appended to the ledger — criterion's estimates are folded in
    // separately by profile_report.py).
    let mut samples = Vec::with_capacity(MEDIAN_SAMPLES);
    for _ in 0..MEDIAN_SAMPLES {
        let t0 = std::time::Instant::now();
        let reader = open_reader_blocking(&rt, &path, &config, &platform);
        samples.push(t0.elapsed().as_nanos());
        std::hint::black_box(reader.header().generation);
    }
    ledger.push((
        ledger_metric.to_string(),
        median_ns(samples),
        "ns".to_string(),
    ));

    let mut group = c.benchmark_group("open");
    group.bench_function(bench_name, |bch| {
        bch.iter(|| {
            let reader = open_reader_blocking(&rt, &path, &config, &platform);
            std::hint::black_box(reader.header().generation)
        });
    });
    group.finish();
}

/// `open/cold_big` over the always-present BIG (`nb`) fixture.
#[cfg(feature = "cli-helpers")]
fn bench_cold_big(c: &mut Criterion) {
    let mut ledger = Vec::new();
    bench_cold_open(
        c,
        fixtures::ReadFixture::SIMPLE,
        "cold_big",
        "cold_big_median_ns",
        &mut ledger,
    );
    append_ledger(&ledger);
}

/// `open/cold_bti` over the optional BTI (`da`) fixture (skip-register if absent).
#[cfg(feature = "cli-helpers")]
fn bench_cold_bti(c: &mut Criterion) {
    let mut ledger = Vec::new();
    bench_cold_open(
        c,
        fixtures::ReadFixture::SIMPLE_BTI,
        "cold_bti",
        "cold_bti_median_ns",
        &mut ledger,
    );
    append_ledger(&ledger);
}

/// `mem/open_n_readers`: open `N` readers, record the process RSS after (per-reader
/// and total), append that memory metric to the ledger, and time the batch open.
#[cfg(feature = "cli-helpers")]
fn bench_mem_open_n_readers(c: &mut Criterion) {
    let fx = fixtures::ReadFixture::SIMPLE;
    let Some(path) = data_db_path(&fx) else {
        eprintln!(
            "mem/open_n_readers: fixture {} not present — skipping (skip-register)",
            fx.qualified()
        );
        return;
    };
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let config = cqlite_core::Config::default();
    let platform = std::sync::Arc::new(
        rt.block_on(cqlite_core::Platform::new(&config))
            .expect("platform init"),
    );

    // Hold N readers open, then sample RSS with them all resident.
    let baseline = rss_bytes();
    let readers: Vec<_> = (0..N_READERS)
        .map(|_| open_reader_blocking(&rt, &path, &config, &platform))
        .collect();
    let after = rss_bytes();

    let mut ledger: Vec<(String, f64, String)> = Vec::new();
    if let Some(after) = after {
        ledger.push((
            "rss_after_n_readers_bytes".to_string(),
            after as f64,
            "bytes".to_string(),
        ));
        // Per-reader delta over the baseline (never negative; a best-effort gauge).
        if let Some(base) = baseline {
            let delta = after.saturating_sub(base);
            ledger.push((
                "rss_per_reader_bytes".to_string(),
                delta as f64 / N_READERS as f64,
                "bytes".to_string(),
            ));
        }
        ledger.push((
            "n_readers".to_string(),
            N_READERS as f64,
            "count".to_string(),
        ));
    } else {
        eprintln!(
            "mem/open_n_readers: RSS sampling unsupported on this platform — \
             recording no memory metric (skip, not fail)"
        );
    }
    drop(readers);
    append_ledger(&ledger);

    // Criterion timing of the N-reader batch open (drops the readers each iter).
    let mut group = c.benchmark_group("mem");
    group.bench_function("open_n_readers", |bch| {
        bch.iter(|| {
            let readers: Vec<_> = (0..N_READERS)
                .map(|_| open_reader_blocking(&rt, &path, &config, &platform))
                .collect();
            std::hint::black_box(readers.len())
        });
    });
    group.finish();
}

/// Append `metrics` under the `open` bench id, best-effort (log, never fail).
#[cfg(feature = "cli-helpers")]
fn append_ledger(metrics: &[(String, f64, String)]) {
    if metrics.is_empty() {
        return;
    }
    let rows: Vec<(&str, f64, &str)> = metrics
        .iter()
        .map(|(m, v, u)| (m.as_str(), *v, u.as_str()))
        .collect();
    if let Err(e) = bench_ledger::append_metrics("open", &rows) {
        eprintln!(
            "open: could not append unified ledger {}: {e}",
            bench_ledger::ledger_path().display()
        );
    }
}

// ---------------------------------------------------------------------------
// criterion_group! / criterion_main! — feature-gated so the bench compiles under
// default features (no cli-helpers) with an empty but valid group.
// ---------------------------------------------------------------------------

#[cfg(feature = "cli-helpers")]
criterion_group!(
    name = benches;
    config = profiling::configure();
    targets = bench_cold_big, bench_cold_bti, bench_mem_open_n_readers
);

#[cfg(not(feature = "cli-helpers"))]
fn bench_noop(_c: &mut Criterion) {
    // Nothing to bench without cli-helpers. The binary still compiles and runs.
}

#[cfg(not(feature = "cli-helpers"))]
criterion_group!(
    name = benches;
    config = profiling::configure();
    targets = bench_noop
);

criterion_main!(benches);
