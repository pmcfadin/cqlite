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
//! - `open/metadata_parse_big` / `open/metadata_parse_bti` — the FULL Statistics.db
//!   metadata parse cost per open (header + SerializationHeader + STATS post-passes;
//!   issue #1658), plus the **repeated-TOC-walk count per open** via
//!   `parser::toc_walk_metrics`. The BTI variant skip-registers when `test_da` is
//!   absent.
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

/// Locate the `*-Statistics.db` file inside a present fixture's table directory.
/// Returns `None` when absent (caller skip-registers) and panics only on a
/// genuinely broken directory it could not read.
#[cfg(feature = "cli-helpers")]
fn statistics_db_path(fx: &fixtures::ReadFixture) -> Option<std::path::PathBuf> {
    if !fixtures::fixture_present(fx) {
        return None;
    }
    let dir = fixtures::table_dir(fx.keyspace, fx.table);
    let entry = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("cannot read fixture dir {}: {e}", dir.display()))
        .filter_map(|e| e.ok())
        .find(|e| e.file_name().to_string_lossy().ends_with("-Statistics.db"));
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

/// Sample the process's CURRENT resident set size in bytes: `/proc/self/statm`
/// (Linux) or the mach `task_info(MACH_TASK_BASIC_INFO).resident_size` (macOS).
/// `None` elsewhere so the caller records nothing rather than a bogus value.
/// Best-effort.
///
/// macOS note (Finding 3, roborev): `getrusage.ru_maxrss` is PEAK RSS, not current,
/// so a per-reader delta computed from two peak samples can be 0/stale when an
/// earlier peak already exceeded the post-open footprint. mach `task_info` with
/// `MACH_TASK_BASIC_INFO` reports the live `resident_size`, so the delta honestly
/// reflects the readers just opened.
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
        // Current resident size via mach task_info(MACH_TASK_BASIC_INFO). Unlike
        // getrusage.ru_maxrss (peak), resident_size is the live RSS. The task port
        // comes from mach2 (libc's mach_task_self is deprecated); the info struct,
        // flavor, count, and task_info entry point are the non-deprecated libc ones.
        let mut info: libc::mach_task_basic_info = unsafe { std::mem::zeroed() };
        let mut count: libc::mach_msg_type_number_t = libc::MACH_TASK_BASIC_INFO_COUNT;
        // SAFETY: mach_task_self() returns this process's task port; task_info writes
        // `count` integers into `info` (zeroed, correctly sized via
        // MACH_TASK_BASIC_INFO_COUNT). We check the kern_return_t before reading it.
        let rc = unsafe {
            libc::task_info(
                mach2::traps::mach_task_self(),
                libc::MACH_TASK_BASIC_INFO,
                &mut info as *mut _ as libc::task_info_t,
                &mut count,
            )
        };
        if rc != libc::KERN_SUCCESS {
            return None;
        }
        Some(info.resident_size)
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

/// `open/metadata_parse` — the FULL Statistics.db metadata parse cost per open
/// (issue #1658). This is the `Statistics.db` half of a cold open: parse the
/// nb-format header, walk the TOC for the SerializationHeader offset, decode the
/// EncodingStats + SerializationHeader columns, and run the best-effort STATS
/// post-passes (`read_table_counts` + `parse_stats_extras`). It reads the whole
/// `*-Statistics.db` file ONCE, then benches the pure in-memory parse so the
/// number isolates parse cost from disk I/O.
///
/// It also records the **repeated-TOC-walk count per open** — how many times the
/// Statistics.db TOC is walked during a single metadata parse — via the
/// `parser::toc_walk_metrics` counter (reset before one parse, read after). This
/// surfaces the `enhanced_statistics_parser/mod.rs:187`/`:345` redundancy (issue
/// #1658 AC) as a measured integer rather than a guess.
///
/// Skip-registers (no group) when the fixture / Statistics.db is absent; panics
/// at setup when a present Statistics.db fails to parse (present-but-broken).
#[cfg(feature = "cli-helpers")]
fn bench_metadata_parse(
    c: &mut Criterion,
    fx: fixtures::ReadFixture,
    bench_name: &str,
    ledger: &mut Vec<(String, f64, String)>,
) {
    let Some(path) = statistics_db_path(&fx) else {
        eprintln!(
            "open/{bench_name}: fixture {} Statistics.db not present — skipping (skip-register)",
            fx.qualified()
        );
        return;
    };
    let buffer = std::fs::read(&path)
        .unwrap_or_else(|e| panic!("cannot read Statistics.db {}: {e}", path.display()));
    // Gates match the file's real on-disk version (nb / da) — the same derivation
    // `StatisticsReader::open` uses; a below-floor/unparseable descriptor is a
    // present-but-broken setup and panics rather than recording a bogus number.
    let gates = cqlite_core::storage::sstable::version_gate::VersionGates::from_path(&path)
        .unwrap_or_else(|e| {
            panic!(
                "open/{bench_name}: could not derive VersionGates from {} — {e}",
                path.display()
            )
        });

    // Setup guard: one parse must succeed (present-but-broken → panic). `is_ok()`
    // is read inside each call so the borrowed-buffer result never escapes.
    if !parse_statistics_ok(&buffer, &gates) {
        panic!(
            "open/{bench_name}: Statistics.db {} present but the metadata parse failed — \
             a broken component would make this a misleading measurement",
            path.display()
        );
    }

    // Repeated-TOC-walk count for ONE metadata parse (issue #1658 AC). Reset the
    // process-wide counter, do exactly one parse, then read it.
    cqlite_core::parser::toc_walk_metrics::reset_toc_walk_count();
    let _ = parse_statistics_ok(&buffer, &gates);
    let toc_walks_per_open = cqlite_core::parser::toc_walk_metrics::toc_walk_count();
    eprintln!(
        "open/{bench_name}: TOC walks per metadata parse = {toc_walks_per_open} (issue #1658)"
    );
    ledger.push((
        format!("{bench_name}_toc_walks_per_open"),
        toc_walks_per_open as f64,
        "count".to_string(),
    ));

    // Manual median of the metadata parse (appended to the ledger).
    let mut samples = Vec::with_capacity(MEDIAN_SAMPLES);
    for _ in 0..MEDIAN_SAMPLES {
        let t0 = std::time::Instant::now();
        let ok = parse_statistics_ok(&buffer, &gates);
        samples.push(t0.elapsed().as_nanos());
        std::hint::black_box(ok);
    }
    ledger.push((
        format!("{bench_name}_median_ns"),
        median_ns(samples),
        "ns".to_string(),
    ));

    let mut group = c.benchmark_group("open");
    group.bench_function(bench_name, |bch| {
        bch.iter(|| std::hint::black_box(parse_statistics_ok(&buffer, &gates)));
    });
    group.finish();
}

/// Parse `buffer` as an nb-format Statistics.db with `gates` and return only
/// whether it succeeded — the borrowed-buffer `Ok` result never escapes, so the
/// caller can reuse `buffer` across iterations without a lifetime tangle.
#[cfg(feature = "cli-helpers")]
fn parse_statistics_ok(
    buffer: &[u8],
    gates: &cqlite_core::storage::sstable::version_gate::VersionGates,
) -> bool {
    cqlite_core::parser::enhanced_statistics_parser::parse_statistics_with_fallback(
        buffer,
        Some(gates),
    )
    .is_ok()
}

/// `open/metadata_parse_big` over the always-present BIG (`nb`) fixture, plus
/// `open/metadata_parse_bti` over the optional BTI (`da`) fixture.
#[cfg(feature = "cli-helpers")]
fn bench_metadata_parse_all(c: &mut Criterion) {
    let mut ledger = Vec::new();
    bench_metadata_parse(
        c,
        fixtures::ReadFixture::SIMPLE,
        "metadata_parse_big",
        &mut ledger,
    );
    bench_metadata_parse(
        c,
        fixtures::ReadFixture::SIMPLE_BTI,
        "metadata_parse_bti",
        &mut ledger,
    );
    append_ledger(&ledger);
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
    targets = bench_cold_big, bench_cold_bti, bench_mem_open_n_readers, bench_metadata_parse_all
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
