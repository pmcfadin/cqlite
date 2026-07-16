//! Resource-leak soak test (issue #2013).
//!
//! Repeatedly opens a real [`SSTableReader`] on a real fixture `Data.db`, drains a
//! full `scan_stream()` to completion, then drops the reader — exercising the REAL
//! reader/scan open+drop path (no mocks; wiring evidence). Every K iterations it
//! samples two OS-level resource counters from `/proc` and, at the end, feeds the
//! series to a pure detector that flags a leak.
//!
//! ## What a leak looks like
//! - **FD leak**: `/proc/self/fd` count climbs monotonically as iterations run (an
//!   `SSTableReader::open` that leaves a `File`/mmap fd open per open/drop cycle).
//! - **RSS leak**: resident-set bytes grow past a bounded ceiling (a cache entry or
//!   buffer retained across the reader drop).
//!
//! ## Two soak variants (both `#[ignore]`; run with `-- --ignored`)
//! - [`soak_open_scan_drop_no_cache`] — block cache DISABLED, isolates FD/mmap leaks.
//! - [`soak_open_scan_drop_with_cache`] — block cache ENABLED with a small bounded
//!   budget, so RSS must plateau at a known ceiling (catches cache-entry leaks
//!   distinct from FD leaks).
//!
//! ## Always-on guards (run in the normal gate)
//! - [`sabotage_fd_leak_is_detected`] — runs the loop with a DELIBERATE fd leak and
//!   asserts the detector fires on real `/proc` samples (proves the detector works —
//!   AC #2). The sabotage is a permanent self-test mode local to this file; there is
//!   NO leak toggle anywhere in library code.
//! - [`analyze_detects_monotonic_fd_growth`] / [`analyze_ignores_bounded_wiggle`] —
//!   pure, deterministic, `/proc`-free unit tests of the detector logic.
//!
//! ## Env knobs (documented defaults)
//! - `CQLITE_SOAK_ITERATIONS` — loop iterations (default `120`; nightly sets `500+`).
//! - `CQLITE_SOAK_SAMPLE_EVERY` — sample every N iterations (default `10`).
//! - `CQLITE_REQUIRE_FIXTURES=1` — panic instead of SKIP when datasets are absent
//!   (so a fixtures-present lane can never pass vacuously).
//!
//! Linux-only: `/proc/self/{fd,statm}` are Linux interfaces. On other OSes the soak
//! test bodies no-op with a SKIP notice; the pure detector unit tests run everywhere.

/// Leak-detection thresholds. Both variants share the detector; only the numbers
/// (and the block-cache config) differ between the no-cache and with-cache soaks.
#[derive(Debug, Clone)]
struct LeakThresholds {
    /// A run of `>=` this many consecutive STRICTLY-increasing FD samples (after
    /// warmup) is treated as a leak.
    fd_consecutive_increase: usize,
    /// Max allowed growth of the OPEN FD COUNT over the post-warmup baseline
    /// (peak - first post-warmup sample). Catches a real but NOISY leak (dips
    /// interleaved with opens, e.g. from tokio worker fd churn) that never forms an
    /// unbroken run long enough to trip `fd_consecutive_increase` alone — a strict
    /// consecutive-run check can under-detect a leak whose growth isn't monotonic
    /// sample-to-sample (roborev finding, issue #2013).
    fd_growth_ceiling: usize,
    /// Max allowed growth of resident bytes over the post-warmup baseline.
    rss_growth_ceiling_bytes: u64,
}

/// Pure leak detector — deterministically unit-testable, no `/proc`, no I/O.
///
/// `warmup_samples` samples at the head are ignored (JIT/alloc/mmap warmup). Returns
/// `Err(diagnostic)` on a detected leak; the diagnostic ALWAYS embeds the full FD and
/// RSS series so the leak-onset iteration is visible (AC #3).
fn analyze_samples(
    fd: &[usize],
    rss: &[u64],
    warmup_samples: usize,
    thr: &LeakThresholds,
) -> Result<(), String> {
    let series = format!(
        "fd_samples={fd:?}\nrss_samples={rss:?}\nwarmup_samples={warmup_samples} thresholds={thr:?}"
    );

    // FD rule: longest run of consecutive strictly-increasing samples, post-warmup.
    let fd_tail = fd.get(warmup_samples..).unwrap_or(&[]);
    let mut longest_run = if fd_tail.is_empty() { 0 } else { 1 };
    let mut current_run = longest_run;
    for w in fd_tail.windows(2) {
        if w[1] > w[0] {
            current_run += 1;
            longest_run = longest_run.max(current_run);
        } else {
            current_run = 1;
        }
    }
    if longest_run >= thr.fd_consecutive_increase {
        return Err(format!(
            "FD leak: {longest_run} consecutive strictly-increasing samples \
             (threshold {})\n{series}",
            thr.fd_consecutive_increase
        ));
    }

    // FD net-growth rule: catches a real leak whose growth is noisy (interleaved
    // dips) rather than a clean monotonic run, which the strict-run rule above
    // would miss.
    if let Some(&fd_baseline) = fd_tail.first() {
        let fd_peak = fd_tail.iter().copied().max().unwrap_or(fd_baseline);
        let fd_growth = fd_peak.saturating_sub(fd_baseline);
        if fd_growth > thr.fd_growth_ceiling {
            return Err(format!(
                "FD leak: net growth {fd_growth} over baseline {fd_baseline} \
                 (ceiling {})\n{series}",
                thr.fd_growth_ceiling
            ));
        }
    }

    // RSS rule: growth over the post-warmup baseline must stay under the ceiling.
    let rss_tail = rss.get(warmup_samples..).unwrap_or(&[]);
    if let Some(&baseline) = rss_tail.first() {
        let peak = rss_tail.iter().copied().max().unwrap_or(baseline);
        let growth = peak.saturating_sub(baseline);
        if growth > thr.rss_growth_ceiling_bytes {
            return Err(format!(
                "RSS leak: grew {growth} bytes over baseline {baseline} \
                 (ceiling {})\n{series}",
                thr.rss_growth_ceiling_bytes
            ));
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Pure detector unit tests (run everywhere, always in the gate)
// ---------------------------------------------------------------------------

#[test]
fn analyze_detects_monotonic_fd_growth() {
    // Flat during warmup, then one fd leaked per sample thereafter.
    let fd = vec![10, 10, 10, 11, 12, 13, 14, 15, 16, 17, 18];
    let rss = vec![1_000_000u64; fd.len()];
    let thr = LeakThresholds {
        fd_consecutive_increase: 5,
        fd_growth_ceiling: 100, // disable the net-growth rule for this test
        rss_growth_ceiling_bytes: 64 * 1024 * 1024,
    };
    let res = analyze_samples(&fd, &rss, 2, &thr);
    assert!(res.is_err(), "monotonic fd growth must be detected");
    let msg = res.unwrap_err();
    // The diagnostic must embed the full series (AC #3).
    assert!(
        msg.contains("fd_samples="),
        "diagnostic missing fd series: {msg}"
    );
    assert!(
        msg.contains("rss_samples="),
        "diagnostic missing rss series: {msg}"
    );
}

#[test]
fn analyze_ignores_bounded_wiggle() {
    // FD bounces within a bounded band (no long increasing run); RSS bounded.
    let fd = vec![20, 21, 20, 21, 20, 21, 20, 21, 20, 21];
    let rss = vec![
        1_000_000, 1_010_000, 1_005_000, 1_012_000, 1_008_000, 1_011_000, 1_009_000, 1_010_000,
        1_007_000, 1_011_000,
    ];
    let thr = LeakThresholds {
        fd_consecutive_increase: 5,
        fd_growth_ceiling: 4,
        rss_growth_ceiling_bytes: 64 * 1024 * 1024,
    };
    assert!(
        analyze_samples(&fd, &rss, 1, &thr).is_ok(),
        "bounded wiggle must NOT be flagged as a leak"
    );
}

#[test]
fn analyze_detects_noisy_fd_growth() {
    // A real leak whose samples are NOISY (dips interleaved with growth) so the
    // longest strictly-increasing run never reaches the run threshold — the
    // strict-run rule alone would miss this; the net-growth rule must catch it.
    let fd = vec![10, 10, 11, 10, 12, 11, 13, 12, 14, 13, 15];
    let rss = vec![1_000_000u64; fd.len()];
    let thr = LeakThresholds {
        fd_consecutive_increase: 6, // longest run in `fd` below is only 2
        fd_growth_ceiling: 4,       // but net growth (10 -> 15 = 5) exceeds this
        rss_growth_ceiling_bytes: 64 * 1024 * 1024,
    };
    let res = analyze_samples(&fd, &rss, 1, &thr);
    assert!(
        res.is_err(),
        "noisy-but-real fd growth must be detected via the net-growth rule"
    );
    let msg = res.unwrap_err();
    assert!(
        msg.contains("net growth"),
        "expected the net-growth rule to fire, got: {msg}"
    );
}

#[test]
fn analyze_detects_rss_growth() {
    let fd = vec![10; 10];
    let mut rss = vec![10_000_000u64; 10];
    // Grow 200 MiB over baseline — well past a 64 MiB ceiling.
    for (i, r) in rss.iter_mut().enumerate().skip(2) {
        *r = 10_000_000 + (i as u64) * 30 * 1024 * 1024;
    }
    let thr = LeakThresholds {
        fd_consecutive_increase: 100, // disable the fd run rule for this test
        fd_growth_ceiling: 100,       // disable the fd net-growth rule too
        rss_growth_ceiling_bytes: 64 * 1024 * 1024,
    };
    assert!(
        analyze_samples(&fd, &rss, 2, &thr).is_err(),
        "unbounded rss growth must be detected"
    );
}

// ---------------------------------------------------------------------------
// Fixture resolution (shared with the linux soak bodies)
// ---------------------------------------------------------------------------

fn datasets_root() -> Option<std::path::PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(std::path::PathBuf::from)
        .filter(|p| p.is_dir())
}

fn require_fixtures() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").ok().as_deref(),
        Some("1") | Some("true")
    )
}

/// Resolve a real `test_basic/simple_table` `Data.db` (any generation dir). Returns
/// `None` after printing SKIP when datasets are absent; honors
/// `CQLITE_REQUIRE_FIXTURES=1` by panicking so this can never pass vacuously.
#[cfg(target_os = "linux")]
fn simple_table_data_db(test: &str) -> Option<std::path::PathBuf> {
    let Some(root) = datasets_root() else {
        if require_fixtures() {
            panic!("[{test}] CQLITE_REQUIRE_FIXTURES=1 but datasets absent");
        }
        eprintln!("SKIP [{test}]: datasets absent");
        return None;
    };

    let ks_dir = root.join("sstables/test_basic");
    let found = std::fs::read_dir(&ks_dir).ok().and_then(|entries| {
        entries.flatten().find_map(|e| {
            if !e.file_name().to_string_lossy().starts_with("simple_table-") {
                return None;
            }
            let dir = e.path();
            let data = std::fs::read_dir(&dir)
                .ok()?
                .flatten()
                .map(|f| f.file_name().to_string_lossy().into_owned())
                .find(|n| n.ends_with("-Data.db"))?;
            Some(dir.join(data))
        })
    });

    if found.is_none() {
        if require_fixtures() {
            panic!(
                "[{test}] CQLITE_REQUIRE_FIXTURES=1 but test_basic/simple_table \
                 (simple_table-*/*-Data.db) not found under {}",
                ks_dir.display()
            );
        }
        eprintln!("SKIP [{test}]: test_basic/simple_table fixture not present");
    }
    found
}

// ---------------------------------------------------------------------------
// Linux soak machinery
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
mod linux_soak {
    use super::{analyze_samples, simple_table_data_db, LeakThresholds};
    use std::sync::Arc;

    fn iterations() -> usize {
        std::env::var("CQLITE_SOAK_ITERATIONS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(120)
    }

    fn sample_every() -> usize {
        std::env::var("CQLITE_SOAK_SAMPLE_EVERY")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|&n| n > 0)
            .unwrap_or(10)
    }

    /// Open fd count via `/proc/self/fd`.
    fn open_fd_count() -> usize {
        std::fs::read_dir("/proc/self/fd")
            .map(|d| d.count())
            .unwrap_or(0)
    }

    /// Page size in bytes (libc is a dev-dependency, so available to this test crate).
    fn page_size() -> u64 {
        // SAFETY: `sysconf` with a compile-time constant name is always safe; it only
        // reads a system parameter and returns a `long`.
        let sz = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if sz > 0 {
            sz as u64
        } else {
            4096
        }
    }

    /// Resident-set bytes: second whitespace field of `/proc/self/statm` (resident
    /// pages) × page size.
    fn rss_bytes() -> u64 {
        let statm = std::fs::read_to_string("/proc/self/statm").expect("read /proc/self/statm");
        let resident_pages: u64 = statm
            .split_whitespace()
            .nth(1)
            .expect("statm resident field")
            .parse()
            .expect("parse resident pages");
        resident_pages * page_size()
    }

    /// One soak run. Per iteration: open a real reader → drain `scan_stream` → drop.
    /// When `leak` is true, each iteration also opens a raw `File` on the same path
    /// and stashes it in a `Vec` that OUTLIVES the loop — a genuine fd leak that
    /// exercises the detector on real `/proc` samples (sabotage self-test, AC #2).
    ///
    /// `iters`/`every` are passed EXPLICITLY rather than read from process env
    /// inside this function (roborev finding, issue #2013): reading env here would
    /// let a caller's `std::env::set_var` override race a sibling test's read of
    /// the SAME process-global env var if both ran in the same test binary
    /// concurrently (e.g. under `cargo test -- --include-ignored`) — unsound
    /// per `std::env::set_var`'s own safety notes, and a real correctness gap
    /// beyond just the CI-sanctioned invocation. Callers resolve env themselves.
    ///
    /// Returns `(fd_samples, rss_samples, total_rows_scanned)`.
    async fn run_soak(
        data_db: &std::path::Path,
        config: &cqlite_core::Config,
        leak: bool,
        iters: usize,
        every: usize,
    ) -> (Vec<usize>, Vec<u64>, u64) {
        use cqlite_core::storage::sstable::SSTableReader;

        let platform = Arc::new(
            cqlite_core::platform::Platform::new(config)
                .await
                .expect("platform"),
        );
        let table_id = cqlite_core::TableId::from("test_basic.simple_table");

        let mut fd_samples = Vec::new();
        let mut rss_samples = Vec::new();
        let mut total_rows: u64 = 0;
        // Deliberately leaked fds (sabotage mode only); outlives the loop.
        let mut leaked: Vec<std::fs::File> = Vec::new();

        for i in 0..iters {
            let reader = Arc::new(
                SSTableReader::open(data_db, config, platform.clone())
                    .await
                    .expect("open reader"),
            );
            let mut rx = reader
                .clone()
                .scan_stream(table_id.clone(), None, None, None, 64);
            while let Some(item) = rx.recv().await {
                let _ = item.expect("scan item");
                total_rows += 1;
            }
            drop(rx);
            drop(reader);

            if leak {
                leaked.push(std::fs::File::open(data_db).expect("sabotage leak open"));
            }

            if i % every == 0 {
                fd_samples.push(open_fd_count());
                rss_samples.push(rss_bytes());
            }
        }

        // Clean up the deliberately-leaked fds now that sampling is done.
        drop(leaked);
        (fd_samples, rss_samples, total_rows)
    }

    fn warmup_for(samples: usize) -> usize {
        (samples / 10).max(1)
    }

    /// Run the healthy soak (no deliberate leak) and assert the detector stays green,
    /// printing the full FD/RSS series for diagnosis regardless of outcome.
    async fn run_healthy(test: &str, config: &cqlite_core::Config, thr: &LeakThresholds) {
        let Some(data_db) = simple_table_data_db(test) else {
            return;
        };
        let (fd, rss, rows) = run_soak(&data_db, config, false, iterations(), sample_every()).await;
        eprintln!("[{test}] iterations complete, rows_scanned={rows}");
        eprintln!("[{test}] fd_samples={fd:?}");
        eprintln!("[{test}] rss_samples={rss:?}");
        assert!(rows > 0, "[{test}] scan must yield rows (wiring evidence)");
        let warmup = warmup_for(fd.len());
        if let Err(diag) = analyze_samples(&fd, &rss, warmup, thr) {
            panic!("[{test}] resource leak detected:\n{diag}");
        }
        eprintln!("[{test}] PASS: no resource leak (fd/rss plateaued)");
    }

    /// Soak variant A — block cache DISABLED. Isolates FD/mmap leaks in the reader
    /// open/drop path.
    #[tokio::test]
    #[ignore = "soak test; run via `-- --ignored` (bounded local / nightly)"]
    async fn soak_open_scan_drop_no_cache() {
        let mut config = cqlite_core::Config::default();
        config.memory.block_cache.enabled = false;
        config.memory.block_cache.max_size = 1;
        let thr = LeakThresholds {
            fd_consecutive_increase: 6,
            fd_growth_ceiling: 8,
            // No cache, so RSS should plateau; 96 MiB is generous slack over any
            // allocator retention while still catching an mmap/buffer leak.
            rss_growth_ceiling_bytes: 96 * 1024 * 1024,
        };
        run_healthy("soak_open_scan_drop_no_cache", &config, &thr).await;
    }

    /// Soak variant B — block cache ENABLED with a small bounded budget, so RSS must
    /// plateau at a known ceiling. Catches cache-entry leaks distinct from FD leaks.
    #[tokio::test]
    #[ignore = "soak test; run via `-- --ignored` (bounded local / nightly)"]
    async fn soak_open_scan_drop_with_cache() {
        let cache_budget: u64 = 4 * 1024 * 1024; // 4 MiB bounded block cache.
        let mut config = cqlite_core::Config::default();
        config.memory.block_cache.enabled = true;
        config.memory.block_cache.max_size = cache_budget;
        let thr = LeakThresholds {
            fd_consecutive_increase: 6,
            fd_growth_ceiling: 8,
            // Ceiling = cache budget + generous slack; RSS must plateau under this.
            rss_growth_ceiling_bytes: cache_budget + 124 * 1024 * 1024,
        };
        run_healthy("soak_open_scan_drop_with_cache", &config, &thr).await;
    }

    /// Sabotage self-test (AC #2): run the loop with a DELIBERATE fd leak and assert
    /// the detector fires on real `/proc` samples. NOT `#[ignore]` — a fast permanent
    /// regression guard. Keep iterations small for speed.
    ///
    /// Iteration/sample counts are passed to `run_soak` as LITERALS (40/4), never
    /// via `std::env::set_var` (roborev finding, issue #2013): mutating the SAME
    /// `CQLITE_SOAK_ITERATIONS`/`CQLITE_SOAK_SAMPLE_EVERY` vars that
    /// `soak_open_scan_drop_{no_cache,with_cache}` read via `iterations()`/
    /// `sample_every()` would race those tests if a caller ever ran the whole file
    /// with `--include-ignored` (this test is not itself `#[ignore]`), silently
    /// shrinking a real soak run's iteration count. Threading explicit parameters
    /// removes the shared global entirely, not just the specific invocation that
    /// happened not to trigger it.
    #[tokio::test]
    async fn sabotage_fd_leak_is_detected() {
        let test = "sabotage_fd_leak_is_detected";
        let Some(data_db) = simple_table_data_db(test) else {
            return;
        };
        // Small, fast run; leak one fd per iteration.
        let sabotage_iters = 40;
        let sabotage_sample_every = 4;

        let mut config = cqlite_core::Config::default();
        config.memory.block_cache.enabled = false;
        config.memory.block_cache.max_size = 1;
        let thr = LeakThresholds {
            fd_consecutive_increase: 6,
            fd_growth_ceiling: 8,
            rss_growth_ceiling_bytes: 96 * 1024 * 1024,
        };

        let (fd, rss, rows) = run_soak(
            &data_db,
            &config,
            true,
            sabotage_iters,
            sabotage_sample_every,
        )
        .await;
        eprintln!("[{test}] SABOTAGE fd_samples={fd:?}");
        eprintln!("[{test}] SABOTAGE rss_samples={rss:?}");
        assert!(
            rows > 0,
            "[{test}] scan must yield rows even under sabotage"
        );
        let warmup = warmup_for(fd.len());
        let res = analyze_samples(&fd, &rss, warmup, &thr);
        assert!(
            res.is_err(),
            "[{test}] detector FAILED to catch a deliberate fd leak — series: \
             fd={fd:?} rss={rss:?}"
        );
        eprintln!(
            "[{test}] PASS: detector caught the deliberate fd leak: {}",
            res.unwrap_err()
        );
    }
}

// ---------------------------------------------------------------------------
// Non-linux stubs: the /proc-based soaks cannot run; skip with a notice.
// ---------------------------------------------------------------------------

#[cfg(not(target_os = "linux"))]
#[tokio::test]
#[ignore = "soak test (linux-only)"]
async fn soak_open_scan_drop_no_cache() {
    let _ = (datasets_root, require_fixtures);
    eprintln!("SKIP soak_open_scan_drop_no_cache: linux-only (/proc/self/fd,statm)");
}

#[cfg(not(target_os = "linux"))]
#[tokio::test]
#[ignore = "soak test (linux-only)"]
async fn soak_open_scan_drop_with_cache() {
    eprintln!("SKIP soak_open_scan_drop_with_cache: linux-only (/proc/self/fd,statm)");
}

#[cfg(not(target_os = "linux"))]
#[tokio::test]
async fn sabotage_fd_leak_is_detected() {
    eprintln!("SKIP sabotage_fd_leak_is_detected: linux-only (/proc/self/fd,statm)");
}
