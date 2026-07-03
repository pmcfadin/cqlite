//! Shared mixed-load tail-latency harness (Issue #1563, Epic A / A2).
//!
//! This module is the measurement core for the `tail_latency` bench and the
//! `tail_latency_harness` integration test. Both include it via `#[path]`, so it
//! refers to the shared fixtures loader as [`crate::fixtures`] (it never declares
//! its own `mod fixtures`). Like `fixtures/mod.rs`, everything here is shared
//! support code — each includer uses only a subset — so the module allows dead
//! code rather than treating an unused helper as a smell.
//!
//! # What it measures
//!
//! The July 2026 read-path audit (`docs/reports/read-path-performance-audit-2026-07-01.md`
//! §Epic A) found the three biggest read-path defects (C2 cursor convoy, F1
//! reader-map FIFO stall, F3 blocking I/O on async workers) are all **tail**
//! pathologies: they barely move the median but inflate p99/p999 under a mixed
//! load (a background scan running while point reads arrive). The median gate
//! (A1, #1562) cannot see them.
//!
//! This harness reproduces that mixed load. Over one shared `Database` opened on
//! the BIG multi-chunk fixture (`test_basic.simple_table`, reused from A1) it:
//!
//! 1. runs a fixed-length stream of real partition-targeted **point reads**
//!    (`SELECT id, name … WHERE id = <uuid-literal>`, the #949/#956 path A1 proved)
//!    with **no** background scan — the *scan-free baseline*; then
//! 2. runs the identical point-read stream while **one continuous background
//!    full-table scan** (`SELECT *` looped) hammers the same reader set — the
//!    *mixed* load.
//!
//! It records per-op latency for each mode, computes `{p50, p99, p999}` (ns) for
//! both, and derives the gate ratios `p99_over_p50` (tail spread within the mixed
//! load) and `p99_mixed_over_scan_free` (convoy inflation). Everything is
//! additive (bench/gate/test only): no read-path production code changes.
//!
//! # Determinism & honesty
//!
//! The measured set is a fixed count (`WARMUP` + `MEASURED_N`), never a
//! wall-clock-bounded loop, so the sample size is identical run-to-run. Setup
//! asserts the point read returns >=1 row AND reports a *targeted* `AccessPath`
//! (`PartitionLookup`) — otherwise it panics loudly rather than measuring a scan
//! proxy (parity-is-truth; same guards as A1). The gate/tests compare **ratios**,
//! never wall-clock absolutes, so shared-runner noise cannot flap them.

#![allow(dead_code)]

use std::io::Write as _;
use std::path::{Path, PathBuf};

#[cfg(feature = "cli-helpers")]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(feature = "cli-helpers")]
use std::sync::Arc;
#[cfg(feature = "cli-helpers")]
use std::time::Instant;

#[cfg(feature = "cli-helpers")]
use cqlite_core::Database;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Warmup point reads issued (and discarded) before measurement, so the first
/// measured op is not paying one-time cache/JIT/allocation costs. 200 is enough
/// to warm the reader caches for the ~999-row fixture without dominating runtime.
pub const WARMUP: usize = 200;

/// Measured point reads per stream. The tail (p999) needs a sample large enough
/// that the 99.9th percentile is a real observation, not a single outlier: 2000
/// ops gives 2 samples at/above the p999 rank while still running in a few
/// seconds against the small fixture (point reads are sub-millisecond).
pub const MEASURED_N: usize = 2000;

/// Default history-ledger path, anchored to the crate dir (not the CWD) so the
/// bench appends to the same file regardless of where cargo runs it.
///
/// This holds **generated run data** (machine/run-specific) and is gitignored.
/// Epic A5 (cold-open bench + persisted ledger) introduces a unified
/// `history.jsonl`; this path is documented to fold into A5's ledger then.
pub fn default_ledger_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("benches/tail-latency-history.jsonl")
}

// ---------------------------------------------------------------------------
// Percentile math (pure — unit-tested with no dataset dependency)
// ---------------------------------------------------------------------------

/// Nearest-rank percentile (ns) of a latency sample.
///
/// `q` is a percentile in `[0, 100]` (e.g. `99.9` for p999). Sorts a copy of the
/// input (so the caller's slice need not be pre-sorted) and returns the value at
/// nearest rank `ceil(q/100 * N)`, 1-indexed and clamped to `[1, N]`. Returns `0`
/// for an empty sample (a total function; real streams are never empty because
/// setup guards >=1 row).
pub fn percentile_ns(latencies: &[u128], q: f64) -> u128 {
    if latencies.is_empty() {
        return 0;
    }
    let mut sorted = latencies.to_vec();
    sorted.sort_unstable();
    let n = sorted.len() as f64;
    let rank_f = ((q / 100.0) * n).ceil();
    let rank = if rank_f < 1.0 {
        1usize
    } else {
        rank_f as usize
    };
    let idx = rank.min(sorted.len()) - 1;
    sorted[idx]
}

/// The three tracked percentiles (nanoseconds) of one point-read stream.
///
/// `p50 <= p99 <= p999` holds **by construction**: nearest-rank percentiles are
/// monotonic in `q`, so a higher percentile can never pick a smaller sorted
/// value. No assertion is needed to maintain the invariant.
#[derive(Clone, Copy, Debug, serde::Serialize)]
pub struct TailStats {
    pub p50: u128,
    pub p99: u128,
    pub p999: u128,
}

impl TailStats {
    /// Compute `{p50, p99, p999}` from recorded per-op latencies.
    pub fn from_latencies(latencies: &[u128]) -> Self {
        Self {
            p50: percentile_ns(latencies, 50.0),
            p99: percentile_ns(latencies, 99.0),
            p999: percentile_ns(latencies, 99.9),
        }
    }
}

/// Ratio `num / den`, returning `0.0` when `den == 0` (a degenerate guard — real
/// ns latencies are microseconds, so the denominators here are never zero).
fn ratio(num: u128, den: u128) -> f64 {
    if den == 0 {
        return 0.0;
    }
    (num as f64) / (den as f64)
}

/// One harness run: point-read tail stats under the mixed load and the scan-free
/// baseline, plus the two derived gate ratios.
///
/// - `p99_over_p50` is the tail spread **within the mixed load** (`mixed.p99 /
///   mixed.p50`).
/// - `p99_mixed_over_scan_free` is the convoy inflation (`mixed.p99 /
///   scan_free.p99`) — the headline number the C2/F1/F3 fixes must drive down.
#[derive(Clone, Debug, serde::Serialize)]
pub struct HarnessReport {
    pub mixed: TailStats,
    pub scan_free: TailStats,
    pub p99_over_p50: f64,
    pub p99_mixed_over_scan_free: f64,
}

impl HarnessReport {
    /// Build a report from the two measured stat blocks, deriving the ratios.
    pub fn new(mixed: TailStats, scan_free: TailStats) -> Self {
        let p99_over_p50 = ratio(mixed.p99, mixed.p50);
        let p99_mixed_over_scan_free = ratio(mixed.p99, scan_free.p99);
        Self {
            mixed,
            scan_free,
            p99_over_p50,
            p99_mixed_over_scan_free,
        }
    }

    /// Machine-readable JSON: `{mixed:{p50,p99,p999}, scan_free:{p50,p99,p999},
    /// p99_over_p50, p99_mixed_over_scan_free}`. Field names match the spec.
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self)
            .unwrap_or_else(|e| format!("{{\"error\":\"serialize HarnessReport: {e}\"}}"))
    }
}

// ---------------------------------------------------------------------------
// History ledger
// ---------------------------------------------------------------------------

/// One appended ledger record: timestamp + commit + both stat blocks + ratios.
#[derive(serde::Serialize)]
struct LedgerRecord {
    ts: u64,
    commit: String,
    mixed: TailStats,
    scan_free: TailStats,
    p99_over_p50: f64,
    p99_mixed_over_scan_free: f64,
}

/// Best-effort current commit SHA: `GIT_COMMIT` env override, else `git rev-parse
/// HEAD`, else `"unknown"`. Never fails (ledger append must not abort a run).
fn current_commit() -> String {
    if let Ok(sha) = std::env::var("GIT_COMMIT") {
        let sha = sha.trim().to_string();
        if !sha.is_empty() {
            return sha;
        }
    }
    std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .ok()
        .filter(|out| out.status.success())
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

/// Append one JSON-line record for `report` to the ledger at `path`
/// (creating it if absent). Generated run data; the ledger is gitignored.
pub fn append_ledger(path: &Path, report: &HarnessReport) -> std::io::Result<()> {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let record = LedgerRecord {
        ts,
        commit: current_commit(),
        mixed: report.mixed,
        scan_free: report.scan_free,
        p99_over_p50: report.p99_over_p50,
        p99_mixed_over_scan_free: report.p99_mixed_over_scan_free,
    };
    let line = serde_json::to_string(&record).map_err(std::io::Error::other)?;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    writeln!(file, "{line}")
}

// ---------------------------------------------------------------------------
// Measurement (requires cli-helpers for the read fixture loader)
// ---------------------------------------------------------------------------

/// Format a 16-byte UUID as the canonical 8-4-4-4-12 unquoted-UUID literal the
/// SELECT parser accepts (issue #956). Duplicated from `benches/read.rs` so the
/// harness has no cross-bench dependency.
#[cfg(feature = "cli-helpers")]
fn uuid_to_literal(bytes: &[u8; 16]) -> String {
    let h = |range: std::ops::Range<usize>| -> String {
        bytes[range].iter().map(|b| format!("{b:02x}")).collect()
    };
    format!(
        "{}-{}-{}-{}-{}",
        h(0..4),
        h(4..6),
        h(6..8),
        h(8..10),
        h(10..16)
    )
}

/// Open the fixture DB, learn a present partition key, build the projected point
/// SQL, and run the two honesty guards. Returns `(shared db, point sql)`.
///
/// Panics loudly (never returns a mis-measurement) if the point read returns 0
/// rows or does not report a *targeted* `AccessPath` — the same wiring-evidence
/// guards as A1's `bench_get_partition`. The caller must have already confirmed
/// the fixture is present (see [`run`]).
///
/// The returned `Arc<Database>` owns a **leaked** temp copy of the SSTable
/// (see [`crate::fixtures::ReadDb::into_shared_db`]) so it is safe to share across
/// the scan thread and the point-read stream for the process lifetime.
#[cfg(feature = "cli-helpers")]
pub fn setup(fx: &crate::fixtures::ReadFixture) -> (Arc<Database>, String) {
    use cqlite_core::Value;

    let db = crate::fixtures::open_read_db(fx).into_shared_db();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tail_latency setup runtime");

    // Learn a present partition key from a one-shot scan.
    let scan = rt
        .block_on(db.execute(&format!("SELECT id FROM {}", fx.qualified())))
        .expect("tail_latency setup scan");
    let first = scan.rows.first().unwrap_or_else(|| {
        panic!(
            "tail_latency: scan of {} returned zero rows — fixtures not fetched?",
            fx.qualified()
        )
    });
    let id = match first.values.get("id") {
        Some(Value::Uuid(b)) => *b,
        other => panic!(
            "tail_latency: first row `id` did not decode as Value::Uuid (got {other:?}) for {}",
            fx.qualified()
        ),
    };
    let literal = uuid_to_literal(&id);

    // Projected (>8 tokens) so it routes through the modern SelectExecutor and
    // engages the #949 fast path (see benches/read.rs on the legacy routing quirk).
    let sql = format!(
        "SELECT id, name FROM {} WHERE id = {}",
        fx.qualified(),
        literal
    );

    // Guard 1: never silently measure a 0-row query.
    let probe = rt
        .block_on(db.execute(&sql))
        .expect("tail_latency setup point read");
    assert!(
        !probe.rows.is_empty(),
        "tail_latency: point read on {} returned zero rows for a known-present key — \
         #949/#956 regressed?",
        fx.qualified()
    );

    // Guard 2: the point read MUST take a partition-targeted path, else we would
    // be measuring a scan proxy under the "point read" name.
    let targeted = probe
        .metadata
        .access_path
        .as_ref()
        .map(|p| p.is_targeted())
        .unwrap_or(false);
    assert!(
        targeted,
        "tail_latency: point read fell back to full scan (access_path = {:?}) on {} — \
         #956/#949 regressed, or the query routed to the legacy executor",
        probe.metadata.access_path,
        fx.qualified()
    );

    (db, sql)
}

/// Issue `warmup` discarded then `n` measured point reads of `sql` against `db`,
/// returning the per-op latency (ns) of the measured ops. Uses its own
/// current-thread Tokio runtime so it can run on any OS thread.
#[cfg(feature = "cli-helpers")]
pub fn run_point_read_stream(db: &Arc<Database>, sql: &str, n: usize, warmup: usize) -> Vec<u128> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tail_latency point-read runtime");

    for _ in 0..warmup {
        let res = rt
            .block_on(db.execute(sql))
            .expect("tail_latency warmup point read");
        std::hint::black_box(res.rows.len());
    }

    let mut latencies = Vec::with_capacity(n);
    for _ in 0..n {
        let t0 = Instant::now();
        let res = rt
            .block_on(db.execute(sql))
            .expect("tail_latency measured point read");
        latencies.push(t0.elapsed().as_nanos());
        std::hint::black_box(res.rows.len());
    }
    latencies
}

/// Run the full harness against `fx`: scan-free baseline then mixed load, and the
/// derived report.
///
/// Returns `None` (with a skip message) when the fixture binary is absent, so a
/// clean checkout skips rather than fails. When the fixture is present it runs to
/// completion and panics loudly on a 0-row / non-targeted setup (see [`setup`]).
///
/// The mixed load spawns one background full-table-scan thread (its own runtime,
/// a clone of the shared `Arc<Database>`, and an `AtomicBool` stop flag). The
/// scan is started **before** and joined **after** the measured point-read stream
/// so the tail is captured under live contention, and the join asserts the scan
/// actually completed at least one full pass (a wedged scan is a bug, not a pass).
#[cfg(feature = "cli-helpers")]
pub fn run(fx: crate::fixtures::ReadFixture) -> Option<HarnessReport> {
    if !crate::fixtures::fixture_present(&fx) {
        eprintln!(
            "tail_latency: fixture {} not present — skipping (fetch: bash test-data/scripts/fetch-datasets.sh)",
            fx.qualified()
        );
        return None;
    }

    let (db, sql) = setup(&fx);

    // Scan-free baseline first (no background contention).
    let scan_free_lat = run_point_read_stream(&db, &sql, MEASURED_N, WARMUP);
    let scan_free = TailStats::from_latencies(&scan_free_lat);

    // Mixed load: one continuous background full-table scan on its own thread.
    let stop = Arc::new(AtomicBool::new(false));
    // Readiness signal: the scan thread sets this once it has *completed a full
    // scan* (and is looping into the next), so the measured point-read stream
    // overlaps a demonstrably-live scan rather than racing thread startup (roborev
    // finding — without this, "scans > 0" after join only proves a scan ran at some
    // point, not during the measured window).
    let scan_started = Arc::new(AtomicBool::new(false));
    let scan_db = Arc::clone(&db);
    let scan_stop = Arc::clone(&stop);
    let scan_started_w = Arc::clone(&scan_started);
    let scan_sql = format!("SELECT * FROM {}", fx.qualified());
    let scan_handle = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tail_latency background-scan runtime");
        let mut scans: u64 = 0;
        while !scan_stop.load(Ordering::Relaxed) {
            let res = rt
                .block_on(scan_db.execute(&scan_sql))
                .expect("tail_latency background scan");
            std::hint::black_box(res.rows.len());
            scans += 1;
            // Signal readiness only AFTER a full scan has actually executed, and
            // keep looping: this proves the scan path is live (one pass done, the
            // next already starting) before the foreground measured stream is
            // released, rather than merely proving the thread was scheduled
            // (roborev — a pre-execute flag could fire while this thread is
            // descheduled, leaving the "mixed" window mostly scan-free).
            scan_started_w.store(true, Ordering::Relaxed);
        }
        scans
    });

    // Wait (bounded) until the background scan has actually begun, so the measured
    // point reads run under live contention. A timeout is a HARNESS FAILURE, not a
    // free pass: measuring the "mixed" stream without a live scan would mostly
    // record the scan-free path and could still satisfy `scans > 0` from one late
    // scan (roborev). Stop + join the thread and panic rather than mis-measure.
    let wait_start = std::time::Instant::now();
    while !scan_started.load(Ordering::Relaxed) {
        if wait_start.elapsed() > std::time::Duration::from_secs(30) {
            stop.store(true, Ordering::Relaxed);
            let _ = scan_handle.join();
            panic!(
                "tail_latency: background scan did not complete a full pass within 30s — cannot \
                 measure the point-read stream under live contention (mixed load would be invalid)"
            );
        }
        std::thread::yield_now();
    }

    let mixed_lat = run_point_read_stream(&db, &sql, MEASURED_N, WARMUP);
    let mixed = TailStats::from_latencies(&mixed_lat);

    stop.store(true, Ordering::Relaxed);
    let scans = scan_handle
        .join()
        .expect("tail_latency background-scan thread panicked");
    assert!(
        scans > 0,
        "tail_latency: background scan completed zero full passes — wedged scan path?"
    );

    Some(HarnessReport::new(mixed, scan_free))
}
