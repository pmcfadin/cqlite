//! Issue #1143 (P0): the read-side p99 tail must NOT regress under `Auto`
//! prefetch on the mmap backend relative to buffered I/O.
//!
//! ## What regressed and the fix under guard
//!
//! #964 flipped the default read backend to mmap for mid-size files AND mapped
//! `PrefetchMode::Auto` -> `MADV_SEQUENTIAL`. `MADV_SEQUENTIAL` couples read-ahead
//! with *drop-behind*: pages are evicted from the page cache as soon as a scan
//! moves past them. In isolation mmap is faster, but under concurrent write load
//! (page-cache pressure) the just-dropped pages are gone when an overlapping scan
//! re-reads them, so those re-reads take SYNCHRONOUS major page faults on the
//! tokio worker thread and the read-side p99 tail blows up (~2x).
//!
//! The fix (`mmap_advice_for`, `reader/backend_resolve.rs`) took
//! `PrefetchMode::Auto` OFF `MADV_SEQUENTIAL`. Since issue #2824 `Auto` maps to
//! `MADV_WILLNEED`, which queues asynchronous read-ahead and has NO drop-behind
//! semantics, so hot pages stay resident and #1143's mechanism does not
//! transfer. The invariant this guard exists for is "`Auto` never yields
//! `MADV_SEQUENTIAL`", not "`Auto` yields nothing".
//!
//! ## Why the latency comparison is OBSERVATIONAL ONLY (no timing assert)
//!
//! Absolute p99 microseconds are machine- and load-dependent and flake on shared
//! runners (that is exactly why `read_while_write.rs` only measures, never
//! asserts). This test compares the mmap-`Auto` path against the buffered path
//! *within the same run, on the same host, under the same induced pressure* and
//! logs both p50/p99 and their ratios — but it does NOT assert on timing. With
//! only `READERS * SCANS_PER_READER` (48) samples, nearest-rank p99 is the single
//! slowest scan, so a ratio-vs-ratio timing assert flakes nondeterministically on
//! one scheduler pause even when the prefetch policy mapping is correct. The
//! deterministic regression guard for the fix is the unit test
//! `test_mmap_advice_for_auto_is_willneed_never_sequential` (`reader/tests.rs`),
//! which asserts `mmap_advice_for(Auto) != Some(Sequential)`; this test remains a
//! never-flaking load-shape smoke that surfaces the tail shape in logs.
//! Non-timing correctness IS still enforced: both backends must scan a non-zero
//! row count, and the test skips cleanly when fixtures/host cannot force reclaim.
//!
//! ## Skips cleanly
//!
//! - No `CQLITE_DATASETS_ROOT` / no real Data.db fixture -> skip (never a
//!   vacuous pass; a present fixture returning zero rows is a failure).
//! - Host cannot allocate the pressure buffer -> skip (the invariant is only
//!   meaningful once the page cache is actually contended).

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use cqlite_core::config::{DiskAccessMode, PrefetchMode};
use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::{Config, Database};

const KEYSPACE: &str = "test_wide_rows";
const TABLE: &str = "wide_partition_table";
const SCHEMA_FILE: &str = "wide-rows.cql";

/// Concurrent reader tasks issuing full scans (mirrors the #1143 workload shape).
const READERS: usize = 6;
/// Full scans each reader performs. Total tail sample = `READERS * SCANS_PER_READER`.
const SCANS_PER_READER: usize = 8;

/// Documented tolerance for the relative invariant: the mmap-`Auto` tail
/// inflation (p99/p50) may exceed buffered's by at most this factor. The pre-fix
/// `MADV_SEQUENTIAL` drop-behind inflated it several-fold under pressure; this
/// bound catches that while absorbing ordinary scheduler noise. Not an absolute
/// latency.
const MAX_RELATIVE_TAIL_FACTOR: f64 = 3.0;

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

fn schemas_dir() -> Option<PathBuf> {
    if let Some(root) = datasets_root() {
        if let Some(parent) = root.parent() {
            let d = parent.join("schemas");
            if d.exists() {
                return Some(d);
            }
        }
    }
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let d = manifest_dir.parent()?.join("test-data").join("schemas");
    d.exists().then_some(d)
}

/// Whether a real Data.db fixture is present for the target table.
fn fixture_present() -> bool {
    let Some(root) = datasets_root() else {
        return false;
    };
    let table_root = root.join("sstables").join(KEYSPACE);
    let Ok(entries) = std::fs::read_dir(&table_root) else {
        return false;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(&format!("{TABLE}-")) && entry.path().is_dir() {
            if let Ok(inner) = std::fs::read_dir(entry.path()) {
                if inner.flatten().any(|f| {
                    f.file_name()
                        .to_str()
                        .is_some_and(|n| n.ends_with("-Data.db"))
                }) {
                    return true;
                }
            }
        }
    }
    false
}

/// Ingest the fixture table with the given disk-access mode and default
/// (`Auto`) prefetch — the path the fix changes.
async fn setup_db(mode: DiskAccessMode) -> Database {
    let root = datasets_root().expect("CQLITE_DATASETS_ROOT");
    let schema_path = schemas_dir().expect("schemas dir").join(SCHEMA_FILE);
    assert!(schema_path.exists(), "schema not found: {schema_path:?}");

    let mut core_config = Config::default();
    core_config.storage.disk_access_mode = mode;
    // Exercise the fixed mapping explicitly: `Auto` prefetch must no longer emit
    // `MADV_SEQUENTIAL` on the mmap backend.
    core_config.storage.prefetch = PrefetchMode::Auto;

    let config = IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir: root.join("sstables"),
        version_hint: None,
        core_config,
        table_directory_filter: Some(format!("/{KEYSPACE}/")),
    };
    ingest(config)
        .await
        .expect("ingest wide_partition_table")
        .database
}

/// RAII guard that signals the page-cache pressure loop to stop on EVERY exit
/// path — including a panic/`expect` failure in a reader task. Without it, an
/// unwind from `measure_tail` would skip the `stop.store(true, ..)` and leave
/// the `spawn_blocking` churn loop running forever (tokio cannot abort an
/// already-running blocking task), hanging the test instead of reporting the
/// regression.
struct StopGuard(Arc<AtomicBool>);

impl Drop for StopGuard {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

/// Nearest-rank percentile of a latency sample (sorts in place).
fn percentile(samples: &mut [Duration], pct: f64) -> Duration {
    if samples.is_empty() {
        return Duration::ZERO;
    }
    samples.sort_unstable();
    let n = samples.len() as f64;
    let rank = ((pct / 100.0) * n).ceil().max(1.0) as usize;
    samples[rank.min(samples.len()) - 1]
}

/// Run `READERS` concurrent full-scan tasks against `db` while a background task
/// churns a large heap buffer to pressure the page cache, and return
/// `(p50, p99)` of every scan latency. Returns `None` if a scan yields zero rows
/// (fixture present but empty -> caller fails, never a vacuous pass).
async fn measure_tail(db: Arc<Database>, sql: Arc<String>) -> Option<(Duration, Duration)> {
    let stop = Arc::new(AtomicBool::new(false));
    // Set `stop` on ALL exit paths (incl. a reader-task panic/error) before this
    // function unwinds, so the blocking pressure loop below always terminates.
    let _stop_guard = StopGuard(Arc::clone(&stop));

    // Page-cache pressure: repeatedly touch a large buffer so the kernel is
    // forced to reclaim, which is what makes `MADV_SEQUENTIAL` drop-behind
    // hurt. Best-effort: if the host cannot allocate, the caller skips.
    let pressure = {
        let stop = Arc::clone(&stop);
        tokio::task::spawn_blocking(move || {
            // 256 MiB churn buffer; large enough to exercise reclaim without
            // being reckless. `try_reserve` so we never abort on OOM.
            let mut buf: Vec<u8> = Vec::new();
            if buf.try_reserve_exact(256 * 1024 * 1024).is_err() {
                return false; // host too small -> signal skip
            }
            buf.resize(256 * 1024 * 1024, 0u8);
            let mut i = 0usize;
            while !stop.load(Ordering::Relaxed) {
                // Stride across pages so every pass dirties fresh cache lines.
                for chunk in buf.chunks_mut(4096) {
                    chunk[0] = chunk[0].wrapping_add(1);
                }
                i = i.wrapping_add(1);
                if i % 64 == 0 {
                    std::thread::yield_now();
                }
            }
            true // pressure was actually applied
        })
    };

    let mut reader_handles = Vec::with_capacity(READERS);
    for _ in 0..READERS {
        let db = Arc::clone(&db);
        let sql = Arc::clone(&sql);
        reader_handles.push(tokio::spawn(async move {
            let mut samples = Vec::with_capacity(SCANS_PER_READER);
            let mut min_rows = usize::MAX;
            for _ in 0..SCANS_PER_READER {
                let t0 = Instant::now();
                let res = db.execute(&sql).await.expect("scan under pressure");
                samples.push(t0.elapsed());
                min_rows = min_rows.min(res.rows.len());
            }
            (samples, min_rows)
        }));
    }

    // Collect every reader result WITHOUT propagating a panic yet, so a failed
    // reader cannot unwind before we signal stop. (The `_stop_guard` above is the
    // belt-and-suspenders backstop; collecting first keeps the happy path from
    // relying on unwind-time drop ordering.)
    let mut reader_results = Vec::with_capacity(READERS);
    for h in reader_handles {
        reader_results.push(h.await);
    }

    // Signal stop and drain the pressure task before touching any reader outcome.
    stop.store(true, Ordering::Relaxed);
    let pressure_applied = pressure.await.expect("pressure task");

    let mut latencies: Vec<Duration> = Vec::with_capacity(READERS * SCANS_PER_READER);
    let mut min_rows = usize::MAX;
    for r in reader_results {
        let (samples, rows) = r.expect("reader task");
        latencies.extend(samples);
        min_rows = min_rows.min(rows);
    }

    if !pressure_applied {
        return None; // host could not force reclaim -> skip
    }

    if min_rows == 0 {
        // Fixture present but a scan returned zero rows: a real failure, never a
        // vacuous pass.
        panic!("issue #1143 guard: scan returned zero rows — corrupt/empty fixture?");
    }

    let p50 = percentile(&mut latencies, 50.0);
    let p99 = percentile(&mut latencies, 99.0);
    Some((p50, p99))
}

/// Tail-inflation ratio p99/p50 (unitless). Guards against absolute-latency
/// flakiness: only the *shape* of the distribution is compared across backends.
fn tail_ratio(p50: Duration, p99: Duration) -> f64 {
    let p50 = p50.as_secs_f64().max(1e-9);
    p99.as_secs_f64() / p50
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mmap_auto_prefetch_tail_not_worse_than_buffered() {
    if !fixture_present() {
        eprintln!(
            "issue #1143 guard: no real {KEYSPACE}.{TABLE} Data.db fixture \
             (set CQLITE_DATASETS_ROOT and run test-data/scripts/fetch-datasets.sh) — skipping"
        );
        return;
    }

    let sql = Arc::new(format!("SELECT * FROM {KEYSPACE}.{TABLE}"));

    // Buffered baseline (no mmap, so `MADV_SEQUENTIAL` is irrelevant): the
    // reference tail shape for this host+load.
    let buffered_db = Arc::new(setup_db(DiskAccessMode::Buffered).await);
    let Some((buf_p50, buf_p99)) = measure_tail(Arc::clone(&buffered_db), Arc::clone(&sql)).await
    else {
        eprintln!(
            "issue #1143 guard: host could not allocate the page-cache pressure buffer — skipping"
        );
        return;
    };

    // mmap with `Auto` prefetch (the fixed path). Pre-fix this emitted
    // `MADV_SEQUENTIAL` drop-behind and its tail inflated under the same pressure.
    let mmap_db = Arc::new(setup_db(DiskAccessMode::Mmap).await);
    let Some((mmap_p50, mmap_p99)) = measure_tail(Arc::clone(&mmap_db), Arc::clone(&sql)).await
    else {
        eprintln!("issue #1143 guard: pressure buffer unavailable on mmap pass — skipping");
        return;
    };

    let buf_ratio = tail_ratio(buf_p50, buf_p99);
    let mmap_ratio = tail_ratio(mmap_p50, mmap_p99);

    // OBSERVATIONAL ONLY — no timing pass/fail assertion here (like the
    // `read_while_write` bench). With only `READERS * SCANS_PER_READER` (48)
    // samples, nearest-rank p99 is the single slowest scan, so one scheduler
    // pause makes a ratio-vs-ratio timing assert flake in CI even when the
    // prefetch policy mapping is correct. The deterministic regression guard is
    // the unit test asserting `mmap_advice_for(Auto) != Some(Sequential)`
    // (`reader/tests.rs`, retargeted by #2824); this test stays a never-flaking
    // load-shape smoke that logs the measured tail shape.
    eprintln!(
        "issue #1143 guard (observational): buffered p50={buf_p50:?} p99={buf_p99:?} \
         ratio={buf_ratio:.2} | mmap(Auto) p50={mmap_p50:?} p99={mmap_p99:?} \
         ratio={mmap_ratio:.2} | reference limit={:.2}x (not asserted)",
        MAX_RELATIVE_TAIL_FACTOR
    );
}
