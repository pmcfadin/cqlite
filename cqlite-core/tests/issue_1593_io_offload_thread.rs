//! Issue #1593 (Epic F, F3): the windowed streaming scan must run its BLOCKING
//! I/O — the raw chunk read on a synchronously-faulting backend (mmap page fault
//! / `O_DIRECT` `pread`) — OFF the async worker pool, on a `spawn_blocking`
//! thread.
//!
//! ## What this guards
//!
//! On the mmap and direct backends the disk I/O happens *inside* `poll_read`
//! (`source.rs`), so reading them on a tokio async worker BLOCKS that worker for
//! the duration of the disk read. `K` concurrent cold scans can pin every worker,
//! stalling all warm point reads — the p99-diverges-under-mixed-load mechanism
//! the July 2026 read-path audit (§Epic F / F3) named. The parse half was already
//! moved to `spawn_blocking` (#1143); this closes the remaining gap on the I/O
//! half for faulting backends.
//!
//! ## Why a thread-identity guard (not a wall-clock p99)
//!
//! Identical rationale to the #1143 parse-offload guard: absolute latency
//! thresholds are machine-dependent and flaky, and the corpus fixtures are tiny,
//! so a single scan never monopolizes a worker long enough to observe wall-clock
//! starvation deterministically. This guard pins the MECHANISM directly: it
//! records the `ThreadId` on which the scan's raw chunk read ran (via the
//! `scan-offload-probe`, compiled only under the non-default feature so it never
//! ships) and asserts that thread is NOT one of the async worker threads.
//!   - inline-on-async-worker read (regressed / `main`): read runs ON a worker
//!     → recorded thread is in the worker set → FAIL;
//!   - `spawn_blocking` offload (fixed): read runs on a blocking-pool thread
//!     → recorded thread is OUTSIDE the worker set → PASS.
//!
//! Requirements: `CQLITE_DATASETS_ROOT` pointing at `test-data/datasets` and the
//! real multi-chunk `nb`-compressed fixture (skip-not-fail when absent; a present
//! fixture returning zero rows is a FAILURE, never a vacuous pass).

// Requires the non-default `scan-offload-probe` feature: it gates the probe
// instrumentation this guard reads. The agent gate runs this binary with the
// feature enabled.
#![cfg(all(
    feature = "state_machine",
    feature = "cli-helpers",
    feature = "scan-offload-probe"
))]

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::thread::ThreadId;

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::result::StreamingConfig;
use cqlite_core::storage::sstable::reader::scan_stream_windowed::probe as scan_offload_probe;
use cqlite_core::Database;

const KEYSPACE: &str = "test_wide_rows";
const TABLE: &str = "wide_partition_table";
const SCHEMA_FILE: &str = "wide-rows.cql";
/// Async worker threads on this test's runtime. MUST match the literal in the
/// `#[tokio::test(... worker_threads = N)]` attribute below.
const WORKER_THREADS: usize = 2;

fn get_datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

fn get_schemas_dir() -> Option<PathBuf> {
    if let Some(datasets_root) = get_datasets_root() {
        if let Some(parent) = datasets_root.parent() {
            let schemas_dir = parent.join("schemas");
            if schemas_dir.exists() {
                return Some(schemas_dir);
            }
        }
    }
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let schemas_dir = manifest_dir.parent()?.join("test-data").join("schemas");
    if schemas_dir.exists() {
        return Some(schemas_dir);
    }
    None
}

/// Directory holding the fixture's SSTable components, if a Data.db is present.
fn fixture_dir() -> Option<PathBuf> {
    let root = get_datasets_root()?;
    let table_root = root.join("sstables").join(KEYSPACE);
    let entries = std::fs::read_dir(&table_root).ok()?;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(&format!("{TABLE}-"))
            && entry.path().is_dir()
            && std::fs::read_dir(entry.path()).ok()?.flatten().any(|f| {
                f.file_name()
                    .to_str()
                    .is_some_and(|n| n.ends_with("-Data.db"))
            })
        {
            return Some(entry.path());
        }
    }
    None
}

/// Open the fixture with `use_mmap = true` so its scan uses the memory-mapped
/// backend whose reads fault synchronously — the exact backend F3 must offload.
async fn setup_db_mmap() -> Database {
    let mut core_config = cqlite_core::Config::default();
    core_config.storage.use_mmap = true;
    setup_db_with_config(core_config).await
}

/// Open the fixture requesting the `Direct` (`O_DIRECT` / `F_NOCACHE`) backend,
/// the OTHER synchronously-faulting backend F3 must offload (issue #1593). If the
/// test filesystem refuses direct I/O the reader degrades to buffered at open
/// (`faults_synchronously()` then returns false) — the calling test detects that
/// via a `None` recorded I/O-read thread and skips rather than asserting on the
/// buffered path.
#[cfg(unix)]
async fn setup_db_direct() -> Database {
    let mut core_config = cqlite_core::Config::default();
    core_config.storage.disk_access_mode = cqlite_core::config::DiskAccessMode::Direct;
    setup_db_with_config(core_config).await
}

async fn setup_db_with_config(core_config: cqlite_core::Config) -> Database {
    let datasets_root = get_datasets_root().expect("CQLITE_DATASETS_ROOT");
    let schemas_dir = get_schemas_dir().expect("schemas dir");
    let schema_path = schemas_dir.join(SCHEMA_FILE);
    assert!(schema_path.exists(), "schema not found: {schema_path:?}");

    let config = IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir: datasets_root.join("sstables"),
        version_hint: None,
        core_config,
        table_directory_filter: Some(format!("/{KEYSPACE}/")),
    };
    ingest(config)
        .await
        .expect("ingest wide_partition_table")
        .database
}

/// Enumerate the runtime's async worker `ThreadId`s by parking exactly
/// `WORKER_THREADS` tasks at a barrier so each must occupy a distinct worker.
async fn worker_thread_ids() -> HashSet<ThreadId> {
    let barrier = Arc::new(Barrier::new(WORKER_THREADS));
    let mut handles = Vec::with_capacity(WORKER_THREADS);
    for _ in 0..WORKER_THREADS {
        let b = Arc::clone(&barrier);
        handles.push(tokio::spawn(async move {
            let id = std::thread::current().id();
            b.wait();
            id
        }));
    }
    let mut ids = HashSet::new();
    for h in handles {
        ids.insert(h.await.expect("worker probe task"));
    }
    ids
}

/// Drain a single full streaming scan to completion, returning the row count.
/// `buffer_size = 1` maximizes per-partition backpressure.
async fn drain_one_scan(db: &Database, sql: &str) -> usize {
    let config = StreamingConfig {
        buffer_size: 1,
        ..StreamingConfig::default()
    };
    let mut iter = db
        .execute_streaming(sql, config)
        .await
        .expect("execute_streaming should succeed");
    let mut n = 0usize;
    while let Some(row) = iter.next_async().await {
        row.expect("streamed row should be Ok");
        n += 1;
    }
    n
}

/// The windowed streaming scan's blocking raw chunk read on an mmap backend must
/// run off the async worker pool (on a `spawn_blocking` thread), not inline on a
/// tokio worker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)] // keep == WORKER_THREADS
async fn mmap_scan_io_read_runs_off_async_worker_pool() {
    let Some(_dir) = fixture_dir() else {
        eprintln!(
            "Skipping {KEYSPACE}.{TABLE}: no Data.db present (run fetch-datasets.sh). \
             This guard is non-vacuous only with the real multi-chunk fixture."
        );
        return;
    };

    let workers = worker_thread_ids().await;
    assert_eq!(
        workers.len(),
        WORKER_THREADS,
        "expected to enumerate {WORKER_THREADS} distinct async worker threads, saw {}",
        workers.len()
    );

    let db = setup_db_mmap().await;
    let sql = format!("SELECT * FROM {KEYSPACE}.{TABLE}");

    scan_offload_probe::arm();
    let rows = drain_one_scan(&db, &sql).await;
    let io_read_thread = scan_offload_probe::recorded_io_read_thread();
    scan_offload_probe::disarm();

    // Non-vacuous: a present fixture must return rows AND must have routed through
    // the windowed (chunk-stitching) scan path that records the I/O read thread.
    assert!(
        rows > 0,
        "Issue #1593: {KEYSPACE}.{TABLE} is present but the streaming scan returned 0 rows — \
         guard would be vacuous"
    );
    let io_read_thread = io_read_thread.expect(
        "Issue #1593: scan_offload_probe recorded no I/O read thread — either the windowed \
         (chunk-stitching) scan path did not run, or the mmap backend was not selected \
         (file below mmap_min_size_bytes?). The guard must exercise the faulting-backend feed.",
    );

    eprintln!(
        "Issue #1593 I/O-offload guard: rows={rows} io_read_thread={io_read_thread:?} \
         async_workers={workers:?}"
    );

    assert!(
        !workers.contains(&io_read_thread),
        "Issue #1593 REGRESSION: the mmap-backed windowed scan's raw chunk read ran on async \
         worker thread {io_read_thread:?} (one of {workers:?}) instead of a spawn_blocking \
         thread. mmap page faults / O_DIRECT preads block the polling thread inside poll_read, \
         so K concurrent cold scans would pin the whole async pool and stall warm point reads. \
         Route the faulting-backend read loop onto spawn_blocking (F3)."
    );
}

/// Companion to the mmap guard: the `Direct` (`O_DIRECT` / `F_NOCACHE`) backend is
/// the OTHER `faults_synchronously()` backend that shares the identical
/// `feed_raw_chunks_blocking` `spawn_blocking` feed path (coverage gap, roborev
/// finding, issue #1593). It must likewise run its raw chunk read OFF the async
/// worker pool.
///
/// UNIX-gated (there is no `O_DIRECT`/`F_NOCACHE` on non-unix). Direct I/O is NOT
/// reliably provisionable in every test filesystem (tmpfs / overlayfs refuse
/// `O_DIRECT`), and the reader degrades GRACEFULLY to buffered at open when it is
/// refused. On that fallback the buffered feed runs inline on the async runtime by
/// design (it is genuinely async and does NOT fault synchronously), and never
/// records an I/O-read thread — so a recorded thread of `None` means "Direct was
/// refused; buffered fallback in effect" and we SKIP (documented, not a silent
/// omission) rather than asserting on a backend the environment could not provide.
/// When Direct IS provisioned, the recorded read thread must be OFF the worker set,
/// exactly like the mmap guard.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)] // keep == WORKER_THREADS
async fn direct_scan_io_read_runs_off_async_worker_pool() {
    let Some(_dir) = fixture_dir() else {
        eprintln!(
            "Skipping {KEYSPACE}.{TABLE}: no Data.db present (run fetch-datasets.sh). \
             This guard is non-vacuous only with the real multi-chunk fixture."
        );
        return;
    };

    let workers = worker_thread_ids().await;
    assert_eq!(
        workers.len(),
        WORKER_THREADS,
        "expected to enumerate {WORKER_THREADS} distinct async worker threads, saw {}",
        workers.len()
    );

    let db = setup_db_direct().await;
    let sql = format!("SELECT * FROM {KEYSPACE}.{TABLE}");

    scan_offload_probe::arm();
    let rows = drain_one_scan(&db, &sql).await;
    let io_read_thread = scan_offload_probe::recorded_io_read_thread();
    scan_offload_probe::disarm();

    // A present fixture must return rows regardless of backend.
    assert!(
        rows > 0,
        "Issue #1593: {KEYSPACE}.{TABLE} is present but the streaming scan returned 0 rows — \
         guard would be vacuous"
    );

    // No recorded I/O-read thread => the faulting-backend (spawn_blocking) feed
    // path did not run: Direct was refused and the reader fell back to buffered
    // (inline-on-async by design). Document and skip rather than fail — Direct is
    // not reliably provisionable in every CI filesystem.
    let Some(io_read_thread) = io_read_thread else {
        eprintln!(
            "Skipping Direct-backend offload assertion for {KEYSPACE}.{TABLE}: O_DIRECT/F_NOCACHE \
             was refused here, so the reader degraded to the buffered (inline-on-async) backend \
             (no I/O-read thread recorded). The Direct backend shares the identical \
             feed_raw_chunks_blocking spawn_blocking path proven by the mmap guard \
             (covered-by-construction); this environment could not exercise it directly."
        );
        return;
    };

    eprintln!(
        "Issue #1593 Direct-backend I/O-offload guard: rows={rows} io_read_thread={io_read_thread:?} \
         async_workers={workers:?}"
    );

    assert!(
        !workers.contains(&io_read_thread),
        "Issue #1593 REGRESSION: the Direct-backed windowed scan's raw chunk read ran on async \
         worker thread {io_read_thread:?} (one of {workers:?}) instead of a spawn_blocking \
         thread. O_DIRECT/F_NOCACHE preads block the polling thread inside poll_read, so K \
         concurrent cold scans would pin the whole async pool and stall warm point reads. \
         Route the faulting-backend read loop onto spawn_blocking (F3)."
    );
}
