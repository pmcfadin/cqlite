//! Issue #1940 (runtime-placement regression guard): the windowed streaming scan
//! must run its chunk DECOMPRESSION OFF the async worker pool — on a
//! `spawn_blocking` thread — for the **buffered** backend too, not just the
//! synchronously-faulting (mmap / `O_DIRECT`) backends.
//!
//! ## What regressed (and this guard pins)
//!
//! The D2 substrate change (moving decompression into the IO-half feed loop so a
//! decompressed refcounted `Bytes` substrate is shipped on the chunk channel, ≤1
//! alloc/chunk) initially left the BUFFERED backend's feed loop running inline on
//! the tokio async runtime (a plain `async fn` awaited on a worker). That put full
//! per-chunk decompression CPU back onto the async reactor for the buffered path —
//! the exact regression Epic F (#1143 parse-offload, #1593 IO-offload) exists to
//! prevent: heavy decode competing with everything else scheduled on the small
//! worker pool starves concurrent work (writer flush/compaction) and halves reader
//! throughput under mixed load.
//!
//! On `origin/main` (pre-D2) decompression ran in the parse half under
//! `spawn_blocking` for BOTH backends. The fix restructures the buffered feed loop
//! to run its read+decode on a blocking context (mirroring the faulting backend),
//! so the decode CPU never lands on the reactor for any backend.
//!
//! ## Why a thread-identity guard (not a wall-clock p99)
//!
//! Identical rationale to the #1143 parse-offload and #1593 IO-offload guards:
//! absolute latency thresholds are machine-dependent and flaky, and the corpus
//! fixtures are tiny, so a single scan never monopolizes a worker long enough to
//! observe wall-clock starvation deterministically. This guard pins the MECHANISM:
//! it records the `ThreadId` on which the scan's chunk decompression actually ran
//! (at the `decode_scan_chunk` decompress site, via the `scan-offload-probe`
//! compiled only under the non-default feature so it never ships) and asserts that
//! thread is NOT one of the async worker threads.
//!   - decode inline on async worker (regressed): decode runs ON a worker
//!     → recorded thread is in the worker set → FAIL;
//!   - decode under `spawn_blocking` (fixed): decode runs on a blocking-pool thread
//!     → recorded thread is OUTSIDE the worker set → PASS.
//!
//! Crucially this drives the **buffered** backend (`DiskAccessMode::Buffered`),
//! which is the path the D2 change regressed; the mmap/direct guard is #1593.
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

/// A multi-chunk (>1 CompressionInfo chunk), fully-compressed `nb` fixture, so the
/// windowed chunk-stitching scan path runs and decompresses more than one chunk.
const KEYSPACE: &str = "test_timeseries";
const TABLE: &str = "sensor_data";
const SCHEMA_FILE: &str = "time-series.cql";
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

/// Open the fixture with the BUFFERED backend explicitly selected — the
/// genuinely-async path the D2 substrate change regressed by leaving its feed loop
/// (and thus decompression) inline on the reactor. Forcing `Buffered` here also
/// keeps `faults_synchronously()` false, so the fix's routing decision must place
/// decode off the reactor by RESTRUCTURING the buffered feed, not merely by the
/// pre-existing faulting-backend branch.
async fn setup_db_buffered() -> Database {
    let datasets_root = get_datasets_root().expect("CQLITE_DATASETS_ROOT");
    let schemas_dir = get_schemas_dir().expect("schemas dir");
    let schema_path = schemas_dir.join(SCHEMA_FILE);
    assert!(schema_path.exists(), "schema not found: {schema_path:?}");

    let mut core_config = cqlite_core::Config::default();
    core_config.storage.use_mmap = false;
    core_config.storage.disk_access_mode = cqlite_core::config::DiskAccessMode::Buffered;

    let config = IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir: datasets_root.join("sstables"),
        version_hint: Some("5.0".to_string()),
        core_config,
        table_directory_filter: Some(format!("/{KEYSPACE}/")),
    };
    ingest(config).await.expect("ingest sensor_data").database
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

/// The BUFFERED-backend windowed streaming scan's decompression must run off the
/// async worker pool (on a `spawn_blocking` thread), not inline on a tokio worker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)] // keep == WORKER_THREADS
async fn buffered_scan_decode_runs_off_async_worker_pool() {
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

    let db = setup_db_buffered().await;
    let sql = format!("SELECT * FROM {KEYSPACE}.{TABLE}");

    scan_offload_probe::arm();
    let rows = drain_one_scan(&db, &sql).await;
    let decode_thread = scan_offload_probe::recorded_decode_thread();
    scan_offload_probe::disarm();

    // Non-vacuous: a present fixture must return rows AND must have routed through
    // the windowed (chunk-stitching) scan path that decompresses and records the
    // decode thread.
    assert!(
        rows > 0,
        "Issue #1940: {KEYSPACE}.{TABLE} is present but the streaming scan returned 0 rows — \
         guard would be vacuous"
    );
    let decode_thread = decode_thread.expect(
        "Issue #1940: scan_offload_probe recorded no decode thread — either the windowed \
         (chunk-stitching) scan path did not run, or the fixture was served entirely from the \
         B1 cache (no decompress). The guard must exercise a real per-chunk decompress on the \
         buffered feed.",
    );

    eprintln!(
        "Issue #1940 decode-offload guard (buffered): rows={rows} decode_thread={decode_thread:?} \
         async_workers={workers:?}"
    );

    assert!(
        !workers.contains(&decode_thread),
        "Issue #1940 REGRESSION: the buffered-backend windowed scan's chunk decompression ran on \
         async worker thread {decode_thread:?} (one of {workers:?}) instead of a spawn_blocking \
         thread. The D2 substrate change moved decompression into the IO-half feed loop; for the \
         buffered backend that loop ran inline on the reactor, putting full-scan decode CPU back \
         on the async worker pool (the Epic F regression). Restructure the buffered feed to run \
         its read+decode on a blocking context (mirroring the faulting backend)."
    );
}
