//! Issue #1143 (regression guard for PR #1156): the windowed streaming
//! `SELECT *` scan must run its CPU-bound work (decompress +
//! `parse_one_partition_with_timestamps`) OFF the async worker pool, on a
//! `spawn_blocking` thread.
//!
//! ## What regressed
//!
//! PR #1156 reworked the V5CompressedLegacy full-scan path into a bounded
//! sliding-window stitch+parse (`run_scan_stream_windowed` /
//! `drain_scan_window`, `scan_stream_windowed.rs`). That change bundled TWO
//! things:
//!   1. a bounded `window: Vec<u8>` (peak `max_partition_size + one_chunk`
//!      instead of O(file)) — correct, kept;
//!   2. moving the decompress+parse work from a dedicated `spawn_blocking`
//!      thread (the pre-#1156 `parse_stitched_stream`) ONTO the async worker
//!      pool, relying only on `tokio::task::yield_now()` between partitions.
//!
//! Change (2) is the regression: heavy decode/parse on the small async worker
//! pool competes with everything else scheduled there; cooperative `yield_now()`
//! is far weaker isolation than a dedicated blocking pool, so concurrent work
//! (writer flush/compaction in production) starves and the reader distribution
//! shifts (reader scans/s halved in the measured A/B).
//!
//! ## Why a thread-identity guard (not an absolute p99 / timing threshold)
//!
//! `read_while_write.rs` (criterion) measures but does not ASSERT, and absolute
//! latency thresholds are machine-dependent and flaky — that bench went green
//! and missed this regression. A *timing*-based starvation guard is also
//! unreliable here: the test corpus fixtures are tiny (tens of KiB), so a single
//! scan's parse is sub-millisecond and never monopolizes a worker long enough to
//! observe wall-clock starvation (the regression only surfaced under sustained
//! 6-reader load against far larger production data).
//!
//! So this guard pins the MECHANISM directly and deterministically: it records
//! the `ThreadId` on which the windowed scan's parse actually ran (via the
//! `scan_offload_probe`, compiled only under the non-default
//! `scan-offload-probe` feature so the instrumentation never ships in a normal
//! build — issue #1143 finding 1), and asserts that thread is NOT one of the
//! tokio async worker threads. This is exactly the distinction the fix turns on:
//!   - inline-on-async-worker parse (regressed): parse runs ON a worker thread
//!     → recorded thread is in the worker set → FAIL;
//!   - `spawn_blocking` offload (fixed): parse runs on a blocking-pool thread
//!     → recorded thread is OUTSIDE the worker set → PASS.
//!
//! Requirements:
//! - `CQLITE_DATASETS_ROOT` pointing to `test-data/datasets`
//! - real SSTable Data.db files (`bash test-data/scripts/fetch-datasets.sh`).
//!   Dataset-dependent: skips when Data.db is absent, but a present fixture that
//!   returns zero rows is a FAILURE (never vacuously passes).

// Requires the non-default `scan-offload-probe` feature: that gates the
// `scan_stream_windowed::probe` instrumentation this guard reads (issue #1143
// finding 1). The agent gate runs this binary with the feature enabled.
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
/// `#[tokio::test(... worker_threads = N)]` attribute below (the macro requires a
/// literal, so they are kept in sync by hand).
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

async fn setup_db() -> Database {
    let datasets_root = get_datasets_root().expect("CQLITE_DATASETS_ROOT");
    let schemas_dir = get_schemas_dir().expect("schemas dir");
    let schema_path = schemas_dir.join(SCHEMA_FILE);
    assert!(schema_path.exists(), "schema not found: {schema_path:?}");

    let config = IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir: datasets_root.join("sstables"),
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(format!("/{KEYSPACE}/")),
    };
    ingest(config)
        .await
        .expect("ingest wide_partition_table")
        .database
}

/// Enumerate the runtime's async worker `ThreadId`s by parking exactly
/// `WORKER_THREADS` tasks at a barrier so each must occupy a distinct worker,
/// each recording its own thread id. (A task can only make progress to the
/// barrier if scheduled on a worker; with all workers held simultaneously we
/// observe the full worker set.)
async fn worker_thread_ids() -> HashSet<ThreadId> {
    let barrier = Arc::new(Barrier::new(WORKER_THREADS));
    let mut handles = Vec::with_capacity(WORKER_THREADS);
    for _ in 0..WORKER_THREADS {
        let b = Arc::clone(&barrier);
        handles.push(tokio::spawn(async move {
            let id = std::thread::current().id();
            // Block this worker until all workers are here (forces distinct
            // workers). Blocking in a task is fine for this one-shot probe.
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
/// `buffer_size = 1` maximizes per-partition backpressure so the parse loop runs
/// in tight bursts (the worst case for an inline-on-worker parse).
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

/// The windowed streaming scan's decompress+parse must run off the async worker
/// pool (on a `spawn_blocking` thread), not inline on a tokio worker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)] // keep == WORKER_THREADS
async fn streaming_scan_parse_runs_off_async_worker_pool() {
    let Some(_dir) = fixture_dir() else {
        eprintln!(
            "Skipping {KEYSPACE}.{TABLE}: no Data.db present (run fetch-datasets.sh). \
             This guard is non-vacuous only with the real multi-chunk fixture."
        );
        return;
    };

    // Capture the runtime's async worker thread ids up front.
    let workers = worker_thread_ids().await;
    assert_eq!(
        workers.len(),
        WORKER_THREADS,
        "expected to enumerate {WORKER_THREADS} distinct async worker threads, saw {}",
        workers.len()
    );

    let db = setup_db().await;
    let sql = format!("SELECT * FROM {KEYSPACE}.{TABLE}");

    // Arm the probe, run a full streaming scan over the multi-chunk fixture,
    // then read back the thread that ran the parse.
    scan_offload_probe::arm();
    let rows = drain_one_scan(&db, &sql).await;
    // No settle sleep needed: `record_parse_thread()` stores the ThreadId under
    // the probe Mutex strictly BEFORE the parse closure drops its output sender,
    // and `drain_one_scan` returns only after the stream ends — which is observed
    // only once every sender (including that one) has dropped. That channel-close
    // happens-before ordering already guarantees the recorded thread is visible
    // to this read (issue #1143 finding 3).
    let parse_thread = scan_offload_probe::recorded_parse_thread();
    scan_offload_probe::disarm();

    // Non-vacuous: a present fixture must return rows AND must have routed
    // through the windowed (chunk-stitching) parse path that arms the probe.
    assert!(
        rows > 0,
        "Issue #1143: {KEYSPACE}.{TABLE} is present but the streaming scan \
         returned 0 rows — guard would be vacuous"
    );
    let parse_thread = parse_thread.expect(
        "Issue #1143: scan_offload_probe recorded no parse thread — the windowed \
         (chunk-stitching) scan path did not run; fixture may not be the `nb` \
         chunk-compressed format the probe instruments",
    );

    eprintln!(
        "Issue #1143 offload guard: rows={rows} parse_thread={parse_thread:?} \
         async_workers={workers:?}"
    );

    assert!(
        !workers.contains(&parse_thread),
        "Issue #1143 REGRESSION: the windowed scan's decompress/parse ran on async \
         worker thread {parse_thread:?} (one of {workers:?}) instead of a \
         spawn_blocking thread. PR #1156 ran the CPU-bound parse inline on the async \
         worker pool, starving concurrent work (writer flush/compaction) and halving \
         reader throughput. Move the parse back onto spawn_blocking."
    );
}
