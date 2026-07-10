//! Issue #1940 (BLOCKER-1): opening a compressed windowed scan whose
//! `CompressionInfo.db` metadata stamps `max_compressed_length = i32::MAX` must
//! NOT attempt a multi-GB scratch allocation.
//!
//! ## What regressed (and this guard pins)
//!
//! The #1940 restructure's compressed feed pre-reserved the reused read scratch
//! ONCE to `max_compressed_length + 4` from the authoritative CompressionInfo
//! metadata, on the theory that this is the largest compressed-chunk RECORD any
//! chunk can occupy. But Cassandra COMMONLY sets `max_compressed_length` to
//! `i32::MAX` — it equals `i32::MAX` whenever `minCompressRatio == 0`, which is
//! the DEFAULT (CompressionParams.java:186-189). So opening an ORDINARY compressed
//! scan (16 KiB chunks, a ~120 KB file) attempted a ~2 GiB `Vec::reserve` BEFORE
//! any actual, bounds-checked chunk size was known — an OOM/DoS on normal files.
//!
//! The real `test_timeseries/sensor_data` fixture carries exactly this metadata
//! (`max_compressed_length == 2147483647`, 8 chunks of 16 KiB), so a full scan of
//! it reproduces the regression deterministically with real bytes.
//!
//! ## The fix (measured here)
//!
//! The feed no longer reserves against the metadata bound. It grows the scratch
//! on demand to each chunk's CHECKED `total_chunk_size` (verified against the real
//! Data.db length first) via `try_reserve_exact`. The largest single heap
//! allocation the whole scan performs is therefore bounded by the largest actual
//! compressed-chunk record (a small multiple of the 16 KiB chunk length), NEVER by
//! the `i32::MAX` metadata value.
//!
//! ## How it is measured
//!
//! A process-global allocator records the LARGEST single allocation/reallocation
//! size requested during the armed scan window. Under the old reserve that peak
//! was ≥ `i32::MAX` (~2 GiB); under the fix it stays far below a conservative
//! `MAX_SINGLE_ALLOC_BYTES` ceiling (a few MiB — comfortably above any legitimate
//! per-chunk or row-materialisation buffer for this tiny fixture, but ~1000x below
//! the 2 GiB the old reserve requested). The ceiling sits in the gap so the guard
//! fails closed if any single allocation approaches the metadata-driven size.
//!
//! **Requirements**: `CQLITE_DATASETS_ROOT` + the real multi-chunk compressed
//! `sensor_data` fixture. Skips (never fails) when absent; a present fixture that
//! returns zero rows is a FAILURE, never a vacuous pass. The counters are a
//! process-global, so this test serializes on `serial_test`.

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::result::StreamingConfig;
use cqlite_core::Database;
use serial_test::serial;

/// Records the LARGEST single allocation/reallocation size requested while ARMED.
/// A `Relaxed` load on the fast path and, when armed, a `max`-fetch — never
/// re-enters the allocator, so it is safe as the global allocator.
struct PeakSingleAllocTracker;

static ARMED: AtomicBool = AtomicBool::new(false);
static PEAK_SINGLE_ALLOC: AtomicUsize = AtomicUsize::new(0);

fn record(size: usize) {
    if ARMED.load(Ordering::Relaxed) {
        PEAK_SINGLE_ALLOC.fetch_max(size, Ordering::Relaxed);
    }
}

// SAFETY: every operation delegates verbatim to `System`; the only added work is a
// relaxed atomic load and (when armed) a `fetch_max`, neither of which allocates.
unsafe impl GlobalAlloc for PeakSingleAllocTracker {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record(new_size);
        System.realloc(ptr, layout, new_size)
    }
}

#[global_allocator]
static ALLOC: PeakSingleAllocTracker = PeakSingleAllocTracker;

/// Ceiling on the largest single heap allocation during the scan. The fixture's
/// chunks are 16 KiB (a few KiB compressed each); even generous row-materialisation
/// buffers for a ~120 KB file stay well under 8 MiB. The old metadata-driven
/// reserve requested `i32::MAX + 4` (~2 GiB), ~256x this ceiling — so this bound
/// sits firmly in the gap and fails closed on any reintroduced metadata-sized
/// allocation.
const MAX_SINGLE_ALLOC_BYTES: usize = 8 * 1024 * 1024;

const KEYSPACE: &str = "test_timeseries";
const TABLE: &str = "sensor_data";
const SCHEMA_FILE: &str = "time-series.cql";

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

fn schemas_dir() -> Option<PathBuf> {
    if let Some(root) = datasets_root() {
        if let Some(dir) = root.parent().and_then(|p| {
            let d = p.join("schemas");
            d.exists().then_some(d)
        }) {
            return Some(dir);
        }
    }
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dir = manifest_dir.parent()?.join("test-data").join("schemas");
    dir.exists().then_some(dir)
}

fn fixture_data_present() -> bool {
    let Some(root) = datasets_root() else {
        return false;
    };
    let Ok(entries) = std::fs::read_dir(root.join("sstables").join(KEYSPACE)) else {
        return false;
    };
    let prefix = format!("{TABLE}-");
    for e in entries.flatten() {
        if !e.file_name().to_string_lossy().starts_with(&prefix) {
            continue;
        }
        if let Ok(files) = std::fs::read_dir(e.path()) {
            for f in files.flatten() {
                if f.file_name().to_string_lossy().ends_with("-Data.db") {
                    return true;
                }
            }
        }
    }
    false
}

async fn setup() -> Option<Database> {
    let root = datasets_root()?;
    let schema_path = schemas_dir()?.join(SCHEMA_FILE);
    if !schema_path.exists() {
        return None;
    }
    let data_dir = root.join("sstables");
    if !data_dir.exists() {
        return None;
    }
    let config = IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir,
        version_hint: Some("5.0".to_string()),
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(format!("/{KEYSPACE}/")),
    };
    let result = ingest(config).await.ok()?;
    if result.schema_load_result.schemas_loaded == 0 {
        return None;
    }
    Some(result.database)
}

async fn drain_scan(db: &Database, sql: &str) -> usize {
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

#[tokio::test]
#[serial]
async fn compressed_scan_does_not_allocate_metadata_sized_scratch() {
    if !fixture_data_present() {
        eprintln!(
            "Skipping (#1940 no-metadata-OOM): {KEYSPACE}/{TABLE} Data.db not present \
             (run fetch-datasets.sh)"
        );
        return;
    }
    let Some(db) = setup().await else {
        eprintln!("Skipping (#1940 no-metadata-OOM): could not ingest {KEYSPACE}");
        return;
    };
    let sql = format!("SELECT * FROM {KEYSPACE}.{TABLE}");

    // Arm the peak tracker around a full scan. The fixture's CompressionInfo.db
    // carries max_compressed_length == i32::MAX (Cassandra's minCompressRatio=0
    // default), so the pre-fix feed reserved ~2 GiB up front on opening the scan;
    // the fix grows the scratch only to each CHECKED chunk size.
    PEAK_SINGLE_ALLOC.store(0, Ordering::SeqCst);
    ARMED.store(true, Ordering::SeqCst);
    let rows = drain_scan(&db, &sql).await;
    ARMED.store(false, Ordering::SeqCst);
    let peak = PEAK_SINGLE_ALLOC.load(Ordering::SeqCst);

    eprintln!(
        "#1940 no-metadata-OOM: {rows} rows, largest single allocation = {peak} bytes \
         (ceiling {MAX_SINGLE_ALLOC_BYTES})"
    );

    assert!(
        rows > 0,
        "present fixture must return rows (0 rows = read regression, not a skip)"
    );
    assert!(
        peak <= MAX_SINGLE_ALLOC_BYTES,
        "#1940 REGRESSION (BLOCKER-1): a single allocation of {peak} bytes occurred during a \
         compressed scan of a ~120 KB, 16 KiB-chunk fixture — far above the {MAX_SINGLE_ALLOC_BYTES}\
         -byte ceiling. The feed is reserving the scratch against the metadata \
         `max_compressed_length` (i32::MAX ~= 2 GiB for this fixture) instead of growing it to each \
         CHECKED per-chunk size. Grow-on-demand to the bounds-checked `total_chunk_size` via \
         `try_reserve_exact`, never a metadata-driven reserve (see scan_stream_windowed_read.rs)."
    );
}
