//! Issue #1589 (E7 — window-drain cursor): the sliding-window scan/compaction
//! drivers must consume confirmed partitions with a FRONT CURSOR, not
//! `window.drain(0..consumed)`.
//!
//! ## What this guards
//!
//! `drain_scan_window` (`scan_stream_windowed.rs`) and `drain_compaction_window`
//! (`data_access/compaction.rs`) parse CONFIRMED partitions from the FRONT of a
//! `window: Vec<u8>` and used to remove each one with `window.drain(0..consumed)`.
//! `Vec::drain` at the front memmoves the ENTIRE residual tail — so a
//! partition-dense window (thousands of one-row partitions packed into one window)
//! moves Θ(P·W) bytes: for each of P partitions it shifts the ~W-byte remainder
//! down. The fix advances a cursor instead and compacts the reclaimed prefix ONCE
//! per refill, so each byte is physically moved at most ~once per window — Θ(W).
//!
//! ## The measured invariant (why it fails today, passes after the fix)
//!
//! The `scan-offload-probe` instrumentation counts total bytes physically memmoved
//! by the window (`recorded_bytes_memmoved`) and total decompressed bytes appended
//! (`recorded_bytes_appended`). On a partition-dense fixture:
//!
//! - **Before the fix (front-drain):** each confirmed partition memmoves the whole
//!   residual tail, so bytes_memmoved grows ~quadratically in partitions-per-window
//!   and is many times bytes_appended. `bytes_memmoved <= bytes_appended` FAILS.
//! - **After the fix (cursor + compact-once-per-refill):** the window compacts only
//!   the surviving straddle once per chunk refill, so bytes_memmoved is a small
//!   fraction of bytes_appended (each byte moved at most ~once). The bound PASSES
//!   with a large margin.
//!
//! `test_basic.simple_table` is UUID-keyed with no clustering column, so every row
//! is its OWN partition (999 partitions on the pinned fixture) — the partition-dense
//! shape this guard needs. Both the user-facing windowed scan and the streaming
//! compaction driver refill the same window abstraction, so the guard exercises
//! BOTH sites (issue #1589 touches both, kept aligned).
//!
//! Requirements:
//! - `CQLITE_DATASETS_ROOT` pointing to `test-data/datasets`
//! - real SSTable Data.db files (`bash test-data/scripts/fetch-datasets.sh`).
//!   Dataset-dependent: skips when Data.db is absent, but a present fixture that
//!   returns zero rows / does not route through the windowed path is a FAILURE.

// Requires the non-default `scan-offload-probe` feature: it gates the
// `window_cursor::probe` instrumentation this guard reads (issue #1589). The agent
// gate runs this binary with the feature enabled.
#![cfg(all(
    feature = "state_machine",
    feature = "cli-helpers",
    feature = "scan-offload-probe"
))]

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::platform::Platform;
use cqlite_core::query::result::StreamingConfig;
use cqlite_core::storage::scan_cancel::ScanCancel;
use cqlite_core::storage::sstable::reader::window_cursor::probe;
use cqlite_core::storage::sstable::SSTableReader;
use cqlite_core::{Config, Database};

/// UUID-keyed, no-clustering table: one row per partition, so the fixture has as
/// many partitions as rows (999 on the pinned dataset) — the partition-dense shape
/// this guard needs.
const KEYSPACE: &str = "test_basic";
const TABLE: &str = "simple_table";
const SCHEMA_FILE: &str = "basic-types.cql";

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
    schemas_dir.exists().then_some(schemas_dir)
}

/// Directory holding the fixture's SSTable components, if a Data.db is present.
fn fixture_dir() -> Option<PathBuf> {
    let root = get_datasets_root()?;
    let table_root = root.join("sstables").join(KEYSPACE);
    for entry in std::fs::read_dir(&table_root).ok()?.flatten() {
        let name = entry.file_name();
        if name.to_string_lossy().starts_with(&format!("{TABLE}-"))
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

/// Path to the fixture's Data.db, if present.
fn fixture_data_db() -> Option<PathBuf> {
    let dir = fixture_dir()?;
    std::fs::read_dir(&dir).ok()?.flatten().find_map(|f| {
        f.file_name()
            .to_str()
            .is_some_and(|n| n.ends_with("-Data.db"))
            .then(|| f.path())
    })
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
        core_config: Config::default(),
        table_directory_filter: Some(format!("/{KEYSPACE}/")),
    };
    ingest(config).await.expect("ingest simple_table").database
}

async fn open_reader(data_db: &Path) -> SSTableReader {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    SSTableReader::open(data_db, &config, platform)
        .await
        .expect("open reader")
}

/// Drain a single full streaming scan to completion, returning the row count.
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

/// Assert the byte-movement bound, printing the observed figures. `context` names
/// the driver (scan vs compaction) so a failure is unambiguous.
fn assert_bytes_moved_bounded(context: &str, units: usize, appended: usize, memmoved: usize) {
    // Non-vacuous: the fixture must have produced rows AND routed through the
    // chunk-stitching window path (the only path that appends into the window /
    // arms this probe). Zero appended bytes means a non-stitching fallback ran and
    // the guard would be meaningless.
    assert!(
        units > 0,
        "Issue #1589 [{context}]: fixture is present but produced 0 units — guard would be vacuous"
    );
    assert!(
        appended > 0,
        "Issue #1589 [{context}]: 0 bytes appended into the window — the chunk-stitching \
         window path this guard instruments did not run (fixture may not be the `nb` \
         chunk-compressed format)"
    );

    eprintln!(
        "Issue #1589 [{context}] window byte-movement: units={units} \
         bytes_appended={appended} bytes_memmoved={memmoved} \
         (ratio memmoved/appended = {:.2})",
        memmoved as f64 / appended as f64
    );

    // The load-bearing assertion. Front-drain moves the whole residual tail per
    // partition (Θ(P·W)) and blows far past bytes_appended on a partition-dense
    // fixture; the cursor + compact-once-per-refill moves each byte at most ~once,
    // so total bytes memmoved stays at or below bytes appended (with large margin).
    assert!(
        memmoved <= appended,
        "Issue #1589 REGRESSION [{context}]: the window memmoved {memmoved} bytes over a \
         {units}-unit run while only {appended} bytes were appended — the front-drain \
         (`window.drain(0..consumed)`) is moving the residual tail once PER PARTITION \
         (Θ(P·W)). Replace it with a front cursor that advances on consume and compacts \
         the reclaimed prefix ONCE per refill (see WindowCursor, issue #1589)."
    );
}

/// Both the user-facing windowed scan AND the streaming compaction driver must
/// move O(appended) bytes, not Θ(P·W) (issue #1589 touches both, kept aligned).
///
/// Combined into ONE test: the byte-movement probe is a process-global counter, so
/// two probe tests in this single binary would race if run in parallel. Driving
/// scan then compaction sequentially (arm/measure/disarm around each) keeps the
/// counters isolated without depending on the harness thread count.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scan_and_compaction_windows_move_bytes_bounded_by_appended() {
    let Some(data_db) = fixture_data_db() else {
        eprintln!(
            "Skipping {KEYSPACE}.{TABLE}: no Data.db present (run fetch-datasets.sh). \
             This guard is non-vacuous only with the real partition-dense fixture."
        );
        return;
    };

    // --- Scan path -------------------------------------------------------------
    let db = setup_db().await;
    let sql = format!("SELECT * FROM {KEYSPACE}.{TABLE}");

    probe::arm();
    let rows = drain_one_scan(&db, &sql).await;
    // `drain_one_scan` returns only after the stream ends, which the consumer
    // observes only once the blocking parse task dropped its sender — after its
    // last window refill/consume. That channel-close happens-before ordering makes
    // every recorded byte-move visible to this read without a settle sleep.
    let scan_memmoved = probe::recorded_bytes_memmoved();
    let scan_appended = probe::recorded_bytes_appended();
    probe::disarm();

    assert_bytes_moved_bounded("scan", rows, scan_appended, scan_memmoved);

    // --- Streaming compaction path (production k-way-merge feed) ---------------
    let reader = open_reader(&data_db).await;
    let count: Arc<Mutex<usize>> = Arc::new(Mutex::new(0));
    let sink = Arc::clone(&count);

    probe::arm();
    reader
        .stream_all_partitions_for_compaction(None, &ScanCancel::default(), move |_row| {
            *sink.lock().expect("count lock") += 1;
            Ok(std::ops::ControlFlow::Continue(()))
        })
        .await
        .expect("stream_all_partitions_for_compaction");
    let comp_memmoved = probe::recorded_bytes_memmoved();
    let comp_appended = probe::recorded_bytes_appended();
    probe::disarm();

    let emitted = *count.lock().expect("count lock");
    assert_bytes_moved_bounded("compaction", emitted, comp_appended, comp_memmoved);
}
