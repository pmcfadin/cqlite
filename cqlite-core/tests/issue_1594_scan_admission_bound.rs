//! Issue #1594 (Epic F, F4): the windowed streaming scan bounds how many scans
//! are admitted to tokio's blocking pool concurrently, fixing the priority
//! inversion where `K` concurrent cold scans pin `~2K` blocking threads (the F3
//! feed + parse pair per faulting-backend scan) and starve latency-critical
//! point-read fs ops.
//!
//! ## What this guards (wiring evidence)
//!
//! The top-level scan operation (`run_scan_stream` / the fan-out merge) acquires
//! ONE admission permit before spawning its `spawn_blocking` work and holds it
//! (RAII) for the whole scan. This guard
//! installs a LOW admission limit `L`, runs `N > L` full scans concurrently with
//! the `scan-offload-probe` in-flight instrumentation armed, and asserts the
//! recorded MAXIMUM number of concurrently-admitted scans never exceeds `L`.
//!
//! Deterministic — no wall-clock race. The assertion is the SAFETY bound
//! (`max_in_flight <= L`), which holds regardless of how the scans interleave; it
//! does NOT require the scans to reach exactly `L` simultaneously (that would be
//! timing-dependent). Non-vacuity is pinned separately: at least one scan must
//! have been admitted (`max_in_flight >= 1`, proving the admission path is wired
//! and the counter fires) and the fixture must return rows.
//!
//! On `main` (no admission mechanism) this test does not compile — the
//! `scan_admission::probe` surface does not exist. With admission wired but the
//! permit NOT acquired, `max_in_flight` would stay `0` and the `>= 1` assertion
//! fails. With the semaphore mis-sized larger than `L`, the `<= L` assertion
//! catches it when overlap reaches the cap.
//!
//! Requirements: `CQLITE_DATASETS_ROOT` pointing at `test-data/datasets` and the
//! real multi-chunk fixture (skip-not-fail when absent; a present fixture
//! returning zero rows is a FAILURE, never a vacuous pass).

// Requires the non-default `scan-offload-probe` feature: it gates the admission
// test surface (`set_test_limit` / in-flight counters) this guard reads. The
// agent gate runs this binary with the feature enabled.
#![cfg(all(
    feature = "state_machine",
    feature = "cli-helpers",
    feature = "scan-offload-probe"
))]

use std::path::PathBuf;
use std::sync::Arc;

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::result::StreamingConfig;
use cqlite_core::storage::sstable::reader::scan_stream_windowed::scan_admission::probe as admission;
use cqlite_core::Database;

const KEYSPACE: &str = "test_wide_rows";
const TABLE: &str = "wide_partition_table";
const SCHEMA_FILE: &str = "wide-rows.cql";
/// Admission limit installed for the test — deliberately small so `N > L` scans
/// contend for admission.
const LIMIT: usize = 2;
/// Number of concurrent full scans launched (must exceed `LIMIT`).
const CONCURRENT_SCANS: usize = 6;

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

/// Drain a single full streaming scan to completion, returning the row count.
/// `buffer_size = 1` maximizes per-partition backpressure so a scan stays "in
/// flight" (holding its admission permit) longer, encouraging overlap.
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

/// Concurrent windowed scans never exceed the installed admission limit, and the
/// admission path is demonstrably exercised (non-vacuous).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_scans_never_exceed_admission_limit() {
    let Some(_dir) = fixture_dir() else {
        eprintln!(
            "Skipping {KEYSPACE}.{TABLE}: no Data.db present (run fetch-datasets.sh). \
             This guard is non-vacuous only with the real multi-chunk fixture."
        );
        return;
    };

    let db = Arc::new(setup_db().await);
    let sql = format!("SELECT * FROM {KEYSPACE}.{TABLE}");

    // Install a low admission limit and reset the in-flight counters.
    admission::set_test_limit(LIMIT);

    // Launch N > L full scans concurrently. Each acquires an admission permit for
    // its whole duration; the semaphore caps concurrently-admitted scans at LIMIT.
    let mut handles = Vec::with_capacity(CONCURRENT_SCANS);
    for _ in 0..CONCURRENT_SCANS {
        let db = Arc::clone(&db);
        let sql = sql.clone();
        handles.push(tokio::spawn(async move { drain_one_scan(&db, &sql).await }));
    }

    let mut total_rows = 0usize;
    for h in handles {
        total_rows += h.await.expect("scan task joins");
    }

    let max_admitted = admission::max_in_flight();
    let residual = admission::current_in_flight();
    admission::clear_test_limit();

    eprintln!(
        "Issue #1594 admission guard: limit={LIMIT} concurrent_scans={CONCURRENT_SCANS} \
         max_admitted={max_admitted} total_rows={total_rows}"
    );

    // Non-vacuous: a present fixture must return rows across the scans.
    assert!(
        total_rows > 0,
        "Issue #1594: {KEYSPACE}.{TABLE} is present but the scans returned 0 rows total — \
         guard would be vacuous"
    );
    // Wiring: at least one scan was admitted (the admission path ran and the
    // counter fired). Zero here means the permit acquisition is not wired.
    assert!(
        max_admitted >= 1,
        "Issue #1594: no scan was ever recorded as admitted — the admission permit \
         acquisition in the top-level scan operation (run_scan_stream / the fan-out merge) \
         is not wired"
    );
    // The bound: concurrently-admitted scans never exceeded the installed limit.
    assert!(
        max_admitted <= LIMIT,
        "Issue #1594 REGRESSION: {max_admitted} windowed scans were admitted to the blocking \
         pool at once, exceeding the admission limit of {LIMIT}. Without admission control, \
         K concurrent cold scans pin ~2K blocking threads and starve point reads (priority \
         inversion). Bound concurrent windowed-scan admission with the scan_admission semaphore."
    );
    // Every permit was released (RAII on scan completion): no slot leaked.
    assert_eq!(
        residual, 0,
        "Issue #1594: {residual} admission permits were still held after all scans finished — \
         a scan leaked its admission slot instead of releasing it on completion"
    );
}
