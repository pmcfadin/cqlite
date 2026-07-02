//! Issue #1333 (follow-up to #1046): the windowed streaming scan's per-partition
//! scratch buffer must be REUSED across partitions, not reallocated per
//! partition.
//!
//! ## What this guards
//!
//! `drain_scan_window` (`scan_stream_windowed.rs`) parses ONE confirmed partition
//! into a scratch `Vec<(RowKey, Value)>` before moving its entries into the
//! outgoing batch. That scratch used to be a fresh `Vec::new()` allocated INSIDE
//! the per-partition loop, so every non-empty partition allocated a new backing
//! store. Issue #1333 hoists it OUT of the loop into `drain_scan_window_blocking`
//! and `.clear()`-reuses it each partition; `clear()` preserves capacity, so a
//! warmed buffer performs ZERO per-partition backing allocations. This is the
//! #1046 mandate ("buffers should be reused, do not allocate as we iterate")
//! applied to the per-partition scratch.
//!
//! ## Why a scratch-growth counter (not a global dhat delta)
//!
//! The hoist saves ONE backing allocation per non-empty partition. Against the
//! full-scan allocation total (tens of allocations PER ROW for row
//! materialization, per #1046) that single per-partition allocation is a fraction
//! of a percent — far below the noise floor of a global `dhat` allocation-count
//! delta, so a whole-scan dhat assertion could not distinguish the hoisted from
//! the reallocating code without a flaky, razor-thin ceiling. Instead the
//! `scan-offload-probe` instrumentation counts the SCRATCH BUFFER's OWN growth
//! events directly (`recorded_scratch_allocs`): the Vec grows its backing store
//! only by reallocating, so this is exactly "how many times was the scratch
//! (re)allocated". This is the precise quantity the issue targets, measured
//! without noise.
//!
//! ## Why it FAILS if the hoist is reverted
//!
//! `test_basic.simple_table` is UUID-keyed with no clustering column, so every
//! row is its OWN partition (999 partitions on the pinned fixture). With the
//! scratch reused, it grows only while climbing to its high-water mark — a small
//! bounded count (`<= MAX_SCRATCH_GROWTHS`), INDEPENDENT of the 999 partitions.
//! Revert the hoist (a fresh `Vec::new()` per partition) and the buffer grows
//! from empty on EVERY non-empty partition, so the growth count jumps to ~999 —
//! two orders of magnitude past the bound, failing this guard loudly. The
//! assertion also requires the fixture to have produced far more partitions than
//! the bound, so it can never pass vacuously.
//!
//! Requirements:
//! - `CQLITE_DATASETS_ROOT` pointing to `test-data/datasets`
//! - real SSTable Data.db files (`bash test-data/scripts/fetch-datasets.sh`).
//!   Dataset-dependent: skips when Data.db is absent, but a present fixture that
//!   returns zero rows / does not route through the windowed path is a FAILURE.

// Requires the non-default `scan-offload-probe` feature: it gates the
// `scan_stream_windowed::probe` instrumentation this guard reads (issue #1143
// finding 1 / #1333). The agent gate runs this binary with the feature enabled.
#![cfg(all(
    feature = "state_machine",
    feature = "cli-helpers",
    feature = "scan-offload-probe"
))]

use std::path::PathBuf;

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::result::StreamingConfig;
use cqlite_core::storage::sstable::reader::scan_stream_windowed::probe as scan_offload_probe;
use cqlite_core::Database;

/// UUID-keyed, no-clustering table: one row per partition, so the fixture has as
/// many partitions as rows (999 on the pinned dataset) — the many-partition shape
/// this guard needs.
const KEYSPACE: &str = "test_basic";
const TABLE: &str = "simple_table";
const SCHEMA_FILE: &str = "basic-types.cql";

/// Upper bound on scratch-buffer growth events for a full scan when the scratch is
/// reused. The buffer grows only while climbing to its high-water mark: for this
/// fixture's single-row partitions that is a 0 -> small-capacity step that happens
/// essentially once, plus generous headroom for any legitimate high-water climb.
/// It is a small CONSTANT — crucially NOT proportional to partition count — so a
/// per-partition reallocation (the reverted code, ~999 growths) blows past it.
const MAX_SCRATCH_GROWTHS: usize = 16;

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
    ingest(config).await.expect("ingest simple_table").database
}

/// Drain a single full streaming scan to completion, returning the row count.
/// `buffer_size = 1` maximizes per-partition backpressure so the parse loop runs
/// in tight per-partition bursts — the exact shape that would reallocate a fresh
/// scratch buffer every partition if it were not hoisted.
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

/// The windowed streaming scan's per-partition scratch buffer must be reused, so
/// its backing-store growth count stays a small constant regardless of how many
/// partitions the scan drains.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scratch_buffer_is_reused_across_partitions() {
    let Some(_dir) = fixture_dir() else {
        eprintln!(
            "Skipping {KEYSPACE}.{TABLE}: no Data.db present (run fetch-datasets.sh). \
             This guard is non-vacuous only with the real many-partition fixture."
        );
        return;
    };

    let db = setup_db().await;
    let sql = format!("SELECT * FROM {KEYSPACE}.{TABLE}");

    // Arm the probe, run a full streaming scan over the many-partition fixture,
    // then read back how many times the scratch buffer grew.
    scan_offload_probe::arm();
    let rows = drain_one_scan(&db, &sql).await;
    // `drain_one_scan` returns only after the stream ends, which the consumer
    // observes only once the blocking parse task has dropped its sender — after
    // its last `note_scratch_capacity`. That channel-close happens-before ordering
    // makes every recorded growth visible to this read without a settle sleep.
    let scratch_growths = scan_offload_probe::recorded_scratch_allocs();
    scan_offload_probe::disarm();

    // Non-vacuous: a present fixture must return rows AND must have routed through
    // the windowed (chunk-stitching) parse path that arms the probe (a growth is
    // recorded once per partition, so a windowed scan of a non-empty table records
    // at least one).
    assert!(
        rows > 0,
        "Issue #1333: {KEYSPACE}.{TABLE} is present but the streaming scan returned \
         0 rows — guard would be vacuous"
    );
    assert!(
        scratch_growths >= 1,
        "Issue #1333: the scan returned {rows} rows but recorded 0 scratch-growth \
         events — the windowed (chunk-stitching) scan path that this guard \
         instruments did not run; fixture may not be the `nb` chunk-compressed \
         format the probe instruments"
    );

    // Every row is its own partition (UUID PK, no clustering), so partitions ==
    // rows. The bound must be far below the partition count or the guard is not
    // meaningful: if it held, the "constant vs per-partition" distinction would be
    // untestable on this fixture.
    assert!(
        rows > MAX_SCRATCH_GROWTHS * 4,
        "Issue #1333: fixture has only {rows} partitions; need many more than the \
         MAX_SCRATCH_GROWTHS={MAX_SCRATCH_GROWTHS} bound for the reuse assertion to \
         be meaningful (a per-partition realloc must be able to overshoot it)"
    );

    eprintln!(
        "Issue #1333 scratch-reuse guard: partitions(rows)={rows} \
         scratch_growths={scratch_growths} (bound {MAX_SCRATCH_GROWTHS})"
    );

    assert!(
        scratch_growths <= MAX_SCRATCH_GROWTHS,
        "Issue #1333 REGRESSION: the per-partition scratch buffer grew its backing \
         store {scratch_growths} times over a {rows}-partition scan (> \
         {MAX_SCRATCH_GROWTHS}). A reused scratch grows only up to its high-water \
         mark (a small constant); a count scaling with the {rows} partitions means \
         the scratch is being reallocated per partition. Hoist the scratch `Vec` \
         out of the per-partition loop in `drain_scan_window` and `.clear()`-reuse \
         it (see `drain_scan_window_blocking`)."
    );
}
