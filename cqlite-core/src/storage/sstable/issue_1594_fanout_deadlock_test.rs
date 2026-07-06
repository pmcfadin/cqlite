//! Issue #1594 (Epic F, F4) — regression guard for the fan-out admission
//! DEADLOCK a per-sub-scan admission design introduced.
//!
//! ## The deadlock this pins
//!
//! [`SSTableManager::scan_stream`]'s lazy per-generation k-way merge opens ONE
//! windowed sub-scan per generation and primes a head from EVERY sub-scan before
//! draining any (it holds all N sub-scans open at once). A per-SUB-SCAN admission
//! design made each windowed sub-scan acquire its own blocking-pool permit, so a
//! single query that fans out to `N > cap` compressed generations deadlocked:
//! `cap` sub-scans won permits and parked in consumer backpressure (nobody
//! draining — the merge is still priming) while the remaining `N - cap` sub-scans
//! blocked forever at `admit()`; the priming merge, waiting on those blocked
//! sub-scans, never drained the permit-holders, so no permit ever freed. Permanent
//! hang. Reachable via schema-`None` multi-generation reads (the merge falls
//! through to the lazy fan-out at `scan_stream`, even in the default `write-support`
//! build), the merge-error fallback, and no-`write-support` builds.
//!
//! ## The fix this proves
//!
//! Admission is per top-level scan OPERATION: the fan-out merge acquires exactly
//! ONE permit for the whole operation and opens each sub-scan
//! `ScanAdmission::Exempt`, so a single query can never hold-and-wait on itself.
//!
//! ## What this test does (deterministic, no wall-clock race in the assertion)
//!
//! It builds a REAL multi-generation COMPRESSED table (opens a compressed
//! multi-chunk fixture reader and registers `GENERATIONS` clones under one table
//! key — each `scan_stream` opens its own independent cursor, so the fan-out drives
//! `GENERATIONS` genuinely-concurrent windowed sub-scans, the deadlocking
//! topology), installs a deliberately tiny admission cap of 1, and drives ONE full
//! scan via `manager.scan_stream(table_id, None, …)` with `schema = None` (forcing
//! the lazy fan-out on EVERY feature config). It asserts the scan COMPLETES
//! (returns all rows) within a generous hard timeout. Without the fix this hangs
//! forever (the timeout fires and the test FAILS); with it, the fan-out holds one
//! permit, its sub-scans are exempt, and the scan drains to completion.
//!
//! Requires the non-default `scan-offload-probe` feature (for `set_test_limit`),
//! `CQLITE_DATASETS_ROOT`, and the real multi-chunk compressed fixture
//! (skip-not-fail when absent; a present fixture returning zero rows is a FAILURE,
//! never a vacuous pass).
//!
//! [`SSTableManager::scan_stream`]: super::SSTableManager::scan_stream

#![cfg(all(
    test,
    feature = "state_machine",
    feature = "cli-helpers",
    feature = "scan-offload-probe"
))]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serial_test::serial;
use tokio::sync::RwLock;

use super::reader::scan_stream_windowed::scan_admission::probe as admission;
use super::SSTableManager;
use crate::schema::{AggregatorConfig, SchemaAggregator, SchemaRegistry, SchemaRegistryConfig};
use crate::types::TableId;
use crate::{Config, Platform};

const KEYSPACE: &str = "test_wide_rows";
const TABLE: &str = "wide_partition_table";
const SCHEMA_FILE: &str = "wide-rows.cql";
/// Deliberately tiny admission cap. With a per-sub-scan admission design, a
/// fan-out to `GENERATIONS > CAP` compressed generations deadlocks.
const CAP: usize = 1;
/// Number of concurrent windowed sub-scans the fan-out drives. Must exceed `CAP`.
const GENERATIONS: usize = 4;
/// Generous completion budget. A healthy scan finishes in well under a second; the
/// pre-fix deadlock never completes, so this bound only trips on a hang.
const COMPLETION_TIMEOUT: Duration = Duration::from_secs(60);

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

/// The single-generation compressed fixture directory, if a `-Data.db` and a
/// `-CompressionInfo.db` (chunk-stitching → the windowed scan path) are present.
fn compressed_fixture_dir() -> Option<PathBuf> {
    let root = get_datasets_root()?;
    let table_root = root.join("sstables").join(KEYSPACE);
    for entry in std::fs::read_dir(&table_root).ok()?.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with(&format!("{TABLE}-")) || !entry.path().is_dir() {
            continue;
        }
        let files: Vec<String> = std::fs::read_dir(entry.path())
            .ok()?
            .flatten()
            .filter_map(|f| f.file_name().to_str().map(str::to_owned))
            .collect();
        let has_data = files.iter().any(|n| n.ends_with("-Data.db"));
        let has_compression = files.iter().any(|n| n.ends_with("-CompressionInfo.db"));
        if has_data && has_compression {
            return Some(entry.path());
        }
    }
    None
}

/// Build a schema registry preloaded with the fixture's schema so the readers
/// parse rows (the sub-scans must produce rows to park on and reproduce the
/// deadlock topology).
async fn build_schema_registry(
    platform: Arc<Platform>,
    config: &Config,
) -> Arc<RwLock<SchemaRegistry>> {
    let schema_path = get_schemas_dir().expect("schemas dir").join(SCHEMA_FILE);
    assert!(schema_path.exists(), "schema not found: {schema_path:?}");
    let registry = Arc::new(RwLock::new(
        SchemaRegistry::new(
            SchemaRegistryConfig::default(),
            platform.clone(),
            config.clone(),
        )
        .await
        .expect("schema registry"),
    ));
    let udt_registry = registry.read().await.get_udt_registry();
    let mut aggregator = SchemaAggregator::new(
        registry.clone(),
        udt_registry,
        AggregatorConfig {
            graceful_degradation: false,
            validate_udt_dependencies: true,
        },
    );
    aggregator
        .load_from_paths(&[schema_path])
        .await
        .expect("load schema");
    registry
}

/// A single full scan over an `N > cap`-generation compressed table completes (no
/// hang) under a tiny admission cap.
///
/// RED (per-sub-scan admission): the fan-out opens `GENERATIONS` sub-scans, `CAP`
/// win permits and park in backpressure, the rest block forever at `admit()`, the
/// priming merge never drains the holders → permanent hang → `COMPLETION_TIMEOUT`
/// fires and this test FAILS. GREEN (per-operation admission): the fan-out holds
/// ONE permit, its sub-scans are `Exempt`, the scan drains to completion.
// `#[serial]` (serial_test): this guard installs a process-global admission cap
// via `set_test_limit` (which zeros `IN_FLIGHT`/`MAX_IN_FLIGHT`) and reads
// `max_in_flight`. Serializing it against the other probe counter tests (also
// `#[serial]`) prevents a concurrent test's permit-holder from underflowing the
// global counter across our `reset` when the new gate lib run executes them in one
// binary (issue #1594 roborev Low).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn fanout_over_more_generations_than_cap_completes() {
    let Some(fixture_dir) = compressed_fixture_dir() else {
        eprintln!(
            "Skipping {KEYSPACE}.{TABLE}: no compressed Data.db present (run fetch-datasets.sh). \
             This deadlock guard is non-vacuous only with the real multi-chunk compressed fixture."
        );
        return;
    };

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    let registry = build_schema_registry(platform.clone(), &config).await;

    // One generation on disk → one reader wired with the schema.
    let manager = SSTableManager::new(
        &fixture_dir,
        &config,
        platform,
        #[cfg(feature = "state_machine")]
        Some(registry),
    )
    .await
    .expect("manager");

    // Discover the table key + its reader, then register GENERATIONS clones under
    // that key. Each clone is the SAME Arc<SSTableReader>, but every `scan_stream`
    // opens its OWN scan cursor, so the fan-out drives GENERATIONS independent
    // concurrent windowed sub-scans (the deadlocking topology) without needing
    // GENERATIONS distinct on-disk generations.
    let (table_key, reader) = {
        let map = manager.table_readers.read().await;
        let (k, v) = map.iter().next().expect("one table discovered");
        assert!(!v.is_empty(), "table has at least one reader");
        (k.clone(), v[0].clone())
    };
    {
        let mut map = manager.table_readers.write().await;
        let entry = map.get_mut(&table_key).expect("table key present");
        *entry = vec![reader; GENERATIONS];
    }
    let table_id = TableId::new(table_key);

    // Force the tiny cap. GENERATIONS (4) > CAP (1): the fan-out needs more
    // concurrent sub-scans than the cap admits.
    admission::set_test_limit(CAP);

    // Drive the fan-out with schema = None → the lazy per-generation merge (mod.rs
    // falls through the schema-present branch), on EVERY feature config. Drain the
    // whole scan under a hard timeout; a hang (the pre-fix deadlock) never
    // completes, so the timeout is what catches the regression. `buffer_size = 1`
    // maximizes per-partition backpressure so sub-scans park mid-scan.
    let drained = tokio::time::timeout(COMPLETION_TIMEOUT, async {
        let mut rx = manager
            .scan_stream(&table_id, None, None, None, 1)
            .await
            .expect("scan_stream opens");
        let mut n = 0usize;
        while let Some(item) = rx.recv().await {
            item.expect("streamed row should be Ok");
            n += 1;
        }
        n
    })
    .await;

    let max_admitted = admission::max_in_flight();
    admission::clear_test_limit();

    let total_rows = drained.unwrap_or_else(|_| {
        panic!(
            "Issue #1594 DEADLOCK: a single full scan over {GENERATIONS} compressed generations \
             with admission cap {CAP} did NOT complete within {COMPLETION_TIMEOUT:?}. The fan-out \
             k-way merge opens one windowed sub-scan per generation and primes every head before \
             draining; with per-SUB-SCAN admission, {CAP} sub-scan(s) hold permits and park in \
             backpressure while the rest block forever at admit(), and the priming merge never \
             drains the holders (permanent hang). Admission must be per top-level scan OPERATION: \
             the fan-out holds ONE permit and its sub-scans are Exempt."
        )
    });

    eprintln!(
        "Issue #1594 fan-out deadlock guard: generations={GENERATIONS} cap={CAP} \
         max_admitted={max_admitted} total_rows={total_rows}"
    );

    // Non-vacuous: each of the GENERATIONS streams contributes the same rows in
    // this non-dedup fan-out, so a present fixture must return rows — proving the
    // sub-scans actually ran and produced output to park on.
    assert!(
        total_rows > 0,
        "Issue #1594: the multi-generation scan completed but returned 0 rows — the guard would \
         be vacuous (sub-scans never produced rows to park on, so the deadlock topology was not \
         exercised)"
    );
    // The whole fan-out is ONE operation holding ONE permit: admission never
    // exceeds the cap even though the operation runs GENERATIONS sub-scans. (Holds
    // regardless of interleaving with other tests: the cap is a global maximum.)
    assert!(
        max_admitted <= CAP,
        "Issue #1594 REGRESSION: {max_admitted} scans were admitted at once, exceeding the cap \
         of {CAP}. The fan-out sub-scans must be Exempt (the merge holds the single permit)."
    );
    // Wiring: the fan-out acquired its single operation-level permit.
    assert!(
        max_admitted >= 1,
        "Issue #1594: no scan was ever admitted — the fan-out merge's operation-level permit \
         acquisition is not wired"
    );
}
