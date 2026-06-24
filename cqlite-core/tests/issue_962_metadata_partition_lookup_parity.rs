//! Issue #962 (Epic #951): a fully-constrained `WHERE pk = ?` WRITETIME/TTL
//! metadata projection must use a partition-targeted lookup that prunes SSTables,
//! NOT a full table/SSTable scan.
//!
//! Before #962 the metadata-carrying scan path (triggered by `WRITETIME(col)` /
//! `TTL(col)` in the SELECT) always passed `None, None` to
//! `scan_with_cell_metadata`, opening and decoding every SSTable for the table
//! even when the partition key was fully pinned. #960 reported that honestly as
//! `FallbackFullScan { MetadataScanPath }` and pinned it with a test. #962 flips
//! the metadata branch to reuse the SAME `classify_partition_lookup` decision the
//! plain path uses, routing a single complete-pk equality through
//! `Storage::scan_partition_with_cell_metadata` (bloom/BTI prune + cross-generation
//! reconciliation + per-cell metadata).
//!
//! This file asserts the two properties #962 requires, on signals that a
//! correct-but-full-scanning regression cannot satisfy:
//!   1. **Work bound** (`work_counters::sstables_scanned`): a multi-generation
//!      fixture queried for a key in one generation must touch O(candidates), not
//!      O(N). A regression to a full metadata scan balloons this to N.
//!   2. **Access path** (`query::access_path`): the query must report
//!      `AccessPath::MetadataPartitionLookup`.
//!   3. **Parity**: the targeted metadata result (rows + WRITETIME values) must
//!      equal the full-scan-filtered metadata result (an unrestricted WRITETIME
//!      scan filtered in the test to the same key).
//!
//! The fixture is built deterministically in-process via the public write API
//! (one `flush()` per generation, no compaction), so it does not depend on
//! fetched binary datasets. Needs `write-support` (flush generations) +
//! `cli-helpers` + `state_machine` (the ingest/query stack).
//!
//! Excluded under `tombstones` for the same reason as issue #958: that feature
//! switches the manager to the full-scan fallback and compiles out the
//! `work_counters` mutators, so `sstables_scanned()` reads 0 and the prune-path
//! gate would spuriously fail under `--all-features`.

#![cfg(all(
    feature = "write-support",
    feature = "cli-helpers",
    feature = "state_machine",
    not(feature = "tombstones")
))]

use std::sync::Arc;

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::access_path::{self, AccessPath};
use cqlite_core::query::result::QueryRow;
use cqlite_core::storage::sstable::work_counters;
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use cqlite_core::Config;
use tempfile::TempDir;

const KS: &str = "meta_ks";
const TBL: &str = "items";

/// Number of SSTable generations the fixture writes. The single-partition
/// metadata read must touch O(1) candidates, NOT this many.
const N_GENERATIONS: usize = 8;

/// Upper bound on `sstables_scanned()` for a key in exactly one generation.
/// 1 true holder + a small allowance for bloom false positives, independent of
/// `N_GENERATIONS` (8). See issue #958's identical rationale.
const MAX_CANDIDATES_SCANNED: u64 = 3;

/// Serialize the work-counter / access-path tests in this file: both the
/// `work_counters` and `access_path::last()` globals are process-wide, so two
/// tests reading them concurrently would clobber each other.
static PROBE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn make_schema_cql() -> String {
    format!("CREATE TABLE {KS}.{TBL} (\n  id int PRIMARY KEY,\n  name text,\n  score int\n);\n")
}

fn write_row(id: i32, name: &str, score: i32, ts: i64) -> Mutation {
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![
        CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text(name.to_string()),
        },
        CellOperation::Write {
            column: "score".to_string(),
            value: Value::Integer(score),
        },
    ];
    Mutation::new(TableId::new(KS, TBL), pk, None, ops, ts, None)
}

fn count_data_files(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .expect("read sstable dir")
        .filter_map(|e| e.ok())
        .filter(|e| {
            let n = e.file_name();
            let n = n.to_string_lossy();
            n.ends_with("-big-Data.db") || n.ends_with("-Data.db")
        })
        .count()
}

/// Build `N_GENERATIONS` SSTables, each holding a disjoint single partition, and
/// return the key that lives in exactly one generation (the middle one).
fn build_multi_generation_fixture(data_dir: &std::path::Path, wal_dir: &std::path::Path) -> i32 {
    use cqlite_core::schema::parse_cql_schema;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let schema = parse_cql_schema(&make_schema_cql()).expect("parse fixture schema");
    let config = WriteEngineConfig::new(data_dir.to_path_buf(), wal_dir.to_path_buf(), schema);
    let mut engine = WriteEngine::new(config).expect("engine creation");

    for g in 0..N_GENERATIONS {
        let id = (g as i32) * 100 + 1;
        let ts = 100 + g as i64;
        engine
            .write(write_row(id, &format!("name-{id}"), id, ts))
            .expect("write row");
        rt.block_on(engine.flush())
            .expect("flush")
            .unwrap_or_else(|| panic!("generation {g} produced no SSTable"));
    }
    rt.block_on(engine.close()).expect("close engine");

    let sstable_dir = data_dir.join(KS).join(TBL);
    assert_eq!(
        count_data_files(&sstable_dir),
        N_GENERATIONS,
        "fixture must produce exactly {N_GENERATIONS} generations (no compaction)"
    );

    (N_GENERATIONS as i32 / 2) * 100 + 1
}

/// Open the full query stack over a freshly built multi-generation fixture and
/// return the DB plus the target key that lives in exactly one generation.
async fn open_fixture() -> (Arc<cqlite_core::Database>, i32, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    let schema_path = temp_dir.path().join("schema.cql");
    std::fs::write(&schema_path, make_schema_cql()).expect("write schema file");

    let target_id = {
        let data_dir = data_dir.clone();
        let wal_dir = wal_dir.clone();
        tokio::task::spawn_blocking(move || build_multi_generation_fixture(&data_dir, &wal_dir))
            .await
            .expect("fixture build task")
    };

    let result = ingest(IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir,
        version_hint: None,
        core_config: Config::default(),
        table_directory_filter: None,
    })
    .await
    .expect("ingest multi-generation fixture");
    assert!(
        result.schema_load_result.schemas_loaded >= 1,
        "schema must load"
    );
    (Arc::new(result.database), target_id, temp_dir)
}

fn writetime_of(row: &QueryRow, alias: &str) -> Option<i64> {
    match row.values.get(alias) {
        Some(Value::BigInt(v)) => Some(*v),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// 1. Work bound + access path: a single-partition WRITETIME read prunes SSTables.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_single_partition_read_touches_o1_sstables_not_n() {
    let _guard = PROBE_LOCK.lock().await;
    let (db, target_id, _temp) = open_fixture().await;

    work_counters::reset();
    access_path::reset();
    let q = format!("SELECT id, WRITETIME(name) FROM {KS}.{TBL} WHERE id = {target_id}");
    let targeted = db.execute(&q).await.expect("metadata lookup must succeed");

    // Signal 1: the executor took the partition-targeted metadata path (#962).
    assert_eq!(
        targeted.metadata.access_path,
        Some(AccessPath::MetadataPartitionLookup),
        "Issue #962: a fully-constrained WHERE id = ? WRITETIME query must report \
         MetadataPartitionLookup, got {:?}",
        targeted.metadata.access_path
    );
    assert_eq!(
        access_path::last(),
        Some(AccessPath::MetadataPartitionLookup)
    );

    // Signal 2: the work counter proves O(candidates), not O(N).
    let scanned = work_counters::sstables_scanned();
    assert!(
        scanned <= MAX_CANDIDATES_SCANNED,
        "Issue #962: a single-partition WRITETIME read over {N_GENERATIONS} SSTables must parse at \
         most {MAX_CANDIDATES_SCANNED} candidate(s) (bloom false-positive allowance), but parsed \
         {scanned}. A count near {N_GENERATIONS} means the metadata path regressed to a full scan.",
    );
    assert!(
        scanned >= 1,
        "the target key exists, so at least one candidate must have been parsed (got {scanned})",
    );

    assert_eq!(
        targeted.rows.len(),
        1,
        "expected exactly the one targeted partition, got {}",
        targeted.rows.len()
    );
}

// ---------------------------------------------------------------------------
// 2. Parity: targeted metadata result == full-scan-filtered metadata result.
//    Same rows AND same WRITETIME values, while avoiding the full SSTable scan.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_targeted_matches_full_scan_filtered() {
    let _guard = PROBE_LOCK.lock().await;
    let (db, target_id, _temp) = open_fixture().await;

    // Targeted metadata read (partition-pruned).
    let targeted = db
        .execute(&format!(
            "SELECT id, WRITETIME(name) AS wt FROM {KS}.{TBL} WHERE id = {target_id}"
        ))
        .await
        .expect("targeted metadata query");
    assert_eq!(
        targeted.metadata.access_path,
        Some(AccessPath::MetadataPartitionLookup),
    );

    // Full-scan metadata read, filtered in the test to the same key — the
    // correctness oracle the targeted path must match exactly.
    let full = db
        .execute(&format!("SELECT id, WRITETIME(name) AS wt FROM {KS}.{TBL}"))
        .await
        .expect("full metadata scan");
    assert!(
        full.metadata
            .access_path
            .as_ref()
            .map(|p| p.is_full_scan())
            .unwrap_or(false),
        "the unrestricted WRITETIME scan must report a full scan, got {:?}",
        full.metadata.access_path
    );

    let oracle: Vec<&QueryRow> = full
        .rows
        .iter()
        .filter(|r| matches!(r.values.get("id"), Some(Value::Integer(v)) if *v == target_id))
        .collect();

    assert_eq!(
        targeted.rows.len(),
        oracle.len(),
        "targeted metadata row count must equal the full-scan-filtered count",
    );
    assert_eq!(targeted.rows.len(), 1, "expected exactly one matching row");

    let t_wt = writetime_of(&targeted.rows[0], "wt");
    let o_wt = writetime_of(oracle[0], "wt");
    assert!(
        t_wt.is_some(),
        "targeted WRITETIME(name) must be present, got {:?}",
        targeted.rows[0].values.get("wt")
    );
    assert_eq!(
        t_wt, o_wt,
        "Issue #962: targeted WRITETIME must equal the full-scan WRITETIME for the same key",
    );
}

// ---------------------------------------------------------------------------
// 3. Honest fallback: a WRITETIME projection with NO usable restriction still
//    reports a full-scan fallback (not a faked targeted path).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_unrestricted_reports_full_scan_fallback() {
    let _guard = PROBE_LOCK.lock().await;
    let (db, _target_id, _temp) = open_fixture().await;

    access_path::reset();
    let result = db
        .execute(&format!("SELECT id, WRITETIME(name) FROM {KS}.{TBL}"))
        .await
        .expect("unrestricted metadata scan");

    let path = result
        .metadata
        .access_path
        .clone()
        .expect("a metadata SELECT must report an access path");
    assert!(
        path.is_full_scan(),
        "Issue #962: an unrestricted WRITETIME projection must report a full scan, got {path:?}",
    );
}
