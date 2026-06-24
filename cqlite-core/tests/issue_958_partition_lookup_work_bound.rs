//! Issue #958 (Epic #951): a single-partition `WHERE pk = ?` read must NOT open
//! and parse every SSTable.
//!
//! #949 added a partition-targeted lookup that prunes SSTables via the bloom
//! filter / BTI trie before parsing. Correct result rows do not prove the prune
//! happened — a regression could revert to "open and scan every SSTable, then
//! filter in memory" and still return the right answer. This test makes that
//! regression a HARD CI failure by asserting the *work* a single-partition read
//! does, on two independent signals:
//!
//!   1. **Work counters** (`storage::sstable::work_counters`): a synthetic table
//!      backed by N SSTable generations is queried for a key living in exactly
//!      one generation. `sstables_scanned()` must stay O(candidates) — well below
//!      N — not grow with N. If pruning regresses, this count balloons to N and
//!      the test fails.
//!   2. **Access path** (`query::access_path`): the same query must report
//!      `AccessPath::PartitionLookup` (issue #960), proving the executor actually
//!      took the targeted path and not a full scan that happened to return the
//!      same rows.
//!
//! The fixture is built deterministically in-process via the public write API
//! (one `flush()` per generation, no compaction), so the test does not depend on
//! fetched binary datasets. It needs `write-support` (to flush generations),
//! `cli-helpers` + `state_machine` (the ingest/query stack).
//!
//! NOTE: excluded under `tombstones`. That feature switches
//! `SSTableManager::scan_partition` to the full-scan fallback and compiles out the
//! `work_counters` mutators, so `sstables_scanned()` would read 0 and this
//! prune-path gate would spuriously fail under `--all-features`.
//!
//! Run with:
//!   cargo test --package cqlite-core \
//!     --features write-support,cli-helpers,state_machine \
//!     --test issue_958_partition_lookup_work_bound

#![cfg(all(
    feature = "write-support",
    feature = "cli-helpers",
    feature = "state_machine",
    not(feature = "tombstones")
))]

use std::sync::Arc;

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::access_path::AccessPath;
use cqlite_core::storage::sstable::work_counters;
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use cqlite_core::Config;
use tempfile::TempDir;

const KS: &str = "work_ks";
const TBL: &str = "items";

/// Number of SSTable generations the fixture writes. The single-partition read
/// must touch O(1) candidates, NOT this many.
const N_GENERATIONS: usize = 8;

/// Upper bound on `sstables_scanned()` for a key in exactly one generation.
///
/// Why `3` is safe (and why it is strictly less than `N_GENERATIONS = 8`):
/// - The target key is written into exactly ONE generation, so at most one
///   SSTable truly *contains* it.
/// - Pruning is by bloom filter (BIG/`nb` format). A bloom filter has **no false
///   negatives** — a generation that lacks the key can register `false` and is
///   skipped without parsing — but may yield a **false positive**, admitting an
///   extra candidate that is then parsed and filtered out. (Observed on this
///   fixture: exactly one false positive, so 2 candidates are parsed.)
/// - The bound `3` = 1 true holder + a small allowance for bloom false positives.
///   It is a constant independent of `N_GENERATIONS`, so the gate enforces the
///   essential property — sub-linear scaling — and a regression to a full scan
///   (which would parse all 8) fails loudly. The headroom over the observed 2
///   keeps the gate robust without weakening it (3 << 8).
const MAX_CANDIDATES_SCANNED: u64 = 3;

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

/// Build `N_GENERATIONS` SSTables under `data_dir`, each holding a disjoint set
/// of partition keys (one row per generation), and return the data dir + a key
/// that lives in exactly one generation (the middle one).
fn build_multi_generation_fixture(data_dir: &std::path::Path, wal_dir: &std::path::Path) -> i32 {
    use cqlite_core::schema::parse_cql_schema;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let schema = parse_cql_schema(&make_schema_cql()).expect("parse fixture schema");
    let config = WriteEngineConfig::new(data_dir.to_path_buf(), wal_dir.to_path_buf(), schema);
    let mut engine = WriteEngine::new(config).expect("engine creation");

    // Each generation g (0..N) holds the single partition id = g*100 + 1. The key
    // we later look up (id = (N/2)*100 + 1) therefore lives in exactly ONE of the
    // N SSTables; pruning must skip the other N-1.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_partition_read_touches_o1_sstables_not_n() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    let schema_path = temp_dir.path().join("schema.cql");
    std::fs::write(&schema_path, make_schema_cql()).expect("write schema file");

    // Build the fixture on a blocking thread (it spins its own runtime).
    let target_id = {
        let data_dir = data_dir.clone();
        let wal_dir = wal_dir.clone();
        tokio::task::spawn_blocking(move || build_multi_generation_fixture(&data_dir, &wal_dir))
            .await
            .expect("fixture build task")
    };

    // Open the full query stack over the multi-generation directory.
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
    let db = Arc::new(result.database);

    // ── Targeted single-partition read for a key in exactly one generation ──────
    work_counters::reset();
    let q = format!("SELECT id, name, score FROM {KS}.{TBL} WHERE id = {target_id}");
    let targeted = db.execute(&q).await.expect("targeted lookup must succeed");

    // Signal 1: the executor took the partition-targeted path (issue #960).
    assert_eq!(
        targeted.metadata.access_path,
        Some(AccessPath::PartitionLookup),
        "Issue #958: a fully-constrained WHERE id = ? must report PartitionLookup, got {:?}",
        targeted.metadata.access_path
    );

    // Signal 2: the work counter proves O(candidates), not O(N) (issue #958).
    let scanned = work_counters::sstables_scanned();
    assert!(
        scanned <= MAX_CANDIDATES_SCANNED,
        "Issue #958: a single-partition read over {N_GENERATIONS} SSTables must parse at most \
         {MAX_CANDIDATES_SCANNED} candidate(s) (bloom false-positive allowance), but it parsed \
         {scanned}. A count near {N_GENERATIONS} means SSTable pruning regressed and every \
         SSTable is being opened for one partition (the #949 behaviour this gate forbids).",
    );
    assert!(
        scanned >= 1,
        "the target key exists, so at least one candidate must have been parsed (got {scanned})",
    );

    // The targeted read returns exactly the one partition it asked for.
    assert_eq!(
        targeted.rows.len(),
        1,
        "expected exactly the one targeted partition, got {}",
        targeted.rows.len()
    );
    let parsed = work_counters::partitions_parsed();
    assert!(
        parsed <= MAX_CANDIDATES_SCANNED,
        "Issue #958: partitions_parsed must be O(1) for a point lookup, got {parsed}",
    );

    // ── Control: an absent key prunes to zero candidates (no SSTable parsed) ────
    work_counters::reset();
    let absent_id = 999_999;
    let absent = db
        .execute(&format!("SELECT id FROM {KS}.{TBL} WHERE id = {absent_id}"))
        .await
        .expect("absent-key lookup must succeed");
    assert!(absent.rows.is_empty(), "absent key must return no rows");
    let scanned_absent = work_counters::sstables_scanned();
    assert!(
        scanned_absent <= MAX_CANDIDATES_SCANNED,
        "Issue #958: an absent-key lookup must prune (near-)all SSTables, but parsed \
         {scanned_absent} of {N_GENERATIONS}",
    );

    drop(temp_dir);
}
