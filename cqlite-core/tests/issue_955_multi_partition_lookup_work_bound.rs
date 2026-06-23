//! Issue #955 (Epic #951): `WHERE pk IN (...)` must touch only candidate
//! SSTables, not every SSTable for the table.
//!
//! Mirrors the #958 single-partition work-bound gate, extended to the IN
//! fan-out. A synthetic table is built with N SSTable generations, each holding
//! one distinct partition key. An `IN` over a SMALL subset of those keys must
//! report `AccessPath::MultiPartitionLookup` and parse O(candidates) SSTables —
//! NOT all N. If the IN path regressed to a full scan, `sstables_scanned()`
//! would balloon to N and this gate fails.
//!
//! NOTE: excluded under `tombstones` for the same reason as #958 — that feature
//! switches `scan_partition` to the full-scan fallback and compiles out the
//! `work_counters` mutators.
//!
//! Run with:
//!   cargo test --package cqlite-core \
//!     --features write-support,cli-helpers,state_machine \
//!     --test issue_955_multi_partition_lookup_work_bound

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

const KS: &str = "in_work_ks";
const TBL: &str = "items";

/// Number of SSTable generations. The IN over a few keys must touch far fewer.
const N_GENERATIONS: usize = 8;

/// Number of distinct keys the IN query asks for.
const IN_KEYS: usize = 3;

/// Upper bound on `sstables_scanned()` for an IN over `IN_KEYS` keys, each in
/// exactly one generation. = `IN_KEYS` true holders + a small bloom
/// false-positive allowance, and crucially `< N_GENERATIONS` so a regression to
/// a full scan (which parses all 8) fails loudly.
const MAX_CANDIDATES_SCANNED: u64 = (IN_KEYS as u64) + 3;

fn make_schema_cql() -> String {
    format!("CREATE TABLE {KS}.{TBL} (\n  id int PRIMARY KEY,\n  name text\n);\n")
}

fn write_row(id: i32, name: &str, ts: i64) -> Mutation {
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![CellOperation::Write {
        column: "name".to_string(),
        value: Value::Text(name.to_string()),
    }];
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

/// Build `N_GENERATIONS` SSTables, each holding one partition (id = g*100 + 1),
/// and return the three target ids living in three distinct generations.
fn build_multi_generation_fixture(
    data_dir: &std::path::Path,
    wal_dir: &std::path::Path,
) -> Vec<i32> {
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
            .write(write_row(id, &format!("name-{id}"), ts))
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

    // Three keys in three distinct generations (generations 1, 3, 5).
    vec![101, 301, 501]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_lookup_touches_o_candidates_not_all_sstables() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");
    let schema_path = temp_dir.path().join("schema.cql");
    std::fs::write(&schema_path, make_schema_cql()).expect("write schema file");

    let target_ids = {
        let data_dir = data_dir.clone();
        let wal_dir = wal_dir.clone();
        tokio::task::spawn_blocking(move || build_multi_generation_fixture(&data_dir, &wal_dir))
            .await
            .expect("fixture build task")
    };
    assert_eq!(target_ids.len(), IN_KEYS);

    let result = ingest(IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir,
        version_hint: None,
        core_config: Config::default(),
        table_directory_filter: None,
    })
    .await
    .expect("ingest multi-generation fixture");
    assert!(result.schema_load_result.schemas_loaded >= 1);
    let db = Arc::new(result.database);

    let in_list = target_ids
        .iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(", ");

    work_counters::reset();
    let q = format!("SELECT id, name FROM {KS}.{TBL} WHERE id IN ({in_list})");
    let in_result = db.execute(&q).await.expect("IN lookup must succeed");

    // Signal 1: the executor took the multi-partition targeted path (#955/#960).
    assert_eq!(
        in_result.metadata.access_path,
        Some(AccessPath::MultiPartitionLookup),
        "Issue #955: WHERE id IN (...) must report MultiPartitionLookup, got {:?}",
        in_result.metadata.access_path
    );

    // Signal 2: the work counter proves O(candidates), not O(N) (#958-style).
    let scanned = work_counters::sstables_scanned();
    assert!(
        scanned <= MAX_CANDIDATES_SCANNED,
        "Issue #955: an IN over {IN_KEYS} keys across {N_GENERATIONS} SSTables must parse at most \
         {MAX_CANDIDATES_SCANNED} candidates (one per key + bloom false-positive allowance), but \
         it parsed {scanned}. A count near {N_GENERATIONS} means the IN fan-out regressed to a \
         full scan per key (the #955 behaviour this gate forbids).",
    );
    assert!(
        scanned >= IN_KEYS as u64,
        "each of the {IN_KEYS} present keys lives in its own generation, so at least {IN_KEYS} \
         candidates must be parsed (got {scanned})",
    );

    // Exactly the three requested partitions are returned.
    assert_eq!(
        in_result.rows.len(),
        IN_KEYS,
        "expected exactly {IN_KEYS} targeted partitions, got {}",
        in_result.rows.len()
    );

    drop(temp_dir);
}
