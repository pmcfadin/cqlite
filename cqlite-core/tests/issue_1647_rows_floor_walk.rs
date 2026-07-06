//! Issue #1647 (Epic L / L1): a BTI (`da`) clustering read locates its row-index
//! block(s) with an O(key-length) `Rows.db` separator-floor walk instead of
//! materializing EVERY row-index block and linearly filtering.
//!
//! Wiring evidence, driven through the public `Database` API against the BTI
//! wide-partition fixture `test_da.wide_table` (`PRIMARY KEY (pk, ck)`, int pk, 3
//! partitions pk=1/2/3, each 300 rows ck=0..299). Each partition's `Rows.db` trie
//! indexes **38** row-index blocks, so the pre-L1 enumerate-then-filter path
//! visited ~42 BTI nodes per clustering read (a full DFS) and deserialized the
//! per-partition `TrieIndexEntry` TWICE. This test pins the two work-counter
//! invariants L1 delivers:
//!
//!   1. **`BTI_NODES_VISITED < 40`** on a point clustering read — the floor/ceiling
//!      walks visit O(len(ck)) nodes, not O(blocks). (Pre-L1: ~42, a full DFS.)
//!   2. **`ROWS_DB_ENTRY_RESOLVES == 1`** per clustering read — the window path
//!      resolves the per-partition entry ONCE (pre-L1: 2 — once directly, once
//!      inside `iterate_rows_for_partition`).
//!
//! Both counters are measured AFTER a warm-up read so the reader's per-reader
//! next-partition successor cache (`bti_partition_offsets`, which itself enumerates
//! the Partitions.db trie and resolves each wide partition's entry) is already
//! populated — otherwise the FIRST clustering read would carry that one-time
//! enumeration's nodes/resolves. The measured read therefore reflects only the
//! clustering-window work, which is exactly what L1 optimizes.
//!
//! Compiled only with `--features work-counters` (the counter getters/`reset` live
//! behind it). Requires `CQLITE_DATASETS_ROOT` + the optional `test_da` corpus;
//! skips (never fails) when absent. Excluded under `tombstones` (that build serves
//! reads by a full-scan filter, compiling out the clustering seek).

#![cfg(all(
    feature = "state_machine",
    feature = "cli-helpers",
    feature = "work-counters",
    not(feature = "tombstones")
))]

use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::storage::sstable::read_work_counters as rwc;
use cqlite_core::Database;
use serial_test::serial;

const QUALIFIED_TABLE: &str = "test_da.wide_table";
const KEYSPACE_FILTER: &str = "/test_da/";
/// The pre-L1 full-DFS node-visit count for one partition's 38-block row-index
/// trie is 42; the floor+ceiling walks stay well under this bound.
const NODES_BOUND: u64 = 40;

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

fn schemas_dir() -> Option<PathBuf> {
    if let Some(root) = datasets_root() {
        let dir = root.parent()?.join("schemas");
        if dir.exists() {
            return Some(dir);
        }
    }
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dir = manifest_dir.parent()?.join("test-data").join("schemas");
    dir.exists().then_some(dir)
}

async fn skip_or_db() -> Option<Database> {
    let root = datasets_root()?;
    let schema_path = schemas_dir()?.join("wide-table-bti.cql");
    if !schema_path.exists() {
        eprintln!("Skipping (L1): wide-table-bti.cql schema not found");
        return None;
    }
    let data_dir = root.join("sstables");
    if !data_dir.exists() {
        eprintln!("Skipping (L1): sstables dir not found");
        return None;
    }
    let config = IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir,
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(KEYSPACE_FILTER.to_string()),
    };
    let result = ingest(config).await.ok()?;
    if result.schema_load_result.schemas_loaded == 0 {
        eprintln!("Skipping (L1): no schemas loaded");
        return None;
    }
    Some(result.database)
}

/// Read the full pk=1 partition to (a) confirm the Data.db is fetched and (b) warm
/// the reader's next-partition successor cache, so the measured clustering reads
/// carry only the clustering-window work. Returns false to SKIP when 0 rows come
/// back (binaries not fetched).
async fn warm_up(db: &Database) -> bool {
    let full = db
        .execute(&format!(
            "SELECT pk, ck FROM {QUALIFIED_TABLE} WHERE pk = 1"
        ))
        .await
        .expect("warm-up full partition read must succeed");
    if full.rows.is_empty() {
        eprintln!("Skipping (L1): wide_table returned 0 rows (Data.db not fetched?)");
        return false;
    }
    assert_eq!(
        full.rows.len(),
        300,
        "fixture invariant: pk=1 must hold 300 clustering rows",
    );
    true
}

/// A point clustering read (`ck = 150`) visits fewer than 40 BTI nodes and
/// resolves the per-partition `Rows.db` entry exactly once.
#[tokio::test]
#[serial]
async fn point_clustering_read_walks_floor_not_all_blocks() {
    let Some(db) = skip_or_db().await else {
        return;
    };
    if !warm_up(&db).await {
        return;
    }

    rwc::reset();
    let res = db
        .execute(&format!(
            "SELECT pk, ck, payload FROM {QUALIFIED_TABLE} WHERE pk = 1 AND ck = 150"
        ))
        .await
        .expect("point clustering read must succeed");

    // Correctness first: the exact row is returned.
    assert_eq!(
        res.rows.len(),
        1,
        "L1: `pk = 1 AND ck = 150` must return exactly the one matching row",
    );

    let nodes = rwc::bti_nodes_visited();
    let resolves = rwc::rows_db_entry_resolves();
    println!("L1 point read: bti_nodes_visited={nodes} rows_db_entry_resolves={resolves}");

    assert!(
        nodes > 0 && nodes < NODES_BOUND,
        "L1: a point clustering read must visit (0, {NODES_BOUND}) BTI nodes via the \
         separator-floor walk; got {nodes} (pre-L1 full DFS over 38 blocks visited ~42)",
    );
    assert_eq!(
        resolves, 1,
        "L1: the clustering-window path must resolve the per-partition Rows.db entry \
         EXACTLY once; got {resolves} (pre-L1 resolved it twice)",
    );
}

/// A bounded range clustering read (`ck >= 100 AND ck < 110`) also floors in
/// O(key-length) — the forward window end is a strict-ceiling walk, not a scan of
/// every block.
#[tokio::test]
#[serial]
async fn range_clustering_read_walks_floor_not_all_blocks() {
    let Some(db) = skip_or_db().await else {
        return;
    };
    if !warm_up(&db).await {
        return;
    }

    rwc::reset();
    let res = db
        .execute(&format!(
            "SELECT pk, ck, payload FROM {QUALIFIED_TABLE} WHERE pk = 1 AND ck >= 100 AND ck < 110"
        ))
        .await
        .expect("range clustering read must succeed");

    assert_eq!(
        res.rows.len(),
        10,
        "L1: `ck in [100, 110)` must return exactly ck=100..=109",
    );

    let nodes = rwc::bti_nodes_visited();
    let resolves = rwc::rows_db_entry_resolves();
    println!("L1 range read: bti_nodes_visited={nodes} rows_db_entry_resolves={resolves}");

    assert!(
        nodes > 0 && nodes < NODES_BOUND,
        "L1: a range clustering read must visit (0, {NODES_BOUND}) BTI nodes via the \
         floor + strict-ceiling walks; got {nodes} (pre-L1 full DFS visited ~42)",
    );
    assert_eq!(
        resolves, 1,
        "L1: the clustering-window path must resolve the per-partition Rows.db entry \
         EXACTLY once; got {resolves} (pre-L1 resolved it twice)",
    );
}
