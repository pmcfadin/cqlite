//! Issue #1749: explicit SSTable directory refresh — public-surface integration
//! tests for the storage-freshness spec (`Database::refresh()`).
//!
//! Fixtures are REAL CQLite-written uncompressed SSTables (the write path is
//! byte-parity with Cassandra, M5) built in-test, giving deterministic,
//! distinct-partition generations. Because the fixtures are generated here, an
//! "absent dataset" case cannot silently skip: a fixture-write failure fails the
//! test, and every row assertion is an exact count (never `>= 0`), so a
//! 0-rows-on-present-data regression fails loudly.
//!
//! Gated on `write-support` (to build the fixtures) and `state_machine` (to run
//! SELECTs via `Database::execute`), so the file is empty in the minimal build —
//! no ungated write-support test items leak into the minimal-features CI step.

#![cfg(all(feature = "write-support", feature = "state_machine"))]

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use cqlite_core::{Config, Database};
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use tempfile::TempDir;

const KEYSPACE: &str = "test_freshness";
const TABLE: &str = "users";

fn users_schema() -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.to_string(),
        table: TABLE.to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "value".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// Build a SOURCE directory holding two SSTable generations of the same table:
/// `nb-1-big-*` contains only partition id=1, `nb-2-big-*` only partition id=2
/// (each flush of a single WriteEngine advances the generation). Returns the
/// `.../<keyspace>/<table>` directory containing both generations.
async fn build_two_generations(root: &Path) -> PathBuf {
    let data_dir = root.join("data");
    let wal_dir = root.join("wal");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, users_schema());
    let mut engine = WriteEngine::new(config).expect("write engine");

    for id in [1_i32, 2_i32] {
        let table_id = TableId::new(KEYSPACE, TABLE);
        let pk = PartitionKey::single("id", Value::Integer(id));
        let ops = vec![CellOperation::Write {
            column: "value".to_string(),
            value: Value::Text(format!("v{}", id)),
        }];
        engine
            .write_async(Mutation::new(
                table_id,
                pk,
                None,
                ops,
                1_000 + id as i64,
                None,
            ))
            .await
            .expect("write partition");
        engine.flush().await.expect("flush generation");
    }

    let table_dir = data_dir.join(KEYSPACE).join(TABLE);
    assert!(
        table_dir.join("nb-1-big-Data.db").exists(),
        "gen-1 must exist in source"
    );
    assert!(
        table_dir.join("nb-2-big-Data.db").exists(),
        "gen-2 must exist in source"
    );
    table_dir
}

/// Copy every component file of one generation (`nb-<n>-big-*`) from
/// `src_table_dir` into `dst_table_dir`, creating the destination if needed.
/// Returns the number of component files copied (must be > 0).
fn copy_generation(src_table_dir: &Path, dst_table_dir: &Path, gen: u32) -> usize {
    std::fs::create_dir_all(dst_table_dir).expect("create dst table dir");
    let prefix = format!("nb-{}-big-", gen);
    let mut copied = 0;
    for entry in std::fs::read_dir(src_table_dir).expect("read src table dir") {
        let entry = entry.expect("dir entry");
        let name = entry.file_name();
        let name = name.to_str().expect("utf8 filename");
        if name.starts_with(&prefix) {
            std::fs::copy(entry.path(), dst_table_dir.join(name)).expect("copy component");
            copied += 1;
        }
    }
    assert!(copied > 0, "expected to copy generation {} components", gen);
    copied
}

/// Delete every component file of one generation (`nb-<n>-big-*`) from
/// `table_dir` (simulating a compaction that removed the generation).
fn delete_generation(table_dir: &Path, gen: u32) -> usize {
    let prefix = format!("nb-{}-big-", gen);
    let mut removed = 0;
    for entry in std::fs::read_dir(table_dir).expect("read table dir") {
        let entry = entry.expect("dir entry");
        let name = entry.file_name();
        let name = name.to_str().expect("utf8 filename");
        if name.starts_with(&prefix) {
            std::fs::remove_file(entry.path()).expect("remove component");
            removed += 1;
        }
    }
    assert!(removed > 0, "expected to remove generation {} components", gen);
    removed
}

/// The set of partition-key `id` values in a `SELECT *` result. The single-int
/// partition key is the 4-byte big-endian row key.
fn partition_ids(rows: &[cqlite_core::query::result::QueryRow]) -> BTreeSet<i32> {
    rows.iter()
        .map(|r| {
            let b = r.key.as_bytes();
            assert_eq!(b.len(), 4, "int partition key must be 4 bytes, got {:?}", b);
            i32::from_be_bytes([b[0], b[1], b[2], b[3]])
        })
        .collect()
}

async fn select_all_ids(db: &Database) -> BTreeSet<i32> {
    let sql = format!("SELECT * FROM {}.{}", KEYSPACE, TABLE);
    let res = db.execute(&sql).await.expect("select");
    partition_ids(&res.rows)
}

/// Spec: "New generation invisible until refresh, visible after" + report
/// `readers_added == 1`, `readers_removed == 0`.
#[tokio::test]
async fn added_generation_invisible_until_refresh_then_visible() {
    let src = TempDir::new().unwrap();
    let src_table_dir = build_two_generations(src.path()).await;

    // Live directory starts with ONLY generation 1 (partition id=1).
    let live = TempDir::new().unwrap();
    let live_table_dir = live.path().join(KEYSPACE).join(TABLE);
    copy_generation(&src_table_dir, &live_table_dir, 1);

    let db = Database::open(live.path(), Config::default())
        .await
        .expect("open db");

    // Pre-copy result: exactly {1}.
    let before = select_all_ids(&db).await;
    assert_eq!(
        before,
        BTreeSet::from([1]),
        "only gen-1 partition visible at open"
    );

    // Copy in generation 2 (partition id=2) but do NOT refresh yet.
    copy_generation(&src_table_dir, &live_table_dir, 2);
    let stale = select_all_ids(&db).await;
    assert_eq!(
        stale, before,
        "stale-until-refresh: same result before refresh() despite new file on disk"
    );

    // Refresh, then re-run the SELECT.
    let report = db.refresh().await.expect("refresh");
    assert_eq!(report.readers_added, 1, "one generation added");
    assert_eq!(report.readers_removed, 0, "none removed");

    let after = select_all_ids(&db).await;
    assert_eq!(
        after,
        BTreeSet::from([1, 2]),
        "new generation's partition visible after refresh"
    );
}

/// Spec: "Removed generation dropped safely" — `readers_removed == 1`, the
/// subsequent SELECT returns only the remaining generation, no panic.
#[tokio::test]
async fn removed_generation_dropped_on_refresh() {
    let src = TempDir::new().unwrap();
    let src_table_dir = build_two_generations(src.path()).await;

    // Live directory starts with BOTH generations.
    let live = TempDir::new().unwrap();
    let live_table_dir = live.path().join(KEYSPACE).join(TABLE);
    copy_generation(&src_table_dir, &live_table_dir, 1);
    copy_generation(&src_table_dir, &live_table_dir, 2);

    let db = Database::open(live.path(), Config::default())
        .await
        .expect("open db");
    assert_eq!(
        select_all_ids(&db).await,
        BTreeSet::from([1, 2]),
        "both partitions visible at open"
    );

    // Remove generation 2 (simulated compaction) and refresh.
    delete_generation(&live_table_dir, 2);
    let report = db.refresh().await.expect("refresh");
    assert_eq!(report.readers_removed, 1, "one generation removed");
    assert_eq!(report.readers_added, 0, "none added");

    assert_eq!(
        select_all_ids(&db).await,
        BTreeSet::from([1]),
        "only the remaining generation's partition after removal"
    );
}

/// Spec: "Corrupt new generation rejects the whole refresh" — typed error, the
/// pre-refresh result set is fully intact (fail-closed, #1626 inherited).
#[tokio::test]
async fn corrupt_new_generation_rejects_whole_refresh() {
    let src = TempDir::new().unwrap();
    let src_table_dir = build_two_generations(src.path()).await;

    let live = TempDir::new().unwrap();
    let live_table_dir = live.path().join(KEYSPACE).join(TABLE);
    copy_generation(&src_table_dir, &live_table_dir, 1);

    let db = Database::open(live.path(), Config::default())
        .await
        .expect("open db");
    let before = select_all_ids(&db).await;
    assert_eq!(before, BTreeSet::from([1]), "gen-1 visible at open");

    // Copy generation 2 in, then corrupt its Statistics.db (truncate + garble),
    // which fails SSTableReader::open per the #1626 hard-fail posture.
    copy_generation(&src_table_dir, &live_table_dir, 2);
    let stats = live_table_dir.join("nb-2-big-Statistics.db");
    std::fs::write(&stats, b"\x00\x01\x02corrupt-not-a-statistics-db\xff\xff")
        .expect("corrupt statistics");

    let err = db.refresh().await.expect_err("refresh must fail-closed on corrupt generation");
    // Typed error (no panic); any Error variant is acceptable — assert it Displays.
    let _ = err.to_string();

    // Pre-refresh result set fully intact — the corrupt gen-2 is NOT visible.
    let after = select_all_ids(&db).await;
    assert_eq!(
        after, before,
        "fail-closed: reader set unchanged, corrupt generation not partially visible"
    );
}

/// Spec: unchanged directory is a zero-delta no-op at the `Database::refresh()`
/// surface (the reader-Arc pointer-identity half is a core-level unit test in
/// `storage::sstable::refresh`).
#[tokio::test]
async fn unchanged_directory_is_zero_delta_noop() {
    let src = TempDir::new().unwrap();
    let src_table_dir = build_two_generations(src.path()).await;

    let live = TempDir::new().unwrap();
    let live_table_dir = live.path().join(KEYSPACE).join(TABLE);
    copy_generation(&src_table_dir, &live_table_dir, 1);

    let db = Database::open(live.path(), Config::default())
        .await
        .expect("open db");
    let before = select_all_ids(&db).await;

    let report = db.refresh().await.expect("refresh");
    assert_eq!(report.readers_added, 0, "no-op: nothing added");
    assert_eq!(report.readers_removed, 0, "no-op: nothing removed");

    assert_eq!(select_all_ids(&db).await, before, "result unchanged by no-op");
}

// The "In-flight scan unaffected by concurrent refresh" spec scenario is covered
// as a core-level test in `storage::sstable::refresh` (module
// `in_flight_scan_unaffected_by_concurrent_refresh`): it drives the actual
// streaming primitive, `SSTableManager::scan_stream`, which resolves and holds
// its `Arc` reader snapshot before yielding — the invariant this scenario pins.
// The public-surface half ("a query issued after the refresh sees the new set")
// is exercised by `added_generation_invisible_until_refresh_then_visible` above.
