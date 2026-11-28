//! Issue #4626 phase 2 (task 2.1, requirements G6 and G7): the write engine allocates its next
//! SSTable id through the generation module, so it sees every format, version, backup and id form
//! the old recursive `nb-*-big` scan missed (#4623).
//!
//! Oracles: Cassandra 5.0.8-written fixtures (`test-data/fixtures/issue_4626`) and committed corpus
//! generations (`oa`, BTI `da`), copied into a tempdir (never mutated in place). The allocation
//! rule is Cassandra's `Directories.getUIDGenerator` (`SequenceBasedSSTableId.Builder`, pinned
//! `cassandra-5.0.8`). Each case resolves its own fixture and panics by name when it is missing.

#![cfg(feature = "write-support")]

#[path = "support/datasets_root.rs"]
mod datasets_root;
#[path = "support/generation_fixtures.rs"]
mod generation_fixtures;

use std::collections::HashMap;
use std::path::Path;

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use generation_fixtures::*;
use tempfile::TempDir;

const KS: &str = "gd4626";
const TABLE: &str = "alloc_target";

fn schema() -> TableSchema {
    TableSchema {
        keyspace: KS.to_string(),
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
                name: "name".to_string(),
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

/// `<root>/<ks>/<table>`, the directory a flush publishes into.
fn table_dir(root: &Path) -> std::path::PathBuf {
    let d = root.join(KS).join(TABLE);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn engine_at(root: &Path) -> cqlite_core::Result<WriteEngine> {
    engine_with_wal(root, "wal")
}

/// An engine over `root` with its own WAL directory (two engines may share a data directory).
fn engine_with_wal(root: &Path, wal: &str) -> cqlite_core::Result<WriteEngine> {
    let wal = root.join(wal);
    WriteEngine::new(WriteEngineConfig::new(root.to_path_buf(), wal, schema()))
}

fn row(id: i32) -> Mutation {
    Mutation::new(
        TableId::new(KS, TABLE),
        PartitionKey::single("id", Value::Integer(id)),
        None,
        vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::text(format!("n{id}")),
        }],
        1_000,
        None,
    )
}

/// Flush one row; returns the Data.db file name that appeared in the table directory.
async fn flush_and_name(engine: &mut WriteEngine, dir: &Path) -> String {
    let before = content_signature(dir);
    engine.write(row(1)).expect("write");
    engine.flush().await.expect("flush").expect("info");
    let after = content_signature(dir);
    after
        .iter()
        .map(|(n, _)| n.clone())
        .find(|n| n.ends_with("-Data.db") && !before.iter().any(|(b, _)| b == n))
        .expect("flush published a new Data.db")
}

/// G6.1 with real bytes: `oa-4-big` and `da-6-bti` (corpus generations, renamed copies) plus a
/// Cassandra-written UUID-id generation and sequence ids 1 and 2. The old scan matched only
/// `nb-*-big`, so it allocated 1 and a flush OVERWROTE nothing only by luck; the module gives 7
/// (max sequence over every format and version, UUID ids not counted).
#[tokio::test]
async fn flush_allocates_above_every_format_and_version() {
    let tmp = TempDir::new().unwrap();
    let dir = table_dir(tmp.path());
    copy_generation_as(
        &corpus_table("test_oa", "simple_table"),
        "oa-2-big",
        &dir,
        "oa-4-big",
    );
    copy_generation_as(
        &corpus_table("test_da", "simple_table"),
        "da-2-bti",
        &dir,
        "da-6-bti",
    );
    let uuid = fixture_table("uuid_ids", "uuid_mixed");
    copy_generation_as(&uuid, "nb-1-big", &dir, "nb-1-big");
    copy_generation_as(
        &uuid,
        "nb-3h5t_0i6r_2dipm2lxu6305iz8wl-big",
        &dir,
        "nb-3h5t_0i6r_2dipm2lxu6305iz8wl-big",
    );
    let before = content_signature(&dir);

    let mut engine = engine_at(tmp.path()).expect("engine");
    assert_eq!(engine.generation(), 7, "next id over oa-4, da-6, nb-1");
    let name = flush_and_name(&mut engine, &dir).await;
    assert_eq!(name, "nb-7-big-Data.db");
    // Everything that was there is byte-for-byte (size) untouched.
    let after = content_signature(&dir);
    for entry in &before {
        assert!(after.contains(entry), "{entry:?} changed or vanished");
    }
}

/// G6.2 and G6.3: live max 1, `backups/` max 4, `snapshots/` max 5. The id is 5: backups count,
/// snapshots do not (the old recursive scan saw all three and answered 6).
#[tokio::test]
async fn backups_count_and_snapshots_do_not() {
    let tmp = TempDir::new().unwrap();
    let dir = table_dir(tmp.path());
    copy_dir(&fixture_table("snap_backup_live_low", "snap_backup"), &dir);
    let mut engine = engine_at(tmp.path()).expect("engine");
    assert_eq!(engine.generation(), 5);
    let name = flush_and_name(&mut engine, &dir).await;
    assert_eq!(name, "nb-5-big-Data.db");
}

/// G6.4: UUID ids do not contribute. `uuid_only` has no sequence id (next is 1); `uuid_mixed`
/// has sequence 1 and 2 (next is 3).
#[test]
fn uuid_ids_do_not_contribute() {
    for (table, expected) in [("uuid_only", 1u64), ("uuid_mixed", 3u64)] {
        let tmp = TempDir::new().unwrap();
        let dir = table_dir(tmp.path());
        copy_dir(&fixture_table("uuid_ids", table), &dir);
        let engine = engine_at(tmp.path()).expect("engine");
        assert_eq!(engine.generation(), expected, "{table}");
    }
}

/// The scan covers only the table directory: a generation under a sibling table's directory is
/// not this table's (the old scan recursed the whole data root).
#[test]
fn a_sibling_tables_generations_do_not_count() {
    let tmp = TempDir::new().unwrap();
    let sibling = tmp.path().join(KS).join("other_table");
    copy_dir(&fixture_table("snap_backup", "snap_backup"), &sibling);
    let engine = engine_at(tmp.path()).expect("engine");
    assert_eq!(engine.generation(), 1);
}

/// G7.1: at the Java `int` limit the engine refuses to open; it never wraps or saturates.
#[test]
fn allocation_at_the_int_limit_is_refused() {
    let tmp = TempDir::new().unwrap();
    let dir = table_dir(tmp.path());
    copy_generation_as(
        &fixture_table("snap_backup_live_low", "snap_backup"),
        "nb-1-big",
        &dir,
        "nb-2147483647-big",
    );
    let err = engine_at(tmp.path()).expect_err("must refuse").to_string();
    assert!(err.contains("2147483647"), "{err}");
}

/// G7.2: one below the limit allocates the limit.
#[test]
fn allocation_next_to_the_int_limit_succeeds() {
    let tmp = TempDir::new().unwrap();
    let dir = table_dir(tmp.path());
    copy_generation_as(
        &fixture_table("snap_backup_live_low", "snap_backup"),
        "nb-1-big",
        &dir,
        "nb-2147483646-big",
    );
    let engine = engine_at(tmp.path()).expect("engine");
    assert_eq!(engine.generation(), 2_147_483_647);
}

/// G7: the engine advances its id after a flush; the flush after the limit id is refused instead
/// of publishing `nb-2147483648`.
#[tokio::test]
async fn a_flush_past_the_int_limit_is_refused() {
    let tmp = TempDir::new().unwrap();
    let dir = table_dir(tmp.path());
    copy_generation_as(
        &fixture_table("snap_backup_live_low", "snap_backup"),
        "nb-1-big",
        &dir,
        "nb-2147483646-big",
    );
    let mut engine = engine_at(tmp.path()).expect("engine");
    let name = flush_and_name(&mut engine, &dir).await;
    assert_eq!(name, "nb-2147483647-big-Data.db");
    engine.write(row(2)).expect("write");
    let err = engine.flush().await.expect_err("must refuse").to_string();
    assert!(err.contains("2147483647"), "{err}");
    assert!(!dir.join("nb-2147483648-big-Data.db").exists());
}


/// B1: two engines opened on one directory both allocate the same id; each flush must publish a
/// DISTINCT generation and neither may overwrite the other (real bytes compared). The old writers
/// used `File::create`, so the second flush replaced `nb-1-big-*`.
#[tokio::test]
async fn two_engines_on_one_directory_never_overwrite_each_other() {
    let tmp = TempDir::new().unwrap();
    let dir = table_dir(tmp.path());
    let mut a = engine_with_wal(tmp.path(), "wal_a").expect("engine a");
    let mut b = engine_with_wal(tmp.path(), "wal_b").expect("engine b");
    assert_eq!(a.generation(), b.generation(), "both picked the same id");

    a.write(row(1)).expect("write a");
    a.flush().await.expect("flush a").expect("info a");
    let after_a = content_signature(&dir);
    assert!(after_a.iter().any(|(n, _)| n == "nb-1-big-Data.db"));

    b.write(row(2)).expect("write b");
    b.flush().await.expect("flush b").expect("info b");
    let after_b = content_signature(&dir);

    for entry in &after_a {
        assert!(after_b.contains(entry), "{} was overwritten", entry.0);
    }
    assert!(
        after_b.iter().any(|(n, _)| n == "nb-2-big-Data.db"),
        "engine b published a distinct generation: {:?}",
        after_b.iter().map(|(n, _)| n).collect::<Vec<_>>()
    );
    assert!(
        after_b.iter().all(|(n, _)| !n.contains("cqlite-publish")),
        "no stage left behind"
    );
    assert_eq!(b.generation(), 3, "the counter moved past the id it used");
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn flush_row(engine: &mut WriteEngine, id: i32) {
    engine.write(row(id)).expect("write");
    rt().block_on(engine.flush()).expect("flush").expect("info");
}

fn stcs() -> cqlite_core::storage::write_engine::merge_policy::STCSPolicy {
    cqlite_core::storage::write_engine::merge_policy::STCSPolicy::new(2, 32, 0.5, 1.5, 1024 * 1024)
        .unwrap()
}

/// Run `maintenance_step` until no compaction is pending.
fn drain_compaction(engine: &mut WriteEngine) -> cqlite_core::Result<()> {
    for _ in 0..10_000 {
        let report = engine.maintenance_step(std::time::Duration::from_secs(60))?;
        if !report.pending_compaction {
            return Ok(());
        }
    }
    panic!("compaction never completed");
}

/// B1: an engine compaction whose output id was taken by another publisher (here a Cassandra-
/// written generation, standing in for a concurrent `cqlite compact`) after the merge started must
/// re-allocate instead of overwriting it. The foreign `nb-3-big-*` stays byte-identical.
#[test]
fn engine_compaction_never_overwrites_an_id_taken_meanwhile() {
    let tmp = TempDir::new().unwrap();
    let dir = table_dir(tmp.path());
    let mut engine = engine_at(tmp.path()).expect("engine");
    flush_row(&mut engine, 1);
    flush_row(&mut engine, 2);
    assert_eq!(engine.generation(), 3, "the compaction output will be nb-3");
    engine.set_merge_policy(Box::new(stcs())).unwrap();

    // Another writer publishes nb-3 before this engine's compaction publishes.
    copy_generation_as(
        &fixture_table("snap_backup_live_low", "snap_backup"),
        "nb-1-big",
        &dir,
        "nb-3-big",
    );
    let foreign: Vec<_> = content_signature(&dir)
        .into_iter()
        .filter(|(n, _)| n.starts_with("nb-3-big-"))
        .collect();
    assert!(!foreign.is_empty());

    drain_compaction(&mut engine).expect("compaction");
    let after = content_signature(&dir);
    for entry in &foreign {
        assert!(after.contains(entry), "{} was overwritten", entry.0);
    }
    assert!(
        after.iter().any(|(n, _)| n == "nb-4-big-Data.db"),
        "output re-allocated to nb-4: {:?}",
        after.iter().map(|(n, _)| n).collect::<Vec<_>>()
    );
}

/// M1 (G7): a compaction whose output id would exceed the Java int range is refused, as a flush is.
#[test]
fn engine_compaction_refuses_an_id_past_the_int_limit() {
    let tmp = TempDir::new().unwrap();
    let dir = table_dir(tmp.path());
    copy_generation_as(
        &fixture_table("snap_backup_live_low", "snap_backup"),
        "nb-1-big",
        &dir,
        "nb-2147483645-big",
    );
    let mut engine = engine_at(tmp.path()).expect("engine");
    assert_eq!(engine.generation(), 2_147_483_646);
    flush_row(&mut engine, 1); // nb-2147483646
    flush_row(&mut engine, 2); // nb-2147483647; the counter is now past the limit
    engine.set_merge_policy(Box::new(stcs())).unwrap();
    let before = content_signature(&dir);
    let err = drain_compaction(&mut engine).expect_err("must refuse").to_string();
    assert!(err.contains("2147483647"), "{err}");
    assert_eq!(before, content_signature(&dir), "refusal wrote something");
}
