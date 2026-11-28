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
    let wal = root.join("wal");
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
    let before = tree_signature(dir);
    engine.write(row(1)).expect("write");
    engine.flush().await.expect("flush").expect("info");
    let after = tree_signature(dir);
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
    let before = tree_signature(&dir);

    let mut engine = engine_at(tmp.path()).expect("engine");
    assert_eq!(engine.generation(), 7, "next id over oa-4, da-6, nb-1");
    let name = flush_and_name(&mut engine, &dir).await;
    assert_eq!(name, "nb-7-big-Data.db");
    // Everything that was there is byte-for-byte (size) untouched.
    let after = tree_signature(&dir);
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
