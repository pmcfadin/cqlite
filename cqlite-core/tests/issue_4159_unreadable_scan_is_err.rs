//! Issue #4159 — a read over an SSTable that cannot be OPENED must return `Err`
//! naming the cause, never `Ok(empty)`.
//!
//! Fixture: a Cassandra-5.0.8-written `test_basic.simple_table` generation
//! (datasets-v4) copied into a TempDir. The ONLY edit is documented here: the
//! generation's `nb-1-big-Statistics.db` is truncated to its first 20 bytes
//! (shorter than the 32-byte outer header), so the metadata parse refuses. The
//! test MEASURES that refusal (`SSTableReader::open` errs) before asserting on
//! any manager surface, and pairs every case with a PRISTINE copy that must
//! return rows, so "Err" can never be a lane that refuses healthy data and
//! "Ok(empty)" can never be taken for success. Every case is `must_run`:
//! a missing corpus is a hard failure (#3220), never a skip.

#![cfg(feature = "state_machine")]

#[path = "support/datasets_root.rs"]
mod datasets_root;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::SSTableManager;
use cqlite_core::types::{RowKey, TableId};
use cqlite_core::{Config, Error};

const KEYSPACE: &str = "test_basic";
const TABLE: &str = "simple_table";
const BYSTANDER: &str = "uncompressed_table";

fn generation_dir(table: &str) -> PathBuf {
    let root = datasets_root::sstables_root_for_table(KEYSPACE, table).unwrap_or_else(|| {
        panic!(
            "corpus missing: {}",
            datasets_root::describe_search(KEYSPACE, table)
        )
    });
    datasets_root::table_generation_dirs(&root, KEYSPACE, table)
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("no generation dir for {table}"))
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("mkdir");
    for e in std::fs::read_dir(src).expect("read_dir") {
        let e = e.expect("entry");
        std::fs::copy(e.path(), dst.join(e.file_name())).expect("copy");
    }
}

/// Stage `<base>/test_basic/<table>-<uuid>/` from the corpus; optionally damage Statistics.db.
fn stage(base: &Path, table: &str, damage: bool) -> PathBuf {
    let src = generation_dir(table);
    let dst = base.join(KEYSPACE).join(src.file_name().expect("dir name"));
    copy_dir(&src, &dst);
    if damage {
        let stats = dst.join("nb-1-big-Statistics.db");
        let bytes = std::fs::read(&stats).expect("read stats");
        std::fs::write(&stats, &bytes[..20.min(bytes.len())]).expect("truncate stats");
    }
    dst
}

async fn manager(base: &Path) -> SSTableManager {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    SSTableManager::new(base, &config, platform, None)
        .await
        .expect("SSTableManager::new must stay Ok: one refused file is recorded, not fatal")
}

fn tid(table: &str) -> TableId {
    TableId::from(format!("{KEYSPACE}.{table}").as_str())
}

fn assert_refusal_names_cause(e: &Error, what: &str) {
    let msg = e.to_string();
    assert!(
        msg.contains("unreadable") && msg.contains(TABLE) && msg.contains("nb-1-big-Data.db"),
        "{what}: error must name the table, the refused file and the cause, got: {msg}"
    );
}

#[tokio::test]
async fn pristine_control_returns_rows() {
    let tmp = tempfile::tempdir().expect("tmp");
    stage(tmp.path(), TABLE, false);
    let m = manager(tmp.path()).await;
    let rows = m
        .scan(&tid(TABLE), None, None, None, None)
        .await
        .expect("pristine scan");
    assert!(
        !rows.is_empty(),
        "pristine control must return rows (0 rows when data present is a failure)"
    );
}

#[tokio::test]
async fn the_damage_really_refuses_at_open() {
    let tmp = tempfile::tempdir().expect("tmp");
    let dir = stage(tmp.path(), TABLE, true);
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    let r = cqlite_core::storage::sstable::reader::SSTableReader::open(
        &dir.join("nb-1-big-Data.db"),
        &config,
        platform,
    )
    .await;
    assert!(
        r.is_err(),
        "fixture staging: the truncated Statistics.db must refuse the open"
    );
}

#[tokio::test]
async fn scan_over_unreadable_table_is_err_not_empty() {
    let tmp = tempfile::tempdir().expect("tmp");
    stage(tmp.path(), TABLE, true);
    let m = manager(tmp.path()).await;
    let r = m.scan(&tid(TABLE), None, None, None, None).await;
    let e = r.expect_err("scan over an unreadable SSTable must be Err, not Ok(empty)");
    assert_refusal_names_cause(&e, "scan");
}

#[tokio::test]
async fn scan_with_cell_metadata_over_unreadable_table_is_err() {
    let tmp = tempfile::tempdir().expect("tmp");
    stage(tmp.path(), TABLE, true);
    let m = manager(tmp.path()).await;
    let e = m
        .scan_with_cell_metadata(&tid(TABLE), None, None, None, None)
        .await
        .expect_err("metadata scan must be Err");
    assert_refusal_names_cause(&e, "scan_with_cell_metadata");
}

#[tokio::test]
async fn point_read_over_unreadable_table_is_err() {
    let tmp = tempfile::tempdir().expect("tmp");
    stage(tmp.path(), TABLE, true);
    let m = manager(tmp.path()).await;
    let key = RowKey::from(vec![0u8, 0, 0, 1]);
    let e = m
        .get(&tid(TABLE), &key)
        .await
        .expect_err("point read must be Err, not Ok(None)");
    assert_refusal_names_cause(&e, "get");
}

#[tokio::test]
async fn stream_scan_over_unreadable_table_is_err() {
    let tmp = tempfile::tempdir().expect("tmp");
    stage(tmp.path(), TABLE, true);
    let m = manager(tmp.path()).await;
    let r = m.scan_stream(&tid(TABLE), None, None, None, 16).await;
    let e = match r {
        Ok(_) => panic!("scan_stream over an unreadable table must be Err"),
        Err(e) => e,
    };
    assert_refusal_names_cause(&e, "scan_stream");
}

/// One refused table must not make a healthy neighbour unreadable (the reason
/// `new` records instead of failing), and the refusal must not bleed across tables.
#[tokio::test]
async fn healthy_bystander_table_still_reads() {
    let tmp = tempfile::tempdir().expect("tmp");
    stage(tmp.path(), TABLE, true);
    stage(tmp.path(), BYSTANDER, false);
    let m = manager(tmp.path()).await;
    let rows = m
        .scan(&tid(BYSTANDER), None, None, None, None)
        .await
        .expect("bystander scan");
    assert!(!rows.is_empty(), "bystander must return rows");
    assert!(m.scan(&tid(TABLE), None, None, None, None).await.is_err());
}

/// A table that was never discovered is genuinely absent: `Ok(empty)` stays
/// distinguishable from `Err` (AC2).
#[tokio::test]
async fn genuinely_absent_table_is_ok_empty() {
    let tmp = tempfile::tempdir().expect("tmp");
    stage(tmp.path(), BYSTANDER, false);
    let m = manager(tmp.path()).await;
    let rows = m
        .scan(&tid("no_such_table"), None, None, None, None)
        .await
        .expect("absent table");
    assert!(rows.is_empty());
}
