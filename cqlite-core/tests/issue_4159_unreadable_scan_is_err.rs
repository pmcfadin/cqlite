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

// ---- refresh x refusal ledger (roborev round 1) --------------------------------

fn repair(dir: &Path, table: &str) {
    let src = generation_dir(table).join("nb-1-big-Statistics.db");
    std::fs::copy(src, dir.join("nb-1-big-Statistics.db")).expect("restore stats");
}

/// A (previously refused) now opens, B (new) fails: the refresh `Err`s NAMING B and changes
/// NOTHING (storage-freshness spec: a failed refresh leaves the reader set AND the refusal
/// ledger exactly as they were). Reads afterwards are exactly the pre-refresh state: A is still
/// refused (`Err`), B was never recorded (its table is simply absent, `Ok(empty)`). A later
/// successful refresh installs both and clears A.
#[tokio::test]
async fn failed_refresh_changes_nothing_and_names_the_failing_candidate() {
    let tmp = tempfile::tempdir().expect("tmp");
    let a = stage(tmp.path(), TABLE, true);
    let m = manager(tmp.path()).await;
    assert!(m.scan(&tid(TABLE), None, None, None, None).await.is_err());

    repair(&a, TABLE);
    let b = stage(tmp.path(), BYSTANDER, true);
    let b_name = b.file_name().expect("b name").to_string_lossy().to_string();

    let refreshed = m
        .refresh_tables()
        .await
        .expect_err("refresh must abort on candidate B");
    assert!(
        refreshed.to_string().contains(&b_name),
        "the Err must name B: {refreshed}"
    );

    let ea = m
        .scan(&tid(TABLE), None, None, None, None)
        .await
        .expect_err("A stays refused");
    assert!(ea.to_string().contains("unreadable"), "{ea}");
    assert!(
        !ea.to_string().contains(&b_name),
        "B must NOT have been recorded by the failed refresh: {ea}"
    );
    let pre = m
        .scan(&tid(BYSTANDER), None, None, None, None)
        .await
        .expect("B's table is unchanged by a failed refresh (absent, not unreadable)");
    assert!(pre.is_empty());

    // Once B is repaired, the next refresh installs both and clears A's refusal.
    repair(&b, BYSTANDER);
    m.refresh_tables().await.expect("refresh after repair");
    assert!(!m
        .scan(&tid(TABLE), None, None, None, None)
        .await
        .expect("A reads")
        .is_empty());
    assert!(!m
        .scan(&tid(BYSTANDER), None, None, None, None)
        .await
        .expect("B reads")
        .is_empty());
}

/// A refused SSTable that is deleted and accepted by refresh stops poisoning reads.
#[tokio::test]
async fn deleted_refused_sstable_is_reconciled_by_refresh() {
    let tmp = tempfile::tempdir().expect("tmp");
    let a = stage(tmp.path(), TABLE, true);
    stage(tmp.path(), BYSTANDER, false);
    let m = manager(tmp.path()).await;
    assert!(m.scan(&tid(TABLE), None, None, None, None).await.is_err());

    std::fs::remove_dir_all(&a).expect("delete refused generation");
    m.refresh_tables()
        .await
        .expect("refresh accepts the removal");

    let gone = m
        .scan(&tid(TABLE), None, None, None, None)
        .await
        .expect("refused table is gone, not unreadable");
    assert!(gone.is_empty());
    assert!(!m
        .scan(&tid(BYSTANDER), None, None, None, None)
        .await
        .expect("bystander")
        .is_empty());
}

// ---- directory-level refusals + unresolvable keys (roborev round 2) --------------

#[cfg(unix)]
async fn dirs_manager(base: &Path, dirs: Vec<PathBuf>) -> SSTableManager {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    SSTableManager::new_from_discovered_paths(base, dirs, &config, platform, None)
        .await
        .expect("new_from_discovered_paths stays Ok")
}

/// Restores a directory's mode on drop (also on a failing assertion).
#[cfg(unix)]
struct ModeGuard(PathBuf, u32);
#[cfg(unix)]
impl Drop for ModeGuard {
    fn drop(&mut self) {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(self.1));
    }
}

/// An existing table directory that cannot be listed is `Err` (not empty); once it
/// is readable again a refresh clears that directory-level refusal.
#[cfg(unix)]
#[tokio::test]
async fn unreadable_table_dir_recovers_after_refresh() {
    use std::os::unix::fs::PermissionsExt;
    let tmp = tempfile::tempdir().expect("tmp");
    let dir = stage(tmp.path(), TABLE, false);
    let guard = ModeGuard(dir.clone(), 0o755);
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o000)).expect("chmod 000");
    if std::fs::read_dir(&dir).is_ok() {
        return; // running as root: mode bits do not apply; nothing to prove here
    }
    let m = dirs_manager(tmp.path(), vec![dir.clone()]).await;
    assert!(
        m.scan(&tid(TABLE), None, None, None, None).await.is_err(),
        "unlistable dir must be Err"
    );
    assert!(
        m.refresh_tables().await.is_err(),
        "refresh over a still-unlistable dir must fail closed"
    );
    assert!(m.scan(&tid(TABLE), None, None, None, None).await.is_err());

    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).expect("restore");
    m.refresh_tables().await.expect("refresh once readable");
    assert!(!m
        .scan(&tid(TABLE), None, None, None, None)
        .await
        .expect("recovered")
        .is_empty());
    drop(guard);
}

/// A refused SSTable whose table cannot be derived from its (non-UTF-8) directory
/// name is recorded as UNRESOLVED and fails EVERY read on the manager.
#[cfg(unix)]
#[tokio::test]
async fn unresolvable_path_fails_every_read() {
    use std::os::unix::ffi::OsStrExt;
    let tmp = tempfile::tempdir().expect("tmp");
    let healthy = stage(tmp.path(), BYSTANDER, false);
    let odd = tmp
        .path()
        .join(std::ffi::OsStr::from_bytes(b"\xff\xfe-not-utf8"));
    copy_dir(&generation_dir(TABLE), &odd);
    damage_stats(&odd);
    let m = dirs_manager(tmp.path(), vec![healthy, odd]).await;
    for t in [TABLE, BYSTANDER, "no_such_table"] {
        let e = m
            .scan(&tid(t), None, None, None, None)
            .await
            .expect_err("unresolved refusal fails all reads");
        assert!(e.to_string().contains("unreadable"), "{t}: {e}");
    }
}

fn damage_stats(dir: &Path) {
    let stats = dir.join("nb-1-big-Statistics.db");
    let bytes = std::fs::read(&stats).expect("read stats");
    std::fs::write(&stats, &bytes[..20.min(bytes.len())]).expect("truncate stats");
}

/// A refused SSTable whose table the layout helpers cannot name (here: files laid
/// directly in the scan base directory, so the only path-derived "name" would be the
/// base directory's own) must not be keyed by a parent-directory name: it is
/// UNRESOLVED and fails reads for every table, never `Ok(empty)`.
#[tokio::test]
async fn refused_sstable_without_a_layout_derived_table_fails_reads() {
    let tmp = tempfile::tempdir().expect("tmp");
    copy_dir(&generation_dir(TABLE), tmp.path());
    damage_stats(tmp.path());
    let m = manager(tmp.path()).await;
    for t in [TABLE, "anything_else"] {
        let e = m
            .scan(&tid(t), None, None, None, None)
            .await
            .expect_err("must be Err, not Ok(empty)");
        assert!(e.to_string().contains("unreadable"), "{t}: {e}");
    }
}
