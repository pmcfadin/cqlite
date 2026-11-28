//! Transaction-log liveness (#4626, spec G1, G2, G6.1, G13) against Cassandra's
//! `LogAwareFileLister` semantics.
//!
//! The SSTable bytes are Cassandra-written (copied from `test-data/fixtures/issue_4626/txn_*`).
//! Cassandra's own log records carry the container's absolute paths. Cassandra refuses to apply
//! them to another directory (`LogRecord.isInFolder`); the module applies them when the keyspace
//! and table directory names match (owner ruling Q47). The cases that need a log with a
//! tempdir's own prefix rebuild the SAME five records for the tempdir's path (the checksum function was proven
//! against Cassandra-written records in the module's unit tests) and set each REMOVE input's
//! mtime to the record's `updateTime`, restoring the state Cassandra wrote.

#[path = "support/datasets_root.rs"]
mod datasets_root;
#[path = "support/generation_fixtures.rs"]
mod generation_fixtures;

use std::path::Path;
use std::time::{Duration, UNIX_EPOCH};

use cqlite_core::storage::sstable::generation::{
    list_generations, next_generation_id, DirectoryNote, GenerationError, GenerationId, HiddenBy,
    Listing, Scope, TocPolicy,
};
use generation_fixtures::*;

const LOG_NAME: &str = "nb_txn_compaction_dcc367f0-e899-11f1-8c24-9315fc18c6f9.log";
/// An mtime to give the REMOVE inputs, in milliseconds.
const STAMP_MS: i64 = 1_700_000_000_000;
const FILES_PER_GENERATION: usize = 8;

/// `LogRecord.computeChecksum`, written independently of the module's copy.
fn checksum(path: &str, ty: &str, update_time: i64, num_files: i32) -> i64 {
    let mut crc = crc32fast::Hasher::new();
    crc.update(path.as_bytes());
    crc.update(ty.as_bytes());
    crc.update(&(update_time as i32).to_be_bytes());
    crc.update(&((update_time as u64 >> 32) as i32).to_be_bytes());
    crc.update(&num_files.to_be_bytes());
    i64::from(crc.finalize())
}

fn record(ty: &str, prefix: &str, update_time: i64, num_files: i32) -> String {
    let (path, ut, n) = if ty == "ADD" || ty == "REMOVE" {
        (
            prefix,
            if ty == "REMOVE" { update_time } else { 0 },
            num_files,
        )
    } else {
        ("", 0, 0)
    };
    format!("{ty}:[{path},{ut},{n}][{}]", checksum(path, ty, ut, n))
}

/// A tempdir holding generations 1 to 4 of the compaction fixture, the inputs' mtimes restored.
fn staged() -> tempfile::TempDir {
    let src = fixture_table("txn_committed", "txn_compaction");
    let tmp = tempfile::tempdir().unwrap();
    for entry in std::fs::read_dir(&src).unwrap().filter_map(|e| e.ok()) {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.ends_with(".log") {
            continue;
        }
        std::fs::copy(entry.path(), tmp.path().join(&name)).unwrap();
    }
    stamp_inputs(tmp.path());
    tmp
}

fn stamp_inputs(dir: &Path) {
    let when = UNIX_EPOCH + Duration::from_millis(STAMP_MS as u64);
    for entry in std::fs::read_dir(dir).unwrap().filter_map(|e| e.ok()) {
        let name = entry.file_name().to_string_lossy().into_owned();
        if ["nb-1-", "nb-2-", "nb-3-"]
            .iter()
            .any(|p| name.starts_with(p))
        {
            std::fs::OpenOptions::new()
                .write(true)
                .open(entry.path())
                .unwrap()
                .set_modified(when)
                .unwrap();
        }
    }
}

/// Cassandra's compaction log for `dir`: ADD nb-4, REMOVE nb-3, 2, 1, then optionally COMMIT.
fn write_log(dir: &Path, commit: bool) {
    let canonical = std::fs::canonicalize(dir).unwrap();
    let prefix = |n: u32| format!("{}/nb-{n}-big-", canonical.display());
    let n = FILES_PER_GENERATION as i32;
    let mut lines = vec![
        record("ADD", &prefix(4), 0, n),
        record("REMOVE", &prefix(3), STAMP_MS, n),
        record("REMOVE", &prefix(2), STAMP_MS, n),
        record("REMOVE", &prefix(1), STAMP_MS, n),
    ];
    if commit {
        lines.push(record("COMMIT", "", 0, 0));
    }
    std::fs::write(dir.join(LOG_NAME), lines.join("\n") + "\n").unwrap();
}

fn list(dir: &Path) -> Listing {
    list_generations(dir, &Scope::Live, TocPolicy::Accept)
        .unwrap_or_else(|e| panic!("listing {} failed: {e}", dir.display()))
}

fn live(l: &Listing) -> Vec<String> {
    l.live.iter().map(|g| g.base_name()).collect()
}

/// G13: every file is a component of a listed generation, of an Orphan, hidden by a log, a log or
/// ignored.
fn assert_all_accounted(dir: &Path, l: &Listing) {
    let accounted = l
        .live
        .iter()
        .chain(&l.orphans)
        .map(|g| g.components().len())
        .sum::<usize>()
        + l.hidden.len()
        + l.txn_logs.len()
        + l.ignored.len();
    assert_eq!(accounted, tree_signature(dir).len());
}

/// G1.1
#[test]
fn committed_transaction_hides_its_remove_inputs() {
    let tmp = staged();
    write_log(tmp.path(), true);
    let l = list(tmp.path());
    assert_eq!(live(&l), vec!["nb-4-big"]);
    assert!(l.notes.is_empty(), "{:?}", l.notes);
    assert_eq!(l.hidden.len(), 3 * FILES_PER_GENERATION);
    assert!(l
        .hidden
        .iter()
        .all(|h| h.reason == HiddenBy::CommittedRemove));
    assert!(l.hidden.iter().all(|h| h.log == tmp.path().join(LOG_NAME)));
    // the inputs are still on disk, hidden regardless of disk status
    assert!(tmp.path().join("nb-1-big-Data.db").exists());
    // the log is never a component
    assert_eq!(l.txn_logs, vec![tmp.path().join(LOG_NAME)]);
    assert!(l.live[0]
        .components()
        .iter()
        .all(|c| !c.name.ends_with(".log")));
    assert_all_accounted(tmp.path(), &l);
}

/// G1.2: the log has no final record but every REMOVE input is present: in progress, aborted.
#[test]
fn incomplete_transaction_with_all_inputs_present_hides_its_outputs() {
    let tmp = staged();
    write_log(tmp.path(), false);
    let l = list(tmp.path());
    assert_eq!(live(&l), vec!["nb-1-big", "nb-2-big", "nb-3-big"]);
    assert_eq!(l.hidden.len(), FILES_PER_GENERATION);
    assert!(l
        .hidden
        .iter()
        .all(|h| h.reason == HiddenBy::UncommittedAdd));
    assert_all_accounted(tmp.path(), &l);
}

/// G2.1: incomplete log, inputs gone: the whole listing fails and names directory and log.
#[test]
fn incomplete_transaction_with_missing_inputs_fails_the_listing() {
    let tmp = staged();
    write_log(tmp.path(), false);
    for entry in std::fs::read_dir(tmp.path())
        .unwrap()
        .filter_map(|e| e.ok())
    {
        let name = entry.file_name().to_string_lossy().into_owned();
        if ["nb-1-", "nb-2-", "nb-3-"]
            .iter()
            .any(|p| name.starts_with(p))
        {
            std::fs::remove_file(entry.path()).unwrap();
        }
    }
    match list_generations(tmp.path(), &Scope::Live, TocPolicy::Accept) {
        Err(GenerationError::TxnInconsistent { directory, log }) => {
            assert_eq!(directory, tmp.path());
            assert_eq!(log, LOG_NAME);
        }
        other => panic!("expected TxnInconsistent, got {other:?}"),
    }
}

/// G2.2: a record that fails its checksum is not an error. Classification continues from the
/// readable records, as Cassandra's IGNORE lister does, and a directory note names the log.
#[test]
fn corrupt_record_follows_ignore_and_is_named_in_a_note() {
    let tmp = staged();
    write_log(tmp.path(), true);
    let path = tmp.path().join(LOG_NAME);
    let text = std::fs::read_to_string(&path).unwrap();
    // break the ADD record's checksum (its last digit)
    let mut lines: Vec<String> = text.lines().map(str::to_string).collect();
    let add = lines[0].clone();
    let (head, _) = add.rsplit_once('[').unwrap();
    lines[0] = format!("{head}[1]");
    std::fs::write(&path, lines.join("\n") + "\n").unwrap();

    let l = list(tmp.path());
    // the committed REMOVE records still hide the inputs; the invalid ADD record is not used
    assert_eq!(live(&l), vec!["nb-4-big"]);
    assert_eq!(l.notes.len(), 1);
    let DirectoryNote::CorruptTxnLog {
        path: noted,
        reasons,
    } = &l.notes[0]
    else {
        panic!("expected CorruptTxnLog, got {:?}", l.notes[0]);
    };
    assert_eq!(noted, &path);
    assert!(
        reasons.iter().any(|r| r.contains("Invalid checksum")),
        "{reasons:?}"
    );
    assert_all_accounted(tmp.path(), &l);
}

/// G2.2 with a stale mtime: a REMOVE record whose `updateTime` no longer matches its files (what a
/// copied or checked-out directory looks like) fails Cassandra's verification. It is an IGNORE-path
/// record, not an error: the other records still classify, and the note names the log.
#[test]
fn stale_mtime_remove_records_follow_ignore() {
    let src = fixture_table("txn_committed", "txn_compaction");
    let tmp = tempfile::tempdir().unwrap();
    for entry in std::fs::read_dir(&src).unwrap().filter_map(|e| e.ok()) {
        if !entry.file_name().to_string_lossy().ends_with(".log") {
            std::fs::copy(entry.path(), tmp.path().join(entry.file_name())).unwrap();
        }
    }
    // checkout mtimes: not stamped, so they differ from the records' updateTime
    write_log(tmp.path(), true);
    let l = list(tmp.path());
    assert_eq!(
        live(&l),
        vec!["nb-1-big", "nb-2-big", "nb-3-big", "nb-4-big"]
    );
    assert!(l.hidden.is_empty());
    assert_eq!(l.notes.len(), 1);
    let DirectoryNote::CorruptTxnLog { path, reasons } = &l.notes[0] else {
        panic!("expected CorruptTxnLog, got {:?}", l.notes[0]);
    };
    assert_eq!(path, &tmp.path().join(LOG_NAME));
    assert!(
        reasons
            .iter()
            .any(|r| r.contains("Unexpected files detected")),
        "{reasons:?}"
    );
}

/// A name that matches `LogFile.FILE_REGEX` but is not a valid log name fails like `LogFile.make`.
#[test]
fn invalid_log_name_fails_the_listing() {
    let tmp = staged();
    std::fs::write(
        tmp.path()
            .join("nb_txn_bogus_dcc367f0-e899-11f1-8c24-9315fc18c6f9.log"),
        "",
    )
    .unwrap();
    assert!(matches!(
        list_generations(tmp.path(), &Scope::Live, TocPolicy::Accept),
        Err(GenerationError::TxnLogName { .. })
    ));
}

/// G6.1: hidden inputs and a damaged log never lower the maximum, never block allocation.
#[test]
fn allocation_counts_hidden_generations_and_ignores_a_damaged_log() {
    let tmp = staged();
    write_log(tmp.path(), true);
    assert_eq!(
        next_generation_id(tmp.path()).unwrap(),
        GenerationId::Sequence(5)
    );

    let path = tmp.path().join(LOG_NAME);
    std::fs::write(&path, "ADD:[x][garbage\n").unwrap();
    assert_eq!(
        next_generation_id(tmp.path()).unwrap(),
        GenerationId::Sequence(5)
    );
    let l = list(tmp.path());
    assert_eq!(l.notes.len(), 1);
}

/// Q47 (owner ruling 2026-11-27): a Cassandra-written log whose records name another absolute
/// prefix applies when the keyspace directory, the `<table>-<tableid>` directory and the file name
/// match, and the listing says the log came from a relocated path.
#[test]
fn relocated_log_applies_and_emits_the_relocated_path_note() {
    let dir = fixture_table("txn_committed", "txn_compaction");
    let l = list(&dir);
    assert!(
        l.notes.iter().any(|n| matches!(
            n,
            DirectoryNote::RelocatedTxnLog { original_prefix, .. }
                if original_prefix == Path::new("/var/lib/cassandra/data")
        )),
        "{:?}",
        l.notes
    );
    assert_eq!(l.txn_logs.len(), 1);
}

/// Q47 negative: a record naming a different table id (or keyspace) never applies.
#[test]
fn record_naming_another_table_id_does_not_apply() {
    let src = fixture_table("txn_committed", "txn_compaction");
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp
        .path()
        .join("gd4626")
        .join("txn_compaction-ffffffffffffffffffffffffffffffff");
    std::fs::create_dir_all(&dir).unwrap();
    for entry in std::fs::read_dir(&src).unwrap().filter_map(|e| e.ok()) {
        std::fs::copy(entry.path(), dir.join(entry.file_name())).unwrap();
    }
    let l = list(&dir);
    assert_eq!(
        live(&l),
        vec!["nb-1-big", "nb-2-big", "nb-3-big", "nb-4-big"]
    );
    assert!(l.hidden.is_empty() && l.notes.is_empty(), "{:?}", l.notes);
    assert_eq!(l.txn_logs.len(), 1);
}

/// G1.1 on the Cassandra-written fixture itself.
#[test]
fn spec_g1_1_on_the_cassandra_written_fixture() {
    let (_root, dir) = stamped_fixture_copy("txn_committed");
    let l = list(&dir);
    assert_eq!(live(&l), vec!["nb-4-big"]);
}

/// G1.2 on the Cassandra-derived fixture.
#[test]
fn spec_g1_2_on_the_derived_fixture() {
    let (_root, dir) = stamped_fixture_copy("txn_incomplete");
    let l = list(&dir);
    assert_eq!(live(&l), vec!["nb-1-big", "nb-2-big", "nb-3-big"]);
}

/// G2.1 on the Cassandra-derived fixture.
#[test]
fn spec_g2_1_on_the_derived_fixture() {
    let (_root, dir) = stamped_fixture_copy("txn_inconsistent");
    assert!(matches!(
        list_generations(&dir, &Scope::Live, TocPolicy::Accept),
        Err(GenerationError::TxnInconsistent { .. })
    ));
}

/// Stage `root/<base>/gd4626/txn_compaction-<id>` with the compaction fixture's generations 1-4.
/// The REMOVE inputs are stamped with the record time only when `stamp` is true.
fn stage_under(root: &Path, base: &str, stamp: bool) -> std::path::PathBuf {
    let src = fixture_table("txn_committed", "txn_compaction");
    let dir = root
        .join(base)
        .join("gd4626")
        .join(src.file_name().unwrap());
    std::fs::create_dir_all(&dir).unwrap();
    for entry in std::fs::read_dir(&src).unwrap().filter_map(|e| e.ok()) {
        if !entry.file_name().to_string_lossy().ends_with(".log") {
            std::fs::copy(entry.path(), dir.join(entry.file_name())).unwrap();
        }
    }
    if stamp {
        stamp_inputs(&dir);
    }
    dir
}

/// The committed compaction log of `LOG_NAME`, its records naming `prefix_dir`, written into `into`.
fn write_log_naming(into: &Path, prefix_dir: &Path, commit: bool) {
    let prefix = |n: u32| format!("{}/nb-{n}-big-", prefix_dir.display());
    let n = FILES_PER_GENERATION as i32;
    let mut lines = vec![
        record("ADD", &prefix(4), 0, n),
        record("REMOVE", &prefix(3), STAMP_MS, n),
        record("REMOVE", &prefix(2), STAMP_MS, n),
        record("REMOVE", &prefix(1), STAMP_MS, n),
    ];
    if commit {
        lines.push(record("COMMIT", "", 0, 0));
    }
    std::fs::write(into.join(LOG_NAME), lines.join("\n") + "\n").unwrap();
}

fn corrupt_note(l: &Listing) -> bool {
    l.notes
        .iter()
        .any(|n| matches!(n, DirectoryNote::CorruptTxnLog { .. }))
}

/// Q47 + G8: a relocated record is verified against the COPY. The original path does not exist
/// here, and the copy's mtimes disagree with the records, so verification must say so.
#[test]
fn relocated_record_verifies_against_the_copy_when_the_original_is_absent() {
    let root = tempfile::tempdir().unwrap();
    let copy = stage_under(root.path(), "copy", false); // unstamped: mtimes differ from the log
                                                        // names the same table dir, under an absolute prefix that is not there
    let named = root
        .path()
        .join("gone")
        .join("gd4626")
        .join(copy.file_name().unwrap());
    write_log_naming(&copy, &named, true);
    let l = list(&copy);
    assert!(corrupt_note(&l), "{:?}", l.notes);
}

/// Q47 + G8: when the original path DOES exist on this host the module never reads it. The
/// original holds files stamped to match the records; the copy does not, so the copy decides.
#[test]
fn relocated_record_never_reads_the_original_that_exists() {
    let root = tempfile::tempdir().unwrap();
    let original = stage_under(root.path(), "orig", true); // would verify cleanly
    let copy = stage_under(root.path(), "copy", false);
    write_log_naming(&copy, &original, true);
    let l = list(&copy);
    assert!(corrupt_note(&l), "{:?}", l.notes);
    // and the converse: the copy is stamped, the original is not
    let root = tempfile::tempdir().unwrap();
    let original = stage_under(root.path(), "orig", false);
    let copy = stage_under(root.path(), "copy", true);
    write_log_naming(&copy, &original, true);
    let l = list(&copy);
    assert!(!corrupt_note(&l), "{:?}", l.notes);
    assert_eq!(live(&l), vec!["nb-4-big"]);
}

fn snapshot_with_log(
    records_dir: &str,
    scope_sub: &str,
) -> (tempfile::TempDir, std::path::PathBuf) {
    let root = tempfile::tempdir().unwrap();
    let table = root.path().join("gd4626").join("txn_compaction-aaaa");
    let sub = table.join(scope_sub);
    std::fs::create_dir_all(&sub).unwrap();
    let src = fixture_table("txn_committed", "txn_compaction");
    for entry in std::fs::read_dir(&src).unwrap().filter_map(|e| e.ok()) {
        if !entry.file_name().to_string_lossy().ends_with(".log") {
            std::fs::copy(entry.path(), sub.join(entry.file_name())).unwrap();
        }
    }
    let prefix = |n: u32| format!("{records_dir}/nb-{n}-big-");
    let lines = [
        record("REMOVE", &prefix(1), 0, 8),
        record("COMMIT", "", 0, 0),
    ];
    std::fs::write(sub.join(LOG_NAME), lines.join("\n") + "\n").unwrap();
    (root, table)
}

/// Q47 applies to Scope::Live only: another table's `snapshots/<tag>` names the same last two
/// components as the listed snapshot directory but is not this directory's record.
#[test]
fn snapshot_scope_record_naming_another_tables_snapshot_does_not_apply() {
    let (_root, table) = snapshot_with_log("/x/gd4626/other-bbbb/snapshots/tag", "snapshots/tag");
    let l = list_generations(&table, &Scope::Snapshot("tag".into()), TocPolicy::Accept).unwrap();
    assert_eq!(l.live.len(), 4);
    assert!(l.hidden.is_empty() && l.notes.is_empty(), "{:?}", l.notes);
}

/// Neither does a backups record under another keyspace.
#[test]
fn backups_scope_record_under_another_keyspace_does_not_apply() {
    let (_root, table) = snapshot_with_log("/x/otherks/txn_compaction-aaaa/backups", "backups");
    let l = list_generations(&table, &Scope::Backups, TocPolicy::Accept).unwrap();
    assert_eq!(l.live.len(), 4);
    assert!(l.hidden.is_empty() && l.notes.is_empty(), "{:?}", l.notes);
}

/// G8: a log applies within its one directory. A record for a file in a subdirectory is not a
/// record for the directory's files, exact or relocated.
#[test]
fn record_for_a_file_in_a_subdirectory_does_not_apply() {
    let tmp = staged();
    let canonical = std::fs::canonicalize(tmp.path()).unwrap();
    let lines = [
        record(
            "REMOVE",
            &format!("{}/sub/nb-1-big-", canonical.display()),
            0,
            0,
        ),
        record("COMMIT", "", 0, 0),
    ];
    std::fs::write(tmp.path().join(LOG_NAME), lines.join("\n") + "\n").unwrap();
    let l = list(tmp.path());
    assert!(l.hidden.is_empty(), "{:?}", l.hidden);

    let dir = fixture_table("txn_committed", "txn_compaction");
    let name = dir.file_name().unwrap().to_string_lossy().into_owned();
    let root = tempfile::tempdir().unwrap();
    let copy = root.path().join("gd4626").join(&name);
    std::fs::create_dir_all(&copy).unwrap();
    for e in std::fs::read_dir(&dir).unwrap().filter_map(|e| e.ok()) {
        if !e.file_name().to_string_lossy().ends_with(".log") {
            std::fs::copy(e.path(), copy.join(e.file_name())).unwrap();
        }
    }
    let lines = [
        record("REMOVE", &format!("/o/gd4626/{name}/sub/nb-1-big-"), 0, 0),
        record("COMMIT", "", 0, 0),
    ];
    std::fs::write(copy.join(LOG_NAME), lines.join("\n") + "\n").unwrap();
    let l = list(&copy);
    assert!(l.hidden.is_empty() && l.notes.is_empty(), "{:?}", l.notes);
}

/// Pin (`LogFile.java:195-198` vs `:214-217`): `completed` is decided before checksums are
/// verified, so a final COMMIT with a bad checksum IS complete, but is not `committed()` (which
/// re-reads validity after verification): the ADD files are hidden, the REMOVE inputs are not.
#[test]
fn final_commit_with_a_bad_checksum_is_complete_but_not_committed() {
    let tmp = staged();
    write_log(tmp.path(), true);
    let path = tmp.path().join(LOG_NAME);
    let text = std::fs::read_to_string(&path).unwrap();
    let mut lines: Vec<String> = text.lines().map(String::from).collect();
    let last = lines.last_mut().unwrap();
    *last = "COMMIT:[,0,0][1]".to_string();
    std::fs::write(&path, lines.join("\n") + "\n").unwrap();
    let l = list(tmp.path());
    assert!(corrupt_note(&l));
    assert_eq!(live(&l), vec!["nb-1-big", "nb-2-big", "nb-3-big"]);
}
