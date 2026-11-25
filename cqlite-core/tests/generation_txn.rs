//! Transaction-log liveness (#4626, spec G1, G2, G6.1, G13) against Cassandra's
//! `LogAwareFileLister` semantics.
//!
//! The SSTable bytes are Cassandra-written (copied from `test-data/fixtures/issue_4626/txn_*`).
//! Cassandra's own log records carry the container's absolute paths, which Cassandra refuses to
//! apply to any other directory (`LogRecord.isInFolder`), so the cases that need a log to take
//! effect rebuild the SAME five records for the tempdir's path (the checksum function was proven
//! against Cassandra-written records in the module's unit tests) and set each REMOVE input's
//! mtime to the record's `updateTime`, restoring the state Cassandra wrote.

#[path = "support/datasets_root.rs"]
mod datasets_root;
#[path = "support/generation_fixtures.rs"]
mod generation_fixtures;

use std::path::Path;
use std::time::{Duration, UNIX_EPOCH};

use cqlite_core::storage::sstable::generation::{
    list_generations, next_generation_id, Condition, DirectoryNote, GenerationError, GenerationId,
    HiddenBy, Listing, Scope, TocPolicy,
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
    } = &l.notes[0];
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
    let DirectoryNote::CorruptTxnLog { path, reasons } = &l.notes[0];
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

/// What Cassandra itself does with its own committed log in a directory it was not written for:
/// `LogRecord.isInFolder` is false for every record, so nothing is hidden. This pins the faithful
/// port against the Cassandra-written bytes; see the report for the consequence.
#[test]
fn cassandra_written_log_in_a_relocated_directory_hides_nothing() {
    for (state, expected) in [
        (
            "txn_committed",
            vec!["nb-1-big", "nb-2-big", "nb-3-big", "nb-4-big"],
        ),
        (
            "txn_incomplete",
            vec!["nb-1-big", "nb-2-big", "nb-3-big", "nb-4-big"],
        ),
    ] {
        let dir = fixture_table(state, "txn_compaction");
        let l = list(&dir);
        assert_eq!(live(&l), expected, "{state}");
        assert!(l.hidden.is_empty() && l.notes.is_empty(), "{state}");
        assert_eq!(l.txn_logs.len(), 1, "{state}");
        assert!(l
            .live
            .iter()
            .all(|g| g.components().iter().all(|c| !c.name.ends_with(".log"))));
        assert!(l.live.iter().all(|g| !g.has_condition(Condition::Orphan)));
    }
}

const BLOCKED: &str =
    "blocked on an owner decision (#4626 report): Cassandra's isInFolder makes its \
own log inert outside the directory it was written for, so the spec outcome on the committed \
fixture cannot hold under a faithful port";

/// G1.1 as the spec states it, on the Cassandra-written fixture itself.
#[test]
#[ignore = "blocked on an owner decision (#4626 report): isInFolder vs spec G1.1 on relocated fixtures"]
fn spec_g1_1_on_the_cassandra_written_fixture() {
    let _ = BLOCKED;
    let l = list(&fixture_table("txn_committed", "txn_compaction"));
    assert_eq!(live(&l), vec!["nb-4-big"]);
}

/// G1.2 as the spec states it, on the Cassandra-derived fixture.
#[test]
#[ignore = "blocked on an owner decision (#4626 report): isInFolder vs spec G1.2 on relocated fixtures"]
fn spec_g1_2_on_the_derived_fixture() {
    let l = list(&fixture_table("txn_incomplete", "txn_compaction"));
    assert_eq!(live(&l), vec!["nb-1-big", "nb-2-big", "nb-3-big"]);
}

/// G2.1 as the spec states it, on the Cassandra-derived fixture.
#[test]
#[ignore = "blocked on an owner decision (#4626 report): isInFolder vs spec G2.1 on relocated fixtures"]
fn spec_g2_1_on_the_derived_fixture() {
    let dir = fixture_table("txn_inconsistent", "txn_compaction");
    assert!(matches!(
        list_generations(&dir, &Scope::Live, TocPolicy::Accept),
        Err(GenerationError::TxnInconsistent { .. })
    ));
}
