//! Issue #4626 phase 3 (tasks 3.3 and 3.5; GD8, GD9): the library surfaces under `diagnose` and
//! the directory scan list through the generation module.
//!
//! Oracles: Cassandra 5.0.8-written fixtures (`test-data/fixtures/issue_4626`), copied to a
//! tempdir before any mutation. Each resolver panics by name when its fixture is missing (#3220).

#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

#[path = "support/datasets_root.rs"]
mod datasets_root;
#[path = "support/generation_fixtures.rs"]
mod generation_fixtures;

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use cqlite_core::storage::sstable::diagnose::{
    diagnose_table, diagnose_table_with, DiagnoseOptions,
};
use cqlite_core::storage::sstable::directory::SSTableDirectory;
use cqlite_core::storage::sstable::generation::Condition;
use generation_fixtures::*;
use tempfile::TempDir;

fn options(deep: bool) -> DiagnoseOptions {
    DiagnoseOptions {
        now_secs: 1_795_588_300,
        deep,
        top_n: 3,
        gc_grace_seconds: None,
        schema: None,
    }
}

fn copy_fixture(state: &str, table: &str) -> (TempDir, std::path::PathBuf) {
    let root = TempDir::new().unwrap();
    let src = fixture_table(state, table);
    let dir = root.path().join(src.file_name().unwrap());
    copy_dir(&src, &dir);
    (root, dir)
}

/// GD9.1: a five-generation Cassandra-written table, a counting file-open shim on the one
/// `Statistics.db` read diagnose owns. With `--deep` (the second consumer of the same metadata,
/// R4's overlap and R3's scan) each `*-Statistics.db` is opened exactly once by the diagnose
/// code, and every generation was in fact read.
///
/// Scope: the shim sees the reads diagnose makes. `SSTableReader::open` (the deep scan) and the
/// merge's dry compaction read `Statistics.db` through their own paths, which belong to phase 5
/// (task 5.1) and are not counted here.
#[tokio::test]
async fn each_statistics_db_is_read_once_per_diagnose_run() {
    let (_root, dir) = copy_fixture("snap_backup", "snap_backup");
    let opens: Arc<Mutex<BTreeMap<String, u32>>> = Arc::default();
    let counter = opens.clone();
    let counting = move |path: &Path| {
        *counter
            .lock()
            .unwrap()
            .entry(path.file_name().unwrap().to_string_lossy().into_owned())
            .or_default() += 1;
        cqlite_core::storage::sstable::statistics_reader::integrity::read_checked_statistics(path)
    };
    let report = diagnose_table_with(&dir, &options(true), Arc::new(counting))
        .await
        .expect("diagnose --deep");
    assert_eq!(
        report.generations.len(),
        5,
        "{:?}",
        report.unreadable_generations
    );
    let opens = opens.lock().unwrap().clone();
    assert_eq!(opens.len(), 5, "{opens:?}");
    for (name, n) in &opens {
        assert!(name.ends_with("-Statistics.db"), "{name}");
        assert_eq!(*n, 1, "{name} was opened {n} times");
    }
}

/// Roborev round 2 (2): the public diagnose futures are `Send`, so a multithreaded runtime can
/// spawn them. A compile-time assertion.
#[tokio::test]
async fn the_diagnose_futures_are_send() {
    fn assert_send<T: Send>(_: &T) {}
    let (_root, dir) = copy_fixture("snap_backup", "snap_backup");
    let opts = options(false);
    let fut = diagnose_table(&dir, &opts);
    assert_send(&fut);
    let fut2 = diagnose_table_with(
        &dir,
        &opts,
        Arc::new(
            cqlite_core::storage::sstable::statistics_reader::integrity::read_checked_statistics,
        ),
    );
    assert_send(&fut2);
    let report = tokio::spawn({
        let dir = dir.clone();
        async move { diagnose_table(&dir, &options(false)).await }
    })
    .await
    .unwrap()
    .expect("diagnose on a spawned task");
    assert_eq!(report.generations.len(), 5);
}

/// GD8.1: `TocMissing` is carried onto the report, the generation is still diagnosed.
#[tokio::test]
async fn toc_missing_is_reported_and_the_generation_is_still_diagnosed() {
    let (_root, dir) = copy_fixture("snap_backup", "snap_backup");
    std::fs::remove_file(dir.join("nb-2-big-TOC.txt")).unwrap();
    let report = diagnose_table(&dir, &options(false))
        .await
        .expect("diagnose");
    assert_eq!(report.generations.len(), 5);
    let conditions: Vec<(&str, Vec<Condition>)> = report
        .generations
        .iter()
        .map(|g| (g.generation.as_str(), g.conditions.clone()))
        .collect();
    assert_eq!(conditions[1], ("2", vec![Condition::TocMissing]));
    assert!(conditions.iter().filter(|(_, c)| !c.is_empty()).count() == 1);
}

/// GD8: a truncated `Statistics.db` is an `unreadable_generations` entry naming the condition and
/// the path, never an `Err`.
#[tokio::test]
async fn a_corrupt_statistics_db_is_an_unreadable_generation_not_a_refusal() {
    let (_root, dir) = copy_fixture("snap_backup", "snap_backup");
    let stats = dir.join("nb-4-big-Statistics.db");
    let len = std::fs::metadata(&stats).unwrap().len();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&stats)
        .unwrap()
        .set_len(len - 5)
        .unwrap();
    let report = diagnose_table(&dir, &options(false))
        .await
        .expect("diagnose");
    assert_eq!(report.generations.len(), 4);
    let [bad] = report.unreadable_generations.as_slice() else {
        panic!("{:?}", report.unreadable_generations);
    };
    assert!(bad.data_path.ends_with("nb-4-big-Data.db"));
    assert!(bad.cause.starts_with("Corrupt"), "{}", bad.cause);
    assert!(
        bad.cause.contains("nb-4-big-Statistics.db"),
        "{}",
        bad.cause
    );
}

/// Q45 / G2.1: an inconsistent transaction state is an `Err` naming the log (the CLI exits 1).
#[tokio::test]
async fn an_inconsistent_transaction_state_is_an_error_naming_the_log() {
    let (_root, dir) = stamped_fixture_copy("txn_inconsistent");
    let error = diagnose_table(&dir, &options(false)).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("nb_txn_compaction_dcc367f0-e899-11f1-8c24-9315fc18c6f9.log"),
        "{error}"
    );
}

/// Q44 / G1.1: a committed compaction's inputs are not diagnosed, and the relocated log is a note.
#[tokio::test]
async fn a_committed_compaction_hides_its_inputs_and_notes_the_relocated_log() {
    let (_root, dir) = stamped_fixture_copy("txn_committed");
    let report = diagnose_table(&dir, &options(false))
        .await
        .expect("diagnose");
    let ids: Vec<&str> = report
        .generations
        .iter()
        .map(|g| g.generation.as_str())
        .collect();
    assert_eq!(ids, ["4"]);
    assert_eq!(report.notes.len(), 1, "{:?}", report.notes);
}

/// Task 3.5: `SSTableDirectory::scan` follows the module. A committed compaction's inputs are not
/// scanned, and a UUID-id generation is a NAMED error rather than the silent skip the old
/// filename parser did.
#[test]
fn the_directory_scan_follows_the_generation_module() {
    let (_root, dir) = stamped_fixture_copy("txn_committed");
    let scanned = SSTableDirectory::scan(&dir).expect("scan");
    let ids: Vec<u32> = scanned.generations.iter().map(|g| g.generation).collect();
    assert_eq!(ids, [4]);

    let (_root, dir) = copy_fixture("snap_backup", "snap_backup");
    let scanned = SSTableDirectory::scan(&dir).expect("scan");
    let ids: Vec<u32> = scanned.generations.iter().map(|g| g.generation).collect();
    assert_eq!(
        ids,
        [5, 4, 3, 2, 1],
        "snapshots/ and backups/ are never read"
    );

    let (_root, dir) = copy_fixture("uuid_ids", "uuid_mixed");
    let error = SSTableDirectory::scan(&dir).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("nb-3h5t_0i6r_2dipm2lxu6305iz8wl-big"),
        "{error}"
    );
}

/// #4671 finding 6: the wording of a condition is a stable, pinned contract (verbs print it).
#[test]
fn condition_display_wording_is_pinned() {
    // An exhaustive match: a new variant fails to compile here until its wording is pinned.
    for c in [
        Condition::TocMissing,
        Condition::Orphan,
        Condition::UnsupportedVersion,
        Condition::Unreadable,
        Condition::Corrupt,
        Condition::Absent,
    ] {
        let want = match c {
            Condition::TocMissing => "TocMissing",
            Condition::Orphan => "Orphan",
            Condition::UnsupportedVersion => "UnsupportedVersion",
            Condition::Unreadable => "Unreadable",
            Condition::Corrupt => "Corrupt",
            Condition::Absent => "Absent",
        };
        assert_eq!(c.to_string(), want);
    }
}

/// A `MakeWriter` collecting everything `tracing` emits on this thread.
#[derive(Clone, Default)]
struct Captured(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
    type Writer = Captured;
    fn make_writer(&'a self) -> Captured {
        self.clone()
    }
}

/// #4671 finding 6: the directory scan does not drop a corrupt-log note silently: it warns,
/// naming the log. (An inconsistent state stays an error, covered by the module's own tests.)
#[test]
fn the_directory_scan_warns_about_a_corrupt_transaction_log() {
    let (_root, dir) = stamped_fixture_copy("txn_committed");
    let log = dir.join("nb_txn_compaction_dcc367f0-e899-11f1-8c24-9315fc18c6f9.log");
    let text = std::fs::read_to_string(&log).unwrap();
    std::fs::write(&log, text.replace("[1949738987]", "[1949738988]")).unwrap();
    let captured = Captured::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(captured.clone())
        .with_max_level(tracing::Level::WARN)
        .finish();
    let scanned = tracing::subscriber::with_default(subscriber, || SSTableDirectory::scan(&dir));
    scanned.expect("a corrupt record is a note, not a refusal");
    let logged = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
    assert!(
        logged.contains("WARN")
            && logged.contains("nb_txn_compaction_dcc367f0-e899-11f1-8c24-9315fc18c6f9.log"),
        "{logged}"
    );
}
