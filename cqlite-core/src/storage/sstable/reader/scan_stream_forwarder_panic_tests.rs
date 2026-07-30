//! Issue #3124, site 4 — END-TO-END pin: the windowed scan's FORWARDER task dying
//! must FAIL the scan, not end it as a clean (silently short) one.
//!
//! # Why this needs a real compressed fixture
//!
//! The forwarder exists only on the chunk-stitching branch
//! (`requires_chunk_stitching()`), i.e. for SSTables with a `CompressionInfo.db`.
//! CQLite's write surface emits UNCOMPRESSED SSTables only (issue #1406), so no
//! CQLite-written fixture can reach this code at all — the oracle has to be a real
//! Cassandra 5.0 compressed SSTable from `test-data/datasets`. The test asserts
//! `requires_chunk_stitching()` up front, so a fixture that took the other branch
//! fails LOUDLY instead of passing vacuously.
//!
//! Dataset-dependent: SKIPS when the fixture's `Data.db` is absent (it is gitignored
//! and fetched by `test-data/scripts/fetch-datasets.sh`). Present-but-empty is a
//! FAILURE, never a vacuous pass — the control arm asserts a non-zero complete count.
//!
//! # The fixture is COPIED to a private `TempDir`
//!
//! The fault arm is keyed by `(site, scope)` where the scope is matched against the
//! reader's `Data.db` path. Scoped to the shared dataset path, a sibling test in this
//! same lib test binary scanning the same table could consume the arm (libtest runs
//! tests in parallel), and this test would then fail for the wrong reason. Copying the
//! ~200 KiB component set into a per-run `TempDir` makes the scope unique by
//! construction.
//!
//! Included via `#[cfg(test)] #[path = ...] mod panic_tests;` in [`super`].

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tempfile::TempDir;

use super::super::SSTableReader;
use crate::platform::Platform;
use crate::storage::producer_fault::{
    arm_scan_task_panic, silence_injected_panics, ScanTaskSite, INJECTED_PANIC_MESSAGE,
};
use crate::types::TableId;
use crate::Config;

/// A COMPRESSED Cassandra 5.0 fixture, i.e. one that takes the chunk-stitching branch
/// the forwarder lives on.
const KEYSPACE: &str = "test_wide_rows";
const TABLE: &str = "wide_partition_table";

/// Small enough that the forwarder parks in backpressure, so the fault lands with the
/// scan genuinely in flight rather than after everything has drained.
const BUFFER: usize = 2;

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

/// The fixture's SSTable component DIRECTORY (containing `*-Data.db` +
/// `*-CompressionInfo.db`), if present.
fn compressed_fixture_dir() -> Option<PathBuf> {
    let table_root = datasets_root()?.join("sstables").join(KEYSPACE);
    for entry in std::fs::read_dir(&table_root).ok()?.flatten() {
        if !entry.path().is_dir()
            || !entry
                .file_name()
                .to_string_lossy()
                .starts_with(&format!("{TABLE}-"))
        {
            continue;
        }
        let files: Vec<String> = std::fs::read_dir(entry.path())
            .ok()?
            .flatten()
            .filter_map(|f| f.file_name().to_str().map(str::to_owned))
            .collect();
        if files.iter().any(|n| n.ends_with("-Data.db"))
            && files.iter().any(|n| n.ends_with("-CompressionInfo.db"))
        {
            return Some(entry.path());
        }
    }
    None
}

/// Copy every component file into `dest` and return the copied `Data.db` path, so this
/// test's reader has a path no other test can match.
fn copy_fixture(src_dir: &Path, dest: &Path) -> PathBuf {
    std::fs::create_dir_all(dest).expect("create fixture copy dir");
    let mut data_db = None;
    for entry in std::fs::read_dir(src_dir)
        .expect("read fixture dir")
        .flatten()
    {
        let name = entry.file_name();
        let target = dest.join(&name);
        if entry.path().is_file() {
            std::fs::copy(entry.path(), &target).expect("copy fixture component");
            if name.to_string_lossy().ends_with("-Data.db") {
                data_db = Some(target);
            }
        }
    }
    data_db.expect("the copied fixture must contain a Data.db")
}

/// How a drained stream ended, plus how many rows it delivered.
struct Drained {
    rows: usize,
    error: Option<String>,
}

async fn drain(reader: &Arc<SSTableReader>) -> Drained {
    // The stitching path does not filter by `table_id` (issue #1578), so any id drives
    // the full scan; naming the real one keeps the call honest.
    let table_id = TableId::from(format!("{KEYSPACE}.{TABLE}").as_str());
    let mut stream = Arc::clone(reader).scan_stream(table_id, None, None, None, BUFFER);
    let mut rows = 0usize;
    while let Some(item) = stream.recv().await {
        match item {
            Ok(_) => rows += 1,
            Err(e) => {
                return Drained {
                    rows,
                    error: Some(e.to_string()),
                }
            }
        }
    }
    Drained { rows, error: None }
}

/// The pin: with the windowed forwarder killed, the scan must terminate with an ERROR
/// and a SHORT row count.
///
/// RED (pre-fix): the driver joined the forwarder as `let _ = forwarder.await;`, so a
/// dead forwarder dropped both the caller's rows and `batch_rx` (stopping the parse
/// half as if the consumer had left), the driver returned `Ok(())`, and the caller saw
/// a clean end of stream over a truncated result set. GREEN: the join outcome is
/// observed and becomes an `Error::Internal`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dead_windowed_forwarder_fails_the_scan_instead_of_truncating_it_silently() {
    let Some(fixture_dir) = compressed_fixture_dir() else {
        eprintln!(
            "Skipping {KEYSPACE}.{TABLE}: no compressed Data.db present (run \
             test-data/scripts/fetch-datasets.sh). The windowed forwarder exists only on \
             the chunk-stitching branch, which needs a real compressed SSTable."
        );
        return;
    };
    let temp_dir = TempDir::new().expect("tempdir");
    let data_path = copy_fixture(&fixture_dir, &temp_dir.path().join("fixture"));

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    let reader = Arc::new(
        SSTableReader::open(&data_path, &config, platform)
            .await
            .expect("open reader"),
    );
    assert!(
        reader.requires_chunk_stitching(),
        "test precondition: this fixture must take the chunk-stitching branch, which is \
         the only branch that spawns the forwarder under test"
    );

    // ── Control arm: no fault. Establishes the COMPLETE row count and that a healthy
    // scan still ends cleanly (the observed forwarder join must not invent an error).
    let complete = drain(&reader).await;
    assert_eq!(
        complete.error, None,
        "a healthy windowed scan must still end CLEANLY — observing the forwarder's \
         join must not turn a live forwarder into an error"
    );
    assert!(
        complete.rows > 0,
        "test precondition: the control drain must return rows for a PRESENT fixture, \
         or 'the faulted drain is short' proves nothing (0-rows-when-present is a \
         failure, never a vacuous pass)"
    );

    // ── Fault arm: the forwarder panics at its entry checkpoint, with the parse half
    // already producing — so every row it would have forwarded is lost.
    let scope = temp_dir.path().to_string_lossy().to_string();
    let faulted = {
        // Silence ONLY the injected panic, and restore the hook before any assertion.
        let _silence = silence_injected_panics();
        let _fault = arm_scan_task_panic(&scope, ScanTaskSite::WindowedForwarder);
        drain(&reader).await
    };

    let message = faulted.error.expect(
        "issue #3124 site 4: a forwarder that PANICKED must fail the scan. A clean end \
         of stream here is a silently TRUNCATED result set reported as a complete scan",
    );
    assert!(
        message.contains("DIED without reporting") && message.contains("TRUNCATED"),
        "the error must name the dead forwarder and the truncation, got: {message}"
    );
    assert!(
        message.contains(INJECTED_PANIC_MESSAGE),
        "the error must carry the injected panic's message, proving THIS fault ended \
         the scan rather than an unrelated failure, got: {message}"
    );
    assert!(
        faulted.rows < complete.rows,
        "the faulted drain returned {} of {} rows — it is not short, so the fault \
         truncated nothing and the test would be vacuous",
        faulted.rows,
        complete.rows
    );
}
