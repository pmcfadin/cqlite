//! Issue #1704 — a scan that FAILS must land in `cqlite.errors.total`.
//!
//! `ERRORS_TOTAL` (`cqlite.errors.total{category,subsystem}`, issue #1038) was
//! emitted for engine/open/lookup/write/compaction errors, but NOTHING in the
//! scan paths recorded one: mid-scan corruption propagated to the caller
//! correctly and yet an operator's error dashboard stayed clean while every scan
//! of the table failed. These tests pin the three disjoint scan exit seams:
//!
//! 1. the streaming surfaces, through `JoinedStream::recv`
//!    (`SSTableReader::scan_stream` and everything layered on it);
//! 2. `scan_delta` (issue #698), which returns a RAW `mpsc::Receiver` and so is
//!    NOT covered by seam 1;
//! 3. the materializing `SSTableReader::scan` (index / sequential / BTI).
//!
//! # What is asserted, and why not the issue's literal AC1
//!
//! AC1 as filed asks for `{category=corruption, subsystem=reader} == 1` on the
//! bit-flip fixture. That is UNSATISFIABLE without changing either the classifier
//! or the error variant, and both are out of scope: the per-chunk CRC mismatch
//! surfaces as `Error::InvalidFormat` (documented at
//! `issue_1397_corrupt_query_surface.rs:30-38`; variant unification is tracked on
//! #1411), which `error_schema::classify` maps to `parsing`, not `corruption`.
//! Forcing the literal string would mean hand-rolling a category at the call site
//! — exactly the inline classification the issue forbids.
//!
//! So each seam asserts the two properties that actually matter:
//!
//! * EXACTLY ONE `{subsystem=reader}` increment per failed scan operation — not
//!   zero (the defect), not one per chunk/retry/`recv`; and
//! * the emitted `category` EQUALS `err.obs_category().as_str()` of the terminal
//!   error the caller actually received. That is a mechanical proof that the
//!   classifier is the sole authority, WITHOUT pinning a literal that a future
//!   (correct) variant change would falsify.
//!
//! `{category=corruption}` is additionally pinned LITERALLY where a path genuinely
//! produces `Error::Corruption`: `scan_delta` wraps its open/parse failures in
//! `Error::corruption(...)` (`delta_scan/scan.rs`), so the delta seam's terminal
//! error classifies to `corruption` and that test asserts the literal label.
//!
//! # Fixture
//!
//! `corruption/test_comp_corrupt/data_db_bit_flip/nb-1-big-Data.db` — real
//! Cassandra 5.0.2 bytes with ONE bit flipped (offset 64, `0x61`→`0x60`) inside
//! LZ4 chunk 0 of `test_comp.lz4_table`, a single-partition table whose whole
//! payload lives in chunk 0. Corruption is therefore unavoidable on any read of
//! the data section. Gating follows #1094 doctrine (see `require_fixtures`).
//!
//! # Why one serial test per scenario
//!
//! The production metric helpers record through a single process-global `Meter`
//! bound on first use, and the capture harness uses DELTA temporality — the same
//! discipline `observability_correctness.rs` documents. Each scenario resets the
//! capture and reads it back, so they must not overlap; `#[serial]` enforces that
//! within this binary, and a `tests/*.rs` file is its own process, so there is no
//! cross-file race.
//!
//! Run with:
//! ```text
//! cargo test -p cqlite-core \
//!   --features observability-testing,delta-scan --test issue_1704_scan_path_error_counts
//! ```

#![cfg(all(feature = "observability-testing", feature = "delta-scan"))]

use cqlite_core::observability::{catalog, testing};
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::types::TableId;
use cqlite_core::{Config, Error, Platform};
use serial_test::serial;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Relative path of the corrupt COMPRESSED `Data.db` under the datasets root.
const CORRUPT_DATA_DB: &str = "corruption/test_comp_corrupt/data_db_bit_flip/nb-1-big-Data.db";

/// Relative path of the CLEAN source `Data.db` the corrupt fixture was derived from.
/// Used by the negative control: a SUCCESSFUL scan must record nothing.
const CLEAN_DATA_DB: &str =
    "sstables/test_comp/lz4_table-25801a0071a911f19b3225f9984c6a77/nb-1-big-Data.db";

/// Fully-qualified table the corrupt fixture was derived from.
const TABLE: &str = "test_comp.lz4_table";

/// `true` when the full-dataset/nightly lanes demand the corpus be present.
fn require_fixtures() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE")
    )
}

/// Locate the datasets root, honoring `CQLITE_DATASETS_ROOT` with a worktree fallback.
fn datasets_root() -> Option<PathBuf> {
    if let Ok(root) = std::env::var("CQLITE_DATASETS_ROOT") {
        let p = PathBuf::from(root);
        if p.is_dir() {
            return Some(p);
        }
    }
    let fallback = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(|p| p.join("test-data/datasets"))?;
    fallback.is_dir().then_some(fallback)
}

/// Resolve the corrupt `Data.db`, applying the #1094 fail-closed gate:
/// present → `Some`; absent + `CQLITE_REQUIRE_FIXTURES=1` → panic; absent → skip-clean.
///
/// A fixture that is PRESENT but no longer corrupt is not skipped: every test below
/// asserts an `Err`, so regeneration rot fails the suite unconditionally.
fn corrupt_data_db_or_gate() -> Option<PathBuf> {
    match datasets_root().map(|r| r.join(CORRUPT_DATA_DB)) {
        Some(p) if p.is_file() => Some(p),
        _ => {
            assert!(
                !require_fixtures(),
                "CQLITE_REQUIRE_FIXTURES=1 but the corruption fixture is absent: \
                 {CORRUPT_DATA_DB}. Fetch the corpus \
                 (bash test-data/scripts/fetch-datasets.sh)."
            );
            eprintln!(
                "SKIP: corruption fixture absent ({CORRUPT_DATA_DB}); \
                 set CQLITE_REQUIRE_FIXTURES=1 to enforce."
            );
            None
        }
    }
}

/// The CLEAN fixture, under the same #1094 gate as [`corrupt_data_db_or_gate`]. It is
/// a FETCHED (gitignored) corpus binary, so it is skip-clean when absent and a hard
/// failure under `CQLITE_REQUIRE_FIXTURES=1` — never a silent skip that would let the
/// negative control stop running behind a green suite (#3220).
fn clean_data_db_or_gate() -> Option<PathBuf> {
    match datasets_root().map(|r| r.join(CLEAN_DATA_DB)) {
        Some(p) if p.is_file() => Some(p),
        _ => {
            assert!(
                !require_fixtures(),
                "CQLITE_REQUIRE_FIXTURES=1 but the clean lz4_table fixture is absent: \
                 {CLEAN_DATA_DB}. Fetch the corpus \
                 (bash test-data/scripts/fetch-datasets.sh)."
            );
            eprintln!(
                "SKIP: clean lz4_table fixture absent ({CLEAN_DATA_DB}); \
                 set CQLITE_REQUIRE_FIXTURES=1 to enforce."
            );
            None
        }
    }
}

/// The DIRECTORY holding the corrupt fixture — `scan_delta` takes a generation dir.
fn corrupt_table_dir_or_gate() -> Option<PathBuf> {
    corrupt_data_db_or_gate().and_then(|p| p.parent().map(Path::to_path_buf))
}

async fn open_reader(path: &Path) -> SSTableReader {
    let config = Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("platform init should succeed"),
    );
    SSTableReader::open(path, &config, platform)
        .await
        .expect("the corrupt fixture is structurally valid: corruption is in a chunk payload")
}

/// The typed, non-recoverable chunk-CRC error #1397 pins. Re-asserted here so each
/// seam proves the error the caller receives is UNCHANGED by the new emission —
/// recording is a pure side effect, never a rewrap or a swallow.
fn assert_typed_chunk_corruption(err: &Error) {
    assert!(
        !err.is_recoverable(),
        "chunk CRC-mismatch must stay non-recoverable, got recoverable: {err}"
    );
    let msg = err.to_string();
    assert!(
        msg.contains("chunk 0"),
        "corruption error must still name the chunk index ('chunk 0'), got: {msg}"
    );
    assert!(
        msg.to_uppercase().contains("CRC"),
        "corruption error must still identify the CRC mismatch, got: {msg}"
    );
}

/// Assert the capture holds EXACTLY ONE `cqlite.errors.total{subsystem=reader}`
/// increment, carried on the category the CLASSIFIER derives for `err` — never a
/// literal the call site chose.
fn assert_one_reader_error_classified_as(m: &testing::CapturedMetrics, err: &Error, seam: &str) {
    let total = m.sum_where(
        catalog::ERRORS_TOTAL,
        &[(catalog::attr::SUBSYSTEM, "reader")],
    );
    assert_eq!(
        total,
        1.0,
        "{seam}: a failed scan must record EXACTLY ONE \
         cqlite.errors.total{{subsystem=reader}} increment (0 = the #1704 defect, \
         >1 = double counting); entry: {:?}",
        m.find(catalog::ERRORS_TOTAL)
    );

    // The category must be the CLASSIFIER's answer for the error the caller got.
    // Asserting equality with `obs_category()` (rather than a literal) is what
    // proves no inline classification happened at the seam.
    let expected = err.obs_category().as_str();
    let classified = m.sum_where(
        catalog::ERRORS_TOTAL,
        &[
            (catalog::attr::SUBSYSTEM, "reader"),
            (catalog::attr::ERROR_CATEGORY, expected),
        ],
    );
    assert_eq!(
        classified,
        1.0,
        "{seam}: the increment must carry the category the classifier derives for the \
         delivered error ({expected}), not a hand-rolled string; entry: {:?}",
        m.find(catalog::ERRORS_TOTAL)
    );
}

/// The `test_comp.lz4_table` schema the corrupt fixture was written from — `scan_delta`
/// takes a schema by value rather than reading one from disk.
fn lz4_table_schema() -> cqlite_core::schema::TableSchema {
    cqlite_core::schema::TableSchema {
        keyspace: "test_comp".to_string(),
        table: "lz4_table".to_string(),
        partition_keys: vec![cqlite_core::schema::KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        dropped_columns: std::collections::HashMap::new(),
        columns: vec![cqlite_core::schema::Column {
            name: "v".to_string(),
            data_type: "text".to_string(),
            nullable: true,
            is_static: false,
            default: None,
        }],
        comments: std::collections::HashMap::new(),
    }
}

// ---------------------------------------------------------------------------
// Seam 1 — the streaming surfaces (`JoinedStream::recv`)
// ---------------------------------------------------------------------------

/// AC1: a streaming scan whose producer fails mid-scan records exactly one reader
/// error, and keeps delivering the SAME typed error to the consumer.
///
/// Also AC5 (exactly-once under repeated polling): the consumer keeps calling
/// `recv()` after the terminal error, which must not add further increments.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn streaming_scan_over_corrupt_chunk_records_one_reader_error() {
    let Some(path) = corrupt_data_db_or_gate() else {
        return;
    };
    let mc = testing::metrics_capture();
    let reader = Arc::new(open_reader(&path).await);
    let table_id = TableId::new(TABLE.to_string());

    // Reset AFTER open: `SSTableReader::open` has its own (successful) instrumentation
    // and we want only the scan's emissions in the window.
    mc.reset();

    let mut stream = reader.scan_stream(table_id, None, None, None, 8);

    let mut delivered: Option<Error> = None;
    while let Some(item) = stream.recv().await {
        match item {
            Ok(_) => continue,
            Err(e) => {
                delivered = Some(e);
                break;
            }
        }
    }
    let err = delivered.expect(
        "FIXTURE ROT or read-path regression: the streaming scan over the bit-flipped \
         chunk completed without an error",
    );
    assert_typed_chunk_corruption(&err);

    // AC5 — poll the exhausted/failed stream repeatedly. A sticky failure must be
    // re-REPORTED to the consumer but counted ONCE.
    for _ in 0..3 {
        let _ = stream.recv().await;
    }

    let m = mc.flush_and_collect();
    assert_one_reader_error_classified_as(&m, &err, "scan_stream");
}

// ---------------------------------------------------------------------------
// Seam 2 — `scan_delta` (raw receiver, not a `JoinedStream`)
// ---------------------------------------------------------------------------

/// AC2: `scan_delta`'s terminal `Err` send is its own exit seam and records one
/// reader error. This is also the LITERAL `{category=corruption}` coverage: the
/// delta driver wraps its failures in `Error::corruption(..)`.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn scan_delta_over_corrupt_chunk_records_one_reader_error() {
    let Some(dir) = corrupt_table_dir_or_gate() else {
        return;
    };
    let mc = testing::metrics_capture();
    mc.reset();

    let (mut rx, _summary) = cqlite_core::storage::sstable::reader::delta_scan::scan_delta(
        dir.clone(),
        lz4_table_schema(),
        8,
    );

    let mut delivered: Option<Error> = None;
    while let Some(item) = rx.recv().await {
        if let Err(e) = item {
            delivered = Some(e);
            break;
        }
    }
    let err = delivered.expect(
        "FIXTURE ROT or read-path regression: scan_delta over the bit-flipped chunk \
         completed without an error",
    );
    // Drain to the channel close so the producer task is definitively finished
    // before the metric window is read.
    while rx.recv().await.is_some() {}

    let m = mc.flush_and_collect();
    assert_one_reader_error_classified_as(&m, &err, "scan_delta");
}

/// LITERAL `{category=corruption, subsystem=reader}` coverage — the issue's AC1
/// wording — on a path that genuinely constructs `Error::Corruption`.
///
/// The bit-flip fixture cannot serve this: its CRC mismatch is `Error::InvalidFormat`
/// (`parsing`), by deliberate design documented on #1397/#1411. `scan_delta`'s
/// `find_data_db` DOES raise `Error::corruption(..)` for a generation directory with
/// no `Data.db`, and it raises it INSIDE the spawned driver — so the failure travels
/// the same terminal-send seam a mid-scan failure does, with no `SSTableReader::open`
/// (whose own instrumentation would add a second, unrelated increment) in the way.
/// Hermetic: an empty temp dir, no corpus needed.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn scan_delta_corruption_error_is_counted_under_the_corruption_category() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mc = testing::metrics_capture();
    mc.reset();

    let (mut rx, _summary) = cqlite_core::storage::sstable::reader::delta_scan::scan_delta(
        dir.path().to_path_buf(),
        lz4_table_schema(),
        8,
    );

    let mut delivered: Option<Error> = None;
    while let Some(item) = rx.recv().await {
        if let Err(e) = item {
            delivered = Some(e);
            break;
        }
    }
    let err = delivered.expect("a generation directory with no Data.db must fail the scan");
    assert!(
        matches!(err, Error::Corruption(_)),
        "find_data_db raises Error::corruption for a directory with no Data.db, got: {err:?}"
    );
    while rx.recv().await.is_some() {}

    let m = mc.flush_and_collect();
    assert_one_reader_error_classified_as(&m, &err, "scan_delta (no Data.db)");
    assert_eq!(
        m.sum_where(
            catalog::ERRORS_TOTAL,
            &[
                (catalog::attr::SUBSYSTEM, "reader"),
                (catalog::attr::ERROR_CATEGORY, "corruption"),
            ],
        ),
        1.0,
        "scan_delta: {{category=corruption, subsystem=reader}} must be exactly 1; entry: {:?}",
        m.find(catalog::ERRORS_TOTAL)
    );
}

/// An OPEN failure inside `scan_delta` must count ONCE, not twice (roborev, #1704).
///
/// `SSTableReader::open` self-instruments its own `Err` arm, so before this test the
/// delta seam counted the SAME failed open a second time — and, because the open
/// error was rewrapped as `Error::corruption(..)` on the way out, under a DIFFERENT
/// category than `open` had recorded. One failed operation, two increments, two
/// categories: exactly the double count the three-seam design exists to prevent.
///
/// The corpus corruption cases cannot catch this — they fail MID-SCAN, long after a
/// successful open — so the fixture here is a directory whose `-Data.db` is not an
/// SSTable at all. Hermetic; no corpus needed.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn scan_delta_open_failure_is_counted_once_not_twice() {
    let dir = tempfile::tempdir().expect("temp dir");
    // A DANGLING SYMLINK named `*-Data.db`: `find_data_db` lists it by name (it does
    // not stat the entry), and the subsequent `File::open` fails with ENOENT — for
    // every uid, root included, so the arrangement is deterministic rather than
    // permission-dependent. Deliberately NOT "a file of garbage bytes": that OPENS
    // fine (the reader tolerates an absent TOC) and fails later mid-parse, which is
    // the mid-scan case the corpus tests already cover, not the open case.
    std::os::unix::fs::symlink(
        dir.path().join("does-not-exist"),
        dir.path().join("nb-1-big-Data.db"),
    )
    .expect("create the dangling Data.db symlink");

    let mc = testing::metrics_capture();
    mc.reset();

    let (mut rx, _summary) = cqlite_core::storage::sstable::reader::delta_scan::scan_delta(
        dir.path().to_path_buf(),
        lz4_table_schema(),
        8,
    );

    let mut delivered: Option<Error> = None;
    while let Some(item) = rx.recv().await {
        if let Err(e) = item {
            delivered = Some(e);
            break;
        }
    }
    let err = delivered.expect("a generation whose Data.db cannot be opened must fail the scan");
    assert!(
        !matches!(err, Error::Corruption(_)),
        "the open error must propagate VERBATIM, not rewrapped as corruption — an \
         unreadable file is an io failure, and the rewrap made the delivered error \
         disagree with the category `SSTableReader::open` recorded. Got: {err:?}"
    );
    while rx.recv().await.is_some() {}

    let m = mc.flush_and_collect();
    // ONE increment total — the assertion that fails if the open is counted at both
    // `SSTableReader::open` and the delta seam.
    assert_one_reader_error_classified_as(&m, &err, "scan_delta (open failure)");
}

// ---------------------------------------------------------------------------
// Seam 3 — the materializing `SSTableReader::scan`
// ---------------------------------------------------------------------------

/// AC3: the non-streaming full scan (index / sequential) records one reader error
/// and returns the identical typed error.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn full_scan_over_corrupt_chunk_records_one_reader_error() {
    let Some(path) = corrupt_data_db_or_gate() else {
        return;
    };
    let mc = testing::metrics_capture();
    let reader = open_reader(&path).await;
    let table_id = TableId::new(TABLE.to_string());
    mc.reset();

    let err = match reader.scan(&table_id, None, None, None, None).await {
        Ok(rows) => panic!(
            "FIXTURE ROT or read-path regression: the full scan over the bit-flipped \
             chunk returned Ok with {} rows",
            rows.len()
        ),
        Err(e) => e,
    };
    assert_typed_chunk_corruption(&err);

    let m = mc.flush_and_collect();
    assert_one_reader_error_classified_as(&m, &err, "SSTableReader::scan");
}

/// A SUCCESSFUL scan must record NOTHING: the seam is an error path, and a
/// counter that also fires on success is worse than no counter.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn successful_scan_records_no_reader_error() {
    let Some(clean) = clean_data_db_or_gate() else {
        return;
    };

    let mc = testing::metrics_capture();
    let reader = open_reader(&clean).await;
    let table_id = TableId::new(TABLE.to_string());
    mc.reset();

    let rows = reader
        .scan(&table_id, None, None, None, None)
        .await
        .expect("the clean fixture must scan successfully");
    assert!(!rows.is_empty(), "the clean fixture must yield rows");

    let m = mc.flush_and_collect();
    assert_eq!(
        m.sum_where(
            catalog::ERRORS_TOTAL,
            &[(catalog::attr::SUBSYSTEM, "reader")],
        ),
        0.0,
        "a successful scan must record no reader error; entry: {:?}",
        m.find(catalog::ERRORS_TOTAL)
    );
}
