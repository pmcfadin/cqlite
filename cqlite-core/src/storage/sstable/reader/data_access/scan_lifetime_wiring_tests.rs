//! Issue #3853 fix round 1 — the whole-data-section walks that were NOT
//! covered by the first round's entry-point wiring.
//!
//! The first round wired 23 scan entry points and asserted the property from
//! `tests/issue_3853_scan_lifetime_advice.rs`, which can only reach what is
//! `pub` on `SSTableReader`. A sweep of every reachable function that mints a
//! scan cursor or drives `stitch_all_chunks*` found three sites the integration
//! test structurally cannot see:
//!
//! 1. [`SSTableReader::prepare_delta_scan`] — `pub`, but behind
//!    `#[cfg(feature = "delta-scan")]`, so it is not compiled in the feature set
//!    the local gate's `core-tests` component runs.
//! 2. [`SSTableReader::scan_for_key`] — `pub(super)` (visible only inside
//!    `data_access`), the sequential whole-file fallback of a point lookup.
//! 3. [`SSTableReader::stitch_all_chunks_cancellable`] — `pub(super)` on
//!    `reader`, the single funnel every stitched whole-section read passes
//!    through.
//!
//! # Where these tests EXECUTE (#3522)
//!
//! In the LIB, deliberately. A `#![cfg(feature = "delta-scan")]` INTEGRATION
//! target under `cqlite-core/tests/` executes in NO gate component — the gate's
//! `feature-iso-delta-scan` lane is `--lib --no-run` and `core-tests` runs
//! `--features cli-helpers` — so the feature-gated case would have been coverage
//! that never runs, the exact defect class CLAUDE.md records under #3522. As a
//! lib test the delta-scan case executes under `pr-gate-core`'s
//! `cargo test -p cqlite-core --lib --all-features`, a REQUIRED check;
//! **it does NOT execute in the local gate of record**, whose `core-tests`
//! feature set does not include `delta-scan`. The two non-feature-gated cases
//! execute in both.
//!
//! # Fixture
//!
//! CQLite-WRITTEN, and sound here for the reason the integration test states:
//! the property under test is which `madvise` calls the reader issues and when,
//! for which CQLite IS the subject and no Cassandra oracle exists. It must clear
//! the 8 MiB `POINT_MMAP_MADV_RANDOM_MIN_BYTES` threshold, because below it the
//! point plane IS the scan mapping and the seam is deliberately disabled — so
//! the committed corpus (largest `Data.db` ~647 KiB) cannot serve this test.
//!
//! No page-cache claim is made or implied anywhere: `MADV_DONTNEED` on a shared
//! file-backed mapping is an RSS control.

use std::collections::HashMap;
use std::sync::Arc;

use tempfile::TempDir;

use crate::config::{DiskAccessMode, PrefetchMode};
use crate::schema::{Column, KeyColumn, TableSchema};
use crate::storage::sstable::reader::SSTableReader;
use crate::storage::write_engine::mutation::{
    CellOperation, Mutation, PartitionKey, TableId as MutationTableId,
};
use crate::types::{RowKey, TableId, Value};
use crate::{Config, Platform};

/// `POINT_MMAP_MADV_RANDOM_MIN_BYTES` (issue #2210). Mirrored, not imported: the
/// assertion that the gate actually opened is `scan_lifetime_enabled()`, never
/// this number, which exists only to make the fixture-size failure legible.
const POINT_MMAP_THRESHOLD: u64 = 8 * 1024 * 1024;

const CELL_BYTES: usize = 512 * 1024;
const PARTITIONS: i32 = 24;

fn schema() -> TableSchema {
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "scan_lifetime".to_string(),
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
                name: "payload".to_string(),
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

/// Write an uncompressed BIG (`nb`-tagged) SSTable large enough to arm the seam.
///
/// `nb`-tagged + `V5CompressedLegacy` means `requires_chunk_stitching()` is
/// TRUE, which is what routes the scan and point-lookup paths through the
/// `stitch_all_chunks*` funnel these tests are about.
async fn write_large_fixture() -> (TempDir, std::path::PathBuf) {
    let schema = schema();
    let temp = TempDir::new().expect("temp dir");
    let mut writer =
        crate::storage::sstable::writer::SSTableWriter::new(temp.path().to_path_buf(), 1, &schema)
            .expect("writer");
    let payload = "x".repeat(CELL_BYTES);
    let mut keyed: Vec<_> = (0..PARTITIONS)
        .map(|id| {
            let m = Mutation::new(
                MutationTableId::new("test_ks", "scan_lifetime"),
                PartitionKey::single("id", Value::Integer(id)),
                None,
                vec![CellOperation::Write {
                    column: "payload".to_string(),
                    value: Value::text(payload.clone()),
                }],
                1_000_000 + id as i64,
                None,
            );
            let key = m.decorated_key(&schema).expect("decorated key");
            (key, m)
        })
        .collect();
    keyed.sort_by(|a, b| a.0.cmp(&b.0));
    for (key, m) in keyed {
        writer.write_partition(key, vec![m]).expect("write");
    }
    let info = writer.finish().await.expect("finish");
    let data_path = info.data_path.clone();
    (temp, data_path)
}

/// Open the fixture as an mmap reader at an explicit `WillNeed` — the ONE
/// configuration in which the seam arms — and assert it IS armed.
///
/// The `scan_lifetime_enabled()` assertion is the POSITIVE CONTROL the
/// integration suite established: `(0, 0)` is also what a buffered reader, an
/// `Auto` reader and a non-unix target report, so a count assertion without it
/// is vacuous.
async fn armed_reader() -> (TempDir, SSTableReader) {
    let (temp, data_path) = write_large_fixture().await;
    let size = std::fs::metadata(&data_path).expect("stat").len();
    assert!(
        size >= POINT_MMAP_THRESHOLD,
        "fixture must clear the #2210 point-mapping threshold to arm the seam \
         ({size} < {POINT_MMAP_THRESHOLD})"
    );
    let mut config = Config::default();
    config.storage.disk_access_mode = DiskAccessMode::Mmap;
    config.storage.prefetch = PrefetchMode::WillNeed;
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    let reader = SSTableReader::open(&data_path, &config, platform)
        .await
        .expect("open reader");
    assert!(
        reader.scan_lifetime_enabled(),
        "POSITIVE CONTROL FAILED: the scan-lifetime seam is not armed, so every \
         count assertion below would be vacuous"
    );
    assert!(
        reader.requires_chunk_stitching(),
        "this fixture must take the stitching path — it is what routes these \
         sites through the stitch funnel under test"
    );
    assert_eq!(reader.scan_lifetime_advice_counts(), (0, 0));
    (temp, reader)
}

fn assert_advised_once(label: &str, reader: &SSTableReader) {
    assert_eq!(
        reader.scan_lifetime_advice_counts(),
        (1, 1),
        "{label}: expected exactly one WILLNEED and one DONTNEED"
    );
    assert_eq!(
        reader.scan_lifetime_in_flight(),
        0,
        "{label}: the guard must have released"
    );
}

/// Fix 1: the `delta-scan` bridge reads the WHOLE data section (seek past the
/// header, then `stitch_all_chunks`) and must hold a scan-lifetime guard.
///
/// The guard's scope is this function's BODY, which is correct because it
/// returns an OWNED `Vec<u8>` — no borrow into the mapping survives the return,
/// so releasing when the stitch completes is the RSS control working, not a
/// premature release.
#[cfg(feature = "delta-scan")]
#[tokio::test]
async fn prepare_delta_scan_advises_once_and_releases() {
    let (_temp, reader) = armed_reader().await;
    let (stitched, _parser) = reader.prepare_delta_scan().await.expect("prepare");
    assert!(
        !stitched.is_empty(),
        "prepare_delta_scan returned an empty data section — the walk this \
         test is about did not happen"
    );
    assert_advised_once("prepare_delta_scan", &reader);
}

/// Fix 2: `scan_for_key` is the sequential whole-file fallback of a point
/// lookup. It reads the ENTIRE data section for a SINGLE key (both branches: the
/// stitched one below, and the raw block loop for non-stitching formats), so by
/// the seam's own definition it is a scan and must hold a guard.
///
/// A MISSING key is used deliberately: the stitched branch early-returns on the
/// first match, so a present key can stop before the walk completes. A miss is
/// the O(whole file) case, which is exactly the RSS the `DONTNEED` releases.
#[tokio::test]
async fn scan_for_key_advises_once_and_releases() {
    let (_temp, reader) = armed_reader().await;
    let table_id = TableId::new("test_ks.scan_lifetime");
    let absent = RowKey::from(b"no-such-key-anywhere".to_vec());
    let found = reader
        .scan_for_key(&table_id, &absent)
        .await
        .expect("scan_for_key");
    assert!(found.is_none(), "the fixture must not hold this key");
    assert_advised_once("scan_for_key (miss)", &reader);
}

/// Fix 3, direction A — the funnel guard FIRES when the funnel is reached with
/// no entry-point guard above it.
///
/// This is the defence-in-depth property: a caller that stitches the whole data
/// section without holding a guard of its own is still covered. It is the RED
/// arm for fix 3 — before the guard existed this read `(0, 0)`.
#[tokio::test]
async fn stitch_funnel_advises_when_reached_without_an_entry_point_guard() {
    let (_temp, reader) = armed_reader().await;
    let cursor = reader.new_scan_cursor().await.expect("cursor");
    let cancel = crate::storage::scan_cancel::ScanCancel::new();
    let stitched = reader
        .stitch_all_chunks_cancellable(&cursor, &cancel)
        .await
        .expect("stitch");
    assert!(!stitched.is_empty(), "the stitch returned no bytes");
    assert_advised_once("stitch funnel (unguarded caller)", &reader);
}

/// Fix 3, direction B — the funnel guard does NOT double-advise under a guarded
/// caller.
///
/// This is the assertion that proves item 3 is free rather than additive: the
/// nested `begin` raises the count 1 -> 2 and its drop lowers it 2 -> 1, and
/// neither transition issues advice, so the whole scan reports exactly ONE
/// WILLNEED and ONE DONTNEED. It is green both before and after fix 3 by
/// construction (an absent nested guard also cannot double-advise); its
/// discriminating power is against a MIS-implemented funnel guard — one that
/// advises unconditionally instead of on the counter transition — which reads
/// `(2, 2)` here.
#[tokio::test]
async fn stitch_funnel_does_not_double_advise_under_a_guarded_caller() {
    let (_temp, reader) = armed_reader().await;
    let table_id = TableId::new("test_ks.scan_lifetime");
    // `scan` -> `scan_inner` (guarded) -> the stitching fallback -> the funnel.
    let rows = reader
        .scan(&table_id, None, None, None, None)
        .await
        .expect("scan")
        .len();
    assert!(rows > 0, "scanned a present fixture and got 0 rows");
    assert_advised_once("scan through the stitch funnel", &reader);
}
