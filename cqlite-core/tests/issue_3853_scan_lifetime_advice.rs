//! Issue #3853: the `MADV_WILLNEED` an explicit `PrefetchMode::WillNeed` asks
//! for must be issued when a SCAN BEGINS, not when the reader is OPENED, and it
//! must be withdrawn (`MADV_DONTNEED`) when the last in-flight scan on that
//! reader ends.
//!
//! ## What was wrong
//!
//! `mmap_advice_for(WillNeed) -> Some(Advice::WillNeed)` was applied inside
//! `build_block_sources`, i.e. at reader OPEN. So a reader that was opened and
//! never scanned — the `SSTableManager` warm-handle shape, and every
//! point-read-only workload — paid a full-file read-ahead it never used, and the
//! advice was never withdrawn for the reader's whole lifetime. The headline
//! assertion here is therefore [`open_issues_no_advice`]: `(0, 0)` at open, with
//! a positive control that the seam was armed at all.
//!
//! ## The subject is which madvise calls fire, NOT what the bytes decode to
//!
//! Every fixture below is CQLite-WRITTEN. For a format/framing property that
//! would be worthless (CLAUDE.md: a CQLite-written + CQLite-read round trip is
//! invariant to a uniform framing error, and the oracle must be
//! Cassandra-written bytes). It is sound HERE because the property under test is
//! the reader's own scan-lifetime bookkeeping — which syscalls the reader issues,
//! and when — for which CQLite IS the subject and no Cassandra oracle exists or
//! could exist. The committed corpus cannot serve this test at all: the seam is
//! armed only above the 8 MiB `POINT_MMAP_MADV_RANDOM_MIN_BYTES` point-mapping
//! threshold (below it the point plane IS the scan mapping — see
//! `point_plane_sharing_the_scan_mapping_disables_the_seam`), and the largest
//! `Data.db` in `test-data/datasets` is ~647 KiB.
//!
//! ## No page-cache claim is made anywhere
//!
//! `MADV_DONTNEED` on a shared file-backed mapping is an RSS control: the clean
//! file pages stay resident in the kernel's cache and a later touch repopulates
//! from them. Nothing here asserts, measures or implies eviction of anything.

#![cfg(feature = "write-support")]

use std::collections::HashMap;
use std::sync::Arc;

use cqlite_core::config::{DiskAccessMode, PrefetchMode};
use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::scan_cancel::ScanCancel;
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::storage::sstable::writer::SSTableWriter;
use cqlite_core::storage::write_engine::mutation::{
    CellOperation, Mutation, PartitionKey, TableId as MutationTableId,
};
use cqlite_core::types::{TableId, Value};
use cqlite_core::{Config, Platform};
use tempfile::TempDir;

/// `POINT_MMAP_MADV_RANDOM_MIN_BYTES` (issue #2210). Above this the point plane
/// gets its OWN `MADV_RANDOM` mapping, which is the third condition the seam is
/// gated on. Mirrored here because the constant is crate-private; the assertion
/// that the gate actually opened is `scan_lifetime_enabled()`, never this number.
const POINT_MMAP_THRESHOLD: u64 = 8 * 1024 * 1024;

/// Cell payload per partition, and partition count, chosen so the emitted
/// `Data.db` clears `POINT_MMAP_THRESHOLD` with margin.
const CELL_BYTES: usize = 512 * 1024;
const PARTITIONS: usize = 24;

/// Entry points the reader exposes to a caller OUTSIDE `cqlite-core` and that
/// this test therefore drives directly. The `pub(crate)` /
/// `pub(in ...::reader)` siblings are covered by DELEGATION (an inner guard
/// raises the counter; only the outermost drop releases, so the exact `(1, 1)`
/// asserted below is also a proof that the delegating chain advises ONCE) and
/// are declared in [`declared_gaps`].
fn declared_gaps() -> &'static [&'static str] {
    &[
        "iterate_all_partitions_cancellable (pub(crate)) — reached only via \
         iterate_all_partitions; covered by nesting",
        "iterate_all_partitions_via_full_index (pub(in reader)) — reached via \
         iterate_all_partitions_cancellable; covered by nesting",
        "stream_partitions_summary_guided / _compaction (pub(in reader) / private) — \
         reached via stream_all_partitions_for_query; covered by nesting",
        "stream_all_partitions_via_full_index / stream_all_partitions_cancellable \
         (pub(in reader)) — not reachable from outside the crate; wired and \
         compile-checked only",
        "bti_scan_with_metadata / _cancellable / stream_bti_scan (pub(super)) — BTI \
         (`da`) only, and no BTI writer path produces an >= 8 MiB fixture in this \
         test; wired and compile-checked only, NOT behaviourally covered",
        "sequential_scan (pub(in reader)) — reached via scan()/iterate_all_partitions \
         on a reader with no usable index; covered by nesting",
    ]
}

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

/// Write an uncompressed BIG SSTable of `partitions` partitions, each carrying a
/// `cell_bytes`-long text cell, and return its `Data.db` path.
fn write_fixture(temp: &TempDir, partitions: usize, cell_bytes: usize) -> std::path::PathBuf {
    let schema = schema();
    let mut writer = SSTableWriter::new(temp.path().to_path_buf(), 1, &schema).expect("writer");
    let payload = "x".repeat(cell_bytes);
    // Partition order must be token order for a well-formed SSTable; collect the
    // decorated keys first and write in that order.
    let mut rows: Vec<_> = (0..partitions as i32)
        .map(|i| {
            let m = Mutation::new(
                MutationTableId::new("test_ks", "scan_lifetime"),
                PartitionKey::single("id", Value::Integer(i)),
                None,
                vec![CellOperation::Write {
                    column: "payload".to_string(),
                    value: Value::text(payload.clone()),
                }],
                1_000_000,
                None,
            );
            let key = m.decorated_key(&schema).expect("decorated key");
            (key, m)
        })
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    for (key, m) in rows {
        writer
            .write_partition(key, vec![m])
            .expect("write partition");
    }
    let info = futures::executor::block_on(writer.finish()).expect("finish");
    info.data_path
}

fn config(mode: DiskAccessMode, prefetch: PrefetchMode) -> Config {
    let mut config = Config::default();
    config.storage.disk_access_mode = mode;
    config.storage.prefetch = prefetch;
    config
}

async fn open_reader(path: &std::path::Path, config: &Config) -> SSTableReader {
    let platform = Arc::new(Platform::new(config).await.expect("platform"));
    SSTableReader::open(path, config, Arc::clone(&platform))
        .await
        .expect("open reader")
}

/// Open a reader over an `>= 8 MiB` mmap fixture at an explicit `WillNeed`, i.e.
/// the ONE configuration in which the seam is armed, and assert it IS armed.
/// Every behavioural case below starts here, so a silently-disabled seam can
/// never be mistaken for "the counters legitimately stayed at zero".
async fn armed_reader(temp: &TempDir) -> SSTableReader {
    let path = write_fixture(temp, PARTITIONS, CELL_BYTES);
    let size = std::fs::metadata(&path).expect("stat").len();
    assert!(
        size >= POINT_MMAP_THRESHOLD,
        "fixture must clear the #2210 point-mapping threshold to arm the seam \
         ({size} < {POINT_MMAP_THRESHOLD})"
    );
    let reader = open_reader(&path, &config(DiskAccessMode::Mmap, PrefetchMode::WillNeed)).await;
    assert!(
        reader.scan_lifetime_enabled(),
        "POSITIVE CONTROL FAILED: the scan-lifetime seam is not armed, so every \
         count assertion below would be vacuous. It is armed only for an \
         mmap-backed reader (this proves the mmap backend was taken) at an \
         explicit PrefetchMode::WillNeed whose point plane holds its own mapping."
    );
    reader
}

/// THE headline acceptance: opening a reader issues NO scan-lifetime advice.
///
/// SCOPE, stated because the obvious reading of this test is wrong: the counters
/// track THIS SEAM's transitions, so they cannot see a `madvise` issued by some
/// OTHER code path — including a reintroduced open-time
/// `mmap.advise(Advice::WillNeed)` inside `build_block_sources`, which is the
/// literal shape of the defect. The deterministic guard for the open-time site is
/// the unit test `reader::tests::test_mmap_advice_for_auto_is_no_madvise`, which
/// asserts `mmap_open_advice_for(WillNeed) == None` (and `Auto`/`Off` likewise,
/// issue #1143). What THIS case adds is the other half: the seam is armed (so a
/// `(0, 0)` here is not the vacuous reading a buffered or `Auto` reader gives) and
/// it has NOT fired — i.e. the advice was not simply relocated to open under the
/// new mechanism, and an opened-but-never-scanned reader is idle.
#[tokio::test]
async fn open_issues_no_advice() {
    let temp = TempDir::new().expect("temp dir");
    let reader = armed_reader(&temp).await;
    assert_eq!(
        reader.scan_lifetime_advice_counts(),
        (0, 0),
        "issue #3853: reader OPEN must issue no madvise — WillNeed is a \
         scan-lifetime advice now"
    );
    assert_eq!(reader.scan_lifetime_in_flight(), 0);
}

/// Assert one scan on `reader` advised exactly once, released exactly once, and
/// really read the fixture (a 0-row pass over a present fixture is a failure).
fn assert_advised_once(label: &str, reader: &SSTableReader, rows: usize) {
    assert!(
        rows > 0,
        "{label}: scanned a present fixture and got 0 rows"
    );
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

/// One property per entry point reachable from outside the crate: the scan
/// advises EXACTLY once and releases EXACTLY once, and the reader is left idle.
///
/// Each case gets its OWN reader — the counters are cumulative per reader, so
/// sharing one would turn "advises once" into arithmetic about call order.
#[tokio::test]
async fn every_reachable_entry_point_advises_once_and_releases() {
    for gap in declared_gaps() {
        println!("DECLARED GAP (not behaviourally covered here): {gap}");
    }

    let temp = TempDir::new().expect("temp dir");
    let table_id = TableId::new("test_ks.scan_lifetime");

    // 1. `scan_inner`, via the public `scan`.
    {
        let reader = armed_reader(&temp).await;
        let rows = reader
            .scan(&table_id, None, None, None, None)
            .await
            .expect("scan")
            .len();
        assert_advised_once("scan", &reader, rows);
    }

    // 2. `iterate_all_partitions`, which delegates through `_cancellable` and
    //    `_via_full_index` — so `(1, 1)` also proves NESTING advises once.
    {
        let reader = armed_reader(&temp).await;
        let rows = reader
            .iterate_all_partitions()
            .await
            .expect("iterate")
            .len();
        assert_advised_once("iterate_all_partitions", &reader, rows);
    }

    // 3. `iterate_all_partitions_for_compaction`.
    {
        let reader = armed_reader(&temp).await;
        let rows = reader
            .iterate_all_partitions_for_compaction(Some(&schema()))
            .await
            .expect("compaction iterate")
            .len();
        assert_advised_once("iterate_all_partitions_for_compaction", &reader, rows);
    }

    // 4. `stream_all_partitions_for_query` — the callback-walk shape, which
    //    delegates to the two Summary-guided sites.
    {
        let reader = armed_reader(&temp).await;
        let mut rows = 0usize;
        let cancel = ScanCancel::new();
        reader
            .stream_all_partitions_for_query(Some(&schema()), &cancel, None, |_row| {
                rows += 1;
                Ok(std::ops::ControlFlow::Continue(()))
            })
            .await
            .expect("summary-guided stream");
        assert_advised_once("stream_all_partitions_for_query", &reader, rows);
    }

    // 5. `get_all_entries` — a public full walk NOT in the issue's list.
    {
        let reader = armed_reader(&temp).await;
        let rows = reader
            .get_all_entries()
            .await
            .expect("get_all_entries")
            .len();
        assert_advised_once("get_all_entries", &reader, rows);
    }

    // 6. `scan_with_cell_metadata` — ditto.
    {
        let reader = armed_reader(&temp).await;
        let rows = reader
            .scan_with_cell_metadata(&table_id, None, None, None, Some(&schema()))
            .await
            .expect("metadata scan")
            .len();
        assert_advised_once("scan_with_cell_metadata", &reader, rows);
    }

    // 7. `distinct_partition_keys` — ditto (whole-Data.db stitch).
    {
        let reader = armed_reader(&temp).await;
        let rows = reader
            .distinct_partition_keys()
            .await
            .expect("distinct keys")
            .len();
        assert_advised_once("distinct_partition_keys", &reader, rows);
    }

    // 8. `stream_all_partitions_for_compaction` — ditto (callback walk).
    {
        let reader = armed_reader(&temp).await;
        let mut rows = 0usize;
        let cancel = ScanCancel::new();
        reader
            .stream_all_partitions_for_compaction(Some(&schema()), &cancel, |_row| {
                rows += 1;
                Ok(std::ops::ControlFlow::Continue(()))
            })
            .await
            .expect("compaction stream");
        assert_advised_once("stream_all_partitions_for_compaction", &reader, rows);
    }
}

/// Shape C: the guard lives INSIDE the spawned scan task, so it must be released
/// when the task ends — including when the consumer drains the channel to
/// completion on another task.
#[tokio::test]
async fn spawned_stream_scans_advise_once_and_release() {
    let temp = TempDir::new().expect("temp dir");
    let table_id = TableId::new("test_ks.scan_lifetime");

    let reader = Arc::new(armed_reader(&temp).await);
    let mut rx = Arc::clone(&reader).scan_stream(table_id.clone(), None, None, Some(schema()), 16);
    let mut rows = 0usize;
    while let Some(item) = rx.recv().await {
        item.expect("streamed row");
        rows += 1;
    }
    drop(rx);
    assert!(rows > 0, "scan_stream returned 0 rows on a present fixture");
    // The producer task drops its guard as it finishes; the channel closing is
    // the observable that it did (`recv` returned `None` because every sender
    // was dropped, and the guard is dropped with the task's locals).
    assert_eq!(reader.scan_lifetime_advice_counts(), (1, 1), "scan_stream");
    assert_eq!(reader.scan_lifetime_in_flight(), 0, "scan_stream");

    let reader = Arc::new(armed_reader(&temp).await);
    let mut rx = Arc::clone(&reader).scan_stream_batched(table_id, None, None, Some(schema()), 16);
    let mut rows = 0usize;
    while let Some(item) = rx.recv().await {
        rows += item.expect("streamed batch").len();
    }
    drop(rx);
    assert!(rows > 0, "scan_stream_batched returned 0 rows");
    assert_eq!(
        reader.scan_lifetime_advice_counts(),
        (1, 1),
        "scan_stream_batched"
    );
    assert_eq!(reader.scan_lifetime_in_flight(), 0, "scan_stream_batched");
}

/// The guard is RAII, so an error return and a cancellation release it exactly
/// as a success does — the release is not a success-path decrement.
#[tokio::test]
async fn error_and_cancelled_scans_still_release() {
    let temp = TempDir::new().expect("temp dir");

    // Error: the walk's own `emit` callback fails mid-scan.
    let reader = armed_reader(&temp).await;
    let cancel = ScanCancel::new();
    let result = reader
        .stream_all_partitions_for_query(Some(&schema()), &cancel, None, |_row| {
            Err(cqlite_core::Error::corruption("induced emit failure"))
        })
        .await;
    assert!(result.is_err(), "the induced emit failure must propagate");
    assert_eq!(
        reader.scan_lifetime_advice_counts(),
        (1, 1),
        "an Err scan must still release"
    );
    assert_eq!(reader.scan_lifetime_in_flight(), 0);

    // Cancellation: the reader-wide token is tripped before the walk starts.
    let path = write_fixture(&temp, PARTITIONS, CELL_BYTES);
    let mut reader =
        open_reader(&path, &config(DiskAccessMode::Mmap, PrefetchMode::WillNeed)).await;
    let cancel = ScanCancel::new();
    reader.set_scan_cancel(cancel.clone());
    assert!(reader.scan_lifetime_enabled(), "POSITIVE CONTROL FAILED");
    cancel.cancel();
    let result = reader.iterate_all_partitions().await;
    assert!(result.is_err(), "a cancelled enumeration must return Err");
    assert_eq!(
        reader.scan_lifetime_in_flight(),
        0,
        "a cancelled scan must still release"
    );
    assert_eq!(
        reader.scan_lifetime_advice_counts(),
        (1, 1),
        "a cancelled scan advises on entry and releases on drop"
    );
}

/// Issue #3853 AC bullet 3 / constraint 3: below the #2210 threshold the point
/// plane IS the scan mapping, so releasing the scan mapping would degrade the
/// point plane. The seam must be DISABLED, and a full scan must record `(0, 0)`.
#[tokio::test]
async fn point_plane_sharing_the_scan_mapping_disables_the_seam() {
    let temp = TempDir::new().expect("temp dir");
    let path = write_fixture(&temp, 2, 1024);
    let size = std::fs::metadata(&path).expect("stat").len();
    assert!(
        size < POINT_MMAP_THRESHOLD,
        "this case needs a sub-threshold fixture ({size} >= {POINT_MMAP_THRESHOLD})"
    );

    let reader = open_reader(&path, &config(DiskAccessMode::Mmap, PrefetchMode::WillNeed)).await;
    assert!(
        !reader.scan_lifetime_enabled(),
        "a sub-8-MiB reader shares ONE mapping between the point and scan planes, \
         so the seam must be disabled"
    );
    let rows = reader
        .iterate_all_partitions()
        .await
        .expect("iterate")
        .len();
    assert!(rows > 0, "scanned a present fixture and got 0 rows");
    assert_eq!(
        reader.scan_lifetime_advice_counts(),
        (0, 0),
        "the shared-mapping reader must never release the scan mapping"
    );
}

/// Issue #1143, asserted at the reader surface and not only at
/// `mmap_open_advice_for`: the DEFAULT `Auto` prefetch issues no madvise at any
/// point in a reader's life — not at open, and not at scan start either.
#[tokio::test]
async fn auto_prefetch_issues_no_advice_ever() {
    let temp = TempDir::new().expect("temp dir");
    let path = write_fixture(&temp, PARTITIONS, CELL_BYTES);
    let table_id = TableId::new("test_ks.scan_lifetime");

    for prefetch in [
        PrefetchMode::Auto,
        PrefetchMode::Off,
        PrefetchMode::Sequential,
    ] {
        let reader = open_reader(&path, &config(DiskAccessMode::Mmap, prefetch)).await;
        assert!(
            !reader.scan_lifetime_enabled(),
            "prefetch {prefetch:?} must leave the scan-lifetime seam disabled \
             (issue #1143 for Auto)"
        );
        let rows = reader
            .scan(&table_id, None, None, None, None)
            .await
            .expect("scan")
            .len();
        assert!(
            rows > 0,
            "prefetch {prefetch:?}: 0 rows on a present fixture"
        );
        assert_eq!(
            reader.scan_lifetime_advice_counts(),
            (0, 0),
            "prefetch {prefetch:?} must issue NO scan-lifetime madvise"
        );
    }
}

/// A non-mmap backend has no per-mapping advice concept at all.
#[tokio::test]
async fn buffered_backend_never_advises() {
    let temp = TempDir::new().expect("temp dir");
    let path = write_fixture(&temp, PARTITIONS, CELL_BYTES);
    let reader = open_reader(
        &path,
        &config(DiskAccessMode::Buffered, PrefetchMode::WillNeed),
    )
    .await;
    assert!(!reader.scan_lifetime_enabled());
    assert!(!reader
        .iterate_all_partitions()
        .await
        .expect("iterate")
        .is_empty());
    assert_eq!(reader.scan_lifetime_advice_counts(), (0, 0));
}
