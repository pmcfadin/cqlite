//! Issue #2264 — the compaction streaming scan must observe a cooperative
//! cancel token and abandon a multi-partition Data.db WITHOUT running to
//! completion.
//!
//! World 2 (field-proven): an index-less (Summary.db-absent) SSTable is handed
//! to `stream_all_partitions_for_compaction`, whose parser loops over EVERY
//! partition in a single uninterruptible pass on a detached producer thread. A
//! cancelled Flight `do_get` could not stop it — the merge's between-step poll
//! never runs because it is parked waiting for the producer, and PR #2282's
//! channel race never reaches the CPU-bound loop.
//!
//! This file's fixtures are SSTableWriter-produced, filename-tagged 'nb', which
//! `requires_chunk_stitching()` routes to `stream_all_partitions_for_compaction`'s
//! STITCHING branch (`drain_compaction_window`, `compaction.rs`) regardless of
//! whether the file is actually compressed — that is the loop these tests
//! exercise and pin. The field's specific `V5_0Uncompressed`-tagged snapshot
//! instead takes the NON-stitching branch (`sequential.rs` /
//! `block_emit_windowed.rs`), a structurally distinct loop with its own poll,
//! covered by no test in THIS file.
//!
//! These tests drive the reader scan DIRECTLY (no `KWayMerger`, so the merge's
//! own between-step poll is absent) — the reader's `scan_cancel` poll is the
//! ONLY thing that can abort the walk here, so a green test proves THIS fix, not
//! the pre-existing between-step check.
//!
//! The final test in this file (`pre_cancelled_scan_does_not_probe_index_on_
//! index_backed_path`, roborev round 3) instead calls `iterate_all_partitions`
//! directly against a REAL Cassandra fixture (`CQLITE_DATASETS_ROOT`) — see its
//! doc comment for why a synthesized `SSTableWriter` fixture cannot exercise
//! that resolution mode.

use crate::schema::{Column, KeyColumn, TableSchema};
use crate::storage::scan_cancel::ScanCancel;
use crate::storage::sstable::reader::SSTableReader;
use crate::storage::write_engine::mutation::{CellOperation, Mutation, PartitionKey, TableId};
use crate::types::Value;
use crate::{Config, Platform};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

fn schema() -> TableSchema {
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
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

fn mutation(id: i32) -> Mutation {
    Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(id)),
        None,
        vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text(format!("v{id}")),
        }],
        1_000_000 + id as i64,
        None,
    )
}

/// Write `n` single-row partitions to a fresh uncompressed SSTable, keeping
/// EVERY emitted component (Summary.db/Index.db/Filter.db included). Returns
/// the temp dir (keep alive) and the `Data.db` path.
async fn write_fixture(n: i32) -> (TempDir, std::path::PathBuf) {
    let schema = schema();
    let temp = TempDir::new().unwrap();
    let mut writer =
        crate::storage::sstable::writer::SSTableWriter::new(temp.path().to_path_buf(), 1, &schema)
            .unwrap();

    // Write in token order (the writer enforces it).
    let mut keyed: Vec<_> = (1..=n)
        .map(|id| {
            let m = mutation(id);
            let key = m.decorated_key(&schema).unwrap();
            (key, m)
        })
        .collect();
    keyed.sort_by_key(|(k, _)| k.token);
    for (key, m) in keyed {
        writer.write_partition(key, vec![m]).unwrap();
    }
    let info = writer.finish().await.unwrap();
    let data_path = info.data_path.clone();
    (temp, data_path)
}

/// Write `n` single-row partitions to a fresh uncompressed SSTable, then strip
/// the `Summary.db`/`Index.db`/`Filter.db` sidecars to reproduce the field's
/// index-less snapshot (only Data.db + Statistics.db remain). Returns the temp
/// dir (keep alive) and the `Data.db` path.
async fn index_less_fixture(n: i32) -> (TempDir, std::path::PathBuf) {
    let (temp, data_path) = write_fixture(n).await;

    // Strip the partition-index sidecars so the reader takes the full-scan
    // fallback the field hit — the fix must be correct for legitimately
    // index-less inputs (Phase C snapshot-completeness is filed separately).
    for entry in std::fs::read_dir(temp.path()).unwrap().flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.ends_with("-Summary.db")
            || name.ends_with("-Index.db")
            || name.ends_with("-Filter.db")
        {
            std::fs::remove_file(entry.path()).unwrap();
        }
    }
    (temp, data_path)
}

async fn open_reader(data_path: &std::path::Path) -> SSTableReader {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());
    SSTableReader::open(data_path, &config, platform)
        .await
        .unwrap()
}

/// Build a GENUINELY LZ4-compressed 'nb' fixture (roborev, issue #2264): the
/// production write surface emits uncompressed SSTables only (issue #1406), so
/// this "repacks" an `index_less_fixture`'s output through the fixture-synthesis
/// building blocks `CompressedDataWriter`/`CompressionInfoWriter` — the SAME
/// technique `issue_953_multichunk_cell_seek.rs` uses on a real fixture, applied
/// here to a synthesized one so the test needs no `CQLITE_DATASETS_ROOT`.
///
/// The existing three tests above never make `self.compression_reader.is_some()`
/// true, so `stream_all_partitions_for_compaction`'s actual
/// `Compression::decompress` call (`compaction.rs`) — and the `drain_
/// compaction_window` poll immediately around it — go untested; a refactor could
/// silently drop that poll with all three tests still green. A small chunk size
/// forces MANY chunks over the raw data section, so the scan makes many
/// decompress calls before finishing.
#[cfg(feature = "lz4")]
async fn compressed_fixture(n: i32) -> (TempDir, std::path::PathBuf) {
    use crate::storage::sstable::writer::{
        create_compressor, CompressedDataWriter, CompressionAlgorithm, CompressionInfoWriter,
    };

    // Deliberately tiny so a modest partition count still yields many chunks.
    const REPACK_CHUNK_SIZE: usize = 256;

    let (temp, data_path) = index_less_fixture(n).await;

    // Read the raw (uncompressed) data section back exactly as the reader would
    // see it — skip past `calculate_header_size()` bytes (0 for headerless 'nb').
    let header_size = open_reader(&data_path).await.calculate_header_size();
    let raw = std::fs::read(&data_path).unwrap();
    let data_section = &raw[header_size..];

    let compressor = create_compressor(CompressionAlgorithm::Lz4).unwrap();
    let mut writer = CompressedDataWriter::with_chunk_size(compressor, REPACK_CHUNK_SIZE);
    writer.write(data_section).unwrap();
    let (compressed, metadata) = writer.finish().unwrap();
    assert!(
        metadata.chunk_count() > 1,
        "fixture must span multiple chunks to exercise repeated decompress calls, got {}",
        metadata.chunk_count()
    );

    // Overwrite Data.db with the compressed chunk stream (headerless 'nb' has no
    // prefix to preserve) and write the matching CompressionInfo.db sidecar so
    // `SSTableReader::open` picks up a real `compression_reader`.
    std::fs::write(&data_path, &compressed).unwrap();
    let base = data_path
        .file_stem()
        .unwrap()
        .to_str()
        .unwrap()
        .trim_end_matches("-Data");
    let compression_info_path = data_path
        .parent()
        .unwrap()
        .join(format!("{base}-CompressionInfo.db"));
    CompressionInfoWriter::new(compression_info_path.clone())
        .write(&metadata)
        .unwrap();
    assert!(
        compression_info_path.exists(),
        "CompressionInfo.db must be written so the reader takes the compressed path"
    );

    // A stale CRC.db (checksums the ORIGINAL uncompressed bytes) is now
    // meaningless — the reader only loads it when `compression_info.is_none()`
    // (so it is not even consulted once CompressionInfo.db exists), but remove it
    // for hygiene so nothing could reference mismatched checksums.
    for entry in std::fs::read_dir(temp.path()).unwrap().flatten() {
        if entry.file_name().to_string_lossy().ends_with("-CRC.db") {
            std::fs::remove_file(entry.path()).unwrap();
        }
    }

    (temp, data_path)
}

/// The stitching-path analogue of `mid_scan_cancel_aborts_before_finishing`,
/// over a GENUINELY LZ4-compressed fixture (roborev, issue #2264): proves the
/// `drain_compaction_window` poll fires even when each partition step requires a
/// real `Compression::decompress` call, not just the "no compression_reader" /
/// raw-chunk shape the other tests exercise. FAILS on pre-fix code the same way:
/// no poll → the scan runs to completion (`count == TOTAL`, `Ok`).
#[cfg(feature = "lz4")]
#[tokio::test(flavor = "multi_thread")]
async fn mid_scan_cancel_aborts_on_compressed_stitching_path() {
    const TOTAL: i32 = 2000;
    const TRIP_AT: usize = 300;
    let (_temp, data_path) = compressed_fixture(TOTAL).await;
    let mut reader = open_reader(&data_path).await;

    let cancel = ScanCancel::new();
    reader.set_scan_cancel(cancel.clone());

    let mut count = 0usize;
    let result = reader
        .stream_all_partitions_for_compaction(Some(&schema()), |_row| {
            count += 1;
            if count == TRIP_AT {
                cancel.cancel();
            }
            Ok(std::ops::ControlFlow::Continue(()))
        })
        .await;

    assert!(
        matches!(result, Err(crate::Error::Cancelled)),
        "a mid-scan cancel on the compressed stitching path must abort with \
         Error::Cancelled, got {result:?}"
    );
    assert!(
        count >= TRIP_AT && count < TOTAL as usize,
        "must abort after the trip point but well before the full {TOTAL} partitions, got {count}"
    );
}

/// Positive control (non-vacuity): with a never-cancelled token the scan streams
/// EVERY partition. Proves the fixture really has `TOTAL` partitions on the
/// full-scan path, so the cancellation tests below are cutting real work short.
#[tokio::test(flavor = "multi_thread")]
async fn uncancelled_scan_streams_all_partitions() {
    const TOTAL: i32 = 1000;
    let (_temp, data_path) = index_less_fixture(TOTAL).await;
    let reader = open_reader(&data_path).await;

    let mut count = 0usize;
    let result = reader
        .stream_all_partitions_for_compaction(Some(&schema()), |_row| {
            count += 1;
            Ok(std::ops::ControlFlow::Continue(()))
        })
        .await;

    assert!(result.is_ok(), "uncancelled scan must succeed: {result:?}");
    assert_eq!(
        count, TOTAL as usize,
        "the full-scan path must stream every partition"
    );
}

/// A token tripped BEFORE the scan starts aborts it at the very first poll —
/// zero partitions emitted, a clean `Cancelled` error. The scan does NOT run to
/// completion (which the positive control proves is 1000 partitions), so this is
/// the fix, not a fast fixture. FAILS on pre-fix code: without the `scan_cancel`
/// poll the scan ignores the token and returns `Ok` with all 1000 partitions.
#[tokio::test(flavor = "multi_thread")]
async fn pre_cancelled_scan_aborts_at_first_poll() {
    const TOTAL: i32 = 1000;
    let (_temp, data_path) = index_less_fixture(TOTAL).await;
    let mut reader = open_reader(&data_path).await;

    let cancel = ScanCancel::new();
    cancel.cancel();
    reader.set_scan_cancel(cancel);

    let mut count = 0usize;
    let result = reader
        .stream_all_partitions_for_compaction(Some(&schema()), |_row| {
            count += 1;
            Ok(std::ops::ControlFlow::Continue(()))
        })
        .await;

    assert!(
        matches!(result, Err(crate::Error::Cancelled)),
        "a pre-cancelled scan must abort with Error::Cancelled, got {result:?}"
    );
    assert_eq!(
        count, 0,
        "a pre-cancelled scan must not materialise a single partition"
    );
}

/// A token tripped MID-scan (from the emit callback, after a bounded number of
/// partitions) aborts within one poll interval instead of finishing — the
/// World-2 analogue of PR #2282's channel test. This fixture is filename-tagged
/// 'nb', so `requires_chunk_stitching()` is true and the scan takes the
/// STITCHING branch: the cancel is caught by `drain_compaction_window`'s
/// per-partition poll (`compaction.rs`, between successive
/// `parse_one_partition_for_compaction` calls), NOT by the non-stitching
/// materialization poll in `block_emit_windowed.rs`/`sequential.rs` — those cover
/// a DIFFERENT reader code path (an index-less SSTable that is ALSO not
/// filename-tagged 'nb', e.g. `V5_0Uncompressed`) and are exercised by no test in
/// this file. FAILS on pre-fix code (no poll → runs to completion, `count ==
/// TOTAL`, `Ok`).
#[tokio::test(flavor = "multi_thread")]
async fn mid_scan_cancel_aborts_before_finishing() {
    const TOTAL: i32 = 2000;
    const TRIP_AT: usize = 300;
    let (_temp, data_path) = index_less_fixture(TOTAL).await;
    let mut reader = open_reader(&data_path).await;

    let cancel = ScanCancel::new();
    reader.set_scan_cancel(cancel.clone());

    let mut count = 0usize;
    let result = reader
        .stream_all_partitions_for_compaction(Some(&schema()), |_row| {
            count += 1;
            if count == TRIP_AT {
                cancel.cancel();
            }
            Ok(std::ops::ControlFlow::Continue(()))
        })
        .await;

    assert!(
        matches!(result, Err(crate::Error::Cancelled)),
        "a mid-scan cancel must abort with Error::Cancelled, got {result:?}"
    );
    assert!(
        count >= TRIP_AT && count < TOTAL as usize,
        "must abort after the trip point but well before the full {TOTAL} partitions, got {count}"
    );
}

/// The Summary.db/Index.db-PRESENT resolution mode (roborev round 3, issue
/// #2264): `iterate_all_partitions` (`partition_lookup.rs`) has TWO resolution
/// modes — the index-backed per-entry lookup (this poll's target) and the
/// `sequential_scan` fallback (already covered above via `index_less_fixture`,
/// which strips Summary/Index/Filter).
///
/// Reaching a FULLY-RESOLVED index-backed pass (`results.len() ==
/// entries.len()`, needed for `iterate_all_partitions` to actually RETURN the
/// index-backed result instead of falling through) needs a REAL Cassandra
/// fixture: investigation for this test found `SSTableWriter`-produced
/// Summary.db/Index.db pairs never fully resolve — `lookup_partition_with_index`
/// DOES find a byte-identical key, but the resolved `PartitionIndexEntry`'s
/// `data_offset`/`data_size` are degenerate (`(0, 0)`) for CQLite's own writer
/// output, a pre-existing gap unrelated to #2264 (NOT fixed here — filed
/// separately). So `write_fixture`'s output cannot exercise the FULLY-RESOLVED
/// branch, and `iterate_all_partitions` returns one materialised `Vec` with no
/// emit callback — a cancelled call has no partition-count observable either
/// way, so the callback-based "trip at partition N" technique the other tests
/// use does not apply here.
///
/// This proves the loop's poll instead via the process-global
/// `read_work_counters::index_probes()` counter (same oracle technique as
/// `chunk_cache_wiring_tests.rs`): a pre-cancelled call over a REAL fixture with
/// at least one Summary.db entry must record ZERO Index.db probes — proving the
/// poll aborts BEFORE the loop attempts even the FIRST lookup. Disabling ONLY
/// this poll lets `lookup_partition_with_index` run for entry 0 (recording a
/// probe) before the function falls through to `sequential_scan`'s (unrelated,
/// pre-existing) poll for the SAME final `Err(Cancelled)` — so the probe-count
/// assertion, not the `Err` alone, discriminates this specific poll from the
/// downstream fallback's. `#[serial]`: the counter is process-global.
fn datasets_root() -> Option<std::path::PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(std::path::PathBuf::from)
        .filter(|p| p.is_dir())
}

/// `test_basic.multi_partition_table`: a real fixture with Summary.db +
/// Index.db present and at least one Summary.db entry (confirmed during
/// investigation for this test).
///
/// Only returns a candidate whose Summary.db AND Index.db sidecars both exist —
/// they are what let `SSTableReader::open` populate `summary_reader`/
/// `index_reader`, the precondition for `iterate_all_partitions` to take the
/// index-backed branch this test exercises. A Data.db-only snapshot is skipped
/// (returns `None`) rather than driving the test down the sequential-scan
/// fallback, where the poll under test never runs.
fn real_index_backed_fixture() -> Option<std::path::PathBuf> {
    let base = datasets_root()?.join("sstables/test_basic");
    for entry in std::fs::read_dir(&base).ok()?.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("multi_partition_table-") {
            let dir = entry.path();
            let candidate = dir.join("nb-1-big-Data.db");
            let summary = dir.join("nb-1-big-Summary.db");
            let index = dir.join("nb-1-big-Index.db");
            if candidate.is_file() && summary.is_file() && index.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

/// Skip (not fail) when the real fixture binary is absent — matches the
/// established convention (`issue_1594_fanout_deadlock_test.rs` et al.) for
/// tests needing `CQLITE_DATASETS_ROOT`'s local-only binaries.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn pre_cancelled_scan_does_not_probe_index_on_index_backed_path() {
    let Some(data_path) = real_index_backed_fixture() else {
        eprintln!(
            "Skipping (index-backed cancel poll): real multi_partition_table \
             fixture not present (set CQLITE_DATASETS_ROOT)"
        );
        return;
    };
    let mut reader = open_reader(&data_path).await;

    // ------------------------------------------------------------------
    // Guard 1 (structural): the reader must actually hold BOTH an
    // `index_reader` and a non-empty `summary_reader`, or the index-backed
    // `if let Some(summary_reader)` loop in `iterate_all_partitions` is never
    // entered and the whole call falls through to `sequential_scan` — a path
    // with its OWN (pre-existing, unrelated) poll. Without this, the fixture
    // could satisfy the test vacuously via the fallback, recording zero probes
    // whether or not THIS poll exists.
    let entry_count = reader
        .summary_reader
        .as_ref()
        .map(|s| s.get_entries().len())
        .unwrap_or(0);
    assert!(
        entry_count > 0,
        "fixture must have at least one Summary.db entry for this test to be non-vacuous"
    );
    assert!(
        reader.index_reader.is_some(),
        "fixture must load an Index.db reader — otherwise iterate_all_partitions \
         cannot take the index-backed branch this test targets (issue #2264)"
    );

    // ------------------------------------------------------------------
    // Guard 2 (behavioural, the real fail-if-vacuous check): an UNCANCELLED
    // pass over this exact fixture must record index probes > 0. Each iteration
    // of the index-backed loop calls `lookup_partition_with_index`, which
    // records exactly one probe per real Index.db lookup. A nonzero count here
    // proves the per-entry loop body — the one guarded by the poll under test —
    // is genuinely executed for this fixture (it does not silently resolve via
    // the sequential-scan fallback with zero probes). If a future fixture/refactor
    // stopped exercising the index-backed branch, THIS assertion fails, so the
    // pre-cancelled assertion below can never pass vacuously.
    crate::storage::sstable::read_work_counters::reset();
    let uncancelled = open_reader(&data_path).await;
    let uncancelled_result = uncancelled.iterate_all_partitions().await;
    assert!(
        uncancelled_result.is_ok(),
        "the uncancelled index-backed scan must succeed: {uncancelled_result:?}"
    );
    let uncancelled_probes = crate::storage::sstable::read_work_counters::index_probes();
    assert!(
        uncancelled_probes > 0,
        "the index-backed branch must be exercised — an uncancelled scan over this \
         fixture recorded zero Index.db probes, meaning the loop this test targets is \
         never entered (the test would pass vacuously). Got {uncancelled_probes}"
    );

    // ------------------------------------------------------------------
    // The actual assertion: a pre-cancelled scan must record ZERO probes —
    // the poll aborts BEFORE the loop attempts even the first Index.db lookup.
    crate::storage::sstable::read_work_counters::reset();
    let cancel = ScanCancel::new();
    cancel.cancel();
    reader.set_scan_cancel(cancel);

    let result = reader.iterate_all_partitions().await;

    assert!(
        matches!(result, Err(crate::Error::Cancelled)),
        "a pre-cancelled index-backed scan must abort with Error::Cancelled, got {result:?}"
    );
    assert_eq!(
        crate::storage::sstable::read_work_counters::index_probes(),
        0,
        "the index-backed loop's poll must abort BEFORE attempting even the first \
         Index.db lookup — a nonzero probe count means the loop ran ahead of the \
         cancel check (issue #2264, roborev round 3)"
    );
}
