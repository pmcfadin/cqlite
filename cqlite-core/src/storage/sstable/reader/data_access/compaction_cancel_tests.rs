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
    let reader = open_reader(&data_path).await;

    // Issue #2346: `scan_cancel` is now a PER-CALL parameter, not a field
    // mutated onto the reader — passed directly to
    // `stream_all_partitions_for_compaction` below instead of via
    // `set_scan_cancel`.
    let cancel = ScanCancel::new();

    let mut count = 0usize;
    let result = reader
        .stream_all_partitions_for_compaction(Some(&schema()), &cancel, |_row| {
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
        .stream_all_partitions_for_compaction(Some(&schema()), &ScanCancel::default(), |_row| {
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
    let reader = open_reader(&data_path).await;

    let cancel = ScanCancel::new();
    cancel.cancel();

    let mut count = 0usize;
    let result = reader
        .stream_all_partitions_for_compaction(Some(&schema()), &cancel, |_row| {
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
    let reader = open_reader(&data_path).await;

    let cancel = ScanCancel::new();

    let mut count = 0usize;
    let result = reader
        .stream_all_partitions_for_compaction(Some(&schema()), &cancel, |_row| {
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

/// Red-then-green (issue #2346, BLOCKER 1): the STITCHED `sequential_scan` path
/// (`requires_chunk_stitching()` branch) must honour the PER-CALL cancel token.
///
/// Pre-#2346-fix, that branch called `stitch_and_parse_all_chunks` with NO token
/// (only the non-stitching branch polled), so a pre-cancelled caller blocked
/// until the ENTIRE data section was stitched + parsed and returned `Ok(all
/// partitions)` — the `Err(Cancelled)` assertion below FAILS against that code.
/// After the fix the chunk-stitch loop polls at chunk 0 (`stitch_all_chunks_
/// cancellable`) and aborts with `Error::Cancelled`.
///
/// This drives `sequential_scan` DIRECTLY (rather than through
/// `iterate_all_partitions_cancellable`, whose index-backed branch a
/// writer-produced fixture may enter) so the stitched branch is unambiguously
/// exercised — `table_id` is intentionally ignored there (see the branch's own
/// comment), so any value serves.
///
/// Pre-cancel is the DETERMINISTIC discriminator (same convention as
/// `pre_cancelled_scan_aborts_at_first_poll` above): `sequential_scan` returns a
/// materialised `Vec` with no emit callback, so a concurrent mid-scan cancel
/// would be a wall-clock race — a pre-cancelled token instead proves the poll
/// fires on the FIRST chunk with no timing dependency. The uncancelled control
/// first proves this exact path yields every partition, so the abort is cutting
/// real work short, not passing on an empty scan.
#[tokio::test(flavor = "multi_thread")]
async fn stitched_sequential_scan_honors_per_call_cancel() {
    use crate::types::TableId;
    const TOTAL: i32 = 1000;
    let (_temp, data_path) = index_less_fixture(TOTAL).await;
    let reader = open_reader(&data_path).await;

    // Non-vacuity guard: the fixture MUST take the STITCHED branch (the
    // BLOCKER-1 gap), not the non-stitching branch that already polled.
    assert!(
        reader.requires_chunk_stitching(),
        "fixture must take the stitched sequential-scan branch to cover the \
         #2346 BLOCKER-1 gap"
    );

    let schema = schema();
    // `table_id` is ignored in the stitching branch, so any value works.
    let table_id = TableId::from("test_ks.test_table");

    // Positive control: an uncancelled pass over THIS path returns every
    // partition — so the pre-cancel abort below is cutting real work short.
    let uncancelled = reader
        .sequential_scan(
            &table_id,
            None,
            None,
            None,
            Some(&schema),
            &ScanCancel::default(),
        )
        .await
        .expect("uncancelled stitched scan must succeed");
    assert_eq!(
        uncancelled.len(),
        TOTAL as usize,
        "the stitched sequential-scan path must return every partition when not cancelled"
    );

    let cancel = ScanCancel::new();
    cancel.cancel();
    let result = reader
        .sequential_scan(&table_id, None, None, None, Some(&schema), &cancel)
        .await;
    assert!(
        matches!(result, Err(crate::Error::Cancelled)),
        "a pre-cancelled stitched sequential-scan must abort with Error::Cancelled \
         (issue #2346 BLOCKER-1), got {:?}",
        result.map(|v| v.len())
    );
}

/// RED PROOF (issue #2346): two concurrent scans over ONE SHARED
/// `Arc<SSTableReader>` cancel INDEPENDENTLY.
///
/// This is the core claim #2346 exists to unlock (a future Flight warm-handle
/// registry serving two concurrent requests off ONE cached reader). It is
/// impossible to even express against pre-#2346 `main`: cancellation there was
/// mutable per-reader state set via `SSTableReader::set_scan_cancel(&mut self,
/// ..)`. A SHARED `Arc<SSTableReader>` offers no `&mut self` (short of
/// `Arc::get_mut`, which requires the refcount to be exactly 1 — impossible
/// while a second concurrent scan holds its own clone), so two concurrent
/// callers could not even give the ONE shared reader two DIFFERENT tokens; the
/// only token that existed was whatever was mutated in last, racing/clobbering
/// the other caller's. This test therefore FAILS TO COMPILE on pre-#2346 `main`
/// — `stream_all_partitions_for_compaction` took no per-call `scan_cancel`
/// argument at all, so there was no way to pass two independent tokens for two
/// concurrent calls on one `&self` reader. The compile failure IS the red
/// proof; there is no runtime "before" state to assert against.
///
/// After the fix, `scan_cancel` is a per-call `&ScanCancel` argument (issue
/// #2346) — never reader-mutated state — so this spawns two scans on the SAME
/// `Arc<SSTableReader>`: scan A self-cancels mid-stream via its OWN token; scan
/// B (a different token, never cancelled) must still stream every partition,
/// UNAFFECTED by A's cancellation — proving true independence, not shared
/// mutable state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_concurrent_scans_on_shared_reader_cancel_independently() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    const TOTAL: i32 = 2000;
    const TRIP_AT: usize = 300;
    const PARK_AT: usize = 300;
    let (_temp, data_path) = index_less_fixture(TOTAL).await;
    let reader = std::sync::Arc::new(open_reader(&data_path).await);

    let cancel_a = ScanCancel::new();
    let cancel_b = ScanCancel::new();

    // Two-way handshake (roborev, issue #2346) — WITHOUT it scan B can finish
    // before scan A ever cancels, so the test would pass even if cancellation
    // WERE shared (A's cancel would arrive after B was already done). These
    // latches force BOTH scans demonstrably in flight at the instant A cancels:
    //   * `b_parked`  — B has reached PARK_AT (mid-scan, NOT finished) and is
    //                   waiting; A must not cancel until this is set.
    //   * `a_cancelled` — A has cancelled its own token; B resumes only after.
    // Neither side can deadlock: each waits on a flag the OTHER (running on its
    // own worker thread) sets independently. The busy-wait is a sync section
    // inside the emit callback (which cannot `.await`); `worker_threads = 4`
    // guarantees both tasks run on distinct workers concurrently. The `timeout`
    // around the joins is a safety net, not the discriminator — the counts are.
    let b_parked = std::sync::Arc::new(AtomicBool::new(false));
    let a_cancelled = std::sync::Arc::new(AtomicBool::new(false));

    let reader_a = std::sync::Arc::clone(&reader);
    let cancel_a_task = cancel_a.clone();
    let b_parked_a = std::sync::Arc::clone(&b_parked);
    let a_cancelled_a = std::sync::Arc::clone(&a_cancelled);
    let handle_a = tokio::spawn(async move {
        let mut count = 0usize;
        let result = reader_a
            .stream_all_partitions_for_compaction(Some(&schema()), &cancel_a_task, |_row| {
                count += 1;
                if count == TRIP_AT {
                    // Wait until B is demonstrably parked mid-scan, THEN cancel —
                    // so B is provably still in flight when A's cancel fires.
                    while !b_parked_a.load(Ordering::Acquire) {
                        std::hint::spin_loop();
                    }
                    cancel_a_task.cancel();
                    a_cancelled_a.store(true, Ordering::Release);
                }
                Ok(std::ops::ControlFlow::Continue(()))
            })
            .await;
        (result, count)
    });

    let reader_b = std::sync::Arc::clone(&reader);
    let cancel_b_task = cancel_b.clone();
    let b_parked_b = std::sync::Arc::clone(&b_parked);
    let a_cancelled_b = std::sync::Arc::clone(&a_cancelled);
    let handle_b = tokio::spawn(async move {
        let mut count = 0usize;
        let result = reader_b
            .stream_all_partitions_for_compaction(Some(&schema()), &cancel_b_task, |_row| {
                count += 1;
                if count == PARK_AT {
                    // Announce we are mid-scan, then hold here until A has
                    // cancelled — guaranteeing B has NOT finished at that moment.
                    b_parked_b.store(true, Ordering::Release);
                    while !a_cancelled_b.load(Ordering::Acquire) {
                        std::hint::spin_loop();
                    }
                }
                Ok(std::ops::ControlFlow::Continue(()))
            })
            .await;
        (result, count)
    });

    let (result_a, count_a) = tokio::time::timeout(Duration::from_secs(60), handle_a)
        .await
        .expect("scan A must finish within the safety timeout (no deadlock)")
        .expect("scan A task did not panic");
    let (result_b, count_b) = tokio::time::timeout(Duration::from_secs(60), handle_b)
        .await
        .expect("scan B must finish within the safety timeout (no deadlock)")
        .expect("scan B task did not panic");

    assert!(
        matches!(result_a, Err(crate::Error::Cancelled)),
        "scan A (self-cancelling mid-scan via its OWN token) must abort with \
         Error::Cancelled, got {result_a:?}"
    );
    assert!(
        count_a >= TRIP_AT && count_a < TOTAL as usize,
        "scan A must stop after its own trip point but before the full {TOTAL} \
         partitions, got {count_a}"
    );

    assert!(
        result_b.is_ok(),
        "scan B (never cancelled, independent token) must succeed even though A \
         cancelled while B was provably mid-scan: {result_b:?}"
    );
    assert_eq!(
        count_b, TOTAL as usize,
        "scan B must stream every partition, UNAFFECTED by scan A's cancellation \
         on the SAME shared reader while B was demonstrably in flight — proving \
         the two tokens are truly independent, not per-reader mutable state (issue #2346)"
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
/// This proves the loop's poll via the per-partition BODY-DECODE work-probe
/// (`stream_walk_partitions_parsed`), read through a thread-local
/// `StreamWalkScope` (issue #2428): a pre-cancelled call over a REAL fixture with
/// at least one Summary.db entry must decode ZERO partition bodies — proving the
/// poll aborts BEFORE the loop reaches its first per-partition decode. Disabling
/// ONLY this poll lets the loop decode partition bodies (a nonzero count) before
/// the function returns the SAME final `Err(Cancelled)` — so the body-decode
/// count, not the `Err` alone, discriminates this specific poll.
///
/// Issue #2430 migrated this oracle OFF `read_work_counters::index_probes()`: the
/// materialising walk previously re-probed the index once per partition
/// (`lookup_partition_with_index`), and that redundant probe was what the old
/// oracle counted. The fix resolves each partition offset from the already-loaded
/// `Index.db` entry and never re-probes, so `index_probes` is 0 on the fixed path
/// — the body-decode work-probe is the surviving non-vacuous signal.
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
// Current-thread runtime (issue #2430): the non-vacuity/cancel oracle now bounds
// on `stream_walk_partitions_parsed` via a THREAD-LOCAL `StreamWalkScope` (issue
// #2428), which only observes increments on the thread that opened it. A default
// (current-thread) `#[tokio::test]` drives every inline `.await` of the scan on the
// test's own thread, so the scope sees exactly this scan's per-partition decodes and
// is immune to concurrent scan-driving tests — no `#[serial]`, no global reset.
#[tokio::test]
async fn pre_cancelled_scan_does_not_probe_index_on_index_backed_path() {
    use crate::storage::sstable::work_counters::stream_walk_scope::StreamWalkScope;
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
    // full-index walk in `iterate_all_partitions` is never entered and the whole
    // call falls through to `sequential_scan` — a path with its OWN (pre-existing,
    // unrelated) poll. Without this, the fixture could satisfy the test vacuously
    // via the fallback.
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
    // Guard 2 (behavioural, the real fail-if-vacuous check — migrated off the
    // per-partition index-PROBE count, issue #2430). Directly exercise the method
    // under test with a FRESH (uncancelled) token: it must FULLY resolve this real
    // fixture through the index-backed materialising branch (`Ok(Some(rows))`, not
    // the sequential fallback's `None`) AND decode > 0 partition bodies — the exact
    // per-partition work the cancel poll below guards. If a future fixture/refactor
    // stopped exercising the index-backed branch, THIS assertion fails, so the
    // pre-cancelled assertion below can never pass vacuously. The signal is
    // `stream_walk_partitions_parsed` (partition BODIES decoded), NOT `index_probes`
    // — after issue #2430 the loop resolves each offset from the already-loaded
    // entry and never re-probes the index, so `index_probes` is 0 even here.
    let uncancelled = open_reader(&data_path).await;
    let fresh = ScanCancel::new();
    let scope = StreamWalkScope::new();
    let uncancelled_result = uncancelled
        .iterate_all_partitions_via_full_index(&fresh)
        .await;
    let uncancelled_parsed = scope.count();
    drop(scope);
    assert!(
        matches!(uncancelled_result, Ok(Some(_))),
        "the index-backed materialising branch must FULLY resolve this real fixture \
         (Ok(Some(_)), not a fall-through to sequential_scan): {uncancelled_result:?}"
    );
    assert!(
        uncancelled_parsed > 0,
        "the index-backed loop must decode > 0 partition bodies — a zero count means \
         the loop this test targets never ran (the test would pass vacuously). \
         Got {uncancelled_parsed}"
    );

    // ------------------------------------------------------------------
    // The actual assertion: a pre-cancelled scan must decode ZERO partition
    // bodies — the poll aborts BEFORE the loop reaches even the first per-partition
    // decode. Disabling the poll would let the loop parse partition bodies here,
    // driving the count nonzero and failing this assertion (it discriminates).
    let cancel = ScanCancel::new();
    cancel.cancel();
    reader.set_scan_cancel(cancel);

    let scope = StreamWalkScope::new();
    let result = reader.iterate_all_partitions().await;
    let cancelled_parsed = scope.count();
    drop(scope);

    assert!(
        matches!(result, Err(crate::Error::Cancelled)),
        "a pre-cancelled index-backed scan must abort with Error::Cancelled, got {result:?}"
    );
    assert_eq!(
        cancelled_parsed, 0,
        "the index-backed loop's poll must abort BEFORE decoding even the first \
         partition body — a nonzero count means the loop ran ahead of the cancel \
         check (issue #2264, roborev round 3; oracle migrated in #2430)"
    );
}
