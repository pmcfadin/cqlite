//! Issue #2361 — the non-stitching scan path must TRUE-stream each partition
//! instead of materialising the whole SSTable into one `Vec` before the first
//! emit (the 1.13M-partition hang / unbounded memory). No producer-side `LIMIT`
//! budget exists here (removed in roborev round 2 — a partition count is not a
//! safe proxy for a row-level `LIMIT`; see `stream_all_partitions_cancellable`'s
//! doc) — `LIMIT` bounding is proven at the flight level instead
//! (`streaming_tests.rs`'s backpressured-teardown + sparse-predicate tests).
//!
//! These tests drive [`SSTableReader::stream_all_partitions_cancellable`] and
//! [`SSTableReader::stream_all_partitions_via_full_index`] DIRECTLY over a
//! writer-produced uncompressed SSTable (which carries a full `Index.db`, so the
//! streaming index walk applies). Every one of them references an API that does
//! not exist on pre-#2361 `main`, so the module is COMPILE-RED there (the
//! accepted red-then-green convention for a new streaming seam).

use crate::schema::{Column, KeyColumn, TableSchema};
use crate::storage::scan_cancel::ScanCancel;
use crate::storage::sstable::reader::data_access::full_index_stream::FullIndexStreamOutcome;
use crate::storage::sstable::reader::SSTableReader;
use crate::storage::write_engine::mutation::{CellOperation, Mutation, PartitionKey, TableId};
use crate::types::Value;
use crate::{Config, Platform};
use serial_test::serial;
use std::collections::HashMap;
use std::ops::ControlFlow;
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

/// Write `n` single-row partitions to a fresh uncompressed SSTable, keeping every
/// component (Index.db included). Returns the temp dir (keep alive) + Data.db path.
async fn write_fixture(n: i32) -> (TempDir, std::path::PathBuf) {
    let schema = schema();
    let temp = TempDir::new().unwrap();
    let mut writer =
        crate::storage::sstable::writer::SSTableWriter::new(temp.path().to_path_buf(), 1, &schema)
            .unwrap();
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

async fn open_reader(data_path: &std::path::Path) -> SSTableReader {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());
    SSTableReader::open(data_path, &config, platform)
        .await
        .unwrap()
}

/// Non-vacuity + full enumeration (issue #2361): `stream_all_partitions_cancellable`
/// (no `limit` parameter) emits every one of the fixture's `N` partitions.
///
/// `#[serial(work_counters)]` (issue #2398, roborev 1693): this walk increments
/// the process-global `stream_walk_partitions_parsed` counter as a side effect;
/// every test in this file that does so shares the `work_counters` key so none
/// contaminates `sequential_scan_fallback_counts_each_partition_exactly_once`'s
/// delta assertion (the established convention, issue #1071).
#[tokio::test]
#[serial(work_counters)]
async fn stream_all_partitions_cancellable_emits_every_partition() {
    const N: i32 = 24;
    let (_temp, data_path) = write_fixture(N).await;
    let reader = open_reader(&data_path).await;
    let cancel = ScanCancel::default();

    let mut all = 0usize;
    reader
        .stream_all_partitions_cancellable(&cancel, |_row| {
            all += 1;
            Ok(ControlFlow::Continue(()))
        })
        .await
        .unwrap();
    assert_eq!(
        all, N as usize,
        "streaming scan must emit every one of the {N} partitions \
         (non-vacuity guard: the fixture actually holds {N} partitions)"
    );
}

/// Streaming full-index walk (issue #2361): a writer-produced BIG SSTable with a
/// resolvable `Index.db` streams EVERY partition via the index walk (outcome
/// `Streamed`), in index (token) order, emitting each as it is resolved rather
/// than after a whole-file materialisation. Non-vacuity: all N rows arrive.
///
/// `#[serial(work_counters)]`: see the doc on
/// `stream_all_partitions_cancellable_emits_every_partition` above.
#[tokio::test]
#[serial(work_counters)]
async fn stream_via_full_index_streams_every_partition_in_order() {
    const N: i32 = 16;
    let (_temp, data_path) = write_fixture(N).await;
    let reader = open_reader(&data_path).await;
    let cancel = ScanCancel::default();

    let mut streamed_keys: Vec<crate::RowKey> = Vec::new();
    let outcome = reader
        .stream_all_partitions_via_full_index(&cancel, &mut |(key, _value)| {
            streamed_keys.push(key);
            Ok(ControlFlow::Continue(()))
        })
        .await
        .unwrap();

    // The writer emits a resolvable Index.db, so the streaming walk applies. If a
    // future writer change made the index unresolvable the walk would FellBack —
    // fail loudly here rather than silently pass on an empty emit.
    assert!(
        matches!(outcome, FullIndexStreamOutcome::Streamed),
        "writer fixture with a full Index.db must stream via the index walk, not fall back"
    );
    assert_eq!(
        streamed_keys.len(),
        N as usize,
        "the streaming index walk must emit every partition ({N})"
    );

    // Ordering contract: the streaming walk must emit partitions in the SAME
    // (token) order the materialising walk produces — the order the k-way merger
    // requires. Compare key-for-key against `iterate_all_partitions` (token-sorted).
    let materialised: Vec<crate::RowKey> = reader
        .iterate_all_partitions()
        .await
        .unwrap()
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    assert_eq!(
        streamed_keys, materialised,
        "streaming emission order must match the materialising token order (merge-input contract)"
    );
}

/// Early-break (issue #2361): a consumer returning `ControlFlow::Break` after the
/// FIRST emit stops the walk immediately — the anti-materialisation property. On
/// the pre-#2361 code the whole SSTable was materialised into a `Vec` BEFORE the
/// first emit, so a break could never save that work; here it does.
///
/// `#[serial(work_counters)]`: see the doc on
/// `stream_all_partitions_cancellable_emits_every_partition` above.
#[tokio::test]
#[serial(work_counters)]
async fn stream_all_partitions_cancellable_stops_on_break() {
    const N: i32 = 20;
    let (_temp, data_path) = write_fixture(N).await;
    let reader = open_reader(&data_path).await;
    let cancel = ScanCancel::default();

    let mut emitted = 0usize;
    reader
        .stream_all_partitions_cancellable(&cancel, |_row| {
            emitted += 1;
            Ok(ControlFlow::Break(()))
        })
        .await
        .unwrap();
    assert_eq!(
        emitted, 1,
        "a consumer that breaks after the first emit must stop the walk immediately"
    );
}

/// Cancellation (issue #2361): a scan whose token is already tripped abandons the
/// walk promptly, emitting nothing and returning `Error::Cancelled` — the
/// cooperative poll at the top of the streaming walk.
///
/// `#[serial(work_counters)]`: see the doc on
/// `stream_all_partitions_cancellable_emits_every_partition` above.
#[tokio::test]
#[serial(work_counters)]
async fn stream_all_partitions_cancellable_pre_cancel_emits_nothing() {
    const N: i32 = 12;
    let (_temp, data_path) = write_fixture(N).await;
    let reader = open_reader(&data_path).await;

    let cancel = ScanCancel::default();
    cancel.cancel();

    let mut emitted = 0usize;
    let result = reader
        .stream_all_partitions_cancellable(&cancel, |_row| {
            emitted += 1;
            Ok(ControlFlow::Continue(()))
        })
        .await;

    assert!(
        matches!(result, Err(crate::Error::Cancelled)),
        "a pre-cancelled streaming scan must return Error::Cancelled, got {result:?}"
    );
    assert_eq!(emitted, 0, "a pre-cancelled scan must emit no rows");
}

/// Roborev 1693 (issue #2398): a reader with NO `Index.db` (the sibling deleted
/// before open, so `index_reader` is `None`) routes `stream_all_partitions_cancellable`
/// straight into the SAME materialising `sequential_scan` fallback code path a
/// `FellBack` streaming-walk outcome would use. `stream_walk_partitions_parsed`
/// must land at EXACTLY `N` — one increment per partition, owned solely by
/// `sequential_scan`'s internal accounting — never `2 * N` (the double-count a
/// redundant increment in the fallback's re-emit loop would produce).
///
/// `#[serial(work_counters)]`: `stream_walk_partitions_parsed` is a process-global
/// counter (issue #1071's established convention) — every OTHER test in this file
/// that touches it shares this key so this delta assertion cannot be contaminated
/// by a concurrently-running sibling.
#[tokio::test]
#[serial(work_counters)]
async fn sequential_scan_fallback_counts_each_partition_exactly_once() {
    const N: i32 = 10;
    let (_temp, data_path) = write_fixture(N).await;
    // Force the fallback: delete the sibling Index.db so `index_reader` is `None`
    // and `stream_all_partitions_cancellable` falls straight through to the
    // materialising `sequential_scan` — the same code the streaming walk's
    // `FellBack` outcome reaches.
    let index_path =
        std::path::PathBuf::from(data_path.to_str().unwrap().replace("-Data.db", "-Index.db"));
    assert!(index_path.exists(), "fixture must have written an Index.db");
    std::fs::remove_file(&index_path).unwrap();

    let reader = open_reader(&data_path).await;
    assert!(
        reader.index_reader.is_none(),
        "the reader must open with no usable Index.db (sibling deleted)"
    );
    let cancel = ScanCancel::default();

    crate::storage::sstable::work_counters::reset();
    let mut emitted = 0usize;
    reader
        .stream_all_partitions_cancellable(&cancel, |_row| {
            emitted += 1;
            Ok(ControlFlow::Continue(()))
        })
        .await
        .unwrap();

    assert_eq!(
        emitted, N as usize,
        "the fallback must emit every partition"
    );
    assert_eq!(
        crate::storage::sstable::work_counters::stream_walk_partitions_parsed(),
        N as u64,
        "the fallback must count each partition body EXACTLY ONCE (not 2x — \
         roborev 1693): sequential_scan owns the increment, the re-emit loop \
         in stream_all_partitions_cancellable must not increment again"
    );
}

/// Issue #2366 (AC #1 parity, larger fixture): over an N ≥ 50 uncompressed
/// fixture the sequential-window streaming walk emits EVERY partition, in the
/// SAME token order, with IDENTICAL keys to the materialising sibling
/// `iterate_all_partitions` — the parity pin proving the window-served slices
/// decode to exactly the same partitions as the old per-partition positioned
/// reads. Larger than the smoke fixtures so several partitions share one window.
///
/// `#[serial(work_counters)]`: see
/// `stream_all_partitions_cancellable_emits_every_partition`.
#[tokio::test]
#[serial(work_counters)]
async fn windowed_stream_matches_materialising_over_larger_fixture() {
    const N: i32 = 60;
    let (_temp, data_path) = write_fixture(N).await;
    let reader = open_reader(&data_path).await;
    let cancel = ScanCancel::default();

    let mut streamed_keys: Vec<crate::RowKey> = Vec::new();
    let outcome = reader
        .stream_all_partitions_via_full_index(&cancel, &mut |(key, _value)| {
            streamed_keys.push(key);
            Ok(ControlFlow::Continue(()))
        })
        .await
        .unwrap();
    assert!(
        matches!(outcome, FullIndexStreamOutcome::Streamed),
        "a full-Index.db fixture must stream via the index walk, not fall back"
    );

    let materialised: Vec<crate::RowKey> = reader
        .iterate_all_partitions()
        .await
        .unwrap()
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    assert_eq!(
        streamed_keys.len(),
        N as usize,
        "the windowed streaming walk must emit every one of the {N} partitions"
    );
    assert_eq!(
        streamed_keys, materialised,
        "windowed streaming emission must be key-for-key IDENTICAL (order + \
         content) to the materialising sibling — the merge-input parity pin (#2366)"
    );
}

/// Issue #2366 (AC #1 wiring): the SAME parity holds end-to-end through the real
/// flight full-scan producer surface `stream_all_partitions_for_compaction` (the
/// non-stitching branch routes to the windowed streaming walk). Proves the fix
/// is exercised by the public producer seam, not just the private helper.
///
/// `#[serial(work_counters)]`: see
/// `stream_all_partitions_cancellable_emits_every_partition`.
#[tokio::test]
#[serial(work_counters)]
async fn stream_for_compaction_emits_every_partition_windowed() {
    const N: i32 = 50;
    let (_temp, data_path) = write_fixture(N).await;
    let reader = open_reader(&data_path).await;
    let cancel = ScanCancel::default();

    let mut emitted = 0usize;
    reader
        .stream_all_partitions_for_compaction(None, &cancel, |_row| {
            emitted += 1;
            Ok(ControlFlow::Continue(()))
        })
        .await
        .unwrap();
    assert_eq!(
        emitted, N as usize,
        "the flight full-scan producer surface must emit every one of the {N} \
         partitions through the windowed non-stitching branch"
    );
}

/// Issue #2366 (AC #3 read-pattern benchmark, work-counters): after the windowed
/// streaming walk over a LARGE uncompressed fixture (N = 500) the read pattern is
/// O(N/window), not O(N):
///
/// - `INDEX_PROBES == 0` (was `== N` before this change — one HashMap probe per
///   partition via the dropped `lookup_partition_with_index`); the offset now
///   comes straight from the in-memory offset table.
/// - `seek_calls()` (one per sequential window refill) is DRAMATICALLY fewer than
///   the partition count — the O(N)→O(N/window) reduction. The tiny single-row
///   partitions here pack many per 4 MiB window, so in practice this is a handful
///   of refills for 500 partitions.
///
/// MEASURED (this fixture, 500 single-row int/text partitions):
///   before: INDEX_PROBES == 500, positioned reads == 500 (one per partition)
///   after:  INDEX_PROBES == 0,   seek_calls (window refills) == 1
/// The field ideal (a 1.13M-partition table's wall-time on the #2362/#2157/#2264
/// live testbed) is NOT reproducible locally; this probe/seek count reduction is
/// the acceptable substitute the issue permits.
///
/// `#[serial(work_counters)]`: `INDEX_PROBES`/`SEEK_CALLS` are process-global.
#[tokio::test]
#[serial(work_counters)]
async fn windowed_stream_read_pattern_is_sequential() {
    use crate::storage::sstable::read_work_counters as rwc;
    const N: i32 = 500;
    let (_temp, data_path) = write_fixture(N).await;
    let reader = open_reader(&data_path).await;
    let cancel = ScanCancel::default();

    rwc::reset();
    let mut emitted = 0usize;
    let outcome = reader
        .stream_all_partitions_via_full_index(&cancel, &mut |_row| {
            emitted += 1;
            Ok(ControlFlow::Continue(()))
        })
        .await
        .unwrap();

    assert!(
        matches!(outcome, FullIndexStreamOutcome::Streamed),
        "the large fixture must stream via the index walk"
    );
    assert_eq!(emitted, N as usize, "must emit every one of the {N} partitions");

    // AC #3: zero per-partition index probes (was N before #2366).
    assert_eq!(
        rwc::index_probes(),
        0,
        "the windowed walk must perform ZERO Index.db probes (offset read \
         directly from the in-memory offset table) — was {N} before #2366"
    );
    // AC #3: sequential window refills, not one read per partition. Assert well
    // below the partition count (the O(N)→O(N/window) reduction). It is exactly 1
    // for this tiny-partition fixture, but pin the invariant loosely so the
    // benchmark stays robust to fixture-size / window-target tweaks.
    let seeks = rwc::seek_calls();
    assert!(
        seeks < N as u64 / 4,
        "window refills ({seeks}) must be dramatically fewer than the {N} \
         partitions (O(N/window) sequential reads, not O(N) random reads — #2366)"
    );
    assert!(
        seeks >= 1,
        "a non-empty scan must perform at least one window read ({seeks})"
    );
}
