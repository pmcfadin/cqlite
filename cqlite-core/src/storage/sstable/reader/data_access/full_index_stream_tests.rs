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

use crate::schema::{ClusteringColumn, Column, KeyColumn, TableSchema};
use crate::storage::scan_cancel::ScanCancel;
use crate::storage::sstable::reader::data_access::full_index_stream::FullIndexStreamOutcome;
use crate::storage::sstable::reader::SSTableReader;
use crate::storage::write_engine::mutation::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, TableId,
};
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
/// No `#[serial(work_counters)]` needed (issue #2428): this walk bumps the
/// process-global `stream_walk_partitions_parsed` counter as a side effect, but
/// the only test that ASSERTS a delta on it
/// (`sequential_scan_fallback_counts_each_partition_exactly_once`) measures
/// through a thread-local scope immune to concurrent increments, so no sibling
/// serialization is required.
#[tokio::test]
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
/// No `#[serial(work_counters)]` needed (issue #2428): see the doc on
/// `stream_all_partitions_cancellable_emits_every_partition` above.
#[tokio::test]
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
/// No `#[serial(work_counters)]` needed (issue #2428): see the doc on
/// `stream_all_partitions_cancellable_emits_every_partition` above.
#[tokio::test]
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
/// No `#[serial(work_counters)]` needed (issue #2428): see the doc on
/// `stream_all_partitions_cancellable_emits_every_partition` above.
#[tokio::test]
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
/// Contamination-proof by construction (issue #2428): the assertion measures the
/// delta through a thread-local
/// [`StreamWalkScope`](crate::storage::sstable::work_counters::stream_walk_scope::StreamWalkScope),
/// NOT the process-global `stream_walk_partitions_parsed()` getter. The scope
/// records only increments that execute on THIS test's own thread, so a
/// concurrent scan-driving test on another thread (under thread-parallel
/// `cargo test --lib`, the CI Required-PR-Gate invocation, which does not isolate
/// tests per-process like nextest) cannot inflate the count. That is why this
/// test needs no `#[serial(work_counters)]` tag and no global `reset()`. See the
/// `work_counters::stream_walk_scope` module doc for the full rationale.
#[tokio::test]
async fn sequential_scan_fallback_counts_each_partition_exactly_once() {
    use crate::storage::sstable::work_counters::stream_walk_scope::StreamWalkScope;
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

    // Open the recording scope BEFORE the inline scan; it counts only this
    // thread's increments, so a concurrent scan-driving test on another thread
    // cannot contaminate the delta (issue #2428). No global `reset()` / getter
    // and no `#[serial(work_counters)]` needed: the fallback runs
    // `sequential_scan` INLINE (no `spawn`) on the current-thread runtime, so
    // every increment lands on this thread.
    let scope = StreamWalkScope::new();
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
        scope.count(),
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
/// Contamination-proof by construction (issue #2470): `INDEX_PROBES`/`SEEK_CALLS`
/// are process-global atomics bumped by every read-path probe/seek across the whole
/// `--lib` binary, so a `reset()`→scan→global-getter delta races any concurrent
/// read-driving test on another thread (the tag `#[serial(work_counters)]` — which
/// this test carried on `main` — does NOT help: it only serialises OTHER tagged
/// tests, and the observed contamination came from untagged crate-wide readers,
/// inflating `seek_calls()` to 144 against the `< 125` bound). The assertion now
/// measures the deltas through a thread-local
/// [`ReadWorkScope`](crate::storage::sstable::read_work_counters::read_work_scope::ReadWorkScope):
/// the uncompressed non-stitching walk records its window-refill seek INLINE on this
/// `#[tokio::test]`'s current-thread runtime, so the scope captures exactly this
/// scan's increments and no concurrent test on another thread can inflate them. No
/// global `reset()` and no `#[serial(work_counters)]` tag needed — see the
/// `read_work_counters::read_work_scope` module doc for the full rationale.
#[tokio::test]
async fn windowed_stream_read_pattern_is_sequential() {
    use crate::storage::sstable::read_work_counters as rwc;
    const N: i32 = 500;
    let (_temp, data_path) = write_fixture(N).await;
    let reader = open_reader(&data_path).await;
    let cancel = ScanCancel::default();

    // Open the recording scope BEFORE the inline scan; it counts only this thread's
    // increments, so a concurrent read-driving test on another thread cannot
    // contaminate the deltas (issue #2470). No global `reset()` needed.
    let work = rwc::read_work_scope::ReadWorkScope::new();
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
    assert_eq!(
        emitted, N as usize,
        "must emit every one of the {N} partitions"
    );

    // AC #3: zero per-partition index probes (was N before #2366).
    assert_eq!(
        work.index_probes(),
        0,
        "the windowed walk must perform ZERO Index.db probes (offset read \
         directly from the in-memory offset table) — was {N} before #2366"
    );
    // AC #3: sequential window refills, not one read per partition. Assert well
    // below the partition count (the O(N)→O(N/window) reduction). It is exactly 1
    // for this tiny-partition fixture, but pin the invariant loosely so the
    // benchmark stays robust to fixture-size / window-target tweaks.
    let seeks = work.seeks();
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

/// Roborev follow-up (issue #2366, review-first): every fixture above is tiny
/// (<4 MiB total Data.db), so `seek_calls == 1` in every one of them and the
/// multi-window-refill path (a partition's slice NOT starting at
/// `window_start == 0`, i.e. `lo != 0`) never actually runs — the defining new
/// behavior of this change was unexercised. Note: an EARLIER version of this
/// fixture tried to force the "large single partition" case with one ≈4.6 MiB
/// SINGLE-CELL value; that hit an UNRELATED pre-existing correctness bug (a
/// single-cell `Value::Text`/`Value::Blob` write/read round-trip silently drops
/// the whole row somewhere between ~950 KB and 1 MB — confirmed present on the
/// UNMODIFIED materialising sibling too, so it predates and is independent of
/// this change). Flagged separately; NOT fixed here (out of scope for #2366).
/// This fixture avoids that zone entirely: every per-cell value stays well
/// under 1 MB.
fn wide_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_wide_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "seq".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: Default::default(),
        }],
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "seq".to_string(),
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

fn wide_row_mutation(id: i32, seq: i32, text_len: usize) -> Mutation {
    Mutation::new(
        TableId::new("test_ks", "test_wide_table"),
        PartitionKey::single("id", Value::Integer(id)),
        Some(ClusteringKey::single("seq", Value::Integer(seq))),
        vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text("x".repeat(text_len)),
        }],
        1_000_000 + (id as i64) * 1_000_000 + seq as i64,
        None,
    )
}

/// Fixture for the multi-window-refill + large-single-partition-clamp test
/// (issue #2366 roborev follow-up): builds `small_count` ordinary single-row
/// partitions (`id = 2..=1+small_count`, each `small_text_len` bytes) PLUS ONE
/// "wide" multi-row partition (`id = 1`, `wide_row_count` clustered rows, each
/// `wide_row_text_len` bytes) — all under [`wide_schema`] so they share one
/// Data.db. The wide partition's TOTAL span (not any single cell) is what
/// exceeds `SEQUENTIAL_WINDOW_TARGET_BYTES`, avoiding the unrelated single-cell
/// defect noted on [`wide_schema`]. Every per-cell value here stays under 1 MB.
async fn write_multi_window_fixture(
    small_count: usize,
    small_text_len: usize,
    wide_row_count: usize,
    wide_row_text_len: usize,
) -> (TempDir, std::path::PathBuf) {
    let schema = wide_schema();
    let temp = TempDir::new().unwrap();
    let mut writer =
        crate::storage::sstable::writer::SSTableWriter::new(temp.path().to_path_buf(), 1, &schema)
            .unwrap();

    let mut partitions: Vec<(_, Vec<Mutation>)> = (0..small_count)
        .map(|idx| {
            let id = 2 + idx as i32;
            let m = wide_row_mutation(id, 0, small_text_len);
            let key = m.decorated_key(&schema).unwrap();
            (key, vec![m])
        })
        .collect();

    let wide_mutations: Vec<Mutation> = (0..wide_row_count)
        .map(|seq| wide_row_mutation(1, seq as i32, wide_row_text_len))
        .collect();
    let wide_key = wide_mutations[0].decorated_key(&schema).unwrap();
    partitions.push((wide_key, wide_mutations));

    partitions.sort_by_key(|(k, _)| k.token);
    for (key, muts) in partitions {
        writer.write_partition(key, muts).unwrap();
    }
    let info = writer.finish().await.unwrap();
    let data_path = info.data_path.clone();
    (temp, data_path)
}

/// Issue #2366 (roborev follow-up): every fixture above tops out well under
/// `SEQUENTIAL_WINDOW_TARGET_BYTES` (4 MiB), so `seek_calls == 1` in each of
/// them and the multi-window-refill path — a partition's slice NOT starting at
/// `window_start == 0` (`lo != 0`) — never actually ran. This fixture forces it:
///
/// - 50 ordinary single-row partitions × 100 KB ≈ 4.9 MiB total, so the walk
///   must refill AT LEAST once among just these (they alone exceed the 4 MiB
///   target).
/// - ONE wide (10-row, clustered) partition whose combined span ≈ 5 MB ALONE
///   exceeds the 4 MiB window target, forcing `want == span` — i.e. the window
///   is sized to exactly that partition (`lo == 0`, `hi == span == window.len()`,
///   the single-large-partition clamp roborev asked to pin). Built from
///   MULTIPLE rows rather than one giant cell specifically to avoid the
///   unrelated single-cell defect documented on [`wide_schema`].
///
/// Partitions are token-sorted before writing (this file's convention), so the
/// wide partition's position relative to the small ones — and hence the exact
/// refill count — is NOT controlled; the assertions are robust to that:
/// `seek_calls() >= 2` proves multiple windows fired regardless of order.
/// MEASURED (this fixture, 51 partitions / 60 rows, ≈9.9 MiB Data.db):
/// `INDEX_PROBES == 0`, `seek_calls() == 3` (3 sequential window reads,
/// not one per partition).
///
/// Asserts ROW-FOR-ROW parity (not just key order) against the materialising
/// sibling `iterate_all_partitions` — `(RowKey, ScanRow)` both derive
/// `PartialEq`, so this also proves the wide partition's rows round-tripped
/// intact through the windowed read, not just that some row with that key
/// arrived.
///
/// `#[serial(work_counters)]`: `INDEX_PROBES`/`SEEK_CALLS` are process-global.
#[tokio::test]
#[serial(work_counters)]
async fn windowed_stream_multi_refill_and_large_partition_clamp_parity() {
    use crate::storage::sstable::read_work_counters as rwc;

    const SMALL_COUNT: usize = 50;
    const SMALL_TEXT_LEN: usize = 100_000; // 50 × 100 KB ≈ 4.9 MiB > 4 MiB target.
    const WIDE_ROW_COUNT: usize = 10;
    const WIDE_ROW_TEXT_LEN: usize = 500_000; // 10 × 500 KB ≈ 5 MB > 4 MiB target.
    let expected_rows = SMALL_COUNT + WIDE_ROW_COUNT;

    let (_temp, data_path) = write_multi_window_fixture(
        SMALL_COUNT,
        SMALL_TEXT_LEN,
        WIDE_ROW_COUNT,
        WIDE_ROW_TEXT_LEN,
    )
    .await;
    let reader = open_reader(&data_path).await;
    let cancel = ScanCancel::default();

    rwc::reset();
    let mut streamed: Vec<(crate::RowKey, crate::types::ScanRow)> = Vec::new();
    let outcome = reader
        .stream_all_partitions_via_full_index(&cancel, &mut |row| {
            streamed.push(row);
            Ok(ControlFlow::Continue(()))
        })
        .await
        .unwrap();
    assert!(
        matches!(outcome, FullIndexStreamOutcome::Streamed),
        "a full-Index.db fixture must stream via the index walk, not fall back"
    );
    assert_eq!(
        streamed.len(),
        expected_rows,
        "the windowed walk must emit every one of the {expected_rows} rows \
         ({SMALL_COUNT} single-row partitions + {WIDE_ROW_COUNT} rows in the wide partition)"
    );

    // The defining new-behavior evidence (this roborev round): the read pattern
    // actually crosses a window boundary — unreachable by any prior (tiny)
    // fixture. Zero index probes still holds regardless of fixture size.
    assert_eq!(
        rwc::index_probes(),
        0,
        "the windowed walk must perform ZERO Index.db probes regardless of fixture size"
    );
    let seeks = rwc::seek_calls();
    assert!(
        seeks >= 2,
        "a >4 MiB Data.db (small partitions ≈4.9 MiB + a wide partition ≈5 MB) \
         must force at least 2 window refills (got {seeks}) — proves the \
         multi-window-refill path actually ran, not just the single-window case \
         every prior (tiny) fixture exercised"
    );

    // Row-for-row parity across the refill boundary/boundaries: identical to
    // the materialising sibling, including the wide partition's rows read via
    // the `want == span` window-sizing clamp.
    let materialised = reader.iterate_all_partitions().await.unwrap();
    assert_eq!(
        streamed, materialised,
        "windowed streaming rows (key AND decoded value) must be IDENTICAL to \
         the materialising sibling across the window-refill boundary — proves \
         `window_start` advances correctly and every partition's slice \
         (including the large single-partition clamp) is read intact"
    );
}
