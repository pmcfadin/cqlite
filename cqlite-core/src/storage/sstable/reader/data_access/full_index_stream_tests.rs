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
