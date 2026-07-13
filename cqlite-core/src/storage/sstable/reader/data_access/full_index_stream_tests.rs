//! Issue #2361 — the non-stitching scan path must TRUE-stream each partition and
//! honour a per-producer partition LIMIT, instead of materialising the whole
//! SSTable into one `Vec` before the first emit (the 1.13M-partition hang /
//! unbounded memory).
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

/// LIMIT bound (issue #2361): `stream_all_partitions_cancellable` with a
/// per-producer partition budget of `k` must emit at most `k` partitions over an
/// `N`-partition SSTable (`N > k`) — NOT all `N`. With `None` it emits every
/// partition (non-vacuity: proves the fixture really has `N` partitions).
#[tokio::test]
async fn stream_all_partitions_cancellable_respects_partition_limit() {
    const N: i32 = 24;
    const K: usize = 6;
    let (_temp, data_path) = write_fixture(N).await;
    let reader = open_reader(&data_path).await;
    let cancel = ScanCancel::default();

    // Unbounded: every partition is emitted.
    let mut all = 0usize;
    reader
        .stream_all_partitions_cancellable(&cancel, None, |_row| {
            all += 1;
            Ok(ControlFlow::Continue(()))
        })
        .await
        .unwrap();
    assert_eq!(
        all, N as usize,
        "unbounded streaming scan must emit every one of the {N} partitions \
         (non-vacuity guard: the fixture actually holds {N} partitions)"
    );

    // Bounded to K: the producer stops after ~K partitions (one row per
    // partition), never walking all N — the anti-hang property.
    let mut bounded = 0usize;
    reader
        .stream_all_partitions_cancellable(&cancel, Some(K), |_row| {
            bounded += 1;
            Ok(ControlFlow::Continue(()))
        })
        .await
        .unwrap();
    assert!(
        bounded <= K,
        "a LIMIT-{K} streaming scan must emit at most {K} partitions, got {bounded} \
         (LIMIT must bound the producer, not run to completion over {N})"
    );
    assert!(
        bounded > 0,
        "a LIMIT-{K} scan over {N} partitions must still emit rows"
    );
}

/// Streaming full-index walk (issue #2361): a writer-produced BIG SSTable with a
/// resolvable `Index.db` streams EVERY partition via the index walk (outcome
/// `Streamed`), in index (token) order, emitting each as it is resolved rather
/// than after a whole-file materialisation. Non-vacuity: all N rows arrive.
#[tokio::test]
async fn stream_via_full_index_streams_every_partition_in_order() {
    const N: i32 = 16;
    let (_temp, data_path) = write_fixture(N).await;
    let reader = open_reader(&data_path).await;
    let cancel = ScanCancel::default();

    let mut streamed_keys: Vec<crate::RowKey> = Vec::new();
    let outcome = reader
        .stream_all_partitions_via_full_index(&cancel, None, &mut |(key, _value)| {
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
#[tokio::test]
async fn stream_all_partitions_cancellable_stops_on_break() {
    const N: i32 = 20;
    let (_temp, data_path) = write_fixture(N).await;
    let reader = open_reader(&data_path).await;
    let cancel = ScanCancel::default();

    let mut emitted = 0usize;
    reader
        .stream_all_partitions_cancellable(&cancel, None, |_row| {
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
#[tokio::test]
async fn stream_all_partitions_cancellable_pre_cancel_emits_nothing() {
    const N: i32 = 12;
    let (_temp, data_path) = write_fixture(N).await;
    let reader = open_reader(&data_path).await;

    let cancel = ScanCancel::default();
    cancel.cancel();

    let mut emitted = 0usize;
    let result = reader
        .stream_all_partitions_cancellable(&cancel, None, |_row| {
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
