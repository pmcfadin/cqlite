//! Issue #2264 — the compaction streaming scan must observe a cooperative
//! cancel token and abandon a multi-partition Data.db WITHOUT running to
//! completion.
//!
//! World 2 (field-proven): an index-less (Summary.db-absent) SSTable is handed
//! to `stream_all_partitions_for_compaction` as one contiguous block whose
//! parser loops over EVERY partition in a single uninterruptible pass on a
//! detached producer thread. A cancelled Flight `do_get` could not stop it — the
//! merge's between-step poll never runs because it is parked waiting for the
//! producer, and PR #2282's channel race never reaches the CPU-bound loop.
//!
//! These tests drive the reader scan DIRECTLY (no `KWayMerger`, so the merge's
//! own between-step poll is absent) — the reader's `scan_cancel` poll is the
//! ONLY thing that can abort the walk here, so a green test proves THIS fix, not
//! the pre-existing between-step check.

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

/// Write `n` single-row partitions to a fresh uncompressed SSTable, then strip
/// the `Summary.db`/`Index.db`/`Filter.db` sidecars to reproduce the field's
/// index-less snapshot (only Data.db + Statistics.db remain). Returns the temp
/// dir (keep alive) and the `Data.db` path.
async fn index_less_fixture(n: i32) -> (TempDir, std::path::PathBuf) {
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
    SSTableReader::open(data_path, &config, platform).await.unwrap()
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
/// World-2 analogue of PR #2282's channel test. Proves the CPU-bound loop itself
/// polls the token, not just the boundaries. FAILS on pre-fix code (no poll →
/// runs to completion, `count == TOTAL`, `Ok`).
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
