//! Issue #2361 — dropping a `KWayMerger` must JOIN its producer threads (not
//! detach them), and that join must be BOUNDED even when a producer is blocked
//! on a full `SyncSender::send`.
//!
//! The regression this guards: a producer that has filled the bounded channel
//! (`STREAMING_CHANNEL_CAPACITY`) is parked inside `send`. A naive
//! join-on-drop that did NOT first close the channel would DEADLOCK — the join
//! waits for a thread that is itself waiting for channel space that will never
//! come. `SSTableRowIteratorAdapter::drop` therefore drops the receiver (waking
//! the blocked send with `Err`) and trips `scan_cancel` BEFORE joining. If this
//! test hangs, that teardown ordering regressed. It references
//! `new_cancellable_with_partition_limit`, which does not exist on pre-#2361
//! `main` (compile-red there).

use super::KWayMerger;
use crate::schema::{Column, KeyColumn, TableSchema};
use crate::storage::scan_cancel::ScanCancel;
use crate::storage::write_engine::mutation::{CellOperation, Mutation, PartitionKey, TableId};
use crate::types::Value;
use std::collections::HashMap;
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

/// Write `n` single-row partitions to one uncompressed SSTable; return the temp
/// dir (keep alive) and the Data.db path.
fn write_fixture(n: i32) -> (TempDir, std::path::PathBuf) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
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
    let info = rt.block_on(writer.finish()).unwrap();
    let data_path = info.data_path.clone();
    (temp, data_path)
}

/// Dropping a merger whose producer is BLOCKED on a full channel must not
/// deadlock (issue #2361). The fixture holds far more partitions than
/// `STREAMING_CHANNEL_CAPACITY` (256), and the test NEVER steps the merger, so
/// the producer fills the channel and parks in `send`. The drop at end of scope
/// must close the channel and join the producer without hanging — reaching the
/// final assertion IS the proof (a regressed teardown would hang here).
#[test]
fn dropping_backpressured_merger_does_not_deadlock() {
    // > STREAMING_CHANNEL_CAPACITY so the single producer blocks on send.
    let (_temp, data_path) = write_fixture(400);
    let schema = schema();

    let merger = KWayMerger::new_cancellable_with_partition_limit(
        vec![data_path],
        &schema,
        ScanCancel::default(),
        Some(2),
    )
    .expect("merger constructs");
    // Intentionally do NOT step: let the producer fill the bounded channel and
    // park in `send`, then drop the merger (join-on-drop teardown). RETURNING from
    // this test (no hang) is the proof — a regressed, deadlocking teardown would
    // never reach the end of the function.
    drop(merger);
}

/// A merger dropped mid-consumption (after a few steps) also tears down cleanly:
/// tripping the shared cancel then dropping must join promptly (issue #2361).
#[test]
fn dropping_merger_after_partial_consumption_joins() {
    let (_temp, data_path) = write_fixture(400);
    let schema = schema();
    let cancel = ScanCancel::default();

    let mut merger = KWayMerger::new_cancellable_with_partition_limit(
        vec![data_path],
        &schema,
        cancel.clone(),
        Some(1000),
    )
    .expect("merger constructs");

    // Consume a couple of partitions so the producer is actively streaming.
    let _ = merger.step().expect("first step");
    let _ = merger.step().expect("second step");

    // Client-disconnect analogue: trip cancel, then drop. The drop must join the
    // producer without hanging (reaching the end IS the proof).
    cancel.cancel();
    drop(merger);
}
