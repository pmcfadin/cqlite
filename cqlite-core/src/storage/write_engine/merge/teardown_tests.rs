//! Issue #2361 — dropping a `KWayMerger` must JOIN its producer threads (not
//! detach them), and that join must be BOUNDED even when a producer is blocked
//! on a full `SyncSender::send`.
//!
//! The regression this guards: a producer that has filled the bounded channel is
//! parked inside `send`. Since issue #2820 that channel is bounded in MESSAGES
//! (batches) converted from the `STREAMING_CHANNEL_CAPACITY` ROW budget, and a
//! cold producer fills it with `egress_batch::rows_in_full_channel` rows — far
//! fewer than the pre-batching 256, because the batch-size ramp starts at one row.
//! Every fixture below is sized from that function rather than from a literal, so
//! the "the producer is genuinely parked" premise cannot silently rot. A naive
//! join-on-drop that did NOT first close the channel would DEADLOCK — the join
//! waits for a thread that is itself waiting for channel space that will never
//! come. `SSTableRowIteratorAdapter::drop` therefore drops the receiver (waking
//! the blocked send with `Err`) and trips `scan_cancel` BEFORE joining. If this
//! test hangs, that teardown ordering regressed.
//!
//! No producer-side `LIMIT` budget exists (removed in roborev round 2 — see
//! `stream_all_partitions_cancellable`'s doc): `new_cancellable` never lets a
//! producer exit early on its own, so the fixture (more partitions than a full
//! channel can hold, unstepped) is what forces the
//! producer to genuinely park in `send` — the scenario this test exists to
//! prove doesn't deadlock. This is also the "doesn't scan a 1.13M-partition
//! table to completion" proof: since the channel is never drained, the producer
//! structurally cannot proceed past its backpressure point before teardown.

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
            value: Value::text(format!("v{id}")),
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

/// Rows a fixture must hold for its single producer to genuinely PARK in `send`,
/// derived from the shipped constants (issue #2820): everything a full channel
/// holds from a cold start, plus the full batch the producer then blocks trying to
/// hand over, plus one row it cannot even accumulate. A hard-coded literal would
/// re-rot the moment `BATCH_EMIT_ROWS_MERGE`, the ramp or the row budget moves.
fn rows_that_park_the_producer() -> i32 {
    let probe = super::merge_egress_batch_probe();
    let rows_cap = super::STREAMING_CHANNEL_CAPACITY;
    let needed = probe.rows_in_full_channel(rows_cap) + probe.batch_emit_rows + 1;
    // Keep the historical 400-partition floor so the fixture also stays a
    // multi-partition scan (the "doesn't run to completion" half of the proof).
    needed.max(400) as i32
}

/// Dropping a merger whose producer is BLOCKED on a full channel must not
/// deadlock (issue #2361). The fixture holds more partitions than a full channel
/// plus one in-flight batch can absorb (see [`rows_that_park_the_producer`]), and
/// the test NEVER steps the merger, so the producer fills the channel and parks in
/// `send`. The drop at end of scope must close the channel and join the producer
/// without hanging — reaching the final assertion IS the proof (a regressed
/// teardown would hang here).
#[test]
fn dropping_backpressured_merger_does_not_deadlock() {
    let (_temp, data_path) = write_fixture(rows_that_park_the_producer());
    let schema = schema();

    let merger = KWayMerger::new_cancellable(vec![data_path], &schema, ScanCancel::default())
        .expect("merger constructs");
    // Intentionally do NOT step: with no producer-side budget, the ONLY thing
    // that can stop the producer from filling the channel is genuine
    // backpressure — it parks in `send` once the channel (a bounded number of
    // BATCHES since issue #2820) plus its one in-flight batch are full, well short
    // of the fixture's partition count.
    // Dropping the merger here (join-on-drop teardown) must not hang; RETURNING
    // from this test (no hang) is the proof — a regressed, deadlocking teardown
    // would never reach the end of the function.
    drop(merger);
}

/// A merger dropped mid-consumption (after a few steps) also tears down cleanly:
/// tripping the shared cancel then dropping must join promptly (issue #2361).
#[test]
fn dropping_merger_after_partial_consumption_joins() {
    let (_temp, data_path) = write_fixture(rows_that_park_the_producer());
    let schema = schema();
    let cancel = ScanCancel::default();

    let mut merger = KWayMerger::new_cancellable(vec![data_path], &schema, cancel.clone())
        .expect("merger constructs");

    // Consume a couple of partitions so the producer is actively streaming.
    let _ = merger.step().expect("first step");
    let _ = merger.step().expect("second step");

    // Client-disconnect analogue: trip cancel, then drop. The drop must join the
    // producer without hanging (reaching the end IS the proof).
    cancel.cancel();
    drop(merger);
}

/// Issue #2820 design item 5: a CANCEL must win over a HELD, partially-drained
/// batch — a state that did not exist before batching.
///
/// Before this change the consumer held nothing between `next()` calls, so the
/// top-of-loop cancel check was reached on every poll by construction. Now a
/// batch of `n` rows is pulled off the channel and handed out one entry at a
/// time, which introduces a way to keep serving rows from memory while the scan
/// is being torn down — delaying the very teardown the cancel exists to start
/// (issue #2361), and, on the read path, serving rows for a `do_get` the client
/// has already abandoned.
///
/// Non-vacuous BY CONSTRUCTION, in two ways: the held batch is asserted
/// NON-EMPTY at the moment the cancel is tripped (so there really are buffered
/// rows the adapter could have served), and a CONTROL adapter — identical but
/// uncancelled — is asserted to serve a row at that same poll (so the `Cancelled`
/// verdict is attributable to the cancel, not to an exhausted run).
#[test]
fn a_cancel_wins_over_a_partially_drained_held_batch() {
    use crate::storage::write_engine::merge::producer_iter::SSTableRowIteratorAdapter;
    use crate::storage::write_engine::merge::SSTableRowIterator;

    // Enough rows that the ramp's second batch (2 rows) is complete, so exactly
    // one entry is left HELD after two polls.
    let (_temp, data_path) = write_fixture(64);
    let schema = schema();
    let rows_cap = super::STREAMING_CHANNEL_CAPACITY;

    let open = |cancel: ScanCancel| {
        SSTableRowIteratorAdapter::open(
            &data_path,
            0,
            &schema,
            None,
            cancel,
            rows_cap,
            crate::storage::sstable::reader::OpenErrorReporting::SelfReported,
        )
        .expect("adapter opens")
    };

    // CONTROL: an uncancelled adapter serves a row at the third poll, so the
    // cancelled arm's `Cancelled` cannot be an exhausted-run artefact.
    let mut control = open(ScanCancel::default());
    for poll in 0..3 {
        match control.next() {
            Some(Ok(_)) => {}
            other => panic!("control poll {poll} must yield a row, got {other:?}"),
        }
    }
    drop(control);

    let cancel = ScanCancel::default();
    let mut adapter = open(cancel.clone());
    for poll in 0..2 {
        match adapter.next() {
            Some(Ok(_)) => {}
            other => panic!("poll {poll} must yield a row, got {other:?}"),
        }
    }
    assert!(
        adapter.held.len() > 0,
        "test precondition: the adapter must be HOLDING undelivered entries of a \
         partially-drained batch, or 'cancel wins over a held batch' is vacuous"
    );

    cancel.cancel();
    match adapter.next() {
        Some(Err(crate::error::Error::Cancelled)) => {}
        other => panic!(
            "a cancelled scan must report Cancelled even with buffered entries in \
             hand (issue #2820 design item 5); got {other:?}"
        ),
    }
    // STICKY by way of `ScanCancel` being set-once: a repeat poll keeps answering
    // Cancelled and never decays into a row or a clean end of input.
    match adapter.next() {
        Some(Err(crate::error::Error::Cancelled)) => {}
        other => panic!("a repeat poll must stay Cancelled; got {other:?}"),
    }
}
