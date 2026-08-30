//! Issue #2820 — BATCH the k-way merge egress fan-in: stop one `sync_channel`
//! message per merged ROW.
//!
//! Phase-0 profiling of the single-stream Flight scan measured the per-row
//! producer→consumer hand-off at **49.9% of single-stream CPU, ~94% of it kernel
//! park/wake**. `from_readers::forward_row` now accumulates rows into a `Vec` and
//! sends one message per batch (`merge::egress_batch`), and the consuming adapter
//! holds that batch and hands out one entry per `next()`.
//!
//! These oracles pin, END TO END through the production `KWayMerger` over a REAL
//! flushed `nb` SSTable:
//!
//!   1. **Send-count reduction (AC-1)** — the number of channel MESSAGES a merge
//!      sends is the exact ramp+batch count for the rows it sent, not one per row.
//!      The per-row baseline needs no surviving per-row code path to compare
//!      against: the pre-#2820 producer sent exactly one message per DATA entry,
//!      so `entries_sent` *is* what `messages_sent` would have read then.
//!   2. **Per-batch cap** — no batch exceeds `BATCH_EMIT_ROWS_MERGE`, and a run
//!      long enough to saturate the ramp does produce full batches (so the
//!      reduction is not an artefact of a tiny fixture).
//!   3. **Content + order parity** — the merged stream reproduces, row for row and
//!      value for value in order, an INDEPENDENT direct
//!      `stream_all_partitions_for_compaction` walk of the same SSTable that never
//!      touches a channel.
//!   4. **Sub-batch tail (first-row latency)** — a run of fewer rows than one
//!      batch still delivers every row, in one message per the ramp, in ROWS not
//!      wall-clock (a wall-clock threshold in a correctness test is a mechanized
//!      `--lite` lint failure, #2642).
//!
//! Self-contained: the fixture is flushed by `WriteEngine` into a `TempDir`, so no
//! dataset corpus is required and there is no path on which this passes with zero
//! rows (every oracle asserts a non-vacuous row count).
//!
//!   cargo test -p cqlite-core --features write-support --test issue_2820_merge_fanin_batch

#![cfg(feature = "write-support")]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};

use cqlite_core::platform::Platform;
use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::scan_cancel::ScanCancel;
use cqlite_core::storage::sstable::reader::{CompactionRow, SSTableReader};
use cqlite_core::storage::write_engine::merge::{
    merge_egress_batch_probe, EgressBatchProbe, MergeStep, RowData,
};
use cqlite_core::storage::write_engine::{
    CellOperation, KWayMerger, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use cqlite_core::Config;
use tempfile::TempDir;

/// The egress fan-in counters are PROCESS-GLOBAL (like `work_counters` and the
/// #2765 active-merge hook), so the two tests in this binary that read deltas
/// around a merge must not overlap. A poisoned lock (a panicking sibling) is
/// recovered so one failure never cascades.
static COUNTER_LOCK: Mutex<()> = Mutex::new(());

fn counter_lock() -> MutexGuard<'static, ()> {
    COUNTER_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn schema() -> TableSchema {
    TableSchema {
        keyspace: "fanin_ks".to_string(),
        table: "items".to_string(),
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
                name: "val".to_string(),
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

fn write_row(id: i32) -> Mutation {
    Mutation::new(
        TableId::new("fanin_ks", "items"),
        PartitionKey::single("id", Value::Integer(id)),
        None,
        vec![CellOperation::Write {
            column: "val".to_string(),
            value: Value::text(format!("v-{id}")),
        }],
        1_000_000 + id as i64,
        None,
    )
}

fn collect_data_db(dir: &std::path::Path, out: &mut Vec<PathBuf>, depth: usize) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = path.file_name().unwrap_or_default().to_string_lossy().to_string();
        if name.ends_with("-Data.db") {
            out.push(path);
        } else if depth > 0 && path.is_dir() {
            collect_data_db(&path, out, depth - 1);
        }
    }
}

/// Flush ONE real `nb` SSTable holding `rows` single-row partitions.
///
/// ONE input on purpose: the send-count oracle asserts the EXACT message count
/// for a run, and that count is per SOURCE CHANNEL — with `K` sources the total
/// depends on how the rows split across them, which is not what is under test.
fn flush_one_sstable(rows: i32) -> (TempDir, PathBuf, TableSchema) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime");
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("inputs");
    let schema = schema();
    let config = WriteEngineConfig::new(data_dir.clone(), temp.path().join("wal"), schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine");
    for id in 0..rows {
        engine.write(write_row(id)).expect("write row");
    }
    rt.block_on(engine.flush()).expect("flush").expect("flush info");
    rt.block_on(engine.close()).expect("close engine");

    let mut found = Vec::new();
    collect_data_db(&data_dir, &mut found, 8);
    assert_eq!(
        found.len(),
        1,
        "the fixture must be exactly one flushed SSTable, got {found:?}"
    );
    (temp, found.remove(0), schema)
}

/// One comparable row: partition key bytes + the `val` cell's text.
type Row = (Vec<u8>, Option<String>);

/// A stable textual rendering of a `val` cell. `Debug` on the `Value` rather than
/// a text-only unwrap, so a tombstone/blob/absent cell is COMPARED (and reported)
/// instead of silently collapsing to `None` on both sides.
fn render(value: &Value) -> String {
    format!("{value:?}")
}

fn cell_text(cells: &[cqlite_core::storage::write_engine::merge::CellData]) -> Option<String> {
    cells
        .iter()
        .find(|c| c.column == "val")
        .map(|c| render(&c.value))
}

/// Drain a merge over `path` through the production `KWayMerger`, i.e. THROUGH the
/// batched egress fan-in. Returns the rows in emitted order.
fn merged_rows(path: &std::path::Path, schema: &TableSchema) -> Vec<Row> {
    let mut merger =
        KWayMerger::new(vec![path.to_path_buf()], schema).expect("KWayMerger::new");
    let mut out = Vec::new();
    while let MergeStep::Partition { key, rows } = merger.step().expect("merge step") {
        for entry in rows {
            let text = match &entry.row_data {
                RowData::Live { cells } => cell_text(cells),
                other => Some(format!("{other:?}")),
            };
            out.push((key.key.clone(), text));
        }
    }
    out
}

/// Drain the SAME SSTable through a DIRECT `stream_all_partitions_for_compaction`
/// walk — no producer thread, no channel, no batching. The independent oracle for
/// content + order (the merge's own output cannot be its own oracle).
fn direct_walk_rows(path: &std::path::Path, schema: &TableSchema) -> Vec<Row> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("oracle runtime");
    rt.block_on(async {
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.expect("platform"));
        let reader = SSTableReader::open(path, &config, platform)
            .await
            .expect("open reader");
        let mut out = Vec::new();
        reader
            .stream_all_partitions_for_compaction(
                Some(schema),
                &ScanCancel::default(),
                |row: CompactionRow| {
                    out.push((row.key.as_bytes().to_vec(), compaction_cell_text(&row)));
                    Ok(std::ops::ControlFlow::Continue(()))
                },
            )
            .await
            .expect("direct compaction walk");
        out
    })
}

fn compaction_cell_text(row: &CompactionRow) -> Option<String> {
    use cqlite_core::storage::sstable::reader::CompactionRowData;
    match &row.row_data {
        CompactionRowData::Live { simple, .. } => simple
            .iter()
            .find(|c| c.column == "val")
            .map(|c| render(&c.value)),
        other => Some(format!("{other:?}")),
    }
}

/// Rows enough to SATURATE the ramp several times over, derived from the shipped
/// batch size rather than a literal (a literal re-rots the moment the constant
/// moves).
fn rows_that_saturate_the_ramp(probe: &EgressBatchProbe) -> i32 {
    (probe.batch_emit_rows * 3 + 7) as i32
}

/// Oracles 1–3: one message per BATCH (exact), the per-batch cap, and row/value/
/// order parity against an independent unbatched walk.
#[test]
fn the_merge_fan_in_sends_one_message_per_batch_and_loses_no_row() {
    let _serial = counter_lock();
    let probe = merge_egress_batch_probe();
    let rows = rows_that_saturate_the_ramp(&probe);
    let (_temp, path, schema) = flush_one_sstable(rows);

    let oracle = direct_walk_rows(&path, &schema);
    assert!(
        oracle.len() >= 8,
        "fixture must hold several rows to exercise batching; got {}",
        oracle.len()
    );

    let before = merge_egress_batch_probe();
    let merged = merged_rows(&path, &schema);
    let after = merge_egress_batch_probe();

    // ---- Oracle 3: content + order parity (assert BEFORE the counts, so a
    // correctness failure is never reported as a mere count mismatch).
    assert_eq!(
        merged.len(),
        oracle.len(),
        "the batched merge must emit exactly the rows an unbatched direct walk sees"
    );
    for (i, (got, want)) in merged.iter().zip(oracle.iter()).enumerate() {
        assert_eq!(got.0, want.0, "row {i}: partition key mismatch batched-vs-direct");
        assert_eq!(got.1, want.1, "row {i}: value mismatch batched-vs-direct");
    }
    assert_eq!(
        merged.len(),
        rows as usize,
        "every written row must survive the merge"
    );

    // ---- Oracle 1: send-count reduction.
    let entries = after.entries_sent - before.entries_sent;
    let messages = after.messages_sent - before.messages_sent;
    assert_eq!(
        entries, rows as u64,
        "the producer must have sent one DATA entry per row (got {entries} for {rows})"
    );
    assert_eq!(
        messages,
        after.expected_messages(entries),
        "a run of {entries} entries must cost exactly the ramp+batch message count \
         ({}), not one message per row — the pre-#2820 producer sent {entries}",
        after.expected_messages(entries)
    );
    // Stated as the reduction it is: the per-row baseline IS `entries`.
    assert!(
        messages * 8 < entries,
        "batching must eliminate the overwhelming majority of channel sends: \
         per-row baseline {entries}, batched {messages}"
    );

    // ---- Oracle 2: per-batch cap, and real full batches (non-vacuity).
    assert!(
        after.peak_batch_rows <= after.batch_emit_rows,
        "no batch ({}) may exceed BATCH_EMIT_ROWS_MERGE ({})",
        after.peak_batch_rows,
        after.batch_emit_rows
    );
    assert!(
        entries / messages > 1,
        "the average batch must carry more than one row (entries={entries}, \
         messages={messages})"
    );
    assert_eq!(
        after.peak_batch_rows, after.batch_emit_rows,
        "a {rows}-row run saturates the ramp, so some batch must be FULL — a peak \
         below the cap would mean the reduction came from a short fixture"
    );
}

/// Oracle 4: a SUB-BATCH run delivers every row, and pays only the ramp's message
/// count for them (issue #2820 design item 7). Rows-based, never wall-clock: the
/// property is "nothing waits for a full batch", and the ramp's first flush is one
/// row, so a 3-row merge costs at most 3 messages and yields all 3 rows.
#[test]
fn a_sub_batch_merge_delivers_every_row_without_waiting_for_a_full_batch() {
    let _serial = counter_lock();
    const ROWS: i32 = 3;
    let (_temp, path, schema) = flush_one_sstable(ROWS);

    let before = merge_egress_batch_probe();
    let merged = merged_rows(&path, &schema);
    let after = merge_egress_batch_probe();

    assert_eq!(
        merged.len(),
        ROWS as usize,
        "a sub-batch result set must be delivered in FULL — pure batching (no tail \
         flush before the terminator) would strand these rows in the producer"
    );
    let oracle = direct_walk_rows(&path, &schema);
    assert_eq!(merged, oracle, "sub-batch content + order parity");

    let entries = after.entries_sent - before.entries_sent;
    let messages = after.messages_sent - before.messages_sent;
    assert_eq!(entries, ROWS as u64);
    assert_eq!(
        messages,
        after.expected_messages(entries),
        "a {ROWS}-row run costs the ramp's message count (1-row then 2-row batch), \
         so the FIRST row is never delayed by batching"
    );
    assert!(
        messages <= ROWS as u64,
        "a sub-batch run must never cost MORE messages than rows ({messages} > {ROWS})"
    );
}
