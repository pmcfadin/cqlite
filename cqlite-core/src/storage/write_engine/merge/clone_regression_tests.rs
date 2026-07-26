//! Issue #1664 — kill the `MergeEntry` double clone in the k-way merge core.
//!
//! `KWayMerger::step` used to push `entry.clone()` into `partition_rows` while
//! already holding the owned popped entry, and `KWayMerger::refill_heap` used
//! to `peek()` + `clone()` the front of a run's buffer and then `advance()`
//! past it (discarding the just-cloned original) instead of moving the owned
//! entry `advance()` already returns. This module is a sibling file (not
//! inline in `merge/mod.rs`, per the #1116 campsite rule) housing the
//! regression guard that proves the fix: drive a full `KWayMerger` compaction
//! inside a [`MergeEntryCloneScope`](crate::storage::sstable::work_counters::merge_entry_clone_scope::MergeEntryCloneScope)
//! and assert the observed clone count stays low.

use super::{KWayMerger, MergeEntry, MergeStep, RowData, RunReader, SSTableRowIterator};
use crate::error::Result;
use crate::schema::{KeyColumn, TableSchema};
use crate::storage::sstable::work_counters::merge_entry_clone_scope::MergeEntryCloneScope;
use crate::storage::write_engine::merge::CellData;
use crate::storage::write_engine::mutation::{DecoratedKey, PartitionKey};
use crate::types::Value;
use std::collections::{BinaryHeap, HashMap};

/// Single-column `int` partition-key schema with one regular `name` column —
/// deliberately minimal, mirroring `issue_886_empty_partition_skip::schema`.
fn schema() -> TableSchema {
    TableSchema {
        keyspace: "i1664".to_string(),
        table: "clone_regression".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![crate::schema::Column {
            name: "name".to_string(),
            data_type: "text".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        }],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// Valid on-disk partition-key bytes for `id = n` under [`schema`], built
/// through the shared codec so the merge stream can decode them.
fn pk_bytes(schema: &TableSchema, n: i32) -> Vec<u8> {
    PartitionKey::single("id", Value::Integer(n))
        .to_bytes(schema)
        .expect("encode int partition key")
}

/// An in-memory run yielding a fixed list of `MergeEntry`s in order.
struct VecIterator(std::vec::IntoIter<MergeEntry>);
impl SSTableRowIterator for VecIterator {
    fn next(&mut self) -> Option<Result<MergeEntry>> {
        self.0.next().map(Ok)
    }
}

/// Build a `KWayMerger` over K in-memory runs (one `RunReader` each).
fn merger_over_runs(runs: Vec<Vec<MergeEntry>>, schema: TableSchema) -> KWayMerger {
    let runs = runs
        .into_iter()
        .map(|entries| RunReader::new(Box::new(VecIterator(entries.into_iter())) as _))
        .collect();
    KWayMerger {
        runs,
        heap: BinaryHeap::new(),
        current_partition: None,
        gc_before_secs: None,
        now_secs: None,
        purge_safe: false,
        max_purgeable_timestamp: None,
        schema_arc: std::sync::Arc::new(schema.clone()),
        schema,
        _egress_slot: None,
    }
}

/// Regression guard for issue #1664 (kill the `MergeEntry` double clone in
/// the k-way merge core). Drives a full `KWayMerger` compaction over K=3
/// runs of distinct single-row partitions (N total rows) inside a
/// `MergeEntryCloneScope` and asserts the `MergeEntry::clone` count stays
/// under a threshold that only the post-fix code (owned-move at both the
/// `step` push and the `refill_heap` reload) can meet.
///
/// Empirically observed on this K/N (N=15, see the constants below):
// main today: 30 (== 2N, the two gratuitous clones); post-fix: 0
// (reconcile clones nothing for these single-row partitions).
#[test]
fn kway_merge_does_not_double_clone_entries() {
    const PER_RUN: usize = 5;
    const K: usize = 3;
    const N: u64 = (PER_RUN * K) as u64;

    let schema = schema();

    // K runs, each with PER_RUN distinct single-row partitions. Tokens are
    // globally distinct and ascending WITHIN each run (RunReaders must yield
    // ascending `MergeEntry` order), so every partition is exactly one row.
    let runs: Vec<Vec<MergeEntry>> = (0..K)
        .map(|r| {
            (0..PER_RUN)
                .map(|i| {
                    let token = (r * PER_RUN + i) as i64;
                    MergeEntry::new(
                        r,
                        DecoratedKey::new(token, pk_bytes(&schema, token as i32)),
                        None,
                        100,
                        RowData::Live {
                            cells: vec![CellData::new("name".to_string(), Value::text("v"), 100)],
                        },
                    )
                })
                .collect()
        })
        .collect();

    let mut merger = merger_over_runs(runs, schema);

    let scope = MergeEntryCloneScope::new();
    let mut partitions = 0u64;
    loop {
        match merger.step().expect("merge step must not fail") {
            MergeStep::Complete => break,
            MergeStep::Partition { .. } => partitions += 1,
        }
    }
    let clones = scope.count();
    drop(scope);

    assert_eq!(
        partitions, N,
        "every distinct key is its own single-row partition"
    );

    // The two removed gratuitous clones cost exactly 2N combined (the
    // `step` push + the `refill_heap` reload); reconcile clones nothing for
    // these single-row partitions. Threshold (N + N/2 = 22 for N=15) sits
    // strictly between the observed post-fix count (0) and the pre-fix
    // count (2N = 30): red on main, green post-fix.
    let threshold = N + N / 2; // 22 for N=15
    assert!(
        clones <= threshold,
        "MergeEntry cloned {clones} times for {N} rows (threshold {threshold}); \
         the #1664 double clone in step/refill_heap regressed"
    );
}
