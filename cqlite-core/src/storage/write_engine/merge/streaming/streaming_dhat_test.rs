//! Memory-bound regression test for issue #1668 (within-partition streaming
//! merge) — proves the MERGE LAYER'S OWN peak-heap bound, isolated from the
//! SSTable reader.
//!
//! **Why isolated from the reader**: an earlier version of this proof drove
//! the real production path end to end (`WriteEngine::maintenance_step`
//! reading real, uncompressed `Data.db` files) and measured 424 MiB against
//! the 128 MiB budget. Root-causing that showed the overshoot was NOT this
//! merge layer: `stream_all_partitions_for_compaction`
//! (`storage/sstable/reader/data_access/compaction.rs`) only takes its
//! sliding-window streaming path when `requires_chunk_stitching()` is true —
//! true ONLY for the legacy `V5CompressedLegacy` chunked-compression format.
//! `V5_0Uncompressed` — the ONLY format CQLite's own production write surface
//! ever emits (issue #1406's claim boundary: uncompressed SSTables only) —
//! falls back to `iterate_all_partitions_for_compaction`, which fully
//! materializes the decompressed data section before parsing anything. That
//! reader-side gap is tracked as its own follow-up issue; it is orthogonal to
//! (and pre-dates) this merge-layer's own reconciliation logic.
//!
//! This test therefore bypasses the reader entirely: a hand-written
//! [`LazyWideRun`] implements [`SSTableRowIterator`] by generating each row's
//! payload ON DEMAND inside `next()` — nothing is pre-built into a `Vec`, so
//! there is no way for this fixture ITSELF to hide a whole-partition buffer.
//! Because [`StreamingMerger`]/`KWayMerger` are `pub(crate)` (not part of the
//! public API — the whole point of issue #1668's merge-layer scope), this
//! must be an in-tree `#[cfg(test)]` module (a `tests/` integration test
//! cannot reach them at all); see `lib.rs`'s `DHAT_TEST_ALLOC` for why the
//! dhat global allocator is installed there instead of in this file.
//!
//! Run via (excludes `state_machine`, which claims the SAME global-allocator
//! slot for its own allocation-counting probe):
//!
//! ```text
//! cargo test --package cqlite-core --no-default-features \
//!   --features write-support,dhat-heap --lib --profile bench \
//!   -- storage::write_engine::merge::streaming::streaming_dhat_test --nocapture
//! ```

use super::super::model::CellData;
use super::super::{KWayMerger, RunReader, SSTableRowIterator};
use super::*;
use crate::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use crate::types::Value;
use std::collections::{BinaryHeap, HashMap};

const HEAP_BUDGET_BYTES: usize = 128 * 1024 * 1024;

// Workload sizing: ONE partition, split across RUN_COUNT runs by disjoint
// clustering-key ranges (so the k-way merge heap genuinely interleaves
// several runs into a single wide output partition — not a trivial
// single-run pass-through), ROWS_PER_RUN rows each, PAYLOAD_BYTES payload
// per row. 4 runs x 5,000 rows x 64 KiB = ~1.22 GiB of combined row content
// in ONE partition if it were ever fully resident at once — an order of
// magnitude past the 128 MiB budget, so staying under budget is a genuine
// proof, not a vacuous pass on a fixture too small to matter.
const PAYLOAD_BYTES: usize = 64 * 1024;
const ROWS_PER_RUN: i32 = 5_000;
const RUN_COUNT: i32 = 4;

fn payload_for(ck: i32) -> String {
    let mut s = format!("row-{ck:08}-");
    s.push_str(&"abcdefghij".repeat((PAYLOAD_BYTES.saturating_sub(s.len())) / 10 + 1));
    s.truncate(PAYLOAD_BYTES);
    s
}

fn wide_partition_schema() -> TableSchema {
    TableSchema {
        keyspace: "issue1668_mem_ks".to_string(),
        table: "wide_partition".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
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
                name: "ck".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "payload".to_string(),
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

/// Lazily generates ONE run's slice of a wide partition's rows, ON DEMAND —
/// `next()` builds exactly one row's payload per call and returns
/// immediately; nothing is ever materialized ahead of time into a `Vec` (the
/// property this test needs to rule itself out as the source of any
/// buffering it might observe).
struct LazyWideRun {
    run_index: usize,
    key: DecoratedKey,
    next_ck: i32,
    end_ck_exclusive: i32,
}

impl SSTableRowIterator for LazyWideRun {
    fn next(&mut self) -> Option<Result<MergeEntry>> {
        if self.next_ck >= self.end_ck_exclusive {
            return None;
        }
        let ck = self.next_ck;
        self.next_ck += 1;
        let ts = 1_000_000_000 + i64::from(ck);
        Some(Ok(MergeEntry::new(
            self.run_index,
            self.key.clone(),
            Some(ClusteringKey::single("ck", Value::Integer(ck))),
            ts,
            RowData::Live {
                cells: vec![CellData::new(
                    "payload".to_string(),
                    Value::Text(payload_for(ck)),
                    ts,
                )],
            },
        )))
    }
}

/// Build a `KWayMerger` over `RUN_COUNT` [`LazyWideRun`]s, each covering a
/// disjoint `[base, base + ROWS_PER_RUN)` clustering-key range of the SAME
/// partition (`id = 1`) — mirroring `streaming.rs`'s own `merger_over_runs`
/// test helper (not reused directly: that one takes pre-built
/// `Vec<MergeEntry>` runs, defeating the whole point of a LAZY source here).
fn lazy_merger_over_wide_partition(schema: TableSchema) -> KWayMerger {
    let key = DecoratedKey::new(1, vec![1]);
    let runs: Vec<RunReader> = (0..RUN_COUNT)
        .map(|run_index| {
            let base = run_index * ROWS_PER_RUN;
            RunReader::new(Box::new(LazyWideRun {
                run_index: run_index as usize,
                key: key.clone(),
                next_ck: base,
                end_ck_exclusive: base + ROWS_PER_RUN,
            }))
        })
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
    }
}

#[test]
fn streaming_merge_of_lazy_wide_partition_stays_within_heap_budget() {
    let _profiler = dhat::Profiler::builder().testing().build();

    let schema = wide_partition_schema();
    let mut merger = lazy_merger_over_wide_partition(schema);
    let mut stream = StreamingMerger::new(&mut merger);

    // Drain every increment, dropping each reconciled row IMMEDIATELY after
    // observing it — an in-tree streaming consumer never accumulates output
    // either, so peak heap here measures ONLY what `StreamingMerger` itself
    // holds mid-reconciliation, not an accumulating drain buffer.
    let mut rows_seen: u64 = 0;
    let mut partitions_seen: u64 = 0;
    loop {
        match stream.step_streaming().expect("step_streaming must not error") {
            StreamingStep::ClusterGroup { row, .. } => {
                assert!(
                    row.clustering_key.is_some(),
                    "no carriers in this fixture — every row is a real clustering row"
                );
                rows_seen += 1;
                drop(row);
            }
            StreamingStep::PartitionEnd { .. } => partitions_seen += 1,
            StreamingStep::Complete => break,
        }
    }

    let stats = dhat::HeapStats::get();
    eprintln!(
        "Issue #1668 merge-layer-only streaming: {RUN_COUNT} runs x {ROWS_PER_RUN} rows x \
         {} KiB payload, ONE partition, peak heap {} bytes ({:.2} MiB) vs budget {} MiB",
        PAYLOAD_BYTES / 1024,
        stats.max_bytes,
        stats.max_bytes as f64 / (1024.0 * 1024.0),
        HEAP_BUDGET_BYTES / (1024 * 1024),
    );

    // Not a vacuous pass: every row from every run was reconciled into the
    // SAME single partition (correctness), and the total logical payload
    // volume (~1.22 GiB) is an order of magnitude over budget — so staying
    // under budget is a real property of the streaming path, not an
    // accident of a too-small fixture.
    assert_eq!(partitions_seen, 1, "all runs share ONE partition key");
    assert_eq!(
        rows_seen,
        (ROWS_PER_RUN as u64) * (RUN_COUNT as u64),
        "every row across every run must be reconciled exactly once"
    );

    assert!(
        stats.max_bytes <= HEAP_BUDGET_BYTES,
        "Issue #1668: merge-layer-only streaming peak heap {} bytes ({:.2} MiB) exceeds the \
         {} MiB budget — the merge regressed to whole-partition buffering",
        stats.max_bytes,
        stats.max_bytes as f64 / (1024.0 * 1024.0),
        HEAP_BUDGET_BYTES / (1024 * 1024),
    );
}
