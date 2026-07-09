//! Streaming cluster-group step type (issue #1668, stage 2) — FEATURE-INTERNAL,
//! UNWIRED from every production consumer.
//!
//! `maintenance.rs`, `compact_sstables`, and the Flight producer all still call
//! [`KWayMerger::step`] (the whole-partition path), which remains the sole
//! production path and is completely unchanged by this module.
//! [`StreamingMerger`] below is an ADDITIONAL, not-yet-wired wrapper that
//! proves the eventual streaming design (stage 3+) can hand a caller the SAME
//! reconciled rows one cluster-group at a time instead of one
//! `MergeStep::Partition { rows, .. }` blob per partition — without touching
//! [`MergeStep`]'s shape (a distinct [`StreamingStep`] type) or `KWayMerger`'s
//! own fields (a wrapper holding its own drain state, so none of the existing
//! `KWayMerger { .. }` struct-literal unit tests in `mod.rs` need updating).
//!
//! ## Grouping-contiguity VERIFICATION (issue #1668 flags this as "the crux")
//!
//! The increment design assumes that once a caller has moved past a
//! clustering key while draining the heap, it never sees that key again —
//! i.e. the heap pop order groups every entry for the same `(pk, ck)`
//! contiguously, so "accumulate only the entries for the current clustering
//! key, then move on" never re-opens a finished group. This DOES hold:
//! [`MergeEntry`]'s `Ord` (`model.rs`) orders primarily by
//! `(token, key bytes, clustering_key, run_index)`, and `clustering_key`
//! compares via `ClusteringKey`'s FALLBACK `Ord` (lexicographic by value) —
//! the SAME fallback `Ord`/`Eq` that `merge_partition_rows`'s
//! `clustered_rows: BTreeMap<Option<ClusteringKey>, _>` already groups by
//! today. Since `BinaryHeap::pop` yields a non-increasing (here: `Reverse`,
//! so non-decreasing) sequence under one fixed comparator, and grouping
//! identity here is that SAME comparator's notion of equality, entries for
//! one clustering key are contiguous in heap-pop order — proven directly
//! against the heap (not assumed) by the `heap_groups_contiguously_by_ord`
//! test below.
//!
//! **Residual finding (documented, NOT fixed in this stage):** `ClusteringKey`'s
//! fallback `Ord` (`mutation.rs`) is NOT schema-aware — it does not apply
//! `DESC` clustering-column reversal, and treats an absent trailing component
//! by zip-stopping rather than Cassandra's NULL-first rule (contrast
//! `ClusteringKey::compare`, which IS schema-aware and DOES apply both). So
//! while GROUPING (same-key contiguity, proven above) is safe to stream on,
//! the ORDER in which DISTINCT clustering-key groups are emitted from the
//! heap can diverge from the schema's true collation for `DESC` clustering
//! columns or absent trailing components. Today's whole-partition path masks
//! this with an explicit `merged.sort_by(schema-aware compare)` AFTER
//! collecting every cluster in a partition (`merge_partition_rows`). A future
//! stage that wires a true (non-buffering) streaming second pass to a real
//! writer must either (a) make the heap comparator schema-aware, or (b)
//! re-sort the emitted group sequence before/while writing. `step_streaming`
//! below does not need to solve this: it drains an ALREADY schema-sorted
//! `Vec<MergeEntry>` (the same one `step()` returns), so its own output order
//! is byte-identical to `step()`'s regardless of this residual — but the
//! residual applies to any FUTURE streaming pass that walks the heap directly
//! without that final sort (stage 3+ design point, not resolved here).
//!
//! `step_streaming` does NOT yet avoid whole-partition buffering: it calls
//! the unchanged [`KWayMerger::step`] (which still buffers a whole partition
//! and fully reconciles it via `merge_partition_rows`, itself now built on the
//! stage-1 `carriers::scan_partition_carriers` pre-scan) and then drains the
//! resulting `Vec<MergeEntry>` one row at a time. Removing that buffering is
//! stage 5's job; this stage proves the increment TYPE and consumer-loop
//! shape are safe to build on.

// Stage 2 (#1668) is deliberately UNWIRED: no production caller constructs a
// `StreamingMerger` yet (that is stage 3), so a normal (non-test) build sees
// every item here as unreachable. Matches the crate's existing convention for
// carried-but-not-yet-consumed surface (see `KWayMerger::gc_before_secs` /
// `now_secs` in `mod.rs`). Exercised directly by this module's own tests.
#![cfg_attr(feature = "write-support", allow(dead_code))]

#[cfg(feature = "write-support")]
use super::model::MergeEntry;
#[cfg(feature = "write-support")]
use super::{KWayMerger, MergeStep};
#[cfg(feature = "write-support")]
use crate::error::Result;
#[cfg(feature = "write-support")]
use crate::storage::write_engine::mutation::DecoratedKey;
#[cfg(feature = "write-support")]
use std::collections::VecDeque;

/// A single streaming increment from [`StreamingMerger::step_streaming`]
/// (issue #1668, stage 2). Distinct from [`MergeStep`] (unchanged, still the
/// production shape) so no existing `match`/`while let MergeStep::Partition`
/// call site anywhere in the codebase is affected by this addition.
#[cfg(feature = "write-support")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StreamingStep {
    /// One already-reconciled row belonging to `key`'s partition, in the SAME
    /// relative order `MergeStep::Partition { rows, .. }` would have held it
    /// (range/partition-tombstone carriers first, then clustering rows in
    /// schema-aware clustering order — see `merge_partition_rows`).
    ClusterGroup {
        /// Partition key this row belongs to.
        key: DecoratedKey,
        /// The reconciled row (or carrier re-emission). Boxed: `Complete` is
        /// zero-sized, so an inline `MergeEntry` here would trip
        /// `clippy::large_enum_variant` (denied crate-wide, `lib.rs`).
        row: Box<MergeEntry>,
    },
    /// No more `ClusterGroup`s remain for `key`'s partition.
    PartitionEnd {
        /// Partition key whose cluster groups are exhausted.
        key: DecoratedKey,
    },
    /// The merge is complete (no more partitions in any run).
    Complete,
}

/// Feature-internal streaming wrapper around [`KWayMerger`] (issue #1668,
/// stage 2) — NOT constructed by any production consumer today. See the
/// module doc for the grouping-contiguity proof and the DESC/absent-component
/// residual.
///
/// Holds ONLY its own drain state (`partition_key` / `pending_rows`); it does
/// not add fields to `KWayMerger` itself, so the existing `KWayMerger { .. }`
/// struct-literal unit tests in `mod.rs` are unaffected by this addition.
#[cfg(feature = "write-support")]
pub(crate) struct StreamingMerger<'a> {
    merger: &'a mut KWayMerger,
    partition_key: Option<DecoratedKey>,
    pending_rows: VecDeque<MergeEntry>,
}

#[cfg(feature = "write-support")]
impl<'a> StreamingMerger<'a> {
    /// Wrap a [`KWayMerger`] for streaming cluster-group increments.
    pub(crate) fn new(merger: &'a mut KWayMerger) -> Self {
        Self {
            merger,
            partition_key: None,
            pending_rows: VecDeque::new(),
        }
    }

    /// Yield the next streaming increment.
    ///
    /// Drains any in-progress partition's buffered rows one at a time as
    /// `ClusterGroup`, emits `PartitionEnd` once exhausted, then pulls the
    /// next whole partition from the wrapped [`KWayMerger::step`] and starts
    /// draining it. See the module doc for why this reproduces `step()`'s
    /// output exactly, row-for-row, just split into increments.
    pub(crate) fn step_streaming(&mut self) -> Result<StreamingStep> {
        if let Some(key) = self.partition_key.clone() {
            if let Some(row) = self.pending_rows.pop_front() {
                return Ok(StreamingStep::ClusterGroup {
                    key,
                    row: Box::new(row),
                });
            }
            self.partition_key = None;
            return Ok(StreamingStep::PartitionEnd { key });
        }

        match self.merger.step()? {
            MergeStep::Partition { key, rows } => {
                let mut pending: VecDeque<MergeEntry> = rows.into();
                match pending.pop_front() {
                    Some(row) => {
                        self.partition_key = Some(key.clone());
                        self.pending_rows = pending;
                        Ok(StreamingStep::ClusterGroup {
                            key,
                            row: Box::new(row),
                        })
                    }
                    // A partition with no writer-emittable content still runs
                    // through the SAME boundary the whole-partition path
                    // would traverse — mirror it with an immediate
                    // PartitionEnd rather than silently skipping the
                    // boundary.
                    None => Ok(StreamingStep::PartitionEnd { key }),
                }
            }
            MergeStep::Complete => Ok(StreamingStep::Complete),
        }
    }
}

#[cfg(all(test, feature = "write-support"))]
mod tests {
    use super::*;
    use crate::schema::{ClusteringColumn, ClusteringOrder, KeyColumn, TableSchema};
    use crate::storage::write_engine::merge::model::{CellData, RowData};
    use crate::storage::write_engine::merge::{RunReader, SSTableRowIterator};
    use crate::storage::write_engine::mutation::{ClusteringKey, RangeTombstone};
    use crate::types::Value;
    use std::cmp::Reverse;
    use std::collections::{BinaryHeap, HashMap};

    fn key(token: i64) -> DecoratedKey {
        DecoratedKey::new(token, vec![token as u8])
    }

    fn ck(v: i32) -> ClusteringKey {
        ClusteringKey::single("ck", Value::Integer(v))
    }

    fn live_entry(run_index: usize, token: i64, cluster: i32, ts: i64) -> MergeEntry {
        MergeEntry::new(
            run_index,
            key(token),
            Some(ck(cluster)),
            ts,
            RowData::Live {
                cells: vec![CellData::new("c".to_string(), Value::Integer(cluster), ts)],
            },
        )
    }

    fn range_carrier(token: i64, deletion_time: i64, ldt: i32) -> MergeEntry {
        let rt = RangeTombstone {
            start: crate::storage::write_engine::mutation::ClusteringBound::Bottom,
            end: crate::storage::write_engine::mutation::ClusteringBound::Inclusive(ck(1)),
            deletion_time,
            local_deletion_time: ldt,
        };
        MergeEntry::new(
            usize::MAX,
            key(token),
            None,
            deletion_time,
            RowData::Live { cells: Vec::new() },
        )
        .with_range_deletion(rt)
    }

    fn partition_carrier(token: i64, mfda: i64, ldt: i32) -> MergeEntry {
        MergeEntry::new(
            usize::MAX,
            key(token),
            None,
            mfda,
            RowData::Live { cells: Vec::new() },
        )
        .with_partition_deletion((mfda, ldt))
    }

    /// A single-clustering-column schema, ASC or DESC per `order`.
    fn test_schema(order: ClusteringOrder) -> TableSchema {
        TableSchema {
            keyspace: "ks_1668".to_string(),
            table: "t_1668".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![ClusteringColumn {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                position: 0,
                order,
            }],
            columns: vec![],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }

    /// Test-only `SSTableRowIterator` over a pre-supplied `Vec<MergeEntry>`,
    /// mirroring the `VecIterator` pattern already used by `mod.rs`'s own
    /// merge unit tests (kept local here so `streaming.rs`'s tests need no
    /// cross-module reuse of `mod.rs`'s private test helpers).
    struct VecIterator(std::vec::IntoIter<MergeEntry>);
    impl SSTableRowIterator for VecIterator {
        fn next(&mut self) -> Option<Result<MergeEntry>> {
            self.0.next().map(Ok)
        }
    }

    /// Build a `KWayMerger` with ONE run over `entries`, matching `mod.rs`'s
    /// `merger_over` test helper (not reusable directly here — private to a
    /// sibling module — so reconstructed with the same shape).
    fn merger_over(entries: Vec<MergeEntry>, schema: TableSchema) -> KWayMerger {
        KWayMerger {
            runs: vec![RunReader::new(Box::new(VecIterator(entries.into_iter())))],
            heap: BinaryHeap::new(),
            current_partition: None,
            gc_before_secs: None,
            now_secs: None,
            purge_safe: false,
            max_purgeable_timestamp: None,
            schema,
        }
    }

    /// The grouping-contiguity VERIFICATION test flagged by issue #1668 as
    /// "the crux": push entries for several clustering keys across several
    /// "runs" in SCRAMBLED order directly onto a `BinaryHeap<Reverse<_>>` (the
    /// exact structure `KWayMerger` uses) and assert that popping them yields
    /// every same-clustering-key entry CONTIGUOUSLY — i.e. once a distinct
    /// clustering key is seen, no earlier key ever reappears. This is a
    /// property of the min-heap invariant under `MergeEntry::Ord`, exercised
    /// directly rather than assumed.
    #[test]
    fn heap_groups_contiguously_by_ord() {
        let mut heap: BinaryHeap<Reverse<MergeEntry>> = BinaryHeap::new();
        // Three clustering keys (0, 1, 2), each with entries from three
        // different "runs" pushed in a deliberately scrambled interleaving.
        for &(run, cluster, ts) in &[
            (2, 1, 100),
            (0, 0, 300),
            (1, 2, 50),
            (1, 0, 200),
            (2, 2, 60),
            (0, 1, 150),
            (0, 2, 70),
            (2, 0, 250),
            (1, 1, 120),
        ] {
            heap.push(Reverse(live_entry(run, 1, cluster, ts)));
        }

        let mut popped_clusters = Vec::new();
        while let Some(Reverse(entry)) = heap.pop() {
            popped_clusters.push(entry.clustering_key.clone());
        }

        // Every same-clustering-key run must be contiguous: once we move past
        // a distinct clustering key, it must never reappear later in the
        // sequence. `ClusteringKey` has no `Hash` impl, so track "closed"
        // keys with a `Vec` + linear `contains` (the fixture is tiny) rather
        // than a `HashSet`.
        let mut closed: Vec<Option<ClusteringKey>> = Vec::new();
        let mut current: Option<Option<ClusteringKey>> = None;
        for c in &popped_clusters {
            if current.as_ref() != Some(c) {
                if let Some(prev) = current.take() {
                    assert!(
                        !closed.contains(&prev),
                        "clustering key reappeared non-contiguously after the heap moved past it"
                    );
                    closed.push(prev);
                }
                current = Some(c.clone());
            }
        }
        // Sanity: all three clustering keys were actually observed.
        let mut distinct: Vec<Option<ClusteringKey>> = Vec::new();
        for c in &popped_clusters {
            if !distinct.contains(c) {
                distinct.push(c.clone());
            }
        }
        assert_eq!(distinct.len(), 3, "expected 3 distinct clustering keys");
    }

    #[test]
    fn empty_merger_yields_complete() {
        let mut merger = merger_over(vec![], test_schema(ClusteringOrder::Asc));
        let mut stream = StreamingMerger::new(&mut merger);
        assert!(matches!(
            stream.step_streaming().unwrap(),
            StreamingStep::Complete
        ));
    }

    /// Drain a `StreamingMerger` to completion, collecting every
    /// `ClusterGroup` row and asserting a `PartitionEnd` closes each
    /// partition before the next one starts (or before `Complete`).
    fn drain_streaming(merger: &mut KWayMerger) -> Vec<MergeEntry> {
        let mut stream = StreamingMerger::new(merger);
        let mut rows = Vec::new();
        let mut in_partition = false;
        loop {
            match stream.step_streaming().unwrap() {
                StreamingStep::ClusterGroup { row, .. } => {
                    in_partition = true;
                    rows.push(*row);
                }
                StreamingStep::PartitionEnd { .. } => {
                    assert!(in_partition, "PartitionEnd with no preceding ClusterGroup");
                    in_partition = false;
                }
                StreamingStep::Complete => {
                    assert!(!in_partition, "Complete while a partition was still open");
                    return rows;
                }
            }
        }
    }

    /// Drain the OLD whole-partition `step()` path to completion, collecting
    /// every row across every partition in encounter order — the same shape
    /// `drain_streaming` produces, for direct comparison.
    fn drain_whole_partition(merger: &mut KWayMerger) -> Vec<MergeEntry> {
        let mut rows = Vec::new();
        loop {
            match merger.step().unwrap() {
                MergeStep::Partition {
                    rows: partition_rows,
                    ..
                } => rows.extend(partition_rows),
                MergeStep::Complete => return rows,
            }
        }
    }

    /// THE proof stage 3 needs: for a fixture mixing plain rows, a row
    /// tombstone, a range-tombstone carrier (#933), and a partition-deletion
    /// carrier (#1072) — i.e. exercising stage-1's `scan_partition_carriers`
    /// end to end — the streaming path yields EXACTLY the same reconciled
    /// rows, in the same order, as the old whole-partition path.
    #[test]
    fn step_streaming_matches_step_for_mixed_tombstone_fixture() {
        let schema = test_schema(ClusteringOrder::Asc);
        let entries = vec![
            live_entry(0, 1, 0, 100),
            live_entry(0, 1, 1, 100),
            live_entry(0, 1, 2, 100),
            range_carrier(1, 50, 5),
            partition_carrier(1, 10, 1),
        ];

        let mut old_merger = merger_over(entries.clone(), schema.clone());
        let old_rows = drain_whole_partition(&mut old_merger);

        let mut new_merger = merger_over(entries, schema);
        let new_rows = drain_streaming(&mut new_merger);

        assert!(
            !old_rows.is_empty(),
            "fixture must produce at least one row"
        );
        assert_eq!(
            old_rows, new_rows,
            "streaming path must yield the SAME rows, in the SAME order, as \
             the whole-partition path"
        );
    }

    /// Same proof, but with only plain multi-cluster rows (no carriers) —
    /// the common case.
    #[test]
    fn step_streaming_matches_step_for_plain_multi_cluster_fixture() {
        let schema = test_schema(ClusteringOrder::Asc);
        let entries = vec![
            live_entry(0, 1, 0, 100),
            live_entry(1, 1, 0, 90), // older duplicate, shadowed by LWW
            live_entry(0, 1, 1, 100),
            live_entry(0, 1, 2, 100),
            live_entry(0, 2, 0, 100), // second partition
        ];

        let mut old_merger = merger_over(entries.clone(), schema.clone());
        let old_rows = drain_whole_partition(&mut old_merger);

        let mut new_merger = merger_over(entries, schema);
        let new_rows = drain_streaming(&mut new_merger);

        assert_eq!(old_rows, new_rows);
    }

    /// A range-tombstone carrier round-trips through the streaming path as a
    /// `ClusterGroup` with `clustering_key: None`, exactly as it would sit in
    /// `MergeStep::Partition.rows` (proving stage-1's carriers thread through
    /// the increment API unchanged).
    #[test]
    fn range_tombstone_carrier_round_trips_as_cluster_group() {
        let carrier = range_carrier(9, 500, 50);
        assert!(super::super::carriers::is_range_marker_carrier(&carrier));

        let mut merger = merger_over(vec![carrier.clone()], test_schema(ClusteringOrder::Asc));
        let mut stream = StreamingMerger::new(&mut merger);
        match stream.step_streaming().unwrap() {
            StreamingStep::ClusterGroup { row, .. } => assert_eq!(*row, carrier),
            other => panic!("expected ClusterGroup, got {other:?}"),
        }
    }
}
