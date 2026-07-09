//! Streaming cluster-group step type (issue #1668, stages 2-3b).
//!
//! **Wiring status (stage 3b)**: [`KWayMerger::merge`] (used by
//! `compact_sstables_with_registry`) and `write_engine::maintenance`'s
//! compaction loop both now drain a partition via [`StreamingMerger`]/
//! [`StreamingStep`] instead of calling [`KWayMerger::step`] directly — each
//! accumulates `ClusterGroup` rows until `PartitionEnd`, then hands the SAME
//! `Vec<MergeEntry>` `step()` would have returned to the SAME unchanged
//! writer call, so output stays byte-identical (see stage 3b's `#921`
//! compaction-byte-parity harness run). `step_streaming` still calls the
//! UNCHANGED [`KWayMerger::step`] internally and drains its already-
//! reconciled `Vec<MergeEntry>` one row at a time, so peak memory is
//! UNCHANGED by this wiring (stage 5 removes the whole-partition buffering
//! that still lives inside `step()`/`merge_partition_rows`). The Flight
//! producer (`cqlite-flight/src/producer.rs`) is INTENTIONALLY untouched —
//! that is Q4/mid-partition-budget territory, a later stage.
//!
//! [`StreamingMerger`] does not touch [`MergeStep`]'s shape (a distinct
//! [`StreamingStep`] type) or `KWayMerger`'s own fields (a wrapper holding its
//! own drain state, so none of the existing `KWayMerger { .. }` struct-literal
//! unit tests in `mod.rs` needed updating).
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
//! ## Cross-group emission order — resolved for THIS design (issue #1668, stage 3a)
//!
//! Stage 2 flagged a residual: `ClusteringKey`'s fallback `Ord`
//! (`mutation.rs`) is NOT schema-aware — no `DESC` reversal, and an absent
//! trailing component is handled by zip-stopping + a length tiebreak rather
//! than Cassandra's explicit NULL-first rule (contrast the schema-aware
//! `ClusteringKey::compare`). Stage 3a's blast-radius audit + new tests below
//! resolve whether this is safe to wire a real consumer onto:
//!
//! **Blast radius**: `MergeEntry::Ord`/`PartialOrd` (the fallback comparator)
//! has exactly ONE production consumer in the whole crate — `KWayMerger`'s
//! `heap: BinaryHeap<Reverse<MergeEntry>>` (routing only, in `mod.rs`). No
//! other file constructs a `BinaryHeap<Reverse<MergeEntry>>` or otherwise
//! depends on `MergeEntry::Ord`'s specific clustering semantics; the one
//! direct-heap unit test (`mod.rs::test_merge_entry_min_heap`) only exercises
//! token ordering (`clustering_key: None` throughout), so it is untouched by
//! clustering-comparator semantics either way. Changing `MergeEntry::Ord`
//! globally would therefore be LOW risk in isolation — but it is UNNECESSARY:
//!
//! **Why no Ord change is needed for THIS design**: `step_streaming` (below)
//! never consumes heap-pop order for cross-group sequencing. It drains the
//! `Vec<MergeEntry>` `KWayMerger::step()` already returns, and `step()`
//! (via `merge_partition_rows`) already applies an explicit
//! `merged.sort_by(|a, b| ck_a.compare(ck_b, &self.schema) ...)` — the
//! SCHEMA-AWARE comparator — as its LAST step, unconditionally, regardless of
//! what order the heap or the `clustered_rows: BTreeMap` (also fallback-Ord-
//! keyed) produced internally. So `step()`'s returned order is ALREADY
//! schema-correct today, and `step_streaming` inherits that correctness
//! unchanged. `step_streaming_matches_step_for_desc_clustering_fixture` and
//! `step_streaming_matches_step_for_absent_trailing_component_fixture` below
//! prove this directly: both assert the emitted clustering-key SEQUENCE
//! (independently, not just old-path-equals-new-path) against the
//! Cassandra-correct expected order for a `DESC` column and for an
//! absent-trailing-component (NULL-first) case — neither the fallback Ord's
//! ordering, proving the divergence does NOT leak through this design.
//!
//! **Residual still applies to a DIFFERENT, NOT-YET-BUILT design**: a FUTURE
//! stage-5 rewrite that removes `merge_partition_rows`'s whole-partition
//! buffer-then-sort and instead emits groups directly off the heap (true
//! streaming, no per-partition buffer) would lose that final sort and must
//! independently solve cross-group ordering then — either by making the heap
//! comparator schema-aware (cheap: one production call site, per the blast-
//! radius finding above) or by a small local re-sort at group boundaries.
//! Flagged for that stage's design, not resolved here, because it does not
//! yet exist.
//!
//! `step_streaming` does NOT yet avoid whole-partition buffering: it calls
//! the unchanged [`KWayMerger::step`] (which still buffers a whole partition
//! and fully reconciles it via `merge_partition_rows`, itself now built on the
//! stage-1 `carriers::scan_partition_carriers` pre-scan) and then drains the
//! resulting `Vec<MergeEntry>` one row at a time. Removing that buffering is
//! stage 5's job; this stage proves the increment TYPE and consumer-loop
//! shape are safe to build on.

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

    /// A two-clustering-column schema (`ck1` ASC, `ck2` DESC) for the
    /// absent-trailing-component (NULL-first) test (issue #1668, stage 3a).
    fn two_col_schema() -> TableSchema {
        TableSchema {
            keyspace: "ks_1668".to_string(),
            table: "t_1668_multi".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![
                ClusteringColumn {
                    name: "ck1".to_string(),
                    data_type: "int".to_string(),
                    position: 0,
                    order: ClusteringOrder::Asc,
                },
                ClusteringColumn {
                    name: "ck2".to_string(),
                    data_type: "int".to_string(),
                    position: 1,
                    order: ClusteringOrder::Desc,
                },
            ],
            columns: vec![],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }

    /// Build a `ClusteringKey` from `(column, value)` pairs — may carry FEWER
    /// components than the schema declares, modeling an absent trailing
    /// component.
    fn ck_multi(pairs: &[(&str, i32)]) -> ClusteringKey {
        ClusteringKey::new(
            pairs
                .iter()
                .map(|(n, v)| (n.to_string(), Value::Integer(*v)))
                .collect(),
        )
    }

    fn live_entry_ck(run_index: usize, token: i64, ck: ClusteringKey, ts: i64) -> MergeEntry {
        MergeEntry::new(
            run_index,
            key(token),
            Some(ck),
            ts,
            RowData::Live {
                cells: vec![CellData::new("c".to_string(), Value::Integer(1), ts)],
            },
        )
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

    /// Extract the single-column `ck` value from a `MergeEntry` produced by
    /// `live_entry`, for order-assertion readability.
    fn ck_value(entry: &MergeEntry) -> i32 {
        match entry
            .clustering_key
            .as_ref()
            .and_then(|k| k.columns.first())
        {
            Some((_, Value::Integer(v))) => *v,
            other => panic!("expected a single Integer clustering column, got {other:?}"),
        }
    }

    /// Stage 3a correctness test: a `DESC` clustering column. The heap's
    /// fallback `Ord` would (if consulted directly, uncorrected) pop these in
    /// ASCENDING order (0, 1, 2) since it never applies `DESC` reversal — the
    /// Cassandra-correct order is DESCENDING (2, 1, 0). Assert BOTH `step()`
    /// and `step_streaming()` emit the CORRECT descending order — an
    /// independent check against the expected sequence, not just
    /// old-path-equals-new-path (which would pass even if both were equally
    /// wrong).
    #[test]
    fn step_streaming_matches_step_for_desc_clustering_fixture() {
        let schema = test_schema(ClusteringOrder::Desc);
        let entries = vec![
            live_entry(0, 1, 0, 100),
            live_entry(0, 1, 1, 100),
            live_entry(0, 1, 2, 100),
        ];
        let expected_order = [2, 1, 0];

        let mut old_merger = merger_over(entries.clone(), schema.clone());
        let old_rows = drain_whole_partition(&mut old_merger);
        let old_order: Vec<i32> = old_rows.iter().map(ck_value).collect();
        assert_eq!(
            old_order, expected_order,
            "step() must emit DESC clustering order"
        );

        let mut new_merger = merger_over(entries, schema);
        let new_rows = drain_streaming(&mut new_merger);
        let new_order: Vec<i32> = new_rows.iter().map(ck_value).collect();
        assert_eq!(
            new_order, expected_order,
            "step_streaming() must ALSO emit DESC clustering order, matching step()"
        );
    }

    /// Stage 3a correctness test: absent trailing clustering component
    /// (Cassandra NULL-first rule) combined with a `DESC` second column.
    /// Schema-aware order: the absent-`ck2` row sorts FIRST (NULL always
    /// sorts first, regardless of `ck2`'s `DESC`-ness), then among the two
    /// present-`ck2` rows the LARGER value sorts first (`DESC` reversal).
    /// Expected sequence: `[absent, ck2=2, ck2=1]`.
    #[test]
    fn step_streaming_matches_step_for_absent_trailing_component_fixture() {
        let schema = two_col_schema();
        let entries = vec![
            live_entry_ck(0, 1, ck_multi(&[("ck1", 5), ("ck2", 1)]), 100),
            live_entry_ck(0, 1, ck_multi(&[("ck1", 5)]), 100), // absent ck2
            live_entry_ck(0, 1, ck_multi(&[("ck1", 5), ("ck2", 2)]), 100),
        ];

        // Independent expectation: identify each row by its ck2 presence/value.
        fn ck2_marker(entry: &MergeEntry) -> Option<i32> {
            entry.clustering_key.as_ref().and_then(|k| {
                k.columns
                    .iter()
                    .find(|(name, _)| name == "ck2")
                    .map(|(_, v)| match v {
                        Value::Integer(v) => *v,
                        other => panic!("expected Integer ck2, got {other:?}"),
                    })
            })
        }
        let expected_order = [None, Some(2), Some(1)];

        let mut old_merger = merger_over(entries.clone(), schema.clone());
        let old_rows = drain_whole_partition(&mut old_merger);
        let old_order: Vec<Option<i32>> = old_rows.iter().map(ck2_marker).collect();
        assert_eq!(
            old_order, expected_order,
            "step() must put the absent-ck2 row first (NULL-first), then DESC by ck2"
        );

        let mut new_merger = merger_over(entries, schema);
        let new_rows = drain_streaming(&mut new_merger);
        let new_order: Vec<Option<i32>> = new_rows.iter().map(ck2_marker).collect();
        assert_eq!(
            new_order, expected_order,
            "step_streaming() must ALSO put the absent-ck2 row first, then DESC by ck2"
        );
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
