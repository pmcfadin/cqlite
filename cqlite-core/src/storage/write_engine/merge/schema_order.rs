//! Schema-aware heap-direct ordering proof (issue #1668, stage 5b) — the
//! "deferred crux" from stage 3a, solved for real this time.
//!
//! Stage 3a proved cross-group emission order is safe TODAY only because
//! [`super::KWayMerger::step_streaming`] drains a `Vec<MergeEntry>` that
//! `merge_partition_rows` already schema-aware-sorted (`merged.sort_by(|a,
//! b| ck_a.compare(ck_b, &self.schema) ...)`) as its unconditional last step.
//! Removing that whole-partition sort (stage 5d's job) requires a heap whose
//! POP ORDER is *itself* schema-correct — this module proves that is
//! achievable and correct, WITHOUT yet touching [`super::KWayMerger`]'s own
//! `heap: BinaryHeap<Reverse<MergeEntry>>` field (that wiring is stage 5c/5d's
//! job, once the writer can also accept true increments — mirroring the
//! stage-2-before-stage-3b precedent: introduce and prove the mechanism
//! first, wire it into production once every dependency is ready).
//!
//! ## Why a bounded lookahead/reorder buffer is NOT sufficient
//!
//! The issue's stage-5b framing offered two shapes: make the heap genuinely
//! schema-aware, OR prove a small bounded lookahead is sufficient. The bound
//! does not exist for a general schema: consider a single `DESC` clustering
//! column with N distinct values. The fallback (ASC-only) order pops them
//! ascending; the schema-correct (DESC) order needs the LARGEST value
//! FIRST — which is the LAST value the fallback order would ever produce.
//! Emitting the correct first item therefore requires having already seen
//! ALL N items — the required lookahead is O(N), not a fixed constant
//! independent of partition size. This is proven directly by
//! `stage3a`'s `step_streaming_matches_step_for_desc_clustering_fixture`
//! (fed via the SAME merger machinery) and is why this module makes the
//! comparator itself schema-aware instead.
//!
//! ## Design: a schema-aware comparator with NO lifetime on `KWayMerger`
//!
//! `MergeEntry::Ord` cannot be made schema-aware directly: `Ord::cmp(&self,
//! &other) -> Ordering` takes no extra context, and `ClusteringKey::compare`
//! needs `&TableSchema`. Storing a `&'a TableSchema` (or `Arc<TableSchema>`)
//! on every heap entry, if that heap were a LONG-LIVED struct field (like
//! `KWayMerger.heap`), would need `KWayMerger` to be self-referential (the
//! heap borrowing a sibling field of the SAME struct) — not expressible in
//! safe Rust. This module sidesteps the problem entirely by keeping the
//! schema-aware heap PURELY LOCAL to one function call
//! ([`schema_ordered_pop_all`]): the wrapper's `&'a TableSchema` borrow and
//! the heap it lives in are both stack-local to that call, so there is no
//! self-reference. Wiring this into `KWayMerger.heap` itself (stage 5c/5d)
//! will need ONE of: (a) an `Arc<TableSchema>` field alongside the existing
//! plain `schema: TableSchema` (cheap, additive, no lifetime issue), or (b)
//! a per-entry precomputed schema-independent sort key. Not resolved here —
//! this module only proves the COMPARATOR is correct and sufficient.

// Stage 5b (#1668) is deliberately UNWIRED: no production caller constructs a
// `SchemaOrderedEntry` yet (that is stage 5c/5d), so a normal (non-test)
// build sees every item here as unreachable. Matches the crate's existing
// convention for proof-only surface pending production wiring (see
// `streaming.rs`'s stage-2 history). Exercised directly by this module's own
// tests.
#![cfg_attr(feature = "write-support", allow(dead_code))]

#[cfg(feature = "write-support")]
use super::model::MergeEntry;
#[cfg(feature = "write-support")]
use crate::schema::TableSchema;
#[cfg(feature = "write-support")]
use std::cmp::{Ordering, Reverse};
#[cfg(feature = "write-support")]
use std::collections::BinaryHeap;

/// Heap element pairing a [`MergeEntry`] with the schema it should be
/// ordered against. `Ord` mirrors [`MergeEntry::Ord`] EXACTLY (same
/// token/key-bytes/run_index tiebreaks) except at the clustering-key step,
/// where it defers to the schema-aware [`ClusteringKey::compare`] — falling
/// back to the plain `Ord` on error, the SAME fallback `merge_partition_rows`
/// and `write_partition` already use for a malformed clustering key (more
/// components than the schema declares).
#[cfg(feature = "write-support")]
struct SchemaOrderedEntry<'a> {
    entry: MergeEntry,
    schema: &'a TableSchema,
}

#[cfg(feature = "write-support")]
impl<'a> SchemaOrderedEntry<'a> {
    fn new(entry: MergeEntry, schema: &'a TableSchema) -> Self {
        Self { entry, schema }
    }
}

#[cfg(feature = "write-support")]
impl PartialEq for SchemaOrderedEntry<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

#[cfg(feature = "write-support")]
impl Eq for SchemaOrderedEntry<'_> {}

#[cfg(feature = "write-support")]
impl PartialOrd for SchemaOrderedEntry<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(feature = "write-support")]
impl Ord for SchemaOrderedEntry<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.entry.key.token.cmp(&other.entry.key.token) {
            Ordering::Equal => match self.entry.key.key.cmp(&other.entry.key.key) {
                Ordering::Equal => {
                    match (&self.entry.clustering_key, &other.entry.clustering_key) {
                        (None, None) => self.entry.run_index.cmp(&other.entry.run_index),
                        (None, Some(_)) => Ordering::Less,
                        (Some(_), None) => Ordering::Greater,
                        (Some(a), Some(b)) => {
                            // THE schema-aware substitution for MergeEntry's
                            // fallback `a.cmp(b)`.
                            match a.compare(b, self.schema).unwrap_or_else(|_| a.cmp(b)) {
                                Ordering::Equal => self.entry.run_index.cmp(&other.entry.run_index),
                                other_ord => other_ord,
                            }
                        }
                    }
                }
                other_ord => other_ord,
            },
            other_ord => other_ord,
        }
    }
}

/// Push every entry onto a schema-aware min-heap and pop them all, in order
/// (issue #1668, stage 5b).
///
/// Proves cross-group emission order can be correct DIRECTLY off a heap —
/// no `merged.sort_by` needed afterward. The heap and its `&schema` borrow
/// are both local to this call, so there is no self-referential-storage
/// problem (see the module doc). NOT yet called by any production path;
/// `KWayMerger.heap` (`BinaryHeap<Reverse<MergeEntry>>`) is unchanged.
#[cfg(feature = "write-support")]
pub(crate) fn schema_ordered_pop_all(
    entries: Vec<MergeEntry>,
    schema: &TableSchema,
) -> Vec<MergeEntry> {
    let mut heap: BinaryHeap<Reverse<SchemaOrderedEntry<'_>>> = BinaryHeap::new();
    for entry in entries {
        heap.push(Reverse(SchemaOrderedEntry::new(entry, schema)));
    }
    let mut out = Vec::with_capacity(heap.len());
    while let Some(Reverse(wrapped)) = heap.pop() {
        out.push(wrapped.entry);
    }
    out
}

#[cfg(all(test, feature = "write-support"))]
mod tests {
    use super::*;
    use crate::schema::{ClusteringColumn, ClusteringOrder, KeyColumn};
    use crate::storage::write_engine::merge::model::{CellData, RowData};
    use crate::storage::write_engine::merge::{KWayMerger, RunReader, SSTableRowIterator};
    use crate::storage::write_engine::mutation::{ClusteringKey, DecoratedKey};
    use crate::types::Value;
    use std::collections::HashMap;

    /// Test-only `SSTableRowIterator` over a pre-supplied `Vec<MergeEntry>`,
    /// mirroring the SAME pattern already used by `mod.rs`'s and
    /// `streaming.rs`'s own merge unit tests (kept local here so this
    /// module's tests need no cross-module reuse of another file's private
    /// test helpers).
    struct VecIterator(std::vec::IntoIter<MergeEntry>);
    impl SSTableRowIterator for VecIterator {
        fn next(&mut self) -> Option<crate::error::Result<MergeEntry>> {
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

    fn key(token: i64) -> DecoratedKey {
        DecoratedKey::new(token, vec![token as u8])
    }

    fn live_entry(run_index: usize, token: i64, ck: ClusteringKey, ts: i64) -> MergeEntry {
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

    fn single_col_schema(order: ClusteringOrder) -> TableSchema {
        TableSchema {
            keyspace: "ks_1668_5b".to_string(),
            table: "t_1668_5b".to_string(),
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

    fn two_col_schema() -> TableSchema {
        TableSchema {
            keyspace: "ks_1668_5b".to_string(),
            table: "t_1668_5b_multi".to_string(),
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

    fn ck_multi(pairs: &[(&str, i32)]) -> ClusteringKey {
        ClusteringKey::new(
            pairs
                .iter()
                .map(|(n, v)| (n.to_string(), Value::Integer(*v)))
                .collect(),
        )
    }

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

    /// The grouping-contiguity property (issue #1668's "the crux") re-verified
    /// for THIS comparator: `ClusteringKey::compare` is ALSO a valid total
    /// order (transitive, antisymmetric — `compare_values` is the same
    /// canonical value-ordering function used throughout the write path), so
    /// same-clustering-key entries must still pop contiguously.
    #[test]
    fn heap_groups_contiguously_by_schema_aware_ord() {
        let schema = single_col_schema(ClusteringOrder::Desc);
        let entries = vec![
            live_entry(2, 1, ClusteringKey::single("ck", Value::Integer(1)), 100),
            live_entry(0, 1, ClusteringKey::single("ck", Value::Integer(0)), 300),
            live_entry(1, 1, ClusteringKey::single("ck", Value::Integer(2)), 50),
            live_entry(1, 1, ClusteringKey::single("ck", Value::Integer(0)), 200),
            live_entry(2, 1, ClusteringKey::single("ck", Value::Integer(2)), 60),
            live_entry(0, 1, ClusteringKey::single("ck", Value::Integer(1)), 150),
            live_entry(0, 1, ClusteringKey::single("ck", Value::Integer(2)), 70),
            live_entry(2, 1, ClusteringKey::single("ck", Value::Integer(0)), 250),
            live_entry(1, 1, ClusteringKey::single("ck", Value::Integer(1)), 120),
        ];

        let popped = schema_ordered_pop_all(entries, &schema);
        let popped_cks: Vec<i32> = popped.iter().map(ck_value).collect();

        let mut closed: Vec<i32> = Vec::new();
        let mut current: Option<i32> = None;
        for &c in &popped_cks {
            if current != Some(c) {
                if let Some(prev) = current.take() {
                    assert!(
                        !closed.contains(&prev),
                        "clustering key {prev} reappeared non-contiguously"
                    );
                    closed.push(prev);
                }
                current = Some(c);
            }
        }
        let mut distinct: Vec<i32> = Vec::new();
        for &c in &popped_cks {
            if !distinct.contains(&c) {
                distinct.push(c);
            }
        }
        assert_eq!(distinct.len(), 3, "expected 3 distinct clustering keys");
    }

    /// THE proof: a `DESC` clustering column where the required lookahead to
    /// emit the FIRST correctly-ordered item is the WHOLE partition (see the
    /// module doc) — a bounded reorder buffer cannot solve this, only a
    /// schema-aware comparator can. Assert the heap POPS the Cassandra-
    /// correct descending order directly, with NO subsequent sort — an
    /// independent expected-sequence check, not just "matches some other
    /// path" (which could be equally wrong).
    #[test]
    fn schema_ordered_pop_all_matches_desc_clustering_order() {
        let schema = single_col_schema(ClusteringOrder::Desc);
        let entries = vec![
            live_entry(0, 1, ClusteringKey::single("ck", Value::Integer(0)), 100),
            live_entry(0, 1, ClusteringKey::single("ck", Value::Integer(1)), 100),
            live_entry(0, 1, ClusteringKey::single("ck", Value::Integer(2)), 100),
        ];

        let popped = schema_ordered_pop_all(entries, &schema);
        let order: Vec<i32> = popped.iter().map(ck_value).collect();
        assert_eq!(
            order,
            vec![2, 1, 0],
            "heap-direct pop order must be DESCENDING with NO final sort"
        );
    }

    /// Same DESC fixture, cross-checked against TODAY's whole-partition
    /// buffer-then-sort path (`step()` via a real `KWayMerger`) — proving
    /// the heap-direct order (no final sort) matches the CURRENT production
    /// output row-for-row, not just the independently-expected sequence.
    #[test]
    fn schema_ordered_pop_all_matches_todays_step_output_for_desc_fixture() {
        let schema = single_col_schema(ClusteringOrder::Desc);
        let entries = vec![
            live_entry(0, 1, ClusteringKey::single("ck", Value::Integer(0)), 100),
            live_entry(0, 1, ClusteringKey::single("ck", Value::Integer(1)), 100),
            live_entry(0, 1, ClusteringKey::single("ck", Value::Integer(2)), 100),
        ];

        // Heap-direct (stage 5b, no final sort).
        let heap_direct = schema_ordered_pop_all(entries.clone(), &schema);
        let heap_direct_order: Vec<i32> = heap_direct.iter().map(ck_value).collect();

        // TODAY's path: whole-partition buffer + merge_partition_rows' final
        // schema-aware sort_by, via a real KWayMerger.
        let mut merger = merger_over(entries, schema);
        let today_rows = match merger.step().unwrap() {
            crate::storage::write_engine::merge::MergeStep::Partition { rows, .. } => rows,
            other => panic!("expected Partition, got {other:?}"),
        };
        let today_order: Vec<i32> = today_rows.iter().map(ck_value).collect();

        assert_eq!(
            heap_direct_order, today_order,
            "heap-direct order (no final sort) must match today's whole-\
             partition buffer-then-sort output"
        );
    }

    /// Absent-trailing-component (NULL-first) + `DESC` second column — the
    /// SAME residual case stage 3a proved doesn't leak through the (already-
    /// sorted-Vec-draining) `step_streaming` design. Here it is proven at
    /// the HEAP level directly: absent-`ck2` sorts first (NULL-first,
    /// regardless of `ck2`'s DESC-ness), then present values by DESC.
    #[test]
    fn schema_ordered_pop_all_matches_absent_trailing_component_order() {
        let schema = two_col_schema();
        let entries = vec![
            live_entry(0, 1, ck_multi(&[("ck1", 5), ("ck2", 1)]), 100),
            live_entry(0, 1, ck_multi(&[("ck1", 5)]), 100), // absent ck2
            live_entry(0, 1, ck_multi(&[("ck1", 5), ("ck2", 2)]), 100),
        ];

        let popped = schema_ordered_pop_all(entries, &schema);
        let ck2_markers: Vec<Option<i32>> = popped
            .iter()
            .map(|e| {
                e.clustering_key.as_ref().and_then(|k| {
                    k.columns
                        .iter()
                        .find(|(name, _)| name == "ck2")
                        .map(|(_, v)| match v {
                            Value::Integer(v) => *v,
                            other => panic!("expected Integer ck2, got {other:?}"),
                        })
                })
            })
            .collect();

        assert_eq!(
            ck2_markers,
            vec![None, Some(2), Some(1)],
            "absent-ck2 must sort first (NULL-first), then DESC by ck2 — \
             directly off the heap, no final sort"
        );
    }
}
