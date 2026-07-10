//! Incremental rows+markers interleave (issue #1668, stage 5c-iii).
//!
//! `sort_partition_items` (`partition.rs`) re-sorts the WHOLE combined
//! `rows ++ markers` sequence from scratch every time, even though `rows`
//! already arrives in clustering order (from `merge_clustering_rows`, which
//! itself requires pre-sorted `mutations` — see stage 5a) and the range-
//! tombstone markers are typically FEW (the stage-1 carrier pre-scan already
//! established this). [`merge_rows_and_markers`] replaces that full re-sort
//! with a classic two-way MERGE: sort the (small) marker list once, then
//! interleave it with the already-sorted rows in one forward pass — no
//! re-sorting of the combined, potentially partition-sized sequence.
//!
//! The comparator itself, [`partition_item_cmp`], is extracted VERBATIM from
//! `sort_partition_items`'s prior inline closure (moved, not duplicated) so
//! BOTH the old full-sort path and this new merge path always agree — a
//! future divergence between the two would be a single-function fix, not a
//! two-place hunt.
//!
//! ## Why a merge is safe here (no tie-break ambiguity)
//!
//! A `Row` at a given clustering value always carries `weight = 0`
//! (`CLUSTERING` ordinal), while a `Marker` at ANY clustering value carries
//! `weight = ±1` — never `0`. So a `Row` and a `Marker` can never compare
//! EQUAL under [`partition_item_cmp`] (the tuple always differs at the
//! `weight` field), which is exactly why choosing "take from `rows` on a
//! non-`Greater` comparison" in [`merge_rows_and_markers`]'s tie-break never
//! actually has to arbitrate a real tie between the two streams — only
//! marker-vs-marker or row-vs-row comparisons could tie, and those are each
//! already resolved within their OWN pre-sorted stream before the merge
//! step ever compares across streams.

use super::*;

/// Sort key for clustering-ordered emission: `(position class, clustering
/// values, bound weight, kind ordinal)`. class: -1 = before all rows
/// (Bottom), 0 = positioned by clustering values, 1 = after all rows (Top).
/// The weight orders a marker relative to a row at the SAME clustering value
/// (`ClusteringPrefix.Kind.comparedToClustering`): an inclusive-start /
/// exclusive-end bound sorts before the row, an inclusive-end / exclusive-
/// start bound after it.
///
/// The final element is the Cassandra `ClusteringPrefix.Kind` ordinal, used
/// as a strict tiebreak so that — at an equal clustering point and equal
/// weight — the CLOSING bound of one range always sorts immediately before
/// the matching OPENING bound of the next, regardless of the order the
/// `range_tombstones` arrived in (issue #1220, roborev finding 2).
fn sort_class<'a, 'b>(item: &'b PartitionItem<'a>) -> (i8, Option<&'b ClusteringKey>, i8, u8) {
    match item {
        PartitionItem::Row(row) => (0, row.clustering_key, 0, CLUSTERING),
        PartitionItem::Marker { bound, is_open, .. } => {
            let kind = bound_kind_ordinal(bound, *is_open);
            match bound {
                ClusteringBound::Inclusive(ck) => {
                    (0, Some(ck), if *is_open { -1 } else { 1 }, kind)
                }
                ClusteringBound::Exclusive(ck) => {
                    (0, Some(ck), if *is_open { 1 } else { -1 }, kind)
                }
                ClusteringBound::Bottom => (-1, None, 0, kind),
                ClusteringBound::Top => (1, None, 0, kind),
            }
        }
        PartitionItem::Boundary {
            kind, clustering, ..
        } => (0, Some(clustering), 0, *kind),
    }
}

/// The Cassandra `ClusteringPrefix.Kind` ordinal for a range-tombstone BOUND.
/// Used purely as a sort tiebreak (see [`sort_class`]); the canonical
/// ordering is the one decoded on the read path (`row_decoder/block_emit.rs`)
/// and the constants in `data_writer/mod.rs`:
///   0 EXCL_END_BOUND · 1 INCL_START_BOUND · 6 INCL_END_BOUND · 7 EXCL_START_BOUND.
fn bound_kind_ordinal(bound: &ClusteringBound, is_open: bool) -> u8 {
    match (is_open, bound) {
        (true, ClusteringBound::Inclusive(_)) => INCL_START_BOUND, // 1
        (false, ClusteringBound::Exclusive(_)) => EXCL_END_BOUND,  // 0
        (false, ClusteringBound::Inclusive(_)) => INCL_END_BOUND,  // 6
        (true, ClusteringBound::Exclusive(_)) => EXCL_START_BOUND, // 7
        // Open-ended bounds are positioned by `class` (Bottom/Top), never
        // reach a value-level tiebreak; report their side's kind for
        // completeness.
        (true, ClusteringBound::Bottom | ClusteringBound::Top) => INCL_START_BOUND,
        (false, ClusteringBound::Bottom | ClusteringBound::Top) => INCL_END_BOUND,
    }
}

/// Total order over [`PartitionItem`]s (rows + bound markers), schema-aware.
/// Moved VERBATIM out of `sort_partition_items`'s prior inline closure
/// (issue #1668, stage 5c-iii) so the full-sort and merge paths share ONE
/// comparator.
pub(super) fn partition_item_cmp(
    a: &PartitionItem,
    b: &PartitionItem,
    schema: &TableSchema,
) -> std::cmp::Ordering {
    let (class_a, ck_a, weight_a, kind_a) = sort_class(a);
    let (class_b, ck_b, weight_b, kind_b) = sort_class(b);
    class_a
        .cmp(&class_b)
        .then_with(|| match (ck_a, ck_b) {
            (Some(x), Some(y)) => x.compare(y, schema).unwrap_or_else(|_| x.cmp(y)),
            _ => std::cmp::Ordering::Equal,
        })
        .then(weight_a.cmp(&weight_b))
        .then(kind_a.cmp(&kind_b))
}

/// Merge already-sorted `rows` with the (typically few) markers derived from
/// `range_tombstones`, WITHOUT re-sorting the combined sequence from scratch
/// (issue #1668, stage 5c-iii).
///
/// `rows` MUST already be in clustering order (guaranteed by
/// `merge_clustering_rows`'s pre-sorted `mutations` input — see stage 5a).
/// The markers are built from `range_tombstones` and sorted ONCE (cheap: this
/// list is small), then interleaved with `rows` in a single forward two-way
/// merge pass. Produces the IDENTICAL sequence `sort_partition_items` (the
/// full re-sort) computes for the same inputs — proven by this module's
/// tests.
///
/// NOT yet called by any production path: `write_partition_with_index_blocks`
/// still calls `sort_partition_items` (the full re-sort) — swapping to this
/// merge is stage 5c-iv's job, once the writer's incremental entry point
/// exists to actually feed it row-by-row. `#[allow(dead_code)]` until then,
/// matching the crate's convention for proof-only surface pending wiring
/// (see `schema_order.rs`'s stage-5b history).
#[allow(dead_code)]
pub(super) fn merge_rows_and_markers<'a>(
    rows: Vec<PartitionItem<'a>>,
    range_tombstones: &'a [RangeTombstone],
    schema: &TableSchema,
) -> Vec<PartitionItem<'a>> {
    let mut markers: Vec<PartitionItem<'a>> = Vec::with_capacity(range_tombstones.len() * 2);
    for rt in range_tombstones {
        markers.push(PartitionItem::Marker {
            bound: &rt.start,
            is_open: true,
            deletion_time: rt.deletion_time,
            local_deletion_time: rt.local_deletion_time,
        });
        markers.push(PartitionItem::Marker {
            bound: &rt.end,
            is_open: false,
            deletion_time: rt.deletion_time,
            local_deletion_time: rt.local_deletion_time,
        });
    }
    // The marker list is small (typically far fewer than the row count), so
    // sorting IT (not the combined sequence) stays cheap.
    markers.sort_by(|a, b| partition_item_cmp(a, b, schema));

    let mut out = Vec::with_capacity(rows.len() + markers.len());
    let mut rows_iter = rows.into_iter().peekable();
    let mut markers_iter = markers.into_iter().peekable();
    loop {
        match (rows_iter.peek(), markers_iter.peek()) {
            (Some(r), Some(m)) => {
                // A Row and a Marker never compare Equal (see the module
                // doc), so this branch never has to arbitrate a real tie.
                if partition_item_cmp(r, m, schema) != std::cmp::Ordering::Greater {
                    // SAFETY-free `unwrap`: `peek()` just proved `Some`.
                    if let Some(row) = rows_iter.next() {
                        out.push(row);
                    }
                } else if let Some(marker) = markers_iter.next() {
                    out.push(marker);
                }
            }
            (Some(_), None) => {
                out.extend(rows_iter.by_ref());
                break;
            }
            (None, Some(_)) => {
                out.extend(markers_iter.by_ref());
                break;
            }
            (None, None) => break,
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ClusteringColumn, ClusteringOrder, KeyColumn};
    use crate::storage::write_engine::mutation::ClusteringKey;
    use crate::types::Value;

    fn schema_one_clustering(order: ClusteringOrder) -> TableSchema {
        TableSchema {
            keyspace: "ks".to_string(),
            table: "t".to_string(),
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
            comments: std::collections::HashMap::new(),
            dropped_columns: std::collections::HashMap::new(),
        }
    }

    fn ck(v: i32) -> ClusteringKey {
        ClusteringKey::single("ck", Value::Integer(v))
    }

    fn row_item(v: i32) -> PartitionItem<'static> {
        // A minimal RowWrite: no ops, no deletion — only clustering_key
        // matters for the interleave/sort comparison under test.
        PartitionItem::Row(RowWrite {
            clustering_key: Some(Box::leak(Box::new(ck(v)))),
            liveness_ts: Some(0),
            ttl_seconds: None,
            row_deletion: None,
            ops: Vec::new(),
            complex_element_ops: Vec::new(),
        })
    }

    fn row_key(item: &PartitionItem<'_>) -> Option<i32> {
        match item {
            PartitionItem::Row(row) => match row.clustering_key.and_then(|k| k.columns.first()) {
                Some((_, Value::Integer(v))) => Some(*v),
                _ => None,
            },
            _ => None,
        }
    }

    fn is_marker(item: &PartitionItem<'_>) -> bool {
        matches!(item, PartitionItem::Marker { .. })
    }

    /// THE proof: a partition with range tombstones interleaved with rows —
    /// the incremental merge must produce the IDENTICAL sequence the full
    /// re-sort (`sort_partition_items`) computes for the SAME inputs.
    /// Independently reasoned expected shape (not just old==new): with a
    /// range tombstone covering `[1, 3]` (inclusive both ends) and rows at
    /// 0, 1, 2, 3, 4, the OPEN marker must sort strictly before row 1 (the
    /// inclusive-start weight is -1, ahead of the row's weight 0) and the
    /// CLOSE marker strictly after row 3 (inclusive-end weight is +1).
    #[test]
    fn merge_matches_full_sort_for_range_tombstone_interleaved_with_rows() {
        let schema = schema_one_clustering(ClusteringOrder::Asc);
        let rt = RangeTombstone {
            start: ClusteringBound::Inclusive(ck(1)),
            end: ClusteringBound::Inclusive(ck(3)),
            deletion_time: 1000,
            local_deletion_time: 100,
        };
        let range_tombstones = vec![rt];

        // Incremental merge (stage 5c-iii).
        let merged =
            merge_rows_and_markers((0..5).map(row_item).collect(), &range_tombstones, &schema);

        // Full re-sort (today's path): rows + 2 markers, then sort_by the
        // SAME shared comparator.
        let mut combined: Vec<PartitionItem> = (0..5).map(row_item).collect();
        for rt in &range_tombstones {
            combined.push(PartitionItem::Marker {
                bound: &rt.start,
                is_open: true,
                deletion_time: rt.deletion_time,
                local_deletion_time: rt.local_deletion_time,
            });
            combined.push(PartitionItem::Marker {
                bound: &rt.end,
                is_open: false,
                deletion_time: rt.deletion_time,
                local_deletion_time: rt.local_deletion_time,
            });
        }
        combined.sort_by(|a, b| partition_item_cmp(a, b, &schema));

        // Independent expectation: open marker between row 0 and row 1;
        // close marker between row 3 and row 4.
        let shape: Vec<(Option<i32>, bool)> =
            merged.iter().map(|i| (row_key(i), is_marker(i))).collect();
        assert_eq!(
            shape,
            vec![
                (Some(0), false),
                (None, true), // open marker, before row 1
                (Some(1), false),
                (Some(2), false),
                (Some(3), false),
                (None, true), // close marker, after row 3
                (Some(4), false),
            ],
            "merge must place the open marker before row 1 and the close \
             marker after row 3 (inclusive-both-ends range tombstone)"
        );

        // Merge output must be IDENTICAL to the full re-sort's output.
        let merged_shape: Vec<(Option<i32>, bool)> =
            merged.iter().map(|i| (row_key(i), is_marker(i))).collect();
        let combined_shape: Vec<(Option<i32>, bool)> = combined
            .iter()
            .map(|i| (row_key(i), is_marker(i)))
            .collect();
        assert_eq!(
            merged_shape, combined_shape,
            "incremental merge must produce the IDENTICAL interleaved \
             sequence as the full re-sort for the same inputs"
        );
    }

    /// A DESC clustering column: proves the merge still agrees with the full
    /// re-sort when `rows` arrives in DESC order (the schema-aware order,
    /// per stage 5b/5c-i) rather than ASC.
    #[test]
    fn merge_matches_full_sort_for_desc_clustering_with_range_tombstone() {
        let schema = schema_one_clustering(ClusteringOrder::Desc);
        // Rows arrive in DESC clustering order: 4, 3, 2, 1, 0.
        let rt = RangeTombstone {
            start: ClusteringBound::Exclusive(ck(3)),
            end: ClusteringBound::Exclusive(ck(1)),
            deletion_time: 2000,
            local_deletion_time: 200,
        };
        let range_tombstones = vec![rt];

        let merged = merge_rows_and_markers(
            (0..5).rev().map(row_item).collect(),
            &range_tombstones,
            &schema,
        );

        let mut combined: Vec<PartitionItem> = (0..5).rev().map(row_item).collect();
        for rt in &range_tombstones {
            combined.push(PartitionItem::Marker {
                bound: &rt.start,
                is_open: true,
                deletion_time: rt.deletion_time,
                local_deletion_time: rt.local_deletion_time,
            });
            combined.push(PartitionItem::Marker {
                bound: &rt.end,
                is_open: false,
                deletion_time: rt.deletion_time,
                local_deletion_time: rt.local_deletion_time,
            });
        }
        combined.sort_by(|a, b| partition_item_cmp(a, b, &schema));

        let merged_shape: Vec<(Option<i32>, bool)> =
            merged.iter().map(|i| (row_key(i), is_marker(i))).collect();
        let combined_shape: Vec<(Option<i32>, bool)> = combined
            .iter()
            .map(|i| (row_key(i), is_marker(i)))
            .collect();
        assert_eq!(
            merged_shape, combined_shape,
            "merge must agree with the full re-sort under DESC clustering too"
        );
    }

    /// No range tombstones: the merge degenerates to just the rows,
    /// unchanged, matching the full re-sort of rows alone.
    #[test]
    fn merge_matches_full_sort_with_no_range_tombstones() {
        let schema = schema_one_clustering(ClusteringOrder::Asc);
        let merged = merge_rows_and_markers((0..3).map(row_item).collect(), &[], &schema);
        let merged_keys: Vec<Option<i32>> = merged.iter().map(row_key).collect();
        assert_eq!(merged_keys, vec![Some(0), Some(1), Some(2)]);
    }
}
