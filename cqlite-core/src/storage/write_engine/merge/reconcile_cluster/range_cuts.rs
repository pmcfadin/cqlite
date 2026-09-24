//! Range-cut helpers: coalesce a partition's range tombstones into a
//! non-overlapping canonical sequence (issue #933 / roborev #959) and test
//! clustering-key containment against a single range tombstone.
//!
//! Split out of `reconcile_cluster.rs` (issue #4193, design.md §D5 campsite
//! rule) — pure relocation, no behavior change.

use std::cmp::Ordering;

use super::super::trace::TraceSink;
use super::super::{DecoratedKey, KWayMerger};
use crate::schema::TableSchema;
use crate::storage::write_engine::mutation::{ClusteringKey, RangeTombstone};

/// A cut position on the clustering axis, used to coalesce range tombstones
/// into a NON-OVERLAPPING canonical sequence (issue #933 / roborev #959 High
/// #1).
///
/// The writer emits each [`RangeTombstone`] as an INDEPENDENT open/close marker
/// pair, sorted by clustering position; the reader pairs markers using a single
/// `pending_range_start`. Overlapping or nested ranges with different bounds
/// would therefore mis-pair on read-back (e.g. `start[1,5] start[2,3] end[2,3]
/// end[1,5]` resurfaces as `[2,3]` and `[Bottom,5]`), corrupting the persisted
/// deletion ranges. Coalescing the cross-SSTable union into disjoint ranges
/// before re-emission mirrors Cassandra's `RangeTombstoneList` invariant
/// (on-disk range tombstones within a partition never overlap).
///
/// A range `[start, end]` is modelled as a closed interval over these cut
/// positions. [`Self::At`] is the infinitesimal point just before (`after =
/// false`) or just after (`after = true`) a clustering prefix; an open bound is
/// [`Self::Bottom`] / [`Self::Top`]. The `after` flag at a SHORTER prefix sorts
/// relative to all longer extensions (Cassandra's kind-weighted prefix
/// ordering), so a prefix end bound `[ck1]` correctly covers every `[ck1, *]`.
#[cfg(feature = "write-support")]
#[derive(Clone)]
pub(in super::super) enum RangeCut {
    /// Before all clustering keys (start of partition).
    Bottom,
    /// Infinitesimally before (`after == false`) or after (`after == true`) the
    /// clustering prefix.
    At {
        /// Clustering prefix (possibly shorter than the full arity).
        key: ClusteringKey,
        /// `false` = just before the prefix and all its extensions; `true` =
        /// just after them.
        after: bool,
    },
    /// After all clustering keys (end of partition).
    Top,
}

impl<S: TraceSink> KWayMerger<S> {
    /// Coalesce range tombstones into a NON-OVERLAPPING canonical sequence per
    /// partition, the winning (newest) deletion time per covered segment (issue
    /// #933 / roborev #959 High #1).
    ///
    /// Each input SSTable's range tombstones are individually non-overlapping
    /// (Cassandra's `RangeTombstoneList` invariant), but the cross-SSTable union
    /// gathered during compaction can overlap with different bounds. The writer
    /// emits each retained range as an independent open/close marker pair and the
    /// reader pairs them with a single `pending_range_start`, so OVERLAPPING
    /// re-emitted ranges would mis-pair on read-back and corrupt the persisted
    /// ranges. Splitting the union into disjoint segments (each carrying the max
    /// `markedForDeleteAt` covering it) keeps the on-disk markers a clean
    /// alternating open/close sequence. This also subsumes the prior
    /// identical-bounds dedup.
    pub(in super::super) fn coalesce_range_tombstones(
        rts: &mut Vec<(DecoratedKey, RangeTombstone)>,
        schema: &TableSchema,
    ) {
        // Group by partition-key bytes, preserving the first-seen DecoratedKey as
        // the representative for each group (token + raw bytes are identical
        // across the group).
        let mut groups: Vec<(DecoratedKey, Vec<RangeTombstone>)> = Vec::new();
        for (key, rt) in rts.drain(..) {
            if let Some((_, ranges)) = groups.iter_mut().find(|(k, _)| k.key == key.key) {
                ranges.push(rt);
            } else {
                groups.push((key, vec![rt]));
            }
        }

        let mut out: Vec<(DecoratedKey, RangeTombstone)> = Vec::new();
        for (key, ranges) in groups {
            for rt in Self::coalesce_partition_range_tombstones(ranges, schema) {
                out.push((key.clone(), rt));
            }
        }
        *rts = out;
    }

    /// Coalesce the range tombstones of a SINGLE partition into a disjoint,
    /// clustering-sorted sequence (helper for [`Self::coalesce_range_tombstones`]).
    pub(in super::super) fn coalesce_partition_range_tombstones(
        ranges: Vec<RangeTombstone>,
        schema: &TableSchema,
    ) -> Vec<RangeTombstone> {
        if ranges.len() <= 1 {
            return ranges;
        }

        // Model each range as a closed interval [start_cut, end_cut] with its
        // (mfda, ldt).
        let items: Vec<(RangeCut, RangeCut, i64, i32)> = ranges
            .iter()
            .map(|rt| {
                (
                    Self::range_start_cut(&rt.start),
                    Self::range_end_cut(&rt.end),
                    rt.deletion_time,
                    rt.local_deletion_time,
                )
            })
            .collect();

        // Distinct, sorted cut positions (the candidate segment boundaries).
        let mut cuts: Vec<RangeCut> = Vec::with_capacity(items.len() * 2);
        for (s, e, _, _) in &items {
            cuts.push(s.clone());
            cuts.push(e.clone());
        }
        cuts.sort_by(|a, b| Self::cut_cmp(a, b, schema));
        cuts.dedup_by(|a, b| Self::cut_cmp(a, b, schema) == Ordering::Equal);

        // For each elementary gap (cuts[i], cuts[i+1]), the winning deletion is
        // the max `markedForDeleteAt` among ranges whose closed interval fully
        // contains the gap (start <= lo AND hi <= end).
        let mut segs: Vec<(RangeCut, RangeCut, i64, i32)> = Vec::new();
        for window in cuts.windows(2) {
            let (lo, hi) = (&window[0], &window[1]);
            let mut best: Option<(i64, i32)> = None;
            for (s, e, mfda, ldt) in &items {
                let covers = Self::cut_cmp(s, lo, schema) != Ordering::Greater
                    && Self::cut_cmp(hi, e, schema) != Ordering::Greater;
                if !covers {
                    continue;
                }
                best = Some(match best {
                    Some((bm, bl)) if bm > *mfda || (bm == *mfda && bl >= *ldt) => (bm, bl),
                    _ => (*mfda, *ldt),
                });
            }
            if let Some((mfda, ldt)) = best {
                segs.push((lo.clone(), hi.clone(), mfda, ldt));
            }
        }

        // Merge adjacent segments that share a boundary AND the same deletion
        // (minimal fragmentation); a gap with no covering range breaks the run.
        let mut merged: Vec<(RangeCut, RangeCut, i64, i32)> = Vec::new();
        for seg in segs {
            if let Some(last) = merged.last_mut() {
                if last.2 == seg.2
                    && last.3 == seg.3
                    && Self::cut_cmp(&last.1, &seg.0, schema) == Ordering::Equal
                {
                    last.1 = seg.1;
                    continue;
                }
            }
            merged.push(seg);
        }

        merged
            .into_iter()
            .map(|(lo, hi, mfda, ldt)| RangeTombstone {
                start: Self::cut_to_start_bound(lo),
                end: Self::cut_to_end_bound(hi),
                deletion_time: mfda,
                local_deletion_time: ldt,
            })
            .collect()
    }

    /// Total order of two cut positions on the clustering axis (schema-aware,
    /// honoring per-column ASC/DESC and Cassandra's kind-weighted prefix
    /// ordering). See [`RangeCut`].
    pub(in super::super) fn cut_cmp(a: &RangeCut, b: &RangeCut, schema: &TableSchema) -> Ordering {
        match (a, b) {
            (RangeCut::Bottom, RangeCut::Bottom) => Ordering::Equal,
            (RangeCut::Bottom, _) => Ordering::Less,
            (_, RangeCut::Bottom) => Ordering::Greater,
            (RangeCut::Top, RangeCut::Top) => Ordering::Equal,
            (RangeCut::Top, _) => Ordering::Greater,
            (_, RangeCut::Top) => Ordering::Less,
            (RangeCut::At { key: ka, after: aa }, RangeCut::At { key: kb, after: ab }) => {
                // Compare the common prefix only; a shorter prefix's `after` flag
                // decides its position relative to every longer extension.
                let l = ka.columns.len().min(kb.columns.len());
                let ta = ClusteringKey {
                    columns: ka.columns[..l].to_vec(),
                };
                let tb = ClusteringKey {
                    columns: kb.columns[..l].to_vec(),
                };
                let ord = ta.compare(&tb, schema).unwrap_or_else(|_| ta.cmp(&tb));
                if ord != Ordering::Equal {
                    return ord;
                }
                match ka.columns.len().cmp(&kb.columns.len()) {
                    Ordering::Equal => aa.cmp(ab),
                    // `a` is the shorter prefix: just-after sorts past every
                    // extension of it, just-before sorts ahead of all of them.
                    Ordering::Less => {
                        if *aa {
                            Ordering::Greater
                        } else {
                            Ordering::Less
                        }
                    }
                    // `b` is the shorter prefix (mirror image).
                    Ordering::Greater => {
                        if *ab {
                            Ordering::Less
                        } else {
                            Ordering::Greater
                        }
                    }
                }
            }
        }
    }

    /// Left edge (cut) of a range's start bound.
    pub(in super::super) fn range_start_cut(
        bound: &crate::storage::write_engine::mutation::ClusteringBound,
    ) -> RangeCut {
        use crate::storage::write_engine::mutation::ClusteringBound;
        match bound {
            ClusteringBound::Inclusive(ck) => RangeCut::At {
                key: ck.clone(),
                after: false,
            },
            ClusteringBound::Exclusive(ck) => RangeCut::At {
                key: ck.clone(),
                after: true,
            },
            ClusteringBound::Bottom => RangeCut::Bottom,
            ClusteringBound::Top => RangeCut::Top,
        }
    }

    /// Right edge (cut) of a range's end bound.
    pub(in super::super) fn range_end_cut(
        bound: &crate::storage::write_engine::mutation::ClusteringBound,
    ) -> RangeCut {
        use crate::storage::write_engine::mutation::ClusteringBound;
        match bound {
            ClusteringBound::Inclusive(ck) => RangeCut::At {
                key: ck.clone(),
                after: true,
            },
            ClusteringBound::Exclusive(ck) => RangeCut::At {
                key: ck.clone(),
                after: false,
            },
            ClusteringBound::Top => RangeCut::Top,
            ClusteringBound::Bottom => RangeCut::Bottom,
        }
    }

    /// Whether `outer` fully covers the canonical `inner` range. This is used
    /// only to carry the source generation across range coalescing; the write
    /// path's shadowing decision continues to use the authoritative per-cell
    /// containment check below.
    pub(in super::super) fn range_tombstone_contains_range(
        outer: &RangeTombstone,
        inner: &RangeTombstone,
        schema: &TableSchema,
    ) -> bool {
        let outer_start = Self::range_start_cut(&outer.start);
        let inner_start = Self::range_start_cut(&inner.start);
        let inner_end = Self::range_end_cut(&inner.end);
        let outer_end = Self::range_end_cut(&outer.end);
        Self::cut_cmp(&outer_start, &inner_start, schema) != Ordering::Greater
            && Self::cut_cmp(&inner_end, &outer_end, schema) != Ordering::Greater
    }

    /// Convert a left-edge cut back into a start [`ClusteringBound`].
    pub(in super::super) fn cut_to_start_bound(
        cut: RangeCut,
    ) -> crate::storage::write_engine::mutation::ClusteringBound {
        use crate::storage::write_engine::mutation::ClusteringBound;
        match cut {
            RangeCut::Bottom => ClusteringBound::Bottom,
            RangeCut::At { key, after: false } => ClusteringBound::Inclusive(key),
            RangeCut::At { key, after: true } => ClusteringBound::Exclusive(key),
            RangeCut::Top => ClusteringBound::Top,
        }
    }

    /// Convert a right-edge cut back into an end [`ClusteringBound`].
    pub(in super::super) fn cut_to_end_bound(
        cut: RangeCut,
    ) -> crate::storage::write_engine::mutation::ClusteringBound {
        use crate::storage::write_engine::mutation::ClusteringBound;
        match cut {
            RangeCut::Top => ClusteringBound::Top,
            RangeCut::At { key, after: true } => ClusteringBound::Inclusive(key),
            RangeCut::At { key, after: false } => ClusteringBound::Exclusive(key),
            RangeCut::Bottom => ClusteringBound::Bottom,
        }
    }
    /// Whether a range tombstone's clustering range covers `ck`, comparing bounds
    /// SCHEMA-AWARE (honoring per-column ASC/DESC via [`ClusteringKey::compare`],
    /// NOT the schema-agnostic `cmp`) — issue #933 / roborev #959 Medium #3.
    ///
    /// Bounds may be a PREFIX shorter than the full clustering arity; comparing
    /// only the bound's components (via [`ClusteringKey::compare`], which treats an
    /// absent trailing component as a first-sorting NULL) yields the correct
    /// containment for the `DELETE WHERE pk=? AND ck1=?` prefix case.
    pub(in super::super) fn range_tombstone_covers_ck(
        ck: &ClusteringKey,
        rt: &RangeTombstone,
        schema: &TableSchema,
    ) -> bool {
        // Issue #1669: count coverage comparisons so a bound test can prove the
        // binary search stays O(rows) — one candidate per row — instead of the
        // former O(rows × ranges) linear scan. Vanishes in production builds.
        #[cfg(test)]
        crate::storage::sstable::work_counters::range_coverage_scope::record();
        use crate::storage::write_engine::mutation::ClusteringBound;

        // Compare `ck` against a bound key over the bound's component count so a
        // prefix bound only compares its present components.
        let cmp = |bound: &ClusteringKey| -> Ordering {
            let n = bound.columns.len();
            let truncated = ClusteringKey {
                columns: ck.columns.iter().take(n).cloned().collect(),
            };
            truncated
                .compare(bound, schema)
                .unwrap_or_else(|_| truncated.cmp(bound))
        };

        let after_start = match &rt.start {
            ClusteringBound::Inclusive(b) => cmp(b) != Ordering::Less,
            ClusteringBound::Exclusive(b) => cmp(b) == Ordering::Greater,
            ClusteringBound::Bottom => true,
            ClusteringBound::Top => false,
        };
        let before_end = match &rt.end {
            ClusteringBound::Inclusive(b) => cmp(b) != Ordering::Greater,
            ClusteringBound::Exclusive(b) => cmp(b) == Ordering::Less,
            ClusteringBound::Top => true,
            ClusteringBound::Bottom => false,
        };
        after_start && before_end
    }

    /// Whether a coalesced range tombstone's END bound lies strictly BEFORE `ck`
    /// on the clustering axis — i.e. `ck` is beyond the range's end, so the range
    /// cannot cover it. This is exactly the negation of the `before_end` test in
    /// [`Self::range_tombstone_covers_ck`], kept in lock-step with it.
    ///
    /// It is the monotonic predicate the #1669 binary search feeds to
    /// [`slice::partition_point`]: because `coalesce_range_tombstones` yields a
    /// per-partition sequence sorted by start bound and DISJOINT, the ranges are
    /// also sorted by end bound, so `range_end_before_ck` is `true` for every
    /// range wholly before `ck` and `false` thereafter — a clean partition point.
    /// The first `false` range is the ONLY candidate that can contain `ck`
    /// (disjointness ⇒ at most one covers it).
    ///
    /// Deliberately does NOT bump the `range_coverage_scope` counter: it is the
    /// cheap `O(log ranges)` search step, distinct from the single authoritative
    /// `range_tombstone_covers_ck` containment check the counter measures.
    pub(in super::super) fn range_end_before_ck(
        ck: &ClusteringKey,
        rt: &RangeTombstone,
        schema: &TableSchema,
    ) -> bool {
        use crate::storage::write_engine::mutation::ClusteringBound;

        // Same prefix-aware comparison as `range_tombstone_covers_ck` (compare
        // `ck` against the bound over the bound's component count).
        let cmp = |bound: &ClusteringKey| -> Ordering {
            let n = bound.columns.len();
            let truncated = ClusteringKey {
                columns: ck.columns.iter().take(n).cloned().collect(),
            };
            truncated
                .compare(bound, schema)
                .unwrap_or_else(|_| truncated.cmp(bound))
        };

        match &rt.end {
            // Negation of the `before_end` arms in `range_tombstone_covers_ck`.
            ClusteringBound::Inclusive(b) => cmp(b) == Ordering::Greater,
            ClusteringBound::Exclusive(b) => cmp(b) != Ordering::Less,
            ClusteringBound::Top => false,
            ClusteringBound::Bottom => true,
        }
    }
}
