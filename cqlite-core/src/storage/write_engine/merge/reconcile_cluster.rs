//! Cluster and range reconciliation helpers for the compaction merger.
//!
//! The implementation is intentionally relocated from [`super`] without changing
//! reconciliation behavior; the trace hooks are added separately after this kernel
//! remains parity-clean.

use super::reconcile;
use super::trace::{CellDecision, DecidedBy, NoTrace, TombstoneKind, TraceSink, Verdict};
use super::{CellData, ComplexDeletion, KWayMerger, MergeEntry, PurgeCounts, RowData};
use crate::schema::TableSchema;
use crate::storage::write_engine::mutation::{ClusteringKey, DecoratedKey, RangeTombstone};
use std::cmp::Ordering;
use std::collections::HashMap;

fn trace_cell_value(cell: &CellData) -> Option<crate::types::Value> {
    (!KWayMerger::<NoTrace>::is_cell_tombstone(cell) && !cell.is_deleted)
        .then(|| cell.value.clone())
}

/// Emit the final live-cell winners after range and partition shadowing have
/// completed. Earlier reconcile steps emit terminal loser decisions as soon as
/// the deciding rule removes them.
pub(super) fn trace_entry_winners<T: TraceSink>(
    entry: &MergeEntry,
    winner_runs: Option<&HashMap<reconcile::CellKey, usize>>,
    trace: &mut T,
) {
    if !T::ENABLED {
        return;
    }
    let RowData::Live { cells } = &entry.row_data else {
        return;
    };
    for cell in cells {
        if KWayMerger::<NoTrace>::is_cell_tombstone(cell) || cell.is_deleted {
            continue;
        }
        let cell_key = (cell.column.clone(), cell.cell_path.clone());
        let run_index = winner_runs
            .and_then(|runs| runs.get(&cell_key).copied())
            .unwrap_or(entry.run_index);
        trace.cell(CellDecision {
            run_index,
            clustering: entry.clustering_key.clone(),
            column: cell.column.clone(),
            value: trace_cell_value(cell),
            writetime: cell.timestamp,
            ttl: cell.ttl.and_then(|ttl| i32::try_from(ttl).ok()),
            expires_at: cell.local_deletion_time.map(|ldt| i64::from(ldt as u32)),
            verdict: Verdict::Winner,
            decided_by: DecidedBy::Winner {
                run_index,
                writetime: cell.timestamp,
            },
        });
    }
}

/// Emit cells removed by a range or partition floor.
pub(super) fn trace_entry_shadowed<T: TraceSink>(
    entry: &MergeEntry,
    cells: &[CellData],
    winner_runs: Option<&HashMap<reconcile::CellKey, usize>>,
    trace: &mut T,
    kind: TombstoneKind,
    run_index: usize,
    deletion_time: i64,
    local_deletion_time: i32,
) {
    if !T::ENABLED {
        return;
    }
    for cell in cells {
        let cell_key = (cell.column.clone(), cell.cell_path.clone());
        let source_run_index = winner_runs
            .and_then(|runs| runs.get(&cell_key).copied())
            .unwrap_or(entry.run_index);
        trace.cell(CellDecision {
            run_index: source_run_index,
            clustering: entry.clustering_key.clone(),
            column: cell.column.clone(),
            value: trace_cell_value(cell),
            writetime: cell.timestamp,
            ttl: cell.ttl.and_then(|ttl| i32::try_from(ttl).ok()),
            expires_at: cell.local_deletion_time.map(|ldt| i64::from(ldt as u32)),
            verdict: Verdict::ShadowedByTombstone(kind),
            decided_by: DecidedBy::Tombstone {
                kind,
                run_index,
                deletion_time,
                local_deletion_time,
                droppable_at_now: false,
            },
        });
    }
}

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
pub(super) enum RangeCut {
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
    pub(super) fn coalesce_range_tombstones(
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
    pub(super) fn coalesce_partition_range_tombstones(
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
    pub(super) fn cut_cmp(a: &RangeCut, b: &RangeCut, schema: &TableSchema) -> Ordering {
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
    pub(super) fn range_start_cut(
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
    pub(super) fn range_end_cut(
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
    pub(super) fn range_tombstone_contains_range(
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
    pub(super) fn cut_to_start_bound(
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
    pub(super) fn cut_to_end_bound(
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
    pub(super) fn range_tombstone_covers_ck(
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
    pub(super) fn range_end_before_ck(
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

    /// Shadow the cells of a reconciled cluster entry that are covered by a range
    /// tombstone (issue #933, the re-applied #846 "Step 2c" made schema-aware).
    ///
    /// Computes the max `markedForDeleteAt` among range tombstones covering this
    /// entry's clustering key, then drops every DATA cell whose own `timestamp` is
    /// `<= floor` (the `<=` boundary lets a deletion win an equal-ts tie, #498).
    /// Clustering-key pseudo-cells are retained whenever any data cell survives so
    /// the row keeps its key columns for read-back. A row whose every data cell is
    /// shadowed AND whose row-marker liveness is `<= floor` produces nothing (the
    /// re-emitted range marker covers it); a coexisting row deletion newer than the
    /// floor is preserved as a row tombstone. A row with no clustering key (static
    /// / unclustered) is never covered by a range tombstone.
    pub(super) fn apply_range_shadowing(
        entry: MergeEntry,
        range_tombstones: &[(DecoratedKey, RangeTombstone)],
        schema: &TableSchema,
    ) -> Option<MergeEntry> {
        let mut sink = NoTrace;
        Self::apply_range_shadowing_traced(entry, range_tombstones, schema, None, None, &mut sink)
    }

    /// Trace-enabled range shadowing entry point. The no-trace wrapper above is
    /// retained for the existing streaming and unit-test callers.
    pub(super) fn apply_range_shadowing_traced<T: TraceSink>(
        entry: MergeEntry,
        range_tombstones: &[(DecoratedKey, RangeTombstone)],
        schema: &TableSchema,
        winner_runs: Option<&HashMap<reconcile::CellKey, usize>>,
        range_source_runs: Option<&[Option<usize>]>,
        trace: &mut T,
    ) -> Option<MergeEntry> {
        // Fast path for the overwhelmingly common partition with no range
        // tombstones: skip the clustering-key clone and coverage scan entirely.
        if range_tombstones.is_empty() {
            return Some(entry);
        }
        let Some(ck) = entry.clustering_key.clone() else {
            return Some(entry);
        };

        // Issue #1669: binary search for the covering range instead of a linear
        // `filter().max()` scan run per clustering key. `coalesce_range_tombstones`
        // produces, per partition key, a sequence sorted by start bound and
        // DISJOINT (verified: it partitions the clustering axis into segments
        // between distinct sorted cut boundaries and only merges adjacent
        // same-deletion segments — see `coalesce_partition_range_tombstones`). And
        // `apply_range_shadowing` is called from `merge_partition_rows`, which is
        // strictly per-partition, so this whole slice is ONE partition's
        // sorted+disjoint ranges. Disjoint ⇒ at most ONE range covers a given `ck`,
        // so the former max over the covering set is a max over ≤1 element: a
        // binary search finds it. O(rows × ranges) → O(rows × log ranges + ranges).
        //
        // Defensive guard: only take the binary-search path when the slice is
        // provably a single partition matching `entry`. `coalesce_range_tombstones`
        // groups partitions into CONTIGUOUS blocks, so first.key == last.key ⇒ one
        // group; == `entry.key` ⇒ it is `entry`'s partition. Any other shape (a
        // future multi-partition caller) falls back to the original exact linear
        // scan, so correctness never depends on the guarantee holding — only the
        // speedup does.
        let single_partition = match (range_tombstones.first(), range_tombstones.last()) {
            (Some((first, _)), Some((last, _))) => {
                first.key == entry.key.key && last.key == entry.key.key
            }
            _ => false,
        };
        let floor = if single_partition {
            // First range whose end is NOT before `ck` — the unique candidate that
            // can contain `ck` (disjoint + sorted). Verify full containment (both
            // bounds) via the authoritative `range_tombstone_covers_ck`.
            let idx = range_tombstones
                .partition_point(|(_, rt)| Self::range_end_before_ck(&ck, rt, schema));
            range_tombstones.get(idx).and_then(|(key, rt)| {
                (key.key == entry.key.key && Self::range_tombstone_covers_ck(&ck, rt, schema))
                    .then_some(rt.deletion_time)
            })
        } else {
            // Exact pre-#1669 behavior for any non-single-partition slice.
            range_tombstones
                .iter()
                .filter(|(key, rt)| {
                    key.key == entry.key.key && Self::range_tombstone_covers_ck(&ck, rt, schema)
                })
                .map(|(_, rt)| rt.deletion_time)
                .max()
        };
        let Some(floor) = floor else {
            return Some(entry);
        };

        // `floor` was computed by the authoritative containment check above;
        // recover its marker without a second coverage comparison (the latter
        // is counted by the range-search work-counter tests).
        let covering_range = range_tombstones
            .iter()
            .enumerate()
            .find(|(_, (key, rt))| key.key == entry.key.key && rt.deletion_time == floor);
        if let Some((range_index, (_, range))) = covering_range {
            if T::ENABLED {
                let ck_names: std::collections::HashSet<&str> =
                    ck.columns.iter().map(|(name, _)| name.as_str()).collect();
                let shadowed: Vec<CellData> = match &entry.row_data {
                    RowData::Live { cells } => cells
                        .iter()
                        .filter(|cell| {
                            !ck_names.contains(cell.column.as_str()) && cell.timestamp <= floor
                        })
                        .cloned()
                        .collect(),
                    RowData::Tombstone { .. } => Vec::new(),
                };
                trace_entry_shadowed(
                    &entry,
                    &shadowed,
                    winner_runs,
                    trace,
                    TombstoneKind::Range,
                    range_source_runs
                        .and_then(|runs| runs.get(range_index).copied().flatten())
                        .unwrap_or(entry.run_index),
                    range.deletion_time,
                    range.local_deletion_time,
                );
            }
        }

        // A complex (collection) deletion OLDER than the covering range is
        // subsumed by the range marker; one STRICTLY NEWER must survive, else
        // older collection elements from a non-compacted SSTable could resurrect
        // (roborev #959 High #2 — `apply_range_shadowing` previously dropped
        // `entry.complex_deletions` on the whole-row-covered path). Filtered once
        // here and reused by every arm below.
        let surviving_complex: Vec<ComplexDeletion> = entry
            .complex_deletions
            .into_iter()
            .filter(|cd| cd.marked_for_delete_at > floor)
            .collect();

        match entry.row_data {
            RowData::Tombstone {
                deletion_time,
                local_deletion_time,
            } => {
                // A row tombstone fully covered by a newer/equal range deletion is
                // redundant (the range marker shadows it); drop it. A strictly
                // newer row tombstone survives.
                if deletion_time > floor {
                    let mut rebuilt = MergeEntry::new(
                        entry.run_index,
                        entry.key,
                        Some(ck),
                        deletion_time,
                        RowData::Tombstone {
                            deletion_time,
                            local_deletion_time,
                        },
                    );
                    if !surviving_complex.is_empty() {
                        rebuilt = rebuilt.with_complex_deletions(surviving_complex);
                    }
                    Some(rebuilt)
                } else if !surviving_complex.is_empty() {
                    // The row tombstone is subsumed by the range, but a newer
                    // complex deletion must persist as a metadata-only carrier.
                    Some(
                        MergeEntry::new(
                            entry.run_index,
                            entry.key,
                            Some(ck),
                            entry.timestamp,
                            RowData::Live { cells: Vec::new() },
                        )
                        .with_complex_deletions(surviving_complex),
                    )
                } else {
                    None
                }
            }
            RowData::Live { cells } => {
                let ck_names: std::collections::HashSet<&str> =
                    ck.columns.iter().map(|(n, _)| n.as_str()).collect();
                let is_data = |c: &CellData| !ck_names.contains(c.column.as_str());

                // Keep clustering pseudo-cells; keep data cells strictly newer than
                // the covering range deletion.
                let kept: Vec<CellData> = cells
                    .into_iter()
                    .filter(|c| !is_data(c) || c.timestamp > floor)
                    .collect();
                let has_data = kept.iter().any(is_data);

                // A coexisting row deletion (#932) older than the range floor is
                // subsumed by the range marker; a newer one is preserved.
                let surviving_row_del = entry.row_deletion.filter(|(dt, _)| *dt > floor);

                // The row-marker liveness survives the range only if its own
                // timestamp is strictly newer than the covering deletion.
                let marker_live = entry.timestamp > floor;

                if !has_data && !marker_live {
                    // Whole row covered by the range. A coexisting row deletion
                    // (#932) newer than the floor wins and subsumes any collection
                    // deletion. Otherwise a complex deletion newer than the floor
                    // must persist as a metadata-only carrier (roborev #959 High
                    // #2); failing that, the re-emitted range marker is the sole
                    // survivor and this entry contributes nothing.
                    if let Some((dt, ldt)) = surviving_row_del {
                        return Some(MergeEntry::new(
                            entry.run_index,
                            entry.key,
                            Some(ck),
                            dt,
                            RowData::Tombstone {
                                deletion_time: dt,
                                local_deletion_time: ldt,
                            },
                        ));
                    }
                    if !surviving_complex.is_empty() {
                        return Some(
                            MergeEntry::new(
                                entry.run_index,
                                entry.key,
                                Some(ck),
                                entry.timestamp,
                                RowData::Live { cells: Vec::new() },
                            )
                            .with_complex_deletions(surviving_complex),
                        );
                    }
                    return None;
                }

                let row_ts = if has_data {
                    kept.iter()
                        .filter(|c| is_data(c))
                        .map(|c| c.timestamp)
                        .max()
                        .unwrap_or(entry.timestamp)
                } else {
                    entry.timestamp
                };

                let mut rebuilt = MergeEntry::new(
                    entry.run_index,
                    entry.key,
                    Some(ck),
                    row_ts,
                    RowData::Live { cells: kept },
                );
                // Issue #2374/#2789: carry the primary-key liveness marker forward
                // when it survives the range floor, so a key-only live row (INSERT
                // with no/all-null regular columns) that coexists with an older
                // covering range tombstone stays VISIBLE through the read path.
                // Dropped (default) when the marker did not survive the floor.
                rebuilt = rebuilt.with_row_liveness(if marker_live {
                    entry.row_liveness
                } else {
                    Default::default()
                });
                if !surviving_complex.is_empty() {
                    rebuilt = rebuilt.with_complex_deletions(surviving_complex);
                }
                if let Some((dt, ldt)) = surviving_row_del {
                    rebuilt = rebuilt.with_row_deletion(dt, ldt);
                }
                Some(rebuilt)
            }
        }
    }
    /// Reconcile all entries for a single clustering-key group into at most one
    /// merged `MergeEntry`, applying per-cell last-write-wins plus row-tombstone
    /// shadowing (Issue #533). See [`Self::merge_partition_rows`] for the rules.
    ///
    /// `cluster_rows` is in heap-routing order (run_index ascending within equal
    /// keys), so when two cells tie on both timestamp and liveness the first-seen
    /// (newer file) is kept.
    ///
    /// Thin wrapper over [`Self::reconcile_cluster_with_overlap`] that defaults the
    /// overlap-aware max-purgeable timestamp to `i64::MAX` (unrestricted — the
    /// full-compaction semantics in effect before #935). The production merge path
    /// (`merge_partition_rows`) calls the `_with_overlap` form directly to pass the
    /// real bound for a partial compaction.
    #[cfg(test)]
    pub(super) fn reconcile_cluster(
        clustering_key: Option<ClusteringKey>,
        cluster_rows: Vec<MergeEntry>,
        dropped_columns: &std::collections::HashMap<String, i64>,
        gc_before_secs: Option<i64>,
    ) -> Option<MergeEntry> {
        Self::reconcile_cluster_with_overlap(
            clustering_key,
            cluster_rows,
            dropped_columns,
            gc_before_secs,
            i64::MAX,
        )
    }

    /// Reconcile a clustering-key group with an explicit overlap-aware
    /// max-purgeable timestamp (#935). See [`Self::reconcile_cluster`] for the base
    /// reconciliation rules.
    ///
    /// Thin wrapper over [`Self::reconcile_cluster_with_overlap_counted`] that
    /// discards the tombstone-purge tally. The production merge path
    /// (`merge_partition_rows`) calls the `_counted` form directly to accumulate
    /// genuine gc/overlap-safe purges for `COMPACTION_TOMBSTONES_PURGED` (#1037);
    /// this wrapper keeps the simpler signature for tests and other callers.
    #[cfg(test)]
    pub(super) fn reconcile_cluster_with_overlap(
        clustering_key: Option<ClusteringKey>,
        cluster_rows: Vec<MergeEntry>,
        dropped_columns: &std::collections::HashMap<String, i64>,
        gc_before_secs: Option<i64>,
        max_purgeable_timestamp: i64,
    ) -> Option<MergeEntry> {
        let mut sink = PurgeCounts::default();
        Self::reconcile_cluster_with_overlap_counted(
            clustering_key,
            cluster_rows,
            dropped_columns,
            gc_before_secs,
            max_purgeable_timestamp,
            // #1382: this test-only wrapper keeps the pre-#1382 default of NO
            // TTL expiry (`now_secs = None` = strict no-op). Tests that exercise
            // TTL expiry drive the real `compact_sstables` surface instead.
            None,
            &mut sink,
        )
    }

    /// Reconcile a clustering-key group, accumulating genuine gc/overlap-safe
    /// tombstone purges into `purges` (issue #1037).
    ///
    /// Identical merge OUTPUT to [`Self::reconcile_cluster_with_overlap`]; the
    /// only addition is that each true purge decision (a cell tombstone, row
    /// tombstone, or complex deletion dropped because it is gc/overlap-safe to
    /// drop in Step 3c) increments the matching `purges` field. Last-write-wins
    /// reconciliation collapse is NOT counted.
    pub(super) fn reconcile_cluster_with_overlap_counted(
        clustering_key: Option<ClusteringKey>,
        cluster_rows: Vec<MergeEntry>,
        dropped_columns: &std::collections::HashMap<String, i64>,
        // EFFECTIVE gc_grace cutoff (`gcBefore`, GC-clock seconds), threaded from
        // the merger. A tombstone whose `localDeletionTime < gc_before_secs` is
        // PURGEABLE; `None` disables purging (issue #845).
        //
        // OVERLAP SAFETY (#921 finding 1, #935): the caller
        // (`merge_partition_rows`) collapses this to `None` only when the
        // compaction is a PARTIAL one with NO overlap bound, so the purge stage is
        // a strict no-op there. With a bound it runs and each tombstone is
        // additionally gated on `max_purgeable_timestamp` below.
        gc_before_secs: Option<i64>,
        // EFFECTIVE overlap-aware max-purgeable timestamp (`markedForDeleteAt`,
        // micros), threaded from the merger (#935). A tombstone is purgeable ONLY
        // when its own deletion timestamp is STRICTLY LESS THAN this value, so it
        // provably shadows no data living in a non-included overlapping SSTable.
        // `i64::MAX` for a full compaction (no outside overlap — every gc-purgeable
        // tombstone passes); the min outside timestamp for an overlap-aware partial
        // compaction; `i64::MIN` when purging is disabled (`gc_before_secs` is then
        // `None`, so this is unused).
        max_purgeable_timestamp: i64,
        // Pinned TTL-expiry evaluation instant (`now`, GC-clock seconds), threaded
        // from the merger (#1382). A live expiring cell whose `localDeletionTime`
        // is STRICTLY LESS THAN this is turned into a cell tombstone (Step 3b′)
        // and then purged by the SAME gc/overlap gate as any other cell tombstone.
        // `None` disables expiry (a strict no-op), preserving pre-#1382 behavior.
        now_secs: Option<i64>,
        // Tombstone-purge tally accumulated at the true purge decision points
        // (issue #1037). Never read here; only incremented.
        purges: &mut PurgeCounts,
    ) -> Option<MergeEntry> {
        let mut sink = NoTrace;
        Self::reconcile_cluster_with_overlap_counted_traced(
            clustering_key,
            cluster_rows,
            dropped_columns,
            gc_before_secs,
            max_purgeable_timestamp,
            now_secs,
            purges,
            &mut sink,
        )
    }

    /// Trace-enabled reconciliation entry point used by the production merger.
    /// The compatibility wrapper above keeps the existing no-trace callers and
    /// tests on the same default construction path.
    pub(super) fn reconcile_cluster_with_overlap_counted_traced<T: TraceSink>(
        clustering_key: Option<ClusteringKey>,
        cluster_rows: Vec<MergeEntry>,
        dropped_columns: &std::collections::HashMap<String, i64>,
        gc_before_secs: Option<i64>,
        max_purgeable_timestamp: i64,
        now_secs: Option<i64>,
        purges: &mut PurgeCounts,
        trace: &mut T,
    ) -> Option<MergeEntry> {
        Self::reconcile_cluster_with_overlap_counted_traced_with_metadata(
            clustering_key,
            cluster_rows,
            dropped_columns,
            gc_before_secs,
            max_purgeable_timestamp,
            now_secs,
            purges,
            trace,
        )
        .0
    }

    /// Trace-enabled reconciliation with the per-cell source map retained for
    /// the outer range/partition shadowing stages.
    pub(super) fn reconcile_cluster_with_overlap_counted_traced_with_metadata<T: TraceSink>(
        clustering_key: Option<ClusteringKey>,
        cluster_rows: Vec<MergeEntry>,
        dropped_columns: &std::collections::HashMap<String, i64>,
        gc_before_secs: Option<i64>,
        max_purgeable_timestamp: i64,
        now_secs: Option<i64>,
        purges: &mut PurgeCounts,
        trace: &mut T,
    ) -> (Option<MergeEntry>, reconcile::ReconcileTraceMetadata) {
        // Issue #3058: explicit "the compaction reconciler ran" marker (see
        // `storage::read_path_probe`) — a single relaxed add on the merge arm.
        crate::storage::read_path_probe::record_reconcile_entry();
        // Decomposed into named, parity-load-bearing steps in `reconcile.rs`
        // (issue #945). The step ORDER is critical: Step 2b before Steps 3/3c so
        // a surviving complex deletion cannot resurrect a covered element on a
        // later purge (`f66fa14f`); `had_data_before` is captured pre-purge and
        // consulted post-purge (#921 finding 3). Behavior and the #1037 purge
        // tally are byte-identical.
        let mut state =
            reconcile::ReconcileState::with_trace(clustering_key, trace, gc_before_secs, now_secs);
        // Step 1: fold per-entry row/complex/range deletion metadata.
        state.fold_row_deletions(&cluster_rows);
        // Step 2: per-(column, cell_path) last-write-wins winner resolution.
        state.resolve_cell_winners(&cluster_rows);
        // Empty group => nothing to emit (original `let key = key?`).
        if !state.has_key() {
            return (
                None,
                reconcile::ReconcileTraceMetadata { winner_runs: None },
            );
        }
        // Step 2b: complex-deletion strict-supersede + shadow-before-purge.
        state.apply_complex_deletions();
        // Step 3: row-tombstone shadowing (tallies #2163 suppression).
        state.shadow_by_row_deletion(purges);
        // Step 3b: dropped-column filtering (captures the phantom-row guard).
        state.filter_dropped_columns(dropped_columns);
        // Step 3b′ (#1382): TTL expiry — convert expired live cells to cell
        // tombstones BEFORE the gc-grace purge so an expired-past-grace cell is
        // dropped by Step 3c and an expired-within-grace cell is emitted as a
        // tombstone (its live value never resurfaces).
        state.expire_ttl_cells(now_secs);
        // Step 3c: gc_grace / overlap-aware tombstone purging (tallies #1037).
        state.purge_gc_grace(gc_before_secs, max_purgeable_timestamp, purges);
        // Step 4: phantom-row guard + emit the merged entry (tallies #2163
        // emitted row-tombstone markers).
        state.build_with_trace_metadata(purges)
    }
}
