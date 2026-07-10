//! Streaming cluster-group step type (issue #1668, stages 2-5d).
//!
//! **Wiring status (stage 3b)**: [`KWayMerger::merge`] (used by
//! `compact_sstables_with_registry`) and `write_engine::maintenance`'s
//! compaction loop both drain a partition via [`StreamingMerger`]/
//! [`StreamingStep`] instead of calling [`KWayMerger::step`] directly.
//!
//! **Stage 5d — the whole-partition buffer is GONE.** Through stage 5c,
//! `step_streaming` called the UNCHANGED [`KWayMerger::step`] (which still
//! buffered a WHOLE partition into `partition_rows: Vec<MergeEntry>`,
//! grouped it into `clustered_rows: BTreeMap<Option<ClusteringKey>, _>`, and
//! produced one `merged: Vec<MergeEntry>` via `merge_partition_rows`) and
//! merely DRAINED that already-fully-computed `Vec` one row at a time — peak
//! memory still scaled with partition width. `step_streaming` now pulls
//! DIRECTLY off `KWayMerger`'s heap and reconciles ONE clustering-key group
//! at a time (bounded by the number of input runs sharing that EXACT key,
//! never partition width) via [`StreamingMerger::pull_one`]/
//! [`StreamingMerger::finalize_current_cluster`], calling the SAME
//! `reconcile_cluster_with_overlap_counted`/`apply_range_shadowing`/
//! `apply_partition_shadowing`/`coalesce_range_tombstones` primitives
//! `merge_partition_rows` always used — just triggered per-group instead of
//! in a bulk loop over a pre-built map. `KWayMerger::step`/
//! `merge_partition_rows` are UNCHANGED (nothing else calls them in
//! production; they remain this module's own byte-for-byte oracle — see the
//! `step_streaming_matches_step_for_*` tests below, which now prove the
//! NATIVE streaming path, not a thin drain wrapper, matches the buffered
//! path row-for-row).
//!
//! ## Two independent accumulation dimensions (issue #1668, stage 5d finding)
//!
//! A partition's raw entry stream has TWO things that must be tracked
//! SIMULTANEOUSLY, not sequentially:
//!
//! 1. **Carriers** (partition-tombstone / range-tombstone markers,
//!    [`MergeEntry::is_partition_delete_carrier`] /
//!    [`super::carriers::is_range_marker_carrier`]) — siphoned into
//!    `range_tombstones`/`max_partition_deletion` as encountered.
//! 2. **The current clustering-key group** — non-carrier entries sharing
//!    ONE `clustering_key`, accumulated until a DIFFERENT key (or the
//!    partition/heap) ends it, then reconciled via
//!    `reconcile_cluster_with_overlap_counted` + shadowed by the (by-then
//!    fully resolved) carrier state.
//!
//! For a table WITH clustering columns, [`schema_order::SchemaOrderedEntry`]'s
//! `Ord` puts EVERY `clustering_key: None` entry (carriers AND the static-row
//! candidates, which are ordinary non-carrier entries with `clustering_key:
//! None`) strictly before ANY `Some(ck)` entry — so carriers form a clean
//! prefix relative to the clustering-row tail. But for an UNCLUSTERED table
//! (no clustering columns declared), EVERY entry — carriers AND the sole
//! row's cross-run duplicates — shares `clustering_key: None` and is
//! tie-broken ONLY by `run_index`, so a carrier can sort BETWEEN two of that
//! row's duplicates (e.g. run 0's row, run 1's OWN partition-tombstone
//! carrier, run 1's row, run 2's row — all four tie on `(token, key, None,
//! run_index)` pairwise except run_index, so a same-run carrier and row are
//! not even guaranteed relative order). This is why carriers are classified
//! and siphoned UNCONDITIONALLY on every pulled entry — never assumed to
//! form a clean prefix relative to the group currently accumulating.
//!
//! `partition_deletion_resolved` (the PARTITION-tombstone half only — see
//! below for why range tombstones are handled separately) still flips ONCE
//! per partition: by the time the FIRST cluster needs shadowing, every
//! partition-tombstone carrier has necessarily already been seen, because
//! the partition-level deletion is always part of the on-disk HEADER (never
//! interleaved with rows), so it precedes anything else in that run's own
//! sequence, and thus (by the same argument as above) everything currently
//! poppable from the heap for this partition.
//!
//! ## Range tombstones can arrive AFTER the rows they cover (issue #1668, stage 5d finding — bounded-open-range design)
//!
//! Unlike the partition tombstone, a range tombstone is NOT guaranteed to
//! precede its covered rows. Tracing the real reader
//! (`row_decoder/compaction.rs`'s `on_range_marker`/`on_data_row`): the
//! parser holds the OPEN bound in `self.pending_range_start` (purely
//! internal state, never surfaced) while rows between it and the CLOSE
//! bound are pushed to the merge stream via `on_data_row` IMMEDIATELY, one
//! at a time; the COMPLETE, paired `RangeMarker` `MergeEntry` is only
//! pushed once the CLOSE bound is parsed — i.e. strictly AFTER every row it
//! covers. This matches the on-disk format
//! (`docs/sstables-definitive-guide/chapters/05-data-db-format.md`: markers
//! sit "between rows in a partition", not bunched into a prefix). So a
//! cluster can reconcile and be ready to flush BEFORE we've seen the range
//! tombstone that should shadow it.
//!
//! **Design (issue #1668, per-issue owner decision — the two-pass
//! reader-side redesign that would let the READER expose the open bound
//! early is explicitly OUT OF SCOPE; filed as a follow-up, see
//! `PartitionReconcileCheckpoint::awaiting_flush`'s doc)**: a cluster's
//! survivor flushes straight to `pending_rows` with ZERO extra buffering
//! as long as NO range tombstone has been seen for this partition yet (the
//! common case — most partitions have none at all, and the issue's own
//! motivating scenario is a huge ROW COUNT, not necessarily a wide range
//! tombstone). Once the FIRST range tombstone's complete entry arrives,
//! every SUBSEQUENT survivor is held in `awaiting_flush` instead — it
//! might still need shadowing from a DIFFERENT/later range tombstone (e.g.
//! from a slower-moving run in a multi-run merge) — and every entry
//! currently held there is re-shadowed and flushed as soon as EITHER
//! another range tombstone resolves or the partition ends. This bounds
//! peak memory to `max(current cluster group size, rows accumulated since
//! the last flush point)` for the common case of narrow/occasional range
//! tombstones, degrading toward whole-partition width only for a
//! pathological range tombstone whose span covers nearly the entire
//! partition — an honestly documented residual, not a silent correctness
//! gap (every row is STILL correctly shadowed once its covering marker
//! resolves; only the PEAK MEMORY bound degrades for that specific shape).
//!
//! ## Load-bearing precondition: each run must arrive pre-sorted (issue #1668, stage 5d finding)
//!
//! `KWayMerger`'s heap holds AT MOST one "candidate" entry per input run at
//! any time (`initialize_heap`/`refill_heap` — the classic K-WAY MERGE
//! shape: interleave `k` ALREADY-SORTED streams, never re-sort within one).
//! Consequently the heap can only ever choose correctly ACROSS runs; it
//! cannot reorder a single run's OWN entries relative to each other, since
//! it never sees more than one of that run's entries at a time. This is
//! SAFE for real data: a genuine SSTable file's rows are always written in
//! schema-consistent order (`SSTableWriter::write_partition`'s own
//! `mutations.sort_by(|a, b| ck_a.compare(ck_b, &self.schema) ...)` — the
//! SAME schema-aware comparator, which DOES reverse `DESC` columns — always
//! runs before ANY row is written), so a `RunReader` backed by a real file
//! naturally yields that file's rows already in the correct order; the
//! heap's real job is purely to interleave MULTIPLE such runs. A test
//! fixture that pushes a SINGLE run's entries in an order inconsistent with
//! the CURRENT schema (e.g. ascending `MergeEntry`s for a `DESC` column) is
//! therefore not modeling a real SSTable file — `merger_over_runs` (below)
//! puts each independently-orderable entry in its OWN run so the fixture
//! exercises the heap's REAL job (cross-run interleaving) instead of an
//! unrealistic single-run reordering. The pre-stage-5d `step()` masked one
//! such unrealistic fixture (raw carriers tagged with a `usize::MAX`
//! run_index sentinel, which real carriers never carry — see
//! `build_merge_entry`) via an unrelated bug: `current_partition` is never
//! actually assigned, so `step()`'s "lazy init" guard re-triggers
//! `initialize_heap()` — which happens to force-refill a stalled run — on
//! EVERY subsequent `step()` call whenever the heap empties, silently
//! splitting one logical partition into extra `MergeStep::Partition` steps
//! that `drain_whole_partition` then concatenates back together. This
//! module's tests construct carriers with their REAL run_index instead
//! (see `range_carrier`/`partition_carrier`'s docs), matching
//! `build_merge_entry`.
//!
//! ## Grouping-contiguity VERIFICATION (issue #1668 flags this as "the crux")
//!
//! The design assumes that once processing has moved past a clustering key
//! while draining the heap, it never sees that key again — i.e. the heap pop
//! order groups every NON-CARRIER entry for the same `(pk, ck)` contiguously
//! (carriers interleave freely, per the finding above, but never split a
//! real group since they are filtered out before the grouping decision).
//! [`MergeEntry`]'s `Ord` (`model.rs`) orders primarily by `(token, key
//! bytes, clustering_key, run_index)`; [`schema_order::SchemaOrderedEntry`]'s
//! `Ord` (stage 5c-i, now what the heap actually uses) substitutes a
//! schema-aware clustering-key comparison at that same step. Since
//! `BinaryHeap::pop` yields a non-increasing (here: `Reverse`, so
//! non-decreasing) sequence under one fixed total order, and grouping
//! identity is that SAME comparator's notion of equality, entries for one
//! clustering key are contiguous in heap-pop order — proven directly against
//! the heap (not assumed) by the `heap_groups_contiguously_by_ord`/
//! `heap_groups_contiguously_by_schema_aware_ord` tests.
//!
//! ## Cross-group emission order — schema-aware, no final sort (issue #1668, stages 3a/5c-i/5d)
//!
//! Stage 2 flagged a residual: the (then-fallback, non-schema-aware) heap
//! `Ord` doesn't reverse `DESC` columns or NULL-first an absent trailing
//! component. Stage 5c-i made the heap's OWN comparator schema-aware
//! (`schema_order::SchemaOrderedEntry`), so stage 5d's native per-group
//! emission needs no compensating final sort (`schema_order.rs`'s
//! `schema_ordered_pop_all_matches_todays_step_output_for_desc_fixture` test
//! already proved heap-direct order matches `merge_partition_rows`'s
//! sorted output for a DESC fixture; the tests below prove the SAME for the
//! native per-group reconciliation path specifically). Stage 3a's residual
//! (below) documents the ORIGINAL fallback-Ord concern, since superseded:
//!
//! **Historical note (stage 3a, since superseded by 5c-i/5d)**:
//! `MergeEntry::Ord`/`PartialOrd` (the fallback comparator) still has exactly
//! ONE production consumer in the whole crate — nothing else constructs a
//! `BinaryHeap<Reverse<MergeEntry>>` or depends on its clustering semantics
//! (the one direct-heap unit test, `mod.rs::test_merge_entry_min_heap`, only
//! exercises token ordering). Stage 3a reasoned that changing it was
//! unnecessary because `step()`'s final `merged.sort_by` already fixed up
//! any heap-order imperfection; stage 5c-i made the heap's OWN comparator
//! schema-aware anyway (`schema_order::SchemaOrderedEntry`), and stage 5d
//! now relies on THAT comparator directly (no final sort at all) — so this
//! residual is fully closed, not merely deferred.

#[cfg(feature = "write-support")]
use super::model::{MergeEntry, RowData};
#[cfg(feature = "write-support")]
use super::{carriers, KWayMerger, PurgeCounts};
#[cfg(feature = "write-support")]
use crate::error::{Error, Result};
#[cfg(feature = "write-support")]
use crate::storage::write_engine::mutation::{ClusteringKey, DecoratedKey, RangeTombstone};
#[cfg(feature = "write-support")]
use std::cmp::Reverse;
#[cfg(feature = "write-support")]
use std::collections::VecDeque;

/// dhat memory-bound proof for the merge layer's OWN streaming bound,
/// isolated from the reader (issue #1668 — see its module doc for why this
/// must be an in-tree `#[cfg(test)]` module rather than a `tests/` integration
/// test). Kept in its own file so this addition does not grow `streaming.rs`
/// itself past the campsite threshold.
#[cfg(all(test, feature = "write-support", feature = "dhat-heap"))]
mod streaming_dhat_test;

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
/// Opaque, resumable snapshot of a [`StreamingMerger`]'s per-partition
/// reconciliation state (issue #1668, stage 5d). Bundles EVERYTHING
/// [`StreamingMerger::into_paused_state`]/[`StreamingMerger::resume`] need
/// to survive a `maintenance_step_inner` budget pause with zero
/// re-computation and zero lost/duplicated rows, now that reconciliation
/// happens incrementally (cluster by cluster, directly off the heap)
/// instead of via one whole-partition `step()` call whose OUTPUT `Vec` was
/// simply drained. `write_engine::maintenance` holds this OPAQUELY (it never
/// inspects a field) — see `ActiveMerge::pending_partition`.
#[cfg(feature = "write-support")]
#[derive(Debug)]
pub(crate) struct PartitionReconcileCheckpoint {
    pending_rows: VecDeque<MergeEntry>,
    /// EVERY range tombstone seen so far for this partition (never removed
    /// once added — issue #1668 stage 5d's bounded-open-range design keeps
    /// this deliberately simple: a row is re-checked against the FULL set
    /// seen so far each time it might be affected, rather than tracking
    /// exactly which ranges are still "in flight").
    range_tombstones: Vec<(DecoratedKey, RangeTombstone)>,
    max_partition_deletion: Option<(i64, i32)>,
    partition_delete_key: Option<DecoratedKey>,
    /// Resolves EXACTLY ONCE per partition, when the bounded `clustering_key:
    /// None` PREFIX ends (the first non-carrier entry arrives) — covers the
    /// PARTITION-level tombstone (always part of the on-disk header, so
    /// always fully known by then) and re-emits whatever range tombstones
    /// happened to also be seen during that prefix. Renamed from
    /// `carriers_resolved` (issue #1668 stage 5d): a range tombstone
    /// specifically can ALSO arrive well after this point (see
    /// `awaiting_flush`'s doc) — this flag no longer covers that case.
    partition_deletion_resolved: bool,
    current_cluster: Option<(Option<ClusteringKey>, Vec<MergeEntry>)>,
    purges: PurgeCounts,
    /// Reconciled rows held back from `pending_rows` because at least one
    /// range tombstone has been seen for this partition (issue #1668 stage
    /// 5d's bounded-open-range design — see the module doc). A row lands
    /// here instead of `pending_rows` whenever `range_tombstones` is
    /// non-empty at the moment its cluster reconciles; every entry here is
    /// re-shadowed and flushed to `pending_rows` as soon as either ANOTHER
    /// range tombstone's complete entry arrives (it might affect an entry
    /// held from before we knew about it) or the partition ends. Bounds
    /// peak memory to "rows accumulated since the last flush point" for the
    /// common case of narrow/occasional range tombstones, degrading toward
    /// whole-partition width only for a tombstone spanning nearly the whole
    /// partition — the documented residual (see the module doc and the
    /// linked follow-up issue).
    awaiting_flush: VecDeque<MergeEntry>,
    /// Set once [`StreamingMerger::flush_range_tombstones`] has re-emitted
    /// every range tombstone known so far as its own marker entry. Issue
    /// #1668 stage 5d: a range tombstone's complete entry can arrive at any
    /// point relative to the rows it covers (see `awaiting_flush`'s doc), so
    /// this marker re-emission is deferred to a SINGLE point — partition end
    /// — where the FULL, final set is known and can be coalesced exactly
    /// once (coalescing a partial set early, then coalescing again later,
    /// risks re-emitting overlapping marker pairs — roborev #959 High #1's
    /// concern — if a later-discovered range overlaps an already-emitted
    /// one). This flag makes that single call idempotent across the
    /// `step_streaming` loop's repeated visits to the exhausted-partition
    /// branch.
    range_tombstones_emitted: bool,
}

#[cfg(feature = "write-support")]
impl PartitionReconcileCheckpoint {
    fn fresh() -> Self {
        Self {
            pending_rows: VecDeque::new(),
            range_tombstones: Vec::new(),
            max_partition_deletion: None,
            partition_delete_key: None,
            partition_deletion_resolved: false,
            current_cluster: None,
            purges: PurgeCounts::default(),
            awaiting_flush: VecDeque::new(),
            range_tombstones_emitted: false,
        }
    }
}

/// Feature-internal streaming wrapper around [`KWayMerger`] (issue #1668,
/// stages 2-5d) — reconciles ONE clustering-key group at a time DIRECTLY off
/// `merger`'s heap (see the module doc's "two independent accumulation
/// dimensions" section for the full design and its correctness proof).
///
/// Holds ONLY its own drain/reconciliation state; it does not add fields to
/// `KWayMerger` itself, so the existing `KWayMerger { .. }` struct-literal
/// unit tests in `mod.rs` are unaffected by this module's changes.
#[cfg(feature = "write-support")]
pub(crate) struct StreamingMerger<'a> {
    merger: &'a mut KWayMerger,
    partition_key: Option<DecoratedKey>,
    state: PartitionReconcileCheckpoint,
}

#[cfg(feature = "write-support")]
impl<'a> StreamingMerger<'a> {
    /// Wrap a [`KWayMerger`] for streaming cluster-group increments.
    pub(crate) fn new(merger: &'a mut KWayMerger) -> Self {
        Self {
            merger,
            partition_key: None,
            state: PartitionReconcileCheckpoint::fresh(),
        }
    }

    /// Resume (or start fresh) draining, seeded with a PREVIOUSLY paused
    /// partition's full reconciliation checkpoint (issue #1668, stage 4;
    /// checkpoint shape widened in stage 5d — see
    /// [`PartitionReconcileCheckpoint`]).
    ///
    /// `StreamingMerger` cannot itself be stored across calls — it borrows
    /// `&'a mut KWayMerger`, so it is not self-referential-storable on a
    /// struct that also owns that `KWayMerger` (e.g. `ActiveMerge`). A
    /// caller that must pause mid-partition (the "Q4" mid-partition budget
    /// check) instead persists the OWNED `(key, checkpoint)` extracted via
    /// [`Self::into_paused_state`], then reconstructs a fresh
    /// `StreamingMerger` next call via THIS constructor, seeded with that
    /// saved state. `saved: None` starts fresh, identical to [`Self::new`].
    ///
    /// Nothing is re-computed and nothing is lost: the checkpoint carries
    /// every raw entry already popped off the heap for `key`'s partition but
    /// not yet fully reconciled (the in-progress cluster's raw rows, the
    /// carrier accumulator) PLUS every already-reconciled, not-yet-yielded
    /// row — the heap itself has moved past them, so resuming via this
    /// checkpoint (instead of starting fresh and re-scanning) is required
    /// for correctness, not just an optimization.
    pub(crate) fn resume(
        merger: &'a mut KWayMerger,
        saved: Option<(DecoratedKey, PartitionReconcileCheckpoint)>,
    ) -> Self {
        let (partition_key, state) = match saved {
            Some((key, checkpoint)) => (Some(key), checkpoint),
            None => (None, PartitionReconcileCheckpoint::fresh()),
        };
        Self {
            merger,
            partition_key,
            state,
        }
    }

    /// Extract the in-progress partition's full reconciliation checkpoint
    /// (issue #1668, stage 4/5d), for a caller that must pause mid-partition
    /// and resume later via [`Self::resume`]. Returns `None` if no partition
    /// was in progress (nothing to resume — the next call should start fresh
    /// via [`Self::new`]/`resume(merger, None)`).
    pub(crate) fn into_paused_state(self) -> Option<(DecoratedKey, PartitionReconcileCheckpoint)> {
        self.partition_key.map(|key| (key, self.state))
    }

    /// Pop one raw entry belonging to the CURRENT partition
    /// (`self.partition_key`, which must already be `Some`) off the heap,
    /// refilling from its run — the SAME peek/pop/refill mechanism
    /// `KWayMerger::step` uses, just triggered one entry at a time instead
    /// of in a bulk `while let` loop. Returns `None` if the heap is empty or
    /// its next entry belongs to a DIFFERENT partition — i.e. the current
    /// partition has no more entries.
    fn pull_one(&mut self) -> Result<Option<MergeEntry>> {
        let Some(current_key) = &self.partition_key else {
            return Ok(None);
        };
        match self.merger.heap.peek() {
            Some(Reverse(top)) if &top.entry.key == current_key => {}
            _ => return Ok(None),
        }
        let Reverse(top) = self
            .merger
            .heap
            .pop()
            .ok_or_else(|| Error::InvalidInput("Merge heap unexpectedly empty".to_string()))?;
        let entry = top.entry;
        self.merger.refill_heap(entry.run_index)?;
        Ok(Some(entry))
    }

    /// Resolve the partition-tombstone half of the carrier accumulator
    /// EXACTLY ONCE per partition, when the bounded `clustering_key: None`
    /// PREFIX ends (see the module doc's "load-bearing precondition"
    /// section: the partition-level tombstone is always part of the on-disk
    /// header, so it is FULLY known by the time any non-carrier entry
    /// arrives). Re-emits the partition tombstone (gc/overlap-purge-gated)
    /// into `pending_rows` — mirrors `merge_partition_rows`'s equivalent
    /// re-emission, just triggered at the moment it is first NEEDED instead
    /// of unconditionally after the whole partition is buffered.
    ///
    /// Deliberately does NOT also flush range tombstones here (issue #1668
    /// stage 5d): a range tombstone's complete entry can arrive well after
    /// this point (see `awaiting_flush`'s doc), so ALL range-tombstone
    /// marker re-emission is deferred to the single partition-end call —
    /// see `range_tombstones_emitted`'s doc for why.
    fn resolve_partition_prefix(&mut self) {
        if self.state.partition_deletion_resolved {
            return;
        }
        self.state.partition_deletion_resolved = true;

        let (effective_gc_before, max_purgeable_timestamp) = self.merger.effective_gc_settings();

        if let (Some((pmfda, pldt)), Some(key)) = (
            self.state.max_partition_deletion,
            self.state.partition_delete_key.take(),
        ) {
            let purge = match effective_gc_before {
                Some(gc_before) => {
                    i64::from(pldt as u32) < gc_before && pmfda < max_purgeable_timestamp
                }
                None => false,
            };
            if purge {
                self.state.purges.partition_tombstones += 1;
            } else {
                self.state.purges.emitted += 1;
                self.state.pending_rows.push_back(
                    MergeEntry::new(
                        usize::MAX,
                        key,
                        None,
                        pmfda,
                        RowData::Tombstone {
                            deletion_time: pmfda,
                            local_deletion_time: pldt,
                        },
                    )
                    .with_partition_deletion((pmfda, pldt)),
                );
            }
        }
    }

    /// Coalesce `range_tombstones`, drop any subsumed by the partition
    /// floor, gc/overlap-purge-gate every survivor, and re-emit the
    /// retained markers into `pending_rows` — the marker-persistence half
    /// of what `merge_partition_rows` does. Called EXACTLY ONCE per
    /// partition, at partition end (guarded by `range_tombstones_emitted`
    /// in the `step_streaming` caller), once the FULL, final set of range
    /// tombstones is known — coalescing (and therefore re-emitting) a
    /// partial set here and a later-discovered remainder in a SECOND call
    /// would risk two marker pairs overlapping in the output (roborev #959
    /// High #1's concern).
    fn flush_range_tombstones(
        &mut self,
        effective_gc_before: Option<i64>,
        max_purgeable_timestamp: i64,
    ) {
        self.state.range_tombstones_emitted = true;
        KWayMerger::coalesce_range_tombstones(
            &mut self.state.range_tombstones,
            &self.merger.schema,
        );

        if let Some((pmfda, _)) = self.state.max_partition_deletion {
            self.state
                .range_tombstones
                .retain(|(_, rt)| rt.deletion_time > pmfda);
        }

        for (key, rt) in self.state.range_tombstones.clone() {
            if let Some(gc_before) = effective_gc_before {
                if i64::from(rt.local_deletion_time as u32) < gc_before
                    && rt.deletion_time < max_purgeable_timestamp
                {
                    self.state.purges.range_tombstones += 1;
                    continue;
                }
            }
            self.state.purges.emitted += 1;
            self.state.pending_rows.push_back(
                MergeEntry::new(
                    usize::MAX,
                    key,
                    None,
                    rt.deletion_time,
                    RowData::Live { cells: Vec::new() },
                )
                .with_range_deletion(rt),
            );
        }
    }

    /// Re-apply range- then partition-shadowing (using EVERY range
    /// tombstone known so far) to everything currently held in
    /// `awaiting_flush`, then move every survivor into `pending_rows`
    /// (issue #1668 stage 5d's bounded-open-range design — see
    /// `awaiting_flush`'s field doc and the module doc's finding on why a
    /// range tombstone's complete entry can arrive strictly after every row
    /// it covers has already been popped off the heap).
    fn reshadow_and_flush_awaiting(&mut self) {
        if self.state.awaiting_flush.is_empty() {
            return;
        }
        KWayMerger::coalesce_range_tombstones(
            &mut self.state.range_tombstones,
            &self.merger.schema,
        );
        for entry in std::mem::take(&mut self.state.awaiting_flush) {
            if let Some(shadowed) = KWayMerger::apply_range_shadowing(
                entry,
                &self.state.range_tombstones,
                &self.merger.schema,
            ) {
                if let Some(survivor) = KWayMerger::apply_partition_shadowing(
                    shadowed,
                    self.state.max_partition_deletion,
                ) {
                    self.state.pending_rows.push_back(survivor);
                }
            }
        }
    }

    /// Finalize whatever cluster is currently accumulating (if any):
    /// reconcile its raw rows, apply range- then partition-shadowing, and
    /// route the survivor to `pending_rows` (zero extra buffering — the
    /// common case, when no range tombstone has been seen for this
    /// partition YET) or `awaiting_flush` (issue #1668 stage 5d's
    /// bounded-open-range design: held because a range tombstone HAS been
    /// seen, so a DIFFERENT/later one might still need to shadow this
    /// survivor too). A no-op beyond resolving the partition-tombstone
    /// prefix if no cluster is in progress (the tombstone-only or
    /// truly-empty partition cases).
    fn finalize_current_cluster(&mut self) -> Result<()> {
        let Some((ck, rows)) = self.state.current_cluster.take() else {
            self.resolve_partition_prefix();
            return Ok(());
        };
        self.resolve_partition_prefix();

        let (effective_gc_before, max_purgeable_timestamp) = self.merger.effective_gc_settings();
        if let Some(entry) = KWayMerger::reconcile_cluster_with_overlap_counted(
            ck,
            rows,
            &self.merger.schema.dropped_columns,
            effective_gc_before,
            max_purgeable_timestamp,
            self.merger.now_secs,
            &mut self.state.purges,
        ) {
            if self.state.range_tombstones.is_empty() {
                // Common case: nothing has EVER shadowed anything in this
                // partition so far — partition-shadowing alone still
                // applies (it resolves fully during the prefix, unlike
                // range tombstones), then flush straight through with zero
                // extra buffering.
                if let Some(survivor) =
                    KWayMerger::apply_partition_shadowing(entry, self.state.max_partition_deletion)
                {
                    self.state.pending_rows.push_back(survivor);
                }
            } else {
                self.state.awaiting_flush.push_back(entry);
            }
        }
        Ok(())
    }

    /// Yield the next streaming increment.
    ///
    /// Drains any already-reconciled, ready-to-emit rows first; otherwise
    /// pulls raw entries directly off the heap, siphoning carriers into the
    /// running accumulator and growing the current clustering-key group,
    /// until either a reconciled row becomes available, the partition ends
    /// (`PartitionEnd`), or the whole merge ends (`Complete`). See the
    /// module doc for the full design and its correctness proof.
    pub(crate) fn step_streaming(&mut self) -> Result<StreamingStep> {
        loop {
            if let Some(row) = self.state.pending_rows.pop_front() {
                let Some(key) = self.partition_key.clone() else {
                    return Err(Error::InvalidInput(
                        "pending row without an active partition key".to_string(),
                    ));
                };
                return Ok(StreamingStep::ClusterGroup {
                    key,
                    row: Box::new(row),
                });
            }

            if self.partition_key.is_none() {
                // Lazy heap init on first use — mirrors `KWayMerger::step`'s
                // OWN check exactly (issue #1668 stage 5d: `step_streaming`
                // no longer delegates to `step`, so it must replicate this
                // one-time setup itself).
                if self.merger.heap.is_empty() && self.merger.current_partition.is_none() {
                    self.merger.initialize_heap()?;
                }
                let Some(Reverse(top)) = self.merger.heap.peek() else {
                    return Ok(StreamingStep::Complete);
                };
                self.partition_key = Some(top.entry.key.clone());
                self.state = PartitionReconcileCheckpoint::fresh();
            }

            match self.pull_one()? {
                Some(entry) => {
                    // Carriers are siphoned off REGARDLESS of any in-progress
                    // cluster (issue #1668 stage 5d finding: a carrier can
                    // share a run_index tie-break with — and interleave
                    // arbitrarily with — the current cluster for an
                    // unclustered table's sole `ck: None` group).
                    if entry.is_partition_delete_carrier() {
                        if let Some((mfda, ldt)) = entry.partition_deletion {
                            self.state
                                .partition_delete_key
                                .get_or_insert_with(|| entry.key.clone());
                            match self.state.max_partition_deletion {
                                Some((cur_mfda, _)) if cur_mfda >= mfda => {}
                                _ => self.state.max_partition_deletion = Some((mfda, ldt)),
                            }
                            continue;
                        }
                    }
                    if carriers::is_range_marker_carrier(&entry) {
                        if let Some(rt) = entry.range_deletion.clone() {
                            self.state.range_tombstones.push((entry.key.clone(), rt));
                            // Issue #1668 stage 5d: a range tombstone's
                            // COMPLETE entry can arrive strictly AFTER the
                            // bounded prefix (the reader only surfaces it
                            // once its close bound is parsed — see the
                            // module doc's finding). Once the prefix has
                            // already resolved, this is a NEW mid-partition
                            // discovery: immediately re-check every row
                            // held in `awaiting_flush` against it (and
                            // every range tombstone seen so far) rather
                            // than waiting — bounding the buffer to "since
                            // the last flush point," not the whole
                            // partition.
                            if self.state.partition_deletion_resolved {
                                self.reshadow_and_flush_awaiting();
                            }
                        }
                        continue;
                    }

                    // Not a carrier: belongs to a clustering-key group.
                    let starts_new_cluster = match &self.state.current_cluster {
                        Some((ck, _)) => *ck != entry.clustering_key,
                        None => false,
                    };
                    if starts_new_cluster {
                        self.finalize_current_cluster()?;
                    }
                    match &mut self.state.current_cluster {
                        Some((_, rows)) => rows.push(entry),
                        None => {
                            self.state.current_cluster =
                                Some((entry.clustering_key.clone(), vec![entry]));
                        }
                    }
                }
                None => {
                    // The current partition is exhausted (pulling again is
                    // safe and cheap — the heap's `peek()` stays exhausted
                    // for this partition, and `finalize_current_cluster` is
                    // idempotent once `current_cluster` has already been
                    // taken — issue #1668 stage 5d note): finalize the last
                    // cluster (if any) and resolve any never-needed
                    // carriers, then flush whatever `awaiting_flush` still
                    // holds — the partition is DEFINITELY ending, so this
                    // is the final, authoritative flush regardless of
                    // whether ANOTHER range tombstone ever showed up to
                    // trigger an earlier one. If any of this produced NEW
                    // ready rows, loop back to drain them (via step 1,
                    // `partition_key` untouched) before ending the
                    // partition. Only once finalization truly yields
                    // nothing further do we emit the purge-counter
                    // observability EXACTLY ONCE and signal PartitionEnd.
                    self.finalize_current_cluster()?;
                    self.reshadow_and_flush_awaiting();
                    if !self.state.range_tombstones.is_empty()
                        && !self.state.range_tombstones_emitted
                    {
                        let (effective_gc_before, max_purgeable_timestamp) =
                            self.merger.effective_gc_settings();
                        self.flush_range_tombstones(effective_gc_before, max_purgeable_timestamp);
                    }
                    if !self.state.pending_rows.is_empty() {
                        continue;
                    }
                    let purged = self.state.purges.total();
                    if purged > 0 {
                        crate::observability::add_counter(
                            crate::observability::catalog::COMPACTION_TOMBSTONES_PURGED,
                            purged,
                            &[],
                        );
                    }
                    let Some(key) = self.partition_key.take() else {
                        return Err(Error::InvalidInput(
                            "partition ended without an active key".to_string(),
                        ));
                    };
                    return Ok(StreamingStep::PartitionEnd { key });
                }
            }
        }
    }
}

#[cfg(all(test, feature = "write-support"))]
mod tests {
    use super::*;
    // `MergeStep` is only exercised by these tests' `drain_whole_partition`
    // oracle (the unchanged `KWayMerger::step` reference path) — not by any
    // non-test code in this module (issue #1668 stage 5d removed the
    // production `step()` delegation from `step_streaming`).
    use super::super::MergeStep;
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

    /// A RAW range-tombstone carrier, as a real reader would construct it
    /// (`KWayMerger::build_merge_entry`'s `CompactionRowData::RangeMarker`
    /// branch) — tagged with the run it ACTUALLY came from, `run_index`,
    /// NOT the `usize::MAX` sentinel `merge_partition_rows`'s OWN
    /// re-emission logic uses for its SYNTHETIC output entries (issue #1668
    /// stage 5d finding: a raw carrier's `run_index` field must match its
    /// real `self.runs` slot, or `KWayMerger::refill_heap` — which indexes
    /// `self.runs` by the popped entry's OWN `run_index` field — silently
    /// stops refilling that run after this carrier is popped, exactly as a
    /// genuine multi-entry run would starve if mis-tagged this way. The
    /// PRE-stage-5d `step()` masked this with an unrelated bug of its own:
    /// `current_partition` is never actually assigned, so `step()`'s "lazy
    /// init" guard (`heap.is_empty() && current_partition.is_none()`) is
    /// unconditionally true whenever the heap empties, re-triggering
    /// `initialize_heap()` — which happens to "rescue" a stalled run by
    /// force-refilling it — on EVERY subsequent `step()` call, silently
    /// splitting one logical partition into extra `MergeStep::Partition`
    /// steps that `drain_whole_partition` then concatenates back together.
    /// `step_streaming` has no such accidental rescue (by design — one
    /// partition is one cohesive state machine, not repeated fresh
    /// `step()` calls), so a mis-tagged carrier's absence is no longer
    /// masked.
    fn range_carrier(run_index: usize, token: i64, deletion_time: i64, ldt: i32) -> MergeEntry {
        let rt = RangeTombstone {
            start: crate::storage::write_engine::mutation::ClusteringBound::Bottom,
            end: crate::storage::write_engine::mutation::ClusteringBound::Inclusive(ck(1)),
            deletion_time,
            local_deletion_time: ldt,
        };
        MergeEntry::new(
            run_index,
            key(token),
            None,
            deletion_time,
            RowData::Live { cells: Vec::new() },
        )
        .with_range_deletion(rt)
    }

    /// A RAW partition-tombstone carrier — see [`range_carrier`]'s doc for
    /// why `run_index` must be the entry's REAL run, not `usize::MAX`.
    fn partition_carrier(run_index: usize, token: i64, mfda: i64, ldt: i32) -> MergeEntry {
        MergeEntry::new(
            run_index,
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
        merger_over_runs(vec![entries], schema)
    }

    /// Build a `KWayMerger` with ONE run PER `Vec<MergeEntry>` in `runs` —
    /// for tests that must exercise the heap's REAL cross-run interleaving
    /// (issue #1668 stage 5d), not just a single run's own (already
    /// necessarily monotonic) content. Each inner `Vec` must already be in
    /// its OWN correct on-disk order — real SSTable files are always
    /// written via the schema-aware sort `write_partition` applies before
    /// writing (`ClusteringKey::compare`, which DOES reverse `DESC`
    /// columns), so a genuine `RunReader` for one file never needs
    /// re-sorting; only the heap's job of picking the next-smallest
    /// candidate ACROSS runs remains.
    fn merger_over_runs(runs: Vec<Vec<MergeEntry>>, schema: TableSchema) -> KWayMerger {
        KWayMerger {
            runs: runs
                .into_iter()
                .map(|entries| RunReader::new(Box::new(VecIterator(entries.into_iter()))))
                .collect(),
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

    /// Split a drained partition's rows into (carrier entries, clustering
    /// rows) — `clustering_key.is_none()` marks a carrier (a range- or
    /// partition-tombstone re-emission), matching `merge_partition_rows`'s
    /// own `clustered_rows[None]` grouping.
    fn split_carriers_and_rows(rows: Vec<MergeEntry>) -> (Vec<MergeEntry>, Vec<MergeEntry>) {
        rows.into_iter()
            .partition(|entry| entry.clustering_key.is_none())
    }

    /// A stable sort key for a carrier entry, independent of WHEN it was
    /// re-emitted — distinguishes a range-tombstone marker from a
    /// partition-tombstone marker (this fixture has at most one of each),
    /// so two carrier sets can be compared as sets regardless of order.
    fn carrier_sort_key(entry: &MergeEntry) -> (bool, i64) {
        (entry.partition_deletion.is_some(), entry.timestamp)
    }

    /// THE proof stage 3/5d needs: for a fixture mixing plain rows, a row
    /// tombstone, a range-tombstone carrier (#933), and a partition-deletion
    /// carrier (#1072) — i.e. exercising stage-1's `scan_partition_carriers`
    /// end to end — the streaming path reconciles the SAME rows, with the
    /// SAME shadowing outcome, as the old whole-partition path.
    ///
    /// Unlike the plain-fixture / DESC / absent-component tests, this test
    /// does NOT assert byte-identical SEQUENCE order: issue #1668 stage 5d's
    /// bounded-open-range design (see the module doc) defers a range
    /// tombstone's own marker re-emission to partition end so the FULL,
    /// final set can be coalesced exactly once, so a range marker can land
    /// AFTER the clustering rows it shadows in the streaming path, whereas
    /// `merge_partition_rows`'s whole-partition buffering always sorts
    /// EVERY carrier before EVERY clustering row. The two invariants that
    /// DO still hold — and that this test asserts — are: (a) the clustering
    /// ROWS appear in the identical order in both paths (grouping/
    /// reconciliation correctness is unaffected), and (b) the SET of
    /// carrier entries (their content, not their position) is identical.
    #[test]
    fn step_streaming_matches_step_for_mixed_tombstone_fixture() {
        let schema = test_schema(ClusteringOrder::Asc);
        // Realistic ordering (issue #1668 stage 5d finding): the partition
        // tombstone is always part of the on-disk header, so it precedes
        // every row; the range tombstone [Bottom, ck=1] closes right after
        // the last row it covers (ck=1), before the row it does NOT cover
        // (ck=2) — matching how the real reader pairs open/close bounds
        // (`row_decoder/compaction.rs`'s `on_range_marker`/`on_data_row`).
        let entries = vec![
            partition_carrier(0, 1, 10, 1),
            live_entry(0, 1, 0, 100),
            live_entry(0, 1, 1, 100),
            range_carrier(0, 1, 50, 5),
            live_entry(0, 1, 2, 100),
        ];

        let mut old_merger = merger_over(entries.clone(), schema.clone());
        let old_rows = drain_whole_partition(&mut old_merger);

        let mut new_merger = merger_over(entries, schema);
        let new_rows = drain_streaming(&mut new_merger);

        assert!(
            !old_rows.is_empty(),
            "fixture must produce at least one row"
        );

        let (mut old_carriers, old_clustering_rows) = split_carriers_and_rows(old_rows);
        let (mut new_carriers, new_clustering_rows) = split_carriers_and_rows(new_rows);
        assert_eq!(
            old_clustering_rows, new_clustering_rows,
            "streaming path must reconcile the SAME clustering rows, in the \
             SAME order, as the whole-partition path"
        );
        old_carriers.sort_by_key(carrier_sort_key);
        new_carriers.sort_by_key(carrier_sort_key);
        assert_eq!(
            old_carriers, new_carriers,
            "streaming path must re-emit the SAME carrier markers (content, \
             not necessarily position) as the whole-partition path"
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

    /// Stage 3a/5d correctness test: a `DESC` clustering column, across
    /// GENUINELY SEPARATE runs (issue #1668 stage 5d finding — see
    /// `merger_over_runs`'s doc: a REAL SSTable file is always written in
    /// schema-consistent order via `write_partition`'s own sort, so a
    /// single run's entries never need reordering; the property THIS test
    /// exercises is the HEAP correctly choosing, across MULTIPLE runs
    /// (`ck=2`'s run, `ck=1`'s run, `ck=0`'s run — each independently
    /// correctly "sorted" since it holds a single entry), the Cassandra-
    /// correct DESCENDING order (2, 1, 0), which the FALLBACK
    /// (non-schema-aware) `Ord` would get backwards. Assert BOTH `step()`
    /// and `step_streaming()` emit the CORRECT descending order — an
    /// independent check against the expected sequence, not just
    /// old-path-equals-new-path (which would pass even if both were
    /// equally wrong).
    #[test]
    fn step_streaming_matches_step_for_desc_clustering_fixture() {
        let schema = test_schema(ClusteringOrder::Desc);
        let runs = vec![
            vec![live_entry(0, 1, 2, 100)],
            vec![live_entry(1, 1, 1, 100)],
            vec![live_entry(2, 1, 0, 100)],
        ];
        let expected_order = [2, 1, 0];

        let mut old_merger = merger_over_runs(runs.clone(), schema.clone());
        let old_rows = drain_whole_partition(&mut old_merger);
        let old_order: Vec<i32> = old_rows.iter().map(ck_value).collect();
        assert_eq!(
            old_order, expected_order,
            "step() must emit DESC clustering order"
        );

        let mut new_merger = merger_over_runs(runs, schema);
        let new_rows = drain_streaming(&mut new_merger);
        let new_order: Vec<i32> = new_rows.iter().map(ck_value).collect();
        assert_eq!(
            new_order, expected_order,
            "step_streaming() must ALSO emit DESC clustering order, matching step()"
        );
    }

    /// Stage 3a/5d correctness test: absent trailing clustering component
    /// (Cassandra NULL-first rule) combined with a `DESC` second column,
    /// across GENUINELY SEPARATE runs (see the DESC test's doc for why —
    /// same finding). Schema-aware order: the absent-`ck2` row sorts FIRST
    /// (NULL always sorts first, regardless of `ck2`'s `DESC`-ness), then
    /// among the two present-`ck2` rows the LARGER value sorts first
    /// (`DESC` reversal). Expected sequence: `[absent, ck2=2, ck2=1]`.
    #[test]
    fn step_streaming_matches_step_for_absent_trailing_component_fixture() {
        let schema = two_col_schema();
        let runs = vec![
            vec![live_entry_ck(
                0,
                1,
                ck_multi(&[("ck1", 5), ("ck2", 1)]),
                100,
            )],
            vec![live_entry_ck(1, 1, ck_multi(&[("ck1", 5)]), 100)], // absent ck2
            vec![live_entry_ck(
                2,
                1,
                ck_multi(&[("ck1", 5), ("ck2", 2)]),
                100,
            )],
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

        let mut old_merger = merger_over_runs(runs.clone(), schema.clone());
        let old_rows = drain_whole_partition(&mut old_merger);
        let old_order: Vec<Option<i32>> = old_rows.iter().map(ck2_marker).collect();
        assert_eq!(
            old_order, expected_order,
            "step() must put the absent-ck2 row first (NULL-first), then DESC by ck2"
        );

        let mut new_merger = merger_over_runs(runs, schema);
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
    /// the increment API unchanged). The re-emitted entry matches the raw
    /// input carrier in every field EXCEPT `run_index`: both the old
    /// (`merge_partition_rows`) and new (`flush_range_tombstones`) paths
    /// always reconstruct the re-emitted marker with `run_index: usize::MAX`
    /// (it's a synthetic entry, not read from any particular run) —
    /// `range_carrier`'s OWN `run_index` (its REAL run, since #1668 stage 5d
    /// fixed this fixture to match `build_merge_entry`'s convention) is
    /// deliberately different, so a literal round-trip was never the real
    /// contract.
    #[test]
    fn range_tombstone_carrier_round_trips_as_cluster_group() {
        let carrier = range_carrier(0, 9, 500, 50);
        assert!(super::super::carriers::is_range_marker_carrier(&carrier));
        let expected = MergeEntry {
            run_index: usize::MAX,
            ..carrier.clone()
        };

        let mut merger = merger_over(vec![carrier], test_schema(ClusteringOrder::Asc));
        let mut stream = StreamingMerger::new(&mut merger);
        match stream.step_streaming().unwrap() {
            StreamingStep::ClusterGroup { row, .. } => assert_eq!(*row, expected),
            other => panic!("expected ClusterGroup, got {other:?}"),
        }
    }

    /// Issue #1668, stage 5c-iv design verification (NOT yet a stage
    /// deliverable — a load-bearing precondition check before building the
    /// incremental writer entry point). Confirms CONCRETELY, through the
    /// real production `step()`, that a partition's `clustering_key: None`
    /// entry (the reconciled static row — `merge_partition_rows` groups it
    /// into `clustered_rows[None]` alongside any other `None`-keyed carrier,
    /// same as a range/partition-tombstone re-emission) ALWAYS sorts FIRST
    /// in `MergeStep::Partition.rows`, before every `Some(ck)` clustering
    /// row — regardless of how many regular rows the partition has. This
    /// means the static row (and any range/partition-tombstone carriers) sit
    /// in a SMALL, PARTITION-SIZE-INDEPENDENT prefix, decoupled from the
    /// (potentially huge) clustering-row tail — the key fact the incremental
    /// writer design leans on to avoid buffering the whole partition just to
    /// resolve statics.
    #[test]
    fn static_row_carrier_always_sorts_first_regardless_of_partition_width() {
        use crate::schema::Column;
        use crate::storage::write_engine::merge::model::CellData;

        let mut schema = test_schema(ClusteringOrder::Asc);
        schema.columns = vec![Column {
            name: "region".to_string(),
            data_type: "text".to_string(),
            nullable: true,
            default: None,
            is_static: true,
        }];

        // A static-row carrier (clustering_key: None, cells target the
        // static column) PLUS a wide tail of 50 regular clustering rows.
        let mut entries = vec![MergeEntry::new(
            0,
            key(1),
            None,
            100,
            RowData::Live {
                cells: vec![CellData::new(
                    "region".to_string(),
                    Value::Text("us-east".to_string()),
                    100,
                )],
            },
        )];
        for ck in 0..50 {
            entries.push(live_entry(0, 1, ck, 100));
        }

        let mut merger = merger_over(entries, schema);
        let rows = match merger.step().unwrap() {
            MergeStep::Partition { rows, .. } => rows,
            other => panic!("expected Partition, got {other:?}"),
        };

        assert_eq!(
            rows.len(),
            51,
            "expected the static row + 50 clustering rows"
        );
        assert!(
            rows[0].clustering_key.is_none(),
            "the static row (clustering_key: None) must be FIRST, regardless \
             of the 50 regular clustering rows that follow"
        );
        assert!(
            rows[1..].iter().all(|r| r.clustering_key.is_some()),
            "every row AFTER the first must be a regular Some(ck) clustering \
             row — the None-keyed prefix is exactly one entry wide here"
        );
    }
}
