//! Clustering-group reconciliation kernel (issue #945 decomposition).
//!
//! `KWayMerger::<NoTrace>::reconcile_cluster_with_overlap_counted` used to be a single
//! ~470-line function. This module hosts the same logic decomposed into named,
//! separately testable steps threaded through a [`ReconcileState`] accumulator.
//! The orchestrator in `mod.rs` calls the steps **in a fixed, parity-load-bearing
//! order** — see [`ReconcileState`] for the contract. This is a pure refactor:
//! the per-step bodies are lifted verbatim from the original `// Step N` blocks,
//! so compaction output (and the #1037 purge tally) is byte-for-byte identical.
//!
//! ## Step order (do not reorder — parity-critical, Cassandra `f66fa14f`)
//!
//! 1. [`fold_row_deletions`](ReconcileState::fold_row_deletions) — Step 1:
//!    effective row deletion (max `deletion_time` + paired LDT), carried
//!    complex/range deletions, key/run_index.
//! 2. [`resolve_cell_winners`](ReconcileState::resolve_cell_winners) — Step 2:
//!    per-`(column, cell_path)` last-write-wins winner resolution.
//! 3. [`apply_complex_deletions`](ReconcileState::apply_complex_deletions) —
//!    Step 2b: strict-supersede + shadow-before-purge. MUST run before the
//!    row-tombstone / gc-grace filters so a surviving complex deletion cannot
//!    resurrect a covered element on a later purge.
//! 4. [`shadow_by_row_deletion`](ReconcileState::shadow_by_row_deletion) —
//!    Step 3: drop cells with `ts <= row_del`.
//! 5. [`filter_dropped_columns`](ReconcileState::filter_dropped_columns) —
//!    Step 3b: drop cells `ts <= drop_time`, capturing the pre-purge
//!    `had_data_before` phantom-row guard state.
//! 5b. [`expire_ttl_cells`](ReconcileState::expire_ttl_cells) — Step 3b′
//!    (#1382): turn each expired live TTL cell (`localDeletionTime < now_secs`)
//!    into a cell tombstone so Step 3c purges it under the same gc/overlap gate.
//! 6. [`purge_gc_grace`](ReconcileState::purge_gc_grace) — Step 3c: drop
//!    purgeable tombstones (overlap-safe), tallying genuine purges (#1037).
//! 7. [`build`](ReconcileState::build) — Step 4: phantom-row guard + emit the
//!    merged `MergeEntry`.

use std::collections::{HashMap, HashSet};

use crate::storage::write_engine::mutation::{ClusteringKey, DecoratedKey, RangeTombstone};

use super::trace::{CellDecision, DecidedBy, NoTrace, TombstoneRecord, TraceSink, Verdict};
use super::{CellData, ComplexDeletion, KWayMerger, MergeEntry, PurgeCounts, RowData};

// Reconciliation Steps 1-3b (fold, cell winners, complex-deletion
// shadow-before-purge, row-tombstone shadowing, dropped-column filtering).
// Split out for the campsite file-size rule (issue #4193, design.md §D5) —
// pure relocation, no behavior change.
mod steps;

// Issue #1537: parity coverage that TTL expiry applies to complex/collection/UDT
// elements (not only simple cells). Kept in its own file so `reconcile.rs` stays
// within the campsite size target.
#[cfg(test)]
mod ttl_complex_tests;

// Issue #2374/#2789: direct coverage of the cross-generation row-marker liveness
// FOLD (Step 1) through the REAL `ReconcileState` add/consume path — the
// timestamp-LWW winner carried onto the emitted entry. Only the end-to-end Flight
// parity lane exercised this fold, and it SKIPs when fixtures are absent.
#[cfg(test)]
mod row_liveness_fold_tests;

/// Per-cell reconcile key: `(column, cell_path)` so each element of a multi-cell
/// column reconciles independently (epic #899). Simple cells have
/// `cell_path == None`.
pub(super) type CellKey = (String, Option<Vec<u8>>);

/// Trace-only source metadata that must survive the reconciled `MergeEntry`
/// boundary. `MergeEntry` intentionally stays unchanged; the outer range and
/// partition shadowing stages still need the original per-cell generation when
/// they emit the final winner/loser records.
pub(super) struct ReconcileTraceMetadata {
    pub(super) winner_runs: Option<HashMap<CellKey, usize>>,
}

/// In-flight working set for reconciling one clustering-key group.
///
/// Built by [`Self::new`], then mutated through the step methods in the fixed
/// order documented at the module level. The load-bearing invariants the
/// original ~470-line function depended on are preserved:
///
/// - **Step 2b before Step 3/3c** — a surviving complex deletion shadows its
///   covered elements before any marker is purged (`f66fa14f`).
/// - **`had_data_before` captured pre-purge** — recorded in
///   [`Self::filter_dropped_columns`] (after row-tombstone + dropped-column
///   filtering) and consulted in [`Self::build`] AFTER the gc-grace purge, so a
///   clustered row whose only non-key data is a purgeable cell tombstone is
///   recognized as data-less and emits nothing instead of a phantom key-only
///   live row (#921 finding 3).
pub(super) struct ReconcileState<S: TraceSink = NoTrace> {
    /// Clustering key shared by every entry in the group (carry-through).
    clustering_key: Option<ClusteringKey>,
    /// Partition key, taken from the first entry seen.
    key: Option<DecoratedKey>,
    /// Lowest run_index seen (newest file) for stable downstream ordering.
    run_index: usize,
    /// Effective row deletion timestamp (`markedForDeleteAt`, micros).
    row_del: Option<i64>,
    /// `localDeletionTime` (GC-clock seconds) paired with the winning `row_del`.
    row_del_ldt: i32,
    /// First-seen cell-key order for deterministic output.
    order: Vec<CellKey>,
    /// Winning cell per `(column, cell_path)`.
    winners: HashMap<CellKey, CellData>,
    /// Carried complex (collection / UDT) deletion markers.
    complex_deletions: Vec<ComplexDeletion>,
    /// Carried range deletion (timestamp-max preserved).
    range_deletion: Option<RangeTombstone>,
    /// Cells surviving row-tombstone shadowing (Step 3 output).
    after_row_del: Vec<CellData>,
    /// Cells surviving dropped-column filtering (Step 3b output), then mutated
    /// by the gc-grace purge.
    surviving: Vec<CellData>,
    /// Whether the row held non-key data BEFORE the gc-grace purge (phantom-row
    /// guard, captured pre-purge).
    had_data_before: bool,
    /// Reconciled primary-key (row-marker) liveness across the cluster's entries
    /// (issue #2374/#2789), carry-only for the read path (Flight `do_get` / cross-
    /// gen read merge). Folded so the latest-expiry / live-forever marker wins.
    row_liveness: crate::storage::sstable::reader::compaction_row::RowLiveness,
    /// Source generation for the effective row tombstone, when one exists.
    row_del_run_index: Option<usize>,
    /// Source generation for current cell winners. This map is allocated only
    /// for an enabled sink; the default no-trace state retains the old shape's
    /// hot-path allocations.
    winner_runs: Option<HashMap<CellKey, usize>>,
    /// Source generation and timestamp for each active complex deletion. This
    /// is trace-only because the carried `ComplexDeletion` model predates the
    /// explain surface and intentionally has no run identity field.
    complex_deletion_sources: Option<HashMap<String, (i64, usize)>>,
    /// Cells already assigned a terminal trace verdict.
    emitted_cells: Option<HashSet<(CellKey, usize)>>,
    trace: S,
    trace_gc_before_secs: Option<i64>,
    trace_now_secs: Option<i64>,
}

impl ReconcileState<NoTrace> {
    /// Initialize an empty accumulator for the group sharing `clustering_key`.
    #[allow(dead_code)]
    pub(super) fn new(clustering_key: Option<ClusteringKey>) -> Self {
        Self::with_trace(clustering_key, NoTrace, None, None)
    }
}

impl<S: TraceSink> ReconcileState<S> {
    /// Initialize an accumulator with a statically-dispatched decision sink.
    pub(super) fn with_trace(
        clustering_key: Option<ClusteringKey>,
        trace: S,
        trace_gc_before_secs: Option<i64>,
        trace_now_secs: Option<i64>,
    ) -> Self {
        Self {
            clustering_key,
            key: None,
            run_index: usize::MAX,
            row_del: None,
            row_del_ldt: 0,
            order: Vec::new(),
            winners: HashMap::new(),
            complex_deletions: Vec::new(),
            range_deletion: None,
            after_row_del: Vec::new(),
            surviving: Vec::new(),
            had_data_before: false,
            row_liveness: crate::storage::sstable::reader::compaction_row::RowLiveness::default(),
            row_del_run_index: None,
            winner_runs: S::ENABLED.then(HashMap::new),
            complex_deletion_sources: S::ENABLED.then(HashMap::new),
            emitted_cells: S::ENABLED.then(HashSet::new),
            trace,
            trace_gc_before_secs,
            trace_now_secs,
        }
    }

    #[inline]
    fn emit_cell(
        &mut self,
        cell: &CellData,
        run_index: usize,
        verdict: Verdict,
        decided_by: DecidedBy,
    ) {
        if !S::ENABLED {
            return;
        }
        let key = (cell.column.clone(), cell.cell_path.clone());
        if let Some(emitted) = &mut self.emitted_cells {
            if !emitted.insert((key, run_index)) {
                return;
            }
        }
        self.trace.cell(CellDecision {
            run_index,
            clustering: self.clustering_key.clone(),
            column: cell.column.clone(),
            value: (!KWayMerger::<NoTrace>::is_cell_tombstone(cell) && !cell.is_deleted)
                .then(|| cell.value.clone()),
            writetime: cell.timestamp,
            ttl: cell.ttl.and_then(|ttl| i32::try_from(ttl).ok()),
            expires_at: cell.local_deletion_time.map(|ldt| i64::from(ldt as u32)),
            verdict,
            decided_by,
        });
    }

    #[inline]
    fn emit_tombstone(&mut self, tombstone: TombstoneRecord) {
        if S::ENABLED {
            self.trace.tombstone(tombstone);
        }
    }

    #[inline]
    fn tombstone_droppable(&self, ldt: i32) -> bool {
        self.trace_gc_before_secs
            .is_some_and(|gc| i64::from(ldt as u32) < gc)
    }

    /// True once an input entry has been folded (i.e. the group is non-empty).
    /// Mirrors the original `let key = key?` early-out: an empty group emits
    /// nothing.
    pub(super) fn has_key(&self) -> bool {
        self.key.is_some()
    }

    /// Step 3b′ — TTL EXPIRY (issue #1382, parity Cassandra `ExpiringCell`
    /// / `TTLExpiryTest`). Runs AFTER dropped-column filtering and BEFORE the
    /// gc-grace purge (Step 3c) so an expired cell is first turned into a cell
    /// tombstone and then purged by the SAME gc/overlap gate as any other cell
    /// tombstone — matching Cassandra, which treats an expiring cell whose
    /// `localDeletionTime` is past as a tombstone with `localDeletionTime =`
    /// the expiry instant and the cell's own `markedForDeleteAt` unchanged.
    ///
    /// NO-HEURISTICS (#28): a cell is "expiring" ONLY when the reader surfaced
    /// its authoritative expiry metadata — BOTH `ttl` (Some) AND
    /// `local_deletion_time` (Some) — from the on-disk cell (never inferred
    /// from the value's byte shape). `now_secs` is the compaction's pinned
    /// evaluation instant (`None` disables expiry: a strict no-op).
    ///
    /// A cell that is ALREADY a tombstone (`is_cell_tombstone`, which also covers
    /// an `is_deleted` complex-element tombstone) is left untouched — it has no
    /// live value to expire.
    ///
    /// COMPLEX / COLLECTION / UDT ELEMENTS (#1537): an expiring ELEMENT of a
    /// non-frozen complex column (`is_complex_element`) is expired here too, not
    /// just simple cells (the #1382 scope gap — F2). Cassandra
    /// `AbstractCell.purge(DeletionPurger, nowInSec)` treats an expired expiring
    /// cell UNIFORMLY whether simple or complex: it converts it to
    /// `BufferCell.tombstone(column, timestamp(), localDeletionTime() - ttl(), path())` —
    /// PRESERVING the element's cell path and setting the tombstone's
    /// `localDeletionTime` to `ldt - ttl` (the cell's creation time). So an
    /// expired element becomes an element-level tombstone at the SAME path,
    /// carrying its own IS_DELETED flag (epic #899), which the per-element writer
    /// emits as an element tombstone.
    ///
    /// LDT NORMALIZATION (#921 finding 2): on-disk `localDeletionTime` is an
    /// UNSIGNED 32-bit GC-clock second count carried as a wrapped `i32`;
    /// reinterpret the bits as unsigned (`i64::from(ldt as u32)`) before the
    /// compare so a far-future expiry is not treated as already-past.
    pub(super) fn expire_ttl_cells(&mut self, now_secs: Option<i64>) {
        let Some(now) = now_secs else {
            return; // Expiry disabled — strict no-op.
        };
        let mut expired: Vec<(CellData, usize, i64)> = Vec::new();
        for cell in &mut self.surviving {
            // An existing tombstone (simple cell tombstone OR a complex-element
            // tombstone via `is_deleted`) has no live value to expire.
            if KWayMerger::<NoTrace>::is_cell_tombstone(cell) {
                continue;
            }
            let (Some(ttl), Some(ldt)) = (cell.ttl, cell.local_deletion_time) else {
                continue; // Not an authoritative expiring cell.
            };
            // PARITY — off-by-one at `now == ldt` (issue #1537). Cassandra
            // `Cell.isLive(nowInSec)` (`Cell.java`) is LIVE iff
            // `nowInSec < localDeletionTime`, so an expiring cell is EXPIRED iff
            // `localDeletionTime <= nowInSec`. Keep the cell LIVE only when its
            // expiry instant (unsigned GC-clock seconds) is STRICTLY in the
            // future; `ldt == now` means already expired.
            if i64::from(ldt as u32) > now {
                continue; // Not yet expired (strictly-future expiry).
            }
            // PARITY — tombstone `localDeletionTime` == `ldt - ttl` (issue #1537).
            // Cassandra `AbstractCell.purge` converts an expired expiring cell via
            // `BufferCell.tombstone(column, timestamp(), localDeletionTime() - ttl(),
            // path())`. CQLite stores an expiring cell's on-disk
            // `local_deletion_time` as the EXPIRY instant (`now + ttl`), so the
            // parity-correct tombstone value is `ldt - ttl` = the cell's
            // creation-time-in-seconds. gc grace is therefore measured from the
            // cell's creation time (see the comment in `AbstractCell.purge`).
            //
            // GUARD: this cannot underflow for a VALID expiring cell — `ldt`
            // (= creation + ttl) is always `>= ttl`. A `saturating_sub` on the
            // unsigned seconds keeps a malformed pair from ever panicking (it
            // floors at 0, an ancient/purgeable tombstone) with no `unwrap`.
            let creation_secs: u32 = (ldt as u32).saturating_sub(ttl);
            let tombstone_ldt: i64 = i64::from(creation_secs);
            let old_cell = if S::ENABLED { Some(cell.clone()) } else { None };
            let run_index = self
                .winner_runs
                .as_ref()
                .and_then(|runs| {
                    runs.get(&(cell.column.clone(), cell.cell_path.clone()))
                        .copied()
                })
                .unwrap_or(self.run_index);
            // Convert the expired live cell into a (cell / complex-element)
            // tombstone whose `localDeletionTime` is the creation-time instant
            // (`ldt - ttl`) and whose `markedForDeleteAt` is the cell's own write
            // timestamp (unchanged). Step 3c then purges it exactly like any other
            // cell tombstone once its LDT is < gcBefore and the overlap gate
            // allows. `ttl` is cleared: a tombstone carries no TTL.
            cell.value = crate::types::Value::Tombstone(Box::new(crate::types::TombstoneInfo {
                deletion_time: cell.timestamp,
                tombstone_type: crate::types::TombstoneType::CellTombstone,
                local_deletion_time: tombstone_ldt,
                ttl: None,
                range_start: None,
                range_end: None,
            }));
            cell.ttl = None;
            // A complex ELEMENT additionally carries its deletion via the
            // authoritative IS_DELETED flag (epic #899). Set it so the per-element
            // writer emits an element tombstone (preserving the element's
            // cell_path, Cassandra `path()`) rather than a live element, and so the
            // gc-grace purge's `cell.is_deleted` branch (Step 3c (a)) treats it as
            // a purgeable complex-element tombstone. The cell path is untouched.
            if cell.is_complex_element {
                cell.is_deleted = true;
            }
            // PARITY: the converted tombstone's `localDeletionTime` is the
            // creation-time instant (`ldt - ttl`), so update `cell.local_deletion_time`
            // to match. `cell_effective_ldt` prefers this field, so both the
            // gc-grace purge (Step 3c) and the writer surface the creation-time
            // LDT — measuring gc grace from creation, per `AbstractCell.purge`.
            // Stored as the wrapped `i32` GC-clock value (re-widened unsigned by
            // `cell_effective_ldt`); `creation_secs` fits `i32` for any real
            // creation time.
            cell.local_deletion_time = Some(creation_secs as i32);
            if let Some(old_cell) = old_cell {
                expired.push((old_cell, run_index, i64::from(ldt as u32)));
            }
        }
        for (cell, run_index, expires_at) in expired {
            self.emit_cell(
                &cell,
                run_index,
                Verdict::Expired,
                DecidedBy::Expiry { expires_at, now },
            );
        }
    }

    /// Step 3c — gc_grace / gcBefore tombstone PURGING (issue #845, parity
    /// Cassandra `8d47ebb2`). A tombstone whose on-disk `localDeletionTime`
    /// (GC-clock seconds) is STRICTLY LESS THAN `gcBefore` is purgeable and
    /// dropped; one within grace (`>= gcBefore`) is retained. Runs AFTER the
    /// complex-deletion shadow-before-purge (Step 2b) and the row-tombstone /
    /// dropped-column filters above, so the now-redundant marker cannot
    /// resurrect data within this set.
    ///
    /// OVERLAP SAFETY (#921 finding 1, #935): `gc_before_secs` is the EFFECTIVE
    /// cutoff (caller collapses it to `None` for an unbounded partial compaction,
    /// making this a strict no-op). With a bound, a tombstone is purged only when
    /// BOTH its gc grace has elapsed AND its own deletion timestamp
    /// (`markedForDeleteAt`) is STRICTLY LESS THAN `max_purgeable_timestamp` — so
    /// it provably shadows nothing in a non-included overlapping SSTable.
    ///
    /// LDT NORMALIZATION (#921 finding 2): on-disk `localDeletionTime` is an
    /// UNSIGNED 32-bit GC-clock second count carried as a wrapped `i32`;
    /// reinterpret the bits as unsigned (`i64::from(ldt as u32)`) before every
    /// compare so a far-future tombstone is not purged immediately. All three
    /// sites below (cell / row / complex-deletion), plus the expiring-cell check
    /// in `expire_ttl_cells`, MUST widen unsigned — a signed compare would treat
    /// a value in `[2^31, 2^32)` (a negative `i32`, e.g. `i32::MIN == 2^31`) as
    /// ANCIENT and purge a not-yet-expired far-future tombstone, resurrecting any
    /// older cells it shadows. Pinned by `tests/issue_1386_wrapped_negative_ldt.rs`.
    ///
    /// DIVERGENCE FROM CASSANDRA — RECORDED DECISION (#1386): CQLite SILENTLY
    /// reinterprets the LDT bits as unsigned (year-2106 GC-clock semantics) and
    /// never flags the value as "suspect". Cassandra 5.0's deletion-time handling
    /// additionally treats an out-of-range `localDeletionTime` as suspicious
    /// (e.g. `Cell`/`DeletionTime` validation paths log / mark it). CQLite's
    /// posture is deliberately reinterpret-unsigned WITHOUT suspect-marking: a
    /// far-future tombstone round-trips and purges purely on the unsigned GC-clock
    /// compare, matching Cassandra's PURGE DECISION while omitting its diagnostics.
    ///
    /// Each genuine gc/overlap-safe purge increments the matching `purges` field
    /// (issue #1037); last-write-wins reconciliation collapse is NOT counted.
    pub(super) fn purge_gc_grace(
        &mut self,
        gc_before_secs: Option<i64>,
        max_purgeable_timestamp: i64,
        purges: &mut PurgeCounts,
    ) {
        if let Some(gc_before) = gc_before_secs {
            let mut purged_cells: Vec<(CellData, usize, i32)> = Vec::new();
            if S::ENABLED {
                for cell in &self.surviving {
                    if (KWayMerger::<NoTrace>::is_cell_tombstone(cell) || cell.is_deleted)
                        && KWayMerger::<NoTrace>::cell_effective_ldt(cell)
                            .is_some_and(|ldt| i64::from(ldt as u32) < gc_before)
                        && cell.timestamp < max_purgeable_timestamp
                    {
                        let run_index = self
                            .winner_runs
                            .as_ref()
                            .and_then(|runs| {
                                runs.get(&(cell.column.clone(), cell.cell_path.clone()))
                                    .copied()
                            })
                            .unwrap_or(self.run_index);
                        let ldt = KWayMerger::<NoTrace>::cell_effective_ldt(cell).unwrap_or(0);
                        purged_cells.push((cell.clone(), run_index, ldt));
                    }
                }
            }
            // (a) Cell tombstones: drop any purgeable simple cell tombstone (and
            // purgeable complex-element tombstone) from the surviving set. A cell
            // whose `local_deletion_time` is not surfaced (`None`) is conservative-
            // ly RETAINED — we never purge on unknown LDT (no-heuristics mandate).
            self.surviving.retain(|cell| {
                if KWayMerger::<NoTrace>::is_cell_tombstone(cell) || cell.is_deleted {
                    // #921 finding 1: a simple cell tombstone surfaces its LDT in
                    // its `Value::Tombstone` payload, not `CellData.local_deletion_time`
                    // (which the reader fills only for expiring cells). Consult both
                    // via `cell_effective_ldt` so a purgeable cell tombstone is
                    // actually purged here — matching the survivor pre-pass.
                    let gc_purgeable = match KWayMerger::<NoTrace>::cell_effective_ldt(cell) {
                        Some(ldt) => i64::from(ldt as u32) < gc_before,
                        // Unknown LDT: never purge (no-heuristics mandate).
                        None => false,
                    };
                    // #935 overlap gate: the cell tombstone's own write timestamp
                    // (`markedForDeleteAt`) must be STRICTLY BELOW the min outside
                    // timestamp to prove it shadows nothing in a non-included
                    // overlapping SSTable. `i64::MAX` for a full compaction lets
                    // every gc-purgeable tombstone through unchanged.
                    let overlap_purgeable = cell.timestamp < max_purgeable_timestamp;
                    // RETAIN unless BOTH gates allow the purge.
                    let keep = !(gc_purgeable && overlap_purgeable);
                    if !keep {
                        // True gc/overlap-safe purge of a cell tombstone (simple
                        // or complex-element), issue #1037.
                        purges.cell_tombstones += 1;
                    }
                    keep
                } else {
                    true
                }
            });

            // (b) Row tombstone: a purgeable row deletion is dropped so no
            // tombstone entry is emitted for it. Row-tombstone shadowing already
            // ran above (`after_row_del`), so cells it covered are gone.
            //
            // UNKNOWN-LDT RETENTION (#921 finding 2): `row_del_ldt == 0` is the
            // "LDT not surfaced" placeholder; treat 0 as UNKNOWN and RETAIN the
            // row tombstone. Only a real, non-zero, surfaced LDT strictly below
            // `gcBefore` purges.
            //
            // #935 overlap gate: also require the row deletion's own timestamp
            // (`markedForDeleteAt` = `row_del`) to be STRICTLY BELOW the min
            // outside timestamp so it shadows nothing in a non-included
            // overlapping SSTable. `i64::MAX` (full compaction) is a no-op.
            if self.row_del_ldt != 0
                && i64::from(self.row_del_ldt as u32) < gc_before
                && self.row_del.is_some_and(|d| d < max_purgeable_timestamp)
            {
                self.row_del = None;
                // True gc/overlap-safe purge of a row tombstone (issue #1037).
                purges.row_tombstones += 1;
            }

            // (c) Complex-deletion markers: drop each purgeable marker. The
            // strict-supersede reduction + shadow-before-purge already ran in
            // Step 2b, so a covered element is gone before its marker is purged.
            // #935 overlap gate: also require the marker's `marked_for_delete_at`
            // to be STRICTLY BELOW the min outside timestamp.
            self.complex_deletions.retain(|cd| {
                let gc_purgeable = i64::from(cd.local_deletion_time as u32) < gc_before;
                let overlap_purgeable = cd.marked_for_delete_at < max_purgeable_timestamp;
                let keep = !(gc_purgeable && overlap_purgeable);
                if !keep {
                    // True gc/overlap-safe purge of a complex-deletion marker
                    // (issue #1037).
                    purges.complex_deletions += 1;
                }
                keep
            });
            for (cell, run_index, ldt) in purged_cells {
                self.emit_cell(
                    &cell,
                    run_index,
                    Verdict::Purgeable,
                    DecidedBy::GcGrace {
                        ldt,
                        gc_before,
                        now: self.trace_now_secs.unwrap_or(gc_before),
                    },
                );
            }
        }
    }

    /// Step 4 — build the merged result, applying the phantom key-only-row guard
    /// (#921 finding 3): recompute `purged_to_empty` AFTER the gc-grace
    /// cell-tombstone purge, so a CLUSTERED row whose only non-key data was a
    /// purgeable cell tombstone emits NOTHING instead of a phantom key-only live
    /// row. A row that was always key-only (a genuine row marker) keeps
    /// `had_data_before == false` and is preserved.
    ///
    /// Returns `None` for an empty group (the original `let key = key?`
    /// early-out) or a truly absent row.
    #[allow(dead_code)]
    pub(super) fn build(self, purges: &mut PurgeCounts) -> Option<MergeEntry> {
        self.build_with_trace_metadata(purges).0
    }

    /// Build the merged row and return the trace-only source map alongside it.
    /// The map is moved out before the behavior-preserving build body consumes
    /// the state, so untraced builds never clone cell keys or allocate a side
    /// channel.
    pub(super) fn build_with_trace_metadata(
        mut self,
        purges: &mut PurgeCounts,
    ) -> (Option<MergeEntry>, ReconcileTraceMetadata) {
        let winner_runs = self.winner_runs.take();
        let built = self.build_inner(purges);
        (built, ReconcileTraceMetadata { winner_runs })
    }

    fn build_inner(self, purges: &mut PurgeCounts) -> Option<MergeEntry> {
        let ReconcileState {
            clustering_key,
            key,
            run_index,
            row_del,
            row_del_ldt,
            complex_deletions,
            range_deletion,
            surviving,
            had_data_before,
            row_liveness,
            ..
        } = self;

        let key = key?; // empty group => nothing to emit

        let ck_names: HashSet<&str> = clustering_key
            .as_ref()
            .map(|ck| ck.columns.iter().map(|(n, _)| n.as_str()).collect())
            .unwrap_or_default();
        let is_data_cell = |cell: &CellData| !ck_names.contains(cell.column.as_str());
        let has_data_after = surviving.iter().any(is_data_cell);
        let purged_to_empty = had_data_before && !has_data_after;
        drop(ck_names);

        // Issue #2163 (roborev r7): a cell tombstone (simple or complex-element,
        // the SAME predicate `purge_gc_grace`'s retain closure uses) that
        // SURVIVES into `surviving` — whether because gc-grace/overlap kept it,
        // or because purging was inactive for this merge entirely (no
        // `gc_before_secs`, e.g. an unbounded partial compaction) — is a marker
        // RETAINED into the output, matching the row/range/partition tombstone
        // "emitted" contract established above. Counted here from the FINAL
        // surviving set (not inside `purge_gc_grace`'s conditional retain) so a
        // merge with purging disabled still counts its retained cell
        // tombstones; a cell tombstone always satisfies `is_data_cell` (it is
        // never a clustering-key pseudo-cell), so its presence here already
        // implies `purged_to_empty == false` and the row below is genuinely
        // emitted with it intact.
        let retained_cell_tombstones = surviving
            .iter()
            .filter(|cell| KWayMerger::<NoTrace>::is_cell_tombstone(cell) || cell.is_deleted)
            .count() as u64;
        purges.emitted += retained_cell_tombstones;

        // Attach the carried deletion metadata to whichever entry is emitted so it
        // is not dropped by reconciliation (#886 plumbing preservation).
        let has_carried_metadata = !complex_deletions.is_empty() || range_deletion.is_some();

        // Emit a live row only when real data survives. `!purged_to_empty`
        // suppresses a clustered row whose only data was a dropped column (phantom
        // key-only row); a genuine row marker (always key-only) has
        // `had_data_before == false` so it is preserved.
        let built = if !surviving.is_empty() && !purged_to_empty {
            // `surviving` is non-empty, so `max()` is `Some`; `unwrap_or(0)` only
            // guards the type and never triggers.
            let row_ts = surviving.iter().map(|c| c.timestamp).max().unwrap_or(0);
            let live = MergeEntry::new(
                run_index,
                key,
                clustering_key,
                row_ts,
                RowData::Live { cells: surviving },
            );
            // Issue #932: a row deletion that survived purging COEXISTS with the
            // surviving (strictly-newer) cells. Carry it on the live entry so the
            // merge→mutation step emits a `HAS_DELETION` row holding both — the row
            // deletion keeps shadowing older cells of OTHER columns living in
            // SSTables not part of a partial compaction, preventing resurrection.
            // Step 3 already dropped the cells this deletion covers (ts <= row_del),
            // so the deletion shadows nothing within `surviving`.
            Some(match row_del {
                Some(deletion_time) => {
                    // Issue #2163: this row-tombstone marker is RETAINED alongside
                    // newer live cells (the common "retained-but-coexisting" case,
                    // NOT the sole-output case below) — count it as emitted too, so
                    // `tombstones_emitted` covers every marker that survives
                    // reconciliation into the output, not only the row-absent case.
                    purges.emitted += 1;
                    live.with_row_deletion(deletion_time, row_del_ldt)
                }
                None => live,
            })
        } else if let Some(deletion_time) = row_del {
            // No surviving data. If a row tombstone exists, keep the row shadowed
            // so downstream still emits the deletion (preserves #505/#498 absence).
            // Issue #2163: this row-tombstone marker is RETAINED into the output —
            // count it as emitted (a marker carried forward, not purged).
            purges.emitted += 1;
            Some(MergeEntry::new(
                run_index,
                key,
                clustering_key,
                deletion_time,
                RowData::Tombstone {
                    deletion_time,
                    // Preserve the source LDT paired with the winning deletion_time
                    // (#873) instead of resetting it to 0; the writer encodes it
                    // verbatim and gc_grace decisions stay faithful.
                    local_deletion_time: row_del_ldt,
                },
            ))
        } else if has_carried_metadata {
            // No surviving data AND no row tombstone, but the cluster still carries
            // complex/range deletion metadata. Emit a metadata-only entry (an empty
            // `Live` row) so it survives reconciliation and reaches downstream
            // consumers (#844/#846/#899).
            Some(MergeEntry::new(
                run_index,
                key,
                clustering_key,
                0,
                RowData::Live { cells: vec![] },
            ))
        } else {
            // Truly empty/absent row.
            None
        };

        built.map(|entry| {
            // Issue #2374/#2789: carry the reconciled row-marker liveness onto the
            // emitted entry for the read path's visibility decision (carry-only).
            let entry = entry.with_row_liveness(row_liveness);
            let entry = if complex_deletions.is_empty() {
                entry
            } else {
                entry.with_complex_deletions(complex_deletions)
            };
            match range_deletion {
                Some(rd) => entry.with_range_deletion(rd),
                None => entry,
            }
        })
    }
}
