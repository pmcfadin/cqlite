//! Clustering-group reconciliation kernel (issue #945 decomposition).
//!
//! `KWayMerger::reconcile_cluster_with_overlap_counted` used to be a single
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
use crate::storage::write_engine::reconcile_rules;

use super::{CellData, ComplexDeletion, KWayMerger, MergeEntry, PurgeCounts, RowData};

// Issue #1537: parity coverage that TTL expiry applies to complex/collection/UDT
// elements (not only simple cells). Kept in its own file so `reconcile.rs` stays
// within the campsite size target.
#[cfg(test)]
mod ttl_complex_tests;

/// Per-cell reconcile key: `(column, cell_path)` so each element of a multi-cell
/// column reconciles independently (epic #899). Simple cells have
/// `cell_path == None`.
type CellKey = (String, Option<Vec<u8>>);

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
pub(super) struct ReconcileState {
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
}

impl ReconcileState {
    /// Initialize an empty accumulator for the group sharing `clustering_key`.
    pub(super) fn new(clustering_key: Option<ClusteringKey>) -> Self {
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
        }
    }

    /// True once an input entry has been folded (i.e. the group is non-empty).
    /// Mirrors the original `let key = key?` early-out: an empty group emits
    /// nothing.
    pub(super) fn has_key(&self) -> bool {
        self.key.is_some()
    }

    /// Step 1 — fold the per-entry deletion metadata: effective row deletion
    /// (max `deletion_time` with its PAIRED source LDT, #873), the carried
    /// complex-deletion union (first-seen, de-duplicated) and the
    /// timestamp-max range deletion, plus the carry-through key / run_index.
    ///
    /// The per-cell winner resolution (the `RowData::Live` arm) is split out
    /// into [`Self::resolve_cell_winners`]; both passes iterate `cluster_rows`
    /// in heap-routing order, and the accumulators are independent, so the
    /// result is identical to the original single combined loop.
    pub(super) fn fold_row_deletions(&mut self, cluster_rows: &[MergeEntry]) {
        for entry in cluster_rows {
            if self.key.is_none() {
                self.key = Some(entry.key.clone());
            }
            self.run_index = self.run_index.min(entry.run_index);

            for cd in &entry.complex_deletions {
                if !self.complex_deletions.contains(cd) {
                    self.complex_deletions.push(cd.clone());
                }
            }
            if let Some(rd) = &entry.range_deletion {
                let replace = match &self.range_deletion {
                    None => true,
                    Some(current) => rd.deletion_time > current.deletion_time,
                };
                if replace {
                    self.range_deletion = Some(rd.clone());
                }
            }

            // Issue #932: a coexisting row deletion carried on a LIVE entry (a
            // read-back row that held BOTH a row tombstone and surviving newer
            // cells) contributes to the winning row deletion exactly as a
            // standalone `RowData::Tombstone` does — capture its paired LDT when
            // it sets the new max so the rebuilt deletion preserves its
            // wall-clock `localDeletionTime` (#873).
            if let Some((del_ts, del_ldt)) = entry.row_deletion {
                if self.row_del.is_none_or(|d| del_ts > d) {
                    self.row_del = Some(del_ts);
                    self.row_del_ldt = del_ldt;
                }
            }

            if let RowData::Tombstone {
                deletion_time,
                local_deletion_time,
            } = &entry.row_data
            {
                // When this tombstone's deletion_time becomes (or sets) the new
                // max, capture its paired LDT too so the winning tombstone's
                // source `localDeletionTime` survives reconciliation (#873).
                if self.row_del.is_none_or(|d| *deletion_time > d) {
                    self.row_del = Some(*deletion_time);
                    self.row_del_ldt = *local_deletion_time;
                }
            }
        }
    }

    /// Step 2 — per-cell winner resolution keyed by `(column, cell_path)` so
    /// each element of a multi-cell column reconciles independently (epic #899).
    /// Preserves first-seen key order while resolving winners in `winners`.
    ///
    /// `cluster_rows` is in heap-routing order (run_index ascending within equal
    /// keys), so when two cells tie on both timestamp and liveness the
    /// first-seen (newer file) is kept (see
    /// [`reconcile_rules::cell_wins`], the shared tie-break rule).
    pub(super) fn resolve_cell_winners(&mut self, cluster_rows: &[MergeEntry]) {
        for entry in cluster_rows {
            if let RowData::Live { cells } = &entry.row_data {
                for cell in cells {
                    let cell_key: CellKey = (cell.column.clone(), cell.cell_path.clone());
                    match self.winners.get(&cell_key) {
                        None => {
                            self.order.push(cell_key.clone());
                            self.winners.insert(cell_key, cell.clone());
                        }
                        Some(existing) => {
                            // Higher timestamp wins. At EQUAL timestamp a cell
                            // DELETION (tombstone) beats a LIVE or EXPIRING
                            // (TTL) cell, decided BEFORE any localDeletionTime
                            // compare (parity Cassandra `a62c749`,
                            // `Cells#reconcile`; issue #848 / #498). At equal ts
                            // + equal deletion-status, keep the first-seen
                            // (newer file) winner. The tie-break is the SHARED
                            // [`reconcile_rules::cell_wins`] rule (issue #947),
                            // also used by the flush/write path.
                            if reconcile_rules::cell_wins(cell, existing) {
                                self.winners.insert(cell_key, cell.clone());
                            }
                        }
                    }
                }
            }
        }
    }

    /// Step 2b — COMPLEX-DELETION reconcile: strict-supersede + shadow-before-purge
    /// (issue #887). Runs AFTER per-cell winner resolution and BEFORE the
    /// row-tombstone / dropped-column filters, so an element shadowed by a
    /// surviving complex deletion cannot be resurrected by a later purge of the
    /// deletion marker.
    ///
    /// 1. STRICT SUPERSEDE (parity Cassandra `bd244649`): per complex column the
    ///    ACTIVE deletion is the one with the greatest `marked_for_delete_at`. A
    ///    merged deletion supersedes the active one only when its mfda is STRICTLY
    ///    GREATER — EQUAL timestamps do NOT supersede. Reduce the carried
    ///    first-seen union to ONE deletion per column NAME (the strict max),
    ///    preserving first-seen column order for deterministic output. Matched BY
    ///    NAME (`ComplexDeletion.column`), never by header identity (consistent
    ///    with #888's match-by-name substrate).
    ///
    /// 2. SHADOW BEFORE PURGE (parity Cassandra `f66fa14f`): for the surviving
    ///    deletion on a column, drop every per-element winner of THAT column
    ///    (matched by NAME, only complex ELEMENTS carrying a `cell_path`) whose own
    ///    timestamp is `<= marked_for_delete_at`. The boundary mirrors the
    ///    cell-vs-deletion rule (#498): an element with ts STRICTLY GREATER than
    ///    mfda survives; `<=` is shadowed. A simple cell sharing the name is never
    ///    collapsed by a complex marker.
    pub(super) fn apply_complex_deletions(&mut self) {
        if !self.complex_deletions.is_empty() {
            // Strict-supersede: collapse to the max-mfda deletion per column name.
            let mut active: HashMap<String, ComplexDeletion> = HashMap::new();
            let mut active_order: Vec<String> = Vec::new();
            for cd in self.complex_deletions.drain(..) {
                match active.get_mut(&cd.column) {
                    None => {
                        active_order.push(cd.column.clone());
                        active.insert(cd.column.clone(), cd);
                    }
                    // STRICTLY GREATER supersedes; equal/lesser does NOT
                    // (bd244649). Shared rule (issue #947).
                    Some(existing)
                        if reconcile_rules::complex_deletion_supersedes(
                            cd.marked_for_delete_at,
                            existing.marked_for_delete_at,
                        ) =>
                    {
                        *existing = cd;
                    }
                    Some(_) => {}
                }
            }

            // Shadow-before-purge: drop covered per-element winners of each column
            // whose ts <= mfda (matched by NAME), so purging the marker later cannot
            // resurrect them (f66fa14f). Elements strictly newer than mfda survive.
            for column in &active_order {
                if let Some(cd) = active.get(column) {
                    let mfda = cd.marked_for_delete_at;
                    let winners = &mut self.winners;
                    self.order.retain(|cell_key| {
                        let (cell_column, cell_path) = cell_key;
                        // Only complex elements (those with a cell_path) of THIS
                        // column are candidates for shadowing.
                        if cell_column != column || cell_path.is_none() {
                            return true;
                        }
                        match winners.get(cell_key) {
                            // ts > mfda survives; ts <= mfda is shadowed (purged).
                            // Shared shadow-before-purge boundary (issue #947).
                            Some(cell)
                                if reconcile_rules::element_survives_complex_deletion(
                                    cell.timestamp,
                                    mfda,
                                ) =>
                            {
                                true
                            }
                            Some(_) => {
                                winners.remove(cell_key);
                                false
                            }
                            None => false,
                        }
                    });
                }
            }

            // Rebuild the carried union as the surviving per-column deletions in
            // first-seen column order.
            self.complex_deletions = active_order
                .into_iter()
                .filter_map(|column| active.remove(&column))
                .collect();
        }
    }

    /// Step 3 — apply row-tombstone shadowing per cell. A cell whose timestamp is
    /// `<= row_del` is shadowed (`<=` lets the tombstone win at equal ts, #498).
    /// Cells written strictly after `row_del` survive. This shadowing applies to
    /// cell tombstones too: a row tombstone at ts=T supersedes a cell tombstone at
    /// ts<=T (real Cassandra semantics).
    ///
    /// COMPLEX-DELETION shadowing has already been applied in Step 2b above,
    /// BEFORE this row-tombstone filter, so a surviving complex deletion cannot
    /// resurrect a covered element on a later purge (issue #887, parity
    /// `f66fa14f`). The output `after_row_del` is kept so the dropped-column
    /// stage (Step 3b) can tell whether its purge is what emptied the row of real
    /// data (phantom-row guard).
    pub(super) fn shadow_by_row_deletion(&mut self, purges: &mut PurgeCounts) {
        let row_del = self.row_del;
        // Issue #2163 (roborev r5): clustering-key pseudo-cells are intentionally
        // RETAINED in the cell list for read-back (see `extract_clustering_key`
        // and the `had_data_before` computation in `filter_dropped_columns`
        // below) — they are not real data, so shadowing one must not inflate
        // `tombstones_suppressed`. Build the SAME `is_data_cell` exclusion
        // `filter_dropped_columns`/`build` already use, computed here (before the
        // mutable `winners` borrow) so a clustering-key cell shadowed by the row
        // tombstone is excluded from the count.
        let ck_names: HashSet<&str> = self
            .clustering_key
            .as_ref()
            .map(|ck| ck.columns.iter().map(|(n, _)| n.as_str()).collect())
            .unwrap_or_default();
        let winners = &mut self.winners;
        self.after_row_del = std::mem::take(&mut self.order)
            .into_iter()
            .filter_map(|cell_key| winners.remove(&cell_key))
            .filter(|cell| match row_del {
                Some(d) => {
                    let survives = cell.timestamp > d;
                    // Issue #2163: a LIVE DATA cell dropped here was SHADOWED by
                    // the row tombstone (suppressed) — count it, distinct from a
                    // gc/overlap-safe purge. A cell that is itself a tombstone,
                    // or a clustering-key pseudo-cell (retained for read-back,
                    // never real data), is not "live data suppressed" and is not
                    // counted.
                    if !survives
                        && !KWayMerger::is_cell_tombstone(cell)
                        && !ck_names.contains(cell.column.as_str())
                    {
                        purges.suppressed += 1;
                    }
                    survives
                }
                None => true,
            })
            .collect();
    }

    /// Step 3b — dropped-column filtering (Cassandra `cb34ad47`,
    /// `compaction.purge`). A column dropped at `drop_time` discards every cell
    /// whose `timestamp <= drop_time`; a cell written strictly after the drop
    /// (the column was re-added) survives. Scoped per column via the
    /// `dropped_columns` map (#904 plumbing, #847 filter).
    ///
    /// EXACT PER-CELL GRANULARITY (#922): `cell.timestamp` is the cell's OWN
    /// writetime, so a row mixing a pre-drop dropped-column cell with a post-drop
    /// cell of ANOTHER column purges the dropped cell by its own writetime while
    /// the sibling survives by its.
    ///
    /// Also captures the pre-purge `had_data_before` for the phantom-row guard:
    /// clustering-key columns are intentionally left in the cell list (see
    /// `extract_clustering_key`) for read-back, so we must remember whether the
    /// row HAD non-key data before the gc-grace purge runs (the
    /// `has_data_after` / `purged_to_empty` determination is DEFERRED to
    /// [`Self::build`], after Step 3c — #921 finding 3).
    pub(super) fn filter_dropped_columns(&mut self, dropped_columns: &HashMap<String, i64>) {
        self.surviving = self
            .after_row_del
            .iter()
            .filter(|cell| match dropped_columns.get(&cell.column) {
                Some(drop_time) => cell.timestamp > *drop_time,
                None => true,
            })
            .cloned()
            .collect();

        let ck_names: HashSet<&str> = self
            .clustering_key
            .as_ref()
            .map(|ck| ck.columns.iter().map(|(n, _)| n.as_str()).collect())
            .unwrap_or_default();
        let is_data_cell = |cell: &CellData| !ck_names.contains(cell.column.as_str());
        self.had_data_before = self.after_row_del.iter().any(is_data_cell);
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
        for cell in &mut self.surviving {
            // An existing tombstone (simple cell tombstone OR a complex-element
            // tombstone via `is_deleted`) has no live value to expire.
            if KWayMerger::is_cell_tombstone(cell) {
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
            // (a) Cell tombstones: drop any purgeable simple cell tombstone (and
            // purgeable complex-element tombstone) from the surviving set. A cell
            // whose `local_deletion_time` is not surfaced (`None`) is conservative-
            // ly RETAINED — we never purge on unknown LDT (no-heuristics mandate).
            self.surviving.retain(|cell| {
                if KWayMerger::is_cell_tombstone(cell) || cell.is_deleted {
                    // #921 finding 1: a simple cell tombstone surfaces its LDT in
                    // its `Value::Tombstone` payload, not `CellData.local_deletion_time`
                    // (which the reader fills only for expiring cells). Consult both
                    // via `cell_effective_ldt` so a purgeable cell tombstone is
                    // actually purged here — matching the survivor pre-pass.
                    let gc_purgeable = match KWayMerger::cell_effective_ldt(cell) {
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
    pub(super) fn build(self, purges: &mut PurgeCounts) -> Option<MergeEntry> {
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
