//! Reconciliation steps 1-3b: fold, cell-winner resolution, complex-deletion
//! shadow-before-purge, row-tombstone shadowing, dropped-column filtering.
//!
//! Split out of `reconcile.rs` (issue #4193, design.md §D5 campsite rule) —
//! pure relocation, no behavior change. See the module-level step-order
//! documentation in `super` (`reconcile.rs`) for the fixed order these methods
//! run in; that order is parity-load-bearing and is NOT re-derivable from this
//! file alone.

use std::collections::{HashMap, HashSet};

use crate::storage::write_engine::reconcile_rules;

use super::super::trace::{DecidedBy, NoTrace, TombstoneKind, TombstoneRecord, TraceSink, Verdict};
use super::super::{CellData, ComplexDeletion, KWayMerger, MergeEntry, PurgeCounts, RowData};
use super::{CellKey, ReconcileState};

impl<S: TraceSink> ReconcileState<S> {
    /// Step 1 — fold the per-entry deletion metadata: effective row deletion
    /// (max `deletion_time` with its PAIRED source LDT, #873), the carried
    /// complex-deletion union (first-seen, de-duplicated) and the
    /// timestamp-max range deletion, plus the carry-through key / run_index.
    ///
    /// The per-cell winner resolution (the `RowData::Live` arm) is split out
    /// into [`Self::resolve_cell_winners`]; both passes iterate `cluster_rows`
    /// in heap-routing order, and the accumulators are independent, so the
    /// result is identical to the original single combined loop.
    pub(in super::super) fn fold_row_deletions(&mut self, cluster_rows: &[MergeEntry]) {
        for entry in cluster_rows {
            if self.key.is_none() {
                self.key = Some(entry.key.clone());
            }
            self.run_index = self.run_index.min(entry.run_index);

            if let Some((deletion_time, local_deletion_time)) = entry.partition_deletion {
                self.emit_tombstone(TombstoneRecord {
                    kind: TombstoneKind::Partition,
                    run_index: entry.run_index,
                    clustering: None,
                    column: None,
                    deletion_time,
                    local_deletion_time,
                    range_start: None,
                    range_end: None,
                    droppable_at_now: self.tombstone_droppable(local_deletion_time),
                });
            }
            if let Some(range) = &entry.range_deletion {
                self.emit_tombstone(TombstoneRecord {
                    kind: TombstoneKind::Range,
                    run_index: entry.run_index,
                    clustering: entry.clustering_key.clone(),
                    column: None,
                    deletion_time: range.deletion_time,
                    local_deletion_time: range.local_deletion_time,
                    range_start: Some(range.start.clone()),
                    range_end: Some(range.end.clone()),
                    droppable_at_now: self.tombstone_droppable(range.local_deletion_time),
                });
            }
            for deletion in &entry.complex_deletions {
                self.emit_tombstone(TombstoneRecord {
                    kind: TombstoneKind::Collection,
                    run_index: entry.run_index,
                    clustering: entry.clustering_key.clone(),
                    column: Some(deletion.column.clone()),
                    deletion_time: deletion.marked_for_delete_at,
                    local_deletion_time: deletion.local_deletion_time,
                    range_start: None,
                    range_end: None,
                    droppable_at_now: self.tombstone_droppable(deletion.local_deletion_time),
                });
            }

            // Issue #2374/#2789: fold the row-marker liveness across generations
            // (latest-expiry / live-forever wins) so the read path can decide
            // row visibility. Carry-only — never a write-path decision.
            self.row_liveness = self.row_liveness.merge(entry.row_liveness);

            for cd in &entry.complex_deletions {
                if let Some(sources) = &mut self.complex_deletion_sources {
                    let replace = sources
                        .get(&cd.column)
                        .is_none_or(|(timestamp, _)| cd.marked_for_delete_at > *timestamp);
                    if replace {
                        sources.insert(
                            cd.column.clone(),
                            (cd.marked_for_delete_at, entry.run_index),
                        );
                    }
                }
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
                self.emit_tombstone(TombstoneRecord {
                    kind: TombstoneKind::Row,
                    run_index: entry.run_index,
                    clustering: entry.clustering_key.clone(),
                    column: None,
                    deletion_time: del_ts,
                    local_deletion_time: del_ldt,
                    range_start: None,
                    range_end: None,
                    droppable_at_now: self.tombstone_droppable(del_ldt),
                });
                if self.row_del.is_none_or(|d| del_ts > d) {
                    self.row_del = Some(del_ts);
                    self.row_del_ldt = del_ldt;
                    self.row_del_run_index = Some(entry.run_index);
                }
            }

            if let RowData::Tombstone {
                deletion_time,
                local_deletion_time,
            } = &entry.row_data
            {
                self.emit_tombstone(TombstoneRecord {
                    kind: TombstoneKind::Row,
                    run_index: entry.run_index,
                    clustering: entry.clustering_key.clone(),
                    column: None,
                    deletion_time: *deletion_time,
                    local_deletion_time: *local_deletion_time,
                    range_start: None,
                    range_end: None,
                    droppable_at_now: self.tombstone_droppable(*local_deletion_time),
                });
                // When this tombstone's deletion_time becomes (or sets) the new
                // max, capture its paired LDT too so the winning tombstone's
                // source `localDeletionTime` survives reconciliation (#873).
                if self.row_del.is_none_or(|d| *deletion_time > d) {
                    self.row_del = Some(*deletion_time);
                    self.row_del_ldt = *local_deletion_time;
                    self.row_del_run_index = Some(entry.run_index);
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
    pub(in super::super) fn resolve_cell_winners(&mut self, cluster_rows: &[MergeEntry]) {
        for entry in cluster_rows {
            if let RowData::Live { cells } = &entry.row_data {
                for cell in cells {
                    let cell_key: CellKey = (cell.column.clone(), cell.cell_path.clone());
                    if KWayMerger::<NoTrace>::is_cell_tombstone(cell) || cell.is_deleted {
                        let local_deletion_time = cell
                            .local_deletion_time
                            .or(match &cell.value {
                                crate::types::Value::Tombstone(info) => {
                                    Some(info.local_deletion_time as i32)
                                }
                                _ => None,
                            })
                            .unwrap_or(0);
                        self.emit_tombstone(TombstoneRecord {
                            kind: TombstoneKind::Cell,
                            run_index: entry.run_index,
                            clustering: entry.clustering_key.clone(),
                            column: Some(cell.column.clone()),
                            deletion_time: cell.timestamp,
                            local_deletion_time,
                            range_start: None,
                            range_end: None,
                            droppable_at_now: self.tombstone_droppable(local_deletion_time),
                        });
                    }

                    // Issue #4193 review finding: use the `Entry` API (single
                    // hash of `self.winners`, `order`/`winner_runs` reuse the
                    // slot's owned key on the vacant path) rather than a
                    // preliminary `get()` + a separate `insert()` — that
                    // double-hashes `self.winners` and clones `cell_key`/
                    // `cell` an extra time on every cell of every merge
                    // (read AND compaction), whether or not tracing is
                    // enabled. All trace-only bookkeeping (whether the key
                    // was already occupied, the previous winner's
                    // `run_index`) is derived from the `Entry` match arms
                    // themselves.
                    match self.winners.entry(cell_key) {
                        std::collections::hash_map::Entry::Vacant(slot) => {
                            self.order.push(slot.key().clone());
                            if let Some(runs) = &mut self.winner_runs {
                                runs.insert(slot.key().clone(), entry.run_index);
                            }
                            slot.insert(cell.clone());
                        }
                        std::collections::hash_map::Entry::Occupied(mut slot) => {
                            // Higher timestamp wins. At EQUAL timestamp a cell
                            // DELETION (tombstone) beats a LIVE or EXPIRING (TTL) cell,
                            // before any localDeletionTime comparison (Cassandra
                            // `Cells#reconcile`; issue #848 / #498).
                            let existing_run = self
                                .winner_runs
                                .as_ref()
                                .and_then(|runs| runs.get(slot.key()).copied())
                                .unwrap_or(entry.run_index);
                            let cell_wins = reconcile_rules::cell_wins(cell, slot.get());
                            if cell_wins {
                                let old = if S::ENABLED {
                                    Some(slot.get().clone())
                                } else {
                                    None
                                };
                                if let Some(runs) = &mut self.winner_runs {
                                    runs.insert(slot.key().clone(), entry.run_index);
                                }
                                slot.insert(cell.clone());
                                if let Some(old) = old {
                                    let (verdict, decided_by) = if cell.timestamp == old.timestamp
                                        && (KWayMerger::<NoTrace>::is_cell_tombstone(cell)
                                            || cell.is_deleted)
                                        && !(KWayMerger::<NoTrace>::is_cell_tombstone(&old)
                                            || old.is_deleted)
                                    {
                                        (
                                            Verdict::ShadowedByTombstone(TombstoneKind::Cell),
                                            DecidedBy::Tombstone {
                                                kind: TombstoneKind::Cell,
                                                run_index: entry.run_index,
                                                deletion_time: cell.timestamp,
                                                local_deletion_time:
                                                    KWayMerger::<NoTrace>::cell_effective_ldt(cell)
                                                        .unwrap_or(0),
                                                droppable_at_now: self.tombstone_droppable(
                                                    KWayMerger::<NoTrace>::cell_effective_ldt(
                                                        cell,
                                                    )
                                                    .unwrap_or(0),
                                                ),
                                            },
                                        )
                                    } else {
                                        (
                                            Verdict::ShadowedByTimestamp,
                                            DecidedBy::Winner {
                                                run_index: entry.run_index,
                                                writetime: cell.timestamp,
                                            },
                                        )
                                    };
                                    self.emit_cell(&old, existing_run, verdict, decided_by);
                                }
                            } else if S::ENABLED {
                                // Copy the fields we need out of `slot.get()`
                                // into owned locals FIRST — a single `existing:
                                // &CellData` reused across this whole
                                // if/else would keep `slot`'s borrow of
                                // `self.winners` alive through the
                                // `self.tombstone_droppable(&self, ..)` call
                                // below, which needs an unaliased `&self`.
                                let existing_timestamp = slot.get().timestamp;
                                let existing_is_tombstone = {
                                    let existing = slot.get();
                                    KWayMerger::<NoTrace>::is_cell_tombstone(existing)
                                        || existing.is_deleted
                                };
                                let existing_effective_ldt =
                                    KWayMerger::<NoTrace>::cell_effective_ldt(slot.get())
                                        .unwrap_or(0);
                                let cell_is_tombstone =
                                    KWayMerger::<NoTrace>::is_cell_tombstone(cell)
                                        || cell.is_deleted;
                                let (verdict, decided_by) = if cell.timestamp == existing_timestamp
                                    && existing_is_tombstone
                                    && !cell_is_tombstone
                                {
                                    (
                                        Verdict::ShadowedByTombstone(TombstoneKind::Cell),
                                        DecidedBy::Tombstone {
                                            kind: TombstoneKind::Cell,
                                            run_index: existing_run,
                                            deletion_time: existing_timestamp,
                                            local_deletion_time: existing_effective_ldt,
                                            droppable_at_now: self
                                                .tombstone_droppable(existing_effective_ldt),
                                        },
                                    )
                                } else {
                                    (
                                        Verdict::ShadowedByTimestamp,
                                        DecidedBy::Winner {
                                            run_index: existing_run,
                                            writetime: existing_timestamp,
                                        },
                                    )
                                };
                                self.emit_cell(cell, entry.run_index, verdict, decided_by);
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
    pub(in super::super) fn apply_complex_deletions(&mut self) {
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
                    let mut shadowed: Vec<(CellKey, CellData)> = Vec::new();
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
                                if S::ENABLED {
                                    if let Some(cell) = winners.get(cell_key) {
                                        shadowed.push((cell_key.clone(), cell.clone()));
                                    }
                                }
                                winners.remove(cell_key);
                                false
                            }
                            None => false,
                        }
                    });
                    for (cell_key, cell) in shadowed {
                        let run_index = self
                            .winner_runs
                            .as_ref()
                            .and_then(|runs| runs.get(&cell_key).copied())
                            .unwrap_or(self.run_index);
                        let deciding_run = self
                            .complex_deletion_sources
                            .as_ref()
                            .and_then(|sources| sources.get(column).map(|(_, run)| *run))
                            .unwrap_or(self.run_index);
                        self.emit_cell(
                            &cell,
                            run_index,
                            Verdict::ShadowedByTombstone(TombstoneKind::Collection),
                            DecidedBy::Tombstone {
                                kind: TombstoneKind::Collection,
                                run_index: deciding_run,
                                deletion_time: mfda,
                                local_deletion_time: cd.local_deletion_time,
                                droppable_at_now: self.tombstone_droppable(cd.local_deletion_time),
                            },
                        );
                    }
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
    pub(in super::super) fn shadow_by_row_deletion(&mut self, purges: &mut PurgeCounts) {
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
        let mut shadowed: Vec<(CellKey, CellData, usize)> = Vec::new();
        if S::ENABLED {
            if let Some(deletion_time) = row_del {
                for cell_key in &self.order {
                    if let Some(cell) = self.winners.get(cell_key) {
                        if cell.timestamp <= deletion_time {
                            let run_index = self
                                .winner_runs
                                .as_ref()
                                .and_then(|runs| runs.get(cell_key).copied())
                                .unwrap_or(self.run_index);
                            shadowed.push((cell_key.clone(), cell.clone(), run_index));
                        }
                    }
                }
            }
        }
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
                        && !KWayMerger::<NoTrace>::is_cell_tombstone(cell)
                        && !ck_names.contains(cell.column.as_str())
                    {
                        purges.suppressed += 1;
                    }
                    survives
                }
                None => true,
            })
            .collect();
        for (_, cell, run_index) in shadowed {
            let kind = TombstoneKind::Row;
            self.emit_cell(
                &cell,
                run_index,
                Verdict::ShadowedByTombstone(kind),
                DecidedBy::Tombstone {
                    kind,
                    run_index: self.row_del_run_index.unwrap_or(self.run_index),
                    deletion_time: row_del.unwrap_or(cell.timestamp),
                    local_deletion_time: self.row_del_ldt,
                    droppable_at_now: self.tombstone_droppable(self.row_del_ldt),
                },
            );
        }
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
    pub(in super::super) fn filter_dropped_columns(
        &mut self,
        dropped_columns: &HashMap<String, i64>,
    ) {
        // Issue #1665: capture the pre-purge phantom-row guard state FIRST — while
        // `after_row_del` is still populated — so we can then MOVE it into the
        // survivor filter instead of deep-cloning every survivor. `after_row_del`
        // is DEAD after this method (VERIFIED against the reconcile step sequence
        // in `merge/mod.rs`: shadow_by_row_deletion → filter_dropped_columns →
        // expire_ttl_cells → purge_gc_grace → build, none of which read it again,
        // and `build` destructures `self` with `..`), so `mem::take` is safe and
        // output is byte-identical (same `had_data_before`, same `surviving`
        // contents and order — only the survivor clone is eliminated).
        let ck_names: HashSet<&str> = self
            .clustering_key
            .as_ref()
            .map(|ck| ck.columns.iter().map(|(n, _)| n.as_str()).collect())
            .unwrap_or_default();
        let is_data_cell = |cell: &CellData| !ck_names.contains(cell.column.as_str());
        self.had_data_before = self.after_row_del.iter().any(is_data_cell);
        drop(ck_names);

        // Issue #4193 (found by `issue_4193_verdict_fixtures.rs::verdict_dropped_column`):
        // Step 3 (`shadow_by_row_deletion`) already `mem::take`s `self.order` and
        // drains `self.winners` into `self.after_row_del` BEFORE this step runs, so
        // scanning `self.order`/`self.winners` here (as the untraced code shape
        // did before the trace sink was added) always iterates an EMPTY pair —
        // the dropped-column verdict could never be emitted. Scan the actual
        // current survivor set, `self.after_row_del`, instead.
        let mut dropped: Vec<(CellData, usize, i64)> = Vec::new();
        if S::ENABLED {
            for cell in &self.after_row_del {
                if let Some(drop_time) = dropped_columns.get(&cell.column) {
                    if cell.timestamp <= *drop_time {
                        let cell_key = (cell.column.clone(), cell.cell_path.clone());
                        let run_index = self
                            .winner_runs
                            .as_ref()
                            .and_then(|runs| runs.get(&cell_key).copied())
                            .unwrap_or(self.run_index);
                        dropped.push((cell.clone(), run_index, *drop_time));
                    }
                }
            }
        }

        self.surviving = std::mem::take(&mut self.after_row_del)
            .into_iter()
            .filter(|cell| match dropped_columns.get(&cell.column) {
                Some(drop_time) => cell.timestamp > *drop_time,
                None => true,
            })
            .collect();
        for (cell, run_index, drop_time) in dropped {
            self.emit_cell(
                &cell,
                run_index,
                Verdict::DroppedColumn,
                DecidedBy::DropTime(drop_time),
            );
        }
    }
}
