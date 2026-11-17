//! Step 4 of the reconcile kernel — build the merged `MergeEntry` (split out of `reconcile.rs`
//! under the campsite rule, epic #1116). Unchanged by the split except issue #4581: a
//! clustering-less row whose only content is its primary-key liveness marker is emitted (with the
//! marker's real timestamp) on reads AND on compaction, and a marker newer than a surviving row
//! deletion keeps the row live alongside that deletion.

use std::collections::HashSet;

use super::super::{CellData, KWayMerger, MergeEntry, PurgeCounts, RowData};
use super::ReconcileState;

impl ReconcileState {
    /// Step 4 — build the merged result, applying the phantom key-only-row guard
    /// (#921 finding 3): recompute `purged_to_empty` AFTER the gc-grace
    /// cell-tombstone purge, so a CLUSTERED row whose only non-key data was a
    /// purgeable cell tombstone emits NOTHING instead of a phantom key-only live
    /// row. A row that was always key-only (a genuine row marker) keeps
    /// `had_data_before == false` and is preserved.
    ///
    /// Returns `None` for an empty group (the original `let key = key?`
    /// early-out) or a truly absent row.
    pub(in crate::storage::write_engine::merge) fn build(
        self,
        purges: &mut PurgeCounts,
        now: Option<i64>,
    ) -> Option<MergeEntry> {
        let ReconcileState {
            clustering_key,
            key,
            run_index,
            row_del,
            purged_row_del,
            row_del_ldt,
            complex_deletions,
            range_deletion,
            mut surviving,
            had_data_before,
            row_liveness,
            marker_purged,
            ..
        } = self;

        let key = key?; // empty group => nothing to emit

        let ck_names: HashSet<&str> = clustering_key
            .as_ref()
            .map(|ck| ck.columns.iter().map(|(n, _)| n.as_str()).collect())
            .unwrap_or_default();
        let is_data_cell = |cell: &CellData| !ck_names.contains(cell.column.as_str());
        // #4545: key pseudo-cells survive a row deletion only beside LIVE data or a still-live newer
        // marker (decided here, after expiry/purge; a gc-purged deletion still sheds them).
        if let Some(d) = row_del.or(purged_row_del) {
            super::super::model::shed_key_cells(&mut surviving, &ck_names, &row_liveness, d, now);
        }
        let has_data_after = surviving.iter().any(is_data_cell);
        let purged_to_empty = had_data_before && !has_data_after;
        // #4581: a purged expired marker leaves a clustered key-only row (its clustering
        // pseudo-cells) with no liveness and no data: `BTreeRow.purge` -> null.
        let marker_gone = marker_purged && !has_data_after;
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
            .filter(|cell| KWayMerger::is_cell_tombstone(cell) || cell.is_deleted)
            .count() as u64;
        purges.emitted += retained_cell_tombstones;

        // Attach the carried deletion metadata to whichever entry is emitted so it
        // is not dropped by reconciliation (#886 plumbing preservation).
        let has_carried_metadata = !complex_deletions.is_empty() || range_deletion.is_some();
        // #4581: a clustering-less row whose marker survived shadowing and purging is a live row
        // whatever else happened to its cells (`Row.Merger.merge` returns null only when liveness,
        // deletion AND data are all empty, Row.java:778; `BTreeRow.purge` keeps a live marker,
        // BTreeRow.java:464). A clustered row's marker rides its clustering pseudo-cells instead.
        let marker_row = clustering_key.is_none() && row_liveness.has_marker;

        // Emit a live row only when real data survives. `!purged_to_empty`
        // suppresses a clustered row whose only data was a dropped column (phantom
        // key-only row); a genuine row marker (always key-only) has
        // `had_data_before == false` so it is preserved.
        let built = if (!surviving.is_empty() && !purged_to_empty && !marker_gone) || marker_row {
            // Newest surviving cell; a marker-only row (no cells) takes the marker's own timestamp
            // (never 0 — the writer would emit ts=0, #4581).
            let row_ts = surviving
                .iter()
                .map(|c| c.timestamp)
                .max()
                .or(row_liveness.marker_timestamp)
                .unwrap_or(0);
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
            // No data/row tombstone, but carried deletion metadata (#844/#846/#899): an empty `Live` row.
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
