//! `KWayMerger<NoTrace>`: cell classification and `MergeEntry` to `PartitionMutation` conversion
//! (split out of `merge/mod.rs`, #4658).

use super::*;

#[cfg(feature = "write-support")]
impl KWayMerger<NoTrace> {
    /// The cell's effective `localDeletionTime` (GC-clock seconds, on-disk
    /// width) for gc_grace purge decisions (#921 finding 1/2).
    ///
    /// The reader surfaces an EXPIRING simple cell's LDT in
    /// [`CellData::local_deletion_time`], but a simple cell TOMBSTONE carries its
    /// LDT inside its `Value::Tombstone(CellTombstone)` payload
    /// (`TombstoneInfo::local_deletion_time`, seconds) — `CellData::local_deletion_time`
    /// stays `None` for it (`row_decoder` only fills that field from
    /// `cell_meta` expiration). Without consulting the tombstone payload, the
    /// purge stage would always see `None` for a cell tombstone and conservatively
    /// retain it, so a purgeable dropped-column tombstone would be (wrongly)
    /// counted as a survivor. Prefer the explicit `CellData` field, then fall back
    /// to a non-zero tombstone-payload LDT; `0` is the "not surfaced" placeholder
    /// and yields `None` (retain on unknown LDT — no-heuristics mandate).
    pub(super) fn cell_effective_ldt(cell: &CellData) -> Option<i32> {
        if let Some(ldt) = cell.local_deletion_time {
            return Some(ldt);
        }
        if let crate::types::Value::Tombstone(ref info) = cell.value {
            if info.tombstone_type == crate::types::TombstoneType::CellTombstone
                && info.local_deletion_time != 0
            {
                return Some(info.local_deletion_time as i32);
            }
        }
        None
    }

    /// Returns true when a cell carries a cell-level tombstone
    /// (`Value::Tombstone(CellTombstone)`), the representation produced by #505.
    ///
    /// Cell tombstones participate in per-cell reconcile like any other cell, but
    /// at EQUAL timestamp a cell tombstone beats a live value (Cassandra
    /// `Cells#reconcile`, same rule as #498 applied per cell).
    pub(super) fn is_cell_tombstone(cell: &CellData) -> bool {
        matches!(
            cell.value,
            crate::types::Value::Tombstone(ref info)
                if info.tombstone_type == crate::types::TombstoneType::CellTombstone
        )
            // Complex-element tombstones carry their delete via the IS_DELETED
            // (0x01) flag (epic #899); their value is wrapped as a CellTombstone in
            // `compaction_row_data_to_row_data`, so the `matches!` above already
            // covers them. Keep `is_deleted` as a belt-and-braces signal so a
            // future element representation that sets the flag without the wrapped
            // value still counts as a deletion for the tie-break.
            || cell.is_deleted
    }

    /// Convert reconciled `CellData`s into writer `CellOperation`s (epic #899,
    /// Phase C).
    ///
    /// A simple cell (`is_complex_element == false`) maps 1:1 to a whole-column
    /// `Write` / `WriteWithTtl` / `Delete` exactly as before. A complex element
    /// (`is_complex_element == true`) maps to a
    /// [`CellOperation::WriteComplexElement`] carrying the element's authoritative
    /// `cell_path`, per-element `timestamp` / `ttl` / `local_deletion_time`, and
    /// — crucially, per the no-heuristics mandate — its AUTHORITATIVE `is_deleted`
    /// flag (threaded verbatim from the reader's `ComplexElement.is_deleted`, NOT
    /// re-derived from value/ttl shape; an expiring SET member is value-None,
    /// ttl-Some yet not a tombstone). The element's on-disk `has_empty_value`
    /// decides whether a value is written, so a SET member round-trips with its
    /// member in the cell_path and no cell value.
    ///
    /// [`CellOperation::WriteComplexElement`]: crate::storage::write_engine::mutation::CellOperation::WriteComplexElement
    fn cells_to_cell_operations(
        cells: Vec<CellData>,
    ) -> Vec<crate::storage::write_engine::mutation::CellOperation> {
        use crate::storage::write_engine::mutation::CellOperation;
        use crate::types::{TombstoneType, Value};

        cells
            .into_iter()
            .map(|cell| {
                if cell.is_complex_element {
                    // Per-element op. The on-disk value is present only for a live
                    // element that is NOT empty-value and NOT deleted. A SET member
                    // (has_empty_value) carries its identity in cell_path; a
                    // deleted element carries no value.
                    let value = if cell.is_deleted || cell.has_empty_value {
                        None
                    } else {
                        Some(cell.value)
                    };
                    return CellOperation::WriteComplexElement {
                        column: cell.column,
                        // A complex element always has a cell_path; default to
                        // empty rather than panicking if a future producer omits
                        // it (no unwrap in lib code).
                        cell_path: cell.cell_path.unwrap_or_default(),
                        value,
                        timestamp_micros: cell.timestamp,
                        ttl_seconds: cell.ttl,
                        local_deletion_time: cell.local_deletion_time,
                        is_deleted: cell.is_deleted,
                    };
                }

                // Issue #505: simple cell-level tombstones are represented as
                // Value::Tombstone(CellTombstone); translate to
                // CellOperation::Delete so the writer emits a proper cell tombstone
                // rather than a live cell with a null value.
                if let Value::Tombstone(ref info) = cell.value {
                    if info.tombstone_type == TombstoneType::CellTombstone {
                        // #921 finding 2: preserve the SOURCE cell tombstone's own
                        // `localDeletionTime` (GC clock, seconds) so the writer
                        // emits it verbatim instead of deriving one from the
                        // enclosing mutation's timestamp. A within-grace cell
                        // tombstone that survives THIS compaction keeps its
                        // original GC clock — no drift that would purge it too
                        // early / keep it too long in a LATER compaction. `0` is
                        // the "not surfaced" placeholder (the reader writes it
                        // when the on-disk LDT is absent), in which case we leave
                        // the op LDT `None` so the writer keeps its historical
                        // timestamp-derived behavior. The width conversion mirrors
                        // the row-tombstone path (`info.local_deletion_time as
                        // i32`, #873).
                        let preserved_ldt = match info.local_deletion_time {
                            0 => None,
                            ldt => Some(ldt as i32),
                        };
                        return CellOperation::Delete {
                            column: cell.column,
                            local_deletion_time: preserved_ldt,
                        };
                    }
                }
                if let Some(ttl) = cell.ttl.filter(|t| *t != 0) {
                    // #1538: preserve the SOURCE expiring cell's authoritative
                    // on-disk `localDeletionTime` (= writetime_s + ttl) so a live
                    // TTL cell that survives THIS compaction is re-emitted
                    // byte-identically — the writer stamps this LDT verbatim
                    // instead of recomputing `now + ttl` (which would drift by the
                    // compaction wall-clock skew). `None` (LDT not surfaced by the
                    // reader) leaves the writer's historical derivation in place.
                    CellOperation::WriteWithTtl {
                        column: cell.column,
                        value: cell.value,
                        ttl_seconds: ttl,
                        local_deletion_time: cell.local_deletion_time,
                    }
                } else {
                    CellOperation::Write {
                        column: cell.column,
                        value: cell.value,
                    }
                }
            })
            .collect()
    }

    /// Convert a MergeEntry back to Mutation for writing
    pub(crate) fn merge_entry_to_mutation(
        entry: MergeEntry,
        schema: &TableSchema,
    ) -> Result<crate::storage::write_engine::mutation::Mutation> {
        use crate::storage::write_engine::mutation::{
            CellOperation, MarkerMode, Mutation, PartitionKey, RowMarker, TableId,
        };

        let partition_key = PartitionKey::from_bytes(&entry.key.key, schema)?;
        let table_id = TableId::new(&schema.keyspace, &schema.table);

        // Issue #1072: a partition-tombstone carrier produces a mutation carrying
        // ONLY a `partition_tombstone` (no operations, no clustering key, no row /
        // range tombstone). The writer (`write_partition`) lifts the
        // partition_tombstone onto the partition HEADER. Emitting any
        // `CellOperation` here (e.g. `DeleteRow` from a `RowData::Tombstone`) would
        // wrongly write a clustering-row tombstone instead. Handled first so the
        // carrier's `row_data` (carried as a deletion so the merge step does not
        // surface a phantom live row) never reaches the operation builder below.
        if let Some((deletion_time, local_deletion_time)) = entry.partition_deletion {
            let mutation = Mutation::new(
                table_id,
                partition_key,
                None,
                Vec::new(),
                deletion_time,
                None,
            );
            let mut mutation = mutation;
            mutation.partition_tombstone =
                Some(crate::storage::write_engine::mutation::PartitionTombstone {
                    deletion_time,
                    local_deletion_time,
                });
            return Ok(mutation);
        }

        // Issue #933: a surviving range tombstone rides on the mutation's
        // `range_tombstones`, which the writer interleaves as on-disk bound markers
        // (and uses to shadow same-partition rows). Captured before `entry` is
        // consumed below. A range carrier has empty operations and no clustering
        // key, so it produces NO row — only the marker is written.
        let range_tombstone = entry.range_deletion.clone();

        // Capture the row tombstone's source LDT (GC-clock seconds) by borrow,
        // before the `match` below moves `cells` out of `entry.row_data` (#873).
        //
        // `0` is the established "LDT not surfaced by the reader" placeholder
        // (the legacy `CompactionRow::from_legacy_value` fallback and pre-V5 row
        // tombstones both build `local_deletion_time: 0`), NOT an authoritative
        // epoch-1970 deletion — a real Cassandra row tombstone always carries a
        // nonzero wall-clock LDT. Only thread a genuinely-surfaced (nonzero) LDT;
        // for the placeholder leave it `None` so the writer keeps deriving LDT
        // from `entry.timestamp` exactly as before this change. Threading `0`
        // here would both lose that fallback and trip the new below-baseline
        // writer guard against a nonzero pre-seeded `min_local_deletion_time`,
        // rejecting previously-valid legacy-path compactions (#873 review #946).
        // A live row leaves it `None` so any cell tombstones keep their historical
        // timestamp-derived behavior.
        let row_tombstone_ldt = match &entry.row_data {
            RowData::Tombstone {
                local_deletion_time,
                ..
            } if *local_deletion_time != 0 => Some(*local_deletion_time),
            _ => None,
        };

        // Issue #932: a coexisting row deletion carried on a LIVE entry must be
        // emitted as the mutation's `row_tombstone` (deletion time decoupled from
        // the row's liveness `timestamp_micros`), so the writer re-emits a
        // `HAS_DELETION` row holding both the deletion and the surviving cells. A
        // pure `RowData::Tombstone` entry carries its deletion via `DeleteRow`
        // (below) and leaves `row_deletion` unset, so this only fires for the
        // coexistence case.
        let coexisting_row_tombstone = match &entry.row_data {
            RowData::Live { .. } => entry.row_deletion,
            RowData::Tombstone { .. } => None,
        };

        // Capture the row tombstone's deletion time (`row_del`) BEFORE moving
        // `row_data` into `operations`. A row tombstone shadows only cells/markers
        // whose timestamp is `<= row_del`; a complex-deletion marker whose
        // `marked_for_delete_at` is STRICTLY GREATER than `row_del` covers a range
        // the row tombstone does NOT, and must still be emitted (see below).
        let row_del = match &entry.row_data {
            RowData::Tombstone { deletion_time, .. } => Some(*deletion_time),
            RowData::Live { .. } => None,
        };

        // Issue #1018: capture each surviving SIMPLE cell's OWN per-cell
        // timestamp BEFORE the cells are consumed by `cells_to_cell_operations`
        // (which turns them into `CellOperation::Write`/`WriteWithTtl`/`Delete`
        // ops that carry no per-cell timestamp). A reconciled row's
        // `entry.timestamp` is the MAX surviving cell timestamp; promoting every
        // sibling to that max would rewrite a cell's timestamp:
        //   * a live `name`@100 next to a `score` cell tombstone@300 → `name`
        //     wrongly rewritten to 300 (the original fix), AND
        //   * a `c1` cell tombstone@100 next to a live `c2`@300 → the tombstone's
        //     marked-for-delete-at wrongly rewritten to 300, so a LATER
        //     compaction that sees `c1` live@200 would be incorrectly shadowed
        //     and DROPPED (over-deletion — roborev HIGH).
        // For a SIMPLE cell tombstone (`Value::Tombstone(CellTombstone)`) the
        // cell's `timestamp` IS its `markedForDeleteAt` (µs); its GC-clock
        // `localDeletionTime` is preserved INDEPENDENTLY via
        // `CellOperation::Delete::local_deletion_time` (#921). Recording any
        // simple cell (live OR tombstone) whose own timestamp DIFFERS from
        // `entry.timestamp` keeps the side-channel empty for the common
        // single-writetime row (zero behavior change there) and lets the writer
        // emit the differing cell with its own explicit timestamp. The map is
        // keyed by column name; per-cell reconciliation already collapses each
        // column to a single surviving op, so one timestamp per column is exact.
        // Complex-element cells already carry their own timestamp via
        // `WriteComplexElement` and are excluded (`!c.is_complex_element`).
        let cell_write_timestamps: Option<std::collections::HashMap<String, i64>> =
            match &entry.row_data {
                RowData::Live { cells } => {
                    let map: std::collections::HashMap<String, i64> = cells
                        .iter()
                        .filter(|c| !c.is_complex_element && c.timestamp != entry.timestamp)
                        .map(|c| (c.column.clone(), c.timestamp))
                        .collect();
                    (!map.is_empty()).then_some(map)
                }
                RowData::Tombstone { .. } => None,
            };

        // #4581: the reconciled marker rides to the writer verbatim (ts/TTL/LDT), never re-derived.
        let marker_mode = MarkerMode::Explicit(match &entry.row_data {
            RowData::Live { .. } => RowMarker::from_liveness(&entry.row_liveness),
            RowData::Tombstone { .. } => None,
        });
        let mut operations = match entry.row_data {
            RowData::Live { cells } => Self::cells_to_cell_operations(cells),
            RowData::Tombstone { .. } => vec![CellOperation::DeleteRow],
        };

        // Epic #899 Phase C: emit a REAL per-column complex deletion marker for
        // each carried `ComplexDeletion`, replacing the writer's hardcoded LIVE
        // sentinel. The writer pairs this with the column's surviving per-element
        // cells (WriteComplexElement ops above). `reconcile_cluster` has already
        // applied strict-supersede + shadow-before-purge to this carried deletion
        // (issue #887), so the marker emitted here is the surviving (max-mfda)
        // deletion and the paired elements are only the survivors. gc_grace purging
        // of the surviving marker remains future work (#845).
        //
        // Issue #887: for a row that reduces to a ROW TOMBSTONE we must NOT
        // unconditionally drop carried complex-deletion markers. A row tombstone at
        // `row_del` shadows only `timestamp <= row_del`. A carried marker whose
        // `marked_for_delete_at` is STRICTLY GREATER than `row_del` covers elements
        // in `(row_del, mfda]` — including elements living in OTHER SSTables not part
        // of this compaction. Dropping such a marker would let those elements be
        // RESURRECTED. So we emit any marker that strictly supersedes the row
        // tombstone (`mfda > row_del`) alongside the `DeleteRow`, mirroring #887's
        // strict boundary. A marker with `mfda <= row_del` is fully covered by the
        // row tombstone and is dropped. For a live row (`row_del == None`) every
        // carried marker is emitted as before.
        for cd in entry.complex_deletions {
            let strictly_supersedes_row_tombstone =
                row_del.is_none_or(|rd| cd.marked_for_delete_at > rd);
            if strictly_supersedes_row_tombstone {
                operations.push(CellOperation::ComplexDeletion {
                    column: cd.column,
                    marked_for_delete_at: cd.marked_for_delete_at,
                    local_deletion_time: cd.local_deletion_time,
                });
            }
        }

        // Thread the row tombstone's preserved source `localDeletionTime` onto the
        // mutation so the writer emits it verbatim (#873) rather than re-deriving
        // it from `entry.timestamp` (`timestamp_micros / 1_000_000`). That keeps
        // gc_grace semantics intact and avoids underflowing the unsigned
        // row-deletion LDT delta for logical-timestamp deletes. A live row leaves
        // the mutation LDT unset (`None`).
        let mutation = Mutation::new(
            table_id,
            partition_key,
            entry.clustering_key,
            operations,
            entry.timestamp,
            None,
        );
        let mutation = match row_tombstone_ldt {
            Some(ldt) => mutation.with_local_deletion_time(ldt),
            None => mutation,
        };
        // Issue #932: attach the coexisting row deletion (deletion time + LDT) so
        // the writer keeps both the row tombstone and the surviving newer cells.
        let mut mutation = match coexisting_row_tombstone {
            Some((deletion_time, ldt)) => mutation.with_row_tombstone(deletion_time, ldt),
            None => mutation,
        };
        // Issue #933: thread the surviving range tombstone so the writer emits the
        // on-disk bound markers (and shadows same-partition rows the marker covers).
        if let Some(rt) = range_tombstone {
            mutation.range_tombstones.push(rt);
        }
        // Issue #1018: thread per-cell timestamps so the writer preserves each
        // surviving sibling cell's own timestamp instead of the row max — for
        // BOTH a live cell's writetime AND a cell tombstone's markedForDeleteAt.
        mutation.cell_write_timestamps = cell_write_timestamps;
        mutation.marker_mode = marker_mode;
        Ok(mutation)
    }
}
