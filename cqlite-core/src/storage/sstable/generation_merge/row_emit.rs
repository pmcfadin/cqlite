//! Row emission for the cross-generation read drivers (issue #4640).
//!
//! The k-way merge yields ONE `CellData` per collection element (keyed by its
//! `cell_path`). Emitting each as a `(column, value)` pair, as this module did before
//! #4640, handed N duplicate pairs to the executor for an N-element collection, and the
//! name-keyed row map kept only the last. Every emission here therefore routes the
//! surviving cells through [`assemble_read_cells`], the ONE reassembler (#2324/#2339, the
//! Flight producer's), which folds per-element cells into `Value::List`/`Set`/`Map` and
//! fails closed on a column that mixes simple and complex cells or whose elements cannot
//! be ordered (no heuristics, #28).
//!
//! The manager-level read drivers carry neither a projection nor a UDT registry, so the
//! assembly is column-complete (`needed = None`) and registry-less
//! ([`assemble_read_cells`]); a collection whose element/key is a bare UDT reference
//! fails closed rather than collapsing.

use super::{partition_cover, row_visibility, ReadShadow};
use crate::storage::write_engine::merge::{assemble_read_cells, MergeEntry, RowData};
use crate::{Result, RowCells, RowKey, ScanRow};

/// One reconciled metadata row inside the merge task, before per-cell metadata is
/// attached: `(partition key bytes, ScanRow row carrier, [(column,
/// write_timestamp_micros)])`.
pub(super) type MergedMetaRow = (Vec<u8>, ScanRow, Vec<(String, i64)>);

/// Convert one reconciled partition's merge rows into the live scan rows the read
/// path emits: drop cell tombstones, drop row tombstones, drop rows left empty,
/// and reassemble per-element collection cells into whole collections.
///
/// The single emission kernel shared by the materializing plain merge and the
/// streaming driver so tombstone filtering can never drift between them (issue
/// #1334: emit the interned-name `ScanRow` carrier the read path consumes).
pub(super) fn partition_live_rows(
    row_key: &RowKey,
    rows: Vec<MergeEntry>,
    shadow: &ReadShadow,
    schema: &crate::schema::TableSchema,
) -> Result<Vec<(RowKey, ScanRow)>> {
    // Issue #1849: partition-tombstone cover for this partition's rows, applied as a
    // read-side shadow floor alongside per-cell read-time TTL expiry (post-merge).
    let cover = partition_cover(&rows);
    let mut out = Vec::new();
    for entry in rows {
        let vis = row_visibility::RowVis::of(&entry); // before `row_data` is moved out
        match entry.row_data {
            RowData::Live { cells } => {
                // Read-time visibility: drop cell tombstones + TTL-expired /
                // partition-shadowed data cells; `None` hides the whole row.
                let Some(surviving) = shadow.filter_visible(cover, vis, cells) else {
                    continue;
                };
                let row_cells: RowCells = assemble_read_cells(surviving, schema, None)?;
                out.push((row_key.clone(), ScanRow::Row(row_cells)));
            }
            // Row tombstone: the row is deleted across all generations — suppress.
            RowData::Tombstone { .. } => {}
        }
    }
    Ok(out)
}

/// Convert one reconciled partition's merge rows into metadata-carrying rows: the
/// live cells (collections reassembled, #4640) plus each surviving cell's `(column,
/// write_timestamp)`. Shared by the range and point-targeted branches of the metadata
/// merge so the tombstone filtering + timestamp capture live once.
pub(super) fn push_metadata_rows(
    key_bytes: &[u8],
    rows: Vec<MergeEntry>,
    out: &mut Vec<MergedMetaRow>,
    shadow: &ReadShadow,
    schema: &crate::schema::TableSchema,
) -> Result<()> {
    // Issue #1849: same read-side partition cover + per-cell TTL/shadow filter as the
    // plain path, so the WRITETIME/TTL projection agrees with `SELECT *`.
    let cover = partition_cover(&rows);
    for entry in rows {
        let vis = row_visibility::RowVis::of(&entry); // before `row_data` is moved out
        if let RowData::Live { cells } = entry.row_data {
            // `filter_live` drops cell tombstones + TTL-expired / partition-shadowed
            // data cells; `None` hides the whole row (partition-shadowed).
            let Some(surviving) = shadow.filter_visible(cover, vis, cells) else {
                continue;
            };
            // One entry per surviving CELL (a collection contributes one per element).
            let timestamps: Vec<(String, i64)> = surviving
                .iter()
                .map(|c| (c.column.clone(), c.timestamp))
                .collect();
            let row_cells: RowCells = assemble_read_cells(surviving, schema, None)?;
            out.push((key_bytes.to_vec(), ScanRow::Row(row_cells), timestamps));
        }
        // Row tombstones suppress the row entirely (no emission).
    }
    Ok(())
}
