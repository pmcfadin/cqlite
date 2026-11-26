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
//! The assembly is projection-scoped and UDT-aware (owner ruling Q51): [`ReadAssembly`]
//! carries the SELECT's needed columns (`storage::read_projection`) and the reader's
//! UDT registry, and calls [`assemble_read_cells_with_udts`], the assembler the Flight
//! producer uses. Columns the query does not read are never assembled, so a table that
//! merely HAS a UDT collection cannot fail a query that selects other columns.

use std::collections::HashSet;
use std::sync::Arc;

use super::{partition_cover, row_visibility, ReadShadow};
use crate::schema::UdtRegistry;
use crate::storage::sstable::reader::SSTableReader;
use crate::storage::write_engine::merge::{
    assemble_read_cells_with_udts, MergeEntry, RowData, UdtScope,
};
use crate::{Result, RowCells, RowKey, ScanRow};

/// One reconciled metadata row inside the merge task, before per-cell metadata is
/// attached: `(partition key bytes, ScanRow row carrier, [(column,
/// write_timestamp_micros)])`.
pub(super) type MergedMetaRow = (Vec<u8>, ScanRow, Vec<(String, i64)>);

/// What the merged read needs to reassemble collections, captured ONCE per read on the
/// caller's task (before any hop to a blocking thread).
#[derive(Clone)]
pub(super) struct ReadAssembly {
    /// The SELECT's columns plus the key columns; `None` = every column.
    needed: Option<HashSet<String>>,
    /// The readers' UDT registry (the manager's; no second registry is built).
    pub(super) registry: Option<UdtRegistry>,
    keyspace: String,
}

impl ReadAssembly {
    pub(super) fn capture(
        readers: &[Arc<SSTableReader>],
        schema: &crate::schema::TableSchema,
    ) -> Self {
        let needed = crate::storage::read_projection::current().map(|n| {
            let mut set: HashSet<String> = (*n).clone();
            // Key columns surface as pseudo-cells the row needs regardless of projection.
            set.extend(schema.partition_keys.iter().map(|k| k.name.clone()));
            set.extend(schema.clustering_keys.iter().map(|k| k.name.clone()));
            set
        });
        Self {
            needed,
            registry: readers.iter().find_map(|r| r.udt_registry.clone()),
            keyspace: schema.keyspace.clone(),
        }
    }

    fn assemble(
        &self,
        cells: Vec<crate::storage::write_engine::merge::CellData>,
        schema: &crate::schema::TableSchema,
    ) -> Result<RowCells> {
        let scope = self.registry.as_ref().map(|registry| UdtScope {
            registry,
            keyspace: &self.keyspace,
        });
        assemble_read_cells_with_udts(cells, schema, self.needed.as_ref(), scope)
    }
}

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
    asm: &ReadAssembly,
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
                let row_cells: RowCells = asm.assemble(surviving, schema)?;
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
    asm: &ReadAssembly,
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
            let row_cells: RowCells = asm.assemble(surviving, schema)?;
            out.push((key_bytes.to_vec(), ScanRow::Row(row_cells), timestamps));
        }
        // Row tombstones suppress the row entirely (no emission).
    }
    Ok(())
}
