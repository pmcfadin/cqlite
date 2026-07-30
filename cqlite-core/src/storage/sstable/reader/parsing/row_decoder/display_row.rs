//! Row DISPLAY + static-merge helpers for the `V5CompressedLegacy` row decoder
//! (split out of `mod.rs` per the campsite rule, epic #1116).
//!
//! Three decisions live here, each shared by every decode site so the rule exists
//! in exactly ONE place:
//! * `row_has_non_key_cell` — is there any non-primary-key data cell (issue #932);
//! * `merge_static_cells` — positional, clustering-row-wins static injection (#1642);
//! * `build_display_row` — the row-tombstone display rule (#505/#932);
//! * `extract_clustering_values` — clustering identity for range-tombstone coverage.

use super::*;

/// Issue #932: does the decoded cell map hold any NON-primary-key data cell?
///
/// Primary-key (partition + clustering) columns are surfaced into the cell map
/// as pseudo-cells (#229) so the read-back path can recover the clustering
/// identity; they are NOT row data. A row carrying `HAS_DELETION` is a PURE row
/// tombstone only when no such data cell survives — otherwise the row deletion
/// COEXISTS with surviving (strictly-newer) cells and the row displays as live.
pub(super) fn row_has_non_key_cell(cells: &[(Arc<str>, Value)], schema: &TableSchema) -> bool {
    cells.iter().any(|(name, _)| {
        let name: &str = name;
        !schema.partition_keys.iter().any(|k| k.name == name)
            && !schema.clustering_keys.iter().any(|c| c.name == name)
    })
}

/// Issue #1642 (K3): append accumulated static-column cells onto a clustering
/// row's positional cell vector. This is an unconditional `extend` (O(n_static))
/// — NOT a per-cell membership scan — because a static column name can NEVER
/// collide with a name already in the clustering row's cells, so there is no
/// clustering-row-wins conflict to resolve.
///
/// Disjointness proof (this codebase). `static_cells` is the cell vector of an
/// IS_STATIC row; its names are exactly `RowColumnResolution::columns_for(true)`
/// — header columns with `is_static == true` AND `!is_primary_key` AND
/// `!is_clustering`. A static row has no clustering prefix, so it receives zero
/// clustering-key pseudo-cells (row_data.rs: `clustering_values` is empty when
/// static). A clustering row's `cells` names are the clustering-key pseudo-cells
/// (issue #229) PLUS `columns_for(false)` — header columns with `is_static ==
/// false` AND `!is_primary_key` AND `!is_clustering`. Every column has exactly
/// one `is_static` value, so the `is_static == want_static` filter makes
/// `columns_for(true)` and `columns_for(false)` name-disjoint; and
/// `columns_for(true)` excludes clustering-key columns, so no static cell shares
/// a clustering-key pseudo-cell name. Hence the two name sets are disjoint and
/// the former membership guard could never fire.
///
/// Appending AFTER the clustering row's own cells keeps the merged order
/// deterministic-by-construction (never user-visible: the query result is a
/// name-keyed map, issue #1334).
pub(super) fn merge_static_cells(cells: &mut RowCells, static_cells: &RowCells) {
    cells.extend(
        static_cells
            .iter()
            .map(|(name, value)| (Arc::clone(name), value.clone())),
    );
}

/// Issue #932/#1741: build the user-facing `ScanRow` display value for a parsed
/// clustering row from its decoded `cells` and row header. Shared by every
/// user-facing emit path so the row-tombstone display rule lives in ONE place:
/// a `HAS_DELETION` row that carries NO surviving non-key cell displays as a pure
/// `Tombstone` marker (suppressed downstream by `filter_tombstone`); a row that
/// still carries surviving cells displays as a live `Row` (the deletion shadows
/// only already-absent older cells). An empty cell set becomes a null marker.
pub(super) fn build_display_row(
    cells: RowCells,
    row_header_opt: Option<&RowHeader>,
    schema: &TableSchema,
) -> ScanRow {
    let row_tombstone = row_header_opt.filter(|h| h.is_row_tombstone());
    let has_data_cell = row_has_non_key_cell(&cells, schema);
    if row_tombstone.is_some() && !has_data_cell {
        ScanRow::Marker(
            row_tombstone
                .map(|h| h.row_tombstone())
                .unwrap_or(Value::Null),
        )
    } else if cells.is_empty() {
        ScanRow::Marker(Value::Null)
    } else {
        // Issue #1642 (K3): the decoder already emits cells positionally, in
        // serialization-header (schema) column order — determinism comes from
        // CONSTRUCTION, not a per-row sort. The former per-row `HashMap`
        // allocation and alphabetical `sort_by` are gone; the interned
        // `Arc<str>` name handles (#1334) move straight into the carrier.
        ScanRow::Row(cells)
    }
}

/// Issue #1741: extract a clustering row's clustering-key values (in schema
/// clustering order) from its decoded cell map. Only called when a range
/// tombstone is currently OPEN in the partition, so the per-row clone is off the
/// tombstone-free hot path. Returns fewer values than the clustering arity only
/// for a malformed/partial row (missing clustering pseudo-cells).
pub(super) fn extract_clustering_values(
    cells: &[(Arc<str>, Value)],
    schema: &TableSchema,
) -> Vec<Value> {
    schema
        .clustering_keys
        .iter()
        .filter_map(|ck| {
            cells
                .iter()
                .find(|(name, _)| name.as_ref() == ck.name.as_str())
                .map(|(_, v)| v.clone())
        })
        .collect()
}

/// Whether a built display row is VISIBLE to a user-facing `SELECT` (issue #3095).
///
/// [`build_display_row`] returns `ScanRow::Marker` for a pure row tombstone / absent
/// row, and EVERY user-facing consumer suppresses a marker downstream
/// (`integrity::filter_tombstone`, and `build_row_from_scan`'s `into_cells`, issue
/// #505) — so a marker is not a row a `SELECT` returns. This is the single predicate
/// the static-content-on-an-empty-partition rule uses for Cassandra's
/// `partition.hasNext()`, which is likewise evaluated over the already-FILTERED
/// `RowIterator` (`UnfilteredRowIterators.filter`).
pub(super) fn row_is_visible(row: &ScanRow) -> bool {
    matches!(row, ScanRow::Row(_) | ScanRow::RawRow(_))
}

/// Build the display row for a CLUSTERING row of a static-bearing partition, on a
/// user-facing SELECT read (issue #3095).
///
/// The ORDER is load-bearing: the row-tombstone display decision is taken over the
/// row's OWN cells, and static cells are injected only into a row that survives it.
///
/// Cassandra authority — the static row is a PARTITION-level object
/// (`BaseRowIterator.staticRow()`), never part of a clustering `Row`, and
/// `UnfilteredRowIterators.filter` drops a clustering row whose own
/// `hasLiveData(nowInSec, ...)` is false. So a static cell can NEVER make a
/// row-tombstoned clustering row live. Injecting first (which
/// [`merge_static_cells`] + [`build_display_row`] do when called in that order)
/// made `row_has_non_key_cell` true for a PURE row tombstone, so the row surfaced
/// as a live `ScanRow::Row` carrying the static value — a phantom row on the
/// single-generation arm that the k-way merge arm correctly suppresses
/// (`entry_to_row` drops `RowData::Tombstone`). That divergence is what made the
/// two arms disagree on a static partition holding only deleted rows.
///
/// PHYSICAL consumers (compaction, `verify`, delta-scan) keep the historical
/// inject-then-decide order — their callers must see every on-disk unfiltered and
/// their output is byte-pinned — so this is used only where `read_shadowing` is on.
pub(super) fn build_display_row_read_path(
    cells: RowCells,
    static_cells: &RowCells,
    row_header_opt: Option<&RowHeader>,
    schema: &TableSchema,
) -> ScanRow {
    match build_display_row(cells, row_header_opt, schema) {
        ScanRow::Row(mut kept) => {
            merge_static_cells(&mut kept, static_cells);
            ScanRow::Row(kept)
        }
        // A pure row tombstone (or an empty row): statics do not revive it.
        other => other,
    }
}
