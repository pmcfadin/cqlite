//! Issue #899 / #3809: the per-element compaction ROW BUILDER — the one place a
//! decoded row's pieces become a [`CompactionRowData`], and the one place the
//! row-deletion clustering-identity invariant is enforced (#3809, Finding 1).
//!
//! Split out of `compaction.rs` under the campsite rule (epic #1116), the same
//! way `compaction_stream.rs` was (#2299): that file owns the block/partition
//! ENTRY POINTS and the `CompactionPolicy`, this one owns the row build. A CHILD
//! module of it (not a sibling) so the split costs no declaration line in
//! `row_decoder/mod.rs`, which is itself at the size limit; the method is
//! `pub(super)` only so its former host can still call it.

use super::super::*;

impl V5CompressedLegacyParser {
    /// Build a [`CompactionRow`] from a parsed row's pieces (epic #899).
    ///
    /// `cells` is the collapsed column→value map (simple columns plus the
    /// collapsed `Value` for each complex column); `cell_meta` carries per-simple
    /// -cell write timestamps / TTLs; `complex` carries the per-element capture
    /// for the complex columns. The complex columns are split out of `cells` (the
    /// collapsed complex `Value` is dropped in favour of the per-element cells).
    ///
    /// A row deletion with no surviving cell produces
    /// [`CompactionRowData::Tombstone`]; one that COEXISTS with surviving cells
    /// (issue #932) produces `Live { row_deletion: Some(..) }`; an empty row (no
    /// cells, no tombstone) produces an empty `Live`.
    ///
    /// `is_static` is the row kind the decoder reported (issue #3809): it is what
    /// makes an EMPTY clustering legitimate rather than a lost identity — see
    /// [`CompactionRowData::require_tombstone_clustering_identity`], which this
    /// returns `Err` from rather than emit a row deletion that identifies no row.
    /// That check covers EVERY non-static row carrying a row deletion, on BOTH of
    /// the arms above, because both carry the same `deletion_time` into the merge.
    /// A row with NO row deletion is not checked — see the boundary note at the
    /// call site.
    ///
    /// The caller reports that `Err` as `DataRowOutcome::Refused`, never as
    /// `DataRowOutcome::DecodeFailed`: the row DECODED, so the refusal is a
    /// judgement about content and no refill can change it (#3782 vs #3809).
    #[allow(clippy::too_many_arguments)]
    pub(super) fn build_compaction_row_data(
        &self,
        cells: RowCells,
        cell_meta: Option<HashMap<String, CellWriteMetadata>>,
        complex: CompactionComplexColumns,
        row_header_opt: &Option<RowHeader>,
        row_ts: i64,
        schema: &TableSchema,
        is_static: bool,
    ) -> Result<crate::storage::sstable::reader::compaction_row::CompactionRowData> {
        use crate::storage::sstable::reader::compaction_row::{
            CompactionRowData, ComplexColumn, SimpleCell,
        };

        // Issue #932: a row with `HAS_DELETION` may ALSO carry surviving cells
        // (cells written strictly after the row deletion). The row deletion is
        // captured here either as the coexisting `row_deletion` on a `Live` row
        // (when data cells survive) or as a pure `Tombstone` (when only the
        // deletion remains). The decision is made AFTER building the cell sets so
        // we can tell whether any NON-clustering data cell survived.
        let row_deletion: Option<(i64, i32)> = row_header_opt
            .as_ref()
            .filter(|h| h.is_row_tombstone())
            .map(|h| {
                (
                    h.row_tombstone_deletion_time(),
                    // localDeletionTime in SECONDS (GC-grace clock). Preserve the
                    // far-future [2^31, 2^32) encoding via wrapping `as u32 as i32`.
                    h.local_deletion_time.unwrap_or(0),
                )
            });

        // Build complex columns (sorted by name for deterministic output, mirroring
        // the collapsed-value path's column ordering).
        let mut complex_cols: Vec<ComplexColumn> = complex
            .into_iter()
            .map(
                |(column, (complex_deletion, elements, collapsed_value))| ComplexColumn {
                    column,
                    complex_deletion,
                    elements,
                    collapsed_value,
                },
            )
            .collect();
        complex_cols.sort_by(|a, b| a.column.cmp(&b.column));

        // Simple cells are every collapsed cell whose column is NOT a complex
        // column. Per-cell timestamp / ttl / local-deletion-time come from
        // `cell_meta` when present, else inherit the row timestamp.
        let complex_names: std::collections::HashSet<&str> =
            complex_cols.iter().map(|c| c.column.as_str()).collect();

        let mut simple_cells: Vec<SimpleCell> = cells
            .into_iter()
            .filter(|(name, _)| !complex_names.contains(name.as_ref()))
            .map(|(column, value)| {
                let (timestamp, ttl, local_deletion_time) =
                    match cell_meta.as_ref().and_then(|m| m.get(column.as_ref())) {
                        Some(meta) => {
                            let ttl = meta.expiration.as_ref().map(|e| e.ttl_seconds as u32);
                            let ldt = meta
                                .expiration
                                .as_ref()
                                .map(|e| e.expires_at_seconds as u32 as i32);
                            (meta.write_timestamp_micros, ttl, ldt)
                        }
                        None => (row_ts, None, None),
                    };
                SimpleCell {
                    column: column.to_string(),
                    value,
                    timestamp,
                    ttl,
                    local_deletion_time,
                }
            })
            .collect();
        simple_cells.sort_by(|a, b| a.column.cmp(&b.column));

        // Issue #932: a row deletion either COEXISTS with surviving data cells
        // (kept as `Live { row_deletion: Some(..) }`) or — when no NON-primary-key
        // cell and no complex element survives — is a pure row tombstone (kept as
        // `Tombstone`, preserving the #912 clustering-prefix capture). The earlier
        // code always took the `Tombstone` branch, DROPPING surviving cells and
        // letting older cells of other columns resurrect in a partial compaction.
        if let Some((deletion_time, local_deletion_time)) = row_deletion {
            let primary_key: std::collections::HashSet<&str> = schema
                .partition_keys
                .iter()
                .map(|k| k.name.as_str())
                .chain(schema.clustering_keys.iter().map(|c| c.name.as_str()))
                .collect();
            let has_simple_data = simple_cells
                .iter()
                .any(|c| !primary_key.contains(c.column.as_str()));
            let has_complex_data = complex_cols
                .iter()
                .any(|c| !c.elements.is_empty() || c.complex_deletion.is_some());

            // Issue #3809 (Finding 1): rebuild the clustering prefix in schema
            // order from the surfaced clustering pseudo-cells (#912), and REFUSE
            // an incomplete one — for EVERY non-static row that carries a ROW
            // DELETION, i.e. BEFORE the pure-tombstone / coexisting-deletion
            // branch below. Both arms hand the SAME `deletion_time` to the merge
            // and `extract_clustering_key_from_compaction` maps an incomplete
            // clustering to `None` on both (from `Tombstone.clustering`, and for a
            // `Live` row from its `simple` cells), so both lose the row's identity
            // into the `None` reconcile bucket with the same consequence — the
            // whole harm path, and the two EXEMPT shapes (`is_static`, a table with
            // no clustering columns), are stated on
            // `require_tombstone_clustering_identity` itself.
            //
            // THE BOUNDARY, in consequence terms (issue #3809 AC4): a row carrying
            // NO row deletion is deliberately NOT validated here. Without a
            // `deletion_time` it can never become the `None` bucket's row deletion
            // and so shadows nothing — the harm above does not arise — while
            // refusing it would red a whole compaction read over a shape whose
            // worst outcome is the pre-#912 unclustered reconciliation of its own
            // cells, which this invariant is not the authority for. Pinned both
            // ways in `compaction_build_identity_tests.rs`.
            //
            // The vector is built ONCE for both arms and dropped on the coexistence
            // arm (which needs only its length): the clone is bounded by the
            // clustering arity and paid only by a row carrying a row deletion,
            // which beats two copies of the gap rule to keep in agreement.
            let mut clustering: Vec<(String, Value)> =
                Vec::with_capacity(schema.clustering_keys.len());
            for ck in &schema.clustering_keys {
                // PRESENCE is the whole test — the VALUE is deliberately not
                // judged, `Value::Null` included; the refutation and its Cassandra
                // citations are on the invariant itself (roborev #3809 job 93).
                match simple_cells.iter().find(|c| c.column == ck.name) {
                    Some(c) => clustering.push((ck.name.clone(), c.value.clone())),
                    // Stop at the first gap: `clustering.len()` is then the
                    // number of clustering values actually recovered, which is
                    // what the invariant below judges.
                    None => break,
                }
            }
            CompactionRowData::require_tombstone_clustering_identity(
                &self.keyspace,
                &self.table_name,
                is_static,
                schema.clustering_keys.len(),
                clustering.len(),
            )?;

            if !has_simple_data && !has_complex_data {
                // Pure row tombstone: emit the clustering prefix rebuilt above.
                //
                // A static row reaches here with `clustering` already EMPTY (it
                // carries no clustering prefix on disk, so no clustering
                // pseudo-cell is ever surfaced for it) — the `[]` spelling of
                // `Clustering.STATIC_CLUSTERING`. Nothing needs clearing.
                return Ok(CompactionRowData::Tombstone {
                    deletion_time,
                    local_deletion_time,
                    clustering,
                });
            }
        }

        // Issue #2374/#2789: carry the row-marker liveness so the READ path can
        // hide a row whose only content is an expired liveness marker + already-
        // tombstoned cells (carry-only; the write path ignores it).
        let row_liveness = row_header_opt
            .as_ref()
            .map(|h| h.row_liveness())
            .unwrap_or_default();

        Ok(CompactionRowData::Live {
            simple: simple_cells,
            complex: complex_cols,
            row_deletion,
            row_liveness,
        })
    }
}

// Issue #3809 (Finding 1, review round 2): the row-BUILD site of the
// clustering-identity invariant — that BOTH arms carrying a row deletion (a pure
// `Tombstone` and a `Live { row_deletion: Some(..) }`, issue #932) refuse an
// incomplete clustering, and that the stated boundary (a row with NO row
// deletion) does not. A child module of THIS one because that is where
// `build_compaction_row_data` lives; a separate file to keep this source under
// the campsite-rule size limit (epic #1116).
#[cfg(test)]
#[path = "compaction_build_identity_tests.rs"]
mod compaction_build_identity_tests;
