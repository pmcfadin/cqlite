//! Reader→merge row/cell conversion helpers for [`SSTableRowIteratorAdapter`].
//!
//! PURE CODE MOTION out of `merge/mod.rs` (issue #3139), same rationale as the
//! sibling [`producer_iter`](super::producer_iter): `mod.rs` is ~13.5k lines,
//! far over the ~800-line source campsite target (epic #1116). The moved items
//! are byte-identical to their pre-move form apart from the `pub(super)`
//! visibility the move requires.
//!
//! These are the pure translation steps BOTH producer shapes run per streamed
//! source entry — the path-based producer thread
//! ([`producer_iter`](super::producer_iter)) and the shared-reader one
//! ([`from_readers`](super::from_readers)) both funnel through
//! [`SSTableRowIteratorAdapter::build_merge_entry`]: `CompactionRow` →
//! [`MergeEntry`], `CompactionRowData` → [`RowData`], reader `Value` →
//! [`RowData`], plus clustering-key extraction and range-bound translation.
//! They touch no channel and no gauge; they are kept as inherent methods on the
//! adapter (an `impl` block in a sibling file, exactly as
//! [`from_readers`](super::from_readers) does) so no call site changes.

use super::producer_iter::SSTableRowIteratorAdapter;
use super::{CellData, ComplexDeletion, MergeEntry, RowData};
use crate::error::Result;
use crate::schema::TableSchema;
use crate::storage::write_engine::mutation::{ClusteringKey, DecoratedKey, RangeTombstone};
use crate::types::Value;

#[cfg(feature = "write-support")]
impl SSTableRowIteratorAdapter {
    /// Convert one streamed `(RowKey, Value, timestamp)` source entry into a
    /// [`MergeEntry`] for run `run_index` (issue #827).
    ///
    /// Factored out of the producer loop so the streaming emit callback can call
    /// it inline. Populates the clustering key from the decoded cells so wide-row
    /// (clustering) partitions reconcile per `(pk, ck)` instead of collapsing
    /// into one row.
    pub(super) fn build_merge_entry(
        run_index: usize,
        compaction_row: crate::storage::sstable::reader::compaction_row::CompactionRow,
        schema: &TableSchema,
    ) -> Result<MergeEntry> {
        use crate::storage::sstable::reader::compaction_row::CompactionRowData;

        let crate::storage::sstable::reader::compaction_row::CompactionRow {
            key,
            row_timestamp,
            row_data,
        } = compaction_row;
        let decorated_key = DecoratedKey::from_key_bytes(key.as_bytes().to_vec())?;

        // Issue #1072: a partition-level tombstone surfaces as a self-contained
        // carrier `MergeEntry` (empty live row + `partition_deletion`, no
        // clustering). `merge_partition_rows` extracts these, applies the MAX
        // partition floor across sources to shadow older cells/rows/ranges, and
        // re-emits the surviving partition tombstone. Handled BEFORE the
        // `RangeMarker` block so the partition floor is the outermost shadow.
        if let CompactionRowData::PartitionDelete {
            deletion_time,
            local_deletion_time,
        } = row_data
        {
            // Carry the deletion in `RowData::Tombstone` so the carrier never
            // surfaces as a phantom live row to consumers iterating the merge step
            // stream; `partition_deletion` marks it as the partition-level carrier
            // and `merge_entry_to_mutation` lifts it onto the partition header
            // (emitting NO clustering-row `DeleteRow`).
            return Ok(MergeEntry::new(
                run_index,
                decorated_key,
                None,
                // Use markedForDeleteAt as the entry timestamp so stats baselines
                // see a real value (not 0) for the carrier.
                deletion_time,
                RowData::Tombstone {
                    deletion_time,
                    local_deletion_time,
                },
            )
            .with_partition_deletion((deletion_time, local_deletion_time)));
        }

        // Issue #933: a range-tombstone marker surfaces as a self-contained
        // carrier `MergeEntry` (empty live row + `range_deletion`), routed into the
        // partition's `None` clustering bucket so it is never collapsed with data
        // rows. `merge_partition_rows` extracts these to shadow covered cells and
        // re-emit the surviving marker to the output SSTable.
        if let CompactionRowData::RangeMarker {
            start,
            end,
            deletion_time,
            local_deletion_time,
        } = row_data
        {
            let range = RangeTombstone {
                start: Self::compaction_bound_to_mutation(start),
                end: Self::compaction_bound_to_mutation(end),
                deletion_time,
                local_deletion_time,
            };
            return Ok(MergeEntry::new(
                run_index,
                decorated_key,
                None,
                // Use the deletion time as the entry timestamp so the writer's
                // stats baselines see a real value (not 0) for the carrier.
                deletion_time,
                RowData::Live { cells: Vec::new() },
            )
            .with_range_deletion(range));
        }
        // #912: derive the clustering identity from the per-element compaction row
        // BEFORE collapsing it to `RowData`. A row tombstone carries no cells in
        // `RowData::Tombstone`, so its clustering key must come from the
        // tombstone's captured clustering prefix; otherwise it would collapse into
        // the partition's `None` bucket and mis-reconcile against the static row
        // and against other clustering-row tombstones.
        let clustering_key = Self::extract_clustering_key_from_compaction(&row_data, schema);
        let (row_data, complex_deletions, row_deletion, row_liveness) =
            Self::compaction_row_data_to_row_data(row_data, row_timestamp);
        let entry = MergeEntry::new(
            run_index,
            decorated_key,
            clustering_key,
            row_timestamp,
            row_data,
        )
        // Issue #2374/#2789: carry the row-marker liveness for the read path.
        .with_row_liveness(row_liveness);
        let entry = if complex_deletions.is_empty() {
            entry
        } else {
            entry.with_complex_deletions(complex_deletions)
        };
        // Issue #932: carry the coexisting row deletion so reconciliation keeps it
        // alongside the surviving live cells (preventing resurrection of older
        // cells in non-compacted SSTables).
        let entry = match row_deletion {
            Some((deletion_time, ldt)) => entry.with_row_deletion(deletion_time, ldt),
            None => entry,
        };
        Ok(entry)
    }

    /// Convert a reader-native [`CompactionBound`] into a write-engine
    /// [`ClusteringBound`] (issue #933).
    ///
    /// [`CompactionBound`]: crate::storage::sstable::reader::compaction_row::CompactionBound
    /// [`ClusteringBound`]: crate::storage::write_engine::mutation::ClusteringBound
    fn compaction_bound_to_mutation(
        bound: crate::storage::sstable::reader::compaction_row::CompactionBound,
    ) -> crate::storage::write_engine::mutation::ClusteringBound {
        use crate::storage::sstable::reader::compaction_row::CompactionBound;
        use crate::storage::write_engine::mutation::ClusteringBound;
        match bound {
            CompactionBound::Inclusive(cols) => {
                ClusteringBound::Inclusive(ClusteringKey { columns: cols })
            }
            CompactionBound::Exclusive(cols) => {
                ClusteringBound::Exclusive(ClusteringKey { columns: cols })
            }
            CompactionBound::Bottom => ClusteringBound::Bottom,
            CompactionBound::Top => ClusteringBound::Top,
        }
    }

    /// Extract a `ClusteringKey` from the row's live cells using the schema.
    ///
    /// For each clustering column declared in the schema (in position order),
    /// look for a cell with that column name in the decoded `RowData::Live`
    /// cells.  If all clustering columns are found, return `Some(ClusteringKey)`;
    /// otherwise (including for tombstone entries that have no cells) return
    /// `None`.
    ///
    /// The clustering columns are intentionally left inside the cells so the
    /// downstream read-back path can still find them.
    fn extract_clustering_key_from_compaction(
        row_data: &crate::storage::sstable::reader::compaction_row::CompactionRowData,
        schema: &TableSchema,
    ) -> Option<ClusteringKey> {
        use crate::storage::sstable::reader::compaction_row::CompactionRowData;

        if schema.clustering_keys.is_empty() {
            return None;
        }

        match row_data {
            // A live row surfaces its clustering columns as simple cells (#229).
            CompactionRowData::Live { simple, .. } => {
                let mut ck_columns: Vec<(String, Value)> =
                    Vec::with_capacity(schema.clustering_keys.len());
                for ck_col in &schema.clustering_keys {
                    // Any missing clustering column ⇒ the WHOLE key is discarded (the `?`
                    // returns `None`, treating the row as unclustered).
                    let pair = simple
                        .iter()
                        .find(|c| c.column == ck_col.name)
                        .map(|c| (ck_col.name.clone(), c.value.clone()))?;
                    ck_columns.push(pair);
                }
                Some(ClusteringKey {
                    columns: ck_columns,
                })
            }
            // #912: a row tombstone carries its own clustering prefix so it
            // reconciles in its own bucket instead of collapsing into `None`.
            // An empty `clustering` (unclustered table / partial prefix) keeps the
            // pre-#912 `None`-bucket behavior.
            CompactionRowData::Tombstone { clustering, .. } => {
                if clustering.is_empty() {
                    return None;
                }
                // Reorder defensively into schema order; bail to `None` if any
                // declared clustering column is absent.
                let mut ck_columns: Vec<(String, Value)> =
                    Vec::with_capacity(schema.clustering_keys.len());
                for ck_col in &schema.clustering_keys {
                    // Any missing clustering column ⇒ the WHOLE key is discarded (the `?`
                    // returns `None`, treating the row as unclustered).
                    let pair = clustering
                        .iter()
                        .find(|(name, _)| name == &ck_col.name)
                        .map(|(name, v)| (name.clone(), v.clone()))?;
                    ck_columns.push(pair);
                }
                Some(ClusteringKey {
                    columns: ck_columns,
                })
            }
            // Issue #933: a range marker is handled by `build_merge_entry` before
            // this point and routed into the `None` clustering bucket.
            CompactionRowData::RangeMarker { .. } => None,
            // Issue #1072: a partition tombstone is handled by `build_merge_entry`
            // before this point and routed into the `None` clustering bucket.
            CompactionRowData::PartitionDelete { .. } => None,
        }
    }

    /// Convert a [`CompactionRowData`] into the merge `RowData` plus the row's
    /// complex-deletion markers (epic #899, Phase C — the behavioral flip).
    ///
    /// Simple columns become one [`CellData`] each (cell-own ts/ttl/ldt). A
    /// complex (non-frozen collection / UDT) column is NO LONGER collapsed to a
    /// single whole-column value: each [`ComplexElement`] becomes its OWN
    /// per-element [`CellData`] carrying the element's authoritative `cell_path`,
    /// per-element `timestamp`, `ttl`, `local_deletion_time`, `is_deleted`, and
    /// on-disk `has_empty_value`. `reconcile_cluster` keys winners on
    /// `(column, cell_path)`, so disjoint elements of the same column written
    /// across SSTables all survive (Cassandra `Cells#reconcile`), and the
    /// merge→mutation step emits a [`CellOperation::WriteComplexElement`] per
    /// element — preserving the per-element on-disk layout byte-for-byte (epic
    /// #899 north star). The reader's whole-collection `collapsed_value` is no
    /// longer threaded to the writer (the per-element path is now authoritative);
    /// it remains on the reader contract for user-facing reads.
    ///
    /// The real per-column complex deletion (`markedForDeleteAt` +
    /// `localDeletionTime`) is surfaced on `complex_deletions` so the writer
    /// emits a REAL deletion marker (replacing the LIVE sentinel). `reconcile_cluster`
    /// reduces these to the strict-superseding (max-mfda) deletion per column NAME
    /// and SHADOWS covered elements (ts <= mfda) before purge (issue #887). gc_grace
    /// purging of the surviving marker remains future work (#845).
    ///
    /// [`ComplexElement`]: crate::storage::sstable::reader::compaction_row::ComplexElement
    /// [`CellOperation::WriteComplexElement`]: crate::storage::write_engine::mutation::CellOperation::WriteComplexElement
    #[allow(clippy::type_complexity)]
    pub(super) fn compaction_row_data_to_row_data(
        row_data: crate::storage::sstable::reader::compaction_row::CompactionRowData,
        _row_timestamp: i64,
    ) -> (
        RowData,
        Vec<ComplexDeletion>,
        Option<(i64, i32)>,
        crate::storage::sstable::reader::compaction_row::RowLiveness,
    ) {
        use crate::storage::sstable::reader::compaction_row::{CompactionRowData, RowLiveness};

        match row_data {
            CompactionRowData::Tombstone {
                deletion_time,
                local_deletion_time,
                // Clustering identity is consumed by
                // `extract_clustering_key_from_compaction` before this conversion
                // (#912); `RowData::Tombstone` itself carries only the timestamps.
                clustering: _,
            } => (
                RowData::Tombstone {
                    deletion_time,
                    local_deletion_time,
                },
                Vec::new(),
                None,
                RowLiveness::default(),
            ),
            CompactionRowData::Live {
                simple,
                complex,
                // Issue #932: a coexisting row deletion surfaces here so the
                // merge entry carries it alongside the live cells.
                row_deletion,
                // Issue #2374/#2789: primary-key liveness carried for the read path.
                row_liveness,
            } => {
                let element_count: usize = complex.iter().map(|c| c.elements.len()).sum();
                let mut cells = Vec::with_capacity(simple.len() + element_count);
                let mut complex_deletions = Vec::new();

                for sc in simple {
                    cells.push(CellData {
                        column: sc.column,
                        value: sc.value,
                        timestamp: sc.timestamp,
                        ttl: sc.ttl,
                        cell_path: None,
                        local_deletion_time: sc.local_deletion_time,
                        is_complex_element: false,
                        is_deleted: false,
                        has_empty_value: false,
                    });
                }

                for col in complex {
                    // Surface the REAL complex deletion (replacing the LIVE
                    // sentinel) so the writer emits a genuine marker (Phase C).
                    if let Some((marked_for_delete_at, ldt)) = col.complex_deletion {
                        complex_deletions.push(ComplexDeletion {
                            column: col.column.clone(),
                            marked_for_delete_at,
                            local_deletion_time: ldt,
                        });
                    }

                    // Per-element emit: one CellData per ComplexElement, keyed by
                    // its authoritative cell_path, carrying per-element write
                    // metadata verbatim. An element tombstone is represented with
                    // `is_deleted = true` and a `Value::Tombstone(CellTombstone)`
                    // value so the per-cell reconcile tie-break (tombstone beats
                    // live at equal ts) still applies; a live element keeps its
                    // decoded value (the reader stores the SET member decoded from
                    // the path, but `has_empty_value` records the on-disk emptiness
                    // so the writer reproduces it byte-for-byte).
                    for elem in col.elements {
                        let value = if elem.is_deleted {
                            Value::Tombstone(Box::new(crate::types::TombstoneInfo {
                                deletion_time: elem.timestamp,
                                tombstone_type: crate::types::TombstoneType::CellTombstone,
                                // Element's on-disk localDeletionTime (GC clock,
                                // seconds); `0` when not surfaced (#873).
                                local_deletion_time: elem.local_deletion_time.unwrap_or(0) as i64,
                                ttl: None,
                                range_start: None,
                                range_end: None,
                            }))
                        } else {
                            elem.value.unwrap_or(Value::Null)
                        };
                        cells.push(CellData {
                            column: col.column.clone(),
                            value,
                            timestamp: elem.timestamp,
                            ttl: elem.ttl,
                            cell_path: Some(elem.cell_path),
                            local_deletion_time: elem.local_deletion_time,
                            is_complex_element: true,
                            is_deleted: elem.is_deleted,
                            has_empty_value: elem.has_empty_value,
                        });
                    }
                }

                (
                    RowData::Live { cells },
                    complex_deletions,
                    row_deletion,
                    row_liveness,
                )
            }
            // Issue #933: range markers are intercepted in `build_merge_entry`
            // (carrier entry with `range_deletion`); they never reach this
            // conversion. Map defensively to an empty live row.
            CompactionRowData::RangeMarker { .. } => (
                RowData::Live { cells: Vec::new() },
                Vec::new(),
                None,
                RowLiveness::default(),
            ),
            // Issue #1072: partition tombstones are intercepted in
            // `build_merge_entry` (carrier entry with `partition_deletion`); they
            // never reach this conversion. Map defensively to an empty live row.
            CompactionRowData::PartitionDelete { .. } => (
                RowData::Live { cells: Vec::new() },
                Vec::new(),
                None,
                RowLiveness::default(),
            ),
        }
    }

    /// Convert a reader Value to RowData.
    ///
    /// `row_timestamp` is the per-row timestamp decoded from the on-disk row
    /// header (see [`SSTableReader::iterate_all_partitions_for_compaction`]). The
    /// reader does not surface per-cell timestamps for live cells, so each live
    /// cell inherits the row timestamp. This is required for per-cell reconcile
    /// and row-tombstone shadowing to compare cell timestamps correctly
    /// (Issue #533) — without it live cells would default to 0 and be wrongly
    /// shadowed by any row tombstone.
    ///
    /// Issue #505: `Value::Tombstone(RowTombstone)` is now correctly emitted by
    /// the V5CompressedLegacy parser for deleted rows, and
    /// `Value::Tombstone(CellTombstone)` appears inside `Value::Map` entries for
    /// deleted cells.  Both are surfaced here so the merger can apply shadowing
    /// semantics.  A cell tombstone keeps its own `deletion_time` so equal-ts
    /// reconcile still resolves it correctly.
    ///
    /// Epic #899: superseded on the production path by
    /// [`Self::compaction_row_data_to_row_data`] (the reader now surfaces
    /// per-element [`CompactionRow`]s). Retained for the legacy-collapse merge
    /// tests that assert the old whole-column collapse behavior.
    #[cfg(test)]
    pub(super) fn value_to_row_data(value: &crate::types::Value, row_timestamp: i64) -> Result<RowData> {
        match value {
            crate::types::Value::Tombstone(info) => Ok(RowData::Tombstone {
                deletion_time: info.deletion_time,
                // #873: TombstoneInfo now carries the source localDeletionTime
                // (GC clock, seconds). `RowData::Tombstone` stores it as i32
                // (the on-disk width); the value fits an i32 in practice
                // (epoch-seconds, incl. far-future negative-i32 bit patterns).
                // NOTE: this legacy adapter is `#[cfg(test)]` only — the
                // production compaction tombstone path is
                // `compaction_row_data_to_row_data`, which already threads the
                // LDT from `CompactionRowData::Tombstone`.
                local_deletion_time: info.local_deletion_time as i32,
            }),
            crate::types::Value::Map(map_entries) => {
                let mut cells = Vec::with_capacity(map_entries.len());
                for (key, val) in map_entries {
                    let column = match key {
                        crate::types::Value::Text(s) => String::from_utf8_lossy(s).into_owned(),
                        other => format!("{:?}", other),
                    };
                    // Cell tombstones carry their own deletion_time (Issue #505);
                    // live cells inherit the row timestamp (Issue #533) so per-cell
                    // shadowing and LWW order them against row tombstones correctly.
                    let cell_ts = match val {
                        crate::types::Value::Tombstone(info) => info.deletion_time,
                        _ => row_timestamp,
                    };
                    cells.push(CellData {
                        column,
                        value: val.clone(),
                        timestamp: cell_ts,
                        // ttl / local_deletion_time / cell_path are threaded for
                        // the followup behaviors (#844, #848) but the reader's
                        // `(RowKey, Value, ts)` compaction stream does not yet
                        // surface per-cell ttl, the cell's local-deletion-time,
                        // or a complex-column cell-path (the map key here is the
                        // top-level column name, not a collection element path),
                        // so they stay `None`. Populating them is part of #844 /
                        // #848 once the reader is extended (issue #886 plumbing).
                        ttl: None,
                        cell_path: None,
                        local_deletion_time: None,
                        is_complex_element: false,
                        is_deleted: false,
                        has_empty_value: false,
                    });
                }
                Ok(RowData::Live { cells })
            }
            // Single value or other formats - wrap as a single cell
            other => Ok(RowData::Live {
                cells: vec![CellData {
                    column: "value".to_string(),
                    value: other.clone(),
                    timestamp: row_timestamp,
                    // Not surfaced by the reader's compaction stream yet; see the
                    // note on the map-entry path above (issue #886 plumbing).
                    ttl: None,
                    cell_path: None,
                    local_deletion_time: None,
                    is_complex_element: false,
                    is_deleted: false,
                    has_empty_value: false,
                }],
            }),
        }
    }
}
