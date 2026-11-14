//! Row visibility for the cross-generation read merge (issue #4524).

use super::{CellData, HashSet, ReadShadow};
use crate::storage::sstable::reader::compaction_row::RowLiveness;

/// Names of the schema's STATIC columns.
pub(super) fn static_column_names(schema: &crate::schema::TableSchema) -> HashSet<String> {
    (schema.columns.iter())
        .filter(|c| c.is_static)
        .map(|c| c.name.clone())
        .collect()
}

/// The per-entry facts [`ReadShadow::filter_visible`] needs besides the cells (all `Copy`, read
/// off the `MergeEntry` BEFORE its `row_data` is moved out).
#[derive(Clone, Copy)]
pub(super) struct RowVis {
    pub(super) liveness: RowLiveness,
    /// `entry.clustering_key.is_none()`: the partition's static row.
    pub(super) static_row: bool,
    /// The entry's coexisting row deletion `(markedForDeleteAt µs, localDeletionTime s)`.
    pub(super) row_deletion: Option<(i64, i32)>,
}

impl RowVis {
    pub(super) fn of(entry: &super::MergeEntry) -> Self {
        Self {
            liveness: entry.row_liveness,
            static_row: entry.clustering_key.is_none(),
            row_deletion: entry.row_deletion,
        }
    }
}

impl ReadShadow {
    /// [`ReadShadow::filter_live`] plus the row-level visibility rule: `None` also when the row
    /// has neither a LIVE primary-key liveness marker nor a live non-key (data) cell.
    ///
    /// Cassandra 5.0.8 `BTreeRow.purge` -> `update` keeps a row iff it has a live liveness marker
    /// OR a live data cell; deleted cells are purged and never keep a row. The previous "drop
    /// rows left empty" rule lost the liveness-only / explicit-null row (marker, no live cell)
    /// and, on a clustered table, kept a marker-less row whose only surviving cells were its
    /// clustering pseudo-cells. Partition/clustering pseudo-cells (`key_columns`) are structural,
    /// kept verbatim by `filter_live`, and never count as data.
    ///
    /// A STATIC cell is never data of a CLUSTERING row either: Cassandra 5.0.8
    /// `AbstractRow.hasLiveData` (`db/rows/AbstractRow.java:48`) consults only the row's OWN
    /// liveness and its OWN `cells()`, and the static cells belong to the separate static `Row`
    /// (`AbstractRow.isStatic`: `clustering() == Clustering.STATIC_CLUSTERING`), which a read
    /// emits on its own. So a live static value must not keep a markerless, tombstone-only or
    /// expired clustering row visible. `static_row` is the static row itself (no clustering
    /// key), whose own static cells ARE its data.
    ///
    /// `row_deletion` is the entry's coexisting row deletion (`MergeEntry::row_deletion`, kept only
    /// when newer than some surviving cell): a marker it covers is NOT liveness.
    pub(super) fn filter_visible(
        &self,
        cover: Option<i64>,
        vis: RowVis,
        cells: Vec<CellData>,
    ) -> Option<Vec<CellData>> {
        let RowVis {
            liveness,
            static_row,
            row_deletion,
        } = vis;
        let surviving = self.filter_live(cover, liveness.marker_timestamp, cells)?;
        let has_data = surviving.iter().any(|c| {
            !self.key_columns.contains(&c.column)
                && (static_row || !self.static_columns.contains(&c.column))
        });
        // Roborev job 307: the marker counts only if it survives the row's coexisting deletion —
        // Cassandra 5.0.8 `DeletionTime.deletes(ts)` is `ts <= markedForDeleteAt` (equal loses).
        let marker_survives = row_deletion
            .zip(liveness.marker_timestamp)
            .is_none_or(|((deleted_at, _), marker)| marker > deleted_at);
        (has_data || (marker_survives && liveness.marker_live_at(self.now_secs)))
            .then_some(surviving)
    }
}
