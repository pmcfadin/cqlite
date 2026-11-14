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
    pub(super) fn filter_visible(
        &self,
        cover: Option<i64>,
        liveness: RowLiveness,
        cells: Vec<CellData>,
        static_row: bool,
    ) -> Option<Vec<CellData>> {
        let surviving = self.filter_live(cover, liveness.marker_timestamp, cells)?;
        let has_data = surviving.iter().any(|c| {
            !self.key_columns.contains(&c.column)
                && (static_row || !self.static_columns.contains(&c.column))
        });
        (has_data || liveness.marker_live_at(self.now_secs)).then_some(surviving)
    }
}
