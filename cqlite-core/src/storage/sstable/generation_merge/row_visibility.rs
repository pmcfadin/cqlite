//! Row visibility for the cross-generation read merge (issue #4524).

use super::{CellData, ReadShadow};
use crate::storage::sstable::reader::compaction_row::RowLiveness;

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
    pub(super) fn filter_visible(
        &self,
        cover: Option<i64>,
        liveness: RowLiveness,
        cells: Vec<CellData>,
    ) -> Option<Vec<CellData>> {
        let surviving = self.filter_live(cover, liveness.marker_timestamp, cells)?;
        let has_data = surviving
            .iter()
            .any(|c| !self.key_columns.contains(&c.column));
        (has_data || liveness.marker_live_at(self.now_secs)).then_some(surviving)
    }
}
