//! `--deep`: ONE bounded streaming pass per generation (spec R3, `design.md` D1/D2).
//!
//! The pass rides the existing compaction streaming driver (one structure resident at a time) via
//! [`SSTableReader::scan_rows_with_partition_starts`], which reports each partition's START offset
//! and key plus every RAW (unreconciled) row. From that single pass:
//!
//! * **size** = the partition's extent in `Data.db` in the UNCOMPRESSED domain (next partition
//!   start − this start; the data-section length for the last one). This is what the file
//!   physically holds. NOTE: `sstablemetadata -s` ranks "largest" by the in-memory
//!   `Row.dataSize()` sum instead, which is not derivable from the file layout alone; the field
//!   description says so.
//! * **width** = rows in the partition (a static row counts as one, a row deletion is a row), as
//!   `sstablemetadata -s`'s `rowCount`/`pcount`.
//! * **tombstones** = EXACTLY `printScannedOverview`'s `ptombcount` (cassandra-5.0.8
//!   `SSTableMetadataViewer.java`): +1 for a partition-level deletion, +1 per range-tombstone
//!   marker (a paired [`CompactionRowData::RangeMarker`] is its start and end bound = 2), +1 per
//!   cell tombstone (simple cell holding a tombstone, or a deleted complex element). A row
//!   deletion, an expired TTL cell and a complex-column deletion are NOT counted by Cassandra's
//!   tool, so they are not counted here. No `gcBefore` filter, no cross-generation reconciliation
//!   (that is `cqlite tombstones`, #4200).

use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};

use crate::error::Error;
use crate::schema::TableSchema;
use crate::storage::sstable::reader::compaction_row::{CompactionRow, CompactionRowData};
use crate::storage::sstable::reader::SSTableReader;
use crate::types::Value;
use crate::Result;

/// Partition-size bucket lower edges (bytes); each bucket is `[lo, next_lo)`, the last unbounded.
pub const SIZE_BUCKET_LOWER_EDGES: &[u64] = &[
    0, 64, 256, 1_024, 4_096, 16_384, 65_536, 262_144, 1_048_576, 4_194_304,
];
/// Clustering-width bucket lower edges (rows); same half-open rule.
pub const WIDTH_BUCKET_LOWER_EDGES: &[u64] = &[0, 1, 2, 4, 16, 64, 256, 1_024, 4_096, 16_384];

/// `(lower_inclusive, upper_exclusive, count)`; the last bucket's upper edge is `u64::MAX`.
pub type HistogramBucket = (u64, u64, u64);

/// A ranked partition: raw key bytes and the ranking metric.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RankedPartition {
    pub key: Vec<u8>,
    pub metric: u64,
}

/// Everything one generation's pass produced.
#[derive(Debug, Clone, Default)]
pub struct DeepScanResult {
    /// Raw rows streamed (every [`CompactionRow`], tombstone markers included).
    pub rows_scanned: u64,
    pub partitions: u64,
    pub partition_size_histogram: Vec<HistogramBucket>,
    pub clustering_width_histogram: Vec<HistogramBucket>,
    pub top_largest_partitions: Vec<RankedPartition>,
    pub top_tombstone_heaviest_partitions: Vec<RankedPartition>,
    /// Sum of every partition's raw tombstone count (Cassandra's `tombstoneCount`).
    pub tombstones_total: u64,
}

fn empty_histogram(lower_edges: &[u64]) -> Vec<HistogramBucket> {
    lower_edges
        .iter()
        .enumerate()
        .map(|(i, lo)| (*lo, lower_edges.get(i + 1).copied().unwrap_or(u64::MAX), 0))
        .collect()
}

fn add_to_histogram(h: &mut [HistogramBucket], value: u64) {
    if let Some(b) = h.iter_mut().rev().find(|b| value >= b.0) {
        b.2 += 1;
    }
}

/// Keep the `top_n` highest metrics, descending; ties keep first-seen order.
fn insert_ranked(list: &mut Vec<RankedPartition>, top_n: usize, key: &[u8], metric: u64) {
    if top_n == 0 {
        return;
    }
    let pos = list.partition_point(|p| p.metric >= metric);
    if pos < top_n {
        list.insert(
            pos,
            RankedPartition {
                key: key.to_vec(),
                metric,
            },
        );
        list.truncate(top_n);
    }
}

/// Raw tombstone markers carried by one row, per `printScannedOverview` (see module doc).
pub(crate) fn raw_tombstones_in_row(row: &CompactionRow) -> u64 {
    match &row.row_data {
        CompactionRowData::PartitionDelete { .. } => 1,
        CompactionRowData::RangeMarker { .. } => 2,
        // A row deletion is a ROW with no cells: Cassandra counts no tombstone for it.
        CompactionRowData::Tombstone { .. } => 0,
        CompactionRowData::Live {
            simple, complex, ..
        } => {
            let cells = simple
                .iter()
                .filter(|c| matches!(c.value, Value::Tombstone(_)))
                .count();
            let elements = complex
                .iter()
                .flat_map(|c| c.elements.iter())
                .filter(|e| e.is_deleted)
                .count();
            (cells + elements) as u64
        }
    }
}

/// Whether the row is a Cassandra `Row`/static row (counts toward width), as opposed to a
/// partition deletion or a range-tombstone marker.
fn is_row(row: &CompactionRow) -> bool {
    matches!(
        row.row_data,
        CompactionRowData::Live { .. } | CompactionRowData::Tombstone { .. }
    )
}

struct Current {
    key: Vec<u8>,
    start: u64,
    rows: u64,
    tombstones: u64,
}

struct Accumulator {
    top_n: usize,
    current: Option<Current>,
    /// Rows already seen for a partition whose boundary has NOT been reported yet (the driver
    /// emits a partition-level deletion row before it reports that partition's start).
    ahead: Option<Current>,
    rows_scanned: u64,
    partitions: u64,
    tombstones_total: u64,
    size_hist: Vec<HistogramBucket>,
    width_hist: Vec<HistogramBucket>,
    largest: Vec<RankedPartition>,
    tombstoned: Vec<RankedPartition>,
}

impl Accumulator {
    fn new(top_n: usize) -> Self {
        Self {
            top_n,
            current: None,
            ahead: None,
            rows_scanned: 0,
            partitions: 0,
            tombstones_total: 0,
            size_hist: empty_histogram(SIZE_BUCKET_LOWER_EDGES),
            width_hist: empty_histogram(WIDTH_BUCKET_LOWER_EDGES),
            largest: Vec::new(),
            tombstoned: Vec::new(),
        }
    }

    /// Close the current partition at `end` (exclusive offset).
    fn close(&mut self, end: u64) {
        let Some(cur) = self.current.take() else {
            return;
        };
        let size = end.saturating_sub(cur.start);
        self.partitions += 1;
        add_to_histogram(&mut self.size_hist, size);
        add_to_histogram(&mut self.width_hist, cur.rows);
        insert_ranked(&mut self.largest, self.top_n, &cur.key, size);
        if cur.tombstones > 0 {
            insert_ranked(&mut self.tombstoned, self.top_n, &cur.key, cur.tombstones);
        }
    }

    fn open(&mut self, start: u64, key: &[u8]) {
        self.close(start);
        let mut cur = match self.ahead.take() {
            Some(a) if a.key == key => a,
            other => {
                // Rows buffered for a DIFFERENT key cannot exist (partitions are sequential);
                // keep them out of this partition rather than mis-attribute them.
                self.ahead = other;
                Current {
                    key: key.to_vec(),
                    start,
                    rows: 0,
                    tombstones: 0,
                }
            }
        };
        cur.start = start;
        self.current = Some(cur);
    }

    fn row(&mut self, row: &CompactionRow) {
        self.rows_scanned += 1;
        let tombs = raw_tombstones_in_row(row);
        self.tombstones_total += tombs;
        let key = row.key.as_bytes();
        let target = match self.current.as_mut() {
            Some(cur) if cur.key == key => cur,
            _ => self.ahead.get_or_insert_with(|| Current {
                key: key.to_vec(),
                start: 0,
                rows: 0,
                tombstones: 0,
            }),
        };
        target.tombstones += tombs;
        if is_row(row) {
            target.rows += 1;
        }
    }
}

fn poisoned() -> Error {
    Error::internal("deep-scan accumulator lock poisoned".to_string())
}

/// Run the single `--deep` pass over one generation.
pub(crate) async fn deep_scan_generation(
    reader: &SSTableReader,
    schema: Option<&TableSchema>,
    top_n: usize,
) -> Result<DeepScanResult> {
    let Some(data_len) = reader.uncompressed_data_section_len() else {
        return Err(Error::invalid_input(
            "no authoritative uncompressed Data.db length (missing/zero CompressionInfo.db \
             data_length): partition sizes would be guesses"
                .to_string(),
        ));
    };
    let acc = Arc::new(Mutex::new(Accumulator::new(top_n)));
    {
        let for_start = Arc::clone(&acc);
        let for_row = Arc::clone(&acc);
        let mut on_partition = move |start: u64, key: &crate::types::RowKey| -> Result<()> {
            for_start
                .lock()
                .map_err(|_| poisoned())?
                .open(start, key.as_bytes());
            Ok(())
        };
        reader
            .scan_rows_with_partition_starts(schema, &mut on_partition, move |row| {
                for_row.lock().map_err(|_| poisoned())?.row(&row);
                Ok(ControlFlow::Continue(()))
            })
            .await?;
    }
    let mut a = acc.lock().map_err(|_| poisoned())?;
    a.close(data_len);
    Ok(DeepScanResult {
        rows_scanned: a.rows_scanned,
        partitions: a.partitions,
        partition_size_histogram: std::mem::take(&mut a.size_hist),
        clustering_width_histogram: std::mem::take(&mut a.width_hist),
        top_largest_partitions: std::mem::take(&mut a.largest),
        top_tombstone_heaviest_partitions: std::mem::take(&mut a.tombstoned),
        tombstones_total: a.tombstones_total,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_buckets_are_half_open_and_cover_everything() {
        let mut h = empty_histogram(SIZE_BUCKET_LOWER_EDGES);
        for v in [0, 63, 64, 255, 256, 5_000_000, u64::MAX] {
            add_to_histogram(&mut h, v);
        }
        assert_eq!(h[0].2, 2, "0 and 63 are in [0,64)");
        assert_eq!(h[1].2, 2, "64 and 255 are in [64,256)");
        assert_eq!(h[2].2, 1, "256 is in [256,1024)");
        assert_eq!(h.last().map(|b| b.2), Some(2), "last bucket is unbounded");
        assert_eq!(h.iter().map(|b| b.2).sum::<u64>(), 7);
    }

    #[test]
    fn insert_ranked_keeps_top_n_descending_and_zero_is_a_noop() {
        let mut list = Vec::new();
        insert_ranked(&mut list, 2, b"a", 5);
        insert_ranked(&mut list, 2, b"b", 10);
        insert_ranked(&mut list, 2, b"c", 1);
        assert_eq!(list.len(), 2);
        assert_eq!((list[0].metric, list[1].metric), (10, 5));
        let mut none = Vec::new();
        insert_ranked(&mut none, 0, b"a", 5);
        assert!(none.is_empty());
    }
}
