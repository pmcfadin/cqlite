//! `--deep`'s one additional bounded streaming scan per generation (issue #4204,
//! `design.md` D1/D2, spec R3).
//!
//! Reuses the reader-level per-row streaming scan
//! ([`SSTableReader::scan_stream`], issue #790) — the SAME bounded-channel,
//! one-partition-buffered-at-a-time primitive the point-compaction/full-scan
//! paths already use — rather than a materializing `scan()`/`get_all_entries()`
//! call, so peak resident memory stays O(`top_n` + histogram buckets), never
//! O(partitions) (spec R3.3).
//!
//! # Scope decision (flagged, issue #4204 implementer report)
//!
//! `sstablemetadata -s`'s `mostTombstones`/`largestPartitions`/
//! `widestPartitions` queues are filled from a scan that has direct access to
//! Cassandra's on-disk row/marker encoding. CQLite's equivalent signal here is
//! built from the ALREADY-DECODED [`ScanRow`] stream: partition "size" is the SUM
//! of each row entry's [`ScanRow::len`] (cell count for a decoded [`ScanRow::Row`],
//! raw byte length for [`ScanRow::RawRow`]) — an diagnose-OWN, independently
//! defined byte/weight proxy (D1: "independently bucketed by diagnose, NOT read
//! from Statistics.db"), not a byte-for-byte replica of Cassandra's on-disk
//! partition size. Tombstone counting matches D2 exactly in KIND (a flat,
//! per-generation, un-reconciled marker count) but is scoped to markers the
//! per-row stream actually surfaces as [`ScanRow::Marker`] — a partition-level
//! deletion that never reaches the row stream as its own item is not separately
//! counted here (this scan counts ROW/RANGE/CELL tombstone markers, not
//! partition-level deletions). Both are stated here plainly rather than silently
//! approximated.

use std::sync::Arc;

use crate::schema::TableSchema;
use crate::storage::sstable::reader::SSTableReader;
use crate::types::{ScanRow, TableId, Value};
use crate::Result;

/// Fixed histogram bucket edges shared by the partition-size and
/// clustering-width histograms (diagnose's own bucketing, D1) — a simple
/// power-of-roughly-4 ladder wide enough to span a single-row partition through
/// a multi-megabyte wide partition.
const BUCKET_EDGES: &[u64] = &[
    0,
    64,
    256,
    1_024,
    4_096,
    16_384,
    65_536,
    262_144,
    1_048_576,
    4_194_304,
    u64::MAX,
];

/// One `(lo, hi, count)` histogram bucket. `hi == u64::MAX` marks the open-ended
/// overflow bucket.
pub type HistogramBucket = (u64, u64, u64);

/// One top-N ranking entry: the partition's raw key bytes plus its ranked metric.
#[derive(Debug, Clone)]
pub struct RankedPartition {
    pub key: Vec<u8>,
    pub metric: u64,
}

/// Result of one generation's `--deep` scan (spec R3).
#[derive(Debug, Clone, Default)]
pub struct DeepScanResult {
    /// Total row/entry count the scan observed (never opens Data.db unless
    /// `--deep` — this field only exists when it does).
    pub rows_scanned: u64,
    pub partition_size_histogram: Vec<HistogramBucket>,
    pub clustering_width_histogram: Vec<HistogramBucket>,
    /// Top `top_n` largest partitions by the diagnose-own size proxy (D1).
    pub top_largest_partitions: Vec<RankedPartition>,
    /// Top `top_n` tombstone-heaviest partitions by RAW per-generation marker
    /// count (D2 — never #4200's reconciled count).
    pub top_tombstone_heaviest_partitions: Vec<RankedPartition>,
    /// Every tombstone marker's raw count, this generation.
    pub tombstones_total: u64,
    /// Of those, the count whose `local_deletion_time` is `> 0` (a genuinely
    /// decoded LDT, never the "unknown" `0` sentinel) AND `< gc_before_secs` —
    /// i.e. droppable at the report's `--now` (feeds `reclaim_prediction`).
    pub tombstones_droppable_at_gc_before: u64,
}

fn bucketize(mut values: Vec<u64>) -> Vec<HistogramBucket> {
    let mut buckets: Vec<HistogramBucket> = BUCKET_EDGES
        .windows(2)
        .map(|w| (w[0], w[1], 0u64))
        .collect();
    values.sort_unstable();
    for v in values {
        for b in buckets.iter_mut() {
            if v >= b.0 && v <= b.1 {
                b.2 += 1;
                break;
            }
        }
    }
    buckets
}

/// Insert `(key, metric)` into a capped, descending-by-metric top-N list. O(N)
/// per insert (N = `top_n`, a small operator-chosen cap), so memory stays
/// O(top_n) regardless of the table's partition count (spec R3.3).
fn insert_ranked(list: &mut Vec<RankedPartition>, top_n: usize, key: Vec<u8>, metric: u64) {
    if top_n == 0 {
        return;
    }
    let pos = list.partition_point(|p| p.metric >= metric);
    if pos < top_n {
        list.insert(pos, RankedPartition { key, metric });
        list.truncate(top_n);
    }
}

/// Owns the deep scan's bounded (O(`top_n` + bucket-count)) accumulator state,
/// so the per-row loop in [`deep_scan_generation`] doesn't need an 8-argument
/// flush closure (clippy `too_many_arguments`). One partition's running
/// size/width/tombstone tally is flushed into this accumulator each time the
/// scan crosses a partition-key boundary.
struct Accumulator {
    top_n: usize,
    size_values: Vec<u64>,
    width_values: Vec<u64>,
    top_largest: Vec<RankedPartition>,
    top_tombstones: Vec<RankedPartition>,
}

impl Accumulator {
    fn new(top_n: usize) -> Self {
        Self {
            top_n,
            size_values: Vec::new(),
            width_values: Vec::new(),
            top_largest: Vec::new(),
            top_tombstones: Vec::new(),
        }
    }

    /// Fold one completed partition's tally into the running state.
    fn flush(&mut self, key: Option<Vec<u8>>, size: u64, width: u64, tombstones: u64) {
        let Some(key) = key else { return };
        self.size_values.push(size);
        self.width_values.push(width);
        insert_ranked(&mut self.top_largest, self.top_n, key.clone(), size);
        if tombstones > 0 {
            insert_ranked(&mut self.top_tombstones, self.top_n, key, tombstones);
        }
    }
}

/// Run the `--deep` bounded streaming scan over one generation's `Data.db`
/// (spec R3.1/R3.2/R3.3). `schema` is threaded through when the caller supplied
/// one (CLI `--schema`); `None` still scans and still buckets — it renders raw
/// key/row bytes rather than schema-decoded values (spec R6, cli-diagnose).
pub(crate) async fn deep_scan_generation(
    reader: Arc<SSTableReader>,
    schema: Option<TableSchema>,
    top_n: usize,
    gc_before_secs: i64,
) -> Result<DeepScanResult> {
    let table_id = TableId::new(format!(
        "{}.{}",
        reader.header().keyspace,
        reader.header().table_name
    ));

    // Bounded channel (issue #790): resident rows are capped by `buffer_size`,
    // not by table size.
    const BUFFER_SIZE: usize = 256;
    let mut stream = reader.scan_stream(table_id, None, None, schema, BUFFER_SIZE);

    let mut rows_scanned: u64 = 0;
    let mut tombstones_total: u64 = 0;
    let mut tombstones_droppable: u64 = 0;

    let mut acc = Accumulator::new(top_n);

    let mut current_key: Option<Vec<u8>> = None;
    let mut current_size: u64 = 0;
    let mut current_width: u64 = 0;
    let mut current_tombstones: u64 = 0;

    while let Some(item) = stream.recv().await {
        let (row_key, scan_row) = item?;
        rows_scanned += 1;
        let key_bytes = row_key.as_bytes().to_vec();

        if current_key.as_deref() != Some(key_bytes.as_slice()) {
            acc.flush(current_key.take(), current_size, current_width, current_tombstones);
            current_key = Some(key_bytes);
            current_size = 0;
            current_width = 0;
            current_tombstones = 0;
        }

        current_size = current_size.saturating_add(scan_row.len() as u64);
        current_width = current_width.saturating_add(1);

        if let ScanRow::Marker(Value::Tombstone(info)) = &scan_row {
            current_tombstones += 1;
            tombstones_total += 1;
            if info.local_deletion_time > 0 && info.local_deletion_time < gc_before_secs {
                tombstones_droppable += 1;
            }
        }
    }
    // Flush the final in-progress partition.
    acc.flush(current_key.take(), current_size, current_width, current_tombstones);

    Ok(DeepScanResult {
        rows_scanned,
        partition_size_histogram: bucketize(acc.size_values),
        clustering_width_histogram: bucketize(acc.width_values),
        top_largest_partitions: acc.top_largest,
        top_tombstone_heaviest_partitions: acc.top_tombstones,
        tombstones_total,
        tombstones_droppable_at_gc_before: tombstones_droppable,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucketize_places_values_in_expected_ranges() {
        let buckets = bucketize(vec![0, 10, 100, 1_000, 10_000_000]);
        // bucket (0,64] should hold 0 and 10.
        let b0 = buckets.iter().find(|b| b.0 == 0 && b.1 == 64).unwrap();
        assert_eq!(b0.2, 2);
        // overflow bucket holds the 10M value.
        let overflow = buckets.iter().find(|b| b.1 == u64::MAX).unwrap();
        assert_eq!(overflow.2, 1);
    }

    #[test]
    fn insert_ranked_keeps_top_n_descending() {
        let mut list = Vec::new();
        insert_ranked(&mut list, 2, b"a".to_vec(), 5);
        insert_ranked(&mut list, 2, b"b".to_vec(), 10);
        insert_ranked(&mut list, 2, b"c".to_vec(), 1);
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].metric, 10);
        assert_eq!(list[1].metric, 5);
    }

    #[test]
    fn insert_ranked_top_n_zero_is_noop() {
        let mut list = Vec::new();
        insert_ranked(&mut list, 0, b"a".to_vec(), 5);
        assert!(list.is_empty());
    }
}
