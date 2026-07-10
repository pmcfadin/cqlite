//! Full-`Index.db` partition enumeration for BIG SSTables (issue #2302).
//!
//! `iterate_all_partitions` (in the sibling `partition_lookup` module) historically
//! walked only the SPARSE `Summary.db` samples (≈1-in-128 partitions) and passed
//! `data_size = 0` to the partition parser — `Index.db` never stores a partition
//! size — so it read zero bytes per entry, resolved zero partitions, and SILENTLY
//! fell back to a full `sequential_scan` on EVERY read, even with complete valid
//! components. This module enumerates EVERY partition via the full `Index.db`
//! offset table instead, bounding each partition by the successor entry's offset
//! (the last by the data-section end) — authoritative on-disk structure, no size
//! guessing (issue #28).
//!
//! Lives in `data_access` so it reuses that module's `pub(super)` helpers
//! (`build_v5_parser`, `read_compressed_offset_window`, `sort_by_token_order`)
//! without widening their visibility.

use super::super::SSTableReader;
use super::model::sort_by_token_order;
use crate::types::ScanRow;
use crate::{Result, RowKey};

impl SSTableReader {
    /// Enumerate every partition of a BIG SSTable through the FULL `Index.db`
    /// partition-offset table (issue #2302), one index-random-read per partition.
    ///
    /// Each `Index.db` entry stores a partition's start offset (relative to the data
    /// section) but NOT its byte size. The exclusive end of partition `i` is the
    /// start of partition `i+1` (entries are token-ordered, matching Data.db physical
    /// order); the LAST partition ends at the data-section end. Every real probe
    /// increments `INDEX_PROBES` via
    /// [`lookup_partition_with_index`](SSTableReader::lookup_partition_with_index).
    ///
    /// Both compression modes are handled: the offsets are in the uncompressed
    /// data-section domain, so an uncompressed reader reads the raw file slice
    /// (CRC-verified) while a compressed reader maps the slice to its covering
    /// compression chunk(s) and decompresses them
    /// ([`read_compressed_offset_window`](SSTableReader::read_compressed_offset_window)).
    ///
    /// Returns:
    /// - `Ok(Some(rows))` — every index entry resolved to a decodable partition; the
    ///   rows are token-ordered + tombstone-filtered, matching `sequential_scan`.
    /// - `Ok(None)` — the index carried no entries, offsets were not monotonically
    ///   ascending, the data-section length is unknown, or a partition failed to
    ///   decode. The caller then emits a loud WARN and falls back to
    ///   `sequential_scan` (never a silent fallback).
    pub(in crate::storage::sstable::reader) async fn iterate_all_partitions_via_full_index(
        &self,
    ) -> Result<Option<Vec<(RowKey, ScanRow)>>> {
        let Some(index_reader) = &self.index_reader else {
            return Ok(None);
        };
        let entries = index_reader.get_partition_entries();
        if entries.is_empty() {
            return Ok(None);
        }

        // Exclusive end of the last partition = the UNCOMPRESSED data-section length.
        // Compressed reader: the authoritative `CompressionInfo.data_length`.
        // Uncompressed reader: the raw Data.db file length minus the header (the raw
        // and uncompressed domains coincide). A zero/unknown length is unusable, so
        // bail to the safe full scan.
        let data_section_end = match self.compression_info.as_deref() {
            Some(ci) => ci.data_length,
            None => self
                .stats
                .file_size
                .saturating_sub(self.actual_header_size as u64),
        };
        if data_section_end == 0 {
            return Ok(None);
        }

        // V5 parser over ALREADY-DECOMPRESSED partition bytes (never decompresses
        // internally, unlike `parse_block_entries_with_schema`), so a compressed
        // reader's decompressed slice is not double-decompressed. `read_shadowing =
        // true` applies the same partition/range-tombstone shadowing + TTL expiry a
        // user-facing SELECT scan uses (matching `sequential_scan`).
        let parser = self.build_v5_parser(true);
        let reader_schema = self.get_table_schema(None);
        let schema = reader_schema.as_ref();
        let mut results = Vec::new();
        for i in 0..entries.len() {
            // Cooperative cancellation (issue #2264): one real index-random-read +
            // Data.db parse per partition — poll every entry so a cancelled Flight
            // `do_get` abandons the walk promptly.
            self.scan_cancel.check()?;

            let partition_key = entries[i].raw_key.as_deref().unwrap_or(&[]);
            if partition_key.is_empty() {
                return Ok(None);
            }

            // Resolve through the shared probe so INDEX_PROBES / the B4 key cache /
            // the observability counters all fire exactly as a point read's would.
            let Some((data_offset, _size)) =
                self.lookup_partition_with_index(partition_key).await?
            else {
                return Ok(None);
            };

            // Bound the partition: successor entry's offset, else the data-section
            // end for the final partition. Offsets must ascend (Data.db physical
            // order); a non-ascending pair means the index is inconsistent with the
            // data section, so bail to the safe full scan.
            let next_offset = if i + 1 < entries.len() {
                entries[i + 1].data_offset
            } else {
                data_section_end
            };
            if next_offset <= data_offset {
                return Ok(None);
            }
            let span = next_offset - data_offset;
            let Ok(size) = u32::try_from(span) else {
                return Ok(None);
            };

            // Read the partition's Data.db slice into the UNCOMPRESSED byte domain,
            // then parse it with the NB-aware V5 block parser — the same producer
            // `sequential_scan` uses.
            let raw = if let Some(ci) = self.compression_info.as_deref() {
                // Compressed: `data_offset` is an uncompressed-domain offset; map it
                // to the covering compression chunk(s) and decompress.
                self.read_compressed_offset_window(ci, data_offset, size)
                    .await?
            } else {
                // Uncompressed: raw file offset = data_offset + header (0 for `nb`).
                let absolute_offset = data_offset + self.actual_header_size as u64;
                self.read_uncompressed_verified(&self.file, absolute_offset, size as usize)
                    .await?
            };
            let parsed = parser.parse_block(&raw, schema, self)?;
            if parsed.is_empty() {
                // A non-empty on-disk slice that decodes to nothing means the parser
                // could not interpret it — bail to the authoritative full scan rather
                // than silently drop the partition.
                return Ok(None);
            }
            for (_table_id, row_key, value) in parsed {
                if self.filter_tombstone(&value) {
                    results.push((row_key, value));
                }
            }
        }

        // Token order matches `sequential_scan` (Data.db is already token-ordered, so
        // this is effectively a no-op, but keep the ordering guarantee explicit).
        sort_by_token_order(&mut results);
        Ok(Some(results))
    }
}
