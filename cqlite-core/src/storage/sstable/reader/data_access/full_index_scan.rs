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
use crate::schema::TableSchema;
use crate::storage::sstable::reader::parsing::{ParseStep, V5CompressedLegacyParser};
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
    /// A partition that decodes SUCCESSFULLY to zero live rows (legitimately
    /// all-shadowed / all-TTL-expired / all-tombstoned, or a pure partition-delete)
    /// contributes zero rows and the walk CONTINUES — it is NOT a decode failure, so
    /// one fully-shadowed partition in an otherwise-healthy SSTable never demotes the
    /// whole read to a sequential scan.
    ///
    /// Returns:
    /// - `Ok(Some(rows))` — every index entry resolved to a structurally decodable
    ///   partition; the rows are token-ordered + tombstone-filtered, matching
    ///   `sequential_scan`.
    /// - `Ok(None)` — the index carried no entries, offsets were not monotonically
    ///   ascending, the data-section length is unknown, or a partition's bytes could
    ///   not be structurally interpreted (its partition header failed to parse). The
    ///   caller then emits a loud WARN and falls back to `sequential_scan` (never a
    ///   silent fallback).
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

        // Completeness Signal A (issue #2302, roborev job 1606): `IndexReader::open`
        // accepts a truncated/partially-corrupt Index.db as a parsed PREFIX (it
        // `break`s on the first unparseable entry). An index cut MID-ENTRY leaves
        // unparsed trailing bytes, so `is_fully_parsed()` is false — the entry set is
        // NOT a complete enumeration. Bail to the loud sequential-scan fallback
        // rather than silently returning an under-enumerated "complete" set.
        // (Boundary-aligned whole-entry drops leave no trailing bytes; those are
        // caught below by the final-partition coverage check.)
        if !index_reader.is_fully_parsed() {
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

            // Completeness Signal B (issue #2302, roborev job 1606): the FINAL entry
            // is bounded by `data_section_end` rather than a successor offset, so a
            // boundary-aligned trailing-truncated Index.db (whole entries dropped —
            // `is_fully_parsed()` still true, Signal A above cannot see it) would make
            // this last slice silently span the DROPPED partitions. Authoritative
            // invariant: every index entry bounds EXACTLY ONE partition, so the final
            // slice must be exactly one partition. If a following partition header
            // begins in the leftover bytes, the index is missing ≥1 trailing entry —
            // not provably complete → bail to the loud sequential-scan fallback.
            if i + 1 == entries.len()
                && !self.last_index_partition_covers_slice(&parser, &raw, schema)?
            {
                return Ok(None);
            }

            let parsed = parser.parse_block(&raw, schema, self)?;
            if parsed.is_empty() {
                // An empty result is AMBIGUOUS. It is either (a) a partition that
                // decoded SUCCESSFULLY to zero LIVE rows — a legitimately
                // all-shadowed / all-TTL-expired / all-tombstoned partition, whose
                // rows the read-shadowing filter correctly hides (or a pure
                // partition-delete with no cells) — or (b) a slice the parser could
                // NOT structurally interpret. Only (b) is a real failure; demoting
                // the WHOLE walk to a sequential scan on (a) would re-introduce the
                // exact per-read perf cliff #2302 exists to kill whenever one healthy
                // partition happens to be fully shadowed.
                //
                // Distinguish them authoritatively (no heuristics, issue #28) by
                // re-parsing just the partition header at offset 0: `raw` is a single
                // partition's slice (`span > 0`, guarded above), so a structurally
                // valid partition MUST begin with a parseable header. A parseable
                // header ⇒ case (a): contribute zero rows and KEEP enumerating. A
                // header that fails to parse ⇒ case (b): bail to the authoritative
                // full scan (the caller WARNs — never a silent fallback).
                if parser.parse_partition_header_full(&raw, 0).is_err() {
                    return Ok(None);
                }
                continue;
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

    /// Authoritative coverage check for the FINAL index entry's Data.db slice
    /// (issue #2302, roborev job 1606).
    ///
    /// Every BIG `Index.db` entry bounds EXACTLY ONE partition. Non-final entries
    /// are bounded by their successor's offset (exactly one partition by
    /// construction); the FINAL entry is bounded by the authoritative data-section
    /// end, so a trailing-truncated index (whole entries dropped at an exact entry
    /// boundary, which `IndexReader::open` accepts as a clean prefix) would make
    /// this last slice silently span the DROPPED partitions too.
    ///
    /// Returns `Ok(true)` iff `raw` decodes to exactly one partition: parse ONE
    /// physical partition from offset 0, then confirm the leftover bytes (if any)
    /// do NOT begin a second partition header. A following header ⇒ the index
    /// dropped ≥1 trailing entry ⇒ `Ok(false)` (caller bails to the loud
    /// sequential-scan fallback). Leftover bytes that are NOT a header are benign
    /// trailing framing, so one partition legitimately fills the slice. Structural,
    /// schema-driven — no heuristics (issue #28).
    fn last_index_partition_covers_slice(
        &self,
        parser: &V5CompressedLegacyParser,
        raw: &[u8],
        schema: Option<&TableSchema>,
    ) -> Result<bool> {
        // Physical single-partition parse: byte extent is independent of read
        // shadowing, so the enumeration's `read_shadowing=true` parser is reused.
        // `at_final_chunk=true`: `raw` is the whole final slice, never a mid-chunk
        // fragment, so a complete partition reports `Emitted(consumed)`.
        let mut noop = |_row| Ok(std::ops::ControlFlow::Continue(()));
        let consumed =
            match parser.parse_one_partition_for_compaction(raw, schema, self, true, &mut noop)? {
                ParseStep::Emitted(consumed) if consumed > 0 => consumed,
                // Could not structurally decode even one partition from the final
                // slice: not provably complete — reject (caller falls back loud).
                _ => return Ok(false),
            };
        if consumed >= raw.len() {
            // One partition filled the slice exactly: complete coverage.
            return Ok(true);
        }
        // Bytes remain after the first partition. If they begin ANOTHER valid
        // partition header, the index is missing the entry that would have bounded
        // it ⇒ incomplete. A parse error here means no header follows (benign
        // trailing framing) ⇒ the single partition covers the slice.
        Ok(parser.parse_partition_header_full(raw, consumed).is_err())
    }
}
