//! BIG ("nb" / uncompressed) partition point lookup (issue #1572).
//!
//! Before this module, `SSTableReader::get()` on a BIG SSTable read and
//! decompressed the ENTIRE `Data.db` on every lookup: the legacy `SSTableIndex`
//! (`self.index`) is keyed on key *digests*, so `find_entry()` called with the
//! raw partition-key bytes always missed and the miss fell through to
//! `scan_for_key` → whole-file stitch + decompress.
//!
//! The fix resolves the partition's uncompressed `Data.db` offset via the raw-key
//! `index_reader` (the O(1) `Index.db` map, keyed on raw partition-key bytes since
//! issue #552), then seeks and decompresses ONLY the chunk(s) covering that offset
//! — reusing the exact chunk-targeted decode the BTI point lookup already proves
//! correct ([`SSTableReader::bti_decompress_and_parse_target`], bti.rs). The
//! `Index.db` data offset and the BTI trie offset share the same uncompressed
//! `Data.db` offset domain (headerless NB starts the data section at file offset
//! 0), so the same chunk-targeting arithmetic applies verbatim.
//!
//! No-heuristics (issue #28): the offset comes from `Index.db` and the covering
//! chunk from `CompressionInfo` — chunk boundaries are never guessed. CRC / decode
//! errors surface (the shared decode path runs the same per-chunk CRC as the scan),
//! never swallowed (guardrail from #1411).

use super::super::SSTableReader;
use crate::types::{ScanRow, TableId};
use crate::{Result, RowKey};

impl SSTableReader {
    /// Point-lookup entry for BIG ("nb"/uncompressed) readers (issue #1572).
    ///
    /// Resolution order:
    /// 1. Bloom pre-check (unchanged) — a definite miss short-circuits to `None`.
    ///    This is the fast definitive-absent path for the common case: the
    ///    bloom filter answers "not present" for an absent key WITHOUT a scan.
    /// 2. Fast path: `index_reader` (raw-key `Index.db` map) resolves the
    ///    uncompressed partition offset; only the covering chunk(s) are read,
    ///    CRC-checked, decompressed, and decoded.
    /// 3. `scan_for_key` is the whole-file oracle. It is used for genuinely
    ///    index-less readers (no `index_reader`); for the rare exact index hit
    ///    that decodes to no matching row (schema-unavailable soft-miss or a
    ///    benign per-row table-guard rejection); AND for EVERY `index_reader`
    ///    MISS (issue #1572 correctness). An `Index.db` map miss is NOT treated
    ///    as a definitive absent, because `IndexReader::open` opens a truncated /
    ///    partially-corrupt Index.db with a PARTIAL prefix map and — critically —
    ///    truncation aligned to an exact entry boundary (whole trailing entries
    ///    dropped) leaves the prefix parsing cleanly to EOF, so it is
    ///    indistinguishable from a complete map at parse time. There is no
    ///    cleanly-available authoritative partition count at this layer to close
    ///    that gap, so a miss conservatively falls back to the scan oracle,
    ///    keeping `get()`/`scan()` in agreement on degraded inputs. The fast
    ///    definitive-absent path for the common (non-degraded) case is the bloom
    ///    pre-check in step 1, so this fallback fires only on a bloom
    ///    false-positive — rare and acceptable. `SCAN_FOR_KEY_CALLS` stays
    ///    observable on the fallback.
    ///
    /// `fully_qualified_match` is threaded through to the shared decode's per-row
    /// table-consistency guard exactly as the BTI path does (issue #1321 / #1284).
    ///
    /// Returns `(row, oracle_pruned)` (issue #2163): `oracle_pruned` is `true`
    /// ONLY for the Step 1 bloom-definite-miss branch — this SSTable was excluded
    /// from the read by the presence oracle BEFORE any Index.db probe, decode, or
    /// scan. Every other `None` outcome (including one reached via the authoritative
    /// `scan_for_key` fallback, which already performed its OWN confirming scan) is
    /// NOT an oracle exclusion and reports `oracle_pruned = false`, so the caller
    /// (`get_with_resolution`) neither double-counts `cqlite.read.sstables_pruned`
    /// nor runs a REDUNDANT second confirmation scan when the opt-in false-negative
    /// verification is enabled (roborev r4, #2163).
    pub(super) async fn big_get_with_resolution(
        &self,
        table_id: &TableId,
        key: &RowKey,
        fully_qualified_match: bool,
    ) -> Result<(Option<ScanRow>, bool)> {
        use crate::observability::{self as obs, catalog};

        // 1. Bloom pre-check (unchanged behaviour): a definite miss short-circuits.
        if let Some(bloom_filter) = &self.bloom_filter {
            let present = bloom_filter.might_contain(key.as_bytes());
            obs::add_counter(
                catalog::READ_BLOOM_CHECKS,
                1,
                &[
                    (
                        catalog::attr::RESULT,
                        if present { "hit" } else { "miss" }.into(),
                    ),
                    (
                        catalog::attr::SSTABLE_FORMAT,
                        self.sstable_format_label().into(),
                    ),
                ],
            );
            if !present {
                // Definitive presence-oracle negative — an oracle exclusion.
                return Ok((None, true));
            }
        }

        // 2. Fast path: resolve the partition offset via the one `locate` façade
        //    (issue #1599 / G3) and decode ONLY the covering chunk(s). For a BIG
        //    reader `locate` composes the C5 step (a no-op here — the point read
        //    already cleared the `get_with_resolution` pre-dispatch C5 guard, so the
        //    key is in range and nothing is re-recorded) and the raw-key `Index.db`
        //    map, which is the complete authoritative partition set for a BIG
        //    SSTable. Bloom-first ordering above is unchanged. An `Index.db` MISS is
        //    still NOT a definitive absent (#1572), so it falls to `scan_for_key`.
        if self.index_reader.is_some() {
            match self.locate(key.as_bytes()).await? {
                Some((data_offset, _size)) => {
                    // `_size` (Index.db does not store partition size) is unused:
                    // the shared chunk-targeted decode parses forward from the
                    // offset until the partition is complete, so no size is needed.
                    let schema_opt = self.get_table_schema(None);
                    let parser = self.build_v5_parser(true);
                    // Pass `data_offset` raw (NO `actual_header_size` add): the
                    // chunk-targeted decode operates in the uncompressed data-section
                    // domain that begins at offset 0 (and for `nb`
                    // `actual_header_size == 0` anyway). The legacy whole-section
                    // fallback below seeks the file past the header itself, so it must
                    // add `actual_header_size`; this path must not.
                    let found = self
                        .bti_decompress_and_parse_target(
                            data_offset as usize,
                            key,
                            table_id,
                            fully_qualified_match,
                            schema_opt.as_ref(),
                            &parser,
                        )
                        .await?;
                    if let Some(value) = found {
                        if !self.filter_tombstone(&value) {
                            return Ok((None, false));
                        }
                        return Ok((Some(value), false));
                    }
                    // Exact index hit but no matching row decoded (schema-unavailable
                    // soft-miss / benign table-guard rejection). Fall through to the
                    // safe scan_for_key oracle to preserve the pre-#1572 result.
                }
                None => {
                    // Index.db map miss. An index miss is NOT treated as a
                    // definitive absent (issue #1572 correctness): `IndexReader::open`
                    // opens a truncated / partially-corrupt Index.db with a PARTIAL
                    // prefix map, and truncation aligned to an EXACT entry boundary
                    // (whole trailing entries dropped) leaves the surviving prefix
                    // parsing cleanly to EOF — indistinguishable from a complete map
                    // at parse time, with no cleanly-available authoritative partition
                    // count at this layer to detect the loss. A partition whose entry
                    // was dropped would then miss the map yet still be present in
                    // Data.db — a silent get/scan divergence. So conservatively fall
                    // back to the whole-file `scan_for_key` oracle, which keeps
                    // `get()`/`scan()` in agreement and keeps the SCAN_FOR_KEY_CALLS
                    // counter observable. The fast definitive-absent path for the
                    // common (non-degraded) case is the bloom pre-check above; this
                    // fallback fires only on a bloom false-positive (rare, acceptable).
                    // NOT an oracle exclusion: the scan itself is the authority here.
                    return self
                        .scan_for_key(table_id, key)
                        .await
                        .map(|row| (row, false));
                }
            }
        }

        // 3. Legacy fallbacks for index-less readers (or the rare hit-but-soft-miss).
        //    NOTE: `self.index` (SSTableIndex) is keyed on key digests, so
        //    `find_entry()` with raw key bytes misses; retained for the size==0
        //    uncompressed handling and the writer-produced-index cases (issue #517).
        //    Neither branch below is an oracle exclusion: each either scans
        //    authoritatively or decodes a resolved offset.
        if let Some(index) = &self.index {
            if let Some(entry) = index.find_entry(table_id, key).await? {
                if entry.size == 0 {
                    tracing::debug!(
                        "Index reports size=0 for key {:?}, using sequential scan fallback",
                        key
                    );
                    return self
                        .scan_for_key(table_id, key)
                        .await
                        .map(|row| (row, false));
                }
                // Index offsets are relative to data section start - adjust for header.
                let file_offset = entry.offset + self.actual_header_size as u64;
                return self
                    .read_value_at_offset(file_offset, entry.size)
                    .await
                    .map(|row| (row, false));
            }
        }
        self.scan_for_key(table_id, key)
            .await
            .map(|row| (row, false))
    }
}
