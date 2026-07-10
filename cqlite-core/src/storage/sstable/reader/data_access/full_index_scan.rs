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
//!
//! ## Every `Ok(None)` / `Ok(Some(...))` exit, exhaustively (roborev job 1609)
//!
//! | Site | Condition | Classification | Loud/Quiet |
//! |------|-----------|-----------------|------------|
//! | entries empty + index fully parsed + data section empty | genuinely zero-partition SSTable | complete `Ok(Some(vec![]))` | quiet |
//! | entries empty + (index NOT fully parsed OR data section non-empty) | index unusable/inconsistent | incomplete `Ok(None)` | loud (caller WARNs) |
//! | entries non-empty + data section empty | structurally inconsistent | incomplete `Ok(None)` | loud |
//! | `!index_reader.is_fully_parsed()` | mid-entry-truncated Index.db (Signal A) | incomplete `Ok(None)` | loud |
//! | `partition_key.is_empty()` | entry carries no raw key | incomplete `Ok(None)` | loud |
//! | `lookup_partition_with_index` misses | index/probe disagreement | incomplete `Ok(None)` | loud |
//! | `next_offset <= data_offset` | non-ascending offsets | incomplete `Ok(None)` | loud |
//! | `u32::try_from(span)` fails | partition span overflows `u32` | incomplete `Ok(None)` | loud |
//! | `partition_slice_fully_consumed` false (Signal B) | dropped trailing entries OR a corrupt/truncated partition body (any entry, empty or non-empty result) | incomplete `Ok(None)` | loud |
//! | loop completes for every entry | every partition proven structurally complete | complete `Ok(Some(rows))` | quiet |

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
    /// whole read to a sequential scan. This is proven authoritative (never a guess,
    /// issue #28) BEFORE the shadowed decode even runs: see
    /// [`partition_slice_fully_consumed`](Self::partition_slice_fully_consumed).
    ///
    /// Returns (see the module-level exit table for the exhaustive enumeration):
    /// - `Ok(Some(rows))` — every index entry resolved to a structurally decodable
    ///   partition (or the SSTable is genuinely, provably empty); the rows are
    ///   token-ordered + tombstone-filtered, matching `sequential_scan`.
    /// - `Ok(None)` — the index/data section could not be proven complete or
    ///   internally consistent. The caller then emits a loud WARN and falls back to
    ///   `sequential_scan` (never a silent fallback).
    pub(in crate::storage::sstable::reader) async fn iterate_all_partitions_via_full_index(
        &self,
    ) -> Result<Option<Vec<(RowKey, ScanRow)>>> {
        let Some(index_reader) = &self.index_reader else {
            return Ok(None);
        };
        let entries = index_reader.get_partition_entries();

        // Exclusive end of the last partition = the UNCOMPRESSED data-section length.
        // Compressed reader: the authoritative `CompressionInfo.data_length`.
        // Uncompressed reader: the raw Data.db file length minus the header (the raw
        // and uncompressed domains coincide).
        let data_section_end = match self.compression_info.as_deref() {
            Some(ci) => ci.data_length,
            None => self
                .stats
                .file_size
                .saturating_sub(self.actual_header_size as u64),
        };

        if entries.is_empty() {
            // Roborev job 1609 (LOW): a zero-entry Index.db is EITHER a genuinely
            // empty BIG SSTable (the writer supports this — a valid, if
            // degenerate, on-disk state) OR an unusable index (corrupt before even
            // one entry, or structurally inconsistent with a non-empty data
            // section). Authoritative, no heuristics (#28): BOTH must
            // independently prove emptiness — the index carries no leftover bytes
            // (`is_fully_parsed()`, so it isn't merely corrupt-from-byte-zero) AND
            // the data section is itself empty. Either alone contradicts the
            // other, so only BOTH together is a quiet, complete empty result;
            // otherwise bail to the loud WARN fallback rather than fabricate a
            // false "complete empty" answer on an unusable index.
            return if index_reader.is_fully_parsed() && data_section_end == 0 {
                Ok(Some(Vec::new()))
            } else {
                Ok(None)
            };
        }
        if data_section_end == 0 {
            // Entries present but the data section is empty: structurally
            // inconsistent with a real BIG SSTable — not provably complete.
            return Ok(None);
        }

        // Completeness Signal A (issue #2302, roborev job 1606): `IndexReader::open`
        // accepts a truncated/partially-corrupt Index.db as a parsed PREFIX (it
        // `break`s on the first unparseable entry). An index cut MID-ENTRY leaves
        // unparsed trailing bytes, so `is_fully_parsed()` is false — the entry set is
        // NOT a complete enumeration. Bail to the loud sequential-scan fallback
        // rather than silently returning an under-enumerated "complete" set.
        // (Boundary-aligned whole-entry drops leave no trailing bytes; those are
        // caught below by the per-partition coverage check.)
        if !index_reader.is_fully_parsed() {
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

            // Completeness + corruption Signal B (issue #2302, roborev jobs 1606 +
            // 1609): physically decode exactly ONE partition from `raw` and require
            // it consume the WHOLE slice. One structural check subsumes both:
            //  - dropped trailing index entries (only the FINAL entry's extent is
            //    NOT pinned by a successor offset — it comes from
            //    `data_section_end`, so it silently spans any dropped tail);
            //  - a truncated/corrupted partition body. `parser.parse_block` (the
            //    shadowed decode below) SWALLOWS a row-parse failure internally
            //    (it `tracing::debug!`s and moves on) and exposes no consumed-vs-
            //    available signal, so a corrupt body whose header still parses was
            //    silently accepted as "legitimately zero rows" — the exact
            //    silent-acceptance class this issue exists to kill, one layer
            //    deeper. `consumed == raw.len()` with zero rows afterward = a
            //    genuinely empty/all-shadowed partition (proven, not guessed);
            //    `consumed < raw.len()` — whether or not rows were already
            //    produced before the failure — is a structural failure, never
            //    accepted as complete. No heuristics (issue #28): every entry
            //    bounds EXACTLY one partition by construction, so full byte
            //    coverage is the ONE provable completeness criterion; there is no
            //    "leftover, but not a header, so assume benign" carve-out anymore
            //    — that assumption is precisely what let body corruption slip
            //    through as "not a header either" before this fix.
            if !self.partition_slice_fully_consumed(&parser, &raw, schema)? {
                return Ok(None);
            }

            // `parsed.is_empty()` here is UNAMBIGUOUS: the check above already
            // proved `raw` decodes to exactly one structurally complete partition,
            // so zero rows means legitimately zero LIVE rows (all-shadowed /
            // all-TTL-expired / all-tombstoned / a pure partition-delete) — never a
            // swallowed parse failure.
            let parsed = parser.parse_block(&raw, schema, self)?;
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

    /// Authoritative structural coverage check for ONE index entry's Data.db slice
    /// (issue #2302, roborev jobs 1606 + 1609): physically decode `raw` (no read
    /// shadowing — the byte extent a partition occupies is independent of it) as
    /// exactly one partition, and require the decode to consume the slice in full.
    ///
    /// Every BIG `Index.db` entry bounds EXACTLY ONE partition — for a non-final
    /// entry the successor's offset marks precisely where the next partition
    /// begins; for the final entry the authoritative data-section end does, but
    /// (unlike a successor offset) does not itself prove no trailing entries were
    /// dropped. Either way, a structurally sound, uncorrupted partition consumes
    /// its ENTIRE bounded slice — no more, no less. `consumed < raw.len()` covers
    /// every failure shape at once: a dropped trailing entry (extra bytes after
    /// the real last partition), a truncated body, or a body that fails to decode
    /// partway through (regardless of how many rows had already been produced
    /// before the failure). `at_final_chunk=true`: `raw` is the whole bounded
    /// slice, never a mid-chunk fragment, so a complete partition always reports
    /// `Emitted(consumed)` (the sliding-window driver's `NeedMore`/`Done` cases are
    /// unreachable here: `NeedMore` requires `!at_final_chunk`; `Done` requires an
    /// empty `data`, but every entry's span is proven `> 0` by the caller).
    /// Structural, schema-driven — no heuristics (issue #28).
    fn partition_slice_fully_consumed(
        &self,
        parser: &V5CompressedLegacyParser,
        raw: &[u8],
        schema: Option<&TableSchema>,
    ) -> Result<bool> {
        let mut noop = |_row| Ok(std::ops::ControlFlow::Continue(()));
        let step = parser.parse_one_partition_for_compaction(raw, schema, self, true, &mut noop)?;
        Ok(matches!(step, ParseStep::Emitted(consumed) if consumed == raw.len()))
    }
}
