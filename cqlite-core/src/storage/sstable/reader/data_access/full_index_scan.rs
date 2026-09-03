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
//! ## Every `Ok(None)` / `Ok(Some(...))` exit, exhaustively (roborev jobs 1609, 1610)
//!
//! | Site | Condition | Classification | Loud/Quiet |
//! |------|-----------|-----------------|------------|
//! | entries empty | zero-entry Index.db — UNREACHABLE as a valid empty SSTable (`IndexReader::open` rejects a zero-byte file, and any non-empty buffer parsing to zero entries leaves unparsed trailing bytes ⇒ `is_fully_parsed()` false); reached only via a corrupt-from-byte-zero / structurally-unusable index (roborev job 1615, see the site) | incomplete `Ok(None)` | loud (caller WARNs) |
//! | entries non-empty + data section empty | structurally inconsistent | incomplete `Ok(None)` | loud |
//! | `!index_reader.is_fully_parsed()` | mid-entry-truncated Index.db (Signal A) | incomplete `Ok(None)` | loud |
//! | `partition_key.is_empty()` | entry carries a present-but-zero-length key — legal Cassandra shape, but `key_len == 0` is unsafe to read back through the shared row/partition scanner on EITHER path (job 1610, see the call-site comment) | incomplete `Ok(None)` | loud |
//! | `next_offset <= data_offset` | non-ascending offsets | incomplete `Ok(None)` | loud |
//! | `u32::try_from(span)` fails | partition span overflows `u32` | incomplete `Ok(None)` | loud |
//! | `partition_slice_fully_consumed` false (Signal B) | dropped trailing entries, a corrupt/truncated partition body (any entry, empty or non-empty result), OR (job 1610) a slice truncated exactly at a parseable row boundary with no CONFIRMED end-of-partition terminator | incomplete `Ok(None)` | loud |
//! | loop completes for every entry | every partition proven structurally complete (full byte coverage AND a confirmed terminator) | complete `Ok(Some(rows))` | quiet |

use super::super::SSTableReader;
use super::model::sort_by_token_order;
use crate::storage::sstable::reader::parsing::BufferExtent;
use crate::types::ScanRow;
use crate::{Result, RowKey};

impl SSTableReader {
    /// Enumerate every partition of a BIG SSTable through the FULL `Index.db`
    /// partition-offset table (issue #2302), one index-random-read per partition.
    ///
    /// Each `Index.db` entry stores a partition's start offset (relative to the data
    /// section) but NOT its byte size. The exclusive end of partition `i` is the
    /// start of partition `i+1` (entries are token-ordered, matching Data.db physical
    /// order); the LAST partition ends at the data-section end. Each partition's
    /// start offset is read DIRECTLY from its already-loaded `Index.db` entry
    /// (`entries[i].data_offset`) — the walk does NOT re-probe
    /// [`lookup_partition_with_index`](SSTableReader::lookup_partition_with_index)
    /// per partition (issue #2430: a redundant Summary binary search + interval read
    /// on every entry, O(N) on a full scan).
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
    ///
    /// `scan_cancel` is an explicit PER-CALL cancellation token (issue #2346);
    /// the sole caller ([`SSTableReader::iterate_all_partitions_cancellable`])
    /// threads either the reader's own field (the pre-#2346 default) or a
    /// caller-supplied token (the compaction path), so a shared/cached reader's
    /// concurrent scans cancel independently.
    pub(in crate::storage::sstable::reader) async fn iterate_all_partitions_via_full_index(
        &self,
        scan_cancel: &crate::storage::scan_cancel::ScanCancel,
    ) -> Result<Option<Vec<(RowKey, ScanRow)>>> {
        let Some(index_reader) = &self.index_reader else {
            return Ok(None);
        };
        // Issue #2412 Stage 2: a lazily-opened reader defers the full parse to
        // first use — a full materializing enumeration IS that first use (Stage 4
        // replaces this consumer with a true streaming walk that never
        // materializes the whole map). No-op for an eagerly-opened reader.
        index_reader.ensure_materialized(scan_cancel).await?;
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
            // Roborev job 1615 (MEDIUM): a zero-entry Index.db is NEVER a reachable
            // "genuinely empty SSTable" here. `IndexReader::open`
            // (index_reader/mod.rs) rejects a zero-byte Index.db as corruption
            // BEFORE constructing the reader, and any NON-empty buffer that parses
            // to zero entries necessarily failed on its FIRST entry, leaving
            // unparsed trailing bytes — so `is_fully_parsed()` would be false there
            // too (parse.rs: the entry loop only stops early on a parse error, which
            // leaves a non-empty remainder). Neither Cassandra nor CQLite's
            // `SSTableWriter` emits a zero-partition SSTable at all (a flush/compaction
            // with no partitions writes no SSTable), so the former quiet
            // `is_fully_parsed() && data_section_end == 0 => Ok(Some(vec![]))` success
            // branch was DEAD CODE and has been removed. `entries.is_empty()` here
            // therefore only ever means a corrupt-from-byte-zero / structurally
            // unusable index: bail to the caller's loud WARN + sequential fallback
            // (fail-safe), never fabricate a "complete empty" answer. No heuristics (#28).
            return Ok(None);
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
            // Cooperative checkpoint (issue #2264): one real index-random-read +
            // Data.db parse per partition — poll every entry so a cancelled Flight
            // `do_get` abandons the walk promptly, and yield every 256th so the
            // chokepoint `max_execution_time` timeout can elapse mid-walk (#1695).
            // Issue #2346: PER-CALL token.
            // `checkpoint_polled`, which is the ONLY one of the three that leaves
            // this loop's cadences BOTH unchanged: it polled cancellation on every
            // iteration before #1695 (one real index random read each is already
            // coarse) and had NO yield at all. `checkpoint(i)` would lower the poll
            // to one per 256; `checkpoint_now()` would add a runtime reschedule per
            // partition to every large index-backed scan.
            scan_cancel.checkpoint_polled(i).await?;

            // Roborev job 1610 (finding 1): `raw_key: None` never actually occurs
            // in practice — `parse_big_index_entry` always sets `Some(raw_key)` —
            // so this early bail is really about a PRESENT-but-ZERO-LENGTH key,
            // which roborev correctly notes IS a legal Cassandra shape (a
            // single-component TEXT/BLOB/etc. partition key whose value is the
            // empty string serializes to zero bytes) — routing it into the normal
            // index lookup, rather than bailing, was the suggested fix.
            //
            // Empirically verified (not assumed) before implementing that: the
            // write surface does NOT reject an empty-string single-column TEXT
            // partition key — `Mutation::decorated_key` succeeds (0-byte key,
            // Murmur3 token computed over zero bytes), `SSTableWriter::write_partition`
            // and `finish()` both succeed, and the resulting reader opens fine.
            // BUT reading that partition back is unsafe on EITHER path: the shared
            // structural row/partition scanner (`scan_partition_header`,
            // `row_framing.rs`) explicitly rejects `key_len == 0` as "not a valid
            // partition header" (issue #258 — needed to disambiguate a partition
            // header's key-length byte from ordinary row-data bytes, since Data.db
            // carries no other framing to tell them apart). A hand-built two-
            // partition fixture (one empty-key, one normal) proved this is not a
            // theoretical concern: `iterate_all_partitions` (bailing here to
            // `sequential_scan`, since `sequential_scan` uses the SAME shared
            // scanner) returned 3 corrupted rows for 2 written partitions —
            // `sequential_scan` itself mis-parses this on-disk shape too, not just
            // the index path.
            //
            // Given that, flowing an empty raw_key into `lookup_partition_with_index`
            // + `partition_slice_fully_consumed` + `parser.parse_block` would risk
            // the index path ALSO silently accepting corrupted bytes as a
            // "complete" partition (worse than the current loud bail: a WRONG
            // answer presented as success). The safe, evidence-based choice is to
            // KEEP bailing here — never silently, always via the caller's loud WARN
            // + `sequential_scan` fallback — until the underlying `key_len == 0`
            // structural ambiguity in `scan_partition_header` is resolved (a
            // deeper, format-level fix outside #2302's routing-layer scope).
            //
            // ADJUDICATED (roborev job 1615, finding 1 — DECLINED, behavior
            // unchanged): empty raw partition keys are rejected fail-safe because a
            // write/read round-trip of an empty PK is corrupt via BOTH the index
            // and scan paths (verified empirically, see above). The structural fix
            // for the empty-PK write/read asymmetry is tracked in issue #2325. Do
            // NOT route empty keys into the walker until #2325 lands.
            let partition_key = entries[i].raw_key.as_deref().unwrap_or(&[]);
            if partition_key.is_empty() {
                return Ok(None);
            }

            // Resolve the partition's start offset DIRECTLY from the already-loaded
            // entry (issue #2430). `get_partition_entries()` above returned the
            // fully-materialised offset table, so `entries[i].data_offset` IS this
            // partition's start — the same value a `lookup_partition_with_index`
            // probe would recover, since both read the one resident index. Re-probing
            // per partition was pure redundant work: after #2412 it routes through a
            // fresh Summary binary search + interval read (file open + seek + parse),
            // turning a full scan into O(N) redundant index re-resolutions.
            let data_offset = entries[i].data_offset;
            // Non-vacuity signal (issue #2430): one per partition resolved from an
            // already-loaded `Index.db` entry — the surviving discriminator for
            // "the index-backed materialising path was genuinely taken", since this
            // walk no longer re-probes `lookup_partition_with_index` per partition
            // (that redundant probe was what `INDEX_PROBES` used to count here).
            crate::storage::sstable::read_work_counters::record_index_backed_partition_resolved();

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
            // SCAN intent (issue #2876): a full-`Index.db` enumeration walks Data.db
            // in ascending offset order, so both the body read and the uncompressed
            // `CRC.db` covering-chunk reads use the reader's UNADVISED
            // `scan_positional_source`, not the `MADV_RANDOM` point mapping (#2210).
            let scan_source = self.scan_positional_source.clone();
            let raw = if let Some(ci) = self.compression_info.as_deref() {
                // Compressed: `data_offset` is an uncompressed-domain offset; map it
                // to the covering compression chunk(s) and decompress.
                self.read_compressed_offset_window(scan_source.as_ref(), ci, data_offset, size)
                    .await?
            } else {
                // Uncompressed: raw file offset = data_offset + header (0 for `nb`).
                let absolute_offset = data_offset + self.actual_header_size as u64;
                self.read_uncompressed_verified(
                    scan_source.as_ref(),
                    &self.file,
                    absolute_offset,
                    size as usize,
                )
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

            // Work-probe (issue #2398/#2430): one partition body read + parsed on
            // the MATERIALISING full-index walk, the same per-partition decode the
            // streaming sibling counts. This is the non-probe signal the cancel
            // oracle (`compaction_cancel_tests`) bounds on after issue #2430 dropped
            // the redundant per-partition `lookup_partition_with_index` re-probe:
            // a pre-cancelled scan aborts BEFORE reaching this decode, so it records
            // zero here.
            crate::storage::sstable::work_counters::add_stream_walk_partition_parsed();

            // `parsed.is_empty()` here is UNAMBIGUOUS: the check above already
            // proved `raw` decodes to exactly one structurally complete partition,
            // so zero rows means legitimately zero LIVE rows (all-shadowed /
            // all-TTL-expired / all-tombstoned / a pure partition-delete) — never a
            // swallowed parse failure.
            // #3782: `partition_slice_fully_consumed` above PROVED this slice
            // decodes to exactly one structurally complete partition consuming
            // every byte of `raw`, so nothing further can arrive to finish a row
            // — a decode failure here is corruption, not a straddle.
            let parsed = parser.parse_block(&raw, BufferExtent::Complete, schema, self)?;
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

    // The structural coverage oracle `partition_slice_fully_consumed` (its full
    // `at_final_chunk = false` rationale, issue #2302 roborev jobs 1606/1609/1610)
    // now lives on the sibling `full_index_stream` module (issue #2361) so the
    // materialising walk above and the streaming walk share ONE implementation.
}
