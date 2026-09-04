//! BTI ("da") POINT-READ path (issue #1599 / G3 split of `bti.rs`, campsite #1116).
//!
//! Extracted from `bti.rs`: the trie-resolved single-partition point lookup
//! (`bti_point_lookup`) and its chunk-targeted decode machinery — offset resolution
//! via the one [`locate`](super::super::SSTableReader::locate) façade, the
//! positional (`read_at`) chunk/section fetch, the growing-window single-partition
//! parse, and the prefix-collision key guard. The whole-Data.db BTI scan, the
//! single-partition seek, and clustering-slice narrowing stay in `bti.rs`.

// Issue #3890: the partition-extent parse bound, a CHILD module of this one (see
// its header for why it is not a `data_access` sibling).
#[cfg(not(feature = "tombstones"))]
mod partition_extent;

use super::super::SSTableReader;
use super::model::{
    bti_lookup_step, point_read_absence_or_remembered, point_read_remember_or_bail,
    table_header_consistent_for_seek, BtiLookupStep,
};
use crate::storage::sstable::reader::parsing::BufferExtent;
use crate::types::{ScanRow, TableId};
use crate::{Error, Result, RowKey};
use tracing::debug;

impl SSTableReader {
    /// BTI ("da") point lookup: resolve a partition key via the Partitions.db
    /// trie, decode the partition at the resolved offset, and return its row
    /// `Value` (issue #831).
    ///
    /// Correctness invariants (see issue #831 / #755):
    ///
    /// - **Offset domain**: the trie returns an *uncompressed* Data.db offset, so
    ///   we decode the partition out of the DECOMPRESSED data section, never via
    ///   `read_value_at_offset`/`get_cached_data` (which seek raw file bytes).
    /// - **Own decompression**: `requires_chunk_stitching()` is `false` for BTI,
    ///   so this path decompresses the chunk-compressed Data.db itself via the
    ///   reader's CompressionInfo + compression_reader. Because the trie already
    ///   resolved the EXACT uncompressed offset of the target partition, this only
    ///   decompresses the chunk that contains that offset and continues forward
    ///   chunk-by-chunk ONLY until the target partition is fully parsed — it never
    ///   decompresses earlier chunks or the rest of the file (issue #831 perf
    ///   finding). The whole-section `point_read_whole_section` fallback (one
    ///   positioned read of the entire data section) is used only when chunk
    ///   targeting is impossible (no/zero `chunk_length`).
    /// - **Prefix-collision guard**: the trie may return a candidate for a
    ///   prefix-colliding key, so the decoded partition key is verified to equal
    ///   the queried key before any row is returned.
    ///
    /// `fully_qualified_match` is the authoritative resolution-mode signal threaded
    /// from the manager's `resolve_reader_list` (issue #1321, mirroring #1284's
    /// seek path): `true` iff the query's fully-qualified `keyspace.table` key
    /// matched this reader's map slot EXACTLY (or the query was unqualified),
    /// `false` iff a fully-qualified query reached this reader via the bare-name
    /// fallback. It gates the per-row table-consistency guard in
    /// `bti_decompress_and_parse_target`: an exact FQ match may relax across a
    /// header-keyspace divergence, while a fallback keeps strict keyspace matching.
    ///
    /// Returns `(row, oracle_pruned)` (issue #2163): `oracle_pruned` is `true`
    /// ONLY for the Step 1 trie-miss branch — this SSTable was excluded from the
    /// read by the presence oracle BEFORE any decode was attempted. Every other
    /// `None` outcome (a decoded-but-non-matching prefix-collision candidate, a
    /// tombstone) is NOT an oracle exclusion and reports `oracle_pruned = false`,
    /// so the caller (`get_with_resolution`) emits `cqlite.read.sstables_pruned`
    /// and runs the opt-in false-negative verification ONLY for a genuine
    /// definitive negative — never for a row this reader actually examined.
    pub(super) async fn bti_point_lookup(
        &self,
        table_id: &TableId,
        key: &RowKey,
        fully_qualified_match: bool,
    ) -> Result<(Option<ScanRow>, bool)> {
        // 1. Resolve the uncompressed Data.db offset via the one `locate` façade
        //    (issue #1599 / G3). For a BTI reader `locate` runs the C5 step (a no-op
        //    — BTI has no Summary bound, so nothing is short-circuited or recorded)
        //    then the `Partitions.db` trie, which is the AUTHORITATIVE presence
        //    oracle and emits the single READ_BLOOM_CHECKS; the bloom filter is never
        //    consulted. A trie miss is definitive absence. BTI records no size, so
        //    `locate` returns `(offset, 0)` and we use only the offset.
        let offset = match self.locate(key.as_bytes()).await? {
            Some((off, _size)) => off as usize,
            // Not in this SSTable (authoritative trie miss) — an oracle exclusion.
            None => return Ok((None, true)),
        };

        // 2. Obtain a DECOMPRESSED window that contains the target partition.
        //
        //    `window_base` is the uncompressed offset of the window's first byte
        //    and `window` holds the decompressed bytes from there onward. The
        //    target partition starts at `offset - window_base` inside `window`
        //    (INVARIANT 1: the trie offset indexes the uncompressed data section).
        //
        //    For the chunk-targeted path the window starts at the chunk that
        //    contains `offset` (so `window_base = target_chunk * chunk_length`);
        //    for the whole-section fallback the window starts at offset 0
        //    (`window_base = 0`). Either way the parse below uses the same
        //    `within = offset - window_base` index.
        let schema_opt = self.get_table_schema(None);
        let parser = self.build_v5_parser(true);

        let found = self
            .bti_decompress_and_parse_target(
                offset,
                key,
                table_id,
                fully_qualified_match,
                schema_opt.as_ref(),
                &parser,
            )
            .await?;

        // Steps 2+ decoded (or attempted to decode) actual partition data — the
        // trie admitted this SSTable, so any `None` here (prefix-collision
        // candidate that didn't match, or a tombstone) is NOT an oracle exclusion.
        match found {
            Some(value) => {
                if !self.filter_tombstone(&value) {
                    return Ok((None, false));
                }
                Ok((Some(value), false))
            }
            None => Ok((None, false)),
        }
    }

    /// Compute the chunk that contains uncompressed `offset`, the uncompressed
    /// offset of that chunk's start, and the within-chunk index — given the
    /// CompressionInfo `chunk_length` (issue #831).
    ///
    /// Returns `(target_chunk, window_base, within)` where
    /// `window_base = target_chunk * chunk_length` and `within = offset - window_base`.
    /// Pure arithmetic so it can be unit-tested independently of any I/O.
    #[inline]
    fn bti_chunk_target(offset: usize, chunk_length: usize) -> (usize, usize, usize) {
        let target_chunk = offset / chunk_length;
        let window_base = target_chunk * chunk_length;
        let within = offset - window_base;
        (target_chunk, window_base, within)
    }

    /// Positional (`pread`) read of the ENTIRE uncompressed data section for the
    /// point-read whole-section fallback (issue #1573, C2) — used when chunk
    /// targeting is impossible (no/zero `chunk_length`, i.e. an uncompressed BTI or
    /// nb-without-CompressionInfo Data.db). Reads `[header_size, file_size)` in one
    /// positioned read and, when a `CRC.db` is present (uncompressed BIG), verifies
    /// the covering chunks BEFORE the bytes are parsed — preserving the CRC-then-use
    /// ordering the cursor path enforced via `read_uncompressed_data_block`.
    pub(super) async fn point_read_whole_section(&self) -> Result<Vec<u8>> {
        let header_size = self.calculate_header_size() as u64;
        // Authoritative file length straight from the positional source (== the
        // reader's `file_size`; using the source keeps the read self-consistent).
        let end = self.point_source.len();
        let len = end.saturating_sub(header_size);
        let mut whole = vec![0u8; len as usize];
        if len > 0 {
            // Read the section in BOUNDED windows rather than one section-sized
            // `read_exact_at`. A `DirectReadAt` backend allocates a per-call
            // aligned bounce buffer as large as the requested range, so a single
            // whole-section read would transiently ~double resident memory vs the
            // <128MB target for a large section. Windowing caps the bounce buffer
            // at ~`WHOLE_SECTION_READ_WINDOW` regardless of backend (issue #1573
            // roborev); `whole` itself is the returned data and is unavoidable.
            const WHOLE_SECTION_READ_WINDOW: usize = 1 << 20; // 1 MiB
            let mut filled = 0usize;
            while filled < whole.len() {
                let win_end = (filled + WHOLE_SECTION_READ_WINDOW).min(whole.len());
                self.point_source
                    .read_exact_at(header_size + filled as u64, &mut whole[filled..win_end])?;
                filled = win_end;
            }
        }
        // CRC-verify the covering chunk(s) when a CRC.db is present (no-op for BTI
        // and compressed tables) BEFORE returning the bytes. The section is already
        // resident in `whole`, so verify against those in-memory bytes rather than
        // re-reading the identical range from `point_source` — the section is
        // transferred from disk EXACTLY ONCE (issue #1573 roborev), preserving the
        // CRC-before-use ordering and the CRC algorithm unchanged.
        if self.crc_reader.is_some() {
            // POINT intent (issue #2876): the only I/O the verifier can still need
            // is the straddling-chunk re-read below `header_size`, and this is a
            // point path, so it stays on the advised `MADV_RANDOM` mapping (#2210) —
            // the same plane the section itself was just read from.
            let point_source = self.point_source.clone();
            self.verify_uncompressed_section_in_buffer(point_source.as_ref(), header_size, &whole)
                .await?;
        }
        Ok(super::super::chunk_source::counted_raw_chunk(whole, None))
    }

    /// Decompress only the chunk(s) needed to fully parse the target partition at
    /// uncompressed `offset`, then parse and return its row value (issue #831).
    ///
    /// Chunk targeting (the fast path): when `CompressionInfo` with a non-zero
    /// `chunk_length` is present, the chunk containing `offset` is
    /// `target_chunk = offset / chunk_length`; we seek that chunk via its
    /// `chunk_offsets` entry, set the cursor's chunk index to `target_chunk`, then
    /// decompress forward chunk-by-chunk, appending each into `window`. After each
    /// appended chunk we attempt to parse the FIRST partition at `window[within..]`
    /// (`within = offset % chunk_length`). The stop condition (correctness-critical
    /// — never return a truncated parse):
    ///   - parse returns `Ok` AND the emit closure fired (a COMPLETE partition was
    ///     decoded) -> stop and return what the closure captured;
    ///   - parse returns `Err` (buffer truncated mid-partition) OR the closure
    ///     never fired -> append the next chunk and retry;
    ///   - `read_next_block()` returns `None` (EOF) and still not parsed -> stop
    ///     (the caller treats `None` as "absent", matching prior behaviour).
    ///
    /// Fallbacks (preserve prior behaviour exactly): when `compression_info` is
    /// `None` (uncompressed BTI Data.db) or `chunk_length` is 0/absent, this
    /// reads the WHOLE section in one positioned read via
    /// `point_read_whole_section` (`window_base = 0`, CRC-verified when a CRC.db
    /// is present) and runs the same single-partition parse.
    ///
    /// Chunks are fetched with positioned (`read_at`) reads on the shared
    /// `point_source` — no per-lookup `open(2)`, no `ScanCursor`, and no mutex.
    /// `chunk_index` is a plain local (a lookup is single-threaded within
    /// itself), so concurrent lookups run in parallel without serialization;
    /// safety comes from `read_at` taking `&self` (issue #1573, superseding the
    /// per-scan-cursor approach of issue #815).
    pub(super) async fn bti_decompress_and_parse_target(
        &self,
        offset: usize,
        key: &RowKey,
        table_id: &TableId,
        // Issue #1321: authoritative resolution mode (see `bti_point_lookup`).
        // Gates the per-row table-consistency guard exactly like the seek path
        // (#1284): an EXACT fully-qualified resolution may accept rows across a
        // benign header-keyspace divergence on a consistent table name, while a
        // fully-qualified query resolved via the bare-name fallback keeps STRICT
        // keyspace matching (no wrong-keyspace rows).
        fully_qualified_match: bool,
        schema_opt: Option<&crate::schema::TableSchema>,
        parser: &crate::storage::sstable::reader::parsing::V5CompressedLegacyParser,
    ) -> Result<Option<ScanRow>> {
        use crate::storage::sstable::compression::Compression;

        // Issue #1573 (C2): the point path fetches chunks via positioned reads on
        // the shared `point_source` — no per-lookup `open(2)`, no cursor, no mutex.
        // `chunk_index` is a plain local (a lookup is single-threaded within
        // itself); concurrency safety comes from `read_at` being `&self`.

        // Determine the chunk-targeting parameters. `chunk_length == 0` (or no
        // CompressionInfo) means we cannot chunk-target -> whole-section fallback.
        let chunk_length = self
            .compression_info
            .as_ref()
            .map(|ci| ci.chunk_length as usize)
            .filter(|&len| len > 0);

        let mut chunk_index = 0usize;
        let (target_chunk, window_base, mut window) = match chunk_length {
            Some(len) => {
                let (target_chunk, window_base, _within) = Self::bti_chunk_target(offset, len);
                // Positioned reads resolve their own offset from the chunk index, so
                // no pre-seek is needed — just start at `target_chunk`.
                chunk_index = target_chunk;
                (target_chunk, window_base, Vec::<u8>::new())
            }
            None => {
                // Whole-section fallback (uncompressed BTI, or chunk_length absent/0):
                // one positioned read of the whole data section, CRC-verified when a
                // CRC.db is present (see `point_read_whole_section`).
                let whole = self.point_read_whole_section().await?;
                (0usize, 0usize, whole)
            }
        };

        // `within` is the start of the target partition inside `window`.
        if offset < window_base {
            return Err(Error::corruption(format!(
                "BTI point lookup: resolved offset {} precedes window base {} (chunk {})",
                offset, window_base, target_chunk
            )));
        }
        let within = offset - window_base;

        // For the chunk-targeted path we still need to populate `window`. For the
        // whole-section fallback `window` is already complete.
        let chunk_targeted = chunk_length.is_some();

        // #3721 (job 80): last parse failure that cannot mean absence.
        let mut undecodable: Option<Error> = None;

        loop {
            // If chunk-targeted, append the next chunk before each parse attempt
            // (the whole-section fallback already has all bytes in `window`).
            if chunk_targeted {
                // Single decode plane (issue #1598, G2): positioned read → CRC →
                // decompress → B1 cache via `ChunkSource`. A shared-cache hit skips
                // the decompressor; a miss reads + CRC-checks (guardrail #1411) the
                // compressed bytes before decompressing them (issue #1567 cache key
                // = the ABSOLUTE chunk index in the NS_BTI_CHUNK namespace).
                let compression_opt = self
                    .compression_reader
                    .as_ref()
                    .map(|cr| Compression::new(*cr.algorithm()))
                    .transpose()?;
                let comp_info = self.compression_info.as_deref().ok_or_else(|| {
                    Error::corruption(
                        "BTI chunk_targeted path requires CompressionInfo but it is absent",
                    )
                })?;
                let chunk_source = super::super::chunk_source::ChunkSource::new(
                    self.point_source.as_ref(),
                    comp_info,
                    compression_opt.as_ref(),
                    &self.chunk_cache,
                    self.stats.file_size,
                    0, // NB/BTI: chunk offsets are absolute from Data.db byte 0
                    super::NS_BTI_CHUNK,
                    self.chunk_cache_id,
                );
                match chunk_source.chunk(chunk_index)? {
                    Some(decompressed_chunk) => {
                        chunk_index += 1;
                        window.extend_from_slice(&decompressed_chunk);
                    }
                    None => {
                        // EOF (#3721 job 80: a remembered failure is not absence).
                        return point_read_absence_or_remembered(&mut undecodable);
                    }
                }
            }

            // Need at least the partition header to attempt a match.
            if within >= window.len() {
                if chunk_targeted {
                    // Not enough bytes yet; pull the next chunk.
                    continue;
                }
                // Whole-section window can't grow: offset is past the data.
                return Err(Error::corruption(format!(
                    "BTI trie resolved Data.db offset {} beyond decompressed data section ({} bytes)",
                    offset,
                    window.len()
                )));
            }

            // INVARIANT 3 + chunk-straddle gate. The parse/pull/absent decision is
            // factored into the pure `bti_lookup_step` so the chunk-straddle control
            // flow is unit-testable without a multi-chunk fixture (issue #831 review):
            // when the header/key prefix is not yet fully buffered we must NOT invoke
            // the parser on a truncated header (it can skip bytes and emit a later
            // false-positive entry), and must read the next chunk first.
            let key_available =
                Self::bti_partition_key_bytes_available(&window, within, key.as_bytes());
            let key_matches =
                key_available && self.bti_partition_key_matches(&window, within, key.as_bytes());
            match bti_lookup_step(key_available, key_matches, chunk_targeted) {
                BtiLookupStep::Parse => { /* full key prefix buffered and matches */ }
                BtiLookupStep::PullNextChunk => continue,
                BtiLookupStep::Absent => {
                    if key_available {
                        debug!(
                            "BTI trie candidate at offset {} did not match queried key \
                             (prefix collision); treating as absent",
                            offset
                        );
                    }
                    return Ok(None);
                }
            }

            // Attempt to parse the FIRST partition at window[within..]. The parser
            // detects the next partition boundary / 0x01 end-of-partition marker and
            // stops; we break after the first emitted entry. A COMPLETE partition
            // means: parse returned Ok AND the closure emitted OUR queried partition
            // key (see `emitted_our_key` below).
            let mut found: Option<ScanRow> = None;
            let mut emitted_our_key = false;
            // #3782: a chunk-covering WINDOW — a truncated tail here is this
            // reader's straddle signal ("pull the next chunk"), not corruption.
            let parse_result = parser.parse_block_emit(
                &window[within..],
                // Issue #3782 x #3721: the extent MUST come from the same finality state the
                // guard below uses (`chunk_targeted`), or the two mechanisms cancel.
                //
                // `Window` unconditionally is what broke `issue_3721_bti_point_read_absence`:
                // on the WHOLE-SECTION path every byte is already present, but a tolerant
                // extent makes the parse SWALLOW the decode failure and return `Ok(())`, so
                // `point_read_remember_or_bail` remembers nothing and
                // `point_read_absence_or_remembered` answers `Ok(None)` — job 80's phantom
                // ABSENCE, reintroduced through an AUTO-MERGED line, not a resolved conflict.
                //
                // Chunk-targeted, more bytes can arrive, so `Window` is right there and the
                // #1572 straddle retry stays intact.
                if chunk_targeted {
                    BufferExtent::Window
                } else {
                    BufferExtent::Complete
                },
                schema_opt,
                self,
                |(tid, entry_key, entry_value)| {
                    // Did the parser decode OUR queried partition key? This is the
                    // authoritative "the partition was fully buffered and parsed
                    // cleanly" signal: `bti_partition_key_matches` already confirmed
                    // our key's raw bytes sit at `within`, so a COMPLETE parse must
                    // re-decode exactly those bytes. A DIFFERENT decoded key means the
                    // window tail was truncated mid-partition and the parser resynced
                    // onto garbage (the chunk-straddle case handled below).
                    let is_our_key = entry_key.as_bytes() == key.as_bytes();
                    if is_our_key {
                        emitted_our_key = true;
                    }
                    // Verify BOTH the emitted table id is consistent with the
                    // queried table AND the parser-decoded partition key equals the
                    // queried key. The table check is resolution-mode-aware (issue
                    // #1321, mirroring the seek path #1284): an EXACT fully-qualified
                    // resolution accepts a consistent table name across a benign
                    // header-keyspace divergence; a fully-qualified query resolved via
                    // the bare-name fallback keeps STRICT keyspace matching so it can
                    // never return another keyspace's same-named rows. A genuinely
                    // different table name is always rejected (issue #831).
                    if table_header_consistent_for_seek(&tid, table_id, fully_qualified_match)
                        && is_our_key
                    {
                        found = Some(entry_value);
                    }
                    Ok(std::ops::ControlFlow::Break(()))
                },
            );

            match parse_result {
                Ok(()) if emitted_our_key => {
                    // The partition at `within` parsed COMPLETELY: our queried key was
                    // decoded from a fully-buffered window. Return the row when the
                    // table guard also passed (`found`), else `None` — a genuine
                    // soft-miss (schema-unavailable / benign table-guard rejection)
                    // the caller resolves via `scan_for_key`.
                    return Ok(found);
                }
                _ => {
                    // The parse did NOT decode our key: an `Err`, the closure never
                    // firing, or a FOREIGN key. Chunk-targeted, the latter two are the
                    // #1572 STRADDLE case, answered by the next chunk (absence would
                    // wrongly fall back to a whole-file `scan_for_key`).
                    // #3721 (job 80): an `Err` is a THIRD state and a decode failure
                    // is not absence — see `point_read_remember_or_bail`, which also
                    // says why the straddle retry stays exactly as it was.
                    point_read_remember_or_bail(parse_result, chunk_targeted, &mut undecodable)?;
                    if chunk_targeted {
                        continue;
                    }
                    // Every byte present, and no absence-ruling-out failure: prior
                    // behaviour (closure never fired, or a foreign key decoded).
                    return Ok(None);
                }
            }
        }
    }

    /// Collect-ALL-rows variant of [`bti_decompress_and_parse_target`] for the
    /// within-SSTable seek (`scan_single_partition`, Issue #953 / #951).
    ///
    /// [`bti_decompress_and_parse_target`] stops after the FIRST emitted row of the
    /// decoded partition — correct for a `get()` point lookup that returns a single
    /// `Value`, but WRONG for `scan_partition`, which must hand the query layer
    /// EVERY clustering row of the partition so it can apply clustering predicates.
    /// A `WHERE pk = ?` over a table with multiple clustering rows per partition
    /// would otherwise drop every row after the first whenever the seek succeeds
    /// (the original #953 bug — see the multi-row regression test).
    ///
    /// This variant reuses the identical window-building (chunk targeting or
    /// whole-section fallback), the identical prefix-collision key re-verification,
    /// and the identical `parse_block_emit` decode that the user-facing scan path
    /// runs — but instead of breaking after the first row it COLLECTS every row the
    /// parser emits for the ONE target partition. The emit closure keeps each
    /// `Value` whose decoded key equals the queried key (and whose table id
    /// matches) and `Break`s the instant the parser emits a row with a DIFFERENT
    /// partition key.
    ///
    /// Bounding the decompression window (Issue #953 / #951 MEDIUM fix). The seek
    /// must materialize ONLY the chunks covering the target partition — never
    /// stitch to EOF (for a head-of-file point lookup on a large SSTable that would
    /// decompress nearly the whole `Data.db`, full-table I/O for one partition).
    /// The bound is AUTHORITATIVE, not a heuristic boundary scan:
    ///
    ///   - **`decode_end_bound = Some(end)`** — the caller resolved the SUCCESSOR
    ///     partition's uncompressed start offset (next trie/index entry). The
    ///     target partition occupies `[offset, end)`, so we pull chunks only until
    ///     `window.len() >= end - window_base` (or EOF) and then parse ONCE over a
    ///     window that fully contains the partition. Because the WHOLE `[offset,
    ///     end)` extent is decompressed before parsing, a row/cell that spans
    ///     multiple compression chunks is present in full — no mid-stream
    ///     truncation, no boundary guessing. This is the exact bound for every
    ///     non-last partition in both BTI (`da`) and BIG (`nb`).
    ///
    ///   - **`decode_end_bound = None`** — `offset` is the LAST partition (no successor).
    ///     The end is then the authoritative data-section length
    ///     (`CompressionInfo.data_length`); we buffer to that length (or EOF) and
    ///     parse once. If that length is unavailable (no usable `CompressionInfo`),
    ///     we CANNOT bound the last partition authoritatively, so we return
    ///     `Ok(None)` and the caller falls back to the safe full-scan + retain path
    ///     (correctness over optimization). The previous row-count *stability
    ///     guard* — itself a heuristic that could falsely accept a next-partition
    ///     boundary while the target partition was incomplete (a single large
    ///     multi-chunk cell, static/range-marker regions, or a truncated tail
    ///     parsed as garbage headers) — has been REMOVED entirely.
    ///
    /// The whole-section fallback (uncompressed BTI) already has every byte so its
    /// first parse holds the whole partition regardless of the bound. This yields
    /// byte-for-byte the same rows as the full-scan path filtered down to
    /// `partition_key`.
    ///
    /// Bounding the PARSE input (Issue #3890). Bounding the DECOMPRESSION is not
    /// the same as bounding the PARSE: chunks are fixed-size, so the buffered
    /// window overruns the partition's end by up to one chunk, and the parser this
    /// hands off to is a MULTI-partition block walker. Both call sites below
    /// therefore pass the partition's authoritative exclusive end down to
    /// [`bti_collect_partition_rows`](Self::bti_collect_partition_rows), which
    /// slices the parser's input at it. That end is `partition_end_bound` — the
    /// UN-narrowed successor offset — never `decode_end_bound`, which an engaged
    /// #954 clustering slice tightens to a row-index block extent INSIDE the
    /// partition (bounding the parse there would truncate a row that starts just
    /// before the block end; the clamp to the buffered window makes the bound a
    /// no-op in that case, preserving the #954 behaviour exactly). See that
    /// function's doc comment for what the overrun actually caused.
    ///
    /// Returns:
    /// - `Ok(Some(rows))` — the partition's rows (empty when the trie/index
    ///   candidate was a prefix collision for an absent key). The caller wraps each
    ///   in a `(RowKey, ScanRow)` and applies the same tombstone suppression the scan
    ///   path applies.
    /// - `Ok(None)` — could not bound the (last) partition authoritatively; the
    ///   caller must fall back to a full scan + retain.
    #[cfg(not(feature = "tombstones"))]
    pub(super) async fn bti_decompress_and_parse_target_all(
        &self,
        offset: usize,
        // The end of the byte extent to DECOMPRESS: the successor partition's
        // uncompressed start, TIGHTENED by an engaged #954 clustering slice to that
        // slice's row-index block extent. `None` = last partition.
        decode_end_bound: Option<usize>,
        // Issue #3890: the end of the target PARTITION itself — the successor
        // offset BEFORE any clustering-slice narrowing, so it is the authoritative
        // extent to bound the PARSE by. `None` = last partition (no successor); the
        // data-section length is then used, and if that is also unavailable no bound
        // is invented (#28 no-heuristics) and today's whole-window parse stands.
        partition_end_bound: Option<usize>,
        // Issue #954: when `Some((start_rel, end_rel))`, bound the partition's
        // row-body parse to that within-partition byte window (relative to the
        // partition start) so only the clustering slice's row-index block(s) are
        // decoded. `None` decodes the whole partition (the #953 behaviour).
        row_body_window: Option<(usize, usize)>,
        key: &RowKey,
        table_id: &TableId,
        // See `bti_collect_partition_rows`: `true` iff the manager resolved this
        // reader by an exact fully-qualified `keyspace.table` match (or the query
        // was unqualified). Threaded into the seek table-consistency guard (#1284).
        fully_qualified_match: bool,
        schema_opt: Option<&crate::schema::TableSchema>,
        parser: &crate::storage::sstable::reader::parsing::V5CompressedLegacyParser,
    ) -> Result<Option<Vec<ScanRow>>> {
        // Issue #1573 (C2): positioned reads on the shared `point_source` — no
        // per-lookup `open(2)`, no cursor, no mutex. `chunk_index` is a plain local.

        // Determine the chunk-targeting parameters. `chunk_length == 0` (or no
        // CompressionInfo) means we cannot chunk-target -> whole-section fallback.
        let chunk_length = self
            .compression_info
            .as_ref()
            .map(|ci| ci.chunk_length as usize)
            .filter(|&len| len > 0);

        let mut chunk_index = 0usize;
        let (target_chunk, window_base, mut window) = match chunk_length {
            Some(len) => {
                let (target_chunk, window_base, _within) = Self::bti_chunk_target(offset, len);
                // Positioned reads resolve their own offset from the chunk index.
                chunk_index = target_chunk;
                (target_chunk, window_base, Vec::<u8>::new())
            }
            None => {
                // Whole-section fallback (uncompressed BTI, or chunk_length absent/0):
                // one positioned read of the whole data section, CRC-verified when a
                // CRC.db is present (see `point_read_whole_section`).
                let whole = self.point_read_whole_section().await?;
                (0usize, 0usize, whole)
            }
        };

        if offset < window_base {
            return Err(Error::corruption(format!(
                "BTI single-partition seek: resolved offset {} precedes window base {} (chunk {})",
                offset, window_base, target_chunk
            )));
        }
        let within = offset - window_base;
        let chunk_targeted = chunk_length.is_some();

        if chunk_targeted {
            // Resolve the AUTHORITATIVE exclusive end of the target partition in
            // the UNCOMPRESSED offset domain. Non-last partitions are bounded by
            // the successor partition's start (`end_bound`); the LAST partition is
            // bounded by the data-section length. When NEITHER is known we cannot
            // bound the last partition without re-introducing a heuristic, so we
            // return `Ok(None)` and let the caller fall back to a full scan.
            let data_length = self
                .compression_info
                .as_ref()
                .map(|ci| ci.data_length as usize)
                .filter(|&len| len > offset);
            let end_offset = match decode_end_bound.or(data_length) {
                Some(end) => end,
                None => {
                    debug!(
                        "BTI single-partition seek: last partition at offset {} has no \
                         authoritative end (no successor, no usable data_length); falling \
                         back to full scan",
                        offset
                    );
                    return Ok(None);
                }
            };
            // Issue #3890: the PARSE bound is the partition's OWN authoritative
            // extent, which equals `decode_end_bound` only when no #954 clustering
            // slice narrowed it. Same domain shift as `needed` below.
            let partition_end_within = partition_end_bound
                .or(data_length)
                .map(|end| end.saturating_sub(window_base));

            // Step 1: buffer enough chunks to expose the partition header, then run
            // the prefix-collision / chunk-straddle gate. This bails out cheaply
            // (without decompressing the rest of the partition) when the trie/index
            // candidate is a prefix collision for an absent key.
            loop {
                // Pull a chunk if the header is not yet (fully) buffered.
                if within + 2 > window.len()
                    || !Self::bti_partition_key_bytes_available(&window, within, key.as_bytes())
                {
                    match self
                        .bti_pull_decompressed_chunk(&mut chunk_index, &mut window)
                        .await?
                    {
                        true => continue, // chunk appended; re-check the header
                        false => {
                            // EOF before the header is buffered: nothing decodable
                            // at the resolved offset.
                            return Ok(Some(Vec::new()));
                        }
                    }
                }

                let key_matches = self.bti_partition_key_matches(&window, within, key.as_bytes());
                if !key_matches {
                    debug!(
                        "BTI seek candidate at offset {} did not match queried key \
                         (prefix collision); treating as absent",
                        offset
                    );
                    return Ok(Some(Vec::new()));
                }
                break; // header buffered AND key matches
            }

            // Step 2: buffer EXACTLY the chunks covering `[offset, end_offset)` —
            // never stitch to EOF (the #953 MEDIUM finding: a head-of-file lookup
            // would otherwise decompress the whole file). `end_offset` is in the
            // same uncompressed-offset domain as `window_base + window.len()`, so
            // the window holds the whole partition once `window.len()` reaches
            // `end_offset - window_base` (or EOF — a stale end never reads past
            // EOF). Decompressing the FULL extent before parsing means a row/cell
            // that spans multiple compression chunks is present in full, so the
            // single parse below collects every target row without truncation.
            let needed = end_offset.saturating_sub(window_base);
            while window.len() < needed {
                if !self
                    .bti_pull_decompressed_chunk(&mut chunk_index, &mut window)
                    .await?
                {
                    break; // EOF: window holds all available bytes.
                }
            }
            return self
                .bti_collect_partition_rows(
                    &window,
                    within,
                    partition_end_within,
                    row_body_window,
                    key,
                    table_id,
                    fully_qualified_match,
                    schema_opt,
                    parser,
                )
                .map(|(rows, _complete)| Some(rows));
        }

        // Whole-section fallback (uncompressed BTI): every byte is already present,
        // so the first parse is authoritative.
        if within >= window.len() {
            return Err(Error::corruption(format!(
                "BTI trie resolved Data.db offset {} beyond decompressed data section ({} bytes)",
                offset,
                window.len()
            )));
        }
        // Issue #3890: `window_base` is 0 on this arm (the whole data section was
        // read), so `partition_end_bound` is already a `window` index. When it is
        // absent (the LAST partition) the data section's own length IS the
        // authoritative end and `window` holds exactly that section, so `None` —
        // parse to `window.len()` — is authoritative here too; no bound is invented.
        self.bti_collect_partition_rows(
            &window,
            within,
            partition_end_bound,
            row_body_window,
            key,
            table_id,
            fully_qualified_match,
            schema_opt,
            parser,
        )
        .map(|(rows, _complete)| Some(rows))
    }

    /// Read the next compressed chunk from `cursor`, decompress it (if the reader
    /// has a compression algorithm), and append the decompressed bytes to
    /// `window`. Returns `true` when a chunk was appended, `false` at EOF.
    ///
    /// Shared by the chunk-targeted seek so the header-buffering and
    /// partition-bounding loops use one decompression code path; each call bumps
    /// `work_counters::chunks_decompressed` so a test can prove the seek bounded
    /// its decompression to the target partition's chunk span (Issue #953/#951).
    ///
    /// Issue #1750 (regression fix): this now fetches through the CACHING
    /// [`ChunkSource::chunk`] — read → CRC → decompress → B1 cache — exactly like
    /// the legacy `get()` point-lookup path (`bti_decompress_and_parse_target`)
    /// did. Before #1750 retired the `is_simple_id_lookup` fork, a `WHERE pk = ?`
    /// point read routed to `get()` and populated/served the shared B1
    /// [`DecompressedChunkCache`]; rerouting it to the modern seek path made the
    /// seek's chunk fetch UNCACHED (`decompress_only`), so a repeated point read
    /// re-decompressed every chunk and `Database::stats().memory_stats` reported a
    /// structural zero B1 hit rate (the `dead_cache_delete` regression). Routing
    /// through `chunk()` keyed in the shared `NS_BTI_CHUNK` namespace makes a warm
    /// repeat read a refcount-bump cache HIT — restoring the pre-#1750 caching
    /// behavior — while remaining the SAME single decode plane (issue #1598, G2).
    ///
    /// `work_counters::chunks_decompressed` is bumped once per chunk the seek
    /// materializes into its `window` (a cache miss decompresses; a hit serves the
    /// resident bytes). The issue_953/#951 bound tests reset the counter per query
    /// and read each partition's chunks cold within the process, so materialized ==
    /// decompressed there and the "seek bounded its chunk span" assertions are
    /// unchanged; the counter still forbids a stitch-to-EOF regression.
    #[cfg(not(feature = "tombstones"))]
    async fn bti_pull_decompressed_chunk(
        &self,
        chunk_index: &mut usize,
        window: &mut Vec<u8>,
    ) -> Result<bool> {
        use crate::storage::sstable::compression::Compression;
        // Issue #1573 (C2): positioned chunk fetch — no cursor, no mutex, no
        // per-lookup open. CRC is verified inside `ChunkSource::chunk` BEFORE
        // decompression (guardrail #1411).
        //
        // Single decode plane (issue #1598, G2) WITH the shared B1 cache (issue
        // #1750): `ChunkSource::chunk` does read → CRC → decompress → cache, keyed
        // by the ABSOLUTE chunk index in the NS_BTI_CHUNK namespace — the same key
        // space the legacy `get()` point read used, so a warm repeat read hits.
        let compression_opt = self
            .compression_reader
            .as_ref()
            .map(|cr| Compression::new(*cr.algorithm()))
            .transpose()?;
        let comp_info = self.compression_info.as_deref().ok_or_else(|| {
            Error::corruption(
                "BTI seek chunk-targeted path requires CompressionInfo but it is absent",
            )
        })?;
        let chunk_source = super::super::chunk_source::ChunkSource::new(
            self.point_source.as_ref(),
            comp_info,
            compression_opt.as_ref(),
            &self.chunk_cache,
            self.stats.file_size,
            0, // NB/BTI: chunk offsets are absolute from Data.db byte 0
            super::NS_BTI_CHUNK,
            self.chunk_cache_id,
        );
        match chunk_source.chunk(*chunk_index)? {
            Some(decompressed_chunk) => {
                *chunk_index += 1;
                // Issue #953/#951: count every chunk the seek materializes so a
                // bound test can prove the window is bounded to the target
                // partition's chunk span, not stitched to EOF.
                super::super::super::work_counters::add_chunk_decompressed();
                window.extend_from_slice(&decompressed_chunk);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Returns true when the `[flags][key_len: u8][key bytes]` prefix at `within`
    /// is fully present in `window` AND `key_len` equals `expected_key.len()`.
    ///
    /// Used by the chunk-targeted BTI lookup to decide whether the INVARIANT-3
    /// key match can be evaluated yet, or whether more chunk bytes must be pulled
    /// first (issue #831).
    fn bti_partition_key_bytes_available(
        window: &[u8],
        within: usize,
        _expected_key: &[u8],
    ) -> bool {
        // Need flags + key_len byte first.
        if within + 2 > window.len() {
            return false;
        }
        let key_len = window[within + 1] as usize;
        // The declared key bytes must all be buffered. (Whether `key_len` equals
        // the expected length is decided by the subsequent match check, which
        // fails fast on a mismatch — here we only require the bytes be present.)
        within + 2 + key_len <= window.len()
    }

    /// Verify the on-disk partition-key bytes at `offset` in the decompressed
    /// data section equal `expected_key` (issue #831, INVARIANT 3).
    ///
    /// Reads the `[flags][key_len: u8][key bytes]` prefix. Returns `false` (rather
    /// than erroring) on any structural mismatch so the caller can treat the trie
    /// candidate as absent.
    fn bti_partition_key_matches(
        &self,
        decompressed: &[u8],
        offset: usize,
        expected_key: &[u8],
    ) -> bool {
        // Need at least flags + key_len.
        if offset + 2 > decompressed.len() {
            return false;
        }
        let key_len = decompressed[offset + 1] as usize;
        let key_start = offset + 2;
        let key_end = key_start + key_len;
        if key_end > decompressed.len() {
            return false;
        }
        &decompressed[key_start..key_end] == expected_key
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The chunk-index arithmetic must match `CompressionInfo`'s definitions:
    /// `target_chunk = off / chunk_length`, `window_base = target_chunk *
    /// chunk_length`, `within = off - window_base` (== `off % chunk_length`).
    #[test]
    fn bti_chunk_target_arithmetic() {
        // Single-chunk case (simple_table fixture shape): chunk_length 16384,
        // offset 0/63/125 all land in chunk 0 with within == offset.
        let chunk_length = 16384;
        for off in [0usize, 63, 125] {
            let (chunk, base, within) = SSTableReader::bti_chunk_target(off, chunk_length);
            assert_eq!(chunk, 0, "off {off} must be in chunk 0");
            assert_eq!(base, 0, "chunk 0 window base must be 0");
            assert_eq!(within, off, "within must equal offset in chunk 0");
        }

        // Multi-chunk arithmetic with a small chunk_length to exercise the math.
        let cl = 100usize;
        // Exactly on a chunk boundary.
        assert_eq!(SSTableReader::bti_chunk_target(100, cl), (1, 100, 0));
        assert_eq!(SSTableReader::bti_chunk_target(200, cl), (2, 200, 0));
        // Inside chunk 1.
        assert_eq!(SSTableReader::bti_chunk_target(150, cl), (1, 100, 50));
        // Just before a boundary.
        assert_eq!(SSTableReader::bti_chunk_target(99, cl), (0, 0, 99));
        // Within always equals off % chunk_length, base = chunk * chunk_length.
        for off in [0usize, 1, 99, 100, 101, 250, 999] {
            let (chunk, base, within) = SSTableReader::bti_chunk_target(off, cl);
            assert_eq!(within, off % cl);
            assert_eq!(base, chunk * cl);
            assert_eq!(base + within, off);
        }
    }

    /// `bti_partition_key_bytes_available` drives the growing-window stop
    /// condition: while the `[flags][key_len][key bytes]` prefix is NOT yet fully
    /// buffered it returns false (the chunk-targeted loop pulls another chunk);
    /// once the declared key bytes have all arrived it returns true (the
    /// INVARIANT-3 key match can be evaluated). This is the SYNTHETIC spanning
    /// test: the key prefix straddles a simulated chunk boundary and the window
    /// grows one byte at a time across it.
    ///
    /// NOTE: a full multi-chunk-spanning parse against a real
    /// `V5CompressedLegacyParser` has NO real BTI DataOffset fixture — these are
    /// narrow partitions that fit within a single chunk — so the spanning *parse*
    /// path is only exercised structurally here via the byte-availability gate
    /// that decides when a parse may even be attempted. This calls the real
    /// associated function (no I/O), so a regression in its boundary math is
    /// caught.
    #[test]
    fn bti_partition_key_bytes_available_growing_window() {
        // Header at within=0: [flags=0x00][key_len=4][k0 k1 k2 k3]. Simulate a
        // window that grows from 0 bytes up to the full prefix; availability must
        // flip to true exactly when all 4 declared key bytes are buffered.
        let expected_key = [0xAA, 0xBB, 0xCC, 0xDD];
        let within = 0usize;
        let full = {
            let mut v = vec![0x00u8, expected_key.len() as u8];
            v.extend_from_slice(&expected_key);
            v
        };

        let avail = |len: usize| {
            SSTableReader::bti_partition_key_bytes_available(&full[..len], within, &expected_key)
        };

        // Not enough for flags+key_len yet.
        assert!(!avail(0));
        assert!(!avail(1));
        // flags+key_len present but key bytes not fully buffered.
        assert!(!avail(2));
        assert!(!avail(3)); // 1 key byte
        assert!(!avail(4)); // 2 key bytes
        assert!(!avail(5)); // 3 key bytes
                            // All 4 key bytes buffered -> available (boundary fully crossed).
        assert!(avail(6));
        assert!(avail(full.len()));

        // A non-zero `within` (target partition not at window start) must use the
        // same relative math.
        let mut padded = vec![0x77u8, 0x88];
        padded.extend_from_slice(&full);
        assert!(!SSTableReader::bti_partition_key_bytes_available(
            &padded[..2 + 5],
            2,
            &expected_key
        ));
        assert!(SSTableReader::bti_partition_key_bytes_available(
            &padded,
            2,
            &expected_key
        ));
    }
}
