//! Data access methods for SSTableReader
//!
//! This module contains all methods related to reading data from SSTables,
//! including point lookups, range scans, and sequential access.

use super::source::ScanCursor;
use super::SSTableReader;
use crate::parser::DataFormat;
use crate::types::{CellWriteMetadata, TableId, Value};
use crate::util::cassandra_murmur3::cassandra_murmur3_token;
use crate::{Error, Result, RowKey};
use log::{debug, warn};
use std::io::SeekFrom;
use tokio::io::AsyncSeekExt;
use tokio::sync::mpsc;

/// Counter of `scan_for_key` invocations, used by tests to prove the BTI
/// point-lookup path never falls through to a sequential scan (issue #831).
///
/// Incremented at the top of [`SSTableReader::scan_for_key`] and read via
/// [`SSTableReader::scan_for_key_call_count`]. The increment is a single
/// `Relaxed` atomic add on a cold path, so the runtime cost is negligible; it is
/// not gated behind `cfg(test)` because integration tests in the `tests/`
/// directory compile against the library crate without its `test` cfg.
pub(crate) static SCAN_FOR_KEY_CALLS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Compare two table IDs, handling both qualified (keyspace.table) and unqualified (table) formats.
///
/// This function allows flexible matching:
/// - "keyspace.table" matches "keyspace.table" (exact match)
/// - "table" matches "keyspace.table" (unqualified matches qualified)
/// - "keyspace.table" matches "table" (qualified matches unqualified)
/// - "table" matches "table" (exact match)
///
/// This is necessary because:
/// - Dataset mode SSTables store qualified table_ids (e.g., "test_basic.simple_table")
/// - Queries can use either qualified ("test_basic.simple_table") or unqualified ("simple_table") names
/// - Production SSTables may use unqualified table_ids
fn table_ids_match(entry_table_id: &TableId, query_table_id: &TableId) -> bool {
    let entry_name = entry_table_id.name();
    let query_name = query_table_id.name();

    // Fast path: exact match
    if entry_name == query_name {
        return true;
    }

    // Extract unqualified table names for comparison
    let entry_unqualified = if let Some(dot_pos) = entry_name.rfind('.') {
        &entry_name[dot_pos + 1..]
    } else {
        entry_name
    };

    let query_unqualified = if let Some(dot_pos) = query_name.rfind('.') {
        &query_name[dot_pos + 1..]
    } else {
        query_name
    };

    // Match if unqualified names are the same
    entry_unqualified == query_unqualified
}

/// Stricter table-id match used by the BTI point-lookup guard (issue #831 review).
///
/// [`table_ids_match`] matches on the unqualified table name, so it treats
/// `ks_a.users` and `ks_b.users` as equal — fine for index lookups that are
/// already scoped to one table, but too permissive as a defensive guard against
/// a fully-qualified wrong-keyspace query. When BOTH ids are qualified
/// (`keyspace.table`), this requires exact `keyspace.table` equality; it only
/// falls back to the permissive unqualified match when one side lacks a
/// keyspace (preserving qualified-vs-unqualified flexibility).
fn table_ids_match_strict(entry_table_id: &TableId, query_table_id: &TableId) -> bool {
    let entry_qualified = entry_table_id.name().contains('.');
    let query_qualified = query_table_id.name().contains('.');
    if entry_qualified && query_qualified {
        entry_table_id.name() == query_table_id.name()
    } else {
        table_ids_match(entry_table_id, query_table_id)
    }
}

/// Per-iteration decision for the BTI chunk-targeted point-lookup loop.
#[derive(Debug, PartialEq, Eq)]
enum BtiLookupStep {
    /// The full partition-key prefix is buffered and matches the queried key —
    /// parse the partition.
    Parse,
    /// The header/key prefix straddles a chunk boundary and is not yet fully
    /// buffered — read the next chunk before parsing (chunk-targeted path only).
    PullNextChunk,
    /// Treat the partition as absent: either the buffered key prefix does not
    /// match, or a whole-section window is structurally too short to grow.
    Absent,
}

/// Decide what the BTI point-lookup loop should do for the current window state.
///
/// Pure so the chunk-straddle control flow is unit-testable without a real
/// multi-chunk BTI fixture (DataOffset partitions are narrow and fit in one
/// chunk, so a boundary-straddling header cannot be produced by the available
/// fixtures). Crucially, when the key prefix is not yet buffered on the
/// chunk-targeted path this returns [`BtiLookupStep::PullNextChunk`] — it must
/// NOT lead to parsing a truncated header (issue #831 review).
fn bti_lookup_step(
    key_prefix_available: bool,
    key_matches: bool,
    chunk_targeted: bool,
) -> BtiLookupStep {
    if key_prefix_available {
        if key_matches {
            BtiLookupStep::Parse
        } else {
            BtiLookupStep::Absent
        }
    } else if chunk_targeted {
        BtiLookupStep::PullNextChunk
    } else {
        BtiLookupStep::Absent
    }
}

/// Sort a result slice in ascending Cassandra token order.
///
/// The authoritative ordering for SSTable partitions is ascending Murmur3 token, with
/// equal-token ties broken by raw key bytes (lexicographic). This matches the on-disk
/// physical order (spec §5, Appendix B §313) and the write engine's `PartitionPosition::cmp`.
///
/// Computes each key's token once to avoid O(n log n) recomputation inside the comparator.
fn sort_by_token_order(results: &mut Vec<(RowKey, Value)>) {
    // Map to (token, RowKey, Value), sort, then reassemble.
    let mut tagged: Vec<(i64, RowKey, Value)> = results
        .drain(..)
        .map(|(k, v)| {
            let t = cassandra_murmur3_token(k.as_bytes());
            (t, k, v)
        })
        .collect();
    tagged.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    results.extend(tagged.into_iter().map(|(_, k, v)| (k, v)));
}

/// Sort `(RowKey, Value, CellMeta)` triples by Cassandra Murmur3 token order.
fn sort_by_token_order_with_meta(
    results: &mut Vec<(
        RowKey,
        Value,
        std::collections::HashMap<String, CellWriteMetadata>,
    )>,
) {
    let mut tagged: Vec<(
        i64,
        RowKey,
        Value,
        std::collections::HashMap<String, CellWriteMetadata>,
    )> = results
        .drain(..)
        .map(|(k, v, m)| {
            let t = cassandra_murmur3_token(k.as_bytes());
            (t, k, v, m)
        })
        .collect();
    tagged.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    results.extend(tagged.into_iter().map(|(_, k, v, m)| (k, v, m)));
}

impl SSTableReader {
    /// Return `true` when Data.db uses the V5CompressedLegacy NB chunked format and
    /// therefore requires all chunks to be stitched before parsing.
    ///
    /// The correct predicate is:
    ///   data_format == V5CompressedLegacy  AND  is_nb_format()
    ///
    /// Rationale:
    /// - `V5CompressedLegacy` identifies the row serialization format (u16 length
    ///   prefixes, legacy encoding) used by all Cassandra 5 'nb' SSTables.
    /// - `is_nb_format()` identifies the chunked-compression read path. It intentionally
    ///   EXCLUDES `V5_0Uncompressed`, which uses the same row format but stores data as
    ///   a single contiguous block (no chunk boundaries, no stitching needed).
    /// - Using `is_compressed` (compression_reader.is_some()) would be wrong for NB
    ///   format because the per-chunk decompression is handled inside `stitch_and_parse_all_chunks`,
    ///   and `is_compressed` may differ from `is_nb_format` for edge-case versions.
    fn requires_chunk_stitching(&self) -> bool {
        let data_format = self.header.cassandra_version.data_format();
        matches!(data_format, DataFormat::V5CompressedLegacy)
            && self.header.cassandra_version.is_nb_format()
    }

    /// Get a value by key from the SSTable
    pub async fn get(&self, table_id: &TableId, key: &RowKey) -> Result<Option<Value>> {
        // Issue #831 / #909: BTI ("da") readers resolve partitions via the
        // Partitions.db trie (O(log n)), never via Index.db (absent for BTI) or
        // the sequential scan. The trie is the AUTHORITATIVE presence oracle for a
        // BTI SSTable — it answers present/absent definitively — so we branch here
        // BEFORE the bloom-filter pre-check. Skipping the bloom filter for BTI is
        // both correct (the trie is authoritative; bloom is only an optimization)
        // and necessary: a writer-produced Filter.db whose hashing does not match
        // the reader's would otherwise cause false negatives and drop live
        // partitions (the writer→reader roundtrip #909 must read back). It also
        // guarantees a BTI get() can never fall through to scan_for_key.
        if self.bti_partitions_db.is_some() {
            return self.bti_point_lookup(table_id, key).await;
        }

        // First check bloom filter if available
        if let Some(bloom_filter) = &self.bloom_filter {
            if !bloom_filter.might_contain(key.as_bytes()) {
                return Ok(None);
            }
        }

        // Use index for efficient lookup if available
        if let Some(index) = &self.index {
            if let Some(entry) = index.find_entry(table_id, key).await? {
                // When Index.db reports size=0 (Cassandra 5.0), fall back to sequential scan
                if entry.size == 0 {
                    log::debug!(
                        "Index reports size=0 for key {:?}, using sequential scan fallback",
                        key
                    );
                    return self.scan_for_key(table_id, key).await;
                }

                // Index offsets are relative to data section start - adjust for header
                let file_offset = entry.offset + self.actual_header_size as u64;
                return self.read_value_at_offset(file_offset, entry.size).await;
            }

            // Issue #517: The SSTableIndex is built from Index.db key *digests* (16-byte
            // Murmur3 hashes), not raw partition key bytes.  A raw-key lookup via
            // find_entry() always misses.  Fall back to scan_for_key() so that get()
            // and scan() agree on which partitions exist.
            log::debug!(
                "Index lookup returned no entry for key {:?} (possible digest/raw-key mismatch), \
                 falling back to sequential scan",
                key
            );
            return self.scan_for_key(table_id, key).await;
        } else {
            // No index at all — fall back to sequential scan
            return self.scan_for_key(table_id, key).await;
        }
    }

    /// Current value of the test-only `scan_for_key` invocation counter.
    ///
    /// Issue #831: tests use this to assert that a BTI `get()` resolves entirely
    /// through the Partitions.db trie and never falls through to the sequential
    /// scan. See [`SCAN_FOR_KEY_CALLS`].
    pub fn scan_for_key_call_count() -> u64 {
        SCAN_FOR_KEY_CALLS.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Single-partition *seek* for the partition-targeted lookup fast path
    /// (Issue #953, Epic #951).
    ///
    /// Where [`scan`](Self::scan) decodes EVERY partition in this SSTable and the
    /// caller retains one, this resolves the target partition's `Data.db` offset
    /// from the authoritative index (the BTI Partitions.db trie or the BIG
    /// `Index.db`) and decodes ONLY that partition — the same per-partition decode
    /// `scan` runs (`parse_block_emit` over the chunk-targeted decompressed
    /// window), so its output is byte-for-byte identical to filtering the full
    /// `scan` result down to `partition_key`.
    ///
    /// Return contract (the caller branches on it):
    /// - `Ok(Some(rows))` — the seek RESOLVED and DECODED authoritatively. `rows`
    ///   is the target partition's rows in on-disk form: one `(RowKey, Value)` for
    ///   a hit (the `RowKey` is `partition_key`), or empty for a key the index
    ///   proves absent / that decoded to a tombstone. The caller uses `rows`
    ///   directly and does NOT fall back.
    /// - `Ok(None)` — the seek is NOT APPLICABLE for this reader (no trie and no
    ///   `Index.db` hit, or a format/offset the seek cannot target). The caller
    ///   MUST fall back to `scan` + retain for correctness (Constraint #4).
    ///
    /// Offset domains (no-heuristics: authoritative resolved offsets only):
    /// - **BTI ("da")** — `lookup_partition_via_bti_trie` returns the UNCOMPRESSED
    ///   `Data.db` offset; a trie miss is authoritative absence (`Ok(Some(vec![]))`).
    /// - **BIG (`nb`)** — `lookup_partition_with_index` returns the partition's
    ///   offset into the (uncompressed) data section. A hit is authoritative
    ///   present; a MISS returns `Ok(None)` (the `Index.db` may be digest-keyed or
    ///   incomplete, exactly the `get()` fallback rationale at #517) so the caller
    ///   re-checks via a full scan rather than risk a false negative.
    ///
    /// Prefix-collision / wrong-offset guard: the decode
    /// ([`bti_decompress_and_parse_target_all`]) re-verifies the decoded partition
    /// key equals `partition_key` before collecting any row, so a BTI
    /// prefix-collision candidate or a stale/mismatched index offset decodes to
    /// nothing and is reported as absent — never a wrong partition. Every
    /// clustering row of the matched partition is collected (not just the first),
    /// so a multi-row partition returns all rows.
    ///
    /// [`bti_decompress_and_parse_target_all`]: Self::bti_decompress_and_parse_target_all
    ///
    /// Compiled only for the default (`not(tombstones)`) build: the manager's
    /// seek-driven `scan_partition` exists only there, so under `tombstones` this
    /// would be dead code.
    #[cfg(not(feature = "tombstones"))]
    pub(crate) async fn scan_single_partition(
        &self,
        table_id: &TableId,
        partition_key: &[u8],
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<Option<Vec<(RowKey, Value)>>> {
        // 1. Resolve the partition's uncompressed Data.db offset, and record
        //    whether THIS path's "decoded nothing" is authoritative absence (BTI
        //    trie) or merely inconclusive (BIG Index.db).
        let is_bti = self.bti_partitions_db.is_some();
        // `size_hint` is the AUTHORITATIVE uncompressed byte length of the target
        // partition when the index provides it (BIG `Index.db`). It bounds the
        // decompression window EXACTLY to `[offset, offset + size)` so a
        // head-of-file point lookup never decompresses the file's tail. The BTI
        // trie carries no size, so its window is bounded by next-partition
        // boundary detection instead (see `bti_decompress_and_parse_target_all`).
        let (offset, size_hint) = if is_bti {
            match self.lookup_partition_via_bti_trie(partition_key)? {
                // Trie hit: candidate uncompressed offset (re-verified on decode).
                Some(off) => (off as usize, None),
                // Trie miss is AUTHORITATIVE absence for BTI.
                None => return Ok(Some(Vec::new())),
            }
        } else {
            match self.lookup_partition_with_index(partition_key).await? {
                // Index.db hit: a candidate offset into the data section plus the
                // partition's authoritative uncompressed byte size (#953 bound).
                Some((off, size)) => (off as usize, Some(size as usize)),
                // No Index.db hit: cannot seek authoritatively (the index may be
                // digest-keyed / incomplete, exactly the get() #517 rationale).
                // Fall back to a full scan.
                None => return Ok(None),
            }
        };

        // 2. Decode ONLY the target partition at the resolved offset, using the
        //    SAME parser the scan path uses. `bti_decompress_and_parse_target_all`
        //    chunk-targets the decompression (decodes just the chunk window that
        //    holds the partition) and re-verifies the decoded key, so this is
        //    O(1) PARTITIONS decoded regardless of the SSTable's partition count.
        //
        //    Issue #953 correctness fix: this collects EVERY clustering row of the
        //    one target partition (stopping at the next-partition boundary), not
        //    just the first row, so a `WHERE pk = ?` over a multi-clustering-row
        //    partition returns all rows — byte-identical to filtering the full
        //    scan down to `partition_key`. The single-row `*_target` decoder is
        //    still used by the `get()` point-lookup path, which returns one Value.
        let schema_opt = self.get_table_schema(schema);
        let parser = self.build_v5_parser();
        let key = RowKey::from(partition_key.to_vec());
        let decoded_rows = self
            .bti_decompress_and_parse_target_all(
                offset,
                size_hint,
                &key,
                table_id,
                schema_opt.as_ref(),
                &parser,
            )
            .await?;

        // 3. Record the per-partition decode (Issue #953 / #958): exactly ONE
        //    partition is decoded for a hit regardless of how many clustering rows
        //    it yields — `partitions_decoded` counts partitions, not rows. This is
        //    the signal that proves the within-SSTable seek (vs a full
        //    parse-then-retain). A non-empty decode means the partition exists.
        if !decoded_rows.is_empty() {
            super::super::work_counters::add_partition_decoded();
            // Tombstone suppression matches the user-facing scan path
            // (`sequential_scan`/`bti_scan_with_metadata` both apply it),
            // applied per-row so a row tombstone is dropped while live rows in
            // the same partition survive.
            let rows: Vec<(RowKey, Value)> = decoded_rows
                .into_iter()
                .filter(|value| self.filter_tombstone(value))
                .map(|value| (key.clone(), value))
                .collect();
            return Ok(Some(rows));
        }

        // Decoded nothing at the resolved offset. Whether that is authoritative
        // depends on HOW the offset was resolved (Constraint #4: never return a
        // wrong/empty result from an unsupported/inconclusive seek):
        //
        // - **BTI** — the trie is the authoritative present/absent oracle and the
        //   decode re-verified the key, so "decoded nothing" means the trie
        //   candidate was a prefix-collision for an absent key. AUTHORITATIVE
        //   empty: the caller does NOT fall back.
        // - **BIG** — the `Index.db` offset is only a candidate position; its
        //   promoted-index / chunk layout is not as load-bearing as the BTI trie,
        //   so a failed decode at the resolved offset is INCONCLUSIVE (a partition
        //   that straddles a chunk boundary, or a stale offset, can fail the
        //   chunk-targeted decode yet be found by a full parse). Fall back to a
        //   full scan rather than risk a false negative.
        if is_bti {
            Ok(Some(Vec::new()))
        } else {
            Ok(None)
        }
    }

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
    ///   finding). The whole-section [`stitch_all_chunks`] fallback is used only
    ///   when chunk targeting is impossible (no/zero `chunk_length`).
    /// - **Prefix-collision guard**: the trie may return a candidate for a
    ///   prefix-colliding key, so the decoded partition key is verified to equal
    ///   the queried key before any row is returned.
    async fn bti_point_lookup(&self, table_id: &TableId, key: &RowKey) -> Result<Option<Value>> {
        // 1. Resolve the uncompressed Data.db offset via the trie.
        let offset = match self.lookup_partition_via_bti_trie(key.as_bytes())? {
            Some(off) => off as usize,
            None => return Ok(None), // not in this SSTable
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
        let parser = self.build_v5_parser();

        let found = self
            .bti_decompress_and_parse_target(offset, key, table_id, schema_opt.as_ref(), &parser)
            .await?;

        match found {
            Some(value) => {
                if !self.filter_tombstone(&value) {
                    return Ok(None);
                }
                Ok(Some(value))
            }
            None => Ok(None),
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
    /// decompresses the WHOLE section via [`stitch_all_chunks`] (`window_base = 0`)
    /// and runs the same single-partition parse.
    ///
    /// Uses its own per-scan [`ScanCursor`] (private file position + chunk
    /// index), so concurrent lookups run in parallel without serialization
    /// (issue #815).
    async fn bti_decompress_and_parse_target(
        &self,
        offset: usize,
        key: &RowKey,
        table_id: &TableId,
        schema_opt: Option<&crate::schema::TableSchema>,
        parser: &crate::storage::sstable::reader::parsing::V5CompressedLegacyParser,
    ) -> Result<Option<Value>> {
        use crate::storage::sstable::compression::Compression;

        // Issue #815: each lookup uses its own cursor so concurrent lookups on
        // this reader never share a mutable file position / chunk index.
        let cursor = self.new_scan_cursor().await?;

        // Determine the chunk-targeting parameters. `chunk_length == 0` (or no
        // CompressionInfo) means we cannot chunk-target -> whole-section fallback.
        let chunk_length = self
            .compression_info
            .as_ref()
            .map(|ci| ci.chunk_length as usize)
            .filter(|&len| len > 0);

        let (target_chunk, window_base, mut window) = match chunk_length {
            Some(len) => {
                let (target_chunk, window_base, _within) = Self::bti_chunk_target(offset, len);
                // Seek to the START of target_chunk so read_next_block() reads it
                // first, and set the shared chunk index accordingly. Chunk offsets
                // are relative to file start for NB/BTI (header_offset = 0).
                let chunk_start = self
                    .compression_info
                    .as_ref()
                    .and_then(|ci| ci.compressed_chunk_offset(target_chunk))
                    .ok_or_else(|| {
                        Error::corruption(format!(
                            "BTI point lookup: no compressed offset for target chunk {} \
                             (offset {}, chunk_length {})",
                            target_chunk, offset, len
                        ))
                    })?;
                {
                    let mut file_guard = cursor.file.lock().await;
                    file_guard.seek(SeekFrom::Start(chunk_start)).await?;
                }
                cursor
                    .chunk_index
                    .store(target_chunk, std::sync::atomic::Ordering::Relaxed);
                (target_chunk, window_base, Vec::<u8>::new())
            }
            None => {
                // Whole-section fallback (uncompressed BTI, or chunk_length absent/0).
                let header_size = self.calculate_header_size();
                {
                    let mut file_guard = cursor.file.lock().await;
                    file_guard.seek(SeekFrom::Start(header_size as u64)).await?;
                }
                let whole = self.stitch_all_chunks(&cursor).await?;
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

        loop {
            // If chunk-targeted, append the next chunk before each parse attempt
            // (the whole-section fallback already has all bytes in `window`).
            if chunk_targeted {
                match self.read_next_block(&cursor).await? {
                    Some(compressed_chunk) => {
                        let decompressed_chunk = if let Some(compression_reader) =
                            &self.compression_reader
                        {
                            let compression = Compression::new(*compression_reader.algorithm())?;
                            compression.decompress(&compressed_chunk).map_err(|e| {
                                Error::corruption(format!(
                                    "BTI point lookup: failed to decompress chunk: {}",
                                    e
                                ))
                            })?
                        } else {
                            // No compression reader despite CompressionInfo:
                            // treat raw chunk bytes as the decompressed data.
                            compressed_chunk
                        };
                        window.extend_from_slice(&decompressed_chunk);
                    }
                    None => {
                        // EOF: no more chunks. If we never parsed a complete
                        // partition, the partition is treated as absent (matching
                        // the prior whole-section behaviour for an unparseable tail).
                        return Ok(None);
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
            // stops; we break after the first emitted entry. A complete partition
            // means: parse returned Ok AND the closure fired.
            let mut found: Option<Value> = None;
            let mut emitted = false;
            let parse_result = parser.parse_block_emit(
                &window[within..],
                schema_opt,
                self,
                |(tid, entry_key, entry_value)| {
                    emitted = true;
                    // Verify BOTH the emitted table id matches the queried table
                    // (a wrong-table query never returns a row, issue #831 review)
                    // AND the parser-decoded partition key equals the queried key.
                    if table_ids_match_strict(&tid, table_id)
                        && entry_key.as_bytes() == key.as_bytes()
                    {
                        found = Some(entry_value);
                    }
                    Ok(std::ops::ControlFlow::Break(()))
                },
            );

            match parse_result {
                Ok(()) if emitted => {
                    // A COMPLETE partition was decoded — accept it and stop.
                    return Ok(found);
                }
                _ => {
                    // Either Err (truncated mid-partition) or the closure never
                    // fired (no complete partition yet). For the chunk-targeted
                    // path, pull the next chunk and retry; never accept a partial.
                    if chunk_targeted {
                        continue;
                    }
                    // Whole-section fallback already has every byte: a failure here
                    // means the partition genuinely could not be parsed -> absent.
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
    /// parser emits for the ONE target partition and stops at the next partition
    /// boundary. Concretely the emit closure:
    ///   - keeps each `Value` whose decoded key equals the queried key (and whose
    ///     table id matches), and
    ///   - `Break`s the instant the parser emits a row with a DIFFERENT partition
    ///     key — that row belongs to the NEXT partition, which proves the target
    ///     partition was fully decoded (definitive completeness, even mid-window).
    ///
    /// Bounding the decompression window (Issue #953 MEDIUM fix). The seek must
    /// materialize ONLY the chunks covering the target partition — never stitch to
    /// EOF, which for a head-of-file point lookup on a large SSTable would
    /// decompress nearly the whole `Data.db` (full-table I/O for one partition).
    /// Two authoritative bounds replace the old to-EOF stitch:
    ///
    ///   - **`size_hint = Some(size)` (BIG `nb`)** — `Index.db` gives the target
    ///     partition's exact uncompressed byte length. The partition occupies
    ///     `window[within .. within + size]`, so we pull chunks only until
    ///     `window.len() >= within + size` (or EOF) and then parse once over a
    ///     window that fully contains the partition. This is the exact bound: the
    ///     last chunk pulled is the one holding the partition's final byte.
    ///
    ///   - **`size_hint = None` (BTI `da`)** — the trie carries no size, so the
    ///     window is bounded by NEXT-PARTITION-BOUNDARY detection. After each
    ///     appended chunk we re-parse from scratch (deterministic, re-collected
    ///     each attempt, so no duplication) and ask `bti_collect_partition_rows`
    ///     whether it reached a DEFINITIVE boundary: either a row with a different
    ///     partition key whose header is FULLY present (a real next partition, not
    ///     a truncated tail), or a clean end-of-data within the buffered bytes. If
    ///     the parse ended ambiguously (buffer truncated mid-row/mid-header, so a
    ///     boundary cannot be told from truncation) we pull ONE more chunk and
    ///     retry; we stop at the first definitive boundary OR EOF — never reading
    ///     past the partition we need. This avoids the truncation bug (a row cut at
    ///     the buffer tail tripping a bogus boundary) WITHOUT reading to EOF.
    ///
    /// In both cases a row with a different partition key terminates collection,
    /// and the whole-section fallback (uncompressed BTI) already has every byte so
    /// its first parse is authoritative. This yields byte-for-byte the same rows as
    /// the full-scan path filtered down to `partition_key`.
    ///
    /// Returns the partition's rows as `Vec<Value>` (empty when the trie/index
    /// candidate was a prefix collision for an absent key); the caller wraps each
    /// in a `(RowKey, Value)` keyed by the queried partition key and applies the
    /// same tombstone suppression the scan path applies.
    #[cfg(not(feature = "tombstones"))]
    async fn bti_decompress_and_parse_target_all(
        &self,
        offset: usize,
        size_hint: Option<usize>,
        key: &RowKey,
        table_id: &TableId,
        schema_opt: Option<&crate::schema::TableSchema>,
        parser: &crate::storage::sstable::reader::parsing::V5CompressedLegacyParser,
    ) -> Result<Vec<Value>> {
        // Issue #815: each lookup uses its own cursor so concurrent lookups on
        // this reader never share a mutable file position / chunk index.
        let cursor = self.new_scan_cursor().await?;

        // Determine the chunk-targeting parameters. `chunk_length == 0` (or no
        // CompressionInfo) means we cannot chunk-target -> whole-section fallback.
        let chunk_length = self
            .compression_info
            .as_ref()
            .map(|ci| ci.chunk_length as usize)
            .filter(|&len| len > 0);

        let (target_chunk, window_base, mut window) = match chunk_length {
            Some(len) => {
                let (target_chunk, window_base, _within) = Self::bti_chunk_target(offset, len);
                let chunk_start = self
                    .compression_info
                    .as_ref()
                    .and_then(|ci| ci.compressed_chunk_offset(target_chunk))
                    .ok_or_else(|| {
                        Error::corruption(format!(
                            "BTI single-partition seek: no compressed offset for target chunk {} \
                             (offset {}, chunk_length {})",
                            target_chunk, offset, len
                        ))
                    })?;
                {
                    let mut file_guard = cursor.file.lock().await;
                    file_guard.seek(SeekFrom::Start(chunk_start)).await?;
                }
                cursor
                    .chunk_index
                    .store(target_chunk, std::sync::atomic::Ordering::Relaxed);
                (target_chunk, window_base, Vec::<u8>::new())
            }
            None => {
                // Whole-section fallback (uncompressed BTI, or chunk_length absent/0).
                let header_size = self.calculate_header_size();
                {
                    let mut file_guard = cursor.file.lock().await;
                    file_guard.seek(SeekFrom::Start(header_size as u64)).await?;
                }
                let whole = self.stitch_all_chunks(&cursor).await?;
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

        // Step 1 (chunk-targeted only): buffer enough chunks to expose the
        // partition header, then run the prefix-collision / chunk-straddle gate.
        // This bails out cheaply (without decompressing the rest of the file) when
        // the trie/index candidate is a prefix collision for an absent key.
        if chunk_targeted {
            loop {
                // Pull a chunk if the header is not yet (fully) buffered.
                if within + 2 > window.len()
                    || !Self::bti_partition_key_bytes_available(&window, within, key.as_bytes())
                {
                    match self
                        .bti_pull_decompressed_chunk(&cursor, &mut window)
                        .await?
                    {
                        true => continue, // chunk appended; re-check the header
                        false => {
                            // EOF before the header is buffered: nothing decodable
                            // at the resolved offset.
                            return Ok(Vec::new());
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
                    return Ok(Vec::new());
                }
                break; // header buffered AND key matches
            }

            // Step 2: buffer ONLY the chunks covering the target partition — never
            // stitch to EOF (the #953 MEDIUM finding: a head-of-file lookup would
            // otherwise decompress the whole file).
            //
            // Authoritative-size fast path: when the index supplies a NON-ZERO
            // partition byte length, the partition occupies
            // window[within .. within+size]; buffer exactly that range (or EOF — a
            // stale size never reads past EOF) and parse once. NOTE: the BIG (`nb`)
            // `Index.db` parser does NOT store this size (it is left 0 — see
            // `parse_big_index_entry`), so in practice `size_hint` is `Some(0)`
            // there and we fall through to boundary detection below. The branch is
            // kept (gated on `size > 0`) so that if a future index populates the
            // size we use the exact bound without code change. Using a 0 size as a
            // bound would truncate the window to the partition start and drop every
            // row, so the `> 0` guard is load-bearing, not cosmetic.
            if let Some(size) = size_hint.filter(|&s| s > 0) {
                let needed = within.saturating_add(size);
                while window.len() < needed {
                    if !self
                        .bti_pull_decompressed_chunk(&cursor, &mut window)
                        .await?
                    {
                        break; // EOF: window holds all available bytes.
                    }
                }
                return self
                    .bti_collect_partition_rows(&window, within, key, table_id, schema_opt, parser)
                    .map(|(rows, _complete)| rows);
            }

            // Size-free bound (BTI `da`, and BIG `nb` whose index carries no size):
            // bound by NEXT-PARTITION detection WITH a row-count stability guard
            // that is robust against the truncation bug.
            //
            // The hazard (why a single different-key row is NOT enough): when the
            // buffer truncates mid-row, the parser's row loop fails and the outer
            // loop scans the truncated tail byte-by-byte, which can decode garbage
            // into a bogus "different-key" partition — a FALSE boundary that would
            // drop the rest of the target partition (the bug the old to-EOF stitch
            // worked around by reading everything).
            //
            // The guard: only stop when BOTH hold —
            //   (a) a different-key row was emitted (a candidate next-partition
            //       boundary), AND
            //   (b) the count of TARGET-key rows did NOT grow when the latest chunk
            //       was appended (the partition stopped growing).
            // A stable count means the newly added chunk contributed no target-key
            // row, so every target row is already buffered: stopping cannot lose a
            // row. Truncation garbage fails this guard — while the target partition
            // is still being read, each appended chunk adds more target rows, so the
            // count keeps growing and the bogus boundary is rejected until the real
            // end (or EOF) is reached. Re-parsing from scratch each iteration is
            // deterministic (rows re-collected), so no duplication.
            let mut prev_target_rows: Option<usize> = None;
            loop {
                let (rows, saw_next_partition) = self.bti_collect_partition_rows(
                    &window, within, key, table_id, schema_opt, parser,
                )?;
                let count_stable = prev_target_rows == Some(rows.len());
                if saw_next_partition && count_stable {
                    // Real boundary: a next partition is present AND the target
                    // partition stopped growing — every target row is buffered.
                    return Ok(rows);
                }
                prev_target_rows = Some(rows.len());

                // Not yet definitively complete: pull one more chunk and re-parse.
                if !self
                    .bti_pull_decompressed_chunk(&cursor, &mut window)
                    .await?
                {
                    // EOF: the whole tail is buffered, so the collected rows are the
                    // complete (last) partition regardless of the boundary guard.
                    return Ok(rows);
                }
            }
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
        self.bti_collect_partition_rows(&window, within, key, table_id, schema_opt, parser)
            .map(|(rows, _complete)| rows)
    }

    /// Read the next compressed chunk from `cursor`, decompress it (if the reader
    /// has a compression algorithm), and append the decompressed bytes to
    /// `window`. Returns `true` when a chunk was appended, `false` at EOF.
    ///
    /// Shared by the chunk-targeted seek so the header-buffering and
    /// partition-bounding loops use one decompression code path; each call bumps
    /// `work_counters::chunks_decompressed` so a test can prove the seek bounded
    /// its decompression to the target partition's chunk span (Issue #953/#951).
    #[cfg(not(feature = "tombstones"))]
    async fn bti_pull_decompressed_chunk(
        &self,
        cursor: &ScanCursor,
        window: &mut Vec<u8>,
    ) -> Result<bool> {
        use crate::storage::sstable::compression::Compression;
        match self.read_next_block(cursor).await? {
            Some(compressed_chunk) => {
                let decompressed_chunk = if let Some(compression_reader) = &self.compression_reader
                {
                    let compression = Compression::new(*compression_reader.algorithm())?;
                    compression.decompress(&compressed_chunk).map_err(|e| {
                        Error::corruption(format!(
                            "BTI single-partition seek: failed to decompress chunk: {}",
                            e
                        ))
                    })?
                } else {
                    // No compression reader despite CompressionInfo: treat the raw
                    // chunk bytes as already-decompressed data.
                    compressed_chunk
                };
                // Issue #953/#951: count every chunk the seek materializes so a
                // bound test can prove the decompression window is bounded to the
                // target partition's chunk span, not stitched to EOF.
                super::super::work_counters::add_chunk_decompressed();
                window.extend_from_slice(&decompressed_chunk);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Parse the buffered `window` from `within`, collecting every row of the
    /// FIRST (target) partition and stopping at the next partition boundary.
    ///
    /// Returns `(rows, complete)`:
    /// - `rows` — the target partition's row `Value`s (those whose decoded key
    ///   equals `key` and whose table id matches, issue #831 wrong-table guard),
    ///   in on-disk order.
    /// - `complete` — `true` iff a DEFINITIVE next-partition boundary was reached:
    ///   the parser emitted a fully-decoded row whose partition key DIFFERS from
    ///   `key`. Because `parse_block_emit` only emits a row after decoding its
    ///   entire header + cells, a different-key emission proves the next
    ///   partition's header is FULLY present in the buffer — a real boundary, not a
    ///   tail truncation. `complete == false` means the parse consumed the buffer
    ///   without seeing a different-key row, which is AMBIGUOUS: either the target
    ///   is the last partition in the buffer (truly complete once EOF is reached)
    ///   or the buffer truncated mid-partition. The caller resolves the ambiguity
    ///   by pulling another chunk and re-parsing, or by treating EOF as complete.
    ///
    /// This is the bounding primitive for the BTI (`da`) seek, which has no
    /// authoritative partition size: it lets the caller stop the moment the next
    /// partition appears instead of stitching to EOF (Issue #953). The BIG (`nb`)
    /// and whole-section callers already have a complete window and ignore the
    /// flag.
    #[cfg(not(feature = "tombstones"))]
    fn bti_collect_partition_rows(
        &self,
        window: &[u8],
        within: usize,
        key: &RowKey,
        table_id: &TableId,
        schema_opt: Option<&crate::schema::TableSchema>,
        parser: &crate::storage::sstable::reader::parsing::V5CompressedLegacyParser,
    ) -> Result<(Vec<Value>, bool)> {
        let mut rows: Vec<Value> = Vec::new();
        let mut saw_next_partition = false;
        parser.parse_block_emit(
            &window[within..],
            schema_opt,
            self,
            |(tid, entry_key, entry_value)| {
                if entry_key.as_bytes() == key.as_bytes() {
                    // A row of the TARGET partition. Verify the table id matches
                    // (a wrong-table query never returns a row, issue #831).
                    if table_ids_match_strict(&tid, table_id) {
                        rows.push(entry_value);
                    }
                    Ok(std::ops::ControlFlow::Continue(()))
                } else {
                    // First row of the NEXT partition — fully decoded, so its
                    // header is entirely present (a real boundary, not a truncation
                    // artifact). The target partition is definitively complete.
                    saw_next_partition = true;
                    Ok(std::ops::ControlFlow::Break(()))
                }
            },
        )?;
        Ok((rows, saw_next_partition))
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

    /// BTI ("da") full scan: decompress the whole Data.db section and parse
    /// every partition in token order (issue #660).
    ///
    /// BTI SSTables carry no Index.db/Summary.db, so a range/full scan cannot
    /// use the index path. Instead we stitch the entire (chunk-compressed) data
    /// section into one buffer and run [`parse_block_with_cell_metadata`], which
    /// walks ALL partitions — the same per-partition decode the point-lookup
    /// path uses, but without stopping at the first match.
    ///
    /// Returns entries with per-cell write metadata so the WRITETIME/TTL scan
    /// (`scan_with_cell_metadata`) and the plain `scan` (which drops the metadata)
    /// can share a single implementation. Results are filtered by the optional
    /// `[start_key, end_key]` range and tombstone-suppressed, then sorted into
    /// Murmur3 token order and truncated to `limit` — identical post-processing
    /// to the V5CompressedLegacy stitched path.
    ///
    /// Uses its own per-scan [`ScanCursor`], so it runs in parallel with other
    /// scans on this reader without serialization (issue #815).
    ///
    /// [`parse_block_with_cell_metadata`]: crate::storage::sstable::reader::parsing::V5CompressedLegacyParser::parse_block_with_cell_metadata
    async fn bti_scan_with_metadata(
        &self,
        start_key: Option<&RowKey>,
        end_key: Option<&RowKey>,
        limit: Option<usize>,
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<
        Vec<(
            RowKey,
            Value,
            std::collections::HashMap<String, CellWriteMetadata>,
        )>,
    > {
        let cursor = self.new_scan_cursor().await?;

        // Decompress the entire data section. Precondition for stitch_all_chunks:
        // cursor's file seeked to data-section start (fresh cursor is at chunk 0).
        let header_size = self.calculate_header_size();
        {
            let mut file_guard = cursor.file.lock().await;
            file_guard.seek(SeekFrom::Start(header_size as u64)).await?;
        }
        let whole = self.stitch_all_chunks(&cursor).await?;

        // Resolve schema via the four-tier strategy (provided > header > registry).
        // V5CompressedLegacy partition decode requires a schema (cells lack names).
        let effective_schema = self.get_table_schema(schema);
        let parser = self.build_v5_parser();
        let parsed =
            parser.parse_block_with_cell_metadata(&whole, effective_schema.as_ref(), self)?;

        let mut results = Vec::new();
        for (_entry_table_id, entry_key, entry_value, cell_meta) in parsed {
            if let Some(start) = start_key {
                if &entry_key < start {
                    continue;
                }
            }
            if let Some(end) = end_key {
                if &entry_key > end {
                    continue;
                }
            }
            if !self.filter_tombstone(&entry_value) {
                continue;
            }
            results.push((entry_key, entry_value, cell_meta));
        }

        sort_by_token_order_with_meta(&mut results);
        if let Some(lim) = limit {
            results.truncate(lim);
        }

        log::debug!(
            "SSTableReader::bti_scan_with_metadata - Returning {} results",
            results.len()
        );
        Ok(results)
    }

    /// Scan a range of keys
    ///
    /// # Arguments
    /// * `table_id` - The table to scan
    /// * `start_key` - Optional start key for range scan
    /// * `end_key` - Optional end key for range scan
    /// * `limit` - Optional limit on number of results
    /// * `schema` - Optional table schema for schema-aware parsing. When provided,
    ///   enables accurate type detection and avoids heuristic-based parsing.
    ///   Strongly recommended for Cassandra 5.0+ formats.
    pub async fn scan(
        &self,
        table_id: &TableId,
        start_key: Option<&RowKey>,
        end_key: Option<&RowKey>,
        limit: Option<usize>,
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<Vec<(RowKey, Value)>> {
        log::debug!("SSTableReader::scan - Starting scan");
        log::debug!("SSTableReader::scan - File path: {:?}", self.file_path);
        log::debug!("SSTableReader::scan - Table ID: {}", table_id);
        log::debug!("SSTableReader::scan - Start key: {:?}", start_key);
        log::debug!("SSTableReader::scan - End key: {:?}", end_key);
        log::debug!("SSTableReader::scan - Limit: {:?}", limit);
        log::debug!("SSTableReader::scan - Has schema: {}", schema.is_some());
        log::debug!("SSTableReader::scan - Has index: {}", self.index.is_some());
        log::debug!(
            "SSTableReader::scan - Has bloom filter: {}",
            self.bloom_filter.is_some()
        );

        // Issue #660: BTI ("da") readers have no Index.db/Summary.db. A full scan
        // walks the whole (chunk-compressed) Data.db once and parses every
        // partition — the same partition decode the point-lookup path proves
        // correct, but emitting ALL partitions instead of stopping at the first.
        if self.bti_partitions_db.is_some() {
            let entries = self
                .bti_scan_with_metadata(start_key, end_key, limit, schema)
                .await?;
            return Ok(entries.into_iter().map(|(k, v, _meta)| (k, v)).collect());
        }

        let mut results = Vec::new();

        // Use index for efficient range scan if available
        if let Some(index) = &self.index {
            log::debug!("SSTableReader::scan - Using index-based scan");
            let entries = index.get_range(table_id, start_key, end_key)?;
            log::debug!(
                "SSTableReader::scan - Index returned {} entries",
                entries.len()
            );

            // Issue #256 FIX: Fall back to sequential scan when index returns no entries
            //
            // This handles BTI (Big Trie Index) format where parsing may be incomplete or
            // where the index format is not yet fully supported. Without this check, tables
            // using BTI format return 0 rows because:
            // 1. The index exists (so we take the index-based path)
            // 2. But get_range() returns 0 entries (BTI parsing incomplete)
            // 3. The has_zero_size check never triggers (no entries to check)
            // 4. The for loop iterates 0 times, returning empty results
            //
            // Sequential scan correctly parses Data.db directly, bypassing index issues.
            if entries.is_empty() {
                log::debug!(
                    "SSTableReader::scan - Index returned 0 entries (BTI format or incomplete parsing), falling back to sequential scan"
                );
                return self
                    .sequential_scan(table_id, start_key, end_key, limit, schema)
                    .await;
            }

            // Check if any entry has size=0 (Cassandra 5.0 format)
            let has_zero_size = entries.iter().any(|e| e.size == 0);
            if has_zero_size {
                log::debug!("SSTableReader::scan - Index reports size=0 for some entries, using sequential scan fallback");
                return self
                    .sequential_scan(table_id, start_key, end_key, limit, schema)
                    .await;
            }

            // Collect ALL index entries (limit applied after sort — BLOCKING-1).
            for (i, entry) in entries.iter().enumerate() {
                // Index offsets are relative to data section start - adjust for header
                let file_offset = entry.offset + self.actual_header_size as u64;
                log::debug!(
                    "SSTableReader::scan - Processing index entry {}: index_offset={}, file_offset={}, size={}",
                    i, entry.offset, file_offset, entry.size
                );

                if let Some(value) = self.read_value_at_offset(file_offset, entry.size).await? {
                    log::debug!(
                        "SSTableReader::scan - Successfully read value at offset {}",
                        entry.offset
                    );
                    results.push((entry.key.clone(), value));
                } else {
                    log::debug!("SSTableReader::scan - Value at offset {} was filtered out (tombstone or expired)", entry.offset);
                }
            }
        } else {
            // Fallback to sequential scan.  sequential_scan() already returns results in
            // token order (NON-BLOCKING-1: avoid double-sort — return directly).
            log::debug!("SSTableReader::scan - No index, falling back to sequential scan");
            let seq_results = self
                .sequential_scan(table_id, start_key, end_key, limit, schema)
                .await?;
            log::debug!(
                "SSTableReader::scan - Sequential scan returned {} results",
                seq_results.len()
            );
            log::debug!(
                "SSTableReader::scan - Returning {} final results",
                seq_results.len()
            );
            return Ok(seq_results);
        }

        // Index-based path: sort by Murmur3 token order (ascending token, then key bytes).
        // This matches the on-disk physical order (spec §5, Appendix B §313) and the write
        // engine's PartitionPosition::cmp.  Compute each key's token once before sorting to
        // avoid O(n log n) recomputation inside the comparator.
        sort_by_token_order(&mut results);
        // Limit applied AFTER sort so LIMIT N returns the N token-smallest partitions.
        if let Some(lim) = limit {
            results.truncate(lim);
        }

        log::debug!(
            "SSTableReader::scan - Returning {} final results",
            results.len()
        );
        Ok(results)
    }

    /// Get all entries in the SSTable.
    ///
    /// # Tombstone contract (Issue #505)
    ///
    /// This is a **user-facing** accessor: row tombstones are filtered out via
    /// [`Self::filter_tombstone`] and never appear in the returned entries. The
    /// underlying `parse_block` path emits `Value::Tombstone(RowTombstone)` for
    /// deleted rows, but those are suppressed here so callers see exactly the live
    /// rows (matching the previous `Value::Null` suppression behaviour).
    ///
    /// The compaction k-way merger must instead use
    /// [`Self::iterate_all_partitions_for_compaction`], which preserves
    /// `Value::Tombstone` entries (with their authoritative deletion timestamps)
    /// so that tombstone-shadowing semantics can be applied during the merge.
    pub async fn get_all_entries(&self) -> Result<Vec<(TableId, RowKey, Value)>> {
        // Issue #660: BTI ("da") tables have no Index.db; route through the
        // whole-Data.db BTI scan, which resolves schema via get_table_schema
        // (header/registry) and decodes every partition. It mints its own
        // per-scan cursor, as does the non-BTI path below (issue #815).
        if self.bti_partitions_db.is_some() {
            let table_id = TableId::new(format!(
                "{}.{}",
                self.header.keyspace, self.header.table_name
            ));
            let entries = self.bti_scan_with_metadata(None, None, None, None).await?;
            return Ok(entries
                .into_iter()
                .map(|(k, v, _meta)| (table_id.clone(), k, v))
                .collect());
        }

        // Issue #815: independent per-scan cursor — no cross-scan serialization.
        let cursor = self.new_scan_cursor().await?;

        let mut results = Vec::new();

        // Reset to beginning of data section
        let header_size = self.calculate_header_size();
        {
            let mut file_guard = cursor.file.lock().await;
            file_guard.seek(SeekFrom::Start(header_size as u64)).await?;
        }

        if self.requires_chunk_stitching() {
            // V5CompressedLegacy: Row payloads can span multiple compressed chunks
            // We must decompress and stitch all chunks together before parsing
            log::debug!(
                "V5CompressedLegacy format detected, decompressing and stitching all chunks before parsing"
            );

            // Use shared stitching helper method
            let entries = self.stitch_and_parse_all_chunks(&cursor, None).await?;
            results.extend(entries);
        } else {
            // Other formats: Read and parse blocks individually
            while let Some(block) = self.read_next_block(&cursor).await? {
                let entries = self.parse_block_entries(&block, None)?;
                results.extend(entries);
            }
        }

        // Issue #505: suppress row tombstones from user-facing output. The compaction
        // path (iterate_all_partitions_for_compaction) bypasses this filter.
        results.retain(|(_tid, _key, value)| self.filter_tombstone(value));

        Ok(results)
    }

    /// Stitch all compressed chunks and parse as a single buffer (V5CompressedLegacy)
    ///
    /// This helper method extracts the stitching logic from get_all_entries so it can be
    /// reused by sequential_scan and other methods that need to handle V5CompressedLegacy
    /// format where partitions can span chunk boundaries.
    async fn stitch_and_parse_all_chunks(
        &self,
        cursor: &ScanCursor,
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<Vec<(TableId, RowKey, Value)>> {
        let stitched_buffer = self.stitch_all_chunks(cursor).await?;
        let parser = self.build_v5_parser();

        // Get schema (use provided schema or reader's schema)
        let reader_schema;
        let table_schema = if let Some(s) = schema {
            Some(s)
        } else {
            reader_schema = self.get_table_schema(None);
            reader_schema.as_ref()
        };

        // Parse the stitched decompressed buffer
        let entries = parser.parse_block(&stitched_buffer, table_schema, self)?;
        log::debug!(
            "stitch_and_parse_all_chunks: Parsed {} entries from stitched buffer",
            entries.len()
        );

        Ok(entries)
    }

    /// Like [`stitch_and_parse_all_chunks`] but also returns per-cell write metadata.
    ///
    /// Used when `ProjectionFlags::include_cell_metadata` is set (issue #693).
    async fn stitch_and_parse_all_chunks_with_metadata(
        &self,
        cursor: &ScanCursor,
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<
        Vec<(
            TableId,
            RowKey,
            Value,
            std::collections::HashMap<String, CellWriteMetadata>,
        )>,
    > {
        let stitched_buffer = self.stitch_all_chunks(cursor).await?;
        let parser = self.build_v5_parser();

        let reader_schema;
        let table_schema = if let Some(s) = schema {
            Some(s)
        } else {
            reader_schema = self.get_table_schema(None);
            reader_schema.as_ref()
        };

        let entries =
            parser.parse_block_with_cell_metadata(&stitched_buffer, table_schema, self)?;
        log::debug!(
            "stitch_and_parse_all_chunks_with_metadata: Parsed {} entries with metadata",
            entries.len()
        );

        Ok(entries)
    }

    /// Read, decompress, and concatenate every compressed chunk of the data
    /// section into a single buffer.
    ///
    /// V5CompressedLegacy partitions can span chunk boundaries, so the whole
    /// data section must be stitched before parsing. The returned buffer is
    /// bounded by the *uncompressed data-section size* — it scales with on-disk
    /// bytes, not row count (issue #790).
    ///
    /// Precondition: the caller has seeked `cursor`'s file to the start of the
    /// data section (the cursor's chunk index starts at 0 when freshly minted).
    async fn stitch_all_chunks(&self, cursor: &ScanCursor) -> Result<Vec<u8>> {
        use crate::storage::sstable::compression::Compression;

        // Pre-allocate buffer for ~2.5MB (estimated max size for test data)
        let mut stitched_buffer = Vec::with_capacity(2_500_000);

        let mut chunk_count = 0;
        while let Some(compressed_chunk) = self.read_next_block(cursor).await? {
            let decompressed_chunk = if let Some(compression_reader) = &self.compression_reader {
                let compression = Compression::new(*compression_reader.algorithm())?;
                match compression.decompress(&compressed_chunk) {
                    Ok(decompressed) => decompressed,
                    Err(e) => {
                        return Err(Error::corruption(format!(
                            "stitch_all_chunks: Failed to decompress chunk {}: {}",
                            chunk_count, e
                        )));
                    }
                }
            } else {
                // No compression (should not happen for V5CompressedLegacy)
                log::warn!("stitch_all_chunks: No compression reader, using raw chunk data");
                compressed_chunk
            };

            stitched_buffer.extend_from_slice(&decompressed_chunk);
            chunk_count += 1;
        }

        log::debug!(
            "stitch_all_chunks: Stitched {} chunks, total buffer: {} bytes",
            chunk_count,
            stitched_buffer.len()
        );

        Ok(stitched_buffer)
    }

    /// Build a [`V5CompressedLegacyParser`] configured from this reader's header,
    /// statistics (EncodingStats), version gates, and UDT registry.
    ///
    /// [`V5CompressedLegacyParser`]: crate::storage::sstable::reader::parsing::V5CompressedLegacyParser
    fn build_v5_parser(
        &self,
    ) -> crate::storage::sstable::reader::parsing::V5CompressedLegacyParser {
        let keyspace = self.header.keyspace.clone();
        let table_name = self.header.table_name.clone();

        // Extract EncodingStats from statistics_reader (if available)
        let (min_timestamp, min_local_deletion_time, min_ttl) =
            if let Some(stats_reader) = &self.statistics_reader {
                let ts_stats = &stats_reader.statistics().timestamp_stats;
                (
                    ts_stats.min_timestamp,
                    ts_stats.min_deletion_time,
                    ts_stats.min_ttl,
                )
            } else {
                (0, 0, None)
            };

        let parser = crate::storage::sstable::reader::parsing::V5CompressedLegacyParser::new(
            keyspace,
            table_name,
            min_timestamp,
            min_local_deletion_time,
            min_ttl,
        )
        // VG1: thread VersionGates from SSTableReader down to row parser so
        // that VG3 can flip gate-sensitive code paths without re-deriving gates.
        .with_version_gates(self.version_gates.clone());
        // Add UDT registry if available for UDT-aware collection parsing (Issue #238)
        if let Some(ref registry) = self.udt_registry {
            parser.with_udt_registry(registry.clone())
        } else {
            parser
        }
    }

    /// Streaming scan (issue #790): yield `(RowKey, Value)` entries lazily
    /// through a bounded channel instead of materializing the whole result in a
    /// `Vec`. Live heap is bounded by `buffer_size` rows (plus the stitched
    /// data-section buffer) rather than growing O(rows).
    ///
    /// Entries are yielded in on-disk order — token order for a single SSTable —
    /// matching the order of the materializing [`scan`](Self::scan) path. The
    /// bounded channel applies backpressure: parsing pauses when the consumer
    /// falls behind and stops entirely if the consumer is dropped.
    pub fn scan_stream(
        self: std::sync::Arc<Self>,
        table_id: TableId,
        start_key: Option<RowKey>,
        end_key: Option<RowKey>,
        schema: Option<crate::schema::TableSchema>,
        buffer_size: usize,
    ) -> mpsc::Receiver<Result<(RowKey, Value)>> {
        let (tx, rx) = mpsc::channel(buffer_size.max(1));
        tokio::spawn(async move {
            if let Err(e) = self
                .run_scan_stream(table_id, start_key, end_key, schema, tx.clone())
                .await
            {
                // Surface the error to the consumer as a terminal stream item.
                let _ = tx.send(Err(e)).await;
            }
        });
        rx
    }

    async fn run_scan_stream(
        self: std::sync::Arc<Self>,
        table_id: TableId,
        start_key: Option<RowKey>,
        end_key: Option<RowKey>,
        schema: Option<crate::schema::TableSchema>,
        tx: mpsc::Sender<Result<(RowKey, Value)>>,
    ) -> Result<()> {
        // Issue #815: independent per-scan cursor — no cross-scan serialization.
        let cursor = self.new_scan_cursor().await?;

        // Position at the start of the data section (mirrors sequential_scan).
        let header_size = self.calculate_header_size();
        {
            let mut file_guard = cursor.file.lock().await;
            file_guard.seek(SeekFrom::Start(header_size as u64)).await?;
        }

        if self.requires_chunk_stitching() {
            // Stitch the (bounded) data section, then parse on a blocking thread,
            // emitting one entry at a time. `blocking_send` provides backpressure
            // so parsed Values are never all live at once.
            let stitched = self.stitch_all_chunks(&cursor).await?;
            let reader = std::sync::Arc::clone(&self);
            let parse = tokio::task::spawn_blocking(move || {
                reader.parse_stitched_stream(&stitched, schema.as_ref(), start_key, end_key, &tx)
            })
            .await;
            match parse {
                Ok(result) => result,
                Err(join_err) => Err(Error::corruption(format!(
                    "scan_stream: parse task failed: {join_err}"
                ))),
            }
        } else {
            // Non-stitching formats already read block-by-block; emit per block so
            // only one block's entries are live at a time.
            while let Some(block) = self.read_next_block(&cursor).await? {
                let entries = self.parse_block_entries_with_schema(&block, schema.as_ref())?;
                for (entry_table_id, entry_key, entry_value) in entries {
                    if !table_ids_match(&entry_table_id, &table_id) {
                        continue;
                    }
                    if let Some(ref start) = start_key {
                        if &entry_key < start {
                            continue;
                        }
                    }
                    if let Some(ref end) = end_key {
                        if &entry_key > end {
                            continue;
                        }
                    }
                    if !self.filter_tombstone(&entry_value) {
                        continue;
                    }
                    if tx.send(Ok((entry_key, entry_value))).await.is_err() {
                        return Ok(()); // consumer dropped
                    }
                }
            }
            Ok(())
        }
    }

    /// Parse a stitched V5CompressedLegacy buffer, sending each filtered
    /// `(RowKey, Value)` through `tx` with `blocking_send` for backpressure.
    ///
    /// CPU-bound and synchronous: must be invoked via `spawn_blocking`, never on
    /// an async worker thread (`blocking_send` would otherwise stall the runtime).
    fn parse_stitched_stream(
        &self,
        stitched: &[u8],
        schema: Option<&crate::schema::TableSchema>,
        start_key: Option<RowKey>,
        end_key: Option<RowKey>,
        tx: &mpsc::Sender<Result<(RowKey, Value)>>,
    ) -> Result<()> {
        let parser = self.build_v5_parser();
        let reader_schema;
        let table_schema = if let Some(s) = schema {
            Some(s)
        } else {
            reader_schema = self.get_table_schema(None);
            reader_schema.as_ref()
        };

        parser.parse_block_emit(stitched, table_schema, self, |(_table_id, key, value)| {
            // Key-range filter (start/end inclusive), mirroring sequential_scan.
            if let Some(ref start) = start_key {
                if &key < start {
                    return Ok(std::ops::ControlFlow::Continue(()));
                }
            }
            if let Some(ref end) = end_key {
                if &key > end {
                    return Ok(std::ops::ControlFlow::Continue(()));
                }
            }
            // Suppress row tombstones from user-facing scan output (Issue #505).
            if !self.filter_tombstone(&value) {
                return Ok(std::ops::ControlFlow::Continue(()));
            }
            match tx.blocking_send(Ok((key, value))) {
                Ok(()) => Ok(std::ops::ControlFlow::Continue(())),
                Err(_) => Ok(std::ops::ControlFlow::Break(())), // consumer dropped
            }
        })
    }

    /// Stitch all compressed chunks and parse with per-row timestamps (for compaction).
    ///
    /// Identical to [`stitch_and_parse_all_chunks`] but delegates to
    /// [`V5CompressedLegacyParser::parse_block_with_timestamps`] so that each
    /// entry carries its actual row-level write timestamp rather than
    /// `SystemTime::now()`.  Row and cell tombstones are emitted as
    /// `Value::Tombstone` with their authoritative deletion timestamps.
    ///
    /// Used exclusively by the compaction k-way merger path (Issue #505).
    async fn stitch_and_parse_all_chunks_for_compaction(
        &self,
        cursor: &ScanCursor,
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<Vec<super::compaction_row::CompactionRow>> {
        log::debug!("stitch_and_parse_all_chunks_for_compaction: stitching chunks");

        let mut stitched_buffer = Vec::with_capacity(2_500_000);
        let mut chunk_count = 0;

        while let Some(compressed_chunk) = self.read_next_block(cursor).await? {
            use crate::storage::sstable::compression::Compression;
            let decompressed_chunk = if let Some(compression_reader) = &self.compression_reader {
                let compression = Compression::new(*compression_reader.algorithm())?;
                compression.decompress(&compressed_chunk).map_err(|e| {
                    Error::corruption(format!(
                        "stitch_and_parse_all_chunks_for_compaction: Failed to decompress chunk {}: {}",
                        chunk_count, e
                    ))
                })?
            } else {
                compressed_chunk
            };
            stitched_buffer.extend_from_slice(&decompressed_chunk);
            chunk_count += 1;
        }

        log::debug!(
            "stitch_and_parse_all_chunks_for_compaction: {} chunks, {} bytes total",
            chunk_count,
            stitched_buffer.len()
        );

        let keyspace = self.header.keyspace.clone();
        let table_name = self.header.table_name.clone();

        let (min_timestamp, min_local_deletion_time, min_ttl) =
            if let Some(stats_reader) = &self.statistics_reader {
                let ts_stats = &stats_reader.statistics().timestamp_stats;
                (
                    ts_stats.min_timestamp,
                    ts_stats.min_deletion_time,
                    ts_stats.min_ttl,
                )
            } else {
                (0, 0, None)
            };

        let parser = crate::storage::sstable::reader::parsing::V5CompressedLegacyParser::new(
            keyspace,
            table_name,
            min_timestamp,
            min_local_deletion_time,
            min_ttl,
        )
        // VG1: thread VersionGates from SSTableReader down to row parser.
        .with_version_gates(self.version_gates.clone());
        let parser = if let Some(ref registry) = self.udt_registry {
            parser.with_udt_registry(registry.clone())
        } else {
            parser
        };

        let reader_schema;
        let table_schema = if let Some(s) = schema {
            Some(s)
        } else {
            reader_schema = self.get_table_schema(None);
            reader_schema.as_ref()
        };

        let entries = parser.parse_block_for_compaction(&stitched_buffer, table_schema, self)?;
        log::debug!(
            "stitch_and_parse_all_chunks_for_compaction: parsed {} entries",
            entries.len()
        );

        Ok(entries)
    }

    /// Iterate all partitions with per-row timestamps, for use by the compaction merger.
    ///
    /// Returns `(RowKey, Value, row_timestamp_micros)` for every row in the SSTable.
    /// Unlike [`iterate_all_partitions`]:
    ///
    /// - Row tombstones are returned as `Value::Tombstone(RowTombstone)` carrying
    ///   the actual deletion timestamp extracted from the on-disk row header.
    /// - Cell tombstones within live rows are stored as `Value::Tombstone(CellTombstone)`
    ///   inside the `Value::Map`, also carrying the actual cell-level deletion timestamp.
    /// - The third tuple element is the decoded row-level write timestamp, so the
    ///   merger can perform timestamp-accurate last-write-wins comparisons.
    ///
    /// Normal user-facing reads use [`scan`] / [`get`] / [`iterate_all_partitions`],
    /// which apply tombstone filtering.  Do NOT use this method for user-visible queries.
    ///
    /// (Issue #505)
    pub async fn iterate_all_partitions_for_compaction(
        &self,
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<Vec<super::compaction_row::CompactionRow>> {
        // Only the V5CompressedLegacy NB chunk-stitching path is supported here
        // (that is the format the WriteEngine produces).  For other formats, fall
        // back to iterate_all_partitions and attach timestamp 0 as a conservative
        // default (LWW ordering then relies solely on run_index).
        if self.requires_chunk_stitching() {
            // We need schema; retrieve it once.
            // `schema` is Option<&TableSchema>; clone it into an owned value so we
            // can pass it to the async helper without borrow-checker issues.
            let owned_schema = schema.cloned().or_else(|| self.get_table_schema(None));

            // Reset chunk reader to start of data section (own per-scan cursor).
            let cursor = self.new_scan_cursor().await?;
            let header_size = self.calculate_header_size();
            {
                let mut file_guard = cursor.file.lock().await;
                use tokio::io::AsyncSeekExt;
                file_guard
                    .seek(std::io::SeekFrom::Start(header_size as u64))
                    .await?;
            }

            let entries = self
                .stitch_and_parse_all_chunks_for_compaction(&cursor, owned_schema.as_ref())
                .await?;

            return Ok(entries);
        }

        // Non-stitching fallback: use iterate_all_partitions and attach ts=0.
        let entries = self.iterate_all_partitions().await?;
        Ok(entries
            .into_iter()
            .map(|(key, value)| {
                super::compaction_row::CompactionRow::from_legacy_value(key, value, 0)
            })
            .collect())
    }

    /// Streaming compaction read (issue #827): yield `(RowKey, Value, ts)`
    /// entries via `emit` one partition at a time, so peak memory is bounded by
    /// `max_partition_size + one_chunk` rather than by the total input size.
    ///
    /// This is the incremental counterpart of
    /// [`iterate_all_partitions_for_compaction`], which fully materialises the
    /// decompressed data section and parses every entry into a `Vec` before
    /// returning. The k-way merge producer (`merge::producer_thread`) uses this
    /// to forward entries into its bounded channel directly, so a source's
    /// decompressed content is never fully resident.
    ///
    /// ## Sliding-window driver
    ///
    /// The V5CompressedLegacy chunk-stitching path keeps a `window: Vec<u8>` of
    /// decompressed bytes. After appending each decompressed chunk it drains
    /// confirmed partitions via `parse_one_partition_with_timestamps`,
    /// `drain(0..consumed)`-ing the front of the window after every `Emitted`,
    /// and stopping at `NeedMore` to await the next chunk (a partition can
    /// straddle a chunk boundary). At EOF a final drain pass runs with
    /// `at_final_chunk = true` so the trailing (possibly truncated) partition is
    /// terminal rather than requesting a refill that will never come.
    ///
    /// Returning `ControlFlow::Break` from `emit` stops the scan early
    /// (consumer dropped). Tombstone / timestamp semantics are byte-identical to
    /// the Vec variant (Issue #505/#533).
    pub async fn stream_all_partitions_for_compaction<F>(
        &self,
        schema: Option<&crate::schema::TableSchema>,
        mut emit: F,
    ) -> Result<()>
    where
        F: FnMut(super::compaction_row::CompactionRow) -> Result<std::ops::ControlFlow<()>>,
    {
        // Reset chunk reader to the start of the data section (mirrors
        // iterate_all_partitions_for_compaction) using an own per-scan cursor.
        let cursor = self.new_scan_cursor().await?;
        let header_size = self.calculate_header_size();
        {
            let mut file_guard = cursor.file.lock().await;
            file_guard.seek(SeekFrom::Start(header_size as u64)).await?;
        }

        // Non-stitching formats are single-block / small: emit via the
        // materialising iterator with ts=0 (matches the Vec-variant fallback).
        if !self.requires_chunk_stitching() {
            let entries = self.iterate_all_partitions().await?;
            for (key, value) in entries {
                let row = super::compaction_row::CompactionRow::from_legacy_value(key, value, 0);
                match emit(row)? {
                    std::ops::ControlFlow::Continue(()) => {}
                    std::ops::ControlFlow::Break(()) => return Ok(()),
                }
            }
            return Ok(());
        }

        // Resolve the schema the parser needs (cells lack column names on disk).
        let owned_schema = schema.cloned().or_else(|| self.get_table_schema(None));
        let parser = self.build_v5_parser();

        let mut window: Vec<u8> = Vec::new();
        let mut broke = false;

        use crate::storage::sstable::compression::Compression;
        let mut chunk_count = 0;
        while let Some(compressed_chunk) = self.read_next_block(&cursor).await? {
            let decompressed_chunk = if let Some(compression_reader) = &self.compression_reader {
                let compression = Compression::new(*compression_reader.algorithm())?;
                compression.decompress(&compressed_chunk).map_err(|e| {
                    Error::corruption(format!(
                        "stream_all_partitions_for_compaction: Failed to decompress chunk {}: {}",
                        chunk_count, e
                    ))
                })?
            } else {
                compressed_chunk
            };
            window.extend_from_slice(&decompressed_chunk);
            chunk_count += 1;

            // Not the final chunk yet: NeedMore means "await more bytes". Drain
            // every confirmed partition from the front of the window.
            self.drain_compaction_window(
                &parser,
                owned_schema.as_ref(),
                &mut window,
                false,
                &mut emit,
                &mut broke,
            )?;
            if broke {
                return Ok(());
            }
        }

        // EOF: final drain — a truncated/unterminated trailing partition is now
        // terminal (Done), not a refill request.
        if !broke {
            self.drain_compaction_window(
                &parser,
                owned_schema.as_ref(),
                &mut window,
                true,
                &mut emit,
                &mut broke,
            )?;
        }

        log::debug!(
            "stream_all_partitions_for_compaction: drained {} chunks (final window {} bytes)",
            chunk_count,
            window.len()
        );

        Ok(())
    }

    /// Drain every confirmed partition from the front of the sliding `window`,
    /// emitting each row via `emit` (issue #827). After each `Emitted` the
    /// consumed prefix is removed so the window's peak size stays bounded by
    /// `max_partition_size + one_chunk`. Stops at `NeedMore` / `Done` (await the
    /// next chunk / genuine end) or when `emit` returns `Break` (sets `*broke`).
    fn drain_compaction_window<F>(
        &self,
        parser: &crate::storage::sstable::reader::parsing::V5CompressedLegacyParser,
        schema: Option<&crate::schema::TableSchema>,
        window: &mut Vec<u8>,
        at_final_chunk: bool,
        emit: &mut F,
        broke: &mut bool,
    ) -> Result<()>
    where
        F: FnMut(super::compaction_row::CompactionRow) -> Result<std::ops::ControlFlow<()>>,
    {
        use crate::storage::sstable::reader::parsing::ParseStep;
        loop {
            if *broke || window.is_empty() {
                return Ok(());
            }
            let mut local_break = false;
            let step = parser.parse_one_partition_for_compaction(
                window.as_slice(),
                schema,
                self,
                at_final_chunk,
                &mut |row: super::compaction_row::CompactionRow| match emit(row)? {
                    std::ops::ControlFlow::Continue(()) => Ok(std::ops::ControlFlow::Continue(())),
                    std::ops::ControlFlow::Break(()) => {
                        local_break = true;
                        Ok(std::ops::ControlFlow::Break(()))
                    }
                },
            )?;
            match step {
                ParseStep::Emitted(consumed) => {
                    let take = if consumed == 0 { 1 } else { consumed };
                    window.drain(0..take.min(window.len()));
                    if local_break {
                        *broke = true;
                        return Ok(());
                    }
                }
                ParseStep::NeedMore | ParseStep::Done => return Ok(()),
            }
        }
    }

    /// Read value at a specific offset with caching
    pub async fn read_value_at_offset(&self, offset: u64, size: u32) -> Result<Option<Value>> {
        use crate::parser::header::CassandraVersion;
        use crate::storage::sstable::compression::Compression;

        // Size must be non-zero for offset-based reading
        if size == 0 {
            return Err(Error::corruption(format!(
                "Cannot read value at offset {} with size=0. This should have been caught earlier and handled via sequential scan.",
                offset
            )));
        }

        // Use cached reading with metrics tracking
        let buffer = self.get_cached_data(offset, size).await?;

        // Decompress if needed
        let data = if let Some(compression_reader) = &self.compression_reader {
            let compression = Compression::new(*compression_reader.algorithm())?;
            match compression.decompress(&buffer) {
                Ok(decompressed) => {
                    debug!(
                        "Successfully decompressed {} bytes to {} bytes",
                        buffer.len(),
                        decompressed.len()
                    );
                    decompressed
                }
                Err(e) => {
                    // For modern formats (4.x/5.x), decompression failure is an error
                    if self.header.cassandra_version != CassandraVersion::Legacy {
                        return Err(Error::corruption(format!(
                            "Decompression failed for modern format at offset={}, size={}, algorithm={:?}: {}",
                            offset,
                            size,
                            compression_reader.algorithm(),
                            e
                        )));
                    } else {
                        // Only allow fallback for legacy formats
                        warn!(
                            "Decompression failed for legacy format ({}), using raw data",
                            e
                        );
                        debug!(
                            "First 32 bytes of raw data: {:02x?}",
                            &buffer[..std::cmp::min(32, buffer.len())]
                        );
                        buffer
                    }
                }
            }
        } else {
            buffer
        };

        // TODO: Parse value using schema-driven type information
        // For now, preserve raw data until schema is available
        let value = Value::Blob(data.to_vec());

        // Extract write time from value (placeholder - would need to be parsed from SSTable)
        let _write_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or_else(|e| {
                warn!("Failed to get system time: {}; using fallback value 0", e);
                0
            });

        // Filter out tombstones and expired data
        if !self.filter_tombstone(&value) {
            return Ok(None);
        }

        Ok(Some(value))
    }

    /// Read block with caching support and hit/miss tracking
    async fn get_cached_data(&self, block_offset: u64, size: u32) -> Result<Vec<u8>> {
        use crate::parser::header::CassandraVersion;
        use crate::storage::sstable::compression::Compression;
        use tokio::io::AsyncReadExt;

        // Calculate block identifier based on offset and size
        let _block_id = block_offset;

        // For now, always read from disk and track as cache miss
        self.record_cache_miss();

        // Read from disk
        let mut file = self.file.lock().await;
        file.seek(SeekFrom::Start(block_offset)).await?;

        let mut buffer = vec![0u8; size as usize];
        file.read_exact(&mut buffer).await?;
        drop(file); // Release file lock early

        // Decompress if needed
        let data = if let Some(compression_reader) = &self.compression_reader {
            let compression = Compression::new(*compression_reader.algorithm())?;
            match compression.decompress(&buffer) {
                Ok(decompressed) => decompressed,
                Err(e) => {
                    // Handle decompression errors based on format
                    if self.header.cassandra_version != CassandraVersion::Legacy {
                        return Err(Error::corruption(format!(
                            "Decompression failed at offset={}, size={}: {}",
                            block_offset, size, e
                        )));
                    } else {
                        buffer // Fall back to raw data for legacy formats
                    }
                }
            }
        } else {
            buffer
        };

        Ok(data)
    }

    async fn scan_for_key(&self, table_id: &TableId, key: &RowKey) -> Result<Option<Value>> {
        // Issue #831: record the call so tests can assert the BTI point-lookup
        // path never reaches the sequential scan.
        SCAN_FOR_KEY_CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Issue #815: independent per-scan cursor — no cross-scan serialization.
        let cursor = self.new_scan_cursor().await?;

        let header_size = self.calculate_header_size();

        // For V5CompressedLegacy NB format, partitions can span chunk boundaries.
        // The block-by-block parser will miss any partition whose bytes cross a
        // chunk boundary.  Use the same stitched-buffer path that sequential_scan()
        // uses so that get() and scan() share a consistent view of the data.
        // (Issue #517)
        if self.requires_chunk_stitching() {
            log::debug!(
                "scan_for_key: V5CompressedLegacy NB detected, using stitched buffer for key lookup"
            );
            // `stitch_all_chunks` reads from the CURRENT cursor position forward,
            // so its precondition is "seeked to the data-section start" (the fresh
            // cursor's chunk index already starts at 0). Each call uses its own
            // cursor (issue #815), so there is no cross-call position to reset.
            {
                let mut file_guard = cursor.file.lock().await;
                file_guard.seek(SeekFrom::Start(header_size as u64)).await?;
            }

            // Pass the reader's own schema so that V5CompressedLegacy rows can be fully
            // parsed and their partition RowKeys emitted.  Without a schema, parse_row_v5
            // fails for all rows in a partition, causing no entries to be pushed and making
            // the key comparison always miss even when the key exists.
            let schema_opt = self.get_table_schema(None);
            let all_entries = match self
                .stitch_and_parse_all_chunks(&cursor, schema_opt.as_ref())
                .await
            {
                Ok(entries) => entries,
                Err(e) => {
                    // Schema may not be available for this reader (e.g., wrong table type).
                    // Return None so the caller can try the next reader.
                    log::debug!(
                        "scan_for_key: stitch_and_parse_all_chunks failed (schema missing?): {}",
                        e
                    );
                    return Ok(None);
                }
            };

            // NOTE: The SSTableIndex is built from 16-byte Murmur3 *digests*, not raw keys,
            // so find_entry() always misses and falls through to this path.  For a found key
            // we stop early (O(found position)); for a key not present we must scan the whole
            // stitched buffer — O(file size).  This O(file) miss cost is an existing
            // limitation of the digest-index design and is tracked separately as a follow-up.
            //
            // NON-BLOCKING-2: Table-id matching is intentionally skipped in the stitching path
            // (consistent with sequential_scan's stitching path).  The V5CompressedLegacy parser
            // returns entries tagged with the table_id from the SSTable header, which may hold
            // default or incorrect values when headers use bare keyspace/table names rather than
            // the query's fully-qualified form.  Since all entries in this stitch buffer come from
            // the single SSTable being queried, skipping the check is correct and safe.
            for (_, entry_key, entry_value) in all_entries {
                if entry_key == *key {
                    // Early-return on first match (BLOCKING-2: don't parse the rest of the file).
                    if !self.filter_tombstone(&entry_value) {
                        return Ok(None);
                    }
                    return Ok(Some(entry_value));
                }
            }

            return Ok(None);
        }

        {
            let mut file_guard = cursor.file.lock().await;
            file_guard.seek(SeekFrom::Start(header_size as u64)).await?;
        }

        // Sequential scan through blocks
        while let Some(block) = self.read_next_block(&cursor).await? {
            let entries = self.parse_block_entries(&block, None)?;

            for (entry_table_id, entry_key, entry_value) in entries {
                if table_ids_match(&entry_table_id, table_id) && entry_key == *key {
                    // Extract write time from entry metadata
                    let _write_time = self.extract_write_time_from_entry(&entry_key, &entry_value);

                    // Filter out tombstones and expired data
                    if !self.filter_tombstone(&entry_value) {
                        return Ok(None);
                    }

                    return Ok(Some(entry_value));
                }
            }
        }

        Ok(None)
    }

    pub(super) async fn sequential_scan(
        &self,
        table_id: &TableId,
        start_key: Option<&RowKey>,
        end_key: Option<&RowKey>,
        limit: Option<usize>,
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<Vec<(RowKey, Value)>> {
        log::debug!("SSTableReader::sequential_scan - Starting sequential scan");
        log::debug!("SSTableReader::sequential_scan - Table ID: {}", table_id);
        log::debug!(
            "SSTableReader::sequential_scan - Has schema: {}",
            schema.is_some()
        );

        // Issue #815: each scan uses its own cursor (private file position and
        // chunk index), so concurrent scans on this reader run in parallel
        // without the per-scan serialization #805 introduced for correctness.
        let cursor = self.new_scan_cursor().await?;

        let mut results = Vec::new();

        let header_size = self.calculate_header_size();
        log::debug!(
            "SSTableReader::sequential_scan - Header size: {} bytes",
            header_size
        );

        {
            let mut file_guard = cursor.file.lock().await;
            file_guard.seek(SeekFrom::Start(header_size as u64)).await?;
            log::debug!(
                "SSTableReader::sequential_scan - Seeked to start of data section at offset {}",
                header_size
            );
        }

        // CRITICAL FIX: V5CompressedLegacy partitions can span chunk boundaries.
        // We must stitch all chunks together before parsing to avoid dropping partitions.
        // Use `requires_chunk_stitching()` as the single source of truth for whether
        // stitching is needed (BLOCKING-3: unified predicate).
        //
        // Note: We intentionally skip table_id matching in the stitching path because the
        // parser may return incorrect table_ids from header defaults.  Since sequential_scan
        // is called with a specific table_id, all entries from this SSTable match it.
        if self.requires_chunk_stitching() {
            log::debug!(
                "SSTableReader::sequential_scan - V5CompressedLegacy NB detected, using stitched buffer"
            );

            // Stitch all chunks together (reuse logic from get_all_entries)
            let all_entries = self.stitch_and_parse_all_chunks(&cursor, schema).await?;
            log::debug!(
                "SSTableReader::sequential_scan - Stitched parsing returned {} total entries",
                all_entries.len()
            );

            // Apply key-range filter and tombstone filter; collect ALL matching entries
            // before sorting.  Limit is applied AFTER sort so that LIMIT N returns the N
            // token-smallest partitions, not the first N encountered in parse order.
            // (BLOCKING-1: limit-after-order)
            for (_entry_table_id, entry_key, entry_value) in all_entries {
                if let Some(start) = start_key {
                    if &entry_key < start {
                        continue;
                    }
                }

                if let Some(end) = end_key {
                    if &entry_key > end {
                        continue;
                    }
                }

                if !self.filter_tombstone(&entry_value) {
                    continue;
                }

                results.push((entry_key, entry_value));
            }

            log::debug!(
                "SSTableReader::sequential_scan - Filtered to {} results before limit (limit: {:?})",
                results.len(),
                limit
            );

            // Sort by Murmur3 token order (spec §5, Appendix B §313), then truncate to limit.
            sort_by_token_order(&mut results);
            if let Some(lim) = limit {
                results.truncate(lim);
            }

            log::debug!(
                "SSTableReader::sequential_scan - Returning {} results after sort+limit",
                results.len()
            );
            return Ok(results);
        }

        // Non-stitching path for other formats
        let mut block_count = 0;
        while let Some(block) = self.read_next_block(&cursor).await? {
            block_count += 1;
            log::debug!(
                "SSTableReader::sequential_scan - Read block {}, size {} bytes",
                block_count,
                block.len()
            );

            let entries = self.parse_block_entries_with_schema(&block, schema)?;
            log::debug!(
                "SSTableReader::sequential_scan - Block {} contains {} entries",
                block_count,
                entries.len()
            );

            for (i, (entry_table_id, entry_key, entry_value)) in entries.iter().enumerate() {
                log::debug!(
                    "SSTableReader::sequential_scan - Block {} entry {}: table_id='{}', key={:?}",
                    block_count,
                    i,
                    entry_table_id,
                    entry_key
                );

                // Match table IDs - supports both qualified (keyspace.table) and unqualified (table) formats
                // This allows queries with either format to match SSTables stored with either format
                if !table_ids_match(entry_table_id, table_id) {
                    log::debug!("SSTableReader::sequential_scan - Skipping entry: table_id mismatch ('{}' != '{}')",
                              entry_table_id, table_id);
                    continue;
                }

                // Check key range
                if let Some(start) = start_key {
                    if entry_key < start {
                        log::debug!(
                            "SSTableReader::sequential_scan - Skipping entry: key < start_key"
                        );
                        continue;
                    }
                }

                if let Some(end) = end_key {
                    if entry_key > end {
                        log::debug!(
                            "SSTableReader::sequential_scan - Skipping entry: key > end_key"
                        );
                        continue;
                    }
                }

                // Extract write time from entry metadata
                let _write_time = self.extract_write_time_from_entry(entry_key, entry_value);

                // Filter out tombstones and expired data
                if !self.filter_tombstone(entry_value) {
                    log::debug!("SSTableReader::sequential_scan - Skipping entry: filtered out (tombstone or expired)");
                    continue;
                }

                log::debug!("SSTableReader::sequential_scan - Including entry in results");
                results.push((entry_key.clone(), entry_value.clone()));
            }
        }

        log::debug!(
            "SSTableReader::sequential_scan - Finished scanning {} blocks",
            block_count
        );
        log::debug!(
            "SSTableReader::sequential_scan - {} results before sort+limit",
            results.len()
        );

        // Sort by Murmur3 token order (spec §5, Appendix B §313), then apply limit.
        // Limit is applied AFTER sort so that LIMIT N returns the N token-smallest
        // partitions (BLOCKING-1: limit-after-order).
        sort_by_token_order(&mut results);
        if let Some(lim) = limit {
            results.truncate(lim);
        }

        log::debug!(
            "SSTableReader::sequential_scan - Returning {} results after sort+limit",
            results.len()
        );
        Ok(results)
    }

    /// Scan a range of keys AND return per-cell write metadata.
    ///
    /// Used when `ProjectionFlags::include_cell_metadata` is set (issue #693).
    /// Falls through to `stitch_and_parse_all_chunks_with_metadata` for
    /// V5CompressedLegacy format (the common path for real SSTables).
    /// Returns `None` as the metadata for non-V5 formats (they do not carry
    /// per-cell timestamps in a way the parser currently surfaces).
    pub async fn scan_with_cell_metadata(
        &self,
        table_id: &TableId,
        start_key: Option<&RowKey>,
        end_key: Option<&RowKey>,
        limit: Option<usize>,
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<
        Vec<(
            RowKey,
            Value,
            std::collections::HashMap<String, CellWriteMetadata>,
        )>,
    > {
        log::debug!("SSTableReader::scan_with_cell_metadata - Starting");

        // Issue #660: BTI ("da") metadata scan — same whole-Data.db walk as the
        // plain BTI scan, but surfaces per-cell write metadata for WRITETIME/TTL.
        if self.bti_partitions_db.is_some() {
            return self
                .bti_scan_with_metadata(start_key, end_key, limit, schema)
                .await;
        }

        // Issue #815: independent per-scan cursor — no cross-scan serialization.
        let cursor = self.new_scan_cursor().await?;

        let header_size = self.calculate_header_size();
        {
            let mut file_guard = cursor.file.lock().await;
            file_guard.seek(SeekFrom::Start(header_size as u64)).await?;
        }

        // V5CompressedLegacy (stitching) path — the common path for Cassandra 5.0 SSTables.
        if self.requires_chunk_stitching() {
            let all_entries = self
                .stitch_and_parse_all_chunks_with_metadata(&cursor, schema)
                .await?;

            let mut results = Vec::new();
            for (_entry_table_id, entry_key, entry_value, cell_meta) in all_entries {
                if let Some(start) = start_key {
                    if &entry_key < start {
                        continue;
                    }
                }
                if let Some(end) = end_key {
                    if &entry_key > end {
                        continue;
                    }
                }
                if !self.filter_tombstone(&entry_value) {
                    continue;
                }
                results.push((entry_key, entry_value, cell_meta));
            }

            sort_by_token_order_with_meta(&mut results);
            if let Some(lim) = limit {
                results.truncate(lim);
            }

            log::debug!(
                "SSTableReader::scan_with_cell_metadata - Returning {} results (stitched path)",
                results.len()
            );
            return Ok(results);
        }

        // Non-stitching path: fall back to regular scan + empty metadata.
        // Per-cell metadata is not yet surfaced for block-entry formats.
        let plain = self
            .sequential_scan(table_id, start_key, end_key, limit, schema)
            .await?;
        Ok(plain
            .into_iter()
            .map(|(k, v)| (k, v, std::collections::HashMap::new()))
            .collect())
    }

    /// Mint a fresh, independent cursor for one scan (issue #815).
    ///
    /// Each cursor owns a private file handle (or mmap cursor) and chunk index,
    /// so concurrent scans on this reader never share a mutable file position —
    /// they run in parallel without the per-scan serialization #805 required.
    pub(super) async fn new_scan_cursor(&self) -> Result<ScanCursor> {
        Ok(ScanCursor::new(
            self.scan_source.open(&self.file_path).await?,
        ))
    }

    /// Read the next block from a scan-local `cursor` (its own file position and
    /// chunk index). See [`Self::new_scan_cursor`].
    pub(super) async fn read_next_block(&self, cursor: &ScanCursor) -> Result<Option<Vec<u8>>> {
        use super::block_io;
        block_io::read_next_block(
            &cursor.file,
            &self.header.cassandra_version,
            &self.config,
            &self.compression_info,
            &cursor.chunk_index,
            self.actual_header_size as u64,
        )
        .await
    }

    /// Prepare for a delta-scan pass: stitch all compressed chunks of the data
    /// section and return the decompressed buffer together with a pre-configured
    /// parser.
    ///
    /// Uses its own per-scan cursor (issue #815), so it no longer needs the
    /// caller to serialize against concurrent reads. This method is gated on the
    /// `delta-scan` feature and is the only bridge between the SSTableReader
    /// internals and the `delta_scan` module, which cannot access private
    /// helpers directly.
    ///
    /// The `schema` parameter is not used here — it is threaded through the
    /// caller's `parse_block_emit_delta` invocation instead.  The parser is
    /// built via `build_v5_parser()` which handles version-gates and UDT
    /// registry without needing the schema at construction time.
    #[cfg(feature = "delta-scan")]
    pub async fn prepare_delta_scan(
        &self,
    ) -> Result<(Vec<u8>, super::parsing::V5CompressedLegacyParser)> {
        use tokio::io::AsyncSeekExt;

        // Seek the per-scan cursor to the start of the data section.
        let cursor = self.new_scan_cursor().await?;
        let header_size = self.calculate_header_size();
        {
            let mut file_guard = cursor.file.lock().await;
            file_guard
                .seek(std::io::SeekFrom::Start(header_size as u64))
                .await?;
        }

        // Stitch all compressed chunks (bounded by uncompressed data-section size).
        let stitched = self.stitch_all_chunks(&cursor).await?;

        // Build a parser (re-using the existing builder so version-gates and
        // UDT registry are threaded through correctly).
        let parser = self.build_v5_parser();

        Ok((stitched, parser))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // table_ids_match tests
    // =========================================================================

    #[test]
    fn test_table_ids_match_strict_keyspace_aware() {
        let a = TableId::new("ks_a.users".to_string());
        let b = TableId::new("ks_b.users".to_string());
        // Both qualified, different keyspace, same table name → must NOT match
        // (the permissive helper would match these).
        assert!(table_ids_match(&a, &b), "permissive helper matches on name");
        assert!(
            !table_ids_match_strict(&a, &b),
            "strict guard must reject a wrong-keyspace same-name query"
        );
        // Both qualified, identical → match.
        let a2 = TableId::new("ks_a.users".to_string());
        assert!(table_ids_match_strict(&a, &a2));
        // One side unqualified → fall back to permissive name match.
        let unq = TableId::new("users".to_string());
        assert!(table_ids_match_strict(&a, &unq));
        assert!(table_ids_match_strict(&unq, &a));
    }

    #[test]
    fn test_bti_lookup_step_decision() {
        // Key prefix buffered and matches → parse.
        assert_eq!(bti_lookup_step(true, true, true), BtiLookupStep::Parse);
        assert_eq!(bti_lookup_step(true, true, false), BtiLookupStep::Parse);
        // Key prefix buffered but does NOT match → absent (prefix collision).
        assert_eq!(bti_lookup_step(true, false, true), BtiLookupStep::Absent);
        assert_eq!(bti_lookup_step(true, false, false), BtiLookupStep::Absent);
        // Key prefix NOT yet buffered (header straddles a chunk boundary):
        //  - chunk-targeted path MUST pull the next chunk, never parse a
        //    truncated header (issue #831 review regression);
        assert_eq!(
            bti_lookup_step(false, false, true),
            BtiLookupStep::PullNextChunk
        );
        //  - whole-section fallback cannot grow → absent.
        assert_eq!(bti_lookup_step(false, false, false), BtiLookupStep::Absent);
    }

    #[test]
    fn test_table_ids_match_exact() {
        // Exact match cases
        let id1 = TableId::new("simple_table".to_string());
        let id2 = TableId::new("simple_table".to_string());
        assert!(table_ids_match(&id1, &id2));

        let id3 = TableId::new("test_basic.simple_table".to_string());
        let id4 = TableId::new("test_basic.simple_table".to_string());
        assert!(table_ids_match(&id3, &id4));
    }

    #[test]
    fn test_table_ids_match_qualified_vs_unqualified() {
        // Qualified matches unqualified
        let qualified = TableId::new("test_basic.simple_table".to_string());
        let unqualified = TableId::new("simple_table".to_string());

        assert!(table_ids_match(&qualified, &unqualified));
        assert!(table_ids_match(&unqualified, &qualified));
    }

    #[test]
    fn test_table_ids_match_different_keyspaces() {
        // Different keyspaces but same table name - should match on table name
        let id1 = TableId::new("keyspace1.users".to_string());
        let id2 = TableId::new("keyspace2.users".to_string());

        assert!(
            table_ids_match(&id1, &id2),
            "Same table name should match across keyspaces"
        );
    }

    #[test]
    fn test_table_ids_match_completely_different() {
        // Completely different tables - should not match
        let id1 = TableId::new("users".to_string());
        let id2 = TableId::new("orders".to_string());

        assert!(!table_ids_match(&id1, &id2));

        let id3 = TableId::new("test.users".to_string());
        let id4 = TableId::new("test.orders".to_string());

        assert!(!table_ids_match(&id3, &id4));
    }

    // =========================================================================
    // Issue #831: BTI chunk-targeting math + window stop-condition logic
    // =========================================================================

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

    #[test]
    fn test_table_ids_match_edge_cases() {
        // Table names with dots (unusual but possible)
        let id1 = TableId::new("schema.table.subtable".to_string());
        let id2 = TableId::new("subtable".to_string());

        assert!(
            table_ids_match(&id1, &id2),
            "Should match on last component"
        );
    }

    #[test]
    fn test_table_ids_match_empty() {
        // Empty table IDs
        let id1 = TableId::new("".to_string());
        let id2 = TableId::new("".to_string());

        assert!(table_ids_match(&id1, &id2), "Empty IDs should match");
    }

    // =========================================================================
    // Key comparison tests
    // =========================================================================

    #[test]
    fn test_row_key_comparison() {
        let key1 = RowKey::new(vec![1, 2, 3]);
        let key2 = RowKey::new(vec![1, 2, 3]);
        let key3 = RowKey::new(vec![1, 2, 4]);

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
        assert!(key1 < key3);
    }

    #[test]
    fn test_row_key_ordering() {
        let key_a = RowKey::new(vec![0x01]);
        let key_b = RowKey::new(vec![0x02]);
        let key_c = RowKey::new(vec![0x01, 0x00]); // Longer but starts with 0x01

        assert!(key_a < key_b);
        assert!(key_a < key_c); // Shorter prefix comes first in lexicographic order
    }

    // =========================================================================
    // Value tests
    // =========================================================================

    #[test]
    fn test_value_blob_creation() {
        let data = vec![1, 2, 3, 4, 5];
        let value = Value::Blob(data.clone());

        if let Value::Blob(v) = value {
            assert_eq!(v, data);
        } else {
            panic!("Expected Value::Blob");
        }
    }

    // =========================================================================
    // Integration tests with real SSTable data
    // =========================================================================

    #[tokio::test]
    async fn test_get_nonexistent_key() {
        use std::path::PathBuf;
        use std::sync::Arc;

        // Test with real SSTable data if available
        let datasets_root = match std::env::var("CQLITE_DATASETS_ROOT") {
            Ok(root) => PathBuf::from(root),
            Err(_) => {
                eprintln!("CQLITE_DATASETS_ROOT not set, skipping test");
                return;
            }
        };

        let simple_table_dir = datasets_root.join("sstables/test_basic");
        if !simple_table_dir.exists() {
            eprintln!("test_basic not found, skipping test");
            return;
        }

        // Find simple_table
        let table_dir = std::fs::read_dir(&simple_table_dir)
            .ok()
            .and_then(|entries| {
                entries
                    .filter_map(|e| e.ok())
                    .find(|e| {
                        e.file_name()
                            .to_str()
                            .map(|n| n.starts_with("simple_table"))
                            .unwrap_or(false)
                    })
                    .map(|e| e.path())
            });

        let Some(table_path) = table_dir else {
            eprintln!("simple_table not found, skipping");
            return;
        };

        // Find Data.db file
        let data_file = std::fs::read_dir(&table_path).ok().and_then(|entries| {
            entries
                .filter_map(|e| e.ok())
                .find(|e| {
                    e.file_name()
                        .to_str()
                        .map(|n| n.ends_with("-Data.db"))
                        .unwrap_or(false)
                })
                .map(|e| e.path())
        });

        let Some(data_path) = data_file else {
            eprintln!("Data.db not found, skipping");
            return;
        };

        let config = crate::Config::default();
        let platform = Arc::new(
            crate::Platform::new(&config)
                .await
                .expect("Failed to create platform"),
        );

        let reader = SSTableReader::open(&data_path, &config, platform)
            .await
            .expect("Failed to open SSTable");

        // Try to get a key that doesn't exist
        let table_id = TableId::new("test_basic.simple_table".to_string());
        let nonexistent_key = RowKey::new(vec![0xFF, 0xFF, 0xFF, 0xFF]); // Very unlikely to exist

        let result = reader.get(&table_id, &nonexistent_key).await;
        assert!(
            result.is_ok(),
            "get() should succeed even for nonexistent key"
        );
        assert!(
            result.unwrap().is_none(),
            "Nonexistent key should return None"
        );
    }

    #[tokio::test]
    async fn test_scan_with_limit() {
        use std::path::PathBuf;
        use std::sync::Arc;

        let datasets_root = match std::env::var("CQLITE_DATASETS_ROOT") {
            Ok(root) => PathBuf::from(root),
            Err(_) => {
                eprintln!("CQLITE_DATASETS_ROOT not set, skipping test");
                return;
            }
        };

        let simple_table_dir = datasets_root.join("sstables/test_basic");
        if !simple_table_dir.exists() {
            eprintln!("test_basic not found, skipping test");
            return;
        }

        // Find simple_table
        let table_dir = std::fs::read_dir(&simple_table_dir)
            .ok()
            .and_then(|entries| {
                entries
                    .filter_map(|e| e.ok())
                    .find(|e| {
                        e.file_name()
                            .to_str()
                            .map(|n| n.starts_with("simple_table"))
                            .unwrap_or(false)
                    })
                    .map(|e| e.path())
            });

        let Some(table_path) = table_dir else {
            eprintln!("simple_table not found, skipping");
            return;
        };

        let data_file = std::fs::read_dir(&table_path).ok().and_then(|entries| {
            entries
                .filter_map(|e| e.ok())
                .find(|e| {
                    e.file_name()
                        .to_str()
                        .map(|n| n.ends_with("-Data.db"))
                        .unwrap_or(false)
                })
                .map(|e| e.path())
        });

        let Some(data_path) = data_file else {
            eprintln!("Data.db not found, skipping");
            return;
        };

        let config = crate::Config::default();
        let platform = Arc::new(
            crate::Platform::new(&config)
                .await
                .expect("Failed to create platform"),
        );

        let reader = SSTableReader::open(&data_path, &config, platform)
            .await
            .expect("Failed to open SSTable");

        let table_id = TableId::new("test_basic.simple_table".to_string());

        // Test scan with limit
        let result = reader.scan(&table_id, None, None, Some(5), None).await;
        assert!(result.is_ok(), "scan() should succeed");

        let entries = result.unwrap();
        assert!(
            entries.len() <= 5,
            "Scan with limit 5 should return at most 5 entries, got {}",
            entries.len()
        );

        eprintln!("Scan with limit 5 returned {} entries", entries.len());
    }

    #[tokio::test]
    async fn test_scan_full_table() {
        use std::path::PathBuf;
        use std::sync::Arc;

        let datasets_root = match std::env::var("CQLITE_DATASETS_ROOT") {
            Ok(root) => PathBuf::from(root),
            Err(_) => {
                eprintln!("CQLITE_DATASETS_ROOT not set, skipping test");
                return;
            }
        };

        let simple_table_dir = datasets_root.join("sstables/test_basic");
        if !simple_table_dir.exists() {
            eprintln!("test_basic not found, skipping test");
            return;
        }

        // Find simple_table
        let table_dir = std::fs::read_dir(&simple_table_dir)
            .ok()
            .and_then(|entries| {
                entries
                    .filter_map(|e| e.ok())
                    .find(|e| {
                        e.file_name()
                            .to_str()
                            .map(|n| n.starts_with("simple_table"))
                            .unwrap_or(false)
                    })
                    .map(|e| e.path())
            });

        let Some(table_path) = table_dir else {
            eprintln!("simple_table not found, skipping");
            return;
        };

        let data_file = std::fs::read_dir(&table_path).ok().and_then(|entries| {
            entries
                .filter_map(|e| e.ok())
                .find(|e| {
                    e.file_name()
                        .to_str()
                        .map(|n| n.ends_with("-Data.db"))
                        .unwrap_or(false)
                })
                .map(|e| e.path())
        });

        let Some(data_path) = data_file else {
            eprintln!("Data.db not found, skipping");
            return;
        };

        let config = crate::Config::default();
        let platform = Arc::new(
            crate::Platform::new(&config)
                .await
                .expect("Failed to create platform"),
        );

        let reader = SSTableReader::open(&data_path, &config, platform)
            .await
            .expect("Failed to open SSTable");

        let table_id = TableId::new("test_basic.simple_table".to_string());

        // Full table scan (no limit)
        let result = reader.scan(&table_id, None, None, None, None).await;
        assert!(result.is_ok(), "Full scan should succeed");

        let entries = result.unwrap();
        eprintln!("Full scan returned {} entries", entries.len());
    }

    #[tokio::test]
    async fn test_get_all_entries() {
        use std::path::PathBuf;
        use std::sync::Arc;

        let datasets_root = match std::env::var("CQLITE_DATASETS_ROOT") {
            Ok(root) => PathBuf::from(root),
            Err(_) => {
                eprintln!("CQLITE_DATASETS_ROOT not set, skipping test");
                return;
            }
        };

        let simple_table_dir = datasets_root.join("sstables/test_basic");
        if !simple_table_dir.exists() {
            eprintln!("test_basic not found, skipping test");
            return;
        }

        // Find simple_table
        let table_dir = std::fs::read_dir(&simple_table_dir)
            .ok()
            .and_then(|entries| {
                entries
                    .filter_map(|e| e.ok())
                    .find(|e| {
                        e.file_name()
                            .to_str()
                            .map(|n| n.starts_with("simple_table"))
                            .unwrap_or(false)
                    })
                    .map(|e| e.path())
            });

        let Some(table_path) = table_dir else {
            eprintln!("simple_table not found, skipping");
            return;
        };

        let data_file = std::fs::read_dir(&table_path).ok().and_then(|entries| {
            entries
                .filter_map(|e| e.ok())
                .find(|e| {
                    e.file_name()
                        .to_str()
                        .map(|n| n.ends_with("-Data.db"))
                        .unwrap_or(false)
                })
                .map(|e| e.path())
        });

        let Some(data_path) = data_file else {
            eprintln!("Data.db not found, skipping");
            return;
        };

        let config = crate::Config::default();
        let platform = Arc::new(
            crate::Platform::new(&config)
                .await
                .expect("Failed to create platform"),
        );

        let reader = SSTableReader::open(&data_path, &config, platform)
            .await
            .expect("Failed to open SSTable");

        // Get all entries (for compaction use case)
        let result = reader.get_all_entries().await;
        assert!(result.is_ok(), "get_all_entries() should succeed");

        let entries = result.unwrap();
        eprintln!("get_all_entries() returned {} entries", entries.len());
    }

    /// Regression test for Issue #480: static cell duplication on read.
    ///
    /// static_columns_table has 100 partitions, each containing one static_block
    /// and one clustering row. CQLite should return exactly 100 result rows — one
    /// per partition — not 200 (which would occur if static rows were emitted as
    /// separate result entries).
    ///
    /// Two bugs were fixed:
    /// 1. Snappy varint collision: bytes `0xC0 0x51` at the start of the Snappy
    ///    stream were misidentified as the V5_0StaticColumns magic number, causing
    ///    the file pointer to advance past part of the compressed data before
    ///    decompression, resulting in "corrupt input" errors.
    /// 2. Static row duplication: static rows were pushed into `results` just like
    ///    clustering rows. They should be accumulated per-partition and merged into
    ///    each subsequent clustering row instead.
    #[tokio::test]
    async fn test_static_columns_table_row_count_issue480() {
        use std::path::PathBuf;
        use std::sync::Arc;

        let datasets_root = match std::env::var("CQLITE_DATASETS_ROOT") {
            Ok(root) => PathBuf::from(root),
            Err(_) => {
                eprintln!("CQLITE_DATASETS_ROOT not set, skipping Issue #480 regression test");
                return;
            }
        };

        let table_base = datasets_root.join("sstables/test_basic");
        if !table_base.exists() {
            eprintln!("test_basic dir not found, skipping Issue #480 regression test");
            return;
        }

        // Locate the static_columns_table directory
        let table_dir = std::fs::read_dir(&table_base).ok().and_then(|entries| {
            entries
                .filter_map(|e| e.ok())
                .find(|e| {
                    e.file_name()
                        .to_str()
                        .map(|n| n.starts_with("static_columns_table"))
                        .unwrap_or(false)
                })
                .map(|e| e.path())
        });

        let Some(table_path) = table_dir else {
            eprintln!("static_columns_table not found, skipping Issue #480 regression test");
            return;
        };

        // Find the Data.db file (must be real binary, not macOS ._resource_fork)
        let data_file = std::fs::read_dir(&table_path).ok().and_then(|entries| {
            entries
                .filter_map(|e| e.ok())
                .find(|e| {
                    let name = e.file_name();
                    let s = name.to_str().unwrap_or("");
                    s.ends_with("-Data.db") && !s.starts_with("._")
                })
                .map(|e| e.path())
        });

        let Some(data_path) = data_file else {
            eprintln!("Data.db not found in static_columns_table dir, skipping");
            return;
        };

        let config = crate::Config::default();
        let platform = Arc::new(
            crate::Platform::new(&config)
                .await
                .expect("Failed to create platform"),
        );

        let reader = SSTableReader::open(&data_path, &config, platform)
            .await
            .expect("Failed to open static_columns_table SSTable");

        let table_id = crate::types::TableId::new("test_basic.static_columns_table".to_string());
        let result = reader.scan(&table_id, None, None, None, None).await;
        assert!(
            result.is_ok(),
            "Scan of static_columns_table should succeed: {:?}",
            result.err()
        );

        let entries = result.unwrap();
        eprintln!(
            "Issue #480 regression: static_columns_table scan returned {} rows",
            entries.len()
        );

        // Expected: 100 rows (one per partition, static data merged into clustering row)
        // Before fix: 0 rows (Snappy decompression failure)
        // After fixing only decompression: 200 rows (static rows emitted separately)
        // After full fix: 100 rows
        assert_eq!(
            entries.len(),
            100,
            "static_columns_table should return 100 rows (one per partition), \
             got {}. Regression for Issue #480: static cell duplication on read.",
            entries.len()
        );
    }
}
