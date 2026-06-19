//! Data access methods for SSTableReader
//!
//! This module contains all methods related to reading data from SSTables,
//! including point lookups, range scans, and sequential access.

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
        // First check bloom filter if available
        if let Some(bloom_filter) = &self.bloom_filter {
            if !bloom_filter.might_contain(key.as_bytes()) {
                return Ok(None);
            }
        }

        // Issue #831: BTI ("da") readers resolve partitions via the Partitions.db
        // trie (O(log n)), never via Index.db (absent for BTI) or the sequential
        // scan. Branch BEFORE the Index.db block so a BTI get() can never fall
        // through to scan_for_key.
        if self.bti_partitions_db.is_some() {
            return self.bti_point_lookup(table_id, key).await;
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
    ///   reader's CompressionInfo + compression_reader (`stitch_all_chunks`).
    /// - **Prefix-collision guard**: the trie may return a candidate for a
    ///   prefix-colliding key, so the decoded partition key is verified to equal
    ///   the queried key before any row is returned.
    async fn bti_point_lookup(&self, _table_id: &TableId, key: &RowKey) -> Result<Option<Value>> {
        // 1. Resolve the uncompressed Data.db offset via the trie.
        let offset = match self.lookup_partition_via_bti_trie(key.as_bytes())? {
            Some(off) => off as usize,
            None => return Ok(None), // not in this SSTable
        };

        // 2. Decompress the data section into a single buffer. The trie offset
        //    indexes into THIS decompressed buffer (INVARIANT 1).
        //
        //    Reuse the shared chunk-stitch helper, which decompresses every chunk
        //    via the reader's compression_reader. Serialise against other scans
        //    (shared file position + chunk index) per issue #805.
        let decompressed = {
            let _scan_guard = self.scan_mutex.lock().await;
            let header_size = self.calculate_header_size();
            {
                let mut file_guard = self.file.lock().await;
                file_guard.seek(SeekFrom::Start(header_size as u64)).await?;
            }
            self.current_chunk_index
                .store(0, std::sync::atomic::Ordering::Relaxed);
            self.stitch_all_chunks().await?
        };

        if offset >= decompressed.len() {
            return Err(Error::corruption(format!(
                "BTI trie resolved Data.db offset {} beyond decompressed data section ({} bytes)",
                offset,
                decompressed.len()
            )));
        }

        // 3. Verify the partition-key bytes at the resolved offset match the
        //    queried key (INVARIANT 3: guard against prefix-collision candidates).
        //
        //    Partition on-disk header (oa/da, hasUIntDeletionTime):
        //      [0]      flags (0x00)
        //      [1]      key_len (u8)
        //      [2..2+L] raw partition-key bytes
        //      [..]     partition deletion time
        if !self.bti_partition_key_matches(&decompressed, offset, key.as_bytes()) {
            debug!(
                "BTI trie candidate at offset {} did not match queried key (prefix collision); \
                 treating as absent",
                offset
            );
            return Ok(None);
        }

        // 4. Decode the partition. Feed the decompressed buffer FROM the resolved
        //    offset to the schema-aware V5 parser, which parses the partition-key
        //    header itself and stops at the next partition boundary / end-of-buffer
        //    (it detects the next partition header structurally and honours the
        //    0x01 end-of-partition marker). Stop after the first emitted entry so
        //    only the target partition is decoded.
        let schema_opt = self.get_table_schema(None);
        let parser = self.build_v5_parser();

        let mut found: Option<Value> = None;
        parser.parse_block_emit(
            &decompressed[offset..],
            schema_opt.as_ref(),
            self,
            |(_tid, entry_key, entry_value)| {
                // The first partition in the slice is the one the trie resolved.
                // Defensively re-check the key (the parser-decoded partition key
                // must equal the queried key).
                if entry_key.as_bytes() == key.as_bytes() {
                    found = Some(entry_value);
                }
                Ok(std::ops::ControlFlow::Break(()))
            },
        )?;

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
        // Issue #805: serialise concurrent scans (shared file position + chunk index).
        let _scan_guard = self.scan_mutex.lock().await;

        let mut results = Vec::new();

        // Reset to beginning of data section
        let header_size = self.calculate_header_size();
        {
            let mut file_guard = self.file.lock().await;
            file_guard.seek(SeekFrom::Start(header_size as u64)).await?;
        }
        // Reset chunk index when seeking to start
        self.current_chunk_index
            .store(0, std::sync::atomic::Ordering::Relaxed);

        if self.requires_chunk_stitching() {
            // V5CompressedLegacy: Row payloads can span multiple compressed chunks
            // We must decompress and stitch all chunks together before parsing
            log::debug!(
                "V5CompressedLegacy format detected, decompressing and stitching all chunks before parsing"
            );

            // Use shared stitching helper method
            let entries = self.stitch_and_parse_all_chunks(None).await?;
            results.extend(entries);
        } else {
            // Other formats: Read and parse blocks individually
            while let Some(block) = self.read_next_block().await? {
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
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<Vec<(TableId, RowKey, Value)>> {
        let stitched_buffer = self.stitch_all_chunks().await?;
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
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<
        Vec<(
            TableId,
            RowKey,
            Value,
            std::collections::HashMap<String, CellWriteMetadata>,
        )>,
    > {
        let stitched_buffer = self.stitch_all_chunks().await?;
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
    /// Precondition: the caller has seeked the file to the start of the data
    /// section and reset `current_chunk_index` to 0.
    async fn stitch_all_chunks(&self) -> Result<Vec<u8>> {
        use crate::storage::sstable::compression::Compression;

        // Pre-allocate buffer for ~2.5MB (estimated max size for test data)
        let mut stitched_buffer = Vec::with_capacity(2_500_000);

        let mut chunk_count = 0;
        while let Some(compressed_chunk) = self.read_next_block().await? {
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
        // Issue #805: serialise concurrent scans (shared file position + chunk index).
        let _scan_guard = self.scan_mutex.lock().await;

        // Position at the start of the data section (mirrors sequential_scan).
        let header_size = self.calculate_header_size();
        {
            let mut file_guard = self.file.lock().await;
            file_guard.seek(SeekFrom::Start(header_size as u64)).await?;
        }
        self.current_chunk_index
            .store(0, std::sync::atomic::Ordering::Relaxed);

        if self.requires_chunk_stitching() {
            // Stitch the (bounded) data section, then parse on a blocking thread,
            // emitting one entry at a time. `blocking_send` provides backpressure
            // so parsed Values are never all live at once.
            let stitched = self.stitch_all_chunks().await?;
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
            while let Some(block) = self.read_next_block().await? {
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
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<Vec<(TableId, RowKey, Value, i64)>> {
        log::debug!("stitch_and_parse_all_chunks_for_compaction: stitching chunks");

        let mut stitched_buffer = Vec::with_capacity(2_500_000);
        let mut chunk_count = 0;

        while let Some(compressed_chunk) = self.read_next_block().await? {
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

        let entries = parser.parse_block_with_timestamps(&stitched_buffer, table_schema, self)?;
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
    ) -> Result<Vec<(RowKey, Value, i64)>> {
        // Only the V5CompressedLegacy NB chunk-stitching path is supported here
        // (that is the format the WriteEngine produces).  For other formats, fall
        // back to iterate_all_partitions and attach timestamp 0 as a conservative
        // default (LWW ordering then relies solely on run_index).
        if self.requires_chunk_stitching() {
            // We need schema; retrieve it once.
            // `schema` is Option<&TableSchema>; clone it into an owned value so we
            // can pass it to the async helper without borrow-checker issues.
            let owned_schema = schema.cloned().or_else(|| self.get_table_schema(None));

            // Reset chunk reader to start of data section.
            let header_size = self.calculate_header_size();
            {
                let mut file_guard = self.file.lock().await;
                use tokio::io::AsyncSeekExt;
                file_guard
                    .seek(std::io::SeekFrom::Start(header_size as u64))
                    .await?;
            }
            self.current_chunk_index
                .store(0, std::sync::atomic::Ordering::Relaxed);

            let entries = self
                .stitch_and_parse_all_chunks_for_compaction(owned_schema.as_ref())
                .await?;

            return Ok(entries
                .into_iter()
                .map(|(_tid, key, value, ts)| (key, value, ts))
                .collect());
        }

        // Non-stitching fallback: use iterate_all_partitions and attach ts=0.
        let entries = self.iterate_all_partitions().await?;
        Ok(entries
            .into_iter()
            .map(|(key, value)| (key, value, 0))
            .collect())
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

        // Issue #805: serialise concurrent scans (shared file position + chunk index).
        let _scan_guard = self.scan_mutex.lock().await;

        // For V5CompressedLegacy NB format, partitions can span chunk boundaries.
        // The block-by-block parser will miss any partition whose bytes cross a
        // chunk boundary.  Use the same stitched-buffer path that sequential_scan()
        // uses so that get() and scan() share a consistent view of the data.
        // (Issue #517)
        if self.requires_chunk_stitching() {
            log::debug!(
                "scan_for_key: V5CompressedLegacy NB detected, using stitched buffer for key lookup"
            );
            // Reset chunk index before stitching
            self.current_chunk_index
                .store(0, std::sync::atomic::Ordering::Relaxed);

            // Pass the reader's own schema so that V5CompressedLegacy rows can be fully
            // parsed and their partition RowKeys emitted.  Without a schema, parse_row_v5
            // fails for all rows in a partition, causing no entries to be pushed and making
            // the key comparison always miss even when the key exists.
            let schema_opt = self.get_table_schema(None);
            let all_entries = match self.stitch_and_parse_all_chunks(schema_opt.as_ref()).await {
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

        let header_size = self.calculate_header_size();
        {
            let mut file_guard = self.file.lock().await;
            file_guard.seek(SeekFrom::Start(header_size as u64)).await?;
        }
        // Reset chunk index when seeking to start
        self.current_chunk_index
            .store(0, std::sync::atomic::Ordering::Relaxed);

        // Sequential scan through blocks
        while let Some(block) = self.read_next_block().await? {
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

        // Issue #805: Serialise concurrent sequential scans on this reader.
        // The file seek-position and current_chunk_index are shared state;
        // two concurrent callers advancing them simultaneously corrupt each
        // other's reads. Hold the scan_mutex for the full scan lifetime.
        let _scan_guard = self.scan_mutex.lock().await;

        let mut results = Vec::new();

        let header_size = self.calculate_header_size();
        log::debug!(
            "SSTableReader::sequential_scan - Header size: {} bytes",
            header_size
        );

        {
            let mut file_guard = self.file.lock().await;
            file_guard.seek(SeekFrom::Start(header_size as u64)).await?;
            log::debug!(
                "SSTableReader::sequential_scan - Seeked to start of data section at offset {}",
                header_size
            );
        }
        // Reset chunk index when seeking to start
        self.current_chunk_index
            .store(0, std::sync::atomic::Ordering::Relaxed);

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
            let all_entries = self.stitch_and_parse_all_chunks(schema).await?;
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
        while let Some(block) = self.read_next_block().await? {
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

        let _scan_guard = self.scan_mutex.lock().await;

        let header_size = self.calculate_header_size();
        {
            let mut file_guard = self.file.lock().await;
            file_guard.seek(SeekFrom::Start(header_size as u64)).await?;
        }
        self.current_chunk_index
            .store(0, std::sync::atomic::Ordering::Relaxed);

        // V5CompressedLegacy (stitching) path — the common path for Cassandra 5.0 SSTables.
        if self.requires_chunk_stitching() {
            let all_entries = self
                .stitch_and_parse_all_chunks_with_metadata(schema)
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

    /// Read next block with enhanced error handling and streaming support
    pub(super) async fn read_next_block(&self) -> Result<Option<Vec<u8>>> {
        use super::block_io;
        block_io::read_next_block(
            &self.file,
            &self.header.cassandra_version,
            &self.config,
            &self.compression_info,
            &self.current_chunk_index,
            self.actual_header_size as u64,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // table_ids_match tests
    // =========================================================================

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
