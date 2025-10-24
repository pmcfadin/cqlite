//! Data access methods for SSTableReader
//!
//! This module contains all methods related to reading data from SSTables,
//! including point lookups, range scans, and sequential access.

use super::SSTableReader;
use crate::parser::DataFormat;
use crate::types::{TableId, Value};
use crate::{Error, Result, RowKey};
use log::{debug, warn};
use std::io::SeekFrom;
use tokio::io::AsyncSeekExt;

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

impl SSTableReader {
    /// Get a value by key from the SSTable
    pub async fn get(&self, table_id: &TableId, key: &RowKey) -> Result<Option<Value>> {
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
        } else {
            // Fallback to sequential scan
            return self.scan_for_key(table_id, key).await;
        }

        Ok(None)
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
        let mut count = 0;

        // Use index for efficient range scan if available
        if let Some(index) = &self.index {
            log::debug!("SSTableReader::scan - Using index-based scan");
            let entries = index.get_range(table_id, start_key, end_key)?;
            log::debug!(
                "SSTableReader::scan - Index returned {} entries",
                entries.len()
            );

            // Check if any entry has size=0 (Cassandra 5.0 format)
            let has_zero_size = entries.iter().any(|e| e.size == 0);
            if has_zero_size {
                log::debug!("SSTableReader::scan - Index reports size=0 for some entries, using sequential scan fallback");
                return self
                    .sequential_scan(table_id, start_key, end_key, limit, schema)
                    .await;
            }

            for (i, entry) in entries.iter().enumerate() {
                // Index offsets are relative to data section start - adjust for header
                let file_offset = entry.offset + self.actual_header_size as u64;
                log::debug!(
                    "SSTableReader::scan - Processing index entry {}: index_offset={}, file_offset={}, size={}",
                    i, entry.offset, file_offset, entry.size
                );

                if let Some(limit) = limit {
                    if count >= limit {
                        log::debug!("SSTableReader::scan - Reached limit {}", limit);
                        break;
                    }
                }

                if let Some(value) = self.read_value_at_offset(file_offset, entry.size).await? {
                    log::debug!(
                        "SSTableReader::scan - Successfully read value at offset {}",
                        entry.offset
                    );
                    results.push((entry.key.clone(), value));
                    count += 1;
                } else {
                    log::debug!("SSTableReader::scan - Value at offset {} was filtered out (tombstone or expired)", entry.offset);
                }
            }
        } else {
            // Fallback to sequential scan
            log::debug!("SSTableReader::scan - No index, falling back to sequential scan");
            results = self
                .sequential_scan(table_id, start_key, end_key, limit, schema)
                .await?;
            log::debug!(
                "SSTableReader::scan - Sequential scan returned {} results",
                results.len()
            );
        }

        log::debug!(
            "SSTableReader::scan - Returning {} final results",
            results.len()
        );
        Ok(results)
    }

    /// Get all entries in the SSTable (for compaction)
    pub async fn get_all_entries(&self) -> Result<Vec<(TableId, RowKey, Value)>> {
        use crate::parser::header::DataFormat;

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

        // Check if this is V5CompressedLegacy format which requires chunk stitching
        let data_format = self.header.cassandra_version.data_format();
        let requires_stitching = matches!(data_format, DataFormat::V5CompressedLegacy);

        if requires_stitching {
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
        log::debug!("stitch_and_parse_all_chunks: Decompressing and stitching all chunks");

        // Pre-allocate buffer for ~2.5MB (estimated max size for test data)
        let mut stitched_buffer = Vec::with_capacity(2_500_000);

        // Read, decompress, and concatenate all chunks
        let mut chunk_count = 0;
        while let Some(compressed_chunk) = self.read_next_block().await? {
            // Decompress this chunk before stitching
            use crate::storage::sstable::compression::Compression;
            let decompressed_chunk = if let Some(compression_reader) = &self.compression_reader {
                let compression = Compression::new(*compression_reader.algorithm())?;
                match compression.decompress(&compressed_chunk) {
                    Ok(decompressed) => {
                        log::debug!(
                            "stitch_and_parse_all_chunks: Chunk {} decompressed {} bytes to {} bytes",
                            chunk_count,
                            compressed_chunk.len(),
                            decompressed.len()
                        );
                        decompressed
                    }
                    Err(e) => {
                        return Err(Error::corruption(format!(
                            "stitch_and_parse_all_chunks: Failed to decompress chunk {}: {}",
                            chunk_count, e
                        )));
                    }
                }
            } else {
                // No compression (should not happen for V5CompressedLegacy)
                log::warn!(
                    "stitch_and_parse_all_chunks: No compression reader, using raw chunk data"
                );
                compressed_chunk
            };

            stitched_buffer.extend_from_slice(&decompressed_chunk);
            chunk_count += 1;
            log::debug!(
                "stitch_and_parse_all_chunks: Stitched chunk {}, total buffer size: {} bytes",
                chunk_count,
                stitched_buffer.len()
            );
        }

        log::debug!(
            "stitch_and_parse_all_chunks: Finished stitching {} chunks, total buffer: {} bytes",
            chunk_count,
            stitched_buffer.len()
        );

        // Extract keyspace/table from header
        let keyspace = self.header.keyspace.clone();
        let table_name = self.header.table_name.clone();

        log::debug!(
            "stitch_and_parse_all_chunks: Using keyspace='{}', table_name='{}'",
            keyspace,
            table_name
        );

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
        );

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
                if entry_table_id == *table_id && entry_key == *key {
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

        let mut results = Vec::new();
        let mut count = 0;

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

        // CRITICAL FIX: V5CompressedLegacy partitions can span chunk boundaries
        // We must stitch all chunks together before parsing to avoid dropping partitions
        let data_format = self.header.cassandra_version.data_format();
        let requires_stitching = matches!(data_format, DataFormat::V5CompressedLegacy);

        if requires_stitching {
            log::debug!(
                "SSTableReader::sequential_scan - V5CompressedLegacy detected, using stitched buffer"
            );

            // Stitch all chunks together (reuse logic from get_all_entries)
            let all_entries = self.stitch_and_parse_all_chunks(schema).await?;
            log::debug!(
                "SSTableReader::sequential_scan - Stitched parsing returned {} total entries",
                all_entries.len()
            );

            // Apply filtering (table_id, key range, limit)
            // Note: We skip table_id matching because the parser may return incorrect table_ids
            // from header defaults. Since sequential_scan is called with a specific table_id,
            // all entries from this SSTable should match that table_id.
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
                count += 1;

                if let Some(lim) = limit {
                    if count >= lim {
                        break;
                    }
                }
            }

            log::debug!(
                "SSTableReader::sequential_scan - Filtered to {} results (limit: {:?})",
                results.len(),
                limit
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

                log::debug!(
                    "SSTableReader::sequential_scan - Including entry in results (count={})",
                    count + 1
                );
                results.push((entry_key.clone(), entry_value.clone()));
                count += 1;

                if let Some(limit) = limit {
                    if count >= limit {
                        log::debug!(
                            "SSTableReader::sequential_scan - Reached limit {}, stopping scan",
                            limit
                        );
                        return Ok(results);
                    }
                }
            }
        }

        log::debug!(
            "SSTableReader::sequential_scan - Finished scanning {} blocks",
            block_count
        );
        log::debug!(
            "SSTableReader::sequential_scan - Returning {} total results",
            results.len()
        );

        Ok(results)
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
        )
        .await
    }
}
