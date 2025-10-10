//! Data access methods for SSTableReader
//!
//! This module contains all methods related to reading data from SSTables,
//! including point lookups, range scans, and sequential access.

use super::SSTableReader;
use crate::types::{TableId, Value};
use crate::{Error, Result, RowKey};
use log::{debug, warn};
use std::io::SeekFrom;
use tokio::io::AsyncSeekExt;

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
    pub async fn scan(
        &self,
        table_id: &TableId,
        start_key: Option<&RowKey>,
        end_key: Option<&RowKey>,
        limit: Option<usize>,
    ) -> Result<Vec<(RowKey, Value)>> {
        eprintln!("[DEBUG SSTableReader::scan] Starting scan");
        eprintln!(
            "[DEBUG SSTableReader::scan] File path: {:?}",
            self.file_path
        );
        eprintln!("[DEBUG SSTableReader::scan] Table ID: {}", table_id);
        eprintln!("[DEBUG SSTableReader::scan] Start key: {:?}", start_key);
        eprintln!("[DEBUG SSTableReader::scan] End key: {:?}", end_key);
        eprintln!("[DEBUG SSTableReader::scan] Limit: {:?}", limit);
        eprintln!(
            "[DEBUG SSTableReader::scan] Has index: {}",
            self.index.is_some()
        );
        eprintln!(
            "[DEBUG SSTableReader::scan] Has bloom filter: {}",
            self.bloom_filter.is_some()
        );

        let mut results = Vec::new();
        let mut count = 0;

        // Use index for efficient range scan if available
        if let Some(index) = &self.index {
            eprintln!("[DEBUG SSTableReader::scan] Using index-based scan");
            let entries = index.get_range(table_id, start_key, end_key)?;
            eprintln!(
                "[DEBUG SSTableReader::scan] Index returned {} entries",
                entries.len()
            );

            // Check if any entry has size=0 (Cassandra 5.0 format)
            let has_zero_size = entries.iter().any(|e| e.size == 0);
            if has_zero_size {
                eprintln!("[DEBUG SSTableReader::scan] Index reports size=0 for some entries, using sequential scan fallback");
                return self
                    .sequential_scan(table_id, start_key, end_key, limit)
                    .await;
            }

            for (i, entry) in entries.iter().enumerate() {
                // Index offsets are relative to data section start - adjust for header
                let file_offset = entry.offset + self.actual_header_size as u64;
                eprintln!(
                    "[DEBUG SSTableReader::scan] Processing index entry {}: index_offset={}, file_offset={}, size={}",
                    i, entry.offset, file_offset, entry.size
                );

                if let Some(limit) = limit {
                    if count >= limit {
                        eprintln!("[DEBUG SSTableReader::scan] Reached limit {}", limit);
                        break;
                    }
                }

                if let Some(value) = self.read_value_at_offset(file_offset, entry.size).await? {
                    eprintln!(
                        "[DEBUG SSTableReader::scan] Successfully read value at offset {}",
                        entry.offset
                    );
                    results.push((entry.key.clone(), value));
                    count += 1;
                } else {
                    eprintln!("[DEBUG SSTableReader::scan] Value at offset {} was filtered out (tombstone or expired)", entry.offset);
                }
            }
        } else {
            // Fallback to sequential scan
            eprintln!("[DEBUG SSTableReader::scan] No index, falling back to sequential scan");
            results = self
                .sequential_scan(table_id, start_key, end_key, limit)
                .await?;
            eprintln!(
                "[DEBUG SSTableReader::scan] Sequential scan returned {} results",
                results.len()
            );
        }

        eprintln!(
            "[DEBUG SSTableReader::scan] Returning {} final results",
            results.len()
        );
        Ok(results)
    }

    /// Get all entries in the SSTable (for compaction)
    pub async fn get_all_entries(&self) -> Result<Vec<(TableId, RowKey, Value)>> {
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

        // Read all blocks sequentially
        while let Some(block) = self.read_next_block().await? {
            let entries = self.parse_block_entries(&block)?;
            results.extend(entries);
        }

        Ok(results)
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
            let entries = self.parse_block_entries(&block)?;

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
    ) -> Result<Vec<(RowKey, Value)>> {
        eprintln!("[DEBUG SSTableReader::sequential_scan] Starting sequential scan");
        eprintln!(
            "[DEBUG SSTableReader::sequential_scan] Table ID: {}",
            table_id
        );

        let mut results = Vec::new();
        let mut count = 0;

        let header_size = self.calculate_header_size();
        eprintln!(
            "[DEBUG SSTableReader::sequential_scan] Header size: {} bytes",
            header_size
        );

        {
            let mut file_guard = self.file.lock().await;
            file_guard.seek(SeekFrom::Start(header_size as u64)).await?;
            eprintln!("[DEBUG SSTableReader::sequential_scan] Seeked to start of data section at offset {}", header_size);
        }
        // Reset chunk index when seeking to start
        self.current_chunk_index
            .store(0, std::sync::atomic::Ordering::Relaxed);

        // Sequential scan through blocks
        let mut block_count = 0;
        while let Some(block) = self.read_next_block().await? {
            block_count += 1;
            eprintln!(
                "[DEBUG SSTableReader::sequential_scan] Read block {}, size {} bytes",
                block_count,
                block.len()
            );

            let entries = self.parse_block_entries(&block)?;
            eprintln!(
                "[DEBUG SSTableReader::sequential_scan] Block {} contains {} entries",
                block_count,
                entries.len()
            );

            for (i, (entry_table_id, entry_key, entry_value)) in entries.iter().enumerate() {
                eprintln!("[DEBUG SSTableReader::sequential_scan] Block {} entry {}: table_id='{}', key={:?}",
                          block_count, i, entry_table_id, entry_key);

                if entry_table_id != table_id {
                    eprintln!("[DEBUG SSTableReader::sequential_scan] Skipping entry: table_id mismatch ('{}' != '{}')",
                              entry_table_id, table_id);
                    continue;
                }

                // Check key range
                if let Some(start) = start_key {
                    if entry_key < start {
                        eprintln!("[DEBUG SSTableReader::sequential_scan] Skipping entry: key < start_key");
                        continue;
                    }
                }

                if let Some(end) = end_key {
                    if entry_key > end {
                        eprintln!(
                            "[DEBUG SSTableReader::sequential_scan] Skipping entry: key > end_key"
                        );
                        continue;
                    }
                }

                // Extract write time from entry metadata
                let _write_time = self.extract_write_time_from_entry(entry_key, entry_value);

                // Filter out tombstones and expired data
                if !self.filter_tombstone(entry_value) {
                    eprintln!("[DEBUG SSTableReader::sequential_scan] Skipping entry: filtered out (tombstone or expired)");
                    continue;
                }

                eprintln!(
                    "[DEBUG SSTableReader::sequential_scan] Including entry in results (count={})",
                    count + 1
                );
                results.push((entry_key.clone(), entry_value.clone()));
                count += 1;

                if let Some(limit) = limit {
                    if count >= limit {
                        eprintln!("[DEBUG SSTableReader::sequential_scan] Reached limit {}, stopping scan", limit);
                        return Ok(results);
                    }
                }
            }
        }

        eprintln!(
            "[DEBUG SSTableReader::sequential_scan] Finished scanning {} blocks",
            block_count
        );
        eprintln!(
            "[DEBUG SSTableReader::sequential_scan] Returning {} total results",
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
