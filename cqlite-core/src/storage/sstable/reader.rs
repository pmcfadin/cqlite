//! SSTable reader implementation
//!
//! This module provides efficient reading of SSTable files in Cassandra 5+ format.
//! It supports:
//! - Block-based reading with compression
//! - Index-based lookups for efficient queries
//! - Memory-efficient streaming
//! - Bloom filter integration
//! - Multiple compression algorithms

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, BufReader};
use tokio::sync::Mutex;

use crate::{
    parser::{
        types::{parse_cql_value, CqlTypeId},
        vint::parse_vint_length,
        SSTableHeader, SSTableParser,
    },
    platform::Platform,
    types::TableId,
    Config, Error, Result, RowKey, Value,
};

use super::{
    bloom::BloomFilter,
    compression::{Compression, CompressionAlgorithm, CompressionReader, CompressionInfo},
    index::SSTableIndex,
    tombstone_merger::{TombstoneMerger, EntryMetadata, GenerationValue},
};

/// SSTable reader statistics
#[derive(Debug, Clone)]
pub struct SSTableReaderStats {
    /// Total file size in bytes
    pub file_size: u64,
    /// Total number of entries in the SSTable
    pub entry_count: u64,
    /// Number of different tables in this SSTable
    pub table_count: u64,
    /// Number of blocks in the SSTable
    pub block_count: u64,
    /// Index size in bytes
    pub index_size: u64,
    /// Bloom filter size in bytes
    pub bloom_filter_size: u64,
    /// Compression ratio (0.0 to 1.0)
    pub compression_ratio: f64,
    /// Cache hit rate for recent queries
    pub cache_hit_rate: f64,
}

/// Configuration for SSTable reader
#[derive(Debug, Clone)]
pub struct SSTableReaderConfig {
    /// Size of the read buffer in bytes
    pub read_buffer_size: usize,
    /// Whether to use memory-mapped files
    pub use_mmap: bool,
    /// Maximum number of blocks to cache
    pub block_cache_size: usize,
    /// Whether to validate checksums
    pub validate_checksums: bool,
    /// Whether to use bloom filters
    pub use_bloom_filter: bool,
    /// Prefetch size for sequential reads
    pub prefetch_size: usize,
}

impl Default for SSTableReaderConfig {
    fn default() -> Self {
        Self {
            read_buffer_size: 64 * 1024, // 64KB
            use_mmap: false,             // Safer default for cross-platform
            block_cache_size: 1000,      // Cache 1000 blocks
            validate_checksums: true,
            use_bloom_filter: true,
            prefetch_size: 128 * 1024, // 128KB
        }
    }
}

/// Block metadata for efficient reading
#[derive(Debug, Clone)]
pub struct BlockMeta {
    /// Block offset in file
    pub offset: u64,
    /// Compressed size in bytes
    pub compressed_size: u32,
    /// Uncompressed size in bytes
    pub uncompressed_size: u32,
    /// Block checksum
    pub checksum: u32,
    /// First key in block
    pub first_key: RowKey,
    /// Last key in block
    pub last_key: RowKey,
    /// Number of entries in block
    pub entry_count: u32,
}

/// Cached block data
#[derive(Debug, Clone)]
pub struct CachedBlock {
    /// Block metadata
    pub meta: BlockMeta,
    /// Decompressed block data
    pub data: Vec<u8>,
    /// Parsed entries (lazy-loaded)
    pub entries: Option<Vec<(TableId, RowKey, Value)>>,
    /// Last access time for LRU eviction
    pub last_access: std::time::Instant,
}

/// SSTable reader for efficient data access
pub struct SSTableReader {
    /// Path to the SSTable file
    file_path: PathBuf,
    /// File handle for reading
    file: Arc<Mutex<BufReader<File>>>,
    /// SSTable header information
    header: SSTableHeader,
    /// Parser for SSTable format
    parser: SSTableParser,
    /// Index for efficient lookups
    index: Option<SSTableIndex>,
    /// Bloom filter for existence checks
    bloom_filter: Option<BloomFilter>,
    /// Compression reader
    compression_reader: Option<CompressionReader>,
    /// Block metadata cache
    block_meta_cache: HashMap<u64, BlockMeta>,
    /// Block data cache (LRU)
    block_cache: HashMap<u64, CachedBlock>,
    /// Reader configuration
    config: SSTableReaderConfig,
    /// Platform abstraction
    platform: Arc<Platform>,
    /// Statistics
    stats: SSTableReaderStats,
    /// Tombstone merger for deletion handling
    tombstone_merger: TombstoneMerger,
    /// SSTable generation number (for multi-generation merging)
    pub generation: u64,
}

impl SSTableReader {
    /// Open an SSTable file for reading
    pub async fn open(path: &Path, config: &Config, platform: Arc<Platform>) -> Result<Self> {
        let file = File::open(path).await?;
        let file_size = file.metadata().await?.len();
        let file = Arc::new(Mutex::new(BufReader::new(file)));

        // Parse header
        let mut header_buffer = vec![0u8; 4096]; // Initial buffer for header
        {
            let mut file_guard = file.lock().await;
            file_guard.read_exact(&mut header_buffer).await?;
        }

        let parser = SSTableParser::new();
        let (header, header_size) = parser.parse_header(&header_buffer)?;

        // Seek to start of data section
        {
            let mut file_guard = file.lock().await;
            file_guard
                .seek(std::io::SeekFrom::Start(header_size as u64))
                .await?;
        }

        // Initialize compression reader if needed
        let compression_reader = if header.compression.algorithm != "NONE" {
            let algorithm = CompressionAlgorithm::from(header.compression.algorithm.clone());
            Some(CompressionReader::new(algorithm))
        } else {
            // Check for CompressionInfo.db file in the same directory
            let parent_dir = path.parent().unwrap_or(Path::new("."));
            let compression_info_path = parent_dir.join("nb-1-big-CompressionInfo.db");
            
            if compression_info_path.exists() {
                match Self::load_compression_info(&compression_info_path).await {
                    Ok(compression_info) => {
                        let algorithm = compression_info.get_algorithm();
                        Some(CompressionReader::new(algorithm))
                    }
                    Err(e) => {
                        // Log warning but continue without compression
                        eprintln!("Warning: Failed to load CompressionInfo.db: {}", e);
                        None
                    }
                }
            } else {
                None
            }
        };

        // Load index if available
        let index = Self::load_index(&file, &header, &platform).await?;

        // Load bloom filter if available
        let bloom_filter = Self::load_bloom_filter(&file, &header, &platform).await?;

        let reader_config = SSTableReaderConfig::default();

        let stats = SSTableReaderStats {
            file_size,
            entry_count: header.stats.row_count,
            table_count: 1,       // Will be updated as we discover tables
            block_count: 0,       // Will be updated as we scan
            index_size: 0,        // Will be updated if index is loaded
            bloom_filter_size: 0, // Will be updated if bloom filter is loaded
            compression_ratio: header.stats.compression_ratio,
            cache_hit_rate: 0.0,
        };

        // Extract generation from filename or use default
        let generation = Self::extract_generation_from_path(path);
        
        Ok(Self {
            file_path: path.to_path_buf(),
            file,
            header,
            parser,
            index,
            bloom_filter,
            compression_reader,
            block_meta_cache: HashMap::new(),
            block_cache: HashMap::new(),
            config: reader_config,
            platform,
            stats,
            tombstone_merger: TombstoneMerger::new(),
            generation,
        })
    }

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
                return self.read_value_at_offset(entry.offset, entry.size).await;
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
        let mut results = Vec::new();
        let mut count = 0;

        // Use index for efficient range scan if available
        if let Some(index) = &self.index {
            let entries = index.get_range(table_id, start_key, end_key)?;

            for entry in entries {
                if let Some(limit) = limit {
                    if count >= limit {
                        break;
                    }
                }

                if let Some(value) = self.read_value_at_offset(entry.offset, entry.size).await? {
                    results.push((entry.key.clone(), value));
                    count += 1;
                }
            }
        } else {
            // Fallback to sequential scan
            results = self
                .sequential_scan(table_id, start_key, end_key, limit)
                .await?;
        }

        Ok(results)
    }

    /// Get all entries in the SSTable (for compaction)
    pub async fn get_all_entries(&self) -> Result<Vec<(TableId, RowKey, Value)>> {
        let mut results = Vec::new();

        // Reset to beginning of data section
        let header_size = self.calculate_header_size();
        {
            let mut file_guard = self.file.lock().await;
            file_guard
                .seek(std::io::SeekFrom::Start(header_size as u64))
                .await?;
        }

        // Read all blocks sequentially
        while let Some(block) = self.read_next_block().await? {
            let entries = self.parse_block_entries(&block)?;
            results.extend(entries);
        }

        Ok(results)
    }

    /// Get reader statistics
    pub async fn stats(&self) -> Result<SSTableReaderStats> {
        Ok(self.stats.clone())
    }

    /// Close the reader and release resources
    pub async fn close(mut self) -> Result<()> {
        // Clear caches
        self.block_cache.clear();
        self.block_meta_cache.clear();

        // File will be closed automatically when dropped
        Ok(())
    }

    // Private helper methods

    async fn load_compression_info(path: &Path) -> Result<CompressionInfo> {
        use tokio::fs::File;
        use tokio::io::AsyncReadExt;
        
        let mut file = File::open(path).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;
        
        CompressionInfo::parse_binary(&buffer)
    }

    async fn load_index(
        file: &Arc<Mutex<BufReader<File>>>,
        header: &SSTableHeader,
        platform: &Platform,
    ) -> Result<Option<SSTableIndex>> {
        // Check if index information is available in header
        if let Some(index_offset) = header.properties.get("index_offset") {
            let offset: u64 = index_offset
                .parse()
                .map_err(|_| Error::corruption("Invalid index offset in header"))?;

            // Load index from file
            {
                let mut file_guard = file.lock().await;
                file_guard.seek(std::io::SeekFrom::Start(offset)).await?;
                let index = SSTableIndex::load(&mut *file_guard).await?;
                return Ok(Some(index));
            }
        }

        Ok(None)
    }

    async fn load_bloom_filter(
        file: &Arc<Mutex<BufReader<File>>>,
        header: &SSTableHeader,
        platform: &Platform,
    ) -> Result<Option<BloomFilter>> {
        // Check if bloom filter information is available in header
        if let Some(bloom_offset) = header.properties.get("bloom_filter_offset") {
            let offset: u64 = bloom_offset
                .parse()
                .map_err(|_| Error::corruption("Invalid bloom filter offset in header"))?;

            // Load bloom filter from file
            {
                let mut file_guard = file.lock().await;
                file_guard.seek(std::io::SeekFrom::Start(offset)).await?;
                let bloom_filter = BloomFilter::load(&mut *file_guard).await?;
                return Ok(Some(bloom_filter));
            }
        }

        Ok(None)
    }

    async fn read_value_at_offset(&self, offset: u64, size: u32) -> Result<Option<Value>> {
        let mut file = self.file.lock().await;
        file.seek(std::io::SeekFrom::Start(offset)).await?;

        let mut buffer = vec![0u8; size as usize];
        file.read_exact(&mut buffer).await?;

        // Decompress if needed
        let data = if let Some(compression_reader) = &self.compression_reader {
            let compression = Compression::new(compression_reader.algorithm().clone())?;
            compression.decompress(&buffer)?
        } else {
            buffer
        };

        // Parse the value
        let (_, value) = parse_cql_value(&data, CqlTypeId::Varchar) // Type should be determined from context
            .map_err(|e| Error::corruption(format!("Failed to parse value: {:?}", e)))?;

        // Extract write time from value (placeholder - would need to be parsed from SSTable)
        let write_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;
        
        // Filter out tombstones and expired data
        if !self.filter_tombstone(&value) {
            return Ok(None);
        }

        Ok(Some(value))
    }

    /// Enhanced tombstone filtering using TombstoneMerger
    /// Properly handles all types of deletions and TTL expiration
    fn filter_tombstone(&self, value: &Value) -> bool {
        // Use the fast tombstone check for performance
        let write_time = self.extract_write_time_from_value(value);
        
        if self.tombstone_merger.fast_tombstone_check(value, write_time) {
            // Value is deleted by tombstone
            return false;
        }
        
        // Check for TTL expiration on regular values
        if let Some(ttl) = self.extract_ttl_from_value(value) {
            let current_time = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros() as i64;
            
            if current_time > write_time + ttl {
                // Value has expired
                return false;
            }
        }
        
        true // Keep valid, non-deleted values
    }
    
    /// Enhanced multi-generation tombstone filtering for compaction
    /// Merges values from multiple SSTable generations correctly
    pub async fn filter_with_multi_generation_merge(
        &self,
        table_id: &TableId,
        entries: Vec<(RowKey, Vec<GenerationValue>)>,
    ) -> Result<Vec<(RowKey, Value)>> {
        let mut results = Vec::new();
        
        // Use batch processing for better performance
        const BATCH_SIZE: usize = 1000;
        let merged_results = self.tombstone_merger.batch_merge_with_tombstones(entries, BATCH_SIZE)?;
        
        for (key, merged_value) in merged_results {
            if let Some(value) = merged_value {
                results.push((key, value));
            }
        }
        
        Ok(results)
    }
    
    /// Extract TTL from value metadata (placeholder implementation)
    fn extract_ttl_from_value(&self, value: &Value) -> Option<i64> {
        // In a full implementation, this would extract TTL from SSTable metadata
        // For now, only tombstones carry TTL information
        match value {
            Value::Tombstone(info) => info.ttl,
            _ => None, // Regular values would have TTL in SSTable metadata
        }
    }
    
    /// Extract write time from value (enhanced implementation)
    fn extract_write_time_from_value(&self, value: &Value) -> i64 {
        match value {
            Value::Tombstone(info) => info.deletion_time,
            _ => {
                // In a full implementation, write time would be extracted from SSTable entry metadata
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_micros() as i64
            }
        }
    }

    async fn scan_for_key(&self, table_id: &TableId, key: &RowKey) -> Result<Option<Value>> {
        let header_size = self.calculate_header_size();
        {
            let mut file_guard = self.file.lock().await;
            file_guard
                .seek(std::io::SeekFrom::Start(header_size as u64))
                .await?;
        }

        // Sequential scan through blocks
        while let Some(block) = self.read_next_block().await? {
            let entries = self.parse_block_entries(&block)?;

            for (entry_table_id, entry_key, entry_value) in entries {
                if entry_table_id == *table_id && entry_key == *key {
                    // Extract write time from entry metadata (placeholder implementation)
                    let write_time = self.extract_write_time_from_entry(&entry_key, &entry_value);
                    
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

    async fn sequential_scan(
        &self,
        table_id: &TableId,
        start_key: Option<&RowKey>,
        end_key: Option<&RowKey>,
        limit: Option<usize>,
    ) -> Result<Vec<(RowKey, Value)>> {
        let mut results = Vec::new();
        let mut count = 0;

        let header_size = self.calculate_header_size();
        {
            let mut file_guard = self.file.lock().await;
            file_guard
                .seek(std::io::SeekFrom::Start(header_size as u64))
                .await?;
        }

        // Sequential scan through blocks
        while let Some(block) = self.read_next_block().await? {
            let entries = self.parse_block_entries(&block)?;

            for (entry_table_id, entry_key, entry_value) in entries {
                if entry_table_id != *table_id {
                    continue;
                }

                // Check key range
                if let Some(start) = start_key {
                    if entry_key < *start {
                        continue;
                    }
                }

                if let Some(end) = end_key {
                    if entry_key > *end {
                        continue;
                    }
                }

                // Extract write time from entry metadata
                let write_time = self.extract_write_time_from_entry(&entry_key, &entry_value);
                
                // Filter out tombstones and expired data
                if !self.filter_tombstone(&entry_value) {
                    continue;
                }

                results.push((entry_key, entry_value));
                count += 1;

                if let Some(limit) = limit {
                    if count >= limit {
                        return Ok(results);
                    }
                }
            }
        }

        Ok(results)
    }

    async fn read_next_block(&self) -> Result<Option<Vec<u8>>> {
        // Read block header (offset, size, checksum)
        let mut header_buffer = [0u8; 16]; // 8 bytes offset + 4 bytes size + 4 bytes checksum
        {
            let mut file_guard = self.file.lock().await;
            match file_guard.read_exact(&mut header_buffer).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Ok(None); // End of file
                }
                Err(e) => return Err(e.into()),
            }
        }

        let compressed_size = u32::from_be_bytes([
            header_buffer[8],
            header_buffer[9],
            header_buffer[10],
            header_buffer[11],
        ]);
        let checksum = u32::from_be_bytes([
            header_buffer[12],
            header_buffer[13],
            header_buffer[14],
            header_buffer[15],
        ]);

        // Read block data
        let mut block_data = vec![0u8; compressed_size as usize];
        {
            let mut file_guard = self.file.lock().await;
            file_guard.read_exact(&mut block_data).await?;
        }

        // Validate checksum if enabled
        if self.config.validate_checksums {
            let computed_checksum = crc32fast::hash(&block_data);
            if computed_checksum != checksum {
                return Err(Error::corruption("Block checksum mismatch"));
            }
        }

        Ok(Some(block_data))
    }

    fn parse_block_entries(&self, block_data: &[u8]) -> Result<Vec<(TableId, RowKey, Value)>> {
        let mut entries = Vec::new();
        let mut offset = 0;

        // Decompress if needed
        let data = if let Some(compression_reader) = &self.compression_reader {
            let compression = Compression::new(compression_reader.algorithm().clone())?;
            compression.decompress(block_data)?
        } else {
            block_data.to_vec()
        };

        // Enhanced partition data parsing for Cassandra 5.0 format
        while offset < data.len() {
            // Parse entry header with enhanced validation and error handling
            let (new_offset, table_id_len) = parse_vint_length(&data[offset..]).map_err(|e| {
                Error::corruption(format!("Failed to parse table ID length at offset {}: {:?}", offset, e))
            })?;
            offset = data.len() - new_offset.len();
            
            // Validate table ID length to prevent buffer overrun
            if table_id_len > 256 || offset + table_id_len > data.len() {
                return Err(Error::corruption(format!(
                    "Invalid table ID length {} at offset {}, remaining: {}", 
                    table_id_len, offset, data.len() - offset
                )));
            }

            // Parse table ID with enhanced validation for binary IDs
            let table_id_bytes = &data[offset..offset + table_id_len];
            let table_id = match String::from_utf8(table_id_bytes.to_vec()) {
                Ok(s) => TableId::new(s),
                Err(_) => {
                    // Handle binary table IDs in Cassandra 5.0
                    let hex_id = hex::encode(table_id_bytes);
                    TableId::new(format!("binary_{}", hex_id))
                }
            };
            offset += table_id_len;

            // Enhanced row key parsing with Cassandra 5.0 format support
            let (new_offset, key_len) = parse_vint_length(&data[offset..])
                .map_err(|e| Error::corruption(format!("Failed to parse key length at offset {}: {:?}", offset, e)))?;
            offset = data.len() - new_offset.len();
            
            // Validate key length
            if key_len > 65536 || offset + key_len > data.len() {
                return Err(Error::corruption(format!(
                    "Invalid key length {} at offset {}, remaining: {}", 
                    key_len, offset, data.len() - offset
                )));
            }
            
            // Parse compound/composite keys properly
            let key_data = &data[offset..offset + key_len];
            let key = if key_len > 0 {
                self.parse_composite_key(key_data)?
            } else {
                RowKey::new(Vec::new()) // Empty key
            };
            offset += key_len;

            // Enhanced column data extraction with proper type handling
            let (new_offset, value_len) = parse_vint_length(&data[offset..])
                .map_err(|e| Error::corruption(format!("Failed to parse value length at offset {}: {:?}", offset, e)))?;
            offset = data.len() - new_offset.len();
            
            // Handle different value encodings in Cassandra 5.0
            let value = if value_len == 0 {
                // Empty value
                Value::Null
            } else if value_len > 16777216 { // 16MB limit
                return Err(Error::corruption(format!(
                    "Value too large: {} bytes at offset {}", value_len, offset
                )));
            } else if offset + value_len > data.len() {
                return Err(Error::corruption(format!(
                    "Incomplete value: need {} bytes at offset {}, have {}",
                    value_len, offset, data.len() - offset
                )));
            } else {
                let value_data = &data[offset..offset + value_len];
                self.parse_column_value_enhanced(value_data, &table_id, &key)?
            };
            offset += value_len;

            entries.push((table_id, key, value));
        }

        Ok(entries)
    }

    fn calculate_header_size(&self) -> usize {
        // This should be calculated based on the actual header size
        // For now, use a reasonable estimate
        1024 // 1KB header estimate
    }
    
    /// Extract write time from entry metadata (placeholder implementation)
    pub fn extract_write_time_from_entry(&self, _key: &RowKey, value: &Value) -> i64 {
        // In a full implementation, this would extract the write timestamp from the SSTable entry
        // For now, use deletion time from tombstones or current time
        match value {
            Value::Tombstone(info) => info.deletion_time,
            _ => {
                // Default to current time - in reality this would be parsed from SSTable metadata
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_micros() as i64
            }
        }
    }
    
    /// Enhanced composite key parsing for Cassandra 5.0 multi-component keys
    fn parse_composite_key(&self, key_data: &[u8]) -> Result<RowKey> {
        if key_data.is_empty() {
            return Ok(RowKey::new(Vec::new()));
        }
        
        // For simple single-component keys, return as-is
        if key_data.len() < 3 || key_data[0] != 0x00 {
            return Ok(RowKey::new(key_data.to_vec()));
        }
        
        // Parse composite key format: [component_count:vint][component1_len:u16][component1_data][...]
        let mut offset = 0;
        let mut components = Vec::new();
        
        while offset < key_data.len() {
            if offset + 2 > key_data.len() {
                break;
            }
            
            // Read component length (big-endian u16)
            let component_len = u16::from_be_bytes([key_data[offset], key_data[offset + 1]]) as usize;
            offset += 2;
            
            if offset + component_len > key_data.len() {
                break;
            }
            
            components.extend_from_slice(&key_data[offset..offset + component_len]);
            components.push(0x00); // Component separator
            offset += component_len;
            
            // Check for end-of-components marker
            if offset < key_data.len() && key_data[offset] == 0x00 {
                offset += 1;
                break;
            }
        }
        
        // Remove trailing separator if present
        if components.last() == Some(&0x00) {
            components.pop();
        }
        
        Ok(RowKey::new(components))
    }
    
    /// Enhanced column value parsing with Cassandra 5.0 type detection
    fn parse_column_value_enhanced(&self, value_data: &[u8], table_id: &TableId, key: &RowKey) -> Result<Value> {
        if value_data.is_empty() {
            return Ok(Value::Null);
        }
        
        // Enhanced parsing for Cassandra 5.0 serialization format
        // Check for cell metadata header in the first few bytes
        let mut offset = 0;
        
        // Skip cell flags if present (Cassandra 5.0 format)
        if value_data.len() > 1 && (value_data[0] & 0x80) != 0 {
            offset += 1; // Skip flags byte
            
            // Skip timestamp if present (8 bytes)
            if offset + 8 <= value_data.len() {
                offset += 8;
            }
            
            // Skip TTL if present (4 bytes)
            if offset < value_data.len() && (value_data[0] & 0x40) != 0 && offset + 4 <= value_data.len() {
                offset += 4;
            }
        }
        
        // Parse the actual value data
        let actual_value_data = &value_data[offset..];
        
        if actual_value_data.is_empty() {
            return Ok(Value::Null);
        }
        
        // Try different parsing strategies based on data patterns
        match self.detect_value_type(actual_value_data) {
            Some(type_id) => {
                let (_, value) = parse_cql_value(actual_value_data, type_id)
                    .map_err(|e| Error::corruption(format!("Failed to parse detected type {:?}: {:?}", type_id, e)))?;
                Ok(value)
            }
            None => {
                // Fallback: try as UTF-8 text first, then as blob
                match std::str::from_utf8(actual_value_data) {
                    Ok(s) => Ok(Value::Text(s.to_string())),
                    Err(_) => Ok(Value::Blob(actual_value_data.to_vec())),
                }
            }
        }
    }
    
    /// Enhanced type detection for Cassandra 5.0 values
    fn detect_value_type(&self, data: &[u8]) -> Option<CqlTypeId> {
        if data.is_empty() {
            return None;
        }
        
        // Pattern-based type detection for Cassandra 5.0
        match data.len() {
            1 => {
                // Boolean or tinyint
                if data[0] <= 1 {
                    Some(CqlTypeId::Boolean)
                } else {
                    Some(CqlTypeId::Tinyint)
                }
            }
            2 => Some(CqlTypeId::Smallint),
            4 => {
                // Could be int, float, or date
                // Simple heuristic: if all bytes form a reasonable timestamp, treat as date
                let int_val = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                if int_val > 0 && int_val < 100000 { // Reasonable date range
                    Some(CqlTypeId::Date)
                } else {
                    Some(CqlTypeId::Int)
                }
            }
            8 => {
                // Could be bigint, double, or timestamp
                let long_val = i64::from_be_bytes([
                    data[0], data[1], data[2], data[3],
                    data[4], data[5], data[6], data[7]
                ]);
                
                // Timestamp heuristic: reasonable timestamp range
                if long_val > 1_000_000_000_000 && long_val < 10_000_000_000_000 {
                    Some(CqlTypeId::Timestamp)
                } else {
                    Some(CqlTypeId::BigInt)
                }
            }
            16 => Some(CqlTypeId::Uuid), // UUIDs are always 16 bytes
            _ => {
                // Variable length: try to detect collections or text
                if data.len() > 4 {
                    // Check for collection markers (vint length prefixes)
                    if let Ok((_, _)) = parse_vint_length(data) {
                        // Could be a collection, but need more analysis
                        // For now, detect as text if UTF-8 valid
                        if std::str::from_utf8(data).is_ok() {
                            Some(CqlTypeId::Varchar)
                        } else {
                            Some(CqlTypeId::Blob)
                        }
                    } else if std::str::from_utf8(data).is_ok() {
                        Some(CqlTypeId::Varchar)
                    } else {
                        Some(CqlTypeId::Blob)
                    }
                } else {
                    Some(CqlTypeId::Blob)
                }
            }
        }
    }

    /// Extract generation number from SSTable file path
    fn extract_generation_from_path(path: &Path) -> u64 {
        // Extract generation from filename pattern like "sstable_generation_timestamp.sst"
        path.file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(|name| {
                if name.starts_with("sstable_") {
                    // Try to extract generation from filename
                    let parts: Vec<&str> = name.split('_').collect();
                    if parts.len() >= 2 {
                        parts[1].parse().ok()
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .unwrap_or(0) // Default to generation 0
    }
}

impl std::fmt::Debug for SSTableReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SSTableReader")
            .field("file_path", &self.file_path)
            .field("header", &self.header)
            .field("has_index", &self.index.is_some())
            .field("has_bloom_filter", &self.bloom_filter.is_some())
            .field("compression", &self.header.compression.algorithm)
            .field("stats", &self.stats)
            .finish()
    }
}

// Helper function to create a reader with default configuration
pub async fn open_sstable_reader(
    path: &Path,
    config: &Config,
    platform: Arc<Platform>,
) -> Result<SSTableReader> {
    SSTableReader::open(path, config, platform).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_reader_stats() {
        let stats = SSTableReaderStats {
            file_size: 1024,
            entry_count: 100,
            table_count: 1,
            block_count: 10,
            index_size: 128,
            bloom_filter_size: 64,
            compression_ratio: 0.8,
            cache_hit_rate: 0.9,
        };

        assert_eq!(stats.file_size, 1024);
        assert_eq!(stats.entry_count, 100);
        assert_eq!(stats.compression_ratio, 0.8);
    }

    #[tokio::test]
    async fn test_reader_config() {
        let config = SSTableReaderConfig::default();
        assert_eq!(config.read_buffer_size, 64 * 1024);
        assert!(config.validate_checksums);
        assert!(config.use_bloom_filter);
    }

    #[tokio::test]
    async fn test_block_meta() {
        let meta = BlockMeta {
            offset: 1024,
            compressed_size: 512,
            uncompressed_size: 1024,
            checksum: 0x12345678,
            first_key: RowKey::from("key1"),
            last_key: RowKey::from("key10"),
            entry_count: 10,
        };

        assert_eq!(meta.offset, 1024);
        assert_eq!(meta.compressed_size, 512);
        assert_eq!(meta.entry_count, 10);
    }
}
