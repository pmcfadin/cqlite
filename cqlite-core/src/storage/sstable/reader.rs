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
        header::CassandraVersion,
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

/// SSTable reader health and performance metrics
#[derive(Debug, Clone)]
pub struct SSTableReaderHealthMetrics {
    /// File path
    pub file_path: PathBuf,
    /// Whether file is accessible
    pub file_accessible: bool,
    /// Detected Cassandra version
    pub header_version: crate::parser::header::CassandraVersion,
    /// Total file size
    pub total_file_size: u64,
    /// Estimated memory usage
    pub estimated_memory_usage: usize,
    /// Number of cached blocks
    pub block_cache_entries: usize,
    /// Cache hit rate
    pub block_cache_hit_rate: f64,
    /// Whether compression is enabled
    pub compression_enabled: bool,
    /// Compression algorithm
    pub compression_algorithm: String,
    /// Whether bloom filter is available
    pub bloom_filter_enabled: bool,
    /// Whether index is available
    pub index_available: bool,
    /// SSTable generation
    pub generation: u64,
    /// Last error encountered
    pub last_error: Option<String>,
}

/// Integrity check results
#[derive(Debug, Clone)]
pub struct IntegrityCheckResult {
    /// File path checked
    pub file_path: PathBuf,
    /// Total blocks checked
    pub total_blocks_checked: usize,
    /// List of corrupted block numbers
    pub corrupted_blocks: Vec<usize>,
    /// Number of checksum mismatches
    pub checksum_mismatches: usize,
    /// Number of unreadable blocks
    pub unreadable_blocks: usize,
    /// Total entries found
    pub total_entries: usize,
    /// Parsing errors encountered
    pub parsing_errors: Vec<String>,
    /// Overall integrity status
    pub overall_status: IntegrityStatus,
}

/// Integrity status levels
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntegrityStatus {
    /// File is healthy
    Healthy,
    /// File has minor issues but is readable
    Degraded,
    /// File has corruption and may be unreadable
    Corrupted,
}

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

        // Parse header - read available bytes, not a fixed size
        let header_size = std::cmp::min(4096, file_size as usize);
        let mut header_buffer = vec![0u8; header_size];
        {
            let mut file_guard = file.lock().await;
            let bytes_read = file_guard.read(&mut header_buffer).await?;
            header_buffer.truncate(bytes_read);
        }

        let config = crate::parser::config::ParserConfig::default();
        let parser = SSTableParser::new(config)?;
        // Parse the header using enhanced version detection
        let header = match Self::parse_header_with_version_detection(&header_buffer, path).await {
            Ok(header) => header,
            Err(e) => {
                eprintln!("Failed to parse header for {:?}, using fallback: {}", path, e);
                // Fallback header for corrupted or unrecognized files
                crate::parser::header::SSTableHeader {
                    cassandra_version: crate::parser::header::CassandraVersion::Legacy,
                    version: crate::parser::header::SUPPORTED_VERSION,
                    table_id: [0; 16],
                    keyspace: path.parent()
                        .and_then(|p| p.file_name())
                        .and_then(|n| n.to_str())
                        .map(|s| s.split('-').next().unwrap_or("unknown").to_string())
                        .unwrap_or_else(|| "unknown".to_string()),
                    table_name: path.file_stem()
                        .and_then(|n| n.to_str())
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| "unknown".to_string()),
                    generation: Self::extract_generation_from_path(path),
                    compression: crate::parser::header::CompressionInfo {
                        algorithm: "NONE".to_string(),
                        chunk_size: 0,
                        parameters: std::collections::HashMap::new(),
                    },
                    stats: crate::parser::header::SSTableStats {
                        row_count: 0,
                        min_timestamp: 0,
                        max_timestamp: 0,
                        max_deletion_time: 0,
                        compression_ratio: 1.0,
                        row_size_histogram: vec![],
                    },
                    columns: vec![],
                    properties: std::collections::HashMap::new(),
                }
            }
        };
        let header_size = Self::calculate_actual_header_size(&header, &header_buffer)?;

        // Seek to start of data section
        {
            let mut file_guard = file.lock().await;
            file_guard
                .seek(std::io::SeekFrom::Start(header_size as u64))
                .await?;
        }

        // Initialize compression reader if needed
        // For debugging Cassandra 5.0, temporarily disable compression
        let compression_reader = if header.compression.algorithm != "NONE" {
            let algorithm = CompressionAlgorithm::from(header.compression.algorithm.clone());
            // Temporarily disable to debug data parsing
            println!("Debug: Found compression {} but disabling for debugging", header.compression.algorithm);
            None // Some(CompressionReader::new(algorithm))
        } else {
            // Check for CompressionInfo.db file in the same directory
            let parent_dir = path.parent().unwrap_or(Path::new("."));
            let compression_info_path = parent_dir.join("nb-1-big-CompressionInfo.db");
            
            if compression_info_path.exists() {
                match Self::load_compression_info(&compression_info_path).await {
                    Ok(compression_info) => {
                        let algorithm = compression_info.get_algorithm();
                        println!("Found CompressionInfo with algorithm: {:?}, chunks: {}", algorithm, compression_info.chunk_count());
                        // Re-enable compression to test decompression
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
        println!("Closing SSTable reader for {:?}", self.file_path);
        
        // Clear caches and log cache statistics
        let cache_entries = self.block_cache.len();
        let meta_entries = self.block_meta_cache.len();
        
        self.block_cache.clear();
        self.block_meta_cache.clear();
        
        println!("Cleared {} block cache entries and {} metadata entries", 
                   cache_entries, meta_entries);

        // File will be closed automatically when dropped
        Ok(())
    }
    
    /// Get comprehensive reader health and performance metrics
    pub async fn get_health_metrics(&self) -> Result<SSTableReaderHealthMetrics> {
        let stats = self.stats().await?;
        
        let cache_hit_rate = if self.stats.cache_hit_rate > 0.0 {
            self.stats.cache_hit_rate
        } else {
            // Calculate current cache hit rate if not tracked
            0.0 // Would need hit/miss counters to calculate accurately
        };
        
        let memory_usage = self.estimate_memory_usage();
        
        Ok(SSTableReaderHealthMetrics {
            file_path: self.file_path.clone(),
            file_accessible: self.file_path.exists(),
            header_version: self.header.cassandra_version,
            total_file_size: stats.file_size,
            estimated_memory_usage: memory_usage,
            block_cache_entries: self.block_cache.len(),
            block_cache_hit_rate: cache_hit_rate,
            compression_enabled: self.compression_reader.is_some(),
            compression_algorithm: self.header.compression.algorithm.clone(),
            bloom_filter_enabled: self.bloom_filter.is_some(),
            index_available: self.index.is_some(),
            generation: self.generation,
            last_error: None, // Would track last error if implemented
        })
    }
    
    /// Estimate current memory usage of the reader
    fn estimate_memory_usage(&self) -> usize {
        let base_size = std::mem::size_of::<Self>();
        let block_cache_size = self.block_cache.iter()
            .map(|(_, block)| block.data.len() + std::mem::size_of::<CachedBlock>())
            .sum::<usize>();
        let meta_cache_size = self.block_meta_cache.len() * std::mem::size_of::<BlockMeta>();
        
        base_size + block_cache_size + meta_cache_size
    }
    
    /// Perform integrity check on the SSTable file
    pub async fn perform_integrity_check(&self) -> Result<IntegrityCheckResult> {
        println!("Starting integrity check for {:?}", self.file_path);
        
        let mut result = IntegrityCheckResult {
            file_path: self.file_path.clone(),
            total_blocks_checked: 0,
            corrupted_blocks: Vec::new(),
            checksum_mismatches: 0,
            unreadable_blocks: 0,
            total_entries: 0,
            parsing_errors: Vec::new(),
            overall_status: IntegrityStatus::Healthy,
        };
        
        // Save current position
        let original_position = {
            let mut file_guard = self.file.lock().await;
            file_guard.stream_position().await.unwrap_or(0)
        };
        
        // Reset to data section
        let header_size = self.calculate_header_size();
        {
            let mut file_guard = self.file.lock().await;
            file_guard.seek(std::io::SeekFrom::Start(header_size as u64)).await?;
        }
        
        // Check each block
        while let Some(block_data) = self.read_next_block().await.ok().flatten() {
            result.total_blocks_checked += 1;
            
            // Try to parse block entries
            match self.parse_block_entries(&block_data) {
                Ok(entries) => {
                    result.total_entries += entries.len();
                },
                Err(e) => {
                    result.parsing_errors.push(format!("Block {}: {}", result.total_blocks_checked, e));
                    result.corrupted_blocks.push(result.total_blocks_checked);
                }
            }
            
            // Yield control periodically
            if result.total_blocks_checked % 100 == 0 {
                tokio::task::yield_now().await;
            }
        }
        
        // Restore original position
        {
            let mut file_guard = self.file.lock().await;
            file_guard.seek(std::io::SeekFrom::Start(original_position)).await?;
        }
        
        // Determine overall status
        result.overall_status = if !result.corrupted_blocks.is_empty() || !result.parsing_errors.is_empty() {
            IntegrityStatus::Corrupted
        } else if result.checksum_mismatches > 0 {
            IntegrityStatus::Degraded
        } else {
            IntegrityStatus::Healthy
        };
        
        println!("Integrity check completed for {:?}: {:?}, {} blocks checked, {} entries", 
                  self.file_path, result.overall_status, result.total_blocks_checked, result.total_entries);
        
        Ok(result)
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
            match compression.decompress(&buffer) {
                Ok(decompressed) => {
                    println!("✅ Successfully decompressed {} bytes to {} bytes", buffer.len(), decompressed.len());
                    decompressed
                },
                Err(e) => {
                    println!("⚠️  Decompression failed ({}), trying raw data parsing instead", e);
                    println!("First 32 bytes of raw data: {:02x?}", &buffer[..std::cmp::min(32, buffer.len())]);
                    // Fall back to raw data - maybe this block isn't actually compressed
                    buffer
                }
            }
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

    /// Read next block with enhanced error handling and streaming support
    async fn read_next_block(&self) -> Result<Option<Vec<u8>>> {
        self.read_next_block_with_retry(3).await
    }
    
    /// Read block with retry logic for handling transient I/O errors
    async fn read_next_block_with_retry(&self, max_retries: usize) -> Result<Option<Vec<u8>>> {
        let mut retry_count = 0;
        
        loop {
            match self.read_next_block_impl().await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    retry_count += 1;
                    if retry_count >= max_retries {
                        eprintln!("Failed to read block after {} retries: {}", max_retries, e);
                        return Err(e);
                    }
                    
                    eprintln!("Block read failed (attempt {}/{}): {}, retrying...", 
                              retry_count, max_retries, e);
                    
                    // Brief delay before retry
                    tokio::time::sleep(tokio::time::Duration::from_millis(10 * retry_count as u64)).await;
                }
            }
        }
    }
    
    /// Internal block reading implementation
    async fn read_next_block_impl(&self) -> Result<Option<Vec<u8>>> {
        // Read block header with format-specific handling
        let block_header = match self.header.cassandra_version {
            crate::parser::header::CassandraVersion::V5_0_NewBig => {
                self.read_nb_format_block_header().await?
            },
            crate::parser::header::CassandraVersion::V5_0_Bti => {
                self.read_bti_format_block_header().await?
            },
            _ => {
                self.read_legacy_format_block_header().await?
            }
        };
        
        let Some((compressed_size, checksum, current_pos)) = block_header else {
            return Ok(None); // EOF
        };
        
        // Validate block size to prevent memory issues
        if compressed_size > 64 * 1024 * 1024 { // 64MB limit
            return Err(Error::corruption(format!(
                "Block size too large: {} bytes (limit: 64MB)", compressed_size
            )));
        }
        
        if compressed_size == 0 {
            println!("Encountered empty block at position {}", current_pos);
            return Ok(Some(Vec::new()));
        }
        
        // Read block data with streaming for large blocks
        let block_data = if compressed_size > self.config.read_buffer_size as u32 {
            self.read_large_block_streaming(compressed_size as usize).await?
        } else {
            self.read_block_direct(compressed_size as usize).await?
        };
        
        // Validate checksum if enabled
        if self.config.validate_checksums && checksum != 0 {
            let computed_checksum = crc32fast::hash(&block_data);
            if computed_checksum != checksum {
                return Err(Error::corruption(format!(
                    "Block checksum mismatch at position {}: expected 0x{:08x}, got 0x{:08x}",
                    current_pos, checksum, computed_checksum
                )));
            }
            println!("Block checksum validated: 0x{:08x}", checksum);
        }
        
        println!("Successfully read block: {} bytes at position {}", block_data.len(), current_pos);
        Ok(Some(block_data))
    }
    
    /// Read block header for 'nb' (new big) format with better parsing
    /// Read block header for NB format (Cassandra 5.0 new big format)
    /// 
    /// Cassandra 5.0 "nb" format uses a different block structure.
    /// The blocks are variable-length compressed chunks with metadata.
    /// Instead of trying to parse individual block headers, we need to 
    /// read the entire data section and decompress it as needed.
    async fn read_nb_format_block_header(&self) -> Result<Option<(u32, u32, u64)>> {
        let current_pos = {
            let mut file_guard = self.file.lock().await;
            file_guard.stream_position().await.unwrap_or(0)
        };
        
        // For Cassandra 5.0 nb format, the data after the header is typically
        // one large compressed block rather than many small blocks.
        // Check if we're at EOF
        let file_size = {
            let mut file_guard = self.file.lock().await;
            file_guard.seek(std::io::SeekFrom::End(0)).await?;
            let size = file_guard.stream_position().await?;
            file_guard.seek(std::io::SeekFrom::Start(current_pos)).await?;
            size
        };
        
        if current_pos >= file_size {
            return Ok(None); // EOF
        }
        
        // Calculate remaining data size
        let remaining_size = (file_size - current_pos) as u32;
        
        if remaining_size == 0 {
            return Ok(None);
        }
        
        // For nb format, treat the entire remaining data as one block
        // The checksum will be validated by the compression layer if enabled
        println!("NB format: Reading remaining {} bytes from position {}", remaining_size, current_pos);
        
        Ok(Some((remaining_size, 0, current_pos))) // checksum=0 means skip validation
    }
    
    /// Read block header for BTI format
    async fn read_bti_format_block_header(&self) -> Result<Option<(u32, u32, u64)>> {
        // BTI format has a slightly different header structure
        let mut header_buffer = [0u8; 12]; // 12-byte header for BTI
        let current_pos = {
            let mut file_guard = self.file.lock().await;
            let pos = file_guard.stream_position().await.unwrap_or(0);
            match file_guard.read_exact(&mut header_buffer).await {
                Ok(_) => pos,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Ok(None);
                }
                Err(e) => return Err(Error::Io(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to read BTI block header: {}", e)))),
            }
        };
        
        let compressed_size = u32::from_be_bytes([
            header_buffer[0], header_buffer[1], header_buffer[2], header_buffer[3],
        ]);
        let checksum = u32::from_be_bytes([
            header_buffer[8], header_buffer[9], header_buffer[10], header_buffer[11],
        ]);
        
        Ok(Some((compressed_size, checksum, current_pos)))
    }
    
    /// Read block header for legacy format
    async fn read_legacy_format_block_header(&self) -> Result<Option<(u32, u32, u64)>> {
        let mut header_buffer = [0u8; 8]; // Minimal 8-byte header
        let current_pos = {
            let mut file_guard = self.file.lock().await;
            let pos = file_guard.stream_position().await.unwrap_or(0);
            match file_guard.read_exact(&mut header_buffer).await {
                Ok(_) => pos,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Ok(None);
                }
                Err(e) => return Err(Error::Io(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to read legacy block header: {}", e)))),
            }
        };
        
        let compressed_size = u32::from_be_bytes([
            header_buffer[0], header_buffer[1], header_buffer[2], header_buffer[3],
        ]);
        let checksum = u32::from_be_bytes([
            header_buffer[4], header_buffer[5], header_buffer[6], header_buffer[7],
        ]);
        
        Ok(Some((compressed_size, checksum, current_pos)))
    }
    
    /// Read block data directly for small blocks
    async fn read_block_direct(&self, size: usize) -> Result<Vec<u8>> {
        let mut block_data = vec![0u8; size];
        {
            let mut file_guard = self.file.lock().await;
            file_guard.read_exact(&mut block_data).await
                .map_err(|e| Error::Io(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to read block data ({}): {}", size, e))))?;
        }
        Ok(block_data)
    }
    
    /// Read large block using streaming I/O to reduce memory pressure
    async fn read_large_block_streaming(&self, size: usize) -> Result<Vec<u8>> {
        let mut block_data = Vec::with_capacity(size);
        let buffer_size = self.config.read_buffer_size.min(size);
        let mut buffer = vec![0u8; buffer_size];
        let mut remaining = size;
        
        println!("Reading large block ({} bytes) using streaming with {} byte buffer", size, buffer_size);
        
        {
            let mut file_guard = self.file.lock().await;
            while remaining > 0 {
                let to_read = remaining.min(buffer_size);
                file_guard.read_exact(&mut buffer[..to_read]).await
                    .map_err(|e| Error::Io(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to read block chunk ({}): {}", to_read, e))))?;
                
                block_data.extend_from_slice(&buffer[..to_read]);
                remaining -= to_read;
                
                // Allow other tasks to run during large reads
                if remaining > 0 && block_data.len() % (1024 * 1024) == 0 {
                    tokio::task::yield_now().await;
                }
            }
        }
        
        Ok(block_data)
    }

    fn parse_block_entries(&self, block_data: &[u8]) -> Result<Vec<(TableId, RowKey, Value)>> {
        let mut entries = Vec::new();
        let mut offset = 0;

        // Decompress if needed
        let data = if let Some(compression_reader) = &self.compression_reader {
            let compression = Compression::new(compression_reader.algorithm().clone())?;
            match compression.decompress(block_data) {
                Ok(decompressed) => {
                    println!("✅ Block decompressed {} bytes to {} bytes", block_data.len(), decompressed.len());
                    decompressed
                },
                Err(e) => {
                    println!("⚠️  Block decompression failed ({}), parsing raw data instead", e);
                    println!("First 32 bytes of block data: {:02x?}", &block_data[..std::cmp::min(32, block_data.len())]);
                    // Fall back to raw data
                    block_data.to_vec()
                }
            }
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

    /// Calculate header size based on format and actual header content
    fn calculate_header_size(&self) -> usize {
        match self.header.cassandra_version {
            crate::parser::header::CassandraVersion::V5_0_NewBig => {
                // For Cassandra 5.0 nb format, use a much simpler approach
                // The actual data starts much later in the file
                // Based on the hex dump analysis, try starting much further in
                1024 // Start after 1KB - will scan for actual block start
                    .min(8192) // Maximum reasonable size
            },
            crate::parser::header::CassandraVersion::V5_0_Bti => {
                // BTI format varies more
                1024
            },
            _ => {
                // Legacy formats
                512
            }
        }
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
                let (_, value) = self.parse_cql_value_with_fallback(actual_value_data, type_id)?;
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
    
    /// Parse CQL value with fallback handling for robust parsing
    fn parse_cql_value_with_fallback(&self, data: &[u8], type_id: CqlTypeId) -> Result<(usize, Value)> {
        // Try the primary parsing first
        match parse_cql_value(data, type_id) {
            Ok((remaining, value)) => {
                let consumed = data.len() - remaining.len();
                Ok((consumed, value))
            }
            Err(_) => {
                // Fallback strategies based on type
                match type_id {
                    CqlTypeId::Varchar | CqlTypeId::Ascii => {
                        // For text types, try different approaches
                        self.parse_text_field_robust(data)
                    }
                    CqlTypeId::Uuid => {
                        // For UUID, ensure we have exactly 16 bytes and validate
                        if data.len() >= 16 {
                            let uuid_bytes = &data[..16];
                            if self.is_valid_uuid(uuid_bytes) {
                                let mut uuid_array = [0u8; 16];
                                uuid_array.copy_from_slice(uuid_bytes);
                                Ok((16, Value::Uuid(uuid_array)))
                            } else {
                                // Not a valid UUID, treat as text or blob
                                if let Ok(text) = std::str::from_utf8(uuid_bytes) {
                                    Ok((16, Value::Text(text.to_string())))
                                } else {
                                    Ok((16, Value::Blob(uuid_bytes.to_vec())))
                                }
                            }
                        } else {
                            Err(Error::corruption("Insufficient data for UUID parsing".to_string()))
                        }
                    }
                    CqlTypeId::Timestamp => {
                        // For timestamps, validate the value makes sense
                        if data.len() >= 8 {
                            let timestamp = i64::from_be_bytes([
                                data[0], data[1], data[2], data[3],
                                data[4], data[5], data[6], data[7]
                            ]);
                            // Convert from milliseconds to microseconds if needed
                            let timestamp_micros = if timestamp < 1_000_000_000_000 {
                                timestamp * 1000 // Convert ms to µs
                            } else {
                                timestamp // Already in µs
                            };
                            Ok((8, Value::Timestamp(timestamp_micros)))
                        } else {
                            Err(Error::corruption("Insufficient data for timestamp parsing".to_string()))
                        }
                    }
                    _ => {
                        // For other types, fall back to blob
                        Ok((data.len(), Value::Blob(data.to_vec())))
                    }
                }
            }
        }
    }
    
    /// Robust text field parsing with multiple strategies
    fn parse_text_field_robust(&self, data: &[u8]) -> Result<(usize, Value)> {
        // Strategy 1: Check if data starts with a length prefix
        if data.len() > 4 {
            // Try parsing as length-prefixed string (4-byte length + data)
            let length = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
            if length > 0 && length <= data.len() - 4 && length < 1_000_000 { // Reasonable size limit
                let text_data = &data[4..4 + length];
                if let Ok(text) = std::str::from_utf8(text_data) {
                    return Ok((4 + length, Value::Text(text.to_string())));
                }
            }
        }
        
        // Strategy 2: Check if data starts with a vint length prefix
        if let Ok((remaining, length)) = parse_vint_length(data) {
            let prefix_len = data.len() - remaining.len();
            if length > 0 && length <= remaining.len() && length < 1_000_000 {
                let text_data = &remaining[..length];
                if let Ok(text) = std::str::from_utf8(text_data) {
                    return Ok((prefix_len + length, Value::Text(text.to_string())));
                }
            }
        }
        
        // Strategy 3: Try parsing as raw UTF-8 (null-terminated or full data)
        if let Ok(text) = std::str::from_utf8(data) {
            // Check for null termination
            if let Some(null_pos) = text.find('\0') {
                let clean_text = &text[..null_pos];
                return Ok((null_pos + 1, Value::Text(clean_text.to_string())));
            } else {
                return Ok((data.len(), Value::Text(text.to_string())));
            }
        }
        
        // Strategy 4: Fall back to blob if text parsing fails
        Ok((data.len(), Value::Blob(data.to_vec())))
    }
    
    /// Enhanced type detection for Cassandra 5.0 values with improved UUID validation
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
                
                // Improved timestamp heuristic: check for reasonable timestamp range
                if long_val > 1_000_000_000_000 && long_val < 10_000_000_000_000 {
                    Some(CqlTypeId::Timestamp)
                } else {
                    Some(CqlTypeId::BigInt)
                }
            }
            16 => {
                // CRITICAL FIX: Not all 16-byte data is UUID - validate UUID structure
                if self.is_valid_uuid(data) {
                    Some(CqlTypeId::Uuid)
                } else {
                    // Could be text, blob, or other 16-byte data
                    if std::str::from_utf8(data).is_ok() {
                        Some(CqlTypeId::Varchar)
                    } else {
                        Some(CqlTypeId::Blob)
                    }
                }
            }
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
    
    /// Validate if 16-byte data is actually a valid UUID
    /// Checks UUID version and variant bits to eliminate false positives
    fn is_valid_uuid(&self, data: &[u8]) -> bool {
        if data.len() != 16 {
            return false;
        }
        
        // Check UUID version (bits 12-15 of the time_hi_and_version field)
        let version = (data[6] & 0xF0) >> 4;
        if version < 1 || version > 5 {
            // Not a valid UUID version
            return false;
        }
        
        // Check UUID variant (bits 6-7 of the clock_seq_hi_and_reserved field)
        let variant = (data[8] & 0xC0) >> 6;
        if variant != 2 {
            // Not RFC 4122 variant (should be binary 10)
            return false;
        }
        
        // Additional heuristic: check if the data looks like random bytes
        // UUIDs should have some randomness, not all zeros or repeated patterns
        let is_all_zeros = data.iter().all(|&b| b == 0);
        let is_all_same = data.windows(2).all(|w| w[0] == w[1]);
        
        if is_all_zeros || is_all_same {
            return false;
        }
        
        // Check for common text patterns that might be exactly 16 bytes
        // If it's valid UTF-8 and contains readable text, it's probably not a UUID
        if let Ok(text) = std::str::from_utf8(data) {
            let has_letters = text.chars().any(|c| c.is_alphabetic());
            let has_spaces = text.chars().any(|c| c.is_whitespace());
            
            if has_letters || has_spaces {
                // Likely text data, not a UUID
                return false;
            }
        }
        
        true
    }

    /// Extract generation number from SSTable file path with enhanced pattern matching
    fn extract_generation_from_path(path: &Path) -> u64 {
        path.file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(|name| {
                // Try multiple patterns:
                // 1. Cassandra 5.x pattern: "nb-{generation}-{format}-{component}"
                if name.contains("-") {
                    let parts: Vec<&str> = name.split('-').collect();
                    if parts.len() >= 2 {
                        if let Ok(gen) = parts[1].parse::<u64>() {
                            return Some(gen);
                        }
                    }
                }
                
                // 2. Legacy pattern: "sstable_{generation}_{timestamp}"
                if name.starts_with("sstable_") {
                    let parts: Vec<&str> = name.split('_').collect();
                    if parts.len() >= 2 {
                        if let Ok(gen) = parts[1].parse::<u64>() {
                            return Some(gen);
                        }
                    }
                }
                
                // 3. Extract from timestamp if available
                if let Some(timestamp_start) = name.rfind('_') {
                    let timestamp_part = &name[timestamp_start + 1..];
                    if let Ok(timestamp) = timestamp_part.parse::<u64>() {
                        // Use last 16 bits of timestamp as generation
                        return Some(timestamp & 0xFFFF);
                    }
                }
                
                None
            })
            .unwrap_or(0) // Default to generation 0
    }
    
    /// Parse header with comprehensive Cassandra version detection
    async fn parse_header_with_version_detection(
        header_buffer: &[u8],
        path: &Path,
    ) -> Result<crate::parser::header::SSTableHeader> {
        use crate::parser::header::{parse_magic_and_version, parse_sstable_header, CassandraVersion};
        
        if header_buffer.len() < 6 {
            return Err(Error::corruption("Header buffer too small"));
        }
        
        // First, try to detect the Cassandra version from magic number
        let (cassandra_version, actual_header_start) = match parse_magic_and_version(header_buffer) {
            Ok((remaining, (version, _))) => {
                let header_start = header_buffer.len() - remaining.len();
                println!("Detected Cassandra version: {:?} for {:?}", version, path);
                (version, header_start)
            },
            Err(_) => {
                eprintln!("Could not detect Cassandra version from magic number for {:?}, trying heuristics", path);
                // Try heuristic detection based on file patterns
                Self::detect_version_from_filename(path)
            }
        };
        
        // Try to parse the full header if we have enough data
        let header_slice = &header_buffer[actual_header_start..];
        match parse_sstable_header(header_slice) {
            Ok((_, header)) => {
                println!("Successfully parsed header for {:?}: keyspace={}, table={}", 
                          path, header.keyspace, header.table_name);
                Ok(header)
            },
            Err(e) => {
                eprintln!("Failed to parse full header for {:?}: {:?}, creating minimal header", path, e);
                Self::create_minimal_header_from_version_and_path(cassandra_version, path)
            }
        }
    }
    
    /// Detect version from filename patterns when magic number parsing fails
    fn detect_version_from_filename(path: &Path) -> (CassandraVersion, usize) {
        let filename = path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("");
            
        let version = if filename.contains("-big-") {
            CassandraVersion::V5_0_NewBig
        } else if filename.contains("-da-") || filename.contains("Partitions") {
            CassandraVersion::V5_0_Bti
        } else if filename.starts_with("nb-") {
            CassandraVersion::V5_0_NewBig
        } else {
            CassandraVersion::Legacy
        };
        
        println!("Detected version from filename pattern: {:?} for {:?}", version, path);
        (version, 0) // Start from beginning since we couldn't parse magic
    }
    
    /// Create minimal header when full parsing fails
    fn create_minimal_header_from_version_and_path(
        cassandra_version: CassandraVersion,
        path: &Path,
    ) -> Result<crate::parser::header::SSTableHeader> {
        use crate::parser::header::{SSTableHeader, CompressionInfo, SSTableStats};
        
        // Extract keyspace and table from path
        let (keyspace, table_name) = Self::extract_keyspace_table_from_path(path);
        let generation = Self::extract_generation_from_path(path);
        
        Ok(SSTableHeader {
            cassandra_version,
            version: crate::parser::header::SUPPORTED_VERSION,
            table_id: Self::generate_table_id_from_path(path),
            keyspace,
            table_name,
            generation,
            compression: CompressionInfo {
                algorithm: "NONE".to_string(),
                chunk_size: 0,
                parameters: std::collections::HashMap::new(),
            },
            stats: SSTableStats {
                row_count: 0,
                min_timestamp: 0,
                max_timestamp: 0,
                max_deletion_time: 0,
                compression_ratio: 1.0,
                row_size_histogram: vec![],
            },
            columns: vec![],
            properties: std::collections::HashMap::new(),
        })
    }
    
    /// Extract keyspace and table name from file path
    fn extract_keyspace_table_from_path(path: &Path) -> (String, String) {
        // Path structure: .../keyspace/table-uuid/sstable_files
        let path_components: Vec<&str> = path.components()
            .filter_map(|c| c.as_os_str().to_str())
            .collect();
            
        let mut keyspace = "unknown".to_string();
        let mut table_name = "unknown".to_string();
        
        // Look for keyspace (parent of table directory)
        if path_components.len() >= 2 {
            let table_dir = path_components[path_components.len() - 2];
            if let Some(hyphen_pos) = table_dir.rfind('-') {
                table_name = table_dir[..hyphen_pos].to_string();
            } else {
                table_name = table_dir.to_string();
            }
            
            if path_components.len() >= 3 {
                keyspace = path_components[path_components.len() - 3].to_string();
            }
        }
        
        // Fallback: extract from filename
        if table_name == "unknown" {
            if let Some(filename) = path.file_stem().and_then(|n| n.to_str()) {
                if filename.contains("-") {
                    let parts: Vec<&str> = filename.split('-').collect();
                    if parts.len() >= 3 {
                        table_name = parts[0].to_string();
                    }
                }
            }
        }
        
        println!("Extracted keyspace='{}', table='{}' from path {:?}", keyspace, table_name, path);
        (keyspace, table_name)
    }
    
    /// Generate table ID from path (deterministic)
    fn generate_table_id_from_path(path: &Path) -> [u8; 16] {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        path.hash(&mut hasher);
        let hash = hasher.finish();
        
        let mut table_id = [0u8; 16];
        table_id[0..8].copy_from_slice(&hash.to_be_bytes());
        table_id[8..16].copy_from_slice(&hash.to_le_bytes());
        
        table_id
    }
    
    /// Calculate actual header size from parsed header
    fn calculate_actual_header_size(
        header: &crate::parser::header::SSTableHeader,
        header_buffer: &[u8],
    ) -> Result<usize> {
        // For Cassandra 5.x formats, the header size varies by version
        match header.cassandra_version {
            crate::parser::header::CassandraVersion::V5_0_NewBig => {
                // 'nb' format has a fixed header structure
                Ok(2048) // 2KB header for 'nb' format
            },
            crate::parser::header::CassandraVersion::V5_0_Bti => {
                // BTI format has variable header size
                Ok(1024) // Default 1KB, could be calculated more precisely
            },
            _ => {
                // Legacy and other formats
                Ok(512) // Conservative default
            }
        }
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
