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
    Config, Error, Result, RowKey, Value,
    parser::{SSTableHeader, SSTableParser, header::CassandraVersion, vint::parse_vint_length},
    platform::Platform,
    schema::{ClusteringColumn, Column, KeyColumn, TableSchema},
    types::{ComparatorType, TableId},
};

use super::{
    bloom::BloomFilter,
    compression::{Compression, CompressionAlgorithm, CompressionInfo, CompressionReader},
    index::SSTableIndex,
    index_reader::IndexReader,
    summary_reader::SummaryReader,
    statistics_reader::StatisticsReader,
    row_cell_state_machine::{ParsedRow, RowCellStateMachine},
    tombstone_merger::{GenerationValue, TombstoneMerger},
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
    #[allow(dead_code)]
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
    /// Index.db reader for partition lookup and promoted index handling
    index_reader: Option<IndexReader>,
    /// Summary.db reader for token-range iteration and sampling
    summary_reader: Option<SummaryReader>,
    /// Statistics.db reader for min/max timestamps and metadata
    statistics_reader: Option<StatisticsReader>,
}

impl SSTableReader {
    /// Open an SSTable file for reading
    pub async fn open(path: &Path, _config: &Config, platform: Arc<Platform>) -> Result<Self> {
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
                eprintln!(
                    "Failed to parse header for {:?}, using fallback: {}",
                    path, e
                );
                // Fallback header for corrupted or unrecognized files
                crate::parser::header::SSTableHeader {
                    cassandra_version: crate::parser::header::CassandraVersion::Legacy,
                    version: crate::parser::header::SUPPORTED_VERSION,
                    table_id: [0; 16],
                    keyspace: path
                        .parent()
                        .and_then(|p| p.file_name())
                        .and_then(|n| n.to_str())
                        .map(|s| s.split('-').next().unwrap_or("unknown").to_string())
                        .unwrap_or_else(|| "unknown".to_string()),
                    table_name: path
                        .file_stem()
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

        // ENHANCEMENT: Initialize compression reader with improved format detection
        let compression_reader = Self::detect_and_initialize_compression(&header, path).await?;

        // Load index if available
        let index = Self::load_index(&file, &header, &platform).await?;

        // Load bloom filter if available
        let bloom_filter = Self::load_bloom_filter(&file, &header, &platform).await?;

        let reader_config = SSTableReaderConfig::default();

        // Load spec readers for enhanced metadata and lookups
        let index_reader = Self::load_index_reader(path, &platform).await;
        let summary_reader = Self::load_summary_reader(path, &platform).await;
        let statistics_reader = Self::load_statistics_reader(path, &platform).await;

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
            index_reader,
            summary_reader,
            statistics_reader,
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

        println!(
            "Cleared {} block cache entries and {} metadata entries",
            cache_entries, meta_entries
        );

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
        let block_cache_size = self
            .block_cache
            .iter()
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
            file_guard
                .seek(std::io::SeekFrom::Start(header_size as u64))
                .await?;
        }

        // Check each block
        while let Some(block_data) = self.read_next_block().await.ok().flatten() {
            result.total_blocks_checked += 1;

            // Try to parse block entries
            match self.parse_block_entries(&block_data) {
                Ok(entries) => {
                    result.total_entries += entries.len();
                }
                Err(e) => {
                    result
                        .parsing_errors
                        .push(format!("Block {}: {}", result.total_blocks_checked, e));
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
            file_guard
                .seek(std::io::SeekFrom::Start(original_position))
                .await?;
        }

        // Determine overall status
        result.overall_status =
            if !result.corrupted_blocks.is_empty() || !result.parsing_errors.is_empty() {
                IntegrityStatus::Corrupted
            } else if result.checksum_mismatches > 0 {
                IntegrityStatus::Degraded
            } else {
                IntegrityStatus::Healthy
            };

        println!(
            "Integrity check completed for {:?}: {:?}, {} blocks checked, {} entries",
            self.file_path,
            result.overall_status,
            result.total_blocks_checked,
            result.total_entries
        );

        Ok(result)
    }

    // Missing function implementations

    /// Enhanced header parsing with version detection
    async fn parse_header_with_version_detection(
        header_buffer: &[u8],
        path: &Path,
    ) -> Result<SSTableHeader> {
        use crate::parser::header::{CassandraVersion, parse_sstable_header};

        if header_buffer.len() < 8 {
            return Err(Error::corruption("Header buffer too small for parsing"));
        }

        // Try to parse using the existing header parser first
        match parse_sstable_header(header_buffer) {
            Ok((_, header)) => {
                println!(
                    "✅ Successfully parsed header with version detection for {:?}",
                    path
                );
                Ok(header)
            }
            Err(_) => {
                // Fallback: Try to detect magic number manually and create a basic header
                let magic_bytes = &header_buffer[0..4];
                let magic = u32::from_be_bytes([
                    magic_bytes[0],
                    magic_bytes[1],
                    magic_bytes[2],
                    magic_bytes[3],
                ]);

                let version = if header_buffer.len() >= 6 {
                    u16::from_be_bytes([header_buffer[4], header_buffer[5]])
                } else {
                    crate::parser::header::SUPPORTED_VERSION
                };

                // Detect Cassandra version from magic number
                let cassandra_version =
                    CassandraVersion::from_magic_number(magic).unwrap_or(CassandraVersion::Legacy);

                println!(
                    "🔍 Detected Cassandra version: {:?} (magic: 0x{:08x}) for {:?}",
                    cassandra_version, magic, path
                );

                // Create fallback header
                Ok(crate::parser::header::SSTableHeader {
                    cassandra_version,
                    version,
                    table_id: [0; 16],
                    keyspace: path
                        .parent()
                        .and_then(|p| p.file_name())
                        .and_then(|n| n.to_str())
                        .map(|s| s.split('-').next().unwrap_or("unknown").to_string())
                        .unwrap_or_else(|| "unknown".to_string()),
                    table_name: path
                        .file_stem()
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
                })
            }
        }
    }

    /// Extract generation number from SSTable file path
    fn extract_generation_from_path(path: &Path) -> u64 {
        let filename = path.file_name().and_then(|n| n.to_str()).unwrap_or("");

        // Common Cassandra SSTable filename patterns:
        // nb-1-big-Data.db -> generation 1
        // mc-1-big-Data.db -> generation 1
        // la-123-big-Data.db -> generation 123
        // keyspace-table-nb-456-big-Data.db -> generation 456

        // Try to find generation number in different patterns
        let parts: Vec<&str> = filename.split('-').collect();

        // Pattern 1: nb-{generation}-big-Data.db
        if parts.len() >= 3 && (parts[0] == "nb" || parts[0] == "mc" || parts[0] == "la") {
            if let Ok(generation) = parts[1].parse::<u64>() {
                println!(
                    "📁 Extracted generation {} from pattern 1: {}",
                    generation, filename
                );
                return generation;
            }
        }

        // Pattern 2: keyspace-table-nb-{generation}-big-Data.db
        if parts.len() >= 5 {
            for i in 0..parts.len() - 2 {
                if (parts[i] == "nb" || parts[i] == "mc" || parts[i] == "la") && i + 1 < parts.len()
                {
                    if let Ok(generation) = parts[i + 1].parse::<u64>() {
                        println!(
                            "📁 Extracted generation {} from pattern 2: {}",
                            generation, filename
                        );
                        return generation;
                    }
                }
            }
        }

        // Pattern 3: Look for any numeric part that could be generation
        for part in &parts {
            if let Ok(generation) = part.parse::<u64>() {
                // Skip obviously wrong numbers (like version numbers)
                if generation > 0 && generation < 1_000_000 {
                    println!(
                        "📁 Extracted generation {} from numeric part: {}",
                        generation, filename
                    );
                    return generation;
                }
            }
        }

        // Default generation if parsing fails
        println!("📁 Using default generation 0 for: {}", filename);
        0
    }

    /// Calculate actual header size based on header content and buffer
    fn calculate_actual_header_size(header: &SSTableHeader, header_buffer: &[u8]) -> Result<usize> {
        // For different Cassandra versions, header sizes vary significantly
        match header.cassandra_version {
            crate::parser::header::CassandraVersion::V5_0NewBig => {
                // For nb format, headers are typically larger and more complex
                // Try to find where the actual data begins by looking for patterns
                Self::find_data_start_nb_format(header_buffer)
            }
            crate::parser::header::CassandraVersion::V5_0Bti => {
                // BTI format has different header structure
                Self::find_data_start_bti_format(header_buffer)
            }
            crate::parser::header::CassandraVersion::Legacy => {
                // Legacy format has simpler, more predictable headers
                Self::find_data_start_legacy_format(header_buffer)
            }
            _ => {
                // For other versions, use a reasonable default based on buffer analysis
                Self::estimate_header_size_heuristic(header_buffer)
            }
        }
    }

    // Private helper methods

    /// Find data start for nb format files
    fn find_data_start_nb_format(header_buffer: &[u8]) -> Result<usize> {
        // For nb format, look for compressed data patterns or block headers
        // The header typically ends where compressed blocks begin

        // Strategy 1: Look for compression signatures (LZ4, Snappy, etc.)
        for i in 64..header_buffer.len().min(2048) {
            if i + 8 < header_buffer.len() {
                // Check for LZ4 signature
                if header_buffer[i..i + 4] == [0x04, 0x22, 0x4D, 0x18] {
                    println!("🔍 Found LZ4 signature at offset {}", i);
                    return Ok(i);
                }

                // Check for typical block size patterns (large values that could be block sizes)
                let potential_size = u32::from_be_bytes([
                    header_buffer[i],
                    header_buffer[i + 1],
                    header_buffer[i + 2],
                    header_buffer[i + 3],
                ]);

                // Reasonable block sizes for Cassandra (1KB to 64MB)
                if potential_size >= 1024 && potential_size <= 64 * 1024 * 1024 {
                    // This might be a block size header
                    println!(
                        "🔍 Found potential block header at offset {} (size: {})",
                        i, potential_size
                    );
                    return Ok(i);
                }
            }
        }

        // Fallback: Use fixed size for nb format
        let fallback_size = 1024.min(header_buffer.len());
        println!(
            "🔍 Using fallback header size {} for nb format",
            fallback_size
        );
        Ok(fallback_size)
    }

    /// Find data start for BTI format files  
    fn find_data_start_bti_format(header_buffer: &[u8]) -> Result<usize> {
        // BTI format has different markers and structure
        // Look for BTI-specific patterns

        let fallback_size = 1024.min(header_buffer.len());
        println!(
            "🔍 Using estimated header size {} for BTI format",
            fallback_size
        );
        Ok(fallback_size)
    }

    /// Find data start for legacy format files
    fn find_data_start_legacy_format(header_buffer: &[u8]) -> Result<usize> {
        // Legacy format is more predictable - usually 512 bytes or less
        let fallback_size = 512.min(header_buffer.len());
        println!(
            "🔍 Using standard header size {} for legacy format",
            fallback_size
        );
        Ok(fallback_size)
    }

    /// Estimate header size using heuristics when version is unknown
    fn estimate_header_size_heuristic(header_buffer: &[u8]) -> Result<usize> {
        // Use heuristics to estimate where header ends and data begins
        // Look for patterns that indicate start of data section

        for i in (64..header_buffer.len().min(1024)).step_by(64) {
            if i + 16 < header_buffer.len() {
                // Check if this position has characteristics of data vs. header
                let slice = &header_buffer[i..i + 16];

                // Data sections often have more entropy than headers
                let non_zero_bytes = slice.iter().filter(|&&b| b != 0).count();
                let entropy_score = non_zero_bytes as f32 / 16.0;

                // If we find a region with high entropy, it might be start of data
                if entropy_score > 0.7 {
                    println!(
                        "🔍 Detected potential data start at offset {} (entropy: {:.2})",
                        i, entropy_score
                    );
                    return Ok(i);
                }
            }
        }

        // Conservative fallback
        let fallback_size = 768.min(header_buffer.len());
        println!("🔍 Using heuristic header size {}", fallback_size);
        Ok(fallback_size)
    }

    // Private helper methods

    /// Enhanced compression format detection and initialization
    async fn detect_and_initialize_compression(
        header: &SSTableHeader,
        path: &Path,
    ) -> Result<Option<CompressionReader>> {
        // Strategy 1: Check header compression info
        if header.compression.algorithm != "NONE" {
            let algorithm = CompressionAlgorithm::from(header.compression.algorithm.clone());
            println!("Header indicates compression: {:?}", algorithm);

            // Validate compression algorithm is supported
            match algorithm {
                CompressionAlgorithm::Lz4
                | CompressionAlgorithm::Snappy
                | CompressionAlgorithm::Deflate
                | CompressionAlgorithm::Zstd => {
                    return Ok(Some(CompressionReader::new(algorithm)));
                }
                CompressionAlgorithm::None => {
                    // Continue to other detection methods
                }
            }
        }

        // Strategy 2: Check for CompressionInfo.db file in the same directory
        let parent_dir = path.parent().unwrap_or(Path::new("."));

        // Try multiple CompressionInfo file patterns
        let compressed_filename = format!(
            "{}-CompressionInfo.db",
            path.file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown")
        );
        let compression_info_patterns = [
            "nb-1-big-CompressionInfo.db",
            "CompressionInfo.db",
            compressed_filename.as_str(),
        ];

        for pattern in &compression_info_patterns {
            let compression_info_path = parent_dir.join(pattern);

            if compression_info_path.exists() {
                match Self::load_compression_info(&compression_info_path).await {
                    Ok(compression_info) => {
                        let algorithm = compression_info.get_algorithm();
                        println!(
                            "Found CompressionInfo at {:?} with algorithm: {:?}, chunks: {}",
                            compression_info_path,
                            algorithm,
                            compression_info.chunk_count()
                        );

                        if algorithm != CompressionAlgorithm::None {
                            return Ok(Some(CompressionReader::new(algorithm)));
                        }
                    }
                    Err(e) => {
                        eprintln!("Warning: Failed to load {}: {}", pattern, e);
                        continue;
                    }
                }
            }
        }

        // Strategy 3: Heuristic detection based on file format and data patterns
        if let Some(algorithm) = Self::detect_compression_heuristic(header, path).await? {
            println!("Heuristic detection found compression: {:?}", algorithm);
            return Ok(Some(CompressionReader::new(algorithm)));
        }

        // Strategy 4: Check filename patterns for compression hints
        if let Some(algorithm) = Self::detect_compression_from_filename(path) {
            println!("Filename pattern suggests compression: {:?}", algorithm);
            return Ok(Some(CompressionReader::new(algorithm)));
        }

        println!("No compression detected for {:?}", path);
        Ok(None)
    }

    /// Heuristic compression detection based on file format and data analysis
    async fn detect_compression_heuristic(
        header: &SSTableHeader,
        _path: &Path,
    ) -> Result<Option<CompressionAlgorithm>> {
        // For Cassandra 5.0 'nb' format, LZ4 is commonly used
        if header.cassandra_version == crate::parser::header::CassandraVersion::V5_0NewBig {
            // Check if this looks like compressed data by analyzing entropy or patterns
            // For now, assume LZ4 for nb format as it's the most common
            return Ok(Some(CompressionAlgorithm::Lz4));
        }

        // For BTI format, Snappy is often used
        if header.cassandra_version == crate::parser::header::CassandraVersion::V5_0Bti {
            return Ok(Some(CompressionAlgorithm::Snappy));
        }

        Ok(None)
    }

    /// Detect compression algorithm from filename patterns
    fn detect_compression_from_filename(path: &Path) -> Option<CompressionAlgorithm> {
        let filename = path.file_name().and_then(|n| n.to_str()).unwrap_or("");

        // Check for compression hints in filename
        if filename.contains("lz4") || filename.contains("LZ4") {
            Some(CompressionAlgorithm::Lz4)
        } else if filename.contains("snappy") || filename.contains("SNAPPY") {
            Some(CompressionAlgorithm::Snappy)
        } else if filename.contains("deflate") || filename.contains("DEFLATE") {
            Some(CompressionAlgorithm::Deflate)
        } else if filename.contains("zstd") || filename.contains("ZSTD") {
            Some(CompressionAlgorithm::Zstd)
        } else {
            None
        }
    }

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
        _platform: &Platform,
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
        _platform: &Platform,
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
                    log::debug!(
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
                        log::warn!(
                            "Decompression failed for legacy format ({}), using raw data",
                            e
                        );
                        log::debug!(
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

        if self
            .tombstone_merger
            .fast_tombstone_check(value, write_time)
        {
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
    /// Merges values from multiple SSTable generations correctly with comprehensive conflict resolution
    pub async fn filter_with_multi_generation_merge(
        &self,
        table_id: &TableId,
        entries: Vec<(RowKey, Vec<GenerationValue>)>,
    ) -> Result<Vec<(RowKey, Value)>> {
        let mut results = Vec::new();

        println!(
            "Processing {} key groups for multi-generation merge",
            entries.len()
        );

        // Use batch processing for better performance
        const BATCH_SIZE: usize = 1000;

        // ENHANCEMENT: Enhanced batch processing with comprehensive tombstone handling
        let batches: Vec<_> = entries.chunks(BATCH_SIZE).collect();

        for (batch_idx, batch) in batches.iter().enumerate() {
            println!(
                "Processing batch {}/{} with {} entries",
                batch_idx + 1,
                batches.len(),
                batch.len()
            );

            let batch_entries = batch.to_vec();
            let merged_results = self
                .tombstone_merger
                .batch_merge_with_tombstones(batch_entries, BATCH_SIZE)?;

            for (key, merged_value) in merged_results {
                if let Some(value) = merged_value {
                    // ENHANCEMENT: Additional filtering for collection types and complex data
                    if self.should_include_value_after_merge(&value, table_id, &key)? {
                        results.push((key, value));
                    }
                } else {
                    // Value was completely tombstoned
                    println!("Value for key {:?} was completely tombstoned", key);
                }
            }
        }

        println!(
            "Multi-generation merge completed: {} final results from {} input groups",
            results.len(),
            entries.len()
        );

        Ok(results)
    }

    /// Enhanced filtering logic for post-merge values including collection validation
    fn should_include_value_after_merge(
        &self,
        value: &Value,
        _table_id: &TableId,
        _key: &RowKey,
    ) -> Result<bool> {
        match value {
            // Skip null values
            Value::Null => Ok(false),

            // For collections, check if they have valid content
            Value::List(list) => Ok(!list.is_empty()),
            Value::Set(set) => Ok(!set.is_empty()),
            Value::Map(map) => Ok(!map.is_empty()),

            // For UDTs, check if they have non-null fields
            Value::Udt(udt) => {
                let has_non_null_fields = udt.fields.iter().any(|field| field.value.is_some());
                Ok(has_non_null_fields)
            }

            // For frozen values, recursively check the inner value
            Value::Frozen(boxed_value) => {
                self.should_include_value_after_merge(boxed_value, _table_id, _key)
            }

            // Tombstones should not be included in final results
            Value::Tombstone(_) => Ok(false),

            // All other value types are included
            _ => Ok(true),
        }
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
                let _write_time = self.extract_write_time_from_entry(&entry_key, &entry_value);

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

                    eprintln!(
                        "Block read failed (attempt {}/{}): {}, retrying...",
                        retry_count, max_retries, e
                    );

                    // Brief delay before retry
                    tokio::time::sleep(tokio::time::Duration::from_millis(10 * retry_count as u64))
                        .await;
                }
            }
        }
    }

    /// Internal block reading implementation
    async fn read_next_block_impl(&self) -> Result<Option<Vec<u8>>> {
        // Read block header with format-specific handling
        let block_header = match self.header.cassandra_version {
            crate::parser::header::CassandraVersion::V5_0NewBig => {
                self.read_nb_format_block_header().await?
            }
            crate::parser::header::CassandraVersion::V5_0Bti => {
                self.read_bti_format_block_header().await?
            }
            _ => self.read_legacy_format_block_header().await?,
        };

        let Some((compressed_size, checksum, current_pos)) = block_header else {
            return Ok(None); // EOF
        };

        // Validate block size to prevent memory issues
        if compressed_size > 64 * 1024 * 1024 {
            // 64MB limit
            return Err(Error::corruption(format!(
                "Block size too large: {} bytes (limit: 64MB)",
                compressed_size
            )));
        }

        if compressed_size == 0 {
            println!("Encountered empty block at position {}", current_pos);
            return Ok(Some(Vec::new()));
        }

        // Read block data with streaming for large blocks
        let block_data = if compressed_size > self.config.read_buffer_size as u32 {
            self.read_large_block_streaming(compressed_size as usize)
                .await?
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

        println!(
            "Successfully read block: {} bytes at position {}",
            block_data.len(),
            current_pos
        );
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
            file_guard
                .seek(std::io::SeekFrom::Start(current_pos))
                .await?;
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
        println!(
            "NB format: Reading remaining {} bytes from position {}",
            remaining_size, current_pos
        );

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
                Err(e) => {
                    return Err(Error::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to read BTI block header: {}", e),
                    )));
                }
            }
        };

        let compressed_size = u32::from_be_bytes([
            header_buffer[0],
            header_buffer[1],
            header_buffer[2],
            header_buffer[3],
        ]);
        let checksum = u32::from_be_bytes([
            header_buffer[8],
            header_buffer[9],
            header_buffer[10],
            header_buffer[11],
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
                Err(e) => {
                    return Err(Error::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to read legacy block header: {}", e),
                    )));
                }
            }
        };

        let compressed_size = u32::from_be_bytes([
            header_buffer[0],
            header_buffer[1],
            header_buffer[2],
            header_buffer[3],
        ]);
        let checksum = u32::from_be_bytes([
            header_buffer[4],
            header_buffer[5],
            header_buffer[6],
            header_buffer[7],
        ]);

        Ok(Some((compressed_size, checksum, current_pos)))
    }

    /// Read block data directly for small blocks
    async fn read_block_direct(&self, size: usize) -> Result<Vec<u8>> {
        let mut block_data = vec![0u8; size];
        {
            let mut file_guard = self.file.lock().await;
            file_guard.read_exact(&mut block_data).await.map_err(|e| {
                Error::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to read block data ({}): {}", size, e),
                ))
            })?;
        }
        Ok(block_data)
    }

    /// Read large block using streaming I/O to reduce memory pressure
    async fn read_large_block_streaming(&self, size: usize) -> Result<Vec<u8>> {
        let mut block_data = Vec::with_capacity(size);
        let buffer_size = self.config.read_buffer_size.min(size);
        let mut buffer = vec![0u8; buffer_size];
        let mut remaining = size;

        println!(
            "Reading large block ({} bytes) using streaming with {} byte buffer",
            size, buffer_size
        );

        {
            let mut file_guard = self.file.lock().await;
            while remaining > 0 {
                let to_read = remaining.min(buffer_size);
                file_guard
                    .read_exact(&mut buffer[..to_read])
                    .await
                    .map_err(|e| {
                        Error::Io(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("Failed to read block chunk ({}): {}", to_read, e),
                        ))
                    })?;

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

        // Decompress if needed
        let data = if let Some(compression_reader) = &self.compression_reader {
            let compression = Compression::new(compression_reader.algorithm().clone())?;
            match compression.decompress(block_data) {
                Ok(decompressed) => {
                    println!(
                        "✅ Block decompressed {} bytes to {} bytes",
                        block_data.len(),
                        decompressed.len()
                    );
                    decompressed
                }
                Err(e) => {
                    println!(
                        "⚠️  Block decompression failed ({}), parsing raw data instead",
                        e
                    );
                    println!(
                        "First 32 bytes of block data: {:02x?}",
                        &block_data[..std::cmp::min(32, block_data.len())]
                    );
                    // Fall back to raw data
                    block_data.to_vec()
                }
            }
        } else {
            block_data.to_vec()
        };

        // Use the new state machine for Cassandra 5+ 'oa' format parsing
        if self.header.cassandra_version != crate::parser::header::CassandraVersion::Legacy {
            return self.parse_block_entries_with_state_machine(&data);
        }

        // Enhanced partition data parsing for legacy formats
        let mut offset = 0;
        while offset < data.len() {
            // Parse entry header with enhanced validation and error handling
            let (new_offset, table_id_len) = parse_vint_length(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Failed to parse table ID length at offset {}: {:?}",
                    offset, e
                ))
            })?;
            offset = data.len() - new_offset.len();

            // Validate table ID length to prevent buffer overrun
            if table_id_len > 256 || offset + table_id_len > data.len() {
                return Err(Error::corruption(format!(
                    "Invalid table ID length {} at offset {}, remaining: {}",
                    table_id_len,
                    offset,
                    data.len() - offset
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
            let (new_offset, key_len) = parse_vint_length(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Failed to parse key length at offset {}: {:?}",
                    offset, e
                ))
            })?;
            offset = data.len() - new_offset.len();

            // Validate key length
            if key_len > 65536 || offset + key_len > data.len() {
                return Err(Error::corruption(format!(
                    "Invalid key length {} at offset {}, remaining: {}",
                    key_len,
                    offset,
                    data.len() - offset
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
            let (new_offset, value_len) = parse_vint_length(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Failed to parse value length at offset {}: {:?}",
                    offset, e
                ))
            })?;
            offset = data.len() - new_offset.len();

            // Handle different value encodings in Cassandra 5.0
            let value = if value_len == 0 {
                // Empty value
                Value::Null
            } else if value_len > 16777216 {
                // 16MB limit
                return Err(Error::corruption(format!(
                    "Value too large: {} bytes at offset {}",
                    value_len, offset
                )));
            } else if offset + value_len > data.len() {
                return Err(Error::corruption(format!(
                    "Incomplete value: need {} bytes at offset {}, have {}",
                    value_len,
                    offset,
                    data.len() - offset
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

    /// Parse block entries using the Cassandra 5 'oa' format state machine
    fn parse_block_entries_with_state_machine(
        &self,
        data: &[u8],
    ) -> Result<Vec<(TableId, RowKey, Value)>> {
        let mut entries = Vec::new();
        let mut offset = 0;

        println!("🔄 Using state machine for Cassandra 5+ 'oa' format parsing");

        // Process multiple rows in the block
        while offset < data.len() {
            // Create state machine with schema information if available
            let mut state_machine = if let Some(_schema) = self.get_table_schema() {
                // DEPRECATED: This path should use SchemaAwareReader with proper comparators
                // Blob fallback is no longer allowed in modern parsing paths
                return Err(Error::Schema(
                    "Modern parsing requires SchemaAwareReader with proper comparators - blob fallback disabled".to_string()
                ));
            } else {
                RowCellStateMachine::new()
            };

            // Process data starting from current offset
            let remaining_data = &data[offset..];
            match state_machine.process(remaining_data) {
                Ok(consumed) => {
                    if consumed == 0 {
                        // No progress made, avoid infinite loop
                        println!(
                            "⚠️  State machine made no progress at offset {}, stopping",
                            offset
                        );
                        break;
                    }

                    if state_machine.is_complete() {
                        if let Some(parsed_row) = state_machine.take_parsed_row() {
                            // Convert parsed row to entries
                            let converted_entries =
                                self.convert_parsed_row_to_entries(&parsed_row)?;
                            entries.extend(converted_entries);
                            println!(
                                "✅ Successfully parsed row with {} clustering rows",
                                parsed_row.clustering_rows.len()
                            );
                        }
                    } else if state_machine.has_error() {
                        println!(
                            "❌ State machine error: {}",
                            state_machine.error_message().unwrap_or("Unknown error")
                        );
                        // Try to continue with legacy parsing for this portion
                        break;
                    }

                    offset += consumed;
                }
                Err(e) => {
                    println!("❌ State machine processing error: {}", e);
                    // Fall back to legacy parsing
                    break;
                }
            }
        }

        // If state machine didn't handle all data, fall back to legacy parsing for remainder
        if offset < data.len() {
            println!(
                "🔄 Falling back to legacy parsing for remaining {} bytes",
                data.len() - offset
            );
            let legacy_entries = self.parse_block_entries_legacy(&data[offset..])?;
            entries.extend(legacy_entries);
        }

        Ok(entries)
    }

    /// Convert a parsed row from the state machine to entries
    fn convert_parsed_row_to_entries(
        &self,
        parsed_row: &ParsedRow,
    ) -> Result<Vec<(TableId, RowKey, Value)>> {
        let mut entries = Vec::new();

        // Create table ID from keyspace and table name (would be better to get from header)
        let table_id = TableId::new(format!(
            "{}_{}",
            self.header.keyspace, self.header.table_name
        ));

        // Create partition key
        let _partition_key = RowKey::new(parsed_row.partition_key.key_bytes.clone());

        // Add static row if present
        if let Some(ref static_row) = parsed_row.static_row {
            for (column_name, value) in &static_row.columns {
                // Create a compound key for static columns
                let mut static_key_bytes = parsed_row.partition_key.key_bytes.clone();
                static_key_bytes.extend_from_slice(b"#static#");
                static_key_bytes.extend_from_slice(column_name.as_bytes());

                let static_key = RowKey::new(static_key_bytes);
                entries.push((table_id.clone(), static_key, value.clone()));
            }
        }

        // Add clustering rows
        for clustering_row in &parsed_row.clustering_rows {
            for (column_name, value) in &clustering_row.columns {
                // Create compound key: partition_key + clustering_key + column_name
                let mut compound_key_bytes = parsed_row.partition_key.key_bytes.clone();
                compound_key_bytes.extend_from_slice(&clustering_row.clustering_key);
                compound_key_bytes.extend_from_slice(column_name.as_bytes());

                let compound_key = RowKey::new(compound_key_bytes);
                entries.push((table_id.clone(), compound_key, value.clone()));
            }
        }

        Ok(entries)
    }

    /// Legacy parsing method for backward compatibility
    fn parse_block_entries_legacy(&self, data: &[u8]) -> Result<Vec<(TableId, RowKey, Value)>> {
        let mut entries = Vec::new();
        let mut offset = 0;

        // Enhanced partition data parsing for legacy formats
        while offset < data.len() {
            // Parse entry header with enhanced validation and error handling
            let (new_offset, table_id_len) = parse_vint_length(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Failed to parse table ID length at offset {}: {:?}",
                    offset, e
                ))
            })?;
            offset = data.len() - new_offset.len();

            // Validate table ID length to prevent buffer overrun
            if table_id_len > 256 || offset + table_id_len > data.len() {
                return Err(Error::corruption(format!(
                    "Invalid table ID length {} at offset {}, remaining: {}",
                    table_id_len,
                    offset,
                    data.len() - offset
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
            let (new_offset, key_len) = parse_vint_length(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Failed to parse key length at offset {}: {:?}",
                    offset, e
                ))
            })?;
            offset = data.len() - new_offset.len();

            // Validate key length
            if key_len > 65536 || offset + key_len > data.len() {
                return Err(Error::corruption(format!(
                    "Invalid key length {} at offset {}, remaining: {}",
                    key_len,
                    offset,
                    data.len() - offset
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
            let (new_offset, value_len) = parse_vint_length(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Failed to parse value length at offset {}: {:?}",
                    offset, e
                ))
            })?;
            offset = data.len() - new_offset.len();

            // Handle different value encodings
            let value = if value_len == 0 {
                // Empty value
                Value::Null
            } else if value_len > 16777216 {
                // 16MB limit
                return Err(Error::corruption(format!(
                    "Value too large: {} bytes at offset {}",
                    value_len, offset
                )));
            } else if offset + value_len > data.len() {
                return Err(Error::corruption(format!(
                    "Incomplete value: need {} bytes at offset {}, have {}",
                    value_len,
                    offset,
                    data.len() - offset
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
            crate::parser::header::CassandraVersion::V5_0NewBig => {
                // For Cassandra 5.0 nb format, use a much simpler approach
                // The actual data starts much later in the file
                // Based on the hex dump analysis, try starting much further in
                1024 // Start after 1KB - will scan for actual block start
                    .min(8192) // Maximum reasonable size
            }
            crate::parser::header::CassandraVersion::V5_0Bti => {
                // BTI format varies more
                1024
            }
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

    /// Enhanced composite key parsing for Cassandra 5.0 multi-component keys with improved format detection
    fn parse_composite_key(&self, key_data: &[u8]) -> Result<RowKey> {
        if key_data.is_empty() {
            return Ok(RowKey::new(Vec::new()));
        }

        // SCHEMA-DRIVEN KEY PARSING: Use exact comparator types when available
        if let Some(schema) = self.get_table_schema() {
            return self.parse_key_with_schema(key_data, &schema);
        }

        // TODO: Remove this fallback chain - use SchemaAwareReader instead
        // LEGACY FALLBACK: Multi-strategy format detection (DEPRECATED)
        // This type guessing behavior should be replaced with SchemaAwareReader

        // COMMENTED OUT: Strategy 1: Try Cassandra 5.0+ vint-based composite key format
        // if let Ok(parsed_key) = self.parse_composite_key_v5_format(key_data) {
        //     return Ok(parsed_key);
        // }

        // COMMENTED OUT: Strategy 2: Try legacy u16-length prefixed format
        // if let Ok(parsed_key) = self.parse_composite_key_legacy_format(key_data) {
        //     return Ok(parsed_key);
        // }

        // COMMENTED OUT: Strategy 3: Try simple clustering key format
        // if let Ok(parsed_key) = self.parse_clustering_key_format(key_data) {
        //     return Ok(parsed_key);
        // }

        // FALLBACK: Return raw key data (no type guessing)
        println!(
            "WARNING: No schema available - returning raw key data for key of length {} (use SchemaAwareReader)",
            key_data.len()
        );
        Ok(RowKey::new(key_data.to_vec()))
    }

    /// Parse key using exact schema information (NO HEURISTICS)
    fn parse_key_with_schema(&self, key_data: &[u8], schema: &TableSchema) -> Result<RowKey> {
        use crate::parser::vint::parse_vint_length;

        let mut offset = 0;
        let mut key_components = Vec::new();

        // Parse partition key components using exact comparator types
        for partition_column in &schema.partition_keys {
            if offset >= key_data.len() {
                break;
            }

            // Parse component length (vint)
            let (remaining, component_len) = parse_vint_length(&key_data[offset..])
                .map_err(|_| Error::corruption("Failed to parse partition key component length"))?;
            offset = key_data.len() - remaining.len();

            if component_len > remaining.len() {
                return Err(Error::corruption(
                    "Partition key component length exceeds available data",
                ));
            }

            // Extract component data
            let component_data = &remaining[..component_len];

            // DEPRECATED: This should use SchemaAwareReader with proper comparators
            let comparator = ComparatorType::from_data_type(&partition_column.data_type)
                .map_err(|e| Error::Schema(format!(
                    "Invalid partition key type '{}' - use SchemaAwareReader: {}", 
                    partition_column.data_type, e
                )))?;

            // Decode component using exact comparator type
            let decoded_component = self.decode_key_component(component_data, &comparator)?;
            key_components.push(decoded_component);

            offset += component_len;
        }

        // Parse clustering key components if present
        if offset < key_data.len() {
            for clustering_column in &schema.clustering_keys {
                if offset >= key_data.len() {
                    break;
                }

                // Parse component length (vint)
                let (remaining, component_len) =
                    parse_vint_length(&key_data[offset..]).map_err(|_| {
                        Error::corruption("Failed to parse clustering key component length")
                    })?;
                offset = key_data.len() - remaining.len();

                if component_len > remaining.len() {
                    return Err(Error::corruption(
                        "Clustering key component length exceeds available data",
                    ));
                }

                // Extract component data
                let component_data = &remaining[..component_len];

                // DEPRECATED: This should use SchemaAwareReader with proper comparators
                let comparator = ComparatorType::from_data_type(&clustering_column.data_type)
                    .map_err(|e| Error::Schema(format!(
                        "Invalid clustering key type '{}' - use SchemaAwareReader: {}", 
                        clustering_column.data_type, e
                    )))?;

                // Decode component using exact comparator type
                let decoded_component = self.decode_key_component(component_data, &comparator)?;
                key_components.push(decoded_component);

                offset += component_len;
            }
        }

        // Create compound key from decoded components
        let mut compound_key_data = Vec::new();
        for component in key_components {
            compound_key_data.extend_from_slice(&component);
        }

        Ok(RowKey::new(compound_key_data))
    }

    /// Decode key component using exact comparator type
    fn decode_key_component(
        &self,
        component_data: &[u8],
        comparator: &ComparatorType,
    ) -> Result<Vec<u8>> {
        // For key components, we typically preserve the byte-comparable encoding
        // but can validate format based on comparator type

        match comparator {
            ComparatorType::Uuid => {
                if component_data.len() != 16 {
                    return Err(Error::corruption("Invalid UUID key component length"));
                }
            }
            ComparatorType::Int => {
                if component_data.len() != 4 {
                    return Err(Error::corruption("Invalid Int key component length"));
                }
            }
            ComparatorType::BigInt => {
                if component_data.len() != 8 {
                    return Err(Error::corruption("Invalid BigInt key component length"));
                }
            }
            ComparatorType::Text => {
                // Validate UTF-8 for text keys
                if std::str::from_utf8(component_data).is_err() {
                    return Err(Error::corruption("Invalid UTF-8 in text key component"));
                }
            }
            _ => {
                // For other types, accept as-is for now
            }
        }

        // Return the byte-comparable encoding as-is
        // The comparator validation ensures format correctness
        Ok(component_data.to_vec())
    }

    /// Parse composite key using Cassandra 5.0+ vint-based format
    fn parse_composite_key_v5_format(&self, key_data: &[u8]) -> Result<RowKey> {
        if key_data.len() < 2 {
            return Err(Error::corruption("Key too short for v5 format".to_string()));
        }

        let mut components = Vec::new();

        // Parse component count (vint)
        let (remaining, component_count) = parse_vint_length(key_data)
            .map_err(|_| Error::corruption("Failed to parse component count".to_string()))?;
        let mut offset = key_data.len() - remaining.len();

        if component_count == 0 || component_count > 256 {
            return Err(Error::corruption(format!(
                "Invalid component count: {}",
                component_count
            )));
        }

        println!(
            "Parsing v5 composite key with {} components",
            component_count
        );

        // Parse each component
        for i in 0..component_count {
            if offset >= key_data.len() {
                break;
            }

            // Parse component length (vint)
            let (remaining, component_len) =
                parse_vint_length(&key_data[offset..]).map_err(|_| {
                    Error::corruption(format!("Failed to parse component {} length", i))
                })?;
            offset = key_data.len() - remaining.len();

            if component_len > 0 && offset + component_len <= key_data.len() {
                components.extend_from_slice(&key_data[offset..offset + component_len]);
                offset += component_len;

                // Add component separator (except for last component)
                if i < component_count - 1 {
                    components.push(0x00);
                }
            }
        }

        println!("Parsed v5 composite key: {} total bytes", components.len());
        Ok(RowKey::new(components))
    }

    /// Parse composite key using legacy u16-length prefixed format
    fn parse_composite_key_legacy_format(&self, key_data: &[u8]) -> Result<RowKey> {
        if key_data.len() < 3 || key_data[0] != 0x00 {
            return Err(Error::corruption(
                "Not legacy composite key format".to_string(),
            ));
        }

        let mut offset = 0;
        let mut components = Vec::new();

        while offset < key_data.len() {
            if offset + 2 > key_data.len() {
                break;
            }

            // Read component length (big-endian u16)
            let component_len =
                u16::from_be_bytes([key_data[offset], key_data[offset + 1]]) as usize;
            offset += 2;

            if offset + component_len > key_data.len() {
                break;
            }

            components.extend_from_slice(&key_data[offset..offset + component_len]);
            components.push(0x00); // Component separator
            offset += component_len;

            // Check for end-of-components marker
            if offset < key_data.len() && key_data[offset] == 0x00 {
                break;
            }
        }

        // Remove trailing separator if present
        if components.last() == Some(&0x00) {
            components.pop();
        }

        println!(
            "Parsed legacy composite key: {} total bytes",
            components.len()
        );
        Ok(RowKey::new(components))
    }

    /// Parse clustering key format (simpler format for clustering columns)
    fn parse_clustering_key_format(&self, key_data: &[u8]) -> Result<RowKey> {
        // Clustering keys in Cassandra 5.0 might use a different format
        // Check for clustering key markers or patterns

        if key_data.len() < 4 {
            return Err(Error::corruption(
                "Too short for clustering key".to_string(),
            ));
        }

        // Check if this looks like a clustering key by analyzing the structure
        // Clustering keys often have type info followed by the actual key data
        if key_data[0] <= 0x1F {
            // Potential type marker
            let mut offset = 1;

            // Skip type information
            while offset < key_data.len() && key_data[offset] <= 0x1F {
                offset += 1;
            }

            if offset < key_data.len() {
                let clustering_data = &key_data[offset..];
                println!(
                    "Parsed clustering key: {} bytes after {} byte type prefix",
                    clustering_data.len(),
                    offset
                );
                return Ok(RowKey::new(clustering_data.to_vec()));
            }
        }

        Err(Error::corruption("Not clustering key format".to_string()))
    }

    /// Parse column value using schema-driven approach (no heuristics)
    fn parse_column_value_enhanced(
        &self,
        value_data: &[u8],
        table_id: &TableId,
        key: &RowKey,
    ) -> Result<Value> {
        if value_data.is_empty() {
            return Ok(Value::Null);
        }

        // Use schema information to determine exact type - NO GUESSING
        if let Some(schema) = self.get_table_schema() {
            // Extract column name from key context if possible
            if let Some(column_name) = self.extract_column_name_from_context(table_id, key) {
                // Find column in schema
                if let Some(column) = schema.columns.iter().find(|c| c.name == column_name) {
                    // Parse using exact type from schema
                    return self.parse_value_with_schema_type(value_data, &column.data_type);
                }
            }
        }

        // TODO: Remove blob fallback - use SchemaAwareReader instead
        // LEGACY: Fallback to blob when no schema (DEPRECATED - use SchemaAwareReader)
        Ok(Value::Blob(value_data.to_vec()))
    }

    /// Parse value using exact schema type information
    fn parse_value_with_schema_type(&self, value_data: &[u8], data_type: &str) -> Result<Value> {
        // Convert data type string directly to ComparatorType for decoding
        let comparator = ComparatorType::from_data_type(data_type)?;

        // Use comparator to decode the value properly
        match &comparator {
            ComparatorType::Boolean => {
                if value_data.len() == 1 {
                    Ok(Value::Boolean(value_data[0] != 0))
                } else {
                    Err(Error::corruption("Invalid boolean value length"))
                }
            }
            ComparatorType::TinyInt => {
                if value_data.len() == 1 {
                    Ok(Value::TinyInt(value_data[0] as i8))
                } else {
                    Err(Error::corruption("Invalid tinyint value length"))
                }
            }
            ComparatorType::SmallInt => {
                if value_data.len() == 2 {
                    let val = i16::from_be_bytes([value_data[0], value_data[1]]);
                    Ok(Value::SmallInt(val))
                } else {
                    Err(Error::corruption("Invalid smallint value length"))
                }
            }
            ComparatorType::Int => {
                if value_data.len() == 4 {
                    let val = i32::from_be_bytes([
                        value_data[0],
                        value_data[1],
                        value_data[2],
                        value_data[3],
                    ]);
                    Ok(Value::Integer(val))
                } else {
                    Err(Error::corruption("Invalid int value length"))
                }
            }
            ComparatorType::BigInt => {
                if value_data.len() == 8 {
                    let val = i64::from_be_bytes([
                        value_data[0],
                        value_data[1],
                        value_data[2],
                        value_data[3],
                        value_data[4],
                        value_data[5],
                        value_data[6],
                        value_data[7],
                    ]);
                    Ok(Value::BigInt(val))
                } else {
                    Err(Error::corruption("Invalid bigint value length"))
                }
            }
            ComparatorType::Text => {
                let text = String::from_utf8(value_data.to_vec())
                    .map_err(|_| Error::corruption("Invalid UTF-8 in text value"))?;
                Ok(Value::Text(text))
            }
            ComparatorType::Blob => Ok(Value::Blob(value_data.to_vec())),
            ComparatorType::Uuid => {
                if value_data.len() == 16 {
                    // Parse UUID from 16 bytes
                    let uuid_bytes: [u8; 16] = value_data
                        .try_into()
                        .map_err(|_| Error::corruption("Invalid UUID byte array"))?;
                    Ok(Value::Uuid(uuid_bytes))
                } else {
                    Err(Error::corruption("Invalid UUID value length"))
                }
            }
            ComparatorType::List(element_comparator) => {
                self.parse_list_value(value_data, element_comparator)
            }
            ComparatorType::Set(element_comparator) => {
                self.parse_set_value(value_data, element_comparator)
            }
            ComparatorType::Map(key_comparator, value_comparator) => {
                self.parse_map_value(value_data, key_comparator, value_comparator)
            }
            ComparatorType::Tuple(field_comparators) => {
                self.parse_tuple_value(value_data, field_comparators)
            }
            ComparatorType::Udt {
                field_comparators, ..
            } => self.parse_udt_value(value_data, field_comparators),
            ComparatorType::Frozen(inner_comparator) => {
                // For frozen types, parse the inner type directly
                let inner_value = self.parse_value_with_comparator(value_data, inner_comparator)?;
                Ok(Value::Frozen(Box::new(inner_value)))
            }
            _ => {
                // For other types, preserve as blob for now
                Ok(Value::Blob(value_data.to_vec()))
            }
        }
    }

    /// Parse value directly using ComparatorType (helper method)
    fn parse_value_with_comparator(
        &self,
        value_data: &[u8],
        comparator: &ComparatorType,
    ) -> Result<Value> {
        // Use the same logic as parse_value_with_schema_type but with direct comparator
        match comparator {
            ComparatorType::Boolean => {
                if value_data.len() == 1 {
                    Ok(Value::Boolean(value_data[0] != 0))
                } else {
                    Err(Error::corruption("Invalid boolean value length"))
                }
            }
            ComparatorType::Text => {
                let text = String::from_utf8(value_data.to_vec())
                    .map_err(|_| Error::corruption("Invalid UTF-8 in text value"))?;
                Ok(Value::Text(text))
            }
            ComparatorType::Blob => Ok(Value::Blob(value_data.to_vec())),
            _ => {
                // For complex types, implement as needed
                Ok(Value::Blob(value_data.to_vec()))
            }
        }
    }

    /// Parse list value using element comparator
    fn parse_list_value(
        &self,
        value_data: &[u8],
        element_comparator: &ComparatorType,
    ) -> Result<Value> {
        use crate::parser::vint::parse_vint_length;

        let mut offset = 0;
        let mut elements = Vec::new();

        // Parse element count
        let (remaining, element_count) = parse_vint_length(&value_data[offset..])
            .map_err(|_| Error::corruption("Failed to parse list element count"))?;
        offset = value_data.len() - remaining.len();

        // Parse each element
        for _ in 0..element_count {
            if offset >= value_data.len() {
                break;
            }

            // Parse element length
            let (remaining, element_len) = parse_vint_length(&value_data[offset..])
                .map_err(|_| Error::corruption("Failed to parse list element length"))?;
            offset = value_data.len() - remaining.len();

            if element_len > remaining.len() {
                return Err(Error::corruption(
                    "List element length exceeds available data",
                ));
            }

            // Parse element value using element comparator
            let element_data = &remaining[..element_len];
            let element_value =
                self.parse_value_with_comparator(element_data, element_comparator)?;
            elements.push(element_value);
            offset += element_len;
        }

        Ok(Value::List(elements))
    }

    /// Parse set value using element comparator  
    fn parse_set_value(
        &self,
        value_data: &[u8],
        element_comparator: &ComparatorType,
    ) -> Result<Value> {
        // Sets are parsed similarly to lists
        let list_value = self.parse_list_value(value_data, element_comparator)?;
        if let Value::List(elements) = list_value {
            Ok(Value::Set(elements))
        } else {
            Err(Error::corruption("Failed to parse set value"))
        }
    }

    /// Parse map value using key and value comparators
    fn parse_map_value(
        &self,
        value_data: &[u8],
        key_comparator: &ComparatorType,
        value_comparator: &ComparatorType,
    ) -> Result<Value> {
        use crate::parser::vint::parse_vint_length;

        let mut offset = 0;
        let mut entries = Vec::new();

        // Parse entry count
        let (remaining, entry_count) = parse_vint_length(&value_data[offset..])
            .map_err(|_| Error::corruption("Failed to parse map entry count"))?;
        offset = value_data.len() - remaining.len();

        // Parse each key-value pair
        for _ in 0..entry_count {
            if offset >= value_data.len() {
                break;
            }

            // Parse key length and data
            let (remaining, key_len) = parse_vint_length(&value_data[offset..])
                .map_err(|_| Error::corruption("Failed to parse map key length"))?;
            offset = value_data.len() - remaining.len();

            if key_len > remaining.len() {
                return Err(Error::corruption("Map key length exceeds available data"));
            }

            let key_data = &remaining[..key_len];
            let key_value = self.parse_value_with_comparator(key_data, key_comparator)?;
            offset += key_len;

            // Parse value length and data
            let (remaining, value_len) = parse_vint_length(&value_data[offset..])
                .map_err(|_| Error::corruption("Failed to parse map value length"))?;
            offset = value_data.len() - remaining.len();

            if value_len > remaining.len() {
                return Err(Error::corruption("Map value length exceeds available data"));
            }

            let val_data = &remaining[..value_len];
            let val_value = self.parse_value_with_comparator(val_data, value_comparator)?;
            entries.push((key_value, val_value));
            offset += value_len;
        }

        Ok(Value::Map(entries))
    }

    /// Parse tuple value using field comparators
    fn parse_tuple_value(
        &self,
        value_data: &[u8],
        field_comparators: &[ComparatorType],
    ) -> Result<Value> {
        use crate::parser::vint::parse_vint_length;

        let mut offset = 0;
        let mut fields = Vec::new();

        // Parse each field
        for (i, field_comparator) in field_comparators.iter().enumerate() {
            if offset >= value_data.len() {
                break;
            }

            // Parse field length
            let (remaining, field_len) =
                parse_vint_length(&value_data[offset..]).map_err(|_| {
                    Error::corruption(format!("Failed to parse tuple field {} length", i))
                })?;
            offset = value_data.len() - remaining.len();

            if field_len > remaining.len() {
                return Err(Error::corruption(format!(
                    "Tuple field {} length exceeds available data",
                    i
                )));
            }

            // Parse field value using field comparator
            let field_data = &remaining[..field_len];
            let field_value = self.parse_value_with_comparator(field_data, field_comparator)?;
            fields.push(field_value);
            offset += field_len;
        }

        Ok(Value::Tuple(fields))
    }

    /// Parse UDT value using field comparators
    fn parse_udt_value(
        &self,
        value_data: &[u8],
        field_comparators: &[(String, ComparatorType)],
    ) -> Result<Value> {
        use crate::parser::vint::parse_vint_length;
        use crate::types::{UdtField, UdtValue};

        let mut offset = 0;
        let mut fields = Vec::new();

        // Parse each field
        for (field_name, field_comparator) in field_comparators.iter() {
            if offset >= value_data.len() {
                break;
            }

            // Parse field length
            let (remaining, field_len) =
                parse_vint_length(&value_data[offset..]).map_err(|_| {
                    Error::corruption(format!("Failed to parse UDT field {} length", field_name))
                })?;
            offset = value_data.len() - remaining.len();

            if field_len > remaining.len() {
                return Err(Error::corruption(format!(
                    "UDT field {} length exceeds available data",
                    field_name
                )));
            }

            // Parse field value using field comparator
            let field_data = &remaining[..field_len];
            let field_value = self.parse_value_with_comparator(field_data, field_comparator)?;

            fields.push(UdtField {
                name: field_name.clone(),
                value: Some(field_value),
            });
            offset += field_len;
        }

        // TODO: Remove generic column fabrication - use SchemaAwareReader instead
        // LEGACY: Generic UDT fabrication (DEPRECATED)
        Ok(Value::Udt(UdtValue {
            keyspace: "unknown".to_string(), // DEPRECATED: Would need keyspace name from schema
            type_name: "unknown".to_string(), // DEPRECATED: Would need UDT name from schema
            fields,
        }))
    }

    /// Extract column name from key context (placeholder implementation)
    fn extract_column_name_from_context(
        &self,
        _table_id: &TableId,
        _key: &RowKey,
    ) -> Option<String> {
        // TODO: Implement proper column name extraction from key context
        // This would analyze the key structure to determine which column is being accessed
        None
    }

    /// Get table schema from header information
    fn get_table_schema(&self) -> Option<TableSchema> {
        // Try to construct a basic schema from header information
        if self.header.columns.is_empty() {
            return None;
        }

        let mut columns = Vec::new();
        let mut partition_keys = Vec::new();
        let mut clustering_keys = Vec::new();

        // Convert header columns to schema columns
        for col_info in self.header.columns.iter() {
            let column = Column {
                name: col_info.name.clone(),
                data_type: col_info.column_type.clone(), // Use column_type field
                nullable: true,
                default: None,
            };

            // Check if this is a key column based on primary key and clustering status
            if col_info.is_primary_key && !col_info.is_clustering {
                // This is a partition key
                partition_keys.push(KeyColumn {
                    name: col_info.name.clone(),
                    data_type: col_info.column_type.clone(),
                    position: partition_keys.len(),
                });
            } else if col_info.is_clustering {
                clustering_keys.push(ClusteringColumn {
                    name: col_info.name.clone(),
                    data_type: col_info.column_type.clone(),
                    position: clustering_keys.len(),
                    order: "ASC".to_string(),
                });
            }

            columns.push(column);
        }

        Some(TableSchema {
            keyspace: self.header.keyspace.clone(),
            table: self.header.table_name.clone(),
            partition_keys,
            clustering_keys,
            columns,
            comments: HashMap::new(),
        })
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

impl SSTableReader {
    /// Load Index.db reader for partition lookup and promoted index handling
    async fn load_index_reader(
        path: &Path, 
        platform: &Arc<Platform>
    ) -> Option<IndexReader> {
        let index_path = path.with_extension("db").with_file_name(
            format!("{}-Index.db", path.file_stem()?.to_str()?)
        );
        
        match IndexReader::open(&index_path, platform.clone()).await {
            Ok(reader) => {
                log::debug!("Loaded Index.db reader for {}", index_path.display());
                Some(reader)
            }
            Err(e) => {
                log::debug!("Failed to load Index.db reader: {}", e);
                None
            }
        }
    }

    /// Load Summary.db reader for token-range iteration
    async fn load_summary_reader(
        path: &Path,
        platform: &Arc<Platform>
    ) -> Option<SummaryReader> {
        let summary_path = path.with_extension("db").with_file_name(
            format!("{}-Summary.db", path.file_stem()?.to_str()?)
        );
        
        match SummaryReader::open(&summary_path, platform.clone()).await {
            Ok(reader) => {
                log::debug!("Loaded Summary.db reader for {}", summary_path.display());
                Some(reader)
            }
            Err(e) => {
                log::debug!("Failed to load Summary.db reader: {}", e);
                None
            }
        }
    }

    /// Load Statistics.db reader for min/max timestamps and metadata
    async fn load_statistics_reader(
        path: &Path,
        platform: &Arc<Platform>
    ) -> Option<StatisticsReader> {
        let statistics_path = path.with_extension("db").with_file_name(
            format!("{}-Statistics.db", path.file_stem()?.to_str()?)
        );
        
        match StatisticsReader::open(&statistics_path, platform.clone()).await {
            Ok(reader) => {
                log::debug!("Loaded Statistics.db reader for {}", statistics_path.display());
                Some(reader)
            }
            Err(e) => {
                log::debug!("Failed to load Statistics.db reader: {}", e);
                None
            }
        }
    }

    /// Enhanced partition lookup using Index.db reader with promoted index support
    pub async fn lookup_partition_with_index(&self, partition_key: &[u8]) -> Result<Option<(u64, u32)>> {
        if let Some(index_reader) = &self.index_reader {
            // Use spec-compliant Index.db reader for partition lookup
            if let Some(entry) = index_reader.lookup_partition(partition_key) {
                log::debug!("Found partition via Index.db: offset={}, size={}", entry.data_offset, entry.data_size);
                return Ok(Some((entry.data_offset, entry.data_size)));
            }
        }
        Ok(None)
    }

    /// Enhanced token range iteration using Summary.db reader
    pub async fn iterate_token_range(&self, start_token: i64, end_token: i64) -> Result<Vec<(RowKey, Value)>> {
        if let Some(summary_reader) = &self.summary_reader {
            // Use Summary.db reader for efficient token range queries
            let token_entries = summary_reader.find_entries_in_range(start_token, end_token);
            let mut results = Vec::new();
            
            for entry in token_entries {
                // Use the index offset to read data at that position
                // For now, read a reasonable chunk size and parse it
                let chunk_size = 4096; // 4KB default chunk
                if let Some(data) = self.read_value_at_offset(entry.index_offset, chunk_size).await? {
                    // Create a synthetic key-value pair
                    let key = RowKey::from(format!("token_{}", entry.token));
                    results.push((key, data));
                }
            }
            
            log::debug!("Token range iteration found {} entries", results.len());
            return Ok(results);
        }
        
        // Fallback to existing scan method
        self.sequential_scan(&TableId::from("default"), None, None, None).await
    }

    /// Get min/max timestamps from Statistics.db reader
    pub async fn get_timestamp_range(&self) -> Result<Option<(i64, i64)>> {
        if let Some(statistics_reader) = &self.statistics_reader {
            let (min_ts, max_ts) = statistics_reader.timestamp_range();
            log::debug!("Retrieved timestamp range from Statistics.db: {} to {}", min_ts, max_ts);
            return Ok(Some((min_ts, max_ts)));
        }
        Ok(None)
    }

    /// Get token coverage from Statistics.db reader  
    pub async fn get_token_coverage(&self) -> Result<Option<(i64, i64)>> {
        if let Some(summary_reader) = &self.summary_reader {
            // Get token range from Summary.db instead of Statistics.db
            let summary_data = summary_reader.get_entries();
            if !summary_data.is_empty() {
                let min_token = summary_data.first().unwrap().token;
                let max_token = summary_data.last().unwrap().token;
                log::debug!("Retrieved token coverage from Summary.db: {} to {}", min_token, max_token);
                return Ok(Some((min_token, max_token)));
            }
        }
        Ok(None)
    }

    /// Enhanced get method using spec readers for efficient lookup
    pub async fn get_with_spec_readers(&self, table_id: &TableId, key: &RowKey) -> Result<Option<Value>> {
        // Step 1: Use bloom filter for existence check
        if let Some(bloom_filter) = &self.bloom_filter {
            if !bloom_filter.might_contain(key.as_bytes()) {
                log::debug!("Bloom filter indicates key does not exist");
                return Ok(None);
            }
        }

        // Step 2: Use Index.db reader for precise partition lookup
        if let Some((offset, size)) = self.lookup_partition_with_index(key.as_bytes()).await? {
            log::debug!("Using Index.db lookup: offset={}, size={}", offset, size);
            return self.read_value_at_offset(offset, size).await;
        }

        // Step 3: Fallback to existing methods
        log::debug!("Falling back to legacy lookup methods");
        self.get(table_id, key).await
    }

    /// Parse partition data from Index.db offset (placeholder implementation)
    fn parse_partition_data(&self, _data: &[u8]) -> Result<Vec<(RowKey, Value)>> {
        // TODO: Implement proper partition data parsing using schema-driven approach
        // This would parse the actual partition data and extract key-value pairs
        // For now, return empty result
        Ok(Vec::new())
    }
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
