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
    compression::{Compression, CompressionReader},
    index::SSTableIndex,
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
            file_guard.seek(std::io::SeekFrom::Start(header_size as u64)).await?;
        }

        // Initialize compression reader if needed
        let compression_reader = if header.compression.algorithm != "NONE" {
            Some(CompressionReader::new(
                header.compression.algorithm.clone(),
                header.compression.chunk_size,
                header.compression.parameters.clone(),
            )?)
        } else {
            None
        };

        // Load index if available
        let index = Self::load_index(&mut file, &header, &platform).await?;

        // Load bloom filter if available
        let bloom_filter = Self::load_bloom_filter(&mut file, &header, &platform).await?;

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
        })
    }

    /// Get a value by key from the SSTable
    pub async fn get(&mut self, table_id: &TableId, key: &RowKey) -> Result<Option<Value>> {
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
            let entries = index
                .get_range(table_id, start_key, end_key)?;

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
    pub async fn get_all_entries(&mut self) -> Result<Vec<(TableId, RowKey, Value)>> {
        let mut results = Vec::new();

        // Reset to beginning of data section  
        let header_size = self.calculate_header_size();
        self.file.seek(std::io::SeekFrom::Start(header_size as u64))
            .await?;

        // Read all blocks sequentially
        while let Some(block) = self.read_next_block(&mut self.file).await? {
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

    async fn load_index(
        file: &mut BufReader<File>,
        header: &SSTableHeader,
        platform: &Platform,
    ) -> Result<Option<SSTableIndex>> {
        // Check if index information is available in header
        if let Some(index_offset) = header.properties.get("index_offset") {
            let offset: u64 = index_offset
                .parse()
                .map_err(|_| Error::corruption("Invalid index offset in header"))?;

            // Load index from file
            file.seek(std::io::SeekFrom::Start(offset)).await?;
            let index = SSTableIndex::load(file).await?;
            return Ok(Some(index));
        }

        Ok(None)
    }

    async fn load_bloom_filter(
        file: &mut BufReader<File>,
        header: &SSTableHeader,
        platform: &Platform,
    ) -> Result<Option<BloomFilter>> {
        // Check if bloom filter information is available in header
        if let Some(bloom_offset) = header.properties.get("bloom_filter_offset") {
            let offset: u64 = bloom_offset
                .parse()
                .map_err(|_| Error::corruption("Invalid bloom filter offset in header"))?;

            // Load bloom filter from file
            file.seek(std::io::SeekFrom::Start(offset)).await?;
            let bloom_filter = BloomFilter::load(file).await?;
            return Ok(Some(bloom_filter));
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

        Ok(Some(value))
    }

    async fn scan_for_key(&mut self, table_id: &TableId, key: &RowKey) -> Result<Option<Value>> {
        let header_size = self.calculate_header_size();
        self.file.seek(std::io::SeekFrom::Start(header_size as u64))
            .await?;

        // Sequential scan through blocks
        while let Some(block) = self.read_next_block(&mut self.file).await? {
            let entries = self.parse_block_entries(&block)?;

            for (entry_table_id, entry_key, entry_value) in entries {
                if entry_table_id == *table_id && entry_key == *key {
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
        self.file.seek(std::io::SeekFrom::Start(header_size as u64))
            .await?;

        // Sequential scan through blocks
        while let Some(block) = self.read_next_block(&mut self.file).await? {
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

    async fn read_next_block(&self, file: &mut BufReader<File>) -> Result<Option<Vec<u8>>> {
        // Read block header (offset, size, checksum)
        let mut header_buffer = [0u8; 16]; // 8 bytes offset + 4 bytes size + 4 bytes checksum
        match file.read_exact(&mut header_buffer).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(None); // End of file
            }
            Err(e) => return Err(e.into()),
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
        file.read_exact(&mut block_data).await?;

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

        // Parse entries from block
        while offset < data.len() {
            // Parse entry header: table_id_length, table_id, key_length, key, value_length, value
            let (new_offset, table_id_len) = parse_vint_length(&data[offset..]).map_err(|e| {
                Error::corruption(format!("Failed to parse table ID length: {:?}", e))
            })?;
            offset += data.len() - new_offset.len();

            if offset + table_id_len > data.len() {
                break; // Incomplete entry
            }

            let table_id = TableId::new(String::from_utf8_lossy(
                &data[offset..offset + table_id_len],
            ));
            offset += table_id_len;

            let (new_offset, key_len) = parse_vint_length(&data[offset..])
                .map_err(|e| Error::corruption(format!("Failed to parse key length: {:?}", e)))?;
            offset += data.len() - new_offset.len();

            if offset + key_len > data.len() {
                break; // Incomplete entry
            }

            let key = RowKey::new(data[offset..offset + key_len].to_vec());
            offset += key_len;

            let (new_offset, value_len) = parse_vint_length(&data[offset..])
                .map_err(|e| Error::corruption(format!("Failed to parse value length: {:?}", e)))?;
            offset += data.len() - new_offset.len();

            if offset + value_len > data.len() {
                break; // Incomplete entry
            }

            // Parse value based on type (simplified - should use proper type information)
            let (_, value) = parse_cql_value(&data[offset..offset + value_len], CqlTypeId::Varchar)
                .map_err(|e| Error::corruption(format!("Failed to parse value: {:?}", e)))?;
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
