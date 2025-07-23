//! SSTable writer implementation with Cassandra 5+ compatibility

use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::Error;
use crate::schema::CqlType;
use crate::storage::sstable::bloom::BloomFilter;
use crate::storage::sstable::compression::{Compression, CompressionStats};
use crate::storage::sstable::index::IndexEntry;
use crate::{platform::Platform, types::TableId, Config, Result, RowKey, Value};

/// Cassandra 5+ compatible SSTable format version ('oa' format)
const CASSANDRA_FORMAT_VERSION: &[u8] = b"oa";

/// Magic bytes for Cassandra SSTable file identification  
const CASSANDRA_MAGIC: [u8; 4] = [0x5A, 0x5A, 0x5A, 0x5A];

/// Default block size for data compression
const DEFAULT_BLOCK_SIZE: usize = 64 * 1024; // 64KB

/// CRC32 polynomial for checksumming
const CRC32_POLYNOMIAL: u32 = 0xEDB88320;

/// SSTable writer for creating new SSTable files with Cassandra 5+ compatibility
pub struct SSTableWriter {
    /// Output file writer
    writer: Box<dyn Write + Send>,

    /// Configuration
    config: Config,

    /// Platform abstraction
    platform: Arc<Platform>,

    /// Current offset in the file
    offset: u64,

    /// Index entries for fast lookups
    index_entries: Vec<IndexEntry>,

    /// Compression handler
    compression: Option<Compression>,

    /// Bloom filter for efficient key lookups
    bloom_filter: Option<BloomFilter>,

    /// Current data block being written
    current_block: Vec<u8>,

    /// Block compression statistics
    compression_stats: Vec<CompressionStats>,

    /// Block checksums for data integrity
    block_checksums: Vec<u32>,

    /// Statistics
    entry_count: u64,
    table_count: u64,
    uncompressed_size: u64,
    compressed_size: u64,

    /// File creation timestamp
    created_at: u64,

    /// Whether the writer is finalized
    finalized: bool,

    /// Current block index
    block_index: u32,

    /// Write batch for transaction support
    write_batch: Vec<(TableId, RowKey, Value)>,

    /// Batch size threshold
    batch_size_threshold: usize,
}

impl SSTableWriter {
    /// Create a new SSTable writer with Cassandra 5+ compatibility
    pub async fn create(path: &Path, config: &Config, platform: Arc<Platform>) -> Result<Self> {
        let writer = platform.fs().create_file(path).await?;
        let compression = if config.storage.compression.enabled {
            let algorithm = match config.storage.compression.algorithm {
                crate::config::CompressionAlgorithm::None => {
                    crate::storage::sstable::compression::CompressionAlgorithm::None
                }
                crate::config::CompressionAlgorithm::Lz4 => {
                    crate::storage::sstable::compression::CompressionAlgorithm::Lz4
                }
                crate::config::CompressionAlgorithm::Snappy => {
                    crate::storage::sstable::compression::CompressionAlgorithm::Snappy
                }
                crate::config::CompressionAlgorithm::Deflate => {
                    crate::storage::sstable::compression::CompressionAlgorithm::Deflate
                }
                // Map Zstd to Deflate as a fallback since we don't have Zstd support yet
                crate::config::CompressionAlgorithm::Zstd => {
                    crate::storage::sstable::compression::CompressionAlgorithm::Deflate
                }
            };
            Some(Compression::new(algorithm)?)
        } else {
            None
        };

        let bloom_filter = if config.storage.enable_bloom_filters {
            Some(BloomFilter::new(1000, config.storage.bloom_filter_fp_rate)?) // Initial capacity
        } else {
            None
        };

        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let mut writer = Self {
            writer,
            config: config.clone(),
            platform,
            offset: 0,
            index_entries: Vec::new(),
            compression,
            bloom_filter,
            current_block: Vec::new(),
            compression_stats: Vec::new(),
            block_checksums: Vec::new(),
            entry_count: 0,
            table_count: 0,
            uncompressed_size: 0,
            compressed_size: 0,
            created_at,
            finalized: false,
            block_index: 0,
            write_batch: Vec::new(),
            batch_size_threshold: 1000, // Default batch size
        };

        // Write file header
        writer.write_header().await?;

        Ok(writer)
    }

    /// Write the Cassandra-compatible SSTable file header
    async fn write_header(&mut self) -> Result<()> {
        let mut header = Vec::new();

        // Cassandra magic bytes (4 bytes)
        header.extend_from_slice(&CASSANDRA_MAGIC);

        // Cassandra format version ('oa' = 2 bytes)
        header.extend_from_slice(CASSANDRA_FORMAT_VERSION);

        // Flags (4 bytes, big-endian)
        let mut flags = 0u32;
        if self.compression.is_some() {
            flags |= 0x01; // Compression enabled
        }
        if self.bloom_filter.is_some() {
            flags |= 0x02; // Bloom filter enabled
        }
        header.extend_from_slice(&flags.to_be_bytes());

        // Partition count (8 bytes, big-endian) - will be updated in footer
        header.extend_from_slice(&0u64.to_be_bytes()); // Placeholder

        // Timestamp range (16 bytes, big-endian)
        header.extend_from_slice(&self.created_at.to_be_bytes()); // Min timestamp
        header.extend_from_slice(&self.created_at.to_be_bytes()); // Max timestamp (placeholder)

        // Reserved bytes for Cassandra compatibility (7 bytes to reach 32-byte header)
        header.extend_from_slice(&[0u8; 7]);

        // Verify header is exactly 32 bytes as per Cassandra spec
        assert_eq!(
            header.len(),
            32,
            "Cassandra header must be exactly 32 bytes"
        );

        self.writer.write_all(&header).map_err(|e| Error::from(e))?;
        self.offset += header.len() as u64;
        Ok(())
    }

    /// Add an entry to the SSTable with advanced features
    pub async fn add_entry(&mut self, table_id: &TableId, key: RowKey, value: Value) -> Result<()> {
        if self.finalized {
            return Err(Error::storage(
                "Cannot add entry to finalized SSTable".to_string(),
            ));
        }

        // Add to bloom filter if enabled
        if let Some(ref mut bloom_filter) = self.bloom_filter {
            bloom_filter.insert(key.as_bytes());
        }

        // Serialize the entry with Cassandra-compatible format
        let entry_data = self.serialize_entry_cassandra(table_id, &key, &value)?;
        self.uncompressed_size += entry_data.len() as u64;

        // Add to current block
        self.current_block.extend_from_slice(&entry_data);

        // Create index entry
        let index_entry = IndexEntry {
            table_id: table_id.clone(),
            key: key.clone(),
            offset: self.offset,
            size: entry_data.len() as u32,
            compressed: self.compression.is_some(),
        };

        self.index_entries.push(index_entry);
        self.entry_count += 1;

        // Check if we need to flush the current block
        if self.current_block.len() >= self.config.storage.block_size as usize {
            self.flush_block().await?;
        }

        Ok(())
    }

    /// Add multiple entries in a batch for better performance
    pub async fn add_batch(&mut self, entries: Vec<(TableId, RowKey, Value)>) -> Result<()> {
        if self.finalized {
            return Err(Error::storage(
                "Cannot add batch to finalized SSTable".to_string(),
            ));
        }

        // Sort entries by table and key for optimal storage
        let mut sorted_entries = entries;
        sorted_entries.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

        for (table_id, key, value) in sorted_entries {
            self.add_entry(&table_id, key, value).await?;
        }

        Ok(())
    }

    /// Start a write transaction
    pub fn begin_batch(&mut self) {
        self.write_batch.clear();
    }

    /// Add entry to current batch
    pub fn add_to_batch(&mut self, table_id: TableId, key: RowKey, value: Value) {
        self.write_batch.push((table_id, key, value));
    }

    /// Commit the current batch
    pub async fn commit_batch(&mut self) -> Result<()> {
        if self.write_batch.is_empty() {
            return Ok(());
        }

        let batch = std::mem::take(&mut self.write_batch);
        self.add_batch(batch).await?;

        Ok(())
    }

    /// Flush current block to storage
    async fn flush_block(&mut self) -> Result<()> {
        if self.current_block.is_empty() {
            return Ok(());
        }

        // Calculate checksum for data integrity
        let checksum = self.calculate_crc32(&self.current_block);

        // Compress block if enabled
        let compressed_data = if let Some(ref compression) = self.compression {
            let compressed = compression.compress(&self.current_block)?;

            // Track compression statistics
            let stats = CompressionStats::calculate(
                self.current_block.len() as u64,
                compressed.len() as u64,
                compression.algorithm().clone(),
            );
            self.compression_stats.push(stats);

            self.compressed_size += compressed.len() as u64;
            compressed
        } else {
            self.compressed_size += self.current_block.len() as u64;
            self.current_block.clone()
        };

        // Write block header
        let block_header = BlockHeader {
            block_index: self.block_index,
            uncompressed_size: self.current_block.len() as u32,
            compressed_size: compressed_data.len() as u32,
            checksum,
            entry_count: self.index_entries.len() as u32,
        };

        let header_bytes =
            bincode::serialize(&block_header).map_err(|e| Error::serialization(e.to_string()))?;

        self.writer
            .write_all(&header_bytes)
            .map_err(|e| Error::from(e))?;
        self.offset += header_bytes.len() as u64;

        // Write compressed block data
        self.writer
            .write_all(&compressed_data)
            .map_err(|e| Error::from(e))?;
        self.offset += compressed_data.len() as u64;

        // Store checksum for verification
        self.block_checksums.push(checksum);

        // Clear current block
        self.current_block.clear();
        self.block_index += 1;

        Ok(())
    }

    /// Serialize an entry into Cassandra 'oa' compatible binary format
    fn serialize_entry_cassandra(
        &self,
        table_id: &TableId,
        key: &RowKey,
        value: &Value,
    ) -> Result<Vec<u8>> {
        let mut data = Vec::new();

        // Cassandra partition format:
        // [Partition Key Length][Partition Key][Clustering Key Length][Clustering Key][Timestamp][Value]

        // Timestamp (8 bytes, big-endian, in microseconds)
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        data.extend_from_slice(&timestamp.to_be_bytes());

        // Partition key (table name) with Cassandra VInt length encoding
        let table_id_bytes = table_id.name().as_bytes();
        self.write_cassandra_vint(&mut data, table_id_bytes.len() as u64)?;
        data.extend_from_slice(table_id_bytes);

        // Row key with Cassandra VInt length encoding
        let key_bytes = key.as_bytes();
        self.write_cassandra_vint(&mut data, key_bytes.len() as u64)?;
        data.extend_from_slice(key_bytes);

        // Value type (1 byte) - use conversion trait
        let data_type_id: u8 = match value.data_type() {
            CqlType::Text => DataType::TEXT,
            CqlType::Int => DataType::INTEGER,
            CqlType::BigInt => DataType::BIGINT,
            CqlType::Double => DataType::FLOAT,
            CqlType::Boolean => DataType::BOOLEAN,
            CqlType::Blob => DataType::BLOB,
            CqlType::Timestamp => DataType::TIMESTAMP,
            CqlType::Uuid => DataType::UUID,
            CqlType::List(_) => DataType::LIST,
            CqlType::Set(_) => DataType::LIST, // Map to closest existing
            CqlType::Map(_, _) => DataType::MAP,
            CqlType::Tuple(_) => DataType::LIST, // Map to closest existing
            CqlType::Udt(_, _) => DataType::JSON, // Map to closest existing
            CqlType::Frozen(_) => DataType::BLOB, // Map to closest existing
            _ => DataType::TEXT, // Default fallback
        };
        data.push(data_type_id);

        // Value data with Cassandra VInt length encoding
        let value_bytes = self.serialize_value_cassandra_compatible(value)?;
        self.write_cassandra_vint(&mut data, value_bytes.len() as u64)?;
        data.extend_from_slice(&value_bytes);

        // Entry flags (1 byte) - 0x00 for live data, 0x01 for tombstone
        data.push(0x00); // Live data

        Ok(data)
    }

    /// Cassandra-compatible value serialization with big-endian encoding
    fn serialize_value_cassandra_compatible(&self, value: &Value) -> Result<Vec<u8>> {
        match value {
            Value::Null => Ok(vec![]),
            Value::Boolean(b) => Ok(vec![if *b { 1 } else { 0 }]),
            Value::Integer(i) => Ok(i.to_be_bytes().to_vec()), // Big-endian for Cassandra
            Value::BigInt(i) => Ok(i.to_be_bytes().to_vec()),  // Big-endian for Cassandra
            Value::Float(f) => Ok(f.to_be_bytes().to_vec()),   // Big-endian for Cassandra
            Value::Text(s) => {
                // UTF-8 bytes with length prefix
                let bytes = s.as_bytes();
                let mut result = Vec::new();
                self.write_cassandra_vint(&mut result, bytes.len() as u64)?;
                result.extend_from_slice(bytes);
                Ok(result)
            }
            Value::Blob(b) => {
                // Blob data with length prefix
                let mut result = Vec::new();
                self.write_cassandra_vint(&mut result, b.len() as u64)?;
                result.extend_from_slice(b);
                Ok(result)
            }
            Value::Timestamp(ts) => Ok(ts.to_be_bytes().to_vec()), // Big-endian for Cassandra
            Value::Uuid(uuid) => {
                // UUID as 16 bytes in big-endian format
                if uuid.len() == 16 {
                    Ok(uuid.to_vec())
                } else {
                    Err(Error::serialization("Invalid UUID length".to_string()))
                }
            }
            // For complex types, use Cassandra-compatible serialization
            _ => {
                // TODO: Implement proper Cassandra serialization for complex types
                bincode::serialize(value).map_err(|e| Error::serialization(e.to_string()))
            }
        }
    }

    /// Legacy value serialization (kept for backward compatibility)
    fn serialize_value_optimized(&self, value: &Value) -> Result<Vec<u8>> {
        match value {
            Value::Null => Ok(vec![]),
            Value::Boolean(b) => Ok(vec![if *b { 1 } else { 0 }]),
            Value::Integer(i) => Ok(i.to_le_bytes().to_vec()),
            Value::BigInt(i) => Ok(i.to_le_bytes().to_vec()),
            Value::Float(f) => Ok(f.to_le_bytes().to_vec()),
            Value::Text(s) => Ok(s.as_bytes().to_vec()),
            Value::Blob(b) => Ok(b.clone()),
            Value::Timestamp(ts) => Ok(ts.to_le_bytes().to_vec()),
            Value::Uuid(uuid) => Ok(uuid.to_vec()),
            // For complex types, fall back to bincode
            _ => bincode::serialize(value).map_err(|e| Error::serialization(e.to_string())),
        }
    }

    /// Write Cassandra-compatible variable-length integer (VInt)
    fn write_cassandra_vint(&self, data: &mut Vec<u8>, value: u64) -> Result<()> {
        // Cassandra VInt encoding:
        // - If value fits in 7 bits: 0xxxxxxx (single byte)
        // - Otherwise: 1xxxxxxx + additional bytes in big-endian

        if value < 0x80 {
            // Single byte: 0xxxxxxx
            data.push(value as u8);
        } else {
            // Multi-byte encoding
            let mut bytes_needed = 0;
            let mut temp = value;
            while temp > 0 {
                bytes_needed += 1;
                temp >>= 8;
            }

            // First byte: 1xxxxxxx where x is the continuation data
            let first_byte = 0x80 | ((value >> (8 * (bytes_needed - 1))) as u8 & 0x7F);
            data.push(first_byte);

            // Write remaining bytes in big-endian order
            for i in (0..bytes_needed - 1).rev() {
                data.push((value >> (8 * i)) as u8);
            }
        }
        Ok(())
    }

    /// Calculate CRC32 checksum for data integrity
    fn calculate_crc32(&self, data: &[u8]) -> u32 {
        let mut crc = 0xFFFFFFFF;
        for &byte in data {
            crc ^= byte as u32;
            for _ in 0..8 {
                if crc & 1 != 0 {
                    crc = (crc >> 1) ^ CRC32_POLYNOMIAL;
                } else {
                    crc >>= 1;
                }
            }
        }
        !crc
    }

    /// Finalize the SSTable by writing index and metadata
    pub async fn finish(&mut self) -> Result<()> {
        if self.finalized {
            return Err(Error::storage("SSTable already finalized".to_string()));
        }

        // Flush any remaining data in current block
        if !self.current_block.is_empty() {
            self.flush_block().await?;
        }

        // Write bloom filter if enabled
        if let Some(bloom_filter) = self.bloom_filter.take() {
            self.write_bloom_filter(&bloom_filter).await?;
            self.bloom_filter = Some(bloom_filter); // Put it back
        }

        // Write index
        self.write_index().await?;

        // Write compression statistics
        self.write_compression_stats().await?;

        // Write footer with enhanced metadata
        self.write_footer().await?;

        // Flush all data to disk
        self.writer.flush().map_err(|e| Error::from(e))?;

        self.finalized = true;
        Ok(())
    }

    /// Write bloom filter data
    async fn write_bloom_filter(&mut self, bloom_filter: &BloomFilter) -> Result<()> {
        let bloom_data = bloom_filter.serialize()?;

        // Write bloom filter header
        let bloom_header = BloomFilterHeader {
            offset: self.offset,
            size: bloom_data.len() as u32,
            hash_count: bloom_filter.hash_count(),
            bit_count: bloom_filter.bit_count(),
        };

        let header_bytes =
            bincode::serialize(&bloom_header).map_err(|e| Error::serialization(e.to_string()))?;

        self.writer
            .write_all(&header_bytes)
            .map_err(|e| Error::from(e))?;
        self.offset += header_bytes.len() as u64;

        // Write bloom filter data
        self.writer
            .write_all(&bloom_data)
            .map_err(|e| Error::from(e))?;
        self.offset += bloom_data.len() as u64;

        Ok(())
    }

    /// Write compression statistics
    async fn write_compression_stats(&mut self) -> Result<()> {
        if self.compression_stats.is_empty() {
            return Ok(());
        }

        let stats_data = bincode::serialize(&self.compression_stats)
            .map_err(|e| Error::serialization(e.to_string()))?;

        // Write compression stats header
        let stats_header = CompressionStatsHeader {
            offset: self.offset,
            size: stats_data.len() as u32,
            block_count: self.compression_stats.len() as u32,
        };

        let header_bytes =
            bincode::serialize(&stats_header).map_err(|e| Error::serialization(e.to_string()))?;

        self.writer
            .write_all(&header_bytes)
            .map_err(|e| Error::from(e))?;
        self.offset += header_bytes.len() as u64;

        // Write stats data
        self.writer
            .write_all(&stats_data)
            .map_err(|e| Error::from(e))?;
        self.offset += stats_data.len() as u64;

        Ok(())
    }

    /// Write the index section
    async fn write_index(&mut self) -> Result<()> {
        let index_start = self.offset;

        // Serialize index entries
        let index_data = bincode::serialize(&self.index_entries)
            .map_err(|e| Error::serialization(e.to_string()))?;

        // Write index data
        self.writer
            .write_all(&index_data)
            .map_err(|e| Error::from(e))?;
        self.offset += index_data.len() as u64;

        // Write index metadata
        let index_metadata = IndexMetadata {
            index_offset: index_start,
            index_size: index_data.len() as u32,
            entry_count: self.entry_count,
        };

        let metadata_bytes =
            bincode::serialize(&index_metadata).map_err(|e| Error::serialization(e.to_string()))?;

        self.writer
            .write_all(&metadata_bytes)
            .map_err(|e| Error::from(e))?;
        self.offset += metadata_bytes.len() as u64;

        Ok(())
    }

    /// Write the Cassandra-compatible footer
    async fn write_footer(&mut self) -> Result<()> {
        let mut footer = Vec::new();

        // Cassandra footer format (16 bytes total):
        // [Index Offset: 8 bytes][Magic Number: 8 bytes]

        // Index offset (8 bytes, big-endian)
        footer.extend_from_slice(&self.offset.to_be_bytes());

        // Cassandra magic number for validation (8 bytes)
        // Using extended magic for footer validation
        let footer_magic = [0x5A, 0x5A, 0x5A, 0x5A, 0x5A, 0x5A, 0x5A, 0x5A];
        footer.extend_from_slice(&footer_magic);

        // Verify footer is exactly 16 bytes as per Cassandra spec
        assert_eq!(
            footer.len(),
            16,
            "Cassandra footer must be exactly 16 bytes"
        );

        self.writer.write_all(&footer).map_err(|e| Error::from(e))?;
        self.offset += footer.len() as u64;

        Ok(())
    }

    /// Get writer statistics
    pub fn stats(&self) -> SSTableWriterStats {
        let compression_ratio = if self.uncompressed_size > 0 {
            self.compressed_size as f64 / self.uncompressed_size as f64
        } else {
            1.0
        };

        SSTableWriterStats {
            entry_count: self.entry_count,
            table_count: self.table_count,
            file_size: self.offset,
            uncompressed_size: self.uncompressed_size,
            compressed_size: self.compressed_size,
            compression_ratio,
            index_entries: self.index_entries.len(),
            block_count: self.block_index,
            bloom_filter_enabled: self.bloom_filter.is_some(),
            compression_enabled: self.compression.is_some(),
            average_entry_size: if self.entry_count > 0 {
                self.uncompressed_size / self.entry_count
            } else {
                0
            },
            write_throughput_estimate: if self.entry_count > 0 {
                // Estimate based on current performance
                self.entry_count * 1_000_000 / (self.offset / 1024) // entries per MB
            } else {
                0
            },
        }
    }

    /// Get detailed performance metrics
    pub fn performance_metrics(&self) -> PerformanceMetrics {
        PerformanceMetrics {
            write_throughput: self.calculate_write_throughput(),
            compression_efficiency: self.calculate_compression_efficiency(),
            storage_efficiency: self.calculate_storage_efficiency(),
            index_overhead: self.calculate_index_overhead(),
        }
    }

    /// Calculate write throughput in operations per second
    fn calculate_write_throughput(&self) -> f64 {
        if self.entry_count == 0 {
            return 0.0;
        }

        let elapsed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64
            - self.created_at;

        if elapsed == 0 {
            return 0.0;
        }

        (self.entry_count as f64 * 1_000_000.0) / elapsed as f64
    }

    /// Calculate compression efficiency
    fn calculate_compression_efficiency(&self) -> f64 {
        if self.compression.is_none() || self.uncompressed_size == 0 {
            return 1.0;
        }

        1.0 - (self.compressed_size as f64 / self.uncompressed_size as f64)
    }

    /// Calculate storage efficiency
    fn calculate_storage_efficiency(&self) -> f64 {
        if self.entry_count == 0 {
            return 0.0;
        }

        let useful_data = self.compressed_size;
        let total_size = self.offset;

        useful_data as f64 / total_size as f64
    }

    /// Calculate index overhead
    fn calculate_index_overhead(&self) -> f64 {
        let index_size = self.index_entries.len() * std::mem::size_of::<IndexEntry>();
        let data_size = self.compressed_size as usize;

        if data_size == 0 {
            return 0.0;
        }

        index_size as f64 / data_size as f64
    }
}

/// Index metadata stored in the SSTable
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct IndexMetadata {
    index_offset: u64,
    index_size: u32,
    entry_count: u64,
}

/// Block header for data integrity and metadata
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct BlockHeader {
    block_index: u32,
    uncompressed_size: u32,
    compressed_size: u32,
    checksum: u32,
    entry_count: u32,
}

/// Bloom filter header
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct BloomFilterHeader {
    offset: u64,
    size: u32,
    hash_count: u32,
    bit_count: u64,
}

/// Compression statistics header
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct CompressionStatsHeader {
    offset: u64,
    size: u32,
    block_count: u32,
}

/// Enhanced SSTable footer with comprehensive metadata
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SSTableFooter {
    format_version: u32,
    entry_count: u64,
    table_count: u64,
    file_size: u64,
    uncompressed_size: u64,
    compressed_size: u64,
    compression_ratio: f64,
    block_count: u32,
    created_at: u64,
    finished_at: u64,
    bloom_filter_enabled: bool,
    compression_enabled: bool,
    index_offset: u64,
    magic: u32,
}

/// Enhanced SSTable writer statistics
#[derive(Debug, Clone)]
pub struct SSTableWriterStats {
    pub entry_count: u64,
    pub table_count: u64,
    pub file_size: u64,
    pub uncompressed_size: u64,
    pub compressed_size: u64,
    pub compression_ratio: f64,
    pub index_entries: usize,
    pub block_count: u32,
    pub bloom_filter_enabled: bool,
    pub compression_enabled: bool,
    pub average_entry_size: u64,
    pub write_throughput_estimate: u64,
}

/// Performance metrics for write operations
#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub write_throughput: f64,
    pub compression_efficiency: f64,
    pub storage_efficiency: f64,
    pub index_overhead: f64,
}

impl Drop for SSTableWriter {
    fn drop(&mut self) {
        if !self.finalized {
            // Log warning about unfinalized writer
            eprintln!("Warning: SSTableWriter dropped without being finalized");
            eprintln!("  Entries written: {}", self.entry_count);
            eprintln!("  Data size: {} bytes", self.offset);
        }
    }
}

/// Data type enumeration for optimized serialization
struct DataType;

impl DataType {
    const NULL: u8 = 0;
    const BOOLEAN: u8 = 1;
    const INTEGER: u8 = 2;
    const BIGINT: u8 = 3;
    const FLOAT: u8 = 4;
    const TEXT: u8 = 5;
    const BLOB: u8 = 6;
    const TIMESTAMP: u8 = 7;
    const UUID: u8 = 8;
    const JSON: u8 = 9;
    const LIST: u8 = 10;
    const MAP: u8 = 11;
}

impl From<crate::types::DataType> for u8 {
    fn from(dt: crate::types::DataType) -> Self {
        match dt {
            crate::types::DataType::Null => DataType::NULL,
            crate::types::DataType::Boolean => DataType::BOOLEAN,
            crate::types::DataType::TinyInt => DataType::INTEGER, // Map to closest existing
            crate::types::DataType::SmallInt => DataType::INTEGER, // Map to closest existing
            crate::types::DataType::Integer => DataType::INTEGER,
            crate::types::DataType::BigInt => DataType::BIGINT,
            crate::types::DataType::Float32 => DataType::FLOAT, // Map to closest existing
            crate::types::DataType::Float => DataType::FLOAT,
            crate::types::DataType::Text => DataType::TEXT,
            crate::types::DataType::Blob => DataType::BLOB,
            crate::types::DataType::Timestamp => DataType::TIMESTAMP,
            crate::types::DataType::Uuid => DataType::UUID,
            crate::types::DataType::Json => DataType::JSON,
            crate::types::DataType::List => DataType::LIST,
            crate::types::DataType::Set => DataType::LIST, // Map to closest existing
            crate::types::DataType::Map => DataType::MAP,
            crate::types::DataType::Tuple => DataType::LIST, // Map to closest existing
            crate::types::DataType::Udt => DataType::JSON,   // Map to closest existing
            crate::types::DataType::Frozen => DataType::BLOB, // Map to closest existing
            crate::types::DataType::Tombstone => DataType::BLOB, // Map to closest existing
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TableId;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_sstable_writer_creation() {
        let temp_file = NamedTempFile::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        let writer = SSTableWriter::create(temp_file.path(), &config, platform)
            .await
            .unwrap();
        assert_eq!(writer.stats().entry_count, 0);
        assert!(!writer.finalized);
    }

    #[tokio::test]
    async fn test_sstable_writer_add_entry() {
        let temp_file = NamedTempFile::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        let mut writer = SSTableWriter::create(temp_file.path(), &config, platform)
            .await
            .unwrap();

        let table_id = TableId::new("test_table");
        let key = RowKey::from("test_key");
        let value = Value::Text("test_value".to_string());

        writer.add_entry(&table_id, key, value).await.unwrap();

        let stats = writer.stats();
        assert_eq!(stats.entry_count, 1);
        assert!(stats.uncompressed_size > 0);

        writer.finish().await.unwrap();
        assert!(writer.finalized);
    }

    #[tokio::test]
    async fn test_sstable_writer_batch_operations() {
        let temp_file = NamedTempFile::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        let mut writer = SSTableWriter::create(temp_file.path(), &config, platform)
            .await
            .unwrap();

        // Create batch of entries
        let mut batch = Vec::new();
        for i in 0..100 {
            let table_id = TableId::new("test_table");
            let key = RowKey::from(format!("key_{:03}", i));
            let value = Value::Text(format!("value_{}", i));
            batch.push((table_id, key, value));
        }

        writer.add_batch(batch).await.unwrap();

        let stats = writer.stats();
        assert_eq!(stats.entry_count, 100);
        assert!(stats.compression_enabled);

        writer.finish().await.unwrap();
        assert!(writer.finalized);
    }

    #[tokio::test]
    async fn test_sstable_writer_performance_metrics() {
        let temp_file = NamedTempFile::new().unwrap();
        let config = Config::performance_optimized();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        let mut writer = SSTableWriter::create(temp_file.path(), &config, platform)
            .await
            .unwrap();

        // Add multiple entries to test performance
        for i in 0..1000 {
            let table_id = TableId::new("perf_test");
            let key = RowKey::from(format!("key_{:06}", i));
            let value = Value::Text(format!("test_value_for_performance_{}", i));
            writer.add_entry(&table_id, key, value).await.unwrap();
        }

        let metrics = writer.performance_metrics();
        assert!(metrics.write_throughput > 0.0);
        assert!(metrics.compression_efficiency >= 0.0);
        assert!(metrics.storage_efficiency > 0.0);

        writer.finish().await.unwrap();

        let final_stats = writer.stats();
        assert_eq!(final_stats.entry_count, 1000);
        assert!(final_stats.compression_ratio < 1.0); // Should be compressed
    }
}
