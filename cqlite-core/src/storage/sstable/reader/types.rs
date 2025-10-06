//! Public types for SSTable reader

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::BufReader;
use tokio::sync::Mutex;

use crate::{
    parser::SSTableHeader, parser::SSTableParser, platform::Platform, types::TableId, RowKey, Value,
};

use super::super::{
    bloom::BloomFilter, compression::CompressionReader, index::SSTableIndex,
    index_reader::IndexReader, statistics_reader::StatisticsReader, summary_reader::SummaryReader,
};

#[cfg(feature = "tombstones")]
use super::super::tombstone_merger::TombstoneMerger;

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

impl Default for SSTableReaderStats {
    fn default() -> Self {
        Self {
            file_size: 0,
            entry_count: 0,
            table_count: 0,
            block_count: 0,
            index_size: 0,
            bloom_filter_size: 0,
            compression_ratio: 0.0,
            cache_hit_rate: 0.0,
        }
    }
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
#[allow(dead_code)]
pub struct SSTableReader {
    /// Path to the SSTable file
    pub(crate) file_path: PathBuf,
    /// File handle for reading
    pub(crate) file: Arc<Mutex<BufReader<File>>>,
    /// SSTable header information
    pub(crate) header: SSTableHeader,
    /// Parser for SSTable format
    #[allow(dead_code)]
    pub(crate) parser: SSTableParser,
    /// Index for efficient lookups
    pub(crate) index: Option<SSTableIndex>,
    /// Bloom filter for existence checks
    pub(crate) bloom_filter: Option<BloomFilter>,
    /// Compression reader
    pub(crate) compression_reader: Option<CompressionReader>,
    /// Block metadata cache
    pub(crate) block_meta_cache: HashMap<u64, BlockMeta>,
    /// Block data cache (LRU)
    pub(crate) block_cache: HashMap<u64, CachedBlock>,
    /// Reader configuration
    pub(crate) config: SSTableReaderConfig,
    /// Platform abstraction
    pub(crate) platform: Arc<Platform>,
    /// Statistics
    pub(crate) stats: SSTableReaderStats,
    /// Cache hit counter for accurate metrics tracking
    pub(crate) cache_hits: AtomicU64,
    /// Cache miss counter for accurate metrics tracking
    pub(crate) cache_misses: AtomicU64,
    /// Tombstone merger for deletion handling
    #[cfg(feature = "tombstones")]
    pub(crate) tombstone_merger: TombstoneMerger,
    /// SSTable generation number (for multi-generation merging)
    pub generation: u64,
    /// Actual header size calculated during parsing
    pub(crate) actual_header_size: usize,
    /// Index.db reader for partition lookup and promoted index handling
    pub(crate) index_reader: Option<IndexReader>,
    /// Summary.db reader for token-range iteration and sampling
    pub(crate) summary_reader: Option<SummaryReader>,
    /// Statistics.db reader for min/max timestamps and metadata
    pub(crate) statistics_reader: Option<StatisticsReader>,
    /// Schema registry for schema-driven operations (modern formats)
    pub(crate) schema_registry: Option<Arc<crate::schema::SchemaRegistry>>,
}
