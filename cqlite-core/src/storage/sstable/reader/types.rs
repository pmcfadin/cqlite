//! Public types for SSTable reader

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::sync::Mutex;

use super::source::{BlockSource, ScanSource};

use crate::{
    parser::SSTableHeader,
    parser::SSTableParser,
    platform::Platform,
    schema::{TableSchema, UdtRegistry},
    types::TableId,
    RowKey, ScanRow,
};

use super::super::{
    bloom::BloomFilter, compression::CompressionReader, compression_info::CompressionInfo,
    index::SSTableIndex, index_reader::IndexReader, statistics_reader::StatisticsReader,
    summary_reader::SummaryReader, version_gate::VersionGates,
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
    /// Whether to memory-map SSTable files instead of using buffered file I/O.
    ///
    /// **Opt-in; defaults to `false`.** When enabled, files at or above
    /// [`Self::mmap_min_size_bytes`] are mapped into the address space and
    /// served from the OS page cache with no per-block read syscall. This
    /// mirrors Cassandra's `disk_access_mode: mmap` and benefits repeated local
    /// scans of the same files. Enable only for immutable local SSTables — see
    /// [`crate::Config`]'s `storage.use_mmap` for the platform/filesystem
    /// constraints (network FS and external mutation can `SIGBUS`).
    pub use_mmap: bool,
    /// Minimum file size (bytes) for memory mapping to kick in.
    ///
    /// Files smaller than this use buffered I/O even when [`Self::use_mmap`] is
    /// set, since the per-file mapping overhead is not worthwhile for tiny
    /// files and mapping a zero-length file is invalid.
    pub mmap_min_size_bytes: usize,
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
            use_mmap: false,             // Opt-in; buffered I/O is the portable, safe default
            mmap_min_size_bytes: 4096,   // Skip mmap for files smaller than a page
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
    pub entries: Option<Vec<(TableId, RowKey, ScanRow)>>,
    /// Last access time for LRU eviction
    pub last_access: std::time::Instant,
}

/// SSTable reader for efficient data access
#[allow(dead_code)]
pub struct SSTableReader {
    /// Path to the SSTable file
    pub(crate) file_path: PathBuf,
    /// Backing byte source for point reads (buffered file I/O or memory map).
    ///
    /// Used only by positioned point-read helpers (`get_cached_data`,
    /// integrity checks) that lock, seek, read, and unlock atomically. Full
    /// scans no longer use this shared cursor — they mint their own
    /// [`ScanCursor`](super::source::ScanCursor) via [`Self::scan_source`] so
    /// they run in parallel (issue #815).
    pub(crate) file: Arc<Mutex<BlockSource>>,
    /// Template for minting fresh per-scan [`BlockSource`]s so concurrent scans
    /// never share a mutable file position or chunk index (issue #815).
    pub(crate) scan_source: ScanSource,
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
    #[cfg(feature = "state_machine")]
    pub(crate) schema_registry: Option<Arc<tokio::sync::RwLock<crate::schema::SchemaRegistry>>>,
    /// Schema registry for schema-driven operations (modern formats) - non-state_machine builds
    #[cfg(not(feature = "state_machine"))]
    pub(crate) schema_registry: Option<Arc<crate::schema::SchemaRegistry>>,
    /// Table schema extracted from SSTable header
    pub(super) schema: Option<Arc<TableSchema>>,
    /// UDT registry for UDT-aware parsing (cached for sync access)
    pub(crate) udt_registry: Option<UdtRegistry>,
    /// CompressionInfo metadata for chunked decompression (if compressed)
    pub compression_info: Option<Arc<CompressionInfo>>,
    /// Version-feature gates derived from the SSTable filename.
    ///
    /// Computed once in `SSTableReader::open` via `VersionGates::from_path` and
    /// stored here so every downstream consumer (header parsing,
    /// enhanced_statistics_parser, v5_compressed_legacy row parsing) can read the
    /// gate values without re-deriving them from the filename each time.
    ///
    /// Decision points that WILL be gated in VG3 are annotated with
    /// `// VG3: use self.version_gates.has_XXX()` comments at each call site.
    pub(crate) version_gates: Arc<VersionGates>,
    /// Raw bytes of the sibling BTI `*-Partitions.db` trie, when this reader was
    /// opened on a BTI ("da") SSTable (issue #831).
    ///
    /// `Some` for BTI SSTables (Partitions.db is tiny — a single small trie),
    /// `None` for BIG-format SSTables. The BTI point-lookup path
    /// (`lookup_partition_via_bti_trie` / `bti_point_lookup`) wraps these bytes in
    /// a `std::io::Cursor` per lookup and walks the trie to resolve the
    /// uncompressed Data.db offset for a partition key — an O(log n) point lookup
    /// instead of the sequential scan used when no index is available.
    pub(crate) bti_partitions_db: Option<Arc<Vec<u8>>>,
    /// Raw bytes of the sibling BTI `*-Rows.db` within-partition row-index trie,
    /// when this reader was opened on a BTI ("da") SSTable (issue #909, #910).
    ///
    /// `Some` for BTI SSTables (always emitted, possibly 0 bytes for a
    /// narrow-only table), `None` for BIG-format SSTables. The BTI point-lookup
    /// path uses this to resolve a WIDE partition: the `Partitions.db` trie
    /// returns a positive `RowsOffset` pointing at the partition's
    /// `TrieIndexEntry` inside `Rows.db`; [`resolve_rows_db_entry`] then recovers
    /// the partition's uncompressed `Data.db` position (`data_position`), which is
    /// the same offset domain a NARROW partition's direct `DataOffset` uses.
    ///
    /// [`resolve_rows_db_entry`]: crate::storage::sstable::bti::resolve_rows_db_entry
    pub(crate) bti_rows_db: Option<Arc<Vec<u8>>>,
    /// Lazily-computed, ascending-sorted list of every partition's UNCOMPRESSED
    /// `Data.db` start offset, enumerated authoritatively from the BTI
    /// `Partitions.db` trie (issue #953 / #951).
    ///
    /// `None` until the first within-SSTable seek requests a successor offset;
    /// computed once via [`SSTableReader::bti_partition_offsets`] (a full trie DFS
    /// resolving WIDE-partition `RowsOffset`s through `Rows.db`) and cached so the
    /// successor lookup is an O(log n) binary search per seek, not an O(n) DFS.
    /// Only ever populated for BTI readers; BIG readers use the sorted `Index.db`
    /// entries directly.
    pub(crate) bti_partition_offsets: std::sync::OnceLock<Vec<u64>>,
}
