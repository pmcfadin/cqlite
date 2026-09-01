//! Public types for SSTable reader

use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

use super::source::{BlockSource, ScanSource};

use crate::{
    parser::SSTableHeader,
    parser::SSTableParser,
    platform::Platform,
    schema::{TableSchema, UdtRegistry},
    Config, RowKey,
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
    /// Number of checksum mismatches.
    ///
    /// Deprecated (issue #1283): the consolidated integrity check projects over the
    /// authoritative verifier `verify::verify_sstable`, which does not surface a
    /// per-block checksum-mismatch count. This field is retained for API
    /// compatibility but is ALWAYS `0`; the dead computation that once populated it
    /// has been removed. Inspect `parsing_errors` / `overall_status` instead.
    #[deprecated(
        since = "0.12.0",
        note = "consolidated integrity check never populates this; it is always 0 (#1283) — use parsing_errors/overall_status"
    )]
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
///
/// Only two states are PRODUCED: the integrity check delegates to the authoritative
/// verifier `verify::verify_sstable` (issue #1283), which reports either zero
/// findings (`Healthy`) or one-or-more findings (`Corrupted`). The former `Degraded`
/// state was driven by a `checksum_mismatches` counter that was never incremented —
/// it was unreachable dead code. `Degraded` is retained as a deprecated variant for
/// API compatibility but is NEVER returned by `perform_integrity_check`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntegrityStatus {
    /// File is healthy (the verifier reported no findings)
    Healthy,
    /// File has minor issues but is readable.
    ///
    /// Deprecated (issue #1283): the consolidated integrity check never produces
    /// this status — it projects `verify::verify_sstable` findings onto exactly
    /// `Healthy` (no findings) or `Corrupted` (≥1 finding). Retained for API
    /// compatibility only.
    #[deprecated(
        since = "0.12.0",
        note = "consolidated integrity check never produces Degraded; only Healthy|Corrupted (#1283)"
    )]
    Degraded,
    /// File has corruption (the verifier reported at least one finding)
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
    /// Cap (bytes) on the scratch buffer `block_io.rs` reads a block through
    /// (`read_into_vec_capped`), applied to EVERY backend, `Mapped` included. It is
    /// NOT a `BufReader` capacity — that is tokio's 8 KiB default (#3068).
    pub read_buffer_size: usize,
    /// Legacy promote-only flag: it upgrades an **explicit**
    /// [`Buffered`](crate::config::DiskAccessMode::Buffered) request to
    /// [`Mmap`](crate::config::DiskAccessMode::Mmap). It does **not** select the
    /// read backend, and `false` does **not** mean "buffered I/O".
    ///
    /// The backend is chosen by `storage.disk_access_mode` ([`crate::Config`]),
    /// which defaults to [`Auto`](crate::config::DiskAccessMode::Auto). See
    /// `reader::backend_resolve::resolve_disk_access_mode` for the exact resolution:
    /// `Auto` yields buffered I/O for files below [`Self::mmap_min_size_bytes`]
    /// (4 KiB) and **mmap** for anything larger, up to
    /// `storage.direct_io_memory_fraction` of system RAM (default half); above that
    /// it picks direct I/O where that backend is compiled in (Linux/Android/macOS —
    /// elsewhere, or when system memory cannot be read, `Auto` never escalates and
    /// stays on mmap). So a reader built from a default `Config` maps a Data.db
    /// between one page and that fraction, and this flag cannot make it buffered;
    /// only an explicit `disk_access_mode: buffered` can (as the write/compaction
    /// merge readers do). Mapping is still best-effort: if the map itself fails
    /// (network/overlay mount, `ENOMEM`), `build_block_sources` falls back to
    /// buffered I/O rather than failing the open.
    ///
    /// A mapped file is served from the OS page cache with no per-block read
    /// syscall, mirroring Cassandra's `disk_access_mode: mmap`. Map only
    /// immutable local SSTables — see [`crate::Config`]'s `storage.use_mmap` for
    /// the platform/filesystem constraints (network FS and external mutation can
    /// `SIGBUS`).
    pub use_mmap: bool,
    /// Minimum file size (bytes) at which the `Auto` heuristic memory-maps.
    ///
    /// Under the default `disk_access_mode: Auto`, files below this use buffered I/O,
    /// since the per-file mapping overhead is not worthwhile for tiny files. It does
    /// NOT gate an explicit `Mmap` request — including one [`Self::use_mmap`] promoted
    /// from an explicit `Buffered` — which is size-independent; only a zero-length
    /// file, which cannot be mapped at all, always falls back to buffered I/O.
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
            // Promote-only legacy flag (see the field doc); the backend itself
            // comes from `storage.disk_access_mode`, whose `Auto` default
            // resolves to mmap for any file above `mmap_min_size_bytes`.
            use_mmap: false,
            mmap_min_size_bytes: 4096, // Skip mmap for files smaller than a page
            block_cache_size: 1000,    // Cache 1000 blocks
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

/// Single-entry same-key memo of a BTI partition resolution (issue #1574, C3):
/// `(partition-key bytes, resolved uncompressed Data.db offset or authoritative
/// absence)`. See [`SSTableReader::bti_lookup_memo`].
type BtiLookupMemo = std::sync::Mutex<Option<(Box<[u8]>, Option<u64>)>>;

/// SSTable reader for efficient data access
#[allow(dead_code)]
pub struct SSTableReader {
    /// Path to the SSTable file.
    ///
    /// Interior-mutable (issue #2383 / #2356, "rebind-by-inode"): a warm reader
    /// whose backing snapshot dir the Trino connector cleared can be REBOUND to a
    /// fresh same-inode hardlink path WITHOUT re-opening/re-parsing the whole
    /// Index.db — see [`Self::rebind_path`]. The already-open point/scan handles
    /// stay valid (the inode outlives the cleared dir via the surviving
    /// hardlinks); only [`Self::new_scan_cursor`]'s lazy `File::open`-by-path
    /// needs the live path, so a lock-free `ArcSwap` swap is all a rebind costs.
    pub(crate) file_path: arc_swap::ArcSwap<PathBuf>,
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
    /// Positional (`pread`-style) source for the POINT-READ path (issue #1573,
    /// Epic C / C2). Opened ONCE at reader open and shared for the reader's
    /// lifetime. Because [`ReadAt`](super::read_at::ReadAt) takes `&self` (no seek,
    /// no mutable position), concurrent point reads on one reader neither
    /// serialize on a cursor mutex nor `open(2)` `Data.db` per lookup — the two
    /// point-path pathologies C2 removes. Shares the reader's `Arc<Mmap>` when the
    /// backend is mmap (no extra mapping / fd); otherwise it holds one dedicated
    /// read-only fd. Scans still use [`scan_source`](Self::scan_source); this
    /// source is intentionally shaped so scans could adopt it later (audit F3)
    /// without requiring it now.
    pub(crate) point_source: std::sync::Arc<dyn super::read_at::ReadAt>,
    /// Positional source for SCAN-side offset reads (issue #2876): the
    /// Summary-guided compressed partition walk (`compressed_offset.rs`) and the
    /// windowed streaming scan feed (`scan_stream_windowed_read.rs`) read Data.db
    /// positionally too, but they must NOT share [`point_source`](Self::point_source)
    /// — that source is deliberately advised `MADV_RANDOM` for a large mmap-backed
    /// file (issue #2210) to suppress kernel readahead for scattered point faults,
    /// which is exactly backwards for a mostly-sequential scan walk (one 4 KiB page
    /// fault per positional read, #2210 × #1940 cross-path regression). Built once
    /// at open from the SAME never-`MADV_RANDOM` mapping
    /// [`scan_source`](Self::scan_source)
    /// already uses for the mmap backend (an `Arc` clone, no new mapping / fd,
    /// #1143 preserved); the Direct/Buffered backends have no per-mapping advice
    /// concept, so they share `point_source` unchanged.
    pub(crate) scan_positional_source: std::sync::Arc<dyn super::read_at::ReadAt>,
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
    /// Reader configuration
    pub(crate) config: SSTableReaderConfig,
    /// The full [`Config`] this reader was opened with.
    ///
    /// Retained so `perform_integrity_check` can delegate to the authoritative
    /// verifier `verify::verify_sstable` — the single source of truth for SSTable
    /// integrity (issue #1283) — using the SAME configuration the reader itself
    /// was opened under, rather than re-deriving or defaulting it.
    pub(crate) open_config: Config,
    /// Platform abstraction
    pub(crate) platform: Arc<Platform>,
    /// Statistics
    pub(crate) stats: SSTableReaderStats,
    /// Tombstone merger for deletion handling
    #[cfg(feature = "tombstones")]
    pub(crate) tombstone_merger: TombstoneMerger,
    /// SSTable generation number (for multi-generation merging)
    pub generation: u64,
    /// Actual header size calculated during parsing
    pub(crate) actual_header_size: usize,
    /// Index.db reader for partition lookup and promoted index handling
    pub(crate) index_reader: Option<IndexReader>,
    /// `true` iff a sibling `Index.db` file EXISTS on disk but failed to
    /// open/parse (issue #2302, roborev job 1606), i.e. `index_reader` is `None`
    /// for a present-but-unusable reason rather than a genuinely absent file. The
    /// full enumeration uses this to emit a LOUD warning (present-but-unusable
    /// index + Summary.db loaded is the exact silent-degradation class #2302 kills)
    /// instead of silently falling back to a sequential scan. A genuinely absent
    /// Index.db leaves this `false`.
    pub(crate) index_present_but_unloadable: bool,
    /// Summary.db reader for token-range iteration and sampling
    pub(crate) summary_reader: Option<SummaryReader>,
    /// Cached Cassandra Murmur3 tokens of this SSTable's authoritative
    /// `[first_key, last_key]` partition-key bound (issue #1576, Epic C/C5 perf
    /// finding). The two endpoint keys come from `Summary.db` and are IMMUTABLE
    /// for the reader's lifetime, so their tokens are computed ONCE at reader
    /// open rather than re-hashed on every point read by
    /// [`partition_key_out_of_range`](SSTableReader::partition_key_out_of_range).
    /// `Some((first_token, last_token))` only when `Summary.db` was present with
    /// two non-empty endpoints (the exact condition under which the range
    /// short-circuit is armed); `None` otherwise (no `Summary.db` — e.g. a BTI
    /// reader — or a degenerate/empty endpoint), in which case the check
    /// conservatively cannot rule out and the normal presence path runs. The hot
    /// path only hashes the QUERY key.
    pub(crate) endpoint_tokens: Option<(i64, i64)>,
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
    /// Schema pre-resolved from the [`schema_registry`](Self::schema_registry) at
    /// wiring time (async), cached for the SYNC schema-fallback tier of
    /// `get_table_schema`. Issue #1692 (AG3): the sync parse path must never
    /// `block_on` the async registry lock on a tokio worker thread, so the
    /// registry lookup is resolved once, up front, into this field instead.
    /// `Some` only when the header schema was absent AND the registry resolved a
    /// schema for this table; otherwise `None` (the sync path then falls through
    /// to header-column construction, exactly as before).
    #[cfg(feature = "state_machine")]
    pub(super) registry_schema: Option<Arc<TableSchema>>,
    /// UDT registry for UDT-aware parsing (cached for sync access)
    pub(crate) udt_registry: Option<UdtRegistry>,
    /// Cooperative cancellation for long-running scans (issue #2264).
    ///
    /// Polled at a bounded interval inside the compaction streaming read and its
    /// sequential-scan fallback so a Flight `do_get` whose client disconnected
    /// abandons the (otherwise uninterruptible, fully-materialising) walk
    /// promptly instead of burning CPU until the coarse ~1–2 min backstop. The
    /// default is a never-cancelled flag, so readers opened without a wired token
    /// behave exactly as before. Set via [`SSTableReader::set_scan_cancel`].
    pub(crate) scan_cancel: crate::storage::scan_cancel::ScanCancel,
    /// CompressionInfo metadata for chunked decompression (if compressed)
    pub compression_info: Option<Arc<CompressionInfo>>,
    /// `CRC.db` per-chunk checksums for read-time integrity of **uncompressed**
    /// BIG (`nb`) SSTables (issue #1396). `Some` when this is an uncompressed BIG
    /// SSTable that ships a `CRC.db` (Cassandra writes one for every uncompressed
    /// BIG table); `None` for compressed tables (they carry inline per-chunk CRCs
    /// instead), BTI (`da`) tables (no `CRC.db`), or an uncompressed BIG table
    /// whose `CRC.db` is absent (warn-and-proceed, owner-pinned decision D4).
    /// When present, every uncompressed Data.db chunk returned by
    /// `read_uncompressed_data_block` is verified against it, default-on.
    pub(crate) crc_reader: Option<Arc<super::crc::CrcDb>>,
    /// Memo of uncompressed Data.db chunk indices already CRC-verified on the
    /// offset-read path (`read_value_at_offset`, used by the index-based scan and
    /// point lookups). A partition read touches a sub-range of one or more chunks;
    /// verifying a chunk requires the WHOLE chunk, so this set ensures each chunk
    /// is read+checked at most once per reader lifetime — keeping the cost at the
    /// budgeted "one CRC32 pass per chunk" (issue #1396) even when many small
    /// partitions share a chunk. Only ever populated when [`Self::crc_reader`] is
    /// `Some`. The contiguous scan path verifies inline in `block_io` and does not
    /// use this memo.
    pub(crate) verified_uncompressed_chunks: std::sync::Mutex<std::collections::HashSet<u64>>,
    /// Version-feature gates derived from the SSTable filename.
    ///
    /// Computed once in `SSTableReader::open` via `VersionGates::from_path` and
    /// stored here so every downstream consumer (header parsing,
    /// enhanced_statistics_parser, row_decoder row parsing) can read the
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
    /// (`lookup_partition_via_bti_trie` / `bti_point_lookup`) walks these bytes in
    /// place (a borrowed `&[u8]` view of this buffer — no per-lookup copy, issue
    /// #1574 C3) to resolve the uncompressed Data.db offset for a partition key —
    /// an O(log n) point lookup instead of the sequential scan used with no index.
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
    /// Shared, bytes-bounded decompressed-chunk cache (issue #1567, Epic B/B1).
    ///
    /// Every reader opened for the same logical dataset via `SSTableManager`
    /// shares ONE instance (cloned `Arc`), so a chunk decompressed by one read
    /// path is reused by any other. Readers opened via the back-compat
    /// [`SSTableReader::open`] get a fresh per-reader cache. Consulted by the
    /// three wired read sites before reading+decompressing; a hit is an
    /// `Arc::clone` (never a memcpy or a re-decompress).
    pub(crate) chunk_cache: Arc<crate::storage::cache::DecompressedChunkCache>,
    /// Stable per-reader cache identity: a hash of `file_path` + `generation`.
    /// Combined with a per-site namespace salt to form [`ChunkKey::sstable`], so
    /// two different SSTables never collide in the shared cache. Authoritative
    /// (derived from immutable reader identity), never from byte content.
    ///
    /// [`ChunkKey::sstable`]: crate::storage::cache::ChunkKey::sstable
    pub(crate) chunk_cache_id: u64,
    /// Single-slot same-key memo of the most recent BTI partition resolution
    /// (issue #1574, audit C3). A single-candidate `WHERE pk = ?` point read
    /// descends the `Partitions.db` trie twice — once for the candidate prune
    /// (`might_contain_partition`) and once for the seek — for the SAME key. The
    /// resolved uncompressed `Data.db` offset (`Some(off)`) or authoritative
    /// absence (`None`) is a pure function of the immutable trie + key, so the
    /// prune stores it here and the seek reuses it without a second descent
    /// (`TRIE_WALKS` stays 1 per point read). A stale slot (a different key, or a
    /// concurrent read) simply misses and re-walks — never a wrong result. Best
    /// effort: a poisoned lock is treated as a miss. This is NOT a cross-lookup
    /// key/offset cache (that is Epic B/B4); it is bounded to one entry.
    pub(crate) bti_lookup_memo: BtiLookupMemo,
    /// Per-reader key→partition-offset cache (issue #1570, Epic B/B4) — the
    /// Cassandra key-cache analogue. Maps the raw partition-key bytes to the
    /// location the index/trie descent produces, so a repeated hot point read can
    /// return the location without re-probing `Index.db` (BIG,
    /// `lookup_partition_with_index`) or re-descending the `Partitions.db` trie
    /// (BTI, `lookup_partition_via_bti_trie`, `TRIE_WALKS`). Issue #2059: a shared
    /// handle to the PROCESS-GLOBAL byte-bounded cache (or a per-reader
    /// [`disabled`](crate::storage::cache::GlobalKeyOffsetCache::disabled) no-op
    /// when `block_cache.enabled == false`), keyed on
    /// `(generation_identity, raw key)` so aggregate resident memory is bounded by
    /// ONE global cap regardless of open-reader count. Positive-only (absent keys
    /// are never stored, so a hit can never be fabricated).
    pub(crate) key_offset_cache: Arc<crate::storage::cache::GlobalKeyOffsetCache>,
    /// This reader's authoritative inode-stable generation identity
    /// (device+inode+size+generation, #2345/#2059) — the namespacing half of every
    /// global key-cache entry. Computed ONCE at open and IMMUTABLE thereafter, so it
    /// stays stable across a #2383 rebind-by-inode (a path swap over a byte-identical
    /// generation), which is exactly why cached entries survive a rebind. `None` when
    /// the `Data.db` could not be `stat`ed at open (the cache is then bypassed rather
    /// than fabricating an identity — no-heuristics #28).
    pub(crate) generation_identity: Option<crate::storage::cache::GenerationIdentity>,
}
