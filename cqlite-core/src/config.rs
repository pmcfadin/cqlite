//! Configuration management for CQLite

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Main configuration structure for CQLite database
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Config {
    /// Storage engine configuration
    pub storage: StorageConfig,

    /// Memory management configuration
    pub memory: MemoryConfig,

    /// Query engine configuration
    pub query: QueryConfig,

    /// Performance and optimization settings
    pub performance: PerformanceConfig,

    /// WASM-specific configuration
    #[cfg(target_arch = "wasm32")]
    pub wasm: WasmConfig,
}

/// Storage engine configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Maximum SSTable file size in bytes (default: 64MB)
    pub max_sstable_size: u64,

    /// MemTable size threshold for flushing (default: 16MB)
    pub memtable_size_threshold: u64,

    /// Compaction configuration
    pub compaction: CompactionConfig,

    /// Block size for SSTable data blocks (default: 64KB)
    pub block_size: u32,

    /// Compression configuration
    pub compression: CompressionConfig,

    /// Enable bloom filters for SSTables
    pub enable_bloom_filters: bool,

    /// Bloom filter false positive rate (default: 0.01)
    pub bloom_filter_fp_rate: f64,

    /// Number of background threads for I/O operations
    pub io_threads: usize,

    /// Sync mode for durability
    pub sync_mode: SyncMode,

    /// Memory-map SSTable Data.db files instead of using buffered file I/O.
    ///
    /// **Opt-in.** Defaults to `false` (buffered I/O), which is portable and
    /// safe on every filesystem. When enabled, the reader maps Data.db files at
    /// or above [`Self::mmap_min_size_bytes`] into the process address space and
    /// serves reads from the OS page cache with no per-block `read` syscall,
    /// mirroring Cassandra's `disk_access_mode: mmap`. This speeds up repeated
    /// local scans of the same files.
    ///
    /// # Safety / platform constraints
    ///
    /// A memory map aliases the file's bytes for the reader's lifetime. Only
    /// enable this when the SSTables are **immutable local files**:
    /// - Mutating, truncating, or deleting a mapped file out from under a live
    ///   reader is undefined behaviour and can raise `SIGBUS`, terminating the
    ///   process. CQLite never rewrites its own mapped inputs, but external
    ///   tools must not either.
    /// - Network and overlay filesystems (NFS, SMB, FUSE, some container
    ///   overlays) can fault mid-read after a successful map; prefer buffered
    ///   I/O there.
    ///
    /// # Interaction with the write engine (Issue #591)
    ///
    /// This setting only affects the read path. Compaction always reads its
    /// input SSTables through buffered I/O regardless of `use_mmap`, and deletes
    /// each input by removing its `TOC.txt` first (unpublishing it) before the
    /// data components, best-effort. So enabling mmap for queries is safe
    /// alongside background compaction: a compaction never holds a mapping over a
    /// file it then deletes, and on Windows a data file still pinned by a mapped
    /// reader becomes an invisible orphan (reclaimed on the next startup) rather
    /// than a failed delete or a source of duplicate rows.
    ///
    /// Can also be enabled at runtime by setting `CQLITE_USE_MMAP=1`.
    ///
    /// `#[serde(default)]` keeps configs serialized before this field existed
    /// (which omit it) deserializing successfully, defaulting to buffered I/O.
    #[serde(default = "default_use_mmap")]
    pub use_mmap: bool,

    /// Minimum Data.db file size (bytes) before [`Self::use_mmap`] takes effect.
    ///
    /// Files smaller than this use buffered I/O even when `use_mmap` is set,
    /// since the per-file mapping overhead is not worthwhile for tiny files and
    /// mapping a zero-length file is invalid. Defaults to one page (4096).
    ///
    /// `#[serde(default)]` for backward compatibility with older payloads.
    #[serde(default = "default_mmap_min_size_bytes")]
    pub mmap_min_size_bytes: usize,

    /// How the SSTable read path accesses Data.db on disk.
    ///
    /// Defaults to [`DiskAccessMode::Auto`], which sizes each Data.db file
    /// against system RAM and picks the backend automatically:
    /// - files below [`Self::mmap_min_size_bytes`] use buffered I/O (mapping a
    ///   tiny file is not worth the setup cost);
    /// - files up to [`Self::direct_io_memory_fraction`] of system memory are
    ///   **memory-mapped**, so repeated scans stay resident in the page cache;
    /// - files larger than that fraction use **direct I/O** (`O_DIRECT` on
    ///   Linux, `F_NOCACHE` on macOS), which bypasses the page cache so a
    ///   single huge scan does not evict everything else the host has cached.
    ///
    /// Set an explicit [`DiskAccessMode::Buffered`], [`DiskAccessMode::Mmap`],
    /// or [`DiskAccessMode::Direct`] to override the heuristic. The legacy
    /// [`Self::use_mmap`] flag still forces mmap when `Auto` would otherwise
    /// pick buffered, for backward compatibility.
    ///
    /// Can also be set at runtime via `CQLITE_DISK_ACCESS_MODE`
    /// (`auto` / `buffered` / `mmap` / `direct`).
    #[serde(default)]
    pub disk_access_mode: DiskAccessMode,

    /// Fraction of total system memory above which [`DiskAccessMode::Auto`]
    /// switches a file from memory-mapped to direct I/O. Defaults to `0.5`
    /// (half of RAM). Clamped to `(0.0, 1.0]`; values outside that range fall
    /// back to the default. Ignored when system memory cannot be determined
    /// (in which case `Auto` never escalates to direct I/O).
    #[serde(default = "default_direct_io_memory_fraction")]
    pub direct_io_memory_fraction: f64,

    /// Read-ahead / prefetch strategy applied to the chosen backend.
    ///
    /// Defaults to [`PrefetchMode::Auto`], which issues **no** mmap `madvise`
    /// (relying on the kernel's default read-ahead) and only enables the
    /// direct-I/O prefetch window of [`Self::direct_io_prefetch_bytes`]. Set
    /// [`PrefetchMode::Off`] to disable explicit hints (relying only on default
    /// kernel read-ahead / single-block direct reads). Can also be set via
    /// `CQLITE_PREFETCH` (`off` / `sequential` / `willneed` / `auto`).
    #[serde(default)]
    pub prefetch: PrefetchMode,

    /// Size in bytes of the read-ahead window used by the direct-I/O backend
    /// (and the buffered backend's reader capacity hint). Rounded up to the
    /// I/O alignment. Defaults to 1 MiB. Only takes effect when
    /// [`Self::prefetch`] is not [`PrefetchMode::Off`].
    #[serde(default = "default_direct_io_prefetch_bytes")]
    pub direct_io_prefetch_bytes: usize,
}

/// Selects which backend the SSTable read path uses for Data.db I/O.
///
/// See [`StorageConfig::disk_access_mode`] for the per-variant semantics and
/// the [`DiskAccessMode::Auto`] sizing heuristic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum DiskAccessMode {
    /// Size each file against system RAM and pick buffered / mmap / direct.
    #[default]
    Auto,
    /// Always use buffered file I/O through the OS page cache.
    Buffered,
    /// Always memory-map the file. Unlike the [`DiskAccessMode::Auto`] heuristic,
    /// this honors the user's explicit request and is **not** gated by
    /// [`StorageConfig::mmap_min_size_bytes`] (the size threshold only steers
    /// `Auto`); a zero-length file still falls back to buffered I/O since an
    /// empty map is invalid.
    Mmap,
    /// Always use direct I/O, bypassing the OS page cache.
    Direct,
}

/// Selects the read-ahead hint applied to the active disk-access backend.
///
/// See [`StorageConfig::prefetch`]. `Sequential` / `WillNeed` map to the
/// corresponding `madvise(2)` advice on the mmap backend; on the direct-I/O
/// backend any non-`Off` value enables the [`StorageConfig::direct_io_prefetch_bytes`]
/// read-ahead window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum PrefetchMode {
    /// No explicit prefetch hint; rely on default kernel behaviour.
    Off,
    /// Hint sequential access (aggressive read-ahead, drop-behind).
    Sequential,
    /// Hint that the mapped/region bytes will be needed soon (eager fault-in).
    WillNeed,
    /// Let the backend choose. For mmap this issues **no** madvise and relies on
    /// the kernel's default read-ahead: `MADV_SEQUENTIAL`'s drop-behind evicts
    /// hot pages under concurrent write load and inflates the read-side p99 tail
    /// (issue #1143), so `Auto` avoids it while keeping the isolated mmap win.
    /// For direct I/O it enables the windowed read-ahead
    /// ([`StorageConfig::direct_io_prefetch_bytes`]). Request
    /// [`PrefetchMode::Sequential`] explicitly for `MADV_SEQUENTIAL` behaviour.
    #[default]
    Auto,
}

/// Default for [`StorageConfig::use_mmap`]: mmap is opt-in, so buffered I/O.
fn default_use_mmap() -> bool {
    false
}

/// Default for [`StorageConfig::mmap_min_size_bytes`]: one page.
fn default_mmap_min_size_bytes() -> usize {
    4096
}

/// Default for [`StorageConfig::direct_io_memory_fraction`]: half of RAM.
fn default_direct_io_memory_fraction() -> f64 {
    0.5
}

/// Default for [`StorageConfig::direct_io_prefetch_bytes`]: 1 MiB.
fn default_direct_io_prefetch_bytes() -> usize {
    1024 * 1024
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            max_sstable_size: 64 * 1024 * 1024,        // 64MB
            memtable_size_threshold: 16 * 1024 * 1024, // 16MB
            compaction: CompactionConfig::default(),
            block_size: 64 * 1024, // 64KB
            compression: CompressionConfig::default(),
            enable_bloom_filters: true,
            bloom_filter_fp_rate: 0.01,
            io_threads: num_cpus::get().min(4),
            sync_mode: SyncMode::Normal,
            // Opt-in; buffered I/O is the portable, safe default. Shared with
            // the serde defaults so the two can never drift.
            use_mmap: default_use_mmap(),
            mmap_min_size_bytes: default_mmap_min_size_bytes(),
            disk_access_mode: DiskAccessMode::default(),
            direct_io_memory_fraction: default_direct_io_memory_fraction(),
            prefetch: PrefetchMode::default(),
            direct_io_prefetch_bytes: default_direct_io_prefetch_bytes(),
        }
    }
}

/// Compaction strategy configuration.
///
/// The write engine implements Size-Tiered Compaction Strategy (STCS) and
/// installs it by default (issue #1619). `auto_compaction` is the authoritative
/// on/off switch consumed by the write path via
/// `WriteEngineConfig::with_compaction_config`. Previously this struct also
/// carried `strategy`/`max_sstables`/`size_ratio`/`max_threads`/
/// `background_interval` fields that were never read by any behavior; they were
/// removed (issue #1619) rather than left decorative.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionConfig {
    /// Enable automatic (STCS) compaction. When `false`, the write engine
    /// installs no merge policy and `maintenance_step` is a no-op.
    pub auto_compaction: bool,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            auto_compaction: true,
        }
    }
}

/// Memory management configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryConfig {
    /// Maximum total memory usage (default: 1GB)
    pub max_memory: u64,

    /// Block cache configuration
    pub block_cache: CacheConfig,

    /// Row cache configuration  
    pub row_cache: CacheConfig,

    /// Query result cache configuration
    pub query_cache: CacheConfig,

    /// Memory allocator settings
    pub allocator: AllocatorConfig,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        let max_memory = 1024 * 1024 * 1024; // 1GB

        Self {
            max_memory,
            block_cache: CacheConfig {
                enabled: true,
                max_size: max_memory / 4, // 256MB
                policy: CachePolicy::Lru,
            },
            row_cache: CacheConfig {
                enabled: true,
                max_size: max_memory / 8, // 128MB
                policy: CachePolicy::Lru,
            },
            query_cache: CacheConfig {
                enabled: true,
                max_size: max_memory / 16, // 64MB
                policy: CachePolicy::Lru,
            },
            allocator: AllocatorConfig::default(),
        }
    }
}

/// Cache configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Enable this cache
    pub enabled: bool,

    /// Maximum cache size in bytes
    pub max_size: u64,

    /// Cache eviction policy
    pub policy: CachePolicy,
}

/// Cache eviction policies
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CachePolicy {
    /// Least Recently Used
    Lru,
    /// Least Frequently Used
    Lfu,
    /// Adaptive Replacement Cache
    Arc,
}

/// Memory allocator configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocatorConfig {
    /// Use custom allocator for better performance
    pub use_custom: bool,

    /// Pool size for small allocations
    pub small_pool_size: u64,

    /// Pool size for large allocations
    pub large_pool_size: u64,
}

impl Default for AllocatorConfig {
    fn default() -> Self {
        Self {
            use_custom: false,                  // Conservative default
            small_pool_size: 64 * 1024 * 1024,  // 64MB
            large_pool_size: 256 * 1024 * 1024, // 256MB
        }
    }
}

/// Default byte ceiling for a materialized SELECT result set (issue #1582).
///
/// 64 MiB. See [`QueryConfig::max_result_bytes`] for the derivation from the
/// project's <128MB process memory target.
pub const DEFAULT_MAX_RESULT_BYTES: u64 = 64 * 1024 * 1024;

/// Serde default for [`QueryConfig::max_result_bytes`] (issue #1582).
///
/// Backward-compat: a `QueryConfig` serialized before this field existed (e.g.
/// a Python JSON/dict config) has no `max_result_bytes` key. Without a serde
/// default, deserialization fails with a missing-field error; with it, such a
/// config takes the shipped [`DEFAULT_MAX_RESULT_BYTES`] budget.
fn default_max_result_bytes() -> u64 {
    DEFAULT_MAX_RESULT_BYTES
}

/// Serde default for [`QueryConfig::max_result_rows`] (issue #1582).
///
/// Backward-compat + robustness: a `QueryConfig` serialized without this key
/// (or a partial JSON/dict config) still deserializes, taking the shipped
/// 1,000,000-row secondary safety valve rather than failing with a missing
/// field. Keeps the knob real (not decorative) and consistent with
/// [`default_max_result_bytes`].
fn default_max_result_rows() -> u64 {
    1_000_000
}

/// Query engine configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryConfig {
    /// Maximum query execution time
    pub max_execution_time: Duration,

    /// Maximum number of rows to return in a result set.
    ///
    /// A *secondary* safety valve, retained for defense-in-depth (issue #1582).
    /// The primary guard on a materialized result is now `max_result_bytes`: a
    /// row count is the wrong unit because 1M skinny rows can fit comfortably
    /// while 100k wide rows blow the <128MB memory target. Still load-bearing:
    /// the materializing SELECT path enforces this row-count ceiling alongside
    /// the byte budget (lowering it makes a wide-row-count result trip even
    /// under the byte budget), so it is a real knob, not decoration.
    #[serde(default = "default_max_result_rows")]
    pub max_result_rows: u64,

    /// Byte ceiling on a MATERIALIZED result set (issue #1582 / D6).
    ///
    /// While the SELECT executor collects a materialized `Vec<QueryRow>`, it
    /// tracks a running estimate of the result's logical size (via the shared
    /// [`crate::memory::estimate_row_size`] estimator) and fails with
    /// [`crate::Error::ResultTooLarge`] once this ceiling is crossed — telling
    /// the caller to add a `LIMIT` or use the streaming API. This is the
    /// correct-unit primary guard; `max_result_rows` remains as a secondary
    /// valve. Streaming queries are bounded by their channel buffer, so this
    /// budget does not apply to them.
    ///
    /// Default: [`DEFAULT_MAX_RESULT_BYTES`] (64 MiB). Chosen well below the
    /// project's <128MB process memory target: the estimator measures *logical*
    /// content bytes and does not count per-row container overhead
    /// (`HashMap<Arc<str>, Value>` slots, `String`/`Vec` capacity slack, row
    /// metadata), which in practice roughly doubles real heap use — so a 64 MiB
    /// logical ceiling keeps a fully-materialized result comfortably inside the
    /// process budget while leaving headroom for readers, caches, and decode
    /// buffers.
    #[serde(default = "default_max_result_bytes")]
    pub max_result_bytes: u64,

    /// Query plan cache size
    pub plan_cache_size: usize,

    /// Enable query optimization
    pub enable_optimization: bool,

    /// Parallel query execution configuration
    pub parallel: ParallelQueryConfig,

    /// Query cache size (for plan caching)
    pub query_cache_size: Option<usize>,

    /// Query parallelism thread count
    pub query_parallelism: Option<usize>,

    /// Number of iterations for query analysis
    pub analyze_iterations: Option<usize>,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            max_execution_time: Duration::from_secs(300), // 5 minutes
            max_result_rows: 1_000_000,
            max_result_bytes: DEFAULT_MAX_RESULT_BYTES,
            plan_cache_size: 1000,
            enable_optimization: true,
            parallel: ParallelQueryConfig::default(),
            query_cache_size: Some(100),
            query_parallelism: Some(num_cpus::get()),
            analyze_iterations: Some(5),
        }
    }
}

/// Parallel query execution configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParallelQueryConfig {
    /// Enable parallel query execution
    pub enabled: bool,

    /// Maximum number of parallel threads
    pub max_threads: usize,

    /// Minimum result set size to trigger parallel execution
    pub min_parallel_rows: u64,
}

impl Default for ParallelQueryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_threads: num_cpus::get(),
            min_parallel_rows: 10_000,
        }
    }
}

/// Performance and optimization configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    /// Enable performance metrics collection
    pub enable_metrics: bool,

    /// Metrics collection interval
    pub metrics_interval: Duration,

    /// Enable detailed profiling
    pub enable_profiling: bool,

    /// Background task configuration
    pub background_tasks: BackgroundTaskConfig,
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            enable_metrics: true,
            metrics_interval: Duration::from_secs(60),
            enable_profiling: false,
            background_tasks: BackgroundTaskConfig::default(),
        }
    }
}

/// Background task configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackgroundTaskConfig {
    /// Enable background statistics collection
    pub enable_stats: bool,

    /// Statistics collection interval
    pub stats_interval: Duration,

    /// Enable background cleanup tasks
    pub enable_cleanup: bool,

    /// Cleanup task interval
    pub cleanup_interval: Duration,
}

impl Default for BackgroundTaskConfig {
    fn default() -> Self {
        Self {
            enable_stats: true,
            stats_interval: Duration::from_secs(300), // 5 minutes
            enable_cleanup: true,
            cleanup_interval: Duration::from_secs(3600), // 1 hour
        }
    }
}

/// WASM-specific configuration
#[cfg(target_arch = "wasm32")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmConfig {
    /// Use IndexedDB for persistent storage
    pub use_indexeddb: bool,

    /// Maximum memory usage in WASM (default: 256MB)
    pub max_memory: u64,

    /// Enable WASM SIMD optimizations
    pub enable_simd: bool,

    /// Enable Web Workers for background tasks
    pub enable_workers: bool,

    /// Maximum number of Web Workers
    pub max_workers: usize,
}

#[cfg(target_arch = "wasm32")]
impl Default for WasmConfig {
    fn default() -> Self {
        Self {
            use_indexeddb: true,
            max_memory: 256 * 1024 * 1024, // 256MB
            enable_simd: true,
            enable_workers: true,
            max_workers: 4,
        }
    }
}

/// Compression algorithms
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompressionAlgorithm {
    /// No compression
    None,
    /// LZ4 compression (fast)
    Lz4,
    /// Snappy compression (balanced)
    Snappy,
    /// Deflate compression (good compression ratio)
    Deflate,
    /// ZSTD compression (high compression ratio)
    Zstd,
}

/// Compression configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionConfig {
    /// Enable compression
    pub enabled: bool,

    /// Compression algorithm to use
    pub algorithm: CompressionAlgorithm,

    /// Compression level (algorithm-specific)
    pub level: i32,

    /// Minimum block size to compress (smaller blocks are stored uncompressed)
    pub min_block_size: u32,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            algorithm: CompressionAlgorithm::Lz4,
            level: 1,             // Fast compression
            min_block_size: 1024, // 1KB minimum
        }
    }
}

/// Durability sync modes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncMode {
    /// No explicit syncing (fastest, least durable)
    None,
    /// Normal syncing (balanced)
    Normal,
    /// Full sync for every write (slowest, most durable)
    Full,
}

impl Config {
    /// Create a configuration optimized for memory usage
    pub fn memory_optimized() -> Self {
        let mut config = Self::default();

        // Reduce memory usage
        config.storage.memtable_size_threshold = 4 * 1024 * 1024; // 4MB
        config.storage.max_sstable_size = 16 * 1024 * 1024; // 16MB
        config.memory.max_memory = 256 * 1024 * 1024; // 256MB
        config.memory.block_cache.max_size = 64 * 1024 * 1024; // 64MB
        config.memory.row_cache.max_size = 32 * 1024 * 1024; // 32MB
        config.memory.query_cache.max_size = 16 * 1024 * 1024; // 16MB

        // Enable aggressive compression
        config.storage.compression.algorithm = CompressionAlgorithm::Zstd;
        config.storage.compression.enabled = true;

        config
    }

    /// Create a configuration optimized for performance
    pub fn performance_optimized() -> Self {
        let mut config = Self::default();

        // Increase memory usage for better performance
        config.storage.memtable_size_threshold = 64 * 1024 * 1024; // 64MB
        config.storage.max_sstable_size = 256 * 1024 * 1024; // 256MB
        config.memory.max_memory = 4 * 1024 * 1024 * 1024; // 4GB

        // Use faster compression
        config.storage.compression.algorithm = CompressionAlgorithm::Lz4;
        config.storage.compression.enabled = true;

        // More aggressive caching
        config.memory.block_cache.max_size = 1024 * 1024 * 1024; // 1GB
        config.memory.row_cache.max_size = 512 * 1024 * 1024; // 512MB
        config.memory.query_cache.max_size = 256 * 1024 * 1024; // 256MB

        // More I/O threads
        config.storage.io_threads = num_cpus::get();

        config
    }

    /// Create a configuration optimized for WASM deployment
    #[cfg(target_arch = "wasm32")]
    pub fn wasm_optimized() -> Self {
        let mut config = Self::memory_optimized();

        // WASM-specific optimizations
        config.wasm.max_memory = 128 * 1024 * 1024; // 128MB
        config.wasm.enable_simd = true;
        config.wasm.enable_workers = false; // Conservative default

        // Reduce overall memory usage for WASM
        config.memory.max_memory = 128 * 1024 * 1024; // 128MB
        config.storage.memtable_size_threshold = 2 * 1024 * 1024; // 2MB
        config.storage.max_sstable_size = 8 * 1024 * 1024; // 8MB

        // Disable background tasks that may not work well in WASM
        config.storage.compaction.auto_compaction = false;
        config.performance.background_tasks.enable_stats = false;
        config.performance.background_tasks.enable_cleanup = false;

        config
    }

    /// Create a test-optimized configuration
    #[cfg(test)]
    pub fn test_config() -> Self {
        let mut config = Config::default();

        // Disable background tasks that can cause test hangs
        config.storage.compaction.auto_compaction = false;
        config.performance.background_tasks.enable_stats = false;
        config.performance.background_tasks.enable_cleanup = false;

        // Reduce timeouts for faster test execution
        config.query.max_execution_time = std::time::Duration::from_secs(1);

        // Smaller memory usage for tests
        config.memory.max_memory = 64 * 1024 * 1024; // 64MB
        config.storage.memtable_size_threshold = 1024 * 1024; // 1MB
        config.storage.max_sstable_size = 4 * 1024 * 1024; // 4MB

        config
    }

    /// Validate the configuration
    pub fn validate(&self) -> crate::Result<()> {
        // Validate memory limits
        if self.memory.max_memory == 0 {
            return Err(crate::Error::configuration(
                "max_memory must be greater than 0",
            ));
        }

        // Validate cache sizes don't exceed total memory
        let total_cache = self.memory.block_cache.max_size
            + self.memory.row_cache.max_size
            + self.memory.query_cache.max_size;

        if total_cache > self.memory.max_memory {
            return Err(crate::Error::configuration(
                "total cache size exceeds max_memory",
            ));
        }

        // Validate storage settings
        if self.storage.block_size == 0 {
            return Err(crate::Error::configuration(
                "block_size must be greater than 0",
            ));
        }

        if self.storage.memtable_size_threshold == 0 {
            return Err(crate::Error::configuration(
                "memtable_size_threshold must be greater than 0",
            ));
        }

        // Validate bloom filter settings
        if self.storage.enable_bloom_filters
            && (self.storage.bloom_filter_fp_rate <= 0.0
                || self.storage.bloom_filter_fp_rate >= 1.0)
        {
            return Err(crate::Error::configuration(
                "bloom_filter_fp_rate must be between 0 and 1",
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert!(config.storage.compression.enabled);
        assert!(config.storage.enable_bloom_filters);
        assert!(config.memory.block_cache.enabled);
    }

    #[test]
    fn test_memory_optimized_config() {
        let config = Config::memory_optimized();
        assert!(
            config.storage.memtable_size_threshold
                < Config::default().storage.memtable_size_threshold
        );
        assert!(config.memory.max_memory < Config::default().memory.max_memory);
    }

    #[test]
    fn test_performance_optimized_config() {
        let config = Config::performance_optimized();
        assert!(
            config.storage.memtable_size_threshold
                > Config::default().storage.memtable_size_threshold
        );
        assert!(config.memory.max_memory > Config::default().memory.max_memory);
    }

    /// Issue #1582: a `QueryConfig` serialized BEFORE the byte-budget fields
    /// existed (e.g. a pre-upgrade Python JSON/dict config) has no
    /// `max_result_bytes`/`max_result_rows` keys. The `#[serde(default = ...)]`
    /// on both fields must let it deserialize, taking the shipped defaults rather
    /// than failing with a missing-field error.
    #[test]
    fn budget_fields_deserialize_with_serde_default_when_absent() {
        // Serialize a default QueryConfig, then STRIP both budget fields to
        // emulate an old serialized config that predates them.
        let mut value =
            serde_json::to_value(QueryConfig::default()).expect("serialize QueryConfig");
        let obj = value
            .as_object_mut()
            .expect("QueryConfig serializes as object");
        obj.remove("max_result_bytes");
        obj.remove("max_result_rows");
        assert!(
            !obj.contains_key("max_result_bytes") && !obj.contains_key("max_result_rows"),
            "both fields must be absent for this regression to be meaningful"
        );

        let restored: QueryConfig = serde_json::from_value(value)
            .expect("old config (no budget fields) must deserialize");
        assert_eq!(
            restored.max_result_bytes, DEFAULT_MAX_RESULT_BYTES,
            "absent max_result_bytes must take the serde default"
        );
        assert_eq!(
            restored.max_result_rows,
            default_max_result_rows(),
            "absent max_result_rows must take the serde default"
        );
    }

    #[test]
    fn test_config_validation() {
        let mut config = Config::default();
        assert!(config.validate().is_ok());

        // Test invalid max_memory
        config.memory.max_memory = 0;
        assert!(config.validate().is_err());

        // Reset and test invalid cache sizes
        config = Config::default();
        config.memory.block_cache.max_size = config.memory.max_memory + 1;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_storage_validation_errors() {
        let mut config = Config::default();

        // Test invalid block_size (should trigger line 573-574)
        config.storage.block_size = 0;
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("block_size must be greater than 0"));

        // Reset and test invalid memtable_size_threshold (should trigger line 579-580)
        config = Config::default();
        config.storage.memtable_size_threshold = 0;
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("memtable_size_threshold must be greater than 0"));

        // Reset and test invalid bloom filter false positive rate (should trigger line 589-590)
        config = Config::default();
        config.storage.enable_bloom_filters = true;
        config.storage.bloom_filter_fp_rate = 0.0; // Invalid: exactly 0
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("bloom_filter_fp_rate must be between 0 and 1"));

        // Test another invalid bloom filter false positive rate
        config.storage.bloom_filter_fp_rate = 1.0; // Invalid: exactly 1
        let result = config.validate();
        assert!(result.is_err());

        // Test bloom filter rate above 1
        config.storage.bloom_filter_fp_rate = 1.5; // Invalid: greater than 1
        let result = config.validate();
        assert!(result.is_err());

        // Test bloom filter rate below 0
        config.storage.bloom_filter_fp_rate = -0.1; // Invalid: less than 0
        let result = config.validate();
        assert!(result.is_err());
    }

    #[test]
    fn test_valid_bloom_filter_config() {
        let mut config = Config::default();
        config.storage.enable_bloom_filters = true;
        config.storage.bloom_filter_fp_rate = 0.01; // Valid rate
        assert!(config.validate().is_ok());

        config.storage.bloom_filter_fp_rate = 0.5; // Valid rate
        assert!(config.validate().is_ok());

        config.storage.bloom_filter_fp_rate = 0.99; // Valid rate
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_storage_config_deserializes_without_mmap_fields() {
        // Backward compatibility: a config payload serialized before the mmap
        // fields existed omits `use_mmap` / `mmap_min_size_bytes`. It must still
        // deserialize, defaulting to the safe buffered backend.
        let mut value = serde_json::to_value(StorageConfig::default()).unwrap();
        let obj = value.as_object_mut().unwrap();
        obj.remove("use_mmap");
        obj.remove("mmap_min_size_bytes");
        assert!(!obj.contains_key("use_mmap"));

        let restored: StorageConfig =
            serde_json::from_value(value).expect("old payload must still deserialize");
        assert!(!restored.use_mmap, "missing use_mmap must default to false");
        assert_eq!(
            restored.mmap_min_size_bytes, 4096,
            "missing mmap_min_size_bytes must default to one page"
        );
    }

    #[test]
    fn test_full_config_deserializes_without_mmap_fields() {
        // Same guarantee through the top-level Config, mirroring how the Python
        // bindings parse a JSON/dict payload into `cqlite_core::Config`.
        let mut value = serde_json::to_value(Config::default()).unwrap();
        let storage = value
            .get_mut("storage")
            .and_then(|s| s.as_object_mut())
            .unwrap();
        storage.remove("use_mmap");
        storage.remove("mmap_min_size_bytes");

        let restored: Config =
            serde_json::from_value(value).expect("old Config payload must still deserialize");
        assert!(!restored.storage.use_mmap);
        assert_eq!(restored.storage.mmap_min_size_bytes, 4096);
        restored.validate().expect("restored config must validate");
    }

    #[test]
    fn test_mmap_fields_roundtrip_when_present() {
        // When the fields ARE present (e.g. a user opting in), they round-trip.
        let mut config = StorageConfig::default();
        config.use_mmap = true;
        config.mmap_min_size_bytes = 8192;
        let json = serde_json::to_string(&config).unwrap();
        let restored: StorageConfig = serde_json::from_str(&json).unwrap();
        assert!(restored.use_mmap);
        assert_eq!(restored.mmap_min_size_bytes, 8192);
    }

    #[test]
    fn test_disk_access_defaults() {
        // The new fields default to the size-aware Auto backend with Auto
        // prefetch, a half-RAM direct-I/O threshold, and a 1 MiB window.
        let config = StorageConfig::default();
        assert_eq!(config.disk_access_mode, DiskAccessMode::Auto);
        assert_eq!(config.prefetch, PrefetchMode::Auto);
        assert_eq!(config.direct_io_memory_fraction, 0.5);
        assert_eq!(config.direct_io_prefetch_bytes, 1024 * 1024);
    }

    #[test]
    fn test_storage_config_deserializes_without_disk_access_fields() {
        // Backward compatibility: a payload predating the disk-access fields must
        // still deserialize, defaulting to Auto / Auto / 0.5 / 1 MiB.
        let mut value = serde_json::to_value(StorageConfig::default()).unwrap();
        let obj = value.as_object_mut().unwrap();
        for key in [
            "disk_access_mode",
            "direct_io_memory_fraction",
            "prefetch",
            "direct_io_prefetch_bytes",
        ] {
            obj.remove(key);
        }

        let restored: StorageConfig =
            serde_json::from_value(value).expect("old payload must still deserialize");
        assert_eq!(restored.disk_access_mode, DiskAccessMode::Auto);
        assert_eq!(restored.prefetch, PrefetchMode::Auto);
        assert_eq!(restored.direct_io_memory_fraction, 0.5);
        assert_eq!(restored.direct_io_prefetch_bytes, 1024 * 1024);
    }

    #[test]
    fn test_disk_access_fields_roundtrip() {
        // Explicit selections round-trip, including the lowercase enum encoding.
        let mut config = StorageConfig::default();
        config.disk_access_mode = DiskAccessMode::Direct;
        config.prefetch = PrefetchMode::WillNeed;
        config.direct_io_memory_fraction = 0.25;
        config.direct_io_prefetch_bytes = 2 * 1024 * 1024;
        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains("\"direct\""), "enum must serialize lowercase");
        assert!(json.contains("\"willneed\""));
        let restored: StorageConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.disk_access_mode, DiskAccessMode::Direct);
        assert_eq!(restored.prefetch, PrefetchMode::WillNeed);
        assert_eq!(restored.direct_io_memory_fraction, 0.25);
        assert_eq!(restored.direct_io_prefetch_bytes, 2 * 1024 * 1024);
    }

    #[test]
    fn test_bloom_filter_disabled() {
        let mut config = Config::default();
        config.storage.enable_bloom_filters = false;
        config.storage.bloom_filter_fp_rate = 0.0; // Should be ignored when bloom filters disabled
        assert!(config.validate().is_ok());

        config.storage.bloom_filter_fp_rate = 1.0; // Should be ignored when bloom filters disabled
        assert!(config.validate().is_ok());

        config.storage.bloom_filter_fp_rate = -1.0; // Should be ignored when bloom filters disabled
        assert!(config.validate().is_ok());
    }
}
