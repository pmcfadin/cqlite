//! Configuration management for CQLite

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Main configuration structure for CQLite database
#[derive(Debug, Clone, Serialize, Deserialize)]
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

impl Default for Config {
    fn default() -> Self {
        Self {
            storage: StorageConfig::default(),
            memory: MemoryConfig::default(),
            query: QueryConfig::default(),
            performance: PerformanceConfig::default(),

            #[cfg(target_arch = "wasm32")]
            wasm: WasmConfig::default(),
        }
    }
}

/// Storage engine configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Maximum SSTable file size in bytes (default: 64MB)
    pub max_sstable_size: u64,

    /// MemTable size threshold for flushing (default: 16MB)
    pub memtable_size_threshold: u64,

    /// Write-ahead log (WAL) configuration
    pub wal: WalConfig,

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
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            max_sstable_size: 64 * 1024 * 1024,        // 64MB
            memtable_size_threshold: 16 * 1024 * 1024, // 16MB
            wal: WalConfig::default(),
            compaction: CompactionConfig::default(),
            block_size: 64 * 1024, // 64KB
            compression: CompressionConfig::default(),
            enable_bloom_filters: true,
            bloom_filter_fp_rate: 0.01,
            io_threads: num_cpus::get().min(4),
            sync_mode: SyncMode::Normal,
        }
    }
}

/// Write-ahead log configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalConfig {
    /// Enable WAL for durability
    pub enabled: bool,

    /// Maximum WAL file size (default: 32MB)
    pub max_file_size: u64,

    /// Sync WAL writes to disk
    pub sync_writes: bool,

    /// WAL sync interval for async writes
    pub sync_interval: Duration,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_file_size: 32 * 1024 * 1024, // 32MB
            sync_writes: false,
            sync_interval: Duration::from_millis(100),
        }
    }
}

/// Compaction strategy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionConfig {
    /// Compaction strategy to use
    pub strategy: CompactionStrategy,

    /// Maximum number of SSTables before triggering compaction
    pub max_sstables: usize,

    /// Size ratio for triggering compaction
    pub size_ratio: f64,

    /// Maximum compaction threads
    pub max_threads: usize,

    /// Compaction interval for background compaction
    pub background_interval: Duration,

    /// Enable automatic background compaction
    pub auto_compaction: bool,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            strategy: CompactionStrategy::Leveled,
            max_sstables: 10,
            size_ratio: 2.0,
            max_threads: 2,
            background_interval: Duration::from_secs(300), // 5 minutes
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

/// Query engine configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryConfig {
    /// Maximum query execution time
    pub max_execution_time: Duration,

    /// Maximum number of rows to return in a result set
    pub max_result_rows: u64,

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

/// Compaction strategies
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompactionStrategy {
    /// Simple size-based compaction
    Size,
    /// Leveled compaction (like LevelDB)
    Leveled,
    /// Universal compaction
    Universal,
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
}
