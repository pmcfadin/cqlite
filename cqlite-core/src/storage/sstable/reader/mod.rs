//! SSTable reader implementation
//!
//! This module provides efficient reading of SSTable files in Cassandra 5+ format.
//! It supports:
//! - Block-based reading with compression
//! - Index-based lookups for efficient queries
//! - Memory-efficient streaming
//! - Bloom filter integration
//! - Multiple compression algorithms

// Submodules
mod block_io;
mod cache;
mod component_loading;
mod compression;
mod data_access;
mod header;
mod header_helpers;
mod integrity;
mod key_digest;
pub(crate) mod parsing; // Needs to be accessible from row_cell_state_machine
mod partition_lookup;
mod source;
#[cfg(test)]
mod tests;
mod types;

// Re-export public types
pub use types::{
    BlockMeta, CachedBlock, IntegrityCheckResult, IntegrityStatus, SSTableReader,
    SSTableReaderConfig, SSTableReaderHealthMetrics, SSTableReaderStats,
};

// Re-export V5CompressedLegacyParser for integration testing (Issue #166 regression tests)
#[doc(hidden)]
pub use parsing::PublicV5CompressedLegacyParser as V5CompressedLegacyParser;

// Re-export compression utilities for testing (Issue #202)
#[doc(hidden)]
pub use compression::extract_sstable_base_name;

// Internal imports from submodules
use compression::detect_and_initialize_compression;
use header::{
    calculate_actual_header_size, extract_generation_from_path, parse_header_with_version_detection,
};

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::Mutex;

use source::BlockSource;

use crate::{
    parser::{header::CassandraVersion, SSTableHeader, SSTableParser},
    platform::Platform,
    schema::TableSchema,
    storage::sstable::{
        compression_info::CompressionInfo,
        version_gate::{BigVersionGates, VersionGates},
    },
    Config, Error, Result, RowKey, Value,
};

// Structured logging
use log::debug;

#[cfg(feature = "tombstones")]
use super::tombstone_merger::TombstoneMerger;

/// Returns `true` when memory-mapped reads are force-enabled via the
/// `CQLITE_USE_MMAP` environment variable.
///
/// Accepts `1`, `true`, `yes`, `on` (case-insensitive). Any other value — or
/// an unset variable — leaves the decision to [`Config`]. This is an opt-in
/// escape hatch for ad-hoc local use without threading a custom config.
fn mmap_enabled_via_env() -> bool {
    std::env::var("CQLITE_USE_MMAP")
        .ok()
        .as_deref()
        .map(parse_truthy_env)
        .unwrap_or(false)
}

/// Parse a truthy environment-variable value (`1`/`true`/`yes`/`on`,
/// case-insensitive). Split out so it can be unit-tested without mutating the
/// process-global environment (which would race other `open()` tests).
fn parse_truthy_env(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

impl SSTableReader {
    /// Open an SSTable file for reading
    pub async fn open(path: &Path, config: &Config, platform: Arc<Platform>) -> Result<Self> {
        // Honor the caller's storage config for the mmap decision. Memory
        // mapping is opt-in (buffered I/O is the portable default); it can be
        // turned on via `config.storage.use_mmap` or the `CQLITE_USE_MMAP`
        // environment variable. See `Config::storage.use_mmap` for the
        // platform/filesystem safety constraints.
        let mut reader_config = SSTableReaderConfig::default();
        reader_config.use_mmap = config.storage.use_mmap || mmap_enabled_via_env();
        reader_config.mmap_min_size_bytes = config.storage.mmap_min_size_bytes;

        let file_size = tokio::fs::metadata(path).await?.len();

        // Select the backing store for block I/O. Memory-map the file when mmap
        // is enabled and the file is large enough to be worth it; otherwise use
        // buffered file I/O. Mapping a zero-length file is invalid, so empty
        // files always fall back to buffered reads.
        let use_mmap = reader_config.use_mmap
            && file_size > 0
            && file_size >= reader_config.mmap_min_size_bytes as u64;
        let source = if use_mmap {
            match Self::map_file(path) {
                Ok(mmap) => {
                    log::debug!(
                        "Opened {} via memory map ({} bytes)",
                        path.display(),
                        file_size
                    );
                    BlockSource::mapped(Arc::new(mmap))
                }
                Err(e) => {
                    // Memory mapping can fail on some filesystems (e.g. certain
                    // network mounts). Degrade gracefully to buffered I/O rather
                    // than failing the open outright.
                    log::warn!(
                        "Memory-mapping {} failed ({}); falling back to buffered I/O",
                        path.display(),
                        e
                    );
                    BlockSource::buffered(File::open(path).await?)
                }
            }
        } else {
            BlockSource::buffered(File::open(path).await?)
        };
        let file = Arc::new(Mutex::new(source));

        // Parse header - read available bytes, not a fixed size
        // NOTE: For NB format files (Cassandra 4.x+), Data.db often contains compressed row data
        // with no embedded header. The header.rs module detects this via filename pattern and
        // returns a minimal header loaded from Statistics.db instead.
        let header_size = std::cmp::min(4096, file_size as usize);
        let mut header_buffer = vec![0u8; header_size];
        {
            let mut file_guard = file.lock().await;
            let bytes_read = file_guard.read(&mut header_buffer).await?;
            header_buffer.truncate(bytes_read);
        }

        // Derive VersionGates from the SSTable filename BEFORE header parsing so
        // parse_header_with_version_detection can receive them.  Gates are derived
        // solely from the filename and need no file I/O, so this is safe to do here.
        //
        // Falls back to nb-compatible BIG gates when the filename is not a valid
        // SSTable descriptor (e.g. paths used in unit tests).  Using nb-fallback
        // maintains existing behaviour — the gates will not change parsing
        // decisions until VG3 actually flips behaviour.
        let version_gates = Arc::new(match VersionGates::from_path(path) {
            Ok(gates) => gates,
            Err(e) => {
                log::debug!(
                    "SSTableReader::open: could not derive VersionGates from {:?} ({}); \
                     defaulting to nb-compatible BIG gates",
                    path,
                    e
                );
                VersionGates::Big(BigVersionGates::nb_fallback())
            }
        });

        // VG5: BTI (da) read support is not yet implemented.
        // Detect the BTI format early — before header parsing — and return a
        // structured, actionable error rather than a confusing parse failure.
        // Full BTI reading is tracked in the scoping issue created by issue #657.
        if matches!(*version_gates, VersionGates::Bti(_)) {
            return Err(Error::unsupported_format(format!(
                "BTI (da) read support not yet implemented for '{}'. \
                 da-format SSTables use Partitions.db/Rows.db trie indexes instead of \
                 Index.db/Summary.db and require a dedicated BTI read path. \
                 See docs/reports/bti-read-support-scoping.md for the implementation plan.",
                path.display()
            )));
        }

        let config = crate::cql::config::ParserConfig::default();
        let parser = SSTableParser::new(config)?;
        // Parse the header using enhanced version detection - strict error propagation.
        // VersionGates are passed so VG3 can flip version-sensitive parsing decisions
        // inside header parsing without re-deriving gates from the filename.
        let header = parse_header_with_version_detection(&header_buffer, path, &version_gates)
            .await
            .map_err(|e| {
                Error::corruption(format!(
                    "Failed to parse SSTable header for file '{}': {}. This indicates either \
                     file corruption or an unsupported SSTable format. File size: {} bytes, \
                     header buffer size: {} bytes.",
                    path.display(),
                    e,
                    file_size,
                    header_buffer.len()
                ))
            })?;
        let header_size = calculate_actual_header_size(&header, &header_buffer)?;

        // Schema extraction deferred until after Statistics.db columns are loaded (Issue #163)
        // See schema extraction code after statistics_reader loading below

        // Seek to start of data section
        {
            let mut file_guard = file.lock().await;
            file_guard
                .seek(std::io::SeekFrom::Start(header_size as u64))
                .await?;
        }

        // Initialize compression reader with improved format detection
        let compression_reader = detect_and_initialize_compression(&header, path).await?;

        // Load CompressionInfo.db for chunked decompression (if it exists)
        let compression_info = Self::load_compression_info_metadata(path, &platform).await?;

        // Pre-validate component architecture for better error handling
        let components = Self::detect_component_files(path).await?;
        if !components.is_empty() {
            let integrity_issues = Self::validate_component_integrity(path, &components).await?;
            if !integrity_issues.is_empty() {
                log::warn!(
                    "Component integrity issues detected but proceeding with loading: {:?}",
                    integrity_issues
                );
            }
        }

        // Load index if available (supports both integrated and component-based)
        let index = Self::load_index(&file, &header, &platform, path).await?;

        // Load bloom filter if available (supports both integrated and component-based)
        let bloom_filter = Self::load_bloom_filter(&file, &header, &platform, path).await?;

        // Load spec readers for enhanced metadata and lookups
        let index_reader = Self::load_index_reader(path, &platform).await;
        let summary_reader = Self::load_summary_reader(path, &platform).await;
        let statistics_reader = Self::load_statistics_reader(path, &platform).await;

        // Extract SerializationHeader columns from Statistics.db (Issue #163)
        // This enables schema extraction for V5CompressedLegacy format
        let mut header = header; // Make mutable to populate columns
        if let Some(ref stats_reader) = statistics_reader {
            let statistics = stats_reader.statistics();
            let partition_columns = &statistics.serialization_header_partition_keys;
            let clustering_columns = &statistics.serialization_header_clustering_keys;
            let regular_columns = &statistics.serialization_header_columns;

            if !partition_columns.is_empty()
                || !clustering_columns.is_empty()
                || !regular_columns.is_empty()
            {
                log::debug!(
                    "Populating header columns from Statistics.db SerializationHeader: {} partition keys, {} clustering keys, {} regular columns",
                    partition_columns.len(),
                    clustering_columns.len(),
                    regular_columns.len()
                );

                let mut merged_columns = Vec::with_capacity(
                    partition_columns.len() + clustering_columns.len() + regular_columns.len(),
                );
                merged_columns.extend_from_slice(partition_columns);
                merged_columns.extend_from_slice(clustering_columns);
                merged_columns.extend_from_slice(regular_columns);

                header.columns = merged_columns;
            }
        }

        // Extract schema from header for V5.0+ formats (after columns are populated)
        let schema = if matches!(
            header.cassandra_version,
            CassandraVersion::V5_0NewBig
                | CassandraVersion::V5_0Bti
                | CassandraVersion::V5_0DataFormat
                | CassandraVersion::V5_0FormatC
                | CassandraVersion::V5_0FormatD
                | CassandraVersion::V5_0FormatE
                | CassandraVersion::V5_0FormatF
                | CassandraVersion::V5_0FormatG
        ) {
            match TableSchema::from_sstable_header(&header) {
                Ok(s) => {
                    log::debug!(
                        "Extracted schema from SSTable header: {}.{} ({} columns, {} partition keys, {} clustering keys)",
                        s.keyspace,
                        s.table,
                        s.columns.len(),
                        s.partition_keys.len(),
                        s.clustering_keys.len()
                    );
                    Some(Arc::new(s))
                }
                Err(e) => {
                    log::warn!(
                        "Failed to extract schema from SSTable header for {}: {}. Schema-aware parsing will not be available.",
                        path.display(),
                        e
                    );
                    None
                }
            }
        } else {
            // Legacy formats don't have schema in header
            None
        };

        // Derive block_count from CompressionInfo.db when available — this is the
        // authoritative source for compressed SSTables (no-heuristics mandate #28).
        // Each entry in chunk_offsets corresponds to one compressed block in Data.db.
        let block_count = compression_info
            .as_ref()
            .map(|ci| ci.chunk_offsets.len() as u64)
            .unwrap_or(0);

        let stats = SSTableReaderStats {
            file_size,
            entry_count: header.stats.row_count,
            table_count: 1, // Will be updated as we discover tables
            block_count,
            index_size: 0,        // Will be updated if index is loaded
            bloom_filter_size: 0, // Will be updated if bloom filter is loaded
            compression_ratio: header.stats.compression_ratio,
            cache_hit_rate: 0.0,
        };

        // Extract generation from filename or use default
        let generation = extract_generation_from_path(path);

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
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            #[cfg(feature = "tombstones")]
            tombstone_merger: TombstoneMerger::new(),
            generation,
            actual_header_size: header_size,
            index_reader,
            summary_reader,
            statistics_reader,
            schema_registry: None, // Will be set by set_schema_registry() after construction
            schema,
            udt_registry: None, // Will be set when available for UDT-aware parsing
            compression_info: compression_info.map(Arc::new),
            current_chunk_index: AtomicUsize::new(0),
            version_gates,
        })
    }

    /// Whether this reader's block source is backed by a memory map.
    ///
    /// Test-only hook used to verify that the `use_mmap` config / env wiring
    /// actually selects the intended backend end-to-end.
    #[cfg(test)]
    pub(crate) async fn is_mmap_backed(&self) -> bool {
        self.file.lock().await.is_mmap()
    }

    /// Memory-map an SSTable file read-only.
    ///
    /// # Safety / correctness
    ///
    /// The returned [`Mmap`](memmap2::Mmap) aliases the file's bytes for its
    /// entire lifetime. SSTables are immutable once written, and CQLite treats
    /// them as read-only inputs, so this matches Cassandra's own mmap read
    /// strategy. Mutating the underlying file while a reader is open is
    /// undefined behaviour — callers must not do so.
    ///
    /// Note that only the initial mapping is fallible here. Once mapped, a later
    /// page fault — caused by truncation, deletion, or a network/overlay
    /// filesystem hiccup — raises `SIGBUS` and **cannot** be recovered as an
    /// `io::Error`. This is why mmap is opt-in and gated on immutable local
    /// files; see [`Config`]'s `storage.use_mmap` for the full constraints.
    fn map_file(path: &Path) -> Result<memmap2::Mmap> {
        let std_file = std::fs::File::open(path)?;
        // SAFETY: read-only mapping of a file assumed immutable for the
        // reader's lifetime; see the function-level note above.
        let mmap = unsafe { memmap2::MmapOptions::new().map(&std_file)? };
        Ok(mmap)
    }

    /// Load CompressionInfo.db metadata for chunked reading
    async fn load_compression_info_metadata(
        path: &Path,
        _platform: &Arc<Platform>,
    ) -> Result<Option<CompressionInfo>> {
        use tokio::fs::File;
        use tokio::io::AsyncReadExt;

        // Try to find CompressionInfo.db in same directory
        let parent_dir = path.parent().unwrap_or(Path::new("."));
        let base_name = path.file_stem().and_then(|s| s.to_str()).and_then(|s| {
            // Extract base name: "nb-1-big-Data.db" -> "nb-1-big"
            let parts: Vec<&str> = s.split('-').collect();
            if parts.len() >= 4 {
                Some(parts[0..3].join("-"))
            } else {
                None
            }
        });

        if let Some(base) = base_name {
            let compression_info_path = parent_dir.join(format!("{}-CompressionInfo.db", base));
            if compression_info_path.exists() {
                let mut file = File::open(&compression_info_path).await?;
                let mut buffer = Vec::new();
                file.read_to_end(&mut buffer).await?;

                match CompressionInfo::parse(&buffer) {
                    Ok(info) => {
                        log::debug!(
                            "Loaded CompressionInfo: algorithm={}, chunk_length={}, chunks={}",
                            info.algorithm,
                            info.chunk_length,
                            info.chunk_offsets.len()
                        );
                        return Ok(Some(info));
                    }
                    Err(e) => {
                        log::warn!("Failed to parse CompressionInfo.db: {}", e);
                    }
                }
            }
        }

        Ok(None)
    }

    /// Set the schema registry for schema-driven operations
    #[cfg(feature = "state_machine")]
    pub fn set_schema_registry(
        &mut self,
        schema_registry: Arc<tokio::sync::RwLock<crate::schema::SchemaRegistry>>,
    ) {
        self.schema_registry = Some(schema_registry);
        log::debug!(
            "Schema registry set for {}.{} - enabling schema-driven digest computation",
            self.header.keyspace,
            self.header.table_name
        );
    }

    /// Set the schema registry for schema-driven operations (non-state_machine builds)
    #[cfg(not(feature = "state_machine"))]
    pub fn set_schema_registry(&mut self, schema_registry: Arc<crate::schema::SchemaRegistry>) {
        self.schema_registry = Some(schema_registry);
        log::debug!(
            "Schema registry set for {}.{} - enabling schema-driven digest computation",
            self.header.keyspace,
            self.header.table_name
        );
    }

    /// Set the UDT registry for UDT-aware parsing in collections
    ///
    /// This enables proper parsing of UDTs inside collections (List, Set, Map)
    /// by providing the UDT field definitions needed for nested type resolution.
    pub fn set_udt_registry(&mut self, registry: crate::schema::UdtRegistry) {
        self.udt_registry = Some(registry);
        log::debug!(
            "UDT registry set for {}.{} - enabling UDT-aware collection parsing",
            self.header.keyspace,
            self.header.table_name
        );
    }

    /// Get reader statistics
    pub async fn stats(&self) -> Result<&SSTableReaderStats> {
        Ok(&self.stats)
    }

    /// Close the reader and release resources
    pub async fn close(mut self) -> Result<()> {
        debug!("Closing SSTable reader for {:?}", self.file_path);

        // Clear caches and log cache statistics
        let cache_entries = self.block_cache.len();
        let meta_entries = self.block_meta_cache.len();

        self.block_cache.clear();
        self.block_meta_cache.clear();

        debug!(
            "Cleared {} block cache entries and {} metadata entries",
            cache_entries, meta_entries
        );

        // File will be closed automatically when dropped
        Ok(())
    }

    /// Calculate header size based on format and actual header content
    pub fn calculate_header_size(&self) -> usize {
        self.actual_header_size
    }

    /// Get the Cassandra version from the SSTable header
    pub fn cassandra_version(&self) -> CassandraVersion {
        self.header.cassandra_version
    }

    /// Get the SSTable format version string
    pub fn format_version(&self) -> Result<String> {
        let filename = self
            .file_path
            .file_name()
            .and_then(|f| f.to_str())
            .ok_or_else(|| {
                Error::InvalidPath(format!("Invalid SSTable filename: {:?}", self.file_path))
            })?;

        let parts: Vec<&str> = filename.split('-').collect();
        if parts.is_empty() {
            return Err(Error::InvalidFormat(format!(
                "Cannot extract format version from filename: {}",
                filename
            )));
        }

        Ok(parts[0].to_string())
    }

    /// Get a reference to the SSTable header
    pub fn header(&self) -> &SSTableHeader {
        &self.header
    }

    /// Get the table schema extracted from the SSTable header
    ///
    /// Returns `None` for legacy formats or if schema extraction failed.
    pub fn schema(&self) -> Option<&TableSchema> {
        self.schema.as_deref()
    }

    /// Extract write time from entry metadata
    pub fn extract_write_time_from_entry(&self, _key: &RowKey, value: &Value) -> i64 {
        use log::warn;

        match value {
            Value::Tombstone(info) => info.deletion_time,
            _ => std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_micros() as i64)
                .unwrap_or_else(|e| {
                    warn!("Failed to get system time: {}; using fallback value 0", e);
                    0
                }),
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

/// Helper function to create a reader with default configuration
pub async fn open_sstable_reader(
    path: &Path,
    config: &Config,
    platform: Arc<Platform>,
) -> Result<SSTableReader> {
    SSTableReader::open(path, config, platform).await
}
