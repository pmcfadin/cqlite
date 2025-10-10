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
mod parsing;
mod partition_lookup;
#[cfg(test)]
mod tests;
mod types;

// Re-export public types
pub use types::{
    BlockMeta, CachedBlock, IntegrityCheckResult, IntegrityStatus, SSTableReader,
    SSTableReaderConfig, SSTableReaderHealthMetrics, SSTableReaderStats,
};

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
use tokio::io::{AsyncReadExt, AsyncSeekExt, BufReader};
use tokio::sync::Mutex;

use crate::{
    parser::{header::CassandraVersion, SSTableHeader, SSTableParser},
    platform::Platform,
    storage::sstable::compression_info::CompressionInfo,
    Config, Error, Result, RowKey, Value,
};

// Structured logging
use log::debug;

#[cfg(feature = "tombstones")]
use super::tombstone_merger::TombstoneMerger;

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
        // Parse the header using enhanced version detection - strict error propagation
        let header = parse_header_with_version_detection(&header_buffer, path)
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
            compression_info: compression_info.map(Arc::new),
            current_chunk_index: AtomicUsize::new(0),
        })
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
    pub fn set_schema_registry(&mut self, schema_registry: Arc<crate::schema::SchemaRegistry>) {
        self.schema_registry = Some(schema_registry);
        log::debug!(
            "Schema registry set for {}.{} - enabling schema-driven digest computation",
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
