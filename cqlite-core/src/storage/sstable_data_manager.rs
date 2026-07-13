//! SSTable Data Loading and Caching System
//!
//! This module provides efficient data loading, caching, and access for the REPL system.
//! It integrates with existing SSTable parsers and provides high-performance data access
//! for interactive queries and exploration.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use parking_lot::RwLock;
use tokio::sync::{RwLock as AsyncRwLock, Semaphore};

use crate::{
    parser::header::CassandraVersion,
    platform::Platform,
    schema::{SchemaManager, TableSchema},
    storage::sstable::reader::SSTableReader,
    Config, Error, Result, RowKey, ScanRow, Value,
};

/// Configuration for the SSTable data manager
#[derive(Debug, Clone)]
pub struct SSTableDataManagerConfig {
    /// Maximum memory for caching in MB
    pub max_cache_size_mb: usize,
    /// Cache TTL for data entries
    pub cache_ttl_seconds: u64,
    /// Maximum concurrent file operations
    pub max_concurrent_ops: usize,
    /// Enable background preloading
    pub enable_preloading: bool,
    /// Preload batch size
    pub preload_batch_size: usize,
    /// Discovery scan interval in seconds
    pub discovery_interval_seconds: u64,
    /// Enable integrity checks
    pub enable_integrity_checks: bool,
}

impl Default for SSTableDataManagerConfig {
    fn default() -> Self {
        Self {
            max_cache_size_mb: 512,
            cache_ttl_seconds: 300, // 5 minutes
            max_concurrent_ops: 10,
            enable_preloading: true,
            preload_batch_size: 1000,
            discovery_interval_seconds: 30,
            enable_integrity_checks: true,
        }
    }
}

/// Cached data entry with metadata
#[derive(Debug, Clone)]
pub struct CachedDataEntry {
    /// The actual data rows
    pub rows: Vec<DataRow>,
    /// When this entry was cached
    pub cached_at: Instant,
    /// Size in bytes (approximate)
    pub size_bytes: usize,
    /// Access count for LRU eviction
    pub access_count: u64,
    /// Last access time
    pub last_accessed: Instant,
}

/// Unified data row representation
#[derive(Debug, Clone)]
pub struct DataRow {
    /// Row key
    pub key: RowKey,
    /// Column data
    pub columns: HashMap<String, Value>,
    /// Row metadata
    pub metadata: RowMetadata,
}

/// Row metadata for tracking and validation
#[derive(Debug, Clone)]
pub struct RowMetadata {
    /// Source SSTable file
    pub source_file: PathBuf,
    /// Write timestamp
    pub write_time: Option<i64>,
    /// TTL information
    pub ttl: Option<Duration>,
    /// Generation number
    pub generation: u64,
}

/// Table discovery results
#[derive(Debug, Clone)]
pub struct TableDiscovery {
    /// Discovered keyspaces
    pub keyspaces: Vec<KeyspaceInfo>,
    /// Total SSTables found
    pub total_sstables: usize,
    /// Discovery completion time
    pub discovery_time: Duration,
}

/// Keyspace information
#[derive(Debug, Clone)]
pub struct KeyspaceInfo {
    /// Keyspace name
    pub name: String,
    /// Tables in this keyspace
    pub tables: Vec<TableInfo>,
    /// SSTable directory path
    pub path: PathBuf,
}

/// Table information with metadata
#[derive(Debug, Clone)]
pub struct TableInfo {
    /// Table name
    pub name: String,
    /// Schema information
    pub schema: Option<TableSchema>,
    /// Associated SSTable files
    pub sstable_files: Vec<SSTableFileInfo>,
    /// Total estimated rows
    pub estimated_rows: usize,
    /// Total size in bytes
    pub total_size_bytes: u64,
    /// Last modified time
    pub last_modified: Option<std::time::SystemTime>,
}

/// SSTable file information
#[derive(Debug, Clone)]
pub struct SSTableFileInfo {
    /// File path
    pub path: PathBuf,
    /// File size
    pub size_bytes: u64,
    /// Cassandra version detected
    pub version: Option<CassandraVersion>,
    /// Compression info
    pub compression: Option<String>,
    /// Estimated row count
    pub estimated_rows: usize,
    /// Health status
    pub health_status: FileHealthStatus,
}

/// File health status
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileHealthStatus {
    /// File is healthy and readable
    Healthy,
    /// File has minor issues but is usable
    Degraded,
    /// File is corrupted or unreadable
    Corrupted,
    /// File access permission issues
    AccessDenied,
}

/// Memory-efficient SSTable data manager
#[allow(dead_code)]
pub struct SSTableDataManager {
    /// Configuration
    config: SSTableDataManagerConfig,
    /// Platform abstraction
    platform: Arc<Platform>,
    /// Core configuration
    core_config: Config,
    /// Schema manager for metadata
    schema_manager: Arc<SchemaManager>,
    /// Data cache with LRU eviction
    data_cache: Arc<DashMap<String, CachedDataEntry>>,
    /// Discovered tables cache
    discovered_tables: Arc<AsyncRwLock<HashMap<String, TableInfo>>>,
    /// SSTable readers pool
    readers_pool: Arc<DashMap<PathBuf, Arc<SSTableReader>>>,
    /// Concurrency control
    operation_semaphore: Arc<Semaphore>,
    /// Background discovery state
    discovery_state: Arc<RwLock<DiscoveryState>>,
    /// Cache statistics
    cache_stats: Arc<RwLock<CacheStatistics>>,
}

/// Discovery state tracking
#[derive(Debug, Clone)]
struct DiscoveryState {
    /// Last discovery run
    last_discovery: Option<Instant>,
    /// Discovery in progress flag
    discovery_in_progress: bool,
    /// Discovery results
    last_results: Option<TableDiscovery>,
}

/// Cache performance statistics
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CacheStatistics {
    /// Total cache hits
    pub cache_hits: u64,
    /// Total cache misses
    pub cache_misses: u64,
    /// Current cache size in bytes
    pub current_cache_size_bytes: usize,
    /// Number of cache entries
    pub cache_entries: usize,
    /// Number of evictions
    pub evictions: u64,
    /// Average access time in microseconds
    pub avg_access_time_micros: u64,
    /// Background operations count
    pub background_operations: u64,
}

impl SSTableDataManager {
    /// Create a new SSTable data manager
    pub async fn new(
        config: SSTableDataManagerConfig,
        platform: Arc<Platform>,
        core_config: Config,
        schema_manager: Arc<SchemaManager>,
    ) -> Result<Self> {
        let operation_semaphore = Arc::new(Semaphore::new(config.max_concurrent_ops));

        let manager = Self {
            config,
            platform,
            core_config,
            schema_manager,
            data_cache: Arc::new(DashMap::new()),
            discovered_tables: Arc::new(AsyncRwLock::new(HashMap::new())),
            readers_pool: Arc::new(DashMap::new()),
            operation_semaphore,
            discovery_state: Arc::new(RwLock::new(DiscoveryState {
                last_discovery: None,
                discovery_in_progress: false,
                last_results: None,
            })),
            cache_stats: Arc::new(RwLock::new(CacheStatistics {
                cache_hits: 0,
                cache_misses: 0,
                current_cache_size_bytes: 0,
                cache_entries: 0,
                evictions: 0,
                avg_access_time_micros: 0,
                background_operations: 0,
            })),
        };

        Ok(manager)
    }

    /// Discover all available keyspaces and tables
    pub async fn discover_tables(&self, data_dir: &Path) -> Result<TableDiscovery> {
        let _start_time = Instant::now();

        // Check if discovery is already in progress
        {
            let mut state = self.discovery_state.write();
            if state.discovery_in_progress {
                // Return cached results if available
                if let Some(ref results) = state.last_results {
                    return Ok(results.clone());
                }
            }
            state.discovery_in_progress = true;
        }

        let discovery_result = self.perform_discovery(data_dir).await;

        // Update discovery state
        {
            let mut state = self.discovery_state.write();
            state.discovery_in_progress = false;
            state.last_discovery = Some(Instant::now());
            if let Ok(ref results) = discovery_result {
                state.last_results = Some(results.clone());
            }
        }

        discovery_result
    }

    /// Perform the actual table discovery
    async fn perform_discovery(&self, data_dir: &Path) -> Result<TableDiscovery> {
        let start_time = Instant::now();
        let mut keyspaces = Vec::new();
        let mut total_sstables = 0;

        // Scan for keyspace directories
        let mut keyspace_entries = self.platform.fs().read_dir(data_dir).await.map_err(|e| {
            Error::Io(std::io::Error::other(format!(
                "Failed to read data directory: {}",
                e
            )))
        })?;

        while let Some(entry) = keyspace_entries.next_entry().await.map_err(|e| {
            Error::Io(std::io::Error::other(format!(
                "Error reading directory entry: {}",
                e
            )))
        })? {
            let path = entry.path();
            if path.is_dir() {
                if let Some(keyspace_name) = path.file_name().and_then(|n| n.to_str()) {
                    // Skip system directories
                    if keyspace_name.starts_with('.') || keyspace_name == "system" {
                        continue;
                    }

                    if let Ok(keyspace_info) =
                        self.discover_keyspace_tables(&path, keyspace_name).await
                    {
                        total_sstables += keyspace_info
                            .tables
                            .iter()
                            .map(|t| t.sstable_files.len())
                            .sum::<usize>();
                        keyspaces.push(keyspace_info);
                    }
                }
            }
        }

        // Update discovered tables cache
        {
            let mut discovered = self.discovered_tables.write().await;
            discovered.clear();

            for keyspace in &keyspaces {
                for table in &keyspace.tables {
                    let full_name = format!("{}.{}", keyspace.name, table.name);
                    discovered.insert(full_name, table.clone());
                }
            }
        }

        Ok(TableDiscovery {
            keyspaces,
            total_sstables,
            discovery_time: start_time.elapsed(),
        })
    }

    /// Discover tables within a keyspace
    async fn discover_keyspace_tables(
        &self,
        keyspace_path: &Path,
        keyspace_name: &str,
    ) -> Result<KeyspaceInfo> {
        let mut tables = Vec::new();

        let mut table_entries = self
            .platform
            .fs()
            .read_dir(keyspace_path)
            .await
            .map_err(|e| {
                Error::Io(std::io::Error::other(format!(
                    "Failed to read keyspace directory: {}",
                    e
                )))
            })?;

        while let Some(entry) = table_entries.next_entry().await.map_err(|e| {
            Error::Io(std::io::Error::other(format!(
                "Error reading table entry: {}",
                e
            )))
        })? {
            let path = entry.path();
            if path.is_dir() {
                if let Some(table_name) = path.file_name().and_then(|n| n.to_str()) {
                    // Skip UUID-based table directories unless they contain valid SSTables
                    if let Ok(table_info) = self.discover_table_sstables(&path, table_name).await {
                        if !table_info.sstable_files.is_empty() {
                            tables.push(table_info);
                        }
                    }
                }
            }
        }

        Ok(KeyspaceInfo {
            name: keyspace_name.to_string(),
            tables,
            path: keyspace_path.to_path_buf(),
        })
    }

    /// Discover SSTable files for a specific table
    async fn discover_table_sstables(
        &self,
        table_path: &Path,
        table_name: &str,
    ) -> Result<TableInfo> {
        let mut sstable_files = Vec::new();
        let mut total_size_bytes = 0u64;
        let mut last_modified = None;

        let mut file_entries = self.platform.fs().read_dir(table_path).await.map_err(|e| {
            Error::Io(std::io::Error::other(format!(
                "Failed to read table directory: {}",
                e
            )))
        })?;

        while let Some(entry) = file_entries.next_entry().await.map_err(|e| {
            Error::Io(std::io::Error::other(format!(
                "Error reading file entry: {}",
                e
            )))
        })? {
            let path = entry.path();
            if let Some(extension) = path.extension() {
                if extension == "db" {
                    // Cassandra SSTable data files
                    let metadata = entry.metadata().await.map_err(|e| {
                        Error::Io(std::io::Error::other(format!(
                            "Failed to get file metadata: {}",
                            e
                        )))
                    })?;

                    let size_bytes = metadata.len();
                    total_size_bytes += size_bytes;

                    if last_modified.is_none() || metadata.modified().ok() > last_modified {
                        last_modified = metadata.modified().ok();
                    }

                    let file_info = self.analyze_sstable_file(&path, size_bytes).await;
                    sstable_files.push(file_info);
                }
            }
        }

        // Try to load schema information
        let schema = self.load_table_schema(table_name).await.ok();

        // Estimate total rows
        let estimated_rows = sstable_files.iter().map(|f| f.estimated_rows).sum();

        Ok(TableInfo {
            name: table_name.to_string(),
            schema,
            sstable_files,
            estimated_rows,
            total_size_bytes,
            last_modified,
        })
    }

    /// Analyze an individual SSTable file
    async fn analyze_sstable_file(&self, file_path: &Path, size_bytes: u64) -> SSTableFileInfo {
        let mut file_info = SSTableFileInfo {
            path: file_path.to_path_buf(),
            size_bytes,
            version: None,
            compression: None,
            estimated_rows: 0,
            health_status: FileHealthStatus::Healthy,
        };

        // Try to read header information
        if let Ok(reader) = self.get_or_create_reader(file_path).await {
            // Extract version and compression info
            let header = reader.header();
            file_info.version = Some(header.cassandra_version);
            file_info.compression = Some(header.compression.algorithm.clone());

            // Estimate row count based on file size and typical row size
            file_info.estimated_rows = self.estimate_row_count(size_bytes, &reader).await;

            // Perform basic health check if enabled
            if self.config.enable_integrity_checks {
                file_info.health_status = self.check_file_health(&reader).await;
            }
        } else {
            file_info.health_status = FileHealthStatus::Corrupted;
        }

        file_info
    }

    /// Get or create a reader for the specified file
    async fn get_or_create_reader(&self, file_path: &Path) -> Result<Arc<SSTableReader>> {
        if let Some(reader) = self.readers_pool.get(file_path) {
            return Ok(reader.clone());
        }

        let _permit = self
            .operation_semaphore
            .acquire()
            .await
            .map_err(|_| Error::Io(std::io::Error::other("Semaphore acquisition failed")))?;

        // Double-check after acquiring permit
        if let Some(reader) = self.readers_pool.get(file_path) {
            return Ok(reader.clone());
        }

        let reader = Arc::new(
            SSTableReader::open(file_path, &self.core_config, self.platform.clone()).await?,
        );

        self.readers_pool
            .insert(file_path.to_path_buf(), reader.clone());
        Ok(reader)
    }

    /// Load data for a specific table with caching
    pub async fn load_table_data(
        &self,
        keyspace: &str,
        table: &str,
        limit: Option<usize>,
    ) -> Result<Vec<DataRow>> {
        let start_time = Instant::now();
        let cache_key = format!("{}:{}", keyspace, table);

        // Check cache first
        if let Some(cached) = self.data_cache.get(&cache_key) {
            if !self.is_cache_expired(&cached) {
                self.update_cache_stats(true, start_time.elapsed());
                return Ok(cached.rows.clone());
            }
        }

        // Load from disk
        let rows = self
            .load_table_data_from_disk(keyspace, table, limit)
            .await?;

        // Cache the results
        let cache_entry = CachedDataEntry {
            size_bytes: self.estimate_rows_size(&rows),
            rows: rows.clone(),
            cached_at: Instant::now(),
            access_count: 1,
            last_accessed: Instant::now(),
        };

        self.data_cache.insert(cache_key, cache_entry);
        self.update_cache_stats(false, start_time.elapsed());
        self.maybe_evict_cache().await;

        Ok(rows)
    }

    /// Load table data directly from disk
    async fn load_table_data_from_disk(
        &self,
        keyspace: &str,
        table: &str,
        limit: Option<usize>,
    ) -> Result<Vec<DataRow>> {
        let full_table_name = format!("{}.{}", keyspace, table);

        // Get table info
        let table_info = {
            let discovered = self.discovered_tables.read().await;
            discovered.get(&full_table_name).cloned()
        };

        let table_info = table_info
            .ok_or_else(|| Error::Table(format!("Table {}.{} not found", keyspace, table)))?;

        let mut all_rows = Vec::new();
        let mut loaded_count = 0;

        // Load data from all SSTable files
        for file_info in &table_info.sstable_files {
            if file_info.health_status != FileHealthStatus::Healthy {
                continue; // Skip corrupted files
            }

            let reader = self.get_or_create_reader(&file_info.path).await?;
            let file_rows = self
                .load_rows_from_reader(&reader, &table_info, limit)
                .await?;

            for row in file_rows {
                all_rows.push(row);
                loaded_count += 1;

                if let Some(limit) = limit {
                    if loaded_count >= limit {
                        break;
                    }
                }
            }

            if let Some(limit) = limit {
                if loaded_count >= limit {
                    break;
                }
            }
        }

        Ok(all_rows)
    }

    /// Load rows from a specific SSTable reader
    ///
    /// Converts SSTableReader entries (TableId, RowKey, ScanRow) to DataRow format.
    ///
    /// # Value Type Handling
    /// - `Value::Map`: Normal case - extracts all column name/value pairs (Issue #191 fix)
    /// - `Value::Null`: Tombstoned rows - skipped, not included in results
    /// - Other types: Unexpected - logs warning and uses fallback single-column format
    ///
    /// # Note
    /// The `limit` parameter applies to the number of rows returned AFTER filtering
    /// tombstones, so the actual number of entries scanned may be higher.
    async fn load_rows_from_reader(
        &self,
        reader: &SSTableReader,
        _table_info: &TableInfo,
        limit: Option<usize>,
    ) -> Result<Vec<DataRow>> {
        let mut rows = Vec::new();

        // TODO(Issue #190): SSTableReader::get_all_entries() replaces streaming API
        // Future enhancement: Add true streaming support to SSTableReader if needed
        let all_entries = reader.get_all_entries().await?;
        let entries_to_process = if let Some(lim) = limit {
            all_entries.into_iter().take(lim).collect::<Vec<_>>()
        } else {
            all_entries
        };

        for (_table_id, row_key, value) in entries_to_process {
            // Convert SSTableReader entry format to DataRow
            // FIXED (Issue #191): SSTableReader returns Value::Map with all columns
            // Extract each (column_name, column_value) pair from the map
            // Performance optimization: consume value instead of cloning (no ref)
            let columns: HashMap<String, Value> = match value {
                // Issue #1334: a live row's interned cells (`ScanRow::Row`) keyed
                // by the `Arc<str>` column-name handle; materialise `String` keys
                // for the `DataRow` column map.
                ScanRow::Row(cells) => cells
                    .into_iter()
                    .map(|(name, val)| (name.to_string(), val))
                    .collect(),
                // Issue #1334: a raw undecoded fallback row (no schema here)
                // surfaces its bytes as a single "value" blob column — the exact
                // pre-#1334 shape a bare `Value::Blob` produced via this manager's
                // fallback arm.
                ScanRow::RawRow(bytes) => {
                    HashMap::from([("value".to_string(), Value::Blob(bytes))])
                }
                // Markers (row tombstone / null row) carry no columns.
                ScanRow::Marker(marker) => {
                    tracing::debug!(
                        "Skipping non-row scan marker for key {:?}: {:?}",
                        row_key,
                        marker
                    );
                    continue; // Skip this row entirely
                }
            };

            let metadata = RowMetadata {
                source_file: reader.file_path(),
                write_time: None,
                ttl: None,
                generation: reader.generation,
            };

            rows.push(DataRow {
                key: row_key,
                columns,
                metadata,
            });
        }

        Ok(rows)
    }

    /// Convert SSTable entry to DataRow
    /// TODO(Issue #190): Legacy method from BulletproofReader API - may be removed
    #[allow(dead_code)]
    async fn convert_entry_to_row(
        &self,
        entry: crate::storage::sstable::bulletproof_reader::SSTableEntry,
        table_info: &TableInfo,
        source_file: &Path,
    ) -> Result<DataRow> {
        let mut columns = HashMap::new();

        // Parse entry data based on schema
        if let Some(ref schema) = table_info.schema {
            for (i, column) in schema.columns.iter().enumerate() {
                if i < entry.values.len() {
                    let parsed_value =
                        self.parse_column_value(&entry.values[i], &column.data_type)?;
                    columns.insert(column.name.clone(), parsed_value);
                }
            }
        } else {
            // Fallback: create generic columns
            for (i, value) in entry.values.iter().enumerate() {
                columns.insert(format!("column_{}", i), value.clone());
            }
        }

        let metadata = RowMetadata {
            source_file: source_file.to_path_buf(),
            write_time: entry.timestamp,
            ttl: None, // Would be extracted from entry metadata
            generation: entry.generation.unwrap_or(0),
        };

        Ok(DataRow {
            key: entry.key,
            columns,
            metadata,
        })
    }

    /// Parse column value based on data type
    /// TODO(Issue #190): Legacy method from BulletproofReader API - may be removed
    #[allow(dead_code)]
    fn parse_column_value(&self, value: &Value, _data_type: &str) -> Result<Value> {
        // For now, return the value as-is
        // In a real implementation, this would handle type conversions
        Ok(value.clone())
    }

    /// Query data with CQL-like filtering
    pub async fn query_data(
        &self,
        keyspace: &str,
        table: &str,
        where_clause: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<DataRow>> {
        let rows = self.load_table_data(keyspace, table, None).await?;

        // Apply filtering if where clause is provided
        let filtered_rows = if let Some(_where_clause) = where_clause {
            // TODO: Implement proper CQL WHERE clause parsing and filtering
            rows
        } else {
            rows
        };

        // Apply limit
        let final_rows = if let Some(limit) = limit {
            filtered_rows.into_iter().take(limit).collect()
        } else {
            filtered_rows
        };

        Ok(final_rows)
    }

    /// Get table schema information
    pub async fn get_table_schema(
        &self,
        keyspace: &str,
        table: &str,
    ) -> Result<Option<TableSchema>> {
        let full_table_name = format!("{}.{}", keyspace, table);
        let discovered = self.discovered_tables.read().await;

        if let Some(table_info) = discovered.get(&full_table_name) {
            Ok(table_info.schema.clone())
        } else {
            Ok(None)
        }
    }

    /// List all discovered keyspaces
    pub async fn list_keyspaces(&self) -> Result<Vec<String>> {
        let discovered = self.discovered_tables.read().await;
        let mut keyspaces: Vec<String> = discovered
            .keys()
            .map(|full_name| full_name.split('.').next().unwrap_or("").to_string())
            .collect();

        keyspaces.sort();
        keyspaces.dedup();
        Ok(keyspaces)
    }

    /// List tables in a keyspace
    pub async fn list_tables(&self, keyspace: &str) -> Result<Vec<String>> {
        let discovered = self.discovered_tables.read().await;
        let tables: Vec<String> = discovered
            .keys()
            .filter_map(|full_name| {
                let parts: Vec<&str> = full_name.split('.').collect();
                if parts.len() == 2 && parts[0] == keyspace {
                    Some(parts[1].to_string())
                } else {
                    None
                }
            })
            .collect();

        Ok(tables)
    }

    /// Get cache statistics
    pub fn get_cache_stats(&self) -> CacheStatistics {
        self.cache_stats.read().clone()
    }

    /// Get discovery status
    pub fn get_discovery_status(&self) -> (bool, Option<Duration>) {
        let state = self.discovery_state.read();
        let time_since_last = state.last_discovery.map(|last| last.elapsed());
        (state.discovery_in_progress, time_since_last)
    }

    // Helper methods

    async fn load_table_schema(&self, table_name: &str) -> Result<TableSchema> {
        // Try to load schema from schema manager
        self.schema_manager.get_table_schema(table_name).await
    }

    async fn estimate_row_count(&self, file_size_bytes: u64, _reader: &SSTableReader) -> usize {
        // Estimate based on file size and average row size
        // This is a rough estimate - in practice you'd sample some rows
        let estimated_avg_row_size = 256; // bytes
        (file_size_bytes / estimated_avg_row_size) as usize
    }

    async fn check_file_health(&self, reader: &SSTableReader) -> FileHealthStatus {
        // TODO(Issue #190): SSTableReader integrity checking API differs
        // For now, perform basic file accessibility check
        // Future: use reader.check_integrity() when available
        if reader.file_path().exists() {
            FileHealthStatus::Healthy
        } else {
            FileHealthStatus::AccessDenied
        }
    }

    fn is_cache_expired(&self, entry: &CachedDataEntry) -> bool {
        let ttl = Duration::from_secs(self.config.cache_ttl_seconds);
        entry.cached_at.elapsed() > ttl
    }

    fn estimate_rows_size(&self, rows: &[DataRow]) -> usize {
        // Rough estimation of memory usage
        rows.len() * 256 // Average row size estimate
    }

    fn update_cache_stats(&self, hit: bool, access_time: Duration) {
        let mut stats = self.cache_stats.write();
        if hit {
            stats.cache_hits += 1;
        } else {
            stats.cache_misses += 1;
        }

        // Update average access time (simple moving average)
        let new_time_micros = access_time.as_micros() as u64;
        stats.avg_access_time_micros = (stats.avg_access_time_micros + new_time_micros) / 2;

        stats.cache_entries = self.data_cache.len();
        stats.current_cache_size_bytes = self.data_cache.iter().map(|entry| entry.size_bytes).sum();
    }

    async fn maybe_evict_cache(&self) {
        let max_size_bytes = self.config.max_cache_size_mb * 1024 * 1024;
        let current_size: usize = self.data_cache.iter().map(|entry| entry.size_bytes).sum();

        if current_size > max_size_bytes {
            self.evict_lru_entries(current_size - max_size_bytes).await;
        }
    }

    async fn evict_lru_entries(&self, bytes_to_evict: usize) {
        let mut entries_to_remove = Vec::new();
        let mut bytes_evicted = 0;

        // Collect entries sorted by last access time
        let mut sorted_entries: Vec<_> = self
            .data_cache
            .iter()
            .map(|entry| (entry.key().clone(), entry.last_accessed, entry.size_bytes))
            .collect();

        sorted_entries.sort_by_key(|(_, last_accessed, _)| *last_accessed);

        for (key, _, size) in sorted_entries {
            entries_to_remove.push(key);
            bytes_evicted += size;

            if bytes_evicted >= bytes_to_evict {
                break;
            }
        }

        // Remove the entries
        for key in entries_to_remove {
            self.data_cache.remove(&key);
            let mut stats = self.cache_stats.write();
            stats.evictions += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_data_manager_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = SSTableDataManagerConfig::default();
        let core_config = Config::default();
        let platform = Arc::new(Platform::new(&core_config).await.unwrap());
        let schema_manager = Arc::new(SchemaManager::new(temp_dir.path()).await.unwrap());

        let manager = SSTableDataManager::new(config, platform, core_config, schema_manager)
            .await
            .unwrap();

        let stats = manager.get_cache_stats();
        assert_eq!(stats.cache_entries, 0);
        assert_eq!(stats.cache_hits, 0);
    }

    #[tokio::test]
    async fn test_cache_statistics() {
        let temp_dir = TempDir::new().unwrap();
        let config = SSTableDataManagerConfig::default();
        let core_config = Config::default();
        let platform = Arc::new(Platform::new(&core_config).await.unwrap());
        let schema_manager = Arc::new(SchemaManager::new(temp_dir.path()).await.unwrap());

        let manager = SSTableDataManager::new(config, platform, core_config, schema_manager)
            .await
            .unwrap();

        // Test initial state
        let stats = manager.get_cache_stats();
        assert_eq!(stats.cache_hits, 0);
        assert_eq!(stats.cache_misses, 0);

        // Test discovery status
        let (in_progress, last_discovery) = manager.get_discovery_status();
        assert!(!in_progress);
        assert!(last_discovery.is_none());
    }
}
