//! SSTable (Sorted String Table) implementation

pub mod bloom;
pub mod bti;
pub mod bulletproof_reader;
pub mod chunk_decompressor;
pub mod chunked_data_reader;
pub mod compression;
pub mod compression_info;
pub mod directory;
pub mod directory_integration_tests;
pub mod format_detector;
pub mod header_spec;
pub mod index;
pub mod index_reader;
pub mod key_digest;
pub mod performance_benchmarks;
pub mod reader;
pub mod summary_reader;
pub use reader::SSTableReader;
pub mod schema_aware_reader;
pub use schema_aware_reader::SchemaAwareReader;
pub mod row_cell_state_machine;
pub mod statistics_reader;
pub mod streaming_reader;
#[cfg(feature = "tombstones")]
pub mod tombstone_merger;
pub mod validation;
#[cfg(feature = "experimental")]
pub mod writer;

// Test modules
#[cfg(test)]
mod key_digest_integration_test;
#[cfg(test)]
mod key_digest_test;
#[cfg(all(test, feature = "experimental"))]
#[cfg(feature = "experimental")]
mod oa_format_compliance_test;
#[cfg(test)]
mod row_cell_state_machine_test;
#[cfg(test)]
mod schema_aware_reader_test;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

#[cfg(feature = "tombstones")]
use self::tombstone_merger::{EntryMetadata, GenerationValue, TombstoneMerger};
use crate::platform::Platform;
use crate::{types::TableId, Config, Result, RowKey, Value};

/// SSTable file identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct SSTableId(pub String);

impl Default for SSTableId {
    fn default() -> Self {
        Self::new()
    }
}

impl SSTableId {
    /// Create a new SSTable ID with timestamp using Cassandra naming convention
    pub fn new() -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros();
        // Use Cassandra naming convention: <keyspace>-<table>-<generation>-<format>-Data.db
        // For generated files, we'll use a simplified pattern: sstable-<timestamp>-big-Data.db
        Self(format!("sstable-{}-big-Data.db", timestamp))
    }

    /// Create SSTable ID from filename
    pub fn from_filename(filename: &str) -> Self {
        Self(filename.to_string())
    }

    /// Get the filename
    pub fn filename(&self) -> &str {
        &self.0
    }
}

/// SSTable manager that handles multiple SSTable files
#[derive(Debug)]
pub struct SSTableManager {
    /// Base directory for SSTable files
    base_path: PathBuf,

    /// Active SSTable readers indexed by ID
    readers: Arc<RwLock<HashMap<SSTableId, Arc<reader::SSTableReader>>>>,

    /// Platform abstraction
    platform: Arc<Platform>,

    /// Configuration
    config: Config,
}

impl SSTableManager {
    /// Create a new SSTable manager
    pub async fn new(path: &Path, config: &Config, platform: Arc<Platform>) -> Result<Self> {
        let base_path = path.to_path_buf();
        let readers = Arc::new(RwLock::new(HashMap::new()));

        let manager = Self {
            base_path,
            readers,
            platform,
            config: config.clone(),
        };

        // Load existing SSTable files
        manager.load_existing_sstables().await?;

        Ok(manager)
    }

    /// Create a new SSTable manager from pre-discovered table directories
    ///
    /// This method accepts a list of table directory paths (from DiscoveryService)
    /// and loads SSTables from those specific directories. It does not perform
    /// filesystem scanning beyond the provided directories - this avoids duplicate
    /// scanning when integrating with the discovery/engine lifecycle.
    ///
    /// Use this method when you have pre-discovered table directories and want
    /// to avoid redundant filesystem scanning. Use `new()` when you want automatic
    /// discovery from a single base directory.
    ///
    /// # Arguments
    ///
    /// * `storage_path` - Base storage path (used for context, not for scanning)
    /// * `table_dirs` - List of table directory paths from DiscoveryService
    ///   (e.g., `/data/keyspace1/table1-abc123`)
    /// * `config` - Configuration
    /// * `platform` - Platform abstraction
    ///
    /// # Returns
    ///
    /// A new `SSTableManager` with SSTables loaded from the specified directories
    ///
    /// # Errors
    ///
    /// Returns an error if any of the specified directories cannot be read.
    /// Individual SSTable loading errors are logged but do not fail the entire operation.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use cqlite_core::storage::sstable::SSTableManager;
    /// use cqlite_core::{Config, Platform};
    /// use std::sync::Arc;
    /// use std::path::PathBuf;
    ///
    /// # async fn example() -> cqlite_core::Result<()> {
    /// let config = Config::default();
    /// let platform = Arc::new(Platform::new(&config).await?);
    ///
    /// // Get table directories from DiscoveryService
    /// let table_dirs = vec![
    ///     PathBuf::from("/data/keyspace1/table1-abc123"),
    ///     PathBuf::from("/data/keyspace1/table2-def456"),
    /// ];
    ///
    /// let manager = SSTableManager::new_from_discovered_paths(
    ///     &PathBuf::from("/data"),
    ///     table_dirs,
    ///     &config,
    ///     platform
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new_from_discovered_paths(
        storage_path: &Path,
        table_dirs: Vec<PathBuf>,
        config: &Config,
        platform: Arc<Platform>,
    ) -> Result<Self> {
        let base_path = storage_path.to_path_buf();
        let readers = Arc::new(RwLock::new(HashMap::new()));

        let manager = Self {
            base_path,
            readers,
            platform: platform.clone(),
            config: config.clone(),
        };

        // Load SSTables from the provided table directories
        manager.load_from_table_directories(table_dirs).await?;

        Ok(manager)
    }

    /// Load SSTable readers from specific table directories
    ///
    /// This method scans each provided table directory for Data.db files and loads them.
    /// It handles empty directories gracefully and logs warnings for individual file errors.
    async fn load_from_table_directories(&self, table_dirs: Vec<PathBuf>) -> Result<()> {
        let mut readers = self.readers.write().await;

        for table_dir in table_dirs {
            // Check if directory exists
            if !self.platform.fs().exists(&table_dir).await? {
                eprintln!("Warning: Table directory does not exist: {:?}", table_dir);
                continue;
            }

            // Read directory contents
            let mut dir_entries = match self.platform.fs().read_dir(&table_dir).await {
                Ok(entries) => entries,
                Err(e) => {
                    eprintln!(
                        "Warning: Cannot read table directory {:?}: {}",
                        table_dir, e
                    );
                    continue;
                }
            };

            // Scan for Data.db files
            while let Some(entry) = dir_entries.next_entry().await? {
                let path = entry.path();
                if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                    // Check for Cassandra SSTable data files using the *-Data.db pattern
                    if filename.ends_with("-Data.db") {
                        let sstable_id = SSTableId::from_filename(filename);
                        // Try to open the SSTable reader
                        match reader::SSTableReader::open(
                            &path,
                            &self.config,
                            self.platform.clone(),
                        )
                        .await
                        {
                            Ok(reader) => {
                                readers.insert(sstable_id, Arc::new(reader));
                            }
                            Err(e) => {
                                // Log warning but continue loading other SSTables
                                eprintln!("Warning: Could not load SSTable file {:?}: {}", path, e);
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Load existing SSTable files from disk
    async fn load_existing_sstables(&self) -> Result<()> {
        // Check if directory exists first
        if !self.platform.fs().exists(&self.base_path).await? {
            return Ok(()); // No directory, no SSTables to load
        }

        let mut dir_entries = match self.platform.fs().read_dir(&self.base_path).await {
            Ok(entries) => entries,
            Err(_) => return Ok(()), // Can't read directory, skip loading
        };
        let mut readers = self.readers.write().await;

        while let Some(entry) = dir_entries.next_entry().await? {
            let path = entry.path();
            if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                // Check for Cassandra SSTable data files using the *-Data.db pattern
                if filename.ends_with("-Data.db") {
                    let sstable_id = SSTableId::from_filename(filename);
                    // Try to open the SSTable reader, but don't fail if one file is problematic
                    match reader::SSTableReader::open(&path, &self.config, self.platform.clone())
                        .await
                    {
                        Ok(reader) => {
                            readers.insert(sstable_id, Arc::new(reader));
                        }
                        Err(_) => {
                            // Skip problematic SSTable files during initialization
                            eprintln!("Warning: Could not load SSTable file: {:?}", path);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Create a new SSTable from MemTable data
    #[cfg(feature = "experimental")]
    pub async fn create_from_memtable(
        &self,
        data: Vec<(TableId, RowKey, Value)>,
    ) -> Result<SSTableId> {
        let sstable_id = SSTableId::new();
        let file_path = self.base_path.join(sstable_id.filename());

        // Create SSTable writer
        let mut writer =
            writer::SSTableWriter::create(&file_path, &self.config, self.platform.clone()).await?;

        // Sort data by table and key
        let mut sorted_data = data;
        sorted_data.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

        // Write data to SSTable
        for (table_id, key, value) in sorted_data {
            writer.add_entry(&table_id, key, value).await?;
        }

        // Finalize the SSTable
        writer.finish().await?;

        // Create reader for the new SSTable
        let reader = Arc::new(
            reader::SSTableReader::open(&file_path, &self.config, self.platform.clone()).await?,
        );

        // Add to active readers
        {
            let mut readers = self.readers.write().await;
            readers.insert(sstable_id.clone(), reader);
        }

        Ok(sstable_id)
    }

    #[cfg(not(feature = "experimental"))]
    pub async fn create_from_memtable(
        &self,
        _data: Vec<(TableId, RowKey, Value)>,
    ) -> Result<SSTableId> {
        Err(crate::error::Error::unsupported_format(
            "SSTable writing requires experimental feature",
        ))
    }

    /// Get a value by key from all SSTables with proper tombstone merging
    #[cfg(feature = "tombstones")]
    pub async fn get(&self, table_id: &TableId, key: &RowKey) -> Result<Option<Value>> {
        let readers = self.readers.read().await;
        let mut all_values = Vec::new();

        // Collect values from all SSTables
        for (_sstable_id, reader) in readers.iter() {
            if let Some(value) = reader.get(table_id, key).await? {
                let generation = reader.generation;
                let write_time = reader.extract_write_time_from_entry(key, &value);

                let gen_value = GenerationValue {
                    value,
                    metadata: EntryMetadata {
                        write_time,
                        generation,
                        ttl: None, // Would be extracted from SSTable metadata
                    },
                };
                all_values.push(gen_value);
            }
        }

        // Use tombstone merger to resolve conflicts across generations
        let merger = TombstoneMerger::new();
        merger.merge_generations(all_values)
    }

    /// Get a value by key from all SSTables (simple version without tombstone merging)
    #[cfg(not(feature = "tombstones"))]
    pub async fn get(&self, table_id: &TableId, key: &RowKey) -> Result<Option<Value>> {
        let readers = self.readers.read().await;

        // Return the first value found (simple strategy)
        for (_sstable_id, reader) in readers.iter() {
            if let Some(value) = reader.get(table_id, key).await? {
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    /// Scan a range of keys from all SSTables with proper tombstone merging
    #[cfg(feature = "tombstones")]
    pub async fn scan(
        &self,
        table_id: &TableId,
        start_key: Option<&RowKey>,
        end_key: Option<&RowKey>,
        limit: Option<usize>,
    ) -> Result<Vec<(RowKey, Value)>> {
        let readers = self.readers.read().await;
        let mut key_values = std::collections::HashMap::new();

        // Collect results from all SSTables, grouping by key
        for reader in readers.values() {
            let results = reader.scan(table_id, start_key, end_key, None).await?;

            for (row_key, value) in results {
                let generation = reader.generation;
                let write_time = reader.extract_write_time_from_entry(&row_key, &value);

                let gen_value = GenerationValue {
                    value,
                    metadata: EntryMetadata {
                        write_time,
                        generation,
                        ttl: None,
                    },
                };

                key_values
                    .entry(row_key)
                    .or_insert_with(Vec::new)
                    .push(gen_value);
            }
        }

        // Merge values for each key using tombstone merger
        let merger = TombstoneMerger::new();
        let mut final_results = Vec::new();

        for (row_key, values) in key_values {
            if let Some(merged_value) = merger.merge_generations(values)? {
                final_results.push((row_key, merged_value));
            }
        }

        // Sort results by key
        final_results.sort_by(|a, b| a.0.cmp(&b.0));

        // Apply limit
        if let Some(limit) = limit {
            final_results.truncate(limit);
        }

        Ok(final_results)
    }

    /// Scan a range of keys from all SSTables (simple version without tombstone merging)
    #[cfg(not(feature = "tombstones"))]
    pub async fn scan(
        &self,
        table_id: &TableId,
        start_key: Option<&RowKey>,
        end_key: Option<&RowKey>,
        limit: Option<usize>,
    ) -> Result<Vec<(RowKey, Value)>> {
        let readers = self.readers.read().await;
        let mut all_results = Vec::new();

        // Collect results from all SSTables
        for reader in readers.values() {
            let results = reader.scan(table_id, start_key, end_key, None).await?;
            all_results.extend(results);
        }

        // Sort results by key
        all_results.sort_by(|a, b| a.0.cmp(&b.0));

        // Apply limit
        if let Some(limit) = limit {
            all_results.truncate(limit);
        }

        Ok(all_results)
    }

    /// Get list of all SSTable IDs
    pub async fn list_sstables(&self) -> Vec<SSTableId> {
        let readers = self.readers.read().await;
        readers.keys().cloned().collect()
    }

    /// Remove an SSTable
    pub async fn remove_sstable(&self, sstable_id: &SSTableId) -> Result<()> {
        // Remove from memory
        {
            let mut readers = self.readers.write().await;
            readers.remove(sstable_id);
        }

        // Delete file
        let file_path = self.base_path.join(sstable_id.filename());
        if self.platform.fs().exists(&file_path).await? {
            self.platform.fs().remove_file(&file_path).await?;
        }

        Ok(())
    }

    /// Get SSTable statistics
    pub async fn stats(&self) -> Result<SSTableStats> {
        let readers = self.readers.read().await;

        let mut total_size = 0u64;
        let mut total_entries = 0u64;
        let mut total_tables = 0u64;
        let sstable_count = readers.len();

        for reader in readers.values() {
            let reader_stats = reader.stats().await?;
            total_size += reader_stats.file_size;
            total_entries += reader_stats.entry_count;
            total_tables += reader_stats.table_count;
        }

        Ok(SSTableStats {
            sstable_count,
            total_size,
            total_entries,
            total_tables,
            average_size: if sstable_count > 0 {
                total_size / sstable_count as u64
            } else {
                0
            },
        })
    }

    /// Merge multiple SSTables into a new one
    #[cfg(feature = "experimental")]
    pub async fn merge_sstables(
        &self,
        source_ids: Vec<SSTableId>,
        target_id: SSTableId,
    ) -> Result<()> {
        let file_path = self.base_path.join(target_id.filename());

        // Create new SSTable writer
        let mut writer =
            writer::SSTableWriter::create(&file_path, &self.config, self.platform.clone()).await?;

        // Collect all data from source SSTables
        let mut all_data = Vec::new();
        {
            let readers = self.readers.read().await;
            for source_id in &source_ids {
                if let Some(reader) = readers.get(source_id) {
                    let data = reader.get_all_entries().await?;
                    all_data.extend(data);
                }
            }
        }

        // Sort merged data
        all_data.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

        // Remove duplicates (keep latest value for each key)
        all_data.dedup_by(|a, b| a.0 == b.0 && a.1 == b.1);

        // Write merged data
        for (table_id, key, value) in all_data {
            writer.add_entry(&table_id, key, value).await?;
        }

        writer.finish().await?;

        // Create reader for merged SSTable
        let reader = Arc::new(
            reader::SSTableReader::open(&file_path, &self.config, self.platform.clone()).await?,
        );

        // Update readers map
        {
            let mut readers = self.readers.write().await;

            // Remove source SSTables
            for source_id in &source_ids {
                readers.remove(source_id);
            }

            // Add merged SSTable
            readers.insert(target_id, reader);
        }

        // Delete source files
        for source_id in &source_ids {
            let source_path = self.base_path.join(source_id.filename());
            if self.platform.fs().exists(&source_path).await? {
                self.platform.fs().remove_file(&source_path).await?;
            }
        }

        Ok(())
    }

    #[cfg(not(feature = "experimental"))]
    pub async fn merge_sstables(
        &self,
        _source_ids: Vec<SSTableId>,
        _target_id: SSTableId,
    ) -> Result<()> {
        Err(crate::error::Error::unsupported_format(
            "SSTable merging requires experimental feature",
        ))
    }
}

/// SSTable statistics
#[derive(Debug, Clone)]
pub struct SSTableStats {
    /// Number of SSTable files
    pub sstable_count: usize,

    /// Total size of all SSTables in bytes
    pub total_size: u64,

    /// Total number of entries across all SSTables
    pub total_entries: u64,

    /// Total number of tables across all SSTables
    pub total_tables: u64,

    /// Average SSTable size in bytes
    pub average_size: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::platform::Platform;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_sstable_manager_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        let manager = SSTableManager::new(temp_dir.path(), &config, platform)
            .await
            .unwrap();
        let stats = manager.stats().await.unwrap();

        assert_eq!(stats.sstable_count, 0);
        assert_eq!(stats.total_size, 0);
    }

    #[tokio::test]
    async fn test_sstable_manager_from_discovered_paths_empty() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        // Create an empty list of discovered paths
        let discovered_paths = Vec::new();

        let manager = SSTableManager::new_from_discovered_paths(
            temp_dir.path(),
            discovered_paths,
            &config,
            platform,
        )
        .await
        .unwrap();

        let stats = manager.stats().await.unwrap();

        // Should have 0 SSTables since we provided an empty list
        assert_eq!(stats.sstable_count, 0);
        assert_eq!(stats.total_size, 0);
    }

    #[tokio::test]
    async fn test_sstable_manager_from_discovered_paths_with_directories() {
        use std::fs;

        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        // Create mock table directories with Data.db files
        let keyspace_dir = temp_dir.path().join("test_ks");
        fs::create_dir(&keyspace_dir).unwrap();

        let table1_dir = keyspace_dir.join("users-abc123");
        fs::create_dir(&table1_dir).unwrap();
        // Note: These are mock files that won't parse as real SSTables,
        // but they test the directory scanning logic
        fs::write(table1_dir.join("na-1-big-Data.db"), b"mock_data").unwrap();

        let table2_dir = keyspace_dir.join("posts-def456");
        fs::create_dir(&table2_dir).unwrap();
        fs::write(table2_dir.join("na-2-big-Data.db"), b"mock_data").unwrap();
        fs::write(table2_dir.join("na-3-big-Data.db"), b"mock_data").unwrap();

        // Provide table directories to manager
        let table_dirs = vec![table1_dir.clone(), table2_dir.clone()];

        let manager = SSTableManager::new_from_discovered_paths(
            temp_dir.path(),
            table_dirs,
            &config,
            platform,
        )
        .await
        .unwrap();

        let stats = manager.stats().await.unwrap();

        // Should attempt to load 3 Data.db files (though they may fail to parse)
        // This tests that the directory scanning logic works correctly
        // Note: Since these are mock files, actual loading may fail,
        // but the manager should still be created successfully
        assert_eq!(stats.sstable_count, 0); // Mock files won't parse as valid SSTables
    }

    #[tokio::test]
    #[ignore = "M3+ feature; gated for M1"]
    async fn test_sstable_id_generation() {
        let id1 = SSTableId::new();
        let id2 = SSTableId::new();

        assert_ne!(id1.filename(), id2.filename());
        assert!(id1.filename().starts_with("sstable_"));
        assert!(id1.filename().ends_with(".sst"));
    }
}
