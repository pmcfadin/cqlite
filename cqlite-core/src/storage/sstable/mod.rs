//! SSTable (Sorted String Table) implementation

pub mod bloom;
pub mod compression;
pub mod index;
pub mod reader;
pub mod writer;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::platform::Platform;
use crate::{types::TableId, Config, Result, RowKey, Value};

/// SSTable file identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SSTableId(pub String);

impl SSTableId {
    /// Create a new SSTable ID with timestamp
    pub fn new() -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros();
        Self(format!("sstable_{}.sst", timestamp))
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

    /// Load existing SSTable files from disk
    async fn load_existing_sstables(&self) -> Result<()> {
        let mut dir_entries = self.platform.fs().read_dir(&self.base_path).await?;
        let mut readers = self.readers.write().await;

        while let Some(entry) = dir_entries.next_entry().await? {
            let path = entry.path();
            if let Some(extension) = path.extension() {
                if extension == "sst" {
                    if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                        let sstable_id = SSTableId::from_filename(filename);
                        let reader = Arc::new(
                            reader::SSTableReader::open(&path, &self.config, self.platform.clone())
                                .await?,
                        );
                        readers.insert(sstable_id, reader);
                    }
                }
            }
        }

        Ok(())
    }

    /// Create a new SSTable from MemTable data
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

    /// Get a value by key from all SSTables
    pub async fn get(&self, table_id: &TableId, key: &RowKey) -> Result<Option<Value>> {
        let readers = self.readers.read().await;

        // Search through SSTables in reverse chronological order
        // (newer SSTables have precedence)
        let mut sstable_ids: Vec<_> = readers.keys().collect();
        sstable_ids.sort_by(|a, b| b.0.cmp(&a.0)); // Reverse sort by filename/timestamp

        for sstable_id in sstable_ids {
            if let Some(reader) = readers.get(sstable_id) {
                if let Some(value) = reader.get(table_id, key).await? {
                    return Ok(Some(value));
                }
            }
        }

        Ok(None)
    }

    /// Scan a range of keys from all SSTables
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
            let results = reader.scan(table_id, start_key, end_key, limit).await?;
            all_results.extend(results);
        }

        // Sort and deduplicate results
        all_results.sort_by(|a, b| a.0.cmp(&b.0));
        all_results.dedup_by(|a, b| a.0 == b.0);

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
    async fn test_sstable_id_generation() {
        let id1 = SSTableId::new();
        let id2 = SSTableId::new();

        assert_ne!(id1.filename(), id2.filename());
        assert!(id1.filename().starts_with("sstable_"));
        assert!(id1.filename().ends_with(".sst"));
    }
}
