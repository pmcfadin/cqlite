//! Storage engine implementation for CQLite

pub mod batch_writer;
pub mod compaction;
pub mod manifest;
pub mod memtable;
pub mod sstable;
pub mod wal;

use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::platform::Platform;
use crate::storage::batch_writer::{BatchWriter, BatchWriterBuilder};
use crate::{types::TableId, Config, Result, RowKey, Value};

/// Main storage engine that coordinates all storage components
#[derive(Debug)]
pub struct StorageEngine {
    /// In-memory write buffer
    memtable: Arc<RwLock<memtable::MemTable>>,

    /// SSTable manager for persistent storage
    sstables: Arc<sstable::SSTableManager>,

    /// Write-ahead log for durability
    wal: Arc<wal::WriteAheadLog>,

    /// Compaction manager for background maintenance
    compaction: Arc<compaction::CompactionManager>,

    /// Manifest for metadata management
    manifest: Arc<manifest::Manifest>,

    /// Platform abstraction
    platform: Arc<Platform>,

    /// Storage configuration
    config: Config,

    /// Batch writer for efficient bulk operations
    batch_writer: Option<BatchWriter>,
}

impl StorageEngine {
    /// Open a storage engine at the given path
    pub async fn open(path: &Path, config: &Config, platform: Arc<Platform>) -> Result<Self> {
        // Create storage directory if it doesn't exist
        platform.fs().create_dir_all(path).await?;

        // Initialize manifest first
        let manifest = Arc::new(manifest::Manifest::open(path, config).await?);

        // Initialize SSTable manager
        let sstables =
            Arc::new(sstable::SSTableManager::new(path, config, platform.clone()).await?);

        // Initialize WAL
        let wal = Arc::new(wal::WriteAheadLog::open(path, config, platform.clone()).await?);

        // Initialize MemTable
        let memtable = Arc::new(RwLock::new(memtable::MemTable::new(config)?));

        // Initialize compaction manager
        let compaction = Arc::new(
            compaction::CompactionManager::new(sstables.clone(), manifest.clone(), config).await?,
        );

        // Initialize batch writer for efficient bulk operations
        let batch_writer = if config.storage.memtable_size_threshold > 1024 * 1024 {
            // Only for larger configurations
            Some(
                BatchWriterBuilder::new(config.clone())
                    .with_auto_flush_size(1000)
                    .with_auto_flush_interval(std::time::Duration::from_millis(100))
                    .build(sstables.clone(), wal.clone()),
            )
        } else {
            None
        };

        Ok(Self {
            memtable,
            sstables,
            wal,
            compaction,
            manifest,
            platform,
            config: config.clone(),
            batch_writer,
        })
    }

    /// Insert a key-value pair
    pub async fn put(&self, table_id: &TableId, key: RowKey, value: Value) -> Result<()> {
        // Write to WAL first for durability
        if self.config.storage.wal.enabled {
            self.wal.append(table_id, &key, &value).await?;
        }

        // Write to MemTable
        {
            let mut memtable = self.memtable.write().await;
            memtable.put(table_id, key, value)?;

            // Check if MemTable needs to be flushed
            if memtable.size() >= self.config.storage.memtable_size_threshold {
                // Trigger async flush
                self.flush_memtable().await?;
            }
        }

        Ok(())
    }

    /// Get a value by key
    pub async fn get(&self, table_id: &TableId, key: &RowKey) -> Result<Option<Value>> {
        // Check MemTable first
        {
            let memtable = self.memtable.read().await;
            if let Some(value) = memtable.get(table_id, key)? {
                return Ok(Some(value));
            }
        }

        // Check SSTables
        self.sstables.get(table_id, key).await
    }

    /// Delete a key
    pub async fn delete(&self, table_id: &TableId, key: RowKey) -> Result<()> {
        // Write tombstone to WAL
        if self.config.storage.wal.enabled {
            self.wal.append_tombstone(table_id, &key).await?;
        }

        // Write tombstone to MemTable
        {
            let mut memtable = self.memtable.write().await;
            memtable.delete(table_id, key)?;
        }

        Ok(())
    }

    /// Scan a range of keys
    pub async fn scan(
        &self,
        table_id: &TableId,
        start_key: Option<&RowKey>,
        end_key: Option<&RowKey>,
        limit: Option<usize>,
    ) -> Result<Vec<(RowKey, Value)>> {
        let mut results = Vec::new();

        // Scan MemTable
        {
            let memtable = self.memtable.read().await;
            let memtable_results = memtable.scan(table_id, start_key, end_key, limit)?;
            results.extend(memtable_results);
        }

        // Scan SSTables and merge with MemTable results
        let sstable_results = self
            .sstables
            .scan(table_id, start_key, end_key, limit)
            .await?;

        // Merge results, with MemTable taking precedence
        let merged = self.merge_scan_results(results, sstable_results, limit);

        Ok(merged)
    }

    /// Flush MemTable to SSTable
    async fn flush_memtable(&self) -> Result<()> {
        let memtable_data = {
            let mut memtable = self.memtable.write().await;
            let data = memtable.flush()?;
            *memtable = memtable::MemTable::new(&self.config)?;
            data
        };

        if !memtable_data.is_empty() {
            // Create new SSTable from MemTable data
            self.sstables.create_from_memtable(memtable_data).await?;

            // Update manifest
            self.manifest.add_sstable_created().await?;

            // Trigger compaction if needed
            if self.config.storage.compaction.auto_compaction {
                self.compaction.maybe_trigger_compaction().await?;
            }
        }

        Ok(())
    }

    /// Force flush all pending writes
    pub async fn flush(&self) -> Result<()> {
        // Flush MemTable
        self.flush_memtable().await?;

        // Flush WAL
        if self.config.storage.wal.enabled {
            self.wal.flush().await?;
        }

        Ok(())
    }

    /// Perform manual compaction
    pub async fn compact(&self) -> Result<()> {
        self.compaction.run_compaction().await
    }

    /// Get storage statistics
    pub async fn stats(&self) -> Result<StorageStats> {
        let memtable_stats = {
            let memtable = self.memtable.read().await;
            memtable.stats()
        };

        let sstable_stats = self.sstables.stats().await?;
        let wal_stats = if self.config.storage.wal.enabled {
            Some(self.wal.stats().await?)
        } else {
            None
        };
        let compaction_stats = self.compaction.stats().await?;

        let batch_writer_stats = self.batch_writer.as_ref().map(|bw| bw.stats().clone());

        Ok(StorageStats {
            memtable: memtable_stats,
            sstables: sstable_stats,
            wal: wal_stats,
            compaction: compaction_stats,
            batch_writer: batch_writer_stats,
        })
    }

    /// Batch write operations for better performance
    pub async fn batch_write(&mut self, operations: Vec<BatchOperation>) -> Result<()> {
        if let Some(ref mut batch_writer) = self.batch_writer {
            for op in operations {
                match op {
                    BatchOperation::Put {
                        table_id,
                        key,
                        value,
                    } => {
                        batch_writer.put(table_id, key, value)?;
                    }
                    BatchOperation::Delete { table_id, key } => {
                        batch_writer.delete(table_id, key)?;
                    }
                    BatchOperation::Merge {
                        table_id,
                        key,
                        value,
                    } => {
                        batch_writer.merge(table_id, key, value)?;
                    }
                }
            }

            // Flush if batch is large enough
            batch_writer.maybe_flush().await?;
        } else {
            // Fall back to individual operations
            for op in operations {
                match op {
                    BatchOperation::Put {
                        table_id,
                        key,
                        value,
                    } => {
                        self.put(&table_id, key, value).await?;
                    }
                    BatchOperation::Delete { table_id, key } => {
                        self.delete(&table_id, key).await?;
                    }
                    BatchOperation::Merge {
                        table_id,
                        key,
                        value,
                    } => {
                        // For now, treat merge as put
                        self.put(&table_id, key, value).await?;
                    }
                }
            }
        }

        Ok(())
    }

    /// Explicit batch flush
    pub async fn flush_batch(&mut self) -> Result<()> {
        if let Some(ref mut batch_writer) = self.batch_writer {
            batch_writer.flush().await?;
        }
        Ok(())
    }

    /// Get batch writer statistics
    pub fn batch_stats(&self) -> Option<&crate::storage::batch_writer::BatchStats> {
        self.batch_writer.as_ref().map(|bw| bw.stats())
    }

    /// Shutdown the storage engine
    pub async fn shutdown(&mut self) -> Result<()> {
        // Flush any pending batch operations
        if let Some(ref mut batch_writer) = self.batch_writer {
            batch_writer.flush().await?;
        }

        // Stop compaction first
        self.compaction.shutdown().await?;

        // Flush any remaining data
        self.flush().await?;

        // Close WAL
        if self.config.storage.wal.enabled {
            self.wal.close().await?;
        }

        Ok(())
    }

    /// Merge scan results from MemTable and SSTables
    fn merge_scan_results(
        &self,
        memtable_results: Vec<(RowKey, Value)>,
        sstable_results: Vec<(RowKey, Value)>,
        limit: Option<usize>,
    ) -> Vec<(RowKey, Value)> {
        // Simple merge - in a real implementation, this would be more sophisticated
        // with proper tombstone handling and deduplication
        let mut merged = memtable_results;

        for (key, value) in sstable_results {
            // Only add if not already present in memtable results
            if !merged.iter().any(|(k, _)| k == &key) {
                merged.push((key, value));
            }
        }

        // Sort by key
        merged.sort_by(|a, b| a.0.cmp(&b.0));

        // Apply limit
        if let Some(limit) = limit {
            merged.truncate(limit);
        }

        merged
    }
}

/// Batch operation types
#[derive(Debug, Clone)]
pub enum BatchOperation {
    /// Put operation
    Put {
        table_id: TableId,
        key: RowKey,
        value: Value,
    },
    /// Delete operation
    Delete { table_id: TableId, key: RowKey },
    /// Merge operation
    Merge {
        table_id: TableId,
        key: RowKey,
        value: Value,
    },
}

/// Storage engine statistics
#[derive(Debug, Clone)]
pub struct StorageStats {
    /// MemTable statistics
    pub memtable: memtable::MemTableStats,

    /// SSTable statistics
    pub sstables: sstable::SSTableStats,

    /// WAL statistics (if enabled)
    pub wal: Option<wal::WalStats>,

    /// Compaction statistics
    pub compaction: compaction::CompactionStats,

    /// Batch writer statistics (if enabled)
    pub batch_writer: Option<batch_writer::BatchStats>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_storage_engine_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        let mut storage = StorageEngine::open(temp_dir.path(), &config, platform)
            .await
            .unwrap();
        let stats = storage.stats().await.unwrap();

        assert_eq!(stats.sstables.sstable_count, 0);
        storage.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_batch_operations() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        let mut storage = StorageEngine::open(temp_dir.path(), &config, platform)
            .await
            .unwrap();

        // Test batch write operations
        let batch_ops = vec![
            BatchOperation::Put {
                table_id: TableId::new("test_table"),
                key: RowKey::from("key1"),
                value: Value::Text("value1".to_string()),
            },
            BatchOperation::Put {
                table_id: TableId::new("test_table"),
                key: RowKey::from("key2"),
                value: Value::Text("value2".to_string()),
            },
            BatchOperation::Delete {
                table_id: TableId::new("test_table"),
                key: RowKey::from("key3"),
            },
        ];

        storage.batch_write(batch_ops).await.unwrap();
        storage.flush_batch().await.unwrap();

        // Verify data
        let result1 = storage
            .get(&TableId::new("test_table"), &RowKey::from("key1"))
            .await
            .unwrap();
        assert!(result1.is_some());

        let result2 = storage
            .get(&TableId::new("test_table"), &RowKey::from("key2"))
            .await
            .unwrap();
        assert!(result2.is_some());

        storage.shutdown().await.unwrap();
    }
}
