//! Batch writer for efficient write operations

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::storage::sstable::SSTableManager;
use crate::storage::wal::WriteAheadLog;
use crate::{types::TableId, Config, Result, RowKey, Value};

/// Batch writer for efficient write operations
#[derive(Debug)]
pub struct BatchWriter {
    /// Configuration
    config: Config,

    /// SSTable manager
    sstable_manager: Arc<SSTableManager>,

    /// Write-ahead log
    wal: Arc<WriteAheadLog>,

    /// Current batch
    batch: Vec<BatchEntry>,

    /// Batch statistics
    stats: BatchStats,

    /// Last flush time
    last_flush: Instant,

    /// Auto-flush settings
    auto_flush_size: usize,
    auto_flush_interval: Duration,
}

/// Entry in a write batch
#[derive(Debug, Clone)]
pub struct BatchEntry {
    /// Table identifier
    pub table_id: TableId,

    /// Row key
    pub key: RowKey,

    /// Value (None for deletions)
    pub value: Option<Value>,

    /// Timestamp for ordering
    pub timestamp: u64,

    /// Entry type
    pub entry_type: EntryType,
}

/// Type of batch entry
#[derive(Debug, Clone, PartialEq)]
pub enum EntryType {
    /// Insert or update operation
    Put,
    /// Delete operation
    Delete,
    /// Merge operation (for counters, sets, etc.)
    Merge,
}

/// Batch writer statistics
#[derive(Debug, Clone, Default)]
pub struct BatchStats {
    /// Total entries processed
    pub total_entries: u64,

    /// Total batches written
    pub total_batches: u64,

    /// Total bytes written
    pub total_bytes: u64,

    /// Average batch size
    pub avg_batch_size: f64,

    /// Write throughput (entries/second)
    pub write_throughput: f64,

    /// Total write time
    pub total_write_time: Duration,

    /// Last write time
    pub last_write_time: Duration,
}

impl BatchWriter {
    /// Create a new batch writer
    pub fn new(
        config: Config,
        sstable_manager: Arc<SSTableManager>,
        wal: Arc<WriteAheadLog>,
    ) -> Self {
        Self {
            auto_flush_size: config.storage.memtable_size_threshold as usize / 1024, // Convert to entry count estimate
            auto_flush_interval: Duration::from_millis(100),
            config,
            sstable_manager,
            wal,
            batch: Vec::new(),
            stats: BatchStats::default(),
            last_flush: Instant::now(),
        }
    }

    /// Add a put operation to the batch
    pub fn put(&mut self, table_id: TableId, key: RowKey, value: Value) -> Result<()> {
        let entry = BatchEntry {
            table_id,
            key,
            value: Some(value),
            timestamp: self.current_timestamp(),
            entry_type: EntryType::Put,
        };

        self.batch.push(entry);
        self.check_auto_flush()?;

        Ok(())
    }

    /// Add a delete operation to the batch
    pub fn delete(&mut self, table_id: TableId, key: RowKey) -> Result<()> {
        let entry = BatchEntry {
            table_id,
            key,
            value: None,
            timestamp: self.current_timestamp(),
            entry_type: EntryType::Delete,
        };

        self.batch.push(entry);
        self.check_auto_flush()?;

        Ok(())
    }

    /// Add a merge operation to the batch
    pub fn merge(&mut self, table_id: TableId, key: RowKey, value: Value) -> Result<()> {
        let entry = BatchEntry {
            table_id,
            key,
            value: Some(value),
            timestamp: self.current_timestamp(),
            entry_type: EntryType::Merge,
        };

        self.batch.push(entry);
        self.check_auto_flush()?;

        Ok(())
    }

    /// Add multiple operations in a single batch
    pub fn add_batch(&mut self, entries: Vec<BatchEntry>) -> Result<()> {
        for entry in entries {
            self.batch.push(entry);
        }

        self.check_auto_flush()?;
        Ok(())
    }

    /// Flush the current batch to storage
    pub async fn flush(&mut self) -> Result<()> {
        if self.batch.is_empty() {
            return Ok(());
        }

        let start_time = Instant::now();

        // Sort batch by table and key for optimal storage
        self.batch.sort_by(|a, b| {
            a.table_id
                .cmp(&b.table_id)
                .then_with(|| a.key.cmp(&b.key))
                .then_with(|| a.timestamp.cmp(&b.timestamp))
        });

        // Group entries by table for efficient processing
        let mut table_groups: HashMap<TableId, Vec<BatchEntry>> = HashMap::new();
        for entry in &self.batch {
            table_groups
                .entry(entry.table_id.clone())
                .or_insert_with(Vec::new)
                .push(entry.clone());
        }

        // Write to WAL first for durability
        if self.config.storage.wal.enabled {
            self.write_to_wal(&self.batch).await?;
        }

        // Process each table group
        for (table_id, entries) in table_groups {
            self.process_table_batch(&table_id, entries).await?;
        }

        // Update statistics
        let write_time = start_time.elapsed();
        self.update_stats(write_time);

        // Clear batch
        self.batch.clear();
        self.last_flush = Instant::now();

        Ok(())
    }

    /// Process a batch of entries for a specific table
    async fn process_table_batch(
        &self,
        table_id: &TableId,
        entries: Vec<BatchEntry>,
    ) -> Result<()> {
        // Convert batch entries to storage format
        let mut sstable_entries = Vec::new();

        for entry in entries {
            match entry.entry_type {
                EntryType::Put => {
                    if let Some(value) = entry.value {
                        sstable_entries.push((table_id.clone(), entry.key, value));
                    }
                }
                EntryType::Delete => {
                    // Use tombstone marker for deletions
                    sstable_entries.push((table_id.clone(), entry.key, Value::Null));
                }
                EntryType::Merge => {
                    // For now, treat merge as put (can be enhanced later)
                    if let Some(value) = entry.value {
                        sstable_entries.push((table_id.clone(), entry.key, value));
                    }
                }
            }
        }

        // Write to SSTable if we have entries
        if !sstable_entries.is_empty() {
            self.sstable_manager
                .create_from_memtable(sstable_entries)
                .await?;
        }

        Ok(())
    }

    /// Write batch to WAL for durability
    async fn write_to_wal(&self, entries: &[BatchEntry]) -> Result<()> {
        for entry in entries {
            match entry.entry_type {
                EntryType::Put => {
                    if let Some(ref value) = entry.value {
                        self.wal.append(&entry.table_id, &entry.key, value).await?;
                    }
                }
                EntryType::Delete => {
                    self.wal
                        .append_tombstone(&entry.table_id, &entry.key)
                        .await?;
                }
                EntryType::Merge => {
                    // For now, treat merge as put in WAL
                    if let Some(ref value) = entry.value {
                        self.wal.append(&entry.table_id, &entry.key, value).await?;
                    }
                }
            }
        }

        Ok(())
    }

    /// Check if auto-flush conditions are met
    fn check_auto_flush(&mut self) -> Result<()> {
        let should_flush = self.batch.len() >= self.auto_flush_size
            || self.last_flush.elapsed() >= self.auto_flush_interval;

        if should_flush {
            // For now, just mark that we need to flush
            // In a real implementation, this would trigger async flush
            return Ok(());
        }

        Ok(())
    }

    /// Get current timestamp in microseconds
    fn current_timestamp(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64
    }

    /// Update batch statistics
    fn update_stats(&mut self, write_time: Duration) {
        let batch_size = self.batch.len() as u64;

        self.stats.total_entries += batch_size;
        self.stats.total_batches += 1;
        self.stats.total_write_time += write_time;
        self.stats.last_write_time = write_time;

        // Update average batch size
        self.stats.avg_batch_size =
            self.stats.total_entries as f64 / self.stats.total_batches as f64;

        // Update throughput (entries per second)
        if self.stats.total_write_time.as_secs_f64() > 0.0 {
            self.stats.write_throughput =
                self.stats.total_entries as f64 / self.stats.total_write_time.as_secs_f64();
        }
    }

    /// Get batch writer statistics
    pub fn stats(&self) -> &BatchStats {
        &self.stats
    }

    /// Get current batch size
    pub fn batch_size(&self) -> usize {
        self.batch.len()
    }

    /// Check if batch is empty
    pub fn is_empty(&self) -> bool {
        self.batch.is_empty()
    }

    /// Clear the current batch without writing
    pub fn clear(&mut self) {
        self.batch.clear();
    }

    /// Set auto-flush parameters
    pub fn set_auto_flush(&mut self, size: usize, interval: Duration) {
        self.auto_flush_size = size;
        self.auto_flush_interval = interval;
    }

    /// Force flush if conditions are met
    pub async fn maybe_flush(&mut self) -> Result<()> {
        if self.batch.len() >= self.auto_flush_size
            || self.last_flush.elapsed() >= self.auto_flush_interval
        {
            self.flush().await?;
        }
        Ok(())
    }
}

/// Builder for configuring batch writer
pub struct BatchWriterBuilder {
    config: Config,
    auto_flush_size: Option<usize>,
    auto_flush_interval: Option<Duration>,
}

impl BatchWriterBuilder {
    /// Create a new builder
    pub fn new(config: Config) -> Self {
        Self {
            config,
            auto_flush_size: None,
            auto_flush_interval: None,
        }
    }

    /// Set auto-flush size
    pub fn with_auto_flush_size(mut self, size: usize) -> Self {
        self.auto_flush_size = Some(size);
        self
    }

    /// Set auto-flush interval
    pub fn with_auto_flush_interval(mut self, interval: Duration) -> Self {
        self.auto_flush_interval = Some(interval);
        self
    }

    /// Build the batch writer
    pub fn build(
        self,
        sstable_manager: Arc<SSTableManager>,
        wal: Arc<WriteAheadLog>,
    ) -> BatchWriter {
        let mut writer = BatchWriter::new(self.config, sstable_manager, wal);

        if let Some(size) = self.auto_flush_size {
            writer.auto_flush_size = size;
        }

        if let Some(interval) = self.auto_flush_interval {
            writer.auto_flush_interval = interval;
        }

        writer
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::platform::Platform;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_batch_writer_put_operations() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        let sstable_manager = Arc::new(
            SSTableManager::new(temp_dir.path(), &config, platform.clone())
                .await
                .unwrap(),
        );
        let wal = Arc::new(
            WriteAheadLog::open(temp_dir.path(), &config, platform)
                .await
                .unwrap(),
        );

        let mut writer = BatchWriter::new(config, sstable_manager, wal);

        // Add some put operations
        writer
            .put(
                TableId::new("test_table"),
                RowKey::from("key1"),
                Value::Text("value1".to_string()),
            )
            .unwrap();

        writer
            .put(
                TableId::new("test_table"),
                RowKey::from("key2"),
                Value::Text("value2".to_string()),
            )
            .unwrap();

        assert_eq!(writer.batch_size(), 2);
        assert!(!writer.is_empty());

        // Flush and check stats
        writer.flush().await.unwrap();

        assert_eq!(writer.batch_size(), 0);
        assert!(writer.is_empty());

        let stats = writer.stats();
        assert_eq!(stats.total_entries, 2);
        assert_eq!(stats.total_batches, 1);
    }

    #[tokio::test]
    async fn test_batch_writer_mixed_operations() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        let sstable_manager = Arc::new(
            SSTableManager::new(temp_dir.path(), &config, platform.clone())
                .await
                .unwrap(),
        );
        let wal = Arc::new(
            WriteAheadLog::open(temp_dir.path(), &config, platform)
                .await
                .unwrap(),
        );

        let mut writer = BatchWriter::new(config, sstable_manager, wal);

        // Add mixed operations
        writer
            .put(
                TableId::new("test_table"),
                RowKey::from("key1"),
                Value::Text("value1".to_string()),
            )
            .unwrap();

        writer
            .delete(TableId::new("test_table"), RowKey::from("key2"))
            .unwrap();

        writer
            .merge(
                TableId::new("test_table"),
                RowKey::from("key3"),
                Value::Integer(42),
            )
            .unwrap();

        assert_eq!(writer.batch_size(), 3);

        writer.flush().await.unwrap();

        let stats = writer.stats();
        assert_eq!(stats.total_entries, 3);
        assert!(stats.write_throughput > 0.0);
    }

    #[tokio::test]
    async fn test_batch_writer_builder() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        let sstable_manager = Arc::new(
            SSTableManager::new(temp_dir.path(), &config, platform.clone())
                .await
                .unwrap(),
        );
        let wal = Arc::new(
            WriteAheadLog::open(temp_dir.path(), &config, platform)
                .await
                .unwrap(),
        );

        let writer = BatchWriterBuilder::new(config)
            .with_auto_flush_size(100)
            .with_auto_flush_interval(Duration::from_millis(50))
            .build(sstable_manager, wal);

        assert_eq!(writer.auto_flush_size, 100);
        assert_eq!(writer.auto_flush_interval, Duration::from_millis(50));
    }
}
