//! Write-Ahead Log (WAL) implementation for durability

use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::Mutex;

use crate::error::Error;
use crate::{platform::Platform, types::TableId, Config, Result, RowKey, Value};

/// WAL entry types
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum WalEntry {
    /// Insert or update entry
    Put {
        table_id: TableId,
        key: RowKey,
        value: Value,
        timestamp: u64,
    },
    /// Delete entry (tombstone)
    Delete {
        table_id: TableId,
        key: RowKey,
        timestamp: u64,
    },
    /// Checkpoint marker
    Checkpoint { timestamp: u64 },
}

/// Write-Ahead Log for durability
#[derive(Debug)]
pub struct WriteAheadLog {
    /// Path to the WAL file
    file_path: PathBuf,

    /// File handle for writing
    file: Arc<Mutex<tokio::fs::File>>,

    /// Platform abstraction
    platform: Arc<Platform>,

    /// Configuration
    config: Config,

    /// Current file size
    file_size: Arc<Mutex<u64>>,

    /// Entry count
    entry_count: Arc<Mutex<u64>>,
}

impl WriteAheadLog {
    /// Open or create a WAL file
    pub async fn open(path: &Path, config: &Config, platform: Arc<Platform>) -> Result<Self> {
        let file_path = path.join("wal.log");

        // Create file if it doesn't exist
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&file_path)
            .await
            .map_err(|e| Error::from(e))?;

        let file_size = file.metadata().await.map_err(|e| Error::from(e))?.len();

        Ok(Self {
            file_path,
            file: Arc::new(Mutex::new(file)),
            platform,
            config: config.clone(),
            file_size: Arc::new(Mutex::new(file_size)),
            entry_count: Arc::new(Mutex::new(0)),
        })
    }

    /// Append a put entry to the WAL
    pub async fn append(&self, table_id: &TableId, key: &RowKey, value: &Value) -> Result<()> {
        let timestamp = self.platform.time().now_micros();

        let entry = WalEntry::Put {
            table_id: table_id.clone(),
            key: key.clone(),
            value: value.clone(),
            timestamp,
        };

        self.write_entry(&entry).await
    }

    /// Append a delete entry (tombstone) to the WAL
    pub async fn append_tombstone(&self, table_id: &TableId, key: &RowKey) -> Result<()> {
        let timestamp = self.platform.time().now_micros();

        let entry = WalEntry::Delete {
            table_id: table_id.clone(),
            key: key.clone(),
            timestamp,
        };

        self.write_entry(&entry).await
    }

    /// Write a checkpoint marker
    pub async fn checkpoint(&self) -> Result<()> {
        let timestamp = self.platform.time().now_micros();

        let entry = WalEntry::Checkpoint { timestamp };

        self.write_entry(&entry).await
    }

    /// Write an entry to the WAL
    async fn write_entry(&self, entry: &WalEntry) -> Result<()> {
        let serialized =
            bincode::serialize(entry).map_err(|e| Error::serialization(e.to_string()))?;

        // Write length prefix
        let length = serialized.len() as u32;
        let length_bytes = length.to_le_bytes();

        let mut file = self.file.lock().await;
        let mut file_size = self.file_size.lock().await;
        let mut entry_count = self.entry_count.lock().await;

        // Write length prefix
        file.write_all(&length_bytes)
            .await
            .map_err(|e| Error::from(e))?;

        // Write entry data
        file.write_all(&serialized)
            .await
            .map_err(|e| Error::from(e))?;

        // Update counters
        *file_size += (length_bytes.len() + serialized.len()) as u64;
        *entry_count += 1;

        // Auto-sync if configured
        if self.config.storage.wal.sync_writes {
            file.sync_all().await.map_err(|e| Error::from(e))?;
        }

        Ok(())
    }

    /// Flush all pending writes to disk
    pub async fn flush(&self) -> Result<()> {
        let mut file = self.file.lock().await;
        file.sync_all().await.map_err(|e| Error::from(e))?;
        Ok(())
    }

    /// Read all entries from the WAL
    pub async fn read_all(&self) -> Result<Vec<WalEntry>> {
        let mut entries = Vec::new();
        let mut file = self.file.lock().await;

        // Seek to beginning
        file.seek(SeekFrom::Start(0))
            .await
            .map_err(|e| Error::from(e))?;

        // Read entries
        loop {
            // Read length prefix
            let mut length_bytes = [0u8; 4];
            match file.read_exact(&mut length_bytes).await {
                Ok(_) => {
                    let length = u32::from_le_bytes(length_bytes) as usize;

                    // Read entry data
                    let mut entry_data = vec![0u8; length];
                    file.read_exact(&mut entry_data)
                        .await
                        .map_err(|e| Error::from(e))?;

                    // Deserialize entry
                    let entry: WalEntry = bincode::deserialize(&entry_data)
                        .map_err(|e| Error::serialization(e.to_string()))?;

                    entries.push(entry);
                }
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // End of file reached
                    break;
                }
                Err(e) => {
                    return Err(Error::from(e));
                }
            }
        }

        Ok(entries)
    }

    /// Truncate the WAL file
    pub async fn truncate(&self) -> Result<()> {
        let mut file = self.file.lock().await;
        let mut file_size = self.file_size.lock().await;
        let mut entry_count = self.entry_count.lock().await;

        file.set_len(0).await.map_err(|e| Error::from(e))?;
        file.seek(SeekFrom::Start(0))
            .await
            .map_err(|e| Error::from(e))?;

        *file_size = 0;
        *entry_count = 0;

        Ok(())
    }

    /// Get WAL statistics
    pub async fn stats(&self) -> Result<WalStats> {
        let file_size = *self.file_size.lock().await;
        let entry_count = *self.entry_count.lock().await;

        Ok(WalStats {
            file_size,
            entry_count,
            file_path: self.file_path.clone(),
        })
    }

    /// Close the WAL
    pub async fn close(&self) -> Result<()> {
        self.flush().await
    }

    /// Rotate the WAL file (create a new one)
    pub async fn rotate(&self) -> Result<()> {
        // Create backup of current WAL
        let backup_path = self.file_path.with_extension("log.backup");
        self.platform
            .fs()
            .copy(&self.file_path, &backup_path)
            .await?;

        // Truncate current WAL
        self.truncate().await?;

        Ok(())
    }
}

/// WAL statistics
#[derive(Debug, Clone)]
pub struct WalStats {
    /// Size of the WAL file in bytes
    pub file_size: u64,

    /// Number of entries in the WAL
    pub entry_count: u64,

    /// Path to the WAL file
    pub file_path: PathBuf,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TableId;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_wal_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        let wal = WriteAheadLog::open(temp_dir.path(), &config, platform)
            .await
            .unwrap();
        let stats = wal.stats().await.unwrap();

        assert_eq!(stats.entry_count, 0);
        assert_eq!(stats.file_size, 0);
    }

    #[tokio::test]
    async fn test_wal_append() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        let wal = WriteAheadLog::open(temp_dir.path(), &config, platform)
            .await
            .unwrap();

        let table_id = TableId::new("test_table");
        let key = RowKey::from("test_key");
        let value = Value::Text("test_value".to_string());

        wal.append(&table_id, &key, &value).await.unwrap();

        let stats = wal.stats().await.unwrap();
        assert_eq!(stats.entry_count, 1);
        assert!(stats.file_size > 0);
    }

    #[tokio::test]
    async fn test_wal_read_all() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        let wal = WriteAheadLog::open(temp_dir.path(), &config, platform)
            .await
            .unwrap();

        let table_id = TableId::new("test_table");
        let key1 = RowKey::from("key1");
        let key2 = RowKey::from("key2");
        let value1 = Value::Text("value1".to_string());
        let value2 = Value::Text("value2".to_string());

        wal.append(&table_id, &key1, &value1).await.unwrap();
        wal.append(&table_id, &key2, &value2).await.unwrap();
        wal.append_tombstone(&table_id, &key1).await.unwrap();

        let entries = wal.read_all().await.unwrap();
        assert_eq!(entries.len(), 3);

        match &entries[0] {
            WalEntry::Put {
                table_id: tid,
                key,
                value,
                ..
            } => {
                assert_eq!(tid, &table_id);
                assert_eq!(key, &key1);
                assert_eq!(value, &value1);
            }
            _ => panic!("Expected Put entry"),
        }

        match &entries[2] {
            WalEntry::Delete {
                table_id: tid, key, ..
            } => {
                assert_eq!(tid, &table_id);
                assert_eq!(key, &key1);
            }
            _ => panic!("Expected Delete entry"),
        }
    }

    #[tokio::test]
    async fn test_wal_truncate() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        let wal = WriteAheadLog::open(temp_dir.path(), &config, platform)
            .await
            .unwrap();

        let table_id = TableId::new("test_table");
        let key = RowKey::from("test_key");
        let value = Value::Text("test_value".to_string());

        wal.append(&table_id, &key, &value).await.unwrap();

        let stats_before = wal.stats().await.unwrap();
        assert_eq!(stats_before.entry_count, 1);
        assert!(stats_before.file_size > 0);

        wal.truncate().await.unwrap();

        let stats_after = wal.stats().await.unwrap();
        assert_eq!(stats_after.entry_count, 0);
        assert_eq!(stats_after.file_size, 0);
    }
}
