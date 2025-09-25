//! Write-Ahead Log (WAL) implementation for durability

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::Mutex;
use tokio::time::timeout;

use crate::error::Error;
use crate::{Config, Result, RowKey, Value, platform::Platform, types::TableId};

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

/// WAL state protected by a single lock to prevent deadlocks
#[derive(Debug)]
struct WalState {
    /// File handle for writing
    file: tokio::fs::File,

    /// Current file size
    file_size: u64,

    /// Entry count
    entry_count: u64,
}

/// Write-Ahead Log for durability
#[derive(Debug)]
pub struct WriteAheadLog {
    /// Path to the WAL file
    file_path: PathBuf,

    /// WAL state protected by a single lock
    state: Arc<Mutex<WalState>>,

    /// Platform abstraction
    #[allow(dead_code)]
    platform: Arc<Platform>,

    /// Configuration
    config: Config,

    /// Operation timeout
    operation_timeout: Duration,
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
            .map_err(Error::from)?;

        let file_size = file.metadata().await.map_err(Error::from)?.len();

        let state = WalState {
            file,
            file_size,
            entry_count: 0,
        };

        Ok(Self {
            file_path,
            state: Arc::new(Mutex::new(state)),
            platform,
            config: config.clone(),
            operation_timeout: Duration::from_secs(30), // 30 second timeout
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

    /// Write an entry to the WAL with timeout protection
    async fn write_entry(&self, entry: &WalEntry) -> Result<()> {
        let serialized =
            bincode::serialize(entry).map_err(|e| Error::serialization(e.to_string()))?;

        // Write length prefix
        let length = serialized.len() as u32;
        let length_bytes = length.to_le_bytes();

        // Use timeout to prevent hanging
        let result = timeout(self.operation_timeout, async {
            let mut state = self.state.lock().await;

            // Write length prefix
            state
                .file
                .write_all(&length_bytes)
                .await
                .map_err(Error::from)?;

            // Write entry data
            state
                .file
                .write_all(&serialized)
                .await
                .map_err(Error::from)?;

            // Update counters
            state.file_size += (length_bytes.len() + serialized.len()) as u64;
            state.entry_count += 1;

            // Auto-sync if configured
            if self.config.storage.wal.sync_writes {
                state.file.sync_all().await.map_err(Error::from)?;
            }

            Ok::<(), Error>(())
        })
        .await;

        match result {
            Ok(inner_result) => inner_result,
            Err(_) => Err(Error::Timeout(
                "WAL write operation timed out after 30 seconds".to_string(),
            )),
        }
    }

    /// Flush all pending writes to disk
    pub async fn flush(&self) -> Result<()> {
        let result = timeout(self.operation_timeout, async {
            let state = self.state.lock().await;
            state.file.sync_all().await.map_err(Error::from)?;
            Ok::<(), Error>(())
        })
        .await;

        match result {
            Ok(inner_result) => inner_result,
            Err(_) => Err(Error::Timeout(
                "WAL flush operation timed out after 30 seconds".to_string(),
            )),
        }
    }

    /// Read all entries from the WAL
    pub async fn read_all(&self) -> Result<Vec<WalEntry>> {
        let mut entries = Vec::new();
        let mut state = self.state.lock().await;

        // Seek to beginning
        state
            .file
            .seek(SeekFrom::Start(0))
            .await
            .map_err(Error::from)?;

        // Read entries
        loop {
            // Read length prefix
            let mut length_bytes = [0u8; 4];
            match state.file.read_exact(&mut length_bytes).await {
                Ok(_) => {
                    let length = u32::from_le_bytes(length_bytes) as usize;

                    // Read entry data
                    let mut entry_data = vec![0u8; length];
                    state
                        .file
                        .read_exact(&mut entry_data)
                        .await
                        .map_err(Error::from)?;

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
        let mut state = self.state.lock().await;

        state.file.set_len(0).await.map_err(Error::from)?;
        state
            .file
            .seek(SeekFrom::Start(0))
            .await
            .map_err(Error::from)?;

        state.file_size = 0;
        state.entry_count = 0;

        Ok(())
    }

    /// Get WAL statistics
    pub async fn stats(&self) -> Result<WalStats> {
        let state = self.state.lock().await;
        let file_size = state.file_size;
        let entry_count = state.entry_count;

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
