//! Manifest management for metadata and transaction logs

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::error::Error;
use crate::storage::sstable::SSTableId;
use crate::{types::TableId, Config, Result};

/// Manifest entry types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ManifestEntry {
    /// SSTable creation
    SSTableCreated {
        sstable_id: SSTableId,
        table_id: TableId,
        timestamp: u64,
        size: u64,
    },
    /// SSTable deletion
    SSTableDeleted {
        sstable_id: SSTableId,
        timestamp: u64,
    },
    /// Compaction operation
    Compaction {
        input_sstables: Vec<SSTableId>,
        output_sstable: SSTableId,
        timestamp: u64,
    },
    /// Schema change
    SchemaChange {
        table_id: TableId,
        schema_version: u32,
        timestamp: u64,
    },
}

/// Manifest state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestState {
    /// Active SSTables
    pub active_sstables: HashMap<SSTableId, SSTableMetadata>,

    /// Schema versions
    pub schema_versions: HashMap<TableId, u32>,

    /// Current manifest version
    pub version: u64,

    /// Last updated timestamp
    pub last_updated: u64,
}

/// SSTable metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSTableMetadata {
    /// Table ID
    pub table_id: TableId,

    /// File size in bytes
    pub size: u64,

    /// Creation timestamp
    pub created_at: u64,

    /// Entry count
    pub entry_count: u64,

    /// Compression enabled
    pub compressed: bool,
}

/// Manifest manager
#[derive(Debug)]
pub struct Manifest {
    /// Path to manifest file
    manifest_path: PathBuf,

    /// Current manifest state
    state: Arc<RwLock<ManifestState>>,

    /// Configuration
    config: Config,

    /// Sequence number for ordering
    sequence: Arc<RwLock<u64>>,
}

impl Manifest {
    /// Create a new manifest
    pub async fn new(base_path: &Path, config: &Config) -> Result<Self> {
        let manifest_path = base_path.join("MANIFEST");

        let state = if tokio::fs::metadata(&manifest_path).await.is_ok() {
            // Load existing manifest
            let data = tokio::fs::read(&manifest_path)
                .await
                .map_err(|e| Error::io(e.to_string()))?;

            bincode::deserialize(&data).map_err(|e| Error::serialization(e.to_string()))?
        } else {
            // Create new manifest
            ManifestState {
                active_sstables: HashMap::new(),
                schema_versions: HashMap::new(),
                version: 1,
                last_updated: 0,
            }
        };

        Ok(Self {
            manifest_path,
            state: Arc::new(RwLock::new(state)),
            config: config.clone(),
            sequence: Arc::new(RwLock::new(0)),
        })
    }

    /// Open an existing manifest
    pub async fn open(base_path: &Path, config: &Config) -> Result<Self> {
        Self::new(base_path, config).await
    }

    /// Add SSTable created entry
    pub async fn add_sstable_created(&self) -> Result<()> {
        let timestamp = self.current_timestamp();

        // This would normally record specific SSTable information
        // For now, just increment version
        let mut state = self.state.write().await;
        state.version += 1;
        state.last_updated = timestamp;

        self.persist_state(&state).await?;

        Ok(())
    }

    /// Record SSTable creation
    pub async fn record_sstable_created(
        &self,
        sstable_id: SSTableId,
        table_id: TableId,
        size: u64,
        entry_count: u64,
        compressed: bool,
    ) -> Result<()> {
        let timestamp = self.current_timestamp();

        let metadata = SSTableMetadata {
            table_id,
            size,
            created_at: timestamp,
            entry_count,
            compressed,
        };

        let mut state = self.state.write().await;
        state.active_sstables.insert(sstable_id, metadata);
        state.version += 1;
        state.last_updated = timestamp;

        self.persist_state(&state).await?;

        Ok(())
    }

    /// Record SSTable deletion
    pub async fn record_sstable_deleted(&self, sstable_id: &SSTableId) -> Result<()> {
        let timestamp = self.current_timestamp();

        let mut state = self.state.write().await;
        state.active_sstables.remove(sstable_id);
        state.version += 1;
        state.last_updated = timestamp;

        self.persist_state(&state).await?;

        Ok(())
    }

    /// Record compaction operation
    pub async fn record_compaction(
        &self,
        input_sstables: &[SSTableId],
        output_sstable: &SSTableId,
    ) -> Result<()> {
        let timestamp = self.current_timestamp();

        let mut state = self.state.write().await;

        // Remove input SSTables
        for sstable_id in input_sstables {
            state.active_sstables.remove(sstable_id);
        }

        // Add output SSTable metadata (placeholder)
        let metadata = SSTableMetadata {
            table_id: TableId::new("unknown"), // Would be determined from compaction
            size: 0,
            created_at: timestamp,
            entry_count: 0,
            compressed: false,
        };

        state
            .active_sstables
            .insert(output_sstable.clone(), metadata);
        state.version += 1;
        state.last_updated = timestamp;

        self.persist_state(&state).await?;

        Ok(())
    }

    /// Record schema change
    pub async fn record_schema_change(
        &self,
        table_id: &TableId,
        schema_version: u32,
    ) -> Result<()> {
        let timestamp = self.current_timestamp();

        let mut state = self.state.write().await;
        state
            .schema_versions
            .insert(table_id.clone(), schema_version);
        state.version += 1;
        state.last_updated = timestamp;

        self.persist_state(&state).await?;

        Ok(())
    }

    /// Get active SSTables
    pub async fn get_active_sstables(&self) -> HashMap<SSTableId, SSTableMetadata> {
        let state = self.state.read().await;
        state.active_sstables.clone()
    }

    /// Get schema version for table
    pub async fn get_schema_version(&self, table_id: &TableId) -> Option<u32> {
        let state = self.state.read().await;
        state.schema_versions.get(table_id).copied()
    }

    /// Get manifest statistics
    pub async fn stats(&self) -> ManifestStats {
        let state = self.state.read().await;

        let sstable_count = state.active_sstables.len();
        let total_size = state.active_sstables.values().map(|m| m.size).sum();
        let table_count = state.schema_versions.len();

        ManifestStats {
            version: state.version,
            last_updated: state.last_updated,
            sstable_count,
            total_size,
            table_count,
        }
    }

    /// Persist manifest state to disk
    async fn persist_state(&self, state: &ManifestState) -> Result<()> {
        let data = bincode::serialize(state).map_err(|e| Error::serialization(e.to_string()))?;

        tokio::fs::write(&self.manifest_path, data)
            .await
            .map_err(|e| Error::io(e.to_string()))?;

        Ok(())
    }

    /// Get current timestamp
    fn current_timestamp(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64
    }

    /// Get next sequence number
    pub async fn next_sequence(&self) -> u64 {
        let mut seq = self.sequence.write().await;
        *seq += 1;
        *seq
    }

    /// Checkpoint the manifest
    pub async fn checkpoint(&self) -> Result<()> {
        let state = self.state.read().await;
        let timestamp = self.current_timestamp();

        // Create checkpoint file
        let checkpoint_path = self.manifest_path.with_extension("checkpoint");
        let data = bincode::serialize(&*state).map_err(|e| Error::serialization(e.to_string()))?;

        tokio::fs::write(&checkpoint_path, data)
            .await
            .map_err(|e| Error::io(e.to_string()))?;

        Ok(())
    }
}

/// Manifest statistics
#[derive(Debug, Clone)]
pub struct ManifestStats {
    /// Manifest version
    pub version: u64,

    /// Last updated timestamp
    pub last_updated: u64,

    /// Number of active SSTables
    pub sstable_count: usize,

    /// Total size of all SSTables
    pub total_size: u64,

    /// Number of tables
    pub table_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TableId;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_manifest_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();

        let manifest = Manifest::new(temp_dir.path(), &config).await.unwrap();
        let stats = manifest.stats().await;

        assert_eq!(stats.version, 1);
        assert_eq!(stats.sstable_count, 0);
        assert_eq!(stats.table_count, 0);
    }

    #[tokio::test]
    async fn test_manifest_sstable_operations() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();

        let manifest = Manifest::new(temp_dir.path(), &config).await.unwrap();

        let sstable_id = SSTableId::new();
        let table_id = TableId::new("test_table");

        manifest
            .record_sstable_created(sstable_id.clone(), table_id.clone(), 1024, 100, false)
            .await
            .unwrap();

        let active_sstables = manifest.get_active_sstables().await;
        assert_eq!(active_sstables.len(), 1);
        assert!(active_sstables.contains_key(&sstable_id));

        let metadata = &active_sstables[&sstable_id];
        assert_eq!(metadata.table_id, table_id);
        assert_eq!(metadata.size, 1024);
        assert_eq!(metadata.entry_count, 100);
        assert!(!metadata.compressed);

        manifest.record_sstable_deleted(&sstable_id).await.unwrap();

        let active_sstables = manifest.get_active_sstables().await;
        assert_eq!(active_sstables.len(), 0);
    }

    #[tokio::test]
    async fn test_manifest_schema_operations() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();

        let manifest = Manifest::new(temp_dir.path(), &config).await.unwrap();

        let table_id = TableId::new("test_table");

        manifest.record_schema_change(&table_id, 1).await.unwrap();

        let schema_version = manifest.get_schema_version(&table_id).await;
        assert_eq!(schema_version, Some(1));

        manifest.record_schema_change(&table_id, 2).await.unwrap();

        let schema_version = manifest.get_schema_version(&table_id).await;
        assert_eq!(schema_version, Some(2));
    }

    #[tokio::test]
    async fn test_manifest_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();

        let table_id = TableId::new("test_table");

        // Create manifest and add data
        {
            let manifest = Manifest::new(temp_dir.path(), &config).await.unwrap();
            manifest.record_schema_change(&table_id, 1).await.unwrap();
        }

        // Reopen manifest and verify data persisted
        {
            let manifest = Manifest::open(temp_dir.path(), &config).await.unwrap();
            let schema_version = manifest.get_schema_version(&table_id).await;
            assert_eq!(schema_version, Some(1));
        }
    }

    #[tokio::test]
    async fn test_manifest_sequence_numbers() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();

        let manifest = Manifest::new(temp_dir.path(), &config).await.unwrap();

        let seq1 = manifest.next_sequence().await;
        let seq2 = manifest.next_sequence().await;
        let seq3 = manifest.next_sequence().await;

        assert_eq!(seq1, 1);
        assert_eq!(seq2, 2);
        assert_eq!(seq3, 3);
    }
}
