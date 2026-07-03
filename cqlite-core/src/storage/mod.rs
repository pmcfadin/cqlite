//! Storage engine implementation for CQLite

pub mod sstable;

// Canonical partition-key (de)serialization, shared by the read (query) path
// and the write engine so the two never drift (Issue #586). Always compiled —
// the scan path needs it even without `write-support`.
pub mod partition_key_codec;

// M5: Write engine and serialization (Issue #359)
#[cfg(feature = "write-support")]
pub mod serialization;
#[cfg(feature = "write-support")]
pub mod write_engine;

// REPL data access components (Issue #249: CLI-specific)
#[cfg(feature = "cli-helpers")]
pub mod repl_data_api;
pub mod schema_discovery;
pub mod sstable_data_manager;

use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(feature = "state_machine")]
use tokio::sync::RwLock;

use crate::platform::Platform;
use crate::{
    types::{CellWriteMetadata, TableId},
    Config, Result, RowKey, ScanRow,
};
// `Value` is only referenced by the experimental write API (`put` / `BatchOperation`);
// gate the import so the default build does not flag it unused (issue #1334).
#[cfg(feature = "experimental")]
use crate::types::Value;

/// Main storage engine that coordinates all storage components
///
/// NOTE: Issue #176 removed write infrastructure (compaction, manifest).
/// This is now a read-only storage layer focused on SSTable access.
#[derive(Debug)]
pub struct StorageEngine {
    /// SSTable manager for persistent storage
    sstables: Arc<sstable::SSTableManager>,

    /// Platform abstraction
    #[allow(dead_code)]
    _platform: Arc<Platform>,

    /// Storage configuration
    #[allow(dead_code)]
    config: Config,

    /// Schema registry for schema-aware operations (feature-gated)
    #[cfg(feature = "state_machine")]
    schema_registry: Arc<RwLock<Option<Arc<RwLock<crate::schema::SchemaRegistry>>>>>,
}

impl StorageEngine {
    /// Open a storage engine at the given path
    ///
    /// This method discovers SSTables by scanning the storage directory.
    /// For pre-discovered SSTables, use `open_with_sstables` instead.
    ///
    /// NOTE: Issue #176 removed write infrastructure (compaction, manifest).
    /// This is now a read-only storage layer focused on SSTable access.
    // `skip_all` (not an explicit skip list): the `schema_registry` parameter is
    // `#[cfg(feature = "state_machine")]`-gated, so naming it in `skip(...)` would
    // reference a nonexistent binding in the minimal build. `skip_all` records no
    // args as fields regardless of cfg; the `fields(...)` below are set in the body.
    #[tracing::instrument(
        name = "storage.engine.open",
        skip_all,
        fields(sstables = tracing::field::Empty, bytes = tracing::field::Empty)
    )]
    pub async fn open(
        path: &Path,
        config: &Config,
        platform: Arc<Platform>,
        #[cfg(feature = "state_machine")] schema_registry: Option<
            Arc<RwLock<crate::schema::SchemaRegistry>>,
        >,
    ) -> Result<Self> {
        // Create storage directory if it doesn't exist
        crate::observability::record_result("reader", platform.fs().create_dir_all(path).await)?;

        // Initialize SSTable manager with schema registry
        let sstables = Arc::new(crate::observability::record_result(
            "reader",
            sstable::SSTableManager::new(
                path,
                config,
                platform.clone(),
                #[cfg(feature = "state_machine")]
                schema_registry.clone(),
            )
            .await,
        )?);

        Self::record_discovery_metrics(&sstables).await;

        Ok(Self {
            sstables,
            _platform: platform,
            config: config.clone(),
            #[cfg(feature = "state_machine")]
            schema_registry: Arc::new(RwLock::new(schema_registry)),
        })
    }

    /// Emit SSTable-discovery telemetry (issue #1034) for a freshly built
    /// manager: total SSTables discovered, their on-disk byte total, and the
    /// number of logical tables. Best-effort — a stats failure is logged and the
    /// open continues, since telemetry must never change open behaviour.
    async fn record_discovery_metrics(sstables: &sstable::SSTableManager) {
        use crate::observability::{self as obs, catalog};
        match sstables.stats().await {
            Ok(stats) => {
                tracing::Span::current().record("sstables", stats.sstable_count as u64);
                tracing::Span::current().record("bytes", stats.total_size);
                obs::add_counter(
                    catalog::STORAGE_OPEN_SSTABLES,
                    stats.sstable_count as u64,
                    &[],
                );
                obs::add_counter(catalog::STORAGE_OPEN_BYTES, stats.total_size, &[]);
                obs::add_counter(catalog::STORAGE_OPEN_TABLES, stats.total_tables, &[]);
            }
            Err(e) => {
                log::debug!("storage.engine.open: discovery metrics unavailable: {}", e);
            }
        }
    }

    /// Open a storage engine with pre-discovered SSTable table directories
    ///
    /// This method is used when SSTables have been discovered externally (e.g., by DiscoveryService)
    /// and allows the storage engine to be initialized with specific table directories rather than
    /// scanning the storage directory. Each table directory will be scanned for Data.db files.
    ///
    /// # Arguments
    /// * `path` - Base storage path for manifest and SSTable operations
    /// * `discovered_table_dirs` - Vector of table directory paths (each containing SSTable files)
    /// * `config` - Storage configuration
    /// * `platform` - Platform abstraction for I/O operations
    ///
    /// # Returns
    /// A StorageEngine instance with all components initialized, including SSTable readers
    /// for all Data.db files found in the discovered table directories.
    ///
    /// # Example
    /// ```no_run
    /// # use std::path::{Path, PathBuf};
    /// # use std::sync::Arc;
    /// # use cqlite_core::{Config, Platform, storage::StorageEngine};
    /// # async fn example() -> cqlite_core::Result<()> {
    /// let config = Config::default();
    /// let platform = Arc::new(Platform::new(&config).await?);
    /// let storage_path = Path::new("/var/lib/cqlite/storage");
    /// let discovered_table_dirs = vec![
    ///     PathBuf::from("/var/lib/cassandra/keyspace1/table1-abc123"),
    ///     PathBuf::from("/var/lib/cassandra/keyspace1/table2-def456"),
    /// ];
    ///
    /// let engine = StorageEngine::open_with_sstables(
    ///     storage_path,
    ///     discovered_table_dirs,
    ///     &config,
    ///     platform,
    ///     #[cfg(feature = "state_machine")]
    ///     None,
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    // `skip_all`: see the note on `open` — the cfg-gated `schema_registry` cannot
    // be named in an explicit `skip(...)` without breaking the minimal build.
    #[tracing::instrument(
        name = "storage.engine.open",
        skip_all,
        fields(sstables = tracing::field::Empty, bytes = tracing::field::Empty)
    )]
    pub async fn open_with_sstables(
        path: &Path,
        discovered_table_dirs: Vec<PathBuf>,
        config: &Config,
        platform: Arc<Platform>,
        #[cfg(feature = "state_machine")] schema_registry: Option<
            Arc<RwLock<crate::schema::SchemaRegistry>>,
        >,
    ) -> Result<Self> {
        // Create storage directory if it doesn't exist
        crate::observability::record_result("reader", platform.fs().create_dir_all(path).await)?;

        // Initialize SSTable manager with pre-discovered paths and schema registry
        let sstables = Arc::new(crate::observability::record_result(
            "reader",
            sstable::SSTableManager::new_from_discovered_paths(
                path,
                discovered_table_dirs,
                config,
                platform.clone(),
                #[cfg(feature = "state_machine")]
                schema_registry.clone(),
            )
            .await,
        )?);

        Self::record_discovery_metrics(&sstables).await;

        Ok(Self {
            sstables,
            _platform: platform,
            config: config.clone(),
            #[cfg(feature = "state_machine")]
            schema_registry: Arc::new(RwLock::new(schema_registry)),
        })
    }

    /// Insert a key-value pair
    ///
    /// NOTE: Write functionality removed in Issue #175 (WAL/MemTable infrastructure deleted).
    /// This method is feature-gated behind 'experimental' but currently unimplemented.
    #[cfg(feature = "experimental")]
    pub async fn put(&self, _table_id: &TableId, _key: RowKey, _value: Value) -> Result<()> {
        Err(crate::error::Error::UnsupportedFormat(
            "Write operations (put) removed in Issue #175 - WAL and MemTable infrastructure deleted".to_string()
        ))
    }

    /// Get a value by key
    pub async fn get(&self, table_id: &TableId, key: &RowKey) -> Result<Option<ScanRow>> {
        // Check SSTables
        self.sstables.get(table_id, key).await
    }

    /// Re-scan the data directory and atomically apply added/removed SSTable
    /// generations to the held reader set (issue #1749).
    ///
    /// # Freshness contract
    ///
    /// A `StorageEngine` (and the [`Database`](crate::Database) built on it)
    /// snapshots the discovered SSTable generations **at open**; it does not
    /// re-scan on its own. This is the ONLY way the reader set changes for a
    /// long-lived handle. Re-runs the same TOC/filename-based discovery `open`
    /// used — no content sniffing.
    ///
    /// - Added generations become queryable; removed generations stop being
    ///   queried; unchanged generations keep their warm parsed state.
    /// - **In-flight queries are unaffected**: a scan already running holds its
    ///   own `Arc` reader clones and completes against the pre-refresh set;
    ///   queries started after the refresh see the new set.
    /// - **Atomic / fail-closed**: if any newly discovered generation fails to
    ///   open (e.g. a corrupt `Statistics.db`, issue #1626), the typed error is
    ///   returned and the previously held reader set is left fully unchanged.
    pub async fn refresh(&self) -> Result<sstable::RefreshReport> {
        self.sstables.refresh_tables().await
    }

    /// Delete a key
    ///
    /// NOTE: Write functionality removed in Issue #175 (WAL/MemTable infrastructure deleted).
    /// This method is feature-gated behind 'experimental' but currently unimplemented.
    #[cfg(feature = "experimental")]
    pub async fn delete(&self, _table_id: &TableId, _key: RowKey) -> Result<()> {
        Err(crate::error::Error::UnsupportedFormat(
            "Write operations (delete) removed in Issue #175 - WAL and MemTable infrastructure deleted".to_string()
        ))
    }

    /// Scan a range of keys
    ///
    /// # Arguments
    /// * `table_id` - The table to scan
    /// * `start_key` - Optional start key for range scan
    /// * `end_key` - Optional end key for range scan
    /// * `limit` - Optional limit on number of results
    /// * `schema` - Optional table schema for schema-aware parsing. When provided,
    ///   enables accurate type detection and avoids heuristic-based parsing.
    ///   Strongly recommended for Cassandra 5.0+ formats.
    pub async fn scan(
        &self,
        table_id: &TableId,
        start_key: Option<&RowKey>,
        end_key: Option<&RowKey>,
        limit: Option<usize>,
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<Vec<(RowKey, ScanRow)>> {
        // Scan SSTables directly
        self.sstables
            .scan(table_id, start_key, end_key, limit, schema)
            .await
    }

    /// Partition-targeted scan for a fully-constrained `WHERE pk = ?` (Issue #949).
    ///
    /// Returns only the rows for the single partition identified by the raw
    /// `partition_key` bytes, after pruning the SSTable set down to those whose
    /// bloom filter / BTI trie admit the key — so unrelated SSTables are never
    /// parsed. Output matches filtering the full [`scan`](Self::scan) result to the
    /// partition. Delegates to [`SSTableManager::scan_partition`] (which has a
    /// bloom-prune implementation for the default build and a scan-and-filter
    /// fallback for the `tombstones` build, so callers need no cfg branching).
    ///
    /// Returns `(rows, engaged)`: `engaged` is `true` only when the call actually
    /// pruned the SSTable set to partition candidates (the default build). The
    /// `tombstones` build returns `false` because it full-scans and retains with no
    /// prune, so the caller reports an honest fallback access path (Epic #951).
    pub async fn scan_partition(
        &self,
        table_id: &TableId,
        partition_key: &[u8],
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<(Vec<(RowKey, ScanRow)>, bool)> {
        self.sstables
            .scan_partition(table_id, partition_key, schema)
            .await
    }

    /// Clustering-slice-aware partition-targeted scan (Issue #954, Epic #951).
    ///
    /// Like [`scan_partition`](Self::scan_partition) but pushes a single-column
    /// clustering-key restriction (`ck </>/= ?` / two-bound range) down to a
    /// within-partition seek when the candidate's authoritative row index supports
    /// it, so a wide-partition slice decodes O(matched rows + index) rather than
    /// the whole partition. Returns `(rows, clustering_seek_engaged)`: the rows are
    /// the full partition (or a clustering-narrowed superset) so the caller's
    /// post-scan filter yields byte-identical output, and the bool reports whether
    /// the clustering narrowing actually engaged (for the `ClusteringSlice` access
    /// path). Delegates to [`SSTableManager::scan_partition_clustering`].
    #[cfg(not(feature = "tombstones"))]
    pub async fn scan_partition_clustering(
        &self,
        table_id: &TableId,
        partition_key: &[u8],
        clustering: Option<&crate::storage::sstable::reader::ClusteringSlice>,
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<(Vec<(RowKey, ScanRow)>, bool)> {
        self.sstables
            .scan_partition_clustering(table_id, partition_key, clustering, schema)
            .await
    }

    /// Reverse single-partition clustering scan for a BIG (`nb`) wide partition
    /// (Issue #1184). Returns `Ok(Some(rows))` in DESCENDING clustering order when
    /// the BIG promoted-index reverse iterator applied, or `Ok(None)` to tell the
    /// caller to keep the in-memory `ORDER BY DESC` sort (small / BTI /
    /// multi-generation cases). Delegates to
    /// [`SSTableManager::scan_partition_clustering_reverse`].
    #[cfg(not(feature = "tombstones"))]
    pub async fn scan_partition_clustering_reverse(
        &self,
        table_id: &TableId,
        partition_key: &[u8],
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<Option<Vec<(RowKey, ScanRow)>>> {
        self.sstables
            .scan_partition_clustering_reverse(table_id, partition_key, schema)
            .await
    }

    /// Partition-targeted, metadata-carrying scan for a fully-constrained
    /// `WHERE pk = ?` WRITETIME/TTL projection (Issue #962).
    ///
    /// The metadata sibling of [`scan_partition`](Self::scan_partition): returns
    /// only the rows for the single partition identified by the raw
    /// `partition_key` bytes, WITH per-cell write metadata, after pruning the
    /// SSTable set down to the candidates whose bloom filter / BTI trie admit the
    /// key — so a `SELECT WRITETIME(col) ... WHERE pk = ?` never opens all N
    /// SSTables. Output matches filtering the full
    /// [`scan_with_cell_metadata`](Self::scan_with_cell_metadata) result to the
    /// partition; cross-generation reconciliation runs over the pruned candidates.
    /// Delegates to [`SSTableManager::scan_partition_with_cell_metadata`].
    ///
    /// Returns `(rows, engaged)`: `engaged` is `true` only when the call pruned the
    /// SSTable set to partition candidates (the default build). The `tombstones`
    /// build returns `false` because it full-scans with metadata and retains with no
    /// prune, so the caller reports an honest fallback access path (Epic #951).
    pub async fn scan_partition_with_cell_metadata(
        &self,
        table_id: &TableId,
        partition_key: &[u8],
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<(
        Vec<(
            RowKey,
            ScanRow,
            std::collections::HashMap<String, CellWriteMetadata>,
        )>,
        bool,
    )> {
        self.sstables
            .scan_partition_with_cell_metadata(table_id, partition_key, schema)
            .await
    }

    /// Scan a table and return per-cell write metadata alongside row values.
    ///
    /// Delegates to [`SSTableManager::scan_with_cell_metadata`].  Used when
    /// `ProjectionFlags::include_cell_metadata` is set (issue #693).
    pub async fn scan_with_cell_metadata(
        &self,
        table_id: &TableId,
        start_key: Option<&RowKey>,
        end_key: Option<&RowKey>,
        limit: Option<usize>,
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<
        Vec<(
            RowKey,
            ScanRow,
            std::collections::HashMap<String, CellWriteMetadata>,
        )>,
    > {
        self.sstables
            .scan_with_cell_metadata(table_id, start_key, end_key, limit, schema)
            .await
    }

    /// Streaming scan (issue #790): return a bounded channel that yields
    /// `(RowKey, ScanRow)` entries lazily in key (token) order, instead of the
    /// materializing [`scan`](Self::scan) that returns the whole `Vec`.
    ///
    /// Live heap is bounded by `buffer_size` rows rather than growing O(rows),
    /// so streaming a large `SELECT *` no longer holds the entire result set in
    /// memory at once. Delegates to [`SSTableManager::scan_stream`].
    ///
    /// [`SSTableManager::scan_stream`]: sstable::SSTableManager::scan_stream
    pub async fn scan_stream(
        &self,
        table_id: &TableId,
        start_key: Option<&RowKey>,
        end_key: Option<&RowKey>,
        schema: Option<&crate::schema::TableSchema>,
        buffer_size: usize,
    ) -> Result<tokio::sync::mpsc::Receiver<Result<(RowKey, ScanRow)>>> {
        self.sstables
            .scan_stream(table_id, start_key, end_key, schema, buffer_size)
            .await
    }

    /// Flush MemTable to SSTable
    ///
    /// NOTE: Write functionality removed in Issue #175 (WAL/MemTable infrastructure deleted).
    /// This method is feature-gated behind 'experimental' but currently unimplemented.
    #[allow(dead_code)]
    #[cfg(feature = "experimental")]
    async fn flush_memtable(&self) -> Result<()> {
        Err(crate::error::Error::UnsupportedFormat(
            "Write operations (flush_memtable) removed in Issue #175 - WAL and MemTable infrastructure deleted".to_string()
        ))
    }

    /// Force flush all pending writes
    ///
    /// NOTE: Write functionality removed in Issue #175 (WAL/MemTable infrastructure deleted).
    /// This method is feature-gated behind 'experimental' but currently unimplemented.
    #[cfg(feature = "experimental")]
    pub async fn flush(&self) -> Result<()> {
        Err(crate::error::Error::UnsupportedFormat(
            "Write operations (flush) removed in Issue #175 - WAL and MemTable infrastructure deleted".to_string()
        ))
    }

    /// Perform manual compaction
    #[cfg(feature = "experimental")]
    pub async fn compact(&self) -> Result<()> {
        // TODO: Implement proper compaction logic
        // This would need to identify candidates and call CompactionManager::run_compaction
        Ok(())
    }

    /// Get storage statistics
    ///
    /// NOTE: Issue #176 removed compaction stats (compaction.rs deleted).
    pub async fn stats(&self) -> Result<StorageStats> {
        let sstable_stats = self.sstables.stats().await?;

        Ok(StorageStats {
            sstables: sstable_stats,
        })
    }

    /// Batch write operations for better performance
    ///
    /// NOTE: Write functionality removed in Issue #175 (WAL/MemTable infrastructure deleted).
    /// This method is feature-gated behind 'experimental' but currently unimplemented.
    #[cfg(feature = "experimental")]
    pub async fn batch_write(&mut self, _operations: Vec<BatchOperation>) -> Result<()> {
        Err(crate::error::Error::UnsupportedFormat(
            "Write operations (batch_write) removed in Issue #175 - WAL and MemTable infrastructure deleted".to_string()
        ))
    }

    /// Explicit batch flush
    ///
    /// NOTE: Write functionality removed in Issue #175 (WAL/MemTable infrastructure deleted).
    /// This method is feature-gated behind 'experimental' but currently unimplemented.
    #[cfg(feature = "experimental")]
    pub async fn flush_batch(&mut self) -> Result<()> {
        Err(crate::error::Error::UnsupportedFormat(
            "Write operations (flush_batch) removed in Issue #175 - WAL and MemTable infrastructure deleted".to_string()
        ))
    }

    /// Get batch writer statistics
    ///
    /// NOTE: Write functionality removed in Issue #175 (WAL/MemTable infrastructure deleted).
    /// This method is feature-gated behind 'experimental' but currently unimplemented.
    #[cfg(feature = "experimental")]
    pub fn batch_stats(&self) -> Option<()> {
        None
    }

    /// Shutdown the storage engine
    ///
    /// NOTE: Issue #176 removed compaction shutdown (compaction.rs deleted).
    /// Issue #175 removed flush operations (WAL/MemTable deleted).
    pub async fn shutdown(&self) -> Result<()> {
        // Nothing to shutdown - read-only storage layer
        Ok(())
    }

    /// Set the schema registry for schema-aware operations
    ///
    /// This method propagates the schema registry to the SSTable manager,
    /// which will apply it to all SSTable readers for schema-aware parsing.
    #[cfg(feature = "state_machine")]
    pub async fn set_schema_registry(
        &self,
        registry: Arc<RwLock<crate::schema::SchemaRegistry>>,
    ) -> Result<()> {
        // Store in our field
        {
            let mut schema_reg = self.schema_registry.write().await;
            *schema_reg = Some(registry.clone());
        }

        // Propagate to SSTable manager
        self.sstables.set_schema_registry(registry).await
    }
}

/// Batch operation types
#[cfg(feature = "experimental")]
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
///
/// NOTE: Issue #176 removed compaction statistics (compaction.rs deleted).
#[derive(Debug, Clone)]
pub struct StorageStats {
    /// SSTable statistics
    pub sstables: sstable::SSTableStats,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_storage_engine_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::test_config();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        let storage = StorageEngine::open(
            temp_dir.path(),
            &config,
            platform,
            #[cfg(feature = "state_machine")]
            None,
        )
        .await
        .unwrap();
        let stats = storage.stats().await.unwrap();

        assert_eq!(stats.sstables.sstable_count, 0);
        storage.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_storage_engine_with_discovered_sstables() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::test_config();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        // Create an empty list of discovered SSTables for this test
        let discovered_paths = Vec::new();

        let storage = StorageEngine::open_with_sstables(
            temp_dir.path(),
            discovered_paths,
            &config,
            platform,
            #[cfg(feature = "state_machine")]
            None,
        )
        .await
        .unwrap();

        let stats = storage.stats().await.unwrap();

        // Should have 0 SSTables since we provided an empty list
        assert_eq!(stats.sstables.sstable_count, 0);
        storage.shutdown().await.unwrap();
    }

    #[tokio::test]
    #[cfg(all(feature = "legacy-heuristics", feature = "experimental"))]
    async fn test_batch_operations() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        let mut storage = StorageEngine::open(
            temp_dir.path(),
            &config,
            platform,
            #[cfg(feature = "state_machine")]
            None,
        )
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
        storage.shutdown().await.unwrap();
    }

    #[tokio::test]
    #[cfg(all(feature = "legacy-heuristics", feature = "experimental"))]
    async fn test_batch_operations_fallback() {
        // Add timeout to prevent hanging in parallel test execution
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let temp_dir = TempDir::new().unwrap();
            let mut config = Config::default();
            // Force fallback path by setting small threshold so batch writer is not initialized
            config.storage.memtable_size_threshold = 1024; // 1KB - smaller than 1MB threshold
            let platform = Arc::new(Platform::new(&config).await.unwrap());

            let mut storage = StorageEngine::open(
                temp_dir.path(),
                &config,
                platform,
                #[cfg(feature = "state_machine")]
                None,
            )
            .await
            .unwrap();

            // Test batch write operations (should use fallback path)
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
            storage.shutdown().await.unwrap();
        })
        .await
        .expect("Test should complete within 30 seconds");
    }
}
