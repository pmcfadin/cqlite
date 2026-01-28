//! Write engine for SSTable generation and persistence (M5)
//!
//! This module provides the write path for CQLite, implementing WAL-backed
//! memtable flushing and K-way merge for producing valid Cassandra 5.0 SSTables.
//!
//! ## Architecture
//!
//! The WriteEngine is the public API that coordinates:
//! 1. WAL (Write-Ahead Log) - Durability
//! 2. Memtable - In-memory buffer
//! 3. SSTableWriter - On-disk persistence
//!
//! ## Write Flow
//!
//! 1. User calls `write(mutation)` or `execute(cql_statement)`
//! 2. WriteEngine appends to WAL (durability)
//! 3. WriteEngine inserts into Memtable
//! 4. When Memtable exceeds threshold → flush to SSTable
//! 5. After successful flush → truncate WAL
//!
//! ## Recovery
//!
//! On startup, the WriteEngine replays WAL entries into the memtable.

#[cfg(feature = "write-support")]
pub mod wal;
#[cfg(feature = "write-support")]
pub mod memtable;
#[cfg(feature = "write-support")]
pub mod mutation;
#[cfg(feature = "write-support")]
pub mod merge;

#[cfg(feature = "write-support")]
pub use wal::WriteAheadLog;
#[cfg(feature = "write-support")]
pub use memtable::Memtable;
#[cfg(feature = "write-support")]
pub use mutation::{
    CellOperation, ClusteringKey, DecoratedKey, Mutation, PartitionKey, TableId,
};
#[cfg(feature = "write-support")]
pub use merge::KWayMerger;

use crate::error::{Error, Result};
use crate::schema::TableSchema;
use crate::storage::sstable::writer::SSTableInfo;
use std::path::{Path, PathBuf};

/// Write engine configuration
#[cfg(feature = "write-support")]
#[derive(Debug, Clone)]
pub struct WriteEngineConfig {
    /// Directory for SSTable data files
    pub data_dir: PathBuf,
    /// Directory for WAL files
    pub wal_dir: PathBuf,
    /// Memtable flush threshold in bytes (default: 64MB)
    pub memtable_flush_threshold: usize,
    /// Table schema for column metadata
    pub schema: TableSchema,
}

#[cfg(feature = "write-support")]
impl WriteEngineConfig {
    /// Default flush threshold (64 MB)
    pub const DEFAULT_FLUSH_THRESHOLD: usize = 64 * 1024 * 1024;

    /// Create a new configuration with default flush threshold
    pub fn new(data_dir: PathBuf, wal_dir: PathBuf, schema: TableSchema) -> Self {
        Self {
            data_dir,
            wal_dir,
            memtable_flush_threshold: Self::DEFAULT_FLUSH_THRESHOLD,
            schema,
        }
    }

    /// Set a custom flush threshold
    pub fn with_flush_threshold(mut self, threshold: usize) -> Self {
        self.memtable_flush_threshold = threshold;
        self
    }
}

/// Write engine coordinator
///
/// Orchestrates WAL, memtable, and SSTable flushing for write operations.
/// This is the primary public API for all write operations in CQLite.
///
/// ## Thread Safety
///
/// WriteEngine follows a single-writer model. It is NOT thread-safe and
/// should be used from a single thread or protected by external locking.
///
/// ## Example
///
/// ```rust,ignore
/// use cqlite_core::storage::write_engine::{WriteEngine, WriteEngineConfig, Mutation};
/// use std::path::PathBuf;
///
/// // Create configuration
/// let config = WriteEngineConfig::new(
///     PathBuf::from("data"),
///     PathBuf::from("wal"),
///     schema
/// );
///
/// // Create engine
/// let mut engine = WriteEngine::new(config)?;
///
/// // Write a mutation
/// engine.write(mutation)?;
///
/// // Execute CQL statement
/// engine.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")?;
///
/// // Flush to SSTable
/// engine.flush()?;
///
/// // Close cleanly
/// engine.close()?;
/// ```
#[cfg(feature = "write-support")]
#[derive(Debug)]
pub struct WriteEngine {
    /// Configuration
    config: WriteEngineConfig,
    /// Write-ahead log for durability
    wal: WriteAheadLog,
    /// In-memory write buffer
    memtable: Memtable,
    /// SSTable generation counter (increments on each flush)
    generation: u32,
    /// Whether the engine has been closed
    closed: bool,
}

#[cfg(feature = "write-support")]
impl WriteEngine {
    /// Create a new write engine
    ///
    /// This initializes the WAL and memtable. If a WAL exists in the
    /// wal_dir, it will be replayed to recover in-flight writes.
    ///
    /// # Arguments
    ///
    /// * `config` - Write engine configuration
    ///
    /// # Returns
    ///
    /// A new WriteEngine ready to accept writes.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - WAL directory doesn't exist
    /// - Data directory doesn't exist
    /// - WAL replay fails
    pub fn new(config: WriteEngineConfig) -> Result<Self> {
        // Ensure directories exist
        std::fs::create_dir_all(&config.data_dir).map_err(|e| {
            Error::Storage(format!(
                "Failed to create data directory {:?}: {}",
                config.data_dir, e
            ))
        })?;

        std::fs::create_dir_all(&config.wal_dir).map_err(|e| {
            Error::Storage(format!(
                "Failed to create WAL directory {:?}: {}",
                config.wal_dir, e
            ))
        })?;

        // Initialize WAL
        let wal_path = config.wal_dir.join(WriteAheadLog::WAL_FILENAME);
        let wal = if wal_path.exists() {
            // Recover from existing WAL
            WriteAheadLog::open_existing(&wal_path)?
        } else {
            // Create new WAL
            WriteAheadLog::create(&config.wal_dir)?
        };

        // Replay WAL into memtable
        let mut memtable = Memtable::new();
        let mutations = wal.replay()?;

        if !mutations.is_empty() {
            log::info!("Replaying {} mutations from WAL", mutations.len());

            for mutation in mutations {
                // Compute decorated key
                let decorated_key = mutation.decorated_key(&config.schema)?;

                // Insert into memtable
                memtable.insert_with_key(decorated_key, mutation)?;
            }

            log::info!(
                "WAL replay complete: {} rows in memtable, {} bytes",
                memtable.row_count(),
                memtable.size_bytes()
            );
        }

        // Determine next generation number by scanning data directory
        let generation = Self::determine_next_generation(&config.data_dir)?;

        Ok(Self {
            config,
            wal,
            memtable,
            generation,
            closed: false,
        })
    }

    /// Write a mutation to the write engine
    ///
    /// This appends the mutation to the WAL for durability, then inserts it
    /// into the memtable. If the memtable exceeds the flush threshold,
    /// an automatic flush is triggered.
    ///
    /// **Note**: Automatic flush is disabled when called from an async context.
    /// Use `write_async()` for async contexts with automatic flush support.
    ///
    /// # Arguments
    ///
    /// * `mutation` - The mutation to write
    ///
    /// # Returns
    ///
    /// Ok(()) on success, or an error if the write fails.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Engine has been closed
    /// - WAL append fails
    /// - Memtable insert fails
    /// - Automatic flush fails (sync context only)
    pub fn write(&mut self, mutation: Mutation) -> Result<()> {
        if self.closed {
            return Err(Error::InvalidInput(
                "WriteEngine has been closed".to_string(),
            ));
        }

        // 1. Append to WAL (durability)
        self.wal.append(&mutation)?;
        self.wal.sync()?;

        // 2. Compute decorated key from partition key
        let decorated_key = mutation.decorated_key(&self.config.schema)?;

        // 3. Insert into memtable
        self.memtable.insert_with_key(decorated_key, mutation)?;

        // 4. Check if memtable should be flushed (only in non-async context)
        if self
            .memtable
            .should_flush(self.config.memtable_flush_threshold)
        {
            log::warn!(
                "Memtable size {} exceeds threshold {} - call flush() manually in async context",
                self.memtable.size_bytes(),
                self.config.memtable_flush_threshold
            );

            // Try to flush synchronously only if we're not in an async context
            if tokio::runtime::Handle::try_current().is_err() {
                log::info!("Triggering automatic flush");
                self.flush_internal()?;
            }
        }

        Ok(())
    }

    /// Write a mutation with async automatic flush support
    ///
    /// This is the async version of `write()` that supports automatic flushing
    /// in async contexts. Use this method when calling from async code.
    ///
    /// # Arguments
    ///
    /// * `mutation` - The mutation to write
    ///
    /// # Returns
    ///
    /// Ok(()) on success, or an error if the write fails.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Engine has been closed
    /// - WAL append fails
    /// - Memtable insert fails
    /// - Automatic flush fails
    pub async fn write_async(&mut self, mutation: Mutation) -> Result<()> {
        if self.closed {
            return Err(Error::InvalidInput(
                "WriteEngine has been closed".to_string(),
            ));
        }

        // 1. Append to WAL (durability)
        self.wal.append(&mutation)?;
        self.wal.sync()?;

        // 2. Compute decorated key from partition key
        let decorated_key = mutation.decorated_key(&self.config.schema)?;

        // 3. Insert into memtable
        self.memtable.insert_with_key(decorated_key, mutation)?;

        // 4. Check if memtable should be flushed
        if self
            .memtable
            .should_flush(self.config.memtable_flush_threshold)
        {
            log::info!(
                "Memtable size {} exceeds threshold {}, triggering flush",
                self.memtable.size_bytes(),
                self.config.memtable_flush_threshold
            );
            self.flush_internal_async().await?;
        }

        Ok(())
    }

    /// Execute a CQL statement (INSERT, UPDATE, DELETE)
    ///
    /// This parses the CQL statement and converts it to a mutation,
    /// then writes it using the `write()` method.
    ///
    /// # Arguments
    ///
    /// * `statement` - CQL statement string
    ///
    /// # Returns
    ///
    /// Ok(()) on success, or an error if parsing or writing fails.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - CQL parsing fails
    /// - Statement is not a mutation (INSERT/UPDATE/DELETE)
    /// - Mutation conversion fails
    /// - Write fails
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// engine.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")?;
    /// engine.execute("UPDATE users SET name = 'Bob' WHERE id = 1")?;
    /// engine.execute("DELETE FROM users WHERE id = 1")?;
    /// ```
    pub fn execute(&mut self, statement: &str) -> Result<()> {
        if self.closed {
            return Err(Error::InvalidInput(
                "WriteEngine has been closed".to_string(),
            ));
        }

        // Parse CQL statement
        let mutation = self.parse_cql_to_mutation(statement)?;

        // Write mutation
        self.write(mutation)
    }

    /// Force a flush of the memtable to SSTable
    ///
    /// This writes all data in the memtable to a new SSTable generation,
    /// then truncates the WAL. The memtable is cleared after a successful flush.
    ///
    /// # Returns
    ///
    /// Returns `Some(SSTableInfo)` if data was flushed, or `None` if the
    /// memtable was empty.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Engine has been closed
    /// - SSTable write fails
    /// - WAL truncate fails
    pub async fn flush(&mut self) -> Result<Option<SSTableInfo>> {
        if self.closed {
            return Err(Error::InvalidInput(
                "WriteEngine has been closed".to_string(),
            ));
        }

        self.flush_internal_async().await
    }

    /// Internal synchronous flush helper
    fn flush_internal(&mut self) -> Result<()> {
        // Try to use existing runtime, or create a new one if none exists
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                // We're inside a runtime, use it
                handle.block_on(self.flush_internal_async())?;
            }
            Err(_) => {
                // No runtime, create one
                let rt = tokio::runtime::Runtime::new()
                    .map_err(|e| Error::Storage(format!("Failed to create tokio runtime: {}", e)))?;
                rt.block_on(self.flush_internal_async())?;
            }
        }
        Ok(())
    }

    /// Internal async flush implementation
    async fn flush_internal_async(&mut self) -> Result<Option<SSTableInfo>> {
        // Check if memtable is empty
        if self.memtable.is_empty() {
            return Ok(None);
        }

        log::info!(
            "Flushing memtable: {} partitions, {} rows, {} bytes",
            self.memtable.iter().count(),
            self.memtable.row_count(),
            self.memtable.size_bytes()
        );

        // Create SSTable writer
        let mut writer = crate::storage::sstable::writer::SSTableWriter::new(
            self.config.data_dir.clone(),
            self.generation,
            &self.config.schema,
        )?;

        // Write all partitions from memtable (already in token order)
        for (decorated_key, mutations) in self.memtable.iter() {
            writer.write_partition(decorated_key.clone(), mutations.to_vec())?;
        }

        // Finalize SSTable
        let info = writer.finish().await?;

        log::info!(
            "SSTable flush complete: generation {}, {} partitions, {} bytes",
            self.generation,
            info.partition_count,
            info.data_size
        );

        // Truncate WAL (data now persisted to SSTable)
        self.wal.truncate()?;

        // Clear memtable
        self.memtable.clear();

        // Increment generation for next flush
        self.generation += 1;

        Ok(Some(info))
    }

    /// Close the write engine
    ///
    /// This flushes any remaining data in the memtable to SSTable,
    /// then closes the WAL. After calling close(), the engine
    /// cannot be used for further writes.
    ///
    /// # Returns
    ///
    /// Ok(()) on success.
    ///
    /// # Errors
    ///
    /// Returns an error if the final flush fails.
    pub async fn close(mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }

        log::info!("Closing WriteEngine");

        // Flush any remaining data
        if !self.memtable.is_empty() {
            log::info!("Flushing memtable before close");
            self.flush_internal_async().await?;
        }

        self.closed = true;
        log::info!("WriteEngine closed");

        Ok(())
    }

    /// Get the current memtable size in bytes
    pub fn memtable_size(&self) -> usize {
        self.memtable.size_bytes()
    }

    /// Get the current memtable row count
    pub fn memtable_row_count(&self) -> usize {
        self.memtable.row_count()
    }

    /// Get the current WAL size in bytes
    pub fn wal_size(&self) -> u64 {
        self.wal.size()
    }

    /// Get the current generation number
    pub fn generation(&self) -> u32 {
        self.generation
    }

    /// Parse a CQL statement to a Mutation
    ///
    /// This is a placeholder for full CQL parsing integration.
    /// For M5, we'll support basic INSERT statements.
    fn parse_cql_to_mutation(&self, statement: &str) -> Result<Mutation> {
        // TODO: Full CQL parser integration in M5.0-8
        // For now, return error to indicate not implemented
        Err(Error::InvalidInput(format!(
            "CQL parsing not yet implemented: {}",
            statement
        )))
    }

    /// Determine the next SSTable generation number
    ///
    /// Scans the data directory for existing SSTable files and returns
    /// the next generation number.
    fn determine_next_generation(data_dir: &Path) -> Result<u32> {
        let mut max_generation = 0u32;

        if !data_dir.exists() {
            return Ok(1);
        }

        // Scan directory for SSTable files
        for entry in std::fs::read_dir(data_dir).map_err(|e| {
            Error::Storage(format!("Failed to read data directory: {}", e))
        })? {
            let entry = entry.map_err(|e| {
                Error::Storage(format!("Failed to read directory entry: {}", e))
            })?;

            let filename = entry.file_name();
            let filename_str = filename.to_string_lossy();

            // Parse generation from filename: nb-{generation}-big-{Component}.db
            if filename_str.starts_with("nb-") && filename_str.contains("-big-") {
                if let Some(gen_str) = filename_str
                    .strip_prefix("nb-")
                    .and_then(|s| s.split('-').next())
                {
                    if let Ok(gen) = gen_str.parse::<u32>() {
                        max_generation = max_generation.max(gen);
                    }
                }
            }
        }

        Ok(max_generation + 1)
    }
}

#[cfg(all(test, feature = "write-support"))]
mod tests {
    use super::*;
    use crate::storage::write_engine::mutation::{CellOperation, PartitionKey, TableId};
    use crate::schema::{Column, KeyColumn};
    use crate::types::Value;
    use std::collections::HashMap;
    use tempfile::TempDir;

    fn create_test_schema() -> TableSchema {
        TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
        }
    }

    fn create_test_mutation(id: i32, name: &str, timestamp: i64) -> Mutation {
        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(id));
        let ops = vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text(name.to_string()),
        }];

        Mutation::new(table_id, pk, None, ops, timestamp, None)
    }

    #[test]
    fn test_write_engine_config() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        assert_eq!(
            config.memtable_flush_threshold,
            WriteEngineConfig::DEFAULT_FLUSH_THRESHOLD
        );

        let config = config.with_flush_threshold(128 * 1024 * 1024);
        assert_eq!(config.memtable_flush_threshold, 128 * 1024 * 1024);
    }

    #[test]
    fn test_write_engine_new() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let engine = WriteEngine::new(config).unwrap();

        assert_eq!(engine.generation(), 1);
        assert_eq!(engine.memtable_size(), 0);
        assert_eq!(engine.memtable_row_count(), 0);
        assert!(!engine.closed);
    }

    #[test]
    fn test_write_engine_write_single_mutation() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let mut engine = WriteEngine::new(config).unwrap();

        let mutation = create_test_mutation(1, "Alice", 1000000);
        engine.write(mutation).unwrap();

        assert_eq!(engine.memtable_row_count(), 1);
        assert!(engine.memtable_size() > 0);
        assert!(engine.wal_size() > 0);
    }

    #[test]
    fn test_write_engine_write_multiple_mutations() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let mut engine = WriteEngine::new(config).unwrap();

        // Write 10 mutations
        for i in 0..10 {
            let mutation = create_test_mutation(i, &format!("User{}", i), 1000000 + i as i64);
            engine.write(mutation).unwrap();
        }

        assert_eq!(engine.memtable_row_count(), 10);
        assert!(engine.memtable_size() > 0);
    }

    #[tokio::test]
    async fn test_write_engine_flush_empty() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let mut engine = WriteEngine::new(config).unwrap();

        // Flush empty memtable
        let result = engine.flush().await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_write_engine_flush_with_data() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let mut engine = WriteEngine::new(config).unwrap();

        // Write mutations
        for i in 0..5 {
            let mutation = create_test_mutation(i, &format!("User{}", i), 1000000 + i as i64);
            engine.write(mutation).unwrap();
        }

        let initial_generation = engine.generation();

        // Flush
        let info = engine.flush().await.unwrap();
        assert!(info.is_some());

        let info = info.unwrap();
        assert_eq!(info.partition_count, 5);
        assert!(info.data_size > 0);
        assert!(info.data_path.exists());

        // Memtable should be empty after flush
        assert_eq!(engine.memtable_row_count(), 0);
        assert_eq!(engine.memtable_size(), 0);

        // WAL should be truncated
        assert_eq!(engine.wal_size(), 0);

        // Generation should increment
        assert_eq!(engine.generation(), initial_generation + 1);
    }

    #[test]
    fn test_write_engine_automatic_flush() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        // Set very low flush threshold (1KB)
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        )
        .with_flush_threshold(1024);

        let mut engine = WriteEngine::new(config).unwrap();

        // Write enough mutations to trigger automatic flush
        for i in 0..100 {
            let mutation = create_test_mutation(i, &format!("User{}", i), 1000000 + i as i64);
            engine.write(mutation).unwrap();
        }

        // Should have automatically flushed
        // Memtable may have some data if writes continued after flush
        // But generation should have incremented
        assert!(engine.generation() > 1 || engine.memtable_size() < 10000);
    }

    #[tokio::test]
    async fn test_write_engine_close_with_data() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let mut engine = WriteEngine::new(config).unwrap();

        // Write mutations
        for i in 0..5 {
            let mutation = create_test_mutation(i, &format!("User{}", i), 1000000 + i as i64);
            engine.write(mutation).unwrap();
        }

        // Close should flush
        engine.close().await.unwrap();

        // Verify SSTable was created
        let data_dir = temp_dir.path().join("data");
        let entries: Vec<_> = std::fs::read_dir(&data_dir).unwrap().collect();
        assert!(entries.len() > 0, "SSTable files should exist");
    }

    #[tokio::test]
    async fn test_write_engine_close_empty() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let engine = WriteEngine::new(config).unwrap();

        // Close empty engine
        engine.close().await.unwrap();
    }

    #[test]
    fn test_write_engine_write_after_close() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let engine = WriteEngine::new(config).unwrap();

        // Close
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(engine.close())
            .unwrap();

        // Create new engine with same config (simulating restart)
        let schema2 = create_test_schema();
        let config2 = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema2,
        );

        let mut engine2 = WriteEngine::new(config2).unwrap();

        // Write should fail on closed engine (if we still had reference)
        // But new engine should work
        let mutation = create_test_mutation(1, "Alice", 1000000);
        engine2.write(mutation).unwrap();
        assert_eq!(engine2.memtable_row_count(), 1);
    }

    #[test]
    fn test_write_engine_wal_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema.clone(),
        );

        // Write mutations and close without flushing
        {
            let mut engine = WriteEngine::new(config.clone()).unwrap();

            for i in 0..5 {
                let mutation = create_test_mutation(i, &format!("User{}", i), 1000000 + i as i64);
                engine.write(mutation).unwrap();
            }

            // Don't flush - just drop engine (simulating crash)
        }

        // Create new engine - should recover from WAL
        let config2 = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let engine = WriteEngine::new(config2).unwrap();

        // Should have recovered 5 mutations
        assert_eq!(engine.memtable_row_count(), 5);
        assert!(engine.memtable_size() > 0);
    }

    #[test]
    fn test_write_engine_generation_tracking() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema.clone(),
        );

        // First engine
        {
            let mut engine = WriteEngine::new(config.clone()).unwrap();
            assert_eq!(engine.generation(), 1);

            // Write and flush
            let mutation = create_test_mutation(1, "Alice", 1000000);
            engine.write(mutation).unwrap();

            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(engine.flush())
                .unwrap();

            assert_eq!(engine.generation(), 2);
        }

        // Second engine - should detect existing generation
        let config2 = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let engine = WriteEngine::new(config2).unwrap();
        assert_eq!(engine.generation(), 2);
    }

    #[test]
    fn test_write_engine_execute_not_implemented() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let mut engine = WriteEngine::new(config).unwrap();

        // CQL parsing not yet implemented
        let result = engine.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')");
        assert!(result.is_err());
    }

    #[test]
    fn test_determine_next_generation_empty_dir() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();

        let generation = WriteEngine::determine_next_generation(&data_dir).unwrap();
        assert_eq!(generation, 1);
    }

    #[test]
    fn test_determine_next_generation_with_sstables() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();

        // Create dummy SSTable files
        std::fs::write(data_dir.join("nb-1-big-Data.db"), b"").unwrap();
        std::fs::write(data_dir.join("nb-2-big-Data.db"), b"").unwrap();
        std::fs::write(data_dir.join("nb-5-big-Data.db"), b"").unwrap();

        let generation = WriteEngine::determine_next_generation(&data_dir).unwrap();
        assert_eq!(generation, 6);
    }
}
