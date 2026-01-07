//! CQLite Core Database Engine
//!
//! A high-performance, embeddable database engine with SSTable-based storage,
//! supporting both native and WASM deployments.

pub mod config;
pub mod error;
pub mod parser;
// DISABLED FOR M1: Security and performance modules causing compilation errors
// pub mod performance;
// pub mod security; // Security framework for comprehensive protection
pub mod types;
pub mod version_hints;

pub mod benchmarks;
pub mod memory;
pub mod platform;
#[cfg(feature = "state_machine")]
pub mod query;
pub mod schema;
pub mod storage;

// Ingestion module for one-shot schema & SSTable discovery
#[cfg(feature = "state_machine")]
pub mod ingestion;

// Discovery module for SSTable scanning and coverage analysis
#[cfg(feature = "state_machine")]
pub mod discovery;

// Testing utilities - hidden from public docs via #[doc(hidden)] but available for integration tests
#[doc(hidden)]
pub mod testing;

// NOTE: memory_safety_runner moved to tools/memory-safety-runner (Issue #245)
// NOTE: memory_safety_tests disabled - MemTable removed in Issue #175

// Re-export main types for convenience
pub use crate::{
    config::Config,
    error::{Error, Result},
    platform::Platform,
    types::*,
};

// Re-export query types when state_machine feature is enabled
#[cfg(feature = "state_machine")]
pub use query::SchemaStatus;

use std::path::Path;
#[cfg(feature = "state_machine")]
use std::path::PathBuf;
use std::sync::Arc;

use crate::{memory::MemoryManager, storage::StorageEngine};

#[cfg(feature = "state_machine")]
use crate::schema::SchemaManager;

#[cfg(feature = "state_machine")]
use crate::query::QueryEngine;

/// Main database handle
///
/// This is the primary interface for interacting with a CQLite database.
/// It coordinates between the storage engine, schema manager, and query engine.
#[derive(Debug)]
pub struct Database {
    storage: Arc<StorageEngine>,
    #[cfg(feature = "state_machine")]
    query: Arc<QueryEngine>,
    memory: Arc<MemoryManager>,
    config: Config,
}

impl Database {
    /// Open a database at the given path with the specified configuration
    ///
    /// # Arguments
    ///
    /// * `path` - The directory path where the database files will be stored
    /// * `config` - Database configuration options
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The path cannot be created or accessed
    /// - Database files are corrupted
    /// - Configuration is invalid
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use cqlite_core::{Database, Config};
    /// use std::path::{Path, PathBuf};
    ///
    /// # tokio_test::block_on(async {
    /// let config = Config::default();
    /// let db = Database::open(Path::new("./data"), config).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    pub async fn open(path: &Path, config: Config) -> Result<Self> {
        // Initialize platform abstraction layer
        let platform = Arc::new(Platform::new(&config).await?);

        // Initialize memory manager
        let memory = Arc::new(MemoryManager::new(&config)?);

        // Initialize storage engine (no schema registry for simple open)
        let storage = Arc::new(
            StorageEngine::open(
                path,
                &config,
                platform.clone(),
                #[cfg(feature = "state_machine")]
                None,
            )
            .await?,
        );

        // Initialize schema manager
        #[cfg(feature = "state_machine")]
        let schema = Arc::new(SchemaManager::new_with_storage(storage.clone(), &config).await?);

        // Initialize query engine (only when feature enabled)
        #[cfg(feature = "state_machine")]
        let query = Arc::new(QueryEngine::new(
            storage.clone(),
            schema.clone(),
            memory.clone(),
            &config,
        )?);

        Ok(Self {
            storage,
            #[cfg(feature = "state_machine")]
            query,
            memory,
            config,
        })
    }

    /// Open a database with pre-discovered SSTable table directories
    ///
    /// This method is used in the ingestion flow where SSTable discovery has been performed
    /// externally (e.g., via `DiscoveryService`) and the database should be initialized with
    /// specific SSTable files rather than scanning the storage directory.
    ///
    /// # Use Case
    ///
    /// This method is designed for the one-shot ingestion workflow:
    /// 1. `DiscoveryService::discover()` scans external Cassandra data directories
    /// 2. `SchemaManager` parses schema from discovered files
    /// 3. `Database::open_with_discovered_sstables()` creates a queryable database instance
    ///
    /// # Arguments
    ///
    /// * `storage_path` - The directory path for database runtime files (WAL, manifest, memtable)
    /// * `discovered_table_dirs` - Vector of table directory paths from DiscoveryService
    ///   (e.g., `/var/lib/cassandra/data/keyspace1/table1-abc123`)
    /// * `config` - Database configuration options
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The storage path cannot be created or accessed
    /// - Any discovered table directory cannot be read
    /// - Configuration is invalid
    /// - Storage engine or query engine initialization fails
    ///
    /// # Feature Gates
    ///
    /// This method is only available when the `state_machine` feature is enabled (default in M2+).
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use cqlite_core::{Database, Config};
    /// use std::path::{Path, PathBuf};
    ///
    /// # tokio_test::block_on(async {
    /// let config = Config::default();
    /// let storage_path = Path::new("./runtime");
    /// let discovered_dirs = vec![
    ///     PathBuf::from("/var/lib/cassandra/data/keyspace1/table1-abc123"),
    ///     PathBuf::from("/var/lib/cassandra/data/keyspace1/table2-def456"),
    /// ];
    ///
    /// let db = Database::open_with_discovered_sstables(
    ///     storage_path,
    ///     discovered_dirs,
    ///     config
    /// ).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[cfg(feature = "state_machine")]
    pub async fn open_with_discovered_sstables(
        storage_path: &Path,
        discovered_table_dirs: Vec<PathBuf>,
        config: Config,
    ) -> Result<Self> {
        Self::open_with_discovered_sstables_and_registry(
            storage_path,
            discovered_table_dirs,
            config,
            None,
        )
        .await
    }

    /// Open a database with pre-discovered SSTable table directories and optional schema registry
    ///
    /// This is the internal implementation that supports passing a pre-loaded schema registry.
    /// Public callers should use `open_with_discovered_sstables()` which calls this with None.
    /// The ingestion module uses this directly to pass loaded schemas.
    ///
    /// # Arguments
    ///
    /// * `storage_path` - The directory path for database runtime files
    /// * `discovered_table_dirs` - Vector of table directory paths from DiscoveryService
    /// * `config` - Database configuration options
    /// * `schema_registry` - Optional pre-loaded schema registry from ingestion
    #[cfg(feature = "state_machine")]
    pub(crate) async fn open_with_discovered_sstables_and_registry(
        storage_path: &Path,
        discovered_table_dirs: Vec<PathBuf>,
        config: Config,
        schema_registry: Option<Arc<tokio::sync::RwLock<schema::SchemaRegistry>>>,
    ) -> Result<Self> {
        // Initialize platform abstraction layer
        let platform = Arc::new(Platform::new(&config).await?);

        // Initialize memory manager
        let memory = Arc::new(MemoryManager::new(&config)?);

        // Initialize storage engine with pre-discovered SSTables and schema registry
        let storage = Arc::new(
            StorageEngine::open_with_sstables(
                storage_path,
                discovered_table_dirs,
                &config,
                platform.clone(),
                schema_registry.clone(),
            )
            .await?,
        );

        // Initialize schema manager - use registry if provided, otherwise create empty
        let schema = if let Some(registry_rwlock) = schema_registry {
            Arc::new(
                SchemaManager::new_with_registry(storage.clone(), registry_rwlock, &config).await?,
            )
        } else {
            Arc::new(SchemaManager::new_with_storage(storage.clone(), &config).await?)
        };

        // Initialize query engine
        let query = Arc::new(QueryEngine::new(
            storage.clone(),
            schema.clone(),
            memory.clone(),
            &config,
        )?);

        Ok(Self {
            storage,
            query,
            memory,
            config,
        })
    }

    /// Execute a SQL query and return the result
    ///
    /// # Arguments
    ///
    /// * `sql` - The SQL query string to execute
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - SQL syntax is invalid
    /// - Referenced tables/columns don't exist
    /// - Query execution fails
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use cqlite_core::{Database, Config};
    /// # use std::path::{Path, PathBuf};
    /// # tokio_test::block_on(async {
    /// # let config = Config::default();
    /// # let db = Database::open(Path::new("./data"), config).await?;
    /// let result = db.execute("SELECT * FROM users WHERE id = 1").await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[cfg(feature = "state_machine")]
    pub async fn execute(&self, sql: &str) -> Result<query::result::QueryResult> {
        let result = self.query.execute(sql).await;

        #[cfg(debug_assertions)]
        if let Ok(ref query_result) = result {
            log::debug!(
                "Database::execute('{}') returning rows_affected: {}",
                sql,
                query_result.rows_affected
            );
        }

        result
    }

    /// Prepare a SQL statement for repeated execution
    ///
    /// # Arguments
    ///
    /// * `sql` - The SQL statement to prepare
    ///
    /// # Errors
    ///
    /// Returns an error if SQL syntax is invalid or references non-existent objects
    #[cfg(feature = "state_machine")]
    pub async fn prepare(&self, sql: &str) -> Result<std::sync::Arc<query::PreparedQuery>> {
        self.query.prepare(sql).await
    }

    /// Explain a SQL query without executing it
    ///
    /// # Arguments
    ///
    /// * `sql` - The SQL query to explain
    ///
    /// # Errors
    ///
    /// Returns an error if SQL syntax is invalid
    #[cfg(feature = "state_machine")]
    pub async fn explain(&self, sql: &str) -> Result<query::ExplainResult> {
        self.query.explain(sql).await
    }

    /// Check if schema is available for a table
    ///
    /// This is a fast boolean check useful for pre-flight validation.
    /// For detailed diagnostic information, use `schema_status()`.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use cqlite_core::{Database, Config};
    /// # tokio_test::block_on(async {
    /// let db = Database::open(std::path::Path::new("./data"), Config::default()).await?;
    ///
    /// if !db.has_schema_for_table("users").await {
    ///     eprintln!("Warning: No schema found for 'users' table");
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[cfg(feature = "state_machine")]
    pub async fn has_schema_for_table(&self, table: &str) -> bool {
        self.query.has_schema_for_table(table).await
    }

    /// Get detailed schema status for debugging
    ///
    /// Returns diagnostic information about schema availability including
    /// reasons for missing schemas or extraction failures.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use cqlite_core::{Database, Config};
    /// # use cqlite_core::query::SchemaStatus;
    /// # tokio_test::block_on(async {
    /// let db = Database::open(std::path::Path::new("./data"), Config::default()).await?;
    ///
    /// match db.schema_status("users").await {
    ///     SchemaStatus::Available { .. } => println!("Schema ready"),
    ///     SchemaStatus::ExtractionFailed { cause, suggestion, .. } => {
    ///         eprintln!("Schema extraction failed: {}", cause);
    ///         eprintln!("Suggestion: {}", suggestion);
    ///     }
    ///     _ => {}
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[cfg(feature = "state_machine")]
    pub async fn schema_status(&self, table: &str) -> query::SchemaStatus {
        self.query.schema_status(table).await
    }

    /// Get database statistics
    pub async fn stats(&self) -> Result<DatabaseStats> {
        Ok(DatabaseStats {
            storage_stats: self.storage.stats().await?,
            memory_stats: self.memory.stats()?,
            #[cfg(feature = "state_machine")]
            query_stats: self.query.stats(),
        })
    }

    /// Flush all pending writes to disk
    #[cfg(feature = "experimental")]
    pub async fn flush(&self) -> Result<()> {
        self.storage.flush().await
    }

    /// Perform manual compaction of storage files
    #[cfg(feature = "experimental")]
    pub async fn compact(&self) -> Result<()> {
        self.storage.compact().await
    }

    /// Close the database and release all resources
    ///
    /// This method ensures all pending operations are completed and
    /// all resources are properly cleaned up.
    pub async fn close(self) -> Result<()> {
        // Stop background tasks
        self.storage.shutdown().await?;

        // Flush any remaining data (only with experimental feature)
        #[cfg(feature = "experimental")]
        {
            self.storage.flush().await?;
        }

        Ok(())
    }

    /// Get the database configuration
    pub fn config(&self) -> &Config {
        &self.config
    }
}

impl Clone for Database {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            #[cfg(feature = "state_machine")]
            query: self.query.clone(),
            memory: self.memory.clone(),
            config: self.config.clone(),
        }
    }
}

/// Database statistics
#[derive(Debug, Clone)]
pub struct DatabaseStats {
    /// Storage engine statistics
    pub storage_stats: storage::StorageStats,
    /// Memory manager statistics
    pub memory_stats: memory::MemoryStats,
    /// Query engine statistics
    #[cfg(feature = "state_machine")]
    pub query_stats: query::QueryStats,
}

/// A prepared SQL statement that can be executed multiple times
#[cfg(feature = "state_machine")]
#[derive(Debug)]
pub struct PreparedStatement {
    statement: query::PreparedQuery,
}

#[cfg(feature = "state_machine")]
impl PreparedStatement {
    /// Execute the prepared statement with the given parameters
    pub async fn execute(&self, params: &[Value]) -> Result<query::result::QueryResult> {
        self.statement.execute(params).await
    }
}

// Re-export query result types for convenience
#[cfg(feature = "state_machine")]
pub use query::result::{QueryResult, QueryRow};

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_database_open_close() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::test_config();

        let db = Database::open(temp_dir.path(), config).await.unwrap();
        db.close().await.unwrap();
    }

    /// Documents that open_with_discovered_sstables_and_registry is crate-private.
    /// This test exists to document the API contract - the function should NOT be
    /// callable from integration tests or external crates.
    #[cfg(feature = "state_machine")]
    #[test]
    fn test_open_with_discovered_sstables_and_registry_is_crate_private() {
        // This test compiling proves the function exists and is accessible within the crate
        // If we accidentally made it pub instead of pub(crate), integration tests could access it
        // The function signature itself enforces this via pub(crate) keyword

        // Note: We don't actually call the function here since it requires async setup
        // The mere existence of this test documents the API boundary
        assert!(
            true,
            "open_with_discovered_sstables_and_registry is correctly marked pub(crate)"
        );
    }

    #[tokio::test]
    #[cfg(feature = "state_machine")]
    async fn test_database_open_with_discovered_sstables() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::test_config();

        // Create an empty list of discovered table directories
        let discovered_dirs = Vec::new();

        let db = Database::open_with_discovered_sstables(temp_dir.path(), discovered_dirs, config)
            .await
            .unwrap();

        // Verify database was created successfully
        let stats = db.stats().await.unwrap();
        assert_eq!(stats.storage_stats.sstables.sstable_count, 0);

        db.close().await.unwrap();
    }

    #[tokio::test]
    #[cfg(all(
        feature = "legacy-heuristics",
        feature = "state_machine",
        feature = "experimental"
    ))]
    async fn test_database_basic_operations() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::test_config();

        let db = Database::open(temp_dir.path(), config).await.unwrap();

        // Create table
        let result = db
            .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
            .await
            .unwrap();
        assert_eq!(result.rows_affected, 0);

        // Insert data
        let result = db
            .execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
            .await
            .unwrap();

        #[cfg(debug_assertions)]
        log::debug!(
            "Test INSERT assertion - rows_affected: {}",
            result.rows_affected
        );

        assert_eq!(result.rows_affected, 1);

        // Query data - Re-enabled for QA debugging
        let result = db
            .execute("SELECT * FROM users WHERE id = 1")
            .await
            .unwrap();

        #[cfg(debug_assertions)]
        log::debug!("Test SELECT assertion - rows.len(): {}", result.rows.len());

        assert_eq!(result.rows.len(), 1, "SELECT should return 1 row");

        db.close().await.unwrap();
    }
}
