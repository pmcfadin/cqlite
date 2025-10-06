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

pub mod benchmarks;
pub mod memory;
pub mod performance_monitor;
pub mod platform;
#[cfg(feature = "state_machine")]
pub mod query;
pub mod schema;
pub mod storage;

// Docker integration for testing
pub mod docker;
pub mod testing;

// Memory safety testing modules
#[cfg(test)]
pub mod memory_safety_runner;
#[cfg(test)]
pub mod memory_safety_tests;

// Re-export main types for convenience
pub use crate::{
    config::Config,
    error::{Error, Result},
    types::*,
};

use std::path::Path;
use std::sync::Arc;

use crate::{memory::MemoryManager, platform::Platform, storage::StorageEngine};

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
    /// use std::path::Path;
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

        // Initialize storage engine
        let storage = Arc::new(StorageEngine::open(path, &config, platform.clone()).await?);

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
    /// # use std::path::Path;
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
            eprintln!(
                "DEBUG: Database::execute('{}') returning rows_affected: {}",
                sql, query_result.rows_affected
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
    pub async fn flush(&self) -> Result<()> {
        self.storage.flush().await
    }

    /// Perform manual compaction of storage files
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

        // Flush any remaining data
        self.storage.flush().await?;

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

    #[tokio::test]
    #[cfg(all(feature = "legacy-heuristics", feature = "state_machine"))]
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
        eprintln!(
            "DEBUG: Test INSERT assertion - rows_affected: {}",
            result.rows_affected
        );

        assert_eq!(result.rows_affected, 1);

        // Query data - Re-enabled for QA debugging
        let result = db
            .execute("SELECT * FROM users WHERE id = 1")
            .await
            .unwrap();

        #[cfg(debug_assertions)]
        eprintln!(
            "DEBUG: Test SELECT assertion - rows.len(): {}",
            result.rows.len()
        );

        assert_eq!(result.rows.len(), 1, "SELECT should return 1 row");

        db.close().await.unwrap();
    }
}
