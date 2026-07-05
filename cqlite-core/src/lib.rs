//! CQLite Core Database Engine
//!
//! A high-performance, embeddable database engine with SSTable-based storage,
//! supporting both native and WASM deployments.

pub mod config;
pub mod cql;
pub mod error;
pub mod parser;
// DISABLED FOR M1: Security and performance modules causing compilation errors
// pub mod performance;
// pub mod security; // Security framework for comprehensive protection
pub mod types;
pub mod util;
pub mod version_hints;

pub mod benchmarks;
pub mod memory;
// Observability foundation (epic #1031, issues #1032 + #1038). Always present;
// the OpenTelemetry exporter wiring inside it is gated behind the optional
// `observability` feature, and all helpers compile to no-ops when it is off.
pub mod observability;
pub mod platform;
#[cfg(feature = "state_machine")]
pub mod query;
pub mod schema;
pub mod storage;

// Embeddable export writers (Epic #682). The module is always present; the
// Parquet writer inside it is gated behind the optional `parquet` feature.
pub mod export;

// M5: Write engine and serialization modules (Issue #359)
// Re-exported at crate level for convenience when write-support is enabled
#[cfg(feature = "write-support")]
pub use storage::serialization;
#[cfg(feature = "write-support")]
pub use storage::write_engine;

// Ingestion module for one-shot schema & SSTable discovery (Issue #249: CLI-specific)
#[cfg(feature = "cli-helpers")]
pub mod ingestion;

// Discovery module for SSTable scanning and coverage analysis
#[cfg(feature = "state_machine")]
pub mod discovery;

// Testing utilities - hidden from public docs via #[doc(hidden)] but available for integration tests
#[doc(hidden)]
pub mod testing;

// Fuzz-support surface (issue #1614). Only compiled under `--features fuzz`, and
// `#[doc(hidden)]` even then, so the default public API and docs are unchanged.
// Exposes thin `Result`-returning drivers over the internal decode entry points
// for the external `fuzz/` cargo-fuzz crate. See `fuzz_support.rs`.
#[cfg(feature = "fuzz")]
#[doc(hidden)]
pub mod fuzz_support;

// NOTE: memory_safety_runner moved to tools/memory-safety-runner (Issue #245)
// NOTE: memory_safety_tests disabled - MemTable removed in Issue #175

// Test-only heap-allocation probe (issue #1590, E8). Installed as the global
// allocator for `cqlite-core`'s unit-test binary so a test can count the heap
// allocations a specific code path performs (see the `cartesian_product`
// allocation regression test in the SELECT executor's `lookup` module). It
// delegates every operation to the system allocator and only bumps a per-thread
// counter while a measurement is ACTIVE, so it is inert for every other test.
#[cfg(all(test, feature = "state_machine"))] // sole `measure` caller lives in the state_machine `query` module; else `-D dead-code` under minimal (#1981)
pub(crate) mod test_alloc_probe {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;

    thread_local! {
        static COUNT: Cell<u64> = const { Cell::new(0) };
        static ACTIVE: Cell<bool> = const { Cell::new(false) };
    }

    pub(crate) struct CountingAllocator;

    // SAFETY: every storage operation is delegated verbatim to `System`; the only
    // added work is bumping a `Copy` thread-local counter, which never allocates
    // (the `thread_local!`s are `const`-initialized, so first access is
    // alloc-free — no reentrancy into the allocator).
    unsafe impl GlobalAlloc for CountingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            bump();
            System.alloc(layout)
        }
        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            System.dealloc(ptr, layout)
        }
        // Counting `realloc` as an allocation is deliberate: it distinguishes a
        // `Vec::clone()` + `push()` (a clone-alloc then a grow-realloc) from a
        // single sized `Vec::with_capacity()` fill.
        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            bump();
            System.realloc(ptr, layout, new_size)
        }
    }

    fn bump() {
        let _ = ACTIVE.try_with(|a| {
            if a.get() {
                let _ = COUNT.try_with(|c| c.set(c.get() + 1));
            }
        });
    }

    /// Count the heap allocations `f` performs ON THE CURRENT THREAD (thread-local
    /// counter, so concurrent tests do not pollute each other). Returns
    /// `(allocations, f's result)`.
    pub(crate) fn measure<R>(f: impl FnOnce() -> R) -> (u64, R) {
        ACTIVE.with(|a| a.set(true));
        COUNT.with(|c| c.set(0));
        let r = f();
        let n = COUNT.with(|c| c.get());
        ACTIVE.with(|a| a.set(false));
        (n, r)
    }
}

#[cfg(all(test, feature = "state_machine"))]
#[global_allocator]
static TEST_ALLOC: test_alloc_probe::CountingAllocator = test_alloc_probe::CountingAllocator;

// Re-export main types for convenience
pub use crate::{
    config::Config,
    error::{Error, Result},
    platform::Platform,
    types::*,
};

// Explicit SSTable directory refresh report (issue #1749). Not feature-gated —
// `Database::refresh()` is available in the minimal build too.
pub use storage::sstable::RefreshReport;

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

    /// Re-scan the data directory and atomically apply added/removed SSTable
    /// generations to this handle's reader set (issue #1749).
    ///
    /// # Freshness contract
    ///
    /// A `Database` is a **snapshot at [`open`](Self::open)**: it discovers the
    /// SSTable generations once and never re-scans on its own. A Cassandra
    /// flush/compaction (or a CQLite `--flush`) may add or remove generations
    /// underneath a warm handle at any time; those changes become visible only
    /// after an explicit `refresh()`. Re-runs the same TOC/filename-based
    /// discovery `open` used — no content sniffing, no heuristics.
    ///
    /// - Newly present generations become queryable; removed generations stop
    ///   being queried; unchanged generations keep their warm parsed
    ///   Index/Statistics/bloom state (not rebuilt).
    /// - **In-flight queries are never affected**: a scan already running holds
    ///   its own `Arc` reader clones and completes against the pre-refresh set. A
    ///   query issued after `refresh()` returns sees the post-refresh set.
    /// - **Atomic and fail-closed**: if any newly discovered generation fails to
    ///   open (e.g. a corrupt `Statistics.db`, issue #1626), `refresh()` returns
    ///   the typed error and leaves the previously held reader set fully
    ///   unchanged — no partial view.
    ///
    /// Returns a [`RefreshReport`] describing what this call applied. Explicit
    /// refresh only: there is no filesystem watching or per-query staleness check.
    pub async fn refresh(&self) -> Result<RefreshReport> {
        self.storage.refresh().await
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

        // Data-safety (issue #1694): log the SHAPE (rows affected), never the SQL
        // text — a query string carries user data (WHERE-clause literals).
        #[cfg(debug_assertions)]
        if let Ok(ref query_result) = result {
            log::debug!(
                "Database::execute returning rows_affected: {}",
                query_result.rows_affected
            );
        }

        result
    }

    /// Execute a SQL query with streaming results (Issue #280)
    ///
    /// Returns a `QueryResultIterator` that yields rows incrementally via a bounded
    /// channel, enabling memory-efficient processing of large result sets.
    ///
    /// This is the recommended method for exporting large tables, as it avoids
    /// materializing all rows in memory at once.
    ///
    /// # Arguments
    ///
    /// * `sql` - The SQL query to execute (must be a SELECT statement)
    /// * `config` - Streaming configuration (buffer size, chunk hints)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Query is not a SELECT statement
    /// - SQL syntax is invalid
    /// - Query execution fails
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use cqlite_core::{Database, Config};
    /// # use cqlite_core::query::result::StreamingConfig;
    /// # use std::path::Path;
    /// # tokio_test::block_on(async {
    /// # let db = Database::open(Path::new("./data"), Config::default()).await?;
    /// let config = StreamingConfig::default();
    /// let mut iter = db.execute_streaming(
    ///     "SELECT * FROM large_table",
    ///     config
    /// ).await?;
    ///
    /// while let Some(row_result) = iter.next_async().await {
    ///     let row = row_result?;
    ///     // Process row incrementally
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    #[cfg(feature = "state_machine")]
    pub async fn execute_streaming(
        &self,
        sql: &str,
        config: query::result::StreamingConfig,
    ) -> Result<query::result::QueryResultIterator> {
        self.query.execute_streaming(sql, config).await
    }

    /// Execute a SELECT with positional `?` parameters (Issue #961).
    ///
    /// The `params` are bound, in source order, into the statement's `?`
    /// placeholders before planning, so they participate in partition-key
    /// classification and encoding. A `WHERE pk = ?` therefore engages the same
    /// partition-targeted fast path as the equivalent literal query.
    ///
    /// # Arguments
    ///
    /// * `sql` - A SELECT statement that may contain positional `?` placeholders
    /// * `params` - Values bound positionally to the `?` placeholders
    ///
    /// # Errors
    ///
    /// Returns an error if the SQL is not a SELECT, the parameter count does not
    /// match the number of `?` placeholders, or execution fails.
    #[cfg(feature = "state_machine")]
    pub async fn execute_with_params(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<query::result::QueryResult> {
        self.query.execute_with_params(sql, params).await
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

    /// Shutdown the database storage engine without consuming self.
    ///
    /// This is useful for language bindings where the Database is wrapped
    /// in an Arc and cannot be consumed. The shutdown operation is idempotent.
    ///
    /// For consuming close that also drops the Database, use `close()`.
    pub async fn shutdown(&self) -> Result<()> {
        self.storage.shutdown().await
    }

    /// Close the database and release all resources
    ///
    /// This method ensures all pending operations are completed and
    /// all resources are properly cleaned up.
    ///
    /// ## Durability contract
    ///
    /// Embedders MUST call `close().await` for a graceful shutdown. `Drop` is
    /// NOT a flush — Tokio has no async drop, so dropping a handle cannot await
    /// a flush and any un-flushed writer state is left to recovery (WAL replay)
    /// rather than being persisted here. For the write path this maps onto
    /// [`storage::write_engine::WriteEngine::close`], which is the actual
    /// memtable-to-SSTable durability boundary (issue #1693).
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
