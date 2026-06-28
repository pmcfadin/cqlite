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
pub mod cql_to_mutation;
#[cfg(feature = "write-support")]
pub mod export;
#[cfg(feature = "write-support")]
pub mod memtable;
#[cfg(feature = "write-support")]
pub mod merge;
#[cfg(feature = "write-support")]
pub mod merge_policy;
#[cfg(feature = "write-support")]
pub mod mutation;
#[cfg(feature = "write-support")]
pub mod wal;

#[cfg(feature = "write-support")]
pub use export::{ExportOptions, ExportReport};
#[cfg(feature = "write-support")]
pub use memtable::Memtable;
#[cfg(feature = "write-support")]
pub use merge::KWayMerger;
#[cfg(feature = "write-support")]
pub use merge_policy::STCSPolicy;
#[cfg(feature = "write-support")]
pub use mutation::{
    CellOperation, ClusteringBound, ClusteringKey, DecoratedKey, Mutation, PartitionKey,
    PartitionTombstone, RangeTombstone, TableId,
};
#[cfg(feature = "write-support")]
pub use wal::WriteAheadLog;

#[cfg(feature = "write-support")]
mod compaction;
#[cfg(feature = "write-support")]
mod maintenance;
#[cfg(feature = "write-support")]
mod stats;
#[cfg(all(test, feature = "write-support"))]
mod test_support;

#[cfg(feature = "write-support")]
pub use maintenance::MaintenanceReport;
#[cfg(feature = "write-support")]
pub use stats::CompactionStats;

use crate::error::{Error, Result};
use crate::schema::{TableSchema, UdtRegistry};
use crate::storage::sstable::writer::SSTableInfo;
#[cfg(feature = "write-support")]
use maintenance::ActiveMerge;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

/// Trait for merge policy implementations (M5.2, Issue #383)
///
/// A merge policy decides which SSTables should be compacted together.
/// This trait allows different compaction strategies (STCS, LCS, TWCS, etc.)
/// to be plugged into the WriteEngine.
#[cfg(feature = "write-support")]
pub trait MergePolicy: Send + std::fmt::Debug {
    /// Select SSTables for the next compaction
    ///
    /// # Arguments
    ///
    /// * `candidates` - Available SSTable paths in the data directory
    ///
    /// # Returns
    ///
    /// Paths to SSTables that should be merged, ordered newest to oldest.
    /// Returns empty Vec if no compaction is needed.
    fn select_merge(&self, candidates: &[PathBuf]) -> Result<Vec<PathBuf>>;
}

/// WAL durability mode for the write engine.
///
/// Controls whether `write` and `write_async` append to and fsync the
/// write-ahead log on every call.  The default (`SyncEachWrite`) matches the
/// pre-existing behavior and is the **only safe choice for production
/// workloads** — a process crash between a successful `write` and a later
/// `flush` will lose mutations written with `Disabled`.
///
/// ## When to use `Disabled`
///
/// - **Bulk-load / import pipelines** where the source data is replayable and
///   you are willing to re-run the load on failure.
/// - **Benchmarking** where you want to isolate CPU-bound write throughput from
///   fsync latency.  The companion `write/ingest_wal_off` Criterion bench uses
///   this variant (see `cqlite-core/benches/write.rs`).
///
/// In both cases, call [`WriteEngine::flush`] (and, optionally,
/// [`WriteEngine::close`]) when the load is finished so the data is durably
/// persisted to SSTables.
///
/// ## WAL replay on restart
///
/// When `Disabled`, no WAL entries are written.  Reopening the engine on the
/// same `wal_dir` after a crash will replay **zero** mutations, even if
/// `flush` was never called.  If you need crash-safe recovery, use
/// `SyncEachWrite`.
///
/// # Example
///
/// ```rust,ignore
/// use cqlite_core::storage::write_engine::{Durability, WriteEngineConfig};
///
/// // Production (default)
/// let config = WriteEngineConfig::new(data, wal, schema);
///
/// // Bulk-load / benchmarking
/// let config = WriteEngineConfig::new(data, wal, schema)
///     .with_durability(Durability::Disabled);
/// ```
#[cfg(feature = "write-support")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Durability {
    /// Append to the WAL and call `fsync` on every `write` / `write_async`
    /// call.  A successful return guarantees the mutation is durable on disk.
    ///
    /// This is the **default** and the safe choice for all production
    /// workloads.
    #[default]
    SyncEachWrite,

    /// Skip WAL append **and** fsync on every `write` / `write_async` call.
    /// Mutations are buffered in the memtable only.  Data is durable only
    /// after a successful [`WriteEngine::flush`].
    ///
    /// **Use only for bulk-load pipelines and benchmarks where durability can
    /// be traded for throughput.**
    Disabled,
}

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
    /// Memtable hard limit in bytes (default: 256MB)
    /// When this limit is reached, writes will fail with an error
    pub memtable_hard_limit: usize,
    /// Table schema for column metadata
    pub schema: TableSchema,
    /// WAL durability mode (default: [`Durability::SyncEachWrite`])
    pub durability: Durability,
    /// Optional UDT registry for resolving bare CQL UDT column types to their
    /// `UserType(...)` marshal form at flush time (issue #929). When `None`
    /// (the default), a column whose `data_type` is a bare UDT name is written
    /// as a single simple cell (documented fallback).
    pub udt_registry: Option<UdtRegistry>,
}

#[cfg(feature = "write-support")]
impl WriteEngineConfig {
    /// Default flush threshold (64 MB)
    pub const DEFAULT_FLUSH_THRESHOLD: usize = 64 * 1024 * 1024;
    /// Default hard limit (256 MB)
    pub const DEFAULT_HARD_LIMIT: usize = 256 * 1024 * 1024;

    /// Create a new configuration with default flush threshold
    pub fn new(data_dir: PathBuf, wal_dir: PathBuf, schema: TableSchema) -> Self {
        Self {
            data_dir,
            wal_dir,
            memtable_flush_threshold: Self::DEFAULT_FLUSH_THRESHOLD,
            memtable_hard_limit: Self::DEFAULT_HARD_LIMIT,
            schema,
            durability: Durability::default(),
            udt_registry: None,
        }
    }

    /// Attach a [`UdtRegistry`] used to resolve bare CQL UDT column types at
    /// flush time (issue #929). See [`WriteEngineConfig::udt_registry`].
    pub fn with_udt_registry(mut self, registry: UdtRegistry) -> Self {
        self.udt_registry = Some(registry);
        self
    }

    /// Set a custom flush threshold
    pub fn with_flush_threshold(mut self, threshold: usize) -> Self {
        self.memtable_flush_threshold = threshold;
        self
    }

    /// Set a custom hard limit
    pub fn with_hard_limit(mut self, limit: usize) -> Self {
        self.memtable_hard_limit = limit;
        self
    }

    /// Set the WAL durability mode.
    ///
    /// Mirrors `with_flush_threshold` in style. See [`Durability`] for the
    /// trade-offs between [`Durability::SyncEachWrite`] (default, production)
    /// and [`Durability::Disabled`] (bulk-load / benchmarking).
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use cqlite_core::storage::write_engine::{Durability, WriteEngineConfig};
    ///
    /// let config = WriteEngineConfig::new(data, wal, schema)
    ///     .with_durability(Durability::Disabled);
    /// ```
    pub fn with_durability(mut self, durability: Durability) -> Self {
        self.durability = durability;
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
/// The `closed` flag uses atomic operations for safe concurrent access checking.
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
    generation: u64,
    /// Whether the engine has been closed (atomic for thread safety)
    closed: AtomicBool,
    /// Active merge state for incremental compaction (M5.2)
    active_merge: Option<ActiveMerge>,
    /// Merge policy for compaction decisions (M5.2)
    merge_policy: Option<Box<dyn MergePolicy>>,
    /// Cumulative compaction statistics (M5.2, Issue #474)
    cumulative_stats: CompactionStats,
    /// Cumulative count of rows written since the engine was opened (Issue #486).
    ///
    /// Incremented for each row that enters the memtable via `write()` or
    /// `write_async()`.  Unlike `memtable.row_count()`, this counter is NOT
    /// reset when the memtable is flushed, so it reflects the total number of
    /// rows written across the lifetime of the session.
    rows_written: u64,
    /// Number of L0 SSTable files successfully flushed since the engine was
    /// opened (Issue #486).
    ///
    /// Incremented once per successful `flush_internal_async()` call that
    /// produces a non-empty SSTable.  This is an in-process counter; it is
    /// reset to zero when the engine is re-opened (a directory scan could
    /// also be used for persistence, but the counter is more robust and avoids
    /// scanning the filesystem on every stats query).
    l0_count: u64,
    /// Advisory exclusive lock on `write_dir` (`.lock` file).
    ///
    /// Held for the lifetime of the `WriteEngine`; released on `close()` or
    /// `Drop`.  The lock is acquired in `new()` via
    /// `fs2::FileExt::try_lock_exclusive`, which returns an error immediately
    /// if another process already holds it (fail-fast, no blocking).
    ///
    /// The lock prevents two `Database` / `WriteEngine` instances from sharing
    /// the same `write_dir`, which would corrupt WAL files and SSTables
    /// (Issue #485).
    dir_lock: std::fs::File,
}

/// Reject any mutation that contains a counter cell write.
///
/// Counter columns require server-side distributed increment semantics and
/// cannot be expressed as a last-write-wins mutation.  Both the sync
/// `write()` and the async `write_async()` paths call this guard immediately
/// after the closed-check.
#[cfg(feature = "write-support")]
fn reject_counter_cells(mutation: &Mutation) -> Result<()> {
    for op in &mutation.operations {
        match op {
            CellOperation::Write { value, .. } | CellOperation::WriteWithTtl { value, .. } => {
                if matches!(value, crate::types::Value::Counter(_)) {
                    return Err(Error::invalid_operation(
                        "counter writes are not supported via the standard mutation path; \
                         counter columns require server-side distributed increment semantics",
                    ));
                }
            }
            _ => {}
        }
    }
    Ok(())
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

        // Acquire an exclusive advisory lock on the WAL directory to prevent
        // two WriteEngine / Database instances from sharing the same write_dir
        // and silently corrupting WAL files or SSTables (Issue #485).
        //
        // We use `try_lock_exclusive` (non-blocking) so that callers get an
        // immediate, actionable error rather than hanging forever.
        let lock_path = config.wal_dir.join(".lock");
        let dir_lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)
            .map_err(|e| {
                Error::Storage(format!("Failed to create lock file {:?}: {}", lock_path, e))
            })?;
        fs2::FileExt::try_lock_exclusive(&dir_lock)
            .map_err(|_| Error::write_dir_locked(config.wal_dir.to_string_lossy().into_owned()))?;

        // Startup sweep: remove orphaned compaction artifacts left by a previous crash.
        //
        // Two kinds of orphans can be left if the process crashes mid-rename in
        // `finalize_merge_async`:
        //
        //   (a) A `.compaction-tmp-{gen}/` directory under `data_dir` with partial
        //       component files.
        //
        //   (b) A partial set of renamed components in `data_dir/{keyspace}/{table}/`
        //       — specifically one or more `nb-{gen}-big-*.db` files without a
        //       matching `TOC.txt`. Because `scan_data_files` discovers SSTables by
        //       `nb-*-big-Data.db` glob, an orphaned Data.db without TOC.txt will be
        //       picked up by the merge policy and fed to `KWayMerger`, which may
        //       produce garbled output.
        //
        // Both sweeps are best-effort: individual failures are logged as warnings but
        // do not abort engine startup.
        Self::sweep_orphaned_compaction_tmp(&config.data_dir);
        Self::sweep_orphaned_partial_sstables(
            &config.data_dir,
            &config.schema.keyspace,
            &config.schema.table,
        );

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
            closed: AtomicBool::new(false),
            active_merge: None,
            merge_policy: None,
            cumulative_stats: CompactionStats::default(),
            rows_written: 0,
            l0_count: 0,
            dir_lock,
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
    #[tracing::instrument(name = "write.mutation", skip(self, mutation))]
    pub fn write(&mut self, mutation: Mutation) -> Result<()> {
        crate::observability::record_result("write", self.write_inner(mutation))
    }

    fn write_inner(&mut self, mutation: Mutation) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(Error::InvalidInput(
                "WriteEngine has been closed".to_string(),
            ));
        }

        reject_counter_cells(&mutation)?;

        // Check hard limit before accepting write
        if self.memtable.size_bytes() >= self.config.memtable_hard_limit {
            return Err(Error::Storage(format!(
                "Memtable at hard limit ({} bytes >= {} bytes). Flush required before accepting more writes.",
                self.memtable.size_bytes(),
                self.config.memtable_hard_limit
            )));
        }

        // 1. Append to WAL (durability) — skipped when Durability::Disabled
        if self.config.durability == Durability::SyncEachWrite {
            self.wal.append(&mutation)?;
            self.wal.sync()?;
        }

        // 2. Compute decorated key from partition key
        let decorated_key = mutation.decorated_key(&self.config.schema)?;

        // 3. Insert into memtable
        self.memtable.insert_with_key(decorated_key, mutation)?;

        // 4. Increment the cumulative rows-written counter (Issue #486).
        //    Done after a successful insert so that failed writes are not counted.
        self.rows_written += 1;
        crate::observability::add_counter(crate::observability::catalog::WRITE_MUTATIONS, 1, &[]);
        self.record_memtable_gauges();

        // 5. Check if memtable should be flushed (only in non-async context)
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
    #[tracing::instrument(name = "write.mutation", skip(self, mutation))]
    pub async fn write_async(&mut self, mutation: Mutation) -> Result<()> {
        crate::observability::record_result("write", self.write_async_inner(mutation).await)
    }

    async fn write_async_inner(&mut self, mutation: Mutation) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(Error::InvalidInput(
                "WriteEngine has been closed".to_string(),
            ));
        }

        reject_counter_cells(&mutation)?;

        // Check hard limit before accepting write
        if self.memtable.size_bytes() >= self.config.memtable_hard_limit {
            return Err(Error::Storage(format!(
                "Memtable at hard limit ({} bytes >= {} bytes). Flush required before accepting more writes.",
                self.memtable.size_bytes(),
                self.config.memtable_hard_limit
            )));
        }

        // 1. Append to WAL (durability) — skipped when Durability::Disabled
        if self.config.durability == Durability::SyncEachWrite {
            self.wal.append(&mutation)?;
            self.wal.sync()?;
        }

        // 2. Compute decorated key from partition key
        let decorated_key = mutation.decorated_key(&self.config.schema)?;

        // 3. Insert into memtable
        self.memtable.insert_with_key(decorated_key, mutation)?;

        // 4. Increment the cumulative rows-written counter (Issue #486).
        //    Done after a successful insert so that failed writes are not counted.
        self.rows_written += 1;
        crate::observability::add_counter(crate::observability::catalog::WRITE_MUTATIONS, 1, &[]);
        self.record_memtable_gauges();

        // 5. Check if memtable should be flushed
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

    /// Emit the current memtable size/row gauges (issue #1036). No-op when the
    /// `observability` feature is off.
    fn record_memtable_gauges(&self) {
        crate::observability::record_gauge(
            crate::observability::catalog::MEMTABLE_SIZE_BYTES,
            self.memtable.size_bytes() as i64,
            &[],
        );
        crate::observability::record_gauge(
            crate::observability::catalog::MEMTABLE_ROWS,
            self.memtable.row_count() as i64,
            &[],
        );
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
    #[tracing::instrument(name = "write.cql_execute", skip(self, statement))]
    pub fn execute(&mut self, statement: &str) -> Result<()> {
        crate::observability::record_result("write", self.execute_inner(statement))
    }

    fn execute_inner(&mut self, statement: &str) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(Error::InvalidInput(
                "WriteEngine has been closed".to_string(),
            ));
        }

        let trimmed = statement.trim();

        // BATCH statements produce multiple mutations.
        //
        // Single-boundary error recording (issue #1036): call the *unrecorded*
        // `write_inner` here rather than the public `write`. `execute` already
        // wraps this method in `record_result`, so the escaping error is counted
        // exactly once at the public boundary instead of twice.
        if trimmed.len() >= 5 && trimmed.as_bytes()[..5].eq_ignore_ascii_case(b"BEGIN") {
            let mutations =
                cql_to_mutation::convert_cql_to_mutations(trimmed, &self.config.schema)?;
            for mutation in mutations {
                self.write_inner(mutation)?;
            }
            Ok(())
        } else {
            let mutation = self.parse_cql_to_mutation(statement)?;
            self.write_inner(mutation)
        }
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
    #[tracing::instrument(name = "flush.public", skip(self))]
    pub async fn flush(&mut self) -> Result<Option<SSTableInfo>> {
        // Single-boundary error recording (issue #1036): `flush` is a public API
        // entry point, so it records here. The nested `flush_internal_async`
        // helper is intentionally *unrecorded* to avoid double-counting when the
        // write/close paths trigger a flush internally.
        crate::observability::record_result(
            "write",
            async {
                if self.closed.load(Ordering::SeqCst) {
                    return Err(Error::InvalidInput(
                        "WriteEngine has been closed".to_string(),
                    ));
                }

                self.flush_internal_async().await
            }
            .await,
        )
    }

    /// Internal synchronous flush helper.
    ///
    /// Bridges to the async flush via [`merge::block_on_async`], which is safe to
    /// call whether or not a Tokio runtime is already running on this thread
    /// (Issue #587).
    fn flush_internal(&mut self) -> Result<()> {
        merge::block_on_async(self.flush_internal_async())?;
        Ok(())
    }

    /// Internal async flush implementation.
    ///
    /// This is an *unrecorded* inner helper (issue #1036): it never calls
    /// `record_error`/`record_result` itself. Error counting happens exactly once
    /// at the public boundary that invoked it (`flush`, `close`, or the
    /// `write`/`write_async` auto-flush path, all of which wrap their work in
    /// `record_result`).
    #[tracing::instrument(name = "flush.memtable", skip(self))]
    async fn flush_internal_async(&mut self) -> Result<Option<SSTableInfo>> {
        // Check if memtable is empty
        if self.memtable.is_empty() {
            return Ok(None);
        }

        let flush_start = Instant::now();
        let rows_to_flush = self.memtable.row_count() as u64;

        log::info!(
            "Flushing memtable: {} partitions, {} rows, {} bytes",
            self.memtable.iter().count(),
            self.memtable.row_count(),
            self.memtable.size_bytes()
        );

        // Create SSTable writer with hint for Bloom filter sizing
        let partition_count_hint = self.memtable.iter().count();
        let mut writer =
            crate::storage::sstable::writer::SSTableWriter::with_expected_partitions_and_registry(
                self.config.data_dir.clone(),
                self.generation,
                &self.config.schema,
                partition_count_hint,
                self.config.udt_registry.as_ref(),
            )?;

        // Two-pass flush (issue #729): compute FINAL encoding baselines from all
        // partitions before writing any data.  Delta encoding in Data.db must use
        // the same baselines that will end up in Statistics.db's EncodingStats.
        // Without this, an early partition encoded against a higher baseline becomes
        // silently corrupted when a later partition lowers the minimum.
        let mut baseline_min_ts = i64::MAX;
        let mut baseline_min_ldt = i32::MAX;
        let mut baseline_min_ttl = i32::MAX;
        for (_, mutations) in self.memtable.iter() {
            let (ts, ldt, ttl) =
                crate::storage::sstable::writer::SSTableWriter::compute_mutations_baseline_stats(
                    mutations,
                );
            baseline_min_ts = baseline_min_ts.min(ts);
            baseline_min_ldt = baseline_min_ldt.min(ldt);
            baseline_min_ttl = baseline_min_ttl.min(ttl);
        }
        writer.pre_seed_encoding_baselines(baseline_min_ts, baseline_min_ldt, baseline_min_ttl);

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
        // If truncate fails, log warning but don't fail - data is already in SSTable
        if let Err(e) = self.wal.truncate() {
            log::warn!(
                "Failed to truncate WAL after successful SSTable flush: {}. \
                Data is safe in SSTable, but WAL cleanup failed.",
                e
            );
            // Don't return error - SSTable write succeeded, which is the important part
        }

        // Clear memtable
        self.memtable.clear();

        // Increment the L0 SSTable counter (Issue #486).
        self.l0_count += 1;

        // Increment generation for next flush
        self.generation += 1;

        // Flush metrics (issue #1036): latency in seconds, rows/bytes flushed,
        // and one L0 SSTable created. Emit the post-clear memtable gauges (now
        // zero) and the current compaction lag (L0 pending) gauge.
        {
            use crate::observability::{self as obs, catalog};
            obs::record_histogram(
                catalog::FLUSH_DURATION,
                flush_start.elapsed().as_secs_f64(),
                &[],
            );
            obs::add_counter(catalog::FLUSH_ROWS, rows_to_flush, &[]);
            obs::add_counter(catalog::FLUSH_BYTES, info.data_size, &[]);
            obs::add_counter(catalog::FLUSH_SSTABLES, 1, &[]);
            obs::record_gauge(catalog::COMPACTION_LAG, self.l0_count as i64, &[]);
        }
        self.record_memtable_gauges();

        Ok(Some(info))
    }

    /// Close the write engine
    ///
    /// This flushes any remaining data in the memtable to SSTable,
    /// syncs the WAL, then marks the engine as closed. After calling close(),
    /// the engine cannot be used for further writes.
    ///
    /// This method is idempotent - calling it multiple times is safe.
    ///
    /// # Returns
    ///
    /// Ok(()) on success.
    ///
    /// # Errors
    ///
    /// Returns an error if the final flush fails. If the WAL truncate fails
    /// after a successful SSTable write, a warning is logged but no error
    /// is returned (the data is already persisted).
    pub async fn close(&mut self) -> Result<()> {
        // Check if already closed (idempotent)
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        log::info!("Closing WriteEngine");

        // Flush any remaining data
        if !self.memtable.is_empty() {
            log::info!("Flushing memtable before close");

            // Attempt to flush to SSTable
            match self.flush_internal_async().await {
                Ok(_) => {
                    log::info!("Memtable flushed successfully");
                }
                Err(e) => {
                    // If flush fails, log error and return it
                    log::error!("Failed to flush memtable during close: {}", e);
                    // Single-boundary error recording (issue #1036): `close` is a
                    // public entry point and `flush_internal_async` is unrecorded,
                    // so record the escaping flush failure here, exactly once.
                    crate::observability::record_error(&e, "write");
                    // Reset closed flag since we failed to close cleanly
                    self.closed.store(false, Ordering::SeqCst);
                    return Err(e);
                }
            }
        }

        // Sync WAL before closing
        if let Err(e) = self.wal.sync() {
            log::warn!("Failed to sync WAL during close: {}", e);
            // Don't fail close if sync fails - data is already persisted to SSTable
        }

        // Release the exclusive advisory lock on write_dir so a subsequent
        // WriteEngine on the same directory can acquire it immediately.
        // On Drop the OS would release it anyway, but explicit unlock is more
        // deterministic in async / multi-phase shutdown sequences.
        if let Err(e) = fs2::FileExt::unlock(&self.dir_lock) {
            log::warn!("Failed to release write_dir advisory lock: {}", e);
        }

        log::info!("WriteEngine closed");

        Ok(())
    }

    /// Parse a CQL statement to a Mutation
    ///
    /// Supports INSERT, UPDATE, and DELETE statements.
    fn parse_cql_to_mutation(&self, statement: &str) -> Result<Mutation> {
        cql_to_mutation::convert_cql_to_mutation(statement, &self.config.schema)
    }

    /// Determine the next SSTable generation number
    ///
    /// Scans the data directory for existing SSTable files and returns
    /// the next generation number.
    fn determine_next_generation(data_dir: &Path) -> Result<u64> {
        let mut max_generation = 0u64;

        if !data_dir.exists() {
            return Ok(1);
        }

        // Recursively scan for SSTable files (writer places them in keyspace/table/ subdirs)
        Self::scan_generations(
            data_dir,
            &mut max_generation,
            crate::storage::sstable::MAX_SSTABLE_SCAN_DEPTH,
        )?;

        Ok(max_generation + 1)
    }

    /// Recursively scan directory for SSTable generation numbers
    fn scan_generations(dir: &Path, max_generation: &mut u64, depth: usize) -> Result<()> {
        for entry in std::fs::read_dir(dir)
            .map_err(|e| Error::Storage(format!("Failed to read data directory: {}", e)))?
        {
            let entry = entry
                .map_err(|e| Error::Storage(format!("Failed to read directory entry: {}", e)))?;

            let filename = entry.file_name();
            let filename_str = filename.to_string_lossy();

            // Parse generation from filename: nb-{generation}-big-{Component}.db
            if filename_str.starts_with("nb-") && filename_str.contains("-big-") {
                if let Some(gen_str) = filename_str
                    .strip_prefix("nb-")
                    .and_then(|s| s.split('-').next())
                {
                    if let Ok(gen) = gen_str.parse::<u64>() {
                        *max_generation = (*max_generation).max(gen);
                    }
                }
            } else if depth > 0 {
                let path = entry.path();
                if path.is_dir() {
                    Self::scan_generations(&path, max_generation, depth - 1)?;
                }
            }
        }
        Ok(())
    }
}

/// Safety-net `Drop` implementation for `WriteEngine`.
///
/// When a `WriteEngine` is dropped without calling `close()` (e.g. due to an
/// early return or a panic), the OS would release the advisory lock anyway once
/// the file descriptor is closed.  This explicit `Drop` makes the release
/// deterministic and logs a warning so callers can distinguish a normal shutdown
/// from an ungraceful one.
#[cfg(feature = "write-support")]
impl Drop for WriteEngine {
    fn drop(&mut self) {
        // If close() was already called the lock was already released; a second
        // unlock is a no-op on most platforms (Linux returns ENOLCK which we
        // ignore here).  The important invariant is that the lock is always
        // released before the file descriptor is closed by the OS on drop.
        if let Err(e) = fs2::FileExt::unlock(&self.dir_lock) {
            log::debug!(
                "WriteEngine drop: advisory lock release returned: {} \
                 (may have been released by close() already)",
                e
            );
        }
    }
}

#[cfg(all(test, feature = "write-support"))]
mod tests {
    use super::*;
    use crate::storage::write_engine::test_support::{create_test_mutation, create_test_schema};
    use tempfile::TempDir;

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
        assert_eq!(
            config.memtable_hard_limit,
            WriteEngineConfig::DEFAULT_HARD_LIMIT
        );

        let config = config.with_flush_threshold(128 * 1024 * 1024);
        assert_eq!(config.memtable_flush_threshold, 128 * 1024 * 1024);

        let config = config.with_hard_limit(512 * 1024 * 1024);
        assert_eq!(config.memtable_hard_limit, 512 * 1024 * 1024);
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
        assert!(!engine.closed.load(std::sync::atomic::Ordering::Relaxed));
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
        assert!(!entries.is_empty(), "SSTable files should exist");
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

        let mut engine = WriteEngine::new(config).unwrap();

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

        let mut engine = WriteEngine::new(config).unwrap();

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
    fn test_write_engine_execute_table_mismatch() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let mut engine = WriteEngine::new(config).unwrap();

        // Schema defines test_table, but statement targets users → table mismatch
        let result = engine.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("targets table 'users'")
                && err_msg.contains("schema is for 'test_table'"),
            "Expected table mismatch error, got: {}",
            err_msg
        );
    }

    #[test]
    fn test_write_engine_execute_insert_success() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let mut engine = WriteEngine::new(config).unwrap();

        assert_eq!(engine.memtable_row_count(), 0);

        // INSERT matching the test schema: test_ks.test_table(id int PK, name text)
        let result = engine.execute("INSERT INTO test_table (id, name) VALUES (1, 'Alice')");
        assert!(
            result.is_ok(),
            "execute() failed: {:?}",
            result.unwrap_err()
        );

        assert_eq!(engine.memtable_row_count(), 1);
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

    #[tokio::test]
    async fn test_write_engine_close_idempotent() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let mut engine = WriteEngine::new(config).unwrap();

        // Close once
        engine.close().await.unwrap();
        assert!(engine.closed.load(Ordering::SeqCst));

        // Close again - should be idempotent
        engine.close().await.unwrap();
        assert!(engine.closed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_write_engine_close_syncs_wal() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let mut engine = WriteEngine::new(config).unwrap();

        // Write a mutation
        let mutation = create_test_mutation(1, "Alice", 1000000);
        engine.write(mutation).unwrap();

        // Close should sync WAL before completing
        engine.close().await.unwrap();

        // Verify WAL was truncated (because data was flushed to SSTable)
        assert_eq!(engine.wal_size(), 0);
    }

    #[test]
    fn test_write_engine_closed_flag_atomic() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let engine = WriteEngine::new(config).unwrap();

        // Verify closed flag is atomic
        assert!(!engine.closed.load(Ordering::SeqCst));

        // Store true
        engine.closed.store(true, Ordering::SeqCst);
        assert!(engine.closed.load(Ordering::SeqCst));

        // Swap back to false
        let prev = engine.closed.swap(false, Ordering::SeqCst);
        assert!(prev);
        assert!(!engine.closed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_write_engine_write_after_close_fails() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let mut engine = WriteEngine::new(config).unwrap();

        // Close the engine
        engine.close().await.unwrap();

        // Try to write - should fail
        let mutation = create_test_mutation(1, "Alice", 1000000);
        let result = engine.write(mutation);

        assert!(result.is_err());
        match result {
            Err(Error::InvalidInput(_)) => {}
            _ => panic!("Expected InvalidInput error"),
        }
    }

    #[tokio::test]
    async fn test_write_engine_flush_after_close_fails() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let mut engine = WriteEngine::new(config).unwrap();

        // Close the engine
        engine.close().await.unwrap();

        // Try to flush - should fail
        let result = engine.flush().await;

        assert!(result.is_err());
        match result {
            Err(Error::InvalidInput(_)) => {}
            _ => panic!("Expected InvalidInput error"),
        }
    }

    #[test]
    fn test_write_engine_hard_limit_enforcement() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        // Set very low hard limit (2KB) with flush threshold higher (to prevent auto-flush)
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        )
        .with_flush_threshold(10 * 1024) // 10KB flush threshold (higher than hard limit for test)
        .with_hard_limit(2048); // 2KB hard limit

        let mut engine = WriteEngine::new(config).unwrap();

        // Write mutations until we hit the hard limit
        let mut write_count = 0;
        for i in 0..1000 {
            let mutation = create_test_mutation(i, &format!("User{}", i), 1000000 + i as i64);
            let result = engine.write(mutation);

            match result {
                Ok(()) => {
                    write_count += 1;
                }
                Err(Error::Storage(msg)) => {
                    assert!(msg.contains("hard limit"));
                    break;
                }
                Err(e) => panic!("Expected Storage error, got: {:?}", e),
            }
        }

        // Should have stopped before 1000 writes due to hard limit
        assert!(
            write_count < 1000,
            "Should have hit hard limit before 1000 writes"
        );
        assert!(
            write_count > 0,
            "Should have accepted at least some writes before hitting limit"
        );
    }

    #[tokio::test]
    async fn test_write_engine_hard_limit_enforcement_async() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        // Set very low hard limit (2KB) with flush threshold higher (to prevent auto-flush)
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        )
        .with_flush_threshold(10 * 1024) // 10KB flush threshold (higher than hard limit for test)
        .with_hard_limit(2048); // 2KB hard limit

        let mut engine = WriteEngine::new(config).unwrap();

        // Write mutations until we hit the hard limit
        let mut write_count = 0;
        for i in 0..1000 {
            let mutation = create_test_mutation(i, &format!("User{}", i), 1000000 + i as i64);
            let result = engine.write_async(mutation).await;

            match result {
                Ok(()) => {
                    write_count += 1;
                }
                Err(Error::Storage(msg)) => {
                    assert!(msg.contains("hard limit"));
                    break;
                }
                Err(e) => panic!("Expected Storage error, got: {:?}", e),
            }
        }

        // Should have stopped before 1000 writes due to hard limit
        assert!(
            write_count < 1000,
            "Should have hit hard limit before 1000 writes"
        );
        assert!(
            write_count > 0,
            "Should have accepted at least some writes before hitting limit"
        );
    }

    #[tokio::test]
    async fn test_write_engine_hard_limit_recovery_after_flush() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        // Set low hard limit
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        )
        .with_flush_threshold(1024)
        .with_hard_limit(2048);

        let mut engine = WriteEngine::new(config).unwrap();

        // Write until hard limit
        let mut first_batch_count = 0;
        for i in 0..1000 {
            let mutation = create_test_mutation(i, &format!("User{}", i), 1000000 + i as i64);
            let result = engine.write(mutation);

            if result.is_err() {
                break;
            }

            first_batch_count += 1;
        }

        assert!(
            first_batch_count > 0,
            "Should have accepted some writes before limit"
        );

        // Flush to clear memtable
        engine.flush().await.unwrap();

        // Should be able to write again after flush
        let mutation = create_test_mutation(9999, "After flush", 2000000);
        let result = engine.write(mutation);
        assert!(result.is_ok(), "Should accept writes after flush");

        assert_eq!(engine.memtable_row_count(), 1);
    }

    #[test]
    fn test_generation_counter_is_u64() {
        // Verify that generation counter is u64 to prevent overflow (Issue #410)
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let engine = WriteEngine::new(config).unwrap();

        // Verify type by checking the value is within u64 range
        let generation: u64 = engine.generation();
        assert_eq!(generation, 1u64);

        // This compile-time check ensures generation() returns u64
        // If it returned u32, this assignment would be a no-op but still compile
        let _type_check: u64 = generation;

        // Verify that u64 can handle generations beyond u32::MAX
        // This would overflow with u32 (max value: 4,294,967,295)
        let large_generation: u64 = u32::MAX as u64 + 1000;
        assert!(large_generation > u32::MAX as u64);
        assert_eq!(large_generation, 4_294_968_295u64);
    }

    #[test]
    fn test_determine_next_generation_large_numbers() {
        // Verify that generation parsing handles large u64 values (Issue #410)
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();

        // Create dummy SSTable files with large generation numbers
        // These would overflow if we used u32 (max: 4,294,967,295)
        let large_gen: u64 = u32::MAX as u64 + 100;
        std::fs::write(data_dir.join(format!("nb-{}-big-Data.db", large_gen)), b"").unwrap();

        let generation = WriteEngine::determine_next_generation(&data_dir).unwrap();
        assert_eq!(generation, large_gen + 1);
        assert!(generation > u32::MAX as u64);
    }

    // ============================================================================
    // Issue #547: WAL durability toggle tests
    // ============================================================================

    /// `Durability::SyncEachWrite` is the default variant.
    #[test]
    fn test_durability_default_is_sync_each_write() {
        assert_eq!(Durability::default(), Durability::SyncEachWrite);
    }

    /// `WriteEngineConfig` defaults to `Durability::SyncEachWrite`.
    #[test]
    fn test_config_default_durability() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );
        assert_eq!(config.durability, Durability::SyncEachWrite);
    }

    /// `with_durability` builder sets the field and returns `Self`.
    #[test]
    fn test_config_with_durability_builder() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        )
        .with_durability(Durability::Disabled);
        assert_eq!(config.durability, Durability::Disabled);
    }

    /// With `Durability::SyncEachWrite`, the WAL grows after each `write`.
    #[test]
    fn test_wal_on_produces_wal_growth() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        )
        .with_durability(Durability::SyncEachWrite);

        let mut engine = WriteEngine::new(config).unwrap();
        assert_eq!(engine.wal_size(), 0, "WAL must start empty");

        let mutation = create_test_mutation(1, "Alice", 1_000_000);
        engine.write(mutation).unwrap();

        assert!(
            engine.wal_size() > 0,
            "WAL must grow after write with SyncEachWrite"
        );
    }

    /// With `Durability::Disabled`, the WAL is never written — `wal_size()` stays 0.
    #[test]
    fn test_wal_off_produces_no_wal_growth() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        )
        .with_durability(Durability::Disabled);

        let mut engine = WriteEngine::new(config).unwrap();
        assert_eq!(engine.wal_size(), 0, "WAL must start empty");

        // Write several mutations — none should touch the WAL.
        for i in 0..10 {
            let mutation = create_test_mutation(i, &format!("User{}", i), 1_000_000 + i as i64);
            engine.write(mutation).unwrap();
        }

        assert_eq!(
            engine.wal_size(),
            0,
            "WAL must remain empty with Durability::Disabled"
        );
        assert_eq!(
            engine.memtable_row_count(),
            10,
            "Mutations must reach the memtable even without WAL"
        );
    }

    /// With `Durability::Disabled`, async writes also skip the WAL.
    #[tokio::test]
    async fn test_wal_off_write_async_produces_no_wal_growth() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        )
        .with_durability(Durability::Disabled);

        let mut engine = WriteEngine::new(config).unwrap();

        for i in 0..5 {
            let mutation = create_test_mutation(i, &format!("User{}", i), 1_000_000 + i as i64);
            engine.write_async(mutation).await.unwrap();
        }

        assert_eq!(
            engine.wal_size(),
            0,
            "WAL must remain empty with Durability::Disabled (async path)"
        );
        assert_eq!(engine.memtable_row_count(), 5);
    }

    /// With `Durability::Disabled`, data that was never WAL'd is NOT replayed on
    /// restart — confirming the documented durability trade-off.
    #[test]
    fn test_wal_off_no_replay_on_restart() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        {
            let config = WriteEngineConfig::new(
                temp_dir.path().join("data"),
                temp_dir.path().join("wal"),
                schema.clone(),
            )
            .with_durability(Durability::Disabled);

            let mut engine = WriteEngine::new(config).unwrap();

            for i in 0..5 {
                let mutation = create_test_mutation(i, &format!("User{}", i), 1_000_000 + i as i64);
                engine.write(mutation).unwrap();
            }

            // Drop without flushing — simulating crash.
        }

        // Reopen with default durability.  Because the WAL was never written, the
        // memtable must be empty.
        let config2 = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );
        let engine2 = WriteEngine::new(config2).unwrap();

        assert_eq!(
            engine2.memtable_row_count(),
            0,
            "No WAL entries were written with Disabled, so no replay is possible"
        );
    }

    /// With `Durability::SyncEachWrite`, mutations ARE replayed after a simulated crash.
    #[test]
    fn test_wal_on_replays_on_restart() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        {
            let config = WriteEngineConfig::new(
                temp_dir.path().join("data"),
                temp_dir.path().join("wal"),
                schema.clone(),
            )
            .with_durability(Durability::SyncEachWrite);

            let mut engine = WriteEngine::new(config).unwrap();

            for i in 0..5 {
                let mutation = create_test_mutation(i, &format!("User{}", i), 1_000_000 + i as i64);
                engine.write(mutation).unwrap();
            }

            // Drop without flushing — WAL entries remain on disk.
        }

        // Reopen — WAL replay must restore the 5 mutations.
        let config2 = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        )
        .with_durability(Durability::SyncEachWrite);

        let engine2 = WriteEngine::new(config2).unwrap();

        assert_eq!(
            engine2.memtable_row_count(),
            5,
            "SyncEachWrite must replay mutations durably on restart"
        );
    }

    // ============================================================================
    // Issue #485: write_dir file lock tests
    // ============================================================================

    /// A second WriteEngine on the same write_dir must fail fast with a clear error
    /// while the first engine is still open.
    #[test]
    fn test_write_dir_lock_second_engine_fails_fast() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config1 = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema.clone(),
        );

        // First engine acquires the lock.
        let _engine1 = WriteEngine::new(config1).unwrap();

        // Second engine on the same wal_dir must fail immediately.
        let config2 = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let result = WriteEngine::new(config2);
        assert!(
            result.is_err(),
            "A second WriteEngine on the same write_dir must fail"
        );

        let err = result.unwrap_err();
        assert!(
            matches!(err, Error::WriteDirLocked { .. }),
            "Expected WriteDirLocked error, got: {:?}",
            err
        );

        // Error message must contain the path and actionable advice.
        let msg = err.to_string();
        assert!(
            msg.contains("already locked"),
            "Error message must mention the lock: {}",
            msg
        );
        assert!(
            msg.contains("Only one Database instance"),
            "Error message must explain the constraint: {}",
            msg
        );
    }

    /// After the first engine is closed, a new engine must successfully acquire the lock.
    #[tokio::test]
    async fn test_write_dir_lock_reacquired_after_close() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config1 = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema.clone(),
        );

        // First engine acquires the lock.
        let mut engine1 = WriteEngine::new(config1).unwrap();

        // Close releases the lock.
        engine1.close().await.unwrap();

        // A new engine on the same directory must now succeed.
        let config2 = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let engine2 = WriteEngine::new(config2);
        assert!(
            engine2.is_ok(),
            "WriteEngine must acquire lock after the previous engine closed: {:?}",
            engine2.err()
        );
    }

    /// After the first engine is dropped (without calling close()), a new engine
    /// must also successfully acquire the lock.
    #[test]
    fn test_write_dir_lock_reacquired_after_drop() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config1 = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema.clone(),
        );

        // First engine acquires the lock; drop releases it.
        {
            let _engine1 = WriteEngine::new(config1).unwrap();
            // engine1 dropped here, lock released
        }

        // A new engine on the same directory must now succeed.
        let config2 = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let engine2 = WriteEngine::new(config2);
        assert!(
            engine2.is_ok(),
            "WriteEngine must acquire lock after the previous engine was dropped: {:?}",
            engine2.err()
        );
    }
}
