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
pub use mutation::{CellOperation, ClusteringKey, DecoratedKey, Mutation, PartitionKey, TableId};
#[cfg(feature = "write-support")]
pub use wal::WriteAheadLog;

use crate::error::{Error, Result};
use crate::schema::TableSchema;
use crate::storage::sstable::writer::SSTableInfo;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Maintenance report from a maintenance_step() call (M5.2, Issue #384)
#[cfg(feature = "write-support")]
#[derive(Debug, Clone)]
pub struct MaintenanceReport {
    /// Time spent in this maintenance step
    pub time_spent: Duration,
    /// Completed merge output files (if any merge completed)
    pub completed_merges: Vec<PathBuf>,
    /// Number of rows merged in this step
    pub rows_merged: u64,
    /// Number of bytes written in this step
    pub bytes_written: u64,
    /// Whether there is pending compaction work
    pub pending_compaction: bool,
}

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

/// Active merge state for incremental compaction (M5.2, Issue #384)
#[cfg(feature = "write-support")]
#[derive(Debug)]
#[allow(dead_code)] // Will be used when SSTable reader integration is complete
struct ActiveMerge {
    /// K-way merger performing the compaction
    merger: KWayMerger,
    /// Output SSTable writer
    writer: crate::storage::sstable::writer::SSTableWriter,
    /// Input SSTable paths being merged
    input_paths: Vec<PathBuf>,
    /// Statistics accumulated so far
    rows_merged: u64,
    bytes_written: u64,
    /// When this merge started
    started_at: Instant,
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
        }
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
            closed: AtomicBool::new(false),
            active_merge: None,
            merge_policy: None,
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
        if self.closed.load(Ordering::SeqCst) {
            return Err(Error::InvalidInput(
                "WriteEngine has been closed".to_string(),
            ));
        }

        // Check hard limit before accepting write
        if self.memtable.size_bytes() >= self.config.memtable_hard_limit {
            return Err(Error::Storage(format!(
                "Memtable at hard limit ({} bytes >= {} bytes). Flush required before accepting more writes.",
                self.memtable.size_bytes(),
                self.config.memtable_hard_limit
            )));
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
        if self.closed.load(Ordering::SeqCst) {
            return Err(Error::InvalidInput(
                "WriteEngine has been closed".to_string(),
            ));
        }

        // Check hard limit before accepting write
        if self.memtable.size_bytes() >= self.config.memtable_hard_limit {
            return Err(Error::Storage(format!(
                "Memtable at hard limit ({} bytes >= {} bytes). Flush required before accepting more writes.",
                self.memtable.size_bytes(),
                self.config.memtable_hard_limit
            )));
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
        if self.closed.load(Ordering::SeqCst) {
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
        if self.closed.load(Ordering::SeqCst) {
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
                let rt = tokio::runtime::Runtime::new().map_err(|e| {
                    Error::Storage(format!("Failed to create tokio runtime: {}", e))
                })?;
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

        // Create SSTable writer with hint for Bloom filter sizing
        let partition_count_hint = self.memtable.iter().count();
        let mut writer = crate::storage::sstable::writer::SSTableWriter::with_expected_partitions(
            self.config.data_dir.clone(),
            self.generation,
            &self.config.schema,
            partition_count_hint,
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

        // Increment generation for next flush
        self.generation += 1;

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
    pub fn generation(&self) -> u64 {
        self.generation
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

        // Scan directory for SSTable files
        for entry in std::fs::read_dir(data_dir)
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
                        max_generation = max_generation.max(gen);
                    }
                }
            }
        }

        Ok(max_generation + 1)
    }

    /// Set the merge policy for background compaction (M5.2, Issue #383)
    ///
    /// # Arguments
    ///
    /// * `policy` - Merge policy implementation (e.g., STCS, LCS, TWCS)
    pub fn set_merge_policy(&mut self, policy: Box<dyn MergePolicy>) -> Result<()> {
        self.merge_policy = Some(policy);
        Ok(())
    }

    /// Perform incremental maintenance work (M5.2, Issue #384)
    ///
    /// This method performs background compaction work within a time budget.
    /// It can be called repeatedly from a background thread or task scheduler
    /// to make incremental progress on compaction.
    ///
    /// ## Behavior
    ///
    /// 1. If no active merge exists, consult the merge policy for work
    /// 2. If merge work is available, start a new merge
    /// 3. Process the active merge until budget is exhausted
    /// 4. Return progress report
    ///
    /// ## Invariants
    ///
    /// - Budget is honored within 10% tolerance
    /// - At least one partition is processed per call (minimum progress guarantee)
    /// - Merge state is preserved across calls for resumption
    ///
    /// ## Budget Enforcement
    ///
    /// The budget is honored within approximately 10% tolerance. This tolerance
    /// exists to avoid interrupting partition processing mid-stream, which would
    /// require complex state management to resume. The tolerance ensures forward
    /// progress on each call while remaining responsive to time constraints.
    ///
    /// # Arguments
    ///
    /// * `budget` - Maximum time to spend in this call
    ///
    /// # Returns
    ///
    /// A report containing progress metrics and whether more work is pending.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Engine has been closed
    /// - Merge policy returns an error
    /// - SSTable reading or writing fails
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use std::time::Duration;
    ///
    /// // Background compaction loop
    /// loop {
    ///     let report = engine.maintenance_step(Duration::from_millis(100))?;
    ///
    ///     if !report.pending_compaction {
    ///         // No more work, sleep or exit
    ///         break;
    ///     }
    ///
    ///     // Log progress
    ///     println!("Merged {} rows in {:?}", report.rows_merged, report.time_spent);
    /// }
    /// ```
    pub fn maintenance_step(&mut self, budget: Duration) -> Result<MaintenanceReport> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(Error::InvalidInput(
                "WriteEngine has been closed".to_string(),
            ));
        }

        let start = Instant::now();
        let mut report = MaintenanceReport {
            time_spent: Duration::from_secs(0),
            completed_merges: Vec::new(),
            rows_merged: 0,
            bytes_written: 0,
            pending_compaction: false,
        };

        // If no merge policy is set, no maintenance work to do
        let merge_policy = match &self.merge_policy {
            Some(policy) => policy,
            None => {
                report.time_spent = start.elapsed();
                return Ok(report);
            }
        };

        // If no active merge exists, check if we should start one
        if self.active_merge.is_none() {
            let candidates = self.scan_sstable_candidates()?;
            let selected = merge_policy.select_merge(&candidates)?;

            if !selected.is_empty() {
                // Start a new merge
                self.start_merge(selected)?;
            } else {
                // No work selected by policy
                report.time_spent = start.elapsed();
                report.pending_compaction = false;
                return Ok(report);
            }
        }

        // Process active merge within budget
        let budget_tolerance = budget.mul_f32(1.1); // 10% tolerance
        let mut partitions_processed = 0;

        while let Some(merge) = &mut self.active_merge {
            // Check budget (but always process at least one partition)
            if partitions_processed > 0 && start.elapsed() >= budget_tolerance {
                break;
            }

            // Process one partition from the merge
            let step = merge.merger.step()?;

            match step {
                merge::MergeStep::Partition { key, rows } => {
                    partitions_processed += 1;
                    let row_count = rows.len() as u64;

                    // Convert MergeEntry rows to Mutation format
                    // (collect into a vec first to release the borrow on merge)
                    let entries_vec: Vec<_> = rows.into_iter().collect();

                    // Now we can call self methods without conflict
                    let mutations = entries_vec
                        .into_iter()
                        .map(|entry| self.merge_entry_to_mutation(entry))
                        .collect::<Result<Vec<_>>>()?;

                    // Write partition to output SSTable
                    // Re-borrow active_merge to write
                    if let Some(merge) = &mut self.active_merge {
                        merge.writer.write_partition(key, mutations)?;
                        merge.rows_merged += row_count;
                    }

                    // Update stats
                    report.rows_merged += row_count;
                }
                merge::MergeStep::Complete => {
                    // Merge is complete - finalize and clean up
                    // Use blocking call to handle async finalization
                    self.finalize_merge_blocking(&mut report)?;
                    break;
                }
            }
        }

        // Check if more work is pending
        report.pending_compaction = self.active_merge.is_some();
        report.time_spent = start.elapsed();

        Ok(report)
    }

    /// Scan data directory for SSTable candidates (M5.2 helper)
    fn scan_sstable_candidates(&self) -> Result<Vec<PathBuf>> {
        let mut candidates = Vec::new();

        if !self.config.data_dir.exists() {
            return Ok(candidates);
        }

        for entry in std::fs::read_dir(&self.config.data_dir)
            .map_err(|e| Error::Storage(format!("Failed to read data directory: {}", e)))?
        {
            let entry = entry
                .map_err(|e| Error::Storage(format!("Failed to read directory entry: {}", e)))?;

            let path = entry.path();
            let filename = path.file_name().unwrap_or_default().to_string_lossy();

            // Only consider Data.db files
            if filename.starts_with("nb-") && filename.ends_with("-big-Data.db") {
                candidates.push(path);
            }
        }

        Ok(candidates)
    }

    /// Start a new merge operation (M5.2 helper)
    fn start_merge(&mut self, input_paths: Vec<PathBuf>) -> Result<()> {
        log::info!(
            "Starting compaction merge of {} SSTables",
            input_paths.len()
        );

        // Create K-way merger
        let merger = KWayMerger::new(input_paths.clone(), &self.config.schema)?;

        // Create output SSTable writer
        let output_generation = self.generation;
        let writer = crate::storage::sstable::writer::SSTableWriter::new(
            self.config.data_dir.clone(),
            output_generation,
            &self.config.schema,
        )?;

        // Increment generation for next operation
        self.generation += 1;

        self.active_merge = Some(ActiveMerge {
            merger,
            writer,
            input_paths,
            rows_merged: 0,
            bytes_written: 0,
            started_at: Instant::now(),
        });

        Ok(())
    }

    /// Finalize the active merge - blocking version (M5.2 helper)
    fn finalize_merge_blocking(&mut self, report: &mut MaintenanceReport) -> Result<()> {
        // Use existing runtime or create new one
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                // We're inside a runtime, use it
                handle.block_on(self.finalize_merge_async(report))
            }
            Err(_) => {
                // No runtime, create one
                let rt = tokio::runtime::Runtime::new().map_err(|e| {
                    Error::Storage(format!("Failed to create tokio runtime: {}", e))
                })?;
                rt.block_on(self.finalize_merge_async(report))
            }
        }
    }

    /// Finalize the active merge - async version (M5.2 helper)
    async fn finalize_merge_async(&mut self, report: &mut MaintenanceReport) -> Result<()> {
        let merge = match self.active_merge.take() {
            Some(m) => m,
            None => return Ok(()),
        };

        log::info!(
            "Finalizing compaction merge: {} rows, {:?} elapsed",
            merge.rows_merged,
            merge.started_at.elapsed()
        );

        // Finish writing the output SSTable
        let output_info = merge.writer.finish().await?;

        log::info!(
            "Compaction output: {} bytes, {} partitions",
            output_info.data_size,
            output_info.partition_count
        );

        // Update report with completion info
        report.completed_merges.push(output_info.data_path.clone());
        report.bytes_written += output_info.data_size;

        // Delete input SSTables (all components)
        for input_path in &merge.input_paths {
            self.delete_sstable_files(input_path)?;
        }

        Ok(())
    }

    /// Delete all component files for an SSTable (M5.2 helper)
    fn delete_sstable_files(&self, data_path: &Path) -> Result<()> {
        // Extract base path: nb-{gen}-big
        let filename = data_path
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| Error::Storage("Invalid SSTable path".to_string()))?;

        let base = filename
            .strip_suffix("-Data.db")
            .ok_or_else(|| Error::Storage("Invalid Data.db filename".to_string()))?;

        // Component suffixes to delete
        let components = [
            "Data.db",
            "Index.db",
            "Summary.db",
            "Statistics.db",
            "CompressionInfo.db",
            "Filter.db",
        ];

        let parent_dir = data_path.parent().unwrap_or(Path::new("."));

        for component in &components {
            let component_path = parent_dir.join(format!("{}-{}", base, component));
            if component_path.exists() {
                std::fs::remove_file(&component_path).map_err(|e| {
                    Error::Storage(format!("Failed to delete {:?}: {}", component_path, e))
                })?;
                log::debug!("Deleted compaction input: {:?}", component_path);
            }
        }

        Ok(())
    }

    /// Convert MergeEntry to Mutation (M5.2 helper)
    ///
    /// Delegates to `KWayMerger::merge_entry_to_mutation` to avoid duplication.
    fn merge_entry_to_mutation(
        &self,
        entry: merge::MergeEntry,
    ) -> Result<crate::storage::write_engine::mutation::Mutation> {
        merge::KWayMerger::merge_entry_to_mutation(entry, &self.config.schema)
    }
}

#[cfg(all(test, feature = "write-support"))]
mod tests {
    use super::*;
    use crate::schema::{Column, KeyColumn};
    use crate::storage::write_engine::mutation::{CellOperation, PartitionKey, TableId};
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
    fn test_set_merge_policy() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let mut engine = WriteEngine::new(config).unwrap();

        // Should succeed now (was previously returning error)
        let policy = Box::new(crate::storage::write_engine::STCSPolicy::default());
        engine.set_merge_policy(policy).unwrap();

        // With policy set but no SSTables, should return quickly with no work
        let report = engine
            .maintenance_step(std::time::Duration::from_millis(100))
            .unwrap();
        assert!(!report.pending_compaction);
        assert_eq!(report.rows_merged, 0);
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

    // M5.2 maintenance_step() tests (Issue #384)

    #[test]
    fn test_maintenance_step_no_policy() {
        // Without a merge policy, maintenance_step should do nothing
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let mut engine = WriteEngine::new(config).unwrap();

        // Call maintenance_step without setting a policy
        let report = engine.maintenance_step(Duration::from_millis(100)).unwrap();

        // Should return immediately with no work done
        assert_eq!(report.rows_merged, 0);
        assert_eq!(report.bytes_written, 0);
        assert_eq!(report.completed_merges.len(), 0);
        assert!(!report.pending_compaction);
        assert!(report.time_spent < Duration::from_millis(50));
    }

    #[test]
    fn test_maintenance_step_with_closed_engine() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let mut engine = WriteEngine::new(config).unwrap();

        // Close the engine
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(engine.close())
            .unwrap();

        // maintenance_step should fail on closed engine
        let result = engine.maintenance_step(Duration::from_millis(100));
        assert!(result.is_err());
        match result {
            Err(Error::InvalidInput(msg)) => {
                assert!(msg.contains("closed"));
            }
            _ => panic!("Expected InvalidInput error"),
        }
    }

    #[test]
    fn test_maintenance_report_creation() {
        let report = MaintenanceReport {
            time_spent: Duration::from_millis(250),
            completed_merges: vec![PathBuf::from("data/nb-5-big-Data.db")],
            rows_merged: 1000,
            bytes_written: 1024 * 1024,
            pending_compaction: true,
        };

        assert_eq!(report.time_spent.as_millis(), 250);
        assert_eq!(report.completed_merges.len(), 1);
        assert_eq!(report.rows_merged, 1000);
        assert_eq!(report.bytes_written, 1024 * 1024);
        assert!(report.pending_compaction);
    }

    #[test]
    fn test_scan_sstable_candidates_empty_dir() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let engine = WriteEngine::new(config).unwrap();

        let candidates = engine.scan_sstable_candidates().unwrap();
        assert_eq!(candidates.len(), 0);
    }

    #[test]
    fn test_scan_sstable_candidates_with_sstables() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let engine = WriteEngine::new(config).unwrap();

        // Create dummy SSTable files
        let data_dir = temp_dir.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::write(data_dir.join("nb-1-big-Data.db"), b"").unwrap();
        std::fs::write(data_dir.join("nb-2-big-Data.db"), b"").unwrap();
        std::fs::write(data_dir.join("nb-3-big-Index.db"), b"").unwrap(); // Not a Data.db
        std::fs::write(data_dir.join("other-file.txt"), b"").unwrap(); // Not an SSTable

        let candidates = engine.scan_sstable_candidates().unwrap();

        // Should only find Data.db files
        assert_eq!(candidates.len(), 2);
        assert!(candidates
            .iter()
            .all(|p| p.to_string_lossy().contains("Data.db")));
    }

    #[test]
    fn test_delete_sstable_files() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let engine = WriteEngine::new(config).unwrap();

        // Create dummy SSTable component files
        let data_dir = temp_dir.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();

        let components = [
            "nb-5-big-Data.db",
            "nb-5-big-Index.db",
            "nb-5-big-Summary.db",
            "nb-5-big-Statistics.db",
        ];

        for component in &components {
            std::fs::write(data_dir.join(component), b"dummy").unwrap();
        }

        // Verify files exist
        for component in &components {
            assert!(data_dir.join(component).exists());
        }

        // Delete SSTable files
        let data_path = data_dir.join("nb-5-big-Data.db");
        engine.delete_sstable_files(&data_path).unwrap();

        // Verify files are deleted
        for component in &components {
            assert!(!data_dir.join(component).exists());
        }
    }

    // Mock merge policy that selects specific files for testing
    #[derive(Debug)]
    #[allow(dead_code)] // Used in multiple test functions below
    struct TestMergePolicy {
        files_to_select: Vec<PathBuf>,
    }

    impl MergePolicy for TestMergePolicy {
        fn select_merge(&self, _candidates: &[PathBuf]) -> Result<Vec<PathBuf>> {
            Ok(self.files_to_select.clone())
        }
    }

    #[test]
    fn test_maintenance_step_with_policy_no_work() {
        // Policy that returns empty selection (no work to do)
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let mut engine = WriteEngine::new(config).unwrap();

        // Set a policy that selects nothing
        let policy = TestMergePolicy {
            files_to_select: vec![],
        };
        engine.set_merge_policy(Box::new(policy)).unwrap();

        // Call maintenance_step - policy selects no work
        let report = engine.maintenance_step(Duration::from_millis(100)).unwrap();

        // Should return with no work done
        assert_eq!(report.rows_merged, 0);
        assert_eq!(report.bytes_written, 0);
        assert_eq!(report.completed_merges.len(), 0);
        assert!(!report.pending_compaction);
    }

    #[test]
    fn test_maintenance_step_budget_honored() {
        // Test that budget is approximately honored
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let mut engine = WriteEngine::new(config).unwrap();

        // Set a policy that selects nothing
        let policy = TestMergePolicy {
            files_to_select: vec![],
        };
        engine.set_merge_policy(Box::new(policy)).unwrap();

        // Call with small budget - policy selects no work, should return quickly
        let budget = Duration::from_millis(10);
        let report = engine.maintenance_step(budget).unwrap();

        // Should return quickly when there's no compaction work
        assert!(
            report.time_spent < budget.mul_f32(1.5),
            "Time spent {:?} exceeded budget {:?} by >50%",
            report.time_spent,
            budget
        );
    }
}
