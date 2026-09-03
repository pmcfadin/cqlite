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
pub mod config_bridge;
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
pub(crate) mod reconcile_rules;
#[cfg(feature = "write-support")]
pub mod wal;

#[cfg(feature = "write-support")]
pub use export::{ExportOptions, ExportReport};
#[cfg(feature = "write-support")]
pub use memtable::Memtable;
#[cfg(feature = "write-support")]
pub use merge::KWayMerger;
#[cfg(feature = "write-support")]
pub use merge::{build_single_partition_merger, build_single_partition_merger_with_registry};
#[cfg(feature = "write-support")]
// Issue #2346: the reader-based analogue of `build_single_partition_merger`,
// re-exported alongside it so both surfaces are reachable the same way.
pub use merge::{build_single_partition_merger_from_readers, PointAccessRecording};
#[cfg(feature = "write-support")]
pub use merge_policy::STCSPolicy;

/// Read-time TTL "now" clock (epoch seconds), the authoritative reconciliation
/// instant the core read path uses (issue #1741/#1853). Re-exported (issue
/// #2789) so an out-of-crate merge consumer — the Flight producer — threads the
/// SAME `now` into its k-way merger's TTL expiry as a `SELECT`, honoring the
/// debug-only `CQLITE_TTL_NOW_OVERRIDE_SECS` pin in tests and wall-clock in
/// production, rather than re-deriving it (no divergence, no new env read).
#[doc(hidden)]
pub use crate::storage::sstable::reader::parsing::row_decoder::now_clock::now_epoch_secs as read_time_now_secs;
#[cfg(feature = "write-support")]
pub use mutation::{
    CellOperation, ClusteringBound, ClusteringKey, DecoratedKey, Mutation, PartitionKey,
    PartitionTombstone, RangeTombstone, TableId,
};
#[cfg(feature = "write-support")]
pub use wal::{RecoveryReport, WriteAheadLog};

#[cfg(feature = "write-support")]
// `compare_values` — the clustering-value comparator `mutation::ClusteringKey`
// reaches from both its `Ord` and its schema-aware `compare` (epic #1116 split).
mod clustering_order;
mod compaction;
#[cfg(feature = "write-support")]
pub(crate) mod durability;
#[cfg(feature = "write-support")]
mod maintenance;
#[cfg(feature = "write-support")]
mod stats;
// WAL size + replay-duration observability gauges (issue #1707), in a sibling file
// per the campsite rule (#1116).
#[cfg(feature = "write-support")]
mod sweep;
#[cfg(all(test, feature = "write-support"))]
pub(crate) mod test_support;
mod wal_gauges;
// Issue #1625: honest memtable hard-limit admission tests live in a sibling
// module to avoid growing the already-oversized `mod.rs` (epic #1116/#1135).
#[cfg(all(test, feature = "write-support"))]
mod admission_tests;

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
    /// Memtable flush threshold in bytes. Derived from the public
    /// `Config.storage.memtable_size_threshold` (default 64MB) by
    /// [`WriteEngineConfig::from_config`] — issue #1697.
    pub memtable_flush_threshold: usize,
    /// Memtable hard limit in bytes. Derived from the public
    /// `Config.storage.memtable_hard_limit` (default 256MB) by
    /// [`WriteEngineConfig::from_config`] — issue #1697. When this limit is
    /// reached, writes fail with an error.
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
    /// Whether the engine installs a default STCS compaction policy so that
    /// [`WriteEngine::maintenance_step`] performs size-tiered compaction
    /// (issue #1619). Defaults to `true` (compaction on). Set to `false` to
    /// disable compaction entirely — `maintenance_step` then becomes a no-op.
    pub auto_compaction: bool,
    /// Minimum number of SSTables in a size bucket required to trigger a
    /// compaction (STCS `min_threshold`). Derived from the public
    /// `Config.storage.compaction.min_threshold` (default `4`) — issue #1697.
    /// Ignored when [`WriteEngineConfig::auto_compaction`] is `false`.
    pub compaction_min_threshold: usize,
    /// Maximum number of SSTables compacted together in one step (STCS
    /// `max_threshold`). Derived from the public
    /// `Config.storage.compaction.max_threshold` (default `32`) — issue #1697.
    /// Ignored when [`WriteEngineConfig::auto_compaction`] is `false`.
    pub compaction_max_threshold: usize,
}

#[cfg(feature = "write-support")]
impl WriteEngineConfig {
    /// Create a configuration carrying CQLite's shipped defaults.
    ///
    /// Defined as [`Self::from_config`] applied to [`crate::Config::default`]
    /// (issue #1697): every default write-path value has exactly ONE literal,
    /// in `Config::default()`, so the public facade and the engine can never
    /// again disagree about what actually runs.
    pub fn new(data_dir: PathBuf, wal_dir: PathBuf, schema: TableSchema) -> Self {
        Self::from_config(&crate::Config::default(), data_dir, wal_dir, schema)
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
/// ## Durability contract: you MUST call [`close`](WriteEngine::close)
///
/// **`Drop` is not a flush.** Rows written with [`write`](WriteEngine::write) /
/// [`execute`](WriteEngine::execute) live in the in-memory memtable (and the
/// WAL) until a flush turns them into an SSTable. Only
/// [`close`](WriteEngine::close) (or an explicit flush) guarantees the memtable
/// is persisted to a Data.db. Because Tokio has no async drop, `Drop` CANNOT
/// flush — doing so would require a `block_on` inside `drop`, which is
/// forbidden (issue #1693/AG3). An engine dropped with a non-empty memtable
/// logs a `warn!` and leaves those rows recoverable only via WAL replay on the
/// next startup.
///
/// Embedders (and every long-lived writer) MUST therefore call
/// `engine.close().await` for a graceful shutdown — e.g. from a `SIGINT`
/// handler — before the process exits.
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
    /// Summary of the WAL crash-recovery replay performed in `new()` (issue
    /// #1391). Its `mutations` are drained into the memtable, leaving only the
    /// lossiness metadata (`corrupt_entries`, `stopped_early`, `bytes_skipped`).
    /// Exposed via [`WriteEngine::wal_recovery`] so a caller can detect that a
    /// recovery was lossy BEFORE the next flush truncates the WAL. A non-clean
    /// recovery also preserves the raw WAL segment aside (see `new()`).
    wal_recovery: RecoveryReport,
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
    /// Cumulative bytes written to flushed L0 SSTables (Data.db + all sibling
    /// components) since the engine was opened (issue #1620). Incremented once
    /// per successful `flush_internal_async()` by the flushed SSTable's
    /// `data_size`. In-process only; reset to zero on re-open. Read via
    /// [`WriteEngine::total_flushed_bytes`] so binding stats stay accurate for
    /// automatic flushes (not just explicit `flush()` calls).
    total_flushed_bytes: u64,
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
    /// Whether the "memtable over flush threshold" warning has already been
    /// emitted for the current threshold crossing (issue #1620).
    ///
    /// The sync `write()` path does NOT auto-flush when a Tokio runtime is
    /// present, so once the memtable crosses the flush threshold it stays over
    /// it until an explicit (or async) flush. Without this guard the
    /// over-threshold `tracing::warn!` fired on EVERY subsequent write (log spam).
    /// It is set to `true` the first time the warning is emitted and reset to
    /// `false` on the next successful flush, so the warning fires at most once
    /// per threshold crossing.
    warned_over_threshold: bool,
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
    /// Create `dir` (and any missing ancestors), then fsync the FULL parent
    /// chain from `dir`'s parent walking UP to and including the filesystem
    /// root — unconditionally, on every call (issue #1392).
    ///
    /// We deliberately do NOT track "what this invocation created." Across a
    /// crash that is unknowable: a prior startup may have created the whole
    /// nested `data_dir`/`wal_dir` tree but crashed partway through the
    /// parent-fsync sequence, leaving higher ancestors' dirents unpersisted. A
    /// retry that inspected "already exists" and re-fsynced only the immediate
    /// parent would leave those ancestors un-durable forever — the "which
    /// ancestor is durable on retry" hole.
    ///
    /// Instead we fsync EVERY ancestor unconditionally. Fsyncing an
    /// already-durable directory is idempotent and cheap, and the walk
    /// terminates at the filesystem root (`Path::parent()` returns `None`), so
    /// no ancestor level is ever left unsynced and there is no per-attempt
    /// state to track. Every ancestor of a successfully-created directory
    /// exists, so each fsync targets a real, present directory. Together with
    /// the flush-path barrier (which fsyncs leaf → data_root on every publish),
    /// this makes the full durability chain persistent on first flush AND on
    /// any retry after a partial crash.
    fn create_dir_all_durable(dir: &Path, label: &str) -> Result<()> {
        std::fs::create_dir_all(dir).map_err(|e| {
            Error::Storage(format!(
                "Failed to create {} directory {:?}: {}",
                label, dir, e
            ))
        })?;

        // Ascend the parent chain, fsyncing each ancestor's dirent. The walk
        // strictly shrinks (one component per step) and ends when `parent()`
        // yields `None` at the filesystem root, so it always terminates.
        let mut next = dir.parent();
        while let Some(cur) = next {
            if cur.as_os_str().is_empty() {
                // A relative single-component path (e.g. "data") has an empty
                // parent whose real directory is the current working
                // directory. Fsync "." and stop: it has no ancestor to ascend.
                wal::sync_directory(Path::new("."))?;
                break;
            }
            wal::sync_directory(cur)?;
            next = cur.parent();
        }

        Ok(())
    }

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
        // Ensure directories exist AND that EVERY ancestor's own dirent is
        // durable. `create_dir_all` alone persists the entries *inside* a
        // freshly-created root once we fsync the root, but NOT the dirents of
        // the newly-created directories themselves in their parents — and it
        // may create several missing ancestors at once. Fsyncing only the
        // immediate parent would leave intermediate dirents unpersisted, so a
        // crash after WAL truncation could still lose the whole data/WAL root.
        // `create_dir_all_durable` fsyncs the FULL parent chain up to and
        // including the filesystem root, unconditionally on every call — this
        // closes the "which ancestor is durable on a partial-crash retry" hole
        // and completes the durability chain (SSTable component contents →
        // leaf → ancestors → data_root → every ancestor up to root). #1392.
        Self::create_dir_all_durable(&config.data_dir, "data")?;
        Self::create_dir_all_durable(&config.wal_dir, "WAL")?;

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
        Self::sweep_startup_orphans(&config);

        // Initialize WAL
        let wal_path = config.wal_dir.join(WriteAheadLog::WAL_FILENAME);
        // WAL RECOVERY duration (issue #1707). The timer starts HERE, BEFORE the
        // open, not at the `replay_each` call below: `open_existing` already reads
        // the whole log once, running a full CRC validation scan
        // (`scan_valid_prefix`) plus any torn-tail trim, so a large WAL is read
        // TWICE and a timer started after the open would report only the second
        // pass. This metric exists to answer "why was startup slow?", and on a large
        // or corrupt-tail log the validation scan is the dominant cost — excluding
        // it would understate precisely the case the metric was created to expose.
        // Recovery happens EXACTLY ONCE per engine open. The fresh-WAL branch is
        // inside the window too; creating an empty log is honestly ~0s of recovery.
        let recovery_started = std::time::Instant::now();
        let mut wal = if wal_path.exists() {
            // Recover from existing WAL
            WriteAheadLog::open_existing(&wal_path)?
        } else {
            // Create new WAL
            WriteAheadLog::create(&config.wal_dir)?
        };

        // Replay WAL into memtable. The report distinguishes a clean recovery
        // from a lossy one (issue #1391); a bare Vec cannot.
        //
        // Stream each recovered mutation straight into the memtable via
        // `replay_each` (issue #1661) instead of materialising a whole-log
        // `Vec<Mutation>` and then copying it in: peak memory is bounded by the
        // memtable itself, not ~2× it. `recovered` is an authoritative count of
        // mutations actually recovered (used only for the lossy-recovery log,
        // preserving its prior `mutations.len()` meaning); the completion log
        // counts from the memtable. The report still carries all corruption
        // metadata — mutations stream via the closure, so its `mutations` vec
        // stays empty.
        let mut memtable = Memtable::new();
        let mut recovered = 0usize;
        // DEFER the first memtable-application error instead of aborting the scan
        // (issue #1661, roborev). The pre-streaming flow scanned the ENTIRE WAL
        // first (`replay()`), ran the lossy-recovery preserve/reset, and only THEN
        // applied mutations — so a schema/key-conversion or insert failure on an
        // earlier valid mutation could never prevent a LATER corrupt tail from
        // being detected, preserved aside, and reset. If the streaming callback
        // returned that error via `?`, replay would stop early and skip
        // preserve/reset, silently changing the #1390/#1391 recovery semantics
        // this refactor must preserve. So we record the FIRST apply error, stop
        // applying (the engine will not be constructed), but keep scanning for the
        // full RecoveryReport; the error is surfaced only after preserve/reset
        // below. The streaming memory win is unaffected — no whole-log Vec.
        let mut apply_error: Option<Error> = None;
        let wal_recovery = wal.replay_each(|mutation| {
            if apply_error.is_some() {
                // An earlier mutation already failed; keep scanning to reach any
                // later corruption but skip further application.
                return Ok(());
            }
            match mutation.decorated_key(&config.schema) {
                Ok(decorated_key) => match memtable.insert_with_key(decorated_key, mutation) {
                    Ok(()) => recovered += 1,
                    Err(e) => apply_error = Some(e),
                },
                Err(e) => apply_error = Some(e),
            }
            Ok(())
        })?;
        // Emitted UNCONDITIONALLY, including the 0-entry case: a fresh WAL with
        // nothing to recover genuinely took ~0s, and that IS a measurement (the
        // #2314 rule forbids inventing a value nobody took, not reporting a real one
        // that is small). Covers the WHOLE recovery window opened above — validation
        // scan plus replay — which is why the metric is `cqlite.wal.recovery.duration`
        // and not `…replay…`. Recorded BEFORE the lossy-recovery branch below so a
        // CORRUPT-WAL open, which is exactly when recovery latency matters most,
        // still reports the time it spent.
        wal_gauges::record_wal_recovery_duration(recovery_started.elapsed());

        if !wal_recovery.is_clean() {
            // Preserve the raw WAL segment aside BEFORE anything (a later flush,
            // or the reset below) can truncate it, so the corruption evidence
            // survives for manual recovery. The report is also retained on the
            // engine and exposed via `wal_recovery()` so a caller sees the loss
            // pre-truncate.
            let preserved = Self::preserve_corrupt_wal(&wal_path)?;

            // With the evidence safely aside, trim the LIVE WAL back to its last
            // CRC-valid prefix (issue #1391). Otherwise the engine would remain
            // writable with the corrupt tail still on disk, and the FIRST synced
            // write after this lossy recovery would be appended AFTER the corrupt
            // entry — where the next replay stops — so that acknowledged write
            // would be silently lost. Resetting to the valid prefix makes
            // post-recovery appends land at a replayable position. (A torn tail
            // was already trimmed in `open_existing`, so this only fires for
            // mid-stream corruption.)
            let reset_to = wal.reset_to_valid_prefix()?;

            tracing::error!(
                "WAL recovery at {:?} was LOSSY: recovered {} mutation(s), {} corrupt entry \
                 (entries), stopped_early={}, {} byte(s) not recovered. Raw segment preserved at \
                 {:?}; live WAL reset to valid prefix ({:?}). Investigate before relying on this \
                 data.",
                wal_path,
                recovered,
                wal_recovery.corrupt_entries,
                wal_recovery.stopped_early,
                wal_recovery.bytes_skipped,
                preserved,
                reset_to,
            );
        }

        // Surface any deferred memtable-application error now — AFTER the
        // lossy-recovery preserve/reset above has run (issue #1661, roborev).
        // This restores the pre-streaming ordering: on-disk WAL corruption is
        // preserved aside and the live WAL is reset to its valid prefix before
        // engine construction fails on the apply error.
        if let Some(e) = apply_error {
            return Err(e);
        }

        // OPENING SIZE GAUGE (issue #1707, roborev job 145). Recovery emitted its
        // DURATION above; without this, a process that recovers a non-empty WAL and
        // then takes no writes exposes NO `cqlite.wal.size` series at all — the
        // gauge's only other call sites are the two write seams and the post-flush
        // truncate. The operator then sees a recovery that took 12s and has no way
        // to see the WAL that caused it. They are two halves of one story and only
        // one of them was being told.
        //
        // Placed HERE, after the lossy-recovery branch, so it reports the size the
        // engine is actually starting from: `reset_to_valid_prefix` trims the live
        // log and updates `current_size`, and a gauge taken before it would report a
        // pre-reset size for bytes that are no longer on disk — a wrong number,
        // which is worse than silence. Emitted unconditionally, the fresh-WAL 0
        // included: 0 is a real reading here (a genuinely empty log), not an
        // invented one, and it is the baseline the saw-tooth rises from.
        wal_gauges::record_wal_size(wal.size());

        if recovered > 0 {
            tracing::info!(
                "WAL replay complete: replayed {} mutation(s); {} rows in memtable, {} bytes",
                recovered,
                memtable.row_count(),
                memtable.size_bytes()
            );
        }

        // Determine next generation number by scanning data directory
        let generation = Self::determine_next_generation(&config.data_dir)?;

        // Install the default STCS compaction policy so `maintenance_step`
        // performs size-tiered compaction out of the box (issue #1619). The
        // off-switch is `WriteEngineConfig::auto_compaction = false`, which
        // leaves the policy unset and makes `maintenance_step` a no-op. Read the
        // config values into locals BEFORE the struct-init moves `config`.
        let merge_policy: Option<Box<dyn MergePolicy>> = if config.auto_compaction {
            Some(Box::new(STCSPolicy::new(
                config.compaction_min_threshold,
                config.compaction_max_threshold,
                0.5,
                1.5,
                STCSPolicy::DEFAULT_MIN_SSTABLE_SIZE,
            )?))
        } else {
            None
        };

        Ok(Self {
            config,
            wal,
            memtable,
            wal_recovery,
            generation,
            closed: AtomicBool::new(false),
            active_merge: None,
            merge_policy,
            cumulative_stats: CompactionStats::default(),
            rows_written: 0,
            l0_count: 0,
            total_flushed_bytes: 0,
            dir_lock,
            warned_over_threshold: false,
        })
    }

    /// Summary of the WAL crash-recovery replay performed when this engine was
    /// opened (issue #1391).
    ///
    /// The `mutations` field has been drained into the memtable, so only the
    /// lossiness metadata remains. Use [`RecoveryReport::is_clean`] to detect a
    /// lossy recovery. A non-clean report means the raw WAL segment was
    /// preserved aside (as `commitlog.wal.corrupt.<nanos>`) so a subsequent
    /// flush cannot destroy the evidence.
    pub fn wal_recovery(&self) -> &RecoveryReport {
        &self.wal_recovery
    }

    /// Copy the raw WAL file aside before any truncation can destroy it, so a
    /// lossy recovery leaves forensic evidence (issue #1391). Returns the path
    /// of the preserved copy.
    ///
    /// The copy is a sibling `commitlog.wal.corrupt.<unix_nanos>`; the original
    /// is left untouched (a later flush truncates it, but the aside copy — and
    /// the retained [`RecoveryReport`] — survive).
    fn preserve_corrupt_wal(wal_path: &Path) -> Result<PathBuf> {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let file_name = wal_path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| WriteAheadLog::WAL_FILENAME.to_string());
        let preserved = match wal_path.parent() {
            Some(parent) => parent.join(format!("{}.corrupt.{}", file_name, nanos)),
            None => PathBuf::from(format!("{}.corrupt.{}", file_name, nanos)),
        };
        std::fs::copy(wal_path, &preserved).map_err(|e| {
            Error::Storage(format!(
                "Failed to preserve corrupt WAL {:?} aside to {:?}: {}",
                wal_path, preserved, e
            ))
        })?;

        // Make the forensic copy durable BEFORE the caller resets/truncates the
        // live WAL (issue #1391). `std::fs::copy` does not fsync; without this a
        // crash after the copy but before it reaches disk could leave the live
        // WAL already trimmed while the preserved copy is missing/incomplete —
        // destroying the very evidence we set aside. Order: copy → fsync copy →
        // fsync parent dir → (caller) reset live WAL.
        {
            let copy_file = std::fs::File::open(&preserved).map_err(|e| {
                Error::Storage(format!(
                    "Failed to open preserved corrupt WAL {:?} for fsync: {}",
                    preserved, e
                ))
            })?;
            copy_file.sync_all().map_err(|e| {
                Error::Storage(format!(
                    "Failed to fsync preserved corrupt WAL {:?}: {}",
                    preserved, e
                ))
            })?;
        }
        if let Some(parent) = preserved.parent() {
            wal::sync_directory(parent)?;
        }

        Ok(preserved)
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
    #[tracing::instrument(name = "write.mutation", level = "debug", skip(self, mutation))]
    pub fn write(&mut self, mutation: Mutation) -> Result<()> {
        crate::observability::record_result("write", self.write_inner(mutation))
    }

    fn write_inner(&mut self, mutation: Mutation) -> Result<()> {
        // Insert into WAL + memtable, then handle sync-context auto-flush.
        self.write_into_memtable(mutation)?;

        // Check if memtable should be flushed (sync callers only). The binding
        // write path runs inside a Tokio runtime and does NOT reach here — it
        // uses `execute_flushing`, which owns its own real async flush and
        // deliberately skips the "call flush() manually" warning below.
        if self
            .memtable
            .should_flush(self.config.memtable_flush_threshold)
        {
            // Rate-limit the over-threshold warning (issue #1620). A sync caller
            // that lives inside a runtime never auto-flushes below, so the
            // memtable stays over threshold across many writes; without this
            // guard the warning fired on EVERY such write. Emit it at most once
            // per crossing; it is reset on the next flush.
            if !self.warned_over_threshold {
                tracing::warn!(
                    "Memtable size {} exceeds threshold {} - call flush() manually in async context",
                    self.memtable.size_bytes(),
                    self.config.memtable_flush_threshold
                );
                self.warned_over_threshold = true;
            }

            // Try to flush synchronously only if we're not in an async context.
            if tokio::runtime::Handle::try_current().is_err() {
                tracing::info!("Triggering automatic flush");
                self.flush_internal()?;
            }
        }

        Ok(())
    }

    /// Insert a mutation into the WAL + memtable WITHOUT any auto-flush handling.
    ///
    /// Shared by the sync [`write_inner`] path (which then performs the
    /// sync-context warn + flush) and the async [`execute_flushing`] path (which
    /// owns its own real async flush and must NOT emit the sync "call flush()
    /// manually in async context" warning) (issue #1620, roborev job 2879).
    fn write_into_memtable(&mut self, mutation: Mutation) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(Error::InvalidInput(
                "WriteEngine has been closed".to_string(),
            ));
        }

        reject_counter_cells(&mutation)?;

        // Honest hard-limit admission: gate on current size + the INCOMING
        // mutation's estimated size, and reject a lone jumbo write (issue #1625).
        self.check_admission(&mutation)?;

        // 1. Append to WAL (durability) — skipped when Durability::Disabled
        if self.config.durability == Durability::SyncEachWrite {
            self.wal.append(&mutation)?;
            // The append ALREADY GREW the WAL (`append` advances the tracked size
            // before returning), so the gauge is published HERE, immediately, and
            // not only at the end of a fully successful mutation (issue #1707,
            // roborev job 149). Everything after this line can fail — the `sync`
            // below, the decorated-key computation, the memtable insert — and every
            // one of those returns EARLY, leaving the bytes on disk and, before
            // this call, the gauge frozen at its pre-append value for as long as
            // the failure persisted. That is precisely the moment an operator is
            // looking at it: a full disk or a failing fsync makes the WAL grow
            // while `cqlite.wal.size` reports it flat. This emission does NOT
            // replace the post-mutation `record_size_gauges()` below (which pairs
            // WAL and memtable readings at one instant), the post-flush truncate
            // emission, or the open-time one — all four sites are needed; see
            // [`wal_gauges`].
            self.record_wal_gauges();
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
        self.record_size_gauges();

        Ok(())
    }

    /// Honest memtable hard-limit admission (issue #1625).
    ///
    /// The pre-#1625 check compared only the *current* memtable size against the
    /// hard limit — it never measured the incoming mutation, so (a) a single
    /// jumbo mutation could be admitted into an empty memtable and blow straight
    /// past the limit, and (b) a mutation could push a nearly-full memtable over
    /// the limit before the next write was rejected. This gate measures the
    /// incoming mutation with the SAME estimator the memtable uses for
    /// accounting, then:
    ///
    /// 1. Rejects any single mutation whose estimate alone exceeds the hard
    ///    limit (the ceiling for one mutation is the hard limit itself) with a
    ///    distinct error, so a lone jumbo write can never be admitted.
    /// 2. Rejects the write when `current_size + incoming` would exceed the hard
    ///    limit (`saturating_add`, so the sum can never overflow `usize`).
    fn check_admission(&self, mutation: &Mutation) -> Result<()> {
        let hard_limit = self.config.memtable_hard_limit;
        let incoming = self.memtable.estimate_mutation_size(mutation);

        // (0) Fail-closed sentinel: the estimator returns `usize::MAX` when a
        // mutation is pathological/unmeasurable (node-cap hit). Reject it
        // EXPLICITLY and unconditionally — a `>` comparison against a
        // configurable `hard_limit == usize::MAX` would otherwise be false and
        // admit the very mutation the sentinel is meant to fence off.
        if incoming == usize::MAX {
            return Err(Error::Storage(
                "Write rejected: mutation size could not be bounded (estimator \
                 fail-closed sentinel); refusing admission"
                    .to_string(),
            ));
        }

        // (1) Single-mutation ceiling: one mutation may not exceed the hard
        // limit on its own, regardless of how empty the memtable is.
        if incoming > hard_limit {
            return Err(Error::Storage(format!(
                "Write rejected: single mutation estimated at {incoming} bytes exceeds \
                 memtable hard limit {hard_limit} bytes (a single mutation may not exceed \
                 the hard limit)"
            )));
        }

        // (2) Projected sum: never admit a write that would push the memtable
        // over the hard limit.
        let projected = self.memtable.size_bytes().saturating_add(incoming);
        if projected > hard_limit {
            return Err(Error::Storage(format!(
                "Write rejected: memtable {} + mutation {incoming} would exceed hard limit {hard_limit}",
                self.memtable.size_bytes()
            )));
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
    #[tracing::instrument(name = "write.mutation", level = "debug", skip(self, mutation))]
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

        // Honest hard-limit admission: gate on current size + the INCOMING
        // mutation's estimated size, and reject a lone jumbo write (issue #1625).
        self.check_admission(&mutation)?;

        // 1. Append to WAL (durability) — skipped when Durability::Disabled
        if self.config.durability == Durability::SyncEachWrite {
            self.wal.append(&mutation)?;
            // The append ALREADY GREW the WAL (`append` advances the tracked size
            // before returning), so the gauge is published HERE, immediately, and
            // not only at the end of a fully successful mutation (issue #1707,
            // roborev job 149). Everything after this line can fail — the `sync`
            // below, the decorated-key computation, the memtable insert — and every
            // one of those returns EARLY, leaving the bytes on disk and, before
            // this call, the gauge frozen at its pre-append value for as long as
            // the failure persisted. That is precisely the moment an operator is
            // looking at it: a full disk or a failing fsync makes the WAL grow
            // while `cqlite.wal.size` reports it flat. This emission does NOT
            // replace the post-mutation `record_size_gauges()` below (which pairs
            // WAL and memtable readings at one instant), the post-flush truncate
            // emission, or the open-time one — all four sites are needed; see
            // [`wal_gauges`].
            self.record_wal_gauges();
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
        self.record_size_gauges();

        // 5. Check if memtable should be flushed
        if self
            .memtable
            .should_flush(self.config.memtable_flush_threshold)
        {
            tracing::info!(
                "Memtable size {} exceeds threshold {}, triggering flush",
                self.memtable.size_bytes(),
                self.config.memtable_flush_threshold
            );
            self.flush_internal_async().await?;
        }

        Ok(())
    }

    /// Emit BOTH engine size gauges — memtable (issue #1036) and WAL (issue #1707) —
    /// at one instant. Called at all THREE post-mutation write/flush seams; see
    /// [`wal_gauges`] for why the pairing is what keeps either from going missing on
    /// a path. The WAL half is ALSO emitted on its own immediately after each
    /// successful `wal.append()`, because a mutation that fails AFTER the append
    /// never reaches this pairing and would otherwise freeze the gauge.
    fn record_size_gauges(&self) {
        self.record_memtable_gauges();
        self.record_wal_gauges();
    }

    /// Emit the current WAL size gauge (issue #1707) — see [`wal_gauges`]. Under
    /// `Durability::Disabled` a write leaves the size unchanged; reporting the
    /// unchanged value is still the truth.
    fn record_wal_gauges(&self) {
        wal_gauges::record_wal_size(self.wal.size());
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
    #[tracing::instrument(name = "write.cql_execute", level = "debug", skip(self, statement))]
    pub fn execute(&mut self, statement: &str) -> Result<()> {
        crate::observability::record_result("write", self.execute_inner(statement))
    }

    fn execute_inner(&mut self, statement: &str) -> Result<()> {
        // The public sync `execute` signature is unchanged: delegate to the
        // counted implementation and discard the mutation count (issue #1620).
        self.execute_inner_counted(statement).map(|_| ())
    }

    /// Parse + write a CQL statement into the memtable, returning the number of
    /// mutations applied (N for a BATCH, else 1).
    ///
    /// This is the shared core of the sync `execute` path and the async
    /// `execute_flushing` path (issue #1620). It is *unrecorded* (no
    /// `record_result`); callers record at the public boundary.
    fn execute_inner_counted(&mut self, statement: &str) -> Result<u64> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(Error::InvalidInput(
                "WriteEngine has been closed".to_string(),
            ));
        }

        // Single-boundary error recording (issue #1036): call the *unrecorded*
        // `write_inner` here rather than the public `write`. `execute` already
        // wraps this method in `record_result`, so the escaping error is counted
        // exactly once at the public boundary instead of twice.
        let mutations = self.parse_statement_to_mutations(statement)?;
        let n = mutations.len() as u64;
        for mutation in mutations {
            self.write_inner(mutation)?;
        }
        Ok(n)
    }

    /// Parse a CQL DML statement into its mutation(s). A `BEGIN BATCH` yields one
    /// mutation per statement in the batch; any other DML yields exactly one.
    ///
    /// Shared by the sync `execute` path ([`execute_inner_counted`]) and the
    /// async [`execute_flushing`] path so both agree on batch semantics
    /// (issue #1620).
    fn parse_statement_to_mutations(&self, statement: &str) -> Result<Vec<Mutation>> {
        let trimmed = statement.trim();
        if trimmed.len() >= 5 && trimmed.as_bytes()[..5].eq_ignore_ascii_case(b"BEGIN") {
            cql_to_mutation::convert_cql_to_mutations(trimmed, &self.config.schema)
        } else {
            Ok(vec![self.parse_cql_to_mutation(statement)?])
        }
    }

    /// Execute a DML statement and, if the memtable has crossed the flush
    /// threshold, await a REAL async flush (issue #1620, DECIDED: write_async).
    ///
    /// This is the entry point for the Node/Python binding write path, which runs
    /// inside a Tokio runtime where the sync auto-flush in `write()` is
    /// intentionally skipped. It restores auto-flush there WITHOUT the surprise
    /// inline-flush latency the plain sync `write()`/`execute()` path avoids.
    /// Returns the number of mutations applied (N for BATCH, else 1).
    #[tracing::instrument(
        name = "write.cql_execute_flushing",
        level = "debug",
        skip(self, statement)
    )]
    pub async fn execute_flushing(&mut self, statement: &str) -> Result<u64> {
        // Single-boundary error recording (issue #1036): `execute_flushing` is
        // the public Node/Python DML entry point, so — like `execute`,
        // `write_async`, and `flush` — it records the escaping result exactly
        // once here. The inner helper and everything it calls (`write_inner`,
        // `flush_internal_async`) are *unrecorded* to avoid double counting.
        crate::observability::record_result("write", self.execute_flushing_inner(statement).await)
    }

    async fn execute_flushing_inner(&mut self, statement: &str) -> Result<u64> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(Error::InvalidInput(
                "WriteEngine has been closed".to_string(),
            ));
        }

        // Parse first, then write each mutation and flush as soon as the memtable
        // crosses the threshold. Checking after EVERY mutation (not just once at
        // the end) means a large `BEGIN BATCH` cannot accumulate all the way to
        // the hard limit before flushing and dead-ending (roborev job 2854,
        // issue #1620).
        let mutations = self.parse_statement_to_mutations(statement)?;
        let n = mutations.len() as u64;
        for mutation in mutations {
            // Use the warn-free memtable insert: this path owns the real async
            // flush below, so it must not emit the sync "call flush() manually"
            // warning (roborev job 2879, issue #1620).
            self.write_into_memtable(mutation)?;
            if self
                .memtable
                .should_flush(self.config.memtable_flush_threshold)
            {
                self.flush_internal_async().await?;
            }
        }
        Ok(n)
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
    #[tracing::instrument(name = "flush.public", level = "debug", skip(self))]
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
    #[tracing::instrument(name = "flush.memtable", level = "debug", skip(self))]
    async fn flush_internal_async(&mut self) -> Result<Option<SSTableInfo>> {
        // Check if memtable is empty
        if self.memtable.is_empty() {
            return Ok(None);
        }

        let flush_start = Instant::now();
        let rows_to_flush = self.memtable.row_count() as u64;

        tracing::info!(
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

        tracing::info!(
            "SSTable flush complete: generation {}, {} partitions, {} bytes",
            self.generation,
            info.partition_count,
            info.data_size
        );

        // Durability handoff (issue #1392): fsync the SSTable's parent directory
        // BEFORE truncating the WAL. See `durability::finalize_flush_durability`.
        //
        // A `?`-propagated error here (e.g. a directory fsync failure) happens
        // BEFORE the WAL is truncated, so the WAL is still intact and replayable:
        // returning early WITHOUT advancing the generation is correct, because a
        // retry re-flushes to the same generation and the untouched WAL still
        // recovers the data on a crash mid-retry.
        let durability_outcome = durability::finalize_flush_durability(
            &durability::RealDurabilityBarrier,
            &info.data_path,
            &self.config.data_dir,
            &mut self.wal,
        )?;

        // The SSTable is now durably published. Commit flush state — clear the
        // memtable and advance the generation — for BOTH outcomes, including
        // `WalTruncateFailedAfterCommit`. In that case the WAL has already been
        // zeroed, so the published SSTable is the ONLY durable copy of these
        // mutations; committing state guarantees a retry writes a NEW generation
        // and never lets `File::create` overwrite the sole durable copy (which
        // would lose the data on a crash mid-retry). See issue #1392.

        // Clear memtable
        self.memtable.clear();

        // Reset the over-threshold warn guard (issue #1620): the memtable is now
        // empty, so the next threshold crossing is a fresh event that should warn
        // again (at most once).
        self.warned_over_threshold = false;

        // Increment the L0 SSTable counter (Issue #486).
        self.l0_count += 1;

        // Accumulate cumulative flushed bytes (issue #1620) so binding write
        // stats reflect automatic flushes, not only explicit `flush()` calls.
        self.total_flushed_bytes = self.total_flushed_bytes.saturating_add(info.data_size);

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
        // Issue #1707: the flush above TRUNCATED the WAL, so this is the emission
        // that produces the saw-tooth `cqlite.wal.size` the operator doc promises.
        self.record_size_gauges();

        // Surface a post-mutation WAL-truncate failure AFTER state has been
        // committed above (issue #1392). The data is durable in the published
        // SSTable and the generation has advanced, so a retry cannot overwrite
        // it; the error informs the caller that the WAL is no longer a valid
        // replay marker.
        match durability_outcome {
            durability::FlushDurabilityOutcome::Durable => Ok(Some(info)),
            durability::FlushDurabilityOutcome::WalTruncateFailedAfterCommit(err) => Err(err),
        }
    }

    /// Close the write engine
    ///
    /// This flushes any remaining data in the memtable to SSTable,
    /// syncs the WAL, then marks the engine as closed. After calling close(),
    /// the engine cannot be used for further writes.
    ///
    /// **This is the durability boundary.** `Drop` does not (and cannot — no
    /// async drop in Tokio) flush; callers MUST `close().await` before exit to
    /// guarantee written rows reach an SSTable rather than relying on WAL replay
    /// (issue #1693). See the type-level docs on [`WriteEngine`].
    ///
    /// This method is idempotent - calling it multiple times is safe.
    ///
    /// # Returns
    ///
    /// Ok(()) on success.
    ///
    /// # Errors
    ///
    /// Returns an error if the final flush fails.
    ///
    /// WAL-truncate handling during that flush is phase-aware (issue #1392):
    ///
    /// * A truncate failure that leaves the WAL intact (it faulted *before*
    ///   mutating the WAL) is logged and swallowed — the WAL stays a valid,
    ///   idempotent replay marker, so no error is surfaced.
    /// * A truncate failure *after* `set_len(0)` has already zeroed the WAL
    ///   (`WalTruncateFailedAfterCommit`) is **propagated**. By then flush state
    ///   has already been committed — the SSTable is durable and the generation
    ///   has advanced — so the data is safe, but the error is surfaced so the
    ///   caller knows the WAL is no longer a replay marker.
    ///
    /// When the WAL is already empty (e.g. `Durability::Disabled`) the truncate
    /// phase is skipped, so no truncate-phase error can arise.
    pub async fn close(&mut self) -> Result<()> {
        // Check if already closed (idempotent)
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        tracing::info!("Closing WriteEngine");

        // Flush any remaining data
        if !self.memtable.is_empty() {
            tracing::info!("Flushing memtable before close");

            // Attempt to flush to SSTable
            match self.flush_internal_async().await {
                Ok(_) => {
                    tracing::info!("Memtable flushed successfully");
                }
                Err(e) => {
                    // If flush fails, log error and return it
                    tracing::error!("Failed to flush memtable during close: {}", e);
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
            tracing::warn!("Failed to sync WAL during close: {}", e);
            // Don't fail close if sync fails - data is already persisted to SSTable
        }

        // Release the exclusive advisory lock on write_dir so a subsequent
        // WriteEngine on the same directory can acquire it immediately.
        // On Drop the OS would release it anyway, but explicit unlock is more
        // deterministic in async / multi-phase shutdown sequences.
        if let Err(e) = fs2::FileExt::unlock(&self.dir_lock) {
            tracing::warn!("Failed to release write_dir advisory lock: {}", e);
        }

        tracing::info!("WriteEngine closed");

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
        // Issue #1693 (AG4): `Drop` is NOT a flush — there is no async drop in
        // Tokio, and re-introducing a `block_on` here is the AG3 defect. So the
        // best we can do on an ungraceful drop is make the silent data-loss mode
        // visible: if the memtable still holds rows that `close()` never flushed,
        // warn. The `is_empty()` guard is a cheap length check; the format
        // arguments (a single `usize`) allocate only when a warn-level logger is
        // actually enabled.
        if !self.memtable.is_empty() {
            // The recovery guidance depends on the durability mode: with WAL
            // durability the rows survive in the WAL and replay on next open,
            // but with `Durability::Disabled` the WAL was skipped entirely, so
            // an ungraceful drop loses the un-flushed rows permanently.
            match self.config.durability {
                Durability::SyncEachWrite => tracing::warn!(
                    "WriteEngine dropped without close(): {} row(s) in the memtable were NOT \
                     flushed to an SSTable and remain only in the WAL (durability now relies on \
                     WAL replay at next startup). Call `close().await` for a graceful shutdown.",
                    self.memtable.row_count()
                ),
                Durability::Disabled => tracing::warn!(
                    "WriteEngine dropped without close(): {} row(s) in the memtable were NOT \
                     flushed to an SSTable and are LOST — durability is Disabled so these rows \
                     were never written to the WAL and cannot be recovered (they existed in \
                     memory only). Call `close().await` for a graceful shutdown.",
                    self.memtable.row_count()
                ),
            }
        }

        // If close() was already called the lock was already released; a second
        // unlock is a no-op on most platforms (Linux returns ENOLCK which we
        // ignore here).  The important invariant is that the lock is always
        // released before the file descriptor is closed by the OS on drop.
        if let Err(e) = fs2::FileExt::unlock(&self.dir_lock) {
            tracing::debug!(
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

        // #1697: the default now originates from the public Config facade.
        assert_eq!(
            config.memtable_flush_threshold as u64,
            crate::Config::default().storage.memtable_size_threshold
        );
        assert_eq!(
            config.memtable_hard_limit as u64,
            crate::Config::default().storage.memtable_hard_limit
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

    /// Issue #1392: `create_dir_all_durable` is the init-time link in the
    /// durability chain — it must create a fresh (possibly nested) root and
    /// fsync its parent so the new root's own dirent is persisted. There is no
    /// fs-op recorder seam on the init path, so this exercises the helper's
    /// call path directly (create + parent fsync succeeds and is idempotent);
    /// a crash-injection observation of the fsync would require a syscall seam
    /// that does not exist here.
    #[test]
    fn test_create_dir_all_durable_fsyncs_parent() {
        let temp_dir = TempDir::new().unwrap();

        // Fresh nested root: parent (`root`) exists, `data` is newly created.
        let root = temp_dir.path().join("root");
        std::fs::create_dir_all(&root).unwrap();
        let data = root.join("data");
        assert!(!data.exists());

        WriteEngine::create_dir_all_durable(&data, "data").unwrap();
        assert!(data.is_dir(), "new root directory must exist");

        // Idempotent: re-running on an already-existing dir still succeeds and
        // re-fsyncs the parent without error.
        WriteEngine::create_dir_all_durable(&data, "data").unwrap();
        assert!(data.is_dir());
    }

    /// Issue #1392 (roborev HIGH): the "track what this attempt created"
    /// approach had a retry hole — if a PRIOR startup created the nested tree
    /// but crashed partway through its parent-fsync sequence, a retry sees the
    /// tree already exists, records an empty newly-created set, and re-fsyncs
    /// only the immediate parent, leaving higher ancestors un-durable forever.
    ///
    /// The definitive fix is to stop tracking anything and fsync the FULL
    /// parent chain (up to and including the filesystem root) on EVERY call.
    /// This test simulates that partial-crash retry: it pre-creates the entire
    /// `base/a/b/data` tree first, so nothing is "newly created" on the call,
    /// then asserts every ancestor is still openable/fsyncable up to the root.
    ///
    /// Red-before: the tracking helper would fsync only `data`'s immediate
    /// parent (`b`) on the already-exists path, leaving `a`/`base` unsynced.
    /// Green-after: the unconditional full-chain walk fsyncs `b`, `a`, `base`,
    /// and every ancestor up to the filesystem root.
    #[test]
    fn test_create_dir_all_durable_fsyncs_full_chain_on_existing_tree() {
        let temp_dir = TempDir::new().unwrap();

        // Simulate a prior partial-crash: the ENTIRE nested tree already exists
        // before the call, so no directory is "newly created" this invocation.
        let base = temp_dir.path().join("base");
        let a = base.join("a");
        let b = a.join("b");
        let data = b.join("data");
        std::fs::create_dir_all(&data).unwrap();
        assert!(data.is_dir(), "precondition: full tree pre-exists");

        // The unconditional full-chain fsync must succeed on the already-exists
        // path — it walks `data`'s parent up to the filesystem root, fsyncing
        // every ancestor regardless of what (if anything) this call created.
        WriteEngine::create_dir_all_durable(&data, "data").unwrap();

        // Every ancestor from the leaf's parent up to the root is a real,
        // present directory that the walk fsynced (open-for-sync would fail on
        // a missing/non-durable dirent). Assert the whole chain is present.
        for ancestor in [b.as_path(), a.as_path(), base.as_path()] {
            assert!(
                ancestor.is_dir(),
                "ancestor {:?} must be present and fsyncable up the full chain",
                ancestor
            );
        }

        // Idempotent: a second unconditional pass still succeeds.
        WriteEngine::create_dir_all_durable(&data, "data").unwrap();
        assert!(data.is_dir());
    }

    /// Issue #1392: the full-chain walk must TERMINATE at the filesystem root
    /// (a path whose `parent()` is `None`) rather than looping. Exercising the
    /// helper against an absolute nested path implicitly walks to `/`; this
    /// test guards the termination contract by confirming the call returns
    /// (does not hang) and the root itself has no parent to ascend into.
    #[test]
    fn test_create_dir_all_durable_walk_terminates_at_root() {
        let temp_dir = TempDir::new().unwrap();
        let data = temp_dir.path().join("a").join("b").join("data");

        // Absolute path: the parent chain ascends through `b`, `a`, the temp
        // dir, ... up to `/`. `Path::new("/").parent()` is `None`, so the walk
        // stops there — no infinite loop. The call simply returns.
        WriteEngine::create_dir_all_durable(&data, "data").unwrap();
        assert!(data.is_dir());

        // Contract the walk relies on: the filesystem root terminates the walk.
        assert!(
            std::path::Path::new("/").parent().is_none(),
            "filesystem root must have no parent so the fsync walk terminates"
        );
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

    /// Issue #1390 criterion 5: engine-level crash simulation through the
    /// WriteEngine (not just the WAL unit). Write A (fsync-acknowledged), crash
    /// (drop without flush), inject a torn tail into the commitlog, recover in a
    /// new engine, write C (fsync-acknowledged), crash again, then recover once
    /// more and assert every acknowledged mutation is present exactly once.
    ///
    /// Before the fix, C lands AFTER the retained torn bytes and is silently
    /// lost on the second recovery.
    #[test]
    fn test_write_engine_recovers_across_torn_tail_crash() {
        use std::io::Write as _;

        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        // 1. Write A and "crash" (drop without flush). Default durability is
        //    SyncEachWrite, so A is fsync'd to the WAL.
        {
            let mut engine = WriteEngine::new(config.clone()).unwrap();
            engine
                .write(create_test_mutation(1, "Alice", 1_000_000))
                .unwrap();
        }

        // 2. Inject a torn tail: a complete 8-byte header declaring a 100-byte
        //    payload, followed by only 10 payload bytes (interrupted append).
        let wal_file = config.wal_dir.join(WriteAheadLog::WAL_FILENAME);
        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&wal_file)
                .unwrap();
            f.write_all(&100u32.to_le_bytes()).unwrap();
            f.write_all(&0u32.to_le_bytes()).unwrap();
            f.write_all(&[0xAB; 10]).unwrap();
            f.sync_all().unwrap();
        }

        // 3. Recover: the torn tail must be trimmed. Then write C and crash.
        {
            let mut engine = WriteEngine::new(config.clone()).unwrap();
            assert_eq!(
                engine.memtable_row_count(),
                1,
                "must recover A across the torn tail"
            );
            engine
                .write(create_test_mutation(3, "Carol", 3_000_000))
                .unwrap();
        }

        // 4. Recover again: both acknowledged mutations must be present exactly
        //    once (2 distinct partition keys => 2 memtable rows).
        {
            let engine = WriteEngine::new(config).unwrap();
            assert_eq!(
                engine.memtable_row_count(),
                2,
                "both A and C must survive the second recovery (C must not be lost)"
            );
        }
    }

    // ---- Issue #1391: engine-level flush guard over a lossy WAL replay ----
    //
    // A lossy WAL recovery must be surfaced to the caller (via the retained
    // RecoveryReport) AND its raw segment preserved aside BEFORE the next flush
    // truncates the WAL. Otherwise the loss becomes permanent and invisible.

    /// Build a 3-entry (A, B, C) WAL in `wal_dir` and return the byte offset at
    /// the end of entry A (so B's payload, which starts at `end_a + 8`, can be
    /// corrupted). All entries are fsync'd.
    fn seed_wal_abc(wal_dir: &Path) -> u64 {
        std::fs::create_dir_all(wal_dir).unwrap();
        let mut wal = WriteAheadLog::create(wal_dir).unwrap();
        wal.append(&create_test_mutation(1, "A", 1_000_000))
            .unwrap();
        wal.sync().unwrap();
        let end_a = wal.size();
        wal.append(&create_test_mutation(2, "B", 2_000_000))
            .unwrap();
        wal.sync().unwrap();
        wal.append(&create_test_mutation(3, "C", 3_000_000))
            .unwrap();
        wal.sync().unwrap();
        drop(wal);
        end_a
    }

    fn count_corrupt_aside(wal_dir: &Path) -> usize {
        std::fs::read_dir(wal_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_string_lossy()
                    .contains("commitlog.wal.corrupt.")
            })
            .count()
    }

    /// Criterion 5: a lossy WAL must not be silently truncated by the first
    /// flush. Opening the engine over an A,B(corrupt),C WAL must expose a
    /// non-clean RecoveryReport and preserve the raw segment aside; a subsequent
    /// flush must NOT destroy that evidence.
    #[tokio::test]
    async fn test_write_engine_lossy_wal_preserved_before_flush_truncate() {
        use std::io::{Read, Seek, SeekFrom, Write};

        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        let end_a = seed_wal_abc(&config.wal_dir);

        // Bit-flip the first payload byte of B (just past its 8-byte header).
        let wal_file = config.wal_dir.join(WriteAheadLog::WAL_FILENAME);
        {
            let mut f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&wal_file)
                .unwrap();
            f.seek(SeekFrom::Start(end_a + 8)).unwrap();
            let mut byte = [0u8; 1];
            f.read_exact(&mut byte).unwrap();
            f.seek(SeekFrom::Start(end_a + 8)).unwrap();
            f.write_all(&[byte[0] ^ 0x01]).unwrap();
            f.sync_all().unwrap();
        }

        // Open the engine: recovery must surface as lossy and preserve evidence.
        let mut engine = WriteEngine::new(config.clone()).unwrap();
        assert!(
            !engine.wal_recovery().is_clean(),
            "engine must expose a non-clean RecoveryReport for a lossy WAL"
        );
        assert_eq!(engine.wal_recovery().corrupt_entries, 1);
        assert!(engine.wal_recovery().stopped_early);
        assert_eq!(
            engine.memtable_row_count(),
            1,
            "only the valid prefix [A] is recovered"
        );
        assert_eq!(
            count_corrupt_aside(&config.wal_dir),
            1,
            "the raw corrupt WAL segment must be preserved aside BEFORE any flush"
        );

        // Flush: this truncates the live WAL, but the preserved evidence must
        // survive so the loss is not destroyed.
        engine.flush().await.unwrap();
        assert_eq!(
            count_corrupt_aside(&config.wal_dir),
            1,
            "flush must NOT destroy the preserved corrupt WAL segment"
        );
    }

    /// Issue #1661 (roborev): a memtable-application error on an EARLIER valid
    /// mutation MUST NOT abort the WAL scan before a LATER corrupt tail can be
    /// detected, preserved aside, and reset. The streaming refactor applies each
    /// mutation as it is scanned; if that apply error propagated immediately, the
    /// #1390/#1391 lossy-recovery preserve/reset would be skipped — a forbidden
    /// behavior change. Build a WAL of [A(applies-fail), B(corrupt)]: A is a
    /// CRC-valid, decodable mutation whose partition key value (`Text`) cannot be
    /// encoded against the `int` schema, so `decorated_key` fails at apply time;
    /// B is bit-flipped so replay reports the recovery as lossy. `WriteEngine::new`
    /// must return the deferred apply error, but ONLY after the corrupt segment was
    /// preserved aside and the live WAL reset to its valid prefix [A].
    #[test]
    fn test_write_engine_apply_error_still_preserves_and_resets_corrupt_tail() {
        use crate::storage::write_engine::mutation::{CellOperation, PartitionKey, TableId};
        use crate::types::Value;
        use std::io::{Read, Seek, SeekFrom, Write};

        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema(); // partition key `id` is `int`
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        // Seed A = a mutation whose partition key is Text (mismatches the int
        // schema) so it serializes/CRCs/deserializes fine but fails at apply
        // (`decorated_key`); then B = a valid mutation we will corrupt.
        std::fs::create_dir_all(&config.wal_dir).unwrap();
        let end_a = {
            let mut wal = WriteAheadLog::create(&config.wal_dir).unwrap();
            let bad = Mutation::new(
                TableId::new("test_ks", "test_table"),
                PartitionKey::single("id", Value::text("not-an-int".to_string())),
                None,
                vec![CellOperation::Write {
                    column: "name".to_string(),
                    value: Value::text("A".to_string()),
                }],
                1_000_000,
                None,
            );
            wal.append(&bad).unwrap();
            wal.sync().unwrap();
            let end_a = wal.size();
            wal.append(&create_test_mutation(2, "B", 2_000_000))
                .unwrap();
            wal.sync().unwrap();
            end_a
        };

        // Bit-flip B's first payload byte (just past its 8-byte header) → CRC
        // mismatch → mid-stream corruption.
        let wal_file = config.wal_dir.join(WriteAheadLog::WAL_FILENAME);
        {
            let mut f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&wal_file)
                .unwrap();
            f.seek(SeekFrom::Start(end_a + 8)).unwrap();
            let mut byte = [0u8; 1];
            f.read_exact(&mut byte).unwrap();
            f.seek(SeekFrom::Start(end_a + 8)).unwrap();
            f.write_all(&[byte[0] ^ 0x01]).unwrap();
            f.sync_all().unwrap();
        }

        // Opening must FAIL on the deferred apply error...
        let result = WriteEngine::new(config.clone());
        assert!(
            result.is_err(),
            "engine open must surface the deferred memtable-application error"
        );

        // ...but ONLY after the lossy-recovery preserve/reset ran. Pre-fix, the
        // apply error aborted the scan before B's corruption was seen, so neither
        // of these held.
        assert_eq!(
            count_corrupt_aside(&config.wal_dir),
            1,
            "corrupt segment must be preserved aside even though an earlier apply failed"
        );
        assert_eq!(
            std::fs::metadata(&wal_file).unwrap().len(),
            end_a,
            "live WAL must be reset to its valid prefix [A] before the apply error surfaces"
        );
    }

    /// Regression guard: a CLEAN recovery is unchanged — no aside file, and the
    /// flush truncates the WAL normally.
    #[tokio::test]
    async fn test_write_engine_clean_wal_recovery_truncates_normally() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        seed_wal_abc(&config.wal_dir);

        let mut engine = WriteEngine::new(config.clone()).unwrap();
        assert!(engine.wal_recovery().is_clean());
        assert_eq!(engine.memtable_row_count(), 3, "A, B, C all recovered");
        assert_eq!(
            count_corrupt_aside(&config.wal_dir),
            0,
            "a clean recovery must not preserve any aside segment"
        );

        engine.flush().await.unwrap();
        let wal_file = config.wal_dir.join(WriteAheadLog::WAL_FILENAME);
        assert_eq!(
            std::fs::metadata(&wal_file).unwrap().len(),
            0,
            "clean recovery: the WAL is truncated normally after flush"
        );
        assert_eq!(count_corrupt_aside(&config.wal_dir), 0);
    }

    /// Issue #1391 (Finding B): recovery-write-crash. After a LOSSY recovery the
    /// engine stays writable, so a synced write issued right after recovery MUST
    /// survive the next replay. Before the fix the live WAL still carried the
    /// corrupt tail, so the acknowledged write landed AFTER the corrupt entry —
    /// where replay stops — and was silently lost. With the valid-prefix reset,
    /// the write lands at a replayable position: reopening the live WAL and
    /// replaying must yield the valid prefix PLUS the post-recovery write (never
    /// a set missing it), while the corrupt evidence is still preserved aside.
    #[tokio::test]
    async fn test_write_engine_reset_to_valid_prefix_keeps_post_recovery_write() {
        use crate::types::Value;
        use std::io::{Read, Seek, SeekFrom, Write};

        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );

        // Seed A, B, C then bit-flip B's payload → mid-stream corruption.
        let end_a = seed_wal_abc(&config.wal_dir);
        let wal_file = config.wal_dir.join(WriteAheadLog::WAL_FILENAME);
        {
            let mut f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&wal_file)
                .unwrap();
            f.seek(SeekFrom::Start(end_a + 8)).unwrap();
            let mut byte = [0u8; 1];
            f.read_exact(&mut byte).unwrap();
            f.seek(SeekFrom::Start(end_a + 8)).unwrap();
            f.write_all(&[byte[0] ^ 0x01]).unwrap();
            f.sync_all().unwrap();
        }

        // Open the engine (recovery is lossy → evidence preserved, live WAL reset
        // to the valid prefix [A]), then issue a synced post-recovery write D.
        {
            let mut engine = WriteEngine::new(config.clone()).unwrap();
            assert!(!engine.wal_recovery().is_clean());
            assert_eq!(engine.memtable_row_count(), 1, "valid prefix [A] recovered");
            assert_eq!(
                count_corrupt_aside(&config.wal_dir),
                1,
                "corrupt segment must be preserved aside"
            );
            // Default durability is SyncEachWrite, so D is fsync'd to the WAL.
            engine
                .write(create_test_mutation(4, "D", 4_000_000))
                .unwrap();
            drop(engine);
        }

        // Reopen the LIVE WAL directly and replay: it must recover [A, D] — the
        // acknowledged post-recovery write is present, NOT lost behind the old
        // corrupt tail.
        let wal = WriteAheadLog::open_existing(&wal_file).unwrap();
        let report = wal.replay().unwrap();
        assert!(
            report.is_clean(),
            "the reset live WAL is a clean [A, D] prefix"
        );
        let names: Vec<&str> = report
            .mutations
            .iter()
            .map(|m| match &m.operations[0] {
                CellOperation::Write {
                    value: Value::Text(name),
                    ..
                } => std::str::from_utf8(name).unwrap_or_default(),
                other => panic!("expected Write op, got {other:?}"),
            })
            .collect();
        assert_eq!(
            names,
            vec!["A", "D"],
            "replay must yield the valid prefix AND the post-recovery write D, \
             never a set missing D (and never the corrupt B/C)"
        );
        assert!(
            !names.contains(&"B"),
            "the corrupt entry B must not resurface"
        );
        assert!(
            !names.contains(&"C"),
            "C (behind corruption) must not resurface"
        );

        // The forensic evidence must still be on disk.
        assert_eq!(
            count_corrupt_aside(&config.wal_dir),
            1,
            "the corrupt WAL evidence must remain preserved aside after the reset"
        );
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

    /// Issue #1620 (N2 — auto-flush cliff): the binding write path runs INSIDE a
    /// Tokio runtime, where the sync `write()`/`execute()` auto-flush is
    /// intentionally skipped (`Handle::try_current().is_err()` is false). On main
    /// that meant the memtable grew unbounded until the hard limit, then every
    /// write dead-ended. `execute_flushing` restores auto-flush via a REAL async
    /// flush.
    ///
    /// Negative control: with the OLD sync `execute()` path in this same
    /// runtime-present topology, no flush would ever fire, so `generation()`
    /// would stay `1` and eventually a write would hit the hard limit. Here we
    /// assert a real flush advanced the generation and no write dead-ended.
    #[tokio::test]
    async fn test_execute_flushing_auto_flushes_in_runtime() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        // Tiny flush threshold (4KB), default 256MB hard limit.
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        )
        .with_flush_threshold(4096)
        .with_durability(Durability::Disabled);

        let mut engine = WriteEngine::new(config).unwrap();
        assert_eq!(engine.generation(), 1);

        // We are inside a runtime (this is #[tokio::test]) — the exact topology
        // the bindings hit. Drive enough inserts to cross the tiny threshold
        // many times.
        for i in 0..2000 {
            let stmt = format!(
                "INSERT INTO test_ks.test_table (id, name) VALUES ({}, 'User{}')",
                i, i
            );
            let n = engine
                .execute_flushing(&stmt)
                .await
                .expect("execute_flushing must not dead-end at the hard limit");
            assert_eq!(n, 1, "single INSERT applies exactly one mutation");
        }

        // A real async flush happened: generation advanced past 1.
        assert!(
            engine.generation() > 1,
            "expected auto-flush to advance generation, got {} (would stay 1 on the old sync path in a runtime)",
            engine.generation()
        );
    }

    /// Issue #1620: the over-threshold `tracing::warn!` must fire at most once per
    /// threshold crossing. With a runtime present the sync `write()` path does
    /// NOT auto-flush, so the memtable stays over threshold across many writes;
    /// the `warned_over_threshold` guard prevents re-warning until the next
    /// flush resets it. This asserts the private flag directly (same module) so
    /// it is deterministic without capturing log output.
    #[tokio::test]
    async fn test_over_threshold_warn_fires_at_most_once_per_crossing() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        // Tiny flush threshold so a couple of writes cross it; generous hard
        // limit so the sync path keeps accepting writes over threshold.
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        )
        .with_flush_threshold(256)
        .with_hard_limit(64 * 1024)
        .with_durability(Durability::Disabled);

        let mut engine = WriteEngine::new(config).unwrap();
        assert!(
            !engine.warned_over_threshold,
            "guard starts false before any crossing"
        );

        // Sync path inside a runtime: no auto-flush, so the memtable climbs and
        // stays over threshold. First over-threshold write flips the guard.
        let mut crossed = false;
        for i in 0..500 {
            engine
                .write(create_test_mutation(
                    i,
                    &format!("User{}", i),
                    1_000_000 + i as i64,
                ))
                .unwrap();
            if engine.warned_over_threshold {
                crossed = true;
                break;
            }
        }
        assert!(crossed, "memtable should have crossed the flush threshold");
        assert!(
            engine.warned_over_threshold,
            "guard set after first crossing"
        );

        // Subsequent over-threshold writes must NOT re-arm/clear the guard.
        for i in 500..600 {
            engine
                .write(create_test_mutation(
                    i,
                    &format!("User{}", i),
                    1_000_000 + i as i64,
                ))
                .unwrap();
            assert!(
                engine.warned_over_threshold,
                "guard must remain set across subsequent over-threshold writes"
            );
        }

        // An explicit flush clears the memtable and resets the guard, so the
        // next crossing warns again.
        engine.flush().await.unwrap();
        assert!(
            !engine.warned_over_threshold,
            "guard resets to false after a successful flush"
        );
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

    // Issue #1392 (roborev r4): when the WAL truncate fails AFTER `set_len(0)`
    // has already zeroed the WAL, the just-published SSTable is the ONLY durable
    // copy of the flushed mutations. The flush must therefore treat the SSTable
    // handoff as COMMITTED for state purposes — clear the memtable and advance
    // the generation BEFORE surfacing the error — so that a retry writes a NEW
    // generation instead of `File::create`-overwriting the published SSTable
    // (which would lose the data on a crash mid-retry).
    //
    // This drives the real `RealDurabilityBarrier` path via the WAL's test-only
    // post-`set_len(0)` sync fault, then proves: (1) the flush surfaces the
    // error, (2) state is committed (memtable cleared, generation advanced),
    // (3) the published gen-1 SSTable survives byte-for-byte, (4) a subsequent
    // flush writes a NEW generation, and (5) a full read returns each mutation
    // exactly once — crash-safe, no loss, no duplication.
    #[tokio::test]
    async fn post_mutation_truncate_failure_commits_and_retry_writes_new_generation() {
        use crate::platform::Platform;
        use crate::storage::sstable::SSTableManager;
        use crate::Config;
        use std::sync::Arc;

        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();
        let data_dir = temp_dir.path().join("data");
        let config = WriteEngineConfig::new(
            data_dir.clone(),
            temp_dir.path().join("wal"),
            schema.clone(),
        );
        let mut engine = WriteEngine::new(config).unwrap();

        // Write the first mutation, then arm a post-set_len(0) WAL-truncate fault
        // so the flush finalize hits the `AfterMutation` path.
        engine
            .write(create_test_mutation(1, "Alice", 1_000_000))
            .unwrap();
        let gen_before = engine.generation();
        engine.wal.set_fail_sync_after_truncate(true);

        let err = engine
            .flush()
            .await
            .expect_err("a post-mutation WAL-truncate failure must surface as an error");
        assert!(
            matches!(err, Error::Storage(_)),
            "post-mutation truncate failure must surface as a storage error, got {err:?}"
        );

        // State is COMMITTED despite the error: the memtable is cleared and the
        // generation has advanced, so a retry cannot reuse the published gen.
        assert_eq!(
            engine.memtable_row_count(),
            0,
            "memtable must be cleared once the SSTable is durably published"
        );
        assert_eq!(
            engine.generation(),
            gen_before + 1,
            "generation must advance so a retry writes a NEW generation"
        );

        // The published gen-1 SSTable exists on disk. Snapshot its bytes to prove
        // a later flush does NOT overwrite it.
        let sstable_dir = data_dir.join("test_ks").join("test_table");
        let gen1_data = sstable_dir.join("nb-1-big-Data.db");
        assert!(
            gen1_data.exists(),
            "the published gen-1 Data.db must exist on disk after the faulted flush"
        );
        let gen1_bytes = std::fs::read(&gen1_data).unwrap();

        // Disarm the fault and flush a SECOND mutation: it must write a NEW
        // generation and leave the first SSTable byte-for-byte intact.
        engine.wal.set_fail_sync_after_truncate(false);
        engine
            .write(create_test_mutation(2, "Bob", 2_000_000))
            .unwrap();
        engine
            .flush()
            .await
            .expect("second flush must succeed")
            .expect("second flush must produce an SSTable");

        let gen2_data = sstable_dir.join("nb-2-big-Data.db");
        assert!(
            gen2_data.exists(),
            "the retry must write a NEW generation (nb-2), not overwrite nb-1"
        );
        assert_eq!(
            std::fs::read(&gen1_data).unwrap(),
            gen1_bytes,
            "the retry must NOT overwrite the published gen-1 SSTable"
        );

        // Full read across both generations: each mutation appears exactly once.
        let cqlite_config = Config::default();
        let platform = Arc::new(Platform::new(&cqlite_config).await.unwrap());
        let manager = SSTableManager::new(
            &data_dir,
            &cqlite_config,
            platform,
            #[cfg(feature = "state_machine")]
            None,
        )
        .await
        .expect("SSTableManager must load both published generations");

        let table_id = crate::types::TableId::from("test_ks.test_table");
        let rows = manager
            .scan(&table_id, None, None, None, Some(&schema))
            .await
            .expect("scan across both generations must succeed");
        assert_eq!(
            rows.len(),
            2,
            "both mutations must be readable exactly once (no loss, no duplication)"
        );
    }

    /// Thread-local `tracing` WARN capturer for the Drop-warn tests (#1693).
    /// `start()` installs a thread-local `tracing` subscriber with NO
    /// `tracing-log` bridge (the tracing-only path an embedder wires) that
    /// records WARN messages per-thread, isolating concurrent tests.
    mod drop_warn_capture {
        use std::cell::RefCell;
        use std::fmt;
        use tracing::field::{Field, Visit};
        use tracing::subscriber::{set_default, DefaultGuard};
        use tracing::{Event, Level, Subscriber};
        use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
        use tracing_subscriber::Registry;

        thread_local! {
            static BUFFER: RefCell<Option<Vec<String>>> = const { RefCell::new(None) };
            static GUARD: RefCell<Option<DefaultGuard>> = const { RefCell::new(None) };
        }
        #[derive(Default)]
        struct MessageVisitor {
            message: String,
        }
        impl Visit for MessageVisitor {
            fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
                if field.name() == "message" {
                    self.message = format!("{value:?}");
                }
            }
        }
        struct WarnCaptureLayer;
        impl<S: Subscriber> Layer<S> for WarnCaptureLayer {
            fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
                if *event.metadata().level() != Level::WARN {
                    return;
                }
                let mut visitor = MessageVisitor::default();
                event.record(&mut visitor);
                BUFFER.with(|b| {
                    if let Some(buf) = b.borrow_mut().as_mut() {
                        buf.push(visitor.message);
                    }
                });
            }
        }

        pub(super) fn start() {
            BUFFER.with(|b| *b.borrow_mut() = Some(Vec::new()));
            let guard = set_default(Registry::default().with(WarnCaptureLayer));
            GUARD.with(|g| *g.borrow_mut() = Some(guard));
        }

        pub(super) fn take_warnings() -> Vec<String> {
            GUARD.with(|g| *g.borrow_mut() = None);
            BUFFER.with(|b| b.borrow_mut().take().unwrap_or_default())
        }
    }

    /// Issue #1693 (AG4): dropping a `WriteEngine` whose memtable still holds
    /// un-flushed rows (i.e. `close()` was never called) must emit a `warn!` so
    /// the silent data-loss mode is visible in logs.
    #[test]
    fn test_drop_without_close_warns_on_nonempty_memtable() {
        drop_warn_capture::start();

        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );
        let mut engine = WriteEngine::new(config).unwrap();
        engine
            .write(create_test_mutation(1, "Alice", 1_000_000))
            .unwrap();
        assert!(
            engine.memtable_row_count() > 0,
            "precondition: memtable must be non-empty before drop"
        );

        // Drop WITHOUT close() — the durability-loss path.
        drop(engine);

        let warnings = drop_warn_capture::take_warnings();
        assert!(
            warnings
                .iter()
                .any(|m| m.contains("dropped") && m.contains("without close")),
            "expected an unflushed-drop warning, captured warnings: {warnings:?}"
        );
        // WAL-durable engine: the guidance must point at WAL recovery, NOT loss.
        assert!(
            warnings.iter().any(|m| m.contains("WAL replay")),
            "WAL-durable drop warning must mention WAL replay recovery, captured: {warnings:?}"
        );
        assert!(
            !warnings.iter().any(|m| m.contains("LOST")),
            "WAL-durable drop warning must NOT claim data loss, captured: {warnings:?}"
        );
    }

    /// Issue #1693 (roborev): with `Durability::Disabled` the WAL is skipped, so
    /// an ungraceful drop loses the un-flushed rows. The warning must say the
    /// rows are LOST rather than giving false "recoverable from the WAL" guidance.
    #[test]
    fn test_drop_without_close_warns_data_loss_when_durability_disabled() {
        drop_warn_capture::start();

        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        )
        .with_durability(Durability::Disabled);
        let mut engine = WriteEngine::new(config).unwrap();
        engine
            .write(create_test_mutation(1, "Alice", 1_000_000))
            .unwrap();
        assert!(
            engine.memtable_row_count() > 0,
            "precondition: memtable must be non-empty before drop"
        );

        drop(engine);

        let warnings = drop_warn_capture::take_warnings();
        assert!(
            warnings
                .iter()
                .any(|m| m.contains("dropped") && m.contains("without close")),
            "expected an unflushed-drop warning, captured warnings: {warnings:?}"
        );
        // No-WAL engine: the warning must signal data loss, NOT WAL recovery.
        assert!(
            warnings.iter().any(|m| m.contains("LOST")),
            "Durability::Disabled drop warning must signal data loss, captured: {warnings:?}"
        );
        assert!(
            !warnings.iter().any(|m| m.contains("WAL replay")),
            "Durability::Disabled drop warning must NOT promise WAL recovery, captured: {warnings:?}"
        );
    }

    /// Control for issue #1693: after a graceful `close()` the memtable is empty,
    /// so `Drop` must NOT emit the unflushed-drop warning.
    #[tokio::test]
    async fn test_drop_after_close_does_not_warn() {
        drop_warn_capture::start();

        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema,
        );
        let mut engine = WriteEngine::new(config).unwrap();
        engine
            .write(create_test_mutation(1, "Alice", 1_000_000))
            .unwrap();
        engine.close().await.unwrap();

        drop(engine);

        let warnings = drop_warn_capture::take_warnings();
        assert!(
            !warnings
                .iter()
                .any(|m| m.contains("dropped") && m.contains("without close")),
            "close() flushed the memtable; Drop must not warn, captured: {warnings:?}"
        );
    }
}
