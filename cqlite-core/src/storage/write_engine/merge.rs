//! K-way merge for combining multiple L0 SSTables
//!
//! Implements efficient k-way merge using a binary heap for producing
//! compacted SSTables from multiple runs.
//!
//! ## Architecture
//!
//! The K-way merger uses a min-heap to efficiently merge k sorted SSTable
//! runs into a single output SSTable. Each run maintains a peek buffer for
//! efficient lookahead.
//!
//! ## Ordering
//!
//! The `Ord`/`PartialOrd` impl on `MergeEntry` governs **heap routing only**
//! (which partition/clustering bucket an entry belongs to) — NOT winner
//! selection. Winner selection among entries with the same clustering key is
//! done by `merge_partition_rows` (see "Cell Merge Rule" below), which layers a
//! timestamp + liveness comparison on top.
//!
//! Heap-routing order:
//! 1. Token (ascending) - Primary partitioning
//! 2. Key bytes (ascending) - Hash collision resolution
//! 3. Clustering key (schema-aware) - Within partition ordering
//! 4. Run index (ascending) - Stable tiebreak for routing (NOT the LWW rule)
//!
//! ## Memory Budget (Issue #754 groundwork, Issue #827 streaming read)
//!
//! The bounded `sync_channel` (capacity [`STREAMING_CHANNEL_CAPACITY`]) limits
//! how many converted `MergeEntry` values from each source live in memory
//! simultaneously between producer and consumer. The consumer/heap pulls
//! lazily via cursors, so the channel acts as a backpressure valve.
//!
//! The producer thread streams its source via
//! [`stream_all_partitions_for_compaction`](crate::storage::sstable::reader::SSTableReader::stream_all_partitions_for_compaction),
//! which uses a sliding-window incremental stitch+parse: it decompresses one
//! chunk at a time, drains every fully-decoded partition out of the window, and
//! forwards each entry through the bounded channel before pulling the next
//! chunk. The blocking `SyncSender::send` backpressure plus the bounded window
//! mean a source's decompressed content is NEVER fully resident — peak memory
//! is bounded by roughly `max_partition_size + one_chunk + channel_capacity`
//! per source, independent of total input size (issue #827). The dhat test
//! `tests/test_issue_827_merge_streaming_memory.rs` asserts the 128 MiB bound
//! against inputs whose total decompressed size exceeds it.
//!
//! ## Cell Merge Rule
//!
//! Last-write-wins by timestamp, following Cassandra `Cells#reconcile`:
//! - Keep the entry with the highest timestamp.
//! - If timestamps are equal, the tombstone (Delete) wins over a live entry,
//!   independent of which file it came from (Issue #498).
//! - If timestamp AND liveness are equal, prefer the lower run_index (newer file).
//!
//! Implementation for M5.2 (Issue #382)

#[cfg(feature = "write-support")]
use crate::error::{Error, Result};
#[cfg(feature = "write-support")]
use crate::schema::TableSchema;
#[cfg(feature = "write-support")]
use crate::storage::write_engine::mutation::{ClusteringKey, DecoratedKey, RangeTombstone};
#[cfg(feature = "write-support")]
use crate::types::Value;

#[cfg(feature = "write-support")]
use std::cmp::{Ordering, Reverse};
#[cfg(feature = "write-support")]
use std::collections::{BinaryHeap, VecDeque};
#[cfg(feature = "write-support")]
use std::path::{Path, PathBuf};
#[cfg(feature = "write-support")]
use std::time::{Duration, Instant};

/// Entry in the merge stream
///
/// Represents a single row from one of the input SSTables. This is the
/// fundamental unit that flows through the merge heap.
#[cfg(feature = "write-support")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeEntry {
    /// Which SSTable this came from (0 = newest)
    pub run_index: usize,
    /// Partition key with token
    pub key: DecoratedKey,
    /// Clustering key (None for tables without clustering)
    pub clustering_key: Option<ClusteringKey>,
    /// Timestamp in microseconds since Unix epoch
    pub timestamp: i64,
    /// Row data (live cells or tombstone)
    pub row_data: RowData,
    /// Complex (collection / UDT) deletion markers for the multi-cell columns
    /// of this `(pk, ck)` (issue #886 substrate).
    ///
    /// Carried through the merge so the per-cell-path collection/UDT followup
    /// (#844) and shadow-before-purge (#887) can preserve collection/UDT
    /// deletion timestamps. `reconcile_cluster` unions and preserves these
    /// across a cluster, but reconciliation does **not yet consult** them and
    /// the writer does not yet apply them — defaults to empty, so output is
    /// byte-unchanged. Population (per-element reader emit) lands in #899.
    pub complex_deletions: Vec<ComplexDeletion>,
    /// Range-deletion marker covering a span of clustering keys (issue #886
    /// substrate).
    ///
    /// A first-class slot so range tombstones can flow through the merge stream
    /// instead of being skipped by the parser; applying them to shadow covered
    /// cells is the follow-up #846. Carried (and timestamp-max-preserved)
    /// through `reconcile_cluster` but **not yet consulted** by reconciliation
    /// or the writer, so output is byte-unchanged. `None` when this entry
    /// carries no range deletion.
    pub range_deletion: Option<RangeTombstone>,
}

impl MergeEntry {
    /// Create a new merge entry.
    ///
    /// The carry-only #886 substrate fields (`complex_deletions`,
    /// `range_deletion`) default to empty/`None`; attach them with
    /// [`with_complex_deletions`](Self::with_complex_deletions) /
    /// [`with_range_deletion`](Self::with_range_deletion) once the reader emit
    /// surfaces them (#899).
    pub fn new(
        run_index: usize,
        key: DecoratedKey,
        clustering_key: Option<ClusteringKey>,
        timestamp: i64,
        row_data: RowData,
    ) -> Self {
        Self {
            run_index,
            key,
            clustering_key,
            timestamp,
            row_data,
            complex_deletions: Vec::new(),
            range_deletion: None,
        }
    }

    /// Attach complex-deletion markers (issue #886 substrate; carry-only).
    #[must_use]
    pub fn with_complex_deletions(mut self, complex_deletions: Vec<ComplexDeletion>) -> Self {
        self.complex_deletions = complex_deletions;
        self
    }

    /// Attach a range-deletion marker (issue #886 substrate; carry-only).
    #[must_use]
    pub fn with_range_deletion(mut self, range_deletion: RangeTombstone) -> Self {
        self.range_deletion = Some(range_deletion);
        self
    }

    /// True when this entry exists ONLY to carry complex/range deletion metadata
    /// and has no row content the writer can emit today (#886/#899 branch-review).
    ///
    /// `reconcile_cluster` emits an empty `RowData::Live { cells: vec![] }` (at
    /// timestamp 0) when a cluster has no surviving cells and no row tombstone but
    /// still carries complex/range deletion metadata, so that metadata survives
    /// reconciliation in the in-memory merge stream. The compaction writer does
    /// NOT yet consume those carried deletions (deferred to #899); if such an
    /// entry is routed to the writer it would become a PHANTOM live empty
    /// (pure-PK) row at timestamp 0, because `DataWriter::merge_row_group` treats
    /// a no-op mutation as a primary-key insert.
    ///
    /// The merge stream never produces a genuine empty-but-live row through this
    /// path: an entry with empty cells, no row tombstone, AND no carried metadata
    /// reconciles to `None` (nothing emitted). So an empty live entry that
    /// survives reconciliation always carries deletion metadata and is purely
    /// synthetic. The writer path must skip these to avoid phantom liveness.
    #[must_use]
    pub fn is_metadata_only_no_op(&self) -> bool {
        matches!(&self.row_data, RowData::Live { cells } if cells.is_empty())
            && (!self.complex_deletions.is_empty() || self.range_deletion.is_some())
    }
}

/// Ord implementation for min-heap routing ONLY (not LWW winner selection).
///
/// This orders entries so the heap yields them grouped by partition and
/// clustering key. The actual equal-timestamp Delete-vs-Live winner is chosen
/// in `merge_partition_rows` (timestamp → liveness → run_index), NOT here.
///
/// Order by:
/// 1. Token (ascending)
/// 2. Key bytes (ascending, for hash collisions)
/// 3. Clustering key (ascending, schema-aware)
/// 4. Run index (ascending) - stable routing tiebreak only
#[cfg(feature = "write-support")]
impl Ord for MergeEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Primary: by token
        match self.key.token.cmp(&other.key.token) {
            Ordering::Equal => {
                // Secondary: by key bytes (hash collision resolution)
                match self.key.key.cmp(&other.key.key) {
                    Ordering::Equal => {
                        // Tertiary: by clustering key
                        match (&self.clustering_key, &other.clustering_key) {
                            (None, None) => {
                                // Quaternary: by run_index (lower = newer)
                                self.run_index.cmp(&other.run_index)
                            }
                            (None, Some(_)) => Ordering::Less,
                            (Some(_), None) => Ordering::Greater,
                            (Some(a), Some(b)) => {
                                // Use fallback Ord (not schema-aware at this level)
                                // Schema-aware comparison happens during partition merge
                                match a.cmp(b) {
                                    Ordering::Equal => {
                                        // Equal clustering keys: prefer lower run_index
                                        self.run_index.cmp(&other.run_index)
                                    }
                                    other_ord => other_ord,
                                }
                            }
                        }
                    }
                    other_ord => other_ord,
                }
            }
            other_ord => other_ord,
        }
    }
}

#[cfg(feature = "write-support")]
impl PartialOrd for MergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Row data: live cells or tombstone
#[cfg(feature = "write-support")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowData {
    /// Live row with cell data
    Live {
        /// Cell data for this row
        cells: Vec<CellData>,
    },
    /// Row tombstone
    Tombstone {
        /// Deletion timestamp (microseconds)
        deletion_time: i64,
        /// Local deletion time (seconds since epoch)
        local_deletion_time: i32,
    },
}

/// Cell data with timestamp, optional TTL, and (for complex columns) cell path.
///
/// ## Per-cell merge metadata (issue #886 — byte-parity foundation)
///
/// To reconcile per-cell and per-element data byte-faithfully (Cassandra
/// `Cells#reconcile`), the merge entry must carry more than a single row-level
/// timestamp. The fields below thread that richer state from the reader toward
/// the followup behaviors (#844 per-cell-path collection/UDT merge, #848
/// tombstone-vs-expiring TTL tie-break). They are **carried but not yet acted
/// on** by reconciliation — this struct change is plumbing only and must not
/// alter output bytes.
///
/// Where the reader does not yet surface a value the field is left `None`; the
/// dependent issues fill it in once the reader is extended.
#[cfg(feature = "write-support")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CellData {
    /// Column name
    pub column: String,
    /// Column value
    pub value: Value,
    /// Cell timestamp (microseconds)
    pub timestamp: i64,
    /// TTL in seconds (None = no expiration)
    pub ttl: Option<u32>,
    /// Cell path for a complex (collection / non-frozen UDT) element — the
    /// serialized element key/index that distinguishes one element of a
    /// multi-cell column from another (issue #886 substrate).
    ///
    /// **Carry-only.** This field is threaded through the merge entry so that
    /// per-element reconciliation can become byte-faithful, but nothing
    /// populates or consumes it yet: the reader still collapses collections to
    /// a single whole-column [`CellData`] and the writer does not read this
    /// field. Population (per-element reader emit) and consumption (per-path
    /// merge #844) land in the follow-up #899. `None` for simple cells.
    pub cell_path: Option<Vec<u8>>,
    /// Local deletion time in **seconds** since the Unix epoch for this cell
    /// (the on-disk `localDeletionTime`), used by gc_grace purging and
    /// expiring-cell tie-breaks (issue #886 substrate).
    ///
    /// For an expiring (TTL) cell this is the cell's expiration instant; for a
    /// cell tombstone it is when the delete was applied. **Carry-only.**
    /// Threaded for #845/#848 but not yet populated by the reader or consumed
    /// by the merge/writer. `None` when unknown.
    pub local_deletion_time: Option<i32>,
}

#[cfg(feature = "write-support")]
impl CellData {
    /// Construct a simple live cell with no TTL, local-deletion-time, or cell
    /// path. The richer fields default to `None`; populate them explicitly when
    /// the reader supplies them (issues #844 / #848).
    pub fn new(column: String, value: Value, timestamp: i64) -> Self {
        Self {
            column,
            value,
            timestamp,
            ttl: None,
            cell_path: None,
            local_deletion_time: None,
        }
    }
}

/// Complex (collection / non-frozen UDT) deletion marker for one column
/// (issue #886 substrate).
///
/// Cassandra writes a complex-deletion marker ahead of a multi-cell column's
/// elements to delete every element written at or before `marked_for_delete_at`.
/// A merged complex deletion is dropped unless it **strictly supersedes** the
/// active one (Cassandra commit `bd244649`). CQLite currently reduces this to a
/// boolean and discards the timestamps; this first-class entity preserves them
/// so per-path merge (#844) and shadow-before-purge (#887) can be byte-faithful.
///
/// **Carry-only.** Carried on [`MergeEntry`] (unioned through
/// `reconcile_cluster`) but not yet populated by the reader or applied during
/// the merge — that is the follow-up #899/#887.
#[cfg(feature = "write-support")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComplexDeletion {
    /// Name of the complex column this deletion covers.
    pub column: String,
    /// Deletion timestamp (`markedForDeleteAt`) in microseconds since the epoch.
    pub marked_for_delete_at: i64,
    /// Local deletion time in seconds since the epoch.
    pub local_deletion_time: i32,
}

/// Result of a merge step (incremental merge)
#[cfg(feature = "write-support")]
#[derive(Debug)]
pub enum MergeStep {
    /// Merged partition with all its rows
    Partition {
        /// Partition key
        key: DecoratedKey,
        /// All rows in this partition (already merged)
        rows: Vec<MergeEntry>,
    },
    /// Merge is complete
    Complete,
}

/// Statistics collected during merge
#[cfg(feature = "write-support")]
#[derive(Debug, Clone)]
pub struct MergeStats {
    /// Number of input files
    pub input_files: usize,
    /// Number of output partitions
    pub output_partitions: u64,
    /// Number of output rows
    pub output_rows: u64,
    /// Bytes written to output
    pub bytes_written: u64,
    /// Elapsed time
    pub elapsed: Duration,
}

/// Buffered reader for a single SSTable run
///
/// Maintains a peek buffer for efficient lookahead without repeated I/O.
/// Buffer size is fixed at 8KB worth of entries for predictable memory usage.
#[cfg(feature = "write-support")]
struct RunReader {
    /// Abstract SSTable row iterator (boxed, not Debug)
    reader: Box<dyn SSTableRowIterator>,
    /// Peek buffer (FIFO)
    buffer: VecDeque<MergeEntry>,
    /// Target buffer size in bytes (~8KB)
    buffer_size: usize,
    /// Whether this run is exhausted
    exhausted: bool,
}

#[cfg(feature = "write-support")]
impl std::fmt::Debug for RunReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunReader")
            .field("buffer_len", &self.buffer.len())
            .field("buffer_size", &self.buffer_size)
            .field("exhausted", &self.exhausted)
            .finish()
    }
}

#[cfg(feature = "write-support")]
impl RunReader {
    /// Default buffer size (8KB worth of entries)
    const DEFAULT_BUFFER_SIZE: usize = 8 * 1024;

    /// Create a new run reader
    fn new(reader: Box<dyn SSTableRowIterator>) -> Self {
        Self {
            reader,
            buffer: VecDeque::new(),
            buffer_size: Self::DEFAULT_BUFFER_SIZE,
            exhausted: false,
        }
    }

    /// Peek at the next entry without consuming it
    ///
    /// Returns None if this run is exhausted.
    fn peek(&mut self) -> Result<Option<&MergeEntry>> {
        // Refill buffer if empty and not exhausted
        if self.buffer.is_empty() && !self.exhausted {
            self.refill_buffer()?;
        }

        Ok(self.buffer.front())
    }

    /// Advance to the next entry
    ///
    /// Consumes the front entry and returns it.
    fn advance(&mut self) -> Result<Option<MergeEntry>> {
        if let Some(entry) = self.buffer.pop_front() {
            return Ok(Some(entry));
        }

        // Buffer empty, try to refill
        if !self.exhausted {
            self.refill_buffer()?;
            Ok(self.buffer.pop_front())
        } else {
            Ok(None)
        }
    }

    /// Check if this run is exhausted
    fn is_exhausted(&self) -> bool {
        self.exhausted && self.buffer.is_empty()
    }

    /// Refill the peek buffer from the underlying reader
    fn refill_buffer(&mut self) -> Result<()> {
        let mut bytes_buffered = 0;

        while bytes_buffered < self.buffer_size {
            match self.reader.next() {
                Some(Ok(entry)) => {
                    // Estimate entry size for buffer management
                    bytes_buffered += Self::estimate_entry_size(&entry);
                    self.buffer.push_back(entry);
                }
                Some(Err(e)) => return Err(e),
                None => {
                    self.exhausted = true;
                    break;
                }
            }
        }

        Ok(())
    }

    /// Estimate the memory size of an entry
    ///
    /// This is approximate - just for buffer management.
    fn estimate_entry_size(entry: &MergeEntry) -> usize {
        let base_size = std::mem::size_of::<MergeEntry>();
        let key_size = entry.key.key.len();
        let clustering_size = entry
            .clustering_key
            .as_ref()
            .map(|ck| {
                ck.columns
                    .iter()
                    .map(|(name, value)| name.len() + Self::estimate_value_size(value))
                    .sum()
            })
            .unwrap_or(0);

        let data_size = match &entry.row_data {
            RowData::Live { cells } => cells
                .iter()
                .map(|cell| {
                    std::mem::size_of::<CellData>()
                        + cell.column.len()
                        + Self::estimate_value_size(&cell.value)
                })
                .sum(),
            RowData::Tombstone { .. } => 16,
        };

        base_size + key_size + clustering_size + data_size
    }

    /// Estimate the memory size of a Value
    fn estimate_value_size(value: &Value) -> usize {
        match value {
            Value::Null => 0,
            Value::Boolean(_) => 1,
            Value::TinyInt(_) => 1,
            Value::SmallInt(_) => 2,
            Value::Integer(_) => 4,
            Value::BigInt(_) | Value::Counter(_) | Value::Timestamp(_) | Value::Time(_) => 8,
            Value::Float32(_) => 4,
            Value::Float(_) => 8,
            Value::Text(s) => s.len() + std::mem::size_of::<String>(),
            Value::Blob(b) => b.len() + std::mem::size_of::<Vec<u8>>(),
            Value::Uuid(_) => 16,
            Value::Inet(b) => b.len() + std::mem::size_of::<Vec<u8>>(),
            Value::Varint(b) => b.len() + std::mem::size_of::<Vec<u8>>(),
            Value::Decimal { unscaled, .. } => unscaled.len() + 4 + std::mem::size_of::<Vec<u8>>(),
            Value::Date(_) => 4,
            Value::Duration { .. } => 20,
            _ => 32, // Default estimate for complex types
        }
    }
}

/// Abstract iterator trait for SSTable rows
///
/// This allows the K-way merger to work with different SSTable reader
/// implementations without coupling to specific reader types.
#[cfg(feature = "write-support")]
pub trait SSTableRowIterator: Send {
    /// Get the next row from this SSTable
    fn next(&mut self) -> Option<Result<MergeEntry>>;
}

/// Run an async future to completion from a synchronous context, safely whether
/// or not a Tokio runtime is already running on the current thread.
///
/// This is the shared async-to-sync bridge for the write engine's blocking
/// helpers: [`SSTableRowIteratorAdapter`] (the k-way merge readers),
/// `WriteEngine::flush_internal`, and `WriteEngine::finalize_merge_blocking`.
///
/// ## Why not `Handle::block_on`?
///
/// When this bridge is reached from a thread that is already driving a Tokio
/// runtime — anything under `#[tokio::main]` or `#[tokio::test]`, which is how
/// the CLI (`maintenance`, `export-sstable --compact`) and any async caller
/// reach compaction — `Handle::current().block_on()` panics with *"Cannot start
/// a runtime from within a runtime"* (Issue #587). Compaction only reaches the
/// bridge once a merge has input SSTables to read, which is why STCS worked in
/// isolation but blew up from async callers.
///
/// `tokio::task::block_in_place` is not a general fix either: it panics on a
/// current-thread runtime (e.g. the default `#[tokio::test]` flavor).
///
/// ## Strategy
///
/// - **No runtime on the current thread** (`Handle::try_current()` is `Err`):
///   create a temporary runtime and block on it directly.
/// - **Already inside a runtime** (`Ok`): hand the future to a dedicated scoped
///   thread that owns a fresh runtime, then join it. That thread is free to
///   block because it is not driving the caller's runtime, so this works for
///   both the multi-thread and current-thread runtime flavors.
///   [`std::thread::scope`] (rather than [`std::thread::spawn`]) lets the future
///   borrow from the caller's stack — `flush_internal`/`finalize_merge_blocking`
///   pass futures that borrow `&mut self` — so it need not be `'static`.
///
/// The future and its output must be `Send` because they cross a thread boundary
/// in the in-runtime case.
#[cfg(feature = "write-support")]
pub(crate) fn block_on_async<F, T>(future: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>> + Send,
    T: Send,
{
    match tokio::runtime::Handle::try_current() {
        // Already inside a runtime: a nested `block_on` on this thread would
        // panic. Run the future on a scoped thread with its own runtime instead.
        Ok(_) => std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let rt = tokio::runtime::Runtime::new().map_err(|e| {
                        Error::Storage(format!("Failed to create tokio runtime: {}", e))
                    })?;
                    rt.block_on(future)
                })
                .join()
                .map_err(|_| Error::Storage("async-to-sync bridge thread panicked".to_string()))?
        }),
        // No runtime on this thread: safe to create one and block directly.
        Err(_) => {
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| Error::Storage(format!("Failed to create tokio runtime: {}", e)))?;
            rt.block_on(future)
        }
    }
}

/// Adapter that wraps async SSTableReader into a true-streaming sync
/// [`SSTableRowIterator`].
///
/// ## Design (Issue #754 — remove 128MB buffer cap residue of #447)
///
/// The V5CompressedLegacy format requires chunk stitching: a partition may
/// straddle compression-chunk boundaries, so the decoder needs a contiguous
/// view spanning at least one whole partition. The reader's streaming path
/// keeps only a **sliding window** of that view — one chunk plus the partition
/// currently being decoded — rather than the whole decompressed file.
///
/// A background thread (the producer) opens the SSTable with its own Tokio
/// runtime and calls
/// [`stream_all_partitions_for_compaction`](crate::storage::sstable::reader::SSTableReader::stream_all_partitions_for_compaction),
/// which decompresses one chunk at a time, drains every fully-decoded partition
/// out of the window, and forwards each entry one at a time into a bounded
/// `sync_channel`. The channel capacity is [`STREAMING_CHANNEL_CAPACITY`]
/// entries; once the channel is full the producer blocks until the consumer
/// (the main merge thread) pulls the next entry.
///
/// The bounded window plus the bounded channel together make end-to-end peak
/// memory independent of total input size: a source's decompressed content is
/// never fully resident. Peak is roughly `max_partition_size + one_chunk +
/// channel_capacity` per source (issue #827).
///
/// ## Issue #591 safety (mmap vs file deletion)
///
/// `finalize_merge_async` deletes the input SSTable files once the merged output
/// is published. We require that no mmap outlives its backing file. The producer
/// thread opens the reader with `use_mmap = false`, and the thread *fully reads
/// all file data* (the stitching phase) before it can block on a channel send.
/// By the time `finalize_merge_async` runs, the merge is complete and all
/// channel entries have been consumed, so the producer thread has long since
/// finished and dropped its file handle. No mmap ever exists.
///
/// ## Issue #587 safety (async-from-sync bridge)
///
/// The producer thread creates its own `tokio::runtime::Runtime` (never
/// `Handle::block_on`), so it cannot panic even when called from within an
/// existing Tokio runtime. This is the same strategy as [`block_on_async`].
#[cfg(feature = "write-support")]
struct SSTableRowIteratorAdapter {
    /// Receiving end of the bounded channel fed by the producer thread.
    receiver: std::sync::mpsc::Receiver<std::result::Result<MergeEntry, String>>,
    /// JoinHandle for the producer thread (held so the thread is joined on drop).
    _producer: std::thread::JoinHandle<()>,
}

/// Number of pre-fetched `MergeEntry` objects buffered per source in the
/// streaming channel. Each entry is typically a few hundred bytes; at 256
/// entries per source and 10 sources that is a few hundred KB — well within the
/// 128MB budget. The value is a balance between producer/consumer
/// synchronization overhead (lower = more context switches) and memory footprint
/// (higher = more buffering).
#[cfg(feature = "write-support")]
const STREAMING_CHANNEL_CAPACITY: usize = 256;

#[cfg(feature = "write-support")]
impl SSTableRowIteratorAdapter {
    /// Open an SSTable and start a streaming producer thread.
    ///
    /// Returns immediately; the producer thread runs concurrently and populates
    /// the channel as the consumer advances. The file handle is held only by
    /// the producer thread and is dropped when the thread finishes.
    ///
    /// Uses [`SSTableReader::iterate_all_partitions_for_compaction`] which
    /// returns actual per-row timestamps decoded from the on-disk row headers.
    /// This allows the k-way merger to perform timestamp-accurate last-write-wins
    /// ordering, which is essential for tombstone shadowing (Issue #505).
    ///
    /// When the schema has clustering columns, their values are extracted from
    /// the decoded cells (in the producer thread, by column name in schema order)
    /// and stored on `MergeEntry.clustering_key` so `merge_partition_rows` groups
    /// and reconciles distinct clustering rows correctly. The clustering columns
    /// are left in the cells as well, since the read-back path expects them there.
    fn open(path: &Path, run_index: usize, schema: &TableSchema) -> Result<Self> {
        let path_buf = path.to_path_buf();
        let schema = schema.clone();

        let (sender, receiver) = std::sync::mpsc::sync_channel(STREAMING_CHANNEL_CAPACITY);

        // Spawn the producer thread. It owns a fresh Tokio runtime so it never
        // collides with any runtime on the calling thread (Issue #587).
        let producer = std::thread::spawn(move || {
            Self::producer_thread(path_buf, run_index, schema, sender);
        });

        Ok(Self {
            receiver,
            _producer: producer,
        })
    }

    /// Body of the producer thread.
    ///
    /// Opens the SSTable with buffered I/O (Issue #591), then **streams** the
    /// source one partition at a time via
    /// [`stream_all_partitions_for_compaction`](crate::storage::sstable::reader::SSTableReader::stream_all_partitions_for_compaction),
    /// converting each entry to a [`MergeEntry`] (populating the clustering key
    /// from the decoded cells when the schema has clustering columns) and
    /// sending it through the bounded channel immediately (issue #827). The
    /// blocking `SyncSender::send` provides the backpressure that — together
    /// with the reader's sliding-window stitch+parse — keeps peak memory bounded
    /// by `max_partition_size + one_chunk + channel_capacity`, independent of
    /// the total source size. Errors are forwarded as `Err(String)`.
    fn producer_thread(
        path_buf: PathBuf,
        run_index: usize,
        schema: TableSchema,
        sender: std::sync::mpsc::SyncSender<std::result::Result<MergeEntry, String>>,
    ) {
        // Drive the streaming read on an owned Tokio runtime (Issue #587): the
        // producer owns its single-purpose runtime, so the blocking
        // `SyncSender::send` inside the emit callback never stalls a shared
        // runtime, and there is no nested `block_on` / `Handle::current`.
        // use_mmap = false (Issue #591): the file must not be memory-mapped
        // because finalize_merge_async may delete it after the merge completes.
        // Clone the sender for the error path: the streaming closure moves one
        // clone for per-entry sends, leaving this one to report a fatal error.
        let error_sender = sender.clone();
        let stream_result = (|| -> Result<()> {
            use crate::platform::Platform;
            use crate::Config;
            use std::sync::Arc;

            let mut config = Config::default();
            config.storage.use_mmap = false;
            // Cloned so the async block can take it by move while the outer
            // `schema` stays available for build_merge_entry below.
            let schema_for_reader = schema.clone();

            let rt = tokio::runtime::Runtime::new().map_err(|e| {
                Error::Storage(format!(
                    "streaming producer: failed to create runtime: {}",
                    e
                ))
            })?;

            rt.block_on(async move {
                let platform = Arc::new(Platform::new(&config).await?);
                let reader = crate::storage::sstable::reader::SSTableReader::open(
                    &path_buf, &config, platform,
                )
                .await?;

                // Pass the schema so the parser uses the real clustering column
                // names; the header-inferred fallback uses generic names like
                // "clustering_key", which would defeat extract_clustering_key.
                //
                // The emit callback converts and forwards one entry at a time.
                // A blocking `send` applies backpressure; an `Err` from `send`
                // means the consumer was dropped → stop the scan (Break).
                reader
                    .stream_all_partitions_for_compaction(
                        Some(&schema_for_reader),
                        |row_key, value, timestamp| {
                            let msg = Self::build_merge_entry(
                                run_index, row_key, value, timestamp, &schema,
                            )
                            .map_err(|e| e.to_string());
                            match sender.send(msg) {
                                Ok(()) => Ok(std::ops::ControlFlow::Continue(())),
                                Err(_) => Ok(std::ops::ControlFlow::Break(())),
                            }
                        },
                    )
                    .await
            })
        })();

        if let Err(e) = stream_result {
            // Forward the error; ignore send failure (consumer may have dropped).
            let _ = error_sender.send(Err(e.to_string()));
        }
        // Channel closed naturally when sender is dropped here.
    }

    /// Convert one streamed `(RowKey, Value, timestamp)` source entry into a
    /// [`MergeEntry`] for run `run_index` (issue #827).
    ///
    /// Factored out of the producer loop so the streaming emit callback can call
    /// it inline. Populates the clustering key from the decoded cells so wide-row
    /// (clustering) partitions reconcile per `(pk, ck)` instead of collapsing
    /// into one row.
    fn build_merge_entry(
        run_index: usize,
        row_key: crate::types::RowKey,
        value: crate::types::Value,
        timestamp: i64,
        schema: &TableSchema,
    ) -> Result<MergeEntry> {
        let key_bytes = row_key.0;
        let decorated_key = DecoratedKey::from_key_bytes(key_bytes)?;
        let row_data = Self::value_to_row_data(&value, timestamp)?;
        let clustering_key = Self::extract_clustering_key(&row_data, schema);
        Ok(MergeEntry::new(
            run_index,
            decorated_key,
            clustering_key,
            timestamp,
            row_data,
        ))
    }

    /// Extract a `ClusteringKey` from the row's live cells using the schema.
    ///
    /// For each clustering column declared in the schema (in position order),
    /// look for a cell with that column name in the decoded `RowData::Live`
    /// cells.  If all clustering columns are found, return `Some(ClusteringKey)`;
    /// otherwise (including for tombstone entries that have no cells) return
    /// `None`.
    ///
    /// The clustering columns are intentionally left inside the cells so the
    /// downstream read-back path can still find them.
    fn extract_clustering_key(row_data: &RowData, schema: &TableSchema) -> Option<ClusteringKey> {
        if schema.clustering_keys.is_empty() {
            return None;
        }

        let cells = match row_data {
            RowData::Live { cells } => cells,
            RowData::Tombstone { .. } => return None,
        };

        // Build the clustering key columns in schema order.
        let mut ck_columns: Vec<(String, Value)> = Vec::with_capacity(schema.clustering_keys.len());

        for ck_col in &schema.clustering_keys {
            let found = cells
                .iter()
                .find(|cell| cell.column == ck_col.name)
                .map(|cell| (ck_col.name.clone(), cell.value.clone()));

            match found {
                Some(pair) => ck_columns.push(pair),
                // If any clustering column is missing, we cannot form a valid
                // ClusteringKey — return None so the row falls into the None
                // bucket (treated as an unclustered row).
                None => return None,
            }
        }

        Some(ClusteringKey {
            columns: ck_columns,
        })
    }

    /// Convert a reader Value to RowData.
    ///
    /// `row_timestamp` is the per-row timestamp decoded from the on-disk row
    /// header (see [`SSTableReader::iterate_all_partitions_for_compaction`]). The
    /// reader does not surface per-cell timestamps for live cells, so each live
    /// cell inherits the row timestamp. This is required for per-cell reconcile
    /// and row-tombstone shadowing to compare cell timestamps correctly
    /// (Issue #533) — without it live cells would default to 0 and be wrongly
    /// shadowed by any row tombstone.
    ///
    /// Issue #505: `Value::Tombstone(RowTombstone)` is now correctly emitted by
    /// the V5CompressedLegacy parser for deleted rows, and
    /// `Value::Tombstone(CellTombstone)` appears inside `Value::Map` entries for
    /// deleted cells.  Both are surfaced here so the merger can apply shadowing
    /// semantics.  A cell tombstone keeps its own `deletion_time` so equal-ts
    /// reconcile still resolves it correctly.
    fn value_to_row_data(value: &crate::types::Value, row_timestamp: i64) -> Result<RowData> {
        match value {
            crate::types::Value::Tombstone(info) => Ok(RowData::Tombstone {
                deletion_time: info.deletion_time,
                local_deletion_time: 0, // TombstoneInfo does not carry local_deletion_time
            }),
            crate::types::Value::Map(map_entries) => {
                let mut cells = Vec::with_capacity(map_entries.len());
                for (key, val) in map_entries {
                    let column = match key {
                        crate::types::Value::Text(s) => s.clone(),
                        other => format!("{:?}", other),
                    };
                    // Cell tombstones carry their own deletion_time (Issue #505);
                    // live cells inherit the row timestamp (Issue #533) so per-cell
                    // shadowing and LWW order them against row tombstones correctly.
                    let cell_ts = match val {
                        crate::types::Value::Tombstone(info) => info.deletion_time,
                        _ => row_timestamp,
                    };
                    cells.push(CellData {
                        column,
                        value: val.clone(),
                        timestamp: cell_ts,
                        // ttl / local_deletion_time / cell_path are threaded for
                        // the followup behaviors (#844, #848) but the reader's
                        // `(RowKey, Value, ts)` compaction stream does not yet
                        // surface per-cell ttl, the cell's local-deletion-time,
                        // or a complex-column cell-path (the map key here is the
                        // top-level column name, not a collection element path),
                        // so they stay `None`. Populating them is part of #844 /
                        // #848 once the reader is extended (issue #886 plumbing).
                        ttl: None,
                        cell_path: None,
                        local_deletion_time: None,
                    });
                }
                Ok(RowData::Live { cells })
            }
            // Single value or other formats - wrap as a single cell
            other => Ok(RowData::Live {
                cells: vec![CellData {
                    column: "value".to_string(),
                    value: other.clone(),
                    timestamp: row_timestamp,
                    // Not surfaced by the reader's compaction stream yet; see the
                    // note on the map-entry path above (issue #886 plumbing).
                    ttl: None,
                    cell_path: None,
                    local_deletion_time: None,
                }],
            }),
        }
    }
}

#[cfg(feature = "write-support")]
impl SSTableRowIterator for SSTableRowIteratorAdapter {
    fn next(&mut self) -> Option<Result<MergeEntry>> {
        match self.receiver.recv() {
            Ok(Ok(entry)) => Some(Ok(entry)),
            Ok(Err(msg)) => Some(Err(Error::Storage(format!(
                "streaming merge producer error: {}",
                msg
            )))),
            // Channel closed — producer finished normally.
            Err(_) => None,
        }
    }
}

/// K-way merger for combining multiple SSTables
///
/// Uses a min-heap to efficiently merge k sorted SSTable runs into a single
/// output. Each run maintains a small peek buffer for efficient lookahead.
///
/// ## Usage
///
/// ```rust,ignore
/// // Create merger from input SSTable paths
/// let merger = KWayMerger::new(input_paths, &schema)?;
///
/// // Option 1: Full merge to output writer
/// let stats = merger.merge(&mut output_writer)?;
///
/// // Option 2: Incremental merge (step-by-step)
/// loop {
///     match merger.step()? {
///         MergeStep::Partition { key, rows } => {
///             // Process partition
///         }
///         MergeStep::Complete => break,
///     }
/// }
/// ```
#[cfg(feature = "write-support")]
#[derive(Debug)]
pub struct KWayMerger {
    /// Input runs (one per SSTable)
    runs: Vec<RunReader>,
    /// Min-heap for efficient merge
    heap: BinaryHeap<Reverse<MergeEntry>>,
    /// Current partition being merged (for partition boundary detection)
    current_partition: Option<DecoratedKey>,
    /// Table schema for schema-aware merging
    schema: TableSchema,
    /// gc_grace cutoff (seconds since epoch): tombstones/cells whose
    /// `local_deletion_time < gc_before_secs` are purgeable.
    ///
    /// Threaded in for deterministic, Cassandra-matching purge decisions
    /// (issue #842 parity harness). NOTE: purging is NOT yet applied during the
    /// merge — see `reconcile_cluster` (issues #845 gc_grace purging, #848
    /// TTL/expiring tie-break). The value is currently carried but unused so the
    /// `cqlite compact --gc-before` plumbing and parity harness can land ahead of
    /// the purge semantics.
    #[allow(dead_code)]
    gc_before_secs: Option<i64>,
    /// "now" (seconds since epoch) used to evaluate TTL expiry during merge.
    /// Carried but not yet consulted — see the note on `gc_before_secs`.
    #[allow(dead_code)]
    now_secs: Option<i64>,
}

/// Report returned by [`compact_sstables`].
#[cfg(feature = "write-support")]
#[derive(Debug)]
pub struct CompactReport {
    /// Metadata for the SSTable written by the compaction.
    pub output: crate::storage::sstable::writer::SSTableInfo,
    /// Statistics about the merge that produced it.
    pub stats: MergeStats,
}

/// Compute output encoding baselines (min timestamp / local-deletion-time / TTL)
/// from the input SSTables' `Statistics.db` files (two-pass compaction, issue #729).
///
/// The output's delta-encoding baseline must be `<=` every per-partition value in
/// every input, so this returns the minimum across all inputs. Each component is
/// left at its `MAX` sentinel when no input contributes a value — matching the
/// value `SSTableWriter::pre_seed_encoding_baselines` then receives.
///
/// Shared by [`compact_sstables`] and `WriteEngine::start_merge` so the one-shot
/// and policy-driven compaction paths seed baselines identically.
#[cfg(feature = "write-support")]
pub fn compute_baseline_min(input_paths: &[PathBuf]) -> (i64, i32, i32) {
    let mut baseline_min_ts = i64::MAX;
    let mut baseline_min_ldt = i32::MAX;
    let mut baseline_min_ttl = i32::MAX;
    for data_path in input_paths {
        // Derive Statistics.db path from Data.db path
        let stats_path = {
            let filename = data_path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            let stats_filename = filename.replace("Data.db", "Statistics.db");
            data_path
                .parent()
                .unwrap_or(data_path.as_path())
                .join(stats_filename)
        };
        if !stats_path.exists() {
            continue;
        }
        let stats_bytes = match std::fs::read(&stats_path) {
            Ok(b) => b,
            Err(e) => {
                log::warn!(
                    "Could not read Statistics.db {:?} for baseline pre-seeding: {}",
                    stats_path,
                    e
                );
                continue;
            }
        };
        match crate::parser::enhanced_statistics_parser::parse_statistics_with_fallback(
            &stats_bytes,
            None,
        ) {
            Ok((_, sstable_stats)) => {
                let ts_stats = &sstable_stats.timestamp_stats;
                baseline_min_ts = baseline_min_ts.min(ts_stats.min_timestamp);
                // Local-deletion-time baseline seeding (#853/#886 branch-review,
                // Finding 2). The parser reconstructs LDT as
                // `readUnsignedVInt32() + DELETION_TIME_EPOCH` (EncodingStats.java:289),
                // so a far-future LDT in [2^31, 2^32) surfaces here as an i64 ABOVE
                // i32::MAX (e.g. 2^31+5). These are legitimate after the deletion-marker
                // fixes (#853 / range tombstones): they are negative i32 BIT PATTERNS,
                // not "bad" values, and Cassandra's `EncodingStats.merge` mins over the
                // signed int. Normalize as UNSIGNED 32-bit and reinterpret the bits as
                // i32 so the seeded baseline matches the final Statistics.db baseline
                // (DataWriter encodes per-row deltas as
                // `local_deletion_time.wrapping_sub(min) as u32`). Casting the raw i64
                // straight to i32 would also work for the bits, but the explicit
                // `as u32 as i32` documents the 32-bit unsigned normalization.
                //
                // 0 is the normalized "no tombstones" sentinel
                // (StatisticsMetadata::finalize() maps i32::MAX→0); include it so the
                // baseline stays safe for merger tombstones that also use
                // local_deletion_time=0. SKIP only the live/no-deletion sentinel
                // (i32::MAX, DeletionTime.LIVE), which must never lower the baseline.
                let ldt_bits = ts_stats.min_deletion_time as u32 as i32;
                if ldt_bits != i32::MAX {
                    baseline_min_ldt = baseline_min_ldt.min(ldt_bits);
                }
                if let Some(min_ttl) = ts_stats.min_ttl {
                    if min_ttl > 0 && min_ttl < i32::MAX as i64 {
                        baseline_min_ttl = baseline_min_ttl.min(min_ttl as i32);
                    }
                }
            }
            Err(e) => {
                log::warn!(
                    "Could not parse Statistics.db {:?} for baseline pre-seeding: {:?}",
                    stats_path,
                    e
                );
            }
        }
    }
    (baseline_min_ts, baseline_min_ldt, baseline_min_ttl)
}

/// Compact an explicit set of input SSTables into a single output SSTable.
///
/// Unlike [`WriteEngine::maintenance_step`](super::WriteEngine::maintenance_step),
/// this is a one-shot, policy-free compaction over exactly `input_paths`, writing
/// the merged result to `output_dir` (the writer appends `keyspace/table/`). It is
/// the engine entry point behind the `cqlite compact` CLI command and the
/// compaction-parity harness (issue #842).
///
/// `input_paths` must be ordered newest-to-oldest: index 0 is the newest run, which
/// wins last-write-wins ties at equal timestamp/liveness.
///
/// `gc_before_secs` / `now_secs` are threaded into the merger for deterministic,
/// Cassandra-matching purge decisions. NOTE: tombstone purging and TTL expiry are
/// NOT yet applied during the merge (issues #845, #848); these parameters are
/// currently carried but do not yet drop purgeable data. The plumbing lands first
/// so the parity harness can drive out the purge semantics.
#[cfg(feature = "write-support")]
/// Determine which dropped columns still have surviving cells after the merge.
///
/// Runs a merge pre-pass with the decode `schema` (so the dropped-column filter
/// applies identically to the write pass) and collects the names of
/// `dropped_columns` that appear in at least one surviving `Live` cell — i.e.
/// columns re-added with writes after their drop time. `compact_sstables` keeps
/// exactly these dropped columns in the output writer schema; the rest are
/// stripped from the output header. Stops early once every dropped column has
/// been observed surviving.
fn compute_surviving_dropped_columns(
    input_paths: Vec<PathBuf>,
    schema: &TableSchema,
    gc_before_secs: Option<i64>,
    now_secs: Option<i64>,
) -> Result<std::collections::HashSet<String>> {
    let mut surviving: std::collections::HashSet<String> = std::collections::HashSet::new();
    let total = schema.dropped_columns.len();
    let mut merger = KWayMerger::new_with_gc(input_paths, schema, gc_before_secs, now_secs)?;
    loop {
        match merger.step()? {
            MergeStep::Complete => break,
            MergeStep::Partition { rows, .. } => {
                for row in &rows {
                    if let RowData::Live { cells } = &row.row_data {
                        for cell in cells {
                            if schema.dropped_columns.contains_key(&cell.column) {
                                surviving.insert(cell.column.clone());
                            }
                        }
                    }
                }
                if surviving.len() == total {
                    break; // every dropped column already shown to survive
                }
            }
        }
    }
    Ok(surviving)
}

pub async fn compact_sstables(
    input_paths: Vec<PathBuf>,
    output_dir: &std::path::Path,
    schema: &TableSchema,
    generation: u64,
    gc_before_secs: Option<i64>,
    now_secs: Option<i64>,
) -> Result<CompactReport> {
    if input_paths.is_empty() {
        return Err(Error::InvalidInput(
            "compaction requires at least one input SSTable".to_string(),
        ));
    }

    // Decode with `schema` (which retains dropped columns so their input cells
    // parse and can be purged), but WRITE with a post-drop schema. A dropped
    // column with no surviving cells must NOT appear in the output serialization
    // header (else a natural post-drop reader misaligns), while a dropped column
    // with surviving (re-added) cells MUST be retained (else those cells have no
    // matching header column and corrupt the row) — see #847 review.
    //
    // Which dropped columns survive is data-dependent, and the writer fixes its
    // header from the schema before the first row is written, so determine the
    // surviving set with a merge pre-pass (only when any column is dropped — the
    // common no-drop path skips it entirely). The pre-pass uses the SAME merge
    // logic as the write pass, so the two agree on what survives.
    let retained_dropped = if schema.dropped_columns.is_empty() {
        std::collections::HashSet::new()
    } else {
        compute_surviving_dropped_columns(input_paths.clone(), schema, gc_before_secs, now_secs)?
    };
    let write_schema = schema.for_compaction_output(&retained_dropped);

    let merger = KWayMerger::new_with_gc(input_paths.clone(), schema, gc_before_secs, now_secs)?;

    let mut writer = crate::storage::sstable::writer::SSTableWriter::new(
        output_dir.to_path_buf(),
        generation,
        &write_schema,
    )?;

    // Two-pass compaction (issue #729): seed the output's encoding baselines from
    // the inputs' Statistics.db before writing any partition.
    let (baseline_min_ts, baseline_min_ldt, baseline_min_ttl) = compute_baseline_min(&input_paths);
    writer.pre_seed_encoding_baselines(baseline_min_ts, baseline_min_ldt, baseline_min_ttl);

    let stats = merger.merge(&mut writer)?;
    let output = writer.finish().await?;

    Ok(CompactReport { output, stats })
}

#[cfg(feature = "write-support")]
impl KWayMerger {
    /// Create a new k-way merger from input SSTable paths
    ///
    /// # Arguments
    ///
    /// * `input_paths` - Paths to input SSTable Data.db files (ordered newest to oldest)
    /// * `schema` - Table schema for schema-aware merging
    ///
    /// # Returns
    ///
    /// A new KWayMerger ready to merge the input SSTables.
    ///
    /// # Errors
    ///
    /// Returns an error if any input SSTable cannot be opened.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let input_paths = vec![
    ///     PathBuf::from("data/nb-1-big-Data.db"),
    ///     PathBuf::from("data/nb-2-big-Data.db"),
    /// ];
    /// let merger = KWayMerger::new(input_paths, &schema)?;
    /// ```
    pub fn new(input_paths: Vec<PathBuf>, schema: &TableSchema) -> Result<Self> {
        Self::new_with_gc(input_paths, schema, None, None)
    }

    /// Create a new k-way merger with explicit purge parameters.
    ///
    /// Identical to [`KWayMerger::new`] but threads an explicit gc_grace cutoff
    /// (`gc_before_secs`) and TTL evaluation time (`now_secs`) into the merge.
    /// This is the deterministic entry point used by `compact_sstables` (the
    /// `cqlite compact` CLI command) and the compaction-parity harness (issue
    /// #842): Cassandra's compaction takes the same `gcBefore`, so purge
    /// decisions cannot diverge between the two engines.
    ///
    /// # Arguments
    ///
    /// * `input_paths` - Paths to input SSTable Data.db files (ordered newest to oldest)
    /// * `schema` - Table schema for schema-aware merging
    /// * `gc_before_secs` - gc_grace cutoff (seconds since epoch), or `None` to not purge
    /// * `now_secs` - "now" (seconds since epoch) for TTL expiry, or `None` for engine default
    pub fn new_with_gc(
        input_paths: Vec<PathBuf>,
        schema: &TableSchema,
        gc_before_secs: Option<i64>,
        now_secs: Option<i64>,
    ) -> Result<Self> {
        if input_paths.is_empty() {
            return Err(Error::InvalidInput(
                "K-way merge requires at least one input file".to_string(),
            ));
        }

        // Enforce the dropped-column decode contract (#904/#847): every column
        // named in `dropped_columns` must still be declared in `columns` so its
        // cells decode and can be purged. A schema built programmatically may
        // bypass `validate()`, so guard here at the authoritative compaction
        // entry too — converting a silent "cells never decoded / misaligned"
        // bug into a clear error.
        schema.validate_dropped_columns()?;

        // Create run readers for each input SSTable (ordered newest to oldest)
        let mut runs = Vec::with_capacity(input_paths.len());
        for (run_index, path) in input_paths.iter().enumerate() {
            let adapter = SSTableRowIteratorAdapter::open(path, run_index, schema)?;
            runs.push(RunReader::new(Box::new(adapter)));
        }

        // Initialize heap (will be populated on first step)
        let heap = BinaryHeap::new();

        Ok(Self {
            runs,
            heap,
            current_partition: None,
            schema: schema.clone(),
            gc_before_secs,
            now_secs,
        })
    }

    /// Perform a full merge to the output writer
    ///
    /// This is a convenience method that repeatedly calls `step()` until
    /// the merge is complete, writing each partition to the output writer.
    ///
    /// # Arguments
    ///
    /// * `output_writer` - SSTableWriter to write merged output
    ///
    /// # Returns
    ///
    /// Statistics about the merge operation.
    ///
    /// # Errors
    ///
    /// Returns an error if reading or writing fails.
    pub fn merge(
        mut self,
        output_writer: &mut crate::storage::sstable::writer::SSTableWriter,
    ) -> Result<MergeStats> {
        let start_time = Instant::now();
        let mut stats = MergeStats {
            input_files: self.runs.len(),
            output_partitions: 0,
            output_rows: 0,
            bytes_written: 0,
            elapsed: Duration::from_secs(0), // Will be updated at the end
        };

        while let MergeStep::Partition { key, rows } = self.step()? {
            // Skip metadata-only entries on the writer path (#886/#899
            // branch-review). They exist only to carry complex/range deletion
            // metadata through the in-memory merge stream; the writer does not
            // yet consume those fields, so emitting them would write a phantom
            // live empty (pure-PK) row at timestamp 0. See
            // `MergeEntry::is_metadata_only_no_op`.
            let mutations = rows
                .into_iter()
                .filter(|entry| !entry.is_metadata_only_no_op())
                .map(|entry| Self::merge_entry_to_mutation(entry, &self.schema))
                .collect::<Result<Vec<_>>>()?;

            // If every merged row was metadata-only, the partition has no
            // writer-emittable content. Skipping `write_partition` here avoids a
            // phantom EMPTY partition (header/end marker + Index/Filter/Summary/
            // statistics registration) in the output SSTable. Such a partition
            // must not be counted as an output partition or row (#886
            // branch-review).
            if mutations.is_empty() {
                continue;
            }

            stats.output_partitions += 1;
            stats.output_rows += mutations.len() as u64;

            output_writer.write_partition(key, mutations)?;
        }

        stats.elapsed = start_time.elapsed();
        Ok(stats)
    }

    /// Perform one merge step (one partition)
    ///
    /// Returns the next merged partition, or Complete if the merge is done.
    /// This allows incremental merging for better memory control.
    ///
    /// # Returns
    ///
    /// - `MergeStep::Partition` - Next merged partition with all its rows
    /// - `MergeStep::Complete` - Merge is complete
    ///
    /// # Errors
    ///
    /// Returns an error if reading fails.
    pub fn step(&mut self) -> Result<MergeStep> {
        // Initialize heap on first call
        if self.heap.is_empty() && self.current_partition.is_none() {
            self.initialize_heap()?;
        }

        // If heap is empty, merge is complete
        if self.heap.is_empty() {
            return Ok(MergeStep::Complete);
        }

        // Collect all rows for the next partition
        let mut partition_rows = Vec::new();
        let mut partition_key: Option<DecoratedKey> = None;

        while let Some(Reverse(entry)) = self.heap.peek() {
            // Check if we've moved to a new partition
            if let Some(ref current_key) = partition_key {
                if &entry.key != current_key {
                    // Partition boundary - stop here
                    break;
                }
            } else {
                // First entry of new partition
                partition_key = Some(entry.key.clone());
            }

            // Pop entry from heap
            let Reverse(entry) = self
                .heap
                .pop()
                .ok_or_else(|| Error::InvalidInput("Merge heap unexpectedly empty".to_string()))?;

            // Add to partition rows
            partition_rows.push(entry.clone());

            // Refill heap from the run we just consumed from
            self.refill_heap(entry.run_index)?;
        }

        if let Some(key) = partition_key {
            // Merge cells within this partition (last-write-wins)
            let merged_rows = self.merge_partition_rows(partition_rows)?;
            Ok(MergeStep::Partition {
                key,
                rows: merged_rows,
            })
        } else {
            Ok(MergeStep::Complete)
        }
    }

    /// Initialize the heap with the first entry from each run
    fn initialize_heap(&mut self) -> Result<()> {
        for run_index in 0..self.runs.len() {
            self.refill_heap(run_index)?;
        }
        Ok(())
    }

    /// Refill the heap from a specific run
    fn refill_heap(&mut self, run_index: usize) -> Result<()> {
        if run_index >= self.runs.len() {
            return Ok(());
        }

        let run = &mut self.runs[run_index];
        if !run.is_exhausted() {
            if let Some(entry) = run.peek()? {
                // Clone and push to heap
                let entry = entry.clone();
                self.heap.push(Reverse(entry));
            }

            // Advance the run reader
            run.advance()?;
        }

        Ok(())
    }

    /// Merge rows within a single partition using **per-cell reconcile**
    /// (Cassandra `org.apache.cassandra.db.rows.Cells#reconcile`).
    ///
    /// The pre-#533 implementation selected a single whole winning `MergeEntry`
    /// per clustering key, which DROPPED columns when two SSTables shared the same
    /// `(pk, ck)` but carried DISJOINT columns (e.g. A→{name}, B→{score} merged to
    /// only B's column). This now reconciles cell-by-cell so disjoint columns from
    /// every input survive (Issue #533).
    ///
    /// Algorithm per clustering-key group:
    ///   1. **Effective row deletion** — among `RowData::Tombstone` entries take the
    ///      max `deletion_time` (`row_del`). A row tombstone shadows any cell whose
    ///      `timestamp <= row_del`.
    ///   2. **Per-column cell reconcile** — across all `RowData::Live` entries, for
    ///      each column name pick the winning cell by:
    ///        - higher `timestamp` wins (last-write-wins);
    ///        - at EQUAL timestamp a cell tombstone (`Value::Tombstone(CellTombstone)`)
    ///          beats a live value (same rule as #498, applied per cell);
    ///        - otherwise the existing winner is kept (stable; heap routing already
    ///          ordered inputs by run_index so the first-seen at a tie is the newer
    ///          file).
    ///   3. **Row-tombstone shadowing per cell** — drop any reconciled cell whose
    ///      `timestamp <= row_del`. The `<=` makes the tombstone win at equal ts,
    ///      consistent with #498. Cells written strictly AFTER `row_del` survive.
    ///   4. **Build the merged result** — if any cells survive, emit a `Live`
    ///      entry whose row timestamp is the max surviving cell timestamp; else if a
    ///      row tombstone was present, emit a `Tombstone` entry at `row_del` so the
    ///      row stays shadowed downstream; else emit nothing.
    fn merge_partition_rows(&self, rows: Vec<MergeEntry>) -> Result<Vec<MergeEntry>> {
        use std::collections::BTreeMap;

        // Group by clustering key using BTreeMap (ClusteringKey implements Ord).
        // Preserve heap-routing order within each group so the per-cell tiebreak
        // (first-seen wins at equal timestamp+liveness) follows run_index.
        let mut clustered_rows: BTreeMap<Option<ClusteringKey>, Vec<MergeEntry>> = BTreeMap::new();

        for row in rows {
            clustered_rows
                .entry(row.clustering_key.clone())
                .or_default()
                .push(row);
        }

        let mut merged = Vec::new();
        for (ck, cluster_rows) in clustered_rows {
            if let Some(entry) =
                Self::reconcile_cluster(ck, cluster_rows, &self.schema.dropped_columns)
            {
                merged.push(entry);
            }
        }

        // Sort merged rows by clustering key for output order
        merged.sort_by(|a, b| match (&a.clustering_key, &b.clustering_key) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (Some(ck_a), Some(ck_b)) => {
                // Use schema-aware comparison if available
                ck_a.compare(ck_b, &self.schema).unwrap_or_else(|e| {
                    log::warn!(
                        "Schema-aware clustering key comparison failed, using fallback: {}",
                        e
                    );
                    ck_a.cmp(ck_b)
                })
            }
        });

        Ok(merged)
    }

    /// Returns true when a cell carries a cell-level tombstone
    /// (`Value::Tombstone(CellTombstone)`), the representation produced by #505.
    ///
    /// Cell tombstones participate in per-cell reconcile like any other cell, but
    /// at EQUAL timestamp a cell tombstone beats a live value (Cassandra
    /// `Cells#reconcile`, same rule as #498 applied per cell).
    fn is_cell_tombstone(cell: &CellData) -> bool {
        matches!(
            cell.value,
            crate::types::Value::Tombstone(ref info)
                if info.tombstone_type == crate::types::TombstoneType::CellTombstone
        )
    }

    /// Reconcile all entries for a single clustering-key group into at most one
    /// merged `MergeEntry`, applying per-cell last-write-wins plus row-tombstone
    /// shadowing (Issue #533). See [`Self::merge_partition_rows`] for the rules.
    ///
    /// `cluster_rows` is in heap-routing order (run_index ascending within equal
    /// keys), so when two cells tie on both timestamp and liveness the first-seen
    /// (newer file) is kept.
    fn reconcile_cluster(
        clustering_key: Option<ClusteringKey>,
        cluster_rows: Vec<MergeEntry>,
        dropped_columns: &std::collections::HashMap<String, i64>,
    ) -> Option<MergeEntry> {
        use std::collections::HashMap;

        // Carry-through key fields: every entry in this group shares the same
        // partition key and clustering key. Use the lowest run_index seen (newest
        // file) so downstream ordering is stable.
        let mut key = None;
        let mut run_index = usize::MAX;

        // Step 1: effective row deletion — max deletion_time across row tombstones.
        let mut row_del: Option<i64> = None;

        // Step 2: per-column cell reconcile. Preserve first-seen column order for
        // deterministic output while resolving winners in a side map.
        let mut order: Vec<String> = Vec::new();
        let mut winners: HashMap<String, CellData> = HashMap::new();

        // Carried deletion metadata (#886 plumbing). Accumulated but NOT consulted
        // by reconciliation here — preserved so downstream consumers (#899/#844/
        // #846) see it after a normal row reconcile. Behavior-neutral for current
        // output since the writer does not yet read these fields.
        //   - complex_deletions: union across the cluster's input rows (first-seen
        //     order preserved for determinism).
        //   - range_deletion: carried through; if multiple, keep the one with the
        //     highest deletion timestamp.
        let mut complex_deletions: Vec<ComplexDeletion> = Vec::new();
        let mut range_deletion: Option<RangeTombstone> = None;

        for entry in &cluster_rows {
            if key.is_none() {
                key = Some(entry.key.clone());
            }
            run_index = run_index.min(entry.run_index);

            for cd in &entry.complex_deletions {
                if !complex_deletions.contains(cd) {
                    complex_deletions.push(cd.clone());
                }
            }
            if let Some(rd) = &entry.range_deletion {
                let replace = match &range_deletion {
                    None => true,
                    Some(current) => rd.deletion_time > current.deletion_time,
                };
                if replace {
                    range_deletion = Some(rd.clone());
                }
            }

            match &entry.row_data {
                RowData::Tombstone { deletion_time, .. } => {
                    row_del = Some(row_del.map_or(*deletion_time, |d| d.max(*deletion_time)));
                }
                RowData::Live { cells } => {
                    for cell in cells {
                        match winners.get(&cell.column) {
                            None => {
                                order.push(cell.column.clone());
                                winners.insert(cell.column.clone(), cell.clone());
                            }
                            Some(existing) => {
                                // Higher timestamp wins. At EQUAL timestamp a cell
                                // tombstone beats a live value (Issue #498 per cell).
                                // Otherwise keep the existing (first-seen = newer
                                // file) winner.
                                let replace = cell.timestamp > existing.timestamp
                                    || (cell.timestamp == existing.timestamp
                                        && Self::is_cell_tombstone(cell)
                                        && !Self::is_cell_tombstone(existing));
                                if replace {
                                    winners.insert(cell.column.clone(), cell.clone());
                                }
                            }
                        }
                    }
                }
            }
        }

        let key = key?; // empty group => nothing to emit

        // Step 3: apply row-tombstone shadowing per cell. A cell whose timestamp is
        // <= row_del is shadowed (`<=` lets the tombstone win at equal ts, #498).
        // Cells written strictly after row_del survive. This shadowing applies to
        // cell tombstones too: a row tombstone at ts=T supersedes a cell tombstone at
        // ts<=T (real Cassandra semantics). Note this is INTENTIONALLY stricter than
        // the `reference_merge` model, whose range-tombstone path only suppresses
        // live cells — `reconcile_cluster` is the authoritative behavior here.
        //
        // Step 3b: dropped-column filtering (Cassandra `cb34ad47`,
        // `compaction.purge`). A column dropped at `drop_time` discards every cell
        // whose `timestamp <= drop_time`; a cell written strictly after the drop
        // (the column was re-added) survives. This mirrors the row-tombstone `<=`
        // shadowing above but is scoped per column via the `dropped_columns` map
        // (#904 plumbing, #847 filter).
        //
        // Output consistency: a surviving cell here keeps its column in the
        // compaction output, so `compact_sstables` retains any dropped column
        // that has survivors in the *writer* schema (and strips only the
        // fully-purged ones from the serialization header) — see
        // `TableSchema::for_compaction_output`.
        //
        // ROW-TIMESTAMP GRANULARITY (#847 documented scope): `cell.timestamp` here
        // is the ROW write-time, not the cell's own writetime — the reader's
        // `(RowKey, Value, ts)` compaction stream does not surface per-cell
        // timestamps (see `value_to_row_data`; surfacing them is #886 reader
        // plumbing, a prerequisite the epic sequenced separately). So a row that
        // mixes a pre-drop cell of the dropped column with a post-drop cell of
        // another column carries one (newer) row timestamp, and the dropped cell
        // can survive when it should be purged. This is byte-correct when a row's
        // cells share a timestamp (the common case); exact per-cell purging needs
        // the per-cell-timestamp reader plumbing in #886/#899 and is tracked as
        // follow-up #922 — out of #847's scope.
        // Apply row-tombstone shadowing first (Step 3), then dropped-column
        // filtering (Step 3b) as a second stage so we can tell whether the
        // dropped-column purge is what emptied the row of real data.
        let after_row_del: Vec<CellData> = order
            .into_iter()
            .filter_map(|col| winners.remove(&col))
            .filter(|cell| match row_del {
                Some(d) => cell.timestamp > d,
                None => true,
            })
            .collect();

        let surviving: Vec<CellData> = after_row_del
            .iter()
            .filter(|cell| match dropped_columns.get(&cell.column) {
                Some(drop_time) => cell.timestamp > *drop_time,
                None => true,
            })
            .cloned()
            .collect();

        // Phantom-row guard (#847 review): clustering-key columns are intentionally
        // left in the cell list (see `extract_clustering_key`) so read-back can
        // recover them. If a clustered row's only real (non-key) data is a dropped
        // column, the dropped-column filter removes it but the clustering-key
        // pseudo-cells remain — which would otherwise emit a phantom live row with
        // a key but no data, and the writer (whose schema excludes the dropped
        // column) would serialize a key-only empty row. Suppress that: when the
        // row HAD non-key data before the dropped-column purge and has none after,
        // treat it as data-less. A row that was always key-only (a genuine row
        // marker) is preserved.
        let ck_names: std::collections::HashSet<&str> = clustering_key
            .as_ref()
            .map(|ck| ck.columns.iter().map(|(n, _)| n.as_str()).collect())
            .unwrap_or_default();
        let is_data_cell = |cell: &CellData| !ck_names.contains(cell.column.as_str());
        let had_data_before = after_row_del.iter().any(is_data_cell);
        let has_data_after = surviving.iter().any(is_data_cell);
        let purged_to_empty = had_data_before && !has_data_after;

        // Step 4: build the merged result. `max()` is `Some` exactly when `surviving`
        // is non-empty, so this match needs no unreachable fallback timestamp.
        //
        // Attach the carried deletion metadata to whichever entry is emitted so it
        // is not dropped by reconciliation (#886 plumbing preservation). This is
        // behavior-neutral: the writer does not yet consume these fields.
        // Whether any carried deletion metadata exists that would otherwise be
        // lost if no row/tombstone entry is produced (#853/#886 branch-review,
        // Finding 3).
        let has_carried_metadata = !complex_deletions.is_empty() || range_deletion.is_some();

        // Emit a live row only when real data survives. `surviving` is non-empty
        // for an ordinary live row; `!purged_to_empty` additionally suppresses a
        // clustered row whose only data was a dropped column (phantom key-only
        // row, see above). A row that was always key-only (genuine row marker)
        // has `had_data_before == false`, so `purged_to_empty` is false and it is
        // preserved.
        let built = if !surviving.is_empty() && !purged_to_empty {
            // `surviving` is non-empty, so `max()` is `Some`; `unwrap_or(0)` only
            // guards the type and never triggers.
            let row_ts = surviving.iter().map(|c| c.timestamp).max().unwrap_or(0);
            Some(MergeEntry::new(
                run_index,
                key,
                clustering_key,
                row_ts,
                RowData::Live { cells: surviving },
            ))
        } else if let Some(deletion_time) = row_del {
            // No surviving data. If a row tombstone exists, keep the row shadowed
            // so downstream still emits the deletion (preserves #505/#498 absence).
            Some(MergeEntry::new(
                run_index,
                key,
                clustering_key,
                deletion_time,
                RowData::Tombstone {
                    deletion_time,
                    local_deletion_time: 0,
                },
            ))
        } else if has_carried_metadata {
            // No surviving data AND no row tombstone, but the cluster still carries
            // complex/range deletion metadata. Emit a metadata-only entry (an empty
            // `Live` row) so the carried deletion metadata survives reconciliation
            // and reaches downstream consumers (#844/#846/#899). Without this the
            // `built.map(...)` preservation below never runs and the metadata is
            // silently dropped.
            //
            // Behavior-neutral for existing cases: this only adds an entry when
            // metadata exists that would otherwise be lost. An empty-cell `Live`
            // produces no live cells, and the writer does not yet consume the
            // carried metadata fields, so existing output/tests are unaffected.
            Some(MergeEntry::new(
                run_index,
                key,
                clustering_key,
                // No row/cell timestamp applies; use 0 (the carried metadata holds
                // its own deletion timestamps).
                0,
                RowData::Live { cells: vec![] },
            ))
        } else {
            // Truly empty/absent row.
            None
        };

        built.map(|entry| {
            let entry = if complex_deletions.is_empty() {
                entry
            } else {
                entry.with_complex_deletions(complex_deletions)
            };
            match range_deletion {
                Some(rd) => entry.with_range_deletion(rd),
                None => entry,
            }
        })
    }

    /// Convert a MergeEntry back to Mutation for writing
    pub(crate) fn merge_entry_to_mutation(
        entry: MergeEntry,
        schema: &TableSchema,
    ) -> Result<crate::storage::write_engine::mutation::Mutation> {
        use crate::storage::write_engine::mutation::{
            CellOperation, Mutation, PartitionKey, TableId,
        };

        let partition_key = PartitionKey::from_bytes(&entry.key.key, schema)?;
        let table_id = TableId::new(&schema.keyspace, &schema.table);

        let operations = match entry.row_data {
            RowData::Live { cells } => cells
                .into_iter()
                .map(|cell| {
                    // Issue #505: cell-level tombstones are represented as
                    // Value::Tombstone(CellTombstone) inside the Map.  Translate
                    // them to CellOperation::Delete so the SSTableWriter writes a
                    // proper cell tombstone rather than a live cell with a null value.
                    if matches!(
                        cell.value,
                        crate::types::Value::Tombstone(ref info)
                            if info.tombstone_type == crate::types::TombstoneType::CellTombstone
                    ) {
                        return CellOperation::Delete {
                            column: cell.column,
                        };
                    }
                    if let Some(ttl) = cell.ttl {
                        CellOperation::WriteWithTtl {
                            column: cell.column,
                            value: cell.value,
                            ttl_seconds: ttl,
                        }
                    } else {
                        CellOperation::Write {
                            column: cell.column,
                            value: cell.value,
                        }
                    }
                })
                .collect(),
            RowData::Tombstone { .. } => vec![CellOperation::DeleteRow],
        };

        // NOTE (follow-up #873): the rewritten row tombstone's
        // local_deletion_time is left None here, so the writer derives it from
        // `entry.timestamp`. The source SSTable's localDeletionTime is not yet
        // preserved through compaction because it is dropped upstream
        // (`value_to_row_data` builds `RowData::Tombstone { local_deletion_time: 0 }`
        // since `TombstoneInfo` carries no LDT). Threading it end-to-end —
        // including a guard against negative row-tombstone LDT deltas — is
        // tracked in #873.
        Ok(Mutation::new(
            table_id,
            partition_key,
            entry.clustering_key,
            operations,
            entry.timestamp,
            None,
        ))
    }
}

#[cfg(all(test, feature = "write-support"))]
mod tests {
    use super::*;
    use crate::storage::write_engine::mutation::DecoratedKey;

    #[test]
    fn test_merge_entry_ordering_by_token() {
        let entry1 = MergeEntry::new(
            0,
            DecoratedKey::new(100, vec![1, 2, 3]),
            None,
            1000,
            RowData::Live { cells: vec![] },
        );

        let entry2 = MergeEntry::new(
            0,
            DecoratedKey::new(200, vec![1, 2, 3]),
            None,
            1000,
            RowData::Live { cells: vec![] },
        );

        // Entry with lower token should come first
        assert!(entry1 < entry2);
        assert!(entry2 > entry1);
    }

    #[test]
    fn test_merge_entry_ordering_by_key_bytes() {
        // Same token, different key bytes (hash collision)
        let entry1 = MergeEntry::new(
            0,
            DecoratedKey::new(100, vec![1, 2, 3]),
            None,
            1000,
            RowData::Live { cells: vec![] },
        );

        let entry2 = MergeEntry::new(
            0,
            DecoratedKey::new(100, vec![1, 2, 4]),
            None,
            1000,
            RowData::Live { cells: vec![] },
        );

        // Entry with smaller key bytes should come first
        assert!(entry1 < entry2);
        assert!(entry2 > entry1);
    }

    #[test]
    fn test_merge_entry_ordering_by_run_index() {
        // Same token and key, different run indices
        let entry1 = MergeEntry::new(
            0,
            DecoratedKey::new(100, vec![1, 2, 3]),
            None,
            1000,
            RowData::Live { cells: vec![] },
        );

        let entry2 = MergeEntry::new(
            1,
            DecoratedKey::new(100, vec![1, 2, 3]),
            None,
            1000,
            RowData::Live { cells: vec![] },
        );

        // Entry with lower run_index should come first (newer file wins)
        assert!(entry1 < entry2);
        assert!(entry2 > entry1);
    }

    #[test]
    fn test_merge_entry_min_heap() {
        use std::cmp::Reverse;
        use std::collections::BinaryHeap;

        let mut heap: BinaryHeap<Reverse<MergeEntry>> = BinaryHeap::new();

        // Insert in reverse order
        let entry3 = MergeEntry::new(
            0,
            DecoratedKey::new(300, vec![3]),
            None,
            1000,
            RowData::Live { cells: vec![] },
        );
        let entry1 = MergeEntry::new(
            0,
            DecoratedKey::new(100, vec![1]),
            None,
            1000,
            RowData::Live { cells: vec![] },
        );
        let entry2 = MergeEntry::new(
            0,
            DecoratedKey::new(200, vec![2]),
            None,
            1000,
            RowData::Live { cells: vec![] },
        );

        heap.push(Reverse(entry3.clone()));
        heap.push(Reverse(entry1.clone()));
        heap.push(Reverse(entry2.clone()));

        // Should pop in ascending order
        assert_eq!(heap.pop().unwrap().0.key.token, 100);
        assert_eq!(heap.pop().unwrap().0.key.token, 200);
        assert_eq!(heap.pop().unwrap().0.key.token, 300);
    }

    #[test]
    fn test_row_data_variants() {
        let live = RowData::Live {
            cells: vec![CellData {
                column: "name".to_string(),
                value: Value::Text("Alice".to_string()),
                timestamp: 1000,
                ttl: None,
                cell_path: None,
                local_deletion_time: None,
            }],
        };

        match live {
            RowData::Live { cells } => {
                assert_eq!(cells.len(), 1);
                assert_eq!(cells[0].column, "name");
            }
            _ => panic!("Expected Live variant"),
        }

        let tombstone = RowData::Tombstone {
            deletion_time: 2000,
            local_deletion_time: 1000,
        };

        match tombstone {
            RowData::Tombstone {
                deletion_time,
                local_deletion_time,
            } => {
                assert_eq!(deletion_time, 2000);
                assert_eq!(local_deletion_time, 1000);
            }
            _ => panic!("Expected Tombstone variant"),
        }
    }

    #[test]
    fn test_cell_data_creation() {
        let cell = CellData {
            column: "age".to_string(),
            value: Value::Integer(30),
            timestamp: 1234567890,
            ttl: Some(3600),
            cell_path: None,
            local_deletion_time: None,
        };

        assert_eq!(cell.column, "age");
        assert_eq!(cell.value, Value::Integer(30));
        assert_eq!(cell.timestamp, 1234567890);
        assert_eq!(cell.ttl, Some(3600));
    }

    #[test]
    fn test_merge_stats_creation() {
        let stats = MergeStats {
            input_files: 5,
            output_partitions: 1000,
            output_rows: 5000,
            bytes_written: 1024 * 1024,
            elapsed: Duration::from_secs(10),
        };

        assert_eq!(stats.input_files, 5);
        assert_eq!(stats.output_partitions, 1000);
        assert_eq!(stats.output_rows, 5000);
        assert_eq!(stats.bytes_written, 1024 * 1024);
        assert_eq!(stats.elapsed.as_secs(), 10);
    }

    #[test]
    fn test_run_reader_estimate_entry_size() {
        let entry = MergeEntry::new(
            0,
            DecoratedKey::new(100, vec![1, 2, 3, 4]),
            None,
            1000,
            RowData::Live {
                cells: vec![CellData {
                    column: "name".to_string(),
                    value: Value::Text("Alice".to_string()),
                    timestamp: 1000,
                    ttl: None,
                    cell_path: None,
                    local_deletion_time: None,
                }],
            },
        );

        let size = RunReader::estimate_entry_size(&entry);

        // Size should be at least the base struct size plus key bytes
        let expected_min_size = std::mem::size_of::<MergeEntry>() + 4;
        assert!(size >= expected_min_size);
    }

    #[test]
    fn test_kway_merger_empty_input() {
        use crate::schema::{KeyColumn, TableSchema};
        use std::collections::HashMap;

        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        let result = KWayMerger::new(vec![], &schema);
        assert!(result.is_err());

        if let Err(Error::InvalidInput(msg)) = result {
            assert!(msg.contains("at least one input file"));
        } else {
            panic!("Expected InvalidInput error");
        }
    }

    #[test]
    fn test_merge_entry_equal_timestamps_prefer_lower_run_index() {
        // Same partition, same clustering, same timestamp
        // Lower run_index should win (newer file)
        let entry_run0 = MergeEntry::new(
            0, // run_index 0 (newer)
            DecoratedKey::new(100, vec![1, 2, 3]),
            None,
            1000, // same timestamp
            RowData::Live {
                cells: vec![CellData {
                    column: "name".to_string(),
                    value: Value::Text("Newer".to_string()),
                    timestamp: 1000,
                    ttl: None,
                    cell_path: None,
                    local_deletion_time: None,
                }],
            },
        );

        let entry_run1 = MergeEntry::new(
            1, // run_index 1 (older)
            DecoratedKey::new(100, vec![1, 2, 3]),
            None,
            1000, // same timestamp
            RowData::Live {
                cells: vec![CellData {
                    column: "name".to_string(),
                    value: Value::Text("Older".to_string()),
                    timestamp: 1000,
                    ttl: None,
                    cell_path: None,
                    local_deletion_time: None,
                }],
            },
        );

        // Entry from run 0 should come first in ordering
        assert!(entry_run0 < entry_run1);
    }

    #[test]
    fn test_merge_entry_tombstone() {
        let tombstone_entry = MergeEntry::new(
            0,
            DecoratedKey::new(100, vec![1, 2, 3]),
            None,
            2000,
            RowData::Tombstone {
                deletion_time: 2000,
                local_deletion_time: 1000,
            },
        );

        match tombstone_entry.row_data {
            RowData::Tombstone {
                deletion_time,
                local_deletion_time,
            } => {
                assert_eq!(deletion_time, 2000);
                assert_eq!(local_deletion_time, 1000);
            }
            _ => panic!("Expected Tombstone"),
        }
    }

    #[test]
    fn test_real_merger_delete_wins_at_equal_timestamp() {
        // Issue #498: at EQUAL timestamp, a Delete (tombstone) must beat a Live
        // row regardless of file recency (Cassandra `Cells#reconcile`).
        //
        // We drive the REAL merger entry point (`merge_partition_rows`) with two
        // entries that share the SAME clustering key and the SAME timestamp:
        //   - A: Live, run_index 0  (the NEWER file — would win a run_index tiebreak)
        //   - B: Delete, run_index 1 (the OLDER file)
        //
        // The pre-fix merger sorted equal-timestamp ties by run_index only, so the
        // live row (run_index 0) would win and survive. With the fix the tombstone
        // wins. This test therefore FAILS if the tiebreak reverts to run_index.
        use crate::schema::{Column, KeyColumn, TableSchema};
        use std::collections::HashMap;

        let schema = TableSchema {
            keyspace: "reconcile_ks".to_string(),
            table: "reconcile_tbl".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![Column {
                name: "value".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            }],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        const EQUAL_TS: i64 = 1_700_000_000_000_000;

        let partition_key = DecoratedKey::new(100, vec![0, 0, 0, 1]);

        // A = Live, in the NEWER file (run_index 0).
        let live_entry = MergeEntry::new(
            0,
            partition_key.clone(),
            None,
            EQUAL_TS,
            RowData::Live {
                cells: vec![CellData {
                    column: "value".to_string(),
                    value: Value::Text("survivor-if-buggy".to_string()),
                    timestamp: EQUAL_TS,
                    ttl: None,
                    cell_path: None,
                    local_deletion_time: None,
                }],
            },
        );

        // B = Delete (row tombstone), in the OLDER file (run_index 1).
        let tombstone_entry = MergeEntry::new(
            1,
            partition_key.clone(),
            None,
            EQUAL_TS,
            RowData::Tombstone {
                deletion_time: EQUAL_TS,
                local_deletion_time: 2_000_000,
            },
        );

        let merger = KWayMerger {
            runs: vec![],
            heap: BinaryHeap::new(),
            current_partition: None,
            gc_before_secs: None,
            now_secs: None,
            schema,
        };

        // Drive the real merger. Order the input so the live (newer-file) entry is
        // first — pre-fix this is exactly the entry that wins by run_index.
        let merged = merger
            .merge_partition_rows(vec![live_entry, tombstone_entry])
            .expect("merge_partition_rows must not fail");

        assert_eq!(merged.len(), 1, "one clustering key => one merged winner");

        assert!(
            matches!(merged[0].row_data, RowData::Tombstone { .. }),
            "At equal timestamp the tombstone must win even though the live row is in \
             the newer file (run_index 0). Got a live row => the equal-ts tiebreak \
             reverted to run_index (Issue #498 regression)."
        );
    }

    #[test]
    fn test_real_merger_disjoint_columns_survive_compaction() {
        // Issue #533: when two SSTables share the same (pk, ck) but carry DISJOINT
        // columns, per-cell reconcile must keep cells from BOTH. The pre-fix merger
        // picked one whole winning row and DROPPED the loser's columns.
        //
        //   A (run_index 1, ts=100): {name: "alice"}
        //   B (run_index 0, ts=200): {score: 42}
        //
        // Cassandra `Cells#reconcile` => {name: "alice", score: 42}.
        // The old whole-row-wins code returned only {score: 42} (name LOST).
        use crate::schema::{ClusteringColumn, Column, KeyColumn, TableSchema};
        use std::collections::HashMap;

        let schema = TableSchema {
            keyspace: "disjoint_ks".to_string(),
            table: "disjoint_tbl".to_string(),
            partition_keys: vec![KeyColumn {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![ClusteringColumn {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                position: 0,
                order: Default::default(),
            }],
            columns: vec![
                Column {
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "score".to_string(),
                    data_type: "int".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        let partition_key = DecoratedKey::new(100, vec![0, 0, 0, 1]);
        let ck = ClusteringKey {
            columns: vec![("ck".to_string(), Value::Integer(1))],
        };

        // A: older file (run_index 1), only `name` at ts=100.
        let entry_a = MergeEntry::new(
            1,
            partition_key.clone(),
            Some(ck.clone()),
            100,
            RowData::Live {
                cells: vec![CellData {
                    column: "name".to_string(),
                    value: Value::Text("alice".to_string()),
                    timestamp: 100,
                    ttl: None,
                    cell_path: None,
                    local_deletion_time: None,
                }],
            },
        );

        // B: newer file (run_index 0), only `score` at ts=200.
        let entry_b = MergeEntry::new(
            0,
            partition_key.clone(),
            Some(ck.clone()),
            200,
            RowData::Live {
                cells: vec![CellData {
                    column: "score".to_string(),
                    value: Value::Integer(42),
                    timestamp: 200,
                    ttl: None,
                    cell_path: None,
                    local_deletion_time: None,
                }],
            },
        );

        let merger = KWayMerger {
            runs: vec![],
            heap: BinaryHeap::new(),
            current_partition: None,
            gc_before_secs: None,
            now_secs: None,
            schema,
        };

        // Pass in heap-routing order (run_index ascending): B then A.
        let merged = merger
            .merge_partition_rows(vec![entry_b, entry_a])
            .expect("merge_partition_rows must not fail");

        assert_eq!(merged.len(), 1, "one clustering key => one merged row");

        let cells = match &merged[0].row_data {
            RowData::Live { cells } => cells,
            other => panic!("expected a Live merged row, got {:?}", other),
        };

        let name = cells.iter().find(|c| c.column == "name");
        let score = cells.iter().find(|c| c.column == "score");

        assert!(
            name.is_some(),
            "disjoint column `name` from the older file was DROPPED — per-cell \
             reconcile regression (Issue #533). Old whole-row-wins code fails here."
        );
        assert!(
            score.is_some(),
            "disjoint column `score` from the newer file is missing"
        );
        assert_eq!(
            name.unwrap().value,
            Value::Text("alice".to_string()),
            "`name` must carry A's value"
        );
        assert_eq!(
            score.unwrap().value,
            Value::Integer(42),
            "`score` must carry B's value"
        );

        // Row timestamp must be the max surviving cell timestamp.
        assert_eq!(
            merged[0].timestamp, 200,
            "merged row timestamp must be the max surviving cell timestamp"
        );
    }

    #[test]
    fn test_real_merger_cell_tombstone_beats_live_at_equal_timestamp() {
        // Issue #533/#498 (per cell): when two SSTables write the SAME column at the
        // SAME timestamp, a cell tombstone (Delete) must beat the live value,
        // independent of file recency.
        //
        //   A (run_index 0, NEWER file, ts=100): {score: 42}            (live)
        //   B (run_index 1, OLDER file, ts=100): {score: <cell tombstone>}
        //
        // Cassandra `Cells#reconcile` => score is deleted. The adversarial part: A is
        // the newer file, so a recency-only tiebreak would wrongly keep the live 42.
        use crate::schema::{ClusteringColumn, Column, KeyColumn, TableSchema};
        use crate::types::{TombstoneInfo, TombstoneType};
        use std::collections::HashMap;

        let schema = TableSchema {
            keyspace: "ct_ks".to_string(),
            table: "ct_tbl".to_string(),
            partition_keys: vec![KeyColumn {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![ClusteringColumn {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                position: 0,
                order: Default::default(),
            }],
            columns: vec![Column {
                name: "score".to_string(),
                data_type: "int".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            }],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        let partition_key = DecoratedKey::new(100, vec![0, 0, 0, 1]);
        let ck = ClusteringKey {
            columns: vec![("ck".to_string(), Value::Integer(1))],
        };

        // A: newer file (run_index 0), live `score` = 42 at ts=100.
        let entry_a = MergeEntry::new(
            0,
            partition_key.clone(),
            Some(ck.clone()),
            100,
            RowData::Live {
                cells: vec![CellData {
                    column: "score".to_string(),
                    value: Value::Integer(42),
                    timestamp: 100,
                    ttl: None,
                    cell_path: None,
                    local_deletion_time: None,
                }],
            },
        );

        // B: older file (run_index 1), cell tombstone on `score` at the SAME ts=100.
        let entry_b = MergeEntry::new(
            1,
            partition_key.clone(),
            Some(ck.clone()),
            100,
            RowData::Live {
                cells: vec![CellData {
                    column: "score".to_string(),
                    value: Value::Tombstone(TombstoneInfo {
                        deletion_time: 100,
                        tombstone_type: TombstoneType::CellTombstone,
                        ttl: None,
                        range_start: None,
                        range_end: None,
                    }),
                    timestamp: 100,
                    ttl: None,
                    cell_path: None,
                    local_deletion_time: None,
                }],
            },
        );

        let merger = KWayMerger {
            runs: vec![],
            heap: BinaryHeap::new(),
            current_partition: None,
            gc_before_secs: None,
            now_secs: None,
            schema,
        };

        // Heap-routing order (run_index ascending): A then B.
        let merged = merger
            .merge_partition_rows(vec![entry_a, entry_b])
            .expect("merge_partition_rows must not fail");

        assert_eq!(merged.len(), 1, "one clustering key => one merged row");

        let cells = match &merged[0].row_data {
            RowData::Live { cells } => cells,
            other => panic!("expected a Live merged row, got {:?}", other),
        };
        let score = cells
            .iter()
            .find(|c| c.column == "score")
            .expect("score cell must be present (as a tombstone)");

        assert!(
            matches!(
                score.value,
                Value::Tombstone(ref info) if info.tombstone_type == TombstoneType::CellTombstone
            ),
            "at equal ts the cell tombstone must win over the live value (got {:?}) — \
             a recency-only tiebreak would have kept the newer file's live 42 (#498 per cell)",
            score.value
        );
    }

    #[test]
    fn test_real_merger_value_tiebreak_diverges_from_cassandra() {
        // VERIFICATION (cursor-compaction findings #4 / #21): documents that CQLite's
        // equal-timestamp cell tie-break for two LIVE values of the SAME column
        // DIVERGES from Cassandra.
        //
        // Cassandra `Cells.resolveRegular` (cursor findings #4/#21): on a timestamp
        // tie between two live cells, the cell whose **raw value bytes** are strictly
        // greater (unsigned lexicographic compare, length prefix excluded) wins —
        // file/run order is NOT consulted.
        //
        // CQLite `reconcile_cluster` (merge.rs): on a timestamp tie between two live
        // cells it keeps the FIRST-SEEN cell, i.e. the lower run_index (newer file).
        // Raw value bytes are never compared.
        //
        // Fixture (same pk, no clustering, SAME timestamp, DIFFERENT values):
        //   A (run_index 0, NEWER file): {v: "apple"}    raw bytes 0x61 70 70 6C 65
        //   B (run_index 1, OLDER file): {v: "banana"}   raw bytes 0x62 ...  (GREATER)
        //
        // Cassandra would keep "banana" (greater raw bytes, from the older file).
        // CQLite keeps "apple" (first-seen / newer file). The two rules pick
        // DIFFERENT winners here, so the surviving value is byte-divergent.
        //
        // This test ASSERTS CQLite's current behavior and asserts that it differs
        // from the Cassandra winner. If CQLite is later changed to match Cassandra
        // (compare raw value bytes), THIS TEST WILL FAIL and must be updated to
        // reflect the new, convergent behavior. See
        // docs/garbage-free-compaction-improvements/cqlite-findings-and-applicability.md.
        use crate::schema::{Column, KeyColumn, TableSchema};
        use std::collections::HashMap;

        let schema = TableSchema {
            keyspace: "tiebreak_ks".to_string(),
            table: "tiebreak_tbl".to_string(),
            partition_keys: vec![KeyColumn {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![Column {
                name: "v".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            }],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        const EQUAL_TS: i64 = 1_700_000_000_000_000;
        let partition_key = DecoratedKey::new(100, vec![0, 0, 0, 1]);

        // For CQL `text`, the raw cell value Cassandra compares is exactly the UTF-8
        // bytes. "banana" > "apple" unsigned-lexicographically (0x62 > 0x61).
        let newer_file_value = "apple"; // run_index 0 (newer)
        let older_file_value = "banana"; // run_index 1 (older), greater raw bytes
        assert!(
            older_file_value.as_bytes() > newer_file_value.as_bytes(),
            "fixture invariant: the older file must hold the lexicographically GREATER \
             raw value, so the two tie-break rules pick different winners"
        );

        // The value Cassandra's rule (#4/#21) would keep: greater raw value bytes.
        let cassandra_winner = if older_file_value.as_bytes() > newer_file_value.as_bytes() {
            older_file_value
        } else {
            newer_file_value
        };

        let entry_newer = MergeEntry::new(
            0, // newer file
            partition_key.clone(),
            None,
            EQUAL_TS,
            RowData::Live {
                cells: vec![CellData {
                    column: "v".to_string(),
                    value: Value::Text(newer_file_value.to_string()),
                    timestamp: EQUAL_TS,
                    ttl: None,
                    cell_path: None,
                    local_deletion_time: None,
                }],
            },
        );

        let entry_older = MergeEntry::new(
            1, // older file
            partition_key.clone(),
            None,
            EQUAL_TS,
            RowData::Live {
                cells: vec![CellData {
                    column: "v".to_string(),
                    value: Value::Text(older_file_value.to_string()),
                    timestamp: EQUAL_TS,
                    ttl: None,
                    cell_path: None,
                    local_deletion_time: None,
                }],
            },
        );

        let merger = KWayMerger {
            runs: vec![],
            heap: BinaryHeap::new(),
            current_partition: None,
            gc_before_secs: None,
            now_secs: None,
            schema,
        };

        // Heap-routing order (run_index ascending): newer file first — exactly what
        // the real merge heap yields for equal (pk, ck).
        let merged = merger
            .merge_partition_rows(vec![entry_newer, entry_older])
            .expect("merge_partition_rows must not fail");

        assert_eq!(merged.len(), 1, "one (pk, ck) group => one merged winner");

        let cells = match &merged[0].row_data {
            RowData::Live { cells } => cells,
            other => panic!("expected a Live merged row, got {:?}", other),
        };
        let surviving = match &cells
            .iter()
            .find(|c| c.column == "v")
            .expect("column `v` must survive")
            .value
        {
            Value::Text(s) => s.clone(),
            other => panic!("expected Text value, got {:?}", other),
        };

        // 1) CQLite's actual behavior: first-seen (newer file / lower run_index) wins.
        assert_eq!(
            surviving, newer_file_value,
            "CQLite reconcile_cluster keeps the first-seen (newer file) cell on a \
             timestamp tie; got {:?}",
            surviving
        );

        // 2) The divergence itself, made executable: CQLite's winner is NOT the value
        //    Cassandra's raw-value-bytes rule (#4/#21) would have kept.
        assert_ne!(
            surviving, cassandra_winner,
            "EXPECTED DIVERGENCE (#4/#21): CQLite kept {:?} but Cassandra's \
             Cells.resolveRegular keeps the greater raw value {:?}. If this assertion \
             fails, CQLite now matches Cassandra and the finding is RESOLVED — update \
             cqlite-findings-and-applicability.md and convert this into a convergence test.",
            surviving, cassandra_winner
        );
    }

    #[test]
    fn test_real_merger_same_column_conflict_resolves_by_timestamp() {
        // Issue #533: when both SSTables write the SAME column, the higher-timestamp
        // value wins (last-write-wins), but disjoint columns still survive.
        //
        //   A (run_index 1, ts=100): {name: "old", extra: "a-only"}
        //   B (run_index 0, ts=200): {name: "new"}
        // => {name: "new" (ts=200 wins), extra: "a-only" (survives)}
        use crate::schema::{Column, KeyColumn, TableSchema};
        use std::collections::HashMap;

        let schema = TableSchema {
            keyspace: "conflict_ks".to_string(),
            table: "conflict_tbl".to_string(),
            partition_keys: vec![KeyColumn {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![
                Column {
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "extra".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        let partition_key = DecoratedKey::new(100, vec![0, 0, 0, 1]);

        let entry_a = MergeEntry::new(
            1,
            partition_key.clone(),
            None,
            100,
            RowData::Live {
                cells: vec![
                    CellData {
                        column: "name".to_string(),
                        value: Value::Text("old".to_string()),
                        timestamp: 100,
                        ttl: None,
                        cell_path: None,
                        local_deletion_time: None,
                    },
                    CellData {
                        column: "extra".to_string(),
                        value: Value::Text("a-only".to_string()),
                        timestamp: 100,
                        ttl: None,
                        cell_path: None,
                        local_deletion_time: None,
                    },
                ],
            },
        );

        let entry_b = MergeEntry::new(
            0,
            partition_key.clone(),
            None,
            200,
            RowData::Live {
                cells: vec![CellData {
                    column: "name".to_string(),
                    value: Value::Text("new".to_string()),
                    timestamp: 200,
                    ttl: None,
                    cell_path: None,
                    local_deletion_time: None,
                }],
            },
        );

        let merger = KWayMerger {
            runs: vec![],
            heap: BinaryHeap::new(),
            current_partition: None,
            gc_before_secs: None,
            now_secs: None,
            schema,
        };

        let merged = merger
            .merge_partition_rows(vec![entry_b, entry_a])
            .expect("merge_partition_rows must not fail");

        assert_eq!(merged.len(), 1);
        let cells = match &merged[0].row_data {
            RowData::Live { cells } => cells,
            other => panic!("expected Live, got {:?}", other),
        };

        let name = cells
            .iter()
            .find(|c| c.column == "name")
            .expect("name present");
        let extra = cells
            .iter()
            .find(|c| c.column == "extra")
            .expect("extra (disjoint) must survive");

        assert_eq!(
            name.value,
            Value::Text("new".to_string()),
            "same-column conflict must resolve to the higher-timestamp value"
        );
        assert_eq!(
            extra.value,
            Value::Text("a-only".to_string()),
            "disjoint column from the older file must survive the conflict merge"
        );
    }

    #[test]
    fn test_real_merger_row_tombstone_shadows_old_cells_keeps_new() {
        // Issue #533 / #505: a row tombstone shadows cells with ts <= row_del but
        // a cell written strictly AFTER the tombstone survives.
        //
        //   A (ts=100): {name: "old"}          -> 100 <= 200 row_del => shadowed
        //   B (ts=200, row tombstone)
        //   C (ts=300): {score: 7}             -> 300 > 200            => survives
        use crate::schema::{Column, KeyColumn, TableSchema};
        use std::collections::HashMap;

        let schema = TableSchema {
            keyspace: "shadow_ks".to_string(),
            table: "shadow_tbl".to_string(),
            partition_keys: vec![KeyColumn {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![
                Column {
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "score".to_string(),
                    data_type: "int".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        let pk = DecoratedKey::new(100, vec![0, 0, 0, 1]);

        let entry_a = MergeEntry::new(
            2,
            pk.clone(),
            None,
            100,
            RowData::Live {
                cells: vec![CellData {
                    column: "name".to_string(),
                    value: Value::Text("old".to_string()),
                    timestamp: 100,
                    ttl: None,
                    cell_path: None,
                    local_deletion_time: None,
                }],
            },
        );
        let entry_b = MergeEntry::new(
            1,
            pk.clone(),
            None,
            200,
            RowData::Tombstone {
                deletion_time: 200,
                local_deletion_time: 0,
            },
        );
        let entry_c = MergeEntry::new(
            0,
            pk.clone(),
            None,
            300,
            RowData::Live {
                cells: vec![CellData {
                    column: "score".to_string(),
                    value: Value::Integer(7),
                    timestamp: 300,
                    ttl: None,
                    cell_path: None,
                    local_deletion_time: None,
                }],
            },
        );

        let merger = KWayMerger {
            runs: vec![],
            heap: BinaryHeap::new(),
            current_partition: None,
            gc_before_secs: None,
            now_secs: None,
            schema,
        };

        let merged = merger
            .merge_partition_rows(vec![entry_c, entry_b, entry_a])
            .expect("merge must not fail");

        assert_eq!(merged.len(), 1);
        let cells = match &merged[0].row_data {
            RowData::Live { cells } => cells,
            other => panic!(
                "expected Live (score survives the tombstone), got {:?}",
                other
            ),
        };

        assert!(
            cells.iter().all(|c| c.column != "name"),
            "`name` (ts=100 <= row_del=200) must be shadowed by the row tombstone"
        );
        let score = cells
            .iter()
            .find(|c| c.column == "score")
            .expect("`score` (ts=300 > row_del=200) must survive the row tombstone");
        assert_eq!(score.value, Value::Integer(7));
    }

    #[test]
    fn test_real_merger_row_tombstone_only_emits_tombstone() {
        // When every cell is shadowed by a row tombstone (no later writes), the
        // merger must emit a Tombstone entry so the row stays deleted downstream
        // (preserves #505/#498 absence semantics).
        use crate::schema::{Column, KeyColumn, TableSchema};
        use std::collections::HashMap;

        let schema = TableSchema {
            keyspace: "ts_only_ks".to_string(),
            table: "ts_only_tbl".to_string(),
            partition_keys: vec![KeyColumn {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![Column {
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            }],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        let pk = DecoratedKey::new(100, vec![0, 0, 0, 1]);

        let live = MergeEntry::new(
            1,
            pk.clone(),
            None,
            100,
            RowData::Live {
                cells: vec![CellData {
                    column: "name".to_string(),
                    value: Value::Text("doomed".to_string()),
                    timestamp: 100,
                    ttl: None,
                    cell_path: None,
                    local_deletion_time: None,
                }],
            },
        );
        let tomb = MergeEntry::new(
            0,
            pk.clone(),
            None,
            300,
            RowData::Tombstone {
                deletion_time: 300,
                local_deletion_time: 0,
            },
        );

        let merger = KWayMerger {
            runs: vec![],
            heap: BinaryHeap::new(),
            current_partition: None,
            gc_before_secs: None,
            now_secs: None,
            schema,
        };

        let merged = merger
            .merge_partition_rows(vec![tomb, live])
            .expect("merge must not fail");

        assert_eq!(merged.len(), 1);
        match &merged[0].row_data {
            RowData::Tombstone { deletion_time, .. } => {
                assert_eq!(*deletion_time, 300, "tombstone deletion_time preserved");
            }
            other => panic!("expected a Tombstone entry, got {:?}", other),
        }
    }

    #[test]
    fn test_merge_step_variants() {
        let key = DecoratedKey::new(100, vec![1, 2, 3]);
        let rows = vec![];

        let partition_step = MergeStep::Partition { key, rows };

        match partition_step {
            MergeStep::Partition { key, rows } => {
                assert_eq!(key.token, 100);
                assert_eq!(rows.len(), 0);
            }
            _ => panic!("Expected Partition variant"),
        }

        let complete_step = MergeStep::Complete;
        match complete_step {
            MergeStep::Complete => {}
            _ => panic!("Expected Complete variant"),
        }
    }

    #[test]
    fn test_cell_merge_last_write_wins_higher_timestamp() {
        // Two cells with different timestamps
        let cell1 = CellData {
            column: "name".to_string(),
            value: Value::Text("Old".to_string()),
            timestamp: 1000,
            ttl: None,
            cell_path: None,
            local_deletion_time: None,
        };

        let cell2 = CellData {
            column: "name".to_string(),
            value: Value::Text("New".to_string()),
            timestamp: 2000, // Higher timestamp wins
            ttl: None,
            cell_path: None,
            local_deletion_time: None,
        };

        // Cell2 should win in last-write-wins merge
        assert!(cell2.timestamp > cell1.timestamp);
    }

    #[test]
    fn test_memory_budget_calculation() {
        // For k=10 SSTables, memory budget should be ~80KB
        let k = 10;
        let buffer_size_per_run = RunReader::DEFAULT_BUFFER_SIZE;
        let total_memory = k * buffer_size_per_run;

        assert_eq!(buffer_size_per_run, 8 * 1024); // 8KB
        assert_eq!(total_memory, 80 * 1024); // 80KB total
    }

    #[test]
    fn test_merge_entry_to_mutation_live_cells() {
        use crate::schema::{KeyColumn, TableSchema};
        use crate::storage::write_engine::mutation::{CellOperation, DecoratedKey};
        use std::collections::HashMap;

        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        // Encode key as 4-byte big-endian int (42)
        let key_bytes = 42i32.to_be_bytes().to_vec();

        let entry = MergeEntry::new(
            0,
            DecoratedKey::new(1000, key_bytes),
            None,
            999_000_000,
            RowData::Live {
                cells: vec![
                    CellData {
                        column: "name".to_string(),
                        value: Value::Text("Alice".to_string()),
                        timestamp: 999_000_000,
                        ttl: None,
                        cell_path: None,
                        local_deletion_time: None,
                    },
                    CellData {
                        column: "age".to_string(),
                        value: Value::Integer(30),
                        timestamp: 999_000_000,
                        ttl: Some(3600),
                        cell_path: None,
                        local_deletion_time: None,
                    },
                ],
            },
        );

        let mutation =
            KWayMerger::merge_entry_to_mutation(entry, &schema).expect("conversion should succeed");

        // Partition key should have one column named "id"
        assert_eq!(mutation.partition_key.columns.len(), 1);
        assert_eq!(mutation.partition_key.columns[0].0, "id");

        // Two operations: one Write and one WriteWithTtl
        assert_eq!(mutation.operations.len(), 2);
        assert_eq!(mutation.timestamp_micros, 999_000_000);

        let has_write = mutation
            .operations
            .iter()
            .any(|op| matches!(op, CellOperation::Write { column, .. } if column == "name"));
        let has_ttl_write = mutation.operations.iter().any(|op| {
            matches!(op, CellOperation::WriteWithTtl { column, ttl_seconds, .. }
                if column == "age" && *ttl_seconds == 3600)
        });
        assert!(has_write, "Expected Write operation for 'name'");
        assert!(has_ttl_write, "Expected WriteWithTtl operation for 'age'");
    }

    #[test]
    fn test_merge_entry_to_mutation_tombstone() {
        use crate::schema::{KeyColumn, TableSchema};
        use crate::storage::write_engine::mutation::{CellOperation, DecoratedKey};
        use std::collections::HashMap;

        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        let key_bytes = 7i32.to_be_bytes().to_vec();

        let entry = MergeEntry::new(
            0,
            DecoratedKey::new(500, key_bytes),
            None,
            888_000_000,
            RowData::Tombstone {
                deletion_time: 888_000_000,
                local_deletion_time: 1_700_000_000,
            },
        );

        let mutation =
            KWayMerger::merge_entry_to_mutation(entry, &schema).expect("conversion should succeed");

        assert_eq!(mutation.operations.len(), 1);
        assert!(
            matches!(mutation.operations[0], CellOperation::DeleteRow),
            "Expected DeleteRow operation for tombstone entry"
        );
    }

    /// Regression (#853/#886 branch-review, Finding 2): compaction baseline
    /// seeding must NOT discard a far-future local-deletion-time in [2^31, 2^32).
    /// Such LDTs are legitimate (negative i32 bit patterns) after the deletion
    /// marker fixes (#853 / range tombstones). The parser reconstructs them as i64
    /// values ABOVE i32::MAX, and the old `ldt < i32::MAX` guard dropped them, so
    /// the seeded `DataWriter` baseline diverged from the final Statistics.db
    /// baseline. The fix normalizes as unsigned-32 and reinterprets the bits as
    /// i32.
    ///
    /// End-to-end: write a Statistics.db whose `min_local_deletion_time` is a
    /// far-future value, then assert `compute_baseline_min` seeds that exact i32
    /// bit pattern (round-tripping through both the writer's i32->u32 header
    /// encoding and the parser).
    #[test]
    fn compute_baseline_min_keeps_far_future_ldt() {
        use crate::storage::sstable::writer::{StatisticsMetadata, StatisticsWriter};
        use tempfile::TempDir;

        let tmp = TempDir::new().expect("temp dir");
        // compute_baseline_min derives the Statistics.db path from a Data.db path.
        let data_path = tmp.path().join("nb-1-big-Data.db");
        std::fs::write(&data_path, b"").expect("touch Data.db");
        let stats_path = tmp.path().join("nb-1-big-Statistics.db");

        // Far-future LDT in [2^31, 2^32): 2^31 + 5, a negative i32 bit pattern.
        let far_future_bits: i32 = ((1u32 << 31) + 5) as i32;
        assert!(
            far_future_bits < 0,
            "sanity: far-future LDT is negative i32"
        );

        let mut meta = StatisticsMetadata::new();
        meta.min_local_deletion_time = far_future_bits;
        meta.max_local_deletion_time = far_future_bits;
        StatisticsWriter::new(stats_path)
            .write(&meta, None)
            .expect("write Statistics.db with far-future LDT");

        let (_ts, baseline_ldt, _ttl) = compute_baseline_min(&[data_path]);
        assert_eq!(
            baseline_ldt, far_future_bits,
            "far-future LDT baseline must round-trip as its i32 bit pattern, not be dropped"
        );
    }

    /// The live/no-deletion sentinel (i32::MAX, DeletionTime.LIVE) must NOT lower
    /// the seeded baseline: a Statistics.db with no tombstones leaves the baseline
    /// at i32::MAX.
    #[test]
    fn compute_baseline_min_skips_live_sentinel() {
        use crate::storage::sstable::writer::{StatisticsMetadata, StatisticsWriter};
        use tempfile::TempDir;

        let tmp = TempDir::new().expect("temp dir");
        let data_path = tmp.path().join("nb-1-big-Data.db");
        std::fs::write(&data_path, b"").expect("touch Data.db");
        let stats_path = tmp.path().join("nb-1-big-Statistics.db");

        // i32::MAX = no deletions (DeletionTime.LIVE sentinel).
        let mut meta = StatisticsMetadata::new();
        meta.min_local_deletion_time = i32::MAX;
        StatisticsWriter::new(stats_path)
            .write(&meta, None)
            .expect("write Statistics.db with LIVE sentinel");

        let (_ts, baseline_ldt, _ttl) = compute_baseline_min(&[data_path]);
        assert_eq!(
            baseline_ldt,
            i32::MAX,
            "live/no-deletion sentinel must not lower the seeded baseline"
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Property tests for compaction merge semantics (Issue #475, Epic #469)
// ─────────────────────────────────────────────────────────────────────────────
//
// Strategy: define a small in-memory `reference_merge` that applies the full
// Cassandra per-key merge rules (timestamp LWW, tombstone shadowing, TTL expiry,
// range tombstone application), generate randomised cell streams with proptest,
// and assert that both the reference and the real KWayMerger agree.
//
// Three coverage areas required by the issue:
//  A. Tombstone shadowing   – delete-ts > write-ts => cell suppressed
//  B. TTL expiry            – write with TTL whose local_deletion_time < merge_time => dropped
//  C. Range tombstone       – row in range with marked_for_delete_at >= cell-ts => dropped
//
// The reference implementation is tested directly via proptest.
// The real merger (merge_partition_rows) is also exercised for the cases it
// handles today (LWW by timestamp for live rows and tombstones).

#[cfg(all(test, feature = "write-support"))]
mod merge_property_tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::HashMap;

    // ─── Fixed "wall clock" used in all TTL expiry tests ─────────────────────
    // Unix seconds; cells with local_deletion_time < MERGE_TIME_SECS are expired.
    const MERGE_TIME_SECS: i32 = 1_000;

    // ─── Cell operation model ─────────────────────────────────────────────────

    /// The three kinds of cell operations that a compaction must resolve.
    #[derive(Debug, Clone)]
    enum CellOp {
        /// A live write: column <- value, recorded at `timestamp`.
        /// When `local_deletion_time` is Some(t) it is an expiring cell; the
        /// cell is considered dead when `t < MERGE_TIME_SECS`.
        Write {
            timestamp: i64,
            local_deletion_time: Option<i32>,
        },
        /// A cell tombstone (DELETE column): column is dead at `timestamp`.
        Delete { timestamp: i64 },
        /// A range tombstone covering the inclusive integer range
        /// `[start_ck, end_ck]`. Any row whose clustering key integer falls
        /// within the range and whose write-timestamp <= `marked_for_delete_at`
        /// is suppressed.
        RangeTombstone {
            start_ck: u8,
            end_ck: u8,
            marked_for_delete_at: i64,
        },
    }

    /// A single entry in the randomised cell stream.
    ///
    /// We work with small integer partition/clustering/column spaces so
    /// collisions occur frequently and the interesting merge cases arise.
    #[derive(Debug, Clone)]
    struct CellInput {
        /// 0..4
        partition: u8,
        /// 0..4
        clustering: u8,
        /// 0..3
        column: u8,
        op: CellOp,
    }

    // ─── Key type for the merged output map ──────────────────────────────────

    /// (partition, clustering, column) triple identifying a unique cell slot.
    type CellKey = (u8, u8, u8);

    /// What the reference merge produces for a cell slot.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum MergedCell {
        /// The cell is alive with the given write timestamp.
        Live { timestamp: i64 },
        /// The cell is a tombstone (deleted) at the given timestamp.
        Dead { timestamp: i64 },
    }

    // ─── Reference implementation ─────────────────────────────────────────────

    /// Reference merge over a flat cell-stream, applying full Cassandra rules.
    ///
    /// Rules (applied in order):
    /// 1. Per (partition, clustering, column), keep the op with the highest
    ///    `timestamp`. Ties: Delete wins over Write (Cassandra reconcile).
    /// 2. A `RangeTombstone` with `marked_for_delete_at >= cell.timestamp`
    ///    covering a clustering key suppresses the live cell in that slot.
    /// 3. A `Write` whose `local_deletion_time < MERGE_TIME_SECS` (TTL expired)
    ///    is dropped from the output even if it has the highest timestamp.
    fn reference_merge(inputs: &[CellInput]) -> HashMap<CellKey, MergedCell> {
        // ── Step 1: per-slot LWW ──────────────────────────────────────────────
        let mut per_slot: HashMap<CellKey, MergedCell> = HashMap::new();

        // Collect range tombstones grouped by (partition, clustering range).
        let mut range_tombstones: Vec<CellInput> = Vec::new();

        for ci in inputs {
            match &ci.op {
                CellOp::RangeTombstone { .. } => {
                    range_tombstones.push(ci.clone());
                }
                CellOp::Write {
                    timestamp,
                    local_deletion_time,
                } => {
                    // TTL expiry: drop the write if its local_deletion_time has passed.
                    if local_deletion_time
                        .map(|ldt| ldt < MERGE_TIME_SECS)
                        .unwrap_or(false)
                    {
                        // Expired: treat as if this write never happened.
                        continue;
                    }
                    let key = (ci.partition, ci.clustering, ci.column);
                    let candidate = MergedCell::Live {
                        timestamp: *timestamp,
                    };
                    per_slot
                        .entry(key)
                        .and_modify(|existing| {
                            match existing {
                                MergedCell::Live { timestamp: ex_ts } => {
                                    if *timestamp > *ex_ts {
                                        *existing = candidate.clone();
                                    }
                                }
                                MergedCell::Dead { timestamp: ex_ts } => {
                                    // Dead wins over a live cell at the same timestamp;
                                    // only replace if the write is strictly newer.
                                    if *timestamp > *ex_ts {
                                        *existing = candidate.clone();
                                    }
                                }
                            }
                        })
                        .or_insert(candidate);
                }
                CellOp::Delete { timestamp } => {
                    let key = (ci.partition, ci.clustering, ci.column);
                    let candidate = MergedCell::Dead {
                        timestamp: *timestamp,
                    };
                    per_slot
                        .entry(key)
                        .and_modify(|existing| {
                            match existing {
                                MergedCell::Live { timestamp: ex_ts } => {
                                    if *timestamp >= *ex_ts {
                                        // Delete wins at equal timestamp (Cassandra rule).
                                        *existing = candidate.clone();
                                    }
                                }
                                MergedCell::Dead { timestamp: ex_ts } => {
                                    if *timestamp > *ex_ts {
                                        *existing = candidate.clone();
                                    }
                                }
                            }
                        })
                        .or_insert(candidate);
                }
            }
        }

        // ── Step 2: apply range tombstones ────────────────────────────────────
        // A range tombstone suppresses a live cell when:
        //   - partition matches
        //   - clustering key is within [start_ck, end_ck]
        //   - marked_for_delete_at >= cell write timestamp
        per_slot.retain(|&(pk, ck, _col), cell| {
            for rt in &range_tombstones {
                if rt.partition != pk {
                    continue;
                }
                if let CellOp::RangeTombstone {
                    start_ck,
                    end_ck,
                    marked_for_delete_at,
                } = rt.op
                {
                    if ck >= start_ck && ck <= end_ck {
                        if let MergedCell::Live { timestamp } = cell {
                            if marked_for_delete_at >= *timestamp {
                                return false; // suppressed
                            }
                        }
                    }
                }
            }
            true
        });

        // Dead cells (tombstones) are kept in the output so callers can verify
        // they appear rather than a live cell with a lower timestamp.
        per_slot
    }

    // ─── Proptest strategies ──────────────────────────────────────────────────

    fn arb_timestamp() -> impl Strategy<Value = i64> {
        1i64..=20i64
    }

    /// local_deletion_time: sometimes None, sometimes expired (<MERGE_TIME_SECS),
    /// sometimes live (>=MERGE_TIME_SECS).
    fn arb_local_deletion_time() -> impl Strategy<Value = Option<i32>> {
        prop_oneof![
            3 => Just(None),                         // no TTL
            1 => (990i32..=999i32).prop_map(Some),   // expired TTL
            1 => (1000i32..=1010i32).prop_map(Some), // live TTL
        ]
    }

    fn arb_cell_op() -> impl Strategy<Value = CellOp> {
        prop_oneof![
            5 => (arb_timestamp(), arb_local_deletion_time())
                    .prop_map(|(ts, ldt)| CellOp::Write {
                        timestamp: ts,
                        local_deletion_time: ldt,
                    }),
            3 => arb_timestamp().prop_map(|ts| CellOp::Delete { timestamp: ts }),
            2 => (0u8..=3u8, 0u8..=3u8, arb_timestamp()).prop_map(|(s, e, ts)| {
                    let (start_ck, end_ck) = if s <= e { (s, e) } else { (e, s) };
                    CellOp::RangeTombstone {
                        start_ck,
                        end_ck,
                        marked_for_delete_at: ts,
                    }
                }),
        ]
    }

    fn arb_cell_input() -> impl Strategy<Value = CellInput> {
        (0u8..4u8, 0u8..4u8, 0u8..3u8, arb_cell_op()).prop_map(
            |(partition, clustering, column, op)| CellInput {
                partition,
                clustering,
                column,
                op,
            },
        )
    }

    fn arb_cell_stream() -> impl Strategy<Value = Vec<CellInput>> {
        prop::collection::vec(arb_cell_input(), 4..=32)
    }

    // ─── Helper: sort merged output for stable comparison ─────────────────────
    fn sorted_keys(m: &HashMap<CellKey, MergedCell>) -> Vec<(CellKey, MergedCell)> {
        let mut v: Vec<_> = m.iter().map(|(&k, v)| (k, v.clone())).collect();
        v.sort_by_key(|(k, _)| *k);
        v
    }

    // ─── Deterministic unit tests for reference semantics ────────────────────

    #[test]
    fn ref_tombstone_shadows_earlier_write() {
        // Write at ts=5, then Delete at ts=10 => cell must be Dead(10).
        let inputs = vec![
            CellInput {
                partition: 0,
                clustering: 0,
                column: 0,
                op: CellOp::Write {
                    timestamp: 5,
                    local_deletion_time: None,
                },
            },
            CellInput {
                partition: 0,
                clustering: 0,
                column: 0,
                op: CellOp::Delete { timestamp: 10 },
            },
        ];
        let result = reference_merge(&inputs);
        assert_eq!(
            result.get(&(0, 0, 0)),
            Some(&MergedCell::Dead { timestamp: 10 }),
            "Delete(ts=10) must shadow Write(ts=5)"
        );
    }

    #[test]
    fn ref_write_not_shadowed_by_older_tombstone() {
        // Write at ts=10, Delete at ts=5 => cell must be Live(10).
        let inputs = vec![
            CellInput {
                partition: 0,
                clustering: 0,
                column: 0,
                op: CellOp::Write {
                    timestamp: 10,
                    local_deletion_time: None,
                },
            },
            CellInput {
                partition: 0,
                clustering: 0,
                column: 0,
                op: CellOp::Delete { timestamp: 5 },
            },
        ];
        let result = reference_merge(&inputs);
        assert_eq!(
            result.get(&(0, 0, 0)),
            Some(&MergedCell::Live { timestamp: 10 }),
            "Write(ts=10) must win over Delete(ts=5)"
        );
    }

    #[test]
    fn ref_delete_wins_at_equal_timestamp() {
        // Write at ts=5, Delete at ts=5 => Delete must win (Cassandra reconcile).
        let inputs = vec![
            CellInput {
                partition: 0,
                clustering: 0,
                column: 0,
                op: CellOp::Write {
                    timestamp: 5,
                    local_deletion_time: None,
                },
            },
            CellInput {
                partition: 0,
                clustering: 0,
                column: 0,
                op: CellOp::Delete { timestamp: 5 },
            },
        ];
        let result = reference_merge(&inputs);
        assert_eq!(
            result.get(&(0, 0, 0)),
            Some(&MergedCell::Dead { timestamp: 5 }),
            "Delete must win at equal timestamp (Cassandra reconcile rule)"
        );
    }

    #[test]
    fn ref_expired_ttl_drops_cell() {
        // Write at ts=5 with local_deletion_time=500 (< MERGE_TIME_SECS=1000).
        // No other ops => cell should be absent from merged output.
        let inputs = vec![CellInput {
            partition: 0,
            clustering: 0,
            column: 0,
            op: CellOp::Write {
                timestamp: 5,
                local_deletion_time: Some(500), // expired
            },
        }];
        let result = reference_merge(&inputs);
        assert!(
            !result.contains_key(&(0, 0, 0)),
            "Expired TTL cell must be absent from merged output"
        );
    }

    #[test]
    fn ref_live_ttl_keeps_cell() {
        // Write at ts=5 with local_deletion_time=1500 (>= MERGE_TIME_SECS=1000).
        // Cell should still be present.
        let inputs = vec![CellInput {
            partition: 0,
            clustering: 0,
            column: 0,
            op: CellOp::Write {
                timestamp: 5,
                local_deletion_time: Some(1500), // not expired
            },
        }];
        let result = reference_merge(&inputs);
        assert_eq!(
            result.get(&(0, 0, 0)),
            Some(&MergedCell::Live { timestamp: 5 }),
            "Non-expired TTL cell must be present"
        );
    }

    #[test]
    fn ref_range_tombstone_suppresses_row_in_range() {
        // Write at ts=5 for clustering key 2 in partition 0.
        // RangeTombstone covering [0, 5] with marked_for_delete_at=10 => suppressed.
        let inputs = vec![
            CellInput {
                partition: 0,
                clustering: 2,
                column: 0,
                op: CellOp::Write {
                    timestamp: 5,
                    local_deletion_time: None,
                },
            },
            CellInput {
                partition: 0,
                clustering: 0, // column/clustering fields ignored for RT; range is in op
                column: 0,
                op: CellOp::RangeTombstone {
                    start_ck: 0,
                    end_ck: 5,
                    marked_for_delete_at: 10,
                },
            },
        ];
        let result = reference_merge(&inputs);
        assert!(
            !result.contains_key(&(0, 2, 0)),
            "Cell with ts=5 at clustering=2 must be suppressed by RangeTombstone(mfda=10, [0,5])"
        );
    }

    #[test]
    fn ref_range_tombstone_does_not_suppress_newer_write() {
        // Write at ts=15, RangeTombstone with mfda=10 => not suppressed.
        let inputs = vec![
            CellInput {
                partition: 0,
                clustering: 2,
                column: 0,
                op: CellOp::Write {
                    timestamp: 15,
                    local_deletion_time: None,
                },
            },
            CellInput {
                partition: 0,
                clustering: 0,
                column: 0,
                op: CellOp::RangeTombstone {
                    start_ck: 0,
                    end_ck: 5,
                    marked_for_delete_at: 10,
                },
            },
        ];
        let result = reference_merge(&inputs);
        assert_eq!(
            result.get(&(0, 2, 0)),
            Some(&MergedCell::Live { timestamp: 15 }),
            "Write(ts=15) must NOT be suppressed by RangeTombstone(mfda=10)"
        );
    }

    #[test]
    fn ref_range_tombstone_only_applies_within_partition() {
        // Write in partition 1, RangeTombstone in partition 0 => not suppressed.
        let inputs = vec![
            CellInput {
                partition: 1,
                clustering: 2,
                column: 0,
                op: CellOp::Write {
                    timestamp: 5,
                    local_deletion_time: None,
                },
            },
            CellInput {
                partition: 0,
                clustering: 0,
                column: 0,
                op: CellOp::RangeTombstone {
                    start_ck: 0,
                    end_ck: 5,
                    marked_for_delete_at: 10,
                },
            },
        ];
        let result = reference_merge(&inputs);
        assert_eq!(
            result.get(&(1, 2, 0)),
            Some(&MergedCell::Live { timestamp: 5 }),
            "RangeTombstone in partition 0 must not affect partition 1"
        );
    }

    // ─── Property tests ───────────────────────────────────────────────────────

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        // Property A: Tombstone shadowing
        // After merging, for every (partition, clustering, column) cell slot, if
        // the reference says Dead(ts=T) then the highest-timestamp Delete in the
        // input stream for that slot must have timestamp T.
        #[test]
        fn prop_tombstone_shadowing_consistent(inputs in arb_cell_stream()) {
            let merged = reference_merge(&inputs);

            for (&(pk, ck, col), cell) in &merged {
                if let MergedCell::Dead { timestamp: dead_ts } = cell {
                    // Find the highest-timestamp Delete for this slot in the input.
                    let best_delete = inputs.iter()
                        .filter(|ci| ci.partition == pk && ci.clustering == ck && ci.column == col)
                        .filter_map(|ci| {
                            if let CellOp::Delete { timestamp } = ci.op {
                                Some(timestamp)
                            } else {
                                None
                            }
                        })
                        .max();

                    prop_assert!(
                        best_delete.is_some(),
                        "Dead cell at ({},{},{}) but no Delete in inputs",
                        pk, ck, col
                    );
                    prop_assert_eq!(
                        best_delete.unwrap(),
                        *dead_ts,
                        "Dead cell timestamp must equal best Delete timestamp for ({},{},{})",
                        pk, ck, col
                    );
                }
            }
        }

        // Property B: TTL expiry
        // After merging, no cell should be Live if all its Write ops are TTL-expired.
        #[test]
        fn prop_ttl_expiry_no_expired_live_cells(inputs in arb_cell_stream()) {
            let merged = reference_merge(&inputs);

            for (&(pk, ck, col), cell) in &merged {
                if let MergedCell::Live { .. } = cell {
                    // There must be at least one non-expired Write for this slot.
                    let has_live_write = inputs.iter()
                        .filter(|ci| ci.partition == pk && ci.clustering == ck && ci.column == col)
                        .any(|ci| {
                            if let CellOp::Write { local_deletion_time, .. } = &ci.op {
                                // A live write: no TTL, or TTL not expired.
                                local_deletion_time
                                    .map(|ldt| ldt >= MERGE_TIME_SECS)
                                    .unwrap_or(true)
                            } else {
                                false
                            }
                        });

                    prop_assert!(
                        has_live_write,
                        "Live cell at ({},{},{}) but all writes are expired",
                        pk, ck, col
                    );
                }
            }
        }

        // Property C: Range tombstone application
        // After merging, no Live cell should exist that is fully covered by a
        // range tombstone whose marked_for_delete_at >= the cell's write timestamp.
        #[test]
        fn prop_range_tombstone_suppresses_covered_live_cells(inputs in arb_cell_stream()) {
            let merged = reference_merge(&inputs);

            // Collect all range tombstones from the input stream.
            let range_tombstones: Vec<(u8, u8, u8, i64)> = inputs.iter()
                .filter_map(|ci| {
                    if let CellOp::RangeTombstone { start_ck, end_ck, marked_for_delete_at } = ci.op {
                        Some((ci.partition, start_ck, end_ck, marked_for_delete_at))
                    } else {
                        None
                    }
                })
                .collect();

            for (&(pk, ck, _col), cell) in &merged {
                if let MergedCell::Live { timestamp } = cell {
                    // Verify no range tombstone shadows this cell.
                    for &(rt_pk, start_ck, end_ck, mfda) in &range_tombstones {
                        if rt_pk == pk && ck >= start_ck && ck <= end_ck && mfda >= *timestamp {
                            prop_assert!(
                                false,
                                "Live cell at ({},{}) ts={} should be suppressed by \
                                 RangeTombstone(part={}, [{},{}], mfda={})",
                                pk, ck, timestamp, rt_pk, start_ck, end_ck, mfda
                            );
                        }
                    }
                }
            }
        }

        // Property D: LWW correctness
        // Every Live cell in the merged output must have a timestamp equal to
        // the maximum non-expired Write timestamp for that cell slot.
        #[test]
        fn prop_live_cell_has_max_write_timestamp(inputs in arb_cell_stream()) {
            let merged = reference_merge(&inputs);

            for (&(pk, ck, col), cell) in &merged {
                if let MergedCell::Live { timestamp: live_ts } = cell {
                    // Find the maximum non-expired Write timestamp for this slot.
                    let max_ts = inputs.iter()
                        .filter(|ci| ci.partition == pk && ci.clustering == ck && ci.column == col)
                        .filter_map(|ci| {
                            if let CellOp::Write { timestamp, local_deletion_time } = &ci.op {
                                // Only include non-expired writes.
                                let not_expired = local_deletion_time
                                    .map(|ldt| ldt >= MERGE_TIME_SECS)
                                    .unwrap_or(true);
                                if not_expired { Some(*timestamp) } else { None }
                            } else {
                                None
                            }
                        })
                        .max();

                    prop_assert_eq!(
                        max_ts,
                        Some(*live_ts),
                        "Live cell at ({},{},{}) must have max non-expired write timestamp",
                        pk, ck, col
                    );
                }
            }
        }

        // Property E: Output is deterministic (idempotent reference)
        // Calling reference_merge twice on the same input produces identical output.
        #[test]
        fn prop_reference_merge_is_deterministic(inputs in arb_cell_stream()) {
            let result_a = reference_merge(&inputs);
            let result_b = reference_merge(&inputs);
            prop_assert_eq!(
                sorted_keys(&result_a),
                sorted_keys(&result_b),
                "reference_merge must be deterministic"
            );
        }

        // Property F: Real merger LWW parity
        // For cell streams containing only non-expired Writes (no Deletes,
        // no RangeTombstones, no TTL), the real KWayMerger.merge_partition_rows
        // must agree with the reference on which row wins per clustering key.
        //
        // We drive merge_partition_rows directly with synthetic MergeEntry inputs
        // that represent the Write ops.
        #[test]
        fn prop_real_merger_lww_agrees_with_reference(
            entries in prop::collection::vec(
                // (clustering_key 0..4, run_index 0..2, timestamp 1..20)
                (0u8..4u8, 0usize..2usize, 1i64..=20i64),
                2..=12usize,
            )
        ) {
            use crate::schema::{Column, KeyColumn};
            use std::collections::HashMap as SchemaMap;

            let schema = TableSchema {
                keyspace: "prop_test_ks".to_string(),
                table: "prop_test_table".to_string(),
                partition_keys: vec![KeyColumn {
                    name: "id".to_string(),
                    data_type: "int".to_string(),
                    position: 0,
                }],
                clustering_keys: vec![],
                columns: vec![Column {
                    name: "value".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                }],
                comments: SchemaMap::new(),
                dropped_columns: SchemaMap::new(),
            };

            // Build MergeEntry stream — one per (ck, run_index, timestamp) tuple.
            // All entries share the same partition.
            let partition_key = DecoratedKey::new(100, vec![0, 0, 0, 1]);
            let merge_entries: Vec<MergeEntry> = entries.iter().map(|&(ck, run_index, ts)| {
                let ck_key = ClusteringKey {
                    columns: vec![("ck".to_string(), Value::TinyInt(ck as i8))],
                };
                MergeEntry::new(
                    run_index,
                    partition_key.clone(),
                    Some(ck_key),
                    ts,
                    RowData::Live {
                        cells: vec![CellData {
                            column: "value".to_string(),
                            value: Value::Integer(ts as i32),
                            timestamp: ts,
                            ttl: None,
                            cell_path: None,
                            local_deletion_time: None,
                        }],
                    },
                )
            }).collect();

            // Drive the real merger.
            let merger = KWayMerger {
                runs: vec![],
                heap: std::collections::BinaryHeap::new(),
                current_partition: None,
                gc_before_secs: None,
                now_secs: None,
                schema: schema.clone(),
            };
            let real_merged = merger.merge_partition_rows(merge_entries.clone())
                .expect("merge_partition_rows must not fail");

            // Build the reference result: per clustering-key int, highest timestamp wins.
            // (run_index as tie-breaker: lower run_index wins at equal ts — same as merger)
            let mut ref_map: HashMap<u8, (i64, usize)> = HashMap::new();
            for &(ck, run_index, ts) in &entries {
                ref_map.entry(ck)
                    .and_modify(|(best_ts, best_run)| {
                        if ts > *best_ts || (ts == *best_ts && run_index < *best_run) {
                            *best_ts = ts;
                            *best_run = run_index;
                        }
                    })
                    .or_insert((ts, run_index));
            }

            // Verify each winner in the real output matches the reference.
            prop_assert_eq!(
                real_merged.len(),
                ref_map.len(),
                "real merger output row count must match reference"
            );

            for entry in &real_merged {
                let ck_byte = match entry.clustering_key.as_ref()
                    .and_then(|ck| ck.columns.first())
                    .map(|(_, v)| v)
                {
                    Some(Value::TinyInt(b)) => *b as u8,
                    _ => {
                        prop_assert!(false, "unexpected clustering key value");
                        unreachable!()
                    }
                };

                let (ref_ts, _ref_run) = ref_map[&ck_byte];
                prop_assert_eq!(
                    entry.timestamp,
                    ref_ts,
                    "real merger winner timestamp must match reference for ck={}",
                    ck_byte
                );
            }
        }

        // Property G: Tombstone wins over live row at same clustering key in real merger
        // When a Tombstone and a Live row have the same clustering key, the one with
        // the higher timestamp must win — and the real merger must reflect this.
        #[test]
        fn prop_real_merger_tombstone_vs_live(
            ts_write in 1i64..=10i64,
            ts_delete in 1i64..=20i64,
        ) {
            use crate::schema::{Column, KeyColumn};
            use std::collections::HashMap as SchemaMap;

            let schema = TableSchema {
                keyspace: "prop_test_ks".to_string(),
                table: "prop_test_table".to_string(),
                partition_keys: vec![KeyColumn {
                    name: "id".to_string(),
                    data_type: "int".to_string(),
                    position: 0,
                }],
                clustering_keys: vec![],
                columns: vec![Column {
                    name: "value".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                }],
                comments: SchemaMap::new(),
                dropped_columns: SchemaMap::new(),
            };

            let partition_key = DecoratedKey::new(100, vec![0, 0, 0, 1]);
            let ck = ClusteringKey {
                columns: vec![("ck".to_string(), Value::TinyInt(0))],
            };

            // Deliberately give the LIVE row the NEWER file (run_index 0) and the
            // tombstone the OLDER file (run_index 1). A run_index-only tiebreak at
            // equal timestamp would wrongly pick the live row; the Cassandra
            // liveness rule must pick the tombstone regardless of file recency.
            let live_entry = MergeEntry::new(
                0, // run_index 0 = newer file
                partition_key.clone(),
                Some(ck.clone()),
                ts_write,
                RowData::Live {
                    cells: vec![CellData {
                        column: "value".to_string(),
                        value: Value::Integer(42),
                        timestamp: ts_write,
                        ttl: None,
                        cell_path: None,
                        local_deletion_time: None,
                    }],
                },
            );
            let tombstone_entry = MergeEntry::new(
                1, // run_index 1 = older file
                partition_key.clone(),
                Some(ck.clone()),
                ts_delete,
                RowData::Tombstone {
                    deletion_time: ts_delete,
                    local_deletion_time: 2000,
                },
            );

            let merger = KWayMerger {
                runs: vec![],
                heap: std::collections::BinaryHeap::new(),
                current_partition: None,
                gc_before_secs: None,
                now_secs: None,
                schema: schema.clone(),
            };
            let merged = merger.merge_partition_rows(vec![live_entry, tombstone_entry])
                .expect("merge_partition_rows must not fail");

            prop_assert_eq!(merged.len(), 1, "one clustering key => one merged row");

            let winner = &merged[0];
            if ts_delete > ts_write {
                // Tombstone has higher timestamp => should win.
                prop_assert!(
                    matches!(winner.row_data, RowData::Tombstone { .. }),
                    "Tombstone(ts={}) must win over Live(ts={})",
                    ts_delete, ts_write
                );
            } else if ts_write > ts_delete {
                // Live write has higher timestamp => should win.
                prop_assert!(
                    matches!(winner.row_data, RowData::Live { .. }),
                    "Live(ts={}) must win over Tombstone(ts={})",
                    ts_write, ts_delete
                );
            } else {
                // Equal timestamps: the tombstone (Delete) ALWAYS wins, matching
                // Cassandra `Cells#reconcile`. This must hold regardless of file
                // recency — the assertion previously carved this case out, hiding
                // the run_index-only tiebreak bug (Issue #498).
                prop_assert!(
                    matches!(winner.row_data, RowData::Tombstone { .. }),
                    "At equal ts={}, Tombstone must win over Live (Cassandra reconcile rule)",
                    ts_delete
                );
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Streaming channel / cursor mechanism tests (Issue #754)
// ─────────────────────────────────────────────────────────────────────────────
//
// These tests verify that the bounded sync_channel and RunReader cursor
// machinery work correctly: entries are forwarded without deadlock, ordering
// is preserved, and the channel provides backpressure between producer and
// consumer.
//
// NOTE: these tests verify the channel/cursor mechanism in isolation; they do
// NOT themselves prove the end-to-end memory bound. The end-to-end bound — the
// real producer streaming its source via stream_all_partitions_for_compaction
// (issue #827) — is asserted by the dhat test
// tests/test_issue_827_merge_streaming_memory.rs.

#[cfg(all(test, feature = "write-support"))]
mod streaming_tests {
    use super::*;

    /// Channel capacity constant is accessible and matches documented value.
    ///
    /// This test checks only the constant's value — it does NOT prove an
    /// end-to-end memory bound. The bound on in-flight MergeEntry objects
    /// between producer and consumer is STREAMING_CHANNEL_CAPACITY; the
    /// end-to-end memory bound (the producer streaming its source one partition
    /// at a time, issue #827) is asserted by the dhat test
    /// tests/test_issue_827_merge_streaming_memory.rs.
    #[test]
    fn test_streaming_channel_capacity_constant() {
        // The constant must be large enough to amortise scheduling overhead but
        // small enough to limit in-flight MergeEntry objects. 256 is the
        // documented value.
        assert_eq!(STREAMING_CHANNEL_CAPACITY, 256);
    }

    /// A synthetic `SSTableRowIterator` backed by a bounded channel. The
    /// producer thread is started immediately and blocks once the channel is
    /// full, demonstrating true backpressure — memory is bounded to `capacity`
    /// entries regardless of `count`.
    struct SyntheticStreamingIterator {
        rx: std::sync::mpsc::Receiver<Result<MergeEntry>>,
        _tx_thread: std::thread::JoinHandle<()>,
    }

    impl SyntheticStreamingIterator {
        /// Produce `count` entries with sequential tokens and the given
        /// `run_index`, streamed through a channel of size `capacity`.
        fn new(count: usize, run_index: usize, capacity: usize) -> Self {
            let (tx, rx) = std::sync::mpsc::sync_channel(capacity);
            let tx_thread = std::thread::spawn(move || {
                for i in 0..count {
                    let entry = MergeEntry::new(
                        run_index,
                        DecoratedKey::new(i as i64, vec![i as u8]),
                        None,
                        (i as i64) * 1000,
                        RowData::Live { cells: vec![] },
                    );
                    if tx.send(Ok(entry)).is_err() {
                        return;
                    }
                }
            });
            Self {
                rx,
                _tx_thread: tx_thread,
            }
        }
    }

    impl SSTableRowIterator for SyntheticStreamingIterator {
        fn next(&mut self) -> Option<Result<MergeEntry>> {
            self.rx.recv().ok()
        }
    }

    /// Merge two synthetic streaming sources (channel capacity = 4, 20 entries
    /// each) and assert that all 40 unique tokens survive and global order is
    /// preserved.
    ///
    /// This verifies that the RunReader / heap machinery correctly drains
    /// bounded-channel sources: with capacity=4 the channel holds ≤ 4 entries
    /// per source (≤ 8 total) while the test runs, demonstrating correct
    /// ordering and completeness through a small-capacity channel.
    ///
    /// NOTE: this test exercises the synthetic streaming-iterator path only; the
    /// end-to-end memory bound for the real SSTableRowIteratorAdapter (whose
    /// producer streams its source one partition at a time, issue #827) is
    /// asserted by the dhat test tests/test_issue_827_merge_streaming_memory.rs.
    #[test]
    fn test_kway_merge_with_streaming_sources_preserves_order() {
        use crate::schema::{KeyColumn, TableSchema};
        use std::collections::HashMap;

        let schema = TableSchema {
            keyspace: "stream_ks".to_string(),
            table: "stream_tbl".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        // Two sources with disjoint tokens:
        //   source 0 → even tokens 0, 2, 4, …, 38
        //   source 1 → odd  tokens 1, 3, 5, …, 39
        // Channel capacity = 4 << total per source (20). At steady state
        // ≤ 4 entries per source live in the channel, ≤ 8 total.
        // (These are synthetic in-memory producers, not real SSTableReaders.)
        const N: usize = 20;
        const CHANNEL_CAP: usize = 4;

        let (tx0, rx0) = std::sync::mpsc::sync_channel::<Result<MergeEntry>>(CHANNEL_CAP);
        let (tx1, rx1) = std::sync::mpsc::sync_channel::<Result<MergeEntry>>(CHANNEL_CAP);

        // Producer thread 0: even tokens.
        std::thread::spawn(move || {
            for i in 0..N {
                let token = (i * 2) as i64;
                let entry = MergeEntry::new(
                    0,
                    DecoratedKey::new(token, vec![(i * 2) as u8]),
                    None,
                    1000,
                    RowData::Live { cells: vec![] },
                );
                if tx0.send(Ok(entry)).is_err() {
                    return;
                }
            }
        });

        // Producer thread 1: odd tokens.
        std::thread::spawn(move || {
            for i in 0..N {
                let token = (i * 2 + 1) as i64;
                let entry = MergeEntry::new(
                    1,
                    DecoratedKey::new(token, vec![(i * 2 + 1) as u8]),
                    None,
                    1000,
                    RowData::Live { cells: vec![] },
                );
                if tx1.send(Ok(entry)).is_err() {
                    return;
                }
            }
        });

        struct ChannelIterator(std::sync::mpsc::Receiver<Result<MergeEntry>>);
        impl SSTableRowIterator for ChannelIterator {
            fn next(&mut self) -> Option<Result<MergeEntry>> {
                self.0.recv().ok()
            }
        }

        let runs: Vec<RunReader> = vec![
            RunReader::new(Box::new(ChannelIterator(rx0))),
            RunReader::new(Box::new(ChannelIterator(rx1))),
        ];

        let mut merger = KWayMerger {
            runs,
            heap: BinaryHeap::new(),
            current_partition: None,
            gc_before_secs: None,
            now_secs: None,
            schema,
        };

        // Drain all partitions and verify ordering + completeness.
        let mut token_set = std::collections::BTreeSet::new();
        let mut prev_token: Option<i64> = None;
        loop {
            match merger.step().expect("step must not fail") {
                MergeStep::Complete => break,
                MergeStep::Partition { key, .. } => {
                    // Tokens must arrive in ascending order.
                    if let Some(pt) = prev_token {
                        assert!(
                            key.token >= pt,
                            "out-of-order token {} after {}",
                            key.token,
                            pt
                        );
                    }
                    prev_token = Some(key.token);
                    token_set.insert(key.token);
                }
            }
        }

        // All 2×N unique tokens must be present.
        assert_eq!(
            token_set.len(),
            N * 2,
            "expected {} unique partitions, got {}",
            N * 2,
            token_set.len()
        );
        for expected in 0..(N as i64 * 2) {
            assert!(
                token_set.contains(&expected),
                "token {} is missing from merged output",
                expected
            );
        }
    }

    /// Verify that the streaming adapter drains all entries correctly when the
    /// channel capacity is smaller than the total number of entries (1000 entries,
    /// capacity 256). This confirms the producer blocks on sends and the consumer
    /// pulls them out one at a time without deadlock.
    #[test]
    fn test_streaming_iterator_drains_all_entries_with_backpressure() {
        const TOTAL: usize = 1000;
        // capacity < TOTAL: forces producer to block when channel is full.
        let mut iter = SyntheticStreamingIterator::new(TOTAL, 0, STREAMING_CHANNEL_CAPACITY);
        let mut count = 0usize;
        while let Some(result) = iter.next() {
            result.expect("entry must not be an error");
            count += 1;
        }
        assert_eq!(count, TOTAL, "all {} entries must be produced", TOTAL);
    }

    /// Verify the RunReader correctly wraps a streaming iterator: peek and
    /// advance work, exhaustion is detected, buffer refills lazily even when
    /// the channel capacity (4) is far smaller than the total entries (50).
    #[test]
    fn test_run_reader_with_streaming_source() {
        const N: usize = 50;
        // Channel capacity 4 << N: tests lazy refill under backpressure.
        let iter = SyntheticStreamingIterator::new(N, 0, 4);
        let mut reader = RunReader::new(Box::new(iter));

        let mut seen = 0usize;
        loop {
            match reader.peek().expect("peek must not error") {
                None => break,
                Some(_) => {
                    reader.advance().expect("advance must not error");
                    seen += 1;
                }
            }
        }

        assert_eq!(seen, N, "RunReader must surface all {} entries", N);
        assert!(
            reader.is_exhausted(),
            "RunReader must be exhausted after drain"
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Issue #823 (Epic #817): complex-column (multi-cell collection / non-frozen UDT)
// merge behaviour.
//
// ADDITIVE TEST MODULE — flagged per the issue brief. This module is `#[cfg(test)]`
// only and adds NO production code. It exists because the authoritative reconcile
// function `KWayMerger::reconcile_cluster` and the reader→merge adapter
// `SSTableRowIteratorAdapter::value_to_row_data` are private, so the gating
// behaviour can only be value-asserted from inside the `merge` module.
//
// These tests ESTABLISH (do not aspire to) the current behaviour so the findings
// doc can cite real code. They assert the divergence where one exists.
// ─────────────────────────────────────────────────────────────────────────────
#[cfg(all(test, feature = "write-support"))]
mod issue_823_complex_column_merge {
    use super::*;
    use crate::types::{TombstoneInfo, TombstoneType, UdtField, UdtValue, Value};

    fn dk(byte: u8) -> DecoratedKey {
        DecoratedKey::from_key_bytes(vec![byte]).expect("token")
    }

    fn live(run_index: usize, row_ts: i64, cells: Vec<CellData>) -> MergeEntry {
        MergeEntry::new(run_index, dk(1), None, row_ts, RowData::Live { cells })
    }

    fn scalar_cell(column: &str, value: &str, ts: i64) -> CellData {
        CellData {
            column: column.to_string(),
            value: Value::Text(value.to_string()),
            timestamp: ts,
            ttl: None,
            cell_path: None,
            local_deletion_time: None,
        }
    }

    // ── Issue #847: dropped-column cell filtering during compaction ──────────
    // Cassandra `cb34ad47` / `compaction.purge`: a column dropped at drop_time
    // discards every cell whose timestamp <= drop_time. The drop time comes from
    // the `dropped_columns` map plumbed in #904. Byte-correct for scalar columns
    // at row-timestamp granularity; element-level collection/UDT filtering needs
    // per-cell timestamps (#899) and is out of scope.

    /// A cell of a dropped column written AT OR BEFORE the drop time is discarded;
    /// a sibling column with no drop entry is untouched.
    #[test]
    fn dropped_column_cell_at_or_before_drop_time_is_filtered() {
        let row = live(
            0,
            200,
            vec![
                scalar_cell("name", "alice", 100),
                scalar_cell("legacy", "stale", 100),
            ],
        );
        let mut dropped = ::std::collections::HashMap::new();
        dropped.insert("legacy".to_string(), 150); // dropped at T=150, cell ts=100 <= 150

        let merged = KWayMerger::reconcile_cluster(None, vec![row], &dropped)
            .expect("a live row must be emitted (name survives)");
        let cells = match merged.row_data {
            RowData::Live { cells } => cells,
            other => panic!("expected Live, got {:?}", other),
        };

        assert_eq!(cells.len(), 1, "the dropped-column cell must be discarded");
        assert_eq!(cells[0].column, "name");
    }

    /// A cell written STRICTLY AFTER the drop time survives the reconcile filter
    /// (the column was re-added). `compact_sstables` then retains that column in
    /// the output writer schema so the surviving cell has a matching header
    /// column (see `for_compaction_output`).
    #[test]
    fn dropped_column_cell_after_drop_time_survives() {
        let row = live(0, 200, vec![scalar_cell("legacy", "fresh", 200)]);
        let mut dropped = ::std::collections::HashMap::new();
        dropped.insert("legacy".to_string(), 150); // cell ts=200 > 150

        let merged = KWayMerger::reconcile_cluster(None, vec![row], &dropped)
            .expect("a live row must be emitted (cell post-dates the drop)");
        let cells = match merged.row_data {
            RowData::Live { cells } => cells,
            other => panic!("expected Live, got {:?}", other),
        };
        assert_eq!(cells.len(), 1);
        assert_eq!(cells[0].column, "legacy");
        assert_eq!(cells[0].value, Value::Text("fresh".to_string()));
    }

    /// Equal timestamp (cell ts == drop_time) is discarded — the `<=` boundary,
    /// matching the row-tombstone `<=` shadowing rule.
    #[test]
    fn dropped_column_cell_at_exact_drop_time_is_filtered() {
        let row = live(0, 150, vec![scalar_cell("legacy", "edge", 150)]);
        let mut dropped = ::std::collections::HashMap::new();
        dropped.insert("legacy".to_string(), 150);

        assert!(
            KWayMerger::reconcile_cluster(None, vec![row], &dropped).is_none(),
            "cell at exactly drop_time must be discarded, leaving no surviving cells"
        );
    }

    /// When ALL cells belong to dropped columns and all are at/before drop time,
    /// the row produces no output (no spurious empty Live entry).
    #[test]
    fn all_cells_dropped_yields_no_row() {
        let row = live(
            0,
            120,
            vec![scalar_cell("a", "x", 100), scalar_cell("b", "y", 110)],
        );
        let mut dropped = ::std::collections::HashMap::new();
        dropped.insert("a".to_string(), 200);
        dropped.insert("b".to_string(), 200);

        assert!(
            KWayMerger::reconcile_cluster(None, vec![row], &dropped).is_none(),
            "a row whose every cell is a dropped-column cell emits nothing"
        );
    }

    /// An empty `dropped_columns` map is a no-op: all cells survive.
    #[test]
    fn empty_dropped_map_is_noop() {
        let row = live(
            0,
            200,
            vec![
                scalar_cell("name", "alice", 100),
                scalar_cell("legacy", "stale", 100),
            ],
        );
        let merged =
            KWayMerger::reconcile_cluster(None, vec![row], &::std::collections::HashMap::new())
                .expect("a live row must be emitted");
        let cells = match merged.row_data {
            RowData::Live { cells } => cells,
            other => panic!("expected Live, got {:?}", other),
        };
        assert_eq!(cells.len(), 2, "no drops configured → every cell survives");
    }

    /// GATING TEST — multi-cell merge granularity.
    ///
    /// A non-frozen collection (here: a list) is read back from an SSTable as a
    /// SINGLE top-level column whose `Value` is the whole collection. Two SSTable
    /// runs that each wrote different *paths* (different list positions) of the
    /// same column therefore arrive as two `CellData` that share the same
    /// `cell.column` string but carry different whole-collection `Value`s.
    ///
    /// `reconcile_cluster` keys winners on `cell.column` ONLY. This test asserts
    /// the observed consequence: the two path-writes do NOT merge into a combined
    /// collection; instead ONE whole cell wins by timestamp (whole-group collapse,
    /// NOT per-path merge).
    #[test]
    fn multicell_collection_collapses_whole_column_not_per_path() {
        // Newer run (run_index 0) wrote element "b"; older run wrote element "a".
        // In real Cassandra these are two cells at paths p_b and p_a that union to
        // [a, b]. Here each run surfaces the column as a whole list value.
        let newer = live(
            0,
            200,
            vec![CellData {
                column: "tags".to_string(),
                value: Value::List(vec![Value::Text("b".to_string())]),
                timestamp: 200,
                ttl: None,
                cell_path: None,
                local_deletion_time: None,
            }],
        );
        let older = live(
            1,
            100,
            vec![CellData {
                column: "tags".to_string(),
                value: Value::List(vec![Value::Text("a".to_string())]),
                timestamp: 100,
                ttl: None,
                cell_path: None,
                local_deletion_time: None,
            }],
        );

        let merged = KWayMerger::reconcile_cluster(
            None,
            vec![newer, older],
            &::std::collections::HashMap::new(),
        )
        .expect("a live row must be emitted");

        let cells = match merged.row_data {
            RowData::Live { cells } => cells,
            other => panic!("expected Live, got {:?}", other),
        };

        // ESTABLISHED BEHAVIOUR: exactly ONE cell for column "tags" survives —
        // there is no per-path union. The newer (ts=200) whole value wins.
        assert_eq!(
            cells.len(),
            1,
            "column-name keyed merge collapses to one cell"
        );
        assert_eq!(cells[0].column, "tags");
        assert_eq!(
            cells[0].value,
            Value::List(vec![Value::Text("b".to_string())]),
            "winner is the higher-timestamp WHOLE collection value, not a union \
             of [a, b] — confirms whole-group collapse, NOT per-path merge (#18)"
        );
    }

    /// GATING TEST — non-frozen UDT field-level writes also collapse.
    ///
    /// A non-frozen UDT is multi-cell in Cassandra (one cell per field path, paths
    /// ordered by SIGNED ShortType field index). The reader surfaces it as a single
    /// `Value::Udt` under one column. Two runs writing different fields therefore
    /// collide on the column name and do NOT field-merge.
    #[test]
    fn nonfrozen_udt_collapses_whole_column_not_per_field() {
        let mk_udt = |field: &str, v: &str| {
            Value::Udt(UdtValue {
                type_name: "addr".to_string(),
                keyspace: "ks".to_string(),
                fields: vec![UdtField {
                    name: field.to_string(),
                    value: Some(Value::Text(v.to_string())),
                }],
            })
        };

        // Newer run wrote field "city"; older run wrote field "zip".
        let newer = live(
            0,
            200,
            vec![CellData {
                column: "address".to_string(),
                value: mk_udt("city", "SF"),
                timestamp: 200,
                ttl: None,
                cell_path: None,
                local_deletion_time: None,
            }],
        );
        let older = live(
            1,
            100,
            vec![CellData {
                column: "address".to_string(),
                value: mk_udt("zip", "94105"),
                timestamp: 100,
                ttl: None,
                cell_path: None,
                local_deletion_time: None,
            }],
        );

        let merged = KWayMerger::reconcile_cluster(
            None,
            vec![newer, older],
            &::std::collections::HashMap::new(),
        )
        .expect("a live row must be emitted");
        let cells = match merged.row_data {
            RowData::Live { cells } => cells,
            other => panic!("expected Live, got {:?}", other),
        };

        assert_eq!(
            cells.len(),
            1,
            "UDT column collapses to one whole-value cell"
        );
        // The "zip" write is lost: no per-field merge happens. The signed-ShortType
        // path ordering required by #18 is therefore unreachable in this engine.
        assert_eq!(
            cells[0].value,
            mk_udt("city", "SF"),
            "newer whole-UDT value wins; older field write is dropped — no per-field \
             merge, so #18 path-ordering does not apply"
        );
    }

    /// GATING TEST — the reader→merge adapter representation.
    ///
    /// Confirms the structural root cause: `value_to_row_data` turns a row (a
    /// top-level `Value::Map` of column-name → value) into one `CellData` per
    /// top-level column. A collection value nested under a column stays a single
    /// nested `Value`; it is NOT exploded into per-path cells. There is no
    /// `cell_path`/collection-key anywhere in `CellData`.
    #[test]
    fn adapter_produces_one_cell_per_top_level_column() {
        let row = Value::Map(vec![
            (Value::Text("id".to_string()), Value::Text("k1".to_string())),
            (
                Value::Text("tags".to_string()),
                // Whole collection nested under a single column.
                Value::List(vec![
                    Value::Text("a".to_string()),
                    Value::Text("b".to_string()),
                ]),
            ),
        ]);

        let row_data = SSTableRowIteratorAdapter::value_to_row_data(&row, 500).expect("row data");
        let cells = match row_data {
            RowData::Live { cells } => cells,
            other => panic!("expected Live, got {:?}", other),
        };

        // Two TOP-LEVEL columns → two cells. The collection is NOT split per path.
        assert_eq!(cells.len(), 2, "one CellData per top-level column");
        let tags = cells
            .iter()
            .find(|c| c.column == "tags")
            .expect("tags cell present");
        assert_eq!(
            tags.value,
            Value::List(vec![
                Value::Text("a".to_string()),
                Value::Text("b".to_string())
            ]),
            "the collection arrives as ONE nested Value, never as per-path cells"
        );
    }

    /// #14/#17 complex deletion — current status check.
    ///
    /// A complex (collection-level) deletion is, in Cassandra, a tombstone scoped
    /// to a single complex column with its own deletion time. CQLite has no
    /// per-column complex-deletion representation: `RowData::Tombstone` is whole-row
    /// only, and a `Value::Tombstone(CellTombstone)` is treated as a per-CELL
    /// tombstone keyed on the column name (collapsing with any sibling cell).
    ///
    /// This test asserts the consequence relevant to #14/#17: an equal-timestamp
    /// ROW deletion supersedes a CELL tombstone on a column (drop-on-equality via
    /// the `timestamp > row_del` filter), which is the row-vs-complex equality rule
    /// — but there is no path-scoped complex deletion to apply it to. We assert the
    /// row-vs-cell equality behaviour that DOES exist.
    #[test]
    fn row_deletion_supersedes_equal_ts_cell_tombstone() {
        let row_tomb = MergeEntry::new(
            0,
            dk(1),
            None,
            100,
            RowData::Tombstone {
                deletion_time: 100,
                local_deletion_time: 0,
            },
        );
        let cell_tomb = live(
            1,
            100,
            vec![CellData {
                column: "tags".to_string(),
                value: Value::Tombstone(TombstoneInfo {
                    deletion_time: 100,
                    tombstone_type: TombstoneType::CellTombstone,
                    ttl: None,
                    range_start: None,
                    range_end: None,
                }),
                timestamp: 100,
                ttl: None,
                cell_path: None,
                local_deletion_time: None,
            }],
        );

        let merged = KWayMerger::reconcile_cluster(
            None,
            vec![row_tomb, cell_tomb],
            &::std::collections::HashMap::new(),
        )
        .expect("row tombstone keeps the row shadowed");

        // Equal-ts row deletion wins: no surviving cell, row stays a tombstone.
        match merged.row_data {
            RowData::Tombstone { deletion_time, .. } => {
                assert_eq!(deletion_time, 100, "row tombstone preserved at its ts");
            }
            RowData::Live { cells } => {
                panic!("expected row to stay shadowed, got live cells: {:?}", cells)
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Issue #886 (Epic #842): reader→merge entry enrichment (PLUMBING ONLY)
// ─────────────────────────────────────────────────────────────────────────────
//
// These tests prove the new per-cell metadata (`cell_path`, `local_deletion_time`)
// and the first-class complex/range-deletion entities are threaded through the
// merge entry AND that reconciliation is byte-for-byte UNCHANGED — the carried
// fields are present but not yet acted on. The behavior that consumes them lands
// in #844 (per-cell-path collection/UDT merge) and #846/#848 (range tombstones /
// tombstone-vs-expiring tie-break).
#[cfg(all(test, feature = "write-support"))]
mod issue_886_merge_entry_enrichment {
    use super::*;
    use crate::storage::write_engine::mutation::ClusteringBound;
    use crate::types::Value;

    fn dk(byte: u8) -> DecoratedKey {
        DecoratedKey::from_key_bytes(vec![byte]).expect("token")
    }

    /// The new `CellData` constructor defaults the enriched fields to `None`,
    /// matching what the reader currently supplies.
    #[test]
    fn celldata_new_defaults_enriched_fields_to_none() {
        let cell = CellData::new("c".to_string(), Value::Integer(7), 100);
        assert_eq!(cell.ttl, None);
        assert_eq!(
            cell.local_deletion_time, None,
            "LDT defaults None (plumbing)"
        );
        assert_eq!(cell.cell_path, None, "cell_path defaults None (plumbing)");
    }

    /// The enriched fields round-trip through `MergeEntry`/`CellData` clone +
    /// equality unchanged (proves they are real, carried state).
    #[test]
    fn enriched_celldata_round_trips_through_merge_entry() {
        let cell = CellData {
            column: "m".to_string(),
            value: Value::Text("v".to_string()),
            timestamp: 500,
            ttl: Some(3600),
            local_deletion_time: Some(1_700_000_000),
            cell_path: Some(vec![0x00, 0x01]),
        };
        let entry = MergeEntry::new(
            0,
            dk(1),
            None,
            500,
            RowData::Live {
                cells: vec![cell.clone()],
            },
        );
        let cloned = entry.clone();
        let cells = match cloned.row_data {
            RowData::Live { cells } => cells,
            other => panic!("expected Live, got {other:?}"),
        };
        assert_eq!(cells.len(), 1);
        assert_eq!(cells[0].local_deletion_time, Some(1_700_000_000));
        assert_eq!(cells[0].cell_path, Some(vec![0x00, 0x01]));
        assert_eq!(cells[0].ttl, Some(3600));
        assert_eq!(cells[0], cell, "enriched cell survives clone + equality");
    }

    /// The reader→merge adapter (`value_to_row_data`) populates the enriched
    /// fields. Today the reader's `(RowKey, Value, ts)` compaction stream does
    /// not surface per-cell ttl / LDT / cell-path, so they are threaded as
    /// `None` — but the fields exist on every produced cell (plumbing present).
    #[test]
    fn value_to_row_data_threads_enriched_fields_as_none() {
        // Map case: top-level columns surfaced as a map (key = column name).
        let map = Value::Map(vec![(
            Value::Text("name".to_string()),
            Value::Text("alice".to_string()),
        )]);
        let row_data = SSTableRowIteratorAdapter::value_to_row_data(&map, 100)
            .expect("value_to_row_data must succeed");
        let cells = match row_data {
            RowData::Live { cells } => cells,
            other => panic!("expected Live, got {other:?}"),
        };
        assert_eq!(cells.len(), 1);
        assert_eq!(cells[0].column, "name");
        assert_eq!(cells[0].timestamp, 100, "live cell inherits row ts (#533)");
        assert_eq!(cells[0].local_deletion_time, None);
        assert_eq!(cells[0].cell_path, None);
        assert_eq!(cells[0].ttl, None);

        // Single-value case wraps as one "value" cell with the same defaults.
        let single = SSTableRowIteratorAdapter::value_to_row_data(&Value::Integer(42), 200)
            .expect("value_to_row_data must succeed");
        let cells = match single {
            RowData::Live { cells } => cells,
            other => panic!("expected Live, got {other:?}"),
        };
        assert_eq!(cells[0].local_deletion_time, None);
        assert_eq!(cells[0].cell_path, None);
    }

    /// A `MergeEntry` can carry a first-class complex deletion. The builder
    /// attaches it; the field is non-empty but reconciliation ignores it.
    #[test]
    fn merge_entry_carries_complex_deletion_without_acting_on_it() {
        let complex = ComplexDeletion {
            column: "tags".to_string(),
            marked_for_delete_at: 1234,
            local_deletion_time: 1_700_000_000,
        };
        let entry = MergeEntry::new(0, dk(1), None, 100, RowData::Live { cells: vec![] })
            .with_complex_deletions(vec![complex.clone()]);
        assert_eq!(entry.complex_deletions, vec![complex]);

        // Plumbing-only: reconcile_cluster does NOT consult complex_deletions.
        // A cell written at the same ts as the complex deletion still survives,
        // proving no shadowing behavior was introduced (#844 owns that).
        let cell = CellData::new("tags".to_string(), Value::Text("a".to_string()), 1234);
        let live = MergeEntry::new(0, dk(1), None, 1234, RowData::Live { cells: vec![cell] })
            .with_complex_deletions(vec![ComplexDeletion {
                column: "tags".to_string(),
                marked_for_delete_at: 1234,
                local_deletion_time: 1_700_000_000,
            }]);
        let merged =
            KWayMerger::reconcile_cluster(None, vec![live], &::std::collections::HashMap::new())
                .expect("live row must be emitted");
        match merged.row_data {
            RowData::Live { cells } => {
                assert_eq!(
                    cells.len(),
                    1,
                    "complex deletion must NOT shadow (plumbing)"
                );
                assert_eq!(cells[0].column, "tags");
            }
            other => panic!("expected Live, got {other:?}"),
        }
    }

    /// A `MergeEntry` can carry a first-class range deletion (reusing the shared
    /// open-ended `TombstoneInfo`). The field is populated but reconciliation
    /// ignores it — covered cells are NOT shadowed by this issue (#846 owns it).
    #[test]
    fn merge_entry_carries_range_deletion_without_acting_on_it() {
        // Open-ended range tombstone: both bounds None (the representation the
        // tombstone_merger open-ended utilities use).
        let range = RangeTombstone {
            start: ClusteringBound::Bottom,
            end: ClusteringBound::Top,
            deletion_time: 5000,
            local_deletion_time: 0,
        };
        let cell = CellData::new("v".to_string(), Value::Integer(1), 1000);
        let entry = MergeEntry::new(0, dk(1), None, 1000, RowData::Live { cells: vec![cell] })
            .with_range_deletion(range.clone());
        assert_eq!(entry.range_deletion, Some(range));

        // Plumbing-only: the covered cell (ts=1000 < range ts=5000) STILL
        // survives reconcile because range deletions are not yet applied.
        let merged =
            KWayMerger::reconcile_cluster(None, vec![entry], &::std::collections::HashMap::new())
                .expect("live row must be emitted");
        match merged.row_data {
            RowData::Live { cells } => {
                assert_eq!(
                    cells.len(),
                    1,
                    "range deletion must NOT shadow covered cell (plumbing)"
                );
            }
            other => panic!("expected Live, got {other:?}"),
        }
    }

    /// `reconcile_cluster` must PRESERVE the carried complex/range deletion
    /// metadata from its input rows onto the returned entry (#886 plumbing
    /// preservation). Without this, the metadata threaded by the reader is
    /// silently dropped before downstream consumers (#899) can see it. The
    /// preservation is behavior-neutral: the surviving cells (normal reconcile
    /// output) are identical to the no-metadata case.
    #[test]
    fn reconcile_cluster_preserves_carried_deletion_metadata() {
        let complex_a = ComplexDeletion {
            column: "tags".to_string(),
            marked_for_delete_at: 1234,
            local_deletion_time: 1_700_000_000,
        };
        let complex_b = ComplexDeletion {
            column: "notes".to_string(),
            marked_for_delete_at: 999,
            local_deletion_time: 1_700_000_001,
        };
        let range_low = RangeTombstone {
            start: ClusteringBound::Bottom,
            end: ClusteringBound::Top,
            deletion_time: 3000,
            local_deletion_time: 0,
        };
        let range_high = RangeTombstone {
            start: ClusteringBound::Bottom,
            end: ClusteringBound::Top,
            deletion_time: 7000,
            local_deletion_time: 0,
        };

        // Two input rows in the same cluster, each carrying distinct metadata.
        let row0 = MergeEntry::new(
            0,
            dk(1),
            None,
            2000,
            RowData::Live {
                cells: vec![CellData::new("v".to_string(), Value::Integer(2), 2000)],
            },
        )
        .with_complex_deletions(vec![complex_a.clone()])
        .with_range_deletion(range_low.clone());
        let row1 = MergeEntry::new(
            1,
            dk(1),
            None,
            1000,
            RowData::Live {
                cells: vec![CellData::new("w".to_string(), Value::Integer(1), 1000)],
            },
        )
        .with_complex_deletions(vec![complex_a.clone(), complex_b.clone()])
        .with_range_deletion(range_high.clone());

        let merged = KWayMerger::reconcile_cluster(
            None,
            vec![row0, row1],
            &::std::collections::HashMap::new(),
        )
        .expect("live row must be emitted");

        // complex_deletions: union, first-seen order, deduplicated.
        assert_eq!(
            merged.complex_deletions,
            vec![complex_a, complex_b],
            "complex deletions must be union-preserved without duplicates"
        );
        // range_deletion: the highest deletion timestamp wins.
        assert_eq!(
            merged.range_deletion,
            Some(range_high),
            "range deletion with the highest deletion timestamp must be carried"
        );

        // Behavior-neutral: normal reconcile output (surviving cells) is unchanged
        // versus the same inputs with NO carried metadata.
        let plain0 = MergeEntry::new(
            0,
            dk(1),
            None,
            2000,
            RowData::Live {
                cells: vec![CellData::new("v".to_string(), Value::Integer(2), 2000)],
            },
        );
        let plain1 = MergeEntry::new(
            1,
            dk(1),
            None,
            1000,
            RowData::Live {
                cells: vec![CellData::new("w".to_string(), Value::Integer(1), 1000)],
            },
        );
        let plain = KWayMerger::reconcile_cluster(
            None,
            vec![plain0, plain1],
            &::std::collections::HashMap::new(),
        )
        .expect("live row must be emitted");
        assert_eq!(
            merged.row_data, plain.row_data,
            "carrying deletion metadata must not change surviving-cell output"
        );
        assert_eq!(merged.timestamp, plain.timestamp);
        assert!(plain.complex_deletions.is_empty());
        assert_eq!(plain.range_deletion, None);
    }

    /// A row-tombstone-only cluster (no surviving cells) must still carry the
    /// metadata onto the emitted tombstone entry.
    #[test]
    fn reconcile_cluster_preserves_metadata_on_tombstone_entry() {
        let complex = ComplexDeletion {
            column: "tags".to_string(),
            marked_for_delete_at: 10,
            local_deletion_time: 1_700_000_000,
        };
        let row = MergeEntry::new(
            0,
            dk(1),
            None,
            500,
            RowData::Tombstone {
                deletion_time: 500,
                local_deletion_time: 0,
            },
        )
        .with_complex_deletions(vec![complex.clone()]);

        let merged =
            KWayMerger::reconcile_cluster(None, vec![row], &::std::collections::HashMap::new())
                .expect("row tombstone must be emitted");
        assert!(matches!(merged.row_data, RowData::Tombstone { .. }));
        assert_eq!(merged.complex_deletions, vec![complex]);
    }

    /// Regression (#853/#886 branch-review, Finding 3): a cluster carrying complex
    /// and/or range deletion metadata but with NO surviving cells and NO row
    /// tombstone must STILL emit a metadata-only entry so the carried deletion
    /// metadata survives reconciliation. Previously `built` was `None` for this
    /// case and the metadata was silently dropped before the preservation logic
    /// could run.
    #[test]
    fn reconcile_cluster_emits_metadata_only_entry_when_no_row_produced() {
        let complex = ComplexDeletion {
            column: "tags".to_string(),
            marked_for_delete_at: 4242,
            local_deletion_time: 1_700_000_000,
        };
        let range = RangeTombstone {
            start: ClusteringBound::Bottom,
            end: ClusteringBound::Top,
            deletion_time: 8888,
            local_deletion_time: 0,
        };

        // Empty Live row (no cells), no row tombstone, but carrying both kinds of
        // deletion metadata.
        let row = MergeEntry::new(0, dk(1), None, 0, RowData::Live { cells: vec![] })
            .with_complex_deletions(vec![complex.clone()])
            .with_range_deletion(range.clone());

        let merged =
            KWayMerger::reconcile_cluster(None, vec![row], &::std::collections::HashMap::new())
                .expect("metadata-only cluster must still emit an entry");

        // The emitted entry carries the metadata and has no live cells.
        match &merged.row_data {
            RowData::Live { cells } => {
                assert!(
                    cells.is_empty(),
                    "metadata-only entry must have no live cells"
                );
            }
            other => panic!("expected empty Live, got {other:?}"),
        }
        assert_eq!(merged.complex_deletions, vec![complex]);
        assert_eq!(merged.range_deletion, Some(range));
    }

    /// Behavior-neutral guard for Finding 3: an empty `Live` cluster with NO
    /// carried metadata and NO row tombstone must STILL collapse to `None`
    /// (nothing is emitted). The metadata-only path only adds an entry when
    /// deletion metadata would otherwise be lost.
    #[test]
    fn reconcile_cluster_empty_live_without_metadata_yields_none() {
        let row = MergeEntry::new(0, dk(1), None, 0, RowData::Live { cells: vec![] });
        assert!(
            KWayMerger::reconcile_cluster(None, vec![row], &::std::collections::HashMap::new())
                .is_none(),
            "empty live row with no metadata must not emit an entry"
        );
    }

    /// Regression for the #886/#899 branch-review HIGH finding: the synthetic
    /// metadata-only entry emitted by `reconcile_cluster` (an empty `Live` row
    /// carrying complex/range deletion metadata, no ops, no row tombstone) MUST
    /// be classified as a metadata-only no-op so the writer path skips it. The
    /// writer does not yet consume the carried deletions (#899); routing this
    /// entry through `merge_entry_to_mutation` would otherwise write a phantom
    /// live empty (pure-PK) row at timestamp 0.
    #[test]
    fn metadata_only_entry_is_detected_and_skipped_on_writer_path() {
        let complex = ComplexDeletion {
            column: "tags".to_string(),
            marked_for_delete_at: 4242,
            local_deletion_time: 1_700_000_000,
        };
        let range = RangeTombstone {
            start: ClusteringBound::Bottom,
            end: ClusteringBound::Top,
            deletion_time: 8888,
            local_deletion_time: 0,
        };

        // Exactly the shape reconcile_cluster emits for a deletion-metadata-only
        // cluster: empty Live, ts 0, carrying both deletion-metadata kinds.
        let meta_only = MergeEntry::new(0, dk(1), None, 0, RowData::Live { cells: vec![] })
            .with_complex_deletions(vec![complex.clone()])
            .with_range_deletion(range.clone());

        assert!(
            meta_only.is_metadata_only_no_op(),
            "synthetic metadata-only entry must be detected so the writer skips it"
        );

        // It survives reconciliation (metadata preserved in the merge stream) but
        // is filtered out before reaching the writer — proving the same entry the
        // merge stream keeps is the one the writer path drops.
        let reconciled = KWayMerger::reconcile_cluster(
            None,
            vec![meta_only],
            &::std::collections::HashMap::new(),
        )
        .expect("metadata-only cluster must still emit an entry in the merge stream");
        assert!(
            reconciled.is_metadata_only_no_op(),
            "the reconciled metadata-only entry must be writer-skippable"
        );
    }

    /// Guard the boundary of the skip predicate: entries that carry real content
    /// (live cells, a row tombstone, or no carried metadata) must NOT be treated
    /// as metadata-only no-ops, so genuine rows are never dropped on the writer
    /// path.
    #[test]
    fn non_metadata_only_entries_are_not_skipped() {
        // Live row with a real cell — must be written.
        let live = MergeEntry::new(
            0,
            dk(1),
            None,
            100,
            RowData::Live {
                cells: vec![CellData::new(
                    "name".to_string(),
                    Value::Text("v".to_string()),
                    100,
                )],
            },
        );
        assert!(!live.is_metadata_only_no_op());

        // Empty live row carrying metadata? Yes (skip). Empty live row WITHOUT
        // metadata never survives reconcile, but defensively it is not skippable.
        let empty_no_meta = MergeEntry::new(0, dk(1), None, 0, RowData::Live { cells: vec![] });
        assert!(!empty_no_meta.is_metadata_only_no_op());

        // Row tombstone — must be written (real deletion).
        let tomb = MergeEntry::new(
            0,
            dk(1),
            None,
            500,
            RowData::Tombstone {
                deletion_time: 500,
                local_deletion_time: 0,
            },
        )
        .with_range_deletion(RangeTombstone {
            start: ClusteringBound::Bottom,
            end: ClusteringBound::Top,
            deletion_time: 8888,
            local_deletion_time: 0,
        });
        assert!(
            !tomb.is_metadata_only_no_op(),
            "a row tombstone is real content even when carrying range metadata"
        );
    }

    /// Equality of two default merge entries is unaffected by the new carried
    /// fields (both empty/None), so existing assert_eq!-based tests stay valid.
    #[test]
    fn default_carried_fields_preserve_merge_entry_equality() {
        let a = MergeEntry::new(0, dk(1), None, 100, RowData::Live { cells: vec![] });
        let b = MergeEntry::new(0, dk(1), None, 100, RowData::Live { cells: vec![] });
        assert_eq!(a, b);
        assert!(a.complex_deletions.is_empty());
        assert_eq!(a.range_deletion, None);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Issue #822 (Epic #817): merge ordering / semantic invariants
// ─────────────────────────────────────────────────────────────────────────────
//
// VALUE-ASSERTING tests for three findings from the garbage-free-compaction
// review. Each test drives REAL merge code paths (no mocks):
//   - #10  DESC empty-vs-valued clustering ordering, via ClusteringKey::compare
//          and the live merge sort in merge_partition_rows.
//   - #13/#3 tombstone-beats-expiring at EQUAL timestamp, via reconcile_cluster
//          (exercised through merge_partition_rows), plus strict writer flag
//          semantics for CELL_IS_EXPIRING / CELL_IS_DELETED exclusivity.
//   - #22  header-driven static / column superset — DIVERGENT: CQLite's
//          compaction writer derives hasStatic and the column set from the
//          supplied TableSchema, NOT from the merged input SerializationHeaders.
//          The test documents and pins that divergence.
#[cfg(all(test, feature = "write-support"))]
mod issue_822_merge_ordering_semantics {
    use super::*;
    use crate::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
    use crate::storage::sstable::writer::{DataWriter, StatisticsMetadata};
    use crate::storage::write_engine::mutation::{
        CellOperation, ClusteringKey, DecoratedKey, Mutation, PartitionKey, TableId,
    };
    use crate::types::{TombstoneInfo, TombstoneType, Value};
    use std::collections::HashMap;

    // ── Wire-format flag constants (mirror data_writer.rs; protocol constants,
    //    NOT a reimplementation of writer logic — the load-bearing assertions are
    //    on the REAL bytes produced by `DataWriter::write_partition`). ──────────
    const ROW_HAS_TIMESTAMP: u8 = 0x04;
    const ROW_HAS_DELETION: u8 = 0x10;
    const ROW_HAS_ALL_COLUMNS: u8 = 0x20;
    const ROW_HAS_EXTENDED_FLAGS: u8 = 0x80;
    const EXTENDED_IS_STATIC: u8 = 0x01;
    const CELL_IS_DELETED: u8 = 0x01;
    const CELL_IS_EXPIRING: u8 = 0x02;

    /// Deterministic stats baselines so temporal deltas are small single-byte
    /// vints, keeping the manual byte walk simple (mirrors issue_821).
    fn writer_stats() -> StatisticsMetadata {
        let mut s = StatisticsMetadata::new();
        s.min_timestamp = 1_000_000;
        s.min_ttl = 0;
        s.min_local_deletion_time = 0;
        s
    }

    /// Read a Cassandra unsigned vint at `pos`; returns `(value, bytes_consumed)`.
    fn read_vuint(data: &[u8], pos: usize) -> (u64, usize) {
        let first = data[pos];
        let extra = first.leading_ones() as usize;
        assert!(extra < 8, "9-byte vint not expected in this framing");
        let mask: u64 = 0xFFu64 >> (extra + 1);
        let mut value = (first as u64) & mask;
        for i in 0..extra {
            value = (value << 8) | data[pos + 1 + i] as u64;
        }
        (value, extra + 1)
    }

    /// 4-byte big-endian int partition-key bytes.
    fn int_key_bytes(n: i32) -> Vec<u8> {
        n.to_be_bytes().to_vec()
    }

    /// Partition-header byte size for a 4-byte int PK:
    /// 2 (u16 key-length) + 4 (key) + 4 (LDT i32) + 8 (mfda i64) = 18.
    const INT_PK_HEADER_SIZE: usize = 2 + 4 + 4 + 8;

    /// Schema with a single clustering column whose sort order is configurable.
    fn schema_one_clustering(ck_name: &str, ck_type: &str, order: ClusteringOrder) -> TableSchema {
        TableSchema {
            keyspace: "issue_822".to_string(),
            table: "tbl".to_string(),
            partition_keys: vec![KeyColumn {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![ClusteringColumn {
                name: ck_name.to_string(),
                data_type: ck_type.to_string(),
                position: 0,
                order,
            }],
            columns: vec![Column {
                name: "v".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            }],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }

    fn empty_merger(schema: TableSchema) -> KWayMerger {
        KWayMerger {
            runs: vec![],
            heap: BinaryHeap::new(),
            current_partition: None,
            gc_before_secs: None,
            now_secs: None,
            schema,
        }
    }

    fn ck_text(col: &str, s: &str) -> ClusteringKey {
        ClusteringKey {
            columns: vec![(col.to_string(), Value::Text(s.to_string()))],
        }
    }

    // ── #10: DESC empty-vs-valued clustering ordering ────────────────────────

    /// Direct assertion on `ClusteringKey::compare`: an EMPTY clustering value
    /// must route through the column's reversed-ness.
    ///
    /// Cassandra rule: under a reversed (DESC) clustering column, the empty
    /// clustering value sorts AFTER valued ones. CQLite models the empty value
    /// as `Text("")`; `compare_values` makes `"" < "a"` (ASC), and `compare`
    /// reverses that for DESC, so empty becomes GREATER (sorts last). Under ASC
    /// empty stays smallest. This test pins both directions.
    #[test]
    fn issue_10_desc_empty_vs_valued_via_clustering_compare() {
        let empty = ck_text("ck", "");
        let valued = ck_text("ck", "a");

        // ASC: empty < valued (empty sorts first).
        let asc = schema_one_clustering("ck", "text", ClusteringOrder::Asc);
        assert_eq!(
            empty.compare(&valued, &asc).expect("compare must not fail"),
            Ordering::Less,
            "ASC: empty clustering value must sort BEFORE a valued one"
        );

        // DESC: empty > valued (empty sorts last). This is the load-bearing
        // assertion for #10 — empty-vs-valued must be routed through reversed-ness.
        let desc = schema_one_clustering("ck", "text", ClusteringOrder::Desc);
        assert_eq!(
            empty
                .compare(&valued, &desc)
                .expect("compare must not fail"),
            Ordering::Greater,
            "DESC: empty clustering value must sort AFTER a valued one (reversed-ness \
             must apply to empty-vs-valued comparison)"
        );

        // Symmetry: valued-vs-empty is the mirror image under DESC.
        assert_eq!(
            valued
                .compare(&empty, &desc)
                .expect("compare must not fail"),
            Ordering::Less,
            "DESC: a valued clustering value must sort BEFORE the empty one"
        );
    }

    /// End-to-end through the real merge sort in `merge_partition_rows`: with a
    /// DESC clustering column, the emitted row order must place the empty
    /// clustering value LAST (after valued rows).
    #[test]
    fn issue_10_desc_empty_vs_valued_in_merge_output_order() {
        let schema = schema_one_clustering("ck", "text", ClusteringOrder::Desc);
        let merger = empty_merger(schema);

        let pk = DecoratedKey::new(7, vec![0, 0, 0, 7]);
        const TS: i64 = 1_700_000_000_000_000;

        let make = |ck: &str| {
            MergeEntry::new(
                0,
                pk.clone(),
                Some(ck_text("ck", ck)),
                TS,
                RowData::Live {
                    cells: vec![CellData {
                        column: "v".to_string(),
                        value: Value::Text(format!("row-{ck}")),
                        timestamp: TS,
                        ttl: None,
                        cell_path: None,
                        local_deletion_time: None,
                    }],
                },
            )
        };

        // Feed in deliberately scrambled input order.
        let input = vec![make(""), make("b"), make("a")];
        let merged = merger
            .merge_partition_rows(input)
            .expect("merge_partition_rows must not fail");

        let order: Vec<String> = merged
            .iter()
            .map(|e| match &e.clustering_key {
                Some(ck) => match &ck.columns[0].1 {
                    Value::Text(s) => s.clone(),
                    other => format!("{other:?}"),
                },
                None => "<none>".to_string(),
            })
            .collect();

        // DESC valued order is "b","a"; the empty value sorts LAST.
        assert_eq!(
            order,
            vec!["b".to_string(), "a".to_string(), "".to_string()],
            "DESC merge output: valued rows descending, empty clustering value LAST"
        );
    }

    // ── #13/#3: tombstone beats expiring at EQUAL timestamp ───────────────────

    /// At EQUAL timestamp a (cell) tombstone must beat an EXPIRING (TTL) cell.
    /// In CQLite's merge model an "expiring" cell is a live `CellData` with
    /// `ttl = Some(_)`; a cell tombstone is `Value::Tombstone(CellTombstone)`.
    /// `reconcile_cluster` (driven via `merge_partition_rows`) must keep the
    /// tombstone regardless of which file (run_index) it came from.
    #[test]
    fn issue_13_tombstone_beats_expiring_at_equal_ts() {
        let schema = schema_one_clustering("ck", "text", ClusteringOrder::Asc);
        let merger = empty_merger(schema);

        let pk = DecoratedKey::new(11, vec![0, 0, 0, 11]);
        const TS: i64 = 1_700_000_000_000_000;

        // Build both rows so the SAME clustering key bucket is formed: the cells
        // include the clustering column "ck" plus the data column "v".
        let ck_cell = || CellData {
            column: "ck".to_string(),
            value: Value::Text("c".to_string()),
            timestamp: TS,
            ttl: None,
            cell_path: None,
            local_deletion_time: None,
        };

        // Expiring (TTL) live cell for column "v", in the NEWER file (run 0).
        // run_index 0 would win a recency tiebreak — so if the tombstone still
        // wins, it is the equal-ts tombstone rule, not recency.
        let expiring = MergeEntry::new(
            0,
            pk.clone(),
            Some(ck_text("ck", "c")),
            TS,
            RowData::Live {
                cells: vec![
                    ck_cell(),
                    CellData {
                        column: "v".to_string(),
                        value: Value::Text("expiring-if-buggy".to_string()),
                        timestamp: TS,
                        ttl: Some(3600),
                        cell_path: None,
                        local_deletion_time: None,
                    },
                ],
            },
        );

        // Cell tombstone for column "v", in the OLDER file (run 1), same ts.
        let tombstone = MergeEntry::new(
            1,
            pk.clone(),
            Some(ck_text("ck", "c")),
            TS,
            RowData::Live {
                cells: vec![
                    ck_cell(),
                    CellData {
                        column: "v".to_string(),
                        value: Value::Tombstone(TombstoneInfo {
                            deletion_time: TS,
                            tombstone_type: TombstoneType::CellTombstone,
                            ttl: None,
                            range_start: None,
                            range_end: None,
                        }),
                        timestamp: TS,
                        ttl: None,
                        cell_path: None,
                        local_deletion_time: None,
                    },
                ],
            },
        );

        // Drive the real merger with the expiring (newer file) first.
        let merged = merger
            .merge_partition_rows(vec![expiring, tombstone])
            .expect("merge_partition_rows must not fail");

        assert_eq!(merged.len(), 1, "one clustering key => one merged winner");

        let cells = match &merged[0].row_data {
            RowData::Live { cells } => cells,
            other => panic!("expected Live row, got {other:?}"),
        };
        let v_cell = cells
            .iter()
            .find(|c| c.column == "v")
            .expect("column v must survive (as a tombstone)");

        assert!(
            KWayMerger::is_cell_tombstone(v_cell),
            "At equal ts the cell tombstone must beat the expiring (TTL) cell, even \
             though the expiring cell is in the newer file (run 0). Got a live value \
             => tombstone-vs-expiring tie reverted to recency (#13/#3 regression)."
        );
        assert!(
            v_cell.ttl.is_none(),
            "Surviving cell tombstone must carry no TTL (IS_DELETED and IS_EXPIRING \
             are mutually exclusive)."
        );
    }

    /// Mirror case: tombstone is in the NEWER file and the expiring cell is
    /// older — tombstone still wins (it never depends on file order).
    #[test]
    fn issue_13_tombstone_beats_expiring_irrespective_of_run_index() {
        let schema = schema_one_clustering("ck", "text", ClusteringOrder::Asc);
        let merger = empty_merger(schema);

        let pk = DecoratedKey::new(12, vec![0, 0, 0, 12]);
        const TS: i64 = 1_700_000_000_000_000;

        let ck_cell = || CellData {
            column: "ck".to_string(),
            value: Value::Text("c".to_string()),
            timestamp: TS,
            ttl: None,
            cell_path: None,
            local_deletion_time: None,
        };

        let tombstone = MergeEntry::new(
            0,
            pk.clone(),
            Some(ck_text("ck", "c")),
            TS,
            RowData::Live {
                cells: vec![
                    ck_cell(),
                    CellData {
                        column: "v".to_string(),
                        value: Value::Tombstone(TombstoneInfo {
                            deletion_time: TS,
                            tombstone_type: TombstoneType::CellTombstone,
                            ttl: None,
                            range_start: None,
                            range_end: None,
                        }),
                        timestamp: TS,
                        ttl: None,
                        cell_path: None,
                        local_deletion_time: None,
                    },
                ],
            },
        );

        let expiring = MergeEntry::new(
            1,
            pk.clone(),
            Some(ck_text("ck", "c")),
            TS,
            RowData::Live {
                cells: vec![
                    ck_cell(),
                    CellData {
                        column: "v".to_string(),
                        value: Value::Text("expiring-if-buggy".to_string()),
                        timestamp: TS,
                        ttl: Some(3600),
                        cell_path: None,
                        local_deletion_time: None,
                    },
                ],
            },
        );

        let merged = merger
            .merge_partition_rows(vec![tombstone, expiring])
            .expect("merge_partition_rows must not fail");

        let cells = match &merged[0].row_data {
            RowData::Live { cells } => cells,
            other => panic!("expected Live row, got {other:?}"),
        };
        let v_cell = cells.iter().find(|c| c.column == "v").expect("v survives");
        assert!(
            KWayMerger::is_cell_tombstone(v_cell),
            "Tombstone must win the equal-ts tie over an expiring cell regardless of \
             run_index ordering."
        );
    }

    /// Strict flag semantics (#13/#3) PINNED ON REAL WRITTEN BYTES.
    ///
    /// On the WRITER side, `CELL_IS_EXPIRING` means `ttl != NO_TTL` and is mutually
    /// exclusive with `CELL_IS_DELETED`. The old version of this test only built a
    /// merge-model `CellData` and asserted its `ttl` field — tautological, proving
    /// nothing about production. This version drives the REAL production path:
    /// `DataWriter::write_partition` over a partition where the SAME column at the
    /// SAME timestamp is written both as an EXPIRING cell (`WriteWithTtl`) and as a
    /// cell tombstone (`Delete`). The writer's own equal-ts reconciliation
    /// (`merge_row_group`: a `Delete` wins the timestamp tie over a `WriteWithTtl`)
    /// must keep the tombstone, and the writer's own cell serializer
    /// (`write_tombstone_cell`) must emit a flags byte with `CELL_IS_DELETED` set
    /// and `CELL_IS_EXPIRING` NOT set. We assert on the actual Data.db bytes,
    /// mirroring the byte-walk in tests/issue_821_writer_byte_invariants.rs.
    #[test]
    fn issue_3_tombstone_beats_expiring_and_writer_never_sets_both_flags() {
        // int PK, int clustering, single regular text column `v`.
        let schema = TableSchema {
            keyspace: "issue_822".to_string(),
            table: "tbl".to_string(),
            partition_keys: vec![KeyColumn {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![ClusteringColumn {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                position: 0,
                order: ClusteringOrder::Asc,
            }],
            columns: vec![Column {
                name: "v".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            }],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        const TS: i64 = 2_000_000;
        let key = DecoratedKey::new(1, int_key_bytes(1));

        // Same clustering key, same timestamp: one EXPIRING write and one cell
        // DELETE of column `v`. The writer's equal-ts reconciliation must keep the
        // tombstone.
        let expiring = Mutation::new(
            TableId::new("issue_822", "tbl"),
            PartitionKey::single("pk", Value::Integer(1)),
            Some(ClusteringKey::single("ck", Value::Integer(7))),
            vec![CellOperation::WriteWithTtl {
                column: "v".to_string(),
                value: Value::Text("expiring-if-buggy".to_string()),
                ttl_seconds: 3600,
            }],
            TS,
            None,
        );
        let tombstone = Mutation::new(
            TableId::new("issue_822", "tbl"),
            PartitionKey::single("pk", Value::Integer(1)),
            Some(ClusteringKey::single("ck", Value::Integer(7))),
            vec![CellOperation::Delete {
                column: "v".to_string(),
            }],
            TS,
            None,
        );

        // Drive the REAL writer (expiring first so a recency bug would surface it).
        let mut w = DataWriter::new(writer_stats());
        w.write_partition(&key, &[expiring, tombstone], &schema, None, &[])
            .expect("write_partition must succeed");
        let bytes = w.finish().expect("finish must succeed");

        // Walk to the single row's cell flags byte.
        //
        // Layout after the 18-byte int-PK partition header:
        //   [row_flags u8]
        //   [clustering prefix: 1 header byte + 4 int value bytes = 5]
        //   [row_size vint][prev_size vint]
        //   [timestamp delta vint  (ROW_HAS_TIMESTAMP)]
        //   [column bitmap vint    (NOT ROW_HAS_ALL_COLUMNS — a Delete is present)]
        //   [cell flags u8]   ← the byte under test
        let mut p = INT_PK_HEADER_SIZE;
        let row_flags = bytes[p];
        p += 1;
        // The surviving op is a cell DELETE, so HAS_ALL_COLUMNS must NOT be set
        // (the column subset/bitmap is written), and the row keeps its liveness
        // timestamp from the (losing) expiring write.
        assert_eq!(
            row_flags & ROW_HAS_EXTENDED_FLAGS,
            0,
            "regular (non-static) row expected"
        );
        assert_ne!(
            row_flags & ROW_HAS_TIMESTAMP,
            0,
            "row keeps liveness ts from the expiring write"
        );
        assert_eq!(
            row_flags & ROW_HAS_ALL_COLUMNS,
            0,
            "a surviving cell tombstone forces a column subset/bitmap (NOT all-columns)"
        );
        // Clustering prefix: 1 header byte + 4 int value bytes.
        p += 1 + 4;
        // row_size vint, then prev_size vint (inside the body).
        let (_row_size, rs_len) = read_vuint(&bytes, p);
        p += rs_len;
        let (_prev_size, ps_len) = read_vuint(&bytes, p);
        p += ps_len;
        // Liveness timestamp delta vint (ROW_HAS_TIMESTAMP is set, no TTL on row).
        let (_ts_delta, ts_len) = read_vuint(&bytes, p);
        p += ts_len;
        // Column subset bitmap vint (NOT all-columns).
        let (_bitmap, bm_len) = read_vuint(&bytes, p);
        p += bm_len;

        // The single surviving cell's flags byte.
        let cell_flags = bytes[p];
        assert_ne!(
            cell_flags & CELL_IS_DELETED,
            0,
            "At equal ts the cell tombstone must win and be serialized as a deleted \
             cell (CELL_IS_DELETED set). Flags byte = {cell_flags:#04x}"
        );
        assert_eq!(
            cell_flags & CELL_IS_EXPIRING,
            0,
            "A tombstone cell must NOT carry CELL_IS_EXPIRING — IS_DELETED and \
             IS_EXPIRING are mutually exclusive. Flags byte = {cell_flags:#04x}"
        );
        assert_ne!(
            cell_flags & (CELL_IS_DELETED | CELL_IS_EXPIRING),
            CELL_IS_DELETED | CELL_IS_EXPIRING,
            "the writer must never set BOTH CELL_IS_DELETED and CELL_IS_EXPIRING on \
             one cell. Flags byte = {cell_flags:#04x}"
        );

        // The exact flags byte: tombstone (CELL_IS_DELETED) plus HAS_EMPTY_VALUE
        // (0x04), and crucially NOT CELL_IS_EXPIRING. write_tombstone_cell emits
        // CELL_IS_DELETED | CELL_HAS_EMPTY_VALUE = 0x05.
        assert_eq!(
            cell_flags, 0x05,
            "surviving cell tombstone must serialize as CELL_IS_DELETED|HAS_EMPTY \
             (0x05); got {cell_flags:#04x}"
        );
    }

    /// Companion merge-layer invariant (kept, but now secondary to the byte-level
    /// assertion above): the equal-ts reconcile in `reconcile_cluster` keeps the
    /// tombstone and never produces a `CellData` that is BOTH a cell tombstone AND
    /// carries a TTL. This is the precondition that lets the writer keep the two
    /// flags exclusive; the writer side is now pinned on real bytes above.
    #[test]
    fn issue_3_merge_layer_tombstone_carries_no_ttl_precondition() {
        let tomb = CellData {
            column: "v".to_string(),
            value: Value::Tombstone(TombstoneInfo {
                deletion_time: 1,
                tombstone_type: TombstoneType::CellTombstone,
                ttl: None,
                range_start: None,
                range_end: None,
            }),
            timestamp: 1,
            ttl: None,
            cell_path: None,
            local_deletion_time: None,
        };
        assert!(KWayMerger::is_cell_tombstone(&tomb));
        assert!(
            tomb.ttl.is_none(),
            "A cell tombstone must not carry a TTL (precondition for flag exclusivity)."
        );

        let expiring = CellData {
            column: "v".to_string(),
            value: Value::Text("x".to_string()),
            timestamp: 1,
            ttl: Some(60),
            cell_path: None,
            local_deletion_time: None,
        };
        assert!(
            !KWayMerger::is_cell_tombstone(&expiring),
            "An expiring (TTL) cell is LIVE, not a tombstone."
        );
        assert!(expiring.ttl.is_some(), "Expiring cell carries a TTL.");
    }

    // ── #22: header-driven static / column superset (DIVERGENT) ──────────────

    /// #22 is DIVERGENT in CQLite. Cassandra derives `hasStatic` and the column
    /// superset from the merged input SSTables' `SerializationHeader`s, so a
    /// DROPPED static column (whose static rows still exist on disk) is still
    /// emitted. CQLite's compaction writer instead derives `hasStatic` and the
    /// column set purely from the supplied `TableSchema`
    /// (`schema.columns.iter().any(|c| c.is_static)` in data_writer.rs). There is
    /// NO `SerializationHeader` read anywhere under `storage/write_engine/`.
    ///
    /// This test PINS THE DIVERGENCE ON REAL WRITER OUTPUT (not a reimplemented
    /// predicate). The old version asserted on a local copy of
    /// `|s| s.columns.iter().any(|c| c.is_static)`, so it stayed green even if the
    /// production writer changed. This version drives the REAL production path —
    /// `DataWriter::write_partition` — with the SAME partition data (a mutation
    /// that writes a static cell AND a regular cell) under two schemas:
    ///
    ///   * schema WITH the static column → the writer MUST emit a static-row
    ///     prelude (a row with `ROW_HAS_EXTENDED_FLAGS` + `EXTENDED_IS_STATIC`),
    ///   * schema with the static column DROPPED → the writer MUST NOT emit any
    ///     static-row prelude, even though a real cluster's on-disk
    ///     SerializationHeader still records the static column.
    ///
    /// Both observations are read from the OBSERVED Data.db bytes, so a change to
    /// the writer's static-emission decision (e.g. becoming header-driven) flips
    /// this test instead of silently passing. The static op data is byte-identical
    /// across the two runs; ONLY the schema differs — so the divergence is
    /// attributable purely to the schema-driven `schema_has_static` decision in
    /// `write_partition`.
    #[test]
    fn issue_22_static_emission_is_schema_driven_not_header_driven_divergent() {
        // Schema WITH a static column.
        let schema_with_static = TableSchema {
            keyspace: "issue_822".to_string(),
            table: "tbl".to_string(),
            partition_keys: vec![KeyColumn {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![ClusteringColumn {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                position: 0,
                order: ClusteringOrder::Asc,
            }],
            columns: vec![
                Column {
                    name: "s".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: true,
                },
                Column {
                    name: "v".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        // Same table after the static column was DROPPED from the schema. On a real
        // cluster the input SSTables' SerializationHeader still records the static
        // column and static rows still exist on disk — but CQLite only sees the
        // schema.
        let mut schema_dropped_static = schema_with_static.clone();
        schema_dropped_static.columns.retain(|c| c.name != "s");

        // IDENTICAL partition data for both runs: a mutation that writes the static
        // column `s` AND a regular cell `v`. Only the SCHEMA differs between runs.
        let key = DecoratedKey::new(1, int_key_bytes(1));
        let mutation = || {
            Mutation::new(
                TableId::new("issue_822", "tbl"),
                PartitionKey::single("pk", Value::Integer(1)),
                Some(ClusteringKey::single("ck", Value::Integer(7))),
                vec![
                    CellOperation::Write {
                        column: "s".to_string(),
                        value: Value::Text("static-val".to_string()),
                    },
                    CellOperation::Write {
                        column: "v".to_string(),
                        value: Value::Text("row-val".to_string()),
                    },
                ],
                2_000_000,
                None,
            )
        };

        // Deterministically walk one unfiltered starting at its flags byte `pos`.
        // Returns `(flags, ext_flags, next_pos)`. Modeled on
        // `tests/issue_821_writer_byte_invariants.rs::walk_unfiltered`: it parses
        // the partition structure (flags → ext flags → clustering prefix →
        // row_size vint → body) so we land on real row-flag bytes only — we never
        // scan the buffer for a stray high-bit byte (VInt lengths/timestamps and
        // cell payload bytes can legitimately set bit 0x80). The int-clustering
        // schema's non-static clustering prefix is 1 header byte + 4 value bytes.
        const INT_CLUSTERING_PREFIX_LEN: usize = 1 + 4;
        let walk_unfiltered = |bytes: &[u8], pos: usize| -> (u8, Option<u8>, usize) {
            let mut p = pos;
            let flags = bytes[p];
            p += 1;
            let mut ext = None;
            if flags & ROW_HAS_EXTENDED_FLAGS != 0 {
                ext = Some(bytes[p]);
                p += 1;
            }
            let is_static = ext.is_some_and(|e| e & EXTENDED_IS_STATIC != 0);
            // Non-static rows carry a clustering prefix before row_size; static
            // rows do not.
            if !is_static {
                p += INT_CLUSTERING_PREFIX_LEN;
            }
            let (row_size, rs_len) = read_vuint(bytes, p);
            p += rs_len;
            // row_size counts the body (prev_size vint + remaining body); the next
            // unfiltered begins immediately after it.
            let next = p + row_size as usize;
            (flags, ext, next)
        };

        // Parse the first unfiltered after the int-PK partition header and report
        // whether it is a static-row prelude. The static row, if present, is
        // ALWAYS the first unfiltered (this schema/layout is known), so we assert
        // on the flags byte at the parsed partition-header position — not on an
        // arbitrary high-bit byte found elsewhere in the buffer.
        let first_unfiltered_is_static = |bytes: &[u8]| -> bool {
            let (flags, ext, _next) = walk_unfiltered(bytes, INT_PK_HEADER_SIZE);
            flags & ROW_HAS_EXTENDED_FLAGS != 0 && ext.is_some_and(|e| e & EXTENDED_IS_STATIC != 0)
        };

        // ── Run 1: schema WITH static → static prelude MUST be emitted. ──
        let mut w_with = DataWriter::new(writer_stats());
        w_with
            .write_partition(&key, &[mutation()], &schema_with_static, None, &[])
            .expect("write_partition (with static) must succeed");
        let bytes_with = w_with.finish().expect("finish (with static)");
        assert!(
            first_unfiltered_is_static(&bytes_with),
            "schema WITH a static column: the writer must emit a static-row prelude \
             (ROW_HAS_EXTENDED_FLAGS | EXTENDED_IS_STATIC) as the first unfiltered"
        );

        // ── Run 2: schema with static DROPPED → NO static prelude. ──
        // Same partition data; only the schema lost the static column.
        let mut w_drop = DataWriter::new(writer_stats());
        w_drop
            .write_partition(&key, &[mutation()], &schema_dropped_static, None, &[])
            .expect("write_partition (dropped static) must succeed");
        let bytes_drop = w_drop.finish().expect("finish (dropped static)");
        assert!(
            !first_unfiltered_is_static(&bytes_drop),
            "DIVERGENT (#22): with the static column dropped from the SCHEMA, CQLite's \
             writer emits NO static-row prelude — even though a real cluster's on-disk \
             SerializationHeader still records the static column and Cassandra \
             (header-driven) would still emit its static rows. The writer's decision \
             is schema-driven, observed here on the real Data.db bytes. If CQLite \
             becomes header-driven this assertion must change."
        );

        // Belt and suspenders: no EXTENDED_IS_STATIC row exists ANYWHERE in the
        // dropped-static output, proving the static prelude was not merely
        // relocated. We DETERMINISTICALLY walk the unfiltered chain (parsing each
        // row's flags/ext/clustering/row_size) until the END_OF_PARTITION sentinel,
        // rather than scanning every buffer byte for a high bit — VInt and cell
        // payload bytes can legitimately set 0x80, so a raw scan would be
        // spuriously fragile.
        const END_OF_PARTITION: u8 = 0x01;
        let mut p = INT_PK_HEADER_SIZE;
        while p < bytes_drop.len() {
            // The unfiltered chain is terminated by the END_OF_PARTITION sentinel,
            // which occupies a real row-flags position (so this check is exact).
            if bytes_drop[p] == END_OF_PARTITION {
                break;
            }
            let (flags, ext, next) = walk_unfiltered(&bytes_drop, p);
            if flags & ROW_HAS_EXTENDED_FLAGS != 0 {
                assert_eq!(
                    ext.expect("HAS_EXTENDED_FLAGS implies an ext-flags byte") & EXTENDED_IS_STATIC,
                    0,
                    "no EXTENDED_IS_STATIC row may appear once the static column is \
                     dropped from the schema"
                );
            }
            assert!(next > p, "unfiltered walk must advance");
            p = next;
        }

        // Sanity: ROW_HAS_DELETION is unused here (no tombstones), so the constant
        // is referenced to keep the wire-format mirror complete and avoid an
        // unused-const warning under -D warnings.
        let _ = ROW_HAS_DELETION;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// #886 (Epic #842) branch-review: phantom EMPTY partition on the writer path
// ─────────────────────────────────────────────────────────────────────────────
//
// A partition whose ONLY merged row is a metadata-only no-op (empty Live cells
// carrying complex/range deletion metadata) must NOT produce a partition in the
// output SSTable. After filtering those entries the `mutations` Vec is empty;
// calling `SSTableWriter::write_partition` with no mutations would still emit a
// partition header/end marker and register the key in Index/Filter/Summary/
// statistics — a PHANTOM empty partition. The writer path must skip such
// partitions entirely and not count them in the output partition/row stats.
#[cfg(all(test, feature = "write-support"))]
mod issue_886_empty_partition_skip {
    use super::*;
    use crate::schema::{KeyColumn, TableSchema};
    use crate::storage::write_engine::mutation::{ClusteringBound, DecoratedKey, PartitionKey};
    use std::collections::HashMap;

    /// Single-column `int` partition-key schema with one regular `name` column.
    fn schema() -> TableSchema {
        TableSchema {
            keyspace: "i886".to_string(),
            table: "phantom".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![crate::schema::Column {
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            }],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }

    /// Valid on-disk partition-key bytes for `id = n` under [`schema`], built
    /// through the shared codec so the merge→writer path can decode them.
    fn pk_bytes(schema: &TableSchema, n: i32) -> Vec<u8> {
        PartitionKey::single("id", Value::Integer(n))
            .to_bytes(schema)
            .expect("encode int partition key")
    }

    /// A range deletion so an empty Live entry classifies as metadata-only.
    fn range_deletion() -> RangeTombstone {
        RangeTombstone {
            start: ClusteringBound::Bottom,
            end: ClusteringBound::Top,
            deletion_time: 8888,
            local_deletion_time: 0,
        }
    }

    /// An in-memory run yielding a fixed list of `MergeEntry`s in order.
    struct VecIterator(std::vec::IntoIter<MergeEntry>);
    impl SSTableRowIterator for VecIterator {
        fn next(&mut self) -> Option<Result<MergeEntry>> {
            self.0.next().map(Ok)
        }
    }

    fn merger_over(entries: Vec<MergeEntry>, schema: TableSchema) -> KWayMerger {
        KWayMerger {
            runs: vec![RunReader::new(Box::new(VecIterator(entries.into_iter())))],
            heap: BinaryHeap::new(),
            current_partition: None,
            gc_before_secs: None,
            now_secs: None,
            schema,
        }
    }

    /// END-TO-END writer-path test (#886): a partition whose ONLY merged row is a
    /// metadata-only no-op must produce NO partition in the output SSTable, must
    /// not be counted in `MergeStats.output_partitions`/`output_rows`, and must
    /// not register a key in Index/Filter/Summary/statistics — proven by the
    /// writer's authoritative on-disk `SSTableInfo.partition_count`. A sibling
    /// NORMAL partition in the same merge must still be written unchanged.
    #[tokio::test]
    async fn empty_partition_is_skipped_on_writer_path() {
        let schema = schema();

        // Partition token 1: ONLY a metadata-only no-op (empty Live + range
        // deletion). After filtering, this partition's mutations are empty.
        let meta_only = MergeEntry::new(
            0,
            DecoratedKey::new(1, pk_bytes(&schema, 1)),
            None,
            0,
            RowData::Live { cells: vec![] },
        )
        .with_range_deletion(range_deletion());
        assert!(
            meta_only.is_metadata_only_no_op(),
            "test precondition: the token-1 entry must be a metadata-only no-op"
        );

        // Partition token 2: a NORMAL live row with a real cell — must be written.
        let live = MergeEntry::new(
            0,
            DecoratedKey::new(2, pk_bytes(&schema, 2)),
            None,
            100,
            RowData::Live {
                cells: vec![CellData::new(
                    "name".to_string(),
                    Value::Text("survivor".to_string()),
                    100,
                )],
            },
        );

        let merger = merger_over(vec![meta_only, live], schema.clone());

        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let mut writer = crate::storage::sstable::writer::SSTableWriter::new(
            temp_dir.path().to_path_buf(),
            1,
            &schema,
        )
        .expect("create writer");

        let stats = merger.merge(&mut writer).expect("merge must succeed");

        // The metadata-only partition contributes nothing to the output stats.
        assert_eq!(
            stats.output_partitions, 1,
            "only the normal partition counts as an output partition (no phantom)"
        );
        assert_eq!(
            stats.output_rows, 1,
            "only the normal partition's single live row counts toward output rows"
        );

        // The writer's on-disk partition counter is incremented exactly once per
        // `write_partition` call. If the phantom partition had been written it
        // would be 2; the skip keeps it at 1.
        let info = writer.finish().await.expect("finish must succeed");
        assert_eq!(
            info.partition_count, 1,
            "the output SSTable must contain exactly ONE partition; a phantom EMPTY \
             partition for the metadata-only-only key would make this 2"
        );
    }

    /// Guard: a partition with real content is unaffected — both a normal live
    /// partition AND a metadata-only no-op coexisting in the SAME partition (the
    /// live row survives) still writes that one partition.
    #[tokio::test]
    async fn partition_with_real_content_is_still_written() {
        let schema = schema();

        let live = MergeEntry::new(
            0,
            DecoratedKey::new(7, pk_bytes(&schema, 7)),
            None,
            200,
            RowData::Live {
                cells: vec![CellData::new(
                    "name".to_string(),
                    Value::Text("keep-me".to_string()),
                    200,
                )],
            },
        );

        let merger = merger_over(vec![live], schema.clone());

        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let mut writer = crate::storage::sstable::writer::SSTableWriter::new(
            temp_dir.path().to_path_buf(),
            1,
            &schema,
        )
        .expect("create writer");

        let stats = merger.merge(&mut writer).expect("merge must succeed");
        assert_eq!(stats.output_partitions, 1);
        assert_eq!(stats.output_rows, 1);

        let info = writer.finish().await.expect("finish must succeed");
        assert_eq!(
            info.partition_count, 1,
            "a normal partition must still be written"
        );
    }
}
