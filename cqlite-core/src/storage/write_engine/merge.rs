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
//! ## Memory Budget
//!
//! Total memory: k × 8KB peek buffers (where k = number of input SSTables)
//! For 10 SSTables: ~80KB memory footprint
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
use crate::storage::write_engine::mutation::{ClusteringKey, DecoratedKey};
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
}

impl MergeEntry {
    /// Create a new merge entry
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
        }
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

/// Cell data with timestamp and optional TTL
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

/// Adapter that wraps async SSTableReader into sync SSTableRowIterator
///
/// Run an async future synchronously, reusing the current tokio runtime if available.
///
/// This is the standard async-to-sync bridge pattern used throughout the write engine
/// (flush_internal, finalize_merge_blocking, SSTableRowIteratorAdapter).
#[cfg(feature = "write-support")]
fn block_on_async<F, T>(future: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => handle.block_on(future),
        Err(_) => {
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| Error::Storage(format!("Failed to create tokio runtime: {}", e)))?;
            rt.block_on(future)
        }
    }
}

/// Adapter that wraps async SSTableReader into sync SSTableRowIterator.
///
/// Pre-loads all entries from an SSTable into memory, converting
/// `(RowKey, Value)` pairs into `MergeEntry` format.
///
/// TODO(#447): Implement true streaming iteration to stay within the 128MB
/// memory budget. Currently loads all entries upfront.
#[cfg(feature = "write-support")]
struct SSTableRowIteratorAdapter {
    /// Pre-loaded entries
    entries: std::vec::IntoIter<MergeEntry>,
}

#[cfg(feature = "write-support")]
impl SSTableRowIteratorAdapter {
    /// Open an SSTable and load all entries as MergeEntry.
    ///
    /// Uses [`SSTableReader::iterate_all_partitions_for_compaction`] which
    /// returns actual per-row timestamps decoded from the on-disk row headers.
    /// This allows the k-way merger to perform timestamp-accurate last-write-wins
    /// ordering, which is essential for tombstone shadowing (Issue #505).
    fn open(path: &Path, run_index: usize) -> Result<Self> {
        use crate::platform::Platform;
        use crate::Config;
        use std::sync::Arc;

        let config = Config::default();
        let path_buf = path.to_path_buf();

        // Open SSTable reader and load all partitions with actual row timestamps.
        let raw_entries = block_on_async(async move {
            let platform = Arc::new(Platform::new(&config).await?);
            let reader =
                crate::storage::sstable::reader::SSTableReader::open(&path_buf, &config, platform)
                    .await?;
            // Use the compaction-specific path: returns (RowKey, Value, row_timestamp_micros).
            // Row/cell tombstones are emitted as Value::Tombstone with their actual
            // deletion timestamps so the merger can apply shadowing semantics (Issue #505).
            reader.iterate_all_partitions_for_compaction(None).await
        })?;

        // Convert (RowKey, Value, timestamp) tuples to MergeEntry
        let mut entries = Vec::with_capacity(raw_entries.len());
        for (row_key, value, timestamp) in raw_entries {
            let key_bytes = row_key.0;
            let decorated_key = DecoratedKey::from_key_bytes(key_bytes)?;
            let row_data = Self::value_to_row_data(&value)?;

            entries.push(MergeEntry::new(
                run_index,
                decorated_key,
                None, // Clustering key extraction deferred
                timestamp,
                row_data,
            ));
        }

        // SSTable data is already in token order from the reader, no sort needed

        Ok(Self {
            entries: entries.into_iter(),
        })
    }

    /// Convert a reader Value to RowData.
    ///
    /// Issue #505: `Value::Tombstone(RowTombstone)` is now correctly emitted by
    /// the V5CompressedLegacy parser for deleted rows, and
    /// `Value::Tombstone(CellTombstone)` appears inside `Value::Map` entries for
    /// deleted cells.  Both are surfaced here so the merger can apply shadowing
    /// semantics.
    fn value_to_row_data(value: &crate::types::Value) -> Result<RowData> {
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
                    // Issue #505: propagate the cell-level timestamp from the
                    // tombstone so merge_partition_rows can order correctly.
                    let cell_ts = match val {
                        crate::types::Value::Tombstone(info) => info.deletion_time,
                        _ => 0,
                    };
                    cells.push(CellData {
                        column,
                        value: val.clone(),
                        timestamp: cell_ts,
                        ttl: None,
                    });
                }
                Ok(RowData::Live { cells })
            }
            // Single value or other formats - wrap as a single cell
            other => Ok(RowData::Live {
                cells: vec![CellData {
                    column: "value".to_string(),
                    value: other.clone(),
                    timestamp: 0,
                    ttl: None,
                }],
            }),
        }
    }
}

#[cfg(feature = "write-support")]
impl SSTableRowIterator for SSTableRowIteratorAdapter {
    fn next(&mut self) -> Option<Result<MergeEntry>> {
        self.entries.next().map(Ok)
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
        if input_paths.is_empty() {
            return Err(Error::InvalidInput(
                "K-way merge requires at least one input file".to_string(),
            ));
        }

        // Create run readers for each input SSTable (ordered newest to oldest)
        let mut runs = Vec::with_capacity(input_paths.len());
        for (run_index, path) in input_paths.iter().enumerate() {
            let adapter = SSTableRowIteratorAdapter::open(path, run_index)?;
            runs.push(RunReader::new(Box::new(adapter)));
        }

        // Initialize heap (will be populated on first step)
        let heap = BinaryHeap::new();

        Ok(Self {
            runs,
            heap,
            current_partition: None,
            schema: schema.clone(),
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
            stats.output_partitions += 1;
            stats.output_rows += rows.len() as u64;

            // Convert MergeEntry rows back to Mutation format for writer
            let mutations = rows
                .into_iter()
                .map(|entry| Self::merge_entry_to_mutation(entry, &self.schema))
                .collect::<Result<Vec<_>>>()?;

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

    /// Liveness priority used as the equal-timestamp tiebreaker.
    ///
    /// Cassandra's reconcile rule (`Cells#reconcile`) gives the tombstone (Delete)
    /// precedence over a live cell/row at the SAME timestamp. We model this with a
    /// numeric priority where a HIGHER value wins: a row tombstone (Delete) returns
    /// `1`, a live row returns `0`. The caller sorts so that the higher priority
    /// sorts first at equal timestamp. (Issue #498)
    fn tombstone_priority(entry: &MergeEntry) -> u8 {
        match entry.row_data {
            RowData::Tombstone { .. } => 1,
            RowData::Live { .. } => 0,
        }
    }

    /// Merge rows within a single partition (last-write-wins by timestamp).
    ///
    /// At equal timestamps the tombstone wins (Cassandra `Cells#reconcile`), and
    /// only when timestamp and liveness are equal does the lower run_index (newer
    /// file) win. See [`Self::tombstone_priority`].
    fn merge_partition_rows(&self, rows: Vec<MergeEntry>) -> Result<Vec<MergeEntry>> {
        use std::collections::BTreeMap;

        // Group by clustering key using BTreeMap (ClusteringKey implements Ord)
        let mut clustered_rows: BTreeMap<Option<ClusteringKey>, Vec<MergeEntry>> = BTreeMap::new();

        for row in rows {
            clustered_rows
                .entry(row.clustering_key.clone())
                .or_default()
                .push(row);
        }

        // Merge cells for each clustering key.
        //
        // Resolution order (Cassandra `org.apache.cassandra.db.rows.Cells#reconcile`):
        //   1. timestamp        — higher wins (last-write-wins).
        //   2. liveness         — at EQUAL timestamp, a Delete (tombstone) beats a
        //                         Live row. This is independent of which file the
        //                         entries came from. (Issue #498)
        //   3. run_index        — only when timestamp AND liveness are equal, the
        //                         lower run_index (newer file) wins.
        //
        // This comparator is total and transitive: each level only acts as a
        // tiebreak for the previous, and the final `run_index.cmp` is itself total.
        let mut merged = Vec::new();
        for (_ck, mut cluster_rows) in clustered_rows {
            cluster_rows.sort_by(|a, b| {
                // Primary: higher timestamp first (descending).
                b.timestamp
                    .cmp(&a.timestamp)
                    // Secondary: at equal timestamp, tombstone (Delete) wins.
                    // `is_tombstone() == true` must sort BEFORE a live row, so we
                    // compare `b`'s liveness against `a`'s to keep descending order.
                    .then_with(|| Self::tombstone_priority(b).cmp(&Self::tombstone_priority(a)))
                    // Tertiary: lower run_index (newer file) wins.
                    .then_with(|| a.run_index.cmp(&b.run_index))
            });

            // Take the first entry: highest timestamp, then tombstone at equal ts,
            // then lowest run_index.
            if let Some(winner) = cluster_rows.into_iter().next() {
                merged.push(winner);
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
        };

        let cell2 = CellData {
            column: "name".to_string(),
            value: Value::Text("New".to_string()),
            timestamp: 2000, // Higher timestamp wins
            ttl: None,
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
                    },
                    CellData {
                        column: "age".to_string(),
                        value: Value::Integer(30),
                        timestamp: 999_000_000,
                        ttl: Some(3600),
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
                        }],
                    },
                )
            }).collect();

            // Drive the real merger.
            let merger = KWayMerger {
                runs: vec![],
                heap: std::collections::BinaryHeap::new(),
                current_partition: None,
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
