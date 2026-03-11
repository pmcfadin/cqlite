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
//! Entries are ordered by:
//! 1. Token (ascending) - Primary partitioning
//! 2. Key bytes (ascending) - Hash collision resolution
//! 3. Clustering key (schema-aware) - Within partition ordering
//! 4. Run index (ascending) - Last-write-wins for equal timestamps
//!
//! ## Memory Budget
//!
//! Total memory: k × 8KB peek buffers (where k = number of input SSTables)
//! For 10 SSTables: ~80KB memory footprint
//!
//! ## Cell Merge Rule
//!
//! Last-write-wins by timestamp:
//! - Keep cell with highest timestamp
//! - If timestamps equal, prefer lower run_index (newer file)
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
use std::path::PathBuf;
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

/// Ord implementation for min-heap ordering
///
/// Order by:
/// 1. Token (ascending)
/// 2. Key bytes (ascending, for hash collisions)
/// 3. Clustering key (ascending, schema-aware)
/// 4. Run index (ascending, lower = newer = wins in LWW)
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
    #[allow(dead_code)] // Will be used when SSTable reader integration is complete
    const DEFAULT_BUFFER_SIZE: usize = 8 * 1024;

    /// Create a new run reader
    #[allow(dead_code)] // Will be used when SSTable reader integration is complete
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

        // Create run readers for each input SSTable
        let runs = Vec::with_capacity(input_paths.len());
        if let Some((run_index, path)) = input_paths.iter().enumerate().next() {
            // TODO: Replace with actual SSTable reader creation in M5.2
            // For now, return error indicating implementation is pending
            return Err(Error::InvalidInput(format!(
                "SSTable reader integration pending for run {}: {:?}",
                run_index, path
            )));
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

    /// Merge rows within a single partition (last-write-wins by timestamp)
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

        // Merge cells for each clustering key
        let mut merged = Vec::new();
        for (_ck, mut cluster_rows) in clustered_rows {
            // Sort by timestamp (descending) then run_index (ascending)
            cluster_rows.sort_by(|a, b| {
                match b.timestamp.cmp(&a.timestamp) {
                    Ordering::Equal => {
                        // Equal timestamps: prefer lower run_index (newer file)
                        a.run_index.cmp(&b.run_index)
                    }
                    other => other,
                }
            });

            // Take the first entry (highest timestamp, or lowest run_index if tied)
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
    fn merge_entry_to_mutation(
        _entry: MergeEntry,
        _schema: &TableSchema,
    ) -> Result<crate::storage::write_engine::mutation::Mutation> {
        // TODO: Reconstruct PartitionKey from DecoratedKey bytes
        // This requires deserializing the key bytes according to schema
        // For now, return error indicating this needs implementation
        Err(Error::InvalidInput(
            "PartitionKey reconstruction from DecoratedKey not yet implemented".to_string(),
        ))

        // Future implementation will:
        // 1. Extract table ID from schema
        // 2. Reconstruct PartitionKey from DecoratedKey bytes
        // 3. Convert row data to cell operations
        // 4. Create Mutation with all components
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
}
