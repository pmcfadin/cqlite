//! Data.db writer - writes partition and row data
//!
//! Generates the Data.db component with V5CompressedLegacy (NB) format.
//! Maintains partition ordering by Murmur3 token and clustering ordering.
//! Tracks file positions for Index.db generation.
//!
//! Critical requirements:
//! - Partition ordering: By Murmur3 token, then key bytes (enforced by caller)
//! - Clustering ordering: By clustering comparator within partition (enforced by caller)
//! - Row size measurement: After VInt length bytes (Issue #237)
//! - Delta encoding: Uses Statistics.db baseline for timestamps/TTL/deletion times
//!
//! # V5CompressedLegacy Row Format
//!
//! Each row is encoded as:
//! ```text
//! [row_flags: u8]
//! [extended_flags: u8 if ROW_HAS_EXTENDED_FLAGS set]
//! [clustering_prefix: variable if present]
//! [row_size: VInt]                       ← Measured from AFTER this VInt
//! [prev_size: VInt]
//! [timestamp: VInt if ROW_HAS_TIMESTAMP]   ← Delta from min_timestamp
//! [ttl: VInt if ROW_HAS_TTL]              ← Delta from min_ttl
//! [deletion: 2 VInts if ROW_HAS_DELETION] ← local_deletion_time delta + deletion timestamp
//! [column_bitmap: VUInt bitmask of missing columns if NOT ROW_HAS_ALL_COLUMNS]
//! [cell_data...]
//! ```
//!
//! ## Row Flags
//! - `0x04` (HAS_TIMESTAMP): Timestamp delta present
//! - `0x08` (HAS_TTL): TTL delta present
//! - `0x10` (HAS_DELETION): Deletion time present (two VInts)
//! - `0x20` (HAS_ALL_COLUMNS): All columns present (no bitmap)
//! - `0x40` (HAS_COMPLEX_DELETION): Row contains complex column with deletion
//! - `0x80` (HAS_EXTENDED_FLAGS): Extended flags byte follows
//!
//! ## Cell Format
//! ```text
//! [flags: u8]
//! [timestamp: VInt if NOT USE_ROW_TIMESTAMP]  ← Delta from min_timestamp
//! [local_deletion_time: VUInt if deleted/expiring and NOT USE_ROW_TTL]
//! [ttl: VUInt if expiring and NOT USE_ROW_TTL]
//! [value_length: VInt]
//! [value_bytes]
//! ```
//!
//! ## Cell Flags
//! - `0x01` (IS_DELETED): Cell is a tombstone
//! - `0x02` (IS_EXPIRING): TTL fields follow
//! - `0x04` (HAS_EMPTY_VALUE): Zero-length value
//! - `0x08` (USE_ROW_TIMESTAMP): Use row-level timestamp (no timestamp delta)
//! - `0x10` (USE_ROW_TTL): Use row-level TTL (no TTL delta)
//!
//! References:
//! - Cassandra 5.0: `org.apache.cassandra.db.rows.UnfilteredSerializer`
//! - Parser: `cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs`
//! - Format docs: `docs/sstables-definitive-guide/chapters/05-data-db-format.md`

// Crate imports shared with the concern submodules: re-exported `pub(crate)`
// so each submodule reaches them through `use super::*` without restating the
// import list (issue #1118 split). They stay crate-internal.
pub(crate) use crate::error::{Error, Result};
pub(crate) use crate::schema::{Column, CqlType, TableSchema, UdtRegistry};
pub(crate) use crate::storage::serialization::types::TypeSerializer;
pub(crate) use crate::storage::serialization::vint::{
    encode_signed, encode_unsigned, unsigned_len,
};
pub(crate) use crate::storage::sstable::writer::index_writer::{
    PromotedIndexBlock, COLUMN_INDEX_SIZE_BYTES,
};
pub(crate) use crate::storage::sstable::writer::stats_writer::StatisticsMetadata;
pub(crate) use crate::storage::write_engine::mutation::{
    ClusteringBound, ClusteringKey, DecoratedKey, Mutation, PartitionTombstone, RangeTombstone,
};
pub(crate) use crate::types::{ComparatorType, UdtTypeDef, Value};
pub(crate) use std::io::Write;
pub(crate) use std::path::PathBuf;

// Row header flag constants (from V5CompressedLegacy parser)
const ROW_HAS_TIMESTAMP: u8 = 0x04;
const ROW_HAS_TTL: u8 = 0x08;
#[allow(dead_code)]
const ROW_HAS_DELETION: u8 = 0x10;
const ROW_HAS_ALL_COLUMNS: u8 = 0x20;
const ROW_HAS_COMPLEX_DELETION: u8 = 0x40;
const ROW_HAS_EXTENDED_FLAGS: u8 = 0x80;

// Extended flag constants (when ROW_HAS_EXTENDED_FLAGS is set)
const EXTENDED_IS_STATIC: u8 = 0x01;

// Cell flag constants (from V5CompressedLegacy parser)
const CELL_IS_DELETED: u8 = 0x01;
const CELL_IS_EXPIRING: u8 = 0x02;
const CELL_HAS_EMPTY_VALUE: u8 = 0x04;
const CELL_USE_ROW_TIMESTAMP: u8 = 0x08;
#[allow(dead_code)]
const CELL_USE_ROW_TTL: u8 = 0x10;

// Range tombstone marker constants
const IS_MARKER: u8 = 0x02;

// Range tombstone bound kinds.
//
// These are the ordinals of Cassandra's `ClusteringPrefix.Kind` enum
// (ClusteringPrefix.java) — the byte written on disk by
// `ClusteringBoundOrBoundary.Serializer.serialize()`:
//   0 = EXCL_END_BOUND, 1 = INCL_START_BOUND,
//   2 = EXCL_END_INCL_START_BOUNDARY, 3 = STATIC_CLUSTERING,
//   4 = CLUSTERING, 5 = INCL_END_EXCL_START_BOUNDARY,
//   6 = INCL_END_BOUND, 7 = EXCL_START_BOUND.
// (Issue #717: the writer previously used a private 0..5 numbering that no
// Cassandra reader understands.)
const EXCL_END_BOUND: u8 = 0;
const INCL_START_BOUND: u8 = 1;
const INCL_END_BOUND: u8 = 6;
const EXCL_START_BOUND: u8 = 7;

// Partition/row markers
const END_OF_PARTITION: u8 = 0x01;

/// Capacity of the streaming Data.db `BufWriter` (Issue #492).
///
/// Large enough that each flushed partition coalesces into a handful of big
/// `write()` syscalls instead of many small default-8 KB ones, preserving the
/// throughput of the previous single whole-file write while keeping resident
/// memory bounded (this buffer plus one partition's scratch).
const DATA_SINK_BUFFER_BYTES: usize = 1024 * 1024;

/// Data.db component writer
///
/// Writes partitions and rows in V5CompressedLegacy format with delta encoding.
/// Caller must provide partitions in token order and rows in clustering order.
///
/// # Memory model (Issue #492)
///
/// The writer supports two modes that produce **byte-identical** Data.db output:
///
/// * **In-memory mode** (`DataWriter::new`): every partition is appended to the
///   `buffer` scratch and never flushed, so `finish()` returns the full Data.db
///   bytes. Used by unit tests that inspect the produced bytes directly.
///
/// * **Streaming mode** (`DataWriter::with_sink`): each partition is built in the
///   `buffer` scratch, written to a `BufWriter<File>` over the Data.db path, and
///   the scratch is cleared. Peak heap is therefore `O(largest partition)` rather
///   than `O(file)`, keeping a multi-GB compaction within the 128 MB target.
///
/// In both modes the file offset of a partition is `position + buffer.len()`
/// measured before any bytes are written. In streaming mode `buffer` is empty at
/// that point (just cleared) so the offset is `position`; in memory mode
/// `position` is always 0 and `buffer` holds all prior partitions, so the offset
/// equals the legacy `buffer.len()`. The within-partition size math uses relative
/// deltas into `buffer`, which are identical regardless of mode.
#[derive(Debug)]
pub struct DataWriter {
    /// Per-partition scratch buffer for Data.db content.
    ///
    /// In streaming mode this is cleared at the start of every `write_partition`
    /// and flushed to `sink` at the end, so only one partition is resident.
    /// In memory mode it accumulates the entire Data.db output.
    buffer: Vec<u8>,
    /// Streaming sink over the Data.db path (streaming mode only).
    ///
    /// Lazily opened on the first `write_partition` so that the keyspace/table
    /// directory exists before the first byte is written. `None` in in-memory
    /// mode.
    sink: Option<std::io::BufWriter<std::fs::File>>,
    /// Data.db output path (streaming mode only); used for lazy sink open.
    data_path: Option<PathBuf>,
    /// Bytes already flushed to `sink`. Always 0 in in-memory mode.
    position: u64,
    /// Statistics metadata for delta encoding
    stats: StatisticsMetadata,
}

mod cells;
mod complex;
mod encoding;
mod index_prefix;
mod partition;
mod rows;
mod schema_helpers;
mod static_rows;
mod types;
mod udt_canon;

#[cfg(test)]
mod tests;

// Re-export the public surface so external paths
// (`...::data_writer::PartitionEmitCounts`, etc.) resolve unchanged after
// the responsibility split (issue #1118).
pub use types::PartitionEmitCounts;

// Crate-internal re-exports so the concern submodules can reach the shared
// writer types and serialization/schema helpers via `use super::*`, and so the
// few items used from outside this module (`op_cell_local_deletion_time`,
// `normalize_schema_udts`, `resolve_bare_udt_marshal`) keep their
// `...::data_writer::<name>` paths. Flag/marker constants stay private items of
// this `mod.rs` and reach the submodules as ancestor privates.
pub(crate) use encoding::*;
pub(crate) use index_prefix::*;
pub(crate) use schema_helpers::*;
pub(crate) use types::*;
pub(crate) use udt_canon::{canonicalize_static_value, canonicalize_udt_value};

impl DataWriter {
    /// Create a new in-memory Data.db writer.
    ///
    /// All partitions accumulate in `buffer`; `finish()` returns the full bytes.
    /// Prefer [`DataWriter::with_sink`] for production writes to bound memory.
    ///
    /// # Arguments
    /// * `stats` - Statistics metadata for delta encoding baselines
    pub fn new(stats: StatisticsMetadata) -> Self {
        Self {
            buffer: Vec::new(),
            sink: None,
            data_path: None,
            position: 0,
            stats,
        }
    }

    /// Create a streaming Data.db writer that flushes each partition to `data_path`.
    ///
    /// The file is opened lazily on the first `write_partition` (creating the
    /// parent directory if needed) so the keyspace/table layout is established
    /// before any bytes are written. Memory is bounded to the largest single
    /// partition.
    ///
    /// # Arguments
    /// * `stats` - Statistics metadata for delta encoding baselines
    /// * `data_path` - Destination path for the Data.db component
    pub fn with_sink(stats: StatisticsMetadata, data_path: PathBuf) -> Self {
        Self {
            buffer: Vec::new(),
            sink: None,
            data_path: Some(data_path),
            position: 0,
            stats,
        }
    }

    /// Lazily open the streaming sink (and create the parent directory).
    ///
    /// No-op in in-memory mode or once the sink is already open.
    pub(super) fn ensure_sink(&mut self) -> Result<()> {
        if self.sink.is_some() {
            return Ok(());
        }
        if let Some(path) = self.data_path.clone() {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let file = std::fs::File::create(&path)?;
            // Use a large BufWriter so a partition's bytes coalesce into a few
            // big write() syscalls rather than many 8 KB-default ones, matching
            // the throughput of the old single whole-file write.
            self.sink = Some(std::io::BufWriter::with_capacity(
                DATA_SINK_BUFFER_BYTES,
                file,
            ));
        }
        Ok(())
    }

    /// In streaming mode, flush the current scratch buffer to the sink, advance
    /// `position`, and clear the scratch so only one partition is ever resident.
    /// No-op in in-memory mode (the scratch keeps accumulating).
    pub(super) fn flush_partition(&mut self) -> Result<()> {
        if self.data_path.is_none() {
            // In-memory mode: keep accumulating in `buffer`.
            return Ok(());
        }
        self.ensure_sink()?;
        if let Some(sink) = self.sink.as_mut() {
            sink.write_all(&self.buffer)?;
        }
        self.position += self.buffer.len() as u64;
        self.buffer.clear();
        Ok(())
    }

    /// Update the statistics metadata
    ///
    /// This should be called after computing stats from all mutations
    /// but before writing any partition data. The stats are used for
    /// delta encoding of timestamps, TTL, and local deletion times.
    pub fn update_stats(&mut self, stats: StatisticsMetadata) {
        self.stats = stats;
    }

    /// Finish writing and return the Data.db bytes (in-memory mode).
    ///
    /// Only valid for writers created via [`DataWriter::new`]. In streaming mode
    /// the bytes live on disk; use [`DataWriter::finish_streaming`] instead.
    pub fn finish(self) -> Result<Vec<u8>> {
        // Hard guard (not debug_assert!, which compiles out in release): on a
        // streaming writer the bytes live on disk and `buffer` is empty after each
        // partition flush, so returning it would silently yield a 0-byte Data.db.
        if self.data_path.is_some() {
            return Err(Error::InvalidInput(
                "DataWriter::finish() called on a streaming writer; use finish_streaming()"
                    .to_string(),
            ));
        }
        Ok(self.buffer)
    }

    /// Finish a streaming writer: flush the sink to disk and return the total
    /// number of Data.db bytes written (i.e. `data_size`).
    ///
    /// Any residual scratch (there is none in normal operation, since
    /// `write_partition` flushes per partition) is flushed first. Returns an
    /// error if the writer was created in in-memory mode.
    pub fn finish_streaming(mut self) -> Result<u64> {
        if self.data_path.is_none() {
            return Err(Error::InvalidInput(
                "finish_streaming() called on an in-memory DataWriter".to_string(),
            ));
        }
        // Flush any residual scratch (normally empty), then flush the sink so all
        // bytes reach the OS file (the subsequent Digest CRC re-read of the same
        // file sees them via the page cache). This matches the durability of the
        // previous `tokio::fs::write`, which did not fsync either.
        self.flush_partition()?;
        if let Some(mut sink) = self.sink.take() {
            sink.flush()?;
        }
        Ok(self.position)
    }

    /// Get current file position (for Index.db offset tracking).
    ///
    /// This is the total number of Data.db bytes produced so far: bytes already
    /// flushed to the sink (`position`) plus bytes currently buffered. Identical
    /// in both streaming and in-memory modes.
    pub fn position(&self) -> u64 {
        self.position + self.buffer.len() as u64
    }

    /// Length of the per-partition scratch buffer.
    ///
    /// In streaming mode this reflects only the most recently written partition
    /// (the scratch is cleared after each flush), which is the basis of the
    /// bounded-memory guarantee. Test-only accessor.
    #[cfg(test)]
    pub(crate) fn scratch_len(&self) -> usize {
        self.buffer.len()
    }

    /// Number of bytes already flushed to the streaming sink. Test-only accessor.
    #[cfg(test)]
    pub(crate) fn flushed_position(&self) -> u64 {
        self.position
    }
}
