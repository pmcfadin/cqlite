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
//! - Parser: `cqlite-core/src/storage/sstable/reader/parsing/row_decoder.rs`
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
pub(crate) use crate::storage::sstable::writer::crc_writer::StreamingCrc;
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
// The CLUSTERING ordinal (a regular row's clustering name). Used by
// `partition::sort_class` as the row's Kind tiebreak so co-located bounds order
// against rows exactly as Cassandra's comparator does (issue #1220).
const CLUSTERING: u8 = 4;

// Range tombstone BOUNDARY kinds (issue #1220). A boundary closes one range and
// opens the next at the SAME clustering point, carrying TWO deletion-time pairs
// (primary = end/close of the previous range, secondary = start/open of the next
// range). Cassandra emits these whenever two adjacent ranges share a boundary
// point with complementary inclusivity (`ClusteringBoundOrBoundary.Serializer`):
//   2 = EXCL_END_INCL_START_BOUNDARY (close exclusive, open inclusive),
//   5 = INCL_END_EXCL_START_BOUNDARY (close inclusive, open exclusive).
const EXCL_END_INCL_START_BOUNDARY: u8 = 2;
const INCL_END_EXCL_START_BOUNDARY: u8 = 5;

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
    /// Reusable scratch buffer for one row's serialized body (issue #1673, R2).
    ///
    /// Each row's body (timestamp/TTL/deletion/column-bitmap/cells, everything
    /// after the row_size VInt) is built into this buffer, then appended to
    /// `buffer`. `build_merged_row_body` / `build_static_row_body` `clear()` it
    /// at the start of every row — `clear()` retains capacity, so after warmup no
    /// per-row body allocation occurs (it previously allocated a fresh throwaway
    /// `Vec` per row). Distinct field from `buffer` so the two can be borrowed
    /// disjointly (`buffer.extend_from_slice(&row_scratch)`).
    row_scratch: Vec<u8>,
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
    /// Incremental whole-file (`Digest.crc32`) + per-chunk (`CRC.db`) checksum
    /// accumulator, fed every flushed byte in write order (issue #1663).
    ///
    /// Streaming mode only: [`flush_partition`](Self::flush_partition) feeds each
    /// flushed scratch buffer through it, so [`finish_streaming`](Self::finish_streaming)
    /// can return both checksums without re-reading the finished `Data.db`. Left
    /// untouched (empty) in in-memory mode, which never calls `finish_streaming`.
    crc: StreamingCrc,
    /// Encoding baselines used for delta encoding.
    stats: EncodingStatsBaselines,
    /// Issue #1741: emit the partition-level `DeletionTime` in the oa/`da`
    /// serialization (1-byte `0x80` LIVE sentinel; `markedForDeleteAt`(i64) +
    /// `localDeletionTime`(u32) when deleted) rather than the legacy na/`nb`
    /// layout (`localDeletionTime`(i32) + `markedForDeleteAt`(i64), LIVE encoded as
    /// `i32::MAX`+`i64::MIN`). `true` ONLY for `da` (BTI) SSTables, whose reader
    /// applies `hasUIntDeletionTime` (oa) decoding; a `da` file written with the
    /// legacy layout has its live-partition sentinel misread as a tombstone (which
    /// the read-side shadowing then treats as a partition delete). Default `false`
    /// preserves byte-identical `nb` output.
    oa_partition_deletion: bool,
    /// Schema-constant ordered column lists + per-column complexity, computed
    /// exactly once per writer (issue #1674, R3).
    ///
    /// The schema is fixed for a writer's lifetime, yet `regular_columns` /
    /// `static_columns` were re-filtered and re-sorted up to 3× per row, and the
    /// sort comparator (`column_order_key` → `is_complex_column`) allocated a
    /// lowercased `String` on every comparison — `O(R·C·log C)` allocations for R
    /// rows × C columns. This memoizes the ordered column INDICES (Cassandra
    /// serialization-header order: `(is_complex, name)` key) plus a per-column
    /// `is_complex` flag, so the per-row hot path reads cached data and
    /// `to_lowercase` never runs per row. Stored as indices (not `&Column`) to
    /// avoid entangling the long-lived cache with the per-call `schema` borrow;
    /// callers resolve `schema.columns[idx]` at use. Filled lazily on first use
    /// via [`OnceCell`](std::cell::OnceCell) (interior mutability keeps the
    /// existing `&self` accessors), which also guarantees single computation.
    column_cache: std::cell::OnceCell<OrderedCols>,
}

/// Schema-constant ordered column lists, memoized once per [`DataWriter`]
/// (issue #1674, R3). See [`DataWriter::column_cache`].
#[derive(Debug)]
pub(super) struct OrderedCols {
    /// Indices into `schema.columns` of the regular (non-PK/CK/static) columns,
    /// in Cassandra serialization-header order (`(is_complex, name)` key).
    pub(super) regular: Vec<usize>,
    /// Indices into `schema.columns` of the static columns, same order key.
    pub(super) static_: Vec<usize>,
    /// Per-column `is_complex_column` classification, parallel to
    /// `schema.columns` (one `to_lowercase` per column at cache-build time).
    pub(super) is_complex: Vec<bool>,
}

mod cells;
mod collection_order;
/// Schema-constant ordered column lists memoized once per writer (issue #1674,
/// R3). See [`column_cache`].
mod column_cache;
mod complex;
mod encoding;
/// Incremental partition-write entry point (issue #1668, stage 5c-iv, part
/// 1 — build + prove, not yet wired). See
/// [`incremental_partition::IncrementalPartitionWriter`].
mod incremental_partition;
mod index_prefix;
/// Incremental rows+markers interleave (issue #1668, stage 5c-iii). See
/// [`marker_merge::merge_rows_and_markers`].
mod marker_merge;
/// Declared-marshal comparators for FROZEN SORTED collection keys/elements
/// (epic #1116 split out of [`udt_canon`]). See
/// [`marshal_comparator::compare_for_marshal`].
mod marshal_comparator;
mod partition;
mod rows;
mod schema_helpers;
/// Incremental static-column last-write-wins tracker (issue #1668, stage
/// 5c-ii). See [`static_ops::StaticOpsTracker`].
mod static_ops;
mod static_rows;
/// Cross-call resumable incremental partition-write session (issue #1668,
/// stage 5c-iv part 3). See
/// [`streaming_partition::StreamingPartitionSession`].
mod streaming_partition;
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
pub(crate) use collection_order::compare_collection_elements;
pub(crate) use encoding::*;
pub(crate) use incremental_partition::IncrementalPartitionWriter;
pub(crate) use index_prefix::*;
pub(crate) use partition::PartitionItem;
pub(crate) use schema_helpers::*;
pub(crate) use static_ops::StaticOpsTracker;
pub(crate) use streaming_partition::StreamingPartitionSession;
pub(crate) use types::*;
pub(crate) use udt_canon::{canonicalize_static_value, canonicalize_udt_value};

/// Delta-encoding baselines needed by the Data.db row/cell serializers.
///
/// `StatisticsMetadata` also owns Statistics.db-only accumulators such as
/// estimated histograms. Keeping only these three baseline fields in the hot
/// DataWriter avoids cloning those larger accumulators on every partition.
#[derive(Debug, Clone, Copy)]
struct EncodingStatsBaselines {
    min_timestamp: i64,
    min_ttl: i32,
    min_local_deletion_time: i32,
}

impl From<&StatisticsMetadata> for EncodingStatsBaselines {
    fn from(stats: &StatisticsMetadata) -> Self {
        Self {
            min_timestamp: stats.min_timestamp,
            min_ttl: stats.min_ttl,
            min_local_deletion_time: stats.min_local_deletion_time,
        }
    }
}

/// Result of finishing a streaming [`DataWriter`] (issue #1663).
///
/// Carries the total Data.db byte size and the checksums accumulated during the
/// streaming write, so `SSTableWriter::finish` writes `Digest.crc32` and
/// `CRC.db` without re-reading the finished `Data.db`.
#[derive(Debug)]
pub struct StreamFinish {
    /// Total number of Data.db bytes written (the `data_size`).
    pub data_size: u64,
    /// Whole-file CRC32 over the raw Data.db bytes — the `Digest.crc32` value.
    pub digest_crc32: u32,
    /// One CRC32 per `CRC_CHUNK_SIZE` chunk of the raw Data.db bytes, in order —
    /// the `CRC.db` chunk values (pass to `crc_writer::assemble_crc_bytes`).
    pub chunk_crcs: Vec<u32>,
}

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
            row_scratch: Vec::new(),
            sink: None,
            data_path: None,
            position: 0,
            crc: StreamingCrc::new(),
            stats: EncodingStatsBaselines::from(&stats),
            oa_partition_deletion: false,
            column_cache: std::cell::OnceCell::new(),
        }
    }

    /// Issue #1741: select the oa/`da` partition-level `DeletionTime` serialization.
    /// Call with `true` when producing a `da` (BTI) SSTable; the default (`false`)
    /// keeps the legacy `nb` layout byte-identical.
    pub fn with_oa_partition_deletion(mut self, on: bool) -> Self {
        self.oa_partition_deletion = on;
        self
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
            row_scratch: Vec::new(),
            sink: None,
            data_path: Some(data_path),
            position: 0,
            crc: StreamingCrc::new(),
            stats: EncodingStatsBaselines::from(&stats),
            oa_partition_deletion: false,
            column_cache: std::cell::OnceCell::new(),
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
                // Create the keyspace/table tree. The flush durability barrier
                // fsyncs the full leaf→data-root chain unconditionally before
                // the WAL truncate, so this creation need not track which
                // ancestors it made (issue #1392).
                crate::storage::write_engine::durability::create_dir_all(parent)?;
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
        // Feed the just-written bytes through the incremental checksum accumulator
        // (issue #1663) so `finish_streaming` never re-reads the finished Data.db.
        // Done here (after the sink write, before the clear) so every byte written
        // to disk is checksummed exactly once, in write order — chunks straddle
        // partition boundaries just as the re-read oracle chunks the raw file.
        self.crc.update(&self.buffer);
        self.position += self.buffer.len() as u64;
        self.buffer.clear();
        Ok(())
    }

    /// Flush the current scratch `buffer` to the streaming sink MID-PARTITION
    /// (issue #2299), advancing `position` and clearing the scratch.
    ///
    /// Mechanically identical to [`Self::flush_partition`] (feed the CRC/`CRC.db`
    /// accumulator in write order, advance `position`, clear `buffer`), but named
    /// distinctly to document the caller's contract: it is safe to call between
    /// two whole promoted-index blocks of an IN-PROGRESS partition ONLY because the
    /// streaming session tracks every partition offset as flush-invariant absolute
    /// math (`writer.position() - partition_offset`), not as a `buffer`-relative
    /// index. Calling it mid-BLOCK would be equally correct for the on-disk bytes
    /// (they are append-only), but the session only calls it at block boundaries so
    /// its bounded-scratch guarantee is exactly one promoted-index block. No-op in
    /// in-memory mode (the scratch keeps accumulating, matching `flush_partition`).
    pub(super) fn flush_buffered_partition_scratch(&mut self) -> Result<()> {
        // Same body as `flush_partition`: the two differ only in intent/documentation.
        self.flush_partition()
    }

    /// Update the statistics metadata
    ///
    /// This should be called after computing stats from all mutations
    /// but before writing any partition data. The stats are used for
    /// delta encoding of timestamps, TTL, and local deletion times.
    pub fn update_stats(&mut self, stats: StatisticsMetadata) {
        self.update_stats_from_metadata(&stats);
    }

    /// Update only the Data.db encoding baselines from full Statistics metadata.
    ///
    /// The estimated histograms added for Statistics.db are intentionally not
    /// copied into `DataWriter`; they are unrelated to Data.db delta encoding.
    pub fn update_stats_from_metadata(&mut self, stats: &StatisticsMetadata) {
        self.stats = EncodingStatsBaselines::from(stats);
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
    /// byte size plus the checksums accumulated during the write (issue #1663).
    ///
    /// Any residual scratch (there is none in normal operation, since
    /// `write_partition` flushes per partition) is flushed first. Returns an
    /// error if the writer was created in in-memory mode.
    ///
    /// The returned [`StreamFinish`] carries the whole-file `Digest.crc32` value
    /// and the per-chunk `CRC.db` values that were accumulated as the data
    /// streamed to disk, so `SSTableWriter::finish` computes neither by
    /// re-reading the finished `Data.db`.
    pub fn finish_streaming(mut self) -> Result<StreamFinish> {
        if self.data_path.is_none() {
            return Err(Error::InvalidInput(
                "finish_streaming() called on an in-memory DataWriter".to_string(),
            ));
        }
        // Flush any residual scratch (normally empty), then flush the BufWriter
        // so all bytes reach the OS file, and fsync the file so its *contents*
        // are durable on the storage device (issue #1392). A plain `flush()`
        // only pushes bytes into the page cache; without the fsync a crash after
        // the WAL is truncated could lose the Data.db contents even though the
        // directory entry was persisted. This fsync (plus the per-component
        // fsyncs in `SSTableWriter::finish`) is the durability guarantee for
        // both the flush and compaction write paths.
        self.flush_partition()?;
        if let Some(mut sink) = self.sink.take() {
            sink.flush()?;
            sink.get_ref()
                .sync_all()
                .map_err(|e| Error::Storage(format!("Failed to fsync Data.db contents: {e}")))?;
        }
        // Finalize the incremental checksums accumulated in `flush_partition`
        // (issue #1663). `finalize` closes any trailing short chunk.
        let data_size = self.position;
        let (digest_crc32, chunk_crcs) = self.crc.finalize();
        Ok(StreamFinish {
            data_size,
            digest_crc32,
            chunk_crcs,
        })
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
