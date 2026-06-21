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

use crate::error::{Error, Result};
use crate::schema::{Column, CqlType, TableSchema};
use crate::storage::serialization::types::TypeSerializer;
use crate::storage::serialization::vint::{encode_signed, encode_unsigned, unsigned_len};
use crate::storage::sstable::writer::index_writer::{PromotedIndexBlock, COLUMN_INDEX_SIZE_BYTES};
use crate::storage::sstable::writer::stats_writer::StatisticsMetadata;
use crate::storage::write_engine::mutation::{
    ClusteringBound, ClusteringKey, DecoratedKey, Mutation, PartitionTombstone, RangeTombstone,
};
use crate::types::{ComparatorType, UdtTypeDef, Value};
use std::io::Write;
use std::path::PathBuf;

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
    fn ensure_sink(&mut self) -> Result<()> {
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
    fn flush_partition(&mut self) -> Result<()> {
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

    /// Write a complete partition (partition key + all rows)
    ///
    /// # Arguments
    /// * `key` - Decorated partition key (token + raw bytes)
    /// * `mutations` - All mutations for this partition (must be in clustering order)
    /// * `schema` - Table schema for column metadata
    /// * `partition_tombstone` - Optional partition-level tombstone
    /// * `range_tombstones` - Range tombstones for this partition (must be in clustering order)
    ///
    /// # Returns
    /// File offset where this partition starts (for Index.db)
    pub fn write_partition(
        &mut self,
        key: &DecoratedKey,
        mutations: &[Mutation],
        schema: &TableSchema,
        partition_tombstone: Option<&PartitionTombstone>,
        range_tombstones: &[RangeTombstone],
    ) -> Result<u64> {
        // File offset of this partition = bytes already flushed (`position`) plus
        // whatever is currently buffered. In streaming mode `buffer` is empty at
        // the start of each partition (flushed + cleared by the previous call),
        // so this is `position`; in in-memory mode `position` is 0 and `buffer`
        // holds all prior partitions, matching the legacy `buffer.len()`.
        let partition_offset = self.position + self.buffer.len() as u64;

        // Write partition header (with optional tombstone)
        let header_start = self.buffer.len();
        self.write_partition_header(key, partition_tombstone)?;
        let mut prev_unfiltered_size = (self.buffer.len() - header_start) as u64;

        // SSTables must be internally reconciled: Cassandra's read path and
        // compaction only reconcile rows against deletions from OTHER sources
        // (memtables / other sstables) — a row shadowed by a partition or
        // range tombstone in the SAME sstable is served live. Cassandra's own
        // flush drops shadowed data, and so must we (Issue #716/#717).
        // `partition_floor` is the shadow timestamp from the partition
        // tombstone; per-row floors additionally account for covering range
        // tombstones.
        let partition_floor = partition_tombstone.map(|pt| pt.deletion_time);

        // Cassandra's SerializationHeader.hasStatic() returns true whenever the schema
        // declares any static column — and both the writer and reader unconditionally
        // emit/consume a static-row prelude in that case.  We must do the same.
        let schema_has_static = schema.columns.iter().any(|c| c.is_static);

        if schema_has_static {
            // Collect static-column operations from ALL mutations in this partition,
            // regardless of whether the mutation also carries a clustering key.
            // Last-write-wins by timestamp_micros when the same column appears twice.
            // Static cells shadowed by the partition tombstone are dropped.
            let merged = collect_static_operations(mutations, schema, partition_floor);

            // Mutations shadowed by the partition tombstone cannot contribute
            // the static row's liveness timestamp or TTL either.
            let unshadowed_static = |m: &&Mutation| {
                partition_floor.is_none_or(|floor| m.timestamp_micros > floor)
                    && has_static_operation(m, schema)
            };

            if merged.is_empty() {
                // Schema declares statics but this partition writes none.
                // Cassandra still expects the prelude; emit the minimal empty form.
                //
                // Issue #821 (finding #2): a static row hard-codes
                // previousUnfilteredSize = 0 and does NOT become the "previous
                // unfiltered" for the chain. The running value (the partition
                // header size) is carried forward by ADDING the static row's
                // bytes, so the first regular row sees its own offset from the
                // partition start (verified against real Cassandra "nb"
                // SSTables: header + static_row_size).
                let static_size = self.write_empty_static_row(0, schema)? as u64;
                prev_unfiltered_size += static_size;
            } else {
                // Row-level liveness timestamp: the latest timestamp seen across
                // contributing mutations. `!merged.is_empty()` implies at least
                // one mutation contributed an unshadowed static op, so `.max()`
                // is guaranteed `Some`.
                let latest_ts = mutations
                    .iter()
                    .filter(unshadowed_static)
                    .map(|m| m.timestamp_micros)
                    .max()
                    .unwrap_or(mutations.first().map(|m| m.timestamp_micros).unwrap_or(0));

                // Pick the TTL from the mutation with the latest timestamp that
                // contributed a static op (mirrors Cassandra's last-write-wins).
                let ttl = mutations
                    .iter()
                    .filter(unshadowed_static)
                    .max_by_key(|m| m.timestamp_micros)
                    .and_then(|m| m.ttl_seconds);

                // Issue #764: pass the per-op merged static ops (each carrying its
                // own originating timestamp + local_deletion_time) so a surviving
                // older static delete keeps its own LDT instead of inheriting the
                // newest static mutation's value.
                //
                // Issue #821 (finding #2): hard-code prev_size = 0 for the static
                // row and carry the running chain value forward by adding the
                // static row's serialized bytes (see comment above).
                let (static_size, _static_cells) =
                    self.write_static_row_with_prev_size(&merged, latest_ts, ttl, schema, 0)?;
                prev_unfiltered_size += static_size as u64;
            }
        }

        // Merge all mutations sharing a clustering key into a single row each
        // (Issue #716/#717: writing one row per mutation produced duplicate
        // rows with equal clustering — e.g. an INSERT row plus a phantom
        // tombstone-carrier row — which is invalid in the OA format). Rows
        // shadowed by the partition tombstone or a covering range tombstone
        // are dropped during the merge.
        let rows = self.merge_clustering_rows(
            mutations,
            schema,
            schema_has_static,
            partition_floor,
            range_tombstones,
        );

        // Interleave merged rows with range tombstone bound markers in
        // clustering order. Cassandra requires every unfiltered (row or
        // marker) of a partition to appear in clustering order; with equal
        // clustering values, inclusive-start/exclusive-end bounds sort before
        // the row and inclusive-end/exclusive-start bounds sort after it
        // (ClusteringPrefix.Kind.comparedToClustering).
        enum PartitionItem<'a> {
            Row(RowWrite<'a>),
            Marker {
                bound: &'a ClusteringBound,
                is_open: bool,
                deletion_time: i64,
                local_deletion_time: i32,
            },
        }

        let mut items: Vec<PartitionItem> = rows.into_iter().map(PartitionItem::Row).collect();
        for rt in range_tombstones {
            items.push(PartitionItem::Marker {
                bound: &rt.start,
                is_open: true,
                deletion_time: rt.deletion_time,
                local_deletion_time: rt.local_deletion_time,
            });
            items.push(PartitionItem::Marker {
                bound: &rt.end,
                is_open: false,
                deletion_time: rt.deletion_time,
                local_deletion_time: rt.local_deletion_time,
            });
        }

        // Sort key: (partition position class, clustering values, bound weight).
        // class: -1 = before all rows (Bottom), 0 = positioned by clustering
        // values, 1 = after all rows (Top).
        fn sort_class<'a, 'b>(item: &'b PartitionItem<'a>) -> (i8, Option<&'b ClusteringKey>, i8) {
            match item {
                PartitionItem::Row(row) => (0, row.clustering_key, 0),
                PartitionItem::Marker { bound, is_open, .. } => match bound {
                    ClusteringBound::Inclusive(ck) => (0, Some(ck), if *is_open { -1 } else { 1 }),
                    ClusteringBound::Exclusive(ck) => (0, Some(ck), if *is_open { 1 } else { -1 }),
                    ClusteringBound::Bottom => (-1, None, 0),
                    ClusteringBound::Top => (1, None, 0),
                },
            }
        }
        items.sort_by(|a, b| {
            let (class_a, ck_a, weight_a) = sort_class(a);
            let (class_b, ck_b, weight_b) = sort_class(b);
            class_a
                .cmp(&class_b)
                .then_with(|| match (ck_a, ck_b) {
                    (Some(x), Some(y)) => x.compare(y, schema).unwrap_or_else(|_| x.cmp(y)),
                    _ => std::cmp::Ordering::Equal,
                })
                .then(weight_a.cmp(&weight_b))
        });

        for item in items {
            prev_unfiltered_size = match item {
                PartitionItem::Row(row) => {
                    let (bytes, _cells) =
                        self.write_merged_row_with_prev_size(&row, schema, prev_unfiltered_size)?;
                    bytes as u64
                }
                PartitionItem::Marker {
                    bound,
                    is_open,
                    deletion_time,
                    local_deletion_time,
                } => self.write_range_bound(
                    bound,
                    is_open,
                    deletion_time,
                    local_deletion_time,
                    schema,
                    prev_unfiltered_size,
                )? as u64,
            };
        }

        // Write end-of-partition marker
        self.buffer.push(END_OF_PARTITION);

        // Streaming mode: flush this partition to disk and clear the scratch so
        // only one partition is ever resident in memory. No-op in memory mode.
        self.flush_partition()?;

        Ok(partition_offset)
    }

    /// Write a complete partition and collect promoted index blocks for wide partitions.
    ///
    /// Same as [`write_partition`] but also tracks `IndexInfo` block boundaries
    /// for partitions whose uncompressed data exceeds `COLUMN_INDEX_SIZE_BYTES` (64 KiB).
    /// The returned `Vec<PromotedIndexBlock>` contains **one entry per 64 KiB block**;
    /// the caller (SSTableWriter) passes this to `IndexWriter::add_partition_with_promoted`.
    ///
    /// Block sampling rules (mirrors `BigFormatPartitionWriter`):
    /// - A new block is opened whenever the bytes written since the last boundary ≥ 64 KiB.
    /// - The last open block is closed when `END_OF_PARTITION` is reached.
    /// - Only rows with clustering keys contribute `firstName`/`lastName`; tables without
    ///   clustering keys produce blocks with empty prefix bytes (`[0x00]` = header-only VInt).
    /// - A promoted index is only meaningful when 2+ blocks result; the caller checks this.
    pub fn write_partition_with_index_blocks(
        &mut self,
        key: &DecoratedKey,
        mutations: &[Mutation],
        schema: &TableSchema,
        partition_tombstone: Option<&PartitionTombstone>,
        range_tombstones: &[RangeTombstone],
    ) -> Result<(u64, Vec<PromotedIndexBlock>, PartitionEmitCounts)> {
        // File offset of this partition
        let partition_offset = self.position + self.buffer.len() as u64;

        // Issue #851: tally the rows/cells actually emitted below so Statistics
        // is fed from the single source of truth (this emitter) instead of a
        // parallel re-derivation that kept diverging from Data.db.
        let mut emit = PartitionEmitCounts::default();

        // Note the absolute buffer position at the start of partition data.
        // For streaming mode this is always `self.position` (buffer is empty at start).
        // For in-memory mode this is `self.buffer.len()` (position == 0).
        let partition_buf_start = self.buffer.len();

        // Write partition header (with optional tombstone)
        let header_start = self.buffer.len();
        self.write_partition_header(key, partition_tombstone)?;
        let mut prev_unfiltered_size = (self.buffer.len() - header_start) as u64;

        let partition_floor = partition_tombstone.map(|pt| pt.deletion_time);
        let schema_has_static = schema.columns.iter().any(|c| c.is_static);

        if schema_has_static {
            let merged = collect_static_operations(mutations, schema, partition_floor);
            let unshadowed_static = |m: &&Mutation| {
                partition_floor.is_none_or(|floor| m.timestamp_micros > floor)
                    && has_static_operation(m, schema)
            };

            if merged.is_empty() {
                // Issue #821 (finding #2): static row hard-codes prev_size = 0 and
                // is skipped by the prev-size chain; carry the running value
                // forward by adding the static row's bytes (see the non-indexed
                // path in `write_partition` for the rationale + Cassandra anchor).
                let static_size = self.write_empty_static_row(0, schema)? as u64;
                prev_unfiltered_size += static_size;
            } else {
                let latest_ts = mutations
                    .iter()
                    .filter(unshadowed_static)
                    .map(|m| m.timestamp_micros)
                    .max()
                    .unwrap_or(mutations.first().map(|m| m.timestamp_micros).unwrap_or(0));

                let ttl = mutations
                    .iter()
                    .filter(unshadowed_static)
                    .max_by_key(|m| m.timestamp_micros)
                    .and_then(|m| m.ttl_seconds);

                // Issue #764: same per-op LDT preservation as the non-indexed path.
                // Issue #821 (finding #2): prev_size = 0 for the static row; chain
                // carries forward by adding the static row's serialized bytes.
                let (static_size, static_cells) =
                    self.write_static_row_with_prev_size(&merged, latest_ts, ttl, schema, 0)?;
                prev_unfiltered_size += static_size as u64;
                // A non-empty static prelude is one physical row. Issue #851
                // (review): count the static cells the writer ACTUALLY serialized
                // (returned by the write path), not `merged.len()` — a merged
                // static null write is skipped by `write_static_cells`, so it
                // must not be counted as an emitted column.
                emit.rows += 1;
                emit.columns += static_cells;
            }
        }

        let rows = self.merge_clustering_rows(
            mutations,
            schema,
            schema_has_static,
            partition_floor,
            range_tombstones,
        );

        enum PartitionItem<'a> {
            Row(RowWrite<'a>),
            Marker {
                bound: &'a ClusteringBound,
                is_open: bool,
                deletion_time: i64,
                local_deletion_time: i32,
            },
        }

        let mut items: Vec<PartitionItem> = rows.into_iter().map(PartitionItem::Row).collect();
        for rt in range_tombstones {
            items.push(PartitionItem::Marker {
                bound: &rt.start,
                is_open: true,
                deletion_time: rt.deletion_time,
                local_deletion_time: rt.local_deletion_time,
            });
            items.push(PartitionItem::Marker {
                bound: &rt.end,
                is_open: false,
                deletion_time: rt.deletion_time,
                local_deletion_time: rt.local_deletion_time,
            });
        }

        fn sort_class<'a, 'b>(item: &'b PartitionItem<'a>) -> (i8, Option<&'b ClusteringKey>, i8) {
            match item {
                PartitionItem::Row(row) => (0, row.clustering_key, 0),
                PartitionItem::Marker { bound, is_open, .. } => match bound {
                    ClusteringBound::Inclusive(ck) => (0, Some(ck), if *is_open { -1 } else { 1 }),
                    ClusteringBound::Exclusive(ck) => (0, Some(ck), if *is_open { 1 } else { -1 }),
                    ClusteringBound::Bottom => (-1, None, 0),
                    ClusteringBound::Top => (1, None, 0),
                },
            }
        }
        items.sort_by(|a, b| {
            let (class_a, ck_a, weight_a) = sort_class(a);
            let (class_b, ck_b, weight_b) = sort_class(b);
            class_a
                .cmp(&class_b)
                .then_with(|| match (ck_a, ck_b) {
                    (Some(x), Some(y)) => x.compare(y, schema).unwrap_or_else(|_| x.cmp(y)),
                    _ => std::cmp::Ordering::Equal,
                })
                .then(weight_a.cmp(&weight_b))
        });

        // ── Promoted index block tracking ────────────────────────────────
        // Mirrors Cassandra's BigFormatPartitionWriter:
        // - Start block at position 0 (immediately after partition header).
        // - Close block + open a new one whenever accumulated bytes ≥ 64 KiB.
        // - The last block is closed at END_OF_PARTITION.
        //
        // `block_start_buf_offset`: absolute position in `self.buffer` where
        //   the current block began (relative to `partition_buf_start`).
        // `current_block_first_ck`: serialized ClusteringPrefix of the first item
        //   in the current block (empty bytes → "header only" VInt 0x00 for no-CK tables).
        // `current_block_last_ck`: serialized ClusteringPrefix of the most-recent item.

        let mut blocks: Vec<PromotedIndexBlock> = Vec::new();
        let mut block_start_buf_offset = self.buffer.len() - partition_buf_start;
        let mut current_block_first_ck: Option<Vec<u8>> = None;
        let mut current_block_last_ck: Option<Vec<u8>> = None;
        // OSS50 byte-comparable separator (first unfiltered's clustering) for the
        // BTI Rows.db row-index trie (issue #910). `None` until the first item.
        let mut current_block_oss50: Option<Option<Vec<u8>>> = None;

        for item in items {
            // Clustering key bytes for this item (serialized as ClusteringPrefix).
            let ck_bytes: Vec<u8> = match &item {
                PartitionItem::Row(row) => {
                    if let Some(ck) = row.clustering_key {
                        serialize_clustering_prefix_to_vec(ck, schema)
                            .unwrap_or_else(|_| vec![0x00])
                    } else {
                        vec![0x00] // no clustering key — empty prefix header
                    }
                }
                PartitionItem::Marker { bound, .. } => match bound {
                    ClusteringBound::Inclusive(ck) | ClusteringBound::Exclusive(ck) => {
                        serialize_clustering_prefix_to_vec(ck, schema)
                            .unwrap_or_else(|_| vec![0x00])
                    }
                    ClusteringBound::Bottom | ClusteringBound::Top => vec![0x00],
                },
            };

            if current_block_first_ck.is_none() {
                current_block_first_ck = Some(ck_bytes.clone());
                // OSS50 byte-comparable separator from the first unfiltered's
                // clustering values (issue #910). Markers and no-CK rows yield
                // None (no row-index separator). Encoding errors degrade to None
                // — the partition then keeps a direct Data.db offset rather than
                // emitting a separator the reader cannot reconstruct.
                let ck_values: Option<Vec<Value>> = match &item {
                    PartitionItem::Row(row) => row
                        .clustering_key
                        .filter(|ck| !ck.columns.is_empty())
                        .map(|ck| ck.columns.iter().map(|(_, v)| v.clone()).collect()),
                    PartitionItem::Marker { .. } => None,
                };
                // Per-column clustering ORDER (ASC/DESC). For a DESC column the
                // OSS50 separator bytes must be the REVERSED byte-comparable form
                // (Cassandra `ReversedType`/`ByteSource.invert`: complement every
                // byte) so that descending value order maps to ascending byte
                // order — otherwise the strict-ascending separator check rejects
                // the trie and the wide partition silently falls back to a direct
                // Data.db offset (roborev MEDIUM, issue #910 follow-up).
                let is_reversed: Vec<bool> = schema
                    .clustering_keys
                    .iter()
                    .map(|c| c.order == crate::schema::ClusteringOrder::Desc)
                    .collect();
                current_block_oss50 = Some(ck_values.and_then(|vals| {
                    crate::storage::sstable::bti::encode_clustering_bound_oss50_with_order(
                        &vals,
                        &is_reversed,
                    )
                    .ok()
                }));
            }
            current_block_last_ck = Some(ck_bytes.clone());

            // Write the item
            prev_unfiltered_size = match item {
                PartitionItem::Row(row) => {
                    // One physical row. Issue #851 (review): count the cells the
                    // writer ACTUALLY serialized (returned by the write path),
                    // not `row.ops.len()` — `write_merged_cells` skips null-valued
                    // writes, so a row whose only write is null contributes a live
                    // row with zero columns, matching Data.db exactly.
                    emit.rows += 1;
                    let (bytes, cells) =
                        self.write_merged_row_with_prev_size(&row, schema, prev_unfiltered_size)?;
                    emit.columns += cells;
                    bytes as u64
                }
                PartitionItem::Marker {
                    bound,
                    is_open,
                    deletion_time,
                    local_deletion_time,
                } => self.write_range_bound(
                    bound,
                    is_open,
                    deletion_time,
                    local_deletion_time,
                    schema,
                    prev_unfiltered_size,
                )? as u64,
            };

            // Bytes written in the current block so far
            let current_buf_offset = self.buffer.len() - partition_buf_start;
            let block_bytes = (current_buf_offset - block_start_buf_offset) as u64;

            if block_bytes >= COLUMN_INDEX_SIZE_BYTES {
                // Close this block and start a new one
                blocks.push(PromotedIndexBlock {
                    first_name: current_block_first_ck.take().unwrap_or_else(|| vec![0x00]),
                    last_name: current_block_last_ck.take().unwrap_or_else(|| vec![0x00]),
                    offset: block_start_buf_offset as u64,
                    width: block_bytes,
                    oss50_separator: current_block_oss50.take().flatten(),
                });
                block_start_buf_offset = current_buf_offset;
                // first/last reset for next block
                current_block_first_ck = None;
                current_block_last_ck = None;
                current_block_oss50 = None;
            }
        }

        // Write end-of-partition marker
        self.buffer.push(END_OF_PARTITION);

        // Close the final block (if any items were written)
        if let (Some(first), Some(last)) = (current_block_first_ck, current_block_last_ck) {
            let current_buf_offset = self.buffer.len() - partition_buf_start;
            let block_bytes = (current_buf_offset - block_start_buf_offset) as u64;
            if block_bytes > 0 {
                blocks.push(PromotedIndexBlock {
                    first_name: first,
                    last_name: last,
                    offset: block_start_buf_offset as u64,
                    width: block_bytes,
                    oss50_separator: current_block_oss50.take().flatten(),
                });
            }
        }

        self.flush_partition()?;

        Ok((partition_offset, blocks, emit))
    }

    /// Write an empty static-row prelude.
    ///
    /// Required by Cassandra whenever the schema has any static column, even
    /// when this particular partition writes no static cells.
    ///
    /// Binary form:
    /// ```text
    /// [0x80]              ← row_flags = ROW_HAS_EXTENDED_FLAGS only
    /// [0x01]              ← extended_flags = EXTENDED_IS_STATIC
    /// [row_size: VUInt]   ← size of (prev_size VInt + bitmap)
    /// [prev_size: VUInt]
    /// [bitmap: VUInt]     ← all-missing bitmap: (1 << N) - 1 for N static cols
    ///                       (encoded via write_column_subset with empty present set)
    /// ```
    fn write_empty_static_row(&mut self, prev_size: u64, schema: &TableSchema) -> Result<usize> {
        let start_len = self.buffer.len();

        // flags = only HAS_EXTENDED_FLAGS; no timestamp, no TTL, no deletion,
        // no HAS_ALL_COLUMNS, no HAS_COMPLEX_DELETION.
        let flags: u8 = ROW_HAS_EXTENDED_FLAGS;
        self.buffer.push(flags);
        self.buffer.push(EXTENDED_IS_STATIC);

        // Build the row body: just prev_size VInt + column bitmap (all missing).
        let mut body = Vec::new();

        // Column bitmap: "all columns missing" for every static column.
        // write_column_subset with an empty present_set.
        let static_columns = self.static_columns(schema);
        let empty_present: std::collections::HashSet<&str> = std::collections::HashSet::new();
        self.write_column_subset(&mut body, &static_columns, &empty_present)?;

        let prev_size_vint_len = unsigned_len(prev_size);
        let row_body_size = prev_size_vint_len as u64 + body.len() as u64;

        let mut row_size_buf = Vec::new();
        encode_unsigned(row_body_size, &mut row_size_buf);
        self.buffer.extend_from_slice(&row_size_buf);

        encode_unsigned(prev_size, &mut self.buffer);
        self.buffer.extend_from_slice(&body);

        Ok(self.buffer.len() - start_len)
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

    /// Write partition header
    ///
    /// Format (V5CompressedLegacy / Cassandra BigFormat):
    /// ```text
    /// [key_length: u16 BE]           ← Partition key length (2-byte unsigned short)
    /// [key_bytes]                    ← Raw partition key bytes
    /// [local_deletion_time: i32 BE]  ← i32::MAX for LIVE (DeletionTime.LIVE)
    /// [deletion_timestamp: i64 BE]   ← i64::MIN for LIVE (DeletionTime.LIVE)
    /// ```
    ///
    /// Note: Cassandra uses `ByteBufferUtil.writeWithShortLength()` for the key,
    /// which is a 2-byte BE unsigned short. There is NO separate flags byte.
    /// DeletionTime.LIVE uses sentinel values (Integer.MAX_VALUE, Long.MIN_VALUE).
    fn write_partition_header(
        &mut self,
        key: &DecoratedKey,
        tombstone: Option<&PartitionTombstone>,
    ) -> Result<()> {
        // Partition key length (u16 BE, matching Cassandra's writeWithShortLength)
        if key.key.len() > 65535 {
            return Err(Error::InvalidInput(format!(
                "Partition key too large: {} bytes (max 65535)",
                key.key.len()
            )));
        }
        self.buffer
            .write_all(&(key.key.len() as u16).to_be_bytes())?;

        // Partition key bytes
        self.buffer.extend_from_slice(&key.key);

        // Partition deletion info
        if let Some(ts) = tombstone {
            // Local deletion time (i32 BE, in seconds)
            self.buffer
                .write_all(&ts.local_deletion_time.to_be_bytes())?;
            // Deletion timestamp (i64 BE, in microseconds)
            self.buffer.write_all(&ts.deletion_time.to_be_bytes())?;
        } else {
            // DeletionTime.LIVE: Cassandra uses (Integer.MAX_VALUE, Long.MIN_VALUE)
            self.buffer.write_all(&i32::MAX.to_be_bytes())?;
            self.buffer.write_all(&i64::MIN.to_be_bytes())?;
        }

        Ok(())
    }

    /// Write a single row
    ///
    /// This implements the V5CompressedLegacy row format with delta encoding.
    #[allow(dead_code)]
    fn write_row(&mut self, mutation: &Mutation, schema: &TableSchema) -> Result<()> {
        self.write_row_with_prev_size(mutation, schema, 0)?;
        Ok(())
    }

    /// Write a single mutation as one row. Thin adapter over the merged-row
    /// path so legacy callers (and unit tests) keep working.
    fn write_row_with_prev_size(
        &mut self,
        mutation: &Mutation,
        schema: &TableSchema,
        prev_size: u64,
    ) -> Result<usize> {
        match Self::merge_row_group(&[mutation], schema, false, None) {
            Some(row) => {
                let (bytes, _cells) =
                    self.write_merged_row_with_prev_size(&row, schema, prev_size)?;
                Ok(bytes)
            }
            // Nothing to write (e.g. a tombstone-carrier mutation with no ops)
            None => Ok(0),
        }
    }

    /// Group same-clustering mutations of a partition and merge each group
    /// into a single [`RowWrite`].
    ///
    /// Mutations must already be sorted by clustering key (the caller —
    /// `SSTableWriter::write_partition` — sorts them); grouping is by
    /// adjacency. Pure-static mutations are excluded (their cells live in the
    /// static-row prelude), and groups that merge to nothing (e.g. mutations
    /// that exist only to carry partition/range tombstones) produce no row.
    fn merge_clustering_rows<'a>(
        &self,
        mutations: &'a [Mutation],
        schema: &TableSchema,
        skip_static_ops: bool,
        partition_floor: Option<i64>,
        range_tombstones: &[RangeTombstone],
    ) -> Vec<RowWrite<'a>> {
        let row_mutations: Vec<&'a Mutation> = mutations
            .iter()
            .filter(|m| !is_static_row_mutation(m, schema))
            .collect();

        let mut rows = Vec::new();
        let mut start = 0;
        while start < row_mutations.len() {
            let mut end = start + 1;
            while end < row_mutations.len()
                && row_mutations[end].clustering_key == row_mutations[start].clustering_key
            {
                end += 1;
            }

            // Shadow floor for this row: partition tombstone plus any range
            // tombstone covering the group's clustering key.
            let clustering_key = row_mutations[start].clustering_key.as_ref();
            let mut shadow_floor = partition_floor;
            for rt in range_tombstones {
                if range_tombstone_covers(rt, clustering_key, schema) {
                    shadow_floor =
                        Some(shadow_floor.map_or(rt.deletion_time, |f| f.max(rt.deletion_time)));
                }
            }

            if let Some(row) = Self::merge_row_group(
                &row_mutations[start..end],
                schema,
                skip_static_ops,
                shadow_floor,
            ) {
                rows.push(row);
            }
            start = end;
        }
        rows
    }

    /// Merge a group of mutations sharing one clustering key into a single
    /// row, applying Cassandra reconciliation semantics at write time:
    ///
    /// - Row deletion: the newest `DeleteRow` wins; mutations at or before
    ///   the deletion timestamp are shadowed (`DeletionTime.deletes` uses
    ///   `timestamp <= markedForDeleteAt`).
    /// - Cells: last-write-wins per column by timestamp; a tombstone wins a
    ///   timestamp tie (Cassandra cell reconciliation).
    /// - Liveness: from the newest surviving mutation that writes cells, or
    ///   a pure primary-key insert (no ops and no tombstone payload). Pure
    ///   row tombstones carry NO liveness, matching Cassandra's serializer.
    ///
    /// Returns `None` when the group produces no row at all (e.g. a mutation
    /// that exists only to carry a partition or range tombstone, or a row
    /// fully shadowed by the partition/range tombstone `shadow_floor`).
    fn merge_row_group<'a>(
        group: &[&'a Mutation],
        schema: &TableSchema,
        skip_static_ops: bool,
        shadow_floor: Option<i64>,
    ) -> Option<RowWrite<'a>> {
        use crate::storage::write_engine::mutation::CellOperation;

        // Newest row deletion in the group (if any). A row deletion at or
        // before the shadow floor is redundant (the partition/range tombstone
        // already covers it) and is dropped.
        let mut row_deletion: Option<(i64, i32)> = None;
        for m in group {
            let has_delete_row = m
                .operations
                .iter()
                .any(|op| matches!(op, CellOperation::DeleteRow));
            if has_delete_row
                && shadow_floor.is_none_or(|floor| m.timestamp_micros > floor)
                && row_deletion.is_none_or(|(ts, _)| m.timestamp_micros >= ts)
            {
                // Issue #764: honor the mutation's explicit local_deletion_time.
                row_deletion = Some((m.timestamp_micros, m.effective_local_deletion_time()));
            }
        }
        // Cells and liveness are shadowed by the strongest covering deletion:
        // the row deletion or the partition/range tombstone floor.
        let deletion_ts = match (row_deletion.map(|(ts, _)| ts), shadow_floor) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        };

        // Per-column last-write-wins; tombstones win timestamp ties.
        let mut cells: std::collections::HashMap<&'a str, MergedOp<'a>> =
            std::collections::HashMap::new();
        // Epic #899 (Phase B): per-element complex ops are NOT deduped per column
        // (a column has many elements). Kept verbatim and emitted via
        // `write_complex_column_per_element`. Empty for every existing scenario.
        let mut complex_element_ops: Vec<MergedOp<'a>> = Vec::new();
        // Liveness: (timestamp, row-level TTL) of the newest contributing mutation
        let mut liveness: Option<(i64, Option<u32>)> = None;

        for m in group {
            // Shadowed entirely by the row deletion
            if deletion_ts.is_some_and(|dts| m.timestamp_micros <= dts) {
                continue;
            }

            let mut contributes_liveness = false;
            for op in &m.operations {
                let column = match op {
                    CellOperation::Write { column, .. }
                    | CellOperation::WriteWithTtl { column, .. }
                    | CellOperation::Delete { column } => column.as_str(),
                    // Epic #899 (Phase B): per-element complex ops keep all
                    // elements (no per-column dedup). A live element write
                    // contributes row liveness; a `ComplexDeletion` marker does
                    // not. Primary-key columns can never be complex, so no
                    // key-column skip is needed.
                    CellOperation::WriteComplexElement { value, .. } => {
                        if skip_static_ops && is_static_operation(op, schema) {
                            continue;
                        }
                        if value.is_some() {
                            contributes_liveness = true;
                        }
                        complex_element_ops.push(MergedOp {
                            op,
                            timestamp_micros: m.timestamp_micros,
                            row_ttl_seconds: m.ttl_seconds,
                            cell_local_deletion_time: m.effective_local_deletion_time(),
                        });
                        continue;
                    }
                    CellOperation::ComplexDeletion { .. } => {
                        if skip_static_ops && is_static_operation(op, schema) {
                            continue;
                        }
                        complex_element_ops.push(MergedOp {
                            op,
                            timestamp_micros: m.timestamp_micros,
                            row_ttl_seconds: m.ttl_seconds,
                            cell_local_deletion_time: m.effective_local_deletion_time(),
                        });
                        continue;
                    }
                    CellOperation::DeleteRow => continue,
                };
                if skip_static_ops && is_static_operation(op, schema) {
                    continue;
                }
                if matches!(
                    op,
                    CellOperation::Write { .. } | CellOperation::WriteWithTtl { .. }
                ) {
                    // A write — even of a primary-key column — means the row is
                    // live. This must be recorded BEFORE the key-column skip below,
                    // so a row whose only cells are clustering values (a pure
                    // primary-key row) keeps its liveness instead of vanishing.
                    contributes_liveness = true;
                }
                // Primary-key columns are encoded positionally (partition key +
                // clustering prefix), never as cells. The compaction path can
                // surface a clustering column as a Write op (#857) — drop it so the
                // writer doesn't emit a phantom cell that corrupts the row body for
                // strict readers.
                if is_primary_key_column(column, schema) {
                    continue;
                }

                let candidate = MergedOp {
                    op,
                    timestamp_micros: m.timestamp_micros,
                    row_ttl_seconds: m.ttl_seconds,
                    cell_local_deletion_time: m.effective_local_deletion_time(),
                };
                match cells.entry(column) {
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(candidate);
                    }
                    std::collections::hash_map::Entry::Occupied(mut entry) => {
                        let existing = entry.get();
                        let candidate_is_tombstone =
                            matches!(candidate.op, CellOperation::Delete { .. });
                        let wins = candidate.timestamp_micros > existing.timestamp_micros
                            || (candidate.timestamp_micros == existing.timestamp_micros
                                && (candidate_is_tombstone
                                    || !matches!(existing.op, CellOperation::Delete { .. })));
                        if wins {
                            entry.insert(candidate);
                        }
                    }
                }
            }

            // A mutation with no ops and no tombstone payload is a pure
            // primary-key insert: it creates row liveness but no cells.
            let pure_pk_insert = m.operations.is_empty()
                && m.partition_tombstone.is_none()
                && m.range_tombstones.is_empty();
            if (contributes_liveness || pure_pk_insert)
                && liveness.is_none_or(|(ts, _)| m.timestamp_micros >= ts)
            {
                liveness = Some((m.timestamp_micros, m.ttl_seconds));
            }
        }

        let ops: Vec<MergedOp<'a>> = cells.into_values().collect();
        if ops.is_empty()
            && complex_element_ops.is_empty()
            && row_deletion.is_none()
            && liveness.is_none()
        {
            return None;
        }

        Some(RowWrite {
            clustering_key: group[0].clustering_key.as_ref(),
            liveness_ts: liveness.map(|(ts, _)| ts),
            ttl_seconds: liveness.and_then(|(_, ttl)| ttl),
            row_deletion,
            ops,
            complex_element_ops,
        })
    }

    /// Write one merged row (flags + clustering prefix + sizes + body).
    /// Write a merged row and return `(bytes_written, cells_written)`.
    ///
    /// Issue #851 (review): `cells_written` is the count of cells physically
    /// serialized for this row (from `build_merged_row_body` →
    /// `write_merged_cells`), so the caller's emit tally equals Data.db. It is 0
    /// for pure row tombstones and for rows whose only writes are null-valued.
    fn write_merged_row_with_prev_size(
        &mut self,
        row: &RowWrite<'_>,
        schema: &TableSchema,
        prev_size: u64,
    ) -> Result<(usize, u64)> {
        use crate::storage::write_engine::mutation::CellOperation;

        let start_len = self.buffer.len();

        // Build row header flags
        let mut flags = 0u8;

        if row.row_deletion.is_some() {
            flags |= ROW_HAS_DELETION; // 0x10
        }
        if row.liveness_ts.is_some() {
            flags |= ROW_HAS_TIMESTAMP;
            if row.ttl_seconds.is_some() {
                flags |= ROW_HAS_TTL;
            }
        }

        // All columns present if there is no deletion, all surviving ops are
        // non-NULL writes, and they cover every regular column.
        if row.row_deletion.is_none() {
            let all_writes = row.ops.iter().all(|mop| {
                matches!(
                    mop.op,
                    CellOperation::Write { .. } | CellOperation::WriteWithTtl { .. }
                )
            });
            let has_nulls = row.ops.iter().any(|mop| match mop.op {
                CellOperation::Write { value, .. } | CellOperation::WriteWithTtl { value, .. } => {
                    matches!(value, Value::Null)
                }
                _ => false,
            });
            let regular_column_count = self.regular_columns(schema).len();
            if all_writes && !has_nulls && row.ops.len() == regular_column_count {
                flags |= ROW_HAS_ALL_COLUMNS;
            }
        }

        // Check if any operation targets a complex column (non-frozen collection)
        let has_complex = row.ops.iter().any(|mop| {
            let col_name = match mop.op {
                CellOperation::Write { column, .. }
                | CellOperation::WriteWithTtl { column, .. }
                | CellOperation::Delete { column } => Some(column.as_str()),
                _ => None,
            };
            col_name.is_some_and(|name| {
                schema
                    .columns
                    .iter()
                    .find(|c| c.name == name)
                    .map(|c| is_complex_column(&c.data_type))
                    .unwrap_or(false)
            })
        });
        if has_complex {
            flags |= ROW_HAS_COMPLEX_DELETION;
        }

        // Write row flags
        self.buffer.push(flags);

        // Write clustering prefix if present (before row_size)
        if let Some(clustering_key) = row.clustering_key {
            self.write_clustering_prefix(clustering_key, schema)?;
        }

        // Calculate row body size (everything after row_size VInt)
        let (row_body, cells_written) = self.build_merged_row_body(row, schema, flags)?;

        let prev_size_vint_len = unsigned_len(prev_size);

        // Write row_size (VInt) — Cassandra's serializedRowBodySize() includes
        // the prev_unfiltered_size VInt as part of the row body
        let row_body_size = prev_size_vint_len as u64 + row_body.len() as u64;
        let mut row_size_buf = Vec::new();
        encode_unsigned(row_body_size, &mut row_size_buf);
        self.buffer.extend_from_slice(&row_size_buf);

        // Write prev_unfiltered_size (VInt, inside the row body)
        encode_unsigned(prev_size, &mut self.buffer);

        // Write rest of row body
        self.buffer.extend_from_slice(&row_body);

        Ok((self.buffer.len() - start_len, cells_written))
    }

    /// Write a static row for the current partition
    ///
    /// Static rows contain STATIC column values at partition level.
    /// They use extended flags and have NO clustering prefix.
    ///
    /// # Arguments
    /// * `mutation` - Mutation containing static column values
    /// * `schema` - Table schema for column metadata
    ///
    /// # Binary Format
    /// ```text
    /// [row_flags: u8]        ← 0x80 | other_flags (always HAS_EXTENDED_FLAGS)
    /// [extended_flags: u8]   ← 0x01 (EXTENDED_IS_STATIC)
    /// [row_size: VInt]       ← Size of body after this
    /// [prev_size: VInt]      ← 0 or previous row size
    /// [timestamp: VInt]      ← If HAS_TIMESTAMP (delta)
    /// [ttl: VInt]            ← If HAS_TTL (delta)
    /// [deletion: 2 VInts]    ← If HAS_DELETION
    /// [column_bitmap]        ← If NOT HAS_ALL_COLUMNS
    /// [cell_data...]         ← Static column cells only
    /// ```
    pub fn write_static_row(&mut self, mutation: &Mutation, schema: &TableSchema) -> Result<()> {
        // Legacy/test entry point: derive per-op metadata from the single
        // mutation (each op inherits the mutation's timestamp + effective LDT).
        let static_ops: Vec<StaticMergedOp> = mutation
            .operations
            .iter()
            .map(|op| StaticMergedOp {
                op: op.clone(),
                timestamp_micros: mutation.timestamp_micros,
                cell_local_deletion_time: mutation.effective_local_deletion_time(),
            })
            .collect();
        self.write_static_row_with_prev_size(
            &static_ops,
            mutation.timestamp_micros,
            mutation.ttl_seconds,
            schema,
            0,
        )?;
        Ok(())
    }

    // (write_static_row_with_prev_size returns (bytes, cells); see Issue #851.)

    /// Write a static row from the merged static operations of a partition.
    ///
    /// Issue #764: each `StaticMergedOp` carries its own originating timestamp
    /// and local deletion time, so a surviving static-column delete from an
    /// older mutation keeps its own LDT instead of inheriting a single
    /// synthetic mutation-level value (which corrupted the unsigned-VInt delta
    /// when stats were seeded from that older delete's explicit lower LDT).
    ///
    /// Returns `(bytes_written, cells_written)` (Issue #851, review). The cell
    /// count is sourced from the physical static-cell write path so Statistics'
    /// column count matches Data.db (0 for a static row tombstone).
    fn write_static_row_with_prev_size(
        &mut self,
        static_ops: &[StaticMergedOp],
        liveness_ts: i64,
        ttl_seconds: Option<u32>,
        schema: &TableSchema,
        prev_size: u64,
    ) -> Result<(usize, u64)> {
        let start_len = self.buffer.len();

        // Build row header flags - always includes HAS_EXTENDED_FLAGS for static rows
        let mut flags = ROW_HAS_EXTENDED_FLAGS;

        // Check if this is a row tombstone (only reachable via the public
        // single-mutation entry point; `collect_static_operations` never emits
        // a DeleteRow into the merged set).
        let is_row_tombstone = static_ops.iter().any(|mop| {
            matches!(
                mop.op,
                crate::storage::write_engine::mutation::CellOperation::DeleteRow
            )
        });

        if is_row_tombstone {
            flags |= ROW_HAS_DELETION;
        }

        // Timestamp is always present for static rows
        flags |= ROW_HAS_TIMESTAMP;

        // TTL if present (not applicable to row tombstones)
        if !is_row_tombstone && ttl_seconds.is_some() {
            flags |= ROW_HAS_TTL;
        }

        // Check if all static columns are present
        if !is_row_tombstone {
            let all_writes = static_ops.iter().all(|mop| {
                matches!(
                    mop.op,
                    crate::storage::write_engine::mutation::CellOperation::Write { .. }
                        | crate::storage::write_engine::mutation::CellOperation::WriteWithTtl { .. }
                )
            });
            let has_nulls = static_ops.iter().any(|mop| match &mop.op {
                crate::storage::write_engine::mutation::CellOperation::Write { value, .. }
                | crate::storage::write_engine::mutation::CellOperation::WriteWithTtl {
                    value,
                    ..
                } => {
                    matches!(value, Value::Null)
                }
                _ => false,
            });
            // Count static columns only for static row
            let static_column_count = schema.columns.iter().filter(|c| c.is_static).count();

            if all_writes && !has_nulls && static_ops.len() == static_column_count {
                flags |= ROW_HAS_ALL_COLUMNS;
            }
        }

        // Write row flags
        self.buffer.push(flags);

        // Write extended flags - always EXTENDED_IS_STATIC for static rows
        self.buffer.push(EXTENDED_IS_STATIC);

        // NO clustering prefix for static rows (key difference from write_row)

        // Build row body
        let (row_body, cells_written) =
            self.build_static_row_body(static_ops, liveness_ts, ttl_seconds, schema, flags)?;

        let prev_size_vint_len = unsigned_len(prev_size);

        // Write row_size (VInt) — includes prev_unfiltered_size VInt + rest of body
        let row_body_size = prev_size_vint_len as u64 + row_body.len() as u64;
        let mut row_size_buf = Vec::new();
        encode_unsigned(row_body_size, &mut row_size_buf);
        self.buffer.extend_from_slice(&row_size_buf);

        // Write prev_unfiltered_size (VInt, inside the row body)
        encode_unsigned(prev_size, &mut self.buffer);

        // Write rest of row body
        self.buffer.extend_from_slice(&row_body);

        Ok((self.buffer.len() - start_len, cells_written))
    }

    /// Build static row body (everything after row_size VInt)
    ///
    /// Similar to build_row_body but only processes static columns.
    ///
    /// Returns the body bytes and the number of static cells (columns)
    /// physically written (Issue #851, review). A static row tombstone writes no
    /// cells (count 0); otherwise the count is sourced from `write_static_cells`.
    fn build_static_row_body(
        &self,
        static_ops: &[StaticMergedOp],
        liveness_ts: i64,
        ttl_seconds: Option<u32>,
        schema: &TableSchema,
        flags: u8,
    ) -> Result<(Vec<u8>, u64)> {
        let mut body = Vec::new();

        // Write timestamp delta (if HAS_TIMESTAMP)
        //
        // Fix #644 (S6): Cassandra writes UNSIGNED VInt for all temporal deltas.
        // SerializationHeader.java:167: out.writeUnsignedVInt(timestamp - stats.minTimestamp)
        if (flags & ROW_HAS_TIMESTAMP) != 0 {
            let timestamp_delta = (liveness_ts - self.stats.min_timestamp) as u64;
            encode_unsigned(timestamp_delta, &mut body);
        }

        // Write TTL delta (if HAS_TTL)
        //
        // Fix #644 (S6): Both TTL and LDT deltas are UNSIGNED VInt.
        // SerializationHeader.java:177: out.writeUnsignedVInt32(ttl - stats.minTTL)
        // SerializationHeader.java:172: out.writeUnsignedVInt32(ldt - stats.minLocalDeletionTime)
        if (flags & ROW_HAS_TTL) != 0 {
            if let Some(ttl) = ttl_seconds {
                let ttl_delta = ttl as i64 - self.stats.min_ttl as i64;
                if ttl_delta < 0 {
                    return Err(Error::InvalidInput(format!(
                        "TTL {} is less than min_ttl {}",
                        ttl, self.stats.min_ttl
                    )));
                }
                encode_unsigned(ttl_delta as u64, &mut body);

                let local_deletion_time = self.expiring_local_deletion_time(ttl)?;
                let ldt_delta =
                    (local_deletion_time as i64) - (self.stats.min_local_deletion_time as i64);
                if ldt_delta < 0 {
                    return Err(Error::InvalidInput(format!(
                        "Local deletion time {} is less than min_local_deletion_time {}",
                        local_deletion_time, self.stats.min_local_deletion_time
                    )));
                }
                encode_unsigned(ldt_delta as u64, &mut body);
            }
        }

        // Write deletion (if HAS_DELETION)
        if (flags & ROW_HAS_DELETION) != 0 {
            // Row tombstone: Cassandra canonical order (markedForDeleteAt first, then localDeletionTime)
            // Per SerializationHeader.writeDeletionTime(): writeTimestamp() then writeLocalDeletionTime()
            // Fix #644 (S6): both are UNSIGNED VInt.
            //
            // The DeleteRow op carries the deletion timestamp + explicit LDT
            // (Issue #764). Reachable only via the single-mutation entry point.
            let delete_op = static_ops.iter().find(|mop| {
                matches!(
                    mop.op,
                    crate::storage::write_engine::mutation::CellOperation::DeleteRow
                )
            });
            let (deletion_ts, local_deletion_time) = delete_op
                .map(|mop| (mop.timestamp_micros, mop.cell_local_deletion_time))
                .unwrap_or((liveness_ts, (liveness_ts / 1_000_000) as i32));
            let ts_delta = (deletion_ts - self.stats.min_timestamp) as u64;
            encode_unsigned(ts_delta, &mut body);

            let ldt_delta =
                local_deletion_time.wrapping_sub(self.stats.min_local_deletion_time) as u32;
            encode_unsigned(ldt_delta as u64, &mut body);

            // Issue #717: the columns subset is NOT optional for tombstone rows.
            // Cassandra's UnfilteredSerializer always reads it after the deletion
            // times whenever HAS_ALL_COLUMNS is unset; omitting it makes the
            // reader consume the next row's bytes as a subset bitmask
            // ("Invalid Columns subset bytes; too many bits set").
            if (flags & ROW_HAS_ALL_COLUMNS) == 0 {
                let static_columns = self.static_columns(schema);
                let empty_present: std::collections::HashSet<&str> =
                    std::collections::HashSet::new();
                self.write_column_subset(&mut body, &static_columns, &empty_present)?;
            }

            // No cells written for row tombstones
            return Ok((body, 0));
        }

        // Write column bitmap (if NOT HAS_ALL_COLUMNS)
        // For static rows, bitmap only covers static columns
        if (flags & ROW_HAS_ALL_COLUMNS) == 0 {
            self.write_static_column_bitmap(&mut body, static_ops, schema)?;
        }

        // Write cell data for static columns only
        let cells_written = self.write_static_cells(&mut body, static_ops, liveness_ts, schema)?;

        Ok((body, cells_written))
    }

    /// Write column bitmap for static columns only.
    ///
    /// Same Cassandra `Columns.Serializer.serializeSubset()` format as
    /// `write_column_bitmap()` but scoped to static columns.
    fn write_static_column_bitmap(
        &self,
        buf: &mut Vec<u8>,
        static_ops: &[StaticMergedOp],
        schema: &TableSchema,
    ) -> Result<()> {
        // Collect names of columns that are present (non-NULL writes + deletes)
        let present_columns: std::collections::HashSet<&str> = static_ops
            .iter()
            .filter_map(|mop| match &mop.op {
                crate::storage::write_engine::mutation::CellOperation::Write { column, value }
                | crate::storage::write_engine::mutation::CellOperation::WriteWithTtl {
                    column,
                    value,
                    ..
                } if !matches!(value, Value::Null) => Some(column.as_str()),
                crate::storage::write_engine::mutation::CellOperation::Delete { column } => {
                    Some(column.as_str())
                }
                _ => None,
            })
            .collect();

        let static_columns = self.static_columns(schema);
        self.write_column_subset(buf, &static_columns, &present_columns)
    }

    /// Write cells for static columns only.
    ///
    /// Issue #764: deletes use their ORIGINATING op's timestamp and local
    /// deletion time (carried in `StaticMergedOp`), not a single synthetic
    /// mutation-level value.
    ///
    /// Issue #851 (review): returns the number of static cells (columns)
    /// physically serialized — sourced from this loop, the only place that
    /// decides whether a static cell is emitted (null writes skipped; deletes
    /// and non-null writes written) — so Statistics cannot drift from Data.db.
    fn write_static_cells(
        &self,
        buf: &mut Vec<u8>,
        static_ops: &[StaticMergedOp],
        liveness_ts: i64,
        schema: &TableSchema,
    ) -> Result<u64> {
        // Get set of static column names for validation
        let static_column_names: std::collections::HashSet<_> = schema
            .columns
            .iter()
            .filter(|c| c.is_static)
            .map(|c| &c.name)
            .collect();

        let mut cells_written: u64 = 0;
        for mop in self.sorted_static_ops(static_ops, schema) {
            match &mop.op {
                crate::storage::write_engine::mutation::CellOperation::Write { column, value } => {
                    // Only write if it's a static column
                    if static_column_names.contains(column) && !matches!(value, Value::Null) {
                        cells_written += 1;
                        // Issue #764: mirror the regular-row path — only borrow the
                        // row liveness timestamp (CELL_USE_ROW_TIMESTAMP) when this
                        // op actually originated at that timestamp; otherwise write
                        // the cell's own timestamp so an older surviving static write
                        // is not promoted to a newer mutation's timestamp.
                        if mop.timestamp_micros == liveness_ts {
                            self.write_cell(buf, column, value, mop.timestamp_micros)?;
                        } else {
                            self.write_cell_explicit_ts(buf, column, value, mop.timestamp_micros)?;
                        }
                    }
                }
                crate::storage::write_engine::mutation::CellOperation::WriteWithTtl {
                    column,
                    value,
                    ttl_seconds,
                } => {
                    // Only write if it's a static column
                    if static_column_names.contains(column) && !matches!(value, Value::Null) {
                        cells_written += 1;
                        self.write_cell_with_ttl(
                            buf,
                            column,
                            value,
                            mop.timestamp_micros,
                            *ttl_seconds,
                        )?;
                    }
                }
                crate::storage::write_engine::mutation::CellOperation::Delete { column } => {
                    // Only process if it's a static column
                    if static_column_names.contains(column) {
                        cells_written += 1;
                        // Issue #764: honor the originating op's explicit LDT.
                        self.write_tombstone_cell(
                            buf,
                            column,
                            mop.timestamp_micros,
                            mop.cell_local_deletion_time,
                        )?;
                    }
                }
                crate::storage::write_engine::mutation::CellOperation::DeleteRow => {
                    // Row deletion handled at row level with HAS_DELETION flag
                }
                // Per-element complex ops (epic #899) are never collected into the
                // static-op set (collect_static_operations skips them); STATIC
                // complex columns are out of scope for the Phase B capability.
                crate::storage::write_engine::mutation::CellOperation::WriteComplexElement {
                    ..
                }
                | crate::storage::write_engine::mutation::CellOperation::ComplexDeletion {
                    ..
                } => {}
            }
        }

        Ok(cells_written)
    }

    /// Sort merged static ops into Cassandra static-column serialization order
    /// (simple columns before complex, then by name).
    fn sorted_static_ops<'a, 'b>(
        &self,
        ops: &'b [StaticMergedOp],
        schema: &'a TableSchema,
    ) -> Vec<&'b StaticMergedOp> {
        let columns = self.static_columns(schema);
        let column_order: std::collections::HashMap<&str, usize> = columns
            .iter()
            .enumerate()
            .map(|(idx, column)| (column.name.as_str(), idx))
            .collect();

        let mut sorted: Vec<&'b StaticMergedOp> = ops.iter().collect();
        sorted.sort_by_key(|mop| match &mop.op {
            crate::storage::write_engine::mutation::CellOperation::Write { column, .. }
            | crate::storage::write_engine::mutation::CellOperation::WriteWithTtl {
                column, ..
            }
            | crate::storage::write_engine::mutation::CellOperation::Delete { column }
            | crate::storage::write_engine::mutation::CellOperation::WriteComplexElement {
                column,
                ..
            }
            | crate::storage::write_engine::mutation::CellOperation::ComplexDeletion {
                column,
                ..
            } => column_order
                .get(column.as_str())
                .copied()
                .unwrap_or(usize::MAX - 1),
            crate::storage::write_engine::mutation::CellOperation::DeleteRow => usize::MAX,
        });
        sorted
    }

    /// Build row body (everything after row_size VInt)
    ///
    /// Returns the bytes for: timestamp, TTL, deletion, column bitmap, and cells.
    /// Build a row body from a single mutation (legacy/test entry point).
    /// Routes through the merged-row body builder.
    #[cfg(test)]
    fn build_row_body(
        &self,
        mutation: &Mutation,
        schema: &TableSchema,
        flags: u8,
    ) -> Result<Vec<u8>> {
        let row = Self::merge_row_group(&[mutation], schema, false, None).unwrap_or(RowWrite {
            clustering_key: mutation.clustering_key.as_ref(),
            liveness_ts: Some(mutation.timestamp_micros),
            ttl_seconds: mutation.ttl_seconds,
            row_deletion: None,
            ops: Vec::new(),
            complex_element_ops: Vec::new(),
        });
        let (body, _cells) = self.build_merged_row_body(&row, schema, flags)?;
        Ok(body)
    }

    /// Build a merged row body (everything after the row_size VInt, excluding
    /// the prev_unfiltered_size VInt written by the caller).
    ///
    /// Field order per Cassandra's `UnfilteredSerializer.serializeRowBody`:
    /// liveness timestamp, TTL + expiration LDT, row deletion, columns
    /// subset, then cells. Issue #717: the columns subset is written for
    /// EVERY row lacking HAS_ALL_COLUMNS — including row tombstones.
    ///
    /// Returns the serialized body bytes and the number of cells (columns)
    /// physically written (Issue #851, review): the count is sourced from
    /// `write_merged_cells`, the only place that decides whether a cell is
    /// emitted, so Statistics' column count cannot drift from Data.db.
    fn build_merged_row_body(
        &self,
        row: &RowWrite<'_>,
        schema: &TableSchema,
        flags: u8,
    ) -> Result<(Vec<u8>, u64)> {
        let mut body = Vec::new();

        // Write timestamp delta (if HAS_TIMESTAMP)
        //
        // Fix #644 (S6): Cassandra writes UNSIGNED VInt for all temporal deltas.
        // SerializationHeader.java:167: out.writeUnsignedVInt(timestamp - stats.minTimestamp)
        if (flags & ROW_HAS_TIMESTAMP) != 0 {
            let liveness_ts = row.liveness_ts.ok_or_else(|| {
                Error::InvalidInput(
                    "ROW_HAS_TIMESTAMP set but row has no liveness timestamp".to_string(),
                )
            })?;
            let timestamp_delta = (liveness_ts - self.stats.min_timestamp) as u64;
            encode_unsigned(timestamp_delta, &mut body);
        }

        // Write TTL delta (if HAS_TTL)
        //
        // Fix #644 (S6): Both TTL and LDT deltas are UNSIGNED VInt.
        // SerializationHeader.java:177: out.writeUnsignedVInt32(ttl - stats.minTTL)
        // SerializationHeader.java:172: out.writeUnsignedVInt32(ldt - stats.minLocalDeletionTime)
        if (flags & ROW_HAS_TTL) != 0 {
            if let Some(ttl) = row.ttl_seconds {
                let ttl_delta = ttl as i64 - self.stats.min_ttl as i64;
                if ttl_delta < 0 {
                    return Err(Error::InvalidInput(format!(
                        "TTL {} is less than min_ttl {}",
                        ttl, self.stats.min_ttl
                    )));
                }
                encode_unsigned(ttl_delta as u64, &mut body);

                let local_deletion_time = self.expiring_local_deletion_time(ttl)?;
                let ldt_delta =
                    (local_deletion_time as i64) - (self.stats.min_local_deletion_time as i64);
                if ldt_delta < 0 {
                    return Err(Error::InvalidInput(format!(
                        "Local deletion time {} is less than min_local_deletion_time {}",
                        local_deletion_time, self.stats.min_local_deletion_time
                    )));
                }
                encode_unsigned(ldt_delta as u64, &mut body);
            }
        }

        // Write deletion (if HAS_DELETION)
        if (flags & ROW_HAS_DELETION) != 0 {
            // Row tombstone: Cassandra canonical order (markedForDeleteAt first, then localDeletionTime)
            // Per SerializationHeader.writeDeletionTime(): writeTimestamp() then writeLocalDeletionTime()
            // Fix #644 (S6): both are UNSIGNED VInt.
            let (deletion_ts, local_deletion_time) = row.row_deletion.ok_or_else(|| {
                Error::InvalidInput("ROW_HAS_DELETION set but row has no deletion time".to_string())
            })?;
            let ts_delta = (deletion_ts - self.stats.min_timestamp) as u64;
            encode_unsigned(ts_delta, &mut body);

            let ldt_delta =
                local_deletion_time.wrapping_sub(self.stats.min_local_deletion_time) as u32;
            encode_unsigned(ldt_delta as u64, &mut body);
        }

        // Write column bitmap (if NOT HAS_ALL_COLUMNS).
        // Issue #717: this is written even for row tombstones — Cassandra's
        // deserializer reads the subset right after the deletion times.
        if (flags & ROW_HAS_ALL_COLUMNS) == 0 {
            self.write_merged_column_bitmap(&mut body, &row.ops, schema)?;
        }

        // Write cell data (none survive for pure row tombstones)
        let cells_written = self.write_merged_cells(&mut body, row, schema)?;

        Ok((body, cells_written))
    }

    /// Write clustering prefix
    ///
    /// Format:
    /// ```text
    /// [header: VInt]              ← 2 bits per clustering column (state)
    /// [value_1: type-specific]    ← Only if state is PRESENT (00)
    /// [value_2: type-specific]
    /// ...
    /// ```
    fn write_clustering_prefix(
        &mut self,
        clustering_key: &crate::storage::write_engine::mutation::ClusteringKey,
        schema: &TableSchema,
    ) -> Result<()> {
        // Build header: 2 bits per column
        // 00 = PRESENT, 01 = EMPTY, 10 = NULL, 11 = reserved
        let mut header = 0u64;
        for (i, (_, value)) in clustering_key.columns.iter().enumerate() {
            let state = match value {
                Value::Null => 2, // NULL
                _ => 0,           // PRESENT
            };
            header |= (state as u64) << (i * 2);
        }

        // Write header as VUInt
        encode_unsigned(header, &mut self.buffer);

        // Write values for PRESENT columns
        for (i, (_, value)) in clustering_key.columns.iter().enumerate() {
            if !matches!(value, Value::Null) {
                // Get clustering column definition
                if i >= schema.clustering_keys.len() {
                    return Err(Error::Schema(format!(
                        "Clustering key has more columns than schema: {} > {}",
                        i + 1,
                        schema.clustering_keys.len()
                    )));
                }
                let cluster_col = &schema.clustering_keys[i];
                let comparator = ComparatorType::from_data_type(&cluster_col.data_type)?;

                // Write value bytes (type-specific encoding)
                let value_bytes = serialize_value_for_clustering(value, &comparator)?;
                self.buffer.extend_from_slice(&value_bytes);
            }
        }

        Ok(())
    }

    /// Write column bitmap
    ///
    /// Cassandra `Columns.Serializer.serializeSubset()` format.
    ///
    /// For <64 regular columns (the common case), this writes a single
    /// unsigned VInt whose bits indicate **missing** columns:
    ///   - bit = 1 → column is MISSING (NULL / not written)
    ///   - bit = 0 → column is PRESENT
    ///   - bitmap = 0 means all columns present (this case is prevented by
    ///     the caller which sets `HAS_ALL_COLUMNS` instead).
    ///
    /// Only regular columns participate in the bitmap — partition key and
    /// clustering key columns are serialized elsewhere.
    #[cfg(test)]
    fn write_column_bitmap(
        &self,
        buf: &mut Vec<u8>,
        mutation: &Mutation,
        schema: &TableSchema,
    ) -> Result<()> {
        // Collect names of columns that are present (non-NULL writes + deletes).
        // Delete operations must be marked as present so the reader parses
        // the tombstone/complex-deletion bytes that write_cells() emits.
        let present_columns: std::collections::HashSet<&str> = mutation
            .operations
            .iter()
            .filter_map(|op| match op {
                crate::storage::write_engine::mutation::CellOperation::Write { column, value }
                | crate::storage::write_engine::mutation::CellOperation::WriteWithTtl {
                    column,
                    value,
                    ..
                } if !matches!(value, Value::Null) => Some(column.as_str()),
                crate::storage::write_engine::mutation::CellOperation::Delete { column } => {
                    Some(column.as_str())
                }
                _ => None,
            })
            .collect();

        let regular_columns = self.regular_columns(schema);
        self.write_column_subset(buf, &regular_columns, &present_columns)
    }

    /// Write the columns subset for a merged row's surviving operations.
    ///
    /// Same encoding as [`Self::write_column_bitmap`]; for a pure row
    /// tombstone the ops list is empty, producing the all-missing bitmask.
    fn write_merged_column_bitmap(
        &self,
        buf: &mut Vec<u8>,
        ops: &[MergedOp<'_>],
        schema: &TableSchema,
    ) -> Result<()> {
        use crate::storage::write_engine::mutation::CellOperation;

        let present_columns: std::collections::HashSet<&str> = ops
            .iter()
            .filter_map(|mop| match mop.op {
                CellOperation::Write { column, value }
                | CellOperation::WriteWithTtl { column, value, .. }
                    if !matches!(value, Value::Null) =>
                {
                    Some(column.as_str())
                }
                CellOperation::Delete { column } => Some(column.as_str()),
                _ => None,
            })
            .collect();

        let regular_columns = self.regular_columns(schema);
        self.write_column_subset(buf, &regular_columns, &present_columns)
    }

    /// Get regular (non-PK, non-CK, non-static) columns from schema.
    ///
    /// Cassandra's column bitmap only covers regular columns — partition key
    /// and clustering key columns are serialized separately in the partition
    /// header and clustering prefix. Within the regular set, simple columns
    /// sort before complex columns, then by name.
    fn regular_columns<'a>(&self, schema: &'a TableSchema) -> Vec<&'a Column> {
        self.ordered_columns(schema, |column| {
            !column.is_static
                && !schema.is_partition_key(&column.name)
                && !schema.is_clustering_key(&column.name)
        })
    }

    /// Get static columns from schema in Cassandra serialization-header order.
    fn static_columns<'a>(&self, schema: &'a TableSchema) -> Vec<&'a Column> {
        self.ordered_columns(schema, |column| column.is_static)
    }

    /// Write cells for this row
    ///
    /// Cells are written in alphabetical column name order to match Cassandra's
    /// `Columns` sorting (regular columns are sorted by name).
    /// Write the surviving cells of a merged row, in regular-column order.
    ///
    /// Cells whose timestamp matches the row liveness timestamp use
    /// USE_ROW_TIMESTAMP; cells merged in from other mutations (e.g. a later
    /// single-cell DELETE) carry an explicit timestamp delta.
    /// Write the surviving cells of a merged row and return the number of cells
    /// (columns) actually serialized.
    ///
    /// Issue #851 (review): Statistics' `totalColumnsSet` must equal the cells
    /// PHYSICALLY written to Data.db, not `row.ops.len()`. This loop is the sole
    /// place that decides whether a cell is emitted (null `Write`/`WriteWithTtl`
    /// ops are skipped; deletes and non-null writes are written), so we return
    /// the count from here — the caller threads it straight into the emit tally,
    /// making the column count impossible to drift from Data.db.
    fn write_merged_cells(
        &self,
        buf: &mut Vec<u8>,
        row: &RowWrite<'_>,
        schema: &TableSchema,
    ) -> Result<u64> {
        use crate::storage::write_engine::mutation::CellOperation;

        let mut cells_written: u64 = 0;
        for mop in self.sorted_merged_ops(&row.ops, schema) {
            match mop.op {
                CellOperation::Write { column, value } => {
                    // Skip NULL values - they are represented by absence in the bitmap
                    if matches!(value, Value::Null) {
                        continue;
                    }
                    cells_written += 1;
                    // Check if this column is a complex column (non-frozen collection)
                    let is_complex = schema
                        .columns
                        .iter()
                        .find(|c| c.name == *column)
                        .map(|c| is_complex_column(&c.data_type))
                        .unwrap_or(false);

                    if is_complex {
                        let col = schema
                            .columns
                            .iter()
                            .find(|c| c.name == *column)
                            .ok_or_else(|| {
                                Error::Schema(format!(
                                    "Complex column '{}' not found in schema",
                                    column
                                ))
                            })?;
                        self.write_complex_column(buf, col, value, mop.timestamp_micros, None)?;
                    } else if let Some(ttl_seconds) = mop.row_ttl_seconds {
                        if row.ttl_seconds == Some(ttl_seconds)
                            && row.liveness_ts == Some(mop.timestamp_micros)
                        {
                            self.write_cell_with_row_ttl(
                                buf,
                                column,
                                value,
                                mop.timestamp_micros,
                                ttl_seconds,
                            )?;
                        } else {
                            self.write_cell_with_ttl(
                                buf,
                                column,
                                value,
                                mop.timestamp_micros,
                                ttl_seconds,
                            )?;
                        }
                    } else if row.liveness_ts == Some(mop.timestamp_micros) {
                        self.write_cell(buf, column, value, mop.timestamp_micros)?;
                    } else {
                        self.write_cell_explicit_ts(buf, column, value, mop.timestamp_micros)?;
                    }
                }
                CellOperation::WriteWithTtl {
                    column,
                    value,
                    ttl_seconds,
                } => {
                    // Skip NULL values - they are represented by absence in the bitmap
                    if matches!(value, Value::Null) {
                        continue;
                    }
                    cells_written += 1;
                    let is_complex = schema
                        .columns
                        .iter()
                        .find(|c| c.name == *column)
                        .map(|c| is_complex_column(&c.data_type))
                        .unwrap_or(false);

                    if is_complex {
                        let col = schema
                            .columns
                            .iter()
                            .find(|c| c.name == *column)
                            .ok_or_else(|| {
                                Error::Schema(format!(
                                    "Complex column '{}' not found in schema",
                                    column
                                ))
                            })?;
                        self.write_complex_column(
                            buf,
                            col,
                            value,
                            mop.timestamp_micros,
                            Some(*ttl_seconds),
                        )?;
                    } else {
                        self.write_cell_with_ttl(
                            buf,
                            column,
                            value,
                            mop.timestamp_micros,
                            *ttl_seconds,
                        )?;
                    }
                }
                CellOperation::Delete { column } => {
                    cells_written += 1;
                    let is_complex = schema
                        .columns
                        .iter()
                        .find(|c| c.name == *column)
                        .map(|c| is_complex_column(&c.data_type))
                        .unwrap_or(false);

                    if is_complex {
                        // Complex column deletion: write empty complex column
                        // with active deletion time (not LIVE).
                        // Issue #764: honor the originating mutation's explicit
                        // local_deletion_time, not a timestamp-derived value.
                        self.write_complex_column_deletion(
                            buf,
                            mop.timestamp_micros,
                            mop.cell_local_deletion_time,
                        )?;
                    } else {
                        // Issue #764: honor explicit local_deletion_time.
                        let local_deletion_time = mop.cell_local_deletion_time;
                        self.write_tombstone_cell(
                            buf,
                            column,
                            mop.timestamp_micros,
                            local_deletion_time,
                        )?;
                    }
                }
                CellOperation::DeleteRow => {
                    // Row deletion handled at row level with HAS_DELETION flag
                }
                // Per-element complex ops are collected into
                // `row.complex_element_ops`, not `row.ops`, and emitted below.
                CellOperation::WriteComplexElement { .. }
                | CellOperation::ComplexDeletion { .. } => {}
            }
        }

        // Epic #899 (Phase B): emit per-element complex columns. Empty for every
        // existing scenario (the real pipeline does not yet route ops here), so
        // this is byte-neutral; exercised by the Phase B writer-capability unit
        // tests via `write_complex_column_per_element` directly.
        cells_written += self.write_complex_element_columns(buf, row, schema)?;

        Ok(cells_written)
    }

    /// Group `row.complex_element_ops` by column and emit each as a per-element
    /// complex column (real deletion marker + surviving element cells), in
    /// regular-column serialization order. Returns the number of complex columns
    /// physically written (one per emitted column, matching Cassandra's
    /// `Row.columnCount()` for non-frozen collections).
    fn write_complex_element_columns(
        &self,
        buf: &mut Vec<u8>,
        row: &RowWrite<'_>,
        schema: &TableSchema,
    ) -> Result<u64> {
        use crate::storage::write_engine::mutation::CellOperation;

        if row.complex_element_ops.is_empty() {
            return Ok(0);
        }

        // Group by column, preserving per-element ops and the (single) deletion.
        // BTreeMap keeps a deterministic intermediate order; final emit order is
        // the schema's regular-column order below.
        let mut per_column: std::collections::BTreeMap<&str, ComplexColumnGroup> =
            std::collections::BTreeMap::new();

        for mop in &row.complex_element_ops {
            match mop.op {
                CellOperation::WriteComplexElement {
                    column,
                    cell_path,
                    value,
                    timestamp_micros,
                    ttl_seconds,
                    local_deletion_time,
                } => {
                    let entry = per_column.entry(column.as_str()).or_default();
                    entry.1.push(ComplexElementWrite {
                        cell_path: cell_path.clone(),
                        value: value.clone(),
                        timestamp_micros: *timestamp_micros,
                        ttl_seconds: *ttl_seconds,
                        local_deletion_time: *local_deletion_time,
                        is_deleted: value.is_none() && local_deletion_time.is_some(),
                    });
                }
                CellOperation::ComplexDeletion {
                    column,
                    marked_for_delete_at,
                    local_deletion_time,
                } => {
                    let entry = per_column.entry(column.as_str()).or_default();
                    // Keep the strongest (highest markedForDeleteAt) marker.
                    let candidate = (*marked_for_delete_at, *local_deletion_time);
                    entry.0 = Some(match entry.0 {
                        Some(existing) if existing.0 >= candidate.0 => existing,
                        _ => candidate,
                    });
                }
                _ => {}
            }
        }

        // Emit in schema regular-column order so complex columns land in the same
        // position the bitmap/`sorted_merged_ops` use.
        let mut cells_written: u64 = 0;
        for col in self.regular_columns(schema) {
            if let Some((complex_deletion, elements)) = per_column.remove(col.name.as_str()) {
                self.write_complex_column_per_element(
                    buf,
                    col,
                    complex_deletion,
                    &elements,
                    row.liveness_ts.unwrap_or(0),
                )?;
                cells_written += 1;
            }
        }

        Ok(cells_written)
    }

    /// Write a complex column (non-frozen collection stored as multiple cells).
    ///
    /// Complex columns use the following wire format:
    /// ```text
    /// [complex_deletion: marked_for_delete_at (signed VInt) + local_deletion_time (unsigned VInt)]
    /// [cell_count: unsigned VInt]
    /// For each cell:
    ///   [flags: u8]
    ///   [cell_path_length: unsigned VInt]
    ///   [cell_path_bytes]
    ///   [value_length: unsigned VInt]  (if not HAS_EMPTY_VALUE)
    ///   [value_bytes]
    /// ```
    ///
    /// Per collection type:
    /// - SET<T>: cell_path = serialized element, value = empty (HAS_EMPTY_VALUE)
    /// - MAP<K,V>: cell_path = serialized key, value = serialized value
    /// - LIST<T>: cell_path = 16-byte TimeUUID, value = serialized element
    fn write_complex_column(
        &self,
        buf: &mut Vec<u8>,
        column: &Column,
        value: &Value,
        timestamp_micros: i64,
        ttl_seconds: Option<u32>,
    ) -> Result<()> {
        // Write complex deletion time: DeletionTime.LIVE
        // Cassandra canonical order: markedForDeleteAt first, then localDeletionTime
        // Per SerializationHeader.writeDeletionTime(): writeTimestamp() then writeLocalDeletionTime()
        // Fix #644 (S6): markedForDeleteAt delta is UNSIGNED VInt.
        // DeletionTime.LIVE.markedForDeleteAt = Long.MIN_VALUE; delta wraps to large positive u64.
        let ts_delta = i64::MIN.wrapping_sub(self.stats.min_timestamp) as u64;
        encode_unsigned(ts_delta, buf);
        // localDeletionTime delta = Integer.MAX_VALUE - stats.min_local_deletion_time (unsigned VInt)
        let ldt_delta = i32::MAX.wrapping_sub(self.stats.min_local_deletion_time) as u32;
        encode_unsigned(ldt_delta as u64, buf);

        let dt = column.data_type.to_lowercase();

        if dt.starts_with("set<") || dt.starts_with("org.apache.cassandra.db.marshal.settype(") {
            self.write_set_complex_cells(buf, value, timestamp_micros, ttl_seconds)?;
        } else if dt.starts_with("map<")
            || dt.starts_with("org.apache.cassandra.db.marshal.maptype(")
        {
            self.write_map_complex_cells(buf, value, timestamp_micros, ttl_seconds)?;
        } else if dt.starts_with("list<")
            || dt.starts_with("org.apache.cassandra.db.marshal.listtype(")
        {
            self.write_list_complex_cells(buf, value, timestamp_micros, ttl_seconds)?;
        } else {
            return Err(Error::InvalidInput(format!(
                "Column '{}' has type '{}' which is not a recognized complex column type",
                column.name, column.data_type
            )));
        }

        Ok(())
    }

    /// Write a complex column deletion (delete all elements of a collection).
    ///
    /// Wire format: active deletion time + zero cells.
    /// Per SerializationHeader.writeDeletionTime(): timestamp first, LDT second.
    /// ```text
    /// [marked_for_delete_at: unsigned VInt]  ← mutation timestamp (delta from min)
    /// [local_deletion_time: unsigned VInt]   ← seconds since epoch (delta from min)
    /// [cell_count: unsigned VInt]            ← 0 (no cells)
    /// ```
    fn write_complex_column_deletion(
        &self,
        buf: &mut Vec<u8>,
        timestamp_micros: i64,
        local_deletion_time: i32,
    ) -> Result<()> {
        // Active deletion: Cassandra canonical order (markedForDeleteAt first, then localDeletionTime)
        // Per SerializationHeader.writeDeletionTime(): writeTimestamp() then writeLocalDeletionTime()
        // Fix #644 (S6): marked_for_delete_at delta is UNSIGNED VInt.
        let ts_delta = (timestamp_micros - self.stats.min_timestamp) as u64;
        encode_unsigned(ts_delta, buf);

        // Issue #853: encode the localDeletionTime delta with the SAME i32 cast +
        // wrapping behaviour that Cassandra's DeletionTime.serialize uses (and that
        // the row-deletion / range-bound paths already use), so the encoded SIZE of
        // this complex-deletion marker equals the bytes actually written for
        // far-future localDeletionTime in [2^31, 2^32) (~year 2038-2106).
        //
        // Cassandra (c81fbae1): localDeletionTime and minLocalDeletionTime are Java
        // `int`s; the wire delta is `writeUnsignedVInt32(localDeletionTime -
        // minLocalDeletionTime)`, a 32-bit subtraction whose result is zero-extended
        // into [0, 2^32). A value in [2^31, 2^32) is a negative i32 here; widening to
        // i64 first (the previous code) both rejected it and would have produced a
        // different byte count than the i32 form, corrupting the row-size vint.
        //
        // Issue #764: still reject a genuine below-baseline ordering violation, but
        // only in normal (non-negative i32) time space; a far-future LDT (negative
        // as i32) is a legitimate value, not corruption.
        if local_deletion_time >= 0
            && self.stats.min_local_deletion_time >= 0
            && local_deletion_time < self.stats.min_local_deletion_time
        {
            return Err(Error::InvalidInput(format!(
                "Complex deletion: local deletion time {} is less than min_local_deletion_time {}",
                local_deletion_time, self.stats.min_local_deletion_time
            )));
        }
        let deletion_time_delta =
            local_deletion_time.wrapping_sub(self.stats.min_local_deletion_time) as u32;
        encode_unsigned(deletion_time_delta as u64, buf);

        // Zero cells
        encode_unsigned(0u64, buf);

        Ok(())
    }

    /// Write per-cell TTL fields for a complex cell.
    ///
    /// When TTL is present, writes:
    /// - flags: CELL_IS_EXPIRING (0x02), NO USE_ROW_TIMESTAMP
    /// - timestamp delta (unsigned VInt; fix #644: all temporal deltas are unsigned)
    /// - local_deletion_time delta (unsigned VInt)
    /// - TTL delta (unsigned VInt)
    ///
    /// When TTL is absent, writes:
    /// - flags: base_flags | CELL_USE_ROW_TIMESTAMP (0x08)
    ///
    /// Returns the flags byte written (for caller to check HAS_EMPTY_VALUE etc.).
    fn write_complex_cell_header(
        &self,
        buf: &mut Vec<u8>,
        base_flags: u8,
        timestamp_micros: i64,
        ttl_seconds: Option<u32>,
    ) -> Result<()> {
        match ttl_seconds {
            Some(ttl) => {
                // Expiring cell: IS_EXPIRING flag, explicit timestamp + LDT + TTL
                let flags = base_flags | CELL_IS_EXPIRING;
                buf.push(flags);

                // Timestamp delta (UNSIGNED VInt, NOT USE_ROW_TIMESTAMP)
                // Fix #644 (S6): SerializationHeader.java:167 uses writeUnsignedVInt.
                let timestamp_delta = (timestamp_micros - self.stats.min_timestamp) as u64;
                encode_unsigned(timestamp_delta, buf);

                // local_deletion_time = now + ttl
                let now_seconds = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(|e| Error::Storage(format!("System time error: {}", e)))?
                    .as_secs() as i32;
                let local_deletion_time = now_seconds.saturating_add(ttl as i32);
                let ldt_delta =
                    (local_deletion_time as i64) - (self.stats.min_local_deletion_time as i64);
                if ldt_delta < 0 {
                    return Err(Error::InvalidInput(format!(
                        "Complex cell: local deletion time {} is less than min_local_deletion_time {}",
                        local_deletion_time, self.stats.min_local_deletion_time
                    )));
                }
                encode_unsigned(ldt_delta as u64, buf);

                // TTL delta
                let ttl_delta = (ttl as i64) - (self.stats.min_ttl as i64);
                if ttl_delta < 0 {
                    return Err(Error::InvalidInput(format!(
                        "Complex cell: TTL {} is less than min_ttl {}",
                        ttl, self.stats.min_ttl
                    )));
                }
                encode_unsigned(ttl_delta as u64, buf);
            }
            None => {
                // Non-expiring cell: use row timestamp
                buf.push(base_flags | CELL_USE_ROW_TIMESTAMP);
            }
        }
        Ok(())
    }

    /// Write SET complex cells.
    ///
    /// SET elements: cell_path = serialized element value, cell value = empty (HAS_EMPTY_VALUE).
    /// Elements are sorted by their serialized byte representation for Cassandra compatibility.
    fn write_set_complex_cells(
        &self,
        buf: &mut Vec<u8>,
        value: &Value,
        timestamp_micros: i64,
        ttl_seconds: Option<u32>,
    ) -> Result<()> {
        let elements = match value {
            Value::Set(elements) => elements,
            _ => {
                return Err(Error::InvalidInput(format!(
                    "Expected Set value for complex SET column, got {:?}",
                    value
                )))
            }
        };

        // Serialize all elements first, then sort by byte representation.
        // serialize_value rejects Value::Null, enforcing CQL semantics.
        let mut serialized: Vec<Vec<u8>> = elements
            .iter()
            .map(|e| serialize_collection_element(e, "SET"))
            .collect::<Result<Vec<_>>>()?;
        serialized.sort();

        // Cell count
        encode_unsigned(serialized.len() as u64, buf);

        for path_bytes in &serialized {
            // Cell header: flags + optional TTL fields
            self.write_complex_cell_header(
                buf,
                CELL_HAS_EMPTY_VALUE,
                timestamp_micros,
                ttl_seconds,
            )?;

            // Cell path: serialized element value
            encode_unsigned(path_bytes.len() as u64, buf);
            buf.extend_from_slice(path_bytes);

            // No value bytes (HAS_EMPTY_VALUE flag set)
        }

        Ok(())
    }

    /// Write MAP complex cells.
    ///
    /// MAP entries: cell_path = serialized key, cell value = serialized value.
    /// Entries are sorted by their serialized key byte representation for Cassandra compatibility.
    fn write_map_complex_cells(
        &self,
        buf: &mut Vec<u8>,
        value: &Value,
        timestamp_micros: i64,
        ttl_seconds: Option<u32>,
    ) -> Result<()> {
        let entries = match value {
            Value::Map(entries) => entries,
            _ => {
                return Err(Error::InvalidInput(format!(
                    "Expected Map value for complex MAP column, got {:?}",
                    value
                )))
            }
        };

        // Serialize all keys and values, then sort by serialized key bytes.
        // Null keys are rejected inline; null values are allowed for MAP.
        let mut serialized: Vec<(Vec<u8>, Vec<u8>)> = entries
            .iter()
            .map(|(key, val)| {
                if matches!(key, Value::Null) {
                    return Err(Error::InvalidInput(
                        "MAP keys cannot be null (CQL semantics)".to_string(),
                    ));
                }
                Ok((serialize_value(key)?, serialize_value(val)?))
            })
            .collect::<Result<Vec<_>>>()?;
        serialized.sort_by(|a, b| a.0.cmp(&b.0));

        // Cell count
        encode_unsigned(serialized.len() as u64, buf);

        for (path_bytes, value_bytes) in &serialized {
            // Cell header: flags + optional TTL fields
            self.write_complex_cell_header(buf, 0, timestamp_micros, ttl_seconds)?;

            // Cell path: serialized key
            encode_unsigned(path_bytes.len() as u64, buf);
            buf.extend_from_slice(path_bytes);

            // Cell value: serialized value
            encode_unsigned(value_bytes.len() as u64, buf);
            buf.extend_from_slice(value_bytes);
        }

        Ok(())
    }

    /// Write LIST complex cells.
    ///
    /// LIST elements: cell_path = 16-byte TimeUUID, cell value = serialized element.
    /// Lists preserve insertion order (no sorting) — TimeUUIDs provide ordering.
    fn write_list_complex_cells(
        &self,
        buf: &mut Vec<u8>,
        value: &Value,
        timestamp_micros: i64,
        ttl_seconds: Option<u32>,
    ) -> Result<()> {
        let elements = match value {
            Value::List(elements) => elements,
            _ => {
                return Err(Error::InvalidInput(format!(
                    "Expected List value for complex LIST column, got {:?}",
                    value
                )))
            }
        };

        // Cell count
        encode_unsigned(elements.len() as u64, buf);

        for (i, elem) in elements.iter().enumerate() {
            // Reject null elements inline (CQL semantics)
            if matches!(elem, Value::Null) {
                return Err(Error::InvalidInput(
                    "LIST elements cannot be null (CQL semantics)".to_string(),
                ));
            }

            // Cell header: flags + optional TTL fields
            self.write_complex_cell_header(buf, 0, timestamp_micros, ttl_seconds)?;

            // Cell path: 16-byte TimeUUID
            let timeuuid = generate_list_cell_path_timeuuid(timestamp_micros, i as u64);
            encode_unsigned(16u64, buf);
            buf.extend_from_slice(&timeuuid);

            // Cell value: serialized element
            let value_bytes = serialize_value(elem)?;
            encode_unsigned(value_bytes.len() as u64, buf);
            buf.extend_from_slice(&value_bytes);
        }

        Ok(())
    }

    /// Write a complex (non-frozen collection) column from per-element cells,
    /// each carrying its OWN timestamp/ttl/local-deletion-time and its PRESERVED
    /// source cell path (epic #899, Phase B — writer capability).
    ///
    /// This is the per-element counterpart of [`write_complex_column`] (which
    /// takes a whole-column `Value` at one row timestamp). It differs in two
    /// ways that are the whole point of epic #899:
    ///
    /// 1. **Real complex deletion** — when `complex_deletion` is `Some((mfda,
    ///    ldt))` the column header is the REAL deletion marker (unsigned VInt
    ///    deltas against the seeded baselines), not the hardcoded
    ///    `DeletionTime.LIVE` sentinel that [`write_complex_column`] always
    ///    writes. `None` writes the LIVE sentinel (byte-identical to the
    ///    whole-column path).
    /// 2. **Per-element metadata** — each element is stamped with its own
    ///    timestamp (kept as `USE_ROW_TIMESTAMP` only when equal to `row_ts`,
    ///    else an explicit unsigned delta), ttl, and local deletion time, and
    ///    its source `cell_path` is written verbatim (LIST 16-byte TimeUUID
    ///    round-trips, NOT regenerated).
    ///
    /// Element ORDER follows the on-disk invariant: SET/MAP are sorted by
    /// `cell_path` bytes (the serialized element / key); LIST preserves the
    /// caller-supplied (insertion) order — per-element timestamps must not
    /// reorder elements.
    ///
    /// PHASE B: exercised by unit tests only; `merge_entry_to_mutation` does NOT
    /// yet emit the ops that reach here (Phase C).
    fn write_complex_column_per_element(
        &self,
        buf: &mut Vec<u8>,
        column: &Column,
        complex_deletion: Option<(i64, i32)>,
        elements: &[ComplexElementWrite],
        row_ts: i64,
    ) -> Result<()> {
        // ---- Column deletion header (markedForDeleteAt then localDeletionTime).
        match complex_deletion {
            None => {
                // DeletionTime.LIVE — byte-identical to write_complex_column.
                let ts_delta = i64::MIN.wrapping_sub(self.stats.min_timestamp) as u64;
                encode_unsigned(ts_delta, buf);
                let ldt_delta = i32::MAX.wrapping_sub(self.stats.min_local_deletion_time) as u32;
                encode_unsigned(ldt_delta as u64, buf);
            }
            Some((marked_for_delete_at, local_deletion_time)) => {
                // Real deletion marker (matches write_complex_column_deletion's
                // header encoding, but followed by surviving cells rather than 0).
                let ts_delta = (marked_for_delete_at - self.stats.min_timestamp) as u64;
                encode_unsigned(ts_delta, buf);

                // Issue #853 / epic #899 invariant: encode the LDT delta with the
                // same i32 wrapping cast Cassandra uses, so a far-future LDT in
                // [2^31, 2^32) keeps the correct byte count. Reject only a genuine
                // below-baseline ordering violation in normal (non-negative) space.
                if local_deletion_time >= 0
                    && self.stats.min_local_deletion_time >= 0
                    && local_deletion_time < self.stats.min_local_deletion_time
                {
                    return Err(Error::InvalidInput(format!(
                        "Complex deletion: local deletion time {} is less than min_local_deletion_time {}",
                        local_deletion_time, self.stats.min_local_deletion_time
                    )));
                }
                let ldt_delta =
                    local_deletion_time.wrapping_sub(self.stats.min_local_deletion_time) as u32;
                encode_unsigned(ldt_delta as u64, buf);
            }
        }

        // ---- Element order: SET/MAP by cell_path bytes; LIST insertion order.
        let is_list = {
            let dt = column.data_type.to_lowercase();
            dt.starts_with("list<") || dt.starts_with("org.apache.cassandra.db.marshal.listtype(")
        };
        let mut ordered: Vec<&ComplexElementWrite> = elements.iter().collect();
        if !is_list {
            ordered.sort_by(|a, b| a.cell_path.cmp(&b.cell_path));
        }

        // ---- Cell count.
        encode_unsigned(ordered.len() as u64, buf);

        // ---- Per-element cells.
        for elem in ordered {
            self.write_complex_element_cell(buf, elem, row_ts)?;
        }

        Ok(())
    }

    /// Write one per-element complex cell (epic #899, Phase B).
    ///
    /// Wire format (matching the reader's `parse_complex_cell_value`):
    /// ```text
    /// [flags: u8]
    /// [timestamp_delta: unsigned VInt]   if NOT USE_ROW_TIMESTAMP
    /// [ldt_delta: unsigned VInt]         if (IS_DELETED || IS_EXPIRING) && !USE_ROW_TTL
    /// [ttl_delta: unsigned VInt]         if IS_EXPIRING && !USE_ROW_TTL
    /// [path_len: unsigned VInt][path_bytes]
    /// [value_len: unsigned VInt][value_bytes]   if NOT (IS_DELETED || HAS_EMPTY_VALUE)
    /// ```
    fn write_complex_element_cell(
        &self,
        buf: &mut Vec<u8>,
        elem: &ComplexElementWrite,
        row_ts: i64,
    ) -> Result<()> {
        // Determine flags. A SET member (and any element with no value that is
        // NOT a tombstone) sets HAS_EMPTY_VALUE; a tombstone sets IS_DELETED.
        let mut flags = 0u8;
        if elem.is_deleted {
            flags |= CELL_IS_DELETED;
        } else if elem.value.is_none() {
            flags |= CELL_HAS_EMPTY_VALUE;
        }
        if elem.ttl_seconds.is_some() {
            flags |= CELL_IS_EXPIRING;
        }
        // Keep USE_ROW_TIMESTAMP only when the element's timestamp equals the row
        // timestamp; otherwise the element carries its own explicit delta.
        let use_row_ts = elem.timestamp_micros == row_ts;
        if use_row_ts {
            flags |= CELL_USE_ROW_TIMESTAMP;
        }

        buf.push(flags);

        // Timestamp delta (unsigned VInt) only when not borrowing the row ts.
        if !use_row_ts {
            let ts_delta = (elem.timestamp_micros - self.stats.min_timestamp) as u64;
            encode_unsigned(ts_delta, buf);
        }

        // Local deletion time delta — present for deleted or expiring cells.
        let is_expiring = elem.ttl_seconds.is_some();
        if elem.is_deleted || is_expiring {
            let ldt = match elem.local_deletion_time {
                Some(ldt) => ldt,
                None => {
                    return Err(Error::InvalidInput(format!(
                        "Complex element (deleted/expiring) requires a local_deletion_time \
                         (cell_path={:?})",
                        elem.cell_path
                    )));
                }
            };
            // Same i32 wrapping cast as the row/range/complex-deletion paths so a
            // far-future LDT in [2^31, 2^32) keeps the right byte count (epic #899).
            if ldt >= 0
                && self.stats.min_local_deletion_time >= 0
                && ldt < self.stats.min_local_deletion_time
            {
                return Err(Error::InvalidInput(format!(
                    "Complex element: local deletion time {} is less than min_local_deletion_time {}",
                    ldt, self.stats.min_local_deletion_time
                )));
            }
            let ldt_delta = ldt.wrapping_sub(self.stats.min_local_deletion_time) as u32;
            encode_unsigned(ldt_delta as u64, buf);
        }

        // TTL delta — present for expiring cells.
        if is_expiring {
            let ttl = elem.ttl_seconds.unwrap_or(0);
            let ttl_delta = (ttl as i64) - (self.stats.min_ttl as i64);
            if ttl_delta < 0 {
                return Err(Error::InvalidInput(format!(
                    "Complex element: TTL {} is less than min_ttl {}",
                    ttl, self.stats.min_ttl
                )));
            }
            encode_unsigned(ttl_delta as u64, buf);
        }

        // Cell path — PRESERVED verbatim (LIST 16-byte TimeUUID round-trips).
        encode_unsigned(elem.cell_path.len() as u64, buf);
        buf.extend_from_slice(&elem.cell_path);

        // Value — written only for a live element with a value. Tombstones and
        // empty-value elements (SET members) write none.
        if let (false, Some(value)) = (elem.is_deleted, &elem.value) {
            let value_bytes = serialize_value(value)?;
            encode_unsigned(value_bytes.len() as u64, buf);
            buf.extend_from_slice(&value_bytes);
        }

        Ok(())
    }

    /// Write a single cell
    ///
    /// Format:
    /// ```text
    /// [flags: u8]
    /// [timestamp_delta: VInt if NOT USE_ROW_TIMESTAMP]
    /// [value_length: VInt]
    /// [value_bytes]
    /// ```
    ///
    /// NOTE: NULL values should NOT be written - they are represented by absence in the bitmap.
    /// This function will return an error if called with Value::Null.
    fn write_cell(
        &self,
        buf: &mut Vec<u8>,
        column: &str,
        value: &Value,
        timestamp: i64,
    ) -> Result<()> {
        // NULL values should not be written as cells - they are represented by absence
        if matches!(value, Value::Null) {
            return Err(Error::InvalidInput(format!(
                "NULL values should not be written as cells (column: {}). They are represented by absence in the bitmap.",
                column
            )));
        }

        // Cell flags
        let mut flags = CELL_USE_ROW_TIMESTAMP; // Use row timestamp by default

        // Empty string: set HAS_EMPTY_VALUE flag
        // This is for actual empty strings (''), not NULLs
        let is_empty_string = matches!(value, Value::Text(s) if s.is_empty());
        if is_empty_string {
            flags |= CELL_HAS_EMPTY_VALUE;
        }

        buf.push(flags);

        // Timestamp (skip if USE_ROW_TIMESTAMP)
        // Fix #644 (S6): Cell timestamp delta is UNSIGNED VInt per Cassandra
        // SerializationHeader.java:167: out.writeUnsignedVInt(timestamp - stats.minTimestamp).
        if (flags & CELL_USE_ROW_TIMESTAMP) == 0 {
            let timestamp_delta = (timestamp - self.stats.min_timestamp) as u64;
            encode_unsigned(timestamp_delta, buf);
        }

        if (flags & CELL_HAS_EMPTY_VALUE) != 0 {
            return Ok(());
        }

        // Value
        let value_bytes = serialize_value(value)?;

        // Bounds check: value length must fit in i64
        if value_bytes.len() > i64::MAX as usize {
            return Err(Error::InvalidInput(format!(
                "Value too large for column '{}': {} bytes (max {})",
                column,
                value_bytes.len(),
                i64::MAX
            )));
        }

        if cell_value_uses_length_prefix(value) {
            encode_unsigned(value_bytes.len() as u64, buf);
        }

        // Write value bytes
        buf.extend_from_slice(&value_bytes);

        Ok(())
    }

    /// Write a live cell that carries its own timestamp (no USE_ROW_TIMESTAMP).
    ///
    /// Used for cells merged into a row from a different mutation than the
    /// one providing the row's liveness timestamp.
    ///
    /// Format:
    /// ```text
    /// [flags: u8]                ← 0x00 (or HAS_EMPTY_VALUE for empty text)
    /// [timestamp_delta: VUInt]   ← delta from min_timestamp
    /// [value_length: VInt]       ← variable-length types only
    /// [value_bytes]
    /// ```
    fn write_cell_explicit_ts(
        &self,
        buf: &mut Vec<u8>,
        column: &str,
        value: &Value,
        timestamp: i64,
    ) -> Result<()> {
        if matches!(value, Value::Null) {
            return Err(Error::InvalidInput(format!(
                "NULL values should not be written as cells (column: {}). They are represented by absence in the bitmap.",
                column
            )));
        }

        let mut flags = 0u8;
        if matches!(value, Value::Text(s) if s.is_empty()) {
            flags |= CELL_HAS_EMPTY_VALUE;
        }
        buf.push(flags);

        // Timestamp delta (UNSIGNED VInt)
        let timestamp_delta = (timestamp - self.stats.min_timestamp) as u64;
        encode_unsigned(timestamp_delta, buf);

        if (flags & CELL_HAS_EMPTY_VALUE) != 0 {
            return Ok(());
        }

        let value_bytes = serialize_value(value)?;
        if value_bytes.len() > i64::MAX as usize {
            return Err(Error::InvalidInput(format!(
                "Value too large for column '{}': {} bytes (max {})",
                column,
                value_bytes.len(),
                i64::MAX
            )));
        }

        if cell_value_uses_length_prefix(value) {
            encode_unsigned(value_bytes.len() as u64, buf);
        }

        buf.extend_from_slice(&value_bytes);
        Ok(())
    }

    /// Write a cell with TTL (expiring cell)
    ///
    /// Format:
    /// ```text
    /// [flags: u8]                    ← CELL_IS_EXPIRING (0x02) set
    /// [timestamp_delta: VInt]        ← Delta from min_timestamp (NOT USE_ROW_TIMESTAMP for TTL cells)
    /// [local_deletion_time_delta: VUInt]  ← When the cell expires (relative to min_local_deletion_time)
    /// [ttl_delta: VUInt]            ← TTL value (relative to min_ttl)
    /// [value_length: VInt]
    /// [value_bytes]
    /// ```
    ///
    /// CRITICAL: TTL cells MUST NOT use USE_ROW_TIMESTAMP or USE_ROW_TTL flags.
    /// They need explicit timestamp and TTL deltas.
    fn write_cell_with_ttl(
        &self,
        buf: &mut Vec<u8>,
        column: &str,
        value: &Value,
        timestamp: i64,
        ttl_seconds: u32,
    ) -> Result<()> {
        // NULL values should not be written as cells
        if matches!(value, Value::Null) {
            return Err(Error::InvalidInput(format!(
                "NULL values should not be written as cells (column: {}). They are represented by absence in the bitmap.",
                column
            )));
        }

        let local_deletion_time = self.expiring_local_deletion_time(ttl_seconds)?;

        // Cell flags - CELL_IS_EXPIRING, NO USE_ROW_TIMESTAMP or USE_ROW_TTL
        let mut flags = CELL_IS_EXPIRING;
        if matches!(value, Value::Text(s) if s.is_empty()) {
            flags |= CELL_HAS_EMPTY_VALUE;
        }
        buf.push(flags);

        // Timestamp delta (required for expiring cells)
        // Fix #644 (S6): Cell timestamp delta is UNSIGNED VInt.
        // SerializationHeader.java:167: out.writeUnsignedVInt(timestamp - stats.minTimestamp)
        let timestamp_delta = (timestamp - self.stats.min_timestamp) as u64;
        encode_unsigned(timestamp_delta, buf);

        // Local deletion time delta
        let ldt_delta = (local_deletion_time as i64) - (self.stats.min_local_deletion_time as i64);
        if ldt_delta < 0 {
            return Err(Error::InvalidInput(format!(
                "Local deletion time {} is less than min_local_deletion_time {}",
                local_deletion_time, self.stats.min_local_deletion_time
            )));
        }
        encode_unsigned(ldt_delta as u64, buf);

        // TTL delta
        let ttl_delta = (ttl_seconds as i64) - (self.stats.min_ttl as i64);
        if ttl_delta < 0 {
            return Err(Error::InvalidInput(format!(
                "TTL {} is less than min_ttl {}",
                ttl_seconds, self.stats.min_ttl
            )));
        }
        encode_unsigned(ttl_delta as u64, buf);

        if (flags & CELL_HAS_EMPTY_VALUE) != 0 {
            return Ok(());
        }

        // Value
        let value_bytes = serialize_value(value)?;

        // Bounds check: value length must fit in i64
        if value_bytes.len() > i64::MAX as usize {
            return Err(Error::InvalidInput(format!(
                "Value too large for column '{}': {} bytes (max {})",
                column,
                value_bytes.len(),
                i64::MAX
            )));
        }

        if cell_value_uses_length_prefix(value) {
            encode_unsigned(value_bytes.len() as u64, buf);
        }

        // Write value bytes
        buf.extend_from_slice(&value_bytes);

        Ok(())
    }

    fn write_cell_with_row_ttl(
        &self,
        buf: &mut Vec<u8>,
        column: &str,
        value: &Value,
        _timestamp: i64,
        _ttl_seconds: u32,
    ) -> Result<()> {
        if matches!(value, Value::Null) {
            return Err(Error::InvalidInput(format!(
                "NULL values should not be written as cells (column: {}). They are represented by absence in the bitmap.",
                column
            )));
        }

        let mut flags = CELL_IS_EXPIRING | CELL_USE_ROW_TIMESTAMP | CELL_USE_ROW_TTL;
        if matches!(value, Value::Text(s) if s.is_empty()) {
            flags |= CELL_HAS_EMPTY_VALUE;
        }
        buf.push(flags);

        if (flags & CELL_HAS_EMPTY_VALUE) != 0 {
            return Ok(());
        }

        let value_bytes = serialize_value(value)?;
        if value_bytes.len() > i64::MAX as usize {
            return Err(Error::InvalidInput(format!(
                "Value too large for column '{}': {} bytes (max {})",
                column,
                value_bytes.len(),
                i64::MAX
            )));
        }

        if cell_value_uses_length_prefix(value) {
            encode_unsigned(value_bytes.len() as u64, buf);
        }

        buf.extend_from_slice(&value_bytes);
        Ok(())
    }

    fn expiring_local_deletion_time(&self, ttl_seconds: u32) -> Result<i32> {
        let now_seconds = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| Error::Storage(format!("System time error: {}", e)))?
            .as_secs() as i32;
        Ok(now_seconds.saturating_add(ttl_seconds as i32))
    }

    /// Write a tombstone cell
    ///
    /// Tombstones require:
    /// - IS_DELETED flag set
    /// - Own timestamp (NOT USE_ROW_TIMESTAMP - tombstones need explicit timestamps)
    /// - local_deletion_time field
    /// - No value data
    fn write_tombstone_cell(
        &self,
        buf: &mut Vec<u8>,
        _column: &str,
        timestamp: i64,
        local_deletion_time: i32,
    ) -> Result<()> {
        // Cell flags for tombstone
        // CRITICAL: Do NOT set USE_ROW_TIMESTAMP - tombstones need their own timestamp
        //
        // Issue #716: HAS_EMPTY_VALUE MUST be set. Cassandra's Cell.Serializer
        // derives `hasValue = (flags & HAS_EMPTY_VALUE_MASK) == 0`, so a deleted
        // cell without this flag makes Cassandra read a value that was never
        // written, desyncing the row stream (EOFException on readback).
        let flags = CELL_IS_DELETED | CELL_HAS_EMPTY_VALUE;
        buf.push(flags);

        // Timestamp delta (VInt) - required for tombstones
        // Fix #644 (S6): tombstone timestamp delta is UNSIGNED VInt per Cassandra.
        // SerializationHeader.java:167: out.writeUnsignedVInt(timestamp - stats.minTimestamp)
        let timestamp_delta = (timestamp - self.stats.min_timestamp) as u64;
        encode_unsigned(timestamp_delta, buf);

        // Local deletion time delta (VUInt) - required for tombstones
        let deletion_time_delta =
            (local_deletion_time as i64) - (self.stats.min_local_deletion_time as i64);
        if deletion_time_delta < 0 {
            return Err(Error::InvalidInput(format!(
                "Local deletion time {} is less than min_local_deletion_time {}",
                local_deletion_time, self.stats.min_local_deletion_time
            )));
        }
        encode_unsigned(deletion_time_delta as u64, buf);

        // No value length or value bytes for tombstones
        // Parser returns immediately after reading local_deletion_time
        Ok(())
    }

    /// Write a single range tombstone bound marker.
    ///
    /// On-disk layout (must mirror the reader's `skip_range_tombstone_marker`
    /// and Cassandra's `UnfilteredSerializer.serialize(RangeTombstoneMarker)`):
    /// ```text
    /// [flags: u8]                      ← IS_MARKER (0x02)
    /// [bound_kind: u8]                 ← ClusteringPrefix.Kind ordinal
    /// [cluster_count: u16 BE]          ← bound.size()
    /// [cluster_header: VUInt]          ← only when cluster_count > 0
    /// [cluster_values: ...]
    /// [marker_body_size: VUInt]        ← size of (prev_size + deletion times)
    /// [prev_unfiltered_size: VUInt]
    /// [marked_for_delete_at: VUInt]    ← delta from min_timestamp (µs)
    /// [local_deletion_time: VUInt]     ← delta from min_local_deletion_time (s)
    /// ```
    ///
    /// Issue #717: the previous writer emitted private bound-kind ordinals,
    /// no u16 cluster count, and no marker_body_size/prev_size VInts — bytes
    /// no Cassandra (or CQLite) reader could parse.
    ///
    /// Returns the total serialized marker size (for prev_unfiltered_size
    /// threading).
    fn write_range_bound(
        &mut self,
        bound: &ClusteringBound,
        is_open: bool,
        deletion_time: i64,
        local_deletion_time: i32,
        schema: &TableSchema,
        prev_size: u64,
    ) -> Result<usize> {
        let start_len = self.buffer.len();

        // Marker flag
        self.buffer.push(IS_MARKER);

        // Bound kind (ClusteringPrefix.Kind ordinal) + clustering values.
        // Bottom/Top are the full-partition bounds: an inclusive bound with
        // zero clustering values.
        let (bound_kind, clustering) = match (is_open, bound) {
            (true, ClusteringBound::Inclusive(ck)) => (INCL_START_BOUND, Some(ck)),
            (true, ClusteringBound::Exclusive(ck)) => (EXCL_START_BOUND, Some(ck)),
            (false, ClusteringBound::Inclusive(ck)) => (INCL_END_BOUND, Some(ck)),
            (false, ClusteringBound::Exclusive(ck)) => (EXCL_END_BOUND, Some(ck)),
            (true, ClusteringBound::Bottom | ClusteringBound::Top) => (INCL_START_BOUND, None),
            (false, ClusteringBound::Bottom | ClusteringBound::Top) => (INCL_END_BOUND, None),
        };
        self.buffer.push(bound_kind);

        // Cluster count (u16 BE) — ClusteringBoundOrBoundary.Serializer
        // writes `out.writeShort(bound.size())` before the values.
        let cluster_count = clustering.map_or(0, |ck| ck.columns.len());
        if cluster_count > u16::MAX as usize {
            return Err(Error::InvalidInput(format!(
                "Range tombstone bound has too many clustering values: {}",
                cluster_count
            )));
        }
        self.buffer
            .write_all(&(cluster_count as u16).to_be_bytes())?;

        // Clustering header + values (only when the bound carries values).
        if let Some(ck) = clustering {
            self.write_clustering_prefix(ck, schema)?;
        }

        // Deletion time: Cassandra canonical order (markedForDeleteAt first,
        // then localDeletionTime), both UNSIGNED VInt deltas.
        //
        // Issue #853 / #889: localDeletionTime and minLocalDeletionTime are Java
        // `int`s; Cassandra's DeletionTime.serialize emits
        // `writeUnsignedVInt32(localDeletionTime - minLocalDeletionTime)`, a 32-bit
        // subtraction zero-extended into [0, 2^32). A far-future LDT in [2^31, 2^32)
        // is a negative i32 here; widening to i64 first (the previous code) produced
        // a 64-bit wrapped delta with a different byte length than Cassandra's i32
        // form, corrupting both the bytes and the marker_body_size vint. Reject only
        // a genuine below-baseline ordering violation in normal (non-negative i32)
        // time space; a far-future LDT (negative as i32) is legitimate.
        if local_deletion_time >= 0
            && self.stats.min_local_deletion_time >= 0
            && local_deletion_time < self.stats.min_local_deletion_time
        {
            return Err(Error::InvalidInput(format!(
                "Range tombstone: local deletion time {} is less than min_local_deletion_time {}",
                local_deletion_time, self.stats.min_local_deletion_time
            )));
        }
        let mut deletion = Vec::new();
        let ts_delta = (deletion_time - self.stats.min_timestamp) as u64;
        encode_unsigned(ts_delta, &mut deletion);
        let ldt_delta = local_deletion_time.wrapping_sub(self.stats.min_local_deletion_time) as u32;
        encode_unsigned(ldt_delta as u64, &mut deletion);

        // marker_body_size covers the prev_size VInt + deletion times (same
        // convention as row_size for rows).
        let body_size = unsigned_len(prev_size) as u64 + deletion.len() as u64;
        encode_unsigned(body_size, &mut self.buffer);
        encode_unsigned(prev_size, &mut self.buffer);
        self.buffer.extend_from_slice(&deletion);

        Ok(self.buffer.len() - start_len)
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

    fn ordered_columns<'a, F>(&self, schema: &'a TableSchema, predicate: F) -> Vec<&'a Column>
    where
        F: Fn(&Column) -> bool,
    {
        let mut columns: Vec<&'a Column> = schema
            .columns
            .iter()
            .filter(|column| predicate(column))
            .collect();
        columns.sort_by_key(|column| column_order_key(column));
        columns
    }

    /// Sort merged ops into regular-column serialization order
    /// (simple columns before complex, then by name).
    fn sorted_merged_ops<'a, 'b>(
        &self,
        ops: &'b [MergedOp<'a>],
        schema: &TableSchema,
    ) -> Vec<&'b MergedOp<'a>> {
        let columns = self.regular_columns(schema);
        let column_order: std::collections::HashMap<&str, usize> = columns
            .iter()
            .enumerate()
            .map(|(idx, column)| (column.name.as_str(), idx))
            .collect();

        let mut sorted: Vec<&'b MergedOp<'a>> = ops.iter().collect();
        sorted.sort_by_key(|mop| match mop.op {
            crate::storage::write_engine::mutation::CellOperation::Write { column, .. }
            | crate::storage::write_engine::mutation::CellOperation::WriteWithTtl {
                column, ..
            }
            | crate::storage::write_engine::mutation::CellOperation::Delete { column }
            | crate::storage::write_engine::mutation::CellOperation::WriteComplexElement {
                column,
                ..
            }
            | crate::storage::write_engine::mutation::CellOperation::ComplexDeletion {
                column,
                ..
            } => column_order
                .get(column.as_str())
                .copied()
                .unwrap_or(usize::MAX - 1),
            crate::storage::write_engine::mutation::CellOperation::DeleteRow => usize::MAX,
        });
        sorted
    }

    fn write_column_subset(
        &self,
        buf: &mut Vec<u8>,
        columns: &[&Column],
        present_columns: &std::collections::HashSet<&str>,
    ) -> Result<()> {
        let mut present_indices = Vec::new();
        let mut missing_indices = Vec::new();

        for (idx, column) in columns.iter().enumerate() {
            if present_columns.contains(column.name.as_str()) {
                present_indices.push(idx);
            } else {
                missing_indices.push(idx);
            }
        }

        if missing_indices.is_empty() {
            encode_unsigned(0, buf);
            return Ok(());
        }

        if columns.len() < 64 {
            let mut bitmap = 0u64;
            for idx in missing_indices {
                bitmap |= 1u64 << idx;
            }
            encode_unsigned(bitmap, buf);
            return Ok(());
        }

        encode_unsigned((columns.len() - present_indices.len()) as u64, buf);

        if present_indices.len() < columns.len() / 2 {
            for idx in present_indices {
                encode_unsigned(idx as u64, buf);
            }
        } else {
            for idx in missing_indices {
                encode_unsigned(idx as u64, buf);
            }
        }

        Ok(())
    }
}

/// Returns true if the column type is a non-frozen collection (complex column).
///
/// Complex columns are stored as multiple cells with cell paths, unlike
/// frozen collections which are stored as a single cell with blob value.
/// Matches the reader logic in `v5_compressed_legacy.rs`.
fn is_complex_column(data_type: &str) -> bool {
    let dt = data_type.to_lowercase();

    // Frozen collections are NOT complex (they're single-cell frozen types)
    if dt.starts_with("frozen<") || dt.starts_with("org.apache.cassandra.db.marshal.frozentype(") {
        return false;
    }

    // CQL-style collection types
    if dt.starts_with("list<") || dt.starts_with("set<") || dt.starts_with("map<") {
        return true;
    }

    // Cassandra internal collection types
    if dt.starts_with("org.apache.cassandra.db.marshal.listtype(")
        || dt.starts_with("org.apache.cassandra.db.marshal.settype(")
        || dt.starts_with("org.apache.cassandra.db.marshal.maptype(")
    {
        return true;
    }

    false
}

/// A surviving cell operation in a merged row, tagged with the timestamp and
/// row-level TTL of the mutation it came from.
///
/// Epic #899 (Phase B): for whole-column ops (`Write`/`WriteWithTtl`/`Delete`)
/// `timestamp_micros` is the originating mutation's row timestamp. For the
/// per-element complex ops (`WriteComplexElement`/`ComplexDeletion`) the
/// element's OWN timestamp/ttl/ldt/cell_path live INSIDE the op itself; the
/// `timestamp_micros` field still carries the originating mutation's row
/// timestamp so the writer can decide `USE_ROW_TIMESTAMP` vs an explicit delta
/// per element.
struct MergedOp<'a> {
    op: &'a crate::storage::write_engine::mutation::CellOperation,
    timestamp_micros: i64,
    /// Row-level TTL (`Mutation::ttl_seconds`) of the originating mutation.
    /// Per-cell TTL lives inside `CellOperation::WriteWithTtl` itself.
    row_ttl_seconds: Option<u32>,
    /// Local deletion time (seconds since epoch) for a `Delete` cell tombstone,
    /// honoring the originating mutation's explicit `local_deletion_time` when
    /// present (Issue #764). Derived from the timestamp otherwise.
    cell_local_deletion_time: i32,
}

/// One element to emit inside a per-element complex column (epic #899, Phase B).
///
/// Carries the element's OWN write metadata and its PRESERVED source cell path
/// (never regenerated). `value == None` with `is_deleted` true is an
/// element-level tombstone; `value == None` without `is_deleted` is an
/// empty-value element (e.g. a SET member). The writer stamps each element with
/// `USE_ROW_TIMESTAMP` only when `timestamp_micros` equals the row timestamp,
/// otherwise it clears the flag and writes an explicit unsigned delta.
#[derive(Debug, Clone)]
struct ComplexElementWrite {
    cell_path: Vec<u8>,
    value: Option<Value>,
    timestamp_micros: i64,
    ttl_seconds: Option<u32>,
    local_deletion_time: Option<i32>,
    is_deleted: bool,
}

/// One complex column's reconciled contents while grouping per-element ops:
/// `(optional complex deletion (markedForDeleteAt µs, localDeletionTime s),
/// surviving elements)` (epic #899, Phase B).
type ComplexColumnGroup = (Option<(i64, i32)>, Vec<ComplexElementWrite>);

/// A surviving static-column operation, tagged with the timestamp and explicit
/// local deletion time of the mutation it came from.
///
/// Issue #764: static-column tombstones must be stamped with their ORIGINATING
/// mutation's `local_deletion_time` (and timestamp), not a single synthetic
/// value taken from the newest static-contributing mutation. A surviving delete
/// from an older mutation otherwise inherits the wrong LDT — corrupting the
/// unsigned-VInt delta when stats were seeded from that older delete's explicit
/// (lower) LDT.
struct StaticMergedOp {
    op: crate::storage::write_engine::mutation::CellOperation,
    /// Timestamp (µs) of the originating mutation.
    timestamp_micros: i64,
    /// Local deletion time (s) for a `Delete` tombstone, honoring the
    /// originating mutation's explicit `local_deletion_time` when set.
    cell_local_deletion_time: i32,
}

/// The exact rows and cells `DataWriter` emitted to Data.db for one partition.
///
/// Issue #851: Statistics' `totalRows` (`row_count`) and `totalColumnsSet`
/// (`column_count`) MUST equal what is physically written. Rather than
/// re-deriving the counts from the raw mutations in a parallel loop (which kept
/// diverging from the emitter — rejected commit `5afce78c`), the emission code
/// is the single source of truth: it tallies a row whenever it writes a row
/// (static prelude or merged clustering row) and tallies cells from the same
/// reconciled `ops` it serializes. The empty static-row prelude and range
/// tombstone markers write no `Row`, so they contribute nothing — matching
/// Cassandra `Row.isEmpty()` / `Row.columnCount()`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PartitionEmitCounts {
    /// Rows physically written to Data.db (static prelude + merged clustering
    /// rows). Excludes the empty static prelude and range tombstone markers.
    pub rows: u64,
    /// Regular + static cells physically written. Primary-key (partition +
    /// clustering) columns are encoded positionally and never counted; row
    /// tombstones (`DeleteRow`) set no columns.
    pub columns: u64,
}

/// One Data.db row assembled by merging every mutation of a partition that
/// shares the same clustering key (Issues #716/#717: a partition must never
/// contain two rows with equal clustering).
struct RowWrite<'a> {
    clustering_key: Option<&'a crate::storage::write_engine::mutation::ClusteringKey>,
    /// Primary-key liveness timestamp. `None` for pure row tombstones —
    /// Cassandra serializes those without HAS_TIMESTAMP.
    liveness_ts: Option<i64>,
    /// Row-level TTL from the liveness-providing mutation.
    ttl_seconds: Option<u32>,
    /// Row deletion as (marked_for_delete_at µs, local_deletion_time s).
    row_deletion: Option<(i64, i32)>,
    /// Surviving WHOLE-COLUMN cell operations (already reconciled, unsorted).
    /// `Write`/`WriteWithTtl`/`Delete` — one per column (last-write-wins).
    ops: Vec<MergedOp<'a>>,
    /// Surviving PER-ELEMENT complex ops (epic #899, Phase B):
    /// `WriteComplexElement` + `ComplexDeletion`. These are NOT deduped per
    /// column (a column has many elements) and are written via
    /// `write_complex_column_per_element`. Empty for every existing scenario —
    /// the real pipeline (`merge_entry_to_mutation`) does not yet emit these
    /// ops, so this stays empty until Phase C (keeping output byte-neutral).
    complex_element_ops: Vec<MergedOp<'a>>,
}

fn column_order_key(column: &Column) -> (bool, &str) {
    (is_complex_column(&column.data_type), column.name.as_str())
}

/// Generate a version-1 TimeUUID for use as a list cell path.
///
/// List elements in Cassandra use TimeUUIDs as cell paths to maintain insertion order.
/// Each call with a different `element_index` produces a monotonically increasing UUID.
///
/// # Arguments
/// * `timestamp_micros` - Mutation timestamp in microseconds since Unix epoch
/// * `element_index` - Index of the element within the list (for monotonic ordering)
fn generate_list_cell_path_timeuuid(timestamp_micros: i64, element_index: u64) -> [u8; 16] {
    // UUID v1 timestamp: 100-nanosecond intervals since UUID epoch (Oct 15, 1582)
    // Offset from Unix epoch to UUID epoch in 100-ns units
    const UUID_EPOCH_OFFSET: u64 = 0x01B2_1DD2_1381_4000;

    let ts_100ns = (timestamp_micros as u64) * 10 + element_index;
    let uuid_ts = ts_100ns + UUID_EPOCH_OFFSET;

    // Extract time fields per RFC 4122
    let time_low = (uuid_ts & 0xFFFF_FFFF) as u32;
    let time_mid = ((uuid_ts >> 32) & 0xFFFF) as u16;
    let time_hi = ((uuid_ts >> 48) & 0x0FFF) as u16 | 0x1000; // version 1

    // Fixed clock_seq and node for deterministic output
    let clock_seq: u16 = 0x80; // variant bits (10xx) + seq=0
    let node: [u8; 6] = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00];

    let mut uuid = [0u8; 16];
    uuid[0..4].copy_from_slice(&time_low.to_be_bytes());
    uuid[4..6].copy_from_slice(&time_mid.to_be_bytes());
    uuid[6..8].copy_from_slice(&time_hi.to_be_bytes());
    uuid[8] = (clock_seq >> 8) as u8;
    uuid[9] = (clock_seq & 0xFF) as u8;
    uuid[10..16].copy_from_slice(&node);

    uuid
}

/// Convert a usize length to i32 for Cassandra's collection wire format.
/// Returns an error if the length exceeds i32::MAX.
fn len_as_i32(len: usize) -> Result<i32> {
    i32::try_from(len).map_err(|_| {
        Error::InvalidInput(format!(
            "Length {} exceeds maximum i32 for collection encoding",
            len
        ))
    })
}

/// Serialize a collection element, rejecting null (CQL semantics: lists/sets cannot contain null).
fn serialize_collection_element(value: &Value, collection_kind: &str) -> Result<Vec<u8>> {
    if matches!(value, Value::Null) {
        return Err(Error::InvalidInput(format!(
            "{} elements cannot be null (CQL semantics)",
            collection_kind
        )));
    }
    serialize_value(value)
}

/// Serialize a Value to bytes for cell storage
///
/// This follows Cassandra's type-specific serialization rules.
fn serialize_value(value: &Value) -> Result<Vec<u8>> {
    match value {
        Value::Null => Ok(Vec::new()),
        Value::Boolean(b) => Ok(vec![if *b { 1 } else { 0 }]),
        Value::TinyInt(n) => Ok(vec![*n as u8]),
        Value::SmallInt(n) => Ok(n.to_be_bytes().to_vec()),
        Value::Integer(n) => Ok(n.to_be_bytes().to_vec()),
        Value::BigInt(n) => Ok(n.to_be_bytes().to_vec()),
        Value::Counter(n) => Ok(n.to_be_bytes().to_vec()),
        Value::Float32(f) => Ok(f.to_bits().to_be_bytes().to_vec()),
        Value::Float(f) => Ok(f.to_bits().to_be_bytes().to_vec()),
        Value::Text(s) => Ok(s.as_bytes().to_vec()),
        Value::Blob(bytes) => Ok(bytes.clone()),
        Value::Timestamp(millis) => Ok(millis.to_be_bytes().to_vec()),
        Value::Date(days) => {
            // Cassandra DATE: stored as unsigned int with Integer.MIN_VALUE offset
            let stored = days.wrapping_sub(i32::MIN) as u32;
            Ok(stored.to_be_bytes().to_vec())
        }
        Value::Time(nanos) => Ok(nanos.to_be_bytes().to_vec()),
        Value::Uuid(bytes) => Ok(bytes.to_vec()),
        Value::Inet(bytes) => Ok(bytes.clone()),
        Value::Varint(bytes) => Ok(bytes.clone()),
        Value::Decimal { scale, unscaled } => {
            let mut result = Vec::new();
            result.extend_from_slice(&scale.to_be_bytes());
            result.extend_from_slice(unscaled);
            Ok(result)
        }
        Value::Duration {
            months,
            days,
            nanos,
        } => {
            let mut result = Vec::new();
            // Cassandra DurationType stores three signed VInts, not fixed-width ints.
            encode_signed(*months as i64, &mut result);
            encode_signed(*days as i64, &mut result);
            encode_signed(*nanos, &mut result);
            Ok(result)
        }
        Value::Udt(udt_value) => {
            // Construct UdtTypeDef from UdtValue fields by inferring types
            let mut schema =
                UdtTypeDef::new(udt_value.keyspace.clone(), udt_value.type_name.clone());

            // Infer field types from values
            for field in &udt_value.fields {
                let field_type = infer_cql_type_from_value(field.value.as_ref());
                schema = schema.with_field(field.name.clone(), field_type, true);
            }

            let serializer = TypeSerializer::new();
            serializer.serialize_udt(value, &schema)
        }
        Value::List(elements) | Value::Set(elements) => {
            let mut buf = Vec::new();
            buf.extend_from_slice(&len_as_i32(elements.len())?.to_be_bytes());
            for elem in elements {
                let elem_bytes = serialize_collection_element(elem, "Collection")?;
                buf.extend_from_slice(&len_as_i32(elem_bytes.len())?.to_be_bytes());
                buf.extend_from_slice(&elem_bytes);
            }
            Ok(buf)
        }
        Value::Map(entries) => {
            let mut buf = Vec::new();
            buf.extend_from_slice(&len_as_i32(entries.len())?.to_be_bytes());
            for (key, val) in entries {
                if matches!(key, Value::Null) {
                    return Err(Error::InvalidInput(
                        "MAP keys cannot be null (CQL semantics)".to_string(),
                    ));
                }
                let key_bytes = serialize_value(key)?;
                buf.extend_from_slice(&len_as_i32(key_bytes.len())?.to_be_bytes());
                buf.extend_from_slice(&key_bytes);
                let val_bytes = serialize_value(val)?;
                buf.extend_from_slice(&len_as_i32(val_bytes.len())?.to_be_bytes());
                buf.extend_from_slice(&val_bytes);
            }
            Ok(buf)
        }
        Value::Tuple(fields) => {
            let mut buf = Vec::new();
            for field in fields {
                match field {
                    Value::Null => buf.extend_from_slice(&(-1i32).to_be_bytes()),
                    other => {
                        let field_bytes = serialize_value(other)?;
                        buf.extend_from_slice(&len_as_i32(field_bytes.len())?.to_be_bytes());
                        buf.extend_from_slice(&field_bytes);
                    }
                }
            }
            Ok(buf)
        }
        Value::Frozen(inner) => serialize_value(inner),
        _ => Err(Error::InvalidInput(format!(
            "Unsupported value type for serialization: {:?}",
            value
        ))),
    }
}

/// Infer CQL type from a Value instance
///
/// Used for UDT serialization when schema context is not available.
/// Empty collections still fall back to `text` because there is no element
/// value available to inspect.
fn infer_cql_type_from_value(value: Option<&Value>) -> CqlType {
    match value {
        None | Some(Value::Null) => CqlType::Text, // Default for NULL
        Some(Value::Boolean(_)) => CqlType::Boolean,
        Some(Value::TinyInt(_)) => CqlType::TinyInt,
        Some(Value::SmallInt(_)) => CqlType::SmallInt,
        Some(Value::Integer(_)) => CqlType::Int,
        Some(Value::BigInt(_)) => CqlType::BigInt,
        Some(Value::Float32(_)) => CqlType::Float,
        Some(Value::Float(_)) => CqlType::Double,
        Some(Value::Text(_)) => CqlType::Text,
        Some(Value::Blob(_)) => CqlType::Blob,
        Some(Value::Timestamp(_)) => CqlType::Timestamp,
        Some(Value::Date(_)) => CqlType::Date,
        Some(Value::Time(_)) => CqlType::Time,
        Some(Value::Uuid(_)) => CqlType::Uuid,
        Some(Value::Inet(_)) => CqlType::Inet,
        Some(Value::Varint(_)) => CqlType::Varint,
        Some(Value::Decimal { .. }) => CqlType::Decimal,
        Some(Value::Duration { .. }) => CqlType::Duration,
        Some(Value::Counter(_)) => CqlType::Counter,
        Some(Value::List(elements)) => CqlType::List(Box::new(
            elements
                .first()
                .map(|elem| infer_cql_type_from_value(Some(elem)))
                .unwrap_or(CqlType::Text),
        )),
        Some(Value::Set(elements)) => CqlType::Set(Box::new(
            elements
                .first()
                .map(|elem| infer_cql_type_from_value(Some(elem)))
                .unwrap_or(CqlType::Text),
        )),
        Some(Value::Map(entries)) => {
            let (key_type, value_type) = entries
                .first()
                .map(|(key, value)| {
                    (
                        infer_cql_type_from_value(Some(key)),
                        infer_cql_type_from_value(Some(value)),
                    )
                })
                .unwrap_or((CqlType::Text, CqlType::Text));
            CqlType::Map(Box::new(key_type), Box::new(value_type))
        }
        Some(Value::Tuple(fields)) => CqlType::Tuple(
            fields
                .iter()
                .map(|field| infer_cql_type_from_value(Some(field)))
                .collect(),
        ),
        Some(Value::Udt(udt)) => CqlType::Udt(
            udt.type_name.clone(),
            udt.fields
                .iter()
                .map(|field| {
                    (
                        field.name.clone(),
                        infer_cql_type_from_value(field.value.as_ref()),
                    )
                })
                .collect(),
        ),
        Some(Value::Frozen(inner)) => {
            CqlType::Frozen(Box::new(infer_cql_type_from_value(Some(inner))))
        }
        Some(Value::Tombstone(_)) => CqlType::Text, // Tombstones shouldn't appear in UDT fields
        Some(Value::Json(_)) => CqlType::Text,      // JSON is stored as text
    }
}

fn cell_value_uses_length_prefix(value: &Value) -> bool {
    !matches!(
        value,
        Value::Boolean(_)
            | Value::Integer(_)
            | Value::BigInt(_)
            | Value::Float32(_)
            | Value::Float(_)
            | Value::Timestamp(_)
            | Value::Uuid(_)
    )
}

fn is_static_row_mutation(mutation: &Mutation, schema: &TableSchema) -> bool {
    if mutation.clustering_key.is_some() || !schema.columns.iter().any(|column| column.is_static) {
        return false;
    }

    mutation.operations.iter().all(|operation| match operation {
        crate::storage::write_engine::mutation::CellOperation::Write { column, .. }
        | crate::storage::write_engine::mutation::CellOperation::WriteWithTtl { column, .. }
        | crate::storage::write_engine::mutation::CellOperation::Delete { column }
        | crate::storage::write_engine::mutation::CellOperation::WriteComplexElement {
            column,
            ..
        }
        | crate::storage::write_engine::mutation::CellOperation::ComplexDeletion {
            column, ..
        } => schema
            .columns
            .iter()
            .find(|candidate| candidate.name == *column)
            .map(|candidate| candidate.is_static)
            .unwrap_or(false),
        crate::storage::write_engine::mutation::CellOperation::DeleteRow => true,
    })
}

/// Returns true if this single operation targets a static column.
fn is_static_operation(
    op: &crate::storage::write_engine::mutation::CellOperation,
    schema: &TableSchema,
) -> bool {
    match op {
        crate::storage::write_engine::mutation::CellOperation::Write { column, .. }
        | crate::storage::write_engine::mutation::CellOperation::WriteWithTtl { column, .. }
        | crate::storage::write_engine::mutation::CellOperation::Delete { column }
        | crate::storage::write_engine::mutation::CellOperation::WriteComplexElement {
            column,
            ..
        }
        | crate::storage::write_engine::mutation::CellOperation::ComplexDeletion {
            column, ..
        } => schema
            .columns
            .iter()
            .find(|c| c.name == *column)
            .map(|c| c.is_static)
            .unwrap_or(false),
        crate::storage::write_engine::mutation::CellOperation::DeleteRow => false,
    }
}

/// Returns true if `column` is part of the primary key — a partition-key or
/// clustering-key column.
///
/// Primary-key columns are encoded positionally (the partition key and the row's
/// clustering prefix); they must NEVER be written as regular cells. The compaction
/// path can surface a clustering column as a `Write` op (the merger keeps the
/// clustering cell for its own read-back, and `merge_entry_to_mutation` turns it
/// into a `Write`); emitting it as a cell writes the value a second time and
/// corrupts the row body for strict readers (#857). The writer drops such ops.
fn is_primary_key_column(column: &str, schema: &TableSchema) -> bool {
    schema.partition_keys.iter().any(|k| k.name == column)
        || schema.clustering_keys.iter().any(|k| k.name == column)
}

/// Returns true if this mutation contributes at least one static-column operation.
fn has_static_operation(mutation: &Mutation, schema: &TableSchema) -> bool {
    mutation
        .operations
        .iter()
        .any(|op| is_static_operation(op, schema))
}

/// Collect and merge static-column operations from all mutations in a partition.
///
/// Scans every mutation (regardless of whether it has a clustering key) and
/// collects operations that target static columns.  Last-write-wins by
/// `timestamp_micros` when the same column is written more than once.
///
/// Mutations at or before `shadow_floor` (the partition tombstone's deletion
/// timestamp) are skipped: their static cells are shadowed and an sstable
/// must be internally reconciled (see `DataWriter::write_partition`).
///
/// Returns the merged operations in an unspecified order (the writer will
/// sort them by schema column order when building the row body). Each op
/// carries the originating mutation's timestamp and explicit local deletion
/// time (Issue #764) so a surviving older static delete keeps its own LDT
/// instead of inheriting the newest static mutation's value.
fn collect_static_operations(
    mutations: &[Mutation],
    schema: &TableSchema,
    shadow_floor: Option<i64>,
) -> Vec<StaticMergedOp> {
    use std::collections::HashMap;

    // Map: column_name → winning StaticMergedOp (last-write-wins by timestamp).
    let mut best: HashMap<String, StaticMergedOp> = HashMap::new();

    for mutation in mutations {
        if shadow_floor.is_some_and(|floor| mutation.timestamp_micros <= floor) {
            continue;
        }
        for op in &mutation.operations {
            if !is_static_operation(op, schema) {
                continue;
            }
            let col_name = match op {
                crate::storage::write_engine::mutation::CellOperation::Write { column, .. }
                | crate::storage::write_engine::mutation::CellOperation::WriteWithTtl {
                    column,
                    ..
                }
                | crate::storage::write_engine::mutation::CellOperation::Delete { column } => {
                    column.clone()
                }
                // Per-element complex ops (epic #899) are not produced for STATIC
                // complex columns by the (Phase B) capability — they flow through
                // the regular-row per-element path. Skip them here defensively.
                crate::storage::write_engine::mutation::CellOperation::WriteComplexElement {
                    ..
                }
                | crate::storage::write_engine::mutation::CellOperation::ComplexDeletion {
                    ..
                } => continue,
                crate::storage::write_engine::mutation::CellOperation::DeleteRow => continue,
            };
            let candidate = StaticMergedOp {
                op: op.clone(),
                timestamp_micros: mutation.timestamp_micros,
                cell_local_deletion_time: mutation.effective_local_deletion_time(),
            };
            match best.entry(col_name) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(candidate);
                }
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    if candidate.timestamp_micros >= entry.get().timestamp_micros {
                        entry.insert(candidate);
                    }
                }
            }
        }
    }

    best.into_values().collect()
}

/// Whether a range tombstone's clustering range covers the given clustering key.
fn range_tombstone_covers(
    rt: &RangeTombstone,
    clustering_key: Option<&ClusteringKey>,
    schema: &TableSchema,
) -> bool {
    use std::cmp::Ordering;

    let Some(ck) = clustering_key else {
        return false;
    };
    let cmp = |bound: &ClusteringKey| ck.compare(bound, schema).unwrap_or_else(|_| ck.cmp(bound));

    let after_start = match &rt.start {
        ClusteringBound::Inclusive(b) => cmp(b) != Ordering::Less,
        ClusteringBound::Exclusive(b) => cmp(b) == Ordering::Greater,
        ClusteringBound::Bottom => true,
        ClusteringBound::Top => false,
    };
    let before_end = match &rt.end {
        ClusteringBound::Inclusive(b) => cmp(b) != Ordering::Greater,
        ClusteringBound::Exclusive(b) => cmp(b) == Ordering::Less,
        ClusteringBound::Top => true,
        ClusteringBound::Bottom => false,
    };
    after_start && before_end
}

/// Serialize value for clustering key (type-specific encoding)
///
/// Fixed-width types: raw bytes (no length prefix)
/// Variable-width types: VInt length + bytes
fn serialize_value_for_clustering(value: &Value, comparator: &ComparatorType) -> Result<Vec<u8>> {
    match (value, comparator) {
        // Fixed-width types (no length prefix)
        (Value::Boolean(b), ComparatorType::Boolean) => Ok(vec![if *b { 1 } else { 0 }]),
        (Value::TinyInt(n), ComparatorType::TinyInt) => Ok(n.to_be_bytes().to_vec()),
        (Value::SmallInt(n), ComparatorType::SmallInt) => Ok(n.to_be_bytes().to_vec()),
        (Value::Integer(n), ComparatorType::Int) => Ok(n.to_be_bytes().to_vec()),
        (Value::BigInt(n), ComparatorType::BigInt) => Ok(n.to_be_bytes().to_vec()),
        (Value::Float32(f), ComparatorType::Float32) => Ok(f.to_bits().to_be_bytes().to_vec()),
        (Value::Float(f), ComparatorType::Float) => Ok(f.to_bits().to_be_bytes().to_vec()),
        (Value::Timestamp(millis), ComparatorType::Timestamp) => Ok(millis.to_be_bytes().to_vec()),
        (Value::Date(days), ComparatorType::Date) => {
            // Cassandra DATE in clustering keys: stored as unsigned int with Integer.MIN_VALUE offset
            let stored = days.wrapping_sub(i32::MIN) as u32;
            let mut result = Vec::new();
            encode_unsigned(4, &mut result);
            result.extend_from_slice(&stored.to_be_bytes());
            Ok(result)
        }
        (Value::Uuid(bytes), ComparatorType::Uuid) => Ok(bytes.to_vec()),

        // Variable-width types (VInt length + bytes)
        (Value::Text(s), ComparatorType::Text) => {
            let bytes = s.as_bytes();
            let mut result = Vec::new();
            encode_unsigned(bytes.len() as u64, &mut result);
            result.extend_from_slice(bytes);
            Ok(result)
        }
        (Value::Blob(bytes), ComparatorType::Blob) => {
            let mut result = Vec::new();
            encode_unsigned(bytes.len() as u64, &mut result);
            result.extend_from_slice(bytes);
            Ok(result)
        }

        // Frozen collections as clustering keys: serialize the full collection bytes with VInt length prefix
        (Value::Frozen(inner), _) => {
            let bytes = serialize_value(inner)?;
            let mut result = Vec::new();
            encode_unsigned(bytes.len() as u64, &mut result);
            result.extend_from_slice(&bytes);
            Ok(result)
        }

        _ => Err(Error::InvalidInput(format!(
            "Type mismatch or unsupported clustering type: value={:?}, comparator={:?}",
            value, comparator
        ))),
    }
}

/// Serialize a `ClusteringKey` as a Cassandra `ClusteringPrefix` byte sequence.
///
/// Format (same as the clustering prefix written in Data.db rows):
/// ```text
/// [header: unsigned VInt]   ← 2 bits per column: 00=present, 10=null
/// [value bytes…]            ← type-specific bytes for each PRESENT column
/// ```
///
/// Returns `Err` if a clustering column type is unknown; the caller falls back
/// to `[0x00]` (empty header VInt, valid for "no columns") in that case.
pub(super) fn serialize_clustering_prefix_to_vec(
    clustering_key: &ClusteringKey,
    schema: &TableSchema,
) -> Result<Vec<u8>> {
    let mut header = 0u64;
    for (i, (_, value)) in clustering_key.columns.iter().enumerate() {
        let state: u64 = match value {
            Value::Null => 2, // NULL
            _ => 0,           // PRESENT
        };
        header |= state << (i * 2);
    }

    let mut buf: Vec<u8> = Vec::new();
    encode_unsigned(header, &mut buf);

    for (i, (_, value)) in clustering_key.columns.iter().enumerate() {
        if !matches!(value, Value::Null) {
            if i >= schema.clustering_keys.len() {
                return Err(crate::error::Error::Schema(format!(
                    "Clustering key index {} out of range (schema has {})",
                    i,
                    schema.clustering_keys.len()
                )));
            }
            let cluster_col = &schema.clustering_keys[i];
            let comparator = ComparatorType::from_data_type(&cluster_col.data_type)?;
            let value_bytes = serialize_value_for_clustering(value, &comparator)?;
            buf.extend_from_slice(&value_bytes);
        }
    }

    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{
        ClusteringColumn, ClusteringOrder, Column, CqlType, KeyColumn, TableSchema,
    };
    use crate::storage::serialization::types::TypeSerializer;
    use crate::storage::write_engine::mutation::{
        CellOperation, ClusteringKey, PartitionKey, TableId,
    };
    use crate::types::UdtValue;
    use std::collections::HashMap;

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
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "age".to_string(),
                    data_type: "int".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
        }
    }

    fn create_test_stats() -> StatisticsMetadata {
        let mut stats = StatisticsMetadata::new();
        stats.min_timestamp = 1000000;
        stats.min_ttl = 0;
        stats.min_local_deletion_time = 0;
        stats
    }

    /// Schema with a clustering column: id (pk) / ck (clustering) / v (regular).
    fn clustering_test_schema() -> TableSchema {
        TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
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
                    name: "id".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "ck".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
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
        }
    }

    fn op_columns(row: &RowWrite<'_>) -> Vec<String> {
        row.ops
            .iter()
            .filter_map(|m| match m.op {
                CellOperation::Write { column, .. }
                | CellOperation::WriteWithTtl { column, .. }
                | CellOperation::Delete { column }
                | CellOperation::WriteComplexElement { column, .. }
                | CellOperation::ComplexDeletion { column, .. } => Some(column.clone()),
                CellOperation::DeleteRow => None,
            })
            .collect()
    }

    /// Regression for #857. In the compaction path, `merge_entry_to_mutation`
    /// turns the retained clustering cell into a `Write` op, so the merged mutation
    /// carries the clustering column in BOTH `clustering_key` AND `operations`.
    /// `merge_row_group` must drop primary-key (partition + clustering) columns from
    /// `RowWrite.ops`; otherwise the writer emits the clustering value a second time
    /// as a phantom regular cell, which:
    ///   - corrupts the row body for Cassandra's reader (CorruptSSTableException at
    ///     Columns$Serializer.deserializeSubset), and
    ///   - desyncs HAS_ALL_COLUMNS (ops.len() != regular_column_count).
    #[test]
    fn merge_row_group_excludes_primary_key_columns_from_ops() {
        let schema = clustering_test_schema();

        // Exactly the shape merge_entry_to_mutation produces for a compacted row.
        let mutation = Mutation::new(
            TableId::new("test_ks", "test_table"),
            PartitionKey::single("id", Value::Integer(1)),
            Some(ClusteringKey::single("ck", Value::Integer(7))),
            vec![
                CellOperation::Write {
                    column: "ck".to_string(),
                    value: Value::Integer(7),
                },
                CellOperation::Write {
                    column: "v".to_string(),
                    value: Value::Text("hello".to_string()),
                },
            ],
            2000,
            None,
        );

        let row = DataWriter::merge_row_group(&[&mutation], &schema, false, None)
            .expect("row group must produce a row");

        let cols = op_columns(&row);
        assert!(
            !cols.iter().any(|c| c == "ck"),
            "clustering column 'ck' must not appear as a cell op (#857); got {cols:?}"
        );
        assert!(
            cols.iter().any(|c| c == "v"),
            "regular column 'v' must be present; got {cols:?}"
        );
        assert_eq!(
            cols.len(),
            1,
            "only the single regular column should remain as a cell op; got {cols:?}"
        );
    }

    /// A partition-key column accidentally present in `operations` must also be
    /// dropped from the row ops (defends the same invariant for the pk).
    #[test]
    fn merge_row_group_excludes_partition_key_column_from_ops() {
        let schema = clustering_test_schema();
        let mutation = Mutation::new(
            TableId::new("test_ks", "test_table"),
            PartitionKey::single("id", Value::Integer(1)),
            Some(ClusteringKey::single("ck", Value::Integer(7))),
            vec![
                CellOperation::Write {
                    column: "id".to_string(),
                    value: Value::Integer(1),
                },
                CellOperation::Write {
                    column: "v".to_string(),
                    value: Value::Text("hello".to_string()),
                },
            ],
            2000,
            None,
        );

        let row = DataWriter::merge_row_group(&[&mutation], &schema, false, None)
            .expect("row group must produce a row");
        let cols = op_columns(&row);
        assert_eq!(
            cols,
            vec!["v".to_string()],
            "partition-key column 'id' must not appear as a cell op; got {cols:?}"
        );
    }

    /// Direct (non-compaction) mutations never put key columns in `operations`, so
    /// the filter must be a no-op for them — guards against over-filtering.
    #[test]
    fn merge_row_group_keeps_all_regular_ops_for_direct_mutation() {
        let schema = clustering_test_schema();
        let mutation = Mutation::new(
            TableId::new("test_ks", "test_table"),
            PartitionKey::single("id", Value::Integer(1)),
            Some(ClusteringKey::single("ck", Value::Integer(7))),
            vec![CellOperation::Write {
                column: "v".to_string(),
                value: Value::Text("hello".to_string()),
            }],
            2000,
            None,
        );

        let row = DataWriter::merge_row_group(&[&mutation], &schema, false, None)
            .expect("row group must produce a row");
        assert_eq!(op_columns(&row), vec!["v".to_string()]);
    }

    /// A row whose only cells are primary-key columns (a pure primary-key row,
    /// e.g. `INSERT INTO t (id, ck) VALUES (...)`) must SURVIVE compaction with its
    /// liveness intact even though the key columns are dropped from the cells. The
    /// key-column write still signals liveness, so the row is emitted (no cells).
    /// Without that, filtering would silently drop such rows.
    #[test]
    fn merge_row_group_keeps_pure_primary_key_row_alive() {
        let schema = clustering_test_schema();
        let mutation = Mutation::new(
            TableId::new("test_ks", "test_table"),
            PartitionKey::single("id", Value::Integer(1)),
            Some(ClusteringKey::single("ck", Value::Integer(7))),
            // Only the clustering column is present as an op (as the compaction path
            // produces for a row that has no regular columns set).
            vec![CellOperation::Write {
                column: "ck".to_string(),
                value: Value::Integer(7),
            }],
            2000,
            None,
        );

        let row = DataWriter::merge_row_group(&[&mutation], &schema, false, None)
            .expect("a pure primary-key row must not be dropped");
        assert!(
            op_columns(&row).is_empty(),
            "no regular cells for a pure primary-key row; got {:?}",
            op_columns(&row)
        );
        assert_eq!(
            row.liveness_ts,
            Some(2000),
            "pure primary-key row must keep its liveness timestamp"
        );
    }

    fn phase3_address_schema() -> UdtTypeDef {
        UdtTypeDef::new("test_ks".to_string(), "address".to_string())
            .with_field("street".to_string(), CqlType::Text, true)
            .with_field("city".to_string(), CqlType::Text, true)
    }

    fn phase3_person_schema() -> UdtTypeDef {
        UdtTypeDef::new("test_ks".to_string(), "person".to_string())
            .with_field("name".to_string(), CqlType::Text, true)
            .with_field(
                "phone_numbers".to_string(),
                CqlType::List(Box::new(CqlType::Frozen(Box::new(CqlType::Udt(
                    "phone_number".to_string(),
                    vec![],
                ))))),
                true,
            )
            .with_field(
                "home_address".to_string(),
                CqlType::Frozen(Box::new(CqlType::Udt("address".to_string(), vec![]))),
                true,
            )
    }

    fn phase3_company_schema() -> UdtTypeDef {
        UdtTypeDef::new("test_ks".to_string(), "company".to_string())
            .with_field("name".to_string(), CqlType::Text, true)
            .with_field(
                "employees".to_string(),
                CqlType::List(Box::new(CqlType::Frozen(Box::new(CqlType::Udt(
                    "person".to_string(),
                    vec![],
                ))))),
                true,
            )
            .with_field(
                "departments".to_string(),
                CqlType::Map(
                    Box::new(CqlType::Text),
                    Box::new(CqlType::Frozen(Box::new(CqlType::List(Box::new(
                        CqlType::Frozen(Box::new(CqlType::Udt("person".to_string(), vec![]))),
                    ))))),
                ),
                true,
            )
    }

    fn phase3_address_value() -> UdtValue {
        UdtValue::new("address".to_string(), "test_ks".to_string())
            .with_field(
                "street".to_string(),
                Some(Value::Text("Main St".to_string())),
            )
            .with_field("city".to_string(), Some(Value::Text("Seattle".to_string())))
    }

    fn phase3_phone_value() -> UdtValue {
        UdtValue::new("phone_number".to_string(), "test_ks".to_string())
            .with_field("label".to_string(), Some(Value::Text("mobile".to_string())))
            .with_field(
                "number".to_string(),
                Some(Value::Text("+1-555-0101".to_string())),
            )
    }

    fn phase3_person_value(name: &str) -> UdtValue {
        UdtValue::new("person".to_string(), "test_ks".to_string())
            .with_field("name".to_string(), Some(Value::Text(name.to_string())))
            .with_field(
                "phone_numbers".to_string(),
                Some(Value::List(vec![Value::Frozen(Box::new(Value::Udt(
                    phase3_phone_value(),
                )))])),
            )
            .with_field(
                "home_address".to_string(),
                Some(Value::Frozen(Box::new(Value::Udt(phase3_address_value())))),
            )
    }

    fn phase3_company_value() -> UdtValue {
        let person = phase3_person_value("Alice");
        UdtValue::new("company".to_string(), "test_ks".to_string())
            .with_field("name".to_string(), Some(Value::Text("Acme".to_string())))
            .with_field(
                "employees".to_string(),
                Some(Value::List(vec![Value::Frozen(Box::new(Value::Udt(
                    person.clone(),
                )))])),
            )
            .with_field(
                "departments".to_string(),
                Some(Value::Map(vec![(
                    Value::Text("platform".to_string()),
                    Value::Frozen(Box::new(Value::List(vec![Value::Frozen(Box::new(
                        Value::Udt(person),
                    ))]))),
                )])),
            )
    }

    fn create_static_test_schema() -> TableSchema {
        TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
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
                    name: "static_val".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: true,
                },
                Column {
                    name: "regular_val".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
        }
    }

    #[test]
    fn test_data_writer_new() {
        let stats = create_test_stats();
        let writer = DataWriter::new(stats);
        assert_eq!(writer.position(), 0);
    }

    #[test]
    fn test_write_partition_header() {
        let stats = create_test_stats();
        let mut writer = DataWriter::new(stats);

        let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]); // int = 42
        writer.write_partition_header(&key, None).unwrap();

        let bytes = writer.finish().unwrap();

        // Verify structure (Cassandra BigFormat):
        // [0x00, 0x04] key length (u16 BE = 4 bytes)
        // [0x00, 0x00, 0x00, 0x2A] key bytes
        // [0x7F, 0xFF, 0xFF, 0xFF] DeletionTime.LIVE local_deletion_time (i32::MAX)
        // [0x80, 0x00...] DeletionTime.LIVE deletion_timestamp (i64::MIN)
        assert_eq!(&bytes[0..2], &[0x00, 0x04]); // key length (u16 BE)
        assert_eq!(&bytes[2..6], &[0x00, 0x00, 0x00, 0x2A]); // key bytes
        assert_eq!(&bytes[6..10], &i32::MAX.to_be_bytes()); // DeletionTime.LIVE ldt
        assert_eq!(&bytes[10..18], &i64::MIN.to_be_bytes()); // DeletionTime.LIVE ts
    }

    #[test]
    fn test_write_simple_row() {
        let stats = create_test_stats();
        let mut writer = DataWriter::new(stats);
        let schema = create_test_schema();

        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let mutation = Mutation::new(
            table_id,
            pk,
            None,
            vec![
                CellOperation::Write {
                    column: "name".to_string(),
                    value: Value::Text("Alice".to_string()),
                },
                CellOperation::Write {
                    column: "age".to_string(),
                    value: Value::Integer(30),
                },
            ],
            1001000, // timestamp (delta = 1000)
            None,
        );

        writer.write_row(&mutation, &schema).unwrap();

        let bytes = writer.finish().unwrap();
        assert!(!bytes.is_empty());

        // Verify row flags
        let flags = bytes[0];
        assert_eq!(
            flags & ROW_HAS_TIMESTAMP,
            ROW_HAS_TIMESTAMP,
            "Should have timestamp"
        );
        assert_eq!(
            flags & ROW_HAS_ALL_COLUMNS,
            ROW_HAS_ALL_COLUMNS,
            "Should have all columns"
        );
    }

    #[test]
    fn test_write_row_with_clustering() {
        let mut schema = create_test_schema();
        schema.clustering_keys = vec![ClusteringColumn {
            name: "ts".to_string(),
            data_type: "timestamp".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }];

        let stats = create_test_stats();
        let mut writer = DataWriter::new(stats);

        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let ck = ClusteringKey::single("ts", Value::Timestamp(1234567890));
        let mutation = Mutation::new(
            table_id,
            pk,
            Some(ck),
            vec![CellOperation::Write {
                column: "name".to_string(),
                value: Value::Text("Bob".to_string()),
            }],
            1001000,
            None,
        );

        writer.write_row(&mutation, &schema).unwrap();

        let bytes = writer.finish().unwrap();
        assert!(!bytes.is_empty());

        // Verify row has flags and clustering prefix
        let flags = bytes[0];
        assert_eq!(flags & ROW_HAS_TIMESTAMP, ROW_HAS_TIMESTAMP);
    }

    #[test]
    fn test_write_partition_complete() {
        let stats = create_test_stats();
        let mut writer = DataWriter::new(stats);
        let schema = create_test_schema();

        let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x01]);
        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(1));

        let mutations = vec![
            Mutation::new(
                table_id.clone(),
                pk.clone(),
                None,
                vec![CellOperation::Write {
                    column: "name".to_string(),
                    value: Value::Text("Alice".to_string()),
                }],
                1001000,
                None,
            ),
            Mutation::new(
                table_id,
                pk,
                None,
                vec![CellOperation::Write {
                    column: "name".to_string(),
                    value: Value::Text("Bob".to_string()),
                }],
                1002000,
                None,
            ),
        ];

        let offset = writer
            .write_partition(&key, &mutations, &schema, None, &[])
            .unwrap();
        assert_eq!(offset, 0); // First partition starts at offset 0

        let bytes = writer.finish().unwrap();
        assert!(!bytes.is_empty());

        // Verify end-of-partition marker is present
        assert_eq!(bytes[bytes.len() - 1], END_OF_PARTITION);
    }

    /// Regression test for bug #644 (S6): temporal deltas MUST use unsigned VInt.
    ///
    /// The writer previously used ZigZag-encoded signed VInt (`encode_signed`) for
    /// all row-header temporal deltas (timestamp, TTL, LDT).  ZigZag maps positive
    /// integer n → 2n, so a delta of 5000 would be encoded as 10000, which the
    /// reader (fixed in S1, using `parse_vuint` = unsigned VInt) would decode as
    /// 10000 — doubling every timestamp on readback.
    ///
    /// Per Cassandra `SerializationHeader.java:167`:
    ///   `out.writeUnsignedVInt(timestamp - stats.minTimestamp)`
    ///   `out.writeUnsignedVInt(ttl - stats.minTTL)`
    ///   `out.writeUnsignedVInt(localDeletionTime - stats.minLocalDeletionTime)`
    ///
    /// Expected encodings (2-byte unsigned VInt, Cassandra format: leading 1-bits + data):
    ///   unsigned VInt(5000 = 0x1388):
    ///     extra_bytes=1, first=(0x80 | (0x1388>>8)&0x3F)=0x93, second=0x88  → [0x93, 0x88]
    ///     ZigZag(5000)=10000 would give [0xA7, 0x10]  ← WRONG (pre-fix value)
    ///
    ///   unsigned VInt(3600 = 0x0E10):
    ///     extra_bytes=1, first=(0x80 | (0x0E10>>8)&0x3F)=0x8E, second=0x10  → [0x8E, 0x10]
    ///     ZigZag(3600)=7200 would give [0x9C, 0x20]  ← WRONG (pre-fix value)
    #[test]
    fn test_delta_encoding_unsigned_vint_fix_644() {
        let mut stats = create_test_stats();
        stats.min_timestamp = 1_000_000;
        stats.min_ttl = 3_600;
        stats.min_local_deletion_time = 0;

        let writer = DataWriter::new(stats.clone());
        let schema = create_test_schema();

        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let mutation = Mutation::new(
            table_id,
            pk,
            None,
            vec![CellOperation::Write {
                column: "name".to_string(),
                value: Value::Text("Test".to_string()),
            }],
            1_005_000,  // timestamp_micros; delta from min_timestamp(1_000_000) = 5_000
            Some(7200), // ttl; delta from min_ttl(3_600) = 3_600
        );

        let row_body = writer
            .build_row_body(&mutation, &schema, ROW_HAS_TIMESTAMP | ROW_HAS_TTL)
            .unwrap();
        assert!(!row_body.is_empty(), "row body must be non-empty");

        // The row body for HAS_TIMESTAMP | HAS_TTL starts with:
        //   [0..2] timestamp delta as unsigned VInt
        //   [2..4] ttl delta as unsigned VInt
        //   [4..]  ldt delta as unsigned VInt (time-dependent, not asserted)
        //   ...    column bitmap, cells
        //
        // timestamp_delta = 5000 → unsigned VInt = [0x93, 0x88]
        // ZigZag(5000) = 10000 → would give [0xA7, 0x10]  ← OLD/WRONG pre-fix encoding
        assert_eq!(
            &row_body[0..2],
            &[0x93u8, 0x88u8],
            "Fix #644: timestamp delta=5000 must encode as unsigned VInt [0x93, 0x88], \
             not ZigZag [0xA7, 0x10]. Reader uses parse_vuint (unsigned), so ZigZag would \
             double the delta on readback (5000 → decoded as 10000)."
        );

        // ttl_delta = 7200 - 3600 = 3600 → unsigned VInt = [0x8E, 0x10]
        // ZigZag(3600) = 7200 → would give [0x9C, 0x20]  ← OLD/WRONG pre-fix encoding
        assert_eq!(
            &row_body[2..4],
            &[0x8Eu8, 0x10u8],
            "Fix #644: TTL delta=3600 must encode as unsigned VInt [0x8E, 0x10], \
             not ZigZag [0x9C, 0x20]. This is the first of two HAS_TTL fields."
        );
    }

    #[test]
    fn test_delta_encoding() {
        let mut stats = create_test_stats();
        stats.min_timestamp = 1000000;
        stats.min_ttl = 3600;

        let writer = DataWriter::new(stats.clone());
        let schema = create_test_schema();

        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let mutation = Mutation::new(
            table_id,
            pk,
            None,
            vec![CellOperation::Write {
                column: "name".to_string(),
                value: Value::Text("Test".to_string()),
            }],
            1005000,    // timestamp (delta = 5000)
            Some(7200), // TTL (delta = 3600)
        );

        let row_body = writer
            .build_row_body(&mutation, &schema, ROW_HAS_TIMESTAMP | ROW_HAS_TTL)
            .unwrap();
        assert!(!row_body.is_empty());
    }

    #[test]
    fn test_serialize_value_types() {
        // Boolean
        let bytes = serialize_value(&Value::Boolean(true)).unwrap();
        assert_eq!(bytes, vec![1]);

        // Integer
        let bytes = serialize_value(&Value::Integer(42)).unwrap();
        assert_eq!(bytes, vec![0x00, 0x00, 0x00, 0x2A]);

        // Text
        let bytes = serialize_value(&Value::Text("hello".to_string())).unwrap();
        assert_eq!(bytes, b"hello");

        // BigInt
        let bytes = serialize_value(&Value::BigInt(9223372036854775807)).unwrap();
        assert_eq!(bytes, vec![0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);

        // Null
        let bytes = serialize_value(&Value::Null).unwrap();
        assert_eq!(bytes, Vec::<u8>::new());
    }

    #[test]
    fn test_column_bitmap() {
        let stats = create_test_stats();
        let writer = DataWriter::new(stats);
        let schema = create_test_schema();

        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(1));

        // Only write "name" column (not "age")
        // Schema has 2 regular columns sorted alphabetically: [age(0), name(1)]
        // "age" is MISSING → bitmap bit 0 set → bitmap = 0b01 = 1
        let mutation = Mutation::new(
            table_id,
            pk,
            None,
            vec![CellOperation::Write {
                column: "name".to_string(),
                value: Value::Text("Alice".to_string()),
            }],
            1001000,
            None,
        );

        let mut buf = Vec::new();
        writer
            .write_column_bitmap(&mut buf, &mutation, &schema)
            .unwrap();

        // Cassandra format: single VUInt of missing columns bitmask
        // "age" (index 0) is missing → bitmap = 0x01
        assert_eq!(buf, vec![0x01]);
    }

    #[test]
    fn test_partition_key_size_limit() {
        let stats = create_test_stats();
        let mut writer = DataWriter::new(stats);

        // 256 bytes should succeed (u16 allows up to 65535)
        let key_256 = vec![0xFF; 256];
        let key = DecoratedKey::new(12345, key_256);
        let result = writer.write_partition_header(&key, None);
        assert!(result.is_ok());

        // Create a partition key larger than 65535 bytes
        let mut writer2 = DataWriter::new(create_test_stats());
        let large_key = vec![0xFF; 65536];
        let key = DecoratedKey::new(12345, large_key);

        let result = writer2.write_partition_header(&key, None);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too large"));
    }

    #[test]
    fn test_write_tombstone_cell() {
        let mut stats = create_test_stats();
        stats.min_timestamp = 1000000;
        stats.min_local_deletion_time = 1700000000; // Jan 2023
        let writer = DataWriter::new(stats);

        let mut buf = Vec::new();
        let timestamp = 1001000; // delta = 1000
        let local_deletion_time = 1700000010; // delta = 10
        writer
            .write_tombstone_cell(&mut buf, "deleted_col", timestamp, local_deletion_time)
            .unwrap();

        assert!(!buf.is_empty());
        // First byte should be tombstone flags (only IS_DELETED, no USE_ROW_TIMESTAMP)
        let flags = buf[0];
        assert_eq!(
            flags & CELL_IS_DELETED,
            CELL_IS_DELETED,
            "Should have IS_DELETED flag"
        );
        assert_eq!(
            flags & CELL_USE_ROW_TIMESTAMP,
            0,
            "Should NOT have USE_ROW_TIMESTAMP flag"
        );

        // Should have timestamp delta and local_deletion_time delta encoded as VInts
        assert!(
            buf.len() > 1,
            "Should have timestamp and deletion_time deltas"
        );
    }

    #[test]
    fn test_serialize_clustering_value_fixed_width() {
        // Integer (fixed-width, no length prefix)
        let bytes =
            serialize_value_for_clustering(&Value::Integer(42), &ComparatorType::Int).unwrap();
        assert_eq!(bytes, vec![0x00, 0x00, 0x00, 0x2A]);

        // BigInt (fixed-width)
        let bytes =
            serialize_value_for_clustering(&Value::BigInt(1000), &ComparatorType::BigInt).unwrap();
        assert_eq!(bytes, vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xE8]);
    }

    #[test]
    fn test_serialize_clustering_value_variable_width() {
        // Text (variable-width, VInt length prefix)
        let bytes =
            serialize_value_for_clustering(&Value::Text("test".to_string()), &ComparatorType::Text)
                .unwrap();
        assert!(!bytes.is_empty());
        // First byte(s) should be VInt length (4), followed by "test"
        // VInt(4) = 0x04, then "test"
        assert_eq!(bytes[0], 0x04); // VInt length = 4
        assert_eq!(&bytes[1..], b"test");
    }

    #[test]
    fn test_serialize_clustering_date_includes_length_prefix() {
        let bytes = serialize_value_for_clustering(&Value::Date(0), &ComparatorType::Date).unwrap();
        assert_eq!(
            bytes[0], 0x04,
            "date clustering values should be length-prefixed"
        );
        assert_eq!(
            bytes.len(),
            5,
            "date clustering value should be 1-byte length + 4-byte payload"
        );
    }

    #[test]
    fn test_serialize_clustering_frozen_list_text() {
        let value = Value::Frozen(Box::new(Value::List(vec![Value::Text("solo".to_string())])));
        let comparator = ComparatorType::Frozen(Box::new(ComparatorType::List(Box::new(
            ComparatorType::Text,
        ))));

        let bytes = serialize_value_for_clustering(&value, &comparator).unwrap();
        let expected_inner =
            serialize_value(&Value::List(vec![Value::Text("solo".to_string())])).unwrap();

        let mut expected = vec![expected_inner.len() as u8];
        expected.extend_from_slice(&expected_inner);

        assert_eq!(bytes, expected);
    }

    #[test]
    fn test_null_vs_empty_string() {
        let stats = create_test_stats();
        let writer = DataWriter::new(stats);

        // Test NULL - should not be written as a cell
        let result = writer.write_cell(&mut Vec::new(), "test_col", &Value::Null, 1001000);
        assert!(result.is_err(), "NULL values should return error");
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("NULL values should not be written"));

        // Test empty string - should have HAS_EMPTY_VALUE flag
        let mut buf = Vec::new();
        writer
            .write_cell(&mut buf, "test_col", &Value::Text(String::new()), 1001000)
            .unwrap();

        assert!(!buf.is_empty());
        let flags = buf[0];
        assert_eq!(
            flags & CELL_HAS_EMPTY_VALUE,
            CELL_HAS_EMPTY_VALUE,
            "Empty string should have HAS_EMPTY_VALUE flag"
        );

        // Test non-empty string - should NOT have HAS_EMPTY_VALUE flag
        let mut buf2 = Vec::new();
        writer
            .write_cell(
                &mut buf2,
                "test_col",
                &Value::Text("test".to_string()),
                1001000,
            )
            .unwrap();

        let flags2 = buf2[0];
        assert_eq!(
            flags2 & CELL_HAS_EMPTY_VALUE,
            0,
            "Non-empty string should NOT have HAS_EMPTY_VALUE flag"
        );

        assert_eq!(buf, vec![CELL_USE_ROW_TIMESTAMP | CELL_HAS_EMPTY_VALUE]);
    }

    #[test]
    fn test_fixed_width_cell_omits_length_prefix() {
        let stats = create_test_stats();
        let writer = DataWriter::new(stats);
        let mut buf = Vec::new();

        writer
            .write_cell(&mut buf, "value", &Value::Integer(42), 1001000)
            .unwrap();

        assert_eq!(buf, vec![CELL_USE_ROW_TIMESTAMP, 0x00, 0x00, 0x00, 0x2A]);
    }

    #[test]
    fn test_variable_width_cell_keeps_length_prefix() {
        let stats = create_test_stats();
        let writer = DataWriter::new(stats);
        let mut buf = Vec::new();

        writer
            .write_cell(&mut buf, "value", &Value::Text("abc".to_string()), 1001000)
            .unwrap();

        assert_eq!(buf, vec![CELL_USE_ROW_TIMESTAMP, 0x03, b'a', b'b', b'c']);
    }

    #[test]
    fn test_value_length_bounds_check() {
        let stats = create_test_stats();
        let writer = DataWriter::new(stats);

        // Create a value that exceeds i64::MAX (simulated via the check)
        // Since we can't actually allocate > i64::MAX bytes, we test the logic path
        // by checking that reasonable values pass
        let mut buf = Vec::new();
        let large_text = "x".repeat(1000);
        let result = writer.write_cell(&mut buf, "test_col", &Value::Text(large_text), 1001000);
        assert!(result.is_ok(), "Reasonable-sized values should succeed");
    }

    #[test]
    fn test_tombstone_requires_deletion_time() {
        let mut stats = create_test_stats();
        stats.min_timestamp = 1000000;
        stats.min_local_deletion_time = 1700000000;
        let writer = DataWriter::new(stats);

        let mut buf = Vec::new();

        // Test with valid deletion_time > min_local_deletion_time
        let result = writer.write_tombstone_cell(
            &mut buf,
            "deleted_col",
            1001000,
            1700000010, // Greater than min
        );
        assert!(result.is_ok(), "Valid deletion_time should succeed");

        // Test with deletion_time < min_local_deletion_time (should error)
        let mut buf2 = Vec::new();
        let result2 = writer.write_tombstone_cell(
            &mut buf2,
            "deleted_col",
            1001000,
            1600000000, // Less than min
        );
        assert!(result2.is_err(), "deletion_time < min should fail");
        assert!(result2
            .unwrap_err()
            .to_string()
            .contains("less than min_local_deletion_time"));
    }

    #[test]
    fn test_column_bitmap_skips_nulls() {
        let stats = create_test_stats();
        let writer = DataWriter::new(stats);
        let schema = create_test_schema();

        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(1));

        // Write "name" with value, "age" with NULL
        // Schema has 2 regular columns sorted alphabetically: [age(0), name(1)]
        // "age" is NULL (missing) → bit 0 = 1
        // "name" is present → bit 1 = 0
        // bitmap = 0b01 = 0x01
        let mutation = Mutation::new(
            table_id,
            pk,
            None,
            vec![
                CellOperation::Write {
                    column: "name".to_string(),
                    value: Value::Text("Alice".to_string()),
                },
                CellOperation::Write {
                    column: "age".to_string(),
                    value: Value::Null,
                },
            ],
            1001000,
            None,
        );

        let mut buf = Vec::new();
        writer
            .write_column_bitmap(&mut buf, &mutation, &schema)
            .unwrap();

        // Cassandra format: single VUInt bitmask where bit=1 means MISSING
        // Only "age" (index 0) is missing → bitmap = 0x01
        assert_eq!(
            buf,
            vec![0x01],
            "Bitmap should encode age as missing (bit 0)"
        );
    }

    #[test]
    fn test_row_with_null_values() {
        let stats = create_test_stats();
        let mut writer = DataWriter::new(stats);
        let schema = create_test_schema();

        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let mutation = Mutation::new(
            table_id,
            pk,
            None,
            vec![
                CellOperation::Write {
                    column: "name".to_string(),
                    value: Value::Text("Alice".to_string()),
                },
                CellOperation::Write {
                    column: "age".to_string(),
                    value: Value::Null, // NULL value
                },
            ],
            1001000,
            None,
        );

        writer.write_row(&mutation, &schema).unwrap();

        let bytes = writer.finish().unwrap();
        assert!(!bytes.is_empty());

        // Verify row flags do NOT have HAS_ALL_COLUMNS (because of NULL)
        let flags = bytes[0];
        assert_eq!(
            flags & ROW_HAS_ALL_COLUMNS,
            0,
            "Row with NULL should NOT have HAS_ALL_COLUMNS flag"
        );
    }

    #[test]
    fn test_multiple_partitions() {
        let stats = create_test_stats();
        let mut writer = DataWriter::new(stats);
        let schema = create_test_schema();

        // Write first partition
        let key1 = DecoratedKey::new(100, vec![0x00, 0x00, 0x00, 0x01]);
        let table_id = TableId::new("test_ks", "test_table");
        let pk1 = PartitionKey::single("id", Value::Integer(1));
        let mutations1 = vec![Mutation::new(
            table_id.clone(),
            pk1,
            None,
            vec![CellOperation::Write {
                column: "name".to_string(),
                value: Value::Text("Alice".to_string()),
            }],
            1001000,
            None,
        )];

        let offset1 = writer
            .write_partition(&key1, &mutations1, &schema, None, &[])
            .unwrap();
        assert_eq!(offset1, 0);

        // Write second partition
        let key2 = DecoratedKey::new(200, vec![0x00, 0x00, 0x00, 0x02]);
        let pk2 = PartitionKey::single("id", Value::Integer(2));
        let mutations2 = vec![Mutation::new(
            table_id,
            pk2,
            None,
            vec![CellOperation::Write {
                column: "name".to_string(),
                value: Value::Text("Bob".to_string()),
            }],
            1002000,
            None,
        )];

        let offset2 = writer
            .write_partition(&key2, &mutations2, &schema, None, &[])
            .unwrap();
        assert!(offset2 > offset1); // Second partition starts after first

        let bytes = writer.finish().unwrap();
        assert!(!bytes.is_empty());

        // Both partitions should have end-of-partition markers
        // Note: END_OF_PARTITION (0x01) may appear elsewhere (e.g., in cell flags)
        // For this test, we verify the file structure is valid and both partitions were written
        assert!(
            offset2 > offset1,
            "Second partition should start after first"
        );

        // The last byte should be an END_OF_PARTITION marker
        assert_eq!(
            bytes[bytes.len() - 1],
            END_OF_PARTITION,
            "File should end with END_OF_PARTITION"
        );
    }

    // ========== M5.2 Tombstone Tests ==========

    #[test]
    fn test_row_tombstone() {
        let stats = create_test_stats();
        let mut writer = DataWriter::new(stats);
        let schema = create_test_schema();

        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let mutation = Mutation::new(
            table_id,
            pk,
            None,
            vec![CellOperation::DeleteRow],
            1001000,
            None,
        );

        writer.write_row(&mutation, &schema).unwrap();

        let bytes = writer.finish().unwrap();
        assert!(!bytes.is_empty());

        // Verify row flags have HAS_DELETION
        let flags = bytes[0];
        assert_eq!(
            flags & ROW_HAS_DELETION,
            ROW_HAS_DELETION,
            "Should have HAS_DELETION flag"
        );
        // Issue #717: a pure row tombstone carries no primary-key liveness —
        // Cassandra serializes DELETE-d rows without HAS_TIMESTAMP.
        assert_eq!(
            flags & ROW_HAS_TIMESTAMP,
            0,
            "Pure row tombstone must not have HAS_TIMESTAMP"
        );
        assert_eq!(
            flags & ROW_HAS_ALL_COLUMNS,
            0,
            "Row tombstone must not claim all columns"
        );

        // Issue #717: the columns subset must follow the deletion times.
        // Layout: [flags][row_size][prev_size=0][deletion mfda][deletion ldt][subset]
        // With create_test_stats baselines both deletion deltas and the
        // all-missing subset are single-byte VInts.
        let row_size = bytes[1] as usize;
        // Body = prev_size(1) + mfda(vint) + ldt(vint) + subset(vint ≥ 1 byte)
        assert!(
            row_size >= 4,
            "Row tombstone body must include the columns subset (got row_size={})",
            row_size
        );
        // The final body byte is the all-missing subset bitmask: 2 regular
        // columns (name, value) in create_test_schema → 0b11.
        let body_end = 2 + row_size; // flags + row_size byte + body
        assert_eq!(
            bytes[body_end - 1],
            0b11,
            "Columns subset must mark every regular column missing"
        );
    }

    #[test]
    fn test_partition_tombstone() {
        use crate::storage::write_engine::mutation::PartitionTombstone;

        let stats = create_test_stats();
        let mut writer = DataWriter::new(stats);

        let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]);
        let tombstone = PartitionTombstone {
            deletion_time: 1001000,          // microseconds
            local_deletion_time: 1700000010, // seconds
        };

        writer
            .write_partition_header(&key, Some(&tombstone))
            .unwrap();

        let bytes = writer.finish().unwrap();

        // Verify structure (Cassandra BigFormat):
        // [0x00, 0x04] key length (u16 BE)
        // [key bytes]
        // [local_deletion_time: i32 BE]
        // [deletion_timestamp: i64 BE]
        assert_eq!(&bytes[0..2], &[0x00, 0x04], "Key length (u16 BE)");

        // Check local_deletion_time (i32 BE at offset 6)
        let ldt_bytes = &bytes[6..10];
        let ldt = i32::from_be_bytes([ldt_bytes[0], ldt_bytes[1], ldt_bytes[2], ldt_bytes[3]]);
        assert_eq!(ldt, 1700000010, "Local deletion time should match");

        // Check deletion_timestamp (i64 BE at offset 10)
        let ts_bytes = &bytes[10..18];
        let ts = i64::from_be_bytes([
            ts_bytes[0],
            ts_bytes[1],
            ts_bytes[2],
            ts_bytes[3],
            ts_bytes[4],
            ts_bytes[5],
            ts_bytes[6],
            ts_bytes[7],
        ]);
        assert_eq!(ts, 1001000, "Deletion timestamp should match");
    }

    #[test]
    fn test_range_tombstone_inclusive_bounds() {
        use crate::storage::write_engine::mutation::{ClusteringBound, RangeTombstone};

        let mut schema = create_test_schema();
        schema.clustering_keys = vec![ClusteringColumn {
            name: "ts".to_string(),
            data_type: "timestamp".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }];

        let stats = create_test_stats();
        let mut writer = DataWriter::new(stats);

        let range = RangeTombstone {
            start: ClusteringBound::Inclusive(ClusteringKey::single("ts", Value::Timestamp(1000))),
            end: ClusteringBound::Inclusive(ClusteringKey::single("ts", Value::Timestamp(2000))),
            deletion_time: 1001000,
            local_deletion_time: 1700000010,
        };

        let open_size = writer
            .write_range_bound(
                &range.start,
                true,
                range.deletion_time,
                range.local_deletion_time,
                &schema,
                0,
            )
            .unwrap();
        writer
            .write_range_bound(
                &range.end,
                false,
                range.deletion_time,
                range.local_deletion_time,
                &schema,
                open_size as u64,
            )
            .unwrap();

        let bytes = writer.finish().unwrap();
        assert!(!bytes.is_empty());

        // Verify opening bound: Cassandra ClusteringPrefix.Kind ordinals
        assert_eq!(bytes[0], IS_MARKER, "Should have IS_MARKER flag");
        assert_eq!(
            bytes[1], INCL_START_BOUND,
            "Should have INCL_START_BOUND kind (ordinal 1)"
        );
        // u16 BE cluster count follows the kind byte
        assert_eq!(
            u16::from_be_bytes([bytes[2], bytes[3]]),
            1,
            "Bound carries one clustering value"
        );

        // Closing bound starts right after the opening marker
        assert_eq!(bytes[open_size], IS_MARKER);
        assert_eq!(
            bytes[open_size + 1],
            INCL_END_BOUND,
            "Should have INCL_END_BOUND kind (ordinal 6)"
        );
    }

    #[test]
    fn test_range_tombstone_exclusive_bounds() {
        use crate::storage::write_engine::mutation::{ClusteringBound, RangeTombstone};

        let mut schema = create_test_schema();
        schema.clustering_keys = vec![ClusteringColumn {
            name: "ts".to_string(),
            data_type: "timestamp".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }];

        let stats = create_test_stats();
        let mut writer = DataWriter::new(stats);

        let range = RangeTombstone {
            start: ClusteringBound::Exclusive(ClusteringKey::single("ts", Value::Timestamp(1000))),
            end: ClusteringBound::Exclusive(ClusteringKey::single("ts", Value::Timestamp(2000))),
            deletion_time: 1001000,
            local_deletion_time: 1700000010,
        };

        let open_size = writer
            .write_range_bound(
                &range.start,
                true,
                range.deletion_time,
                range.local_deletion_time,
                &schema,
                0,
            )
            .unwrap();
        writer
            .write_range_bound(
                &range.end,
                false,
                range.deletion_time,
                range.local_deletion_time,
                &schema,
                open_size as u64,
            )
            .unwrap();

        let bytes = writer.finish().unwrap();
        assert!(!bytes.is_empty());

        // Verify opening bound: Cassandra ClusteringPrefix.Kind ordinals
        assert_eq!(bytes[0], IS_MARKER, "Should have IS_MARKER flag");
        assert_eq!(
            bytes[1], EXCL_START_BOUND,
            "Should have EXCL_START_BOUND kind (ordinal 7)"
        );
        assert_eq!(
            bytes[open_size + 1],
            EXCL_END_BOUND,
            "Should have EXCL_END_BOUND kind (ordinal 0)"
        );
    }

    #[test]
    fn test_range_tombstone_bottom_top_bounds() {
        use crate::storage::write_engine::mutation::{ClusteringBound, RangeTombstone};

        let mut schema = create_test_schema();
        schema.clustering_keys = vec![ClusteringColumn {
            name: "ts".to_string(),
            data_type: "timestamp".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }];

        let stats = create_test_stats();
        let mut writer = DataWriter::new(stats);

        // Delete everything from start to end of partition
        let range = RangeTombstone {
            start: ClusteringBound::Bottom,
            end: ClusteringBound::Top,
            deletion_time: 1001000,
            local_deletion_time: 1700000010,
        };

        let open_size = writer
            .write_range_bound(
                &range.start,
                true,
                range.deletion_time,
                range.local_deletion_time,
                &schema,
                0,
            )
            .unwrap();
        writer
            .write_range_bound(
                &range.end,
                false,
                range.deletion_time,
                range.local_deletion_time,
                &schema,
                open_size as u64,
            )
            .unwrap();

        let bytes = writer.finish().unwrap();
        assert!(!bytes.is_empty());

        // Bottom serializes as an inclusive start bound with zero clustering
        // values (u16 count = 0, no clustering header byte).
        assert_eq!(bytes[0], IS_MARKER, "Should have IS_MARKER flag");
        assert_eq!(
            bytes[1], INCL_START_BOUND,
            "Bottom should serialize as INCL_START_BOUND"
        );
        assert_eq!(
            u16::from_be_bytes([bytes[2], bytes[3]]),
            0,
            "Bottom carries no clustering values"
        );
        // Top serializes as an inclusive end bound with zero values
        assert_eq!(bytes[open_size + 1], INCL_END_BOUND);
    }

    #[test]
    fn test_complete_partition_with_range_tombstone() {
        use crate::storage::write_engine::mutation::{ClusteringBound, RangeTombstone};

        let mut schema = create_test_schema();
        schema.clustering_keys = vec![ClusteringColumn {
            name: "ts".to_string(),
            data_type: "timestamp".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }];

        let stats = create_test_stats();
        let mut writer = DataWriter::new(stats);

        let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x01]);
        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(1));

        // Create mutations
        let mutations = vec![Mutation::new(
            table_id,
            pk,
            Some(ClusteringKey::single("ts", Value::Timestamp(1000))),
            vec![CellOperation::Write {
                column: "name".to_string(),
                value: Value::Text("Alice".to_string()),
            }],
            1001000,
            None,
        )];

        // Create range tombstone
        let range_tombstones = vec![RangeTombstone {
            start: ClusteringBound::Inclusive(ClusteringKey::single("ts", Value::Timestamp(500))),
            end: ClusteringBound::Inclusive(ClusteringKey::single("ts", Value::Timestamp(1500))),
            deletion_time: 1002000, // Later than row timestamp - will shadow it
            local_deletion_time: 1700000020,
        }];

        let offset = writer
            .write_partition(&key, &mutations, &schema, None, &range_tombstones)
            .unwrap();
        assert_eq!(offset, 0);

        let bytes = writer.finish().unwrap();
        assert!(!bytes.is_empty());

        // Verify partition header is present (u16 BE key length)
        assert_eq!(&bytes[0..2], &[0x00, 0x04], "Key length (u16 BE)");

        // Range tombstone markers should appear before rows
        // This is validated by the structure of the output
    }

    #[test]
    fn test_write_cell_with_ttl() {
        let mut stats = create_test_stats();
        stats.min_timestamp = 1000000;
        stats.min_local_deletion_time = 1700000000;
        stats.min_ttl = 3600;
        let writer = DataWriter::new(stats);

        let mut buf = Vec::new();
        let timestamp = 1001000;
        let ttl_seconds = 7200;

        writer
            .write_cell_with_ttl(
                &mut buf,
                "test_col",
                &Value::Text("test".to_string()),
                timestamp,
                ttl_seconds,
            )
            .unwrap();

        assert!(!buf.is_empty());

        // First byte should be CELL_IS_EXPIRING flag (0x02)
        let flags = buf[0];
        assert_eq!(
            flags & CELL_IS_EXPIRING,
            CELL_IS_EXPIRING,
            "Should have IS_EXPIRING flag"
        );
        assert_eq!(
            flags & CELL_USE_ROW_TIMESTAMP,
            0,
            "Should NOT have USE_ROW_TIMESTAMP flag"
        );
        assert_eq!(
            flags & CELL_USE_ROW_TTL,
            0,
            "Should NOT have USE_ROW_TTL flag"
        );

        // Should contain timestamp delta, local_deletion_time delta, TTL delta, and value
        assert!(buf.len() > 10, "Should have all TTL cell fields");
    }

    #[test]
    fn test_row_with_ttl_cells() {
        let mut stats = create_test_stats();
        stats.min_timestamp = 1000000;
        stats.min_local_deletion_time = 1700000000;
        stats.min_ttl = 3600;
        let mut writer = DataWriter::new(stats);
        let schema = create_test_schema();

        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let mutation = Mutation::new(
            table_id,
            pk,
            None,
            vec![
                CellOperation::WriteWithTtl {
                    column: "name".to_string(),
                    value: Value::Text("Alice".to_string()),
                    ttl_seconds: 7200,
                },
                CellOperation::Write {
                    column: "age".to_string(),
                    value: Value::Integer(30),
                },
            ],
            1001000,
            None,
        );

        writer.write_row(&mutation, &schema).unwrap();

        let bytes = writer.finish().unwrap();
        assert!(!bytes.is_empty());

        // Verify row flags
        let flags = bytes[0];
        assert_eq!(
            flags & ROW_HAS_TIMESTAMP,
            ROW_HAS_TIMESTAMP,
            "Should have timestamp"
        );
        assert_eq!(
            flags & ROW_HAS_ALL_COLUMNS,
            ROW_HAS_ALL_COLUMNS,
            "Should have all columns"
        );
    }

    #[test]
    fn test_row_with_multiple_ttl_cells() {
        let mut stats = create_test_stats();
        stats.min_timestamp = 1000000;
        stats.min_local_deletion_time = 1700000000;
        stats.min_ttl = 1800;
        let mut writer = DataWriter::new(stats);
        let schema = create_test_schema();

        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let mutation = Mutation::new(
            table_id,
            pk,
            None,
            vec![
                CellOperation::WriteWithTtl {
                    column: "name".to_string(),
                    value: Value::Text("Alice".to_string()),
                    ttl_seconds: 3600, // 1 hour
                },
                CellOperation::WriteWithTtl {
                    column: "age".to_string(),
                    value: Value::Integer(30),
                    ttl_seconds: 7200, // 2 hours (different TTL)
                },
            ],
            1001000,
            None,
        );

        writer.write_row(&mutation, &schema).unwrap();

        let bytes = writer.finish().unwrap();
        assert!(!bytes.is_empty());

        // Verify both cells were written with their own TTLs
        // The exact validation would require parsing the binary format
    }

    #[test]
    fn test_mixed_ttl_and_regular_cells() {
        let mut stats = create_test_stats();
        stats.min_timestamp = 1000000;
        stats.min_local_deletion_time = 1700000000;
        stats.min_ttl = 3600;
        let mut writer = DataWriter::new(stats);
        let schema = create_test_schema();

        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let mutation = Mutation::new(
            table_id,
            pk,
            None,
            vec![
                CellOperation::Write {
                    column: "name".to_string(),
                    value: Value::Text("Alice".to_string()),
                },
                CellOperation::WriteWithTtl {
                    column: "age".to_string(),
                    value: Value::Integer(30),
                    ttl_seconds: 7200,
                },
            ],
            1001000,
            None,
        );

        writer.write_row(&mutation, &schema).unwrap();

        let bytes = writer.finish().unwrap();
        assert!(!bytes.is_empty());

        // Row should contain both regular and TTL cells
        let flags = bytes[0];
        assert_eq!(flags & ROW_HAS_TIMESTAMP, ROW_HAS_TIMESTAMP);
    }

    #[test]
    fn test_ttl_zero_special_case() {
        let mut stats = create_test_stats();
        stats.min_timestamp = 1000000;
        stats.min_local_deletion_time = 1700000000;
        stats.min_ttl = 0;
        let writer = DataWriter::new(stats);

        let mut buf = Vec::new();
        let timestamp = 1001000;
        let ttl_seconds = 0; // Immediate expiration

        writer
            .write_cell_with_ttl(
                &mut buf,
                "test_col",
                &Value::Text("test".to_string()),
                timestamp,
                ttl_seconds,
            )
            .unwrap();

        assert!(!buf.is_empty());

        // Should have IS_EXPIRING flag even with TTL=0
        let flags = buf[0];
        assert_eq!(flags & CELL_IS_EXPIRING, CELL_IS_EXPIRING);
    }

    #[test]
    fn test_ttl_statistics_tracking() {
        let mut stats = StatisticsMetadata::new();

        // Update with various TTL values
        stats.update_ttl(3600);
        stats.update_ttl(7200);
        stats.update_ttl(1800);
        stats.update_ttl(0); // TTL=0 should be ignored

        assert_eq!(stats.min_ttl, 1800, "min_ttl should be 1800");
        assert_eq!(stats.max_ttl, 7200, "max_ttl should be 7200");
    }

    #[test]
    fn test_ttl_cell_with_null_value() {
        let stats = create_test_stats();
        let writer = DataWriter::new(stats);

        let mut buf = Vec::new();
        let result = writer.write_cell_with_ttl(&mut buf, "test_col", &Value::Null, 1001000, 3600);

        assert!(result.is_err(), "NULL values should return error");
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("NULL values should not be written"));
    }

    #[test]
    fn test_ttl_cell_local_deletion_time_calculation() {
        let mut stats = create_test_stats();
        stats.min_timestamp = 1000000;
        stats.min_local_deletion_time = 1700000000;
        stats.min_ttl = 3600;
        let writer = DataWriter::new(stats);

        let mut buf = Vec::new();
        let timestamp = 1001000;
        let ttl_seconds = 7200; // 2 hours

        // The local_deletion_time should be computed as current_time + ttl_seconds
        writer
            .write_cell_with_ttl(
                &mut buf,
                "test_col",
                &Value::Text("test".to_string()),
                timestamp,
                ttl_seconds,
            )
            .unwrap();

        assert!(!buf.is_empty());
        // Detailed validation would require parsing the encoded deltas
    }

    #[test]
    fn test_row_ttl_uses_row_ttl_cell_flags() {
        // Regression: when a mutation carries a row-level TTL, every regular cell
        // should be encoded with CELL_IS_EXPIRING | CELL_USE_ROW_TIMESTAMP | CELL_USE_ROW_TTL.
        //
        // Previous implementation used a whole-buffer byte-scan to count how many bytes
        // equalled the flag value 0x1A. That was fragile because the LDT delta field is
        // derived from the wall clock and can produce bytes that collide with 0x1A in
        // roughly 1-2% of CI runs.  We now use a structural parse that walks the row
        // header and then reads each cell's flags byte at its exact offset.
        let mut stats = create_test_stats();
        stats.min_timestamp = 1001000;
        stats.min_ttl = 7200;
        stats.min_local_deletion_time = 1;
        let mut writer = DataWriter::new(stats);
        let schema = create_test_schema();

        let mutation = Mutation::new(
            TableId::new("test_ks", "test_table"),
            PartitionKey::single("id", Value::Integer(1)),
            None,
            vec![
                CellOperation::Write {
                    column: "name".to_string(),
                    value: Value::Text("Alice".to_string()),
                },
                CellOperation::Write {
                    column: "age".to_string(),
                    value: Value::Integer(30),
                },
            ],
            1001000,
            Some(7200),
        );

        writer.write_row(&mutation, &schema).unwrap();
        let bytes = writer.finish().unwrap();

        // Verify the row header flags first (non-structural byte is safe here).
        assert_eq!(
            bytes[0] & ROW_HAS_TTL,
            ROW_HAS_TTL,
            "row should have TTL flag"
        );

        // Structurally parse the row body to extract each cell's flags byte.
        // Cassandra sorts regular columns by (is_complex, name) — for simple columns,
        // this is plain alphabetical order.  The schema has "age" (int, 4 bytes fixed)
        // and "name" (text, variable), so "age" sorts before "name".
        let cell_flags = parse_simple_row_cell_flags(
            &bytes,
            &[CellValueSizing::Fixed(4), CellValueSizing::Variable],
        );

        let expected = CELL_IS_EXPIRING | CELL_USE_ROW_TIMESTAMP | CELL_USE_ROW_TTL;
        assert_eq!(
            cell_flags.len(),
            2,
            "should have parsed flags for both cells"
        );
        assert!(
            cell_flags.iter().all(|&f| f == expected),
            "expected both cells to inherit row TTL (flags 0x{:02X}), got: {:?}",
            expected,
            cell_flags
        );
    }

    #[test]
    fn test_write_partition_emits_static_row_before_regular_rows() {
        let stats = create_test_stats();
        let mut writer = DataWriter::new(stats);
        let schema = create_static_test_schema();
        let key = DecoratedKey::new(1, vec![0, 0, 0, 1]);

        let static_mutation = Mutation::new(
            TableId::new("test_ks", "test_table"),
            PartitionKey::single("id", Value::Integer(1)),
            None,
            vec![CellOperation::Write {
                column: "static_val".to_string(),
                value: Value::Text("static".to_string()),
            }],
            1001000,
            None,
        );
        let regular_mutation = Mutation::new(
            TableId::new("test_ks", "test_table"),
            PartitionKey::single("id", Value::Integer(1)),
            Some(ClusteringKey::single("ck", Value::Integer(1))),
            vec![CellOperation::Write {
                column: "regular_val".to_string(),
                value: Value::Text("regular".to_string()),
            }],
            1002000,
            None,
        );

        writer
            .write_partition(
                &key,
                &[static_mutation, regular_mutation],
                &schema,
                None,
                &[],
            )
            .unwrap();
        let bytes = writer.finish().unwrap();

        let partition_header_len = 2 + key.key.len() + 4 + 8;
        assert_eq!(
            bytes[partition_header_len] & ROW_HAS_EXTENDED_FLAGS,
            ROW_HAS_EXTENDED_FLAGS
        );
        assert_eq!(bytes[partition_header_len + 1], EXTENDED_IS_STATIC);
    }

    /// Cassandra switches to large-subset encoding when the superset reaches 64 columns.
    #[test]
    fn test_column_subset_exactly_64_regular_columns_uses_large_subset_encoding() {
        let stats = create_test_stats();
        let writer = DataWriter::new(stats);

        // Create schema with exactly 64 regular columns
        let columns: Vec<Column> = (0..64)
            .map(|i| Column {
                name: format!("col_{:03}", i),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            })
            .collect();

        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns,
            comments: HashMap::new(),
        };

        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(1));

        // Only write col_0 and col_63, forcing the large-subset path.
        let mutation = Mutation::new(
            table_id,
            pk,
            None,
            vec![
                CellOperation::Write {
                    column: "col_000".to_string(),
                    value: Value::Text("first".to_string()),
                },
                CellOperation::Write {
                    column: "col_063".to_string(),
                    value: Value::Text("last".to_string()),
                },
            ],
            1001000,
            None,
        );

        let mut buf = Vec::new();
        writer
            .write_column_bitmap(&mut buf, &mutation, &schema)
            .unwrap();

        // missing_count=62, then present indexes [0, 63]
        assert_eq!(buf, vec![62, 0, 63]);
    }

    /// Large static-column subsets use the same delta encoding as regular columns.
    #[test]
    fn test_column_subset_65_static_columns_uses_missing_indexes_when_present_majority() {
        let stats = create_test_stats();
        let writer = DataWriter::new(stats);

        // Create schema with 65 static columns
        let columns: Vec<Column> = (0..65)
            .map(|i| Column {
                name: format!("scol_{:03}", i),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: true,
            })
            .collect();

        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![ClusteringColumn {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                position: 0,
                order: ClusteringOrder::Asc,
            }],
            columns,
            comments: HashMap::new(),
        };

        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(1));

        // Write all but one static column so the encoding emits missing indexes.
        let mut operations = Vec::new();
        for i in 0..65 {
            if i == 17 {
                continue;
            }
            operations.push(CellOperation::Write {
                column: format!("scol_{:03}", i),
                value: Value::Text(format!("value-{}", i)),
            });
        }

        let mutation = Mutation::new(table_id, pk, None, operations, 1001000, None);
        let static_ops: Vec<StaticMergedOp> = mutation
            .operations
            .iter()
            .map(|op| StaticMergedOp {
                op: op.clone(),
                timestamp_micros: mutation.timestamp_micros,
                cell_local_deletion_time: mutation.effective_local_deletion_time(),
            })
            .collect();

        let mut buf = Vec::new();
        writer
            .write_static_column_bitmap(&mut buf, &static_ops, &schema)
            .unwrap();

        // missing_count=1, followed by the missing column index.
        assert_eq!(buf, vec![1, 17]);
    }

    /// Smaller subsets still use the missing-column bitmap.
    #[test]
    fn test_column_subset_under_64_regular_columns_uses_bitmap() {
        let stats = create_test_stats();
        let writer = DataWriter::new(stats);

        let columns: Vec<Column> = (0..4)
            .map(|i| Column {
                name: format!("col_{i}"),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            })
            .collect();

        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns,
            comments: HashMap::new(),
        };

        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(1));

        // Only col_1 is present, so bits 0, 2, and 3 are set.
        let mutation = Mutation::new(
            table_id,
            pk,
            None,
            vec![CellOperation::Write {
                column: "col_1".to_string(),
                value: Value::Text("present".to_string()),
            }],
            1001000,
            None,
        );

        let mut buf = Vec::new();
        writer
            .write_column_bitmap(&mut buf, &mutation, &schema)
            .unwrap();

        assert_eq!(buf, vec![0b1101]);
    }

    #[test]
    fn test_regular_columns_sort_simple_before_complex() {
        let stats = create_test_stats();
        let writer = DataWriter::new(stats);

        let schema = TableSchema {
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
                    name: "z_simple".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "a_complex".to_string(),
                    data_type: "set<text>".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "m_simple".to_string(),
                    data_type: "int".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
        };

        let ordered = writer.regular_columns(&schema);
        let names: Vec<_> = ordered.iter().map(|column| column.name.as_str()).collect();

        assert_eq!(names, vec!["m_simple", "z_simple", "a_complex"]);
    }

    #[test]
    fn test_static_columns_sort_simple_before_complex() {
        let stats = create_test_stats();
        let writer = DataWriter::new(stats);

        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
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
                    name: "z_static_simple".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: true,
                },
                Column {
                    name: "a_static_complex".to_string(),
                    data_type: "set<text>".to_string(),
                    nullable: true,
                    default: None,
                    is_static: true,
                },
                Column {
                    name: "m_static_simple".to_string(),
                    data_type: "int".to_string(),
                    nullable: true,
                    default: None,
                    is_static: true,
                },
            ],
            comments: HashMap::new(),
        };

        let ordered = writer.static_columns(&schema);
        let names: Vec<_> = ordered.iter().map(|column| column.name.as_str()).collect();

        assert_eq!(
            names,
            vec!["m_static_simple", "z_static_simple", "a_static_complex"]
        );
    }

    #[test]
    fn test_write_column_bitmap_zero_when_all_columns_present() {
        let stats = create_test_stats();
        let writer = DataWriter::new(stats);

        let columns: Vec<Column> = (0..65)
            .map(|i| Column {
                name: format!("col_{:03}", i),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            })
            .collect();

        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns,
            comments: HashMap::new(),
        };

        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(1));

        let operations: Vec<_> = (0..65)
            .map(|i| CellOperation::Write {
                column: format!("col_{:03}", i),
                value: Value::Text(format!("value-{}", i)),
            })
            .collect();

        let mutation = Mutation::new(table_id, pk, None, operations, 1001000, None);

        let mut buf = Vec::new();
        writer
            .write_column_bitmap(&mut buf, &mutation, &schema)
            .unwrap();

        assert_eq!(buf, vec![0]);
    }

    #[test]
    fn test_serialize_list() {
        let list = Value::List(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ]);
        let bytes = serialize_value(&list).unwrap();
        // 4 bytes count + 3 * (4 bytes len + 4 bytes i32)
        assert_eq!(bytes.len(), 4 + 3 * 8);
        // Count = 3
        assert_eq!(&bytes[0..4], &3i32.to_be_bytes());
        // First element length = 4
        assert_eq!(&bytes[4..8], &4i32.to_be_bytes());
        // First element value = 1
        assert_eq!(&bytes[8..12], &1i32.to_be_bytes());
    }

    #[test]
    fn test_serialize_empty_list() {
        let list = Value::List(vec![]);
        let bytes = serialize_value(&list).unwrap();
        assert_eq!(bytes.len(), 4);
        assert_eq!(&bytes[0..4], &0i32.to_be_bytes());
    }

    #[test]
    fn test_serialize_single_element_list() {
        let list = Value::List(vec![Value::Integer(42)]);
        let bytes = serialize_value(&list).unwrap();
        assert_eq!(
            bytes,
            vec![
                0x00, 0x00, 0x00, 0x01, // count = 1
                0x00, 0x00, 0x00, 0x04, // len = 4
                0x00, 0x00, 0x00, 0x2A, // value = 42
            ]
        );
    }

    #[test]
    fn test_serialize_set() {
        let set = Value::Set(vec![
            Value::Text("alpha".to_string()),
            Value::Text("beta".to_string()),
        ]);
        let bytes = serialize_value(&set).unwrap();
        // Count = 2
        assert_eq!(&bytes[0..4], &2i32.to_be_bytes());
        // First element length = 5 ("alpha")
        assert_eq!(&bytes[4..8], &5i32.to_be_bytes());
        assert_eq!(&bytes[8..13], b"alpha");
    }

    #[test]
    fn test_serialize_single_element_set() {
        let set = Value::Set(vec![Value::Text("alpha".to_string())]);
        let bytes = serialize_value(&set).unwrap();
        assert_eq!(
            bytes,
            vec![
                0x00, 0x00, 0x00, 0x01, // count = 1
                0x00, 0x00, 0x00, 0x05, // len = 5
                b'a', b'l', b'p', b'h', b'a', // value = "alpha"
            ]
        );
    }

    #[test]
    fn test_serialize_empty_set() {
        let set = Value::Set(vec![]);
        let bytes = serialize_value(&set).unwrap();
        assert_eq!(bytes, 0i32.to_be_bytes().to_vec());
    }

    #[test]
    fn test_serialize_map() {
        let map = Value::Map(vec![(Value::Text("key1".to_string()), Value::Integer(100))]);
        let bytes = serialize_value(&map).unwrap();
        // Count = 1
        assert_eq!(&bytes[0..4], &1i32.to_be_bytes());
        // Key length = 4 ("key1")
        assert_eq!(&bytes[4..8], &4i32.to_be_bytes());
        assert_eq!(&bytes[8..12], b"key1");
        // Value length = 4 (i32)
        assert_eq!(&bytes[12..16], &4i32.to_be_bytes());
        // Value = 100
        assert_eq!(&bytes[16..20], &100i32.to_be_bytes());
    }

    #[test]
    fn test_serialize_empty_map() {
        let map = Value::Map(vec![]);
        let bytes = serialize_value(&map).unwrap();
        assert_eq!(bytes.len(), 4);
        assert_eq!(&bytes[0..4], &0i32.to_be_bytes());
    }

    #[test]
    fn test_serialize_tuple() {
        let tuple = Value::Tuple(vec![
            Value::Integer(42),
            Value::Text("hello".to_string()),
            Value::Null,
        ]);
        let bytes = serialize_value(&tuple).unwrap();
        // Field 1: 4 bytes len + 4 bytes i32 = 8
        assert_eq!(&bytes[0..4], &4i32.to_be_bytes());
        assert_eq!(&bytes[4..8], &42i32.to_be_bytes());
        // Field 2: 4 bytes len + 5 bytes text = 9
        assert_eq!(&bytes[8..12], &5i32.to_be_bytes());
        assert_eq!(&bytes[12..17], b"hello");
        // Field 3: NULL = -1 as i32
        assert_eq!(&bytes[17..21], &(-1i32).to_be_bytes());
    }

    #[test]
    fn test_serialize_single_element_tuple() {
        let tuple = Value::Tuple(vec![Value::Text("solo".to_string())]);
        let bytes = serialize_value(&tuple).unwrap();
        assert_eq!(
            bytes,
            vec![
                0x00, 0x00, 0x00, 0x04, // len = 4
                b's', b'o', b'l', b'o', // value = "solo"
            ]
        );
    }

    #[test]
    fn test_serialize_frozen() {
        let frozen = Value::Frozen(Box::new(Value::List(vec![
            Value::Integer(10),
            Value::Integer(20),
        ])));
        let frozen_bytes = serialize_value(&frozen).unwrap();
        let list_bytes =
            serialize_value(&Value::List(vec![Value::Integer(10), Value::Integer(20)])).unwrap();
        // Frozen should produce identical bytes to inner value
        assert_eq!(frozen_bytes, list_bytes);
    }

    #[test]
    fn test_serialize_single_element_frozen() {
        let frozen = Value::Frozen(Box::new(Value::List(vec![Value::Text("solo".to_string())])));
        let frozen_bytes = serialize_value(&frozen).unwrap();
        let list_bytes =
            serialize_value(&Value::List(vec![Value::Text("solo".to_string())])).unwrap();
        assert_eq!(frozen_bytes, list_bytes);
    }

    #[test]
    fn test_serialize_nested_collection() {
        // MAP<TEXT, FROZEN<LIST<INT>>>
        let nested = Value::Map(vec![(
            Value::Text("nums".to_string()),
            Value::Frozen(Box::new(Value::List(vec![
                Value::Integer(1),
                Value::Integer(2),
            ]))),
        )]);
        let bytes = serialize_value(&nested).unwrap();
        // Should not error - validates nested serialization works
        assert!(!bytes.is_empty());
        // Count = 1
        assert_eq!(&bytes[0..4], &1i32.to_be_bytes());
    }

    #[test]
    fn test_serialize_udt_with_nested_collections_matches_schema_aware_bytes() {
        let serializer = TypeSerializer::new();
        let company = phase3_company_value();

        let bytes = serialize_value(&Value::Udt(company.clone())).unwrap();
        let expected = serializer
            .serialize_udt(&Value::Udt(company), &phase3_company_schema())
            .unwrap();

        assert_eq!(bytes, expected);
    }

    #[test]
    fn test_serialize_collection_containing_nested_udts() {
        let serializer = TypeSerializer::new();
        let company = phase3_company_value();
        let company_bytes = serializer
            .serialize_udt(&Value::Udt(company.clone()), &phase3_company_schema())
            .unwrap();

        let value = Value::Map(vec![(
            Value::Text("empresa_日本".to_string()),
            Value::Frozen(Box::new(Value::Udt(company))),
        )]);
        let bytes = serialize_value(&value).unwrap();

        let key = "empresa_日本".as_bytes();
        let mut expected = Vec::new();
        expected.extend_from_slice(&1i32.to_be_bytes());
        expected.extend_from_slice(&(key.len() as i32).to_be_bytes());
        expected.extend_from_slice(key);
        expected.extend_from_slice(&(company_bytes.len() as i32).to_be_bytes());
        expected.extend_from_slice(&company_bytes);

        assert_eq!(bytes, expected);
    }

    #[test]
    fn test_serialize_tuple_with_collection_fields_and_udt() {
        let serializer = TypeSerializer::new();
        let address = phase3_address_value();
        let person = phase3_person_value("Tuple User");
        let address_bytes = serializer
            .serialize_udt(&Value::Udt(address.clone()), &phase3_address_schema())
            .unwrap();
        let person_bytes = serializer
            .serialize_udt(&Value::Udt(person.clone()), &phase3_person_schema())
            .unwrap();

        let tuple = Value::Tuple(vec![
            Value::Text("phase3".to_string()),
            Value::Frozen(Box::new(Value::List(vec![
                Value::Integer(3),
                Value::Integer(5),
                Value::Integer(8),
            ]))),
            Value::Frozen(Box::new(Value::Map(vec![(
                Value::Text("home".to_string()),
                Value::Frozen(Box::new(Value::Udt(address))),
            )]))),
            Value::Frozen(Box::new(Value::Udt(person))),
        ]);
        let bytes = serialize_value(&tuple).unwrap();

        let list_bytes = serialize_value(&Value::List(vec![
            Value::Integer(3),
            Value::Integer(5),
            Value::Integer(8),
        ]))
        .unwrap();
        let map_bytes = {
            let key = b"home";
            let mut encoded = Vec::new();
            encoded.extend_from_slice(&1i32.to_be_bytes());
            encoded.extend_from_slice(&(key.len() as i32).to_be_bytes());
            encoded.extend_from_slice(key);
            encoded.extend_from_slice(&(address_bytes.len() as i32).to_be_bytes());
            encoded.extend_from_slice(&address_bytes);
            encoded
        };

        let mut expected = Vec::new();
        expected.extend_from_slice(&6i32.to_be_bytes());
        expected.extend_from_slice(b"phase3");
        expected.extend_from_slice(&(list_bytes.len() as i32).to_be_bytes());
        expected.extend_from_slice(&list_bytes);
        expected.extend_from_slice(&(map_bytes.len() as i32).to_be_bytes());
        expected.extend_from_slice(&map_bytes);
        expected.extend_from_slice(&(person_bytes.len() as i32).to_be_bytes());
        expected.extend_from_slice(&person_bytes);

        assert_eq!(bytes, expected);
    }

    #[test]
    fn test_serialize_high_complexity_nested_collection() {
        let nested = Value::Map(vec![(
            Value::Text("outer".to_string()),
            Value::Frozen(Box::new(Value::List(vec![Value::Frozen(Box::new(
                Value::Map(vec![(
                    Value::Text("inner".to_string()),
                    Value::Frozen(Box::new(Value::List(vec![
                        Value::Integer(1),
                        Value::Integer(2),
                    ]))),
                )]),
            ))]))),
        )]);

        let bytes = serialize_value(&nested).unwrap();

        assert!(!bytes.is_empty());
        assert_eq!(&bytes[0..4], &1i32.to_be_bytes());
    }

    // ========== Complex Column (Multi-Cell) Tests ==========

    #[test]
    fn test_is_complex_column() {
        // Non-frozen collections ARE complex (CQL syntax)
        assert!(is_complex_column("set<int>"));
        assert!(is_complex_column("list<text>"));
        assert!(is_complex_column("map<text, int>"));
        assert!(is_complex_column("SET<INT>"));
        assert!(is_complex_column("List<Text>"));
        assert!(is_complex_column("Map<Text, Int>"));

        // Non-frozen collections ARE complex (Cassandra internal syntax)
        assert!(is_complex_column(
            "org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.Int32Type)"
        ));
        assert!(is_complex_column(
            "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.UTF8Type)"
        ));
        assert!(is_complex_column(
            "org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.Int32Type)"
        ));

        // Frozen collections are NOT complex (CQL syntax)
        assert!(!is_complex_column("frozen<set<int>>"));
        assert!(!is_complex_column("frozen<list<text>>"));
        assert!(!is_complex_column("frozen<map<text, int>>"));
        assert!(!is_complex_column("FROZEN<SET<INT>>"));

        // Frozen collections are NOT complex (Cassandra internal syntax)
        assert!(!is_complex_column(
            "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.Int32Type))"
        ));

        // Primitives are NOT complex
        assert!(!is_complex_column("int"));
        assert!(!is_complex_column("text"));
        assert!(!is_complex_column("uuid"));
        assert!(!is_complex_column("timestamp"));
    }

    #[test]
    fn test_generate_list_cell_path_timeuuid() {
        let ts = 1_704_067_200_000_000i64; // 2024-01-01 00:00:00 UTC

        let uuid0 = generate_list_cell_path_timeuuid(ts, 0);
        let uuid1 = generate_list_cell_path_timeuuid(ts, 1);
        let uuid2 = generate_list_cell_path_timeuuid(ts, 2);

        // All should be 16 bytes
        assert_eq!(uuid0.len(), 16);
        assert_eq!(uuid1.len(), 16);

        // Version bits should be 1 (0x1X in byte 6)
        assert_eq!(uuid0[6] & 0xF0, 0x10, "Should be UUID version 1");
        assert_eq!(uuid1[6] & 0xF0, 0x10, "Should be UUID version 1");

        // UUIDs should be monotonically increasing (as byte arrays)
        assert!(uuid0 < uuid1, "UUID0 should be less than UUID1");
        assert!(uuid1 < uuid2, "UUID1 should be less than UUID2");
    }

    #[test]
    fn test_write_set_complex_column() {
        let stats = create_test_stats();
        let writer = DataWriter::new(stats);

        let column = Column {
            name: "tags".to_string(),
            data_type: "set<text>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        };

        let value = Value::Set(vec![
            Value::Text("alpha".to_string()),
            Value::Text("beta".to_string()),
        ]);

        let mut buf = Vec::new();
        writer
            .write_complex_column(&mut buf, &column, &value, 1001000, None)
            .unwrap();

        assert!(!buf.is_empty());

        // Structurally parse cell flags so DeletionTime.LIVE header bytes
        // (which can coincide with flag values) are not misidentified.
        let expected_cell_flags = CELL_USE_ROW_TIMESTAMP | CELL_HAS_EMPTY_VALUE;
        let cell_flags = parse_complex_cell_flags(&buf);
        assert_eq!(cell_flags.len(), 2, "Should have 2 SET cells");
        assert!(
            cell_flags.iter().all(|&f| f == expected_cell_flags),
            "Should have 2 SET cells with USE_ROW_TIMESTAMP | HAS_EMPTY_VALUE flags, got: {:?}",
            cell_flags
        );
    }

    #[test]
    fn test_write_map_complex_column() {
        let stats = create_test_stats();
        let writer = DataWriter::new(stats);

        let column = Column {
            name: "props".to_string(),
            data_type: "map<text, int>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        };

        let value = Value::Map(vec![
            (Value::Text("key1".to_string()), Value::Integer(100)),
            (Value::Text("key2".to_string()), Value::Integer(200)),
        ]);

        let mut buf = Vec::new();
        writer
            .write_complex_column(&mut buf, &column, &value, 1001000, None)
            .unwrap();

        assert!(!buf.is_empty());

        // MAP cells have USE_ROW_TIMESTAMP (0x08) but NOT HAS_EMPTY_VALUE.
        // Use structural parse so DeletionTime.LIVE header bytes are not misidentified.
        let cell_flags = parse_complex_cell_flags(&buf);
        assert_eq!(cell_flags.len(), 2, "Should have 2 MAP cells");
        assert!(
            cell_flags.iter().all(|&f| f == CELL_USE_ROW_TIMESTAMP),
            "Should have 2 MAP cells with USE_ROW_TIMESTAMP flags, got: {:?}",
            cell_flags
        );
    }

    #[test]
    fn test_write_list_complex_column() {
        let stats = create_test_stats();
        let writer = DataWriter::new(stats);

        let column = Column {
            name: "items".to_string(),
            data_type: "list<int>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        };

        let value = Value::List(vec![Value::Integer(10), Value::Integer(20)]);

        let mut buf = Vec::new();
        writer
            .write_complex_column(&mut buf, &column, &value, 1001000, None)
            .unwrap();

        assert!(!buf.is_empty());

        // LIST cells have USE_ROW_TIMESTAMP (0x08) and 16-byte TimeUUID paths.
        // Use structural parse so DeletionTime.LIVE header bytes are not misidentified.
        let cell_flags = parse_complex_cell_flags(&buf);
        assert_eq!(cell_flags.len(), 2, "Should have 2 LIST cells");
        assert!(
            cell_flags.iter().all(|&f| f == CELL_USE_ROW_TIMESTAMP),
            "Should have 2 LIST cells with USE_ROW_TIMESTAMP flags, got: {:?}",
            cell_flags
        );
        // The TimeUUID path length (16) is structurally verified by parse_complex_cell_flags
        // successfully parsing each cell's path — if path_len were wrong, parsing would
        // overshoot or the cell count would be wrong.
    }

    #[test]
    fn test_frozen_collection_not_complex() {
        // Frozen collections should still use simple cell (serialize_value), not complex column
        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![Column {
                name: "frozen_tags".to_string(),
                data_type: "frozen<set<text>>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            }],
            comments: HashMap::new(),
        };

        let stats = create_test_stats();
        let mut writer = DataWriter::new(stats);

        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let mutation = Mutation::new(
            table_id,
            pk,
            None,
            vec![CellOperation::Write {
                column: "frozen_tags".to_string(),
                value: Value::Frozen(Box::new(Value::Set(vec![
                    Value::Text("a".to_string()),
                    Value::Text("b".to_string()),
                ]))),
            }],
            1001000,
            None,
        );

        writer.write_row(&mutation, &schema).unwrap();

        let bytes = writer.finish().unwrap();
        assert!(!bytes.is_empty());

        // Frozen collection should NOT have HAS_COMPLEX_DELETION flag
        let flags = bytes[0];
        assert_eq!(
            flags & ROW_HAS_COMPLEX_DELETION,
            0,
            "Frozen collection should NOT have HAS_COMPLEX_DELETION flag"
        );
    }

    #[test]
    fn test_mixed_simple_and_complex_columns() {
        let schema = TableSchema {
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
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "tags".to_string(),
                    data_type: "set<text>".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
        };

        let stats = create_test_stats();
        let mut writer = DataWriter::new(stats);

        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let mutation = Mutation::new(
            table_id,
            pk,
            None,
            vec![
                CellOperation::Write {
                    column: "name".to_string(),
                    value: Value::Text("Alice".to_string()),
                },
                CellOperation::Write {
                    column: "tags".to_string(),
                    value: Value::Set(vec![
                        Value::Text("admin".to_string()),
                        Value::Text("user".to_string()),
                    ]),
                },
            ],
            1001000,
            None,
        );

        writer.write_row(&mutation, &schema).unwrap();

        let bytes = writer.finish().unwrap();
        assert!(!bytes.is_empty());

        // Row should have HAS_COMPLEX_DELETION flag because of the SET column
        let flags = bytes[0];
        assert_eq!(
            flags & ROW_HAS_COMPLEX_DELETION,
            ROW_HAS_COMPLEX_DELETION,
            "Row with non-frozen SET should have HAS_COMPLEX_DELETION flag"
        );
        assert_eq!(
            flags & ROW_HAS_TIMESTAMP,
            ROW_HAS_TIMESTAMP,
            "Should have timestamp"
        );
        assert_eq!(
            flags & ROW_HAS_ALL_COLUMNS,
            ROW_HAS_ALL_COLUMNS,
            "Should have all columns"
        );
    }

    #[test]
    fn test_set_canonical_ordering() {
        // Elements provided out of order should be sorted by serialized bytes
        let stats = create_test_stats();
        let writer = DataWriter::new(stats);

        let column = Column {
            name: "tags".to_string(),
            data_type: "set<text>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        };

        // Input: zebra, alpha, mango (unsorted)
        let value = Value::Set(vec![
            Value::Text("zebra".to_string()),
            Value::Text("alpha".to_string()),
            Value::Text("mango".to_string()),
        ]);

        let mut buf = Vec::new();
        writer
            .write_complex_column(&mut buf, &column, &value, 1001000, None)
            .unwrap();

        // Extract cell paths from the binary output.
        // After complex deletion (2 VInts) and cell count (1 VInt), each cell is:
        //   flags(1) + path_len(VInt) + path_bytes
        // Find the text values in order by scanning for ASCII strings.
        let buf_str = String::from_utf8_lossy(&buf);
        let alpha_pos = buf_str.find("alpha").expect("alpha should be in output");
        let mango_pos = buf_str.find("mango").expect("mango should be in output");
        let zebra_pos = buf_str.find("zebra").expect("zebra should be in output");

        assert!(
            alpha_pos < mango_pos && mango_pos < zebra_pos,
            "SET elements should be in sorted order: alpha({}) < mango({}) < zebra({})",
            alpha_pos,
            mango_pos,
            zebra_pos
        );
    }

    #[test]
    fn test_map_canonical_ordering() {
        let stats = create_test_stats();
        let writer = DataWriter::new(stats);

        let column = Column {
            name: "props".to_string(),
            data_type: "map<text, int>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        };

        // Input: keys out of order (z_key, a_key)
        let value = Value::Map(vec![
            (Value::Text("z_key".to_string()), Value::Integer(1)),
            (Value::Text("a_key".to_string()), Value::Integer(2)),
        ]);

        let mut buf = Vec::new();
        writer
            .write_complex_column(&mut buf, &column, &value, 1001000, None)
            .unwrap();

        let buf_str = String::from_utf8_lossy(&buf);
        let a_pos = buf_str.find("a_key").expect("a_key should be in output");
        let z_pos = buf_str.find("z_key").expect("z_key should be in output");

        assert!(
            a_pos < z_pos,
            "MAP entries should be sorted by key: a_key({}) < z_key({})",
            a_pos,
            z_pos
        );
    }

    #[test]
    fn test_set_rejects_list_value() {
        let stats = create_test_stats();
        let writer = DataWriter::new(stats);

        let column = Column {
            name: "tags".to_string(),
            data_type: "set<text>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        };

        // Pass a List value to a SET column — should be rejected
        let value = Value::List(vec![Value::Text("x".to_string())]);
        let mut buf = Vec::new();
        let result = writer.write_complex_column(&mut buf, &column, &value, 1001000, None);
        assert!(result.is_err(), "SET column should reject Value::List");
    }

    #[test]
    fn test_list_rejects_set_value() {
        let stats = create_test_stats();
        let writer = DataWriter::new(stats);

        let column = Column {
            name: "items".to_string(),
            data_type: "list<text>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        };

        // Pass a Set value to a LIST column — should be rejected
        let value = Value::Set(vec![Value::Text("x".to_string())]);
        let mut buf = Vec::new();
        let result = writer.write_complex_column(&mut buf, &column, &value, 1001000, None);
        assert!(result.is_err(), "LIST column should reject Value::Set");
    }

    #[test]
    fn test_complex_column_deletion() {
        let stats = create_test_stats();
        let writer = DataWriter::new(stats);

        let mut buf = Vec::new();
        // Issue #764: the caller now supplies the local_deletion_time explicitly.
        writer
            .write_complex_column_deletion(&mut buf, 1001000, 42)
            .unwrap();

        assert!(!buf.is_empty());

        // Should contain: marked_for_delete_at delta + local_deletion_time delta + cell_count(0)
        // The last byte should be 0x00 (cell_count = 0 encoded as unsigned VInt)
        assert_eq!(
            buf[buf.len() - 1],
            0x00,
            "Last byte should be cell_count = 0"
        );
    }

    #[test]
    fn test_complex_column_deletion_rejects_ldt_below_baseline() {
        // Issue #764: an explicit local_deletion_time below min_local_deletion_time
        // must be rejected, not silently wrapped into a corrupt unsigned VInt.
        let mut stats = create_test_stats();
        stats.min_local_deletion_time = 100;
        let writer = DataWriter::new(stats);

        let mut buf = Vec::new();
        let result = writer.write_complex_column_deletion(&mut buf, 1001000, 50);
        assert!(
            result.is_err(),
            "LDT below baseline must be rejected to avoid VInt wrap corruption"
        );
    }

    /// Issue #853: a complex-deletion marker whose localDeletionTime lands in
    /// [2^31, 2^32) (far future, ~2038-2106) must encode the LDT delta with the
    /// SAME i32 cast + wrapping that Cassandra's DeletionTime.serialize uses, so the
    /// number of bytes written equals the size the row-size vint accounts for. The
    /// previous i64-widened path both rejected these values and would have produced
    /// a divergent byte count.
    #[test]
    fn test_complex_column_deletion_far_future_ldt_size_matches_written() {
        use crate::parser::vint::parse_vuint;

        // min baseline of 0 (DeletionTime.LIVE-derived stats min), the common case.
        let stats = create_test_stats();
        assert_eq!(stats.min_local_deletion_time, 0);
        let writer = DataWriter::new(stats);

        // Boundary 2^31 and a high value near 2^32 - 1, both representable only as
        // negative i32 bit patterns.
        let far_future: [u32; 3] = [1u32 << 31, (1u32 << 31) + 12345, u32::MAX - 1];

        for raw in far_future {
            let ldt = raw as i32; // negative i32 bit pattern for [2^31, 2^32)
            assert!(
                ldt < 0,
                "value {raw} must be a negative i32 in [2^31, 2^32)"
            );

            let mut buf = Vec::new();
            writer
                .write_complex_column_deletion(&mut buf, 1_001_000, ldt)
                .expect("far-future complex deletion must be accepted, not rejected");

            // Skip the markedForDeleteAt VInt (timestamp delta) to reach the LDT delta.
            // parse_vuint is a nom parser: Ok((remaining, value)).
            let (ldt_bytes, _ts_delta) =
                parse_vuint(&buf).expect("markedForDeleteAt VInt must decode");

            // The encoded LDT delta must equal the i32-wrapping u32 value Cassandra
            // would write: localDeletionTime - minLocalDeletionTime in 32-bit space.
            let expected_delta = ldt.wrapping_sub(0) as u32; // min = 0
            assert_eq!(
                expected_delta, raw,
                "delta must equal the raw far-future value"
            );

            let (rest, decoded_delta) = parse_vuint(ldt_bytes).expect("LDT delta VInt must decode");
            assert_eq!(
                decoded_delta, expected_delta as u64,
                "round-tripped LDT delta must match the i32-wrapping value for raw={raw}"
            );

            // SIZE == WRITTEN: the bytes consumed by the LDT delta VInt must equal
            // the canonical unsigned_len of that delta (no over/under-count), and the
            // only remaining byte is the cell_count(0).
            let ldt_vint_len = ldt_bytes.len() - rest.len();
            assert_eq!(
                ldt_vint_len,
                unsigned_len(expected_delta as u64),
                "encoded LDT delta size must equal bytes written for raw={raw}"
            );
            assert_eq!(
                rest,
                &[0u8],
                "trailing byte must be cell_count = 0 for raw={raw}"
            );
        }
    }

    /// Branch-review (#853/#889): a range-tombstone marker whose localDeletionTime
    /// lands in [2^31, 2^32) (far future, ~2038-2106) must encode the LDT delta with
    /// the SAME i32 cast + wrapping that Cassandra's DeletionTime.serialize uses, so
    /// the bytes written equal the size the marker_body_size vint accounts for. The
    /// previous i64-widened path produced a 64-bit wrapped delta with a divergent
    /// byte count (and a corrupted body_size vint).
    #[test]
    fn test_range_tombstone_far_future_ldt_size_matches_written() {
        use crate::parser::vint::parse_vuint;

        // min baseline of 0 (DeletionTime.LIVE-derived stats min), the common case.
        let mut stats = create_test_stats();
        stats.min_timestamp = 0;
        stats.min_local_deletion_time = 0;
        assert_eq!(stats.min_local_deletion_time, 0);
        let schema = create_test_schema();

        // Boundary 2^31 and a high value near 2^32 - 1, both representable only as
        // negative i32 bit patterns.
        let far_future: [u32; 3] = [1u32 << 31, (1u32 << 31) + 12345, u32::MAX - 1];

        for raw in far_future {
            let ldt = raw as i32; // negative i32 bit pattern for [2^31, 2^32)
            assert!(
                ldt < 0,
                "value {raw} must be a negative i32 in [2^31, 2^32)"
            );

            let mut writer = DataWriter::new(stats.clone());
            // Bottom bound: an inclusive start with zero clustering values, keeping
            // the marker framing minimal.
            let prev_size = 0u64;
            let written = writer
                .write_range_bound(
                    &ClusteringBound::Bottom,
                    /* is_open */ true,
                    /* deletion_time */ 1_001_000,
                    ldt,
                    &schema,
                    prev_size,
                )
                .expect("far-future range tombstone must be accepted, not rejected");

            // SIZE == WRITTEN: the returned marker size must equal the buffer growth.
            assert_eq!(
                written,
                writer.buffer.len(),
                "returned marker size must equal bytes written for raw={raw}"
            );

            // Walk the marker layout to reach the deletion-time VInts:
            //   [IS_MARKER][bound_kind][cluster_count u16=0][body_size vuint]
            //   [prev_size vuint][ts_delta vuint][ldt_delta vuint]
            let buf = &writer.buffer;
            assert_eq!(buf[0], IS_MARKER, "first byte must be IS_MARKER");
            assert_eq!(buf[1], INCL_START_BOUND, "Bottom open bound kind");
            assert_eq!(&buf[2..4], &[0u8, 0u8], "cluster_count u16 = 0 for Bottom");

            let after_count = &buf[4..];
            let (after_body_size, body_size) =
                parse_vuint(after_count).expect("body_size VInt must decode");
            let body_start_remaining = after_body_size.len();
            let (after_prev, _prev) =
                parse_vuint(after_body_size).expect("prev_size VInt must decode");
            let (after_ts, _ts_delta) =
                parse_vuint(after_prev).expect("markedForDeleteAt VInt must decode");
            let (rest, decoded_ldt) = parse_vuint(after_ts).expect("LDT delta VInt must decode");

            // The encoded LDT delta must equal the i32-wrapping u32 value Cassandra
            // would write: localDeletionTime - minLocalDeletionTime in 32-bit space.
            let expected_delta = ldt.wrapping_sub(0) as u32; // min = 0
            assert_eq!(
                expected_delta, raw,
                "delta must equal the raw far-future value for raw={raw}"
            );
            assert_eq!(
                decoded_ldt, expected_delta as u64,
                "round-tripped LDT delta must match the i32-wrapping value for raw={raw}"
            );

            // body_size must exactly account for prev_size + ts_delta + ldt_delta:
            // the bytes from the start of prev_size to the end of the marker.
            assert!(
                rest.is_empty(),
                "marker must end after LDT delta for raw={raw}"
            );
            assert_eq!(
                body_size as usize, body_start_remaining,
                "body_size vint must equal bytes of (prev_size + deletion times) for raw={raw}"
            );
        }
    }

    /// Issue #853: the same far-future marker, written inside a full row, must keep
    /// the row-size vint exactly equal to the row-body bytes that follow it. A
    /// schema with no clustering key keeps the framing simple: after the row-flags
    /// byte the next bytes are the row-size vint itself.
    #[test]
    fn test_complex_deletion_far_future_row_size_vint_matches_body() {
        use crate::parser::vint::parse_vuint;

        let schema = TableSchema {
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
                    name: "tags".to_string(),
                    data_type: "set<text>".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
        };

        // Boundary 2^31 and a high value near 2^32 - 1, both negative i32 patterns.
        for raw in [1u32 << 31, u32::MAX - 1] {
            let ldt = raw as i32;
            let mut writer = DataWriter::new(create_test_stats());

            let table_id = TableId::new("test_ks", "test_table");
            let pk = PartitionKey::single("id", Value::Integer(1));
            let mutation = Mutation::new(
                table_id,
                pk,
                None,
                vec![CellOperation::Delete {
                    column: "tags".to_string(),
                }],
                2_000_000,
                None,
            )
            .with_local_deletion_time(ldt);

            writer
                .write_row(&mutation, &schema)
                .expect("far-future complex-deletion row must write, not error");
            let out = writer.finish().expect("finish");

            // out = [row_flags u8][row_size vint][prev_size vint][body...].
            // (no clustering key, so nothing between flags and row_size.)
            assert!(!out.is_empty(), "row must be written for raw={raw}");
            let after_flags = &out[1..];
            let (body_after_size, row_size) =
                parse_vuint(after_flags).expect("row-size vint must decode");

            // Size == written: the row-size vint must exactly account for the body
            // bytes that follow it (a divergent far-future LDT byte count would make
            // this mismatch and corrupt the row framing).
            assert_eq!(
                row_size as usize,
                body_after_size.len(),
                "row-size vint must equal the row-body bytes written for raw={raw}"
            );
        }
    }

    #[test]
    fn test_write_with_ttl_complex_column() {
        // WriteWithTtl on a complex column should use complex format, not simple cell
        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![Column {
                name: "tags".to_string(),
                data_type: "set<text>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            }],
            comments: HashMap::new(),
        };

        let stats = create_test_stats();
        let mut writer = DataWriter::new(stats);

        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let mutation = Mutation::new(
            table_id,
            pk,
            None,
            vec![CellOperation::WriteWithTtl {
                column: "tags".to_string(),
                value: Value::Set(vec![
                    Value::Text("a".to_string()),
                    Value::Text("b".to_string()),
                ]),
                ttl_seconds: 3600,
            }],
            1001000,
            None,
        );

        // Should succeed without error — complex format should be used
        writer.write_row(&mutation, &schema).unwrap();

        let bytes = writer.finish().unwrap();
        assert!(!bytes.is_empty());

        // Should have HAS_COMPLEX_DELETION flag
        let flags = bytes[0];
        assert_eq!(
            flags & ROW_HAS_COMPLEX_DELETION,
            ROW_HAS_COMPLEX_DELETION,
            "WriteWithTtl on SET should set HAS_COMPLEX_DELETION"
        );
    }

    #[test]
    fn test_delete_complex_column() {
        // Delete on a complex column should write complex deletion, not simple tombstone
        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![Column {
                name: "tags".to_string(),
                data_type: "set<text>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            }],
            comments: HashMap::new(),
        };

        let stats = create_test_stats();
        let mut writer = DataWriter::new(stats);

        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let mutation = Mutation::new(
            table_id,
            pk,
            None,
            vec![CellOperation::Delete {
                column: "tags".to_string(),
            }],
            1001000,
            None,
        );

        // Should succeed — uses complex deletion format
        writer.write_row(&mutation, &schema).unwrap();

        let bytes = writer.finish().unwrap();
        assert!(!bytes.is_empty());

        // Should have HAS_COMPLEX_DELETION flag
        let flags = bytes[0];
        assert_eq!(
            flags & ROW_HAS_COMPLEX_DELETION,
            ROW_HAS_COMPLEX_DELETION,
            "Delete on SET should set HAS_COMPLEX_DELETION"
        );
    }

    #[test]
    fn test_internal_type_string_complex_column() {
        // Cassandra internal type strings should be recognized as complex
        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![Column {
                name: "tags".to_string(),
                data_type: "org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UTF8Type)".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            }],
            comments: HashMap::new(),
        };

        let stats = create_test_stats();
        let mut writer = DataWriter::new(stats);

        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let mutation = Mutation::new(
            table_id,
            pk,
            None,
            vec![CellOperation::Write {
                column: "tags".to_string(),
                value: Value::Set(vec![Value::Text("test".to_string())]),
            }],
            1001000,
            None,
        );

        writer.write_row(&mutation, &schema).unwrap();

        let bytes = writer.finish().unwrap();
        let flags = bytes[0];
        assert_eq!(
            flags & ROW_HAS_COMPLEX_DELETION,
            ROW_HAS_COMPLEX_DELETION,
            "Internal type string should be recognized as complex column"
        );
    }

    /// Whether a simple (non-complex) cell value has a fixed byte size or a variable
    /// length encoded by a preceding unsigned VInt.
    ///
    /// Mirrors [`cell_value_uses_length_prefix`]: Boolean, Integer, BigInt, Float32,
    /// Float, Timestamp, and Uuid are fixed-size; everything else (Text, Blob, …) is
    /// variable and prefixed with a VUInt length.
    #[derive(Clone, Copy)]
    enum CellValueSizing {
        /// The value is exactly this many bytes (no length prefix).
        Fixed(usize),
        /// The value is prefixed by an unsigned VInt length.
        Variable,
    }

    /// Parse the output of `write_row` / `writer.finish()` and return the flags byte
    /// for each simple (non-complex) cell, in schema column order.
    ///
    /// This walks the deterministic row-header structure so wall-clock-derived bytes
    /// inside the TTL/LDT delta fields cannot be misidentified as cell-flag bytes:
    ///
    /// ```text
    /// [row_flags: u8]                        ← byte 0; no clustering prefix here
    /// [row_body_size: unsigned VInt]
    /// [prev_size: unsigned VInt]
    /// [timestamp_delta: unsigned VInt]       ← present when ROW_HAS_TIMESTAMP
    /// [ttl_delta: unsigned VInt]             ← present when ROW_HAS_TTL
    /// [ldt_delta: unsigned VInt]             ← present when ROW_HAS_TTL (wall-clock!)
    /// [column_bitmap: unsigned VInt]         ← present when NOT ROW_HAS_ALL_COLUMNS
    /// per cell (one per `column_sizings` entry):
    ///   [flags: u8]                          ← captured here
    ///   if NOT CELL_USE_ROW_TIMESTAMP:
    ///     [timestamp_delta: unsigned VInt]
    ///   if CELL_IS_DELETED:
    ///     [ldt_delta: unsigned VInt]
    ///   if NOT CELL_HAS_EMPTY_VALUE:
    ///     match sizing:
    ///       Variable  → [value_len: unsigned VInt] + [value_len bytes]
    ///       Fixed(n)  → [n bytes]
    /// ```
    ///
    /// `column_sizings` must list one entry per regular column in schema order.
    fn parse_simple_row_cell_flags(buf: &[u8], column_sizings: &[CellValueSizing]) -> Vec<u8> {
        fn read_uvint(buf: &[u8], pos: &mut usize) -> u64 {
            let first = buf[*pos];
            *pos += 1;
            if first == 0xFF {
                let mut v = 0u64;
                for _ in 0..8 {
                    v = (v << 8) | buf[*pos] as u64;
                    *pos += 1;
                }
                return v;
            }
            let extra = first.leading_ones() as usize;
            let mask = 0xFF_u8.wrapping_shr((extra + 1) as u32);
            let mut v = (first & mask) as u64;
            for _ in 0..extra {
                v = (v << 8) | buf[*pos] as u64;
                *pos += 1;
            }
            v
        }

        let mut pos = 0usize;

        // Row flags — byte 0, no clustering prefix for the test cases using this helper.
        let row_flags = buf[pos];
        pos += 1;

        // row_body_size + prev_size (two VInts we skip)
        read_uvint(buf, &mut pos); // row_body_size
        read_uvint(buf, &mut pos); // prev_size

        // Liveness timestamp delta
        if (row_flags & ROW_HAS_TIMESTAMP) != 0 {
            read_uvint(buf, &mut pos);
        }
        // TTL delta + LDT delta (LDT is wall-clock-derived — the source of flakiness)
        if (row_flags & ROW_HAS_TTL) != 0 {
            read_uvint(buf, &mut pos); // ttl_delta
            read_uvint(buf, &mut pos); // ldt_delta
        }
        // Deletion time (2 VInts)
        if (row_flags & ROW_HAS_DELETION) != 0 {
            read_uvint(buf, &mut pos);
            read_uvint(buf, &mut pos);
        }
        // Column bitmap (1 VInt; present when NOT ROW_HAS_ALL_COLUMNS)
        if (row_flags & ROW_HAS_ALL_COLUMNS) == 0 {
            read_uvint(buf, &mut pos);
        }

        // Now read one flags byte per column.
        let mut flags_out = Vec::with_capacity(column_sizings.len());
        for &sizing in column_sizings {
            let cell_flags = buf[pos];
            pos += 1;
            flags_out.push(cell_flags);

            // Skip timestamp delta when the cell carries its own timestamp.
            if (cell_flags & CELL_USE_ROW_TIMESTAMP) == 0 {
                read_uvint(buf, &mut pos);
            }
            // Tombstone cells carry an LDT delta.
            if (cell_flags & CELL_IS_DELETED) != 0 {
                read_uvint(buf, &mut pos);
            }
            // Skip value (absent when HAS_EMPTY_VALUE is set).
            if (cell_flags & CELL_HAS_EMPTY_VALUE) == 0 {
                match sizing {
                    CellValueSizing::Variable => {
                        let value_len = read_uvint(buf, &mut pos) as usize;
                        pos += value_len;
                    }
                    CellValueSizing::Fixed(n) => {
                        pos += n;
                    }
                }
            }
        }

        flags_out
    }

    /// Parse a `write_complex_column` output buffer and return the flag byte for every cell.
    ///
    /// The buffer has this deterministic structure:
    /// ```text
    /// [complex_deletion_ts_delta:  unsigned VInt]  ← 2 VInts, time-derived but fixed per stats
    /// [complex_deletion_ldt_delta: unsigned VInt]
    /// [cell_count: unsigned VInt]
    /// per cell:
    ///   [flags: u8]
    ///   if IS_EXPIRING (0x02 set):
    ///     [ts_delta:  unsigned VInt]
    ///     [ldt_delta: unsigned VInt]   ← wall-clock-derived
    ///     [ttl_delta: unsigned VInt]
    ///   [path_len:  unsigned VInt]
    ///   [path_bytes: path_len]
    ///   if !HAS_EMPTY_VALUE (0x04 NOT set):
    ///     [value_len: unsigned VInt]
    ///     [value_bytes: value_len]
    /// ```
    ///
    /// Scanning the raw buffer for a flag byte value is fragile because
    /// wall-clock-derived LDT bytes can coincidentally equal the flag byte (~1-2% of CI runs).
    /// This helper walks the structure deterministically so each flag byte is read at
    /// its exact position.
    fn parse_complex_cell_flags(buf: &[u8]) -> Vec<u8> {
        /// Read one unsigned VInt from `buf` starting at `*pos`; advance `*pos`.
        fn read_uvint(buf: &[u8], pos: &mut usize) -> u64 {
            let first = buf[*pos];
            *pos += 1;
            if first == 0xFF {
                // 9-byte form: 0xFF + 8 big-endian bytes
                let mut v = 0u64;
                for _ in 0..8 {
                    v = (v << 8) | buf[*pos] as u64;
                    *pos += 1;
                }
                return v;
            }
            // Count leading 1-bits in `first` to determine extra bytes
            let extra = first.leading_ones() as usize;
            // Data bits in first byte: mask off the leading 1s and the 0 separator
            let mask = 0xFF_u8.wrapping_shr((extra + 1) as u32);
            let mut v = (first & mask) as u64;
            for _ in 0..extra {
                v = (v << 8) | buf[*pos] as u64;
                *pos += 1;
            }
            v
        }

        let mut pos = 0usize;
        // Skip complex deletion header: 2 unsigned VInts
        read_uvint(buf, &mut pos);
        read_uvint(buf, &mut pos);

        // Cell count
        let cell_count = read_uvint(buf, &mut pos) as usize;

        let mut flags_out = Vec::with_capacity(cell_count);
        for _ in 0..cell_count {
            let flags = buf[pos];
            pos += 1;
            flags_out.push(flags);

            if (flags & CELL_IS_EXPIRING) != 0 {
                // IS_EXPIRING: ts_delta + ldt_delta + ttl_delta (3 unsigned VInts)
                read_uvint(buf, &mut pos);
                read_uvint(buf, &mut pos);
                read_uvint(buf, &mut pos);
            }
            // USE_ROW_TIMESTAMP / non-expiring cells: no extra fields before path

            // Cell path: path_len VInt + path_len bytes
            let path_len = read_uvint(buf, &mut pos) as usize;
            pos += path_len;

            // Cell value: only present when HAS_EMPTY_VALUE is NOT set
            if (flags & CELL_HAS_EMPTY_VALUE) == 0 {
                let value_len = read_uvint(buf, &mut pos) as usize;
                pos += value_len;
            }
        }

        flags_out
    }

    #[test]
    fn test_set_complex_column_with_ttl() {
        // SET with TTL should write IS_EXPIRING flag per cell, not USE_ROW_TIMESTAMP.
        // Uses structural parsing to read cell flags at their exact byte positions,
        // avoiding false positives from time-derived LDT bytes that can equal 0x02.
        let stats = create_test_stats();
        let writer = DataWriter::new(stats);

        let column = Column {
            name: "tags".to_string(),
            data_type: "set<text>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        };

        let value = Value::Set(vec![
            Value::Text("alpha".to_string()),
            Value::Text("beta".to_string()),
        ]);

        let mut buf = Vec::new();
        writer
            .write_complex_column(&mut buf, &column, &value, 1001000, Some(3600))
            .unwrap();

        // Parse cell flags structurally so wall-clock LDT bytes in the header and
        // per-cell TTL fields cannot be misidentified as flag bytes.
        let cell_flags = parse_complex_cell_flags(&buf);
        let expected_flags = CELL_IS_EXPIRING | CELL_HAS_EMPTY_VALUE; // 0x06

        assert_eq!(
            cell_flags.len(),
            2,
            "SET with 2 elements should produce 2 cells"
        );
        assert!(
            cell_flags.iter().all(|&f| f == expected_flags),
            "SET with TTL: all cells should have IS_EXPIRING | HAS_EMPTY_VALUE (0x06), got: {:?}",
            cell_flags
        );

        // Confirm absence of USE_ROW_TIMESTAMP on all cells
        assert!(
            cell_flags
                .iter()
                .all(|&f| (f & CELL_USE_ROW_TIMESTAMP) == 0),
            "SET with TTL should NOT have USE_ROW_TIMESTAMP on any cell, got: {:?}",
            cell_flags
        );
    }

    #[test]
    fn test_map_complex_column_with_ttl() {
        // MAP with TTL should write IS_EXPIRING flag per cell.
        // Uses structural parsing to read cell flags at their exact byte positions,
        // avoiding false positives from time-derived LDT bytes that can equal 0x02.
        let stats = create_test_stats();
        let writer = DataWriter::new(stats);

        let column = Column {
            name: "props".to_string(),
            data_type: "map<text, int>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        };

        let value = Value::Map(vec![(Value::Text("key1".to_string()), Value::Integer(100))]);

        let mut buf = Vec::new();
        writer
            .write_complex_column(&mut buf, &column, &value, 1001000, Some(7200))
            .unwrap();

        // Parse cell flags structurally so wall-clock LDT bytes cannot be
        // misidentified as IS_EXPIRING (0x02) flag bytes.
        let cell_flags = parse_complex_cell_flags(&buf);

        assert_eq!(
            cell_flags.len(),
            1,
            "MAP with 1 entry should produce 1 cell"
        );
        assert_eq!(
            cell_flags[0] & CELL_IS_EXPIRING,
            CELL_IS_EXPIRING,
            "MAP with TTL: cell should have IS_EXPIRING flag set, got flags byte: 0x{:02X}",
            cell_flags[0]
        );
        assert_eq!(
            cell_flags[0] & CELL_HAS_EMPTY_VALUE,
            0,
            "MAP with TTL: cell should NOT have HAS_EMPTY_VALUE, got flags byte: 0x{:02X}",
            cell_flags[0]
        );
    }

    #[test]
    fn test_list_complex_column_with_ttl() {
        // LIST with TTL should write IS_EXPIRING per cell, producing a larger
        // output than without TTL (extra timestamp/LDT/TTL delta fields).
        // Uses structural parsing to read cell flags at their exact byte positions,
        // avoiding false positives from time-derived LDT bytes.
        let stats = create_test_stats();
        let writer_ttl = DataWriter::new(stats.clone());
        let writer_no_ttl = DataWriter::new(stats);

        let column = Column {
            name: "items".to_string(),
            data_type: "list<int>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        };

        let value = Value::List(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ]);

        let mut buf_ttl = Vec::new();
        writer_ttl
            .write_complex_column(&mut buf_ttl, &column, &value, 1001000, Some(1800))
            .unwrap();

        let mut buf_no_ttl = Vec::new();
        writer_no_ttl
            .write_complex_column(&mut buf_no_ttl, &column, &value, 1001000, None)
            .unwrap();

        // TTL version must be larger: each cell gets timestamp + LDT + TTL deltas
        // instead of just USE_ROW_TIMESTAMP flag.
        assert!(
            buf_ttl.len() > buf_no_ttl.len(),
            "LIST with TTL ({} bytes) should be larger than without TTL ({} bytes)",
            buf_ttl.len(),
            buf_no_ttl.len()
        );

        // Structurally verify IS_EXPIRING is set on every cell in the TTL version.
        let cell_flags_ttl = parse_complex_cell_flags(&buf_ttl);
        assert_eq!(
            cell_flags_ttl.len(),
            3,
            "LIST with 3 elements should produce 3 cells"
        );
        assert!(
            cell_flags_ttl.iter().all(|&f| (f & CELL_IS_EXPIRING) != 0),
            "LIST with TTL: all cells should have IS_EXPIRING flag set, got: {:?}",
            cell_flags_ttl
        );

        // Verify the no-TTL version uses USE_ROW_TIMESTAMP instead.
        let cell_flags_no_ttl = parse_complex_cell_flags(&buf_no_ttl);
        assert_eq!(cell_flags_no_ttl.len(), 3);
        assert!(
            cell_flags_no_ttl
                .iter()
                .all(|&f| (f & CELL_IS_EXPIRING) == 0),
            "LIST without TTL: no cells should have IS_EXPIRING flag, got: {:?}",
            cell_flags_no_ttl
        );
    }

    #[test]
    fn test_complex_column_no_ttl_uses_row_timestamp() {
        // Regression: without TTL, cells should still use USE_ROW_TIMESTAMP
        let stats = create_test_stats();
        let writer = DataWriter::new(stats);

        let column = Column {
            name: "tags".to_string(),
            data_type: "set<text>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        };

        let value = Value::Set(vec![Value::Text("x".to_string())]);

        let mut buf = Vec::new();
        writer
            .write_complex_column(&mut buf, &column, &value, 1001000, None)
            .unwrap();

        // Without TTL: USE_ROW_TIMESTAMP | HAS_EMPTY_VALUE = 0x0C.
        // Use structural parse so DeletionTime.LIVE header bytes are not misidentified.
        let expected_flags = CELL_USE_ROW_TIMESTAMP | CELL_HAS_EMPTY_VALUE;
        let cell_flags = parse_complex_cell_flags(&buf);
        assert_eq!(
            cell_flags.len(),
            1,
            "SET with 1 element should produce 1 cell"
        );
        assert_eq!(
            cell_flags[0], expected_flags,
            "Without TTL, SET cells should use USE_ROW_TIMESTAMP | HAS_EMPTY_VALUE, got: 0x{:02X}",
            cell_flags[0]
        );
    }

    #[test]
    fn test_bitmap_includes_deleted_columns() {
        // Delete operations should mark columns as present in the bitmap
        let schema = TableSchema {
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
                    name: "age".to_string(),
                    data_type: "int".to_string(),
                    nullable: true,
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
        };

        let stats = create_test_stats();
        let writer = DataWriter::new(stats);

        // Write "name" and delete "age"
        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let mutation = Mutation::new(
            table_id,
            pk,
            None,
            vec![
                CellOperation::Delete {
                    column: "age".to_string(),
                },
                CellOperation::Write {
                    column: "name".to_string(),
                    value: Value::Text("Alice".to_string()),
                },
            ],
            1001000,
            None,
        );

        // Write bitmap — both columns should be present (bitmap = 0)
        let mut buf = Vec::new();
        writer
            .write_column_bitmap(&mut buf, &mutation, &schema)
            .unwrap();

        // bitmap = 0 means all columns present (no MISSING bits set)
        // Since we have 2 regular columns and both are in operations,
        // all should be marked present
        assert_eq!(buf.len(), 1, "Bitmap should be a single byte");
        assert_eq!(
            buf[0], 0,
            "Bitmap should be 0 (all columns present) when both write and delete cover all columns"
        );
    }

    #[test]
    fn test_bitmap_delete_only_column_is_present() {
        // A column that ONLY has a Delete should still be marked present
        let schema = TableSchema {
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
                    name: "age".to_string(),
                    data_type: "int".to_string(),
                    nullable: true,
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
        };

        let stats = create_test_stats();
        let writer = DataWriter::new(stats);

        // Only delete "age", don't write "name"
        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let mutation = Mutation::new(
            table_id,
            pk,
            None,
            vec![CellOperation::Delete {
                column: "age".to_string(),
            }],
            1001000,
            None,
        );

        let mut buf = Vec::new();
        writer
            .write_column_bitmap(&mut buf, &mutation, &schema)
            .unwrap();

        // Regular columns sorted alphabetically: [age, name]
        // age (idx 0) = present (Delete), name (idx 1) = missing
        // bitmap bit 1 = 1, bit 0 = 0 → bitmap = 0b10 = 2
        assert_eq!(buf.len(), 1);
        assert_eq!(
            buf[0], 2,
            "Bitmap should mark 'name' as missing (bit 1) but 'age' as present (bit 0)"
        );
    }

    // ========== Issue #492: streaming DataWriter tests ==========

    /// Build a deterministic set of partitions used by the streaming tests.
    fn streaming_test_partitions() -> Vec<(DecoratedKey, Vec<Mutation>)> {
        let table_id = TableId::new("test_ks", "test_table");
        (0..16u32)
            .map(|i| {
                let key = DecoratedKey::new(i as i64, i.to_be_bytes().to_vec());
                let pk = PartitionKey::single("id", Value::Integer(i as i32));
                let mutation = Mutation::new(
                    table_id.clone(),
                    pk,
                    None,
                    vec![CellOperation::Write {
                        column: "name".to_string(),
                        value: Value::Text(format!("partition-{i}")),
                    }],
                    1_001_000 + i as i64,
                    None,
                );
                (key, vec![mutation])
            })
            .collect()
    }

    /// Byte-identical guard (Issue #492): the streaming writer (flushing each
    /// partition to a file) must produce a Data.db byte sequence that is
    /// identical to the legacy in-memory writer, and the returned partition
    /// offsets must match exactly. Anything else breaks Index.db offsets.
    #[test]
    fn test_streaming_writer_byte_identical_to_in_memory() {
        let schema = create_test_schema();
        let partitions = streaming_test_partitions();

        // In-memory reference: accumulate every partition in `buffer`.
        let mut mem_writer = DataWriter::new(create_test_stats());
        let mut mem_offsets = Vec::new();
        for (key, mutations) in &partitions {
            mem_offsets.push(
                mem_writer
                    .write_partition(key, mutations, &schema, None, &[])
                    .unwrap(),
            );
        }
        let expected_bytes = mem_writer.finish().unwrap();

        // Streaming: flush each partition to a temp Data.db file.
        let dir = tempfile::tempdir().unwrap();
        let data_path = dir.path().join("nb-1-big-Data.db");
        let mut stream_writer = DataWriter::with_sink(create_test_stats(), data_path.clone());
        let mut stream_offsets = Vec::new();
        for (key, mutations) in &partitions {
            stream_offsets.push(
                stream_writer
                    .write_partition(key, mutations, &schema, None, &[])
                    .unwrap(),
            );
        }
        let data_size = stream_writer.finish_streaming().unwrap();

        // Offsets returned to the caller (fed to Index.db) must be identical.
        assert_eq!(
            stream_offsets, mem_offsets,
            "streaming partition offsets must equal in-memory offsets"
        );

        // The on-disk Data.db must be byte-for-byte identical to the in-memory
        // bytes, and the reported data_size must match the file length.
        let on_disk = std::fs::read(&data_path).unwrap();
        assert_eq!(
            on_disk, expected_bytes,
            "streamed Data.db must be byte-identical to in-memory Data.db"
        );
        assert_eq!(
            data_size as usize,
            expected_bytes.len(),
            "finish_streaming() data_size must equal file length"
        );

        // Every returned offset must point at the actual start byte in the file:
        // a partition starts with its 2-byte key length, here always 0x0004.
        for &off in &stream_offsets {
            assert_eq!(
                &on_disk[off as usize..off as usize + 2],
                &[0x00, 0x04],
                "offset {off} must land on a partition's key-length prefix"
            );
        }
    }

    /// Bounded-memory evidence (Issue #492): after each `write_partition` the
    /// scratch buffer must hold only the most recent partition, while the
    /// flushed `position` grows monotonically. This is the proof that peak heap
    /// is O(largest partition) rather than O(file).
    #[test]
    fn test_streaming_writer_bounds_memory_to_one_partition() {
        let schema = create_test_schema();
        let partitions = streaming_test_partitions();

        let dir = tempfile::tempdir().unwrap();
        let data_path = dir.path().join("nb-1-big-Data.db");
        let mut writer = DataWriter::with_sink(create_test_stats(), data_path);

        let mut prev_flushed = 0u64;
        // Tracks the largest single-partition flushed size. Because the scratch is
        // cleared after every partition (asserted below), peak resident Data.db
        // bytes are bounded by this value, not the whole file.
        let mut max_partition_size = 0usize;
        for (i, (key, mutations)) in partitions.iter().enumerate() {
            let flushed_before = writer.flushed_position();
            writer
                .write_partition(key, mutations, &schema, None, &[])
                .unwrap();

            // After a partition is written it has been flushed and the scratch
            // cleared: the scratch must be empty, never accumulating prior
            // partitions.
            assert_eq!(
                writer.scratch_len(),
                0,
                "scratch must be cleared after partition {i} (bounded memory)"
            );

            // Flushed bytes must strictly increase by this partition's size.
            let flushed_after = writer.flushed_position();
            assert!(
                flushed_after > flushed_before,
                "flushed position must grow after writing partition {i}"
            );
            let this_partition_size = (flushed_after - flushed_before) as usize;
            max_partition_size = max_partition_size.max(this_partition_size);
            assert!(flushed_after > prev_flushed);
            prev_flushed = flushed_after;
        }

        let total = writer.finish_streaming().unwrap();
        assert_eq!(
            total, prev_flushed,
            "total size must equal last flushed pos"
        );

        // Peak resident bytes were bounded by the largest single partition,
        // which is far smaller than the whole file for many partitions.
        assert!(
            (max_partition_size as u64) < total,
            "largest single partition ({max_partition_size}) must be smaller than the full file ({total})"
        );
    }

    // ===================================================================
    // Epic #899 Phase B — per-element complex-column writer capability.
    //
    // These tests exercise the WRITER capability directly (the new
    // `WriteComplexElement` / `ComplexDeletion` ops are NOT yet emitted by the
    // real `merge_entry_to_mutation` pipeline — that flip is Phase C). They
    // assert the writer emits CORRECT per-element bytes:
    //   (a) two elements at DIFFERENT per-element timestamps → two cells with
    //       explicit (non-row) timestamp deltas, not one promoted timestamp;
    //   (b) a REAL complex deletion marker (not the LIVE sentinel) followed by
    //       surviving per-element cells;
    //   (c) a LIST element's source 16-byte cell path round-trips byte-for-byte.
    // ===================================================================

    /// One fully-decoded complex cell from a `write_complex_column_per_element`
    /// output buffer, for byte-level assertions.
    #[derive(Debug)]
    struct DecodedComplexCell {
        flags: u8,
        /// Absolute timestamp delta from `min_timestamp` (only when an explicit
        /// timestamp was written, i.e. NOT USE_ROW_TIMESTAMP).
        ts_delta: Option<u64>,
        /// LDT delta from `min_local_deletion_time` (only when deleted/expiring
        /// and not USE_ROW_TTL).
        ldt_delta: Option<u64>,
        /// TTL delta from `min_ttl` (only when expiring and not USE_ROW_TTL).
        ttl_delta: Option<u64>,
        cell_path: Vec<u8>,
        value: Option<Vec<u8>>,
    }

    /// Decode `(complex_deletion_ts_delta, complex_deletion_ldt_delta, cells)`
    /// from a per-element complex-column buffer, walking the exact wire format
    /// the reader (`parse_complex_cell_value`) parses.
    fn decode_complex_column(buf: &[u8]) -> (u64, u64, Vec<DecodedComplexCell>) {
        fn read_uvint(buf: &[u8], pos: &mut usize) -> u64 {
            let first = buf[*pos];
            *pos += 1;
            if first == 0xFF {
                let mut v = 0u64;
                for _ in 0..8 {
                    v = (v << 8) | buf[*pos] as u64;
                    *pos += 1;
                }
                return v;
            }
            let extra = first.leading_ones() as usize;
            let mask = 0xFF_u8.wrapping_shr((extra + 1) as u32);
            let mut v = (first & mask) as u64;
            for _ in 0..extra {
                v = (v << 8) | buf[*pos] as u64;
                *pos += 1;
            }
            v
        }

        let mut pos = 0usize;
        let del_ts = read_uvint(buf, &mut pos);
        let del_ldt = read_uvint(buf, &mut pos);
        let cell_count = read_uvint(buf, &mut pos) as usize;

        let mut cells = Vec::with_capacity(cell_count);
        for _ in 0..cell_count {
            let flags = buf[pos];
            pos += 1;
            let is_deleted = (flags & CELL_IS_DELETED) != 0;
            let is_expiring = (flags & CELL_IS_EXPIRING) != 0;
            let has_empty_value = (flags & CELL_HAS_EMPTY_VALUE) != 0;
            let use_row_ts = (flags & CELL_USE_ROW_TIMESTAMP) != 0;
            let use_row_ttl = (flags & CELL_USE_ROW_TTL) != 0;

            let ts_delta = if !use_row_ts {
                Some(read_uvint(buf, &mut pos))
            } else {
                None
            };
            let ldt_delta = if !use_row_ttl && (is_deleted || is_expiring) {
                Some(read_uvint(buf, &mut pos))
            } else {
                None
            };
            let ttl_delta = if !use_row_ttl && is_expiring {
                Some(read_uvint(buf, &mut pos))
            } else {
                None
            };

            let path_len = read_uvint(buf, &mut pos) as usize;
            let cell_path = buf[pos..pos + path_len].to_vec();
            pos += path_len;

            let value = if is_deleted || has_empty_value {
                None
            } else {
                let value_len = read_uvint(buf, &mut pos) as usize;
                let v = buf[pos..pos + value_len].to_vec();
                pos += value_len;
                Some(v)
            };

            cells.push(DecodedComplexCell {
                flags,
                ts_delta,
                ldt_delta,
                ttl_delta,
                cell_path,
                value,
            });
        }
        (del_ts, del_ldt, cells)
    }

    fn set_column(name: &str) -> Column {
        Column {
            name: name.to_string(),
            data_type: "set<int>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        }
    }

    fn list_column(name: &str) -> Column {
        Column {
            name: name.to_string(),
            data_type: "list<int>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        }
    }

    /// (a) Two elements at DIFFERENT per-element timestamps must produce two
    /// cells, each carrying its OWN explicit timestamp delta (NOT
    /// USE_ROW_TIMESTAMP, NOT a single promoted row timestamp).
    #[test]
    fn per_element_distinct_timestamps_emit_explicit_deltas() {
        let mut stats = StatisticsMetadata::new();
        stats.min_timestamp = 1_000_000;
        stats.min_ttl = 0;
        stats.min_local_deletion_time = 0;
        let writer = DataWriter::new(stats);

        let column = set_column("tags");
        // Row liveness timestamp differs from BOTH element timestamps, so neither
        // element may use USE_ROW_TIMESTAMP.
        let row_ts = 1_000_000i64;
        let elem_a = ComplexElementWrite {
            cell_path: serialize_collection_element(&Value::Integer(10), "SET").unwrap(),
            value: None, // SET element: empty value
            timestamp_micros: 1_005_000,
            ttl_seconds: None,
            local_deletion_time: None,
            is_deleted: false,
        };
        let elem_b = ComplexElementWrite {
            cell_path: serialize_collection_element(&Value::Integer(20), "SET").unwrap(),
            value: None,
            timestamp_micros: 1_009_000,
            ttl_seconds: None,
            local_deletion_time: None,
            is_deleted: false,
        };

        let mut buf = Vec::new();
        writer
            .write_complex_column_per_element(&mut buf, &column, None, &[elem_a, elem_b], row_ts)
            .unwrap();

        let (_del_ts, _del_ldt, cells) = decode_complex_column(&buf);
        assert_eq!(cells.len(), 2, "two SET elements => two cells");
        for c in &cells {
            assert_eq!(
                c.flags & CELL_USE_ROW_TIMESTAMP,
                0,
                "element ts differs from row ts => USE_ROW_TIMESTAMP must be CLEARED, flags=0x{:02x}",
                c.flags
            );
            assert!(
                c.ts_delta.is_some(),
                "an explicit per-element timestamp delta must be written"
            );
        }
        // The two distinct timestamps must survive as two DISTINCT deltas — not
        // collapsed/promoted to one.
        assert_eq!(cells[0].ts_delta, Some(5_000));
        assert_eq!(cells[1].ts_delta, Some(9_000));
        assert_ne!(
            cells[0].ts_delta, cells[1].ts_delta,
            "disjoint per-element timestamps must NOT be promoted to one"
        );
    }

    /// An element whose per-element timestamp EQUALS the row timestamp keeps
    /// USE_ROW_TIMESTAMP (0x08) and writes no explicit delta; a sibling at a
    /// different timestamp clears it. (Mixed case in one column.)
    #[test]
    fn per_element_row_timestamp_kept_only_when_equal() {
        let mut stats = StatisticsMetadata::new();
        stats.min_timestamp = 1_000_000;
        stats.min_ttl = 0;
        stats.min_local_deletion_time = 0;
        let writer = DataWriter::new(stats);

        let column = set_column("tags");
        let row_ts = 1_007_000i64;
        let same = ComplexElementWrite {
            cell_path: serialize_collection_element(&Value::Integer(10), "SET").unwrap(),
            value: None,
            timestamp_micros: row_ts, // equal to row ts
            ttl_seconds: None,
            local_deletion_time: None,
            is_deleted: false,
        };
        let diff = ComplexElementWrite {
            cell_path: serialize_collection_element(&Value::Integer(20), "SET").unwrap(),
            value: None,
            timestamp_micros: 1_009_000, // != row ts
            ttl_seconds: None,
            local_deletion_time: None,
            is_deleted: false,
        };

        let mut buf = Vec::new();
        writer
            .write_complex_column_per_element(&mut buf, &column, None, &[same, diff], row_ts)
            .unwrap();

        let (_del_ts, _del_ldt, cells) = decode_complex_column(&buf);
        assert_eq!(cells.len(), 2);
        // Cell for element 10 (path-sorted first): equals row ts → USE_ROW_TIMESTAMP.
        assert_ne!(cells[0].flags & CELL_USE_ROW_TIMESTAMP, 0);
        assert_eq!(cells[0].ts_delta, None);
        // Cell for element 20: differs → explicit delta.
        assert_eq!(cells[1].flags & CELL_USE_ROW_TIMESTAMP, 0);
        assert_eq!(cells[1].ts_delta, Some(9_000));
    }

    /// (b) A REAL complex deletion marker (markedForDeleteAt + localDeletionTime,
    /// NOT the LIVE sentinel) must be written, followed by surviving cells.
    #[test]
    fn per_element_real_complex_deletion_then_surviving_cells() {
        let mut stats = StatisticsMetadata::new();
        stats.min_timestamp = 1_000_000;
        stats.min_ttl = 0;
        stats.min_local_deletion_time = 1_700_000_000;
        let writer = DataWriter::new(stats);

        let column = set_column("tags");
        let row_ts = 1_012_000i64;
        let mfda = 1_010_000i64; // markedForDeleteAt
        let ldt = 1_700_000_005i32; // localDeletionTime (seconds)

        // One element survives the complex deletion (written after mfda).
        let survivor = ComplexElementWrite {
            cell_path: serialize_collection_element(&Value::Integer(30), "SET").unwrap(),
            value: None,
            timestamp_micros: row_ts,
            ttl_seconds: None,
            local_deletion_time: None,
            is_deleted: false,
        };

        let mut buf = Vec::new();
        writer
            .write_complex_column_per_element(
                &mut buf,
                &column,
                Some((mfda, ldt)),
                &[survivor],
                row_ts,
            )
            .unwrap();

        let (del_ts, del_ldt, cells) = decode_complex_column(&buf);

        // LIVE sentinel deltas (what the old hardcoded path wrote):
        let live_ts_delta = i64::MIN.wrapping_sub(1_000_000) as u64;
        let live_ldt_delta = i32::MAX.wrapping_sub(1_700_000_000) as u32 as u64;
        assert_ne!(
            del_ts, live_ts_delta,
            "must NOT be the LIVE markedForDeleteAt sentinel"
        );
        assert_ne!(
            del_ldt, live_ldt_delta,
            "must NOT be the LIVE localDeletionTime sentinel"
        );
        // Real deletion deltas (unsigned VInt against seeded baselines).
        assert_eq!(del_ts, (mfda - 1_000_000) as u64);
        assert_eq!(del_ldt, (ldt - 1_700_000_000) as u64);
        // The surviving element is still emitted after the marker.
        assert_eq!(
            cells.len(),
            1,
            "the surviving element must follow the marker"
        );
    }

    /// (c) A LIST element's source 16-byte cell path must round-trip byte-for-byte
    /// (it is the preserved TimeUUID, NOT a freshly generated one).
    #[test]
    fn per_element_list_cell_path_roundtrips_byte_for_byte() {
        let mut stats = StatisticsMetadata::new();
        stats.min_timestamp = 1_000_000;
        stats.min_ttl = 0;
        stats.min_local_deletion_time = 0;
        let writer = DataWriter::new(stats);

        let column = list_column("items");
        let row_ts = 1_003_000i64;

        // A specific, recognizable 16-byte TimeUUID we must NOT regenerate.
        let source_path: Vec<u8> = vec![
            0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE,
            0xFF, 0x01,
        ];
        let elem = ComplexElementWrite {
            cell_path: source_path.clone(),
            value: Some(Value::Integer(42)),
            timestamp_micros: row_ts,
            ttl_seconds: None,
            local_deletion_time: None,
            is_deleted: false,
        };

        let mut buf = Vec::new();
        writer
            .write_complex_column_per_element(&mut buf, &column, None, &[elem], row_ts)
            .unwrap();

        let (_del_ts, _del_ldt, cells) = decode_complex_column(&buf);
        assert_eq!(cells.len(), 1);
        assert_eq!(
            cells[0].cell_path, source_path,
            "the source 16-byte LIST cell path must round-trip byte-for-byte (not regenerated)"
        );
        assert_eq!(
            cells[0].value,
            Some(serialize_value(&Value::Integer(42)).unwrap()),
            "LIST element value must be serialized after the preserved path"
        );
    }

    /// An element-level tombstone (`value == None`, `is_deleted`) writes
    /// IS_DELETED (0x01), an explicit ts (when != row ts) and an LDT, and no
    /// value bytes.
    #[test]
    fn per_element_element_tombstone_writes_is_deleted_and_ldt() {
        let mut stats = StatisticsMetadata::new();
        stats.min_timestamp = 1_000_000;
        stats.min_ttl = 0;
        stats.min_local_deletion_time = 1_700_000_000;
        let writer = DataWriter::new(stats);

        let column = list_column("items");
        let row_ts = 1_000_000i64;
        let elem = ComplexElementWrite {
            cell_path: vec![0xAB; 16],
            value: None,
            timestamp_micros: 1_004_000,
            ttl_seconds: None,
            local_deletion_time: Some(1_700_000_009),
            is_deleted: true,
        };

        let mut buf = Vec::new();
        writer
            .write_complex_column_per_element(&mut buf, &column, None, &[elem], row_ts)
            .unwrap();

        let (_del_ts, _del_ldt, cells) = decode_complex_column(&buf);
        assert_eq!(cells.len(), 1);
        assert_ne!(
            cells[0].flags & CELL_IS_DELETED,
            0,
            "IS_DELETED must be set"
        );
        assert_eq!(cells[0].ts_delta, Some(4_000));
        assert_eq!(
            cells[0].ldt_delta,
            Some((1_700_000_009 - 1_700_000_000) as u64)
        );
        assert_eq!(cells[0].ttl_delta, None, "a tombstone is not expiring");
        assert!(cells[0].value.is_none(), "tombstone writes no value bytes");
        assert_eq!(cells[0].cell_path, vec![0xAB; 16]);
    }

    /// An expiring per-element write emits IS_EXPIRING with explicit ts + ldt +
    /// ttl deltas (against the seeded baselines).
    #[test]
    fn per_element_expiring_writes_ts_ldt_ttl_deltas() {
        let mut stats = StatisticsMetadata::new();
        stats.min_timestamp = 1_000_000;
        stats.min_ttl = 100;
        stats.min_local_deletion_time = 1_700_000_000;
        let writer = DataWriter::new(stats);

        let column = list_column("items");
        let row_ts = 1_000_000i64;
        let elem = ComplexElementWrite {
            cell_path: vec![0xCD; 16],
            value: Some(Value::Integer(7)),
            timestamp_micros: 1_006_000,
            ttl_seconds: Some(3_600),
            local_deletion_time: Some(1_700_003_600),
            is_deleted: false,
        };

        let mut buf = Vec::new();
        writer
            .write_complex_column_per_element(&mut buf, &column, None, &[elem], row_ts)
            .unwrap();

        let (_del_ts, _del_ldt, cells) = decode_complex_column(&buf);
        assert_eq!(cells.len(), 1);
        assert_ne!(
            cells[0].flags & CELL_IS_EXPIRING,
            0,
            "IS_EXPIRING must be set"
        );
        assert_eq!(cells[0].ts_delta, Some(6_000));
        assert_eq!(
            cells[0].ldt_delta,
            Some((1_700_003_600 - 1_700_000_000) as u64)
        );
        assert_eq!(cells[0].ttl_delta, Some((3_600 - 100) as u64));
        assert_eq!(
            cells[0].value,
            Some(serialize_value(&Value::Integer(7)).unwrap())
        );
    }
}
