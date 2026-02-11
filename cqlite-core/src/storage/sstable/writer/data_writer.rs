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
use crate::storage::serialization::vint::{encode_signed, encode_unsigned};
use crate::storage::sstable::writer::stats_writer::StatisticsMetadata;
use crate::storage::write_engine::mutation::{
    ClusteringBound, DecoratedKey, Mutation, PartitionTombstone, RangeTombstone,
};
use crate::types::{ComparatorType, UdtTypeDef, Value};
use std::io::Write;

// Row header flag constants (from V5CompressedLegacy parser)
const ROW_HAS_TIMESTAMP: u8 = 0x04;
const ROW_HAS_TTL: u8 = 0x08;
#[allow(dead_code)]
const ROW_HAS_DELETION: u8 = 0x10;
const ROW_HAS_ALL_COLUMNS: u8 = 0x20;
#[allow(dead_code)]
const ROW_HAS_COMPLEX_DELETION: u8 = 0x40;
const ROW_HAS_EXTENDED_FLAGS: u8 = 0x80;

// Extended flag constants (when ROW_HAS_EXTENDED_FLAGS is set)
const EXTENDED_IS_STATIC: u8 = 0x01;

// Cell flag constants (from V5CompressedLegacy parser)
const CELL_IS_DELETED: u8 = 0x01;
#[allow(dead_code)]
const CELL_IS_EXPIRING: u8 = 0x02;
const CELL_HAS_EMPTY_VALUE: u8 = 0x04;
const CELL_USE_ROW_TIMESTAMP: u8 = 0x08;
#[allow(dead_code)]
const CELL_USE_ROW_TTL: u8 = 0x10;

// Range tombstone marker constants
const IS_MARKER: u8 = 0x02;

// Range tombstone bound kinds
#[allow(dead_code)]
const INCL_START_BOUND: u8 = 0; // Inclusive start bound
#[allow(dead_code)]
const EXCL_START_BOUND: u8 = 1; // Exclusive start bound
#[allow(dead_code)]
const INCL_END_BOUND: u8 = 2; // Inclusive end bound
#[allow(dead_code)]
const EXCL_END_BOUND: u8 = 3; // Exclusive end bound
#[allow(dead_code)]
const START_BOUNDARY: u8 = 4; // Bottom (start of partition)
#[allow(dead_code)]
const END_BOUNDARY: u8 = 5; // Top (end of partition)

// Partition/row markers
const END_OF_PARTITION: u8 = 0x01;

/// Data.db component writer
///
/// Writes partitions and rows in V5CompressedLegacy format with delta encoding.
/// Caller must provide partitions in token order and rows in clustering order.
#[derive(Debug)]
pub struct DataWriter {
    /// Output buffer for Data.db content
    buffer: Vec<u8>,
    /// Statistics metadata for delta encoding
    stats: StatisticsMetadata,
}

impl DataWriter {
    /// Create a new Data.db writer
    ///
    /// # Arguments
    /// * `stats` - Statistics metadata for delta encoding baselines
    pub fn new(stats: StatisticsMetadata) -> Self {
        Self {
            buffer: Vec::new(),
            stats,
        }
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
        let partition_offset = self.buffer.len() as u64;

        // Write partition header (with optional tombstone)
        self.write_partition_header(key, partition_tombstone)?;

        // Write range tombstones before rows
        for rt in range_tombstones {
            self.write_range_tombstone(rt, schema)?;
        }

        // Write all rows for this partition
        for mutation in mutations {
            self.write_row(mutation, schema)?;
        }

        // Write end-of-partition marker
        self.buffer.push(END_OF_PARTITION);

        Ok(partition_offset)
    }

    /// Finish writing and return the Data.db bytes
    pub fn finish(self) -> Result<Vec<u8>> {
        Ok(self.buffer)
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
    fn write_row(&mut self, mutation: &Mutation, schema: &TableSchema) -> Result<()> {
        // Build row header flags
        let mut flags = 0u8;

        // Check if this is a row tombstone
        let is_row_tombstone = mutation.operations.iter().any(|op| {
            matches!(
                op,
                crate::storage::write_engine::mutation::CellOperation::DeleteRow
            )
        });

        if is_row_tombstone {
            flags |= ROW_HAS_DELETION; // 0x10
        }

        // Timestamp is always present for writes and row tombstones
        flags |= ROW_HAS_TIMESTAMP;

        // TTL if present (not applicable to row tombstones)
        if !is_row_tombstone && mutation.ttl_seconds.is_some() {
            flags |= ROW_HAS_TTL;
        }

        // All columns present if all operations are writes (no deletes) AND no NULLs
        if !is_row_tombstone {
            let all_writes = mutation.operations.iter().all(|op| {
                matches!(
                    op,
                    crate::storage::write_engine::mutation::CellOperation::Write { .. }
                        | crate::storage::write_engine::mutation::CellOperation::WriteWithTtl { .. }
                )
            });
            let has_nulls = mutation.operations.iter().any(|op| match op {
                crate::storage::write_engine::mutation::CellOperation::Write { value, .. }
                | crate::storage::write_engine::mutation::CellOperation::WriteWithTtl {
                    value,
                    ..
                } => {
                    matches!(value, Value::Null)
                }
                _ => false,
            });
            // Count regular columns only (exclude PK, CK, and static)
            let regular_column_count = self.regular_columns(schema).len();

            if all_writes && !has_nulls && mutation.operations.len() == regular_column_count {
                flags |= ROW_HAS_ALL_COLUMNS;
            }
        }

        // Write row flags
        self.buffer.push(flags);

        // Write clustering prefix if present (before row_size)
        if let Some(ref clustering_key) = mutation.clustering_key {
            self.write_clustering_prefix(clustering_key, schema)?;
        }

        // Calculate row body size (everything after row_size VInt)
        let row_body = self.build_row_body(mutation, schema, flags)?;

        // Write row_size (VInt)
        let mut row_size_buf = Vec::new();
        encode_unsigned(row_body.len() as u64, &mut row_size_buf);
        self.buffer.extend_from_slice(&row_size_buf);

        // Write prev_size (VInt, 0 for now - could optimize with prev row tracking)
        let mut prev_size_buf = Vec::new();
        encode_unsigned(0, &mut prev_size_buf);
        self.buffer.extend_from_slice(&prev_size_buf);

        // Write row body
        self.buffer.extend_from_slice(&row_body);

        Ok(())
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
        // Build row header flags - always includes HAS_EXTENDED_FLAGS for static rows
        let mut flags = ROW_HAS_EXTENDED_FLAGS;

        // Check if this is a row tombstone
        let is_row_tombstone = mutation.operations.iter().any(|op| {
            matches!(
                op,
                crate::storage::write_engine::mutation::CellOperation::DeleteRow
            )
        });

        if is_row_tombstone {
            flags |= ROW_HAS_DELETION;
        }

        // Timestamp is always present for static rows
        flags |= ROW_HAS_TIMESTAMP;

        // TTL if present (not applicable to row tombstones)
        if !is_row_tombstone && mutation.ttl_seconds.is_some() {
            flags |= ROW_HAS_TTL;
        }

        // Check if all static columns are present
        if !is_row_tombstone {
            let all_writes = mutation.operations.iter().all(|op| {
                matches!(
                    op,
                    crate::storage::write_engine::mutation::CellOperation::Write { .. }
                        | crate::storage::write_engine::mutation::CellOperation::WriteWithTtl { .. }
                )
            });
            let has_nulls = mutation.operations.iter().any(|op| match op {
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

            if all_writes && !has_nulls && mutation.operations.len() == static_column_count {
                flags |= ROW_HAS_ALL_COLUMNS;
            }
        }

        // Write row flags
        self.buffer.push(flags);

        // Write extended flags - always EXTENDED_IS_STATIC for static rows
        self.buffer.push(EXTENDED_IS_STATIC);

        // NO clustering prefix for static rows (key difference from write_row)

        // Build row body
        let row_body = self.build_static_row_body(mutation, schema, flags)?;

        // Write row_size (VInt)
        let mut row_size_buf = Vec::new();
        encode_unsigned(row_body.len() as u64, &mut row_size_buf);
        self.buffer.extend_from_slice(&row_size_buf);

        // Write prev_size (VInt, 0 for now)
        let mut prev_size_buf = Vec::new();
        encode_unsigned(0, &mut prev_size_buf);
        self.buffer.extend_from_slice(&prev_size_buf);

        // Write row body
        self.buffer.extend_from_slice(&row_body);

        Ok(())
    }

    /// Build static row body (everything after row_size VInt)
    ///
    /// Similar to build_row_body but only processes static columns.
    fn build_static_row_body(
        &self,
        mutation: &Mutation,
        schema: &TableSchema,
        flags: u8,
    ) -> Result<Vec<u8>> {
        let mut body = Vec::new();

        // Write timestamp delta (if HAS_TIMESTAMP)
        if (flags & ROW_HAS_TIMESTAMP) != 0 {
            let timestamp_delta = mutation.timestamp_micros - self.stats.min_timestamp;
            encode_signed(timestamp_delta, &mut body);
        }

        // Write TTL delta (if HAS_TTL)
        if (flags & ROW_HAS_TTL) != 0 {
            if let Some(ttl) = mutation.ttl_seconds {
                let ttl_delta = ttl as i64 - self.stats.min_ttl as i64;
                if ttl_delta < 0 {
                    return Err(Error::InvalidInput(format!(
                        "TTL {} is less than min_ttl {}",
                        ttl, self.stats.min_ttl
                    )));
                }
                encode_signed(ttl_delta, &mut body);
            }
        }

        // Write deletion (if HAS_DELETION)
        if (flags & ROW_HAS_DELETION) != 0 {
            // Row tombstone: write local_deletion_time and deletion_timestamp deltas
            let local_deletion_time = (mutation.timestamp_micros / 1_000_000) as i32;
            let ldt_delta =
                (local_deletion_time as i64) - (self.stats.min_local_deletion_time as i64);
            encode_signed(ldt_delta, &mut body);

            let ts_delta = mutation.timestamp_micros - self.stats.min_timestamp;
            encode_signed(ts_delta, &mut body);

            // No cells written for row tombstones - return early
            return Ok(body);
        }

        // Write column bitmap (if NOT HAS_ALL_COLUMNS)
        // For static rows, bitmap only covers static columns
        if (flags & ROW_HAS_ALL_COLUMNS) == 0 {
            self.write_static_column_bitmap(&mut body, mutation, schema)?;
        }

        // Write cell data for static columns only
        self.write_static_cells(&mut body, mutation, schema)?;

        Ok(body)
    }

    /// Write column bitmap for static columns only.
    ///
    /// Same Cassandra `Columns.Serializer.serializeSubset()` format as
    /// `write_column_bitmap()` but scoped to static columns.
    fn write_static_column_bitmap(
        &self,
        buf: &mut Vec<u8>,
        mutation: &Mutation,
        schema: &TableSchema,
    ) -> Result<()> {
        let mut static_columns: Vec<_> = schema.columns.iter().filter(|c| c.is_static).collect();
        static_columns.sort_by(|a: &&Column, b: &&Column| a.name.cmp(&b.name));
        let col_count = static_columns.len();

        // Collect names of columns that have non-NULL writes
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
                _ => None,
            })
            .collect();

        if col_count > 64 {
            return Err(Error::InvalidInput(format!(
                "Static column bitmap for >64 columns not yet supported (have {} static columns)",
                col_count
            )));
        }

        // Build bitmask of MISSING columns (Cassandra convention: bit=1 → missing)
        let mut bitmap: u64 = 0;
        for (idx, col) in static_columns.iter().enumerate() {
            if !present_columns.contains(col.name.as_str()) {
                bitmap |= 1u64 << idx;
            }
        }

        encode_unsigned(bitmap, buf);
        Ok(())
    }

    /// Write cells for static columns only
    fn write_static_cells(
        &self,
        buf: &mut Vec<u8>,
        mutation: &Mutation,
        schema: &TableSchema,
    ) -> Result<()> {
        // Get set of static column names for validation
        let static_column_names: std::collections::HashSet<_> = schema
            .columns
            .iter()
            .filter(|c| c.is_static)
            .map(|c| &c.name)
            .collect();

        for op in &mutation.operations {
            match op {
                crate::storage::write_engine::mutation::CellOperation::Write { column, value } => {
                    // Only write if it's a static column
                    if static_column_names.contains(column) && !matches!(value, Value::Null) {
                        self.write_cell(buf, column, value, mutation.timestamp_micros)?;
                    }
                }
                crate::storage::write_engine::mutation::CellOperation::WriteWithTtl {
                    column,
                    value,
                    ttl_seconds,
                } => {
                    // Only write if it's a static column
                    if static_column_names.contains(column) && !matches!(value, Value::Null) {
                        self.write_cell_with_ttl(
                            buf,
                            column,
                            value,
                            mutation.timestamp_micros,
                            *ttl_seconds,
                        )?;
                    }
                }
                crate::storage::write_engine::mutation::CellOperation::Delete { column } => {
                    // Only process if it's a static column
                    if static_column_names.contains(column) {
                        let local_deletion_time = (mutation.timestamp_micros / 1_000_000) as i32;
                        self.write_tombstone_cell(
                            buf,
                            column,
                            mutation.timestamp_micros,
                            local_deletion_time,
                        )?;
                    }
                }
                crate::storage::write_engine::mutation::CellOperation::DeleteRow => {
                    // Row deletion handled at row level with HAS_DELETION flag
                }
            }
        }

        Ok(())
    }

    /// Build row body (everything after row_size VInt)
    ///
    /// Returns the bytes for: timestamp, TTL, deletion, column bitmap, and cells.
    fn build_row_body(
        &self,
        mutation: &Mutation,
        schema: &TableSchema,
        flags: u8,
    ) -> Result<Vec<u8>> {
        let mut body = Vec::new();

        // Write timestamp delta (if HAS_TIMESTAMP)
        if (flags & ROW_HAS_TIMESTAMP) != 0 {
            let timestamp_delta = mutation.timestamp_micros - self.stats.min_timestamp;
            encode_signed(timestamp_delta, &mut body);
        }

        // Write TTL delta (if HAS_TTL)
        if (flags & ROW_HAS_TTL) != 0 {
            if let Some(ttl) = mutation.ttl_seconds {
                let ttl_delta = ttl as i64 - self.stats.min_ttl as i64;
                if ttl_delta < 0 {
                    return Err(Error::InvalidInput(format!(
                        "TTL {} is less than min_ttl {}",
                        ttl, self.stats.min_ttl
                    )));
                }
                encode_signed(ttl_delta, &mut body);
            }
        }

        // Write deletion (if HAS_DELETION)
        if (flags & ROW_HAS_DELETION) != 0 {
            // Row tombstone: write local_deletion_time and deletion_timestamp deltas
            let local_deletion_time = (mutation.timestamp_micros / 1_000_000) as i32;
            let ldt_delta =
                (local_deletion_time as i64) - (self.stats.min_local_deletion_time as i64);
            encode_signed(ldt_delta, &mut body);

            let ts_delta = mutation.timestamp_micros - self.stats.min_timestamp;
            encode_signed(ts_delta, &mut body);

            // No cells written for row tombstones - return early
            return Ok(body);
        }

        // Write column bitmap (if NOT HAS_ALL_COLUMNS)
        if (flags & ROW_HAS_ALL_COLUMNS) == 0 {
            self.write_column_bitmap(&mut body, mutation, schema)?;
        }

        // Write cell data
        self.write_cells(&mut body, mutation, schema)?;

        Ok(body)
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
    fn write_column_bitmap(
        &self,
        buf: &mut Vec<u8>,
        mutation: &Mutation,
        schema: &TableSchema,
    ) -> Result<()> {
        let regular_cols = self.regular_columns(schema);
        let col_count = regular_cols.len();

        // Collect names of columns that have non-NULL writes
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
                _ => None,
            })
            .collect();

        if col_count > 64 {
            // >64 columns: use large-subset delta encoding (not yet implemented)
            return Err(Error::InvalidInput(format!(
                "Column bitmap for >64 columns not yet supported (have {} regular columns)",
                col_count
            )));
        }

        // Build bitmask of MISSING columns (Cassandra convention: bit=1 → missing)
        let mut bitmap: u64 = 0;
        for (idx, col) in regular_cols.iter().enumerate() {
            if !present_columns.contains(col.name.as_str()) {
                bitmap |= 1u64 << idx;
            }
        }

        encode_unsigned(bitmap, buf);
        Ok(())
    }

    /// Get regular (non-PK, non-CK, non-static) columns from schema.
    ///
    /// Cassandra's column bitmap only covers regular columns — partition key
    /// and clustering key columns are serialized separately in the partition
    /// header and clustering prefix.
    fn regular_columns<'a>(&self, schema: &'a TableSchema) -> Vec<&'a Column> {
        let mut cols: Vec<&'a Column> = schema
            .columns
            .iter()
            .filter(|c| {
                !c.is_static
                    && !schema.is_partition_key(&c.name)
                    && !schema.is_clustering_key(&c.name)
            })
            .collect();
        // Cassandra sorts regular columns alphabetically by name
        cols.sort_by(|a, b| a.name.cmp(&b.name));
        cols
    }

    /// Write cells for this row
    ///
    /// Cells are written in alphabetical column name order to match Cassandra's
    /// `Columns` sorting (regular columns are sorted by name).
    fn write_cells(
        &self,
        buf: &mut Vec<u8>,
        mutation: &Mutation,
        _schema: &TableSchema,
    ) -> Result<()> {
        // Sort operations alphabetically by column name to match Cassandra ordering
        let mut sorted_ops: Vec<_> = mutation.operations.iter().collect();
        sorted_ops.sort_by(|a, b| {
            let name_a = match a {
                crate::storage::write_engine::mutation::CellOperation::Write { column, .. }
                | crate::storage::write_engine::mutation::CellOperation::WriteWithTtl {
                    column,
                    ..
                }
                | crate::storage::write_engine::mutation::CellOperation::Delete { column } => {
                    column.as_str()
                }
                crate::storage::write_engine::mutation::CellOperation::DeleteRow => "",
            };
            let name_b = match b {
                crate::storage::write_engine::mutation::CellOperation::Write { column, .. }
                | crate::storage::write_engine::mutation::CellOperation::WriteWithTtl {
                    column,
                    ..
                }
                | crate::storage::write_engine::mutation::CellOperation::Delete { column } => {
                    column.as_str()
                }
                crate::storage::write_engine::mutation::CellOperation::DeleteRow => "",
            };
            name_a.cmp(name_b)
        });
        for op in &sorted_ops {
            match op {
                crate::storage::write_engine::mutation::CellOperation::Write { column, value } => {
                    // Skip NULL values - they are represented by absence in the bitmap
                    if !matches!(value, Value::Null) {
                        self.write_cell(buf, column, value, mutation.timestamp_micros)?;
                    }
                }
                crate::storage::write_engine::mutation::CellOperation::WriteWithTtl {
                    column,
                    value,
                    ttl_seconds,
                } => {
                    // Skip NULL values - they are represented by absence in the bitmap
                    if !matches!(value, Value::Null) {
                        self.write_cell_with_ttl(
                            buf,
                            column,
                            value,
                            mutation.timestamp_micros,
                            *ttl_seconds,
                        )?;
                    }
                }
                crate::storage::write_engine::mutation::CellOperation::Delete { column } => {
                    // Cell tombstones: local_deletion_time = mutation timestamp as seconds
                    //
                    // This is the Cassandra default when no explicit deletion time is provided.
                    // The local_deletion_time determines when tombstones are eligible for GC
                    // (gc_grace_seconds after local_deletion_time). Using the mutation timestamp
                    // converted to seconds provides a reasonable default that preserves the
                    // temporal relationship between write and delete operations.
                    //
                    // For explicit control over GC timing, a future enhancement could add
                    // CellOperation::DeleteWithTime { column, local_deletion_time }.
                    let local_deletion_time = (mutation.timestamp_micros / 1_000_000) as i32;
                    self.write_tombstone_cell(
                        buf,
                        column,
                        mutation.timestamp_micros,
                        local_deletion_time,
                    )?;
                }
                crate::storage::write_engine::mutation::CellOperation::DeleteRow => {
                    // Row deletion handled at row level with HAS_DELETION flag
                    // Not implemented yet
                }
            }
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
        if (flags & CELL_USE_ROW_TIMESTAMP) == 0 {
            let timestamp_delta = timestamp - self.stats.min_timestamp;
            encode_signed(timestamp_delta, buf);
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

        // Write value length as UNSIGNED VInt (Cassandra uses writeUnsignedVInt)
        encode_unsigned(value_bytes.len() as u64, buf);

        // Write value bytes
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

        // Calculate local_deletion_time = current_time_seconds + ttl_seconds
        let now_seconds = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| Error::Storage(format!("System time error: {}", e)))?
            .as_secs() as i32;
        let local_deletion_time = now_seconds.saturating_add(ttl_seconds as i32);

        // Cell flags - CELL_IS_EXPIRING, NO USE_ROW_TIMESTAMP or USE_ROW_TTL
        let flags = CELL_IS_EXPIRING;
        buf.push(flags);

        // Timestamp delta (required for expiring cells)
        let timestamp_delta = timestamp - self.stats.min_timestamp;
        encode_signed(timestamp_delta, buf);

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

        // Write value length as UNSIGNED VInt (Cassandra uses writeUnsignedVInt)
        encode_unsigned(value_bytes.len() as u64, buf);

        // Write value bytes
        buf.extend_from_slice(&value_bytes);

        Ok(())
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
        let flags = CELL_IS_DELETED;
        buf.push(flags);

        // Timestamp delta (VInt) - required for tombstones
        let timestamp_delta = timestamp - self.stats.min_timestamp;
        encode_signed(timestamp_delta, buf);

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

    /// Write a range tombstone marker
    ///
    /// Range tombstones are written as markers within the partition data:
    /// ```text
    /// [marker_flags: u8]           ← IS_MARKER (0x02) set
    /// [bound_kind: u8]             ← Open/Close/Boundary type
    /// [clustering_prefix: variable] ← Clustering key bound
    /// [deletion_time: VInt]        ← Delta from min_timestamp
    /// [local_deletion_time: VInt]  ← Delta from min_local_deletion_time
    /// ```
    fn write_range_tombstone(
        &mut self,
        range: &RangeTombstone,
        schema: &TableSchema,
    ) -> Result<()> {
        // Write opening bound
        self.write_range_bound(
            &range.start,
            true,
            range.deletion_time,
            range.local_deletion_time,
            schema,
        )?;

        // Write closing bound
        self.write_range_bound(
            &range.end,
            false,
            range.deletion_time,
            range.local_deletion_time,
            schema,
        )?;

        Ok(())
    }

    /// Write a single range tombstone bound
    fn write_range_bound(
        &mut self,
        bound: &ClusteringBound,
        is_open: bool,
        deletion_time: i64,
        local_deletion_time: i32,
        schema: &TableSchema,
    ) -> Result<()> {
        // Marker flag
        self.buffer.push(IS_MARKER);

        // Determine bound kind
        let bound_kind = match (is_open, bound) {
            (true, ClusteringBound::Inclusive(_)) => INCL_START_BOUND,
            (true, ClusteringBound::Exclusive(_)) => EXCL_START_BOUND,
            (false, ClusteringBound::Inclusive(_)) => INCL_END_BOUND,
            (false, ClusteringBound::Exclusive(_)) => EXCL_END_BOUND,
            (_, ClusteringBound::Bottom) => START_BOUNDARY,
            (_, ClusteringBound::Top) => END_BOUNDARY,
        };
        self.buffer.push(bound_kind);

        // Write clustering prefix if present
        match bound {
            ClusteringBound::Inclusive(ck) | ClusteringBound::Exclusive(ck) => {
                self.write_clustering_prefix(ck, schema)?;
            }
            ClusteringBound::Bottom | ClusteringBound::Top => {
                // Empty clustering prefix (header = 0)
                encode_unsigned(0, &mut self.buffer);
            }
        }

        // Deletion time delta
        let ts_delta = deletion_time - self.stats.min_timestamp;
        encode_signed(ts_delta, &mut self.buffer);

        // Local deletion time delta
        let ldt_delta = (local_deletion_time as i64) - (self.stats.min_local_deletion_time as i64);
        encode_signed(ldt_delta, &mut self.buffer);

        Ok(())
    }

    /// Get current file position (for Index.db offset tracking)
    pub fn position(&self) -> u64 {
        self.buffer.len() as u64
    }
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
            result.extend_from_slice(&months.to_be_bytes());
            result.extend_from_slice(&days.to_be_bytes());
            result.extend_from_slice(&nanos.to_be_bytes());
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
                let elem_bytes = serialize_value(elem)?;
                buf.extend_from_slice(&len_as_i32(elem_bytes.len())?.to_be_bytes());
                buf.extend_from_slice(&elem_bytes);
            }
            Ok(buf)
        }
        Value::Map(entries) => {
            let mut buf = Vec::new();
            buf.extend_from_slice(&len_as_i32(entries.len())?.to_be_bytes());
            for (key, val) in entries {
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
/// For nested collections, defaults to Text element type since we don't
/// inspect element values (YAGNI - full recursive inference would be needed
/// for proper nested collection support in M5.3).
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
        // Nested collections default to Text element type (M5.3 for full support)
        Some(Value::List(_)) => CqlType::List(Box::new(CqlType::Text)),
        Some(Value::Set(_)) => CqlType::Set(Box::new(CqlType::Text)),
        Some(Value::Map(_)) => CqlType::Map(Box::new(CqlType::Text), Box::new(CqlType::Text)),
        Some(Value::Tuple(fields)) => CqlType::Tuple(vec![CqlType::Text; fields.len()]),
        Some(Value::Udt(udt)) => CqlType::Udt(udt.type_name.clone(), vec![]),
        Some(Value::Frozen(inner)) => {
            CqlType::Frozen(Box::new(infer_cql_type_from_value(Some(inner))))
        }
        Some(Value::Tombstone(_)) => CqlType::Text, // Tombstones shouldn't appear in UDT fields
        Some(Value::Json(_)) => CqlType::Text,      // JSON is stored as text
    }
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
            Ok(stored.to_be_bytes().to_vec())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
    use crate::storage::write_engine::mutation::{
        CellOperation, ClusteringKey, PartitionKey, TableId,
    };
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

        // Verify timestamp delta is encoded
        // Delta = 5000, should be at start of row body
        // VInt encoding of 5000 as signed: zigzag(5000) = 10000, encoded as VInt
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
        assert_eq!(
            flags & ROW_HAS_TIMESTAMP,
            ROW_HAS_TIMESTAMP,
            "Should have HAS_TIMESTAMP flag"
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

        writer.write_range_tombstone(&range, &schema).unwrap();

        let bytes = writer.finish().unwrap();
        assert!(!bytes.is_empty());

        // Verify opening bound
        assert_eq!(bytes[0], IS_MARKER, "Should have IS_MARKER flag");
        assert_eq!(
            bytes[1], INCL_START_BOUND,
            "Should have INCL_START_BOUND kind"
        );

        // Second marker should also be present (closing bound)
        // Note: Position will vary based on VInt encoding of clustering prefix
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

        writer.write_range_tombstone(&range, &schema).unwrap();

        let bytes = writer.finish().unwrap();
        assert!(!bytes.is_empty());

        // Verify opening bound
        assert_eq!(bytes[0], IS_MARKER, "Should have IS_MARKER flag");
        assert_eq!(
            bytes[1], EXCL_START_BOUND,
            "Should have EXCL_START_BOUND kind"
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

        writer.write_range_tombstone(&range, &schema).unwrap();

        let bytes = writer.finish().unwrap();
        assert!(!bytes.is_empty());

        // Verify opening bound (Bottom)
        assert_eq!(bytes[0], IS_MARKER, "Should have IS_MARKER flag");
        assert_eq!(bytes[1], START_BOUNDARY, "Should have START_BOUNDARY kind");
        // Empty clustering prefix for Bottom: header = 0 (single byte VInt)
        assert_eq!(bytes[2], 0x00, "Bottom should have empty clustering prefix");
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

    /// Verify that 64+ regular columns returns an error (not a panic from shift overflow).
    #[test]
    fn test_column_bitmap_64_plus_regular_columns_returns_error() {
        let stats = create_test_stats();
        let writer = DataWriter::new(stats);

        // Create schema with 65 regular columns
        let columns: Vec<Column> = (0..65)
            .map(|i| Column {
                name: format!("col_{}", i),
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

        // Only write one column — the other 64 are missing
        let mutation = Mutation::new(
            table_id,
            pk,
            None,
            vec![CellOperation::Write {
                column: "col_0".to_string(),
                value: Value::Text("test".to_string()),
            }],
            1001000,
            None,
        );

        let mut buf = Vec::new();
        let result = writer.write_column_bitmap(&mut buf, &mutation, &schema);
        assert!(
            result.is_err(),
            "Should return error for >64 regular columns"
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains(">64 columns not yet supported"),
            "Error message should mention >64 columns"
        );
    }

    /// Verify that 64+ static columns returns an error (not a panic from shift overflow).
    #[test]
    fn test_column_bitmap_64_plus_static_columns_returns_error() {
        let stats = create_test_stats();
        let writer = DataWriter::new(stats);

        // Create schema with 65 static columns
        let columns: Vec<Column> = (0..65)
            .map(|i| Column {
                name: format!("scol_{}", i),
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

        // Only write one static column — the other 64 are missing
        let mutation = Mutation::new(
            table_id,
            pk,
            None,
            vec![CellOperation::Write {
                column: "scol_0".to_string(),
                value: Value::Text("test".to_string()),
            }],
            1001000,
            None,
        );

        let mut buf = Vec::new();
        let result = writer.write_static_column_bitmap(&mut buf, &mutation, &schema);
        assert!(
            result.is_err(),
            "Should return error for >64 static columns"
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains(">64 columns not yet supported"),
            "Error message should mention >64 columns"
        );
    }

    /// Verify that exactly 64 regular columns works (u64 can represent bits 0..63).
    #[test]
    fn test_column_bitmap_exactly_64_regular_columns_succeeds() {
        let stats = create_test_stats();
        let writer = DataWriter::new(stats);

        // Create schema with exactly 64 regular columns
        let columns: Vec<Column> = (0..64)
            .map(|i| Column {
                name: format!("col_{}", i),
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

        // Write only col_0 and col_63 — the middle 62 columns are missing
        let mutation = Mutation::new(
            table_id,
            pk,
            None,
            vec![
                CellOperation::Write {
                    column: "col_0".to_string(),
                    value: Value::Text("first".to_string()),
                },
                CellOperation::Write {
                    column: "col_63".to_string(),
                    value: Value::Text("last".to_string()),
                },
            ],
            1001000,
            None,
        );

        let mut buf = Vec::new();
        let result = writer.write_column_bitmap(&mut buf, &mutation, &schema);
        assert!(
            result.is_ok(),
            "Should succeed for exactly 64 regular columns: {:?}",
            result.err()
        );

        // Verify bitmap was written (should be non-empty)
        assert!(!buf.is_empty(), "Bitmap should be written");
    }

    /// Verify that exactly 64 static columns works (u64 can represent bits 0..63).
    #[test]
    fn test_column_bitmap_exactly_64_static_columns_succeeds() {
        let stats = create_test_stats();
        let writer = DataWriter::new(stats);

        // Create schema with exactly 64 static columns
        let columns: Vec<Column> = (0..64)
            .map(|i| Column {
                name: format!("scol_{}", i),
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

        // Write only scol_0 and scol_63 — the middle 62 columns are missing
        let mutation = Mutation::new(
            table_id,
            pk,
            None,
            vec![
                CellOperation::Write {
                    column: "scol_0".to_string(),
                    value: Value::Text("first".to_string()),
                },
                CellOperation::Write {
                    column: "scol_63".to_string(),
                    value: Value::Text("last".to_string()),
                },
            ],
            1001000,
            None,
        );

        let mut buf = Vec::new();
        let result = writer.write_static_column_bitmap(&mut buf, &mutation, &schema);
        assert!(
            result.is_ok(),
            "Should succeed for exactly 64 static columns: {:?}",
            result.err()
        );

        // Verify bitmap was written (should be non-empty)
        assert!(!buf.is_empty(), "Bitmap should be written");
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
}
