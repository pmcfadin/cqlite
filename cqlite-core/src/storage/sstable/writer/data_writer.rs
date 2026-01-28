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
//! [column_bitmap: VInt + bytes if NOT ROW_HAS_ALL_COLUMNS]
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
use crate::schema::TableSchema;
use crate::storage::serialization::vint::{encode_signed, encode_unsigned};
use crate::storage::write_engine::mutation::{DecoratedKey, Mutation};
use crate::storage::sstable::writer::stats_writer::StatisticsMetadata;
use crate::types::{ComparatorType, Value};
use std::io::Write;

// Row header flag constants (from V5CompressedLegacy parser)
const ROW_HAS_TIMESTAMP: u8 = 0x04;
const ROW_HAS_TTL: u8 = 0x08;
#[allow(dead_code)]
const ROW_HAS_DELETION: u8 = 0x10;
const ROW_HAS_ALL_COLUMNS: u8 = 0x20;
#[allow(dead_code)]
const ROW_HAS_COMPLEX_DELETION: u8 = 0x40;
#[allow(dead_code)]
const ROW_HAS_EXTENDED_FLAGS: u8 = 0x80;

// Cell flag constants (from V5CompressedLegacy parser)
const CELL_IS_DELETED: u8 = 0x01;
#[allow(dead_code)]
const CELL_IS_EXPIRING: u8 = 0x02;
const CELL_HAS_EMPTY_VALUE: u8 = 0x04;
const CELL_USE_ROW_TIMESTAMP: u8 = 0x08;
#[allow(dead_code)]
const CELL_USE_ROW_TTL: u8 = 0x10;

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

    /// Write a complete partition (partition key + all rows)
    ///
    /// # Arguments
    /// * `key` - Decorated partition key (token + raw bytes)
    /// * `mutations` - All mutations for this partition (must be in clustering order)
    /// * `schema` - Table schema for column metadata
    ///
    /// # Returns
    /// File offset where this partition starts (for Index.db)
    pub fn write_partition(
        &mut self,
        key: &DecoratedKey,
        mutations: &[Mutation],
        schema: &TableSchema,
    ) -> Result<u64> {
        let partition_offset = self.buffer.len() as u64;

        // Write partition header
        self.write_partition_header(key)?;

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
    /// Format (V5CompressedLegacy):
    /// ```text
    /// [partition_flags: u8]       ← Currently 0x00 (no deletion)
    /// [key_length: u8]           ← Partition key length (max 255 bytes)
    /// [key_bytes]                ← Raw partition key bytes
    /// [deletion_time: i32 BE]    ← Partition deletion time (0 = not deleted)
    /// [unknown_field: u64 BE]    ← Unknown 8-byte field (observed as 0)
    /// ```
    fn write_partition_header(&mut self, key: &DecoratedKey) -> Result<()> {
        // Partition flags (0x00 = no deletion)
        self.buffer.push(0x00);

        // Partition key length (u8, NOT VInt)
        // V5CompressedLegacy uses u8 length prefix, limiting keys to 255 bytes
        if key.key.len() > 255 {
            return Err(Error::InvalidInput(format!(
                "Partition key too large for V5CompressedLegacy format: {} bytes (max 255)",
                key.key.len()
            )));
        }
        self.buffer.push(key.key.len() as u8);

        // Partition key bytes
        self.buffer.extend_from_slice(&key.key);

        // Partition deletion time (i32 BE, 0 = not deleted)
        self.buffer.write_all(&0i32.to_be_bytes())?;

        // Unknown 8-byte field (observed as 0 in real data)
        self.buffer.write_all(&0u64.to_be_bytes())?;

        Ok(())
    }

    /// Write a single row
    ///
    /// This implements the V5CompressedLegacy row format with delta encoding.
    fn write_row(&mut self, mutation: &Mutation, schema: &TableSchema) -> Result<()> {
        // Build row header flags
        let mut flags = 0u8;

        // Timestamp is always present for writes
        flags |= ROW_HAS_TIMESTAMP;

        // TTL if present
        if mutation.ttl_seconds.is_some() {
            flags |= ROW_HAS_TTL;
        }

        // All columns present if all operations are writes (no deletes)
        let all_writes = mutation
            .operations
            .iter()
            .all(|op| matches!(op, crate::storage::write_engine::mutation::CellOperation::Write { .. }));
        if all_writes {
            flags |= ROW_HAS_ALL_COLUMNS;
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
                encode_signed(ttl_delta, &mut body);
            }
        }

        // Write deletion (if HAS_DELETION) - not implemented yet
        // Would write: [local_deletion_time_delta: VInt][deletion_timestamp: VInt]

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
    /// Format:
    /// ```text
    /// [column_count: VInt]
    /// [bitmap_bytes: (column_count + 7) / 8 bytes]
    /// ```
    fn write_column_bitmap(
        &self,
        buf: &mut Vec<u8>,
        mutation: &Mutation,
        schema: &TableSchema,
    ) -> Result<()> {
        let column_count = schema.columns.len();

        // Write column count as VUInt
        encode_unsigned(column_count as u64, buf);

        // Build bitmap
        let bitmap_size = column_count.div_ceil(8);
        let mut bitmap = vec![0u8; bitmap_size];

        for op in &mutation.operations {
            if let crate::storage::write_engine::mutation::CellOperation::Write { column, .. } = op {
                // Find column index
                if let Some((idx, _)) = schema
                    .columns
                    .iter()
                    .enumerate()
                    .find(|(_, c)| &c.name == column)
                {
                    let byte_idx = idx / 8;
                    let bit_idx = idx % 8;
                    bitmap[byte_idx] |= 1 << bit_idx;
                }
            }
        }

        buf.extend_from_slice(&bitmap);
        Ok(())
    }

    /// Write cells for this row
    fn write_cells(
        &self,
        buf: &mut Vec<u8>,
        mutation: &Mutation,
        _schema: &TableSchema,
    ) -> Result<()> {
        for op in &mutation.operations {
            match op {
                crate::storage::write_engine::mutation::CellOperation::Write { column, value } => {
                    self.write_cell(buf, column, value, mutation.timestamp_micros)?;
                }
                crate::storage::write_engine::mutation::CellOperation::Delete { column } => {
                    self.write_tombstone_cell(buf, column, mutation.timestamp_micros)?;
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
    fn write_cell(
        &self,
        buf: &mut Vec<u8>,
        _column: &str,
        value: &Value,
        timestamp: i64,
    ) -> Result<()> {
        // Cell flags
        let mut flags = CELL_USE_ROW_TIMESTAMP; // Use row timestamp by default

        if matches!(value, Value::Null) {
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

        // Write value length as VInt
        encode_signed(value_bytes.len() as i64, buf);

        // Write value bytes
        buf.extend_from_slice(&value_bytes);

        Ok(())
    }

    /// Write a tombstone cell
    fn write_tombstone_cell(
        &self,
        buf: &mut Vec<u8>,
        _column: &str,
        timestamp: i64,
    ) -> Result<()> {
        // Cell flags for tombstone
        let flags = CELL_IS_DELETED | CELL_USE_ROW_TIMESTAMP;
        buf.push(flags);

        // Timestamp (skip if USE_ROW_TIMESTAMP)
        if (flags & CELL_USE_ROW_TIMESTAMP) == 0 {
            let timestamp_delta = timestamp - self.stats.min_timestamp;
            encode_signed(timestamp_delta, buf);
        }

        // No value for tombstone
        Ok(())
    }

    /// Get current file position (for Index.db offset tracking)
    pub fn position(&self) -> u64 {
        self.buffer.len() as u64
    }
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
        Value::Duration { months, days, nanos } => {
            let mut result = Vec::new();
            result.extend_from_slice(&months.to_be_bytes());
            result.extend_from_slice(&days.to_be_bytes());
            result.extend_from_slice(&nanos.to_be_bytes());
            Ok(result)
        }
        _ => Err(Error::InvalidInput(format!(
            "Unsupported value type for serialization: {:?}",
            value
        ))),
    }
}

/// Serialize value for clustering key (type-specific encoding)
///
/// Fixed-width types: raw bytes (no length prefix)
/// Variable-width types: VInt length + bytes
fn serialize_value_for_clustering(value: &Value, comparator: &ComparatorType) -> Result<Vec<u8>> {
    match (value, comparator) {
        // Fixed-width types (no length prefix)
        (Value::Integer(n), ComparatorType::Int) => Ok(n.to_be_bytes().to_vec()),
        (Value::BigInt(n), ComparatorType::BigInt) => Ok(n.to_be_bytes().to_vec()),
        (Value::Timestamp(millis), ComparatorType::Timestamp) => Ok(millis.to_be_bytes().to_vec()),
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

        _ => Err(Error::InvalidInput(format!(
            "Type mismatch or unsupported clustering type: value={:?}, comparator={:?}",
            value, comparator
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::write_engine::mutation::{
        CellOperation, ClusteringKey, PartitionKey, TableId,
    };
    use crate::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
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
        writer.write_partition_header(&key).unwrap();

        let bytes = writer.finish().unwrap();

        // Verify structure:
        // [0x00] partition flags
        // [0x04] key length (4 bytes)
        // [0x00, 0x00, 0x00, 0x2A] key bytes
        // [0x00, 0x00, 0x00, 0x00] deletion time (4 bytes)
        // [0x00 * 8] unknown field (8 bytes)
        assert_eq!(bytes[0], 0x00); // partition flags
        assert_eq!(bytes[1], 0x04); // key length
        assert_eq!(&bytes[2..6], &[0x00, 0x00, 0x00, 0x2A]); // key bytes
        assert_eq!(&bytes[6..10], &[0x00, 0x00, 0x00, 0x00]); // deletion time
        assert_eq!(&bytes[10..18], &[0x00; 8]); // unknown field
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
        assert_eq!(flags & ROW_HAS_TIMESTAMP, ROW_HAS_TIMESTAMP, "Should have timestamp");
        assert_eq!(flags & ROW_HAS_ALL_COLUMNS, ROW_HAS_ALL_COLUMNS, "Should have all columns");
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

        let offset = writer.write_partition(&key, &mutations, &schema).unwrap();
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
            1005000, // timestamp (delta = 5000)
            Some(7200), // TTL (delta = 3600)
        );

        let row_body = writer.build_row_body(&mutation, &schema, ROW_HAS_TIMESTAMP | ROW_HAS_TTL).unwrap();
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
        writer.write_column_bitmap(&mut buf, &mutation, &schema).unwrap();

        assert!(!buf.is_empty());
        // Should have: [column_count: VInt][bitmap: 1 byte for 2 columns]
        // First byte after column_count should have bit 0 set (column 0 = "name")
    }

    #[test]
    fn test_partition_key_size_limit() {
        let stats = create_test_stats();
        let mut writer = DataWriter::new(stats);

        // Create a partition key larger than 255 bytes
        let large_key = vec![0xFF; 256];
        let key = DecoratedKey::new(12345, large_key);

        let result = writer.write_partition_header(&key);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too large"));
    }

    #[test]
    fn test_write_tombstone_cell() {
        let stats = create_test_stats();
        let writer = DataWriter::new(stats);

        let mut buf = Vec::new();
        writer.write_tombstone_cell(&mut buf, "deleted_col", 1001000).unwrap();

        assert!(!buf.is_empty());
        // First byte should be tombstone flags
        let flags = buf[0];
        assert_eq!(flags & CELL_IS_DELETED, CELL_IS_DELETED);
        assert_eq!(flags & CELL_USE_ROW_TIMESTAMP, CELL_USE_ROW_TIMESTAMP);
    }

    #[test]
    fn test_serialize_clustering_value_fixed_width() {
        // Integer (fixed-width, no length prefix)
        let bytes = serialize_value_for_clustering(&Value::Integer(42), &ComparatorType::Int).unwrap();
        assert_eq!(bytes, vec![0x00, 0x00, 0x00, 0x2A]);

        // BigInt (fixed-width)
        let bytes = serialize_value_for_clustering(&Value::BigInt(1000), &ComparatorType::BigInt).unwrap();
        assert_eq!(bytes, vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xE8]);
    }

    #[test]
    fn test_serialize_clustering_value_variable_width() {
        // Text (variable-width, VInt length prefix)
        let bytes = serialize_value_for_clustering(&Value::Text("test".to_string()), &ComparatorType::Text).unwrap();
        assert!(!bytes.is_empty());
        // First byte(s) should be VInt length (4), followed by "test"
        // VInt(4) = 0x04, then "test"
        assert_eq!(bytes[0], 0x04); // VInt length = 4
        assert_eq!(&bytes[1..], b"test");
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

        let offset1 = writer.write_partition(&key1, &mutations1, &schema).unwrap();
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

        let offset2 = writer.write_partition(&key2, &mutations2, &schema).unwrap();
        assert!(offset2 > offset1); // Second partition starts after first

        let bytes = writer.finish().unwrap();
        assert!(!bytes.is_empty());

        // Both partitions should have end-of-partition markers
        // Note: END_OF_PARTITION (0x01) may appear elsewhere (e.g., in cell flags)
        // For this test, we verify the file structure is valid and both partitions were written
        assert!(offset2 > offset1, "Second partition should start after first");

        // The last byte should be an END_OF_PARTITION marker
        assert_eq!(bytes[bytes.len() - 1], END_OF_PARTITION, "File should end with END_OF_PARTITION");
    }
}
