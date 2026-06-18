//! Parquet export writer for QueryResult (feature = "parquet")
//!
//! Converts CQL query results to Apache Parquet format with proper type mapping.
//! Uses Snappy compression by default (Cassandra default, good speed/size balance).
//!
//! This module is compiled only when the `parquet` cargo feature is enabled
//! (off by default, Epic #682).  CQLite produces Parquet *files*; committing
//! those files to Iceberg/Delta table formats is an external committer's job
//! and is deliberately out of scope (see `export` module docs).
//!
//! The CQL → Arrow type mapping and array-building logic lives in the sibling
//! `arrow_convert` module (feature = "arrow"), which this module re-exports.
//! That separation allows a third-party Arrow IPC writer to reuse the
//! conversion without depending on the `parquet` crate.
//!
//! # CQL → Arrow type mapping
//!
//! See [`super::arrow_convert`] for the full mapping table, decimal strategy,
//! and recursive builder design.

use crate::export::arrow_convert::{build_arrow_schema, convert_to_arrays, ArrowConvertError};
use crate::query::{ColumnInfo, QueryMetadata, QueryResult, QueryRow};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use thiserror::Error;

// ============================================================================
// Export options and error type (Issues #683, #684)
// ============================================================================

/// Compression codec for Parquet output.
///
/// Snappy is the default (Cassandra default, good speed/size balance).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ParquetCompression {
    /// Snappy compression (default).
    #[default]
    Snappy,
    /// Zstandard compression (better ratio, slower).
    Zstd,
    /// No compression.
    Uncompressed,
}

impl ParquetCompression {
    /// Map to the `parquet` crate's compression enum.
    fn to_parquet(self) -> Compression {
        match self {
            ParquetCompression::Snappy => Compression::SNAPPY,
            ParquetCompression::Zstd => Compression::ZSTD(ZstdLevel::default()),
            ParquetCompression::Uncompressed => Compression::UNCOMPRESSED,
        }
    }
}

/// Writer-owned options for Parquet export.
///
/// Replaces the CLI-only `OutputConfig` parameter (Issue #683) so the writer
/// has no dependency on CLI types.  The CLI constructs this from its own
/// configuration; library consumers construct it directly.
#[derive(Debug, Clone)]
pub struct ParquetExportOptions {
    /// Maximum number of rows to write (batch writer only). `None` = all rows.
    pub row_limit: Option<usize>,
    /// Rows per Parquet row group (streaming writer). Default: 10,000.
    pub row_group_size: usize,
    /// Compression codec. Default: Snappy.
    pub compression: ParquetCompression,
}

impl Default for ParquetExportOptions {
    fn default() -> Self {
        Self {
            row_limit: None,
            row_group_size: 10_000,
            compression: ParquetCompression::default(),
        }
    }
}

/// Errors produced by the Parquet export writers.
///
/// A dedicated `thiserror` enum (Issue #683) so the writer does not depend on
/// the CLI's `OutputError`.  The CLI maps these to its own error type at the
/// boundary.
#[derive(Debug, Error)]
pub enum ParquetExportError {
    /// Underlying I/O failure.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// Arrow array or schema construction failure.
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    /// Parquet encoding failure.
    #[error("Parquet error: {0}")]
    Parquet(#[from] ::parquet::errors::ParquetError),
    /// A value could not be represented in the target Arrow/Parquet type.
    #[error("{0}")]
    InvalidValue(String),
    /// Invalid writer configuration (e.g. zero row group size).
    #[error("invalid Parquet export options: {0}")]
    InvalidOptions(String),
}

impl From<ArrowConvertError> for ParquetExportError {
    fn from(e: ArrowConvertError) -> Self {
        match e {
            ArrowConvertError::Arrow(a) => ParquetExportError::Arrow(a),
            ArrowConvertError::InvalidValue(s) => ParquetExportError::InvalidValue(s),
        }
    }
}

/// Parquet writer for QueryResult
///
/// Converts query results to Apache Parquet binary format.
/// Unlike JSON/CSV writers, this returns `Vec<u8>` (binary data).
pub struct ParquetWriter;

impl ParquetWriter {
    /// Write QueryResult to Parquet binary format
    ///
    /// # Arguments
    ///
    /// * `result` - The query result to convert to Parquet
    /// * `options` - Export options (row limit, compression)
    ///
    /// # Returns
    ///
    /// Binary Parquet data or error
    pub fn write(
        result: &QueryResult,
        options: &ParquetExportOptions,
    ) -> Result<Vec<u8>, ParquetExportError> {
        // Handle empty results
        if result.metadata.columns.is_empty() {
            return Self::write_empty_parquet(options);
        }

        // Build Arrow schema from column metadata (delegates to arrow_convert)
        let schema = build_arrow_schema(&result.metadata.columns)?;

        // Apply row limit if specified in the options
        let rows_to_process = if let Some(limit) = options.row_limit {
            &result.rows[..result.rows.len().min(limit)]
        } else {
            &result.rows
        };

        // Convert rows to Arrow arrays (one per column) — delegates to arrow_convert
        let arrays = convert_to_arrays(&result.metadata.columns, rows_to_process)?;

        // Create RecordBatch
        let batch = RecordBatch::try_new(Arc::new(schema), arrays)?;

        // Write to Parquet with the configured compression
        Self::write_parquet(&batch, options.compression)
    }

    /// Write an empty Parquet file
    fn write_empty_parquet(options: &ParquetExportOptions) -> Result<Vec<u8>, ParquetExportError> {
        let schema = Schema::empty();
        let batch = RecordBatch::new_empty(Arc::new(schema));
        Self::write_parquet(&batch, options.compression)
    }

    /// Build Arrow schema from CQL column metadata.
    ///
    /// Delegates to [`arrow_convert::build_arrow_schema`].  This associated
    /// function is kept for callers inside this module (e.g. `StreamingParquetWriter`)
    /// that hold a `&[ColumnInfo]` directly.
    pub(crate) fn build_schema(columns: &[ColumnInfo]) -> Result<Schema, ParquetExportError> {
        build_arrow_schema(columns).map_err(Into::into)
    }

    /// Convert all rows to Arrow arrays (column-oriented).
    ///
    /// Delegates to [`arrow_convert::convert_to_arrays`].
    pub(crate) fn convert_to_arrays(
        columns: &[ColumnInfo],
        rows: &[QueryRow],
    ) -> Result<Vec<arrow::array::ArrayRef>, ParquetExportError> {
        convert_to_arrays(columns, rows).map_err(Into::into)
    }

    /// Write RecordBatch to Parquet bytes
    fn write_parquet(
        batch: &RecordBatch,
        compression: ParquetCompression,
    ) -> Result<Vec<u8>, ParquetExportError> {
        let mut buffer = Vec::new();

        let props = WriterProperties::builder()
            .set_compression(compression.to_parquet())
            .build();

        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props))?;
        writer.write(batch)?;
        writer.close()?;

        Ok(buffer)
    }
}

// ============================================================================
// Streaming Parquet Writer (Issue #280)
// ============================================================================

/// Streaming Parquet writer for memory-efficient export of large datasets
///
/// Unlike the batch `ParquetWriter`, this writer processes data incrementally
/// using Parquet row groups. Each chunk is converted to a row group, allowing
/// export of arbitrarily large result sets within memory constraints.
///
/// # Row Group Strategy
///
/// The writer buffers rows until `row_group_size` is reached (default: 10,000),
/// then writes a complete row group to the output. This balances memory usage
/// against I/O efficiency.
///
/// # Schema parity
///
/// The constructor uses the same [`arrow_convert::build_arrow_schema`] call as
/// the batch writer, so the schema produced by the streaming path is always
/// identical to the schema produced by the batch `ParquetWriter`.
///
/// # Example
///
/// ```ignore
/// let file = File::create("output.parquet")?;
/// let mut writer = StreamingParquetWriter::new(
///     file,
///     &metadata,
///     &ParquetExportOptions::default(),
/// )?;
///
/// for chunk in result_iterator.chunks(10_000) {
///     writer.write_chunk(&chunk)?;
/// }
///
/// writer.finalize()?;
/// ```
pub struct StreamingParquetWriter<W: Write + Send> {
    /// Inner Arrow/Parquet writer (`None` after `finalize`)
    writer: Option<ArrowWriter<W>>,
    /// Arrow schema
    schema: Arc<Schema>,
    /// Column metadata
    columns: Vec<ColumnInfo>,
    /// Buffered rows for current row group
    row_buffer: Vec<QueryRow>,
    /// Row group size (rows per group)
    row_group_size: usize,
    /// Total rows written
    rows_written: u64,
}

impl<W: Write + Send> StreamingParquetWriter<W> {
    /// Create a streaming Parquet writer over any `W: Write + Send`.
    ///
    /// The Arrow schema is built from `metadata.columns` using the same
    /// mapping as the batch [`ParquetWriter`], and the Parquet file header
    /// is initialized immediately.
    ///
    /// # Errors
    ///
    /// Returns [`ParquetExportError::InvalidOptions`] if
    /// `options.row_group_size` is zero, or an Arrow/Parquet error if the
    /// schema cannot be built or the writer cannot be initialized.
    pub fn new(
        output: W,
        metadata: &QueryMetadata,
        options: &ParquetExportOptions,
    ) -> Result<Self, ParquetExportError> {
        if options.row_group_size == 0 {
            return Err(ParquetExportError::InvalidOptions(
                "row_group_size must be greater than 0".to_string(),
            ));
        }

        // Build Arrow schema — same helper used by the batch ParquetWriter.
        let schema = Arc::new(ParquetWriter::build_schema(&metadata.columns)?);

        // set_max_row_group_size makes `row_group_size` authoritative: without
        // it, ArrowWriter coalesces written batches into ~1M-row groups, which
        // both ignored the documented "row group per chunk" behavior and let
        // memory grow unbounded on large streaming exports.
        let props = WriterProperties::builder()
            .set_compression(options.compression.to_parquet())
            .set_max_row_group_size(options.row_group_size)
            .build();

        let arrow_writer = ArrowWriter::try_new(output, Arc::clone(&schema), Some(props))?;

        Ok(Self {
            writer: Some(arrow_writer),
            schema,
            columns: metadata.columns.clone(),
            row_buffer: Vec::with_capacity(options.row_group_size),
            row_group_size: options.row_group_size,
            rows_written: 0,
        })
    }

    /// Buffer rows and flush complete row groups.
    ///
    /// Returns the number of rows flushed to the output in this call (rows
    /// remaining in the buffer are flushed by [`finalize`](Self::finalize)).
    pub fn write_chunk(&mut self, rows: &[QueryRow]) -> Result<usize, ParquetExportError> {
        self.row_buffer.extend(rows.iter().cloned());
        self.rows_written += rows.len() as u64;

        let mut flushed = 0;
        while self.row_buffer.len() >= self.row_group_size {
            let chunk: Vec<QueryRow> = self.row_buffer.drain(..self.row_group_size).collect();
            self.write_row_group(&chunk)?;
            flushed += self.row_group_size;
        }

        Ok(flushed)
    }

    /// Flush any buffered rows and close the Parquet file.
    ///
    /// Must be called exactly once after all chunks are written; dropping the
    /// writer without calling `finalize` produces a truncated file.
    pub fn finalize(&mut self) -> Result<(), ParquetExportError> {
        if !self.row_buffer.is_empty() {
            let remaining = std::mem::take(&mut self.row_buffer);
            self.write_row_group(&remaining)?;
        }

        if let Some(writer) = self.writer.take() {
            writer.close()?;
        }

        Ok(())
    }

    /// Total number of rows accepted by `write_chunk` so far.
    pub fn rows_written(&self) -> u64 {
        self.rows_written
    }

    /// Convert a slice of rows to a RecordBatch and write it as a row group.
    fn write_row_group(&mut self, rows: &[QueryRow]) -> Result<(), ParquetExportError> {
        let writer = self.writer.as_mut().ok_or_else(|| {
            ParquetExportError::InvalidOptions(
                "writer already finalized - cannot write more rows".to_string(),
            )
        })?;

        let arrays = ParquetWriter::convert_to_arrays(&self.columns, rows)?;
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), arrays)?;
        writer.write(&batch)?;

        Ok(())
    }
}

/// Create a StreamingParquetWriter that writes to a file.
///
/// Convenience wrapper around [`StreamingParquetWriter::new`].
pub fn create_streaming_parquet_writer(
    file: File,
    metadata: &QueryMetadata,
    options: &ParquetExportOptions,
) -> Result<StreamingParquetWriter<File>, ParquetExportError> {
    StreamingParquetWriter::new(file, metadata, options)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{ColumnInfo, QueryRow};
    use crate::{RowKey, Value};
    use arrow::array::{Array, FixedSizeBinaryArray, StringArray};
    use arrow::array::BinaryArray;
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::collections::HashMap;
    use std::error::Error as StdError;

    fn default_options() -> ParquetExportOptions {
        ParquetExportOptions::default()
    }

    /// Helper to verify Parquet output by reading it back
    fn read_parquet_back(bytes: &[u8]) -> Result<RecordBatch, Box<dyn StdError>> {
        // Use Bytes which implements ChunkReader
        let bytes = Bytes::copy_from_slice(bytes);
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
        let mut reader = builder.build()?;
        reader
            .next()
            .ok_or_else(|| "No batches in Parquet file".to_string())?
            .map_err(|e| Box::new(e) as Box<dyn StdError>)
    }

    #[test]
    fn test_empty_result() {
        let result = QueryResult::new();
        let bytes = ParquetWriter::write(&result, &default_options()).unwrap();

        // Should produce valid (empty) Parquet
        assert!(!bytes.is_empty());
        // Parquet magic bytes: PAR1
        assert_eq!(&bytes[0..4], b"PAR1");
    }

    #[test]
    fn test_boolean_values() {
        use crate::types::DataType;
        let mut result = QueryResult::new();
        result.metadata.columns = vec![ColumnInfo::new(
            "bool_col".to_string(),
            DataType::Boolean,
            true,
            0,
        )];

        let mut values = HashMap::new();
        values.insert("bool_col".to_string(), Value::Boolean(true));
        let row = QueryRow::with_values(RowKey::new(vec![1]), values);
        result.rows.push(row);

        let bytes = ParquetWriter::write(&result, &default_options()).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);
    }

    #[test]
    fn test_integer_types() {
        use crate::types::DataType;
        let mut result = QueryResult::new();
        result.metadata.columns = vec![
            ColumnInfo::new("tiny".to_string(), DataType::TinyInt, false, 0),
            ColumnInfo::new("small".to_string(), DataType::SmallInt, false, 1),
            ColumnInfo::new("int".to_string(), DataType::Integer, false, 2),
            ColumnInfo::new("big".to_string(), DataType::BigInt, false, 3),
        ];

        let mut values = HashMap::new();
        values.insert("tiny".to_string(), Value::TinyInt(127));
        values.insert("small".to_string(), Value::SmallInt(32767));
        values.insert("int".to_string(), Value::Integer(2147483647));
        values.insert("big".to_string(), Value::BigInt(9223372036854775807));
        let row = QueryRow::with_values(RowKey::new(vec![1]), values);
        result.rows.push(row);

        let bytes = ParquetWriter::write(&result, &default_options()).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 4);
    }

    #[test]
    fn test_float_types() {
        use crate::types::DataType;
        let mut result = QueryResult::new();
        result.metadata.columns = vec![
            ColumnInfo::new("f32".to_string(), DataType::Float32, false, 0),
            ColumnInfo::new("f64".to_string(), DataType::Float, false, 1),
        ];

        let mut values = HashMap::new();
        values.insert("f32".to_string(), Value::Float32(3.5));
        values.insert("f64".to_string(), Value::Float(2.75));
        let row = QueryRow::with_values(RowKey::new(vec![1]), values);
        result.rows.push(row);

        let bytes = ParquetWriter::write(&result, &default_options()).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_text_values() {
        use crate::types::DataType;
        let mut result = QueryResult::new();
        result.metadata.columns = vec![ColumnInfo::new(
            "text_col".to_string(),
            DataType::Text,
            false,
            0,
        )];

        let mut values = HashMap::new();
        values.insert(
            "text_col".to_string(),
            Value::Text("Hello, Parquet!".to_string()),
        );
        let row = QueryRow::with_values(RowKey::new(vec![1]), values);
        result.rows.push(row);

        let bytes = ParquetWriter::write(&result, &default_options()).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(batch.num_rows(), 1);
        let col = batch.column(0);
        let string_array = col.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(string_array.value(0), "Hello, Parquet!");
    }

    #[test]
    fn test_blob_values() {
        use crate::types::DataType;
        let mut result = QueryResult::new();
        result.metadata.columns = vec![ColumnInfo::new(
            "blob_col".to_string(),
            DataType::Blob,
            false,
            0,
        )];

        let mut values = HashMap::new();
        values.insert(
            "blob_col".to_string(),
            Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]),
        );
        let row = QueryRow::with_values(RowKey::new(vec![1]), values);
        result.rows.push(row);

        let bytes = ParquetWriter::write(&result, &default_options()).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(batch.num_rows(), 1);
        let col = batch.column(0);
        let binary_array = col.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(binary_array.value(0), &[0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[test]
    fn test_timestamp_values() {
        use crate::types::DataType;
        let mut result = QueryResult::new();
        result.metadata.columns = vec![ColumnInfo::new(
            "ts_col".to_string(),
            DataType::Timestamp,
            false,
            0,
        )];

        // 2023-01-15 10:30:45.123 UTC = 1673778645123 milliseconds
        let mut values = HashMap::new();
        values.insert("ts_col".to_string(), Value::Timestamp(1673778645123));
        let row = QueryRow::with_values(RowKey::new(vec![1]), values);
        result.rows.push(row);

        let bytes = ParquetWriter::write(&result, &default_options()).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_uuid_values() {
        use crate::types::DataType;
        let mut result = QueryResult::new();
        result.metadata.columns = vec![ColumnInfo::new(
            "uuid_col".to_string(),
            DataType::Uuid,
            false,
            0,
        )];

        let uuid_bytes = [
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
            0x77, 0x88,
        ];

        let mut values = HashMap::new();
        values.insert("uuid_col".to_string(), Value::Uuid(uuid_bytes));
        let row = QueryRow::with_values(RowKey::new(vec![1]), values);
        result.rows.push(row);

        let bytes = ParquetWriter::write(&result, &default_options()).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(batch.num_rows(), 1);
        let col = batch.column(0);
        let uuid_array = col.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
        assert_eq!(uuid_array.value(0), uuid_bytes);
    }

    #[test]
    fn test_null_values() {
        use crate::types::DataType;
        let mut result = QueryResult::new();
        result.metadata.columns = vec![ColumnInfo::new(
            "nullable_col".to_string(),
            DataType::Text,
            true,
            0,
        )];

        // First row with value, second row with null
        let mut values1 = HashMap::new();
        values1.insert(
            "nullable_col".to_string(),
            Value::Text("present".to_string()),
        );
        result
            .rows
            .push(QueryRow::with_values(RowKey::new(vec![1]), values1));

        let mut values2 = HashMap::new();
        values2.insert("nullable_col".to_string(), Value::Null);
        result
            .rows
            .push(QueryRow::with_values(RowKey::new(vec![2]), values2));

        let bytes = ParquetWriter::write(&result, &default_options()).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(batch.num_rows(), 2);
        let col = batch.column(0);
        let string_array = col.as_any().downcast_ref::<StringArray>().unwrap();
        assert!(string_array.is_valid(0));
        assert!(!string_array.is_valid(1)); // Null
    }

    #[test]
    fn test_list_values() {
        use crate::types::DataType;
        let mut result = QueryResult::new();
        result.metadata.columns = vec![ColumnInfo::new(
            "list_col".to_string(),
            DataType::List,
            false,
            0,
        )];

        let mut values = HashMap::new();
        values.insert(
            "list_col".to_string(),
            Value::List(vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3),
            ]),
        );
        let row = QueryRow::with_values(RowKey::new(vec![1]), values);
        result.rows.push(row);

        let bytes = ParquetWriter::write(&result, &default_options()).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_map_values() {
        use crate::types::DataType;
        let mut result = QueryResult::new();
        result.metadata.columns = vec![ColumnInfo::new(
            "map_col".to_string(),
            DataType::Map,
            false,
            0,
        )];

        let mut values = HashMap::new();
        values.insert(
            "map_col".to_string(),
            Value::Map(vec![
                (Value::Text("key1".to_string()), Value::Integer(1)),
                (Value::Text("key2".to_string()), Value::Integer(2)),
            ]),
        );
        let row = QueryRow::with_values(RowKey::new(vec![1]), values);
        result.rows.push(row);

        let bytes = ParquetWriter::write(&result, &default_options()).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_config_limit() {
        use crate::types::DataType;
        let mut result = QueryResult::new();
        result.metadata.columns = vec![ColumnInfo::new(
            "id".to_string(),
            DataType::Integer,
            false,
            0,
        )];

        // Add 10 rows
        for i in 1..=10 {
            let mut values = HashMap::new();
            values.insert("id".to_string(), Value::Integer(i));
            let row = QueryRow::with_values(RowKey::new(vec![i as u8]), values);
            result.rows.push(row);
        }

        // Limit to 3 rows
        let options = ParquetExportOptions {
            row_limit: Some(3),
            ..Default::default()
        };
        let bytes = ParquetWriter::write(&result, &options).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(
            batch.num_rows(),
            3,
            "Limit should restrict output to 3 rows"
        );
    }

    #[test]
    fn test_multiple_rows() {
        use crate::types::DataType;
        let mut result = QueryResult::new();
        result.metadata.columns = vec![
            ColumnInfo::new("id".to_string(), DataType::Integer, false, 0),
            ColumnInfo::new("name".to_string(), DataType::Text, false, 1),
        ];

        for i in 1..=5 {
            let mut values = HashMap::new();
            values.insert("id".to_string(), Value::Integer(i));
            values.insert("name".to_string(), Value::Text(format!("row_{i}")));
            let row = QueryRow::with_values(RowKey::new(vec![i as u8]), values);
            result.rows.push(row);
        }

        let bytes = ParquetWriter::write(&result, &default_options()).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(batch.num_rows(), 5);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_counter_values() {
        use crate::types::DataType;
        let mut result = QueryResult::new();
        result.metadata.columns = vec![ColumnInfo::new(
            "counter_col".to_string(),
            DataType::BigInt, // Counters map to BigInt in DataType
            false,
            0,
        )];

        let mut values = HashMap::new();
        values.insert("counter_col".to_string(), Value::Counter(1000000));
        let row = QueryRow::with_values(RowKey::new(vec![1]), values);
        result.rows.push(row);

        let bytes = ParquetWriter::write(&result, &default_options()).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_parquet_magic_bytes() {
        use crate::types::DataType;
        let mut result = QueryResult::new();
        result.metadata.columns = vec![ColumnInfo::new(
            "col".to_string(),
            DataType::Integer,
            false,
            0,
        )];

        let mut values = HashMap::new();
        values.insert("col".to_string(), Value::Integer(42));
        result
            .rows
            .push(QueryRow::with_values(RowKey::new(vec![1]), values));

        let bytes = ParquetWriter::write(&result, &default_options()).unwrap();

        // Parquet files start and end with PAR1 magic bytes
        assert_eq!(&bytes[0..4], b"PAR1", "Should start with PAR1 magic bytes");
        assert_eq!(
            &bytes[bytes.len() - 4..],
            b"PAR1",
            "Should end with PAR1 magic bytes"
        );
    }
}
