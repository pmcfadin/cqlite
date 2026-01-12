//! Parquet output writer for QueryResult
//!
//! Converts CQL query results to Apache Parquet format with proper type mapping.
//! Uses Snappy compression by default (Cassandra default, good speed/size balance).

use crate::config::OutputConfig;
use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, ListArray, MapArray, StringArray, StructArray,
    TimestampMillisecondArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType as ArrowDataType, Field, Fields, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use cqlite_core::query::{ColumnInfo, QueryResult};
use cqlite_core::types::DataType;
use cqlite_core::Value;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::error::Error as StdError;
use std::sync::Arc;

use super::value_fmt::ValueFormatter;

/// Parquet writer for QueryResult
///
/// Converts query results to Apache Parquet binary format.
/// Unlike JSON/CSV writers, this returns `Vec<u8>` (binary data).
#[allow(dead_code)]
pub struct ParquetWriter;

impl ParquetWriter {
    /// Write QueryResult to Parquet binary format
    ///
    /// # Arguments
    ///
    /// * `result` - The query result to convert to Parquet
    /// * `config` - Output configuration for row limits
    ///
    /// # Returns
    ///
    /// Binary Parquet data or error
    #[allow(dead_code)]
    pub fn write(
        result: &QueryResult,
        config: &OutputConfig,
    ) -> Result<Vec<u8>, Box<dyn StdError>> {
        // Handle empty results
        if result.metadata.columns.is_empty() {
            return Self::write_empty_parquet();
        }

        // Build Arrow schema from column metadata
        let schema = Self::build_schema(&result.metadata.columns)?;

        // Apply row limit if specified in config
        let rows_to_process = if let Some(limit) = config.limit {
            &result.rows[..result.rows.len().min(limit)]
        } else {
            &result.rows
        };

        // Convert rows to Arrow arrays (one per column)
        let arrays = Self::convert_to_arrays(&result.metadata.columns, rows_to_process)?;

        // Create RecordBatch
        let batch = RecordBatch::try_new(Arc::new(schema), arrays)?;

        // Write to Parquet with Snappy compression
        Self::write_parquet(&batch)
    }

    /// Write an empty Parquet file
    fn write_empty_parquet() -> Result<Vec<u8>, Box<dyn StdError>> {
        let schema = Schema::empty();
        let batch = RecordBatch::new_empty(Arc::new(schema));
        Self::write_parquet(&batch)
    }

    /// Build Arrow schema from CQL column metadata
    fn build_schema(columns: &[ColumnInfo]) -> Result<Schema, Box<dyn StdError>> {
        let fields: Vec<Field> = columns
            .iter()
            .map(|col| Self::column_to_field(col))
            .collect();
        Ok(Schema::new(fields))
    }

    /// Convert a CQL column to Arrow field
    fn column_to_field(col: &ColumnInfo) -> Field {
        let arrow_type = Self::data_type_to_arrow(&col.data_type);
        Field::new(&col.name, arrow_type, col.nullable)
    }

    /// Map CQL DataType to Arrow DataType
    fn data_type_to_arrow(data_type: &DataType) -> ArrowDataType {
        match data_type {
            DataType::Null => ArrowDataType::Null,
            DataType::Boolean => ArrowDataType::Boolean,
            DataType::TinyInt => ArrowDataType::Int8,
            DataType::SmallInt => ArrowDataType::Int16,
            DataType::Integer => ArrowDataType::Int32,
            DataType::BigInt => ArrowDataType::Int64,
            DataType::Float32 => ArrowDataType::Float32,
            DataType::Float => ArrowDataType::Float64,
            DataType::Text => ArrowDataType::Utf8,
            DataType::Blob => ArrowDataType::Binary,
            DataType::Timestamp => {
                ArrowDataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
            }
            DataType::Uuid => ArrowDataType::FixedSizeBinary(16),
            DataType::Json => ArrowDataType::Utf8,
            DataType::List => {
                ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Utf8, true)))
            }
            DataType::Set => {
                ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Utf8, true)))
            }
            DataType::Map => ArrowDataType::Map(
                Arc::new(Field::new(
                    "entries",
                    ArrowDataType::Struct(Fields::from(vec![
                        Field::new("key", ArrowDataType::Utf8, false),
                        Field::new("value", ArrowDataType::Utf8, true),
                    ])),
                    false,
                )),
                false,
            ),
            DataType::Tuple => ArrowDataType::Utf8, // Serialize as JSON string
            DataType::Udt => ArrowDataType::Utf8,   // Serialize as JSON string
            DataType::Frozen => ArrowDataType::Utf8,
            DataType::Tombstone => ArrowDataType::Utf8,
        }
    }

    /// Convert all rows to Arrow arrays (column-oriented)
    fn convert_to_arrays(
        columns: &[ColumnInfo],
        rows: &[cqlite_core::query::QueryRow],
    ) -> Result<Vec<ArrayRef>, Box<dyn StdError>> {
        columns
            .iter()
            .map(|col| Self::convert_column_to_array(col, rows))
            .collect()
    }

    /// Convert a single column across all rows to an Arrow array
    fn convert_column_to_array(
        col: &ColumnInfo,
        rows: &[cqlite_core::query::QueryRow],
    ) -> Result<ArrayRef, Box<dyn StdError>> {
        match &col.data_type {
            DataType::Boolean => Self::build_boolean_array(col, rows),
            DataType::TinyInt => Self::build_int8_array(col, rows),
            DataType::SmallInt => Self::build_int16_array(col, rows),
            DataType::Integer => Self::build_int32_array(col, rows),
            DataType::BigInt => Self::build_int64_array(col, rows),
            DataType::Float32 => Self::build_float32_array(col, rows),
            DataType::Float => Self::build_float64_array(col, rows),
            DataType::Text | DataType::Json => Self::build_string_array(col, rows),
            DataType::Blob => Self::build_binary_array(col, rows),
            DataType::Timestamp => Self::build_timestamp_array(col, rows),
            DataType::Uuid => Self::build_uuid_array(col, rows),
            DataType::List | DataType::Set => Self::build_list_array(col, rows),
            DataType::Map => Self::build_map_array(col, rows),
            DataType::Tuple
            | DataType::Udt
            | DataType::Frozen
            | DataType::Tombstone
            | DataType::Null => {
                Self::build_string_array(col, rows) // Fallback to string representation
            }
        }
    }

    // =========================================================================
    // Type-specific array builders
    // =========================================================================

    fn build_boolean_array(
        col: &ColumnInfo,
        rows: &[cqlite_core::query::QueryRow],
    ) -> Result<ArrayRef, Box<dyn StdError>> {
        let values: Vec<Option<bool>> = rows
            .iter()
            .map(|row| {
                row.values.get(&col.name).and_then(|v| match v {
                    Value::Boolean(b) => Some(*b),
                    Value::Null => None,
                    _ => None,
                })
            })
            .collect();
        Ok(Arc::new(BooleanArray::from(values)))
    }

    fn build_int8_array(
        col: &ColumnInfo,
        rows: &[cqlite_core::query::QueryRow],
    ) -> Result<ArrayRef, Box<dyn StdError>> {
        let values: Vec<Option<i8>> = rows
            .iter()
            .map(|row| {
                row.values.get(&col.name).and_then(|v| match v {
                    Value::TinyInt(i) => Some(*i),
                    Value::Null => None,
                    _ => None,
                })
            })
            .collect();
        Ok(Arc::new(Int8Array::from(values)))
    }

    fn build_int16_array(
        col: &ColumnInfo,
        rows: &[cqlite_core::query::QueryRow],
    ) -> Result<ArrayRef, Box<dyn StdError>> {
        let values: Vec<Option<i16>> = rows
            .iter()
            .map(|row| {
                row.values.get(&col.name).and_then(|v| match v {
                    Value::SmallInt(i) => Some(*i),
                    Value::Null => None,
                    _ => None,
                })
            })
            .collect();
        Ok(Arc::new(Int16Array::from(values)))
    }

    fn build_int32_array(
        col: &ColumnInfo,
        rows: &[cqlite_core::query::QueryRow],
    ) -> Result<ArrayRef, Box<dyn StdError>> {
        let values: Vec<Option<i32>> = rows
            .iter()
            .map(|row| {
                row.values.get(&col.name).and_then(|v| match v {
                    Value::Integer(i) => Some(*i),
                    Value::Date(d) => Some(*d), // Date is stored as i32 days
                    Value::Null => None,
                    _ => None,
                })
            })
            .collect();
        Ok(Arc::new(Int32Array::from(values)))
    }

    fn build_int64_array(
        col: &ColumnInfo,
        rows: &[cqlite_core::query::QueryRow],
    ) -> Result<ArrayRef, Box<dyn StdError>> {
        let values: Vec<Option<i64>> = rows
            .iter()
            .map(|row| {
                row.values.get(&col.name).and_then(|v| match v {
                    Value::BigInt(i) => Some(*i),
                    Value::Counter(c) => Some(*c),
                    Value::Time(t) => Some(*t), // Time is stored as i64 nanos
                    Value::Null => None,
                    _ => None,
                })
            })
            .collect();
        Ok(Arc::new(Int64Array::from(values)))
    }

    fn build_float32_array(
        col: &ColumnInfo,
        rows: &[cqlite_core::query::QueryRow],
    ) -> Result<ArrayRef, Box<dyn StdError>> {
        let values: Vec<Option<f32>> = rows
            .iter()
            .map(|row| {
                row.values.get(&col.name).and_then(|v| match v {
                    Value::Float32(f) => Some(*f),
                    Value::Null => None,
                    _ => None,
                })
            })
            .collect();
        Ok(Arc::new(Float32Array::from(values)))
    }

    fn build_float64_array(
        col: &ColumnInfo,
        rows: &[cqlite_core::query::QueryRow],
    ) -> Result<ArrayRef, Box<dyn StdError>> {
        let values: Vec<Option<f64>> = rows
            .iter()
            .map(|row| {
                row.values.get(&col.name).and_then(|v| match v {
                    Value::Float(f) => Some(*f),
                    Value::Float32(f) => Some(*f as f64),
                    Value::Null => None,
                    _ => None,
                })
            })
            .collect();
        Ok(Arc::new(Float64Array::from(values)))
    }

    fn build_string_array(
        col: &ColumnInfo,
        rows: &[cqlite_core::query::QueryRow],
    ) -> Result<ArrayRef, Box<dyn StdError>> {
        let values: Vec<Option<String>> = rows
            .iter()
            .map(|row| {
                row.values.get(&col.name).and_then(|v| match v {
                    Value::Null => None,
                    Value::Text(s) => Some(s.clone()),
                    Value::Json(j) => Some(j.to_string()),
                    // Use ValueFormatter for complex types
                    other => Some(ValueFormatter::format_value(other)),
                })
            })
            .collect();
        Ok(Arc::new(StringArray::from(values)))
    }

    fn build_binary_array(
        col: &ColumnInfo,
        rows: &[cqlite_core::query::QueryRow],
    ) -> Result<ArrayRef, Box<dyn StdError>> {
        let values: Vec<Option<&[u8]>> = rows
            .iter()
            .map(|row| {
                row.values.get(&col.name).and_then(|v| match v {
                    Value::Blob(b) => Some(b.as_slice()),
                    Value::Null => None,
                    _ => None,
                })
            })
            .collect();
        Ok(Arc::new(BinaryArray::from(values)))
    }

    fn build_timestamp_array(
        col: &ColumnInfo,
        rows: &[cqlite_core::query::QueryRow],
    ) -> Result<ArrayRef, Box<dyn StdError>> {
        let values: Vec<Option<i64>> = rows
            .iter()
            .map(|row| {
                row.values.get(&col.name).and_then(|v| match v {
                    Value::Timestamp(ts) => Some(*ts),
                    Value::Null => None,
                    _ => None,
                })
            })
            .collect();
        Ok(Arc::new(
            TimestampMillisecondArray::from(values).with_timezone("UTC"),
        ))
    }

    fn build_uuid_array(
        col: &ColumnInfo,
        rows: &[cqlite_core::query::QueryRow],
    ) -> Result<ArrayRef, Box<dyn StdError>> {
        let values: Vec<Option<[u8; 16]>> = rows
            .iter()
            .map(|row| {
                row.values.get(&col.name).and_then(|v| match v {
                    Value::Uuid(uuid) => Some(*uuid),
                    Value::Null => None,
                    _ => None,
                })
            })
            .collect();

        // Build FixedSizeBinaryArray manually
        let mut builder = arrow::array::FixedSizeBinaryBuilder::new(16);
        for opt in values {
            match opt {
                Some(uuid) => builder.append_value(&uuid)?,
                None => builder.append_null(),
            }
        }
        Ok(Arc::new(builder.finish()))
    }

    fn build_list_array(
        col: &ColumnInfo,
        rows: &[cqlite_core::query::QueryRow],
    ) -> Result<ArrayRef, Box<dyn StdError>> {
        // For lists/sets, we serialize elements as strings for simplicity
        let mut offsets: Vec<i32> = vec![0];
        let mut values: Vec<Option<String>> = Vec::new();
        let mut null_bitmap: Vec<bool> = Vec::new();

        for row in rows {
            match row.values.get(&col.name) {
                Some(Value::List(items)) | Some(Value::Set(items)) => {
                    null_bitmap.push(true);
                    for item in items {
                        values.push(Some(ValueFormatter::format_value(item)));
                    }
                    offsets.push(values.len() as i32);
                }
                Some(Value::Null) | None => {
                    null_bitmap.push(false);
                    offsets.push(values.len() as i32);
                }
                _ => {
                    null_bitmap.push(false);
                    offsets.push(values.len() as i32);
                }
            }
        }

        let values_array = Arc::new(StringArray::from(values)) as ArrayRef;
        let field = Arc::new(Field::new("item", ArrowDataType::Utf8, true));
        let offset_buffer = OffsetBuffer::new(offsets.into());
        let null_buffer = NullBuffer::from(null_bitmap);

        Ok(Arc::new(ListArray::new(
            field,
            offset_buffer,
            values_array,
            Some(null_buffer),
        )))
    }

    fn build_map_array(
        col: &ColumnInfo,
        rows: &[cqlite_core::query::QueryRow],
    ) -> Result<ArrayRef, Box<dyn StdError>> {
        // For maps, serialize key-value pairs as structs
        let mut offsets: Vec<i32> = vec![0];
        let mut keys: Vec<Option<String>> = Vec::new();
        let mut values: Vec<Option<String>> = Vec::new();
        let mut null_bitmap: Vec<bool> = Vec::new();

        for row in rows {
            match row.values.get(&col.name) {
                Some(Value::Map(pairs)) => {
                    null_bitmap.push(true);
                    for (k, v) in pairs {
                        keys.push(Some(ValueFormatter::format_value(k)));
                        values.push(Some(ValueFormatter::format_value(v)));
                    }
                    offsets.push(keys.len() as i32);
                }
                Some(Value::Null) | None => {
                    null_bitmap.push(false);
                    offsets.push(keys.len() as i32);
                }
                _ => {
                    null_bitmap.push(false);
                    offsets.push(keys.len() as i32);
                }
            }
        }

        // Build struct array for entries
        let key_array = Arc::new(StringArray::from(keys)) as ArrayRef;
        let value_array = Arc::new(StringArray::from(values)) as ArrayRef;

        let struct_fields = Fields::from(vec![
            Field::new("key", ArrowDataType::Utf8, false),
            Field::new("value", ArrowDataType::Utf8, true),
        ]);

        let entries_array =
            StructArray::new(struct_fields.clone(), vec![key_array, value_array], None);

        let map_field = Arc::new(Field::new(
            "entries",
            ArrowDataType::Struct(struct_fields),
            false,
        ));
        let offset_buffer = OffsetBuffer::new(offsets.into());
        let null_buffer = NullBuffer::from(null_bitmap);

        Ok(Arc::new(MapArray::new(
            map_field,
            offset_buffer,
            entries_array,
            Some(null_buffer),
            false,
        )))
    }

    /// Write RecordBatch to Parquet bytes
    fn write_parquet(batch: &RecordBatch) -> Result<Vec<u8>, Box<dyn StdError>> {
        let mut buffer = Vec::new();

        // Configure Snappy compression (Cassandra default)
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props))?;
        writer.write(batch)?;
        writer.close()?;

        Ok(buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, FixedSizeBinaryArray};
    use bytes::Bytes;
    use cqlite_core::query::{ColumnInfo, QueryRow};
    use cqlite_core::{RowKey, Value};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::collections::HashMap;

    fn default_config() -> OutputConfig {
        OutputConfig::default()
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
        let bytes = ParquetWriter::write(&result, &default_config()).unwrap();

        // Should produce valid (empty) Parquet
        assert!(!bytes.is_empty());
        // Parquet magic bytes: PAR1
        assert_eq!(&bytes[0..4], b"PAR1");
    }

    #[test]
    fn test_boolean_values() {
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

        let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);
    }

    #[test]
    fn test_integer_types() {
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

        let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 4);
    }

    #[test]
    fn test_float_types() {
        let mut result = QueryResult::new();
        result.metadata.columns = vec![
            ColumnInfo::new("f32".to_string(), DataType::Float32, false, 0),
            ColumnInfo::new("f64".to_string(), DataType::Float, false, 1),
        ];

        let mut values = HashMap::new();
        values.insert("f32".to_string(), Value::Float32(3.14));
        values.insert("f64".to_string(), Value::Float(2.71828));
        let row = QueryRow::with_values(RowKey::new(vec![1]), values);
        result.rows.push(row);

        let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_text_values() {
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

        let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(batch.num_rows(), 1);
        let col = batch.column(0);
        let string_array = col.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(string_array.value(0), "Hello, Parquet!");
    }

    #[test]
    fn test_blob_values() {
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

        let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(batch.num_rows(), 1);
        let col = batch.column(0);
        let binary_array = col.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(binary_array.value(0), &[0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[test]
    fn test_timestamp_values() {
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

        let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_uuid_values() {
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

        let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(batch.num_rows(), 1);
        let col = batch.column(0);
        let uuid_array = col.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
        assert_eq!(uuid_array.value(0), uuid_bytes);
    }

    #[test]
    fn test_null_values() {
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

        let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(batch.num_rows(), 2);
        let col = batch.column(0);
        let string_array = col.as_any().downcast_ref::<StringArray>().unwrap();
        assert!(string_array.is_valid(0));
        assert!(!string_array.is_valid(1)); // Null
    }

    #[test]
    fn test_list_values() {
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

        let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_map_values() {
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

        let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_config_limit() {
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
        let config = OutputConfig {
            color_enabled: true,
            limit: Some(3),
            page_size: None,
            target: crate::output::OutputTarget::Stdout,
            overwrite: false,
        };
        let bytes = ParquetWriter::write(&result, &config).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(
            batch.num_rows(),
            3,
            "Limit should restrict output to 3 rows"
        );
    }

    #[test]
    fn test_multiple_rows() {
        let mut result = QueryResult::new();
        result.metadata.columns = vec![
            ColumnInfo::new("id".to_string(), DataType::Integer, false, 0),
            ColumnInfo::new("name".to_string(), DataType::Text, false, 1),
        ];

        for i in 1..=5 {
            let mut values = HashMap::new();
            values.insert("id".to_string(), Value::Integer(i));
            values.insert("name".to_string(), Value::Text(format!("row_{}", i)));
            let row = QueryRow::with_values(RowKey::new(vec![i as u8]), values);
            result.rows.push(row);
        }

        let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(batch.num_rows(), 5);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_counter_values() {
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

        let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
        let batch = read_parquet_back(&bytes).unwrap();

        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_parquet_magic_bytes() {
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

        let bytes = ParquetWriter::write(&result, &default_config()).unwrap();

        // Parquet files start and end with PAR1 magic bytes
        assert_eq!(&bytes[0..4], b"PAR1", "Should start with PAR1 magic bytes");
        assert_eq!(
            &bytes[bytes.len() - 4..],
            b"PAR1",
            "Should end with PAR1 magic bytes"
        );
    }
}
