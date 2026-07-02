//! JSON output writer for QueryResult
//!
//! Emits deterministic JSON with keys in column order (as defined in metadata.columns).
//! This ensures that JSON object keys appear in the same order as columns, NOT in
//! arbitrary HashMap iteration order.

use crate::config::OutputConfig;
use crate::output::{OutputError, StreamingWriter};
use cqlite_core::query::{QueryMetadata, QueryResult, QueryRow};
use cqlite_core::Value;
use serde_json::{json, Map, Value as JsonValue};
use std::error::Error as StdError;
use std::io::Write;

use super::value_fmt::ValueFormatter;

/// JSON writer for QueryResult
#[allow(dead_code)]
pub struct JSONWriter;

impl JSONWriter {
    /// Write QueryResult to JSON string with deterministic key ordering
    ///
    /// # Key Ordering Guarantee
    ///
    /// JSON object keys will appear in the SAME order as columns in `metadata.columns`.
    /// This is critical for testing and ensures deterministic output regardless of
    /// HashMap iteration order.
    ///
    /// # Example
    ///
    /// If metadata.columns = [c, b, a], the JSON will be:
    /// ```json
    /// [
    ///   {"c": 1, "b": 2, "a": 3}
    /// ]
    /// ```
    /// NOT {"a": 3, "b": 2, "c": 1}
    ///
    /// # Arguments
    ///
    /// * `result` - The query result to convert to JSON
    /// * `config` - Output configuration for row limits
    ///
    /// # Returns
    ///
    /// Pretty-printed JSON string or error
    #[allow(dead_code)]
    pub fn write(result: &QueryResult, config: &OutputConfig) -> Result<String, Box<dyn StdError>> {
        let mut rows_json = Vec::new();

        // Apply row limit if specified in config
        let rows_to_display = if let Some(limit) = config.limit {
            &result.rows[..result.rows.len().min(limit)]
        } else {
            &result.rows
        };

        for row in rows_to_display {
            // CRITICAL: Use LinkedHashMap-like pattern by iterating columns in order
            let mut row_obj = Map::new();

            // Iterate columns in metadata order, NOT HashMap order!
            for col in &result.metadata.columns {
                let value_opt = row.values.get(col.name.as_str());
                let json_value = match value_opt {
                    Some(value) => Self::value_to_json(value),
                    None => JsonValue::Null,
                };
                row_obj.insert(col.name.clone(), json_value);
            }

            rows_json.push(JsonValue::Object(row_obj));
        }

        // Pretty-print for readability
        serde_json::to_string_pretty(&rows_json).map_err(|e| e.into())
    }

    /// Convert a CQLite Value to a serde_json::Value
    ///
    /// Uses string representations for complex types to ensure human readability.
    #[allow(dead_code)]
    fn value_to_json(value: &Value) -> JsonValue {
        match value {
            Value::Null => JsonValue::Null,
            Value::Boolean(b) => JsonValue::Bool(*b),
            Value::Integer(i) => JsonValue::Number((*i).into()),
            Value::BigInt(i) => JsonValue::Number((*i).into()),
            Value::Counter(c) => JsonValue::Number((*c).into()),
            Value::TinyInt(i) => JsonValue::Number((*i as i64).into()),
            Value::SmallInt(i) => JsonValue::Number((*i as i64).into()),
            Value::Float(f) => serde_json::Number::from_f64(*f)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null),
            Value::Float32(f) => serde_json::Number::from_f64(*f as f64)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null),
            Value::Text(s) => JsonValue::String(s.clone()),
            // Use ValueFormatter for human-readable Blob formatting (0x... hex)
            Value::Blob(_) => JsonValue::String(ValueFormatter::format_value(value)),
            // Use ValueFormatter for human-readable Timestamp (YYYY-MM-DD HH:MM:SS.fff+0000)
            Value::Timestamp(_) => JsonValue::String(ValueFormatter::format_value(value)),
            // Use ValueFormatter for human-readable Date (YYYY-MM-DD)
            Value::Date(_) => JsonValue::String(ValueFormatter::format_value(value)),
            // Use ValueFormatter for human-readable Time (HH:MM:SS.nnnnnnnnn)
            Value::Time(_) => JsonValue::String(ValueFormatter::format_value(value)),
            Value::Uuid(uuid) => {
                // Format UUID as standard hyphenated string
                let uuid_str = format!(
                    "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                    uuid[0], uuid[1], uuid[2], uuid[3],
                    uuid[4], uuid[5],
                    uuid[6], uuid[7],
                    uuid[8], uuid[9],
                    uuid[10], uuid[11], uuid[12], uuid[13], uuid[14], uuid[15]
                );
                JsonValue::String(uuid_str)
            }
            // Use ValueFormatter for human-readable Varint (decimal string)
            Value::Varint(_) => JsonValue::String(ValueFormatter::format_value(value)),
            // Use ValueFormatter for human-readable Decimal (e.g., "69799.73")
            Value::Decimal { .. } => JsonValue::String(ValueFormatter::format_value(value)),
            // Use ValueFormatter for human-readable Duration (XmoYdZns format)
            Value::Duration { .. } => JsonValue::String(ValueFormatter::format_value(value)),
            Value::Json(j) => j.clone(),
            Value::List(list) => {
                let json_list: Vec<JsonValue> = list.iter().map(Self::value_to_json).collect();
                JsonValue::Array(json_list)
            }
            Value::Set(set) => {
                let json_list: Vec<JsonValue> = set.iter().map(Self::value_to_json).collect();
                JsonValue::Array(json_list)
            }
            Value::Map(map) => {
                // Maps are Vec<(Value, Value)> in CQLite
                // Represent as array of {"key": k, "value": v} objects for clarity
                let entries: Vec<JsonValue> = map
                    .iter()
                    .map(|(k, v)| {
                        json!({
                            "key": Self::value_to_json(k),
                            "value": Self::value_to_json(v)
                        })
                    })
                    .collect();
                JsonValue::Array(entries)
            }
            Value::Tuple(tuple) => {
                let json_list: Vec<JsonValue> = tuple.iter().map(Self::value_to_json).collect();
                JsonValue::Array(json_list)
            }
            Value::Udt(udt) => {
                let mut udt_obj = Map::new();
                udt_obj.insert(
                    "_type".to_string(),
                    JsonValue::String(udt.type_name.clone()),
                );

                // Preserve field order from UDT definition
                for field in &udt.fields {
                    let field_json = match &field.value {
                        Some(value) => Self::value_to_json(value),
                        None => JsonValue::Null,
                    };
                    udt_obj.insert(field.name.clone(), field_json);
                }

                JsonValue::Object(udt_obj)
            }
            Value::Frozen(boxed_value) => Self::value_to_json(boxed_value),
            // Tombstoned cells represent deleted values. Emit JSON null to match
            // cqlsh and Python binding behaviour (issue #806).
            Value::Tombstone(_) => JsonValue::Null,
            Value::Inet(bytes) => {
                // Format as IP address string if possible
                if bytes.len() == 4 {
                    JsonValue::String(format!(
                        "{}.{}.{}.{}",
                        bytes[0], bytes[1], bytes[2], bytes[3]
                    ))
                } else if bytes.len() == 16 {
                    // IPv6 - use std::net::Ipv6Addr for canonical formatting
                    use std::net::Ipv6Addr;
                    let mut octets = [0u8; 16];
                    octets.copy_from_slice(bytes);
                    let addr = Ipv6Addr::from(octets);
                    JsonValue::String(addr.to_string())
                } else {
                    // Invalid length, encode as base64
                    use base64::Engine;
                    let engine = base64::engine::general_purpose::STANDARD;
                    JsonValue::String(engine.encode(bytes))
                }
            }
        }
    }
}

// ============================================================================
// Streaming JSON Writer (Issue #280)
// ============================================================================

/// Streaming JSON writer for memory-efficient export of large datasets
///
/// Outputs a JSON array with one object per row. Unlike the batch `JSONWriter`,
/// this writer processes data incrementally, allowing export of arbitrarily
/// large result sets within memory constraints.
///
/// # Output Format
///
/// ```json
/// [
///   {"col1": "value1", "col2": 123},
///   {"col1": "value2", "col2": 456}
/// ]
/// ```
///
/// # Example
///
/// ```ignore
/// let file = File::create("output.json")?;
/// let mut writer = StreamingJSONWriter::new(file);
///
/// writer.write_header(&metadata)?;
///
/// for chunk in result_iterator.chunks(10_000) {
///     writer.write_chunk(&chunk)?;
/// }
///
/// writer.finalize()?;
/// ```
pub struct StreamingJSONWriter<W: Write> {
    /// Inner writer
    writer: W,
    /// Column names in order
    columns: Vec<String>,
    /// Count of rows written
    rows_written: u64,
    /// Whether we've written any rows (for comma handling)
    first_row: bool,
    /// Whether to pretty-print
    pretty: bool,
}

impl<W: Write> StreamingJSONWriter<W> {
    /// Create a new streaming JSON writer with pretty-printing
    pub fn new(output: W) -> Self {
        Self {
            writer: output,
            columns: Vec::new(),
            rows_written: 0,
            first_row: true,
            pretty: true,
        }
    }

    /// Create with compact (non-pretty) output
    #[allow(dead_code)]
    pub fn compact(output: W) -> Self {
        Self {
            writer: output,
            columns: Vec::new(),
            rows_written: 0,
            first_row: true,
            pretty: false,
        }
    }

    /// Convert a single row to JSON object with deterministic key ordering
    #[allow(dead_code)]
    fn row_to_json(&self, row: &QueryRow) -> JsonValue {
        let mut row_obj = Map::new();

        // Iterate columns in metadata order, NOT HashMap order
        for col in &self.columns {
            let value_opt = row.values.get(col.as_str());
            let json_value = match value_opt {
                Some(value) => JSONWriter::value_to_json(value),
                None => JsonValue::Null,
            };
            row_obj.insert(col.clone(), json_value);
        }

        JsonValue::Object(row_obj)
    }
}

impl<W: Write + Send> StreamingWriter for StreamingJSONWriter<W> {
    fn write_header(&mut self, metadata: &QueryMetadata) -> Result<(), OutputError> {
        // Store column names for row writing
        self.columns = metadata.columns.iter().map(|c| c.name.clone()).collect();

        // Write opening bracket
        if self.pretty {
            writeln!(self.writer, "[").map_err(OutputError::Io)?;
        } else {
            write!(self.writer, "[").map_err(OutputError::Io)?;
        }

        Ok(())
    }

    fn write_chunk(&mut self, rows: &[QueryRow]) -> Result<usize, OutputError> {
        for row in rows {
            let json_obj = self.row_to_json(row);

            // Handle comma separator between rows
            if !self.first_row {
                if self.pretty {
                    writeln!(self.writer, ",").map_err(OutputError::Io)?;
                } else {
                    write!(self.writer, ",").map_err(OutputError::Io)?;
                }
            }
            self.first_row = false;

            // Write JSON object
            if self.pretty {
                let json_str = serde_json::to_string_pretty(&json_obj).map_err(|e| {
                    OutputError::Io(std::io::Error::new(std::io::ErrorKind::Other, e))
                })?;
                // Indent each line
                for line in json_str.lines() {
                    write!(self.writer, "  {}", line).map_err(OutputError::Io)?;
                    writeln!(self.writer).map_err(OutputError::Io)?;
                }
            } else {
                let json_str = serde_json::to_string(&json_obj).map_err(|e| {
                    OutputError::Io(std::io::Error::new(std::io::ErrorKind::Other, e))
                })?;
                write!(self.writer, "{}", json_str).map_err(OutputError::Io)?;
            }

            self.rows_written += 1;
        }

        Ok(rows.len())
    }

    fn finalize(&mut self) -> Result<(), OutputError> {
        // Write closing bracket
        if self.pretty {
            writeln!(self.writer, "]").map_err(OutputError::Io)?;
        } else {
            write!(self.writer, "]").map_err(OutputError::Io)?;
        }

        self.writer.flush().map_err(OutputError::Io)
    }

    fn rows_written(&self) -> u64 {
        self.rows_written
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqlite_core::query::ColumnInfo;
    use cqlite_core::{RowKey, Value};
    use std::collections::HashMap;

    fn default_config() -> OutputConfig {
        OutputConfig::default()
    }

    #[test]
    fn test_deterministic_key_ordering() {
        // Create QueryResult with columns in reverse alphabetical order: [c, b, a]
        let mut result = QueryResult::new();

        // Set metadata with columns in specific order
        result.metadata.columns = vec![
            ColumnInfo::new(
                "c".to_string(),
                cqlite_core::types::DataType::Integer,
                false,
                0,
            ),
            ColumnInfo::new(
                "b".to_string(),
                cqlite_core::types::DataType::Integer,
                false,
                1,
            ),
            ColumnInfo::new(
                "a".to_string(),
                cqlite_core::types::DataType::Integer,
                false,
                2,
            ),
        ];

        // Add a row
        let mut values = HashMap::new();
        values.insert("a".to_string(), Value::Integer(1));
        values.insert("b".to_string(), Value::Integer(2));
        values.insert("c".to_string(), Value::Integer(3));

        let row = QueryRow::with_values(RowKey::new(vec![1]), values);
        result.rows.push(row);

        // Write to JSON
        let json_str = JSONWriter::write(&result, &default_config()).unwrap();

        // Parse to verify structure
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&json_str).unwrap();
        assert_eq!(parsed.len(), 1);

        let row_obj = parsed[0].as_object().unwrap();

        // CRITICAL: Verify key order matches column order [c, b, a], NOT [a, b, c]
        let keys: Vec<&String> = row_obj.keys().collect();
        assert_eq!(keys, vec!["c", "b", "a"], "Keys must be in column order");

        // Verify JSON string representation has keys in correct order
        assert!(
            json_str.find("\"c\"").unwrap() < json_str.find("\"b\"").unwrap(),
            "Key 'c' must appear before 'b' in JSON string"
        );
        assert!(
            json_str.find("\"b\"").unwrap() < json_str.find("\"a\"").unwrap(),
            "Key 'b' must appear before 'a' in JSON string"
        );
    }

    #[test]
    fn test_null_values() {
        let mut result = QueryResult::new();
        result.metadata.columns = vec![ColumnInfo::new(
            "nullable_col".to_string(),
            cqlite_core::types::DataType::Text,
            true,
            0,
        )];

        // Row with missing value (should be null)
        let values = HashMap::<String, Value>::new(); // Empty - no value for nullable_col
        let row = QueryRow::with_values(RowKey::new(vec![1]), values);
        result.rows.push(row);

        let json_str = JSONWriter::write(&result, &default_config()).unwrap();
        assert!(
            json_str.contains("null"),
            "Missing values should be JSON null"
        );
    }

    #[test]
    fn test_value_types() {
        let mut result = QueryResult::new();
        result.metadata.columns = vec![
            ColumnInfo::new(
                "int_col".to_string(),
                cqlite_core::types::DataType::Integer,
                false,
                0,
            ),
            ColumnInfo::new(
                "text_col".to_string(),
                cqlite_core::types::DataType::Text,
                false,
                1,
            ),
            ColumnInfo::new(
                "bool_col".to_string(),
                cqlite_core::types::DataType::Boolean,
                false,
                2,
            ),
        ];

        let mut values = HashMap::new();
        values.insert("int_col".to_string(), Value::Integer(42));
        values.insert("text_col".to_string(), Value::Text("hello".to_string()));
        values.insert("bool_col".to_string(), Value::Boolean(true));

        let row = QueryRow::with_values(RowKey::new(vec![1]), values);
        result.rows.push(row);

        let json_str = JSONWriter::write(&result, &default_config()).unwrap();

        // Verify values are correctly represented
        assert!(json_str.contains("42"));
        assert!(json_str.contains("\"hello\""));
        assert!(json_str.contains("true"));
    }

    #[test]
    fn test_empty_result() {
        let result = QueryResult::new();
        let json_str = JSONWriter::write(&result, &default_config()).unwrap();

        // Empty result should be empty array
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&json_str).unwrap();
        assert_eq!(parsed.len(), 0);
    }

    #[test]
    fn test_multiple_rows() {
        let mut result = QueryResult::new();
        result.metadata.columns = vec![ColumnInfo::new(
            "id".to_string(),
            cqlite_core::types::DataType::Integer,
            false,
            0,
        )];

        // Add multiple rows
        for i in 1..=3 {
            let mut values = HashMap::new();
            values.insert("id".to_string(), Value::Integer(i));
            let row = QueryRow::with_values(RowKey::new(vec![i as u8]), values);
            result.rows.push(row);
        }

        let json_str = JSONWriter::write(&result, &default_config()).unwrap();
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&json_str).unwrap();
        assert_eq!(parsed.len(), 3);
    }

    #[test]
    fn test_config_limit() {
        let mut result = QueryResult::new();
        result.metadata.columns = vec![ColumnInfo::new(
            "id".to_string(),
            cqlite_core::types::DataType::Integer,
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

        // Apply limit of 3 rows
        let config = OutputConfig {
            color_enabled: true,
            limit: Some(3),
            page_size: None,
            target: crate::output::OutputTarget::Stdout,
            overwrite: false,
        };
        let json_str = JSONWriter::write(&result, &config).unwrap();
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&json_str).unwrap();

        // Should only have 3 rows, not 10
        assert_eq!(parsed.len(), 3, "Limit should restrict output to 3 rows");
    }

    #[test]
    fn test_uuid_formatting() {
        let uuid_bytes = [
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
            0x77, 0x88,
        ];

        let json_val = JSONWriter::value_to_json(&Value::Uuid(uuid_bytes));
        let uuid_str = json_val.as_str().unwrap();

        // Should be formatted as hyphenated UUID
        assert_eq!(uuid_str, "12345678-9abc-def0-1122-334455667788");
    }

    #[test]
    fn test_list_value() {
        let list_value = Value::List(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ]);

        let json_val = JSONWriter::value_to_json(&list_value);
        assert!(json_val.is_array());

        let array = json_val.as_array().unwrap();
        assert_eq!(array.len(), 3);
        assert_eq!(array[0], serde_json::json!(1));
        assert_eq!(array[1], serde_json::json!(2));
        assert_eq!(array[2], serde_json::json!(3));
    }

    #[test]
    fn test_map_value() {
        let map_value = Value::Map(vec![
            (Value::Text("key1".to_string()), Value::Integer(1)),
            (Value::Text("key2".to_string()), Value::Integer(2)),
        ]);

        let json_val = JSONWriter::value_to_json(&map_value);
        assert!(json_val.is_array());

        let array = json_val.as_array().unwrap();
        assert_eq!(array.len(), 2);

        // Each entry should have "key" and "value" fields
        let entry1 = array[0].as_object().unwrap();
        assert_eq!(entry1.get("key").unwrap().as_str().unwrap(), "key1");
        assert_eq!(entry1.get("value").unwrap().as_i64().unwrap(), 1);
    }

    // Issue #227: Tests for human-readable formatting of complex types

    #[test]
    fn test_blob_formatting() {
        let blob = Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        let json_val = JSONWriter::value_to_json(&blob);
        // Should be 0x hex format, not base64
        assert_eq!(json_val.as_str().unwrap(), "0xdeadbeef");
    }

    #[test]
    fn test_timestamp_formatting() {
        // 2023-01-15 10:30:45.123 UTC = 1673778645123 milliseconds
        let timestamp = Value::Timestamp(1673778645123);
        let json_val = JSONWriter::value_to_json(&timestamp);
        let formatted = json_val.as_str().unwrap();
        // Should be human-readable format, not raw milliseconds
        assert!(formatted.starts_with("2023-01-15"));
        assert!(formatted.contains("10:30:45"));
        assert!(formatted.ends_with("+0000"));
    }

    #[test]
    fn test_date_formatting() {
        // 2023-01-01 = 19358 days since 1970-01-01
        let date = Value::Date(19358);
        let json_val = JSONWriter::value_to_json(&date);
        // Should be YYYY-MM-DD format, not raw days number
        assert_eq!(json_val.as_str().unwrap(), "2023-01-01");
    }

    #[test]
    fn test_time_formatting() {
        // 14:30:45.123456789 in nanoseconds
        let nanos =
            14 * 3600 * 1_000_000_000 + 30 * 60 * 1_000_000_000 + 45 * 1_000_000_000 + 123_456_789;
        let time = Value::Time(nanos);
        let json_val = JSONWriter::value_to_json(&time);
        // Should be HH:MM:SS.nnnnnnnnn format, not raw nanoseconds
        assert_eq!(json_val.as_str().unwrap(), "14:30:45.123456789");
    }

    #[test]
    fn test_varint_formatting() {
        let varint = Value::Varint(vec![0x01, 0x00]); // 256
        let json_val = JSONWriter::value_to_json(&varint);
        // Should be decimal string, not base64
        assert_eq!(json_val.as_str().unwrap(), "256");
    }

    #[test]
    fn test_decimal_formatting() {
        // 123.45 with scale=2, unscaled=12345 (big-endian: 0x30, 0x39)
        let decimal = Value::Decimal {
            scale: 2,
            unscaled: vec![0x30, 0x39],
        };
        let json_val = JSONWriter::value_to_json(&decimal);
        // Should be human-readable decimal string, not {scale, unscaled} object
        let formatted = json_val.as_str().unwrap();
        assert!(formatted.contains('.'));
    }

    #[test]
    fn test_duration_formatting() {
        let duration = Value::Duration {
            months: 2,
            days: 15,
            nanos: 123456789,
        };
        let json_val = JSONWriter::value_to_json(&duration);
        // Should be "XmoYdZns" format, not {months, days, nanos} object
        assert_eq!(json_val.as_str().unwrap(), "2mo15d123456789ns");
    }

    /// Issue #806: tombstoned cells must render as JSON null, not as an internal
    /// metadata object.  This matches cqlsh and Python binding behaviour.
    #[test]
    fn test_cell_tombstone_renders_as_null() {
        use cqlite_core::types::{TombstoneInfo, TombstoneType};

        let tombstone = Value::Tombstone(TombstoneInfo {
            deletion_time: 1673778645000000,
            tombstone_type: TombstoneType::CellTombstone,
            local_deletion_time: 0,
            ttl: None,
            range_start: None,
            range_end: None,
        });

        let json_val = JSONWriter::value_to_json(&tombstone);
        assert!(
            json_val.is_null(),
            "CellTombstone must render as JSON null, got: {json_val}"
        );
    }

    #[test]
    fn test_row_tombstone_renders_as_null() {
        use cqlite_core::types::{TombstoneInfo, TombstoneType};

        let tombstone = Value::Tombstone(TombstoneInfo {
            deletion_time: 1673778645000000,
            tombstone_type: TombstoneType::RowTombstone,
            local_deletion_time: 0,
            ttl: None,
            range_start: None,
            range_end: None,
        });

        let json_val = JSONWriter::value_to_json(&tombstone);
        assert!(
            json_val.is_null(),
            "RowTombstone must render as JSON null, got: {json_val}"
        );
    }

    #[test]
    fn test_tombstone_column_in_result_is_null() {
        use cqlite_core::types::{TombstoneInfo, TombstoneType};

        let mut result = QueryResult::new();
        result.metadata.columns = vec![ColumnInfo::new(
            "deleted_col".to_string(),
            cqlite_core::types::DataType::Tombstone,
            true,
            0,
        )];

        let mut values = HashMap::new();
        values.insert(
            "deleted_col".to_string(),
            Value::Tombstone(TombstoneInfo {
                deletion_time: 0,
                tombstone_type: TombstoneType::CellTombstone,
                local_deletion_time: 0,
                ttl: None,
                range_start: None,
                range_end: None,
            }),
        );
        let row = QueryRow::with_values(RowKey::new(vec![1]), values);
        result.rows.push(row);

        let json_str = JSONWriter::write(&result, &default_config()).unwrap();
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&json_str).unwrap();

        let col_val = &parsed[0]["deleted_col"];
        assert!(
            col_val.is_null(),
            "Tombstoned column must be JSON null in output, got: {col_val}"
        );
        // Ensure NO internal metadata leaked
        assert!(
            !json_str.contains("tombstone_type"),
            "Internal tombstone metadata must not appear in output: {json_str}"
        );
    }
}
