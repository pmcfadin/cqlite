//! JSON output writer for QueryResult
//!
//! Emits deterministic JSON with keys in column order (as defined in metadata.columns).
//! This ensures that JSON object keys appear in the same order as columns, NOT in
//! arbitrary HashMap iteration order.

use crate::config::OutputConfig;
use crate::output::{OutputError, StreamingWriter};
use cqlite_core::query::{QueryMetadata, QueryResult, QueryRow};
use serde::ser::{Serialize, SerializeMap, SerializeSeq, Serializer};
use std::error::Error as StdError;
use std::io::Write;

use super::json_cell::JsonCell;

/// A single result row serialized as a JSON object with keys in `metadata.columns`
/// order, borrowing each column name (`&str`) as the object key instead of cloning
/// a `String` per row (issue #1499).
///
/// Because it drives serde_json's own serializer, the produced bytes are identical
/// to building a `serde_json::Map` and calling `to_string`/`to_string_pretty`.
struct RowObj<'a> {
    row: &'a QueryRow,
    keys: &'a [&'a str],
}

/// De-duplicate output column keys keeping the FIRST position and the LAST value,
/// matching the historical `serde_json::Map::insert` (with `preserve_order`)
/// collapse of duplicate keys. A query with duplicate output column names (e.g.
/// `SELECT a, a` or duplicate aliases) must render a SINGLE JSON key, not two.
///
/// Returns `None` when there are no duplicates so the caller keeps using the
/// borrowed key slice allocation-free (the common, unique-key case). The
/// duplicate check itself does not allocate.
///
/// Because the row's values live in a `HashMap` keyed by column name, every
/// occurrence of a duplicate key resolves to the same (last-written) value, so
/// keeping the first position with a single entry is byte-identical to the old
/// `Map::insert` last-wins behaviour.
fn dedup_keys_last_wins<'a>(keys: &[&'a str]) -> Option<Vec<&'a str>> {
    let has_dup = keys.iter().enumerate().any(|(i, k)| keys[..i].contains(k));
    if !has_dup {
        return None;
    }
    let mut unique: Vec<&str> = Vec::with_capacity(keys.len());
    for &k in keys {
        if !unique.contains(&k) {
            unique.push(k);
        }
    }
    Some(unique)
}

impl Serialize for RowObj<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.keys.len()))?;
        for &key in self.keys {
            // Missing column → JSON null, matching the historical Map behaviour.
            let cell = match self.row.values.get(key) {
                Some(value) => JsonCell::from_value(value),
                None => JsonCell::Plain(serde_json::Value::Null),
            };
            map.serialize_entry(key, &cell)?;
        }
        map.end()
    }
}

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
        // Apply row limit if specified in config
        let rows_to_display = if let Some(limit) = config.limit {
            &result.rows[..result.rows.len().min(limit)]
        } else {
            &result.rows
        };

        // Column names are borrowed once (not cloned per row) as object keys.
        let keys: Vec<&str> = result
            .metadata
            .columns
            .iter()
            .map(|col| col.name.as_str())
            .collect();
        // Collapse duplicate output column names to a single (last-wins) key,
        // matching the historical `Map::insert` semantics (issue #1499). Only
        // allocated when a duplicate is actually present.
        let deduped = dedup_keys_last_wins(&keys);
        let keys: &[&str] = deduped.as_deref().unwrap_or(&keys);

        // Serialize directly through serde_json's pretty serializer so the bytes
        // are identical to the previous `to_string_pretty(Vec<Object>)` path while
        // avoiding a per-row key clone (issue #1499).
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut serializer = serde_json::Serializer::pretty(&mut buf);
            let mut seq = serializer.serialize_seq(Some(rows_to_display.len()))?;
            for row in rows_to_display {
                seq.serialize_element(&RowObj { row, keys })?;
            }
            SerializeSeq::end(seq)?;
        }
        String::from_utf8(buf).map_err(|e| e.into())
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

    /// Serialize a single row to a JSON object string with deterministic key
    /// ordering, borrowing column names as keys (no per-row key clone, issue
    /// #1499). Output is byte-identical to serializing a `serde_json::Map`.
    fn row_to_json_string(&self, row: &QueryRow) -> Result<String, serde_json::Error> {
        let keys: Vec<&str> = self.columns.iter().map(|c| c.as_str()).collect();
        // Collapse duplicate column names to a single (last-wins) key so the
        // streaming writer matches the batch writer and the old `Map::insert`
        // semantics (issue #1499). Allocation-free when there are no duplicates.
        let deduped = dedup_keys_last_wins(&keys);
        let keys: &[&str] = deduped.as_deref().unwrap_or(&keys);
        let obj = RowObj { row, keys };
        if self.pretty {
            serde_json::to_string_pretty(&obj)
        } else {
            serde_json::to_string(&obj)
        }
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
            let json_str = self
                .row_to_json_string(row)
                .map_err(|e| OutputError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;

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
                // Indent each line
                for line in json_str.lines() {
                    write!(self.writer, "  {}", line).map_err(OutputError::Io)?;
                    writeln!(self.writer).map_err(OutputError::Io)?;
                }
            } else {
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

    /// Issue #1499: the borrowed-key serializer must produce byte-identical pretty
    /// JSON to the previous `serde_json::Map` + `to_string_pretty` path.
    #[test]
    fn test_borrowed_key_pretty_output_is_byte_identical() {
        let mut result = QueryResult::new();
        result.metadata.columns = vec![
            ColumnInfo::new(
                "id".to_string(),
                cqlite_core::types::DataType::Integer,
                false,
                0,
            ),
            ColumnInfo::new(
                "name".to_string(),
                cqlite_core::types::DataType::Text,
                false,
                1,
            ),
        ];
        let mut values = HashMap::new();
        values.insert("id".to_string(), Value::Integer(7));
        values.insert("name".to_string(), Value::text("null".to_string()));
        result
            .rows
            .push(QueryRow::with_values(RowKey::new(vec![7]), values));

        let json_str = JSONWriter::write(&result, &default_config()).unwrap();

        // Reference: what the old Map-based path produced.
        let mut map = serde_json::Map::new();
        map.insert("id".to_string(), serde_json::json!(7));
        map.insert("name".to_string(), serde_json::json!("null"));
        let expected = serde_json::to_string_pretty(&vec![serde_json::Value::Object(map)]).unwrap();

        assert_eq!(json_str, expected);
        // A literal text "null" is a JSON string, never dropped.
        assert!(json_str.contains("\"null\""));
    }

    /// Issue #1499: a result whose `metadata.columns` contains a duplicate output
    /// column name (e.g. `SELECT a, a`) must render a SINGLE `"a"` key holding the
    /// LAST value, byte-identical to the old `serde_json::Map::insert` (last-wins)
    /// path — NOT two duplicate `"a"` keys.
    #[test]
    fn test_duplicate_column_names_collapse_last_wins_batch() {
        let mut result = QueryResult::new();
        result.metadata.columns = vec![
            ColumnInfo::new(
                "a".to_string(),
                cqlite_core::types::DataType::Integer,
                false,
                0,
            ),
            ColumnInfo::new(
                "a".to_string(),
                cqlite_core::types::DataType::Integer,
                false,
                1,
            ),
        ];
        // The row's HashMap holds a single value per name — the LAST written value.
        let mut values = HashMap::new();
        values.insert("a".to_string(), Value::Integer(2));
        result
            .rows
            .push(QueryRow::with_values(RowKey::new(vec![1]), values));

        let json_str = JSONWriter::write(&result, &default_config()).unwrap();

        // Reference: old Map-based path, inserting both duplicate columns in order
        // (first=1, then last=2) collapses to a single `"a"` key holding 2.
        let mut map = serde_json::Map::new();
        map.insert("a".to_string(), serde_json::json!(1));
        map.insert("a".to_string(), serde_json::json!(2));
        let expected = serde_json::to_string_pretty(&vec![serde_json::Value::Object(map)]).unwrap();

        assert_eq!(
            json_str, expected,
            "duplicate column name must collapse to a single last-wins key"
        );
        // Exactly one occurrence of the `"a"` key.
        assert_eq!(json_str.matches("\"a\"").count(), 1);
    }

    /// Issue #1499: the streaming writer must apply the same duplicate-key collapse
    /// as the batch writer.
    #[test]
    fn test_duplicate_column_names_collapse_last_wins_streaming() {
        let metadata = {
            let mut m = QueryResult::new().metadata;
            m.columns = vec![
                ColumnInfo::new(
                    "a".to_string(),
                    cqlite_core::types::DataType::Integer,
                    false,
                    0,
                ),
                ColumnInfo::new(
                    "a".to_string(),
                    cqlite_core::types::DataType::Integer,
                    false,
                    1,
                ),
            ];
            m
        };

        let mut values = HashMap::new();
        values.insert("a".to_string(), Value::Integer(2));
        let row = QueryRow::with_values(RowKey::new(vec![1]), values);

        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer = StreamingJSONWriter::new(&mut buf);
            writer.write_header(&metadata).unwrap();
            writer.write_chunk(std::slice::from_ref(&row)).unwrap();
            writer.finalize().unwrap();
        }
        let json_str = String::from_utf8(buf).unwrap();

        // Parsing into a Map (last-wins) proves there is exactly one `"a"` key.
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&json_str).unwrap();
        assert_eq!(parsed.len(), 1);
        let obj = parsed[0].as_object().unwrap();
        assert_eq!(obj.len(), 1, "duplicate key must collapse to one entry");
        assert_eq!(obj.get("a").unwrap(), &serde_json::json!(2));
        // No duplicate `"a"` key in the raw bytes.
        assert_eq!(json_str.matches("\"a\"").count(), 1);
    }

    #[test]
    fn test_empty_result_is_empty_array_bytes() {
        // serialize_seq(Some(0)) must still render exactly "[]".
        let result = QueryResult::new();
        let json_str = JSONWriter::write(&result, &default_config()).unwrap();
        assert_eq!(json_str, "[]");
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
        let values = HashMap::new(); // Empty - no value for nullable_col
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
        values.insert("text_col".to_string(), Value::text("hello".to_string()));
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
            Value::Tombstone(Box::new(TombstoneInfo {
                deletion_time: 0,
                tombstone_type: TombstoneType::CellTombstone,
                local_deletion_time: 0,
                ttl: None,
                range_start: None,
                range_end: None,
            })),
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
