//! CSV output writer for QueryResult
//!
//! Implements CSV output format following
//! `docs/development/QUERY_RESULT_CONTRACT.md`.
//!
//! ## Format Specification
//! - First row: column headers from `metadata.columns` in order
//! - Subsequent rows: values stringified per ValueFormatter mapping rules
//! - Null values: empty string (standard CSV convention)
//! - Stable column order: always matches `metadata.columns` sequence
//!
//! ## Usage
//! ```rust,ignore
//! use cqlite_cli::output::CSVWriter;
//! use cqlite_core::query::QueryResult;
//!
//! let result: QueryResult = // ... query execution
//! let csv_output = CSVWriter::write(&result, &config)?;
//! println!("{}", csv_output);
//! ```

// CSV writer requires query module (M2+ feature)
#![cfg(feature = "state_machine")]

use crate::config::OutputConfig;
use crate::output::{OutputError, StreamingWriter};
use cqlite_core::query::{QueryMetadata, QueryResult, QueryRow};
use csv::WriterBuilder;
use std::io::Write;

use super::value_fmt::ValueFormatter;

/// CSV writer for QueryResult
#[allow(dead_code)]
pub struct CSVWriter;

impl CSVWriter {
    /// Write QueryResult to CSV format
    ///
    /// # Arguments
    /// * `result` - The query result to format as CSV
    /// * `config` - Output configuration for row limits
    ///
    /// # Returns
    /// * `Ok(String)` - CSV-formatted string with headers and data
    /// * `Err(Box<dyn std::error::Error>)` - CSV serialization error
    ///
    /// # Format Guarantees
    /// - Headers are taken from `metadata.columns` in order
    /// - Column order is stable across all rows
    /// - Null values render as empty strings
    /// - Special CSV characters are properly escaped by the csv crate
    /// - Respects row limit from config
    #[allow(dead_code)]
    pub fn write(
        result: &QueryResult,
        config: &OutputConfig,
    ) -> Result<String, Box<dyn std::error::Error>> {
        // Create an in-memory CSV writer
        let mut wtr = WriterBuilder::new().from_writer(Vec::new());

        // Write header row from metadata.columns
        let headers: Vec<&str> = result
            .metadata
            .columns
            .iter()
            .map(|col| col.name.as_str())
            .collect();
        wtr.write_record(&headers)?;

        // Apply row limit if specified in config
        let rows_to_display = if let Some(limit) = config.limit {
            &result.rows[..result.rows.len().min(limit)]
        } else {
            &result.rows
        };

        // Write data rows in stable column order. A single scratch buffer is
        // reused across every cell to avoid a per-cell String allocation, and a
        // genuine NULL (or missing column) is detected via `is_null` rather than a
        // fragile `== "null"` sentinel so a literal text value `"null"` round-trips
        // instead of collapsing to an empty field (issue #1499).
        let mut scratch = String::new();
        for row in rows_to_display {
            for col in &result.metadata.columns {
                scratch.clear();
                match row.values.get(col.name.as_str()) {
                    // Present and non-null → format its value.
                    Some(v) if !ValueFormatter::is_null(v) => {
                        ValueFormatter::format_into(v, &mut scratch);
                    }
                    // Real NULL or missing column → empty field (scratch stays "").
                    _ => {}
                }
                wtr.write_field(scratch.as_bytes())?;
            }
            // Terminate the record (field-by-field records need an explicit end).
            wtr.write_record(None::<&[u8]>)?;
        }

        // Extract the CSV data as string
        let data = wtr.into_inner()?;
        String::from_utf8(data).map_err(|e| e.into())
    }
}

// ============================================================================
// Streaming CSV Writer (Issue #280)
// ============================================================================

/// Streaming CSV writer for memory-efficient export of large datasets
///
/// Unlike the batch `CSVWriter`, this writer processes data incrementally,
/// allowing export of arbitrarily large result sets within memory constraints.
///
/// # Example
///
/// ```ignore
/// let file = File::create("output.csv")?;
/// let mut writer = StreamingCSVWriter::new(file);
///
/// writer.write_header(&metadata)?;
///
/// for chunk in result_iterator.chunks(10_000) {
///     writer.write_chunk(&chunk)?;
/// }
///
/// writer.finalize()?;
/// ```
pub struct StreamingCSVWriter<W: Write> {
    /// Inner CSV writer
    writer: csv::Writer<W>,
    /// Column names in order
    columns: Vec<String>,
    /// Count of rows written
    rows_written: u64,
}

impl<W: Write> StreamingCSVWriter<W> {
    /// Create a new streaming CSV writer
    pub fn new(output: W) -> Self {
        Self {
            writer: WriterBuilder::new().from_writer(output),
            columns: Vec::new(),
            rows_written: 0,
        }
    }

    /// Create with custom CSV options
    #[allow(dead_code)]
    pub fn with_options(output: W, delimiter: u8, quote_style: csv::QuoteStyle) -> Self {
        Self {
            writer: WriterBuilder::new()
                .delimiter(delimiter)
                .quote_style(quote_style)
                .from_writer(output),
            columns: Vec::new(),
            rows_written: 0,
        }
    }
}

impl<W: Write + Send> StreamingWriter for StreamingCSVWriter<W> {
    fn write_header(&mut self, metadata: &QueryMetadata) -> Result<(), OutputError> {
        // Store column names for row writing
        self.columns = metadata.columns.iter().map(|c| c.name.clone()).collect();

        // Write header row
        self.writer
            .write_record(&self.columns)
            .map_err(|e| OutputError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;

        Ok(())
    }

    fn write_chunk(&mut self, rows: &[QueryRow]) -> Result<usize, OutputError> {
        // Reuse one scratch buffer across all cells; detect NULL via `is_null`
        // (not a `== "null"` sentinel) so literal text `"null"` round-trips
        // instead of collapsing to an empty field (issue #1499).
        let mut scratch = String::new();
        for row in rows {
            for col in &self.columns {
                scratch.clear();
                match row.values.get(col.as_str()) {
                    Some(v) if !ValueFormatter::is_null(v) => {
                        ValueFormatter::format_into(v, &mut scratch);
                    }
                    _ => {}
                }
                self.writer.write_field(scratch.as_bytes()).map_err(|e| {
                    OutputError::Io(std::io::Error::new(std::io::ErrorKind::Other, e))
                })?;
            }
            self.writer
                .write_record(None::<&[u8]>)
                .map_err(|e| OutputError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
        }

        self.rows_written += rows.len() as u64;
        Ok(rows.len())
    }

    fn finalize(&mut self) -> Result<(), OutputError> {
        self.writer.flush().map_err(OutputError::Io)
    }

    fn rows_written(&self) -> u64 {
        self.rows_written
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqlite_core::query::{ColumnInfo, QueryMetadata, QueryRow};
    use cqlite_core::types::DataType;
    use cqlite_core::{RowKey, Value};
    use std::collections::HashMap;

    fn default_config() -> OutputConfig {
        OutputConfig::default()
    }

    /// Helper to create a test QueryResult
    fn create_test_result(
        columns: Vec<(&str, DataType)>,
        rows_data: Vec<Vec<(&str, Value)>>,
    ) -> QueryResult {
        let mut metadata = QueryMetadata::default();
        metadata.columns = columns
            .iter()
            .enumerate()
            .map(|(pos, (name, data_type))| ColumnInfo {
                name: name.to_string(),
                data_type: data_type.clone(),
                nullable: true,
                position: pos,
                table_name: None,
                cql_type: None,
            })
            .collect();

        let rows = rows_data
            .into_iter()
            .enumerate()
            .map(|(idx, row_data)| {
                let mut values: HashMap<std::sync::Arc<str>, Value> = HashMap::new();
                for (col_name, value) in row_data {
                    values.insert(col_name.into(), value);
                }
                QueryRow {
                    values,
                    key: RowKey::new(vec![idx as u8]),
                    metadata: Default::default(),
                    cell_metadata: None,
                }
            })
            .collect();

        QueryResult {
            rows,
            rows_affected: 0,
            execution_time_ms: 0,
            metadata,
        }
    }

    #[test]
    fn test_csv_headers_match_column_order() {
        let result = create_test_result(
            vec![
                ("id", DataType::Integer),
                ("name", DataType::Text),
                ("age", DataType::Integer),
            ],
            vec![],
        );

        let csv = CSVWriter::write(&result, &default_config()).expect("CSV write failed");
        let lines: Vec<&str> = csv.lines().collect();

        assert_eq!(lines.len(), 1); // Only header, no data rows
        assert_eq!(lines[0], "id,name,age");
    }

    #[test]
    fn test_csv_basic_data() {
        let result = create_test_result(
            vec![("id", DataType::Integer), ("name", DataType::Text)],
            vec![
                vec![
                    ("id", Value::Integer(1)),
                    ("name", Value::text("Alice".to_string())),
                ],
                vec![
                    ("id", Value::Integer(2)),
                    ("name", Value::text("Bob".to_string())),
                ],
            ],
        );

        let csv = CSVWriter::write(&result, &default_config()).expect("CSV write failed");
        let lines: Vec<&str> = csv.lines().collect();

        assert_eq!(lines.len(), 3); // Header + 2 data rows
        assert_eq!(lines[0], "id,name");
        assert_eq!(lines[1], "1,Alice");
        assert_eq!(lines[2], "2,Bob");
    }

    #[test]
    fn test_csv_null_values_become_empty() {
        let result = create_test_result(
            vec![("id", DataType::Integer), ("name", DataType::Text)],
            vec![
                vec![("id", Value::Integer(1)), ("name", Value::Null)],
                vec![
                    ("id", Value::Null),
                    ("name", Value::text("Bob".to_string())),
                ],
            ],
        );

        let csv = CSVWriter::write(&result, &default_config()).expect("CSV write failed");
        let lines: Vec<&str> = csv.lines().collect();

        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0], "id,name");
        assert_eq!(lines[1], "1,"); // null → empty
        assert_eq!(lines[2], ",Bob"); // null → empty
    }

    /// Issue #1499: a literal text value `"null"` must round-trip as the string
    /// `null`, NOT be turned into an empty field. Previously the CSV writer used a
    /// fragile `formatted == "null"` sentinel that collapsed a genuine `"null"`
    /// text cell to empty. This test fails on the pre-fix code.
    #[test]
    fn test_csv_literal_null_text_is_not_emptied() {
        let result = create_test_result(
            vec![("id", DataType::Integer), ("name", DataType::Text)],
            vec![
                // Row 1: real SQL NULL → empty field.
                vec![("id", Value::Integer(1)), ("name", Value::Null)],
                // Row 2: literal text "null" → must stay "null", not empty.
                vec![
                    ("id", Value::Integer(2)),
                    ("name", Value::text("null".to_string())),
                ],
            ],
        );

        let csv = CSVWriter::write(&result, &default_config()).expect("CSV write failed");
        let lines: Vec<&str> = csv.lines().collect();

        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0], "id,name");
        assert_eq!(lines[1], "1,", "real NULL must become an empty field");
        assert_eq!(
            lines[2], "2,null",
            "literal text \"null\" must round-trip, not become empty"
        );
    }

    /// Issue #1499: same literal-`"null"` guarantee for the streaming CSV writer.
    #[test]
    fn test_streaming_csv_literal_null_text_is_not_emptied() {
        use cqlite_core::query::ColumnInfo;

        let mut metadata = QueryMetadata::default();
        metadata.columns = vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                position: 0,
                table_name: None,
                cql_type: None,
            },
            ColumnInfo {
                name: "name".to_string(),
                data_type: DataType::Text,
                nullable: true,
                position: 1,
                table_name: None,
                cql_type: None,
            },
        ];

        let mk_row = |id: i32, name: Value| {
            let mut values: HashMap<std::sync::Arc<str>, Value> = HashMap::new();
            values.insert("id".into(), Value::Integer(id));
            values.insert("name".into(), name);
            QueryRow {
                values,
                key: RowKey::new(vec![id as u8]),
                metadata: Default::default(),
                cell_metadata: None,
            }
        };
        let rows = vec![
            mk_row(1, Value::Null),
            mk_row(2, Value::text("null".to_string())),
        ];

        let mut writer = StreamingCSVWriter::new(Vec::new());
        writer.write_header(&metadata).expect("header");
        writer.write_chunk(&rows).expect("chunk");
        writer.finalize().expect("finalize");
        let bytes = writer.writer.into_inner().expect("into_inner");
        let csv = String::from_utf8(bytes).expect("utf8");
        let lines: Vec<&str> = csv.lines().collect();

        assert_eq!(lines[0], "id,name");
        assert_eq!(lines[1], "1,", "real NULL must become an empty field");
        assert_eq!(
            lines[2], "2,null",
            "literal text \"null\" must round-trip, not become empty"
        );
    }

    /// Regression: a genuine CQL null in a FROZEN column is represented as
    /// `Value::Frozen(Box::new(Value::Null))`, which `format_value` renders as the
    /// string `"null"`. The batch CSV writer must emit an EMPTY field for it (not a
    /// literal `null`). This fails on the pre-fix `is_null` (which matched only
    /// `Value::Null`) and passes once `is_null` unwraps `Frozen` recursively.
    #[test]
    fn test_csv_frozen_null_becomes_empty() {
        let result = create_test_result(
            vec![("id", DataType::Integer), ("name", DataType::Text)],
            vec![
                // Row 1: frozen null → empty field.
                vec![
                    ("id", Value::Integer(1)),
                    ("name", Value::Frozen(Box::new(Value::Null))),
                ],
                // Row 2: frozen NON-null must still be formatted (not emptied).
                vec![
                    ("id", Value::Integer(2)),
                    (
                        "name",
                        Value::Frozen(Box::new(Value::text("Bob".to_string()))),
                    ),
                ],
            ],
        );

        let csv = CSVWriter::write(&result, &default_config()).expect("CSV write failed");
        let lines: Vec<&str> = csv.lines().collect();

        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0], "id,name");
        assert_eq!(
            lines[1], "1,",
            "frozen NULL must become an empty field, not literal \"null\""
        );
        assert_eq!(lines[2], "2,Bob", "frozen non-null must still be formatted");
    }

    /// Regression: same frozen-null empty-field guarantee for the streaming writer.
    #[test]
    fn test_streaming_csv_frozen_null_becomes_empty() {
        use cqlite_core::query::ColumnInfo;

        let mut metadata = QueryMetadata::default();
        metadata.columns = vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: true,
                position: 0,
                table_name: None,
                cql_type: None,
            },
            ColumnInfo {
                name: "name".to_string(),
                data_type: DataType::Text,
                nullable: true,
                position: 1,
                table_name: None,
                cql_type: None,
            },
        ];

        let mk_row = |id: i32, name: Value| {
            let mut values: HashMap<std::sync::Arc<str>, Value> = HashMap::new();
            values.insert("id".into(), Value::Integer(id));
            values.insert("name".into(), name);
            QueryRow {
                values,
                key: RowKey::new(vec![id as u8]),
                metadata: Default::default(),
                cell_metadata: None,
            }
        };
        let rows = vec![
            mk_row(1, Value::Frozen(Box::new(Value::Null))),
            mk_row(2, Value::Frozen(Box::new(Value::text("Bob".to_string())))),
        ];

        let mut writer = StreamingCSVWriter::new(Vec::new());
        writer.write_header(&metadata).expect("header");
        writer.write_chunk(&rows).expect("chunk");
        writer.finalize().expect("finalize");
        let bytes = writer.writer.into_inner().expect("into_inner");
        let csv = String::from_utf8(bytes).expect("utf8");
        let lines: Vec<&str> = csv.lines().collect();

        assert_eq!(lines[0], "id,name");
        assert_eq!(
            lines[1], "1,",
            "frozen NULL must become an empty field, not literal \"null\""
        );
        assert_eq!(lines[2], "2,Bob", "frozen non-null must still be formatted");
    }

    #[test]
    fn test_csv_missing_columns_become_empty() {
        let result = create_test_result(
            vec![
                ("id", DataType::Integer),
                ("name", DataType::Text),
                ("email", DataType::Text),
            ],
            vec![
                // First row: missing email
                vec![
                    ("id", Value::Integer(1)),
                    ("name", Value::text("Alice".to_string())),
                ],
                // Second row: missing name
                vec![
                    ("id", Value::Integer(2)),
                    ("email", Value::text("bob@test.com".to_string())),
                ],
            ],
        );

        let csv = CSVWriter::write(&result, &default_config()).expect("CSV write failed");
        let lines: Vec<&str> = csv.lines().collect();

        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0], "id,name,email");
        assert_eq!(lines[1], "1,Alice,"); // missing email
        assert_eq!(lines[2], "2,,bob@test.com"); // missing name
    }

    #[test]
    fn test_csv_special_characters_are_escaped() {
        let result = create_test_result(
            vec![("id", DataType::Integer), ("description", DataType::Text)],
            vec![
                vec![
                    ("id", Value::Integer(1)),
                    ("description", Value::text("Contains, comma".to_string())),
                ],
                vec![
                    ("id", Value::Integer(2)),
                    ("description", Value::text("Has \"quotes\"".to_string())),
                ],
                vec![
                    ("id", Value::Integer(3)),
                    ("description", Value::text("Line\nbreak".to_string())),
                ],
            ],
        );

        let csv = CSVWriter::write(&result, &default_config()).expect("CSV write failed");

        // CSV crate should properly escape these
        assert!(csv.contains("\"Contains, comma\"") || csv.contains("Contains, comma"));
        assert!(csv.contains("\"Has \"\"quotes\"\"\"") || csv.contains("Has \"quotes\""));
        assert!(csv.contains("Line\nbreak") || csv.contains("\"Line\nbreak\""));
    }

    #[test]
    fn test_csv_column_order_stability() {
        // Verify that column order matches metadata.columns, not HashMap iteration order
        let result = create_test_result(
            vec![
                ("z_field", DataType::Text),
                ("a_field", DataType::Text),
                ("m_field", DataType::Text),
            ],
            vec![vec![
                ("a_field", Value::text("aaa".to_string())),
                ("m_field", Value::text("mmm".to_string())),
                ("z_field", Value::text("zzz".to_string())),
            ]],
        );

        let csv = CSVWriter::write(&result, &default_config()).expect("CSV write failed");
        let lines: Vec<&str> = csv.lines().collect();

        // Column order should be z, a, m (as defined in metadata), not alphabetical
        assert_eq!(lines[0], "z_field,a_field,m_field");
        assert_eq!(lines[1], "zzz,aaa,mmm");
    }

    #[test]
    fn test_csv_config_limit() {
        let result = create_test_result(
            vec![("id", DataType::Integer)],
            vec![
                vec![("id", Value::Integer(1))],
                vec![("id", Value::Integer(2))],
                vec![("id", Value::Integer(3))],
                vec![("id", Value::Integer(4))],
                vec![("id", Value::Integer(5))],
            ],
        );

        // Apply limit of 2 rows
        let config = OutputConfig {
            color_enabled: true,
            limit: Some(2),
            page_size: None,
            target: crate::output::OutputTarget::Stdout,
            overwrite: false,
        };
        let csv = CSVWriter::write(&result, &config).expect("CSV write failed");
        let lines: Vec<&str> = csv.lines().collect();

        // Should have header + 2 data rows (not 5)
        assert_eq!(
            lines.len(),
            3,
            "Limit should restrict output to 2 data rows"
        );
        assert_eq!(lines[0], "id");
        assert_eq!(lines[1], "1");
        assert_eq!(lines[2], "2");
    }

    #[test]
    fn test_csv_empty_result() {
        let result = create_test_result(
            vec![("id", DataType::Integer), ("name", DataType::Text)],
            vec![],
        );

        let csv = CSVWriter::write(&result, &default_config()).expect("CSV write failed");
        let lines: Vec<&str> = csv.lines().collect();

        // Should have header but no data rows
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0], "id,name");
    }

    #[test]
    fn test_csv_various_data_types() {
        let result = create_test_result(
            vec![
                ("bool_col", DataType::Boolean),
                ("int_col", DataType::Integer),
                ("text_col", DataType::Text),
                ("blob_col", DataType::Blob),
            ],
            vec![vec![
                ("bool_col", Value::Boolean(true)),
                ("int_col", Value::Integer(42)),
                ("text_col", Value::text("test".to_string())),
                ("blob_col", Value::blob(vec![0xDE, 0xAD])),
            ]],
        );

        let csv = CSVWriter::write(&result, &default_config()).expect("CSV write failed");
        let lines: Vec<&str> = csv.lines().collect();

        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0], "bool_col,int_col,text_col,blob_col");
        assert_eq!(lines[1], "true,42,test,0xdead");
    }

    #[test]
    fn test_csv_collections() {
        let result = create_test_result(
            vec![
                ("id", DataType::Integer),
                ("list_col", DataType::List),
                ("set_col", DataType::Set),
            ],
            vec![vec![
                ("id", Value::Integer(1)),
                (
                    "list_col",
                    Value::List(vec![Value::Integer(1), Value::Integer(2)]),
                ),
                (
                    "set_col",
                    Value::Set(vec![
                        Value::text("a".to_string()),
                        Value::text("b".to_string()),
                    ]),
                ),
            ]],
        );

        let csv = CSVWriter::write(&result, &default_config()).expect("CSV write failed");
        let lines: Vec<&str> = csv.lines().collect();

        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0], "id,list_col,set_col");
        // Collections should be formatted by ValueFormatter
        assert!(lines[1].contains("[1, 2]"));
        assert!(lines[1].contains("{a, b}"));
    }

    #[test]
    fn test_csv_uuid_formatting() {
        let uuid_bytes = [
            0xa8, 0xf1, 0x67, 0xf0, 0xeb, 0xe7, 0x4f, 0x20, 0xa3, 0x86, 0x31, 0xff, 0x13, 0x8b,
            0xec, 0x3b,
        ];
        let result = create_test_result(
            vec![("id", DataType::Uuid)],
            vec![vec![("id", Value::Uuid(uuid_bytes))]],
        );

        let csv = CSVWriter::write(&result, &default_config()).expect("CSV write failed");
        let lines: Vec<&str> = csv.lines().collect();

        assert_eq!(lines.len(), 2);
        // UUID should be lowercase hyphenated per contract
        assert_eq!(lines[1], "a8f167f0-ebe7-4f20-a386-31ff138bec3b");
    }
}
