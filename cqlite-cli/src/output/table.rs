//! Table output writer for QueryResult
//!
//! This module provides a writer that adapts QueryResult to the existing
//! CqlshTableFormatter for consistent cqlsh-compatible table output.

use crate::config::OutputConfig;
use crate::formatter::CqlshTableFormatter;
use crate::output::value_fmt::ValueFormatter;
use cqlite_core::query::QueryResult;

/// Table writer for QueryResult
///
/// Converts QueryResult instances to cqlsh-compatible table format
/// using the existing CqlshTableFormatter infrastructure.
pub struct TableWriter;

impl TableWriter {
    /// Write QueryResult as a cqlsh-compatible table
    ///
    /// # Arguments
    /// * `result` - The query result to format
    /// * `config` - Output configuration for color support and row limits
    ///
    /// # Returns
    /// Formatted table string with headers, data rows, and row count footer
    ///
    /// # Contract Compliance
    /// - Uses `metadata.columns` for headers and column order (single source of truth)
    /// - Right-aligns numeric columns via CqlshTableFormatter
    /// - Prints `(N rows)` footer matching cqlsh style
    /// - Handles missing values as empty cells (null convention)
    /// - Applies color settings from config
    /// - Respects row limit from config (truncates display if set)
    pub fn write(
        result: &QueryResult,
        config: &OutputConfig,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let mut formatter = CqlshTableFormatter::new();

        // Apply color support from config
        formatter.set_color_support(config.color_enabled);

        // Extract headers from metadata.columns in order
        // This is the single source of truth for column names and order
        let headers: Vec<String> = result
            .metadata
            .columns
            .iter()
            .map(|col| col.name.clone())
            .collect();

        formatter.set_headers(headers);

        // Apply row limit if specified in config
        let rows_to_display = if let Some(limit) = config.limit {
            &result.rows[..result.rows.len().min(limit)]
        } else {
            &result.rows
        };

        // Add rows in column order from metadata
        for row in rows_to_display {
            let row_data: Vec<String> = result
                .metadata
                .columns
                .iter()
                .map(|col| {
                    // Look up value by column name from metadata
                    // If missing, treat as null (empty string per contract)
                    row.values.get(col.name.as_str())
                        .map(|v| ValueFormatter::format_value(v))
                        .unwrap_or_else(|| String::new())
                })
                .collect();
            formatter.add_row(row_data);
        }

        // CqlshTableFormatter handles row count footer automatically
        Ok(formatter.format())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqlite_core::query::{ColumnInfo, QueryRow};
    use cqlite_core::types::DataType;
    use cqlite_core::{RowKey, Value};

    #[test]
    fn test_empty_result() {
        let result = QueryResult::new();
        let config = OutputConfig::default();
        let output = TableWriter::write(&result, &config).unwrap();
        assert!(
            output.is_empty(),
            "Empty result should produce empty output"
        );
    }

    #[test]
    fn test_basic_table() {
        let mut result = QueryResult::new();

        // Set up metadata with column order
        result.metadata.columns = vec![
            ColumnInfo::new("id".to_string(), DataType::Integer, false, 0),
            ColumnInfo::new("name".to_string(), DataType::Text, true, 1),
        ];

        // Add rows
        let mut row1 = QueryRow::new(RowKey::new(vec![1]));
        row1.set("id".to_string(), Value::Integer(1));
        row1.set("name".to_string(), Value::Text("Alice".to_string()));

        let mut row2 = QueryRow::new(RowKey::new(vec![2]));
        row2.set("id".to_string(), Value::Integer(2));
        row2.set("name".to_string(), Value::Text("Bob".to_string()));

        result.rows = vec![row1, row2];

        let config = OutputConfig::default();
        let output = TableWriter::write(&result, &config).unwrap();

        // Verify headers are present
        assert!(output.contains("id"), "Output should contain 'id' header");
        assert!(
            output.contains("name"),
            "Output should contain 'name' header"
        );

        // Verify data is present
        assert!(output.contains("Alice"), "Output should contain 'Alice'");
        assert!(output.contains("Bob"), "Output should contain 'Bob'");

        // Verify row count footer
        assert!(
            output.contains("(2 rows)"),
            "Output should contain '(2 rows)' footer"
        );
    }

    #[test]
    fn test_column_order_from_metadata() {
        let mut result = QueryResult::new();

        // Set up metadata with specific column order
        result.metadata.columns = vec![
            ColumnInfo::new("z_last".to_string(), DataType::Integer, false, 0),
            ColumnInfo::new("a_first".to_string(), DataType::Text, false, 1),
        ];

        // Add row with values (order in HashMap doesn't matter)
        let mut row = QueryRow::new(RowKey::new(vec![1]));
        row.set("a_first".to_string(), Value::Text("first".to_string()));
        row.set("z_last".to_string(), Value::Integer(999));

        result.rows = vec![row];

        let config = OutputConfig::default();
        let output = TableWriter::write(&result, &config).unwrap();

        // The output should follow metadata.columns order (z_last before a_first)
        // We can verify this by checking the position of headers in the output
        let z_pos = output.find("z_last").expect("Should find z_last");
        let a_pos = output.find("a_first").expect("Should find a_first");
        assert!(
            z_pos < a_pos,
            "z_last should appear before a_first in output"
        );
    }

    #[test]
    fn test_null_values() {
        let mut result = QueryResult::new();

        result.metadata.columns = vec![
            ColumnInfo::new("id".to_string(), DataType::Integer, false, 0),
            ColumnInfo::new("optional".to_string(), DataType::Text, true, 1),
        ];

        // Row with missing value (treated as null)
        let mut row = QueryRow::new(RowKey::new(vec![1]));
        row.set("id".to_string(), Value::Integer(1));
        // "optional" is not set, should be treated as null

        result.rows = vec![row];

        let config = OutputConfig::default();
        let output = TableWriter::write(&result, &config).unwrap();

        // Should not crash and should handle missing value
        assert!(output.contains("id"));
        assert!(output.contains("optional"));
        assert!(output.contains("(1 rows)"));
    }

    #[test]
    fn test_row_count_footer() {
        let mut result = QueryResult::new();

        result.metadata.columns = vec![ColumnInfo::new(
            "id".to_string(),
            DataType::Integer,
            false,
            0,
        )];

        // Add 5 rows
        for i in 1..=5 {
            let mut row = QueryRow::new(RowKey::new(vec![i as u8]));
            row.set("id".to_string(), Value::Integer(i));
            result.rows.push(row);
        }

        let config = OutputConfig::default();
        let output = TableWriter::write(&result, &config).unwrap();

        // Should show (5 rows)
        assert!(
            output.contains("(5 rows)"),
            "Output should contain '(5 rows)' footer"
        );
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
            let mut row = QueryRow::new(RowKey::new(vec![i as u8]));
            row.set("id".to_string(), Value::Integer(i));
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
        let output = TableWriter::write(&result, &config).unwrap();

        // Should only show first 3 rows
        assert!(output.contains("1"), "Output should contain row 1");
        assert!(output.contains("2"), "Output should contain row 2");
        assert!(output.contains("3"), "Output should contain row 3");

        // Should show (3 rows) in footer, not (10 rows)
        assert!(
            output.contains("(3 rows)"),
            "Output should contain '(3 rows)' footer"
        );
    }

    #[test]
    fn test_config_no_limit() {
        let mut result = QueryResult::new();

        result.metadata.columns = vec![ColumnInfo::new(
            "id".to_string(),
            DataType::Integer,
            false,
            0,
        )];

        // Add 5 rows
        for i in 1..=5 {
            let mut row = QueryRow::new(RowKey::new(vec![i as u8]));
            row.set("id".to_string(), Value::Integer(i));
            result.rows.push(row);
        }

        // No limit
        let config = OutputConfig {
            color_enabled: true,
            limit: None,
            page_size: None,
            target: crate::output::OutputTarget::Stdout,
            overwrite: false,
        };
        let output = TableWriter::write(&result, &config).unwrap();

        // Should show all 5 rows
        assert!(
            output.contains("(5 rows)"),
            "Output should contain '(5 rows)' footer"
        );
    }

    #[test]
    fn test_config_colors_disabled() {
        let mut result = QueryResult::new();

        result.metadata.columns = vec![ColumnInfo::new(
            "id".to_string(),
            DataType::Integer,
            false,
            0,
        )];

        let mut row = QueryRow::new(RowKey::new(vec![1]));
        row.set("id".to_string(), Value::Integer(1));
        result.rows = vec![row];

        // Disable colors
        let config = OutputConfig {
            color_enabled: false,
            limit: None,
            page_size: None,
            target: crate::output::OutputTarget::Stdout,
            overwrite: false,
        };
        let output = TableWriter::write(&result, &config).unwrap();

        // Output should not contain ANSI color codes (e.g., \x1b[)
        assert!(
            !output.contains("\x1b["),
            "Output should not contain ANSI color codes when colors are disabled"
        );
    }
}
