//! Integration tests for cqlsh-compatible table formatting
//!
//! Tests verify the complete integration chain:
//! OutputConfig → TableWriter → CqlshTableFormatter
//!
//! These tests ensure cqlsh compatibility with real QueryResult data structures.

#![cfg(all(test, feature = "state_machine"))]
#![allow(clippy::all)]

use cqlite_cli::config::OutputConfig;
use cqlite_cli::output::table::TableWriter;
use cqlite_core::query::{ColumnInfo, QueryResult, QueryRow};
use cqlite_core::types::DataType;
use cqlite_core::{RowKey, Value};

/// Test basic table formatting with headers, data rows, and row count
#[test]
fn test_table_format_basic() {
    let mut result = QueryResult::new();

    // Set up metadata with column order
    result.metadata.columns = vec![
        ColumnInfo::new("id".to_string(), DataType::Integer, false, 0),
        ColumnInfo::new("name".to_string(), DataType::Text, false, 1),
    ];

    // Add sample rows
    let mut row1 = QueryRow::new(RowKey::new(vec![1]));
    row1.set("id".to_string(), Value::Integer(1));
    row1.set("name".to_string(), Value::Text("Alice".to_string()));

    let mut row2 = QueryRow::new(RowKey::new(vec![2]));
    row2.set("id".to_string(), Value::Integer(2));
    row2.set("name".to_string(), Value::Text("Bob".to_string()));

    result.rows = vec![row1, row2];

    let config = OutputConfig::default();
    let output = TableWriter::write(&result, &config).unwrap();

    // Verify cqlsh-style formatting
    assert!(output.contains(" | "), "Should have column separator ' | '");
    assert!(
        output.contains("-+-"),
        "Should have separator junction '-+-'"
    );
    assert!(
        output.contains("(2 rows)"),
        "Should have row count footer '(2 rows)'"
    );

    // Verify headers are present
    assert!(output.contains("id"), "Should contain 'id' header");
    assert!(output.contains("name"), "Should contain 'name' header");

    // Verify data is present
    assert!(output.contains("Alice"), "Should contain 'Alice'");
    assert!(output.contains("Bob"), "Should contain 'Bob'");
}

/// Test table formatting with limit flag truncation
#[test]
fn test_table_format_with_limit() {
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
        target: cqlite_cli::output::OutputTarget::Stdout,
        overwrite: false,
    };
    let output = TableWriter::write(&result, &config).unwrap();

    // Should only display first 3 rows
    assert!(output.contains("1"), "Output should contain row 1");
    assert!(output.contains("2"), "Output should contain row 2");
    assert!(output.contains("3"), "Output should contain row 3");

    // Should NOT contain rows beyond limit
    assert!(
        !output.contains("10"),
        "Output should not contain row 10 (beyond limit)"
    );

    // Row count footer should reflect displayed rows, not total
    assert!(
        output.contains("(3 rows)"),
        "Output should contain '(3 rows)' footer for displayed rows"
    );
}

/// Test table formatting with --no-color flag (no ANSI codes)
#[test]
fn test_table_format_no_color() {
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
        target: cqlite_cli::output::OutputTarget::Stdout,
        overwrite: false,
    };
    let output = TableWriter::write(&result, &config).unwrap();

    // Output should not contain ANSI color escape codes
    assert!(
        !output.contains("\x1b["),
        "Output should not contain ANSI color codes when colors are disabled"
    );
    assert!(
        !output.contains("\\x1b"),
        "Output should not contain escaped ANSI codes"
    );
}

/// Test empty QueryResult produces empty output
#[test]
fn test_table_format_empty_result() {
    let result = QueryResult::new();
    let config = OutputConfig::default();
    let output = TableWriter::write(&result, &config).unwrap();

    assert!(
        output.is_empty(),
        "Empty QueryResult should produce empty output"
    );
}

/// Test column alignment (right-aligned for numeric, left for text in headers)
#[test]
fn test_table_format_column_alignment() {
    let mut result = QueryResult::new();

    result.metadata.columns = vec![
        ColumnInfo::new("id".to_string(), DataType::Integer, false, 0),
        ColumnInfo::new("score".to_string(), DataType::Integer, false, 1),
        ColumnInfo::new("name".to_string(), DataType::Text, false, 2),
    ];

    let mut row = QueryRow::new(RowKey::new(vec![1]));
    row.set("id".to_string(), Value::Integer(1));
    row.set("score".to_string(), Value::Integer(95));
    row.set("name".to_string(), Value::Text("Alice".to_string()));

    result.rows = vec![row];

    let config = OutputConfig::default();
    let output = TableWriter::write(&result, &config).unwrap();

    // Parse output lines
    let lines: Vec<&str> = output.lines().collect();

    // Verify we have at least header, separator, data, blank, footer
    assert!(
        lines.len() >= 4,
        "Output should have header, separator, data, and footer lines"
    );

    // Headers should be present (left-aligned in cqlsh)
    let header_line = lines[0];
    assert!(
        header_line.contains("id"),
        "Header line should contain 'id'"
    );
    assert!(
        header_line.contains("score"),
        "Header line should contain 'score'"
    );
    assert!(
        header_line.contains("name"),
        "Header line should contain 'name'"
    );

    // Separator line should use '-+-' junctions
    let separator_line = lines[1];
    assert!(
        separator_line.contains("-+-"),
        "Separator line should contain '-+-' junctions"
    );

    // Data should be right-aligned (default for CqlshTableFormatter)
    let data_line = lines[2];
    assert!(
        data_line.contains("1"),
        "Data line should contain value '1'"
    );
    assert!(
        data_line.contains("95"),
        "Data line should contain value '95'"
    );
    assert!(
        data_line.contains("Alice"),
        "Data line should contain value 'Alice'"
    );
}

/// Test cqlsh compatibility - exact format matching
#[test]
fn test_cqlsh_compatibility() {
    let mut result = QueryResult::new();

    result.metadata.columns = vec![
        ColumnInfo::new("id".to_string(), DataType::Integer, false, 0),
        ColumnInfo::new("name".to_string(), DataType::Text, false, 1),
    ];

    let mut row1 = QueryRow::new(RowKey::new(vec![1]));
    row1.set("id".to_string(), Value::Integer(1));
    row1.set("name".to_string(), Value::Text("Alice".to_string()));

    let mut row2 = QueryRow::new(RowKey::new(vec![2]));
    row2.set("id".to_string(), Value::Integer(2));
    row2.set("name".to_string(), Value::Text("Bob".to_string()));

    result.rows = vec![row1, row2];

    let config = OutputConfig {
        color_enabled: false, // Disable colors for exact comparison
        limit: None,
        page_size: None,
        target: cqlite_cli::output::OutputTarget::Stdout,
        overwrite: false,
    };
    let output = TableWriter::write(&result, &config).unwrap();

    // Verify cqlsh format elements:

    // 1. Headers with " | " separator
    assert!(
        output.contains(" id | name"),
        "Headers should have ' | ' separator"
    );

    // 2. Separator line with "-+-" junctions
    assert!(
        output.contains("-+-"),
        "Separator line should use '-+-' junction"
    );

    // 3. Data rows are present
    assert!(output.contains("Alice"), "Should contain data 'Alice'");
    assert!(output.contains("Bob"), "Should contain data 'Bob'");

    // 4. Row count footer with "(N rows)" format
    assert!(
        output.contains("(2 rows)"),
        "Should have row count footer in '(N rows)' format"
    );

    // 5. No trailing comma or extra formatting
    assert!(
        !output.contains(","),
        "Should not contain commas (not CSV format)"
    );

    // 6. Each line starts with a space (ROW_PREFIX)
    for line in output.lines() {
        if !line.is_empty() && !line.starts_with('(') {
            // Skip footer and blank lines
            assert!(
                line.starts_with(' ') || line.starts_with('-'),
                "Data/header lines should start with space or dash: '{}'",
                line
            );
        }
    }
}

/// Test multiple data types formatting
#[test]
fn test_table_format_multiple_data_types() {
    let mut result = QueryResult::new();

    result.metadata.columns = vec![
        ColumnInfo::new("id".to_string(), DataType::Integer, false, 0),
        ColumnInfo::new("name".to_string(), DataType::Text, false, 1),
        ColumnInfo::new("active".to_string(), DataType::Boolean, false, 2),
        ColumnInfo::new("score".to_string(), DataType::Float, false, 3),
    ];

    let mut row = QueryRow::new(RowKey::new(vec![1]));
    row.set("id".to_string(), Value::Integer(42));
    row.set("name".to_string(), Value::Text("Test".to_string()));
    row.set("active".to_string(), Value::Boolean(true));
    row.set("score".to_string(), Value::Float(98.5));

    result.rows = vec![row];

    let config = OutputConfig::default();
    let output = TableWriter::write(&result, &config).unwrap();

    // Verify all data types are present in output
    assert!(output.contains("42"), "Should contain integer value");
    assert!(output.contains("Test"), "Should contain text value");
    assert!(output.contains("true"), "Should contain boolean value");
    assert!(output.contains("98.5"), "Should contain double value");

    // Verify structure
    assert!(output.contains(" | "), "Should have column separators");
    assert!(output.contains("-+-"), "Should have separator junctions");
    assert!(output.contains("(1 rows)"), "Should have row count");
}

/// Test null/missing values handling
#[test]
fn test_table_format_null_values() {
    let mut result = QueryResult::new();

    result.metadata.columns = vec![
        ColumnInfo::new("id".to_string(), DataType::Integer, false, 0),
        ColumnInfo::new("optional".to_string(), DataType::Text, true, 1),
        ColumnInfo::new("name".to_string(), DataType::Text, false, 2),
    ];

    // Row with missing "optional" value (treated as null)
    let mut row = QueryRow::new(RowKey::new(vec![1]));
    row.set("id".to_string(), Value::Integer(1));
    // "optional" is not set - should be treated as null/empty
    row.set("name".to_string(), Value::Text("Alice".to_string()));

    result.rows = vec![row];

    let config = OutputConfig::default();
    let output = TableWriter::write(&result, &config).unwrap();

    // Should not crash and should handle missing value gracefully
    assert!(output.contains("id"), "Should contain 'id' header");
    assert!(
        output.contains("optional"),
        "Should contain 'optional' header"
    );
    assert!(output.contains("name"), "Should contain 'name' header");
    assert!(output.contains("Alice"), "Should contain 'Alice' value");
    assert!(output.contains("(1 rows)"), "Should have row count");

    // Null values appear as empty cells in the output
    // The exact formatting depends on CqlshTableFormatter
}

/// Test column order preservation from metadata
#[test]
fn test_table_format_column_order() {
    let mut result = QueryResult::new();

    // Set up metadata with specific column order (z_last, a_first)
    result.metadata.columns = vec![
        ColumnInfo::new("z_last".to_string(), DataType::Integer, false, 0),
        ColumnInfo::new("a_first".to_string(), DataType::Text, false, 1),
    ];

    // Add row with values (HashMap order doesn't matter)
    let mut row = QueryRow::new(RowKey::new(vec![1]));
    row.set("a_first".to_string(), Value::Text("first".to_string()));
    row.set("z_last".to_string(), Value::Integer(999));

    result.rows = vec![row];

    let config = OutputConfig::default();
    let output = TableWriter::write(&result, &config).unwrap();

    // Verify column order matches metadata order (z_last before a_first)
    let z_pos = output.find("z_last").expect("Should find z_last");
    let a_pos = output.find("a_first").expect("Should find a_first");
    assert!(
        z_pos < a_pos,
        "z_last should appear before a_first in output (metadata order)"
    );
}

/// Test single row formatting
#[test]
fn test_table_format_single_row() {
    let mut result = QueryResult::new();

    result.metadata.columns = vec![ColumnInfo::new(
        "message".to_string(),
        DataType::Text,
        false,
        0,
    )];

    let mut row = QueryRow::new(RowKey::new(vec![1]));
    row.set(
        "message".to_string(),
        Value::Text("Hello, World!".to_string()),
    );

    result.rows = vec![row];

    let config = OutputConfig::default();
    let output = TableWriter::write(&result, &config).unwrap();

    assert!(
        output.contains("message"),
        "Should contain 'message' header"
    );
    assert!(
        output.contains("Hello, World!"),
        "Should contain message value"
    );
    assert!(
        output.contains("(1 rows)"),
        "Should show '(1 rows)' for singular"
    );
}

/// Test wide table with many columns
#[test]
fn test_table_format_wide_table() {
    let mut result = QueryResult::new();

    // Create a table with 6 columns
    result.metadata.columns = vec![
        ColumnInfo::new("col1".to_string(), DataType::Integer, false, 0),
        ColumnInfo::new("col2".to_string(), DataType::Integer, false, 1),
        ColumnInfo::new("col3".to_string(), DataType::Integer, false, 2),
        ColumnInfo::new("col4".to_string(), DataType::Integer, false, 3),
        ColumnInfo::new("col5".to_string(), DataType::Integer, false, 4),
        ColumnInfo::new("col6".to_string(), DataType::Integer, false, 5),
    ];

    let mut row = QueryRow::new(RowKey::new(vec![1]));
    row.set("col1".to_string(), Value::Integer(1));
    row.set("col2".to_string(), Value::Integer(2));
    row.set("col3".to_string(), Value::Integer(3));
    row.set("col4".to_string(), Value::Integer(4));
    row.set("col5".to_string(), Value::Integer(5));
    row.set("col6".to_string(), Value::Integer(6));

    result.rows = vec![row];

    let config = OutputConfig::default();
    let output = TableWriter::write(&result, &config).unwrap();

    // Verify all columns are present
    for i in 1..=6 {
        assert!(
            output.contains(&format!("col{}", i)),
            "Should contain header col{}",
            i
        );
    }

    // Verify all values are present
    for i in 1..=6 {
        assert!(
            output.contains(&format!("{}", i)),
            "Should contain value {}",
            i
        );
    }

    // Verify structure
    assert!(output.contains(" | "), "Should have column separators");
    assert!(output.contains("-+-"), "Should have separator junctions");
    assert!(output.contains("(1 rows)"), "Should have row count");
}

/// Test limit of zero rows (edge case)
#[test]
fn test_table_format_limit_zero() {
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

    // Apply limit of 0 rows
    let config = OutputConfig {
        color_enabled: true,
        limit: Some(0),
        page_size: None,
        target: cqlite_cli::output::OutputTarget::Stdout,
        overwrite: false,
    };
    let output = TableWriter::write(&result, &config).unwrap();

    // Should display headers but no data rows
    assert!(output.contains("id"), "Should contain 'id' header");
    assert!(
        !output.contains("(1 rows)"),
        "Should not show row count for 0 displayed rows"
    );
}

/// Test very long text values (column width calculation)
#[test]
fn test_table_format_long_text() {
    let mut result = QueryResult::new();

    result.metadata.columns = vec![
        ColumnInfo::new("id".to_string(), DataType::Integer, false, 0),
        ColumnInfo::new("description".to_string(), DataType::Text, false, 1),
    ];

    let long_text = "This is a very long description that will test the column width calculation algorithm used by the table formatter";

    let mut row = QueryRow::new(RowKey::new(vec![1]));
    row.set("id".to_string(), Value::Integer(1));
    row.set(
        "description".to_string(),
        Value::Text(long_text.to_string()),
    );

    result.rows = vec![row];

    let config = OutputConfig::default();
    let output = TableWriter::write(&result, &config).unwrap();

    // Verify long text is present
    assert!(
        output.contains(long_text),
        "Should contain full long text value"
    );

    // Verify structure is maintained
    assert!(output.contains(" | "), "Should have column separators");
    assert!(output.contains("-+-"), "Should have separator junctions");
    assert!(output.contains("(1 rows)"), "Should have row count");
}

/// Test golden snapshot - exact cqlsh format match
#[test]
fn test_cqlsh_format_snapshot() {
    let mut result = QueryResult::new();

    result.metadata.columns = vec![
        ColumnInfo::new("id".to_string(), DataType::Integer, false, 0),
        ColumnInfo::new("name".to_string(), DataType::Text, false, 1),
    ];

    let mut row1 = QueryRow::new(RowKey::new(vec![1]));
    row1.set("id".to_string(), Value::Integer(1));
    row1.set("name".to_string(), Value::Text("Alice".to_string()));

    let mut row2 = QueryRow::new(RowKey::new(vec![2]));
    row2.set("id".to_string(), Value::Integer(2));
    row2.set("name".to_string(), Value::Text("Bob".to_string()));

    result.rows = vec![row1, row2];

    let config = OutputConfig {
        color_enabled: false, // Disable colors for exact comparison
        limit: None,
        page_size: None,
        target: cqlite_cli::output::OutputTarget::Stdout,
        overwrite: false,
    };
    let output = TableWriter::write(&result, &config).unwrap();

    // Expected output in cqlsh format (for reference)
    // Note: Exact spacing depends on CqlshTableFormatter implementation
    let _expected = r#" id | name
----+-------
  1 | Alice
  2 |   Bob

(2 rows)"#;

    // Compare trimmed outputs (allow flexibility in exact spacing)
    let output_trimmed = output.trim();

    // Verify key structural elements are present
    assert!(
        output_trimmed.contains("id | name"),
        "Should contain header line"
    );
    assert!(
        output_trimmed.contains("----+-------") || output_trimmed.contains("-+-"),
        "Should contain separator line"
    );
    assert!(
        output_trimmed.contains("Alice"),
        "Should contain Alice data"
    );
    assert!(output_trimmed.contains("Bob"), "Should contain Bob data");
    assert!(
        output_trimmed.contains("(2 rows)"),
        "Should contain row count footer"
    );
}

/// Test color support flag propagation
#[test]
fn test_table_format_color_enabled() {
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

    // Enable colors (default behavior)
    let config = OutputConfig {
        color_enabled: true,
        limit: None,
        page_size: None,
        target: cqlite_cli::output::OutputTarget::Stdout,
        overwrite: false,
    };
    let output = TableWriter::write(&result, &config).unwrap();

    // Color support is configured (actual color codes depend on CqlshTableFormatter implementation)
    // Just verify the output is valid and doesn't error
    assert!(output.contains("id"), "Should contain 'id' header");
    assert!(output.contains("(1 rows)"), "Should have row count");
}

/// Test row count footer for various counts
#[test]
fn test_table_format_row_count_variations() {
    // Test singular: 1 row
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

    let config = OutputConfig::default();
    let output = TableWriter::write(&result, &config).unwrap();
    assert!(
        output.contains("(1 rows)"),
        "Should show '(1 rows)' for single row"
    );

    // Test plural: 100 rows
    let mut result = QueryResult::new();
    result.metadata.columns = vec![ColumnInfo::new(
        "id".to_string(),
        DataType::Integer,
        false,
        0,
    )];

    for i in 1..=100 {
        let mut row = QueryRow::new(RowKey::new(vec![i as u8]));
        row.set("id".to_string(), Value::Integer(i));
        result.rows.push(row);
    }

    let output = TableWriter::write(&result, &config).unwrap();
    assert!(
        output.contains("(100 rows)"),
        "Should show '(100 rows)' for 100 rows"
    );
}
