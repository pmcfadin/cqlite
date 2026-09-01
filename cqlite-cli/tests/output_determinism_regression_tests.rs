//! Output Determinism Regression Tests
//!
//! These tests ensure that JSON and CSV output formats maintain deterministic, stable
//! column ordering as specified in `docs/development/QUERY_RESULT_CONTRACT.md`
//! (section 1, "Column order", which names these tests). They serve as regression
//! protection against accidental ordering changes.
//!
//! # Contract Guarantees Tested
//!
//! 1. **JSON Key Ordering**: Keys must appear in metadata.columns order, NOT alphabetical
//!    or HashMap iteration order
//! 2. **CSV Column Ordering**: Headers and data must follow metadata.columns order exactly
//! 3. **HashMap Independence**: Output order must NOT change based on value HashMap
//!    insertion order
//! 4. **String Position Verification**: Use character position assertions to detect
//!    subtle ordering regressions
//!
//! # Why These Tests Exist
//!
//! Without explicit ordering guarantees, HashMap iteration could produce:
//! - Non-deterministic test failures
//! - Inconsistent user experience
//! - Breaking changes in JSON/CSV parsers that depend on column order
//! - Difficult debugging when columns appear in unexpected orders
//!
//! These tests MUST FAIL if ordering behavior changes, serving as canaries for
//! accidental regressions during refactoring or optimization work.

#![cfg(feature = "state_machine")]

use cqlite_core::query::{ColumnInfo, QueryMetadata, QueryResult, QueryRow};
use cqlite_core::types::DataType;
use cqlite_core::{RowKey, Value};
use std::collections::HashMap;

#[cfg(feature = "state_machine")]
use cqlite_cli::config::OutputConfig;
#[cfg(feature = "state_machine")]
use cqlite_cli::output::{CSVWriter, JSONWriter};

// ============================================================================
// Helper Functions
// ============================================================================

/// Default output config for tests (no limit)
#[cfg(feature = "state_machine")]
fn default_config() -> OutputConfig {
    OutputConfig::default()
}

/// Create a QueryResult with specified column order and row data
///
/// This helper allows precise control over:
/// - metadata.columns order (the source of truth for output ordering)
/// - HashMap insertion order (which should NOT affect output)
fn create_result_with_column_order(
    columns: Vec<(&str, DataType)>,
    rows_data: Vec<Vec<(&str, Value)>>,
) -> QueryResult {
    let columns_vec: Vec<ColumnInfo> = columns
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

    let metadata = QueryMetadata {
        columns: columns_vec,
        ..Default::default()
    };

    let rows = rows_data
        .into_iter()
        .enumerate()
        .map(|(idx, row_data)| {
            let mut values = HashMap::new();
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

/// Assert that string positions follow expected order
///
/// Verifies that substrings appear in the specified order within a larger string.
/// This is critical for detecting ordering regressions that JSON/CSV parsers might hide.
fn assert_string_order(haystack: &str, needles: &[&str], context: &str) {
    let mut last_pos = 0;
    for (i, needle) in needles.iter().enumerate() {
        let pos = haystack.find(needle).unwrap_or_else(|| {
            panic!(
                "{context}: Expected to find '{needle}' in output, but it's missing. Full output:\n{haystack}"
            )
        });

        if i > 0 && pos < last_pos {
            panic!(
                "{}: Expected '{}' (at position {}) to appear after '{}' (at position {}), but order is reversed. Full output:\n{}",
                context, needle, pos, needles[i - 1], last_pos, haystack
            );
        }
        last_pos = pos;
    }
}

// ============================================================================
// JSON Ordering Tests
// ============================================================================

#[test]
fn test_json_preserves_non_alphabetical_column_order() {
    // PURPOSE: Verify that columns defined as [z, a, m] appear as [z, a, m] in JSON,
    // NOT as [a, m, z] (alphabetical) or any other order

    let result = create_result_with_column_order(
        vec![
            ("z_field", DataType::Text),
            ("a_field", DataType::Text),
            ("m_field", DataType::Text),
        ],
        vec![vec![
            ("z_field", Value::text("zzz".to_string())),
            ("a_field", Value::text("aaa".to_string())),
            ("m_field", Value::text("mmm".to_string())),
        ]],
    );

    let json_str =
        JSONWriter::write(&result, &default_config()).expect("JSON write should succeed");

    // Parse to verify structure
    let parsed: Vec<serde_json::Value> =
        serde_json::from_str(&json_str).expect("JSON should parse");
    assert_eq!(parsed.len(), 1, "Should have one row");

    let row_obj = parsed[0].as_object().expect("Row should be JSON object");
    let keys: Vec<&String> = row_obj.keys().collect();

    // CRITICAL ASSERTION: Keys must be [z_field, a_field, m_field]
    assert_eq!(
        keys,
        vec!["z_field", "a_field", "m_field"],
        "JSON keys must match metadata.columns order [z, a, m], not alphabetical [a, m, z]"
    );
}

#[test]
fn test_json_key_position_in_string_matches_column_order() {
    // PURPOSE: Verify that the actual string representation has keys in the correct order
    // This catches cases where serde_json might change its serialization behavior

    let result = create_result_with_column_order(
        vec![
            ("zebra", DataType::Integer),
            ("apple", DataType::Integer),
            ("mango", DataType::Integer),
        ],
        vec![vec![
            ("zebra", Value::Integer(3)),
            ("apple", Value::Integer(1)),
            ("mango", Value::Integer(2)),
        ]],
    );

    let json_str =
        JSONWriter::write(&result, &default_config()).expect("JSON write should succeed");

    // Use string position assertions to verify ordering
    assert_string_order(
        &json_str,
        &["\"zebra\"", "\"apple\"", "\"mango\""],
        "JSON key string positions",
    );

    // Additional verification: zebra must appear before apple
    let zebra_pos = json_str.find("\"zebra\"").unwrap();
    let apple_pos = json_str.find("\"apple\"").unwrap();
    let mango_pos = json_str.find("\"mango\"").unwrap();

    assert!(
        zebra_pos < apple_pos,
        "Key 'zebra' at position {zebra_pos} must appear before 'apple' at position {apple_pos}"
    );
    assert!(
        apple_pos < mango_pos,
        "Key 'apple' at position {apple_pos} must appear before 'mango' at position {mango_pos}"
    );
}

#[test]
fn test_json_ordering_with_complex_types() {
    // PURPOSE: Verify that complex types (collections, UDTs) maintain column ordering

    let result = create_result_with_column_order(
        vec![
            ("z_list", DataType::List),
            ("a_int", DataType::Integer),
            ("m_map", DataType::Map),
        ],
        vec![vec![
            (
                "z_list",
                Value::List(vec![Value::Integer(1), Value::Integer(2)]),
            ),
            ("a_int", Value::Integer(42)),
            (
                "m_map",
                Value::Map(vec![(
                    Value::text("key".to_string()),
                    Value::text("val".to_string()),
                )]),
            ),
        ]],
    );

    let json_str =
        JSONWriter::write(&result, &default_config()).expect("JSON write should succeed");

    // Verify key order in string representation
    assert_string_order(
        &json_str,
        &["\"z_list\"", "\"a_int\"", "\"m_map\""],
        "JSON keys with complex types",
    );
}

#[test]
fn test_json_ordering_independent_of_hashmap_insertion() {
    // PURPOSE: Demonstrate that HashMap insertion order does NOT affect output order
    // This is the critical regression test: if we accidentally use HashMap iteration
    // instead of metadata.columns iteration, this test will fail

    let result = create_result_with_column_order(
        vec![
            ("third", DataType::Integer),
            ("first", DataType::Integer),
            ("second", DataType::Integer),
        ],
        vec![
            // Row 1: Insert in alphabetical order
            vec![
                ("first", Value::Integer(1)),
                ("second", Value::Integer(2)),
                ("third", Value::Integer(3)),
            ],
            // Row 2: Insert in reverse order
            vec![
                ("third", Value::Integer(6)),
                ("second", Value::Integer(5)),
                ("first", Value::Integer(4)),
            ],
            // Row 3: Insert in random order
            vec![
                ("second", Value::Integer(8)),
                ("third", Value::Integer(9)),
                ("first", Value::Integer(7)),
            ],
        ],
    );

    let json_str =
        JSONWriter::write(&result, &default_config()).expect("JSON write should succeed");
    let parsed: Vec<serde_json::Value> =
        serde_json::from_str(&json_str).expect("JSON should parse");

    // All three rows must have identical key order
    for (idx, row) in parsed.iter().enumerate() {
        let row_obj = row.as_object().expect("Row should be object");
        let keys: Vec<&String> = row_obj.keys().collect();

        assert_eq!(
            keys,
            vec!["third", "first", "second"],
            "Row {idx} must have keys in metadata order [third, first, second] regardless of HashMap insertion order"
        );
    }
}

#[test]
fn test_json_multiple_rows_maintain_consistent_ordering() {
    // PURPOSE: Verify that ordering is consistent across multiple rows

    let result = create_result_with_column_order(
        vec![
            ("col_c", DataType::Integer),
            ("col_a", DataType::Integer),
            ("col_b", DataType::Integer),
        ],
        vec![
            vec![
                ("col_c", Value::Integer(1)),
                ("col_a", Value::Integer(2)),
                ("col_b", Value::Integer(3)),
            ],
            vec![
                ("col_c", Value::Integer(4)),
                ("col_a", Value::Integer(5)),
                ("col_b", Value::Integer(6)),
            ],
            vec![
                ("col_c", Value::Integer(7)),
                ("col_a", Value::Integer(8)),
                ("col_b", Value::Integer(9)),
            ],
        ],
    );

    let json_str =
        JSONWriter::write(&result, &default_config()).expect("JSON write should succeed");
    let parsed: Vec<serde_json::Value> =
        serde_json::from_str(&json_str).expect("JSON should parse");

    // Verify all rows have same key order
    for (idx, row) in parsed.iter().enumerate() {
        let row_obj = row.as_object().unwrap();
        let keys: Vec<&String> = row_obj.keys().collect();
        assert_eq!(
            keys,
            vec!["col_c", "col_a", "col_b"],
            "Row {idx} key order must match metadata.columns"
        );
    }
}

#[test]
fn test_json_ordering_with_null_and_missing_values() {
    // PURPOSE: Verify that null and missing values don't affect column ordering

    let result = create_result_with_column_order(
        vec![
            ("z_col", DataType::Text),
            ("a_col", DataType::Text),
            ("m_col", DataType::Text),
        ],
        vec![
            // Row with explicit null
            vec![
                ("z_col", Value::text("z1".to_string())),
                ("a_col", Value::Null),
                ("m_col", Value::text("m1".to_string())),
            ],
            // Row with missing column (a_col not in HashMap)
            vec![
                ("z_col", Value::text("z2".to_string())),
                ("m_col", Value::text("m2".to_string())),
            ],
        ],
    );

    let json_str =
        JSONWriter::write(&result, &default_config()).expect("JSON write should succeed");
    let parsed: Vec<serde_json::Value> =
        serde_json::from_str(&json_str).expect("JSON should parse");

    // Both rows must have same key order
    for (idx, row) in parsed.iter().enumerate() {
        let row_obj = row.as_object().unwrap();
        let keys: Vec<&String> = row_obj.keys().collect();
        assert_eq!(
            keys,
            vec!["z_col", "a_col", "m_col"],
            "Row {idx} must maintain column order even with null/missing values"
        );
    }

    // Verify nulls are represented as JSON null
    assert!(parsed[0].get("a_col").unwrap().is_null());
    assert!(parsed[1].get("a_col").unwrap().is_null());
}

#[test]
#[should_panic(expected = "Keys must be in column order")]
fn test_json_regression_detection_wrong_order() {
    // PURPOSE: This test verifies that our assertions WILL catch ordering regressions
    // If this test passes (doesn't panic), it means our assertions are too weak

    let result = create_result_with_column_order(
        vec![
            ("c", DataType::Integer),
            ("b", DataType::Integer),
            ("a", DataType::Integer),
        ],
        vec![vec![
            ("c", Value::Integer(3)),
            ("b", Value::Integer(2)),
            ("a", Value::Integer(1)),
        ]],
    );

    let json_str = JSONWriter::write(&result, &default_config()).unwrap();
    let parsed: Vec<serde_json::Value> = serde_json::from_str(&json_str).unwrap();
    let row_obj = parsed[0].as_object().unwrap();
    let keys: Vec<&String> = row_obj.keys().collect();

    // This should panic because we're asserting the WRONG order
    // If it doesn't panic, our test is not sensitive enough
    assert_eq!(keys, vec!["a", "b", "c"], "Keys must be in column order");
}

// ============================================================================
// CSV Ordering Tests
// ============================================================================

#[test]
fn test_csv_header_order_matches_metadata_columns() {
    // PURPOSE: Verify CSV headers follow metadata.columns order exactly

    let result = create_result_with_column_order(
        vec![
            ("z_field", DataType::Text),
            ("a_field", DataType::Text),
            ("m_field", DataType::Text),
        ],
        vec![],
    );

    let csv = CSVWriter::write(&result, &default_config()).expect("CSV write should succeed");
    let lines: Vec<&str> = csv.lines().collect();

    assert_eq!(lines.len(), 1, "Should have header only");
    assert_eq!(
        lines[0], "z_field,a_field,m_field",
        "CSV header must match metadata.columns order [z, a, m], not alphabetical"
    );
}

#[test]
fn test_csv_data_order_independent_of_hashmap_insertion() {
    // PURPOSE: Verify that CSV column order is independent of HashMap insertion order
    // This is the critical CSV regression test

    let result = create_result_with_column_order(
        vec![
            ("third", DataType::Integer),
            ("first", DataType::Integer),
            ("second", DataType::Integer),
        ],
        vec![
            // Insert in different orders for each row
            vec![
                ("first", Value::Integer(1)),
                ("second", Value::Integer(2)),
                ("third", Value::Integer(3)),
            ],
            vec![
                ("third", Value::Integer(6)),
                ("second", Value::Integer(5)),
                ("first", Value::Integer(4)),
            ],
            vec![
                ("second", Value::Integer(8)),
                ("third", Value::Integer(9)),
                ("first", Value::Integer(7)),
            ],
        ],
    );

    let csv = CSVWriter::write(&result, &default_config()).expect("CSV write should succeed");
    let lines: Vec<&str> = csv.lines().collect();

    assert_eq!(lines.len(), 4, "Should have header + 3 data rows");

    // Header must match metadata order
    assert_eq!(lines[0], "third,first,second");

    // Data rows must follow header order regardless of HashMap insertion
    assert_eq!(lines[1], "3,1,2", "Row 1 data must follow header order");
    assert_eq!(lines[2], "6,4,5", "Row 2 data must follow header order");
    assert_eq!(lines[3], "9,7,8", "Row 3 data must follow header order");
}

#[test]
fn test_csv_ordering_with_null_and_missing_values() {
    // PURPOSE: Verify null and missing values maintain correct column positions

    let result = create_result_with_column_order(
        vec![
            ("z_col", DataType::Integer),
            ("a_col", DataType::Integer),
            ("m_col", DataType::Integer),
        ],
        vec![
            // Row with explicit null in middle
            vec![
                ("z_col", Value::Integer(1)),
                ("a_col", Value::Null),
                ("m_col", Value::Integer(3)),
            ],
            // Row with missing column (a_col not present)
            vec![("z_col", Value::Integer(4)), ("m_col", Value::Integer(6))],
            // Row with null at start
            vec![
                ("z_col", Value::Null),
                ("a_col", Value::Integer(5)),
                ("m_col", Value::Integer(6)),
            ],
        ],
    );

    let csv = CSVWriter::write(&result, &default_config()).expect("CSV write should succeed");
    let lines: Vec<&str> = csv.lines().collect();

    assert_eq!(lines.len(), 4, "Should have header + 3 data rows");
    assert_eq!(lines[0], "z_col,a_col,m_col");

    // Null and missing values should be empty, but maintain position
    assert_eq!(lines[1], "1,,3", "Row 1: null in middle position");
    assert_eq!(lines[2], "4,,6", "Row 2: missing value in middle position");
    assert_eq!(lines[3], ",5,6", "Row 3: null at start position");
}

#[test]
fn test_csv_ordering_remains_stable_across_result_sets() {
    // PURPOSE: Verify that column order is consistent when processing multiple result sets

    // Create two separate result sets with same column definitions but different data
    let result1 = create_result_with_column_order(
        vec![
            ("col_3", DataType::Text),
            ("col_1", DataType::Text),
            ("col_2", DataType::Text),
        ],
        vec![vec![
            ("col_3", Value::text("c".to_string())),
            ("col_1", Value::text("a".to_string())),
            ("col_2", Value::text("b".to_string())),
        ]],
    );

    let result2 = create_result_with_column_order(
        vec![
            ("col_3", DataType::Text),
            ("col_1", DataType::Text),
            ("col_2", DataType::Text),
        ],
        vec![vec![
            ("col_1", Value::text("x".to_string())),
            ("col_2", Value::text("y".to_string())),
            ("col_3", Value::text("z".to_string())),
        ]],
    );

    let csv1 = CSVWriter::write(&result1, &default_config()).expect("CSV write 1 should succeed");
    let csv2 = CSVWriter::write(&result2, &default_config()).expect("CSV write 2 should succeed");

    let lines1: Vec<&str> = csv1.lines().collect();
    let lines2: Vec<&str> = csv2.lines().collect();

    // Both should have identical header order
    assert_eq!(lines1[0], lines2[0], "Headers must be identical");
    assert_eq!(lines1[0], "col_3,col_1,col_2");

    // Data should follow header order
    assert_eq!(lines1[1], "c,a,b");
    assert_eq!(lines2[1], "z,x,y");
}

#[test]
fn test_csv_ordering_with_complex_types() {
    // PURPOSE: Verify complex types maintain column ordering

    let result = create_result_with_column_order(
        vec![
            ("z_list", DataType::List),
            ("a_int", DataType::Integer),
            ("m_set", DataType::Set),
        ],
        vec![vec![
            (
                "z_list",
                Value::List(vec![Value::Integer(1), Value::Integer(2)]),
            ),
            ("a_int", Value::Integer(42)),
            (
                "m_set",
                Value::Set(vec![
                    Value::text("x".to_string()),
                    Value::text("y".to_string()),
                ]),
            ),
        ]],
    );

    let csv = CSVWriter::write(&result, &default_config()).expect("CSV write should succeed");
    let lines: Vec<&str> = csv.lines().collect();

    assert_eq!(lines[0], "z_list,a_int,m_set");
    // Verify data row has values in correct positions
    assert!(lines[1].starts_with("[1, 2]") || lines[1].starts_with("\"[1, 2]\""));
}

#[test]
#[should_panic(expected = "CSV header must match")]
fn test_csv_regression_detection_wrong_header_order() {
    // PURPOSE: Verify that our CSV assertions will catch ordering regressions
    // If this test doesn't panic, our assertions are too weak

    let result = create_result_with_column_order(
        vec![
            ("c", DataType::Integer),
            ("b", DataType::Integer),
            ("a", DataType::Integer),
        ],
        vec![],
    );

    let csv = CSVWriter::write(&result, &default_config()).unwrap();
    let lines: Vec<&str> = csv.lines().collect();

    // This should panic because we're asserting the WRONG order
    assert_eq!(lines[0], "a,b,c", "CSV header must match metadata order");
}

#[test]
fn test_csv_multiple_rows_consistent_column_positions() {
    // PURPOSE: Verify that each row's values appear in the same column positions

    let result = create_result_with_column_order(
        vec![
            ("zebra", DataType::Integer),
            ("apple", DataType::Integer),
            ("mango", DataType::Integer),
        ],
        vec![
            vec![
                ("zebra", Value::Integer(1)),
                ("apple", Value::Integer(2)),
                ("mango", Value::Integer(3)),
            ],
            vec![
                ("zebra", Value::Integer(4)),
                ("apple", Value::Integer(5)),
                ("mango", Value::Integer(6)),
            ],
            vec![
                ("zebra", Value::Integer(7)),
                ("apple", Value::Integer(8)),
                ("mango", Value::Integer(9)),
            ],
        ],
    );

    let csv = CSVWriter::write(&result, &default_config()).expect("CSV write should succeed");
    let lines: Vec<&str> = csv.lines().collect();

    assert_eq!(lines.len(), 4);
    assert_eq!(lines[0], "zebra,apple,mango");

    // Each data row must have values in header order
    for (idx, expected) in [(1, "1,2,3"), (2, "4,5,6"), (3, "7,8,9")].iter() {
        assert_eq!(
            lines[*idx], *expected,
            "Row {idx} values must be in zebra,apple,mango order"
        );
    }
}

// ============================================================================
// Cross-Format Consistency Tests
// ============================================================================

#[test]
fn test_json_and_csv_have_consistent_column_order() {
    // PURPOSE: Verify that JSON and CSV produce the same column order for the same result

    let result = create_result_with_column_order(
        vec![
            ("z_col", DataType::Text),
            ("a_col", DataType::Text),
            ("m_col", DataType::Text),
        ],
        vec![vec![
            ("z_col", Value::text("z".to_string())),
            ("a_col", Value::text("a".to_string())),
            ("m_col", Value::text("m".to_string())),
        ]],
    );

    // Get JSON order
    let json_str =
        JSONWriter::write(&result, &default_config()).expect("JSON write should succeed");
    let parsed: Vec<serde_json::Value> =
        serde_json::from_str(&json_str).expect("JSON should parse");
    let json_keys: Vec<&String> = parsed[0].as_object().unwrap().keys().collect();

    // Get CSV order
    let csv = CSVWriter::write(&result, &default_config()).expect("CSV write should succeed");
    let csv_header = csv.lines().next().unwrap();
    let csv_columns: Vec<&str> = csv_header.split(',').collect();

    // Both should match metadata order
    assert_eq!(json_keys, vec!["z_col", "a_col", "m_col"]);
    assert_eq!(csv_columns, vec!["z_col", "a_col", "m_col"]);

    // JSON and CSV column orders must be identical
    for (json_key, csv_col) in json_keys.iter().zip(csv_columns.iter()) {
        assert_eq!(
            json_key.as_str(),
            *csv_col,
            "JSON and CSV column order must match"
        );
    }
}

#[test]
fn test_ordering_with_single_column() {
    // PURPOSE: Edge case - verify ordering works with single column

    let result = create_result_with_column_order(
        vec![("only_col", DataType::Integer)],
        vec![vec![("only_col", Value::Integer(42))]],
    );

    // JSON
    let json_str =
        JSONWriter::write(&result, &default_config()).expect("JSON write should succeed");
    let parsed: Vec<serde_json::Value> =
        serde_json::from_str(&json_str).expect("JSON should parse");
    let keys: Vec<&String> = parsed[0].as_object().unwrap().keys().collect();
    assert_eq!(keys, vec!["only_col"]);

    // CSV
    let csv = CSVWriter::write(&result, &default_config()).expect("CSV write should succeed");
    let lines: Vec<&str> = csv.lines().collect();
    assert_eq!(lines[0], "only_col");
    assert_eq!(lines[1], "42");
}

#[test]
fn test_ordering_with_many_columns() {
    // PURPOSE: Stress test with many columns to ensure ordering is stable at scale

    let columns: Vec<(&str, DataType)> = vec![
        ("col_10", DataType::Integer),
        ("col_01", DataType::Integer),
        ("col_05", DataType::Integer),
        ("col_03", DataType::Integer),
        ("col_08", DataType::Integer),
        ("col_02", DataType::Integer),
        ("col_07", DataType::Integer),
        ("col_04", DataType::Integer),
        ("col_09", DataType::Integer),
        ("col_06", DataType::Integer),
    ];

    let row_data: Vec<(&str, Value)> = columns
        .iter()
        .enumerate()
        .map(|(i, (name, _))| (*name, Value::Integer(i as i32)))
        .collect();

    let result = create_result_with_column_order(columns, vec![row_data]);

    // JSON ordering
    let json_str =
        JSONWriter::write(&result, &default_config()).expect("JSON write should succeed");
    let parsed: Vec<serde_json::Value> =
        serde_json::from_str(&json_str).expect("JSON should parse");
    let json_keys: Vec<&String> = parsed[0].as_object().unwrap().keys().collect();

    let expected_order = vec![
        "col_10", "col_01", "col_05", "col_03", "col_08", "col_02", "col_07", "col_04", "col_09",
        "col_06",
    ];
    assert_eq!(json_keys, expected_order);

    // CSV ordering
    let csv = CSVWriter::write(&result, &default_config()).expect("CSV write should succeed");
    let header = csv.lines().next().unwrap();
    assert_eq!(header, expected_order.join(","));
}
