//! Issue #702 (review fixup): CI-guarded unit tests for the `is_static` bitmap
//! filtering logic introduced in `v5_compressed_legacy.rs`.
//!
//! ## What this tests
//!
//! The `columns_in_order` filtering at the core of the row-cell parsing path
//! (v5_compressed_legacy.rs ~3310-3343) filters `ColumnInfo` entries from the
//! serialization header to include only the column group that matches the current
//! row kind (`is_static`).  This is critical for tables that have BOTH static and
//! regular columns: including the wrong group shifts all bitmap indices and causes
//! cells to be misread or dropped.
//!
//! These tests reproduce that logic with synthetic `ColumnInfo` / `Column` data
//! so they run in CI **without** any binary SSTable data.
//!
//! ## Why this matters
//!
//! Before issue #702 the bitmap index was computed over ALL non-key columns,
//! mixing static and regular columns.  For a table with 1 static column + 2
//! regular columns, the static row's bitmap was size-3 but only 1 column was
//! static, so bit-0 mapped to the static column correctly — but when Cassandra
//! encoded a regular row's bitmap it was relative to the 2 regular columns only.
//! Using a combined list caused bit-0 to hit the STATIC column instead of the
//! first REGULAR column, silently dropping the regular column's data.

use cqlite_core::parser::ColumnInfo;
use cqlite_core::schema::{ClusteringColumn, KeyColumn};
use cqlite_core::schema::{Column, TableSchema};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Helpers — mirror the filtering logic from v5_compressed_legacy.rs
// ---------------------------------------------------------------------------

/// Simulate the `columns_in_order` build-and-filter logic from the row parser:
///   1. Build a schema-column lookup map.
///   2. Iterate serialization-header columns, keep only those where
///      `!is_primary_key && !is_clustering && col.is_static == row_is_static`.
///   3. Look up each surviving entry in the schema map.
///
/// Returns the ordered list of schema `Column` references that should be parsed.
fn columns_in_order_for_row<'schema>(
    header_cols: &[ColumnInfo],
    schema: &'schema TableSchema,
    row_is_static: bool,
) -> Vec<&'schema Column> {
    let schema_map: HashMap<&str, &Column> = schema
        .columns
        .iter()
        .map(|c| (c.name.as_str(), c))
        .collect();

    header_cols
        .iter()
        .filter(|c| !c.is_primary_key && !c.is_clustering && c.is_static == row_is_static)
        .filter_map(|c| schema_map.get(c.name.as_str()).copied())
        .collect()
}

/// Apply a `missing_columns_bitmap` to a column list.
/// Bit `i` set → column `i` is ABSENT.
/// Columns at index >= 64 are always included.
fn apply_bitmap(columns: Vec<&Column>, bitmap: u64) -> Vec<&Column> {
    columns
        .into_iter()
        .enumerate()
        .filter(|(idx, _)| *idx >= 64 || (bitmap & (1u64 << idx)) == 0)
        .map(|(_, col)| col)
        .collect()
}

// ---------------------------------------------------------------------------
// Synthetic schema factory
// ---------------------------------------------------------------------------

/// Build a schema with 1 static column (`static_col`) and 2 regular columns
/// (`reg_a`, `reg_b`), plus pk and ck keys.
fn mixed_static_schema() -> TableSchema {
    TableSchema {
        keyspace: "test".to_string(),
        table: "mixed".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: cqlite_core::schema::ClusteringOrder::Asc,
        }],
        columns: vec![
            Column {
                name: "static_col".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: true,
            },
            Column {
                name: "reg_a".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "reg_b".to_string(),
                data_type: "int".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// Serialization-header columns for `mixed_static_schema`, in Cassandra's
/// serialization order (alphabetical by name within each kind, keys first).
fn mixed_static_header_cols() -> Vec<ColumnInfo> {
    vec![
        // pk (primary key)
        ColumnInfo {
            name: "pk".to_string(),
            column_type: "int".to_string(),
            is_primary_key: true,
            key_position: Some(0),
            is_static: false,
            is_clustering: false,
            clustering_reversed: false,
        },
        // ck (clustering)
        ColumnInfo {
            name: "ck".to_string(),
            column_type: "int".to_string(),
            is_primary_key: false,
            key_position: None,
            is_static: false,
            is_clustering: true,
            clustering_reversed: false,
        },
        // static_col (static)
        ColumnInfo {
            name: "static_col".to_string(),
            column_type: "text".to_string(),
            is_primary_key: false,
            key_position: None,
            is_static: true,
            is_clustering: false,
            clustering_reversed: false,
        },
        // reg_a (regular)
        ColumnInfo {
            name: "reg_a".to_string(),
            column_type: "text".to_string(),
            is_primary_key: false,
            key_position: None,
            is_static: false,
            is_clustering: false,
            clustering_reversed: false,
        },
        // reg_b (regular)
        ColumnInfo {
            name: "reg_b".to_string(),
            column_type: "int".to_string(),
            is_primary_key: false,
            key_position: None,
            is_static: false,
            is_clustering: false,
            clustering_reversed: false,
        },
    ]
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// A regular row (is_static=false) must see only regular columns, NOT the
/// static column, so that its missing_columns_bitmap indices are correct.
#[test]
fn regular_row_sees_only_regular_columns() {
    let schema = mixed_static_schema();
    let header = mixed_static_header_cols();

    let cols = columns_in_order_for_row(&header, &schema, false);

    assert_eq!(
        cols.len(),
        2,
        "regular row must see exactly 2 regular columns, not {:?}",
        cols.iter().map(|c| &c.name).collect::<Vec<_>>()
    );
    assert_eq!(cols[0].name, "reg_a", "first regular column must be reg_a");
    assert_eq!(cols[1].name, "reg_b", "second regular column must be reg_b");
    // The static column must NOT appear.
    assert!(
        cols.iter().all(|c| !c.is_static),
        "no static column should appear in a regular-row column list"
    );
}

/// A static row (is_static=true) must see only static columns.
#[test]
fn static_row_sees_only_static_columns() {
    let schema = mixed_static_schema();
    let header = mixed_static_header_cols();

    let cols = columns_in_order_for_row(&header, &schema, true);

    assert_eq!(
        cols.len(),
        1,
        "static row must see exactly 1 static column, not {:?}",
        cols.iter().map(|c| &c.name).collect::<Vec<_>>()
    );
    assert_eq!(cols[0].name, "static_col");
    assert!(cols[0].is_static, "static_col must be marked is_static");
}

/// When Cassandra sets bit-0 of the regular-row bitmap the FIRST regular column
/// is absent.  With the old (pre-#702) logic that merged static+regular columns,
/// the static column occupied index-0 and setting bit-0 would wrongly drop the
/// static column from the mixed list — but since the static column is also
/// excluded from a regular row, the net effect was that bit-0 hit `reg_a` on
/// the old path only if static was listed first.  The fix ensures bit-0 ALWAYS
/// maps to the first REGULAR column for a regular row.
#[test]
fn bitmap_bit0_drops_first_regular_column_not_static() {
    let schema = mixed_static_schema();
    let header = mixed_static_header_cols();

    // Regular-row column list: [reg_a, reg_b]
    let cols = columns_in_order_for_row(&header, &schema, false);
    assert_eq!(cols.len(), 2);

    // Bitmap: bit-0 set → reg_a is absent.
    let present = apply_bitmap(cols, 0b01);

    assert_eq!(
        present.len(),
        1,
        "only 1 column should survive bitmap 0b01 on a 2-column regular list"
    );
    assert_eq!(
        present[0].name, "reg_b",
        "with bit-0 set, reg_a should be absent and reg_b should remain"
    );
}

/// Bit-1 drops the second regular column (`reg_b`).
#[test]
fn bitmap_bit1_drops_second_regular_column() {
    let schema = mixed_static_schema();
    let header = mixed_static_header_cols();

    let cols = columns_in_order_for_row(&header, &schema, false);
    let present = apply_bitmap(cols, 0b10);

    assert_eq!(present.len(), 1);
    assert_eq!(present[0].name, "reg_a");
}

/// All-columns-absent bitmap (0b11 for 2 columns) yields an empty list.
#[test]
fn bitmap_all_absent_yields_empty() {
    let schema = mixed_static_schema();
    let header = mixed_static_header_cols();

    let cols = columns_in_order_for_row(&header, &schema, false);
    let present = apply_bitmap(cols, 0b11);

    assert!(
        present.is_empty(),
        "all-bits-set bitmap should yield zero columns"
    );
}

/// A schema with ONLY regular columns (no static) should behave identically to
/// pre-#702 behaviour: all non-key columns appear in a regular row.
#[test]
fn pure_regular_schema_unaffected_by_static_filter() {
    let schema = TableSchema {
        keyspace: "test".to_string(),
        table: "no_static".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "col_a".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "col_b".to_string(),
                data_type: "int".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let header = vec![
        ColumnInfo {
            name: "pk".to_string(),
            column_type: "int".to_string(),
            is_primary_key: true,
            key_position: Some(0),
            is_static: false,
            is_clustering: false,
            clustering_reversed: false,
        },
        ColumnInfo {
            name: "col_a".to_string(),
            column_type: "text".to_string(),
            is_primary_key: false,
            key_position: None,
            is_static: false,
            is_clustering: false,
            clustering_reversed: false,
        },
        ColumnInfo {
            name: "col_b".to_string(),
            column_type: "int".to_string(),
            is_primary_key: false,
            key_position: None,
            is_static: false,
            is_clustering: false,
            clustering_reversed: false,
        },
    ];

    let cols = columns_in_order_for_row(&header, &schema, false);
    assert_eq!(cols.len(), 2, "all regular columns must appear");
    assert_eq!(cols[0].name, "col_a");
    assert_eq!(cols[1].name, "col_b");
}
