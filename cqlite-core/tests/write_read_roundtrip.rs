//! Write-Read Roundtrip Tests for CQLite M5 Write Support
//!
//! This module contains comprehensive TDD validation tests that verify
//! the writer produces files that the reader can correctly parse.
//!
//! ## Test Structure
//!
//! Tests are organized by SSTable component:
//! - `statistics.rs` - Statistics.db roundtrip
//! - `index.rs` - Index.db roundtrip
//! - `filter.rs` - Filter.db roundtrip (Bloom filter)
//! - `summary.rs` - Summary.db roundtrip
//! - `data_single.rs` - Single partition Data.db roundtrip
//! - `data_multi.rs` - Multi partition Data.db roundtrip
//! - `type_coverage.rs` - All CQL type roundtrips
//! - `edge_cases.rs` - Edge cases and boundary conditions
//!
//! ## Feature Gate
//!
//! All tests require the `write-support` feature:
//! ```bash
//! cargo test --package cqlite-core --features write-support
//! ```
//!
//! ## TDD Pattern
//!
//! Tests follow the TDD pattern:
//! 1. Start with `#[ignore]` attribute
//! 2. Remove `#[ignore]` when test passes
//! 3. Document specific bugs in comments if test fails

#![cfg(feature = "write-support")]

#[path = "write_read_roundtrip/data_multi.rs"]
mod data_multi;
#[path = "write_read_roundtrip/data_single.rs"]
mod data_single;
#[path = "write_read_roundtrip/edge_cases.rs"]
mod edge_cases;
#[path = "write_read_roundtrip/filter.rs"]
mod filter;
#[path = "write_read_roundtrip/full_roundtrip.rs"]
mod full_roundtrip;
#[path = "write_read_roundtrip/index.rs"]
mod index;
#[path = "write_read_roundtrip/statistics.rs"]
mod statistics;
#[path = "write_read_roundtrip/summary.rs"]
mod summary;
#[path = "write_read_roundtrip/type_coverage.rs"]
mod type_coverage;

use cqlite_core::platform::Platform;
use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::SSTableManager;
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use cqlite_core::Config;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;

/// Create a simple test schema with partition key and two columns
pub fn create_simple_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_roundtrip".to_string(),
        table: "simple".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "value".to_string(),
                data_type: "int".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
    }
}

/// Create a schema with clustering key for testing wide partitions
pub fn create_clustering_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_roundtrip".to_string(),
        table: "clustered".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "text".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "ck".to_string(),
                data_type: "text".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "data".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
    }
}

/// Create a nullable column with standard defaults
fn col(name: &str, data_type: &str) -> Column {
    Column {
        name: name.to_string(),
        data_type: data_type.to_string(),
        nullable: true,
        default: None,
        is_static: false,
    }
}

/// Create a non-nullable column (for primary key components)
fn key_col(name: &str, data_type: &str) -> Column {
    Column {
        name: name.to_string(),
        data_type: data_type.to_string(),
        nullable: false,
        default: None,
        is_static: false,
    }
}

/// Create a comprehensive schema with all supported types
pub fn create_comprehensive_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_roundtrip".to_string(),
        table: "all_types".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "text".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![
            key_col("pk", "int"),
            key_col("ck", "text"),
            col("text_col", "text"),
            col("int_col", "int"),
            col("bigint_col", "bigint"),
            col("boolean_col", "boolean"),
            col("timestamp_col", "timestamp"),
            col("uuid_col", "uuid"),
            col("tinyint_col", "tinyint"),
            col("smallint_col", "smallint"),
            col("float_col", "float"),
            col("double_col", "double"),
            col("blob_col", "blob"),
            col("date_col", "date"),
            col("time_col", "time"),
            col("inet_col", "inet"),
            col("varint_col", "varint"),
            col("decimal_col", "decimal"),
            col("duration_col", "duration"),
            col("tuple_col", "tuple<int, text>"),
            col("frozen_col", "frozen<list<int>>"),
        ],
        comments: HashMap::new(),
    }
}

/// Create a simple mutation
pub fn create_simple_mutation(id: i32, name: &str, value: i32, timestamp: i64) -> Mutation {
    let table_id = TableId::new("test_roundtrip", "simple");
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![
        CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text(name.to_string()),
        },
        CellOperation::Write {
            column: "value".to_string(),
            value: Value::Integer(value),
        },
    ];

    Mutation::new(table_id, pk, None, ops, timestamp, None)
}

/// Create a mutation with clustering key
pub fn create_clustered_mutation(pk: i32, ck: &str, data: &str, timestamp: i64) -> Mutation {
    let table_id = TableId::new("test_roundtrip", "clustered");
    let partition_key = PartitionKey::single("pk", Value::Integer(pk));
    let clustering_key = Some(ClusteringKey::single("ck", Value::Text(ck.to_string())));
    let ops = vec![CellOperation::Write {
        column: "data".to_string(),
        value: Value::Text(data.to_string()),
    }];

    Mutation::new(
        table_id,
        partition_key,
        clustering_key,
        ops,
        timestamp,
        None,
    )
}

/// Create a comprehensive mutation with all supported types
pub fn create_comprehensive_mutation(pk: i32, ck: &str, timestamp: i64) -> Mutation {
    let table_id = TableId::new("test_roundtrip", "all_types");
    let partition_key = PartitionKey::single("pk", Value::Integer(pk));
    let clustering_key = Some(ClusteringKey::single("ck", Value::Text(ck.to_string())));

    let ops = vec![
        CellOperation::Write {
            column: "text_col".to_string(),
            value: Value::Text(format!("Text for {}-{}", pk, ck)),
        },
        CellOperation::Write {
            column: "int_col".to_string(),
            value: Value::Integer(pk * 100),
        },
        CellOperation::Write {
            column: "bigint_col".to_string(),
            value: Value::BigInt((pk as i64) * 1_000_000),
        },
        CellOperation::Write {
            column: "boolean_col".to_string(),
            value: Value::Boolean(pk % 2 == 0),
        },
        CellOperation::Write {
            column: "timestamp_col".to_string(),
            value: Value::Timestamp(timestamp),
        },
        CellOperation::Write {
            column: "uuid_col".to_string(),
            value: Value::Uuid(*uuid::Uuid::new_v4().as_bytes()),
        },
        CellOperation::Write {
            column: "tinyint_col".to_string(),
            value: Value::TinyInt((pk % 128) as i8),
        },
        CellOperation::Write {
            column: "smallint_col".to_string(),
            value: Value::SmallInt((pk * 10) as i16),
        },
        CellOperation::Write {
            column: "float_col".to_string(),
            value: Value::Float32(pk as f32 * 1.5),
        },
        CellOperation::Write {
            column: "double_col".to_string(),
            value: Value::Float(pk as f64 * 2.5),
        },
        CellOperation::Write {
            column: "blob_col".to_string(),
            value: Value::Blob(vec![0xDE, 0xAD, (pk & 0xFF) as u8]),
        },
        CellOperation::Write {
            column: "date_col".to_string(),
            value: Value::Date(19723 + pk),
        },
        CellOperation::Write {
            column: "time_col".to_string(),
            value: Value::Time(43_200_000_000_000 + pk as i64),
        },
        CellOperation::Write {
            column: "inet_col".to_string(),
            value: Value::Inet(vec![192, 168, 1, (pk & 0xFF) as u8]),
        },
        CellOperation::Write {
            column: "varint_col".to_string(),
            value: Value::Varint(vec![(pk & 0xFF) as u8]),
        },
        CellOperation::Write {
            column: "decimal_col".to_string(),
            value: Value::Decimal {
                scale: 2,
                unscaled: vec![(pk & 0xFF) as u8],
            },
        },
        CellOperation::Write {
            column: "duration_col".to_string(),
            value: Value::Duration {
                months: pk,
                days: pk * 2,
                nanos: pk as i64 * 1_000_000_000,
            },
        },
        CellOperation::Write {
            column: "tuple_col".to_string(),
            value: Value::Tuple(vec![
                Value::Integer(pk),
                Value::Text(format!("tuple_{}", pk)),
            ]),
        },
        CellOperation::Write {
            column: "frozen_col".to_string(),
            value: Value::Frozen(Box::new(Value::List(vec![
                Value::Integer(pk),
                Value::Integer(pk * 2),
            ]))),
        },
    ];

    Mutation::new(
        table_id,
        partition_key,
        clustering_key,
        ops,
        timestamp,
        None,
    )
}

/// Helper to create write engine with temp directories
pub fn create_test_engine(
    temp_dir: &TempDir,
    schema: TableSchema,
) -> cqlite_core::error::Result<WriteEngine> {
    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );
    WriteEngine::new(config)
}

/// Helper to verify a file exists and is non-empty
pub fn assert_file_exists_and_nonempty(path: &Path, component: &str) {
    assert!(
        path.exists(),
        "{} should exist at {}",
        component,
        path.display()
    );
    let metadata = std::fs::metadata(path).expect("Should read file metadata");
    assert!(
        metadata.len() > 0,
        "{} should be non-empty (got {} bytes)",
        component,
        metadata.len()
    );
}

/// Helper to read file contents
pub fn read_file_bytes(path: &Path) -> Vec<u8> {
    std::fs::read(path).unwrap_or_else(|_| panic!("Should read file: {}", path.display()))
}

/// Read back the raw row value from a flushed SSTable.
///
/// Opens SSTableManager on the data directory, scans the table,
/// and returns the raw Value for the first (and only) row.
/// Panics if the scan returns anything other than exactly 1 row.
pub async fn read_back_raw_row(temp_dir: &TempDir, schema: &TableSchema) -> Value {
    let mut rows = read_back_all_rows(temp_dir, schema).await;
    assert_eq!(
        rows.len(),
        1,
        "Expected exactly 1 row in {}.{}, got {}",
        schema.keyspace,
        schema.table,
        rows.len()
    );
    rows.remove(0)
}

/// Read back all raw row values from a flushed SSTable.
///
/// Opens SSTableManager on the data directory, scans the table,
/// and returns the raw Value for every row found.
/// Useful when testing multi-row partitions (e.g., clustering key tests).
pub async fn read_back_all_rows(temp_dir: &TempDir, schema: &TableSchema) -> Vec<Value> {
    let data_dir = temp_dir.path().join("data");
    let config = Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("Platform::new should succeed in test environment"),
    );

    let manager = SSTableManager::new(
        &data_dir,
        &config,
        platform,
        #[cfg(feature = "state_machine")]
        None,
    )
    .await
    .expect("SSTableManager should load written SSTables");

    let table_id =
        cqlite_core::types::TableId::from(format!("{}.{}", schema.keyspace, schema.table).as_str());
    let results = manager
        .scan(&table_id, None, None, None, Some(schema))
        .await
        .expect("Scan should succeed");

    results.into_iter().map(|(_key, value)| value).collect()
}

/// Read back a single column value from a flushed SSTable.
///
/// Opens SSTableManager on the data directory, scans the table,
/// and extracts the named column from the first (and only) row.
/// The row is expected to be Value::Map(Vec<(Text(col_name), value)>).
/// Panics if the row is not a Map (use `read_back_raw_row` for types
/// where the reader may return a non-Map value).
pub async fn read_back_column(temp_dir: &TempDir, schema: &TableSchema, col_name: &str) -> Value {
    let row_value = read_back_raw_row(temp_dir, schema).await;

    // Row is Value::Map(Vec<(Value::Text(col_name), value)>)
    match &row_value {
        Value::Map(entries) => {
            for (key, value) in entries {
                if let Value::Text(name) = key {
                    if name == col_name {
                        return value.clone();
                    }
                }
            }
            panic!(
                "Column '{}' not found in row. Available columns: {:?}",
                col_name,
                entries
                    .iter()
                    .filter_map(|(k, _)| {
                        if let Value::Text(n) = k {
                            Some(n.as_str())
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
            );
        }
        other => panic!(
            "Expected row to be Value::Map, got {:?}",
            std::mem::discriminant(other)
        ),
    }
}
