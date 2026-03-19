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
#[path = "write_read_roundtrip/index.rs"]
mod index;
#[path = "write_read_roundtrip/statistics.rs"]
mod statistics;
#[path = "write_read_roundtrip/summary.rs"]
mod summary;
#[path = "write_read_roundtrip/type_coverage.rs"]
mod type_coverage;

use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use std::collections::HashMap;
use std::path::Path;
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

/// Create a comprehensive schema with all Stage 0 supported types
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
                name: "text_col".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "int_col".to_string(),
                data_type: "int".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "bigint_col".to_string(),
                data_type: "bigint".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "boolean_col".to_string(),
                data_type: "boolean".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "timestamp_col".to_string(),
                data_type: "timestamp".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "uuid_col".to_string(),
                data_type: "uuid".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "tinyint_col".to_string(),
                data_type: "tinyint".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "smallint_col".to_string(),
                data_type: "smallint".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "float_col".to_string(),
                data_type: "float".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "double_col".to_string(),
                data_type: "double".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "blob_col".to_string(),
                data_type: "blob".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "date_col".to_string(),
                data_type: "date".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "time_col".to_string(),
                data_type: "time".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "inet_col".to_string(),
                data_type: "inet".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "varint_col".to_string(),
                data_type: "varint".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "decimal_col".to_string(),
                data_type: "decimal".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "duration_col".to_string(),
                data_type: "duration".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "tuple_col".to_string(),
                data_type: "tuple<int, text>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "frozen_col".to_string(),
                data_type: "frozen<list<int>>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
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
            value: Value::Blob(vec![0xDE, 0xAD, pk as u8]),
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
            value: Value::Inet(vec![192, 168, 1, pk as u8]),
        },
        CellOperation::Write {
            column: "varint_col".to_string(),
            value: Value::Varint(vec![pk as u8]),
        },
        CellOperation::Write {
            column: "decimal_col".to_string(),
            value: Value::Decimal {
                scale: 2,
                unscaled: vec![pk as u8],
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
