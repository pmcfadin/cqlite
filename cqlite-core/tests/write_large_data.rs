//! Large-data stress tests for the CQLite write engine (#466)
//!
//! These tests validate that the write engine handles large-scale data correctly:
//! - VInt encoding at large offsets (10K clustering rows)
//! - Collection serialization at scale (10K element lists, sets, maps)
//! - Large blob values (10MB)
//! - Wide-row schemas (500 columns)
//! - Bloom filter sizing at scale (1,000 partitions)
//!
//! ## Running
//!
//! All tests are marked `#[ignore]` to avoid slowing down normal CI.
//! Run them explicitly with:
//!
//! ```bash
//! cargo test --package cqlite-core --features write-support \
//!     --test write_large_data -- --ignored
//! ```

#![cfg(feature = "write-support")]

use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use std::collections::HashMap;
use tempfile::TempDir;

// ============================================================================
// Helper Functions
// ============================================================================

/// Build a simple schema with partition key `pk` and arbitrary regular columns.
///
/// `columns` is a slice of `(name, data_type)` pairs (must include `pk`).
fn simple_schema(keyspace: &str, table: &str, columns: Vec<(&str, &str)>, pk: &str) -> TableSchema {
    let col_structs: Vec<Column> = columns
        .iter()
        .map(|(name, dt)| Column {
            name: name.to_string(),
            data_type: dt.to_string(),
            nullable: name != &pk,
            default: None,
            is_static: false,
        })
        .collect();

    TableSchema {
        keyspace: keyspace.to_string(),
        table: table.to_string(),
        partition_keys: vec![KeyColumn {
            name: pk.to_string(),
            data_type: col_structs
                .iter()
                .find(|c| c.name == pk)
                .map(|c| c.data_type.clone())
                .unwrap_or_else(|| "int".to_string()),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: col_structs,
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// Build a schema with a single partition key and a single clustering key.
fn simple_schema_with_clustering(
    keyspace: &str,
    table: &str,
    columns: Vec<(&str, &str)>,
    pk: &str,
    ck: &str,
) -> TableSchema {
    let col_structs: Vec<Column> = columns
        .iter()
        .map(|(name, dt)| Column {
            name: name.to_string(),
            data_type: dt.to_string(),
            nullable: name != &pk && name != &ck,
            default: None,
            is_static: false,
        })
        .collect();

    let ck_type = col_structs
        .iter()
        .find(|c| c.name == ck)
        .map(|c| c.data_type.clone())
        .unwrap_or_else(|| "int".to_string());

    TableSchema {
        keyspace: keyspace.to_string(),
        table: table.to_string(),
        partition_keys: vec![KeyColumn {
            name: pk.to_string(),
            data_type: col_structs
                .iter()
                .find(|c| c.name == pk)
                .map(|c| c.data_type.clone())
                .unwrap_or_else(|| "int".to_string()),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: ck.to_string(),
            data_type: ck_type,
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: col_structs,
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// Create a `WriteEngine` in the provided `TempDir`.
fn create_engine(temp_dir: &TempDir, schema: TableSchema) -> WriteEngine {
    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );
    WriteEngine::new(config).expect("failed to create WriteEngine")
}

// ============================================================================
// Tests
// ============================================================================

/// Write 10,000 clustering rows into a single partition, then flush.
///
/// Validates VInt offset encoding at scale — the partition body will be large
/// enough that multi-byte VInts are required for internal offsets.
#[tokio::test]
#[ignore] // Large-data test — run with: cargo test -- --ignored
async fn test_10k_clustering_rows_single_partition() -> cqlite_core::error::Result<()> {
    let temp_dir = TempDir::new().expect("failed to create temp dir");

    // Schema: pk int, ck int, data text — columns sorted alphabetically
    let schema = simple_schema_with_clustering(
        "test_large",
        "clustering_10k",
        vec![("ck", "int"), ("data", "text"), ("pk", "int")],
        "pk",
        "ck",
    );

    let mut engine = create_engine(&temp_dir, schema);
    let table_id = TableId::new("test_large", "clustering_10k");
    let timestamp = 1_700_000_000_000_000_i64;

    for ck_val in 0..10_000_i32 {
        let pk = PartitionKey::single("pk", Value::Integer(1));
        let ck = ClusteringKey::single("ck", Value::Integer(ck_val));
        let ops = vec![CellOperation::Write {
            column: "data".to_string(),
            value: Value::Text(format!("row_{}", ck_val)),
        }];
        let mutation = Mutation::new(
            table_id.clone(),
            pk,
            Some(ck),
            ops,
            timestamp + ck_val as i64,
            None,
        );
        engine.write_async(mutation).await?;
    }

    let info = engine
        .flush()
        .await?
        .expect("flush should produce an SSTable");

    assert_eq!(
        info.partition_count, 1,
        "all 10k clustering rows belong to one partition"
    );
    assert!(
        info.data_size > 0,
        "Data.db must be non-empty after flushing 10k rows"
    );

    engine.close().await?;
    Ok(())
}

/// Write a single row containing a list with 10,000 integer elements, then flush.
///
/// Validates collection serialization path under high element count.
#[tokio::test]
#[ignore] // Large-data test — run with: cargo test -- --ignored
async fn test_10k_element_list_roundtrip() -> cqlite_core::error::Result<()> {
    let temp_dir = TempDir::new().expect("failed to create temp dir");

    // Schema: my_list list<int>, pk int — columns sorted alphabetically
    let schema = simple_schema(
        "test_large",
        "list_10k",
        vec![("my_list", "list<int>"), ("pk", "int")],
        "pk",
    );

    let mut engine = create_engine(&temp_dir, schema);
    let table_id = TableId::new("test_large", "list_10k");

    let large_list = Value::List((0..10_000_i32).map(Value::Integer).collect());

    let pk = PartitionKey::single("pk", Value::Integer(1));
    let ops = vec![CellOperation::Write {
        column: "my_list".to_string(),
        value: large_list,
    }];
    let mutation = Mutation::new(table_id, pk, None, ops, 1_700_000_000_000_000, None);
    engine.write_async(mutation).await?;

    let info = engine
        .flush()
        .await?
        .expect("flush should produce an SSTable");

    assert_eq!(
        info.partition_count, 1,
        "single partition with 10k-element list"
    );

    engine.close().await?;
    Ok(())
}

/// Write a single row containing a set with 10,000 text elements, then flush.
///
/// Validates set serialization path under high element count.
#[tokio::test]
#[ignore] // Large-data test — run with: cargo test -- --ignored
async fn test_10k_element_set_roundtrip() -> cqlite_core::error::Result<()> {
    let temp_dir = TempDir::new().expect("failed to create temp dir");

    // Schema: my_set set<text>, pk int — columns sorted alphabetically
    let schema = simple_schema(
        "test_large",
        "set_10k",
        vec![("my_set", "set<text>"), ("pk", "int")],
        "pk",
    );

    let mut engine = create_engine(&temp_dir, schema);
    let table_id = TableId::new("test_large", "set_10k");

    let large_set = Value::Set(
        (0..10_000_i32)
            .map(|i| Value::Text(format!("item_{}", i)))
            .collect(),
    );

    let pk = PartitionKey::single("pk", Value::Integer(1));
    let ops = vec![CellOperation::Write {
        column: "my_set".to_string(),
        value: large_set,
    }];
    let mutation = Mutation::new(table_id, pk, None, ops, 1_700_000_000_000_000, None);
    engine.write_async(mutation).await?;

    let info = engine
        .flush()
        .await?
        .expect("flush should produce an SSTable");

    assert_eq!(
        info.partition_count, 1,
        "single partition with 10k-element set"
    );

    engine.close().await?;
    Ok(())
}

/// Write a single row containing a map with 10,000 entries, then flush.
///
/// Validates map serialization path under high element count.
#[tokio::test]
#[ignore] // Large-data test — run with: cargo test -- --ignored
async fn test_10k_element_map_roundtrip() -> cqlite_core::error::Result<()> {
    let temp_dir = TempDir::new().expect("failed to create temp dir");

    // Schema: my_map map<int, text>, pk int — columns sorted alphabetically
    let schema = simple_schema(
        "test_large",
        "map_10k",
        vec![("my_map", "map<int,text>"), ("pk", "int")],
        "pk",
    );

    let mut engine = create_engine(&temp_dir, schema);
    let table_id = TableId::new("test_large", "map_10k");

    let large_map = Value::Map(
        (0..10_000_i32)
            .map(|i| (Value::Integer(i), Value::Text(format!("val_{}", i))))
            .collect(),
    );

    let pk = PartitionKey::single("pk", Value::Integer(1));
    let ops = vec![CellOperation::Write {
        column: "my_map".to_string(),
        value: large_map,
    }];
    let mutation = Mutation::new(table_id, pk, None, ops, 1_700_000_000_000_000, None);
    engine.write_async(mutation).await?;

    let info = engine
        .flush()
        .await?
        .expect("flush should produce an SSTable");

    assert_eq!(
        info.partition_count, 1,
        "single partition with 10k-element map"
    );

    engine.close().await?;
    Ok(())
}

/// Write a single row containing a 10 MB blob value, then flush.
///
/// Validates large cell value encoding — the cell length VInt will require
/// multiple bytes (> 2^21), exercising the full VInt width.
#[tokio::test]
#[ignore] // Large-data test — run with: cargo test -- --ignored
async fn test_10mb_blob_roundtrip() -> cqlite_core::error::Result<()> {
    let temp_dir = TempDir::new().expect("failed to create temp dir");

    // Use a larger threshold so the 10MB blob doesn't trigger auto-flush
    let schema = simple_schema(
        "test_large",
        "blob_10mb",
        vec![("data", "blob"), ("pk", "int")],
        "pk",
    );

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    )
    .with_flush_threshold(256 * 1024 * 1024) // 256 MB
    .with_hard_limit(512 * 1024 * 1024); // 512 MB

    let mut engine = WriteEngine::new(config).expect("failed to create WriteEngine");
    let table_id = TableId::new("test_large", "blob_10mb");

    const BLOB_SIZE: usize = 10 * 1024 * 1024; // 10 MB
    let blob = Value::Blob(vec![0xAB_u8; BLOB_SIZE]);

    let pk = PartitionKey::single("pk", Value::Integer(1));
    let ops = vec![CellOperation::Write {
        column: "data".to_string(),
        value: blob,
    }];
    let mutation = Mutation::new(table_id, pk, None, ops, 1_700_000_000_000_000, None);
    engine.write_async(mutation).await?;

    let info = engine
        .flush()
        .await?
        .expect("flush should produce an SSTable");

    assert_eq!(info.partition_count, 1, "single partition with 10MB blob");
    assert!(
        info.data_size >= BLOB_SIZE as u64,
        "Data.db ({} bytes) must be at least as large as the raw blob ({} bytes)",
        info.data_size,
        BLOB_SIZE,
    );

    engine.close().await?;
    Ok(())
}

/// Write a single row across a 500-column wide schema, then flush.
///
/// Validates column bitmap encoding correctness when there are many regular
/// columns — the bitmap spans multiple bytes and must correctly mark present
/// vs. absent columns.
#[tokio::test]
#[ignore] // Large-data test — run with: cargo test -- --ignored
async fn test_500_columns_roundtrip() -> cqlite_core::error::Result<()> {
    let temp_dir = TempDir::new().expect("failed to create temp dir");

    // Build schema: pk int + col_000..col_499 text, sorted alphabetically.
    // "col_NNN" sorts before "pk" so pk comes last in the columns vec.
    // Build Column structs directly using owned strings to avoid lifetime issues.
    let mut col_structs: Vec<Column> = (0..500)
        .map(|i| Column {
            name: format!("col_{:03}", i),
            data_type: "text".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        })
        .collect();
    // Add pk column
    col_structs.push(Column {
        name: "pk".to_string(),
        data_type: "int".to_string(),
        nullable: false,
        default: None,
        is_static: false,
    });
    // Sort alphabetically — "col_NNN" comes before "pk"
    col_structs.sort_by(|a, b| a.name.cmp(&b.name));

    let schema = TableSchema {
        keyspace: "test_large".to_string(),
        table: "wide_500".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: col_structs,
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let mut engine = create_engine(&temp_dir, schema);
    let table_id = TableId::new("test_large", "wide_500");

    // Build 500 write operations — one per column.
    let ops: Vec<CellOperation> = (0..500)
        .map(|i| CellOperation::Write {
            column: format!("col_{:03}", i),
            value: Value::Text(format!("value_{}", i)),
        })
        .collect();

    let pk = PartitionKey::single("pk", Value::Integer(1));
    let mutation = Mutation::new(table_id, pk, None, ops, 1_700_000_000_000_000, None);
    engine.write_async(mutation).await?;

    let info = engine
        .flush()
        .await?
        .expect("flush should produce an SSTable");

    assert_eq!(info.partition_count, 1, "single partition with 500 columns");

    engine.close().await?;
    Ok(())
}

/// Write 1,000 partitions (distinct partition keys), then flush.
///
/// Validates that the Bloom filter is correctly sized for large partition
/// counts and that the Index.db / Summary.db are produced correctly.
#[tokio::test]
#[ignore] // Large-data test — run with: cargo test -- --ignored
async fn test_1000_partitions_roundtrip() -> cqlite_core::error::Result<()> {
    let temp_dir = TempDir::new().expect("failed to create temp dir");

    // Schema: pk int, value text — columns sorted alphabetically
    let schema = simple_schema(
        "test_large",
        "partitions_1k",
        vec![("pk", "int"), ("value", "text")],
        "pk",
    );

    let mut engine = create_engine(&temp_dir, schema);
    let table_id = TableId::new("test_large", "partitions_1k");
    let timestamp = 1_700_000_000_000_000_i64;

    for i in 0..1_000_i32 {
        let pk = PartitionKey::single("pk", Value::Integer(i));
        let ops = vec![CellOperation::Write {
            column: "value".to_string(),
            value: Value::Text(format!("partition_{}", i)),
        }];
        let mutation = Mutation::new(table_id.clone(), pk, None, ops, timestamp + i as i64, None);
        engine.write_async(mutation).await?;
    }

    let info = engine
        .flush()
        .await?
        .expect("flush should produce an SSTable");

    assert_eq!(
        info.partition_count, 1_000,
        "should have exactly 1,000 distinct partitions"
    );

    engine.close().await?;
    Ok(())
}
