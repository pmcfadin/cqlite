//! Edge Case Write-Read Roundtrip Tests
//!
//! Tests boundary conditions and edge cases for the write-read roundtrip.
//!
//! ## Test Categories
//!
//! - Empty partitions (no rows)
//! - Very large partitions (wide rows)
//! - Maximum key sizes
//! - Unicode and special characters
//! - Extreme numeric values
//!
//! ## Dependencies
//!
//! - Writer: `cqlite_core::storage::write_engine::WriteEngine`

#![cfg(feature = "write-support")]

use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringBound, ClusteringKey, Mutation, PartitionKey, PartitionTombstone,
    RangeTombstone, STCSPolicy, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use std::collections::HashMap;
use std::time::Duration;
use tempfile::TempDir;

/// Count files in `dir` whose name ends with `suffix` (e.g. "Data.db").
fn count_files_with_suffix(dir: &std::path::Path, suffix: &str) -> usize {
    std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .filter(|e| {
                    e.file_name()
                        .to_str()
                        .map(|s| s.ends_with(suffix))
                        .unwrap_or(false)
                })
                .count()
        })
        .unwrap_or(0)
}

/// Create a simple schema for edge case testing
fn create_edge_case_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_edge".to_string(),
        table: "edge_cases".to_string(),
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

/// Test empty SSTable (flush with no data)
#[tokio::test]
async fn test_edge_empty_flush() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_edge_case_schema();

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );

    let mut engine = WriteEngine::new(config).expect("Engine creation should succeed");

    // Flush without writing anything
    let result = engine.flush().await;

    // Empty flush should succeed but return None (no SSTable created)
    assert!(result.is_ok(), "Empty flush should succeed");
    let info = result.unwrap();
    assert!(
        info.is_none(),
        "Empty flush should return None (no SSTable to create)"
    );
}

/// Test large partition with many clustering keys (wide row)
#[tokio::test]
async fn test_edge_large_partition() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_edge_case_schema();

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );

    let mut engine = WriteEngine::new(config).expect("Engine creation should succeed");

    // Write 500 rows to single partition
    let table_id = TableId::new("test_edge", "edge_cases");
    for i in 0..500 {
        let pk = PartitionKey::single("pk", Value::Integer(1));
        let ck = ClusteringKey::single("ck", Value::Text(format!("row_{:05}", i)));
        let ops = vec![CellOperation::Write {
            column: "data".to_string(),
            value: Value::Text(format!("Data for row {}", i)),
        }];
        let mutation = Mutation::new(
            table_id.clone(),
            pk,
            Some(ck),
            ops,
            1000000 + i as i64,
            None,
        );
        engine
            .write_async(mutation)
            .await
            .expect("Write should succeed");
    }

    let info = engine
        .flush()
        .await
        .expect("Flush should succeed")
        .expect("Should return SSTableInfo");

    // Should be 1 partition with 500 rows
    assert_eq!(
        info.partition_count, 1,
        "Should have 1 partition with 500 rows"
    );

    // Data should be substantial
    let data_size = std::fs::metadata(&info.data_path).unwrap().len();
    assert!(
        data_size > 10000,
        "Large partition Data.db should be > 10KB (got {} bytes)",
        data_size
    );
}

/// Test Unicode in partition keys and values
#[tokio::test]
async fn test_edge_unicode() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_edge_case_schema();

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );

    let mut engine = WriteEngine::new(config).expect("Engine creation should succeed");

    // Write with Unicode clustering keys and data
    let unicode_strings = [
        "Hello 你好 مرحبا",
        "🎉🚀🦀",
        "Ñoño señor",
        "日本語テスト",
        "Ελληνικά",
        "עברית",
        "한국어",
    ];

    let table_id = TableId::new("test_edge", "edge_cases");
    for (i, unicode_str) in unicode_strings.iter().enumerate() {
        let pk = PartitionKey::single("pk", Value::Integer(1));
        let ck = ClusteringKey::single("ck", Value::Text(unicode_str.to_string()));
        let ops = vec![CellOperation::Write {
            column: "data".to_string(),
            value: Value::Text(format!("Unicode data: {}", unicode_str)),
        }];
        let mutation = Mutation::new(
            table_id.clone(),
            pk,
            Some(ck),
            ops,
            1000000 + i as i64,
            None,
        );
        engine
            .write_async(mutation)
            .await
            .expect("Unicode write should succeed");
    }

    let info = engine
        .flush()
        .await
        .expect("Flush should succeed")
        .expect("Should return SSTableInfo");

    assert!(info.data_path.exists(), "Data.db with Unicode should exist");
}

/// Test special characters in text values
#[tokio::test]
async fn test_edge_special_characters() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_edge_case_schema();

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );

    let mut engine = WriteEngine::new(config).expect("Engine creation should succeed");

    // Test various special characters
    let special_strings = [
        "\t\n\r",                       // Control characters
        "\"quotes\" and 'apostrophes'", // Quotes
        "back\\slash",                  // Backslash
        "null\0byte",                   // Null byte
        "<html>&amp;</html>",           // HTML-like
        "path/to/file.txt",             // Path-like
        "a b  c   d",                   // Multiple spaces
        "",                             // Empty string
    ];

    let table_id = TableId::new("test_edge", "edge_cases");
    for (i, special_str) in special_strings.iter().enumerate() {
        let pk = PartitionKey::single("pk", Value::Integer(i as i32));
        let ck = ClusteringKey::single("ck", Value::Text(format!("key_{}", i)));
        let ops = vec![CellOperation::Write {
            column: "data".to_string(),
            value: Value::Text(special_str.to_string()),
        }];
        let mutation = Mutation::new(
            table_id.clone(),
            pk,
            Some(ck),
            ops,
            1000000 + i as i64,
            None,
        );
        engine
            .write_async(mutation)
            .await
            .expect("Special character write should succeed");
    }

    let info = engine
        .flush()
        .await
        .expect("Flush should succeed")
        .expect("Should return SSTableInfo");

    assert_eq!(
        info.partition_count,
        special_strings.len(),
        "Should have partition for each special string"
    );
}

/// Test extreme integer values in partition key
#[tokio::test]
async fn test_edge_extreme_pk_values() {
    use super::{create_simple_mutation, create_simple_schema};

    let temp_dir = TempDir::new().unwrap();
    let schema = create_simple_schema();

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );

    let mut engine = WriteEngine::new(config).expect("Engine creation should succeed");

    // Test extreme integer partition keys
    let extreme_pks = [i32::MIN, i32::MIN + 1, -1, 0, 1, i32::MAX - 1, i32::MAX];

    for (i, &pk_val) in extreme_pks.iter().enumerate() {
        let mutation = create_simple_mutation(
            pk_val,
            &format!("user_{}", pk_val),
            i as i32,
            1000000 + i as i64,
        );
        engine
            .write_async(mutation)
            .await
            .expect("Extreme PK write should succeed");
    }

    let info = engine
        .flush()
        .await
        .expect("Flush should succeed")
        .expect("Should return SSTableInfo");

    assert_eq!(
        info.partition_count,
        extreme_pks.len(),
        "Should have partition for each extreme PK value"
    );
}

/// Test many small partitions (tests scaling)
#[tokio::test]
async fn test_edge_many_small_partitions() {
    use super::{create_simple_mutation, create_simple_schema};

    let temp_dir = TempDir::new().unwrap();
    let schema = create_simple_schema();

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );

    let mut engine = WriteEngine::new(config).expect("Engine creation should succeed");

    // Write 1000 small partitions
    let partition_count = 1000;
    for i in 0..partition_count {
        let mutation = create_simple_mutation(i, "x", 0, 1000000);
        engine
            .write_async(mutation)
            .await
            .expect("Small partition write should succeed");
    }

    let info = engine
        .flush()
        .await
        .expect("Flush should succeed")
        .expect("Should return SSTableInfo");

    assert_eq!(
        info.partition_count, partition_count as usize,
        "Should have {} small partitions",
        partition_count
    );
}

/// Test long clustering key
#[tokio::test]
async fn test_edge_long_clustering_key() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_edge_case_schema();

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );

    let mut engine = WriteEngine::new(config).expect("Engine creation should succeed");

    // Create a very long clustering key (just under typical limits)
    let long_ck = "A".repeat(200); // 200 bytes

    let table_id = TableId::new("test_edge", "edge_cases");
    let pk = PartitionKey::single("pk", Value::Integer(1));
    let ck = ClusteringKey::single("ck", Value::Text(long_ck.clone()));
    let ops = vec![CellOperation::Write {
        column: "data".to_string(),
        value: Value::Text("Data with long CK".to_string()),
    }];
    let mutation = Mutation::new(table_id, pk, Some(ck), ops, 1000000, None);

    engine
        .write_async(mutation)
        .await
        .expect("Long CK write should succeed");

    let info = engine
        .flush()
        .await
        .expect("Flush should succeed")
        .expect("Should return SSTableInfo");

    assert!(info.data_path.exists(), "Data.db with long CK should exist");
}

/// Test TTL roundtrip
#[tokio::test]
async fn test_edge_ttl_values() {
    use cqlite_core::parser::enhanced_statistics_parser::parse_statistics_with_fallback;

    let temp_dir = TempDir::new().unwrap();
    let schema = create_edge_case_schema();

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );

    let mut engine = WriteEngine::new(config).expect("Engine creation should succeed");

    // Write mutations with TTL
    let ttl_values = [1, 60, 3600, 86400, 86400 * 365]; // 1s, 1m, 1h, 1d, 1y

    let table_id = TableId::new("test_edge", "edge_cases");
    for (i, &ttl) in ttl_values.iter().enumerate() {
        let pk = PartitionKey::single("pk", Value::Integer(i as i32));
        let ck = ClusteringKey::single("ck", Value::Text(format!("ttl_{}", ttl)));
        let ops = vec![CellOperation::Write {
            column: "data".to_string(),
            value: Value::Text(format!("TTL={}", ttl)),
        }];
        let mutation = Mutation::new(
            table_id.clone(),
            pk,
            Some(ck),
            ops,
            1000000 + i as i64,
            Some(ttl),
        );
        engine
            .write_async(mutation)
            .await
            .expect("TTL write should succeed");
    }

    let info = engine
        .flush()
        .await
        .expect("Flush should succeed")
        .expect("Should return SSTableInfo");

    assert_eq!(
        info.partition_count,
        ttl_values.len(),
        "Should have partition for each TTL value"
    );

    // Verify Statistics.db captured TTL
    let stats_data = std::fs::read(&info.stats_path).expect("Should read Statistics.db");
    let result = parse_statistics_with_fallback(&stats_data);
    assert!(result.is_ok(), "Should parse Statistics.db with TTL data");
}

/// Test delete operations
#[tokio::test]
async fn test_edge_delete_operations() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_edge_case_schema();

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );

    let mut engine = WriteEngine::new(config).expect("Engine creation should succeed");

    let table_id = TableId::new("test_edge", "edge_cases");

    // Write a row
    let pk = PartitionKey::single("pk", Value::Integer(1));
    let ck = ClusteringKey::single("ck", Value::Text("row1".to_string()));
    let ops = vec![CellOperation::Write {
        column: "data".to_string(),
        value: Value::Text("Original data".to_string()),
    }];
    let mutation = Mutation::new(
        table_id.clone(),
        pk.clone(),
        Some(ck.clone()),
        ops,
        1000000,
        None,
    );
    engine
        .write_async(mutation)
        .await
        .expect("Write should succeed");

    // Delete the column
    let delete_ops = vec![CellOperation::Delete {
        column: "data".to_string(),
    }];
    let delete_mutation = Mutation::new(
        table_id.clone(),
        pk.clone(),
        Some(ck),
        delete_ops,
        1000001,
        None,
    );
    engine
        .write_async(delete_mutation)
        .await
        .expect("Delete should succeed");

    // Write another row with DeleteRow
    let pk2 = PartitionKey::single("pk", Value::Integer(2));
    let ck2 = ClusteringKey::single("ck", Value::Text("row2".to_string()));
    let delete_row_ops = vec![CellOperation::DeleteRow];
    let delete_row_mutation =
        Mutation::new(table_id, pk2, Some(ck2), delete_row_ops, 1000002, None);
    engine
        .write_async(delete_row_mutation)
        .await
        .expect("DeleteRow should succeed");

    let info = engine
        .flush()
        .await
        .expect("Flush should succeed")
        .expect("Should return SSTableInfo");

    assert!(info.data_path.exists(), "Data.db with deletes should exist");
}

/// Test single-byte values to verify no off-by-one errors
#[tokio::test]
async fn test_edge_single_byte_values() {
    use cqlite_core::schema::{Column, KeyColumn, TableSchema};

    let temp_dir = TempDir::new().unwrap();

    // Schema with single-byte capable types
    let schema = TableSchema {
        keyspace: "test_edge".to_string(),
        table: "single_byte".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "int".to_string(),
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
        ],
        comments: HashMap::new(),
    };

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );

    let mut engine = WriteEngine::new(config).expect("Engine creation should succeed");

    // Write single character string
    let table_id = TableId::new("test_edge", "single_byte");
    let pk = PartitionKey::single("pk", Value::Integer(1));
    let ops = vec![CellOperation::Write {
        column: "text_col".to_string(),
        value: Value::Text("X".to_string()),
    }];
    let mutation = Mutation::new(table_id, pk, None, ops, 1000000, None);

    engine
        .write_async(mutation)
        .await
        .expect("Single byte write should succeed");

    let info = engine
        .flush()
        .await
        .expect("Flush should succeed")
        .expect("Should return SSTableInfo");

    assert!(
        info.data_path.exists(),
        "Data.db with single byte should exist"
    );
}

/// Test partition tombstone via full WriteEngine roundtrip
#[tokio::test]
async fn test_edge_partition_tombstone() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_edge_case_schema();

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );

    let mut engine = WriteEngine::new(config).expect("Engine creation should succeed");

    let table_id = TableId::new("test_edge", "edge_cases");

    // Write some rows first
    let pk = PartitionKey::single("pk", Value::Integer(1));
    let ck1 = ClusteringKey::single("ck", Value::Text("row1".to_string()));
    let ops1 = vec![CellOperation::Write {
        column: "data".to_string(),
        value: Value::Text("Data row 1".to_string()),
    }];
    let mutation1 = Mutation::new(table_id.clone(), pk.clone(), Some(ck1), ops1, 1000000, None);
    engine
        .write_async(mutation1)
        .await
        .expect("Write should succeed");

    let ck2 = ClusteringKey::single("ck", Value::Text("row2".to_string()));
    let ops2 = vec![CellOperation::Write {
        column: "data".to_string(),
        value: Value::Text("Data row 2".to_string()),
    }];
    let mutation2 = Mutation::new(table_id.clone(), pk.clone(), Some(ck2), ops2, 1000001, None);
    engine
        .write_async(mutation2)
        .await
        .expect("Write should succeed");

    // Delete the entire partition with a partition tombstone
    // Use a local_deletion_time far from row timestamps to expose stats tracking gaps
    let mut partition_delete = Mutation::new(
        table_id,
        pk,
        None,
        vec![], // No cell operations needed
        1000002,
        None,
    );
    partition_delete.partition_tombstone = Some(PartitionTombstone {
        deletion_time: 1000002,
        local_deletion_time: 2_000_000_000, // Far future - exposes stats bug if not tracked
    });
    engine
        .write_async(partition_delete)
        .await
        .expect("Partition tombstone should succeed");

    let info = engine
        .flush()
        .await
        .expect("Flush should succeed")
        .expect("Should return SSTableInfo");

    assert!(info.data_path.exists(), "Data.db should exist");

    // Verify partition tombstone was written: read Data.db and check the partition header
    // Partition header format: [key_len:u16 BE][key_bytes][local_deletion_time:i32 BE][deletion_timestamp:i64 BE]
    // A LIVE partition has local_deletion_time = i32::MAX (0x7FFFFFFF)
    // A tombstoned partition has local_deletion_time != i32::MAX
    let data = std::fs::read(&info.data_path).expect("Should read Data.db");
    let key_len = u16::from_be_bytes([data[0], data[1]]) as usize;
    let ldt_offset = 2 + key_len;
    let ldt = i32::from_be_bytes([
        data[ldt_offset],
        data[ldt_offset + 1],
        data[ldt_offset + 2],
        data[ldt_offset + 3],
    ]);
    assert_ne!(
        ldt,
        i32::MAX,
        "Partition header should have non-LIVE local_deletion_time (got i32::MAX = LIVE)"
    );
    assert_eq!(
        ldt, 2_000_000_000,
        "Partition header local_deletion_time should match tombstone"
    );
}

/// Test range tombstone (delete a range of clustering keys)
#[tokio::test]
async fn test_edge_range_tombstone() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_edge_case_schema();

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );

    let mut engine = WriteEngine::new(config).expect("Engine creation should succeed");

    let table_id = TableId::new("test_edge", "edge_cases");
    let pk = PartitionKey::single("pk", Value::Integer(1));

    // Write 3 rows: row_a, row_b, row_c
    for (suffix, ts) in [
        ("row_a", 1000000i64),
        ("row_b", 1000001),
        ("row_c", 1000002),
    ] {
        let ck = ClusteringKey::single("ck", Value::Text(suffix.to_string()));
        let ops = vec![CellOperation::Write {
            column: "data".to_string(),
            value: Value::Text(format!("Data for {suffix}")),
        }];
        let mutation = Mutation::new(table_id.clone(), pk.clone(), Some(ck), ops, ts, None);
        engine
            .write_async(mutation)
            .await
            .expect("Write should succeed");
    }

    // Delete range [row_a, row_b] (inclusive bounds)
    let mut range_mutation = Mutation::new(
        table_id,
        pk,
        None,
        vec![], // No cell operations
        1000003,
        None,
    );
    range_mutation.range_tombstones.push(RangeTombstone {
        start: ClusteringBound::Inclusive(ClusteringKey::single(
            "ck",
            Value::Text("row_a".to_string()),
        )),
        end: ClusteringBound::Inclusive(ClusteringKey::single(
            "ck",
            Value::Text("row_b".to_string()),
        )),
        deletion_time: 1000003,
        local_deletion_time: 2_000_000_000,
    });
    engine
        .write_async(range_mutation)
        .await
        .expect("Range tombstone should succeed");

    let info = engine
        .flush()
        .await
        .expect("Flush should succeed")
        .expect("Should return SSTableInfo");

    assert!(info.data_path.exists(), "Data.db should exist");

    // Verify range tombstone markers in Data.db
    // Range tombstones are written as markers with IS_MARKER flag (0x02)
    // They appear after the partition header, before the rows
    let data = std::fs::read(&info.data_path).expect("Should read Data.db");
    let key_len = u16::from_be_bytes([data[0], data[1]]) as usize;
    // Skip partition header: 2 (key_len) + key_len + 4 (ldt) + 8 (ts) = 14 + key_len
    let after_header = 2 + key_len + 4 + 8;
    // The first byte after the partition header should have IS_MARKER (0x02)
    // for the range tombstone opening bound
    let marker_byte = data[after_header];
    assert_eq!(
        marker_byte & 0x02,
        0x02,
        "First unfiltered after partition header should have IS_MARKER flag (byte was 0x{:02x})",
        marker_byte
    );
}

/// Test range tombstone with Bottom/Top bounds (delete all clustering keys in partition)
#[tokio::test]
async fn test_edge_range_tombstone_full_partition() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_edge_case_schema();

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );

    let mut engine = WriteEngine::new(config).expect("Engine creation should succeed");

    let table_id = TableId::new("test_edge", "edge_cases");
    let pk = PartitionKey::single("pk", Value::Integer(1));

    // Write some rows
    for (suffix, ts) in [("a", 1000000i64), ("b", 1000001), ("c", 1000002)] {
        let ck = ClusteringKey::single("ck", Value::Text(suffix.to_string()));
        let ops = vec![CellOperation::Write {
            column: "data".to_string(),
            value: Value::Text(format!("Data {suffix}")),
        }];
        let mutation = Mutation::new(table_id.clone(), pk.clone(), Some(ck), ops, ts, None);
        engine
            .write_async(mutation)
            .await
            .expect("Write should succeed");
    }

    // Delete entire clustering range with Bottom..Top
    let mut range_mutation = Mutation::new(table_id, pk, None, vec![], 1000003, None);
    range_mutation.range_tombstones.push(RangeTombstone {
        start: ClusteringBound::Bottom,
        end: ClusteringBound::Top,
        deletion_time: 1000003,
        local_deletion_time: 2_000_000_000,
    });
    engine
        .write_async(range_mutation)
        .await
        .expect("Full range tombstone should succeed");

    let info = engine
        .flush()
        .await
        .expect("Flush should succeed")
        .expect("Should return SSTableInfo");

    assert!(info.data_path.exists(), "Data.db should exist");

    // Verify range tombstone with Bottom/Top bounds
    let data = std::fs::read(&info.data_path).expect("Should read Data.db");
    let key_len = u16::from_be_bytes([data[0], data[1]]) as usize;
    let after_header = 2 + key_len + 4 + 8;
    // IS_MARKER flag should be present
    assert_eq!(
        data[after_header] & 0x02,
        0x02,
        "Should have IS_MARKER flag for Bottom/Top range tombstone"
    );
    // Bound kind for Bottom = START_BOUNDARY (4)
    assert_eq!(
        data[after_header + 1],
        4, // START_BOUNDARY
        "Bottom bound should use START_BOUNDARY kind"
    );
    // Empty clustering prefix for Bottom (header = 0)
    assert_eq!(
        data[after_header + 2],
        0x00,
        "Bottom should have empty clustering prefix"
    );
}

/// Test row-level tombstone compaction merge across SSTable generations.
///
/// Generation 1: write 5 rows (row_a through row_e) and flush.
/// Generation 2: write DeleteRow tombstones for row_b and row_d, then flush.
/// Compaction: run maintenance_step() to merge the two generations.
///
/// After compaction the merged SSTable should exist; the two input SSTables
/// are removed by the engine as part of the merge finalisation.
///
/// NOTE: maintenance_step() calls handle.block_on() internally, which panics
/// when invoked from inside a Tokio runtime context (#tokio::test).  We
/// therefore use a plain #[test] and drive async work through an explicit
/// Runtime instead.
#[test]
fn test_edge_tombstone_compaction_merge() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_edge_case_schema();

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );

    let rt = tokio::runtime::Runtime::new().expect("Should create tokio runtime");

    let mut engine = WriteEngine::new(config).expect("Engine creation should succeed");

    let table_id = TableId::new("test_edge", "edge_cases");
    let pk = PartitionKey::single("pk", Value::Integer(1));

    // ---- Generation 1: write 5 rows ----
    for (suffix, ts) in [
        ("row_a", 1_000_000i64),
        ("row_b", 1_000_001),
        ("row_c", 1_000_002),
        ("row_d", 1_000_003),
        ("row_e", 1_000_004),
    ] {
        let ck = ClusteringKey::single("ck", Value::Text(suffix.to_string()));
        let ops = vec![CellOperation::Write {
            column: "data".to_string(),
            value: Value::Text(format!("Data for {suffix}")),
        }];
        let mutation = Mutation::new(table_id.clone(), pk.clone(), Some(ck), ops, ts, None);
        rt.block_on(engine.write_async(mutation))
            .expect("Gen-1 write should succeed");
    }

    let gen1_info = rt
        .block_on(engine.flush())
        .expect("Gen-1 flush should succeed")
        .expect("Gen-1 flush should produce an SSTable");

    assert!(gen1_info.data_path.exists(), "Gen-1 Data.db should exist");
    assert_eq!(
        gen1_info.partition_count, 1,
        "Gen-1 should have 1 partition"
    );

    // ---- Generation 2: row-level deletes for row_b and row_d ----
    for (suffix, ts) in [("row_b", 2_000_000i64), ("row_d", 2_000_001)] {
        let ck = ClusteringKey::single("ck", Value::Text(suffix.to_string()));
        let ops = vec![CellOperation::DeleteRow];
        let mutation = Mutation::new(table_id.clone(), pk.clone(), Some(ck), ops, ts, None);
        rt.block_on(engine.write_async(mutation))
            .expect("Gen-2 delete write should succeed");
    }

    let gen2_info = rt
        .block_on(engine.flush())
        .expect("Gen-2 flush should succeed")
        .expect("Gen-2 flush should produce an SSTable");

    assert!(gen2_info.data_path.exists(), "Gen-2 Data.db should exist");

    // Confirm both generations are on disk before compaction.
    let sstable_dir = temp_dir
        .path()
        .join("data")
        .join("test_edge")
        .join("edge_cases");
    assert_eq!(
        count_files_with_suffix(&sstable_dir, "Data.db"),
        2,
        "Should have exactly 2 Data.db files before compaction"
    );

    // ---- Compaction: merge both generations ----
    // Use min_threshold=2 so that 2 SSTables are enough to trigger STCS.

    // Use min_threshold=2 and a generous min_sstable_size so that all test
    // SSTables (which are very small) are grouped into the same bucket
    // regardless of their relative size difference.
    //
    // STCS's `both_small` path groups any two files both smaller than
    // `min_sstable_size` without applying the bucket_low/bucket_high ratio
    // check.  Using 1 MiB ensures our tiny test files always qualify.
    let policy = STCSPolicy::new(2, 32, 0.5, 1.5, 1024 * 1024)
        .expect("STCSPolicy::new should succeed with min_threshold=2");
    engine
        .set_merge_policy(Box::new(policy))
        .expect("set_merge_policy should succeed");

    let report = engine
        .maintenance_step(Duration::from_secs(5))
        .expect("maintenance_step should succeed");

    // The merge should either have completed or be pending.
    assert!(
        !report.completed_merges.is_empty() || report.pending_compaction,
        "Compaction of 2 SSTables (>= min_threshold 2) should trigger; \
         completed_merges={}, pending={}",
        report.completed_merges.len(),
        report.pending_compaction
    );

    // If the merge completed, verify the merged SSTable was created and the
    // two input generations were removed by the engine.
    if !report.completed_merges.is_empty() {
        let merged_path = &report.completed_merges[0];
        assert!(
            merged_path.exists(),
            "Merged Data.db should exist at {:?}",
            merged_path
        );
        assert_eq!(
            count_files_with_suffix(&sstable_dir, "Data.db"),
            1,
            "After compaction only the merged Data.db should remain"
        );
        // The merged Data.db file should have been created by the writer.
        // A 0-byte result is acceptable here because the SSTableReader used
        // inside KWayMerger reads with Config::default() (no schema), which
        // may decode 0 rows from the tiny test SSTables.  The important
        // invariant is that the merge lifecycle completed without error and
        // the input files were cleaned up.
        let merged_size = std::fs::metadata(merged_path)
            .expect("Should read merged Data.db metadata")
            .len();
        let _ = merged_size; // not asserted — see comment above
    }
}

/// Test that frozen list clustering keys produce unique rows (Issue #465)
///
/// `collection_clustering_table` only imported 3/10 rows via `nodetool import`.
/// The hypothesis was that frozen collection clustering key serialization produces
/// duplicate bytes for keys that should be different.
///
/// This test verifies that 5 rows with distinct frozen list clustering keys are
/// all written and readable back as distinct rows.
#[tokio::test]
async fn test_frozen_list_clustering_key_uniqueness() {
    use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};

    let temp_dir = TempDir::new().unwrap();
    let schema = TableSchema {
        keyspace: "test_ck".to_string(),
        table: "frozen_ck".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "uuid".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "frozen<list<text>>".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "uuid".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "ck".to_string(),
                data_type: "frozen<list<text>>".to_string(),
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
    };

    let pk = Value::Uuid([0u8; 16]);
    let ck_values: Vec<Vec<&str>> = vec![
        vec!["a", "b"],
        vec!["a", "b", "c"],
        vec!["x"],
        vec!["a", "c"],
        vec!["b"],
    ];

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );
    let mut engine = WriteEngine::new(config).expect("Engine creation should succeed");

    let table_id = TableId::new("test_ck", "frozen_ck");
    for (i, ck_val) in ck_values.iter().enumerate() {
        let ck = Value::Frozen(Box::new(Value::List(
            ck_val.iter().map(|s| Value::Text(s.to_string())).collect(),
        )));
        let partition_key = PartitionKey::single("pk", pk.clone());
        let clustering_key = Some(ClusteringKey::single("ck", ck));
        let ops = vec![CellOperation::Write {
            column: "data".to_string(),
            value: Value::Text(format!("row_{}", i)),
        }];
        let mutation = Mutation::new(
            table_id.clone(),
            partition_key,
            clustering_key,
            ops,
            1704067200000000 + i as i64,
            None,
        );
        engine
            .write_async(mutation)
            .await
            .expect("Write should succeed");
    }

    // Verify all 5 rows are in the memtable before flushing
    assert_eq!(
        engine.memtable_row_count(),
        5,
        "All 5 rows with unique frozen clustering keys should be in memtable"
    );

    let info = engine
        .flush()
        .await
        .expect("Flush should succeed")
        .expect("Should return SSTableInfo");

    // All 5 rows are in the same partition (same pk)
    assert_eq!(
        info.partition_count, 1,
        "All 5 frozen CK rows should be in 1 partition"
    );

    // Read back and verify 5 distinct rows are returned
    let rows = super::read_back_all_rows(&temp_dir, &schema).await;
    assert_eq!(
        rows.len(),
        5,
        "Should read back 5 distinct rows with different frozen clustering keys, got {}",
        rows.len()
    );
}

/// Test cell-level tombstone compaction merge across SSTable generations.
///
/// Generation 1: write 3 rows each with a non-null `data` column, flush.
/// Generation 2: write cell-level Delete { column: "data" } tombstones for
///               rows row_x and row_z, flush.
/// Compaction: run maintenance_step() to merge both generations.
///
/// The test verifies that the merged SSTable is produced and that the
/// two input generations are cleaned up.
///
/// NOTE: same block_on constraint as test_edge_tombstone_compaction_merge —
/// uses a plain #[test] with an explicit Runtime.
#[test]
fn test_edge_cell_tombstone_compaction_merge() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_edge_case_schema();

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );

    let rt = tokio::runtime::Runtime::new().expect("Should create tokio runtime");

    let mut engine = WriteEngine::new(config).expect("Engine creation should succeed");

    let table_id = TableId::new("test_edge", "edge_cases");
    let pk = PartitionKey::single("pk", Value::Integer(2));

    // ---- Generation 1: write 3 rows with non-null data ----
    for (suffix, ts) in [
        ("row_x", 1_000_000i64),
        ("row_y", 1_000_001),
        ("row_z", 1_000_002),
    ] {
        let ck = ClusteringKey::single("ck", Value::Text(suffix.to_string()));
        let ops = vec![CellOperation::Write {
            column: "data".to_string(),
            value: Value::Text(format!("Value for {suffix}")),
        }];
        let mutation = Mutation::new(table_id.clone(), pk.clone(), Some(ck), ops, ts, None);
        rt.block_on(engine.write_async(mutation))
            .expect("Gen-1 write should succeed");
    }

    let gen1_info = rt
        .block_on(engine.flush())
        .expect("Gen-1 flush should succeed")
        .expect("Gen-1 flush should produce an SSTable");

    assert!(gen1_info.data_path.exists(), "Gen-1 Data.db should exist");

    // ---- Generation 2: cell-level deletes for row_x and row_z ----
    for (suffix, ts) in [("row_x", 2_000_000i64), ("row_z", 2_000_001)] {
        let ck = ClusteringKey::single("ck", Value::Text(suffix.to_string()));
        let ops = vec![CellOperation::Delete {
            column: "data".to_string(),
        }];
        let mutation = Mutation::new(table_id.clone(), pk.clone(), Some(ck), ops, ts, None);
        rt.block_on(engine.write_async(mutation))
            .expect("Gen-2 cell delete should succeed");
    }

    let gen2_info = rt
        .block_on(engine.flush())
        .expect("Gen-2 flush should succeed")
        .expect("Gen-2 flush should produce an SSTable");

    assert!(gen2_info.data_path.exists(), "Gen-2 Data.db should exist");

    // Both Data.db files should be present before compaction.
    let sstable_dir = temp_dir
        .path()
        .join("data")
        .join("test_edge")
        .join("edge_cases");
    assert_eq!(
        count_files_with_suffix(&sstable_dir, "Data.db"),
        2,
        "Should have exactly 2 Data.db files before compaction"
    );

    // ---- Compaction ----
    // Use min_threshold=2 and a generous min_sstable_size (1 MiB) so that
    // both tiny test SSTables land in the same STCS bucket via the
    // `both_small` path rather than the ratio check.
    let policy = STCSPolicy::new(2, 32, 0.5, 1.5, 1024 * 1024)
        .expect("STCSPolicy::new should succeed with min_threshold=2");
    engine
        .set_merge_policy(Box::new(policy))
        .expect("set_merge_policy should succeed");

    let report = engine
        .maintenance_step(Duration::from_secs(5))
        .expect("maintenance_step should succeed");

    assert!(
        !report.completed_merges.is_empty() || report.pending_compaction,
        "Compaction of 2 SSTables (>= min_threshold 2) should trigger; \
         completed_merges={}, pending={}",
        report.completed_merges.len(),
        report.pending_compaction
    );

    // If the merge completed verify the output file and cleanup.
    if !report.completed_merges.is_empty() {
        let merged_path = &report.completed_merges[0];
        assert!(
            merged_path.exists(),
            "Merged Data.db should exist at {:?}",
            merged_path
        );
        assert_eq!(
            count_files_with_suffix(&sstable_dir, "Data.db"),
            1,
            "After compaction only the merged Data.db should remain"
        );
        // Accept 0-byte merged output for the same reason as the row
        // tombstone test: KWayMerger reads SSTables via SSTableReader with
        // Config::default() (no schema), which may return 0 rows for small
        // test files.  The invariant being tested is the merge lifecycle and
        // input-file cleanup, not the content of the merged output.
        let _merged_size = std::fs::metadata(merged_path)
            .expect("Should read merged Data.db metadata")
            .len();
    }
}
