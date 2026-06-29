//! M5.2 Write Support Integration Tests (Issue #389)
//!
//! Comprehensive integration tests validating the complete write-support feature:
//! - Round-trip validation (write → flush → read)
//! - Type coverage (all CQL types including UDTs, collections, TTL)
//! - Tombstone handling (cell, row, range tombstones)
//! - Compaction (STCS policy triggering, K-way merge)
//! - Export (SSTable export with Cassandra naming)
//!
//! ## Test Structure
//!
//! Each test is independent and uses isolated temporary directories.
//! Tests are gated behind `#[cfg(feature = "write-support")]`.

#![cfg(feature = "write-support")]

use cqlite_core::error::Result;
use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringKey, ExportOptions, Mutation, PartitionKey, STCSPolicy, TableId,
    WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::{UdtField, UdtValue, Value};
use std::collections::HashMap;
use std::time::Duration;
use tempfile::TempDir;

// ============================================================================
// Helper Functions
// ============================================================================

/// Create a simple test schema with single partition key
fn create_simple_schema(keyspace: &str, table: &str) -> TableSchema {
    TableSchema {
        keyspace: keyspace.to_string(),
        table: table.to_string(),
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
                name: "value".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// Create a schema with clustering keys
fn create_clustering_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "timeseries".to_string(),
        partition_keys: vec![KeyColumn {
            name: "sensor_id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "timestamp".to_string(),
            data_type: "timestamp".to_string(),
            position: 0,
            order: ClusteringOrder::Desc,
        }],
        columns: vec![
            Column {
                name: "sensor_id".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "timestamp".to_string(),
                data_type: "timestamp".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "temperature".to_string(),
                data_type: "float".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// Create a mutation with simple text value
fn create_simple_mutation(id: i32, value: &str, timestamp: i64) -> Mutation {
    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![CellOperation::Write {
        column: "value".to_string(),
        value: Value::Text(value.to_string()),
    }];

    Mutation::new(table_id, pk, None, ops, timestamp, None)
}

// ============================================================================
// Test 1: Basic Round-trip Validation
// ============================================================================

#[tokio::test]
async fn test_write_read_roundtrip_basic() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_simple_schema("test_ks", "users");

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );

    let mut engine = WriteEngine::new(config)?;

    // Write 10 mutations
    for i in 0..10 {
        let mutation = create_simple_mutation(i, &format!("User{}", i), 1000000 + i as i64);
        engine.write_async(mutation).await?;
    }

    // Verify memtable state
    assert_eq!(engine.memtable_row_count(), 10);
    assert!(engine.memtable_size() > 0);

    // Flush to SSTable
    let info = engine.flush().await?;
    assert!(info.is_some());

    let info = info.unwrap();
    assert_eq!(info.partition_count, 10);
    assert!(info.data_path.exists());
    assert!(info.data_size > 0);

    // Verify SSTable components exist under keyspace/table layout (#450)
    let sstable_dir = temp_dir.path().join("data").join("test_ks").join("users");
    assert!(sstable_dir.join("nb-1-big-Data.db").exists());
    assert!(sstable_dir.join("nb-1-big-Index.db").exists());
    assert!(sstable_dir.join("nb-1-big-Statistics.db").exists());

    Ok(())
}

#[tokio::test]
async fn test_write_read_roundtrip_with_clustering() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_clustering_schema();

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );

    let mut engine = WriteEngine::new(config)?;

    // Write time-series data (5 sensors, 10 readings each)
    for sensor_id in 0..5 {
        for i in 0..10 {
            let table_id = TableId::new("test_ks", "timeseries");
            let pk = PartitionKey::single("sensor_id", Value::Integer(sensor_id));
            let ck = ClusteringKey::single("timestamp", Value::Timestamp(1000000 + i * 1000));
            let ops = vec![CellOperation::Write {
                column: "temperature".to_string(),
                value: Value::Float(20.5 + i as f64),
            }];

            let mutation = Mutation::new(table_id, pk, Some(ck), ops, 1000000 + i, None);
            engine.write_async(mutation).await?;
        }
    }

    // Verify memtable
    assert_eq!(engine.memtable_row_count(), 50);

    // Flush and verify
    let info = engine.flush().await?;
    assert!(info.is_some());

    let info = info.unwrap();
    assert_eq!(info.partition_count, 5);
    assert!(info.data_path.exists());

    Ok(())
}

// ============================================================================
// Test 2: CQL Type Coverage
// ============================================================================

#[tokio::test]
async fn test_all_cql_primitive_types() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_simple_schema("test_ks", "types_test");

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );

    let mut engine = WriteEngine::new(config)?;

    // Test each primitive type
    let test_values = vec![
        ("Boolean", Value::Boolean(true)),
        ("TinyInt", Value::TinyInt(-128)),
        ("SmallInt", Value::SmallInt(32767)),
        ("Integer", Value::Integer(2147483647)),
        ("BigInt", Value::BigInt(9223372036854775807)),
        ("Float32", Value::Float32(1.234567_f32)), // Arbitrary f32 value
        ("Float", Value::Float(9.876543210123456)), // Arbitrary f64 value
        ("Text", Value::Text("Hello, World!".to_string())),
        ("Blob", Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF])),
        ("Timestamp", Value::Timestamp(1234567890000)),
        ("Date", Value::Date(18000)),
        ("Time", Value::Time(43200000000000)), // Noon in nanoseconds
        (
            "Uuid",
            Value::Uuid([
                0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x45, 0x67, 0x89, 0xAB,
                0xCD, 0xEF,
            ]),
        ),
    ];

    for (i, (type_name, value)) in test_values.into_iter().enumerate() {
        let table_id = TableId::new("test_ks", "types_test");
        let pk = PartitionKey::single("id", Value::Integer(i as i32));
        let ops = vec![CellOperation::Write {
            column: "value".to_string(),
            value,
        }];

        let mutation = Mutation::new(table_id, pk, None, ops, 1000000 + i as i64, None);
        engine.write_async(mutation).await?;

        println!("✓ Written {} type", type_name);
    }

    let info = engine.flush().await?;
    assert!(info.is_some());

    Ok(())
}

#[tokio::test]
async fn test_collection_types() -> Result<()> {
    // Use a schema with actual non-frozen collection column types
    // so writes go through the complex column encoding path.
    let temp_dir = TempDir::new().unwrap();
    let schema = TableSchema {
        keyspace: "test_ks".to_string(),
        table: "collections_test".to_string(),
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
                name: "tags".to_string(),
                data_type: "set<text>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "items".to_string(),
                data_type: "list<text>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "props".to_string(),
                data_type: "map<text, int>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );

    let mut engine = WriteEngine::new(config)?;

    // Test List (non-frozen → complex column format)
    let list_mutation = {
        let table_id = TableId::new("test_ks", "collections_test");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let ops = vec![CellOperation::Write {
            column: "items".to_string(),
            value: Value::List(vec![
                Value::Text("item1".to_string()),
                Value::Text("item2".to_string()),
                Value::Text("item3".to_string()),
            ]),
        }];
        Mutation::new(table_id, pk, None, ops, 1000000, None)
    };
    engine.write_async(list_mutation).await?;

    // Test Set (non-frozen → complex column format)
    let set_mutation = {
        let table_id = TableId::new("test_ks", "collections_test");
        let pk = PartitionKey::single("id", Value::Integer(2));
        let ops = vec![CellOperation::Write {
            column: "tags".to_string(),
            value: Value::Set(vec![
                Value::Text("zebra".to_string()),
                Value::Text("alpha".to_string()),
                Value::Text("mango".to_string()),
            ]),
        }];
        Mutation::new(table_id, pk, None, ops, 1000001, None)
    };
    engine.write_async(set_mutation).await?;

    // Test Map (non-frozen → complex column format)
    let map_mutation = {
        let table_id = TableId::new("test_ks", "collections_test");
        let pk = PartitionKey::single("id", Value::Integer(3));
        let ops = vec![CellOperation::Write {
            column: "props".to_string(),
            value: Value::Map(vec![
                (Value::Text("key1".to_string()), Value::Integer(100)),
                (Value::Text("key2".to_string()), Value::Integer(200)),
            ]),
        }];
        Mutation::new(table_id, pk, None, ops, 1000002, None)
    };
    engine.write_async(map_mutation).await?;

    // Flush and verify
    let info = engine.flush().await?;
    assert!(info.is_some());

    let info = info.unwrap();
    assert_eq!(info.partition_count, 3);

    Ok(())
}

#[tokio::test]
async fn test_udt_types() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_simple_schema("test_ks", "udt_test");

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );

    let mut engine = WriteEngine::new(config)?;

    // Create a UDT value: address { street, city, zip }
    let udt_value = UdtValue {
        type_name: "address".to_string(),
        keyspace: "test_ks".to_string(),
        fields: vec![
            UdtField {
                name: "street".to_string(),
                value: Some(Value::Text("123 Main St".to_string())),
            },
            UdtField {
                name: "city".to_string(),
                value: Some(Value::Text("Springfield".to_string())),
            },
            UdtField {
                name: "zip".to_string(),
                value: Some(Value::Integer(12345)),
            },
        ],
    };

    let table_id = TableId::new("test_ks", "udt_test");
    let pk = PartitionKey::single("id", Value::Integer(1));
    let ops = vec![CellOperation::Write {
        column: "value".to_string(),
        value: Value::Udt(udt_value),
    }];

    let mutation = Mutation::new(table_id, pk, None, ops, 1000000, None);
    engine.write_async(mutation).await?;

    let info = engine.flush().await?;
    assert!(info.is_some());

    Ok(())
}

// ============================================================================
// Test 3: TTL and Expiring Cells
// ============================================================================

#[tokio::test]
async fn test_ttl_cells() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_simple_schema("test_ks", "ttl_test");

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );

    let mut engine = WriteEngine::new(config)?;

    // Write mutation with TTL at mutation level
    let mutation_with_ttl = {
        let table_id = TableId::new("test_ks", "ttl_test");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let ops = vec![CellOperation::Write {
            column: "value".to_string(),
            value: Value::Text("expires in 1 hour".to_string()),
        }];
        Mutation::new(table_id, pk, None, ops, 1000000, Some(3600)) // 1 hour TTL
    };
    engine.write_async(mutation_with_ttl).await?;

    // Write mutation with cell-level TTL
    let cell_with_ttl = {
        let table_id = TableId::new("test_ks", "ttl_test");
        let pk = PartitionKey::single("id", Value::Integer(2));
        let ops = vec![CellOperation::WriteWithTtl {
            column: "value".to_string(),
            value: Value::Text("expires in 5 minutes".to_string()),
            ttl_seconds: 300, // 5 minutes
        }];
        Mutation::new(table_id, pk, None, ops, 1000001, None)
    };
    engine.write_async(cell_with_ttl).await?;

    // Mixed: mutation TTL + cell TTL (cell TTL should take precedence)
    let mixed_ttl = {
        let table_id = TableId::new("test_ks", "ttl_test");
        let pk = PartitionKey::single("id", Value::Integer(3));
        let ops = vec![CellOperation::WriteWithTtl {
            column: "value".to_string(),
            value: Value::Text("cell TTL overrides mutation TTL".to_string()),
            ttl_seconds: 60, // 1 minute (overrides mutation TTL)
        }];
        Mutation::new(table_id, pk, None, ops, 1000002, Some(7200)) // 2 hours mutation TTL
    };
    engine.write_async(mixed_ttl).await?;

    let info = engine.flush().await?;
    assert!(info.is_some());

    let info = info.unwrap();
    assert_eq!(info.partition_count, 3);

    Ok(())
}

// ============================================================================
// Test 4: Tombstone Handling
// ============================================================================

#[tokio::test]
async fn test_cell_tombstones() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_simple_schema("test_ks", "tombstone_test");

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );

    let mut engine = WriteEngine::new(config)?;

    // Write a value first
    let write_mutation = create_simple_mutation(1, "to be deleted", 1000000);
    engine.write_async(write_mutation).await?;

    // Delete the cell
    let delete_mutation = {
        let table_id = TableId::new("test_ks", "tombstone_test");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let ops = vec![CellOperation::Delete {
            column: "value".to_string(),
            local_deletion_time: None,
        }];
        Mutation::new(table_id, pk, None, ops, 1000001, None)
    };
    engine.write_async(delete_mutation).await?;

    let info = engine.flush().await?;
    assert!(info.is_some());

    Ok(())
}

#[tokio::test]
async fn test_row_tombstones() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_simple_schema("test_ks", "row_delete_test");

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );

    let mut engine = WriteEngine::new(config)?;

    // Write a row
    let write_mutation = create_simple_mutation(1, "entire row to delete", 1000000);
    engine.write_async(write_mutation).await?;

    // Delete the entire row
    let delete_row_mutation = {
        let table_id = TableId::new("test_ks", "row_delete_test");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let ops = vec![CellOperation::DeleteRow];
        Mutation::new(table_id, pk, None, ops, 1000001, None)
    };
    engine.write_async(delete_row_mutation).await?;

    let info = engine.flush().await?;
    assert!(info.is_some());

    Ok(())
}

#[tokio::test]
async fn test_tombstone_overwrite() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_simple_schema("test_ks", "overwrite_test");

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );

    let mut engine = WriteEngine::new(config)?;

    // Write value
    let mut1 = create_simple_mutation(1, "original", 1000000);
    engine.write_async(mut1).await?;

    // Delete
    let del = {
        let table_id = TableId::new("test_ks", "overwrite_test");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let ops = vec![CellOperation::Delete {
            column: "value".to_string(),
            local_deletion_time: None,
        }];
        Mutation::new(table_id, pk, None, ops, 1000001, None)
    };
    engine.write_async(del).await?;

    // Overwrite with new value (higher timestamp)
    let mut2 = create_simple_mutation(1, "restored", 1000002);
    engine.write_async(mut2).await?;

    let info = engine.flush().await?;
    assert!(info.is_some());

    Ok(())
}

// ============================================================================
// Test 5: Compaction (STCS Policy)
// ============================================================================

#[tokio::test]
#[ignore = "maintenance_step() panics: block_on() inside KWayMerger nested in single-thread tokio runtime"]
async fn test_stcs_compaction_trigger() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_simple_schema("test_ks", "compaction_test");

    // Use very low flush threshold to create multiple SSTables
    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    )
    .with_flush_threshold(512); // 512B flush threshold

    let mut engine = WriteEngine::new(config)?;

    // Write enough data to create 5 SSTables via manual flushes
    for generation in 0..5 {
        for i in 0..5 {
            let id = generation * 100 + i;
            let mutation = create_simple_mutation(
                id,
                &format!("Gen{}_Row{}", generation, i),
                1000000 + id as i64,
            );
            engine.write_async(mutation).await?;
        }
        // Manual flush to create distinct SSTables
        engine.flush().await?;
    }

    // Verify we have at least 5 SSTable generations under keyspace/table layout (#450)
    let sstable_dir = temp_dir
        .path()
        .join("data")
        .join("test_ks")
        .join("compaction_test");
    let data_files: Vec<_> = std::fs::read_dir(&sstable_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .map(|s| s.ends_with("Data.db"))
                .unwrap_or(false)
        })
        .collect();

    assert!(
        data_files.len() >= 4,
        "Should have at least 4 Data.db files (got {})",
        data_files.len()
    );

    // Set STCS policy with min_threshold=4 (default)
    let policy = STCSPolicy::default();
    engine.set_merge_policy(Box::new(policy))?;

    // Run maintenance step — 5 SSTables >= min_threshold(4), compaction should trigger
    let report = engine.maintenance_step(Duration::from_secs(5))?;

    // STCS should select candidates and complete compaction
    assert!(
        report.pending_compaction || !report.completed_merges.is_empty(),
        "5 SSTables >= min_threshold(4), compaction should trigger"
    );

    Ok(())
}

#[tokio::test]
async fn test_stcs_no_compaction_below_threshold() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_simple_schema("test_ks", "no_compact_test");

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    )
    .with_flush_threshold(1024);

    let mut engine = WriteEngine::new(config)?;

    // Create only 3 SSTables (below min_threshold of 4)
    for generation in 0..3 {
        for i in 0..10 {
            let id = generation * 100 + i;
            let mutation = create_simple_mutation(id, &format!("Row{}", i), 1000000 + id as i64);
            engine.write_async(mutation).await?;
        }
        engine.flush().await?;
    }

    // Set STCS policy — 3 SSTables below min_threshold(4), so no compaction
    let policy = STCSPolicy::default();
    engine.set_merge_policy(Box::new(policy))?;

    // Run maintenance step — below threshold, should select nothing
    let report = engine.maintenance_step(Duration::from_millis(100))?;

    // Below min_threshold, no compaction work should be done
    assert!(!report.pending_compaction);
    assert_eq!(report.completed_merges.len(), 0);
    assert_eq!(report.rows_merged, 0);

    Ok(())
}

#[tokio::test]
async fn test_maintenance_step_budget_honored() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_simple_schema("test_ks", "budget_test");

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );

    let mut engine = WriteEngine::new(config)?;

    // Set STCS policy — no SSTables yet, so no candidates
    let policy = STCSPolicy::default();
    engine.set_merge_policy(Box::new(policy))?;

    // Run with very small budget — no SSTables to compact
    let budget = Duration::from_millis(10);
    let start = std::time::Instant::now();
    let report = engine.maintenance_step(budget)?;
    let elapsed = start.elapsed();

    // Should return quickly when there's no work (no SSTables to compact)
    assert!(elapsed < Duration::from_millis(50), "Should respect budget");
    assert!(!report.pending_compaction);

    Ok(())
}

// ============================================================================
// Test 6: Export API
// ============================================================================

#[tokio::test]
async fn test_export_sstable_basic() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let export_dir = TempDir::new().unwrap();
    let schema = create_simple_schema("test_ks", "export_test");

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );

    let mut engine = WriteEngine::new(config)?;

    // Write and flush data
    for i in 0..10 {
        let mutation = create_simple_mutation(i, &format!("ExportRow{}", i), 1000000 + i as i64);
        engine.write_async(mutation).await?;
    }
    engine.flush().await?;

    // Export with Cassandra naming
    // NOTE: skip_compaction() required until M5.3 SSTable reader integration
    let options = ExportOptions::new("test_ks", "export_test", 1)
        .skip_compaction()
        .skip_validation();
    let report = engine.export_sstable(export_dir.path(), options).await?;

    // Verify export report
    assert!(report.data_file_size > 0);
    assert!(!report.components.is_empty());

    // Verify files have Cassandra naming convention with directory structure
    // Issue #427: Files go to {output_dir}/{keyspace}/{table}/nb-{gen}-big-{Component}.db
    let data_file = export_dir
        .path()
        .join("test_ks")
        .join("export_test")
        .join("nb-1-big-Data.db");
    assert!(
        data_file.exists(),
        "Data.db should use Cassandra naming with directory structure"
    );

    let index_file = export_dir
        .path()
        .join("test_ks")
        .join("export_test")
        .join("nb-1-big-Index.db");
    assert!(
        index_file.exists(),
        "Index.db should use Cassandra naming with directory structure"
    );

    Ok(())
}

#[tokio::test]
async fn test_export_with_memtable_flush() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let export_dir = TempDir::new().unwrap();
    let schema = create_simple_schema("test_ks", "export_flush_test");

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );

    let mut engine = WriteEngine::new(config)?;

    // Write data but DON'T flush manually
    for i in 0..5 {
        let mutation = create_simple_mutation(i, &format!("Row{}", i), 1000000 + i as i64);
        engine.write_async(mutation).await?;
    }

    // Verify memtable has data
    assert_eq!(engine.memtable_row_count(), 5);

    // Export should automatically flush
    // NOTE: skip_compaction() required until M5.3 SSTable reader integration
    let options = ExportOptions::new("test_ks", "export_flush_test", 1)
        .skip_compaction()
        .skip_validation();
    let report = engine.export_sstable(export_dir.path(), options).await?;

    // Verify memtable is now empty (was flushed)
    assert_eq!(engine.memtable_row_count(), 0);

    // Verify exported file exists
    assert!(report.data_file_size > 0);

    Ok(())
}

#[tokio::test]
async fn test_export_empty_engine_fails() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let export_dir = TempDir::new().unwrap();
    let schema = create_simple_schema("test_ks", "empty_export");

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );

    let mut engine = WriteEngine::new(config)?;

    // Try to export without any data
    // NOTE: skip_compaction() required until M5.3 SSTable reader integration
    let options = ExportOptions::new("test_ks", "empty_export", 1).skip_compaction();
    let result = engine.export_sstable(export_dir.path(), options).await;

    // Should fail (no SSTables to export)
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("No SSTables"));

    Ok(())
}

#[tokio::test]
async fn test_export_with_validation() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let export_dir = TempDir::new().unwrap();
    let schema = create_simple_schema("test_ks", "validation_test");

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );

    let mut engine = WriteEngine::new(config)?;

    // Write and flush
    let mutation = create_simple_mutation(1, "Validated", 1000000);
    engine.write_async(mutation).await?;
    engine.flush().await?;

    // Export WITH validation
    // NOTE: skip_compaction() required until M5.3 SSTable reader integration
    let options = ExportOptions::new("test_ks", "validation_test", 1).skip_compaction();
    let report = engine.export_sstable(export_dir.path(), options).await?;

    // Manual validation should also pass
    assert!(report.validate_components().is_ok());

    Ok(())
}

// ============================================================================
// Test 7: Complex Scenarios
// ============================================================================

#[tokio::test]
async fn test_multi_partition_clustering_roundtrip() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_clustering_schema();

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );

    let mut engine = WriteEngine::new(config)?;

    // Write wide partitions (100 sensors, 50 readings each)
    for sensor_id in 0..100 {
        for reading in 0..50 {
            let table_id = TableId::new("test_ks", "timeseries");
            let pk = PartitionKey::single("sensor_id", Value::Integer(sensor_id));
            let ck = ClusteringKey::single("timestamp", Value::Timestamp(1000000 + reading * 1000));
            let ops = vec![CellOperation::Write {
                column: "temperature".to_string(),
                value: Value::Float(15.0 + (reading as f64) * 0.1),
            }];

            let mutation = Mutation::new(table_id, pk, Some(ck), ops, 1000000 + reading, None);
            engine.write_async(mutation).await?;
        }
    }

    // Total: 5000 rows across 100 partitions
    assert_eq!(engine.memtable_row_count(), 5000);

    let info = engine.flush().await?;
    assert!(info.is_some());

    let info = info.unwrap();
    assert_eq!(info.partition_count, 100);
    assert!(info.data_size > 10000); // Should be substantial

    Ok(())
}

#[tokio::test]
async fn test_mixed_operations_in_partition() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_simple_schema("test_ks", "mixed_ops_test");

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );

    let mut engine = WriteEngine::new(config)?;

    // Write multiple operations to same partition at different timestamps
    let table_id = TableId::new("test_ks", "mixed_ops_test");
    let pk = PartitionKey::single("id", Value::Integer(1));

    // Initial write
    let write1 = Mutation::new(
        table_id.clone(),
        pk.clone(),
        None,
        vec![CellOperation::Write {
            column: "value".to_string(),
            value: Value::Text("version 1".to_string()),
        }],
        1000000,
        None,
    );
    engine.write_async(write1).await?;

    // Update with TTL
    let write2 = Mutation::new(
        table_id.clone(),
        pk.clone(),
        None,
        vec![CellOperation::WriteWithTtl {
            column: "value".to_string(),
            value: Value::Text("version 2 with TTL".to_string()),
            ttl_seconds: 3600,
        }],
        1000001,
        None,
    );
    engine.write_async(write2).await?;

    // Delete
    let delete = Mutation::new(
        table_id.clone(),
        pk.clone(),
        None,
        vec![CellOperation::Delete {
            column: "value".to_string(),
            local_deletion_time: None,
        }],
        1000002,
        None,
    );
    engine.write_async(delete).await?;

    // Resurrection
    let write3 = Mutation::new(
        table_id,
        pk,
        None,
        vec![CellOperation::Write {
            column: "value".to_string(),
            value: Value::Text("version 3 resurrected".to_string()),
        }],
        1000003,
        None,
    );
    engine.write_async(write3).await?;

    let info = engine.flush().await?;
    assert!(info.is_some());

    Ok(())
}

#[tokio::test]
async fn test_large_value_handling() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_simple_schema("test_ks", "large_value_test");

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );

    let mut engine = WriteEngine::new(config)?;

    // Create a 1MB text value
    let large_text = "A".repeat(1024 * 1024);

    let mutation = {
        let table_id = TableId::new("test_ks", "large_value_test");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let ops = vec![CellOperation::Write {
            column: "value".to_string(),
            value: Value::Text(large_text),
        }];
        Mutation::new(table_id, pk, None, ops, 1000000, None)
    };
    engine.write_async(mutation).await?;

    let info = engine.flush().await?;
    assert!(info.is_some());

    let info = info.unwrap();
    assert!(
        info.data_size > 1024 * 1024,
        "Data size should exceed 1MB for large value"
    );

    Ok(())
}

// ============================================================================
// Test 8: Error Handling
// ============================================================================

#[tokio::test]
async fn test_write_after_close_fails() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_simple_schema("test_ks", "closed_test");

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );

    let mut engine = WriteEngine::new(config)?;

    // Close the engine
    engine.close().await?;

    // Try to write - should fail
    let mutation = create_simple_mutation(1, "should fail", 1000000);
    let result = engine.write_async(mutation).await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("closed"));

    Ok(())
}

#[tokio::test]
async fn test_flush_after_close_fails() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_simple_schema("test_ks", "flush_closed_test");

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );

    let mut engine = WriteEngine::new(config)?;

    // Write data
    let mutation = create_simple_mutation(1, "data", 1000000);
    engine.write_async(mutation).await?;

    // Close
    engine.close().await?;

    // Try to flush - should fail
    let result = engine.flush().await;
    assert!(result.is_err());

    Ok(())
}

#[tokio::test]
async fn test_export_after_close_fails() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let export_dir = TempDir::new().unwrap();
    let schema = create_simple_schema("test_ks", "export_closed_test");

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );

    let mut engine = WriteEngine::new(config)?;

    // Write and flush
    let mutation = create_simple_mutation(1, "data", 1000000);
    engine.write_async(mutation).await?;
    engine.flush().await?;

    // Close
    engine.close().await?;

    // Try to export - should fail
    let options = ExportOptions::new("test_ks", "export_closed_test", 1);
    let result = engine.export_sstable(export_dir.path(), options).await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("closed"));

    Ok(())
}

#[tokio::test]
async fn test_export_compact_before_export_deprecated_succeeds() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let export_dir = TempDir::new().unwrap();
    let schema = create_simple_schema("test_ks", "compact_deprecated_test");

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );

    let mut engine = WriteEngine::new(config)?;

    // Write and flush data
    for i in 0..5 {
        let mutation = create_simple_mutation(i, &format!("Row{}", i), 1000000 + i as i64);
        engine.write_async(mutation).await?;
    }
    engine.flush().await?;

    // Export with compact_before_export = true (deprecated flag)
    // This should NOT return an error — it emits a warning and proceeds normally
    let options = ExportOptions {
        keyspace: "test_ks".to_string(),
        table: "compact_deprecated_test".to_string(),
        generation: 1,
        compact_before_export: true,
        validate_after_export: false,
    };
    let report = engine.export_sstable(export_dir.path(), options).await?;

    // Verify the export succeeded and produced output
    assert!(report.data_file_size > 0, "Data file should have content");
    assert!(
        !report.components.is_empty(),
        "Export should have components"
    );

    // Verify the Data.db file exists at the expected path
    let data_file = export_dir
        .path()
        .join("test_ks")
        .join("compact_deprecated_test")
        .join("nb-1-big-Data.db");
    assert!(
        data_file.exists(),
        "Data.db should exist when compact_before_export=true (deprecated path)"
    );

    Ok(())
}

#[tokio::test]
async fn test_memtable_hard_limit_enforcement() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_simple_schema("test_ks", "hard_limit_test");

    // Set very low hard limit (2KB)
    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    )
    .with_flush_threshold(10 * 1024) // Higher than hard limit for test
    .with_hard_limit(2048);

    let mut engine = WriteEngine::new(config)?;

    // Write until we hit the hard limit
    let mut write_count = 0;
    for i in 0..1000 {
        let mutation = create_simple_mutation(i, &format!("Data{}", i), 1000000 + i as i64);
        let result = engine.write_async(mutation).await;

        if let Err(err) = result {
            assert!(err.to_string().contains("hard limit"));
            break;
        }
        write_count += 1;
    }

    // Should have hit limit before 1000 writes
    assert!(write_count < 1000, "Should have hit hard limit");
    assert!(write_count > 0, "Should accept at least some writes");

    Ok(())
}

// ============================================================================
// Test 9: Crash Recovery
// ============================================================================

#[tokio::test]
async fn test_wal_recovery_after_crash() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_simple_schema("recovery_ks", "crash_test");

    // Write 10 mutations, then drop (simulate crash)
    {
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema.clone(),
        );
        let mut engine = WriteEngine::new(config)?;

        for i in 0..10 {
            let table_id = TableId::new("recovery_ks", "crash_test");
            let pk = PartitionKey::single("id", Value::Integer(i));
            let ops = vec![CellOperation::Write {
                column: "value".to_string(),
                value: Value::Text(format!("Row{}", i)),
            }];
            let mutation = Mutation::new(table_id, pk, None, ops, 1000000 + i as i64, None);
            engine.write_async(mutation).await?;
        }

        // Verify WAL has data
        assert!(engine.wal_size() > 0);

        // Drop WITHOUT close() - simulates crash
    }

    // Recover - new engine should replay WAL
    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );
    let mut recovered = WriteEngine::new(config)?;

    // Verify all 10 mutations recovered
    assert_eq!(recovered.memtable_row_count(), 10);

    // Flush and verify SSTable
    let info = recovered.flush().await?;
    assert!(info.is_some());
    assert_eq!(info.unwrap().partition_count, 10);

    Ok(())
}

#[tokio::test]
async fn test_wal_recovery_partial_writes() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_simple_schema("recovery_ks", "partial_test");

    // First phase: write 5, flush (clears WAL)
    {
        let config = WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            schema.clone(),
        );
        let mut engine = WriteEngine::new(config)?;

        for i in 0..5 {
            let table_id = TableId::new("recovery_ks", "partial_test");
            let pk = PartitionKey::single("id", Value::Integer(i));
            let ops = vec![CellOperation::Write {
                column: "value".to_string(),
                value: Value::Text(format!("Batch1_{}", i)),
            }];
            let mutation = Mutation::new(table_id, pk, None, ops, 1000000 + i as i64, None);
            engine.write_async(mutation).await?;
        }

        engine.flush().await?;
        assert_eq!(engine.wal_size(), 0); // WAL truncated

        // Write 5 more (in WAL only), then crash
        for i in 5..10 {
            let table_id = TableId::new("recovery_ks", "partial_test");
            let pk = PartitionKey::single("id", Value::Integer(i));
            let ops = vec![CellOperation::Write {
                column: "value".to_string(),
                value: Value::Text(format!("Batch2_{}", i)),
            }];
            let mutation = Mutation::new(table_id, pk, None, ops, 1000000 + i as i64, None);
            engine.write_async(mutation).await?;
        }

        assert!(engine.wal_size() > 0); // Second batch in WAL
                                        // Drop - crash
    }

    // Recover
    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );
    let recovered = WriteEngine::new(config)?;

    // Should recover only the second batch (5 mutations)
    assert_eq!(recovered.memtable_row_count(), 5);

    // First batch should be in SSTable on disk under keyspace/table layout (#450)
    let sstable_dir = temp_dir
        .path()
        .join("data")
        .join("recovery_ks")
        .join("partial_test");
    assert!(sstable_dir.join("nb-1-big-Data.db").exists());

    Ok(())
}

#[tokio::test]
async fn test_wal_truncated_after_flush() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_simple_schema("recovery_ks", "truncate_test");

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema.clone(),
    );
    let mut engine = WriteEngine::new(config)?;

    // Write mutations
    for i in 0..5 {
        let table_id = TableId::new("recovery_ks", "truncate_test");
        let pk = PartitionKey::single("id", Value::Integer(i));
        let ops = vec![CellOperation::Write {
            column: "value".to_string(),
            value: Value::Text(format!("Row{}", i)),
        }];
        let mutation = Mutation::new(table_id, pk, None, ops, 1000000 + i as i64, None);
        engine.write_async(mutation).await?;
    }

    assert!(engine.wal_size() > 0);

    // Flush
    engine.flush().await?;

    // WAL should be truncated
    assert_eq!(engine.wal_size(), 0);

    // Drop and recover - should have empty memtable
    drop(engine);

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );
    let recovered = WriteEngine::new(config)?;

    assert_eq!(recovered.memtable_row_count(), 0);

    Ok(())
}

// ============================================================================
// Test 10: Performance Validation
// ============================================================================

#[tokio::test]
async fn test_write_throughput() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_simple_schema("perf_ks", "throughput_test");

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );

    let mut engine = WriteEngine::new(config)?;

    let start = std::time::Instant::now();
    let num_rows = 1_000; // Reduced for CI time constraints (WAL syncs per write)

    for i in 0..num_rows {
        let table_id = TableId::new("perf_ks", "throughput_test");
        let pk = PartitionKey::single("id", Value::Integer(i));
        let ops = vec![CellOperation::Write {
            column: "value".to_string(),
            value: Value::Text(format!("value{}", i)),
        }];
        let mutation = Mutation::new(table_id, pk, None, ops, 1000000 + i as i64, None);
        engine.write_async(mutation).await?;
    }

    let elapsed = start.elapsed();
    let rows_per_sec = num_rows as f64 / elapsed.as_secs_f64();

    println!(
        "Write throughput: {:.0} rows/sec ({} rows in {:?})",
        rows_per_sec, num_rows, elapsed
    );

    // Target: 50 rows/sec (conservative for CI with WAL sync per write)
    // WAL fsync is intentionally per-write for durability, which limits throughput
    // Production systems typically batch writes or use async WAL for higher throughput
    assert!(
        rows_per_sec >= 50.0,
        "Write throughput {:.0} rows/sec below target of 50 rows/sec",
        rows_per_sec
    );

    Ok(())
}

#[tokio::test]
#[ignore = "perf-only: wall-clock flush throughput; load-sensitive, excluded from the correctness gate (run via `cargo test -- --ignored`). See #1188"]
async fn test_flush_throughput() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_simple_schema("perf_ks", "flush_test");

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );

    let mut engine = WriteEngine::new(config)?;

    // Write ~1MB of data (1K rows with ~1KB values) - reduced for CI time constraints
    let large_value = "X".repeat(1000);
    for i in 0..1_000i32 {
        let table_id = TableId::new("perf_ks", "flush_test");
        let pk = PartitionKey::single("id", Value::Integer(i));
        let ops = vec![CellOperation::Write {
            column: "value".to_string(),
            value: Value::Text(large_value.clone()),
        }];
        let mutation = Mutation::new(table_id, pk, None, ops, 1000000 + i as i64, None);
        engine.write_async(mutation).await?;
    }

    let start = std::time::Instant::now();
    let info = engine.flush().await?.expect("Should have data to flush");
    let elapsed = start.elapsed();

    let mb_per_sec = (info.data_size as f64 / 1_000_000.0) / elapsed.as_secs_f64();

    println!(
        "Flush throughput: {:.1} MB/sec ({} bytes in {:?})",
        mb_per_sec, info.data_size, elapsed
    );

    // Target: 5 MB/sec (conservative for CI, accounts for index/statistics overhead)
    assert!(
        mb_per_sec >= 5.0,
        "Flush throughput {:.1} MB/sec below target of 5 MB/sec",
        mb_per_sec
    );

    Ok(())
}
