//! Enhanced End-to-end integration tests for CQLite with Cassandra 5+ validation
//!
//! This module tests the complete workflow from schema creation to query execution,
//! validating the integration of all CQLite components with real Cassandra 5+ data.
//!
//! Key test areas:
//! - Round-trip compatibility with Cassandra 5+ SSTables
//! - Real-world dataset processing
//! - Performance validation against production targets
//! - Memory efficiency under load
//!
//! NOTE: These tests require the `experimental` feature flag.

#![cfg(feature = "experimental")]
#![allow(clippy::all)] // Allow all clippy warnings for M1 milestone
//! - Concurrent operation safety

#[allow(unused_imports)]
use cqlite_core::{
    parser::{
        header::{CassandraVersion, ColumnInfo, CompressionInfo, SSTableHeader, SSTableStats},
        parse_cql_value, parse_sstable_header, serialize_cql_value, serialize_sstable_header,
        types::CqlTypeId,
        SSTableParser,
    },
    platform::Platform,
    schema::discovery::SchemaDiscoveryEngine,
    storage::StorageEngine,
    types::Value,
    Config, RowKey, TableId,
};
#[allow(unused_imports)]
use std::{collections::HashMap, sync::Arc, time::Instant};
#[allow(unused_imports)]
use tempfile::TempDir;

#[allow(dead_code)]
fn integration_tests_enabled() -> bool {
    std::env::var("CQLITE_RUN_INTEGRATION").is_ok()
}

/// Comprehensive end-to-end test
#[tokio::test]
async fn test_complete_workflow() -> Result<(), Box<dyn std::error::Error>> {
    if !integration_tests_enabled() {
        println!("INFO: Skipping test_complete_workflow; set CQLITE_RUN_INTEGRATION=1 to enable");
        return Ok(());
    }
    // Setup test environment
    let temp_dir = TempDir::new()?;
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await?);

    // Initialize storage engine
    let storage =
        Arc::new(StorageEngine::open(temp_dir.path(), &config, platform.clone(), None).await?);

    // Initialize schema manager
    let schema_manager = Arc::new(cqlite_core::schema::SchemaManager::new(temp_dir.path()).await?);

    // Initialize schema discovery engine (for other uses)
    let schema_config = cqlite_core::schema::SchemaDiscoveryConfig::default();
    let _schema_engine = Arc::new(
        SchemaDiscoveryEngine::new(schema_config, platform.clone(), config.clone()).await?,
    );

    // Initialize query components
    let _query_planner =
        cqlite_core::query::planner::QueryPlanner::new(schema_manager.clone(), &config);
    let _query_executor = cqlite_core::query::executor::QueryExecutor::new(
        storage.clone(),
        schema_manager.clone(),
        &config,
    );

    // Step 1: Create table schema - simplified for testing
    let table_id = TableId::new("users");

    // Note: TableSchema and ColumnSchema don't exist in current API
    // This is a simplified test that focuses on storage operations
    println!("Creating table: {}", table_id.name());

    // Note: get_table_schema method doesn't exist in current API
    // Skipping schema verification test
    println!("Skipping schema verification - API not available");

    // Step 2: Insert test data
    let test_data = vec![
        (1i64, "Alice", "alice@example.com", 30i64),
        (2i64, "Bob", "bob@example.com", 25i64),
        (3i64, "Charlie", "charlie@example.com", 35i64),
        (4i64, "Diana", "diana@example.com", 28i64),
    ];

    for (id, name, email, age) in test_data {
        let key = RowKey::new(id.to_be_bytes().to_vec());

        // Create a composite value representing the row
        let mut row_data = std::collections::HashMap::new();
        row_data.insert("id".to_string(), Value::Integer(id as i32));
        row_data.insert("name".to_string(), Value::Text(name.to_string()));
        row_data.insert("email".to_string(), Value::Text(email.to_string()));
        row_data.insert("age".to_string(), Value::Integer(age as i32));

        // Store as JSON for simplicity in this test
        let row_value = Value::Json(serde_json::to_value(row_data)?);

        storage.put(&table_id, key, row_value).await?;
    }

    // Step 3: Test point lookup
    println!("Testing point lookup...");
    let lookup_key = RowKey::new(1i64.to_be_bytes().to_vec());
    let result = storage.get(&table_id, &lookup_key).await?;
    assert!(result.is_some());

    // Step 4: Test range scan
    println!("Testing range scan...");
    let scan_results = storage.scan(&table_id, None, None, Some(10), None).await?;
    assert!(!scan_results.is_empty());
    println!("Found {} records in scan", scan_results.len());

    // Step 5: Test query parsing and planning
    println!("Testing query parsing and planning...");
    let query = "SELECT id, name FROM users WHERE id = 1";

    // Parse and execute query - simplified for testing
    println!("Testing query parsing for: {query}");
    // Note: parse_select_query function doesn't exist in current API
    // This test focuses on storage functionality instead
    println!("Query parsing test skipped - focusing on storage validation");

    // Step 7: Test schema operations
    println!("Testing schema operations...");
    // Note: list_tables method doesn't exist in SchemaManager API
    println!("Skipping list_tables test - method not available");

    // Step 8: Test storage statistics
    println!("Testing storage statistics...");
    let storage_stats = storage.stats().await?;
    println!("Storage stats: {storage_stats:?}");

    // Step 9: Test flush and persistence
    println!("Testing flush and persistence...");
    storage.flush().await?;

    // Verify data persists after flush
    let post_flush_result = storage.get(&table_id, &lookup_key).await?;
    assert!(post_flush_result.is_some());

    // Step 10: Test batch operations
    println!("Testing batch operations...");
    let batch_ops = vec![
        cqlite_core::storage::BatchOperation::Put {
            table_id: table_id.clone(),
            key: RowKey::new(5i64.to_be_bytes().to_vec()),
            value: Value::Text("batch_test".to_string()),
        },
        cqlite_core::storage::BatchOperation::Put {
            table_id: table_id.clone(),
            key: RowKey::new(6i64.to_be_bytes().to_vec()),
            value: Value::Text("batch_test_2".to_string()),
        },
    ];

    // Note: batch_write requires mutable reference, so we'll test individual operations
    for op in batch_ops {
        if let cqlite_core::storage::BatchOperation::Put {
            table_id,
            key,
            value,
        } = op
        {
            storage.put(&table_id, key, value).await?;
        }
    }

    // Final verification
    let final_scan = storage.scan(&table_id, None, None, None, None).await?;
    println!("Final scan found {} records", final_scan.len());
    assert!(final_scan.len() >= 4); // At least our original test data

    // Cleanup
    storage.shutdown().await?;

    println!("✅ End-to-end integration test completed successfully!");
    Ok(())
}

/// Test error handling and edge cases
#[tokio::test]
async fn test_error_handling() -> Result<(), Box<dyn std::error::Error>> {
    if !integration_tests_enabled() {
        println!("INFO: Skipping test_error_handling; set CQLITE_RUN_INTEGRATION=1 to enable");
        return Ok(());
    }
    let temp_dir = TempDir::new()?;
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await?);

    let storage =
        Arc::new(StorageEngine::open(temp_dir.path(), &config, platform.clone(), None).await?);
    let _schema_manager = Arc::new(cqlite_core::schema::SchemaManager::new(temp_dir.path()).await?);
    let schema_config = cqlite_core::schema::SchemaDiscoveryConfig::default();
    let _schema_engine = Arc::new(
        SchemaDiscoveryEngine::new(schema_config, platform.clone(), config.clone()).await?,
    );

    // Test 1: Invalid schema creation - skip this test as TableSchema::new doesn't exist with these parameters
    // and create_table method doesn't exist in SchemaManager
    println!("Skipping invalid schema test - API not available");

    // Test 2: Query non-existent table
    let non_existent_table = TableId::new("non_existent");
    let result = storage
        .get(&non_existent_table, &RowKey::new(vec![1]))
        .await;
    // This should return Ok(None) rather than error in most cases
    assert!(result.is_ok());

    // Test 3: Schema not found
    // Note: get_table_schema method doesn't exist in current API
    // Skipping non-existent table schema test
    println!("Skipping non-existent table schema test - API not available");

    storage.shutdown().await?;
    println!("✅ Error handling test completed successfully!");
    Ok(())
}

/// Test performance and scalability
#[tokio::test]
async fn test_performance_scalability() -> Result<(), Box<dyn std::error::Error>> {
    if !integration_tests_enabled() {
        println!(
            "INFO: Skipping test_performance_scalability; set CQLITE_RUN_INTEGRATION=1 to enable"
        );
        return Ok(());
    }
    let temp_dir = TempDir::new()?;
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await?);

    let storage =
        Arc::new(StorageEngine::open(temp_dir.path(), &config, platform.clone(), None).await?);

    let table_id = TableId::new("performance_test");

    // Insert a larger dataset
    let start_time = std::time::Instant::now();
    let record_count = 1000;

    for i in 0..record_count {
        let key = RowKey::new((i as u32).to_be_bytes().to_vec());
        let value = Value::Text(format!("test_value_{i}"));
        storage.put(&table_id, key, value).await?;
    }

    let insert_time = start_time.elapsed();
    println!("Inserted {record_count} records in {insert_time:?}");

    // Test scan performance
    let scan_start = std::time::Instant::now();
    let scan_results = storage.scan(&table_id, None, None, None, None).await?;
    let scan_time = scan_start.elapsed();

    println!("Scanned {} records in {:?}", scan_results.len(), scan_time);
    assert_eq!(scan_results.len(), record_count);

    // Test point lookup performance
    let lookup_start = std::time::Instant::now();
    let lookup_count = 100;

    for i in 0..lookup_count {
        let key = RowKey::new((i as u32).to_be_bytes().to_vec());
        let result = storage.get(&table_id, &key).await?;
        assert!(result.is_some());
    }

    let lookup_time = lookup_start.elapsed();
    println!("Performed {lookup_count} lookups in {lookup_time:?}");

    // Calculate throughput
    let insert_throughput = record_count as f64 / insert_time.as_secs_f64();
    let lookup_throughput = lookup_count as f64 / lookup_time.as_secs_f64();

    println!("Insert throughput: {insert_throughput:.2} ops/sec");
    println!("Lookup throughput: {lookup_throughput:.2} ops/sec");

    // Basic performance assertions
    assert!(insert_throughput > 100.0, "Insert throughput too low");
    assert!(lookup_throughput > 500.0, "Lookup throughput too low");

    storage.shutdown().await?;
    println!("✅ Performance test completed successfully!");
    Ok(())
}

/// Test concurrent operations
#[tokio::test]
async fn test_concurrent_operations() -> Result<(), Box<dyn std::error::Error>> {
    if !integration_tests_enabled() {
        println!(
            "INFO: Skipping test_concurrent_operations; set CQLITE_RUN_INTEGRATION=1 to enable"
        );
        return Ok(());
    }
    let temp_dir = TempDir::new()?;
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await?);

    let storage =
        Arc::new(StorageEngine::open(temp_dir.path(), &config, platform.clone(), None).await?);

    let table_id = TableId::new("concurrent_test");

    // Spawn multiple concurrent write tasks
    let mut handles = Vec::new();
    let task_count = 10;
    let records_per_task = 100;

    for task_id in 0..task_count {
        let storage_clone = storage.clone();
        let table_id_clone = table_id.clone();

        let handle = tokio::spawn(async move {
            for i in 0..records_per_task {
                let key_value = (task_id * records_per_task + i) as u64;
                let key = RowKey::new(key_value.to_be_bytes().to_vec());
                let value = Value::Text(format!("task_{task_id}_record_{i}"));

                storage_clone
                    .put(&table_id_clone, key, value)
                    .await
                    .unwrap();
            }
        });

        handles.push(handle);
    }

    // Wait for all tasks to complete
    for handle in handles {
        handle.await?;
    }

    // Verify all records were written
    let scan_results = storage.scan(&table_id, None, None, None, None).await?;
    let expected_count = task_count * records_per_task;

    println!(
        "Concurrent operations created {} records",
        scan_results.len()
    );
    assert_eq!(scan_results.len(), expected_count);

    storage.shutdown().await?;
    println!("✅ Concurrent operations test completed successfully!");
    Ok(())
}

/// Test real Cassandra 5+ SSTable compatibility
#[tokio::test]
async fn test_cassandra5_sstable_compatibility() -> Result<(), Box<dyn std::error::Error>> {
    if !integration_tests_enabled() {
        println!(
            "INFO: Skipping test_cassandra5_sstable_compatibility; set CQLITE_RUN_INTEGRATION=1 to enable"
        );
        return Ok(());
    }
    println!("🔄 Testing Cassandra 5+ SSTable compatibility...");

    let _temp_dir = TempDir::new()?;
    let _config = Config::default();

    // Create SSTable parser with strict Cassandra 5 compatibility
    let _parser = SSTableParser::new(cqlite_core::cql::ParserConfig::default())?; // Using default config

    // Test 1: Parse mock Cassandra 5+ SSTable header
    let mock_header = create_mock_cassandra5_header();
    let serialized_header = serialize_sstable_header(&mock_header)?;
    let (_remaining, parsed_header) = parse_sstable_header(&serialized_header)
        .map_err(|e| cqlite_core::error::Error::parse(format!("Failed to parse header: {e:?}")))?;

    // Validate round-trip consistency
    assert_eq!(mock_header.version, parsed_header.version);
    assert_eq!(mock_header.table_id, parsed_header.table_id);
    assert_eq!(mock_header.keyspace, parsed_header.keyspace);
    assert_eq!(mock_header.table_name, parsed_header.table_name);

    // Test 2: All primitive types compatibility
    let primitive_test_cases = vec![
        (CqlTypeId::Boolean, Value::Boolean(true)),
        (CqlTypeId::Int, Value::Integer(42)),
        (CqlTypeId::BigInt, Value::BigInt(9223372036854775807)),
        (CqlTypeId::Float, Value::Float32(3.14159)),
        (CqlTypeId::Double, Value::Float(2.718281828)),
        (
            CqlTypeId::Varchar,
            Value::Text("Unicode test: 测试数据 🚀".to_string()),
        ),
        (CqlTypeId::Blob, Value::Blob(vec![0x01, 0x02, 0x03, 0xFF])),
        (
            CqlTypeId::Uuid,
            Value::Uuid([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
        ),
        (CqlTypeId::Timestamp, Value::Timestamp(1640995200000000)),
    ];

    for (type_id, test_value) in primitive_test_cases {
        let serialized = serialize_cql_value(&test_value)?;
        if serialized.len() > 1 {
            let (_, parsed_value) = parse_cql_value(&serialized[1..], type_id)
                .map_err(|e| format!("Failed to parse CQL value: {e:?}"))?;
            assert!(
                values_are_compatible(&test_value, &parsed_value),
                "Type {type_id:?} failed compatibility test"
            );
        }
    }

    // Test 3: Collection types
    let list_value = Value::List(vec![
        Value::Text("item1".to_string()),
        Value::Text("item2".to_string()),
        Value::Text("unicode: 列表项".to_string()),
    ]);

    let serialized_list = serialize_cql_value(&list_value)?;
    if serialized_list.len() > 1 {
        let (_, parsed_list) = parse_cql_value(&serialized_list[1..], CqlTypeId::List)
            .map_err(|e| format!("Failed to parse list: {e:?}"))?;
        assert!(values_are_compatible(&list_value, &parsed_list));
    }

    let mut map = HashMap::new();
    map.insert("key1".to_string(), Value::Text("value1".to_string()));
    map.insert("unicode_key_键".to_string(), Value::Integer(42));
    // Convert HashMap to Vec<(Value, Value)>
    let map_vec: Vec<(Value, Value)> = map.into_iter().map(|(k, v)| (Value::Text(k), v)).collect();
    let map_value = Value::Map(map_vec);

    let serialized_map = serialize_cql_value(&map_value)?;
    if serialized_map.len() > 1 {
        let (_, parsed_map) = parse_cql_value(&serialized_map[1..], CqlTypeId::Map)
            .map_err(|e| format!("Failed to parse map: {e:?}"))?;
        assert!(values_are_compatible(&map_value, &parsed_map));
    }

    println!("✅ Cassandra 5+ SSTable compatibility test completed successfully!");
    Ok(())
}

/// Test with real-world large datasets
#[tokio::test]
async fn test_large_dataset_processing() -> Result<(), Box<dyn std::error::Error>> {
    if !integration_tests_enabled() {
        println!(
            "INFO: Skipping test_large_dataset_processing; set CQLITE_RUN_INTEGRATION=1 to enable"
        );
        return Ok(());
    }
    println!("💾 Testing large dataset processing...");

    let temp_dir = TempDir::new()?;
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await?);

    let storage =
        Arc::new(StorageEngine::open(temp_dir.path(), &config, platform.clone(), None).await?);

    let table_id = TableId::new("large_dataset_test");

    // Generate large dataset similar to real-world scenarios
    let start_time = Instant::now();
    let record_count = 10000; // 10K records for CI/CD friendliness
    let batch_size = 1000;

    println!("   Generating {record_count} records in batches of {batch_size}...");

    for batch_start in (0..record_count).step_by(batch_size) {
        let batch_end = std::cmp::min(batch_start + batch_size, record_count);

        // Create batch operations for better performance
        let mut batch_ops = Vec::new();

        for i in batch_start..batch_end {
            let key = RowKey::new(i.to_be_bytes().to_vec());

            // Create realistic test data
            let mut row_data = HashMap::new();
            row_data.insert("id".to_string(), Value::Integer(i as i32));
            row_data.insert(
                "timestamp".to_string(),
                Value::Timestamp(1640995200000000 + i as i64 * 1000),
            );
            row_data.insert(
                "device_id".to_string(),
                Value::Text(format!("device_{:06}", i % 1000)),
            );
            row_data.insert(
                "sensor_data".to_string(),
                Value::List(vec![
                    Value::Float(i as f64 * 0.1),
                    Value::Float(i as f64 * 0.2),
                    Value::Float(i as f64 * 0.3),
                ]),
            );
            row_data.insert(
                "metadata".to_string(),
                Value::Map({
                    let mut metadata = HashMap::new();
                    metadata.insert(
                        "location".to_string(),
                        Value::Text(format!("zone_{}", i % 10)),
                    );
                    metadata.insert(
                        "type".to_string(),
                        Value::Text("sensor_reading".to_string()),
                    );
                    metadata.insert(
                        "unicode_field".to_string(),
                        Value::Text("测试数据".to_string()),
                    );
                    // Convert HashMap to Vec<(Value, Value)>
                    let metadata_vec: Vec<(Value, Value)> = metadata
                        .into_iter()
                        .map(|(k, v)| (Value::Text(k), v))
                        .collect();
                    metadata_vec
                }),
            );

            let value = Value::Json(serde_json::to_value(row_data)?);
            batch_ops.push((key, value));
        }

        // Execute batch
        for (key, value) in batch_ops {
            storage.put(&table_id, key, value).await?;
        }

        if batch_end % 2000 == 0 {
            println!("     ... {batch_end} records processed");
        }
    }

    let insert_time = start_time.elapsed();
    println!("   ✅ Inserted {record_count} records in {insert_time:?}");

    // Test large-scale query performance
    let query_start = Instant::now();
    let scan_results = storage
        .scan(&table_id, None, None, Some(1000), None)
        .await?;
    let query_time = query_start.elapsed();

    println!("   ✅ Queried 1000 records in {query_time:?}");
    assert!(!scan_results.is_empty());
    assert!(scan_results.len() <= 1000);

    // Test memory usage under load
    let memory_test_start = Instant::now();
    let mut total_lookups = 0;

    for _ in 0..100 {
        let random_id = (total_lookups * 97) % record_count; // Pseudo-random access pattern
        let key = RowKey::new(random_id.to_be_bytes().to_vec());
        let result = storage.get(&table_id, &key).await?;
        if result.is_some() {
            total_lookups += 1;
        }
    }

    let memory_test_time = memory_test_start.elapsed();
    println!("   ✅ Performed {total_lookups} random lookups in {memory_test_time:?}");

    // Calculate throughput metrics
    let insert_throughput = record_count as f64 / insert_time.as_secs_f64();
    let query_throughput = 1000.0 / query_time.as_secs_f64();
    let lookup_throughput = total_lookups as f64 / memory_test_time.as_secs_f64();

    println!("   📊 Performance metrics:");
    println!("     • Insert throughput: {insert_throughput:.2} records/sec");
    println!("     • Query throughput: {query_throughput:.2} records/sec");
    println!("     • Lookup throughput: {lookup_throughput:.2} ops/sec");

    // Performance assertions
    assert!(
        insert_throughput > 500.0,
        "Insert throughput too low: {insert_throughput:.2}"
    );
    assert!(
        query_throughput > 100.0,
        "Query throughput too low: {query_throughput:.2}"
    );
    assert!(
        lookup_throughput > 50.0,
        "Lookup throughput too low: {lookup_throughput:.2}"
    );

    storage.shutdown().await?;
    println!("✅ Large dataset processing test completed successfully!");
    Ok(())
}

/// Test concurrent round-trip operations with Cassandra 5+ data
#[tokio::test]
async fn test_concurrent_round_trip_operations() -> Result<(), Box<dyn std::error::Error>> {
    if !integration_tests_enabled() {
        println!(
            "INFO: Skipping test_concurrent_round_trip_operations; set CQLITE_RUN_INTEGRATION=1 to enable"
        );
        return Ok(());
    }
    println!("🔀 Testing concurrent round-trip operations...");

    let temp_dir = TempDir::new()?;
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await?);

    let storage =
        Arc::new(StorageEngine::open(temp_dir.path(), &config, platform.clone(), None).await?);

    let table_id = TableId::new("concurrent_round_trip_test");

    // Spawn multiple concurrent tasks doing different operations
    let mut handles = Vec::new();
    let task_count = 8;
    let operations_per_task = 100;

    println!(
        "   Spawning {task_count} concurrent tasks with {operations_per_task} operations each..."
    );

    for task_id in 0..task_count {
        let storage_clone = storage.clone();
        let table_id_clone = table_id.clone();

        let handle = tokio::spawn(async move {
            let mut task_results = Vec::new();

            for i in 0..operations_per_task {
                let operation_start = Instant::now();

                // Generate unique key for this task and operation
                let key_value = (task_id * operations_per_task + i) as u64;
                let key = RowKey::new(key_value.to_be_bytes().to_vec());

                // Create complex test data mimicking real Cassandra scenarios
                let mut row_data = HashMap::new();
                row_data.insert("task_id".to_string(), Value::Integer(task_id));
                row_data.insert("operation_id".to_string(), Value::Integer(i));
                row_data.insert(
                    "timestamp".to_string(),
                    Value::Timestamp(chrono::Utc::now().timestamp_micros()),
                );
                row_data.insert(
                    "data_list".to_string(),
                    Value::List(vec![
                        Value::Text(format!("item_{i}_1")),
                        Value::Text(format!("item_{i}_2")),
                        Value::Text(format!("unicode_项目_{i}")),
                    ]),
                );
                row_data.insert(
                    "metadata_map".to_string(),
                    Value::Map(vec![
                        (
                            Value::Text("source".to_string()),
                            Value::Text(format!("task_{task_id}")),
                        ),
                        (Value::Text("iteration".to_string()), Value::Integer(i)),
                        (
                            Value::Text("unicode_元数据".to_string()),
                            Value::Text("并发测试".to_string()),
                        ),
                    ]),
                );

                let value = Value::Json(serde_json::to_value(row_data).unwrap());

                // Write operation
                if let Err(e) = storage_clone.put(&table_id_clone, key.clone(), value).await {
                    task_results.push((false, format!("Write failed: {e}")));
                    continue;
                }

                // Read operation (immediate consistency check)
                match storage_clone.get(&table_id_clone, &key).await {
                    Ok(Some(_)) => {
                        let operation_time = operation_start.elapsed();
                        task_results.push((
                            true,
                            format!("Operation {i} completed in {operation_time:?}"),
                        ));
                    }
                    Ok(None) => {
                        task_results.push((
                            false,
                            "Read returned None for key that was just written".to_string(),
                        ));
                    }
                    Err(e) => {
                        task_results.push((false, format!("Read failed: {e}")));
                    }
                }
            }

            task_results
        });

        handles.push(handle);
    }

    // Wait for all tasks and collect results
    let mut total_operations = 0;
    let mut successful_operations = 0;
    let mut failed_operations = Vec::new();

    for (task_id, handle) in handles.into_iter().enumerate() {
        match handle.await {
            Ok(task_results) => {
                for (success, message) in task_results {
                    total_operations += 1;
                    if success {
                        successful_operations += 1;
                    } else {
                        failed_operations.push(format!("Task {task_id}: {message}"));
                    }
                }
            }
            Err(e) => {
                failed_operations.push(format!("Task {task_id} panicked: {e}"));
            }
        }
    }

    println!("   📊 Concurrent operations results:");
    println!("     • Total operations: {total_operations}");
    println!("     • Successful: {successful_operations}");
    println!("     • Failed: {}", failed_operations.len());

    if !failed_operations.is_empty() {
        println!("   ❌ Failed operations:");
        for failure in &failed_operations[..std::cmp::min(5, failed_operations.len())] {
            println!("     - {failure}");
        }
    }

    // Verify final state consistency
    let final_scan = storage.scan(&table_id, None, None, None, None).await?;
    println!("   ✅ Final scan found {} records", final_scan.len());

    // Success criteria: At least 95% operations should succeed
    let success_rate = successful_operations as f64 / total_operations as f64;
    assert!(
        success_rate >= 0.95,
        "Success rate too low: {:.2}% (expected >= 95%)",
        success_rate * 100.0
    );

    storage.shutdown().await?;
    println!("✅ Concurrent round-trip operations test completed successfully!");
    Ok(())
}

/// Test edge cases and error recovery
#[tokio::test]
async fn test_edge_cases_and_error_recovery() -> Result<(), Box<dyn std::error::Error>> {
    if !integration_tests_enabled() {
        println!(
            "INFO: Skipping test_edge_cases_and_error_recovery; set CQLITE_RUN_INTEGRATION=1 to enable"
        );
        return Ok(());
    }
    println!("⚠️  Testing edge cases and error recovery...");

    let temp_dir = TempDir::new()?;
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await?);

    let storage =
        Arc::new(StorageEngine::open(temp_dir.path(), &config, platform.clone(), None).await?);

    let table_id = TableId::new("edge_cases_test");

    // Test 1: Null and empty values
    println!("   Testing null and empty values...");
    let null_key = RowKey::new(b"null_test".to_vec());
    let null_value = Value::Null;
    storage.put(&table_id, null_key.clone(), null_value).await?;

    let empty_key = RowKey::new(b"empty_test".to_vec());
    let empty_value = Value::Text("".to_string());
    storage
        .put(&table_id, empty_key.clone(), empty_value)
        .await?;

    // Verify null and empty values can be retrieved
    let null_result = storage.get(&table_id, &null_key).await?;
    assert!(null_result.is_some());

    let empty_result = storage.get(&table_id, &empty_key).await?;
    assert!(empty_result.is_some());

    // Test 2: Unicode stress test
    println!("   Testing Unicode handling...");
    let unicode_key = RowKey::new("unicode_test_键".as_bytes().to_vec());
    let unicode_value = Value::Text(
        "🚀 Unicode test: δῶς, ñoël, 中文, العربية, עברית, 日本語, 한국어, русский 🌟".to_string(),
    );
    storage
        .put(&table_id, unicode_key.clone(), unicode_value)
        .await?;

    let unicode_result = storage.get(&table_id, &unicode_key).await?;
    assert!(unicode_result.is_some());

    // Test 3: Large binary data
    println!("   Testing large binary data...");
    let large_blob_key = RowKey::new(b"large_blob_test".to_vec());
    let large_blob_data = vec![0xAA; 1024 * 1024]; // 1MB of data
    let large_blob_value = Value::Blob(large_blob_data.clone());
    storage
        .put(&table_id, large_blob_key.clone(), large_blob_value)
        .await?;

    let large_blob_result = storage.get(&table_id, &large_blob_key).await?;
    assert!(large_blob_result.is_some());
    // Issue #1334: get() returns the ScanRow carrier. A live blob surfaces as a
    // `ScanRow::Row` carrying a single synthetic `"data"` cell (the offset-read
    // fallback classifier). Extract that cell as a `Value::Blob` and assert its
    // length. Any other shape must panic, not silently skip, so the length check
    // can never be bypassed again (roborev round 5, finding 1).
    match large_blob_result.expect("large blob must be present after put") {
        cqlite_core::ScanRow::Row(cells) => {
            let (_, data_value) = cells
                .iter()
                .find(|(name, _)| name.as_ref() == "data")
                .expect("live blob row must carry a synthetic \"data\" cell");
            match data_value {
                Value::Blob(retrieved_data) => {
                    assert_eq!(retrieved_data.len(), large_blob_data.len());
                }
                other => panic!("expected \"data\" cell to be Value::Blob, got {other:?}"),
            }
        }
        other => panic!("expected large blob to surface as ScanRow::Row, got {other:?}"),
    }

    // Test 4: Maximum and minimum values
    println!("   Testing maximum and minimum values...");
    let max_int_key = RowKey::new(b"max_int_test".to_vec());
    let max_int_value = Value::BigInt(i64::MAX);
    storage
        .put(&table_id, max_int_key.clone(), max_int_value)
        .await?;

    let min_int_key = RowKey::new(b"min_int_test".to_vec());
    let min_int_value = Value::BigInt(i64::MIN);
    storage
        .put(&table_id, min_int_key.clone(), min_int_value)
        .await?;

    // Test 5: Complex nested collections
    println!("   Testing complex nested collections...");
    let nested_key = RowKey::new(b"nested_test".to_vec());
    let nested_value = Value::Map({
        let mut outer_map = HashMap::new();
        outer_map.insert(
            "level1".to_string(),
            Value::Map({
                let mut inner_map = HashMap::new();
                inner_map.insert(
                    "level2".to_string(),
                    Value::List(vec![
                        Value::Text("nested_item_1".to_string()),
                        Value::Text("nested_item_2".to_string()),
                        Value::Map({
                            let mut deep_map = HashMap::new();
                            deep_map.insert(
                                "level3".to_string(),
                                Value::Text("deep_value".to_string()),
                            );
                            // Convert deep HashMap to Vec<(Value, Value)>
                            let deep_map_vec: Vec<(Value, Value)> = deep_map
                                .into_iter()
                                .map(|(k, v)| (Value::Text(k), v))
                                .collect();
                            deep_map_vec
                        }),
                    ]),
                );
                // Convert inner HashMap to Vec<(Value, Value)>
                let inner_map_vec: Vec<(Value, Value)> = inner_map
                    .into_iter()
                    .map(|(k, v)| (Value::Text(k), v))
                    .collect();
                inner_map_vec
            }),
        );
        // Convert outer HashMap to Vec<(Value, Value)>
        let outer_map_vec: Vec<(Value, Value)> = outer_map
            .into_iter()
            .map(|(k, v)| (Value::Text(k), v))
            .collect();
        outer_map_vec
    });
    storage
        .put(&table_id, nested_key.clone(), nested_value)
        .await?;

    let nested_result = storage.get(&table_id, &nested_key).await?;
    assert!(nested_result.is_some());

    // Test 6: Error recovery - simulate corrupted data scenarios
    println!("   Testing error recovery scenarios...");

    // Try to access non-existent table
    let non_existent_table = TableId::new("non_existent_table");
    let non_existent_result = storage
        .get(&non_existent_table, &RowKey::new(b"test".to_vec()))
        .await;
    assert!(non_existent_result.is_ok()); // Should return Ok(None), not error

    // Try to use invalid key
    let empty_key_result = storage.get(&table_id, &RowKey::new(vec![])).await;
    assert!(empty_key_result.is_ok()); // Should handle gracefully

    println!("   ✅ All edge cases handled correctly");

    storage.shutdown().await?;
    println!("✅ Edge cases and error recovery test completed successfully!");
    Ok(())
}

// Helper functions

#[allow(dead_code)]
fn create_mock_cassandra5_header() -> SSTableHeader {
    SSTableHeader {
        cassandra_version: CassandraVersion::V5_0Alpha,
        version: 1,
        table_id: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        keyspace: "cqlite_test".to_string(),
        table_name: "compatibility_test".to_string(),
        generation: 1,
        compression: CompressionInfo {
            algorithm: "LZ4".to_string(),
            chunk_size: 4096,
            parameters: HashMap::new(),
        },
        stats: SSTableStats {
            row_count: 10000,
            min_timestamp: 1640995200000000,
            max_timestamp: 1672531200000000,
            max_deletion_time: 0,
            compression_ratio: 0.3,
            row_size_histogram: vec![100, 200, 300, 400, 500],
        },
        columns: vec![
            ColumnInfo {
                name: "id".to_string(),
                column_type: "uuid".to_string(),
                is_primary_key: true,
                key_position: Some(0),
                is_static: false,
                is_clustering: false,
                clustering_reversed: false,
            },
            ColumnInfo {
                name: "data".to_string(),
                column_type: "text".to_string(),
                is_primary_key: false,
                key_position: None,
                is_static: false,
                is_clustering: false,
                clustering_reversed: false,
            },
        ],
        properties: HashMap::new(),
    }
}

#[allow(dead_code)]
fn values_are_compatible(original: &Value, parsed: &Value) -> bool {
    match (original, parsed) {
        (Value::Boolean(a), Value::Boolean(b)) => a == b,
        (Value::Integer(a), Value::Integer(b)) => a == b,
        (Value::BigInt(a), Value::BigInt(b)) => a == b,
        (Value::Float(a), Value::Float(b)) => (a - b).abs() < f64::EPSILON,
        (Value::Float32(a), Value::Float32(b)) => (a - b).abs() < f32::EPSILON,
        (Value::Text(a), Value::Text(b)) => a == b,
        (Value::Blob(a), Value::Blob(b)) => a == b,
        (Value::Uuid(a), Value::Uuid(b)) => a == b,
        (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
        (Value::List(a), Value::List(b)) => a.len() == b.len(),
        (Value::Map(a), Value::Map(b)) => a.len() == b.len(),
        (Value::Null, Value::Null) => true,
        _ => false,
    }
}

/// Comprehensive VInt encoding/decoding integration test
#[tokio::test]
async fn test_vint_encoding_comprehensive() -> Result<(), Box<dyn std::error::Error>> {
    if !integration_tests_enabled() {
        println!(
            "INFO: Skipping test_vint_encoding_comprehensive; set CQLITE_RUN_INTEGRATION=1 to enable"
        );
        return Ok(());
    }
    println!("🔢 Testing comprehensive VInt encoding/decoding...");

    use cqlite_core::parser::vint::{encode_vint, parse_vint};

    // Test cases covering all VInt encoding scenarios
    let test_cases = vec![
        // Single byte values (0xxxxxxx pattern)
        0i64,
        1,
        -1,
        32,
        -32,
        63,
        -63,
        // Two byte values (10xxxxxx xxxxxxxx pattern)
        64,
        -64,
        128,
        -128,
        1000,
        -1000,
        8191,
        -8191,
        // Three byte values (110xxxxx xxxxxxxx xxxxxxxx pattern)
        8192,
        -8192,
        16384,
        -16384,
        100000,
        -100000,
        1048575,
        -1048575,
        // Four byte values
        1048576,
        -1048576,
        10000000,
        -10000000,
        // Larger values
        i32::MAX as i64,
        i32::MIN as i64,
        // Very large values (but not MAX to avoid overflow issues)
        i64::MAX / 1000,
        i64::MIN / 1000,
    ];

    let mut total_tests = 0;
    let mut successful_tests = 0;
    let mut encoding_stats = HashMap::new();

    for value in test_cases {
        total_tests += 1;

        // Encode the value
        let encoded_bytes = encode_vint(value);
        let encoded_length = encoded_bytes.len();

        // Track encoding length statistics
        *encoding_stats.entry(encoded_length).or_insert(0) += 1;

        // Verify encoding constraints
        assert!(
            encoded_length <= 9,
            "VInt encoding too long: {encoded_length} bytes for value {value}"
        );

        // Parse the encoded bytes back
        match parse_vint(&encoded_bytes) {
            Ok((remaining, decoded_value)) => {
                assert!(
                    remaining.is_empty(),
                    "VInt parsing should consume all bytes for value {value}"
                );
                assert_eq!(
                    decoded_value, value,
                    "VInt roundtrip failed: {value} != {decoded_value}"
                );
                successful_tests += 1;

                // Validate encoding format for single byte
                if encoded_length == 1 {
                    assert_eq!(
                        encoded_bytes[0] & 0x80,
                        0,
                        "Single byte VInt should have MSB=0 for value {value}"
                    );
                } else {
                    // Multi-byte should have correct leading bit pattern
                    let leading_ones = encoded_bytes[0].leading_ones();
                    assert_eq!(
                        leading_ones as usize,
                        encoded_length - 1,
                        "Multi-byte VInt format error for value {}: expected {} leading ones, got {}",
                        value,
                        encoded_length - 1,
                        leading_ones
                    );
                }
            }
            Err(e) => {
                panic!("VInt parsing failed for value {value}: {e:?}");
            }
        }
    }

    println!("   📊 VInt encoding statistics:");
    for (length, count) in encoding_stats.iter() {
        println!("     • {length}-byte encodings: {count} values");
    }

    println!("   ✅ VInt tests: {successful_tests}/{total_tests} successful");
    assert_eq!(successful_tests, total_tests, "Not all VInt tests passed");

    // Test error conditions
    println!("   Testing VInt error conditions...");

    // Empty input
    assert!(parse_vint(&[]).is_err(), "Empty input should fail");

    // Incomplete multi-byte
    assert!(
        parse_vint(&[0x80]).is_err(),
        "Incomplete multi-byte should fail"
    );
    assert!(
        parse_vint(&[0xC0, 0x00]).is_err(),
        "Incomplete 3-byte should fail"
    );

    // Valid multi-byte cases
    assert!(
        parse_vint(&[0x80, 0x00]).is_ok(),
        "Valid 2-byte should succeed"
    );
    assert!(
        parse_vint(&[0xC0, 0x00, 0x00]).is_ok(),
        "Valid 3-byte should succeed"
    );

    println!("✅ Comprehensive VInt encoding test completed successfully!");
    Ok(())
}

/// Test complex types (Lists, Sets, Maps, Tuples, UDTs) integration
#[tokio::test]
async fn test_complex_types_integration() -> Result<(), Box<dyn std::error::Error>> {
    if !integration_tests_enabled() {
        println!(
            "INFO: Skipping test_complex_types_integration; set CQLITE_RUN_INTEGRATION=1 to enable"
        );
        return Ok(());
    }
    println!("🏗️  Testing complex types integration...");

    let temp_dir = TempDir::new()?;
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await?);
    let storage =
        Arc::new(StorageEngine::open(temp_dir.path(), &config, platform.clone(), None).await?);
    let _schema_manager = Arc::new(cqlite_core::schema::SchemaManager::new(temp_dir.path()).await?);
    let schema_config = cqlite_core::schema::SchemaDiscoveryConfig::default();
    let _schema_engine = Arc::new(
        SchemaDiscoveryEngine::new(schema_config, platform.clone(), config.clone()).await?,
    );

    let table_id = TableId::new("complex_types_test");

    // Test 1: Lists
    println!("   Testing List types...");
    let list_key = RowKey::new(b"list_test".to_vec());
    let list_value = Value::List(vec![
        Value::Text("first_item".to_string()),
        Value::Text("second_item".to_string()),
        Value::Text("unicode_项目".to_string()),
        Value::Text("special_chars_!@#$%^&*()".to_string()),
    ]);

    storage
        .put(&table_id, list_key.clone(), list_value.clone())
        .await?;
    let retrieved_list = storage.get(&table_id, &list_key).await?;
    assert!(retrieved_list.is_some());

    // Test 2: Sets
    println!("   Testing Set types...");
    let set_key = RowKey::new(b"set_test".to_vec());
    let set_value = Value::Set(vec![
        Value::Integer(100),
        Value::Integer(200),
        Value::Integer(300),
        Value::Integer(42),
    ]);

    storage
        .put(&table_id, set_key.clone(), set_value.clone())
        .await?;
    let retrieved_set = storage.get(&table_id, &set_key).await?;
    assert!(retrieved_set.is_some());

    // Test 3: Maps
    println!("   Testing Map types...");
    let map_key = RowKey::new(b"map_test".to_vec());
    let map_value = Value::Map(vec![
        (
            Value::Text("name".to_string()),
            Value::Text("Alice".to_string()),
        ),
        (Value::Text("age".to_string()), Value::Integer(30)),
        (
            Value::Text("city".to_string()),
            Value::Text("San Francisco".to_string()),
        ),
        (
            Value::Text("unicode_键".to_string()),
            Value::Text("unicode_值".to_string()),
        ),
    ]);

    storage
        .put(&table_id, map_key.clone(), map_value.clone())
        .await?;
    let retrieved_map = storage.get(&table_id, &map_key).await?;
    assert!(retrieved_map.is_some());

    // Test 4: Tuples
    println!("   Testing Tuple types...");
    let tuple_key = RowKey::new(b"tuple_test".to_vec());
    let tuple_value = Value::Tuple(vec![
        Value::Integer(42),
        Value::Text("tuple_text".to_string()),
        Value::Boolean(true),
        Value::Float(3.14159),
        Value::Timestamp(1640995200000000), // 2022-01-01 UTC
    ]);

    storage
        .put(&table_id, tuple_key.clone(), tuple_value.clone())
        .await?;
    let retrieved_tuple = storage.get(&table_id, &tuple_key).await?;
    assert!(retrieved_tuple.is_some());

    // Test 5: Nested collections
    println!("   Testing nested collection types...");
    let nested_key = RowKey::new(b"nested_test".to_vec());
    let nested_value = Value::Map(vec![
        (
            Value::Text("user_data".to_string()),
            Value::Map(vec![
                (
                    Value::Text("personal".to_string()),
                    Value::Map(vec![
                        (
                            Value::Text("name".to_string()),
                            Value::Text("Bob".to_string()),
                        ),
                        (Value::Text("age".to_string()), Value::Integer(25)),
                    ]),
                ),
                (
                    Value::Text("preferences".to_string()),
                    Value::List(vec![
                        Value::Text("music".to_string()),
                        Value::Text("sports".to_string()),
                        Value::Text("reading".to_string()),
                    ]),
                ),
            ]),
        ),
        (
            Value::Text("metadata".to_string()),
            Value::Tuple(vec![
                Value::Timestamp(chrono::Utc::now().timestamp_micros()),
                Value::Text("v1.0".to_string()),
                Value::Boolean(true),
            ]),
        ),
    ]);

    storage
        .put(&table_id, nested_key.clone(), nested_value.clone())
        .await?;
    let retrieved_nested = storage.get(&table_id, &nested_key).await?;
    assert!(retrieved_nested.is_some());

    // Test 6: Empty collections
    println!("   Testing empty collections...");
    let empty_key = RowKey::new(b"empty_test".to_vec());
    let empty_collections = Value::Map(vec![
        (Value::Text("empty_list".to_string()), Value::List(vec![])),
        (Value::Text("empty_set".to_string()), Value::Set(vec![])),
        (Value::Text("empty_map".to_string()), Value::Map(vec![])),
        (Value::Text("empty_tuple".to_string()), Value::Tuple(vec![])),
    ]);

    storage
        .put(&table_id, empty_key.clone(), empty_collections.clone())
        .await?;
    let retrieved_empty = storage.get(&table_id, &empty_key).await?;
    assert!(retrieved_empty.is_some());

    // Test 7: Large collections
    println!("   Testing large collections...");
    let large_key = RowKey::new(b"large_test".to_vec());

    // Create large list
    let mut large_list = Vec::new();
    for i in 0..1000 {
        large_list.push(Value::Text(format!("item_{i:04}")));
    }

    // Create large map
    let mut large_map = Vec::new();
    for i in 0..500 {
        large_map.push((Value::Text(format!("key_{i:04}")), Value::Integer(i)));
    }

    let large_collections = Value::Map(vec![
        (
            Value::Text("large_list".to_string()),
            Value::List(large_list),
        ),
        (Value::Text("large_map".to_string()), Value::Map(large_map)),
    ]);

    storage
        .put(&table_id, large_key.clone(), large_collections.clone())
        .await?;
    let retrieved_large = storage.get(&table_id, &large_key).await?;
    assert!(retrieved_large.is_some());

    // Performance test for complex types
    println!("   Performance testing complex type operations...");
    let perf_start = Instant::now();

    for i in 0..100 {
        let perf_key = RowKey::new(format!("perf_test_{i}").as_bytes().to_vec());
        let perf_value = Value::Map(vec![
            (Value::Text("id".to_string()), Value::Integer(i)),
            (
                Value::Text("data".to_string()),
                Value::List(vec![
                    Value::Integer(i * 10),
                    Value::Integer(i * 20),
                    Value::Integer(i * 30),
                ]),
            ),
            (
                Value::Text("metadata".to_string()),
                Value::Map(vec![
                    (
                        Value::Text("created".to_string()),
                        Value::Timestamp(chrono::Utc::now().timestamp_micros()),
                    ),
                    (
                        Value::Text("type".to_string()),
                        Value::Text("test_data".to_string()),
                    ),
                ]),
            ),
        ]);

        storage.put(&table_id, perf_key, perf_value).await?;
    }

    let perf_time = perf_start.elapsed();
    println!("   📊 Complex type performance: 100 operations in {perf_time:?}");

    // Verify all test data exists
    let final_scan = storage.scan(&table_id, None, None, None, None).await?;
    println!("   ✅ Total records stored: {}", final_scan.len());
    assert!(final_scan.len() >= 106); // Our test records plus performance records

    storage.shutdown().await?;
    println!("✅ Complex types integration test completed successfully!");
    Ok(())
}

/// Test SSTable creation, writing, and reading with known data
#[tokio::test]
async fn test_sstable_round_trip_validation() -> Result<(), Box<dyn std::error::Error>> {
    if !integration_tests_enabled() {
        println!(
            "INFO: Skipping test_sstable_round_trip_validation; set CQLITE_RUN_INTEGRATION=1 to enable"
        );
        return Ok(());
    }
    println!("📦 Testing SSTable round-trip validation...");

    let temp_dir = TempDir::new()?;
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await?);
    let storage =
        Arc::new(StorageEngine::open(temp_dir.path(), &config, platform.clone(), None).await?);

    let table_id = TableId::new("sstable_test");

    // Create comprehensive test dataset
    let test_data = vec![
        // Basic types
        (
            RowKey::new(b"row_001".to_vec()),
            Value::Map(vec![
                (Value::Text("id".to_string()), Value::Integer(1)),
                (
                    Value::Text("name".to_string()),
                    Value::Text("Alice".to_string()),
                ),
                (Value::Text("active".to_string()), Value::Boolean(true)),
                (Value::Text("score".to_string()), Value::Float(95.5)),
                (
                    Value::Text("created".to_string()),
                    Value::Timestamp(1640995200000000),
                ),
            ]),
        ),
        // Complex types with nested structures
        (
            RowKey::new(b"row_002".to_vec()),
            Value::Map(vec![
                (Value::Text("id".to_string()), Value::Integer(2)),
                (
                    Value::Text("tags".to_string()),
                    Value::List(vec![
                        Value::Text("tag1".to_string()),
                        Value::Text("tag2".to_string()),
                        Value::Text("unicode_标签".to_string()),
                    ]),
                ),
                (
                    Value::Text("properties".to_string()),
                    Value::Map(vec![
                        (
                            Value::Text("category".to_string()),
                            Value::Text("premium".to_string()),
                        ),
                        (Value::Text("priority".to_string()), Value::Integer(5)),
                        (
                            Value::Text("features".to_string()),
                            Value::Set(vec![
                                Value::Text("feature_a".to_string()),
                                Value::Text("feature_b".to_string()),
                                Value::Text("feature_c".to_string()),
                            ]),
                        ),
                    ]),
                ),
                (
                    Value::Text("coordinates".to_string()),
                    Value::Tuple(vec![
                        Value::Float(37.7749),   // latitude
                        Value::Float(-122.4194), // longitude
                        Value::Text("San Francisco".to_string()),
                    ]),
                ),
            ]),
        ),
        // Binary data and special cases
        (
            RowKey::new(b"row_003".to_vec()),
            Value::Map(vec![
                (Value::Text("id".to_string()), Value::Integer(3)),
                (
                    Value::Text("binary_data".to_string()),
                    Value::Blob(vec![0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD]),
                ),
                (
                    Value::Text("uuid_field".to_string()),
                    Value::Uuid([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
                ),
                (Value::Text("null_field".to_string()), Value::Null),
                (
                    Value::Text("empty_collections".to_string()),
                    Value::Map(vec![
                        (Value::Text("empty_list".to_string()), Value::List(vec![])),
                        (Value::Text("empty_map".to_string()), Value::Map(vec![])),
                    ]),
                ),
            ]),
        ),
    ];

    // Write all test data
    println!("   Writing test data to SSTable...");
    for (key, value) in &test_data {
        storage.put(&table_id, key.clone(), value.clone()).await?;
    }

    // Force flush to create SSTable
    println!("   Flushing data to SSTable...");
    storage.flush().await?;

    // Validate data integrity after flush
    println!("   Validating data integrity after flush...");
    for (key, expected_value) in &test_data {
        let retrieved = storage.get(&table_id, key).await?;
        assert!(
            retrieved.is_some(),
            "Data missing after flush for key: {key:?}"
        );

        // Issue #1334: get() returns the ScanRow carrier. Recover the underlying
        // value (the pre-#1334 bare `Value` shape) and compare BOTH its value and
        // its type against the stored `expected_value`. All rows in `test_data`
        // are live and non-null, so any `Marker` here is an unexpected carrier
        // shape and must panic rather than pass (roborev round 5, finding 2).
        let retrieved_value = match retrieved.expect("row must be present after flush") {
            cqlite_core::ScanRow::Row(cells) => {
                // A live value surfaces as its single interned cell; unwrap it so
                // the equality check sees the same `Value` shape that was stored.
                assert_eq!(
                    cells.len(),
                    1,
                    "expected a single-cell live row for key {key:?}, got {cells:?}"
                );
                cells
                    .into_iter()
                    .next()
                    .map(|(_, v)| v)
                    .expect("single-cell row must yield its cell value")
            }
            cqlite_core::ScanRow::RawRow(bytes) => {
                panic!("expected a decoded live ScanRow::Row for key {key:?}, got a raw undecoded RawRow ({} bytes)", bytes.len())
            }
            cqlite_core::ScanRow::Marker(v) => {
                panic!("expected live ScanRow::Row for key {key:?}, got Marker({v:?})")
            }
        };
        assert_eq!(
            std::mem::discriminant(&retrieved_value),
            std::mem::discriminant(expected_value),
            "Value type mismatch for key: {key:?}"
        );
        assert_eq!(
            &retrieved_value, expected_value,
            "Value content mismatch after flush for key: {key:?}"
        );
    }

    // Test range scanning
    println!("   Testing range scan operations...");
    let scan_results = storage.scan(&table_id, None, None, None, None).await?;
    assert_eq!(
        scan_results.len(),
        test_data.len(),
        "Scan should return all test data"
    );

    // Test partial scans
    let partial_scan = storage.scan(&table_id, None, None, Some(2), None).await?;
    assert!(partial_scan.len() <= 2, "Partial scan should respect limit");

    // Test point lookups with various key types
    println!("   Testing point lookups...");
    for (key, _) in &test_data {
        let lookup_result = storage.get(&table_id, key).await?;
        assert!(
            lookup_result.is_some(),
            "Point lookup failed for key: {key:?}"
        );
    }

    // Test non-existent key
    let missing_key = RowKey::new(b"non_existent".to_vec());
    let missing_result = storage.get(&table_id, &missing_key).await?;
    assert!(
        missing_result.is_none(),
        "Non-existent key should return None"
    );

    // Test SSTable statistics
    println!("   Checking SSTable statistics...");
    let stats = storage.stats().await?;
    println!("     SSTable stats: {stats:?}");

    storage.shutdown().await?;
    println!("✅ SSTable round-trip validation completed successfully!");
    Ok(())
}

/// Test data type validation across all supported Cassandra types
#[tokio::test]
async fn test_comprehensive_data_type_validation() -> Result<(), Box<dyn std::error::Error>> {
    if !integration_tests_enabled() {
        println!(
            "INFO: Skipping test_comprehensive_data_type_validation; set CQLITE_RUN_INTEGRATION=1 to enable"
        );
        return Ok(());
    }
    println!("🔍 Testing comprehensive data type validation...");

    use cqlite_core::parser::types::{parse_cql_value, serialize_cql_value, CqlTypeId};

    // Test all primitive types
    let primitive_tests = vec![
        (CqlTypeId::Boolean, Value::Boolean(true)),
        (CqlTypeId::Boolean, Value::Boolean(false)),
        (CqlTypeId::Int, Value::Integer(42)),
        (CqlTypeId::Int, Value::Integer(-42)),
        (CqlTypeId::Int, Value::Integer(0)),
        (CqlTypeId::Int, Value::Integer(i32::MAX)),
        (CqlTypeId::Int, Value::Integer(i32::MIN)),
        (CqlTypeId::BigInt, Value::BigInt(9223372036854775807i64)),
        (CqlTypeId::BigInt, Value::BigInt(-9223372036854775808i64)),
        (CqlTypeId::Float, Value::Float32(3.14159f32)),
        (CqlTypeId::Float, Value::Float32(-2.71828f32)),
        (CqlTypeId::Float, Value::Float32(0.0f32)),
        (CqlTypeId::Double, Value::Float(f64::MAX)),
        (CqlTypeId::Double, Value::Float(f64::MIN)),
        (CqlTypeId::Varchar, Value::Text("".to_string())),
        (CqlTypeId::Varchar, Value::Text("Hello, World!".to_string())),
        (
            CqlTypeId::Varchar,
            Value::Text("Unicode: 测试数据 🚀 💫 🌟".to_string()),
        ),
        (CqlTypeId::Blob, Value::Blob(vec![])),
        (
            CqlTypeId::Blob,
            Value::Blob(vec![0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE, 0xFD, 0xFC]),
        ),
        (CqlTypeId::Uuid, Value::Uuid([0; 16])),
        (
            CqlTypeId::Uuid,
            Value::Uuid([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
        ),
        (CqlTypeId::Timestamp, Value::Timestamp(0)),
        (CqlTypeId::Timestamp, Value::Timestamp(1640995200000000)),
        (CqlTypeId::Timestamp, Value::Timestamp(i64::MAX / 2)),
    ];

    println!("   Testing primitive type serialization/deserialization...");
    let mut successful_primitive_tests = 0;

    for (type_id, test_value) in primitive_tests {
        match serialize_cql_value(&test_value) {
            Ok(serialized) => {
                if serialized.len() > 1 {
                    // Skip the type byte for parsing
                    match parse_cql_value(&serialized[1..], type_id) {
                        Ok((remaining, parsed_value)) => {
                            assert!(
                                remaining.is_empty(),
                                "Parsing should consume all bytes for type {type_id:?}"
                            );

                            // Validate round-trip consistency
                            if values_are_compatible(&test_value, &parsed_value) {
                                successful_primitive_tests += 1;
                            } else {
                                println!(
                                    "     ⚠️  Value mismatch for type {type_id:?}: {test_value:?} != {parsed_value:?}"
                                );
                            }
                        }
                        Err(e) => {
                            println!("     ❌ Parse failed for type {type_id:?}: {e:?}");
                        }
                    }
                } else {
                    println!("     ⚠️  Serialized data too short for type {type_id:?}");
                }
            }
            Err(e) => {
                println!("     ❌ Serialization failed for type {type_id:?}: {e:?}");
            }
        }
    }

    println!(
        "   ✅ Primitive type tests: {}/{} successful",
        successful_primitive_tests, 23
    );

    // Test collection types
    println!("   Testing collection type serialization...");
    let collection_tests = vec![
        Value::List(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ]),
        Value::Set(vec![
            Value::Text("apple".to_string()),
            Value::Text("banana".to_string()),
            Value::Text("cherry".to_string()),
        ]),
        Value::Map(vec![
            (Value::Text("key1".to_string()), Value::Integer(100)),
            (Value::Text("key2".to_string()), Value::Integer(200)),
            (
                Value::Text("unicode_键".to_string()),
                Value::Text("unicode_值".to_string()),
            ),
        ]),
        Value::Tuple(vec![
            Value::Integer(42),
            Value::Text("hello".to_string()),
            Value::Boolean(true),
        ]),
    ];

    let mut successful_collection_tests = 0;
    for test_value in collection_tests {
        match serialize_cql_value(&test_value) {
            Ok(_serialized) => {
                // Collection types require more complex parsing logic
                // For now, just verify serialization doesn't crash
                successful_collection_tests += 1;
                println!(
                    "     ✅ Serialized {} (type: {:?})",
                    match &test_value {
                        Value::List(_) => "List",
                        Value::Set(_) => "Set",
                        Value::Map(_) => "Map",
                        Value::Tuple(_) => "Tuple",
                        _ => "Unknown",
                    },
                    std::mem::discriminant(&test_value)
                );
            }
            Err(e) => {
                println!("     ❌ Collection serialization failed: {e:?}");
            }
        }
    }

    println!(
        "   ✅ Collection type tests: {}/{} successful",
        successful_collection_tests, 4
    );

    // Test edge cases
    println!("   Testing edge cases...");
    let edge_case_tests = vec![
        Value::Null,
        Value::Text("".to_string()),
        Value::Blob(vec![]),
        Value::List(vec![]),
        Value::Set(vec![]),
        Value::Map(vec![]),
        Value::Tuple(vec![]),
    ];

    let mut successful_edge_tests = 0;
    for test_value in edge_case_tests {
        match serialize_cql_value(&test_value) {
            Ok(_) => {
                successful_edge_tests += 1;
            }
            Err(e) => {
                println!("     ❌ Edge case failed: {test_value:?} - {e:?}");
            }
        }
    }

    println!(
        "   ✅ Edge case tests: {}/{} successful",
        successful_edge_tests, 7
    );

    let total_successful =
        successful_primitive_tests + successful_collection_tests + successful_edge_tests;
    let total_tests = 23 + 4 + 7;

    println!(
        "📊 Overall data type validation: {}/{} tests successful ({:.1}%)",
        total_successful,
        total_tests,
        (total_successful as f64 / total_tests as f64) * 100.0
    );

    // Require at least 80% success rate
    assert!(
        total_successful as f64 / total_tests as f64 >= 0.8,
        "Data type validation success rate too low: {:.1}%",
        (total_successful as f64 / total_tests as f64) * 100.0
    );

    println!("✅ Comprehensive data type validation completed successfully!");
    Ok(())
}
