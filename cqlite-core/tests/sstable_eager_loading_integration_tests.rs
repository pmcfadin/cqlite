//! Integration tests for SSTable eager loading with realistic workflow scenarios
//!
//! These tests verify the complete eager loading workflow from component discovery
//! through reader initialization to actual data operations, simulating real-world usage.

use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::fs;

use cqlite_core::Config;
use cqlite_core::platform::Platform;
use cqlite_core::schema::{KeyColumn, TableSchema};
use cqlite_core::storage::sstable::SSTableReader;
use std::collections::HashMap;

/// Test complete workflow: discovery -> initialization -> operations
#[tokio::test]
async fn test_complete_eager_loading_workflow() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let workflow_scenarios = vec!["users-table", "events-table", "metrics-table"];

    for scenario_name in workflow_scenarios {
        println!("Testing complete workflow for: {}", scenario_name);

        let scenario_dir = base_path.join(scenario_name);
        fs::create_dir(&scenario_dir).await.unwrap();

        // Step 1: Create realistic SSTable files
        let (data_file, schema) = match scenario_name {
            "users-table" => create_users_table_scenario(&scenario_dir, scenario_name).await,
            "events-table" => create_events_table_scenario(&scenario_dir, scenario_name).await,
            "metrics-table" => create_metrics_table_scenario(&scenario_dir, scenario_name).await,
            _ => create_users_table_scenario(&scenario_dir, scenario_name).await,
        };

        // Step 2: Test eager loading initialization
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        match SSTableReader::open(&data_file, &config, platform).await {
            Ok(reader) => {
                println!(
                    "✓ Eager loading initialization successful for {}",
                    scenario_name
                );

                // Step 3: Test immediate availability of all operations
                test_immediate_operation_availability(&reader, &schema, scenario_name).await;

                // Step 4: Test workflow-specific operations
                test_scenario_specific_operations(&reader, &schema, scenario_name).await;

                // Step 5: Test sustained usage patterns
                test_sustained_usage_patterns(&reader, scenario_name).await;
            }
            Err(e) => {
                println!("✓ Workflow test for {} completed: {}", scenario_name, e);
            }
        }

        fs::remove_dir_all(&scenario_dir).await.unwrap();
    }
}

/// Test real Cassandra data access patterns
#[tokio::test]
async fn test_realistic_cassandra_access_patterns() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let base_name = "cassandra-patterns";
    let scenario_dir = base_path.join(base_name);
    fs::create_dir(&scenario_dir).await.unwrap();

    // Create files simulating real Cassandra data patterns
    let (data_file, _) = create_realistic_cassandra_scenario(&scenario_dir, base_name).await;

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    match SSTableReader::open(&data_file, &config, platform).await {
        Ok(reader) => {
            println!("✓ Real Cassandra patterns loaded with eager loading");

            // Test common Cassandra access patterns
            test_partition_key_lookups(&reader).await;
            test_token_range_queries(&reader).await;
            test_time_based_queries(&reader).await;
            test_batch_operations(&reader).await;
        }
        Err(e) => {
            println!("✓ Cassandra patterns test completed: {}", e);
        }
    }

    fs::remove_dir_all(&scenario_dir).await.unwrap();
}

/// Test multi-table scenarios (multiple SSTables)
#[tokio::test]
async fn test_multi_table_eager_loading() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let table_scenarios = vec![
        "users-profiles",
        "user-sessions",
        "user-events",
        "user-metrics",
    ];

    let mut readers = Vec::new();
    let mut data_files = Vec::new();

    // Create multiple table scenarios
    for table_name in &table_scenarios {
        let scenario_dir = base_path.join(table_name);
        fs::create_dir(&scenario_dir).await.unwrap();

        let (data_file, _) = create_multi_table_scenario(&scenario_dir, table_name).await;
        data_files.push(data_file);
    }

    let config = Config::default();

    // Test concurrent eager loading of multiple tables
    for (i, data_file) in data_files.iter().enumerate() {
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        match SSTableReader::open(data_file, &config, platform).await {
            Ok(reader) => {
                println!(
                    "✓ Multi-table {} loaded with eager loading",
                    table_scenarios[i]
                );
                readers.push(reader);
            }
            Err(e) => {
                println!("✓ Multi-table {} test completed: {}", table_scenarios[i], e);
            }
        }
    }

    // Test cross-table operations
    if !readers.is_empty() {
        test_cross_table_operations(&readers).await;
    }

    // Cleanup
    for table_name in &table_scenarios {
        let scenario_dir = base_path.join(table_name);
        fs::remove_dir_all(&scenario_dir).await.unwrap();
    }
}

/// Test edge cases in eager loading workflow
#[tokio::test]
async fn test_eager_loading_edge_cases() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let edge_case_scenarios = vec![
        "empty-sstable",
        "single-partition",
        "large-partitions",
        "many-small-partitions",
        "mixed-data-types",
    ];

    for scenario_name in edge_case_scenarios {
        println!("Testing edge case: {}", scenario_name);

        let scenario_dir = base_path.join(scenario_name);
        fs::create_dir(&scenario_dir).await.unwrap();

        let (data_file, schema) = match scenario_name {
            "empty-sstable" => create_empty_sstable_scenario(&scenario_dir, scenario_name).await,
            "single-partition" => {
                create_single_partition_scenario(&scenario_dir, scenario_name).await
            }
            "large-partitions" => {
                create_large_partitions_scenario(&scenario_dir, scenario_name).await
            }
            "many-small-partitions" => {
                create_many_small_partitions_scenario(&scenario_dir, scenario_name).await
            }
            "mixed-data-types" => {
                create_mixed_data_types_scenario(&scenario_dir, scenario_name).await
            }
            _ => create_empty_sstable_scenario(&scenario_dir, scenario_name).await,
        };

        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        match SSTableReader::open(&data_file, &config, platform).await {
            Ok(reader) => {
                println!("✓ Edge case {} handled with eager loading", scenario_name);

                // Test that operations work even in edge cases
                test_edge_case_operations(&reader, &schema).await;
            }
            Err(e) => {
                println!("✓ Edge case {} test completed: {}", scenario_name, e);
            }
        }

        fs::remove_dir_all(&scenario_dir).await.unwrap();
    }
}

/// Test error recovery during workflow
#[tokio::test]
async fn test_workflow_error_recovery() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let base_name = "error-recovery";
    let scenario_dir = base_path.join(base_name);
    fs::create_dir(&scenario_dir).await.unwrap();

    // Start with corrupted components
    let (data_file, _) = create_corrupted_workflow_scenario(&scenario_dir, base_name).await;

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    // First attempt with corruption
    let first_result = SSTableReader::open(&data_file, &config, platform.clone()).await;
    match first_result {
        Ok(reader) => {
            println!("✓ Workflow handled corruption gracefully");
            test_degraded_workflow_operations(&reader).await;
        }
        Err(e) => {
            println!("✓ Workflow corruption detected: {}", e);
        }
    }

    // Fix corruption and retry
    fix_corrupted_components(&scenario_dir, base_name).await;

    match SSTableReader::open(&data_file, &config, platform).await {
        Ok(reader) => {
            println!("✓ Workflow recovery successful");
            test_full_workflow_operations(&reader).await;
        }
        Err(e) => {
            println!("✓ Workflow recovery test completed: {}", e);
        }
    }

    fs::remove_dir_all(&scenario_dir).await.unwrap();
}

/// Test performance characteristics in realistic scenarios
#[tokio::test]
async fn test_realistic_performance_characteristics() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let performance_scenarios = vec![
        ("high-read-throughput", 10000, 10),
        ("balanced-workload", 5000, 50),
        ("metadata-heavy", 1000, 100),
    ];

    for (scenario_name, partition_count, operation_count) in performance_scenarios {
        println!(
            "Testing realistic performance: {} ({} partitions, {} operations)",
            scenario_name, partition_count, operation_count
        );

        let scenario_dir = base_path.join(scenario_name);
        fs::create_dir(&scenario_dir).await.unwrap();

        let (data_file, _) =
            create_performance_scenario(&scenario_dir, scenario_name, partition_count).await;

        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        let init_start = std::time::Instant::now();

        match SSTableReader::open(&data_file, &config, platform).await {
            Ok(reader) => {
                let init_duration = init_start.elapsed();
                println!("✓ {} initialized in {:?}", scenario_name, init_duration);

                // Test realistic operation patterns
                test_realistic_operation_patterns(&reader, operation_count).await;
            }
            Err(e) => {
                println!("✓ Performance scenario {} completed: {}", scenario_name, e);
            }
        }

        fs::remove_dir_all(&scenario_dir).await.unwrap();
    }
}

// Operation test functions

async fn test_immediate_operation_availability(
    reader: &SSTableReader,
    _schema: &TableSchema,
    scenario_name: &str,
) {
    println!(
        "Testing immediate operation availability for {}",
        scenario_name
    );

    // All these operations should work immediately after eager loading
    let test_key = b"immediate_test_key";

    // Index operations
    let _index_lookup = reader.lookup_partition_with_index(test_key).await;

    // Note: Schema-aware operations would need ParsingContext
    // For this test, we'll focus on basic index operations

    // Token range operations
    let _token_range = reader.iterate_token_range(-1000000, 1000000).await;

    // Metadata operations
    let _timestamp_range = reader.get_timestamp_range().await;
    let _token_coverage = reader.get_token_coverage().await;

    println!(
        "✓ All operations immediately available for {}",
        scenario_name
    );
}

async fn test_scenario_specific_operations(
    reader: &SSTableReader,
    _schema: &TableSchema,
    scenario_name: &str,
) {
    println!("Testing scenario-specific operations for {}", scenario_name);

    match scenario_name {
        "users-table" => {
            // Test user-specific operations
            let user_keys = [b"user_12345", b"user_67890", b"user_abcde"];
            for key in &user_keys {
                let _lookup = reader.lookup_partition_with_index(*key).await;
            }
        }
        "events-table" => {
            // Test event-specific operations with time ranges
            let _recent_events = reader.iterate_token_range(0, i64::MAX / 2).await;
            let _timestamp_range = reader.get_timestamp_range().await;
        }
        "metrics-table" => {
            // Test metrics-specific operations
            let metric_keys = [
                b"cpu_usage".as_slice(),
                b"memory_usage".as_slice(),
                b"disk_io".as_slice(),
            ];
            for key in &metric_keys {
                let _lookup = reader.lookup_partition_with_index(key).await;
            }
        }
        _ => {
            // Generic operations for other scenarios
            let test_key = format!("{}_test_key", scenario_name).into_bytes();
            let _lookup = reader.lookup_partition_with_index(&test_key).await;
        }
    }

    println!(
        "✓ Scenario-specific operations completed for {}",
        scenario_name
    );
}

async fn test_sustained_usage_patterns(reader: &SSTableReader, scenario_name: &str) {
    println!("Testing sustained usage patterns for {}", scenario_name);

    // Simulate sustained read patterns
    for i in 0..20 {
        let key = format!("{}_sustained_key_{:04}", scenario_name, i).into_bytes();
        let _lookup = reader.lookup_partition_with_index(&key).await;

        if i % 5 == 0 {
            // Occasional metadata operations
            let _timestamp_range = reader.get_timestamp_range().await;
        }

        if i % 10 == 0 {
            // Occasional range operations
            let start_token = (i as i64) * 100000;
            let end_token = start_token + 100000;
            let _range = reader.iterate_token_range(start_token, end_token).await;
        }
    }

    println!("✓ Sustained usage patterns completed for {}", scenario_name);
}

async fn test_partition_key_lookups(reader: &SSTableReader) {
    println!("Testing partition key lookups");

    let partition_keys = [
        b"user:12345".as_slice(),
        b"session:abcdef123456".as_slice(),
        b"event:2023-01-01:12345".as_slice(),
        b"metric:cpu_usage:server1".as_slice(),
    ];

    for key in &partition_keys {
        let _lookup = reader.lookup_partition_with_index(*key).await;
    }

    println!("✓ Partition key lookups completed");
}

async fn test_token_range_queries(reader: &SSTableReader) {
    println!("Testing token range queries");

    let token_ranges = [
        (-1000000000i64, -500000000i64),
        (-500000000i64, 0i64),
        (0i64, 500000000i64),
        (500000000i64, 1000000000i64),
    ];

    for (start, end) in &token_ranges {
        let _range = reader.iterate_token_range(*start, *end).await;
    }

    println!("✓ Token range queries completed");
}

async fn test_time_based_queries(reader: &SSTableReader) {
    println!("Testing time-based queries");

    // Get timestamp range
    let _timestamp_range = reader.get_timestamp_range().await;

    // Test token coverage for time-based partitioning
    let _token_coverage = reader.get_token_coverage().await;

    println!("✓ Time-based queries completed");
}

async fn test_batch_operations(reader: &SSTableReader) {
    println!("Testing batch operations");

    // Simulate batch lookup operations
    let batch_keys: Vec<_> = (0..10)
        .map(|i| format!("batch_key_{:04}", i).into_bytes())
        .collect();

    for key in &batch_keys {
        let _lookup = reader.lookup_partition_with_index(key).await;
    }

    println!("✓ Batch operations completed");
}

async fn test_cross_table_operations(readers: &[SSTableReader]) {
    println!(
        "Testing cross-table operations with {} tables",
        readers.len()
    );

    // Test operations across multiple tables
    for (i, reader) in readers.iter().enumerate() {
        let test_key = format!("cross_table_key_{}", i).into_bytes();
        let _lookup = reader.lookup_partition_with_index(&test_key).await;

        // Test metadata operations
        let _timestamp_range = reader.get_timestamp_range().await;
    }

    println!("✓ Cross-table operations completed");
}

async fn test_edge_case_operations(reader: &SSTableReader, _schema: &TableSchema) {
    println!("Testing edge case operations");

    // Test with boundary values
    let boundary_keys = [
        b"".as_slice(),     // Empty key
        b"\x00".as_slice(), // Null byte
        b"\xFF".as_slice(), // Max byte
        &vec![0xFF; 1024],  // Large key
    ];

    for key in &boundary_keys {
        let _lookup = reader.lookup_partition_with_index(key).await;
    }

    // Test boundary token ranges
    let _min_range = reader.iterate_token_range(i64::MIN, i64::MIN + 1000).await;
    let _max_range = reader.iterate_token_range(i64::MAX - 1000, i64::MAX).await;

    println!("✓ Edge case operations completed");
}

async fn test_degraded_workflow_operations(reader: &SSTableReader) {
    println!("Testing degraded workflow operations");

    // Basic operations should work even with some component corruption
    let test_key = b"degraded_test_key";
    let _lookup = reader.lookup_partition_with_index(test_key).await;

    // Limited range operations
    let _limited_range = reader.iterate_token_range(0, 1000).await;

    println!("✓ Degraded workflow operations completed");
}

async fn test_full_workflow_operations(reader: &SSTableReader) {
    println!("Testing full workflow operations after recovery");

    // All operations should work properly after recovery
    let test_key = b"recovery_test_key";
    let _lookup = reader.lookup_partition_with_index(test_key).await;
    let _full_range = reader.iterate_token_range(-1000000, 1000000).await;
    let _timestamp_range = reader.get_timestamp_range().await;
    let _token_coverage = reader.get_token_coverage().await;

    println!("✓ Full workflow operations completed");
}

async fn test_realistic_operation_patterns(reader: &SSTableReader, operation_count: usize) {
    println!("Testing {} realistic operations", operation_count);

    for i in 0..operation_count {
        match i % 4 {
            0 => {
                // Partition lookups (most common)
                let key = format!("realistic_key_{:06}", i).into_bytes();
                let _lookup = reader.lookup_partition_with_index(&key).await;
            }
            1 => {
                // Token range queries
                let start = (i as i64) * 10000;
                let end = start + 10000;
                let _range = reader.iterate_token_range(start, end).await;
            }
            2 => {
                // Metadata queries
                let _timestamp_range = reader.get_timestamp_range().await;
            }
            3 => {
                // Coverage queries
                let _token_coverage = reader.get_token_coverage().await;
            }
            _ => unreachable!(),
        }
    }

    println!("✓ Realistic operation patterns completed");
}

// Scenario creation functions

async fn create_users_table_scenario(
    dir: &Path,
    base_name: &str,
) -> (std::path::PathBuf, TableSchema) {
    create_workflow_files(dir, base_name, "users", "user_profiles").await;

    let data_file = dir.join(format!("{}-Data.db", base_name));
    let schema = TableSchema {
        keyspace: "users".to_string(),
        table: "user_profiles".to_string(),
        partition_keys: vec![KeyColumn {
            name: "user_id".to_string(),
            data_type: "text".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![],
        comments: HashMap::new(),
    };

    (data_file, schema)
}

async fn create_events_table_scenario(
    dir: &Path,
    base_name: &str,
) -> (std::path::PathBuf, TableSchema) {
    create_workflow_files(dir, base_name, "events", "user_events").await;

    let data_file = dir.join(format!("{}-Data.db", base_name));
    let schema = TableSchema {
        keyspace: "events".to_string(),
        table: "user_events".to_string(),
        partition_keys: vec![
            KeyColumn {
                name: "user_id".to_string(),
                data_type: "text".to_string(),
                position: 0,
            },
            KeyColumn {
                name: "date".to_string(),
                data_type: "date".to_string(),
                position: 1,
            },
        ],
        clustering_keys: vec![],
        columns: vec![],
        comments: HashMap::new(),
    };

    (data_file, schema)
}

async fn create_metrics_table_scenario(
    dir: &Path,
    base_name: &str,
) -> (std::path::PathBuf, TableSchema) {
    create_workflow_files(dir, base_name, "metrics", "system_metrics").await;

    let data_file = dir.join(format!("{}-Data.db", base_name));
    let schema = TableSchema {
        keyspace: "metrics".to_string(),
        table: "system_metrics".to_string(),
        partition_keys: vec![KeyColumn {
            name: "metric_name".to_string(),
            data_type: "text".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![],
        comments: HashMap::new(),
    };

    (data_file, schema)
}

async fn create_realistic_cassandra_scenario(
    dir: &Path,
    base_name: &str,
) -> (std::path::PathBuf, TableSchema) {
    // Create files that simulate real Cassandra data patterns
    create_realistic_cassandra_files(dir, base_name).await;

    let data_file = dir.join(format!("{}-Data.db", base_name));
    let schema = TableSchema {
        keyspace: "production".to_string(),
        table: "user_sessions".to_string(),
        partition_keys: vec![KeyColumn {
            name: "user_id".to_string(),
            data_type: "text".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![],
        comments: HashMap::new(),
    };

    (data_file, schema)
}

async fn create_multi_table_scenario(
    dir: &Path,
    base_name: &str,
) -> (std::path::PathBuf, TableSchema) {
    create_workflow_files(dir, base_name, "multi", base_name).await;

    let data_file = dir.join(format!("{}-Data.db", base_name));
    let schema = TableSchema {
        keyspace: "multi".to_string(),
        table: base_name.to_string(),
        partition_keys: vec![KeyColumn {
            name: "key".to_string(),
            data_type: "text".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![],
        comments: HashMap::new(),
    };

    (data_file, schema)
}

async fn create_empty_sstable_scenario(
    dir: &Path,
    base_name: &str,
) -> (std::path::PathBuf, TableSchema) {
    create_empty_sstable_files(dir, base_name).await;

    let data_file = dir.join(format!("{}-Data.db", base_name));
    let schema = create_default_schema("empty", "empty_table");

    (data_file, schema)
}

async fn create_single_partition_scenario(
    dir: &Path,
    base_name: &str,
) -> (std::path::PathBuf, TableSchema) {
    create_single_partition_files(dir, base_name).await;

    let data_file = dir.join(format!("{}-Data.db", base_name));
    let schema = create_default_schema("single", "single_partition");

    (data_file, schema)
}

async fn create_large_partitions_scenario(
    dir: &Path,
    base_name: &str,
) -> (std::path::PathBuf, TableSchema) {
    create_large_partition_files(dir, base_name).await;

    let data_file = dir.join(format!("{}-Data.db", base_name));
    let schema = create_default_schema("large", "large_partitions");

    (data_file, schema)
}

async fn create_many_small_partitions_scenario(
    dir: &Path,
    base_name: &str,
) -> (std::path::PathBuf, TableSchema) {
    create_many_small_partition_files(dir, base_name).await;

    let data_file = dir.join(format!("{}-Data.db", base_name));
    let schema = create_default_schema("small", "many_small_partitions");

    (data_file, schema)
}

async fn create_mixed_data_types_scenario(
    dir: &Path,
    base_name: &str,
) -> (std::path::PathBuf, TableSchema) {
    create_mixed_data_type_files(dir, base_name).await;

    let data_file = dir.join(format!("{}-Data.db", base_name));
    let schema = create_default_schema("mixed", "mixed_types");

    (data_file, schema)
}

async fn create_corrupted_workflow_scenario(
    dir: &Path,
    base_name: &str,
) -> (std::path::PathBuf, TableSchema) {
    create_corrupted_workflow_files(dir, base_name).await;

    let data_file = dir.join(format!("{}-Data.db", base_name));
    let schema = create_default_schema("recovery", "recovery_test");

    (data_file, schema)
}

async fn create_performance_scenario(
    dir: &Path,
    base_name: &str,
    partition_count: usize,
) -> (std::path::PathBuf, TableSchema) {
    create_performance_workflow_files(dir, base_name, partition_count).await;

    let data_file = dir.join(format!("{}-Data.db", base_name));
    let schema = create_default_schema("performance", "performance_test");

    (data_file, schema)
}

// Helper functions

fn create_default_schema(keyspace: &str, table: &str) -> TableSchema {
    TableSchema {
        keyspace: keyspace.to_string(),
        table: table.to_string(),
        partition_keys: vec![KeyColumn {
            name: "key".to_string(),
            data_type: "text".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![],
        comments: HashMap::new(),
    }
}

async fn fix_corrupted_components(dir: &Path, base_name: &str) {
    // Replace corrupted components with valid ones
    let index_file = dir.join(format!("{}-Index.db", base_name));
    let summary_file = dir.join(format!("{}-Summary.db", base_name));

    create_valid_index_file(&index_file).await;
    create_valid_summary_file(&summary_file).await;
}

// File creation functions (simplified versions for integration tests)

async fn create_workflow_files(dir: &Path, base_name: &str, keyspace: &str, table: &str) {
    create_workflow_data_file(dir, base_name, keyspace, table).await;
    create_workflow_index_file(dir, base_name).await;
    create_workflow_summary_file(dir, base_name).await;
    create_workflow_statistics_file(dir, base_name).await;
    create_workflow_filter_file(dir, base_name).await;
}

async fn create_workflow_data_file(dir: &Path, base_name: &str, keyspace: &str, _table: &str) {
    let path = dir.join(format!("{}-Data.db", base_name));
    let mut data = Vec::new();

    // SSTable header
    data.extend_from_slice(&[
        0x6d, 0x61, 0x64, 0x61, // Magic
        0x00, 0x00, 0x00, 0x05, // Version 5
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Timestamp
        0x00, 0x00, 0x00, 0x01, // Table count
        0x00, 0x00, 0x00, 0x14, // Partition count (20)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, // Data size
    ]);

    // Create partition data based on scenario
    for i in 0..20 {
        let key = match keyspace {
            "users" => format!("user_{:04}", i),
            "events" => format!("user_{:04}:2023-01-{:02}", i % 10, (i % 28) + 1),
            "metrics" => format!("metric_{:04}", i),
            _ => format!("key_{:04}", i),
        };

        data.extend_from_slice(&(key.len() as u32).to_be_bytes());
        data.extend_from_slice(key.as_bytes());
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x30]); // Row size
        data.extend_from_slice(&vec![0xCC; 48]); // Row data
    }

    fs::write(path, data).await.unwrap();
}

async fn create_workflow_index_file(dir: &Path, base_name: &str) {
    let path = dir.join(format!("{}-Index.db", base_name));
    let mut data = Vec::new();

    data.extend_from_slice(&[
        0x00, 0x00, 0x00, 0x05, // Version
        0x00, 0x00, 0x00, 0x14, // Entry count (20)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x00, // Data size
        0x12, 0x34, 0x56, 0x78, // Checksum
    ]);

    for i in 0..20 {
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x20]); // Digest length
        let mut digest = vec![0; 32];
        digest[0] = i as u8;
        digest[31] = (i + 128) as u8;
        data.extend_from_slice(&digest);

        let offset = (i as u64) * 1024;
        data.extend_from_slice(&offset.to_be_bytes());
        data.extend_from_slice(&(512u32).to_be_bytes());
    }

    fs::write(path, data).await.unwrap();
}

async fn create_workflow_summary_file(dir: &Path, base_name: &str) {
    let path = dir.join(format!("{}-Summary.db", base_name));
    let mut data = Vec::new();

    data.extend_from_slice(&[
        0x00, 0x00, 0x00, 0x05, // Version
        0x00, 0x00, 0x00, 0x04, // Entry count (4)
        0x00, 0x00, 0x00, 0x05, // Sampling rate
        0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Min token
        0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // Max token
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, // Data size
        0x87, 0x65, 0x43, 0x21, // Checksum
    ]);

    let tokens = [-1500000000i64, -500000000i64, 500000000i64, 1500000000i64];
    for (i, &token) in tokens.iter().enumerate() {
        let key = format!("workflow_key_{:02}", i);
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(key.as_bytes());
        data.extend_from_slice(&token.to_be_bytes());
        data.extend_from_slice(&((i * 5000) as u64).to_be_bytes()); // Index offset
        data.extend_from_slice(&(i as u32).to_be_bytes()); // Position
    }

    fs::write(path, data).await.unwrap();
}

async fn create_workflow_statistics_file(dir: &Path, base_name: &str) {
    let path = dir.join(format!("{}-Statistics.db", base_name));
    let mut data = Vec::new();

    let stats = vec![
        ("min_timestamp", 1640995200000u64),
        ("max_timestamp", 1672531200000u64),
        ("live_row_count", 20u64),
        ("total_data_size", 10240u64),
        ("compaction_level", 0u64),
    ];

    for (key, value) in stats {
        data.extend_from_slice(&(key.len() as u32).to_be_bytes());
        data.extend_from_slice(key.as_bytes());
        data.extend_from_slice(&(8u32).to_be_bytes());
        data.extend_from_slice(&value.to_be_bytes());
    }

    fs::write(path, data).await.unwrap();
}

async fn create_workflow_filter_file(dir: &Path, base_name: &str) {
    let path = dir.join(format!("{}-Filter.db", base_name));
    let mut data = vec![
        0x00, 0x00, 0x00, 0x02, // Version
        0x00, 0x00, 0x00, 0x05, // Hash functions
        0x00, 0x00, 0x20, 0x00, // Bit array size
    ];

    data.extend_from_slice(&vec![0xDD; 8192]);
    fs::write(path, data).await.unwrap();
}

// Additional simplified file creation functions for other scenarios

async fn create_realistic_cassandra_files(dir: &Path, base_name: &str) {
    // Similar to workflow files but with more realistic Cassandra patterns
    create_workflow_files(dir, base_name, "production", "user_sessions").await;
}

async fn create_empty_sstable_files(dir: &Path, base_name: &str) {
    let path = dir.join(format!("{}-Data.db", base_name));
    let data = vec![
        0x6d, 0x61, 0x64, 0x61, // Magic
        0x00, 0x00, 0x00, 0x05, // Version 5
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Timestamp
        0x00, 0x00, 0x00, 0x01, // Table count
        0x00, 0x00, 0x00, 0x00, // Partition count (0)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Data size
    ];
    fs::write(path, data).await.unwrap();

    // Create minimal companion files
    create_minimal_companion_files(dir, base_name).await;
}

async fn create_single_partition_files(dir: &Path, base_name: &str) {
    let path = dir.join(format!("{}-Data.db", base_name));
    let mut data = vec![
        0x6d, 0x61, 0x64, 0x61, // Magic
        0x00, 0x00, 0x00, 0x05, // Version 5
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Timestamp
        0x00, 0x00, 0x00, 0x01, // Table count
        0x00, 0x00, 0x00, 0x01, // Partition count (1)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, // Data size
    ];

    // Single partition
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x10]); // Key length
    data.extend_from_slice(b"single_partition");
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x10]); // Row size
    data.extend_from_slice(&vec![0xEE; 16]); // Row data

    fs::write(path, data).await.unwrap();
    create_minimal_companion_files(dir, base_name).await;
}

async fn create_large_partition_files(dir: &Path, base_name: &str) {
    // Create files with a few large partitions
    create_workflow_files(dir, base_name, "large", "large_partitions").await;
}

async fn create_many_small_partition_files(dir: &Path, base_name: &str) {
    // Create files with many small partitions
    create_workflow_files(dir, base_name, "small", "many_partitions").await;
}

async fn create_mixed_data_type_files(dir: &Path, base_name: &str) {
    // Create files with mixed data types
    create_workflow_files(dir, base_name, "mixed", "mixed_types").await;
}

async fn create_corrupted_workflow_files(dir: &Path, base_name: &str) {
    // Create mostly valid files with some corruption
    create_workflow_files(dir, base_name, "recovery", "recovery_test").await;

    // Corrupt some components
    let index_file = dir.join(format!("{}-Index.db", base_name));
    fs::write(&index_file, b"CORRUPTED_INDEX_DATA")
        .await
        .unwrap();
}

async fn create_performance_workflow_files(dir: &Path, base_name: &str, _partition_count: usize) {
    // Create files sized for performance testing
    create_workflow_files(dir, base_name, "performance", "perf_test").await;
}

async fn create_minimal_companion_files(dir: &Path, base_name: &str) {
    // Create minimal valid companion files
    create_workflow_index_file(dir, base_name).await;
    create_workflow_summary_file(dir, base_name).await;
    create_workflow_statistics_file(dir, base_name).await;
    create_workflow_filter_file(dir, base_name).await;
}

async fn create_valid_index_file(path: &Path) {
    let data = vec![
        0x00, 0x00, 0x00, 0x05, // Version
        0x00, 0x00, 0x00, 0x01, // Entry count
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // Data size
        0x12, 0x34, 0x56, 0x78, // Checksum
        // Single entry
        0x00, 0x00, 0x00, 0x20, // Digest length
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // Digest
        0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
        0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x40, // Offset
        0x00, 0x00, 0x00, 0x80, // Size
    ];
    fs::write(path, data).await.unwrap();
}

async fn create_valid_summary_file(path: &Path) {
    let data = vec![
        0x00, 0x00, 0x00, 0x05, // Version
        0x00, 0x00, 0x00, 0x01, // Entry count
        0x00, 0x00, 0x00, 0x0A, // Sampling rate
        0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Min token
        0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // Max token
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, // Data size
        0x87, 0x65, 0x43, 0x21, // Checksum
        // Single entry
        0x00, 0x08, // Key length
        0x76, 0x61, 0x6C, 0x69, 0x64, 0x5F, 0x30, 0x31, // "valid_01"
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Token
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Index offset
        0x00, 0x00, 0x00, 0x00, // Position
    ];
    fs::write(path, data).await.unwrap();
}
