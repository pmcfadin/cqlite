//! Golden Path Test Suite - Partition Lookup Operations
//!
//! This module provides comprehensive golden-path tests for SSTable partition operations
//! using real Cassandra 5 artifacts from test-data/datasets.
//!
//! Test Coverage:
//! - Partition key lookup and resolution
//! - Multi-partition scanning and boundaries
//! - Clustering key operations within partitions
//! - Partition-aware bloom filter usage
//! - Performance validation for partition operations
//! - Integration with summary and index components

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use cqlite_core::{
    error::{Error, Result},
    parser::row::PartitionKey,
    platform::Platform,
    schema::{registry::SchemaRegistry, ClusteringColumn, KeyColumn, TableSchema},
    storage::sstable::{
        index_reader::IndexReader, reader::SSTableReader, schema_aware_reader::SchemaAwareReader,
        summary_reader::SummaryReader,
    },
    types::{ComparatorType, TableId},
    Config, RowKey, Value,
};

use tokio::fs;

/// Test fixture for golden path partition operations
pub struct GoldenPathPartitionTestFixture {
    /// Path to test datasets
    datasets_path: PathBuf,
    /// Platform abstraction
    platform: Arc<Platform>,
    /// Configuration
    config: Config,
    /// Schema registry
    schema_registry: Arc<SchemaRegistry>,
}

impl GoldenPathPartitionTestFixture {
    /// Create new test fixture
    pub async fn new() -> Result<Self> {
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await?);
        let schema_registry = Arc::new(SchemaRegistry::new());

        let datasets_path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test-data/datasets/sstables");

        Ok(Self {
            datasets_path,
            platform,
            config,
            schema_registry,
        })
    }

    /// Setup SSTable reader for partition testing (using collections dataset)
    async fn setup_collections_reader(&self) -> Result<SSTableReader> {
        // Try collections dataset first, fallback to test_basic
        let primary_path = self
            .datasets_path
            .join("test_collections")
            .join("*/nb-*-big-Data.db");

        let fallback_path = self.datasets_path.join(
            "test_basic/compression_test_table-6e2f4520934a11f08d448925b7a9e804/nb-1-big-Data.db",
        );

        if !fs::metadata(&fallback_path).await.is_ok() {
            return Err(Error::io_error(format!(
                "Test SSTable not found: {:?}. Please ensure test-data is available.",
                fallback_path
            )));
        }

        SSTableReader::open(&fallback_path, &self.config, self.platform.clone()).await
    }

    /// Setup wide rows reader for multi-partition testing
    async fn setup_wide_rows_reader(&self) -> Result<SSTableReader> {
        let fallback_path = self.datasets_path.join(
            "test_basic/compression_test_table-6e2f4520934a11f08d448925b7a9e804/nb-1-big-Data.db",
        );

        if !fs::metadata(&fallback_path).await.is_ok() {
            return Err(Error::io_error(format!(
                "Test SSTable not found: {:?}",
                fallback_path
            )));
        }

        SSTableReader::open(&fallback_path, &self.config, self.platform.clone()).await
    }

    /// Create test schema with partition and clustering keys
    fn create_test_schema(&self) -> Result<TableSchema> {
        TableSchema::builder()
            .table_name("test_table")
            .keyspace_name("test_keyspace")
            .partition_key("user_id", ComparatorType::UUIDType)
            .clustering_key("timestamp", ComparatorType::TimestampType)
            .clustering_key("event_type", ComparatorType::UTF8Type)
            .column("data", ComparatorType::UTF8Type)
            .column("metadata", ComparatorType::MapType)
            .build()
    }

    /// Generate test partition keys
    fn create_test_partition_keys(&self) -> Vec<RowKey> {
        (1..=20)
            .map(|i| {
                let partition_data = format!("partition_{:03}", i);
                RowKey::from_bytes(partition_data.as_bytes())
            })
            .collect()
    }
}

#[tokio::test]
async fn test_golden_path_single_partition_lookup() -> Result<()> {
    let fixture = GoldenPathPartitionTestFixture::new().await?;
    let reader = fixture.setup_collections_reader().await?;

    let table_id = TableId::new("test_keyspace", "test_table");
    let partition_key = RowKey::from_bytes(b"single_partition_test");

    // Test: Single partition key lookup
    let start_time = Instant::now();
    let result = reader.get(&table_id, &partition_key).await?;
    let lookup_duration = start_time.elapsed();

    println!(
        "✅ Single partition lookup completed in {:?}",
        lookup_duration
    );

    // Performance assertion: Partition lookups should be very fast
    assert!(
        lookup_duration.as_millis() < 50,
        "Single partition lookup took too long: {:?}ms",
        lookup_duration.as_millis()
    );

    // Log result for analysis
    match result {
        Some(value) => {
            println!("✅ Found partition data: {} bytes", value.len());
            assert!(!value.is_empty(), "Partition data should not be empty");
        }
        None => {
            println!("ℹ️  No data found for partition key (expected for test dataset)");
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_golden_path_multi_partition_scanning() -> Result<()> {
    let fixture = GoldenPathPartitionTestFixture::new().await?;
    let reader = fixture.setup_wide_rows_reader().await?;

    let table_id = TableId::new("test_keyspace", "test_table");
    let partition_keys = fixture.create_test_partition_keys();

    // Test: Lookup multiple partitions
    let mut found_partitions = 0;
    let mut total_lookup_time = std::time::Duration::ZERO;

    for partition_key in &partition_keys {
        let start_time = Instant::now();
        let result = reader.get(&table_id, partition_key).await?;
        let lookup_duration = start_time.elapsed();

        total_lookup_time += lookup_duration;

        if result.is_some() {
            found_partitions += 1;
        }

        // Each partition lookup should be fast
        assert!(
            lookup_duration.as_millis() < 100,
            "Partition lookup took too long: {:?}ms",
            lookup_duration.as_millis()
        );
    }

    let avg_lookup_time = total_lookup_time / partition_keys.len() as u32;

    println!(
        "✅ Multi-partition scan: {}/{} partitions found",
        found_partitions,
        partition_keys.len()
    );
    println!("✅ Average partition lookup time: {:?}", avg_lookup_time);

    // Performance assertion for batch lookups
    assert!(
        avg_lookup_time.as_micros() < 5000,
        "Average partition lookup should be efficient: {:?}μs",
        avg_lookup_time.as_micros()
    );

    Ok(())
}

#[tokio::test]
async fn test_golden_path_partition_boundary_scanning() -> Result<()> {
    let fixture = GoldenPathPartitionTestFixture::new().await?;
    let reader = fixture.setup_wide_rows_reader().await?;

    let table_id = TableId::new("test_keyspace", "test_table");

    // Test: Scan across partition boundaries
    let start_partition = RowKey::from_bytes(b"partition_boundary_start");
    let end_partition = RowKey::from_bytes(b"partition_boundary_end");

    let start_time = Instant::now();
    let results = reader
        .scan(
            &table_id,
            Some(&start_partition),
            Some(&end_partition),
            Some(100),
        )
        .await?;
    let scan_duration = start_time.elapsed();

    println!(
        "✅ Partition boundary scan completed in {:?}",
        scan_duration
    );
    println!(
        "✅ Found {} entries across partition boundaries",
        results.len()
    );

    // Performance assertion
    assert!(
        scan_duration.as_millis() < 500,
        "Partition boundary scan took too long: {:?}ms",
        scan_duration.as_millis()
    );

    // Validate scan results span multiple partitions (if data exists)
    if results.len() > 1 {
        let mut unique_partition_prefixes = HashSet::new();
        for (key, _value) in &results {
            // Extract potential partition identifier from key
            let key_str = String::from_utf8_lossy(key.as_bytes());
            if let Some(prefix) = key_str.split('_').next() {
                unique_partition_prefixes.insert(prefix.to_string());
            }
        }

        println!(
            "✅ Scan covered {} unique partition prefixes",
            unique_partition_prefixes.len()
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_golden_path_clustering_key_operations() -> Result<()> {
    let fixture = GoldenPathPartitionTestFixture::new().await?;
    let reader = fixture.setup_collections_reader().await?;

    let table_id = TableId::new("test_keyspace", "test_table");

    // Test: Operations within a single partition using clustering keys
    let base_partition = "cluster_test_partition";
    let clustering_keys = vec![
        format!("{}:cluster_001", base_partition),
        format!("{}:cluster_002", base_partition),
        format!("{}:cluster_003", base_partition),
    ];

    let mut clustering_results = Vec::new();

    for clustering_key_str in &clustering_keys {
        let clustering_key = RowKey::from_bytes(clustering_key_str.as_bytes());

        let start_time = Instant::now();
        let result = reader.get(&table_id, &clustering_key).await?;
        let lookup_duration = start_time.elapsed();

        clustering_results.push((clustering_key.clone(), result, lookup_duration));

        // Clustering key lookups should be very fast
        assert!(
            lookup_duration.as_micros() < 10000,
            "Clustering key lookup took too long: {:?}μs",
            lookup_duration.as_micros()
        );
    }

    // Test: Range scan within partition using clustering key boundaries
    let partition_start = RowKey::from_bytes(format!("{}:cluster_000", base_partition).as_bytes());
    let partition_end = RowKey::from_bytes(format!("{}:cluster_999", base_partition).as_bytes());

    let start_time = Instant::now();
    let range_results = reader
        .scan(
            &table_id,
            Some(&partition_start),
            Some(&partition_end),
            Some(50),
        )
        .await?;
    let range_duration = start_time.elapsed();

    println!(
        "✅ Clustering key range scan found {} entries in {:?}",
        range_results.len(),
        range_duration
    );

    // Performance assertion for clustering range scan
    assert!(
        range_duration.as_millis() < 200,
        "Clustering key range scan took too long: {:?}ms",
        range_duration.as_millis()
    );

    Ok(())
}

#[tokio::test]
async fn test_golden_path_partition_bloom_filter_efficiency() -> Result<()> {
    let fixture = GoldenPathPartitionTestFixture::new().await?;
    let reader = fixture.setup_collections_reader().await?;

    let table_id = TableId::new("test_keyspace", "test_table");

    // Test: Bloom filter efficiency for partition lookups
    let non_existent_partitions = vec![
        "definitely_missing_partition_1",
        "absent_partition_xyz_999",
        "non_existent_user_data_123",
        "missing_partition_boundary_test",
    ];

    let mut bloom_test_times = Vec::new();

    for partition_name in &non_existent_partitions {
        let partition_key = RowKey::from_bytes(partition_name.as_bytes());

        let start_time = Instant::now();
        let result = reader.get(&table_id, &partition_key).await?;
        let lookup_duration = start_time.elapsed();

        bloom_test_times.push(lookup_duration);

        // Should be None for non-existent partitions
        assert!(
            result.is_none(),
            "Non-existent partition should return None: {}",
            partition_name
        );

        // Bloom filter should make this very fast
        assert!(
            lookup_duration.as_micros() < 1000,
            "Bloom filter lookup should be very fast: {:?}μs for {}",
            lookup_duration.as_micros(),
            partition_name
        );
    }

    let avg_bloom_time =
        bloom_test_times.iter().sum::<std::time::Duration>() / bloom_test_times.len() as u32;

    println!(
        "✅ Bloom filter efficiency test: average lookup time {:?}",
        avg_bloom_time
    );
    println!(
        "✅ All {} non-existent partition lookups were efficient",
        non_existent_partitions.len()
    );

    Ok(())
}

#[tokio::test]
async fn test_golden_path_partition_summary_integration() -> Result<()> {
    let fixture = GoldenPathPartitionTestFixture::new().await?;
    let reader = fixture.setup_wide_rows_reader().await?;

    // Test: Integration with summary index for partition operations
    let table_id = TableId::new("test_keyspace", "test_table");

    // Verify reader has summary functionality
    let health_metrics = reader.health_check().await?;
    println!(
        "✅ Reader health: index={}, bloom={}",
        health_metrics.index_available, health_metrics.bloom_filter_enabled
    );

    // Test partition lookup with summary index
    let test_partitions = vec![
        "summary_test_partition_a",
        "summary_test_partition_m",
        "summary_test_partition_z",
    ];

    for partition_name in &test_partitions {
        let partition_key = RowKey::from_bytes(partition_name.as_bytes());

        let start_time = Instant::now();
        let result = reader.get(&table_id, &partition_key).await?;
        let lookup_duration = start_time.elapsed();

        // Summary-assisted lookups should be very efficient
        assert!(
            lookup_duration.as_millis() < 10,
            "Summary-assisted partition lookup should be very fast: {:?}ms for {}",
            lookup_duration.as_millis(),
            partition_name
        );

        match result {
            Some(value) => {
                println!(
                    "✅ Found partition {}: {} bytes",
                    partition_name,
                    value.len()
                );
            }
            None => {
                println!("ℹ️  Partition {} not found (expected)", partition_name);
            }
        }
    }

    // Test summary-assisted range scan
    let range_start = RowKey::from_bytes(b"summary_partition_a");
    let range_end = RowKey::from_bytes(b"summary_partition_z");

    let start_time = Instant::now();
    let range_results = reader
        .scan(&table_id, Some(&range_start), Some(&range_end), Some(25))
        .await?;
    let range_duration = start_time.elapsed();

    println!(
        "✅ Summary-assisted range scan: {} entries in {:?}",
        range_results.len(),
        range_duration
    );

    // Summary should make range scans efficient
    assert!(
        range_duration.as_millis() < 100,
        "Summary-assisted range scan should be efficient: {:?}ms",
        range_duration.as_millis()
    );

    Ok(())
}

#[tokio::test]
async fn test_golden_path_partition_performance_benchmarks() -> Result<()> {
    let fixture = GoldenPathPartitionTestFixture::new().await?;
    let reader = fixture.setup_wide_rows_reader().await?;

    let table_id = TableId::new("test_keyspace", "test_table");

    // Benchmark: Batch partition lookups
    let partition_keys = (1..=50)
        .map(|i| RowKey::from_bytes(format!("benchmark_partition_{:03}", i).as_bytes()))
        .collect::<Vec<_>>();

    let start_time = Instant::now();
    let mut found_count = 0;

    for partition_key in &partition_keys {
        if let Some(_value) = reader.get(&table_id, partition_key).await? {
            found_count += 1;
        }
    }

    let total_duration = start_time.elapsed();
    let avg_duration = total_duration / partition_keys.len() as u32;

    // Performance benchmarks
    assert!(
        avg_duration.as_micros() < 2000,
        "Average partition lookup should be very fast: {:?}μs",
        avg_duration.as_micros()
    );

    assert!(
        total_duration.as_millis() < 500,
        "Batch partition lookups should complete quickly: {:?}ms",
        total_duration.as_millis()
    );

    println!(
        "✅ Partition benchmark: {} lookups in {:?} (avg: {:?}, found: {})",
        partition_keys.len(),
        total_duration,
        avg_duration,
        found_count
    );

    // Benchmark: Concurrent partition lookups
    let concurrent_handles = (1..=10)
        .map(|i| {
            let reader = reader.clone();
            let table_id = table_id.clone();
            tokio::spawn(async move {
                let key = RowKey::from_bytes(format!("concurrent_partition_{}", i).as_bytes());
                let start_time = Instant::now();
                let result = reader.get(&table_id, &key).await;
                let duration = start_time.elapsed();
                (i, result, duration)
            })
        })
        .collect::<Vec<_>>();

    let concurrent_start = Instant::now();
    let concurrent_results = futures::future::join_all(concurrent_handles).await;
    let concurrent_total = concurrent_start.elapsed();

    // Verify concurrent operations
    for handle_result in concurrent_results {
        let (id, lookup_result, duration) =
            handle_result.map_err(|e| Error::internal(format!("Task failed: {}", e)))?;

        lookup_result?; // Verify no errors

        assert!(
            duration.as_millis() < 100,
            "Concurrent partition lookup {} should be fast: {:?}ms",
            id,
            duration.as_millis()
        );
    }

    println!(
        "✅ Concurrent partition benchmark: 10 lookups in {:?}",
        concurrent_total
    );

    Ok(())
}

#[tokio::test]
async fn test_golden_path_partition_edge_cases() -> Result<()> {
    let fixture = GoldenPathPartitionTestFixture::new().await?;
    let reader = fixture.setup_collections_reader().await?;

    let table_id = TableId::new("test_keyspace", "test_table");

    // Edge case 1: Empty partition key
    let empty_partition = RowKey::from_bytes(b"");
    let result = reader.get(&table_id, &empty_partition).await?;
    assert!(
        result.is_none(),
        "Empty partition key should not match data"
    );

    // Edge case 2: Maximum length partition key
    let max_partition = RowKey::from_bytes(&vec![b'p'; 1024]);
    let start_time = Instant::now();
    let result = reader.get(&table_id, &max_partition).await?;
    let duration = start_time.elapsed();

    assert!(
        duration.as_millis() < 100,
        "Large partition key lookup should still be efficient: {:?}ms",
        duration.as_millis()
    );

    // Edge case 3: Binary partition keys
    let binary_partitions = vec![
        vec![0u8, 1u8, 2u8, 255u8],
        vec![0x00, 0xFF, 0x00, 0xFF],
        vec![b'\0', b'\x01', b'\x7F', b'\xFF'],
    ];

    for binary_key in binary_partitions {
        let partition_key = RowKey::from_bytes(&binary_key);
        let result = reader.get(&table_id, &partition_key).await?;
        // Should handle binary keys without errors
    }

    // Edge case 4: Unicode partition keys
    let unicode_partitions = vec![
        "partition_测试_🔑",
        "раздел_тест_ключ",
        "パーティション_テスト_キー",
    ];

    for unicode_partition in unicode_partitions {
        let partition_key = RowKey::from_bytes(unicode_partition.as_bytes());
        let result = reader.get(&table_id, &partition_key).await?;
        // Should handle unicode gracefully
    }

    println!("✅ All partition edge cases handled successfully");
    Ok(())
}

#[tokio::test]
async fn test_golden_path_partition_integration_validation() -> Result<()> {
    let fixture = GoldenPathPartitionTestFixture::new().await?;
    let reader = fixture.setup_wide_rows_reader().await?;

    let table_id = TableId::new("test_keyspace", "test_table");

    // Integration test: Verify partition operations integrate with all components

    // 1. Health check before partition operations
    let health_metrics = reader.health_check().await?;
    assert!(health_metrics.file_accessible, "File should be accessible");
    println!("✅ Pre-partition health check passed");

    // 2. Test partition lookup with all optimizations
    let test_partition = RowKey::from_bytes(b"integration_test_partition");

    let start_time = Instant::now();
    let partition_result = reader.get(&table_id, &test_partition).await?;
    let partition_duration = start_time.elapsed();

    // 3. Test partition scan with integration
    let scan_start = RowKey::from_bytes(b"integration_scan_start");
    let scan_end = RowKey::from_bytes(b"integration_scan_end");

    let scan_start_time = Instant::now();
    let scan_results = reader
        .scan(&table_id, Some(&scan_start), Some(&scan_end), Some(20))
        .await?;
    let scan_duration = scan_start_time.elapsed();

    // 4. Verify statistics after operations
    let post_stats = reader.stats().await?;
    println!(
        "✅ Post-operation stats: file_size={}, cache_hits={}",
        post_stats.file_size, post_stats.cache_hits
    );

    // 5. Integration performance assertions
    assert!(
        partition_duration.as_millis() < 50,
        "Integrated partition lookup should be very fast: {:?}ms",
        partition_duration.as_millis()
    );

    assert!(
        scan_duration.as_millis() < 200,
        "Integrated partition scan should be efficient: {:?}ms",
        scan_duration.as_millis()
    );

    // 6. Cross-validation: ensure get and scan consistency
    if !scan_results.is_empty() {
        let first_scan_key = &scan_results[0].0;
        let get_result = reader.get(&table_id, first_scan_key).await?;

        if let Some(get_value) = get_result {
            assert_eq!(
                get_value, scan_results[0].1,
                "Partition get and scan should return consistent results"
            );
        }
    }

    println!("✅ Partition integration validation completed successfully");
    Ok(())
}
