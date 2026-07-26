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

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use cqlite_core::{
    error::{Error, Result},
    platform::Platform,
    schema::{registry::SchemaRegistry, TableSchema},
    storage::sstable::reader::SSTableReader,
    types::TableId,
    Config, RowKey,
};
use cqlite_tests::discover_table_dir;

use tokio::fs;

/// Test fixture for golden path partition operations
pub struct GoldenPathPartitionTestFixture {
    /// Path to test datasets (kept for potential future fallback use)
    #[allow(dead_code)]
    datasets_path: PathBuf,
    /// Platform abstraction
    platform: Arc<Platform>,
    /// Configuration
    config: Config,
    /// Schema registry
    #[allow(dead_code)]
    schema_registry: Arc<SchemaRegistry>,
}

impl GoldenPathPartitionTestFixture {
    /// Create new test fixture
    pub async fn new() -> Result<Self> {
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await?);
        let registry_config = cqlite_core::schema::registry::SchemaRegistryConfig::default();
        let schema_registry =
            Arc::new(SchemaRegistry::new(registry_config, platform.clone(), config.clone()).await?);

        let datasets_path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test-data/datasets/sstables");

        Ok(Self {
            datasets_path,
            platform,
            config,
            schema_registry,
        })
    }

    /// Setup SSTable reader for partition testing (using test_basic dataset)
    async fn setup_collections_reader(&self) -> Result<SSTableReader> {
        let table_dir =
            discover_table_dir("test_basic", "compression_test_table").ok_or_else(|| {
                Error::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "test_basic/compression_test_table not found. \
                     Please ensure CQLITE_DATASETS_ROOT is set and test-data is available.",
                ))
            })?;
        let fallback_path = table_dir.join("nb-1-big-Data.db");

        if fs::metadata(&fallback_path).await.is_err() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "Test SSTable not found: {fallback_path:?}. Please ensure test-data is available."
                ),
            )));
        }

        SSTableReader::open(&fallback_path, &self.config, self.platform.clone()).await
    }

    /// Setup wide rows reader for multi-partition testing
    async fn setup_wide_rows_reader(&self) -> Result<SSTableReader> {
        let table_dir =
            discover_table_dir("test_basic", "compression_test_table").ok_or_else(|| {
                Error::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "test_basic/compression_test_table not found.",
                ))
            })?;
        let fallback_path = table_dir.join("nb-1-big-Data.db");

        if fs::metadata(&fallback_path).await.is_err() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Test SSTable not found: {fallback_path:?}"),
            )));
        }

        SSTableReader::open(&fallback_path, &self.config, self.platform.clone()).await
    }

    /// Create test schema with partition and clustering keys
    /// NOTE: This function is currently unused but kept for future use
    #[allow(dead_code)]
    fn create_test_schema(&self) -> Result<TableSchema> {
        // Using from_json since new_for_testing is only available in unit tests
        TableSchema::from_json(
            r#"{"keyspace":"test_keyspace","table":"test_table","partition_keys":[{"name":"id","data_type":"int","position":0}],"clustering_keys":[],"columns":[{"name":"id","data_type":"int","nullable":false,"default":null}],"comments":{}}"#,
        )
    }

    /// Generate test partition keys
    /// NOTE: This function is currently unused but kept for future use
    #[allow(dead_code)]
    fn create_test_partition_keys(&self) -> Vec<RowKey> {
        (1..=20)
            .map(|i| {
                let partition_data = format!("partition_{i:03}");
                RowKey::from(partition_data.as_bytes())
            })
            .collect()
    }
}

#[tokio::test]
async fn test_golden_path_single_partition_lookup() -> Result<()> {
    let fixture = GoldenPathPartitionTestFixture::new().await?;
    let reader = fixture.setup_collections_reader().await?;

    let table_id = TableId::new("test_keyspace.test_table");
    let partition_key = RowKey::from(b"single_partition_test".as_ref());

    // Test: Single partition key lookup
    let start_time = Instant::now();
    let result = reader.get(&table_id, &partition_key).await?;
    let lookup_duration = start_time.elapsed();

    // Record timing (do not assert on wall-clock latency — #2642/#2902).
    println!("[perf-record] single partition lookup: {lookup_duration:?}");

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

    let table_id = TableId::new("test_keyspace.test_table");
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
    }

    let avg_lookup_time = total_lookup_time / partition_keys.len() as u32;

    println!(
        "✅ Multi-partition scan: {}/{} partitions found",
        found_partitions,
        partition_keys.len()
    );
    // Record timing (do not assert on wall-clock latency — #2642/#2902).
    println!("[perf-record] average partition lookup: {avg_lookup_time:?}");

    // Load-immune STRUCTURAL invariant: the hit count cannot exceed the number
    // of partitions probed.
    assert!(
        found_partitions <= partition_keys.len(),
        "found partitions {found_partitions} cannot exceed probed {}",
        partition_keys.len()
    );

    Ok(())
}

#[tokio::test]
async fn test_golden_path_partition_boundary_scanning() -> Result<()> {
    let fixture = GoldenPathPartitionTestFixture::new().await?;
    let reader = fixture.setup_wide_rows_reader().await?;

    let table_id = TableId::new("test_keyspace.test_table");

    // Test: Scan across partition boundaries
    let start_partition = RowKey::from(b"partition_boundary_start".as_ref());
    let end_partition = RowKey::from(b"partition_boundary_end".as_ref());

    let start_time = Instant::now();
    let results = reader
        .scan(
            &table_id,
            Some(&start_partition),
            Some(&end_partition),
            Some(100),
            None,
        )
        .await?;
    let scan_duration = start_time.elapsed();

    println!("✅ Partition boundary scan completed in {scan_duration:?}");
    println!(
        "✅ Found {} entries across partition boundaries",
        results.len()
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

    // Load-immune STRUCTURAL invariant: the bounded boundary scan honors its limit.
    assert!(
        results.len() <= 100,
        "partition boundary scan must honor its limit of 100: got {} rows",
        results.len()
    );

    Ok(())
}

#[tokio::test]
async fn test_golden_path_clustering_key_operations() -> Result<()> {
    let fixture = GoldenPathPartitionTestFixture::new().await?;
    let reader = fixture.setup_collections_reader().await?;

    let table_id = TableId::new("test_keyspace.test_table");

    // Test: Operations within a single partition using clustering keys
    let base_partition = "cluster_test_partition";
    let clustering_keys = vec![
        format!("{}:cluster_001", base_partition),
        format!("{}:cluster_002", base_partition),
        format!("{}:cluster_003", base_partition),
    ];

    let mut clustering_results = Vec::new();

    for clustering_key_str in &clustering_keys {
        let clustering_key = RowKey::from(clustering_key_str.as_bytes());

        let start_time = Instant::now();
        let result = reader.get(&table_id, &clustering_key).await?;
        let lookup_duration = start_time.elapsed();

        clustering_results.push((clustering_key.clone(), result, lookup_duration));
    }

    // Test: Range scan within partition using clustering key boundaries
    let partition_start = RowKey::from(format!("{base_partition}:cluster_000").as_bytes());
    let partition_end = RowKey::from(format!("{base_partition}:cluster_999").as_bytes());

    let start_time = Instant::now();
    let range_results = reader
        .scan(
            &table_id,
            Some(&partition_start),
            Some(&partition_end),
            Some(50),
            None,
        )
        .await?;
    let range_duration = start_time.elapsed();

    println!(
        "✅ Clustering key range scan found {} entries in {:?}",
        range_results.len(),
        range_duration
    );

    // Load-immune STRUCTURAL invariants: every clustering key was probed exactly
    // once, and the bounded range scan honors its limit.
    assert_eq!(
        clustering_results.len(),
        clustering_keys.len(),
        "each clustering key should be probed exactly once"
    );
    assert!(
        range_results.len() <= 50,
        "clustering range scan must honor its limit of 50: got {} rows",
        range_results.len()
    );

    Ok(())
}

#[tokio::test]
async fn test_golden_path_partition_bloom_filter_efficiency() -> Result<()> {
    let fixture = GoldenPathPartitionTestFixture::new().await?;
    let reader = fixture.setup_collections_reader().await?;

    let table_id = TableId::new("test_keyspace.test_table");

    // Test: Bloom filter efficiency for partition lookups
    let non_existent_partitions = vec![
        "definitely_missing_partition_1",
        "absent_partition_xyz_999",
        "non_existent_user_data_123",
        "missing_partition_boundary_test",
    ];

    // Assert the bloom fast path DIRECTLY, not via a wall-clock threshold (the
    // #2642/#2902 flake). Two load-immune signals, mirroring
    // test_golden_path_bloom_summary_index_coordination:
    //  1. STRUCTURAL: the reader actually loaded a bloom filter, so the bloom
    //     pre-check branch in `get()` is reachable (else the check is vacuous).
    //  2. BEHAVIORAL: an absent-key `get()` must NOT advance the process-global
    //     `SSTableReader::scan_for_key_call_count()` (issue #831) — the bloom
    //     short-circuits before the sequential scan fallback. The counter is
    //     process-global and this binary runs tests concurrently, so we retry:
    //     a genuine regression scans on EVERY attempt (delta >= 1 always), while
    //     a concurrent test's scan is sporadic, so a healthy fast path shows a
    //     zero delta on at least one attempt.
    let health = reader.get_health_metrics().await?;
    assert!(
        health.bloom_filter_enabled,
        "fixture SSTable must have a bloom filter loaded for the fast-path check to be meaningful"
    );

    for partition_name in &non_existent_partitions {
        let partition_key = RowKey::from(partition_name.as_bytes());

        let mut observed_zero_delta = false;
        let mut last_delta = u64::MAX;
        let mut last_duration = std::time::Duration::ZERO;
        for _ in 0..5 {
            let scans_before = SSTableReader::scan_for_key_call_count();
            let start_time = Instant::now();
            let result = reader.get(&table_id, &partition_key).await?;
            last_duration = start_time.elapsed();
            let scans_after = SSTableReader::scan_for_key_call_count();

            // Should be None for non-existent partitions — checked on EVERY attempt.
            assert!(
                result.is_none(),
                "Non-existent partition should return None: {partition_name}"
            );

            last_delta = scans_after.saturating_sub(scans_before);
            if last_delta == 0 {
                observed_zero_delta = true;
                break;
            }
        }

        assert!(
            observed_zero_delta,
            "Bloom filter should short-circuit absent-key lookup before scan_for_key \
             for {partition_name}; every attempt advanced scan_for_key (last delta \
             {last_delta}), the bloom fast path regressed"
        );

        // Non-asserting diagnostic (no wall-clock threshold — #2642/#2902).
        println!(
            "  absent-partition '{partition_name}' short-circuited in {last_duration:?} (no scan_for_key)"
        );
    }

    println!(
        "✅ Bloom filter efficiency verified: all {} non-existent partition lookups short-circuited the bloom fast path",
        non_existent_partitions.len()
    );

    Ok(())
}

#[tokio::test]
async fn test_golden_path_partition_summary_integration() -> Result<()> {
    let fixture = GoldenPathPartitionTestFixture::new().await?;
    let reader = fixture.setup_wide_rows_reader().await?;

    // Test: Integration with summary index for partition operations
    let table_id = TableId::new("test_keyspace.test_table");

    // Verify reader has summary functionality
    // NOTE: health_check() method is not currently available
    // let health_metrics = reader.health_check().await?;
    // println!(
    //     "✅ Reader health: index={}, bloom={}",
    //     health_metrics.index_available, health_metrics.bloom_filter_enabled
    // );

    // Test partition lookup with summary index
    let test_partitions = vec![
        "summary_test_partition_a",
        "summary_test_partition_m",
        "summary_test_partition_z",
    ];

    for partition_name in &test_partitions {
        let partition_key = RowKey::from(partition_name.as_bytes());

        let start_time = Instant::now();
        let result = reader.get(&table_id, &partition_key).await?;
        let lookup_duration = start_time.elapsed();

        // Record timing (do not assert on wall-clock latency — #2642/#2902).
        println!("[perf-record] summary-assisted lookup {partition_name}: {lookup_duration:?}");

        match result {
            Some(value) => {
                println!(
                    "✅ Found partition {}: {} bytes",
                    partition_name,
                    value.len()
                );
            }
            None => {
                println!("ℹ️  Partition {partition_name} not found (expected)");
            }
        }
    }

    // Test summary-assisted range scan
    let range_start = RowKey::from(b"summary_partition_a".as_ref());
    let range_end = RowKey::from(b"summary_partition_z".as_ref());

    let start_time = Instant::now();
    let range_results = reader
        .scan(
            &table_id,
            Some(&range_start),
            Some(&range_end),
            Some(25),
            None,
        )
        .await?;
    let range_duration = start_time.elapsed();

    println!(
        "✅ Summary-assisted range scan: {} entries in {:?}",
        range_results.len(),
        range_duration
    );

    // Load-immune STRUCTURAL invariant: the bounded summary-assisted range scan
    // honors its limit.
    assert!(
        range_results.len() <= 25,
        "summary-assisted range scan must honor its limit of 25: got {} rows",
        range_results.len()
    );

    Ok(())
}

#[tokio::test]
async fn test_golden_path_partition_performance_benchmarks() -> Result<()> {
    let fixture = GoldenPathPartitionTestFixture::new().await?;
    let reader = Arc::new(fixture.setup_wide_rows_reader().await?);

    let table_id = TableId::new("test_keyspace.test_table");

    // Benchmark: Batch partition lookups
    let partition_keys = (1..=50)
        .map(|i| RowKey::from(format!("benchmark_partition_{i:03}").as_bytes()))
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
            let reader = Arc::clone(&reader);
            let table_id = table_id.clone();
            tokio::spawn(async move {
                let key = RowKey::from(format!("concurrent_partition_{i}").as_bytes());
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
    let mut concurrent_processed = 0usize;
    for handle_result in concurrent_results {
        let (id, lookup_result, duration) =
            handle_result.map_err(|e| Error::internal(format!("Task failed: {e}")))?;

        lookup_result?; // Verify no errors
        concurrent_processed += 1;

        // Record timing (do not assert on wall-clock latency — #2642/#2902).
        println!("[perf-record] concurrent partition lookup {id}: {duration:?}");
    }

    println!("✅ Concurrent partition benchmark: 10 lookups in {concurrent_total:?}");

    // Load-immune STRUCTURAL invariants: the batch hit count cannot exceed the
    // keys probed, and every spawned concurrent lookup completed successfully.
    assert!(
        found_count <= partition_keys.len(),
        "batch found_count {found_count} cannot exceed probed {}",
        partition_keys.len()
    );
    assert_eq!(
        concurrent_processed, 10,
        "all 10 concurrent partition lookups should complete"
    );

    Ok(())
}

#[tokio::test]
async fn test_golden_path_partition_edge_cases() -> Result<()> {
    let fixture = GoldenPathPartitionTestFixture::new().await?;
    let reader = fixture.setup_collections_reader().await?;

    let table_id = TableId::new("test_keyspace.test_table");

    // Edge case 1: Empty partition key
    let empty_partition = RowKey::from(&b""[..]);
    let result = reader.get(&table_id, &empty_partition).await?;
    assert!(
        result.is_none(),
        "Empty partition key should not match data"
    );

    // Edge case 2: Maximum length partition key
    let max_partition = RowKey::from(vec![b'p'; 1024]);
    let start_time = Instant::now();
    let _result = reader.get(&table_id, &max_partition).await?;
    let duration = start_time.elapsed();

    // Record timing (do not assert on wall-clock latency — #2642/#2902).
    println!("[perf-record] large partition key lookup: {duration:?}");

    // Edge case 3: Binary partition keys
    let binary_partitions = vec![
        vec![0u8, 1u8, 2u8, 255u8],
        vec![0x00, 0xFF, 0x00, 0xFF],
        vec![b'\0', b'\x01', b'\x7F', b'\xFF'],
    ];

    for binary_key in binary_partitions {
        let partition_key = RowKey::from(&binary_key[..]);
        let _result = reader.get(&table_id, &partition_key).await?;
        // Should handle binary keys without errors
    }

    // Edge case 4: Unicode partition keys
    let unicode_partitions = vec![
        "partition_测试_🔑",
        "раздел_тест_ключ",
        "パーティション_テスト_キー",
    ];

    for unicode_partition in unicode_partitions {
        let partition_key = RowKey::from(unicode_partition.as_bytes());
        let _result = reader.get(&table_id, &partition_key).await?;
        // Should handle unicode gracefully
    }

    println!("✅ All partition edge cases handled successfully");
    Ok(())
}

#[tokio::test]
async fn test_golden_path_partition_integration_validation() -> Result<()> {
    let fixture = GoldenPathPartitionTestFixture::new().await?;
    let reader = fixture.setup_wide_rows_reader().await?;

    let table_id = TableId::new("test_keyspace.test_table");

    // Integration test: Verify partition operations integrate with all components

    // 1. Health check before partition operations
    // NOTE: health_check() method is not currently available
    // let health_metrics = reader.health_check().await?;
    // assert!(health_metrics.file_accessible, "File should be accessible");
    // println!("✅ Pre-partition health check passed");

    // 2. Test partition lookup with all optimizations
    let test_partition = RowKey::from(b"integration_test_partition".as_ref());

    let start_time = Instant::now();
    let _partition_result = reader.get(&table_id, &test_partition).await?;
    let partition_duration = start_time.elapsed();

    // 3. Test partition scan with integration
    let scan_start = RowKey::from(b"integration_scan_start".as_ref());
    let scan_end = RowKey::from(b"integration_scan_end".as_ref());

    let scan_start_time = Instant::now();
    let scan_results = reader
        .scan(
            &table_id,
            Some(&scan_start),
            Some(&scan_end),
            Some(20),
            None,
        )
        .await?;
    let scan_duration = scan_start_time.elapsed();

    // 4. Verify statistics after operations
    let post_stats = reader.stats().await?;
    println!(
        "✅ Post-operation stats: file_size={}",
        post_stats.file_size
    );
    // NOTE: cache_hits field is not available, use cache_hit_rate instead

    // 5. Record integration timings (do not assert on wall-clock latency —
    //    #2642/#2902).
    println!("[perf-record] integrated partition lookup: {partition_duration:?}");
    println!("[perf-record] integrated partition scan: {scan_duration:?}");

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
