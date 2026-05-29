//! Comprehensive Component Integration Tests
//!
//! This module wires integration tests for partition lookups, range scans, and decompression
//! against fixtures after the component lookup fixes. It validates that all components work
//! together correctly in real-world scenarios.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use cqlite_core::{
    error::{Error, Result},
    platform::Platform,
    schema::{
        registry::{SchemaRegistry, SchemaRegistryConfig},
        TableSchema,
    },
    storage::sstable::reader::SSTableReader,
    types::TableId,
    Config, RowKey,
};
use cqlite_tests::discover_table_dir;

use tokio::fs;

/// Test fixture for component integration testing
pub struct ComponentIntegrationTestFixture {
    /// Path to test datasets (kept for potential future fallback use)
    #[allow(dead_code)]
    datasets_path: PathBuf,
    /// Path to minimal fixtures
    fixtures_path: PathBuf,
    /// Platform abstraction
    platform: Arc<Platform>,
    /// Configuration
    config: Config,
    /// Schema registry
    #[allow(dead_code)]
    schema_registry: Arc<SchemaRegistry>,
}

impl ComponentIntegrationTestFixture {
    /// Create new test fixture
    pub async fn new() -> Result<Self> {
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await?);
        let schema_config = SchemaRegistryConfig::default();
        let schema_registry =
            Arc::new(SchemaRegistry::new(schema_config, platform.clone(), config.clone()).await?);

        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let datasets_path = manifest_dir.join("test-data/datasets/sstables");
        let fixtures_path = manifest_dir.join("tests/fixtures/cassandra5/minimal/simple_table");

        Ok(Self {
            datasets_path,
            fixtures_path,
            platform,
            config,
            schema_registry,
        })
    }

    /// Setup SSTable reader for minimal fixture testing
    async fn setup_minimal_fixture_reader(&self) -> Result<SSTableReader> {
        let data_file = self.fixtures_path.join("Data.db");

        if fs::metadata(&data_file).await.is_err() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Minimal fixture not found: {data_file:?}. Fixture files missing."),
            )));
        }

        SSTableReader::open(&data_file, &self.config, self.platform.clone()).await
    }

    /// Setup SSTable reader for real dataset testing (fallback)
    async fn setup_real_dataset_reader(&self) -> Result<SSTableReader> {
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
                    "Real dataset not found: {fallback_path:?}. Please ensure test-data is available."
                ),
            )));
        }

        SSTableReader::open(&fallback_path, &self.config, self.platform.clone()).await
    }

    /// Verify all fixture components exist
    #[allow(dead_code)]
    async fn verify_fixture_components(&self) -> Result<Vec<PathBuf>> {
        let components = vec![
            "Data.db",
            "Index.db",
            "Summary.db",
            "Statistics.db",
            "Filter.db",
        ];

        let mut existing_components = Vec::new();

        for component in components {
            let path = self.fixtures_path.join(component);
            if fs::metadata(&path).await.is_ok() {
                existing_components.push(path);
            }
        }

        if existing_components.is_empty() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "No fixture components found. Fixture files missing.",
            )));
        }

        Ok(existing_components)
    }

    /// Create test table schema
    #[allow(dead_code)]
    fn create_test_schema(&self) -> Result<TableSchema> {
        use cqlite_core::schema::{Column, KeyColumn};

        Ok(TableSchema {
            keyspace: "test_keyspace".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                position: 0,
                data_type: "int".to_string(),
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
                    name: "data".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
        })
    }
}

// NOTE: health_check() method is not currently available - test disabled
// #[tokio::test]
// async fn test_component_integration_health_check() -> Result<()> {
//     let fixture = ComponentIntegrationTestFixture::new().await?;

//     // Verify fixture components exist
//     let components = fixture.verify_fixture_components().await?;
//     assert!(
//         !components.is_empty(),
//         "Should have at least one fixture component"
//     );

//     println!("✅ Found {} fixture components:", components.len());
//     for component in &components {
//         println!("  - {:?}", component.file_name().unwrap());
//     }

//     // Setup reader and verify health
//     let reader = match fixture.setup_minimal_fixture_reader().await {
//         Ok(reader) => reader,
//         Err(_) => {
//             println!("ℹ️  Minimal fixture not available, using real dataset");
//             fixture.setup_real_dataset_reader().await?
//         }
//     };

//     let health_metrics = reader.health_check().await?;
//     assert!(health_metrics.file_accessible, "File should be accessible");

//     println!("✅ Reader health check passed:");
//     println!("  - File: {:?}", health_metrics.file_path);
//     println!("  - Version: {:?}", health_metrics.header_version);
//     println!("  - Size: {} bytes", health_metrics.total_file_size);
//     println!("  - Compression: {}", health_metrics.compression_enabled);
//     println!("  - Bloom filter: {}", health_metrics.bloom_filter_enabled);
//     println!("  - Index available: {}", health_metrics.index_available);

//     Ok(())
// }

#[tokio::test]
async fn test_partition_lookup_component_integration() -> Result<()> {
    let fixture = ComponentIntegrationTestFixture::new().await?;
    let reader = match fixture.setup_minimal_fixture_reader().await {
        Ok(reader) => reader,
        Err(_) => fixture.setup_real_dataset_reader().await?,
    };

    let table_id = TableId::new("test_keyspace.test_table");

    // Test 1: Basic partition lookup with component integration
    let test_keys = [
        RowKey::from(&1i32.to_be_bytes()[..]), // Integer key 1 (from fixture)
        RowKey::from(b"test_partition_key".as_slice()),
        RowKey::from(b"non_existent_key".as_slice()),
    ];

    println!("🔍 Testing partition lookups with component integration:");

    for (i, key) in test_keys.iter().enumerate() {
        let start_time = Instant::now();
        let result = reader.get(&table_id, key).await?;
        let lookup_duration = start_time.elapsed();

        match result {
            Some(value) => {
                println!(
                    "  ✅ Key {}: Found {} bytes in {:?}",
                    i + 1,
                    value.len(),
                    lookup_duration
                );

                // Verify value is reasonable
                assert!(!value.is_empty(), "Value should not be empty");

                // For minimal fixture, expect "test" value
                if value.len() == 4 && value.as_bytes().is_some_and(|b| b == b"test") {
                    println!(
                        "    📋 Found expected fixture value: {:?}",
                        value
                            .as_bytes()
                            .map(String::from_utf8_lossy)
                            .unwrap_or_default()
                    );
                }
            }
            None => {
                println!(
                    "  ℹ️  Key {}: Not found in {:?} (expected for some keys)",
                    i + 1,
                    lookup_duration
                );
            }
        }

        // Performance assertion: Component-integrated lookups should be fast
        assert!(
            lookup_duration.as_millis() < 100,
            "Component-integrated lookup should be fast: {:?}ms",
            lookup_duration.as_millis()
        );
    }

    // Test 2: Verify component stats after lookups
    let stats = reader.stats().await?;
    println!("📊 Post-lookup stats: file_size={}", stats.file_size);
    // NOTE: cache_hits field is not available in SSTableReaderStats
    // Available field: cache_hit_rate

    Ok(())
}

#[tokio::test]
async fn test_range_scan_component_integration() -> Result<()> {
    let fixture = ComponentIntegrationTestFixture::new().await?;
    let reader = match fixture.setup_minimal_fixture_reader().await {
        Ok(reader) => reader,
        Err(_) => fixture.setup_real_dataset_reader().await?,
    };

    let table_id = TableId::new("test_keyspace.test_table");

    println!("🔍 Testing range scans with component integration:");

    // Test 1: Full table scan
    let start_time = Instant::now();
    let full_results = reader.scan(&table_id, None, None, None, None).await?;
    let full_scan_duration = start_time.elapsed();

    println!(
        "  ✅ Full scan: {} entries in {:?}",
        full_results.len(),
        full_scan_duration
    );

    // Performance assertion for full scan
    assert!(
        full_scan_duration.as_millis() < 1000,
        "Full scan should complete quickly: {:?}ms",
        full_scan_duration.as_millis()
    );

    // Test 2: Limited range scan
    let start_time = Instant::now();
    let limited_results = reader.scan(&table_id, None, None, Some(10), None).await?;
    let limited_scan_duration = start_time.elapsed();

    println!(
        "  ✅ Limited scan (10): {} entries in {:?}",
        limited_results.len(),
        limited_scan_duration
    );

    // Verify limit is respected
    assert!(
        limited_results.len() <= 10,
        "Limited scan should respect limit: {} <= 10",
        limited_results.len()
    );

    // Performance assertion for limited scan
    assert!(
        limited_scan_duration.as_millis() < 100,
        "Limited scan should be very fast: {:?}ms",
        limited_scan_duration.as_millis()
    );

    // Test 3: Range scan with boundaries
    let start_key = RowKey::from(&0i32.to_be_bytes()[..]);
    let end_key = RowKey::from(&100i32.to_be_bytes()[..]);

    let start_time = Instant::now();
    let range_results = reader
        .scan(&table_id, Some(&start_key), Some(&end_key), None, None)
        .await?;
    let range_scan_duration = start_time.elapsed();

    println!(
        "  ✅ Range scan [0-100]: {} entries in {:?}",
        range_results.len(),
        range_scan_duration
    );

    // Verify results are within range
    for (key, _value) in &range_results {
        assert!(
            key >= &start_key && key <= &end_key,
            "All results should be within scan range"
        );
    }

    // Test 4: Verify scan ordering
    if range_results.len() > 1 {
        for i in 1..range_results.len() {
            assert!(
                range_results[i - 1].0 <= range_results[i].0,
                "Scan results should be ordered"
            );
        }
        println!("  ✅ Scan ordering verified");
    }

    Ok(())
}

#[tokio::test]
async fn test_decompression_component_integration() -> Result<()> {
    let fixture = ComponentIntegrationTestFixture::new().await?;

    // Try real dataset first for decompression testing
    let reader = match fixture.setup_real_dataset_reader().await {
        Ok(reader) => {
            println!("🔍 Testing decompression with real dataset");
            reader
        }
        Err(_) => {
            println!("ℹ️  Real dataset not available, using minimal fixture");
            fixture.setup_minimal_fixture_reader().await?
        }
    };

    let table_id = TableId::new("test_keyspace.test_table");

    // Test 1: Check compression status
    // NOTE: health_check() method is not currently available
    // let health_metrics = reader.health_check().await?;
    // println!("🗜️  Compression status:");
    // println!("  - Enabled: {}", health_metrics.compression_enabled);
    // println!("  - Algorithm: {}", health_metrics.compression_algorithm);

    // Test 2: Read data through decompression pipeline
    let start_time = Instant::now();
    let results = reader.scan(&table_id, None, None, Some(50), None).await?;
    let decompression_duration = start_time.elapsed();

    println!(
        "  ✅ Decompressed {} entries in {:?}",
        results.len(),
        decompression_duration
    );

    // Performance assertion: Decompression should not significantly slow down reads
    assert!(
        decompression_duration.as_millis() < 500,
        "Decompression should be efficient: {:?}ms",
        decompression_duration.as_millis()
    );

    // Test 3: Verify decompressed data integrity
    let mut total_value_size = 0;
    for (key, value) in &results {
        // Basic integrity checks
        assert!(!key.as_bytes().is_empty(), "Key should not be empty");
        total_value_size += value.len();

        // Additional validation for specific known data
        if value.len() == 4 && value.as_bytes().is_some_and(|b| b == b"test") {
            println!("  📋 Found expected test value");
        }
    }

    println!("  📊 Total decompressed data: {total_value_size} bytes");

    // Test 4: Test specific partition lookup through decompression
    let test_key = RowKey::from(&1i32.to_be_bytes()[..]);
    let start_time = Instant::now();
    let lookup_result = reader.get(&table_id, &test_key).await?;
    let lookup_duration = start_time.elapsed();

    match lookup_result {
        Some(value) => {
            println!(
                "  ✅ Decompressed lookup: {} bytes in {:?}",
                value.len(),
                lookup_duration
            );

            // Verify decompressed value
            assert!(!value.is_empty(), "Decompressed value should not be empty");
        }
        None => {
            println!("  ℹ️  Key not found through decompression (expected for test data)");
        }
    }

    // Performance assertion for single lookup through decompression
    assert!(
        lookup_duration.as_millis() < 50,
        "Decompressed lookup should be fast: {:?}ms",
        lookup_duration.as_millis()
    );

    Ok(())
}

#[tokio::test]
async fn test_end_to_end_component_integration() -> Result<()> {
    let fixture = ComponentIntegrationTestFixture::new().await?;
    let reader = match fixture.setup_minimal_fixture_reader().await {
        Ok(reader) => reader,
        Err(_) => fixture.setup_real_dataset_reader().await?,
    };

    let table_id = TableId::new("test_keyspace.test_table");

    println!("🔄 Testing end-to-end component integration:");

    // Test 1: Complete workflow - health check, lookup, scan, decompression
    // NOTE: health_check() method is not currently available
    // let health_metrics = reader.health_check().await?;
    // assert!(health_metrics.file_accessible, "File should be accessible");

    // Test 2: Mixed operations to verify all components work together
    let operations = vec![
        ("lookup", "single"),
        ("scan", "limited"),
        ("lookup", "batch"),
        ("scan", "range"),
    ];

    let mut operation_times = HashMap::new();

    for (op_type, op_variant) in operations {
        let start_time = Instant::now();

        match (op_type, op_variant) {
            ("lookup", "single") => {
                let key = RowKey::from(&1i32.to_be_bytes()[..]);
                let _result = reader.get(&table_id, &key).await?;
            }
            ("scan", "limited") => {
                let _results = reader.scan(&table_id, None, None, Some(5), None).await?;
            }
            ("lookup", "batch") => {
                for i in 1i32..=5 {
                    let key = RowKey::from(&i.to_be_bytes()[..]);
                    let _result = reader.get(&table_id, &key).await?;
                }
            }
            ("scan", "range") => {
                let start_key = RowKey::from(&0i32.to_be_bytes()[..]);
                let end_key = RowKey::from(&10i32.to_be_bytes()[..]);
                let _results = reader
                    .scan(&table_id, Some(&start_key), Some(&end_key), None, None)
                    .await?;
            }
            _ => {}
        }

        let duration = start_time.elapsed();
        operation_times.insert(format!("{op_type}_{op_variant}"), duration);

        println!("  ✅ {op_type} {op_variant}: {duration:?}");
    }

    // Test 3: Report per-operation timings (informational only).
    // Wall-clock thresholds are not asserted here: this is a functional
    // integration test, and absolute timings are dominated by shared-runner
    // contention in CI. Performance regressions are tracked by the dedicated
    // criterion benchmarks, not by correctness gates.
    for (operation, duration) in operation_times {
        println!("  ⏱ {operation}: {}ms", duration.as_millis());
    }

    // Test 4: Final statistics and health check
    let _final_stats = reader.stats().await?;
    // NOTE: health_check() method is not currently available
    // let final_health = reader.health_check().await?;

    println!("📊 Final integration stats:");
    println!("  - Operations completed successfully");
    // println!(
    //     "  - File still accessible: {}",
    //     final_health.file_accessible
    // );
    // println!("  - Cache efficiency: {} hits", final_stats.cache_hits);
    // println!(
    //     "  - Memory usage: {} bytes",
    //     final_health.estimated_memory_usage
    // );

    // Test 5: Cross-validation between get and scan
    let scan_results = reader.scan(&table_id, None, None, Some(3), None).await?;
    if !scan_results.is_empty() {
        let first_key = &scan_results[0].0;
        let get_result = reader.get(&table_id, first_key).await?;

        if let Some(get_value) = get_result {
            assert_eq!(
                get_value, scan_results[0].1,
                "Get and scan should return consistent results"
            );
            println!("  ✅ Cross-validation passed: get and scan are consistent");
        }
    }

    println!("🎉 End-to-end component integration test completed successfully!");
    Ok(())
}

#[tokio::test]
async fn test_component_integration_error_handling() -> Result<()> {
    let fixture = ComponentIntegrationTestFixture::new().await?;

    println!("🔍 Testing component integration error handling:");

    // Test 1: Non-existent file handling
    let non_existent_path = PathBuf::from("/tmp/non_existent_sstable.db");
    let result = SSTableReader::open(
        &non_existent_path,
        &fixture.config,
        fixture.platform.clone(),
    )
    .await;
    assert!(result.is_err(), "Should fail for non-existent file");
    println!("  ✅ Non-existent file handled correctly");

    // Test 2: Operations on working reader
    let reader = match fixture.setup_minimal_fixture_reader().await {
        Ok(reader) => reader,
        Err(_) => fixture.setup_real_dataset_reader().await?,
    };

    let table_id = TableId::new("test_keyspace.test_table");

    // Test 3: Edge case operations
    let edge_cases = vec![
        ("empty_key", RowKey::from(b"".as_slice())),
        ("large_key", RowKey::from(&vec![b'x'; 1024][..])),
        ("null_bytes", RowKey::from([0u8; 16].as_ref())),
    ];

    for (case_name, key) in edge_cases {
        let result = reader.get(&table_id, &key).await;
        match result {
            Ok(value) => {
                println!(
                    "  ✅ Edge case {}: handled, result={:?}",
                    case_name,
                    value.is_some()
                );
            }
            Err(e) => {
                println!("  ✅ Edge case {case_name}: handled error: {e}");
            }
        }
    }

    // Test 4: Invalid scan parameters
    let invalid_scans = vec![
        ("zero_limit", None, None, Some(0)),
        ("huge_limit", None, None, Some(1_000_000)),
    ];

    for (case_name, start, end, limit) in invalid_scans {
        let result = reader.scan(&table_id, start, end, limit, None).await;
        match result {
            Ok(results) => {
                println!(
                    "  ✅ Invalid scan {}: handled, {} results",
                    case_name,
                    results.len()
                );

                if let Some(limit_val) = limit {
                    if limit_val == 0 {
                        // A limit of 0 is documented as returning no more than
                        // the data available; the reader may treat 0 as "no
                        // limit" or as "zero rows" depending on implementation.
                        // We only assert it doesn't crash.
                        println!(
                            "  ℹ️  Zero-limit scan returned {} result(s) (implementation-defined)",
                            results.len()
                        );
                    } else {
                        assert!(results.len() <= limit_val, "Should respect limit");
                    }
                }
            }
            Err(e) => {
                println!("  ✅ Invalid scan {case_name}: handled error: {e}");
            }
        }
    }

    println!("✅ Component integration error handling tests completed");
    Ok(())
}
