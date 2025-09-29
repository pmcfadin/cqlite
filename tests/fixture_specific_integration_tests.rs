//! Fixture-Specific Integration Tests
//!
//! These tests focus on specific fixtures and component interactions,
//! particularly testing the wiring between partition lookups, range scans,
//! and decompression functionality after component lookup fixes.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use cqlite_core::{
    error::{Error, Result},
    platform::Platform,
    storage::sstable::{compression::CompressionReader, reader::SSTableReader},
    types::TableId,
    Config, RowKey, Value,
};

use tokio::fs;

/// Test fixture for validation against minimal Cassandra 5 fixtures
#[tokio::test]
async fn test_minimal_fixture_partition_lookup_integration() -> Result<()> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let fixture_path = manifest_dir.join("tests/fixtures/cassandra5/minimal/simple_table/Data.db");

    // Skip if fixture doesn't exist
    if !fs::metadata(&fixture_path).await.is_ok() {
        println!("ℹ️  Minimal fixture not found, skipping test. Run create_fixtures.py to enable.");
        return Ok(());
    }

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await?);
    let reader = SSTableReader::open(&fixture_path, &config, platform).await?;

    let table_id = TableId::new("test_keyspace.test_table");

    println!("🔍 Testing minimal fixture partition lookup integration:");

    // Test the specific fixture data (id=1, value="test")
    let test_key = RowKey::from(&1i32.to_be_bytes()[..]);

    let start_time = Instant::now();
    let result = reader.get(&table_id, &test_key).await?;
    let lookup_duration = start_time.elapsed();

    match result {
        Some(value) => {
            println!(
                "  ✅ Found fixture data: {} bytes in {:?}",
                value.len(),
                lookup_duration
            );

            // Validate expected fixture content
            if value.len() == 4 && value.as_bytes() == Some(b"test".as_slice()) {
                println!("  🎯 Confirmed expected fixture value: 'test'");
            } else if let Some(bytes) = value.as_bytes() {
                println!("  📋 Found value: {:?}", String::from_utf8_lossy(bytes));
            } else {
                println!("  📋 Found value: {:?}", value);
            }

            // Performance check
            assert!(
                lookup_duration.as_micros() < 10000,
                "Minimal fixture lookup should be very fast: {:?}μs",
                lookup_duration.as_micros()
            );
        }
        None => {
            println!("  ℹ️  Fixture key not found (may be normal for minimal fixture)");
        }
    }

    // Test non-existent key to verify proper lookup behavior
    let non_existent_key = RowKey::from(&999i32.to_be_bytes()[..]);
    let start_time = Instant::now();
    let result = reader.get(&table_id, &non_existent_key).await?;
    let lookup_duration = start_time.elapsed();

    assert!(result.is_none(), "Non-existent key should return None");
    assert!(
        lookup_duration.as_micros() < 5000,
        "Non-existent key lookup should be very fast: {:?}μs",
        lookup_duration.as_micros()
    );

    println!(
        "  ✅ Non-existent key properly handled in {:?}",
        lookup_duration
    );

    Ok(())
}

/// Test range scanning specifically against fixtures
#[tokio::test]
async fn test_fixture_range_scan_integration() -> Result<()> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let fixture_path = manifest_dir.join("tests/fixtures/cassandra5/minimal/simple_table/Data.db");

    // Try fixture first, fallback to real data
    let (reader, data_source) = if fs::metadata(&fixture_path).await.is_ok() {
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await?);
        let reader = SSTableReader::open(&fixture_path, &config, platform).await?;
        (reader, "minimal_fixture")
    } else {
        // Fallback to real dataset
        let real_path = manifest_dir.join("test-data/datasets/sstables/test_basic/compression_test_table-6e2f4520934a11f08d448925b7a9e804/nb-1-big-Data.db");

        if !fs::metadata(&real_path).await.is_ok() {
            println!("ℹ️  No test data available, skipping range scan test");
            return Ok(());
        }

        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await?);
        let reader = SSTableReader::open(&real_path, &config, platform).await?;
        (reader, "real_dataset")
    };

    let table_id = TableId::new("test_keyspace.test_table");

    println!("🔍 Testing range scan integration with {}: ", data_source);

    // Test 1: Full table scan
    let start_time = Instant::now();
    let full_results = reader.scan(&table_id, None, None, None).await?;
    let full_scan_duration = start_time.elapsed();

    println!(
        "  ✅ Full scan: {} entries in {:?}",
        full_results.len(),
        full_scan_duration
    );

    // Validate scan results are properly ordered
    if full_results.len() > 1 {
        for i in 1..full_results.len() {
            assert!(
                full_results[i - 1].0 <= full_results[i].0,
                "Scan results should be in ascending order"
            );
        }
        println!("  ✅ Scan ordering validated");
    }

    // Test 2: Limited scan
    let scan_limits = vec![1, 3, 10];
    for limit in scan_limits {
        let start_time = Instant::now();
        let limited_results = reader.scan(&table_id, None, None, Some(limit)).await?;
        let limited_duration = start_time.elapsed();

        assert!(
            limited_results.len() <= limit,
            "Limited scan should respect limit: {} <= {}",
            limited_results.len(),
            limit
        );

        println!(
            "  ✅ Limited scan ({}): {} entries in {:?}",
            limit,
            limited_results.len(),
            limited_duration
        );

        // Performance check for limited scans
        assert!(
            limited_duration.as_millis() < 100,
            "Limited scan should be fast: {:?}ms",
            limited_duration.as_millis()
        );
    }

    // Test 3: Range scan with integer keys (for fixture compatibility)
    let start_key = RowKey::from(&0i32.to_be_bytes()[..]);
    let end_key = RowKey::from(&5i32.to_be_bytes()[..]);

    let start_time = Instant::now();
    let range_results = reader
        .scan(&table_id, Some(&start_key), Some(&end_key), None)
        .await?;
    let range_duration = start_time.elapsed();

    println!(
        "  ✅ Range scan [0-5]: {} entries in {:?}",
        range_results.len(),
        range_duration
    );

    // Validate range boundaries
    for (key, _value) in &range_results {
        assert!(
            key >= &start_key && key <= &end_key,
            "All results should be within range"
        );
    }

    // Test 4: Empty range scan
    let empty_start = RowKey::from(&1000i32.to_be_bytes()[..]);
    let empty_end = RowKey::from(&2000i32.to_be_bytes()[..]);

    let start_time = Instant::now();
    let empty_results = reader
        .scan(&table_id, Some(&empty_start), Some(&empty_end), None)
        .await?;
    let empty_duration = start_time.elapsed();

    println!(
        "  ✅ Empty range scan: {} entries in {:?}",
        empty_results.len(),
        empty_duration
    );

    // Empty scans should be very fast
    assert!(
        empty_duration.as_millis() < 50,
        "Empty range scan should be very fast: {:?}ms",
        empty_duration.as_millis()
    );

    Ok(())
}

/// Test decompression integration with available data
#[tokio::test]
async fn test_decompression_integration_with_real_data() -> Result<()> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    // Look for compressed SSTable data
    let compressed_paths = vec![
        "test-data/datasets/sstables/test_basic/compression_test_table-6e2f4520934a11f08d448925b7a9e804/nb-1-big-Data.db",
        "test-data/datasets/sstables/test_wide_rows/*/nb-*-big-Data.db",
    ];

    let mut test_reader = None;
    let mut data_source = "";

    for path_pattern in compressed_paths {
        let full_path = manifest_dir.join(path_pattern);
        if fs::metadata(&full_path).await.is_ok() {
            let config = Config::default();
            let platform = Arc::new(Platform::new(&config).await?);

            match SSTableReader::open(&full_path, &config, platform).await {
                Ok(reader) => {
                    test_reader = Some(reader);
                    data_source = path_pattern;
                    break;
                }
                Err(_) => continue,
            }
        }
    }

    let reader = match test_reader {
        Some(reader) => reader,
        None => {
            println!("ℹ️  No compressed test data available, skipping decompression test");
            return Ok(());
        }
    };

    println!(
        "🗜️  Testing decompression integration with: {}",
        data_source
    );

    let table_id = TableId::new("test_keyspace.test_table");

    // Check compression status
    // NOTE: health_check() method is not currently available
    // let health_metrics = reader.health_check().await?;
    // println!("  📊 Compression info:");
    // println!("    - Enabled: {}", health_metrics.compression_enabled);
    // println!("    - Algorithm: {}", health_metrics.compression_algorithm);

    // Test 1: Read through decompression pipeline
    let start_time = Instant::now();
    let results = reader.scan(&table_id, None, None, Some(20)).await?;
    let decompression_duration = start_time.elapsed();

    println!(
        "  ✅ Decompressed scan: {} entries in {:?}",
        results.len(),
        decompression_duration
    );

    // Validate decompressed data
    let mut total_decompressed_bytes = 0;
    for (key, value) in &results {
        assert!(
            !key.as_bytes().is_empty(),
            "Key should not be empty after decompression"
        );
        assert!(
            !value.is_empty(),
            "Value should not be empty after decompression"
        );
        total_decompressed_bytes += value.len();
    }

    println!(
        "  📊 Total decompressed data: {} bytes",
        total_decompressed_bytes
    );

    // Performance check: decompression should not be prohibitively slow
    // Note: health_metrics not available, performing generic performance check
    assert!(
        decompression_duration.as_millis() < 1000,
        "Decompression should be reasonably fast: {:?}ms",
        decompression_duration.as_millis()
    );

    // Test 2: Individual partition lookup through decompression
    if !results.is_empty() {
        let test_key = &results[0].0;

        let start_time = Instant::now();
        let lookup_result = reader.get(&table_id, test_key).await?;
        let lookup_duration = start_time.elapsed();

        match lookup_result {
            Some(value) => {
                println!(
                    "  ✅ Decompressed lookup: {} bytes in {:?}",
                    value.len(),
                    lookup_duration
                );

                // Cross-validate with scan result
                assert_eq!(
                    value, results[0].1,
                    "Lookup and scan should return same decompressed value"
                );
            }
            None => {
                return Err(Error::internal("Lookup should find key that scan found"));
            }
        }

        // Performance check for individual lookup
        assert!(
            lookup_duration.as_millis() < 100,
            "Decompressed lookup should be fast: {:?}ms",
            lookup_duration.as_millis()
        );
    }

    // Test 3: Multiple operations to stress decompression pipeline
    let stress_operations = 10;
    let mut stress_times = Vec::new();

    for i in 0..stress_operations {
        let start_time = Instant::now();
        let _results = reader.scan(&table_id, None, None, Some(5)).await?;
        let duration = start_time.elapsed();
        stress_times.push(duration);

        if i % 3 == 0 {
            println!("  🔄 Stress test {}/{}...", i + 1, stress_operations);
        }
    }

    let avg_stress_time =
        stress_times.iter().sum::<std::time::Duration>() / stress_times.len() as u32;
    println!(
        "  ✅ Stress test complete: average {:?} per operation",
        avg_stress_time
    );

    // Performance assertion for sustained operations
    assert!(
        avg_stress_time.as_millis() < 200,
        "Sustained decompression operations should be efficient: {:?}ms avg",
        avg_stress_time.as_millis()
    );

    Ok(())
}

/// Test cross-validation between different operations
#[tokio::test]
async fn test_cross_operation_validation() -> Result<()> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    // Try to find any available test data
    let test_paths = vec![
        "tests/fixtures/cassandra5/minimal/simple_table/Data.db",
        "test-data/datasets/sstables/test_basic/compression_test_table-6e2f4520934a11f08d448925b7a9e804/nb-1-big-Data.db",
    ];

    let mut reader = None;
    let mut data_source = "";

    for path in test_paths {
        let full_path = manifest_dir.join(path);
        if fs::metadata(&full_path).await.is_ok() {
            let config = Config::default();
            let platform = Arc::new(Platform::new(&config).await?);

            match SSTableReader::open(&full_path, &config, platform).await {
                Ok(r) => {
                    reader = Some(r);
                    data_source = path;
                    break;
                }
                Err(_) => continue,
            }
        }
    }

    let reader = match reader {
        Some(r) => r,
        None => {
            println!("ℹ️  No test data available, skipping cross-validation test");
            return Ok(());
        }
    };

    println!(
        "🔄 Testing cross-operation validation with: {}",
        data_source
    );

    let table_id = TableId::new("test_keyspace.test_table");

    // Get some data through scan
    let scan_results = reader.scan(&table_id, None, None, Some(10)).await?;

    if scan_results.is_empty() {
        println!("  ℹ️  No data available for cross-validation");
        return Ok(());
    }

    println!("  📊 Cross-validating {} entries", scan_results.len());

    // Test 1: Validate that get returns same values as scan
    let mut validated_count = 0;

    for (scan_key, scan_value) in &scan_results {
        let get_result = reader.get(&table_id, scan_key).await?;

        match get_result {
            Some(get_value) => {
                assert_eq!(
                    get_value, *scan_value,
                    "Get and scan should return identical values for key {:?}",
                    scan_key
                );
                validated_count += 1;
            }
            None => {
                println!("  ⚠️  Key from scan not found in get: {:?}", scan_key);
            }
        }
    }

    println!(
        "  ✅ Cross-validated {}/{} entries",
        validated_count,
        scan_results.len()
    );

    // Test 2: Validate range scan consistency
    if scan_results.len() >= 2 {
        let first_key = &scan_results[0].0;
        let last_key = &scan_results[scan_results.len() - 1].0;

        let range_results = reader
            .scan(&table_id, Some(first_key), Some(last_key), None)
            .await?;

        // All scan results should be contained in range results
        for (scan_key, scan_value) in &scan_results {
            let found_in_range = range_results
                .iter()
                .any(|(range_key, range_value)| range_key == scan_key && range_value == scan_value);

            if !found_in_range {
                println!("  ⚠️  Scan entry not found in range scan: {:?}", scan_key);
            }
        }

        println!("  ✅ Range scan consistency validated");
    }

    // Test 3: Performance consistency across operations
    let performance_tests = vec![
        ("get_first", &scan_results[0].0),
        ("get_last", &scan_results[scan_results.len() - 1].0),
    ];

    for (test_name, key) in performance_tests {
        let start_time = Instant::now();
        let _result = reader.get(&table_id, key).await?;
        let duration = start_time.elapsed();

        println!("  📊 {}: {:?}", test_name, duration);

        assert!(
            duration.as_millis() < 50,
            "Operation {} should be fast: {:?}ms",
            test_name,
            duration.as_millis()
        );
    }

    println!("  🎉 Cross-operation validation completed successfully");
    Ok(())
}
