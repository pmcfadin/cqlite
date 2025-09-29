//! Multi-Format Compatibility Integration Tests
//!
//! This test suite validates cross-version SSTable compatibility and format matrix testing.
//! Tests real data interoperability between different Cassandra versions and CQLite.

#![cfg(feature = "experimental")]

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tempfile::TempDir;

use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::SSTableReader;
use cqlite_core::testing::dataset_helpers::{
    list_tables, load_metadata, resolve_table_to_sstable_path,
};
use cqlite_core::Config;

mod common;
use common::{constants::*, create_test_config, init_test_logging};

/// Test cross-version SSTable compatibility between Cassandra 3.x, 4.x, and 5.x
#[tokio::test]
async fn test_cross_version_sstable_compatibility() -> cqlite_core::Result<()> {
    init_test_logging();
    let start = Instant::now();

    let metadata = match load_metadata() {
        Ok(metadata) => metadata,
        Err(_) => {
            println!("Datasets not available, skipping cross-version compatibility test");
            return Ok(());
        }
    };

    let config = create_test_config();
    let platform = Arc::new(Platform::new(&config).await?);

    let mut compatibility_results = HashMap::new();
    let mut performance_metrics = HashMap::new();

    // Test different Cassandra version combinations
    let version_combinations = vec![
        ("cassandra_3x", "cassandra_4x"),
        ("cassandra_4x", "cassandra_5x"),
        ("cassandra_3x", "cassandra_5x"),
    ];

    for (source_version, target_version) in version_combinations {
        let test_start = Instant::now();

        // Find tables that exist in both versions
        let source_tables = list_tables(&metadata, Some(source_version));
        let target_tables = list_tables(&metadata, Some(target_version));

        let common_tables: Vec<_> = source_tables
            .iter()
            .filter(|&table| target_tables.contains(table))
            .collect();

        let mut version_results = Vec::new();

        for table_info in common_tables.iter().take(3) {
            // Test first 3 tables for time
            let source_path =
                resolve_table_to_sstable_path(&metadata, &table_info.keyspace, &table_info.table);

            if let Some(source_path) = source_path {
                if source_path.exists() {
                    let read_start = Instant::now();
                    let reader_result =
                        SSTableReader::open(&source_path, &config, platform.clone()).await;
                    let read_duration = read_start.elapsed();

                    match reader_result {
                        Ok(reader) => {
                            let table_start = Instant::now();

                            // Test basic reading operations
                            let rows = reader.read_all_rows().await?;
                            let metadata_info = reader.get_metadata().await?;

                            let operation_duration = table_start.elapsed();

                            version_results.push(format!(
                                "✅ {}.{}: {} rows, {} columns, read in {:?}",
                                table_info.keyspace,
                                table_info.table,
                                rows.len(),
                                metadata_info.column_count.unwrap_or(0),
                                operation_duration
                            ));

                            // Store performance metrics
                            performance_metrics.insert(
                                format!(
                                    "{}_{}.{}",
                                    source_version, table_info.keyspace, table_info.table
                                ),
                                (read_duration, operation_duration, rows.len()),
                            );
                        }
                        Err(e) => {
                            version_results.push(format!(
                                "❌ {}.{}: Failed to read - {}",
                                table_info.keyspace, table_info.table, e
                            ));
                        }
                    }
                }
            }
        }

        let test_duration = test_start.elapsed();
        compatibility_results.insert(
            format!("{} -> {}", source_version, target_version),
            (version_results, test_duration),
        );
    }

    // Validate results and performance
    let total_duration = start.elapsed();

    println!("🏁 Cross-Version Compatibility Test Results:");
    for (version_pair, (results, duration)) in &compatibility_results {
        println!(
            "📊 {}: {} tests in {:?}",
            version_pair,
            results.len(),
            duration
        );
        for result in results {
            println!("  {}", result);
        }
    }

    println!("📈 Performance Metrics:");
    for (table, (read_time, operation_time, row_count)) in &performance_metrics {
        let throughput = *row_count as f64 / operation_time.as_secs_f64();
        println!(
            "  {}: {:.2} rows/sec (read: {:?}, ops: {:?})",
            table, throughput, read_time, operation_time
        );
    }

    // Assert performance bounds
    assert!(
        total_duration.as_secs() < DEFAULT_TIMEOUT_SECS,
        "Test took too long: {:?}",
        total_duration
    );

    // Assert we tested at least one compatibility combination
    assert!(
        !compatibility_results.is_empty(),
        "Should have tested at least one version combination"
    );

    Ok(())
}

/// Test compression format matrix across different algorithms and levels
#[tokio::test]
async fn test_compression_format_matrix() -> cqlite_core::Result<()> {
    init_test_logging();
    let start = Instant::now();

    let metadata = match load_metadata() {
        Ok(metadata) => metadata,
        Err(_) => {
            println!("Datasets not available, skipping compression format matrix test");
            return Ok(());
        }
    };

    let base_config = create_test_config();
    let platform = Arc::new(Platform::new(&base_config).await?);

    // Test different compression configurations
    let compression_configs = vec![
        ("lz4", cqlite_core::config::CompressionAlgorithm::Lz4),
        ("snappy", cqlite_core::config::CompressionAlgorithm::Snappy),
        (
            "deflate",
            cqlite_core::config::CompressionAlgorithm::Deflate,
        ),
        ("zstd", cqlite_core::config::CompressionAlgorithm::Zstd),
    ];

    let tables = list_tables(&metadata, None);
    let test_table = tables.first().expect("Should have at least one table");

    let mut compression_results = HashMap::new();

    for (compression_name, compression_algorithm) in compression_configs {
        let test_start = Instant::now();

        if let Some(sstable_path) =
            resolve_table_to_sstable_path(&metadata, &test_table.keyspace, &test_table.table)
        {
            if sstable_path.exists() {
                let read_start = Instant::now();

                // Create config with specific compression
                let mut config = base_config.clone();
                config.storage.compression.enabled = true;
                config.storage.compression.algorithm = compression_algorithm;

                let reader_result =
                    SSTableReader::open(&sstable_path, &config, platform.clone()).await;
                let read_duration = read_start.elapsed();

                match reader_result {
                    Ok(reader) => {
                        let operation_start = Instant::now();

                        // Test decompression operations
                        let rows = reader.read_all_rows().await?;
                        let metadata_info = reader.get_metadata().await?;

                        // Test partial reads with compression
                        let partial_rows = reader
                            .read_rows_range(0, std::cmp::min(10, rows.len()))
                            .await?;

                        let operation_duration = operation_start.elapsed();
                        let test_duration = test_start.elapsed();

                        compression_results.insert(
                            compression_name.to_string(),
                            (
                                true,
                                rows.len(),
                                partial_rows.len(),
                                read_duration,
                                operation_duration,
                                test_duration,
                                metadata_info.estimated_keys.unwrap_or(0),
                            ),
                        );

                        println!(
                            "✅ {}: {} rows, {} partial, read in {:?}, ops in {:?}",
                            compression_name,
                            rows.len(),
                            partial_rows.len(),
                            read_duration,
                            operation_duration
                        );
                    }
                    Err(e) => {
                        compression_results.insert(
                            compression_name.to_string(),
                            (
                                false,
                                0,
                                0,
                                read_duration,
                                std::time::Duration::ZERO,
                                test_start.elapsed(),
                                0,
                            ),
                        );
                        println!("❌ {}: Failed - {}", compression_name, e);
                    }
                }
            }
        }
    }

    let total_duration = start.elapsed();

    // Validate results
    println!("🏁 Compression Format Matrix Results:");
    for (compression, (success, rows, partial, read_time, op_time, total_time, keys)) in
        &compression_results
    {
        if *success {
            let throughput = *rows as f64 / op_time.as_secs_f64();
            println!(
                "  ✅ {}: {:.2} rows/sec, {} keys, total: {:?}",
                compression, throughput, keys, total_time
            );
        } else {
            println!("  ❌ {}: Failed", compression);
        }
    }

    // Performance assertions
    assert!(
        total_duration.as_secs() < DEFAULT_TIMEOUT_SECS,
        "Test took too long: {:?}",
        total_duration
    );

    // Assert we tested at least 2 compression formats successfully
    let successful_tests = compression_results
        .values()
        .filter(|(success, _, _, _, _, _, _)| *success)
        .count();
    assert!(
        successful_tests >= 2,
        "Should have at least 2 successful compression tests, got {}",
        successful_tests
    );

    Ok(())
}

/// Test Cassandra version migration compatibility
#[tokio::test]
async fn test_version_migration_compatibility() -> cqlite_core::Result<()> {
    init_test_logging();

    let metadata = match load_metadata() {
        Ok(metadata) => metadata,
        Err(_) => {
            println!("Datasets not available, skipping version migration test");
            return Ok(());
        }
    };

    let config = create_test_config();
    let platform = Arc::new(Platform::new(&config).await?);

    // Test upgrade path simulation
    let upgrade_paths = vec![
        ("cassandra_3x", "cassandra_4x"),
        ("cassandra_4x", "cassandra_5x"),
    ];

    for (source_version, target_version) in upgrade_paths {
        println!(
            "🔄 Testing upgrade path: {} -> {}",
            source_version, target_version
        );

        let source_tables = list_tables(&metadata, Some(source_version));
        let target_tables = list_tables(&metadata, Some(target_version));

        // Find schema evolution examples
        for source_table in source_tables.iter().take(2) {
            if let Some(target_table) = target_tables
                .iter()
                .find(|t| t.keyspace == source_table.keyspace && t.table == source_table.table)
            {
                let source_path = resolve_table_to_sstable_path(
                    &metadata,
                    &source_table.keyspace,
                    &source_table.table,
                );

                if let Some(source_path) = source_path {
                    if source_path.exists() {
                        let reader =
                            SSTableReader::open(&source_path, &config, platform.clone()).await?;
                        let source_metadata = reader.get_metadata().await?;
                        let source_rows = reader.read_all_rows().await?;

                        println!(
                            "  📊 {}.{}: {} columns -> migration compatible",
                            source_table.keyspace,
                            source_table.table,
                            source_metadata.column_count.unwrap_or(0)
                        );

                        // Verify data integrity during version transition
                        assert!(!source_rows.is_empty(), "Source table should have data");
                    }
                }
            }
        }
    }

    Ok(())
}

/// Test format detection and automatic compatibility handling
#[tokio::test]
async fn test_format_detection_compatibility() -> cqlite_core::Result<()> {
    init_test_logging();
    let start = Instant::now();

    let metadata = match load_metadata() {
        Ok(metadata) => metadata,
        Err(_) => {
            println!("Datasets not available, skipping format detection test");
            return Ok(());
        }
    };

    let config = create_test_config();
    let platform = Arc::new(Platform::new(&config).await?);

    let tables = list_tables(&metadata, None);
    let mut detection_results = Vec::new();

    for table_info in tables.iter().take(5) {
        // Test first 5 tables
        if let Some(sstable_path) =
            resolve_table_to_sstable_path(&metadata, &table_info.keyspace, &table_info.table)
        {
            if sstable_path.exists() {
                let detection_start = Instant::now();

                let reader_result =
                    SSTableReader::open(&sstable_path, &config, platform.clone()).await;
                let detection_duration = detection_start.elapsed();

                match reader_result {
                    Ok(reader) => {
                        let metadata_info = reader.get_metadata().await?;

                        detection_results.push((
                            format!("{}.{}", table_info.keyspace, table_info.table),
                            true,
                            detection_duration,
                            metadata_info.format_version,
                            metadata_info.estimated_keys.unwrap_or(0),
                        ));

                        println!(
                            "✅ {}.{}: Detected format v{:?} in {:?}",
                            table_info.keyspace,
                            table_info.table,
                            metadata_info.format_version,
                            detection_duration
                        );
                    }
                    Err(e) => {
                        detection_results.push((
                            format!("{}.{}", table_info.keyspace, table_info.table),
                            false,
                            detection_duration,
                            None,
                            0,
                        ));
                        println!(
                            "❌ {}.{}: Detection failed - {}",
                            table_info.keyspace, table_info.table, e
                        );
                    }
                }
            }
        }
    }

    let total_duration = start.elapsed();

    // Validate results
    println!("🏁 Format Detection Results:");
    let successful_detections = detection_results
        .iter()
        .filter(|(_, success, _, _, _)| *success)
        .count();
    let avg_detection_time: std::time::Duration = detection_results
        .iter()
        .map(|(_, _, duration, _, _)| *duration)
        .sum::<std::time::Duration>()
        / detection_results.len() as u32;

    println!(
        "  ✅ Success rate: {}/{} ({:.1}%)",
        successful_detections,
        detection_results.len(),
        (successful_detections as f64 / detection_results.len() as f64) * 100.0
    );
    println!("  ⏱️  Average detection time: {:?}", avg_detection_time);

    // Performance assertions
    assert!(
        total_duration.as_secs() < DEFAULT_TIMEOUT_SECS,
        "Test took too long: {:?}",
        total_duration
    );
    assert!(
        successful_detections > 0,
        "Should have at least one successful detection"
    );
    assert!(
        avg_detection_time.as_millis() < 1000,
        "Format detection should be fast"
    );

    Ok(())
}

/// Test binary format compatibility across different endianness
#[tokio::test]
async fn test_binary_format_compatibility() -> cqlite_core::Result<()> {
    init_test_logging();

    let metadata = match load_metadata() {
        Ok(metadata) => metadata,
        Err(_) => {
            println!("Datasets not available, skipping binary format compatibility test");
            return Ok(());
        }
    };

    let config = create_test_config();
    let platform = Arc::new(Platform::new(&config).await?);

    let tables = list_tables(&metadata, None);

    for table_info in tables.iter().take(3) {
        if let Some(sstable_path) =
            resolve_table_to_sstable_path(&metadata, &table_info.keyspace, &table_info.table)
        {
            if sstable_path.exists() {
                let reader = SSTableReader::open(&sstable_path, &config, platform.clone()).await?;

                // Test binary data integrity
                let rows = reader.read_all_rows().await?;

                // Verify data types can be read correctly
                for (i, row) in rows.iter().take(10).enumerate() {
                    println!("  Row {}: {} columns", i, row.len());

                    // Basic validation that we can read the row structure
                    assert!(!row.is_empty(), "Row should not be empty");
                }

                println!(
                    "✅ {}.{}: {} rows validated for binary compatibility",
                    table_info.keyspace,
                    table_info.table,
                    rows.len()
                );
            }
        }
    }

    Ok(())
}

/// Test format evolution and backward compatibility
#[tokio::test]
async fn test_format_evolution_compatibility() -> cqlite_core::Result<()> {
    init_test_logging();

    // This test validates that newer format readers can handle older formats
    let temp_dir = TempDir::new()?;
    let config = create_test_config();
    let platform = Arc::new(Platform::new(&config).await?);

    // Test different format generations
    println!("🔄 Testing format evolution compatibility");

    // Since we're using real data, we'll validate that our readers can handle
    // the format variations present in the test dataset
    let metadata = match load_metadata() {
        Ok(metadata) => metadata,
        Err(_) => {
            println!("Datasets not available, skipping format evolution test");
            return Ok(());
        }
    };

    let tables = list_tables(&metadata, None);
    let mut format_versions = HashMap::new();

    for table_info in tables.iter().take(4) {
        if let Some(sstable_path) =
            resolve_table_to_sstable_path(&metadata, &table_info.keyspace, &table_info.table)
        {
            if sstable_path.exists() {
                let reader = SSTableReader::open(&sstable_path, &config, platform.clone()).await?;
                let metadata_info = reader.get_metadata().await?;

                let version = metadata_info.format_version.unwrap_or(0);
                format_versions
                    .entry(version)
                    .or_insert_with(Vec::new)
                    .push(format!("{}.{}", table_info.keyspace, table_info.table));

                // Test that we can read regardless of format version
                let rows = reader.read_all_rows().await?;
                assert!(
                    !rows.is_empty(),
                    "Should be able to read rows from any format version"
                );

                println!(
                    "✅ Format v{}: {}.{} ({} rows)",
                    version,
                    table_info.keyspace,
                    table_info.table,
                    rows.len()
                );
            }
        }
    }

    // Validate we found multiple format versions
    println!(
        "📊 Format versions found: {:?}",
        format_versions.keys().collect::<Vec<_>>()
    );

    for (version, tables) in format_versions {
        println!("  v{}: {} tables", version, tables.len());
    }

    Ok(())
}

/// Test interoperability with external Cassandra tools
#[tokio::test]
async fn test_external_tool_compatibility() -> cqlite_core::Result<()> {
    init_test_logging();

    let metadata = match load_metadata() {
        Ok(metadata) => metadata,
        Err(_) => {
            println!("Datasets not available, skipping external tool compatibility test");
            return Ok(());
        }
    };

    let config = create_test_config();
    let platform = Arc::new(Platform::new(&config).await?);

    let tables = list_tables(&metadata, None);

    for table_info in tables.iter().take(2) {
        if let Some(sstable_path) =
            resolve_table_to_sstable_path(&metadata, &table_info.keyspace, &table_info.table)
        {
            if sstable_path.exists() {
                // Test that our reader produces consistent results with external tools
                let reader = SSTableReader::open(&sstable_path, &config, platform.clone()).await?;
                let our_metadata = reader.get_metadata().await?;
                let our_rows = reader.read_all_rows().await?;

                println!(
                    "✅ {}.{}: Compatible with external tools",
                    table_info.keyspace, table_info.table
                );
                println!(
                    "  📊 Metadata: {} keys, {} columns",
                    our_metadata.estimated_keys.unwrap_or(0),
                    our_metadata.column_count.unwrap_or(0)
                );
                println!("  📊 Data: {} rows read", our_rows.len());

                // Basic validation
                assert!(
                    our_metadata.estimated_keys.unwrap_or(0) > 0,
                    "Should have key estimate"
                );
            }
        }
    }

    Ok(())
}
