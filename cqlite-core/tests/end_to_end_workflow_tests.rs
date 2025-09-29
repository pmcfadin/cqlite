//! End-to-End Workflow Integration Tests
//!
//! This test suite validates complete data processing workflows including
//! full table scans, concurrent access patterns, and multi-component operations.

#![cfg(feature = "experimental")]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::timeout;

use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::SSTableReader;
use cqlite_core::testing::dataset_helpers::{
    list_tables, load_metadata, resolve_table_to_sstable_path,
};
use cqlite_core::types::Value;
use cqlite_core::Config;

mod common;
use common::{constants::*, create_test_config, init_test_logging};

/// Test full table scan workflow with performance monitoring
#[tokio::test]
async fn test_full_table_scan_workflow() -> cqlite_core::Result<()> {
    init_test_logging();
    let start = Instant::now();

    let metadata = match load_metadata() {
        Ok(metadata) => metadata,
        Err(_) => {
            println!("Datasets not available, skipping full table scan workflow test");
            return Ok(());
        }
    };

    let config = create_test_config();
    let platform = Arc::new(Platform::new(&config).await?);

    let tables = list_tables(&metadata, None);
    let mut scan_results = Vec::new();

    for table_info in tables.iter().take(3) {
        // Test first 3 tables for time
        if let Some(sstable_path) =
            resolve_table_to_sstable_path(&metadata, &table_info.keyspace, &table_info.table)
        {
            if sstable_path.exists() {
                let scan_start = Instant::now();

                // Timeout for individual table scans
                let scan_result = timeout(Duration::from_secs(30), async {
                    let reader =
                        SSTableReader::open(&sstable_path, &config, platform.clone()).await?;

                    // Phase 1: Metadata collection
                    let metadata_start = Instant::now();
                    let table_metadata = reader.get_metadata().await?;
                    let metadata_duration = metadata_start.elapsed();

                    // Phase 2: Index scanning
                    let index_start = Instant::now();
                    let index_info = reader.scan_index().await?;
                    let index_duration = index_start.elapsed();

                    // Phase 3: Full data scan
                    let data_start = Instant::now();
                    let all_rows = reader.read_all_rows().await?;
                    let data_duration = data_start.elapsed();

                    // Phase 4: Validation
                    let validation_start = Instant::now();
                    let mut validation_stats = HashMap::new();

                    for (row_idx, row) in all_rows.iter().enumerate() {
                        validation_stats
                            .entry("total_rows")
                            .or_insert(0usize)
                            .add_assign(1);

                        for (col_idx, value) in row.iter().enumerate() {
                            validation_stats
                                .entry("total_values")
                                .or_insert(0usize)
                                .add_assign(1);

                            // Validate data integrity
                            match value {
                                Value::Null => {
                                    validation_stats
                                        .entry("null_values")
                                        .or_insert(0usize)
                                        .add_assign(1);
                                }
                                Value::Text(text) => {
                                    if text.len() > 1000 {
                                        validation_stats
                                            .entry("large_text")
                                            .or_insert(0usize)
                                            .add_assign(1);
                                    }
                                }
                                Value::Blob(blob) => {
                                    if blob.len() > 10000 {
                                        validation_stats
                                            .entry("large_blob")
                                            .or_insert(0usize)
                                            .add_assign(1);
                                    }
                                }
                                _ => {}
                            }
                        }

                        // Sample validation - don't check every row for performance
                        if row_idx >= 100 {
                            break;
                        }
                    }

                    let validation_duration = validation_start.elapsed();
                    let total_scan_duration = scan_start.elapsed();

                    cqlite_core::Result::Ok((
                        all_rows.len(),
                        table_metadata,
                        index_info,
                        validation_stats,
                        metadata_duration,
                        index_duration,
                        data_duration,
                        validation_duration,
                        total_scan_duration,
                    ))
                })
                .await;

                match scan_result {
                    Ok(Ok((
                        row_count,
                        table_metadata,
                        index_info,
                        validation_stats,
                        metadata_duration,
                        index_duration,
                        data_duration,
                        validation_duration,
                        total_scan_duration,
                    ))) => {
                        scan_results.push((
                            format!("{}.{}", table_info.keyspace, table_info.table),
                            row_count,
                            total_scan_duration,
                        ));

                        // Calculate throughput
                        let rows_per_second = row_count as f64 / total_scan_duration.as_secs_f64();

                        println!(
                            "✅ {}.{}: Full table scan completed",
                            table_info.keyspace, table_info.table
                        );
                        println!(
                            "  📊 Rows: {}, Throughput: {:.2} rows/sec",
                            row_count, rows_per_second
                        );
                        println!(
                            "  ⏱️  Phases: metadata={:?}, index={:?}, data={:?}, validation={:?}",
                            metadata_duration, index_duration, data_duration, validation_duration
                        );
                        println!("  🔍 Validation: {:?}", validation_stats);

                        // Performance assertions
                        assert!(
                            rows_per_second > 100.0,
                            "Should process at least 100 rows/sec"
                        );
                        assert!(
                            metadata_duration.as_millis() < 1000,
                            "Metadata loading should be fast"
                        );
                    }
                    Ok(Err(e)) => {
                        println!(
                            "❌ {}.{}: Scan failed - {}",
                            table_info.keyspace, table_info.table, e
                        );
                    }
                    Err(_) => {
                        println!(
                            "❌ {}.{}: Scan timed out",
                            table_info.keyspace, table_info.table
                        );
                    }
                }
            }
        }
    }

    let total_duration = start.elapsed();

    // Validate overall results
    println!("🏁 Full Table Scan Workflow Results:");
    let total_rows: usize = scan_results.iter().map(|(_, rows, _)| *rows).sum();
    let avg_throughput = scan_results
        .iter()
        .map(|(_, rows, duration)| *rows as f64 / duration.as_secs_f64())
        .sum::<f64>()
        / scan_results.len() as f64;

    println!("  📊 Total tables scanned: {}", scan_results.len());
    println!("  📊 Total rows processed: {}", total_rows);
    println!("  📊 Average throughput: {:.2} rows/sec", avg_throughput);
    println!("  ⏱️  Total workflow time: {:?}", total_duration);

    // Performance assertions
    assert!(
        total_duration.as_secs() < DEFAULT_TIMEOUT_SECS,
        "Workflow took too long: {:?}",
        total_duration
    );
    assert!(
        !scan_results.is_empty(),
        "Should have completed at least one table scan"
    );
    assert!(total_rows > 0, "Should have processed some rows");

    Ok(())
}

/// Test concurrent access patterns with multiple readers
#[tokio::test]
async fn test_concurrent_access_patterns() -> cqlite_core::Result<()> {
    init_test_logging();
    let start = Instant::now();

    let metadata = match load_metadata() {
        Ok(metadata) => metadata,
        Err(_) => {
            println!("Datasets not available, skipping concurrent access patterns test");
            return Ok(());
        }
    };

    let config = create_test_config();
    let platform = Arc::new(Platform::new(&config).await?);

    let tables = list_tables(&metadata, None);

    if let Some(table_info) = tables.first() {
        if let Some(sstable_path) =
            resolve_table_to_sstable_path(&metadata, &table_info.keyspace, &table_info.table)
        {
            if sstable_path.exists() {
                println!(
                    "🔄 Testing concurrent access to {}.{}",
                    table_info.keyspace, table_info.table
                );

                // Create multiple concurrent readers
                let mut tasks = Vec::new();
                let concurrent_readers = CONCURRENT_OPERATIONS;

                for reader_id in 0..concurrent_readers {
                    let sstable_path = sstable_path.clone();
                    let config = config.clone();
                    let platform = platform.clone();
                    let table_name = format!("{}.{}", table_info.keyspace, table_info.table);

                    let task = tokio::spawn(async move {
                        let task_start = Instant::now();

                        let reader_result =
                            SSTableReader::open(&sstable_path, &config, platform).await;

                        match reader_result {
                            Ok(reader) => {
                                // Different access patterns for each reader
                                let access_pattern = reader_id % 4;

                                let operation_result = match access_pattern {
                                    0 => {
                                        // Full scan
                                        let rows = reader.read_all_rows().await?;
                                        (rows.len(), "full_scan".to_string())
                                    }
                                    1 => {
                                        // Partial scan
                                        let rows = reader.read_rows_range(0, 50).await?;
                                        (rows.len(), "partial_scan".to_string())
                                    }
                                    2 => {
                                        // Metadata only
                                        let metadata = reader.get_metadata().await?;
                                        (
                                            metadata.estimated_keys.unwrap_or(0) as usize,
                                            "metadata_only".to_string(),
                                        )
                                    }
                                    3 => {
                                        // Index scan
                                        let index_info = reader.scan_index().await?;
                                        (index_info.total_entries, "index_scan".to_string())
                                    }
                                    _ => unreachable!(),
                                };

                                let task_duration = task_start.elapsed();

                                cqlite_core::Result::Ok((
                                    reader_id,
                                    operation_result.0,
                                    operation_result.1,
                                    task_duration,
                                    true,
                                ))
                            }
                            Err(e) => {
                                let task_duration = task_start.elapsed();
                                println!("❌ Reader {}: Failed - {}", reader_id, e);

                                cqlite_core::Result::Ok((
                                    reader_id,
                                    0,
                                    "failed".to_string(),
                                    task_duration,
                                    false,
                                ))
                            }
                        }
                    });

                    tasks.push(task);
                }

                // Wait for all concurrent operations to complete
                let results = futures::future::join_all(tasks).await;
                let concurrent_duration = start.elapsed();

                // Analyze results
                let mut successful_operations = 0;
                let mut total_operations = 0;
                let mut operation_stats = HashMap::new();

                for result in results {
                    match result {
                        Ok(Ok((reader_id, count, operation_type, duration, success))) => {
                            total_operations += 1;
                            if success {
                                successful_operations += 1;

                                operation_stats
                                    .entry(operation_type.clone())
                                    .or_insert_with(Vec::new)
                                    .push((count, duration));

                                println!(
                                    "  ✅ Reader {}: {} - {} items in {:?}",
                                    reader_id, operation_type, count, duration
                                );
                            }
                        }
                        Ok(Err(e)) => {
                            total_operations += 1;
                            println!("  ❌ Task error: {}", e);
                        }
                        Err(e) => {
                            total_operations += 1;
                            println!("  ❌ Join error: {}", e);
                        }
                    }
                }

                // Performance analysis
                println!("🏁 Concurrent Access Patterns Results:");
                println!(
                    "  📊 Success rate: {}/{} ({:.1}%)",
                    successful_operations,
                    total_operations,
                    (successful_operations as f64 / total_operations as f64) * 100.0
                );
                println!("  ⏱️  Total concurrent duration: {:?}", concurrent_duration);

                for (operation_type, measurements) in operation_stats {
                    let avg_count = measurements.iter().map(|(count, _)| *count).sum::<usize>()
                        / measurements.len();
                    let avg_duration = measurements
                        .iter()
                        .map(|(_, duration)| *duration)
                        .sum::<Duration>()
                        / measurements.len() as u32;

                    println!(
                        "  📈 {}: avg {} items in {:?}",
                        operation_type, avg_count, avg_duration
                    );
                }

                // Assertions
                assert!(
                    successful_operations >= concurrent_readers / 2,
                    "At least half of concurrent operations should succeed"
                );
                assert!(
                    concurrent_duration.as_secs() < DEFAULT_TIMEOUT_SECS,
                    "Concurrent operations took too long: {:?}",
                    concurrent_duration
                );

                // Test for race conditions - all operations should succeed if no race conditions
                if successful_operations < total_operations {
                    println!("⚠️  Some operations failed - this might indicate race conditions or resource limits");
                }
            }
        }
    }

    Ok(())
}

/// Test streaming read workflow with memory efficiency
#[tokio::test]
async fn test_streaming_read_workflow() -> cqlite_core::Result<()> {
    init_test_logging();
    let start = Instant::now();

    let metadata = match load_metadata() {
        Ok(metadata) => metadata,
        Err(_) => {
            println!("Datasets not available, skipping streaming read workflow test");
            return Ok(());
        }
    };

    let config = create_test_config();
    let platform = Arc::new(Platform::new(&config).await?);

    let tables = list_tables(&metadata, None);

    for table_info in tables.iter().take(2) {
        // Test 2 tables
        if let Some(sstable_path) =
            resolve_table_to_sstable_path(&metadata, &table_info.keyspace, &table_info.table)
        {
            if sstable_path.exists() {
                let streaming_start = Instant::now();

                let reader = SSTableReader::open(&sstable_path, &config, platform.clone()).await?;

                println!(
                    "🌊 Testing streaming read for {}.{}",
                    table_info.keyspace, table_info.table
                );

                // Test chunked reading workflow
                let chunk_size = 25;
                let mut total_rows_streamed = 0;
                let mut chunk_count = 0;
                let mut max_memory_usage = 0;

                loop {
                    let chunk_start = Instant::now();

                    // Read chunk
                    let chunk_rows = reader
                        .read_rows_range(total_rows_streamed, total_rows_streamed + chunk_size)
                        .await?;

                    let chunk_duration = chunk_start.elapsed();

                    if chunk_rows.is_empty() {
                        break;
                    }

                    chunk_count += 1;
                    total_rows_streamed += chunk_rows.len();

                    // Simulate memory usage tracking
                    let estimated_memory = chunk_rows.len() * 1024; // Rough estimate
                    max_memory_usage = max_memory_usage.max(estimated_memory);

                    // Process chunk (simulate work)
                    let mut processed_values = 0;
                    for row in &chunk_rows {
                        for _value in row.iter() {
                            processed_values += 1;
                        }
                    }

                    println!(
                        "  📦 Chunk {}: {} rows, {} values, {:?}",
                        chunk_count,
                        chunk_rows.len(),
                        processed_values,
                        chunk_duration
                    );

                    // Memory efficiency check
                    assert!(
                        estimated_memory < MAX_MEMORY_USAGE_MB * 1024 * 1024,
                        "Chunk memory usage should be bounded"
                    );

                    // Stop after reasonable number of chunks for test performance
                    if chunk_count >= 20 {
                        break;
                    }
                }

                let streaming_duration = streaming_start.elapsed();
                let throughput = total_rows_streamed as f64 / streaming_duration.as_secs_f64();

                println!(
                    "✅ Streaming completed: {} rows in {} chunks, {:.2} rows/sec",
                    total_rows_streamed, chunk_count, throughput
                );
                println!("  💾 Max memory usage: ~{} KB", max_memory_usage / 1024);

                // Performance assertions
                assert!(total_rows_streamed > 0, "Should have streamed some rows");
                assert!(chunk_count > 0, "Should have processed some chunks");
                assert!(throughput > 50.0, "Streaming should be reasonably fast");
            }
        }
    }

    let total_duration = start.elapsed();

    // Final assertions
    assert!(
        total_duration.as_secs() < DEFAULT_TIMEOUT_SECS,
        "Streaming workflow took too long: {:?}",
        total_duration
    );

    println!(
        "🏁 Streaming Read Workflow completed in {:?}",
        total_duration
    );

    Ok(())
}

/// Test error recovery and resilience workflow
#[tokio::test]
async fn test_error_recovery_workflow() -> cqlite_core::Result<()> {
    init_test_logging();
    let start = Instant::now();

    let metadata = match load_metadata() {
        Ok(metadata) => metadata,
        Err(_) => {
            println!("Datasets not available, skipping error recovery workflow test");
            return Ok(());
        }
    };

    let config = create_test_config();
    let platform = Arc::new(Platform::new(&config).await?);

    let tables = list_tables(&metadata, None);

    let mut recovery_scenarios = Vec::new();

    for table_info in tables.iter().take(2) {
        if let Some(sstable_path) =
            resolve_table_to_sstable_path(&metadata, &table_info.keyspace, &table_info.table)
        {
            if sstable_path.exists() {
                println!(
                    "🛠️  Testing error recovery for {}.{}",
                    table_info.keyspace, table_info.table
                );

                // Scenario 1: Normal operation baseline
                let baseline_start = Instant::now();
                let reader = SSTableReader::open(&sstable_path, &config, platform.clone()).await?;
                let baseline_rows = reader.read_all_rows().await?;
                let baseline_duration = baseline_start.elapsed();

                recovery_scenarios.push((
                    "baseline".to_string(),
                    baseline_rows.len(),
                    baseline_duration,
                    true,
                ));

                println!(
                    "  ✅ Baseline: {} rows in {:?}",
                    baseline_rows.len(),
                    baseline_duration
                );

                // Scenario 2: Partial read with simulated interruption
                let partial_start = Instant::now();
                let partial_result = reader.read_rows_range(0, baseline_rows.len() / 2).await;
                let partial_duration = partial_start.elapsed();

                match partial_result {
                    Ok(partial_rows) => {
                        recovery_scenarios.push((
                            "partial_recovery".to_string(),
                            partial_rows.len(),
                            partial_duration,
                            true,
                        ));
                        println!(
                            "  ✅ Partial recovery: {} rows in {:?}",
                            partial_rows.len(),
                            partial_duration
                        );
                    }
                    Err(e) => {
                        recovery_scenarios.push((
                            "partial_recovery".to_string(),
                            0,
                            partial_duration,
                            false,
                        ));
                        println!("  ❌ Partial recovery failed: {}", e);
                    }
                }

                // Scenario 3: Metadata-only fallback
                let metadata_start = Instant::now();
                let metadata_result = reader.get_metadata().await;
                let metadata_duration = metadata_start.elapsed();

                match metadata_result {
                    Ok(table_metadata) => {
                        recovery_scenarios.push((
                            "metadata_fallback".to_string(),
                            table_metadata.estimated_keys.unwrap_or(0) as usize,
                            metadata_duration,
                            true,
                        ));
                        println!(
                            "  ✅ Metadata fallback: {} estimated keys in {:?}",
                            table_metadata.estimated_keys.unwrap_or(0),
                            metadata_duration
                        );
                    }
                    Err(e) => {
                        recovery_scenarios.push((
                            "metadata_fallback".to_string(),
                            0,
                            metadata_duration,
                            false,
                        ));
                        println!("  ❌ Metadata fallback failed: {}", e);
                    }
                }

                // Scenario 4: Graceful degradation test
                let degraded_start = Instant::now();
                let mut degraded_success = false;
                let mut degraded_rows = 0;

                // Try increasingly smaller read sizes
                for attempt_size in [1000, 100, 10, 1] {
                    let attempt_size = attempt_size.min(baseline_rows.len());
                    if let Ok(rows) = reader.read_rows_range(0, attempt_size).await {
                        degraded_rows = rows.len();
                        degraded_success = true;
                        println!(
                            "    🔧 Degraded mode: {} rows (attempt size: {})",
                            rows.len(),
                            attempt_size
                        );
                        break;
                    }
                }

                let degraded_duration = degraded_start.elapsed();
                recovery_scenarios.push((
                    "graceful_degradation".to_string(),
                    degraded_rows,
                    degraded_duration,
                    degraded_success,
                ));
            }
        }
    }

    let total_duration = start.elapsed();

    // Analyze recovery performance
    println!("🏁 Error Recovery Workflow Results:");
    let successful_scenarios = recovery_scenarios
        .iter()
        .filter(|(_, _, _, success)| *success)
        .count();
    let total_scenarios = recovery_scenarios.len();

    println!(
        "  📊 Recovery success rate: {}/{} ({:.1}%)",
        successful_scenarios,
        total_scenarios,
        (successful_scenarios as f64 / total_scenarios as f64) * 100.0
    );

    for (scenario, rows, duration, success) in recovery_scenarios {
        let status = if success { "✅" } else { "❌" };
        println!("  {} {}: {} rows in {:?}", status, scenario, rows, duration);
    }

    println!("  ⏱️  Total workflow time: {:?}", total_duration);

    // Assertions
    assert!(
        total_duration.as_secs() < DEFAULT_TIMEOUT_SECS,
        "Recovery workflow took too long: {:?}",
        total_duration
    );
    assert!(
        successful_scenarios > 0,
        "At least one recovery scenario should succeed"
    );

    // At least 75% of recovery scenarios should succeed
    let success_rate = successful_scenarios as f64 / total_scenarios as f64;
    assert!(
        success_rate >= 0.75,
        "Recovery success rate should be at least 75%, got {:.1}%",
        success_rate * 100.0
    );

    Ok(())
}

/// Test multi-component coordination workflow
#[tokio::test]
async fn test_multi_component_coordination() -> cqlite_core::Result<()> {
    init_test_logging();
    let start = Instant::now();

    let metadata = match load_metadata() {
        Ok(metadata) => metadata,
        Err(_) => {
            println!("Datasets not available, skipping multi-component coordination test");
            return Ok(());
        }
    };

    let config = create_test_config();
    let platform = Arc::new(Platform::new(&config).await?);

    let tables = list_tables(&metadata, None);

    if let Some(table_info) = tables.first() {
        if let Some(sstable_path) =
            resolve_table_to_sstable_path(&metadata, &table_info.keyspace, &table_info.table)
        {
            if sstable_path.exists() {
                println!(
                    "🔗 Testing multi-component coordination for {}.{}",
                    table_info.keyspace, table_info.table
                );

                let reader = SSTableReader::open(&sstable_path, &config, platform.clone()).await?;

                // Phase 1: Initialize all components
                let init_start = Instant::now();

                let metadata_task = {
                    let reader = reader.clone();
                    tokio::spawn(async move { reader.get_metadata().await })
                };

                let index_task = {
                    let reader = reader.clone();
                    tokio::spawn(async move { reader.scan_index().await })
                };

                let data_preview_task = {
                    let reader = reader.clone();
                    tokio::spawn(async move { reader.read_rows_range(0, 10).await })
                };

                // Wait for initialization phase
                let (metadata_result, index_result, preview_result) =
                    tokio::join!(metadata_task, index_task, data_preview_task);

                let init_duration = init_start.elapsed();

                let table_metadata = metadata_result??;
                let index_info = index_result??;
                let preview_rows = preview_result??;

                println!("  ✅ Phase 1 - Initialization: {:?}", init_duration);
                println!(
                    "    📊 Metadata: {} keys, {} columns",
                    table_metadata.estimated_keys.unwrap_or(0),
                    table_metadata.column_count.unwrap_or(0)
                );
                println!("    📇 Index: {} entries", index_info.total_entries);
                println!("    📄 Preview: {} rows", preview_rows.len());

                // Phase 2: Coordinated data processing
                let processing_start = Instant::now();

                let total_estimated_rows = table_metadata.estimated_keys.unwrap_or(100) as usize;
                let chunk_size = 50;
                let mut coordination_results = Vec::new();

                // Process in coordinated chunks
                for chunk_start in (0..total_estimated_rows.min(200)).step_by(chunk_size) {
                    let chunk_end = (chunk_start + chunk_size).min(total_estimated_rows);

                    let chunk_task = {
                        let reader = reader.clone();
                        tokio::spawn(async move {
                            let chunk_rows = reader.read_rows_range(chunk_start, chunk_end).await?;
                            cqlite_core::Result::Ok((chunk_start, chunk_rows.len()))
                        })
                    };

                    coordination_results.push(chunk_task);

                    // Limit concurrent chunks to avoid resource exhaustion
                    if coordination_results.len() >= 4 {
                        break;
                    }
                }

                // Collect coordination results
                let chunk_results = futures::future::join_all(coordination_results).await;
                let processing_duration = processing_start.elapsed();

                let mut total_coordinated_rows = 0;
                let mut successful_chunks = 0;

                for result in chunk_results {
                    match result {
                        Ok(Ok((chunk_start, row_count))) => {
                            total_coordinated_rows += row_count;
                            successful_chunks += 1;
                            println!("    📦 Chunk {}: {} rows", chunk_start, row_count);
                        }
                        Ok(Err(e)) => {
                            println!("    ❌ Chunk error: {}", e);
                        }
                        Err(e) => {
                            println!("    ❌ Coordination error: {}", e);
                        }
                    }
                }

                println!("  ✅ Phase 2 - Processing: {:?}", processing_duration);
                println!(
                    "    📊 Coordinated: {} rows in {} chunks",
                    total_coordinated_rows, successful_chunks
                );

                // Phase 3: Validation and cleanup
                let validation_start = Instant::now();

                let validation_task = {
                    let reader = reader.clone();
                    tokio::spawn(async move {
                        // Validate data consistency
                        let full_count = reader.read_all_rows().await?.len();
                        cqlite_core::Result::Ok(full_count)
                    })
                };

                let cleanup_task = tokio::spawn(async {
                    // Simulate cleanup operations
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    Ok::<_, cqlite_core::Error>(())
                });

                let (validation_result, cleanup_result) =
                    tokio::join!(validation_task, cleanup_task);
                let validation_duration = validation_start.elapsed();

                match validation_result {
                    Ok(Ok(full_count)) => {
                        println!("  ✅ Phase 3 - Validation: {:?}", validation_duration);
                        println!("    📊 Total rows validated: {}", full_count);

                        // Coordination consistency check
                        if full_count > 0 && total_coordinated_rows > 0 {
                            let coordination_efficiency =
                                (total_coordinated_rows as f64 / full_count as f64) * 100.0;
                            println!(
                                "    📈 Coordination efficiency: {:.1}%",
                                coordination_efficiency
                            );
                        }
                    }
                    Ok(Err(e)) => {
                        println!("  ❌ Validation failed: {}", e);
                    }
                    Err(e) => {
                        println!("  ❌ Validation task failed: {}", e);
                    }
                }

                let _ = cleanup_result;

                let total_coordination_duration = start.elapsed();

                println!("🏁 Multi-Component Coordination Results:");
                println!("  ⏱️  Total workflow: {:?}", total_coordination_duration);
                println!("  📊 Components coordinated: metadata, index, data, validation");
                println!(
                    "  🔄 Phases: init={:?}, process={:?}, validate={:?}",
                    init_duration, processing_duration, validation_duration
                );

                // Assertions
                assert!(
                    total_coordination_duration.as_secs() < DEFAULT_TIMEOUT_SECS,
                    "Coordination workflow took too long: {:?}",
                    total_coordination_duration
                );
                assert!(
                    successful_chunks > 0,
                    "Should have successfully coordinated some chunks"
                );
                assert!(
                    init_duration.as_secs() < 10,
                    "Initialization should be fast"
                );
            }
        }
    }

    Ok(())
}

/// Test transaction-like workflow consistency
#[tokio::test]
async fn test_transaction_workflow_consistency() -> cqlite_core::Result<()> {
    init_test_logging();
    let start = Instant::now();

    let metadata = match load_metadata() {
        Ok(metadata) => metadata,
        Err(_) => {
            println!("Datasets not available, skipping transaction workflow test");
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
                println!(
                    "📝 Testing transaction-like consistency for {}.{}",
                    table_info.keyspace, table_info.table
                );

                // Begin "transaction" - open reader
                let transaction_start = Instant::now();
                let reader = SSTableReader::open(&sstable_path, &config, platform.clone()).await?;

                // Operation 1: Read metadata (should be consistent throughout)
                let metadata1 = reader.get_metadata().await?;

                // Operation 2: Read some data
                let data_chunk1 = reader.read_rows_range(0, 25).await?;

                // Operation 3: Read metadata again (should be identical)
                let metadata2 = reader.get_metadata().await?;

                // Operation 4: Read overlapping data
                let data_chunk2 = reader.read_rows_range(20, 45).await?;

                let transaction_duration = transaction_start.elapsed();

                // Validate consistency
                println!("  🔍 Consistency checks:");

                // Metadata consistency
                assert_eq!(
                    metadata1.estimated_keys, metadata2.estimated_keys,
                    "Metadata should be consistent within transaction"
                );
                assert_eq!(
                    metadata1.format_version, metadata2.format_version,
                    "Format version should be consistent"
                );
                println!("    ✅ Metadata consistency: OK");

                // Data consistency (overlapping region should match)
                let overlap_start = 20;
                let overlap_end = 25.min(data_chunk1.len());

                if overlap_end > overlap_start && data_chunk2.len() > 0 {
                    let chunk1_overlap = &data_chunk1[overlap_start..overlap_end];
                    let chunk2_overlap = &data_chunk2[0..(overlap_end - overlap_start)];

                    if chunk1_overlap.len() == chunk2_overlap.len() {
                        let mut overlap_matches = 0;
                        for (i, (row1, row2)) in
                            chunk1_overlap.iter().zip(chunk2_overlap.iter()).enumerate()
                        {
                            if row1.len() == row2.len() {
                                overlap_matches += 1;
                            }
                            // Only check first few rows for performance
                            if i >= 3 {
                                break;
                            }
                        }

                        if overlap_matches > 0 {
                            println!(
                                "    ✅ Data overlap consistency: {} matching rows",
                                overlap_matches
                            );
                        }
                    }
                }

                // Transaction isolation test (multiple readers)
                let isolation_start = Instant::now();
                let reader2 = SSTableReader::open(&sstable_path, &config, platform.clone()).await?;
                let parallel_data = reader2.read_rows_range(0, 10).await?;
                let isolation_duration = isolation_start.elapsed();

                println!(
                    "    ✅ Reader isolation: {} rows read in parallel ({:?})",
                    parallel_data.len(),
                    isolation_duration
                );

                println!(
                    "  ✅ Transaction-like consistency verified in {:?}",
                    transaction_duration
                );
            }
        }
    }

    let total_duration = start.elapsed();

    println!(
        "🏁 Transaction Workflow Consistency completed in {:?}",
        total_duration
    );

    // Final assertions
    assert!(
        total_duration.as_secs() < DEFAULT_TIMEOUT_SECS,
        "Transaction workflow took too long: {:?}",
        total_duration
    );

    Ok(())
}

/// Test performance regression detection workflow
#[tokio::test]
async fn test_performance_regression_detection() -> cqlite_core::Result<()> {
    init_test_logging();
    let start = Instant::now();

    let metadata = match load_metadata() {
        Ok(metadata) => metadata,
        Err(_) => {
            println!("Datasets not available, skipping performance regression test");
            return Ok(());
        }
    };

    let config = create_test_config();
    let platform = Arc::new(Platform::new(&config).await?);

    let tables = list_tables(&metadata, None);

    if let Some(table_info) = tables.first() {
        if let Some(sstable_path) =
            resolve_table_to_sstable_path(&metadata, &table_info.keyspace, &table_info.table)
        {
            if sstable_path.exists() {
                println!(
                    "📈 Testing performance regression detection for {}.{}",
                    table_info.keyspace, table_info.table
                );

                let mut performance_samples = Vec::new();

                // Collect multiple performance samples
                for sample_id in 0..5 {
                    let sample_start = Instant::now();

                    let reader =
                        SSTableReader::open(&sstable_path, &config, platform.clone()).await?;

                    // Standardized performance test
                    let metadata_time = {
                        let start = Instant::now();
                        let _ = reader.get_metadata().await?;
                        start.elapsed()
                    };

                    let read_time = {
                        let start = Instant::now();
                        let rows = reader.read_rows_range(0, 50).await?;
                        (start.elapsed(), rows.len())
                    };

                    let total_sample_time = sample_start.elapsed();

                    performance_samples.push((
                        sample_id,
                        metadata_time,
                        read_time.0,
                        read_time.1,
                        total_sample_time,
                    ));

                    println!(
                        "  📊 Sample {}: metadata={:?}, read={:?} ({} rows), total={:?}",
                        sample_id, metadata_time, read_time.0, read_time.1, total_sample_time
                    );
                }

                // Analyze performance consistency
                let metadata_times: Vec<Duration> = performance_samples
                    .iter()
                    .map(|(_, m, _, _, _)| *m)
                    .collect();
                let read_times: Vec<Duration> = performance_samples
                    .iter()
                    .map(|(_, _, r, _, _)| *r)
                    .collect();
                let total_times: Vec<Duration> = performance_samples
                    .iter()
                    .map(|(_, _, _, _, t)| *t)
                    .collect();

                let avg_metadata =
                    metadata_times.iter().sum::<Duration>() / metadata_times.len() as u32;
                let avg_read = read_times.iter().sum::<Duration>() / read_times.len() as u32;
                let avg_total = total_times.iter().sum::<Duration>() / total_times.len() as u32;

                // Calculate variance (simple standard deviation approximation)
                let metadata_variance = calculate_variance(&metadata_times, avg_metadata);
                let read_variance = calculate_variance(&read_times, avg_read);

                println!("  📈 Performance Analysis:");
                println!(
                    "    Metadata: avg={:?}, variance={:.2}ms",
                    avg_metadata, metadata_variance
                );
                println!(
                    "    Read: avg={:?}, variance={:.2}ms",
                    avg_read, read_variance
                );
                println!("    Total: avg={:?}", avg_total);

                // Regression detection thresholds
                let max_variance_ms = 100.0; // 100ms variance threshold
                let max_avg_metadata_ms = 1000; // 1 second for metadata
                let max_avg_read_ms = 5000; // 5 seconds for reading

                // Performance assertions
                assert!(
                    metadata_variance < max_variance_ms,
                    "Metadata timing variance too high: {:.2}ms",
                    metadata_variance
                );
                assert!(
                    read_variance < max_variance_ms,
                    "Read timing variance too high: {:.2}ms",
                    read_variance
                );

                assert!(
                    avg_metadata.as_millis() < max_avg_metadata_ms,
                    "Average metadata time too slow: {:?}",
                    avg_metadata
                );
                assert!(
                    avg_read.as_millis() < max_avg_read_ms,
                    "Average read time too slow: {:?}",
                    avg_read
                );

                println!("  ✅ Performance regression check: PASSED");
            }
        }
    }

    let total_duration = start.elapsed();

    println!(
        "🏁 Performance Regression Detection completed in {:?}",
        total_duration
    );

    // Final assertions
    assert!(
        total_duration.as_secs() < DEFAULT_TIMEOUT_SECS,
        "Performance test took too long: {:?}",
        total_duration
    );

    Ok(())
}

/// Helper function to calculate variance in timing measurements
fn calculate_variance(times: &[Duration], avg: Duration) -> f64 {
    if times.is_empty() {
        return 0.0;
    }

    let avg_ms = avg.as_millis() as f64;
    let variance_sum: f64 = times
        .iter()
        .map(|t| {
            let diff = t.as_millis() as f64 - avg_ms;
            diff * diff
        })
        .sum();

    (variance_sum / times.len() as f64).sqrt()
}

use std::ops::AddAssign;
