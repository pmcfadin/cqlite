//! Integration test demonstrating the SSTable test utilities
//!
//! This test validates that our common test utilities work correctly
//! with real SSTable data.

mod common;

use common::sstable_test_utils::{
    AssertionHelpers, DatasetUtils, PerformanceTestUtils, TestContext,
};

// Define the constant locally since it's not defined in the utilities
const MAX_MEMORY_USAGE_MB: usize = 100;
use cqlite_core::{Error, Result};

#[tokio::test]
async fn test_basic_dataset_discovery() -> Result<()> {
    // Test dataset discovery
    let datasets = DatasetUtils::get_available_datasets()?;
    assert!(!datasets.is_empty(), "Should find at least one dataset");

    println!("Available datasets: {:?}", datasets);

    // Should have test_basic dataset
    assert!(
        datasets.contains(&"test_basic".to_string()),
        "Should contain test_basic dataset"
    );

    Ok(())
}

#[tokio::test]
async fn test_dataset_descriptor_creation() -> Result<()> {
    let descriptor = DatasetUtils::create_dataset_descriptor("test_basic").await?;

    assert_eq!(descriptor.name, "test_basic");
    assert!(
        !descriptor.tables.is_empty(),
        "Should have at least one table"
    );

    println!("Dataset descriptor: {:?}", descriptor);

    // Verify table names are reasonable
    for table in &descriptor.tables {
        assert!(!table.name.is_empty(), "Table name should not be empty");
        assert!(
            !table.uuid_dir.is_empty(),
            "UUID directory should not be empty"
        );
        println!(
            "Found table: {} with components: {:?}",
            table.name, table.expected_components
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_context_creation_and_cleanup() -> Result<()> {
    let context = TestContext::new("test_basic").await?;

    // Validate context properties
    assert_eq!(context.cassandra_version, "5.0");
    assert!(context.dataset_path.exists(), "Dataset path should exist");
    assert!(
        context.temp_dir.path().exists(),
        "Temp directory should exist"
    );

    // Test cleanup
    let metrics = context.cleanup()?;
    assert_eq!(metrics.cache_hits, 0); // No operations performed yet
    assert_eq!(metrics.cache_misses, 0);

    Ok(())
}

#[tokio::test]
async fn test_table_discovery() -> Result<()> {
    let context = TestContext::new("test_basic").await?;
    let tables = context.get_available_tables()?;

    assert!(!tables.is_empty(), "Should discover at least one table");

    // Each table should have required components
    for table in &tables {
        assert!(
            !table.expected_components.is_empty(),
            "Table {} should have at least one component",
            table.name
        );
        println!("Table {}: {:?}", table.name, table.expected_components);
    }

    let _ = context.cleanup()?;
    Ok(())
}

#[tokio::test]
async fn test_performance_timing() -> Result<()> {
    // Test the timing utility
    let (result, duration) = PerformanceTestUtils::time_operation(|| async {
        // Simulate some work
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        Ok::<(), Error>(())
    })
    .await;

    assert!(result.is_ok(), "Timed operation should succeed");
    assert!(duration.as_millis() >= 10, "Should take at least 10ms");
    assert!(
        duration.as_millis() < 1000,
        "Should take less than 1 second"
    );

    println!("Timed operation took: {:?}", duration);
    Ok(())
}

#[tokio::test]
async fn test_concurrent_performance() -> Result<()> {
    let durations = PerformanceTestUtils::concurrent_access_test(
        || async {
            tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
            Ok(())
        },
        3, // Run 3 concurrent operations
    )
    .await;

    assert_eq!(durations.len(), 3, "Should have 3 duration measurements");

    for (i, duration) in durations.iter().enumerate() {
        println!("Concurrent operation {}: {:?}", i, duration);
        assert!(
            duration.as_millis() >= 5,
            "Each operation should take at least 5ms"
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_offset_validation_helpers() -> Result<()> {
    // Test valid offsets
    let valid_offsets = vec![(0, 100), (100, 200), (200, 300)];
    let result = AssertionHelpers::validate_offsets(300, &valid_offsets, "test_component");
    assert!(result.is_ok(), "Valid offsets should pass validation");

    // Test invalid offsets (out of bounds)
    let invalid_offsets = vec![(0, 100), (100, 200), (200, 400)];
    let result = AssertionHelpers::validate_offsets(300, &invalid_offsets, "test_component");
    assert!(result.is_err(), "Invalid offsets should fail validation");

    // Test invalid offsets (start >= end)
    let bad_offsets = vec![(0, 100), (150, 100)]; // Second offset has start > end
    let result = AssertionHelpers::validate_offsets(300, &bad_offsets, "test_component");
    assert!(result.is_err(), "Backwards offsets should fail validation");

    println!("Offset validation tests passed");
    Ok(())
}

#[tokio::test]
async fn test_metrics_collection() -> Result<()> {
    let mut context = TestContext::new("test_basic").await?;

    // Simulate some cache operations
    context.record_cache_hit();
    context.record_cache_hit();
    context.record_cache_miss();
    context.record_bytes_read(1024);

    // Check metrics
    assert_eq!(context.metrics.cache_hits, 2);
    assert_eq!(context.metrics.cache_misses, 1);
    assert_eq!(context.metrics.bytes_read, 1024);

    let hit_rate = context.cache_hit_rate();
    assert!(
        (hit_rate - 66.67).abs() < 0.1,
        "Hit rate should be ~66.67%, got {}",
        hit_rate
    );

    println!("Cache hit rate: {:.2}%", hit_rate);

    let _ = context.cleanup()?;
    Ok(())
}

#[tokio::test]
async fn test_memory_profiled_operation() -> Result<()> {
    let (result, memory_samples) = PerformanceTestUtils::memory_profiled_operation(|| async {
        // Simulate memory-intensive operation
        let _data: Vec<u8> = vec![0; 1024 * 1024]; // 1MB allocation
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        "completed"
    })
    .await;

    assert_eq!(result, "completed");

    if !memory_samples.is_empty() {
        println!("Collected {} memory samples", memory_samples.len());
        let max_memory = memory_samples.iter().max().copied().unwrap_or(0);
        println!("Peak memory usage: {} bytes", max_memory);

        // Memory should have increased during operation
        assert!(max_memory > 0, "Should record some memory usage");
    } else {
        println!("No memory samples collected (platform may not support memory monitoring)");
    }

    Ok(())
}

/// Demonstrates how to use the test utilities in a realistic scenario
#[tokio::test]
async fn test_realistic_usage_example() -> Result<()> {
    // This test demonstrates how other integration tests should use the utilities

    // 1. Create test context for specific dataset
    let mut context = TestContext::new("test_basic").await?;

    // 2. Discover available tables
    let tables = context.get_available_tables()?;
    println!("Testing with {} tables", tables.len());

    // 3. Verify each table has the expected components
    for table in &tables {
        let table_path = context.dataset_path.join(&table.uuid_dir);
        AssertionHelpers::verify_component_integrity(&table_path, &table.expected_components)
            .await?;

        println!("✓ Table {} has all expected components", table.name);
    }

    // 4. Record some metrics during operations
    context.record_cache_hit();
    context.record_bytes_read(4096);

    // 5. Verify final metrics are reasonable
    AssertionHelpers::verify_cache_metrics(&context, 0.0, MAX_MEMORY_USAGE_MB)?;

    // 6. Clean up and get final metrics
    let final_metrics = context.cleanup()?;
    println!(
        "Final metrics: cache hits={}, bytes read={}",
        final_metrics.cache_hits, final_metrics.bytes_read
    );

    Ok(())
}

#[cfg(test)]
mod performance_tests {
    use super::*;
    use std::time::Duration;

    /// Test that utilities themselves are performant
    #[tokio::test]
    async fn test_context_creation_performance() -> Result<()> {
        let start = std::time::Instant::now();

        let context = TestContext::new("test_basic").await?;
        let creation_time = start.elapsed();

        // Context creation should be fast
        assert!(
            creation_time < Duration::from_secs(1),
            "Context creation took too long: {:?}",
            creation_time
        );

        println!("Context creation took: {:?}", creation_time);

        let _ = context.cleanup()?;
        Ok(())
    }
}
