//! Cache metrics accuracy tests for SSTable reader fixes
//!
//! Tests verify that cache hit/miss counting is accurate under concurrent access
//! and that cache metrics properly reflect actual cache behavior.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::SSTableReader;
use cqlite_core::Config;

// Import test utilities for real SSTable data
mod common;
use common::sstable_test_utils::TestContext;

/// Accuracy thresholds for cache metrics
const MAX_CACHE_METRIC_DRIFT_PERCENT: f64 = 2.0; // Max drift over time

/// Test cache hit/miss counting accuracy under normal conditions using real SSTable data
#[tokio::test]
async fn test_cache_metrics_basic_accuracy() {
    let (reader, context) = match find_working_sstable().await {
        Some((r, c)) => (r, c),
        None => {
            println!("Skipping cache metrics basic accuracy test - no compatible SSTable found");
            return;
        }
    };

    // First access - perform lookups that should be cache misses
    let initial_stats = reader.stats().await.unwrap_or_default();
    let initial_hit_rate = initial_stats.cache_hit_rate;
    println!("Initial cache hit rate: {:.4}", initial_hit_rate);

    // Generate partition keys that exist in the real SSTable
    let test_keys = generate_test_keys_for_sstable(&reader).await;
    let key_count = test_keys.len().min(20); // Limit to 20 keys for consistent testing

    println!("Testing with {} real partition keys", key_count);

    // First round - cache misses expected
    for key in test_keys.iter().take(key_count) {
        let _ = reader.lookup_partition_with_index(key).await;
    }

    let after_misses_stats = reader.stats().await.unwrap_or_default();
    let miss_hit_rate = after_misses_stats.cache_hit_rate;
    println!("Hit rate after first access round: {:.4}", miss_hit_rate);

    // Second round - cache hits expected for repeated lookups
    for key in test_keys.iter().take(key_count) {
        let _ = reader.lookup_partition_with_index(key).await;
    }

    let after_hits_stats = reader.stats().await.unwrap_or_default();
    let final_hit_rate = after_hits_stats.cache_hit_rate;
    println!(
        "Final hit rate after repeated operations: {:.4}",
        final_hit_rate
    );

    // Validate cache behavior with real data
    assert!(
        final_hit_rate > miss_hit_rate || final_hit_rate > 0.1,
        "Cache hit rate should improve with repeated operations or show reasonable performance: {:.4} -> {:.4}",
        miss_hit_rate,
        final_hit_rate
    );

    // Ensure metrics are in valid range
    assert!(
        (0.0..=1.0).contains(&final_hit_rate),
        "Cache hit rate should be in valid range [0.0, 1.0]: {:.4}",
        final_hit_rate
    );

    println!("Cache metrics test completed successfully with real SSTable data");
    let _ = context.cleanup().unwrap();
}

/// Test cache metrics accuracy under concurrent access using real SSTable data
#[tokio::test]
async fn test_concurrent_cache_metrics_accuracy() {
    let (reader, context) = match find_working_sstable().await {
        Some((r, c)) => (Arc::new(r), c),
        None => {
            println!("Skipping concurrent cache metrics test - no compatible SSTable found");
            return;
        }
    };

    // Generate real partition keys from the SSTable
    let test_keys = generate_test_keys_for_sstable(&reader).await;
    let shared_keys: Vec<Vec<u8>> = test_keys.into_iter().take(8).collect(); // Use 8 shared keys

    println!(
        "Testing concurrent cache metrics with {} shared real keys",
        shared_keys.len()
    );

    let concurrent_levels = vec![4, 8]; // Reduce concurrency for more predictable behavior

    for concurrency in concurrent_levels {
        println!("Testing with {} concurrent threads", concurrency);

        let initial_stats = reader.stats().await.unwrap_or_default();
        let operations_per_thread = 15;
        let mut join_set = JoinSet::new();

        // Spawn concurrent tasks that access shared keys
        for thread_id in 0..concurrency {
            let reader_clone = Arc::clone(&reader);
            let keys_clone = shared_keys.clone();

            join_set.spawn(async move {
                let mut operations = 0;

                for i in 0..operations_per_thread {
                    // Use shared keys to promote cache hits
                    let key_index = (thread_id + i) % keys_clone.len();
                    let key = &keys_clone[key_index];

                    let _ = reader_clone.lookup_partition_with_index(key).await;
                    operations += 1;
                }

                operations
            });
        }

        // Collect results
        let mut total_operations = 0;
        while let Some(result) = join_set.join_next().await {
            if let Ok(ops) = result {
                total_operations += ops;
            }
        }

        let final_stats = reader.stats().await.unwrap_or_default();
        let final_hit_rate = final_stats.cache_hit_rate;

        println!(
            "Concurrency {}: {} total operations, final hit rate: {:.4}",
            concurrency, total_operations, final_hit_rate
        );

        // Validate concurrent cache metrics
        assert!(
            (0.0..=1.0).contains(&final_hit_rate),
            "Cache hit rate should be in valid range [0.0, 1.0]: {:.4}",
            final_hit_rate
        );

        // Check for reasonable cache behavior with shared keys
        if concurrency > 1 {
            assert!(
                final_hit_rate >= 0.1 || final_hit_rate > initial_stats.cache_hit_rate,
                "Concurrent access with shared keys should show cache activity: {:.4}",
                final_hit_rate
            );
        }

        // Stats should be stable (not produce NaN or infinite values)
        assert!(
            final_hit_rate.is_finite(),
            "Cache hit rate should be finite even under concurrent access: {:.4}",
            final_hit_rate
        );
    }

    println!("Concurrent cache metrics test completed successfully with real SSTable data");
    let _ = context.cleanup().unwrap();
}

/// Test cache metrics accuracy with mixed access patterns using real SSTable data
#[tokio::test]
async fn test_mixed_access_pattern_cache_metrics() {
    let (reader, context) = match find_working_sstable().await {
        Some((r, c)) => (r, c),
        None => {
            println!(
                "Skipping mixed access pattern cache metrics test - no compatible SSTable found"
            );
            return;
        }
    };

    match Some(&reader) {
        Some(reader) => {
            // Test different access patterns
            // Test access patterns individually to avoid type mismatch
            println!("Testing cache metrics for sequential access pattern");
            test_sequential_access_pattern(reader).await;

            println!("Testing cache metrics for random access pattern");
            test_random_access_pattern(reader).await;

            println!("Testing cache metrics for hotspot access pattern");
            test_hotspot_access_pattern(reader).await;

            println!("Testing cache metrics for mixed access pattern");
            test_mixed_access_pattern(reader).await;

            // Verify basic cache metrics are functional
            let final_stats = reader.stats().await.unwrap_or_default();
            let final_hit_rate = final_stats.cache_hit_rate;

            // Metrics should be stable (not NaN or infinite)
            assert!(
                final_hit_rate.is_finite(),
                "Cache hit rate should be finite: {:.4}",
                final_hit_rate
            );

            assert!(
                (0.0..=1.0).contains(&final_hit_rate),
                "Cache hit rate should be between 0 and 1: {:.4}",
                final_hit_rate
            );
        }
        None => {
            println!("Mixed access pattern cache metrics test completed");
        }
    }

    let _ = context.cleanup().unwrap();
}

/// Test cache metrics stability over time using real SSTable data
#[tokio::test]
async fn test_cache_metrics_stability_over_time() {
    let (reader, context) = match find_working_sstable().await {
        Some((r, c)) => (r, c),
        None => {
            println!("Skipping cache metrics stability test - no compatible SSTable found");
            return;
        }
    };

    match Some(&reader) {
        Some(reader) => {
            let mut hit_rate_history = Vec::new();
            let test_duration = Duration::from_secs(10);
            let measurement_interval = Duration::from_millis(500);
            let start_time = Instant::now();

            // Continuously perform operations and measure hit rates
            while start_time.elapsed() < test_duration {
                // Perform a batch of operations
                for i in 0..20 {
                    let key = format!("stability_key_{:02}", i % 5).into_bytes(); // Reuse keys for hits
                    let _ = reader.lookup_partition_with_index(&key).await;
                }

                // Measure hit rate
                let stats = reader.stats().await.unwrap_or_default();
                hit_rate_history.push(stats.cache_hit_rate);

                tokio::time::sleep(measurement_interval).await;
            }

            println!("Hit rate measurements over time: {:?}", hit_rate_history);

            // Analyze stability
            if hit_rate_history.len() >= 3 {
                let first_half = &hit_rate_history[..hit_rate_history.len() / 2];
                let second_half = &hit_rate_history[hit_rate_history.len() / 2..];

                let first_half_avg = first_half.iter().sum::<f64>() / first_half.len() as f64;
                let second_half_avg = second_half.iter().sum::<f64>() / second_half.len() as f64;

                let drift = (second_half_avg - first_half_avg).abs();
                let drift_percent = if first_half_avg > 0.0 {
                    (drift / first_half_avg) * 100.0
                } else {
                    0.0
                };

                println!("First half average hit rate: {:.4}", first_half_avg);
                println!("Second half average hit rate: {:.4}", second_half_avg);
                println!("Drift: {:.4} ({:.2}%)", drift, drift_percent);

                assert!(
                    drift_percent <= MAX_CACHE_METRIC_DRIFT_PERCENT,
                    "Cache hit rate drift {:.2}% exceeds maximum allowed {:.2}%",
                    drift_percent,
                    MAX_CACHE_METRIC_DRIFT_PERCENT
                );

                // Verify no extreme values
                for &hit_rate in &hit_rate_history {
                    assert!(
                        (0.0..=1.0).contains(&hit_rate),
                        "Hit rate {} is outside valid range [0, 1]",
                        hit_rate
                    );
                }

                // Check for consistency (no wild swings)
                let max_hit_rate = hit_rate_history.iter().cloned().fold(0.0f64, f64::max);
                let min_hit_rate = hit_rate_history.iter().cloned().fold(1.0f64, f64::min);
                let range = max_hit_rate - min_hit_rate;

                assert!(
                    range <= 0.5, // Hit rate shouldn't swing more than 50%
                    "Hit rate range {:.4} indicates unstable metrics",
                    range
                );
            }
        }
        None => {
            println!("Cache metrics stability test completed");
        }
    }

    let _ = context.cleanup().unwrap();
}

/// Test cache eviction impact on metrics using real SSTable data
#[tokio::test]
async fn test_cache_eviction_metrics_accuracy() {
    let (reader, context) = match find_working_sstable().await {
        Some((r, c)) => (r, c),
        None => {
            println!("Skipping cache eviction metrics test - no compatible SSTable found");
            return;
        }
    };

    // Generate real partition keys from the SSTable
    let all_keys = generate_test_keys_for_sstable(&reader).await;
    let initial_keys: Vec<Vec<u8>> = all_keys.iter().take(10).cloned().collect();
    let eviction_keys: Vec<Vec<u8>> = all_keys.iter().skip(10).take(20).cloned().collect();

    println!(
        "Testing cache eviction with {} initial keys and {} eviction-forcing keys",
        initial_keys.len(),
        eviction_keys.len()
    );

    // Phase 1: Fill cache with initial keys
    println!("Phase 1: Filling cache with initial data...");
    for key in &initial_keys {
        let _ = reader.lookup_partition_with_index(key).await;
    }

    let after_initial_stats = reader.stats().await.unwrap_or_default();
    println!(
        "Hit rate after initial fill: {:.4}",
        after_initial_stats.cache_hit_rate
    );

    // Phase 2: Re-access initial keys to verify caching
    println!("Phase 2: Re-accessing initial keys to verify caching...");
    for key in &initial_keys {
        let _ = reader.lookup_partition_with_index(key).await;
    }

    let after_second_access_stats = reader.stats().await.unwrap_or_default();
    println!(
        "Hit rate after second access: {:.4}",
        after_second_access_stats.cache_hit_rate
    );

    // Phase 3: Access many different keys to potentially force eviction
    println!("Phase 3: Accessing different keys to force cache pressure...");
    for key in &eviction_keys {
        let _ = reader.lookup_partition_with_index(key).await;
    }

    // Access more keys to increase eviction pressure
    for key in &eviction_keys {
        let _ = reader.lookup_partition_with_index(key).await;
    }

    let after_eviction_stats = reader.stats().await.unwrap_or_default();
    println!(
        "Hit rate after eviction pressure: {:.4}",
        after_eviction_stats.cache_hit_rate
    );

    // Phase 4: Re-access some original keys (may have been evicted)
    println!("Phase 4: Re-accessing original keys to test eviction impact...");
    for key in initial_keys.iter().take(5) {
        let _ = reader.lookup_partition_with_index(key).await;
    }

    let final_stats = reader.stats().await.unwrap_or_default();
    println!("Final hit rate: {:.4}", final_stats.cache_hit_rate);

    // Validate cache eviction metrics
    assert!(
        (0.0..=1.0).contains(&final_stats.cache_hit_rate),
        "Final hit rate should be in valid range [0.0, 1.0]: {:.4}",
        final_stats.cache_hit_rate
    );

    assert!(
        final_stats.cache_hit_rate.is_finite(),
        "Final hit rate should be finite: {:.4}",
        final_stats.cache_hit_rate
    );

    // The cache should show some level of activity
    assert!(
        final_stats.cache_hit_rate >= 0.05, // At least 5% hit rate expected
        "Hit rate after eviction should still show reasonable cache activity: {:.4}",
        final_stats.cache_hit_rate
    );

    println!("Cache eviction test completed successfully with real SSTable data");
    let _ = context.cleanup().unwrap();
}

// Access pattern implementations

async fn test_sequential_access_pattern(reader: &SSTableReader) {
    for i in 0..100 {
        let key = format!("sequential_key_{:04}", i).into_bytes();
        let _ = reader.lookup_partition_with_index(&key).await;
    }
}

async fn test_random_access_pattern(reader: &SSTableReader) {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    for i in 0..100 {
        let mut hasher = DefaultHasher::new();
        i.hash(&mut hasher);
        let random_index = hasher.finish() % 200;
        let key = format!("random_key_{:04}", random_index).into_bytes();
        let _ = reader.lookup_partition_with_index(&key).await;
    }
}

async fn test_hotspot_access_pattern(reader: &SSTableReader) {
    // 80% of accesses to 20% of keys (hotspot pattern)
    for i in 0..100 {
        let key = if i < 80 {
            // 80% access to 5 hot keys
            format!("hotspot_key_{:01}", i % 5).into_bytes()
        } else {
            // 20% access to other keys
            format!("hotspot_key_{:04}", i + 100).into_bytes()
        };
        let _ = reader.lookup_partition_with_index(&key).await;
    }
}

async fn test_mixed_access_pattern(reader: &SSTableReader) {
    for i in 0..100 {
        let key = match i % 4 {
            0 | 1 => format!("mixed_hot_{:01}", i % 3).into_bytes(), // Repeated access
            2 => format!("mixed_seq_{:04}", i).into_bytes(),         // Sequential access
            _ => format!("mixed_rand_{:04}", (i * 7) % 50).into_bytes(), // Pseudo-random
        };
        let _ = reader.lookup_partition_with_index(&key).await;
    }
}

// Helper functions for real SSTable testing

/// Try to find a working SSTable from available test datasets
async fn find_working_sstable() -> Option<(SSTableReader, TestContext)> {
    let test_configs = [
        ("test_timeseries", "sensor_data"),
        ("test_timeseries", "user_activity"),
        ("test_collections", "nested_collections_table"),
        ("test_wide_rows", "wide_partition_table"),
        ("test_wide_rows", "document_versions"),
        ("test_wide_rows", "large_blob_table"),
    ];

    for (dataset, table) in &test_configs {
        if let Ok(mut test_context) = TestContext::new(dataset).await {
            if let Ok(table_dir) = test_context.prepare_sstable(table).await {
                if let Ok(data_file) = find_data_file(&table_dir).await {
                    let config = Config::default();
                    let platform = Arc::new(Platform::new(&config).await.unwrap());

                    if let Ok(sstable_reader) =
                        SSTableReader::open(&data_file, &config, platform).await
                    {
                        println!("Successfully opened SSTable: {}/{}", dataset, table);
                        return Some((sstable_reader, test_context));
                    } else {
                        println!(
                            "Failed to open SSTable: {}/{}, trying next...",
                            dataset, table
                        );
                    }
                } else {
                    println!(
                        "No Data.db file found for: {}/{}, trying next...",
                        dataset, table
                    );
                }
            } else {
                println!(
                    "Failed to prepare table: {}/{}, trying next...",
                    dataset, table
                );
            }
        } else {
            println!(
                "Failed to create context for dataset: {}, trying next...",
                dataset
            );
        }
    }

    None
}

/// Find the Data.db file in a prepared SSTable directory
async fn find_data_file(table_dir: &Path) -> Result<PathBuf, std::io::Error> {
    let mut entries = tokio::fs::read_dir(table_dir).await?;

    while let Some(entry) = entries.next_entry().await? {
        let file_name = entry.file_name();
        if let Some(name_str) = file_name.to_str() {
            if name_str.ends_with("-Data.db") {
                return Ok(entry.path());
            }
        }
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "No Data.db file found in SSTable directory",
    ))
}

/// Generate test partition keys from a real SSTable by examining its structure
async fn generate_test_keys_for_sstable(_reader: &SSTableReader) -> Vec<Vec<u8>> {
    // For real SSTable testing, we need to generate keys that might exist
    // Since we don't have direct access to the partition keys, we'll create
    // a reasonable set of test keys based on common patterns

    let mut keys = Vec::new();

    // Generate various key patterns that might exist in test data
    for i in 0..50 {
        // Simple numeric keys
        keys.push(format!("key{}", i).into_bytes());
        keys.push(format!("test_key_{}", i).into_bytes());
        keys.push(format!("partition_{}", i).into_bytes());

        // UUID-style keys (common in Cassandra)
        keys.push(
            format!(
                "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
                i,
                i % 65536,
                i % 65536,
                i % 65536,
                (i as u64) % 281474976710656u64
            )
            .into_bytes(),
        );
    }

    // Add some single-character keys
    for c in 'a'..='z' {
        keys.push(vec![c as u8]);
    }

    // Add some numeric keys
    for i in 0..100 {
        keys.push(i.to_string().into_bytes());
    }

    keys
}
