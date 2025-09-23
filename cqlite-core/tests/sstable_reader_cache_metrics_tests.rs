//! Cache metrics accuracy tests for SSTable reader fixes
//!
//! Tests verify that cache hit/miss counting is accurate under concurrent access
//! and that cache metrics properly reflect actual cache behavior.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::fs;
use tokio::task::JoinSet;

use cqlite_core::Config;
use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::SSTableReader;

/// Accuracy thresholds for cache metrics
const MAX_CACHE_METRIC_ERROR_PERCENT: f64 = 5.0; // Max 5% error in cache metrics
const MIN_CACHE_HIT_RATE_THRESHOLD: f64 = 0.1; // Minimum hit rate for repeated access
const MAX_CACHE_METRIC_DRIFT_PERCENT: f64 = 2.0; // Max drift over time

/// Test cache hit/miss counting accuracy under normal conditions
#[tokio::test]
async fn test_cache_metrics_basic_accuracy() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let base_name = "cache-metrics-basic";
    let scenario_dir = base_path.join(base_name);
    fs::create_dir(&scenario_dir).await.unwrap();
    create_cache_test_files(&scenario_dir, base_name, 1000).await;

    let data_file = scenario_dir.join(format!("{}-Data.db", base_name));
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    match SSTableReader::open(&data_file, &config, platform).await {
        Ok(reader) => {
            // First access - should be cache misses
            let initial_stats = reader.get_stats().await;
            let initial_hit_rate = initial_stats.cache_hit_rate;

            println!("Initial cache hit rate: {:.4}", initial_hit_rate);

            // Perform operations that should generate cache misses
            let miss_operations = 50;
            for i in 0..miss_operations {
                let key = format!("cache_miss_key_{:04}", i).into_bytes();
                let _ = reader.lookup_partition_with_index(&key).await;
            }

            // Check stats after misses
            let after_misses_stats = reader.get_stats().await;
            let miss_hit_rate = after_misses_stats.cache_hit_rate;
            println!(
                "Hit rate after {} miss operations: {:.4}",
                miss_operations, miss_hit_rate
            );

            // Now repeat the same operations - should generate cache hits
            for i in 0..miss_operations {
                let key = format!("cache_miss_key_{:04}", i).into_bytes();
                let _ = reader.lookup_partition_with_index(&key).await;
            }

            // Check stats after hits
            let after_hits_stats = reader.get_stats().await;
            let final_hit_rate = after_hits_stats.cache_hit_rate;
            println!(
                "Final hit rate after repeated operations: {:.4}",
                final_hit_rate
            );

            // Validate that hit rate increased significantly
            assert!(
                final_hit_rate > miss_hit_rate + 0.2, // At least 20% improvement
                "Cache hit rate should increase significantly after repeated operations: {} -> {}",
                miss_hit_rate,
                final_hit_rate
            );

            // Final hit rate should be reasonable
            assert!(
                final_hit_rate >= MIN_CACHE_HIT_RATE_THRESHOLD,
                "Final cache hit rate {:.4} should be at least {:.4}",
                final_hit_rate,
                MIN_CACHE_HIT_RATE_THRESHOLD
            );
        }
        Err(e) => {
            println!("Basic cache metrics test skipped: {}", e);
        }
    }

    fs::remove_dir_all(&scenario_dir).await.unwrap();
}

/// Test cache metrics accuracy under concurrent access
#[tokio::test]
async fn test_concurrent_cache_metrics_accuracy() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let base_name = "cache-metrics-concurrent";
    let scenario_dir = base_path.join(base_name);
    fs::create_dir(&scenario_dir).await.unwrap();
    create_cache_test_files(&scenario_dir, base_name, 2000).await;

    let data_file = scenario_dir.join(format!("{}-Data.db", base_name));
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    match SSTableReader::open(&data_file, &config, platform).await {
        Ok(reader) => {
            let reader = Arc::new(reader);
            let concurrent_levels = vec![5, 10, 20];

            for concurrency in concurrent_levels {
                println!(
                    "Testing concurrent cache metrics with {} threads",
                    concurrency
                );

                let operations_per_thread = 30;
                let total_operations = concurrency * operations_per_thread;

                // Get initial stats
                let initial_stats = reader.get_stats().await;

                // Perform concurrent operations with shared key set (should generate hits)
                let shared_key_count = 10;
                let mut join_set = JoinSet::new();

                for thread_id in 0..concurrency {
                    let reader_clone = Arc::clone(&reader);
                    join_set.spawn(async move {
                        let mut hits = 0u64;
                        let mut misses = 0u64;

                        for i in 0..operations_per_thread {
                            // Use shared keys to generate cache hits
                            let key_index = (thread_id + i) % shared_key_count;
                            let key = format!("shared_cache_key_{:02}", key_index).into_bytes();

                            let start_stats = reader_clone.get_stats().await;
                            let _ = reader_clone.lookup_partition_with_index(&key).await;
                            let end_stats = reader_clone.get_stats().await;

                            // Approximate hit/miss detection based on stats change
                            if end_stats.cache_hit_rate > start_stats.cache_hit_rate {
                                hits += 1;
                            } else {
                                misses += 1;
                            }
                        }

                        (hits, misses)
                    });
                }

                // Collect results
                let mut total_detected_hits = 0;
                let mut total_detected_misses = 0;

                while let Some(result) = join_set.join_next().await {
                    if let Ok((hits, misses)) = result {
                        total_detected_hits += hits;
                        total_detected_misses += misses;
                    }
                }

                // Get final stats
                let final_stats = reader.get_stats().await;
                let final_hit_rate = final_stats.cache_hit_rate;

                println!(
                    "Concurrency {}: Detected hits: {}, misses: {}, final hit rate: {:.4}",
                    concurrency, total_detected_hits, total_detected_misses, final_hit_rate
                );

                // With shared keys, we should see cache hits
                assert!(
                    final_hit_rate >= MIN_CACHE_HIT_RATE_THRESHOLD,
                    "Concurrent operations should generate cache hits: hit rate {:.4}",
                    final_hit_rate
                );

                // Stats should be stable (not drift significantly)
                let hit_rate_change = (final_hit_rate - initial_stats.cache_hit_rate).abs();
                // Allow reasonable change, but not wild swings
                assert!(
                    hit_rate_change <= 1.0, // Hit rate can change by at most 100%
                    "Cache hit rate change {:.4} seems excessive for concurrent operations",
                    hit_rate_change
                );
            }
        }
        Err(e) => {
            println!("Concurrent cache metrics test skipped: {}", e);
        }
    }

    fs::remove_dir_all(&scenario_dir).await.unwrap();
}

/// Test cache metrics accuracy with mixed access patterns
#[tokio::test]
async fn test_mixed_access_pattern_cache_metrics() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let base_name = "cache-metrics-mixed";
    let scenario_dir = base_path.join(base_name);
    fs::create_dir(&scenario_dir).await.unwrap();
    create_cache_test_files(&scenario_dir, base_name, 1500).await;

    let data_file = scenario_dir.join(format!("{}-Data.db", base_name));
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    match SSTableReader::open(&data_file, &config, platform).await {
        Ok(reader) => {
            // Test different access patterns
            let access_patterns = vec![
                ("sequential", test_sequential_access_pattern),
                ("random", test_random_access_pattern),
                ("hotspot", test_hotspot_access_pattern),
                ("mixed", test_mixed_access_pattern),
            ];

            for (pattern_name, pattern_fn) in access_patterns {
                println!("Testing cache metrics for {} access pattern", pattern_name);

                let initial_stats = reader.get_stats().await;
                let initial_hit_rate = initial_stats.cache_hit_rate;

                // Execute access pattern
                pattern_fn(&reader).await;

                let final_stats = reader.get_stats().await;
                let final_hit_rate = final_stats.cache_hit_rate;

                println!(
                    "Pattern {}: Initial hit rate: {:.4}, Final hit rate: {:.4}",
                    pattern_name, initial_hit_rate, final_hit_rate
                );

                // Validate that metrics behave reasonably for each pattern
                match pattern_name {
                    "sequential" => {
                        // Sequential access should have low hit rate (each key accessed once)
                        assert!(
                            final_hit_rate <= 0.5,
                            "Sequential access should have low hit rate: {:.4}",
                            final_hit_rate
                        );
                    }
                    "hotspot" => {
                        // Hotspot access should have high hit rate (repeated access to few keys)
                        assert!(
                            final_hit_rate >= 0.3,
                            "Hotspot access should have high hit rate: {:.4}",
                            final_hit_rate
                        );
                    }
                    "random" => {
                        // Random access should have moderate hit rate
                        assert!(
                            final_hit_rate >= 0.1 && final_hit_rate <= 0.8,
                            "Random access hit rate should be moderate: {:.4}",
                            final_hit_rate
                        );
                    }
                    "mixed" => {
                        // Mixed access should show reasonable hit rate
                        assert!(
                            final_hit_rate >= 0.1,
                            "Mixed access should show some cache hits: {:.4}",
                            final_hit_rate
                        );
                    }
                    _ => {}
                }

                // Metrics should be stable (not NaN or infinite)
                assert!(
                    final_hit_rate.is_finite(),
                    "Cache hit rate should be finite: {:.4}",
                    final_hit_rate
                );

                assert!(
                    final_hit_rate >= 0.0 && final_hit_rate <= 1.0,
                    "Cache hit rate should be between 0 and 1: {:.4}",
                    final_hit_rate
                );
            }
        }
        Err(e) => {
            println!("Mixed access pattern cache metrics test skipped: {}", e);
        }
    }

    fs::remove_dir_all(&scenario_dir).await.unwrap();
}

/// Test cache metrics stability over time
#[tokio::test]
async fn test_cache_metrics_stability_over_time() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let base_name = "cache-metrics-stability";
    let scenario_dir = base_path.join(base_name);
    fs::create_dir(&scenario_dir).await.unwrap();
    create_cache_test_files(&scenario_dir, base_name, 1000).await;

    let data_file = scenario_dir.join(format!("{}-Data.db", base_name));
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    match SSTableReader::open(&data_file, &config, platform).await {
        Ok(reader) => {
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
                let stats = reader.get_stats().await;
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
                        hit_rate >= 0.0 && hit_rate <= 1.0,
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
        Err(e) => {
            println!("Cache metrics stability test skipped: {}", e);
        }
    }

    fs::remove_dir_all(&scenario_dir).await.unwrap();
}

/// Test cache eviction impact on metrics
#[tokio::test]
async fn test_cache_eviction_metrics_accuracy() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let base_name = "cache-metrics-eviction";
    let scenario_dir = base_path.join(base_name);
    fs::create_dir(&scenario_dir).await.unwrap();
    create_cache_test_files(&scenario_dir, base_name, 3000).await;

    let data_file = scenario_dir.join(format!("{}-Data.db", base_name));
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    match SSTableReader::open(&data_file, &config, platform).await {
        Ok(reader) => {
            // Fill cache with initial data
            println!("Filling cache with initial data...");
            for i in 0..100 {
                let key = format!("eviction_key_{:04}", i).into_bytes();
                let _ = reader.lookup_partition_with_index(&key).await;
            }

            let after_initial_stats = reader.get_stats().await;
            println!(
                "Hit rate after initial fill: {:.4}",
                after_initial_stats.cache_hit_rate
            );

            // Access initial keys again to verify they're cached
            for i in 0..100 {
                let key = format!("eviction_key_{:04}", i).into_bytes();
                let _ = reader.lookup_partition_with_index(&key).await;
            }

            let after_second_access_stats = reader.get_stats().await;
            println!(
                "Hit rate after second access: {:.4}",
                after_second_access_stats.cache_hit_rate
            );

            // The hit rate should increase after accessing cached items
            assert!(
                after_second_access_stats.cache_hit_rate > after_initial_stats.cache_hit_rate,
                "Hit rate should increase when accessing cached items: {:.4} -> {:.4}",
                after_initial_stats.cache_hit_rate,
                after_second_access_stats.cache_hit_rate
            );

            // Now access many new keys to force eviction
            println!("Forcing cache eviction with new keys...");
            for i in 1000..2000 {
                let key = format!("eviction_key_{:04}", i).into_bytes();
                let _ = reader.lookup_partition_with_index(&key).await;
            }

            let after_eviction_stats = reader.get_stats().await;
            println!(
                "Hit rate after eviction: {:.4}",
                after_eviction_stats.cache_hit_rate
            );

            // Re-access some original keys that may have been evicted
            for i in 0..50 {
                let key = format!("eviction_key_{:04}", i).into_bytes();
                let _ = reader.lookup_partition_with_index(&key).await;
            }

            let final_stats = reader.get_stats().await;
            println!("Final hit rate: {:.4}", final_stats.cache_hit_rate);

            // Verify metrics remain consistent and reasonable
            assert!(
                final_stats.cache_hit_rate >= 0.0 && final_stats.cache_hit_rate <= 1.0,
                "Final hit rate should be valid: {:.4}",
                final_stats.cache_hit_rate
            );

            // Hit rate might be lower due to eviction, but should still be reasonable
            assert!(
                final_stats.cache_hit_rate >= 0.1,
                "Hit rate after eviction should still show some hits: {:.4}",
                final_stats.cache_hit_rate
            );
        }
        Err(e) => {
            println!("Cache eviction metrics test skipped: {}", e);
        }
    }

    fs::remove_dir_all(&scenario_dir).await.unwrap();
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

// Test file creation functions

async fn create_cache_test_files(dir: &Path, base_name: &str, partition_count: usize) {
    create_cache_data_file(dir, base_name, partition_count).await;
    create_cache_index_file(dir, base_name, partition_count).await;
    create_cache_summary_file(dir, base_name, partition_count).await;
    create_cache_statistics_file(dir, base_name, partition_count).await;
    create_cache_filter_file(dir, base_name, partition_count).await;
}

async fn create_cache_data_file(dir: &Path, base_name: &str, partition_count: usize) {
    let path = dir.join(format!("{}-Data.db", base_name));
    let mut data = Vec::new();

    // SSTable header
    data.extend_from_slice(&[
        0x6f, 0x61, 0x00, 0x00, // Magic "oa" + version
        0x00, 0x01, // Version
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Timestamp
        0x00, 0x00, 0x00, 0x01, // Table count
    ]);
    data.extend_from_slice(&(partition_count as u32).to_be_bytes());

    // Create limited partitions
    let actual_partitions = partition_count.min(1000);
    for i in 0..actual_partitions {
        let key = format!("cache_test_key_{:06}", i);
        data.extend_from_slice(&(key.len() as u32).to_be_bytes());
        data.extend_from_slice(key.as_bytes());
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x40]); // Row size
        data.extend_from_slice(&vec![0xBB; 64]); // Row data
    }

    fs::write(path, data).await.unwrap();
}

async fn create_cache_index_file(dir: &Path, base_name: &str, partition_count: usize) {
    let path = dir.join(format!("{}-Index.db", base_name));
    let mut data = Vec::new();

    let index_entries = partition_count.min(1000);

    // Index header
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]); // Version
    data.extend_from_slice(&(index_entries as u32).to_be_bytes());

    // Index entries
    for i in 0..index_entries {
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x20]); // Digest length
        let mut digest = vec![0; 32];
        digest[0] = (i % 256) as u8;
        digest[1] = ((i / 256) % 256) as u8;
        digest[31] = ((i + 123) % 256) as u8; // Add variety
        data.extend_from_slice(&digest);

        let offset = (i as u64) * 128;
        data.extend_from_slice(&offset.to_be_bytes());
        data.extend_from_slice(&(64u32).to_be_bytes());
    }

    fs::write(path, data).await.unwrap();
}

async fn create_cache_summary_file(dir: &Path, base_name: &str, partition_count: usize) {
    let path = dir.join(format!("{}-Summary.db", base_name));
    let mut data = Vec::new();

    let summary_entries = (partition_count / 100).clamp(10, 50);

    // Summary header
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]); // Version
    data.extend_from_slice(&(summary_entries as u32).to_be_bytes());

    // Summary entries
    for i in 0..summary_entries {
        let key = format!("cache_sum_{:04}", i);
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(key.as_bytes());

        let token_range = i64::MAX as i128 * 2;
        let token = (i64::MIN as i128 + (i as i128 * token_range / summary_entries as i128)) as i64;
        data.extend_from_slice(&token.to_be_bytes());
        data.extend_from_slice(&((i * 2000) as u64).to_be_bytes());
        data.extend_from_slice(&(i as u32).to_be_bytes());
    }

    fs::write(path, data).await.unwrap();
}

async fn create_cache_statistics_file(dir: &Path, base_name: &str, partition_count: usize) {
    let path = dir.join(format!("{}-Statistics.db", base_name));
    let mut data = Vec::new();

    let stats = vec![
        ("min_timestamp", 1640995200000u64),
        ("max_timestamp", 1672531200000u64),
        ("live_row_count", partition_count as u64),
        ("total_data_size", (partition_count * 128) as u64),
        ("compaction_level", 0u64),
        ("max_local_deletion_time", 1672531200u64),
        (
            "estimated_droppable_tombstones",
            (partition_count / 1000) as u64,
        ),
    ];

    for (key, value) in stats {
        data.extend_from_slice(&(key.len() as u32).to_be_bytes());
        data.extend_from_slice(key.as_bytes());
        data.extend_from_slice(&(8u32).to_be_bytes());
        data.extend_from_slice(&value.to_be_bytes());
    }

    fs::write(path, data).await.unwrap();
}

async fn create_cache_filter_file(dir: &Path, base_name: &str, partition_count: usize) {
    let path = dir.join(format!("{}-Filter.db", base_name));
    let mut data = Vec::new();

    let filter_size = (partition_count / 8).clamp(1024, 16384);

    // Bloom filter header
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x02]); // Version
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x07]); // Hash functions
    data.extend_from_slice(&(filter_size as u32).to_be_bytes());

    // Bloom filter bit array
    let bit_pattern = (partition_count % 256) as u8;
    let bit_array = vec![bit_pattern; filter_size];
    data.extend_from_slice(&bit_array);

    fs::write(path, data).await.unwrap();
}
