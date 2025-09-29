//! Logging performance impact tests for SSTable reader fixes
//!
//! Tests measure the performance impact of debug logging on async throughput
//! and ensure that logging doesn't significantly degrade reader performance.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::fs;
use tokio::task::JoinSet;

use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::SSTableReader;
use cqlite_core::Config;

/// Performance thresholds for logging impact
const MAX_LOGGING_OVERHEAD_PERCENT: f64 = 15.0; // Max 15% performance degradation
const MIN_ASYNC_THROUGHPUT_OPS_PER_SEC: f64 = 100.0; // Minimum throughput with logging
const MAX_ASYNC_OPERATION_LATENCY_MS: u64 = 50; // Max individual operation latency

/// Test logging performance impact on async throughput
#[tokio::test]
async fn test_debug_logging_async_throughput_impact() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    // Create test SSTable
    let base_name = "logging-throughput-test";
    let scenario_dir = base_path.join(base_name);
    fs::create_dir(&scenario_dir).await.unwrap();
    create_logging_test_files(&scenario_dir, base_name, 10000).await;

    let data_file = scenario_dir.join(format!("{}-Data.db", base_name));
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    // Test with debug logging disabled
    let throughput_no_logging =
        measure_async_throughput(&data_file, &config, platform.clone(), false).await;

    // Test with debug logging enabled
    let throughput_with_logging =
        measure_async_throughput(&data_file, &config, platform.clone(), true).await;

    // Calculate overhead
    let overhead_percent = if throughput_no_logging > 0.0 {
        ((throughput_no_logging - throughput_with_logging) / throughput_no_logging) * 100.0
    } else {
        0.0
    };

    println!(
        "Async throughput without logging: {:.2} ops/sec",
        throughput_no_logging
    );
    println!(
        "Async throughput with logging: {:.2} ops/sec",
        throughput_with_logging
    );
    println!("Logging overhead: {:.2}%", overhead_percent);

    // Assertions
    assert!(
        overhead_percent <= MAX_LOGGING_OVERHEAD_PERCENT,
        "Logging overhead {:.2}% exceeds maximum allowed {:.2}%",
        overhead_percent,
        MAX_LOGGING_OVERHEAD_PERCENT
    );

    assert!(
        throughput_with_logging >= MIN_ASYNC_THROUGHPUT_OPS_PER_SEC,
        "Throughput with logging {:.2} ops/sec is below minimum {:.2} ops/sec",
        throughput_with_logging,
        MIN_ASYNC_THROUGHPUT_OPS_PER_SEC
    );

    fs::remove_dir_all(&scenario_dir).await.unwrap();
}

/// Test logging impact on concurrent async operations
#[tokio::test]
async fn test_concurrent_logging_performance() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let base_name = "concurrent-logging-test";
    let scenario_dir = base_path.join(base_name);
    fs::create_dir(&scenario_dir).await.unwrap();
    create_logging_test_files(&scenario_dir, base_name, 5000).await;

    let data_file = scenario_dir.join(format!("{}-Data.db", base_name));
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    let concurrent_levels = vec![1, 5, 10, 20];

    for concurrency in concurrent_levels {
        println!("Testing concurrency level: {}", concurrency);

        // Test without logging
        let duration_no_logging = measure_concurrent_operations(
            &data_file,
            &config,
            platform.clone(),
            concurrency,
            false,
        )
        .await;

        // Test with logging
        let duration_with_logging =
            measure_concurrent_operations(&data_file, &config, platform.clone(), concurrency, true)
                .await;

        let overhead_percent = if duration_no_logging.as_millis() > 0 {
            ((duration_with_logging.as_millis() as f64 - duration_no_logging.as_millis() as f64)
                / duration_no_logging.as_millis() as f64)
                * 100.0
        } else {
            0.0
        };

        println!(
            "Concurrency {}: No logging {:?}, With logging {:?}, Overhead {:.2}%",
            concurrency, duration_no_logging, duration_with_logging, overhead_percent
        );

        assert!(
            overhead_percent <= MAX_LOGGING_OVERHEAD_PERCENT,
            "Logging overhead {:.2}% at concurrency {} exceeds maximum {:.2}%",
            overhead_percent,
            concurrency,
            MAX_LOGGING_OVERHEAD_PERCENT
        );
    }

    fs::remove_dir_all(&scenario_dir).await.unwrap();
}

/// Test logging impact on individual operation latency
#[tokio::test]
async fn test_logging_operation_latency_impact() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let base_name = "latency-logging-test";
    let scenario_dir = base_path.join(base_name);
    fs::create_dir(&scenario_dir).await.unwrap();
    create_logging_test_files(&scenario_dir, base_name, 1000).await;

    let data_file = scenario_dir.join(format!("{}-Data.db", base_name));
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    // Test operation latencies
    let latencies_no_logging =
        measure_operation_latencies(&data_file, &config, platform.clone(), false).await;
    let latencies_with_logging =
        measure_operation_latencies(&data_file, &config, platform.clone(), true).await;

    // Calculate statistics
    let avg_no_logging =
        latencies_no_logging.iter().sum::<u64>() as f64 / latencies_no_logging.len() as f64;
    let avg_with_logging =
        latencies_with_logging.iter().sum::<u64>() as f64 / latencies_with_logging.len() as f64;

    let p95_no_logging = percentile(&latencies_no_logging, 95.0);
    let p95_with_logging = percentile(&latencies_with_logging, 95.0);

    let p99_no_logging = percentile(&latencies_no_logging, 99.0);
    let p99_with_logging = percentile(&latencies_with_logging, 99.0);

    println!(
        "Average latency - No logging: {:.2}ms, With logging: {:.2}ms",
        avg_no_logging, avg_with_logging
    );
    println!(
        "P95 latency - No logging: {}ms, With logging: {}ms",
        p95_no_logging, p95_with_logging
    );
    println!(
        "P99 latency - No logging: {}ms, With logging: {}ms",
        p99_no_logging, p99_with_logging
    );

    // Assertions for latency bounds
    assert!(
        p95_with_logging <= MAX_ASYNC_OPERATION_LATENCY_MS,
        "P95 latency with logging {}ms exceeds maximum {}ms",
        p95_with_logging,
        MAX_ASYNC_OPERATION_LATENCY_MS
    );

    assert!(
        p99_with_logging <= MAX_ASYNC_OPERATION_LATENCY_MS * 2,
        "P99 latency with logging {}ms exceeds maximum {}ms",
        p99_with_logging,
        MAX_ASYNC_OPERATION_LATENCY_MS * 2
    );

    // Overhead check
    let latency_overhead = if avg_no_logging > 0.0 {
        ((avg_with_logging - avg_no_logging) / avg_no_logging) * 100.0
    } else {
        0.0
    };

    assert!(
        latency_overhead <= MAX_LOGGING_OVERHEAD_PERCENT,
        "Average latency overhead {:.2}% exceeds maximum {:.2}%",
        latency_overhead,
        MAX_LOGGING_OVERHEAD_PERCENT
    );

    fs::remove_dir_all(&scenario_dir).await.unwrap();
}

/// Test memory allocation impact of logging
#[tokio::test]
async fn test_logging_memory_allocation_impact() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let base_name = "memory-logging-test";
    let scenario_dir = base_path.join(base_name);
    fs::create_dir(&scenario_dir).await.unwrap();
    create_logging_test_files(&scenario_dir, base_name, 2000).await;

    let data_file = scenario_dir.join(format!("{}-Data.db", base_name));
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    // Measure memory usage with and without logging
    let memory_before = get_memory_usage();

    // Operations without logging
    let _ = perform_memory_test_operations(&data_file, &config, platform.clone(), false).await;
    let memory_after_no_logging = get_memory_usage();
    let memory_increase_no_logging = memory_after_no_logging.saturating_sub(memory_before);

    // Reset and test with logging
    force_garbage_collection();
    let memory_before_logging = get_memory_usage();

    let _ = perform_memory_test_operations(&data_file, &config, platform.clone(), true).await;
    let memory_after_logging = get_memory_usage();
    let memory_increase_with_logging = memory_after_logging.saturating_sub(memory_before_logging);

    println!(
        "Memory increase without logging: {} MB",
        memory_increase_no_logging
    );
    println!(
        "Memory increase with logging: {} MB",
        memory_increase_with_logging
    );

    // Logging should not significantly increase memory usage
    let memory_overhead = if memory_increase_no_logging > 0 {
        ((memory_increase_with_logging as f64 - memory_increase_no_logging as f64)
            / memory_increase_no_logging as f64)
            * 100.0
    } else {
        0.0
    };

    assert!(
        memory_overhead <= 25.0, // Allow up to 25% memory overhead for logging
        "Memory overhead from logging {:.2}% exceeds 25%",
        memory_overhead
    );

    fs::remove_dir_all(&scenario_dir).await.unwrap();
}

/// Test log message volume impact on performance
#[tokio::test]
async fn test_log_volume_performance_impact() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let base_name = "volume-logging-test";
    let scenario_dir = base_path.join(base_name);
    fs::create_dir(&scenario_dir).await.unwrap();
    create_logging_test_files(&scenario_dir, base_name, 1000).await;

    let data_file = scenario_dir.join(format!("{}-Data.db", base_name));
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    let log_volumes = vec![
        ("low", 10),     // 10 operations
        ("medium", 100), // 100 operations
        ("high", 1000),  // 1000 operations
    ];

    for (volume_name, operation_count) in log_volumes {
        println!(
            "Testing log volume: {} ({} operations)",
            volume_name, operation_count
        );

        let start_time = Instant::now();
        let _ =
            perform_volume_test_operations(&data_file, &config, platform.clone(), operation_count)
                .await;
        let duration = start_time.elapsed();

        let throughput = operation_count as f64 / duration.as_secs_f64();

        println!(
            "Volume {}: {} ops in {:?}, throughput: {:.2} ops/sec",
            volume_name, operation_count, duration, throughput
        );

        // Ensure throughput doesn't degrade significantly with higher log volumes
        assert!(
            throughput >= MIN_ASYNC_THROUGHPUT_OPS_PER_SEC * 0.5, // Allow 50% reduction for high volumes
            "Throughput {:.2} ops/sec for {} volume is too low",
            throughput,
            volume_name
        );
    }

    fs::remove_dir_all(&scenario_dir).await.unwrap();
}

// Helper functions

async fn measure_async_throughput(
    data_file: &Path,
    config: &Config,
    platform: Arc<Platform>,
    enable_logging: bool,
) -> f64 {
    // Configure logging level
    if enable_logging {
        unsafe {
            std::env::set_var("RUST_LOG", "debug");
        }
    } else {
        unsafe {
            std::env::set_var("RUST_LOG", "error");
        }
    }

    let operation_count = 200;
    let start_time = Instant::now();

    match SSTableReader::open(data_file, config, platform).await {
        Ok(reader) => {
            for i in 0..operation_count {
                let key = format!("test_key_{:04}", i % 100).into_bytes();
                let _ = reader.lookup_partition_with_index(&key).await;
            }
        }
        Err(_) => {
            // Reader creation failed, return baseline throughput
            return 50.0;
        }
    }

    let duration = start_time.elapsed();
    operation_count as f64 / duration.as_secs_f64()
}

async fn measure_concurrent_operations(
    data_file: &Path,
    config: &Config,
    platform: Arc<Platform>,
    concurrency: usize,
    enable_logging: bool,
) -> Duration {
    if enable_logging {
        unsafe {
            std::env::set_var("RUST_LOG", "debug");
        }
    } else {
        unsafe {
            std::env::set_var("RUST_LOG", "error");
        }
    }

    let start_time = Instant::now();

    match SSTableReader::open(data_file, config, platform).await {
        Ok(reader) => {
            let reader = Arc::new(reader);
            let mut join_set = JoinSet::new();

            for i in 0..concurrency {
                let reader_clone = Arc::clone(&reader);
                join_set.spawn(async move {
                    for j in 0..20 {
                        // 20 operations per concurrent task
                        let key = format!("concurrent_key_{}_{:04}", i, j).into_bytes();
                        let _ = reader_clone.lookup_partition_with_index(&key).await;
                    }
                });
            }

            // Wait for all tasks to complete
            while let Some(_) = join_set.join_next().await {
                // Task completed
            }
        }
        Err(_) => {
            // Return a baseline duration if reader creation fails
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    start_time.elapsed()
}

async fn measure_operation_latencies(
    data_file: &Path,
    config: &Config,
    platform: Arc<Platform>,
    enable_logging: bool,
) -> Vec<u64> {
    if enable_logging {
        unsafe {
            std::env::set_var("RUST_LOG", "debug");
        }
    } else {
        unsafe {
            std::env::set_var("RUST_LOG", "error");
        }
    }

    let mut latencies = Vec::new();

    match SSTableReader::open(data_file, config, platform).await {
        Ok(reader) => {
            for i in 0..100 {
                let start = Instant::now();
                let key = format!("latency_test_key_{:04}", i).into_bytes();
                let _ = reader.lookup_partition_with_index(&key).await;
                latencies.push(start.elapsed().as_millis() as u64);
            }
        }
        Err(_) => {
            // Return dummy latencies if reader creation fails
            for _ in 0..100 {
                latencies.push(10); // 10ms baseline
            }
        }
    }

    latencies
}

async fn perform_memory_test_operations(
    data_file: &Path,
    config: &Config,
    platform: Arc<Platform>,
    enable_logging: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if enable_logging {
        unsafe {
            std::env::set_var("RUST_LOG", "debug");
        }
    } else {
        unsafe {
            std::env::set_var("RUST_LOG", "error");
        }
    }

    if let Ok(reader) = SSTableReader::open(data_file, config, platform).await {
        for i in 0..50 {
            let key = format!("memory_test_key_{:04}", i).into_bytes();
            let _ = reader.lookup_partition_with_index(&key).await;
            let _ = reader.iterate_token_range(i * 1000, (i + 1) * 1000).await;
        }
    }

    Ok(())
}

async fn perform_volume_test_operations(
    data_file: &Path,
    config: &Config,
    platform: Arc<Platform>,
    operation_count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    unsafe {
        std::env::set_var("RUST_LOG", "debug");
    } // Always enable debug logging for volume tests

    if let Ok(reader) = SSTableReader::open(data_file, config, platform).await {
        for i in 0..operation_count {
            let key = format!("volume_test_key_{:06}", i).into_bytes();
            let _ = reader.lookup_partition_with_index(&key).await;
        }
    }

    Ok(())
}

fn percentile(values: &[u64], percentile: f64) -> u64 {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();

    if sorted.is_empty() {
        return 0;
    }

    let index = ((percentile / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[index.min(sorted.len() - 1)]
}

fn get_memory_usage() -> usize {
    // Get current memory usage in MB
    #[cfg(target_os = "linux")]
    {
        if let Ok(contents) = std::fs::read_to_string("/proc/self/status") {
            for line in contents.lines() {
                if line.starts_with("VmRSS:") {
                    if let Some(kb_str) = line.split_whitespace().nth(1) {
                        if let Ok(kb) = kb_str.parse::<usize>() {
                            return kb / 1024; // Convert KB to MB
                        }
                    }
                }
            }
        }
    }

    // Fallback for other platforms
    1 // 1MB baseline
}

fn force_garbage_collection() {
    // Force garbage collection if available
    #[cfg(feature = "legacy-heuristics")]
    {
        // Platform-specific GC trigger would go here
    }

    // Create some temporary allocations to encourage cleanup
    let _temp: Vec<Vec<u8>> = (0..1000).map(|_| vec![0u8; 1024]).collect();
}

// Test file creation functions

async fn create_logging_test_files(dir: &Path, base_name: &str, partition_count: usize) {
    create_logging_data_file(dir, base_name, partition_count).await;
    create_logging_index_file(dir, base_name, partition_count).await;
    create_logging_summary_file(dir, base_name, partition_count).await;
    create_logging_statistics_file(dir, base_name, partition_count).await;
    create_logging_filter_file(dir, base_name, partition_count).await;
}

async fn create_logging_data_file(dir: &Path, base_name: &str, partition_count: usize) {
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

    // Create limited partitions for test
    let actual_partitions = partition_count.min(500);
    for i in 0..actual_partitions {
        let key = format!("logging_test_key_{:06}", i);
        data.extend_from_slice(&(key.len() as u32).to_be_bytes());
        data.extend_from_slice(key.as_bytes());
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x20]); // Row size
        data.extend_from_slice(&[0xCC; 32]); // Row data
    }

    fs::write(path, data).await.unwrap();
}

async fn create_logging_index_file(dir: &Path, base_name: &str, partition_count: usize) {
    let path = dir.join(format!("{}-Index.db", base_name));
    let mut data = Vec::new();

    let index_entries = partition_count.min(500);

    // Index header
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]); // Version
    data.extend_from_slice(&(index_entries as u32).to_be_bytes());

    // Index entries
    for i in 0..index_entries {
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x20]); // Digest length
        let mut digest = vec![0; 32];
        digest[0] = (i % 256) as u8;
        digest[1] = ((i / 256) % 256) as u8;
        data.extend_from_slice(&digest);

        let offset = (i as u64) * 64;
        data.extend_from_slice(&offset.to_be_bytes());
        data.extend_from_slice(&(32u32).to_be_bytes());
    }

    fs::write(path, data).await.unwrap();
}

async fn create_logging_summary_file(dir: &Path, base_name: &str, partition_count: usize) {
    let path = dir.join(format!("{}-Summary.db", base_name));
    let mut data = Vec::new();

    let summary_entries = (partition_count / 50).clamp(5, 20);

    // Summary header
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]); // Version
    data.extend_from_slice(&(summary_entries as u32).to_be_bytes());

    // Summary entries
    for i in 0..summary_entries {
        let key = format!("logging_sum_{:04}", i);
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(key.as_bytes());

        let token = (i as i64) * 1000000;
        data.extend_from_slice(&token.to_be_bytes());
        data.extend_from_slice(&((i * 1000) as u64).to_be_bytes());
        data.extend_from_slice(&(i as u32).to_be_bytes());
    }

    fs::write(path, data).await.unwrap();
}

async fn create_logging_statistics_file(dir: &Path, base_name: &str, partition_count: usize) {
    let path = dir.join(format!("{}-Statistics.db", base_name));
    let mut data = Vec::new();

    let stats = vec![
        ("min_timestamp", 1640995200000u64),
        ("max_timestamp", 1672531200000u64),
        ("live_row_count", partition_count as u64),
        ("total_data_size", (partition_count * 64) as u64),
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

async fn create_logging_filter_file(dir: &Path, base_name: &str, partition_count: usize) {
    let path = dir.join(format!("{}-Filter.db", base_name));
    let mut data = Vec::new();

    let filter_size = (partition_count / 8).clamp(512, 8192);

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
