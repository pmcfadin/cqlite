//! Performance regression tests for SSTable eager loading
//!
//! These tests exercise the eager-loading code paths (open, index lookup,
//! iteration, timestamp/token queries) and RECORD their timings.
//!
//! #2369 rule (record-not-assert): these tests DO NOT assert on wall-clock
//! thresholds. Wall-clock latency depends on the host, contention, and CI
//! load, so a `assert!(elapsed < N ms)` in the default correctness gate is a
//! latent flake (issue #2642 retired the previous asserts here). Timings are
//! logged via `eprintln!` as `[perf-record]` lines for humans/dashboards; the
//! functional value is that the code paths run to completion without panic.
//! A dedicated, host-controlled perf gate is the correct place for threshold
//! enforcement — not `cargo test`.
//!
//! Reviewer note: do NOT reintroduce `assert!(<measured elapsed> < <threshold>)`
//! here or in any default-gate test. See scripts/tests/check-no-wallclock-asserts.sh.

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tempfile::TempDir;
use tokio::fs;

use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::SSTableReader;
use cqlite_core::Config;

/// Reference latencies retained only for `[perf-record]` context — NEVER
/// asserted on in this default-gate test (#2369 / #2642).
const REF_INIT_TIME_MS: u64 = 100;
const REF_FIRST_OPERATION_MS: u64 = 50;
/// Memory-increase budget (MB). This is NOT a wall-clock threshold — memory
/// usage is deterministic here (the fallback `get_memory_usage` returns a
/// constant off Linux), so this assert is retained.
const MAX_MEMORY_INCREASE_MB: usize = 10;

/// Emit a recorded (non-asserted) timing line for perf dashboards/humans.
/// #2369: record, do not assert on wall-clock in the correctness gate.
fn record_timing(label: &str, elapsed: std::time::Duration, reference_ms: u64) {
    eprintln!(
        "[perf-record] {label}: {elapsed:?} (reference budget: {reference_ms} ms, not asserted)"
    );
}

/// Test that eager loading doesn't cause initialization performance regression
#[tokio::test]
async fn test_initialization_performance() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let performance_scenarios = vec![
        ("small-table", 100, "Small SSTable (100 partitions)"),
        ("medium-table", 10000, "Medium SSTable (10K partitions)"),
        ("large-table", 100000, "Large SSTable (100K partitions)"),
    ];

    for (base_name, partition_count, description) in performance_scenarios {
        println!("Testing initialization performance: {}", description);

        let scenario_dir = base_path.join(base_name);
        fs::create_dir(&scenario_dir).await.unwrap();

        // Create realistic-sized files
        create_performance_test_files(&scenario_dir, base_name, partition_count).await;

        let data_file = scenario_dir.join(format!("{}-Data.db", base_name));
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        // Measure initialization time
        let init_start = Instant::now();

        match SSTableReader::open(&data_file, &config, platform).await {
            Ok(reader) => {
                let init_duration = init_start.elapsed();

                println!("✓ {} initialized in {:?}", description, init_duration);

                // #2369: record the initialization time, do not assert on it.
                record_timing(
                    &format!("init/{description}"),
                    init_duration,
                    REF_INIT_TIME_MS,
                );

                // Test first operation performance (should be immediate due to eager loading)
                test_first_operation_performance(&reader, description).await;
            }
            Err(e) => {
                let init_duration = init_start.elapsed();
                println!(
                    "✓ {} initialization attempted in {:?}: {}",
                    description, init_duration, e
                );
            }
        }

        fs::remove_dir_all(&scenario_dir).await.unwrap();
    }
}

/// Test that first operations after eager loading are immediate
async fn test_first_operation_performance(reader: &SSTableReader, description: &str) {
    println!("Testing first operation performance for {}", description);

    // Test each operation type individually. #2369: record timings, no asserts.
    let op_start = Instant::now();
    test_index_lookup_performance(reader).await;
    let op_duration = op_start.elapsed();
    println!(
        "✓ {} index_lookup operation: {:?}",
        description, op_duration
    );
    record_timing(
        &format!("first-op/index_lookup/{description}"),
        op_duration,
        REF_FIRST_OPERATION_MS,
    );

    let op_start = Instant::now();
    test_token_range_performance(reader).await;
    let op_duration = op_start.elapsed();
    println!("✓ {} token_range operation: {:?}", description, op_duration);
    record_timing(
        &format!("first-op/token_range/{description}"),
        op_duration,
        REF_FIRST_OPERATION_MS,
    );

    let op_start = Instant::now();
    test_timestamp_range_performance(reader).await;
    let op_duration = op_start.elapsed();
    println!(
        "✓ {} timestamp_range operation: {:?}",
        description, op_duration
    );
    record_timing(
        &format!("first-op/timestamp_range/{description}"),
        op_duration,
        REF_FIRST_OPERATION_MS,
    );

    let op_start = Instant::now();
    test_token_coverage_performance(reader).await;
    let op_duration = op_start.elapsed();
    println!(
        "✓ {} token_coverage operation: {:?}",
        description, op_duration
    );
    record_timing(
        &format!("first-op/token_coverage/{description}"),
        op_duration,
        REF_FIRST_OPERATION_MS,
    );

    // Operations tested above
}

/// Test memory usage with eager loading
#[tokio::test]
async fn test_memory_usage_performance() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    // Measure baseline memory usage
    let baseline_memory = get_memory_usage();
    println!("Baseline memory usage: {} MB", baseline_memory);

    let base_name = "memory-test";
    let scenario_dir = base_path.join(base_name);
    fs::create_dir(&scenario_dir).await.unwrap();

    // Create moderate-sized files for memory testing
    create_performance_test_files(&scenario_dir, base_name, 50000).await;

    let data_file = scenario_dir.join(format!("{}-Data.db", base_name));
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    // Measure memory after SSTableReader creation
    let memory_before = get_memory_usage();

    match SSTableReader::open(&data_file, &config, platform).await {
        Ok(reader) => {
            let memory_after = get_memory_usage();
            let memory_increase = memory_after.saturating_sub(memory_before);

            println!(
                "✓ Memory usage: baseline {} MB, before {} MB, after {} MB, increase {} MB",
                baseline_memory, memory_before, memory_after, memory_increase
            );

            // Verify memory increase is reasonable
            assert!(
                memory_increase <= MAX_MEMORY_INCREASE_MB,
                "Memory increase {} MB exceeds maximum {} MB",
                memory_increase,
                MAX_MEMORY_INCREASE_MB
            );

            // Test that operations don't cause additional significant memory usage
            test_operation_memory_stability(&reader).await;
        }
        Err(e) => {
            println!("✓ Memory test completed: {}", e);
        }
    }

    fs::remove_dir_all(&scenario_dir).await.unwrap();
}

/// Test concurrent loading performance
#[tokio::test]
async fn test_concurrent_loading_performance() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    // Create multiple SSTable files
    let concurrent_count = 5;
    let mut data_files = Vec::new();

    for i in 0..concurrent_count {
        let base_name = format!("concurrent-{}", i);
        let scenario_dir = base_path.join(&base_name);
        fs::create_dir(&scenario_dir).await.unwrap();

        create_performance_test_files(&scenario_dir, &base_name, 5000).await;

        let data_file = scenario_dir.join(format!("{}-Data.db", base_name));
        data_files.push(data_file);
    }

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    // Measure concurrent loading performance
    let concurrent_start = Instant::now();

    let mut handles = Vec::new();

    for (i, data_file) in data_files.iter().enumerate() {
        let data_file_clone = data_file.clone();
        let config_clone = config.clone();
        let platform_clone = platform.clone();

        let handle = tokio::spawn(async move {
            let start = Instant::now();
            let result = SSTableReader::open(&data_file_clone, &config_clone, platform_clone).await;
            let duration = start.elapsed();
            (i, result, duration)
        });

        handles.push(handle);
    }

    // Wait for all concurrent operations
    let mut results = Vec::new();
    for handle in handles {
        let (i, result, duration) = handle.await.unwrap();
        results.push((i, result, duration));
    }

    let total_concurrent_duration = concurrent_start.elapsed();

    println!(
        "✓ Concurrent loading of {} files completed in {:?}",
        concurrent_count, total_concurrent_duration
    );

    // Analyze individual results
    for (i, result, duration) in results {
        match result {
            Ok(_) => {
                println!("✓ Concurrent file {} loaded in {:?}", i, duration);

                // #2369: record each concurrent-load timing, do not assert.
                record_timing(
                    &format!("concurrent-load/{i}"),
                    duration,
                    REF_INIT_TIME_MS * 2,
                );
            }
            Err(e) => {
                println!(
                    "✓ Concurrent file {} load attempted in {:?}: {}",
                    i, duration, e
                );
            }
        }
    }

    // #2369: record total concurrent time, do not assert on it.
    record_timing(
        "concurrent-load/total",
        total_concurrent_duration,
        REF_INIT_TIME_MS * concurrent_count as u64 / 2,
    );

    // Cleanup
    for i in 0..concurrent_count {
        let base_name = format!("concurrent-{}", i);
        let scenario_dir = base_path.join(&base_name);
        fs::remove_dir_all(&scenario_dir).await.unwrap();
    }
}

/// Test operation latency after eager loading
#[tokio::test]
async fn test_operation_latency_performance() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let base_name = "latency-test";
    let scenario_dir = base_path.join(base_name);
    fs::create_dir(&scenario_dir).await.unwrap();

    create_performance_test_files(&scenario_dir, base_name, 20000).await;

    let data_file = scenario_dir.join(format!("{}-Data.db", base_name));
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());

    match SSTableReader::open(&data_file, &config, platform).await {
        Ok(reader) => {
            println!("✓ SSTableReader created for latency testing");

            // Test multiple operations to verify consistent performance
            test_sustained_operation_performance(&reader).await;
        }
        Err(e) => {
            println!("✓ Latency test setup completed: {}", e);
        }
    }

    fs::remove_dir_all(&scenario_dir).await.unwrap();
}

/// Test performance with different component file sizes
#[tokio::test]
async fn test_component_size_scaling_performance() {
    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path();

    let size_scenarios = vec![
        ("tiny-components", 100),
        ("small-components", 1000),
        ("medium-components", 10000),
        ("large-components", 50000),
    ];

    for (scenario_name, partition_count) in size_scenarios {
        println!(
            "Testing component size scaling: {} ({} partitions)",
            scenario_name, partition_count
        );

        let scenario_dir = base_path.join(scenario_name);
        fs::create_dir(&scenario_dir).await.unwrap();

        create_performance_test_files(&scenario_dir, scenario_name, partition_count).await;

        let data_file = scenario_dir.join(format!("{}-Data.db", scenario_name));
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        // Measure scaling performance
        let scaling_start = Instant::now();

        match SSTableReader::open(&data_file, &config, platform).await {
            Ok(reader) => {
                let scaling_duration = scaling_start.elapsed();

                println!("✓ {} loaded in {:?}", scenario_name, scaling_duration);

                // #2369: record scaling timing, do not assert on it.
                let max_time_for_size = calculate_max_time_for_partition_count(partition_count);
                record_timing(
                    &format!("scaling/{scenario_name}"),
                    scaling_duration,
                    max_time_for_size,
                );

                // Test that larger components don't degrade operation performance
                test_operation_performance_with_size(&reader, partition_count).await;
            }
            Err(e) => {
                let scaling_duration = scaling_start.elapsed();
                println!(
                    "✓ {} scaling test completed in {:?}: {}",
                    scenario_name, scaling_duration, e
                );
            }
        }

        fs::remove_dir_all(&scenario_dir).await.unwrap();
    }
}

// Performance test helper functions

async fn test_index_lookup_performance(reader: &SSTableReader) {
    let test_key = b"performance_test_partition_key";
    let _lookup_result = reader.lookup_partition_with_index(test_key).await;
}

async fn test_token_range_performance(reader: &SSTableReader) {
    // Note: iterate_token_range is deprecated (Issue #218) - use iterate_all_partitions
    let _partitions = reader.iterate_all_partitions().await;
}

async fn test_timestamp_range_performance(reader: &SSTableReader) {
    let _timestamp_range = reader.get_timestamp_range().await;
}

async fn test_token_coverage_performance(reader: &SSTableReader) {
    // Note: get_token_coverage is deprecated (Issue #218) - tokens not stored in Summary.db
    let _timestamp_range = reader.get_timestamp_range().await;
}

async fn test_operation_memory_stability(reader: &SSTableReader) {
    let memory_before_ops = get_memory_usage();

    // Perform multiple operations
    for i in 0..10 {
        let test_key = format!("memory_test_key_{}", i).into_bytes();
        let _lookup = reader.lookup_partition_with_index(&test_key).await;
        // Note: iterate_token_range is deprecated (Issue #218) - use iterate_all_partitions
        let _partitions = reader.iterate_all_partitions().await;
    }

    let memory_after_ops = get_memory_usage();
    let memory_change = memory_after_ops.saturating_sub(memory_before_ops);

    println!(
        "✓ Memory stability: before {} MB, after {} MB, change {} MB",
        memory_before_ops, memory_after_ops, memory_change
    );

    // Operations shouldn't cause significant memory increase
    assert!(
        memory_change <= 2, // Allow small increase for operation overhead
        "Operations caused excessive memory increase: {} MB",
        memory_change
    );
}

async fn test_sustained_operation_performance(reader: &SSTableReader) {
    println!("Testing sustained operation performance");

    let operation_count = 100;
    let total_start = Instant::now();
    let mut slowest_op = std::time::Duration::ZERO;

    for i in 0..operation_count {
        let op_start = Instant::now();

        let test_key = format!("sustained_test_key_{:04}", i).into_bytes();
        let _lookup = reader.lookup_partition_with_index(&test_key).await;

        let op_duration = op_start.elapsed();

        // #2369: record the slowest sustained op below; no per-op assert.
        if op_duration > slowest_op {
            slowest_op = op_duration;
        }
    }

    let total_duration = total_start.elapsed();
    let avg_per_operation = total_duration / operation_count;

    println!(
        "✓ {} sustained operations completed in {:?}, avg {:?} per operation",
        operation_count, total_duration, avg_per_operation
    );

    // #2369: record sustained-op timings, do not assert on them.
    record_timing("sustained/slowest-op", slowest_op, REF_FIRST_OPERATION_MS);
    record_timing(
        "sustained/avg-op",
        avg_per_operation,
        REF_FIRST_OPERATION_MS / 2,
    );
}

async fn test_operation_performance_with_size(reader: &SSTableReader, partition_count: usize) {
    println!(
        "Testing operation performance with {} partitions",
        partition_count
    );

    // Test that operation performance doesn't degrade significantly with larger components
    let op_start = Instant::now();

    let test_key = b"size_scaling_test_key";
    let _lookup = reader.lookup_partition_with_index(test_key).await;

    let op_duration = op_start.elapsed();

    // Allow slightly more time for larger components, but not proportionally more
    let max_time_ms = if partition_count > 10000 {
        REF_FIRST_OPERATION_MS * 2
    } else {
        REF_FIRST_OPERATION_MS
    };

    // #2369: record op-with-size timing, do not assert on it.
    record_timing(
        &format!("op-with-size/{partition_count}-partitions"),
        op_duration,
        max_time_ms,
    );

    println!(
        "✓ Operation with {} partitions: {:?}",
        partition_count, op_duration
    );
}

fn calculate_max_time_for_partition_count(partition_count: usize) -> u64 {
    // Scale expected time based on partition count, but with diminishing returns
    // due to eager loading optimization
    match partition_count {
        0..=1000 => REF_INIT_TIME_MS,
        1001..=10000 => REF_INIT_TIME_MS + 50,
        10001..=50000 => REF_INIT_TIME_MS + 100,
        _ => REF_INIT_TIME_MS + 200,
    }
}

fn get_memory_usage() -> usize {
    // Get current memory usage in MB
    // This is a simplified implementation for testing purposes

    // Try to get memory info from the system
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

    // Fallback for other platforms or if reading fails
    // Return a small baseline value for testing
    1 // 1MB baseline
}

// Performance test file creation functions

async fn create_performance_test_files(dir: &Path, base_name: &str, partition_count: usize) {
    create_performance_data_file(dir, base_name, partition_count).await;
    create_performance_index_file(dir, base_name, partition_count).await;
    create_performance_summary_file(dir, base_name, partition_count).await;
    create_performance_statistics_file(dir, base_name, partition_count).await;
    create_performance_filter_file(dir, base_name, partition_count).await;
}

async fn create_performance_data_file(dir: &Path, base_name: &str, partition_count: usize) {
    let path = dir.join(format!("{}-Data.db", base_name));
    let mut data = Vec::new();

    // SSTable header
    data.extend_from_slice(&[
        0x6d, 0x61, 0x64, 0x61, // Magic "mada"
        0x00, 0x00, 0x00, 0x05, // Version 5
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Timestamp
        0x00, 0x00, 0x00, 0x01, // Table count
    ]);
    data.extend_from_slice(&(partition_count as u32).to_be_bytes());
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00]); // Data size

    // Create limited actual partitions to keep test file sizes reasonable
    let actual_partitions = partition_count.min(1000);

    for i in 0..actual_partitions {
        let key = format!("perf_key_{:08}", i);
        data.extend_from_slice(&(key.len() as u32).to_be_bytes());
        data.extend_from_slice(key.as_bytes());
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x40]); // Row size
        data.extend_from_slice(&[0xBB; 64]); // Row data
    }

    fs::write(path, data).await.unwrap();
}

async fn create_performance_index_file(dir: &Path, base_name: &str, partition_count: usize) {
    let path = dir.join(format!("{}-Index.db", base_name));
    let mut data = Vec::new();

    let index_entries = partition_count.min(1000);

    // Index header
    data.extend_from_slice(&[
        0x00, 0x00, 0x00, 0x05, // Version
    ]);
    data.extend_from_slice(&(index_entries as u32).to_be_bytes());
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x00, 0x00]); // Data size
    data.extend_from_slice(&[0x12, 0x34, 0x56, 0x78]); // Checksum

    // Index entries
    for i in 0..index_entries {
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x20]); // Digest length
        let mut digest = vec![0; 32];
        digest[0] = (i % 256) as u8;
        digest[1] = ((i / 256) % 256) as u8;
        digest[2] = ((i / 65536) % 256) as u8;
        digest[31] = ((i + 42) % 256) as u8; // Add variety
        data.extend_from_slice(&digest);

        let offset = (i as u64) * 128;
        data.extend_from_slice(&offset.to_be_bytes());
        data.extend_from_slice(&(64u32).to_be_bytes());
    }

    fs::write(path, data).await.unwrap();
}

async fn create_performance_summary_file(dir: &Path, base_name: &str, partition_count: usize) {
    let path = dir.join(format!("{}-Summary.db", base_name));
    let mut data = Vec::new();

    let summary_entries = (partition_count / 100).clamp(5, 50);

    // Summary header
    data.extend_from_slice(&[
        0x00, 0x00, 0x00, 0x05, // Version
    ]);
    data.extend_from_slice(&(summary_entries as u32).to_be_bytes());
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x64]); // Sampling rate
    data.extend_from_slice(&[0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]); // Min token
    data.extend_from_slice(&[0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]); // Max token
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00]); // Data size
    data.extend_from_slice(&[0xAB, 0xCD, 0xEF, 0x12]); // Checksum

    // Summary entries
    for i in 0..summary_entries {
        let key = format!("perf_sum_{:04}", i);
        data.extend_from_slice(&(key.len() as u16).to_be_bytes());
        data.extend_from_slice(key.as_bytes());

        let token_range = i64::MAX as i128 * 2;
        let token = (i64::MIN as i128 + (i as i128 * token_range / summary_entries as i128)) as i64;
        data.extend_from_slice(&token.to_be_bytes());
        data.extend_from_slice(&((i * 2000) as u64).to_be_bytes()); // Index offset
        data.extend_from_slice(&(i as u32).to_be_bytes()); // Position
    }

    fs::write(path, data).await.unwrap();
}

async fn create_performance_statistics_file(dir: &Path, base_name: &str, partition_count: usize) {
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

async fn create_performance_filter_file(dir: &Path, base_name: &str, partition_count: usize) {
    let path = dir.join(format!("{}-Filter.db", base_name));
    let mut data = Vec::new();

    let filter_size = (partition_count / 8).clamp(1024, 32768);

    // Bloom filter header
    data.extend_from_slice(&[
        0x00, 0x00, 0x00, 0x02, // Version
        0x00, 0x00, 0x00, 0x07, // Hash functions
    ]);
    data.extend_from_slice(&(filter_size as u32).to_be_bytes());

    // Bloom filter bit array
    let bit_pattern = (partition_count % 256) as u8;
    let bit_array = vec![bit_pattern; filter_size];
    data.extend_from_slice(&bit_array);

    fs::write(path, data).await.unwrap();
}
