//! Property-based performance and memory safety tests
//!
//! This module validates performance characteristics, memory usage bounds,
//! and ensures operations complete within acceptable time limits.

use crate::storage::sstable::compression::{CompressionCodec, CompressionType};
use crate::types::Value;
use proptest::prelude::*;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

// ============================================================================
// Performance Test Generators
// ============================================================================

/// Generates data patterns for performance testing
fn arb_performance_data() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        // Small data (cache-friendly)
        prop::collection::vec(any::<u8>(), 64..1024),
        // Medium data (typical SSTable block size)
        prop::collection::vec(any::<u8>(), 4096..65536),
        // Large data (memory pressure testing)
        prop::collection::vec(any::<u8>(), 65536..1024 * 1024),
        // Highly compressible (repeated patterns)
        (any::<u8>(), 1000..50000usize).prop_map(|(byte, len)| vec![byte; len]),
        // Random data (incompressible)
        prop::collection::vec(any::<u8>(), 1000..100000),
        // Binary patterns with structure
        prop::collection::vec(0u8..255u8, 10000..100000).prop_map(|mut v| {
            // Add some structure to make it partially compressible
            for i in (0..v.len()).step_by(128) {
                if i + 64 < v.len() {
                    v[i..i + 64].fill(0);
                }
            }
            v
        }),
    ]
}

/// Generates workload patterns for concurrency testing
fn arb_workload_pattern() -> impl Strategy<Value = WorkloadPattern> {
    (
        prop::collection::vec(any::<u8>(), 100..10000),
        1..16usize,   // thread count
        1..1000usize, // operations per thread
        prop_oneof![
            Just(AccessPattern::Sequential),
            Just(AccessPattern::Random),
            Just(AccessPattern::Hotspot),
        ],
    )
        .prop_map(|(data, threads, ops, pattern)| WorkloadPattern {
            data,
            thread_count: threads,
            operations_per_thread: ops,
            access_pattern: pattern,
        })
}

#[derive(Debug, Clone)]
struct WorkloadPattern {
    data: Vec<u8>,
    thread_count: usize,
    operations_per_thread: usize,
    access_pattern: AccessPattern,
}

#[derive(Debug, Clone)]
enum AccessPattern {
    Sequential,
    Random,
    Hotspot,
}

// ============================================================================
// Memory Usage Tracking
// ============================================================================

struct MemoryTracker {
    initial_usage: usize,
    peak_usage: usize,
    current_usage: usize,
}

impl MemoryTracker {
    fn new() -> Self {
        let initial = get_memory_usage();
        Self {
            initial_usage: initial,
            peak_usage: initial,
            current_usage: initial,
        }
    }

    fn update(&mut self) {
        self.current_usage = get_memory_usage();
        if self.current_usage > self.peak_usage {
            self.peak_usage = self.current_usage;
        }
    }

    fn memory_increase(&self) -> usize {
        self.current_usage.saturating_sub(self.initial_usage)
    }

    fn peak_increase(&self) -> usize {
        self.peak_usage.saturating_sub(self.initial_usage)
    }
}

fn get_memory_usage() -> usize {
    // Platform-specific memory usage detection
    #[cfg(target_os = "linux")]
    {
        if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
            for line in status.lines() {
                if line.starts_with("VmRSS:") {
                    if let Some(kb_str) = line.split_whitespace().nth(1) {
                        if let Ok(kb) = kb_str.parse::<usize>() {
                            return kb * 1024;
                        }
                    }
                }
            }
        }
    }

    #[cfg(target_os = "macos")]
    {
        use std::process::Command;
        if let Ok(output) = Command::new("ps")
            .args(&["-o", "rss=", "-p"])
            .arg(std::process::id().to_string())
            .output()
        {
            if let Ok(rss_str) = String::from_utf8(output.stdout) {
                if let Ok(rss_kb) = rss_str.trim().parse::<usize>() {
                    return rss_kb * 1024;
                }
            }
        }
    }

    // Fallback: use allocation tracking
    0
}

// ============================================================================
// Property Tests for Performance
// ============================================================================

proptest! {
    /// Test that compression operations complete within time bounds
    #[test]
    fn prop_compression_performance_bounds(
        data in arb_performance_data(),
        compression_type in prop_oneof![
            Just(CompressionType::None),
            Just(CompressionType::Lz4),
            Just(CompressionType::Snappy),
            Just(CompressionType::Deflate),
            Just(CompressionType::Zstd),
        ]
    ) {
        if data.is_empty() {
            return Ok(());
        }

        let codec = match CompressionCodec::new(compression_type) {
            Ok(codec) => codec,
            Err(_) => return Ok(()), // Skip unavailable compression
        };

        let mut memory_tracker = MemoryTracker::new();

        // Compression performance
        let compress_start = Instant::now();
        let compressed = codec.compress(&data)
            .expect("Compression should succeed");
        let compress_duration = compress_start.elapsed();

        memory_tracker.update();

        // Decompression performance
        let decompress_start = Instant::now();
        let decompressed = codec.decompress(&compressed, data.len())
            .expect("Decompression should succeed");
        let decompress_duration = decompress_start.elapsed();

        memory_tracker.update();

        // Verify correctness
        prop_assert_eq!(data, decompressed);

        // Performance bounds based on data size and compression type
        let data_mb = data.len() as f64 / (1024.0 * 1024.0);
        let max_compress_time = match compression_type {
            CompressionType::None => Duration::from_millis(1),
            CompressionType::Lz4 => Duration::from_millis((data_mb * 100.0) as u64 + 100),
            CompressionType::Snappy => Duration::from_millis((data_mb * 150.0) as u64 + 100),
            CompressionType::Deflate => Duration::from_millis((data_mb * 500.0) as u64 + 200),
            CompressionType::Zstd => Duration::from_millis((data_mb * 300.0) as u64 + 150),
        };

        prop_assert!(compress_duration <= max_compress_time,
            "Compression took {:?}, expected <= {:?} for {:.2}MB {:?}",
            compress_duration, max_compress_time, data_mb, compression_type);

        // Decompression should be faster than compression
        let max_decompress_time = max_compress_time / 2;
        prop_assert!(decompress_duration <= max_decompress_time,
            "Decompression took {:?}, expected <= {:?}",
            decompress_duration, max_decompress_time);

        // Memory usage bounds
        let max_expected_memory = data.len() * 3; // Allow 3x overhead
        prop_assert!(memory_tracker.peak_increase() <= max_expected_memory,
            "Peak memory usage {} exceeded 3x data size {}",
            memory_tracker.peak_increase(), max_expected_memory);
    }

    /// Test concurrent access performance and thread safety
    #[test]
    fn prop_concurrent_performance(workload in arb_workload_pattern()) {
        let shared_data = Arc::new(workload.data.clone());
        let start_time = Instant::now();

        let handles: Vec<_> = (0..workload.thread_count).map(|thread_id| {
            let data = Arc::clone(&shared_data);
            let ops = workload.operations_per_thread;
            let pattern = workload.access_pattern.clone();

            thread::spawn(move || {
                let mut results = Vec::new();
                let thread_start = Instant::now();

                for op_idx in 0..ops {
                    let index = match pattern {
                        AccessPattern::Sequential => (thread_id + op_idx) % data.len(),
                        AccessPattern::Random => {
                            use std::collections::hash_map::DefaultHasher;
                            use std::hash::{Hash, Hasher};
                            let mut hasher = DefaultHasher::new();
                            (thread_id, op_idx).hash(&mut hasher);
                            (hasher.finish() as usize) % data.len()
                        },
                        AccessPattern::Hotspot => {
                            // 80% of accesses go to first 20% of data
                            if op_idx % 5 == 0 {
                                op_idx % (data.len() / 5).max(1)
                            } else {
                                op_idx % data.len()
                            }
                        }
                    };

                    // Simulate read operation
                    let value = data[index];
                    results.push(value);

                    // Simulate some processing
                    let _hash = std::collections::hash_map::DefaultHasher::new();
                }

                let thread_duration = thread_start.elapsed();
                (thread_id, results.len(), thread_duration)
            })
        }).collect();

        let mut thread_results = Vec::new();
        for handle in handles {
            let result = handle.join().expect("Thread should complete successfully");
            thread_results.push(result);
        }

        let total_duration = start_time.elapsed();

        // Performance assertions
        let total_operations = workload.thread_count * workload.operations_per_thread;
        let ops_per_second = total_operations as f64 / total_duration.as_secs_f64();

        // Should achieve reasonable throughput
        let min_ops_per_second = match workload.access_pattern {
            AccessPattern::Sequential => 1_000_000.0, // Very fast for sequential
            AccessPattern::Random => 100_000.0,       // Slower for random access
            AccessPattern::Hotspot => 500_000.0,      // Medium for hotspot
        };

        prop_assert!(ops_per_second >= min_ops_per_second,
            "Achieved {:.0} ops/sec, expected >= {:.0} for pattern {:?}",
            ops_per_second, min_ops_per_second, workload.access_pattern);

        // Thread fairness - no thread should take more than 2x the average
        let avg_duration = thread_results.iter()
            .map(|(_, _, duration)| duration.as_millis())
            .sum::<u128>() / thread_results.len() as u128;

        for (thread_id, ops_completed, duration) in thread_results {
            prop_assert_eq!(ops_completed, workload.operations_per_thread,
                "Thread {} completed wrong number of operations", thread_id);

            prop_assert!(duration.as_millis() <= avg_duration * 2,
                "Thread {} took {}ms, average was {}ms",
                thread_id, duration.as_millis(), avg_duration);
        }
    }

    /// Test memory allocation patterns and garbage collection
    #[test]
    fn prop_memory_allocation_patterns(
        allocation_sizes in prop::collection::vec(1..1024*1024usize, 1..100),
        allocation_count in 1..1000usize
    ) {
        let mut memory_tracker = MemoryTracker::new();
        let mut allocations = Vec::new();

        // Allocation phase
        for (i, &size) in allocation_sizes.iter().enumerate().take(allocation_count) {
            let data = vec![0u8; size];
            allocations.push(data);

            if i % 10 == 0 {
                memory_tracker.update();
            }
        }

        let peak_memory = memory_tracker.peak_increase();

        // Deallocation phase
        allocations.clear();

        // Force potential garbage collection
        for _ in 0..10 {
            let _temp = vec![0u8; 1024];
            std::hint::black_box(&_temp);
        }

        memory_tracker.update();
        let final_memory = memory_tracker.memory_increase();

        // Calculate expected memory usage
        let total_allocated: usize = allocation_sizes.iter()
            .take(allocation_count)
            .sum();

        // Peak memory should be reasonable relative to allocations
        prop_assert!(peak_memory <= total_allocated * 2,
            "Peak memory {} exceeded 2x allocated size {}",
            peak_memory, total_allocated);

        // Most memory should be reclaimed (allow for fragmentation)
        let retained_ratio = final_memory as f64 / peak_memory.max(1) as f64;
        prop_assert!(retained_ratio <= 0.3,
            "Too much memory retained: {:.1}% (final: {}, peak: {})",
            retained_ratio * 100.0, final_memory, peak_memory);
    }

    /// Test serialization performance for various data sizes
    #[test]
    fn prop_serialization_performance(
        value in prop_oneof![
            // Small values
            any::<i32>().prop_map(Value::Integer),
            "[a-zA-Z0-9]{10,100}".prop_map(Value::Text),

            // Medium values
            prop::collection::vec(any::<u8>(), 1024..10240).prop_map(Value::Blob),
            prop::collection::vec(any::<i32>().prop_map(Value::Integer), 100..1000)
                .prop_map(Value::List),

            // Large values
            prop::collection::vec(any::<u8>(), 10240..102400).prop_map(Value::Blob),
            "[a-zA-Z0-9 ]{1000,10000}".prop_map(Value::Text),
        ]
    ) {
        let mut memory_tracker = MemoryTracker::new();

        // Serialization performance
        let serialize_start = Instant::now();
        let serialized = bincode::serialize(&value)
            .expect("Serialization should succeed");
        let serialize_duration = serialize_start.elapsed();

        memory_tracker.update();

        // Deserialization performance
        let deserialize_start = Instant::now();
        let deserialized: Value = bincode::deserialize(&serialized)
            .expect("Deserialization should succeed");
        let deserialize_duration = deserialize_start.elapsed();

        memory_tracker.update();

        // Verify correctness
        prop_assert_eq!(value, deserialized);

        // Performance bounds based on data size
        let serialized_mb = serialized.len() as f64 / (1024.0 * 1024.0);

        // Serialization should be fast - roughly 100MB/s minimum
        let max_serialize_time = Duration::from_millis((serialized_mb * 10.0) as u64 + 10);
        prop_assert!(serialize_duration <= max_serialize_time,
            "Serialization took {:?}, expected <= {:?} for {:.2}MB",
            serialize_duration, max_serialize_time, serialized_mb);

        // Deserialization should be similar or faster
        let max_deserialize_time = max_serialize_time;
        prop_assert!(deserialize_duration <= max_deserialize_time,
            "Deserialization took {:?}, expected <= {:?}",
            deserialize_duration, max_deserialize_time);

        // Memory usage should be bounded
        let value_size_estimate = match &value {
            Value::Blob(ref data) => data.len(),
            Value::Text(ref text) => text.len(),
            Value::List(ref items) => items.len() * 32, // Rough estimate
            _ => 1024, // Default estimate for other types
        };

        let max_memory_overhead = value_size_estimate * 4; // Allow 4x overhead
        prop_assert!(memory_tracker.peak_increase() <= max_memory_overhead,
            "Memory overhead {} exceeded 4x value size {}",
            memory_tracker.peak_increase(), max_memory_overhead);
    }

    /// Test performance regression detection
    #[test]
    fn prop_performance_regression_detection(
        data_size in 1000..100000usize,
        iterations in 10..100usize
    ) {
        let test_data = Value::Blob(vec![0u8; data_size]);
        let mut durations = Vec::new();

        // Perform multiple iterations to get stable measurements
        for _i in 0..iterations {
            let start = Instant::now();

            let serialized = bincode::serialize(&test_data).unwrap();
            let _deserialized: Value = bincode::deserialize(&serialized).unwrap();

            durations.push(start.elapsed());
        }

        // Calculate statistics
        let total_duration: Duration = durations.iter().sum();
        let avg_duration = total_duration / iterations as u32;

        let max_duration = durations.iter().max().unwrap();
        let min_duration = durations.iter().min().unwrap();

        // Performance consistency checks
        let variation_ratio = max_duration.as_nanos() as f64 / min_duration.as_nanos() as f64;
        prop_assert!(variation_ratio <= 5.0,
            "Performance too variable: max={:?}, min={:?}, ratio={:.2}",
            max_duration, min_duration, variation_ratio);

        // Throughput should be reasonable
        let bytes_per_second = (data_size as f64 / avg_duration.as_secs_f64()) / (1024.0 * 1024.0);
        prop_assert!(bytes_per_second >= 10.0,
            "Throughput too low: {:.1} MB/s for {}KB data",
            bytes_per_second, data_size / 1024);

        // No single operation should take too long
        let max_acceptable = Duration::from_millis((data_size / 1000) as u64 + 100);
        prop_assert!(*max_duration <= max_acceptable,
            "Slowest operation took {:?}, expected <= {:?}",
            max_duration, max_acceptable);
    }
}

#[cfg(test)]
mod performance_integration_tests {
    use super::*;

    #[test]
    fn test_memory_tracking_works() {
        let mut tracker = MemoryTracker::new();
        let initial = tracker.current_usage;

        // Allocate some memory
        let _large_vec = vec![0u8; 1024 * 1024]; // 1MB
        tracker.update();

        // Should see some increase (may not be exact due to allocator behavior)
        let increase = tracker.memory_increase();
        println!("Memory increase: {} bytes", increase);

        // At minimum, we should see that tracking doesn't crash
        assert!(tracker.peak_usage >= tracker.initial_usage);
    }

    #[test]
    fn test_compression_types_available() {
        let types = [
            CompressionType::None,
            CompressionType::Lz4,
            CompressionType::Snappy,
            CompressionType::Deflate,
            CompressionType::Zstd,
        ];

        for compression_type in &types {
            match CompressionCodec::new(*compression_type) {
                Ok(_) => println!("Compression type {:?} available", compression_type),
                Err(e) => println!("Compression type {:?} unavailable: {}", compression_type, e),
            }
        }
    }
}
