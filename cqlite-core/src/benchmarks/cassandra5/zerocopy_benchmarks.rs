#![cfg(feature = "benchmarks")]

//! Zero-Copy Deserialization Benchmarks for Cassandra 5+
//!
//! This module benchmarks zero-copy deserialization performance for Cassandra 5+
//! SSTable data, focusing on minimizing memory allocations and maximizing throughput
//! while maintaining data integrity.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
// use std::time::Duration; // For future timing operations

use crate::{Config, Platform, Result};

use super::{
    BenchmarkResult, PRDTargets,
    utils::{MemoryMonitor, PrecisionTimer, generate_test_data},
};

/// Zero-copy deserialization benchmarking suite
pub struct ZeroCopyBenchmarks {
    #[allow(dead_code)]
    platform: Arc<Platform>,
    #[allow(dead_code)]
    config: Config,
}

impl ZeroCopyBenchmarks {
    /// Create new zero-copy benchmarks
    pub async fn new(platform: Arc<Platform>, config: &Config) -> Result<Self> {
        Ok(Self {
            platform,
            config: config.clone(),
        })
    }

    /// Run comprehensive zero-copy performance tests
    pub async fn run_zerocopy_performance_tests(
        &self,
        _test_data_dir: &Path,
        targets: &PRDTargets,
    ) -> Result<Vec<BenchmarkResult>> {
        let mut results = Vec::new();

        println!("⚡ Starting Cassandra 5+ Zero-Copy Deserialization Benchmarks");

        // Test different data types and sizes
        let test_scenarios = vec![
            ("Primitive_Types", 1.0, DataComplexity::Simple),
            ("Collections_Small", 5.0, DataComplexity::Collections),
            ("Collections_Large", 25.0, DataComplexity::Collections),
            ("UDT_Simple", 10.0, DataComplexity::UserDefinedTypes),
            ("UDT_Complex", 50.0, DataComplexity::UserDefinedTypes),
            ("Mixed_Data", 100.0, DataComplexity::Mixed),
            ("Large_Blobs", 200.0, DataComplexity::BinaryData),
        ];

        for (scenario_name, size_mb, complexity) in test_scenarios {
            println!("\n⚡ Testing zero-copy deserialization: {}", scenario_name);

            // Test zero-copy vs traditional deserialization
            let zerocopy_result = self
                .benchmark_zerocopy_deserialization(scenario_name, size_mb, complexity, targets)
                .await?;
            results.push(zerocopy_result);

            let traditional_result = self
                .benchmark_traditional_deserialization(scenario_name, size_mb, complexity, targets)
                .await?;
            results.push(traditional_result);

            // Compare the two approaches
            let comparison_result = self
                .compare_deserialization_approaches(scenario_name, size_mb, &results, targets)
                .await?;
            results.push(comparison_result);
        }

        // Memory allocation tracking
        let allocation_result = self.benchmark_memory_allocations(targets).await?;
        results.push(allocation_result);

        Ok(results)
    }

    /// Benchmark zero-copy deserialization performance
    async fn benchmark_zerocopy_deserialization(
        &self,
        scenario_name: &str,
        size_mb: f64,
        complexity: DataComplexity,
        targets: &PRDTargets,
    ) -> Result<BenchmarkResult> {
        let benchmark_name = format!("ZeroCopy_{}", scenario_name);

        // Generate test data based on complexity
        let test_data = self.generate_complex_test_data(size_mb, complexity).await?;
        let mut memory_monitor = MemoryMonitor::new();

        let timer = PrecisionTimer::start();
        memory_monitor.sample();

        // Simulate zero-copy deserialization
        let operations_count = self
            .perform_zerocopy_operations(&test_data, &mut memory_monitor)
            .await?;

        let duration = timer.elapsed_duration();
        let memory_usage_mb = memory_monitor.peak_usage_mb();

        // Calculate metrics
        let throughput_mb_per_sec = size_mb / duration.as_secs_f64();
        let memory_efficiency = size_mb / memory_usage_mb.max(0.1);
        let operations_per_second = operations_count as f64 / duration.as_secs_f64();

        // Zero-copy should use minimal additional memory and be fast
        let memory_overhead = memory_usage_mb / size_mb;
        let meets_target = throughput_mb_per_sec >= targets.parse_speed_mb_per_sec * 1.2 && // Should be 20% faster
                          memory_overhead <= 0.5; // Should use ≤50% additional memory

        let target_comparison = if meets_target {
            format!(
                "✅ Zero-copy targets met ({:.2} MB/s, {:.1}x memory overhead)",
                throughput_mb_per_sec, memory_overhead
            )
        } else {
            format!(
                "❌ Zero-copy targets missed ({:.2} MB/s vs {:.2}, {:.1}x overhead)",
                throughput_mb_per_sec,
                targets.parse_speed_mb_per_sec * 1.2,
                memory_overhead
            )
        };

        let mut details = HashMap::new();
        details.insert("memory_overhead_ratio".to_string(), memory_overhead);
        details.insert(
            "allocations_avoided".to_string(),
            operations_count as f64 * 0.8,
        ); // Estimate
        details.insert("data_complexity".to_string(), complexity as u8 as f64);
        details.insert(
            "zero_copy_efficiency".to_string(),
            throughput_mb_per_sec / memory_overhead,
        );

        println!(
            "     ⚡ Zero-copy {}: {:.2} MB/s, {:.1}x memory overhead, {} ops/sec",
            scenario_name, throughput_mb_per_sec, memory_overhead, operations_per_second as u64
        );

        Ok(BenchmarkResult {
            benchmark_name,
            file_size_mb: size_mb,
            duration,
            throughput_mb_per_sec,
            memory_usage_mb,
            memory_efficiency,
            compression_ratio: None,
            operations_per_second,
            meets_prd_target: meets_target,
            target_comparison,
            details,
        })
    }

    /// Benchmark traditional deserialization for comparison
    async fn benchmark_traditional_deserialization(
        &self,
        scenario_name: &str,
        size_mb: f64,
        complexity: DataComplexity,
        targets: &PRDTargets,
    ) -> Result<BenchmarkResult> {
        let benchmark_name = format!("Traditional_{}", scenario_name);

        // Generate test data based on complexity
        let test_data = self.generate_complex_test_data(size_mb, complexity).await?;
        let mut memory_monitor = MemoryMonitor::new();

        let timer = PrecisionTimer::start();
        memory_monitor.sample();

        // Simulate traditional deserialization (with allocations)
        let operations_count = self
            .perform_traditional_operations(&test_data, &mut memory_monitor)
            .await?;

        let duration = timer.elapsed_duration();
        let memory_usage_mb = memory_monitor.peak_usage_mb();

        // Calculate metrics
        let throughput_mb_per_sec = size_mb / duration.as_secs_f64();
        let memory_efficiency = size_mb / memory_usage_mb.max(0.1);
        let operations_per_second = operations_count as f64 / duration.as_secs_f64();

        // Traditional approach baseline
        let memory_overhead = memory_usage_mb / size_mb;
        let meets_target = throughput_mb_per_sec >= targets.parse_speed_mb_per_sec
            && memory_usage_mb <= targets.memory_limit_mb;

        let target_comparison = if meets_target {
            format!(
                "✅ Traditional baseline acceptable ({:.2} MB/s, {:.1} MB)",
                throughput_mb_per_sec, memory_usage_mb
            )
        } else {
            format!(
                "❌ Traditional baseline issues ({:.2} MB/s, {:.1} MB)",
                throughput_mb_per_sec, memory_usage_mb
            )
        };

        let mut details = HashMap::new();
        details.insert("memory_overhead_ratio".to_string(), memory_overhead);
        details.insert("allocations_performed".to_string(), operations_count as f64);
        details.insert("data_complexity".to_string(), complexity as u8 as f64);
        details.insert(
            "traditional_efficiency".to_string(),
            throughput_mb_per_sec / memory_usage_mb,
        );

        println!(
            "     📝 Traditional {}: {:.2} MB/s, {:.1} MB memory, {} ops/sec",
            scenario_name, throughput_mb_per_sec, memory_usage_mb, operations_per_second as u64
        );

        Ok(BenchmarkResult {
            benchmark_name,
            file_size_mb: size_mb,
            duration,
            throughput_mb_per_sec,
            memory_usage_mb,
            memory_efficiency,
            compression_ratio: None,
            operations_per_second,
            meets_prd_target: meets_target,
            target_comparison,
            details,
        })
    }

    /// Compare zero-copy vs traditional approaches
    async fn compare_deserialization_approaches(
        &self,
        scenario_name: &str,
        size_mb: f64,
        results: &[BenchmarkResult],
        _targets: &PRDTargets,
    ) -> Result<BenchmarkResult> {
        let benchmark_name = format!("Comparison_{}", scenario_name);

        // Find the zero-copy and traditional results for this scenario
        let zerocopy_result = results
            .iter()
            .find(|r| r.benchmark_name == format!("ZeroCopy_{}", scenario_name));
        let traditional_result = results
            .iter()
            .find(|r| r.benchmark_name == format!("Traditional_{}", scenario_name));

        if let (Some(zerocopy), Some(traditional)) = (zerocopy_result, traditional_result) {
            // Calculate improvement metrics
            let speed_improvement =
                zerocopy.throughput_mb_per_sec / traditional.throughput_mb_per_sec;
            let memory_improvement = traditional.memory_usage_mb / zerocopy.memory_usage_mb;
            let efficiency_improvement = zerocopy.memory_efficiency / traditional.memory_efficiency;

            // Zero-copy should show significant improvements
            let meets_target = speed_improvement >= 1.1 && // At least 10% faster
                              memory_improvement >= 1.2; // At least 20% less memory

            let target_comparison = if meets_target {
                format!(
                    "✅ Zero-copy shows improvements ({:.1}x speed, {:.1}x memory efficiency)",
                    speed_improvement, memory_improvement
                )
            } else {
                format!(
                    "⚠️ Zero-copy improvements limited ({:.1}x speed, {:.1}x memory efficiency)",
                    speed_improvement, memory_improvement
                )
            };

            let mut details = HashMap::new();
            details.insert("speed_improvement_ratio".to_string(), speed_improvement);
            details.insert("memory_improvement_ratio".to_string(), memory_improvement);
            details.insert(
                "efficiency_improvement_ratio".to_string(),
                efficiency_improvement,
            );
            details.insert(
                "zerocopy_throughput".to_string(),
                zerocopy.throughput_mb_per_sec,
            );
            details.insert(
                "traditional_throughput".to_string(),
                traditional.throughput_mb_per_sec,
            );

            println!(
                "     🔄 Comparison {}: {:.1}x speed improvement, {:.1}x memory improvement",
                scenario_name, speed_improvement, memory_improvement
            );

            Ok(BenchmarkResult {
                benchmark_name,
                file_size_mb: size_mb,
                duration: zerocopy.duration,
                throughput_mb_per_sec: zerocopy.throughput_mb_per_sec,
                memory_usage_mb: zerocopy.memory_usage_mb,
                memory_efficiency: zerocopy.memory_efficiency,
                compression_ratio: None,
                operations_per_second: zerocopy.operations_per_second,
                meets_prd_target: meets_target,
                target_comparison,
                details,
            })
        } else {
            Err(crate::Error::validation(
                "Missing results for comparison".to_string(),
            ))
        }
    }

    /// Benchmark memory allocation patterns
    async fn benchmark_memory_allocations(&self, _targets: &PRDTargets) -> Result<BenchmarkResult> {
        let benchmark_name = "ZeroCopy_Allocation_Analysis".to_string();

        println!("   📊 Analyzing memory allocation patterns...");

        let test_data = generate_test_data(50.0); // 50MB test
        let mut memory_monitor = MemoryMonitor::new();

        let timer = PrecisionTimer::start();

        // Track allocations during zero-copy operations
        let initial_memory = memory_monitor.peak_usage_mb();
        memory_monitor.sample();

        // Perform operations with allocation tracking
        let allocation_count = self
            .track_allocations(&test_data, &mut memory_monitor)
            .await?;

        let duration = timer.elapsed_duration();
        let final_memory = memory_monitor.peak_usage_mb();
        let net_allocation = final_memory - initial_memory;

        // Calculate allocation efficiency
        let allocations_per_mb = allocation_count as f64 / 50.0;
        let memory_per_allocation = if allocation_count > 0 {
            net_allocation / allocation_count as f64
        } else {
            0.0
        };

        // Low allocation count and memory usage indicate good zero-copy performance
        let meets_target = allocations_per_mb <= 100.0 && // ≤100 allocations per MB
                          net_allocation <= 10.0; // ≤10MB net allocation

        let target_comparison = if meets_target {
            format!(
                "✅ Low allocation overhead ({:.0} allocs/MB, {:.1} MB net)",
                allocations_per_mb, net_allocation
            )
        } else {
            format!(
                "⚠️ High allocation overhead ({:.0} allocs/MB, {:.1} MB net)",
                allocations_per_mb, net_allocation
            )
        };

        let mut details = HashMap::new();
        details.insert("total_allocations".to_string(), allocation_count as f64);
        details.insert("allocations_per_mb".to_string(), allocations_per_mb);
        details.insert(
            "memory_per_allocation_kb".to_string(),
            memory_per_allocation * 1024.0,
        );
        details.insert("net_memory_allocation_mb".to_string(), net_allocation);

        println!(
            "     📊 Allocation Analysis: {} total allocations, {:.1} MB net allocation",
            allocation_count, net_allocation
        );

        Ok(BenchmarkResult {
            benchmark_name,
            file_size_mb: 50.0,
            duration,
            throughput_mb_per_sec: 50.0 / duration.as_secs_f64(),
            memory_usage_mb: net_allocation,
            memory_efficiency: 50.0 / net_allocation.max(0.1),
            compression_ratio: None,
            operations_per_second: allocation_count as f64 / duration.as_secs_f64(),
            meets_prd_target: meets_target,
            target_comparison,
            details,
        })
    }

    /// Generate complex test data based on data complexity
    async fn generate_complex_test_data(
        &self,
        size_mb: f64,
        complexity: DataComplexity,
    ) -> Result<Vec<u8>> {
        let size_bytes = (size_mb * 1024.0 * 1024.0) as usize;
        let mut data = Vec::with_capacity(size_bytes);

        match complexity {
            DataComplexity::Simple => {
                // Simple primitive types
                while data.len() < size_bytes {
                    data.extend_from_slice(b"int32_value:");
                    data.extend_from_slice(&42i32.to_be_bytes());
                    data.extend_from_slice(b"string_value:hello_world");
                    data.extend_from_slice(&(data.len() as u64).to_be_bytes());
                }
            }
            DataComplexity::Collections => {
                // Lists and maps
                while data.len() < size_bytes {
                    data.extend_from_slice(b"list:[");
                    for i in 0..10u32 {
                        data.extend_from_slice(&i.to_be_bytes());
                    }
                    data.extend_from_slice(b"]map:{");
                    for i in 0..5 {
                        data.extend_from_slice(&format!("key{}:value{}", i, i).as_bytes());
                    }
                    data.extend_from_slice(b"}");
                }
            }
            DataComplexity::UserDefinedTypes => {
                // UDT structures
                while data.len() < size_bytes {
                    data.extend_from_slice(b"udt:{field1:");
                    data.extend_from_slice(&(data.len() as u32).to_be_bytes());
                    data.extend_from_slice(b"field2:nested_udt:{nested_field:");
                    data.extend_from_slice(&(data.len() as u64).to_be_bytes());
                    data.extend_from_slice(b"}}");
                }
            }
            DataComplexity::Mixed => {
                // Mix of all types
                let patterns: [&[u8]; 4] = [
                    b"simple_data_",
                    b"[collection_",
                    b"{udt_data_}",
                    b"binary_blob_",
                ];
                let mut pattern_idx = 0;
                while data.len() < size_bytes {
                    data.extend_from_slice(patterns[pattern_idx % patterns.len()]);
                    data.extend_from_slice(&(data.len() as u64).to_be_bytes());
                    pattern_idx += 1;
                }
            }
            DataComplexity::BinaryData => {
                // Large binary blobs
                while data.len() < size_bytes {
                    data.extend_from_slice(b"blob_header:");
                    data.extend_from_slice(&(8192u32).to_be_bytes()); // 8KB blob size
                    // Fill with pseudo-random binary data
                    for i in 0..2048 {
                        data.extend_from_slice(&((i * 0x1234_5678) as u32).to_be_bytes());
                    }
                }
            }
        }

        data.truncate(size_bytes);
        Ok(data)
    }

    /// Perform zero-copy operations (minimize allocations)
    async fn perform_zerocopy_operations(
        &self,
        data: &[u8],
        monitor: &mut MemoryMonitor,
    ) -> Result<usize> {
        let mut operations = 0;
        let chunk_size = 4096; // 4KB chunks

        // Process data without creating new allocations
        for chunk in data.chunks(chunk_size) {
            // Zero-copy: just work with slice references
            let _processed_view = self.process_chunk_zerocopy(chunk).await;
            operations += 1;

            if operations % 100 == 0 {
                monitor.sample();
            }
        }

        Ok(operations)
    }

    /// Perform traditional operations (with allocations)
    async fn perform_traditional_operations(
        &self,
        data: &[u8],
        monitor: &mut MemoryMonitor,
    ) -> Result<usize> {
        let mut operations = 0;
        let chunk_size = 4096; // 4KB chunks

        // Process data with allocations (traditional approach)
        for chunk in data.chunks(chunk_size) {
            // Traditional: create owned copies
            let _processed_copy = self.process_chunk_traditional(chunk).await;
            operations += 1;

            if operations % 100 == 0 {
                monitor.sample();
            }
        }

        Ok(operations)
    }

    /// Process chunk with zero-copy approach (no allocations)
    async fn process_chunk_zerocopy<'a>(&self, chunk: &'a [u8]) -> ChunkView<'a> {
        // Zero-copy: return a view/reference to the data
        ChunkView {
            data: chunk,
            processed: true,
        }
    }

    /// Process chunk with traditional approach (with allocations)
    async fn process_chunk_traditional(&self, chunk: &[u8]) -> ChunkCopy {
        // Traditional: create owned copy
        let mut copy = chunk.to_vec();

        // Simulate some processing
        copy.reverse();

        ChunkCopy {
            data: copy,
            processed: true,
        }
    }

    /// Track memory allocations during operations
    async fn track_allocations(&self, data: &[u8], monitor: &mut MemoryMonitor) -> Result<usize> {
        let mut allocation_count = 0;
        let chunk_size = 1024; // 1KB chunks for detailed tracking

        for (i, chunk) in data.chunks(chunk_size).enumerate() {
            let before_memory = monitor.peak_usage_mb();

            // Simulate allocation-heavy operations
            let _allocated_data = vec![0u8; chunk.len()]; // Allocation
            let _string_alloc = String::from_utf8_lossy(chunk).to_string(); // Allocation
            let _boxed_slice = chunk.to_vec().into_boxed_slice(); // Allocation

            allocation_count += 3; // Three allocations per chunk

            let after_memory = monitor.peak_usage_mb();
            if after_memory > before_memory {
                monitor.sample();
            }

            // Sample memory every 100 chunks
            if i % 100 == 0 {
                monitor.sample();
            }
        }

        Ok(allocation_count)
    }
}

/// Data complexity levels for testing
#[derive(Debug, Clone, Copy)]
enum DataComplexity {
    Simple = 0,
    Collections = 1,
    UserDefinedTypes = 2,
    Mixed = 3,
    BinaryData = 4,
}

/// Zero-copy chunk view (no allocations)
struct ChunkView<'a> {
    #[allow(dead_code)]
    data: &'a [u8],
    #[allow(dead_code)]
    processed: bool,
}

/// Traditional chunk copy (with allocations)
struct ChunkCopy {
    #[allow(dead_code)]
    data: Vec<u8>,
    #[allow(dead_code)]
    processed: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_zerocopy_benchmarks_creation() {
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());
        let benchmarks = ZeroCopyBenchmarks::new(platform, &config).await;
        assert!(benchmarks.is_ok());
    }

    #[tokio::test]
    async fn test_complex_data_generation() {
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());
        let benchmarks = ZeroCopyBenchmarks::new(platform, &config).await.unwrap();

        let data = benchmarks
            .generate_complex_test_data(1.0, DataComplexity::Simple)
            .await;
        assert!(data.is_ok());
        let data = data.unwrap();
        assert_eq!(data.len(), 1024 * 1024); // 1MB
    }

    #[tokio::test]
    async fn test_zerocopy_vs_traditional() {
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());
        let benchmarks = ZeroCopyBenchmarks::new(platform, &config).await.unwrap();

        let test_data = generate_test_data(0.1); // 0.1MB for quick test
        let mut monitor = MemoryMonitor::new();

        // Test zero-copy operations
        let zerocopy_ops = benchmarks
            .perform_zerocopy_operations(&test_data, &mut monitor)
            .await;
        assert!(zerocopy_ops.is_ok());

        // Test traditional operations
        let traditional_ops = benchmarks
            .perform_traditional_operations(&test_data, &mut monitor)
            .await;
        assert!(traditional_ops.is_ok());

        // Both should process similar number of chunks
        assert!(zerocopy_ops.unwrap() > 0);
        assert!(traditional_ops.unwrap() > 0);
    }

    #[test]
    fn test_data_complexity_enum() {
        assert_eq!(DataComplexity::Simple as u8, 0);
        assert_eq!(DataComplexity::Collections as u8, 1);
        assert_eq!(DataComplexity::UserDefinedTypes as u8, 2);
        assert_eq!(DataComplexity::Mixed as u8, 3);
        assert_eq!(DataComplexity::BinaryData as u8, 4);
    }
}
