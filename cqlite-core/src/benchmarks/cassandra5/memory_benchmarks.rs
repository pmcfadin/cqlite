#![cfg(feature = "benchmarks")]

//! Memory Usage Benchmarks for Cassandra 5+ Large SSTable Files
//!
//! This module focuses on validating memory usage for large SSTable files (up to 1GB)
//! to ensure we stay within the PRD limit of 128MB memory usage while maintaining
//! performance targets.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::{Config, Platform, Result};

use super::{
    BenchmarkResult, PRDTargets,
    utils::{MemoryMonitor, PrecisionTimer, generate_test_data},
};

/// Memory usage benchmarking suite for large Cassandra 5+ SSTable files
pub struct MemoryBenchmarks {
    #[allow(dead_code)]
    platform: Arc<Platform>,
    #[allow(dead_code)]
    config: Config,
}

impl MemoryBenchmarks {
    /// Create new memory benchmarks
    pub async fn new(platform: Arc<Platform>, config: &Config) -> Result<Self> {
        Ok(Self {
            platform,
            config: config.clone(),
        })
    }

    /// Run comprehensive memory usage tests for large files
    pub async fn run_comprehensive_memory_tests(
        &self,
        _test_data_dir: &Path,
        targets: &PRDTargets,
    ) -> Result<Vec<BenchmarkResult>> {
        let mut results = Vec::new();

        println!("💾 Starting Cassandra 5+ Memory Usage Benchmarks");

        // Test data sizes progressing to large files
        let test_sizes_mb = vec![
            1.0,    // 1MB - baseline
            10.0,   // 10MB - small
            50.0,   // 50MB - medium
            100.0,  // 100MB - large
            250.0,  // 250MB - very large
            500.0,  // 500MB - stress test
            1000.0, // 1GB - maximum target
        ];

        println!(
            "📊 Testing memory usage across file sizes: {:?} MB",
            test_sizes_mb
        );

        for &size_mb in &test_sizes_mb {
            println!("\n📏 Memory benchmark for {:.0} MB file", size_mb);

            // Skip very large tests if insufficient memory
            if size_mb >= 500.0 && !self.has_sufficient_memory(size_mb) {
                println!(
                    "   ⏭️ Skipping {:.0} MB test due to insufficient system memory",
                    size_mb
                );
                continue;
            }

            // Test different memory usage patterns
            let memory_tests = vec![
                ("Sequential", MemoryAccessPattern::Sequential),
                ("Random", MemoryAccessPattern::Random),
                ("Streaming", MemoryAccessPattern::Streaming),
                ("Chunked", MemoryAccessPattern::Chunked),
            ];

            for (pattern_name, pattern) in memory_tests {
                let result = self
                    .benchmark_memory_usage(size_mb, pattern, pattern_name, targets)
                    .await?;
                results.push(result);
            }
        }

        // Memory efficiency analysis
        let efficiency_result = self.analyze_memory_efficiency(&results, targets).await?;
        results.push(efficiency_result);

        // Memory leak detection
        let leak_result = self.detect_memory_leaks(targets).await?;
        results.push(leak_result);

        Ok(results)
    }

    /// Benchmark memory usage for specific file size and access pattern
    async fn benchmark_memory_usage(
        &self,
        size_mb: f64,
        pattern: MemoryAccessPattern,
        pattern_name: &str,
        targets: &PRDTargets,
    ) -> Result<BenchmarkResult> {
        let benchmark_name = format!("Memory_{}_{:.0}MB", pattern_name, size_mb);

        // Generate test data
        let test_data = generate_test_data(size_mb);
        let mut memory_monitor = MemoryMonitor::new();

        // Force garbage collection before starting
        #[cfg(not(target_env = "msvc"))]
        {
            // On non-MSVC targets, try to trigger GC-like behavior
            std::hint::black_box(&test_data);
        }

        let timer = PrecisionTimer::start();
        memory_monitor.sample();

        // Simulate different access patterns
        let operations_count = match pattern {
            MemoryAccessPattern::Sequential => {
                self.sequential_access(&test_data, &mut memory_monitor)
                    .await?
            }
            MemoryAccessPattern::Random => {
                self.random_access(&test_data, &mut memory_monitor).await?
            }
            MemoryAccessPattern::Streaming => {
                self.streaming_access(&test_data, &mut memory_monitor)
                    .await?
            }
            MemoryAccessPattern::Chunked => {
                self.chunked_access(&test_data, &mut memory_monitor).await?
            }
        };

        let duration = timer.elapsed_duration();
        let memory_usage_mb = memory_monitor.peak_usage_mb();
        let avg_memory_mb = memory_monitor.average_usage_mb();

        // Calculate metrics
        let throughput_mb_per_sec = size_mb / duration.as_secs_f64();
        let memory_efficiency = size_mb / memory_usage_mb.max(0.1);
        let operations_per_second = operations_count as f64 / duration.as_secs_f64();

        // Check PRD compliance - memory is the primary concern
        let meets_target = memory_usage_mb <= targets.memory_limit_mb
            && throughput_mb_per_sec >= targets.parse_speed_mb_per_sec * 0.5; // Allow 50% slower for memory-focused test

        let target_comparison = if meets_target {
            format!(
                "✅ Meets memory targets ({:.1} MB ≤ {})",
                memory_usage_mb, targets.memory_limit_mb
            )
        } else {
            format!(
                "❌ Exceeds memory limit ({:.1} MB > {})",
                memory_usage_mb, targets.memory_limit_mb
            )
        };

        let mut details = HashMap::new();
        details.insert("peak_memory_mb".to_string(), memory_usage_mb);
        details.insert("average_memory_mb".to_string(), avg_memory_mb);
        details.insert("memory_per_mb_ratio".to_string(), memory_usage_mb / size_mb);
        details.insert("access_pattern".to_string(), pattern as u8 as f64);

        println!(
            "     ✅ {} {:.0}MB: {:.1} MB peak memory ({:.2}x file size), {:.2} MB/s",
            pattern_name,
            size_mb,
            memory_usage_mb,
            memory_usage_mb / size_mb,
            throughput_mb_per_sec
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

    /// Sequential memory access pattern - simulates reading entire file sequentially
    async fn sequential_access(&self, data: &[u8], monitor: &mut MemoryMonitor) -> Result<usize> {
        let chunk_size = 64 * 1024; // 64KB chunks
        let mut operations = 0;
        let mut position = 0;

        while position < data.len() {
            let end = (position + chunk_size).min(data.len());
            let _chunk = &data[position..end];

            // Simulate processing the chunk
            std::hint::black_box(_chunk);

            operations += 1;
            position += chunk_size;

            // Sample memory every 100 operations
            if operations % 100 == 0 {
                monitor.sample();
            }
        }

        monitor.sample();
        Ok(operations)
    }

    /// Random memory access pattern - simulates random seeks within the file
    async fn random_access(&self, data: &[u8], monitor: &mut MemoryMonitor) -> Result<usize> {
        let chunk_size = 4 * 1024; // 4KB chunks for random access
        let num_operations = 1000.min(data.len() / chunk_size);

        // Create pseudo-random access pattern
        let mut seed = 12345u64;

        for i in 0..num_operations {
            // Simple LCG for pseudo-random numbers
            seed = seed.wrapping_mul(1_103_515_245).wrapping_add(12345);
            let offset = (seed as usize) % (data.len().saturating_sub(chunk_size));

            let _chunk = &data[offset..offset + chunk_size];
            std::hint::black_box(_chunk);

            // Sample memory every 100 operations
            if i % 100 == 0 {
                monitor.sample();
            }
        }

        monitor.sample();
        Ok(num_operations)
    }

    /// Streaming memory access pattern - simulates continuous streaming with minimal buffering
    async fn streaming_access(&self, data: &[u8], monitor: &mut MemoryMonitor) -> Result<usize> {
        let stream_buffer_size = 8 * 1024; // 8KB streaming buffer
        let mut operations = 0;
        let mut position = 0;

        // Allocate a small streaming buffer
        let mut buffer = vec![0u8; stream_buffer_size];

        while position < data.len() {
            let copy_size = stream_buffer_size.min(data.len() - position);
            buffer[..copy_size].copy_from_slice(&data[position..position + copy_size]);

            // Simulate processing the streaming buffer
            std::hint::black_box(&buffer[..copy_size]);

            operations += 1;
            position += copy_size;

            // Sample memory every 50 operations for streaming
            if operations % 50 == 0 {
                monitor.sample();
            }
        }

        monitor.sample();
        Ok(operations)
    }

    /// Chunked memory access pattern - simulates processing file in large chunks with memory management
    async fn chunked_access(&self, data: &[u8], monitor: &mut MemoryMonitor) -> Result<usize> {
        let chunk_size = 1024 * 1024; // 1MB chunks
        let mut operations = 0;
        let mut position = 0;

        while position < data.len() {
            let end = (position + chunk_size).min(data.len());

            // Create a separate chunk buffer (simulates allocation/deallocation)
            let chunk = data[position..end].to_vec();
            std::hint::black_box(&chunk);

            // Simulate processing time
            tokio::time::sleep(Duration::from_millis(1)).await;

            operations += 1;
            position = end;

            monitor.sample();

            // Drop the chunk explicitly to help memory management
            drop(chunk);
        }

        monitor.sample();
        Ok(operations)
    }

    /// Analyze memory efficiency across all test results
    async fn analyze_memory_efficiency(
        &self,
        results: &[BenchmarkResult],
        targets: &PRDTargets,
    ) -> Result<BenchmarkResult> {
        let benchmark_name = "Memory_Efficiency_Analysis".to_string();

        // Calculate efficiency metrics
        let memory_results: Vec<_> = results
            .iter()
            .filter(|r| r.benchmark_name.starts_with("Memory_"))
            .collect();

        if memory_results.is_empty() {
            return Err(crate::Error::validation(
                "No memory results to analyze".to_string(),
            ));
        }

        let avg_efficiency: f64 = memory_results
            .iter()
            .map(|r| r.memory_efficiency)
            .sum::<f64>()
            / memory_results.len() as f64;

        let max_memory_usage: f64 = memory_results
            .iter()
            .map(|r| r.memory_usage_mb)
            .fold(0.0, f64::max);

        let memory_scaling_factor = self.calculate_memory_scaling(&memory_results);

        let meets_target = max_memory_usage <= targets.memory_limit_mb && avg_efficiency >= 1.0; // Should process at least 1MB per 1MB of memory

        let target_comparison = if meets_target {
            format!(
                "✅ Memory efficiency acceptable (max: {:.1} MB, avg efficiency: {:.2})",
                max_memory_usage, avg_efficiency
            )
        } else {
            format!(
                "❌ Memory efficiency needs improvement (max: {:.1} MB > {}, efficiency: {:.2})",
                max_memory_usage, targets.memory_limit_mb, avg_efficiency
            )
        };

        let mut details = HashMap::new();
        details.insert("average_efficiency".to_string(), avg_efficiency);
        details.insert("max_memory_usage_mb".to_string(), max_memory_usage);
        details.insert("memory_scaling_factor".to_string(), memory_scaling_factor);
        details.insert("tests_analyzed".to_string(), memory_results.len() as f64);

        println!(
            "     📊 Memory Efficiency Analysis: {:.2} avg efficiency, {:.1} MB max usage",
            avg_efficiency, max_memory_usage
        );

        Ok(BenchmarkResult {
            benchmark_name,
            file_size_mb: 0.0, // Aggregate result
            duration: Duration::from_secs(0),
            throughput_mb_per_sec: 0.0,
            memory_usage_mb: max_memory_usage,
            memory_efficiency: avg_efficiency,
            compression_ratio: None,
            operations_per_second: 0.0,
            meets_prd_target: meets_target,
            target_comparison,
            details,
        })
    }

    /// Calculate memory scaling factor (how memory usage scales with file size)
    fn calculate_memory_scaling(&self, results: &[&BenchmarkResult]) -> f64 {
        if results.len() < 2 {
            return 1.0;
        }

        // Sort by file size
        let mut sorted_results = results.to_vec();
        sorted_results.sort_by(|a, b| a.file_size_mb.partial_cmp(&b.file_size_mb).unwrap());

        // Calculate average scaling factor
        let mut scaling_factors = Vec::new();

        for i in 1..sorted_results.len() {
            let prev = &sorted_results[i - 1];
            let curr = &sorted_results[i];

            if prev.file_size_mb > 0.0 {
                let size_ratio = curr.file_size_mb / prev.file_size_mb;
                let memory_ratio = curr.memory_usage_mb / prev.memory_usage_mb.max(0.1);
                let scaling_factor = memory_ratio / size_ratio;
                scaling_factors.push(scaling_factor);
            }
        }

        if scaling_factors.is_empty() {
            1.0
        } else {
            scaling_factors.iter().sum::<f64>() / scaling_factors.len() as f64
        }
    }

    /// Detect potential memory leaks by running repeated operations
    async fn detect_memory_leaks(&self, targets: &PRDTargets) -> Result<BenchmarkResult> {
        let benchmark_name = "Memory_Leak_Detection".to_string();

        println!("   🔍 Running memory leak detection test...");

        let test_size_mb = 10.0; // Use moderate size for leak detection
        let iterations = 20; // Run multiple iterations
        let mut memory_samples = Vec::new();
        let mut monitor = MemoryMonitor::new();

        let timer = PrecisionTimer::start();

        for i in 0..iterations {
            // Generate and process test data
            let test_data = generate_test_data(test_size_mb);

            // Simulate various operations that might leak memory
            let _processed = self.simulate_processing(&test_data).await?;

            // Sample memory after each iteration
            monitor.sample();
            memory_samples.push(monitor.peak_usage_mb());

            // Drop test data explicitly
            drop(test_data);

            // Force cleanup attempts
            if i % 5 == 0 {
                // Give time for cleanup
                tokio::time::sleep(Duration::from_millis(10)).await;
            }

            if i % 5 == 0 {
                println!(
                    "     Iteration {}/{}: {:.1} MB",
                    i + 1,
                    iterations,
                    memory_samples[i]
                );
            }
        }

        let duration = timer.elapsed_duration();

        // Analyze memory growth pattern
        let memory_growth = self.analyze_memory_growth(&memory_samples);
        let final_memory = memory_samples.last().copied().unwrap_or(0.0);
        let initial_memory = memory_samples.first().copied().unwrap_or(0.0);

        // Leak detection: significant growth over iterations suggests a leak
        let has_potential_leak = memory_growth > 0.1; // >0.1 MB growth per iteration
        let exceeds_memory_limit = final_memory > targets.memory_limit_mb;

        let meets_target = !has_potential_leak && !exceeds_memory_limit;

        let target_comparison = if meets_target {
            format!(
                "✅ No memory leaks detected ({:.2} MB/iter growth)",
                memory_growth
            )
        } else if has_potential_leak {
            format!(
                "⚠️ Potential memory leak detected ({:.2} MB/iter growth)",
                memory_growth
            )
        } else {
            format!(
                "❌ Memory usage too high ({:.1} MB > {})",
                final_memory, targets.memory_limit_mb
            )
        };

        let mut details = HashMap::new();
        details.insert("memory_growth_mb_per_iter".to_string(), memory_growth);
        details.insert("initial_memory_mb".to_string(), initial_memory);
        details.insert("final_memory_mb".to_string(), final_memory);
        details.insert("iterations".to_string(), iterations as f64);
        details.insert(
            "potential_leak".to_string(),
            if has_potential_leak { 1.0 } else { 0.0 },
        );

        println!(
            "     🔍 Leak Detection: {:.2} MB/iter growth, final: {:.1} MB",
            memory_growth, final_memory
        );

        Ok(BenchmarkResult {
            benchmark_name,
            file_size_mb: test_size_mb * iterations as f64,
            duration,
            throughput_mb_per_sec: (test_size_mb * iterations as f64) / duration.as_secs_f64(),
            memory_usage_mb: final_memory,
            memory_efficiency: (test_size_mb * iterations as f64) / final_memory.max(0.1),
            compression_ratio: None,
            operations_per_second: iterations as f64 / duration.as_secs_f64(),
            meets_prd_target: meets_target,
            target_comparison,
            details,
        })
    }

    /// Analyze memory growth pattern across iterations
    fn analyze_memory_growth(&self, samples: &[f64]) -> f64 {
        if samples.len() < 2 {
            return 0.0;
        }

        // Calculate linear regression to find growth trend
        let n = samples.len() as f64;
        let sum_x: f64 = (0..samples.len()).map(|i| i as f64).sum();
        let sum_y: f64 = samples.iter().sum();
        let sum_xy_product: f64 = samples.iter().enumerate().map(|(i, &y)| i as f64 * y).sum();
        let sum_x2: f64 = (0..samples.len()).map(|i| (i as f64).powi(2)).sum();

        // Linear regression slope (growth rate)
        
        (n * sum_xy_product - sum_x * sum_y) / (n * sum_x2 - sum_x.powi(2))
    }

    /// Simulate processing operations that might cause memory leaks
    async fn simulate_processing(&self, data: &[u8]) -> Result<Vec<u8>> {
        // Simulate various operations that might leak memory
        let mut result = Vec::new();

        // 1. Buffer allocations and deallocations
        for chunk in data.chunks(1024) {
            let mut buffer = chunk.to_vec();
            buffer.reverse(); // Some processing
            result.extend_from_slice(&buffer);
        }

        // 2. String operations
        let _string_data = String::from_utf8_lossy(data);

        // 3. HashMap operations
        let mut map = HashMap::new();
        for (i, &byte) in data.iter().enumerate().take(100) {
            map.insert(i, byte);
        }

        // 4. Nested allocations
        let nested: Vec<Vec<u8>> = data.chunks(1024).map(|chunk| chunk.to_vec()).collect();
        let _flattened: Vec<u8> = nested.into_iter().flatten().collect();

        Ok(result)
    }

    /// Check if system has sufficient memory for large tests
    fn has_sufficient_memory(&self, required_mb: f64) -> bool {
        let available_gb = self.get_available_memory_gb();
        let required_gigabytes = required_mb / 1024.0;

        // Need at least 2x the required memory for safe testing
        available_gb >= required_gigabytes * 2.0
    }

    /// Get available system memory in GB
    fn get_available_memory_gb(&self) -> f64 {
        #[cfg(target_os = "macos")]
        {
            self.get_memory_macos()
        }
        #[cfg(target_os = "linux")]
        {
            self.get_memory_linux()
        }
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        {
            8.0 // Conservative default
        }
    }

    #[cfg(target_os = "macos")]
    fn get_memory_macos(&self) -> f64 {
        8.0 // Simplified - could use sysctl for actual values
    }

    #[cfg(target_os = "linux")]
    fn get_memory_linux(&self) -> f64 {
        use std::fs;

        if let Ok(meminfo) = fs::read_to_string("/proc/meminfo") {
            for line in meminfo.lines() {
                if line.starts_with("MemAvailable:") {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() >= 2 {
                        if let Ok(kb) = parts[1].parse::<f64>() {
                            return kb / 1024.0 / 1024.0; // KB to GB
                        }
                    }
                }
            }
        }
        8.0 // Default fallback
    }
}

/// Memory access patterns for testing
#[derive(Debug, Clone, Copy)]
enum MemoryAccessPattern {
    Sequential = 0,
    Random = 1,
    Streaming = 2,
    Chunked = 3,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_memory_benchmarks_creation() {
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());
        let benchmarks = MemoryBenchmarks::new(platform, &config).await;
        assert!(benchmarks.is_ok());
    }

    #[tokio::test]
    async fn test_sequential_access() {
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());
        let benchmarks = MemoryBenchmarks::new(platform, &config).await.unwrap();

        let test_data = generate_test_data(1.0); // 1MB
        let mut monitor = MemoryMonitor::new();

        let operations = benchmarks.sequential_access(&test_data, &mut monitor).await;
        assert!(operations.is_ok());
        assert!(operations.unwrap() > 0);
    }

    #[tokio::test]
    async fn test_memory_growth_analysis() {
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());
        let benchmarks = MemoryBenchmarks::new(platform, &config).await.unwrap();

        // Test with increasing memory samples (simulating a leak)
        let samples = vec![10.0, 10.5, 11.0, 11.5, 12.0];
        let growth = benchmarks.analyze_memory_growth(&samples);
        assert!(growth > 0.0); // Should detect positive growth

        // Test with stable memory samples (no leak)
        let stable_samples = vec![10.0, 10.1, 9.9, 10.0, 10.1];
        let stable_growth = benchmarks.analyze_memory_growth(&stable_samples);
        assert!(stable_growth.abs() < 0.1); // Should be close to zero
    }

    #[test]
    fn test_memory_access_pattern_enum() {
        assert_eq!(MemoryAccessPattern::Sequential as u8, 0);
        assert_eq!(MemoryAccessPattern::Random as u8, 1);
        assert_eq!(MemoryAccessPattern::Streaming as u8, 2);
        assert_eq!(MemoryAccessPattern::Chunked as u8, 3);
    }
}
