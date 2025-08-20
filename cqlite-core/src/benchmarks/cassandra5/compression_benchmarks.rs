#![cfg(feature = "benchmarks")]

//! Compression Performance Benchmarks for Cassandra 5+
//!
//! This module focuses on benchmarking compression performance for LZ4, Snappy, and Deflate
//! algorithms with Cassandra 5+ SSTable data, measuring both compression and decompression
//! speeds, memory usage, and compression ratios.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
// use std::time::Duration; // For future timing operations

use crate::{
    Config, Platform, Result,
    storage::sstable::compression::{Compression, CompressionAlgorithm},
};

use super::{
    BenchmarkResult, PRDTargets,
    utils::{MemoryMonitor, PrecisionTimer, generate_test_data},
};

/// Compression performance benchmarking suite for Cassandra 5+
pub struct CompressionBenchmarks {
    #[allow(dead_code)]
    platform: Arc<Platform>,
    #[allow(dead_code)]
    config: Config,
}

impl CompressionBenchmarks {
    /// Create new compression benchmarks
    pub async fn new(platform: Arc<Platform>, config: &Config) -> Result<Self> {
        Ok(Self {
            platform,
            config: config.clone(),
        })
    }

    /// Run comprehensive compression performance tests
    pub async fn run_compression_performance_tests(
        &self,
        _test_data_dir: &Path,
        targets: &PRDTargets,
    ) -> Result<Vec<BenchmarkResult>> {
        let mut results = Vec::new();

        println!("🗜️ Starting Cassandra 5+ Compression Benchmarks");

        // Test data sizes (in MB)
        let test_sizes = vec![1.0, 10.0, 50.0, 100.0, 500.0]; // Up to 500MB for practical testing

        // Compression algorithms to test
        let algorithms = vec![
            CompressionAlgorithm::Lz4,
            CompressionAlgorithm::Snappy,
            CompressionAlgorithm::Deflate,
        ];

        for &size_mb in &test_sizes {
            println!("\n📏 Testing with {:.1} MB test data", size_mb);

            // Generate test data
            let test_data = generate_test_data(size_mb);
            println!("   Generated {} bytes of test data", test_data.len());

            for algorithm in &algorithms {
                let algo_name = format!("{:?}", algorithm);
                println!("   🔄 Testing {} compression...", algo_name);

                // Test compression performance
                let compress_result = self
                    .benchmark_compression(&test_data, algorithm.clone(), size_mb, targets)
                    .await?;
                results.push(compress_result);

                // Test decompression performance
                let decompress_result = self
                    .benchmark_decompression(&test_data, algorithm.clone(), size_mb, targets)
                    .await?;
                results.push(decompress_result);

                // Test round-trip performance
                let roundtrip_result = self
                    .benchmark_roundtrip(&test_data, algorithm.clone(), size_mb, targets)
                    .await?;
                results.push(roundtrip_result);
            }
        }

        // Large file stress test (only if system has enough memory)
        if self.should_run_stress_test() {
            println!("\n🏋️ Running large file stress test (1GB)");
            let stress_results = self.run_large_file_stress_test(targets).await?;
            results.extend(stress_results);
        }

        Ok(results)
    }

    /// Benchmark compression performance
    async fn benchmark_compression(
        &self,
        test_data: &[u8],
        algorithm: CompressionAlgorithm,
        size_mb: f64,
        targets: &PRDTargets,
    ) -> Result<BenchmarkResult> {
        let algo_name = format!("{:?}", algorithm);
        let benchmark_name = format!("Compression_{}_{}MB", algo_name, size_mb);

        let compression = Compression::new(algorithm.clone())?;
        let mut memory_monitor = MemoryMonitor::new();

        // Warm up
        for _ in 0..3 {
            let _ = compression.compress(&test_data[..1024.min(test_data.len())]);
        }

        // Benchmark compression
        let timer = PrecisionTimer::start();
        memory_monitor.sample();

        let compressed_data = compression.compress(test_data)?;

        memory_monitor.sample();
        let duration = timer.elapsed_duration();

        // Calculate metrics
        let throughput_mb_per_sec = size_mb / duration.as_secs_f64();
        let memory_usage_mb = memory_monitor.peak_usage_mb();
        let memory_efficiency = size_mb / memory_usage_mb.max(0.1);
        let compression_ratio = compressed_data.len() as f64 / test_data.len() as f64;

        // Check PRD compliance
        let meets_target = throughput_mb_per_sec >= targets.parse_speed_mb_per_sec
            && memory_usage_mb <= targets.memory_limit_mb;

        let target_comparison = if meets_target {
            format!(
                "✅ Meets PRD targets ({:.1} MB/s, {:.1} MB)",
                throughput_mb_per_sec, memory_usage_mb
            )
        } else {
            format!(
                "❌ Below targets ({:.1} MB/s vs {:.1}, {:.1} MB vs {:.1})",
                throughput_mb_per_sec,
                targets.parse_speed_mb_per_sec,
                memory_usage_mb,
                targets.memory_limit_mb
            )
        };

        let mut details = HashMap::new();
        details.insert("compression_ratio".to_string(), compression_ratio);
        details.insert(
            "compressed_size_mb".to_string(),
            compressed_data.len() as f64 / 1024.0 / 1024.0,
        );
        details.insert(
            "space_saved_percent".to_string(),
            (1.0 - compression_ratio) * 100.0,
        );

        println!(
            "     ✅ {} compression: {:.2} MB/s, {:.1} MB memory, {:.1}% compression",
            algo_name,
            throughput_mb_per_sec,
            memory_usage_mb,
            (1.0 - compression_ratio) * 100.0
        );

        Ok(BenchmarkResult {
            benchmark_name,
            file_size_mb: size_mb,
            duration,
            throughput_mb_per_sec,
            memory_usage_mb,
            memory_efficiency,
            compression_ratio: Some(compression_ratio),
            operations_per_second: 1.0 / duration.as_secs_f64(), // One compression operation
            meets_prd_target: meets_target,
            target_comparison,
            details,
        })
    }

    /// Benchmark decompression performance
    async fn benchmark_decompression(
        &self,
        test_data: &[u8],
        algorithm: CompressionAlgorithm,
        size_mb: f64,
        targets: &PRDTargets,
    ) -> Result<BenchmarkResult> {
        let algo_name = format!("{:?}", algorithm);
        let benchmark_name = format!("Decompression_{}_{}MB", algo_name, size_mb);

        let compression = Compression::new(algorithm.clone())?;

        // First, compress the data
        let compressed_data = compression.compress(test_data)?;
        let mut memory_monitor = MemoryMonitor::new();

        // Warm up decompression
        for _ in 0..3 {
            let sample_size = 1024.min(compressed_data.len());
            let sample = &compressed_data[..sample_size];
            if sample.len() >= 4 {
                // Ensure we have enough data for decompression
                let _ = compression.decompress(sample);
            }
        }

        // Benchmark decompression
        let timer = PrecisionTimer::start();
        memory_monitor.sample();

        let decompressed_data = compression.decompress(&compressed_data)?;

        memory_monitor.sample();
        let duration = timer.elapsed_duration();

        // Verify data integrity
        assert_eq!(
            decompressed_data.len(),
            test_data.len(),
            "Decompressed data size mismatch"
        );
        assert_eq!(
            decompressed_data, test_data,
            "Decompressed data content mismatch"
        );

        // Calculate metrics
        let throughput_mb_per_sec = size_mb / duration.as_secs_f64();
        let memory_usage_mb = memory_monitor.peak_usage_mb();
        let memory_efficiency = size_mb / memory_usage_mb.max(0.1);
        let compression_ratio = compressed_data.len() as f64 / test_data.len() as f64;

        // Check PRD compliance - decompression should be faster than compression
        let decompression_target = targets.parse_speed_mb_per_sec * 1.5; // 150% of compression target
        let meets_target = throughput_mb_per_sec >= decompression_target
            && memory_usage_mb <= targets.memory_limit_mb;

        let target_comparison = if meets_target {
            format!(
                "✅ Meets PRD targets ({:.1} MB/s, {:.1} MB)",
                throughput_mb_per_sec, memory_usage_mb
            )
        } else {
            format!(
                "❌ Below targets ({:.1} MB/s vs {:.1}, {:.1} MB vs {:.1})",
                throughput_mb_per_sec,
                decompression_target,
                memory_usage_mb,
                targets.memory_limit_mb
            )
        };

        let mut details = HashMap::new();
        details.insert("compression_ratio".to_string(), compression_ratio);
        details.insert(
            "compressed_size_mb".to_string(),
            compressed_data.len() as f64 / 1024.0 / 1024.0,
        );
        details.insert(
            "decompression_speed_ratio".to_string(),
            throughput_mb_per_sec / targets.parse_speed_mb_per_sec,
        );

        println!(
            "     ✅ {} decompression: {:.2} MB/s, {:.1} MB memory",
            algo_name, throughput_mb_per_sec, memory_usage_mb
        );

        Ok(BenchmarkResult {
            benchmark_name,
            file_size_mb: size_mb,
            duration,
            throughput_mb_per_sec,
            memory_usage_mb,
            memory_efficiency,
            compression_ratio: Some(compression_ratio),
            operations_per_second: 1.0 / duration.as_secs_f64(),
            meets_prd_target: meets_target,
            target_comparison,
            details,
        })
    }

    /// Benchmark round-trip compression/decompression performance
    async fn benchmark_roundtrip(
        &self,
        test_data: &[u8],
        algorithm: CompressionAlgorithm,
        size_mb: f64,
        targets: &PRDTargets,
    ) -> Result<BenchmarkResult> {
        let algo_name = format!("{:?}", algorithm);
        let benchmark_name = format!("Roundtrip_{}_{}MB", algo_name, size_mb);

        let compression = Compression::new(algorithm.clone())?;
        let mut memory_monitor = MemoryMonitor::new();

        // Benchmark full round-trip
        let timer = PrecisionTimer::start();
        memory_monitor.sample();

        // Compress
        let compressed_data = compression.compress(test_data)?;
        memory_monitor.sample();

        // Decompress
        let decompressed_data = compression.decompress(&compressed_data)?;
        memory_monitor.sample();

        let duration = timer.elapsed_duration();

        // Verify data integrity
        assert_eq!(
            decompressed_data, test_data,
            "Round-trip data integrity check failed"
        );

        // Calculate metrics
        let throughput_mb_per_sec = size_mb / duration.as_secs_f64();
        let memory_usage_mb = memory_monitor.peak_usage_mb();
        let memory_efficiency = size_mb / memory_usage_mb.max(0.1);
        let compression_ratio = compressed_data.len() as f64 / test_data.len() as f64;

        // Round-trip should meet at least 75% of parse speed target
        let roundtrip_target = targets.parse_speed_mb_per_sec * 0.75;
        let meets_target =
            throughput_mb_per_sec >= roundtrip_target && memory_usage_mb <= targets.memory_limit_mb;

        let target_comparison = if meets_target {
            format!(
                "✅ Meets PRD targets ({:.1} MB/s, {:.1} MB)",
                throughput_mb_per_sec, memory_usage_mb
            )
        } else {
            format!(
                "❌ Below targets ({:.1} MB/s vs {:.1}, {:.1} MB vs {:.1})",
                throughput_mb_per_sec, roundtrip_target, memory_usage_mb, targets.memory_limit_mb
            )
        };

        let mut details = HashMap::new();
        details.insert("compression_ratio".to_string(), compression_ratio);
        details.insert(
            "space_saved_mb".to_string(),
            (test_data.len() - compressed_data.len()) as f64 / 1024.0 / 1024.0,
        );
        details.insert(
            "roundtrip_efficiency".to_string(),
            throughput_mb_per_sec / targets.parse_speed_mb_per_sec,
        );

        println!(
            "     ✅ {} round-trip: {:.2} MB/s, {:.1} MB memory",
            algo_name, throughput_mb_per_sec, memory_usage_mb
        );

        Ok(BenchmarkResult {
            benchmark_name,
            file_size_mb: size_mb,
            duration,
            throughput_mb_per_sec,
            memory_usage_mb,
            memory_efficiency,
            compression_ratio: Some(compression_ratio),
            operations_per_second: 1.0 / duration.as_secs_f64(),
            meets_prd_target: meets_target,
            target_comparison,
            details,
        })
    }

    /// Run large file stress test for 1GB files
    async fn run_large_file_stress_test(
        &self,
        targets: &PRDTargets,
    ) -> Result<Vec<BenchmarkResult>> {
        let mut results = Vec::new();

        // Only test most efficient algorithms for large files
        let algorithms = vec![
            CompressionAlgorithm::Lz4,    // Fastest
            CompressionAlgorithm::Snappy, // Balanced
        ];

        for algorithm in algorithms {
            let algo_name = format!("{:?}", algorithm);
            println!("   🏋️ Large file stress test: {} with 1GB data", algo_name);

            // Use chunked approach for 1GB to avoid excessive memory usage
            let chunk_size_mb = 50.0; // 50MB chunks
            let num_chunks = (1000.0 / chunk_size_mb) as usize; // 20 chunks for 1GB

            let benchmark_name = format!("LargeFile_{}_1GB", algo_name);
            let compression = Compression::new(algorithm.clone())?;
            let mut memory_monitor = MemoryMonitor::new();

            let timer = PrecisionTimer::start();
            let mut total_original_size = 0;
            let mut total_compressed_size = 0;

            memory_monitor.sample();

            // Process in chunks
            for chunk_idx in 0..num_chunks {
                let chunk_data = generate_test_data(chunk_size_mb);
                total_original_size += chunk_data.len();

                let compressed_chunk = compression.compress(&chunk_data)?;
                total_compressed_size += compressed_chunk.len();

                // Verify decompression works
                let decompressed_chunk = compression.decompress(&compressed_chunk)?;
                assert_eq!(decompressed_chunk.len(), chunk_data.len());

                memory_monitor.sample();

                if chunk_idx % 5 == 0 {
                    println!(
                        "     Progress: {}/{} chunks processed",
                        chunk_idx + 1,
                        num_chunks
                    );
                }
            }

            let duration = timer.elapsed_duration();
            let size_gb = total_original_size as f64 / 1024.0 / 1024.0 / 1024.0;
            let throughput_mb_per_sec =
                (total_original_size as f64 / 1024.0 / 1024.0) / duration.as_secs_f64();
            let memory_usage_mb = memory_monitor.peak_usage_mb();
            let compression_ratio = total_compressed_size as f64 / total_original_size as f64;

            // Large file targets should be more relaxed
            let large_file_speed_target = targets.parse_speed_mb_per_sec * 0.8; // 80% of normal target
            let meets_target = throughput_mb_per_sec >= large_file_speed_target
                && memory_usage_mb <= targets.memory_limit_mb;

            let target_comparison = if meets_target {
                format!(
                    "✅ Meets large file targets ({:.1} MB/s, {:.1} MB)",
                    throughput_mb_per_sec, memory_usage_mb
                )
            } else {
                format!(
                    "❌ Below large file targets ({:.1} MB/s vs {:.1}, {:.1} MB vs {:.1})",
                    throughput_mb_per_sec,
                    large_file_speed_target,
                    memory_usage_mb,
                    targets.memory_limit_mb
                )
            };

            let mut details = HashMap::new();
            details.insert("compression_ratio".to_string(), compression_ratio);
            details.insert("total_size_gb".to_string(), size_gb);
            details.insert("chunks_processed".to_string(), num_chunks as f64);
            details.insert(
                "space_saved_mb".to_string(),
                (total_original_size - total_compressed_size) as f64 / 1024.0 / 1024.0,
            );

            println!(
                "     ✅ {} 1GB test: {:.2} MB/s, {:.1} MB memory, {:.1}% compression",
                algo_name,
                throughput_mb_per_sec,
                memory_usage_mb,
                (1.0 - compression_ratio) * 100.0
            );

            results.push(BenchmarkResult {
                benchmark_name,
                file_size_mb: 1024.0, // 1GB
                duration,
                throughput_mb_per_sec,
                memory_usage_mb,
                memory_efficiency: 1024.0 / memory_usage_mb.max(0.1),
                compression_ratio: Some(compression_ratio),
                operations_per_second: num_chunks as f64 / duration.as_secs_f64(),
                meets_prd_target: meets_target,
                target_comparison,
                details,
            });
        }

        Ok(results)
    }

    /// Determine if we should run the memory-intensive stress test
    fn should_run_stress_test(&self) -> bool {
        // Check available system memory
        let available_memory_gb = self.get_available_memory_gb();

        // Only run stress test if we have at least 4GB available
        // (need ~2GB for data + compression overhead)
        available_memory_gb >= 4.0
    }

    /// Get available system memory in GB (rough estimate)
    fn get_available_memory_gb(&self) -> f64 {
        #[cfg(target_os = "macos")]
        {
            self.get_available_memory_macos()
        }
        #[cfg(target_os = "linux")]
        {
            self.get_available_memory_linux()
        }
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        {
            8.0 // Conservative assumption for other platforms
        }
    }

    #[cfg(target_os = "macos")]
    fn get_available_memory_macos(&self) -> f64 {
        use std::process::{Command, Stdio};

        let output = Command::new("vm_stat").stdout(Stdio::piped()).output();

        if let Ok(output) = output {
            let _vm_stat = String::from_utf8_lossy(&output.stdout);
            // Parse vm_stat output to estimate available memory
            // This is a simplified estimation
            8.0 // Default to 8GB for now
        } else {
            8.0
        }
    }

    #[cfg(target_os = "linux")]
    fn get_available_memory_linux(&self) -> f64 {
        use std::fs;

        if let Ok(meminfo) = fs::read_to_string("/proc/meminfo") {
            for line in meminfo.lines() {
                if line.starts_with("MemAvailable:") {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() >= 2 {
                        if let Ok(kb) = parts[1].parse::<f64>() {
                            return kb / 1024.0 / 1024.0; // Convert KB to GB
                        }
                    }
                }
            }
        }
        8.0 // Default fallback
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[allow(unused_imports)]
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_compression_benchmarks_creation() {
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());
        let benchmarks = CompressionBenchmarks::new(platform, &config).await;
        assert!(benchmarks.is_ok());
    }

    #[tokio::test]
    async fn test_compression_benchmark() {
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());
        let benchmarks = CompressionBenchmarks::new(platform, &config).await.unwrap();

        let test_data = generate_test_data(1.0); // 1MB test data
        let targets = PRDTargets::default();

        let result = benchmarks
            .benchmark_compression(&test_data, CompressionAlgorithm::Lz4, 1.0, &targets)
            .await;

        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.file_size_mb, 1.0);
        assert!(result.throughput_mb_per_sec > 0.0);
        assert!(result.compression_ratio.is_some());
    }

    #[tokio::test]
    async fn test_decompression_benchmark() {
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());
        let benchmarks = CompressionBenchmarks::new(platform, &config).await.unwrap();

        let test_data = generate_test_data(1.0); // 1MB test data
        let targets = PRDTargets::default();

        let result = benchmarks
            .benchmark_decompression(&test_data, CompressionAlgorithm::Lz4, 1.0, &targets)
            .await;

        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.file_size_mb, 1.0);
        assert!(result.throughput_mb_per_sec > 0.0);
    }

    #[tokio::test]
    async fn test_roundtrip_benchmark() {
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());
        let benchmarks = CompressionBenchmarks::new(platform, &config).await.unwrap();

        let test_data = generate_test_data(1.0); // 1MB test data
        let targets = PRDTargets::default();

        // Use Lz4 instead of Snappy since it's enabled by default
        let result = benchmarks
            .benchmark_roundtrip(&test_data, CompressionAlgorithm::Lz4, 1.0, &targets)
            .await;

        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.file_size_mb, 1.0);
        assert!(result.throughput_mb_per_sec > 0.0);
        assert!(result.compression_ratio.is_some());
    }
}
