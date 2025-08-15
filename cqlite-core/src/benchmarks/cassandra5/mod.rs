//! Cassandra 5+ Performance Benchmarking Suite
//!
//! This module provides comprehensive performance benchmarking specifically for Cassandra 5+
//! SSTable format, focusing on the PRD requirements:
//! - Memory usage validation for large files (up to 1GB)
//! - Compression performance (LZ4, Snappy, Deflate)
//! - Zero-copy deserialization benchmarks
//! - Throughput comparison with native Cassandra tools

pub mod compression_benchmarks;
pub mod memory_benchmarks;
pub mod throughput_benchmarks;
pub mod zerocopy_benchmarks;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::{Config, Platform, Result};

/// Cassandra 5+ performance benchmarking coordinator
pub struct Cassandra5PerformanceSuite {
    config: Config,
    platform: Arc<Platform>,
    results: HashMap<String, BenchmarkResult>,
}

/// Performance benchmark result for Cassandra 5+
#[derive(Debug, Clone)]
pub struct BenchmarkResult {
    pub benchmark_name: String,
    pub file_size_mb: f64,
    pub duration: Duration,
    pub throughput_mb_per_sec: f64,
    pub memory_usage_mb: f64,
    pub memory_efficiency: f64,
    pub compression_ratio: Option<f64>,
    pub operations_per_second: f64,
    pub meets_prd_target: bool,
    pub target_comparison: String,
    pub details: HashMap<String, f64>,
}

impl Cassandra5PerformanceSuite {
    /// Create new performance suite
    pub async fn new() -> Result<Self> {
        let config = Config::performance_optimized();
        let platform = Arc::new(Platform::new(&config).await?);

        Ok(Self {
            config,
            platform,
            results: HashMap::new(),
        })
    }

    /// Run comprehensive Cassandra 5+ performance benchmarks
    pub async fn run_comprehensive_benchmarks(
        &mut self,
        test_data_dir: &Path,
    ) -> Result<Vec<BenchmarkResult>> {
        println!("🚀 Starting Cassandra 5+ Performance Benchmarking Suite");
        println!("═══════════════════════════════════════════════════════");

        // PRD Performance Targets
        let prd_targets = PRDTargets {
            parse_speed_mb_per_sec: 100.0,
            memory_limit_mb: 128.0,
            throughput_ops_per_sec: 100_000.0,
            max_file_size_gb: 1.0,
        };

        println!("🎯 PRD Performance Targets:");
        println!(
            "   Parse Speed: ≥{} MB/s",
            prd_targets.parse_speed_mb_per_sec
        );
        println!("   Memory Limit: ≤{} MB", prd_targets.memory_limit_mb);
        println!(
            "   Throughput: ≥{} ops/sec",
            prd_targets.throughput_ops_per_sec
        );
        println!("   Max File Size: {} GB", prd_targets.max_file_size_gb);

        // Run benchmarks in sequence
        let mut all_results = Vec::new();

        // 1. Memory Usage Benchmarks
        println!("\n📊 1. Memory Usage Benchmarks");
        let memory_results = self
            .run_memory_benchmarks(test_data_dir, &prd_targets)
            .await?;
        all_results.extend(memory_results);

        // 2. Compression Benchmarks
        println!("\n🗜️ 2. Compression Performance Benchmarks");
        let compression_results = self
            .run_compression_benchmarks(test_data_dir, &prd_targets)
            .await?;
        all_results.extend(compression_results);

        // 3. Zero-Copy Benchmarks
        println!("\n⚡ 3. Zero-Copy Deserialization Benchmarks");
        let zerocopy_results = self
            .run_zerocopy_benchmarks(test_data_dir, &prd_targets)
            .await?;
        all_results.extend(zerocopy_results);

        // 4. Throughput Benchmarks
        println!("\n🏃 4. Throughput Benchmarks");
        let throughput_results = self
            .run_throughput_benchmarks(test_data_dir, &prd_targets)
            .await?;
        all_results.extend(throughput_results);

        // Store results
        for result in &all_results {
            self.results
                .insert(result.benchmark_name.clone(), result.clone());
        }

        // Generate summary report
        self.print_performance_summary(&all_results, &prd_targets);

        Ok(all_results)
    }

    /// Run memory usage benchmarks for large SSTable files
    async fn run_memory_benchmarks(
        &self,
        test_data_dir: &Path,
        targets: &PRDTargets,
    ) -> Result<Vec<BenchmarkResult>> {
        use memory_benchmarks::MemoryBenchmarks;

        let memory_bench = MemoryBenchmarks::new(Arc::clone(&self.platform), &self.config).await?;
        memory_bench
            .run_comprehensive_memory_tests(test_data_dir, targets)
            .await
    }

    /// Run compression performance benchmarks
    async fn run_compression_benchmarks(
        &self,
        test_data_dir: &Path,
        targets: &PRDTargets,
    ) -> Result<Vec<BenchmarkResult>> {
        use compression_benchmarks::CompressionBenchmarks;

        let compression_bench =
            CompressionBenchmarks::new(Arc::clone(&self.platform), &self.config).await?;
        compression_bench
            .run_compression_performance_tests(test_data_dir, targets)
            .await
    }

    /// Run zero-copy deserialization benchmarks
    async fn run_zerocopy_benchmarks(
        &self,
        test_data_dir: &Path,
        targets: &PRDTargets,
    ) -> Result<Vec<BenchmarkResult>> {
        use zerocopy_benchmarks::ZeroCopyBenchmarks;

        let zerocopy_bench =
            ZeroCopyBenchmarks::new(Arc::clone(&self.platform), &self.config).await?;
        zerocopy_bench
            .run_zerocopy_performance_tests(test_data_dir, targets)
            .await
    }

    /// Run throughput benchmarks vs native tools
    async fn run_throughput_benchmarks(
        &self,
        test_data_dir: &Path,
        targets: &PRDTargets,
    ) -> Result<Vec<BenchmarkResult>> {
        use throughput_benchmarks::ThroughputBenchmarks;

        let throughput_bench =
            ThroughputBenchmarks::new(Arc::clone(&self.platform), &self.config).await?;
        throughput_bench
            .run_throughput_comparison_tests(test_data_dir, targets)
            .await
    }

    /// Print comprehensive performance summary
    fn print_performance_summary(&self, results: &[BenchmarkResult], targets: &PRDTargets) {
        println!("\n📊 CASSANDRA 5+ PERFORMANCE SUMMARY");
        println!("═══════════════════════════════════════════");

        let passed = results.iter().filter(|r| r.meets_prd_target).count();
        let total = results.len();
        let pass_rate = if total > 0 {
            (passed as f64 / total as f64) * 100.0
        } else {
            0.0
        };

        println!(
            "Overall Results: {}/{} benchmarks passed ({:.1}%)",
            passed, total, pass_rate
        );

        // Category breakdown
        let categories = ["Memory", "Compression", "ZeroCopy", "Throughput"];
        for category in &categories {
            let category_results: Vec<_> = results
                .iter()
                .filter(|r| r.benchmark_name.contains(category))
                .collect();

            if !category_results.is_empty() {
                let category_passed = category_results
                    .iter()
                    .filter(|r| r.meets_prd_target)
                    .count();
                let category_total = category_results.len();
                let category_pass_rate = (category_passed as f64 / category_total as f64) * 100.0;

                println!(
                    "\n{} Benchmarks: {}/{} passed ({:.1}%)",
                    category, category_passed, category_total, category_pass_rate
                );

                for result in &category_results {
                    let status = if result.meets_prd_target {
                        "✅"
                    } else {
                        "❌"
                    };
                    println!(
                        "  {} {}: {:.2} MB/s, {:.1} MB memory",
                        status,
                        result.benchmark_name,
                        result.throughput_mb_per_sec,
                        result.memory_usage_mb
                    );
                }
            }
        }

        // Best performers
        if !results.is_empty() {
            let fastest = results
                .iter()
                .max_by(|a, b| {
                    a.throughput_mb_per_sec
                        .partial_cmp(&b.throughput_mb_per_sec)
                        .unwrap()
                })
                .unwrap();
            let most_efficient = results
                .iter()
                .min_by(|a, b| a.memory_usage_mb.partial_cmp(&b.memory_usage_mb).unwrap())
                .unwrap();

            println!("\n🏆 TOP PERFORMERS");
            println!(
                "   🚀 Fastest: {} ({:.2} MB/s)",
                fastest.benchmark_name, fastest.throughput_mb_per_sec
            );
            println!(
                "   💾 Most memory efficient: {} ({:.1} MB)",
                most_efficient.benchmark_name, most_efficient.memory_usage_mb
            );
        }

        // PRD compliance summary
        println!("\n🎯 PRD COMPLIANCE ANALYSIS");
        self.analyze_prd_compliance(results, targets);

        // Optimization recommendations
        println!("\n💡 OPTIMIZATION RECOMMENDATIONS");
        self.generate_optimization_recommendations(results, targets);
    }

    /// Analyze PRD compliance in detail
    fn analyze_prd_compliance(&self, results: &[BenchmarkResult], targets: &PRDTargets) {
        // Parse speed compliance
        let parse_speed_results: Vec<_> = results
            .iter()
            .filter(|r| r.throughput_mb_per_sec > 0.0)
            .collect();

        if !parse_speed_results.is_empty() {
            let avg_parse_speed: f64 = parse_speed_results
                .iter()
                .map(|r| r.throughput_mb_per_sec)
                .sum::<f64>()
                / parse_speed_results.len() as f64;

            let speed_compliance = avg_parse_speed >= targets.parse_speed_mb_per_sec;
            let speed_icon = if speed_compliance { "✅" } else { "❌" };

            println!(
                "   {} Parse Speed: {:.2} MB/s (target: ≥{} MB/s)",
                speed_icon, avg_parse_speed, targets.parse_speed_mb_per_sec
            );
        }

        // Memory usage compliance
        let memory_results: Vec<_> = results.iter().filter(|r| r.memory_usage_mb > 0.0).collect();

        if !memory_results.is_empty() {
            let max_memory = memory_results
                .iter()
                .map(|r| r.memory_usage_mb)
                .fold(0.0, f64::max);

            let memory_compliance = max_memory <= targets.memory_limit_mb;
            let memory_icon = if memory_compliance { "✅" } else { "❌" };

            println!(
                "   {} Memory Usage: {:.1} MB peak (target: ≤{} MB)",
                memory_icon, max_memory, targets.memory_limit_mb
            );
        }

        // Throughput compliance
        let throughput_results: Vec<_> = results
            .iter()
            .filter(|r| r.operations_per_second > 0.0)
            .collect();

        if !throughput_results.is_empty() {
            let max_throughput = throughput_results
                .iter()
                .map(|r| r.operations_per_second)
                .fold(0.0, f64::max);

            let throughput_compliance = max_throughput >= targets.throughput_ops_per_sec;
            let throughput_icon = if throughput_compliance { "✅" } else { "❌" };

            println!(
                "   {} Throughput: {:.0} ops/sec (target: ≥{} ops/sec)",
                throughput_icon, max_throughput, targets.throughput_ops_per_sec
            );
        }
    }

    /// Generate optimization recommendations based on benchmark results
    fn generate_optimization_recommendations(
        &self,
        results: &[BenchmarkResult],
        targets: &PRDTargets,
    ) {
        let mut recommendations = Vec::new();

        // Analyze memory usage patterns
        let high_memory_results: Vec<_> = results
            .iter()
            .filter(|r| r.memory_usage_mb > targets.memory_limit_mb * 0.8) // >80% of limit
            .collect();

        if !high_memory_results.is_empty() {
            recommendations.push(
                "🔧 Consider implementing streaming readers for large files to reduce memory usage",
            );
            recommendations.push("🔧 Optimize buffer pool sizes to stay within memory limits");
        }

        // Analyze throughput issues
        let slow_results: Vec<_> = results
            .iter()
            .filter(|r| r.throughput_mb_per_sec < targets.parse_speed_mb_per_sec * 0.5) // <50% of target
            .collect();

        if !slow_results.is_empty() {
            recommendations.push("⚡ Investigate I/O bottlenecks in slow parsing operations");
            recommendations.push("⚡ Consider parallel processing for large SSTable files");
        }

        // Compression-specific recommendations
        let compression_results: Vec<_> = results
            .iter()
            .filter(|r| r.benchmark_name.contains("Compression"))
            .collect();

        if !compression_results.is_empty() {
            let best_compression = compression_results.iter().max_by(|a, b| {
                a.throughput_mb_per_sec
                    .partial_cmp(&b.throughput_mb_per_sec)
                    .unwrap()
            });

            if let Some(best) = best_compression {
                let _algorithm_name = best.benchmark_name.replace("Compression", "");
                recommendations
                    .push("🗜️ Recommend optimal compression algorithm based on benchmark results");
            }
        }

        // General recommendations
        if results.iter().any(|r| !r.meets_prd_target) {
            recommendations.push(
                "📊 Review benchmark methodology and consider hardware-specific optimizations",
            );
            recommendations
                .push("🔍 Profile critical paths for additional optimization opportunities");
        }

        if recommendations.is_empty() {
            println!("   🎉 All benchmarks performing well! No immediate optimizations needed.");
        } else {
            for (i, rec) in recommendations.iter().enumerate() {
                println!("   {}. {}", i + 1, rec);
            }
        }
    }

    /// Export benchmark results for analysis
    pub fn export_results(&self) -> HashMap<String, BenchmarkResult> {
        self.results.clone()
    }
}

/// PRD performance targets for Cassandra 5+
#[derive(Debug, Clone)]
pub struct PRDTargets {
    pub parse_speed_mb_per_sec: f64,
    pub memory_limit_mb: f64,
    pub throughput_ops_per_sec: f64,
    pub max_file_size_gb: f64,
}

impl Default for PRDTargets {
    fn default() -> Self {
        Self {
            parse_speed_mb_per_sec: 100.0,
            memory_limit_mb: 128.0,
            throughput_ops_per_sec: 100_000.0,
            max_file_size_gb: 1.0,
        }
    }
}

/// Benchmark execution helper utilities
pub mod utils {
    use super::*;
    use std::process::{Command, Stdio};

    /// Measure memory usage during operation
    pub struct MemoryMonitor {
        baseline_mb: f64,
        peak_mb: f64,
        samples: Vec<f64>,
    }

    impl MemoryMonitor {
        pub fn new() -> Self {
            let baseline = get_process_memory_mb();
            Self {
                baseline_mb: baseline,
                peak_mb: baseline,
                samples: vec![baseline],
            }
        }

        pub fn sample(&mut self) {
            let current = get_process_memory_mb();
            self.samples.push(current);
            self.peak_mb = self.peak_mb.max(current);
        }

        pub fn peak_usage_mb(&self) -> f64 {
            self.peak_mb - self.baseline_mb
        }

        pub fn average_usage_mb(&self) -> f64 {
            let sum: f64 = self.samples.iter().map(|&s| s - self.baseline_mb).sum();
            sum / self.samples.len() as f64
        }
    }

    /// Get current process memory usage in MB
    pub fn get_process_memory_mb() -> f64 {
        #[cfg(target_os = "macos")]
        {
            get_memory_usage_macos()
        }
        #[cfg(target_os = "linux")]
        {
            get_memory_usage_linux()
        }
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        {
            0.0 // Fallback for unsupported platforms
        }
    }

    #[cfg(target_os = "macos")]
    fn get_memory_usage_macos() -> f64 {
        use std::process;

        let pid = process::id();
        let output = Command::new("ps")
            .args(["-o", "rss=", "-p", &pid.to_string()])
            .stdout(Stdio::piped())
            .output();

        if let Ok(output) = output {
            let rss_kb = String::from_utf8_lossy(&output.stdout)
                .trim()
                .parse::<f64>()
                .unwrap_or(0.0);
            rss_kb / 1024.0 // Convert KB to MB
        } else {
            0.0
        }
    }

    #[cfg(target_os = "linux")]
    fn get_memory_usage_linux() -> f64 {
        use std::fs;

        let status = fs::read_to_string("/proc/self/status").unwrap_or_default();
        for line in status.lines() {
            if line.starts_with("VmRSS:") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    if let Ok(kb) = parts[1].parse::<f64>() {
                        return kb / 1024.0; // Convert KB to MB
                    }
                }
            }
        }
        0.0
    }

    /// Generate test data of specified size with realistic SSTable patterns
    pub fn generate_test_data(size_mb: f64) -> Vec<u8> {
        let size_bytes = (size_mb * 1024.0 * 1024.0) as usize;
        let mut data = Vec::with_capacity(size_bytes);

        // Pattern that resembles SSTable data with some compressibility
        let patterns = [
            b"SSTable_row_key_",
            b"timestamp_value_",
            b"column_data_cont",
            b"metadata_info___",
        ];

        let mut pattern_idx = 0;
        while data.len() < size_bytes {
            let pattern = patterns[pattern_idx % patterns.len()];
            data.extend_from_slice(pattern);

            // Add some varying data
            let counter = (data.len() / pattern.len()) as u32;
            data.extend_from_slice(&counter.to_be_bytes());

            pattern_idx += 1;
        }

        data.truncate(size_bytes);
        data
    }

    /// Measure operation duration with high precision
    pub struct PrecisionTimer {
        start: Instant,
    }

    impl PrecisionTimer {
        pub fn start() -> Self {
            Self {
                start: Instant::now(),
            }
        }

        pub fn elapsed_ms(&self) -> f64 {
            self.start.elapsed().as_secs_f64() * 1000.0
        }

        pub fn elapsed_duration(&self) -> Duration {
            self.start.elapsed()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[allow(unused_imports)]
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_cassandra5_performance_suite_creation() {
        let suite = Cassandra5PerformanceSuite::new().await;
        assert!(suite.is_ok());
    }

    #[test]
    fn test_prd_targets_default() {
        let targets = PRDTargets::default();
        assert_eq!(targets.parse_speed_mb_per_sec, 100.0);
        assert_eq!(targets.memory_limit_mb, 128.0);
        assert_eq!(targets.throughput_ops_per_sec, 100_000.0);
        assert_eq!(targets.max_file_size_gb, 1.0);
    }

    #[test]
    fn test_benchmark_result_creation() {
        let result = BenchmarkResult {
            benchmark_name: "Test".to_string(),
            file_size_mb: 10.0,
            duration: Duration::from_secs(1),
            throughput_mb_per_sec: 50.0,
            memory_usage_mb: 64.0,
            memory_efficiency: 0.8,
            compression_ratio: Some(0.6),
            operations_per_second: 1000.0,
            meets_prd_target: false,
            target_comparison: "Below target".to_string(),
            details: HashMap::new(),
        };

        assert_eq!(result.benchmark_name, "Test");
        assert!(!result.meets_prd_target);
    }

    #[test]
    fn test_memory_monitor() {
        let mut monitor = utils::MemoryMonitor::new();
        monitor.sample();
        monitor.sample();

        assert!(monitor.peak_usage_mb() >= 0.0);
        assert!(monitor.average_usage_mb() >= 0.0);
    }

    #[test]
    fn test_precision_timer() {
        let timer = utils::PrecisionTimer::start();
        std::thread::sleep(Duration::from_millis(10));

        let elapsed = timer.elapsed_ms();
        assert!(elapsed >= 9.0); // Allow some variance
        assert!(elapsed < 50.0); // Should not be too much longer
    }

    #[test]
    fn test_generate_test_data() {
        let data = utils::generate_test_data(0.1); // 0.1 MB
        let expected_size = (0.1 * 1024.0 * 1024.0) as usize;

        assert_eq!(data.len(), expected_size);
        assert!(data.len() > 0);
    }
}
