//! Performance Regression Testing Framework
//!
//! This module provides comprehensive performance regression detection and tracking
//! to ensure CQLite maintains its performance targets over time and across releases.

use cqlite_core::error::Result;
use cqlite_core::platform::Platform;
use cqlite_core::storage::StorageEngine;
use cqlite_core::{types::TableId, Config, RowKey, Value};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::fs;

/// Performance baseline for regression detection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceBaseline {
    /// Version when baseline was recorded
    pub version: String,
    /// Timestamp when baseline was created
    pub timestamp: String,
    /// Environment information
    pub environment: EnvironmentInfo,
    /// Performance metrics by operation type
    pub metrics: HashMap<String, OperationMetrics>,
    /// Hardware configuration
    pub hardware: HardwareInfo,
}

/// Environment information for context
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentInfo {
    /// Operating system
    pub os: String,
    /// CPU architecture
    pub arch: String,
    /// Rust version used
    pub rust_version: String,
    /// Debug or release build
    pub build_mode: String,
    /// Compiler optimizations enabled
    pub optimizations: Vec<String>,
}

/// Hardware configuration information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HardwareInfo {
    /// CPU model and frequency
    pub cpu_model: String,
    /// Number of CPU cores
    pub cpu_cores: usize,
    /// Total RAM in MB
    pub ram_mb: u64,
    /// Storage type (SSD, HDD, etc.)
    pub storage_type: String,
    /// Available disk space in MB
    pub disk_space_mb: u64,
}

/// Performance metrics for a specific operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationMetrics {
    /// Operation name/description
    pub operation: String,
    /// Average latency in microseconds
    pub avg_latency_us: f64,
    /// 95th percentile latency in microseconds
    pub p95_latency_us: f64,
    /// 99th percentile latency in microseconds
    pub p99_latency_us: f64,
    /// Throughput in operations per second
    pub throughput_ops_sec: f64,
    /// Memory usage in bytes
    pub memory_usage_bytes: u64,
    /// CPU utilization percentage
    pub cpu_utilization_percent: f64,
    /// Number of iterations run
    pub iterations: usize,
    /// Standard deviation of latency
    pub latency_std_dev: f64,
}

/// Regression test result
#[derive(Debug, Clone)]
pub struct RegressionTestResult {
    /// Operation being tested
    pub operation: String,
    /// Current performance metrics
    pub current_metrics: OperationMetrics,
    /// Baseline metrics for comparison
    pub baseline_metrics: Option<OperationMetrics>,
    /// Performance change percentage (positive = improvement, negative = regression)
    pub latency_change_percent: f64,
    /// Throughput change percentage
    pub throughput_change_percent: f64,
    /// Memory usage change percentage
    pub memory_change_percent: f64,
    /// Whether this represents a significant regression
    pub is_regression: bool,
    /// Detailed analysis of the change
    pub analysis: String,
}

/// Configuration for regression testing
#[derive(Debug, Clone)]
pub struct RegressionTestConfig {
    /// Baseline file path
    pub baseline_path: PathBuf,
    /// Regression threshold percentages
    pub regression_thresholds: RegressionThresholds,
    /// Number of iterations for each test
    pub test_iterations: usize,
    /// Warmup iterations before measurement
    pub warmup_iterations: usize,
    /// Test data size multipliers
    pub data_size_multipliers: Vec<usize>,
    /// Enable detailed profiling
    pub enable_profiling: bool,
}

/// Thresholds for detecting regressions
#[derive(Debug, Clone)]
pub struct RegressionThresholds {
    /// Maximum acceptable latency increase (%)
    pub max_latency_regression_percent: f64,
    /// Maximum acceptable throughput decrease (%)
    pub max_throughput_regression_percent: f64,
    /// Maximum acceptable memory increase (%)
    pub max_memory_regression_percent: f64,
    /// Minimum change to consider significant (%)
    pub significance_threshold_percent: f64,
}

impl Default for RegressionTestConfig {
    fn default() -> Self {
        Self {
            baseline_path: PathBuf::from("performance_baseline.json"),
            regression_thresholds: RegressionThresholds {
                max_latency_regression_percent: 10.0,
                max_throughput_regression_percent: 5.0,
                max_memory_regression_percent: 15.0,
                significance_threshold_percent: 2.0,
            },
            test_iterations: 1000,
            warmup_iterations: 100,
            data_size_multipliers: vec![1, 10, 100],
            enable_profiling: true,
        }
    }
}

/// Main performance regression testing framework
pub struct PerformanceRegressionFramework {
    /// Configuration
    config: RegressionTestConfig,
    /// Current baseline
    baseline: Option<PerformanceBaseline>,
    /// Temporary directory for tests
    temp_dir: TempDir,
    /// Storage engine for testing
    storage_engine: Arc<StorageEngine>,
    /// Platform abstraction
    platform: Arc<Platform>,
}

impl PerformanceRegressionFramework {
    /// Create a new regression testing framework
    pub async fn new(config: RegressionTestConfig) -> Result<Self> {
        let temp_dir = TempDir::new().map_err(|e| {
            cqlite_core::error::Error::io_error(format!("Failed to create temp dir: {}", e))
        })?;

        let cqlite_config = Config::performance_optimized();
        let platform = Arc::new(Platform::new(&cqlite_config).await?);
        let storage_engine =
            Arc::new(StorageEngine::open(temp_dir.path(), &cqlite_config, platform.clone()).await?);

        // Load existing baseline if available
        let baseline = Self::load_baseline(&config.baseline_path).await.ok();

        Ok(Self {
            config,
            baseline,
            temp_dir,
            storage_engine,
            platform,
        })
    }

    /// Run comprehensive regression tests
    pub async fn run_regression_tests(&mut self) -> Result<Vec<RegressionTestResult>> {
        println!("🔄 Running Performance Regression Tests");
        println!("{}", "=".repeat(60));

        let mut results = Vec::new();

        // Test various operations with different data sizes
        for &size_multiplier in &self.config.data_size_multipliers {
            println!("📊 Testing with {}x data size", size_multiplier);

            // Test write operations
            let write_result = self.test_write_performance(size_multiplier).await?;
            results.push(write_result);

            // Test read operations
            let read_result = self.test_read_performance(size_multiplier).await?;
            results.push(read_result);

            // Test scan operations
            let scan_result = self.test_scan_performance(size_multiplier).await?;
            results.push(scan_result);

            // Test concurrent operations
            let concurrent_result = self.test_concurrent_performance(size_multiplier).await?;
            results.push(concurrent_result);

            println!("  ✅ Completed tests for {}x data size", size_multiplier);
        }

        // Analyze results and determine if there are significant regressions
        let regression_count = results.iter().filter(|r| r.is_regression).count();

        if regression_count > 0 {
            println!("⚠️  {} performance regressions detected!", regression_count);
        } else {
            println!("✅ No significant performance regressions detected");
        }

        Ok(results)
    }

    /// Create a new performance baseline
    pub async fn create_baseline(&mut self, version: String) -> Result<PerformanceBaseline> {
        println!(
            "📊 Creating new performance baseline for version {}",
            version
        );

        let mut metrics = HashMap::new();

        // Benchmark all operations to create baseline
        for &size_multiplier in &self.config.data_size_multipliers {
            // Write performance
            let write_metrics = self.benchmark_write_operation(size_multiplier).await?;
            metrics.insert(format!("write_{}x", size_multiplier), write_metrics);

            // Read performance
            let read_metrics = self.benchmark_read_operation(size_multiplier).await?;
            metrics.insert(format!("read_{}x", size_multiplier), read_metrics);

            // Scan performance
            let scan_metrics = self.benchmark_scan_operation(size_multiplier).await?;
            metrics.insert(format!("scan_{}x", size_multiplier), scan_metrics);

            // Concurrent performance
            let concurrent_metrics = self.benchmark_concurrent_operation(size_multiplier).await?;
            metrics.insert(
                format!("concurrent_{}x", size_multiplier),
                concurrent_metrics,
            );
        }

        let baseline = PerformanceBaseline {
            version,
            timestamp: chrono::Utc::now().to_rfc3339(),
            environment: self.get_environment_info(),
            metrics,
            hardware: self.get_hardware_info(),
        };

        // Save baseline to file
        self.save_baseline(&baseline).await?;
        self.baseline = Some(baseline.clone());

        println!("✅ Performance baseline created and saved");
        Ok(baseline)
    }

    /// Test write performance and compare to baseline
    async fn test_write_performance(&self, size_multiplier: usize) -> Result<RegressionTestResult> {
        let operation = format!("write_{}x", size_multiplier);
        let current_metrics = self.benchmark_write_operation(size_multiplier).await?;

        self.create_regression_result(operation, current_metrics)
            .await
    }

    /// Test read performance and compare to baseline
    async fn test_read_performance(&self, size_multiplier: usize) -> Result<RegressionTestResult> {
        let operation = format!("read_{}x", size_multiplier);
        let current_metrics = self.benchmark_read_operation(size_multiplier).await?;

        self.create_regression_result(operation, current_metrics)
            .await
    }

    /// Test scan performance and compare to baseline
    async fn test_scan_performance(&self, size_multiplier: usize) -> Result<RegressionTestResult> {
        let operation = format!("scan_{}x", size_multiplier);
        let current_metrics = self.benchmark_scan_operation(size_multiplier).await?;

        self.create_regression_result(operation, current_metrics)
            .await
    }

    /// Test concurrent performance and compare to baseline
    async fn test_concurrent_performance(
        &self,
        size_multiplier: usize,
    ) -> Result<RegressionTestResult> {
        let operation = format!("concurrent_{}x", size_multiplier);
        let current_metrics = self.benchmark_concurrent_operation(size_multiplier).await?;

        self.create_regression_result(operation, current_metrics)
            .await
    }

    /// Benchmark write operations
    async fn benchmark_write_operation(&self, size_multiplier: usize) -> Result<OperationMetrics> {
        let table_id = TableId::new("regression_write_test");
        let iterations = self.config.test_iterations * size_multiplier;

        // Warmup
        for i in 0..self.config.warmup_iterations {
            let key = RowKey::from(format!("warmup_key_{}", i));
            let value = Value::Text(format!("warmup_value_{}", i));
            self.storage_engine.put(&table_id, key, value).await?;
        }

        // Actual benchmark
        let mut latencies = Vec::new();
        let memory_before = self.get_memory_usage().await?;
        let start_time = Instant::now();

        for i in 0..iterations {
            let key = RowKey::from(format!("bench_key_{:08}", i));
            let value = Value::Text(format!(
                "benchmark_value_{}_with_substantial_content_for_realistic_testing_scenarios_{}",
                i,
                "x".repeat(100)
            ));

            let op_start = Instant::now();
            self.storage_engine.put(&table_id, key, value).await?;
            let op_latency = op_start.elapsed().as_micros() as f64;
            latencies.push(op_latency);

            if i % 1000 == 0 && i > 0 {
                println!("    📝 Completed {} write operations", i);
            }
        }

        let total_time = start_time.elapsed();
        let memory_after = self.get_memory_usage().await?;

        self.calculate_operation_metrics(
            "write_operation".to_string(),
            latencies,
            total_time,
            memory_after.saturating_sub(memory_before),
            iterations,
        )
    }

    /// Benchmark read operations
    async fn benchmark_read_operation(&self, size_multiplier: usize) -> Result<OperationMetrics> {
        let table_id = TableId::new("regression_read_test");
        let iterations = self.config.test_iterations * size_multiplier;

        // Pre-populate data
        for i in 0..iterations {
            let key = RowKey::from(format!("read_key_{:08}", i));
            let value = Value::Text(format!("read_value_{}", i));
            self.storage_engine.put(&table_id, key, value).await?;
        }
        self.storage_engine.flush().await?;

        // Warmup reads
        for i in 0..self.config.warmup_iterations {
            let key = RowKey::from(format!("read_key_{:08}", i % iterations));
            let _ = self.storage_engine.get(&table_id, &key).await?;
        }

        // Actual benchmark
        let mut latencies = Vec::new();
        let memory_before = self.get_memory_usage().await?;
        let start_time = Instant::now();

        for i in 0..iterations {
            let key = RowKey::from(format!("read_key_{:08}", i));

            let op_start = Instant::now();
            let _ = self.storage_engine.get(&table_id, &key).await?;
            let op_latency = op_start.elapsed().as_micros() as f64;
            latencies.push(op_latency);

            if i % 1000 == 0 && i > 0 {
                println!("    🔍 Completed {} read operations", i);
            }
        }

        let total_time = start_time.elapsed();
        let memory_after = self.get_memory_usage().await?;

        self.calculate_operation_metrics(
            "read_operation".to_string(),
            latencies,
            total_time,
            memory_after.saturating_sub(memory_before),
            iterations,
        )
    }

    /// Benchmark scan operations
    async fn benchmark_scan_operation(&self, size_multiplier: usize) -> Result<OperationMetrics> {
        let table_id = TableId::new("regression_scan_test");
        let base_data_size = 10_000;
        let data_size = base_data_size * size_multiplier;

        // Pre-populate data
        for i in 0..data_size {
            let key = RowKey::from(format!("scan_key_{:08}", i));
            let value = Value::Text(format!("scan_value_{}", i));
            self.storage_engine.put(&table_id, key, value).await?;
        }
        self.storage_engine.flush().await?;

        // Benchmark range scans
        let scan_iterations = (self.config.test_iterations / 10).max(10); // Fewer iterations for expensive operations
        let mut latencies = Vec::new();
        let memory_before = self.get_memory_usage().await?;
        let start_time = Instant::now();

        for i in 0..scan_iterations {
            let start_idx = (i * 1000) % (data_size - 1000);
            let end_idx = start_idx + 1000;

            let start_key = RowKey::from(format!("scan_key_{:08}", start_idx));
            let end_key = RowKey::from(format!("scan_key_{:08}", end_idx));

            let op_start = Instant::now();
            let _results = self
                .storage_engine
                .scan(&table_id, Some(&start_key), Some(&end_key), Some(1000))
                .await?;
            let op_latency = op_start.elapsed().as_micros() as f64;
            latencies.push(op_latency);

            if i % 10 == 0 && i > 0 {
                println!("    📈 Completed {} scan operations", i);
            }
        }

        let total_time = start_time.elapsed();
        let memory_after = self.get_memory_usage().await?;

        self.calculate_operation_metrics(
            "scan_operation".to_string(),
            latencies,
            total_time,
            memory_after.saturating_sub(memory_before),
            scan_iterations,
        )
    }

    /// Benchmark concurrent operations
    async fn benchmark_concurrent_operation(
        &self,
        size_multiplier: usize,
    ) -> Result<OperationMetrics> {
        let table_id = TableId::new("regression_concurrent_test");
        let thread_count = 4;
        let ops_per_thread = (self.config.test_iterations * size_multiplier) / thread_count;

        let memory_before = self.get_memory_usage().await?;
        let start_time = Instant::now();

        let mut handles = Vec::new();
        for thread_id in 0..thread_count {
            let storage = self.storage_engine.clone();
            let table_id = table_id.clone();

            let handle = tokio::spawn(async move {
                let mut latencies = Vec::new();

                for i in 0..ops_per_thread {
                    let key = RowKey::from(format!("concurrent_{}_{:06}", thread_id, i));
                    let value = Value::Text(format!("concurrent_value_{}_{}", thread_id, i));

                    let op_start = Instant::now();
                    if storage.put(&table_id, key, value).await.is_ok() {
                        let op_latency = op_start.elapsed().as_micros() as f64;
                        latencies.push(op_latency);
                    }
                }

                latencies
            });

            handles.push(handle);
        }

        // Collect all latencies
        let mut all_latencies = Vec::new();
        for handle in handles {
            if let Ok(latencies) = handle.await {
                all_latencies.extend(latencies);
            }
        }

        let total_time = start_time.elapsed();
        let memory_after = self.get_memory_usage().await?;

        println!(
            "    🚀 Completed {} concurrent operations",
            all_latencies.len()
        );

        self.calculate_operation_metrics(
            "concurrent_operation".to_string(),
            all_latencies,
            total_time,
            memory_after.saturating_sub(memory_before),
            thread_count * ops_per_thread,
        )
    }

    /// Calculate operation metrics from raw measurements
    fn calculate_operation_metrics(
        &self,
        operation: String,
        mut latencies: Vec<f64>,
        total_time: Duration,
        memory_usage: u64,
        iterations: usize,
    ) -> Result<OperationMetrics> {
        if latencies.is_empty() {
            return Err(cqlite_core::error::Error::io_error(
                "No latency measurements available".to_string(),
            ));
        }

        latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let avg_latency_us = latencies.iter().sum::<f64>() / latencies.len() as f64;
        let p95_latency_us = latencies[(latencies.len() * 95 / 100).min(latencies.len() - 1)];
        let p99_latency_us = latencies[(latencies.len() * 99 / 100).min(latencies.len() - 1)];

        // Calculate standard deviation
        let variance = latencies
            .iter()
            .map(|&x| (x - avg_latency_us).powi(2))
            .sum::<f64>()
            / latencies.len() as f64;
        let latency_std_dev = variance.sqrt();

        let throughput_ops_sec = iterations as f64 / total_time.as_secs_f64();
        let cpu_utilization_percent = 50.0; // Placeholder - would use actual CPU monitoring

        Ok(OperationMetrics {
            operation,
            avg_latency_us,
            p95_latency_us,
            p99_latency_us,
            throughput_ops_sec,
            memory_usage_bytes: memory_usage,
            cpu_utilization_percent,
            iterations,
            latency_std_dev,
        })
    }

    /// Create regression test result by comparing current metrics to baseline
    async fn create_regression_result(
        &self,
        operation: String,
        current_metrics: OperationMetrics,
    ) -> Result<RegressionTestResult> {
        let baseline_metrics = self
            .baseline
            .as_ref()
            .and_then(|b| b.metrics.get(&operation))
            .cloned();

        let (
            latency_change_percent,
            throughput_change_percent,
            memory_change_percent,
            is_regression,
            analysis,
        ) = if let Some(ref baseline) = baseline_metrics {
            let latency_change = ((current_metrics.avg_latency_us - baseline.avg_latency_us)
                / baseline.avg_latency_us)
                * 100.0;
            let throughput_change = ((current_metrics.throughput_ops_sec
                - baseline.throughput_ops_sec)
                / baseline.throughput_ops_sec)
                * 100.0;
            let memory_change = ((current_metrics.memory_usage_bytes as f64
                - baseline.memory_usage_bytes as f64)
                / baseline.memory_usage_bytes as f64)
                * 100.0;

            let is_regression = latency_change
                > self
                    .config
                    .regression_thresholds
                    .max_latency_regression_percent
                || throughput_change
                    < -self
                        .config
                        .regression_thresholds
                        .max_throughput_regression_percent
                || memory_change
                    > self
                        .config
                        .regression_thresholds
                        .max_memory_regression_percent;

            let analysis = if is_regression {
                let mut issues = Vec::new();
                if latency_change
                    > self
                        .config
                        .regression_thresholds
                        .max_latency_regression_percent
                {
                    issues.push(format!("Latency increased by {:.1}%", latency_change));
                }
                if throughput_change
                    < -self
                        .config
                        .regression_thresholds
                        .max_throughput_regression_percent
                {
                    issues.push(format!(
                        "Throughput decreased by {:.1}%",
                        -throughput_change
                    ));
                }
                if memory_change
                    > self
                        .config
                        .regression_thresholds
                        .max_memory_regression_percent
                {
                    issues.push(format!("Memory usage increased by {:.1}%", memory_change));
                }
                format!("REGRESSION DETECTED: {}", issues.join(", "))
            } else {
                "Performance within acceptable thresholds".to_string()
            };

            (
                latency_change,
                throughput_change,
                memory_change,
                is_regression,
                analysis,
            )
        } else {
            (
                0.0,
                0.0,
                0.0,
                false,
                "No baseline available for comparison".to_string(),
            )
        };

        Ok(RegressionTestResult {
            operation,
            current_metrics,
            baseline_metrics,
            latency_change_percent,
            throughput_change_percent,
            memory_change_percent,
            is_regression,
            analysis,
        })
    }

    /// Load baseline from file
    async fn load_baseline(path: &Path) -> Result<PerformanceBaseline> {
        let content = fs::read_to_string(path).await.map_err(|e| {
            cqlite_core::error::Error::io_error(format!("Failed to read baseline: {}", e))
        })?;

        serde_json::from_str(&content).map_err(|e| {
            cqlite_core::error::Error::io_error(format!("Failed to parse baseline: {}", e))
        })
    }

    /// Save baseline to file
    async fn save_baseline(&self, baseline: &PerformanceBaseline) -> Result<()> {
        let content = serde_json::to_string_pretty(baseline).map_err(|e| {
            cqlite_core::error::Error::io_error(format!("Failed to serialize baseline: {}", e))
        })?;

        fs::write(&self.config.baseline_path, content)
            .await
            .map_err(|e| {
                cqlite_core::error::Error::io_error(format!("Failed to write baseline: {}", e))
            })
    }

    /// Get current memory usage
    async fn get_memory_usage(&self) -> Result<u64> {
        let stats = self.storage_engine.stats().await?;
        Ok(stats.memtable.size_bytes)
    }

    /// Get environment information
    fn get_environment_info(&self) -> EnvironmentInfo {
        EnvironmentInfo {
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            rust_version: env!("CARGO_PKG_RUST_VERSION").to_string(),
            build_mode: if cfg!(debug_assertions) {
                "debug".to_string()
            } else {
                "release".to_string()
            },
            optimizations: vec![
                "opt-level=3".to_string(),
                "lto=true".to_string(),
                "codegen-units=1".to_string(),
            ],
        }
    }

    /// Get hardware information
    fn get_hardware_info(&self) -> HardwareInfo {
        HardwareInfo {
            cpu_model: "Unknown CPU".to_string(), // Would use system detection
            cpu_cores: num_cpus::get(),
            ram_mb: 8192, // Placeholder - would use system detection
            storage_type: "SSD".to_string(),
            disk_space_mb: 100_000, // Placeholder
        }
    }

    /// Print regression test results
    pub fn print_regression_report(&self, results: &[RegressionTestResult]) {
        println!("\n🎯 PERFORMANCE REGRESSION TEST REPORT");
        println!("{}", "=".repeat(80));

        let regression_count = results.iter().filter(|r| r.is_regression).count();

        if regression_count > 0 {
            println!("❌ {} PERFORMANCE REGRESSIONS DETECTED", regression_count);
        } else {
            println!("✅ NO SIGNIFICANT PERFORMANCE REGRESSIONS");
        }

        println!("\n📊 DETAILED RESULTS:\n");

        for result in results {
            let status = if result.is_regression {
                "❌ REGRESSION"
            } else {
                "✅ PASS"
            };

            println!("🔹 Operation: {}", result.operation);
            println!("   Status: {}", status);
            println!(
                "   Current Latency: {:.2}μs (avg), {:.2}μs (p95), {:.2}μs (p99)",
                result.current_metrics.avg_latency_us,
                result.current_metrics.p95_latency_us,
                result.current_metrics.p99_latency_us
            );
            println!(
                "   Current Throughput: {:.2} ops/sec",
                result.current_metrics.throughput_ops_sec
            );
            println!(
                "   Current Memory: {} bytes",
                result.current_metrics.memory_usage_bytes
            );

            if let Some(ref baseline) = result.baseline_metrics {
                println!(
                    "   Baseline Latency: {:.2}μs (avg), {:.2}μs (p95), {:.2}μs (p99)",
                    baseline.avg_latency_us, baseline.p95_latency_us, baseline.p99_latency_us
                );
                println!(
                    "   Baseline Throughput: {:.2} ops/sec",
                    baseline.throughput_ops_sec
                );
                println!("   Baseline Memory: {} bytes", baseline.memory_usage_bytes);

                println!("   Changes:");
                println!("     Latency: {:+.1}%", result.latency_change_percent);
                println!("     Throughput: {:+.1}%", result.throughput_change_percent);
                println!("     Memory: {:+.1}%", result.memory_change_percent);
            }

            println!("   Analysis: {}", result.analysis);
            println!();
        }

        if regression_count > 0 {
            println!("💡 RECOMMENDATIONS:");
            println!("  • Review recent code changes for performance impact");
            println!("  • Profile hot code paths with perf or similar tools");
            println!("  • Check for memory leaks or increased allocation patterns");
            println!("  • Validate that optimizations are enabled in release builds");
            println!("  • Consider hardware or environment changes");
        }

        println!(
            "\n⏱️  Report generated at {}",
            chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_regression_framework_creation() {
        let config = RegressionTestConfig::default();
        let framework = PerformanceRegressionFramework::new(config).await;
        assert!(framework.is_ok());
    }

    #[test]
    fn test_regression_config() {
        let config = RegressionTestConfig::default();
        assert!(config.test_iterations > 0);
        assert!(config.regression_thresholds.max_latency_regression_percent > 0.0);
        assert!(!config.data_size_multipliers.is_empty());
    }

    #[test]
    fn test_operation_metrics() {
        let metrics = OperationMetrics {
            operation: "test_op".to_string(),
            avg_latency_us: 100.0,
            p95_latency_us: 150.0,
            p99_latency_us: 200.0,
            throughput_ops_sec: 1000.0,
            memory_usage_bytes: 1024,
            cpu_utilization_percent: 50.0,
            iterations: 1000,
            latency_std_dev: 10.0,
        };

        assert_eq!(metrics.operation, "test_op");
        assert_eq!(metrics.avg_latency_us, 100.0);
        assert_eq!(metrics.throughput_ops_sec, 1000.0);
    }
}
