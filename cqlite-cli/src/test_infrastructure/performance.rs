//! Performance testing utilities
//!
//! This module provides comprehensive performance testing capabilities,
//! including benchmarking, load testing, and performance regression detection.

use std::time::{Duration, Instant};
// use std::collections::HashMap; // Removed - unused
use super::{CliTestRunner, TestContainer, TestResult};

/// Performance test runner for benchmarking CLI operations
#[derive(Debug)]
pub struct PerformanceTestRunner {
    _container: TestContainer,
    cli_runner: CliTestRunner,
    benchmarks: Vec<PerformanceBenchmark>,
    config: PerformanceConfig,
}

/// Benchmark suite for organized performance testing
#[derive(Debug)]
pub struct BenchmarkSuite {
    name: String,
    benchmarks: Vec<PerformanceBenchmark>,
    _warmup_iterations: usize,
    _measurement_iterations: usize,
}

/// Individual performance benchmark
#[derive(Debug, Clone)]
pub struct PerformanceBenchmark {
    pub name: String,
    pub description: String,
    pub command: String,
    pub args: Vec<String>,
    pub setup_commands: Vec<String>,
    pub expected_max_time: Option<Duration>,
    pub expected_min_throughput: Option<f64>,
    pub input_size: Option<usize>,
    pub tags: Vec<String>,
}

/// Performance testing configuration
#[derive(Debug, Clone)]
pub struct PerformanceConfig {
    pub warmup_iterations: usize,
    pub measurement_iterations: usize,
    pub max_execution_time: Duration,
    pub memory_profiling: bool,
    pub cpu_profiling: bool,
    pub parallel_execution: bool,
    pub output_detailed_stats: bool,
}

/// Performance test results
#[derive(Debug)]
pub struct PerformanceResult {
    pub benchmark_name: String,
    pub success: bool,
    pub statistics: PerformanceStatistics,
    pub resource_usage: ResourceUsage,
    pub error_message: Option<String>,
}

/// Statistical performance metrics
#[derive(Debug, Clone)]
pub struct PerformanceStatistics {
    pub mean_time: Duration,
    pub median_time: Duration,
    pub min_time: Duration,
    pub max_time: Duration,
    pub std_deviation: Duration,
    pub throughput_ops_per_sec: f64,
    pub percentile_95: Duration,
    pub percentile_99: Duration,
}

/// Resource usage metrics
#[derive(Debug, Clone)]
pub struct ResourceUsage {
    pub peak_memory_mb: f64,
    pub avg_memory_mb: f64,
    pub cpu_usage_percent: f64,
    pub disk_read_bytes: u64,
    pub disk_write_bytes: u64,
    pub network_bytes: u64,
}

/// Load testing configuration
#[derive(Debug)]
pub struct LoadTestConfig {
    pub concurrent_users: usize,
    pub test_duration: Duration,
    pub ramp_up_time: Duration,
    pub operations_per_second: Option<f64>,
    pub think_time: Duration,
}

/// Load test results
#[derive(Debug)]
pub struct LoadTestResult {
    pub total_operations: usize,
    pub successful_operations: usize,
    pub failed_operations: usize,
    pub average_response_time: Duration,
    pub throughput_ops_per_sec: f64,
    pub error_rate_percent: f64,
    pub resource_usage: ResourceUsage,
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            warmup_iterations: 3,
            measurement_iterations: 10,
            max_execution_time: Duration::from_secs(300),
            memory_profiling: true,
            cpu_profiling: false,
            parallel_execution: false,
            output_detailed_stats: true,
        }
    }
}

impl PerformanceTestRunner {
    /// Create a new performance test runner
    pub async fn new() -> TestResult<Self> {
        let container = TestContainer::new()?;
        let cli_runner = CliTestRunner::new(container.clone());

        Ok(Self {
            _container: container,
            cli_runner,
            benchmarks: Vec::new(),
            config: PerformanceConfig::default(),
        })
    }

    /// Configure performance testing parameters
    pub fn with_config(mut self, config: PerformanceConfig) -> Self {
        self.config = config;
        self
    }

    /// Add a performance benchmark
    pub fn add_benchmark(mut self, benchmark: PerformanceBenchmark) -> Self {
        self.benchmarks.push(benchmark);
        self
    }

    /// Add multiple benchmarks
    pub fn add_benchmarks(mut self, benchmarks: Vec<PerformanceBenchmark>) -> Self {
        self.benchmarks.extend(benchmarks);
        self
    }

    /// Run all performance benchmarks
    pub async fn run_all_benchmarks(&self) -> TestResult<Vec<PerformanceResult>> {
        let mut results = Vec::new();

        println!("🚀 Starting performance benchmarks...");
        println!(
            "Configuration: {} warmup, {} measurement iterations",
            self.config.warmup_iterations, self.config.measurement_iterations
        );

        for benchmark in &self.benchmarks {
            let result = self.run_benchmark(benchmark).await?;
            results.push(result);
        }

        self.print_summary(&results);
        Ok(results)
    }

    /// Run a single benchmark
    pub async fn run_benchmark(
        &self,
        benchmark: &PerformanceBenchmark,
    ) -> TestResult<PerformanceResult> {
        println!("📊 Running benchmark: {}", benchmark.name);

        // Execute setup commands
        for setup_cmd in &benchmark.setup_commands {
            let args: Vec<&str> = setup_cmd.split_whitespace().collect();
            if !args.is_empty() {
                self.cli_runner.run(&args)?;
            }
        }

        // Warmup runs
        for i in 0..self.config.warmup_iterations {
            println!("  🔥 Warmup {}/{}", i + 1, self.config.warmup_iterations);
            self.execute_benchmark_command(benchmark).await?;
        }

        // Measurement runs
        let mut execution_times = Vec::new();
        let mut resource_measurements = Vec::new();

        for i in 0..self.config.measurement_iterations {
            println!(
                "  📏 Measurement {}/{}",
                i + 1,
                self.config.measurement_iterations
            );

            let (execution_time, resource_usage) =
                self.measure_benchmark_execution(benchmark).await?;
            execution_times.push(execution_time);
            resource_measurements.push(resource_usage);
        }

        // Calculate statistics
        let statistics = self.calculate_statistics(&execution_times, benchmark.input_size);
        let avg_resource_usage = self.average_resource_usage(&resource_measurements);

        // Validate against expectations
        let success = self.validate_performance_expectations(benchmark, &statistics);
        let error_message = if !success {
            Some("Performance expectations not met".to_string())
        } else {
            None
        };

        let result = PerformanceResult {
            benchmark_name: benchmark.name.clone(),
            success,
            statistics,
            resource_usage: avg_resource_usage,
            error_message,
        };

        self.print_benchmark_result(&result);
        Ok(result)
    }

    /// Execute benchmark command and measure time
    async fn execute_benchmark_command(
        &self,
        benchmark: &PerformanceBenchmark,
    ) -> TestResult<Duration> {
        let start_time = Instant::now();

        let mut args = vec![benchmark.command.as_str()];
        args.extend(benchmark.args.iter().map(|s| s.as_str()));

        self.cli_runner.run(&args)?;

        Ok(start_time.elapsed())
    }

    /// Measure benchmark execution with resource monitoring
    async fn measure_benchmark_execution(
        &self,
        benchmark: &PerformanceBenchmark,
    ) -> TestResult<(Duration, ResourceUsage)> {
        let start_time = Instant::now();
        let start_memory = self.get_memory_usage()?;

        let mut args = vec![benchmark.command.as_str()];
        args.extend(benchmark.args.iter().map(|s| s.as_str()));

        self.cli_runner.run(&args)?;

        let execution_time = start_time.elapsed();
        let end_memory = self.get_memory_usage()?;

        let resource_usage = ResourceUsage {
            peak_memory_mb: end_memory.max(start_memory),
            avg_memory_mb: (start_memory + end_memory) / 2.0,
            cpu_usage_percent: 0.0, // Placeholder - would need actual CPU monitoring
            disk_read_bytes: 0,
            disk_write_bytes: 0,
            network_bytes: 0,
        };

        Ok((execution_time, resource_usage))
    }

    /// Get current memory usage (simplified implementation)
    fn get_memory_usage(&self) -> TestResult<f64> {
        // This is a placeholder implementation
        // In a real scenario, you'd use system APIs or external tools
        // to get actual memory usage
        Ok(64.0) // Placeholder: 64MB
    }

    /// Calculate performance statistics
    fn calculate_statistics(
        &self,
        times: &[Duration],
        _input_size: Option<usize>,
    ) -> PerformanceStatistics {
        let mut sorted_times = times.to_vec();
        sorted_times.sort();

        let mean_time = Duration::from_nanos(
            (times.iter().map(|d| d.as_nanos()).sum::<u128>() / times.len() as u128) as u64,
        );

        let median_time = sorted_times[times.len() / 2];
        let min_time = *sorted_times.first().unwrap_or(&Duration::from_secs(0));
        let max_time = *sorted_times.last().unwrap_or(&Duration::from_secs(0));

        // Calculate standard deviation
        let variance: f64 = times
            .iter()
            .map(|d| {
                let diff = d.as_secs_f64() - mean_time.as_secs_f64();
                diff * diff
            })
            .sum::<f64>()
            / times.len() as f64;

        let std_deviation = Duration::from_secs_f64(variance.sqrt());

        // Calculate throughput
        let throughput_ops_per_sec = if mean_time.as_secs_f64() > 0.0 {
            1.0 / mean_time.as_secs_f64()
        } else {
            0.0
        };

        // Calculate percentiles
        let percentile_95_idx = (times.len() as f64 * 0.95) as usize;
        let percentile_99_idx = (times.len() as f64 * 0.99) as usize;

        let percentile_95 = sorted_times
            .get(percentile_95_idx)
            .copied()
            .unwrap_or(max_time);
        let percentile_99 = sorted_times
            .get(percentile_99_idx)
            .copied()
            .unwrap_or(max_time);

        PerformanceStatistics {
            mean_time,
            median_time,
            min_time,
            max_time,
            std_deviation,
            throughput_ops_per_sec,
            percentile_95,
            percentile_99,
        }
    }

    /// Average resource usage across measurements
    fn average_resource_usage(&self, measurements: &[ResourceUsage]) -> ResourceUsage {
        if measurements.is_empty() {
            return ResourceUsage {
                peak_memory_mb: 0.0,
                avg_memory_mb: 0.0,
                cpu_usage_percent: 0.0,
                disk_read_bytes: 0,
                disk_write_bytes: 0,
                network_bytes: 0,
            };
        }

        let count = measurements.len() as f64;

        ResourceUsage {
            peak_memory_mb: measurements
                .iter()
                .map(|r| r.peak_memory_mb)
                .fold(0.0, f64::max),
            avg_memory_mb: measurements.iter().map(|r| r.avg_memory_mb).sum::<f64>() / count,
            cpu_usage_percent: measurements
                .iter()
                .map(|r| r.cpu_usage_percent)
                .sum::<f64>()
                / count,
            disk_read_bytes: measurements.iter().map(|r| r.disk_read_bytes).sum::<u64>()
                / measurements.len() as u64,
            disk_write_bytes: measurements.iter().map(|r| r.disk_write_bytes).sum::<u64>()
                / measurements.len() as u64,
            network_bytes: measurements.iter().map(|r| r.network_bytes).sum::<u64>()
                / measurements.len() as u64,
        }
    }

    /// Validate performance against expectations
    fn validate_performance_expectations(
        &self,
        benchmark: &PerformanceBenchmark,
        stats: &PerformanceStatistics,
    ) -> bool {
        let mut valid = true;

        if let Some(max_time) = benchmark.expected_max_time {
            if stats.mean_time > max_time {
                println!(
                    "  ⚠️  Expected max time {:?}, got {:?}",
                    max_time, stats.mean_time
                );
                valid = false;
            }
        }

        if let Some(min_throughput) = benchmark.expected_min_throughput {
            if stats.throughput_ops_per_sec < min_throughput {
                println!(
                    "  ⚠️  Expected min throughput {:.2} ops/sec, got {:.2}",
                    min_throughput, stats.throughput_ops_per_sec
                );
                valid = false;
            }
        }

        valid
    }

    /// Print individual benchmark result
    fn print_benchmark_result(&self, result: &PerformanceResult) {
        let status = if result.success { "✅" } else { "❌" };
        println!("  {} {}", status, result.benchmark_name);

        if self.config.output_detailed_stats {
            println!("    Mean: {:?}", result.statistics.mean_time);
            println!("    Median: {:?}", result.statistics.median_time);
            println!(
                "    Min/Max: {:?} / {:?}",
                result.statistics.min_time, result.statistics.max_time
            );
            println!(
                "    Throughput: {:.2} ops/sec",
                result.statistics.throughput_ops_per_sec
            );
            println!(
                "    Memory: {:.1} MB peak",
                result.resource_usage.peak_memory_mb
            );
        }
    }

    /// Print performance summary
    fn print_summary(&self, results: &[PerformanceResult]) {
        let total_benchmarks = results.len();
        let passed_benchmarks = results.iter().filter(|r| r.success).count();
        let failed_benchmarks = total_benchmarks - passed_benchmarks;

        println!("\n📊 Performance Test Summary");
        println!("===========================");
        println!("Total Benchmarks: {}", total_benchmarks);
        println!(
            "Passed: {} ({}%)",
            passed_benchmarks,
            (passed_benchmarks * 100) / total_benchmarks.max(1)
        );
        println!("Failed: {}", failed_benchmarks);

        if failed_benchmarks > 0 {
            println!("\n❌ Failed Benchmarks:");
            for result in results {
                if !result.success {
                    println!(
                        "  - {}: {}",
                        result.benchmark_name,
                        result
                            .error_message
                            .as_deref()
                            .unwrap_or("Performance expectations not met")
                    );
                }
            }
        }

        // Overall performance stats
        if !results.is_empty() {
            let avg_time = Duration::from_nanos(
                results
                    .iter()
                    .map(|r| r.statistics.mean_time.as_nanos())
                    .sum::<u128>() as u64
                    / results.len() as u64,
            );

            let total_throughput: f64 = results
                .iter()
                .map(|r| r.statistics.throughput_ops_per_sec)
                .sum();

            println!("\n📈 Overall Performance:");
            println!("  Average execution time: {:?}", avg_time);
            println!("  Total throughput: {:.2} ops/sec", total_throughput);
        }
    }

    /// Run load testing
    pub async fn run_load_test(&self, config: LoadTestConfig) -> TestResult<LoadTestResult> {
        println!(
            "🔄 Starting load test with {} concurrent users",
            config.concurrent_users
        );

        let start_time = Instant::now();
        let mut total_operations = 0;
        let mut successful_operations = 0;
        let mut failed_operations = 0;
        let mut response_times = Vec::new();

        // Simplified load test implementation
        // In a real implementation, you'd use tokio tasks and proper concurrency
        while start_time.elapsed() < config.test_duration {
            for benchmark in &self.benchmarks {
                let _operation_start = Instant::now();

                match self.execute_benchmark_command(benchmark).await {
                    Ok(execution_time) => {
                        successful_operations += 1;
                        response_times.push(execution_time);
                    }
                    Err(_) => {
                        failed_operations += 1;
                    }
                }

                total_operations += 1;

                // Apply think time
                if config.think_time > Duration::from_secs(0) {
                    tokio::time::sleep(config.think_time).await;
                }
            }
        }

        let total_time = start_time.elapsed();
        let average_response_time = if !response_times.is_empty() {
            Duration::from_nanos(
                response_times.iter().map(|d| d.as_nanos()).sum::<u128>() as u64
                    / response_times.len() as u64,
            )
        } else {
            Duration::from_secs(0)
        };

        let throughput_ops_per_sec = total_operations as f64 / total_time.as_secs_f64();
        let error_rate_percent = (failed_operations as f64 / total_operations as f64) * 100.0;

        let result = LoadTestResult {
            total_operations,
            successful_operations,
            failed_operations,
            average_response_time,
            throughput_ops_per_sec,
            error_rate_percent,
            resource_usage: ResourceUsage {
                peak_memory_mb: self.get_memory_usage()?,
                avg_memory_mb: self.get_memory_usage()?,
                cpu_usage_percent: 0.0,
                disk_read_bytes: 0,
                disk_write_bytes: 0,
                network_bytes: 0,
            },
        };

        println!("📊 Load Test Results:");
        println!("  Total Operations: {}", result.total_operations);
        println!("  Success Rate: {:.1}%", 100.0 - result.error_rate_percent);
        println!("  Throughput: {:.2} ops/sec", result.throughput_ops_per_sec);
        println!(
            "  Average Response Time: {:?}",
            result.average_response_time
        );

        Ok(result)
    }
}

impl BenchmarkSuite {
    /// Create a new benchmark suite
    pub fn new<S: Into<String>>(name: S) -> Self {
        Self {
            name: name.into(),
            benchmarks: Vec::new(),
            _warmup_iterations: 3,
            _measurement_iterations: 10,
        }
    }

    /// Add benchmark to suite
    pub fn add_benchmark(mut self, benchmark: PerformanceBenchmark) -> Self {
        self.benchmarks.push(benchmark);
        self
    }

    /// Create common CLI benchmarks
    pub fn create_cli_benchmarks() -> Self {
        Self::new("CLI Operations")
            .add_benchmark(PerformanceBenchmark {
                name: "help_command".to_string(),
                description: "Benchmark help command execution".to_string(),
                command: "--help".to_string(),
                args: vec![],
                setup_commands: vec![],
                expected_max_time: Some(Duration::from_millis(100)),
                expected_min_throughput: Some(10.0),
                input_size: None,
                tags: vec!["cli".to_string(), "basic".to_string()],
            })
            .add_benchmark(PerformanceBenchmark {
                name: "version_command".to_string(),
                description: "Benchmark version command execution".to_string(),
                command: "--version".to_string(),
                args: vec![],
                setup_commands: vec![],
                expected_max_time: Some(Duration::from_millis(50)),
                expected_min_throughput: Some(20.0),
                input_size: None,
                tags: vec!["cli".to_string(), "basic".to_string()],
            })
    }

    /// Create database operation benchmarks
    pub fn create_database_benchmarks() -> Self {
        Self::new("Database Operations").add_benchmark(PerformanceBenchmark {
            name: "simple_query".to_string(),
            description: "Benchmark simple SELECT query".to_string(),
            command: "query".to_string(),
            args: vec!["SELECT * FROM system.local".to_string()],
            setup_commands: vec![],
            expected_max_time: Some(Duration::from_secs(1)),
            expected_min_throughput: Some(1.0),
            input_size: Some(1),
            tags: vec!["database".to_string(), "query".to_string()],
        })
    }
}

impl PerformanceBenchmark {
    /// Create a simple benchmark
    pub fn simple<S: Into<String>>(name: S, command: S) -> Self {
        let name_string = name.into();
        Self {
            name: name_string.clone(),
            description: format!("Benchmark: {}", name_string),
            command: command.into(),
            args: vec![],
            setup_commands: vec![],
            expected_max_time: None,
            expected_min_throughput: None,
            input_size: None,
            tags: vec![],
        }
    }

    /// Set performance expectations
    pub fn with_expectations(mut self, max_time: Duration, min_throughput: f64) -> Self {
        self.expected_max_time = Some(max_time);
        self.expected_min_throughput = Some(min_throughput);
        self
    }

    /// Add setup command
    pub fn with_setup<S: Into<String>>(mut self, setup_command: S) -> Self {
        self.setup_commands.push(setup_command.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_performance_runner_creation() {
        let runner = PerformanceTestRunner::new().await.unwrap();
        assert_eq!(runner.benchmarks.len(), 0);
        assert_eq!(runner.config.warmup_iterations, 3);
    }

    #[test]
    fn test_benchmark_suite_creation() {
        let suite = BenchmarkSuite::create_cli_benchmarks();
        assert_eq!(suite.name, "CLI Operations");
        assert!(suite.benchmarks.len() > 0);
    }

    #[test]
    fn test_performance_benchmark_creation() {
        let benchmark = PerformanceBenchmark::simple("test", "--help")
            .with_expectations(Duration::from_millis(100), 10.0)
            .with_setup("echo setup");

        assert_eq!(benchmark.name, "test");
        assert_eq!(benchmark.command, "--help");
        assert_eq!(
            benchmark.expected_max_time,
            Some(Duration::from_millis(100))
        );
        assert_eq!(benchmark.setup_commands.len(), 1);
    }

    #[test]
    fn test_statistics_calculation() {
        let runner = PerformanceTestRunner {
            _container: TestContainer::new().unwrap(),
            cli_runner: CliTestRunner::new(TestContainer::new().unwrap()),
            benchmarks: vec![],
            config: PerformanceConfig::default(),
        };

        let times = vec![
            Duration::from_millis(100),
            Duration::from_millis(150),
            Duration::from_millis(120),
            Duration::from_millis(130),
            Duration::from_millis(110),
        ];

        let stats = runner.calculate_statistics(&times, None);
        assert!(stats.mean_time > Duration::from_millis(100));
        assert!(stats.mean_time < Duration::from_millis(150));
        assert!(stats.throughput_ops_per_sec > 0.0);
    }
}
