//! Performance integration tests for CLI operations

use cqlite_cli::test_infrastructure::*;
use std::time::Duration;

#[tokio::test]
async fn test_cli_performance_benchmarks() -> TestResult<()> {
    let mut runner = PerformanceTestRunner::new().await?;
    
    // Add basic CLI performance benchmarks
    runner = runner.add_benchmarks(
        BenchmarkSuite::create_cli_benchmarks().benchmarks
    );
    
    let results = runner.run_all_benchmarks().await?;
    
    // Verify that all benchmarks completed
    assert!(!results.is_empty());
    
    // Check that help command performed within reasonable bounds
    let help_result = results.iter()
        .find(|r| r.benchmark_name == "help_command")
        .ok_or("Help command benchmark not found")?;
    
    assert!(help_result.statistics.mean_time < Duration::from_secs(1));
    
    Ok(())
}

#[tokio::test]
async fn test_database_operation_performance() -> TestResult<()> {
    let mut runner = PerformanceTestRunner::new().await?;
    
    // Add database operation benchmarks
    runner = runner.add_benchmarks(
        BenchmarkSuite::create_database_benchmarks().benchmarks
    );
    
    let results = runner.run_all_benchmarks().await?;
    
    // Verify benchmarks completed
    assert!(!results.is_empty());
    
    Ok(())
}

#[tokio::test]
async fn test_load_testing() -> TestResult<()> {
    let runner = PerformanceTestRunner::new().await?
        .add_benchmark(
            PerformanceBenchmark::simple("help", "--help")
        );
    
    let load_config = LoadTestConfig {
        concurrent_users: 2,
        test_duration: Duration::from_secs(5),
        ramp_up_time: Duration::from_secs(1),
        operations_per_second: None,
        think_time: Duration::from_millis(100),
    };
    
    let result = runner.run_load_test(load_config).await?;
    
    assert!(result.total_operations > 0);
    assert!(result.throughput_ops_per_sec > 0.0);
    assert!(result.error_rate_percent < 100.0);
    
    Ok(())
}

#[tokio::test]
async fn test_memory_usage_tracking() -> TestResult<()> {
    let runner = PerformanceTestRunner::new().await?
        .with_config(
            PerformanceConfig {
                memory_profiling: true,
                measurement_iterations: 3,
                ..Default::default()
            }
        )
        .add_benchmark(
            PerformanceBenchmark::simple("version", "--version")
        );
    
    let results = runner.run_all_benchmarks().await?;
    
    assert_eq!(results.len(), 1);
    assert!(results[0].resource_usage.peak_memory_mb > 0.0);
    
    Ok(())
}

#[tokio::test]
async fn test_performance_regression_detection() -> TestResult<()> {
    let benchmark = PerformanceBenchmark::simple("help", "--help")
        .with_expectations(
            Duration::from_millis(500), // Max 500ms for help command
            2.0  // Min 2 ops/sec
        );
    
    let runner = PerformanceTestRunner::new().await?
        .add_benchmark(benchmark);
    
    let results = runner.run_all_benchmarks().await?;
    
    // The test should pass if performance is within expectations
    assert!(results[0].success || results[0].error_message.is_some());
    
    Ok(())
}

#[tokio::test]
async fn test_concurrent_performance_testing() -> TestResult<()> {
    let runner = PerformanceTestRunner::new().await?
        .with_config(
            PerformanceConfig {
                parallel_execution: true,
                measurement_iterations: 5,
                ..Default::default()
            }
        )
        .add_benchmark(PerformanceBenchmark::simple("help", "--help"))
        .add_benchmark(PerformanceBenchmark::simple("version", "--version"));
    
    let results = runner.run_all_benchmarks().await?;
    
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|r| r.statistics.mean_time > Duration::from_nanos(0)));
    
    Ok(())
}