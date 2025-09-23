//! Golden-Path Testing Framework for CQLite
//!
//! This module provides comprehensive testing of happy-path scenarios using real
//! Cassandra 5 SSTable artifacts. It focuses on:
//! - get() operations with known partition keys
//! - scan() operations for range queries and full table scans
//! - lookup_partition_with_index() for efficient partition discovery
//! - End-to-end component coordination across Summary, Index, and Data files
//!
//! The framework uses authentic Cassandra 5.x SSTable triplets to ensure
//! realistic testing conditions and validate proper component integration.

pub mod harness;
pub mod scenarios;
pub mod validation;
pub mod metrics;
pub mod artifacts;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use cqlite_core::{
    Config, Result, RowKey, Value,
    platform::Platform,
    storage::sstable::{SSTableReader, summary_reader::SummaryReader, index_reader::IndexReader},
    types::TableId,
};

/// Golden-path test configuration
#[derive(Debug, Clone)]
pub struct GoldenPathConfig {
    /// Base directory containing test artifacts
    pub artifacts_dir: PathBuf,
    /// Performance regression threshold (percentage)
    pub performance_threshold: f64,
    /// Enable detailed metrics collection
    pub detailed_metrics: bool,
    /// Maximum test execution time per scenario
    pub timeout: Duration,
    /// Enable component coordination validation
    pub validate_integration: bool,
}

impl Default for GoldenPathConfig {
    fn default() -> Self {
        Self {
            artifacts_dir: PathBuf::from("tests/golden_path/artifacts"),
            performance_threshold: 10.0, // 10% regression threshold
            detailed_metrics: true,
            timeout: Duration::from_secs(30),
            validate_integration: true,
        }
    }
}

/// Results from a golden-path test execution
#[derive(Debug, Clone)]
pub struct GoldenPathResults {
    /// Test scenario name
    pub scenario: String,
    /// Whether the test passed
    pub passed: bool,
    /// Execution duration
    pub duration: Duration,
    /// Operations performed
    pub operations_count: usize,
    /// Performance metrics
    pub metrics: TestMetrics,
    /// Validation results
    pub validation: ValidationResults,
    /// Any errors encountered
    pub errors: Vec<String>,
}

/// Performance and operational metrics
#[derive(Debug, Clone)]
pub struct TestMetrics {
    /// Average operation latency
    pub avg_latency: Duration,
    /// Peak memory usage during test
    pub peak_memory_kb: usize,
    /// Cache hit rates
    pub cache_hit_rate: f64,
    /// Component coordination timing
    pub coordination_timing: ComponentTiming,
    /// Throughput (operations per second)
    pub throughput: f64,
}

/// Component coordination timing breakdown
#[derive(Debug, Clone)]
pub struct ComponentTiming {
    /// Time spent in Summary.db operations
    pub summary_time: Duration,
    /// Time spent in Index.db operations
    pub index_time: Duration,
    /// Time spent in Data.db operations
    pub data_time: Duration,
    /// Total coordination overhead
    pub coordination_overhead: Duration,
}

/// Validation results comparing actual vs expected
#[derive(Debug, Clone)]
pub struct ValidationResults {
    /// Data correctness validation
    pub data_correct: bool,
    /// Performance within acceptable bounds
    pub performance_acceptable: bool,
    /// Component integration working properly
    pub integration_valid: bool,
    /// Detailed validation messages
    pub messages: Vec<String>,
}

/// Test suite runner for golden-path scenarios
pub struct GoldenPathTestSuite {
    config: GoldenPathConfig,
    harness: harness::GoldenPathTestHarness,
    metrics_collector: metrics::MetricsCollector,
}

impl GoldenPathTestSuite {
    /// Create a new golden-path test suite
    pub async fn new(config: GoldenPathConfig) -> Result<Self> {
        let harness = harness::GoldenPathTestHarness::new(&config).await?;
        let metrics_collector = metrics::MetricsCollector::new(config.detailed_metrics);

        Ok(Self {
            config,
            harness,
            metrics_collector,
        })
    }

    /// Run all golden-path test scenarios
    pub async fn run_all_scenarios(&mut self) -> Result<Vec<GoldenPathResults>> {
        let mut results = Vec::new();

        // Get operation scenarios
        results.extend(self.run_get_scenarios().await?);

        // Scan operation scenarios
        results.extend(self.run_scan_scenarios().await?);

        // Lookup with index scenarios
        results.extend(self.run_lookup_scenarios().await?);

        // Component integration scenarios
        if self.config.validate_integration {
            results.extend(self.run_integration_scenarios().await?);
        }

        Ok(results)
    }

    /// Run get() operation test scenarios
    async fn run_get_scenarios(&mut self) -> Result<Vec<GoldenPathResults>> {
        let scenarios = vec![
            scenarios::get_single_key_scenario(),
            scenarios::get_multiple_keys_scenario(),
            scenarios::get_nonexistent_key_scenario(),
            scenarios::get_with_bloom_filter_scenario(),
        ];

        let mut results = Vec::new();
        for scenario in scenarios {
            let result = self.execute_scenario(scenario).await?;
            results.push(result);
        }

        Ok(results)
    }

    /// Run scan() operation test scenarios
    async fn run_scan_scenarios(&mut self) -> Result<Vec<GoldenPathResults>> {
        let scenarios = vec![
            scenarios::scan_full_table_scenario(),
            scenarios::scan_token_range_scenario(),
            scenarios::scan_with_limit_scenario(),
            scenarios::scan_empty_range_scenario(),
        ];

        let mut results = Vec::new();
        for scenario in scenarios {
            let result = self.execute_scenario(scenario).await?;
            results.push(result);
        }

        Ok(results)
    }

    /// Run lookup_partition_with_index() scenarios
    async fn run_lookup_scenarios(&mut self) -> Result<Vec<GoldenPathResults>> {
        let scenarios = vec![
            scenarios::lookup_partition_basic_scenario(),
            scenarios::lookup_partition_with_promoted_index_scenario(),
            scenarios::lookup_wide_partition_scenario(),
        ];

        let mut results = Vec::new();
        for scenario in scenarios {
            let result = self.execute_scenario(scenario).await?;
            results.push(result);
        }

        Ok(results)
    }

    /// Run component integration scenarios
    async fn run_integration_scenarios(&mut self) -> Result<Vec<GoldenPathResults>> {
        let scenarios = vec![
            scenarios::summary_index_coordination_scenario(),
            scenarios::index_data_coordination_scenario(),
            scenarios::end_to_end_coordination_scenario(),
        ];

        let mut results = Vec::new();
        for scenario in scenarios {
            let result = self.execute_scenario(scenario).await?;
            results.push(result);
        }

        Ok(results)
    }

    /// Execute a single test scenario
    async fn execute_scenario(&mut self, scenario: scenarios::TestScenario) -> Result<GoldenPathResults> {
        let start_time = Instant::now();

        // Start metrics collection
        self.metrics_collector.start_collection(&scenario.name);

        // Execute the scenario
        let execution_result = self.harness.execute_scenario(&scenario).await;

        let duration = start_time.elapsed();

        // Stop metrics collection
        let metrics = self.metrics_collector.stop_collection(&scenario.name);

        // Validate results
        let validation = match &execution_result {
            Ok(scenario_result) => {
                validation::validate_scenario_result(&scenario, scenario_result, &self.config).await
            }
            Err(_) => ValidationResults {
                data_correct: false,
                performance_acceptable: false,
                integration_valid: false,
                messages: vec!["Scenario execution failed".to_string()],
            }
        };

        let (passed, errors, operations_count) = match execution_result {
            Ok(result) => (validation.data_correct && validation.performance_acceptable, vec![], result.operations_count),
            Err(e) => (false, vec![e.to_string()], 0),
        };

        Ok(GoldenPathResults {
            scenario: scenario.name.clone(),
            passed,
            duration,
            operations_count,
            metrics,
            validation,
            errors,
        })
    }

    /// Generate a comprehensive test report
    pub fn generate_report(&self, results: &[GoldenPathResults]) -> String {
        let total_tests = results.len();
        let passed_tests = results.iter().filter(|r| r.passed).count();
        let total_duration: Duration = results.iter().map(|r| r.duration).sum();
        let total_operations: usize = results.iter().map(|r| r.operations_count).sum();

        let mut report = String::new();
        report.push_str("# Golden-Path Test Results\n\n");
        report.push_str(&format!("## Summary\n"));
        report.push_str(&format!("- **Total Tests**: {}\n", total_tests));
        report.push_str(&format!("- **Passed**: {}\n", passed_tests));
        report.push_str(&format!("- **Failed**: {}\n", total_tests - passed_tests));
        report.push_str(&format!("- **Success Rate**: {:.1}%\n", (passed_tests as f64 / total_tests as f64) * 100.0));
        report.push_str(&format!("- **Total Duration**: {:.2}s\n", total_duration.as_secs_f64()));
        report.push_str(&format!("- **Total Operations**: {}\n", total_operations));

        if total_duration.as_secs_f64() > 0.0 {
            report.push_str(&format!("- **Overall Throughput**: {:.1} ops/sec\n\n", total_operations as f64 / total_duration.as_secs_f64()));
        }

        // Detailed results
        report.push_str("## Detailed Results\n\n");
        for result in results {
            report.push_str(&format!("### {}\n", result.scenario));
            report.push_str(&format!("- **Status**: {}\n", if result.passed { "✅ PASS" } else { "❌ FAIL" }));
            report.push_str(&format!("- **Duration**: {:.3}s\n", result.duration.as_secs_f64()));
            report.push_str(&format!("- **Operations**: {}\n", result.operations_count));
            report.push_str(&format!("- **Avg Latency**: {:.3}ms\n", result.metrics.avg_latency.as_secs_f64() * 1000.0));
            report.push_str(&format!("- **Throughput**: {:.1} ops/sec\n", result.metrics.throughput));

            if !result.passed {
                report.push_str("- **Errors**:\n");
                for error in &result.errors {
                    report.push_str(&format!("  - {}\n", error));
                }
                report.push_str("- **Validation Messages**:\n");
                for msg in &result.validation.messages {
                    report.push_str(&format!("  - {}\n", msg));
                }
            }
            report.push_str("\n");
        }

        report
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_golden_path_suite_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = GoldenPathConfig {
            artifacts_dir: temp_dir.path().to_path_buf(),
            ..Default::default()
        };

        // Should handle missing artifacts gracefully
        let result = GoldenPathTestSuite::new(config).await;
        assert!(result.is_ok() || result.err().unwrap().to_string().contains("artifacts"));
    }

    #[test]
    fn test_golden_path_config_defaults() {
        let config = GoldenPathConfig::default();
        assert_eq!(config.performance_threshold, 10.0);
        assert_eq!(config.detailed_metrics, true);
        assert_eq!(config.validate_integration, true);
    }
}