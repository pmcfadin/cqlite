//! CQLite Testing Framework
//! 
//! A comprehensive framework for comparing cqlsh and cqlite outputs to ensure compatibility
//! and correctness of the CQLite implementation.

pub mod config;
pub mod docker;
pub mod executor;
pub mod output;
pub mod reporter;
pub mod test_case;
pub mod comparator;
pub mod cli;
pub mod utils;
pub mod error;

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Main test framework coordinator
pub struct TestFramework {
    config: config::TestConfig,
    docker_manager: docker::DockerManager,
    executor: executor::TestExecutor,
    comparator: comparator::OutputComparator,
    reporter: reporter::TestReporter,
}

/// Test execution result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestResult {
    pub test_id: Uuid,
    pub test_name: String,
    pub success: bool,
    pub execution_time_ms: u64,
    pub cqlsh_output: output::QueryOutput,
    pub cqlite_output: output::QueryOutput,
    pub comparison: comparator::ComparisonResult,
    pub error: Option<String>,
    pub timestamp: DateTime<Utc>,
}

/// Test suite results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestSuiteResult {
    pub suite_id: Uuid,
    pub suite_name: String,
    pub total_tests: usize,
    pub passed_tests: usize,
    pub failed_tests: usize,
    pub success_rate: f64,
    pub total_execution_time_ms: u64,
    pub test_results: Vec<TestResult>,
    pub summary: HashMap<String, serde_json::Value>,
    pub timestamp: DateTime<Utc>,
}

impl TestFramework {
    /// Create a new test framework instance
    pub async fn new(config: config::TestConfig) -> Result<Self> {
        let docker_manager = docker::DockerManager::new(config.docker.clone());
        let executor = executor::TestExecutor::new(&config);
        let comparator = comparator::OutputComparator::new(&config.comparison);
        let reporter = reporter::TestReporter::new(&config.reporting);

        Ok(Self {
            config,
            docker_manager,
            executor,
            comparator,
            reporter,
        })
    }

    /// Run a single test case
    pub async fn run_test(&mut self, test_case: &test_case::TestCase) -> Result<TestResult> {
        let start_time = std::time::Instant::now();
        let test_id = Uuid::new_v4();

        log::info!("Running test: {} (ID: {})", test_case.name, test_id);

        // Execute on both systems
        let cqlsh_result = self.executor.execute_cqlsh(test_case).await;
        let cqlite_result = self.executor.execute_cqlite(test_case).await;

        let execution_time_ms = start_time.elapsed().as_millis() as u64;

        match (cqlsh_result, cqlite_result) {
            (Ok(cqlsh_output), Ok(cqlite_output)) => {
                // Compare outputs
                let comparison = self.comparator.compare(&cqlsh_output, &cqlite_output)?;
                let success = comparison.is_match();

                Ok(TestResult {
                    test_id,
                    test_name: test_case.name.clone(),
                    success,
                    execution_time_ms,
                    cqlsh_output,
                    cqlite_output,
                    comparison,
                    error: None,
                    timestamp: Utc::now(),
                })
            }
            (Err(e), _) => Ok(TestResult {
                test_id,
                test_name: test_case.name.clone(),
                success: false,
                execution_time_ms,
                cqlsh_output: output::QueryOutput::default(),
                cqlite_output: output::QueryOutput::default(),
                comparison: comparator::ComparisonResult::error("cqlsh execution failed"),
                error: Some(format!("cqlsh error: {}", e)),
                timestamp: Utc::now(),
            }),
            (_, Err(e)) => Ok(TestResult {
                test_id,
                test_name: test_case.name.clone(),
                success: false,
                execution_time_ms,
                cqlsh_output: output::QueryOutput::default(),
                cqlite_output: output::QueryOutput::default(),
                comparison: comparator::ComparisonResult::error("cqlite execution failed"),
                error: Some(format!("cqlite error: {}", e)),
                timestamp: Utc::now(),
            }),
        }
    }

    /// Run a complete test suite
    pub async fn run_test_suite(&mut self, test_cases: Vec<test_case::TestCase>) -> Result<TestSuiteResult> {
        let suite_id = Uuid::new_v4();
        let suite_name = format!("Test Suite {}", chrono::Utc::now().format("%Y-%m-%d %H:%M:%S"));
        let total_tests = test_cases.len();
        let start_time = std::time::Instant::now();

        log::info!("Starting test suite: {} with {} tests", suite_name, total_tests);

        // Ensure Docker environment is ready
        self.docker_manager.ensure_cassandra_ready().await.map_err(|e| anyhow::anyhow!(e))?;

        let mut test_results = Vec::new();
        let mut passed_tests = 0;

        for test_case in test_cases {
            match self.run_test(&test_case).await {
                Ok(result) => {
                    if result.success {
                        passed_tests += 1;
                    }
                    test_results.push(result);
                }
                Err(e) => {
                    log::error!("Test execution failed: {}", e);
                    test_results.push(TestResult {
                        test_id: Uuid::new_v4(),
                        test_name: test_case.name,
                        success: false,
                        execution_time_ms: 0,
                        cqlsh_output: output::QueryOutput::default(),
                        cqlite_output: output::QueryOutput::default(),
                        comparison: comparator::ComparisonResult::error("Test execution failed"),
                        error: Some(e.to_string()),
                        timestamp: Utc::now(),
                    });
                }
            }
        }

        let failed_tests = total_tests - passed_tests;
        let success_rate = if total_tests > 0 {
            (passed_tests as f64 / total_tests as f64) * 100.0
        } else {
            0.0
        };

        let total_execution_time_ms = start_time.elapsed().as_millis() as u64;

        let mut summary = HashMap::new();
        summary.insert("docker_status".to_string(), serde_json::json!("ready"));
        summary.insert("environment".to_string(), serde_json::json!(self.config.environment));

        let suite_result = TestSuiteResult {
            suite_id,
            suite_name,
            total_tests,
            passed_tests,
            failed_tests,
            success_rate,
            total_execution_time_ms,
            test_results,
            summary,
            timestamp: Utc::now(),
        };

        // Generate reports
        self.reporter.generate_report(&suite_result).await?;

        log::info!(
            "Test suite completed: {}/{} tests passed ({:.1}%)",
            passed_tests, total_tests, success_rate
        );

        Ok(suite_result)
    }

    /// Cleanup resources
    pub async fn cleanup(&mut self) -> Result<()> {
        self.docker_manager.cleanup().await.map_err(|e| anyhow::anyhow!(e))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_framework_creation() {
        let config = config::TestConfig::default();
        let framework = TestFramework::new(config).await;
        assert!(framework.is_ok());
    }
}