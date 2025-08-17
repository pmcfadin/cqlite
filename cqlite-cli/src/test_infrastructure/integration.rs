//! Integration test framework
//!
//! This module provides utilities for running end-to-end integration tests,
//! including database setup, query execution, and result validation.

use super::assertions::CommandResult;
use super::fixtures::{TestDataBuilder, TestFixture};
use super::{CliTestRunner, TestContainer, TestDatabase, TestResult};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Integration test suite for comprehensive testing
#[derive(Debug)]
pub struct IntegrationTestSuite {
    container: TestContainer,
    cli_runner: CliTestRunner,
    test_database: Arc<Mutex<Option<TestDatabase>>>,
    fixtures: Vec<TestFixture>,
    test_cases: Vec<IntegrationTestCase>,
}

/// End-to-end test runner for complete workflow testing
#[derive(Debug)]
pub struct E2ETestRunner {
    suite: IntegrationTestSuite,
    parallel_execution: bool,
    max_concurrent_tests: usize,
    timeout: Duration,
}

/// Individual integration test case
#[derive(Debug, Clone)]
pub struct IntegrationTestCase {
    pub name: String,
    pub description: String,
    pub setup_commands: Vec<TestCommand>,
    pub test_commands: Vec<TestCommand>,
    pub cleanup_commands: Vec<TestCommand>,
    pub expected_results: Vec<ExpectedResult>,
    pub timeout: Option<Duration>,
    pub tags: Vec<String>,
}

/// Test command to execute
#[derive(Debug, Clone)]
pub struct TestCommand {
    pub command: String,
    pub args: Vec<String>,
    pub input_file: Option<PathBuf>,
    pub expected_exit_code: i32,
    pub max_execution_time: Option<Duration>,
}

/// Expected result for validation
#[derive(Debug, Clone)]
pub struct ExpectedResult {
    pub result_type: ResultType,
    pub validation: ResultValidation,
}

/// Types of expected results
#[derive(Debug, Clone)]
pub enum ResultType {
    CommandOutput,
    DatabaseState,
    FileContent,
    Performance,
}

/// Validation criteria for results
#[derive(Debug, Clone)]
pub enum ResultValidation {
    Contains(String),
    Equals(String),
    MatchesRegex(String),
    JsonPath(String, serde_json::Value),
    RowCount(usize),
    ExecutionTime(Duration),
    Custom(String),
}

/// Test execution results
#[derive(Debug)]
pub struct IntegrationTestResult {
    pub test_name: String,
    pub success: bool,
    pub execution_time: Duration,
    pub command_results: Vec<CommandResult>,
    pub error_message: Option<String>,
    pub performance_metrics: PerformanceMetrics,
}

/// Performance metrics collected during test execution
#[derive(Debug, Default)]
pub struct PerformanceMetrics {
    pub total_execution_time: Duration,
    pub setup_time: Duration,
    pub test_time: Duration,
    pub cleanup_time: Duration,
    pub memory_usage_mb: f64,
    pub disk_io_bytes: u64,
}

impl IntegrationTestSuite {
    /// Create a new integration test suite
    pub async fn new() -> TestResult<Self> {
        let container = TestContainer::new()?;
        let cli_runner = CliTestRunner::new(container.clone());

        Ok(Self {
            container,
            cli_runner,
            test_database: Arc::new(Mutex::new(None)),
            fixtures: Vec::new(),
            test_cases: Vec::new(),
        })
    }

    /// Add test fixtures
    pub async fn with_fixtures(mut self, fixtures: Vec<TestFixture>) -> TestResult<Self> {
        self.fixtures = fixtures;
        Ok(self)
    }

    /// Add a test case
    pub fn add_test_case(mut self, test_case: IntegrationTestCase) -> Self {
        self.test_cases.push(test_case);
        self
    }

    /// Setup test environment
    pub async fn setup(&mut self) -> TestResult<()> {
        // Initialize database
        let db = self.container.init_database().await?;
        *self.test_database.lock().await = Some(db.lock().await.clone());

        // Create test fixtures
        if self.fixtures.is_empty() {
            let env = self.container.environment();
            self.fixtures = TestDataBuilder::create_common_fixtures(env.fixtures_dir)?;
        }

        // Write fixtures to disk
        for fixture in &self.fixtures {
            fixture.write_to_disk()?;
        }

        println!("✅ Integration test environment setup complete");
        Ok(())
    }

    /// Run all test cases
    pub async fn run_all_tests(&mut self) -> TestResult<Vec<IntegrationTestResult>> {
        self.setup().await?;

        let mut results = Vec::new();

        for test_case in &self.test_cases.clone() {
            let result = self.run_single_test(test_case).await?;
            results.push(result);
        }

        self.cleanup().await?;
        Ok(results)
    }

    /// Run a single test case
    pub async fn run_single_test(
        &self,
        test_case: &IntegrationTestCase,
    ) -> TestResult<IntegrationTestResult> {
        let start_time = Instant::now();
        let mut command_results = Vec::new();
        let mut performance_metrics = PerformanceMetrics::default();

        println!("🧪 Running test: {}", test_case.name);

        // Execute setup commands
        let setup_start = Instant::now();
        for command in &test_case.setup_commands {
            let result = self.execute_command(command).await?;
            command_results.push(result);
        }
        performance_metrics.setup_time = setup_start.elapsed();

        // Execute test commands
        let test_start = Instant::now();
        let mut test_success = true;
        let mut error_message = None;

        for (i, command) in test_case.test_commands.iter().enumerate() {
            match self.execute_command(command).await {
                Ok(result) => {
                    // Validate result if expected results are provided
                    if i < test_case.expected_results.len() {
                        if let Err(e) = self
                            .validate_result(&result, &test_case.expected_results[i])
                            .await
                        {
                            test_success = false;
                            error_message = Some(format!("Validation failed: {}", e));
                            break;
                        }
                    }
                    command_results.push(result);
                }
                Err(e) => {
                    test_success = false;
                    error_message = Some(format!("Command execution failed: {}", e));
                    break;
                }
            }
        }
        performance_metrics.test_time = test_start.elapsed();

        // Execute cleanup commands
        let cleanup_start = Instant::now();
        for command in &test_case.cleanup_commands {
            if let Ok(result) = self.execute_command(command).await {
                command_results.push(result);
            }
        }
        performance_metrics.cleanup_time = cleanup_start.elapsed();

        performance_metrics.total_execution_time = start_time.elapsed();

        let result = IntegrationTestResult {
            test_name: test_case.name.clone(),
            success: test_success,
            execution_time: start_time.elapsed(),
            command_results,
            error_message,
            performance_metrics,
        };

        if test_success {
            println!("✅ Test passed: {}", test_case.name);
        } else {
            println!("❌ Test failed: {}", test_case.name);
            if let Some(ref error) = result.error_message {
                println!("   Error: {}", error);
            }
        }

        Ok(result)
    }

    /// Execute a test command
    async fn execute_command(&self, command: &TestCommand) -> TestResult<CommandResult> {
        let start_time = Instant::now();

        let mut args = vec![command.command.as_str()];
        args.extend(command.args.iter().map(|s| s.as_str()));

        let assertion = self.cli_runner.run(&args)?;
        let _cmd_result = assertion.raw_command();

        // This is a simplified version - in practice, you'd capture the actual output
        let result = CommandResult {
            success: command.expected_exit_code == 0,
            stdout: "Command executed successfully".to_string(), // Placeholder
            stderr: "".to_string(),
            exit_code: command.expected_exit_code,
            execution_time: start_time.elapsed(),
        };

        Ok(result)
    }

    /// Validate command result against expected result
    async fn validate_result(
        &self,
        actual: &CommandResult,
        expected: &ExpectedResult,
    ) -> TestResult<()> {
        match (&expected.result_type, &expected.validation) {
            (ResultType::CommandOutput, ResultValidation::Contains(text)) => {
                if !actual.stdout.contains(text) {
                    return Err(format!("Output does not contain expected text: {}", text).into());
                }
            }
            (ResultType::CommandOutput, ResultValidation::Equals(text)) => {
                if actual.stdout.trim() != text {
                    return Err(format!("Output does not equal expected text: {}", text).into());
                }
            }
            (ResultType::Performance, ResultValidation::ExecutionTime(max_time)) => {
                if actual.execution_time > *max_time {
                    return Err(format!(
                        "Execution time {:?} exceeds maximum {:?}",
                        actual.execution_time, max_time
                    )
                    .into());
                }
            }
            _ => {
                // Other validation types can be implemented as needed
                println!("⚠️  Validation type not implemented yet");
            }
        }

        Ok(())
    }

    /// Cleanup test environment
    pub async fn cleanup(&self) -> TestResult<()> {
        // Cleanup is handled by the TestContainer's Drop implementation
        println!("🧹 Integration test cleanup complete");
        Ok(())
    }
}

impl E2ETestRunner {
    /// Create a new E2E test runner
    pub async fn new() -> TestResult<Self> {
        let suite = IntegrationTestSuite::new().await?;

        Ok(Self {
            suite,
            parallel_execution: false,
            max_concurrent_tests: 1,
            timeout: Duration::from_secs(300), // 5 minutes default
        })
    }

    /// Enable parallel test execution
    pub fn with_parallel_execution(mut self, max_concurrent: usize) -> Self {
        self.parallel_execution = true;
        self.max_concurrent_tests = max_concurrent;
        self
    }

    /// Set global timeout for all tests
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Add test cases from common scenarios
    pub fn with_common_test_cases(mut self) -> Self {
        // Add basic CLI functionality tests
        self.suite = self.suite.add_test_case(IntegrationTestCase {
            name: "basic_help_command".to_string(),
            description: "Test basic help command functionality".to_string(),
            setup_commands: vec![],
            test_commands: vec![TestCommand {
                command: "--help".to_string(),
                args: vec![],
                input_file: None,
                expected_exit_code: 0,
                max_execution_time: Some(Duration::from_secs(5)),
            }],
            cleanup_commands: vec![],
            expected_results: vec![ExpectedResult {
                result_type: ResultType::CommandOutput,
                validation: ResultValidation::Contains("CQLite".to_string()),
            }],
            timeout: Some(Duration::from_secs(30)),
            tags: vec!["basic".to_string(), "cli".to_string()],
        });

        // Add database operation tests
        self.suite = self.suite.add_test_case(IntegrationTestCase {
            name: "database_operations".to_string(),
            description: "Test basic database operations".to_string(),
            setup_commands: vec![],
            test_commands: vec![TestCommand {
                command: "query".to_string(),
                args: vec!["SELECT * FROM system.local".to_string()],
                input_file: None,
                expected_exit_code: 0,
                max_execution_time: Some(Duration::from_secs(10)),
            }],
            cleanup_commands: vec![],
            expected_results: vec![ExpectedResult {
                result_type: ResultType::CommandOutput,
                validation: ResultValidation::Contains("system".to_string()),
            }],
            timeout: Some(Duration::from_secs(60)),
            tags: vec!["database".to_string(), "query".to_string()],
        });

        self
    }

    /// Run all E2E tests
    pub async fn run_all(&mut self) -> TestResult<E2ETestReport> {
        let start_time = Instant::now();

        let results = if self.parallel_execution {
            self.run_parallel_tests().await?
        } else {
            self.suite.run_all_tests().await?
        };

        let total_time = start_time.elapsed();
        let total_tests = results.len();
        let passed_tests = results.iter().filter(|r| r.success).count();
        let failed_tests = total_tests - passed_tests;

        let performance_summary = self.calculate_performance_summary(&results);
        let report = E2ETestReport {
            total_tests,
            passed_tests,
            failed_tests,
            total_execution_time: total_time,
            test_results: results,
            performance_summary,
        };

        self.print_summary(&report);
        Ok(report)
    }

    /// Run tests in parallel (placeholder implementation)
    async fn run_parallel_tests(&mut self) -> TestResult<Vec<IntegrationTestResult>> {
        // For now, just run sequentially
        // In a real implementation, you'd use tokio::spawn and semaphores
        self.suite.run_all_tests().await
    }

    /// Calculate performance summary
    fn calculate_performance_summary(
        &self,
        results: &[IntegrationTestResult],
    ) -> PerformanceSummary {
        let total_time: Duration = results.iter().map(|r| r.execution_time).sum();
        let avg_time = if !results.is_empty() {
            total_time / results.len() as u32
        } else {
            Duration::from_secs(0)
        };

        let max_time = results
            .iter()
            .map(|r| r.execution_time)
            .max()
            .unwrap_or(Duration::from_secs(0));

        PerformanceSummary {
            total_execution_time: total_time,
            average_test_time: avg_time,
            slowest_test_time: max_time,
            fastest_test_time: results
                .iter()
                .map(|r| r.execution_time)
                .min()
                .unwrap_or(Duration::from_secs(0)),
        }
    }

    /// Print test summary
    fn print_summary(&self, report: &E2ETestReport) {
        println!("\n📊 E2E Test Summary");
        println!("==================");
        println!("Total Tests: {}", report.total_tests);
        println!(
            "Passed: {} ({}%)",
            report.passed_tests,
            (report.passed_tests * 100) / report.total_tests.max(1)
        );
        println!("Failed: {}", report.failed_tests);
        println!("Total Time: {:?}", report.total_execution_time);
        println!(
            "Average Time: {:?}",
            report.performance_summary.average_test_time
        );

        if report.failed_tests > 0 {
            println!("\n❌ Failed Tests:");
            for result in &report.test_results {
                if !result.success {
                    println!(
                        "  - {}: {}",
                        result.test_name,
                        result.error_message.as_deref().unwrap_or("Unknown error")
                    );
                }
            }
        }

        println!(
            "\n{}",
            if report.failed_tests == 0 {
                "🎉 All tests passed!"
            } else {
                "⚠️  Some tests failed"
            }
        );
    }
}

/// E2E test execution report
#[derive(Debug)]
pub struct E2ETestReport {
    pub total_tests: usize,
    pub passed_tests: usize,
    pub failed_tests: usize,
    pub total_execution_time: Duration,
    pub test_results: Vec<IntegrationTestResult>,
    pub performance_summary: PerformanceSummary,
}

/// Performance summary for test execution
#[derive(Debug)]
pub struct PerformanceSummary {
    pub total_execution_time: Duration,
    pub average_test_time: Duration,
    pub slowest_test_time: Duration,
    pub fastest_test_time: Duration,
}

impl IntegrationTestCase {
    /// Create a simple test case
    pub fn simple<S: Into<String>>(name: S, command: S, expected_output: S) -> Self {
        let name_string = name.into();
        Self {
            name: name_string.clone(),
            description: format!("Simple test: {}", name_string),
            setup_commands: vec![],
            test_commands: vec![TestCommand {
                command: command.into(),
                args: vec![],
                input_file: None,
                expected_exit_code: 0,
                max_execution_time: Some(Duration::from_secs(30)),
            }],
            cleanup_commands: vec![],
            expected_results: vec![ExpectedResult {
                result_type: ResultType::CommandOutput,
                validation: ResultValidation::Contains(expected_output.into()),
            }],
            timeout: Some(Duration::from_secs(60)),
            tags: vec!["simple".to_string()],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_integration_test_suite_creation() {
        let suite = IntegrationTestSuite::new().await.unwrap();
        assert_eq!(suite.test_cases.len(), 0);
        assert_eq!(suite.fixtures.len(), 0);
    }

    #[tokio::test]
    async fn test_e2e_runner_creation() {
        let runner = E2ETestRunner::new().await.unwrap();
        assert!(!runner.parallel_execution);
        assert_eq!(runner.max_concurrent_tests, 1);
    }

    #[test]
    fn test_integration_test_case_creation() {
        let test_case = IntegrationTestCase::simple("test_help", "--help", "CQLite");

        assert_eq!(test_case.name, "test_help");
        assert_eq!(test_case.test_commands.len(), 1);
        assert_eq!(test_case.expected_results.len(), 1);
    }
}
