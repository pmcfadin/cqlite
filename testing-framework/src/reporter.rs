//! Test reporting functionality

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::Path;
use crate::config::ReportingConfig;
use crate::{TestSuiteResult, TestResult};

/// Test reporter for generating various output formats
pub struct TestReporter {
    config: ReportingConfig,
}

impl TestReporter {
    /// Create a new test reporter
    pub fn new(config: &ReportingConfig) -> Self {
        Self {
            config: config.clone(),
        }
    }

    /// Generate comprehensive test report
    pub async fn generate_report(&self, suite_result: &TestSuiteResult) -> Result<()> {
        // Ensure output directory exists
        std::fs::create_dir_all(&self.config.output_directory)?;

        if self.config.generate_json {
            self.generate_json_report(suite_result).await?;
        }

        if self.config.generate_html {
            self.generate_html_report(suite_result).await?;
        }

        if self.config.generate_csv {
            self.generate_csv_report(suite_result).await?;
        }

        if self.config.generate_diff_files {
            self.generate_diff_files(suite_result).await?;
        }

        Ok(())
    }

    /// Generate JSON report
    async fn generate_json_report(&self, suite_result: &TestSuiteResult) -> Result<()> {
        let report_path = self.config.output_directory.join("test_results.json");
        let json_content = serde_json::to_string_pretty(suite_result)?;
        std::fs::write(report_path, json_content)?;
        Ok(())
    }

    /// Generate HTML report
    async fn generate_html_report(&self, suite_result: &TestSuiteResult) -> Result<()> {
        let report_path = self.config.output_directory.join("test_results.html");
        let html_content = self.generate_html_content(suite_result);
        std::fs::write(report_path, html_content)?;
        Ok(())
    }

    /// Generate CSV report
    async fn generate_csv_report(&self, suite_result: &TestSuiteResult) -> Result<()> {
        let report_path = self.config.output_directory.join("test_results.csv");
        let mut csv_content = String::from("test_name,success,execution_time_ms,error\n");
        
        for test_result in &suite_result.test_results {
            csv_content.push_str(&format!(
                "{},{},{},{}\n",
                test_result.test_name,
                test_result.success,
                test_result.execution_time_ms,
                test_result.error.as_ref().unwrap_or(&String::new()).replace(",", ";")
            ));
        }
        
        std::fs::write(report_path, csv_content)?;
        Ok(())
    }

    /// Generate diff files for failed tests
    async fn generate_diff_files(&self, suite_result: &TestSuiteResult) -> Result<()> {
        let diff_dir = self.config.output_directory.join("diffs");
        std::fs::create_dir_all(&diff_dir)?;

        for test_result in &suite_result.test_results {
            if !test_result.success {
                let diff_path = diff_dir.join(format!("{}.diff", test_result.test_name));
                let diff_content = self.generate_diff_content(test_result);
                std::fs::write(diff_path, diff_content)?;
            }
        }

        Ok(())
    }

    /// Generate HTML content for the report
    fn generate_html_content(&self, suite_result: &TestSuiteResult) -> String {
        format!(
            r#"<!DOCTYPE html>
<html>
<head>
    <title>CQLite Test Results</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .summary {{ background: #f0f0f0; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
        .test-passed {{ background: #d4edda; }}
        .test-failed {{ background: #f8d7da; }}
        table {{ border-collapse: collapse; width: 100%; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
    </style>
</head>
<body>
    <h1>CQLite Test Results</h1>
    <div class="summary">
        <h2>Summary</h2>
        <p><strong>Suite:</strong> {}</p>
        <p><strong>Total Tests:</strong> {}</p>
        <p><strong>Passed:</strong> {} ({:.1}%)</p>
        <p><strong>Failed:</strong> {}</p>
        <p><strong>Execution Time:</strong> {} ms</p>
        <p><strong>Timestamp:</strong> {}</p>
    </div>
    <h2>Test Details</h2>
    <table>
        <tr>
            <th>Test Name</th>
            <th>Status</th>
            <th>Execution Time (ms)</th>
            <th>Error</th>
        </tr>
        {}
    </table>
</body>
</html>"#,
            suite_result.suite_name,
            suite_result.total_tests,
            suite_result.passed_tests,
            suite_result.success_rate,
            suite_result.failed_tests,
            suite_result.total_execution_time_ms,
            suite_result.timestamp.format("%Y-%m-%d %H:%M:%S UTC"),
            suite_result.test_results.iter().map(|test| {
                format!(
                    "<tr class=\"{}\">
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                    </tr>",
                    if test.success { "test-passed" } else { "test-failed" },
                    test.test_name,
                    if test.success { "PASSED" } else { "FAILED" },
                    test.execution_time_ms,
                    test.error.as_ref().unwrap_or(&String::new())
                )
            }).collect::<Vec<_>>().join("\n")
        )
    }

    /// Generate diff content for a failed test
    fn generate_diff_content(&self, test_result: &TestResult) -> String {
        format!(
Test: {}
Status: {}
Execution Time: {} ms
Error: {}

=== CQLSH OUTPUT ===
{}

=== CQLITE OUTPUT ===
{}

=== COMPARISON RESULT ===
{}
",
            test_result.test_name,
            if test_result.success { "PASSED" } else { "FAILED" },
            test_result.execution_time_ms,
            test_result.error.as_ref().unwrap_or(&"No error".to_string()),
            test_result.cqlsh_output.raw_output,
            test_result.cqlite_output.raw_output,
            serde_json::to_string_pretty(&test_result.comparison).unwrap_or_else(|_| "Failed to serialize comparison".to_string())
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ReportingConfig;
    use std::path::PathBuf;
    use tempfile::TempDir;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_reporter_creation() {
        let config = ReportingConfig {
            output_directory: PathBuf::from("/tmp/test"),
            generate_html: true,
            generate_json: true,
            generate_csv: true,
            generate_diff_files: true,
            include_success_details: false,
            max_diff_lines: 1000,
            compress_large_outputs: true,
        };
        
        let reporter = TestReporter::new(&config);
        assert!(true); // Just test creation doesn't panic
    }
}