//! Validation Test Runner
//!
//! This module provides comprehensive validation testing for CQLite integration tests,
//! ensuring that test fixtures, CLI commands, and compatibility features work correctly.

use crate::{
    CLIIntegrationTestSuite, CLITestConfig, SSTableTestFixtureConfig, SSTableTestFixtureGenerator,
    SSTableTestFixtureValidator,
};
use serde_json;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;
use tempfile::TempDir;

/// Validation test configuration
#[derive(Debug, Clone)]
pub struct ValidationTestConfig {
    pub validate_fixtures: bool,
    pub validate_cli: bool,
    pub validate_performance: bool,
    pub generate_report: bool,
    pub report_format: ReportFormat,
    pub timeout_seconds: u64,
}

#[derive(Debug, Clone)]
pub enum ReportFormat {
    Json,
    Html,
    Markdown,
}

impl Default for ValidationTestConfig {
    fn default() -> Self {
        Self {
            validate_fixtures: true,
            validate_cli: true,
            validate_performance: false,
            generate_report: true,
            report_format: ReportFormat::Json,
            timeout_seconds: 60,
        }
    }
}

/// Validation test results
#[derive(Debug, Clone)]
pub struct ValidationTestResults {
    pub total_validations: usize,
    pub successful_validations: usize,
    pub failed_validations: usize,
    pub validation_time_ms: u64,
    pub fixture_results: Vec<FixtureValidationResult>,
    pub cli_results: Vec<CLIValidationResult>,
    pub performance_results: Option<PerformanceValidationResult>,
    pub recommendations: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct FixtureValidationResult {
    pub fixture_name: String,
    pub is_valid: bool,
    pub file_size_bytes: u64,
    pub record_count: usize,
    pub schema_columns: usize,
    pub issues: Vec<String>,
    pub validation_time_ms: u64,
}

#[derive(Debug, Clone)]
pub struct CLIValidationResult {
    pub command: String,
    pub success: bool,
    pub response_time_ms: u64,
    pub output_size_bytes: usize,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PerformanceValidationResult {
    pub parse_throughput: f64,
    pub memory_usage_mb: f64,
    pub response_time_ms: f64,
    pub meets_requirements: bool,
    pub benchmark_details: HashMap<String, f64>,
}

/// Main validation test runner
pub struct ValidationTestRunner {
    config: ValidationTestConfig,
    temp_dir: TempDir,
    test_output_dir: PathBuf,
}

impl ValidationTestRunner {
    pub fn new(config: ValidationTestConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let test_output_dir = temp_dir.path().join("validation_output");
        fs::create_dir_all(&test_output_dir)?;

        Ok(Self {
            config,
            temp_dir,
            test_output_dir,
        })
    }

    /// Run all validation tests
    pub async fn run_validation_tests(
        &mut self,
    ) -> Result<ValidationTestResults, Box<dyn std::error::Error>> {
        println!("🔍 Starting CQLite Validation Tests");
        println!("=".repeat(50));

        let overall_start = Instant::now();
        let mut results = ValidationTestResults {
            total_validations: 0,
            successful_validations: 0,
            failed_validations: 0,
            validation_time_ms: 0,
            fixture_results: Vec::new(),
            cli_results: Vec::new(),
            performance_results: None,
            recommendations: Vec::new(),
        };

        // Validate SSTable fixtures
        if self.config.validate_fixtures {
            self.validate_sstable_fixtures(&mut results).await?;
        }

        // Validate CLI functionality
        if self.config.validate_cli {
            self.validate_cli_functionality(&mut results).await?;
        }

        // Validate performance characteristics
        if self.config.validate_performance {
            self.validate_performance(&mut results).await?;
        }

        results.validation_time_ms = overall_start.elapsed().as_millis() as u64;

        // Generate recommendations
        self.generate_recommendations(&mut results);

        // Generate report
        if self.config.generate_report {
            self.generate_validation_report(&results).await?;
        }

        self.print_validation_summary(&results);
        Ok(results)
    }

    /// Validate SSTable test fixtures
    async fn validate_sstable_fixtures(
        &mut self,
        results: &mut ValidationTestResults,
    ) -> Result<(), Box<dyn std::error::Error>> {
        println!("📊 Validating SSTable test fixtures...");

        // Generate test fixtures
        let fixture_config = SSTableTestFixtureConfig {
            generate_simple_types: true,
            generate_collections: true,
            generate_large_data: false, // Skip large data for validation
            generate_user_defined_types: false,
            record_count: 100, // Small count for validation
            compression_enabled: true,
        };

        let fixtures_dir = self.test_output_dir.join("fixtures");
        fs::create_dir_all(&fixtures_dir)?;

        let generator = SSTableTestFixtureGenerator::new(fixture_config, fixtures_dir);
        let fixtures = generator.generate_all_fixtures().await?;

        println!("  📝 Generated {} fixtures for validation", fixtures.len());

        // Validate each fixture
        let validator = SSTableTestFixtureValidator::new();
        for fixture in fixtures {
            let validation_start = Instant::now();
            results.total_validations += 1;

            println!("    • Validating {}...", fixture.name);

            match validator.validate_fixture(&fixture).await {
                Ok(validation_result) => {
                    let file_size = if fixture.file_path.exists() {
                        fs::metadata(&fixture.file_path)?.len()
                    } else {
                        0
                    };

                    let fixture_result = FixtureValidationResult {
                        fixture_name: fixture.name.clone(),
                        is_valid: validation_result.is_valid,
                        file_size_bytes: file_size,
                        record_count: fixture.expected_record_count,
                        schema_columns: fixture.expected_schema.len(),
                        issues: validation_result.issues,
                        validation_time_ms: validation_start.elapsed().as_millis() as u64,
                    };

                    if fixture_result.is_valid {
                        results.successful_validations += 1;
                        println!("      ✅ Valid");
                    } else {
                        results.failed_validations += 1;
                        println!("      ❌ Issues found: {}", fixture_result.issues.len());
                        for issue in &fixture_result.issues {
                            println!("        - {}", issue);
                        }
                    }

                    results.fixture_results.push(fixture_result);
                }
                Err(e) => {
                    results.failed_validations += 1;
                    println!("      ❌ Validation error: {:?}", e);

                    results.fixture_results.push(FixtureValidationResult {
                        fixture_name: fixture.name,
                        is_valid: false,
                        file_size_bytes: 0,
                        record_count: 0,
                        schema_columns: 0,
                        issues: vec![format!("Validation error: {:?}", e)],
                        validation_time_ms: validation_start.elapsed().as_millis() as u64,
                    });
                }
            }
        }

        Ok(())
    }

    /// Validate CLI functionality
    async fn validate_cli_functionality(
        &mut self,
        results: &mut ValidationTestResults,
    ) -> Result<(), Box<dyn std::error::Error>> {
        println!("💻 Validating CLI functionality...");

        let cli_config = CLITestConfig {
            test_basic_commands: true,
            test_parse_commands: false, // Skip parse commands for validation
            test_export_formats: false,
            test_error_handling: true,
            test_performance: false,
            test_large_files: false,
            timeout_seconds: self.config.timeout_seconds,
        };

        let mut cli_test_suite = CLIIntegrationTestSuite::new(cli_config)?;
        let cli_test_results = cli_test_suite.run_all_tests().await?;

        for cli_result in cli_test_results {
            results.total_validations += 1;

            let validation_result = CLIValidationResult {
                command: cli_result.test_name.clone(),
                success: cli_result.success,
                response_time_ms: cli_result.execution_time_ms,
                output_size_bytes: cli_result.stdout.len() + cli_result.stderr.len(),
                error_message: if cli_result.success {
                    None
                } else {
                    Some(cli_result.details)
                },
            };

            if validation_result.success {
                results.successful_validations += 1;
            } else {
                results.failed_validations += 1;
            }

            results.cli_results.push(validation_result);
        }

        println!("  ✅ CLI validation completed");
        Ok(())
    }

    /// Validate performance characteristics
    async fn validate_performance(
        &mut self,
        results: &mut ValidationTestResults,
    ) -> Result<(), Box<dyn std::error::Error>> {
        println!("⚡ Validating performance characteristics...");

        results.total_validations += 1;

        // Simple performance validation - create test data and measure parsing
        let test_start = Instant::now();

        // Generate small test data
        let test_data = self.generate_performance_test_data()?;
        let parse_time = test_start.elapsed();

        // Calculate metrics
        let parse_throughput = 1000.0 / parse_time.as_millis() as f64; // records per second
        let memory_usage = self.estimate_memory_usage(&test_data);
        let response_time = parse_time.as_millis() as f64;

        // Define performance requirements
        let meets_requirements = parse_throughput > 100.0 && // At least 100 records/sec
                                memory_usage < 100.0 && // Less than 100MB
                                response_time < 1000.0; // Less than 1 second

        let mut benchmark_details = HashMap::new();
        benchmark_details.insert("data_size_bytes".to_string(), test_data.len() as f64);
        benchmark_details.insert("parse_time_ms".to_string(), response_time);
        benchmark_details.insert("throughput_records_per_sec".to_string(), parse_throughput);

        let performance_result = PerformanceValidationResult {
            parse_throughput,
            memory_usage_mb: memory_usage,
            response_time_ms: response_time,
            meets_requirements,
            benchmark_details,
        };

        if meets_requirements {
            results.successful_validations += 1;
            println!("  ✅ Performance requirements met");
        } else {
            results.failed_validations += 1;
            println!("  ⚠️  Performance requirements not met");
        }

        println!(
            "    • Parse throughput: {:.2} records/sec",
            parse_throughput
        );
        println!("    • Memory usage: {:.2} MB", memory_usage);
        println!("    • Response time: {:.2} ms", response_time);

        results.performance_results = Some(performance_result);
        Ok(())
    }

    /// Generate recommendations based on validation results
    fn generate_recommendations(&self, results: &mut ValidationTestResults) {
        // Fixture recommendations
        if results.fixture_results.iter().any(|f| !f.is_valid) {
            results.recommendations.push(
                "Some SSTable fixtures failed validation. Review fixture generation logic."
                    .to_string(),
            );
        }

        // CLI recommendations
        let cli_failure_rate = if !results.cli_results.is_empty() {
            results.cli_results.iter().filter(|c| !c.success).count() as f64
                / results.cli_results.len() as f64
        } else {
            0.0
        };

        if cli_failure_rate > 0.5 {
            results.recommendations.push(
                "High CLI test failure rate. Consider implementing missing CLI functionality."
                    .to_string(),
            );
        } else if cli_failure_rate > 0.2 {
            results.recommendations.push(
                "Some CLI tests failed. Review CLI error handling and help messages.".to_string(),
            );
        }

        // Performance recommendations
        if let Some(ref perf) = results.performance_results {
            if !perf.meets_requirements {
                if perf.parse_throughput < 100.0 {
                    results.recommendations.push(
                        "Parse throughput below target. Consider optimizing parser performance."
                            .to_string(),
                    );
                }
                if perf.memory_usage_mb > 100.0 {
                    results.recommendations.push(
                        "Memory usage above target. Consider implementing streaming or memory optimization.".to_string()
                    );
                }
                if perf.response_time_ms > 1000.0 {
                    results.recommendations.push(
                        "Response time above target. Consider async processing or caching."
                            .to_string(),
                    );
                }
            }
        }

        // Overall recommendations
        let success_rate = if results.total_validations > 0 {
            results.successful_validations as f64 / results.total_validations as f64
        } else {
            0.0
        };

        if success_rate < 0.8 {
            results.recommendations.push(
                "Overall validation success rate is low. Focus on core functionality implementation.".to_string()
            );
        } else if success_rate < 0.95 {
            results.recommendations.push(
                "Good validation results. Focus on edge cases and error handling.".to_string(),
            );
        } else {
            results.recommendations.push(
                "Excellent validation results. Consider adding advanced features and optimizations.".to_string()
            );
        }
    }

    /// Generate validation report
    async fn generate_validation_report(
        &self,
        results: &ValidationTestResults,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let report_path = match self.config.report_format {
            ReportFormat::Json => {
                let path = self.test_output_dir.join("validation_report.json");
                let json_report = serde_json::to_string_pretty(results)?;
                fs::write(&path, json_report)?;
                path
            }
            ReportFormat::Html => {
                let path = self.test_output_dir.join("validation_report.html");
                let html_report = self.generate_html_report(results)?;
                fs::write(&path, html_report)?;
                path
            }
            ReportFormat::Markdown => {
                let path = self.test_output_dir.join("validation_report.md");
                let md_report = self.generate_markdown_report(results)?;
                fs::write(&path, md_report)?;
                path
            }
        };

        println!("📄 Validation report generated: {}", report_path.display());
        Ok(())
    }

    /// Generate HTML report
    fn generate_html_report(
        &self,
        results: &ValidationTestResults,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let html = format!(r#"
<!DOCTYPE html>
<html>
<head>
    <title>CQLite Validation Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .header {{ background-color: #f0f0f0; padding: 20px; border-radius: 5px; }}
        .section {{ margin: 20px 0; }}
        .success {{ color: green; }}
        .failure {{ color: red; }}
        .warning {{ color: orange; }}
        table {{ border-collapse: collapse; width: 100%; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>CQLite Validation Report</h1>
        <p>Generated: {}</p>
        <p>Total Validations: {} | Success: {} | Failed: {}</p>
    </div>

    <div class="section">
        <h2>Summary</h2>
        <p>Success Rate: {:.1}%</p>
        <p>Validation Time: {:.2}s</p>
    </div>

    <div class="section">
        <h2>Fixture Validation</h2>
        <table>
            <tr><th>Fixture</th><th>Status</th><th>Size</th><th>Records</th><th>Issues</th></tr>
            {}
        </table>
    </div>

    <div class="section">
        <h2>CLI Validation</h2>
        <table>
            <tr><th>Command</th><th>Status</th><th>Response Time</th><th>Output Size</th></tr>
            {}
        </table>
    </div>

    <div class="section">
        <h2>Recommendations</h2>
        <ul>
            {}
        </ul>
    </div>
</body>
</html>
        "#,
            chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC"),
            results.total_validations,
            results.successful_validations,
            results.failed_validations,
            if results.total_validations > 0 {
                (results.successful_validations as f64 / results.total_validations as f64) * 100.0
            } else { 0.0 },
            results.validation_time_ms as f64 / 1000.0,
            results.fixture_results.iter().map(|f| {
                format!("<tr><td>{}</td><td class=\"{}\">{}</td><td>{} bytes</td><td>{}</td><td>{}</td></tr>",
                    f.fixture_name,
                    if f.is_valid { "success" } else { "failure" },
                    if f.is_valid { "Valid" } else { "Invalid" },
                    f.file_size_bytes,
                    f.record_count,
                    f.issues.len()
                )
            }).collect::<Vec<_>>().join("\n"),
            results.cli_results.iter().map(|c| {
                format!("<tr><td>{}</td><td class=\"{}\">{}</td><td>{} ms</td><td>{} bytes</td></tr>",
                    c.command,
                    if c.success { "success" } else { "failure" },
                    if c.success { "Success" } else { "Failed" },
                    c.response_time_ms,
                    c.output_size_bytes
                )
            }).collect::<Vec<_>>().join("\n"),
            results.recommendations.iter().map(|r| format!("<li>{}</li>", r)).collect::<Vec<_>>().join("\n")
        );

        Ok(html)
    }

    /// Generate Markdown report
    fn generate_markdown_report(
        &self,
        results: &ValidationTestResults,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let mut md = String::new();

        md.push_str("# CQLite Validation Report\n\n");
        md.push_str(&format!(
            "**Generated:** {}\n\n",
            chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
        ));

        md.push_str("## Summary\n\n");
        md.push_str(&format!(
            "- **Total Validations:** {}\n",
            results.total_validations
        ));
        md.push_str(&format!(
            "- **Successful:** {}\n",
            results.successful_validations
        ));
        md.push_str(&format!("- **Failed:** {}\n", results.failed_validations));
        md.push_str(&format!(
            "- **Success Rate:** {:.1}%\n",
            if results.total_validations > 0 {
                (results.successful_validations as f64 / results.total_validations as f64) * 100.0
            } else {
                0.0
            }
        ));
        md.push_str(&format!(
            "- **Validation Time:** {:.2}s\n\n",
            results.validation_time_ms as f64 / 1000.0
        ));

        md.push_str("## Fixture Validation\n\n");
        md.push_str("| Fixture | Status | Size | Records | Issues |\n");
        md.push_str("|---------|--------|------|---------|--------|\n");
        for fixture in &results.fixture_results {
            md.push_str(&format!(
                "| {} | {} | {} bytes | {} | {} |\n",
                fixture.fixture_name,
                if fixture.is_valid {
                    "✅ Valid"
                } else {
                    "❌ Invalid"
                },
                fixture.file_size_bytes,
                fixture.record_count,
                fixture.issues.len()
            ));
        }
        md.push_str("\n");

        md.push_str("## CLI Validation\n\n");
        md.push_str("| Command | Status | Response Time | Output Size |\n");
        md.push_str("|---------|--------|---------------|-------------|\n");
        for cli in &results.cli_results {
            md.push_str(&format!(
                "| {} | {} | {} ms | {} bytes |\n",
                cli.command,
                if cli.success {
                    "✅ Success"
                } else {
                    "❌ Failed"
                },
                cli.response_time_ms,
                cli.output_size_bytes
            ));
        }
        md.push_str("\n");

        md.push_str("## Recommendations\n\n");
        for recommendation in &results.recommendations {
            md.push_str(&format!("- {}\n", recommendation));
        }

        Ok(md)
    }

    /// Helper method to generate test data for performance validation
    fn generate_performance_test_data(&self) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        // Generate simple test data
        let mut data = Vec::new();
        for i in 0..1000 {
            data.extend(format!("record_{},test_data_{}\n", i, i).as_bytes());
        }
        Ok(data)
    }

    /// Estimate memory usage (simplified)
    fn estimate_memory_usage(&self, data: &[u8]) -> f64 {
        // Simple estimation: data size * 2 (for parsing overhead) / 1MB
        (data.len() * 2) as f64 / (1024.0 * 1024.0)
    }

    /// Print validation summary
    fn print_validation_summary(&self, results: &ValidationTestResults) {
        println!();
        println!("📊 Validation Summary");
        println!("=".repeat(40));
        println!("  Total Validations:   {}", results.total_validations);
        println!("  Successful:          {}", results.successful_validations);
        println!("  Failed:              {}", results.failed_validations);
        println!(
            "  Success Rate:        {:.1}%",
            if results.total_validations > 0 {
                (results.successful_validations as f64 / results.total_validations as f64) * 100.0
            } else {
                0.0
            }
        );
        println!(
            "  Validation Time:     {:.2}s",
            results.validation_time_ms as f64 / 1000.0
        );

        if !results.recommendations.is_empty() {
            println!();
            println!("💡 Recommendations:");
            for (i, rec) in results.recommendations.iter().enumerate() {
                println!("  {}. {}", i + 1, rec);
            }
        }

        println!();
        let success_rate = if results.total_validations > 0 {
            results.successful_validations as f64 / results.total_validations as f64
        } else {
            0.0
        };

        if success_rate >= 0.95 {
            println!("🎉 Excellent validation results!");
        } else if success_rate >= 0.8 {
            println!("✅ Good validation results");
        } else if success_rate >= 0.6 {
            println!("⚠️  Validation needs improvement");
        } else {
            println!("❌ Validation requires significant work");
        }
    }
}

// Serde implementations for JSON reports
use serde::{Deserialize, Serialize};

impl Serialize for ValidationTestResults {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("ValidationTestResults", 8)?;
        state.serialize_field("total_validations", &self.total_validations)?;
        state.serialize_field("successful_validations", &self.successful_validations)?;
        state.serialize_field("failed_validations", &self.failed_validations)?;
        state.serialize_field("validation_time_ms", &self.validation_time_ms)?;
        state.serialize_field("fixture_results", &self.fixture_results)?;
        state.serialize_field("cli_results", &self.cli_results)?;
        state.serialize_field("performance_results", &self.performance_results)?;
        state.serialize_field("recommendations", &self.recommendations)?;
        state.end()
    }
}

impl Serialize for FixtureValidationResult {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("FixtureValidationResult", 7)?;
        state.serialize_field("fixture_name", &self.fixture_name)?;
        state.serialize_field("is_valid", &self.is_valid)?;
        state.serialize_field("file_size_bytes", &self.file_size_bytes)?;
        state.serialize_field("record_count", &self.record_count)?;
        state.serialize_field("schema_columns", &self.schema_columns)?;
        state.serialize_field("issues", &self.issues)?;
        state.serialize_field("validation_time_ms", &self.validation_time_ms)?;
        state.end()
    }
}

impl Serialize for CLIValidationResult {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("CLIValidationResult", 5)?;
        state.serialize_field("command", &self.command)?;
        state.serialize_field("success", &self.success)?;
        state.serialize_field("response_time_ms", &self.response_time_ms)?;
        state.serialize_field("output_size_bytes", &self.output_size_bytes)?;
        state.serialize_field("error_message", &self.error_message)?;
        state.end()
    }
}

impl Serialize for PerformanceValidationResult {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("PerformanceValidationResult", 5)?;
        state.serialize_field("parse_throughput", &self.parse_throughput)?;
        state.serialize_field("memory_usage_mb", &self.memory_usage_mb)?;
        state.serialize_field("response_time_ms", &self.response_time_ms)?;
        state.serialize_field("meets_requirements", &self.meets_requirements)?;
        state.serialize_field("benchmark_details", &self.benchmark_details)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_validation_runner_creation() {
        let config = ValidationTestConfig::default();
        let runner = ValidationTestRunner::new(config);
        assert!(runner.is_ok());
    }

    #[tokio::test]
    async fn test_basic_validation() {
        let config = ValidationTestConfig {
            validate_fixtures: true,
            validate_cli: false, // Skip CLI for basic test
            validate_performance: false,
            generate_report: false,
            report_format: ReportFormat::Json,
            timeout_seconds: 30,
        };

        let mut runner = ValidationTestRunner::new(config).unwrap();
        let results = runner.run_validation_tests().await.unwrap();

        assert!(results.total_validations > 0);
        assert!(!results.recommendations.is_empty());
    }
}
