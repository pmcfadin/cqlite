//! CQL Schema Validation Test Runner
//!
//! Comprehensive test runner for CQL parser validation, integration tests,
//! and performance benchmarks. Coordinates all validation agents and 
//! generates consolidated reports.

use cqlite_tests::cql_parser_validation_suite::{CqlParserValidationSuite, ValidationReport};
use cqlite_tests::cql_integration_tests::{CqlIntegrationTestSuite, IntegrationTestReport};
use cqlite_tests::cql_performance_benchmarks::{CqlPerformanceBenchmarkSuite, BenchmarkReport};
use std::fs;
use std::path::Path;
use std::time::Instant;
use clap::{App, Arg, ArgMatches};

/// Main test runner orchestrating all CQL validation tests
#[derive(Debug)]
pub struct CqlValidationTestRunner {
    /// Configuration for test execution
    config: TestRunnerConfig,
    /// Start time for overall execution tracking
    start_time: Instant,
}

/// Configuration for the test runner
#[derive(Debug, Clone)]
pub struct TestRunnerConfig {
    /// Run validation suite tests
    pub run_validation_suite: bool,
    /// Run integration tests
    pub run_integration_tests: bool,
    /// Run performance benchmarks
    pub run_performance_benchmarks: bool,
    /// Output directory for reports
    pub output_dir: String,
    /// Verbose output
    pub verbose: bool,
    /// Generate HTML reports
    pub generate_html: bool,
    /// Performance benchmark iterations override
    pub benchmark_iterations: Option<usize>,
    /// Test timeout in seconds
    pub timeout_seconds: u64,
}

/// Consolidated test results from all suites
#[derive(Debug)]
pub struct ConsolidatedTestResults {
    pub validation_report: Option<ValidationReport>,
    pub integration_report: Option<IntegrationTestReport>,
    pub benchmark_report: Option<BenchmarkReport>,
    pub total_execution_time_ms: u64,
    pub overall_success: bool,
}

impl Default for TestRunnerConfig {
    fn default() -> Self {
        Self {
            run_validation_suite: true,
            run_integration_tests: true,
            run_performance_benchmarks: true,
            output_dir: "target/test_reports".to_string(),
            verbose: false,
            generate_html: false,
            benchmark_iterations: None,
            timeout_seconds: 300, // 5 minutes default timeout
        }
    }
}

impl CqlValidationTestRunner {
    /// Create new test runner with configuration
    pub fn new(config: TestRunnerConfig) -> Self {
        Self {
            config,
            start_time: Instant::now(),
        }
    }

    /// Run all configured test suites
    pub async fn run_all_tests(&mut self) -> Result<ConsolidatedTestResults, Box<dyn std::error::Error>> {
        println!("🚀 Starting CQL Schema Validation Test Runner");
        println!("=" .repeat(60));
        
        // Create output directory
        fs::create_dir_all(&self.config.output_dir)?;
        
        let mut results = ConsolidatedTestResults {
            validation_report: None,
            integration_report: None,
            benchmark_report: None,
            total_execution_time_ms: 0,
            overall_success: true,
        };
        
        // Run validation suite
        if self.config.run_validation_suite {
            println!("\n🧪 Phase 1: Running CQL Parser Validation Suite");
            println!("-" .repeat(40));
            
            match self.run_validation_suite().await {
                Ok(report) => {
                    let success = report.failed_tests == 0;
                    results.overall_success &= success;
                    results.validation_report = Some(report);
                    
                    if success {
                        println!("✅ Validation suite completed successfully");
                    } else {
                        println!("❌ Validation suite had failures");
                    }
                }
                Err(e) => {
                    eprintln!("❌ Validation suite failed to run: {}", e);
                    results.overall_success = false;
                }
            }
        }
        
        // Run integration tests
        if self.config.run_integration_tests {
            println!("\n🔗 Phase 2: Running CQL Integration Tests");
            println!("-" .repeat(40));
            
            match self.run_integration_tests().await {
                Ok(report) => {
                    let success = report.failed_tests == 0;
                    results.overall_success &= success;
                    results.integration_report = Some(report);
                    
                    if success {
                        println!("✅ Integration tests completed successfully");
                    } else {
                        println!("❌ Integration tests had failures");
                    }
                }
                Err(e) => {
                    eprintln!("❌ Integration tests failed to run: {}", e);
                    results.overall_success = false;
                }
            }
        }
        
        // Run performance benchmarks
        if self.config.run_performance_benchmarks {
            println!("\n⚡ Phase 3: Running CQL Performance Benchmarks");
            println!("-" .repeat(40));
            
            match self.run_performance_benchmarks().await {
                Ok(report) => {
                    let success = report.failed_benchmarks == 0;
                    results.overall_success &= success;
                    results.benchmark_report = Some(report);
                    
                    if success {
                        println!("✅ Performance benchmarks completed successfully");
                    } else {
                        println!("❌ Performance benchmarks had failures");
                    }
                }
                Err(e) => {
                    eprintln!("❌ Performance benchmarks failed to run: {}", e);
                    results.overall_success = false;
                }
            }
        }
        
        results.total_execution_time_ms = self.start_time.elapsed().as_millis() as u64;
        
        // Generate reports
        self.generate_reports(&results).await?;
        
        // Print final summary
        self.print_final_summary(&results);
        
        Ok(results)
    }

    /// Run the validation suite
    async fn run_validation_suite(&self) -> Result<ValidationReport, Box<dyn std::error::Error>> {
        let mut suite = CqlParserValidationSuite::new();
        
        let report = suite.run_all_tests()?;
        
        if self.config.verbose {
            report.print_report();
        }
        
        Ok(report)
    }

    /// Run integration tests
    async fn run_integration_tests(&self) -> Result<IntegrationTestReport, Box<dyn std::error::Error>> {
        let mut suite = CqlIntegrationTestSuite::new().await?;
        
        let report = suite.run_all_tests().await?;
        
        if self.config.verbose {
            report.print_report();
        }
        
        Ok(report)
    }

    /// Run performance benchmarks
    async fn run_performance_benchmarks(&self) -> Result<BenchmarkReport, Box<dyn std::error::Error>> {
        let mut suite = CqlPerformanceBenchmarkSuite::new();
        
        let report = suite.run_all_benchmarks()?;
        
        if self.config.verbose {
            report.print_report();
        }
        
        Ok(report)
    }

    /// Generate all reports (JSON, HTML, etc.)
    async fn generate_reports(&self, results: &ConsolidatedTestResults) -> Result<(), Box<dyn std::error::Error>> {
        println!("\n📊 Generating Test Reports");
        println!("-" .repeat(30));
        
        // Generate JSON reports
        if let Some(validation_report) = &results.validation_report {
            let json_path = Path::new(&self.config.output_dir).join("validation_report.json");
            validation_report.save_to_file(&json_path)?;
            println!("📄 Validation report saved to: {}", json_path.display());
        }
        
        if let Some(integration_report) = &results.integration_report {
            let json_path = Path::new(&self.config.output_dir).join("integration_report.json");
            let json = serde_json::to_string_pretty(integration_report)?;
            fs::write(&json_path, json)?;
            println!("📄 Integration report saved to: {}", json_path.display());
        }
        
        if let Some(benchmark_report) = &results.benchmark_report {
            let json_path = Path::new(&self.config.output_dir).join("benchmark_report.json");
            benchmark_report.save_to_file(&json_path)?;
            println!("📄 Benchmark report saved to: {}", json_path.display());
        }
        
        // Generate consolidated report
        let consolidated_path = Path::new(&self.config.output_dir).join("consolidated_report.json");
        let consolidated_json = serde_json::to_string_pretty(&ConsolidatedReportData {
            validation_summary: results.validation_report.as_ref().map(|r| ValidationSummary {
                total_tests: r.total_tests,
                passed_tests: r.passed_tests,
                failed_tests: r.failed_tests,
                execution_time_ms: r.total_execution_time_ms,
            }),
            integration_summary: results.integration_report.as_ref().map(|r| IntegrationSummary {
                total_tests: r.total_tests,
                passed_tests: r.passed_tests,
                failed_tests: r.failed_tests,
                execution_time_ms: r.total_execution_time_ms,
                schemas_validated: r.total_schemas_validated,
            }),
            benchmark_summary: results.benchmark_report.as_ref().map(|r| BenchmarkSummary {
                total_benchmarks: r.total_benchmarks,
                passed_benchmarks: r.passed_benchmarks,
                failed_benchmarks: r.failed_benchmarks,
                total_iterations: r.total_iterations,
                execution_time_ms: r.total_execution_time_ms,
                peak_memory_kb: r.peak_memory_usage_kb,
            }),
            overall_success: results.overall_success,
            total_execution_time_ms: results.total_execution_time_ms,
            timestamp: chrono::Utc::now().to_rfc3339(),
        })?;
        fs::write(&consolidated_path, consolidated_json)?;
        println!("📄 Consolidated report saved to: {}", consolidated_path.display());
        
        // Generate HTML report if requested
        if self.config.generate_html {
            self.generate_html_report(results).await?;
        }
        
        Ok(())
    }

    /// Generate HTML report
    async fn generate_html_report(&self, results: &ConsolidatedTestResults) -> Result<(), Box<dyn std::error::Error>> {
        let html_content = self.generate_html_content(results);
        let html_path = Path::new(&self.config.output_dir).join("test_report.html");
        fs::write(&html_path, html_content)?;
        println!("🌐 HTML report saved to: {}", html_path.display());
        Ok(())
    }

    /// Generate HTML content for the report
    fn generate_html_content(&self, results: &ConsolidatedTestResults) -> String {
        let mut html = String::new();
        
        html.push_str(r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>CQL Schema Validation Test Report</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .header { text-align: center; color: #333; border-bottom: 2px solid #007acc; padding-bottom: 20px; margin-bottom: 30px; }
        .summary { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin-bottom: 30px; }
        .summary-box { background: #f8f9fa; padding: 20px; border-radius: 6px; border-left: 4px solid #007acc; }
        .success { border-left-color: #28a745; }
        .failure { border-left-color: #dc3545; }
        .metric { font-size: 24px; font-weight: bold; color: #333; }
        .metric-label { font-size: 14px; color: #666; margin-top: 5px; }
        .section { margin-bottom: 30px; }
        .section h2 { color: #333; border-bottom: 1px solid #dee2e6; padding-bottom: 10px; }
        table { width: 100%; border-collapse: collapse; margin-top: 15px; }
        th, td { padding: 12px; text-align: left; border-bottom: 1px solid #dee2e6; }
        th { background-color: #f8f9fa; font-weight: 600; }
        .pass { color: #28a745; font-weight: bold; }
        .fail { color: #dc3545; font-weight: bold; }
        .footer { text-align: center; margin-top: 40px; padding-top: 20px; border-top: 1px solid #dee2e6; color: #666; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🧪 CQL Schema Validation Test Report</h1>
            <p>Generated on "#);
        
        html.push_str(&chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC").to_string());
        html.push_str("</p>\n        </div>\n\n        <div class=\"summary\">\n");
        
        // Overall summary
        let overall_class = if results.overall_success { "success" } else { "failure" };
        html.push_str(&format!(r#"            <div class="summary-box {}">
                <div class="metric">{}</div>
                <div class="metric-label">Overall Status</div>
            </div>
            <div class="summary-box">
                <div class="metric">{:.2}s</div>
                <div class="metric-label">Total Execution Time</div>
            </div>"#, 
            overall_class,
            if results.overall_success { "✅ PASSED" } else { "❌ FAILED" },
            results.total_execution_time_ms as f64 / 1000.0
        ));
        
        // Add summaries for each test type
        if let Some(validation_report) = &results.validation_report {
            let validation_class = if validation_report.failed_tests == 0 { "success" } else { "failure" };
            html.push_str(&format!(r#"            <div class="summary-box {}">
                <div class="metric">{}/{}</div>
                <div class="metric-label">Validation Tests Passed</div>
            </div>"#, 
                validation_class,
                validation_report.passed_tests,
                validation_report.total_tests
            ));
        }
        
        if let Some(integration_report) = &results.integration_report {
            let integration_class = if integration_report.failed_tests == 0 { "success" } else { "failure" };
            html.push_str(&format!(r#"            <div class="summary-box {}">
                <div class="metric">{}/{}</div>
                <div class="metric-label">Integration Tests Passed</div>
            </div>"#, 
                integration_class,
                integration_report.passed_tests,
                integration_report.total_tests
            ));
        }
        
        if let Some(benchmark_report) = &results.benchmark_report {
            let benchmark_class = if benchmark_report.failed_benchmarks == 0 { "success" } else { "failure" };
            html.push_str(&format!(r#"            <div class="summary-box {}">
                <div class="metric">{}/{}</div>
                <div class="metric-label">Benchmarks Passed</div>
            </div>
            <div class="summary-box">
                <div class="metric">{:.1} MB</div>
                <div class="metric-label">Peak Memory Usage</div>
            </div>"#, 
                benchmark_class,
                benchmark_report.passed_benchmarks,
                benchmark_report.total_benchmarks,
                benchmark_report.peak_memory_usage_kb as f64 / 1024.0
            ));
        }
        
        html.push_str("        </div>\n\n");
        
        // Detailed sections for each test type
        if let Some(validation_report) = &results.validation_report {
            html.push_str(&self.generate_validation_section(validation_report));
        }
        
        if let Some(integration_report) = &results.integration_report {
            html.push_str(&self.generate_integration_section(integration_report));
        }
        
        if let Some(benchmark_report) = &results.benchmark_report {
            html.push_str(&self.generate_benchmark_section(benchmark_report));
        }
        
        html.push_str(r#"        <div class="footer">
            <p>Report generated by CQL Schema Validation Test Runner</p>
        </div>
    </div>
</body>
</html>"#);
        
        html
    }

    /// Generate validation section for HTML report
    fn generate_validation_section(&self, report: &ValidationReport) -> String {
        let mut html = String::from(r#"        <div class="section">
            <h2>🧪 Validation Test Results</h2>
            <table>
                <thead>
                    <tr>
                        <th>Test Name</th>
                        <th>Status</th>
                        <th>Execution Time</th>
                        <th>Bytes Processed</th>
                        <th>Error Message</th>
                    </tr>
                </thead>
                <tbody>"#);
        
        let mut sorted_results: Vec<_> = report.test_results.values().collect();
        sorted_results.sort_by_key(|r| &r.test_name);
        
        for result in sorted_results {
            let status_class = if result.passed { "pass" } else { "fail" };
            let status_text = if result.passed { "✅ PASS" } else { "❌ FAIL" };
            let error_msg = result.error_message.as_deref().unwrap_or("");
            
            html.push_str(&format!(r#"                    <tr>
                        <td>{}</td>
                        <td class="{}">{}</td>
                        <td>{}ms</td>
                        <td>{}</td>
                        <td>{}</td>
                    </tr>"#,
                result.test_name,
                status_class,
                status_text,
                result.execution_time_ms,
                result.bytes_processed,
                error_msg
            ));
        }
        
        html.push_str("                </tbody>\n            </table>\n        </div>\n\n");
        html
    }

    /// Generate integration section for HTML report
    fn generate_integration_section(&self, report: &IntegrationTestReport) -> String {
        let mut html = String::from(r#"        <div class="section">
            <h2>🔗 Integration Test Results</h2>
            <table>
                <thead>
                    <tr>
                        <th>Test Name</th>
                        <th>Status</th>
                        <th>Execution Time</th>
                        <th>Files Processed</th>
                        <th>Schemas Validated</th>
                        <th>Error Message</th>
                    </tr>
                </thead>
                <tbody>"#);
        
        let mut sorted_results: Vec<_> = report.test_results.values().collect();
        sorted_results.sort_by_key(|r| &r.test_name);
        
        for result in sorted_results {
            let status_class = if result.passed { "pass" } else { "fail" };
            let status_text = if result.passed { "✅ PASS" } else { "❌ FAIL" };
            let error_msg = result.error_message.as_deref().unwrap_or("");
            
            html.push_str(&format!(r#"                    <tr>
                        <td>{}</td>
                        <td class="{}">{}</td>
                        <td>{}ms</td>
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                    </tr>"#,
                result.test_name,
                status_class,
                status_text,
                result.execution_time_ms,
                result.files_processed,
                result.schemas_validated,
                error_msg
            ));
        }
        
        html.push_str("                </tbody>\n            </table>\n        </div>\n\n");
        html
    }

    /// Generate benchmark section for HTML report
    fn generate_benchmark_section(&self, report: &BenchmarkReport) -> String {
        let mut html = String::from(r#"        <div class="section">
            <h2>⚡ Performance Benchmark Results</h2>
            <table>
                <thead>
                    <tr>
                        <th>Benchmark Name</th>
                        <th>Status</th>
                        <th>Iterations</th>
                        <th>Throughput (ops/sec)</th>
                        <th>Avg Latency (ms)</th>
                        <th>Memory (KB)</th>
                        <th>Error Message</th>
                    </tr>
                </thead>
                <tbody>"#);
        
        let mut sorted_results: Vec<_> = report.benchmark_results.values().collect();
        sorted_results.sort_by_key(|r| &r.benchmark_name);
        
        for result in sorted_results {
            let status_class = if result.passed_performance_targets { "pass" } else { "fail" };
            let status_text = if result.passed_performance_targets { "✅ PASS" } else { "❌ FAIL" };
            let error_msg = result.error_message.as_deref().unwrap_or("");
            
            html.push_str(&format!(r#"                    <tr>
                        <td>{}</td>
                        <td class="{}">{}</td>
                        <td>{}</td>
                        <td>{:.1}</td>
                        <td>{:.2}</td>
                        <td>{}</td>
                        <td>{}</td>
                    </tr>"#,
                result.benchmark_name,
                status_class,
                status_text,
                result.iterations,
                result.throughput_ops_per_sec,
                result.avg_latency_ms,
                result.memory_usage_kb,
                error_msg
            ));
        }
        
        html.push_str("                </tbody>\n            </table>\n        </div>\n\n");
        html
    }

    /// Print final summary to console
    fn print_final_summary(&self, results: &ConsolidatedTestResults) {
        println!("\n🎯 Final Test Summary");
        println!("=" .repeat(60));
        println!("Total Execution Time: {:.2}s", results.total_execution_time_ms as f64 / 1000.0);
        
        if let Some(validation_report) = &results.validation_report {
            println!("Validation Tests: {}/{} passed", 
                    validation_report.passed_tests, validation_report.total_tests);
        }
        
        if let Some(integration_report) = &results.integration_report {
            println!("Integration Tests: {}/{} passed", 
                    integration_report.passed_tests, integration_report.total_tests);
        }
        
        if let Some(benchmark_report) = &results.benchmark_report {
            println!("Performance Benchmarks: {}/{} passed", 
                    benchmark_report.passed_benchmarks, benchmark_report.total_benchmarks);
        }
        
        println!("=" .repeat(60));
        
        if results.overall_success {
            println!("🎉 ALL TESTS PASSED! CQL schema validation is working correctly.");
        } else {
            println!("❌ SOME TESTS FAILED. Review the reports for details.");
            std::process::exit(1);
        }
    }
}

// Data structures for JSON serialization
#[derive(serde::Serialize)]
struct ConsolidatedReportData {
    validation_summary: Option<ValidationSummary>,
    integration_summary: Option<IntegrationSummary>,
    benchmark_summary: Option<BenchmarkSummary>,
    overall_success: bool,
    total_execution_time_ms: u64,
    timestamp: String,
}

#[derive(serde::Serialize)]
struct ValidationSummary {
    total_tests: usize,
    passed_tests: usize,
    failed_tests: usize,
    execution_time_ms: u64,
}

#[derive(serde::Serialize)]
struct IntegrationSummary {
    total_tests: usize,
    passed_tests: usize,
    failed_tests: usize,
    execution_time_ms: u64,
    schemas_validated: usize,
}

#[derive(serde::Serialize)]
struct BenchmarkSummary {
    total_benchmarks: usize,
    passed_benchmarks: usize,
    failed_benchmarks: usize,
    total_iterations: usize,
    execution_time_ms: u64,
    peak_memory_kb: usize,
}

/// Parse command line arguments
fn parse_args() -> ArgMatches<'static> {
    App::new("CQL Validation Test Runner")
        .version("1.0")
        .about("Comprehensive test runner for CQL schema validation")
        .arg(Arg::with_name("validation")
            .long("validation")
            .help("Run validation suite tests")
            .takes_value(false))
        .arg(Arg::with_name("integration")
            .long("integration")
            .help("Run integration tests")
            .takes_value(false))
        .arg(Arg::with_name("benchmarks")
            .long("benchmarks")
            .help("Run performance benchmarks")
            .takes_value(false))
        .arg(Arg::with_name("output")
            .short("o")
            .long("output")
            .value_name("DIR")
            .help("Output directory for reports")
            .takes_value(true))
        .arg(Arg::with_name("verbose")
            .short("v")
            .long("verbose")
            .help("Verbose output")
            .takes_value(false))
        .arg(Arg::with_name("html")
            .long("html")
            .help("Generate HTML reports")
            .takes_value(false))
        .arg(Arg::with_name("timeout")
            .long("timeout")
            .value_name("SECONDS")
            .help("Test timeout in seconds")
            .takes_value(true))
        .get_matches()
}

/// Create test runner configuration from command line arguments
fn create_config_from_args(matches: &ArgMatches) -> TestRunnerConfig {
    let run_all = !matches.is_present("validation") && 
                  !matches.is_present("integration") && 
                  !matches.is_present("benchmarks");
    
    TestRunnerConfig {
        run_validation_suite: run_all || matches.is_present("validation"),
        run_integration_tests: run_all || matches.is_present("integration"),
        run_performance_benchmarks: run_all || matches.is_present("benchmarks"),
        output_dir: matches.value_of("output").unwrap_or("target/test_reports").to_string(),
        verbose: matches.is_present("verbose"),
        generate_html: matches.is_present("html"),
        benchmark_iterations: None,
        timeout_seconds: matches.value_of("timeout")
            .and_then(|s| s.parse().ok())
            .unwrap_or(300),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse command line arguments
    let matches = parse_args();
    let config = create_config_from_args(&matches);
    
    // Create and run test runner
    let mut runner = CqlValidationTestRunner::new(config);
    let results = runner.run_all_tests().await?;
    
    // Exit with appropriate code
    if results.overall_success {
        std::process::exit(0);
    } else {
        std::process::exit(1);
    }
}