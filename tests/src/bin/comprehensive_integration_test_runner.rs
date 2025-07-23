//! Comprehensive Integration Test Runner
//!
//! Standalone executable for running the complete CQLite integration test suite.
//! This runner provides a CI/CD friendly interface with clear pass/fail results.

use cqlite_tests::comprehensive_integration_test_suite::{
    run_comprehensive_integration_tests, run_quick_integration_tests,
    print_integration_test_results, IntegrationTestConfig, ComprehensiveIntegrationTestSuite
};
use std::env;
use std::path::PathBuf;
use std::process;
use tokio;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    
    println!("🧪 CQLite Comprehensive Integration Test Runner");
    println!("{}", "=".repeat(60));
    
    // Parse command line arguments
    let mut test_mode = "full";
    let mut test_data_path = PathBuf::from("test-env/cassandra5/sstables");
    let mut timeout_seconds = 300u64;
    let mut fail_fast = false;
    let mut detailed_reporting = true;
    
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--mode" | "-m" => {
                if i + 1 < args.len() {
                    test_mode = &args[i + 1];
                    i += 2;
                } else {
                    eprintln!("Error: --mode requires a value (full, quick, real-only, performance-only)");
                    process::exit(1);
                }
            }
            "--test-data" | "-d" => {
                if i + 1 < args.len() {
                    test_data_path = PathBuf::from(&args[i + 1]);
                    i += 2;
                } else {
                    eprintln!("Error: --test-data requires a path");
                    process::exit(1);
                }
            }
            "--timeout" | "-t" => {
                if i + 1 < args.len() {
                    timeout_seconds = args[i + 1].parse().unwrap_or(300);
                    i += 2;
                } else {
                    eprintln!("Error: --timeout requires a value in seconds");
                    process::exit(1);
                }
            }
            "--fail-fast" | "-f" => {
                fail_fast = true;
                i += 1;
            }
            "--brief" | "-b" => {
                detailed_reporting = false;
                i += 1;
            }
            "--help" | "-h" => {
                print_help();
                process::exit(0);
            }
            _ => {
                eprintln!("Unknown argument: {}", args[i]);
                print_help();
                process::exit(1);
            }
        }
    }

    println!("🎯 Test Mode: {}", test_mode);
    println!("📁 Test Data: {}", test_data_path.display());
    println!("⏱️  Timeout: {} seconds", timeout_seconds);
    println!("🚨 Fail Fast: {}", fail_fast);
    println!();

    // Validate test data path
    if !test_data_path.exists() {
        eprintln!("❌ Error: Test data path does not exist: {}", test_data_path.display());
        eprintln!("💡 Hint: Run './test-env/cassandra5/manage.sh all' to generate test data");
        process::exit(1);
    }

    // Run tests based on mode
    let start_time = std::time::Instant::now();
    let test_results = match test_mode {
        "quick" => {
            println!("🏃 Running quick integration tests...");
            run_quick_integration_tests().await
        }
        "full" => {
            println!("🔬 Running comprehensive integration tests...");
            run_comprehensive_integration_tests().await
        }
        "real-only" => {
            println!("📂 Running real SSTable tests only...");
            let config = IntegrationTestConfig {
                test_real_sstables: true,
                test_feature_integration: false,
                test_error_handling: false,
                test_performance: false,
                test_cli_commands: false,
                test_multi_generation: false,
                test_collection_types: false,
                test_tombstones: false,
                test_directory_scanning: true,
                stress_test_enabled: false,
                detailed_reporting,
                fail_fast,
                test_data_path,
                timeout_seconds,
            };
            let mut suite = ComprehensiveIntegrationTestSuite::new(config);
            suite.run_all_tests().await
        }
        "performance-only" => {
            println!("🚀 Running performance tests only...");
            let config = IntegrationTestConfig {
                test_real_sstables: false,
                test_feature_integration: false,
                test_error_handling: false,
                test_performance: true,
                test_cli_commands: false,
                test_multi_generation: false,
                test_collection_types: false,
                test_tombstones: false,
                test_directory_scanning: false,
                stress_test_enabled: true,
                detailed_reporting,
                fail_fast,
                test_data_path,
                timeout_seconds,
            };
            let mut suite = ComprehensiveIntegrationTestSuite::new(config);
            suite.run_all_tests().await
        }
        "collections-only" => {
            println!("📋 Running collection type tests only...");
            let config = IntegrationTestConfig {
                test_real_sstables: false,
                test_feature_integration: false,
                test_error_handling: false,
                test_performance: false,
                test_cli_commands: false,
                test_multi_generation: false,
                test_collection_types: true,
                test_tombstones: false,
                test_directory_scanning: false,
                stress_test_enabled: false,
                detailed_reporting,
                fail_fast,
                test_data_path,
                timeout_seconds,
            };
            let mut suite = ComprehensiveIntegrationTestSuite::new(config);
            suite.run_all_tests().await
        }
        _ => {
            eprintln!("❌ Error: Unknown test mode: {}", test_mode);
            eprintln!("💡 Valid modes: full, quick, real-only, performance-only, collections-only");
            process::exit(1);
        }
    };

    let total_time = start_time.elapsed();

    // Process results
    match test_results {
        Ok(results) => {
            println!("\n⏱️  Total execution time: {:.2}s", total_time.as_secs_f64());
            print_integration_test_results(&results);
            
            // Determine exit code based on results
            let success_threshold = 0.8; // 80% compatibility required for success
            let exit_code = if results.overall_compatibility_score >= success_threshold && results.failed_tests == 0 {
                println!("\n🎉 ALL TESTS PASSED - CQLite is compatible with Cassandra 5+");
                0
            } else if results.overall_compatibility_score >= success_threshold {
                println!("\n⚠️  TESTS COMPLETED WITH MINOR ISSUES - Review failed tests");
                1
            } else if !results.critical_issues.is_empty() {
                println!("\n🚨 CRITICAL ISSUES FOUND - Address before production use");
                2
            } else {
                println!("\n❌ COMPATIBILITY BELOW THRESHOLD - Significant work needed");
                1
            };

            // Print CI/CD friendly summary
            println!("\n📊 CI/CD Summary:");
            println!("  • Exit Code: {}", exit_code);
            println!("  • Success Rate: {:.1}%", 
                    (results.passed_tests as f64 / results.total_tests as f64) * 100.0);
            println!("  • Compatibility Score: {:.3}", results.overall_compatibility_score);
            println!("  • Performance Score: {:.3}", results.overall_performance_score);
            println!("  • Total Data Processed: {:.2} MB", 
                    results.total_bytes_processed as f64 / 1024.0 / 1024.0);
            println!("  • Files Tested: {}", results.total_files_tested);

            // Generate machine-readable results for CI/CD
            if let Err(e) = generate_ci_results(&results) {
                eprintln!("⚠️  Warning: Could not generate CI results file: {}", e);
            }

            process::exit(exit_code);
        }
        Err(e) => {
            eprintln!("💥 INTEGRATION TESTS FAILED: {}", e);
            eprintln!("⏱️  Execution time: {:.2}s", total_time.as_secs_f64());
            
            // Check if it's a timeout
            if total_time.as_secs() >= timeout_seconds {
                eprintln!("⏰ Tests exceeded timeout of {} seconds", timeout_seconds);
                eprintln!("💡 Try running with --mode quick or increase --timeout");
            }
            
            // Check common issues
            if e.to_string().contains("No such file") {
                eprintln!("💡 Hint: Make sure test data exists at {}", test_data_path.display());
                eprintln!("        Run: cd test-env/cassandra5 && ./manage.sh all");
            }
            
            process::exit(3);
        }
    }
}

fn print_help() {
    println!("CQLite Comprehensive Integration Test Runner");
    println!();
    println!("USAGE:");
    println!("    comprehensive_integration_test_runner [OPTIONS]");
    println!();
    println!("OPTIONS:");
    println!("    -m, --mode <MODE>           Test mode: full, quick, real-only, performance-only, collections-only");
    println!("                                [default: full]");
    println!("    -d, --test-data <PATH>      Path to test SSTable data");
    println!("                                [default: test-env/cassandra5/sstables]");
    println!("    -t, --timeout <SECONDS>     Timeout in seconds [default: 300]");
    println!("    -f, --fail-fast             Stop on first failure");
    println!("    -b, --brief                 Brief reporting (less detailed output)");
    println!("    -h, --help                  Print this help message");
    println!();
    println!("TEST MODES:");
    println!("    full                        Complete test suite (all features)");
    println!("    quick                       Essential tests only (faster feedback)");
    println!("    real-only                   Real SSTable reading tests only");
    println!("    performance-only            Performance and benchmarking tests");
    println!("    collections-only            Collection type and UDT tests");
    println!();
    println!("EXIT CODES:");
    println!("    0                          All tests passed");
    println!("    1                          Some tests failed or compatibility below threshold");
    println!("    2                          Critical issues found");
    println!("    3                          Test runner error or timeout");
    println!();
    println!("EXAMPLES:");
    println!("    # Run full test suite");
    println!("    comprehensive_integration_test_runner");
    println!();
    println!("    # Quick tests for CI feedback");
    println!("    comprehensive_integration_test_runner --mode quick --fail-fast");
    println!();
    println!("    # Test with custom data location");
    println!("    comprehensive_integration_test_runner --test-data ./my-sstables");
    println!();
    println!("    # Performance testing only");
    println!("    comprehensive_integration_test_runner --mode performance-only --timeout 600");
}

fn generate_ci_results(results: &cqlite_tests::comprehensive_integration_test_suite::IntegrationTestSuiteResults) -> Result<(), Box<dyn std::error::Error>> {
    use std::fs;
    use serde_json;

    // Create a CI-friendly JSON report
    let ci_report = serde_json::json!({
        "test_suite": "CQLite Comprehensive Integration Tests",
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "summary": {
            "total_tests": results.total_tests,
            "passed_tests": results.passed_tests,
            "failed_tests": results.failed_tests,
            "success_rate": (results.passed_tests as f64 / results.total_tests as f64) * 100.0,
            "execution_time_ms": results.total_execution_time_ms,
            "compatibility_score": results.overall_compatibility_score,
            "performance_score": results.overall_performance_score,
            "bytes_processed": results.total_bytes_processed,
            "files_tested": results.total_files_tested
        },
        "results_by_category": {
            "core_reading": results.test_results.iter()
                .filter(|r| r.test_category == "Core Reading")
                .map(|r| serde_json::json!({
                    "name": r.test_name,
                    "passed": r.passed,
                    "score": r.compatibility_score,
                    "execution_time_ms": r.execution_time_ms
                }))
                .collect::<Vec<_>>(),
            "feature_integration": results.test_results.iter()
                .filter(|r| r.test_category == "Feature Integration")
                .map(|r| serde_json::json!({
                    "name": r.test_name,
                    "passed": r.passed,
                    "score": r.compatibility_score
                }))
                .collect::<Vec<_>>(),
            "performance": results.test_results.iter()
                .filter(|r| r.test_category == "Performance")
                .map(|r| serde_json::json!({
                    "name": r.test_name,
                    "passed": r.passed,
                    "score": r.compatibility_score,
                    "metrics": r.performance_metrics
                }))
                .collect::<Vec<_>>()
        },
        "critical_issues": results.critical_issues,
        "recommendations": results.recommendations
    });

    // Write to file
    fs::write("integration_test_results.json", serde_json::to_string_pretty(&ci_report)?)?;
    println!("📄 CI results written to: integration_test_results.json");

    // Also write a simple status file for easier CI integration
    let status = if results.overall_compatibility_score >= 0.8 && results.failed_tests == 0 {
        "PASS"
    } else {
        "FAIL"
    };
    
    fs::write("integration_test_status.txt", format!("{}\n", status))?;
    println!("📄 CI status written to: integration_test_status.txt");

    Ok(())
}

// Additional helper functions for CI/CD integration

/// Generate a GitHub Actions summary
#[allow(dead_code)]
fn generate_github_summary(results: &cqlite_tests::comprehensive_integration_test_suite::IntegrationTestSuiteResults) {
    if let Ok(github_step_summary) = std::env::var("GITHUB_STEP_SUMMARY") {
        let summary = format!(
            "## 🧪 CQLite Integration Test Results\n\n\
            | Metric | Value |\n\
            |--------|-------|\n\
            | Total Tests | {} |\n\
            | Passed | {} |\n\
            | Failed | {} |\n\
            | Success Rate | {:.1}% |\n\
            | Compatibility Score | {:.3}/1.000 |\n\
            | Performance Score | {:.3}/1.000 |\n\
            | Data Processed | {:.2} MB |\n\
            | Execution Time | {:.2}s |\n\n\
            ### Status: {}\n\n\
            {}",
            results.total_tests,
            results.passed_tests,
            results.failed_tests,
            (results.passed_tests as f64 / results.total_tests as f64) * 100.0,
            results.overall_compatibility_score,
            results.overall_performance_score,
            results.total_bytes_processed as f64 / 1024.0 / 1024.0,
            results.total_execution_time_ms as f64 / 1000.0,
            if results.overall_compatibility_score >= 0.8 { "✅ PASS" } else { "❌ FAIL" },
            if results.critical_issues.is_empty() {
                "No critical issues found."
            } else {
                "⚠️ Critical issues require attention."
            }
        );

        if let Err(e) = std::fs::write(&github_step_summary, summary) {
            eprintln!("Warning: Could not write GitHub summary: {}", e);
        }
    }
}

/// Check if running in CI environment
#[allow(dead_code)]
fn is_ci_environment() -> bool {
    std::env::var("CI").is_ok() || 
    std::env::var("GITHUB_ACTIONS").is_ok() ||
    std::env::var("JENKINS_URL").is_ok() ||
    std::env::var("TRAVIS").is_ok() ||
    std::env::var("CIRCLECI").is_ok()
}