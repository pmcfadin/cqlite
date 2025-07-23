//! Compatibility Test Runner Executable
//!
//! Command-line tool for running comprehensive Cassandra 5+ compatibility tests.

use clap::{Arg, Command};
use std::process;
use tests::{
    run_compatibility_validation, run_performance_validation, run_quick_compatibility_check,
    IntegrationTestConfig, IntegrationTestRunner,
};
use tokio;

#[tokio::main]
async fn main() {
    let matches = Command::new("CQLite Compatibility Test Runner")
        .version("1.0.0")
        .author("CQLite Team")
        .about("Comprehensive test suite for Cassandra 5+ compatibility validation")
        .arg(
            Arg::new("mode")
                .short('m')
                .long("mode")
                .value_name("MODE")
                .help("Test mode: full, quick, performance")
                .value_parser(["full", "quick", "performance"])
                .default_value("full"),
        )
        .arg(
            Arg::new("stress")
                .long("stress")
                .help("Enable stress testing with large datasets")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("fail-fast")
                .long("fail-fast")
                .help("Stop testing on first failure")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("detailed")
                .long("detailed")
                .help("Enable detailed reporting")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("format-tests")
                .long("format-tests")
                .help("Run SSTable format tests")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("type-tests")
                .long("type-tests")
                .help("Run CQL type system tests")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("compatibility-tests")
                .long("compatibility-tests")
                .help("Run compatibility framework tests")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("performance-tests")
                .long("performance-tests")
                .help("Run performance benchmarks")
                .action(clap::ArgAction::SetTrue),
        )
        .get_matches();

    println!("🚀 CQLite Cassandra 5+ Compatibility Test Runner");
    println!("{}", "=".repeat(60));

    let mode = matches.get_one::<String>("mode").unwrap();
    let stress_enabled = matches.get_flag("stress");
    let fail_fast = matches.get_flag("fail-fast");
    let detailed = matches.get_flag("detailed");

    let result = match mode.as_str() {
        "quick" => {
            println!("🏃 Running quick compatibility check...");
            run_quick_compatibility_check().await
        }
        "performance" => {
            println!("⚡ Running performance validation...");
            run_performance_validation().await
        }
        "full" | _ => {
            if matches.contains_id("format-tests")
                || matches.contains_id("type-tests")
                || matches.contains_id("compatibility-tests")
                || matches.contains_id("performance-tests")
            {
                // Custom test selection
                println!("🎯 Running custom test selection...");

                let config = IntegrationTestConfig {
                    run_compatibility_tests: matches.get_flag("compatibility-tests"),
                    run_format_tests: matches.get_flag("format-tests"),
                    run_type_tests: matches.get_flag("type-tests"),
                    run_performance_benchmarks: matches.get_flag("performance-tests"),
                    run_stress_tests: stress_enabled,
                    detailed_reporting: detailed,
                    fail_fast,
                };

                let runner = IntegrationTestRunner::new(config);
                runner.run_all_tests().await
            } else {
                // Full test suite
                println!("🔬 Running full compatibility validation...");

                let config = IntegrationTestConfig {
                    run_compatibility_tests: true,
                    run_format_tests: true,
                    run_type_tests: true,
                    run_performance_benchmarks: true,
                    run_stress_tests: stress_enabled,
                    detailed_reporting: detailed,
                    fail_fast,
                };

                let runner = IntegrationTestRunner::new(config);
                runner.run_all_tests().await
            }
        }
    };

    match result {
        Ok(results) => {
            println!("\n✅ Test execution completed successfully!");

            // Exit with appropriate code based on results
            if results.failed_tests > 0 {
                println!("⚠️  Some tests failed. Review the results above.");
                process::exit(1);
            } else if results.overall_score < 0.85 {
                println!("⚠️  Low compatibility score. Consider improvements.");
                process::exit(2);
            } else {
                println!("🎉 All tests passed with good compatibility!");
                process::exit(0);
            }
        }
        Err(e) => {
            eprintln!("❌ Test execution failed: {:?}", e);
            process::exit(3);
        }
    }
}

/// Print help information about the test suite
fn print_test_info() {
    println!("📋 Available Test Categories:");
    println!("  🔧 Format Tests: SSTable header, compression, metadata parsing");
    println!("  🔢 Type Tests: All CQL data types, serialization, edge cases");
    println!("  🏗️  Compatibility Tests: End-to-end Cassandra format validation");
    println!("  ⚡ Performance Tests: Throughput, latency, memory usage benchmarks");
    println!();

    println!("🎯 Test Modes:");
    println!("  • full: Complete test suite (default)");
    println!("  • quick: Essential compatibility checks only");
    println!("  • performance: Benchmarks and performance validation");
    println!();

    println!("💡 Example Usage:");
    println!("  # Run full test suite with stress testing");
    println!("  cargo run --bin compatibility_test_runner -- --stress --detailed");
    println!();
    println!("  # Run only format and type tests");
    println!("  cargo run --bin compatibility_test_runner -- --format-tests --type-tests");
    println!();
    println!("  # Quick compatibility check");
    println!("  cargo run --bin compatibility_test_runner -- --mode quick");
    println!();
    println!("  # Performance validation only");
    println!("  cargo run --bin compatibility_test_runner -- --mode performance --stress");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_runner_compilation() {
        // Basic test to ensure the runner compiles
        assert!(true);
    }
}
