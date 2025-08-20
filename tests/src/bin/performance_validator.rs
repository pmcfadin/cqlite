//! CQLite Performance Validation CLI Tool
//!
//! This tool runs comprehensive performance validation, benchmarking, and
//! regression testing for CQLite to ensure it meets all performance targets.

#[cfg(feature = "benchmarks")]
use clap::{Arg, Command};
#[cfg(feature = "benchmarks")]
use cqlite_tests::{
    BenchmarkRunnerConfig, PerformanceBenchmarkRunner, TestConfiguration,
    performance_benchmark_runner::PerformanceTargets,
};
#[cfg(feature = "benchmarks")]
use std::path::PathBuf;
#[cfg(feature = "benchmarks")]
use std::process;
#[cfg(feature = "benchmarks")]
use tokio;

#[cfg(feature = "benchmarks")]
#[tokio::main]
async fn main() {
    let matches = Command::new("CQLite Performance Validator")
        .version("1.0.0")
        .author("CQLite Development Team")
        .about("Comprehensive performance validation for CQLite")
        .arg(
            Arg::new("version")
                .long("version")
                .action(clap::ArgAction::SetTrue)
                .help("Show version information"),
        )
        .arg(
            Arg::new("config")
                .short('c')
                .long("config")
                .value_name("FILE")
                .help("Configuration file path")
                .value_parser(clap::value_parser!(PathBuf)),
        )
        .arg(
            Arg::new("output")
                .short('o')
                .long("output")
                .value_name("DIR")
                .help("Output directory for reports")
                .value_parser(clap::value_parser!(PathBuf)),
        )
        .arg(
            Arg::new("threads")
                .short('t')
                .long("threads")
                .value_name("NUM")
                .help("Number of threads to use")
                .value_parser(clap::value_parser!(usize)),
        )
        .arg(
            Arg::new("benchmark-only")
                .long("benchmark-only")
                .action(clap::ArgAction::SetTrue)
                .help("Run only performance benchmarks"),
        )
        .arg(
            Arg::new("validation-only")
                .long("validation-only")
                .action(clap::ArgAction::SetTrue)
                .help("Run only validation tests"),
        )
        .arg(
            Arg::new("skip-benchmarks")
                .long("skip-benchmarks")
                .action(clap::ArgAction::SetTrue)
                .help("Skip performance benchmarks"),
        )
        .arg(
            Arg::new("verbose")
                .short('v')
                .long("verbose")
                .action(clap::ArgAction::SetTrue)
                .help("Enable verbose output"),
        )
        .arg(
            Arg::new("targets")
                .long("targets")
                .value_name("FILE")
                .help("Performance targets configuration file")
                .value_parser(clap::value_parser!(PathBuf)),
        )
        .get_matches();

    if matches.get_flag("version") {
        println!("CQLite Performance Validator v1.0.0");
        return;
    }

    let config_file = matches.get_one::<PathBuf>("config");
    let output_dir = matches
        .get_one::<PathBuf>("output")
        .cloned()
        .unwrap_or_else(|| PathBuf::from("./performance-reports"));
    let threads = matches.get_one::<usize>("threads").copied().unwrap_or(4);
    let benchmark_only = matches.get_flag("benchmark-only");
    let validation_only = matches.get_flag("validation-only");
    let enable_benchmarks = !matches.get_flag("skip-benchmarks");
    let verbose = matches.get_flag("verbose");
    let targets_file = matches.get_one::<PathBuf>("targets");

    if benchmark_only && validation_only {
        eprintln!("Error: Cannot specify both --benchmark-only and --validation-only");
        process::exit(1);
    }

    // Create output directory
    if let Err(e) = std::fs::create_dir_all(&output_dir) {
        eprintln!("Error creating output directory: {}", e);
        process::exit(1);
    }

    let mut config = if let Some(config_path) = config_file {
        match BenchmarkRunnerConfig::from_file(config_path) {
            Ok(config) => config,
            Err(e) => {
                eprintln!("Error loading config file: {}", e);
                process::exit(1);
            }
        }
    } else {
        BenchmarkRunnerConfig::default()
    };

    config.output_directory = output_dir;
    config.num_threads = threads;
    config.verbose = verbose;

    if let Some(targets_path) = targets_file {
        match PerformanceTargets::from_file(targets_path) {
            Ok(targets) => config.performance_targets = Some(targets),
            Err(e) => {
                eprintln!("Error loading targets file: {}", e);
                process::exit(1);
            }
        }
    }

    let test_config = TestConfiguration {
        run_benchmarks: enable_benchmarks && !validation_only,
        run_validation: !benchmark_only,
        run_regression_tests: !benchmark_only,
        enable_stress_tests: true,
        max_concurrent_tests: threads,
    };

    println!("🚀 Starting CQLite Performance Validation");
    println!("================================================");
    if verbose {
        println!("Configuration:");
        println!("  Output Directory: {:?}", config.output_directory);
        println!("  Threads: {}", config.num_threads);
        println!("  Benchmarks: {}", test_config.run_benchmarks);
        println!("  Validation: {}", test_config.run_validation);
        println!("  Regression Tests: {}", test_config.run_regression_tests);
        println!();
    }

    let mut runner = PerformanceBenchmarkRunner::new(config);

    match runner.run_with_config(test_config).await {
        Ok(results) => {
            println!("✅ Performance validation completed successfully");
            println!("📊 Results summary:");
            println!("   Total tests: {}", results.total_tests);
            println!("   Passed tests: {}", results.passed_tests);
            println!("   Failed tests: {}", results.failed_tests);
            println!("   Success rate: {:.1}%", results.success_rate * 100.0);
            println!("   Overall runtime: {}ms", results.total_runtime_ms);

            if !results.overall_success {
                println!("❌ Some performance tests failed or didn't meet targets");
                process::exit(1);
            }
        }
        Err(e) => {
            eprintln!("❌ Performance validation failed: {}", e);
            process::exit(1);
        }
    }
}

#[cfg(not(feature = "benchmarks"))]
fn main() {
    eprintln!("This binary requires the 'benchmarks' feature to be enabled.");
    eprintln!("Compile with: cargo build --features benchmarks");
    std::process::exit(1);
}