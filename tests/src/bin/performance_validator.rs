//! CQLite Performance Validation CLI Tool
//!
//! This tool runs comprehensive performance validation, benchmarking, and
//! regression testing for CQLite to ensure it meets all performance targets.

#[cfg(feature = "benchmarks")]
use clap::{Arg, Command};
#[cfg(feature = "benchmarks")]
use cqlite_tests::{
    performance_benchmark_runner::PerformanceTargets, BenchmarkRunnerConfig,
    PerformanceBenchmarkRunner,
};
#[cfg(feature = "benchmarks")]
use std::path::PathBuf;
#[cfg(feature = "benchmarks")]
use std::process;
#[cfg(feature = "benchmarks")]
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
    let _threads = matches.get_one::<usize>("threads").copied().unwrap_or(4);
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
        eprintln!("Error creating output directory: {e}");
        process::exit(1);
    }

    let mut config = if let Some(config_path) = config_file {
        match std::fs::read_to_string(config_path) {
            Ok(content) => match toml::from_str::<BenchmarkRunnerConfig>(&content) {
                Ok(config) => config,
                Err(e) => {
                    eprintln!("Error parsing config file: {e}");
                    process::exit(1);
                }
            },
            Err(e) => {
                eprintln!("Error reading config file: {e}");
                process::exit(1);
            }
        }
    } else {
        BenchmarkRunnerConfig::default()
    };

    config.output_directory = output_dir;

    if let Some(targets_path) = targets_file {
        match std::fs::read_to_string(targets_path) {
            Ok(content) => match toml::from_str::<PerformanceTargets>(&content) {
                Ok(targets) => {
                    config.test_config.performance_targets = targets;
                }
                Err(e) => {
                    eprintln!("Error parsing targets file: {e}");
                    process::exit(1);
                }
            },
            Err(e) => {
                eprintln!("Error reading targets file: {e}");
                process::exit(1);
            }
        }
    }

    // Update config based on command line flags
    config.enable_benchmarking = enable_benchmarks && !validation_only;
    config.enable_validation = !benchmark_only;
    config.enable_regression_testing = !benchmark_only;

    println!("🚀 Starting CQLite Performance Validation");
    println!("================================================");
    if verbose {
        println!("Configuration:");
        println!("  Output Directory: {:?}", config.output_directory);
        println!("  Benchmarks: {}", config.enable_benchmarking);
        println!("  Validation: {}", config.enable_validation);
        println!("  Regression Tests: {}", config.enable_regression_testing);
        println!();
    }

    let runner = PerformanceBenchmarkRunner::new(config);

    match runner.run_all_tests().await {
        Ok(results) => {
            println!("✅ Performance validation completed successfully");
            println!("📊 Results summary:");
            println!(
                "   Total runtime: {:.2}s",
                results.metadata.total_runtime_seconds
            );
            println!("   Overall grade: {}", results.summary.overall_grade);
            println!(
                "   Performance score: {}/100",
                results.summary.performance_score
            );

            if results.summary.overall_grade == "F" {
                println!("❌ Some performance tests failed or didn't meet targets");
                process::exit(1);
            }
        }
        Err(e) => {
            eprintln!("❌ Performance validation failed: {e}");
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
