//! CQLite Performance Validation CLI Tool
//!
//! This tool runs comprehensive performance validation, benchmarking, and
//! regression testing for CQLite to ensure it meets all performance targets.

use clap::{Arg, Command};
use std::path::PathBuf;
use std::process;
use tests::{
    BenchmarkRunnerConfig, PerformanceBenchmarkRunner, PerformanceTargets, TestConfiguration,
};
use tokio;

#[tokio::main]
async fn main() {
    let matches = Command::new("CQLite Performance Validator")
        .version("1.0.0")
        .author("CQLite Development Team")
        .about("Comprehensive performance validation for CQLite")
        .arg(
            Arg::new("version")
                .long("version")
                .short('v')
                .value_name("VERSION")
                .help("Version identifier for this test run")
                .default_value("dev"),
        )
        .arg(
            Arg::new("output")
                .long("output")
                .short('o')
                .value_name("DIR")
                .help("Output directory for reports")
                .default_value("performance_results"),
        )
        .arg(
            Arg::new("iterations")
                .long("iterations")
                .short('i')
                .value_name("COUNT")
                .help("Number of performance test iterations")
                .default_value("1000"),
        )
        .arg(
            Arg::new("dataset-size")
                .long("dataset-size")
                .short('d')
                .value_name("SIZE")
                .help("Size of test dataset")
                .default_value("100000"),
        )
        .arg(
            Arg::new("skip-validation")
                .long("skip-validation")
                .help("Skip performance validation tests")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("skip-benchmarks")
                .long("skip-benchmarks")
                .help("Skip benchmark tests")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("skip-regression")
                .long("skip-regression")
                .help("Skip regression tests")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("no-report")
                .long("no-report")
                .help("Skip generating detailed report")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("no-json")
                .long("no-json")
                .help("Skip JSON export")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("memory-target")
                .long("memory-target")
                .value_name("MB")
                .help("Memory usage target in MB")
                .default_value("128"),
        )
        .arg(
            Arg::new("latency-target")
                .long("latency-target")
                .value_name("MS")
                .help("Maximum lookup latency target in milliseconds")
                .default_value("1.0"),
        )
        .arg(
            Arg::new("parse-target")
                .long("parse-target")
                .value_name("SECONDS")
                .help("Maximum parse time for 1GB file in seconds")
                .default_value("10.0"),
        )
        .arg(
            Arg::new("write-throughput")
                .long("write-throughput")
                .value_name("OPS")
                .help("Minimum write throughput in ops/sec")
                .default_value("10000"),
        )
        .arg(
            Arg::new("read-throughput")
                .long("read-throughput")
                .value_name("OPS")
                .help("Minimum read throughput in ops/sec")
                .default_value("50000"),
        )
        .arg(
            Arg::new("verbose")
                .long("verbose")
                .short('V')
                .help("Enable verbose output")
                .action(clap::ArgAction::SetTrue),
        )
        .get_matches();

    // Parse command line arguments
    let version = matches.get_one::<String>("version").unwrap().clone();
    let output_dir = PathBuf::from(matches.get_one::<String>("output").unwrap());
    let iterations = matches
        .get_one::<String>("iterations")
        .unwrap()
        .parse::<usize>()
        .unwrap_or_else(|_| {
            eprintln!("Error: Invalid iterations value");
            process::exit(1);
        });
    let dataset_size = matches
        .get_one::<String>("dataset-size")
        .unwrap()
        .parse::<usize>()
        .unwrap_or_else(|_| {
            eprintln!("Error: Invalid dataset size value");
            process::exit(1);
        });

    let memory_target = matches
        .get_one::<String>("memory-target")
        .unwrap()
        .parse::<u64>()
        .unwrap_or_else(|_| {
            eprintln!("Error: Invalid memory target value");
            process::exit(1);
        });

    let latency_target = matches
        .get_one::<String>("latency-target")
        .unwrap()
        .parse::<f64>()
        .unwrap_or_else(|_| {
            eprintln!("Error: Invalid latency target value");
            process::exit(1);
        });

    let parse_target = matches
        .get_one::<String>("parse-target")
        .unwrap()
        .parse::<f64>()
        .unwrap_or_else(|_| {
            eprintln!("Error: Invalid parse target value");
            process::exit(1);
        });

    let write_throughput = matches
        .get_one::<String>("write-throughput")
        .unwrap()
        .parse::<f64>()
        .unwrap_or_else(|_| {
            eprintln!("Error: Invalid write throughput value");
            process::exit(1);
        });

    let read_throughput = matches
        .get_one::<String>("read-throughput")
        .unwrap()
        .parse::<f64>()
        .unwrap_or_else(|_| {
            eprintln!("Error: Invalid read throughput value");
            process::exit(1);
        });

    let verbose = matches.get_flag("verbose");
    let enable_validation = !matches.get_flag("skip-validation");
    let enable_benchmarks = !matches.get_flag("skip-benchmarks");
    let enable_regression = !matches.get_flag("skip-regression");
    let generate_report = !matches.get_flag("no-report");
    let export_json = !matches.get_flag("no-json");

    if verbose {
        println!("🔧 Configuration:");
        println!("   Version: {}", version);
        println!("   Output Directory: {}", output_dir.display());
        println!("   Iterations: {}", iterations);
        println!("   Dataset Size: {}", dataset_size);
        println!("   Memory Target: {} MB", memory_target);
        println!("   Latency Target: {} ms", latency_target);
        println!("   Parse Target: {} seconds", parse_target);
        println!("   Write Throughput: {} ops/sec", write_throughput);
        println!("   Read Throughput: {} ops/sec", read_throughput);
        println!("   Enable Validation: {}", enable_validation);
        println!("   Enable Benchmarks: {}", enable_benchmarks);
        println!("   Enable Regression: {}", enable_regression);
        println!("   Generate Report: {}", generate_report);
        println!("   Export JSON: {}", export_json);
        println!();
    }

    // Create configuration
    let config = BenchmarkRunnerConfig {
        enable_validation,
        enable_regression_testing: enable_regression,
        enable_benchmarking: enable_benchmarks,
        generate_report,
        export_json,
        output_directory: output_dir,
        version,
        test_config: TestConfiguration {
            small_dataset_size: dataset_size / 100,
            medium_dataset_size: dataset_size,
            large_dataset_size: dataset_size * 10,
            performance_iterations: iterations,
            enable_profiling: true,
            performance_targets: PerformanceTargets {
                max_parse_time_1gb_seconds: parse_target,
                max_memory_usage_mb: memory_target,
                max_lookup_latency_ms: latency_target,
                min_write_throughput_ops_sec: write_throughput,
                min_read_throughput_ops_sec: read_throughput,
            },
        },
    };

    // Run performance validation
    println!("🚀 Starting CQLite Performance Validation");
    println!("⏰ This may take several minutes depending on dataset size...");
    println!();

    let runner = PerformanceBenchmarkRunner::new(config);

    match runner.run_all_tests().await {
        Ok(results) => {
            if verbose {
                println!("\n✅ Performance validation completed successfully!");
                println!("📊 Overall Grade: {}", results.summary.overall_grade);
                println!(
                    "⚡ Performance Score: {}/100",
                    results.summary.performance_score
                );
                println!("🧠 Memory Score: {}/100", results.summary.memory_score);
                println!(
                    "🛡️  Reliability Score: {}/100",
                    results.summary.reliability_score
                );
            }

            // Determine exit code based on results
            let exit_code = if !results.summary.critical_issues.is_empty() {
                eprintln!("❌ Critical issues detected:");
                for issue in &results.summary.critical_issues {
                    eprintln!("   • {}", issue);
                }
                1
            } else if results.summary.overall_grade == "F" || results.summary.performance_score < 60
            {
                eprintln!(
                    "❌ Performance validation failed with grade: {}",
                    results.summary.overall_grade
                );
                1
            } else {
                println!("✅ Performance validation passed!");
                0
            };

            process::exit(exit_code);
        }
        Err(e) => {
            eprintln!("❌ Performance validation failed: {}", e);
            process::exit(1);
        }
    }
}
