//! M3 Performance Validation Binary
//!
//! Executable for running comprehensive M3 complex type performance validation.
//! This binary can be used in CI/CD pipelines and for development validation.

use clap::{App, Arg, ArgMatches};
use std::process;
use std::time::Instant;

// Include the M3 performance validator
mod m3_performance_validator {
    include!("../m3_performance_validator.rs");
}

use m3_performance_validator::{M3PerformanceValidator, ValidationConfig};
use cqlite_core::parser::PerformanceTargets;

fn main() {
    let matches = App::new("M3 Performance Validator")
        .version("1.0.0")
        .author("CQLite Team")
        .about("Validates M3 complex type performance targets")
        .arg(
            Arg::with_name("output-dir")
                .long("output-dir")
                .value_name("DIR")
                .help("Output directory for reports")
                .default_value("m3_validation_reports")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("no-simd")
                .long("no-simd")
                .help("Disable SIMD optimizations for testing"),
        )
        .arg(
            Arg::with_name("no-regression")
                .long("no-regression")
                .help("Skip regression tests"),
        )
        .arg(
            Arg::with_name("update-baseline")
                .long("update-baseline")
                .help("Update performance baseline after tests"),
        )
        .arg(
            Arg::with_name("no-reports")
                .long("no-reports")
                .help("Skip generating detailed reports"),
        )
        .arg(
            Arg::with_name("strict")
                .long("strict")
                .help("Use strict performance targets (higher thresholds)"),
        )
        .arg(
            Arg::with_name("throughput-target")
                .long("throughput-target")
                .value_name("MB/s")
                .help("Minimum throughput target in MB/s")
                .default_value("100")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("memory-ratio")
                .long("memory-ratio")
                .value_name("RATIO")
                .help("Maximum memory increase ratio")
                .default_value("1.5")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("latency-limit")
                .long("latency-limit")
                .value_name("MS")
                .help("Maximum additional latency in milliseconds")
                .default_value("10")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("slowdown-ratio")
                .long("slowdown-ratio")
                .value_name("RATIO")
                .help("Maximum complex type slowdown ratio")
                .default_value("2.0")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("quiet")
                .short("q")
                .long("quiet")
                .help("Suppress detailed output, only show final results"),
        )
        .arg(
            Arg::with_name("json-output")
                .long("json-output")
                .help("Output results in JSON format for CI integration"),
        )
        .arg(
            Arg::with_name("fail-fast")
                .long("fail-fast")
                .help("Exit immediately on first test failure"),
        )
        .get_matches();

    let start_time = Instant::now();

    // Setup logging
    if !matches.is_present("quiet") {
        env_logger::Builder::from_default_env()
            .filter_level(log::LevelFilter::Info)
            .init();
    }

    // Parse configuration from arguments
    let config = match parse_config(&matches) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("❌ Configuration error: {}", e);
            process::exit(1);
        }
    };

    // Print startup banner
    if !matches.is_present("quiet") {
        print_banner(&config);
    }

    // Create and run validator
    let mut validator = M3PerformanceValidator::with_config(config);

    let results = match validator.run_validation() {
        Ok(results) => results,
        Err(e) => {
            eprintln!("❌ Validation failed: {}", e);
            process::exit(1);
        }
    };

    // Output results
    if matches.is_present("json-output") {
        output_json_results(&results);
    } else if !matches.is_present("quiet") {
        // Summary already printed by validator
    } else {
        print_quiet_summary(&results);
    }

    let total_time = start_time.elapsed();
    
    if !matches.is_present("quiet") {
        println!("🏁 Total execution time: {:.2} seconds", total_time.as_secs_f64());
    }

    // Exit with appropriate code
    if results.passed {
        if !matches.is_present("quiet") {
            println!("✅ M3 performance validation PASSED");
        }
        process::exit(0);
    } else {
        if !matches.is_present("quiet") {
            println!("❌ M3 performance validation FAILED");
        }
        process::exit(1);
    }
}

fn parse_config(matches: &ArgMatches) -> Result<ValidationConfig, String> {
    let throughput_target = matches
        .value_of("throughput-target")
        .unwrap()
        .parse::<f64>()
        .map_err(|_| "Invalid throughput target")?;

    let memory_ratio = matches
        .value_of("memory-ratio")
        .unwrap()
        .parse::<f64>()
        .map_err(|_| "Invalid memory ratio")?;

    let latency_limit = matches
        .value_of("latency-limit")
        .unwrap()
        .parse::<f64>()
        .map_err(|_| "Invalid latency limit")?;

    let slowdown_ratio = matches
        .value_of("slowdown-ratio")
        .unwrap()
        .parse::<f64>()
        .map_err(|_| "Invalid slowdown ratio")?;

    let custom_targets = if matches.is_present("strict") {
        Some(PerformanceTargets {
            max_complex_slowdown_ratio: 1.5, // Stricter: 1.5x instead of 2x
            max_memory_increase_ratio: 1.3,  // Stricter: 1.3x instead of 1.5x
            min_complex_throughput_mbs: 120.0, // Stricter: 120 MB/s instead of 100
            max_additional_latency_ms: 5.0,  // Stricter: 5ms instead of 10ms
        })
    } else {
        Some(PerformanceTargets {
            max_complex_slowdown_ratio: slowdown_ratio,
            max_memory_increase_ratio: memory_ratio,
            min_complex_throughput_mbs: throughput_target,
            max_additional_latency_ms: latency_limit,
        })
    };

    Ok(ValidationConfig {
        enable_simd: !matches.is_present("no-simd"),
        run_regression_tests: !matches.is_present("no-regression"),
        update_baselines: matches.is_present("update-baseline"),
        custom_targets,
        output_dir: matches.value_of("output-dir").unwrap().to_string(),
        generate_reports: !matches.is_present("no-reports"),
    })
}

fn print_banner(config: &ValidationConfig) {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║                 M3 Performance Validator v1.0.0              ║");
    println!("║              Complex Type Performance Validation             ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();
    println!("🎯 VALIDATION CONFIGURATION:");
    println!("   • SIMD Optimizations: {}", if config.enable_simd { "✅ Enabled" } else { "❌ Disabled" });
    println!("   • Regression Tests: {}", if config.run_regression_tests { "✅ Enabled" } else { "❌ Disabled" });
    println!("   • Update Baselines: {}", if config.update_baselines { "✅ Yes" } else { "❌ No" });
    println!("   • Generate Reports: {}", if config.generate_reports { "✅ Yes" } else { "❌ No" });
    println!("   • Output Directory: {}", config.output_dir);
    
    if let Some(ref targets) = config.custom_targets {
        println!("\n🎯 PERFORMANCE TARGETS:");
        println!("   • Minimum Throughput: {:.1} MB/s", targets.min_complex_throughput_mbs);
        println!("   • Maximum Memory Ratio: {:.1}x", targets.max_memory_increase_ratio);
        println!("   • Maximum Latency: {:.1} ms", targets.max_additional_latency_ms);
        println!("   • Maximum Slowdown: {:.1}x", targets.max_complex_slowdown_ratio);
    }
    println!();
}

fn output_json_results(results: &m3_performance_validator::ValidationResults) {
    use serde_json::json;
    
    let json_output = json!({
        "status": if results.passed { "PASSED" } else { "FAILED" },
        "passed": results.passed,
        "validation_time_seconds": results.validation_time.as_secs_f64(),
        "summary": {
            "total_tests": results.performance_summary.total_tests,
            "passed_tests": results.performance_summary.passed_tests,
            "pass_rate_percent": (results.performance_summary.passed_tests as f64 / results.performance_summary.total_tests as f64) * 100.0,
            "average_performance_mbs": results.performance_summary.average_performance_mbs,
            "average_memory_usage_mb": results.performance_summary.average_memory_usage_mb,
            "average_latency_ms": results.performance_summary.average_latency_ms,
            "complex_vs_primitive_ratio": results.performance_summary.complex_vs_primitive_ratio
        },
        "tests": results.test_results.iter().map(|test| {
            json!({
                "name": test.name,
                "category": test.category,
                "passed": test.passed,
                "performance_mbs": test.performance_mbs,
                "memory_usage_mb": test.memory_usage_mb,
                "latency_ms": test.latency_ms,
                "meets_targets": test.meets_targets
            })
        }).collect::<Vec<_>>(),
        "failed_tests": results.test_results.iter()
            .filter(|test| !test.passed)
            .map(|test| {
                json!({
                    "name": test.name,
                    "reason": test.details
                })
            }).collect::<Vec<_>>()
    });

    println!("{}", serde_json::to_string_pretty(&json_output).unwrap());
}

fn print_quiet_summary(results: &m3_performance_validator::ValidationResults) {
    let status = if results.passed { "PASSED" } else { "FAILED" };
    let pass_rate = (results.performance_summary.passed_tests as f64 / results.performance_summary.total_tests as f64) * 100.0;
    
    println!("M3 Validation: {} ({}/{} tests, {:.1}% pass rate)", 
        status, 
        results.performance_summary.passed_tests,
        results.performance_summary.total_tests,
        pass_rate
    );
    
    if !results.passed {
        println!("Failed tests:");
        for test in &results.test_results {
            if !test.passed {
                println!("  - {}: {}", test.name, test.details);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_parsing() {
        let matches = App::new("test")
            .arg(Arg::with_name("throughput-target").long("throughput-target").takes_value(true))
            .arg(Arg::with_name("memory-ratio").long("memory-ratio").takes_value(true))
            .arg(Arg::with_name("latency-limit").long("latency-limit").takes_value(true))
            .arg(Arg::with_name("slowdown-ratio").long("slowdown-ratio").takes_value(true))
            .arg(Arg::with_name("output-dir").long("output-dir").takes_value(true))
            .get_matches_from(vec![
                "test",
                "--throughput-target", "150",
                "--memory-ratio", "1.3",
                "--latency-limit", "5",
                "--slowdown-ratio", "1.8",
                "--output-dir", "test_output"
            ]);

        let config = parse_config(&matches).unwrap();
        
        assert!(config.enable_simd); // Default
        assert!(config.run_regression_tests); // Default
        assert!(!config.update_baselines); // Default
        assert_eq!(config.output_dir, "test_output");
        
        let targets = config.custom_targets.unwrap();
        assert_eq!(targets.min_complex_throughput_mbs, 150.0);
        assert_eq!(targets.max_memory_increase_ratio, 1.3);
        assert_eq!(targets.max_additional_latency_ms, 5.0);
        assert_eq!(targets.max_complex_slowdown_ratio, 1.8);
    }

    #[test]
    fn test_strict_mode() {
        let matches = App::new("test")
            .arg(Arg::with_name("strict").long("strict"))
            .arg(Arg::with_name("throughput-target").long("throughput-target").takes_value(true).default_value("100"))
            .arg(Arg::with_name("memory-ratio").long("memory-ratio").takes_value(true).default_value("1.5"))
            .arg(Arg::with_name("latency-limit").long("latency-limit").takes_value(true).default_value("10"))
            .arg(Arg::with_name("slowdown-ratio").long("slowdown-ratio").takes_value(true).default_value("2.0"))
            .arg(Arg::with_name("output-dir").long("output-dir").takes_value(true).default_value("test"))
            .get_matches_from(vec!["test", "--strict"]);

        let config = parse_config(&matches).unwrap();
        let targets = config.custom_targets.unwrap();
        
        // Strict mode should override individual targets
        assert_eq!(targets.max_complex_slowdown_ratio, 1.5);
        assert_eq!(targets.max_memory_increase_ratio, 1.3);
        assert_eq!(targets.min_complex_throughput_mbs, 120.0);
        assert_eq!(targets.max_additional_latency_ms, 5.0);
    }
}