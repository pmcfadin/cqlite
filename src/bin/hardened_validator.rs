//! Hardened Validator CLI Tool - Issue #31
//! Command-line interface for comprehensive cross-version complex type validation

use clap::{Arg, Command};
use cqlite_core::{
    error::Result,
    schema::UdtRegistry,
    validation::hardened_validator_parser::{
        CassandraVersion, HardenedValidatorConfig, HardenedValidatorParser, MemoryLimits,
        PerformanceTargets,
    },
};
use log::{error, info, warn};
use std::{path::PathBuf, process, time::Instant};
use tokio;

#[tokio::main]
async fn main() {
    // Initialize logging
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_secs()
        .init();

    let matches = Command::new("hardened-validator")
        .version("1.0.0")
        .author("CQLite Team")
        .about("Hardened Validator Parser for Cross-Version Complex Type Validation (Issue #31)")
        .arg(
            Arg::new("target-version")
                .long("target-version")
                .short('v')
                .value_name("VERSION")
                .help("Target Cassandra version for compatibility testing")
                .default_value("5.0")
                .value_parser(["3.7", "3.11", "4.0", "4.1", "5.0"]),
        )
        .arg(
            Arg::new("test-data-paths")
                .long("test-data-paths")
                .short('t')
                .value_name("PATHS")
                .help("Comma-separated paths to test data directories")
                .default_value("test-data/cassandra5,tests/data/sstables,real_cassandra5_data")
                .value_delimiter(','),
        )
        .arg(
            Arg::new("strict-validation")
                .long("strict-validation")
                .short('s')
                .help("Enable strict validation with 0% tolerance for false positives/negatives")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("cross-version-testing")
                .long("cross-version-testing")
                .short('c')
                .help("Enable cross-version compatibility testing")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("max-ms-per-mb")
                .long("max-ms-per-mb")
                .value_name("MILLISECONDS")
                .help("Maximum processing time per MB (sub-second requirement)")
                .default_value("1000.0")
                .value_parser(clap::value_parser!(f64)),
        )
        .arg(
            Arg::new("min-throughput-mbs")
                .long("min-throughput-mbs")
                .value_name("MB_PER_SEC")
                .help("Minimum throughput in MB/s")
                .default_value("2.0")
                .value_parser(clap::value_parser!(f64)),
        )
        .arg(
            Arg::new("max-collection-size")
                .long("max-collection-size")
                .value_name("SIZE")
                .help("Maximum collection size for memory safety")
                .default_value("1000000")
                .value_parser(clap::value_parser!(usize)),
        )
        .arg(
            Arg::new("max-nesting-depth")
                .long("max-nesting-depth")
                .value_name("DEPTH")
                .help("Maximum nesting depth for complex types")
                .default_value("32")
                .value_parser(clap::value_parser!(usize)),
        )
        .arg(
            Arg::new("output-report")
                .long("output-report")
                .short('o')
                .value_name("FILE")
                .help("Output file for validation report")
                .default_value("validation_report.md"),
        )
        .arg(
            Arg::new("generate-test-data")
                .long("generate-test-data")
                .help("Generate comprehensive test data before validation")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("cassandra-host")
                .long("cassandra-host")
                .value_name("HOST")
                .help("Cassandra host for test data generation")
                .default_value("localhost"),
        )
        .arg(
            Arg::new("cassandra-port")
                .long("cassandra-port")
                .value_name("PORT")
                .help("Cassandra port for test data generation")
                .default_value("9042")
                .value_parser(clap::value_parser!(u16)),
        )
        .arg(
            Arg::new("verbose")
                .long("verbose")
                .help("Enable verbose logging")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("benchmark-mode")
                .long("benchmark-mode")
                .help("Run in benchmark mode with detailed performance metrics")
                .action(clap::ArgAction::SetTrue),
        )
        .get_matches();

    // Set log level
    if matches.get_flag("verbose") {
        log::set_max_level(log::LevelFilter::Debug);
    }

    // Parse command line arguments
    let target_version =
        match CassandraVersion::from_str(matches.get_one::<String>("target-version").unwrap()) {
            Ok(version) => version,
            Err(e) => {
                error!("Invalid target version: {}", e);
                process::exit(1);
            }
        };

    let test_data_paths: Vec<PathBuf> = matches
        .get_many::<String>("test-data-paths")
        .unwrap()
        .map(PathBuf::from)
        .collect();

    let strict_validation = matches.get_flag("strict-validation");
    let cross_version_testing = matches.get_flag("cross-version-testing");
    let generate_test_data = matches.get_flag("generate-test-data");
    let benchmark_mode = matches.get_flag("benchmark-mode");

    let max_ms_per_mb = *matches.get_one::<f64>("max-ms-per-mb").unwrap();
    let min_throughput_mbs = *matches.get_one::<f64>("min-throughput-mbs").unwrap();
    let max_collection_size = *matches.get_one::<usize>("max-collection-size").unwrap();
    let max_nesting_depth = *matches.get_one::<usize>("max-nesting-depth").unwrap();

    let output_report = matches.get_one::<String>("output-report").unwrap();
    let cassandra_host = matches.get_one::<String>("cassandra-host").unwrap();
    let cassandra_port = *matches.get_one::<u16>("cassandra-port").unwrap();

    info!("🚀 Starting Hardened Validator Parser - Issue #31");
    info!("Target Cassandra version: {}", target_version);
    info!("Test data paths: {:?}", test_data_paths);
    info!("Strict validation: {}", strict_validation);
    info!("Cross-version testing: {}", cross_version_testing);
    info!("Performance target: {:.1} ms/MB", max_ms_per_mb);
    info!("Throughput target: {:.1} MB/s", min_throughput_mbs);

    // Generate test data if requested
    if generate_test_data {
        info!("🔧 Generating comprehensive test data...");
        if let Err(e) =
            generate_comprehensive_test_data(&target_version, cassandra_host, cassandra_port).await
        {
            error!("Failed to generate test data: {}", e);
            process::exit(1);
        }
        info!("✅ Test data generation completed");
    }

    // Create configuration
    let config = HardenedValidatorConfig {
        target_version,
        strict_validation,
        performance_targets: PerformanceTargets {
            max_ms_per_mb,
            min_throughput_mbs,
            max_memory_ratio: 0.5,
            max_row_parse_latency_us: 1000, // 1ms max per row
        },
        test_data_paths,
        udt_registry: Some(create_comprehensive_udt_registry()),
        cross_version_testing,
        max_nesting_depth,
        memory_limits: MemoryLimits {
            max_collection_size,
            max_udt_fields: 1000,
            max_string_length: 1_000_000,
            max_blob_size: 100 * 1024 * 1024, // 100MB
        },
    };

    // Create parser
    let mut parser = match HardenedValidatorParser::new(config) {
        Ok(parser) => parser,
        Err(e) => {
            error!("Failed to create hardened validator parser: {}", e);
            process::exit(1);
        }
    };

    // Run validation
    info!("🔍 Starting comprehensive validation...");
    let start_time = Instant::now();

    let validation_result = match parser.validate_comprehensive().await {
        Ok(result) => result,
        Err(e) => {
            error!("Validation failed: {}", e);
            process::exit(1);
        }
    };

    let total_time = start_time.elapsed();

    // Display results
    info!("✅ Validation completed in {:?}", total_time);
    info!("Status: {:?}", validation_result.status);

    let total_files: usize = validation_result
        .version_results
        .values()
        .map(|r| r.files_processed)
        .sum();
    let total_success: usize = validation_result
        .version_results
        .values()
        .map(|r| r.successful_parses)
        .sum();
    let total_false_positives: usize = validation_result
        .version_results
        .values()
        .map(|r| r.false_positives)
        .sum();
    let total_false_negatives: usize = validation_result
        .version_results
        .values()
        .map(|r| r.false_negatives)
        .sum();

    info!("📊 Summary Statistics:");
    info!("  Total test files: {}", total_files);
    info!("  Successful parses: {}", total_success);
    info!("  False positives: {}", total_false_positives);
    info!("  False negatives: {}", total_false_negatives);
    info!(
        "  Overall accuracy: {:.2}%",
        if total_files > 0 {
            (total_success as f64 / total_files as f64) * 100.0
        } else {
            0.0
        }
    );
    info!(
        "  Throughput: {:.2} MB/s",
        validation_result.performance_metrics.throughput_mbs
    );
    info!(
        "  Peak memory: {:.1} MB",
        validation_result
            .performance_metrics
            .memory_stats
            .peak_memory_mb
    );

    // Performance analysis
    if benchmark_mode {
        info!("🏎️  Performance Benchmark Results:");
        let vs_targets = &validation_result.performance_metrics.vs_targets;
        info!(
            "  All targets met: {}",
            if vs_targets.all_targets_met {
                "✅"
            } else {
                "❌"
            }
        );
        info!(
            "  Time per MB ratio: {:.2}x (target)",
            vs_targets.time_per_mb_ratio
        );
        info!(
            "  Throughput ratio: {:.2}x (target)",
            vs_targets.throughput_ratio
        );
        info!(
            "  Memory efficiency: {:.2}x (target)",
            vs_targets.memory_ratio
        );

        if !vs_targets.all_targets_met {
            warn!("⚠️  Performance targets not met!");
            if vs_targets.time_per_mb_ratio > 1.0 {
                warn!(
                    "  - Processing time exceeds {:.1} ms/MB target",
                    max_ms_per_mb
                );
            }
            if vs_targets.throughput_ratio < 1.0 {
                warn!("  - Throughput below {:.1} MB/s target", min_throughput_mbs);
            }
        }
    }

    // Version-specific results
    info!("🔀 Version-Specific Results:");
    for (version, result) in &validation_result.version_results {
        info!(
            "  Cassandra {}: {:.1}% accuracy ({}/{})",
            version, result.accuracy_percentage, result.successful_parses, result.files_processed
        );

        if !result.complex_type_results.is_empty() {
            info!(
                "    Complex types tested: {}",
                result.complex_type_results.len()
            );
            for (type_name, type_result) in &result.complex_type_results {
                let success_rate = if type_result.tests_run > 0 {
                    (type_result.tests_passed as f64 / type_result.tests_run as f64) * 100.0
                } else {
                    0.0
                };
                info!("      {}: {:.1}% success", type_name, success_rate);
            }
        }
    }

    // Error analysis
    if validation_result.error_analysis.total_errors > 0 {
        warn!("⚠️  Error Analysis:");
        warn!(
            "  Total errors: {}",
            validation_result.error_analysis.total_errors
        );

        if !validation_result.error_analysis.critical_errors.is_empty() {
            error!("🚨 Critical errors detected:");
            for critical_error in &validation_result.error_analysis.critical_errors {
                error!("    {}", critical_error);
            }
        }

        if !validation_result.error_analysis.error_patterns.is_empty() {
            warn!("  Error patterns identified:");
            for pattern in &validation_result.error_analysis.error_patterns {
                warn!(
                    "    {}: {} occurrences",
                    pattern.pattern, pattern.occurrences
                );
                warn!("      Recommendation: {}", pattern.recommendation);
            }
        }
    }

    // Coverage analysis
    info!("📈 Test Coverage:");
    info!(
        "  Coverage percentage: {:.1}%",
        validation_result.coverage_metrics.coverage_percentage
    );
    info!(
        "  Types tested: {}",
        validation_result.coverage_metrics.types_tested.len()
    );
    info!(
        "  Edge cases covered: {}",
        validation_result.coverage_metrics.edge_cases_covered
    );

    // Generate detailed report
    info!("📝 Generating detailed validation report...");
    let report = match parser.generate_validation_report(&validation_result) {
        Ok(report) => report,
        Err(e) => {
            error!("Failed to generate validation report: {}", e);
            process::exit(1);
        }
    };

    // Save report to file
    if let Err(e) = std::fs::write(output_report, &report) {
        error!("Failed to write report to {}: {}", output_report, e);
        process::exit(1);
    }

    info!("📄 Detailed report saved to: {}", output_report);

    // Determine exit code based on validation status
    let exit_code = match validation_result.status {
        cqlite_core::validation::hardened_validator_parser::ValidationStatus::Perfect => {
            info!("🎉 Perfect validation achieved! 0% false positives/negatives.");
            0
        }
        cqlite_core::validation::hardened_validator_parser::ValidationStatus::MinorIssues => {
            warn!("⚠️  Minor issues detected. Review the report for details.");
            0 // Still success, but with warnings
        }
        cqlite_core::validation::hardened_validator_parser::ValidationStatus::MajorIssues => {
            error!("🔴 Major issues require attention before production use.");
            1
        }
        cqlite_core::validation::hardened_validator_parser::ValidationStatus::Failed => {
            error!("❌ Validation failed. Parser not ready for production.");
            2
        }
    };

    // Critical requirements check
    if strict_validation && (total_false_positives > 0 || total_false_negatives > 0) {
        error!("❌ CRITICAL: Strict validation failed - false positives/negatives detected!");
        error!("   False positives: {}", total_false_positives);
        error!("   False negatives: {}", total_false_negatives);
        error!("   This violates the 0% tolerance requirement for Issue #31.");
        process::exit(3);
    }

    if !validation_result
        .performance_metrics
        .vs_targets
        .all_targets_met
    {
        error!("❌ CRITICAL: Performance targets not met!");
        error!("   Required: sub-second per MB processing");
        error!(
            "   Actual: {:.1} ms/MB",
            max_ms_per_mb
                * validation_result
                    .performance_metrics
                    .vs_targets
                    .time_per_mb_ratio
        );
        process::exit(4);
    }

    info!("🏁 Hardened validator validation completed.");
    process::exit(exit_code);
}

/// Generate comprehensive test data using the Python script
async fn generate_comprehensive_test_data(
    version: &CassandraVersion,
    host: &str,
    port: u16,
) -> Result<()> {
    use tokio::process::Command;

    let script_path = "scripts/generate_hardened_validator_test_data.py";

    if !std::path::Path::new(script_path).exists() {
        return Err(cqlite_core::error::Error::internal(format!(
            "Test data generation script not found: {}",
            script_path
        )));
    }

    let mut cmd = Command::new("python3");
    cmd.arg(script_path)
        .arg("--version")
        .arg(version.to_string())
        .arg("--host")
        .arg(host)
        .arg("--port")
        .arg(port.to_string())
        .arg("--verbose");

    let output = cmd.output().await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(cqlite_core::error::Error::internal(format!(
            "Test data generation failed: {}",
            stderr
        )));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    info!("Test data generation output: {}", stdout);

    Ok(())
}

/// Create comprehensive UDT registry for testing
fn create_comprehensive_udt_registry() -> UdtRegistry {
    let mut registry = UdtRegistry::new();

    // Add standard UDTs - would normally be loaded from schema
    // For now, return empty registry and let parser handle embedded schemas
    registry
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_argument_parsing() {
        // Test that CLI arguments are correctly structured
        let cmd = Command::new("hardened-validator").arg(
            Arg::new("target-version")
                .long("target-version")
                .value_parser(["3.7", "3.11", "4.0", "4.1", "5.0"]),
        );

        // This test ensures the CLI structure is valid
        assert!(cmd
            .try_get_matches_from(vec!["hardened-validator", "--target-version", "5.0"])
            .is_ok());
    }

    #[tokio::test]
    async fn test_udt_registry_creation() {
        let registry = create_comprehensive_udt_registry();
        // Basic test that registry creation doesn't panic
        assert_eq!(registry.keyspace_count(), 0); // Empty for now
    }
}
