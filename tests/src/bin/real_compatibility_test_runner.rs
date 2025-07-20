//! Real Cassandra 5 SSTable Compatibility Test Runner
//!
//! This binary runs comprehensive compatibility tests against real Cassandra 5
//! SSTable files to validate CQLite parser functionality.

use clap::{Arg, Command};
use serde_json::json;
use std::path::PathBuf;
use std::process;
use tests::real_sstable_compatibility_test::{
    RealCompatibilityConfig, RealSSTableCompatibilityTester,
};

fn main() {
    let matches = Command::new("CQLite Real SSTable Compatibility Tester")
        .version("1.0.0")
        .author("CQLite Team")
        .about("Tests CQLite parser against real Cassandra 5 SSTable files")
        .arg(
            Arg::new("test-path")
                .short('p')
                .long("test-path")
                .value_name("PATH")
                .help("Path to Cassandra SSTable test data")
                .default_value("test-env/cassandra5/data/cassandra5-sstables"),
        )
        .arg(
            Arg::new("skip-magic")
                .long("skip-magic")
                .help("Skip magic number validation tests")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("skip-vint")
                .long("skip-vint")
                .help("Skip VInt parsing tests")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("skip-data")
                .long("skip-data")
                .help("Skip data structure analysis")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("skip-stats")
                .long("skip-stats")
                .help("Skip statistics file parsing tests")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("verbose")
                .short('v')
                .long("verbose")
                .help("Enable verbose output")
                .action(clap::ArgAction::SetTrue),
        )
        .get_matches();

    // Configure logging based on verbosity
    if matches.get_flag("verbose") {
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("debug")).init();
    } else {
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    }

    println!("🚀 CQLite Real SSTable Compatibility Test Runner");
    println!("=".repeat(55));

    // Build configuration from command line arguments
    let test_path = PathBuf::from(matches.get_one::<String>("test-path").unwrap());

    let config = RealCompatibilityConfig {
        test_path,
        validate_magic_numbers: !matches.get_flag("skip-magic"),
        test_vint_parsing: !matches.get_flag("skip-vint"),
        test_data_parsing: !matches.get_flag("skip-data"),
        test_statistics_parsing: !matches.get_flag("skip-stats"),
    };

    println!("📋 Test Configuration:");
    println!("  • Test Path: {}", config.test_path.display());
    println!("  • Magic Number Tests: {}", config.validate_magic_numbers);
    println!("  • VInt Parsing Tests: {}", config.test_vint_parsing);
    println!("  • Data Structure Tests: {}", config.test_data_parsing);
    println!(
        "  • Statistics File Tests: {}",
        config.test_statistics_parsing
    );
    println!();

    // Verify test data path exists
    if !config.test_path.exists() {
        eprintln!(
            "❌ Error: Test data path does not exist: {}",
            config.test_path.display()
        );
        eprintln!("💡 Tip: Ensure Cassandra test environment is set up and populated with data.");
        eprintln!("💡 Run: cd test-env/cassandra5 && docker-compose up");
        process::exit(1);
    }

    // Create and run the compatibility tester
    let mut tester = RealSSTableCompatibilityTester::new(config);

    match tester.run_comprehensive_tests() {
        Ok(()) => {
            println!("\n✅ Compatibility testing completed successfully!");

            // Store results in coordination memory
            if let Err(e) = store_coordination_results(&tester) {
                eprintln!("⚠️  Warning: Failed to store coordination results: {}", e);
            }

            process::exit(0);
        }
        Err(e) => {
            eprintln!("\n❌ Compatibility testing failed: {}", e);
            eprintln!("💡 Check the error details above and ensure test data is accessible.");
            process::exit(1);
        }
    }
}

/// Store test results in swarm coordination memory
fn store_coordination_results(
    tester: &RealSSTableCompatibilityTester,
) -> Result<(), Box<dyn std::error::Error>> {
    use serde_json::json;
    use std::process::Command;

    // Calculate summary statistics
    let total_tests = tester.results.len();
    let passed_tests = tester.results.iter().filter(|r| r.test_passed).count();
    let critical_failures = tester
        .results
        .iter()
        .filter(|r| !r.test_passed && (r.file_type == "Data.db" || r.file_type == "Statistics.db"))
        .count();

    let compatibility_score = if critical_failures == 0 {
        (passed_tests as f64 / total_tests as f64) * 100.0
    } else {
        ((passed_tests - critical_failures) as f64 / total_tests as f64) * 100.0
    };

    // Prepare coordination data
    let coordination_data = json!({
        "test_type": "real_sstable_compatibility",
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "summary": {
            "total_tests": total_tests,
            "passed_tests": passed_tests,
            "failed_tests": total_tests - passed_tests,
            "critical_failures": critical_failures,
            "compatibility_score": compatibility_score
        },
        "status": if compatibility_score >= 90.0 {
            "excellent"
        } else if compatibility_score >= 75.0 {
            "good"
        } else if compatibility_score >= 50.0 {
            "needs_work"
        } else {
            "critical"
        },
        "tables_tested": tester.results.iter()
            .map(|r| r.table_name.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>(),
        "findings": {
            "magic_numbers_valid": tester.results.iter()
                .filter(|r| r.file_type == "Data.db")
                .filter(|r| r.parser_details.as_ref()
                    .map(|d| d.magic_number_valid)
                    .unwrap_or(false))
                .count(),
            "vint_compatibility": tester.results.iter()
                .filter_map(|r| r.parser_details.as_ref())
                .map(|d| d.vint_samples.iter().filter(|s| s.encoding_valid).count())
                .sum::<usize>(),
            "total_vint_samples": tester.results.iter()
                .filter_map(|r| r.parser_details.as_ref())
                .map(|d| d.vint_samples.len())
                .sum::<usize>()
        }
    });

    // Store in coordination memory using hooks
    let output = Command::new("npx")
        .args([
            "claude-flow@alpha",
            "hooks",
            "post-edit",
            "--memory-key",
            "compatibility/real_sstable_tests",
            "--telemetry",
            "true",
        ])
        .env("COORDINATION_DATA", coordination_data.to_string())
        .output()?;

    if !output.status.success() {
        return Err(format!(
            "Coordination storage failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }

    println!("💾 Test results stored in swarm coordination memory");
    Ok(())
}
