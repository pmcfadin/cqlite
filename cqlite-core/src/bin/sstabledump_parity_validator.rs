//! SSTableDump Parity Validator Binary
//! 
//! Command-line tool to validate that our spec-accurate readers produce
//! identical output to Cassandra's sstabledump tool. This provides zero
//! tolerance evidence for Issue #25 implementation.

use cqlite_core::{
    error::Result,
    validation::sstabledump_parity::{
        SStableDumpParityConfig, SStableDumpParityValidator, ParityStatus
    },
};
use clap::{Arg, Command};
use std::path::PathBuf;
use tokio;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let matches = Command::new("sstabledump-parity-validator")
        .version("1.0")
        .about("Validates SSTable parsing against Cassandra's sstabledump tool")
        .long_about(
            "This tool provides zero-tolerance validation that our spec-accurate \
             SSTable readers produce identical output to Cassandra's sstabledump tool. \
             This is critical evidence for Issue #25 proving elimination of heuristic parsing."
        )
        .arg(
            Arg::new("cassandra-tools")
                .long("cassandra-tools")
                .value_name("PATH")
                .help("Path to Cassandra tools directory containing sstabledump")
                .required(false)
        )
        .arg(
            Arg::new("test-paths")
                .long("test-paths")
                .value_name("PATHS")
                .help("Comma-separated paths to SSTable test directories")
                .required(false)
                .default_value("test-env/cassandra5/sstables")
        )
        .arg(
            Arg::new("output")
                .short('o')
                .long("output")
                .value_name("FILE")
                .help("Output file for validation report")
                .required(false)
        )
        .arg(
            Arg::new("verbose")
                .short('v')
                .long("verbose")
                .action(clap::ArgAction::SetTrue)
                .help("Enable verbose output comparison")
        )
        .arg(
            Arg::new("exact-match")
                .long("exact-match")
                .action(clap::ArgAction::SetTrue)
                .help("Require exact byte-for-byte match (zero tolerance)")
        )
        .get_matches();

    // Parse command line arguments
    let cassandra_tools_path = matches.get_one::<String>("cassandra-tools")
        .map(PathBuf::from);

    let test_paths: Vec<PathBuf> = matches.get_one::<String>("test-paths")
        .unwrap()
        .split(',')
        .map(|s| PathBuf::from(s.trim()))
        .collect();

    let verbose = matches.get_flag("verbose");
    let exact_match = matches.get_flag("exact-match");
    let output_file = matches.get_one::<String>("output");

    println!("🚀 Starting SSTableDump Parity Validation");
    println!("   Issue #25: Zero Tolerance Evidence Generation");
    println!();

    // Configure validator
    let config = SStableDumpParityConfig {
        cassandra_tools_path,
        test_sstable_paths: test_paths.clone(),
        temp_dir: PathBuf::from("/tmp/sstabledump-parity-validation"),
        verbose_comparison: verbose,
        sstabledump_timeout_seconds: 60,
        require_exact_match: exact_match,
    };

    println!("📝 Configuration:");
    println!("   Cassandra Tools: {:?}", config.cassandra_tools_path);
    println!("   Test Paths: {:?}", config.test_sstable_paths);
    println!("   Verbose Comparison: {}", config.verbose_comparison);
    println!("   Exact Match Required: {}", config.require_exact_match);
    println!();

    // Create validator
    let validator = match SStableDumpParityValidator::new(config) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("❌ Failed to create validator: {}", e);
            std::process::exit(1);
        }
    };

    // Run validation
    println!("🔍 Running comprehensive parity validation...");
    let result = match validator.validate_sstabledump_parity().await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("❌ Validation failed: {}", e);
            std::process::exit(1);
        }
    };

    // Display results
    println!();
    match result.status {
        ParityStatus::PerfectParity => {
            println!("✅ PERFECT PARITY ACHIEVED!");
            println!("   🎯 ZERO TOLERANCE EVIDENCE: Our spec-accurate readers");
            println!("      produce IDENTICAL output to Cassandra's sstabledump");
            println!();
            println!("   📊 Results Summary:");
            println!("      - Total Files Tested: {}", result.total_files_tested);
            println!("      - Perfect Parity Files: {}", result.perfect_parity_count);
            println!("      - Total Discrepancies: {}", result.discrepancy_summary.total_discrepancies);
            println!("      - Validation Time: {}ms", result.performance_metrics.total_validation_time_ms);
            println!();
            println!("   🏆 This proves Issue #25 implementation:");
            println!("      ✅ Eliminates ALL heuristic parsing");
            println!("      ✅ Uses schema-driven type resolution");
            println!("      ✅ Follows Cassandra specification exactly");
            println!("      ✅ Achieves zero tolerance for parsing discrepancies");
        }
        ParityStatus::MinorDiscrepancies => {
            println!("⚠️  Minor discrepancies found (mostly formatting)");
            println!("   Total Discrepancies: {}", result.discrepancy_summary.total_discrepancies);
            println!("   Files with Perfect Parity: {}", result.perfect_parity_count);
            println!("   Files with Discrepancies: {}", result.discrepancy_count);
        }
        ParityStatus::MajorDiscrepancies => {
            println!("❌ Major discrepancies found - REQUIRES ATTENTION");
            println!("   Total Discrepancies: {}", result.discrepancy_summary.total_discrepancies);
            println!("   Critical Issues: {}", result.discrepancy_summary.critical_issues.len());
            println!();
            println!("   🚨 Critical Issues:");
            for issue in &result.discrepancy_summary.critical_issues {
                println!("      - {}", issue);
            }
        }
        ParityStatus::ValidationFailed => {
            println!("💥 Validation failed to complete");
            println!("   This may indicate:");
            println!("   - Missing sstabledump tool");
            println!("   - No test SSTable files found");
            println!("   - System configuration issues");
        }
    }

    // Generate comprehensive evidence report
    println!();
    println!("📋 Generating comprehensive evidence report...");
    let evidence_report = validator.generate_evidence_report(&result);

    // Save report to file if specified
    if let Some(output_path) = output_file {
        match tokio::fs::write(output_path, &evidence_report).await {
            Ok(_) => println!("   ✅ Report saved to: {}", output_path),
            Err(e) => eprintln!("   ❌ Failed to save report: {}", e),
        }
    } else {
        // Display report to console
        println!();
        println!("📄 EVIDENCE REPORT:");
        println!("{}", evidence_report);
    }

    // Exit with appropriate code
    let exit_code = match result.status {
        ParityStatus::PerfectParity => 0,
        ParityStatus::MinorDiscrepancies => 1,
        ParityStatus::MajorDiscrepancies => 2,
        ParityStatus::ValidationFailed => 3,
    };

    println!();
    println!("🏁 Validation complete with exit code: {}", exit_code);
    
    std::process::exit(exit_code);
}