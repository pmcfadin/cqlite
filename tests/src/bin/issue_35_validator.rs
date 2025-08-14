//! Issue #35 standalone validation runner
//!
//! This binary runs the comprehensive validation suite for Index/Summary/Statistics
//! parsing and validation as required by Issue #35.

use anyhow::Result;
use clap::{Arg, Command};

// Import the validation harness
use cqlite_tests::issue_35_validation_tests::Issue35ValidationHarness;

#[tokio::main]
async fn main() -> Result<()> {
    let matches = Command::new("issue-35-validator")
        .version("1.0")
        .about("Issue #35 Index/Summary/Statistics validation runner")
        .arg(
            Arg::new("data-dir")
                .long("data-dir")
                .value_name("DIR")
                .help("Specific data directory to validate")
        )
        .arg(
            Arg::new("component")
                .long("component")
                .value_name("COMPONENT")
                .help("Specific component to validate (index, summary, statistics)")
        )
        .arg(
            Arg::new("verbose")
                .long("verbose")
                .short('v')
                .help("Enable verbose output")
                .action(clap::ArgAction::SetTrue)
        )
        .arg(
            Arg::new("zero-tolerance")
                .long("zero-tolerance")
                .help("Require zero-diff compliance (fail on any differences)")
                .action(clap::ArgAction::SetTrue)
        )
        .get_matches();

    let verbose = matches.get_flag("verbose");
    let zero_tolerance = matches.get_flag("zero-tolerance");

    if verbose {
        println!("Issue #35 Validation Runner");
        println!("============================");
    }

    // Create and run validation harness
    let mut harness = Issue35ValidationHarness::new().await?;
    
    if verbose {
        println!("Running comprehensive validation...");
    }

    let summary = harness.run_comprehensive_validation().await?;
    let report = harness.generate_report();

    // Print the report
    report.print_report();

    // Determine exit code
    let exit_code = if zero_tolerance {
        if summary.zero_diff_compliance {
            0
        } else {
            1 // Fail if not zero-diff compliant
        }
    } else {
        if summary.passed_tests > 0 {
            0
        } else {
            1 // Fail only if no tests passed
        }
    };

    if verbose {
        println!("Validation completed with exit code: {}", exit_code);
    }

    std::process::exit(exit_code);
}