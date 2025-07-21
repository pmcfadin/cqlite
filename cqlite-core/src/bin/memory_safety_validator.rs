//! Memory Safety Validator Binary
//! 
//! Standalone binary to run comprehensive memory safety validation tests
//! for the CQLite core database engine.

use clap::{Arg, Command};
use std::process;
use tokio;

use cqlite_core::memory_safety_tests::MemorySafetyTests;
use cqlite_core::memory_safety_runner::MemorySafetyRunner;

#[tokio::main]
async fn main() {
    let matches = Command::new("Memory Safety Validator")
        .version("1.0")
        .author("CQLite Team")
        .about("Validates memory safety of CQLite core components")
        .arg(
            Arg::new("tool")
                .short('t')
                .long("tool")
                .value_name("TOOL")
                .help("Specific tool to use (miri, valgrind, asan, all)")
                .default_value("all")
        )
        .arg(
            Arg::new("stress")
                .short('s')
                .long("stress")
                .help("Run stress tests")
                .action(clap::ArgAction::SetTrue)
        )
        .arg(
            Arg::new("verbose")
                .short('v')
                .long("verbose")
                .help("Verbose output")
                .action(clap::ArgAction::SetTrue)
        )
        .arg(
            Arg::new("check-tools")
                .long("check-tools")
                .help("Check available tools and exit")
                .action(clap::ArgAction::SetTrue)
        )
        .get_matches();

    let runner = MemorySafetyRunner::new();
    let tests = MemorySafetyTests::new();

    if matches.get_flag("check-tools") {
        runner.print_available_tools();
        return;
    }

    let verbose = matches.get_flag("verbose");
    let tool = matches.get_one::<String>("tool").unwrap();

    if verbose {
        println!("Memory Safety Validator starting...");
        runner.print_available_tools();
        println!();
    }

    let mut exit_code = 0;

    // Run built-in memory safety tests first
    println!("=== Running Built-in Memory Safety Tests ===");
    if let Err(e) = tests.run_all_tests().await {
        eprintln!("Built-in memory safety tests failed: {}", e);
        exit_code = 1;
    }

    // Run stress tests if requested
    if matches.get_flag("stress") {
        println!("\n=== Running Additional Stress Tests ===");
        if let Err(e) = runner.run_stress_tests() {
            eprintln!("Stress tests failed: {}", e);
            exit_code = 1;
        }
    }

    // Run specific or all external tools
    match tool.as_str() {
        "miri" => {
            println!("\n=== Running Miri Tests ===");
            if let Err(e) = runner.run_miri_tests() {
                eprintln!("Miri tests failed: {}", e);
                exit_code = 1;
            }
        }
        "valgrind" => {
            println!("\n=== Running Valgrind Tests ===");
            if let Err(e) = runner.run_valgrind_tests() {
                eprintln!("Valgrind tests failed: {}", e);
                exit_code = 1;
            }
        }
        "asan" => {
            println!("\n=== Running AddressSanitizer Tests ===");
            if let Err(e) = runner.run_asan_tests() {
                eprintln!("AddressSanitizer tests failed: {}", e);
                exit_code = 1;
            }
        }
        "all" => {
            println!("\n=== Running All Available External Tools ===");
            if let Err(e) = runner.run_all_available_tests() {
                eprintln!("Some external tests failed: {}", e);
                exit_code = 1;
            }
        }
        _ => {
            eprintln!("Unknown tool: {}. Use miri, valgrind, asan, or all", tool);
            exit_code = 1;
        }
    }

    if exit_code == 0 {
        println!("\n🎉 All memory safety validation completed successfully!");
    } else {
        println!("\n❌ Memory safety validation detected issues!");
    }

    process::exit(exit_code);
}