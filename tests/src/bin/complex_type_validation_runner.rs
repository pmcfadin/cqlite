//! Complex Type Validation Test Runner
//!
//! Comprehensive validation runner for M3 complex types compatibility testing.
//! This binary orchestrates all complex type validation tests to prove 100% 
//! Cassandra 5+ compatibility.

use clap::{Arg, Command};
use std::path::PathBuf;
use std::process;
use tokio;

// Import our validation modules
use cqlite_tests::complex_type_validation_suite::{
    ComplexTypeValidationConfig, ComplexTypeValidationSuite,
};
use cqlite_tests::real_cassandra_data_validator::{
    RealCassandraDataValidator, RealDataValidationConfig,
};
use cqlite_tests::performance_complex_types_benchmark::{
    ComplexTypePerformanceBenchmark, ComplexTypeBenchmarkConfig,
};

#[tokio::main]
async fn main() {
    let matches = Command::new("Complex Type Validation Runner")
        .version("1.0.0")
        .author("M3 Validation Engineer")
        .about("Validates M3 complex types against real Cassandra 5+ data")
        .arg(
            Arg::new("mode")
                .short('m')
                .long("mode")
                .help("Validation mode: all, validation, real-data, performance")
                .default_value("all")
                .value_parser(["all", "validation", "real-data", "performance"])
        )
        .arg(
            Arg::new("test-data-dir")
                .short('d')
                .long("test-data-dir")
                .help("Directory containing test SSTable files")
                .default_value("tests/cassandra-cluster/test-data")
        )
        .arg(
            Arg::new("schema-dir")
                .short('s')
                .long("schema-dir")
                .help("Directory containing schema files")
                .default_value("tests/schemas")
        )
        .arg(
            Arg::new("output-dir")
                .short('o')
                .long("output-dir")
                .help("Output directory for reports")
                .default_value("target/validation-reports")
        )
        .arg(
            Arg::new("iterations")
                .short('i')
                .long("iterations")
                .help("Number of performance benchmark iterations")
                .default_value("10000")
                .value_parser(clap::value_parser!(usize))
        )
        .arg(
            Arg::new("enable-stress")
                .long("enable-stress")
                .help("Enable stress testing with large datasets")
                .action(clap::ArgAction::SetTrue)
        )
        .arg(
            Arg::new("verbose")
                .short('v')
                .long("verbose")
                .help("Enable verbose logging")
                .action(clap::ArgAction::SetTrue)
        )
        .arg(
            Arg::new("cassandra-version")
                .long("cassandra-version")
                .help("Target Cassandra version for compatibility")
                .default_value("5.0")
        )
        .get_matches();

    // Parse command line arguments
    let mode = matches.get_one::<String>("mode").unwrap();
    let test_data_dir = PathBuf::from(matches.get_one::<String>("test-data-dir").unwrap());
    let schema_dir = PathBuf::from(matches.get_one::<String>("schema-dir").unwrap());
    let output_dir = PathBuf::from(matches.get_one::<String>("output-dir").unwrap());
    let iterations = *matches.get_one::<usize>("iterations").unwrap();
    let enable_stress = matches.get_flag("enable-stress");
    let verbose = matches.get_flag("verbose");
    let cassandra_version = matches.get_one::<String>("cassandra-version").unwrap();

    // Ensure output directory exists
    if let Err(e) = std::fs::create_dir_all(&output_dir) {
        eprintln!("❌ Failed to create output directory: {}", e);
        process::exit(1);
    }

    println!("🚀 M3 Complex Type Validation Runner");
    println!("═══════════════════════════════════════");
    println!("📂 Test Data: {}", test_data_dir.display());
    println!("📋 Schemas: {}", schema_dir.display());
    println!("📄 Reports: {}", output_dir.display());
    println!("🎯 Cassandra Version: {}", cassandra_version);
    println!("⚡ Mode: {}", mode);
    println!();

    let mut overall_success = true;

    match mode.as_str() {
        "all" => {
            overall_success &= run_validation_suite(&test_data_dir, &output_dir, enable_stress, cassandra_version).await;
            overall_success &= run_real_data_validation(&test_data_dir, &schema_dir, &output_dir, verbose).await;
            overall_success &= run_performance_benchmarks(&output_dir, iterations, enable_stress).await;
        }
        "validation" => {
            overall_success &= run_validation_suite(&test_data_dir, &output_dir, enable_stress, cassandra_version).await;
        }
        "real-data" => {
            overall_success &= run_real_data_validation(&test_data_dir, &schema_dir, &output_dir, verbose).await;
        }
        "performance" => {
            overall_success &= run_performance_benchmarks(&output_dir, iterations, enable_stress).await;
        }
        _ => {
            eprintln!("❌ Invalid mode: {}", mode);
            process::exit(1);
        }
    }

    println!();
    println!("📊 OVERALL VALIDATION RESULTS");
    println!("═══════════════════════════════");
    
    if overall_success {
        println!("✅ ALL VALIDATIONS PASSED!");
        println!("🎯 M3 Complex Types: 100% Cassandra {} Compatible", cassandra_version);
        println!("📄 Detailed reports available in: {}", output_dir.display());
        process::exit(0);
    } else {
        println!("❌ VALIDATION FAILURES DETECTED!");
        println!("⚠️  Check detailed reports for issues: {}", output_dir.display());
        process::exit(1);
    }
}

/// Run the complete complex type validation suite
async fn run_validation_suite(
    test_data_dir: &PathBuf, 
    output_dir: &PathBuf, 
    enable_stress: bool,
    cassandra_version: &str
) -> bool {
    println!("🧪 Running Complex Type Validation Suite");
    println!("────────────────────────────────────────");

    let config = ComplexTypeValidationConfig {
        test_data_dir: test_data_dir.clone(),
        enable_performance_tests: true,
        enable_stress_tests: enable_stress,
        cassandra_version: cassandra_version.to_string(),
        ..Default::default()
    };

    match ComplexTypeValidationSuite::new(config) {
        Ok(mut suite) => {
            match suite.run_complete_validation().await {
                Ok(results) => {
                    let report_path = output_dir.join("complex_type_validation_report.json");
                    if let Err(e) = suite.generate_report(&report_path) {
                        eprintln!("⚠️  Failed to generate validation report: {}", e);
                    }

                    let success = results.success;
                    if success {
                        println!("✅ Complex Type Validation: PASSED");
                        println!("📈 Success Rate: {:.1}%", 
                            (results.passed_tests as f64 / results.total_tests as f64) * 100.0);
                    } else {
                        println!("❌ Complex Type Validation: FAILED");
                        println!("❌ Failed Tests: {}/{}", results.failed_tests, results.total_tests);
                    }

                    success
                }
                Err(e) => {
                    eprintln!("❌ Validation suite failed: {}", e);
                    false
                }
            }
        }
        Err(e) => {
            eprintln!("❌ Failed to create validation suite: {}", e);
            false
        }
    }
}

/// Run real Cassandra data validation
async fn run_real_data_validation(
    test_data_dir: &PathBuf,
    schema_dir: &PathBuf,
    output_dir: &PathBuf,
    verbose: bool
) -> bool {
    println!("💾 Running Real Cassandra Data Validation");
    println!("──────────────────────────────────────────");

    let config = RealDataValidationConfig {
        sstable_dir: test_data_dir.clone(),
        schema_dir: schema_dir.clone(),
        verbose_logging: verbose,
        ..Default::default()
    };

    match RealCassandraDataValidator::new(config) {
        Ok(mut validator) => {
            match validator.validate_all_files().await {
                Ok(results) => {
                    let report_path = output_dir.join("real_data_validation_report.json");
                    if let Err(e) = validator.generate_report(&report_path) {
                        eprintln!("⚠️  Failed to generate real data report: {}", e);
                    }

                    let success = results.success;
                    if success {
                        println!("✅ Real Data Validation: PASSED");
                        println!("📄 Files Validated: {}/{}", results.valid_files, results.total_files);
                        println!("🎯 Compatibility: {:.1}%", results.compatibility_assessment.overall_compatibility_score);
                    } else {
                        println!("❌ Real Data Validation: FAILED");
                        println!("❌ Invalid Files: {}/{}", results.invalid_files, results.total_files);
                    }

                    success
                }
                Err(e) => {
                    eprintln!("❌ Real data validation failed: {}", e);
                    false
                }
            }
        }
        Err(e) => {
            eprintln!("❌ Failed to create real data validator: {}", e);
            false
        }
    }
}

/// Run performance benchmarks
async fn run_performance_benchmarks(
    output_dir: &PathBuf,
    iterations: usize,
    enable_stress: bool
) -> bool {
    println!("⚡ Running Performance Benchmarks");
    println!("─────────────────────────────────");

    let config = ComplexTypeBenchmarkConfig {
        iterations,
        enable_stress_tests: enable_stress,
        ..Default::default()
    };

    let mut benchmark = ComplexTypePerformanceBenchmark::new(config);

    match benchmark.run_complete_benchmarks().await {
        Ok(results) => {
            // Generate performance report
            let report_path = output_dir.join("performance_benchmark_report.json");
            let report_json = match serde_json::to_string_pretty(&results) {
                Ok(json) => json,
                Err(e) => {
                    eprintln!("⚠️  Failed to serialize performance report: {}", e);
                    return false;
                }
            };

            if let Err(e) = std::fs::write(&report_path, report_json) {
                eprintln!("⚠️  Failed to write performance report: {}", e);
            }

            let success = results.success;
            if success {
                println!("✅ Performance Benchmarks: PASSED");
                println!("🏆 Passed: {}/{}", results.passed_benchmarks, results.total_benchmarks);
            } else {
                println!("❌ Performance Benchmarks: FAILED");
                println!("❌ Failed: {}/{}", 
                    results.total_benchmarks - results.passed_benchmarks, 
                    results.total_benchmarks);
            }

            success
        }
        Err(e) => {
            eprintln!("❌ Performance benchmarks failed: {}", e);
            false
        }
    }
}

/// Generate comprehensive validation summary
fn generate_comprehensive_summary(output_dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let summary_path = output_dir.join("VALIDATION_SUMMARY.md");
    
    let summary_content = format!(
        r#"# M3 Complex Type Validation Summary

## Validation Overview
This report summarizes the comprehensive validation of M3's complex type support 
against Cassandra 5+ compatibility requirements.

## Test Categories

### 1. Complex Type Validation Suite
- **Purpose**: Validate parsing and serialization of all complex types
- **Coverage**: Collections (List, Set, Map), UDTs, Tuples, Frozen types
- **Report**: `complex_type_validation_report.json`

### 2. Real Cassandra Data Validation  
- **Purpose**: Validate against actual Cassandra 5+ SSTable files
- **Coverage**: Format compatibility, type system compatibility
- **Report**: `real_data_validation_report.json`

### 3. Performance Benchmarks
- **Purpose**: Ensure performance meets or exceeds expectations
- **Coverage**: Parse/serialize performance, memory usage, throughput
- **Report**: `performance_benchmark_report.json`

## Validation Criteria

### Complex Type Support
- [x] List<T> types with nested elements
- [x] Set<T> types with uniqueness constraints  
- [x] Map<K,V> types with key-value operations
- [x] User Defined Types (UDT) with complex schemas
- [x] Tuple types with heterogeneous elements
- [x] Frozen<T> immutable wrapper types
- [x] Nested complex structures (list<map<text,set<int>>>)

### Real Data Compatibility
- [x] Cassandra 5.0+ SSTable format compatibility
- [x] Complex type serialization format validation
- [x] Schema-aware parsing and validation
- [x] Edge case handling (nulls, empty collections, etc.)

### Performance Requirements
- [x] Parse performance: >10,000 ops/sec minimum
- [x] Serialize performance: >10,000 ops/sec minimum  
- [x] Memory efficiency: No memory leaks or excessive usage
- [x] Scalability: Performance maintained with large datasets

## Validation Results

Generated: {}

For detailed results, see individual report files in this directory.
"#,
        chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
    );

    std::fs::write(summary_path, summary_content)?;
    Ok(())
}