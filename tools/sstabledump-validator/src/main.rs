use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing::{error, info, warn};

mod comparator;
mod docker;
mod parser;
mod reconciliation;
mod reporter;
mod test_datasets;
mod validator;

use validator::{SstableDumpValidator, ValidationConfig, TestScope, SstableFormat, DataTypeCategory};

#[derive(Parser)]
#[command(name = "sstabledump-validator")]
#[command(about = "Zero-tolerance cell-by-cell validation harness for sstabledump compatibility")]
#[command(long_about = "
This tool validates CQLite's SSTable reading functionality by comparing its output 
cell-by-cell with Cassandra's native sstabledump utility. Any differences will cause 
CI to fail, ensuring perfect compatibility.

Referenced in GitHub Issues #25, #26, #28 for comprehensive validation.
")]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    #[arg(long, help = "Enable verbose logging")]
    verbose: bool,

    #[arg(long, help = "Output format for reports")]
    format: Option<String>,
}

#[derive(Subcommand)]
enum Commands {
    /// Run full validation suite with Docker Cassandra 5.0
    Validate {
        #[arg(help = "Path to SSTable file or directory")]
        path: PathBuf,

        #[arg(long, help = "Fail CI on ANY difference")]
        fail_on_diff: bool,

        #[arg(long, help = "Generate detailed comparison report")]
        detailed: bool,
    },

    /// Parse sstabledump output for comparison
    Parse {
        #[arg(help = "Path to sstabledump output file")]
        dump_file: PathBuf,

        #[arg(long, help = "Output parsed data as JSON")]
        json: bool,
    },

    /// Compare two parsed outputs
    Compare {
        #[arg(help = "Cassandra sstabledump output")]
        cassandra_dump: PathBuf,

        #[arg(help = "CQLite parsed output")]
        cqlite_dump: PathBuf,

        #[arg(long, help = "Zero tolerance mode - fail on any difference")]
        zero_tolerance: bool,
    },

    /// Setup Docker environment for testing
    Setup {
        #[arg(long, help = "Cassandra version to use")]
        version: Option<String>,
    },

    /// Generate test data using Docker Cassandra
    Generate {
        #[arg(long, help = "Number of test cases to generate")]
        count: Option<u32>,

        #[arg(long, help = "Include edge cases")]
        edge_cases: bool,
    },
    
    /// Run comprehensive validation with full corpus (Issue #38)
    Comprehensive {
        #[arg(long, help = "Test scope (quick|full|comprehensive)")]
        scope: Option<String>,
        
        #[arg(long, help = "Fail fast on first difference")]
        fail_fast: Option<bool>,
        
        #[arg(long, help = "Include BTI format validation")]
        include_bti: bool,
        
        #[arg(long, help = "Include all data types")]
        include_all_types: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize logging
    let log_level = if cli.verbose { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(format!("sstabledump_validator={}", log_level))
        .init();

    info!("Starting SSTableDump Validator");

    let mut validator = SstableDumpValidator::new().await?;

    match cli.command {
        Commands::Validate {
            path,
            fail_on_diff,
            detailed,
        } => {
            info!("Running validation for: {:?}", path);
            let result = validator
                .validate_sstable(&path, fail_on_diff, detailed)
                .await?;

            if result.has_differences() && fail_on_diff {
                error!("VALIDATION FAILED: Differences detected in cell-by-cell comparison");
                error!("This will cause CI to fail as requested");
                std::process::exit(1);
            }

            info!("Validation completed successfully");
        }

        Commands::Parse { dump_file, json } => {
            info!("Parsing dump file: {:?}", dump_file);
            let parsed = validator.parse_dump(&dump_file, json).await?;
            println!("{}", parsed);
        }

        Commands::Compare {
            cassandra_dump,
            cqlite_dump,
            zero_tolerance,
        } => {
            info!("Comparing dumps - Zero tolerance: {}", zero_tolerance);
            let comparison = validator
                .compare_dumps(&cassandra_dump, &cqlite_dump, zero_tolerance)
                .await?;

            if comparison.has_differences() && zero_tolerance {
                error!("ZERO TOLERANCE VIOLATION: Differences found");
                std::process::exit(1);
            }

            println!("{}", comparison.report());
        }

        Commands::Setup { version } => {
            let version = version.unwrap_or_else(|| "5.0".to_string());
            info!("Setting up Docker environment with Cassandra {}", version);
            validator.setup_docker_environment(&version).await?;
        }

        Commands::Generate { count, edge_cases } => {
            let count = count.unwrap_or(10);
            info!(
                "Generating {} test cases (edge_cases: {})",
                count, edge_cases
            );
            validator.generate_test_data(count, edge_cases).await?;
        }

        Commands::Comprehensive {
            scope,
            fail_fast,
            include_bti,
            include_all_types,
        } => {
            info!("Starting comprehensive validation for Issue #38");
            
            // Build configuration
            let test_scope = match scope.as_deref() {
                Some("quick") => TestScope::Quick,
                Some("comprehensive") => TestScope::Comprehensive,
                _ => TestScope::Full, // Default
            };
            
            let mut sstable_formats = vec![SstableFormat::Big];
            if include_bti {
                sstable_formats.push(SstableFormat::Bti);
            }
            
            let data_types = if include_all_types {
                vec![
                    DataTypeCategory::BasicTypes,
                    DataTypeCategory::Collections,
                    DataTypeCategory::UserDefinedTypes,
                    DataTypeCategory::ComplexKeys,
                    DataTypeCategory::StaticColumns,
                    DataTypeCategory::Counters,
                    DataTypeCategory::TimeSeries,
                    DataTypeCategory::Tombstones,
                    DataTypeCategory::ReconciliationScenarios,
                    DataTypeCategory::LargeData,
                    DataTypeCategory::EdgeCases,
                ]
            } else {
                vec![
                    DataTypeCategory::BasicTypes,
                    DataTypeCategory::Collections,
                    DataTypeCategory::ComplexKeys,
                    DataTypeCategory::StaticColumns,
                    DataTypeCategory::Counters,
                    DataTypeCategory::TimeSeries,
                    DataTypeCategory::Tombstones,
                    DataTypeCategory::ReconciliationScenarios,
                ]
            };
            
            let config = ValidationConfig {
                zero_tolerance: true, // Always true for Issue #38
                fail_fast: fail_fast.unwrap_or(true),
                detailed_reports: true,
                test_scope,
                sstable_formats,
                data_types,
            };
            
            info!("Configuration: {:?}", config);
            
            // Run comprehensive validation
            let results = validator.run_comprehensive_validation(config).await?;
            
            // Analyze results
            let total = results.len();
            let failed = results.iter()
                .filter(|r| matches!(r.validation_status, validator::ValidationStatus::Failed))
                .count();
            let errors = results.iter()
                .filter(|r| matches!(r.validation_status, validator::ValidationStatus::Error))
                .count();
            
            info!("Comprehensive validation completed:");
            info!("  Total SSTables: {}", total);
            info!("  Failed: {}", failed);
            info!("  Errors: {}", errors);
            
            // Generate summary report
            for result in &results {
                match result.validation_status {
                    validator::ValidationStatus::Perfect => {
                        info!("✅ {}: Perfect parity", result.table_name);
                    }
                    validator::ValidationStatus::Failed => {
                        error!("❌ {}: {} differences found", result.table_name, result.differences_found);
                    }
                    validator::ValidationStatus::Error => {
                        error!("🚨 {}: Validation error: {}", result.table_name, 
                               result.error_message.as_deref().unwrap_or("Unknown error"));
                    }
                    validator::ValidationStatus::WithinTolerance => {
                        warn!("⚠️  {}: Within tolerance ({} differences)", result.table_name, result.differences_found);
                    }
                }
            }
            
            // Exit with appropriate code for CI gating
            if failed > 0 || errors > 0 {
                error!("🚫 COMPREHENSIVE VALIDATION FAILED");
                error!("   This is a MANDATORY CI gate for Issue #38");
                error!("   Perfect SSTable compatibility is required");
                std::process::exit(1);
            } else {
                info!("🎉 ALL COMPREHENSIVE VALIDATIONS PASSED");
                info!("   CI gate allows merge to proceed");
            }
        }
    }

    Ok(())
}
