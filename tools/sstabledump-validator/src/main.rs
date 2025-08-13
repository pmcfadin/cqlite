use clap::{Parser, Subcommand};
use anyhow::Result;
use std::path::PathBuf;
use tracing::{info, error, warn};

mod validator;
mod parser;
mod comparator;
mod docker;
mod reporter;

use validator::SstableDumpValidator;

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
        Commands::Validate { path, fail_on_diff, detailed } => {
            info!("Running validation for: {:?}", path);
            let result = validator.validate_sstable(&path, fail_on_diff, detailed).await?;
            
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
        
        Commands::Compare { cassandra_dump, cqlite_dump, zero_tolerance } => {
            info!("Comparing dumps - Zero tolerance: {}", zero_tolerance);
            let comparison = validator.compare_dumps(&cassandra_dump, &cqlite_dump, zero_tolerance).await?;
            
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
            info!("Generating {} test cases (edge_cases: {})", count, edge_cases);
            validator.generate_test_data(count, edge_cases).await?;
        }
    }
    
    Ok(())
}