use anyhow::Result;
use clap::{Parser, Subcommand};
use colored::*;
use std::path::PathBuf;
use tracing::{error, info, warn};

mod cli;
mod commands;
mod config;
mod interactive;
mod tui;

use cli::*;

#[derive(Parser)]
#[command(name = "cqlite")]
#[command(about = "CQLite - High-performance embedded database with CQL support")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(author = "CQLite Team")]
struct Cli {
    /// Database file path
    #[arg(short, long, value_name = "FILE")]
    database: Option<PathBuf>,

    /// Configuration file path
    #[arg(short, long, value_name = "FILE")]
    config: Option<PathBuf>,

    /// Verbose output (-v, -vv, -vvv)
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

    /// Quiet mode (suppress output)
    #[arg(short, long)]
    quiet: bool,

    /// Output format
    #[arg(long, value_enum, default_value = "table")]
    format: OutputFormat,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Start interactive REPL mode
    Repl {
        /// Enable TUI mode
        #[arg(long)]
        tui: bool,
    },
    /// Execute a CQL query
    Query {
        /// CQL query to execute
        query: String,
        /// Show execution plan
        #[arg(long)]
        explain: bool,
        /// Show query timing
        #[arg(long)]
        timing: bool,
    },
    /// Import data from file
    Import {
        /// Input file path
        file: PathBuf,
        /// File format (csv, json, parquet)
        #[arg(long, value_enum)]
        format: ImportFormat,
        /// Target table name
        #[arg(long)]
        table: Option<String>,
    },
    /// Export data to file
    Export {
        /// Query or table name
        source: String,
        /// Output file path
        file: PathBuf,
        /// Output format
        #[arg(long, value_enum)]
        format: ExportFormat,
    },
    /// Database administration
    Admin {
        #[command(subcommand)]
        command: AdminCommands,
    },
    /// Schema management
    Schema {
        #[command(subcommand)]
        command: SchemaCommands,
    },
    /// Performance monitoring and benchmarks
    Bench {
        #[command(subcommand)]
        command: BenchCommands,
    },
}

#[derive(clap::ValueEnum, Clone)]
enum OutputFormat {
    Table,
    Json,
    Csv,
    Yaml,
}

#[derive(clap::ValueEnum, Clone)]
enum ImportFormat {
    Csv,
    Json,
    Parquet,
}

#[derive(clap::ValueEnum, Clone)]
enum ExportFormat {
    Csv,
    Json,
    Parquet,
    Sql,
}

#[derive(Subcommand)]
enum AdminCommands {
    /// Display database information
    Info,
    /// Compact database files
    Compact,
    /// Backup database
    Backup {
        /// Backup file path
        output: PathBuf,
    },
    /// Restore from backup
    Restore {
        /// Backup file path
        input: PathBuf,
    },
    /// Repair corrupted database
    Repair,
}

#[derive(Subcommand)]
enum SchemaCommands {
    /// List all tables
    List,
    /// Describe table structure
    Describe {
        /// Table name
        table: String,
    },
    /// Create table from DDL file
    Create {
        /// DDL file path
        file: PathBuf,
    },
    /// Drop table
    Drop {
        /// Table name
        table: String,
        /// Force drop without confirmation
        #[arg(long)]
        force: bool,
    },
}

#[derive(Subcommand)]
enum BenchCommands {
    /// Run read performance benchmark
    Read {
        /// Number of operations
        #[arg(short, long, default_value = "1000")]
        ops: u64,
        /// Number of concurrent threads
        #[arg(short, long, default_value = "1")]
        threads: u32,
    },
    /// Run write performance benchmark
    Write {
        /// Number of operations
        #[arg(short, long, default_value = "1000")]
        ops: u64,
        /// Number of concurrent threads
        #[arg(short, long, default_value = "1")]
        threads: u32,
    },
    /// Run mixed workload benchmark
    Mixed {
        /// Read percentage (0-100)
        #[arg(short, long, default_value = "70")]
        read_pct: u8,
        /// Number of operations
        #[arg(short, long, default_value = "1000")]
        ops: u64,
        /// Number of concurrent threads
        #[arg(short, long, default_value = "1")]
        threads: u32,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize logging based on verbosity
    let log_level = match (cli.quiet, cli.verbose) {
        (true, _) => "error",
        (false, 0) => "info",
        (false, 1) => "debug",
        (false, _) => "trace",
    };

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(log_level)).init();

    info!("Starting CQLite CLI v{}", env!("CARGO_PKG_VERSION"));

    // Load configuration
    let config = config::Config::load(cli.config)?;

    // Initialize database connection
    let db_path = cli
        .database
        .or(config.default_database.clone())
        .unwrap_or_else(|| PathBuf::from("cqlite.db"));

    match cli.command {
        Some(Commands::Repl { tui }) => {
            if tui {
                tui::start_tui_mode(&db_path, &config).await
            } else {
                interactive::start_repl_mode(&db_path, &config).await
            }
        }
        Some(Commands::Query {
            query,
            explain,
            timing,
        }) => commands::execute_query(&db_path, &query, explain, timing, cli.format).await,
        Some(Commands::Import {
            file,
            format,
            table,
        }) => commands::import_data(&db_path, &file, format, table.as_deref()).await,
        Some(Commands::Export {
            source,
            file,
            format,
        }) => commands::export_data(&db_path, &source, &file, format).await,
        Some(Commands::Admin { command }) => {
            commands::admin::handle_admin_command(&db_path, command).await
        }
        Some(Commands::Schema { command }) => {
            commands::schema::handle_schema_command(&db_path, command).await
        }
        Some(Commands::Bench { command }) => {
            commands::bench::handle_bench_command(&db_path, command).await
        }
        None => {
            // Default to interactive REPL mode
            interactive::start_repl_mode(&db_path, &config).await
        }
    }
}
