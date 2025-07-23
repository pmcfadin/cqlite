use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing::info;

mod cli;
mod commands;
mod config;
mod interactive;
mod tui;

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
    format: cli::OutputFormat,

    /// Auto-detect SSTable format version
    #[arg(long)]
    auto_detect: bool,

    /// Override Cassandra version for compatibility (e.g., 3.11, 4.0, 5.0)
    #[arg(long, value_name = "VERSION")]
    cassandra_version: Option<String>,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
pub enum Commands {
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
        format: cli::ImportFormat,
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
        format: cli::ExportFormat,
        /// SSTable file path (for SSTable export)
        #[arg(long)]
        sstable: Option<PathBuf>,
        /// Schema file path (JSON or CQL format - auto-detected by extension)
        #[arg(long)]
        schema: Option<PathBuf>,
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
    /// Read SSTable directory or file with schema
    Read {
        /// SSTable directory path (e.g., users-46436710673711f0b2cf19d64e7cbecb) or legacy file path
        sstable_path: PathBuf,
        /// Schema file path (JSON or CQL format - auto-detected by extension)
        #[arg(long)]
        schema: PathBuf,
        /// Limit number of rows to display
        #[arg(long)]
        limit: Option<usize>,
        /// Skip number of rows
        #[arg(long)]
        skip: Option<usize>,
        /// Generation number to read (if not specified, reads latest)
        #[arg(long)]
        generation: Option<u32>,
    },
    /// Show SSTable directory or file information
    Info {
        /// SSTable directory path (e.g., users-46436710673711f0b2cf19d64e7cbecb) or legacy file path
        sstable_path: PathBuf,
        /// Show component details
        #[arg(long)]
        detailed: bool,
    },
}


#[derive(Subcommand)]
pub enum AdminCommands {
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
pub enum SchemaCommands {
    /// List all tables
    List,
    /// Describe table structure
    Describe {
        /// Table name
        table: String,
    },
    /// Create table from schema file (CQL DDL or JSON)
    Create {
        /// Schema file path (.cql for DDL or .json for schema) - format auto-detected
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
    /// Validate schema file (JSON or CQL format)
    Validate {
        /// Schema file path (.json or .cql) - format auto-detected by extension
        file: PathBuf,
    },
}

#[derive(Subcommand)]
pub enum BenchCommands {
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
            sstable,
            schema,
        }) => {
            if let (Some(sstable), Some(schema)) = (sstable, schema) {
                commands::export_sstable(&sstable, &schema, &file, format).await
            } else {
                commands::export_data(&db_path, &source, &file, format).await
            }
        }
        Some(Commands::Admin { command }) => {
            commands::admin::handle_admin_command(&db_path, command).await
        }
        Some(Commands::Schema { command }) => {
            commands::schema::handle_schema_command(&db_path, command).await
        }
        Some(Commands::Bench { command }) => {
            commands::bench::handle_bench_command(&db_path, command).await
        }
        Some(Commands::Read {
            sstable_path,
            schema,
            limit,
            skip,
            generation,
        }) => commands::read_sstable(&sstable_path, &schema, limit, skip, generation, cli.format, cli.auto_detect, cli.cassandra_version).await,
        Some(Commands::Info { sstable_path, detailed }) => commands::sstable_info(&sstable_path, detailed, cli.auto_detect, cli.cassandra_version).await,
        None => {
            // Default to interactive REPL mode
            interactive::start_repl_mode(&db_path, &config).await
        }
    }
}
