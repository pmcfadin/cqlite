//! CLI type definitions
//!
//! This module contains all the CLI command structures and enums
//! that are used throughout the application.

use clap::{Parser, Subcommand};
use std::path::PathBuf;

use crate::cli::{ExportFormat, ImportFormat, InfoOutputFormat, OutputFormat};

#[derive(Parser)]
#[command(name = "cqlite")]
#[command(about = "CQLite - High-performance embedded database with CQL support")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(author = "CQLite Team")]
pub struct Cli {
    /// Database file path
    #[arg(short, long, value_name = "FILE")]
    pub database: Option<PathBuf>,

    /// Configuration file path
    #[arg(short, long, value_name = "FILE")]
    pub config: Option<PathBuf>,

    /// Verbose output (-v, -vv, -vvv)
    #[arg(short, long, action = clap::ArgAction::Count)]
    pub verbose: u8,

    /// Quiet mode (suppress output)
    #[arg(short, long)]
    pub quiet: bool,

    /// Output format
    #[arg(long, value_enum, default_value = "table")]
    pub format: OutputFormat,

    /// Auto-detect SSTable format version
    #[arg(long)]
    pub auto_detect: bool,

    /// Override Cassandra version for compatibility (e.g., 3.11, 4.0, 5.0)
    #[arg(long, value_name = "VERSION")]
    pub cassandra_version: Option<String>,

    #[command(subcommand)]
    pub command: Option<Commands>,
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
        /// Input format
        #[arg(short, long, value_enum, default_value = "csv")]
        format: ImportFormat,
        /// Target table name
        #[arg(short, long)]
        table: String,
        /// Column mapping (col1:field1,col2:field2)
        #[arg(short, long)]
        mapping: Option<String>,
        /// Batch size for imports
        #[arg(long, default_value = "1000")]
        batch_size: usize,
    },
    /// Export data to file
    Export {
        /// Output file path
        file: PathBuf,
        /// Export format
        #[arg(short, long, value_enum, default_value = "csv")]
        format: ExportFormat,
        /// Source table name
        #[arg(short, long)]
        table: String,
        /// Query filter (WHERE clause)
        #[arg(short, long)]
        query: Option<String>,
    },
    /// Administrative commands
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
    /// Read and display SSTable contents with intelligent formatting
    #[command(name = "read-sstable")]
    ReadSstable {
        /// SSTable file path
        file: PathBuf,
        /// Output format
        #[arg(short, long, value_enum, default_value = "table")]
        format: OutputFormat,
        /// Limit number of rows displayed
        #[arg(short, long)]
        limit: Option<usize>,
        /// Skip number of rows
        #[arg(short, long, default_value = "0")]
        skip: usize,
        /// Show only keys
        #[arg(long)]
        keys_only: bool,
        /// Show raw binary data
        #[arg(long)]
        raw: bool,
        /// Enable detailed output
        #[arg(long)]
        verbose: bool,
    },
    /// Display database or SSTable information
    Info {
        /// Target file or database path
        path: Option<PathBuf>,
        /// Output format
        #[arg(short, long, value_enum, default_value = "text")]
        format: InfoOutputFormat,
        /// Show detailed information
        #[arg(short, long)]
        detailed: bool,
    },
}

#[derive(Subcommand)]
pub enum AdminCommands {
    /// Display database information
    Info,
    /// Compact database files
    Compact {
        /// Force compaction even if not needed
        #[arg(long)]
        force: bool,
    },
    /// Backup database
    Backup {
        /// Backup destination path
        destination: PathBuf,
        /// Compression level (0-9)
        #[arg(long, default_value = "6")]
        compression: u8,
    },
    /// Restore database from backup
    Restore {
        /// Backup file path
        backup: PathBuf,
        /// Force restore (overwrite existing)
        #[arg(long)]
        force: bool,
    },
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
    /// Create table from schema file
    Create {
        /// Schema file path
        schema: PathBuf,
    },
    /// Drop table
    Drop {
        /// Table name
        table: String,
        /// Force drop (ignore dependencies)
        #[arg(long)]
        force: bool,
    },
}

#[derive(Subcommand)]
pub enum BenchCommands {
    /// Run read performance benchmark
    Read {
        /// Number of operations
        #[arg(short, long, default_value = "10000")]
        operations: usize,
        /// Concurrency level
        #[arg(short, long, default_value = "1")]
        concurrency: usize,
        /// Target table
        #[arg(short, long)]
        table: Option<String>,
    },
    /// Run write performance benchmark
    Write {
        /// Number of operations
        #[arg(short, long, default_value = "10000")]
        operations: usize,
        /// Concurrency level
        #[arg(short, long, default_value = "1")]
        concurrency: usize,
        /// Target table
        #[arg(short, long)]
        table: Option<String>,
    },
    /// Run mixed workload benchmark
    Mixed {
        /// Number of operations
        #[arg(short, long, default_value = "10000")]
        operations: usize,
        /// Read/write ratio (0.0-1.0)
        #[arg(long, default_value = "0.8")]
        read_ratio: f64,
        /// Concurrency level
        #[arg(short, long, default_value = "1")]
        concurrency: usize,
    },
}
