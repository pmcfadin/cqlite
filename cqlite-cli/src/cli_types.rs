//! CLI type definitions
//!
//! This module contains all the CLI command structures and enums
//! that are used throughout the application.

use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

use crate::cli::{ExportFormat, ImportFormat, InfoOutputFormat, OutputFormat};

/// Output mode for query results (distinct from display format)
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum OutputMode {
    Table,
    Json,
    Csv,
}

impl OutputMode {
    /// Convert OutputMode to its string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            OutputMode::Table => "table",
            OutputMode::Json => "json",
            OutputMode::Csv => "csv",
        }
    }
}

#[derive(Parser)]
#[command(name = "cqlite")]
#[command(about = "CQLite - Local SSTable query tool with cqlsh-compatible interface")]
#[command(
    long_about = "CQLite provides cqlsh-compatible access to Apache Cassandra 5.0 SSTables locally without cluster dependencies. Supports interactive REPL and one-shot query modes."
)]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(author = "CQLite Team")]
pub struct Cli {
    /// Database file path
    #[arg(short, long, value_name = "FILE")]
    pub database: Option<PathBuf>,

    /// Load config (TOML/YAML/JSON). Precedence: flags > env > file > defaults
    #[arg(short, long, value_name = "FILE")]
    pub config: Option<PathBuf>,

    /// Verbose output (-v, -vv, -vvv for increasing verbosity)
    #[arg(short, long, action = clap::ArgAction::Count)]
    pub verbose: u8,

    /// Quiet mode (suppress non-essential output)
    #[arg(short, long)]
    pub quiet: bool,

    /// Output format
    #[arg(long, value_enum, default_value = "table")]
    pub format: OutputFormat,

    /// Enable best-effort auto detection (format/version) when available
    #[arg(long)]
    pub auto_detect: bool,

    /// Hint (e.g., 5.0) for format compatibility
    #[arg(long, value_name = "VER")]
    pub cassandra_version: Option<String>,

    /// File (.cql or .json) or directory containing schemas. Repeatable; order defines precedence
    #[arg(long, value_name = "PATH", env = "CQLITE_SCHEMA")]
    pub schema: Option<PathBuf>,

    /// Dataset name for test data (e.g., test_basic, test_collections)
    /// Mutually exclusive with --data-dir. Looks for datasets in CQLITE_DATASETS_ROOT/sstables/{dataset}/
    #[arg(long, value_name = "DATASET", conflicts_with = "data_dir")]
    pub dataset: Option<String>,

    /// Cassandra data directory root (e.g., /var/lib/cassandra/data)
    /// Mutually exclusive with --dataset. For production Cassandra directory layouts.
    #[arg(
        long,
        value_name = "DIR",
        env = "CQLITE_DATA_DIR",
        conflicts_with = "dataset"
    )]
    pub data_dir: Option<PathBuf>,

    /// Execute a single CQL statement in one-shot mode
    #[arg(short = 'e', long, value_name = "CQL")]
    pub execute: Option<String>,

    /// Execute statements from a file (semicolon-terminated)
    #[arg(short = 'f', long, value_name = "CQL_FILE")]
    pub file: Option<PathBuf>,

    /// Output format for query results (table = cqlsh-compatible)
    #[arg(long, value_enum, env = "CQLITE_OUT")]
    pub out: Option<OutputMode>,

    /// Cap rows
    #[arg(long, value_name = "N", env = "CQLITE_LIMIT")]
    pub limit: Option<usize>,

    /// Reader and display pagination size
    #[arg(long, value_name = "N", env = "CQLITE_PAGE_SIZE")]
    pub page_size: Option<usize>,

    /// Disable colored output
    #[arg(long, env = "CQLITE_NO_COLOR")]
    pub no_color: bool,

    /// EXPERIMENTAL: Fallback to read-sstable for SELECT when ingestion unavailable (temporary, will be removed in M3)
    #[arg(long, env = "CQLITE_ENABLE_SELECT_FALLBACK")]
    pub enable_select_fallback: bool,

    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Start interactive REPL mode with cqlsh-compatible commands
    #[command(
        long_about = "Interactive REPL supporting meta-commands (:config, :schema, :status, :health) and CQL queries (SELECT, DESCRIBE, USE). Launch with 'cqlite repl' or default 'cqlite'."
    )]
    Repl {
        /// Enable TUI mode
        #[arg(long)]
        tui: bool,
    },
    /// Execute CQL queries against local SSTable data
    #[command(
        long_about = "Friendly wrapper for one-shot query execution. Example: cqlite query --schema schemas/ --data-dir ./test-data -e \"SELECT * FROM ks.users LIMIT 5\" --out json"
    )]
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
    /// Low-level SSTable inspection and reading
    #[command(name = "read-sstable")]
    #[command(
        long_about = "Direct SSTable inspection bypassing schema. Example: cqlite read-sstable ./test-data/datasets/sstables/test_basic/simple_table/na-1-big-Data.db --schema schema.cql --format table"
    )]
    ReadSstable {
        /// SSTable file or directory path
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
    /// Display file metadata and statistics
    #[command(
        long_about = "Show SSTable or database file metadata, stats, and optional validation. Example: cqlite info ./test-data/datasets/sstables/test_basic --format json --detailed"
    )]
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
    /// Load schemas from files or directories
    Load {
        /// Schema file (.cql or .json) or directory paths (repeatable, processed in order)
        #[arg(required = true)]
        paths: Vec<PathBuf>,
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
