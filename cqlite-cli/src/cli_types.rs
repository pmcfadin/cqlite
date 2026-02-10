//! CLI type definitions
//!
//! This module contains all the CLI command structures and enums
//! that are used throughout the application.

use clap::{Args, Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

use crate::cli::{ExportFormat, ImportFormat, InfoOutputFormat, OutputFormat};

/// Output mode for query results (distinct from display format)
/// Mirrors OutputFormat variants for consistency with --format flag.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum OutputMode {
    Table,
    Json,
    Csv,
    /// Parquet binary format (requires --output flag)
    Parquet,
}

impl OutputMode {
    /// Convert OutputMode to its string representation
    #[allow(dead_code)]
    pub fn as_str(&self) -> &'static str {
        match self {
            OutputMode::Table => "table",
            OutputMode::Json => "json",
            OutputMode::Csv => "csv",
            OutputMode::Parquet => "parquet",
        }
    }
}

#[derive(Parser)]
#[command(name = "cqlite")]
#[command(about = "CQLite - Local SSTable query tool with cqlsh-compatible interface")]
#[command(
    long_about = "CQLite provides cqlsh-compatible access to Apache Cassandra 5.0 SSTables locally without cluster dependencies. Supports interactive REPL and one-shot query modes.\n\nIngestion Model: Provide --schema and --data-dir together to trigger schema loading + dataset discovery for query execution.\n\nNote: Timestamps are displayed in UTC for M2 milestone."
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

    /// CQL (.cql) or JSON (.json) schema files; triggers schema loading for ingestion.
    /// When combined with --data-dir, enables schema-aware query execution.
    /// Repeatable; order defines precedence
    #[arg(long, value_name = "PATH", env = "CQLITE_SCHEMA")]
    pub schema: Option<PathBuf>,

    /// Dataset name for test data (e.g., test_basic, test_collections)
    /// Mutually exclusive with --data-dir. Looks for datasets in CQLITE_DATASETS_ROOT/sstables/{dataset}/
    #[arg(long, value_name = "DATASET", conflicts_with = "data_dir")]
    pub dataset: Option<String>,

    /// Cassandra data directory root (e.g., /var/lib/cassandra/data).
    /// Combined with --schema, triggers dataset discovery and ingestion.
    /// Mutually exclusive with --dataset. For production Cassandra directory layouts.
    #[arg(
        long,
        value_name = "DIR",
        env = "CQLITE_DATA_DIR",
        conflicts_with = "dataset"
    )]
    pub data_dir: Option<PathBuf>,

    /// Execute a single CQL statement in one-shot mode (alias: --query)
    #[arg(short = 'e', long, visible_alias = "query", value_name = "CQL")]
    pub execute: Option<String>,

    /// Execute statements from a file (semicolon-terminated)
    #[arg(short = 'f', long, value_name = "CQL_FILE")]
    pub file: Option<PathBuf>,

    /// Output format for query results. Takes precedence over --format when specified.
    /// Applies to: --execute/-e, --query, --file, and query subcommand.
    /// (table = cqlsh-compatible format)
    #[arg(long, value_enum, env = "CQLITE_OUT")]
    pub out: Option<OutputMode>,

    /// Output file path for query results. If not specified, output goes to stdout.
    /// Required when format is 'parquet' (binary format cannot be written to stdout).
    #[arg(short = 'o', long, value_name = "FILE", env = "CQLITE_OUTPUT")]
    pub output: Option<PathBuf>,

    /// Overwrite output file if it exists (default: error if file exists)
    #[arg(long, requires = "output")]
    pub overwrite: bool,

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

    /// Enable write mode (requires --write-dir)
    #[arg(long, env = "CQLITE_WRITABLE", requires = "write_dir")]
    pub writable: bool,

    /// Directory for write operations (WAL and SSTable output)
    #[arg(long, value_name = "DIR", env = "CQLITE_WRITE_DIR")]
    pub write_dir: Option<PathBuf>,

    /// JSON mutation to write (can be repeated). Requires --writable mode.
    /// Format: {"table":"ks.tbl","partition_key":{"col":"val"},"operations":[{"Write":{"column":"c","value":"v"}}],"timestamp_micros":123}
    #[arg(long, value_name = "JSON", requires = "writable")]
    pub mutation: Vec<String>,

    /// File containing mutations (JSONL format, one JSON mutation per line). Requires --writable mode.
    #[arg(long, value_name = "FILE", requires = "writable")]
    pub mutations_file: Option<PathBuf>,

    /// Force flush memtable to SSTable after writes. Requires --writable mode.
    #[arg(long, requires = "writable")]
    pub flush: bool,

    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Start interactive REPL mode (basic text interface)
    #[command(
        long_about = "Interactive REPL supporting meta-commands (:config, :schema, :status, :health) and CQL queries (SELECT, DESCRIBE, USE). This is a basic text-based REPL with status line. For the full terminal UI, use 'cqlite tui'."
    )]
    Repl,
    /// Start Terminal UI mode (full-screen interface)
    #[command(
        long_about = "Full-screen terminal UI with panels for tables, query results, and history. Navigate with Tab, toggle panels with F2-F4, reset layout with F5. Exit with Ctrl+C or Esc. For basic REPL mode, use 'cqlite repl'."
    )]
    Tui,
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
        /// Maximum number of rows to export
        #[arg(short, long)]
        limit: Option<usize>,
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
    /// Run background maintenance (compaction) - requires --writable mode (Issue #392)
    #[command(
        long_about = "Perform incremental background compaction work within a time budget. Call repeatedly from a background task for continuous compaction. Example: cqlite maintenance --budget-ms 100 --writable --write-dir /path/to/data"
    )]
    Maintenance(MaintenanceArgs),
    /// Display write engine statistics - requires --writable mode (Issue #392)
    #[command(name = "write-stats")]
    #[command(
        long_about = "Show current write engine statistics including memtable size, row count, WAL size, and generation number. Example: cqlite write-stats --writable --write-dir /path/to/data"
    )]
    WriteStats,
    /// Export SSTables for Cassandra import - requires --writable mode (Issue #392)
    #[command(name = "export-sstable")]
    #[command(
        long_about = "Export data from the write engine as Cassandra-compatible SSTables. Use the maintenance subcommand to compact before export. Pre-export compaction will be available in M5.3. Example: cqlite export-sstable /tmp/export --writable --write-dir /path/to/data"
    )]
    ExportSstable(ExportSstableArgs),
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

/// Arguments for the maintenance subcommand (Issue #392)
#[derive(Args, Debug, Clone)]
pub struct MaintenanceArgs {
    /// Time budget in milliseconds for this maintenance step
    #[arg(long, default_value = "100")]
    pub budget_ms: u64,
}

/// Arguments for the export-sstable subcommand (Issue #392)
#[derive(Args, Debug, Clone)]
pub struct ExportSstableArgs {
    /// Output directory for exported SSTables
    pub output: PathBuf,
    /// Keyspace name for the exported SSTable
    #[arg(long, default_value = "export")]
    pub keyspace: String,
    /// Table name for the exported SSTable
    #[arg(long, default_value = "data")]
    pub table: String,
    /// Skip compaction before export (no-op until M5.3, compaction is never enabled)
    #[arg(long)]
    pub skip_compact: bool,
    /// Skip validation after export
    #[arg(long)]
    pub skip_validate: bool,
}
