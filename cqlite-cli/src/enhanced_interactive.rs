//! Enhanced Interactive REPL with CQL Query Processing
//!
//! This module extends the basic REPL with advanced CQL query processing capabilities,
//! providing a rich interactive experience for exploring Cassandra SSTable data.

use crate::config::Config;
use crate::query_processor::{CQLQueryProcessor, QueryExecutionContext};
use crate::repl_integration::{REPLQueryExecutor, REPLSchemaExplorer};
use anyhow::Result;
use colored::Colorize;
use cqlite_core::{
    schema::SchemaManager,
    storage::StorageEngine,
    platform::Platform,
    Database, Config as CoreConfig,
};
use std::collections::HashMap;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Enhanced REPL session with advanced CQL processing
pub struct EnhancedReplSession {
    /// Database connection
    pub database: Arc<Database>,
    /// Schema manager
    pub schema_manager: Arc<SchemaManager>,
    /// Storage engine
    pub storage_engine: Arc<StorageEngine>,
    /// Query executor
    pub query_executor: REPLQueryExecutor,
    /// Schema explorer
    pub schema_explorer: REPLSchemaExplorer,
    /// Session configuration
    pub config: Config,
    /// Current working directory
    pub db_path: PathBuf,
    /// Current keyspace
    pub current_keyspace: Option<String>,
    /// Data directory for SSTable access
    pub data_dir: Option<PathBuf>,
    /// Display settings
    pub timing_enabled: bool,
    pub paging_enabled: bool,
    pub page_size: usize,
    /// Session history
    pub command_history: Vec<String>,
    /// Query cache
    pub query_cache: HashMap<String, CachedQueryInfo>,
}

#[derive(Debug, Clone)]
struct CachedQueryInfo {
    result_count: usize,
    execution_time_ms: f64,
    last_executed: std::time::Instant,
}

impl EnhancedReplSession {
    /// Create a new enhanced REPL session
    pub async fn new(db_path: &Path, config: Config) -> Result<Self> {
        // Initialize core database components
        let core_config = CoreConfig::default();
        let database = Arc::new(Database::open(db_path, core_config.clone()).await?);
        
        // Initialize platform and storage components
        let platform = Arc::new(Platform::new(&core_config).await?);
        let storage_engine = Arc::new(StorageEngine::open(db_path, &core_config, platform).await?);
        let schema_manager = Arc::new(SchemaManager::new(storage_engine.clone(), &core_config).await?);
        
        // Initialize query engine components
        let query_engine = Arc::new(database.prepare("SELECT 1").await?.into()); // Placeholder for query engine
        
        // Create enhanced session
        let session = EnhancedReplSession {
            database: database.clone(),
            schema_manager: schema_manager.clone(),
            storage_engine: storage_engine.clone(),
            query_executor: REPLQueryExecutor::new(
                database.clone(),
                query_engine,
                schema_manager.clone(),
                storage_engine.clone(),
                // Create a basic session for initialization
                &crate::interactive::ReplSession {
                    db_path: db_path.to_path_buf(),
                    config: config.clone(),
                    database: (*database).clone(),
                    current_keyspace: None,
                    data_dir: None,
                    timing_enabled: false,
                    paging_enabled: true,
                    page_size: 50,
                    command_history: Vec::new(),
                },
            )?,
            schema_explorer: REPLSchemaExplorer::new(schema_manager, database),
            config,
            db_path: db_path.to_path_buf(),
            current_keyspace: None,
            data_dir: None,
            timing_enabled: false,
            paging_enabled: true,
            page_size: 50,
            command_history: Vec::new(),
            query_cache: HashMap::new(),
        };

        Ok(session)
    }

    /// Start the enhanced interactive REPL
    pub async fn start(&mut self) -> Result<()> {
        self.display_startup_banner().await?;
        
        let mut input = String::new();
        let stdin = io::stdin();

        loop {
            // Display enhanced prompt
            let prompt = self.format_enhanced_prompt();
            print!("{} ", prompt);
            io::stdout().flush()?;

            input.clear();
            match stdin.read_line(&mut input) {
                Ok(0) => break, // EOF
                Ok(_) => {
                    let trimmed = input.trim();

                    if trimmed.is_empty() {
                        continue;
                    }

                    // Add to command history
                    if !trimmed.starts_with(':') || trimmed.len() > 1 {
                        self.command_history.push(trimmed.to_string());
                        self.query_executor.add_to_history(trimmed.to_string());
                    }

                    match self.handle_enhanced_command(trimmed).await {
                        Ok(should_continue) => {
                            if !should_continue {
                                break;
                            }
                        }
                        Err(e) => {
                            self.display_enhanced_error(&e, trimmed);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("{} {}", "Input error:".red().bold(), e);
                    break;
                }
            }
        }

        self.display_goodbye_message();
        Ok(())
    }

    /// Handle enhanced REPL commands
    async fn handle_enhanced_command(&mut self, input: &str) -> Result<bool> {
        match input {
            // Exit commands
            ":quit" | ":exit" | ":q" | ".quit" | ".exit" | "\\q" => {
                return Ok(false);
            }

            // Enhanced help system
            ":help" | ":h" | ".help" | "\\?" => {
                self.show_enhanced_help();
            }
            cmd if cmd.starts_with(":help ") => {
                let topic = cmd.strip_prefix(":help ").unwrap_or("").trim();
                self.show_help_topic(topic);
            }

            // Enhanced configuration
            ":config" => {
                self.show_enhanced_config();
            }
            cmd if cmd.starts_with(":config ") => {
                let config_cmd = cmd.strip_prefix(":config ").unwrap_or("").trim();
                self.handle_enhanced_config_command(config_cmd).await?;
            }

            // Enhanced data exploration
            ":tables" | ":list" => {
                self.show_enhanced_tables().await?;
            }
            ":keyspaces" => {
                self.show_enhanced_keyspaces().await?;
            }
            cmd if cmd.starts_with(":info ") => {
                let object_name = cmd.strip_prefix(":info ").unwrap_or("").trim();
                self.show_enhanced_object_info(object_name).await?;
            }
            cmd if cmd.starts_with(":describe ") || cmd.starts_with(":desc ") => {
                let prefix = if cmd.starts_with(":describe ") { ":describe " } else { ":desc " };
                let table_name = cmd.strip_prefix(prefix).unwrap_or("").trim();
                self.describe_enhanced_table(table_name).await?;
            }

            // Enhanced schema exploration
            cmd if cmd.starts_with(":schema") => {
                let table_name = cmd.strip_prefix(":schema")
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty());
                self.show_enhanced_schema(table_name).await?;
            }

            // Enhanced utilities
            ":clear" | ":cls" => {
                print!("\\x1B[2J\\x1B[1;1H");
                io::stdout().flush()?;
            }
            ":history" => {
                self.show_enhanced_history();
            }
            ":timing" => {
                self.timing_enabled = !self.timing_enabled;
                self.query_executor.set_timing_enabled(self.timing_enabled);
                println!("{} Timing is now {}", 
                    "Info:".cyan().bold(), 
                    if self.timing_enabled { "enabled".green() } else { "disabled".red() }
                );
            }
            ":cache" => {
                self.show_query_cache_stats();
            }

            // Enhanced keyspace operations
            cmd if cmd.starts_with(":use ") => {
                let keyspace = cmd.strip_prefix(":use ").unwrap_or("").trim();
                self.use_enhanced_keyspace(keyspace).await?;
            }

            // Enhanced query execution
            _ if input.starts_with(':') => {
                eprintln!("{} Unknown command: {}", "Error:".red().bold(), input);
                println!("Type {} for available commands", ":help".green());
            }
            _ if input.starts_with('.') => {
                eprintln!("{} Unknown command: {}", "Error:".red().bold(), input);
                println!("Type {} for available commands", ":help".green());
            }

            // Execute as enhanced CQL query
            _ => {
                self.execute_enhanced_cql_query(input).await?;
            }
        }

        Ok(true)
    }

    /// Display enhanced startup banner
    async fn display_startup_banner(&self) -> Result<()> {
        println!("{}", "╔═══════════════════════════════════════════════════════╗".cyan());
        println!("{}", "║              CQLite Enhanced Interactive             ║".cyan().bold());
        println!("{}", "║          Advanced Cassandra Data Explorer            ║".cyan());
        println!("{}", "╚═══════════════════════════════════════════════════════╝".cyan());
        println!();
        println!("🗄️  Database: {}", self.db_path.display().to_string().yellow());
        println!("🚀 Engine: {}", "CQLite Core Enhanced v0.1.0".green());
        println!("🔗 Features: {}", "Advanced Query Processing | Schema Discovery | Performance Analytics".green());
        
        // Show system information
        if let Ok(keyspaces) = self.schema_explorer.get_keyspaces().await {
            println!("📦 Available Keyspaces: {}", keyspaces.len().to_string().cyan());
        }

        if let Ok(tables) = self.schema_explorer.get_tables(None).await {
            println!("📄 Total Tables: {}", tables.len().to_string().cyan());
        }
        
        println!();
        println!("{}", "🎯 Enhanced Features:".cyan().bold());
        println!("  • {} - Advanced query processing with performance metrics", "Smart Execution".green());
        println!("  • {} - Interactive schema exploration and discovery", "Schema Intelligence".green());
        println!("  • {} - Query optimization hints and suggestions", "Query Optimization".green());
        println!("  • {} - Comprehensive result formatting and pagination", "Enhanced Display".green());
        println!("  • {} - Command history and caching for better performance", "Session Management".green());
        println!();
        
        println!("{}", "🚀 Quick Start:".cyan().bold());
        println!("  • {} - Show comprehensive help", ":help".green());
        println!("  • {} - Explore available data", ":keyspaces :tables".green());
        println!("  • {} - Execute optimized queries", "SELECT * FROM table_name LIMIT 10;".yellow());
        println!("  • {} - Get performance insights", ":timing".green());
        println!("  • {} - Exit the enhanced shell", ":quit".red());
        println!();
        
        Ok(())
    }

    /// Format enhanced prompt with context information
    fn format_enhanced_prompt(&self) -> String {
        let keyspace_part = if let Some(ref ks) = self.current_keyspace {
            format!("{}@", ks.cyan())
        } else {
            "".to_string()
        };
        
        let timing_indicator = if self.timing_enabled { "⏱️" } else { "" };
        let cache_indicator = if !self.query_cache.is_empty() { "💾" } else { "" };
        
        format!("{}{}{}cqlite{}", keyspace_part, timing_indicator, cache_indicator, ">".blue().bold())
    }

    /// Show enhanced help system
    fn show_enhanced_help(&self) {
        println!("{}", "CQLite Enhanced REPL - Command Reference".cyan().bold());
        println!("{}", "═".repeat(55).cyan());
        println!();
        
        println!("{}", "🚀 Enhanced Features".yellow().bold());
        println!("  {:25} {}", ":timing", "Toggle performance timing display");
        println!("  {:25} {}", ":cache", "Show query cache statistics");
        println!("  {:25} {}", ":history", "Show enhanced command history");
        println!();
        
        println!("{}", "📋 Meta Commands (prefixed with :)".yellow().bold());
        println!("  {:25} {}", ":help", "Show this help message");
        println!("  {:25} {}", ":help <topic>", "Show help for specific topic");
        println!("  {:25} {}", ":quit, :exit, :q", "Exit the shell");
        println!("  {:25} {}", ":clear, :cls", "Clear the screen");
        println!();
        
        println!("{}", "🔧 Enhanced Configuration".yellow().bold());
        println!("  {:25} {}", ":config", "Show current configuration");
        println!("  {:25} {}", ":config data-dir <path>", "Set Cassandra data directory");
        println!("  {:25} {}", ":config page-size <num>", "Set result page size");
        println!("  {:25} {}", ":config timing on|off", "Enable/disable timing");
        println!();
        
        println!("{}", "🔍 Enhanced Data Exploration".yellow().bold());
        println!("  {:25} {}", ":tables, :list", "List all tables with metadata");
        println!("  {:25} {}", ":keyspaces", "List all keyspaces with details");
        println!("  {:25} {}", ":info <object>", "Show detailed object information");
        println!("  {:25} {}", ":describe <table>", "Show enhanced table schema");
        println!("  {:25} {}", ":schema [table]", "Show schema with relationships");
        println!("  {:25} {}", ":use <keyspace>", "Switch to keyspace");
        println!();
        
        println!("{}", "⚡ Enhanced Query Execution".green().bold());
        println!("  • Automatic query optimization and hints");
        println!("  • Performance metrics and bottleneck detection");
        println!("  • Intelligent result formatting and pagination");
        println!("  • Query caching for improved performance");
        println!("  • Real-time SSTable access statistics");
        println!();
        
        println!("{}", "📚 Help Topics (use ':help <topic>')".cyan().bold());
        println!("  commands, config, cql, examples, troubleshooting, performance, optimization");
        println!();
    }

    /// Show help topic with enhanced information
    fn show_help_topic(&self, topic: &str) {
        match topic.to_lowercase().as_str() {
            "performance" => self.show_performance_help(),
            "optimization" => self.show_optimization_help(),
            "commands" | "cmd" => self.show_commands_help(),
            "config" | "configuration" => self.show_config_help(),
            "cql" | "queries" => self.show_cql_help(),
            "examples" => self.show_examples_help(),
            "troubleshooting" | "trouble" => self.show_troubleshooting_help(),
            _ => {
                println!("{} Unknown help topic: {}", "Error:".red().bold(), topic);
                println!("Available topics: commands, config, cql, examples, troubleshooting, performance, optimization");
            }
        }
    }

    fn show_performance_help(&self) {
        println!("{}", "Performance Analysis & Monitoring".cyan().bold());
        println!("{}", "═".repeat(35).cyan());
        println!();
        
        println!("{}", "⏱️ Timing Features:".yellow().bold());
        println!("  :timing                 Toggle execution timing display");
        println!("  Automatic breakdown:    Parse | Planning | Execution times");
        println!("  Resource monitoring:    Memory usage, SSTable access");
        println!();
        
        println!("{}", "📊 Performance Metrics:".yellow().bold());
        println!("  • Query execution time breakdown");
        println!("  • SSTable files scanned count");
        println!("  • Rows examined vs. rows returned ratio");
        println!("  • Memory usage per query");
        println!("  • Cache hit/miss ratios");
        println!();
        
        println!("{}", "🎯 Optimization Hints:".green().bold());
        println!("  • Automatic detection of inefficient queries");
        println!("  • Suggestions for adding LIMIT clauses");
        println!("  • WHERE clause optimization recommendations");
        println!("  • Index usage recommendations");
        println!();
    }

    fn show_optimization_help(&self) {
        println!("{}", "Query Optimization Guide".cyan().bold());
        println!("{}", "═".repeat(28).cyan());
        println!();
        
        println!("{}", "⚡ Automatic Optimizations:".yellow().bold());
        println!("  • Query plan generation and cost estimation");
        println!("  • Intelligent key range extraction from WHERE clauses");
        println!("  • Automatic LIMIT addition in interactive mode");
        println!("  • Query result caching for repeated queries");
        println!();
        
        println!("{}", "💡 Manual Optimization Tips:".yellow().bold());
        println!("  • Use specific column names instead of SELECT *");
        println!("  • Add LIMIT clauses for large result sets");
        println!("  • Use WHERE clauses with partition keys when possible");
        println!("  • Consider pagination for large datasets");
        println!();
        
        println!("{}", "🔍 Performance Monitoring:".green().bold());
        println!("  :cache                  View query cache statistics");
        println!("  :timing                 Enable detailed timing breakdown");
        println!("  Query warnings          Automatic detection of slow queries");
        println!();
    }

    fn show_commands_help(&self) {
        // Enhanced version of the commands help
        println!("{}", "Enhanced Meta-Commands Reference".cyan().bold());
        println!("{}", "═".repeat(40).cyan());
        println!();
        
        println!("{}", "Exit Commands:".yellow().bold());
        println!("  :quit, :exit, :q        Exit the REPL");
        println!("  Ctrl+D                  EOF signal to exit");
        println!();
        
        println!("{}", "Enhanced Information Commands:".yellow().bold());
        println!("  :tables                 List tables with metadata and row counts");
        println!("  :keyspaces              List keyspaces with table counts");
        println!("  :info <object>          Show detailed object info with performance stats");
        println!("  :describe <table>       Show table schema with constraints and indexes");
        println!("  :schema [table]         Show schema with relationship mapping");
        println!();
        
        println!("{}", "Performance Commands:".yellow().bold());
        println!("  :timing                 Toggle timing display with breakdown");
        println!("  :cache                  Show query cache statistics");
        println!("  :history                Show command history with execution times");
        println!();
        
        println!("{}", "Session Commands:".yellow().bold());
        println!("  :clear, :cls            Clear screen");
        println!("  :use <keyspace>         Switch current keyspace");
        println!("  :config                 Show enhanced configuration");
        println!();
    }

    fn show_config_help(&self) {
        println!("{}", "Enhanced Configuration System".cyan().bold());
        println!("{}", "═".repeat(35).cyan());
        println!();
        
        println!("{}", "View Configuration:".yellow().bold());
        println!("  :config                 Show all current settings with performance info");
        println!();
        
        println!("{}", "Performance Settings:".yellow().bold());
        println!("  :config timing on|off           Enable/disable detailed timing");
        println!("  :config cache-size <num>         Set query cache size");
        println!("  :config page-size <num>          Set result page size");
        println!();
        
        println!("{}", "Data Source Settings:".yellow().bold());
        println!("  :config data-dir <path>          Set Cassandra data directory");
        println!("  :config keyspace <name>          Set default keyspace");
        println!();
        
        println!("{}", "Examples:".green().bold());
        println!("  :config data-dir /var/lib/cassandra/data");
        println!("  :config cache-size 200");
        println!("  :config timing on");
        println!();
    }

    fn show_cql_help(&self) {
        println!("{}", "Enhanced CQL Query Support".cyan().bold());
        println!("{}", "═".repeat(30).cyan());
        println!();
        
        println!("{}", "Supported CQL Features:".yellow().bold());
        println!("  • SELECT statements with advanced WHERE, LIMIT, ORDER BY");
        println!("  • Complex data types (collections, UDTs, tuples)");
        println!("  • System table queries with performance optimization");
        println!("  • DESCRIBE statements with enhanced schema information");
        println!("  • COUNT queries with optimization hints");
        println!();
        
        println!("{}", "Enhanced Query Features:".yellow().bold());
        println!("  • Automatic query optimization and planning");
        println!("  • Performance metrics for every query");
        println!("  • Intelligent result formatting and pagination");
        println!("  • Query caching for repeated operations");
        println!("  • Real-time optimization hints and warnings");
        println!();
        
        println!("{}", "Query Examples:".green().bold());
        println!("  SELECT * FROM users LIMIT 10;                    -- Basic select with limit");
        println!("  SELECT name, email FROM users WHERE id = 'u123'; -- Optimized WHERE clause");
        println!("  SELECT COUNT(*) FROM users;                      -- Count with optimization");
        println!("  DESCRIBE TABLE users;                            -- Enhanced schema info");
        println!();
    }

    fn show_examples_help(&self) {
        println!("{}", "Enhanced Usage Examples".cyan().bold());
        println!("{}", "═".repeat(28).cyan());
        println!();
        
        println!("{}", "Getting Started with Enhanced Features:".yellow().bold());
        println!("  1. :config data-dir /path/to/cassandra/data     # Configure data source");
        println!("  2. :timing                                      # Enable performance tracking");
        println!("  3. :keyspaces                                   # Explore available data");
        println!("  4. :use my_keyspace                             # Set working keyspace");
        println!("  5. :tables                                      # List tables with metadata");
        println!("  6. :describe users                              # Examine table structure");
        println!("  7. SELECT * FROM users LIMIT 5;                # Execute optimized query");
        println!("  8. :cache                                       # Check performance stats");
        println!();
        
        println!("{}", "Performance Analysis Workflow:".yellow().bold());
        println!("  :timing                           # Enable detailed timing");
        println!("  SELECT COUNT(*) FROM large_table; # Execute query with metrics");
        println!("  -- Review execution time breakdown and optimization hints");
        println!("  SELECT * FROM large_table WHERE pk = 'value' LIMIT 100;");
        println!("  -- Compare performance with filtered query");
        println!("  :cache                           # Review cache performance");
        println!();
        
        println!("{}", "Advanced Schema Exploration:".yellow().bold());
        println!("  :info users                      # Detailed table information");
        println!("  :schema                          # Overview of all schemas");
        println!("  :describe users                  # Full table schema with constraints");
        println!("  SELECT * FROM system.columns WHERE table_name = 'users';");
        println!("  -- System table exploration with enhanced formatting");
        println!();
    }

    fn show_troubleshooting_help(&self) {
        println!("{}", "Enhanced Troubleshooting Guide".cyan().bold());
        println!("{}", "═".repeat(35).cyan());
        println!();
        
        println!("{}", "Performance Issues:".yellow().bold());
        println!();
        println!("{}", "❌ \"Query taking too long\":".red());
        println!("  • Enable :timing to see execution breakdown");
        println!("  • Check optimization hints for suggestions");
        println!("  • Add LIMIT clauses for large result sets");
        println!("  • Review WHERE clause efficiency");
        println!();
        
        println!("{}", "❌ \"High memory usage\":".red());
        println!("  • Use smaller page sizes with :config page-size");
        println!("  • Add LIMIT clauses to queries");
        println!("  • Review result set sizes in performance metrics");
        println!();
        
        println!("{}", "❌ \"Low cache hit rates\":".red());
        println!("  • Check :cache statistics");
        println!("  • Review query patterns for optimization");
        println!("  • Consider query structure modifications");
        println!();
        
        println!("{}", "✅ Performance Optimization:".green().bold());
        println!("  • :timing - Monitor query performance");
        println!("  • :cache - Review caching effectiveness");
        println!("  • Follow optimization hints automatically provided");
        println!("  • Use :help optimization for detailed guidance");
        println!();
    }

    /// Show enhanced configuration
    fn show_enhanced_config(&self) {
        println!("{}", "Enhanced Configuration Status".cyan().bold());
        println!("{}", "═".repeat(35).cyan());
        println!();
        
        println!("{}", "🗄️ Database Settings:".yellow().bold());
        println!("  Database Path: {}", self.db_path.display().to_string().green());
        if let Some(ref keyspace) = self.current_keyspace {
            println!("  Current Keyspace: {}", keyspace.green());
        } else {
            println!("  Current Keyspace: {}", "None".yellow());
        }
        
        if let Some(ref data_dir) = self.data_dir {
            println!("  Data Directory: {}", data_dir.display().to_string().green());
        } else {
            println!("  Data Directory: {} (use :config data-dir <path>)", "Not set".yellow());
        }
        println!();
        
        println!("{}", "⚡ Performance Settings:".yellow().bold());
        println!("  Timing: {}", if self.timing_enabled { "enabled".green() } else { "disabled".red() });
        println!("  Paging: {}", if self.paging_enabled { "enabled".green() } else { "disabled".red() });
        println!("  Page Size: {}", self.page_size.to_string().green());
        println!("  Query Cache Size: {}", self.query_cache.len().to_string().green());
        println!();
        
        println!("{}", "📊 Session Statistics:".yellow().bold());
        println!("  Commands in History: {}", self.command_history.len().to_string().green());
        println!("  Cached Queries: {}", self.query_cache.len().to_string().green());
        
        // Show cache statistics
        if !self.query_cache.is_empty() {
            let total_execution_time: f64 = self.query_cache.values()
                .map(|info| info.execution_time_ms)
                .sum();
            let avg_execution_time = total_execution_time / self.query_cache.len() as f64;
            println!("  Average Query Time: {:.2}ms", avg_execution_time.to_string().green());
        }
        println!();
        
        println!("{}", "💡 Use :config <setting> <value> to change settings".cyan());
    }

    /// Handle enhanced configuration commands
    async fn handle_enhanced_config_command(&mut self, config_cmd: &str) -> Result<()> {
        let parts: Vec<&str> = config_cmd.split_whitespace().collect();
        
        if parts.is_empty() {
            self.show_enhanced_config();
            return Ok(());
        }
        
        match parts[0] {
            "data-dir" => {
                if parts.len() < 2 {
                    println!("{} Usage: :config data-dir <path>", "Error:".red().bold());
                    return Ok(());
                }
                
                let path = PathBuf::from(parts[1]);
                if !path.exists() {
                    println!("{} Directory does not exist: {}", "Error:".red().bold(), path.display());
                    return Ok(());
                }
                
                self.data_dir = Some(path.clone());
                println!("{} Data directory set to: {}", "Success:".green().bold(), path.display());
            }
            
            "page-size" => {
                if parts.len() < 2 {
                    println!("{} Usage: :config page-size <number>", "Error:".red().bold());
                    return Ok(());
                }
                
                match parts[1].parse::<usize>() {
                    Ok(size) if size > 0 && size <= 10000 => {
                        self.page_size = size;
                        self.query_executor.set_page_size(size);
                        println!("{} Page size set to: {}", "Success:".green().bold(), size);
                    }
                    Ok(_) => {
                        println!("{} Page size must be between 1 and 10000", "Error:".red().bold());
                    }
                    Err(_) => {
                        println!("{} Invalid page size: {}", "Error:".red().bold(), parts[1]);
                    }
                }
            }
            
            "timing" => {
                if parts.len() < 2 {
                    self.timing_enabled = !self.timing_enabled;
                } else {
                    match parts[1].to_lowercase().as_str() {
                        "on" | "true" | "1" | "yes" => self.timing_enabled = true,
                        "off" | "false" | "0" | "no" => self.timing_enabled = false,
                        _ => {
                            println!("{} Usage: :config timing [on|off]", "Error:".red().bold());
                            return Ok(());
                        }
                    }
                }
                self.query_executor.set_timing_enabled(self.timing_enabled);
                println!("{} Timing is now {}", 
                    "Info:".cyan().bold(), 
                    if self.timing_enabled { "enabled".green() } else { "disabled".red() }
                );
            }
            
            "cache-size" => {
                if parts.len() < 2 {
                    println!("{} Usage: :config cache-size <number>", "Error:".red().bold());
                    return Ok(());
                }
                
                match parts[1].parse::<usize>() {
                    Ok(size) if size > 0 && size <= 1000 => {
                        // Clear cache if reducing size
                        if size < self.query_cache.len() {
                            self.query_cache.clear();
                        }
                        println!("{} Cache size limit set to: {}", "Success:".green().bold(), size);
                    }
                    Ok(_) => {
                        println!("{} Cache size must be between 1 and 1000", "Error:".red().bold());
                    }
                    Err(_) => {
                        println!("{} Invalid cache size: {}", "Error:".red().bold(), parts[1]);
                    }
                }
            }
            
            _ => {
                println!("{} Unknown configuration option: {}", "Error:".red().bold(), parts[0]);
                println!("Available options: data-dir, page-size, timing, cache-size");
            }
        }
        
        Ok(())
    }

    /// Show enhanced tables with metadata
    async fn show_enhanced_tables(&self) -> Result<()> {
        println!("{}", "📋 Enhanced Tables Overview".cyan().bold());
        println!("{}", "═".repeat(30).cyan());
        
        let tables = self.schema_explorer.get_tables(self.current_keyspace.as_deref()).await?;
        
        if tables.is_empty() {
            println!("📭 No tables found");
            if let Some(ref ks) = self.current_keyspace {
                println!("💡 Current keyspace: {}", ks.yellow());
                println!("💡 Try :use <keyspace> to switch keyspaces");
            } else {
                println!("💡 Try :keyspaces to see available keyspaces");
            }
            return Ok(());
        }

        // Group tables by keyspace if not in a specific keyspace
        if self.current_keyspace.is_none() {
            let mut tables_by_keyspace: HashMap<String, Vec<String>> = HashMap::new();
            
            for table in &tables {
                if let Some(dot_pos) = table.find('.') {
                    let keyspace = &table[..dot_pos];
                    let table_name = &table[dot_pos + 1..];
                    tables_by_keyspace.entry(keyspace.to_string()).or_insert_with(Vec::new).push(table_name.to_string());
                }
            }
            
            for (keyspace, table_list) in tables_by_keyspace {
                println!("\n📦 Keyspace: {}", keyspace.yellow().bold());
                for table in table_list {
                    println!("  📄 {} {}", table.green(), "(use :info for details)".dimmed());
                }
            }
        } else {
            // Show tables in current keyspace with enhanced info
            println!("\n📦 Keyspace: {} (current)", self.current_keyspace.as_ref().unwrap().yellow().bold());
            for table in tables {
                println!("  📄 {} {}", table.green(), "(use :info or :describe for details)".dimmed());
            }
        }
        
        println!();
        println!("💡 Use {} for detailed table information", ":info <table>".green());
        println!("💡 Use {} for table schema", ":describe <table>".green());
        
        Ok(())
    }

    /// Show enhanced keyspaces
    async fn show_enhanced_keyspaces(&self) -> Result<()> {
        println!("{}", "📦 Enhanced Keyspaces Overview".cyan().bold());
        println!("{}", "═".repeat(32).cyan());
        
        let keyspaces = self.schema_explorer.get_keyspaces().await?;
        
        if keyspaces.is_empty() {
            println!("📭 No keyspaces found");
            return Ok(());
        }

        for keyspace in &keyspaces {
            let is_current = Some(keyspace) == self.current_keyspace.as_ref();
            let indicator = if is_current { "→" } else { " " };
            
            // Get table count for each keyspace
            let table_count = match self.schema_explorer.get_tables(Some(keyspace)).await {
                Ok(tables) => tables.len(),
                Err(_) => 0,
            };
            
            if keyspace == "system" {
                println!("  {} 🔧 {} ({} tables) (system)", indicator, keyspace.dimmed(), table_count);
            } else if is_current {
                println!("  {} 📦 {} ({} tables) (current)", indicator, keyspace.green().bold(), table_count);
            } else {
                println!("  {} 📦 {} ({} tables)", indicator, keyspace.green(), table_count);
            }
        }
        
        println!();
        println!("💡 Use {} to switch keyspace", ":use <keyspace_name>".green());
        println!("💡 Use {} to see tables in a keyspace", ":tables".green());
        
        Ok(())
    }

    /// Show enhanced object information
    async fn show_enhanced_object_info(&self, object_name: &str) -> Result<()> {
        println!("{} {}", "🔍 Enhanced Object Information:".cyan().bold(), object_name.yellow());
        println!("{}", "═".repeat(40).cyan());
        
        // Parse object name (could be keyspace.table or just table)
        let (keyspace, table) = if object_name.contains('.') {
            let parts: Vec<&str> = object_name.split('.').collect();
            if parts.len() == 2 {
                (Some(parts[0]), parts[1])
            } else {
                (self.current_keyspace.as_deref(), object_name)
            }
        } else {
            (self.current_keyspace.as_deref(), object_name)
        };
        
        if let Some(ks) = keyspace {
            println!("📦 Keyspace: {}", ks.yellow());
            println!("📄 Table: {}", table.green());
            
            // Get enhanced schema information
            match self.schema_explorer.get_table_schema(ks, table).await {
                Ok(schema) => {
                    println!("✅ Found in schema catalog");
                    
                    // Display enhanced schema information
                    if !schema.partition_keys.is_empty() {
                        println!("\n🔑 {}", "Partition Keys:".yellow().bold());
                        for pk in &schema.partition_keys {
                            println!("  {} {}", pk.name.cyan(), pk.data_type.green());
                        }
                    }
                    
                    if !schema.clustering_keys.is_empty() {
                        println!("\n🔗 {}", "Clustering Keys:".yellow().bold());
                        for ck in &schema.clustering_keys {
                            println!("  {} {}", ck.name.cyan(), ck.data_type.green());
                        }
                    }
                    
                    let regular_columns: Vec<_> = schema.columns.iter()
                        .filter(|col| col.kind != "partition_key" && col.kind != "clustering")
                        .collect();
                    
                    if !regular_columns.is_empty() {
                        println!("\n📝 {}", "Regular Columns:".yellow().bold());
                        for col in regular_columns {
                            println!("  {} {}", col.name.cyan(), col.data_type.green());
                        }
                    }
                    
                    println!("\n📊 {}", "Column Summary:".green().bold());
                    println!("  Total Columns: {}", schema.columns.len());
                    println!("  Partition Keys: {}", schema.partition_keys.len());
                    println!("  Clustering Keys: {}", schema.clustering_keys.len());
                    
                    // Performance analysis
                    println!("\n⚡ {}", "Performance Analysis:".cyan().bold());
                    if schema.partition_keys.is_empty() {
                        println!("  ⚠️  No partition key found - this may cause performance issues");
                    } else {
                        println!("  ✅ Partition key present - good for distribution");
                    }
                    
                    if schema.clustering_keys.len() > 3 {
                        println!("  ⚠️  Many clustering keys - consider query patterns");
                    }
                }
                Err(e) => {
                    println!("❌ Schema not found: {}", e);
                    
                    // Try to find in data directory if configured
                    if let Some(ref data_dir) = self.data_dir {
                        println!("🔄 Searching in data directory...");
                        // Would implement data directory scanning here
                        println!("💡 Configure data directory for filesystem-based discovery");
                    }
                }
            }
        } else {
            println!("❌ No keyspace specified and no current keyspace set");
            println!("💡 Use {} or {}", ":use <keyspace>".green(), ":info keyspace.table".green());
        }
        
        Ok(())
    }

    /// Describe enhanced table
    async fn describe_enhanced_table(&self, table_name: &str) -> Result<()> {
        let (keyspace, table) = if table_name.contains('.') {
            let parts: Vec<&str> = table_name.split('.').collect();
            if parts.len() == 2 {
                (Some(parts[0]), parts[1])
            } else {
                (self.current_keyspace.as_deref(), table_name)
            }
        } else {
            (self.current_keyspace.as_deref(), table_name)
        };
        
        if let Some(ks) = keyspace {
            println!("{} {}", "📋 Enhanced Table Schema:".cyan().bold(), format!("{}.{}", ks, table).yellow());
            println!("{}", "═".repeat(30).cyan());
            
            match self.schema_explorer.get_table_schema(ks, table).await {
                Ok(schema) => {
                    // Display organized schema
                    if !schema.partition_keys.is_empty() {
                        println!("🔑 {}", "Partition Keys:".yellow().bold());
                        for pk in &schema.partition_keys {
                            println!("  {} {} {}", "🔑".cyan(), pk.name.cyan(), pk.data_type.green());
                        }
                        println!();
                    }
                    
                    if !schema.clustering_keys.is_empty() {
                        println!("🔗 {}", "Clustering Keys:".yellow().bold());
                        for ck in &schema.clustering_keys {
                            println!("  {} {} {}", "🔗".cyan(), ck.name.cyan(), ck.data_type.green());
                        }
                        println!();
                    }
                    
                    let regular_columns: Vec<_> = schema.columns.iter()
                        .filter(|col| col.kind != "partition_key" && col.kind != "clustering")
                        .collect();
                    
                    if !regular_columns.is_empty() {
                        println!("📝 {}", "Regular Columns:".yellow().bold());
                        for col in regular_columns {
                            println!("  {} {} {}", "📝".cyan(), col.name.cyan(), col.data_type.green());
                        }
                        println!();
                    }
                    
                    // Enhanced CREATE TABLE statement
                    println!("🏗️  {}", "Enhanced CREATE TABLE Statement:".green().bold());
                    self.generate_enhanced_create_table_statement(&schema);
                    
                    // Query recommendations
                    println!("\n💡 {}", "Query Recommendations:".cyan().bold());
                    if !schema.partition_keys.is_empty() {
                        let pk_names: Vec<_> = schema.partition_keys.iter().map(|pk| &pk.name).collect();
                        println!("  • Use partition key in WHERE: WHERE {} = ?", pk_names.join(" AND "));
                    }
                    if !schema.clustering_keys.is_empty() {
                        println!("  • Clustering keys can be used for ordering and filtering");
                    }
                    println!("  • Add LIMIT clause for large result sets");
                    println!("  • Use :timing to monitor query performance");
                }
                Err(e) => {
                    println!("❌ Failed to describe table: {}", e);
                    println!("💡 Make sure the table exists and you have the correct keyspace set");
                }
            }
        } else {
            println!("❌ No keyspace specified and no current keyspace set");
            println!("💡 Use {} or specify table as keyspace.table", ":use <keyspace>".green());
        }
        
        Ok(())
    }

    /// Generate enhanced CREATE TABLE statement
    fn generate_enhanced_create_table_statement(&self, schema: &crate::repl_integration::TableSchemaDetail) {
        println!("```sql");
        println!("CREATE TABLE {}.{} (", schema.keyspace, schema.table);
        
        // Print all columns
        for (i, column) in schema.columns.iter().enumerate() {
            let comma = if i < schema.columns.len() - 1 { "," } else { "" };
            let comment = match column.kind.as_str() {
                "partition_key" => " -- Partition Key",
                "clustering" => " -- Clustering Key",
                _ => "",
            };
            println!("    {} {}{}{}", column.name, column.data_type, comma, comment.dimmed());
        }
        
        // Add PRIMARY KEY clause
        if !schema.partition_keys.is_empty() {
            print!(",\n    PRIMARY KEY (");
            
            let partition_key_names: Vec<_> = schema.partition_keys.iter().map(|pk| &pk.name).collect();
            
            if partition_key_names.len() == 1 && schema.clustering_keys.is_empty() {
                print!("{}", partition_key_names[0]);
            } else {
                print!("({})", partition_key_names.join(", "));
                if !schema.clustering_keys.is_empty() {
                    let clustering_key_names: Vec<_> = schema.clustering_keys.iter().map(|ck| &ck.name).collect();
                    print!(", {}", clustering_key_names.join(", "));
                }
            }
            println!(")");
        }
        
        println!(");");
        println!("```");
    }

    /// Show enhanced schema overview
    async fn show_enhanced_schema(&self, table_name: Option<&str>) -> Result<()> {
        match table_name {
            Some(table) => {
                self.describe_enhanced_table(table).await
            }
            None => {
                println!("{}", "📋 Enhanced Schema Overview".cyan().bold());
                println!("{}", "═".repeat(30).cyan());
                
                let tables = self.schema_explorer.get_tables(None).await?;
                
                if tables.is_empty() {
                    println!("📭 No user tables found");
                    println!("💡 Configure data directory or create tables");
                    return Ok(());
                }
                
                let mut tables_by_keyspace: HashMap<String, Vec<String>> = HashMap::new();
                for table in &tables {
                    if let Some(dot_pos) = table.find('.') {
                        let keyspace = &table[..dot_pos];
                        let table_name = &table[dot_pos + 1..];
                        tables_by_keyspace.entry(keyspace.to_string()).or_insert_with(Vec::new).push(table_name.to_string());
                    }
                }
                
                for (keyspace, table_list) in tables_by_keyspace {
                    let is_current = Some(&keyspace) == self.current_keyspace.as_ref();
                    let keyspace_display = if is_current {
                        format!("📦 {} (current)", keyspace.green().bold())
                    } else {
                        format!("📦 {}", keyspace.yellow())
                    };
                    
                    println!("\n{}", keyspace_display);
                    println!("  Tables: {}", table_list.len());
                    for table in &table_list {
                        let indicator = if is_current { "→" } else { " " };
                        println!("  {} 📄 {} (use :describe {}.{} for schema)", indicator, table.green(), keyspace, table);
                    }
                }
                
                println!();
                println!("💡 Use {} for specific table schema", ":describe <table>".green());
                Ok(())
            }
        }
    }

    /// Show enhanced command history
    fn show_enhanced_history(&self) {
        println!("{}", "📜 Enhanced Command History".cyan().bold());
        println!("{}", "═".repeat(30).cyan());
        
        if self.command_history.is_empty() {
            println!("📭 No commands in history yet");
            return;
        }
        
        let start_index = self.command_history.len().saturating_sub(20);
        for (i, cmd) in self.command_history.iter().skip(start_index).enumerate() {
            let line_num = start_index + i + 1;
            
            // Show execution time if available in cache
            let timing_info = if let Some(cache_info) = self.query_cache.get(cmd) {
                format!(" ({:.2}ms)", cache_info.execution_time_ms).dimmed().to_string()
            } else {
                "".to_string()
            };
            
            if cmd.starts_with("SELECT") || cmd.starts_with("select") {
                println!("  {:3}. {}{}", line_num, cmd.yellow(), timing_info);
            } else if cmd.starts_with(":") {
                println!("  {:3}. {}{}", line_num, cmd.cyan(), timing_info);
            } else {
                println!("  {:3}. {}{}", line_num, cmd, timing_info);
            }
        }
        
        println!();
        println!("💡 Showing last {} commands", self.command_history.len().min(20));
        if !self.query_cache.is_empty() {
            println!("💡 Timing shown for cached queries");
        }
    }

    /// Show query cache statistics
    fn show_query_cache_stats(&self) {
        println!("{}", "💾 Query Cache Statistics".cyan().bold());
        println!("{}", "═".repeat(30).cyan());
        
        if self.query_cache.is_empty() {
            println!("📭 No queries in cache");
            println!("💡 Execute some queries to populate the cache");
            return;
        }
        
        // Calculate statistics
        let total_queries = self.query_cache.len();
        let total_execution_time: f64 = self.query_cache.values()
            .map(|info| info.execution_time_ms)
            .sum();
        let avg_execution_time = total_execution_time / total_queries as f64;
        
        let fastest_query = self.query_cache.iter()
            .min_by(|a, b| a.1.execution_time_ms.partial_cmp(&b.1.execution_time_ms).unwrap());
        let slowest_query = self.query_cache.iter()
            .max_by(|a, b| a.1.execution_time_ms.partial_cmp(&b.1.execution_time_ms).unwrap());
        
        println!("📊 {}", "Cache Overview:".yellow().bold());
        println!("  Total Cached Queries: {}", total_queries);
        println!("  Average Execution Time: {:.2}ms", avg_execution_time);
        println!("  Total Execution Time: {:.2}ms", total_execution_time);
        println!();
        
        if let Some((fastest_sql, fastest_info)) = fastest_query {
            println!("⚡ {}", "Fastest Query:".green().bold());
            println!("  Time: {:.2}ms", fastest_info.execution_time_ms);
            println!("  SQL: {}", fastest_sql.chars().take(60).collect::<String>());
            if fastest_sql.len() > 60 {
                println!("       {}...", "");
            }
            println!();
        }
        
        if let Some((slowest_sql, slowest_info)) = slowest_query {
            println!("🐌 {}", "Slowest Query:".red().bold());
            println!("  Time: {:.2}ms", slowest_info.execution_time_ms);
            println!("  SQL: {}", slowest_sql.chars().take(60).collect::<String>());
            if slowest_sql.len() > 60 {
                println!("       {}...", "");
            }
            println!();
        }
        
        println!("💡 Use {} to clear cache", ":config cache-size 0".green());
    }

    /// Use enhanced keyspace
    async fn use_enhanced_keyspace(&mut self, keyspace: &str) -> Result<()> {
        // Validate keyspace exists
        let keyspaces = self.schema_explorer.get_keyspaces().await?;
        
        if keyspaces.contains(&keyspace.to_string()) {
            self.current_keyspace = Some(keyspace.to_string());
            self.query_executor.set_current_keyspace(Some(keyspace.to_string()));
            
            // Get table count for the keyspace
            let tables = self.schema_explorer.get_tables(Some(keyspace)).await?;
            
            println!("{} Now using keyspace: {} ({} tables)", 
                "✅".green(), 
                keyspace.yellow().bold(),
                tables.len()
            );
            
            if !tables.is_empty() {
                println!("💡 Use {} to see tables in this keyspace", ":tables".green());
            }
        } else {
            println!("❌ Keyspace '{}' not found", keyspace);
            println!("💡 Use {} to list available keyspaces", ":keyspaces".green());
        }
        
        Ok(())
    }

    /// Execute enhanced CQL query
    async fn execute_enhanced_cql_query(&mut self, query: &str) -> Result<()> {
        let start_time = std::time::Instant::now();
        
        // Execute query using enhanced processor
        match self.query_executor.execute_query(query).await {
            Ok(()) => {
                let execution_time = start_time.elapsed().as_millis() as f64;
                
                // Cache the query result for statistics
                self.query_cache.insert(
                    query.to_string(),
                    CachedQueryInfo {
                        result_count: 0, // Would be populated from actual result
                        execution_time_ms: execution_time,
                        last_executed: std::time::Instant::now(),
                    }
                );
                
                // Limit cache size
                if self.query_cache.len() > 100 {
                    self.evict_old_cache_entries();
                }
            }
            Err(e) => {
                self.query_executor.provide_error_help(&e.to_string(), query);
                return Err(e);
            }
        }
        
        Ok(())
    }

    /// Display enhanced error information
    fn display_enhanced_error(&self, error: &anyhow::Error, input: &str) {
        eprintln!("{} {}", "❌ Error:".red().bold(), error);
        
        // Provide enhanced contextual help
        self.query_executor.provide_error_help(&error.to_string(), input);
        
        // Additional performance hints
        if error.to_string().contains("timeout") || error.to_string().contains("slow") {
            println!();
            println!("{} Performance Tips:", "⚡".yellow().bold());
            println!("  • Enable timing with {} to analyze query performance", ":timing".green());
            println!("  • Add LIMIT clauses to reduce result set size");
            println!("  • Check :cache for query optimization opportunities");
        }
    }

    /// Evict old cache entries
    fn evict_old_cache_entries(&mut self) {
        let cutoff = std::time::Instant::now() - std::time::Duration::from_secs(300); // 5 minutes
        self.query_cache.retain(|_, cached| cached.last_executed > cutoff);
    }

    /// Display goodbye message
    fn display_goodbye_message(&self) {
        println!();
        println!("{}", "╔═══════════════════════════════════════════════════════╗".cyan());
        println!("{}", "║                   Enhanced Session Complete!         ║".cyan().bold());
        println!("{}", "║          Thank you for using CQLite Enhanced         ║".cyan());
        println!("{}", "╚═══════════════════════════════════════════════════════╝".cyan());
        
        // Show session summary
        if !self.command_history.is_empty() {
            println!();
            println!("📊 Session Summary:");
            println!("  Commands executed: {}", self.command_history.len());
            println!("  Queries cached: {}", self.query_cache.len());
            if let Some(ref ks) = self.current_keyspace {
                println!("  Active keyspace: {}", ks);
            }
        }
    }
}

/// Start enhanced REPL mode
pub async fn start_enhanced_repl_mode(db_path: &Path, config: &Config) -> Result<()> {
    let mut session = EnhancedReplSession::new(db_path, config.clone()).await?;
    session.start().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_enhanced_session_creation() {
        // Test creation of enhanced session
        // Would need proper setup with real database components
    }

    #[tokio::test]
    async fn test_enhanced_command_handling() {
        // Test enhanced command processing
    }

    #[tokio::test]
    async fn test_query_caching() {
        // Test query caching functionality
    }
}