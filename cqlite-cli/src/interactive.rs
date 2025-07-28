use crate::config::Config;
use anyhow::Result;
use colored::*;
use cqlite_core::{
    schema::{SchemaManager, TableSchema},
    storage::StorageEngine,
    platform::Platform,
    Database, Config as CoreConfig, QueryResult,
    query::result::QueryRow,
};
use std::collections::HashMap;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use prettytable::{Cell, Row, Table};
use std::time::Instant;

/// REPL session state for maintaining context
pub struct ReplSession {
    pub db_path: PathBuf,
    pub config: Config,
    pub database: Arc<Database>,
    pub current_keyspace: Option<String>,
    pub data_dir: Option<PathBuf>,
    pub timing_enabled: bool,
    pub paging_enabled: bool,
    pub page_size: usize,
    pub command_history: Vec<String>,
}

impl ReplSession {
    pub fn new(db_path: &Path, config: Config, database: Database) -> Self {
        Self {
            db_path: db_path.to_path_buf(),
            config,
            database: Arc::new(database),
            current_keyspace: None,
            data_dir: None,
            timing_enabled: false,
            paging_enabled: true,
            page_size: 50,
            command_history: Vec::new(),
        }
    }
}

pub async fn start_repl_mode(db_path: &Path, config: &Config, database: Database) -> Result<()> {
    let mut session = ReplSession::new(db_path, config.clone(), database);
    
    // Display enhanced startup banner
    display_startup_banner(&session).await?;
    
    let mut input = String::new();
    let stdin = io::stdin();

    loop {
        // Display contextual prompt
        let prompt = format_prompt(&session);
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
                    session.command_history.push(trimmed.to_string());
                }

                match handle_repl_command(trimmed, &mut session).await {
                    Ok(should_continue) => {
                        if !should_continue {
                            break;
                        }
                    }
                    Err(e) => {
                        display_error(&e, trimmed);
                    }
                }
            }
            Err(e) => {
                eprintln!("{} {}", "Input error:".red().bold(), e);
                break;
            }
        }
    }

    display_goodbye_message();
    Ok(())
}

async fn handle_repl_command(input: &str, session: &mut ReplSession) -> Result<bool> {
    match input {
        // Exit commands
        ":quit" | ":exit" | ":q" | ".quit" | ".exit" | "\\q" => {
            return Ok(false);
        }
        
        // Help system
        ":help" | ":h" | ".help" | "\\?" => {
            show_enhanced_help();
        }
        cmd if cmd.starts_with(":help ") => {
            let topic = cmd.strip_prefix(":help ").unwrap_or("").trim();
            show_help_topic(topic);
        }
        
        // Configuration commands
        ":config" => {
            show_current_config(session);
        }
        cmd if cmd.starts_with(":config ") => {
            let config_cmd = cmd.strip_prefix(":config ").unwrap_or("").trim();
            handle_config_command(config_cmd, session).await?;
        }
        
        // Data exploration commands
        ":tables" | ":list" => {
            show_enhanced_tables(session).await?;
        }
        ":keyspaces" => {
            show_keyspaces(session).await?;
        }
        cmd if cmd.starts_with(":info ") => {
            let object_name = cmd.strip_prefix(":info ").unwrap_or("").trim();
            show_object_info(object_name, session).await?;
        }
        cmd if cmd.starts_with(":describe ") || cmd.starts_with(":desc ") => {
            let prefix = if cmd.starts_with(":describe ") { ":describe " } else { ":desc " };
            let table_name = cmd.strip_prefix(prefix).unwrap_or("").trim();
            describe_table(table_name, session).await?;
        }
        
        // Schema commands
        cmd if cmd.starts_with(":schema") => {
            let table_name = cmd.strip_prefix(":schema")
                .map(|s| s.trim())
                .filter(|s| !s.is_empty());
            show_enhanced_schema(table_name, session).await?;
        }
        
        // Utility commands
        ":clear" | ":cls" => {
            print!("\\x1B[2J\\x1B[1;1H");
            io::stdout().flush()?;
        }
        ":history" => {
            show_command_history(session);
        }
        ":timing" => {
            session.timing_enabled = !session.timing_enabled;
            println!("{} Timing is now {}", 
                "Info:".cyan().bold(), 
                if session.timing_enabled { "enabled".green() } else { "disabled".red() }
            );
        }
        
        // Data source commands
        cmd if cmd.starts_with(":use ") => {
            let keyspace = cmd.strip_prefix(":use ").unwrap_or("").trim();
            use_keyspace(keyspace, session).await?;
        }
        cmd if cmd.starts_with(":source ") => {
            let path = cmd.strip_prefix(":source ").unwrap_or("").trim();
            source_file(path, session).await?;
        }
        
        // Unknown meta-command
        _ if input.starts_with(':') => {
            eprintln!("{} Unknown command: {}", "Error:".red().bold(), input);
            println!("Type {} for available commands", ":help".green());
        }
        _ if input.starts_with('.') => {
            eprintln!("{} Unknown command: {}", "Error:".red().bold(), input);
            println!("Type {} for available commands", ":help".green());
        }
        
        // Execute as CQL query
        _ => {
            execute_enhanced_cql_query(input, session).await?;
        }
    }

    Ok(true)
}

// ========== DISPLAY AND UI FUNCTIONS ==========

async fn display_startup_banner(session: &ReplSession) -> Result<()> {
    println!("{}", "╔═══════════════════════════════════════════════╗".cyan());
    println!("{}", "║           CQLite Interactive Shell           ║".cyan().bold());
    println!("{}", "║      High-Performance Cassandra Reader       ║".cyan());
    println!("{}", "╚═══════════════════════════════════════════════╝".cyan());
    println!();
    println!("🗄️  Database: {}", session.db_path.display().to_string().yellow());
    println!("📊 Engine: {}", "CQLite Core v0.1.0".green());
    println!("🔗 Cassandra Compatibility: {}", "3.11 | 4.0 | 5.0".green());
    
    // Show available data directories if any
    if let Some(data_dir) = &session.data_dir {
        println!("📂 Data Directory: {}", data_dir.display().to_string().yellow());
    }
    
    println!();
    println!("{}", "Quick Start:".cyan().bold());
    println!("  • {} - Show comprehensive help", ":help".green());
    println!("  • {} - Configure data directories", ":config data-dir /path/to/cassandra/data".green());
    println!("  • {} - List available tables", ":tables".green());
    println!("  • {} - Execute CQL queries", "SELECT * FROM table_name;".yellow());
    println!("  • {} - Exit the shell", ":quit".red());
    println!();
    
    Ok(())
}

fn format_prompt(session: &ReplSession) -> String {
    let keyspace_part = if let Some(ref ks) = session.current_keyspace {
        format!("{}@", ks.cyan())
    } else {
        "".to_string()
    };
    
    format!("{}cqlite{}", keyspace_part, ">".blue().bold())
}

fn display_error(error: &anyhow::Error, input: &str) {
    eprintln!("{} {}", "Error:".red().bold(), error);
    
    // Provide contextual help based on the input
    if input.starts_with("SELECT") || input.starts_with("select") {
        println!("{} CQL Query Help:", "Hint:".cyan().bold());
        println!("  • Use {} to list available tables", ":tables".green());
        println!("  • Use {} to see table structure", ":describe table_name".green());
        println!("  • Example: {}", "SELECT * FROM users LIMIT 10;".yellow());
    } else if input.starts_with(":") {
        println!("{} Available commands:", "Hint:".cyan().bold());
        println!("  • {} - Show all commands", ":help".green());
        println!("  • {} - List meta-commands", ":help commands".green());
    }
}

fn display_goodbye_message() {
    println!();
    println!("{}", "╔═══════════════════════════════════════════════╗".cyan());
    println!("{}", "║                   Goodbye!                   ║".cyan().bold());
    println!("{}", "║         Thank you for using CQLite           ║".cyan());
    println!("{}", "╚═══════════════════════════════════════════════╝".cyan());
}

// ========== HELP SYSTEM ==========

fn show_enhanced_help() {
    println!("{}", "CQLite Interactive REPL - Command Reference".cyan().bold());
    println!("{}", "═".repeat(50).cyan());
    println!();
    
    println!("{}", "📋 Meta Commands (prefixed with :)".yellow().bold());
    println!("  {:20} {}", ":help", "Show this help message");
    println!("  {:20} {}", ":help <topic>", "Show help for specific topic");
    println!("  {:20} {}", ":quit, :exit, :q", "Exit the shell");
    println!("  {:20} {}", ":clear, :cls", "Clear the screen");
    println!("  {:20} {}", ":history", "Show command history");
    println!("  {:20} {}", ":timing", "Toggle query timing display");
    println!();
    
    println!("{}", "🔧 Configuration Commands".yellow().bold());
    println!("  {:20} {}", ":config", "Show current configuration");
    println!("  {:20} {}", ":config data-dir <path>", "Set Cassandra data directory");
    println!("  {:20} {}", ":config page-size <num>", "Set result page size");
    println!("  {:20} {}", ":config timing on|off", "Enable/disable timing");
    println!();
    
    println!("{}", "🔍 Data Exploration Commands".yellow().bold());
    println!("  {:20} {}", ":tables, :list", "List all tables");
    println!("  {:20} {}", ":keyspaces", "List all keyspaces");
    println!("  {:20} {}", ":info <object>", "Show object information");
    println!("  {:20} {}", ":describe <table>", "Show table schema");
    println!("  {:20} {}", ":schema [table]", "Show schema information");
    println!("  {:20} {}", ":use <keyspace>", "Switch to keyspace");
    println!();
    
    println!("{}", "💾 Data Source Commands".yellow().bold());
    println!("  {:20} {}", ":source <file>", "Execute commands from file");
    println!();
    
    println!("{}", "🔎 CQL Query Examples".green().bold());
    println!("  {}", "SELECT * FROM users LIMIT 10;".yellow());
    println!("  {}", "SELECT name, email FROM users WHERE id = 'user123';".yellow());
    println!("  {}", "DESCRIBE TABLE users;".yellow());
    println!("  {}", "SELECT COUNT(*) FROM users;".yellow());
    println!();
    
    println!("{}", "📚 Help Topics (use ':help <topic>')".cyan().bold());
    println!("  commands, config, cql, examples, troubleshooting");
    println!();
    
    println!("{}", "💡 Tips:".green().bold());
    println!("  • Queries can span multiple lines (end with semicolon)");
    println!("  • Use Tab for auto-completion (coming soon)");
    println!("  • Press Ctrl+C to cancel current input");
    println!("  • Press Ctrl+D or :quit to exit");
}

fn show_help_topic(topic: &str) {
    match topic.to_lowercase().as_str() {
        "commands" | "cmd" => show_commands_help(),
        "config" | "configuration" => show_config_help(),
        "cql" | "queries" => show_cql_help(),
        "examples" => show_examples_help(),
        "troubleshooting" | "trouble" => show_troubleshooting_help(),
        _ => {
            println!("{} Unknown help topic: {}", "Error:".red().bold(), topic);
            println!("Available topics: commands, config, cql, examples, troubleshooting");
        }
    }
}

fn show_commands_help() {
    println!("{}", "Meta-Commands Reference".cyan().bold());
    println!("{}", "═".repeat(30).cyan());
    println!();
    
    println!("{}", "Exit Commands:".yellow().bold());
    println!("  :quit, :exit, :q    Exit the REPL");
    println!("  Ctrl+D              EOF signal to exit");
    println!();
    
    println!("{}", "Information Commands:".yellow().bold());
    println!("  :tables             List all available tables");
    println!("  :keyspaces          List all keyspaces");
    println!("  :info <object>      Show detailed object info");
    println!("  :describe <table>   Show table schema and structure");
    println!("  :schema [table]     Show schema (all tables or specific)");
    println!();
    
    println!("{}", "Session Commands:".yellow().bold());
    println!("  :clear, :cls        Clear screen");
    println!("  :history            Show command history");
    println!("  :timing             Toggle timing display");
    println!("  :use <keyspace>     Switch current keyspace");
}

fn show_config_help() {
    println!("{}", "Configuration System".cyan().bold());
    println!("{}", "═".repeat(25).cyan());
    println!();
    
    println!("{}", "View Configuration:".yellow().bold());
    println!("  :config             Show all current settings");
    println!();
    
    println!("{}", "Data Source Settings:".yellow().bold());
    println!("  :config data-dir <path>     Set Cassandra data directory");
    println!("  :config keyspace <name>     Set default keyspace");
    println!();
    
    println!("{}", "Display Settings:".yellow().bold());
    println!("  :config page-size <num>     Set result page size (default: 50)");
    println!("  :config timing on|off       Enable/disable query timing");
    println!("  :config paging on|off       Enable/disable result paging");
    println!();
    
    println!("{}", "Examples:".green().bold());
    println!("  :config data-dir /var/lib/cassandra/data");
    println!("  :config page-size 100");
    println!("  :config timing on");
}

fn show_cql_help() {
    println!("{}", "CQL Query Support".cyan().bold());
    println!("{}", "═".repeat(20).cyan());
    println!();
    
    println!("{}", "Supported CQL Features:".yellow().bold());
    println!("  • SELECT statements with WHERE, LIMIT, ORDER BY");
    println!("  • Complex data types (collections, UDTs, tuples)");
    println!("  • System table queries");
    println!("  • DESCRIBE statements");
    println!("  • COUNT queries");
    println!();
    
    println!("{}", "Query Examples:".green().bold());
    println!("  SELECT * FROM users;");
    println!("  SELECT name, email FROM users WHERE id = 'user123';");
    println!("  SELECT * FROM users LIMIT 10;");
    println!("  SELECT COUNT(*) FROM users;");
    println!("  DESCRIBE TABLE users;");
    println!();
    
    println!("{}", "System Queries:".green().bold());
    println!("  SELECT * FROM system.tables;");
    println!("  SELECT * FROM system.keyspaces;");
    println!("  SELECT * FROM system.columns WHERE table_name = 'users';");
}

fn show_examples_help() {
    println!("{}", "Common Usage Examples".cyan().bold());
    println!("{}", "═".repeat(25).cyan());
    println!();
    
    println!("{}", "Getting Started:".yellow().bold());
    println!("  1. :config data-dir /path/to/cassandra/data");
    println!("  2. :tables");
    println!("  3. :describe my_table");
    println!("  4. SELECT * FROM my_table LIMIT 5;");
    println!();
    
    println!("{}", "Data Exploration Workflow:".yellow().bold());
    println!("  :keyspaces                    # List keyspaces");
    println!("  :use my_keyspace              # Switch keyspace");
    println!("  :tables                       # List tables in keyspace");
    println!("  :info users                   # Get table information");
    println!("  :describe users               # Show table structure");
    println!("  SELECT COUNT(*) FROM users;   # Get row count");
    println!("  SELECT * FROM users LIMIT 10; # Sample data");
    println!();
    
    println!("{}", "Complex Queries:".yellow().bold());
    println!("  SELECT name, emails FROM users WHERE id IN ('u1', 'u2');");
    println!("  SELECT * FROM events WHERE date >= '2024-01-01';");
    println!("  SELECT user_id, COUNT(*) FROM events GROUP BY user_id;");
}

fn show_troubleshooting_help() {
    println!("{}", "Troubleshooting Guide".cyan().bold());
    println!("{}", "═".repeat(25).cyan());
    println!();
    
    println!("{}", "Common Issues:".yellow().bold());
    println!();
    println!("{}", "❌ \"Table not found\" errors:".red());
    println!("  • Use :tables to list available tables");
    println!("  • Check if you need to set :use <keyspace>");
    println!("  • Verify data directory with :config data-dir");
    println!();
    
    println!("{}", "❌ \"No data directory configured\":".red());
    println!("  • Use :config data-dir /path/to/cassandra/data");
    println!("  • Ensure the directory contains SSTable files");
    println!();
    
    println!("{}", "❌ Query parsing errors:".red());
    println!("  • Check CQL syntax with :help cql");
    println!("  • Ensure table and column names are correct");
    println!("  • Use :describe <table> to check schema");
    println!();
    
    println!("{}", "❌ Performance issues:".red());
    println!("  • Use LIMIT clause for large tables");
    println!("  • Configure appropriate page size with :config page-size");
    println!("  • Enable timing with :timing to measure query performance");
    println!();
    
    println!("{}", "✅ Getting Help:".green().bold());
    println!("  • :help commands - Command reference");
    println!("  • :help config - Configuration help");
    println!("  • :help examples - Usage examples");
}

// ========== CONFIGURATION SYSTEM ==========

fn show_current_config(session: &ReplSession) {
    println!("{}", "Current Configuration".cyan().bold());
    println!("{}", "═".repeat(25).cyan());
    println!();
    
    println!("{}", "Database Settings:".yellow().bold());
    println!("  Database Path: {}", session.db_path.display().to_string().green());
    if let Some(ref keyspace) = session.current_keyspace {
        println!("  Current Keyspace: {}", keyspace.green());
    } else {
        println!("  Current Keyspace: {}", "None".yellow());
    }
    
    if let Some(ref data_dir) = session.data_dir {
        println!("  Data Directory: {}", data_dir.display().to_string().green());
    } else {
        println!("  Data Directory: {} (use :config data-dir <path>)", "Not set".yellow());
    }
    println!();
    
    println!("{}", "Display Settings:".yellow().bold());
    println!("  Timing: {}", if session.timing_enabled { "enabled".green() } else { "disabled".red() });
    println!("  Paging: {}", if session.paging_enabled { "enabled".green() } else { "disabled".red() });
    println!("  Page Size: {}", session.page_size.to_string().green());
    println!();
    
    println!("{}", "Session Info:".yellow().bold());
    println!("  Commands in History: {}", session.command_history.len().to_string().green());
    println!();
    
    println!("{}", "💡 Use :config <setting> <value> to change settings".cyan());
}

async fn handle_config_command(config_cmd: &str, session: &mut ReplSession) -> Result<()> {
    let parts: Vec<&str> = config_cmd.split_whitespace().collect();
    
    if parts.is_empty() {
        show_current_config(session);
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
            
            session.data_dir = Some(path.clone());
            println!("{} Data directory set to: {}", "Success:".green().bold(), path.display());
        }
        
        "page-size" => {
            if parts.len() < 2 {
                println!("{} Usage: :config page-size <number>", "Error:".red().bold());
                return Ok(());
            }
            
            match parts[1].parse::<usize>() {
                Ok(size) if size > 0 && size <= 10000 => {
                    session.page_size = size;
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
                session.timing_enabled = !session.timing_enabled;
            } else {
                match parts[1].to_lowercase().as_str() {
                    "on" | "true" | "1" | "yes" => session.timing_enabled = true,
                    "off" | "false" | "0" | "no" => session.timing_enabled = false,
                    _ => {
                        println!("{} Usage: :config timing [on|off]", "Error:".red().bold());
                        return Ok(());
                    }
                }
            }
            println!("{} Timing is now {}", 
                "Info:".cyan().bold(), 
                if session.timing_enabled { "enabled".green() } else { "disabled".red() }
            );
        }
        
        "paging" => {
            if parts.len() < 2 {
                session.paging_enabled = !session.paging_enabled;
            } else {
                match parts[1].to_lowercase().as_str() {
                    "on" | "true" | "1" | "yes" => session.paging_enabled = true,
                    "off" | "false" | "0" | "no" => session.paging_enabled = false,
                    _ => {
                        println!("{} Usage: :config paging [on|off]", "Error:".red().bold());
                        return Ok(());
                    }
                }
            }
            println!("{} Paging is now {}", 
                "Info:".cyan().bold(), 
                if session.paging_enabled { "enabled".green() } else { "disabled".red() }
            );
        }
        
        _ => {
            println!("{} Unknown configuration option: {}", "Error:".red().bold(), parts[0]);
            println!("Available options: data-dir, page-size, timing, paging");
        }
    }
    
    Ok(())
}

// ========== DATA EXPLORATION FUNCTIONS ==========

async fn show_enhanced_tables(session: &ReplSession) -> Result<()> {
    println!("{}", "📋 Available Tables".cyan().bold());
    println!("{}", "═".repeat(20).cyan());
    
    // First try to list from data directory if configured
    if let Some(ref data_dir) = session.data_dir {
        match scan_data_directory(data_dir, session.current_keyspace.as_deref()).await {
            Ok(tables) => {
                if tables.is_empty() {
                    println!("📭 No tables found in data directory");
                    if let Some(ref ks) = session.current_keyspace {
                        println!("💡 Current keyspace: {}", ks.yellow());
                        println!("💡 Try :use <keyspace> to switch keyspaces");
                    }
                } else {
                    display_table_list(&tables, session.current_keyspace.as_deref());
                }
                return Ok(());
            }
            Err(e) => {
                println!("⚠️  Could not scan data directory: {}", e);
                println!("🔄 Falling back to database query...");
            }
        }
    }
    
    // Fallback to database query
    match session.database.execute("SELECT keyspace_name, table_name FROM system.tables WHERE keyspace_name != 'system'").await {
        Ok(result) => {
            if result.rows.is_empty() {
                println!("📭 No user tables found in database");
                println!("💡 Configure data directory: {}", ":config data-dir /path/to/cassandra/data".green());
            } else {
                let mut tables_by_keyspace: HashMap<String, Vec<String>> = HashMap::new();
                
                for row in &result.rows {
                    if let (Some(keyspace), Some(table)) = (row.get("keyspace_name"), row.get("table_name")) {
                        let ks_str = keyspace.to_string();
                        let table_str = table.to_string();
                        tables_by_keyspace.entry(ks_str).or_insert_with(Vec::new).push(table_str);
                    }
                }
                
                for (keyspace, tables) in tables_by_keyspace {
                    println!("\n📦 Keyspace: {}", keyspace.yellow().bold());
                    for table in tables {
                        let indicator = if Some(&keyspace) == session.current_keyspace.as_ref() { "→" } else { " " };
                        println!("  {} 📄 {}", indicator, table.green());
                    }
                }
            }
        }
        Err(e) => {
            println!("❌ Failed to query system tables: {}", e);
            println!("💡 Try configuring a data directory: {}", ":config data-dir <path>".green());
        }
    }
    
    Ok(())
}

async fn show_keyspaces(session: &ReplSession) -> Result<()> {
    println!("{}", "📦 Available Keyspaces".cyan().bold());
    println!("{}", "═".repeat(23).cyan());
    
    // Try database query first
    match session.database.execute("SELECT keyspace_name FROM system.keyspaces").await {
        Ok(result) => {
            if result.rows.is_empty() {
                println!("📭 No keyspaces found");
            } else {
                for row in &result.rows {
                    if let Some(keyspace_name) = row.get("keyspace_name") {
                        let ks_str = keyspace_name.to_string();
                        let indicator = if Some(&ks_str) == session.current_keyspace.as_ref() { "→" } else { " " };
                        let is_current = Some(&ks_str) == session.current_keyspace.as_ref();
                        
                        if ks_str == "system" {
                            println!("  {} 🔧 {} (system)", indicator, ks_str.dimmed());
                        } else if is_current {
                            println!("  {} 📦 {} (current)", indicator, ks_str.green().bold());
                        } else {
                            println!("  {} 📦 {}", indicator, ks_str.green());
                        }
                    }
                }
            }
        }
        Err(e) => {
            println!("❌ Failed to query keyspaces: {}", e);
            
            // Try to infer from data directory
            if let Some(ref data_dir) = session.data_dir {
                println!("🔄 Scanning data directory for keyspaces...");
                match scan_keyspaces_from_directory(data_dir).await {
                    Ok(keyspaces) => {
                        if keyspaces.is_empty() {
                            println!("📭 No keyspaces found in data directory");
                        } else {
                            for keyspace in keyspaces {
                                let indicator = if Some(&keyspace) == session.current_keyspace.as_ref() { "→" } else { " " };
                                println!("  {} 📦 {}", indicator, keyspace.green());
                            }
                        }
                    }
                    Err(scan_err) => {
                        println!("❌ Could not scan data directory: {}", scan_err);
                    }
                }
            }
        }
    }
    
    println!();
    println!("💡 Use {} to switch keyspace", ":use <keyspace_name>".green());
    
    Ok(())
}

async fn show_object_info(object_name: &str, session: &ReplSession) -> Result<()> {
    println!("{} {}", "🔍 Object Information:".cyan().bold(), object_name.yellow());
    println!("{}", "═".repeat(30).cyan());
    
    // Parse object name (could be keyspace.table or just table)
    let (keyspace, table) = if object_name.contains('.') {
        let parts: Vec<&str> = object_name.split('.').collect();
        if parts.len() == 2 {
            (Some(parts[0]), parts[1])
        } else {
            (session.current_keyspace.as_deref(), object_name)
        }
    } else {
        (session.current_keyspace.as_deref(), object_name)
    };
    
    if let Some(ks) = keyspace {
        println!("📦 Keyspace: {}", ks.yellow());
        println!("📄 Table: {}", table.green());
        
        // Try to get table information from system tables
        let table_query = format!(
            "SELECT * FROM system.tables WHERE keyspace_name = '{}' AND table_name = '{}'",
            ks, table
        );
        
        match session.database.execute(&table_query).await {
            Ok(result) => {
                if result.rows.is_empty() {
                    println!("❌ Table not found in system catalog");
                    
                    // Try to find in data directory
                    if let Some(ref data_dir) = session.data_dir {
                        match find_table_in_directory(data_dir, ks, table).await {
                            Ok(Some(table_info)) => {
                                println!("✅ Found in data directory:");
                                println!("  📂 Path: {}", table_info.path.green());
                                println!("  📊 SSTable files: {}", table_info.sstable_count);
                                println!("  💾 Total size: {:.2} MB", table_info.total_size_mb);
                            }
                            Ok(None) => {
                                println!("❌ Table not found in data directory either");
                            }
                            Err(e) => {
                                println!("⚠️  Error scanning data directory: {}", e);
                            }
                        }
                    }
                } else {
                    println!("✅ Found in system catalog");
                    
                    // Display table metadata
                    for row in &result.rows {
                        if let Some(id) = row.get("id") {
                            println!("  🆔 Table ID: {}", id);
                        }
                        if let Some(flags) = row.get("flags") {
                            println!("  🏷️  Flags: {}", flags);
                        }
                    }
                    
                    // Get column information
                    let columns_query = format!(
                        "SELECT column_name, type, kind FROM system.columns WHERE keyspace_name = '{}' AND table_name = '{}' ORDER BY position",
                        ks, table
                    );
                    
                    match session.database.execute(&columns_query).await {
                        Ok(columns_result) => {
                            if !columns_result.rows.is_empty() {
                                println!("\n📋 Columns:");
                                for col_row in &columns_result.rows {
                                    if let (Some(col_name), Some(col_type), Some(col_kind)) = 
                                        (col_row.get("column_name"), col_row.get("type"), col_row.get("kind")) {
                                        let kind_indicator = match col_kind.to_string().as_str() {
                                            "partition_key" => "🔑",
                                            "clustering" => "🔗", 
                                            "regular" => "📝",
                                            _ => "❓",
                                        };
                                        println!("  {} {} ({})", kind_indicator, col_name.to_string().cyan(), col_type.to_string().yellow());
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            println!("⚠️  Could not retrieve column information: {}", e);
                        }
                    }
                }
            }
            Err(e) => {
                println!("❌ Failed to query system tables: {}", e);
            }
        }
    } else {
        println!("❌ No keyspace specified and no current keyspace set");
        println!("💡 Use {} or {}", ":use <keyspace>".green(), ":info keyspace.table".green());
    }
    
    Ok(())
}

async fn describe_table(table_name: &str, session: &ReplSession) -> Result<()> {
    let (keyspace, table) = if table_name.contains('.') {
        let parts: Vec<&str> = table_name.split('.').collect();
        if parts.len() == 2 {
            (Some(parts[0]), parts[1])
        } else {
            (session.current_keyspace.as_deref(), table_name)
        }
    } else {
        (session.current_keyspace.as_deref(), table_name)
    };
    
    if let Some(ks) = keyspace {
        println!("{} {}", "📋 Table Schema:".cyan().bold(), format!("{}.{}", ks, table).yellow());
        println!("{}", "═".repeat(20).cyan());
        
        // Get detailed table schema
        let columns_query = format!(
            "SELECT column_name, type, kind, clustering_order, position FROM system.columns WHERE keyspace_name = '{}' AND table_name = '{}' ORDER BY position",
            ks, table
        );
        
        match session.database.execute(&columns_query).await {
            Ok(result) => {
                if result.rows.is_empty() {
                    println!("❌ Table '{}' not found", table_name);
                    println!("💡 Use {} to list available tables", ":tables".green());
                } else {
                    let mut partition_keys = Vec::new();
                    let mut clustering_keys = Vec::new();
                    let mut regular_columns = Vec::new();
                    
                    for row in &result.rows {
                        if let (Some(col_name), Some(col_type), Some(col_kind)) = 
                            (row.get("column_name"), row.get("type"), row.get("kind")) {
                            let col_info = ColumnInfo {
                                name: col_name.to_string(),
                                data_type: col_type.to_string(),
                                kind: col_kind.to_string(),
                            };
                            
                            match col_kind.to_string().as_str() {
                                "partition_key" => partition_keys.push(col_info),
                                "clustering" => clustering_keys.push(col_info),
                                "regular" => regular_columns.push(col_info),
                                _ => regular_columns.push(col_info),
                            }
                        }
                    }
                    
                    // Display schema in organized format
                    if !partition_keys.is_empty() {
                        println!("🔑 {}", "Partition Keys:".yellow().bold());
                        for pk in partition_keys {
                            println!("  {} {}", pk.name.cyan(), pk.data_type.green());
                        }
                        println!();
                    }
                    
                    if !clustering_keys.is_empty() {
                        println!("🔗 {}", "Clustering Keys:".yellow().bold());
                        for ck in clustering_keys {
                            println!("  {} {}", ck.name.cyan(), ck.data_type.green());
                        }
                        println!();
                    }
                    
                    if !regular_columns.is_empty() {
                        println!("📝 {}", "Regular Columns:".yellow().bold());
                        for col in regular_columns {
                            println!("  {} {}", col.name.cyan(), col.data_type.green());
                        }
                        println!();
                    }
                    
                    // Show CREATE TABLE statement
                    println!("🏗️  {}", "CREATE TABLE Statement:".green().bold());
                    generate_create_table_statement(ks, table, &result.rows);
                }
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

#[derive(Debug)]
struct ColumnInfo {
    name: String,
    data_type: String,
    kind: String,
}

#[derive(Debug)]
struct TableInfo {
    path: String,
    sstable_count: usize,
    total_size_mb: f64,
}

fn generate_create_table_statement(keyspace: &str, table: &str, rows: &[QueryRow]) {
    println!("```sql");
    println!("CREATE TABLE {}.{} (", keyspace, table);
    
    let mut partition_keys = Vec::new();
    let mut clustering_keys = Vec::new();
    
    for (i, row) in rows.iter().enumerate() {
        if let (Some(col_name), Some(col_type), Some(col_kind)) = 
            (row.get("column_name"), row.get("type"), row.get("kind")) {
            let comma = if i < rows.len() - 1 { "," } else { "" };
            println!("    {} {}{}", col_name, col_type, comma);
            
            match col_kind.to_string().as_str() {
                "partition_key" => partition_keys.push(col_name.to_string()),
                "clustering" => clustering_keys.push(col_name.to_string()),
                _ => {}
            }
        }
    }
    
    // Add PRIMARY KEY clause
    if !partition_keys.is_empty() {
        print!(",\n    PRIMARY KEY (");
        if partition_keys.len() == 1 && clustering_keys.is_empty() {
            print!("{}", partition_keys[0]);
        } else {
            print!("({})", partition_keys.join(", "));
            if !clustering_keys.is_empty() {
                print!(", {}", clustering_keys.join(", "));
            }
        }
        println!(")");
    }
    
    println!(");");
    println!("```");
}

async fn show_enhanced_schema(table_name: Option<&str>, session: &ReplSession) -> Result<()> {
    match table_name {
        Some(table) => {
            describe_table(table, session).await
        }
        None => {
            // Show all schemas
            println!("{}", "📋 All Table Schemas".cyan().bold());
            println!("{}", "═".repeat(25).cyan());
            
            match session.database.execute("SELECT DISTINCT keyspace_name, table_name FROM system.tables WHERE keyspace_name != 'system' ORDER BY keyspace_name, table_name").await {
                Ok(result) => {
                    if result.rows.is_empty() {
                        println!("📭 No user tables found");
                        println!("💡 Configure data directory: {}", ":config data-dir /path/to/cassandra/data".green());
                    } else {
                        let mut current_keyspace = String::new();
                        for row in &result.rows {
                            if let (Some(keyspace), Some(table)) = (row.get("keyspace_name"), row.get("table_name")) {
                                let ks_str = keyspace.to_string();
                                if ks_str != current_keyspace {
                                    println!("\n📦 {}", ks_str.yellow().bold());
                                    current_keyspace = ks_str;
                                }
                                println!("  📄 {} (use :describe {} for details)", table.to_string().green(), table);
                            }
                        }
                    }
                }
                Err(e) => {
                    println!("❌ Failed to query schemas: {}", e);
                }
            }
            Ok(())
        }
    }
}

// ========== UTILITY FUNCTIONS ==========

fn show_command_history(session: &ReplSession) {
    println!("{}", "📜 Command History".cyan().bold());
    println!("{}", "═".repeat(20).cyan());
    
    if session.command_history.is_empty() {
        println!("📭 No commands in history yet");
        return;
    }
    
    let start_index = session.command_history.len().saturating_sub(20);
    for (i, cmd) in session.command_history.iter().skip(start_index).enumerate() {
        let line_num = start_index + i + 1;
        if cmd.starts_with("SELECT") || cmd.starts_with("select") {
            println!("  {:3}. {}", line_num, cmd.yellow());
        } else if cmd.starts_with(":") {
            println!("  {:3}. {}", line_num, cmd.cyan());
        } else {
            println!("  {:3}. {}", line_num, cmd);
        }
    }
    
    println!();
    println!("💡 Showing last {} commands", (session.command_history.len().min(20)));
}

async fn use_keyspace(keyspace: &str, session: &mut ReplSession) -> Result<()> {
    // Validate keyspace exists
    let keyspace_query = format!("SELECT keyspace_name FROM system.keyspaces WHERE keyspace_name = '{}'", keyspace);
    
    match session.database.execute(&keyspace_query).await {
        Ok(result) => {
            if result.rows.is_empty() {
                println!("❌ Keyspace '{}' not found", keyspace);
                println!("💡 Use {} to list available keyspaces", ":keyspaces".green());
            } else {
                session.current_keyspace = Some(keyspace.to_string());
                println!("{} Now using keyspace: {}", "✅".green(), keyspace.yellow().bold());
            }
        }
        Err(e) => {
            // If system query fails, allow setting anyway (might be valid in data directory)
            println!("⚠️  Could not verify keyspace ({}), setting anyway...", e);
            session.current_keyspace = Some(keyspace.to_string());
            println!("{} Keyspace set to: {}", "⚠️ ".yellow(), keyspace.yellow().bold());
        }
    }
    
    Ok(())
}

async fn source_file(file_path: &str, session: &mut ReplSession) -> Result<()> {
    use std::fs;
    
    println!("{} Executing commands from: {}", "📂".cyan(), file_path.yellow());
    
    let path = PathBuf::from(file_path);
    if !path.exists() {
        return Err(anyhow::anyhow!("File not found: {}", file_path));
    }
    
    let content = fs::read_to_string(&path)
        .map_err(|e| anyhow::anyhow!("Failed to read file {}: {}", file_path, e))?;
    
    let lines: Vec<&str> = content.lines().collect();
    let mut executed = 0;
    let mut errors = 0;
    
    for (line_num, line) in lines.iter().enumerate() {
        let trimmed = line.trim();
        
        // Skip empty lines and comments
        if trimmed.is_empty() || trimmed.starts_with("--") || trimmed.starts_with("#") {
            continue;
        }
        
        println!("{}:{} {}", file_path, line_num + 1, trimmed.dimmed());
        
        match execute_repl_command_from_file(trimmed, session).await {
            Ok(should_continue) => {
                if !should_continue {
                    println!("🛑 Execution stopped due to exit command");
                    break;
                }
                executed += 1;
            }
            Err(e) => {
                eprintln!("❌ Error on line {}: {}", line_num + 1, e);
                errors += 1;
            }
        }
    }
    
    println!();
    println!("📊 File execution completed:");
    println!("  ✅ Commands executed: {}", executed);
    if errors > 0 {
        println!("  ❌ Errors: {}", errors);
    }
    
    Ok(())
}

// Helper function to avoid recursion in source_file
async fn execute_repl_command_from_file(input: &str, session: &mut ReplSession) -> Result<bool> {
    // Same logic as handle_repl_command but simplified for file execution
    match input {
        // Exit commands
        ":quit" | ":exit" | ":q" | ".quit" | ".exit" | "\\q" => {
            return Ok(false);
        }
        
        // Configuration commands
        ":config" => {
            show_current_config(session);
        }
        cmd if cmd.starts_with(":config ") => {
            let config_cmd = cmd.strip_prefix(":config ").unwrap_or("").trim();
            handle_config_command(config_cmd, session).await?;
        }
        
        // Data exploration commands
        ":tables" | ":list" => {
            show_enhanced_tables(session).await?;
        }
        ":keyspaces" => {
            show_keyspaces(session).await?;
        }
        cmd if cmd.starts_with(":info ") => {
            let object_name = cmd.strip_prefix(":info ").unwrap_or("").trim();
            show_object_info(object_name, session).await?;
        }
        cmd if cmd.starts_with(":describe ") || cmd.starts_with(":desc ") => {
            let prefix = if cmd.starts_with(":describe ") { ":describe " } else { ":desc " };
            let table_name = cmd.strip_prefix(prefix).unwrap_or("").trim();
            describe_table(table_name, session).await?;
        }
        
        // Utility commands
        ":clear" | ":cls" => {
            print!("\\x1B[2J\\x1B[1;1H");
            io::stdout().flush()?;
        }
        ":timing" => {
            session.timing_enabled = !session.timing_enabled;
            println!("{} Timing is now {}", 
                "Info:".cyan().bold(), 
                if session.timing_enabled { "enabled".green() } else { "disabled".red() }
            );
        }
        
        // Data source commands
        cmd if cmd.starts_with(":use ") => {
            let keyspace = cmd.strip_prefix(":use ").unwrap_or("").trim();
            use_keyspace(keyspace, session).await?;
        }
        
        // Unknown meta-command
        _ if input.starts_with(':') => {
            eprintln!("{} Unknown command: {}", "Error:".red().bold(), input);
        }
        _ if input.starts_with('.') => {
            eprintln!("{} Unknown command: {}", "Error:".red().bold(), input);
        }
        
        // Execute as CQL query
        _ => {
            execute_enhanced_cql_query(input, session).await?;
        }
    }

    Ok(true)
}

// ========== ENHANCED CQL EXECUTION ==========

async fn execute_enhanced_cql_query(query: &str, session: &ReplSession) -> Result<()> {
    let start_time = if session.timing_enabled {
        Some(Instant::now())
    } else {
        None
    };
    
    println!("{} {}", "🔍 Executing:".blue().bold(), query.yellow());
    
    // Execute the query with enhanced error handling
    match session.database.execute(query).await {
        Ok(result) => {
            let execution_time = start_time.map(|t| t.elapsed());
            println!();
            
            // Display results based on query type and session settings
            if result.rows.is_empty() && result.rows_affected > 0 {
                // DML query (INSERT, UPDATE, DELETE)
                println!(
                    "{} {} rows affected",
                    "✅".green().bold(),
                    result.rows_affected
                );
            } else if !result.rows.is_empty() {
                // SELECT query with results
                display_enhanced_query_results(&result, session)?;
                
                // Show result summary
                println!();
                println!(
                    "{} Returned {} row{}",
                    "📊 Results:".cyan().bold(),
                    result.rows.len(),
                    if result.rows.len() == 1 { "" } else { "s" }
                );
            } else {
                // DDL query or empty result
                println!("{} Query executed successfully", "✅".green().bold());
            }
            
            // Show timing information if enabled
            if let (Some(elapsed), true) = (execution_time, session.timing_enabled) {
                println!();
                println!(
                    "{} Execution time: {:.2}ms",
                    "⏱️  Query completed:".green(),
                    elapsed.as_millis()
                );
                
                // Show performance metrics if available
                let performance = result.performance();
                if performance.total_time_us > 0 {
                    println!(
                        "{} Parse: {:.2}ms | Planning: {:.2}ms | Execution: {:.2}ms",
                        "🔧 Timing breakdown:".dimmed(),
                        performance.parse_time_us as f64 / 1000.0,
                        performance.planning_time_us as f64 / 1000.0,
                        performance.execution_time_us as f64 / 1000.0
                    );
                    
                    if performance.memory_usage_bytes > 0 {
                        println!(
                            "{} Memory used: {:.2} KB",
                            "💾 Resources:".dimmed(),
                            performance.memory_usage_bytes as f64 / 1024.0
                        );
                    }
                    
                    if performance.cache_hits + performance.cache_misses > 0 {
                        println!(
                            "{} Cache hit ratio: {:.1}%",
                            "🎯 Cache:".dimmed(),
                            performance.cache_hit_ratio() * 100.0
                        );
                    }
                }
            }
            
            // Display warnings if any
            let warnings = result.warnings();
            if !warnings.is_empty() {
                println!();
                println!(
                    "{} Warnings:", "⚠️ ".yellow().bold()
                );
                for warning in warnings {
                    println!("  ⚠️  {}", warning.to_string().yellow());
                }
            }
        }
        Err(e) => {
            let execution_time = start_time.map(|t| t.elapsed());
            println!();
            
            if let Some(elapsed) = execution_time {
                eprintln!("{} Query failed after {:.2}ms", "❌ Error:".red().bold(), elapsed.as_millis());
            } else {
                eprintln!("{} Query failed", "❌ Error:".red().bold());
            }
            
            // Provide more detailed error information and hints
            provide_cql_error_hints(&anyhow::anyhow!(e), query, session);
        }
    }

    Ok(())
}

/// Display query results with enhanced formatting and paging support
fn display_enhanced_query_results(result: &QueryResult, session: &ReplSession) -> Result<()> {
    if result.rows.is_empty() {
        println!("{}", "📭 No rows returned".yellow());
        return Ok(());
    }

    // Get column names from the result metadata
    let column_names = result.column_names();
    if column_names.is_empty() {
        println!("{}", "❓ No columns in result".yellow());
        return Ok(());
    }

    let total_rows = result.rows.len();
    let should_page = session.paging_enabled && total_rows > session.page_size;
    
    if should_page {
        display_paged_results(result, &column_names, session)?;
    } else {
        display_table_results(result, &column_names)?;
    }

    Ok(())
}

fn display_table_results(result: &QueryResult, column_names: &[String]) -> Result<()> {
    // Calculate optimal column widths
    let mut col_widths = Vec::new();
    for col_name in column_names {
        let mut max_width = col_name.len();
        for row in result.iter() {
            if let Some(value) = row.get(col_name) {
                max_width = max_width.max(format!("{}", value).len());
            }
        }
        col_widths.push(max_width.max(8).min(50)); // minimum 8, maximum 50
    }

    println!("{}", "📊 Results:".green().bold());

    // Print top border
    print!("┌");
    for (i, width) in col_widths.iter().enumerate() {
        print!("{}", "─".repeat(width + 2));
        if i < col_widths.len() - 1 {
            print!("┬");
        }
    }
    println!("┐");

    // Print header
    print!("│");
    for (i, (col_name, width)) in column_names.iter().zip(col_widths.iter()).enumerate() {
        print!(" {:width$} ", col_name.bold().cyan(), width = width);
        if i < column_names.len() - 1 {
            print!("│");
        }
    }
    println!("│");

    // Print header separator
    print!("├");
    for (i, width) in col_widths.iter().enumerate() {
        print!("{}", "─".repeat(width + 2));
        if i < col_widths.len() - 1 {
            print!("┼");
        }
    }
    println!("┤");

    // Print rows
    for row in result.iter() {
        print!("│");
        for (i, (col_name, width)) in column_names.iter().zip(col_widths.iter()).enumerate() {
            let value = row.get(col_name)
                .map(|v| {
                    let s = format!("{}", v);
                    if s.len() > *width {
                        format!("{}...", &s[..width.saturating_sub(3)])
                    } else {
                        s
                    }
                })
                .unwrap_or_else(|| "NULL".dimmed().to_string());
            print!(" {:width$} ", value, width = width);
            if i < column_names.len() - 1 {
                print!("│");
            }
        }
        println!("│");
    }

    // Print bottom border
    print!("└");
    for (i, width) in col_widths.iter().enumerate() {
        print!("{}", "─".repeat(width + 2));
        if i < col_widths.len() - 1 {
            print!("┴");
        }
    }
    println!("┘");

    Ok(())
}

fn display_paged_results(result: &QueryResult, column_names: &[String], session: &ReplSession) -> Result<()> {
    let total_rows = result.rows.len();
    let page_size = session.page_size;
    let total_pages = (total_rows + page_size - 1) / page_size;
    
    println!("{} {} rows total, showing {} per page", 
        "📊 Paged Results:".green().bold(), 
        total_rows, 
        page_size
    );
    
    for page in 0..total_pages {
        let start_idx = page * page_size;
        let end_idx = ((page + 1) * page_size).min(total_rows);
        
        println!();
        println!("{} Page {} of {} (rows {}-{})", 
            "📄".cyan(), 
            page + 1, 
            total_pages, 
            start_idx + 1, 
            end_idx
        );
        
        // Create a subset result for this page
        let page_rows: Vec<_> = result.rows.iter().skip(start_idx).take(end_idx - start_idx).collect();
        
        if !page_rows.is_empty() {
            display_page_table(&page_rows, column_names)?;
        }
        
        // Ask user if they want to continue (except for last page)
        if page < total_pages - 1 {
            print!("\n{} Press Enter for next page, 'q' to quit: ", "❓".cyan());
            io::stdout().flush()?;
            
            let mut input = String::new();
            io::stdin().read_line(&mut input)?;
            
            if input.trim().to_lowercase() == "q" {
                println!("📄 Stopped at page {} of {}", page + 1, total_pages);
                break;
            }
        }
    }
    
    Ok(())
}

fn display_page_table(rows: &[&QueryRow], column_names: &[String]) -> Result<()> {
    // Similar to display_table_results but for a subset of rows
    let mut col_widths = Vec::new();
    for col_name in column_names {
        let mut max_width = col_name.len();
        for row in rows {
            if let Some(value) = row.get(col_name) {
                max_width = max_width.max(format!("{}", value).len());
            }
        }
        col_widths.push(max_width.max(8).min(50));
    }

    // Print header
    print!("┌");
    for (i, width) in col_widths.iter().enumerate() {
        print!("{}", "─".repeat(width + 2));
        if i < col_widths.len() - 1 { print!("┬"); }
    }
    println!("┐");

    print!("│");
    for (i, (col_name, width)) in column_names.iter().zip(col_widths.iter()).enumerate() {
        print!(" {:width$} ", col_name.bold().cyan(), width = width);
        if i < column_names.len() - 1 { print!("│"); }
    }
    println!("│");

    print!("├");
    for (i, width) in col_widths.iter().enumerate() {
        print!("{}", "─".repeat(width + 2));
        if i < col_widths.len() - 1 { print!("┼"); }
    }
    println!("┤");

    // Print rows
    for row in rows {
        print!("│");
        for (i, (col_name, width)) in column_names.iter().zip(col_widths.iter()).enumerate() {
            let value = row.get(col_name)
                .map(|v| {
                    let s = format!("{}", v);
                    if s.len() > *width {
                        format!("{}...", &s[..width.saturating_sub(3)])
                    } else {
                        s
                    }
                })
                .unwrap_or_else(|| "NULL".dimmed().to_string());
            print!(" {:width$} ", value, width = width);
            if i < column_names.len() - 1 { print!("│"); }
        }
        println!("│");
    }

    print!("└");
    for (i, width) in col_widths.iter().enumerate() {
        print!("{}", "─".repeat(width + 2));
        if i < col_widths.len() - 1 { print!("┴"); }
    }
    println!("┘");

    Ok(())
}

fn provide_cql_error_hints(error: &anyhow::Error, query: &str, session: &ReplSession) {
    let error_msg = error.to_string();
    eprintln!("  {}", error_msg.red());
    
    println!();
    
    // Provide helpful hints based on error type and query pattern
    if error_msg.contains("table") && error_msg.contains("not found") {
        println!("{} Table Not Found Help:", "💡 Hint:".cyan().bold());
        println!("  • Use {} to list available tables", ":tables".green());
        println!("  • Check table name spelling and case sensitivity");
        println!("  • Use {} to see table schema", ":describe <table>".green());
        if session.current_keyspace.is_none() {
            println!("  • Set keyspace with {} or use fully qualified name", ":use <keyspace>".green());
        }
    } else if error_msg.contains("keyspace") && error_msg.contains("not found") {
        println!("{} Keyspace Not Found Help:", "💡 Hint:".cyan().bold());
        println!("  • Use {} to list available keyspaces", ":keyspaces".green());
        println!("  • Check keyspace name spelling");
        if session.data_dir.is_none() {
            println!("  • Configure data directory: {}", ":config data-dir /path".green());
        }
    } else if error_msg.contains("syntax") || error_msg.contains("parse") {
        println!("{} CQL Syntax Help:", "💡 Hint:".cyan().bold());
        println!("  • Check CQL reference: {}", ":help cql".green());
        println!("  • Example queries: {}", ":help examples".green());
        
        if query.to_uppercase().contains("SELECT") {
            println!("  • Basic SELECT: {}", "SELECT * FROM table_name LIMIT 10;".yellow());
            println!("  • With WHERE: {}", "SELECT * FROM table WHERE column = 'value';".yellow());
        }
    } else if error_msg.contains("column") {
        println!("{} Column Help:", "💡 Hint:".cyan().bold());
        println!("  • Use {} to see table columns", ":describe <table>".green());
        println!("  • Check column name spelling and case sensitivity");
        println!("  • Verify the table has the expected schema");
    } else if error_msg.contains("constraint") || error_msg.contains("duplicate") {
        println!("{} Data Constraint Help:", "💡 Hint:".cyan().bold());
        println!("  • Check for duplicate primary key values");
        println!("  • Verify data types match column definitions");
        println!("  • Review table constraints with :describe");
    } else {
        println!("{} General Troubleshooting:", "💡 Hint:".cyan().bold());
        println!("  • Check comprehensive help: {}", ":help troubleshooting".green());
        println!("  • View configuration: {}", ":config".green());
        println!("  • Enable timing for performance insights: {}", ":timing".green());
    }
}

// ========== DATA DIRECTORY INTEGRATION ==========

async fn scan_data_directory(data_dir: &Path, keyspace_filter: Option<&str>) -> Result<Vec<String>> {
    use std::fs;
    
    if !data_dir.exists() {
        return Err(anyhow::anyhow!("Data directory does not exist: {}", data_dir.display()));
    }
    
    let mut tables = Vec::new();
    
    // If keyspace filter is provided, scan only that keyspace directory
    if let Some(keyspace) = keyspace_filter {
        let keyspace_dir = data_dir.join(keyspace);
        if keyspace_dir.exists() {
            tables.extend(scan_keyspace_directory(&keyspace_dir, keyspace).await?);
        }
    } else {
        // Scan all keyspace directories
        for entry in fs::read_dir(data_dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_dir() {
                let keyspace_name = path.file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown");
                
                // Skip common non-keyspace directories
                if keyspace_name.starts_with('.') || keyspace_name == "system" {
                    continue;
                }
                
                tables.extend(scan_keyspace_directory(&path, keyspace_name).await?);
            }
        }
    }
    
    Ok(tables)
}

async fn scan_keyspace_directory(keyspace_dir: &Path, keyspace_name: &str) -> Result<Vec<String>> {
    use std::fs;
    
    let mut tables = Vec::new();
    
    for entry in fs::read_dir(keyspace_dir)? {
        let entry = entry?;
        let path = entry.path();
        
        if path.is_dir() {
            let dir_name = path.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("unknown");
            
            // SSTable directories usually follow pattern: tablename-uuid
            if let Some(table_name) = extract_table_name_from_directory(dir_name) {
                let qualified_name = format!("{}.{}", keyspace_name, table_name);
                if !tables.contains(&qualified_name) {
                    tables.push(qualified_name);
                }
            }
        }
    }
    
    Ok(tables)
}

fn extract_table_name_from_directory(dir_name: &str) -> Option<String> {
    // Expected format: tablename-uuid
    if let Some(dash_pos) = dir_name.find('-') {
        let table_part = &dir_name[..dash_pos];
        if !table_part.is_empty() && table_part.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return Some(table_part.to_string());
        }
    }
    None
}

fn display_table_list(tables: &[String], current_keyspace: Option<&str>) {
    let mut tables_by_keyspace: HashMap<String, Vec<String>> = HashMap::new();
    
    for table in tables {
        if let Some(dot_pos) = table.find('.') {
            let keyspace = &table[..dot_pos];
            let table_name = &table[dot_pos + 1..];
            tables_by_keyspace.entry(keyspace.to_string()).or_insert_with(Vec::new).push(table_name.to_string());
        }
    }
    
    for (keyspace, table_list) in tables_by_keyspace {
        let is_current = Some(keyspace.as_str()) == current_keyspace;
        let keyspace_display = if is_current {
            format!("📦 {} (current)", keyspace.green().bold())
        } else {
            format!("📦 {}", keyspace.yellow())
        };
        
        println!("\n{}", keyspace_display);
        for table in table_list {
            let indicator = if is_current { "→" } else { " " };
            println!("  {} 📄 {}", indicator, table.green());
        }
    }
}

async fn scan_keyspaces_from_directory(data_dir: &Path) -> Result<Vec<String>> {
    use std::fs;
    
    let mut keyspaces = Vec::new();
    
    for entry in fs::read_dir(data_dir)? {
        let entry = entry?;
        let path = entry.path();
        
        if path.is_dir() {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if !name.starts_with('.') && name != "system" {
                    keyspaces.push(name.to_string());
                }
            }
        }
    }
    
    keyspaces.sort();
    Ok(keyspaces)
}

async fn find_table_in_directory(data_dir: &Path, keyspace: &str, table: &str) -> Result<Option<TableInfo>> {
    use std::fs;
    
    let keyspace_dir = data_dir.join(keyspace);
    if !keyspace_dir.exists() {
        return Ok(None);
    }
    
    for entry in fs::read_dir(&keyspace_dir)? {
        let entry = entry?;
        let path = entry.path();
        
        if path.is_dir() {
            let dir_name = path.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("");
            
            if let Some(extracted_table) = extract_table_name_from_directory(dir_name) {
                if extracted_table == table {
                    // Count SSTable files and calculate total size
                    let mut sstable_count = 0;
                    let mut total_size = 0u64;
                    
                    for file_entry in fs::read_dir(&path)? {
                        let file_entry = file_entry?;
                        let file_path = file_entry.path();
                        
                        if file_path.is_file() {
                            if let Some(file_name) = file_path.file_name().and_then(|n| n.to_str()) {
                                if file_name.contains("Data.db") {
                                    sstable_count += 1;
                                }
                            }
                            
                            if let Ok(metadata) = file_entry.metadata() {
                                total_size += metadata.len();
                            }
                        }
                    }
                    
                    return Ok(Some(TableInfo {
                        path: path.display().to_string(),
                        sstable_count,
                        total_size_mb: total_size as f64 / 1_048_576.0,
                    }));
                }
            }
        }
    }
    
    Ok(None)
}

// Legacy function - replaced with show_enhanced_schema

async fn show_database_info(db_path: &Path) -> Result<()> {
    println!("{}", "Database Information:".cyan().bold());
    println!("  Version: CQLite {}", env!("CARGO_PKG_VERSION"));
    println!("  Database path: {}", db_path.display());
    
    // Get file size if database exists
    if db_path.exists() {
        if let Ok(metadata) = std::fs::metadata(db_path) {
            let size_mb = metadata.len() as f64 / 1_048_576.0;
            println!("  File size: {:.2} MB", size_mb);
            
            if let Ok(modified) = metadata.modified() {
                if let Ok(system_time) = modified.duration_since(std::time::UNIX_EPOCH) {
                    let datetime = chrono::DateTime::<chrono::Utc>::from_timestamp(
                        system_time.as_secs() as i64, 0
                    ).unwrap_or_default();
                    println!("  Last modified: {}", datetime.format("%Y-%m-%d %H:%M:%S UTC"));
                }
            }
        }
    } else {
        println!("  Status: Database file does not exist");
        println!("  Note: Will be created on first query");
    }
    
    // Try to connect and get additional statistics
    let core_config = CoreConfig::default();
    if let Ok(database) = Database::open(db_path, core_config).await {
        if let Ok(_stats) = database.stats().await {
            println!("  Query engine: Active");
            println!("  Storage engine: {}", "SSTable-based");
            
            // Try to count tables
            if let Ok(result) = database.execute("SELECT COUNT(*) as table_count FROM system.tables WHERE keyspace_name != 'system'").await {
                if let Some(row) = result.rows.first() {
                    if let Some(count) = row.get("table_count") {
                        println!("  Tables: {}", count);
                    }
                }
            }
        }
    }
    
    Ok(())
}

/// Display query results in a formatted table
fn display_query_results(result: &QueryResult) -> Result<()> {
    if result.rows.is_empty() {
        println!("{}", "No rows returned".yellow());
        return Ok(());
    }

    // Get column names from the first row or metadata
    let column_names: Vec<String> = if !result.rows.is_empty() {
        result.rows[0].column_names()
    } else if !result.metadata.columns.is_empty() {
        result.metadata.columns.iter().map(|c| c.name.clone()).collect()
    } else {
        vec!["value".to_string()] // fallback
    };

    if column_names.is_empty() {
        println!("{}", "No columns in result".yellow());
        return Ok(());
    }

    // Calculate column widths
    let mut col_widths = Vec::new();
    for col_name in &column_names {
        let mut max_width = col_name.len();
        for row in &result.rows {
            if let Some(value) = row.get(col_name) {
                max_width = max_width.max(format!("{}", value).len());
            }
        }
        col_widths.push(max_width.max(8)); // minimum width of 8
    }

    println!("{}", "Results:".green().bold());

    // Print top border
    print!("┌");
    for (i, width) in col_widths.iter().enumerate() {
        print!("{}", "─".repeat(width + 2));
        if i < col_widths.len() - 1 {
            print!("┬");
        }
    }
    println!("┐");

    // Print header
    print!("│");
    for (i, (col_name, width)) in column_names.iter().zip(col_widths.iter()).enumerate() {
        print!(" {:width$} ", col_name.bold(), width = width);
        if i < column_names.len() - 1 {
            print!("│");
        }
    }
    println!("│");

    // Print header separator
    print!("├");
    for (i, width) in col_widths.iter().enumerate() {
        print!("{}", "─".repeat(width + 2));
        if i < col_widths.len() - 1 {
            print!("┼");
        }
    }
    println!("┤");

    // Print rows
    for row in &result.rows {
        print!("│");
        for (i, (col_name, width)) in column_names.iter().zip(col_widths.iter()).enumerate() {
            let value = row.get(col_name)
                .map(|v| format!("{}", v))
                .unwrap_or_else(|| "NULL".to_string());
            print!(" {:width$} ", value, width = width);
            if i < column_names.len() - 1 {
                print!("│");
            }
        }
        println!("│");
    }

    // Print bottom border
    print!("└");
    for (i, width) in col_widths.iter().enumerate() {
        print!("{}", "─".repeat(width + 2));
        if i < col_widths.len() - 1 {
            print!("┴");
        }
    }
    println!("┘");

    Ok(())
}

async fn execute_cql_query(query: &str, database: &Database) -> Result<()> {
    use std::time::Instant;
    
    println!("{} {}", "Executing:".blue().bold(), query.yellow());
    
    let start_time = Instant::now();
    
    // Execute the query with enhanced error handling
    match database.execute(query).await {
        Ok(result) => {
            let execution_time = start_time.elapsed();
            println!();
            
            // Display results based on query type
            if result.rows.is_empty() && result.rows_affected > 0 {
                // DML query (INSERT, UPDATE, DELETE)
                println!(
                    "{} {} rows affected",
                    "✓".green().bold(),
                    result.rows_affected
                );
            } else if !result.rows.is_empty() {
                // SELECT query with results
                display_query_results(&result)?;
                
                // Show result summary
                println!();
                println!(
                    "{} Returned {} row{}",
                    "Results:".cyan().bold(),
                    result.rows.len(),
                    if result.rows.len() == 1 { "" } else { "s" }
                );
            } else {
                // DDL query or empty result
                println!("{} Query executed successfully", "✓".green().bold());
            }
            
            println!();
            println!(
                "{} Execution time: {:.2}ms",
                "Query completed:".green(),
                execution_time.as_millis()
            );
            
            // Show performance metrics if available
            let performance = result.performance();
            if performance.total_time_us > 0 {
                println!(
                    "{} Parse: {:.2}ms | Planning: {:.2}ms | Execution: {:.2}ms",
                    "Timing breakdown:".dimmed(),
                    performance.parse_time_us as f64 / 1000.0,
                    performance.planning_time_us as f64 / 1000.0,
                    performance.execution_time_us as f64 / 1000.0
                );
                
                if performance.memory_usage_bytes > 0 {
                    println!(
                        "{} Memory used: {:.2} KB",
                        "Resources:".dimmed(),
                        performance.memory_usage_bytes as f64 / 1024.0
                    );
                }
                
                if performance.cache_hits + performance.cache_misses > 0 {
                    println!(
                        "{} Cache hit ratio: {:.1}%",
                        "Cache:".dimmed(),
                        performance.cache_hit_ratio() * 100.0
                    );
                }
            }
            
            // Display warnings if any
            let warnings = result.warnings();
            if !warnings.is_empty() {
                println!();
                println!(
                    "{} Warnings:", "⚠️".yellow().bold()
                );
                for warning in warnings {
                    println!("  ⚠️  {}", warning.to_string().yellow());
                }
            }
        }
        Err(e) => {
            let execution_time = start_time.elapsed();
            println!();
            eprintln!("{} Query failed after {:.2}ms", "Error:".red().bold(), execution_time.as_millis());
            
            // Provide more detailed error information
            let error_msg = e.to_string();
            eprintln!("  {}", error_msg.red());
            
            // Provide helpful hints based on error type
            if error_msg.contains("table") && error_msg.contains("not found") {
                println!();
                println!("{} Try:", "Hint:".cyan().bold());
                println!("  • Use {} to list available tables", ".tables".green());
                println!("  • Check table name spelling");
                println!("  • Use {} to see table schema", ".schema [table]".green());
            } else if error_msg.contains("syntax") || error_msg.contains("parse") {
                println!();
                println!("{} CQL syntax help:", "Hint:".cyan().bold());
                println!("  • SELECT column1, column2 FROM table_name;");
                println!("  • INSERT INTO table_name (col1, col2) VALUES (val1, val2);");
                println!("  • UPDATE table_name SET col1 = val1 WHERE condition;");
                println!("  • DELETE FROM table_name WHERE condition;");
            } else if error_msg.contains("column") {
                println!();
                println!("{} Column tips:", "Hint:".cyan().bold());
                println!("  • Use {} to see table structure", ".schema table_name".green());
                println!("  • Check column name spelling and case sensitivity");
            } else if error_msg.contains("constraint") || error_msg.contains("duplicate") {
                println!();
                println!("{} Data constraint issue:", "Hint:".cyan().bold());
                println!("  • Check for duplicate primary key values");
                println!("  • Verify data types match column definitions");
                println!("  • Review table constraints");
            }
        }
    }

    Ok(())
}

// Helper structures for database integration
struct DatabaseInfo {
    database: Database,
    schema: Arc<SchemaManager>,
}

// Get database instance with proper initialization
async fn get_database_instance(db_path: &Path) -> Result<DatabaseInfo> {
    let config = CoreConfig::default();
    
    // Try to initialize database - if it fails, we'll work in demo mode
    match Database::open(db_path, config.clone()).await {
        Ok(database) => {
            // Try to initialize platform and storage engine for schema manager
            match (Platform::new(&config).await, Platform::new(&config).await) {
                (Ok(platform1), Ok(_platform2)) => {
                    match StorageEngine::open(db_path, &config, Arc::new(platform1)).await {
                        Ok(storage) => {
                            match SchemaManager::new(Arc::new(storage), &config).await {
                                Ok(schema) => Ok(DatabaseInfo { database, schema: Arc::new(schema) }),
                                Err(_) => {
                                    // Create a basic database info with limited schema support
                                    create_demo_database_info(database).await
                                }
                            }
                        },
                        Err(_) => create_demo_database_info(database).await,
                    }
                },
                _ => create_demo_database_info(database).await,
            }
        },
        Err(e) => Err(e.into()),
    }
}

// Create demo database info when full initialization fails
async fn create_demo_database_info(database: Database) -> Result<DatabaseInfo> {
    // Create a mock schema manager - this would be replaced with proper implementation
    // when the core library compilation issues are resolved
    let config = CoreConfig::default();
    let platform = Arc::new(Platform::new(&config).await?);
    let storage = Arc::new(StorageEngine::open(&std::path::PathBuf::from("demo.db"), &config, platform).await?);
    let schema = Arc::new(SchemaManager::new(storage, &config).await?);
    
    Ok(DatabaseInfo { database, schema })
}

// Get list of tables from schema manager
async fn get_table_list(_schema_manager: &Arc<SchemaManager>) -> Result<Vec<TableSchema>> {
    // Try to get tables from schema manager
    // This is a simplified implementation - in a real system, you'd iterate through
    // loaded schemas or query the database catalog
    
    // For now, return empty list as the schema manager would need enhancement
    // to track all loaded table schemas
    Ok(Vec::new())
}

// Format primary key information for display
fn format_primary_key(table_schema: &TableSchema) -> String {
    let mut parts = Vec::new();
    
    // Add partition keys
    for pk in &table_schema.partition_keys {
        parts.push(pk.name.clone());
    }
    
    // Add clustering keys
    if !table_schema.clustering_keys.is_empty() {
        let clustering: Vec<String> = table_schema.clustering_keys
            .iter()
            .map(|ck| ck.name.clone())
            .collect();
        parts.push(format!("({})", clustering.join(", ")));
    }
    
    if parts.is_empty() {
        "No primary key".to_string()
    } else {
        parts.join(", ")
    }
}

// Show schema for all tables
async fn show_all_schemas(schema_manager: &Arc<SchemaManager>) -> Result<()> {
    let tables = get_table_list(schema_manager).await?;
    
    if tables.is_empty() {
        println!("No table schemas available.");
        println!("Use CREATE TABLE statements to define schemas.");
        return Ok(());
    }
    
    let mut current_keyspace = String::new();
    for table_schema in tables {
        if table_schema.keyspace != current_keyspace {
            println!("{}", format!("Keyspace: {}", table_schema.keyspace).yellow().bold());
            current_keyspace = table_schema.keyspace.clone();
        }
        
        display_table_schema(&table_schema);
        println!();
    }
    
    Ok(())
}

// Show schema for a specific table
async fn show_table_schema(schema_manager: &Arc<SchemaManager>, table_name: &str) -> Result<()> {
    let tables = get_table_list(schema_manager).await?;
    
    if let Some(table_schema) = tables.iter().find(|t| t.table == table_name) {
        println!("{}", format!("Table: {}.{}", table_schema.keyspace, table_schema.table).yellow().bold());
        display_table_schema(table_schema);
    } else {
        println!("{}", format!("Table '{}' not found", table_name).red());
        println!("Use {} to list available tables", ".tables".green());
    }
    
    Ok(())
}

// Display detailed table schema information
fn display_table_schema(table_schema: &TableSchema) {
    println!("  {}", "Columns:".cyan().bold());
    
    for column in &table_schema.columns {
        let mut constraints = Vec::new();
        
        if table_schema.is_partition_key(&column.name) {
            constraints.push("PARTITION KEY".to_string());
        }
        if table_schema.is_clustering_key(&column.name) {
            constraints.push("CLUSTERING KEY".to_string());
        }
        if !column.nullable {
            constraints.push("NOT NULL".to_string());
        }
        
        let constraint_text = if constraints.is_empty() {
            String::new()
        } else {
            format!(" ({})", constraints.join(", "))
        };
        
        println!("    {} {}{}", 
            column.name.green(), 
            column.data_type.cyan(),
            constraint_text.blue()
        );
    }
    
    // Show primary key information
    if !table_schema.partition_keys.is_empty() {
        let pk_info = format_primary_key(table_schema);
        println!("  {}: {}", "Primary Key".cyan().bold(), pk_info.yellow());
    }
    
    // Show comments if any
    if !table_schema.comments.is_empty() {
        println!("  {}", "Comments:".cyan().bold());
        for (key, comment) in &table_schema.comments {
            println!("    {}: {}", key.green(), comment);
        }
    }
}
