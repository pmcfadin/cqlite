use crate::config::Config;
use anyhow::Result;
use colored::*;
use cqlite_core::{
    schema::{SchemaManager, TableSchema},
    storage::StorageEngine,
    platform::Platform,
    Database, Config as CoreConfig, QueryResult,
};
use std::collections::HashMap;
use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;
use prettytable::{Cell, Row, Table};

pub async fn start_repl_mode(db_path: &Path, config: &Config, database: Database) -> Result<()> {
    println!("{}", "CQLite Interactive Shell".cyan().bold());
    println!("Database: {}", db_path.display().to_string().yellow());
    println!(
        "Type {} for help, {} to exit\n",
        ".help".green(),
        ".quit".red()
    );

    let mut input = String::new();
    let stdin = io::stdin();

    loop {
        print!("{} ", "cqlite>".blue().bold());
        io::stdout().flush()?;

        input.clear();
        match stdin.read_line(&mut input) {
            Ok(0) => break, // EOF
            Ok(_) => {
                let trimmed = input.trim();

                if trimmed.is_empty() {
                    continue;
                }

                match handle_repl_command(trimmed, db_path, config, &database).await {
                    Ok(should_continue) => {
                        if !should_continue {
                            break;
                        }
                    }
                    Err(e) => {
                        eprintln!("{} {}", "Error:".red().bold(), e);
                    }
                }
            }
            Err(e) => {
                eprintln!("{} {}", "Input error:".red().bold(), e);
                break;
            }
        }
    }

    println!("{}", "Goodbye!".cyan());
    Ok(())
}

async fn handle_repl_command(input: &str, db_path: &Path, _config: &Config, database: &Database) -> Result<bool> {
    match input {
        ".quit" | ".exit" | "\\q" => {
            return Ok(false);
        }
        ".help" | "\\?" => {
            show_help();
        }
        ".tables" => {
            show_tables(db_path).await?;
        }
        cmd if cmd.starts_with(".schema") => {
            let table_name = cmd.strip_prefix(".schema")
                .map(|s| s.trim())
                .filter(|s| !s.is_empty());
            show_schema(db_path, table_name).await?;
        }
        ".info" => {
            show_database_info(db_path).await?;
        }
        ".clear" => {
            // Clear screen
            print!("\\x1B[2J\\x1B[1;1H");
            io::stdout().flush()?;
        }
        _ if input.starts_with('.') => {
            eprintln!("{} Unknown command: {}", "Error:".red().bold(), input);
            println!("Type {} for available commands", ".help".green());
        }
        _ => {
            // Execute as CQL query
            execute_cql_query(input, database).await?;
        }
    }

    Ok(true)
}

fn show_help() {
    println!("{}", "CQLite Interactive Commands:".cyan().bold());
    println!("  {}         Show this help message", ".help".green());
    println!("  {}         Exit the shell", ".quit".green());
    println!("  {}        List all tables", ".tables".green());
    println!("  {}        Show database schema", ".schema [table]".green());
    println!("  {}          Show database information", ".info".green());
    println!("  {}         Clear the screen", ".clear".green());
    println!();
    println!("{}", "CQL Commands:".cyan().bold());
    println!(
        "  {}  Create a new keyspace",
        "CREATE KEYSPACE ...".yellow()
    );
    println!("  {}     Create a new table", "CREATE TABLE ...".yellow());
    println!(
        "  {}       Insert data into table",
        "INSERT INTO ...".yellow()
    );
    println!("  {}       Query data from table", "SELECT ...".yellow());
    println!("  {}       Update existing data", "UPDATE ...".yellow());
    println!("  {}       Delete data from table", "DELETE ...".yellow());
    println!();
    println!(
        "Press {} to auto-complete, {} for command history",
        "Tab".green(),
        "↑/↓".green()
    );
}

async fn show_tables(db_path: &Path) -> Result<()> {
    let core_config = CoreConfig::default();
    match Database::open(db_path, core_config).await {
        Ok(database) => {
            // Query system tables to list user tables
            match database.execute("SELECT table_name FROM system.tables WHERE keyspace_name != 'system'").await {
                Ok(result) => {
                    println!("{}", "Tables:".cyan().bold());
                    if result.rows.is_empty() {
                        println!("  No tables found");
                    } else {
                        for row in &result.rows {
                            if let Some(table_name) = row.get("table_name") {
                                println!("  {}", table_name);
                            }
                        }
                    }
                }
                Err(_) => {
                    // Fallback to mock data if system tables not available
                    println!("{}", "Tables:".cyan().bold());
                    println!("  (Use CREATE TABLE to create tables)");
                }
            }
        }
        Err(e) => {
            eprintln!("{} Failed to connect to database: {}", "Error:".red().bold(), e);
        }
    }
    Ok(())
}

async fn show_schema(db_path: &Path, table_name: Option<&str>) -> Result<()> {
    let core_config = CoreConfig::default();
    match Database::open(db_path, core_config).await {
        Ok(database) => {
            println!("{}", "Database Schema:".cyan().bold());
            println!();
            
            // Query system schema for actual table information
            let table_query = if let Some(table) = table_name {
                format!("SELECT keyspace_name, table_name FROM system.tables WHERE keyspace_name != 'system' AND table_name = '{}'", table)
            } else {
                "SELECT keyspace_name, table_name FROM system.tables WHERE keyspace_name != 'system'".to_string()
            };
            
            match database.execute(&table_query).await {
                Ok(result) => {
                    if result.rows.is_empty() {
                        if let Some(table) = table_name {
                            println!("{}", format!("Table '{}' not found", table).red());
                            println!("Use {} to list all available tables", ".tables".green());
                        } else {
                            println!("No user-defined schemas found. Use CREATE TABLE to define tables.");
                        }
                    } else {
                        let mut current_keyspace = String::new();
                        for row in &result.rows {
                            if let (Some(keyspace), Some(table)) = (row.get("keyspace_name"), row.get("table_name")) {
                                let keyspace_str = format!("{}", keyspace);
                                if keyspace_str != current_keyspace {
                                    println!("{}", format!("Keyspace: {}", keyspace_str).yellow().bold());
                                    current_keyspace = keyspace_str;
                                }
                                println!("  Table: {}", table);
                                
                                // Try to get column information
                                let column_query = format!(
                                    "SELECT column_name, type FROM system.columns WHERE keyspace_name = '{}' AND table_name = '{}'",
                                    keyspace, table
                                );
                                if let Ok(columns) = database.execute(&column_query).await {
                                    for col_row in &columns.rows {
                                        if let (Some(col_name), Some(col_type)) = (col_row.get("column_name"), col_row.get("type")) {
                                            println!("    {} {}", col_name, col_type);
                                        }
                                    }
                                }
                                println!();
                            }
                        }
                    }
                }
                Err(_) => {
                    println!("Schema information not available. Use DESCRIBE <table> for table details.");
                }
            }
        }
        Err(e) => {
            eprintln!("{} Failed to connect to database: {}", "Error:".red().bold(), e);
        }
    }
    Ok(())
}

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
        if let Ok(stats) = database.stats().await {
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
                (Ok(platform1), Ok(platform2)) => {
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
async fn get_table_list(schema_manager: &Arc<SchemaManager>) -> Result<Vec<TableSchema>> {
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
