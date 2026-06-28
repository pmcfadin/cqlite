//! Command parsing and dispatch for the interactive REPL.
//!
//! This module holds the handlers for data-exploration meta-commands
//! (`:tables`, `:keyspaces`, `:info`, `:describe`, `:schema`, `:use`,
//! `:history`, `:source`), CQL query execution, and the database/schema
//! introspection helpers they rely on. Pure presentation lives in [`super::ui`].

use super::config::{handle_config_command, show_current_config};
use super::data_dir::{
    display_table_list, find_table_in_directory, scan_data_directory, scan_keyspaces_from_directory,
};
use super::ui::{
    display_enhanced_query_results, display_query_results, display_table_schema,
    execute_enhanced_cql_query_render, generate_create_table_statement, provide_cql_error_hints,
};
use super::{ColumnInfo, ReplSession};
use anyhow::Result;
use colored::Colorize;
use cqlite_core::{
    platform::Platform,
    schema::{SchemaManager, TableSchema},
    storage::StorageEngine,
    Config as CoreConfig, Database,
};
use std::collections::HashMap;
use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

// ========== DATA EXPLORATION FUNCTIONS ==========

pub(super) async fn show_enhanced_tables(session: &ReplSession) -> Result<()> {
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

pub(super) async fn show_keyspaces(session: &ReplSession) -> Result<()> {
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

pub(super) async fn show_object_info(object_name: &str, session: &ReplSession) -> Result<()> {
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

pub(super) async fn describe_table(table_name: &str, session: &ReplSession) -> Result<()> {
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

pub(super) async fn show_enhanced_schema(table_name: Option<&str>, session: &ReplSession) -> Result<()> {
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

pub(super) fn show_command_history(session: &ReplSession) {
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

pub(super) async fn use_keyspace(keyspace: &str, session: &mut ReplSession) -> Result<()> {
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

pub(super) async fn source_file(file_path: &str, session: &mut ReplSession) -> Result<()> {
    use std::fs;

    println!("{} Executing commands from: {}", "📂".cyan(), file_path.yellow());

    let path = Path::new(file_path);
    if !path.exists() {
        return Err(anyhow::anyhow!("File not found: {}", file_path));
    }

    let content = fs::read_to_string(path)
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

pub(super) async fn execute_enhanced_cql_query(query: &str, session: &ReplSession) -> Result<()> {
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
            execute_enhanced_cql_query_render(&result, execution_time, session.timing_enabled);
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

// ========== LEGACY DATABASE INFO / EXECUTION ==========

pub(super) async fn show_database_info(db_path: &Path) -> Result<()> {
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

pub(super) async fn execute_cql_query(query: &str, database: &Database) -> Result<()> {
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

// ========== DATABASE / SCHEMA INTEGRATION ==========

// Helper structures for database integration
pub(super) struct DatabaseInfo {
    pub database: Database,
    pub schema: Arc<SchemaManager>,
}

// Get database instance with proper initialization
pub(super) async fn get_database_instance(db_path: &Path) -> Result<DatabaseInfo> {
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

// Show schema for all tables
pub(super) async fn show_all_schemas(schema_manager: &Arc<SchemaManager>) -> Result<()> {
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
pub(super) async fn show_table_schema(schema_manager: &Arc<SchemaManager>, table_name: &str) -> Result<()> {
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
