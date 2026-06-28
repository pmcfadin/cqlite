//! Presentation layer for the interactive REPL.
//!
//! Banners, prompt formatting, the `:help` system, query-result table
//! rendering (including paging), error hinting, and schema/DDL display.
//! All functions here are pure output helpers with no command dispatch.

use super::ReplSession;
use anyhow::Result;
use colored::Colorize;
use cqlite_core::{query::result::QueryRow, schema::TableSchema, QueryResult};
use std::io::{self, Write};
use std::time::Duration;

// ========== DISPLAY AND UI FUNCTIONS ==========

pub(super) async fn display_startup_banner(session: &ReplSession) -> Result<()> {
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

pub(super) fn format_prompt(session: &ReplSession) -> String {
    let keyspace_part = if let Some(ref ks) = session.current_keyspace {
        format!("{}@", ks.cyan())
    } else {
        "".to_string()
    };

    format!("{}cqlite{}", keyspace_part, ">".blue().bold())
}

pub(super) fn display_error(error: &anyhow::Error, input: &str) {
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

pub(super) fn display_goodbye_message() {
    println!();
    println!("{}", "╔═══════════════════════════════════════════════╗".cyan());
    println!("{}", "║                   Goodbye!                   ║".cyan().bold());
    println!("{}", "║         Thank you for using CQLite           ║".cyan());
    println!("{}", "╚═══════════════════════════════════════════════╝".cyan());
}

// ========== HELP SYSTEM ==========

pub(super) fn show_enhanced_help() {
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

pub(super) fn show_help_topic(topic: &str) {
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

// ========== SCHEMA / DDL DISPLAY ==========

pub(super) fn generate_create_table_statement(keyspace: &str, table: &str, rows: &[QueryRow]) {
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

pub(super) fn format_primary_key(table_schema: &TableSchema) -> String {
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

pub(super) fn display_table_schema(table_schema: &TableSchema) {
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

// ========== QUERY RESULT RENDERING ==========

/// Display query results with enhanced formatting and paging support
pub(super) fn display_enhanced_query_results(result: &QueryResult, session: &ReplSession) -> Result<()> {
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

/// Render the trailing timing/performance/warnings block for an enhanced
/// CQL query result. Extracted verbatim from `execute_enhanced_cql_query`'s
/// success arm: the timing breakdown is gated on `timing_enabled`, while
/// warnings are always displayed.
pub(super) fn execute_enhanced_cql_query_render(
    result: &QueryResult,
    execution_time: Option<Duration>,
    timing_enabled: bool,
) {
    // Show timing information if enabled
    if let (Some(elapsed), true) = (execution_time, timing_enabled) {
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

pub(super) fn provide_cql_error_hints(error: &anyhow::Error, query: &str, session: &ReplSession) {
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

/// Display query results in a formatted table (legacy renderer).
pub(super) fn display_query_results(result: &QueryResult) -> Result<()> {
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
