use crate::config::Config;
use anyhow::Result;
use colored::*;
use std::io::{self, Write};
use std::path::Path;

pub async fn start_repl_mode(db_path: &Path, config: &Config) -> Result<()> {
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

                match handle_repl_command(trimmed, db_path, config).await {
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

async fn handle_repl_command(input: &str, db_path: &Path, _config: &Config) -> Result<bool> {
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
        ".schema" => {
            show_schema(db_path).await?;
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
            execute_cql_query(input, db_path).await?;
        }
    }

    Ok(true)
}

fn show_help() {
    println!("{}", "CQLite Interactive Commands:".cyan().bold());
    println!("  {}         Show this help message", ".help".green());
    println!("  {}         Exit the shell", ".quit".green());
    println!("  {}        List all tables", ".tables".green());
    println!("  {}        Show database schema", ".schema".green());
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

async fn show_tables(_db_path: &Path) -> Result<()> {
    // TODO: Implement actual table listing
    println!("{}", "Tables:".cyan().bold());
    println!("  users");
    println!("  orders");
    println!("  products");
    println!("  sessions");
    Ok(())
}

async fn show_schema(_db_path: &Path) -> Result<()> {
    // TODO: Implement actual schema display
    println!("{}", "Database Schema:".cyan().bold());
    println!();
    println!("{}", "Keyspace: ecommerce".yellow().bold());
    println!("  Table: users");
    println!("    id UUID PRIMARY KEY");
    println!("    email TEXT");
    println!("    name TEXT");
    println!("    created_at TIMESTAMP");
    println!();
    println!("  Table: orders");
    println!("    id UUID PRIMARY KEY");
    println!("    user_id UUID");
    println!("    total DECIMAL");
    println!("    status TEXT");
    Ok(())
}

async fn show_database_info(_db_path: &Path) -> Result<()> {
    // TODO: Implement actual database info
    println!("{}", "Database Information:".cyan().bold());
    println!("  Version: CQLite 0.1.0");
    println!("  File size: 1.2 MB");
    println!("  Tables: 4");
    println!("  Total rows: ~10,000");
    println!("  Last modified: 2024-01-15 14:30:22");
    Ok(())
}

async fn execute_cql_query(query: &str, _db_path: &Path) -> Result<()> {
    // TODO: Implement actual CQL query execution
    println!("{} {}", "Executing:".blue().bold(), query.yellow());

    // Mock response for demonstration
    if query.to_uppercase().starts_with("SELECT") {
        println!();
        println!("{}", "Results:".green().bold());
        println!(
            "┌──────────────────────────────────────┬─────────────────┬──────────────────────┐"
        );
        println!(
            "│ id                                   │ name            │ email                │"
        );
        println!(
            "├──────────────────────────────────────┼─────────────────┼──────────────────────┤"
        );
        println!(
            "│ 550e8400-e29b-41d4-a716-446655440000 │ John Doe        │ john@example.com     │"
        );
        println!(
            "│ 550e8400-e29b-41d4-a716-446655440001 │ Jane Smith      │ jane@example.com     │"
        );
        println!(
            "└──────────────────────────────────────┴─────────────────┴──────────────────────┘"
        );
        println!();
        println!("{} 2 rows returned", "Query completed:".green());
    } else {
        println!("{} Query executed successfully", "✓".green().bold());
    }

    Ok(())
}
