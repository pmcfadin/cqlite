//! Interactive REPL for CQLite.
//!
//! This module orchestrates the interactive shell: it owns the shared session
//! model ([`ReplSession`]) and the top-level entry points ([`start_repl_mode`],
//! [`handle_repl_command`]), and delegates to per-concern submodules:
//!
//! - [`ui`] — banners, prompt, `:help`, and query-result/schema rendering.
//! - [`commands`] — command dispatch, CQL execution, schema introspection.
//! - [`config`] — `:config` display and mutation.
//! - [`data_dir`] — Cassandra data-directory scanning.
//!
//! The public surface ([`ReplSession`], [`start_repl_mode`]) is re-exported
//! unchanged so external callers see no difference from the original
//! `interactive.rs`.

mod commands;
mod config;
mod data_dir;
mod ui;

use crate::config::Config;
use anyhow::Result;
use colored::Colorize;
use cqlite_core::Database;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

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

/// Column metadata used when describing a table's schema.
#[derive(Debug)]
pub(crate) struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub kind: String,
}

/// Summary of a table located by scanning a data directory.
#[derive(Debug)]
pub(crate) struct TableInfo {
    pub path: String,
    pub sstable_count: usize,
    pub total_size_mb: f64,
}

pub async fn start_repl_mode(db_path: &Path, config: &Config, database: Database) -> Result<()> {
    // For now, use a simplified integration approach due to compilation issues
    // This will be replaced with the full REPL engine once core issues are resolved

    println!("{}", "╔═══════════════════════════════════════════════╗".cyan());
    println!("{}", "║           CQLite REPL Engine v2.0            ║".cyan().bold());
    println!("{}", "║      High-Performance Cassandra Reader       ║".cyan());
    println!("{}", "╚═══════════════════════════════════════════════╝".cyan());
    println!();
    println!("🗄️  Database: {}", db_path.display().to_string().yellow());
    println!("🔧 Mode: Interactive");
    println!("📊 Engine: CQLite Core v0.1.0");
    println!();
    println!("{}", "Quick Commands:".cyan().bold());
    println!("  • {} - Show help", ":help".green());
    println!("  • {} - List tables", ":tables".green());
    println!("  • {} - Execute CQL", "SELECT * FROM table;".yellow());
    println!("  • {} - Exit", ":quit".red());
    println!();

    // Fall back to original implementation for now
    let mut session = ReplSession::new(db_path, config.clone(), database);

    let mut input = String::new();
    let stdin = io::stdin();

    loop {
        let prompt = ui::format_prompt(&session);
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
                        ui::display_error(&e, trimmed);
                    }
                }
            }
            Err(e) => {
                eprintln!("{} {}", "Input error:".red().bold(), e);
                break;
            }
        }
    }

    ui::display_goodbye_message();
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
            ui::show_enhanced_help();
        }
        cmd if cmd.starts_with(":help ") => {
            let topic = cmd.strip_prefix(":help ").unwrap_or("").trim();
            ui::show_help_topic(topic);
        }

        // Configuration commands
        ":config" => {
            config::show_current_config(session);
        }
        cmd if cmd.starts_with(":config ") => {
            let config_cmd = cmd.strip_prefix(":config ").unwrap_or("").trim();
            config::handle_config_command(config_cmd, session).await?;
        }

        // Data exploration commands
        ":tables" | ":list" => {
            commands::show_enhanced_tables(session).await?;
        }
        ":keyspaces" => {
            commands::show_keyspaces(session).await?;
        }
        cmd if cmd.starts_with(":info ") => {
            let object_name = cmd.strip_prefix(":info ").unwrap_or("").trim();
            commands::show_object_info(object_name, session).await?;
        }
        cmd if cmd.starts_with(":describe ") || cmd.starts_with(":desc ") => {
            let prefix = if cmd.starts_with(":describe ") { ":describe " } else { ":desc " };
            let table_name = cmd.strip_prefix(prefix).unwrap_or("").trim();
            commands::describe_table(table_name, session).await?;
        }

        // Schema commands
        cmd if cmd.starts_with(":schema") => {
            let table_name = cmd.strip_prefix(":schema")
                .map(|s| s.trim())
                .filter(|s| !s.is_empty());
            commands::show_enhanced_schema(table_name, session).await?;
        }

        // Utility commands
        ":clear" | ":cls" => {
            print!("\\x1B[2J\\x1B[1;1H");
            io::stdout().flush()?;
        }
        ":history" => {
            commands::show_command_history(session);
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
            commands::use_keyspace(keyspace, session).await?;
        }
        cmd if cmd.starts_with(":source ") => {
            let path = cmd.strip_prefix(":source ").unwrap_or("").trim();
            commands::source_file(path, session).await?;
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
            commands::execute_enhanced_cql_query(input, session).await?;
        }
    }

    Ok(true)
}
