//! Interactive REPL configuration commands (`:config`).
//!
//! Handles displaying the current session configuration and applying
//! `:config <setting> <value>` mutations to the [`ReplSession`].

use super::ReplSession;
use anyhow::Result;
use colored::Colorize;
use std::path::PathBuf;

pub(super) fn show_current_config(session: &ReplSession) {
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

pub(super) async fn handle_config_command(config_cmd: &str, session: &mut ReplSession) -> Result<()> {
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
