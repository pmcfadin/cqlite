// Core REPL Engine Module
//
// This module provides the core REPL (Read-Eval-Print Loop) engine for CQLite.
// It handles command parsing, routing, execution, and provides a clean interface
// for both the basic CLI and enhanced TUI modes.

// Allow dead code during REPL development as many features are not yet fully utilized
#![allow(dead_code)]

pub mod command_parser;
pub mod commands;
pub mod completion;
pub mod engine;
pub mod history;
pub mod session;

// Re-export main interfaces
pub use command_parser::{CommandParser, CommandType, ParsedCommand};
pub use completion::CompletionEngine;
pub use engine::{ReplConfig, ReplEngine};
pub use history::HistoryManager;
pub use session::{ReplSession, SessionState};

/// Core REPL result type
pub type ReplResult<T> = Result<T, ReplError>;

/// REPL-specific error types
#[derive(Debug, thiserror::Error)]
pub enum ReplError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Database error: {0}")]
    Database(#[from] anyhow::Error),

    #[error("Command parsing error: {0}")]
    CommandParsing(String),

    #[error("Invalid configuration: {0}")]
    Config(String),

    #[error("Session error: {0}")]
    Session(String),

    #[error("History error: {0}")]
    History(String),

    #[error("Completion error: {0}")]
    Completion(String),
}

/// Execute result indicating whether REPL should continue
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionResult {
    /// Continue REPL execution
    Continue,
    /// Exit REPL gracefully
    Exit,
    /// Exit with specific code
    ExitWithCode(i32),
}

/// REPL mode configuration
#[derive(Debug, Clone, PartialEq)]
pub enum ReplMode {
    /// Basic command-line REPL
    Basic,
    /// Enhanced TUI mode
    Tui,
    /// Interactive mode with advanced features
    Interactive,
}

/// Output format for query results
#[derive(Debug, Clone, PartialEq)]
pub enum OutputFormat {
    /// Table format with borders
    Table,
    /// CSV format
    Csv,
    /// JSON format
    Json,
    /// Raw format (minimal)
    Raw,
}

/// REPL command line completion context
#[derive(Debug, Clone)]
pub struct CompletionContext {
    /// Current input line
    pub line: String,
    /// Cursor position
    pub pos: usize,
    /// Current session state
    pub session_state: SessionState,
    /// Available tables
    pub tables: Vec<String>,
    /// Available keyspaces
    pub keyspaces: Vec<String>,
}
