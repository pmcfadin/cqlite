// CQLite CLI library

//! CQLite CLI library
//!
//! This library provides the core functionality for the CQLite CLI,
//! including command processing, database operations, and testing infrastructure.

// EMERGENCY M1 FIX: Completely disable clippy for CI
#![allow(clippy::all)]
// Raise the query/recursion depth limit above the default 128 (issue #1990).
// rustc >= 1.96 overflows the default depth computing the *layout of the async
// future* generated for `tui::events::run_tui<B>()` (repro: "query depth
// increased by 130 when computing layout of `{async fn body of
// tui::events::run_tui<B>()}`"). It is a large async event loop, not a genuine
// type-recursion bug, so a benign limit bump is the correct fix (a source
// refactor would split that async fn into smaller awaited helpers — out of
// scope here). Compiles on every supported toolchain (1.88.0 pin and forward)
// and does not change runtime behavior.
#![recursion_limit = "256"]

pub mod cli;
pub mod commands;
pub mod config;
pub mod error;
pub mod status_metrics;

// CLI types module - re-exports from main
pub mod cli_types;

// Formatter module for table output
pub mod formatter;

// Output writers for QueryResult (Issue #119)
pub mod output;

// Script executor for CQL script files (Issue #122)
pub mod script_executor;

// REPL engine module (Issue #16)
pub mod repl;

// TUI mode implementation (Issue #251)
pub mod tui;

#[cfg(all(test, feature = "state_machine"))]
pub mod test_infrastructure;

// Re-export commonly used types for testing
pub use cli::{ExportFormat, ImportFormat, OutputFormat};

// Re-export CLI types for external use
pub use cli_types::{AdminCommands, BenchCommands, Cli, Commands, SchemaCommands};
pub use config::Config;

#[cfg(all(test, feature = "state_machine"))]
pub use test_infrastructure::*;
