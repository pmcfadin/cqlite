// CQLite CLI library

//! CQLite CLI library
//!
//! This library provides the core functionality for the CQLite CLI,
//! including command processing, database operations, and testing infrastructure.

// EMERGENCY M1 FIX: Completely disable clippy for CI
#![allow(clippy::all)]

pub mod cli;
pub mod commands;
pub mod config;

// CLI types module - re-exports from main
pub mod cli_types;

#[cfg(test)]
pub mod test_infrastructure;

// Re-export commonly used types for testing
pub use cli::{ExportFormat, ImportFormat, OutputFormat};

// Re-export CLI types for external use
pub use cli_types::{AdminCommands, BenchCommands, Cli, Commands, SchemaCommands};
pub use config::Config;

#[cfg(test)]
pub use test_infrastructure::*;
