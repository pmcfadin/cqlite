// CQLite CLI library

//! CQLite CLI library
//!
//! This library provides the core functionality for the CQLite CLI,
//! including command processing, database operations, and testing infrastructure.

pub mod cli;
pub mod commands;
pub mod config;

#[cfg(test)]
pub mod test_infrastructure;

// Re-export commonly used types for testing
pub use cli::{OutputFormat, ImportFormat, ExportFormat};
pub use config::Config;

#[cfg(test)]
pub use test_infrastructure::*;