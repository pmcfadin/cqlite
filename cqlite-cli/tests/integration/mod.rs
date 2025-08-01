//! Integration test directory for CQLite CLI
//!
//! This directory contains integration tests organized by functionality.

pub mod cli_basic_tests;
pub mod cli_command_tests;
pub mod database_operation_tests;
pub mod performance_tests;
pub mod error_handling_tests;

// Re-export common test utilities for use in integration tests
pub use cqlite_cli::test_infrastructure::*;