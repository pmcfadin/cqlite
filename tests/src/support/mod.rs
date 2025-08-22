//! Shared test support utilities
//!
//! This module provides common testing utilities used across the test suite,
//! including assertion helpers and test data generation functions.

pub mod assert;

// Re-export commonly used functions for convenience
pub use assert::{approx_eq, assert_fully_consumed};
