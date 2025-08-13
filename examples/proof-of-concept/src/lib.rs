//! CQLite Proof-of-Concept Library
//!
//! This library contains utilities and helpers for the CQLite proof-of-concept demonstration.

pub mod test_data;
pub mod validation;
pub mod performance;

// Re-export main functionality
pub use test_data::*;
pub use validation::*;
pub use performance::*;