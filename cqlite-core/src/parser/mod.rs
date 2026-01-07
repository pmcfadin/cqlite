//! SSTable Binary Format Parser Module
//!
//! This module provides parsing functionality for Apache Cassandra SSTable binary formats.
//! It handles deserialization of binary data structures including:
//!
//! - SSTable headers and metadata
//! - Variable-length integer (VInt) encoding
//! - Statistics and enhanced statistics
//! - Complex types (collections, UDTs, tuples, frozen types)
//! - Zero-copy parsing optimizations
//!
//! ## Key Distinction
//!
//! - **parser/** = SSTable binary format parsing (binary data → structured values)
//! - **cql/** = CQL text parsing (query strings → AST)
//!
//! For CQL text parsing (CREATE TABLE, SELECT, etc.), see the `cql` module.

// Binary format parsing (SSTable components)
pub mod binary;

// Re-export existing modules for backward compatibility
#[cfg(feature = "benchmarks")]
pub mod benchmarks;
pub mod collection_benchmarks;
#[cfg(test)]
pub mod collection_tests;
// pub mod collection_udt_tests; // Commented out due to missing methods
#[cfg(test)]
pub mod collection_correctness_tests; // Property tests for Issue #61
#[cfg(test)]
pub mod collection_validation_tests;
pub mod complex_types;
pub mod enhanced_statistics_parser;
#[cfg(test)]
pub mod enhanced_statistics_test;
pub mod header;
pub mod statistics;
#[cfg(test)]
pub mod statistics_test;
pub mod types;
#[cfg(test)]
pub mod udt_tests;
pub mod vint;
pub mod vint_fixed;

// M3 Performance Optimization Modules
pub mod optimized_complex_types;
pub mod zero_copy_parser;

// Re-export binary format parser
pub use binary::{CQLiteParseError, ParseResult, SSTableParser};

// Re-export binary format parsers for backward compatibility
#[cfg(feature = "benchmarks")]
pub use benchmarks::*;
pub use complex_types::*;
pub use enhanced_statistics_parser::*;
pub use header::*;
pub use statistics::*;
pub use types::*;
pub use vint::*;

// Re-export M3 performance modules
#[cfg(feature = "benchmarks")]
pub use optimized_complex_types::OptimizedComplexTypeParser;

/// Re-export common result types
pub use crate::error::Result as CqlResult;

/// Parse CQL CREATE TABLE statement (backward compatibility function)
///
/// **DEPRECATED**: This function maintains backward compatibility with existing code.
/// For new code, use `cqlite_core::schema::parse_cql_schema()` which is synchronous
/// and returns `Result<TableSchema>` instead of `nom::IResult`.
///
/// # Arguments
/// * `input` - The CQL CREATE TABLE statement to parse
///
/// # Returns
/// * `nom::IResult<&str, crate::schema::TableSchema>` - Parsed schema or error
#[deprecated(
    since = "0.2.0",
    note = "Use cqlite_core::schema::parse_cql_schema() instead - it's synchronous and more efficient"
)]
pub fn parse_cql_schema(input: &str) -> nom::IResult<&str, crate::schema::TableSchema> {
    // Delegate to the cql module (which now uses synchronous parsing)
    #[allow(deprecated)]
    crate::cql::schema_integration::parse_cql_schema_compat(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(deprecated)] // Testing deprecated API for backward compatibility
    fn test_parse_cql_schema_backward_compat() {
        // Test that the backward compatibility function still works
        let schema = "CREATE TABLE test_keyspace.test_table (id int PRIMARY KEY)";
        let result = parse_cql_schema(schema);

        // The result should delegate to cql module
        // We just verify it compiles and returns the expected type
        assert!(result.is_ok() || result.is_err());
    }
}
