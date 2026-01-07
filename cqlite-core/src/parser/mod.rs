//! # SSTable Binary Format Parser Module
//!
//! This module provides parsing functionality for Apache Cassandra SSTable binary formats.
//! It handles deserialization of binary data structures from SSTable files (Data.db,
//! Index.db, Statistics.db, etc.) produced by Cassandra 5.0+.
//!
//! ## Module Architecture
//!
//! ```text
//! parser/ (SSTable Binary Format Parsing)
//! │
//! ├── Core Binary Parsing
//! │   ├── vint.rs              - Variable-length integer (VInt) encoding
//! │   ├── vint_fixed.rs        - Fixed-size integer alternatives
//! │   └── header.rs            - SSTable header parsing (magic numbers, version detection)
//! │
//! ├── Statistics Parsing
//! │   ├── statistics.rs        - Statistics.db basic format
//! │   └── enhanced_statistics_parser.rs - Statistics.db enhanced format (nb/oa)
//! │
//! ├── CQL Type Deserialization
//! │   ├── types.rs             - All CQL primitive types (int, text, uuid, etc.)
//! │   ├── complex_types.rs     - Collections, UDTs, tuples, frozen types
//! │   └── optimized_complex_types.rs - M3 performance-optimized parsing
//! │
//! ├── Performance Utilities
//! │   └── zero_copy_parser.rs  - String interning, zero-copy buffers
//! │
//! └── High-Level Interface
//!     └── binary.rs            - SSTableParser facade
//! ```
//!
//! Note: Test modules (`*_test.rs`, `*_tests.rs`) and benchmarks (`*_benchmarks.rs`)
//! are omitted from the diagram. See feature flag `benchmarks` for performance testing.
//!
//! ## Key Distinction: parser/ vs cql/
//!
//! | Module | Purpose | Input | Output |
//! |--------|---------|-------|--------|
//! | **parser/** | SSTable binary parsing | Raw bytes from .db files | Structured Rust values |
//! | **cql/** | CQL text parsing | Query strings ("SELECT...") | Abstract Syntax Trees |
//!
//! This module (`parser/`) handles **binary deserialization**:
//! - Reading bytes from SSTable files (Data.db, Statistics.db, etc.)
//! - Decoding VInt-encoded integers per Cassandra's wire format
//! - Deserializing CQL values (int → i32, text → String, uuid → Uuid)
//!
//! For **CQL text parsing** (CREATE TABLE, SELECT, etc.), see the [`crate::cql`] module.
//!
//! ## Sub-module Reference
//!
//! ### Variable-Length Integer Encoding
//! - [`vint`] - VInt encoding/decoding per Cassandra specification, with corruption detection
//! - [`vint_fixed`] - Fixed-size integer parsing for when VInt isn't used
//!
//! ### SSTable Headers
//! - [`header`] - SSTable header parsing with version detection (oa/nb/legacy formats)
//!
//! ### Statistics Files
//! - [`statistics`] - Statistics.db parsing for row counts, timestamps, min/max metadata
//! - [`enhanced_statistics_parser`] - Enhanced Statistics.db format for Cassandra 5.0's
//!   nb (nested btree) and oa (open addressing) formats
//!
//! ### CQL Type Deserialization
//! - [`types`] - All 20+ CQL primitive types: int, bigint, text, blob, uuid, timestamp,
//!   date, time, inet, varint, decimal, duration, boolean, float, double, ascii, timeuuid
//! - [`complex_types`] - Collections (list, set, map), UDTs, tuples, with depth tracking
//!   for nested types
//! - [`optimized_complex_types`] - M3 milestone performance optimizations for complex types
//!
//! ### Performance
//! - [`zero_copy_parser`] - Memory optimization utilities: string interning, zero-copy
//!   buffer management to stay under 128MB memory target
//!
//! ### High-Level Interface
//! - [`binary`] - `SSTableParser` facade providing unified access to parsing functionality
//!
//! ## Usage Examples
//!
//! ```rust,ignore
//! use cqlite_core::parser::{parse_vint, SSTableHeader, CqlType};
//!
//! // Parse variable-length integer from raw bytes
//! let bytes = [0x8A, 0x01]; // VInt-encoded value
//! let (remaining, value) = parse_vint(&bytes)?;
//!
//! // Parse SSTable header to detect format version
//! let header = SSTableHeader::parse(&file_bytes)?;
//! println!("SSTable format: {:?}", header.format_type);
//! ```
//!
//! ## Backward Compatibility
//!
//! The [`parse_cql_schema`] function is maintained for backward compatibility with
//! existing code. **New code should use [`crate::cql::parse_cql_schema_enhanced`]**
//! which provides better error handling and configuration options.
//!
//! ## Related Documentation
//!
//! - SSTable format specification: `docs/sstables-definitive-guide/`
//! - Known limitations: `docs/sstables-definitive-guide/chapters/appendix-f-known-limitations.md`

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
    note = "Use cqlite_core::cql::parse_cql_schema_enhanced() instead for better error handling"
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

        // The result should delegate to cql module and parse successfully
        assert!(
            result.is_ok(),
            "Valid schema should parse successfully via backward-compat function"
        );
    }
}
