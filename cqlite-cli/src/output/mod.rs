//! Output formatting for QueryResult
//!
//! This module provides writers that adapt QueryResult to various output formats
//! (table, JSON, CSV) with stable, cqlsh-compatible formatting.
//!
//! ## Contract
//!
//! All writers follow the QUERY_RESULT_CONTRACT.md specification:
//! - Column order determined by `metadata.columns`
//! - Null values handled consistently
//! - Format-specific conventions (e.g., row count footer for tables)

#[cfg(feature = "state_machine")]
pub mod csv;
#[cfg(feature = "state_machine")]
pub mod json;
#[cfg(feature = "state_machine")]
pub mod table;
pub mod value_fmt;

#[cfg(feature = "state_machine")]
pub use csv::CSVWriter;
#[cfg(feature = "state_machine")]
pub use json::JSONWriter;
#[cfg(feature = "state_machine")]
pub use table::TableWriter;
pub use value_fmt::ValueFormatter;
