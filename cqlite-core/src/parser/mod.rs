//! Parser module for SSTable binary format parsing
//!
//! This module provides comprehensive parsing capabilities for Cassandra 5+ SSTable
//! format ('oa' format). It includes:
//!
//! - Variable-length integer encoding/decoding (VInt)
//! - SSTable header parsing with metadata
//! - CQL type system parsing and serialization
//! - Error handling for parser operations

pub mod header;
pub mod types;
pub mod vint;

pub use header::*;
pub use types::*;
pub use vint::*;

use crate::error::{Error, Result};
use nom::{error::ParseError, IResult};

/// Result type for parser operations using nom
pub type ParseResult<'a, T> = IResult<&'a [u8], T>;

/// Common parser error types specific to CQLite
#[derive(Debug, Clone, PartialEq)]
pub enum CQLiteParseError {
    /// Invalid magic number in SSTable header
    InvalidMagic { expected: u32, found: u32 },
    /// Unsupported format version
    UnsupportedVersion(String),
    /// Invalid length field
    InvalidLength { expected: usize, found: usize },
    /// Data corruption detected
    Corruption(String),
    /// Unexpected end of input
    EndOfInput,
}

impl<I> ParseError<I> for CQLiteParseError {
    fn from_error_kind(_input: I, _kind: nom::error::ErrorKind) -> Self {
        CQLiteParseError::EndOfInput
    }

    fn append(_input: I, _kind: nom::error::ErrorKind, other: Self) -> Self {
        other
    }
}

impl From<CQLiteParseError> for Error {
    fn from(err: CQLiteParseError) -> Self {
        match err {
            CQLiteParseError::InvalidMagic { expected, found } => Error::corruption(format!(
                "Invalid magic number: expected 0x{:08X}, found 0x{:08X}",
                expected, found
            )),
            CQLiteParseError::UnsupportedVersion(version) => {
                Error::corruption(format!("Unsupported format version: {}", version))
            }
            CQLiteParseError::InvalidLength { expected, found } => Error::corruption(format!(
                "Invalid length: expected {}, found {}",
                expected, found
            )),
            CQLiteParseError::Corruption(msg) => Error::corruption(msg),
            CQLiteParseError::EndOfInput => {
                Error::corruption("Unexpected end of input during parsing")
            }
        }
    }
}

/// Main parser context for SSTable parsing
#[derive(Debug)]
pub struct SSTableParser {
    /// Whether to validate checksums
    pub validate_checksums: bool,
    /// Maximum allowed VInt size for safety
    pub max_vint_size: usize,
    /// Whether to allow unknown type IDs
    pub allow_unknown_types: bool,
}

impl Default for SSTableParser {
    fn default() -> Self {
        Self {
            validate_checksums: true,
            max_vint_size: vint::MAX_VINT_SIZE,
            allow_unknown_types: false,
        }
    }
}

impl SSTableParser {
    /// Create a new parser with default settings
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a parser with custom settings
    pub fn with_options(validate_checksums: bool, allow_unknown_types: bool) -> Self {
        Self {
            validate_checksums,
            max_vint_size: vint::MAX_VINT_SIZE,
            allow_unknown_types,
        }
    }

    /// Parse a complete SSTable header
    pub fn parse_header(&self, input: &[u8]) -> Result<(SSTableHeader, usize)> {
        match header::parse_sstable_header(input) {
            Ok((remaining, header)) => {
                let parsed_bytes = input.len() - remaining.len();
                Ok((header, parsed_bytes))
            }
            Err(nom::Err::Error(e) | nom::Err::Failure(e)) => Err(Error::corruption(format!(
                "Failed to parse SSTable header: {:?}",
                e
            ))),
            Err(nom::Err::Incomplete(_)) => {
                Err(Error::corruption("Incomplete SSTable header data"))
            }
        }
    }

    /// Parse a CQL value with the given type
    pub fn parse_value(&self, input: &[u8], type_id: CqlTypeId) -> Result<(crate::Value, usize)> {
        match types::parse_cql_value(input, type_id) {
            Ok((remaining, value)) => {
                let parsed_bytes = input.len() - remaining.len();
                Ok((value, parsed_bytes))
            }
            Err(nom::Err::Error(e) | nom::Err::Failure(e)) => Err(Error::corruption(format!(
                "Failed to parse CQL value: {:?}",
                e
            ))),
            Err(nom::Err::Incomplete(_)) => Err(Error::corruption("Incomplete CQL value data")),
        }
    }

    /// Serialize an SSTable header to bytes
    pub fn serialize_header(&self, header: &SSTableHeader) -> Result<Vec<u8>> {
        header::serialize_sstable_header(header)
    }

    /// Serialize a CQL value to bytes
    pub fn serialize_value(&self, value: &crate::Value) -> Result<Vec<u8>> {
        types::serialize_cql_value(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;

    #[test]
    fn test_parser_creation() {
        let parser = SSTableParser::new();
        assert!(parser.validate_checksums);
        assert!(!parser.allow_unknown_types);

        let parser = SSTableParser::with_options(false, true);
        assert!(!parser.validate_checksums);
        assert!(parser.allow_unknown_types);
    }

    #[test]
    fn test_error_conversion() {
        let parse_error = CQLiteParseError::InvalidMagic {
            expected: 0x1234_5678,
            found: 0x8765_4321,
        };
        let error: Error = parse_error.into();
        assert!(matches!(error, Error::Corruption(_)));
    }

    #[test]
    fn test_value_serialization_roundtrip() {
        let parser = SSTableParser::new();
        let value = Value::Text("test".to_string());

        let serialized = parser.serialize_value(&value).unwrap();
        assert!(!serialized.is_empty());

        // The first byte should be the type ID
        assert_eq!(serialized[0], CqlTypeId::Varchar as u8);
    }
}
