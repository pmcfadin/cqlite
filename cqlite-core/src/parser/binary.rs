//! Binary format parsing for backward compatibility
//!
//! This module provides parsing support for existing binary formats
//! used in SSTable parsing, maintaining backward compatibility while
//! transitioning to the new parser abstraction layer.

use crate::error::{Error, Result};
use super::traits::{CqlParser, ParserBackendInfo, ParserFeature, PerformanceCharacteristics};
use super::ast::*;
use async_trait::async_trait;

/// Binary format parser for SSTable compatibility
#[derive(Debug)]
pub struct SSTableParser {
    config: super::config::ParserConfig,
}

impl SSTableParser {
    /// Create a new binary parser
    pub fn new(config: super::config::ParserConfig) -> Result<Self> {
        Ok(Self { config })
    }
    
    /// Get backend information
    pub fn backend_info() -> ParserBackendInfo {
        ParserBackendInfo {
            name: "binary".to_string(),
            version: "1.0.0".to_string(),
            features: vec![ParserFeature::Streaming],
            performance: PerformanceCharacteristics {
                statements_per_second: 50_000,
                memory_per_statement: 512,
                startup_time_ms: 1,
                async_support: true,
            },
        }
    }
}

#[async_trait]
impl CqlParser for SSTableParser {
    async fn parse(&self, _input: &str) -> Result<CqlStatement> {
        Err(Error::invalid_operation("Binary parser does not support text parsing".to_string()))
    }
    
    async fn parse_type(&self, _input: &str) -> Result<CqlDataType> {
        Err(Error::invalid_operation("Binary parser does not support text parsing".to_string()))
    }
    
    async fn parse_expression(&self, _input: &str) -> Result<CqlExpression> {
        Err(Error::invalid_operation("Binary parser does not support text parsing".to_string()))
    }
    
    async fn parse_identifier(&self, _input: &str) -> Result<CqlIdentifier> {
        Err(Error::invalid_operation("Binary parser does not support text parsing".to_string()))
    }
    
    async fn parse_literal(&self, _input: &str) -> Result<CqlLiteral> {
        Err(Error::invalid_operation("Binary parser does not support text parsing".to_string()))
    }
    
    async fn parse_column_definitions(&self, _input: &str) -> Result<Vec<CqlColumnDef>> {
        Err(Error::invalid_operation("Binary parser does not support text parsing".to_string()))
    }
    
    async fn parse_table_options(&self, _input: &str) -> Result<CqlTableOptions> {
        Err(Error::invalid_operation("Binary parser does not support text parsing".to_string()))
    }
    
    fn validate_syntax(&self, _input: &str) -> bool {
        false // Binary parser doesn't validate text syntax
    }
    
    fn backend_info(&self) -> ParserBackendInfo {
        Self::backend_info()
    }
}

/// Legacy error type for backward compatibility
#[derive(Debug, thiserror::Error)]
pub enum CQLiteParseError {
    #[error("Parse error: {0}")]
    ParseError(String),
    
    #[error("Invalid format: {0}")]
    InvalidFormat(String),
    
    #[error("Unsupported operation: {0}")]
    Unsupported(String),
}

impl From<Error> for CQLiteParseError {
    fn from(err: Error) -> Self {
        CQLiteParseError::ParseError(err.to_string())
    }
}

/// Legacy result type for backward compatibility
pub type ParseResult<T> = std::result::Result<T, CQLiteParseError>;

/// Parse binary data (placeholder for backward compatibility)
pub fn parse_binary_data(data: &[u8]) -> ParseResult<Vec<u8>> {
    if data.is_empty() {
        return Err(CQLiteParseError::InvalidFormat("Empty input".to_string()));
    }
    
    // For now, just return the input data
    // In a real implementation, this would parse the binary format
    Ok(data.to_vec())
}

/// Parse variable-length integer from binary data
pub fn parse_vint_binary(data: &[u8]) -> ParseResult<(u64, usize)> {
    if data.is_empty() {
        return Err(CQLiteParseError::InvalidFormat("Empty vint data".to_string()));
    }
    
    // Simple vint parsing - first byte indicates length
    let first_byte = data[0];
    if first_byte & 0x80 == 0 {
        // Single byte
        Ok((first_byte as u64, 1))
    } else {
        // Multi-byte - not fully implemented
        Err(CQLiteParseError::Unsupported("Multi-byte vint not implemented".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_binary_parser_creation() {
        let config = super::super::config::ParserConfig::default();
        let parser = SSTableParser::new(config).unwrap();
        
        let info = parser.backend_info();
        assert_eq!(info.name, "binary");
    }
    
    #[tokio::test]
    async fn test_binary_parser_unsupported() {
        let config = super::super::config::ParserConfig::default();
        let parser = SSTableParser::new(config).unwrap();
        
        let result = parser.parse("SELECT * FROM test").await;
        assert!(result.is_err());
    }
    
    #[test]
    fn test_parse_binary_data() {
        let data = b"test data";
        let result = parse_binary_data(data).unwrap();
        assert_eq!(result, data);
        
        let empty_result = parse_binary_data(&[]);
        assert!(empty_result.is_err());
    }
    
    #[test]
    fn test_parse_vint_binary() {
        let data = &[0x42]; // Single byte vint
        let (value, consumed) = parse_vint_binary(data).unwrap();
        assert_eq!(value, 0x42);
        assert_eq!(consumed, 1);
        
        let empty_result = parse_vint_binary(&[]);
        assert!(empty_result.is_err());
    }
}