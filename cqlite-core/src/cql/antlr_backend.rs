//! ANTLR-based parser implementation
//!
//! This module provides an ANTLR-based implementation of the CqlParser trait,
//! optimized for rich error reporting and advanced language features.

use crate::error::Result;
use async_trait::async_trait;

use super::{
    ast::*,
    config::ParserConfig,
    error::ParserError,
    traits::{CqlParser, ParserBackendInfo, ParserFeature, PerformanceCharacteristics},
};

const NOT_IMPLEMENTED: &str = "ANTLR parser not yet implemented";

fn not_implemented<T>() -> Result<T> {
    Err(ParserError::backend("antlr", NOT_IMPLEMENTED).into())
}

/// ANTLR-based parser implementation
#[derive(Debug, Default)]
pub struct AntlrParser;

impl AntlrParser {
    /// Create a new ANTLR parser with the given configuration
    pub fn new(config: ParserConfig) -> Result<Self> {
        Self::validate_config(&config)?;
        Ok(Self)
    }

    fn validate_config(config: &ParserConfig) -> Result<()> {
        use super::config::ParserFeature;

        if config.has_feature(&ParserFeature::Streaming) {
            return Err(ParserError::unsupported_feature("antlr", "streaming").into());
        }

        Ok(())
    }

    /// Get backend information
    pub fn backend_info() -> ParserBackendInfo {
        ParserBackendInfo {
            name: "antlr".to_string(),
            version: "4.0".to_string(),
            features: vec![
                ParserFeature::ErrorRecovery,
                ParserFeature::SyntaxHighlighting,
                ParserFeature::CodeCompletion,
                ParserFeature::AstTransformation,
            ],
            performance: PerformanceCharacteristics {
                statements_per_second: 5_000,
                memory_per_statement: 2048,
                startup_time_ms: 50,
                async_support: true,
            },
        }
    }
}

#[async_trait]
impl CqlParser for AntlrParser {
    async fn parse(&self, _input: &str) -> Result<CqlStatement> {
        not_implemented()
    }

    async fn parse_type(&self, _input: &str) -> Result<CqlDataType> {
        not_implemented()
    }

    async fn parse_expression(&self, _input: &str) -> Result<CqlExpression> {
        not_implemented()
    }

    async fn parse_identifier(&self, _input: &str) -> Result<CqlIdentifier> {
        not_implemented()
    }

    async fn parse_literal(&self, _input: &str) -> Result<CqlLiteral> {
        not_implemented()
    }

    async fn parse_column_definitions(&self, _input: &str) -> Result<Vec<CqlColumnDef>> {
        not_implemented()
    }

    async fn parse_table_options(&self, _input: &str) -> Result<CqlTableOptions> {
        not_implemented()
    }

    fn validate_syntax(&self, _input: &str) -> bool {
        false
    }

    fn backend_info(&self) -> ParserBackendInfo {
        Self::backend_info()
    }
}

#[cfg(test)]
mod tests {
    use super::super::config::ParserConfig;
    use super::*;

    #[test]
    fn test_antlr_parser_creation() {
        let config =
            ParserConfig::default().with_backend(super::super::config::ParserBackend::Antlr);
        let parser = AntlrParser::new(config).unwrap();

        let info = parser.backend_info();
        assert_eq!(info.name, "antlr");
    }

    #[tokio::test]
    async fn test_not_implemented_error() {
        let config = ParserConfig::default();
        let parser = AntlrParser::new(config).unwrap();

        let err = parser.parse("SELECT * FROM users").await.unwrap_err();
        assert!(err.to_string().contains("not yet implemented"));
    }

    #[test]
    fn test_unsupported_features() {
        use super::super::config::{ParserConfig, ParserFeature};

        let config = ParserConfig::default().with_feature(ParserFeature::Streaming);

        let result = AntlrParser::new(config);
        assert!(result.is_err());
    }
}
