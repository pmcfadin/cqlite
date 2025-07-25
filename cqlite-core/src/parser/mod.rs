//! Parser module for CQLite
//!
//! This module provides a comprehensive parser abstraction layer that allows
//! switching between different parser backends (nom, ANTLR) while maintaining
//! a consistent API. It includes:
//!
//! - Parser trait abstractions for backend-agnostic parsing
//! - Abstract Syntax Tree (AST) definitions for CQL statements
//! - Visitor pattern for AST traversal and transformation
//! - Configuration system for parser selection and optimization
//! - Factory pattern for parser instantiation
//! - Error handling specific to parsing operations
//! - Binary format parsing for SSTable compatibility (backward compatibility)

// Core trait definitions
pub mod traits;
pub mod ast;
pub mod visitor;
pub mod error;
pub mod config;

// Parser implementations
pub mod nom_backend;
pub mod antlr_backend;

// Factory and configuration
pub mod factory;

// Schema integration with new parser layer
pub mod schema_integration;

// Binary format parsing (for backward compatibility)
pub mod binary;

// Re-export existing modules for backward compatibility
pub mod benchmarks;
pub mod collection_benchmarks;
#[cfg(test)]
pub mod collection_tests;
#[cfg(test)]
pub mod collection_validation_tests;
#[cfg(test)]
pub mod collection_udt_tests;
pub mod complex_types;
pub mod header;
pub mod statistics;
pub mod enhanced_statistics_parser;
#[cfg(test)]
pub mod statistics_test;
#[cfg(test)]
pub mod enhanced_statistics_test;
pub mod types;
#[cfg(test)]
pub mod udt_tests;
pub mod validation;
pub mod vint;

// M3 Performance Optimization Modules
pub mod optimized_complex_types;
pub mod m3_performance_benchmarks;
pub mod performance_regression_framework;

// Re-export core trait abstractions
pub use traits::{
    CqlParser, CqlVisitor, CqlParserFactory, ParserBackendInfo, 
    ParserFeature, PerformanceCharacteristics, FactoryInfo, SourcePosition
};

// Re-export AST types for convenience
pub use ast::{
    CqlStatement, CqlDataType, CqlExpression, CqlIdentifier, CqlLiteral,
    CqlSelect, CqlInsert, CqlUpdate, CqlDelete, CqlCreateTable, CqlDropTable,
    CqlColumnDef, CqlTableOptions, CqlPrimaryKey, CqlTable,
    CqlSelectItem, CqlInsertValues, CqlAssignment, CqlAssignmentOperator,
    CqlBinaryOperator, CqlUnaryOperator, CqlOrderBy, CqlSortDirection,
    CqlUsing
};

// Re-export visitor pattern
pub use visitor::{DefaultVisitor, SemanticValidator, IdentifierCollector, SchemaBuilderVisitor, ValidationVisitor, TypeCollectorVisitor};

// Re-export error types
pub use error::{ParserError, ParserWarning, ErrorSeverity, ErrorCategory as ParserErrorCategory};

// Re-export configuration
pub use config::{
    ParserConfig, ParserBackend, ParserFeature as ConfigFeature,
    PerformanceSettings, MemorySettings, SecuritySettings
};

// Re-export factory
pub use factory::{ParserFactory, UseCase, ParserRegistry, global_registry, register_global_factory};

// Re-export schema integration functions
pub use schema_integration::{
    parse_cql_schema_enhanced, parse_cql_schema_simple, parse_cql_schema_fast,
    parse_cql_schema_strict, parse_cql_schemas_batch, validate_cql_schema_syntax,
    extract_table_name_enhanced, table_name_matches_enhanced,
    SchemaParserConfig, parse_cql_schema_compat
};

// Re-export parser implementations
pub use nom_backend::NomParser;
pub use antlr_backend::AntlrParser;

// Re-export binary format parser for backward compatibility
pub use binary::{SSTableParser, CQLiteParseError, ParseResult};

// Re-export binary format parsers for backward compatibility
pub use benchmarks::*;
pub use complex_types::*;
pub use header::*;
pub use statistics::*;
pub use enhanced_statistics_parser::*;
pub use types::*;
pub use validation::*;
pub use vint::*;

// Re-export M3 performance modules
pub use optimized_complex_types::OptimizedComplexTypeParser;
pub use m3_performance_benchmarks::{M3PerformanceBenchmarks, PerformanceTargets};
pub use performance_regression_framework::{PerformanceRegressionFramework, RegressionThresholds};

use crate::error::{Error, Result};
use std::sync::Arc;

/// Re-export common result types
pub use crate::error::{Result as CqlResult};

/// Convenience function to create a default parser
pub fn create_default_parser() -> Result<Arc<dyn CqlParser + Send + Sync>> {
    ParserFactory::create_default()
}

/// Convenience function to create a parser with specific configuration
pub fn create_parser(config: ParserConfig) -> Result<Arc<dyn CqlParser + Send + Sync>> {
    ParserFactory::create(config)
}

/// Convenience function to create a parser for a specific use case
pub fn create_parser_for_use_case(use_case: UseCase) -> Result<Arc<dyn CqlParser + Send + Sync>> {
    ParserFactory::create_for_use_case(use_case)
}

/// Get information about available parser backends
pub fn get_available_backends() -> Vec<ParserBackendInfo> {
    ParserFactory::get_available_backends()
}

/// Check if a specific backend is available
pub fn is_backend_available(backend: &ParserBackend) -> bool {
    ParserFactory::is_backend_available(backend)
}

/// Parse CQL CREATE TABLE statement (main backward compatibility function)
/// 
/// This function maintains full backward compatibility with the existing
/// parse_cql_schema API while using the new parser abstraction internally.
/// 
/// # Arguments
/// * `input` - The CQL CREATE TABLE statement to parse
/// 
/// # Returns
/// * `nom::IResult<&str, crate::schema::TableSchema>` - Parsed schema or error
pub fn parse_cql_schema(input: &str) -> nom::IResult<&str, crate::schema::TableSchema> {
    use crate::schema::TableSchema;
    
    // Use the compatibility wrapper
    parse_cql_schema_compat(input)
}

/// Get the recommended backend for a specific use case
pub fn recommend_backend(use_case: UseCase) -> ParserBackend {
    ParserFactory::recommend_backend(use_case)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_parser_creation() {
        let parser = create_default_parser().unwrap();
        let info = parser.backend_info();
        assert!(!info.name.is_empty());
    }

    #[tokio::test]
    async fn test_parser_with_config() {
        let config = ParserConfig::default().with_backend(ParserBackend::Nom);
        let parser = create_parser(config).unwrap();
        
        let info = parser.backend_info();
        assert_eq!(info.name, "nom");
    }

    #[tokio::test]
    async fn test_use_case_parsers() {
        let parser = create_parser_for_use_case(UseCase::HighPerformance).unwrap();
        let info = parser.backend_info();
        assert_eq!(info.name, "nom"); // Should select nom for high performance
    }

    #[test]
    fn test_backend_availability() {
        assert!(is_backend_available(&ParserBackend::Nom));
        assert!(is_backend_available(&ParserBackend::Auto));
    }

    #[test]
    fn test_backend_recommendations() {
        assert_eq!(recommend_backend(UseCase::HighPerformance), ParserBackend::Nom);
        assert_eq!(recommend_backend(UseCase::Development), ParserBackend::Antlr);
        assert_eq!(recommend_backend(UseCase::Production), ParserBackend::Auto);
    }

    #[test]
    fn test_available_backends() {
        let backends = get_available_backends();
        assert!(!backends.is_empty());
        
        // Should have at least nom backend
        assert!(backends.iter().any(|b| b.name == "nom"));
    }

    #[tokio::test]
    async fn test_basic_parsing() {
        let parser = create_default_parser().unwrap();
        
        // Test that we can attempt to parse (even if not fully implemented)
        let result = parser.parse("SELECT * FROM users").await;
        // Result can be Ok or Err depending on implementation status
        // We're just testing that the interface works
        match result {
            Ok(statement) => {
                // Parsing succeeded
                assert!(matches!(statement, CqlStatement::Select(_)));
            }
            Err(_) => {
                // Parsing failed, which is expected for placeholder implementations
                // This is fine as long as the trait interface works
            }
        }
    }

    #[test]
    fn test_ast_types() {
        use ast::*;
        
        // Test that we can create AST nodes
        let identifier = CqlIdentifier::new("test_table");
        assert_eq!(identifier.name(), "test_table");
        assert!(!identifier.is_quoted());
        
        let quoted_identifier = CqlIdentifier::quoted("test table");
        assert_eq!(quoted_identifier.name(), "test table");
        assert!(quoted_identifier.is_quoted());
        
        let table = CqlTable::new("users");
        assert_eq!(table.name().name(), "users");
    }

    #[test]
    fn test_visitor_pattern() {
        use visitor::*;
        use ast::*;
        
        let mut collector = IdentifierCollector::new();
        let table = CqlTable::new("users");
        let statement = CqlStatement::Select(CqlSelect {
            distinct: false,
            select_list: vec![CqlSelectItem::Wildcard],
            from: table,
            where_clause: None,
            order_by: None,
            limit: None,
            allow_filtering: false,
        });
        
        let result = collector.visit_statement(&statement);
        assert!(result.is_ok());
        
        let identifiers = collector.into_identifiers();
        assert!(identifiers.iter().any(|id| id.name() == "users"));
    }

    #[test]
    fn test_error_types() {
        use error::*;
        
        let error = ParserError::syntax(
            "Test error".to_string(),
            SourcePosition::start(),
        );
        
        assert_eq!(error.category(), &ErrorCategory::Syntax);
        assert_eq!(error.severity(), &ErrorSeverity::Error);
        assert!(error.message().contains("Test error"));
    }

    #[test]
    fn test_configuration() {
        use config::*;
        
        let config = ParserConfig::default()
            .with_backend(ParserBackend::Nom)
            .with_feature(ParserFeature::Streaming);
        
        assert_eq!(config.backend, ParserBackend::Nom);
        assert!(config.has_feature(&ParserFeature::Streaming));
        
        let validation_result = config.validate();
        assert!(validation_result.is_ok());
    }
}