//! Core parser trait definitions for CQLite
//!
//! This module defines the fundamental traits that abstract over different
//! parser implementations (nom, ANTLR, etc.), allowing the system to switch
//! between backends transparently.

use crate::error::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display, Formatter};

use super::ast::*;

/// Main trait for CQL parsing operations.
///
/// Abstracts over different parser backends (nom, ANTLR) and provides a
/// unified interface for parsing CQL statements.
#[async_trait]
pub trait CqlParser: Debug + Send + Sync {
    /// Parse a complete CQL statement from input text.
    async fn parse(&self, input: &str) -> Result<CqlStatement>;

    /// Parse a CQL data type specification (e.g., `list<text>`, `map<uuid, bigint>`).
    async fn parse_type(&self, input: &str) -> Result<CqlDataType>;

    /// Parse a CQL expression (e.g., `id = ? AND name LIKE 'test%'`).
    async fn parse_expression(&self, input: &str) -> Result<CqlExpression>;

    /// Parse a CQL identifier (table name, column name, etc.).
    async fn parse_identifier(&self, input: &str) -> Result<CqlIdentifier>;

    /// Parse a literal value (e.g., `'hello'`, `42`, `true`).
    async fn parse_literal(&self, input: &str) -> Result<CqlLiteral>;

    /// Parse a list of column definitions (for CREATE TABLE).
    async fn parse_column_definitions(&self, input: &str) -> Result<Vec<CqlColumnDef>>;

    /// Parse CREATE TABLE options (e.g., `WITH comment = 'test'`).
    async fn parse_table_options(&self, input: &str) -> Result<CqlTableOptions>;

    /// Lightweight check that input appears to be valid CQL syntax.
    fn validate_syntax(&self, input: &str) -> bool;

    /// Get parser backend information.
    fn backend_info(&self) -> ParserBackendInfo;
}

/// Trait for visiting and transforming AST nodes.
///
/// Implements the visitor pattern for AST traversal, allowing for analysis,
/// transformation, and code generation.
pub trait CqlVisitor<T>: Debug {
    fn visit_statement(&mut self, statement: &CqlStatement) -> Result<T>;
    fn visit_select(&mut self, select: &CqlSelect) -> Result<T>;
    fn visit_insert(&mut self, insert: &CqlInsert) -> Result<T>;
    fn visit_update(&mut self, update: &CqlUpdate) -> Result<T>;
    fn visit_delete(&mut self, delete: &CqlDelete) -> Result<T>;
    fn visit_create_table(&mut self, create: &CqlCreateTable) -> Result<T>;
    fn visit_drop_table(&mut self, drop: &CqlDropTable) -> Result<T>;
    fn visit_create_index(&mut self, create: &CqlCreateIndex) -> Result<T>;
    fn visit_alter_table(&mut self, alter: &CqlAlterTable) -> Result<T>;
    fn visit_data_type(&mut self, data_type: &CqlDataType) -> Result<T>;
    fn visit_expression(&mut self, expression: &CqlExpression) -> Result<T>;
    fn visit_identifier(&mut self, identifier: &CqlIdentifier) -> Result<T>;
    fn visit_literal(&mut self, literal: &CqlLiteral) -> Result<T>;
}

/// Information about a parser backend.
#[derive(Debug, Clone, PartialEq)]
pub struct ParserBackendInfo {
    /// Backend name (e.g., "nom", "antlr").
    pub name: String,
    /// Backend version.
    pub version: String,
    /// Supported features.
    pub features: Vec<ParserFeature>,
    /// Performance characteristics.
    pub performance: PerformanceCharacteristics,
}

/// Parser backend features.
#[derive(Debug, Clone, PartialEq)]
pub enum ParserFeature {
    Incremental,
    ErrorRecovery,
    SyntaxHighlighting,
    CodeCompletion,
    AstTransformation,
    CustomOperators,
    Streaming,
    Parallel,
    Caching,
}

/// Performance characteristics of a parser backend.
#[derive(Debug, Clone, PartialEq)]
pub struct PerformanceCharacteristics {
    /// Average parsing speed (statements per second).
    pub statements_per_second: u32,
    /// Memory usage per statement (bytes).
    pub memory_per_statement: u32,
    /// Startup time (milliseconds).
    pub startup_time_ms: u32,
    /// Supports async parsing.
    pub async_support: bool,
}

/// Context for semantic validation.
#[derive(Debug, Clone)]
pub struct ValidationContext {
    /// Available table schemas.
    pub schemas: std::collections::HashMap<String, crate::schema::TableSchema>,
    /// Available UDT definitions.
    pub udts: std::collections::HashMap<String, crate::types::UdtTypeDef>,
    /// Current keyspace.
    pub current_keyspace: Option<String>,
    /// Validation strictness level.
    pub strictness: ValidationStrictness,
}

/// Validation strictness levels.
#[derive(Debug, Clone, PartialEq)]
pub enum ValidationStrictness {
    /// All references must be valid.
    Strict,
    /// Allow unknown references.
    Lenient,
    /// Minimal validation.
    Permissive,
}

impl ValidationContext {
    /// Create a new validation context with strict validation.
    pub fn new() -> Self {
        Self {
            schemas: std::collections::HashMap::new(),
            udts: std::collections::HashMap::new(),
            current_keyspace: None,
            strictness: ValidationStrictness::Strict,
        }
    }
}

impl Default for ValidationContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Trait for parser factories.
///
/// Allows creation of parser instances with different configurations.
pub trait CqlParserFactory: Debug + Send + Sync {
    /// Create a new parser instance.
    fn create_parser(&self) -> Result<Box<dyn CqlParser>>;

    /// Create a parser with specific configuration.
    fn create_parser_with_config(
        &self,
        config: super::config::ParserConfig,
    ) -> Result<Box<dyn CqlParser>>;

    /// Get factory information.
    fn factory_info(&self) -> FactoryInfo;
}

/// Information about a parser factory.
#[derive(Debug, Clone)]
pub struct FactoryInfo {
    /// Factory name.
    pub name: String,
    /// Supported backends.
    pub supported_backends: Vec<String>,
    /// Default backend.
    pub default_backend: String,
}

/// Source position information for error reporting.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourcePosition {
    /// Line number (1-based).
    pub line: u32,
    /// Column number (1-based).
    pub column: u32,
    /// Byte offset in input.
    pub offset: u32,
    /// Length of the parsed element.
    pub length: u32,
}

impl SourcePosition {
    /// Create a new source position.
    pub fn new(line: u32, column: u32, offset: u32, length: u32) -> Self {
        Self {
            line,
            column,
            offset,
            length,
        }
    }

    /// Create a position at the start of input.
    pub fn start() -> Self {
        Self::new(1, 1, 0, 0)
    }
}

impl Display for SourcePosition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "line {}, column {}", self.line, self.column)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validation_context_creation() {
        let ctx = ValidationContext::new();
        assert_eq!(ctx.strictness, ValidationStrictness::Strict);
        assert!(ctx.schemas.is_empty());
        assert!(ctx.udts.is_empty());
        assert!(ctx.current_keyspace.is_none());
    }

    #[test]
    fn test_source_position() {
        let pos = SourcePosition::new(10, 5, 100, 20);
        assert_eq!(pos.line, 10);
        assert_eq!(pos.column, 5);
        assert_eq!(pos.offset, 100);
        assert_eq!(pos.length, 20);

        let start_pos = SourcePosition::start();
        assert_eq!(start_pos.line, 1);
        assert_eq!(start_pos.column, 1);
        assert_eq!(start_pos.offset, 0);
    }

    #[test]
    fn test_parser_backend_info() {
        let info = ParserBackendInfo {
            name: "test".to_string(),
            version: "1.0.0".to_string(),
            features: vec![ParserFeature::Incremental, ParserFeature::ErrorRecovery],
            performance: PerformanceCharacteristics {
                statements_per_second: 1000,
                memory_per_statement: 1024,
                startup_time_ms: 10,
                async_support: true,
            },
        };

        assert_eq!(info.name, "test");
        assert_eq!(info.features.len(), 2);
        assert!(info.performance.async_support);
    }
}
