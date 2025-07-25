//! Core parser trait definitions for CQLite
//!
//! This module defines the fundamental traits that abstract over different
//! parser implementations (nom, ANTLR, etc.), allowing the system to switch
//! between backends transparently.

use crate::error::Result;
use crate::schema::CqlType;
use std::fmt::{Debug, Display, Formatter};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::{ast::*, visitor::*, error::ParserError};

/// Main trait for CQL parsing operations
/// 
/// This trait abstracts over different parser backends (nom, ANTLR)
/// and provides a unified interface for parsing CQL statements.
#[async_trait]
pub trait CqlParser: Debug + Send + Sync {
    /// Parse a complete CQL statement from input text
    /// 
    /// # Arguments
    /// * `input` - The CQL statement text to parse
    /// 
    /// # Returns
    /// * `Ok(CqlStatement)` - Successfully parsed statement
    /// * `Err(ParserError)` - Parse error with details
    async fn parse(&self, input: &str) -> Result<CqlStatement>;
    
    /// Parse a CQL data type specification
    /// 
    /// # Arguments
    /// * `input` - The type specification (e.g., "list<text>", "map<uuid, bigint>")
    /// 
    /// # Returns
    /// * `Ok(CqlDataType)` - Successfully parsed type
    /// * `Err(ParserError)` - Parse error with details
    async fn parse_type(&self, input: &str) -> Result<CqlDataType>;
    
    /// Parse a CQL expression
    /// 
    /// # Arguments
    /// * `input` - The expression text (e.g., "id = ? AND name LIKE 'test%'")
    /// 
    /// # Returns  
    /// * `Ok(CqlExpression)` - Successfully parsed expression
    /// * `Err(ParserError)` - Parse error with details
    async fn parse_expression(&self, input: &str) -> Result<CqlExpression>;
    
    /// Parse a CQL identifier (table name, column name, etc.)
    /// 
    /// # Arguments
    /// * `input` - The identifier text
    /// 
    /// # Returns
    /// * `Ok(CqlIdentifier)` - Successfully parsed identifier
    /// * `Err(ParserError)` - Parse error with details
    async fn parse_identifier(&self, input: &str) -> Result<CqlIdentifier>;
    
    /// Parse a literal value
    /// 
    /// # Arguments
    /// * `input` - The literal text (e.g., "'hello'", "42", "true")
    /// 
    /// # Returns
    /// * `Ok(CqlLiteral)` - Successfully parsed literal
    /// * `Err(ParserError)` - Parse error with details
    async fn parse_literal(&self, input: &str) -> Result<CqlLiteral>;
    
    /// Parse a list of column definitions (for CREATE TABLE)
    /// 
    /// # Arguments
    /// * `input` - The column definitions text
    /// 
    /// # Returns
    /// * `Ok(Vec<CqlColumnDef>)` - Successfully parsed column definitions
    /// * `Err(ParserError)` - Parse error with details
    async fn parse_column_definitions(&self, input: &str) -> Result<Vec<CqlColumnDef>>;
    
    /// Parse CREATE TABLE options
    /// 
    /// # Arguments
    /// * `input` - The table options text (e.g., "WITH comment = 'test'")
    /// 
    /// # Returns
    /// * `Ok(CqlTableOptions)` - Successfully parsed options
    /// * `Err(ParserError)` - Parse error with details
    async fn parse_table_options(&self, input: &str) -> Result<CqlTableOptions>;
    
    /// Validate if input looks like valid CQL syntax (lightweight check)
    /// 
    /// # Arguments
    /// * `input` - The text to validate
    /// 
    /// # Returns
    /// * `true` if input appears to be valid CQL
    /// * `false` if input is clearly invalid
    fn validate_syntax(&self, input: &str) -> bool;
    
    /// Get parser backend information
    fn backend_info(&self) -> ParserBackendInfo;
}

/// Trait for validating parsed AST nodes
/// 
/// This trait provides semantic validation of parsed statements
/// beyond syntactic correctness.
pub trait CqlValidator: Debug + Send + Sync {
    /// Validate a parsed statement for semantic correctness
    /// 
    /// # Arguments
    /// * `statement` - The parsed statement to validate
    /// 
    /// # Returns
    /// * `Ok(())` - Statement is semantically valid
    /// * `Err(ParserError)` - Validation error with details
    fn validate_statement(&self, statement: &CqlStatement) -> Result<()>;
    
    /// Validate a data type definition
    /// 
    /// # Arguments
    /// * `data_type` - The data type to validate
    /// 
    /// # Returns
    /// * `Ok(())` - Type is valid
    /// * `Err(ParserError)` - Validation error with details
    fn validate_type(&self, data_type: &CqlDataType) -> Result<()>;
    
    /// Validate an expression in a given context
    /// 
    /// # Arguments
    /// * `expression` - The expression to validate
    /// * `context` - The context (table schema, available columns, etc.)
    /// 
    /// # Returns
    /// * `Ok(())` - Expression is valid in context
    /// * `Err(ParserError)` - Validation error with details
    fn validate_expression(&self, expression: &CqlExpression, context: &ValidationContext) -> Result<()>;
}

/// Trait for visiting and transforming AST nodes
/// 
/// This trait implements the visitor pattern for AST traversal,
/// allowing for analysis, transformation, and code generation.
pub trait CqlVisitor<T>: Debug {
    /// Visit a CQL statement
    fn visit_statement(&mut self, statement: &CqlStatement) -> Result<T>;
    
    /// Visit a SELECT statement
    fn visit_select(&mut self, select: &CqlSelect) -> Result<T>;
    
    /// Visit an INSERT statement  
    fn visit_insert(&mut self, insert: &CqlInsert) -> Result<T>;
    
    /// Visit an UPDATE statement
    fn visit_update(&mut self, update: &CqlUpdate) -> Result<T>;
    
    /// Visit a DELETE statement
    fn visit_delete(&mut self, delete: &CqlDelete) -> Result<T>;
    
    /// Visit a CREATE TABLE statement
    fn visit_create_table(&mut self, create: &CqlCreateTable) -> Result<T>;
    
    /// Visit a DROP TABLE statement
    fn visit_drop_table(&mut self, drop: &CqlDropTable) -> Result<T>;
    
    /// Visit a CREATE INDEX statement
    fn visit_create_index(&mut self, create: &CqlCreateIndex) -> Result<T>;
    
    /// Visit an ALTER TABLE statement
    fn visit_alter_table(&mut self, alter: &CqlAlterTable) -> Result<T>;
    
    /// Visit a data type
    fn visit_data_type(&mut self, data_type: &CqlDataType) -> Result<T>;
    
    /// Visit an expression
    fn visit_expression(&mut self, expression: &CqlExpression) -> Result<T>;
    
    /// Visit an identifier
    fn visit_identifier(&mut self, identifier: &CqlIdentifier) -> Result<T>;
    
    /// Visit a literal value
    fn visit_literal(&mut self, literal: &CqlLiteral) -> Result<T>;
}

/// Information about a parser backend
#[derive(Debug, Clone, PartialEq)]
pub struct ParserBackendInfo {
    /// Backend name (e.g., "nom", "antlr")
    pub name: String,
    /// Backend version
    pub version: String,
    /// Supported features
    pub features: Vec<ParserFeature>,
    /// Performance characteristics
    pub performance: PerformanceCharacteristics,
}

/// Parser backend features
#[derive(Debug, Clone, PartialEq)]
pub enum ParserFeature {
    /// Supports incremental parsing
    Incremental,
    /// Supports error recovery
    ErrorRecovery,
    /// Supports syntax highlighting
    SyntaxHighlighting,
    /// Supports code completion
    CodeCompletion,
    /// Supports AST transformation
    AstTransformation,
    /// Supports custom operators
    CustomOperators,
    /// Supports streaming parsing
    Streaming,
    /// Supports parallel parsing
    Parallel,
    /// Supports caching of parse results
    Caching,
}

/// Performance characteristics of a parser backend
#[derive(Debug, Clone, PartialEq)]
pub struct PerformanceCharacteristics {
    /// Average parsing speed (statements per second)
    pub statements_per_second: u32,
    /// Memory usage per statement (bytes)
    pub memory_per_statement: u32,
    /// Startup time (milliseconds)
    pub startup_time_ms: u32,
    /// Supports async parsing
    pub async_support: bool,
}

/// Context for semantic validation
#[derive(Debug, Clone)]
pub struct ValidationContext {
    /// Available table schemas
    pub schemas: std::collections::HashMap<String, crate::schema::TableSchema>,
    /// Available UDT definitions
    pub udts: std::collections::HashMap<String, crate::types::UdtTypeDef>,
    /// Current keyspace
    pub current_keyspace: Option<String>,
    /// Validation strictness level
    pub strictness: ValidationStrictness,
}

/// Validation strictness levels
#[derive(Debug, Clone, PartialEq)]
pub enum ValidationStrictness {
    /// Strict validation - all references must be valid
    Strict,
    /// Lenient validation - allow unknown references
    Lenient,
    /// Permissive validation - minimal validation
    Permissive,
}

impl ValidationContext {
    /// Create a new validation context
    pub fn new() -> Self {
        Self {
            schemas: std::collections::HashMap::new(),
            udts: std::collections::HashMap::new(),
            current_keyspace: None,
            strictness: ValidationStrictness::Strict,
        }
    }
    
    /// Create a lenient validation context
    pub fn lenient() -> Self {
        Self {
            schemas: std::collections::HashMap::new(),
            udts: std::collections::HashMap::new(),
            current_keyspace: None,
            strictness: ValidationStrictness::Lenient,
        }
    }
    
    /// Add a table schema to the context
    pub fn with_schema(mut self, name: String, schema: crate::schema::TableSchema) -> Self {
        self.schemas.insert(name, schema);
        self
    }
    
    /// Add a UDT definition to the context
    pub fn with_udt(mut self, name: String, udt: crate::types::UdtTypeDef) -> Self {
        self.udts.insert(name, udt);
        self
    }
    
    /// Set the current keyspace
    pub fn with_keyspace(mut self, keyspace: String) -> Self {
        self.current_keyspace = Some(keyspace);
        self
    }
}

impl Default for ValidationContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Trait for parser factories
/// 
/// This trait allows creation of parser instances with different configurations
pub trait CqlParserFactory: Debug + Send + Sync {
    /// Create a new parser instance
    fn create_parser(&self) -> Result<Box<dyn CqlParser>>;
    
    /// Create a parser with specific configuration
    fn create_parser_with_config(&self, config: super::config::ParserConfig) -> Result<Box<dyn CqlParser>>;
    
    /// Get factory information
    fn factory_info(&self) -> FactoryInfo;
}

/// Information about a parser factory
#[derive(Debug, Clone)]
pub struct FactoryInfo {
    /// Factory name
    pub name: String,
    /// Supported backends
    pub supported_backends: Vec<String>,
    /// Default backend
    pub default_backend: String,
}

/// Trait for configurable parsers
/// 
/// This trait allows runtime configuration of parser behavior
pub trait ConfigurableParser: CqlParser {
    /// Update parser configuration
    fn update_config(&mut self, config: super::config::ParserConfig) -> Result<()>;
    
    /// Get current configuration
    fn get_config(&self) -> super::config::ParserConfig;
    
    /// Reset to default configuration
    fn reset_config(&mut self) -> Result<()>;
}

/// Trait for parsers that support streaming/incremental parsing
#[async_trait]
pub trait StreamingParser: CqlParser {
    /// Parse multiple statements from a stream
    async fn parse_stream<'a>(&self, input: &'a str) -> Result<Vec<CqlStatement>>;
    
    /// Parse statements incrementally, yielding results as they become available
    async fn parse_incremental<'a>(&self, input: &'a str) -> Result<Box<dyn Iterator<Item = Result<CqlStatement>> + 'a>>;
    
    /// Parse with position tracking for error reporting
    async fn parse_with_positions(&self, input: &str) -> Result<(CqlStatement, SourcePosition)>;
}

/// Source position information for error reporting
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourcePosition {
    /// Line number (1-based)
    pub line: u32,
    /// Column number (1-based)  
    pub column: u32,
    /// Byte offset in input
    pub offset: u32,
    /// Length of the parsed element
    pub length: u32,
}

impl SourcePosition {
    /// Create a new source position
    pub fn new(line: u32, column: u32, offset: u32, length: u32) -> Self {
        Self { line, column, offset, length }
    }
    
    /// Create a position at the start of input
    pub fn start() -> Self {
        Self::new(1, 1, 0, 0)
    }
}

impl Display for SourcePosition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "line {}, column {}", self.line, self.column)
    }
}

/// Macro for implementing visitor pattern dispatch
#[macro_export]
macro_rules! impl_visitor_dispatch {
    ($visitor:ty, $result:ty) => {
        impl CqlVisitor<$result> for $visitor {
            fn visit_statement(&mut self, statement: &CqlStatement) -> Result<$result> {
                match statement {
                    CqlStatement::Select(select) => self.visit_select(select),
                    CqlStatement::Insert(insert) => self.visit_insert(insert),
                    CqlStatement::Update(update) => self.visit_update(update),
                    CqlStatement::Delete(delete) => self.visit_delete(delete),
                    CqlStatement::CreateTable(create) => self.visit_create_table(create),
                    CqlStatement::DropTable(drop) => self.visit_drop_table(drop),
                    CqlStatement::CreateIndex(create) => self.visit_create_index(create),
                    CqlStatement::AlterTable(alter) => self.visit_alter_table(alter),
                }
            }
        }
    };
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
        
        let lenient_ctx = ValidationContext::lenient();
        assert_eq!(lenient_ctx.strictness, ValidationStrictness::Lenient);
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