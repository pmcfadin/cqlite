//! Schema parsing integration with new parser abstraction layer
//!
//! This module provides updated schema parsing that uses the new parser
//! abstraction layer while maintaining full backward compatibility.

use crate::error::{Error, Result};
use crate::schema::{TableSchema, KeyColumn, ClusteringColumn, Column};
use super::{
    factory::ParserFactory,
    config::{ParserConfig, ParserBackend},
    UseCase,
    visitor::SchemaBuilderVisitor,
    traits::CqlVisitor,
    ast::CqlStatement,
};
use std::sync::Arc;

/// Configuration for schema parsing
#[derive(Debug, Clone)]
pub struct SchemaParserConfig {
    /// Parser backend to use
    pub backend: ParserBackend,
    /// Whether to enable strict validation (default: true)
    pub strict_validation: bool,
    /// Whether to allow experimental features (default: false)
    pub allow_experimental: bool,
    /// Timeout for parsing operations (default: 30s)
    pub timeout_secs: u64,
}

impl Default for SchemaParserConfig {
    fn default() -> Self {
        Self {
            backend: ParserBackend::Auto,
            strict_validation: true,
            allow_experimental: false,
            timeout_secs: 30,
        }
    }
}

impl SchemaParserConfig {
    /// Create a fast configuration optimized for performance
    pub fn fast() -> Self {
        Self {
            backend: ParserBackend::Nom,
            strict_validation: false,
            allow_experimental: false,
            timeout_secs: 10,
        }
    }
    
    /// Create a strict configuration with maximum validation
    pub fn strict() -> Self {
        Self {
            backend: ParserBackend::Antlr,
            strict_validation: true,
            allow_experimental: false,
            timeout_secs: 60,
        }
    }
    
    /// Create configuration for a specific use case
    pub fn for_use_case(use_case: UseCase) -> Self {
        let backend = ParserFactory::recommend_backend(use_case.clone());
        Self {
            backend,
            strict_validation: matches!(use_case, UseCase::Production | UseCase::Development),
            allow_experimental: matches!(use_case, UseCase::Development),
            timeout_secs: match use_case {
                UseCase::HighPerformance | UseCase::Embedded => 10,
                UseCase::Interactive => 5,
                UseCase::Batch => 300,
                _ => 30,
            },
        }
    }
}

/// Parse CQL CREATE TABLE statement using the new parser abstraction layer
/// 
/// This is the main entry point that maintains backward compatibility while
/// using the new parser abstraction internally.
/// 
/// # Arguments
/// * `cql` - The CQL CREATE TABLE statement to parse
/// * `config` - Optional configuration for parsing behavior
/// 
/// # Returns
/// * `Ok(TableSchema)` - Successfully parsed table schema
/// * `Err(Error)` - Parsing error with detailed information
/// 
/// # Example
/// ```rust
/// use cqlite_core::parser::schema_integration::parse_cql_schema_enhanced;
/// 
/// let cql = "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT, age INT)";
/// let schema = parse_cql_schema_enhanced(cql, None).await?;
/// assert_eq!(schema.table, "users");
/// ```
pub async fn parse_cql_schema_enhanced(
    cql: &str,
    config: Option<SchemaParserConfig>,
) -> Result<TableSchema> {
    let config = config.unwrap_or_default();
    
    // Create parser configuration
    let parser_config = ParserConfig::default()
        .with_backend(config.backend)
        .with_strict_validation(config.strict_validation)
        .with_timeout(std::time::Duration::from_secs(config.timeout_secs));
    
    // Create parser using the factory
    let parser = ParserFactory::create(parser_config)?;
    
    // Parse the CQL statement
    let statement = parser.parse(cql).await?;
    
    // Use visitor pattern to convert AST to TableSchema
    let mut visitor = SchemaBuilderVisitor::default();
    let schema = visitor.visit_statement(&statement)?;
    
    Ok(schema)
}

/// Parse CQL CREATE TABLE statement with default configuration
/// 
/// This is a convenience function that uses default settings for most use cases.
/// 
/// # Arguments
/// * `cql` - The CQL CREATE TABLE statement to parse
/// 
/// # Returns
/// * `Ok(TableSchema)` - Successfully parsed table schema
/// * `Err(Error)` - Parsing error with detailed information
pub async fn parse_cql_schema_simple(cql: &str) -> Result<TableSchema> {
    parse_cql_schema_enhanced(cql, None).await
}

/// Parse CQL CREATE TABLE statement for high-performance scenarios
/// 
/// Uses nom parser with minimal validation for maximum speed.
/// 
/// # Arguments
/// * `cql` - The CQL CREATE TABLE statement to parse
/// 
/// # Returns
/// * `Ok(TableSchema)` - Successfully parsed table schema
/// * `Err(Error)` - Parsing error with detailed information
pub async fn parse_cql_schema_fast(cql: &str) -> Result<TableSchema> {
    parse_cql_schema_enhanced(cql, Some(SchemaParserConfig::fast())).await
}

/// Parse CQL CREATE TABLE statement with strict validation
/// 
/// Uses ANTLR parser with comprehensive validation and error reporting.
/// 
/// # Arguments
/// * `cql` - The CQL CREATE TABLE statement to parse
/// 
/// # Returns
/// * `Ok(TableSchema)` - Successfully parsed table schema
/// * `Err(Error)` - Parsing error with detailed information
pub async fn parse_cql_schema_strict(cql: &str) -> Result<TableSchema> {
    parse_cql_schema_enhanced(cql, Some(SchemaParserConfig::strict())).await
}

/// Parse multiple CQL CREATE TABLE statements in batch
/// 
/// This function parses multiple statements efficiently using parallel processing
/// when possible.
/// 
/// # Arguments
/// * `statements` - Vector of CQL CREATE TABLE statements to parse
/// * `config` - Optional configuration for parsing behavior
/// 
/// # Returns
/// * `Ok(Vec<TableSchema>)` - Successfully parsed table schemas
/// * `Err(Error)` - Parsing error with detailed information
pub async fn parse_cql_schemas_batch(
    statements: Vec<&str>,
    config: Option<SchemaParserConfig>,
) -> Result<Vec<TableSchema>> {
    let config = config.unwrap_or_default();
    
    // For now, parse sequentially
    // TODO: Implement parallel parsing when supported by backend
    let mut schemas = Vec::with_capacity(statements.len());
    
    for statement in statements {
        let schema = parse_cql_schema_enhanced(statement, Some(config.clone())).await?;
        schemas.push(schema);
    }
    
    Ok(schemas)
}

/// Validate CQL CREATE TABLE statement syntax without full parsing
/// 
/// This is a lightweight function that checks if the statement has valid syntax
/// without building the full AST or schema.
/// 
/// # Arguments
/// * `cql` - The CQL CREATE TABLE statement to validate
/// * `backend` - Optional parser backend to use (default: Auto)
/// 
/// # Returns
/// * `Ok(true)` - Statement has valid syntax
/// * `Ok(false)` - Statement has invalid syntax
/// * `Err(Error)` - Validation error
pub async fn validate_cql_schema_syntax(
    cql: &str,
    backend: Option<ParserBackend>,
) -> Result<bool> {
    let backend = backend.unwrap_or(ParserBackend::Auto);
    
    let parser_config = ParserConfig::minimal().with_backend(backend);
    let parser = ParserFactory::create(parser_config)?;
    
    Ok(parser.validate_syntax(cql))
}

/// Extract table name from CQL CREATE TABLE statement
/// 
/// This is a utility function that quickly extracts just the table name
/// without parsing the full statement.
/// 
/// # Arguments
/// * `cql` - The CQL CREATE TABLE statement
/// 
/// # Returns
/// * `Ok(String)` - Extracted table name
/// * `Err(Error)` - Extraction error
pub async fn extract_table_name_enhanced(cql: &str) -> Result<String> {
    let parser_config = ParserConfig::minimal().with_backend(ParserBackend::Nom);
    let parser = ParserFactory::create(parser_config)?;
    
    let statement = parser.parse(cql).await?;
    
    match statement {
        CqlStatement::CreateTable(create_table) => {
            Ok(create_table.table.name.name)
        }
        _ => Err(Error::invalid_input(
            "Not a CREATE TABLE statement".to_string()
        )),
    }
}

/// Compatibility wrapper for the existing parse_cql_schema function
/// 
/// This function maintains backward compatibility with the existing API
/// while using the new parser abstraction internally.
/// 
/// # Arguments
/// * `cql` - The CQL CREATE TABLE statement to parse
/// 
/// # Returns
/// * `nom::IResult<&str, TableSchema>` - Result in original format for compatibility
pub fn parse_cql_schema_compat(cql: &str) -> nom::IResult<&str, TableSchema> {
    // Use tokio runtime to run the async function
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| nom::Err::Error(nom::error::Error::new(cql, nom::error::ErrorKind::Fail)))?;
    
    match rt.block_on(parse_cql_schema_simple(cql)) {
        Ok(schema) => Ok(("", schema)),
        Err(_) => Err(nom::Err::Error(nom::error::Error::new(cql, nom::error::ErrorKind::Fail))),
    }
}

/// Test if table name matches the given pattern
/// 
/// This is a utility function for backward compatibility.
/// 
/// # Arguments
/// * `schema` - The table schema to check
/// * `pattern` - The pattern to match against
/// 
/// # Returns
/// * `bool` - Whether the table name matches the pattern
pub fn table_name_matches_enhanced(schema: &TableSchema, pattern: &str) -> bool {
    schema.table == pattern || 
    format!("{}.{}", schema.keyspace, schema.table) == pattern
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_parse_cql_schema_enhanced() {
        let cql = r#"
            CREATE TABLE test_keyspace.users (
                id UUID PRIMARY KEY,
                name TEXT,
                age INT,
                email TEXT
            )
        "#;
        
        let schema = parse_cql_schema_enhanced(cql, None).await.unwrap();
        
        assert_eq!(schema.keyspace, "test_keyspace");
        assert_eq!(schema.table, "users");
        assert_eq!(schema.partition_keys.len(), 1);
        assert_eq!(schema.partition_keys[0].name, "id");
        assert_eq!(schema.columns.len(), 4);
    }
    
    #[tokio::test]
    async fn test_parse_cql_schema_simple() {
        let cql = "CREATE TABLE simple_table (id TEXT PRIMARY KEY, value INT)";
        
        let schema = parse_cql_schema_simple(cql).await.unwrap();
        
        assert_eq!(schema.table, "simple_table");
        assert_eq!(schema.partition_keys.len(), 1);
        assert_eq!(schema.columns.len(), 2);
    }
    
    #[tokio::test]
    async fn test_parse_cql_schema_fast() {
        let cql = "CREATE TABLE fast_table (pk UUID PRIMARY KEY, data BLOB)";
        
        let schema = parse_cql_schema_fast(cql).await.unwrap();
        
        assert_eq!(schema.table, "fast_table");
        assert_eq!(schema.partition_keys[0].data_type, "uuid");
    }
    
    #[tokio::test]
    async fn test_parse_cql_schemas_batch() {
        let statements = vec![
            "CREATE TABLE table1 (id UUID PRIMARY KEY, name TEXT)",
            "CREATE TABLE table2 (pk INT PRIMARY KEY, value BIGINT)",
        ];
        
        let schemas = parse_cql_schemas_batch(statements, None).await.unwrap();
        
        assert_eq!(schemas.len(), 2);
        assert_eq!(schemas[0].table, "table1");
        assert_eq!(schemas[1].table, "table2");
    }
    
    #[tokio::test]
    async fn test_validate_cql_schema_syntax() {
        let valid_cql = "CREATE TABLE test (id UUID PRIMARY KEY)";
        let invalid_cql = "CREATE INVALID SYNTAX";
        
        let valid_result = validate_cql_schema_syntax(valid_cql, None).await.unwrap();
        let invalid_result = validate_cql_schema_syntax(invalid_cql, None).await.unwrap();
        
        assert!(valid_result);
        assert!(!invalid_result);
    }
    
    #[tokio::test]
    async fn test_extract_table_name() {
        let cql = "CREATE TABLE my_keyspace.my_table (id UUID PRIMARY KEY)";
        
        let table_name = extract_table_name_enhanced(cql).await.unwrap();
        
        assert_eq!(table_name, "my_table");
    }
    
    #[test]
    fn test_schema_parser_config() {
        let default_config = SchemaParserConfig::default();
        assert!(matches!(default_config.backend, ParserBackend::Auto));
        assert!(default_config.strict_validation);
        
        let fast_config = SchemaParserConfig::fast();
        assert!(matches!(fast_config.backend, ParserBackend::Nom));
        assert!(!fast_config.strict_validation);
        
        let strict_config = SchemaParserConfig::strict();
        assert!(matches!(strict_config.backend, ParserBackend::Antlr));
        assert!(strict_config.strict_validation);
    }
    
    #[test]
    fn test_table_name_matches() {
        let schema = TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![],
            clustering_keys: vec![],
            columns: vec![],
            comments: std::collections::HashMap::new(),
        };
        
        assert!(table_name_matches_enhanced(&schema, "test_table"));
        assert!(table_name_matches_enhanced(&schema, "test_ks.test_table"));
        assert!(!table_name_matches_enhanced(&schema, "other_table"));
    }
}