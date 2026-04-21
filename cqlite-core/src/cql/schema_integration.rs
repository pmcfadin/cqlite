//! Schema parsing integration with new parser abstraction layer
//!
//! This module provides updated schema parsing that uses the new parser
//! abstraction layer while maintaining full backward compatibility.

use super::{
    ast::CqlStatement,
    config::{ParserBackend, ParserConfig},
    factory::ParserFactory,
    traits::CqlVisitor,
    visitor::SchemaBuilderVisitor,
    UseCase,
};
use crate::error::{Error, Result};
use crate::schema::TableSchema;

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

/// Parse a CQL CREATE TABLE statement using the parser abstraction layer.
///
/// This is the main entry point for schema parsing. Passing `None` for `config`
/// uses [`SchemaParserConfig::default`].
///
/// # Example
/// ```rust,no_run
/// # tokio_test::block_on(async {
/// use cqlite_core::cql::schema_integration::parse_cql_schema_enhanced;
///
/// let cql = "CREATE TABLE users (id UUID PRIMARY KEY, name TEXT, age INT)";
/// let schema = parse_cql_schema_enhanced(cql, None).await.unwrap();
/// assert_eq!(schema.table, "users");
/// # });
/// ```
pub async fn parse_cql_schema_enhanced(
    cql: &str,
    config: Option<SchemaParserConfig>,
) -> Result<TableSchema> {
    let config = config.unwrap_or_default();

    let parser_config = ParserConfig::default()
        .with_backend(config.backend)
        .with_strict_validation(config.strict_validation)
        .with_timeout(std::time::Duration::from_secs(config.timeout_secs));

    let parser = ParserFactory::create(parser_config)?;
    let statement = parser.parse(cql).await?;

    let mut visitor = SchemaBuilderVisitor;
    visitor.visit_statement(&statement)
}

/// Parse a CQL CREATE TABLE statement with default configuration.
pub async fn parse_cql_schema_simple(cql: &str) -> Result<TableSchema> {
    parse_cql_schema_enhanced(cql, None).await
}

/// Parse a CQL CREATE TABLE statement using the fast (nom, minimal validation) preset.
pub async fn parse_cql_schema_fast(cql: &str) -> Result<TableSchema> {
    parse_cql_schema_enhanced(cql, Some(SchemaParserConfig::fast())).await
}

/// Parse a CQL CREATE TABLE statement using the strict (ANTLR, full validation) preset.
pub async fn parse_cql_schema_strict(cql: &str) -> Result<TableSchema> {
    parse_cql_schema_enhanced(cql, Some(SchemaParserConfig::strict())).await
}

/// Parse multiple CQL CREATE TABLE statements sequentially using a shared configuration.
pub async fn parse_cql_schemas_batch(
    statements: Vec<&str>,
    config: Option<SchemaParserConfig>,
) -> Result<Vec<TableSchema>> {
    let config = config.unwrap_or_default();
    let mut schemas = Vec::with_capacity(statements.len());

    for statement in statements {
        schemas.push(parse_cql_schema_enhanced(statement, Some(config.clone())).await?);
    }

    Ok(schemas)
}

/// Validate CQL CREATE TABLE syntax without building the full schema.
///
/// Uses `ParserBackend::Auto` when `backend` is `None`.
#[allow(dead_code)]
pub async fn validate_cql_schema_syntax(cql: &str, backend: Option<ParserBackend>) -> Result<bool> {
    let backend = backend.unwrap_or(ParserBackend::Auto);
    let parser = ParserFactory::create(ParserConfig::minimal().with_backend(backend))?;
    Ok(parser.validate_syntax(cql))
}

/// Extract just the table name from a CQL CREATE TABLE statement.
pub async fn extract_table_name_enhanced(cql: &str) -> Result<String> {
    let parser = ParserFactory::create(ParserConfig::minimal().with_backend(ParserBackend::Nom))?;
    let statement = parser.parse(cql).await?;

    match statement {
        CqlStatement::CreateTable(create_table) => Ok(create_table.table.name.name),
        _ => Err(Error::invalid_input(
            "Not a CREATE TABLE statement".to_string(),
        )),
    }
}

/// Backward-compatibility wrapper matching the original `nom::IResult` signature.
///
/// **DEPRECATED**: Prefer `cqlite_core::schema::parse_cql_schema()` — it is synchronous
/// and does not require a tokio runtime. Delegates to the nom backend directly to
/// avoid runtime-creation overhead on every call.
#[deprecated(
    since = "0.2.0",
    note = "Use cqlite_core::schema::parse_cql_schema() instead - it's synchronous and more efficient"
)]
pub fn parse_cql_schema_compat(cql: &str) -> nom::IResult<&str, TableSchema> {
    use super::nom_backend::NomParser;

    fn nom_err(cql: &str) -> nom::Err<nom::error::Error<&str>> {
        nom::Err::Error(nom::error::Error::new(cql, nom::error::ErrorKind::Fail))
    }

    let parser = NomParser::new(ParserConfig::minimal()).map_err(|_| nom_err(cql))?;
    parser
        .parse_create_table_to_schema(cql)
        .map(|schema| ("", schema))
        .map_err(|_| nom_err(cql))
}

/// Returns `true` if `pattern` matches either the unqualified table name or the
/// fully qualified `keyspace.table` form.
pub fn table_name_matches_enhanced(schema: &TableSchema, pattern: &str) -> bool {
    schema.table == pattern || format!("{}.{}", schema.keyspace, schema.table) == pattern
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "M2+ feature; gated for M1"]
    async fn test_parse_cql_schema_enhanced() {
        let cql = r"
            CREATE TABLE test_keyspace.users (
                id UUID PRIMARY KEY,
                name TEXT,
                age INT,
                email TEXT
            )
        ";

        let schema = parse_cql_schema_enhanced(cql, None).await.unwrap();

        assert_eq!(schema.keyspace, "test_keyspace");
        assert_eq!(schema.table, "users");
        assert_eq!(schema.partition_keys.len(), 1);
        assert_eq!(schema.partition_keys[0].name, "id");
        assert_eq!(schema.columns.len(), 4);
    }

    #[tokio::test]
    #[ignore = "M2+ feature; gated for M1"]
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
    #[ignore = "M2+ feature; gated for M1"]
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
    #[ignore = "M2+ feature; gated for M1"]
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
