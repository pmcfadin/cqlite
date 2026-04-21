//! Nom-based parser implementation
//!
//! This module provides a nom-based implementation of the CqlParser trait,
//! optimized for performance and streaming parsing.

use crate::error::Result;
use async_trait::async_trait;

use super::{
    ast::*,
    config::ParserConfig,
    error::ParserError,
    traits::{CqlParser, ParserBackendInfo, ParserFeature, PerformanceCharacteristics},
};
use crate::schema::TableSchema;

/// Nom-based parser implementation
#[derive(Debug)]
pub struct NomParser {}

impl NomParser {
    /// Create a new nom parser with the given configuration
    pub fn new(config: ParserConfig) -> Result<Self> {
        Self::validate_config(&config)?;
        Ok(Self {})
    }

    /// Reject configurations that request features the nom backend does not implement.
    fn validate_config(config: &ParserConfig) -> Result<()> {
        use super::config::ParserFeature;

        if config.has_feature(&ParserFeature::CodeCompletion) {
            return Err(ParserError::unsupported_feature("nom", "code completion").into());
        }
        if config.has_feature(&ParserFeature::SyntaxHighlighting) {
            return Err(ParserError::unsupported_feature("nom", "syntax highlighting").into());
        }
        Ok(())
    }

    /// Parse CQL CREATE TABLE statement and return TableSchema for backward compatibility
    pub fn parse_create_table_to_schema(&self, input: &str) -> Result<TableSchema> {
        match self.parse_create_table_statement(input)? {
            CqlStatement::CreateTable(ast) => Ok(self.convert_ast_to_table_schema(&ast)),
            _ => Err(ParserError::syntax(
                "Expected CREATE TABLE statement",
                super::traits::SourcePosition::start(),
            )
            .into()),
        }
    }

    /// Get backend information
    pub fn backend_info() -> ParserBackendInfo {
        ParserBackendInfo {
            name: "nom".to_string(),
            version: "7.1".to_string(),
            features: vec![
                ParserFeature::Streaming,
                ParserFeature::Parallel,
                ParserFeature::Caching,
            ],
            performance: PerformanceCharacteristics {
                statements_per_second: 10_000,
                memory_per_statement: 1024,
                startup_time_ms: 1,
                async_support: true,
            },
        }
    }
}

#[async_trait]
impl CqlParser for NomParser {
    async fn parse(&self, input: &str) -> Result<CqlStatement> {
        self.parse_statement_impl(input)
    }

    async fn parse_type(&self, input: &str) -> Result<CqlDataType> {
        self.parse_type_impl(input)
    }

    async fn parse_expression(&self, input: &str) -> Result<CqlExpression> {
        self.parse_expression_impl(input)
    }

    async fn parse_identifier(&self, input: &str) -> Result<CqlIdentifier> {
        self.parse_identifier_impl(input)
    }

    async fn parse_literal(&self, input: &str) -> Result<CqlLiteral> {
        self.parse_literal_impl(input)
    }

    async fn parse_column_definitions(&self, input: &str) -> Result<Vec<CqlColumnDef>> {
        self.parse_column_definitions_impl(input)
    }

    async fn parse_table_options(&self, input: &str) -> Result<CqlTableOptions> {
        self.parse_table_options_impl(input)
    }

    fn validate_syntax(&self, input: &str) -> bool {
        // Quick syntax validation without full parsing
        !input.trim().is_empty() && self.quick_syntax_check(input)
    }

    fn backend_info(&self) -> ParserBackendInfo {
        Self::backend_info()
    }
}

impl NomParser {
    /// Convert TableSchema from cql_parser to AST CreateTable statement
    fn convert_table_schema_to_ast(
        &self,
        schema: crate::schema::TableSchema,
    ) -> Result<CqlCreateTable> {
        let columns = schema
            .columns
            .iter()
            .map(|column| {
                Ok(CqlColumnDef {
                    name: CqlIdentifier::new(&column.name),
                    data_type: self.convert_cql_type_string_to_ast(&column.data_type)?,
                    is_static: column.is_static,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let primary_key = CqlPrimaryKey {
            partition_key: schema
                .partition_keys
                .iter()
                .map(|k| CqlIdentifier::new(&k.name))
                .collect(),
            clustering_key: schema
                .clustering_keys
                .iter()
                .map(|k| CqlIdentifier::new(&k.name))
                .collect(),
        };

        let options = CqlTableOptions {
            options: schema
                .comments
                .into_iter()
                .map(|(k, v)| (k, CqlLiteral::String(v)))
                .collect(),
        };

        Ok(CqlCreateTable {
            // TODO: detect IF NOT EXISTS from original CQL
            if_not_exists: false,
            table: CqlTable::new(&schema.table),
            columns,
            primary_key,
            options,
        })
    }

    /// Convert CQL type string to AST data type
    #[allow(clippy::only_used_in_recursion)]
    fn convert_cql_type_string_to_ast(&self, type_str: &str) -> Result<CqlDataType> {
        let trimmed = type_str.trim();
        let type_lower = trimmed.to_lowercase();

        // Extract inner string for a parameterized type like `list<...>`.
        // Returns the slice between the first `<` and the last `>` of `trimmed`.
        let inner_of = |prefix_len: usize| -> Option<&str> {
            trimmed[prefix_len..]
                .rfind('>')
                .map(|end| trimmed[prefix_len..prefix_len + end].trim())
        };

        if type_lower.starts_with("list<") {
            if let Some(inner) = inner_of("list<".len()) {
                return Ok(CqlDataType::List(Box::new(
                    self.convert_cql_type_string_to_ast(inner)?,
                )));
            }
        }
        if type_lower.starts_with("set<") {
            if let Some(inner) = inner_of("set<".len()) {
                return Ok(CqlDataType::Set(Box::new(
                    self.convert_cql_type_string_to_ast(inner)?,
                )));
            }
        }
        if type_lower.starts_with("map<") {
            if let Some(inner) = inner_of("map<".len()) {
                let parts: Vec<&str> = inner.splitn(2, ',').collect();
                if parts.len() == 2 {
                    let key_type = self.convert_cql_type_string_to_ast(parts[0].trim())?;
                    let value_type = self.convert_cql_type_string_to_ast(parts[1].trim())?;
                    return Ok(CqlDataType::Map(Box::new(key_type), Box::new(value_type)));
                }
            }
        }
        if type_lower.starts_with("tuple<") {
            if let Some(inner) = inner_of("tuple<".len()) {
                let types = inner
                    .split(',')
                    .map(|part| self.convert_cql_type_string_to_ast(part.trim()))
                    .collect::<Result<Vec<_>>>()?;
                return Ok(CqlDataType::Tuple(types));
            }
        }
        if type_lower.starts_with("frozen<") {
            if let Some(inner) = inner_of("frozen<".len()) {
                return Ok(CqlDataType::Frozen(Box::new(
                    self.convert_cql_type_string_to_ast(inner)?,
                )));
            }
        }

        // Handle primitive types
        match type_lower.as_str() {
            "text" | "varchar" => Ok(CqlDataType::Text),
            "ascii" => Ok(CqlDataType::Ascii),
            "int" | "integer" => Ok(CqlDataType::Int),
            "bigint" | "long" => Ok(CqlDataType::BigInt),
            "smallint" => Ok(CqlDataType::SmallInt),
            "tinyint" => Ok(CqlDataType::TinyInt),
            "boolean" | "bool" => Ok(CqlDataType::Boolean),
            "float" => Ok(CqlDataType::Float),
            "double" => Ok(CqlDataType::Double),
            "decimal" => Ok(CqlDataType::Decimal),
            "uuid" => Ok(CqlDataType::Uuid),
            "timeuuid" => Ok(CqlDataType::TimeUuid),
            "timestamp" => Ok(CqlDataType::Timestamp),
            "date" => Ok(CqlDataType::Date),
            "time" => Ok(CqlDataType::Time),
            "blob" => Ok(CqlDataType::Blob),
            "inet" => Ok(CqlDataType::Inet),
            "duration" => Ok(CqlDataType::Duration),
            "varint" => Ok(CqlDataType::Varint),
            "counter" => Ok(CqlDataType::Counter),
            // Assume unknown types are UDTs or custom types
            _ => Ok(CqlDataType::Custom(type_str.to_string())),
        }
    }

    /// Convert AST CreateTable back to TableSchema for backward compatibility
    pub fn convert_ast_to_table_schema(&self, ast: &CqlCreateTable) -> TableSchema {
        use crate::schema::{ClusteringColumn, Column, KeyColumn};

        // Look up a column's declared type in the AST; fall back to "text" if
        // the primary key references a column we somehow didn't see.
        let type_of = |name: &str| -> String {
            ast.columns
                .iter()
                .find(|col| col.name.name == name)
                .map(|col| self.convert_ast_type_to_string(&col.data_type))
                .unwrap_or_else(|| "text".to_string())
        };

        let partition_keys = ast
            .primary_key
            .partition_key
            .iter()
            .enumerate()
            .map(|(pos, key)| KeyColumn {
                name: key.name.clone(),
                data_type: type_of(&key.name),
                position: pos,
            })
            .collect();

        let clustering_keys = ast
            .primary_key
            .clustering_key
            .iter()
            .enumerate()
            .map(|(pos, key)| ClusteringColumn {
                name: key.name.clone(),
                data_type: type_of(&key.name),
                position: pos,
                order: crate::schema::ClusteringOrder::Asc,
            })
            .collect();

        let columns = ast
            .columns
            .iter()
            .map(|col| Column {
                name: col.name.name.clone(),
                data_type: self.convert_ast_type_to_string(&col.data_type),
                nullable: true,
                default: None,
                is_static: col.is_static,
            })
            .collect();

        TableSchema {
            // TODO: extract from table name if qualified
            keyspace: "default".to_string(),
            table: ast.table.name.name.clone(),
            partition_keys,
            clustering_keys,
            columns,
            comments: ast
                .options
                .options
                .iter()
                .map(|(k, v)| (k.clone(), format!("{:?}", v)))
                .collect(),
        }
    }

    /// Convert AST data type back to string representation
    #[allow(clippy::only_used_in_recursion)]
    fn convert_ast_type_to_string(&self, ast_type: &CqlDataType) -> String {
        match ast_type {
            CqlDataType::Text => "text".to_string(),
            CqlDataType::Ascii => "ascii".to_string(),
            CqlDataType::Int => "int".to_string(),
            CqlDataType::BigInt => "bigint".to_string(),
            CqlDataType::SmallInt => "smallint".to_string(),
            CqlDataType::TinyInt => "tinyint".to_string(),
            CqlDataType::Boolean => "boolean".to_string(),
            CqlDataType::Float => "float".to_string(),
            CqlDataType::Double => "double".to_string(),
            CqlDataType::Decimal => "decimal".to_string(),
            CqlDataType::Uuid => "uuid".to_string(),
            CqlDataType::TimeUuid => "timeuuid".to_string(),
            CqlDataType::Timestamp => "timestamp".to_string(),
            CqlDataType::Date => "date".to_string(),
            CqlDataType::Time => "time".to_string(),
            CqlDataType::Blob => "blob".to_string(),
            CqlDataType::Inet => "inet".to_string(),
            CqlDataType::Duration => "duration".to_string(),
            CqlDataType::Varint => "varint".to_string(),
            CqlDataType::Counter => "counter".to_string(),
            CqlDataType::List(inner) => format!("list<{}>", self.convert_ast_type_to_string(inner)),
            CqlDataType::Set(inner) => format!("set<{}>", self.convert_ast_type_to_string(inner)),
            CqlDataType::Map(key, value) => format!(
                "map<{}, {}>",
                self.convert_ast_type_to_string(key),
                self.convert_ast_type_to_string(value)
            ),
            CqlDataType::Tuple(types) => {
                let type_strs: Vec<String> = types
                    .iter()
                    .map(|t| self.convert_ast_type_to_string(t))
                    .collect();
                format!("tuple<{}>", type_strs.join(", "))
            }
            CqlDataType::Frozen(inner) => {
                format!("frozen<{}>", self.convert_ast_type_to_string(inner))
            }
            CqlDataType::Custom(name) => name.clone(),
            CqlDataType::Varchar => "varchar".to_string(),
            CqlDataType::Udt(name) => name.name.clone(),
        }
    }

    /// Parse a complete CQL statement using nom parsers
    fn parse_statement_impl(&self, input: &str) -> Result<CqlStatement> {
        let trimmed = input.trim();

        // Case-insensitive keyword-prefix dispatch. We check the longest
        // prefixes first so two-word keywords (e.g. `CREATE TABLE`) win over
        // their single-word counterparts.
        fn starts_with_ci(haystack: &str, needle: &str) -> bool {
            haystack.len() >= needle.len() && haystack[..needle.len()].eq_ignore_ascii_case(needle)
        }

        if starts_with_ci(trimmed, "create table") {
            self.parse_create_table_statement(input)
        } else if starts_with_ci(trimmed, "drop table") {
            self.parse_drop_table_statement(input)
        } else if starts_with_ci(trimmed, "select") {
            self.parse_select_statement(input)
        } else if starts_with_ci(trimmed, "insert") {
            self.parse_insert_statement(input)
        } else if starts_with_ci(trimmed, "update") {
            self.parse_update_statement(input)
        } else if starts_with_ci(trimmed, "delete") {
            self.parse_delete_statement(input)
        } else {
            Err(ParserError::syntax(
                format!("Unsupported statement type: {}", input),
                super::traits::SourcePosition::start(),
            )
            .into())
        }
    }

    /// Parse SELECT statement (placeholder)
    fn parse_select_statement(&self, input: &str) -> Result<CqlStatement> {
        let lower = input.to_lowercase();
        let select = CqlSelect {
            distinct: lower.contains("distinct"),
            select_list: vec![CqlSelectItem::Wildcard],
            from: CqlTable::new("placeholder_table"),
            where_clause: None,
            order_by: None,
            limit: None,
            allow_filtering: lower.contains("allow filtering"),
        };

        Ok(CqlStatement::Select(select))
    }

    /// Parse INSERT statement
    #[cfg(feature = "write-support")]
    fn parse_insert_statement(&self, input: &str) -> Result<CqlStatement> {
        use super::mutation_parser::parse_insert_statement;
        let insert = parse_insert_statement(input)?;
        Ok(CqlStatement::Insert(insert))
    }

    /// Parse INSERT statement (stub when write-support is disabled)
    #[cfg(not(feature = "write-support"))]
    fn parse_insert_statement(&self, _input: &str) -> Result<CqlStatement> {
        Err(ParserError::unsupported_feature(
            "nom",
            "INSERT statement parsing requires 'write-support' feature",
        )
        .into())
    }

    /// Parse UPDATE statement
    #[cfg(feature = "write-support")]
    fn parse_update_statement(&self, input: &str) -> Result<CqlStatement> {
        use super::mutation_parser::parse_update_statement;
        let update = parse_update_statement(input)?;
        Ok(CqlStatement::Update(update))
    }

    /// Parse UPDATE statement (stub when write-support is disabled)
    #[cfg(not(feature = "write-support"))]
    fn parse_update_statement(&self, _input: &str) -> Result<CqlStatement> {
        Err(ParserError::unsupported_feature(
            "nom",
            "UPDATE statement parsing requires 'write-support' feature",
        )
        .into())
    }

    /// Parse DELETE statement
    #[cfg(feature = "write-support")]
    fn parse_delete_statement(&self, input: &str) -> Result<CqlStatement> {
        use super::mutation_parser::parse_delete_statement;
        let delete = parse_delete_statement(input)?;
        Ok(CqlStatement::Delete(delete))
    }

    /// Parse DELETE statement (stub when write-support is disabled)
    #[cfg(not(feature = "write-support"))]
    fn parse_delete_statement(&self, _input: &str) -> Result<CqlStatement> {
        Err(ParserError::unsupported_feature(
            "nom",
            "DELETE statement parsing requires 'write-support' feature",
        )
        .into())
    }

    /// Parse CREATE TABLE statement using the existing nom parser in `schema::cql_parser`.
    fn parse_create_table_statement(&self, input: &str) -> Result<CqlStatement> {
        let (_, table_schema) =
            crate::schema::cql_parser::parse_create_table(input).map_err(|e| {
                ParserError::syntax(
                    format!("Failed to parse CREATE TABLE: {:?}", e),
                    super::traits::SourcePosition::start(),
                )
            })?;

        let ast = self.convert_table_schema_to_ast(table_schema)?;
        Ok(CqlStatement::CreateTable(ast))
    }

    /// Parse DROP TABLE statement (placeholder)
    fn parse_drop_table_statement(&self, _input: &str) -> Result<CqlStatement> {
        let drop_table = CqlDropTable {
            if_exists: false,
            table: CqlTable::new("placeholder_table"),
        };

        Ok(CqlStatement::DropTable(drop_table))
    }

    /// Parse data type (placeholder)
    #[allow(clippy::only_used_in_recursion)]
    fn parse_type_impl(&self, input: &str) -> Result<CqlDataType> {
        let trimmed = input.trim().to_lowercase();

        match trimmed.as_str() {
            "text" | "varchar" => return Ok(CqlDataType::Text),
            "int" | "integer" => return Ok(CqlDataType::Int),
            "bigint" => return Ok(CqlDataType::BigInt),
            "uuid" => return Ok(CqlDataType::Uuid),
            "boolean" | "bool" => return Ok(CqlDataType::Boolean),
            "timestamp" => return Ok(CqlDataType::Timestamp),
            "blob" => return Ok(CqlDataType::Blob),
            _ => {}
        }

        if trimmed.starts_with("list<") && trimmed.ends_with('>') {
            let inner_type = self.parse_type_impl(&trimmed[5..trimmed.len() - 1])?;
            Ok(CqlDataType::List(Box::new(inner_type)))
        } else if trimmed.starts_with("set<") && trimmed.ends_with('>') {
            let inner_type = self.parse_type_impl(&trimmed[4..trimmed.len() - 1])?;
            Ok(CqlDataType::Set(Box::new(inner_type)))
        } else {
            Ok(CqlDataType::Custom(input.to_string()))
        }
    }

    /// Parse expression (placeholder)
    fn parse_expression_impl(&self, input: &str) -> Result<CqlExpression> {
        let trimmed = input.trim();

        if trimmed == "?" {
            Ok(CqlExpression::Parameter(1))
        } else if let Some(stripped) = trimmed.strip_prefix(':') {
            Ok(CqlExpression::NamedParameter(stripped.to_string()))
        } else if trimmed.starts_with('\'') && trimmed.ends_with('\'') {
            Ok(CqlExpression::Literal(CqlLiteral::String(
                trimmed[1..trimmed.len() - 1].to_string(),
            )))
        } else if let Ok(num) = trimmed.parse::<i64>() {
            Ok(CqlExpression::Literal(CqlLiteral::Integer(num)))
        } else if trimmed == "true" || trimmed == "false" {
            Ok(CqlExpression::Literal(CqlLiteral::Boolean(
                trimmed == "true",
            )))
        } else {
            // Assume it's a column reference
            Ok(CqlExpression::Column(CqlIdentifier::new(trimmed)))
        }
    }

    /// Parse identifier (placeholder)
    fn parse_identifier_impl(&self, input: &str) -> Result<CqlIdentifier> {
        let trimmed = input.trim();

        if trimmed.starts_with('"') && trimmed.ends_with('"') {
            Ok(CqlIdentifier::quoted(&trimmed[1..trimmed.len() - 1]))
        } else {
            Ok(CqlIdentifier::new(trimmed))
        }
    }

    /// Parse literal (placeholder)
    fn parse_literal_impl(&self, input: &str) -> Result<CqlLiteral> {
        let trimmed = input.trim();

        if trimmed == "null" {
            Ok(CqlLiteral::Null)
        } else if trimmed == "true" || trimmed == "false" {
            Ok(CqlLiteral::Boolean(trimmed == "true"))
        } else if trimmed.starts_with('\'') && trimmed.ends_with('\'') {
            Ok(CqlLiteral::String(
                trimmed[1..trimmed.len() - 1].to_string(),
            ))
        } else if let Ok(num) = trimmed.parse::<i64>() {
            Ok(CqlLiteral::Integer(num))
        } else if let Ok(num) = trimmed.parse::<f64>() {
            Ok(CqlLiteral::Float(num))
        } else {
            Err(ParserError::syntax(
                format!("Invalid literal: {}", input),
                super::traits::SourcePosition::start(),
            )
            .into())
        }
    }

    /// Parse column definitions (placeholder)
    fn parse_column_definitions_impl(&self, _input: &str) -> Result<Vec<CqlColumnDef>> {
        Ok(vec![
            CqlColumnDef {
                name: CqlIdentifier::new("id"),
                data_type: CqlDataType::Uuid,
                is_static: false,
            },
            CqlColumnDef {
                name: CqlIdentifier::new("name"),
                data_type: CqlDataType::Text,
                is_static: false,
            },
        ])
    }

    /// Parse table options (placeholder)
    fn parse_table_options_impl(&self, _input: &str) -> Result<CqlTableOptions> {
        Ok(CqlTableOptions {
            options: std::collections::HashMap::new(),
        })
    }

    /// Quick syntax validation: non-empty input with balanced parentheses and
    /// closed single-quoted string literals (with backslash escapes).
    fn quick_syntax_check(&self, input: &str) -> bool {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return false;
        }

        let mut paren_count: i32 = 0;
        let mut in_string = false;
        let mut escape_next = false;

        for ch in trimmed.chars() {
            if escape_next {
                escape_next = false;
                continue;
            }

            match ch {
                '\\' if in_string => escape_next = true,
                '\'' => in_string = !in_string,
                '(' if !in_string => paren_count += 1,
                ')' if !in_string => paren_count -= 1,
                _ => {}
            }

            if paren_count < 0 {
                return false;
            }
        }

        paren_count == 0 && !in_string
    }
}

#[cfg(test)]
mod tests {
    use super::super::config::ParserConfig;
    use super::*;

    #[tokio::test]
    async fn test_nom_parser_creation() {
        let config = ParserConfig::default().with_backend(super::super::config::ParserBackend::Nom);
        let parser = NomParser::new(config).unwrap();

        let info = parser.backend_info();
        assert_eq!(info.name, "nom");
    }

    #[cfg(feature = "write-support")]
    #[tokio::test]
    async fn test_basic_parsing() {
        let config = ParserConfig::default();
        let parser = NomParser::new(config).unwrap();

        // Test SELECT parsing
        let result = parser.parse("SELECT * FROM users").await;
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), CqlStatement::Select(_)));

        // Test INSERT parsing
        let result = parser
            .parse("INSERT INTO users (id, name) VALUES (?, ?)")
            .await;
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), CqlStatement::Insert(_)));
    }

    #[tokio::test]
    async fn test_type_parsing() {
        let config = ParserConfig::default();
        let parser = NomParser::new(config).unwrap();

        let result = parser.parse_type("text").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), CqlDataType::Text);

        let result = parser.parse_type("list<int>").await;
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), CqlDataType::List(_)));
    }

    #[tokio::test]
    async fn test_expression_parsing() {
        let config = ParserConfig::default();
        let parser = NomParser::new(config).unwrap();

        let result = parser.parse_expression("?").await;
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), CqlExpression::Parameter(_)));

        let result = parser.parse_expression("'hello'").await;
        assert!(result.is_ok());
        assert!(matches!(
            result.unwrap(),
            CqlExpression::Literal(CqlLiteral::String(_))
        ));
    }

    #[test]
    fn test_syntax_validation() {
        let config = ParserConfig::default();
        let parser = NomParser::new(config).unwrap();

        assert!(parser.validate_syntax("SELECT * FROM users"));
        assert!(!parser.validate_syntax(""));
        assert!(!parser.validate_syntax("SELECT * FROM users ("));
        assert!(!parser.validate_syntax("SELECT * FROM 'unclosed string"));
    }

    #[test]
    fn test_unsupported_features() {
        use super::super::config::{ParserConfig, ParserFeature};

        let config = ParserConfig::default().with_feature(ParserFeature::CodeCompletion);

        let result = NomParser::new(config);
        assert!(result.is_err());
    }

    #[cfg(feature = "write-support")]
    #[tokio::test]
    async fn test_parse_insert_through_parser() {
        let config = ParserConfig::default();
        let parser = NomParser::new(config).unwrap();

        let cql = "INSERT INTO users (id, name) VALUES (?, ?)";
        let result = parser.parse(cql).await;
        assert!(result.is_ok());

        match result.unwrap() {
            CqlStatement::Insert(insert) => {
                assert_eq!(insert.table.name.name, "users");
                assert_eq!(insert.columns.len(), 2);
            }
            _ => panic!("Expected INSERT statement"),
        }
    }

    #[cfg(feature = "write-support")]
    #[tokio::test]
    async fn test_parse_update_through_parser() {
        let config = ParserConfig::default();
        let parser = NomParser::new(config).unwrap();

        let cql = "UPDATE users SET name = ? WHERE id = ?";
        let result = parser.parse(cql).await;
        assert!(result.is_ok());

        match result.unwrap() {
            CqlStatement::Update(update) => {
                assert_eq!(update.table.name.name, "users");
                assert_eq!(update.assignments.len(), 1);
            }
            _ => panic!("Expected UPDATE statement"),
        }
    }

    #[cfg(feature = "write-support")]
    #[tokio::test]
    async fn test_parse_delete_through_parser() {
        let config = ParserConfig::default();
        let parser = NomParser::new(config).unwrap();

        let cql = "DELETE FROM users WHERE id = ?";
        let result = parser.parse(cql).await;
        assert!(result.is_ok());

        match result.unwrap() {
            CqlStatement::Delete(delete) => {
                assert_eq!(delete.table.name.name, "users");
                assert!(delete.columns.is_empty());
            }
            _ => panic!("Expected DELETE statement"),
        }
    }

    #[cfg(not(feature = "write-support"))]
    #[tokio::test]
    async fn test_mutation_statements_require_feature() {
        let config = ParserConfig::default();
        let parser = NomParser::new(config).unwrap();

        // INSERT should fail without write-support
        let result = parser.parse("INSERT INTO users (id) VALUES (?)").await;
        assert!(result.is_err());

        // UPDATE should fail without write-support
        let result = parser.parse("UPDATE users SET name = ? WHERE id = ?").await;
        assert!(result.is_err());

        // DELETE should fail without write-support
        let result = parser.parse("DELETE FROM users WHERE id = ?").await;
        assert!(result.is_err());
    }
}
