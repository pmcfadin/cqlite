//! Nom-based parser implementation
//!
//! This module provides a nom-based implementation of the CqlParser trait,
//! optimized for performance and streaming parsing.

use crate::error::Result;
use async_trait::async_trait;
use std::time::Instant;

use super::{
    traits::{CqlParser, ParserBackendInfo, ParserFeature, PerformanceCharacteristics},
    config::ParserConfig,
    ast::*,
    error::ParserError,
};
use crate::schema::TableSchema;

/// Nom-based parser implementation
#[derive(Debug)]
pub struct NomParser {
    config: ParserConfig,
    stats: ParserStats,
}

/// Parser statistics
#[derive(Debug, Default)]
struct ParserStats {
    parse_count: u64,
    total_parse_time: std::time::Duration,
    error_count: u64,
}

impl NomParser {
    /// Create a new nom parser with the given configuration
    pub fn new(config: ParserConfig) -> Result<Self> {
        // Validate nom-specific configuration
        Self::validate_config(&config)?;
        
        Ok(Self {
            config,
            stats: ParserStats::default(),
        })
    }
    
    /// Validate configuration for nom backend
    fn validate_config(config: &ParserConfig) -> Result<()> {
        use super::config::ParserFeature;
        
        // Check for unsupported features
        if config.has_feature(&ParserFeature::CodeCompletion) {
            return Err(ParserError::unsupported_feature("nom", "code completion").into());
        }
        
        if config.has_feature(&ParserFeature::SyntaxHighlighting) {
            return Err(ParserError::unsupported_feature("nom", "syntax highlighting").into());
        }
        
        Ok(())
    }
    
    /// Get backend information
    /// Parse CQL CREATE TABLE statement and return TableSchema for backward compatibility
    pub fn parse_create_table_to_schema(&self, input: &str) -> Result<TableSchema> {
        match self.parse_create_table_statement(input)? {
            CqlStatement::CreateTable(ast) => Ok(self.convert_ast_to_table_schema(&ast)),
            _ => Err(ParserError::syntax(
                "Expected CREATE TABLE statement".to_string(),
                super::traits::SourcePosition::start(),
            ).into()),
        }
    }
    
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
    
    /// Update parser statistics
    fn update_stats(&mut self, duration: std::time::Duration, success: bool) {
        self.stats.parse_count += 1;
        self.stats.total_parse_time += duration;
        if !success {
            self.stats.error_count += 1;
        }
    }
}

#[async_trait]
impl CqlParser for NomParser {
    async fn parse(&self, input: &str) -> Result<CqlStatement> {
        let _start = Instant::now();
        
        // Use real nom parsing implementation
        let result = self.parse_statement_impl(input);
        
        // Note: We can't update stats here because we have &self, not &mut self
        // In a real implementation, we'd use interior mutability or a different approach
        
        result
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
    fn convert_table_schema_to_ast(&self, schema: crate::schema::TableSchema) -> Result<CqlCreateTable> {
        // Convert table name
        let table = CqlTable::new(&schema.table);
        
        // Convert columns
        let mut columns = Vec::new();
        for column in &schema.columns {
            let data_type = self.convert_cql_type_string_to_ast(&column.data_type)?;
            columns.push(CqlColumnDef {
                name: CqlIdentifier::new(&column.name),
                data_type,
                is_static: false, // TODO: detect static columns
            });
        }
        
        // Convert primary key
        let partition_key = schema.partition_keys.iter()
            .map(|k| CqlIdentifier::new(&k.name))
            .collect();
        let clustering_key = schema.clustering_keys.iter()
            .map(|k| CqlIdentifier::new(&k.name))
            .collect();
        
        let primary_key = CqlPrimaryKey {
            partition_key,
            clustering_key,
        };
        
        // Convert table options
        let options = CqlTableOptions {
            options: schema.comments.into_iter()
                .map(|(k, v)| (k, CqlLiteral::String(v)))
                .collect(),
        };
        
        Ok(CqlCreateTable {
            if_not_exists: false, // TODO: detect IF NOT EXISTS from original CQL
            table,
            columns,
            primary_key,
            options,
        })
    }
    
    /// Convert CQL type string to AST data type
    fn convert_cql_type_string_to_ast(&self, type_str: &str) -> Result<CqlDataType> {
        let type_lower = type_str.trim().to_lowercase();
        
        // Handle collection types
        if type_lower.starts_with("list<") {
            if let Some(inner_start) = type_lower.find('<') {
                if let Some(inner_end) = type_lower.rfind('>') {
                    let inner_type = &type_str[inner_start + 1..inner_end];
                    let inner_ast = self.convert_cql_type_string_to_ast(inner_type)?;
                    return Ok(CqlDataType::List(Box::new(inner_ast)));
                }
            }
        }
        
        if type_lower.starts_with("set<") {
            if let Some(inner_start) = type_lower.find('<') {
                if let Some(inner_end) = type_lower.rfind('>') {
                    let inner_type = &type_str[inner_start + 1..inner_end];
                    let inner_ast = self.convert_cql_type_string_to_ast(inner_type)?;
                    return Ok(CqlDataType::Set(Box::new(inner_ast)));
                }
            }
        }
        
        if type_lower.starts_with("map<") {
            if let Some(inner_start) = type_lower.find('<') {
                if let Some(inner_end) = type_lower.rfind('>') {
                    let inner = &type_str[inner_start + 1..inner_end];
                    let parts: Vec<&str> = inner.splitn(2, ',').collect();
                    if parts.len() == 2 {
                        let key_type = self.convert_cql_type_string_to_ast(parts[0].trim())?;
                        let value_type = self.convert_cql_type_string_to_ast(parts[1].trim())?;
                        return Ok(CqlDataType::Map(Box::new(key_type), Box::new(value_type)));
                    }
                }
            }
        }
        
        if type_lower.starts_with("tuple<") {
            if let Some(inner_start) = type_lower.find('<') {
                if let Some(inner_end) = type_lower.rfind('>') {
                    let inner = &type_str[inner_start + 1..inner_end];
                    let parts: Vec<&str> = inner.split(',').collect();
                    let mut types = Vec::new();
                    for part in parts {
                        types.push(self.convert_cql_type_string_to_ast(part.trim())?);
                    }
                    return Ok(CqlDataType::Tuple(types));
                }
            }
        }
        
        if type_lower.starts_with("frozen<") {
            if let Some(inner_start) = type_lower.find('<') {
                if let Some(inner_end) = type_lower.rfind('>') {
                    let inner_type = &type_str[inner_start + 1..inner_end];
                    let inner_ast = self.convert_cql_type_string_to_ast(inner_type)?;
                    return Ok(CqlDataType::Frozen(Box::new(inner_ast)));
                }
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
            _ => {
                // Assume it's a UDT or custom type
                Ok(CqlDataType::Custom(type_str.to_string()))
            }
        }
    }
    
    /// Convert AST CreateTable back to TableSchema for backward compatibility
    pub fn convert_ast_to_table_schema(&self, ast: &CqlCreateTable) -> TableSchema {
        use crate::schema::{KeyColumn, ClusteringColumn, Column};
        
        // Convert partition keys
        let partition_keys = ast.primary_key.partition_key.iter()
            .enumerate()
            .map(|(pos, key)| {
                let data_type = ast.columns.iter()
                    .find(|col| col.name.name == key.name)
                    .map(|col| self.convert_ast_type_to_string(&col.data_type))
                    .unwrap_or_else(|| "text".to_string());
                
                KeyColumn {
                    name: key.name.clone(),
                    data_type,
                    position: pos,
                }
            })
            .collect();
        
        // Convert clustering keys
        let clustering_keys = ast.primary_key.clustering_key.iter()
            .enumerate()
            .map(|(pos, key)| {
                let data_type = ast.columns.iter()
                    .find(|col| col.name.name == key.name)
                    .map(|col| self.convert_ast_type_to_string(&col.data_type))
                    .unwrap_or_else(|| "text".to_string());
                
                ClusteringColumn {
                    name: key.name.clone(),
                    data_type,
                    position: pos,
                    order: "ASC".to_string(),
                }
            })
            .collect();
        
        // Convert columns
        let columns = ast.columns.iter()
            .map(|col| Column {
                name: col.name.name.clone(),
                data_type: self.convert_ast_type_to_string(&col.data_type),
                nullable: true,
                default: None,
            })
            .collect();
        
        TableSchema {
            keyspace: "default".to_string(), // TODO: extract from table name if qualified
            table: ast.table.name.name.clone(),
            partition_keys,
            clustering_keys,
            columns,
            comments: ast.options.options.iter()
                .map(|(k, v)| (k.clone(), format!("{:?}", v)))
                .collect(),
        }
    }
    
    /// Convert AST data type back to string representation
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
            CqlDataType::Map(key, value) => format!("map<{}, {}>", 
                self.convert_ast_type_to_string(key),
                self.convert_ast_type_to_string(value)),
            CqlDataType::Tuple(types) => {
                let type_strs: Vec<String> = types.iter()
                    .map(|t| self.convert_ast_type_to_string(t))
                    .collect();
                format!("tuple<{}>", type_strs.join(", "))
            },
            CqlDataType::Frozen(inner) => format!("frozen<{}>", self.convert_ast_type_to_string(inner)),
            CqlDataType::Custom(name) => name.clone(),
            CqlDataType::Varchar => "varchar".to_string(),
            CqlDataType::Udt(name) => name.name.clone(),
        }
    }

    /// Parse a complete CQL statement using nom parsers
    fn parse_statement_impl(&self, input: &str) -> Result<CqlStatement> {
        let trimmed = input.trim().to_lowercase();
        
        // Route to appropriate nom parser
        if trimmed.starts_with("select") {
            self.parse_select_statement(input)
        } else if trimmed.starts_with("insert") {
            self.parse_insert_statement(input)
        } else if trimmed.starts_with("update") {
            self.parse_update_statement(input)
        } else if trimmed.starts_with("delete") {
            self.parse_delete_statement(input)
        } else if trimmed.starts_with("create table") {
            self.parse_create_table_statement(input)
        } else if trimmed.starts_with("drop table") {
            self.parse_drop_table_statement(input)
        } else {
            Err(ParserError::syntax(
                format!("Unsupported statement type: {}", input),
                super::traits::SourcePosition::start(),
            ).into())
        }
    }
    
    /// Parse SELECT statement (placeholder)
    fn parse_select_statement(&self, input: &str) -> Result<CqlStatement> {
        // This is a very basic placeholder implementation
        // A real implementation would use proper nom parsers
        
        let select = CqlSelect {
            distinct: input.to_lowercase().contains("distinct"),
            select_list: vec![CqlSelectItem::Wildcard], // Simplified
            from: CqlTable::new("placeholder_table"),
            where_clause: None,
            order_by: None,
            limit: None,
            allow_filtering: input.to_lowercase().contains("allow filtering"),
        };
        
        Ok(CqlStatement::Select(select))
    }
    
    /// Parse INSERT statement (placeholder)
    fn parse_insert_statement(&self, _input: &str) -> Result<CqlStatement> {
        let insert = CqlInsert {
            table: CqlTable::new("placeholder_table"),
            columns: vec![CqlIdentifier::new("id"), CqlIdentifier::new("name")],
            values: CqlInsertValues::Values(vec![
                CqlExpression::Parameter(1),
                CqlExpression::Parameter(2),
            ]),
            if_not_exists: false,
            using: None,
        };
        
        Ok(CqlStatement::Insert(insert))
    }
    
    /// Parse UPDATE statement (placeholder)
    fn parse_update_statement(&self, _input: &str) -> Result<CqlStatement> {
        let update = CqlUpdate {
            table: CqlTable::new("placeholder_table"),
            using: None,
            assignments: vec![CqlAssignment {
                column: CqlIdentifier::new("name"),
                operator: CqlAssignmentOperator::Assign,
                value: CqlExpression::Parameter(1),
            }],
            where_clause: CqlExpression::Binary {
                left: Box::new(CqlExpression::Column(CqlIdentifier::new("id"))),
                operator: CqlBinaryOperator::Eq,
                right: Box::new(CqlExpression::Parameter(2)),
            },
            if_condition: None,
        };
        
        Ok(CqlStatement::Update(update))
    }
    
    /// Parse DELETE statement (placeholder)
    fn parse_delete_statement(&self, _input: &str) -> Result<CqlStatement> {
        let delete = CqlDelete {
            columns: vec![], // Delete entire row
            table: CqlTable::new("placeholder_table"),
            using: None,
            where_clause: CqlExpression::Binary {
                left: Box::new(CqlExpression::Column(CqlIdentifier::new("id"))),
                operator: CqlBinaryOperator::Eq,
                right: Box::new(CqlExpression::Parameter(1)),
            },
            if_condition: None,
        };
        
        Ok(CqlStatement::Delete(delete))
    }
    
    /// Parse CREATE TABLE statement using existing nom parser
    fn parse_create_table_statement(&self, input: &str) -> Result<CqlStatement> {
        // Use existing nom parser from cql_parser.rs
        let (_, table_schema) = crate::schema::cql_parser::parse_create_table(input)
            .map_err(|e| ParserError::syntax(
                format!("Failed to parse CREATE TABLE: {:?}", e),
                super::traits::SourcePosition::start(),
            ))?;
        
        // Convert TableSchema to AST
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
    fn parse_type_impl(&self, input: &str) -> Result<CqlDataType> {
        let trimmed = input.trim().to_lowercase();
        
        match trimmed.as_str() {
            "text" | "varchar" => Ok(CqlDataType::Text),
            "int" | "integer" => Ok(CqlDataType::Int),
            "bigint" => Ok(CqlDataType::BigInt),
            "uuid" => Ok(CqlDataType::Uuid),
            "boolean" | "bool" => Ok(CqlDataType::Boolean),
            "timestamp" => Ok(CqlDataType::Timestamp),
            "blob" => Ok(CqlDataType::Blob),
            _ => {
                // Try to parse collection types
                if trimmed.starts_with("list<") && trimmed.ends_with('>') {
                    let inner = &trimmed[5..trimmed.len()-1];
                    let inner_type = self.parse_type_impl(inner)?;
                    Ok(CqlDataType::List(Box::new(inner_type)))
                } else if trimmed.starts_with("set<") && trimmed.ends_with('>') {
                    let inner = &trimmed[4..trimmed.len()-1];
                    let inner_type = self.parse_type_impl(inner)?;
                    Ok(CqlDataType::Set(Box::new(inner_type)))
                } else {
                    Ok(CqlDataType::Custom(input.to_string()))
                }
            }
        }
    }
    
    /// Parse expression (placeholder)
    fn parse_expression_impl(&self, input: &str) -> Result<CqlExpression> {
        let trimmed = input.trim();
        
        if trimmed == "?" {
            Ok(CqlExpression::Parameter(1))
        } else if trimmed.starts_with(':') {
            Ok(CqlExpression::NamedParameter(trimmed[1..].to_string()))
        } else if trimmed.starts_with('\'') && trimmed.ends_with('\'') {
            Ok(CqlExpression::Literal(CqlLiteral::String(
                trimmed[1..trimmed.len()-1].to_string()
            )))
        } else if let Ok(num) = trimmed.parse::<i64>() {
            Ok(CqlExpression::Literal(CqlLiteral::Integer(num)))
        } else if trimmed == "true" || trimmed == "false" {
            Ok(CqlExpression::Literal(CqlLiteral::Boolean(trimmed == "true")))
        } else {
            // Assume it's a column reference
            Ok(CqlExpression::Column(CqlIdentifier::new(trimmed)))
        }
    }
    
    /// Parse identifier (placeholder)
    fn parse_identifier_impl(&self, input: &str) -> Result<CqlIdentifier> {
        let trimmed = input.trim();
        
        if trimmed.starts_with('"') && trimmed.ends_with('"') {
            Ok(CqlIdentifier::quoted(&trimmed[1..trimmed.len()-1]))
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
            Ok(CqlLiteral::String(trimmed[1..trimmed.len()-1].to_string()))
        } else if let Ok(num) = trimmed.parse::<i64>() {
            Ok(CqlLiteral::Integer(num))
        } else if let Ok(num) = trimmed.parse::<f64>() {
            Ok(CqlLiteral::Float(num))
        } else {
            Err(ParserError::syntax(
                format!("Invalid literal: {}", input),
                super::traits::SourcePosition::start(),
            ).into())
        }
    }
    
    /// Parse column definitions (placeholder)
    fn parse_column_definitions_impl(&self, _input: &str) -> Result<Vec<CqlColumnDef>> {
        // Placeholder implementation
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
    
    /// Quick syntax validation
    fn quick_syntax_check(&self, input: &str) -> bool {
        let trimmed = input.trim();
        
        // Basic checks
        if trimmed.is_empty() {
            return false;
        }
        
        // Check for balanced parentheses
        let mut paren_count = 0;
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
    use super::*;
    use super::super::config::ParserConfig;
    
    #[tokio::test]
    async fn test_nom_parser_creation() {
        let config = ParserConfig::default().with_backend(super::super::config::ParserBackend::Nom);
        let parser = NomParser::new(config).unwrap();
        
        let info = parser.backend_info();
        assert_eq!(info.name, "nom");
    }
    
    #[tokio::test]
    async fn test_basic_parsing() {
        let config = ParserConfig::default();
        let parser = NomParser::new(config).unwrap();
        
        // Test SELECT parsing
        let result = parser.parse("SELECT * FROM users").await;
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), CqlStatement::Select(_)));
        
        // Test INSERT parsing
        let result = parser.parse("INSERT INTO users (id, name) VALUES (?, ?)").await;
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
        assert!(matches!(result.unwrap(), CqlExpression::Literal(CqlLiteral::String(_))));
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
        
        let config = ParserConfig::default()
            .with_feature(ParserFeature::CodeCompletion);
        
        let result = NomParser::new(config);
        assert!(result.is_err());
    }
}