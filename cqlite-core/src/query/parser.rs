//! CQL query parser for CQLite
//!
//! This module provides parsing capabilities for CQL (Cassandra Query Language)
//! queries. It converts SQL text into structured query representations that can
//! be executed by the query engine.

use super::{
    ComparisonOperator, Condition, OrderByClause, ParsedQuery, QueryType, SortDirection,
    WhereClause,
};
use crate::{Config, Error, Result, TableId, Value};
use std::collections::HashMap;

/// CQL query parser
pub struct QueryParser {
    /// Configuration
    config: Config,
}

impl QueryParser {
    /// Create a new query parser
    pub fn new(config: &Config) -> Self {
        Self {
            config: config.clone(),
        }
    }

    /// Parse a CQL query string
    pub fn parse(&self, sql: &str) -> Result<ParsedQuery> {
        let sql = sql.trim();

        // Basic keyword-based parsing
        let first_word = sql
            .split_whitespace()
            .next()
            .ok_or_else(|| Error::InvalidQuery("Empty query".to_string()))?
            .to_uppercase();

        match first_word.as_str() {
            "SELECT" => self.parse_select(sql),
            "INSERT" => self.parse_insert(sql),
            "UPDATE" => self.parse_update(sql),
            "DELETE" => self.parse_delete(sql),
            "CREATE" => self.parse_create(sql),
            "DROP" => self.parse_drop(sql),
            "DESCRIBE" | "DESC" => self.parse_describe(sql),
            "USE" => self.parse_use(sql),
            _ => Err(Error::InvalidQuery(format!(
                "Unsupported query type: {}",
                first_word
            ))),
        }
    }

    /// Parse SELECT statement
    fn parse_select(&self, sql: &str) -> Result<ParsedQuery> {
        let mut columns = Vec::new();
        let mut table = None;
        let mut where_clause = None;
        let mut order_by = Vec::new();
        let mut limit = None;

        // Simple regex-based parsing for demonstration
        // In a real implementation, this would use a proper parser like nom or pest

        // Extract SELECT columns
        if let Some(select_part) = self.extract_between(sql, "SELECT", "FROM") {
            let select_part = select_part.trim();
            if select_part == "*" {
                columns.push("*".to_string());
            } else {
                columns = select_part
                    .split(',')
                    .map(|col| col.trim().to_string())
                    .collect();
            }
        }

        // Extract table name
        if let Some(from_part) = self.extract_after(sql, "FROM") {
            let table_name = from_part
                .split_whitespace()
                .next()
                .ok_or_else(|| Error::InvalidQuery("Missing table name after FROM".to_string()))?;
            table = Some(TableId::new(table_name));
        }

        // Extract WHERE clause
        if let Some(where_part) = self.extract_between(sql, "WHERE", "ORDER BY") {
            where_clause = Some(self.parse_where_clause(where_part)?);
        } else if let Some(where_part) = self.extract_between(sql, "WHERE", "LIMIT") {
            where_clause = Some(self.parse_where_clause(where_part)?);
        } else if let Some(where_part) = self.extract_after(sql, "WHERE") {
            where_clause = Some(self.parse_where_clause(where_part)?);
        }

        // Extract ORDER BY clause
        if let Some(order_part) = self.extract_between(sql, "ORDER BY", "LIMIT") {
            order_by = self.parse_order_by(order_part)?;
        } else if let Some(order_part) = self.extract_after(sql, "ORDER BY") {
            order_by = self.parse_order_by(order_part)?;
        }

        // Extract LIMIT clause
        if let Some(limit_part) = self.extract_after(sql, "LIMIT") {
            let limit_str = limit_part
                .trim()
                .split_whitespace()
                .next()
                .ok_or_else(|| Error::InvalidQuery("Missing limit value".to_string()))?;
            limit = Some(
                limit_str
                    .parse()
                    .map_err(|_| Error::InvalidQuery("Invalid limit value".to_string()))?,
            );
        }

        Ok(ParsedQuery {
            query_type: QueryType::Select,
            table,
            columns,
            where_clause,
            values: Vec::new(),
            set_clause: HashMap::new(),
            order_by,
            limit,
            sql: sql.to_string(),
        })
    }

    /// Parse INSERT statement
    fn parse_insert(&self, sql: &str) -> Result<ParsedQuery> {
        let mut table = None;
        let mut columns = Vec::new();
        let mut values = Vec::new();

        // Extract table name
        if let Some(table_part) = self.extract_between(sql, "INTO", "(") {
            let table_name = table_part.trim();
            table = Some(TableId::new(table_name));
        }

        // Extract columns
        if let Some(columns_part) = self.extract_between(sql, "(", ")") {
            columns = columns_part
                .split(',')
                .map(|col| col.trim().to_string())
                .collect();
        }

        // Extract values
        if let Some(values_part) = self.extract_between(sql, "VALUES (", ")") {
            values = self.parse_values(values_part)?;
        }

        Ok(ParsedQuery {
            query_type: QueryType::Insert,
            table,
            columns,
            where_clause: None,
            values,
            set_clause: HashMap::new(),
            order_by: Vec::new(),
            limit: None,
            sql: sql.to_string(),
        })
    }

    /// Parse UPDATE statement
    fn parse_update(&self, sql: &str) -> Result<ParsedQuery> {
        let mut table = None;
        let mut set_clause = HashMap::new();
        let mut where_clause = None;

        // Extract table name
        let words: Vec<&str> = sql.split_whitespace().collect();
        if words.len() >= 2 {
            table = Some(TableId::new(words[1]));
        }

        // Extract SET clause
        if let Some(set_part) = self.extract_between(sql, "SET", "WHERE") {
            set_clause = self.parse_set_clause(set_part)?;
        } else if let Some(set_part) = self.extract_after(sql, "SET") {
            set_clause = self.parse_set_clause(set_part)?;
        }

        // Extract WHERE clause
        if let Some(where_part) = self.extract_after(sql, "WHERE") {
            where_clause = Some(self.parse_where_clause(where_part)?);
        }

        Ok(ParsedQuery {
            query_type: QueryType::Update,
            table,
            columns: Vec::new(),
            where_clause,
            values: Vec::new(),
            set_clause,
            order_by: Vec::new(),
            limit: None,
            sql: sql.to_string(),
        })
    }

    /// Parse DELETE statement
    fn parse_delete(&self, sql: &str) -> Result<ParsedQuery> {
        let mut table = None;
        let mut where_clause = None;

        // Extract table name
        if let Some(table_part) = self.extract_between(sql, "FROM", "WHERE") {
            let table_name = table_part.trim();
            table = Some(TableId::new(table_name));
        } else if let Some(table_part) = self.extract_after(sql, "FROM") {
            let table_name = table_part.trim();
            table = Some(TableId::new(table_name));
        }

        // Extract WHERE clause
        if let Some(where_part) = self.extract_after(sql, "WHERE") {
            where_clause = Some(self.parse_where_clause(where_part)?);
        }

        Ok(ParsedQuery {
            query_type: QueryType::Delete,
            table,
            columns: Vec::new(),
            where_clause,
            values: Vec::new(),
            set_clause: HashMap::new(),
            order_by: Vec::new(),
            limit: None,
            sql: sql.to_string(),
        })
    }

    /// Parse CREATE statement
    fn parse_create(&self, sql: &str) -> Result<ParsedQuery> {
        let words: Vec<&str> = sql.split_whitespace().collect();
        if words.len() >= 3 && words[1].to_uppercase() == "TABLE" {
            Ok(ParsedQuery {
                query_type: QueryType::CreateTable,
                table: Some(TableId::new(words[2])),
                columns: Vec::new(),
                where_clause: None,
                values: Vec::new(),
                set_clause: HashMap::new(),
                order_by: Vec::new(),
                limit: None,
                sql: sql.to_string(),
            })
        } else {
            Err(Error::InvalidQuery(
                "Unsupported CREATE statement".to_string(),
            ))
        }
    }

    /// Parse DROP statement
    fn parse_drop(&self, sql: &str) -> Result<ParsedQuery> {
        let words: Vec<&str> = sql.split_whitespace().collect();
        if words.len() >= 3 && words[1].to_uppercase() == "TABLE" {
            Ok(ParsedQuery {
                query_type: QueryType::DropTable,
                table: Some(TableId::new(words[2])),
                columns: Vec::new(),
                where_clause: None,
                values: Vec::new(),
                set_clause: HashMap::new(),
                order_by: Vec::new(),
                limit: None,
                sql: sql.to_string(),
            })
        } else {
            Err(Error::InvalidQuery(
                "Unsupported DROP statement".to_string(),
            ))
        }
    }

    /// Parse DESCRIBE statement
    fn parse_describe(&self, sql: &str) -> Result<ParsedQuery> {
        let words: Vec<&str> = sql.split_whitespace().collect();
        if words.len() >= 2 {
            Ok(ParsedQuery {
                query_type: QueryType::Describe,
                table: Some(TableId::new(words[1])),
                columns: Vec::new(),
                where_clause: None,
                values: Vec::new(),
                set_clause: HashMap::new(),
                order_by: Vec::new(),
                limit: None,
                sql: sql.to_string(),
            })
        } else {
            Err(Error::InvalidQuery(
                "Missing table name for DESCRIBE".to_string(),
            ))
        }
    }

    /// Parse USE statement
    fn parse_use(&self, sql: &str) -> Result<ParsedQuery> {
        let words: Vec<&str> = sql.split_whitespace().collect();
        if words.len() >= 2 {
            Ok(ParsedQuery {
                query_type: QueryType::Use,
                table: Some(TableId::new(words[1])), // Using table for keyspace name
                columns: Vec::new(),
                where_clause: None,
                values: Vec::new(),
                set_clause: HashMap::new(),
                order_by: Vec::new(),
                limit: None,
                sql: sql.to_string(),
            })
        } else {
            Err(Error::InvalidQuery(
                "Missing keyspace name for USE".to_string(),
            ))
        }
    }

    /// Parse WHERE clause
    fn parse_where_clause(&self, where_part: &str) -> Result<WhereClause> {
        let mut conditions = Vec::new();

        // Simple parsing for a single condition
        // In a real implementation, this would handle complex expressions
        let parts: Vec<&str> = where_part.split_whitespace().collect();
        if parts.len() >= 3 {
            let column = parts[0].to_string();
            let operator = self.parse_operator(parts[1])?;
            let value = self.parse_value(parts[2])?;

            conditions.push(Condition {
                column,
                operator,
                value,
            });
        }

        Ok(WhereClause { conditions })
    }

    /// Parse comparison operator
    fn parse_operator(&self, op: &str) -> Result<ComparisonOperator> {
        match op {
            "=" => Ok(ComparisonOperator::Equal),
            "<>" | "!=" => Ok(ComparisonOperator::NotEqual),
            "<" => Ok(ComparisonOperator::LessThan),
            "<=" => Ok(ComparisonOperator::LessThanOrEqual),
            ">" => Ok(ComparisonOperator::GreaterThan),
            ">=" => Ok(ComparisonOperator::GreaterThanOrEqual),
            "IN" => Ok(ComparisonOperator::In),
            "LIKE" => Ok(ComparisonOperator::Like),
            _ => Err(Error::InvalidQuery(format!("Unknown operator: {}", op))),
        }
    }

    /// Parse a single value
    fn parse_value(&self, value_str: &str) -> Result<Value> {
        let value_str = value_str.trim();

        // String values (quoted)
        if value_str.starts_with('\'') && value_str.ends_with('\'') {
            let content = &value_str[1..value_str.len() - 1];
            return Ok(Value::Text(content.to_string()));
        }

        // Integer values
        if let Ok(int_val) = value_str.parse::<i32>() {
            return Ok(Value::Integer(int_val));
        }

        // Float values
        if let Ok(float_val) = value_str.parse::<f64>() {
            return Ok(Value::Float(float_val));
        }

        // Boolean values
        match value_str.to_uppercase().as_str() {
            "TRUE" => return Ok(Value::Boolean(true)),
            "FALSE" => return Ok(Value::Boolean(false)),
            "NULL" => return Ok(Value::Null),
            _ => {}
        }

        // Default to text
        Ok(Value::Text(value_str.to_string()))
    }

    /// Parse VALUES clause
    fn parse_values(&self, values_part: &str) -> Result<Vec<Value>> {
        let values: Result<Vec<Value>> = values_part
            .split(',')
            .map(|v| self.parse_value(v.trim()))
            .collect();
        values
    }

    /// Parse SET clause
    fn parse_set_clause(&self, set_part: &str) -> Result<HashMap<String, Value>> {
        let mut set_clause = HashMap::new();

        for assignment in set_part.split(',') {
            let parts: Vec<&str> = assignment.split('=').collect();
            if parts.len() == 2 {
                let column = parts[0].trim().to_string();
                let value = self.parse_value(parts[1].trim())?;
                set_clause.insert(column, value);
            }
        }

        Ok(set_clause)
    }

    /// Parse ORDER BY clause
    fn parse_order_by(&self, order_part: &str) -> Result<Vec<OrderByClause>> {
        let mut order_by = Vec::new();

        for order_item in order_part.split(',') {
            let parts: Vec<&str> = order_item.trim().split_whitespace().collect();
            if !parts.is_empty() {
                let column = parts[0].to_string();
                let direction = if parts.len() > 1 && parts[1].to_uppercase() == "DESC" {
                    SortDirection::Desc
                } else {
                    SortDirection::Asc
                };

                order_by.push(OrderByClause { column, direction });
            }
        }

        Ok(order_by)
    }

    /// Helper: Extract text between two patterns
    fn extract_between(&self, text: &str, start: &str, end: &str) -> Option<&str> {
        let start_pos = text.to_uppercase().find(&start.to_uppercase())? + start.len();
        let end_pos = text.to_uppercase()[start_pos..].find(&end.to_uppercase())?;
        Some(&text[start_pos..start_pos + end_pos])
    }

    /// Helper: Extract text after a pattern
    fn extract_after(&self, text: &str, pattern: &str) -> Option<&str> {
        let start_pos = text.to_uppercase().find(&pattern.to_uppercase())? + pattern.len();
        Some(&text[start_pos..])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_select_basic() {
        let parser = QueryParser::new(&Config::default());
        let result = parser.parse("SELECT * FROM users").unwrap();

        assert_eq!(result.query_type, QueryType::Select);
        assert_eq!(result.table, Some(TableId::new("users")));
        assert_eq!(result.columns, vec!["*"]);
    }

    #[test]
    fn test_parse_select_with_columns() {
        let parser = QueryParser::new(&Config::default());
        let result = parser.parse("SELECT id, name FROM users").unwrap();

        assert_eq!(result.query_type, QueryType::Select);
        assert_eq!(result.columns, vec!["id", "name"]);
    }

    #[test]
    fn test_parse_select_with_where() {
        let parser = QueryParser::new(&Config::default());
        let result = parser.parse("SELECT * FROM users WHERE id = 1").unwrap();

        assert_eq!(result.query_type, QueryType::Select);
        assert!(result.where_clause.is_some());

        let where_clause = result.where_clause.unwrap();
        assert_eq!(where_clause.conditions.len(), 1);
        assert_eq!(where_clause.conditions[0].column, "id");
        assert_eq!(
            where_clause.conditions[0].operator,
            ComparisonOperator::Equal
        );
    }

    #[test]
    fn test_parse_insert() {
        let parser = QueryParser::new(&Config::default());
        let result = parser
            .parse("INSERT INTO users (id, name) VALUES (1, 'Alice')")
            .unwrap();

        assert_eq!(result.query_type, QueryType::Insert);
        assert_eq!(result.table, Some(TableId::new("users")));
        assert_eq!(result.columns, vec!["id", "name"]);
        assert_eq!(result.values.len(), 2);
    }

    #[test]
    fn test_parse_update() {
        let parser = QueryParser::new(&Config::default());
        let result = parser
            .parse("UPDATE users SET name = 'Bob' WHERE id = 1")
            .unwrap();

        assert_eq!(result.query_type, QueryType::Update);
        assert_eq!(result.table, Some(TableId::new("users")));
        assert!(!result.set_clause.is_empty());
        assert!(result.where_clause.is_some());
    }

    #[test]
    fn test_parse_delete() {
        let parser = QueryParser::new(&Config::default());
        let result = parser.parse("DELETE FROM users WHERE id = 1").unwrap();

        assert_eq!(result.query_type, QueryType::Delete);
        assert_eq!(result.table, Some(TableId::new("users")));
        assert!(result.where_clause.is_some());
    }

    #[test]
    fn test_parse_value_types() {
        let parser = QueryParser::new(&Config::default());

        assert_eq!(parser.parse_value("123").unwrap(), Value::Integer(123));
        assert_eq!(parser.parse_value("3.14").unwrap(), Value::Float(3.14));
        assert_eq!(
            parser.parse_value("'hello'").unwrap(),
            Value::Text("hello".to_string())
        );
        assert_eq!(parser.parse_value("true").unwrap(), Value::Boolean(true));
        assert_eq!(parser.parse_value("NULL").unwrap(), Value::Null);
    }
}
