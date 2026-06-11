//! # Query Statement Parser
//!
//! Lightweight keyword-based parser for CQL DML statements in the query engine.
//!
//! ## Purpose
//!
//! This module parses SELECT, INSERT, UPDATE, and DELETE statements for execution
//! by the M2+ query engine. It uses keyword extraction rather than full AST parsing
//! to provide fast, lightweight query handling.
//!
//! ## Architecture Context
//!
//! This is one of four parsing subsystems in cqlite-core:
//!
//! | Module | Purpose |
//! |--------|---------|
//! | `cql/` | Full CQL text → AST parsing |
//! | `parser/` | SSTable binary format parsing |
//! | `schema/cql_parser.rs` | CREATE TABLE → TableSchema |
//! | **`query/parser.rs`** | DML → ParsedQuery (this module) |
//!
//! See `docs/architecture/parser-overview.md` for the complete architecture overview.
//!
//! ## Key Components
//!
//! - [`QueryParser`] - Main parser struct with `parse()` method
//! - [`M2SelectValidator`](super::m2_select_validator::M2SelectValidator) - Validates
//!   SELECT queries against M2 supported subset
//! - [`ParsedQuery`](super::ParsedQuery) - Structured query representation
//!
//! ## Supported Statements
//!
//! - **SELECT** - With WHERE (equality only for M2), ORDER BY, LIMIT
//! - **INSERT** - Explicit or implicit column syntax
//! - **UPDATE** - SET clause with WHERE
//! - **DELETE** - FROM with WHERE
//! - **CREATE TABLE** / **DROP TABLE** - Table name extraction
//! - **DESCRIBE** / **USE** - Basic keyword parsing
//!
//! ## Example
//!
//! ```rust,ignore
//! use cqlite_core::query::QueryParser;
//!
//! let parser = QueryParser::new(&config);
//! let query = parser.parse("SELECT * FROM users WHERE id = 1 LIMIT 10")?;
//! assert_eq!(query.query_type, QueryType::Select);
//! ```
//!
//! ## M2 Limitations
//!
//! The M2 milestone supports a subset of CQL SELECT:
//! - Partition key equality filters only (`=`)
//! - No range operators (`>`, `<`, `>=`, `<=`)
//! - No aggregates (COUNT, SUM, etc.)
//! - No ALLOW FILTERING
//!
//! For advanced CQL parsing with full AST support, use the [`crate::cql`] module.

// CQL (Cassandra Query Language) Reference:
// https://cassandra.apache.org/doc/latest/cassandra/developing/cql/cql_singlefile.html
//
// This implements CQL v3.4.3+ for Apache Cassandra 5.0+
// CQL is NOT SQL - it's a query language specifically designed for Cassandra's distributed architecture.

use super::{
    m2_select_validator::M2SelectValidator, ComparisonOperator, Condition, OrderByClause,
    ParsedQuery, QueryType, SortDirection, WhereClause,
};
use crate::{Config, Error, Result, TableId, Value};
use std::collections::HashMap;

/// CQL query parser
#[derive(Debug)]
pub struct QueryParser {}

/// Build an empty `ParsedQuery` for the given type, leaving non-applicable fields
/// at their defaults. Callers fill in the type-specific fields.
fn empty_parsed(query_type: QueryType, table: Option<TableId>, cql: &str) -> ParsedQuery {
    ParsedQuery {
        query_type,
        table,
        columns: Vec::new(),
        where_clause: None,
        values: Vec::new(),
        set_clause: HashMap::new(),
        order_by: Vec::new(),
        limit: None,
        cql: cql.to_string(),
    }
}

impl QueryParser {
    /// Create a new query parser
    pub fn new(_config: &Config) -> Self {
        Self {}
    }

    /// Parse a CQL query string
    pub fn parse(&self, cql: &str) -> Result<ParsedQuery> {
        let cql = cql.trim();

        let first_word = cql
            .split_whitespace()
            .next()
            .ok_or_else(|| Error::query_execution("Empty query".to_string()))?;

        // ASCII keyword match — CQL keywords are ASCII so case-insensitive ASCII
        // comparison is sufficient and avoids the allocation of `to_uppercase()`.
        if first_word.eq_ignore_ascii_case("SELECT") {
            self.parse_select(cql)
        } else if first_word.eq_ignore_ascii_case("INSERT") {
            self.parse_insert(cql)
        } else if first_word.eq_ignore_ascii_case("UPDATE") {
            self.parse_update(cql)
        } else if first_word.eq_ignore_ascii_case("DELETE") {
            self.parse_delete(cql)
        } else if first_word.eq_ignore_ascii_case("CREATE") {
            self.parse_create(cql)
        } else if first_word.eq_ignore_ascii_case("DROP") {
            self.parse_drop(cql)
        } else if first_word.eq_ignore_ascii_case("DESCRIBE")
            || first_word.eq_ignore_ascii_case("DESC")
        {
            self.parse_describe(cql)
        } else if first_word.eq_ignore_ascii_case("USE") {
            self.parse_use(cql)
        } else {
            Err(Error::query_execution(format!(
                "Unsupported query type: {}",
                first_word.to_uppercase()
            )))
        }
    }

    /// Parse SELECT statement
    fn parse_select(&self, cql: &str) -> Result<ParsedQuery> {
        // M2: Validate SELECT query against supported subset
        M2SelectValidator.validate_select(cql)?;

        // Build the uppercase copy once; all subsequent keyword scans reuse it.
        let upper = cql.to_uppercase();

        // Extract SELECT columns
        let columns = match extract_between(cql, &upper, "SELECT", "FROM") {
            Some(select_part) => {
                let select_part = select_part.trim();
                if select_part == "*" {
                    vec!["*".to_string()]
                } else {
                    select_part
                        .split(',')
                        .map(|c| c.trim().to_string())
                        .collect()
                }
            }
            None => Vec::new(),
        };

        // Extract table name, preserving the full qualified name (e.g.
        // "test_basic.simple_table") so that SSTableManager::get/scan can look up the
        // correct table_readers entry.  Stripping the keyspace caused point-lookup
        // queries (WHERE pk = ...) to miss the "keyspace.table" registry key and
        // return 0 rows (Issue #680).
        let table = match extract_after(cql, &upper, "FROM") {
            Some(from_part) => {
                let qualified_name = from_part.split_whitespace().next().ok_or_else(|| {
                    Error::query_execution("Missing table name after FROM".to_string())
                })?;
                Some(TableId::new(qualified_name))
            }
            None => None,
        };

        // Extract WHERE — terminates at ORDER BY or LIMIT, whichever comes first.
        let where_clause = extract_clause(cql, &upper, "WHERE", &["ORDER BY", "LIMIT"])
            .map(|s| self.parse_where_clause(s))
            .transpose()?;

        // Extract ORDER BY — terminates at LIMIT.
        let order_by = match extract_clause(cql, &upper, "ORDER BY", &["LIMIT"]) {
            Some(part) => self.parse_order_by(part)?,
            None => Vec::new(),
        };

        // Extract LIMIT clause
        let limit = match extract_after(cql, &upper, "LIMIT") {
            Some(limit_part) => {
                let limit_str = limit_part
                    .split_whitespace()
                    .next()
                    .ok_or_else(|| Error::query_execution("Missing limit value".to_string()))?;
                Some(
                    limit_str
                        .parse()
                        .map_err(|_| Error::query_execution("Invalid limit value".to_string()))?,
                )
            }
            None => None,
        };

        let mut parsed = empty_parsed(QueryType::Select, table, cql);
        parsed.columns = columns;
        parsed.where_clause = where_clause;
        parsed.order_by = order_by;
        parsed.limit = limit;
        Ok(parsed)
    }

    /// Parse INSERT statement
    fn parse_insert(&self, cql: &str) -> Result<ParsedQuery> {
        let upper = cql.to_uppercase();

        // Determine column-list style: explicit `INSERT INTO t (cols) VALUES (...)`
        // versus implicit `INSERT INTO t VALUES (...)`. Explicit if a `(` appears
        // before `VALUES`.
        let paren_pos = cql.find('(');
        let values_pos = upper.find("VALUES").unwrap_or(cql.len());
        let explicit_columns = matches!(paren_pos, Some(p) if p < values_pos);

        let (table, columns) = if explicit_columns {
            let table = extract_between(cql, &upper, "INTO", "(").map(|t| TableId::new(t.trim()));
            let columns = extract_between(cql, &upper, "(", ")")
                .map(|c| c.split(',').map(|col| col.trim().to_string()).collect())
                .unwrap_or_default();
            (table, columns)
        } else {
            // Implicit syntax — columns left empty so the executor falls back to schema.
            let table =
                extract_between(cql, &upper, "INTO", "VALUES").map(|t| TableId::new(t.trim()));
            (table, Vec::new())
        };

        let values = match extract_between(cql, &upper, "VALUES (", ")") {
            Some(values_part) => self.parse_values(values_part)?,
            None => Vec::new(),
        };

        let mut parsed = empty_parsed(QueryType::Insert, table, cql);
        parsed.columns = columns;
        parsed.values = values;
        Ok(parsed)
    }

    /// Parse UPDATE statement
    fn parse_update(&self, cql: &str) -> Result<ParsedQuery> {
        let upper = cql.to_uppercase();

        // Table name is the second whitespace-separated token: `UPDATE <table> ...`
        let table = cql.split_whitespace().nth(1).map(TableId::new);

        // SET runs until WHERE (or end of query).
        let set_clause = match extract_clause(cql, &upper, "SET", &["WHERE"]) {
            Some(part) => self.parse_set_clause(part)?,
            None => HashMap::new(),
        };

        let where_clause = extract_after(cql, &upper, "WHERE")
            .map(|s| self.parse_where_clause(s))
            .transpose()?;

        let mut parsed = empty_parsed(QueryType::Update, table, cql);
        parsed.set_clause = set_clause;
        parsed.where_clause = where_clause;
        Ok(parsed)
    }

    /// Parse DELETE statement
    fn parse_delete(&self, cql: &str) -> Result<ParsedQuery> {
        let upper = cql.to_uppercase();

        let table = extract_clause(cql, &upper, "FROM", &["WHERE"]).map(|t| TableId::new(t.trim()));

        let where_clause = extract_after(cql, &upper, "WHERE")
            .map(|s| self.parse_where_clause(s))
            .transpose()?;

        let mut parsed = empty_parsed(QueryType::Delete, table, cql);
        parsed.where_clause = where_clause;
        Ok(parsed)
    }

    /// Parse CREATE statement
    fn parse_create(&self, cql: &str) -> Result<ParsedQuery> {
        parse_keyword_target(
            cql,
            QueryType::CreateTable,
            "TABLE",
            "Unsupported CREATE statement",
        )
    }

    /// Parse DROP statement
    fn parse_drop(&self, cql: &str) -> Result<ParsedQuery> {
        parse_keyword_target(
            cql,
            QueryType::DropTable,
            "TABLE",
            "Unsupported DROP statement",
        )
    }

    /// Parse DESCRIBE statement
    fn parse_describe(&self, cql: &str) -> Result<ParsedQuery> {
        parse_single_target(cql, QueryType::Describe, "Missing table name for DESCRIBE")
    }

    /// Parse USE statement
    fn parse_use(&self, cql: &str) -> Result<ParsedQuery> {
        // Note: TableId is reused to carry the keyspace name.
        parse_single_target(cql, QueryType::Use, "Missing keyspace name for USE")
    }

    /// Parse WHERE clause
    fn parse_where_clause(&self, where_part: &str) -> Result<WhereClause> {
        let mut conditions = Vec::new();

        // Single-condition parser only — complex expressions are not supported here.
        let parts: Vec<&str> = where_part.split_whitespace().collect();
        if parts.len() >= 3 {
            conditions.push(Condition {
                column: parts[0].to_string(),
                operator: self.parse_operator(parts[1])?,
                value: self.parse_value(parts[2])?,
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
            _ => Err(Error::query_execution(format!("Unknown operator: {}", op))),
        }
    }

    /// Parse a single value
    fn parse_value(&self, value_str: &str) -> Result<Value> {
        let value_str = value_str.trim();

        // String values (single-quoted)
        if value_str.starts_with('\'') && value_str.ends_with('\'') && value_str.len() >= 2 {
            return Ok(Value::Text(value_str[1..value_str.len() - 1].to_string()));
        }

        // Integer values
        if let Ok(int_val) = value_str.parse::<i32>() {
            return Ok(Value::Integer(int_val));
        }

        // Float values
        if let Ok(float_val) = value_str.parse::<f64>() {
            return Ok(Value::Float(float_val));
        }

        // Reserved literals
        if value_str.eq_ignore_ascii_case("TRUE") {
            return Ok(Value::Boolean(true));
        }
        if value_str.eq_ignore_ascii_case("FALSE") {
            return Ok(Value::Boolean(false));
        }
        if value_str.eq_ignore_ascii_case("NULL") {
            return Ok(Value::Null);
        }

        // UUID literals: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx (36 chars, 5 hex groups)
        // This handles both UUID and TIMEUUID columns — parse as Value::Uuid in both cases
        // since the underlying 16-byte representation is identical and compare_values handles it.
        if is_uuid_literal(value_str) {
            if let Some(bytes) = parse_uuid_literal(value_str) {
                return Ok(Value::Uuid(bytes));
            }
        }

        // Default to text
        Ok(Value::Text(value_str.to_string()))
    }

    /// Parse VALUES clause
    fn parse_values(&self, values_part: &str) -> Result<Vec<Value>> {
        values_part
            .split(',')
            .map(|v| self.parse_value(v.trim()))
            .collect()
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
            let parts: Vec<&str> = order_item.split_whitespace().collect();
            if let Some(&col) = parts.first() {
                let direction = if parts.get(1).is_some_and(|d| d.eq_ignore_ascii_case("DESC")) {
                    SortDirection::Desc
                } else {
                    SortDirection::Asc
                };
                order_by.push(OrderByClause {
                    column: col.to_string(),
                    direction,
                });
            }
        }

        Ok(order_by)
    }
}

// ---- Free helpers -----------------------------------------------------------
//
// These helpers operate on a pre-uppercased copy of the query so each parse
// allocates the uppercase buffer only once instead of per-keyword.
// Byte positions in `upper` line up with `text` because `to_uppercase()`
// preserves byte length for ASCII inputs (and CQL keywords are ASCII).

/// Locate the byte slice of `text` that lies between `start` and `end` keywords
/// (case-insensitive). `upper` must equal `text.to_uppercase()`.
fn extract_between<'a>(text: &'a str, upper: &str, start: &str, end: &str) -> Option<&'a str> {
    let start_pos = upper.find(&start.to_uppercase())? + start.len();
    let end_pos = upper[start_pos..].find(&end.to_uppercase())?;
    Some(&text[start_pos..start_pos + end_pos])
}

/// Locate the byte slice of `text` that follows `pattern` (case-insensitive).
fn extract_after<'a>(text: &'a str, upper: &str, pattern: &str) -> Option<&'a str> {
    let start_pos = upper.find(&pattern.to_uppercase())? + pattern.len();
    Some(&text[start_pos..])
}

/// Extract the segment beginning at `start` and terminating at the first of
/// `terminators` to appear, or end-of-string if none do. Falls back to
/// `extract_after` when no terminator matches.
fn extract_clause<'a>(
    text: &'a str,
    upper: &str,
    start: &str,
    terminators: &[&str],
) -> Option<&'a str> {
    for term in terminators {
        if let Some(slice) = extract_between(text, upper, start, term) {
            return Some(slice);
        }
    }
    extract_after(text, upper, start)
}

/// Helper for `CREATE TABLE <name>` / `DROP TABLE <name>` style statements:
/// requires `<verb> <expected_keyword> <target>` and emits the target as the
/// table id.
fn parse_keyword_target(
    cql: &str,
    query_type: QueryType,
    expected_keyword: &str,
    err_msg: &str,
) -> Result<ParsedQuery> {
    let words: Vec<&str> = cql.split_whitespace().collect();
    if words.len() >= 3 && words[1].eq_ignore_ascii_case(expected_keyword) {
        Ok(empty_parsed(query_type, Some(TableId::new(words[2])), cql))
    } else {
        Err(Error::query_execution(err_msg.to_string()))
    }
}

/// Helper for `<verb> <target>` statements (DESCRIBE, USE) that need only the
/// second token as their target.
fn parse_single_target(cql: &str, query_type: QueryType, err_msg: &str) -> Result<ParsedQuery> {
    match cql.split_whitespace().nth(1) {
        Some(name) => Ok(empty_parsed(query_type, Some(TableId::new(name)), cql)),
        None => Err(Error::query_execution(err_msg.to_string())),
    }
}

/// Returns true when `s` has the canonical UUID textual form:
/// `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` (8-4-4-4-12 hex digits).
///
/// This check is intentionally conservative so we do not misclassify text
/// values that happen to look UUID-like. Only the exact 36-character
/// hyphenated form is recognised.
fn is_uuid_literal(s: &str) -> bool {
    if s.len() != 36 {
        return false;
    }
    let bytes = s.as_bytes();
    if bytes[8] != b'-' || bytes[13] != b'-' || bytes[18] != b'-' || bytes[23] != b'-' {
        return false;
    }
    for (i, &b) in bytes.iter().enumerate() {
        if i == 8 || i == 13 || i == 18 || i == 23 {
            continue;
        }
        if !b.is_ascii_hexdigit() {
            return false;
        }
    }
    true
}

/// Parse a UUID literal string (`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`) into
/// 16 raw bytes. Returns `None` if the string is malformed.
fn parse_uuid_literal(s: &str) -> Option<[u8; 16]> {
    // Strip hyphens and decode as 32 hex characters → 16 bytes.
    let hex: String = s.chars().filter(|&c| c != '-').collect();
    if hex.len() != 32 {
        return None;
    }
    let mut bytes = [0u8; 16];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let hi = char::from(chunk[0]).to_digit(16)? as u8;
        let lo = char::from(chunk[1]).to_digit(16)? as u8;
        bytes[i] = (hi << 4) | lo;
    }
    Some(bytes)
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
        #[allow(clippy::approx_constant)]
        {
            assert_eq!(parser.parse_value("3.14").unwrap(), Value::Float(3.14));
        }
        assert_eq!(
            parser.parse_value("'hello'").unwrap(),
            Value::Text("hello".to_string())
        );
        assert_eq!(parser.parse_value("true").unwrap(), Value::Boolean(true));
        assert_eq!(parser.parse_value("NULL").unwrap(), Value::Null);
    }

    #[test]
    fn test_parse_select_with_qualified_table_name() {
        let parser = QueryParser::new(&Config::default());
        let result = parser
            .parse("SELECT * FROM test_basic.simple_table LIMIT 5")
            .unwrap();

        assert_eq!(result.query_type, QueryType::Select);
        // Preserve the full qualified name so SSTableManager can find the table (Issue #680)
        assert_eq!(result.table, Some(TableId::new("test_basic.simple_table")));
        assert_eq!(result.columns, vec!["*"]);
        assert_eq!(result.limit, Some(5));
    }

    #[test]
    fn test_parse_select_with_unqualified_table_name() {
        let parser = QueryParser::new(&Config::default());
        let result = parser.parse("SELECT * FROM simple_table LIMIT 5").unwrap();

        assert_eq!(result.query_type, QueryType::Select);
        // Should work with unqualified names too
        assert_eq!(result.table, Some(TableId::new("simple_table")));
        assert_eq!(result.columns, vec!["*"]);
        assert_eq!(result.limit, Some(5));
    }
}
