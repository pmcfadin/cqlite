//! Advanced CQL SELECT Parser
//!
//! This module implements the FIRST EVER CQL SELECT parser for direct SSTable access.
//! It provides comprehensive parsing for complex SELECT statements including:
//! - Advanced WHERE clauses with all operators
//! - Aggregation functions and GROUP BY
//! - ORDER BY and LIMIT clauses
//! - Collection operations
//! - Subqueries and JOINs (future)

// CQL (Cassandra Query Language) Reference:
// https://cassandra.apache.org/doc/latest/cassandra/developing/cql/cql_singlefile.html
//
// This implements CQL v3.4.3+ for Apache Cassandra 5.0+
// CQL is NOT SQL - it's a query language specifically designed for Cassandra's distributed architecture.

use super::select_ast::*;
use crate::{Error, Result, TableId, Value};

// WRITETIME and TTL are reserved words in CQL that introduce a special
// single-argument metadata-retrieval form. They are handled as dedicated tokens
// so the parser can produce a first-class `SelectExpression::WriteTimeTtl`
// rather than falling through to the generic `FunctionCall` path.

/// Advanced CQL SELECT parser
#[derive(Debug)]
pub struct SelectParser {
    /// Current token being parsed (always `Some` after construction; `Token::Eof` marks end)
    current_token: Option<Token>,
    /// Tokenizer for the input
    tokenizer: Tokenizer,
}

/// Token types for CQL parsing
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Keywords
    Select,
    Distinct,
    From,
    Where,
    GroupBy,
    Having,
    OrderBy,
    Limit,
    PerPartitionLimit,
    Offset,
    And,
    Or,
    Not,
    Like,
    In,
    Between,
    As,
    Asc,
    Desc,
    Allow,
    Filtering,
    Count,
    Sum,
    Avg,
    Min,
    Max,
    // JOIN operations are NOT supported in Cassandra CQL
    Is,
    Null,
    Contains,
    Key,
    // Metadata-retrieval functions (CQL reserved words)
    Writetime,
    Ttl,

    // Operators
    Equal,            // =
    NotEqual,         // != or <>
    LessThan,         // <
    LessThanEqual,    // <=
    GreaterThan,      // >
    GreaterThanEqual, // >=
    Plus,             // +
    Minus,            // -
    Multiply,         // *
    Divide,           // /
    Modulo,           // %

    // Literals
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),

    // Identifiers
    Identifier(String),

    // Punctuation
    LeftParen,    // (
    RightParen,   // )
    LeftBracket,  // [
    RightBracket, // ]
    LeftBrace,    // {
    RightBrace,   // }
    Comma,        // ,
    Semicolon,    // ;
    Dot,          // .
    Question,     // ? (for parameters)

    // Special
    Eof,
    Newline,
    Whitespace,
}

/// Map an already-read identifier to its keyword token, or `None` for a plain identifier.
///
/// Uses ASCII-case-insensitive comparison so we never have to allocate an
/// uppercase copy of the source text.
fn keyword_for(ident: &str) -> Option<Token> {
    // Sorted roughly by expected frequency / first-letter group for readability.
    const KEYWORDS: &[(&str, Token)] = &[
        ("SELECT", Token::Select),
        ("DISTINCT", Token::Distinct),
        ("FROM", Token::From),
        ("WHERE", Token::Where),
        ("HAVING", Token::Having),
        ("LIMIT", Token::Limit),
        ("OFFSET", Token::Offset),
        ("AND", Token::And),
        ("OR", Token::Or),
        ("NOT", Token::Not),
        ("LIKE", Token::Like),
        ("IN", Token::In),
        ("BETWEEN", Token::Between),
        ("AS", Token::As),
        ("ASC", Token::Asc),
        ("DESC", Token::Desc),
        ("ALLOW", Token::Allow),
        ("FILTERING", Token::Filtering),
        ("COUNT", Token::Count),
        ("SUM", Token::Sum),
        ("AVG", Token::Avg),
        ("MIN", Token::Min),
        ("MAX", Token::Max),
        ("IS", Token::Is),
        ("NULL", Token::Null),
        ("CONTAINS", Token::Contains),
        ("KEY", Token::Key),
        ("WRITETIME", Token::Writetime),
        ("TTL", Token::Ttl),
        ("TRUE", Token::Boolean(true)),
        ("FALSE", Token::Boolean(false)),
    ];

    KEYWORDS
        .iter()
        .find(|(kw, _)| ident.eq_ignore_ascii_case(kw))
        .map(|(_, tok)| tok.clone())
}

/// Map an aggregate keyword token to its AST type, if any.
fn aggregate_for(token: &Token) -> Option<AggregateType> {
    match token {
        Token::Count => Some(AggregateType::Count),
        Token::Sum => Some(AggregateType::Sum),
        Token::Avg => Some(AggregateType::Avg),
        Token::Min => Some(AggregateType::Min),
        Token::Max => Some(AggregateType::Max),
        _ => None,
    }
}

/// Simple tokenizer for CQL
#[derive(Debug)]
pub struct Tokenizer {
    input: Vec<char>,
    position: usize,
    current_char: Option<char>,
}

impl Tokenizer {
    pub fn new(input: &str) -> Self {
        let chars: Vec<char> = input.chars().collect();
        let current_char = chars.first().copied();

        Self {
            input: chars,
            position: 0,
            current_char,
        }
    }

    fn advance(&mut self) {
        self.position += 1;
        self.current_char = self.input.get(self.position).copied();
    }

    fn peek(&self) -> Option<char> {
        self.input.get(self.position + 1).copied()
    }

    fn skip_whitespace(&mut self) {
        while let Some(ch) = self.current_char {
            if ch.is_whitespace() {
                self.advance();
            } else {
                break;
            }
        }
    }

    fn read_string(&mut self, quote_char: char) -> Result<String> {
        let mut value = String::new();
        self.advance(); // Skip opening quote

        while let Some(ch) = self.current_char {
            if ch == quote_char {
                self.advance(); // Skip closing quote
                return Ok(value);
            } else if ch == '\\' {
                self.advance();
                if let Some(escaped) = self.current_char {
                    let mapped = match escaped {
                        'n' => Some('\n'),
                        't' => Some('\t'),
                        'r' => Some('\r'),
                        '\\' => Some('\\'),
                        '\'' => Some('\''),
                        '"' => Some('"'),
                        _ => None,
                    };
                    match mapped {
                        Some(c) => value.push(c),
                        None => {
                            value.push('\\');
                            value.push(escaped);
                        }
                    }
                    self.advance();
                }
            } else {
                value.push(ch);
                self.advance();
            }
        }

        Err(Error::cql_parse("Unterminated string literal"))
    }

    fn read_number(&mut self) -> Result<Token> {
        let mut value = String::new();
        let mut has_dot = false;

        while let Some(ch) = self.current_char {
            if ch.is_ascii_digit() {
                value.push(ch);
                self.advance();
            } else if ch == '.' && !has_dot {
                has_dot = true;
                value.push(ch);
                self.advance();
            } else {
                break;
            }
        }

        if has_dot {
            value
                .parse::<f64>()
                .map(Token::Float)
                .map_err(|_| Error::cql_parse(format!("Invalid float: {}", value)))
        } else {
            value
                .parse::<i64>()
                .map(Token::Integer)
                .map_err(|_| Error::cql_parse(format!("Invalid integer: {}", value)))
        }
    }

    fn read_identifier(&mut self) -> String {
        let mut value = String::new();

        while let Some(ch) = self.current_char {
            if ch.is_alphanumeric() || ch == '_' {
                value.push(ch);
                self.advance();
            } else {
                break;
            }
        }

        value
    }

    /// Consume the literal keyword `BY` (case-insensitive) following GROUP/ORDER.
    fn expect_by_keyword(&mut self, after: &str) -> Result<()> {
        self.expect_keyword_word("BY", after)
    }

    /// Consume the literal `word` (case-insensitive) as the next identifier,
    /// erroring if it is absent. Used to resolve multi-word keywords (e.g. the
    /// `PARTITION`/`LIMIT` words of `PER PARTITION LIMIT`).
    fn expect_keyword_word(&mut self, word: &str, after: &str) -> Result<()> {
        self.skip_whitespace();
        let next = self.read_identifier();
        if next.eq_ignore_ascii_case(word) {
            Ok(())
        } else {
            Err(Error::cql_parse(format!(
                "Expected {} after {}",
                word, after
            )))
        }
    }

    pub fn next_token(&mut self) -> Result<Token> {
        loop {
            let ch = match self.current_char {
                None => return Ok(Token::Eof),
                Some(c) => c,
            };

            // Single-character punctuation / operators that don't need lookahead.
            let single = match ch {
                '(' => Some(Token::LeftParen),
                ')' => Some(Token::RightParen),
                '[' => Some(Token::LeftBracket),
                ']' => Some(Token::RightBracket),
                '{' => Some(Token::LeftBrace),
                '}' => Some(Token::RightBrace),
                ',' => Some(Token::Comma),
                ';' => Some(Token::Semicolon),
                '.' => Some(Token::Dot),
                '?' => Some(Token::Question),
                '+' => Some(Token::Plus),
                '-' => Some(Token::Minus),
                '*' => Some(Token::Multiply),
                '/' => Some(Token::Divide),
                '%' => Some(Token::Modulo),
                '=' => Some(Token::Equal),
                _ => None,
            };
            if let Some(tok) = single {
                self.advance();
                return Ok(tok);
            }

            match ch {
                c if c.is_whitespace() => self.skip_whitespace(),
                '!' => {
                    if self.peek() == Some('=') {
                        self.advance();
                        self.advance();
                        return Ok(Token::NotEqual);
                    }
                    return Err(Error::cql_parse("Unexpected character: !"));
                }
                '<' => {
                    return Ok(match self.peek() {
                        Some('=') => {
                            self.advance();
                            self.advance();
                            Token::LessThanEqual
                        }
                        Some('>') => {
                            self.advance();
                            self.advance();
                            Token::NotEqual
                        }
                        _ => {
                            self.advance();
                            Token::LessThan
                        }
                    });
                }
                '>' => {
                    return Ok(if self.peek() == Some('=') {
                        self.advance();
                        self.advance();
                        Token::GreaterThanEqual
                    } else {
                        self.advance();
                        Token::GreaterThan
                    });
                }
                '\'' | '"' => return self.read_string(ch).map(Token::String),
                c if c.is_ascii_digit() => return self.read_number(),
                c if c.is_alphabetic() || c == '_' => {
                    let identifier = self.read_identifier();
                    // GROUP BY / ORDER BY are two-word keywords; resolve here so
                    // the parser only ever sees a single GroupBy / OrderBy token.
                    if identifier.eq_ignore_ascii_case("GROUP") {
                        self.expect_by_keyword("GROUP")?;
                        return Ok(Token::GroupBy);
                    }
                    if identifier.eq_ignore_ascii_case("ORDER") {
                        self.expect_by_keyword("ORDER")?;
                        return Ok(Token::OrderBy);
                    }
                    // PER PARTITION LIMIT is a three-word keyword; resolve it
                    // here so the parser only ever sees a single token.
                    if identifier.eq_ignore_ascii_case("PER") {
                        self.expect_keyword_word("PARTITION", "PER")?;
                        self.expect_keyword_word("LIMIT", "PER PARTITION")?;
                        return Ok(Token::PerPartitionLimit);
                    }
                    return Ok(keyword_for(&identifier).unwrap_or(Token::Identifier(identifier)));
                }
                other => return Err(Error::cql_parse(format!("Unexpected character: {}", other))),
            }
        }
    }
}

impl SelectParser {
    /// Create a new SELECT parser
    pub fn new(cql: &str) -> Result<Self> {
        let mut tokenizer = Tokenizer::new(cql);
        let current_token = Some(tokenizer.next_token()?);
        Ok(Self {
            current_token,
            tokenizer,
        })
    }

    /// Advance to the next token
    fn advance(&mut self) -> Result<()> {
        self.current_token = Some(self.tokenizer.next_token()?);
        Ok(())
    }

    /// Borrow the current token. Returns `&Token::Eof` if for some reason the
    /// stream is exhausted (in practice `current_token` is always `Some`).
    fn peek(&self) -> &Token {
        self.current_token.as_ref().unwrap_or(&Token::Eof)
    }

    /// True if the current token equals `tok` (by discriminant, not payload).
    fn at(&self, tok: &Token) -> bool {
        self.current_token
            .as_ref()
            .is_some_and(|cur| std::mem::discriminant(cur) == std::mem::discriminant(tok))
    }

    /// Consume the current token if it matches `tok` (by discriminant); return whether it did.
    fn eat(&mut self, tok: &Token) -> Result<bool> {
        if self.at(tok) {
            self.advance()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Check if current token matches expected token
    fn expect(&mut self, expected: Token) -> Result<()> {
        if let Some(ref current) = self.current_token {
            if std::mem::discriminant(current) == std::mem::discriminant(&expected) {
                self.advance()?;
                Ok(())
            } else {
                Err(Error::cql_parse(format!(
                    "Expected {:?}, found {:?}",
                    expected, current
                )))
            }
        } else {
            Err(Error::cql_parse("Unexpected end of input"))
        }
    }

    /// Consume an integer literal token (used by LIMIT and OFFSET parsers).
    fn expect_integer(&mut self, context: &str) -> Result<i64> {
        if let Some(Token::Integer(n)) = self.current_token {
            self.advance()?;
            Ok(n)
        } else {
            Err(Error::cql_parse(format!(
                "Expected integer after {}",
                context
            )))
        }
    }

    /// Parse `name` or `name . column` into a [`ColumnRef`], assuming the
    /// current token is the leading identifier.
    fn parse_column_ref(&mut self, table_or_column: String) -> Result<ColumnRef> {
        // Caller has already consumed the leading identifier.
        if !self.eat(&Token::Dot)? {
            return Ok(ColumnRef::new(table_or_column));
        }
        if let Some(Token::Identifier(column)) = self.current_token.clone() {
            self.advance()?;
            Ok(ColumnRef::qualified(table_or_column, column))
        } else {
            Err(Error::cql_parse(
                "Expected column name after table qualifier",
            ))
        }
    }

    /// Parse a complete SELECT statement
    pub fn parse_select_statement(&mut self) -> Result<SelectStatement> {
        self.expect(Token::Select)?;
        let select_clause = self.parse_select_clause()?;

        let from_clause = if self.eat(&Token::From)? {
            Some(self.parse_from_clause()?)
        } else {
            None
        };

        let where_clause = if self.eat(&Token::Where)? {
            Some(self.parse_where_expression()?)
        } else {
            None
        };

        let group_by = if self.eat(&Token::GroupBy)? {
            Some(self.parse_group_by_clause()?)
        } else {
            None
        };

        let having_clause = if self.eat(&Token::Having)? {
            Some(self.parse_where_expression()?)
        } else {
            None
        };

        let order_by = if self.eat(&Token::OrderBy)? {
            Some(self.parse_order_by_clause()?)
        } else {
            None
        };

        // PER PARTITION LIMIT precedes the query-wide LIMIT in CQL grammar.
        let per_partition_limit = if self.eat(&Token::PerPartitionLimit)? {
            Some(self.parse_positive_limit("PER PARTITION LIMIT")?)
        } else {
            None
        };

        let limit = if self.eat(&Token::Limit)? {
            Some(self.parse_limit_clause()?)
        } else {
            None
        };

        let offset = if self.eat(&Token::Offset)? {
            Some(self.expect_integer("OFFSET")? as u64)
        } else {
            None
        };

        let allow_filtering = if self.eat(&Token::Allow)? {
            self.expect(Token::Filtering)?;
            true
        } else {
            false
        };

        // PER PARTITION LIMIT must precede LIMIT (and any trailing OFFSET/ALLOW
        // FILTERING). Checking here — after every trailing clause is consumed —
        // rejects all mis-orderings instead of silently ignoring the clause,
        // including `LIMIT n OFFSET m PER PARTITION LIMIT k` (roborev job 38).
        if self.at(&Token::PerPartitionLimit) {
            return Err(Error::cql_parse(
                "PER PARTITION LIMIT must appear before LIMIT",
            ));
        }

        Ok(SelectStatement {
            select_clause,
            from_clause,
            where_clause,
            group_by,
            having_clause,
            order_by,
            limit,
            per_partition_limit,
            offset,
            allow_filtering,
        })
    }

    /// Parse SELECT clause
    fn parse_select_clause(&mut self) -> Result<SelectClause> {
        let distinct = self.eat(&Token::Distinct)?;

        if self.eat(&Token::Multiply)? {
            return Ok(SelectClause::All);
        }

        let mut expressions = Vec::new();
        loop {
            expressions.push(self.parse_select_expression()?);
            if !self.eat(&Token::Comma)? {
                break;
            }
        }

        if distinct {
            Ok(SelectClause::Distinct(expressions))
        } else {
            Ok(SelectClause::Columns(expressions))
        }
    }

    /// Parse a single SELECT expression
    fn parse_select_expression(&mut self) -> Result<SelectExpression> {
        let expr = self.parse_primary_expression()?;

        // Check for AS alias
        if self.eat(&Token::As)? {
            if let Some(Token::Identifier(alias)) = self.current_token.clone() {
                self.advance()?;
                return Ok(SelectExpression::Aliased(Box::new(expr), alias));
            }
            return Err(Error::cql_parse("Expected alias name after AS"));
        }

        Ok(expr)
    }

    /// Parse primary expression (column, function, literal, etc.)
    fn parse_primary_expression(&mut self) -> Result<SelectExpression> {
        if let Some(agg) = aggregate_for(self.peek()) {
            self.advance()?;
            return self.parse_aggregate_function(agg);
        }

        // WRITETIME(col) and TTL(col) — first-class metadata-retrieval functions.
        // They tokenize as dedicated keywords so they are caught here before the
        // generic identifier path.
        if matches!(self.peek(), Token::Writetime | Token::Ttl) {
            let function = match self.current_token.clone() {
                Some(Token::Writetime) => WriteTimeTtlFunction::WriteTime,
                Some(Token::Ttl) => WriteTimeTtlFunction::Ttl,
                _ => unreachable!("peek guard ensures only Writetime or Ttl here"),
            };
            self.advance()?;
            return self.parse_writetime_ttl_call(function);
        }

        // Take ownership/copy of literal payloads up front so we can call &mut self.
        match self.current_token.clone() {
            Some(Token::Identifier(name)) => {
                self.advance()?;

                // Function call: identifier ( args )
                if self.eat(&Token::LeftParen)? {
                    let mut args = Vec::new();
                    if !self.at(&Token::RightParen) {
                        loop {
                            args.push(self.parse_select_expression()?);
                            if !self.eat(&Token::Comma)? {
                                break;
                            }
                        }
                    }
                    self.expect(Token::RightParen)?;
                    return Ok(SelectExpression::Function(FunctionCall { name, args }));
                }

                // Either bare column or qualified table.column.
                let col = self.parse_column_ref(name)?;
                Ok(SelectExpression::Column(col))
            }
            Some(Token::Integer(n)) => {
                self.advance()?;
                Ok(SelectExpression::Literal(Value::BigInt(n)))
            }
            Some(Token::Float(f)) => {
                self.advance()?;
                Ok(SelectExpression::Literal(Value::Float(f)))
            }
            Some(Token::String(s)) => {
                self.advance()?;
                Ok(SelectExpression::Literal(Value::Text(s)))
            }
            Some(Token::Boolean(b)) => {
                self.advance()?;
                Ok(SelectExpression::Literal(Value::Boolean(b)))
            }
            Some(Token::Null) => {
                self.advance()?;
                Ok(SelectExpression::Literal(Value::Null))
            }
            Some(Token::LeftParen) => {
                self.advance()?;
                let expr = self.parse_select_expression()?;
                self.expect(Token::RightParen)?;
                Ok(expr)
            }
            other => Err(Error::cql_parse(format!(
                "Unexpected token in expression: {:?}",
                other
            ))),
        }
    }

    /// Parse `WRITETIME(col)` or `TTL(col)`.
    ///
    /// The function keyword has already been consumed by the caller.
    /// Grammar: `'(' identifier ')'` optionally followed by `AS alias`.
    fn parse_writetime_ttl_call(
        &mut self,
        function: WriteTimeTtlFunction,
    ) -> Result<SelectExpression> {
        self.expect(Token::LeftParen)?;

        // Argument must be a single bare identifier (the column name).
        let column = match self.current_token.clone() {
            Some(Token::Identifier(name)) => {
                self.advance()?;
                name
            }
            other => {
                return Err(Error::cql_parse(format!(
                    "{} requires a single column name argument, found: {:?}",
                    match function {
                        WriteTimeTtlFunction::WriteTime => "WRITETIME",
                        WriteTimeTtlFunction::Ttl => "TTL",
                    },
                    other
                )));
            }
        };

        self.expect(Token::RightParen)?;

        // Optional alias: `WRITETIME(col) AS wt`
        // Aliases in SELECT are supported by the grammar (the surrounding
        // `parse_select_expression` already handles `AS`), but we handle it here
        // too so we can attach it directly to the `WriteTimeTtlCall` for clarity.
        // The outer `parse_select_expression` wraps us in `Aliased` when it sees
        // `AS`; that path is the canonical one and this variant stores it inline.
        let alias = if self.eat(&Token::As)? {
            match self.current_token.clone() {
                Some(Token::Identifier(alias_name)) => {
                    self.advance()?;
                    Some(alias_name)
                }
                other => {
                    return Err(Error::cql_parse(format!(
                        "Expected alias identifier after AS, found: {:?}",
                        other
                    )));
                }
            }
        } else {
            None
        };

        Ok(SelectExpression::WriteTimeTtl(WriteTimeTtlCall {
            function,
            column,
            alias,
        }))
    }

    /// Parse aggregate function
    fn parse_aggregate_function(&mut self, agg_type: AggregateType) -> Result<SelectExpression> {
        self.expect(Token::LeftParen)?;

        let distinct = self.eat(&Token::Distinct)?;
        let mut args = Vec::new();

        if !self.at(&Token::RightParen) {
            // COUNT(*) is the only place `*` is valid as an aggregate arg; treat it as a wildcard column.
            if self.eat(&Token::Multiply)? {
                args.push(SelectExpression::Column(ColumnRef::new("*".to_string())));
            } else {
                loop {
                    args.push(self.parse_select_expression()?);
                    if !self.eat(&Token::Comma)? {
                        break;
                    }
                }
            }
        }

        self.expect(Token::RightParen)?;

        Ok(SelectExpression::Aggregate(AggregateFunction {
            function: agg_type,
            args,
            distinct,
        }))
    }

    /// Parse FROM clause
    fn parse_from_clause(&mut self) -> Result<FromClause> {
        // Cassandra CQL only supports single table queries - NO JOINS
        let Some(Token::Identifier(first_identifier)) = self.current_token.clone() else {
            return Err(Error::cql_parse("Expected table name in FROM clause"));
        };
        self.advance()?;

        // Qualified name: keyspace.table
        let table_name = if self.eat(&Token::Dot)? {
            if let Some(Token::Identifier(actual_table)) = self.current_token.clone() {
                self.advance()?;
                format!("{}.{}", first_identifier, actual_table)
            } else {
                return Err(Error::cql_parse("Expected table name after keyspace"));
            }
        } else {
            first_identifier
        };

        let table = TableId::new(table_name);

        // Optional alias - but only if the next identifier isn't a clause keyword
        // that the lookahead-free tokenizer would otherwise hand us as a plain identifier.
        // (In practice clause keywords already tokenize as their own variants, but we
        // keep this defensive check to preserve historical behavior.)
        const CLAUSE_KEYWORDS: &[&str] = &["WHERE", "GROUP", "ORDER", "HAVING", "LIMIT"];
        if let Some(Token::Identifier(alias)) = self.current_token.clone() {
            let is_clause_kw = CLAUSE_KEYWORDS
                .iter()
                .any(|kw| alias.eq_ignore_ascii_case(kw));
            if !is_clause_kw {
                self.advance()?;
                return Ok(FromClause::TableAlias(table, alias));
            }
        }

        Ok(FromClause::Table(table))
    }

    /// Parse WHERE expression
    fn parse_where_expression(&mut self) -> Result<WhereExpression> {
        self.parse_or_expression()
    }

    /// Parse OR expression
    fn parse_or_expression(&mut self) -> Result<WhereExpression> {
        let first = self.parse_and_expression()?;
        let mut or_exprs = vec![first];
        while self.eat(&Token::Or)? {
            or_exprs.push(self.parse_and_expression()?);
        }
        Ok(unwrap_singleton(or_exprs, WhereExpression::Or))
    }

    /// Parse AND expression
    fn parse_and_expression(&mut self) -> Result<WhereExpression> {
        let first = self.parse_not_expression()?;
        let mut and_exprs = vec![first];
        while self.eat(&Token::And)? {
            and_exprs.push(self.parse_not_expression()?);
        }
        Ok(unwrap_singleton(and_exprs, WhereExpression::And))
    }

    /// Parse NOT expression
    fn parse_not_expression(&mut self) -> Result<WhereExpression> {
        if self.eat(&Token::Not)? {
            let expr = self.parse_comparison_expression()?;
            Ok(WhereExpression::Not(Box::new(expr)))
        } else {
            self.parse_comparison_expression()
        }
    }

    /// Parse comparison expression
    fn parse_comparison_expression(&mut self) -> Result<WhereExpression> {
        if self.eat(&Token::LeftParen)? {
            let expr = self.parse_where_expression()?;
            self.expect(Token::RightParen)?;
            return Ok(WhereExpression::Parentheses(Box::new(expr)));
        }

        let left = self.parse_select_expression()?;

        // Map a "simple" binary comparison token to its operator. For operators
        // with bespoke right-hand-side parsing (IN, BETWEEN, IS, CONTAINS) we
        // handle them in the match below and return early.
        let simple_op = match self.peek() {
            Token::Equal => Some(ComparisonOperator::Equal),
            Token::NotEqual => Some(ComparisonOperator::NotEqual),
            Token::LessThan => Some(ComparisonOperator::LessThan),
            Token::LessThanEqual => Some(ComparisonOperator::LessThanOrEqual),
            Token::GreaterThan => Some(ComparisonOperator::GreaterThan),
            Token::GreaterThanEqual => Some(ComparisonOperator::GreaterThanOrEqual),
            Token::Like => Some(ComparisonOperator::Like),
            _ => None,
        };

        if let Some(op) = simple_op {
            self.advance()?;
            let right = ComparisonRightSide::Value(self.parse_select_expression()?);
            return Ok(WhereExpression::Comparison(ComparisonExpression {
                left,
                operator: op,
                right,
            }));
        }

        let operator = match self.peek() {
            Token::In => {
                self.advance()?;
                let right = self.parse_in_expression()?;
                return Ok(WhereExpression::Comparison(ComparisonExpression {
                    left,
                    operator: ComparisonOperator::In,
                    right,
                }));
            }
            Token::Between => {
                self.advance()?;
                let start = self.parse_select_expression()?;
                self.expect(Token::And)?;
                let end = self.parse_select_expression()?;
                return Ok(WhereExpression::Comparison(ComparisonExpression {
                    left,
                    operator: ComparisonOperator::Between,
                    right: ComparisonRightSide::Range(start, end),
                }));
            }
            Token::Is => {
                self.advance()?;
                let op = if self.eat(&Token::Not)? {
                    ComparisonOperator::IsNotNull
                } else {
                    ComparisonOperator::IsNull
                };
                self.expect(Token::Null)?;
                op
            }
            Token::Contains => {
                self.advance()?;
                if self.eat(&Token::Key)? {
                    ComparisonOperator::ContainsKey
                } else {
                    ComparisonOperator::Contains
                }
            }
            other => {
                return Err(Error::cql_parse(format!(
                    "Expected comparison operator, found {:?}",
                    other
                )));
            }
        };

        // Only IS NULL / IS NOT NULL / CONTAINS / CONTAINS KEY reach here.
        let right = match operator {
            ComparisonOperator::IsNull | ComparisonOperator::IsNotNull => {
                ComparisonRightSide::Value(SelectExpression::Literal(Value::Null))
            }
            _ => ComparisonRightSide::Value(self.parse_select_expression()?),
        };

        Ok(WhereExpression::Comparison(ComparisonExpression {
            left,
            operator,
            right,
        }))
    }

    /// Parse IN expression value list
    fn parse_in_expression(&mut self) -> Result<ComparisonRightSide> {
        self.expect(Token::LeftParen)?;
        let mut values = Vec::new();

        if !self.at(&Token::RightParen) {
            loop {
                values.push(self.parse_select_expression()?);
                if !self.eat(&Token::Comma)? {
                    break;
                }
            }
        }

        self.expect(Token::RightParen)?;
        Ok(ComparisonRightSide::ValueList(values))
    }

    /// Parse GROUP BY clause
    fn parse_group_by_clause(&mut self) -> Result<GroupByClause> {
        let mut columns = Vec::new();

        loop {
            let Some(Token::Identifier(name)) = self.current_token.clone() else {
                return Err(Error::cql_parse("Expected column name in GROUP BY"));
            };
            self.advance()?;
            columns.push(self.parse_column_ref(name)?);

            if !self.eat(&Token::Comma)? {
                break;
            }
        }

        Ok(GroupByClause { columns })
    }

    /// Parse ORDER BY clause
    fn parse_order_by_clause(&mut self) -> Result<OrderByClause> {
        let mut items = Vec::new();

        loop {
            let expression = self.parse_select_expression()?;

            let direction = if self.eat(&Token::Desc)? {
                SortDirection::Descending
            } else if self.eat(&Token::Asc)? {
                SortDirection::Ascending
            } else {
                SortDirection::Ascending
            };

            items.push(OrderByItem {
                expression,
                direction,
            });

            if !self.eat(&Token::Comma)? {
                break;
            }
        }

        Ok(OrderByClause { items })
    }

    /// Parse the query-wide LIMIT clause.
    ///
    /// `LIMIT 0` is intentionally accepted and yields an empty result set
    /// (enforced downstream); this preserves long-standing CQLite behavior
    /// (`test_limit_zero_returns_empty`). Only `PER PARTITION LIMIT` is required
    /// to be strictly positive (Issue #757).
    fn parse_limit_clause(&mut self) -> Result<LimitClause> {
        let count = self.expect_integer("LIMIT")? as u64;
        Ok(LimitClause { count })
    }

    /// Parse a strictly-positive integer limit, rejecting zero/negative values.
    /// Used for `PER PARTITION LIMIT`, which Cassandra requires to be >= 1.
    fn parse_positive_limit(&mut self, clause: &str) -> Result<u64> {
        let value = self.expect_integer(clause)?;
        if value < 1 {
            return Err(Error::cql_parse(format!(
                "{} must be a positive integer, got {}",
                clause, value
            )));
        }
        Ok(value as u64)
    }
}

/// If `exprs` has a single element, return it; otherwise wrap with `wrap`
/// (typically `WhereExpression::And` / `WhereExpression::Or`). The vector is
/// guaranteed non-empty by callers that always push at least one element.
fn unwrap_singleton<F>(mut exprs: Vec<WhereExpression>, wrap: F) -> WhereExpression
where
    F: FnOnce(Vec<WhereExpression>) -> WhereExpression,
{
    if exprs.len() == 1 {
        exprs.pop().expect("checked len == 1")
    } else {
        wrap(exprs)
    }
}

/// Main parsing function for SELECT statements
pub fn parse_select(cql: &str) -> Result<SelectStatement> {
    let mut parser = SelectParser::new(cql)?;
    parser.parse_select_statement()
}

#[cfg(all(test, feature = "state_machine"))]
mod tests {
    use super::super::select_ast::{SelectExpression, WriteTimeTtlFunction};
    use super::*;

    // --- WRITETIME / TTL parser tests (Issue #690) ---

    #[test]
    fn test_writetime_basic() {
        let stmt = parse_select("SELECT WRITETIME(name) FROM ks.tbl").unwrap();
        if let SelectClause::Columns(exprs) = stmt.select_clause {
            assert_eq!(exprs.len(), 1);
            match &exprs[0] {
                SelectExpression::WriteTimeTtl(call) => {
                    assert_eq!(call.function, WriteTimeTtlFunction::WriteTime);
                    assert_eq!(call.column, "name");
                    assert!(call.alias.is_none());
                }
                other => panic!("Expected WriteTimeTtl, got: {:?}", other),
            }
        } else {
            panic!("Expected Columns select clause");
        }
    }

    #[test]
    fn test_ttl_basic() {
        let stmt = parse_select("SELECT TTL(name) FROM ks.tbl").unwrap();
        if let SelectClause::Columns(exprs) = stmt.select_clause {
            assert_eq!(exprs.len(), 1);
            match &exprs[0] {
                SelectExpression::WriteTimeTtl(call) => {
                    assert_eq!(call.function, WriteTimeTtlFunction::Ttl);
                    assert_eq!(call.column, "name");
                    assert!(call.alias.is_none());
                }
                other => panic!("Expected WriteTimeTtl, got: {:?}", other),
            }
        } else {
            panic!("Expected Columns select clause");
        }
    }

    #[test]
    fn test_writetime_lowercase() {
        // CQL is case-insensitive; the keyword should parse regardless of case.
        let stmt = parse_select("SELECT writetime(name) FROM tbl").unwrap();
        if let SelectClause::Columns(exprs) = stmt.select_clause {
            match &exprs[0] {
                SelectExpression::WriteTimeTtl(call) => {
                    assert_eq!(call.function, WriteTimeTtlFunction::WriteTime);
                }
                other => panic!("Expected WriteTimeTtl, got: {:?}", other),
            }
        } else {
            panic!("Expected Columns");
        }
    }

    #[test]
    fn test_ttl_lowercase() {
        let stmt = parse_select("SELECT ttl(name) FROM tbl").unwrap();
        if let SelectClause::Columns(exprs) = stmt.select_clause {
            match &exprs[0] {
                SelectExpression::WriteTimeTtl(call) => {
                    assert_eq!(call.function, WriteTimeTtlFunction::Ttl);
                }
                other => panic!("Expected WriteTimeTtl, got: {:?}", other),
            }
        } else {
            panic!("Expected Columns");
        }
    }

    #[test]
    fn test_writetime_mixed_case() {
        let stmt = parse_select("SELECT WriteTime(name) FROM tbl").unwrap();
        if let SelectClause::Columns(exprs) = stmt.select_clause {
            match &exprs[0] {
                SelectExpression::WriteTimeTtl(call) => {
                    assert_eq!(call.function, WriteTimeTtlFunction::WriteTime);
                }
                other => panic!("Expected WriteTimeTtl, got: {:?}", other),
            }
        } else {
            panic!("Expected Columns");
        }
    }

    #[test]
    fn test_writetime_and_ttl_together() {
        let stmt = parse_select("SELECT WRITETIME(name), TTL(name) FROM ks.tbl").unwrap();
        if let SelectClause::Columns(exprs) = stmt.select_clause {
            assert_eq!(exprs.len(), 2);
            match &exprs[0] {
                SelectExpression::WriteTimeTtl(c) => {
                    assert_eq!(c.function, WriteTimeTtlFunction::WriteTime);
                }
                other => panic!("Expected WriteTimeTtl for first expr, got: {:?}", other),
            }
            match &exprs[1] {
                SelectExpression::WriteTimeTtl(c) => {
                    assert_eq!(c.function, WriteTimeTtlFunction::Ttl);
                }
                other => panic!("Expected WriteTimeTtl for second expr, got: {:?}", other),
            }
        } else {
            panic!("Expected Columns");
        }
    }

    #[test]
    fn test_writetime_with_alias() {
        // Aliases on WRITETIME/TTL are supported; the parser captures them inline.
        let stmt = parse_select("SELECT WRITETIME(name) AS wt FROM tbl").unwrap();
        if let SelectClause::Columns(exprs) = stmt.select_clause {
            assert_eq!(exprs.len(), 1);
            match &exprs[0] {
                SelectExpression::WriteTimeTtl(call) => {
                    assert_eq!(call.function, WriteTimeTtlFunction::WriteTime);
                    assert_eq!(call.alias.as_deref(), Some("wt"));
                }
                other => panic!("Expected WriteTimeTtl with alias, got: {:?}", other),
            }
        } else {
            panic!("Expected Columns");
        }
    }

    #[test]
    fn test_ttl_with_alias() {
        let stmt = parse_select("SELECT ttl(name) AS remaining FROM tbl").unwrap();
        if let SelectClause::Columns(exprs) = stmt.select_clause {
            match &exprs[0] {
                SelectExpression::WriteTimeTtl(call) => {
                    assert_eq!(call.function, WriteTimeTtlFunction::Ttl);
                    assert_eq!(call.alias.as_deref(), Some("remaining"));
                }
                other => panic!("Expected WriteTimeTtl, got: {:?}", other),
            }
        } else {
            panic!("Expected Columns");
        }
    }

    #[test]
    fn test_writetime_alongside_plain_columns() {
        let stmt = parse_select("SELECT id, WRITETIME(name), name FROM tbl").unwrap();
        if let SelectClause::Columns(exprs) = stmt.select_clause {
            assert_eq!(exprs.len(), 3);
            assert!(matches!(&exprs[0], SelectExpression::Column(_)));
            assert!(matches!(&exprs[1], SelectExpression::WriteTimeTtl(_)));
            assert!(matches!(&exprs[2], SelectExpression::Column(_)));
        } else {
            panic!("Expected Columns");
        }
    }

    #[test]
    fn test_column_name_is_preserved() {
        let stmt = parse_select("SELECT WRITETIME(myColumn) FROM tbl").unwrap();
        if let SelectClause::Columns(exprs) = stmt.select_clause {
            match &exprs[0] {
                SelectExpression::WriteTimeTtl(call) => {
                    // The column name is preserved as parsed (not lowercased).
                    assert_eq!(call.column, "myColumn");
                }
                other => panic!("Unexpected: {:?}", other),
            }
        } else {
            panic!("Expected Columns");
        }
    }

    // --- existing tests (preserved) ---

    #[test]
    fn test_simple_select() {
        let stmt = parse_select("SELECT * FROM users").unwrap();
        assert_eq!(stmt.select_clause, SelectClause::All);
        if let Some(FromClause::Table(table)) = stmt.from_clause {
            assert_eq!(table.name(), "users");
        } else {
            panic!("Expected Table in FROM clause");
        }
    }

    #[test]
    fn test_select_with_columns() {
        let stmt = parse_select("SELECT id, name, email FROM users").unwrap();
        if let SelectClause::Columns(exprs) = stmt.select_clause {
            assert_eq!(exprs.len(), 3);
        } else {
            panic!("Expected Columns in SELECT clause");
        }
    }

    #[test]
    fn test_select_constant() {
        let stmt = parse_select("SELECT 1").unwrap();
        assert!(stmt.from_clause.is_none());
        if let SelectClause::Columns(exprs) = stmt.select_clause {
            assert_eq!(exprs.len(), 1);
            if let SelectExpression::Literal(Value::BigInt(1)) = &exprs[0] {
                // Success
            } else {
                panic!("Expected literal BigInt 1, got: {:?}", &exprs[0]);
            }
        } else {
            panic!("Expected Columns in SELECT clause");
        }
    }

    #[test]
    fn test_select_with_where() {
        let stmt = parse_select("SELECT * FROM users WHERE id = 123").unwrap();
        assert!(stmt.where_clause.is_some());
    }

    #[test]
    fn test_select_with_aggregates() {
        let stmt = parse_select("SELECT COUNT(*), AVG(age) FROM users GROUP BY city").unwrap();
        assert!(stmt.requires_aggregation());
        assert!(stmt.group_by.is_some());
    }

    #[test]
    fn test_complex_where_clause() {
        let stmt =
            parse_select("SELECT * FROM users WHERE age > 21 AND (city = 'NYC' OR city = 'LA')")
                .unwrap();
        assert!(stmt.where_clause.is_some());
    }

    #[test]
    fn test_order_by_and_limit() {
        let stmt = parse_select("SELECT * FROM users ORDER BY created_at DESC, name ASC LIMIT 10")
            .unwrap();
        assert!(stmt.order_by.is_some());
        assert!(stmt.limit.is_some());

        if let Some(limit) = stmt.limit {
            assert_eq!(limit.count, 10);
        }
    }

    // --- PER PARTITION LIMIT parser tests (Issue #757) ---

    #[test]
    fn test_per_partition_limit_basic() {
        let stmt = parse_select("SELECT * FROM ks.t PER PARTITION LIMIT 2").unwrap();
        assert_eq!(stmt.per_partition_limit, Some(2));
        assert!(stmt.limit.is_none());
    }

    #[test]
    fn test_per_partition_limit_with_global_limit() {
        let stmt = parse_select("SELECT * FROM ks.t PER PARTITION LIMIT 2 LIMIT 5").unwrap();
        assert_eq!(stmt.per_partition_limit, Some(2));
        assert_eq!(stmt.limit.map(|l| l.count), Some(5));
    }

    #[test]
    fn test_per_partition_limit_after_order_by() {
        let stmt = parse_select("SELECT * FROM ks.t ORDER BY c DESC PER PARTITION LIMIT 3 LIMIT 9")
            .unwrap();
        assert!(stmt.order_by.is_some());
        assert_eq!(stmt.per_partition_limit, Some(3));
        assert_eq!(stmt.limit.map(|l| l.count), Some(9));
    }

    #[test]
    fn test_per_partition_limit_rejects_zero() {
        assert!(parse_select("SELECT * FROM ks.t PER PARTITION LIMIT 0").is_err());
    }

    #[test]
    fn test_global_limit_zero_is_accepted() {
        // Regression: `LIMIT 0` must parse (yields empty result downstream);
        // only PER PARTITION LIMIT requires a strictly-positive value.
        let stmt = parse_select("SELECT * FROM ks.t LIMIT 0").unwrap();
        assert_eq!(stmt.limit.map(|l| l.count), Some(0));
    }

    #[test]
    fn test_per_partition_limit_rejects_negative() {
        assert!(parse_select("SELECT * FROM ks.t PER PARTITION LIMIT -1").is_err());
    }

    #[test]
    fn test_per_partition_limit_rejects_after_global_limit() {
        assert!(parse_select("SELECT * FROM ks.t LIMIT 5 PER PARTITION LIMIT 2").is_err());
    }

    #[test]
    fn test_per_partition_limit_rejects_after_limit_offset() {
        // Regression (roborev job 38): the ordering guard must catch a trailing
        // PER PARTITION LIMIT even when LIMIT is followed by OFFSET, not just
        // when it immediately follows LIMIT.
        assert!(parse_select("SELECT * FROM ks.t LIMIT 5 OFFSET 1 PER PARTITION LIMIT 2").is_err());
    }

    #[test]
    fn test_in_clause() {
        let stmt =
            parse_select("SELECT * FROM users WHERE status IN ('active', 'pending', 'suspended')")
                .unwrap();
        assert!(stmt.where_clause.is_some());
    }

    #[test]
    fn test_between_clause() {
        let stmt = parse_select(
            "SELECT * FROM events WHERE created_at BETWEEN '2024-01-01' AND '2024-12-31'",
        )
        .unwrap();
        assert!(stmt.where_clause.is_some());
    }
}
