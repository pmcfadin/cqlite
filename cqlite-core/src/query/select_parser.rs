//! Advanced CQL SELECT Parser
//!
//! This module implements the FIRST EVER CQL SELECT parser for direct SSTable access.
//! It provides comprehensive parsing for complex SELECT statements including:
//! - Advanced WHERE clauses with all operators
//! - Aggregation functions and GROUP BY
//! - ORDER BY and LIMIT clauses  
//! - Collection operations
//! - Subqueries and JOINs (future)

use super::select_ast::*;
use crate::{Error, Result, TableId, Value};

/// Advanced CQL SELECT parser
#[derive(Debug)]
pub struct SelectParser {
    /// Input SQL text
    input: String,
    /// Current position in input
    position: usize,
    /// Current token being parsed
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
    Inner,
    Left,
    Right,
    Full,
    Join,
    On,
    Is,
    Null,
    Contains,
    Key,

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
        let current_char = chars.get(0).copied();

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
                    match escaped {
                        'n' => value.push('\n'),
                        't' => value.push('\t'),
                        'r' => value.push('\r'),
                        '\\' => value.push('\\'),
                        '\'' => value.push('\''),
                        '"' => value.push('"'),
                        _ => {
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

        Err(Error::sql_parse("Unterminated string literal"))
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
            let float_val = value
                .parse::<f64>()
                .map_err(|_| Error::sql_parse(&format!("Invalid float: {}", value)))?;
            Ok(Token::Float(float_val))
        } else {
            let int_val = value
                .parse::<i64>()
                .map_err(|_| Error::sql_parse(&format!("Invalid integer: {}", value)))?;
            Ok(Token::Integer(int_val))
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

    pub fn next_token(&mut self) -> Result<Token> {
        loop {
            match self.current_char {
                None => return Ok(Token::Eof),
                Some(ch) if ch.is_whitespace() => {
                    self.skip_whitespace();
                }
                Some('(') => {
                    self.advance();
                    return Ok(Token::LeftParen);
                }
                Some(')') => {
                    self.advance();
                    return Ok(Token::RightParen);
                }
                Some('[') => {
                    self.advance();
                    return Ok(Token::LeftBracket);
                }
                Some(']') => {
                    self.advance();
                    return Ok(Token::RightBracket);
                }
                Some('{') => {
                    self.advance();
                    return Ok(Token::LeftBrace);
                }
                Some('}') => {
                    self.advance();
                    return Ok(Token::RightBrace);
                }
                Some(',') => {
                    self.advance();
                    return Ok(Token::Comma);
                }
                Some(';') => {
                    self.advance();
                    return Ok(Token::Semicolon);
                }
                Some('.') => {
                    self.advance();
                    return Ok(Token::Dot);
                }
                Some('?') => {
                    self.advance();
                    return Ok(Token::Question);
                }
                Some('+') => {
                    self.advance();
                    return Ok(Token::Plus);
                }
                Some('-') => {
                    self.advance();
                    return Ok(Token::Minus);
                }
                Some('*') => {
                    self.advance();
                    return Ok(Token::Multiply);
                }
                Some('/') => {
                    self.advance();
                    return Ok(Token::Divide);
                }
                Some('%') => {
                    self.advance();
                    return Ok(Token::Modulo);
                }
                Some('=') => {
                    self.advance();
                    return Ok(Token::Equal);
                }
                Some('!') => {
                    if self.peek() == Some('=') {
                        self.advance();
                        self.advance();
                        return Ok(Token::NotEqual);
                    } else {
                        return Err(Error::sql_parse("Unexpected character: !"));
                    }
                }
                Some('<') => {
                    if self.peek() == Some('=') {
                        self.advance();
                        self.advance();
                        return Ok(Token::LessThanEqual);
                    } else if self.peek() == Some('>') {
                        self.advance();
                        self.advance();
                        return Ok(Token::NotEqual);
                    } else {
                        self.advance();
                        return Ok(Token::LessThan);
                    }
                }
                Some('>') => {
                    if self.peek() == Some('=') {
                        self.advance();
                        self.advance();
                        return Ok(Token::GreaterThanEqual);
                    } else {
                        self.advance();
                        return Ok(Token::GreaterThan);
                    }
                }
                Some('\'') => {
                    let string_val = self.read_string('\'')?;
                    return Ok(Token::String(string_val));
                }
                Some('"') => {
                    let string_val = self.read_string('"')?;
                    return Ok(Token::String(string_val));
                }
                Some(ch) if ch.is_ascii_digit() => {
                    return self.read_number();
                }
                Some(ch) if ch.is_alphabetic() || ch == '_' => {
                    let identifier = self.read_identifier();
                    let token = match identifier.to_uppercase().as_str() {
                        "SELECT" => Token::Select,
                        "DISTINCT" => Token::Distinct,
                        "FROM" => Token::From,
                        "WHERE" => Token::Where,
                        "GROUP" => {
                            // Look for BY
                            self.skip_whitespace();
                            let next_id = self.read_identifier();
                            if next_id.to_uppercase() == "BY" {
                                Token::GroupBy
                            } else {
                                return Err(Error::sql_parse("Expected BY after GROUP"));
                            }
                        }
                        "HAVING" => Token::Having,
                        "ORDER" => {
                            // Look for BY
                            self.skip_whitespace();
                            let next_id = self.read_identifier();
                            if next_id.to_uppercase() == "BY" {
                                Token::OrderBy
                            } else {
                                return Err(Error::sql_parse("Expected BY after ORDER"));
                            }
                        }
                        "LIMIT" => Token::Limit,
                        "OFFSET" => Token::Offset,
                        "AND" => Token::And,
                        "OR" => Token::Or,
                        "NOT" => Token::Not,
                        "LIKE" => Token::Like,
                        "IN" => Token::In,
                        "BETWEEN" => Token::Between,
                        "AS" => Token::As,
                        "ASC" => Token::Asc,
                        "DESC" => Token::Desc,
                        "ALLOW" => Token::Allow,
                        "FILTERING" => Token::Filtering,
                        "COUNT" => Token::Count,
                        "SUM" => Token::Sum,
                        "AVG" => Token::Avg,
                        "MIN" => Token::Min,
                        "MAX" => Token::Max,
                        "INNER" => Token::Inner,
                        "LEFT" => Token::Left,
                        "RIGHT" => Token::Right,
                        "FULL" => Token::Full,
                        "JOIN" => Token::Join,
                        "ON" => Token::On,
                        "IS" => Token::Is,
                        "NULL" => Token::Null,
                        "CONTAINS" => Token::Contains,
                        "KEY" => Token::Key,
                        "TRUE" => Token::Boolean(true),
                        "FALSE" => Token::Boolean(false),
                        _ => Token::Identifier(identifier),
                    };
                    return Ok(token);
                }
                Some(ch) => {
                    return Err(Error::sql_parse(&format!("Unexpected character: {}", ch)));
                }
            }
        }
    }
}

impl SelectParser {
    /// Create a new SELECT parser
    pub fn new(sql: &str) -> Result<Self> {
        let mut tokenizer = Tokenizer::new(sql);
        let current_token = tokenizer.next_token()?;

        Ok(Self {
            input: sql.to_string(),
            position: 0,
            current_token: Some(current_token),
            tokenizer,
        })
    }

    /// Advance to the next token
    fn advance(&mut self) -> Result<()> {
        self.current_token = Some(self.tokenizer.next_token()?);
        Ok(())
    }

    /// Check if current token matches expected token
    fn expect(&mut self, expected: Token) -> Result<()> {
        if let Some(ref current) = self.current_token {
            if std::mem::discriminant(current) == std::mem::discriminant(&expected) {
                self.advance()?;
                Ok(())
            } else {
                Err(Error::sql_parse(&format!(
                    "Expected {:?}, found {:?}",
                    expected, current
                )))
            }
        } else {
            Err(Error::sql_parse("Unexpected end of input"))
        }
    }

    /// Parse a complete SELECT statement
    pub fn parse_select_statement(&mut self) -> Result<SelectStatement> {
        // Parse SELECT clause
        self.expect(Token::Select)?;
        let select_clause = self.parse_select_clause()?;

        // Parse FROM clause
        self.expect(Token::From)?;
        let from_clause = self.parse_from_clause()?;

        // Parse optional WHERE clause
        let where_clause = if self.current_token == Some(Token::Where) {
            self.advance()?;
            Some(self.parse_where_expression()?)
        } else {
            None
        };

        // Parse optional GROUP BY clause
        let group_by = if self.current_token == Some(Token::GroupBy) {
            self.advance()?;
            Some(self.parse_group_by_clause()?)
        } else {
            None
        };

        // Parse optional HAVING clause
        let having_clause = if self.current_token == Some(Token::Having) {
            self.advance()?;
            Some(self.parse_where_expression()?)
        } else {
            None
        };

        // Parse optional ORDER BY clause
        let order_by = if self.current_token == Some(Token::OrderBy) {
            self.advance()?;
            Some(self.parse_order_by_clause()?)
        } else {
            None
        };

        // Parse optional LIMIT clause
        let limit = if self.current_token == Some(Token::Limit) {
            self.advance()?;
            Some(self.parse_limit_clause()?)
        } else {
            None
        };

        // Parse optional OFFSET clause
        let offset = if self.current_token == Some(Token::Offset) {
            self.advance()?;
            if let Some(Token::Integer(n)) = self.current_token {
                self.advance()?;
                Some(n as u64)
            } else {
                return Err(Error::sql_parse("Expected integer after OFFSET"));
            }
        } else {
            None
        };

        // Parse optional ALLOW FILTERING
        let allow_filtering = if self.current_token == Some(Token::Allow) {
            self.advance()?;
            self.expect(Token::Filtering)?;
            true
        } else {
            false
        };

        Ok(SelectStatement {
            select_clause,
            from_clause,
            where_clause,
            group_by,
            having_clause,
            order_by,
            limit,
            offset,
            allow_filtering,
        })
    }

    /// Parse SELECT clause
    fn parse_select_clause(&mut self) -> Result<SelectClause> {
        let distinct = if self.current_token == Some(Token::Distinct) {
            self.advance()?;
            true
        } else {
            false
        };

        if let Some(Token::Multiply) = self.current_token {
            self.advance()?;
            return Ok(SelectClause::All);
        }

        let mut expressions = Vec::new();

        loop {
            expressions.push(self.parse_select_expression()?);

            if self.current_token == Some(Token::Comma) {
                self.advance()?;
            } else {
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
        if self.current_token == Some(Token::As) {
            self.advance()?;
            if let Some(Token::Identifier(alias)) = self.current_token.clone() {
                self.advance()?;
                return Ok(SelectExpression::Aliased(Box::new(expr), alias));
            } else {
                return Err(Error::sql_parse("Expected alias name after AS"));
            }
        }

        Ok(expr)
    }

    /// Parse primary expression (column, function, literal, etc.)
    fn parse_primary_expression(&mut self) -> Result<SelectExpression> {
        match &self.current_token {
            Some(Token::Count) => {
                self.advance()?;
                self.parse_aggregate_function(AggregateType::Count)
            }
            Some(Token::Sum) => {
                self.advance()?;
                self.parse_aggregate_function(AggregateType::Sum)
            }
            Some(Token::Avg) => {
                self.advance()?;
                self.parse_aggregate_function(AggregateType::Avg)
            }
            Some(Token::Min) => {
                self.advance()?;
                self.parse_aggregate_function(AggregateType::Min)
            }
            Some(Token::Max) => {
                self.advance()?;
                self.parse_aggregate_function(AggregateType::Max)
            }
            Some(Token::Identifier(name)) => {
                let name = name.clone();
                self.advance()?;

                // Check for function call
                if self.current_token == Some(Token::LeftParen) {
                    self.advance()?;
                    let mut args = Vec::new();

                    if self.current_token != Some(Token::RightParen) {
                        loop {
                            args.push(self.parse_select_expression()?);
                            if self.current_token == Some(Token::Comma) {
                                self.advance()?;
                            } else {
                                break;
                            }
                        }
                    }

                    self.expect(Token::RightParen)?;
                    Ok(SelectExpression::Function(FunctionCall { name, args }))
                } else {
                    // Check for table qualification
                    if self.current_token == Some(Token::Dot) {
                        self.advance()?;
                        if let Some(Token::Identifier(column)) = self.current_token.clone() {
                            self.advance()?;
                            Ok(SelectExpression::Column(ColumnRef::qualified(name, column)))
                        } else {
                            Err(Error::sql_parse(
                                "Expected column name after table qualifier",
                            ))
                        }
                    } else {
                        Ok(SelectExpression::Column(ColumnRef::new(name)))
                    }
                }
            }
            Some(Token::Integer(n)) => {
                let value = *n;
                self.advance()?;
                Ok(SelectExpression::Literal(Value::BigInt(value)))
            }
            Some(Token::Float(f)) => {
                let value = *f;
                self.advance()?;
                Ok(SelectExpression::Literal(Value::Float(value)))
            }
            Some(Token::String(s)) => {
                let value = s.clone();
                self.advance()?;
                Ok(SelectExpression::Literal(Value::Text(value)))
            }
            Some(Token::Boolean(b)) => {
                let value = *b;
                self.advance()?;
                Ok(SelectExpression::Literal(Value::Boolean(value)))
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
            _ => Err(Error::sql_parse(&format!(
                "Unexpected token in expression: {:?}",
                self.current_token
            ))),
        }
    }

    /// Parse aggregate function
    fn parse_aggregate_function(&mut self, agg_type: AggregateType) -> Result<SelectExpression> {
        self.expect(Token::LeftParen)?;

        let distinct = if self.current_token == Some(Token::Distinct) {
            self.advance()?;
            true
        } else {
            false
        };

        let mut args = Vec::new();

        if self.current_token != Some(Token::RightParen) {
            loop {
                args.push(self.parse_select_expression()?);
                if self.current_token == Some(Token::Comma) {
                    self.advance()?;
                } else {
                    break;
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
        if let Some(Token::Identifier(table_name)) = self.current_token.clone() {
            self.advance()?;

            // Check for alias
            if let Some(Token::Identifier(alias)) = self.current_token.clone() {
                self.advance()?;
                Ok(FromClause::TableAlias(TableId::new(table_name), alias))
            } else {
                Ok(FromClause::Table(TableId::new(table_name)))
            }
        } else {
            Err(Error::sql_parse("Expected table name in FROM clause"))
        }
    }

    /// Parse WHERE expression
    fn parse_where_expression(&mut self) -> Result<WhereExpression> {
        self.parse_or_expression()
    }

    /// Parse OR expression
    fn parse_or_expression(&mut self) -> Result<WhereExpression> {
        let expr = self.parse_and_expression()?;

        let mut or_exprs = vec![expr];
        while self.current_token == Some(Token::Or) {
            self.advance()?;
            or_exprs.push(self.parse_and_expression()?);
        }

        if or_exprs.len() == 1 {
            Ok(or_exprs.into_iter().next().unwrap())
        } else {
            Ok(WhereExpression::Or(or_exprs))
        }
    }

    /// Parse AND expression
    fn parse_and_expression(&mut self) -> Result<WhereExpression> {
        let expr = self.parse_not_expression()?;

        let mut and_exprs = vec![expr];
        while self.current_token == Some(Token::And) {
            self.advance()?;
            and_exprs.push(self.parse_not_expression()?);
        }

        if and_exprs.len() == 1 {
            Ok(and_exprs.into_iter().next().unwrap())
        } else {
            Ok(WhereExpression::And(and_exprs))
        }
    }

    /// Parse NOT expression
    fn parse_not_expression(&mut self) -> Result<WhereExpression> {
        if self.current_token == Some(Token::Not) {
            self.advance()?;
            let expr = self.parse_comparison_expression()?;
            Ok(WhereExpression::Not(Box::new(expr)))
        } else {
            self.parse_comparison_expression()
        }
    }

    /// Parse comparison expression
    fn parse_comparison_expression(&mut self) -> Result<WhereExpression> {
        if self.current_token == Some(Token::LeftParen) {
            self.advance()?;
            let expr = self.parse_where_expression()?;
            self.expect(Token::RightParen)?;
            return Ok(WhereExpression::Parentheses(Box::new(expr)));
        }

        let left = self.parse_select_expression()?;

        let operator = match &self.current_token {
            Some(Token::Equal) => {
                self.advance()?;
                ComparisonOperator::Equal
            }
            Some(Token::NotEqual) => {
                self.advance()?;
                ComparisonOperator::NotEqual
            }
            Some(Token::LessThan) => {
                self.advance()?;
                ComparisonOperator::LessThan
            }
            Some(Token::LessThanEqual) => {
                self.advance()?;
                ComparisonOperator::LessThanOrEqual
            }
            Some(Token::GreaterThan) => {
                self.advance()?;
                ComparisonOperator::GreaterThan
            }
            Some(Token::GreaterThanEqual) => {
                self.advance()?;
                ComparisonOperator::GreaterThanOrEqual
            }
            Some(Token::In) => {
                self.advance()?;
                let right = self.parse_in_expression()?;
                return Ok(WhereExpression::Comparison(ComparisonExpression {
                    left,
                    operator: ComparisonOperator::In,
                    right,
                }));
            }
            Some(Token::Like) => {
                self.advance()?;
                ComparisonOperator::Like
            }
            Some(Token::Between) => {
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
            Some(Token::Is) => {
                self.advance()?;
                if self.current_token == Some(Token::Not) {
                    self.advance()?;
                    self.expect(Token::Null)?;
                    ComparisonOperator::IsNotNull
                } else {
                    self.expect(Token::Null)?;
                    ComparisonOperator::IsNull
                }
            }
            Some(Token::Contains) => {
                self.advance()?;
                if self.current_token == Some(Token::Key) {
                    self.advance()?;
                    ComparisonOperator::ContainsKey
                } else {
                    ComparisonOperator::Contains
                }
            }
            _ => {
                return Err(Error::sql_parse(&format!(
                    "Expected comparison operator, found {:?}",
                    self.current_token
                )));
            }
        };

        let right = if matches!(
            operator,
            ComparisonOperator::IsNull | ComparisonOperator::IsNotNull
        ) {
            ComparisonRightSide::Value(SelectExpression::Literal(Value::Null))
        } else {
            ComparisonRightSide::Value(self.parse_select_expression()?)
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

        if self.current_token != Some(Token::RightParen) {
            loop {
                values.push(self.parse_select_expression()?);
                if self.current_token == Some(Token::Comma) {
                    self.advance()?;
                } else {
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
            if let Some(Token::Identifier(col_name)) = self.current_token.clone() {
                self.advance()?;
                columns.push(ColumnRef::new(col_name));
            } else {
                return Err(Error::sql_parse("Expected column name in GROUP BY"));
            }

            if self.current_token == Some(Token::Comma) {
                self.advance()?;
            } else {
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

            let direction = if self.current_token == Some(Token::Desc) {
                self.advance()?;
                SortDirection::Descending
            } else if self.current_token == Some(Token::Asc) {
                self.advance()?;
                SortDirection::Ascending
            } else {
                SortDirection::Ascending
            };

            items.push(OrderByItem {
                expression,
                direction,
            });

            if self.current_token == Some(Token::Comma) {
                self.advance()?;
            } else {
                break;
            }
        }

        Ok(OrderByClause { items })
    }

    /// Parse LIMIT clause
    fn parse_limit_clause(&mut self) -> Result<LimitClause> {
        if let Some(Token::Integer(count)) = self.current_token {
            self.advance()?;
            Ok(LimitClause {
                count: count as u64,
                per_partition: false, // TODO: Add PER PARTITION support
            })
        } else {
            Err(Error::sql_parse("Expected integer after LIMIT"))
        }
    }
}

/// Main parsing function for SELECT statements
pub fn parse_select(sql: &str) -> Result<SelectStatement> {
    let mut parser = SelectParser::new(sql)?;
    parser.parse_select_statement()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select() {
        let stmt = parse_select("SELECT * FROM users").unwrap();
        assert_eq!(stmt.select_clause, SelectClause::All);
        if let FromClause::Table(table) = stmt.from_clause {
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
