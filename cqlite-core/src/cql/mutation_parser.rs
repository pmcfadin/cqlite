//! CQL Mutation Statement Parser
//!
//! This module provides nom-based parsing for CQL INSERT, UPDATE, and DELETE statements.
//! It is feature-gated behind the `write-support` feature flag for M5.

use super::ast::*;
use super::error::ParserError;
use super::traits::SourcePosition;
use crate::error::Result;
use nom::{
    branch::alt,
    bytes::complete::{tag_no_case, take_while1},
    character::complete::{char, digit1, multispace0, multispace1},
    combinator::{map, opt, recognize},
    multi::{separated_list0, separated_list1},
    sequence::{preceded, separated_pair, tuple},
    IResult,
};

/// CQL keyword parser - case insensitive
fn keyword(s: &str) -> impl Fn(&str) -> IResult<&str, &str> + '_ {
    move |input| tag_no_case(s)(input)
}

/// Parse whitespace
fn ws(input: &str) -> IResult<&str, &str> {
    multispace0(input)
}

/// Parse mandatory whitespace
fn ws1(input: &str) -> IResult<&str, &str> {
    multispace1(input)
}

/// Parse identifier (table name, column name, etc.)
fn identifier(input: &str) -> IResult<&str, CqlIdentifier> {
    // Check if it starts with a quote
    let is_quoted = input.starts_with('"');

    let (remaining, name) = if is_quoted {
        // Quoted identifier
        let (rest, _) = char('"')(input)?;
        let (rest, name) = take_while1(|c: char| c != '"')(rest)?;
        let (rest, _) = char('"')(rest)?;
        (rest, name)
    } else {
        // Unquoted identifier
        take_while1(|c: char| c.is_alphanumeric() || c == '_')(input)?
    };

    Ok((remaining, CqlIdentifier {
        name: name.to_string(),
        quoted: is_quoted,
    }))
}

/// Parse a qualified table name (keyspace.table or just table)
fn qualified_table_name(input: &str) -> IResult<&str, CqlTable> {
    let (input, first) = identifier(input)?;
    let (input, second) = opt(preceded(char('.'), identifier))(input)?;

    match second {
        Some(table) => Ok((input, CqlTable {
            keyspace: Some(first),
            name: table,
        })),
        None => Ok((input, CqlTable {
            keyspace: None,
            name: first,
        })),
    }
}

/// Parse integer literal
fn integer_literal(input: &str) -> IResult<&str, i64> {
    let (input, sign) = opt(char('-'))(input)?;
    let (input, digits) = digit1(input)?;

    let num_str = match sign {
        Some(_) => format!("-{}", digits),
        None => digits.to_string(),
    };

    let value = num_str.parse::<i64>().map_err(|_| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Digit))
    })?;

    Ok((input, value))
}

/// Parse float literal
fn float_literal(input: &str) -> IResult<&str, f64> {
    let (input, sign) = opt(char('-'))(input)?;
    let (input, int_part) = digit1(input)?;
    let (input, _) = char('.')(input)?;
    let (input, frac_part) = digit1(input)?;

    let num_str = match sign {
        Some(_) => format!("-{}.{}", int_part, frac_part),
        None => format!("{}.{}", int_part, frac_part),
    };

    let value = num_str.parse::<f64>().map_err(|_| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Digit))
    })?;

    Ok((input, value))
}

/// Parse string literal (single-quoted)
fn string_literal(input: &str) -> IResult<&str, String> {
    let (input, _) = char('\'')(input)?;
    let mut result = String::new();
    let mut chars = input.chars();
    let mut consumed = 0;

    loop {
        match chars.next() {
            Some('\'') => {
                // Check for escaped quote ''
                if chars.clone().next() == Some('\'') {
                    result.push('\'');
                    chars.next();
                    consumed += 2;
                } else {
                    consumed += 1;
                    break;
                }
            }
            Some('\\') => {
                // Handle escape sequences
                match chars.next() {
                    Some('n') => result.push('\n'),
                    Some('r') => result.push('\r'),
                    Some('t') => result.push('\t'),
                    Some('\\') => result.push('\\'),
                    Some('\'') => result.push('\''),
                    Some(c) => result.push(c),
                    None => return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Escaped))),
                }
                consumed += 2;
            }
            Some(c) => {
                result.push(c);
                consumed += 1;
            }
            None => return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Escaped))),
        }
    }

    Ok((&input[consumed..], result))
}

/// Parse UUID literal
fn uuid_literal(input: &str) -> IResult<&str, String> {
    // UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    let (input, uuid) = recognize(tuple((
        take_while1(|c: char| c.is_ascii_hexdigit()),
        char('-'),
        take_while1(|c: char| c.is_ascii_hexdigit()),
        char('-'),
        take_while1(|c: char| c.is_ascii_hexdigit()),
        char('-'),
        take_while1(|c: char| c.is_ascii_hexdigit()),
        char('-'),
        take_while1(|c: char| c.is_ascii_hexdigit()),
    )))(input)?;

    Ok((input, uuid.to_string()))
}

/// Parse blob literal (hex string with 0x prefix)
fn blob_literal(input: &str) -> IResult<&str, String> {
    let (input, _) = tag_no_case("0x")(input)?;
    let (input, hex) = take_while1(|c: char| c.is_ascii_hexdigit())(input)?;
    Ok((input, hex.to_string()))
}

/// Parse list literal
fn list_literal(input: &str) -> IResult<&str, CqlCollectionLiteral> {
    let (input, _) = char('[')(input)?;
    let (input, _) = ws(input)?;
    let (input, items) = separated_list0(
        tuple((ws, char(','), ws)),
        literal,
    )(input)?;
    let (input, _) = ws(input)?;
    let (input, _) = char(']')(input)?;

    Ok((input, CqlCollectionLiteral::List(items)))
}

/// Parse set literal
fn set_literal(input: &str) -> IResult<&str, CqlCollectionLiteral> {
    let (input, _) = char('{')(input)?;
    let (input, _) = ws(input)?;
    let (input, items) = separated_list0(
        tuple((ws, char(','), ws)),
        literal,
    )(input)?;
    let (input, _) = ws(input)?;
    let (input, _) = char('}')(input)?;

    Ok((input, CqlCollectionLiteral::Set(items)))
}

/// Parse map literal
fn map_literal(input: &str) -> IResult<&str, CqlCollectionLiteral> {
    let (input, _) = char('{')(input)?;
    let (input, _) = ws(input)?;
    let (input, pairs) = separated_list0(
        tuple((ws, char(','), ws)),
        separated_pair(
            literal,
            tuple((ws, char(':'), ws)),
            literal,
        ),
    )(input)?;
    let (input, _) = ws(input)?;
    let (input, _) = char('}')(input)?;

    Ok((input, CqlCollectionLiteral::Map(pairs)))
}

/// Parse CQL literal value
fn literal(input: &str) -> IResult<&str, CqlLiteral> {
    alt((
        // NULL
        map(keyword("null"), |_| CqlLiteral::Null),
        // Boolean
        map(keyword("true"), |_| CqlLiteral::Boolean(true)),
        map(keyword("false"), |_| CqlLiteral::Boolean(false)),
        // String (must come before UUID to avoid UUID being parsed as string)
        map(string_literal, CqlLiteral::String),
        // Blob
        map(blob_literal, CqlLiteral::Blob),
        // UUID (simple heuristic: contains dashes)
        map(uuid_literal, CqlLiteral::Uuid),
        // Float (must come before integer)
        map(float_literal, CqlLiteral::Float),
        // Integer
        map(integer_literal, CqlLiteral::Integer),
        // List
        map(list_literal, CqlLiteral::Collection),
        // Set or Map (distinguish by checking for colon)
        map(set_literal, CqlLiteral::Collection),
        map(map_literal, CqlLiteral::Collection),
    ))(input)
}

/// Parse expression (simple version for M5)
fn expression(input: &str) -> IResult<&str, CqlExpression> {
    alt((
        // Parameter placeholder
        map(char('?'), |_| CqlExpression::Parameter(0)),
        // Named parameter
        map(preceded(char(':'), identifier), |id| {
            CqlExpression::NamedParameter(id.name)
        }),
        // Literal
        map(literal, CqlExpression::Literal),
        // Column reference
        map(identifier, CqlExpression::Column),
    ))(input)
}

/// Parse WHERE clause
fn where_clause(input: &str) -> IResult<&str, CqlExpression> {
    let (input, _) = ws(input)?;
    let (input, _) = keyword("where")(input)?;
    let (input, _) = ws1(input)?;

    // Parse simple conditions with AND
    let (input, conditions) = separated_list1(
        tuple((ws, keyword("and"), ws)),
        where_condition,
    )(input)?;

    // Combine conditions with AND
    let result = if conditions.len() == 1 {
        conditions.into_iter().next().unwrap()
    } else {
        conditions.into_iter().reduce(|acc, cond| {
            CqlExpression::Binary {
                left: Box::new(acc),
                operator: CqlBinaryOperator::And,
                right: Box::new(cond),
            }
        }).unwrap()
    };

    Ok((input, result))
}

/// Parse single WHERE condition
fn where_condition(input: &str) -> IResult<&str, CqlExpression> {
    let (input, left) = identifier(input)?;
    let (input, _) = ws(input)?;
    let (input, op) = comparison_operator(input)?;
    let (input, _) = ws(input)?;
    let (input, right) = expression(input)?;

    Ok((input, CqlExpression::Binary {
        left: Box::new(CqlExpression::Column(left)),
        operator: op,
        right: Box::new(right),
    }))
}

/// Parse comparison operator
fn comparison_operator(input: &str) -> IResult<&str, CqlBinaryOperator> {
    alt((
        map(char('='), |_| CqlBinaryOperator::Eq),
        map(tag_no_case("!="), |_| CqlBinaryOperator::Ne),
        map(tag_no_case("<="), |_| CqlBinaryOperator::Le),
        map(tag_no_case(">="), |_| CqlBinaryOperator::Ge),
        map(char('<'), |_| CqlBinaryOperator::Lt),
        map(char('>'), |_| CqlBinaryOperator::Gt),
    ))(input)
}

/// Parse USING clause (TTL and TIMESTAMP)
fn using_clause(input: &str) -> IResult<&str, CqlUsing> {
    let (input, _) = ws(input)?;
    let (input, _) = keyword("using")(input)?;
    let (input, _) = ws1(input)?;

    let (input, first_option) = using_option(input)?;
    let (input, second_option) = opt(preceded(
        tuple((ws, keyword("and"), ws)),
        using_option,
    ))(input)?;

    let mut ttl = None;
    let mut timestamp = None;

    match first_option {
        UsingOption::Ttl(t) => ttl = Some(t),
        UsingOption::Timestamp(ts) => timestamp = Some(ts),
    }

    if let Some(second) = second_option {
        match second {
            UsingOption::Ttl(t) => ttl = Some(t),
            UsingOption::Timestamp(ts) => timestamp = Some(ts),
        }
    }

    Ok((input, CqlUsing { ttl, timestamp }))
}

/// USING option (TTL or TIMESTAMP)
enum UsingOption {
    Ttl(CqlExpression),
    Timestamp(CqlExpression),
}

/// Parse single USING option
fn using_option(input: &str) -> IResult<&str, UsingOption> {
    alt((
        map(
            preceded(
                tuple((keyword("ttl"), ws)),
                expression,
            ),
            UsingOption::Ttl,
        ),
        map(
            preceded(
                tuple((keyword("timestamp"), ws)),
                expression,
            ),
            UsingOption::Timestamp,
        ),
    ))(input)
}

/// Parse INSERT statement
pub fn parse_insert_statement(input: &str) -> Result<CqlInsert> {
    let result = insert_statement_impl(input);

    match result {
        Ok((_, insert)) => Ok(insert),
        Err(e) => Err(ParserError::syntax(
            format!("Failed to parse INSERT statement: {:?}", e),
            SourcePosition::start(),
        ).into()),
    }
}

fn insert_statement_impl(input: &str) -> IResult<&str, CqlInsert> {
    let (input, _) = ws(input)?;
    let (input, _) = keyword("insert")(input)?;
    let (input, _) = ws1(input)?;
    let (input, _) = keyword("into")(input)?;
    let (input, _) = ws1(input)?;

    // Table name
    let (input, table) = qualified_table_name(input)?;
    let (input, _) = ws(input)?;

    // Column list
    let (input, _) = char('(')(input)?;
    let (input, _) = ws(input)?;
    let (input, columns) = separated_list1(
        tuple((ws, char(','), ws)),
        identifier,
    )(input)?;
    let (input, _) = ws(input)?;
    let (input, _) = char(')')(input)?;
    let (input, _) = ws(input)?;

    // VALUES clause
    let (input, _) = keyword("values")(input)?;
    let (input, _) = ws(input)?;
    let (input, _) = char('(')(input)?;
    let (input, _) = ws(input)?;
    let (input, values) = separated_list1(
        tuple((ws, char(','), ws)),
        expression,
    )(input)?;
    let (input, _) = ws(input)?;
    let (input, _) = char(')')(input)?;
    let (input, _) = ws(input)?;

    // Optional IF NOT EXISTS
    let (input, if_not_exists) = opt(tuple((
        keyword("if"),
        ws1,
        keyword("not"),
        ws1,
        keyword("exists"),
    )))(input)?;
    let if_not_exists = if_not_exists.is_some();

    // Optional USING clause
    let (input, using) = opt(using_clause)(input)?;
    let (input, _) = ws(input)?;

    Ok((input, CqlInsert {
        table,
        columns,
        values: CqlInsertValues::Values(values),
        if_not_exists,
        using,
    }))
}

/// Parse UPDATE statement
pub fn parse_update_statement(input: &str) -> Result<CqlUpdate> {
    let result = update_statement_impl(input);

    match result {
        Ok((_, update)) => Ok(update),
        Err(e) => Err(ParserError::syntax(
            format!("Failed to parse UPDATE statement: {:?}", e),
            SourcePosition::start(),
        ).into()),
    }
}

fn update_statement_impl(input: &str) -> IResult<&str, CqlUpdate> {
    let (input, _) = ws(input)?;
    let (input, _) = keyword("update")(input)?;
    let (input, _) = ws1(input)?;

    // Table name
    let (input, table) = qualified_table_name(input)?;
    let (input, _) = ws(input)?;

    // Optional USING clause (before SET)
    let (input, using) = opt(using_clause)(input)?;
    let (input, _) = ws(input)?;

    // SET clause
    let (input, _) = keyword("set")(input)?;
    let (input, _) = ws1(input)?;
    let (input, assignments) = separated_list1(
        tuple((ws, char(','), ws)),
        assignment,
    )(input)?;
    let (input, _) = ws(input)?;

    // WHERE clause
    let (input, where_expr) = where_clause(input)?;
    let (input, _) = ws(input)?;

    // Optional IF condition
    let (input, if_condition) = opt(preceded(
        tuple((keyword("if"), ws1)),
        where_condition,
    ))(input)?;
    let (input, _) = ws(input)?;

    Ok((input, CqlUpdate {
        table,
        using,
        assignments,
        where_clause: where_expr,
        if_condition,
    }))
}

/// Parse assignment (col = value)
fn assignment(input: &str) -> IResult<&str, CqlAssignment> {
    let (input, column) = identifier(input)?;
    let (input, _) = ws(input)?;
    let (input, operator) = assignment_operator(input)?;
    let (input, _) = ws(input)?;
    let (input, value) = expression(input)?;

    Ok((input, CqlAssignment {
        column,
        operator,
        value,
    }))
}

/// Parse assignment operator
fn assignment_operator(input: &str) -> IResult<&str, CqlAssignmentOperator> {
    alt((
        map(tag_no_case("+="), |_| CqlAssignmentOperator::AddAssign),
        map(tag_no_case("-="), |_| CqlAssignmentOperator::SubAssign),
        map(char('='), |_| CqlAssignmentOperator::Assign),
    ))(input)
}

/// Parse DELETE statement
pub fn parse_delete_statement(input: &str) -> Result<CqlDelete> {
    let result = delete_statement_impl(input);

    match result {
        Ok((_, delete)) => Ok(delete),
        Err(e) => Err(ParserError::syntax(
            format!("Failed to parse DELETE statement: {:?}", e),
            SourcePosition::start(),
        ).into()),
    }
}

fn delete_statement_impl(input: &str) -> IResult<&str, CqlDelete> {
    let (input, _) = ws(input)?;
    let (input, _) = keyword("delete")(input)?;
    let (input, _) = ws(input)?;

    // Check if we have column list or FROM directly
    // Peek ahead to see if next keyword is FROM
    let trimmed = input.trim_start();
    let has_from = trimmed.to_lowercase().starts_with("from");

    let (input, columns) = if has_from {
        (input, vec![])
    } else {
        // Parse column list
        let (input, cols) = separated_list1(
            tuple((ws, char(','), ws)),
            identifier,
        )(input)?;
        let (input, _) = ws(input)?;
        (input, cols)
    };

    // FROM clause
    let (input, _) = keyword("from")(input)?;
    let (input, _) = ws1(input)?;
    let (input, table) = qualified_table_name(input)?;
    let (input, _) = ws(input)?;

    // Optional USING TIMESTAMP
    let (input, using) = opt(using_clause)(input)?;
    let (input, _) = ws(input)?;

    // WHERE clause
    let (input, where_expr) = where_clause(input)?;
    let (input, _) = ws(input)?;

    // Optional IF condition
    let (input, if_condition) = opt(preceded(
        tuple((keyword("if"), ws1)),
        where_condition,
    ))(input)?;
    let (input, _) = ws(input)?;

    Ok((input, CqlDelete {
        columns,
        table,
        using,
        where_clause: where_expr,
        if_condition,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_insert() {
        let cql = "INSERT INTO users (id, name) VALUES (?, ?)";
        let result = parse_insert_statement(cql);
        assert!(result.is_ok());

        let insert = result.unwrap();
        assert_eq!(insert.table.name.name, "users");
        assert_eq!(insert.columns.len(), 2);
        assert_eq!(insert.columns[0].name, "id");
        assert_eq!(insert.columns[1].name, "name");
    }

    #[test]
    fn test_parse_insert_with_literals() {
        let cql = "INSERT INTO users (id, name, age) VALUES (123, 'John', 30)";
        let result = parse_insert_statement(cql);
        assert!(result.is_ok());

        let insert = result.unwrap();
        assert_eq!(insert.columns.len(), 3);
        match &insert.values {
            CqlInsertValues::Values(vals) => {
                assert_eq!(vals.len(), 3);
            }
            _ => panic!("Expected Values variant"),
        }
    }

    #[test]
    fn test_parse_insert_with_ttl() {
        let cql = "INSERT INTO users (id, name) VALUES (?, ?) USING TTL 3600";
        let result = parse_insert_statement(cql);
        assert!(result.is_ok());

        let insert = result.unwrap();
        assert!(insert.using.is_some());
        assert!(insert.using.as_ref().unwrap().ttl.is_some());
    }

    #[test]
    fn test_parse_insert_with_timestamp() {
        let cql = "INSERT INTO users (id, name) VALUES (?, ?) USING TIMESTAMP 12345";
        let result = parse_insert_statement(cql);
        assert!(result.is_ok());

        let insert = result.unwrap();
        assert!(insert.using.is_some());
        assert!(insert.using.as_ref().unwrap().timestamp.is_some());
    }

    #[test]
    fn test_parse_insert_if_not_exists() {
        let cql = "INSERT INTO users (id, name) VALUES (?, ?) IF NOT EXISTS";
        let result = parse_insert_statement(cql);
        assert!(result.is_ok());

        let insert = result.unwrap();
        assert!(insert.if_not_exists);
    }

    #[test]
    fn test_parse_simple_update() {
        let cql = "UPDATE users SET name = ? WHERE id = ?";
        let result = parse_update_statement(cql);
        assert!(result.is_ok());

        let update = result.unwrap();
        assert_eq!(update.table.name.name, "users");
        assert_eq!(update.assignments.len(), 1);
        assert_eq!(update.assignments[0].column.name, "name");
    }

    #[test]
    fn test_parse_update_with_multiple_assignments() {
        let cql = "UPDATE users SET name = ?, age = ? WHERE id = ?";
        let result = parse_update_statement(cql);
        assert!(result.is_ok());

        let update = result.unwrap();
        assert_eq!(update.assignments.len(), 2);
    }

    #[test]
    fn test_parse_update_with_ttl() {
        let cql = "UPDATE users USING TTL 3600 SET name = ? WHERE id = ?";
        let result = parse_update_statement(cql);
        assert!(result.is_ok());

        let update = result.unwrap();
        assert!(update.using.is_some());
        assert!(update.using.as_ref().unwrap().ttl.is_some());
    }

    #[test]
    fn test_parse_simple_delete() {
        let cql = "DELETE FROM users WHERE id = ?";
        let result = parse_delete_statement(cql);
        if result.is_err() {
            eprintln!("Parse error: {:?}", result.as_ref().err());
        }
        assert!(result.is_ok());

        let delete = result.unwrap();
        assert_eq!(delete.table.name.name, "users");
        assert!(delete.columns.is_empty());
    }

    #[test]
    fn test_parse_delete_columns() {
        let cql = "DELETE name, age FROM users WHERE id = ?";
        let result = parse_delete_statement(cql);
        assert!(result.is_ok());

        let delete = result.unwrap();
        assert_eq!(delete.columns.len(), 2);
        assert_eq!(delete.columns[0].name, "name");
        assert_eq!(delete.columns[1].name, "age");
    }

    #[test]
    fn test_parse_delete_with_timestamp() {
        let cql = "DELETE FROM users USING TIMESTAMP 12345 WHERE id = ?";
        let result = parse_delete_statement(cql);
        assert!(result.is_ok());

        let delete = result.unwrap();
        assert!(delete.using.is_some());
        assert!(delete.using.as_ref().unwrap().timestamp.is_some());
    }

    #[test]
    fn test_parse_qualified_table_name() {
        let cql = "INSERT INTO keyspace.users (id) VALUES (?)";
        let result = parse_insert_statement(cql);
        assert!(result.is_ok());

        let insert = result.unwrap();
        assert!(insert.table.keyspace.is_some());
        assert_eq!(insert.table.keyspace.as_ref().unwrap().name, "keyspace");
        assert_eq!(insert.table.name.name, "users");
    }

    #[test]
    fn test_parse_quoted_identifiers() {
        let cql = r#"INSERT INTO "MyTable" ("MyColumn") VALUES (?)"#;
        let result = parse_insert_statement(cql);
        assert!(result.is_ok());

        let insert = result.unwrap();
        assert!(insert.table.name.quoted);
        assert_eq!(insert.table.name.name, "MyTable");
    }

    #[test]
    fn test_parse_string_literals() {
        let cql = "INSERT INTO users (name) VALUES ('John O''Brien')";
        let result = parse_insert_statement(cql);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_null_literal() {
        let cql = "INSERT INTO users (name) VALUES (null)";
        let result = parse_insert_statement(cql);
        assert!(result.is_ok());

        let insert = result.unwrap();
        match &insert.values {
            CqlInsertValues::Values(vals) => {
                assert_eq!(vals.len(), 1);
                match &vals[0] {
                    CqlExpression::Literal(CqlLiteral::Null) => {},
                    _ => panic!("Expected NULL literal"),
                }
            }
            _ => panic!("Expected Values variant"),
        }
    }

    #[test]
    fn test_parse_collection_literals() {
        let cql = "INSERT INTO users (tags) VALUES (['tag1', 'tag2'])";
        let result = parse_insert_statement(cql);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_error_invalid_syntax() {
        let cql = "INSERT INVALID SYNTAX";
        let result = parse_insert_statement(cql);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_insert_with_both_ttl_and_timestamp() {
        let cql = "INSERT INTO users (id, name) VALUES (?, ?) USING TTL 3600 AND TIMESTAMP 12345";
        let result = parse_insert_statement(cql);
        assert!(result.is_ok());

        let insert = result.unwrap();
        assert!(insert.using.is_some());
        let using = insert.using.as_ref().unwrap();
        assert!(using.ttl.is_some());
        assert!(using.timestamp.is_some());
    }

    #[test]
    fn test_parse_update_with_compound_where() {
        let cql = "UPDATE users SET name = ? WHERE id = ? AND age = ?";
        let result = parse_update_statement(cql);
        assert!(result.is_ok());

        let update = result.unwrap();
        assert!(matches!(update.where_clause, CqlExpression::Binary { .. }));
    }

    #[test]
    fn test_parse_delete_with_if_condition() {
        let cql = "DELETE FROM users WHERE id = ? IF name = ?";
        let result = parse_delete_statement(cql);
        assert!(result.is_ok());

        let delete = result.unwrap();
        assert!(delete.if_condition.is_some());
    }

    #[test]
    fn test_parse_update_with_add_assign() {
        let cql = "UPDATE counters SET count += 1 WHERE id = ?";
        let result = parse_update_statement(cql);
        assert!(result.is_ok());

        let update = result.unwrap();
        assert_eq!(update.assignments.len(), 1);
        assert!(matches!(
            update.assignments[0].operator,
            CqlAssignmentOperator::AddAssign
        ));
    }

    #[test]
    fn test_parse_update_with_sub_assign() {
        let cql = "UPDATE counters SET count -= 1 WHERE id = ?";
        let result = parse_update_statement(cql);
        assert!(result.is_ok());

        let update = result.unwrap();
        assert_eq!(update.assignments.len(), 1);
        assert!(matches!(
            update.assignments[0].operator,
            CqlAssignmentOperator::SubAssign
        ));
    }

    #[test]
    fn test_parse_named_parameters() {
        let cql = "INSERT INTO users (id, name) VALUES (:id, :name)";
        let result = parse_insert_statement(cql);
        assert!(result.is_ok());

        let insert = result.unwrap();
        match &insert.values {
            CqlInsertValues::Values(vals) => {
                assert_eq!(vals.len(), 2);
                assert!(matches!(vals[0], CqlExpression::NamedParameter(_)));
                assert!(matches!(vals[1], CqlExpression::NamedParameter(_)));
            }
            _ => panic!("Expected Values variant"),
        }
    }

    #[test]
    fn test_parse_boolean_literals() {
        let cql = "INSERT INTO users (id, active) VALUES (?, true)";
        let result = parse_insert_statement(cql);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_uuid_literal() {
        let cql = "INSERT INTO users (id) VALUES (550e8400-e29b-41d4-a716-446655440000)";
        let result = parse_insert_statement(cql);
        assert!(result.is_ok());

        let insert = result.unwrap();
        match &insert.values {
            CqlInsertValues::Values(vals) => {
                assert_eq!(vals.len(), 1);
                match &vals[0] {
                    CqlExpression::Literal(CqlLiteral::Uuid(_)) => {},
                    _ => panic!("Expected UUID literal"),
                }
            }
            _ => panic!("Expected Values variant"),
        }
    }

    #[test]
    fn test_parse_blob_literal() {
        let cql = "INSERT INTO users (data) VALUES (0xdeadbeef)";
        let result = parse_insert_statement(cql);
        assert!(result.is_ok());

        let insert = result.unwrap();
        match &insert.values {
            CqlInsertValues::Values(vals) => {
                assert_eq!(vals.len(), 1);
                match &vals[0] {
                    CqlExpression::Literal(CqlLiteral::Blob(hex)) => {
                        assert_eq!(hex, "deadbeef");
                    }
                    _ => panic!("Expected Blob literal"),
                }
            }
            _ => panic!("Expected Values variant"),
        }
    }

    #[test]
    fn test_parse_set_literal() {
        let cql = "INSERT INTO users (tags) VALUES ({1, 2, 3})";
        let result = parse_insert_statement(cql);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_map_literal() {
        let cql = "INSERT INTO users (settings) VALUES ({'key': 'value'})";
        let result = parse_insert_statement(cql);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_float_literal() {
        let cql = "INSERT INTO metrics (value) VALUES (3.14)";
        let result = parse_insert_statement(cql);
        assert!(result.is_ok());

        let insert = result.unwrap();
        match &insert.values {
            CqlInsertValues::Values(vals) => {
                assert_eq!(vals.len(), 1);
                match &vals[0] {
                    CqlExpression::Literal(CqlLiteral::Float(f)) => {
                        assert_eq!(*f, 3.14);
                    }
                    _ => panic!("Expected Float literal"),
                }
            }
            _ => panic!("Expected Values variant"),
        }
    }

    #[test]
    fn test_parse_negative_integer() {
        let cql = "INSERT INTO metrics (value) VALUES (-42)";
        let result = parse_insert_statement(cql);
        assert!(result.is_ok());

        let insert = result.unwrap();
        match &insert.values {
            CqlInsertValues::Values(vals) => {
                assert_eq!(vals.len(), 1);
                match &vals[0] {
                    CqlExpression::Literal(CqlLiteral::Integer(i)) => {
                        assert_eq!(*i, -42);
                    }
                    _ => panic!("Expected Integer literal"),
                }
            }
            _ => panic!("Expected Values variant"),
        }
    }

    #[test]
    fn test_parse_escaped_string() {
        let cql = r#"INSERT INTO users (name) VALUES ('O''Brien')"#;
        let result = parse_insert_statement(cql);
        assert!(result.is_ok());

        let insert = result.unwrap();
        match &insert.values {
            CqlInsertValues::Values(vals) => {
                assert_eq!(vals.len(), 1);
                match &vals[0] {
                    CqlExpression::Literal(CqlLiteral::String(s)) => {
                        assert_eq!(s, "O'Brien");
                    }
                    _ => panic!("Expected String literal"),
                }
            }
            _ => panic!("Expected Values variant"),
        }
    }

    #[test]
    fn test_parse_comparison_operators() {
        let operators = vec![
            ("id = ?", CqlBinaryOperator::Eq),
            ("id != ?", CqlBinaryOperator::Ne),
            ("id < ?", CqlBinaryOperator::Lt),
            ("id <= ?", CqlBinaryOperator::Le),
            ("id > ?", CqlBinaryOperator::Gt),
            ("id >= ?", CqlBinaryOperator::Ge),
        ];

        for (where_expr, expected_op) in operators {
            let cql = format!("UPDATE users SET name = ? WHERE {}", where_expr);
            let result = parse_update_statement(&cql);
            assert!(result.is_ok(), "Failed to parse: {}", cql);

            let update = result.unwrap();
            match &update.where_clause {
                CqlExpression::Binary { operator, .. } => {
                    assert_eq!(operator, &expected_op);
                }
                _ => panic!("Expected Binary expression"),
            }
        }
    }
}
