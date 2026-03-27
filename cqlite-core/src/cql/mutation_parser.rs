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
    combinator::{map, opt},
    multi::{separated_list0, separated_list1},
    sequence::{preceded, separated_pair, tuple},
    IResult,
};

// DoS Protection Constants (Issue #402)
const MAX_NESTING_DEPTH: usize = 32;
const MAX_COLLECTION_SIZE: usize = 65536;
const MAX_INPUT_LENGTH: usize = 16 * 1024 * 1024; // 16 MB

// Identifier Limits (Issue #403)
const MAX_IDENTIFIER_LENGTH: usize = 48; // Cassandra limit

// Batch Limits
const MAX_BATCH_STATEMENTS: usize = 65535; // Matches Cassandra's warn threshold

/// Validate identifier (Issue #403)
fn validate_identifier(name: &str) -> Result<()> {
    // Reject empty identifiers
    if name.is_empty() {
        return Err(
            ParserError::lexical("Identifier cannot be empty", SourcePosition::start()).into(),
        );
    }

    // Check length
    if name.len() > MAX_IDENTIFIER_LENGTH {
        return Err(ParserError::resource_limit(
            "identifier_length",
            MAX_IDENTIFIER_LENGTH as u64,
            name.len() as u64,
        )
        .into());
    }

    // Reject control characters (ASCII 0-31, 127)
    if name.chars().any(|c| c.is_ascii_control()) {
        return Err(ParserError::lexical(
            "Identifier contains control characters",
            SourcePosition::start(),
        )
        .into());
    }

    // Reject null bytes
    if name.contains('\0') {
        return Err(ParserError::lexical(
            "Identifier contains null bytes",
            SourcePosition::start(),
        )
        .into());
    }

    Ok(())
}

/// Sanitize identifier for filesystem usage (Issue #403)
/// Replaces potentially problematic characters with safe alternatives
#[allow(dead_code)]
fn sanitize_for_filesystem(identifier: &str) -> String {
    identifier
        .chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            '\0' => '_',
            c if c.is_ascii_control() => '_',
            c => c,
        })
        .take(MAX_IDENTIFIER_LENGTH)
        .collect()
}

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

/// Parse quoted identifier with proper escape handling (Issue #403)
fn parse_quoted_identifier(input: &str) -> IResult<&str, String> {
    let (input, _) = char('"')(input)?;
    let mut result = String::new();
    let mut chars = input.chars();
    let mut consumed = 0;

    loop {
        match chars.next() {
            Some('"') => {
                // Check for escaped quote ""
                if chars.clone().next() == Some('"') {
                    result.push('"');
                    chars.next();
                    consumed += 2;
                } else {
                    consumed += 1;
                    break;
                }
            }
            Some(c) => {
                result.push(c);
                consumed += c.len_utf8();
            }
            None => {
                return Err(nom::Err::Error(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Escaped,
                )))
            }
        }
    }

    Ok((&input[consumed..], result))
}

/// Parse identifier (table name, column name, etc.)
fn identifier(input: &str) -> IResult<&str, CqlIdentifier> {
    // Check if it starts with a quote
    let is_quoted = input.starts_with('"');

    let (remaining, name_str) = if is_quoted {
        // Quoted identifier with proper escape handling
        parse_quoted_identifier(input)?
    } else {
        // Unquoted identifier
        let (rem, n) = take_while1(|c: char| c.is_alphanumeric() || c == '_')(input)?;
        (rem, n.to_string())
    };

    // Validate identifier (Issue #403)
    if let Err(_e) = validate_identifier(&name_str) {
        return Err(nom::Err::Failure(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }

    Ok((
        remaining,
        CqlIdentifier {
            name: name_str,
            quoted: is_quoted,
        },
    ))
}

/// Parse a qualified table name (keyspace.table or just table)
fn qualified_table_name(input: &str) -> IResult<&str, CqlTable> {
    let (input, first) = identifier(input)?;
    let (input, second) = opt(preceded(char('.'), identifier))(input)?;

    match second {
        Some(table) => Ok((
            input,
            CqlTable {
                keyspace: Some(first),
                name: table,
            },
        )),
        None => Ok((
            input,
            CqlTable {
                keyspace: None,
                name: first,
            },
        )),
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
                    None => {
                        return Err(nom::Err::Error(nom::error::Error::new(
                            input,
                            nom::error::ErrorKind::Escaped,
                        )))
                    }
                }
                consumed += 2;
            }
            Some(c) => {
                result.push(c);
                consumed += c.len_utf8();
            }
            None => {
                return Err(nom::Err::Error(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Escaped,
                )))
            }
        }
    }

    Ok((&input[consumed..], result))
}

/// Parse UUID literal with strict 8-4-4-4-12 format validation (Issue #402)
fn uuid_literal(input: &str) -> IResult<&str, String> {
    // UUID format: 8-4-4-4-12 hex digits
    // xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx

    // Parse 8 hex digits
    let (input, part1) =
        nom::bytes::complete::take_while_m_n(8, 8, |c: char| c.is_ascii_hexdigit())(input)?;
    let (input, _) = char('-')(input)?;

    // Parse 4 hex digits
    let (input, part2) =
        nom::bytes::complete::take_while_m_n(4, 4, |c: char| c.is_ascii_hexdigit())(input)?;
    let (input, _) = char('-')(input)?;

    // Parse 4 hex digits
    let (input, part3) =
        nom::bytes::complete::take_while_m_n(4, 4, |c: char| c.is_ascii_hexdigit())(input)?;
    let (input, _) = char('-')(input)?;

    // Parse 4 hex digits
    let (input, part4) =
        nom::bytes::complete::take_while_m_n(4, 4, |c: char| c.is_ascii_hexdigit())(input)?;
    let (input, _) = char('-')(input)?;

    // Parse 12 hex digits
    let (input, part5) =
        nom::bytes::complete::take_while_m_n(12, 12, |c: char| c.is_ascii_hexdigit())(input)?;

    let uuid = format!("{}-{}-{}-{}-{}", part1, part2, part3, part4, part5);
    Ok((input, uuid))
}

/// Parse blob literal (hex string with 0x prefix) with even length validation (Issue #402)
fn blob_literal(input: &str) -> IResult<&str, String> {
    let (input, _) = tag_no_case("0x")(input)?;
    let (input, hex) = take_while1(|c: char| c.is_ascii_hexdigit())(input)?;

    // Validate even hex length (each byte needs 2 hex digits)
    if hex.len() % 2 != 0 {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }

    Ok((input, hex.to_string()))
}

/// Parse list literal with depth tracking (Issue #402)
fn list_literal_depth(input: &str, depth: usize) -> IResult<&str, CqlCollectionLiteral> {
    // Check nesting depth
    if depth >= MAX_NESTING_DEPTH {
        return Err(nom::Err::Failure(nom::error::Error::new(
            input,
            nom::error::ErrorKind::TooLarge,
        )));
    }

    let (input, _) = char('[')(input)?;
    let (input, _) = ws(input)?;
    let (input, items) =
        separated_list0(tuple((ws, char(','), ws)), |i| literal_depth(i, depth + 1))(input)?;
    let (input, _) = ws(input)?;
    let (input, _) = char(']')(input)?;

    // Check collection size limit
    if items.len() > MAX_COLLECTION_SIZE {
        return Err(nom::Err::Failure(nom::error::Error::new(
            input,
            nom::error::ErrorKind::TooLarge,
        )));
    }

    Ok((input, CqlCollectionLiteral::List(items)))
}

/// Parse set literal with depth tracking (Issue #402)
fn set_literal_depth(input: &str, depth: usize) -> IResult<&str, CqlCollectionLiteral> {
    // Check nesting depth
    if depth >= MAX_NESTING_DEPTH {
        return Err(nom::Err::Failure(nom::error::Error::new(
            input,
            nom::error::ErrorKind::TooLarge,
        )));
    }

    let (input, _) = char('{')(input)?;
    let (input, _) = ws(input)?;
    let (input, items) =
        separated_list0(tuple((ws, char(','), ws)), |i| literal_depth(i, depth + 1))(input)?;
    let (input, _) = ws(input)?;
    let (input, _) = char('}')(input)?;

    // Check collection size limit
    if items.len() > MAX_COLLECTION_SIZE {
        return Err(nom::Err::Failure(nom::error::Error::new(
            input,
            nom::error::ErrorKind::TooLarge,
        )));
    }

    Ok((input, CqlCollectionLiteral::Set(items)))
}

/// Parse map literal with depth tracking (Issue #402)
fn map_literal_depth(input: &str, depth: usize) -> IResult<&str, CqlCollectionLiteral> {
    // Check nesting depth
    if depth >= MAX_NESTING_DEPTH {
        return Err(nom::Err::Failure(nom::error::Error::new(
            input,
            nom::error::ErrorKind::TooLarge,
        )));
    }

    let (input, _) = char('{')(input)?;
    let (input, _) = ws(input)?;
    let (input, pairs) = separated_list0(
        tuple((ws, char(','), ws)),
        separated_pair(
            |i| literal_depth(i, depth + 1),
            tuple((ws, char(':'), ws)),
            |i| literal_depth(i, depth + 1),
        ),
    )(input)?;
    let (input, _) = ws(input)?;
    let (input, _) = char('}')(input)?;

    // Check collection size limit
    if pairs.len() > MAX_COLLECTION_SIZE {
        return Err(nom::Err::Failure(nom::error::Error::new(
            input,
            nom::error::ErrorKind::TooLarge,
        )));
    }

    Ok((input, CqlCollectionLiteral::Map(pairs)))
}

/// Parse CQL literal value with depth tracking (Issue #402)
fn literal_depth(input: &str, depth: usize) -> IResult<&str, CqlLiteral> {
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
        map(|i| list_literal_depth(i, depth), CqlLiteral::Collection),
        // Set or Map (distinguish by checking for colon)
        map(|i| set_literal_depth(i, depth), CqlLiteral::Collection),
        map(|i| map_literal_depth(i, depth), CqlLiteral::Collection),
    ))(input)
}

/// Parse CQL literal value (entry point, depth 0)
fn literal(input: &str) -> IResult<&str, CqlLiteral> {
    literal_depth(input, 0)
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
    let (input, conditions) =
        separated_list1(tuple((ws, keyword("and"), ws)), where_condition)(input)?;

    // Combine conditions with AND
    // Safety: separated_list1 guarantees at least one element, so reduce always returns Some
    let result = match conditions
        .into_iter()
        .reduce(|acc, cond| CqlExpression::Binary {
            left: Box::new(acc),
            operator: CqlBinaryOperator::And,
            right: Box::new(cond),
        }) {
        Some(expr) => expr,
        None => {
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Many1,
            )))
        }
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

    Ok((
        input,
        CqlExpression::Binary {
            left: Box::new(CqlExpression::Column(left)),
            operator: op,
            right: Box::new(right),
        },
    ))
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
    let (input, second_option) =
        opt(preceded(tuple((ws, keyword("and"), ws)), using_option))(input)?;

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
            preceded(tuple((keyword("ttl"), ws)), expression),
            UsingOption::Ttl,
        ),
        map(
            preceded(tuple((keyword("timestamp"), ws)), expression),
            UsingOption::Timestamp,
        ),
    ))(input)
}

/// Parse INSERT statement
pub fn parse_insert_statement(input: &str) -> Result<CqlInsert> {
    // Check input length (Issue #402 - DoS protection)
    if input.len() > MAX_INPUT_LENGTH {
        return Err(ParserError::resource_limit(
            "input_length",
            MAX_INPUT_LENGTH as u64,
            input.len() as u64,
        )
        .into());
    }

    let result = insert_statement_impl(input);

    match result {
        Ok((_, insert)) => Ok(insert),
        Err(e) => Err(ParserError::syntax(
            format!("Failed to parse INSERT statement: {:?}", e),
            SourcePosition::start(),
        )
        .into()),
    }
}

/// Parse the trailing clause shared by both VALUES and JSON INSERT forms:
/// optional IF NOT EXISTS followed by optional USING clause.
fn insert_trailer(input: &str) -> IResult<&str, (bool, Option<CqlUsing>)> {
    let (input, _) = ws(input)?;
    let (input, if_not_exists) = opt(tuple((
        keyword("if"),
        ws1,
        keyword("not"),
        ws1,
        keyword("exists"),
    )))(input)?;
    let (input, using) = opt(using_clause)(input)?;
    let (input, _) = ws(input)?;
    Ok((input, (if_not_exists.is_some(), using)))
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

    // Check for JSON syntax: INSERT INTO table JSON '...'
    if let Ok((json_input, _)) = keyword("json")(input) {
        let (json_input, _) = ws1(json_input)?;
        let (json_input, json_str) = string_literal(json_input)?;
        let (json_input, (if_not_exists, using)) = insert_trailer(json_input)?;

        return Ok((
            json_input,
            CqlInsert {
                table,
                columns: vec![],
                values: CqlInsertValues::Json(json_str),
                if_not_exists,
                using,
            },
        ));
    }

    // Column list
    let (input, _) = char('(')(input)?;
    let (input, _) = ws(input)?;
    let (input, columns) = separated_list1(tuple((ws, char(','), ws)), identifier)(input)?;
    let (input, _) = ws(input)?;
    let (input, _) = char(')')(input)?;
    let (input, _) = ws(input)?;

    // VALUES clause
    let (input, _) = keyword("values")(input)?;
    let (input, _) = ws(input)?;
    let (input, _) = char('(')(input)?;
    let (input, _) = ws(input)?;
    let (input, values) = separated_list1(tuple((ws, char(','), ws)), expression)(input)?;
    let (input, _) = ws(input)?;
    let (input, _) = char(')')(input)?;
    let (input, (if_not_exists, using)) = insert_trailer(input)?;

    Ok((
        input,
        CqlInsert {
            table,
            columns,
            values: CqlInsertValues::Values(values),
            if_not_exists,
            using,
        },
    ))
}

/// Parse UPDATE statement
pub fn parse_update_statement(input: &str) -> Result<CqlUpdate> {
    // Check input length (Issue #402 - DoS protection)
    if input.len() > MAX_INPUT_LENGTH {
        return Err(ParserError::resource_limit(
            "input_length",
            MAX_INPUT_LENGTH as u64,
            input.len() as u64,
        )
        .into());
    }

    let result = update_statement_impl(input);

    match result {
        Ok((_, update)) => Ok(update),
        Err(e) => Err(ParserError::syntax(
            format!("Failed to parse UPDATE statement: {:?}", e),
            SourcePosition::start(),
        )
        .into()),
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
    let (input, assignments) = separated_list1(tuple((ws, char(','), ws)), assignment)(input)?;
    let (input, _) = ws(input)?;

    // WHERE clause
    let (input, where_expr) = where_clause(input)?;
    let (input, _) = ws(input)?;

    // Optional IF condition
    let (input, if_condition) = opt(preceded(tuple((keyword("if"), ws1)), where_condition))(input)?;
    let (input, _) = ws(input)?;

    Ok((
        input,
        CqlUpdate {
            table,
            using,
            assignments,
            where_clause: where_expr,
            if_condition,
        },
    ))
}

/// Parse assignment (col = value)
fn assignment(input: &str) -> IResult<&str, CqlAssignment> {
    let (input, column) = identifier(input)?;
    let (input, _) = ws(input)?;
    let (input, operator) = assignment_operator(input)?;
    let (input, _) = ws(input)?;
    let (input, value) = expression(input)?;

    Ok((
        input,
        CqlAssignment {
            column,
            operator,
            value,
        },
    ))
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
    // Check input length (Issue #402 - DoS protection)
    if input.len() > MAX_INPUT_LENGTH {
        return Err(ParserError::resource_limit(
            "input_length",
            MAX_INPUT_LENGTH as u64,
            input.len() as u64,
        )
        .into());
    }

    let result = delete_statement_impl(input);

    match result {
        Ok((_, delete)) => Ok(delete),
        Err(e) => Err(ParserError::syntax(
            format!("Failed to parse DELETE statement: {:?}", e),
            SourcePosition::start(),
        )
        .into()),
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
        let (input, cols) = separated_list1(tuple((ws, char(','), ws)), identifier)(input)?;
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
    let (input, if_condition) = opt(preceded(tuple((keyword("if"), ws1)), where_condition))(input)?;
    let (input, _) = ws(input)?;

    Ok((
        input,
        CqlDelete {
            columns,
            table,
            using,
            where_clause: where_expr,
            if_condition,
        },
    ))
}

/// Parse BATCH statement.
///
/// Accepts multi-table batches syntactically, but the write engine processes each
/// statement independently against the provided schema. Limited to
/// [`MAX_BATCH_STATEMENTS`] statements for DoS protection.
pub fn parse_batch_statement(input: &str) -> Result<CqlBatch> {
    // Check input length (Issue #402 - DoS protection)
    if input.len() > MAX_INPUT_LENGTH {
        return Err(ParserError::resource_limit(
            "input_length",
            MAX_INPUT_LENGTH as u64,
            input.len() as u64,
        )
        .into());
    }

    let result = batch_statement_impl(input);

    match result {
        Ok((_, batch)) => Ok(batch),
        Err(e) => Err(ParserError::syntax(
            format!("Failed to parse BATCH statement: {:?}", e),
            SourcePosition::start(),
        )
        .into()),
    }
}

fn batch_statement_impl(input: &str) -> IResult<&str, CqlBatch> {
    let (input, _) = ws(input)?;
    let (input, _) = keyword("begin")(input)?;
    let (input, _) = ws1(input)?;

    // Optional batch type: UNLOGGED, LOGGED, COUNTER (before BATCH keyword)
    let (input, batch_type) = batch_type_parser(input)?;

    let (input, _) = keyword("batch")(input)?;
    let (input, _) = ws(input)?;

    // Optional USING TIMESTAMP
    let (input, using) = opt(using_clause)(input)?;
    let (input, _) = ws(input)?;

    // Parse inner statements (separated by semicolons, with optional trailing semicolon)
    let mut statements = Vec::new();
    let mut remaining = input;
    loop {
        let trimmed = remaining.trim_start();
        // Check for APPLY BATCH
        if trimmed.to_lowercase().starts_with("apply") {
            remaining = trimmed;
            break;
        }
        if trimmed.is_empty() {
            break;
        }

        // Determine which statement type
        let lowered = trimmed.to_lowercase();
        let (rest, stmt) = if lowered.starts_with("insert") {
            let (r, ins) = insert_statement_impl(trimmed)?;
            (r, CqlBatchStatement::Insert(ins))
        } else if lowered.starts_with("update") {
            let (r, upd) = update_statement_impl(trimmed)?;
            (r, CqlBatchStatement::Update(upd))
        } else if lowered.starts_with("delete") {
            let (r, del) = delete_statement_impl(trimmed)?;
            (r, CqlBatchStatement::Delete(del))
        } else {
            return Err(nom::Err::Failure(nom::error::Error::new(
                trimmed,
                nom::error::ErrorKind::Tag,
            )));
        };

        // DoS protection: limit number of statements in a batch
        if statements.len() >= MAX_BATCH_STATEMENTS {
            return Err(nom::Err::Failure(nom::error::Error::new(
                trimmed,
                nom::error::ErrorKind::TooLarge,
            )));
        }

        statements.push(stmt);

        // Consume optional semicolon and whitespace
        let rest = rest.trim_start();
        remaining = rest.strip_prefix(';').unwrap_or(rest);
    }

    // APPLY BATCH
    let (input, _) = keyword("apply")(remaining)?;
    let (input, _) = ws1(input)?;
    let (input, _) = keyword("batch")(input)?;
    let (input, _) = ws(input)?;
    // Optional trailing semicolon
    let input = input.strip_prefix(';').unwrap_or(input);
    let (input, _) = ws(input)?;

    Ok((
        input,
        CqlBatch {
            batch_type,
            using,
            statements,
        },
    ))
}

fn batch_type_parser(input: &str) -> IResult<&str, CqlBatchType> {
    let trimmed = input.trim_start();
    let lowered = trimmed.to_lowercase();
    if lowered.starts_with("unlogged") {
        let (input, _) = keyword("unlogged")(trimmed)?;
        let (input, _) = ws1(input)?;
        Ok((input, CqlBatchType::Unlogged))
    } else if lowered.starts_with("counter") {
        let (input, _) = keyword("counter")(trimmed)?;
        let (input, _) = ws1(input)?;
        Ok((input, CqlBatchType::Counter))
    } else if lowered.starts_with("logged") {
        let (input, _) = keyword("logged")(trimmed)?;
        let (input, _) = ws1(input)?;
        Ok((input, CqlBatchType::Logged))
    } else {
        // Default: logged
        Ok((input, CqlBatchType::Logged))
    }
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
                    CqlExpression::Literal(CqlLiteral::Null) => {}
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
                    CqlExpression::Literal(CqlLiteral::Uuid(_)) => {}
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
    #[allow(clippy::approx_constant)]
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
                        assert!((*f - 3.14).abs() < 0.001, "Expected float close to 3.14");
                        // Not approximating PI
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

    // Issue #402 - DoS Protection Tests

    #[test]
    fn test_input_length_limit() {
        // Create input exceeding MAX_INPUT_LENGTH (16 MB)
        let large_input = format!(
            "INSERT INTO users (id, name) VALUES (?, '{}')",
            "a".repeat(17 * 1024 * 1024)
        );
        let result = parse_insert_statement(&large_input);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("input_length"));
    }

    #[test]
    fn test_nesting_depth_limit() {
        // Create deeply nested list structure
        let mut nested = "1".to_string();
        for _ in 0..40 {
            nested = format!("[{}]", nested);
        }
        let cql = format!("INSERT INTO users (data) VALUES ({})", nested);
        let result = parse_insert_statement(&cql);
        // Should fail due to depth limit
        assert!(result.is_err());
    }

    #[test]
    fn test_collection_size_limit() {
        // Create a list with MAX_COLLECTION_SIZE + 1 items
        let items: Vec<String> = (0..=MAX_COLLECTION_SIZE).map(|i| i.to_string()).collect();
        let list = format!("[{}]", items.join(", "));
        let cql = format!("INSERT INTO users (data) VALUES ({})", list);
        let result = parse_insert_statement(&cql);
        // Should fail due to size limit
        assert!(result.is_err());
    }

    #[test]
    fn test_valid_nested_collections() {
        // Valid nesting within limits (depth 3)
        let cql = "INSERT INTO users (data) VALUES ([[1, 2], [3, 4]])";
        let result = parse_insert_statement(cql);
        assert!(result.is_ok());
    }

    #[test]
    fn test_uuid_strict_format_valid() {
        // Valid UUID with proper 8-4-4-4-12 format
        let cql = "INSERT INTO users (id) VALUES (550e8400-e29b-41d4-a716-446655440000)";
        let result = parse_insert_statement(cql);
        assert!(result.is_ok());
    }

    #[test]
    fn test_uuid_invalid_segment_length() {
        // Invalid UUID - wrong segment lengths
        let cql = "INSERT INTO users (id) VALUES (550e8400-e29b-41d4-a716-4466554400)"; // Last segment too short
        let result = parse_insert_statement(cql);
        assert!(result.is_err());
    }

    #[test]
    fn test_uuid_missing_dashes() {
        // Invalid UUID - missing dashes (should fail as it won't match UUID pattern)
        let cql = "INSERT INTO users (id) VALUES (550e8400e29b41d4a716446655440000)";
        let result = parse_insert_statement(cql);
        // This will fail as a valid parse (might parse as identifier or fail entirely)
        assert!(result.is_err());
    }

    #[test]
    fn test_blob_even_hex_length_valid() {
        // Valid blob with even hex length
        let cql = "INSERT INTO users (data) VALUES (0xdeadbeef)";
        let result = parse_insert_statement(cql);
        assert!(result.is_ok());
    }

    #[test]
    fn test_blob_odd_hex_length_invalid() {
        // Invalid blob with odd hex length
        let cql = "INSERT INTO users (data) VALUES (0xabc)";
        let result = parse_insert_statement(cql);
        assert!(result.is_err());
    }

    // Issue #403 - Identifier Injection Tests

    #[test]
    fn test_quoted_identifier_with_escaped_quotes() {
        // Quoted identifier with doubled quotes
        let cql = r#"INSERT INTO "My""Table" ("My""Column") VALUES (?)"#;
        let result = parse_insert_statement(cql);
        assert!(result.is_ok());

        let insert = result.unwrap();
        assert_eq!(insert.table.name.name, r#"My"Table"#);
        assert_eq!(insert.columns[0].name, r#"My"Column"#);
    }

    #[test]
    fn test_identifier_max_length() {
        // Identifier at max length (48 characters)
        let valid_name = "a".repeat(48);
        let cql = format!(r#"INSERT INTO "{}" (id) VALUES (?)"#, valid_name);
        let result = parse_insert_statement(&cql);
        assert!(result.is_ok());
    }

    #[test]
    fn test_identifier_exceeds_max_length() {
        // Identifier exceeding max length (49 characters)
        let invalid_name = "a".repeat(49);
        let cql = format!(r#"INSERT INTO "{}" (id) VALUES (?)"#, invalid_name);
        let result = parse_insert_statement(&cql);
        assert!(result.is_err());
    }

    #[test]
    fn test_identifier_with_control_characters() {
        // Identifier with control character (newline)
        let cql = "INSERT INTO \"bad\ntable\" (id) VALUES (?)";
        let result = parse_insert_statement(cql);
        assert!(result.is_err());
    }

    #[test]
    fn test_identifier_with_null_byte() {
        // Identifier with null byte
        let cql = "INSERT INTO \"bad\0table\" (id) VALUES (?)";
        let result = parse_insert_statement(cql);
        assert!(result.is_err());
    }

    #[test]
    fn test_valid_unquoted_identifier() {
        // Valid unquoted identifier
        let cql = "INSERT INTO my_table_123 (id) VALUES (?)";
        let result = parse_insert_statement(cql);
        assert!(result.is_ok());
    }

    #[test]
    fn test_sanitize_for_filesystem() {
        // Test sanitization function
        assert_eq!(sanitize_for_filesystem("normal"), "normal");
        assert_eq!(sanitize_for_filesystem("with/slash"), "with_slash");
        assert_eq!(sanitize_for_filesystem("with\\backslash"), "with_backslash");
        assert_eq!(sanitize_for_filesystem("with:colon"), "with_colon");
        assert_eq!(sanitize_for_filesystem("with*asterisk"), "with_asterisk");
        assert_eq!(sanitize_for_filesystem("with?question"), "with_question");
        assert_eq!(sanitize_for_filesystem("with\"quote"), "with_quote");
        assert_eq!(sanitize_for_filesystem("with<less"), "with_less");
        assert_eq!(sanitize_for_filesystem("with>greater"), "with_greater");
        assert_eq!(sanitize_for_filesystem("with|pipe"), "with_pipe");
        assert_eq!(sanitize_for_filesystem("with\0null"), "with_null");

        // Test length truncation
        let long_name = "a".repeat(100);
        let sanitized = sanitize_for_filesystem(&long_name);
        assert_eq!(sanitized.len(), MAX_IDENTIFIER_LENGTH);
    }

    #[test]
    fn test_quoted_identifier_empty() {
        // Empty quoted identifier should be rejected
        let cql = r#"INSERT INTO "" (id) VALUES (?)"#;
        let result = parse_insert_statement(cql);
        // nom's take_while1 will fail on empty identifier
        assert!(result.is_err());
    }

    #[test]
    fn test_multiple_escaped_quotes() {
        // Multiple consecutive escaped quotes
        let cql = r#"INSERT INTO "Tab""""le" (id) VALUES (?)"#;
        let result = parse_insert_statement(cql);
        assert!(result.is_ok());

        let insert = result.unwrap();
        assert_eq!(insert.table.name.name, r#"Tab""le"#);
    }

    #[test]
    fn test_collection_size_within_limit() {
        // Collection just within the limit
        let items: Vec<String> = (0..1000).map(|i| i.to_string()).collect();
        let list = format!("[{}]", items.join(", "));
        let cql = format!("INSERT INTO users (data) VALUES ({})", list);
        let result = parse_insert_statement(&cql);
        assert!(result.is_ok());
    }

    #[test]
    fn test_map_size_limit() {
        // Map with too many entries
        let pairs: Vec<String> = (0..=MAX_COLLECTION_SIZE)
            .map(|i| format!("{}: {}", i, i))
            .collect();
        let map = format!("{{{}}}", pairs.join(", "));
        let cql = format!("INSERT INTO users (data) VALUES ({})", map);
        let result = parse_insert_statement(&cql);
        // Should fail due to size limit
        assert!(result.is_err());
    }

    #[test]
    fn test_nested_map_depth_limit() {
        // Create nested map structure exceeding depth limit
        let mut nested = "{'k': 1}".to_string();
        for i in 0..40 {
            nested = format!("{{'key{}': {}}}", i, nested);
        }
        let cql = format!("INSERT INTO users (data) VALUES ({})", nested);
        let result = parse_insert_statement(&cql);
        // Should fail due to depth limit
        assert!(result.is_err());
    }

    // Unicode handling in string and identifier parsing

    #[test]
    fn test_string_literal_with_multibyte_utf8() {
        let cql = "INSERT INTO ks.t (name) VALUES ('héllo wörld')";
        let result = parse_insert_statement(cql);
        assert!(
            result.is_ok(),
            "Failed to parse string with accented chars: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_string_literal_with_emoji() {
        let cql = "INSERT INTO ks.t (name) VALUES ('hello 🌍 world')";
        let result = parse_insert_statement(cql);
        assert!(
            result.is_ok(),
            "Failed to parse string with emoji: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_string_literal_with_cjk() {
        let cql = "INSERT INTO ks.t (name) VALUES ('你好世界')";
        let result = parse_insert_statement(cql);
        assert!(
            result.is_ok(),
            "Failed to parse string with CJK chars: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_quoted_identifier_with_multibyte_utf8() {
        let cql = "INSERT INTO ks.t (\"nàme\") VALUES ('test')";
        let result = parse_insert_statement(cql);
        assert!(
            result.is_ok(),
            "Failed to parse quoted identifier with accented chars: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_quoted_identifier_with_emoji() {
        let cql = "INSERT INTO ks.t (\"col_🎉\") VALUES ('test')";
        let result = parse_insert_statement(cql);
        assert!(
            result.is_ok(),
            "Failed to parse quoted identifier with emoji: {:?}",
            result.err()
        );
    }

    // INSERT INTO ... JSON parsing

    #[test]
    fn test_insert_json_basic() {
        let cql = r#"INSERT INTO ks.t JSON '{"id": 1, "name": "test"}'"#;
        let result = parse_insert_statement(cql).unwrap();
        assert_eq!(result.table.keyspace.unwrap().name, "ks");
        assert_eq!(result.table.name.name, "t");
        assert!(result.columns.is_empty());
        match &result.values {
            CqlInsertValues::Json(s) => {
                assert_eq!(s, r#"{"id": 1, "name": "test"}"#);
            }
            CqlInsertValues::Values(_) => panic!("Expected Json variant"),
        }
    }

    #[test]
    fn test_insert_json_if_not_exists() {
        let cql = r#"INSERT INTO ks.t JSON '{"id": 1}' IF NOT EXISTS"#;
        let result = parse_insert_statement(cql).unwrap();
        assert!(result.if_not_exists);
        assert!(matches!(&result.values, CqlInsertValues::Json(_)));
    }

    #[test]
    fn test_insert_json_with_using_timestamp() {
        let cql = r#"INSERT INTO ks.t JSON '{"id": 1}' USING TIMESTAMP 12345"#;
        let result = parse_insert_statement(cql).unwrap();
        assert!(matches!(&result.values, CqlInsertValues::Json(_)));
        assert!(result.using.is_some());
    }

    #[test]
    fn test_insert_values_still_works() {
        // Ensure the JSON branch doesn't break normal VALUES parsing
        let cql = "INSERT INTO ks.t (id, name) VALUES (1, 'test')";
        let result = parse_insert_statement(cql).unwrap();
        assert!(matches!(&result.values, CqlInsertValues::Values(_)));
        assert_eq!(result.columns.len(), 2);
    }
}
