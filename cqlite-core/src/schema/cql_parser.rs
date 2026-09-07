//! CQL Schema Parser
//!
//! This module provides parsing capabilities for CQL CREATE TABLE statements
//! to extract table schema information including table names, column definitions,
//! partition keys, clustering keys, and type information.
//!
//! NOTE (file-size ratchet, epic #1116): this file is over the campsite
//! source-line threshold and a split-by-responsibility is pending under #1116.
//! Issue #2807 grew it by a small grammar fix (keyspace-qualified UDT names) and
//! ran the gate with `CQLITE_ALLOW_FILE_GROWTH=1`; splitting the parser is out of
//! scope for that bug fix.

use crate::cql::{CqlCreateTable, CqlDataType};
use crate::error::{Error, Result};
use crate::parser::types::CqlTypeId;
use crate::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use nom::{
    branch::alt,
    bytes::complete::{tag_no_case, take_while, take_while1},
    character::complete::char,
    combinator::{map, opt},
    multi::{separated_list0, separated_list1},
    sequence::{delimited, preceded, separated_pair, tuple},
    IResult,
};
use serde_json;
use std::collections::HashMap;

/// CQL keyword parser - case insensitive
fn keyword(s: &str) -> impl Fn(&str) -> IResult<&str, &str> + '_ {
    move |input| tag_no_case(s)(input)
}

/// Parse whitespace and comments
fn ws(input: &str) -> IResult<&str, &str> {
    take_while(|c: char| c.is_whitespace())(input)
}

/// Parse mandatory whitespace
fn ws1(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_whitespace())(input)
}

/// Parse identifier (table name, column name, etc.)
fn identifier(input: &str) -> IResult<&str, String> {
    let (input, name) = alt((
        // Quoted identifier
        delimited(char('"'), take_while1(|c: char| c != '"'), char('"')),
        // Unquoted identifier
        take_while1(|c: char| c.is_alphanumeric() || c == '_'),
    ))(input)?;

    Ok((input, name.to_string()))
}

/// Parse a qualified table name (keyspace.table or just table)
fn qualified_table_name(input: &str) -> IResult<&str, (Option<String>, String)> {
    let (input, first) = identifier(input)?;
    let (input, second) = opt(preceded(char('.'), identifier))(input)?;

    match second {
        Some(table) => Ok((input, (Some(first), table))),
        None => Ok((input, (None, first))),
    }
}

/// Parse a UDT type reference, which may be keyspace-qualified
/// (`keyspace.type_name`) or bare (`type_name`). Cassandra's `CREATE TABLE` /
/// `describe` output ALWAYS emits UDT column types keyspace-qualified
/// (e.g. `cassandra_easy_stress.addr`), so the grammar must accept the optional
/// `keyspace.` prefix (issue #2807). The qualified name is RETAINED verbatim in
/// the returned `data_type` string — the keyspace is information downstream
/// lookups need and MUST NOT be dropped here.
///
/// IMPORTANT for any NEW consumer of a `data_type` string: the registry is keyed
/// by BARE UDT name, and `CqlType::parse` does NOT split the qualifier (it yields
/// `Custom("udt:ks.addr")` intact). Only two places split `keyspace.udt`, both via
/// the single shared [`crate::schema::split_qualified_udt`]:
/// [`crate::schema::UdtRegistry::resolve_udt_reference`] (type resolution) and
/// [`crate::schema::TableSchema::validate_udt_references`] (`ensure_udt_exists`);
/// registry-backed *decode* lookups go through
/// [`crate::schema::UdtRegistry::get_udt_qualified`]. A new consumer that calls
/// `registry.get_udt(keyspace, name)` on a raw `data_type` WILL miss a qualified
/// reference and silently degrade the value to `Blob` — split first.
fn qualified_type_name(input: &str) -> IResult<&str, String> {
    let (input, first) = identifier(input)?;
    let (input, second) = opt(preceded(char('.'), identifier))(input)?;

    match second {
        Some(type_name) => Ok((input, format!("{}.{}", first, type_name))),
        None => Ok((input, first)),
    }
}

/// Maximum allowed CQL type nesting depth for the nom schema parser. Mirrors
/// [`crate::parser::complex_types::ComplexTypeParser`] (`max_depth = 32`) and the
/// [`crate::schema::CqlType`] string parser guard.
///
/// Without this bound, a hostile or malformed schema with pathological nesting
/// (e.g. `frozen<` × 50_000) recurses in `parse_type_inner` until the thread
/// stack overflows and, under `panic = "abort"`, aborts the whole process
/// instead of returning an error (issue #1690). The guard is
/// `depth > MAX_TYPE_NESTING_DEPTH`, where `depth` is 0 for the outermost type
/// and increments once per nesting level, so a leaf reached at exactly depth 32
/// (i.e. 32 levels of nesting) is the last allowed depth; a 33rd level returns a
/// nom failure (surfaced as `Err`).
const MAX_TYPE_NESTING_DEPTH: usize = 32;

/// Parse CQL data type
fn cql_type(input: &str) -> IResult<&str, String> {
    // Handle complex types like list<text>, map<text, bigint>, frozen<set<uuid>>.
    // `depth` bounds recursion so pathological nesting cannot overflow the stack.
    fn parse_type_inner(input: &str, depth: usize) -> IResult<&str, String> {
        if depth > MAX_TYPE_NESTING_DEPTH {
            // Unrecoverable failure so the enclosing `alt` short-circuits instead
            // of backtracking; surfaces to the caller as `Err` (never a panic).
            return Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::TooLarge,
            )));
        }

        let (input, base) = alt((
            // Collection types
            map(
                tuple((
                    alt((keyword("list"), keyword("set"))),
                    char('<'),
                    |i| parse_type_inner(i, depth + 1),
                    char('>'),
                )),
                |(collection, _, inner, _)| format!("{}<{}>", collection, inner),
            ),
            // Map type
            map(
                tuple((
                    keyword("map"),
                    char('<'),
                    |i| parse_type_inner(i, depth + 1),
                    char(','),
                    ws,
                    |i| parse_type_inner(i, depth + 1),
                    char('>'),
                )),
                |(_, _, key_type, _, _, value_type, _)| {
                    format!("map<{}, {}>", key_type, value_type)
                },
            ),
            // Tuple type
            map(
                tuple((
                    keyword("tuple"),
                    char('<'),
                    separated_list1(tuple((ws, char(','), ws)), |i| {
                        parse_type_inner(i, depth + 1)
                    }),
                    char('>'),
                )),
                |(_, _, types, _)| format!("tuple<{}>", types.join(", ")),
            ),
            // Frozen type
            map(
                tuple((
                    keyword("frozen"),
                    char('<'),
                    |i| parse_type_inner(i, depth + 1),
                    char('>'),
                )),
                |(_, _, inner, _)| format!("frozen<{}>", inner),
            ),
            // Vector type — `vector<element, n>` (Cassandra 5.0, `CQL3Type.java:589`).
            // Issue #4114: without this arm `vector` parsed as a bare
            // `qualified_type_name` and the grammar then failed on the `<`, so a
            // CREATE TABLE with a vector column could not be read at all (measured:
            // nom `code: Char`). The dimension is carried VERBATIM into the emitted
            // type string; `schema::vector_type` applies the dimension rules (an
            // illegal `0` is refused there, by name).
            map(
                tuple((
                    keyword("vector"),
                    char('<'),
                    |i| parse_type_inner(i, depth + 1),
                    tuple((ws, char(','), ws)),
                    nom::character::complete::digit1,
                    ws,
                    char('>'),
                )),
                |(_, _, element, _, dimension, _, _): (_, _, String, _, &str, _, _)| {
                    format!("vector<{}, {}>", element, dimension)
                },
            ),
            // Simple types and UDTs. UDT type names may be keyspace-qualified
            // (`keyspace.type_name`), which Cassandra always emits (issue #2807);
            // `qualified_type_name` accepts the optional prefix and retains it. The
            // retained qualifier is later split on a single `.` by
            // `UdtRegistry::get_udt_qualified`, which assumes an unquoted, dot-free
            // keyspace name (the only form the grammar / `describe` produce).
            qualified_type_name,
        ))(input)?;

        Ok((input, base))
    }

    let (input, _) = ws(input)?;
    let (input, type_name) = parse_type_inner(input, 0)?;
    let (input, _) = ws(input)?;

    Ok((input, type_name))
}

/// Crate-internal fuzz shim (issue #1614) exposing the module-private nom
/// [`cql_type`] parser so the in-crate `fuzz_support` block can drive it
/// directly against arbitrary strings (proving the `MAX_TYPE_NESTING_DEPTH`
/// guard returns `Err`, never a stack overflow). Not `pub` — reachable only
/// within cqlite-core, so it widens no external surface.
#[cfg(feature = "fuzz")]
pub(crate) fn cql_type_fuzz(input: &str) -> IResult<&str, String> {
    cql_type(input)
}

/// Parse column definition (with optional STATIC modifier and inline PRIMARY KEY)
/// Returns (name, data_type, is_static)
fn column_definition(input: &str) -> IResult<&str, (String, String, bool)> {
    let (input, _) = ws(input)?;
    let (input, name) = identifier(input)?;
    let (input, _) = ws1(input)?;
    let (input, data_type) = cql_type(input)?;
    let (input, _) = ws(input)?;

    // Check for STATIC modifier (Issue #255)
    let (input, is_static) = opt(keyword("static"))(input)?;
    let is_static = is_static.is_some();
    let (input, _) = ws(input)?;

    // Check for inline PRIMARY KEY (parse it but don't modify data_type)
    // The PRIMARY KEY constraint is tracked via partition_keys/clustering_keys, not in data_type
    let (input, _is_primary) = opt(tuple((keyword("primary"), ws1, keyword("key"))))(input)?;

    // Return the data_type as-is (e.g., "uuid", not "uuid PRIMARY KEY")
    // Issue #192: data_type must be a pure CQL type name for proper type matching
    Ok((input, (name, data_type, is_static)))
}

/// Parse PRIMARY KEY specification
fn primary_key_spec(input: &str) -> IResult<&str, (Vec<String>, Vec<String>)> {
    let (input, _) = ws(input)?;
    let (input, _) = keyword("primary")(input)?;
    let (input, _) = ws1(input)?;
    let (input, _) = keyword("key")(input)?;
    let (input, _) = ws(input)?;
    let (input, _) = char('(')(input)?;
    let (input, _) = ws(input)?;

    // Parse partition key (can be composite)
    let (input, partition_keys) = alt((
        // Composite partition key: ((col1, col2), clustering...)
        map(
            tuple((
                char('('),
                ws,
                separated_list1(tuple((ws, char(','), ws)), identifier),
                ws,
                char(')'),
            )),
            |(_, _, keys, _, _)| keys,
        ),
        // Single partition key: (col1, clustering...)
        map(identifier, |key| vec![key]),
    ))(input)?;

    let (input, _) = ws(input)?;

    // Parse clustering keys (optional)
    let (input, clustering_keys) = opt(preceded(
        tuple((char(','), ws)),
        separated_list1(tuple((ws, char(','), ws)), identifier),
    ))(input)?;

    let (input, _) = ws(input)?;
    let (input, _) = char(')')(input)?;

    Ok((input, (partition_keys, clustering_keys.unwrap_or_default())))
}

/// Parse a CQL map (`{ ... }`) or list (`[ ... ]`) literal value, capturing it
/// verbatim (including the delimiters). Handles nested `{}`/`[]` and single
/// quoted strings (which may themselves contain `{`, `}`, `[`, `]`). This lets
/// `table_options` skip past complex option values such as
/// `compression = {'class': 'LZ4Compressor'}` and continue collecting later
/// options (Issue #852 review finding).
fn bracketed_value(input: &str) -> IResult<&str, String> {
    let bytes: &[u8] = input.as_bytes();
    // Must start with an opening brace or bracket.
    match bytes.first() {
        Some(b'{') | Some(b'[') => {}
        _ => {
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Char,
            )))
        }
    }

    // Combined nesting depth across both `{}` and `[]`. The literal is complete
    // when depth returns to zero. Single-quoted strings are skipped so that
    // brackets/braces inside string contents do not affect nesting.
    let mut depth: usize = 0;
    let mut in_string = false;
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i];
        if in_string {
            if c == b'\'' {
                // CQL escapes a single quote by doubling it ('').
                if bytes.get(i + 1) == Some(&b'\'') {
                    i += 2;
                    continue;
                }
                in_string = false;
            }
            i += 1;
            continue;
        }
        match c {
            b'\'' => in_string = true,
            b'{' | b'[' => depth += 1,
            b'}' | b']' => {
                depth -= 1;
                if depth == 0 {
                    let end = i + 1;
                    let (value, rest) = input.split_at(end);
                    return Ok((rest, value.to_string()));
                }
            }
            _ => {}
        }
        i += 1;
    }

    // Unterminated literal.
    Err(nom::Err::Error(nom::error::Error::new(
        input,
        nom::error::ErrorKind::Char,
    )))
}

/// Parse a single-quoted CQL string literal, returning the un-escaped contents.
///
/// CQL escapes a single quote inside a single-quoted string by doubling it
/// (`''`). The naive `delimited(char('\''), take_while(|c| c != '\''), char('\''))`
/// stops at the first inner quote, which corrupts later parsing (e.g.
/// `comment = 'Bob''s table' AND ...` would leave `s table' AND ...`
/// unconsumed and silently drop subsequent options). This parser walks the
/// full literal, collapses each `''` to a single `'`, and consumes through the
/// terminating quote (Issue #852 branch-review finding).
fn single_quoted_string(input: &str) -> IResult<&str, String> {
    let bytes = input.as_bytes();
    if bytes.first() != Some(&b'\'') {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Char,
        )));
    }

    let mut value = String::new();
    let mut i = 1usize; // skip opening quote
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            // Doubled single-quote ('') is an escaped literal quote.
            if bytes.get(i + 1) == Some(&b'\'') {
                value.push('\'');
                i += 2;
                continue;
            }
            // Terminating quote.
            let rest = &input[i + 1..];
            return Ok((rest, value));
        }
        // Push the next UTF-8 char starting at byte index `i`. Slicing on a
        // char boundary is safe because the only multi-byte handling we do is
        // ASCII single-quote detection above.
        let ch_str = &input[i..];
        if let Some(ch) = ch_str.chars().next() {
            value.push(ch);
            i += ch.len_utf8();
        } else {
            break;
        }
    }

    // Unterminated string literal.
    Err(nom::Err::Error(nom::error::Error::new(
        input,
        nom::error::ErrorKind::Char,
    )))
}

/// Parse (and skip) a `CLUSTERING ORDER BY (col ASC|DESC, ...)` WITH item.
///
/// This item is not a `key = value` pair, so it must be matched explicitly;
/// otherwise the generic option parser fails on it, `separated_list0` returns
/// early, and any later `AND`-separated options (e.g. `bloom_filter_fp_chance`)
/// are silently dropped (Issue #852 branch-review finding). The clustering
/// order itself is captured for completeness so it is not lost, but the
/// per-column ordering is already tracked via the schema's clustering columns.
fn clustering_order_item(input: &str) -> IResult<&str, (String, String)> {
    let (input, _) = keyword("clustering")(input)?;
    let (input, _) = ws1(input)?;
    let (input, _) = keyword("order")(input)?;
    let (input, _) = ws1(input)?;
    let (input, _) = keyword("by")(input)?;
    let (input, _) = ws(input)?;

    // Capture the parenthesized column-ordering list verbatim, stopping at the
    // matching close paren. A naive `take_while(|c| c != ')')` truncates the body
    // at the first `)`, but a quoted clustering identifier may itself contain a
    // `)` (e.g. `CLUSTERING ORDER BY ("C)k" DESC)`). Treating that inner `)` as
    // the clause terminator would drop the DESC direction and silently fall back
    // to ASC (Issue #852 branch-review finding). The scan below skips over single-
    // and double-quoted identifiers (honoring CQL's doubled-quote escaping) so a
    // `)` inside a quoted name is not mistaken for the clause terminator.
    let (input, _) = char('(')(input)?;
    let (input, body) = clustering_order_body_scan(input)?;
    let (input, _) = char(')')(input)?;

    Ok((
        input,
        (
            "clustering order by".to_string(),
            format!("({})", body.trim()),
        ),
    ))
}

/// Scan the body of a `CLUSTERING ORDER BY (...)` clause up to (but not
/// including) its matching close paren, treating `)` inside single- or
/// double-quoted identifiers as literal content rather than the terminator.
///
/// CQL escapes a quote inside a quoted identifier/string by doubling it (`""`
/// for double-quoted identifiers, `''` for single-quoted strings), so a doubled
/// quote is consumed as content and does not close the quoted span.
fn clustering_order_body_scan(input: &str) -> IResult<&str, &str> {
    let bytes = input.as_bytes();
    let mut i = 0usize;
    // The active quote character when inside a quoted span (`'` or `"`).
    let mut quote: Option<u8> = None;
    while i < bytes.len() {
        let c = bytes[i];
        match quote {
            Some(q) => {
                if c == q {
                    // Doubled quote is an escaped literal quote, not the close.
                    if bytes.get(i + 1) == Some(&q) {
                        i += 2;
                        continue;
                    }
                    quote = None;
                }
            }
            None => match c {
                b'\'' | b'"' => quote = Some(c),
                b')' => {
                    let (body, rest) = input.split_at(i);
                    return Ok((rest, body));
                }
                _ => {}
            },
        }
        i += 1;
    }

    // No (unquoted) close paren found — unterminated clause body.
    Err(nom::Err::Error(nom::error::Error::new(
        input,
        nom::error::ErrorKind::Char,
    )))
}

/// Parse the body of a `CLUSTERING ORDER BY (...)` clause (the text captured
/// between the parentheses, e.g. `ck DESC` or `c1 ASC, c2 DESC`) into a map of
/// column name -> [`ClusteringOrder`]. Each entry's column name is parsed with
/// the same [`identifier`] parser used for column and primary-key names, so
/// quoted identifiers (e.g. `"Ck"`) have their surrounding quotes stripped and
/// therefore match the clustering column names stored in the schema. Entries
/// with an unrecognized/absent direction default to ASC (per
/// [`ClusteringOrder::from`]).
fn parse_clustering_order_body(body: &str) -> HashMap<String, ClusteringOrder> {
    // Strip the surrounding parens that `clustering_order_item` re-added, if present.
    let inner = body.trim().trim_start_matches('(').trim_end_matches(')');

    // Parse the comma-separated list with nom rather than `inner.split(',')`,
    // because a quoted clustering identifier may itself contain a comma
    // (e.g. `"C,k" DESC`). Splitting on raw commas would break such a name
    // into two entries and silently leave the column at its default ASC
    // (Issue #852 branch-review finding). The shared `identifier` parser
    // already handles quoted identifiers (including embedded commas), so a
    // `separated_list0` over `clustering_order_entry` parses each entry
    // correctly while keeping behavior identical for all normal cases.
    let entries = separated_list0(tuple((ws, char(','), ws)), clustering_order_entry);
    match delimited(ws, entries, ws)(inner) {
        Ok((_, items)) => items.into_iter().collect(),
        Err(_) => HashMap::new(),
    }
}

/// Parse a single `CLUSTERING ORDER BY` entry: a column identifier followed by
/// an optional `ASC`/`DESC` direction (defaulting to ASC). The column name is
/// parsed with the canonical [`identifier`] parser so quoted names are unquoted
/// exactly as the schema's clustering columns were, and quoted names containing
/// a comma are kept intact.
fn clustering_order_entry(input: &str) -> IResult<&str, (String, ClusteringOrder)> {
    let (input, col) = identifier(input)?;
    // Optional whitespace + direction keyword. Absent/unrecognized → ASC.
    let (input, direction) = opt(preceded(ws1, alt((keyword("asc"), keyword("desc")))))(input)?;
    let order = direction
        .map(crate::schema::ClusteringOrder::from)
        .unwrap_or(crate::schema::ClusteringOrder::Asc);
    Ok((input, (col, order)))
}

/// Parse table options (WITH clause)
fn table_options(input: &str) -> IResult<&str, HashMap<String, String>> {
    let (input, _) = ws(input)?;
    let (input, _) = keyword("with")(input)?;
    let (input, _) = ws1(input)?;

    // Parse option = value pairs
    let option_pair = map(
        separated_pair(
            identifier,
            tuple((ws, char('='), ws)),
            alt((
                // Map literal: {'class': 'LZ4Compressor', ...} (possibly nested).
                // Captured verbatim so option collection can continue past it.
                bracketed_value,
                // String value (handles doubled-single-quote `''` escaping).
                single_quoted_string,
                // Numeric or identifier value
                map(
                    take_while1(|c: char| c.is_alphanumeric() || c == '_' || c == '.'),
                    |s: &str| s.to_string(),
                ),
            )),
        ),
        |(key, value)| (key, value),
    );

    // A WITH item is either `CLUSTERING ORDER BY (...)` or a `key = value` pair.
    // Both must be matched so a non-`key=value` item never stops collection and
    // silently drops later `AND`-separated options (Issue #852).
    let with_item = alt((clustering_order_item, option_pair));

    let (input, options) = separated_list0(tuple((ws, keyword("and"), ws)), with_item)(input)?;

    Ok((input, options.into_iter().collect()))
}

/// Split CQL file content into individual statements (semicolon-delimited)
/// Respects string literals and comments to avoid splitting inside them
pub fn split_cql_statements(input: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current_statement = String::new();
    let mut in_string = false;
    let mut in_single_line_comment = false;
    let mut in_multi_line_comment = false;
    let mut escape_next = false;

    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let c = chars[i];

        // Handle escape sequences in strings
        if escape_next {
            current_statement.push(c);
            escape_next = false;
            i += 1;
            continue;
        }

        // Check for multi-line comment start
        if !in_string
            && !in_single_line_comment
            && !in_multi_line_comment
            && i + 1 < chars.len()
            && c == '/'
            && chars[i + 1] == '*'
        {
            in_multi_line_comment = true;
            current_statement.push(c);
            current_statement.push(chars[i + 1]);
            i += 2;
            continue;
        }

        // Check for multi-line comment end
        if in_multi_line_comment && i + 1 < chars.len() && c == '*' && chars[i + 1] == '/' {
            in_multi_line_comment = false;
            current_statement.push(c);
            current_statement.push(chars[i + 1]);
            i += 2;
            continue;
        }

        // Check for single-line comment start
        if !in_string
            && !in_multi_line_comment
            && !in_single_line_comment
            && i + 1 < chars.len()
            && c == '-'
            && chars[i + 1] == '-'
        {
            in_single_line_comment = true;
            current_statement.push(c);
            current_statement.push(chars[i + 1]);
            i += 2;
            continue;
        }

        // Handle newline (ends single-line comment)
        if c == '\n' {
            in_single_line_comment = false;
            current_statement.push(c);
            i += 1;
            continue;
        }

        // Skip processing if inside a comment
        if in_single_line_comment || in_multi_line_comment {
            current_statement.push(c);
            i += 1;
            continue;
        }

        // Handle string literals (single quotes)
        if c == '\'' {
            in_string = !in_string;
            current_statement.push(c);
            i += 1;
            continue;
        }

        // Handle escape in string
        if in_string && c == '\\' {
            escape_next = true;
            current_statement.push(c);
            i += 1;
            continue;
        }

        // Handle semicolon (statement separator)
        if !in_string && c == ';' {
            let trimmed = current_statement.trim();
            if !trimmed.is_empty() {
                statements.push(trimmed.to_string());
            }
            current_statement.clear();
            i += 1;
            continue;
        }

        current_statement.push(c);
        i += 1;
    }

    // Add final statement if non-empty
    let trimmed = current_statement.trim();
    if !trimmed.is_empty() {
        statements.push(trimmed.to_string());
    }

    // Clean up statements: remove leading/trailing comment-only lines
    statements
        .into_iter()
        .map(|stmt| strip_leading_trailing_comments(&stmt))
        .filter(|s| !s.is_empty())
        .collect()
}

/// Strip leading and trailing comment-only lines from a statement
fn strip_leading_trailing_comments(stmt: &str) -> String {
    let lines: Vec<&str> = stmt.lines().collect();
    let mut start = 0;
    let mut end = lines.len();

    // Find first non-comment line
    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim();
        if !trimmed.is_empty() && !trimmed.starts_with("--") && !trimmed.starts_with("/*") {
            start = i;
            break;
        }
    }

    // Find last non-comment line
    for (i, line) in lines.iter().enumerate().rev() {
        let trimmed = line.trim();
        if !trimmed.is_empty() && !trimmed.starts_with("--") && !trimmed.ends_with("*/") {
            end = i + 1;
            break;
        }
    }

    if start >= end {
        return String::new();
    }

    lines[start..end].join("\n")
}

#[cfg(test)]
mod tests_splitter {
    use super::*;

    #[test]
    fn test_split_with_comments() {
        let cql = r#"
        -- Comment
        CREATE TYPE test.udt (field text);

        /* Multi-line
           comment */
        CREATE TABLE test.tbl (id int PRIMARY KEY);
        "#;

        let stmts = split_cql_statements(cql);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("CREATE TYPE"));
        assert!(!stmts[0].contains("--"));
        assert!(stmts[1].contains("CREATE TABLE"));
    }
}

/// Statement type classification
#[derive(Debug, Clone, PartialEq)]
pub enum StatementType {
    CreateTable,
    CreateType,
    Other(String),
}

/// Classify a CQL statement by type
pub fn classify_statement(statement: &str) -> StatementType {
    let normalized = statement.trim().to_lowercase();

    // Remove leading whitespace and comments
    let normalized = normalized
        .lines()
        .map(|line| {
            // Remove single-line comments
            if let Some(pos) = line.find("--") {
                &line[..pos]
            } else {
                line
            }
        })
        .collect::<Vec<&str>>()
        .join(" ");

    let normalized = normalized.trim();

    if normalized.starts_with("create table")
        || normalized.starts_with("create table if not exists")
    {
        StatementType::CreateTable
    } else if normalized.starts_with("create type")
        || normalized.starts_with("create type if not exists")
    {
        StatementType::CreateType
    } else {
        StatementType::Other(
            normalized
                .split_whitespace()
                .next()
                .unwrap_or("unknown")
                .to_string(),
        )
    }
}

/// Parse CREATE TYPE statement to extract UDT definition
#[allow(clippy::type_complexity)]
pub fn parse_create_type(
    input: &str,
) -> IResult<&str, (String, Option<String>, Vec<(String, String)>)> {
    let (input, _) = ws(input)?;
    let (input, _) = keyword("create")(input)?;
    let (input, _) = ws1(input)?;
    let (input, _) = keyword("type")(input)?;
    let (input, _) = ws1(input)?;

    // Optional IF NOT EXISTS
    let (input, _) = opt(tuple((
        keyword("if"),
        ws1,
        keyword("not"),
        ws1,
        keyword("exists"),
        ws1,
    )))(input)?;

    // Type name (qualified or unqualified)
    let (input, (keyspace, type_name)) = qualified_table_name(input)?;

    let (input, _) = ws(input)?;
    let (input, _) = char('(')(input)?;
    let (input, _) = ws(input)?;

    // Parse field definitions
    let (input, fields) = separated_list1(
        tuple((ws, char(','), ws)),
        map(
            tuple((identifier, ws1, cql_type)),
            |(name, _, field_type)| (name, field_type),
        ),
    )(input)?;

    let (input, _) = ws(input)?;
    let (input, _) = char(')')(input)?;

    Ok((input, (type_name, keyspace, fields)))
}

/// Parse a complete CREATE TABLE statement
pub fn parse_create_table(input: &str) -> IResult<&str, TableSchema> {
    let (input, _) = ws(input)?;
    let (input, _) = keyword("create")(input)?;
    let (input, _) = ws1(input)?;
    let (input, _) = keyword("table")(input)?;
    let (input, _) = ws1(input)?;

    // Optional IF NOT EXISTS
    let (input, _) = opt(tuple((
        keyword("if"),
        ws1,
        keyword("not"),
        ws1,
        keyword("exists"),
        ws1,
    )))(input)?;

    // Table name (qualified or unqualified)
    let (input, (keyspace, table_name)) = qualified_table_name(input)?;

    let (input, _) = ws(input)?;
    let (input, _) = char('(')(input)?;
    let (input, _) = ws(input)?;

    // Parse column definitions and constraints
    // Columns are stored as (name, data_type, is_static)
    let mut columns: Vec<(String, String, bool)> = Vec::new();
    let mut partition_keys = Vec::new();
    let mut clustering_keys = Vec::new();
    let mut primary_key_found = false;

    let (input, items) = separated_list1(
        tuple((ws, char(','), ws)),
        alt((
            // Primary key constraint - returns 3-tuple with is_static=false (unused)
            map(primary_key_spec, |keys| {
                (
                    "PRIMARY_KEY".to_string(),
                    serde_json::to_string(&keys).unwrap_or_default(),
                    false, // is_static not applicable for PRIMARY KEY constraint
                )
            }),
            // Column definition - returns (name, data_type, is_static)
            column_definition,
        )),
    )(input)?;

    // Process parsed items
    for (name, value, is_static) in items {
        if name == "PRIMARY_KEY" {
            // Parse the JSON-encoded key specification
            if let Ok(keys_tuple) = serde_json::from_str::<(Vec<String>, Vec<String>)>(&value) {
                partition_keys = keys_tuple.0;
                clustering_keys = keys_tuple.1;
                primary_key_found = true;
            }
            continue;
        }
        columns.push((name, value, is_static));
    }

    let (input, _) = ws(input)?;
    let (input, _) = char(')')(input)?;

    // Parse optional WITH clause. The parsed key=value pairs (e.g.
    // `bloom_filter_fp_chance = 1.0`) are preserved on the schema's `comments`
    // bag so the writer can honor them (Issue #852). Keys are normalized to
    // lowercase since CQL option names are case-insensitive.
    let (input, with_options) = opt(table_options)(input)?;

    // Extract per-column clustering order from the captured `CLUSTERING ORDER BY
    // (...)` option so it can be applied to each clustering column below. Columns
    // not named in the clause default to ASC (#849/#852 branch-review).
    let clustering_order_map: HashMap<String, ClusteringOrder> = with_options
        .as_ref()
        .and_then(|opts| opts.get("clustering order by"))
        .map(|body| parse_clustering_order_body(body))
        .unwrap_or_default();

    // If no primary key was found in constraints, look for inline PRIMARY KEY or use first column
    if !primary_key_found && !columns.is_empty() {
        // Check if any column has "PRIMARY KEY" in its type (inline definition)
        let mut found_inline = false;
        for (col_name, col_type, _is_static) in &columns {
            if col_type.to_lowercase().contains("primary key") {
                partition_keys.push(col_name.clone());
                found_inline = true;
                break;
            }
        }

        // If still no primary key found, assume first column is partition key
        if !found_inline {
            partition_keys.push(columns[0].0.clone());
        }
    }

    // Build schema
    let schema = TableSchema {
        keyspace: keyspace.unwrap_or_else(|| "default".to_string()),
        table: table_name,
        partition_keys: partition_keys
            .into_iter()
            .enumerate()
            .map(|(pos, name)| {
                let data_type = columns
                    .iter()
                    .find(|(col_name, _, _)| col_name == &name)
                    .map(|(_, dt, _)| dt.clone())
                    .unwrap_or_else(|| "text".to_string());

                KeyColumn {
                    name,
                    data_type,
                    position: pos,
                }
            })
            .collect(),
        clustering_keys: clustering_keys
            .into_iter()
            .enumerate()
            .map(|(pos, name)| {
                let data_type = columns
                    .iter()
                    .find(|(col_name, _, _)| col_name == &name)
                    .map(|(_, dt, _)| dt.clone())
                    .unwrap_or_else(|| "text".to_string());

                let order = clustering_order_map
                    .get(&name)
                    .cloned()
                    .unwrap_or(crate::schema::ClusteringOrder::Asc);

                ClusteringColumn {
                    name,
                    data_type,
                    position: pos,
                    order,
                }
            })
            .collect(),
        columns: columns
            .into_iter()
            .map(|(name, data_type_with_constraints, is_static)| {
                // Remove PRIMARY KEY constraint from data type
                let data_type = if data_type_with_constraints
                    .to_lowercase()
                    .contains("primary key")
                {
                    data_type_with_constraints
                        .to_lowercase()
                        .replace("primary key", "")
                        .trim()
                        .to_string()
                } else {
                    data_type_with_constraints
                };

                Column {
                    name,
                    data_type,
                    nullable: true,
                    default: None,
                    is_static,
                }
            })
            .collect(),
        comments: with_options
            .unwrap_or_default()
            .into_iter()
            .map(|(k, v)| (k.to_lowercase(), v))
            .collect(),
        dropped_columns: std::collections::HashMap::new(),
    };

    Ok((input, schema))
}

/// Convert CQL type string to internal CqlTypeId
pub fn cql_type_to_type_id(cql_type: &str) -> Result<CqlTypeId> {
    cql_type_to_type_id_with_depth(cql_type, 0)
}

/// Recursive body of [`cql_type_to_type_id`], threading the current nesting
/// `depth` so the `frozen<...>` unwrap is bounded at [`MAX_TYPE_NESTING_DEPTH`]
/// (issue #1690). Without this bound a pathological `frozen<` × N string reaching
/// this public conversion path (via `SchemaManager::cql_type_to_internal`) would
/// recurse until the stack overflows and, under `panic = "abort"`, abort the
/// whole process instead of returning an error. `depth` is 0 at the top level and
/// increments by one per nesting level.
fn cql_type_to_type_id_with_depth(cql_type: &str, depth: usize) -> Result<CqlTypeId> {
    if depth > MAX_TYPE_NESTING_DEPTH {
        return Err(Error::schema(format!(
            "type nesting too deep (max {})",
            MAX_TYPE_NESTING_DEPTH
        )));
    }

    let type_lower = cql_type.trim().to_lowercase();

    // Handle collection types
    if type_lower.starts_with("list<") {
        return Ok(CqlTypeId::List);
    }
    if type_lower.starts_with("set<") {
        return Ok(CqlTypeId::Set);
    }
    if type_lower.starts_with("map<") {
        return Ok(CqlTypeId::Map);
    }
    if type_lower.starts_with("tuple<") {
        return Ok(CqlTypeId::Tuple);
    }
    if type_lower.starts_with("frozen<") {
        // Extract inner type from frozen<type>
        if let Some(inner_start) = type_lower.find('<') {
            if let Some(inner_end) = type_lower.rfind('>') {
                let inner_type = &type_lower[inner_start + 1..inner_end];
                return cql_type_to_type_id_with_depth(inner_type, depth + 1);
            }
        }
    }

    // Handle primitive types
    match type_lower.as_str() {
        "ascii" => Ok(CqlTypeId::Ascii),
        "bigint" | "long" => Ok(CqlTypeId::BigInt),
        "blob" => Ok(CqlTypeId::Blob),
        "boolean" | "bool" => Ok(CqlTypeId::Boolean),
        "counter" => Ok(CqlTypeId::Counter),
        "decimal" => Ok(CqlTypeId::Decimal),
        "double" => Ok(CqlTypeId::Double),
        "float" => Ok(CqlTypeId::Float),
        "int" | "integer" => Ok(CqlTypeId::Int),
        "timestamp" => Ok(CqlTypeId::Timestamp),
        "uuid" => Ok(CqlTypeId::Uuid),
        "varchar" | "text" => Ok(CqlTypeId::Varchar),
        "varint" => Ok(CqlTypeId::Varint),
        "timeuuid" => Ok(CqlTypeId::Timeuuid),
        "inet" => Ok(CqlTypeId::Inet),
        "date" => Ok(CqlTypeId::Date),
        "time" => Ok(CqlTypeId::Time),
        "smallint" => Ok(CqlTypeId::Smallint),
        "tinyint" => Ok(CqlTypeId::Tinyint),
        "duration" => Ok(CqlTypeId::Duration),
        _ => {
            // Assume it's a UDT if not a known primitive type
            Ok(CqlTypeId::Udt)
        }
    }
}

/// Extract table name from CQL CREATE TABLE statement
pub fn extract_table_name(cql: &str) -> Result<(Option<String>, String)> {
    match parse_create_table(cql) {
        Ok((_, schema)) => {
            let keyspace = if schema.keyspace == "default" {
                None
            } else {
                Some(schema.keyspace)
            };
            Ok((keyspace, schema.table))
        }
        Err(_) => {
            // Fallback: simple regex-like extraction
            let cql_lower = cql.to_lowercase();
            if let Some(table_start) = cql_lower.find("create table") {
                let after_table = &cql[table_start + 12..];
                if let Some(if_not_exists) = after_table.find("if not exists") {
                    let after_if = &after_table[if_not_exists + 13..];
                    return extract_simple_table_name(after_if);
                }
                return extract_simple_table_name(after_table);
            }

            Err(Error::schema(
                "Failed to extract table name from CQL".to_string(),
            ))
        }
    }
}

/// Simple table name extraction fallback
fn extract_simple_table_name(input: &str) -> Result<(Option<String>, String)> {
    let trimmed = input.trim();
    let words: Vec<&str> = trimmed.split_whitespace().collect();

    if words.is_empty() {
        return Err(Error::schema("No table name found".to_string()));
    }

    let table_name = words[0];

    // Handle qualified names
    if let Some(dot_pos) = table_name.find('.') {
        let keyspace = &table_name[..dot_pos];
        let table = &table_name[dot_pos + 1..];
        Ok((Some(keyspace.to_string()), table.to_string()))
    } else {
        Ok((None, table_name.to_string()))
    }
}

/// Check if a table name matches the given pattern
pub fn table_name_matches(
    schema_keyspace: &Option<String>,
    schema_table: &str,
    target_keyspace: &Option<String>,
    target_table: &str,
) -> bool {
    // Table name must match exactly
    if schema_table != target_table {
        return false;
    }

    // If target has no keyspace, match any keyspace
    if target_keyspace.is_none() {
        return true;
    }

    // If both have keyspaces, they must match
    schema_keyspace == target_keyspace
}

/// Parse CQL schema and extract metadata for SSTable reading
pub fn parse_cql_schema(cql: &str) -> Result<TableSchema> {
    match parse_create_table(cql) {
        Ok((_, schema)) => {
            // Validate the parsed schema
            schema.validate()?;
            Ok(schema)
        }
        Err(nom::Err::Error(e) | nom::Err::Failure(e)) => Err(Error::schema(format!(
            "Failed to parse CQL schema: {:?}",
            e
        ))),
        Err(nom::Err::Incomplete(_)) => Err(Error::schema("Incomplete CQL schema".to_string())),
    }
}

/// Parse CQL schema using the visitor pattern (preferred method for new code)
///
/// This function demonstrates how to use the visitor pattern for AST-based parsing.
/// It provides better error handling, validation, and is more maintainable than
/// the legacy nom-based parser.
pub fn parse_cql_schema_with_visitor(cql: &str) -> Result<TableSchema> {
    // Note: This is a demonstration function. In a complete implementation,
    // you would first parse the CQL into an AST using the nom parser,
    // then use the visitor pattern to convert it to TableSchema.
    //
    // For now, this uses the existing nom parser for demonstration purposes.

    use crate::cql::traits::CqlVisitor;
    use crate::cql::visitor::SchemaBuilderVisitor;
    use crate::cql::CqlStatement;

    // Parse using the existing nom parser to get the TableSchema
    let schema = parse_cql_schema(cql)?;

    // Demonstrate the visitor pattern by reconstructing the AST and then using the visitor
    // (In real usage, you would have the AST from a parser)
    let ast = table_schema_to_ast(&schema)?;
    let statement = CqlStatement::CreateTable(ast);

    // Use the visitor to convert AST back to TableSchema
    let mut visitor = SchemaBuilderVisitor;
    visitor.visit_statement(&statement)
}

/// Helper function to convert TableSchema to AST for demonstration
/// (In real usage, the AST would come directly from a parser)
fn table_schema_to_ast(schema: &TableSchema) -> Result<CqlCreateTable> {
    use crate::cql::{
        CqlColumnDef, CqlCreateTable, CqlIdentifier, CqlPrimaryKey, CqlTable, CqlTableOptions,
    };

    // Convert table reference
    let table = if schema.keyspace == "default" {
        CqlTable::new(&schema.table)
    } else {
        CqlTable::with_keyspace(&schema.keyspace, &schema.table)
    };

    // Convert columns
    let columns: Result<Vec<CqlColumnDef>> = schema
        .columns
        .iter()
        .map(|col| {
            Ok(CqlColumnDef {
                name: CqlIdentifier::new(&col.name),
                data_type: string_to_cql_data_type(&col.data_type)?,
                is_static: col.is_static,
            })
        })
        .collect();

    let columns = columns?;

    // Convert primary key
    let partition_key: Vec<CqlIdentifier> = schema
        .partition_keys
        .iter()
        .map(|pk| CqlIdentifier::new(&pk.name))
        .collect();

    let clustering_key: Vec<CqlIdentifier> = schema
        .clustering_keys
        .iter()
        .map(|ck| CqlIdentifier::new(&ck.name))
        .collect();

    Ok(CqlCreateTable {
        if_not_exists: false,
        table,
        columns,
        primary_key: CqlPrimaryKey {
            partition_key,
            clustering_key,
        },
        options: CqlTableOptions {
            options: HashMap::new(),
        },
    })
}

/// Convert string type to CqlDataType (simplified version)
fn string_to_cql_data_type(type_str: &str) -> Result<CqlDataType> {
    use crate::cql::{CqlDataType, CqlIdentifier};

    let type_lower = type_str.trim().to_lowercase();

    // Handle collection types
    if type_lower.starts_with("list<") && type_lower.ends_with('>') {
        let inner_type_str = &type_lower[5..type_lower.len() - 1];
        let inner_type = string_to_cql_data_type(inner_type_str)?;
        return Ok(CqlDataType::List(Box::new(inner_type)));
    }

    if type_lower.starts_with("set<") && type_lower.ends_with('>') {
        let inner_type_str = &type_lower[4..type_lower.len() - 1];
        let inner_type = string_to_cql_data_type(inner_type_str)?;
        return Ok(CqlDataType::Set(Box::new(inner_type)));
    }

    if type_lower.starts_with("map<") && type_lower.ends_with('>') {
        let inner = &type_lower[4..type_lower.len() - 1];
        if let Some(comma_pos) = inner.find(',') {
            let key_type_str = inner[..comma_pos].trim();
            let value_type_str = inner[comma_pos + 1..].trim();
            let key_type = string_to_cql_data_type(key_type_str)?;
            let value_type = string_to_cql_data_type(value_type_str)?;
            return Ok(CqlDataType::Map(Box::new(key_type), Box::new(value_type)));
        }
    }

    if type_lower.starts_with("frozen<") && type_lower.ends_with('>') {
        let inner_type_str = &type_lower[7..type_lower.len() - 1];
        let inner_type = string_to_cql_data_type(inner_type_str)?;
        return Ok(CqlDataType::Frozen(Box::new(inner_type)));
    }

    // Handle primitive types
    match type_lower.as_str() {
        "boolean" | "bool" => Ok(CqlDataType::Boolean),
        "tinyint" => Ok(CqlDataType::TinyInt),
        "smallint" => Ok(CqlDataType::SmallInt),
        "int" => Ok(CqlDataType::Int),
        "bigint" | "long" => Ok(CqlDataType::BigInt),
        "varint" => Ok(CqlDataType::Varint),
        "decimal" => Ok(CqlDataType::Decimal),
        "float" => Ok(CqlDataType::Float),
        "double" => Ok(CqlDataType::Double),
        "text" | "varchar" => Ok(CqlDataType::Text),
        "ascii" => Ok(CqlDataType::Ascii),
        "blob" => Ok(CqlDataType::Blob),
        "timestamp" => Ok(CqlDataType::Timestamp),
        "date" => Ok(CqlDataType::Date),
        "time" => Ok(CqlDataType::Time),
        "uuid" => Ok(CqlDataType::Uuid),
        "timeuuid" => Ok(CqlDataType::TimeUuid),
        "inet" => Ok(CqlDataType::Inet),
        "duration" => Ok(CqlDataType::Duration),
        "counter" => Ok(CqlDataType::Counter),
        _ => {
            // Assume it's a UDT
            Ok(CqlDataType::Udt(CqlIdentifier::new(type_str)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_table_parsing() {
        let cql = r#"
            CREATE TABLE users (
                id uuid PRIMARY KEY,
                name text,
                email text
            )
        "#;

        let schema = parse_cql_schema(cql).unwrap();
        assert_eq!(schema.table, "users");
        assert_eq!(schema.columns.len(), 3);
        assert_eq!(schema.partition_keys.len(), 1);
        assert_eq!(schema.partition_keys[0].name, "id");
    }

    #[test]
    fn test_qualified_table_name() {
        let cql = r#"
            CREATE TABLE myapp.users (
                id bigint PRIMARY KEY,
                name text
            )
        "#;

        let schema = parse_cql_schema(cql).unwrap();
        assert_eq!(schema.keyspace, "myapp");
        assert_eq!(schema.table, "users");
    }

    #[test]
    fn test_complex_types() {
        let cql = r#"
            CREATE TABLE complex_table (
                id uuid PRIMARY KEY,
                tags set<text>,
                metadata map<text, text>,
                coordinates list<double>
            )
        "#;

        let schema = parse_cql_schema(cql).unwrap();
        assert_eq!(schema.columns.len(), 4);

        let tags_col = schema.columns.iter().find(|c| c.name == "tags").unwrap();
        assert_eq!(tags_col.data_type, "set<text>");

        let metadata_col = schema
            .columns
            .iter()
            .find(|c| c.name == "metadata")
            .unwrap();
        assert_eq!(metadata_col.data_type, "map<text, text>");
    }

    #[test]
    fn test_table_name_extraction() {
        let cql = "CREATE TABLE IF NOT EXISTS myapp.users (id uuid PRIMARY KEY)";
        let (keyspace, table) = extract_table_name(cql).unwrap();
        assert_eq!(keyspace, Some("myapp".to_string()));
        assert_eq!(table, "users");
    }

    #[test]
    fn test_cql_type_conversion() {
        assert_eq!(cql_type_to_type_id("text").unwrap(), CqlTypeId::Varchar);
        assert_eq!(cql_type_to_type_id("bigint").unwrap(), CqlTypeId::BigInt);
        assert_eq!(cql_type_to_type_id("list<text>").unwrap(), CqlTypeId::List);
        assert_eq!(
            cql_type_to_type_id("frozen<set<uuid>>").unwrap(),
            CqlTypeId::Set
        );
    }

    #[test]
    fn test_table_name_matching() {
        // Exact match
        assert!(table_name_matches(
            &Some("ks".to_string()),
            "users",
            &Some("ks".to_string()),
            "users"
        ));

        // Match with wildcard keyspace
        assert!(table_name_matches(
            &Some("ks".to_string()),
            "users",
            &None,
            "users"
        ));

        // No match - different table
        assert!(!table_name_matches(
            &Some("ks".to_string()),
            "users",
            &Some("ks".to_string()),
            "orders"
        ));

        // No match - different keyspace
        assert!(!table_name_matches(
            &Some("ks1".to_string()),
            "users",
            &Some("ks2".to_string()),
            "users"
        ));
    }

    #[test]
    fn test_composite_primary_key() {
        let cql = r#"
            CREATE TABLE time_series (
                partition_key text,
                clustering_key timestamp,
                value double,
                PRIMARY KEY (partition_key, clustering_key)
            )
        "#;

        let schema = parse_cql_schema(cql).unwrap();
        assert_eq!(schema.partition_keys.len(), 1);
        assert_eq!(schema.clustering_keys.len(), 1);

        assert_eq!(schema.partition_keys[0].name, "partition_key");
        assert_eq!(schema.clustering_keys[0].name, "clustering_key");
    }

    #[test]
    fn test_frozen_collections() {
        let cql = r#"
            CREATE TABLE frozen_test (
                id uuid PRIMARY KEY,
                frozen_set frozen<set<text>>,
                frozen_map frozen<map<text, bigint>>,
                nested_frozen frozen<list<frozen<set<uuid>>>>
            )
        "#;

        let schema = parse_cql_schema(cql).unwrap();

        let frozen_set = schema
            .columns
            .iter()
            .find(|c| c.name == "frozen_set")
            .unwrap();
        assert_eq!(frozen_set.data_type, "frozen<set<text>>");

        let frozen_map = schema
            .columns
            .iter()
            .find(|c| c.name == "frozen_map")
            .unwrap();
        assert_eq!(frozen_map.data_type, "frozen<map<text, bigint>>");

        let nested = schema
            .columns
            .iter()
            .find(|c| c.name == "nested_frozen")
            .unwrap();
        assert_eq!(nested.data_type, "frozen<list<frozen<set<uuid>>>>");
    }

    #[test]
    fn test_udt_columns() {
        let cql = r#"
            CREATE TABLE user_profiles (
                user_id uuid PRIMARY KEY,
                address address_type,
                preferences frozen<user_prefs>
            )
        "#;

        let schema = parse_cql_schema(cql).unwrap();

        let address_col = schema.columns.iter().find(|c| c.name == "address").unwrap();
        assert_eq!(address_col.data_type, "address_type");

        let prefs_col = schema
            .columns
            .iter()
            .find(|c| c.name == "preferences")
            .unwrap();
        assert_eq!(prefs_col.data_type, "frozen<user_prefs>");
    }

    #[test]
    fn test_tuple_types() {
        let cql = r#"
            CREATE TABLE tuple_test (
                id uuid PRIMARY KEY,
                coordinates tuple<double, double>,
                person_info tuple<text, int, boolean>
            )
        "#;

        let schema = parse_cql_schema(cql).unwrap();

        let coords = schema
            .columns
            .iter()
            .find(|c| c.name == "coordinates")
            .unwrap();
        assert_eq!(coords.data_type, "tuple<double, double>");

        let person = schema
            .columns
            .iter()
            .find(|c| c.name == "person_info")
            .unwrap();
        assert_eq!(person.data_type, "tuple<text, int, boolean>");
    }

    /// Issue #1690 (P0 safety): the nom `cql_type` parser must NOT stack-overflow
    /// on pathologically nested type strings (which under `panic = "abort"` abort
    /// the whole process). Recursion is bounded at [`MAX_TYPE_NESTING_DEPTH`], so
    /// deep input returns `Err` long before the stack is exhausted.
    #[test]
    fn test_cql_type_adversarial_deep_nesting_returns_err_not_abort() {
        let s = "frozen<".repeat(50_000) + "int" + &">".repeat(50_000);
        assert!(
            cql_type(&s).is_err(),
            "pathological nesting must return Err, not abort"
        );
    }

    /// The depth bound is exact and behavior-preserving: a leaf reached at depth
    /// == [`MAX_TYPE_NESTING_DEPTH`] (i.e. that many `frozen<...>` levels) is the
    /// last allowed depth and reconstructs to the identical type string it did
    /// before the guard existed; one level deeper returns `Err`.
    #[test]
    fn test_cql_type_nesting_depth_boundary_is_exact() {
        let depth = MAX_TYPE_NESTING_DEPTH; // 32 — the last allowed nesting level.

        // At the bound: must parse and round-trip to the exact same string.
        let ok_str = "frozen<".repeat(depth) + "int" + &">".repeat(depth);
        let (rest, parsed) =
            cql_type(&ok_str).expect("nesting at the depth bound must still parse");
        assert!(rest.trim().is_empty(), "entire input must be consumed");
        assert_eq!(parsed, ok_str, "type string must be preserved unchanged");

        // One past the bound: must error (nom failure surfaced as Err), not abort.
        let bad_str = "frozen<".repeat(depth + 1) + "int" + &">".repeat(depth + 1);
        assert!(
            cql_type(&bad_str).is_err(),
            "one level past the bound must return Err"
        );
    }

    /// Issue #1690 (P0 safety): the public `cql_type_to_type_id` conversion path
    /// (reached via `SchemaManager::cql_type_to_internal`) unwraps `frozen<...>`
    /// recursively. Pathologically nested `frozen<` input must return `Err`, not
    /// stack-overflow / abort. A leaf at the depth bound still resolves; one level
    /// deeper errors.
    #[test]
    fn test_cql_type_to_type_id_deep_frozen_returns_err_not_abort() {
        let s = "frozen<".repeat(50_000) + "int" + &">".repeat(50_000);
        assert!(
            cql_type_to_type_id(&s).is_err(),
            "pathological frozen nesting must return Err, not abort"
        );

        let depth = MAX_TYPE_NESTING_DEPTH;
        // frozen<...> unwrapping: `depth` frozen levels reach the leaf at `depth`,
        // which is the last allowed level and must still resolve to the leaf id.
        let ok_str = "frozen<".repeat(depth) + "int" + &">".repeat(depth);
        assert_eq!(
            cql_type_to_type_id(&ok_str).expect("frozen nesting at the bound must resolve"),
            CqlTypeId::Int,
            "the leaf type id must be unchanged"
        );

        let bad_str = "frozen<".repeat(depth + 1) + "int" + &">".repeat(depth + 1);
        let err = cql_type_to_type_id(&bad_str).expect_err("one level past the bound must error");
        let msg = err.to_string();
        assert!(
            msg.contains("nesting") || msg.contains("deep"),
            "error message must mention nesting/depth, got: {msg}"
        );
    }

    #[test]
    fn test_case_insensitive_keywords() {
        let cql = r#"
            create table Users (
                ID UUID primary key,
                Name TEXT,
                Email VARCHAR
            )
        "#;

        let schema = parse_cql_schema(cql).unwrap();
        assert_eq!(schema.table, "Users");
        assert_eq!(schema.columns.len(), 3);
    }

    #[test]
    fn test_quoted_identifiers() {
        let cql = r#"
            CREATE TABLE "CaseSensitive" (
                "Id" uuid PRIMARY KEY,
                "Name With Spaces" text
            )
        "#;

        let schema = parse_cql_schema(cql).unwrap();
        assert_eq!(schema.table, "CaseSensitive");

        let space_col = schema.columns.iter().find(|c| c.name == "Name With Spaces");
        assert!(space_col.is_some());
    }

    #[test]
    fn test_fallback_table_extraction() {
        // Test cases where full parsing might fail but we can still extract table name
        let cql = "CREATE TABLE myapp.orders (id bigint PRIMARY KEY)";
        let (keyspace, table) = extract_table_name(cql).unwrap();
        assert_eq!(keyspace, Some("myapp".to_string()));
        assert_eq!(table, "orders");
    }

    #[test]
    fn test_all_primitive_types() {
        let type_mappings = vec![
            ("ascii", CqlTypeId::Ascii),
            ("bigint", CqlTypeId::BigInt),
            ("blob", CqlTypeId::Blob),
            ("boolean", CqlTypeId::Boolean),
            ("counter", CqlTypeId::Counter),
            ("decimal", CqlTypeId::Decimal),
            ("double", CqlTypeId::Double),
            ("float", CqlTypeId::Float),
            ("int", CqlTypeId::Int),
            ("timestamp", CqlTypeId::Timestamp),
            ("uuid", CqlTypeId::Uuid),
            ("varchar", CqlTypeId::Varchar),
            ("text", CqlTypeId::Varchar),
            ("varint", CqlTypeId::Varint),
            ("timeuuid", CqlTypeId::Timeuuid),
            ("inet", CqlTypeId::Inet),
            ("date", CqlTypeId::Date),
            ("time", CqlTypeId::Time),
            ("smallint", CqlTypeId::Smallint),
            ("tinyint", CqlTypeId::Tinyint),
            ("duration", CqlTypeId::Duration),
        ];

        for (cql_type, expected_id) in type_mappings {
            assert_eq!(
                cql_type_to_type_id(cql_type).unwrap(),
                expected_id,
                "Failed for type: {}",
                cql_type
            );
        }
    }

    /// Issue #852 (review finding 2): the parser must preserve `WITH` table
    /// options into `TableSchema.comments` so the writer can honor
    /// `bloom_filter_fp_chance`. Previously the WITH clause was parsed and
    /// discarded, leaving `comments` empty.
    #[test]
    fn test_with_bloom_filter_fp_chance_preserved_in_comments() {
        let cql = "CREATE TABLE ks.t (id int PRIMARY KEY, name text) \
                   WITH bloom_filter_fp_chance = 1.0";
        let schema = parse_cql_schema(cql).expect("schema should parse");
        assert_eq!(
            schema
                .comments
                .get("bloom_filter_fp_chance")
                .map(String::as_str),
            Some("1.0"),
            "WITH bloom_filter_fp_chance must be preserved into comments, got: {:?}",
            schema.comments
        );
    }

    /// The option name is case-insensitive and additional options coexist.
    #[test]
    fn test_with_multiple_options_preserved() {
        let cql = "CREATE TABLE ks.t (id int PRIMARY KEY, name text) \
                   WITH gc_grace_seconds = 0 AND bloom_filter_fp_chance = 0.01";
        let schema = parse_cql_schema(cql).expect("schema should parse");
        assert_eq!(
            schema
                .comments
                .get("bloom_filter_fp_chance")
                .map(String::as_str),
            Some("0.01")
        );
        assert_eq!(
            schema.comments.get("gc_grace_seconds").map(String::as_str),
            Some("0")
        );
    }

    /// Issue #852 (review finding, roborev job 741): a map-valued option such as
    /// `compression = {'class': 'LZ4Compressor'}` appearing BEFORE the bloom
    /// option must not stop option collection. Previously the map value failed
    /// to parse, so `bloom_filter_fp_chance` was dropped and the writer fell
    /// back to 0.01 (emitting Filter.db incorrectly).
    #[test]
    fn test_with_map_valued_option_before_bloom_preserved() {
        let cql = "CREATE TABLE ks.t (id int PRIMARY KEY, name text) \
                   WITH compression = {'class': 'LZ4Compressor'} \
                   AND bloom_filter_fp_chance = 1.0";
        let schema = parse_cql_schema(cql).expect("schema should parse");
        assert_eq!(
            schema
                .comments
                .get("bloom_filter_fp_chance")
                .map(String::as_str),
            Some("1.0"),
            "bloom_filter_fp_chance must survive a preceding map-valued option, got: {:?}",
            schema.comments
        );
    }

    /// A multi-entry compaction map (with nested-looking comma-separated
    /// entries) before the bloom option must also be skipped cleanly.
    #[test]
    fn test_with_compaction_map_before_bloom_preserved() {
        let cql = "CREATE TABLE ks.t (id int PRIMARY KEY, name text) \
                   WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'max_threshold': '32'} \
                   AND bloom_filter_fp_chance = 1.0";
        let schema = parse_cql_schema(cql).expect("schema should parse");
        assert_eq!(
            schema
                .comments
                .get("bloom_filter_fp_chance")
                .map(String::as_str),
            Some("1.0"),
            "bloom_filter_fp_chance must survive a preceding compaction map, got: {:?}",
            schema.comments
        );
    }

    /// List-valued options must also be tolerated without stopping collection.
    #[test]
    fn test_with_list_valued_option_before_bloom_preserved() {
        let cql = "CREATE TABLE ks.t (id int PRIMARY KEY, name text) \
                   WITH some_list = ['a', 'b'] \
                   AND bloom_filter_fp_chance = 1.0";
        let schema = parse_cql_schema(cql).expect("schema should parse");
        assert_eq!(
            schema
                .comments
                .get("bloom_filter_fp_chance")
                .map(String::as_str),
            Some("1.0"),
            "bloom_filter_fp_chance must survive a preceding list-valued option, got: {:?}",
            schema.comments
        );
    }

    /// A map-valued option's value should be captured (round-tripped) too.
    #[test]
    fn test_with_map_valued_option_value_captured() {
        let cql = "CREATE TABLE ks.t (id int PRIMARY KEY, name text) \
                   WITH compression = {'class': 'LZ4Compressor'}";
        let schema = parse_cql_schema(cql).expect("schema should parse");
        assert_eq!(
            schema.comments.get("compression").map(String::as_str),
            Some("{'class': 'LZ4Compressor'}"),
            "map-valued option value should be preserved verbatim, got: {:?}",
            schema.comments
        );
    }

    /// Issue #852 (branch review, roborev job 775): a `CLUSTERING ORDER BY (...)`
    /// WITH item appearing BEFORE the bloom option must not stop option
    /// collection. Previously `option_pair` could not parse the non-`key=value`
    /// CLUSTERING item, so `separated_list0` returned early and the trailing
    /// `AND bloom_filter_fp_chance = 1.0` was silently dropped (the writer then
    /// fell back to 0.01 and emitted Filter.db despite the CQL disabling it).
    #[test]
    fn test_with_clustering_order_before_bloom_preserved() {
        let cql = "CREATE TABLE ks.t (id int, ck int, name text, PRIMARY KEY (id, ck)) \
                   WITH CLUSTERING ORDER BY (ck DESC) AND bloom_filter_fp_chance = 1.0";
        let schema = parse_cql_schema(cql).expect("schema should parse");
        assert_eq!(
            schema
                .comments
                .get("bloom_filter_fp_chance")
                .map(String::as_str),
            Some("1.0"),
            "bloom_filter_fp_chance must survive a preceding CLUSTERING ORDER BY, got: {:?}",
            schema.comments
        );
    }

    /// A `CLUSTERING ORDER BY (...)` with multiple columns and mixed
    /// ASC/DESC ordering must also be skipped cleanly.
    #[test]
    fn test_with_clustering_order_multi_column_before_bloom_preserved() {
        let cql =
            "CREATE TABLE ks.t (id int, c1 int, c2 int, name text, PRIMARY KEY (id, c1, c2)) \
                   WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC) AND bloom_filter_fp_chance = 1.0";
        let schema = parse_cql_schema(cql).expect("schema should parse");
        assert_eq!(
            schema
                .comments
                .get("bloom_filter_fp_chance")
                .map(String::as_str),
            Some("1.0"),
            "bloom_filter_fp_chance must survive a multi-column CLUSTERING ORDER BY, got: {:?}",
            schema.comments
        );
    }

    /// #849/#852 (branch review, roborev job 777): a single DESC clustering
    /// column must be applied to `clustering_keys` so DESC write/merge ordering
    /// is honored for CQL-parsed schemas.
    #[test]
    fn test_clustering_order_desc_applied_to_clustering_keys() {
        let cql = "CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck)) \
                   WITH CLUSTERING ORDER BY (ck DESC)";
        let schema = parse_cql_schema(cql).expect("schema should parse");
        let ck = schema
            .clustering_keys
            .iter()
            .find(|c| c.name == "ck")
            .expect("ck clustering column must exist");
        assert_eq!(
            ck.order,
            ClusteringOrder::Desc,
            "ck must carry DESC ordering, got: {:?}",
            schema.clustering_keys
        );
    }

    /// #849/#852: a mixed multi-column clustering order must apply each column's
    /// direction independently (one ASC, one DESC).
    #[test]
    fn test_clustering_order_mixed_applied_per_column() {
        let cql = "CREATE TABLE ks.t (pk int, c1 int, c2 int, v int, PRIMARY KEY (pk, c1, c2)) \
                   WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC)";
        let schema = parse_cql_schema(cql).expect("schema should parse");
        let c1 = schema
            .clustering_keys
            .iter()
            .find(|c| c.name == "c1")
            .expect("c1 must exist");
        let c2 = schema
            .clustering_keys
            .iter()
            .find(|c| c.name == "c2")
            .expect("c2 must exist");
        assert_eq!(c1.order, ClusteringOrder::Asc, "c1 should be ASC");
        assert_eq!(c2.order, ClusteringOrder::Desc, "c2 should be DESC");
    }

    /// #849/#852: with no CLUSTERING ORDER BY clause, clustering columns default
    /// to ASC.
    #[test]
    fn test_clustering_order_defaults_to_asc_without_clause() {
        let cql = "CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck))";
        let schema = parse_cql_schema(cql).expect("schema should parse");
        let ck = schema
            .clustering_keys
            .iter()
            .find(|c| c.name == "ck")
            .expect("ck must exist");
        assert_eq!(
            ck.order,
            ClusteringOrder::Asc,
            "ck must default to ASC without a CLUSTERING ORDER BY clause"
        );
    }

    /// #852 (branch review, roborev job 788): a QUOTED clustering identifier in
    /// `CLUSTERING ORDER BY (...)` must match the clustering column name (which is
    /// stored unquoted by `identifier()`), so its DESC direction is applied rather
    /// than silently defaulting to ASC.
    #[test]
    fn test_clustering_order_quoted_identifier_applied() {
        let cql = "CREATE TABLE ks.t (pk int, \"Ck\" int, v int, PRIMARY KEY (pk, \"Ck\")) \
                   WITH CLUSTERING ORDER BY (\"Ck\" DESC)";
        let schema = parse_cql_schema(cql).expect("schema should parse");
        let ck = schema
            .clustering_keys
            .iter()
            .find(|c| c.name == "Ck")
            .expect("Ck clustering column must exist");
        assert_eq!(
            ck.order,
            ClusteringOrder::Desc,
            "quoted \"Ck\" must carry DESC ordering, got: {:?}",
            schema.clustering_keys
        );
    }

    /// #852 (job 788): a mixed clustering order mixing a quoted DESC column with
    /// an unquoted ASC column must apply each direction to the correct column.
    #[test]
    fn test_clustering_order_mixed_quoted_and_unquoted() {
        let cql =
            "CREATE TABLE ks.t (pk int, c1 int, \"Ck\" int, v int, PRIMARY KEY (pk, c1, \"Ck\")) \
                   WITH CLUSTERING ORDER BY (c1 ASC, \"Ck\" DESC)";
        let schema = parse_cql_schema(cql).expect("schema should parse");
        let c1 = schema
            .clustering_keys
            .iter()
            .find(|c| c.name == "c1")
            .expect("c1 must exist");
        let ck = schema
            .clustering_keys
            .iter()
            .find(|c| c.name == "Ck")
            .expect("Ck must exist");
        assert_eq!(c1.order, ClusteringOrder::Asc, "c1 should be ASC");
        assert_eq!(ck.order, ClusteringOrder::Desc, "Ck should be DESC");
    }

    /// #852 (branch review, roborev job 797): a QUOTED clustering identifier that
    /// itself CONTAINS a comma (e.g. `"C,k"`) must not be split on the embedded
    /// comma. `parse_clustering_order_body` previously used `inner.split(',')`,
    /// which broke `"C,k" DESC` into two bogus entries and silently left the
    /// column at its default ASC. Parsing the list with nom + the shared
    /// `identifier` parser keeps the quoted name intact and applies its DESC.
    #[test]
    fn test_clustering_order_quoted_identifier_with_comma_applied() {
        let cql = "CREATE TABLE ks.t (pk int, \"C,k\" int, v int, PRIMARY KEY (pk, \"C,k\")) \
                   WITH CLUSTERING ORDER BY (\"C,k\" DESC)";
        let schema = parse_cql_schema(cql).expect("schema should parse");
        let ck = schema
            .clustering_keys
            .iter()
            .find(|c| c.name == "C,k")
            .expect("\"C,k\" clustering column must exist");
        assert_eq!(
            ck.order,
            ClusteringOrder::Desc,
            "quoted \"C,k\" (with embedded comma) must carry DESC ordering, got: {:?}",
            schema.clustering_keys
        );
    }

    /// #852 (branch review, roborev job 816): a QUOTED clustering identifier that
    /// itself CONTAINS a close-paren (e.g. `"C)k"`) must not terminate the
    /// `CLUSTERING ORDER BY (...)` clause body early. The clause-body scan in
    /// `clustering_order_item` previously stopped at the first `)`, truncating the
    /// body to `("C` and silently dropping the DESC direction (the column fell
    /// back to its default ASC). A quote-aware scan keeps the quoted name intact
    /// so its DESC is applied.
    #[test]
    fn test_clustering_order_quoted_identifier_with_close_paren_applied() {
        let cql = "CREATE TABLE ks.t (pk int, \"C)k\" int, v int, PRIMARY KEY (pk, \"C)k\")) \
                   WITH CLUSTERING ORDER BY (\"C)k\" DESC)";
        let schema = parse_cql_schema(cql).expect("schema should parse");
        let ck = schema
            .clustering_keys
            .iter()
            .find(|c| c.name == "C)k")
            .expect("\"C)k\" clustering column must exist");
        assert_eq!(
            ck.order,
            ClusteringOrder::Desc,
            "quoted \"C)k\" (with embedded close-paren) must carry DESC ordering, got: {:?}",
            schema.clustering_keys
        );
    }

    /// Issue #852 (branch review, roborev job 775): a string option value with a
    /// doubled single-quote escape (`''`) must be parsed in full so a later
    /// `AND`-separated option survives. Previously the string parser stopped at
    /// the first inner `'`, leaving `s table' AND ...` unconsumed and dropping
    /// the bloom option.
    ///
    /// The captured comment value uses the CQL convention of un-escaping the
    /// doubled quote, i.e. `Bob's table`.
    #[test]
    fn test_with_quote_escaped_comment_before_bloom_preserved() {
        let cql = "CREATE TABLE ks.t (id int PRIMARY KEY, name text) \
                   WITH comment = 'Bob''s table' AND bloom_filter_fp_chance = 1.0";
        let schema = parse_cql_schema(cql).expect("schema should parse");
        assert_eq!(
            schema
                .comments
                .get("bloom_filter_fp_chance")
                .map(String::as_str),
            Some("1.0"),
            "bloom_filter_fp_chance must survive a quote-escaped comment, got: {:?}",
            schema.comments
        );
        assert_eq!(
            schema.comments.get("comment").map(String::as_str),
            Some("Bob's table"),
            "doubled single-quote must be un-escaped in the captured comment, got: {:?}",
            schema.comments
        );
    }
}
