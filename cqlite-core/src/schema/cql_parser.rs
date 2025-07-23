//! CQL Schema Parser
//!
//! This module provides parsing capabilities for CQL CREATE TABLE statements
//! to extract table schema information including table names, column definitions,
//! partition keys, clustering keys, and type information.

use crate::error::{Error, Result};
use crate::parser::types::CqlTypeId;
use crate::schema::{CqlType, TableSchema, KeyColumn, ClusteringColumn, Column};
use serde_json;
use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while, take_while1},
    character::complete::{char, multispace0, multispace1},
    combinator::{map, opt, recognize},
    multi::{many0, separated_list0, separated_list1},
    sequence::{delimited, preceded, separated_pair, terminated, tuple},
    IResult,
};
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
        delimited(
            char('"'),
            take_while1(|c: char| c != '"'),
            char('"'),
        ),
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

/// Parse CQL data type
fn cql_type(input: &str) -> IResult<&str, String> {
    // Handle complex types like list<text>, map<text, bigint>, frozen<set<uuid>>
    fn parse_type_inner(input: &str) -> IResult<&str, String> {
        let (input, base) = alt((
            // Collection types
            map(
                tuple((
                    alt((keyword("list"), keyword("set"))),
                    char('<'),
                    parse_type_inner,
                    char('>'),
                )),
                |(collection, _, inner, _)| format!("{}<{}>", collection, inner),
            ),
            // Map type
            map(
                tuple((
                    keyword("map"),
                    char('<'),
                    parse_type_inner,
                    char(','),
                    ws,
                    parse_type_inner,
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
                    separated_list1(
                        tuple((ws, char(','), ws)),
                        parse_type_inner,
                    ),
                    char('>'),
                )),
                |(_, _, types, _)| format!("tuple<{}>", types.join(", ")),
            ),
            // Frozen type
            map(
                tuple((
                    keyword("frozen"),
                    char('<'),
                    parse_type_inner,
                    char('>'),
                )),
                |(_, _, inner, _)| format!("frozen<{}>", inner),
            ),
            // Simple types and UDTs
            map(identifier, |name| name),
        ))(input)?;
        
        Ok((input, base))
    }
    
    let (input, _) = ws(input)?;
    let (input, type_name) = parse_type_inner(input)?;
    let (input, _) = ws(input)?;
    
    Ok((input, type_name))
}

/// Parse column definition (with optional inline PRIMARY KEY)
fn column_definition(input: &str) -> IResult<&str, (String, String)> {
    let (input, _) = ws(input)?;
    let (input, name) = identifier(input)?;
    let (input, _) = ws1(input)?;
    let (input, data_type) = cql_type(input)?;
    let (input, _) = ws(input)?;
    
    // Check for inline PRIMARY KEY
    let (input, is_primary) = opt(tuple((
        keyword("primary"),
        ws1,
        keyword("key"),
    )))(input)?;
    
    let final_type = if is_primary.is_some() {
        format!("{} PRIMARY KEY", data_type)
    } else {
        data_type
    };
    
    Ok((input, (name, final_type)))
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
                separated_list1(
                    tuple((ws, char(','), ws)),
                    identifier,
                ),
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
        separated_list1(
            tuple((ws, char(','), ws)),
            identifier,
        ),
    ))(input)?;
    
    let (input, _) = ws(input)?;
    let (input, _) = char(')')(input)?;
    
    Ok((input, (partition_keys, clustering_keys.unwrap_or_default())))
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
                // String value
                delimited(char('\''), take_while(|c: char| c != '\''), char('\'')),
                // Numeric or identifier value
                take_while1(|c: char| c.is_alphanumeric() || c == '_' || c == '.'),
            )),
        ),
        |(key, value)| (key, value.to_string()),
    );
    
    let (input, options) = separated_list0(
        tuple((ws, keyword("and"), ws)),
        option_pair,
    )(input)?;
    
    Ok((input, options.into_iter().collect()))
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
    let mut columns = Vec::new();
    let mut partition_keys = Vec::new();
    let mut clustering_keys = Vec::new();
    let mut primary_key_found = false;
    
    let (input, items) = separated_list1(
        tuple((ws, char(','), ws)),
        alt((
            // Primary key constraint
            map(primary_key_spec, |keys| ("PRIMARY_KEY".to_string(), serde_json::to_string(&keys).unwrap_or_default())),
            // Column definition
            column_definition,
        )),
    )(input)?;
    
    // Process parsed items
    for (name, value) in items {
        if name == "PRIMARY_KEY" {
            // Parse the JSON-encoded key specification
            if let Ok(keys_tuple) = serde_json::from_str::<(Vec<String>, Vec<String>)>(&value) {
                partition_keys = keys_tuple.0;
                clustering_keys = keys_tuple.1;
                primary_key_found = true;
            }
            continue;
        }
        columns.push((name, value));
    }
    
    let (input, _) = ws(input)?;
    let (input, _) = char(')')(input)?;
    
    // Parse optional WITH clause
    let (input, _options) = opt(table_options)(input)?;
    
    // If no primary key was found in constraints, look for inline PRIMARY KEY or use first column
    if !primary_key_found && !columns.is_empty() {
        // Check if any column has "PRIMARY KEY" in its type (inline definition)
        let mut found_inline = false;
        for (col_name, col_type) in &columns {
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
        partition_keys: partition_keys.into_iter().enumerate().map(|(pos, name)| {
            let data_type = columns.iter()
                .find(|(col_name, _)| col_name == &name)
                .map(|(_, dt)| dt.clone())
                .unwrap_or_else(|| "text".to_string());
            
            KeyColumn {
                name,
                data_type,
                position: pos,
            }
        }).collect(),
        clustering_keys: clustering_keys.into_iter().enumerate().map(|(pos, name)| {
            let data_type = columns.iter()
                .find(|(col_name, _)| col_name == &name)
                .map(|(_, dt)| dt.clone())
                .unwrap_or_else(|| "text".to_string());
            
            ClusteringColumn {
                name,
                data_type,
                position: pos,
                order: "ASC".to_string(),
            }
        }).collect(),
        columns: columns.into_iter().map(|(name, data_type_with_constraints)| {
            // Remove PRIMARY KEY constraint from data type
            let data_type = if data_type_with_constraints.to_lowercase().contains("primary key") {
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
            }
        }).collect(),
        comments: HashMap::new(),
    };
    
    Ok((input, schema))
}

/// Convert CQL type string to internal CqlTypeId
pub fn cql_type_to_type_id(cql_type: &str) -> Result<CqlTypeId> {
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
                return cql_type_to_type_id(inner_type);
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
                } else {
                    return extract_simple_table_name(after_table);
                }
            }
            
            Err(Error::schema("Failed to extract table name from CQL".to_string()))
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
        Err(nom::Err::Error(e) | nom::Err::Failure(e)) => {
            Err(Error::schema(format!("Failed to parse CQL schema: {:?}", e)))
        }
        Err(nom::Err::Incomplete(_)) => {
            Err(Error::schema("Incomplete CQL schema".to_string()))
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
        
        let metadata_col = schema.columns.iter().find(|c| c.name == "metadata").unwrap();
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
        assert_eq!(cql_type_to_type_id("frozen<set<uuid>>").unwrap(), CqlTypeId::Set);
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
        
        let frozen_set = schema.columns.iter().find(|c| c.name == "frozen_set").unwrap();
        assert_eq!(frozen_set.data_type, "frozen<set<text>>");
        
        let frozen_map = schema.columns.iter().find(|c| c.name == "frozen_map").unwrap();
        assert_eq!(frozen_map.data_type, "frozen<map<text, bigint>>");
        
        let nested = schema.columns.iter().find(|c| c.name == "nested_frozen").unwrap();
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
        
        let prefs_col = schema.columns.iter().find(|c| c.name == "preferences").unwrap();
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
        
        let coords = schema.columns.iter().find(|c| c.name == "coordinates").unwrap();
        assert_eq!(coords.data_type, "tuple<double, double>");
        
        let person = schema.columns.iter().find(|c| c.name == "person_info").unwrap();
        assert_eq!(person.data_type, "tuple<text, int, boolean>");
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
}