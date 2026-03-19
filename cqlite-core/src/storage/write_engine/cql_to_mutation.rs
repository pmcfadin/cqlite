//! CQL AST to Mutation conversion.
//!
//! Converts parsed CQL literal values into `Value` types that the WriteEngine
//! can persist. The primary entry point is `literal_to_value`, which performs
//! schema-aware coercion from the CQL parser's AST types to the internal
//! `Value` enum.
//!
//! The `insert_to_mutation` function converts a parsed `CqlInsert` AST node into
//! a `Mutation` using schema information to identify partition/clustering keys and
//! regular columns.

#[cfg(feature = "write-support")]
use crate::cql::ast::{
    CqlCollectionLiteral, CqlExpression, CqlInsert, CqlInsertValues, CqlLiteral, CqlTable,
    CqlUnaryOperator, CqlUsing, CqlUdtLiteral,
};
#[cfg(feature = "write-support")]
use crate::schema::{CqlType, TableSchema};
#[cfg(feature = "write-support")]
use crate::storage::write_engine::mutation::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, TableId,
};
#[cfg(feature = "write-support")]
use crate::types::{UdtField, UdtValue, Value};
#[cfg(feature = "write-support")]
use crate::Error;

/// Convert a parsed `CqlInsert` AST node into a `Mutation` using schema information.
///
/// # Errors
///
/// Returns `Error::InvalidInput` when:
/// - The INSERT targets a different table than the schema
/// - The number of columns and values do not match
/// - A required partition key column is missing from the INSERT
/// - JSON INSERT syntax is used (not yet supported)
/// - A value cannot be coerced to its schema type
#[cfg(feature = "write-support")]
#[allow(dead_code)] // Will be wired into WriteEngine in a follow-up task
pub(crate) fn insert_to_mutation(insert: &CqlInsert, schema: &TableSchema) -> Result<Mutation, Error> {
    validate_table(&insert.table, schema)?;

    // Extract (column_name, expression) pairs
    let values = match &insert.values {
        CqlInsertValues::Values(exprs) => exprs,
        CqlInsertValues::Json(_) => {
            return Err(Error::InvalidInput(
                "JSON INSERT syntax is not yet supported in mutations".to_string(),
            ));
        }
    };

    if insert.columns.len() != values.len() {
        return Err(Error::InvalidInput(format!(
            "INSERT has {} columns but {} values",
            insert.columns.len(),
            values.len()
        )));
    }

    // Build a map of column_name → value for easy lookup
    let col_val_pairs: Vec<(String, &CqlExpression)> = insert
        .columns
        .iter()
        .zip(values.iter())
        .map(|(col_id, expr)| (col_id.name.to_lowercase(), expr))
        .collect();

    // Ordered partition key columns from schema
    let ordered_pk = schema.ordered_partition_keys();
    // Ordered clustering key columns from schema
    let ordered_ck = schema.ordered_clustering_keys();

    // Resolve and collect partition key values (in schema order)
    let mut pk_columns: Vec<(String, Value)> = Vec::with_capacity(ordered_pk.len());
    for pk_col in &ordered_pk {
        let col_name_lc = pk_col.name.to_lowercase();
        let (_, expr) = col_val_pairs
            .iter()
            .find(|(name, _)| *name == col_name_lc)
            .ok_or_else(|| {
                Error::InvalidInput(format!(
                    "Partition key column '{}' is missing from INSERT",
                    pk_col.name
                ))
            })?;
        let cql_type = CqlType::parse(&pk_col.data_type)?;
        let value = expression_to_value(expr, &cql_type)?;
        pk_columns.push((pk_col.name.clone(), value));
    }
    let partition_key = PartitionKey::new(pk_columns);

    // Resolve and collect clustering key values (in schema order), if any
    let clustering_key = if ordered_ck.is_empty() {
        None
    } else {
        let mut ck_columns: Vec<(String, Value)> = Vec::with_capacity(ordered_ck.len());
        for ck_col in &ordered_ck {
            let col_name_lc = ck_col.name.to_lowercase();
            // Clustering key columns are optional in INSERT (may be absent)
            if let Some((_, expr)) = col_val_pairs.iter().find(|(name, _)| *name == col_name_lc) {
                let cql_type = CqlType::parse(&ck_col.data_type)?;
                let value = expression_to_value(expr, &cql_type)?;
                ck_columns.push((ck_col.name.clone(), value));
            }
        }
        if ck_columns.is_empty() {
            None
        } else {
            Some(ClusteringKey::new(ck_columns))
        }
    };

    // Collect regular column operations (non-PK, non-CK columns)
    let mut operations: Vec<CellOperation> = Vec::new();
    for (col_name, expr) in &col_val_pairs {
        if schema.is_partition_key(col_name) || schema.is_clustering_key(col_name) {
            continue;
        }
        // Look up column in schema to get type
        let column = schema
            .get_column(col_name)
            .ok_or_else(|| Error::InvalidInput(format!("Unknown column '{}'", col_name)))?;
        let cql_type = CqlType::parse(&column.data_type)?;
        let value = expression_to_value(expr, &cql_type)?;
        operations.push(CellOperation::Write {
            column: column.name.clone(),
            value,
        });
    }

    let timestamp_micros = extract_timestamp(&insert.using)?;
    let ttl_seconds = extract_ttl(&insert.using)?;

    let table_id = TableId::new(schema.keyspace.clone(), schema.table.clone());
    Ok(Mutation::new(
        table_id,
        partition_key,
        clustering_key,
        operations,
        timestamp_micros,
        ttl_seconds,
    ))
}

/// Convert a `CqlExpression` to a `Value` for mutation purposes.
///
/// Only literal expressions and unary minus on literals are supported.
///
/// # Errors
///
/// Returns `Error::InvalidInput` for non-literal expressions, and propagates
/// type-coercion errors from `literal_to_value`.
#[cfg(feature = "write-support")]
fn expression_to_value(expr: &CqlExpression, target_type: &CqlType) -> Result<Value, Error> {
    match expr {
        CqlExpression::Literal(lit) => literal_to_value(lit, target_type),
        CqlExpression::Unary {
            operator: CqlUnaryOperator::Minus,
            operand,
        } => {
            // Handle negative numeric literals: -(integer) or -(float)
            match operand.as_ref() {
                CqlExpression::Literal(CqlLiteral::Integer(i)) => {
                    let negated = i.checked_neg().ok_or_else(|| {
                        Error::InvalidInput(format!(
                            "Integer {} cannot be negated (overflow)",
                            i
                        ))
                    })?;
                    literal_to_value(&CqlLiteral::Integer(negated), target_type)
                }
                CqlExpression::Literal(CqlLiteral::Float(f)) => {
                    literal_to_value(&CqlLiteral::Float(-f), target_type)
                }
                _ => Err(Error::InvalidInput(
                    "Unary minus is only supported on integer or float literals".to_string(),
                )),
            }
        }
        _ => Err(Error::InvalidInput(
            "Only literal values are supported in mutations".to_string(),
        )),
    }
}

/// Validate that the INSERT's table reference matches the provided schema.
///
/// # Errors
///
/// Returns `Error::InvalidInput` if the table name or keyspace does not match.
#[cfg(feature = "write-support")]
fn validate_table(table: &CqlTable, schema: &TableSchema) -> Result<(), Error> {
    if let Some(ks) = &table.keyspace {
        if !ks.name.eq_ignore_ascii_case(&schema.keyspace) {
            return Err(Error::InvalidInput(format!(
                "INSERT targets keyspace '{}' but schema is for '{}'",
                ks.name, schema.keyspace
            )));
        }
    }
    if !table.name.name.eq_ignore_ascii_case(&schema.table) {
        return Err(Error::InvalidInput(format!(
            "INSERT targets table '{}' but schema is for '{}'",
            table.name.name, schema.table
        )));
    }
    Ok(())
}

/// Extract the timestamp from a USING clause.
///
/// If no USING TIMESTAMP is present, returns the current time in microseconds
/// since the Unix epoch.
///
/// # Errors
///
/// Returns `Error::InvalidInput` if the timestamp expression is not an integer literal.
#[cfg(feature = "write-support")]
fn extract_timestamp(using: &Option<CqlUsing>) -> Result<i64, Error> {
    if let Some(u) = using {
        if let Some(ts_expr) = &u.timestamp {
            match ts_expr {
                CqlExpression::Literal(CqlLiteral::Integer(ts)) => return Ok(*ts),
                _ => {
                    return Err(Error::InvalidInput(
                        "USING TIMESTAMP requires an integer literal".to_string(),
                    ))
                }
            }
        }
    }
    // Default: current time in microseconds
    let micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| Error::InvalidInput(format!("System clock error: {}", e)))?
        .as_micros();
    // Cast to i64; saturate if value exceeds i64::MAX (will not happen before year 292k)
    Ok(micros.min(i64::MAX as u128) as i64)
}

/// Extract the TTL from a USING clause.
///
/// Returns `None` if no USING TTL is present.
///
/// # Errors
///
/// Returns `Error::InvalidInput` if the TTL expression is not an integer literal or
/// the value overflows `u32`.
#[cfg(feature = "write-support")]
fn extract_ttl(using: &Option<CqlUsing>) -> Result<Option<u32>, Error> {
    if let Some(u) = using {
        if let Some(ttl_expr) = &u.ttl {
            match ttl_expr {
                CqlExpression::Literal(CqlLiteral::Integer(ttl)) => {
                    let v = u32::try_from(*ttl).map_err(|_| {
                        Error::InvalidInput(format!(
                            "TTL value {} is out of range for u32",
                            ttl
                        ))
                    })?;
                    return Ok(Some(v));
                }
                _ => {
                    return Err(Error::InvalidInput(
                        "USING TTL requires an integer literal".to_string(),
                    ))
                }
            }
        }
    }
    Ok(None)
}

/// Convert a CQL AST literal value to an internal `Value`, guided by the
/// target schema type.
///
/// # Errors
///
/// Returns `Error::InvalidInput` for type mismatches or overflow, and
/// `Error::Parse` for malformed UUID/blob/inet strings.
#[cfg(feature = "write-support")]
pub fn literal_to_value(literal: &CqlLiteral, target_type: &CqlType) -> Result<Value, Error> {
    // Unwrap Frozen – it doesn't affect value representation
    if let CqlType::Frozen(inner) = target_type {
        let inner_value = literal_to_value(literal, inner)?;
        return Ok(Value::Frozen(Box::new(inner_value)));
    }

    match literal {
        CqlLiteral::Null => Ok(Value::Null),

        CqlLiteral::Boolean(b) => match target_type {
            CqlType::Boolean => Ok(Value::Boolean(*b)),
            _ => Err(type_mismatch("boolean", target_type)),
        },

        CqlLiteral::Integer(i) => integer_to_value(*i, target_type),

        CqlLiteral::Float(f) => match target_type {
            CqlType::Double => Ok(Value::Float(*f)),
            CqlType::Float => Ok(Value::Float32(*f as f32)),
            CqlType::Decimal => Err(Error::InvalidInput(
                "Float-to-Decimal conversion not supported; use a string literal".to_string(),
            )),
            _ => Err(type_mismatch("float", target_type)),
        },

        CqlLiteral::String(s) => match target_type {
            CqlType::Text | CqlType::Varchar | CqlType::Ascii => Ok(Value::Text(s.clone())),
            CqlType::Inet => parse_inet(s),
            _ => Err(type_mismatch("string", target_type)),
        },

        CqlLiteral::Uuid(s) => match target_type {
            CqlType::Uuid | CqlType::TimeUuid => parse_uuid(s),
            _ => Err(type_mismatch("uuid", target_type)),
        },

        CqlLiteral::Blob(s) => match target_type {
            CqlType::Blob => parse_blob(s),
            _ => Err(type_mismatch("blob", target_type)),
        },

        CqlLiteral::Collection(coll) => collection_to_value(coll, target_type),

        CqlLiteral::Tuple(elements) => tuple_to_value(elements, target_type),

        CqlLiteral::Udt(udt) => udt_to_value(udt, target_type),
    }
}

/// Coerce an integer literal to the requested numeric type.
#[cfg(feature = "write-support")]
fn integer_to_value(i: i64, target: &CqlType) -> Result<Value, Error> {
    match target {
        CqlType::TinyInt => {
            let v = i8::try_from(i).map_err(|_| overflow_error(i, "tinyint"))?;
            Ok(Value::TinyInt(v))
        }
        CqlType::SmallInt => {
            let v = i16::try_from(i).map_err(|_| overflow_error(i, "smallint"))?;
            Ok(Value::SmallInt(v))
        }
        CqlType::Int => {
            let v = i32::try_from(i).map_err(|_| overflow_error(i, "int"))?;
            Ok(Value::Integer(v))
        }
        CqlType::BigInt => Ok(Value::BigInt(i)),
        CqlType::Counter => Ok(Value::Counter(i)),
        CqlType::Duration => Err(Error::InvalidInput(
            "Duration type requires a duration literal (e.g. '1h30m'), not an integer".to_string(),
        )),
        CqlType::Decimal => {
            let unscaled = varint_to_bytes(i);
            Ok(Value::Decimal { scale: 0, unscaled })
        }
        CqlType::Timestamp => Ok(Value::Timestamp(i)),
        CqlType::Date => {
            let v = i32::try_from(i).map_err(|_| overflow_error(i, "date"))?;
            Ok(Value::Date(v))
        }
        CqlType::Time => Ok(Value::Time(i)),
        CqlType::Float => Ok(Value::Float32(i as f32)),
        CqlType::Double => Ok(Value::Float(i as f64)),
        CqlType::Varint => {
            // Store as big-endian two's complement bytes (minimal encoding)
            let bytes = varint_to_bytes(i);
            Ok(Value::Varint(bytes))
        }
        _ => Err(type_mismatch("integer", target)),
    }
}

/// Encode a signed 64-bit integer as a minimal big-endian two's-complement
/// byte sequence (Cassandra varint format).
#[cfg(feature = "write-support")]
fn varint_to_bytes(i: i64) -> Vec<u8> {
    if i == 0 {
        return vec![0];
    }
    let be = i.to_be_bytes();
    // Find the shortest representation: strip leading bytes that are the sign
    // extension (0x00 for positive, 0xFF for negative) as long as the following
    // byte has the same sign bit.
    let sign_byte = if i < 0 { 0xFF_u8 } else { 0x00_u8 };
    let mut start = 0usize;
    while start < 7 {
        if be[start] == sign_byte {
            // Check that the next byte would not flip the sign bit
            let next_is_same_sign = if i < 0 {
                be[start + 1] & 0x80 != 0
            } else {
                be[start + 1] & 0x80 == 0
            };
            if next_is_same_sign {
                start += 1;
                continue;
            }
        }
        break;
    }
    be[start..].to_vec()
}

/// Parse a UUID string (with or without dashes) into a 16-byte array.
#[cfg(feature = "write-support")]
fn parse_uuid(s: &str) -> Result<Value, Error> {
    // Strip dashes
    let hex: String = s.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 {
        return Err(Error::Parse(format!(
            "invalid UUID string (expected 32 hex chars after stripping dashes): {:?}",
            s
        )));
    }
    let mut bytes = [0u8; 16];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let hi = hex_nibble(chunk[0])?;
        let lo = hex_nibble(chunk[1])?;
        bytes[i] = (hi << 4) | lo;
    }
    Ok(Value::Uuid(bytes))
}

/// Parse a blob hex string (with optional `0x` prefix) into bytes.
#[cfg(feature = "write-support")]
fn parse_blob(s: &str) -> Result<Value, Error> {
    let hex = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s);
    if hex.len() % 2 != 0 {
        return Err(Error::Parse(format!(
            "blob hex string has odd length: {:?}",
            s
        )));
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for chunk in hex.as_bytes().chunks(2) {
        let hi = hex_nibble(chunk[0])?;
        let lo = hex_nibble(chunk[1])?;
        bytes.push((hi << 4) | lo);
    }
    Ok(Value::Blob(bytes))
}

/// Parse an IP address string into 4 (IPv4) or 16 (IPv6) bytes.
#[cfg(feature = "write-support")]
fn parse_inet(s: &str) -> Result<Value, Error> {
    use std::net::IpAddr;
    let addr: IpAddr = s
        .parse()
        .map_err(|_| Error::Parse(format!("invalid inet address: {:?}", s)))?;
    let bytes = match addr {
        IpAddr::V4(a) => a.octets().to_vec(),
        IpAddr::V6(a) => a.octets().to_vec(),
    };
    Ok(Value::Inet(bytes))
}

/// Convert a single hex ASCII byte to its nibble value.
#[cfg(feature = "write-support")]
fn hex_nibble(b: u8) -> Result<u8, Error> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(Error::Parse(format!(
            "invalid hex character: {:?}",
            b as char
        ))),
    }
}

/// Convert a collection literal given the target CQL collection type.
///
/// Expects a non-Frozen target type (Frozen is unwrapped by caller).
#[cfg(feature = "write-support")]
fn collection_to_value(coll: &CqlCollectionLiteral, target: &CqlType) -> Result<Value, Error> {
    match (coll, target) {
        (CqlCollectionLiteral::List(items), CqlType::List(elem_type)) => {
            let values = items
                .iter()
                .map(|lit| literal_to_value(lit, elem_type))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Value::List(values))
        }
        (CqlCollectionLiteral::Set(items), CqlType::Set(elem_type)) => {
            let values = items
                .iter()
                .map(|lit| literal_to_value(lit, elem_type))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Value::Set(values))
        }
        (CqlCollectionLiteral::Map(pairs), CqlType::Map(key_type, val_type)) => {
            let pairs = pairs
                .iter()
                .map(|(k, v)| {
                    Ok((
                        literal_to_value(k, key_type)?,
                        literal_to_value(v, val_type)?,
                    ))
                })
                .collect::<Result<Vec<_>, Error>>()?;
            Ok(Value::Map(pairs))
        }
        _ => Err(type_mismatch("collection", target)),
    }
}

/// Convert a tuple literal to `Value::Tuple`, using the positional types from
/// `CqlType::Tuple`.
///
/// Expects a non-Frozen target type (Frozen is unwrapped by caller).
#[cfg(feature = "write-support")]
fn tuple_to_value(elements: &[CqlLiteral], target: &CqlType) -> Result<Value, Error> {
    match target {
        CqlType::Tuple(field_types) => {
            if elements.len() != field_types.len() {
                return Err(Error::InvalidInput(format!(
                    "tuple has {} elements but schema expects {}",
                    elements.len(),
                    field_types.len()
                )));
            }
            let values = elements
                .iter()
                .zip(field_types.iter())
                .map(|(lit, ft)| literal_to_value(lit, ft))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Value::Tuple(values))
        }
        _ => Err(type_mismatch("tuple", target)),
    }
}

/// Convert a UDT literal to `Value::Udt`, looking up field types from
/// `CqlType::Udt`.
///
/// Expects a non-Frozen target type (Frozen is unwrapped by caller).
#[cfg(feature = "write-support")]
fn udt_to_value(udt: &CqlUdtLiteral, target: &CqlType) -> Result<Value, Error> {
    match target {
        CqlType::Udt(type_name, field_defs) => {
            let mut fields: Vec<UdtField> = Vec::with_capacity(udt.fields.len());
            for (field_id, field_lit) in &udt.fields {
                let field_name = field_id.name.as_str();
                // Find the schema type for this field
                let field_type = field_defs
                    .iter()
                    .find(|(n, _)| n.eq_ignore_ascii_case(field_name))
                    .map(|(_, t)| t)
                    .ok_or_else(|| {
                        Error::InvalidInput(format!(
                            "field {:?} not found in UDT {:?}",
                            field_name, type_name
                        ))
                    })?;
                let value = literal_to_value(field_lit, field_type)?;
                fields.push(UdtField {
                    name: field_name.to_string(),
                    value: Some(value),
                });
            }
            Ok(Value::Udt(UdtValue {
                type_name: type_name.clone(),
                keyspace: String::new(),
                fields,
            }))
        }
        _ => Err(type_mismatch("udt", target)),
    }
}

/// Build a type-mismatch error with a human-readable message.
#[cfg(feature = "write-support")]
fn type_mismatch(literal_type: &str, target: &CqlType) -> Error {
    Error::InvalidInput(format!(
        "cannot coerce {} literal to {:?}",
        literal_type, target
    ))
}

/// Build an overflow error message.
#[cfg(feature = "write-support")]
fn overflow_error(value: i64, target: &str) -> Error {
    Error::InvalidInput(format!(
        "integer value {} overflows target type {}",
        value, target
    ))
}

#[cfg(all(test, feature = "write-support"))]
mod tests {
    use super::*;
    use crate::cql::ast::{CqlIdentifier, CqlInsert, CqlInsertValues, CqlLiteral, CqlTable, CqlUsing};
    use crate::schema::CqlType;
    use crate::storage::write_engine::mutation::CellOperation;
    use crate::types::Value;
    use std::collections::HashMap;

    // ── Test schema helper ────────────────────────────────────────────────────

    fn test_schema() -> crate::schema::TableSchema {
        crate::schema::TableSchema {
            keyspace: "test_ks".into(),
            table: "test_tbl".into(),
            partition_keys: vec![crate::schema::KeyColumn {
                name: "id".into(),
                data_type: "uuid".into(),
                position: 0,
            }],
            clustering_keys: vec![crate::schema::ClusteringColumn {
                name: "ts".into(),
                data_type: "timestamp".into(),
                position: 0,
                order: crate::schema::ClusteringOrder::Asc,
            }],
            columns: vec![
                crate::schema::Column {
                    name: "id".into(),
                    data_type: "uuid".into(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                crate::schema::Column {
                    name: "ts".into(),
                    data_type: "timestamp".into(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                crate::schema::Column {
                    name: "name".into(),
                    data_type: "text".into(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                crate::schema::Column {
                    name: "age".into(),
                    data_type: "int".into(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
        }
    }

    // ── insert_to_mutation tests ──────────────────────────────────────────────

    #[test]
    fn test_insert_to_mutation() {
        let schema = test_schema();
        // INSERT INTO test_ks.test_tbl (id, ts, name, age) VALUES (uuid, 1000, 'Alice', 30)
        let uuid_str = "550e8400-e29b-41d4-a716-446655440000".to_string();
        let insert = CqlInsert {
            table: CqlTable::with_keyspace("test_ks", "test_tbl"),
            columns: vec![
                CqlIdentifier::new("id"),
                CqlIdentifier::new("ts"),
                CqlIdentifier::new("name"),
                CqlIdentifier::new("age"),
            ],
            values: CqlInsertValues::Values(vec![
                CqlExpression::Literal(CqlLiteral::Uuid(uuid_str.clone())),
                CqlExpression::Literal(CqlLiteral::Integer(1_000_000)),
                CqlExpression::Literal(CqlLiteral::String("Alice".to_string())),
                CqlExpression::Literal(CqlLiteral::Integer(30)),
            ]),
            if_not_exists: false,
            using: None,
        };

        let mutation = insert_to_mutation(&insert, &schema).unwrap();

        // Verify table
        assert_eq!(mutation.table.keyspace, "test_ks");
        assert_eq!(mutation.table.table, "test_tbl");

        // Verify partition key
        assert_eq!(mutation.partition_key.columns.len(), 1);
        assert_eq!(mutation.partition_key.columns[0].0, "id");
        matches!(mutation.partition_key.columns[0].1, Value::Uuid(_));

        // Verify clustering key
        let ck = mutation.clustering_key.unwrap();
        assert_eq!(ck.columns.len(), 1);
        assert_eq!(ck.columns[0].0, "ts");
        assert_eq!(ck.columns[0].1, Value::Timestamp(1_000_000));

        // Verify regular column operations (name and age)
        assert_eq!(mutation.operations.len(), 2);
        let op_map: std::collections::HashMap<_, _> = mutation
            .operations
            .into_iter()
            .filter_map(|op| {
                if let CellOperation::Write { column, value } = op {
                    Some((column, value))
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(op_map.get("name"), Some(&Value::Text("Alice".to_string())));
        assert_eq!(op_map.get("age"), Some(&Value::Integer(30)));
    }

    #[test]
    fn test_insert_with_using_timestamp() {
        let schema = test_schema();
        let uuid_str = "550e8400-e29b-41d4-a716-446655440000".to_string();
        let custom_ts: i64 = 1_700_000_000_000_000; // some specific microsecond timestamp
        let insert = CqlInsert {
            table: CqlTable::new("test_tbl"),
            columns: vec![
                CqlIdentifier::new("id"),
                CqlIdentifier::new("ts"),
            ],
            values: CqlInsertValues::Values(vec![
                CqlExpression::Literal(CqlLiteral::Uuid(uuid_str)),
                CqlExpression::Literal(CqlLiteral::Integer(1_000_000)),
            ]),
            if_not_exists: false,
            using: Some(CqlUsing {
                timestamp: Some(CqlExpression::Literal(CqlLiteral::Integer(custom_ts))),
                ttl: None,
            }),
        };

        let mutation = insert_to_mutation(&insert, &schema).unwrap();
        assert_eq!(mutation.timestamp_micros, custom_ts);
        assert_eq!(mutation.ttl_seconds, None);
    }

    #[test]
    fn test_insert_missing_partition_key() {
        let schema = test_schema();
        // INSERT without the partition key column 'id'
        let insert = CqlInsert {
            table: CqlTable::new("test_tbl"),
            columns: vec![CqlIdentifier::new("name")],
            values: CqlInsertValues::Values(vec![CqlExpression::Literal(CqlLiteral::String(
                "Alice".to_string(),
            ))]),
            if_not_exists: false,
            using: None,
        };

        let err = insert_to_mutation(&insert, &schema).unwrap_err();
        match err {
            Error::InvalidInput(msg) => {
                assert!(msg.contains("Partition key column") && msg.contains("id"));
            }
            other => panic!("expected InvalidInput, got {:?}", other),
        }
    }

    #[test]
    fn test_insert_json_unsupported() {
        let schema = test_schema();
        let insert = CqlInsert {
            table: CqlTable::new("test_tbl"),
            columns: vec![],
            values: CqlInsertValues::Json("{\"id\": \"...\"}".to_string()),
            if_not_exists: false,
            using: None,
        };

        let err = insert_to_mutation(&insert, &schema).unwrap_err();
        match err {
            Error::InvalidInput(msg) => assert!(msg.contains("JSON")),
            other => panic!("expected InvalidInput, got {:?}", other),
        }
    }

    #[test]
    fn test_insert_column_count_mismatch() {
        let schema = test_schema();
        let insert = CqlInsert {
            table: CqlTable::new("test_tbl"),
            // 2 columns but only 1 value
            columns: vec![CqlIdentifier::new("id"), CqlIdentifier::new("name")],
            values: CqlInsertValues::Values(vec![CqlExpression::Literal(CqlLiteral::Uuid(
                "550e8400-e29b-41d4-a716-446655440000".to_string(),
            ))]),
            if_not_exists: false,
            using: None,
        };

        let err = insert_to_mutation(&insert, &schema).unwrap_err();
        match err {
            Error::InvalidInput(msg) => {
                assert!(msg.contains("2 columns") && msg.contains("1 values"));
            }
            other => panic!("expected InvalidInput, got {:?}", other),
        }
    }

    #[test]
    fn test_insert_table_mismatch() {
        let schema = test_schema();
        let insert = CqlInsert {
            table: CqlTable::new("wrong_table"),
            columns: vec![],
            values: CqlInsertValues::Values(vec![]),
            if_not_exists: false,
            using: None,
        };

        let err = insert_to_mutation(&insert, &schema).unwrap_err();
        match err {
            Error::InvalidInput(msg) => {
                assert!(msg.contains("wrong_table") || msg.contains("test_tbl"));
            }
            other => panic!("expected InvalidInput, got {:?}", other),
        }
    }

    #[test]
    fn test_expression_negative_integer() {
        // expression_to_value with unary minus on an integer literal
        let expr = CqlExpression::Unary {
            operator: CqlUnaryOperator::Minus,
            operand: Box::new(CqlExpression::Literal(CqlLiteral::Integer(42))),
        };
        let result = expression_to_value(&expr, &CqlType::Int).unwrap();
        assert_eq!(result, Value::Integer(-42));
    }

    #[test]
    fn test_insert_with_ttl() {
        let schema = test_schema();
        let uuid_str = "550e8400-e29b-41d4-a716-446655440000".to_string();
        let insert = CqlInsert {
            table: CqlTable::new("test_tbl"),
            columns: vec![CqlIdentifier::new("id"), CqlIdentifier::new("ts")],
            values: CqlInsertValues::Values(vec![
                CqlExpression::Literal(CqlLiteral::Uuid(uuid_str)),
                CqlExpression::Literal(CqlLiteral::Integer(1_000_000)),
            ]),
            if_not_exists: false,
            using: Some(CqlUsing {
                timestamp: None,
                ttl: Some(CqlExpression::Literal(CqlLiteral::Integer(3600))),
            }),
        };

        let mutation = insert_to_mutation(&insert, &schema).unwrap();
        assert_eq!(mutation.ttl_seconds, Some(3600u32));
    }

    // ── Null ─────────────────────────────────────────────────────────────────

    #[test]
    fn test_null_literal() {
        let result = literal_to_value(&CqlLiteral::Null, &CqlType::Text).unwrap();
        assert_eq!(result, Value::Null);

        let result = literal_to_value(&CqlLiteral::Null, &CqlType::Int).unwrap();
        assert_eq!(result, Value::Null);
    }

    // ── Boolean ──────────────────────────────────────────────────────────────

    #[test]
    fn test_boolean_literal() {
        let t = literal_to_value(&CqlLiteral::Boolean(true), &CqlType::Boolean).unwrap();
        assert_eq!(t, Value::Boolean(true));

        let f = literal_to_value(&CqlLiteral::Boolean(false), &CqlType::Boolean).unwrap();
        assert_eq!(f, Value::Boolean(false));
    }

    // ── Integer coercions ────────────────────────────────────────────────────

    #[test]
    fn test_integer_to_int() {
        let v = literal_to_value(&CqlLiteral::Integer(42), &CqlType::Int).unwrap();
        assert_eq!(v, Value::Integer(42));
    }

    #[test]
    fn test_integer_to_bigint() {
        let v = literal_to_value(&CqlLiteral::Integer(i64::MAX), &CqlType::BigInt).unwrap();
        assert_eq!(v, Value::BigInt(i64::MAX));
    }

    #[test]
    fn test_integer_to_smallint() {
        let v = literal_to_value(&CqlLiteral::Integer(1000), &CqlType::SmallInt).unwrap();
        assert_eq!(v, Value::SmallInt(1000));
    }

    #[test]
    fn test_integer_to_tinyint() {
        let v = literal_to_value(&CqlLiteral::Integer(127), &CqlType::TinyInt).unwrap();
        assert_eq!(v, Value::TinyInt(127));
    }

    #[test]
    fn test_integer_overflow_tinyint() {
        let err = literal_to_value(&CqlLiteral::Integer(999), &CqlType::TinyInt).unwrap_err();
        match err {
            Error::InvalidInput(msg) => assert!(msg.contains("overflow")),
            other => panic!("expected InvalidInput, got {:?}", other),
        }
    }

    #[test]
    fn test_integer_to_timestamp() {
        let ts: i64 = 1_700_000_000_000;
        let v = literal_to_value(&CqlLiteral::Integer(ts), &CqlType::Timestamp).unwrap();
        assert_eq!(v, Value::Timestamp(ts));
    }

    // ── Float coercions ──────────────────────────────────────────────────────

    #[test]
    fn test_float_to_float() {
        // Use 1.5 (exactly representable in f32/f64, not an approximation of a named constant)
        let v = literal_to_value(&CqlLiteral::Float(1.5), &CqlType::Float).unwrap();
        // CqlType::Float → Value::Float32
        match v {
            Value::Float32(f) => assert!((f - 1.5_f32).abs() < f32::EPSILON),
            other => panic!("expected Value::Float32, got {:?}", other),
        }
    }

    #[test]
    fn test_float_to_double() {
        let v = literal_to_value(&CqlLiteral::Float(1.25), &CqlType::Double).unwrap();
        assert_eq!(v, Value::Float(1.25));
    }

    // ── String coercions ─────────────────────────────────────────────────────

    #[test]
    fn test_string_to_text() {
        let v = literal_to_value(&CqlLiteral::String("hello".to_string()), &CqlType::Text).unwrap();
        assert_eq!(v, Value::Text("hello".to_string()));
    }

    #[test]
    fn test_string_to_varchar() {
        let v =
            literal_to_value(&CqlLiteral::String("world".to_string()), &CqlType::Varchar).unwrap();
        assert_eq!(v, Value::Text("world".to_string()));
    }

    #[test]
    fn test_string_to_ascii() {
        let v =
            literal_to_value(&CqlLiteral::String("ascii".to_string()), &CqlType::Ascii).unwrap();
        assert_eq!(v, Value::Text("ascii".to_string()));
    }

    // ── UUID ─────────────────────────────────────────────────────────────────

    #[test]
    fn test_uuid_literal() {
        let uuid_str = "550e8400-e29b-41d4-a716-446655440000".to_string();
        let v = literal_to_value(&CqlLiteral::Uuid(uuid_str), &CqlType::Uuid).unwrap();
        match v {
            Value::Uuid(bytes) => {
                assert_eq!(bytes[0], 0x55);
                assert_eq!(bytes[1], 0x0e);
                assert_eq!(bytes[15], 0x00);
            }
            other => panic!("expected Value::Uuid, got {:?}", other),
        }
    }

    // ── Blob ─────────────────────────────────────────────────────────────────

    #[test]
    fn test_blob_literal() {
        let v =
            literal_to_value(&CqlLiteral::Blob("0xDEADBEEF".to_string()), &CqlType::Blob).unwrap();
        assert_eq!(v, Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]));
    }

    // ── Type mismatch ────────────────────────────────────────────────────────

    #[test]
    fn test_type_mismatch_error() {
        let err = literal_to_value(&CqlLiteral::Boolean(true), &CqlType::Int).unwrap_err();
        match err {
            Error::InvalidInput(msg) => assert!(msg.contains("boolean")),
            other => panic!("expected InvalidInput, got {:?}", other),
        }
    }

    // ── Frozen unwrapping ────────────────────────────────────────────────────

    #[test]
    fn test_frozen_unwraps_to_inner() {
        let frozen_text = CqlType::Frozen(Box::new(CqlType::Text));
        let v = literal_to_value(&CqlLiteral::String("frozen".to_string()), &frozen_text).unwrap();
        match v {
            Value::Frozen(inner) => assert_eq!(*inner, Value::Text("frozen".to_string())),
            other => panic!("expected Value::Frozen, got {:?}", other),
        }
    }

    // ── Inet ─────────────────────────────────────────────────────────────────

    #[test]
    fn test_inet_ipv4() {
        let v =
            literal_to_value(&CqlLiteral::String("127.0.0.1".to_string()), &CqlType::Inet).unwrap();
        assert_eq!(v, Value::Inet(vec![127, 0, 0, 1]));
    }

    #[test]
    fn test_inet_ipv6() {
        let v = literal_to_value(&CqlLiteral::String("::1".to_string()), &CqlType::Inet).unwrap();
        match v {
            Value::Inet(bytes) => assert_eq!(bytes.len(), 16),
            other => panic!("expected Value::Inet, got {:?}", other),
        }
    }

    // ── Collections ──────────────────────────────────────────────────────────

    #[test]
    fn test_list_conversion() {
        let list_lit = CqlLiteral::Collection(CqlCollectionLiteral::List(vec![
            CqlLiteral::Integer(1),
            CqlLiteral::Integer(2),
            CqlLiteral::Integer(3),
        ]));
        let target = CqlType::List(Box::new(CqlType::Int));
        let v = literal_to_value(&list_lit, &target).unwrap();
        assert_eq!(
            v,
            Value::List(vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3)
            ])
        );
    }

    #[test]
    fn test_set_conversion() {
        let set_lit = CqlLiteral::Collection(CqlCollectionLiteral::Set(vec![
            CqlLiteral::String("a".to_string()),
            CqlLiteral::String("b".to_string()),
        ]));
        let target = CqlType::Set(Box::new(CqlType::Text));
        let v = literal_to_value(&set_lit, &target).unwrap();
        assert_eq!(
            v,
            Value::Set(vec![
                Value::Text("a".to_string()),
                Value::Text("b".to_string())
            ])
        );
    }

    #[test]
    fn test_map_conversion() {
        let map_lit = CqlLiteral::Collection(CqlCollectionLiteral::Map(vec![(
            CqlLiteral::String("key".to_string()),
            CqlLiteral::Integer(99),
        )]));
        let target = CqlType::Map(Box::new(CqlType::Text), Box::new(CqlType::BigInt));
        let v = literal_to_value(&map_lit, &target).unwrap();
        assert_eq!(
            v,
            Value::Map(vec![(Value::Text("key".to_string()), Value::BigInt(99))])
        );
    }

    // ── Varint ───────────────────────────────────────────────────────────────

    #[test]
    fn test_integer_to_varint() {
        let v = literal_to_value(&CqlLiteral::Integer(256), &CqlType::Varint).unwrap();
        match v {
            Value::Varint(bytes) => assert!(!bytes.is_empty()),
            other => panic!("expected Value::Varint, got {:?}", other),
        }
    }

    // ── Counter ──────────────────────────────────────────────────────────────

    #[test]
    fn test_integer_to_counter() {
        let v = literal_to_value(&CqlLiteral::Integer(100), &CqlType::Counter).unwrap();
        assert_eq!(v, Value::Counter(100));
    }

    // ── Decimal ──────────────────────────────────────────────────────────────

    #[test]
    fn test_integer_to_decimal() {
        let v = literal_to_value(&CqlLiteral::Integer(42), &CqlType::Decimal).unwrap();
        match v {
            Value::Decimal { scale, unscaled } => {
                assert_eq!(scale, 0);
                assert!(!unscaled.is_empty());
            }
            other => panic!("expected Value::Decimal, got {:?}", other),
        }
    }

    #[test]
    fn test_float_to_decimal_returns_error() {
        let err = literal_to_value(&CqlLiteral::Float(1.23), &CqlType::Decimal).unwrap_err();
        match err {
            Error::InvalidInput(msg) => assert!(msg.contains("Float-to-Decimal")),
            other => panic!("expected InvalidInput, got {:?}", other),
        }
    }

    // ── Duration ─────────────────────────────────────────────────────────────

    #[test]
    fn test_integer_to_duration_returns_error() {
        let err = literal_to_value(&CqlLiteral::Integer(42), &CqlType::Duration).unwrap_err();
        match err {
            Error::InvalidInput(msg) => assert!(msg.contains("Duration")),
            other => panic!("expected InvalidInput, got {:?}", other),
        }
    }

    // ── varint_to_bytes correctness ───────────────────────────────────────────

    #[test]
    fn test_varint_bytes_correctness() {
        // 256 as i64 big-endian is [0,0,0,0,0,0,1,0]
        // varint_to_bytes should produce the minimal representation: [1, 0]
        let bytes = varint_to_bytes(256);
        assert_eq!(bytes, vec![0x01, 0x00]);
    }

    // ── Additional collection tests ───────────────────────────────────────────

    #[test]
    fn test_list_of_int() {
        let lit = CqlLiteral::Collection(CqlCollectionLiteral::List(vec![
            CqlLiteral::Integer(1),
            CqlLiteral::Integer(2),
            CqlLiteral::Integer(3),
        ]));
        let result = literal_to_value(&lit, &CqlType::List(Box::new(CqlType::Int)));
        assert_eq!(
            result.unwrap(),
            Value::List(vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3),
            ])
        );
    }

    #[test]
    fn test_set_of_text() {
        let lit = CqlLiteral::Collection(CqlCollectionLiteral::Set(vec![
            CqlLiteral::String("a".into()),
            CqlLiteral::String("b".into()),
        ]));
        let result = literal_to_value(&lit, &CqlType::Set(Box::new(CqlType::Text)));
        assert_eq!(
            result.unwrap(),
            Value::Set(vec![
                Value::Text("a".into()),
                Value::Text("b".into()),
            ])
        );
    }

    #[test]
    fn test_map_of_text_to_int() {
        let lit = CqlLiteral::Collection(CqlCollectionLiteral::Map(vec![(
            CqlLiteral::String("a".into()),
            CqlLiteral::Integer(1),
        )]));
        let result = literal_to_value(
            &lit,
            &CqlType::Map(Box::new(CqlType::Text), Box::new(CqlType::Int)),
        );
        assert_eq!(
            result.unwrap(),
            Value::Map(vec![(Value::Text("a".into()), Value::Integer(1)),])
        );
    }

    #[test]
    fn test_frozen_list() {
        let lit = CqlLiteral::Collection(CqlCollectionLiteral::List(vec![
            CqlLiteral::Integer(1),
        ]));
        let result = literal_to_value(
            &lit,
            &CqlType::Frozen(Box::new(CqlType::List(Box::new(CqlType::Int)))),
        );
        // literal_to_value wraps the inner value in Value::Frozen
        match result.unwrap() {
            Value::Frozen(inner) => {
                assert_eq!(*inner, Value::List(vec![Value::Integer(1)]));
            }
            other => panic!("expected Value::Frozen, got {:?}", other),
        }
    }

    #[test]
    fn test_tuple() {
        let lit = CqlLiteral::Tuple(vec![
            CqlLiteral::Integer(1),
            CqlLiteral::String("hello".into()),
        ]);
        let result = literal_to_value(
            &lit,
            &CqlType::Tuple(vec![CqlType::Int, CqlType::Text]),
        );
        assert_eq!(
            result.unwrap(),
            Value::Tuple(vec![Value::Integer(1), Value::Text("hello".into()),])
        );
    }

    #[test]
    fn test_tuple_wrong_arity() {
        let lit = CqlLiteral::Tuple(vec![CqlLiteral::Integer(1)]);
        let result = literal_to_value(
            &lit,
            &CqlType::Tuple(vec![CqlType::Int, CqlType::Text]),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_smallint_overflow() {
        let result = literal_to_value(&CqlLiteral::Integer(40000), &CqlType::SmallInt);
        assert!(result.is_err());
    }

    #[test]
    fn test_int_overflow() {
        let result = literal_to_value(&CqlLiteral::Integer(3_000_000_000), &CqlType::Int);
        assert!(result.is_err());
    }

    #[test]
    fn test_timeuuid() {
        let result = literal_to_value(
            &CqlLiteral::Uuid("550e8400-e29b-11d4-a716-446655440000".into()),
            &CqlType::TimeUuid,
        );
        assert!(result.is_ok());
        if let Value::Uuid(bytes) = result.unwrap() {
            assert_eq!(bytes.len(), 16);
        } else {
            panic!("expected Uuid");
        }
    }

    #[test]
    fn test_empty_list() {
        let lit = CqlLiteral::Collection(CqlCollectionLiteral::List(vec![]));
        let result = literal_to_value(&lit, &CqlType::List(Box::new(CqlType::Int)));
        assert_eq!(result.unwrap(), Value::List(vec![]));
    }

    #[test]
    fn test_collection_type_mismatch() {
        // List literal but Map target type
        let lit = CqlLiteral::Collection(CqlCollectionLiteral::List(vec![
            CqlLiteral::Integer(1),
        ]));
        let result = literal_to_value(
            &lit,
            &CqlType::Map(Box::new(CqlType::Text), Box::new(CqlType::Int)),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_nested_collection() {
        // list<frozen<list<int>>>
        let inner_list = CqlLiteral::Collection(CqlCollectionLiteral::List(vec![
            CqlLiteral::Integer(1),
            CqlLiteral::Integer(2),
        ]));
        let outer_list =
            CqlLiteral::Collection(CqlCollectionLiteral::List(vec![inner_list]));
        let target = CqlType::List(Box::new(CqlType::Frozen(Box::new(CqlType::List(
            Box::new(CqlType::Int),
        )))));
        let result = literal_to_value(&outer_list, &target);
        assert!(result.is_ok());
    }
}
