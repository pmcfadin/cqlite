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
    CqlAssignmentOperator, CqlBatch, CqlBinaryOperator, CqlCollectionLiteral, CqlDelete,
    CqlExpression, CqlInsert, CqlInsertValues, CqlLiteral, CqlTable, CqlUdtLiteral,
    CqlUnaryOperator, CqlUpdate, CqlUsing,
};
#[cfg(feature = "write-support")]
use crate::schema::{CqlType, TableSchema};
#[cfg(feature = "write-support")]
use crate::storage::write_engine::mutation::{
    CellOperation, ClusteringBound, ClusteringKey, Mutation, PartitionKey, PartitionTombstone,
    RangeTombstone, TableId,
};
#[cfg(feature = "write-support")]
use crate::types::{UdtField, UdtValue, Value};
#[cfg(feature = "write-support")]
use crate::Error;

/// Return the current wall-clock time as seconds since Unix epoch, cast to i32.
///
/// This is the correct value for `local_deletion_time` in tombstones.  It must
/// reflect real calendar time so that Cassandra's GC-grace expiry logic works
/// correctly; using a logical CQL timestamp instead would break that invariant.
///
/// Returns 0 on the extremely unlikely event that the system clock is before
/// the Unix epoch (e.g. test environments with a mocked clock).
#[cfg(feature = "write-support")]
fn wall_clock_local_deletion_time() -> i32 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i32)
        .unwrap_or(0)
}

/// Convert a parsed `CqlInsert` AST node into a `Mutation` using schema information.
///
/// Both `VALUES` and `JSON` forms of INSERT are supported. For the JSON form the
/// top-level object keys are matched case-insensitively to schema columns; null
/// values are silently dropped (no write operation is emitted).
///
/// # Errors
///
/// Returns `Error::InvalidInput` when:
/// - The statement targets a different table than the schema
/// - The number of columns and values do not match (VALUES form)
/// - A required partition key column is missing from the INSERT
/// - A value cannot be coerced to its schema type
/// - `IF NOT EXISTS` is specified (not supported by the local write engine)
#[cfg(feature = "write-support")]
pub(crate) fn insert_to_mutation(
    insert: &CqlInsert,
    schema: &TableSchema,
) -> Result<Mutation, Error> {
    if insert.if_not_exists {
        return Err(Error::InvalidInput(
            "IF NOT EXISTS is not supported by the local write engine".to_string(),
        ));
    }
    validate_table(&insert.table, schema)?;

    // Extract (column_name, expression) pairs
    let values = match &insert.values {
        CqlInsertValues::Values(exprs) => exprs,
        CqlInsertValues::Json(json_str) => {
            return insert_json_to_mutation(json_str, &insert.table, &insert.using, schema);
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
    let mut operations: Vec<CellOperation> = Vec::with_capacity(col_val_pairs.len());
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

/// Convert a JSON string from `INSERT INTO ... JSON` into a `Mutation`.
///
/// The JSON must be a top-level object whose keys are column names (matched
/// case-insensitively). Null values are skipped (no write operation). The
/// partition key columns must be present; clustering key and regular columns are
/// optional.
///
/// # Errors
///
/// Returns `Error::InvalidInput` when:
/// - The JSON is malformed
/// - The top-level JSON value is not an object
/// - A required partition key column is missing from the JSON
/// - A key in the JSON does not correspond to a schema column
/// - A JSON value cannot be converted to the target CQL type
#[cfg(feature = "write-support")]
fn insert_json_to_mutation(
    json_str: &str,
    table_ref: &CqlTable,
    using: &Option<CqlUsing>,
    schema: &TableSchema,
) -> Result<Mutation, Error> {
    // Parse JSON
    let json_obj: serde_json::Value = serde_json::from_str(json_str)
        .map_err(|e| Error::InvalidInput(format!("Invalid JSON in INSERT: {}", e)))?;

    let obj = json_obj.as_object().ok_or_else(|| {
        Error::InvalidInput(
            "INSERT JSON requires a JSON object, not array or primitive".to_string(),
        )
    })?;

    // Validate table reference
    validate_table(table_ref, schema)?;

    // Build column-value pairs, matching JSON keys to schema columns (case-insensitive)
    let mut col_values: Vec<(String, serde_json::Value)> = Vec::new();
    for (key, val) in obj {
        if val.is_null() {
            continue; // Skip null values (no write operation)
        }
        let col_name_lc = key.to_lowercase();
        let _column = schema.get_column(&col_name_lc).ok_or_else(|| {
            Error::InvalidInput(format!("Unknown column '{}' in JSON INSERT", key))
        })?;
        col_values.push((col_name_lc, val.clone()));
    }

    // Extract partition key values (required)
    let ordered_pk = schema.ordered_partition_keys();
    let mut pk_columns: Vec<(String, Value)> = Vec::with_capacity(ordered_pk.len());
    for pk_col in &ordered_pk {
        let col_name_lc = pk_col.name.to_lowercase();
        let json_val = col_values
            .iter()
            .find(|(n, _)| *n == col_name_lc)
            .map(|(_, v)| v)
            .ok_or_else(|| {
                Error::InvalidInput(format!(
                    "Partition key column '{}' is missing from JSON INSERT",
                    pk_col.name
                ))
            })?;
        let cql_type = CqlType::parse(&pk_col.data_type)?;
        let value = json_value_to_cql_value(json_val, &cql_type)?;
        pk_columns.push((pk_col.name.clone(), value));
    }
    let partition_key = PartitionKey::new(pk_columns);

    // Extract clustering key values (optional)
    let ordered_ck = schema.ordered_clustering_keys();
    let mut ck_columns: Vec<(String, Value)> = Vec::with_capacity(ordered_ck.len());
    for ck_col in &ordered_ck {
        let col_name_lc = ck_col.name.to_lowercase();
        if let Some((_, json_val)) = col_values.iter().find(|(n, _)| *n == col_name_lc) {
            let cql_type = CqlType::parse(&ck_col.data_type)?;
            let value = json_value_to_cql_value(json_val, &cql_type)?;
            ck_columns.push((ck_col.name.clone(), value));
        }
    }
    let clustering_key = if ck_columns.is_empty() {
        None
    } else {
        Some(ClusteringKey::new(ck_columns))
    };

    // Build operations for regular columns
    let pk_names: Vec<String> = ordered_pk.iter().map(|c| c.name.to_lowercase()).collect();
    let ck_names: Vec<String> = ordered_ck.iter().map(|c| c.name.to_lowercase()).collect();

    let mut operations: Vec<CellOperation> = Vec::new();
    for (col_name_lc, json_val) in &col_values {
        if pk_names.contains(col_name_lc) || ck_names.contains(col_name_lc) {
            continue;
        }
        let column = schema
            .get_column(col_name_lc)
            .ok_or_else(|| Error::InvalidInput(format!("Unknown column '{}'", col_name_lc)))?;
        let cql_type = CqlType::parse(&column.data_type)?;
        let value = json_value_to_cql_value(json_val, &cql_type)?;
        operations.push(CellOperation::Write {
            column: column.name.clone(),
            value,
        });
    }

    let timestamp_micros = extract_timestamp(using)?;
    let ttl_seconds = extract_ttl(using)?;
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

/// Convert a `serde_json::Value` to an internal `Value`, guided by the target CQL type.
///
/// Supports boolean, numeric, string, array, and object JSON types. Null values
/// should be filtered out before calling this function.
#[cfg(feature = "write-support")]
fn json_value_to_cql_value(
    json_val: &serde_json::Value,
    target_type: &CqlType,
) -> Result<Value, Error> {
    use serde_json::Value as JV;

    // Unwrap Frozen – it does not affect value representation
    if let CqlType::Frozen(inner) = target_type {
        return json_value_to_cql_value(json_val, inner);
    }

    match (json_val, target_type) {
        // Null — should be filtered before reaching here
        (JV::Null, _) => Err(Error::InvalidInput(
            "Unexpected null value in JSON conversion".to_string(),
        )),

        // Boolean
        (JV::Bool(b), CqlType::Boolean) => Ok(Value::Boolean(*b)),

        // Integer numbers
        (JV::Number(n), CqlType::Int) => {
            let v = n
                .as_i64()
                .ok_or_else(|| Error::InvalidInput(format!("Cannot convert {} to int", n)))?;
            let v = i32::try_from(v)
                .map_err(|_| Error::InvalidInput(format!("Value {} out of range for int", v)))?;
            Ok(Value::Integer(v))
        }
        (JV::Number(n), CqlType::BigInt) => {
            let v = n
                .as_i64()
                .ok_or_else(|| Error::InvalidInput(format!("Cannot convert {} to bigint", n)))?;
            Ok(Value::BigInt(v))
        }
        (JV::Number(n), CqlType::SmallInt) => {
            let v = n
                .as_i64()
                .ok_or_else(|| Error::InvalidInput(format!("Cannot convert {} to smallint", n)))?;
            let v = i16::try_from(v).map_err(|_| {
                Error::InvalidInput(format!("Value {} out of range for smallint", v))
            })?;
            Ok(Value::SmallInt(v))
        }
        (JV::Number(n), CqlType::TinyInt) => {
            let v = n
                .as_i64()
                .ok_or_else(|| Error::InvalidInput(format!("Cannot convert {} to tinyint", n)))?;
            let v = i8::try_from(v).map_err(|_| {
                Error::InvalidInput(format!("Value {} out of range for tinyint", v))
            })?;
            Ok(Value::TinyInt(v))
        }
        (JV::Number(n), CqlType::Timestamp) => {
            let v = n
                .as_i64()
                .ok_or_else(|| Error::InvalidInput(format!("Cannot convert {} to timestamp", n)))?;
            Ok(Value::Timestamp(v))
        }
        (JV::Number(n), CqlType::Float) => {
            let v = n
                .as_f64()
                .ok_or_else(|| Error::InvalidInput(format!("Cannot convert {} to float", n)))?;
            Ok(Value::Float32(v as f32))
        }
        (JV::Number(n), CqlType::Double) => {
            let v = n
                .as_f64()
                .ok_or_else(|| Error::InvalidInput(format!("Cannot convert {} to double", n)))?;
            Ok(Value::Float(v))
        }
        (JV::Number(n), CqlType::Varint) => {
            let v = n
                .as_i64()
                .ok_or_else(|| Error::InvalidInput(format!("Cannot convert {} to varint", n)))?;
            Ok(Value::Varint(varint_to_bytes(v)))
        }

        // String types
        (JV::String(s), CqlType::Text | CqlType::Varchar | CqlType::Ascii) => {
            Ok(Value::Text(s.clone()))
        }
        (JV::String(s), CqlType::Uuid | CqlType::TimeUuid) => parse_uuid(s),
        (JV::String(s), CqlType::Blob) => parse_blob(s),
        (JV::String(s), CqlType::Inet) => parse_inet(s),
        (JV::String(s), CqlType::Int) => {
            let v: i32 = s
                .parse()
                .map_err(|_| Error::InvalidInput(format!("Cannot parse '{}' as int", s)))?;
            Ok(Value::Integer(v))
        }
        (JV::String(s), CqlType::BigInt) => {
            let v: i64 = s
                .parse()
                .map_err(|_| Error::InvalidInput(format!("Cannot parse '{}' as bigint", s)))?;
            Ok(Value::BigInt(v))
        }
        (JV::String(s), CqlType::Boolean) => match s.to_lowercase().as_str() {
            "true" => Ok(Value::Boolean(true)),
            "false" => Ok(Value::Boolean(false)),
            _ => Err(Error::InvalidInput(format!(
                "Cannot parse '{}' as boolean",
                s
            ))),
        },
        (JV::String(s), CqlType::Timestamp) => {
            if let Ok(v) = s.parse::<i64>() {
                return Ok(Value::Timestamp(v));
            }
            Err(Error::InvalidInput(format!(
                "Cannot parse '{}' as timestamp",
                s
            )))
        }
        (JV::String(s), CqlType::Float) => {
            let v: f32 = s
                .parse()
                .map_err(|_| Error::InvalidInput(format!("Cannot parse '{}' as float", s)))?;
            Ok(Value::Float32(v))
        }
        (JV::String(s), CqlType::Double) => {
            let v: f64 = s
                .parse()
                .map_err(|_| Error::InvalidInput(format!("Cannot parse '{}' as double", s)))?;
            Ok(Value::Float(v))
        }

        // Arrays → Lists, Sets, Tuples
        (JV::Array(arr), CqlType::List(element_type)) => {
            let elements = arr
                .iter()
                .map(|item| json_value_to_cql_value(item, element_type))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Value::List(elements))
        }
        (JV::Array(arr), CqlType::Set(element_type)) => {
            let elements = arr
                .iter()
                .map(|item| json_value_to_cql_value(item, element_type))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Value::Set(elements))
        }
        (JV::Array(arr), CqlType::Tuple(types)) => {
            if arr.len() != types.len() {
                return Err(Error::InvalidInput(format!(
                    "JSON array has {} elements but tuple expects {}",
                    arr.len(),
                    types.len()
                )));
            }
            let elements = arr
                .iter()
                .zip(types.iter())
                .map(|(item, t)| json_value_to_cql_value(item, t))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Value::Tuple(elements))
        }

        // Objects → Maps
        (JV::Object(map), CqlType::Map(key_type, val_type)) => {
            let entries = map
                .iter()
                .map(|(k, v)| {
                    let key_json = JV::String(k.clone());
                    let key = json_value_to_cql_value(&key_json, key_type)?;
                    let val = json_value_to_cql_value(v, val_type)?;
                    Ok((key, val))
                })
                .collect::<Result<Vec<_>, Error>>()?;
            Ok(Value::Map(entries))
        }

        // Type mismatch
        _ => Err(Error::InvalidInput(format!(
            "Cannot convert JSON {} to CQL type {:?}",
            json_type_name(json_val),
            target_type
        ))),
    }
}

/// Return a human-readable name for a `serde_json::Value` variant.
#[cfg(feature = "write-support")]
fn json_type_name(val: &serde_json::Value) -> &'static str {
    match val {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

/// Convert a parsed `CqlUpdate` AST node into a `Mutation` using schema information.
///
/// # Errors
///
/// Returns `Error::InvalidInput` when:
/// - The UPDATE targets a different table than the schema
/// - A required partition key column is missing from the WHERE clause
/// - A SET assignment uses a compound operator (only `=` is supported)
/// - A column in SET is unknown in the schema
/// - A value cannot be coerced to its schema type
/// - An `IF condition` is specified (not supported by the local write engine)
#[cfg(feature = "write-support")]
pub(crate) fn update_to_mutation(
    update: &CqlUpdate,
    schema: &TableSchema,
) -> Result<Mutation, Error> {
    if update.if_condition.is_some() {
        return Err(Error::InvalidInput(
            "IF conditions are not supported by the local write engine".to_string(),
        ));
    }
    validate_table(&update.table, schema)?;

    // Extract key bindings from WHERE clause
    let bindings = extract_where_bindings(&update.where_clause)?;
    let keys = resolve_key_bindings(&bindings, schema)?;

    let partition_key = PartitionKey::new(keys.partition);
    let clustering_key = if keys.clustering.is_empty() {
        None
    } else {
        Some(ClusteringKey::new(keys.clustering))
    };

    // Convert SET assignments to CellOperation::Write
    let mut operations: Vec<CellOperation> = Vec::with_capacity(update.assignments.len());
    for assignment in &update.assignments {
        match &assignment.operator {
            CqlAssignmentOperator::Assign => {
                let col_name = assignment.column.name.to_lowercase();
                let column = schema
                    .get_column(&col_name)
                    .ok_or_else(|| Error::InvalidInput(format!("Unknown column '{}'", col_name)))?;
                let cql_type = CqlType::parse(&column.data_type)?;
                let value = expression_to_value(&assignment.value, &cql_type)?;
                operations.push(CellOperation::Write {
                    column: column.name.clone(),
                    value,
                });
            }
            CqlAssignmentOperator::AddAssign
            | CqlAssignmentOperator::ListAppend
            | CqlAssignmentOperator::SetAdd
            | CqlAssignmentOperator::ListPrepend => {
                // Last-write-wins SSTable semantics: write RHS as full cell value.
                // CQLite does not perform read-modify-write; the caller is expected
                // to supply the complete replacement collection.
                let col_name = assignment.column.name.to_lowercase();
                let column = schema
                    .get_column(&col_name)
                    .ok_or_else(|| Error::InvalidInput(format!("Unknown column '{}'", col_name)))?;
                let cql_type = CqlType::parse(&column.data_type)?;
                let value = expression_to_value(&assignment.value, &cql_type)?;
                operations.push(CellOperation::Write {
                    column: column.name.clone(),
                    value,
                });
            }
            CqlAssignmentOperator::SubAssign | CqlAssignmentOperator::SetRemove => {
                return Err(Error::InvalidInput(
                    "SET col -= value is not supported; CQLite uses last-write-wins semantics. \
                     Use SET col = <full_value> instead."
                        .to_string(),
                ));
            }
            CqlAssignmentOperator::MapUpdate(_) => {
                return Err(Error::InvalidInput(
                    "SET col[key] = value is not supported; use SET col = {full_map} instead."
                        .to_string(),
                ));
            }
        }
    }

    let timestamp_micros = extract_timestamp(&update.using)?;
    let ttl_seconds = extract_ttl(&update.using)?;

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

/// Convert a parsed `CqlDelete` AST node into a `Mutation` using schema information.
///
/// If `delete.columns` is empty and there are no range predicates, the result is a row
/// tombstone (`CellOperation::DeleteRow`). If range predicates are present (e.g.
/// `ck > 'a' AND ck < 'z'`), the mutation will carry `range_tombstones` instead.
/// Otherwise, each named column produces a `CellOperation::Delete`.
///
/// # Errors
///
/// Returns `Error::InvalidInput` when:
/// - The DELETE targets a different table than the schema
/// - A required partition key column is missing from the WHERE clause
/// - A range predicate targets a non-clustering column
/// - A value cannot be coerced to its schema type
/// - An `IF condition` is specified (not supported by the local write engine)
#[cfg(feature = "write-support")]
pub(crate) fn delete_to_mutation(
    delete: &CqlDelete,
    schema: &TableSchema,
) -> Result<Mutation, Error> {
    if delete.if_condition.is_some() {
        return Err(Error::InvalidInput(
            "IF conditions are not supported by the local write engine".to_string(),
        ));
    }
    validate_table(&delete.table, schema)?;

    // Extract key bindings and range predicates from WHERE clause
    let predicates = extract_delete_predicates(&delete.where_clause)?;
    let keys = resolve_key_bindings(&predicates.equality_bindings, schema)?;

    let partition_key = PartitionKey::new(keys.partition);
    let clustering_key = if keys.clustering.is_empty() {
        None
    } else {
        Some(ClusteringKey::new(keys.clustering))
    };

    // DELETE does not use TTL
    let timestamp_micros = extract_timestamp(&delete.using)?;

    // Determine if this is a partition-level delete:
    // No clustering keys specified AND no specific columns AND table has clustering columns
    // AND no range predicates → generate a partition tombstone instead of a row tombstone
    let has_clustering_columns = !schema.clustering_keys.is_empty();
    let is_partition_delete = clustering_key.is_none()
        && delete.columns.is_empty()
        && has_clustering_columns
        && predicates.range_predicates.is_empty();

    // Build operations: partition tombstone, row delete, range tombstone, or per-column deletes
    let operations: Vec<CellOperation> = if is_partition_delete {
        // Partition tombstone: no cell operations needed
        vec![]
    } else if delete.columns.is_empty() {
        if predicates.range_predicates.is_empty() {
            vec![CellOperation::DeleteRow]
        } else {
            // Range tombstone — no row-level operation required
            vec![]
        }
    } else {
        delete
            .columns
            .iter()
            .map(|col_id| CellOperation::Delete {
                column: col_id.name.clone(),
                // CQL DELETE has no per-cell surfaced LDT; the writer derives it
                // from the mutation timestamp (historical behavior, #921 finding 2).
                local_deletion_time: None,
            })
            .collect()
    };

    // Build partition tombstone if this is a partition-level delete
    let partition_tombstone = if is_partition_delete {
        Some(PartitionTombstone {
            deletion_time: timestamp_micros,
            local_deletion_time: wall_clock_local_deletion_time(),
        })
    } else {
        None
    };

    // Build range tombstones from range predicates (if any)
    let range_tombstones = if predicates.range_predicates.is_empty() {
        vec![]
    } else {
        build_range_tombstones(&predicates.range_predicates, schema, timestamp_micros)?
    };

    // Row/cell tombstones (DeleteRow / Delete) must carry their localDeletionTime
    // from the wall clock, consistent with partition and range tombstones above.
    // Otherwise the writer would derive it from `USING TIMESTAMP`, which is a
    // logical clock and breaks gc_grace semantics for caller-supplied timestamps.
    let has_row_or_cell_tombstone = operations
        .iter()
        .any(|op| matches!(op, CellOperation::DeleteRow | CellOperation::Delete { .. }));

    let table_id = TableId::new(schema.keyspace.clone(), schema.table.clone());
    let mut mutation = Mutation::new(
        table_id,
        partition_key,
        clustering_key,
        operations,
        timestamp_micros,
        None, // DELETE never has TTL
    );
    if has_row_or_cell_tombstone {
        mutation.local_deletion_time = Some(wall_clock_local_deletion_time());
    }
    mutation.partition_tombstone = partition_tombstone;
    mutation.range_tombstones = range_tombstones;
    Ok(mutation)
}

/// Convert a CQL mutation statement string to a Mutation struct.
///
/// Supports INSERT, UPDATE, and DELETE statements.
/// The statement is parsed using the existing CQL parser, then converted
/// to a Mutation using the provided schema for type resolution.
#[cfg(feature = "write-support")]
pub(crate) fn convert_cql_to_mutation(
    statement: &str,
    schema: &TableSchema,
) -> Result<Mutation, Error> {
    let trimmed = statement.trim();

    if trimmed.len() >= 6 && trimmed.as_bytes()[..6].eq_ignore_ascii_case(b"INSERT") {
        let insert = crate::cql::mutation_parser::parse_insert_statement(trimmed)?;
        insert_to_mutation(&insert, schema)
    } else if trimmed.len() >= 6 && trimmed.as_bytes()[..6].eq_ignore_ascii_case(b"UPDATE") {
        let update = crate::cql::mutation_parser::parse_update_statement(trimmed)?;
        update_to_mutation(&update, schema)
    } else if trimmed.len() >= 6 && trimmed.as_bytes()[..6].eq_ignore_ascii_case(b"DELETE") {
        let delete = crate::cql::mutation_parser::parse_delete_statement(trimmed)?;
        delete_to_mutation(&delete, schema)
    } else {
        Err(Error::InvalidInput(format!(
            "Unsupported mutation statement. Expected INSERT, UPDATE, or DELETE: {}",
            &trimmed[..trimmed.len().min(50)]
        )))
    }
}

/// Convert a CQL statement string to one or more `Mutation` structs.
///
/// Supports INSERT, UPDATE, DELETE, and BATCH statements. BATCH statements
/// produce multiple mutations (one per inner statement). All other statements
/// produce a single-element vector.
#[cfg(feature = "write-support")]
pub(crate) fn convert_cql_to_mutations(
    statement: &str,
    schema: &TableSchema,
) -> Result<Vec<Mutation>, Error> {
    let trimmed = statement.trim();

    if trimmed.len() >= 5 && trimmed.as_bytes()[..5].eq_ignore_ascii_case(b"BEGIN") {
        let batch = crate::cql::mutation_parser::parse_batch_statement(trimmed)?;
        batch_to_mutations(&batch, schema)
    } else {
        let mutation = convert_cql_to_mutation(trimmed, schema)?;
        Ok(vec![mutation])
    }
}

/// Convert a parsed `CqlBatch` into a list of mutations.
///
/// If the batch has a `USING TIMESTAMP` clause and an inner statement does not
/// have its own, the batch timestamp is applied to the inner mutation.
#[cfg(feature = "write-support")]
fn batch_to_mutations(batch: &CqlBatch, schema: &TableSchema) -> Result<Vec<Mutation>, Error> {
    use crate::cql::ast::{CqlBatchStatement, CqlBatchType};

    if batch.batch_type == CqlBatchType::Counter {
        return Err(Error::InvalidInput(
            "COUNTER BATCH is not supported; CQLite uses last-write-wins semantics".to_string(),
        ));
    }

    let batch_timestamp = batch
        .using
        .as_ref()
        .and_then(|u| u.timestamp.as_ref())
        .and_then(|expr| {
            if let CqlExpression::Literal(CqlLiteral::Integer(ts)) = expr {
                Some(*ts)
            } else {
                None
            }
        });

    let mut mutations = Vec::with_capacity(batch.statements.len());
    for stmt in &batch.statements {
        let mut mutation = match stmt {
            CqlBatchStatement::Insert(ins) => insert_to_mutation(ins, schema)?,
            CqlBatchStatement::Update(upd) => update_to_mutation(upd, schema)?,
            CqlBatchStatement::Delete(del) => delete_to_mutation(del, schema)?,
        };

        // Apply batch timestamp if inner statement didn't specify its own
        if let Some(batch_ts) = batch_timestamp {
            let inner_has_timestamp = match stmt {
                CqlBatchStatement::Insert(ins) => ins
                    .using
                    .as_ref()
                    .and_then(|u| u.timestamp.as_ref())
                    .is_some(),
                CqlBatchStatement::Update(upd) => upd
                    .using
                    .as_ref()
                    .and_then(|u| u.timestamp.as_ref())
                    .is_some(),
                CqlBatchStatement::Delete(del) => del
                    .using
                    .as_ref()
                    .and_then(|u| u.timestamp.as_ref())
                    .is_some(),
            };
            if !inner_has_timestamp {
                mutation.timestamp_micros = batch_ts;
            }
        }

        mutations.push(mutation);
    }

    Ok(mutations)
}

/// Extract `(column_name, value_expression)` pairs from a WHERE clause expression.
///
/// Supports AND-chained equality predicates: `col1 = val1 AND col2 = val2 ...`.
///
/// # Errors
///
/// Returns `Error::InvalidInput` for non-equality predicates or unexpected expression forms.
#[cfg(feature = "write-support")]
fn extract_where_bindings(expr: &CqlExpression) -> Result<Vec<(String, CqlExpression)>, Error> {
    let mut bindings = Vec::new();
    collect_equality_bindings(expr, &mut bindings)?;
    Ok(bindings)
}

/// Recursively collect `column = value` bindings from an AND-chained expression tree.
#[cfg(feature = "write-support")]
fn collect_equality_bindings(
    expr: &CqlExpression,
    bindings: &mut Vec<(String, CqlExpression)>,
) -> Result<(), Error> {
    match expr {
        CqlExpression::Binary {
            left,
            operator: CqlBinaryOperator::And,
            right,
        } => {
            collect_equality_bindings(left, bindings)?;
            collect_equality_bindings(right, bindings)?;
        }
        CqlExpression::Binary {
            left,
            operator: CqlBinaryOperator::Eq,
            right,
        } => match left.as_ref() {
            CqlExpression::Column(col_id) => {
                bindings.push((col_id.name.to_lowercase(), (**right).clone()));
            }
            _ => {
                return Err(Error::InvalidInput(
                        "WHERE clause equality predicate must have a column reference on the left-hand side".to_string(),
                    ));
            }
        },
        _ => {
            return Err(Error::InvalidInput(
                "WHERE clause must consist of equality predicates joined with AND".to_string(),
            ));
        }
    }
    Ok(())
}

/// A single range predicate extracted from a DELETE WHERE clause (e.g. `ck > 'a'`).
#[cfg(feature = "write-support")]
struct RangePredicate {
    column: String,
    operator: CqlBinaryOperator,
    value: CqlExpression,
}

/// Equality and range predicates extracted from a DELETE WHERE clause.
#[cfg(feature = "write-support")]
struct DeletePredicates {
    equality_bindings: Vec<(String, CqlExpression)>,
    range_predicates: Vec<RangePredicate>,
}

/// Extract equality and range predicates from a DELETE WHERE clause expression.
///
/// Supports AND-chained equality predicates (`col = val`) and range predicates
/// (`col > val`, `col >= val`, `col < val`, `col <= val`).
#[cfg(feature = "write-support")]
fn extract_delete_predicates(expr: &CqlExpression) -> Result<DeletePredicates, Error> {
    let mut result = DeletePredicates {
        equality_bindings: Vec::new(),
        range_predicates: Vec::new(),
    };
    collect_delete_predicates(expr, &mut result)?;
    Ok(result)
}

/// Recursively collect equality and range predicates from an AND-chained expression tree.
#[cfg(feature = "write-support")]
fn collect_delete_predicates(
    expr: &CqlExpression,
    result: &mut DeletePredicates,
) -> Result<(), Error> {
    match expr {
        CqlExpression::Binary {
            left,
            operator: CqlBinaryOperator::And,
            right,
        } => {
            collect_delete_predicates(left, result)?;
            collect_delete_predicates(right, result)?;
        }
        CqlExpression::Binary {
            left,
            operator: CqlBinaryOperator::Eq,
            right,
        } => match left.as_ref() {
            CqlExpression::Column(col_id) => {
                result
                    .equality_bindings
                    .push((col_id.name.to_lowercase(), (**right).clone()));
            }
            _ => {
                return Err(Error::InvalidInput(
                    "WHERE clause predicate must have a column reference on the left-hand side"
                        .to_string(),
                ));
            }
        },
        CqlExpression::Binary {
            left,
            operator,
            right,
        } if matches!(
            operator,
            CqlBinaryOperator::Lt
                | CqlBinaryOperator::Le
                | CqlBinaryOperator::Gt
                | CqlBinaryOperator::Ge
        ) =>
        {
            match left.as_ref() {
                CqlExpression::Column(col_id) => {
                    result.range_predicates.push(RangePredicate {
                        column: col_id.name.to_lowercase(),
                        operator: operator.clone(),
                        value: (**right).clone(),
                    });
                }
                _ => {
                    return Err(Error::InvalidInput(
                        "WHERE clause predicate must have a column reference on the left-hand side"
                            .to_string(),
                    ));
                }
            }
        }
        _ => {
            return Err(Error::InvalidInput(
                "DELETE WHERE clause must consist of equality or range predicates joined with AND"
                    .to_string(),
            ));
        }
    }
    Ok(())
}

/// Build `RangeTombstone` values from a set of range predicates.
///
/// **Limitation**: This currently only produces correct results for tables with
/// a single clustering key column. For multi-column clustering keys, each bound
/// is constructed from a single column's value, which may not produce the
/// intended composite range.
///
/// All range predicates must reference clustering key columns. The function
/// produces a single `RangeTombstone` covering the intersection of all bounds.
#[cfg(feature = "write-support")]
fn build_range_tombstones(
    range_predicates: &[RangePredicate],
    schema: &TableSchema,
    timestamp_micros: i64,
) -> Result<Vec<RangeTombstone>, Error> {
    let ordered_ck = schema.ordered_clustering_keys();
    let ck_names: Vec<String> = ordered_ck.iter().map(|c| c.name.to_lowercase()).collect();

    // Validate all range predicates reference clustering columns
    for pred in range_predicates {
        if !ck_names.contains(&pred.column) {
            return Err(Error::InvalidInput(format!(
                "Range predicate on non-clustering column '{}'; only clustering key columns support range deletions",
                pred.column
            )));
        }
    }

    // Build bounds: find lower and upper for each clustering column
    let mut lower_bound: Option<ClusteringBound> = None;
    let mut upper_bound: Option<ClusteringBound> = None;

    for pred in range_predicates {
        let ck_col = ordered_ck
            .iter()
            .find(|c| c.name.to_lowercase() == pred.column)
            .ok_or_else(|| {
                Error::InvalidInput(format!(
                    "Internal: clustering column '{}' missing after validation",
                    pred.column
                ))
            })?;
        let cql_type = CqlType::parse(&ck_col.data_type)?;
        let value = expression_to_value(&pred.value, &cql_type)?;
        let ck = ClusteringKey::new(vec![(ck_col.name.clone(), value)]);

        match pred.operator {
            CqlBinaryOperator::Gt => {
                lower_bound = Some(ClusteringBound::Exclusive(ck));
            }
            CqlBinaryOperator::Ge => {
                lower_bound = Some(ClusteringBound::Inclusive(ck));
            }
            CqlBinaryOperator::Lt => {
                upper_bound = Some(ClusteringBound::Exclusive(ck));
            }
            CqlBinaryOperator::Le => {
                upper_bound = Some(ClusteringBound::Inclusive(ck));
            }
            _ => unreachable!("only Lt/Le/Gt/Ge reach build_range_tombstones"),
        }
    }

    Ok(vec![RangeTombstone {
        start: lower_bound.unwrap_or(ClusteringBound::Bottom),
        end: upper_bound.unwrap_or(ClusteringBound::Top),
        deletion_time: timestamp_micros,
        local_deletion_time: wall_clock_local_deletion_time(),
    }])
}

/// Resolved partition key and clustering key columns from a WHERE clause.
#[cfg(feature = "write-support")]
struct ResolvedKeys {
    partition: Vec<(String, Value)>,
    clustering: Vec<(String, Value)>,
}

/// Separate WHERE clause bindings into partition key values and clustering key values.
///
/// Partition key columns are required; an error is returned if any are missing.
/// Clustering key columns are optional (partial WHERE clauses are valid for DELETE).
///
/// # Errors
///
/// Returns `Error::InvalidInput` when a partition key column is missing from the bindings.
#[cfg(feature = "write-support")]
fn resolve_key_bindings(
    bindings: &[(String, CqlExpression)],
    schema: &TableSchema,
) -> Result<ResolvedKeys, Error> {
    let ordered_pk = schema.ordered_partition_keys();
    let ordered_ck = schema.ordered_clustering_keys();

    // Resolve partition key values (required)
    let mut pk_columns: Vec<(String, Value)> = Vec::with_capacity(ordered_pk.len());
    for pk_col in &ordered_pk {
        let col_name_lc = pk_col.name.to_lowercase();
        let (_, expr) = bindings
            .iter()
            .find(|(name, _)| *name == col_name_lc)
            .ok_or_else(|| {
                Error::InvalidInput(format!(
                    "Partition key column '{}' is missing from WHERE clause",
                    pk_col.name
                ))
            })?;
        let cql_type = CqlType::parse(&pk_col.data_type)?;
        let value = expression_to_value(expr, &cql_type)?;
        pk_columns.push((pk_col.name.clone(), value));
    }

    // Resolve clustering key values (optional)
    let mut ck_columns: Vec<(String, Value)> = Vec::with_capacity(ordered_ck.len());
    for ck_col in &ordered_ck {
        let col_name_lc = ck_col.name.to_lowercase();
        if let Some((_, expr)) = bindings.iter().find(|(name, _)| *name == col_name_lc) {
            let cql_type = CqlType::parse(&ck_col.data_type)?;
            let value = expression_to_value(expr, &cql_type)?;
            ck_columns.push((ck_col.name.clone(), value));
        }
    }

    Ok(ResolvedKeys {
        partition: pk_columns,
        clustering: ck_columns,
    })
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
                        Error::InvalidInput(format!("Integer {} cannot be negated (overflow)", i))
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

/// Validate that the statement's table reference matches the provided schema.
///
/// # Errors
///
/// Returns `Error::InvalidInput` if the table name or keyspace does not match.
#[cfg(feature = "write-support")]
fn validate_table(table: &CqlTable, schema: &TableSchema) -> Result<(), Error> {
    if let Some(ks) = &table.keyspace {
        if !ks.name.eq_ignore_ascii_case(&schema.keyspace) {
            return Err(Error::InvalidInput(format!(
                "Statement targets keyspace '{}' but schema is for '{}'",
                ks.name, schema.keyspace
            )));
        }
    }
    if !table.name.name.eq_ignore_ascii_case(&schema.table) {
        return Err(Error::InvalidInput(format!(
            "Statement targets table '{}' but schema is for '{}'",
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
                        Error::InvalidInput(format!("TTL value {} is out of range for u32", ttl))
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
pub(crate) fn literal_to_value(
    literal: &CqlLiteral,
    target_type: &CqlType,
) -> Result<Value, Error> {
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

/// Parse a UUID string in standard format (8-4-4-4-12) into a 16-byte array.
#[cfg(feature = "write-support")]
fn parse_uuid(s: &str) -> Result<Value, Error> {
    let b = s.as_bytes();
    if b.len() != 36 || b[8] != b'-' || b[13] != b'-' || b[18] != b'-' || b[23] != b'-' {
        return Err(Error::Parse(format!(
            "invalid UUID string (expected 8-4-4-4-12 format): {:?}",
            s
        )));
    }
    let mut bytes = [0u8; 16];
    let segments: [&[u8]; 5] = [&b[0..8], &b[9..13], &b[14..18], &b[19..23], &b[24..36]];
    let mut out = 0;
    for seg in segments {
        for pair in seg.chunks(2) {
            bytes[out] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
            out += 1;
        }
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
    use crate::cql::ast::{
        CqlAssignment, CqlAssignmentOperator, CqlBinaryOperator, CqlCollectionLiteral, CqlDelete,
        CqlExpression, CqlIdentifier, CqlInsert, CqlInsertValues, CqlLiteral, CqlTable, CqlUpdate,
        CqlUsing,
    };
    use crate::schema::CqlType;
    use crate::storage::write_engine::mutation::{CellOperation, ClusteringBound};
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
            dropped_columns: HashMap::new(),
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
        assert!(matches!(
            mutation.partition_key.columns[0].1,
            Value::Uuid(_)
        ));

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
            columns: vec![CqlIdentifier::new("id"), CqlIdentifier::new("ts")],
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

    fn simple_json_schema() -> crate::schema::TableSchema {
        crate::schema::TableSchema {
            keyspace: "test_ks".into(),
            table: "test_tbl".into(),
            partition_keys: vec![crate::schema::KeyColumn {
                name: "id".into(),
                data_type: "int".into(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![
                crate::schema::Column {
                    name: "id".into(),
                    data_type: "int".into(),
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
                    name: "value".into(),
                    data_type: "int".into(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                crate::schema::Column {
                    name: "flag".into(),
                    data_type: "boolean".into(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                crate::schema::Column {
                    name: "tags".into(),
                    data_type: "list<text>".into(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }

    #[test]
    fn test_json_insert_basic() {
        let schema = simple_json_schema();
        let insert = CqlInsert {
            table: CqlTable::new("test_tbl"),
            columns: vec![],
            values: CqlInsertValues::Json(r#"{"id": 1, "name": "Alice", "value": 42}"#.to_string()),
            if_not_exists: false,
            using: None,
        };
        let mutation = insert_to_mutation(&insert, &schema).unwrap();
        assert_eq!(mutation.partition_key.columns[0].1, Value::Integer(1));
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
        assert_eq!(op_map.get("value"), Some(&Value::Integer(42)));
    }

    #[test]
    fn test_json_insert_null_skipped() {
        let schema = simple_json_schema();
        let insert = CqlInsert {
            table: CqlTable::new("test_tbl"),
            columns: vec![],
            values: CqlInsertValues::Json(r#"{"id": 1, "name": null, "value": 7}"#.to_string()),
            if_not_exists: false,
            using: None,
        };
        let mutation = insert_to_mutation(&insert, &schema).unwrap();
        let has_name = mutation
            .operations
            .iter()
            .any(|op| matches!(op, CellOperation::Write { column, .. } if column == "name"));
        assert!(!has_name, "null field should not produce Write");
    }

    #[test]
    fn test_json_insert_missing_pk_error() {
        let schema = simple_json_schema();
        let insert = CqlInsert {
            table: CqlTable::new("test_tbl"),
            columns: vec![],
            values: CqlInsertValues::Json(r#"{"name": "Bob"}"#.to_string()),
            if_not_exists: false,
            using: None,
        };
        let err = insert_to_mutation(&insert, &schema).unwrap_err();
        match err {
            Error::InvalidInput(msg) => assert!(msg.contains("id")),
            other => panic!("expected InvalidInput, got {:?}", other),
        }
    }

    #[test]
    fn test_json_insert_unknown_column_error() {
        let schema = simple_json_schema();
        let insert = CqlInsert {
            table: CqlTable::new("test_tbl"),
            columns: vec![],
            values: CqlInsertValues::Json(r#"{"id": 1, "nonexistent": "oops"}"#.to_string()),
            if_not_exists: false,
            using: None,
        };
        let err = insert_to_mutation(&insert, &schema).unwrap_err();
        match err {
            Error::InvalidInput(msg) => assert!(msg.contains("nonexistent")),
            other => panic!("expected InvalidInput, got {:?}", other),
        }
    }

    #[test]
    fn test_json_insert_invalid_json_error() {
        let schema = simple_json_schema();
        let insert = CqlInsert {
            table: CqlTable::new("test_tbl"),
            columns: vec![],
            values: CqlInsertValues::Json("{not valid json".to_string()),
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
    fn test_json_insert_non_object_error() {
        let schema = simple_json_schema();
        let insert = CqlInsert {
            table: CqlTable::new("test_tbl"),
            columns: vec![],
            values: CqlInsertValues::Json("[1, 2, 3]".to_string()),
            if_not_exists: false,
            using: None,
        };
        let err = insert_to_mutation(&insert, &schema).unwrap_err();
        match err {
            Error::InvalidInput(msg) => assert!(msg.contains("object")),
            other => panic!("expected InvalidInput, got {:?}", other),
        }
    }

    #[test]
    fn test_json_insert_case_insensitive() {
        let schema = simple_json_schema();
        let insert = CqlInsert {
            table: CqlTable::new("test_tbl"),
            columns: vec![],
            values: CqlInsertValues::Json(r#"{"ID": 5, "NAME": "Carol"}"#.to_string()),
            if_not_exists: false,
            using: None,
        };
        let mutation = insert_to_mutation(&insert, &schema).unwrap();
        assert_eq!(mutation.partition_key.columns[0].1, Value::Integer(5));
    }

    #[test]
    fn test_json_insert_with_timestamp() {
        let schema = simple_json_schema();
        let custom_ts: i64 = 1_700_000_000_000_000;
        let insert = CqlInsert {
            table: CqlTable::new("test_tbl"),
            columns: vec![],
            values: CqlInsertValues::Json(r#"{"id": 1}"#.to_string()),
            if_not_exists: false,
            using: Some(CqlUsing {
                timestamp: Some(CqlExpression::Literal(CqlLiteral::Integer(custom_ts))),
                ttl: None,
            }),
        };
        let mutation = insert_to_mutation(&insert, &schema).unwrap();
        assert_eq!(mutation.timestamp_micros, custom_ts);
    }

    #[test]
    fn test_json_insert_boolean() {
        let schema = simple_json_schema();
        let insert = CqlInsert {
            table: CqlTable::new("test_tbl"),
            columns: vec![],
            values: CqlInsertValues::Json(r#"{"id": 2, "flag": true}"#.to_string()),
            if_not_exists: false,
            using: None,
        };
        let mutation = insert_to_mutation(&insert, &schema).unwrap();
        let has_flag = mutation.operations.iter().any(|op| {
            matches!(op, CellOperation::Write { column, value }
                if column == "flag" && *value == Value::Boolean(true))
        });
        assert!(has_flag);
    }

    #[test]
    fn test_json_insert_collections() {
        let schema = simple_json_schema();
        let insert = CqlInsert {
            table: CqlTable::new("test_tbl"),
            columns: vec![],
            values: CqlInsertValues::Json(r#"{"id": 3, "tags": ["alpha", "beta"]}"#.to_string()),
            if_not_exists: false,
            using: None,
        };
        let mutation = insert_to_mutation(&insert, &schema).unwrap();
        let list_op = mutation.operations.iter().find_map(|op| {
            if let CellOperation::Write { column, value } = op {
                if column == "tags" {
                    return Some(value.clone());
                }
            }
            None
        });
        assert_eq!(
            list_op,
            Some(Value::List(vec![
                Value::Text("alpha".to_string()),
                Value::Text("beta".to_string()),
            ]))
        );
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

    // ── Collections (see also test_list_of_int, test_set_of_text, test_map_of_text_to_int) ──

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
            Value::Set(vec![Value::Text("a".into()), Value::Text("b".into()),])
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
        let lit = CqlLiteral::Collection(CqlCollectionLiteral::List(vec![CqlLiteral::Integer(1)]));
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
        let result = literal_to_value(&lit, &CqlType::Tuple(vec![CqlType::Int, CqlType::Text]));
        assert_eq!(
            result.unwrap(),
            Value::Tuple(vec![Value::Integer(1), Value::Text("hello".into()),])
        );
    }

    #[test]
    fn test_tuple_wrong_arity() {
        let lit = CqlLiteral::Tuple(vec![CqlLiteral::Integer(1)]);
        let result = literal_to_value(&lit, &CqlType::Tuple(vec![CqlType::Int, CqlType::Text]));
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
        let lit = CqlLiteral::Collection(CqlCollectionLiteral::List(vec![CqlLiteral::Integer(1)]));
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
        let outer_list = CqlLiteral::Collection(CqlCollectionLiteral::List(vec![inner_list]));
        let target = CqlType::List(Box::new(CqlType::Frozen(Box::new(CqlType::List(
            Box::new(CqlType::Int),
        )))));
        let result = literal_to_value(&outer_list, &target);
        assert!(result.is_ok());
    }

    // ── update_to_mutation tests ──────────────────────────────────────────────

    fn make_where_pk_and_ck() -> CqlExpression {
        // WHERE id = <uuid> AND ts = <timestamp>
        CqlExpression::Binary {
            left: Box::new(CqlExpression::Binary {
                left: Box::new(CqlExpression::Column(CqlIdentifier {
                    name: "id".into(),
                    quoted: false,
                })),
                operator: CqlBinaryOperator::Eq,
                right: Box::new(CqlExpression::Literal(CqlLiteral::Uuid(
                    "550e8400-e29b-41d4-a716-446655440000".into(),
                ))),
            }),
            operator: CqlBinaryOperator::And,
            right: Box::new(CqlExpression::Binary {
                left: Box::new(CqlExpression::Column(CqlIdentifier {
                    name: "ts".into(),
                    quoted: false,
                })),
                operator: CqlBinaryOperator::Eq,
                right: Box::new(CqlExpression::Literal(CqlLiteral::Integer(
                    1_704_067_200_000,
                ))),
            }),
        }
    }

    fn make_where_pk_only() -> CqlExpression {
        // WHERE id = <uuid>
        CqlExpression::Binary {
            left: Box::new(CqlExpression::Column(CqlIdentifier {
                name: "id".into(),
                quoted: false,
            })),
            operator: CqlBinaryOperator::Eq,
            right: Box::new(CqlExpression::Literal(CqlLiteral::Uuid(
                "550e8400-e29b-41d4-a716-446655440000".into(),
            ))),
        }
    }

    #[test]
    fn test_update_to_mutation() {
        let schema = test_schema();
        let update = CqlUpdate {
            table: CqlTable {
                keyspace: None,
                name: CqlIdentifier {
                    name: "test_tbl".into(),
                    quoted: false,
                },
            },
            using: None,
            assignments: vec![CqlAssignment {
                column: CqlIdentifier {
                    name: "name".into(),
                    quoted: false,
                },
                operator: CqlAssignmentOperator::Assign,
                value: CqlExpression::Literal(CqlLiteral::String("Updated".into())),
            }],
            where_clause: make_where_pk_and_ck(),
            if_condition: None,
        };

        let mutation = update_to_mutation(&update, &schema).unwrap();
        assert_eq!(mutation.partition_key.columns.len(), 1);
        assert!(mutation.clustering_key.is_some());
        assert_eq!(mutation.operations.len(), 1);
        match &mutation.operations[0] {
            CellOperation::Write { column, value } => {
                assert_eq!(column, "name");
                assert_eq!(*value, Value::Text("Updated".into()));
            }
            _ => panic!("expected Write operation"),
        }
    }

    #[test]
    fn test_update_add_assign_on_scalar_column() {
        // AddAssign on a scalar (int) column is treated as a plain Write under
        // last-write-wins semantics — the RHS value is stored directly.
        let schema = test_schema();
        let update = CqlUpdate {
            table: CqlTable {
                keyspace: None,
                name: CqlIdentifier {
                    name: "test_tbl".into(),
                    quoted: false,
                },
            },
            using: None,
            assignments: vec![CqlAssignment {
                column: CqlIdentifier {
                    name: "age".into(),
                    quoted: false,
                },
                operator: CqlAssignmentOperator::AddAssign,
                value: CqlExpression::Literal(CqlLiteral::Integer(1)),
            }],
            where_clause: make_where_pk_only(),
            if_condition: None,
        };

        let result = update_to_mutation(&update, &schema);
        assert!(
            result.is_ok(),
            "AddAssign on scalar should succeed: {:?}",
            result.err()
        );
        match &result.unwrap().operations[0] {
            CellOperation::Write { column, value } => {
                assert_eq!(column, "age");
                assert_eq!(*value, Value::Integer(1));
            }
            _ => panic!("expected Write operation"),
        }
    }

    #[test]
    fn test_update_missing_partition_key() {
        let schema = test_schema();
        // WHERE clause only provides clustering key, not partition key
        let update = CqlUpdate {
            table: CqlTable {
                keyspace: None,
                name: CqlIdentifier {
                    name: "test_tbl".into(),
                    quoted: false,
                },
            },
            using: None,
            assignments: vec![CqlAssignment {
                column: CqlIdentifier {
                    name: "name".into(),
                    quoted: false,
                },
                operator: CqlAssignmentOperator::Assign,
                value: CqlExpression::Literal(CqlLiteral::String("test".into())),
            }],
            where_clause: CqlExpression::Binary {
                left: Box::new(CqlExpression::Column(CqlIdentifier {
                    name: "ts".into(),
                    quoted: false,
                })),
                operator: CqlBinaryOperator::Eq,
                right: Box::new(CqlExpression::Literal(CqlLiteral::Integer(12345))),
            },
            if_condition: None,
        };

        assert!(update_to_mutation(&update, &schema).is_err());
    }

    // ── delete_to_mutation tests ──────────────────────────────────────────────

    #[test]
    fn test_delete_partition_tombstone_when_no_clustering_key() {
        // DELETE FROM t WHERE pk = X on a table with clustering columns
        // should produce a partition tombstone, not a row tombstone
        let schema = test_schema();
        let delete = CqlDelete {
            columns: vec![],
            table: CqlTable {
                keyspace: None,
                name: CqlIdentifier {
                    name: "test_tbl".into(),
                    quoted: false,
                },
            },
            using: None,
            where_clause: make_where_pk_only(),
            if_condition: None,
        };

        let mutation = delete_to_mutation(&delete, &schema).unwrap();
        assert!(
            mutation.operations.is_empty(),
            "Partition tombstone should have no cell operations"
        );
        assert!(
            mutation.partition_tombstone.is_some(),
            "Should produce a partition tombstone when deleting without clustering key"
        );
        assert!(mutation.clustering_key.is_none());
        assert!(mutation.ttl_seconds.is_none());
    }

    #[test]
    fn test_delete_row_tombstone_with_clustering_key() {
        // DELETE FROM t WHERE pk = X AND ck = Y → row tombstone (not partition tombstone)
        let schema = test_schema();
        let delete = CqlDelete {
            columns: vec![],
            table: CqlTable {
                keyspace: None,
                name: CqlIdentifier {
                    name: "test_tbl".into(),
                    quoted: false,
                },
            },
            using: None,
            where_clause: make_where_pk_and_ck(),
            if_condition: None,
        };

        let mutation = delete_to_mutation(&delete, &schema).unwrap();
        assert_eq!(mutation.operations.len(), 1);
        assert!(matches!(mutation.operations[0], CellOperation::DeleteRow));
        assert!(mutation.partition_tombstone.is_none());
        assert!(mutation.clustering_key.is_some());
    }

    #[test]
    fn test_delete_columns_to_mutation() {
        let schema = test_schema();
        let delete = CqlDelete {
            columns: vec![
                CqlIdentifier {
                    name: "name".into(),
                    quoted: false,
                },
                CqlIdentifier {
                    name: "age".into(),
                    quoted: false,
                },
            ],
            table: CqlTable {
                keyspace: None,
                name: CqlIdentifier {
                    name: "test_tbl".into(),
                    quoted: false,
                },
            },
            using: None,
            where_clause: make_where_pk_and_ck(),
            if_condition: None,
        };

        let mutation = delete_to_mutation(&delete, &schema).unwrap();
        assert_eq!(mutation.operations.len(), 2);
        assert!(
            matches!(&mutation.operations[0], CellOperation::Delete { column, .. } if column == "name")
        );
        assert!(
            matches!(&mutation.operations[1], CellOperation::Delete { column, .. } if column == "age")
        );
    }

    #[test]
    fn test_delete_row_has_clustering_key_when_provided() {
        let schema = test_schema();
        let delete = CqlDelete {
            columns: vec![],
            table: CqlTable {
                keyspace: None,
                name: CqlIdentifier {
                    name: "test_tbl".into(),
                    quoted: false,
                },
            },
            using: None,
            where_clause: make_where_pk_and_ck(),
            if_condition: None,
        };

        let mutation = delete_to_mutation(&delete, &schema).unwrap();
        assert!(mutation.clustering_key.is_some());
        let ck = mutation.clustering_key.unwrap();
        assert_eq!(ck.columns.len(), 1);
        assert_eq!(ck.columns[0].0, "ts");
        // With clustering key provided, should NOT be a partition tombstone
        assert!(mutation.partition_tombstone.is_none());
    }

    #[test]
    fn test_delete_partition_on_table_without_clustering_keys() {
        // DELETE FROM t WHERE pk = X on a table WITHOUT clustering columns
        // should produce a row tombstone (not a partition tombstone)
        use crate::schema::{Column, KeyColumn, TableSchema};
        let schema = TableSchema {
            keyspace: "test_ks".into(),
            table: "no_ck_tbl".into(),
            partition_keys: vec![KeyColumn {
                name: "id".into(),
                data_type: "int".into(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![
                Column {
                    name: "id".into(),
                    data_type: "int".into(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "val".into(),
                    data_type: "text".into(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: std::collections::HashMap::new(),
            dropped_columns: std::collections::HashMap::new(),
        };

        let delete = CqlDelete {
            columns: vec![],
            table: CqlTable {
                keyspace: None,
                name: CqlIdentifier {
                    name: "no_ck_tbl".into(),
                    quoted: false,
                },
            },
            using: None,
            where_clause: CqlExpression::Binary {
                left: Box::new(CqlExpression::Column(CqlIdentifier::new("id"))),
                operator: CqlBinaryOperator::Eq,
                right: Box::new(CqlExpression::Literal(CqlLiteral::Integer(1))),
            },
            if_condition: None,
        };

        let mutation = delete_to_mutation(&delete, &schema).unwrap();
        assert_eq!(mutation.operations.len(), 1);
        assert!(matches!(mutation.operations[0], CellOperation::DeleteRow));
        assert!(
            mutation.partition_tombstone.is_none(),
            "Table without clustering keys should use row tombstone, not partition tombstone"
        );
    }

    // ── extract_where_bindings / resolve_key_bindings tests ──────────────────

    #[test]
    fn test_where_bindings_single_eq() {
        let expr = CqlExpression::Binary {
            left: Box::new(CqlExpression::Column(CqlIdentifier::new("id"))),
            operator: CqlBinaryOperator::Eq,
            right: Box::new(CqlExpression::Literal(CqlLiteral::Integer(42))),
        };
        let bindings = extract_where_bindings(&expr).unwrap();
        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings[0].0, "id");
    }

    #[test]
    fn test_where_bindings_and_chain() {
        let bindings = extract_where_bindings(&make_where_pk_and_ck()).unwrap();
        assert_eq!(bindings.len(), 2);
        let names: Vec<_> = bindings.iter().map(|(n, _)| n.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"ts"));
    }

    #[test]
    fn test_where_bindings_non_eq_rejected() {
        let expr = CqlExpression::Binary {
            left: Box::new(CqlExpression::Column(CqlIdentifier::new("age"))),
            operator: CqlBinaryOperator::Gt,
            right: Box::new(CqlExpression::Literal(CqlLiteral::Integer(18))),
        };
        assert!(extract_where_bindings(&expr).is_err());
    }

    // ── convert_cql_to_mutation integration tests ─────────────────────────────

    #[test]
    fn test_convert_cql_insert_string() {
        let schema = test_schema();
        let sql = "INSERT INTO test_ks.test_tbl (id, ts, name) VALUES (550e8400-e29b-41d4-a716-446655440000, 1704067200000, 'Alice')";
        let result = convert_cql_to_mutation(sql, &schema);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
        let mutation = result.unwrap();
        assert_eq!(mutation.table.keyspace, "test_ks");
        assert_eq!(mutation.table.table, "test_tbl");
        assert_eq!(mutation.operations.len(), 1); // only 'name' is non-key
    }

    #[test]
    fn test_convert_cql_update_string() {
        let schema = test_schema();
        let sql = "UPDATE test_ks.test_tbl SET name = 'Bob' WHERE id = 550e8400-e29b-41d4-a716-446655440000 AND ts = 1704067200000";
        let result = convert_cql_to_mutation(sql, &schema);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
        let mutation = result.unwrap();
        assert_eq!(mutation.operations.len(), 1);
        match &mutation.operations[0] {
            CellOperation::Write { column, value } => {
                assert_eq!(column, "name");
                assert_eq!(*value, Value::Text("Bob".into()));
            }
            _ => panic!("expected Write"),
        }
    }

    #[test]
    fn test_convert_cql_delete_string_partition_tombstone() {
        // DELETE with only PK on a table with clustering columns → partition tombstone
        let schema = test_schema();
        let sql = "DELETE FROM test_ks.test_tbl WHERE id = 550e8400-e29b-41d4-a716-446655440000";
        let result = convert_cql_to_mutation(sql, &schema);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
        let mutation = result.unwrap();
        assert!(
            mutation.operations.is_empty(),
            "Partition tombstone should have no cell operations"
        );
        assert!(
            mutation.partition_tombstone.is_some(),
            "Should generate partition tombstone"
        );
    }

    #[test]
    fn test_convert_cql_delete_string_row_tombstone() {
        // DELETE with PK + CK → row tombstone
        let schema = test_schema();
        let sql = "DELETE FROM test_ks.test_tbl WHERE id = 550e8400-e29b-41d4-a716-446655440000 AND ts = 1704067200000";
        let result = convert_cql_to_mutation(sql, &schema);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
        let mutation = result.unwrap();
        assert_eq!(mutation.operations.len(), 1);
        assert!(matches!(mutation.operations[0], CellOperation::DeleteRow));
        assert!(mutation.partition_tombstone.is_none());
    }

    #[test]
    fn test_convert_unsupported_statement() {
        let schema = test_schema();
        let sql = "SELECT * FROM test_ks.test_tbl";
        let result = convert_cql_to_mutation(sql, &schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_convert_cql_insert_with_using_timestamp() {
        let schema = test_schema();
        let sql = "INSERT INTO test_ks.test_tbl (id, ts, name) VALUES (550e8400-e29b-41d4-a716-446655440000, 1704067200000, 'Alice') USING TIMESTAMP 1704067200000000";
        let result = convert_cql_to_mutation(sql, &schema);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
        assert_eq!(result.unwrap().timestamp_micros, 1704067200000000);
    }

    // ── Collection assignment operator tests ──────────────────────────────────

    fn test_collection_schema() -> crate::schema::TableSchema {
        crate::schema::TableSchema {
            keyspace: "test_ks".into(),
            table: "test_table".into(),
            partition_keys: vec![crate::schema::KeyColumn {
                name: "id".into(),
                data_type: "int".into(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![
                crate::schema::Column {
                    name: "id".into(),
                    data_type: "int".into(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                crate::schema::Column {
                    name: "list_col".into(),
                    data_type: "list<text>".into(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                crate::schema::Column {
                    name: "set_col".into(),
                    data_type: "set<int>".into(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                crate::schema::Column {
                    name: "map_col".into(),
                    data_type: "map<text,int>".into(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }

    fn make_where_pk_int(id: i64) -> CqlExpression {
        CqlExpression::Binary {
            left: Box::new(CqlExpression::Column(CqlIdentifier::new("id"))),
            operator: CqlBinaryOperator::Eq,
            right: Box::new(CqlExpression::Literal(CqlLiteral::Integer(id))),
        }
    }

    #[test]
    fn test_update_add_assign_list() {
        let schema = test_collection_schema();
        let sql = "UPDATE test_ks.test_table SET list_col += ['hello'] WHERE id = 1";
        let result = convert_cql_to_mutation(sql, &schema);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
        let mutation = result.unwrap();
        assert_eq!(mutation.operations.len(), 1);
        match &mutation.operations[0] {
            CellOperation::Write { column, value } => {
                assert_eq!(column, "list_col");
                assert_eq!(*value, Value::List(vec![Value::Text("hello".into())]));
            }
            _ => panic!("expected Write operation"),
        }
    }

    #[test]
    fn test_update_add_assign_set() {
        let schema = test_collection_schema();
        let sql = "UPDATE test_ks.test_table SET set_col += {42} WHERE id = 1";
        let result = convert_cql_to_mutation(sql, &schema);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
        let mutation = result.unwrap();
        match &mutation.operations[0] {
            CellOperation::Write { column, value } => {
                assert_eq!(column, "set_col");
                assert_eq!(*value, Value::Set(vec![Value::Integer(42)]));
            }
            _ => panic!("expected Write operation"),
        }
    }

    #[test]
    fn test_update_add_assign_map() {
        let schema = test_collection_schema();
        let sql = "UPDATE test_ks.test_table SET map_col += {'key': 1} WHERE id = 1";
        let result = convert_cql_to_mutation(sql, &schema);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
        let mutation = result.unwrap();
        match &mutation.operations[0] {
            CellOperation::Write { column, value } => {
                assert_eq!(column, "map_col");
                assert_eq!(
                    *value,
                    Value::Map(vec![(Value::Text("key".into()), Value::Integer(1))])
                );
            }
            _ => panic!("expected Write operation"),
        }
    }

    #[test]
    fn test_update_sub_assign_rejected() {
        let schema = test_collection_schema();
        let sql = "UPDATE test_ks.test_table SET set_col -= {42} WHERE id = 1";
        let result = convert_cql_to_mutation(sql, &schema);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("not supported"), "got: {}", msg);
    }

    #[test]
    fn test_update_map_update_rejected() {
        let schema = test_collection_schema();
        let update = CqlUpdate {
            table: CqlTable::with_keyspace("test_ks", "test_table"),
            using: None,
            assignments: vec![CqlAssignment {
                column: CqlIdentifier::new("map_col"),
                operator: CqlAssignmentOperator::MapUpdate(CqlExpression::Literal(
                    CqlLiteral::String("some_key".into()),
                )),
                value: CqlExpression::Literal(CqlLiteral::Integer(99)),
            }],
            where_clause: make_where_pk_int(1),
            if_condition: None,
        };
        let result = update_to_mutation(&update, &schema);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("not supported"), "got: {}", msg);
    }

    #[test]
    fn test_update_list_prepend() {
        let schema = test_collection_schema();
        let update = CqlUpdate {
            table: CqlTable::with_keyspace("test_ks", "test_table"),
            using: None,
            assignments: vec![CqlAssignment {
                column: CqlIdentifier::new("list_col"),
                operator: CqlAssignmentOperator::ListPrepend,
                value: CqlExpression::Literal(CqlLiteral::Collection(CqlCollectionLiteral::List(
                    vec![CqlLiteral::String("first".into())],
                ))),
            }],
            where_clause: make_where_pk_int(1),
            if_condition: None,
        };
        let result = update_to_mutation(&update, &schema);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
        match &result.unwrap().operations[0] {
            CellOperation::Write { column, value } => {
                assert_eq!(column, "list_col");
                assert_eq!(*value, Value::List(vec![Value::Text("first".into())]));
            }
            _ => panic!("expected Write operation"),
        }
    }

    #[test]
    fn test_update_assign_still_works() {
        let schema = test_collection_schema();
        let sql = "UPDATE test_ks.test_table SET list_col = ['a', 'b'] WHERE id = 1";
        let result = convert_cql_to_mutation(sql, &schema);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
        match &result.unwrap().operations[0] {
            CellOperation::Write { column, value } => {
                assert_eq!(column, "list_col");
                assert_eq!(
                    *value,
                    Value::List(vec![Value::Text("a".into()), Value::Text("b".into())])
                );
            }
            _ => panic!("expected Write operation"),
        }
    }

    // ── Range tombstone tests ─────────────────────────────────────────────────

    fn test_clustering_schema() -> crate::schema::TableSchema {
        crate::schema::TableSchema {
            keyspace: "test_ks".into(),
            table: "test_table".into(),
            partition_keys: vec![crate::schema::KeyColumn {
                name: "id".into(),
                data_type: "int".into(),
                position: 0,
            }],
            clustering_keys: vec![crate::schema::ClusteringColumn {
                name: "ck".into(),
                data_type: "text".into(),
                position: 0,
                order: crate::schema::ClusteringOrder::Asc,
            }],
            columns: vec![
                crate::schema::Column {
                    name: "id".into(),
                    data_type: "int".into(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                crate::schema::Column {
                    name: "ck".into(),
                    data_type: "text".into(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                crate::schema::Column {
                    name: "value".into(),
                    data_type: "text".into(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }

    #[test]
    fn test_delete_range_gt() {
        let schema = test_clustering_schema();
        let delete = CqlDelete {
            columns: vec![],
            table: CqlTable::with_keyspace("test_ks", "test_table"),
            using: None,
            where_clause: CqlExpression::Binary {
                left: Box::new(CqlExpression::Binary {
                    left: Box::new(CqlExpression::Column(CqlIdentifier::new("id"))),
                    operator: CqlBinaryOperator::Eq,
                    right: Box::new(CqlExpression::Literal(CqlLiteral::Integer(1))),
                }),
                operator: CqlBinaryOperator::And,
                right: Box::new(CqlExpression::Binary {
                    left: Box::new(CqlExpression::Column(CqlIdentifier::new("ck"))),
                    operator: CqlBinaryOperator::Gt,
                    right: Box::new(CqlExpression::Literal(CqlLiteral::String("a".into()))),
                }),
            },
            if_condition: None,
        };
        let mutation = delete_to_mutation(&delete, &schema).unwrap();
        assert_eq!(mutation.range_tombstones.len(), 1);
        assert!(mutation.operations.is_empty());
        let rt = &mutation.range_tombstones[0];
        assert!(matches!(&rt.start, ClusteringBound::Exclusive(_)));
        assert!(matches!(&rt.end, ClusteringBound::Top));
    }

    #[test]
    fn test_delete_range_between() {
        let schema = test_clustering_schema();
        let where_clause = CqlExpression::Binary {
            left: Box::new(CqlExpression::Binary {
                left: Box::new(CqlExpression::Binary {
                    left: Box::new(CqlExpression::Column(CqlIdentifier::new("id"))),
                    operator: CqlBinaryOperator::Eq,
                    right: Box::new(CqlExpression::Literal(CqlLiteral::Integer(1))),
                }),
                operator: CqlBinaryOperator::And,
                right: Box::new(CqlExpression::Binary {
                    left: Box::new(CqlExpression::Column(CqlIdentifier::new("ck"))),
                    operator: CqlBinaryOperator::Ge,
                    right: Box::new(CqlExpression::Literal(CqlLiteral::String("a".into()))),
                }),
            }),
            operator: CqlBinaryOperator::And,
            right: Box::new(CqlExpression::Binary {
                left: Box::new(CqlExpression::Column(CqlIdentifier::new("ck"))),
                operator: CqlBinaryOperator::Lt,
                right: Box::new(CqlExpression::Literal(CqlLiteral::String("z".into()))),
            }),
        };
        let delete = CqlDelete {
            columns: vec![],
            table: CqlTable::with_keyspace("test_ks", "test_table"),
            using: None,
            where_clause,
            if_condition: None,
        };
        let mutation = delete_to_mutation(&delete, &schema).unwrap();
        assert_eq!(mutation.range_tombstones.len(), 1);
        let rt = &mutation.range_tombstones[0];
        assert!(matches!(&rt.start, ClusteringBound::Inclusive(_)));
        assert!(matches!(&rt.end, ClusteringBound::Exclusive(_)));
    }

    #[test]
    fn test_delete_range_le() {
        let schema = test_clustering_schema();
        let where_clause = CqlExpression::Binary {
            left: Box::new(CqlExpression::Binary {
                left: Box::new(CqlExpression::Column(CqlIdentifier::new("id"))),
                operator: CqlBinaryOperator::Eq,
                right: Box::new(CqlExpression::Literal(CqlLiteral::Integer(1))),
            }),
            operator: CqlBinaryOperator::And,
            right: Box::new(CqlExpression::Binary {
                left: Box::new(CqlExpression::Column(CqlIdentifier::new("ck"))),
                operator: CqlBinaryOperator::Le,
                right: Box::new(CqlExpression::Literal(CqlLiteral::String("z".into()))),
            }),
        };
        let delete = CqlDelete {
            columns: vec![],
            table: CqlTable::with_keyspace("test_ks", "test_table"),
            using: None,
            where_clause,
            if_condition: None,
        };
        let mutation = delete_to_mutation(&delete, &schema).unwrap();
        assert_eq!(mutation.range_tombstones.len(), 1);
        let rt = &mutation.range_tombstones[0];
        assert!(matches!(&rt.start, ClusteringBound::Bottom));
        assert!(matches!(&rt.end, ClusteringBound::Inclusive(_)));
    }

    #[test]
    fn test_delete_range_on_partition_key_rejected() {
        let schema = test_clustering_schema();
        let delete = CqlDelete {
            columns: vec![],
            table: CqlTable::with_keyspace("test_ks", "test_table"),
            using: None,
            where_clause: CqlExpression::Binary {
                left: Box::new(CqlExpression::Column(CqlIdentifier::new("id"))),
                operator: CqlBinaryOperator::Gt,
                right: Box::new(CqlExpression::Literal(CqlLiteral::Integer(1))),
            },
            if_condition: None,
        };
        let err = delete_to_mutation(&delete, &schema).unwrap_err();
        match err {
            Error::InvalidInput(msg) => {
                assert!(
                    msg.contains("Partition key column") || msg.contains("non-clustering"),
                    "unexpected: {}",
                    msg
                );
            }
            other => panic!("expected InvalidInput, got {:?}", other),
        }
    }

    #[test]
    fn test_delete_equality_still_works() {
        let schema = test_clustering_schema();
        let where_clause = CqlExpression::Binary {
            left: Box::new(CqlExpression::Binary {
                left: Box::new(CqlExpression::Column(CqlIdentifier::new("id"))),
                operator: CqlBinaryOperator::Eq,
                right: Box::new(CqlExpression::Literal(CqlLiteral::Integer(1))),
            }),
            operator: CqlBinaryOperator::And,
            right: Box::new(CqlExpression::Binary {
                left: Box::new(CqlExpression::Column(CqlIdentifier::new("ck"))),
                operator: CqlBinaryOperator::Eq,
                right: Box::new(CqlExpression::Literal(CqlLiteral::String("hello".into()))),
            }),
        };
        let delete = CqlDelete {
            columns: vec![],
            table: CqlTable::with_keyspace("test_ks", "test_table"),
            using: None,
            where_clause,
            if_condition: None,
        };
        let mutation = delete_to_mutation(&delete, &schema).unwrap();
        assert_eq!(mutation.range_tombstones.len(), 0);
        assert_eq!(mutation.operations.len(), 1);
        assert!(matches!(mutation.operations[0], CellOperation::DeleteRow));
    }

    #[test]
    fn test_convert_cql_delete_range_string() {
        let schema = test_clustering_schema();
        let sql = "DELETE FROM test_ks.test_table WHERE id = 1 AND ck > 'a' AND ck < 'z'";
        let result = convert_cql_to_mutation(sql, &schema);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
        let mutation = result.unwrap();
        assert_eq!(mutation.range_tombstones.len(), 1);
        assert!(mutation.operations.is_empty());
    }

    // ── BATCH statement tests ─────────────────────────────────────────────────

    #[test]
    fn test_batch_basic_two_inserts() {
        let schema = simple_json_schema();
        let sql = "BEGIN BATCH \
            INSERT INTO test_ks.test_tbl (id, name) VALUES (1, 'Alice'); \
            INSERT INTO test_ks.test_tbl (id, name) VALUES (2, 'Bob'); \
            APPLY BATCH";
        let result = convert_cql_to_mutations(sql, &schema);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
        let mutations = result.unwrap();
        assert_eq!(mutations.len(), 2);
        assert_eq!(mutations[0].partition_key.columns[0].1, Value::Integer(1));
        assert_eq!(mutations[1].partition_key.columns[0].1, Value::Integer(2));
    }

    #[test]
    fn test_batch_mixed_statements() {
        let schema = simple_json_schema();
        let sql = "BEGIN BATCH \
            INSERT INTO test_ks.test_tbl (id, name) VALUES (1, 'Alice'); \
            UPDATE test_ks.test_tbl SET name = 'Updated' WHERE id = 1; \
            DELETE FROM test_ks.test_tbl WHERE id = 2; \
            APPLY BATCH";
        let result = convert_cql_to_mutations(sql, &schema);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
        let mutations = result.unwrap();
        assert_eq!(mutations.len(), 3);
    }

    #[test]
    fn test_batch_unlogged() {
        let schema = simple_json_schema();
        let sql = "BEGIN UNLOGGED BATCH \
            INSERT INTO test_ks.test_tbl (id, name) VALUES (1, 'Alice'); \
            APPLY BATCH";
        let result = convert_cql_to_mutations(sql, &schema);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
        assert_eq!(result.unwrap().len(), 1);
    }

    #[test]
    fn test_batch_with_timestamp() {
        let schema = simple_json_schema();
        let sql = "BEGIN BATCH USING TIMESTAMP 1704067200000000 \
            INSERT INTO test_ks.test_tbl (id, name) VALUES (1, 'Alice'); \
            INSERT INTO test_ks.test_tbl (id, name) VALUES (2, 'Bob'); \
            APPLY BATCH";
        let result = convert_cql_to_mutations(sql, &schema);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
        let mutations = result.unwrap();
        assert_eq!(mutations.len(), 2);
        assert_eq!(mutations[0].timestamp_micros, 1704067200000000);
        assert_eq!(mutations[1].timestamp_micros, 1704067200000000);
    }

    #[test]
    fn test_batch_inner_timestamp_override() {
        let schema = simple_json_schema();
        let sql = "BEGIN BATCH USING TIMESTAMP 1000000 \
            INSERT INTO test_ks.test_tbl (id, name) VALUES (1, 'Alice') USING TIMESTAMP 2000000; \
            INSERT INTO test_ks.test_tbl (id, name) VALUES (2, 'Bob'); \
            APPLY BATCH";
        let result = convert_cql_to_mutations(sql, &schema);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
        let mutations = result.unwrap();
        // First INSERT has its own timestamp — should be preserved
        assert_eq!(mutations[0].timestamp_micros, 2000000);
        // Second INSERT should use batch timestamp
        assert_eq!(mutations[1].timestamp_micros, 1000000);
    }

    #[test]
    fn test_batch_empty() {
        let schema = simple_json_schema();
        let sql = "BEGIN BATCH APPLY BATCH";
        let result = convert_cql_to_mutations(sql, &schema);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_batch_single_statement() {
        let schema = simple_json_schema();
        let sql = "BEGIN BATCH \
            INSERT INTO test_ks.test_tbl (id, name) VALUES (1, 'Alice'); \
            APPLY BATCH";
        let result = convert_cql_to_mutations(sql, &schema);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
        assert_eq!(result.unwrap().len(), 1);
    }

    #[test]
    fn test_convert_cql_to_mutations_non_batch() {
        // Non-batch statement should return single mutation
        let schema = simple_json_schema();
        let sql = "INSERT INTO test_ks.test_tbl (id, name) VALUES (1, 'Alice')";
        let result = convert_cql_to_mutations(sql, &schema);
        assert!(result.is_ok(), "Failed: {:?}", result.err());
        assert_eq!(result.unwrap().len(), 1);
    }

    // ── Finding 5: range tombstone local_deletion_time uses wall clock ────────

    #[test]
    fn test_range_tombstone_local_deletion_time_is_wall_clock() {
        // The range tombstone's local_deletion_time must reflect real wall-clock
        // time, NOT be derived from the logical CQL timestamp (timestamp_micros).
        // A logical timestamp such as 1_704_067_200_000_000 µs would produce
        // 1_704_067_200 seconds, which happens to be a plausible wall-clock value
        // (2024-01-01). Instead, we use a clearly unrealistic logical timestamp
        // (year ~33000) so that any derivation from it would be obviously wrong.
        let schema = test_clustering_schema();
        let far_future_timestamp_micros: i64 = 1_000_000_000_000_000_000_i64; // ~year 33658

        let delete = CqlDelete {
            columns: vec![],
            table: CqlTable::with_keyspace("test_ks", "test_table"),
            using: Some(CqlUsing {
                timestamp: Some(CqlExpression::Literal(CqlLiteral::Integer(
                    far_future_timestamp_micros,
                ))),
                ttl: None,
            }),
            where_clause: CqlExpression::Binary {
                left: Box::new(CqlExpression::Binary {
                    left: Box::new(CqlExpression::Column(CqlIdentifier::new("id"))),
                    operator: CqlBinaryOperator::Eq,
                    right: Box::new(CqlExpression::Literal(CqlLiteral::Integer(1))),
                }),
                operator: CqlBinaryOperator::And,
                right: Box::new(CqlExpression::Binary {
                    left: Box::new(CqlExpression::Column(CqlIdentifier::new("ck"))),
                    operator: CqlBinaryOperator::Gt,
                    right: Box::new(CqlExpression::Literal(CqlLiteral::String("a".into()))),
                }),
            },
            if_condition: None,
        };

        let mutation = delete_to_mutation(&delete, &schema).unwrap();
        assert_eq!(mutation.range_tombstones.len(), 1);
        let rt = &mutation.range_tombstones[0];

        // local_deletion_time must be within a few seconds of now (wall clock),
        // not a derivative of far_future_timestamp_micros.
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let ldt = rt.local_deletion_time as i64;
        assert!(
            (ldt - now_secs).abs() < 5,
            "local_deletion_time ({}) should be close to now ({}), not derived from logical timestamp",
            ldt,
            now_secs,
        );
    }

    fn now_secs_for_ldt() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
    }

    // Unrealistic far-future logical timestamp (~year 33658). Any LDT derived
    // from it would be obviously wrong vs. wall clock.
    const FAR_FUTURE_TS_MICROS: i64 = 1_000_000_000_000_000_000_i64;

    fn far_future_using() -> Option<CqlUsing> {
        Some(CqlUsing {
            timestamp: Some(CqlExpression::Literal(CqlLiteral::Integer(
                FAR_FUTURE_TS_MICROS,
            ))),
            ttl: None,
        })
    }

    #[test]
    fn test_row_tombstone_local_deletion_time_is_wall_clock() {
        // A row tombstone (DeleteRow) must carry a wall-clock localDeletionTime,
        // not one derived from USING TIMESTAMP.
        let schema = test_schema();
        let delete = CqlDelete {
            columns: vec![],
            table: CqlTable {
                keyspace: None,
                name: CqlIdentifier {
                    name: "test_tbl".into(),
                    quoted: false,
                },
            },
            using: far_future_using(),
            where_clause: make_where_pk_and_ck(),
            if_condition: None,
        };

        let mutation = delete_to_mutation(&delete, &schema).unwrap();
        assert!(matches!(mutation.operations[0], CellOperation::DeleteRow));
        let ldt = mutation
            .local_deletion_time
            .expect("row tombstone must set explicit local_deletion_time") as i64;
        assert!(
            (ldt - now_secs_for_ldt()).abs() < 5,
            "row tombstone local_deletion_time ({}) should be wall-clock now, not derived from USING TIMESTAMP",
            ldt,
        );
    }

    #[test]
    fn test_column_tombstone_local_deletion_time_is_wall_clock() {
        // Per-column deletes (Delete) must also carry a wall-clock LDT.
        let schema = test_schema();
        let delete = CqlDelete {
            columns: vec![CqlIdentifier {
                name: "name".into(),
                quoted: false,
            }],
            table: CqlTable {
                keyspace: None,
                name: CqlIdentifier {
                    name: "test_tbl".into(),
                    quoted: false,
                },
            },
            using: far_future_using(),
            where_clause: make_where_pk_and_ck(),
            if_condition: None,
        };

        let mutation = delete_to_mutation(&delete, &schema).unwrap();
        assert!(matches!(
            &mutation.operations[0],
            CellOperation::Delete { .. }
        ));
        let ldt = mutation
            .local_deletion_time
            .expect("column tombstone must set explicit local_deletion_time")
            as i64;
        assert!(
            (ldt - now_secs_for_ldt()).abs() < 5,
            "column tombstone local_deletion_time ({}) should be wall-clock now, not derived from USING TIMESTAMP",
            ldt,
        );
    }

    // ── Finding 6: IF NOT EXISTS / IF conditions return errors ────────────────

    #[test]
    fn test_insert_if_not_exists_returns_error() {
        let schema = test_schema();
        let insert = CqlInsert {
            table: CqlTable::new("test_tbl"),
            columns: vec![CqlIdentifier::new("id"), CqlIdentifier::new("ts")],
            values: CqlInsertValues::Values(vec![
                CqlExpression::Literal(CqlLiteral::Uuid(
                    "550e8400-e29b-41d4-a716-446655440000".to_string(),
                )),
                CqlExpression::Literal(CqlLiteral::Integer(1_000_000)),
            ]),
            if_not_exists: true,
            using: None,
        };

        let err = insert_to_mutation(&insert, &schema).unwrap_err();
        match err {
            Error::InvalidInput(msg) => {
                assert!(
                    msg.contains("IF NOT EXISTS"),
                    "expected message about IF NOT EXISTS, got: {}",
                    msg
                );
            }
            other => panic!("expected InvalidInput, got {:?}", other),
        }
    }

    #[test]
    fn test_update_if_condition_returns_error() {
        let schema = test_schema();
        let update = CqlUpdate {
            table: CqlTable::with_keyspace("test_ks", "test_tbl"),
            using: None,
            assignments: vec![CqlAssignment {
                column: CqlIdentifier::new("name"),
                operator: CqlAssignmentOperator::Assign,
                value: CqlExpression::Literal(CqlLiteral::String("New".into())),
            }],
            where_clause: make_where_pk_only(),
            if_condition: Some(CqlExpression::Literal(CqlLiteral::Boolean(true))),
        };

        let err = update_to_mutation(&update, &schema).unwrap_err();
        match err {
            Error::InvalidInput(msg) => {
                assert!(
                    msg.contains("IF conditions"),
                    "expected message about IF conditions, got: {}",
                    msg
                );
            }
            other => panic!("expected InvalidInput, got {:?}", other),
        }
    }

    #[test]
    fn test_delete_if_condition_returns_error() {
        let schema = test_schema();
        let delete = CqlDelete {
            columns: vec![],
            table: CqlTable::with_keyspace("test_ks", "test_tbl"),
            using: None,
            where_clause: make_where_pk_only(),
            if_condition: Some(CqlExpression::Literal(CqlLiteral::Boolean(true))),
        };

        let err = delete_to_mutation(&delete, &schema).unwrap_err();
        match err {
            Error::InvalidInput(msg) => {
                assert!(
                    msg.contains("IF conditions"),
                    "expected message about IF conditions, got: {}",
                    msg
                );
            }
            other => panic!("expected InvalidInput, got {:?}", other),
        }
    }
}
