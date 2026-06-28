//! Mutation builders for INSERT, UPDATE, DELETE, and BATCH statements.
//!
//! Each builder converts a parsed CQL AST node into a `Mutation` (or list of
//! mutations) using schema information to identify partition/clustering keys and
//! regular columns, delegating value coercion to `codec` and predicate/USING
//! handling to `delta_helpers`.

#[cfg(feature = "write-support")]
use super::codec::{expression_to_value, json_value_to_cql_value};
#[cfg(feature = "write-support")]
use super::delta_helpers::{
    build_range_tombstones, extract_delete_predicates, extract_timestamp, extract_ttl,
    extract_where_bindings, resolve_key_bindings, validate_table, wall_clock_local_deletion_time,
};
#[cfg(feature = "write-support")]
use crate::cql::ast::{
    CqlAssignmentOperator, CqlBatch, CqlDelete, CqlExpression, CqlInsert, CqlInsertValues,
    CqlLiteral, CqlTable, CqlUpdate, CqlUsing,
};
#[cfg(feature = "write-support")]
use crate::schema::{CqlType, TableSchema};
#[cfg(feature = "write-support")]
use crate::storage::write_engine::mutation::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, PartitionTombstone, TableId,
};
#[cfg(feature = "write-support")]
use crate::types::Value;
#[cfg(feature = "write-support")]
use crate::Error;

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

/// Convert a parsed `CqlBatch` into a list of mutations.
///
/// If the batch has a `USING TIMESTAMP` clause and an inner statement does not
/// have its own, the batch timestamp is applied to the inner mutation.
#[cfg(feature = "write-support")]
pub(crate) fn batch_to_mutations(
    batch: &CqlBatch,
    schema: &TableSchema,
) -> Result<Vec<Mutation>, Error> {
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

#[cfg(all(test, feature = "write-support"))]
mod tests {
    use super::*;
    use crate::cql::ast::{
        CqlAssignment, CqlAssignmentOperator, CqlBinaryOperator, CqlCollectionLiteral, CqlDelete,
        CqlExpression, CqlIdentifier, CqlInsert, CqlInsertValues, CqlLiteral, CqlTable, CqlUpdate,
        CqlUsing,
    };
    use crate::storage::write_engine::cql_to_mutation::convert_cql_to_mutation;
    use crate::storage::write_engine::mutation::{CellOperation, ClusteringBound};
    use crate::types::Value;
    use std::collections::HashMap;

    // ── Test schema helpers ────────────────────────────────────────────────────

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

    fn make_where_pk_int(id: i64) -> CqlExpression {
        CqlExpression::Binary {
            left: Box::new(CqlExpression::Column(CqlIdentifier::new("id"))),
            operator: CqlBinaryOperator::Eq,
            right: Box::new(CqlExpression::Literal(CqlLiteral::Integer(id))),
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

    // ── update_to_mutation tests ──────────────────────────────────────────────

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

    // ── Collection assignment operator tests ──────────────────────────────────

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

    // ── Range tombstone tests ─────────────────────────────────────────────────

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
}
