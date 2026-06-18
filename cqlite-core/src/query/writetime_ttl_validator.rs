//! Schema-aware validation for `WRITETIME()` and `TTL()` select functions.
//!
//! Cassandra imposes the following restrictions on these metadata-retrieval functions:
//!
//! - **Partition-key columns**: not allowed — the error message mirrors Cassandra's:
//!   `"Cannot use selection function writeTime on PRIMARY KEY part <col>"`
//! - **Clustering columns**: same restriction as partition keys.
//! - **Non-frozen collection columns** (`list<T>`, `set<T>`, `map<K,V>` that are *not*
//!   `frozen<...>`): not allowed — multi-cell semantics mean there is no single
//!   writetime/TTL for the whole column.  Frozen collections (single-cell) are OK.
//!   The error mirrors Cassandra's:
//!   `"Cannot use selection function writeTime on non-frozen <type>"`
//! - **Unknown columns**: any column not found in the schema is an error.
//!
//! # Executor TODO (#692)
//! This module is the **parser/planning layer only**.  Evaluation (actually reading
//! `writetime` / `ttl` from SSTable cell metadata) is deferred to issue #692.

use crate::{Error, Result};

/// A parsed `WRITETIME()` or `TTL()` call, as seen during planning.
///
/// The caller supplies a slice of these (one per such item in the SELECT list)
/// together with the schema columns.  Validation is stateless and re-entrant.
#[derive(Debug, Clone, PartialEq)]
pub enum WriteTimeTtlKind {
    WriteTime,
    Ttl,
}

impl WriteTimeTtlKind {
    /// Cassandra spells the function name in lower-case in its error messages.
    fn cassandra_fn_name(&self) -> &'static str {
        match self {
            WriteTimeTtlKind::WriteTime => "writeTime",
            WriteTimeTtlKind::Ttl => "ttl",
        }
    }
}

/// Minimal column descriptor needed for validation.
///
/// The validator works with this lightweight struct so it is not tightly
/// coupled to any particular schema representation.
#[derive(Debug, Clone)]
pub struct ColumnDescriptor {
    /// Column name (case-insensitive matching is applied by the validator)
    pub name: String,
    /// CQL type string (e.g. `"text"`, `"list<int>"`, `"frozen<set<text>>"`)
    pub type_str: String,
    /// True when this column is part of the partition key
    pub is_partition_key: bool,
    /// True when this column is part of the clustering key
    pub is_clustering_key: bool,
}

/// Validate a single `WRITETIME(col)` or `TTL(col)` call against the supplied
/// schema columns.
///
/// Returns `Ok(())` when the call is valid, `Err(...)` with a Cassandra-shaped
/// error message otherwise.
///
/// # Errors
///
/// - Unknown column: `Error::CqlParse` with column name.
/// - Partition-key column: `Error::CqlParse` mirroring Cassandra's message.
/// - Clustering column: `Error::CqlParse` mirroring Cassandra's message.
/// - Non-frozen collection column: `Error::CqlParse` mirroring Cassandra's message.
pub fn validate_writetime_ttl_call(
    kind: &WriteTimeTtlKind,
    column_name: &str,
    columns: &[ColumnDescriptor],
) -> Result<()> {
    // Case-insensitive lookup (CQL identifiers are case-insensitive when unquoted).
    let col = columns
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case(column_name))
        .ok_or_else(|| {
            Error::cql_parse(format!(
                "Undefined column name {} in selection clause",
                column_name
            ))
        })?;

    if col.is_partition_key || col.is_clustering_key {
        return Err(Error::cql_parse(format!(
            "Cannot use selection function {} on PRIMARY KEY part {}",
            kind.cassandra_fn_name(),
            col.name,
        )));
    }

    if is_non_frozen_collection(&col.type_str) {
        return Err(Error::cql_parse(format!(
            "Cannot use selection function {} on non-frozen {}",
            kind.cassandra_fn_name(),
            col.type_str,
        )));
    }

    Ok(())
}

/// Validate all `WRITETIME()`/`TTL()` calls in a SELECT list at once.
///
/// Returns the first error encountered, or `Ok(())` if all are valid.
pub fn validate_all_writetime_ttl_calls(
    calls: &[(WriteTimeTtlKind, String)],
    columns: &[ColumnDescriptor],
) -> Result<()> {
    for (kind, column_name) in calls {
        validate_writetime_ttl_call(kind, column_name, columns)?;
    }
    Ok(())
}

/// Build a `ColumnDescriptor` list from a `crate::schema::TableSchema`.
///
/// This bridge function keeps the validator decoupled from the schema crate.
pub fn descriptors_from_table_schema(schema: &crate::schema::TableSchema) -> Vec<ColumnDescriptor> {
    let pk_names: std::collections::HashSet<&str> = schema
        .partition_keys
        .iter()
        .map(|k| k.name.as_str())
        .collect();
    let ck_names: std::collections::HashSet<&str> = schema
        .clustering_keys
        .iter()
        .map(|k| k.name.as_str())
        .collect();

    schema
        .columns
        .iter()
        .map(|col| ColumnDescriptor {
            name: col.name.clone(),
            type_str: col.data_type.clone(),
            is_partition_key: pk_names.contains(col.name.as_str()),
            is_clustering_key: ck_names.contains(col.name.as_str()),
        })
        .collect()
}

/// Returns `true` when `type_str` describes a non-frozen collection.
///
/// `frozen<list<...>>` / `frozen<set<...>>` / `frozen<map<...>>` are single-cell
/// and are **allowed** with WRITETIME/TTL.  Plain `list<...>`, `set<...>`,
/// `map<...>` are multi-cell and are **not** allowed.
fn is_non_frozen_collection(type_str: &str) -> bool {
    let t = type_str.trim().to_lowercase();
    // A frozen wrapper at the top level means it is single-cell.
    if t.starts_with("frozen<") {
        return false;
    }
    t.starts_with("list<") || t.starts_with("set<") || t.starts_with("map<")
}

/// Convenience: extract `(WriteTimeTtlKind, column_name)` pairs from a parsed
/// `SelectStatement`'s select clause.  Returns an empty vec for `SELECT *`.
#[cfg(feature = "state_machine")]
pub fn extract_writetime_ttl_calls(
    stmt: &super::select_ast::SelectStatement,
) -> Vec<(WriteTimeTtlKind, String)> {
    use super::select_ast::{SelectClause, SelectExpression, WriteTimeTtlFunction};

    let exprs = match &stmt.select_clause {
        SelectClause::Columns(v) | SelectClause::Distinct(v) => v,
        SelectClause::All => return vec![],
    };

    exprs
        .iter()
        .filter_map(|expr| {
            let call = match expr {
                SelectExpression::WriteTimeTtl(c) => c,
                SelectExpression::Aliased(inner, _) => {
                    if let SelectExpression::WriteTimeTtl(c) = inner.as_ref() {
                        c
                    } else {
                        return None;
                    }
                }
                _ => return None,
            };
            let kind = match call.function {
                WriteTimeTtlFunction::WriteTime => WriteTimeTtlKind::WriteTime,
                WriteTimeTtlFunction::Ttl => WriteTimeTtlKind::Ttl,
            };
            Some((kind, call.column.clone()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, type_str: &str, is_pk: bool, is_ck: bool) -> ColumnDescriptor {
        ColumnDescriptor {
            name: name.to_string(),
            type_str: type_str.to_string(),
            is_partition_key: is_pk,
            is_clustering_key: is_ck,
        }
    }

    fn basic_columns() -> Vec<ColumnDescriptor> {
        vec![
            col("user_id", "uuid", true, false),
            col("bucket", "int", false, true),
            col("name", "text", false, false),
            col("scores", "list<int>", false, false),
            col("tags", "set<text>", false, false),
            col("meta", "map<text,text>", false, false),
            col("frozen_scores", "frozen<list<int>>", false, false),
            col("data", "blob", false, false),
        ]
    }

    // --- Happy-path tests ---

    #[test]
    fn test_writetime_on_regular_column_is_valid() {
        let cols = basic_columns();
        let result = validate_writetime_ttl_call(&WriteTimeTtlKind::WriteTime, "name", &cols);
        assert!(
            result.is_ok(),
            "WRITETIME on a plain text column should be valid"
        );
    }

    #[test]
    fn test_ttl_on_regular_column_is_valid() {
        let cols = basic_columns();
        let result = validate_writetime_ttl_call(&WriteTimeTtlKind::Ttl, "name", &cols);
        assert!(result.is_ok(), "TTL on a plain text column should be valid");
    }

    #[test]
    fn test_writetime_on_frozen_collection_is_valid() {
        let cols = basic_columns();
        let result =
            validate_writetime_ttl_call(&WriteTimeTtlKind::WriteTime, "frozen_scores", &cols);
        assert!(
            result.is_ok(),
            "WRITETIME on a frozen<list<int>> should be valid"
        );
    }

    #[test]
    fn test_writetime_on_blob_is_valid() {
        let cols = basic_columns();
        let result = validate_writetime_ttl_call(&WriteTimeTtlKind::WriteTime, "data", &cols);
        assert!(result.is_ok());
    }

    // --- Partition-key rejection ---

    #[test]
    fn test_writetime_on_partition_key_is_rejected() {
        let cols = basic_columns();
        let err = validate_writetime_ttl_call(&WriteTimeTtlKind::WriteTime, "user_id", &cols)
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("writeTime"),
            "Error should name the function: {msg}"
        );
        assert!(
            msg.contains("PRIMARY KEY"),
            "Error should mention PRIMARY KEY: {msg}"
        );
        assert!(
            msg.contains("user_id"),
            "Error should name the column: {msg}"
        );
    }

    #[test]
    fn test_ttl_on_partition_key_is_rejected() {
        let cols = basic_columns();
        let err =
            validate_writetime_ttl_call(&WriteTimeTtlKind::Ttl, "user_id", &cols).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("ttl"), "Error should name the function: {msg}");
        assert!(
            msg.contains("PRIMARY KEY"),
            "Error should mention PRIMARY KEY: {msg}"
        );
    }

    // --- Clustering-key rejection ---

    #[test]
    fn test_writetime_on_clustering_key_is_rejected() {
        let cols = basic_columns();
        let err =
            validate_writetime_ttl_call(&WriteTimeTtlKind::WriteTime, "bucket", &cols).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("PRIMARY KEY"),
            "Clustering-key error should cite PRIMARY KEY: {msg}"
        );
        assert!(msg.contains("bucket"));
    }

    // --- Non-frozen collection rejection ---

    #[test]
    fn test_writetime_on_list_is_rejected() {
        let cols = basic_columns();
        let err =
            validate_writetime_ttl_call(&WriteTimeTtlKind::WriteTime, "scores", &cols).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("non-frozen"),
            "Error should cite non-frozen: {msg}"
        );
        assert!(
            msg.contains("list<int>"),
            "Error should include the type: {msg}"
        );
    }

    #[test]
    fn test_writetime_on_set_is_rejected() {
        let cols = basic_columns();
        let err =
            validate_writetime_ttl_call(&WriteTimeTtlKind::WriteTime, "tags", &cols).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("non-frozen"));
    }

    #[test]
    fn test_writetime_on_map_is_rejected() {
        let cols = basic_columns();
        let err =
            validate_writetime_ttl_call(&WriteTimeTtlKind::WriteTime, "meta", &cols).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("non-frozen"));
    }

    #[test]
    fn test_ttl_on_set_is_rejected() {
        let cols = basic_columns();
        let err = validate_writetime_ttl_call(&WriteTimeTtlKind::Ttl, "tags", &cols).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("non-frozen"));
        assert!(msg.contains("ttl"));
    }

    // --- Unknown-column rejection ---

    #[test]
    fn test_writetime_on_unknown_column_is_rejected() {
        let cols = basic_columns();
        let err =
            validate_writetime_ttl_call(&WriteTimeTtlKind::WriteTime, "nonexistent_col", &cols)
                .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("nonexistent_col"),
            "Error should name the unknown column: {msg}"
        );
    }

    // --- Case-insensitive column lookup ---

    #[test]
    fn test_column_lookup_is_case_insensitive() {
        let cols = basic_columns();
        // Schema stores "name" in lower-case; query uses "NAME".
        let result = validate_writetime_ttl_call(&WriteTimeTtlKind::WriteTime, "NAME", &cols);
        assert!(result.is_ok(), "Column lookup should be case-insensitive");
    }

    // --- Batch validation ---

    #[test]
    fn test_validate_all_calls_stops_at_first_error() {
        let cols = basic_columns();
        let calls = vec![
            (WriteTimeTtlKind::WriteTime, "name".to_string()),
            (WriteTimeTtlKind::Ttl, "user_id".to_string()), // PK — should error
            (WriteTimeTtlKind::WriteTime, "data".to_string()),
        ];
        let result = validate_all_writetime_ttl_calls(&calls, &cols);
        assert!(
            result.is_err(),
            "Batch validation should fail on the PK column"
        );
    }

    #[test]
    fn test_validate_all_calls_succeeds_when_all_valid() {
        let cols = basic_columns();
        let calls = vec![
            (WriteTimeTtlKind::WriteTime, "name".to_string()),
            (WriteTimeTtlKind::Ttl, "data".to_string()),
        ];
        assert!(validate_all_writetime_ttl_calls(&calls, &cols).is_ok());
    }

    // --- is_non_frozen_collection helper ---

    #[test]
    fn test_non_frozen_collection_detection() {
        assert!(is_non_frozen_collection("list<int>"));
        assert!(is_non_frozen_collection("set<text>"));
        assert!(is_non_frozen_collection("map<text,int>"));
        assert!(is_non_frozen_collection("LIST<INT>")); // case-insensitive
        assert!(!is_non_frozen_collection("frozen<list<int>>"));
        assert!(!is_non_frozen_collection("frozen<set<text>>"));
        assert!(!is_non_frozen_collection("frozen<map<text,int>>"));
        assert!(!is_non_frozen_collection("text"));
        assert!(!is_non_frozen_collection("bigint"));
        assert!(!is_non_frozen_collection("uuid"));
    }
}
