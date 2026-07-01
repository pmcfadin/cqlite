//! Row assembly and table-id/type helpers for the SELECT executor.
//!
//! [`build_row_from_scan`] turns a raw `(RowKey, Value)` scan entry into a
//! `QueryRow`, reconstructing partition-key columns from the raw key. It is part
//! of the public surface (re-exported via `query::mod`) so other readers (e.g.
//! the Arrow Flight compaction-merge producer) assemble rows identically.

use super::super::result::{cql_type_to_data_type, ColumnInfo, QueryRow};
use crate::{
    parser::complex_types::ComplexTypeParser,
    schema::CqlType,
    types::{RowKey, Value},
    TableId,
};
use std::collections::HashMap;
use std::sync::Arc;

/// Split a `TableId` of the form `"keyspace.table"` into its parts.
///
/// If no dot is present, the whole name becomes the table component and the
/// keyspace is `None`.
pub(super) fn parse_table_id(table_id: &TableId) -> (Option<String>, String) {
    let table_str = table_id.name();
    match table_str.rfind('.') {
        Some(dot) => (
            Some(table_str[..dot].to_string()),
            table_str[dot + 1..].to_string(),
        ),
        None => (None, table_str.to_string()),
    }
}

/// Parse a CQL type string (e.g. `"list<int>"`, `"text"`) into a [`CqlType`].
///
/// Returns `None` when the type string cannot be parsed (unknown or malformed
/// types). Used to populate `ColumnInfo::cql_type` from the schema's string
/// representation, satisfying the no-heuristics mandate (Issue #28).
pub(super) fn parse_cql_type_str(type_str: &str) -> Option<CqlType> {
    let parser = ComplexTypeParser::new();
    parser
        .parse_type(type_str)
        .ok()
        .map(|parsed| parsed.cql_type)
}

/// Build a `ColumnInfo` from a schema column name + CQL type string, deriving
/// the flat `DataType` from the parsed `CqlType` (Issue #674). Centralises the
/// "name + type string → ColumnInfo" pattern shared by the SELECT-* and
/// explicit-projection column builders.
pub(super) fn column_info_from_type_str(
    name: String,
    type_str: &str,
    position: usize,
    table_name: Option<String>,
) -> ColumnInfo {
    let cql_type_opt = parse_cql_type_str(type_str);
    let data_type = cql_type_opt
        .as_ref()
        .map(cql_type_to_data_type)
        .unwrap_or(crate::types::DataType::Text);
    let mut col_info = ColumnInfo {
        name,
        data_type,
        nullable: true,
        position,
        table_name,
        cql_type: None,
    };
    if let Some(cql_type) = cql_type_opt {
        col_info = col_info.with_cql_type(cql_type);
    }
    col_info
}

/// Build a `QueryRow` from a single `(RowKey, Value)` produced by storage scan,
/// applying optional projection and synthesising partition-key columns from the
/// raw key bytes when a schema is available.
///
/// Partition-key columns are never stored in the cell payload, so they are
/// reconstructed from the raw row key via the canonical
/// [`crate::storage::partition_key_codec::decode_partition_key_columns`] (the
/// same codec the write engine uses). This is the fix for Issue #586: the
/// previous decoder assumed a `u16` length prefix for every TEXT key, which is
/// only correct for composite components — a single-component TEXT partition key
/// is raw bytes, so its column was silently dropped from scan-built rows.
///
/// Returns `None` for tombstoned rows (so the caller can `continue`).
///
/// Exposed publicly so other readers (e.g. the Arrow Flight server's compaction
/// merge producer) can assemble rows identically to the SELECT path, guaranteeing
/// output parity. The `value` is expected to be a `Value::Map` of decoded
/// non-partition-key cells; partition-key columns are reconstructed from `key`.
pub fn build_row_from_scan(
    key: RowKey,
    value: Value,
    projection: &[String],
    schema: Option<&crate::schema::TableSchema>,
) -> Option<QueryRow> {
    // Suppress tombstoned rows from user-visible output. A row tombstone reaches
    // here as `Value::Tombstone` (Issue #505); before that change it was `Value::Null`.
    // Both must be suppressed identically so deleted rows never appear in query results.
    if matches!(value, Value::Null | Value::Tombstone(_)) {
        return None;
    }

    let mut row_values: HashMap<Arc<str>, Value> = HashMap::new();
    let project = |name: &str| projection.is_empty() || projection.iter().any(|p| p == name);

    if let Value::Row(cells) = value {
        // Issue #1334: the decoder carries interned `Arc<str>` column-name
        // handles in the row carrier; move them straight into `QueryRow.values`
        // (an `Arc` move — NO `String` re-allocation of the name).
        for (name, col_value) in cells {
            if project(&name) {
                row_values.insert(name, col_value);
            }
        }
        // Cassandra never serialises partition-key columns in the cell payload;
        // reconstruct them from the raw row key when the schema is known. We
        // decode through the canonical codec shared with the write engine so
        // single-component (raw bytes) and composite (`[u16 len][bytes][0x00]`)
        // keys are handled identically on both paths (Issue #586).
        if let Some(schema) = schema {
            match crate::storage::partition_key_codec::decode_partition_key_columns(&key.0, schema)
            {
                Ok(pk_columns) => {
                    for (name, value) in pk_columns {
                        if project(&name) {
                            row_values.insert(name.into(), value);
                        }
                    }
                }
                // Surface — never silently swallow — a decode failure, so a
                // missing partition-key column can't ship invisibly (Issue #586).
                Err(e) => {
                    log::warn!(
                        "Failed to reconstruct partition-key columns from row key \
                         (len={} bytes) for {}.{}: {}",
                        key.0.len(),
                        schema.keyspace,
                        schema.table,
                        e
                    );
                }
            }
        }
    } else {
        // Non-row fallback: expose the raw value plus a debug-formatted id.
        row_values.insert(Arc::from("data"), value);
        if project("id") {
            row_values.insert(Arc::from("id"), Value::Text(format!("{:?}", key)));
        }
    }

    Some(QueryRow {
        values: row_values,
        key,
        metadata: Default::default(),
        cell_metadata: None,
    })
}

#[cfg(test)]
mod tests {
    use super::super::predicate::evaluate_predicates;
    use super::super::test_support::single_pk_schema;
    use super::*;

    /// Issue #586: a single-component TEXT partition key is stored as raw bytes
    /// with NO length prefix. `build_row_from_scan` must materialise it from the
    /// `RowKey`. Before the fix the column was silently dropped (the decoder
    /// read a phantom `u16` prefix, errored, and the error was swallowed).
    #[test]
    fn build_row_from_scan_materialises_single_text_pk() {
        let key = RowKey::new(b"k0000000000000000".to_vec());
        let value = Value::Row(vec![(Arc::from("name"), Value::Text("name-0".to_string()))]);
        let schema = single_pk_schema("id", "text");

        let row = build_row_from_scan(key, value, &[], Some(&schema))
            .expect("row must be built (not tombstoned)");

        assert_eq!(
            row.values.get("id"),
            Some(&Value::Text("k0000000000000000".to_string())),
            "Issue #586: single TEXT PK column must be reconstructed from the raw row key"
        );
        // Regular columns must still be present.
        assert_eq!(
            row.values.get("name"),
            Some(&Value::Text("name-0".to_string()))
        );
    }

    /// Issue #586: with the PK column materialised, a residual `WHERE id = '...'`
    /// (the path TEXT single-PK queries fall through to) now matches.
    #[test]
    fn scan_built_row_matches_text_pk_equality_predicate() {
        use super::super::super::select_optimizer::{SSTableFilterOp, SSTablePredicate};

        let key = RowKey::new(b"k0000000000000000".to_vec());
        let value = Value::Row(vec![(Arc::from("age"), Value::Integer(0))]);
        let schema = single_pk_schema("id", "text");
        let row = build_row_from_scan(key, value, &[], Some(&schema)).unwrap();

        let predicate = SSTablePredicate::column(
            "id",
            SSTableFilterOp::Equal,
            vec![Value::Text("k0000000000000000".to_string())],
        );

        assert!(
            evaluate_predicates(&row, std::slice::from_ref(&predicate)).unwrap(),
            "Issue #586: WHERE id = '<literal>' must match the reconstructed PK column"
        );
    }
}
