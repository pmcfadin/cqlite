//! Shared `#[cfg(test)]` fixtures for the SELECT executor submodules.
//!
//! Schema builders and `QueryRow` constructors used by more than one submodule's
//! test module. Relocated verbatim from the monolithic `select_executor.rs` test
//! module; behaviour (and the rows/schemas they produce) is unchanged.

use crate::query::result::QueryRow;
use crate::types::{RowKey, Value};

/// Build a single-column partition-key schema.
pub(crate) fn single_pk_schema(name: &str, data_type: &str) -> crate::schema::TableSchema {
    crate::schema::TableSchema {
        keyspace: "ks".to_string(),
        table: "t".to_string(),
        partition_keys: vec![crate::schema::KeyColumn {
            name: name.to_string(),
            data_type: data_type.to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![],
        comments: std::collections::HashMap::new(),
        dropped_columns: std::collections::HashMap::new(),
    }
}

/// Build a two-column composite partition-key schema (`a`, `b` in declared
/// order). Used by the FINDING 2/3 token-validation and composite-IN tests.
pub(crate) fn composite_pk_schema(
    first: (&str, &str),
    second: (&str, &str),
) -> crate::schema::TableSchema {
    crate::schema::TableSchema {
        keyspace: "ks".to_string(),
        table: "t".to_string(),
        partition_keys: vec![
            crate::schema::KeyColumn {
                name: first.0.to_string(),
                data_type: first.1.to_string(),
                position: 0,
            },
            crate::schema::KeyColumn {
                name: second.0.to_string(),
                data_type: second.1.to_string(),
                position: 1,
            },
        ],
        clustering_keys: vec![],
        columns: vec![],
        comments: std::collections::HashMap::new(),
        dropped_columns: std::collections::HashMap::new(),
    }
}

/// Build a one-column `QueryRow` for predicate-evaluation tests.
pub(crate) fn row_with_int(column: &str, value: i64) -> QueryRow {
    let mut values: std::collections::HashMap<std::sync::Arc<str>, Value> =
        std::collections::HashMap::new();
    values.insert(column.into(), Value::Integer(value as i32));
    QueryRow {
        values,
        key: RowKey::new(Vec::new()),
        metadata: Default::default(),
        cell_metadata: None,
    }
}

/// Build a `QueryRow` with only a partition key (no column values).
pub(crate) fn row_with_key(partition: &[u8]) -> QueryRow {
    QueryRow {
        values: std::collections::HashMap::new(),
        key: RowKey::new(partition.to_vec()),
        metadata: Default::default(),
        cell_metadata: None,
    }
}
