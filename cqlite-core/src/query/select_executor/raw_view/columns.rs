//! Table-name suffix detection + the raw view's column contract (issue #4222,
//! design.md D1/D6/D7).

use super::super::{column_info_from_type_str, parse_cql_type_str};
use crate::query::result::ColumnInfo;
use crate::schema::{CqlType, TableSchema};
use std::collections::HashSet;

/// Suffix that marks a table reference as the raw-SSTable-view name for its
/// base table (design.md D1 — a naming convention over the existing flat
/// `keyspace.table` namespace, never a new grammar).
pub(in crate::query::select_executor) const RAW_VIEW_SUFFIX: &str = "_raw_sstable_data";

/// Strip [`RAW_VIEW_SUFFIX`] from a bare table name (no keyspace segment),
/// returning the base table name when the suffix is present and stripping it
/// leaves a non-empty name. `None` for an ordinary table reference.
pub(in crate::query::select_executor) fn strip_raw_view_suffix(table_name: &str) -> Option<&str> {
    table_name
        .strip_suffix(RAW_VIEW_SUFFIX)
        .filter(|base| !base.is_empty())
}

/// `true` for a CQL type whose cells are individually addressable (a
/// non-frozen list/set/map/UDT) — the column gets the `_complex_deletion`
/// trio (design.md D7) instead of relying on a single cell's own metadata.
/// `Frozen(..)` collections/UDTs are single-cell and excluded on purpose:
/// Cassandra never gives them a per-column complex-deletion marker.
fn is_complex_cql_type(t: &CqlType) -> bool {
    matches!(
        t,
        CqlType::List(_) | CqlType::Set(_) | CqlType::Map(_, _) | CqlType::Udt(_, _)
    )
}

/// Build the raw view's full column contract (design.md D7) from the BASE
/// table's schema — a pinned public surface (spec's column-snapshot
/// requirement).
///
/// Order: partition-key columns, clustering-key columns (plain data columns:
/// every physical row, including a range-tombstone bound row, carries them),
/// then per-non-key-column metadata (the base value plus its
/// `_timestamp`/`_ttl`/`_local_deletion_time`/`_tombstone` quad, plus a
/// `_complex_deletion` trio for a collection/UDT column), then the
/// row-level, partition-level, row-kind/range-tombstone, and source columns.
pub(in crate::query::select_executor) fn raw_view_columns(base: &TableSchema) -> Vec<ColumnInfo> {
    let key_names: HashSet<&str> = base
        .partition_keys
        .iter()
        .map(|k| k.name.as_str())
        .chain(base.clustering_keys.iter().map(|k| k.name.as_str()))
        .collect();

    let mut columns: Vec<ColumnInfo> = Vec::new();
    let push = |columns: &mut Vec<ColumnInfo>, name: String, type_str: &str| {
        let position = columns.len();
        columns.push(column_info_from_type_str(name, type_str, position, None));
    };

    for pk in &base.partition_keys {
        push(&mut columns, pk.name.clone(), &pk.data_type);
    }
    for ck in &base.clustering_keys {
        push(&mut columns, ck.name.clone(), &ck.data_type);
    }

    // `TableSchema::columns` carries EVERY declared column, key columns
    // included (issue #4222 research pass) — skip the ones already emitted
    // above so a key column is never duplicated as a "non-key" metadata quad.
    for col in base.columns.iter().filter(|c| !key_names.contains(c.name.as_str())) {
        push(&mut columns, col.name.clone(), &col.data_type);
        push(&mut columns, format!("{}_timestamp", col.name), "bigint");
        push(&mut columns, format!("{}_ttl", col.name), "int");
        push(
            &mut columns,
            format!("{}_local_deletion_time", col.name),
            "int",
        );
        push(&mut columns, format!("{}_tombstone", col.name), "text");

        let is_complex = parse_cql_type_str(&col.data_type)
            .map(|t| is_complex_cql_type(&t))
            .unwrap_or(false);
        if is_complex {
            push(
                &mut columns,
                format!("{}_complex_deletion", col.name),
                "boolean",
            );
            push(
                &mut columns,
                format!("{}_complex_deletion_time", col.name),
                "int",
            );
            push(
                &mut columns,
                format!("{}_complex_deletion_timestamp", col.name),
                "bigint",
            );
        }
    }

    push(&mut columns, "row_timestamp".to_string(), "bigint");
    push(&mut columns, "row_ttl".to_string(), "int");
    push(
        &mut columns,
        "row_local_deletion_time".to_string(),
        "int",
    );
    push(&mut columns, "row_tombstone".to_string(), "text");

    push(
        &mut columns,
        "partition_deletion_time".to_string(),
        "bigint",
    );
    push(
        &mut columns,
        "partition_deletion_timestamp".to_string(),
        "bigint",
    );

    push(&mut columns, "row_kind".to_string(), "text");
    push(&mut columns, "bound_inclusive".to_string(), "boolean");
    push(&mut columns, "range_deletion_time".to_string(), "bigint");
    push(
        &mut columns,
        "range_deletion_timestamp".to_string(),
        "bigint",
    );

    push(&mut columns, "sstable".to_string(), "text");
    push(&mut columns, "generation".to_string(), "int");
    push(&mut columns, "format".to_string(), "text");
    push(&mut columns, "position".to_string(), "bigint");

    columns
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn};

    fn dropped_regular_col_schema() -> TableSchema {
        TableSchema {
            keyspace: "test_tomb".to_string(),
            table: "dropped_regular_col".to_string(),
            partition_keys: vec![KeyColumn {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![ClusteringColumn {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                position: 0,
                order: ClusteringOrder::Asc,
            }],
            columns: vec![
                Column {
                    name: "pk".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "ck".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "keep_col".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "drop_col".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: Default::default(),
            dropped_columns: Default::default(),
        }
    }

    #[test]
    fn strip_raw_view_suffix_recognizes_and_rejects() {
        assert_eq!(
            strip_raw_view_suffix("dropped_regular_col_raw_sstable_data"),
            Some("dropped_regular_col")
        );
        assert_eq!(strip_raw_view_suffix("dropped_regular_col"), None);
        // Bare suffix with no base name is refused, not treated as a
        // zero-length base table.
        assert_eq!(strip_raw_view_suffix("_raw_sstable_data"), None);
    }

    /// Pinned column-contract snapshot (issue #4222, spec's public-surface
    /// requirement) for `test_tomb.dropped_regular_col_raw_sstable_data`. A
    /// column rename/add/remove in [`raw_view_columns`] must show up here as a
    /// diff, not be discovered downstream.
    #[test]
    fn dropped_regular_col_column_contract_snapshot() {
        let schema = dropped_regular_col_schema();
        let columns = raw_view_columns(&schema);
        let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "pk",
                "ck",
                "keep_col",
                "keep_col_timestamp",
                "keep_col_ttl",
                "keep_col_local_deletion_time",
                "keep_col_tombstone",
                "drop_col",
                "drop_col_timestamp",
                "drop_col_ttl",
                "drop_col_local_deletion_time",
                "drop_col_tombstone",
                "row_timestamp",
                "row_ttl",
                "row_local_deletion_time",
                "row_tombstone",
                "partition_deletion_time",
                "partition_deletion_timestamp",
                "row_kind",
                "bound_inclusive",
                "range_deletion_time",
                "range_deletion_timestamp",
                "sstable",
                "generation",
                "format",
                "position",
            ],
            "raw-view column contract changed unexpectedly — update this snapshot \
             deliberately if the change is intended (issue #4222 D7)"
        );
        // Positions must be dense/ordered (metadata.columns is positional).
        for (idx, col) in columns.iter().enumerate() {
            assert_eq!(col.position, idx);
        }
    }

    #[test]
    fn key_columns_are_never_duplicated_as_metadata_quads() {
        let schema = dropped_regular_col_schema();
        let columns = raw_view_columns(&schema);
        let pk_timestamp_present = columns.iter().any(|c| c.name == "pk_timestamp");
        assert!(
            !pk_timestamp_present,
            "a partition-key column must not get a per-cell metadata quad"
        );
    }
}
