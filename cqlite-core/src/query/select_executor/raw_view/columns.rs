//! Table-name suffix detection + the raw view's column contract (issue #4222,
//! design.md D1/D6/D7).

use super::super::{column_info_from_type_str, parse_cql_type_str};
use crate::query::result::ColumnInfo;
use crate::schema::{CqlType, TableSchema};
use crate::Error;
use std::collections::HashSet;

pub(in crate::query::select_executor) use crate::query::raw_view_naming::strip_raw_view_suffix;
/// Suffix that marks a table reference as the raw-SSTable-view name for its
/// base table (design.md D1 — a naming convention over the existing flat
/// `keyspace.table` namespace, never a new grammar). The single source of
/// truth lives in [`crate::query::raw_view_naming`] — shared with
/// `Database::has_schema_for_table`/`schema_status`, so the CLI's pre-flight
/// schema check (issue #199) recognizes a raw-view name too, instead of
/// rejecting every raw-view query before `SelectExecutor` can intercept it.
pub(in crate::query::select_executor) use crate::query::raw_view_naming::RAW_SSTABLE_VIEW_SUFFIX as RAW_VIEW_SUFFIX;

/// `true` for a CQL type whose cells are individually addressable (a
/// non-frozen list/set/map/UDT) — the column gets ONLY the
/// `_complex_deletion` trio (design.md D7), never the single-cell
/// `_timestamp`/`_ttl`/`_local_deletion_time`/`_tombstone` quad: a collection
/// has no ONE cell timestamp to report, and declaring those four columns for
/// a complex type would always render NULL — indistinguishable from "no
/// timestamp exists" (roborev finding, issue #4222). `Frozen(..)`
/// collections/UDTs are single-cell and excluded on purpose: Cassandra never
/// gives them a per-column complex-deletion marker, so they keep the plain
/// quad instead.
fn is_complex_cql_type(t: &CqlType) -> bool {
    match t {
        CqlType::List(_) | CqlType::Set(_) | CqlType::Map(_, _) | CqlType::Udt(_, _) => true,
        // A bare (non-frozen) UDT name parses to `Custom("udt:<name>")`, NEVER
        // `CqlType::Udt(..)` (roborev finding, issue #4222):
        // `ComplexTypeParser::parse_with_depth` only builds the structured
        // `Udt` variant when it has the full field list to hand, which a bare
        // schema type STRING never carries — `schema/cql_type_parser.rs:249`.
        // Matching only `Udt(..)` therefore silently misclassified every
        // real non-frozen UDT column as "simple".
        CqlType::Custom(name) => name.starts_with("udt:"),
        _ => false,
    }
}

/// Build the raw view's full column contract (design.md D7) from the BASE
/// table's schema — a pinned public surface (spec's column-snapshot
/// requirement).
///
/// Order: partition-key columns, clustering-key columns (plain data columns:
/// every physical row, including a range-tombstone bound row, carries them),
/// then per-non-key-column metadata (the base value plus, for a simple
/// column, its `_timestamp`/`_ttl`/`_local_deletion_time`/`_tombstone` quad,
/// or for a collection/UDT column, its `_complex_deletion` trio only), then
/// the row-level, partition-level, row-kind/range-tombstone, and source
/// columns.
///
/// Fails closed (design.md D8) with `Error::Schema` when a synthesized
/// metadata-column name COLLIDES with a real base-table column name (e.g. a
/// base table with its own `generation`/`sstable`/`row_kind` column, or a
/// `val` column alongside a `val_timestamp` column) — silently overwriting
/// one of the two would be a no-heuristics-violating silent data loss
/// (roborev finding, issue #4222), never something this view may do quietly.
pub(in crate::query::select_executor) fn raw_view_columns(
    base: &TableSchema,
) -> crate::Result<Vec<ColumnInfo>> {
    let key_names: HashSet<&str> = base
        .partition_keys
        .iter()
        .map(|k| k.name.as_str())
        .chain(base.clustering_keys.iter().map(|k| k.name.as_str()))
        .collect();

    let mut columns: Vec<ColumnInfo> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut push = |columns: &mut Vec<ColumnInfo>, name: String, type_str: &str| {
        if !seen.insert(name.clone()) {
            return Err(Error::Schema(format!(
                "raw SSTable view: base table '{}.{}' has a column named '{name}' that \
                 collides with a synthesized metadata column name — the raw view's column \
                 contract (design.md D7) cannot be built without silently shadowing one of \
                 the two",
                base.keyspace, base.table
            )));
        }
        let position = columns.len();
        columns.push(column_info_from_type_str(name, type_str, position, None));
        Ok(())
    };

    for pk in &base.partition_keys {
        push(&mut columns, pk.name.clone(), &pk.data_type)?;
    }
    for ck in &base.clustering_keys {
        push(&mut columns, ck.name.clone(), &ck.data_type)?;
    }

    // `TableSchema::columns` carries EVERY declared column, key columns
    // included (issue #4222 research pass) — skip the ones already emitted
    // above so a key column is never duplicated as a "non-key" metadata quad.
    for col in base
        .columns
        .iter()
        .filter(|c| !key_names.contains(c.name.as_str()))
    {
        push(&mut columns, col.name.clone(), &col.data_type)?;

        // Fail closed (design.md D8) on an unparseable declared type rather
        // than defaulting to "simple" (roborev finding, issue #4222): a type
        // this parser cannot classify might be complex, and silently
        // declaring the wrong column shape is the same class of defect the
        // collision check above exists to prevent.
        let cql_type = parse_cql_type_str(&col.data_type).ok_or_else(|| {
            Error::Schema(format!(
                "raw SSTable view: base table '{}.{}' column '{}' has a declared type \
                 ('{}') this parser cannot classify as simple or complex — refusing to guess \
                 the column contract shape (issue #28 no-heuristics)",
                base.keyspace, base.table, col.name, col.data_type
            ))
        })?;
        let is_complex = is_complex_cql_type(&cql_type);
        if is_complex {
            push(
                &mut columns,
                format!("{}_complex_deletion", col.name),
                "boolean",
            )?;
            push(
                &mut columns,
                format!("{}_complex_deletion_time", col.name),
                "int",
            )?;
            push(
                &mut columns,
                format!("{}_complex_deletion_timestamp", col.name),
                "bigint",
            )?;
        } else {
            push(&mut columns, format!("{}_timestamp", col.name), "bigint")?;
            push(&mut columns, format!("{}_ttl", col.name), "int")?;
            push(
                &mut columns,
                format!("{}_local_deletion_time", col.name),
                "int",
            )?;
            push(&mut columns, format!("{}_tombstone", col.name), "text")?;
        }
    }

    push(&mut columns, "row_timestamp".to_string(), "bigint")?;
    push(&mut columns, "row_ttl".to_string(), "int")?;
    push(&mut columns, "row_local_deletion_time".to_string(), "int")?;
    push(&mut columns, "row_tombstone".to_string(), "text")?;
    // The row tombstone's own `markedForDeleteAt` — distinct from
    // `row_local_deletion_time` (the GC-clock seconds), mirroring the
    // partition/range pairs below (roborev finding, issue #4222: this was
    // previously discarded, making a row tombstone's writetime unrecoverable
    // from this view).
    push(&mut columns, "row_deletion_timestamp".to_string(), "bigint")?;

    push(
        &mut columns,
        "partition_deletion_time".to_string(),
        "bigint",
    )?;
    push(
        &mut columns,
        "partition_deletion_timestamp".to_string(),
        "bigint",
    )?;

    push(&mut columns, "row_kind".to_string(), "text")?;
    push(&mut columns, "bound_inclusive".to_string(), "boolean")?;
    push(&mut columns, "range_deletion_time".to_string(), "bigint")?;
    push(
        &mut columns,
        "range_deletion_timestamp".to_string(),
        "bigint",
    )?;

    push(&mut columns, "sstable".to_string(), "text")?;
    push(&mut columns, "generation".to_string(), "int")?;
    push(&mut columns, "format".to_string(), "text")?;
    push(&mut columns, "position".to_string(), "bigint")?;

    Ok(columns)
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
        let columns = raw_view_columns(&schema).expect("no collision in this fixture");
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
                "row_deletion_timestamp",
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
        let columns = raw_view_columns(&schema).expect("no collision in this fixture");
        let pk_timestamp_present = columns.iter().any(|c| c.name == "pk_timestamp");
        assert!(
            !pk_timestamp_present,
            "a partition-key column must not get a per-cell metadata quad"
        );
    }

    /// A base table whose real column name collides with a synthesized
    /// metadata-column name must fail closed (design.md D8), never silently
    /// clobber one of the two (roborev finding, issue #4222).
    #[test]
    fn colliding_base_column_name_fails_closed() {
        let mut schema = dropped_regular_col_schema();
        schema.columns.push(Column {
            name: "generation".to_string(),
            data_type: "int".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        });
        let err = raw_view_columns(&schema)
            .expect_err("a base column literally named 'generation' must be refused");
        assert!(
            matches!(err, Error::Schema(_)),
            "collision must surface as Error::Schema, got: {err:?}"
        );
    }

    /// A base column named `<other>_timestamp` alongside a plain `<other>`
    /// column collides with that OTHER column's synthesized metadata quad.
    #[test]
    fn colliding_quad_suffix_fails_closed() {
        let mut schema = dropped_regular_col_schema();
        schema.columns.push(Column {
            name: "keep_col_timestamp".to_string(),
            data_type: "bigint".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        });
        let err = raw_view_columns(&schema)
            .expect_err("a base column literally named 'keep_col_timestamp' must be refused");
        assert!(matches!(err, Error::Schema(_)));
    }
}
