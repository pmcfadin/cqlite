//! Row assembly and table-id/type helpers for the SELECT executor.
//!
//! [`build_row_from_scan`] turns a raw `(RowKey, ScanRow)` scan entry into a
//! `QueryRow`, reconstructing partition-key columns from the raw key. It is part
//! of the public surface (re-exported via `query::mod`) so other readers (e.g.
//! the Arrow Flight compaction-merge producer) assemble rows identically.

use super::super::result::{cql_type_to_data_type, ColumnInfo, QueryRow};
use crate::{
    parser::complex_types::ComplexTypeParser,
    schema::CqlType,
    types::{RowKey, ScanRow, Value},
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

/// Build a `QueryRow` from a single `(RowKey, ScanRow)` produced by storage scan,
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
/// output parity. The `row` carries the decoded non-partition-key cells as the
/// single [`ScanRow`] row carrier (issue #1334); partition-key columns are
/// reconstructed from `key`.
pub fn build_row_from_scan(
    key: RowKey,
    row: ScanRow,
    projection: &[String],
    schema: Option<&crate::schema::TableSchema>,
) -> Option<QueryRow> {
    // Suppress tombstoned / absent rows from user-visible output. A row tombstone
    // or null row reaches here as `ScanRow::Marker` (Issue #505); it must never
    // appear in query results. A live row is always `ScanRow::Row`, so there is
    // exactly ONE row-carrier path — a live row's column values can never silently
    // fall through to a non-row fallback (issue #1334 / roborev H2).
    let cells = row.into_cells()?;

    // Issue #1584: pre-size the value map to the upper bound of inserts — the
    // decoded cell count plus the reconstructed partition-key columns. This is a
    // single sized allocation with no rehash growth (for `SELECT *` it is exact;
    // for a narrower projection it slightly over-reserves, still one alloc).
    let pk_hint = schema.map(|s| s.partition_keys.len()).unwrap_or(0);
    let mut row_values: HashMap<Arc<str>, Value> = HashMap::with_capacity(cells.len() + pk_hint);
    let project = |name: &str| projection.is_empty() || projection.iter().any(|p| p == name);

    // Issue #1334: the decoder carries interned `Arc<str>` column-name handles in
    // the row carrier; move them straight into `QueryRow.values` (an `Arc` move —
    // NO `String` re-allocation of the name).
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
        match crate::storage::partition_key_codec::decode_partition_key_columns(&key.0, schema) {
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
                tracing::warn!(
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
        let value = ScanRow::Row(vec![(Arc::from("name"), Value::Text("name-0".to_string()))]);
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
        let value = ScanRow::Row(vec![(Arc::from("age"), Value::Integer(0))]);
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

    /// Issue #1334 / roborev H2: a multi-column live `ScanRow::Row` must
    /// disassemble into EVERY named column value — never collapse into a
    /// synthetic `"data"` fallback (the bug where a non-`ScanRow::Row` carrier
    /// dropped all column values). This pins the single-carrier contract:
    /// `build_row_from_scan` yields the real columns and NO fallback key.
    #[test]
    fn build_row_from_scan_multi_column_row_has_no_data_fallback() {
        let key = RowKey::new(b"k0000000000000000".to_vec());
        let value = ScanRow::Row(vec![
            (Arc::from("name"), Value::Text("alice".to_string())),
            (Arc::from("score"), Value::Integer(42)),
        ]);

        // No schema → no partition-key reconstruction; only the row's own cells.
        let row = build_row_from_scan(key, value, &[], None)
            .expect("a live row must build (not tombstoned)");

        assert_eq!(
            row.values.get("name"),
            Some(&Value::Text("alice".to_string())),
            "real text column value must survive the row-carrier disassembly"
        );
        assert_eq!(
            row.values.get("score"),
            Some(&Value::Integer(42)),
            "real int column value must survive the row-carrier disassembly"
        );
        assert!(
            !row.values.contains_key("data"),
            "roborev H2: column values must NOT collapse into a synthetic 'data' fallback"
        );
        assert_eq!(
            row.values.len(),
            2,
            "exactly the two real columns, no extras"
        );
    }

    /// Issue #1584: the row value map is pre-sized to the decoded cell count so a
    /// single sized allocation covers the row (no rehash growth). Pinned via a
    /// narrow projection: 8 cells are decoded but only ONE survives projection —
    /// the map's capacity must still reflect the 8-cell hint (`>= 8`). With the
    /// pre-fix `HashMap::new()` the map is grown from empty to the single inserted
    /// entry (capacity 3), which fails this lower bound.
    #[test]
    fn build_row_from_scan_presizes_value_map() {
        let cells: Vec<(Arc<str>, Value)> = (0..8)
            .map(|i| (Arc::from(format!("c{i}").as_str()), Value::Integer(i)))
            .collect();
        let key = RowKey::new(b"k".to_vec());

        let row = build_row_from_scan(key, ScanRow::Row(cells), &["c0".to_string()], None)
            .expect("a live row must build");

        assert_eq!(row.values.len(), 1, "projection keeps exactly one column");
        assert!(
            row.values.capacity() >= 8,
            "issue #1584: value map must be pre-sized to the decoded cell count \
             (>= 8), not grown from empty to the projected size; got capacity {}",
            row.values.capacity()
        );
    }

    /// A suppressed marker (row tombstone / null row) yields no user-visible row.
    #[test]
    fn build_row_from_scan_marker_is_suppressed() {
        let key = RowKey::new(b"k".to_vec());
        assert!(
            build_row_from_scan(key, ScanRow::Marker(Value::Null), &[], None).is_none(),
            "a marker (tombstone/null) row must be suppressed from user output"
        );
    }

    /// Issue #1334 (roborev round 9, finding 2): the canonical query consumer
    /// SUPPRESSES a `ScanRow::Marker` but SURFACES a live `ScanRow::Row`. This is
    /// exactly why the CLI `read`/`inspect`/`benchmark` bulletproof-reader fallback
    /// producers must NOT wrap a LIVE synthetic value in `Marker` — doing so drops
    /// it from user-visible output. The SAME live value must survive as a `Row`.
    #[test]
    fn live_value_dropped_as_marker_surfaces_as_row() {
        let key = RowKey::new(b"k".to_vec());
        let live = Value::Text("synthetic-fallback".to_string());

        // Wrapped as a Marker (the pre-fix producer): dropped entirely.
        assert!(
            build_row_from_scan(key.clone(), ScanRow::Marker(live.clone()), &[], None).is_none(),
            "a LIVE value mis-wrapped as Marker is dropped — the bug the producers must avoid"
        );

        // Wrapped as a live Row (the fixed producer): surfaces under its cell name.
        let row = build_row_from_scan(
            key,
            ScanRow::Row(vec![(Arc::from("data"), live.clone())]),
            &[],
            None,
        )
        .expect("a live Row must surface");
        assert_eq!(row.values.get("data"), Some(&live));
    }
}
