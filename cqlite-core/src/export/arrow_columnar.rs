//! Per-column accessor resolution for Arrow conversion (issue #1495, AE1).
//!
//! # Why this exists
//!
//! Every Arrow builder in [`super::arrow_convert`] used to do
//! `row.values.get(&col.name)` **per cell** — a `HashMap<Arc<str>, Value>`
//! sip-hash string lookup into each row's value map. For `N` rows × `M` columns
//! that meant `M` independent full passes over the row vector, each probing the
//! large per-row map by name once per row (`N·M` probes total, INCLUDING absent
//! cells), even though a column's name is known once per schema. This is the
//! export-side instance of the string-typed per-cell dispatch that parser epic
//! **J1** condemned; the shared fix shape is "resolve a per-column accessor once,
//! then index rows positionally".
//!
//! # The fix
//!
//! [`transpose_columns`] resolves each schema column to its output position
//! **once** (building a small `name → column-index` map — `M` name hashes), then
//! makes a **single pass** over the rows, iterating each row's OWN entries. Each
//! present cell is routed to its column's slot via one lookup into the tiny
//! `M`-entry index map. The result is column-major: a `Vec<Option<&Value>>` per
//! column, aligned to the row order, that each builder consumes directly.
//!
//! Consequently the large per-row `values` maps are **never** `.get()`-probed by
//! column name at all — the per-cell string-hash lookup against them is gone. The
//! only hashing is (a) the `M`-entry index map built once and (b) one lookup per
//! *present* cell into that tiny map (so sparse rows cost nothing for their
//! absent columns, unlike the old `N·M` full-scan).
//!
//! Output is byte-identical: `None` (absent column) and `Some(&Value::Null)`
//! (explicit null) are preserved exactly as the builders' `None`/`Value::Null`
//! arms already expect, and column/row alignment is unchanged.

use std::collections::HashMap;

use crate::query::{ColumnInfo, QueryRow};
use crate::types::Value;

/// Resolve every schema column to its aligned, column-major value slice in a
/// single pass over `rows`.
///
/// Returns one `Vec<Option<&Value>>` per column (in `columns` order), each of
/// length `rows.len()`. Element `i` of column `c`'s slice is:
/// - `Some(&value)` when row `i` carries a value for `columns[c].name`, or
/// - `None` when the column is absent from row `i` (the builders map both an
///   absent cell and a `Value::Null` to an Arrow null, exactly as before).
///
/// Column names are hashed only `M` times to build the `name → index` map;
/// thereafter the per-row work iterates the row's own entries and does one lookup
/// per PRESENT cell into that small map — the large per-row `values` maps are
/// never probed by column name (issue #1495 / parser epic J1).
///
/// Duplicate column names in `columns` are not expected (a result schema has
/// distinct columns); if present, the last occurrence wins the index slot, which
/// matches the old per-builder `get` (each builder resolved its own `col.name`).
pub(crate) fn transpose_columns<'a>(
    columns: &[ColumnInfo],
    rows: &'a [QueryRow],
) -> Vec<Vec<Option<&'a Value>>> {
    let n_rows = rows.len();
    let n_cols = columns.len();

    // Resolve each column name to its output-slice index ONCE (M name hashes).
    let mut name_to_idx: HashMap<&str, usize> = HashMap::with_capacity(n_cols);
    for (idx, col) in columns.iter().enumerate() {
        name_to_idx.insert(col.name.as_str(), idx);
    }

    // Column-major output, pre-sized so no slot re-allocates. Absent cells stay
    // `None`; only present cells are written.
    let mut columnar: Vec<Vec<Option<&'a Value>>> =
        (0..n_cols).map(|_| vec![None; n_rows]).collect();

    // Single pass over rows. For each row we iterate its OWN entries and route
    // each present cell to its column slot via the tiny name→index map. No
    // `values.get(col.name)` probe against the large per-row map occurs.
    for (row_idx, row) in rows.iter().enumerate() {
        for (name, value) in &row.values {
            if let Some(&col_idx) = name_to_idx.get(name.as_ref()) {
                columnar[col_idx][row_idx] = Some(value);
            }
            // A row entry whose name is not in the schema is ignored, matching
            // the old behaviour (no builder ever requested it).
        }
    }

    columnar
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::ColumnInfo;
    use crate::types::DataType;
    use crate::RowKey;
    use std::sync::Arc;

    fn col(name: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: DataType::Integer,
            nullable: true,
            position: 0,
            table_name: None,
            cql_type: None,
        }
    }

    fn row(pairs: &[(&str, Value)]) -> QueryRow {
        let mut values: HashMap<Arc<str>, Value> = HashMap::new();
        for (name, value) in pairs {
            values.insert(Arc::from(*name), value.clone());
        }
        QueryRow::with_interned_values(RowKey::new(Vec::new()), values)
    }

    /// The transpose returns one aligned slice per column, of length `rows`, with
    /// present cells routed to their column and absent cells left `None`.
    #[test]
    fn transpose_aligns_columns_and_rows() {
        let columns = vec![col("a"), col("b"), col("c")];
        let rows = vec![
            row(&[
                ("a", Value::Integer(1)),
                ("b", Value::Integer(2)),
                ("c", Value::Integer(3)),
            ]),
            // b absent; a null; c present.
            row(&[("a", Value::Null), ("c", Value::Integer(30))]),
        ];

        let cols = transpose_columns(&columns, &rows);
        assert_eq!(cols.len(), 3);
        for slice in &cols {
            assert_eq!(slice.len(), 2);
        }

        // Column a: row0 = 1, row1 = Null.
        assert_eq!(cols[0][0], Some(&Value::Integer(1)));
        assert_eq!(cols[0][1], Some(&Value::Null));
        // Column b: row0 = 2, row1 ABSENT (None).
        assert_eq!(cols[1][0], Some(&Value::Integer(2)));
        assert_eq!(cols[1][1], None);
        // Column c: row0 = 3, row1 = 30.
        assert_eq!(cols[2][0], Some(&Value::Integer(3)));
        assert_eq!(cols[2][1], Some(&Value::Integer(30)));
    }

    /// A row entry whose name is not in the schema is ignored (never panics, never
    /// mis-routed) — matching the old per-builder `get` which simply never asked
    /// for it.
    #[test]
    fn extra_row_entry_not_in_schema_is_ignored() {
        let columns = vec![col("a")];
        let rows = vec![row(&[("a", Value::Integer(1)), ("z", Value::Integer(9))])];
        let cols = transpose_columns(&columns, &rows);
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0][0], Some(&Value::Integer(1)));
    }

    /// Empty inputs: no columns / no rows produce well-formed empty output.
    #[test]
    fn empty_inputs() {
        let no_cols: Vec<ColumnInfo> = vec![];
        let rows = vec![row(&[("a", Value::Integer(1))])];
        assert!(transpose_columns(&no_cols, &rows).is_empty());

        let columns = vec![col("a")];
        let no_rows: Vec<QueryRow> = vec![];
        let cols = transpose_columns(&columns, &no_rows);
        assert_eq!(cols.len(), 1);
        assert!(cols[0].is_empty());
    }
}
