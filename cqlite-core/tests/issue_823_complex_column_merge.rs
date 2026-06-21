//! Issue #823 (Epic #817): complex-column (multi-cell collection / non-frozen
//! UDT) merge fidelity — PUBLIC-API level evidence.
//!
//! The authoritative reconcile function (`KWayMerger::reconcile_cluster`) and the
//! reader→merge adapter (`SSTableRowIteratorAdapter::value_to_row_data`) are
//! private, so the deep value-asserting gating tests live in an additive
//! `#[cfg(test)]` module inside `merge.rs`
//! (`mod issue_823_complex_column_merge`).
//!
//! This external file asserts the STRUCTURAL root cause that is observable from
//! the public API: the public merge data model (`MergeEntry` / `RowData` /
//! `CellData`) has NO per-path / collection-key dimension. A multi-cell column is
//! represented as ONE `CellData` keyed only on the column name string, so per-path
//! merge (#18) and path-scoped complex deletion (#14/#17) are not representable.
//!
//! Run with:
//!   CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!     cargo test --package cqlite-core --features write-support \
//!     --test issue_823_complex_column_merge

#![cfg(feature = "write-support")]

use cqlite_core::storage::write_engine::merge::{CellData, RowData};
use cqlite_core::types::{UdtField, UdtValue, Value};

/// Issue #886 added the carry-only `cell_path` / `local_deletion_time` substrate
/// fields to `CellData` (defaulting to `None`), but per-path merge is still NOT
/// wired: the reader does not yet populate `cell_path` and the merge does not yet
/// consume it (that reader-emit + per-path reconcile work is #899). So two writes
/// to different paths of the same column remain indistinguishable in practice —
/// the field exists but nothing fills it in.
#[test]
fn celldata_path_dimension_is_carry_only() {
    let cell = CellData {
        column: "tags".to_string(),
        value: Value::List(vec![Value::Text("a".to_string())]),
        timestamp: 1,
        ttl: None,
        cell_path: None,
        local_deletion_time: None,
    };

    // The path dimension now exists structurally (#886) but is carry-only: when
    // built from the reader it is `None`. If a later change starts POPULATING
    // `cell_path` from the reader emit, this test should be revisited alongside
    // the #899 per-path merge work.
    let CellData {
        column,
        value,
        timestamp,
        ttl,
        cell_path,
        local_deletion_time,
    } = cell;
    assert_eq!(column, "tags");
    assert!(matches!(value, Value::List(_)));
    assert_eq!(timestamp, 1);
    assert_eq!(ttl, None);
    assert_eq!(cell_path, None, "carry-only: unpopulated until #899");
    assert_eq!(
        local_deletion_time, None,
        "carry-only: unpopulated until #899"
    );
}

/// A multi-cell collection is surfaced as a single nested `Value` under one
/// column. The public `RowData::Live` holds a flat `Vec<CellData>`; the only way
/// to represent a whole non-frozen collection is as one cell. There is no API to
/// express "two cells, same column, different paths" that the merge would union.
#[test]
fn multicell_collection_is_one_nested_cell_value() {
    let row = RowData::Live {
        cells: vec![CellData {
            column: "tags".to_string(),
            value: Value::List(vec![
                Value::Text("a".to_string()),
                Value::Text("b".to_string()),
            ]),
            timestamp: 10,
            ttl: None,
            cell_path: None,
            local_deletion_time: None,
        }],
    };

    match row {
        RowData::Live { cells } => {
            assert_eq!(cells.len(), 1, "whole collection lives in one cell");
            match &cells[0].value {
                Value::List(items) => assert_eq!(items.len(), 2),
                other => panic!("expected nested List value, got {:?}", other),
            }
        }
        RowData::Tombstone { .. } => panic!("expected live row"),
    }
}

/// A non-frozen UDT is likewise one nested `Value::Udt` under one column. Cassandra
/// stores it multi-cell (one cell per field, ordered by SIGNED ShortType field
/// index — #18). That ordering is unobservable here because the whole UDT is a
/// single cell value with no per-field cell granularity in the merge model.
#[test]
fn nonfrozen_udt_is_one_nested_cell_value() {
    let row = RowData::Live {
        cells: vec![CellData {
            column: "address".to_string(),
            value: Value::Udt(UdtValue {
                type_name: "addr".to_string(),
                keyspace: "ks".to_string(),
                fields: vec![
                    UdtField {
                        name: "city".to_string(),
                        value: Some(Value::Text("SF".to_string())),
                    },
                    UdtField {
                        name: "zip".to_string(),
                        value: Some(Value::Text("94105".to_string())),
                    },
                ],
            }),
            timestamp: 10,
            ttl: None,
            cell_path: None,
            local_deletion_time: None,
        }],
    };

    match row {
        RowData::Live { cells } => {
            assert_eq!(cells.len(), 1, "whole UDT lives in one cell");
            match &cells[0].value {
                Value::Udt(u) => assert_eq!(u.fields.len(), 2),
                other => panic!("expected nested Udt value, got {:?}", other),
            }
        }
        RowData::Tombstone { .. } => panic!("expected live row"),
    }
}

/// `RowData::Tombstone` is still whole-row only (deletion_time +
/// local_deletion_time, no column scope). Issue #886 added a dedicated,
/// column-scoped complex-deletion entity (`MergeEntry.complex_deletions` /
/// `ComplexDeletion`) as carry-only substrate, but `RowData` itself is unchanged —
/// a per-column complex deletion is NOT smuggled into `RowData::Tombstone`.
#[test]
fn row_tombstone_has_no_column_scope() {
    let t = RowData::Tombstone {
        deletion_time: 100,
        local_deletion_time: 0,
    };
    // Destructure to assert the variant carries no column / path field.
    match t {
        RowData::Tombstone {
            deletion_time,
            local_deletion_time,
        } => {
            assert_eq!(deletion_time, 100);
            assert_eq!(local_deletion_time, 0);
        }
        RowData::Live { .. } => panic!("expected tombstone"),
    }
}
