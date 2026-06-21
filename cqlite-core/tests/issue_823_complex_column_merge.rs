//! Issue #823 (Epic #817): complex-column (multi-cell collection / non-frozen
//! UDT) merge fidelity — PUBLIC-API level evidence.
//!
//! The authoritative reconcile function (`KWayMerger::reconcile_cluster`) and the
//! reader→merge adapter (`SSTableRowIteratorAdapter::value_to_row_data`) are
//! private, so the deep value-asserting gating tests live in an additive
//! `#[cfg(test)]` module inside `merge.rs`
//! (`mod issue_823_complex_column_merge`).
//!
//! This external file asserts the STRUCTURAL state observable from the public
//! API. Originally (#823) the merge data model (`MergeEntry` / `RowData` /
//! `CellData`) had NO per-path / collection-key dimension at all.
//!
//! Issue #886 (Epic #842) added the plumbing — `CellData` now carries a
//! `cell_path` (and `local_deletion_time`) field — so per-path merge becomes
//! REPRESENTABLE. The behavior that USES it (per-path union, path-scoped complex
//! deletion #18/#14/#17) is NOT delivered by #886: the reader still surfaces a
//! whole non-frozen collection as ONE `CellData` with `cell_path == None`, so the
//! observable runtime shape below is unchanged. These tests pin both facts: the
//! field exists, and it is still defaulted `None` from the reader path.
//!
//! Run with:
//!   CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!     cargo test --package cqlite-core --features write-support \
//!     --test issue_823_complex_column_merge

#![cfg(feature = "write-support")]

use cqlite_core::storage::write_engine::merge::{CellData, RowData};
use cqlite_core::types::{UdtField, UdtValue, Value};

/// Issue #886 added the `cell_path` / `local_deletion_time` substrate fields to
/// `CellData`; epic #899 Phase C added the per-element discriminator
/// (`is_complex_element`), the authoritative `is_deleted` flag, and the on-disk
/// `has_empty_value` flag, and FLIPPED the production path: the compaction reader
/// now emits one per-element `CellData` (populated `cell_path` + per-element
/// metadata) for each non-frozen-collection element instead of one collapsed
/// whole-column cell. This structural test pins the exact public field set and
/// demonstrates a per-element cell round-tripping through the struct; if the
/// field set changes again it must be revisited. (The end-to-end reader-emit
/// assertions live in the `issue_899_per_element_merge` module inside merge.rs.)
#[test]
fn celldata_carries_per_element_substrate() {
    // A per-element cell as the Phase-C reader path now builds it: a populated
    // cell_path, per-element timestamp/ttl/ldt, and the per-element flags.
    let cell = CellData {
        column: "tags".to_string(),
        value: Value::Text("a".to_string()),
        timestamp: 1,
        ttl: Some(3600),
        cell_path: Some(vec![0x00, 0x01, 0xAB]),
        local_deletion_time: Some(1_700_000_000),
        is_complex_element: true,
        is_deleted: false,
        has_empty_value: true,
    };

    // Destructure to pin the exact public field set (a field add/remove breaks
    // this in lock-step with the merge→writer plumbing it feeds).
    let CellData {
        column,
        value,
        timestamp,
        ttl,
        cell_path,
        local_deletion_time,
        is_complex_element,
        is_deleted,
        has_empty_value,
    } = cell;
    assert_eq!(column, "tags");
    assert!(matches!(value, Value::Text(_)));
    assert_eq!(timestamp, 1);
    assert_eq!(ttl, Some(3600));
    assert_eq!(
        cell_path.as_deref(),
        Some(&[0x00, 0x01, 0xAB][..]),
        "per-element cell_path is now populated (Phase C)"
    );
    assert_eq!(local_deletion_time, Some(1_700_000_000));
    assert!(is_complex_element, "marks a complex element");
    assert!(
        !is_deleted,
        "an empty-value live element is NOT a tombstone"
    );
    assert!(has_empty_value, "SET-member on-disk emptiness preserved");
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
            is_complex_element: false,
            is_deleted: false,
            has_empty_value: false,
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
            is_complex_element: false,
            is_deleted: false,
            has_empty_value: false,
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
