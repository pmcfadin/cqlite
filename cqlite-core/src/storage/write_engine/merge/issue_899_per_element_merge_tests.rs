//! Moved out of `merge/mod.rs` unchanged (#4658); the module keeps its name via `#[path]`.

use super::*;
use crate::storage::sstable::reader::compaction_row::{
    CompactionRowData, ComplexColumn, ComplexElement,
};
use crate::types::Value;

fn dk(byte: u8) -> DecoratedKey {
    DecoratedKey::from_key_bytes(vec![byte]).expect("token")
}

fn element(path: &[u8], val: &str, ts: i64) -> ComplexElement {
    ComplexElement {
        cell_path: path.to_vec(),
        value: Some(Value::text(val.to_string())),
        decoded_key: None,
        timestamp: ts,
        ttl: None,
        local_deletion_time: None,
        is_deleted: false,
        has_empty_value: false,
    }
}

/// (a) PHASE C FLIP: a multi-cell collection now surfaces ONE per-element
/// `CellData` PER `ComplexElement` (populated cell_path + per-element
/// timestamp), NOT one collapsed whole-column cell. Each cell carries the
/// element's authoritative path/ts so per-`(column, cell_path)` reconcile and
/// the per-element writer emit are byte-faithful to Cassandra's multi-cell
/// layout.
#[test]
fn multi_cell_collection_surfaces_one_celldata_per_element() {
    let elements = vec![
        element(&[0xAA], "a", 100),
        element(&[0xBB], "b", 200),
        element(&[0xCC], "c", 300),
    ];
    assert_eq!(elements.len(), 3);

    // PHASE C: per-element emit (cell_path + per-element ts preserved); the
    // collapsed_value is no longer threaded to the writer (kept only for the
    // user-facing read contract).
    let row_data = CompactionRowData::Live {
        simple: vec![],
        complex: vec![ComplexColumn {
            column: "tags".to_string(),
            complex_deletion: None,
            elements,
            collapsed_value: Value::Set(vec![
                Value::text("a".to_string()),
                Value::text("b".to_string()),
                Value::text("c".to_string()),
            ]),
        }],
        row_deletion: None,
        row_liveness: Default::default(),
    };

    let (row, complex_deletions, _row_deletion, _row_liveness) =
        SSTableRowIteratorAdapter::compaction_row_data_to_row_data(row_data, 999);
    assert!(complex_deletions.is_empty(), "no complex deletion present");

    let cells = match row {
        RowData::Live { cells } => cells,
        other => panic!("expected Live, got {other:?}"),
    };
    assert_eq!(cells.len(), 3, "one CellData per element (Phase C flip)");
    for c in &cells {
        assert_eq!(c.column, "tags");
        assert!(c.is_complex_element, "each is a complex element");
        assert!(!c.is_deleted, "live elements");
        assert!(c.cell_path.is_some(), "per-element cell_path populated");
    }
    // The per-element cells keep their OWN timestamps (NOT promoted to the
    // row timestamp 999) — the fidelity gain Phase C delivers.
    let mut by_path: Vec<(Vec<u8>, i64, Value)> = cells
        .into_iter()
        .map(|c| (c.cell_path.expect("path"), c.timestamp, c.value))
        .collect();
    by_path.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(by_path[0], (vec![0xAA], 100, Value::text("a".to_string())));
    assert_eq!(by_path[1], (vec![0xBB], 200, Value::text("b".to_string())));
    assert_eq!(by_path[2], (vec![0xCC], 300, Value::text("c".to_string())));
}

/// (b) FOUNDATION: a real complex deletion reaches `MergeEntry.complex_deletions`
/// as a first-class `ComplexDeletion` (not a boolean), carried but not yet
/// consumed by the writer (Phase C).
#[test]
fn complex_deletion_reaches_merge_entry_complex_deletions() {
    let row_data = CompactionRowData::Live {
        simple: vec![],
        complex: vec![ComplexColumn {
            column: "tags".to_string(),
            complex_deletion: Some((12_345, 1_700_000_000)),
            elements: vec![element(&[0xAB], "x", 20_000)],
            collapsed_value: Value::Set(vec![Value::text("x".to_string())]),
        }],
        row_deletion: None,
        row_liveness: Default::default(),
    };

    let (_row, complex_deletions, _row_deletion, _row_liveness) =
        SSTableRowIteratorAdapter::compaction_row_data_to_row_data(row_data, 20_000);
    assert_eq!(complex_deletions.len(), 1);
    assert_eq!(complex_deletions[0].column, "tags");
    assert_eq!(complex_deletions[0].marked_for_delete_at, 12_345);
    assert_eq!(complex_deletions[0].local_deletion_time, 1_700_000_000);
}

/// (c) Per-`(column, cell_path)` reconcile keeps disjoint elements written to
/// the same column across two SSTables (the case the old whole-column key
/// collapsed to one survivor).
#[test]
fn reconcile_keeps_disjoint_elements_per_cell_path() {
    let newer = MergeEntry::new(
        0,
        dk(1),
        None,
        200,
        RowData::Live {
            cells: vec![CellData {
                column: "tags".to_string(),
                value: Value::text("b".to_string()),
                timestamp: 200,
                ttl: None,
                cell_path: Some(vec![0xBB]),
                local_deletion_time: None,
                is_complex_element: false,
                is_deleted: false,
                has_empty_value: false,
            }],
        },
    );
    let older = MergeEntry::new(
        1,
        dk(1),
        None,
        100,
        RowData::Live {
            cells: vec![CellData {
                column: "tags".to_string(),
                value: Value::text("a".to_string()),
                timestamp: 100,
                ttl: None,
                cell_path: Some(vec![0xAA]),
                local_deletion_time: None,
                is_complex_element: false,
                is_deleted: false,
                has_empty_value: false,
            }],
        },
    );

    let merged = KWayMerger::<NoTrace>::reconcile_cluster(
        None,
        vec![newer, older],
        &::std::collections::HashMap::new(),
        None,
    )
    .expect("a live row must be emitted");
    let cells = match merged.row_data {
        RowData::Live { cells } => cells,
        other => panic!("expected Live, got {other:?}"),
    };
    // BOTH elements survive (disjoint cell paths), unlike the old
    // whole-column-keyed collapse which kept only one.
    assert_eq!(cells.len(), 2, "disjoint elements both survive");
    let mut paths: Vec<Vec<u8>> = cells
        .iter()
        .map(|c| c.cell_path.clone().expect("cell_path"))
        .collect();
    paths.sort();
    assert_eq!(paths, vec![vec![0xAA], vec![0xBB]]);
}

/// ISSUE #887 (parity f66fa14f): a surviving complex deletion SHADOWS the
/// elements it covers BEFORE the marker is purged. An element whose timestamp
/// is `<= markedForDeleteAt` is shadowed; an element strictly newer survives.
/// The marker still rides along on `MergeEntry.complex_deletions` so the writer
/// can emit it (and the per-element survivors) faithfully.
#[test]
fn complex_deletion_shadows_covered_elements_before_purge() {
    let old_el = MergeEntry::new(
        1,
        dk(1),
        None,
        100,
        RowData::Live {
            cells: vec![CellData {
                column: "tags".to_string(),
                value: Value::text("old".to_string()),
                timestamp: 100,
                ttl: None,
                cell_path: Some(vec![0x01]),
                local_deletion_time: None,
                is_complex_element: true,
                is_deleted: false,
                has_empty_value: false,
            }],
        },
    );
    let new_el = MergeEntry::new(
        0,
        dk(1),
        None,
        300,
        RowData::Live {
            cells: vec![CellData {
                column: "tags".to_string(),
                value: Value::text("new".to_string()),
                timestamp: 300,
                ttl: None,
                cell_path: Some(vec![0x02]),
                local_deletion_time: None,
                is_complex_element: true,
                is_deleted: false,
                has_empty_value: false,
            }],
        },
    )
    .with_complex_deletions(vec![ComplexDeletion {
        column: "tags".to_string(),
        marked_for_delete_at: 200,
        local_deletion_time: 1_700_000_000,
    }]);

    let merged = KWayMerger::<NoTrace>::reconcile_cluster(
        None,
        vec![new_el, old_el],
        &::std::collections::HashMap::new(),
        None,
    )
    .expect("a live row must be emitted");

    // The complex deletion is carried so the writer can emit the marker...
    assert_eq!(
        merged.complex_deletions,
        vec![ComplexDeletion {
            column: "tags".to_string(),
            marked_for_delete_at: 200,
            local_deletion_time: 1_700_000_000,
        }],
        "complex deletion is carried on the MergeEntry"
    );

    // ...and reconcile shadows the covered element (ts 100 <= mfda 200) BEFORE
    // purge: only the strictly-newer element (ts 300 > mfda 200) survives.
    let cells = match merged.row_data {
        RowData::Live { cells } => cells,
        other => panic!("expected Live, got {other:?}"),
    };
    assert_eq!(
        cells.len(),
        1,
        "the covered element (ts<=mfda) is shadowed before purge (#887 f66fa14f)"
    );
    assert_eq!(
        cells[0].cell_path.as_deref(),
        Some([0x02].as_slice()),
        "the strictly-newer element (ts>mfda) survives"
    );
}

/// ACCEPTANCE #887 (1) — parity bd244649: when two SSTables carry complex
/// deletions on the SAME column at EQUAL `markedForDeleteAt`, the merged
/// deletion does NOT supersede the active one (only STRICTLY-greater
/// supersedes). At equal mfda the boundary is still `<=`, so an element at
/// exactly `mfda` is shadowed, but an element strictly newer survives — proving
/// the equal-ts deletions did not collapse into a stronger one.
#[test]
fn complex_deletion_equal_timestamps_do_not_supersede() {
    let covered = MergeEntry::new(
        1,
        dk(1),
        None,
        200,
        RowData::Live {
            cells: vec![CellData {
                column: "tags".to_string(),
                value: Value::text("at-mfda".to_string()),
                timestamp: 200,
                ttl: None,
                cell_path: Some(vec![0x01]),
                local_deletion_time: None,
                is_complex_element: true,
                is_deleted: false,
                has_empty_value: false,
            }],
        },
    )
    .with_complex_deletions(vec![ComplexDeletion {
        column: "tags".to_string(),
        marked_for_delete_at: 200,
        local_deletion_time: 1_700_000_100,
    }]);
    let survivor = MergeEntry::new(
        0,
        dk(1),
        None,
        300,
        RowData::Live {
            cells: vec![CellData {
                column: "tags".to_string(),
                value: Value::text("after".to_string()),
                timestamp: 300,
                ttl: None,
                cell_path: Some(vec![0x02]),
                local_deletion_time: None,
                is_complex_element: true,
                is_deleted: false,
                has_empty_value: false,
            }],
        },
    )
    // SAME column, EQUAL marked_for_delete_at — must NOT supersede the active.
    .with_complex_deletions(vec![ComplexDeletion {
        column: "tags".to_string(),
        marked_for_delete_at: 200,
        local_deletion_time: 1_700_000_000,
    }]);

    let merged = KWayMerger::<NoTrace>::reconcile_cluster(
        None,
        vec![survivor, covered],
        &::std::collections::HashMap::new(),
        None,
    )
    .expect("a live row must be emitted");

    // The deletion at mfda=200 survives (one carried marker for `tags`)...
    let tags_dels: Vec<&ComplexDeletion> = merged
        .complex_deletions
        .iter()
        .filter(|d| d.column == "tags")
        .collect();
    assert_eq!(
        tags_dels.len(),
        1,
        "the union keeps one complex deletion for the column"
    );
    assert_eq!(
        tags_dels[0].marked_for_delete_at, 200,
        "equal-ts deletions do not produce a stronger mfda (bd244649)"
    );

    // ...the strictly-newer element (ts 300 > 200) survives; the at-mfda element
    // (ts 200 <= 200) is shadowed.
    let cells = match merged.row_data {
        RowData::Live { cells } => cells,
        other => panic!("expected Live, got {other:?}"),
    };
    assert_eq!(
        cells.len(),
        1,
        "only the strictly-newer element survives the active deletion"
    );
    assert_eq!(
        cells[0].cell_path.as_deref(),
        Some([0x02].as_slice()),
        "ts>mfda element survives; equal-ts deletions did not shadow it"
    );
}

/// ACCEPTANCE #887 (2) — parity bd244649 + f66fa14f: a STRICTLY-superseding
/// complex deletion shadows every covered element (ts <= mfda) BEFORE the marker
/// is purged, so a later purge cannot resurrect them. An element with ts STRICTLY
/// GREATER than the surviving mfda MUST survive.
#[test]
fn complex_deletion_strict_supersede_shadows_before_purge() {
    // Older source: weaker deletion (mfda 100) + element at ts 50 (covered by
    // the strong deletion below) + element at ts 500 (survives everything).
    let weak = MergeEntry::new(
        1,
        dk(1),
        None,
        500,
        RowData::Live {
            cells: vec![
                CellData {
                    column: "tags".to_string(),
                    value: Value::text("ancient".to_string()),
                    timestamp: 50,
                    ttl: None,
                    cell_path: Some(vec![0x01]),
                    local_deletion_time: None,
                    is_complex_element: true,
                    is_deleted: false,
                    has_empty_value: false,
                },
                CellData {
                    column: "tags".to_string(),
                    value: Value::text("survivor".to_string()),
                    timestamp: 500,
                    ttl: None,
                    cell_path: Some(vec![0x03]),
                    local_deletion_time: None,
                    is_complex_element: true,
                    is_deleted: false,
                    has_empty_value: false,
                },
            ],
        },
    )
    .with_complex_deletions(vec![ComplexDeletion {
        column: "tags".to_string(),
        marked_for_delete_at: 100,
        local_deletion_time: 1_700_000_000,
    }]);
    // Newer source: STRONG deletion (mfda 300, strictly > 100) + element at ts
    // 300 (== strong mfda → covered).
    let strong = MergeEntry::new(
        0,
        dk(1),
        None,
        300,
        RowData::Live {
            cells: vec![CellData {
                column: "tags".to_string(),
                value: Value::text("covered".to_string()),
                timestamp: 300,
                ttl: None,
                cell_path: Some(vec![0x02]),
                local_deletion_time: None,
                is_complex_element: true,
                is_deleted: false,
                has_empty_value: false,
            }],
        },
    )
    .with_complex_deletions(vec![ComplexDeletion {
        column: "tags".to_string(),
        marked_for_delete_at: 300,
        local_deletion_time: 1_700_000_200,
    }]);

    let merged = KWayMerger::<NoTrace>::reconcile_cluster(
        None,
        vec![strong, weak],
        &::std::collections::HashMap::new(),
        None,
    )
    .expect("a live row must be emitted");

    // The strong deletion (mfda 300) strictly supersedes the weak one (100).
    let tags_dels: Vec<&ComplexDeletion> = merged
        .complex_deletions
        .iter()
        .filter(|d| d.column == "tags")
        .collect();
    assert_eq!(
        tags_dels.len(),
        1,
        "one surviving complex deletion for tags"
    );
    assert_eq!(
        tags_dels[0].marked_for_delete_at, 300,
        "the strictly-greater mfda supersedes (bd244649)"
    );

    // Covered elements (ts 50 and ts 300, both <= 300) are shadowed BEFORE purge;
    // only the ts 500 element (strictly > 300) survives (f66fa14f).
    let cells = match merged.row_data {
        RowData::Live { cells } => cells,
        other => panic!("expected Live, got {other:?}"),
    };
    assert_eq!(
        cells.len(),
        1,
        "all elements with ts<=mfda are shadowed before purge (no resurrection)"
    );
    assert_eq!(
        cells[0].cell_path.as_deref(),
        Some([0x03].as_slice()),
        "the strictly-newer element (ts>mfda) survives"
    );
    assert_eq!(cells[0].timestamp, 500);
}

/// ISSUE #887: a row that reduces to a ROW TOMBSTONE at `row_del = T_low` must
/// STILL emit any carried complex-deletion marker whose `marked_for_delete_at =
/// T_high` is STRICTLY GREATER than `row_del`.
///
/// A row tombstone shadows only `timestamp <= row_del`. A complex deletion at
/// `mfda = T_high > T_low` covers elements in `(T_low, T_high]` — including
/// elements that live in OTHER SSTables NOT part of this compaction. If the
/// marker is dropped, those elements would be RESURRECTED. So the emitted
/// mutation must carry BOTH the `DeleteRow` AND the `ComplexDeletion{T_high}`.
#[test]
fn row_tombstone_keeps_strictly_newer_complex_deletion_marker() {
    use crate::schema::{Column, KeyColumn, TableSchema};
    use crate::storage::write_engine::mutation::CellOperation;
    use std::collections::HashMap;

    const T_LOW: i64 = 100; // row tombstone (row_del)
    const T_HIGH: i64 = 300; // complex deletion mfda (strictly > row_del)

    // Source A (newest, run 0): a ROW TOMBSTONE at T_low.
    let row_tomb = MergeEntry::new(
        0,
        dk(1),
        None,
        T_LOW,
        RowData::Tombstone {
            deletion_time: T_LOW,
            local_deletion_time: 0,
        },
    );

    // Source B (older, run 1): carries the STRONGER complex deletion (mfda
    // T_high > row_del) plus three elements (shadowed-by-row, shadowed-by-complex,
    // survivor).
    let carrier = MergeEntry::new(
        1,
        dk(1),
        None,
        T_HIGH + 200,
        RowData::Live {
            cells: vec![
                CellData {
                    column: "tags".to_string(),
                    value: Value::text("shadowed_by_row".to_string()),
                    timestamp: T_LOW,
                    ttl: None,
                    cell_path: Some(vec![0x01]),
                    local_deletion_time: None,
                    is_complex_element: true,
                    is_deleted: false,
                    has_empty_value: false,
                },
                CellData {
                    column: "tags".to_string(),
                    value: Value::text("shadowed_by_complex".to_string()),
                    timestamp: T_HIGH,
                    ttl: None,
                    cell_path: Some(vec![0x02]),
                    local_deletion_time: None,
                    is_complex_element: true,
                    is_deleted: false,
                    has_empty_value: false,
                },
                CellData {
                    column: "tags".to_string(),
                    value: Value::text("survivor".to_string()),
                    timestamp: T_HIGH + 200,
                    ttl: None,
                    cell_path: Some(vec![0x03]),
                    local_deletion_time: None,
                    is_complex_element: true,
                    is_deleted: false,
                    has_empty_value: false,
                },
            ],
        },
    )
    .with_complex_deletions(vec![ComplexDeletion {
        column: "tags".to_string(),
        marked_for_delete_at: T_HIGH,
        local_deletion_time: 1_700_000_000,
    }]);

    let merged = KWayMerger::<NoTrace>::reconcile_cluster(
        None,
        vec![row_tomb, carrier],
        &::std::collections::HashMap::new(),
        None,
    )
    .expect("a row must be emitted (it carries a row tombstone + survivor)");

    // The strictly-newer element (ts T_high+200) survives both deletions; the
    // ts<=T_low and T_low<ts<=T_high elements are shadowed BEFORE purge.
    let cells = match &merged.row_data {
        RowData::Live { cells } => cells.clone(),
        other => panic!("expected Live (survivor present), got {other:?}"),
    };
    assert_eq!(
        cells.len(),
        1,
        "only the element with ts > T_high survives the complex deletion"
    );
    assert_eq!(cells[0].cell_path.as_deref(), Some([0x03].as_slice()));
    assert_eq!(cells[0].timestamp, T_HIGH + 200);

    // The strictly-newer complex deletion marker is preserved on the merge entry.
    let tags_dels: Vec<&ComplexDeletion> = merged
        .complex_deletions
        .iter()
        .filter(|d| d.column == "tags")
        .collect();
    assert_eq!(tags_dels.len(), 1, "the T_high complex deletion is carried");
    assert_eq!(tags_dels[0].marked_for_delete_at, T_HIGH);

    // Schema with a non-frozen complex column `tags`.
    let schema = TableSchema {
        keyspace: "ks".to_string(),
        table: "t".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![Column {
            name: "tags".to_string(),
            data_type: "list<text>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        }],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    // The crux: `merge_entry_to_mutation` MUST emit BOTH a `DeleteRow` AND the
    // strictly-newer `ComplexDeletion` (mfda = T_high). `merge_entry_to_mutation`
    // decodes the partition key against the schema's `int` PK, so the key must be
    // a 4-byte big-endian int.
    let int_pk = DecoratedKey::new(1, 1i32.to_be_bytes().to_vec());
    let tomb_entry = MergeEntry::new(
        0,
        int_pk,
        None,
        T_LOW,
        RowData::Tombstone {
            deletion_time: T_LOW,
            local_deletion_time: 0,
        },
    )
    .with_complex_deletions(vec![ComplexDeletion {
        column: "tags".to_string(),
        marked_for_delete_at: T_HIGH,
        local_deletion_time: 1_700_000_000,
    }]);

    let mutation = KWayMerger::<NoTrace>::merge_entry_to_mutation(tomb_entry, &schema)
        .expect("conversion should succeed");

    let has_delete_row = mutation
        .operations
        .iter()
        .any(|op| matches!(op, CellOperation::DeleteRow));
    assert!(has_delete_row, "the row tombstone must still be emitted");

    let strictly_newer_marker = mutation.operations.iter().any(|op| {
        matches!(
            op,
            CellOperation::ComplexDeletion {
                column,
                marked_for_delete_at,
                ..
            } if column == "tags" && *marked_for_delete_at == T_HIGH
        )
    });
    assert!(
        strictly_newer_marker,
        "the strictly-newer (mfda > row_del) complex deletion marker must be \
             emitted alongside the row tombstone (else (row_del, mfda] resurrects)"
    );
}

/// ISSUE #887, complement: a carried complex-deletion marker that is FULLY
/// COVERED by the row tombstone (`mfda <= row_del`) IS dropped — the row
/// tombstone already shadows its entire range. Mirrors #887's strict boundary
/// (equal does not supersede).
#[test]
fn row_tombstone_drops_fully_covered_complex_deletion_marker() {
    use crate::schema::{Column, KeyColumn, TableSchema};
    use crate::storage::write_engine::mutation::CellOperation;
    use std::collections::HashMap;

    const ROW_DEL: i64 = 300;

    let schema = TableSchema {
        keyspace: "ks".to_string(),
        table: "t".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![Column {
            name: "tags".to_string(),
            data_type: "list<text>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        }],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let int_pk = DecoratedKey::new(1, 1i32.to_be_bytes().to_vec());
    for covered_mfda in [ROW_DEL - 100, ROW_DEL] {
        let entry = MergeEntry::new(
            0,
            int_pk.clone(),
            None,
            ROW_DEL,
            RowData::Tombstone {
                deletion_time: ROW_DEL,
                local_deletion_time: 0,
            },
        )
        .with_complex_deletions(vec![ComplexDeletion {
            column: "tags".to_string(),
            marked_for_delete_at: covered_mfda,
            local_deletion_time: 1_700_000_000,
        }]);

        let mutation = KWayMerger::<NoTrace>::merge_entry_to_mutation(entry, &schema)
            .expect("conversion should succeed");

        assert!(
            mutation
                .operations
                .iter()
                .all(|op| !matches!(op, CellOperation::ComplexDeletion { .. })),
            "a marker with mfda ({covered_mfda}) <= row_del ({ROW_DEL}) is fully \
                 covered by the row tombstone and must be dropped (strict boundary)"
        );
        assert!(
            matches!(mutation.operations.as_slice(), [CellOperation::DeleteRow]),
            "the fully-covered case reduces to exactly [DeleteRow]"
        );
    }
}
