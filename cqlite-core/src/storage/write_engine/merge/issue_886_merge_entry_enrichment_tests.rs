//! Moved out of `merge/mod.rs` unchanged (#4658); the module keeps its name via `#[path]`.

use super::*;
use crate::storage::write_engine::mutation::ClusteringBound;
use crate::types::Value;

fn dk(byte: u8) -> DecoratedKey {
    DecoratedKey::from_key_bytes(vec![byte]).expect("token")
}

/// The new `CellData` constructor defaults the enriched fields to `None`,
/// matching what the reader currently supplies.
#[test]
fn celldata_new_defaults_enriched_fields_to_none() {
    let cell = CellData::new("c".to_string(), Value::Integer(7), 100);
    assert_eq!(cell.ttl, None);
    assert_eq!(
        cell.local_deletion_time, None,
        "LDT defaults None (plumbing)"
    );
    assert_eq!(cell.cell_path, None, "cell_path defaults None (plumbing)");
}

/// The enriched fields round-trip through `MergeEntry`/`CellData` clone +
/// equality unchanged (proves they are real, carried state).
#[test]
fn enriched_celldata_round_trips_through_merge_entry() {
    let cell = CellData {
        column: "m".to_string(),
        value: Value::text("v".to_string()),
        timestamp: 500,
        ttl: Some(3600),
        local_deletion_time: Some(1_700_000_000),
        cell_path: Some(vec![0x00, 0x01]),
        is_complex_element: false,
        is_deleted: false,
        has_empty_value: false,
    };
    let entry = MergeEntry::new(
        0,
        dk(1),
        None,
        500,
        RowData::Live {
            cells: vec![cell.clone()],
        },
    );
    let cloned = entry.clone();
    let cells = match cloned.row_data {
        RowData::Live { cells } => cells,
        other => panic!("expected Live, got {other:?}"),
    };
    assert_eq!(cells.len(), 1);
    assert_eq!(cells[0].local_deletion_time, Some(1_700_000_000));
    assert_eq!(cells[0].cell_path, Some(vec![0x00, 0x01]));
    assert_eq!(cells[0].ttl, Some(3600));
    assert_eq!(cells[0], cell, "enriched cell survives clone + equality");
}

/// The reader→merge adapter (`value_to_row_data`) populates the enriched
/// fields. Today the reader's `(RowKey, Value, ts)` compaction stream does
/// not surface per-cell ttl / LDT / cell-path, so they are threaded as
/// `None` — but the fields exist on every produced cell (plumbing present).
#[test]
fn value_to_row_data_threads_enriched_fields_as_none() {
    // Map case: top-level columns surfaced as a map (key = column name).
    let map = Value::Map(vec![(
        Value::text("name".to_string()),
        Value::text("alice".to_string()),
    )]);
    let row_data = SSTableRowIteratorAdapter::value_to_row_data(&map, 100)
        .expect("value_to_row_data must succeed");
    let cells = match row_data {
        RowData::Live { cells } => cells,
        other => panic!("expected Live, got {other:?}"),
    };
    assert_eq!(cells.len(), 1);
    assert_eq!(cells[0].column, "name");
    assert_eq!(cells[0].timestamp, 100, "live cell inherits row ts (#533)");
    assert_eq!(cells[0].local_deletion_time, None);
    assert_eq!(cells[0].cell_path, None);
    assert_eq!(cells[0].ttl, None);

    // Single-value case wraps as one "value" cell with the same defaults.
    let single = SSTableRowIteratorAdapter::value_to_row_data(&Value::Integer(42), 200)
        .expect("value_to_row_data must succeed");
    let cells = match single {
        RowData::Live { cells } => cells,
        other => panic!("expected Live, got {other:?}"),
    };
    assert_eq!(cells[0].local_deletion_time, None);
    assert_eq!(cells[0].cell_path, None);
}

/// A `MergeEntry` can carry a first-class complex deletion. The builder
/// attaches it; the field is non-empty but reconciliation ignores it.
#[test]
fn merge_entry_carries_complex_deletion_without_acting_on_it() {
    let complex = ComplexDeletion {
        column: "tags".to_string(),
        marked_for_delete_at: 1234,
        local_deletion_time: 1_700_000_000,
    };
    let entry = MergeEntry::new(0, dk(1), None, 100, RowData::Live { cells: vec![] })
        .with_complex_deletions(vec![complex.clone()]);
    assert_eq!(entry.complex_deletions, vec![complex]);

    // Plumbing-only: reconcile_cluster does NOT consult complex_deletions.
    // A cell written at the same ts as the complex deletion still survives,
    // proving no shadowing behavior was introduced (#844 owns that).
    let cell = CellData::new("tags".to_string(), Value::text("a".to_string()), 1234);
    let live = MergeEntry::new(0, dk(1), None, 1234, RowData::Live { cells: vec![cell] })
        .with_complex_deletions(vec![ComplexDeletion {
            column: "tags".to_string(),
            marked_for_delete_at: 1234,
            local_deletion_time: 1_700_000_000,
        }]);
    let merged = KWayMerger::<NoTrace>::reconcile_cluster(
        None,
        vec![live],
        &::std::collections::HashMap::new(),
        None,
    )
    .expect("live row must be emitted");
    match merged.row_data {
        RowData::Live { cells } => {
            assert_eq!(
                cells.len(),
                1,
                "complex deletion must NOT shadow (plumbing)"
            );
            assert_eq!(cells[0].column, "tags");
        }
        other => panic!("expected Live, got {other:?}"),
    }
}

/// A `MergeEntry` can carry a first-class range deletion (reusing the shared
/// open-ended `TombstoneInfo`). The field is populated but reconciliation
/// ignores it — covered cells are NOT shadowed by this issue (#846 owns it).
#[test]
fn merge_entry_carries_range_deletion_without_acting_on_it() {
    // Open-ended range tombstone: both bounds None (the representation the
    // tombstone_merger open-ended utilities use).
    let range = RangeTombstone {
        start: ClusteringBound::Bottom,
        end: ClusteringBound::Top,
        deletion_time: 5000,
        local_deletion_time: 0,
    };
    let cell = CellData::new("v".to_string(), Value::Integer(1), 1000);
    let entry = MergeEntry::new(0, dk(1), None, 1000, RowData::Live { cells: vec![cell] })
        .with_range_deletion(range.clone());
    assert_eq!(entry.range_deletion, Some(range));

    // Plumbing-only: the covered cell (ts=1000 < range ts=5000) STILL
    // survives reconcile because range deletions are not yet applied.
    let merged = KWayMerger::<NoTrace>::reconcile_cluster(
        None,
        vec![entry],
        &::std::collections::HashMap::new(),
        None,
    )
    .expect("live row must be emitted");
    match merged.row_data {
        RowData::Live { cells } => {
            assert_eq!(
                cells.len(),
                1,
                "range deletion must NOT shadow covered cell (plumbing)"
            );
        }
        other => panic!("expected Live, got {other:?}"),
    }
}

/// `reconcile_cluster` must PRESERVE the carried complex/range deletion
/// metadata from its input rows onto the returned entry (#886 plumbing
/// preservation). Without this, the metadata threaded by the reader is
/// silently dropped before downstream consumers (#899) can see it. The
/// preservation is behavior-neutral: the surviving cells (normal reconcile
/// output) are identical to the no-metadata case.
#[test]
fn reconcile_cluster_preserves_carried_deletion_metadata() {
    let complex_a = ComplexDeletion {
        column: "tags".to_string(),
        marked_for_delete_at: 1234,
        local_deletion_time: 1_700_000_000,
    };
    let complex_b = ComplexDeletion {
        column: "notes".to_string(),
        marked_for_delete_at: 999,
        local_deletion_time: 1_700_000_001,
    };
    let range_low = RangeTombstone {
        start: ClusteringBound::Bottom,
        end: ClusteringBound::Top,
        deletion_time: 3000,
        local_deletion_time: 0,
    };
    let range_high = RangeTombstone {
        start: ClusteringBound::Bottom,
        end: ClusteringBound::Top,
        deletion_time: 7000,
        local_deletion_time: 0,
    };

    // Two input rows in the same cluster, each carrying distinct metadata.
    let row0 = MergeEntry::new(
        0,
        dk(1),
        None,
        2000,
        RowData::Live {
            cells: vec![CellData::new("v".to_string(), Value::Integer(2), 2000)],
        },
    )
    .with_complex_deletions(vec![complex_a.clone()])
    .with_range_deletion(range_low.clone());
    let row1 = MergeEntry::new(
        1,
        dk(1),
        None,
        1000,
        RowData::Live {
            cells: vec![CellData::new("w".to_string(), Value::Integer(1), 1000)],
        },
    )
    .with_complex_deletions(vec![complex_a.clone(), complex_b.clone()])
    .with_range_deletion(range_high.clone());

    let merged = KWayMerger::<NoTrace>::reconcile_cluster(
        None,
        vec![row0, row1],
        &::std::collections::HashMap::new(),
        None,
    )
    .expect("live row must be emitted");

    // complex_deletions: PHASE A NEUTRALITY (roborev #863) — accumulated as a
    // simple first-seen union, deduplicated. reconcile does NOT act on them
    // (no strict-supersede, no shadowing); that is Phase C. row0 contributes
    // "tags" first, then row1 adds "notes" (its "tags" is a dup).
    assert_eq!(
        merged.complex_deletions,
        vec![complex_a.clone(), complex_b.clone()],
        "complex deletions: first-seen union, deduplicated (Phase A neutral)"
    );
    // range_deletion: the highest deletion timestamp wins.
    assert_eq!(
        merged.range_deletion,
        Some(range_high),
        "range deletion with the highest deletion timestamp must be carried"
    );

    // Behavior-neutral: normal reconcile output (surviving cells) is unchanged
    // versus the same inputs with NO carried metadata.
    let plain0 = MergeEntry::new(
        0,
        dk(1),
        None,
        2000,
        RowData::Live {
            cells: vec![CellData::new("v".to_string(), Value::Integer(2), 2000)],
        },
    );
    let plain1 = MergeEntry::new(
        1,
        dk(1),
        None,
        1000,
        RowData::Live {
            cells: vec![CellData::new("w".to_string(), Value::Integer(1), 1000)],
        },
    );
    let plain = KWayMerger::<NoTrace>::reconcile_cluster(
        None,
        vec![plain0, plain1],
        &::std::collections::HashMap::new(),
        None,
    )
    .expect("live row must be emitted");
    assert_eq!(
        merged.row_data, plain.row_data,
        "carrying deletion metadata must not change surviving-cell output"
    );
    assert_eq!(merged.timestamp, plain.timestamp);
    assert!(plain.complex_deletions.is_empty());
    assert_eq!(plain.range_deletion, None);
}

/// A row-tombstone-only cluster (no surviving cells) must still carry the
/// metadata onto the emitted tombstone entry.
#[test]
fn reconcile_cluster_preserves_metadata_on_tombstone_entry() {
    let complex = ComplexDeletion {
        column: "tags".to_string(),
        marked_for_delete_at: 10,
        local_deletion_time: 1_700_000_000,
    };
    let row = MergeEntry::new(
        0,
        dk(1),
        None,
        500,
        RowData::Tombstone {
            deletion_time: 500,
            local_deletion_time: 0,
        },
    )
    .with_complex_deletions(vec![complex.clone()]);

    let merged = KWayMerger::<NoTrace>::reconcile_cluster(
        None,
        vec![row],
        &::std::collections::HashMap::new(),
        None,
    )
    .expect("row tombstone must be emitted");
    assert!(matches!(merged.row_data, RowData::Tombstone { .. }));
    assert_eq!(merged.complex_deletions, vec![complex]);
}

/// Regression (#853/#886 branch-review, Finding 3): a cluster carrying complex
/// and/or range deletion metadata but with NO surviving cells and NO row
/// tombstone must STILL emit a metadata-only entry so the carried deletion
/// metadata survives reconciliation. Previously `built` was `None` for this
/// case and the metadata was silently dropped before the preservation logic
/// could run.
#[test]
fn reconcile_cluster_emits_metadata_only_entry_when_no_row_produced() {
    let complex = ComplexDeletion {
        column: "tags".to_string(),
        marked_for_delete_at: 4242,
        local_deletion_time: 1_700_000_000,
    };
    let range = RangeTombstone {
        start: ClusteringBound::Bottom,
        end: ClusteringBound::Top,
        deletion_time: 8888,
        local_deletion_time: 0,
    };

    // Empty Live row (no cells), no row tombstone, but carrying both kinds of
    // deletion metadata.
    let row = MergeEntry::new(0, dk(1), None, 0, RowData::Live { cells: vec![] })
        .with_complex_deletions(vec![complex.clone()])
        .with_range_deletion(range.clone());

    let merged = KWayMerger::<NoTrace>::reconcile_cluster(
        None,
        vec![row],
        &::std::collections::HashMap::new(),
        None,
    )
    .expect("metadata-only cluster must still emit an entry");

    // The emitted entry carries the metadata and has no live cells.
    match &merged.row_data {
        RowData::Live { cells } => {
            assert!(
                cells.is_empty(),
                "metadata-only entry must have no live cells"
            );
        }
        other => panic!("expected empty Live, got {other:?}"),
    }
    assert_eq!(merged.complex_deletions, vec![complex]);
    assert_eq!(merged.range_deletion, Some(range));
}

/// Behavior-neutral guard for Finding 3: an empty `Live` cluster with NO
/// carried metadata and NO row tombstone must STILL collapse to `None`
/// (nothing is emitted). The metadata-only path only adds an entry when
/// deletion metadata would otherwise be lost.
#[test]
fn reconcile_cluster_empty_live_without_metadata_yields_none() {
    let row = MergeEntry::new(0, dk(1), None, 0, RowData::Live { cells: vec![] });
    assert!(
        KWayMerger::<NoTrace>::reconcile_cluster(
            None,
            vec![row],
            &::std::collections::HashMap::new(),
            None
        )
        .is_none(),
        "empty live row with no metadata must not emit an entry"
    );
}

/// Epic #899 Phase C: a COMPLEX-DELETION-only synthetic entry (empty `Live`
/// row carrying a `ComplexDeletion`, no ops, no row tombstone) must NO LONGER
/// be skipped — the writer now emits a real complex-deletion marker for it
/// (`merge_entry_to_mutation` produces a `CellOperation::ComplexDeletion`), so
/// a fully-deleted-collection marker reaches the SSTable instead of being
/// dropped. Issue #933: a RANGE-deletion-only carrier is ALSO now writer-
/// visible (the writer emits its bound markers), so it too is no longer skipped.
#[test]
fn complex_deletion_only_entry_reaches_writer_but_range_only_is_skipped() {
    let complex = ComplexDeletion {
        column: "tags".to_string(),
        marked_for_delete_at: 4242,
        local_deletion_time: 1_700_000_000,
    };
    let range = RangeTombstone {
        start: ClusteringBound::Bottom,
        end: ClusteringBound::Top,
        deletion_time: 8888,
        local_deletion_time: 0,
    };

    // Complex-deletion-only: now WRITER-VISIBLE (not a no-op) — the marker
    // must be emitted, not dropped.
    let complex_only = MergeEntry::new(0, dk(1), None, 0, RowData::Live { cells: vec![] })
        .with_complex_deletions(vec![complex.clone()]);
    assert!(
        !complex_only.is_metadata_only_no_op(),
        "Phase C: a complex-deletion-only entry must reach the writer"
    );

    // Range-deletion-only (no complex deletion): now WRITER-VISIBLE (#933) —
    // `merge_entry_to_mutation` threads it onto the mutation's range_tombstones
    // so the writer emits the on-disk bound markers; dropping it would
    // resurrect the rows it shadowed.
    let range_only = MergeEntry::new(0, dk(1), None, 0, RowData::Live { cells: vec![] })
        .with_range_deletion(range.clone());
    assert!(
        !range_only.is_metadata_only_no_op(),
        "#933: a range-deletion-only carrier must reach the writer"
    );

    // Both kinds present → the complex deletion is writer-visible, so the
    // entry is NOT skipped (the marker must survive).
    let both = MergeEntry::new(0, dk(1), None, 0, RowData::Live { cells: vec![] })
        .with_complex_deletions(vec![complex])
        .with_range_deletion(range);
    assert!(
        !both.is_metadata_only_no_op(),
        "a carried complex deletion keeps the entry writer-visible"
    );

    // The complex-deletion-only entry survives reconciliation AND stays
    // writer-visible end-to-end.
    let reconciled = KWayMerger::<NoTrace>::reconcile_cluster(
        None,
        vec![complex_only],
        &::std::collections::HashMap::new(),
        None,
    )
    .expect("complex-deletion-only cluster must still emit an entry");
    assert!(
        !reconciled.is_metadata_only_no_op(),
        "the reconciled complex-deletion entry must reach the writer"
    );
}

/// Guard the boundary of the skip predicate: entries that carry real content
/// (live cells, a row tombstone, or no carried metadata) must NOT be treated
/// as metadata-only no-ops, so genuine rows are never dropped on the writer
/// path.
#[test]
fn non_metadata_only_entries_are_not_skipped() {
    // Live row with a real cell — must be written.
    let live = MergeEntry::new(
        0,
        dk(1),
        None,
        100,
        RowData::Live {
            cells: vec![CellData::new(
                "name".to_string(),
                Value::text("v".to_string()),
                100,
            )],
        },
    );
    assert!(!live.is_metadata_only_no_op());

    // Empty live row WITHOUT any metadata: a true phantom no-op. It never
    // survives reconcile in practice, but the defensive filter DOES treat it as
    // skippable (#933 redefinition: no cells, no complex, no range, no row
    // deletion).
    let empty_no_meta = MergeEntry::new(0, dk(1), None, 0, RowData::Live { cells: vec![] });
    assert!(empty_no_meta.is_metadata_only_no_op());

    // Row tombstone — must be written (real deletion).
    let tomb = MergeEntry::new(
        0,
        dk(1),
        None,
        500,
        RowData::Tombstone {
            deletion_time: 500,
            local_deletion_time: 0,
        },
    )
    .with_range_deletion(RangeTombstone {
        start: ClusteringBound::Bottom,
        end: ClusteringBound::Top,
        deletion_time: 8888,
        local_deletion_time: 0,
    });
    assert!(
        !tomb.is_metadata_only_no_op(),
        "a row tombstone is real content even when carrying range metadata"
    );
}

/// Equality of two default merge entries is unaffected by the new carried
/// fields (both empty/None), so existing assert_eq!-based tests stay valid.
#[test]
fn default_carried_fields_preserve_merge_entry_equality() {
    let a = MergeEntry::new(0, dk(1), None, 100, RowData::Live { cells: vec![] });
    let b = MergeEntry::new(0, dk(1), None, 100, RowData::Live { cells: vec![] });
    assert_eq!(a, b);
    assert!(a.complex_deletions.is_empty());
    assert_eq!(a.range_deletion, None);
}
