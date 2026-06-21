//! Issue #886 (foundation for epic #842): enrich the reader→merge entry with the
//! substrate needed for byte-faithful per-cell / per-element reconciliation —
//! `CellData.cell_path` / `CellData.local_deletion_time`, and first-class
//! `MergeEntry.complex_deletions` / `MergeEntry.range_deletion` entities.
//!
//! Scope is **plumbing only**: these fields/entities are added and threaded so the
//! follow-ups can populate and consume them, but #886 itself neither populates nor
//! consumes them (the reader-emit + per-path reconcile work is #899). This test
//! therefore asserts the new substrate exists and round-trips through
//! construction, and that it is **carry-only** — `MergeEntry::new` defaults the new
//! entities to empty/`None`, and the merge data model is byte-neutral because
//! nothing reads them yet.
//!
//! Run with:
//!   CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!     cargo test --package cqlite-core --features write-support \
//!     --test issue_886_merge_entry_substrate

#![cfg(feature = "write-support")]

use cqlite_core::storage::write_engine::merge::{CellData, ComplexDeletion, MergeEntry, RowData};
use cqlite_core::storage::write_engine::mutation::{
    ClusteringBound, ClusteringKey, DecoratedKey, RangeTombstone,
};
use cqlite_core::types::Value;

/// `CellData` now carries the per-element substrate fields. They default to
/// `None` (the value the reader produces today) but can hold a real cell path and
/// local deletion time so #899 can populate them.
#[test]
fn celldata_carries_cell_path_and_local_deletion_time() {
    // Simple cell as built today: both new fields absent.
    let simple = CellData {
        column: "name".to_string(),
        value: Value::Text("v".to_string()),
        timestamp: 100,
        ttl: None,
        cell_path: None,
        local_deletion_time: None,
    };
    assert_eq!(simple.cell_path, None);
    assert_eq!(simple.local_deletion_time, None);

    // A per-element cell as #899 will eventually build it: round-trip the path and
    // local deletion time through the struct.
    let element = CellData {
        column: "tags".to_string(),
        value: Value::Text("element".to_string()),
        timestamp: 200,
        ttl: Some(3600),
        cell_path: Some(vec![0x00, 0x01, 0xAB]),
        local_deletion_time: Some(1_700_000_000),
    };
    assert_eq!(element.cell_path.as_deref(), Some(&[0x00, 0x01, 0xAB][..]));
    assert_eq!(element.local_deletion_time, Some(1_700_000_000));

    // Equality is structural over the new fields too: same column/value but a
    // different cell path is a distinct cell (the point of the substrate).
    let other_path = CellData {
        cell_path: Some(vec![0x00, 0x02]),
        ..element.clone()
    };
    assert_ne!(
        element, other_path,
        "cell_path participates in CellData identity"
    );
}

/// `MergeEntry::new` leaves the carry-only deletion entities empty/`None`, so
/// existing call sites and merge output are unchanged by #886.
#[test]
fn merge_entry_new_defaults_deletion_entities_empty() {
    let entry = MergeEntry::new(
        0,
        DecoratedKey::new(42, vec![1, 2, 3]),
        None,
        100,
        RowData::Live { cells: Vec::new() },
    );
    assert!(
        entry.complex_deletions.is_empty(),
        "complex_deletions default empty (carry-only)"
    );
    assert!(
        entry.range_deletion.is_none(),
        "range_deletion defaults None (carry-only)"
    );
}

/// The complex-deletion marker entity round-trips its column scope and timestamps,
/// and attaches to a `MergeEntry` via the builder without disturbing the row data.
#[test]
fn complex_deletion_entity_round_trips_on_merge_entry() {
    let cd = ComplexDeletion {
        column: "tags".to_string(),
        marked_for_delete_at: 12_345,
        local_deletion_time: 1_700_000_000,
    };

    let entry = MergeEntry::new(
        0,
        DecoratedKey::new(7, vec![9]),
        None,
        12_345,
        RowData::Live { cells: Vec::new() },
    )
    .with_complex_deletions(vec![cd.clone()]);

    assert_eq!(entry.complex_deletions, vec![cd]);
    assert_eq!(entry.complex_deletions[0].column, "tags");
    assert_eq!(entry.complex_deletions[0].marked_for_delete_at, 12_345);
    assert_eq!(
        entry.complex_deletions[0].local_deletion_time,
        1_700_000_000
    );
    // The row payload is untouched by the carry-only marker.
    assert!(matches!(entry.row_data, RowData::Live { .. }));
}

/// A first-class range-deletion entity (reusing the open-ended `RangeTombstone`
/// representation with `ClusteringBound`) flows onto the `MergeEntry` as a
/// carry-only slot, including the open-ended `Bottom`/`Top` bounds.
#[test]
fn range_deletion_entity_attaches_to_merge_entry() {
    let rt = RangeTombstone {
        start: ClusteringBound::Inclusive(ClusteringKey::new(vec![(
            "ck".to_string(),
            Value::Integer(1),
        )])),
        end: ClusteringBound::Top,
        deletion_time: 999,
        local_deletion_time: 1_700_000_001,
    };

    let entry = MergeEntry::new(
        0,
        DecoratedKey::new(7, vec![9]),
        None,
        999,
        RowData::Live { cells: Vec::new() },
    )
    .with_range_deletion(rt.clone());

    let got = entry.range_deletion.expect("range_deletion present");
    assert_eq!(got, rt);
    assert!(matches!(got.end, ClusteringBound::Top));
    assert_eq!(got.deletion_time, 999);
    assert_eq!(got.local_deletion_time, 1_700_000_001);
}
