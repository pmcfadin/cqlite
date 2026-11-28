//! Moved out of `merge/mod.rs` unchanged (#4658); the module keeps its name via `#[path]`.

use super::*;
use crate::types::{TombstoneInfo, TombstoneType, UdtField, UdtValue, Value};

fn dk(byte: u8) -> DecoratedKey {
    DecoratedKey::from_key_bytes(vec![byte]).expect("token")
}

fn live(run_index: usize, row_ts: i64, cells: Vec<CellData>) -> MergeEntry {
    MergeEntry::new(run_index, dk(1), None, row_ts, RowData::Live { cells })
}

fn scalar_cell(column: &str, value: &str, ts: i64) -> CellData {
    CellData::new(column.to_string(), Value::text(value.to_string()), ts)
}

// ── Issue #847: dropped-column cell filtering during compaction ──────────
// A column dropped at drop_time discards cells with `timestamp <= drop_time`;
// cells written after the drop (re-added) survive. The drop time comes from
// the `dropped_columns` map (#904). `cell.timestamp` is the cell's OWN
// writetime (#886/#899 enrichment), so purging is exact per cell even when
// sibling cells in the same row carry different timestamps (#922 — end-to-end
// coverage in `tests/issue_922_per_cell_dropped_purge.rs`).

/// A cell of a dropped column written at/before the drop time is filtered; a
/// sibling column with no drop entry is untouched.
#[test]
fn dropped_column_cell_at_or_before_drop_time_is_filtered() {
    let row = live(
        0,
        200,
        vec![
            scalar_cell("name", "alice", 100),
            scalar_cell("legacy", "stale", 100),
        ],
    );
    let mut dropped = ::std::collections::HashMap::new();
    dropped.insert("legacy".to_string(), 150); // cell ts=100 <= 150

    let merged = KWayMerger::<NoTrace>::reconcile_cluster(None, vec![row], &dropped, None)
        .expect("a live row must be emitted (name survives)");
    let cells = match merged.row_data {
        RowData::Live { cells } => cells,
        other => panic!("expected Live, got {:?}", other),
    };
    assert_eq!(cells.len(), 1, "the dropped-column cell must be discarded");
    assert_eq!(cells[0].column, "name");
}

/// A cell written strictly after the drop time survives the reconcile filter.
#[test]
fn dropped_column_cell_after_drop_time_survives() {
    let row = live(0, 200, vec![scalar_cell("legacy", "fresh", 200)]);
    let mut dropped = ::std::collections::HashMap::new();
    dropped.insert("legacy".to_string(), 150); // cell ts=200 > 150

    let merged = KWayMerger::<NoTrace>::reconcile_cluster(None, vec![row], &dropped, None)
        .expect("a live row must be emitted (cell post-dates the drop)");
    let cells = match merged.row_data {
        RowData::Live { cells } => cells,
        other => panic!("expected Live, got {:?}", other),
    };
    assert_eq!(cells.len(), 1);
    assert_eq!(cells[0].column, "legacy");
    assert_eq!(cells[0].value, Value::text("fresh".to_string()));
}

/// Equal timestamp (cell ts == drop_time) is discarded — the `<=` boundary.
#[test]
fn dropped_column_cell_at_exact_drop_time_is_filtered() {
    let row = live(0, 150, vec![scalar_cell("legacy", "edge", 150)]);
    let mut dropped = ::std::collections::HashMap::new();
    dropped.insert("legacy".to_string(), 150);

    assert!(
        KWayMerger::<NoTrace>::reconcile_cluster(None, vec![row], &dropped, None).is_none(),
        "cell at exactly drop_time must be discarded, leaving no surviving cells"
    );
}

/// When every cell belongs to a fully-dropped column, the row emits nothing.
#[test]
fn all_cells_dropped_yields_no_row() {
    let row = live(
        0,
        120,
        vec![scalar_cell("a", "x", 100), scalar_cell("b", "y", 110)],
    );
    let mut dropped = ::std::collections::HashMap::new();
    dropped.insert("a".to_string(), 200);
    dropped.insert("b".to_string(), 200);

    assert!(
        KWayMerger::<NoTrace>::reconcile_cluster(None, vec![row], &dropped, None).is_none(),
        "a row whose every cell is a dropped-column cell emits nothing"
    );
}

/// An empty `dropped_columns` map is a no-op: all cells survive.
#[test]
fn empty_dropped_map_is_noop() {
    let row = live(
        0,
        200,
        vec![
            scalar_cell("name", "alice", 100),
            scalar_cell("legacy", "stale", 100),
        ],
    );
    let merged = KWayMerger::<NoTrace>::reconcile_cluster(
        None,
        vec![row],
        &::std::collections::HashMap::new(),
        None,
    )
    .expect("a live row must be emitted");
    let cells = match merged.row_data {
        RowData::Live { cells } => cells,
        other => panic!("expected Live, got {:?}", other),
    };
    assert_eq!(cells.len(), 2, "no drops configured → every cell survives");
}

/// Issue #922 — exact per-cell purging. One row carries two cells with
/// DISTINCT writetimes: a pre-drop cell of the dropped column (`legacy`@100)
/// and a post-drop cell of another column (`name`@300). The dropped cell is
/// purged by its OWN timestamp (100 <= drop_time 150) while `name` survives by
/// its own (300 > 150) — proving the filter is per-cell, not row-timestamp.
#[test]
fn dropped_column_purge_is_exact_per_cell_within_one_row() {
    let row = live(
        0,
        300, // row liveness ts = newest cell; must NOT govern the older cell
        vec![
            scalar_cell("name", "alice", 300),
            scalar_cell("legacy", "stale", 100),
        ],
    );
    let mut dropped = ::std::collections::HashMap::new();
    dropped.insert("legacy".to_string(), 150);

    let merged = KWayMerger::<NoTrace>::reconcile_cluster(None, vec![row], &dropped, None)
        .expect("name survives, so a live row must be emitted");
    let cells = match merged.row_data {
        RowData::Live { cells } => cells,
        other => panic!("expected Live, got {:?}", other),
    };
    assert_eq!(cells.len(), 1, "only the post-drop `name` cell survives");
    assert_eq!(cells[0].column, "name");
    assert_eq!(cells[0].timestamp, 300);
}

/// GATING TEST — multi-cell merge granularity.
///
/// A non-frozen collection (here: a list) is read back from an SSTable as a
/// SINGLE top-level column whose `Value` is the whole collection. Two SSTable
/// runs that each wrote different *paths* (different list positions) of the
/// same column therefore arrive as two `CellData` that share the same
/// `cell.column` string but carry different whole-collection `Value`s.
///
/// `reconcile_cluster` keys winners on `cell.column` ONLY. This test asserts
/// the observed consequence: the two path-writes do NOT merge into a combined
/// collection; instead ONE whole cell wins by timestamp (whole-group collapse,
/// NOT per-path merge).
#[test]
fn multicell_collection_collapses_whole_column_not_per_path() {
    // Newer run (run_index 0) wrote element "b"; older run wrote element "a".
    // In real Cassandra these are two cells at paths p_b and p_a that union to
    // [a, b]. Here each run surfaces the column as a whole list value.
    let newer = live(
        0,
        200,
        vec![CellData {
            column: "tags".to_string(),
            value: Value::List(vec![Value::text("b".to_string())]),
            timestamp: 200,
            ttl: None,
            cell_path: None,
            local_deletion_time: None,
            is_complex_element: false,
            is_deleted: false,
            has_empty_value: false,
        }],
    );
    let older = live(
        1,
        100,
        vec![CellData {
            column: "tags".to_string(),
            value: Value::List(vec![Value::text("a".to_string())]),
            timestamp: 100,
            ttl: None,
            cell_path: None,
            local_deletion_time: None,
            is_complex_element: false,
            is_deleted: false,
            has_empty_value: false,
        }],
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
        other => panic!("expected Live, got {:?}", other),
    };

    // ESTABLISHED BEHAVIOUR: exactly ONE cell for column "tags" survives —
    // there is no per-path union. The newer (ts=200) whole value wins.
    assert_eq!(
        cells.len(),
        1,
        "column-name keyed merge collapses to one cell"
    );
    assert_eq!(cells[0].column, "tags");
    assert_eq!(
        cells[0].value,
        Value::List(vec![Value::text("b".to_string())]),
        "winner is the higher-timestamp WHOLE collection value, not a union \
             of [a, b] — confirms whole-group collapse, NOT per-path merge (#18)"
    );
}

/// GATING TEST — non-frozen UDT field-level writes also collapse.
///
/// A non-frozen UDT is multi-cell in Cassandra (one cell per field path, paths
/// ordered by SIGNED ShortType field index). The reader surfaces it as a single
/// `Value::Udt` under one column. Two runs writing different fields therefore
/// collide on the column name and do NOT field-merge.
#[test]
fn nonfrozen_udt_collapses_whole_column_not_per_field() {
    let mk_udt = |field: &str, v: &str| {
        Value::Udt(Box::new(UdtValue {
            type_name: "addr".to_string(),
            keyspace: "ks".to_string(),
            fields: vec![UdtField {
                name: field.to_string(),
                value: Some(Value::text(v.to_string())),
            }],
        }))
    };

    // Newer run wrote field "city"; older run wrote field "zip".
    let newer = live(
        0,
        200,
        vec![CellData {
            column: "address".to_string(),
            value: mk_udt("city", "SF"),
            timestamp: 200,
            ttl: None,
            cell_path: None,
            local_deletion_time: None,
            is_complex_element: false,
            is_deleted: false,
            has_empty_value: false,
        }],
    );
    let older = live(
        1,
        100,
        vec![CellData {
            column: "address".to_string(),
            value: mk_udt("zip", "94105"),
            timestamp: 100,
            ttl: None,
            cell_path: None,
            local_deletion_time: None,
            is_complex_element: false,
            is_deleted: false,
            has_empty_value: false,
        }],
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
        other => panic!("expected Live, got {:?}", other),
    };

    assert_eq!(
        cells.len(),
        1,
        "UDT column collapses to one whole-value cell"
    );
    // The "zip" write is lost: no per-field merge happens. The signed-ShortType
    // path ordering required by #18 is therefore unreachable in this engine.
    assert_eq!(
        cells[0].value,
        mk_udt("city", "SF"),
        "newer whole-UDT value wins; older field write is dropped — no per-field \
             merge, so #18 path-ordering does not apply"
    );
}

/// GATING TEST — the reader→merge adapter representation.
///
/// Confirms the structural root cause: `value_to_row_data` turns a row (a
/// top-level `Value::Map` of column-name → value) into one `CellData` per
/// top-level column. A collection value nested under a column stays a single
/// nested `Value`; it is NOT exploded into per-path cells. There is no
/// `cell_path`/collection-key anywhere in `CellData`.
#[test]
fn adapter_produces_one_cell_per_top_level_column() {
    let row = Value::Map(vec![
        (Value::text("id".to_string()), Value::text("k1".to_string())),
        (
            Value::text("tags".to_string()),
            // Whole collection nested under a single column.
            Value::List(vec![
                Value::text("a".to_string()),
                Value::text("b".to_string()),
            ]),
        ),
    ]);

    let row_data = SSTableRowIteratorAdapter::value_to_row_data(&row, 500).expect("row data");
    let cells = match row_data {
        RowData::Live { cells } => cells,
        other => panic!("expected Live, got {:?}", other),
    };

    // Two TOP-LEVEL columns → two cells. The collection is NOT split per path.
    assert_eq!(cells.len(), 2, "one CellData per top-level column");
    let tags = cells
        .iter()
        .find(|c| c.column == "tags")
        .expect("tags cell present");
    assert_eq!(
        tags.value,
        Value::List(vec![
            Value::text("a".to_string()),
            Value::text("b".to_string())
        ]),
        "the collection arrives as ONE nested Value, never as per-path cells"
    );
}

/// #14/#17 complex deletion — current status check.
///
/// A complex (collection-level) deletion is, in Cassandra, a tombstone scoped
/// to a single complex column with its own deletion time. CQLite has no
/// per-column complex-deletion representation: `RowData::Tombstone` is whole-row
/// only, and a `Value::Tombstone(CellTombstone)` is treated as a per-CELL
/// tombstone keyed on the column name (collapsing with any sibling cell).
///
/// This test asserts the consequence relevant to #14/#17: an equal-timestamp
/// ROW deletion supersedes a CELL tombstone on a column (drop-on-equality via
/// the `timestamp > row_del` filter), which is the row-vs-complex equality rule
/// — but there is no path-scoped complex deletion to apply it to. We assert the
/// row-vs-cell equality behaviour that DOES exist.
#[test]
fn row_deletion_supersedes_equal_ts_cell_tombstone() {
    let row_tomb = MergeEntry::new(
        0,
        dk(1),
        None,
        100,
        RowData::Tombstone {
            deletion_time: 100,
            local_deletion_time: 0,
        },
    );
    let cell_tomb = live(
        1,
        100,
        vec![CellData {
            column: "tags".to_string(),
            value: Value::Tombstone(Box::new(TombstoneInfo {
                deletion_time: 100,
                tombstone_type: TombstoneType::CellTombstone,
                local_deletion_time: 0,
                ttl: None,
                range_start: None,
                range_end: None,
            })),
            timestamp: 100,
            ttl: None,
            cell_path: None,
            local_deletion_time: None,
            is_complex_element: false,
            is_deleted: false,
            has_empty_value: false,
        }],
    );

    let merged = KWayMerger::<NoTrace>::reconcile_cluster(
        None,
        vec![row_tomb, cell_tomb],
        &::std::collections::HashMap::new(),
        None,
    )
    .expect("row tombstone keeps the row shadowed");

    // Equal-ts row deletion wins: no surviving cell, row stays a tombstone.
    match merged.row_data {
        RowData::Tombstone { deletion_time, .. } => {
            assert_eq!(deletion_time, 100, "row tombstone preserved at its ts");
        }
        RowData::Live { cells } => {
            panic!("expected row to stay shadowed, got live cells: {:?}", cells)
        }
    }
}

// ── Issue #848 (Epic #921): tombstone-vs-expiring(TTL) tie-break ──────────
//
// Parity Cassandra `a62c749` (`Cells#reconcile`): at EQUAL timestamp a cell
// DELETION wins over a LIVE/expiring cell, decided BEFORE any
// `localDeletionTime` compare. `main` carries `ttl`/`local_deletion_time` on
// `CellData` (now populated for simple cells via SimpleCell), so an expiring
// cell at equal ts must NOT resurrect data over a same-ts cell tombstone.

/// Build a single-cell expiring (TTL) `CellData` for column `v`.
fn expiring_cell(value: &str, ts: i64, ttl: u32, ldt: i32) -> CellData {
    CellData {
        column: "v".to_string(),
        value: Value::text(value.to_string()),
        timestamp: ts,
        ttl: Some(ttl),
        cell_path: None,
        local_deletion_time: Some(ldt),
        is_complex_element: false,
        is_deleted: false,
        has_empty_value: false,
    }
}

/// Build a single-cell tombstone `CellData` for column `v`.
fn cell_tombstone(ts: i64, ldt: i32) -> CellData {
    CellData {
        column: "v".to_string(),
        value: Value::Tombstone(Box::new(TombstoneInfo {
            deletion_time: ts,
            tombstone_type: TombstoneType::CellTombstone,
            local_deletion_time: ldt as i64,
            ttl: None,
            range_start: None,
            range_end: None,
        })),
        timestamp: ts,
        ttl: None,
        cell_path: None,
        local_deletion_time: Some(ldt),
        is_complex_element: false,
        is_deleted: false,
        has_empty_value: false,
    }
}

/// At equal timestamp the cell tombstone beats an expiring cell — EXPIRING
/// arrives FIRST (newer run_index) so a naive recency/first-seen pick would
/// resurrect it. The tombstone must still win (parity `a62c749`).
#[test]
fn issue_848_tombstone_beats_expiring_at_equal_ts_expiring_first() {
    const TS: i64 = 200;
    // EXPIRING cell carries a LARGER localDeletionTime than the tombstone, so
    // a (wrong) localDeletionTime-first tie-break would pick the expiring
    // cell. The deletion must be decided BEFORE that compare.
    let expiring = live(
        0,
        TS,
        vec![expiring_cell("resurrected-if-buggy", TS, 3600, 9_999)],
    );
    let tombstone = live(1, TS, vec![cell_tombstone(TS, 1_000)]);

    let merged = KWayMerger::<NoTrace>::reconcile_cluster(
        None,
        vec![expiring, tombstone],
        &::std::collections::HashMap::new(),
        None,
    )
    .expect("a row must be emitted");

    let cells = match merged.row_data {
        RowData::Live { cells } => cells,
        other => panic!("expected Live, got {:?}", other),
    };
    assert_eq!(cells.len(), 1, "single column `v`");
    assert!(
        KWayMerger::<NoTrace>::is_cell_tombstone(&cells[0]),
        "at equal ts the cell TOMBSTONE must win over the expiring cell \
             (before any localDeletionTime compare); got {:?}",
        cells[0].value
    );
    assert!(
        cells[0].ttl.is_none(),
        "the surviving winner is the tombstone, which carries no TTL"
    );
}

/// Same equal-ts tie-break with the source order REVERSED — tombstone arrives
/// FIRST. The tombstone must still win (order-independence; the existing
/// `tombstone beats live` rule already covered this, pinned here too).
#[test]
fn issue_848_tombstone_beats_expiring_at_equal_ts_tombstone_first() {
    const TS: i64 = 200;
    let tombstone = live(0, TS, vec![cell_tombstone(TS, 1_000)]);
    let expiring = live(
        1,
        TS,
        vec![expiring_cell("resurrected-if-buggy", TS, 3600, 9_999)],
    );

    let merged = KWayMerger::<NoTrace>::reconcile_cluster(
        None,
        vec![tombstone, expiring],
        &::std::collections::HashMap::new(),
        None,
    )
    .expect("a row must be emitted");

    let cells = match merged.row_data {
        RowData::Live { cells } => cells,
        other => panic!("expected Live, got {:?}", other),
    };
    assert_eq!(cells.len(), 1, "single column `v`");
    assert!(
        KWayMerger::<NoTrace>::is_cell_tombstone(&cells[0]),
        "at equal ts the cell TOMBSTONE must win regardless of source order; \
             got {:?}",
        cells[0].value
    );
}

/// A STRICTLY NEWER expiring cell still beats an older tombstone — the
/// tie-break only fires at EQUAL timestamp, so timestamp recency is honored
/// first (parity `a62c749`: timestamp compared before deletion-status).
#[test]
fn issue_848_newer_expiring_beats_older_tombstone() {
    let tombstone = live(0, 100, vec![cell_tombstone(100, 1_000)]);
    let expiring = live(1, 200, vec![expiring_cell("survives", 200, 3600, 9_999)]);

    let merged = KWayMerger::<NoTrace>::reconcile_cluster(
        None,
        vec![tombstone, expiring],
        &::std::collections::HashMap::new(),
        None,
    )
    .expect("a row must be emitted");

    let cells = match merged.row_data {
        RowData::Live { cells } => cells,
        other => panic!("expected Live, got {:?}", other),
    };
    assert_eq!(cells.len(), 1, "single column `v`");
    assert!(
        !KWayMerger::<NoTrace>::is_cell_tombstone(&cells[0]),
        "a strictly NEWER expiring cell (ts=200) beats the older tombstone \
             (ts=100); the deletion tie-break only fires at equal ts"
    );
    assert_eq!(cells[0].value, Value::text("survives".to_string()));
}
