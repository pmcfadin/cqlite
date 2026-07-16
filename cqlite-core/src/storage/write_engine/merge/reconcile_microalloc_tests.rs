//! Issue #1665 — reconcile micro-alloc guard.
//!
//! Q2 of epic #1610 removed two avoidable per-cluster allocations from the
//! reconcile kernel (`merge/reconcile.rs`):
//!
//! * **Site 1** (`resolve_cell_winners`): the `HashMap::entry()` API replaces the
//!   old `get()`+`insert()` double-hash on the vacant path. This does NOT change
//!   the `CellData` clone count (the map still owns one clone per winner write),
//!   so this guard does not target it directly.
//! * **Site 2** (`filter_dropped_columns`): the survivor set used to be
//!   deep-cloned out of `after_row_del` (`.cloned()`); it is now MOVED
//!   (`mem::take` + `into_iter`), eliminating one `CellData` clone per survivor.
//!
//! This module (a sibling file, not inline in `merge/mod.rs`, per the #1116
//! campsite rule) is the regression guard for Site 2: it drives one
//! clustering-group reconcile inside a
//! [`CellDataCloneScope`](crate::storage::sstable::work_counters::cell_data_clone_scope::CellDataCloneScope)
//! and asserts the observed `CellData::clone` count stays at or below
//! `S + winner_writes` — a bound only the post-fix (moved-survivor) code meets.

use super::reconcile::ReconcileState;
use super::{CellData, MergeEntry, PurgeCounts, RowData};
use crate::storage::sstable::work_counters::cell_data_clone_scope::CellDataCloneScope;
use crate::storage::write_engine::mutation::DecoratedKey;
use crate::types::Value;
use std::collections::HashMap;

/// A single live cell (`CellData::new` — no TTL/path/ldt), created fresh (NOT
/// cloned), so building the input costs zero recorded clones.
fn live_cell(column: &str, ts: i64) -> CellData {
    CellData::new(column.to_string(), Value::Text("v".to_string()), ts)
}

/// One live `MergeEntry` in partition `token=1` with `clustering_key = None`
/// carrying `cells`. The partition-key bytes are arbitrary — `ReconcileState`
/// only carries the first key through, it never decodes them.
fn live_entry(run_index: usize, cells: Vec<CellData>) -> MergeEntry {
    MergeEntry::new(
        run_index,
        DecoratedKey::new(1, vec![0, 0, 0, 1]),
        None,
        cells.iter().map(|c| c.timestamp).max().unwrap_or(0),
        RowData::Live { cells },
    )
}

/// Regression guard for issue #1665 (Site 2 — kill the survivor `.cloned()` in
/// `filter_dropped_columns`). Drives one clustering-group reconcile whose input
/// forces both a winning re-insert (so the winner-map write count exceeds the
/// distinct-winner count) AND a non-empty surviving set under active
/// dropped-column filtering, then asserts the `CellData::clone` count stays at
/// or below `S + winner_writes`.
///
/// Scenario (single partition, single clustering group):
/// * run 0: `a@100, b@100, c@100, d@50`
/// * run 1: `a@200, b@200` (both strictly newer — they win the reconcile)
/// * `dropped_columns = {"d": 100}` → `d@50` is dropped (`50 <= 100`).
///
/// Distinct winners `W = 4` (a, b, c, d); survivors `S = 3` (a, b, c).
/// `resolve_cell_winners` clones once per winner WRITE = `W + 2` re-inserts = 6
/// (Site 1 is unchanged by the fix; the bound uses the DISTINCT-winner count,
/// not the write count, so the pre-fix survivor clones show up as the excess).
///
// CLONE ACCOUNTING (recorded for context):
//   main today (pre-fix): 6 (Site 1 winner writes) + 3 (Site 2 `.cloned()`
//                         survivors) = 9  -> 9 > 7, so this assertion is RED.
//   post-fix:             6 (Site 1 winner writes) + 0 (Site 2 moves) = 6
//                         -> 6 <= 7, GREEN.
/// The bound `S + winner_writes = 3 + 4 = 7` sits strictly between the two: a
/// regression that reintroduces the survivor `.cloned()` fails it.
#[test]
fn filter_dropped_columns_does_not_clone_survivors() {
    // Distinct winner keys across the group (the map owns one CellData per key;
    // that clone is unavoidable — the map is by-value). = 4: a, b, c, d. NB: this
    // is the DISTINCT-winner count, not the winner-map WRITE count (6 = 4 vacant
    // inserts + 2 winning re-inserts); the extra slack from re-inserts is why the
    // pre-fix survivor clones (3) push the total past the bound.
    const DISTINCT_WINNERS: u64 = 4;
    // Cells surviving the dropped-column filter: a, b, c (d@50 is dropped). = 3.
    const S: u64 = 3;

    let cluster_rows = vec![
        live_entry(
            0,
            vec![
                live_cell("a", 100),
                live_cell("b", 100),
                live_cell("c", 100),
                live_cell("d", 50),
            ],
        ),
        live_entry(1, vec![live_cell("a", 200), live_cell("b", 200)]),
    ];

    let mut dropped_columns: HashMap<String, i64> = HashMap::new();
    dropped_columns.insert("d".to_string(), 100);

    let mut state = ReconcileState::new(None);
    let mut purges = PurgeCounts::default();

    // Record clones only across the reconcile itself (input construction above
    // used `CellData::new`, never `clone`, and happens outside the scope).
    let scope = CellDataCloneScope::new();

    state.fold_row_deletions(&cluster_rows);
    state.resolve_cell_winners(&cluster_rows);
    state.apply_complex_deletions();
    state.shadow_by_row_deletion(&mut purges);
    state.filter_dropped_columns(&dropped_columns);
    state.expire_ttl_cells(None);
    state.purge_gc_grace(None, i64::MAX, &mut purges);
    let built = state.build(&mut purges);

    let clones = scope.count();
    drop(scope);

    // Output-parity sanity: the winning cells survive (a, b, c) and `d` was
    // dropped, so a live row with exactly 3 data cells is emitted.
    let entry = built.expect("a live row must be emitted");
    match &entry.row_data {
        RowData::Live { cells } => {
            assert_eq!(
                cells.len(),
                S as usize,
                "d@50 dropped, a/b/c survive as the winning cells"
            );
            let mut cols: Vec<&str> = cells.iter().map(|c| c.column.as_str()).collect();
            cols.sort_unstable();
            assert_eq!(cols, ["a", "b", "c"]);
            // The winning (strictly-newer) values are the ts=200 cells of a, b.
            for cell in cells {
                let expected_ts = if cell.column == "c" { 100 } else { 200 };
                assert_eq!(cell.timestamp, expected_ts, "winner ts for {}", cell.column);
            }
        }
        other => panic!("expected a live row, got {other:?}"),
    }

    let bound = S + DISTINCT_WINNERS; // 3 + 4 = 7
    assert!(
        clones <= bound,
        "reconcile cloned CellData {clones} times (bound {bound} = S={S} + \
         distinct_winners={DISTINCT_WINNERS}); the #1665 survivor `.cloned()` in \
         filter_dropped_columns regressed (main-today was 9)"
    );
}
