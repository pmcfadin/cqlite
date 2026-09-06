//! Issue #2339 (roborev job 132): the invariant the ambiguous-cell GROUPING rests on.
//!
//! `MergeProducer::produce`'s tier-3 preparation groups the ambiguous columns' cells
//! by NAME in a single pass. That is equivalent to the per-column filter it replaced
//! only because `raw_visibility_signal` yields each ambiguous column AT MOST ONCE —
//! keying by name would otherwise collapse two entries the filter form produced
//! separately, and the `remove`-based rebuild would hand the second occurrence an
//! EMPTY cell list, silently dropping cells from a row-VISIBILITY decision.
//!
//! Nothing else in the crate asserts that. The dedup is one `!ambiguous.iter().any(..)`
//! guard, so a refactor could drop it while every other test stayed green: the
//! observable damage is a row wrongly hidden, and no existing case reaches this path
//! with a non-empty ambiguous set at all.
//!
//! A CHILD module (`#[path]` from `producer.rs`) because `raw_visibility_signal` and
//! `RawVisibility` are private to that module — `producer_udt_scope_tests` is declared
//! in `lib.rs` and is a SIBLING, so it cannot see them.
//!
//! Deliberately NOT a timing test: the fix is a complexity change, and a wall-clock
//! threshold in the correctness path is banned and mechanically linted (#2642).

use super::*;
use crate::testutil::simple_schema;
use cqlite_core::storage::write_engine::merge::CellData;
use cqlite_core::Value;

/// One cell. `is_deleted` distinguishes a tombstone from a live cell, which is what
/// `raw_visibility_signal` keys its tiers on.
fn cell(column: &str, timestamp: i64, is_deleted: bool) -> CellData {
    // `is_tomb` accepts EITHER signal (`is_deleted` or a `Value::Tombstone`); this uses
    // `is_deleted`, which is the field the reader sets for a simple cell tombstone.
    CellData {
        column: column.to_string(),
        value: Value::Integer(1),
        timestamp,
        ttl: None,
        cell_path: None,
        local_deletion_time: None,
        is_complex_element: false,
        is_deleted,
        has_empty_value: false,
    }
}

fn producer() -> MergeProducer {
    MergeProducer::new(simple_schema(), 64).expect("producer")
}

/// SEVERAL live cells on ONE outranked column must yield that column ONCE.
///
/// Both live cells sit BELOW the column's tombstone timestamp, so both take the
/// ambiguous branch — the shape that would push a duplicate if the guard were dropped.
#[test]
fn an_outranked_column_appears_once_however_many_live_cells_it_has() {
    let cells = vec![
        cell("name", 100, true),
        cell("name", 50, false),
        cell("name", 60, false),
    ];
    let visibility = producer().raw_visibility_signal(&cells);
    assert!(
        !visibility.outranks_every_tombstone,
        "precondition: every live cell must be outranked, or tier 1 decides the row \
         visible and `ambiguous_columns` is deliberately emptied — which would make \
         this case vacuous"
    );
    assert_eq!(
        visibility.ambiguous_columns,
        vec!["name".to_string()],
        "three cells on one column must yield ONE ambiguous entry; a duplicate makes \
         the by-name grouping in `produce` lossy for the second occurrence"
    );
}

/// The set is unique ACROSS columns too, and keeps first-seen order.
#[test]
fn several_outranked_columns_each_appear_once_in_first_seen_order() {
    let cells = vec![
        cell("name", 100, true),
        cell("score", 100, true),
        cell("score", 10, false),
        cell("name", 20, false),
        cell("score", 30, false),
        cell("name", 40, false),
    ];
    let visibility = producer().raw_visibility_signal(&cells);
    assert!(
        !visibility.outranks_every_tombstone,
        "precondition: all outranked"
    );
    assert_eq!(
        visibility.ambiguous_columns,
        vec!["score".to_string(), "name".to_string()],
        "each column once, in the order its first ambiguous cell was seen (score's \
         live cell precedes name's), so the rebuilt Vec's order is well-defined"
    );
}

/// A PRIMARY KEY column is never ambiguous: it carries no reconcilable data cell, and
/// including it would make `produce` group cells for a column tier 3 must not ask about.
#[test]
fn a_primary_key_column_is_never_ambiguous() {
    let cells = vec![
        cell("id", 100, true),
        cell("id", 10, false),
        cell("name", 100, true),
        cell("name", 10, false),
    ];
    let visibility = producer().raw_visibility_signal(&cells);
    assert_eq!(
        visibility.ambiguous_columns,
        vec!["name".to_string()],
        "the partition key `id` must be excluded even when its cells are outranked"
    );
}
