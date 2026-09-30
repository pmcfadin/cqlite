//! Unit tests for [`super::fold_partition_statistics`] (issue #4197 PASS 2).
//!
//! Loaded via `#[path]` from `components.rs` so the production module stays
//! under the campsite-rule size target (mirrors `stats_fold_tests.rs`'s own
//! split).
//!
//! No `issue_4197_rebuild_*` fixture-based integration test observes any
//! `Statistics.db` field besides `partition_count` — every field this
//! function folds (min/max timestamp, min/max local-deletion-time, the
//! tombstone-drop histogram, `has_partition_level_deletions`) is otherwise
//! unobserved (roborev finding on PR #4250). These tests construct the
//! `Mutation` shapes directly, exactly as `stats_fold_tests.rs` does for the
//! equivalent #1668/#4246 cases, rather than requiring a fixture round trip.

use super::*;
use crate::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use crate::storage::write_engine::mutation::{
    CellOperation, ClusteringBound, ClusteringKey, PartitionKey, TableId,
};
use crate::types::Value;

fn table() -> TableId {
    TableId::new("ks", "t")
}

fn pk() -> PartitionKey {
    PartitionKey::single("id", Value::Integer(1))
}

fn ck(n: i32) -> ClusteringKey {
    ClusteringKey::single("ck", Value::Integer(n))
}

fn col(name: &str, ty: &str, is_static: bool) -> Column {
    Column {
        name: name.to_string(),
        data_type: ty.to_string(),
        nullable: true,
        default: None,
        is_static,
    }
}

/// A static-bearing schema (`s` static): a `clustering_key: None` mutation
/// against it is classified as a static-row carrier by `is_static_row_mutation`.
fn static_schema() -> TableSchema {
    TableSchema {
        keyspace: "ks".to_string(),
        table: "t".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![
            col("id", "int", false),
            col("ck", "int", false),
            col("v", "text", false),
            col("s", "text", true),
        ],
        comments: Default::default(),
        dropped_columns: Default::default(),
    }
}

/// Extracts `partition_tombstone`/`range_tombstones` from `mutations` the
/// SAME way `rebuild_components`'s own PASS-2 loop does (winning-PT-only
/// `max_by_key`, every surviving RT) — so these tests drive
/// `fold_partition_statistics` with the exact authoritative-extraction shape
/// its one real caller always supplies.
fn extract_markers(mutations: &[Mutation]) -> (Option<PartitionTombstone>, Vec<RangeTombstone>) {
    let pt = mutations
        .iter()
        .filter_map(|m| m.partition_tombstone.as_ref())
        .max_by_key(|pt| pt.deletion_time)
        .cloned();
    let rts = mutations
        .iter()
        .flat_map(|m| m.range_tombstones.iter())
        .cloned()
        .collect();
    (pt, rts)
}

const PT_LDT: i32 = 1_000;
const RT_LDT: i32 = 2_000;
const STATIC_DELETE_LDT: i32 = 7_000;

/// `timestamp_micros` matches `deletion_time` — the shape `decode` actually
/// produces (`merge_entry_to_mutation`, `merge/mod.rs:3594`: `Mutation::new(..,
/// deletion_time, None)`), NOT an arbitrary value. This matters:
/// `fold_static_carrier_stats` unconditionally folds `timestamp_micros` (its
/// own doc's KNOWN RESIDUAL), so a carrier whose `timestamp_micros` diverged
/// from `deletion_time` would leak a synthetic value into `max_timestamp` —
/// invisible with a mismatched fixture, since nothing here would assert on
/// it (roborev N1).
fn pt_carrier() -> Mutation {
    let mut m = Mutation::new(table(), pk(), None, vec![], 100, None);
    m.partition_tombstone = Some(PartitionTombstone {
        deletion_time: 100,
        local_deletion_time: PT_LDT,
    });
    m
}

/// See [`pt_carrier`]'s doc comment: `timestamp_micros` matches
/// `deletion_time`, matching `merge_entry_to_mutation`'s actual
/// `MergeEntry::new(.., rt.deletion_time, ..)` → `Mutation::new(..,
/// entry.timestamp, None)` construction (`merge/mod.rs:2456`/`3743`).
fn rt_carrier() -> Mutation {
    let mut m = Mutation::new(table(), pk(), None, vec![], 200, None);
    m.range_tombstones.push(RangeTombstone {
        start: ClusteringBound::Inclusive(ck(1)),
        end: ClusteringBound::Inclusive(ck(3)),
        deletion_time: 200,
        local_deletion_time: RT_LDT,
    });
    m
}

fn static_carrier_delete_row() -> Mutation {
    Mutation::new(
        table(),
        pk(),
        None,
        vec![CellOperation::DeleteRow],
        250,
        None,
    )
    .with_local_deletion_time(STATIC_DELETE_LDT)
}

fn clustering_row(ts: i64) -> Mutation {
    Mutation::new(
        table(),
        pk(),
        Some(ck(9)),
        vec![CellOperation::Write {
            column: "v".to_string(),
            value: Value::text("live".to_string()),
        }],
        ts,
        None,
    )
}

/// I2(a)/I2(b) pinned: a marker-only carrier (partition- or range-tombstone)
/// folds to nothing under `fold_single_mutation_row_group` —
/// `merge_row_group` produces no row for it (`merge_entry_to_mutation` gives
/// both carrier shapes empty `operations` and no row deletion) — so folding
/// each carrier PER-MUTATION cannot double-count what the unconditional
/// marker fold already folded from the caller's authoritative extraction.
/// Static-bearing schema on purpose: also exercises `is_static_row_mutation`'s
/// vacuous-`.all()`-over-empty-`operations` classification (I2(c)) for both
/// carriers, proving that branch is equally inert for them.
#[test]
fn marker_only_carriers_do_not_double_count_the_marker_fold() {
    let schema = static_schema();
    let mutations = vec![pt_carrier(), rt_carrier()];
    let (pt, rts) = extract_markers(&mutations);

    let mut stats = StatisticsMetadata::new();
    fold_partition_statistics(&mut stats, &mutations, pt.as_ref(), &rts, &schema, true);

    assert_eq!(
        stats.min_local_deletion_time,
        PT_LDT.min(RT_LDT),
        "both marker LDTs must be folded exactly once, via the unconditional \
         marker fold alone"
    );
    assert_eq!(
        stats.tombstone_histogram.total_observations(),
        2,
        "exactly two marker observations (PT + RT) from the unconditional \
         marker fold — a carrier additionally routed through \
         is_static_row_mutation's vacuous branch must add ZERO further \
         observations"
    );
    assert!(
        stats.has_partition_level_deletions,
        "a folded partition tombstone must set the flag"
    );
    assert_eq!(
        stats.max_timestamp, 200,
        "positive control (roborev N1): with timestamp_micros == deletion_time \
         (the shape decode actually produces), the vacuously-misclassified \
         static branch's unconditional timestamp fold is a true no-op — it \
         lands on the SAME value the marker fold already folded, never a \
         higher synthetic one"
    );
}

/// The same two carriers, against a schema with NO static columns: now
/// `is_static_row_mutation` returns `false` for them (their emptiness is not
/// "vacuously static" in isolation — the predicate also checks the schema),
/// so they instead route through `fold_single_mutation_row_group` directly.
/// Proves marker-only mutations are a no-op via BOTH classification
/// branches, not just the static one — the two-branch safety argument
/// `fold_partition_statistics`'s doc comment makes.
#[test]
fn marker_only_carriers_are_also_inert_via_the_non_static_branch() {
    let mut schema = static_schema();
    for c in &mut schema.columns {
        c.is_static = false;
    }
    let mutations = vec![pt_carrier(), rt_carrier()];
    let (pt, rts) = extract_markers(&mutations);

    let mut stats = StatisticsMetadata::new();
    fold_partition_statistics(&mut stats, &mutations, pt.as_ref(), &rts, &schema, false);

    assert_eq!(stats.min_local_deletion_time, PT_LDT.min(RT_LDT));
    assert_eq!(
        stats.tombstone_histogram.total_observations(),
        2,
        "the non-static (fold_single_mutation_row_group) branch must also \
         contribute zero additional observations for a marker-only carrier"
    );
    assert_eq!(
        stats.max_timestamp, 200,
        "same positive control as the static-branch test (roborev N1), \
         proving the two classification branches are provably identical for \
         a marker-only carrier"
    );
}

/// C1 (roborev blocker on PR #4250): a static-row carrier's OWN row deletion
/// must not reach `max_local_deletion_time`/the tombstone-drop histogram —
/// mirrors `write_partition`'s issue #4246 round-8 exclusion
/// (`fold_static_carrier_stats`'s doc comment), pinned here through
/// `rebuild_components`'s own PASS-2 dispatch specifically: `stats_fold_tests.rs`
/// already proves `fold_static_carrier_stats` itself excludes this, but
/// nothing previously proved rebuild's dispatch actually ROUTES this shape
/// there rather than through the clustering-row arm.
#[test]
fn static_carrier_row_deletion_is_excluded_from_rebuilds_dispatch() {
    let schema = static_schema();
    let mutations = vec![static_carrier_delete_row()];
    let (pt, rts) = extract_markers(&mutations);

    let mut stats = StatisticsMetadata::new();
    fold_partition_statistics(&mut stats, &mutations, pt.as_ref(), &rts, &schema, true);

    assert_eq!(
        stats.tombstone_histogram.total_observations(),
        0,
        "a static carrier's row deletion is never emitted to Data.db, so \
         rebuild must not persist it into the tombstone-drop histogram"
    );
    assert_eq!(
        stats.min_local_deletion_time,
        i32::MAX,
        "min_local_deletion_time must stay at its untouched default — \
         folding the static carrier's DeleteRow LDT here is exactly the \
         #4246 round-6/7 phantom, now checked through rebuild's own dispatch"
    );
    assert_eq!(
        stats.min_timestamp, 250,
        "positive control (roborev N2): without this, deleting the entire \
         per-mutation dispatch loop would still pass the two assertions \
         above. This proves the mutation actually reached \
         fold_static_carrier_stats (which folds timestamp_micros, 250 — not \
         a live sentinel) rather than the function returning early / \
         skipping it entirely"
    );
}

/// LIVE CONTROL: a genuine clustering row IS folded, so the exclusion above
/// is not vacuously true because nothing gets folded at all.
#[test]
fn clustering_row_updates_timestamp_exactly_once() {
    let schema = static_schema();
    let mutations = vec![clustering_row(12_345)];
    let (pt, rts) = extract_markers(&mutations);

    let mut stats = StatisticsMetadata::new();
    fold_partition_statistics(&mut stats, &mutations, pt.as_ref(), &rts, &schema, true);

    assert_eq!(stats.min_timestamp, 12_345);
    assert_eq!(stats.max_timestamp, 12_345);
}
