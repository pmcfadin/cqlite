//! Unit tests for [`super`] (stats_fold.rs).
//!
//! Loaded via `#[path]` from `stats_fold.rs` so the production module stays
//! under the campsite-rule size target (issue #4246, epic #1135).

use super::*;
use crate::storage::write_engine::mutation::{
    ClusteringBound, ClusteringKey, PartitionKey, PartitionTombstone, RangeTombstone, TableId,
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

/// A STATIC-BEARING schema for the fold-composition mirror below: `s` is a
/// static column, so a `clustering_key: None` mutation is classified as a
/// static-row carrier (the classification `fold_static_carrier_stats` serves).
fn fold_schema() -> crate::schema::TableSchema {
    use crate::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
    fn col(name: &str, ty: &str, is_static: bool) -> Column {
        Column {
            name: name.to_string(),
            data_type: ty.to_string(),
            nullable: true,
            default: None,
            is_static,
        }
    }
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
            col("tags", "set<text>", false),
            col("s", "text", true),
        ],
        comments: Default::default(),
        dropped_columns: Default::default(),
    }
}

/// The per-mutation fold composition PRODUCTION actually runs — a faithful
/// mirror of the dispatch in `KWayMerger::merge`
/// (`write_engine/merge/mod.rs`) and `WriteEngine::maintenance_step`
/// (`write_engine/maintenance.rs`), the two incremental compaction paths
/// `merge_stats_fold` exists to serve (they accumulate a partition-scoped
/// sub-fold and merge it into the SSTable-wide accumulator at partition
/// end).
///
/// Issue #4246 roborev round-8 finding 3: these tests used to compose
/// `fold_mutation_stats`, which is `#[cfg(test)]`-only with NO production
/// caller — so the equivalence they proved no longer covered anything
/// production composes, and a drift between the two could not be detected.
/// The three branches below are the SAME three predicates those paths use,
/// in the SAME order:
///   * `is_partition_only` / `is_range_only` — a marker-only carrier folds
///     markers and NO row content;
///   * `is_static_carrier` (`clustering_key.is_none() && schema_has_static`)
///     — [`fold_static_carrier_stats`];
///   * otherwise a clustering row — [`fold_single_mutation_row_group`].
///
/// ABSTRACTED DELIBERATELY (not modelled here, and not what these tests
/// assert): the `shadow_floor` a real partition derives from its partition
/// tombstone and covering range tombstones. `None` is passed, matching both
/// paths' behavior for a partition with no covering deletion — the property
/// under test is the ASSOCIATIVITY of `merge_stats_fold` over whatever the
/// per-mutation fold produced, which is independent of the shadow gate.
/// `SSTableWriter::write_partition` differs in one respect the mirror does
/// not reproduce: it folds markers from the partition's already-extracted
/// winning `partition_tombstone`/`range_tombstones` rather than via
/// [`fold_marker_stats`] (see its call site), precisely so a carrier
/// mutation is not re-scanned and double-counted.
fn production_fold(
    stats: &mut StatisticsMetadata,
    mutation: &Mutation,
    schema: &crate::schema::TableSchema,
) {
    let schema_has_static = schema.columns.iter().any(|c| c.is_static);
    let is_partition_only = mutation.operations.is_empty()
        && mutation.partition_tombstone.is_some()
        && mutation.row_tombstone.is_none()
        && mutation.range_tombstones.is_empty();
    let is_range_only = mutation.operations.is_empty()
        && mutation.partition_tombstone.is_none()
        && mutation.row_tombstone.is_none()
        && !mutation.range_tombstones.is_empty();

    if is_partition_only || is_range_only {
        fold_marker_stats(stats, mutation);
    } else if mutation.clustering_key.is_none() && schema_has_static {
        fold_static_carrier_stats(stats, mutation);
    } else {
        fold_single_mutation_row_group(stats, mutation, schema, schema_has_static, None);
    }
}

/// The local deletion time stamped on the static-carrier `DeleteRow` in
/// [`representative_mutations`] — distinct from every other LDT in the
/// fixture (1_000..=6_000) so its presence or absence in the tombstone-drop
/// histogram is unambiguous.
const STATIC_CARRIER_DELETE_LDT: i32 = 7_000;

/// A representative set of mutations exercising every branch
/// `fold_mutation_stats` handles: a partition tombstone, a range
/// tombstone, a decoupled row tombstone, a TTL write, a `ComplexDeletion`
/// marker, both a deleted and a LIVE `WriteComplexElement`, a plain live
/// `Write` (which lifts the live-LDT sentinel), and a null `Write` (which
/// must NOT).
fn representative_mutations() -> Vec<Mutation> {
    let mut partition_only = Mutation::new(table(), pk(), None, vec![], 500, None);
    partition_only.partition_tombstone = Some(PartitionTombstone {
        deletion_time: 100,
        local_deletion_time: 1_000,
    });

    let mut range_only = Mutation::new(table(), pk(), None, vec![], 500, None);
    range_only.range_tombstones.push(RangeTombstone {
        start: ClusteringBound::Inclusive(ck(1)),
        end: ClusteringBound::Inclusive(ck(3)),
        deletion_time: 200,
        local_deletion_time: 2_000,
    });

    let row_tombstone_row =
        Mutation::new(table(), pk(), Some(ck(1)), vec![], 300, None).with_row_tombstone(50, 3_000);

    let ttl_row = Mutation::new(
        table(),
        pk(),
        Some(ck(2)),
        vec![CellOperation::WriteWithTtl {
            column: "v".to_string(),
            value: Value::text("x".to_string()),
            ttl_seconds: 60,
            local_deletion_time: Some(4_000),
        }],
        400,
        Some(60),
    );

    let complex_deletion_row = Mutation::new(
        table(),
        pk(),
        Some(ck(3)),
        vec![CellOperation::ComplexDeletion {
            column: "tags".to_string(),
            marked_for_delete_at: 9_000_000,
            local_deletion_time: 5_000,
        }],
        600,
        None,
    );

    let complex_element_row = Mutation::new(
        table(),
        pk(),
        Some(ck(4)),
        vec![
            CellOperation::WriteComplexElement {
                column: "tags".to_string(),
                cell_path: b"a".to_vec(),
                value: None,
                timestamp_micros: 700,
                ttl_seconds: None,
                local_deletion_time: Some(6_000),
                is_deleted: true,
            },
            CellOperation::WriteComplexElement {
                column: "tags".to_string(),
                cell_path: b"b".to_vec(),
                value: Some(Value::text("b".to_string())),
                timestamp_micros: 750,
                ttl_seconds: None,
                local_deletion_time: None,
                is_deleted: false,
            },
        ],
        700,
        None,
    );

    let live_write_row = Mutation::new(
        table(),
        pk(),
        Some(ck(5)),
        vec![CellOperation::Write {
            column: "v".to_string(),
            value: Value::text("live".to_string()),
        }],
        800,
        None,
    );

    let null_write_row = Mutation::new(
        table(),
        pk(),
        Some(ck(6)),
        vec![CellOperation::Write {
            column: "v".to_string(),
            value: Value::Null,
        }],
        900,
        None,
    );

    // Issue #4246 roborev round-8: a STATIC-ROW CARRIER (`clustering_key:
    // None` against a static-bearing schema) carrying a live static cell —
    // the shape `fold_static_carrier_stats` serves, so `production_fold`'s
    // static branch is actually exercised rather than dead.
    let static_carrier = Mutation::new(
        table(),
        pk(),
        None,
        vec![CellOperation::Write {
            column: "s".to_string(),
            value: Value::text("static".to_string()),
        }],
        850,
        None,
    );

    // A static carrier whose only operation is `DeleteRow`, stamped with an
    // LDT no other fixture mutation uses. No production path emits a
    // static-row deletion (see `fold_static_carrier_stats`'s doc comment), so
    // this must contribute NOTHING to the drop-time histogram — pinned
    // directly by `static_carrier_row_deletion_is_never_folded` below, and
    // carried here so the equivalence fold covers the shape too.
    let static_carrier_delete_row = Mutation::new(
        table(),
        pk(),
        None,
        vec![CellOperation::DeleteRow],
        250,
        None,
    )
    .with_local_deletion_time(STATIC_CARRIER_DELETE_LDT);

    vec![
        partition_only,
        range_only,
        row_tombstone_row,
        ttl_row,
        complex_deletion_row,
        complex_element_row,
        live_write_row,
        null_write_row,
        static_carrier,
        static_carrier_delete_row,
    ]
}

/// The relevant `StatisticsMetadata` fields `fold_mutation_stats`/
/// `merge_stats_fold` actually touch, for equivalence comparison (no
/// `PartialEq` on the full struct — the histogram/key-range/repair fields
/// are untouched by this fold and irrelevant here).
#[derive(Debug, PartialEq)]
struct FoldSnapshot {
    min_timestamp: i64,
    max_timestamp: i64,
    min_local_deletion_time: i32,
    max_local_deletion_time: i32,
    min_ttl: i32,
    max_ttl: i32,
    has_partition_level_deletions: bool,
}

impl From<&StatisticsMetadata> for FoldSnapshot {
    fn from(s: &StatisticsMetadata) -> Self {
        Self {
            min_timestamp: s.min_timestamp,
            max_timestamp: s.max_timestamp,
            min_local_deletion_time: s.min_local_deletion_time,
            max_local_deletion_time: s.max_local_deletion_time,
            min_ttl: s.min_ttl,
            max_ttl: s.max_ttl,
            has_partition_level_deletions: s.has_partition_level_deletions,
        }
    }
}

/// Correctness proof (issue #1668 stage 5c-iv part 2): folding every
/// mutation of a partition DIRECTLY into one accumulator must produce the
/// IDENTICAL final min/max/flag aggregates as the incremental path's split —
/// folding disjoint SUBSETS of the same mutations into SEPARATE
/// partition-scoped accumulators (simulating streaming them across several
/// `feed_row`/`feed_static_row` calls) and then merging those sub-folds back
/// together via `merge_stats_fold` (simulating
/// `complete_partition_incremental`).
///
/// Both sides compose [`production_fold`] — the real dispatch the two
/// incremental compaction paths run (issue #4246 roborev round-8 finding 3:
/// the "direct fold" side used to be `fold_mutation_stats`, a
/// `#[cfg(test)]`-only wrapper with no production caller, so the equivalence
/// no longer covered what production actually composes).
#[test]
fn split_and_merge_matches_direct_fold_for_every_mutation_kind() {
    let schema = fold_schema();
    let mutations = representative_mutations();

    // Direct: fold every mutation into one running accumulator.
    let mut direct = StatisticsMetadata::new();
    for m in &mutations {
        production_fold(&mut direct, m, &schema);
    }

    // Incremental-equivalent: split into three arbitrary, non-trivial
    // partition-scoped sub-folds (as if fed across three separate
    // `feed_row`/`feed_static_row` calls before merging at partition
    // end), then merge them all into a fresh running accumulator.
    let mut part_a = StatisticsMetadata::new();
    let mut part_b = StatisticsMetadata::new();
    let mut part_c = StatisticsMetadata::new();
    for (i, m) in mutations.iter().enumerate() {
        match i % 3 {
            0 => production_fold(&mut part_a, m, &schema),
            1 => production_fold(&mut part_b, m, &schema),
            _ => production_fold(&mut part_c, m, &schema),
        }
    }
    let mut merged = StatisticsMetadata::new();
    merge_stats_fold(&mut merged, &part_a);
    merge_stats_fold(&mut merged, &part_b);
    merge_stats_fold(&mut merged, &part_c);

    assert_eq!(
        FoldSnapshot::from(&direct),
        FoldSnapshot::from(&merged),
        "splitting the fold across partition-scoped sub-accumulators and \
         merging must reproduce the direct fold exactly"
    );

    // Sanity: the live sentinel and partition-deletion flag must actually
    // have been exercised by this fixture, else the equality above would
    // pass vacuously without proving the sentinel-handling guards work.
    assert_eq!(
        direct.max_local_deletion_time,
        i32::MAX,
        "live_write_row must have lifted the live-LDT sentinel"
    );
    assert!(
        direct.has_partition_level_deletions,
        "partition_only must have set the partition-level-deletion flag"
    );
    // ...and that the STATIC branch of `production_fold` was reached at all
    // (issue #4246 roborev round-8): `static_carrier`'s writetime is the
    // fixture's maximum, so it can only be the max if that branch ran.
    assert_eq!(
        direct.max_timestamp, 9_000_000,
        "the complex-deletion marker must remain the fixture maximum"
    );
}

/// Issue #4246 roborev round-8 finding 1 — the UNIT-level pin of the
/// adjudication documented on [`fold_static_carrier_stats`]: a static-row
/// carrier's own row deletion, in EITHER representation
/// (`CellOperation::DeleteRow` or the #932 decoupled
/// `Mutation::row_tombstone`), must reach persisted stats NOWHERE, because no
/// production path emits a static-row deletion to Data.db.
///
/// Rounds 6/7 folded it here; that was a PHANTOM marker. The end-to-end
/// counterpart (emitted flags byte + persisted histogram, through the real
/// `WriteEngine` flush) is
/// `cqlite-core/tests/issue_4246_static_carrier_deletion_phantom.rs`.
#[test]
fn static_carrier_row_deletion_is_never_folded() {
    for (label, carrier) in [
        (
            "DeleteRow op",
            Mutation::new(
                table(),
                pk(),
                None,
                vec![CellOperation::DeleteRow],
                250,
                None,
            )
            .with_local_deletion_time(STATIC_CARRIER_DELETE_LDT),
        ),
        (
            "#932 decoupled row_tombstone",
            Mutation::new(
                table(),
                pk(),
                None,
                vec![CellOperation::Write {
                    column: "s".to_string(),
                    value: Value::text("static".to_string()),
                }],
                850,
                None,
            )
            .with_row_tombstone(250, STATIC_CARRIER_DELETE_LDT),
        ),
    ] {
        let mut stats = StatisticsMetadata::new();
        fold_static_carrier_stats(&mut stats, &carrier);
        assert_eq!(
            stats.tombstone_histogram.total_observations(),
            0,
            "{label}: a static carrier's row deletion is never emitted, so it \
             must add 0 RECOGNISED tombstone-drop-time observations"
        );
        assert_eq!(
            stats.min_local_deletion_time,
            i32::MAX,
            "{label}: min_local_deletion_time must stay at its untouched \
             default — folding {STATIC_CARRIER_DELETE_LDT} here is the \
             roborev round-6/7 phantom"
        );
    }
}

/// LIVE CONTROL for the assertion above: the SAME two representations on a
/// CLUSTERING row ARE emitted (`merge_row_group` → `RowWrite.row_deletion` →
/// `ROW_HAS_DELETION`) and so MUST each be folded exactly once. Without this,
/// `static_carrier_row_deletion_is_never_folded` would pass just as well if
/// row deletions had stopped being folded anywhere at all.
#[test]
fn clustering_row_deletion_is_still_folded_exactly_once() {
    let schema = fold_schema();
    let schema_has_static = schema.columns.iter().any(|c| c.is_static);
    for (label, row) in [
        (
            "DeleteRow op",
            Mutation::new(
                table(),
                pk(),
                Some(ck(1)),
                vec![CellOperation::DeleteRow],
                250,
                None,
            )
            .with_local_deletion_time(STATIC_CARRIER_DELETE_LDT),
        ),
        (
            "#932 decoupled row_tombstone",
            Mutation::new(
                table(),
                pk(),
                Some(ck(1)),
                vec![CellOperation::Write {
                    column: "v".to_string(),
                    value: Value::text("live".to_string()),
                }],
                850,
                None,
            )
            .with_row_tombstone(250, STATIC_CARRIER_DELETE_LDT),
        ),
    ] {
        let mut stats = StatisticsMetadata::new();
        fold_single_mutation_row_group(&mut stats, &row, &schema, schema_has_static, None);
        assert_eq!(
            stats.tombstone_histogram.total_observations(),
            1,
            "{label}: an EMITTED clustering-row deletion must be folded \
             exactly once"
        );
        assert_eq!(
            stats.min_local_deletion_time, STATIC_CARRIER_DELETE_LDT,
            "{label}: the emitted deletion's LDT must reach persisted minima"
        );
    }
}

/// Guard: an EMPTY partition-scoped sub-fold (a partition with no
/// tombstones/TTLs/live cells reaching this fold at all) must merge as a
/// true no-op — proving the untouched-default-sentinel guards in
/// `merge_stats_fold` (`max_local_deletion_time > i32::MIN`, `min_ttl !=
/// i32::MAX`) actually prevent corruption, not just coincidentally pass
/// on the representative fixture above.
#[test]
fn merging_an_empty_sub_fold_is_a_no_op() {
    let mut into = StatisticsMetadata::new();
    fold_mutation_stats(
        &mut into,
        &Mutation::new(
            table(),
            pk(),
            Some(ck(1)),
            vec![CellOperation::WriteWithTtl {
                column: "v".to_string(),
                value: Value::text("x".to_string()),
                ttl_seconds: 60,
                local_deletion_time: Some(1_000),
            }],
            100,
            Some(60),
        ),
    );
    let before = FoldSnapshot::from(&into);

    let empty = StatisticsMetadata::new();
    merge_stats_fold(&mut into, &empty);

    assert_eq!(
        FoldSnapshot::from(&into),
        before,
        "merging a never-folded (default) StatisticsMetadata must not \
         change min_local_deletion_time to i32::MIN or max_ttl to i32::MAX"
    );
}

/// Direct unit coverage of `fold_row_content_stats(Some(dts))`,
/// `row_group_survival`, and `fold_row_deletion_marker` in isolation
/// (issue #4246 roborev round-2 finding: the tests above only exercise
/// `shadow_boundary = None` via `fold_mutation_stats`, so drift in the
/// `Some(dts)` gating logic was invisible to this module's own suite).
mod shadow_gating {
    use super::*;
    use crate::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};

    fn schema() -> TableSchema {
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
                Column {
                    name: "id".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "ck".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "v".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: Default::default(),
            dropped_columns: Default::default(),
        }
    }

    fn insert(ck_val: i32, ts: i64) -> Mutation {
        Mutation::new(
            table(),
            pk(),
            Some(ck(ck_val)),
            vec![CellOperation::Write {
                column: "v".to_string(),
                value: Value::text("x".to_string()),
            }],
            ts,
            None,
        )
    }

    fn delete_row(ck_val: i32, ts: i64) -> Mutation {
        Mutation::new(
            table(),
            pk(),
            Some(ck(ck_val)),
            vec![CellOperation::DeleteRow],
            ts,
            None,
        )
    }

    /// A mutation fully below `shadow_boundary` folds nothing.
    #[test]
    fn fold_row_content_stats_below_boundary_folds_nothing() {
        let mut stats = StatisticsMetadata::new();
        fold_row_content_stats(&mut stats, &insert(1, 5), Some(10));
        assert_eq!(
            stats.min_timestamp,
            i64::MAX,
            "a mutation at ts=5 shadowed by dts=10 must fold nothing"
        );
    }

    /// A mutation strictly above `shadow_boundary` folds normally.
    #[test]
    fn fold_row_content_stats_above_boundary_folds_everything() {
        let mut stats = StatisticsMetadata::new();
        fold_row_content_stats(&mut stats, &insert(1, 15), Some(10));
        assert_eq!(stats.min_timestamp, 15);
    }

    /// A shadowed mutation's `ComplexDeletion` still folds when its OWN
    /// `marked_for_delete_at` strictly exceeds `dts` (issue #887/#921's
    /// per-op independence).
    #[test]
    fn fold_row_content_stats_shadowed_mutation_complex_deletion_survives_own_timestamp() {
        let mutation = Mutation::new(
            table(),
            pk(),
            Some(ck(1)),
            vec![CellOperation::ComplexDeletion {
                column: "tags".to_string(),
                marked_for_delete_at: 20,
                local_deletion_time: 2_000,
            }],
            5, // row timestamp is itself shadowed
            None,
        );
        let mut stats = StatisticsMetadata::new();
        fold_row_content_stats(&mut stats, &mutation, Some(10));
        assert_eq!(
            stats.min_timestamp, 20,
            "the marker's own mfda=20 exceeds dts=10, so it must survive \
             even though the carrying mutation's row ts=5 is shadowed"
        );
    }

    /// The exact roborev round-2 finding #1 scenario: two `DeleteRow`s
    /// for the same clustering key in one group, `DeleteRow@5` and
    /// `DeleteRow@20`. `resolve_row_deletion` picks 20 (the winner);
    /// only it may ever reach persisted stats. The losing `DeleteRow@5`
    /// must fold NOTHING via `fold_row_content_stats` (no
    /// `carries_row_deletion` self-exemption), and the winning marker is
    /// folded exactly once via `fold_row_deletion_marker`, not per-mutation.
    #[test]
    fn losing_delete_row_in_same_group_does_not_lower_persisted_minimum() {
        let losing = delete_row(1, 5);
        let winning = delete_row(1, 20);
        let group: Vec<&Mutation> = vec![&losing, &winning];

        let (survives, deletion_ts, row_deletion) =
            row_group_survival(&group, &schema(), false, None);
        assert!(
            survives,
            "a DeleteRow group still produces a row (the tombstone)"
        );
        assert_eq!(
            deletion_ts,
            Some(20),
            "the winning DeleteRow's ts resolves the boundary"
        );
        assert_eq!(
            row_deletion.map(|(ts, _)| ts),
            Some(20),
            "resolve_row_deletion must pick the NEWER DeleteRow, not the older one"
        );

        let mut stats = StatisticsMetadata::new();
        for m in &group {
            fold_row_content_stats(&mut stats, m, deletion_ts);
        }
        assert_eq!(
            stats.min_timestamp,
            i64::MAX,
            "fold_row_content_stats must fold NOTHING for either DeleteRow \
             mutation — both are `mutation_shadowed` (ts <= dts=20) under the \
             uniform check, with no self-exemption for the winner"
        );

        fold_row_deletion_marker(&mut stats, row_deletion);
        assert_eq!(
            stats.min_timestamp, 20,
            "the group's winning row-deletion marker (ts=20), folded exactly \
             once, is the ONLY way this group's timestamp reaches stats — the \
             losing DeleteRow@5 must never lower the persisted minimum"
        );
    }

    /// `fold_marker_stats` folds a partition/range tombstone
    /// unconditionally, independent of any `shadow_boundary` concept (it
    /// takes no such parameter) — a tombstone marker is never itself
    /// row-shadowed.
    #[test]
    fn fold_marker_stats_folds_partition_tombstone_unconditionally() {
        let mut mutation = Mutation::new(table(), pk(), None, vec![], 999, None);
        mutation.partition_tombstone = Some(PartitionTombstone {
            deletion_time: 42,
            local_deletion_time: 4_242,
        });
        let mut stats = StatisticsMetadata::new();
        fold_marker_stats(&mut stats, &mutation);
        assert_eq!(stats.min_timestamp, 42);
        assert!(stats.has_partition_level_deletions);
    }

    /// Roborev round-3 finding #3: the `carries_static` production
    /// callers always pass `shadow_boundary = None` to
    /// `fold_row_content_stats` (static-cell shadowing uses a separate
    /// mechanism), which used to make `mutation_shadowed` unconditionally
    /// `false` — so a `DeleteRow`-carrying mutation on that path
    /// double-folded its LDT into the tombstone-drop-time histogram: once
    /// via the group-level `fold_row_deletion_marker`, and again via
    /// `fold_row_content_stats`'s own (now-removed) `DeleteRow` handling.
    /// Reproduces the exact `carries_static` call shape
    /// (`fold_row_deletion_marker` once, then
    /// `fold_row_content_stats(mutation, None)`) and asserts the
    /// histogram observes the tombstone exactly once.
    #[test]
    fn delete_row_on_carries_static_path_folds_ldt_exactly_once() {
        let mutation = delete_row(1, 42);
        let group: Vec<&Mutation> = vec![&mutation];
        let (survives, _deletion_ts, row_deletion) =
            row_group_survival(&group, &schema(), false, None);
        assert!(
            survives,
            "a lone DeleteRow group still produces a tombstone row"
        );

        let mut stats = StatisticsMetadata::new();
        // Exactly the `carries_static` branch's call shape: the group's
        // marker folded once, then per-mutation content folded with
        // `shadow_boundary = None` (static-cell shadowing is a separate,
        // out-of-scope mechanism — see the `carries_static` call sites'
        // own doc comments).
        fold_row_deletion_marker(&mut stats, row_deletion);
        fold_row_content_stats(&mut stats, &mutation, None);

        assert_eq!(
            stats.tombstone_histogram.total_observations(),
            1,
            "a DeleteRow on the carries_static path must fold its LDT into \
             the tombstone-drop-time histogram exactly once, not twice \
             (total_observations, NOT size() — size() counts distinct \
             bins, and a double-fold of the SAME LDT lands in one bin \
             either way, roborev round-4 HIGH finding)"
        );
    }

    /// The extracted single-mutation-group helper both `KWayMerger::merge`
    /// and `WriteEngine::maintenance_step` call (issue #4246 roborev
    /// round-3 finding #4) — exercised directly, the SAME function both
    /// production paths use, rather than only reachable through whichever
    /// integration test happens to exercise one of them.
    #[test]
    fn fold_single_mutation_row_group_folds_deletion_marker_exactly_once() {
        let mutation = delete_row(1, 42);
        let mut stats = StatisticsMetadata::new();
        fold_single_mutation_row_group(&mut stats, &mutation, &schema(), false, None);
        assert_eq!(stats.min_timestamp, 42);
        assert_eq!(
            stats.tombstone_histogram.total_observations(),
            1,
            "the row's own deletion marker must be folded exactly once \
             through the shared single-mutation-group helper \
             (total_observations, NOT size() — roborev round-4 HIGH finding)"
        );
    }

    /// A live, surviving mutation folds its content normally.
    #[test]
    fn fold_single_mutation_row_group_folds_surviving_content() {
        let mutation = insert(1, 99);
        let mut stats = StatisticsMetadata::new();
        fold_single_mutation_row_group(&mut stats, &mutation, &schema(), false, None);
        assert_eq!(stats.min_timestamp, 99);
    }

    /// A live mutation fully covered by `shadow_floor` folds nothing —
    /// proving the helper threads `shadow_floor` through to
    /// `row_group_survival`, not just a hardcoded `None`.
    #[test]
    fn fold_single_mutation_row_group_gates_shadowed_content() {
        let mutation = insert(1, 5);
        let mut stats = StatisticsMetadata::new();
        fold_single_mutation_row_group(&mut stats, &mutation, &schema(), false, Some(10));
        assert_eq!(
            stats.min_timestamp,
            i64::MAX,
            "a live mutation entirely covered by shadow_floor=10 must fold nothing"
        );
    }

    /// Roborev round-4 Medium finding: `merge_row_group` applies a SECOND,
    /// per-cell shadow filter (`data_writer/rows.rs`, issue #1018) on top
    /// of the row-level `mutation_shadowed` check — a mutation whose ROW
    /// timestamp survives can still carry an individual cell whose OWN
    /// per-cell override timestamp (`Mutation::cell_write_timestamps`) is
    /// itself covered by `shadow_boundary`. Construct exactly that shape:
    /// row ts=100 (survives dts=50), but the `v` column's own per-cell
    /// override is ts=5 (covered by dts=50) — Data.db never emits this
    /// cell, so it must not fold either.
    #[test]
    fn fold_row_content_stats_gates_per_cell_override_independent_of_row_timestamp() {
        let mut mutation = insert(1, 100);
        mutation.cell_write_timestamps =
            Some(std::collections::HashMap::from([("v".to_string(), 5)]));

        let mut stats = StatisticsMetadata::new();
        fold_row_content_stats(&mut stats, &mutation, Some(50));
        assert_eq!(
            stats.min_timestamp, 100,
            "the row's own base timestamp (100, which survives dts=50) is \
             still folded; the shadowed per-cell override (5) must NOT be — \
             if it were, min_timestamp would incorrectly read 5"
        );
    }

    /// The companion control: when the per-cell override timestamp is
    /// ABOVE `shadow_boundary`, it folds normally (proving the gate is on
    /// the override value, not a blanket exclusion once any override is
    /// present).
    #[test]
    fn fold_row_content_stats_folds_surviving_per_cell_override() {
        let mut mutation = insert(1, 100);
        mutation.cell_write_timestamps =
            Some(std::collections::HashMap::from([("v".to_string(), 60)]));

        let mut stats = StatisticsMetadata::new();
        fold_row_content_stats(&mut stats, &mutation, Some(50));
        assert_eq!(
            stats.min_timestamp, 60,
            "a per-cell override (60) that survives shadow_boundary=50 must \
             lower min_timestamp below the row's own base timestamp (100)"
        );
    }
}
