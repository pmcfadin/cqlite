//! Shared per-mutation statistics fold (issue #1668, stage 5c-iv part 2).
//!
//! [`SSTableWriter::write_partition`] folds every mutation of a partition into
//! `StatisticsMetadata` (min/max timestamp, LDT, TTL, the tombstone-drop-time
//! histogram, the live-LDT sentinel, and the partition-level-deletion flag)
//! BEFORE writing any bytes for that partition. The incremental streaming path
//! (`KWayMerger::merge`, `writer/incremental.rs`) sees mutations one at a time
//! and cannot buffer a whole partition to reproduce that ordering, but the
//! fold itself is a pure function of ONE mutation plus the running
//! accumulator — extracted here so both paths call the exact same logic and
//! can never drift.
//!
//! [`SSTableWriter::write_partition`]: super::SSTableWriter::write_partition

use crate::storage::sstable::writer::stats_writer::StatisticsMetadata;
use crate::storage::write_engine::mutation::{CellOperation, Mutation};

/// Fold one mutation's timestamp/TTL/local-deletion-time/tombstone information
/// into `stats`. Mirrors the per-mutation loop body that used to live inline in
/// [`super::SSTableWriter::write_partition`] verbatim — same chokepoints
/// (`update_timestamp`, `update_local_deletion_time`, `update_ttl`,
/// `note_live_local_deletion_time`, `mark_partition_level_deletion`), same
/// per-`CellOperation` handling, so folding every mutation of a partition
/// through this function (in any order — the chokepoints are commutative
/// min/max folds, issue #1668 stage 5a) reproduces `write_partition`'s final
/// `StatisticsMetadata` byte-for-byte.
///
/// Callers: `write_partition` (once per mutation, whole-partition buffered)
/// and the incremental streaming path (once per mutation, as it streams
/// through — see `writer/incremental.rs` doc comments for how the streamed
/// partition-scoped fold is merged into the running `SSTableWriter`-wide
/// `stats` at partition end).
pub(crate) fn fold_mutation_stats(stats: &mut StatisticsMetadata, mutation: &Mutation) {
    stats.update_timestamp(mutation.timestamp_micros);
    // Issue #1018: simple `Write`/`WriteWithTtl`/`Delete` cells may carry their
    // OWN (lower) per-cell timestamps in `Mutation::cell_write_timestamps` (a
    // live cell's writetime OR a cell tombstone's markedForDeleteAt) and are
    // emitted with an explicit `min_timestamp` delta. Fold every per-cell
    // timestamp into the stats BEFORE emitting cells so `min_timestamp` can
    // never exceed an emitted cell's actual timestamp (which would underflow
    // the unsigned-VInt delta). Mirrors the pre-pass fold in
    // `compute_mutations_baseline_stats`.
    if let Some(cell_ts) = &mutation.cell_write_timestamps {
        for ts in cell_ts.values() {
            stats.update_timestamp(*ts);
        }
    }
    if let Some(ttl) = mutation.ttl_seconds {
        stats.update_ttl(ttl as i32);
        let now_seconds = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i32)
            .unwrap_or(0);
        let local_deletion_time = now_seconds.saturating_add(ttl as i32);
        stats.update_local_deletion_time(local_deletion_time);
    }
    // Track local deletion times for tombstones and TTL cells. Issue #764:
    // row/cell tombstones use the caller-supplied `local_deletion_time` when
    // present, else the timestamp-derived value.
    for op in &mutation.operations {
        match op {
            CellOperation::WriteWithTtl {
                ttl_seconds,
                local_deletion_time,
                ..
            } => {
                stats.update_ttl(*ttl_seconds as i32);
                // Issue #1538: honor the authoritative per-cell LDT VERBATIM
                // when present (a surviving expiring cell preserved through
                // compaction); `None` keeps the historical `now + ttl`
                // derivation.
                let local_deletion_time = match local_deletion_time {
                    Some(ldt) => *ldt,
                    None => {
                        let now_seconds = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_secs() as i32)
                            .unwrap_or(0);
                        now_seconds.saturating_add(*ttl_seconds as i32)
                    }
                };
                stats.update_local_deletion_time(local_deletion_time);
            }
            op @ (CellOperation::Delete { .. } | CellOperation::DeleteRow) => {
                // Issue #764 / #921 finding 2: record the EXACT LDT the
                // tombstone is emitted with, via the same helper the emit path
                // uses, so stats and Data.db bytes agree exactly.
                let local_deletion_time =
                    crate::storage::sstable::writer::data_writer::op_cell_local_deletion_time(
                        op, mutation,
                    );
                stats.update_local_deletion_time(local_deletion_time);
            }
            // Issue #887: a `ComplexDeletion` marker is physically written with
            // its OWN `marked_for_delete_at` / `local_deletion_time`, which may
            // fall outside the row's own timestamp/LDT range.
            CellOperation::ComplexDeletion {
                marked_for_delete_at,
                local_deletion_time,
                ..
            } => {
                stats.update_timestamp(*marked_for_delete_at);
                stats.update_local_deletion_time(*local_deletion_time);
            }
            // Issue #887: a per-element complex cell carries its OWN explicit
            // timestamp/ttl/local_deletion_time.
            CellOperation::WriteComplexElement {
                timestamp_micros,
                ttl_seconds,
                local_deletion_time,
                is_deleted,
                ..
            } => {
                stats.update_timestamp(*timestamp_micros);
                if let Some(ttl) = ttl_seconds {
                    stats.update_ttl(*ttl as i32);
                }
                if let Some(ldt) = local_deletion_time {
                    stats.update_local_deletion_time(*ldt);
                }
                // Issue #1728 (roborev finding 2): a LIVE complex element
                // carries Cassandra's `NO_DELETION_TIME` sentinel just like a
                // live simple `Write`.
                if !*is_deleted && ttl_seconds.is_none() && local_deletion_time.is_none() {
                    stats.note_live_local_deletion_time();
                }
            }
            // Issue #1728: a live, non-TTL `Write` cell carries Cassandra's
            // `Cell.NO_DELETION_TIME` sentinel as its localDeletionTime.
            CellOperation::Write { value, .. } => {
                if mutation.ttl_seconds.is_none() && !matches!(value, crate::types::Value::Null) {
                    stats.note_live_local_deletion_time();
                }
            }
        }
    }
    // Track stats for partition tombstones.
    if let Some(pt) = &mutation.partition_tombstone {
        stats.update_timestamp(pt.deletion_time);
        stats.update_local_deletion_time(pt.local_deletion_time);
        stats.mark_partition_level_deletion();
    }
    // Track stats for range tombstones.
    for rt in &mutation.range_tombstones {
        stats.update_timestamp(rt.deletion_time);
        stats.update_local_deletion_time(rt.local_deletion_time);
    }
    // Issue #1721: a decoupled row tombstone (#932
    // `Mutation::row_tombstone = Some((deletion_time, ldt))`) is emitted as a
    // `HAS_DELETION` row stamped with its OWN `(deletion_time, ldt)` —
    // DECOUPLED from `timestamp_micros`, so the per-cell/mutation folds above
    // never see it.
    if let Some((deletion_time, ldt)) = mutation.row_tombstone {
        stats.update_timestamp(deletion_time);
        stats.update_local_deletion_time(ldt);
    }
}

/// Fold `from`'s accumulated range/flags into `into` (issue #1668 stage
/// 5c-iv part 2). Used to merge a partition-scoped fold (accumulated while
/// streaming a partition's mutations one at a time) into the SSTable-wide
/// running `StatisticsMetadata` at partition end, once the incremental
/// session's exclusive borrow of the writer has ended.
///
/// Min/max fields are commutative folds (stage 5a), so feeding both of
/// `from`'s min and max back through the same chokepoints on `into`
/// reproduces exactly what folding every mutation directly into `into` would
/// have produced — PROVIDED `from`'s own untouched-default sentinels are
/// excluded first. `update_timestamp` already self-filters its untouched
/// defaults (`i64::MAX`/`i64::MIN`, both treated as the LIVE/NO_DELETION
/// marker, issue #851), so timestamps are safe to feed unconditionally. LDT
/// and TTL are NOT symmetric:
///   * `update_local_deletion_time` filters `i32::MAX` (live sentinel) but
///     NOT `i32::MIN` (`from.max_local_deletion_time`'s untouched default
///     when no tombstone was ever folded) — guarded explicitly below, else an
///     empty `from` would drag `into.min_local_deletion_time` down to
///     `i32::MIN`.
///   * `update_ttl` only filters non-positive values, so `from.min_ttl`'s
///     untouched default (`i32::MAX`, itself positive) must be excluded
///     explicitly, else it would corrupt `into.max_ttl` to `i32::MAX`.
///
/// The live-LDT sentinel itself is NOT re-derived through
/// `update_local_deletion_time` (which filters `i32::MAX` OUT) —
/// `from.max_local_deletion_time == i32::MAX` is used as the equivalent "saw
/// a live cell" signal instead, since `note_live_local_deletion_time` is the
/// ONLY setter that can assign `i32::MAX` to `max_local_deletion_time` (a
/// real tombstone LDT is always filtered before it can reach the max).
pub(crate) fn merge_stats_fold(into: &mut StatisticsMetadata, from: &StatisticsMetadata) {
    into.update_timestamp(from.min_timestamp);
    into.update_timestamp(from.max_timestamp);

    into.update_local_deletion_time(from.min_local_deletion_time);
    if from.max_local_deletion_time > i32::MIN {
        into.update_local_deletion_time(from.max_local_deletion_time);
    }
    if from.max_local_deletion_time == i32::MAX {
        into.note_live_local_deletion_time();
    }

    if from.min_ttl != i32::MAX {
        into.update_ttl(from.min_ttl);
    }
    into.update_ttl(from.max_ttl);

    if from.has_partition_level_deletions {
        into.mark_partition_level_deletion();
    }
}

#[cfg(test)]
mod tests {
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

        let row_tombstone_row = Mutation::new(table(), pk(), Some(ck(1)), vec![], 300, None)
            .with_row_tombstone(50, 3_000);

        let ttl_row = Mutation::new(
            table(),
            pk(),
            Some(ck(2)),
            vec![CellOperation::WriteWithTtl {
                column: "v".to_string(),
                value: Value::Text("x".to_string()),
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
                    value: Some(Value::Text("b".to_string())),
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
                value: Value::Text("live".to_string()),
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

        vec![
            partition_only,
            range_only,
            row_tombstone_row,
            ttl_row,
            complex_deletion_row,
            complex_element_row,
            live_write_row,
            null_write_row,
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
    /// mutation of a partition DIRECTLY into one accumulator (what
    /// `write_partition` does — the old behavior) must produce the IDENTICAL
    /// final min/max/flag aggregates as the incremental path's split —
    /// folding disjoint SUBSETS of the same mutations into SEPARATE
    /// partition-scoped accumulators (simulating streaming them across
    /// several `feed_row`/`feed_static_row` calls) and then merging those
    /// sub-folds back together via `merge_stats_fold` (simulating
    /// `complete_partition_incremental`).
    #[test]
    fn split_and_merge_matches_direct_fold_for_every_mutation_kind() {
        let mutations = representative_mutations();

        // "write_partition"-equivalent: fold every mutation directly into one
        // running accumulator.
        let mut direct = StatisticsMetadata::new();
        for m in &mutations {
            fold_mutation_stats(&mut direct, m);
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
                0 => fold_mutation_stats(&mut part_a, m),
                1 => fold_mutation_stats(&mut part_b, m),
                _ => fold_mutation_stats(&mut part_c, m),
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
             merging must reproduce write_partition's direct fold exactly"
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
                    value: Value::Text("x".to_string()),
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
}
