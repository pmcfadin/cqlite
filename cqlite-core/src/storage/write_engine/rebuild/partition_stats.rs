//! Per-partition `Statistics.db` fold for rebuild's PASS 2 (issue #4197;
//! issue #4246 doctrine).
//!
//! Split out of `components.rs` under the campsite rule (CLAUDE.md): this
//! one function's derivation — why each branch is provably inert for
//! rebuild's decode-reconciled input — is most of its length, and none of it
//! is orchestration.

use crate::schema::TableSchema;
use crate::storage::sstable::writer::data_writer::{is_static_row_mutation, resolve_shadow_floor};
use crate::storage::sstable::writer::stats_fold::{
    fold_single_mutation_row_group, fold_static_carrier_stats,
};
use crate::storage::sstable::writer::StatisticsMetadata;
use crate::storage::write_engine::mutation::{Mutation, PartitionTombstone, RangeTombstone};

/// Fold one decoded partition's mutations into the running whole-SSTable
/// `stats` accumulator (issue #4197 PASS 2; issue #4246 doctrine).
///
/// Partition/range tombstone MARKERS are folded unconditionally from the
/// caller's own authoritative extraction (`partition_tombstone`/
/// `range_tombstones`, already reduced to "one winning PT, every surviving
/// RT") — they are never row-shadowed, they ARE the deletion, exactly
/// mirroring `SSTableWriter::write_partition`'s own marker fold
/// (`writer/mod.rs`, issue #4246).
///
/// Everything else is folded per-mutation via `fold_single_mutation_row_group`
/// (the SAME per-mutation fold `KWayMerger::merge`'s `PartitionEnd` handling
/// and `WriteEngine::maintenance_step`'s buffered `PartitionEnd` drain use),
/// rather than the group-oriented `for_each_clustering_row_group` /
/// `clustering_row_mutations` machinery `write_partition` needs for RAW,
/// pre-reconciliation buffered input. `mutations` here is NOT raw input: it
/// is `decode_one_partition`'s output, itself `merge_partition_rows`'s
/// (`write_engine/merge/mod.rs`) reconciled result, which has ALREADY
/// applied `apply_range_shadowing`/`apply_partition_shadowing` to every
/// clustering-keyed cluster before this function ever sees it — so the
/// per-mutation `resolve_shadow_floor` gate below is PROVABLY INERT for this
/// input (it can exclude nothing `merge_partition_rows` has not already
/// excluded). It stays, not because it does live work here, but because it
/// makes this fold provably agree with the scratch `DataWriter`'s own
/// emission decision BY CONSTRUCTION, rather than depending on an upstream
/// invariant inside `decode`/`merge` that a future change there could break
/// silently. In that sense this function is a compile-time fix (routing
/// through the shared, already-#4246-hardened primitives instead of the
/// now-test-only `fold_mutation_stats`), not a bug fix: the OLD blind fold
/// was never actually wrong for rebuild's input, because nothing shadow-able
/// survives into `mutations` in the first place.
///
/// One subtlety `fold_single_mutation_row_group`'s own docs do not spell out
/// for THIS caller: NOT every `clustering_key: None` mutation here belongs
/// to a single reconciled clustering group. `merge_partition_rows` emits up
/// to three DISTINCT `None`-keyed shapes per partition — the one reconciled
/// `None`-clustered cluster (an unclustered table's sole row, or a
/// static-row carrier, if the schema has static columns), PLUS one carrier
/// entry per surviving coalesced range tombstone, PLUS one carrier for the
/// partition tombstone (`merge/mod.rs`'s range-tombstone and
/// partition-tombstone re-emit loops) — so splitting them into singleton
/// per-mutation folds (instead of one grouped fold, as
/// `SSTableWriter::write_partition` would for raw buffered input) is safe
/// for two INDEPENDENT reasons, not because "every clustering key is already
/// one mutation":
///
///   1. A marker-only carrier mutation folds to nothing under
///      `fold_single_mutation_row_group`: `merge_entry_to_mutation`
///      (`merge/mod.rs`) gives the partition-tombstone carrier empty
///      `operations` and no row deletion (it returns early, before the
///      operation builder ever runs), and the range-tombstone carrier empty
///      `operations` with `row_deletion` unset too — so `merge_row_group`
///      produces no row for either, matching its own doc comment ("a
///      mutation that exists only to carry a partition or range tombstone").
///   2. Splitting the group cannot double-fold a row's OWN deletion marker
///      (`fold_row_deletion_marker`, whose `update_local_deletion_time` call
///      is NOT idempotent — it increments a histogram bucket) because AT
///      MOST ONE `None`-keyed mutation per partition can carry one: only the
///      single reconciled `None`-clustered entry can; the two marker
///      carriers structurally cannot, per point 1.
///
/// `is_static_row_mutation` (`data_writer::encoding`) also classifies a
/// marker-only carrier as a "static carrier" whenever the schema has any
/// static column — not because it IS one, but because `Iterator::all` over
/// its EMPTY `operations` is vacuously `true`. This is harmless: the static
/// branch (`fold_static_carrier_stats`) only folds `update_timestamp`
/// (idempotent) from a mutation with no ops to iterate, so a carrier
/// misclassified this way contributes nothing beyond what the unconditional
/// marker fold above already folded. It IS, however, load-bearing for this
/// function's branch disjointness, and worth re-deriving by hand — not
/// assumed — before ever "simplifying" this predicate away.
pub(super) fn fold_partition_statistics(
    stats: &mut StatisticsMetadata,
    mutations: &[Mutation],
    partition_tombstone: Option<&PartitionTombstone>,
    range_tombstones: &[RangeTombstone],
    schema: &TableSchema,
    schema_has_static: bool,
) {
    if let Some(pt) = partition_tombstone {
        stats.update_timestamp(pt.deletion_time);
        stats.update_local_deletion_time(pt.local_deletion_time);
        stats.mark_partition_level_deletion();
    }
    for rt in range_tombstones {
        stats.update_timestamp(rt.deletion_time);
        stats.update_local_deletion_time(rt.local_deletion_time);
    }

    let partition_floor = partition_tombstone.map(|pt| pt.deletion_time);
    for mutation in mutations {
        if is_static_row_mutation(mutation, schema) {
            fold_static_carrier_stats(stats, mutation);
            continue;
        }
        let shadow_floor = resolve_shadow_floor(
            partition_floor,
            range_tombstones,
            mutation.clustering_key.as_ref(),
            schema,
        );
        fold_single_mutation_row_group(stats, mutation, schema, schema_has_static, shadow_floor);
    }
}

#[cfg(test)]
#[path = "partition_stats_tests.rs"]
mod tests;
