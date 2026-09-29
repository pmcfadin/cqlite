//! Clustering-row grouping and shadow-floor resolution — the ONE place a
//! partition's clustering rows are selected, grouped and shadow-gated.
//!
//! Extracted by issue #4246 (roborev round-8 finding 2). Six call sites had
//! grown textually-identical copies of the shadow-floor derivation, three of
//! them wrapped in an identical filter-then-adjacency-group loop:
//!
//! | site | what it does with the group |
//! |------|------------------------------|
//! | `rows.rs::merge_clustering_rows` | EMITS the row (`merge_row_group`) — the authority |
//! | `writer/mod.rs::write_partition` | folds the group into persisted `StatisticsMetadata` |
//! | `writer/mod.rs::compute_mutations_baseline_stats` | folds the group into the #729 encoding baseline |
//! | `streaming_partition.rs::feed_row` / `incremental_partition.rs::feed_row` | emit one already-reconciled row |
//! | `merge/mod.rs` + `maintenance.rs` `PartitionEnd` drains | fold one already-reconciled row |
//!
//! Issue #4246's whole thesis is that the stats fold must make the SAME
//! decision the emitter makes. Copies of that decision can drift; a shared
//! function cannot. Sharing it here is therefore not merely tidiness — it is
//! the structural form of the property.
//!
//! LOAD-BEARING (do not "simplify" away): [`clustering_row_mutations`]'s
//! `!is_static_row_mutation` filter is what keeps the clustering-group fold
//! and the separate static-carrier fold DISJOINT. Drop it and a mutation
//! classified both ways is folded twice — and
//! `StatisticsMetadata::update_local_deletion_time` is not idempotent, so a
//! double fold inflates the persisted `estimatedTombstoneDropTime` histogram
//! Cassandra derives compaction scheduling from.

use super::{is_static_row_mutation, range_tombstone_covers};
use crate::schema::TableSchema;
use crate::storage::write_engine::mutation::{ClusteringKey, Mutation, RangeTombstone};

/// A partition's CLUSTERING-ROW mutations, in input order: every mutation
/// EXCEPT the static-row carriers, whose cells live in the static-row
/// prelude instead (`is_static_row_mutation`).
///
/// See this module's doc comment for why the exclusion is load-bearing.
pub(crate) fn clustering_row_mutations<'a>(
    mutations: impl IntoIterator<Item = &'a Mutation>,
    schema: &TableSchema,
) -> Vec<&'a Mutation> {
    mutations
        .into_iter()
        .filter(|m| !is_static_row_mutation(m, schema))
        .collect()
}

/// The shadow floor applying to `clustering_key`: the partition tombstone's
/// deletion timestamp raised by every range tombstone that COVERS that key.
/// `None` when nothing shadows it.
///
/// Mutations at or before this floor are already covered by a
/// partition/range tombstone, so they are never emitted (and must therefore
/// never reach persisted statistics either).
pub(crate) fn resolve_shadow_floor(
    partition_floor: Option<i64>,
    range_tombstones: &[RangeTombstone],
    clustering_key: Option<&ClusteringKey>,
    schema: &TableSchema,
) -> Option<i64> {
    let mut shadow_floor = partition_floor;
    for rt in range_tombstones {
        if range_tombstone_covers(rt, clustering_key, schema) {
            shadow_floor = Some(shadow_floor.map_or(rt.deletion_time, |f| f.max(rt.deletion_time)));
        }
    }
    shadow_floor
}

/// Visit each CLUSTERING-KEY GROUP of `row_mutations` (adjacent mutations
/// sharing a clustering key) together with the group's resolved shadow floor.
///
/// `row_mutations` must come from [`clustering_row_mutations`] and must
/// already be sorted by clustering key — grouping is by ADJACENCY, exactly as
/// `merge_clustering_rows` has always done, so an unsorted slice silently
/// splits one logical group in two. The one caller whose input is not
/// caller-sorted (`compute_mutations_baseline_stats`, reading a
/// memtable-internal slice) sorts it explicitly before calling.
pub(crate) fn for_each_clustering_row_group<'a, F>(
    row_mutations: &[&'a Mutation],
    schema: &TableSchema,
    partition_floor: Option<i64>,
    range_tombstones: &[RangeTombstone],
    mut visit: F,
) where
    F: FnMut(&[&'a Mutation], Option<i64>),
{
    let mut start = 0;
    while start < row_mutations.len() {
        let mut end = start + 1;
        while end < row_mutations.len()
            && row_mutations[end].clustering_key == row_mutations[start].clustering_key
        {
            end += 1;
        }
        let group = &row_mutations[start..end];
        let shadow_floor = resolve_shadow_floor(
            partition_floor,
            range_tombstones,
            group[0].clustering_key.as_ref(),
            schema,
        );
        visit(group, shadow_floor);
        start = end;
    }
}

#[cfg(test)]
#[path = "row_groups_tests.rs"]
mod tests;
