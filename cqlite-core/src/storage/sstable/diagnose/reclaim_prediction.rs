//! `reclaim_at_now` (`design.md` D1/D5): what `cqlite compact --major` of the WHOLE table would read
//! and write at `--now`, predicted by driving the EXISTING [`KWayMerger`] in a dry, no-write run
//! over the same input set with the same gc/now/purge settings the real major compaction uses
//! (`compact_sstables_with_registry`, `purge_safe = true`): never a parallel re-implementation of
//! the merge. Requires the table schema (the merge is schema-driven); without one the prediction is
//! honestly `unmeasured`.
//!
//! This is the major-compaction prediction itself, table-wide (owner ruling 2026-11-14): every
//! generation is merged TOGETHER, so a tombstone that shadows data in another generation is kept or
//! purged exactly as the real merge would decide. Mirroring the real path's preparation:
//!
//! 1. inputs ordered newest-to-oldest (generation id descending; index 0 wins ties);
//! 2. the fully-expired drop (`fully_expired_sstables(&inputs, &[], gc_before)`, outside set empty
//!    because a major compaction spans every SSTable), whose members are excluded from the merge and
//!    reported as `dropped_whole`, with the all-dropped guard (`split_merge_and_dropped`);
//! 3. the effective (decode) schema from the inputs' headers (`effective_compaction_schema`,
//!    `apply_udt_marshals_from_inputs`);
//! 4. `KWayMerger::new_with_gc_and_registry(.., gc_before, Some(now), None).with_purge_safe(true)`.
//!
//! `rows_in` is the rows of the generations that are actually merged, as `--deep` already streamed
//! them (purging OFF) -- so the dry compaction adds exactly ONE merger traversal. The per-tombstone
//! purge tally is internal to the merge and not exposed, so it is reported unmeasured rather than
//! approximated.

use std::path::PathBuf;

use crate::schema::TableSchema;
use crate::storage::write_engine::merge::{
    apply_udt_marshals_from_inputs, effective_compaction_schema, fully_expired_sstables,
    split_merge_and_dropped, KWayMerger, MergeStep,
};
use crate::Result;

/// What a dry major compaction of the whole table would read and keep at `now`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReclaimPrediction {
    /// Rows read from the generations that are merged (dropped-whole generations are not read).
    pub rows_in: u64,
    /// Rows the merge keeps and would write.
    pub rows_out: u64,
    /// Generations merged, newest first.
    pub merged_generations: Vec<String>,
    /// Generations dropped whole as fully expired (excluded from the merge, reclaimed after publish).
    pub dropped_whole: Vec<String>,
}

/// One input generation of the major compaction.
#[derive(Debug, Clone)]
pub(crate) struct ReclaimInput {
    pub data_path: PathBuf,
    pub generation: String,
    /// Rows `--deep` streamed from this generation (unpurged).
    pub rows_scanned: u64,
}

fn count_rows(mut merger: KWayMerger) -> Result<u64> {
    let mut rows = 0u64;
    loop {
        match merger.step()? {
            MergeStep::Partition { rows: r, .. } => rows += r.len() as u64,
            MergeStep::Complete => return Ok(rows),
        }
    }
}

/// Dry-run prediction over `inputs` (newest first). Never writes.
pub(crate) fn predict_table_reclaim(
    inputs: &[ReclaimInput],
    schema: &TableSchema,
    gc_before_secs: Option<i64>,
    now_secs: i64,
) -> Result<ReclaimPrediction> {
    let paths: Vec<PathBuf> = inputs.iter().map(|i| i.data_path.clone()).collect();
    let drop_set = if gc_before_secs.is_some() {
        fully_expired_sstables(&paths, &[], gc_before_secs)
    } else {
        Vec::new()
    };
    let (merge_inputs, dropped) = split_merge_and_dropped(&paths, drop_set);

    let mut effective = effective_compaction_schema(schema, &merge_inputs);
    apply_udt_marshals_from_inputs(&mut effective, &merge_inputs, None)?;
    let merger = KWayMerger::new_with_gc_and_registry(
        merge_inputs.clone(),
        &effective,
        gc_before_secs,
        Some(now_secs),
        None,
    )?
    .with_purge_safe(true);
    let rows_out = count_rows(merger)?;

    let generation_of = |p: &PathBuf| {
        inputs
            .iter()
            .find(|i| &i.data_path == p)
            .map(|i| i.generation.clone())
            .unwrap_or_default()
    };
    let rows_in = inputs
        .iter()
        .filter(|i| merge_inputs.contains(&i.data_path))
        .map(|i| i.rows_scanned)
        .sum();
    Ok(ReclaimPrediction {
        rows_in,
        rows_out,
        merged_generations: merge_inputs.iter().map(generation_of).collect(),
        dropped_whole: dropped.iter().map(generation_of).collect(),
    })
}
