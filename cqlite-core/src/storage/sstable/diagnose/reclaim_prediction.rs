//! `reclaim_at_now` (`design.md` D1/D5): what a full compaction of ONE generation would keep at
//! `--now`, predicted by driving the EXISTING [`KWayMerger`] in a dry, no-write run — the same
//! reconciliation/purge code a real compaction uses (`new_with_gc` + `with_now_secs`), never a
//! parallel re-implementation. Requires the table schema (the merge is schema-driven); without one
//! the prediction is honestly `unmeasured`.
//!
//! `rows_in` is the merger's row count with purging OFF (`gc_before = None`), `rows_out` the same
//! generation merged at `gcBefore`/`now` with `purge_safe(true)` (a single generation is the whole
//! input set, so the overlap gate is open — an optimistic, stated assumption: other generations of
//! the table may still hold shadowed data). The per-tombstone purge tally is internal to the merge
//! and not exposed, so it is reported unmeasured rather than approximated.

use std::path::Path;

use crate::schema::TableSchema;
use crate::storage::write_engine::merge::{KWayMerger, MergeStep};
use crate::Result;

/// Rows a dry compaction of one generation would read and keep at `now`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReclaimPrediction {
    pub rows_in: u64,
    pub rows_out: u64,
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

/// Dry-run prediction for `data_path` (never writes, never touches other generations).
pub(crate) fn predict_reclaim(
    data_path: &Path,
    schema: &TableSchema,
    gc_before_secs: Option<i64>,
    now_secs: i64,
) -> Result<ReclaimPrediction> {
    let paths = vec![data_path.to_path_buf()];
    let rows_in = count_rows(KWayMerger::new_with_gc(
        paths.clone(),
        schema,
        None,
        Some(now_secs),
    )?)?;
    let rows_out = count_rows(
        KWayMerger::new_with_gc(paths, schema, gc_before_secs, Some(now_secs))?
            .with_purge_safe(true),
    )?;
    Ok(ReclaimPrediction { rows_in, rows_out })
}
