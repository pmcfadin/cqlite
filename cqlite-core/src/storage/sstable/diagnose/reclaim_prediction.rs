//! `reclaim_at_now` (`design.md` D1/D5): what a full compaction of ONE generation would keep at
//! `--now`, predicted by driving the EXISTING [`KWayMerger`] in a dry, no-write run — the same
//! reconciliation/purge code a real compaction uses (`new_with_gc` + `with_now_secs`), never a
//! parallel re-implementation. Requires the table schema (the merge is schema-driven); without one
//! the prediction is honestly `unmeasured`.
//!
//! `rows_in` is NOT re-counted here: `--deep` already traverses the generation once through the
//! same merge-stream (purging OFF), and that traversal's row count is passed in — so the dry
//! compaction adds exactly ONE merger traversal (`gc_before`/`now` with `purge_safe(true)`; a
//! single generation is the whole input set, so the overlap gate is open — an optimistic, stated
//! assumption: other generations of the table may still hold shadowed data) instead of two. The
//! per-tombstone purge tally is internal to the merge and not exposed, so it is reported
//! unmeasured rather than approximated.

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
/// `rows_in` is the deep scan's unpurged row count over the same generation.
pub(crate) fn predict_reclaim(
    data_path: &Path,
    schema: &TableSchema,
    gc_before_secs: Option<i64>,
    now_secs: i64,
    rows_in: u64,
) -> Result<ReclaimPrediction> {
    let rows_out = count_rows(
        KWayMerger::new_with_gc(
            vec![data_path.to_path_buf()],
            schema,
            gc_before_secs,
            Some(now_secs),
        )?
        .with_purge_safe(true),
    )?;
    Ok(ReclaimPrediction { rows_in, rows_out })
}
