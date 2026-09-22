//! Bounded full-scan row producer for the raw SSTable view (issue #4222,
//! design.md D5's "no-predicate case" / D9's streaming requirement).
//!
//! Drives [`SSTableReader::stream_all_partitions_for_compaction`] — a true
//! per-generation STREAM, never a materializing `Vec` collection of the whole
//! SSTable (the module's own doc: "true-streams each partition as the index
//! walk resolves it") — once per candidate generation, emitting one/two
//! [`QueryRow`]s per [`CompactionRow`] as they arrive. The running byte
//! estimate is checked INSIDE the callback so a query with no `WHERE`
//! predicate against a huge corpus stops decoding at the budget instead of
//! materializing it all first and failing after the fact (spec: "bounded by
//! the existing result-byte budget, not table size").
//!
//! `max_result_bytes`/`max_result_rows` are explicit parameters (never a
//! `collect::<Vec<_>>()` with no bound) so the gate's `oom-audit` component
//! recognizes this as a BOUNDED scan-shaped function
//! (`xtask/src/oom_audit/rule.rs::fn_is_bounded`, design.md D9).

use super::row_map::{map_compaction_row, RawViewSource};
use crate::query::result::QueryRow;
use crate::query::result_budget::{enforce_result_budget, estimate_query_row_bytes};
use crate::schema::TableSchema;
use crate::storage::scan_cancel::ScanCancel;
use crate::storage::sstable::reader::SSTableReader;
use crate::Result;
use std::ops::ControlFlow;
use std::sync::Arc;

/// Stream every generation's full compaction walk, mapping each
/// [`CompactionRow`](crate::storage::sstable::reader::CompactionRow) into the
/// raw view's row shape as it arrives, bounded by `max_result_bytes` /
/// `max_result_rows` (design.md D9, spec's bounded-scan requirement). Never
/// routes through `KWayMerger`/`StorageEngine::scan` — this is the
/// deliberately UNRECONCILED per-generation full corpus.
pub(in crate::query::select_executor) async fn raw_view_full_scan_rows(
    readers: &[Arc<SSTableReader>],
    schema: &TableSchema,
    max_result_bytes: usize,
    max_result_rows: usize,
) -> Result<Vec<QueryRow>> {
    let scan_cancel = ScanCancel::new();
    let mut out: Vec<QueryRow> = Vec::new();
    let mut running_bytes: usize = 0;

    'generations: for reader in readers {
        scan_cancel.check()?;
        // `position` is left `None` on the full-scan path (module doc):
        // exposing a per-partition byte offset from this streaming walk needs
        // the walk itself to surface it, which nothing in this codebase does
        // today — a documented scope limitation, never a heuristic guess.
        let source = RawViewSource::from_reader(reader, None);

        let mut budget_exceeded = false;
        reader
            .stream_all_partitions_for_compaction(Some(schema), &scan_cancel, |crow| {
                let mapped = match map_compaction_row(crow, schema, &source) {
                    Ok(rows) => rows,
                    Err(e) => return Err(e),
                };
                for row in mapped {
                    running_bytes = running_bytes.saturating_add(estimate_query_row_bytes(&row));
                    out.push(row);
                }
                if running_bytes > max_result_bytes || out.len() > max_result_rows {
                    budget_exceeded = true;
                    return Ok(ControlFlow::Break(()));
                }
                Ok(ControlFlow::Continue(()))
            })
            .await?;

        if budget_exceeded {
            // Enforce via the shared budget check so the error carries the
            // SAME `Error::ResultTooLarge` shape (budget/estimate/rows) every
            // other query path reports (design.md D9 — reuse, no new
            // mechanism).
            enforce_result_budget(&out, running_bytes, max_result_bytes, max_result_rows)?;
            break 'generations;
        }
    }

    Ok(out)
}
