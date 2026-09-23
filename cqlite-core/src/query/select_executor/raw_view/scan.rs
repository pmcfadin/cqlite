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
use super::row_passes_predicates;
use crate::query::result::QueryRow;
use crate::query::result_budget::{enforce_result_budget, estimate_query_row_bytes};
use crate::query::select_optimizer::SSTablePredicate;
use crate::schema::TableSchema;
use crate::storage::scan_cancel::ScanCancel;
use crate::storage::sstable::reader::SSTableReader;
use crate::{Error, Result};
use std::ops::ControlFlow;
use std::sync::Arc;

/// Stream every generation's full compaction walk, mapping each
/// [`CompactionRow`](crate::storage::sstable::reader::CompactionRow) into the
/// raw view's row shape as it arrives, bounded by `max_result_bytes` /
/// `max_result_rows` (design.md D9, spec's bounded-scan requirement). Never
/// routes through `KWayMerger`/`StorageEngine::scan` — this is the
/// deliberately UNRECONCILED per-generation full corpus.
///
/// `always_predicates`/`data_predicates` (roborev finding, issue #4222) are
/// applied to every mapped row HERE, inside the streaming callback, rather
/// than as a materialize-then-filter pass in the caller — which is what lets
/// `stop_after` (`Some(offset + limit)` for an explicit `LIMIT`) stop the
/// walk once enough ACCEPTED rows are collected, instead of decoding the
/// whole corpus to serve `LIMIT 1`.
#[allow(clippy::too_many_arguments)]
pub(in crate::query::select_executor) async fn raw_view_full_scan_rows(
    readers: &[Arc<SSTableReader>],
    schema: &TableSchema,
    always_predicates: &[&SSTablePredicate],
    data_predicates: &[&SSTablePredicate],
    max_result_bytes: usize,
    max_result_rows: usize,
    stop_after: Option<usize>,
) -> Result<Vec<QueryRow>> {
    let scan_cancel = ScanCancel::new();
    let mut out: Vec<QueryRow> = Vec::new();
    let mut running_bytes: usize = 0;

    'generations: for reader in readers {
        scan_cancel.check()?;

        // Fail closed rather than fabricate metadata (roborev finding, issue
        // #4222): a non-`nb`-format, non-BTI reader's compaction stream
        // collapses through `CompactionRow::from_legacy_value`, which zeroes
        // every live cell's timestamp/ttl/local-deletion-time and drops
        // complex cells and row liveness entirely — correct for compaction's
        // own reconciliation (which never reads those fields back), but each
        // is a FABRICATED value if this view rendered it as an authoritative
        // on-disk fact (no-heuristics, issue #28). See
        // `compaction_stream_loses_cell_metadata`'s doc: every real
        // Cassandra 5.0 `nb` SSTable (compressed or not) is classified
        // `is_nb_format() == true` and never takes this branch — this is a
        // defensive guard for a shape no committed fixture exercises.
        if reader.compaction_stream_loses_cell_metadata() {
            return Err(Error::unsupported_query(format!(
                "raw SSTable view: '{}' is a non-'nb'-format BIG SSTable whose compaction \
                 stream does not preserve per-cell write metadata — this view cannot surface \
                 fabricated timestamps/TTLs as authoritative facts (issue #4222)",
                reader.file_path().display()
            )));
        }

        // `position` is left `None` on the full-scan path (module doc):
        // exposing a per-partition byte offset from this streaming walk needs
        // the walk itself to surface it, which nothing in this codebase does
        // today — a documented scope limitation, never a heuristic guess.
        let source = RawViewSource::from_reader(reader, None);

        let mut budget_exceeded = false;
        let mut stop_reached = false;
        reader
            .stream_all_partitions_for_compaction(Some(schema), &scan_cancel, |crow| {
                let mapped = match map_compaction_row(crow, schema, &source) {
                    Ok(rows) => rows,
                    Err(e) => return Err(e),
                };
                for row in mapped {
                    match row_passes_predicates(&row, always_predicates, data_predicates) {
                        Ok(true) => {}
                        Ok(false) => continue,
                        Err(e) => return Err(e),
                    }
                    running_bytes = running_bytes.saturating_add(estimate_query_row_bytes(&row));
                    out.push(row);
                    // An explicit LIMIT (`stop_after.is_some()`) exempts the
                    // row-count safety valve (issue #1578) — mirroring
                    // `PointCollector::push` in `point.rs` (roborev finding,
                    // issue #4222: the two producers previously disagreed,
                    // so a `LIMIT` on a full-scan query could spuriously
                    // trip `max_result_rows` while the SAME `LIMIT` on a
                    // partition-targeted query did not). The byte budget
                    // still guards memory either way.
                    if let Some(cap) = stop_after {
                        if out.len() >= cap {
                            stop_reached = true;
                            return Ok(ControlFlow::Break(()));
                        }
                        if running_bytes > max_result_bytes {
                            budget_exceeded = true;
                            return Ok(ControlFlow::Break(()));
                        }
                    } else if running_bytes > max_result_bytes || out.len() > max_result_rows {
                        budget_exceeded = true;
                        return Ok(ControlFlow::Break(()));
                    }
                }
                Ok(ControlFlow::Continue(()))
            })
            .await?;

        if stop_reached {
            break 'generations;
        }
        if budget_exceeded {
            // Enforce via the shared budget check so the error carries the
            // SAME `Error::ResultTooLarge` shape (budget/estimate/rows) every
            // other query path reports (design.md D9 — reuse, no new
            // mechanism). `usize::MAX` for the row-count valve under an
            // explicit LIMIT, matching the exemption applied above.
            let effective_max_rows = if stop_after.is_some() {
                usize::MAX
            } else {
                max_result_rows
            };
            enforce_result_budget(&out, running_bytes, max_result_bytes, effective_max_rows)?;
            break 'generations;
        }
    }

    Ok(out)
}
