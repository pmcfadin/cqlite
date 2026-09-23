//! Point-key row producer for the raw SSTable view (issue #4222, design.md
//! D5's "point-key case"). Per candidate generation, in the order the reader
//! snapshot returns them:
//!
//! 1. [`SSTableReader::read_single_partition_for_compaction`] resolves the
//!    partition via the SAME bloom/BTI-pruned, offset-seeked primitive the
//!    Flight point-read path uses — never a full-table scan (spec: "pushed to
//!    the point-read path, never a scan"). Not present on the `tombstones`
//!    build (epic #951: that build compiles out the seek machinery entirely),
//!    which always takes the honest scan+filter fallback below instead —
//!    same rows, never a targeted access path.
//! 2. `IndexUnavailable` (no random-access index) degrades to scanning THIS
//!    ONE candidate's compaction stream, filtered to the requested key(s) —
//!    the same fail-safe `read_single_partition_for_compaction`'s own callers
//!    use; it costs speed, never correctness (#2295).
//! 3. Every candidate's rows are kept (never merged/reconciled — design.md D5's
//!    "why not KWayMerger"), tagged with that generation's source identity.

use super::row_map::{map_compaction_row, RawViewSource};
use super::row_passes_predicates;
use crate::query::result::QueryRow;
use crate::query::result_budget::{enforce_result_budget, estimate_query_row_bytes};
use crate::query::select_optimizer::SSTablePredicate;
use crate::schema::TableSchema;
use crate::storage::scan_cancel::ScanCancel;
use crate::storage::sstable::reader::{CompactionRow, SSTableReader};
use crate::{Error, Result};
use std::ops::ControlFlow;
use std::sync::Arc;

/// Accumulates accepted rows for the point-key path under the SAME
/// incremental byte/row-count bound the full-scan producer applies (roborev
/// finding, issue #4222): a wide partition's rows are pushed here as they
/// are decoded, never materialized in full before the budget is checked.
struct PointCollector {
    out: Vec<QueryRow>,
    running_bytes: usize,
    max_result_bytes: usize,
    max_result_rows: usize,
    /// `Some(offset + limit)` for an explicit `LIMIT` — stop once this many
    /// ACCEPTED rows are collected (mirrors the full-scan producer's
    /// `stop_after`).
    stop_after: Option<usize>,
}

impl PointCollector {
    fn new(max_result_bytes: usize, max_result_rows: usize, stop_after: Option<usize>) -> Self {
        Self {
            out: Vec::new(),
            running_bytes: 0,
            max_result_bytes,
            max_result_rows,
            stop_after,
        }
    }

    /// Push one accepted row, returning `true` when the caller should STOP
    /// (the `stop_after` cap was reached — not an error, just "enough").
    /// Returns `Err(Error::ResultTooLarge)` when the byte/row budget is
    /// exceeded WITHOUT an explicit `LIMIT` to exempt it (issue #1578).
    fn push(&mut self, row: QueryRow) -> Result<bool> {
        self.running_bytes = self
            .running_bytes
            .saturating_add(estimate_query_row_bytes(&row));
        self.out.push(row);
        if let Some(cap) = self.stop_after {
            if self.out.len() >= cap {
                return Ok(true);
            }
            // An explicit LIMIT exempts the row-count valve (issue #1578);
            // the byte budget still guards memory even under a LIMIT.
            if self.running_bytes > self.max_result_bytes {
                enforce_result_budget(
                    &self.out,
                    self.running_bytes,
                    self.max_result_bytes,
                    usize::MAX,
                )?;
            }
            return Ok(false);
        }
        if self.running_bytes > self.max_result_bytes || self.out.len() > self.max_result_rows {
            enforce_result_budget(
                &self.out,
                self.running_bytes,
                self.max_result_bytes,
                self.max_result_rows,
            )?;
        }
        Ok(false)
    }
}

/// Resolve the partition's `Data.db` byte offset via the SAME index the reader
/// already consults inside `read_single_partition_for_compaction` (issue
/// #4222's `position` column). A second, cheap index-only lookup — never a
/// decode — so the row producer does not have to thread the offset out of
/// that primitive's private seek path. `None` on any lookup miss/error: the
/// column is best-effort metadata, not a correctness signal (rows are already
/// resolved independently).
#[cfg_attr(feature = "tombstones", allow(dead_code))]
async fn resolve_position(reader: &SSTableReader, pk_bytes: &[u8]) -> Option<i64> {
    let offset = if reader.is_bti() {
        reader
            .lookup_partition_via_bti_trie(pk_bytes)
            .ok()
            .flatten()
    } else {
        reader
            .lookup_partition_with_index(pk_bytes)
            .await
            .ok()
            .flatten()
            .map(|(off, _size)| off)
    };
    offset.and_then(|off| i64::try_from(off).ok())
}

/// Map `row`, keep only the rows that pass the predicate backstop, and push
/// each into `collector`. Returns `true` when the caller should stop (the
/// `stop_after` cap was reached).
fn push_mapped_rows(
    row: CompactionRow,
    schema: &TableSchema,
    source: &RawViewSource,
    always_predicates: &[&SSTablePredicate],
    data_predicates: &[&SSTablePredicate],
    collector: &mut PointCollector,
) -> Result<bool> {
    for mapped in map_compaction_row(row, schema, source)? {
        if row_passes_predicates(&mapped, always_predicates, data_predicates)?
            && collector.push(mapped)?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Scan `reader`'s WHOLE compaction stream, forwarding only rows whose
/// partition key equals `pk_bytes` — the fail-safe every point-key path
/// (targeted or not) falls back to when no random-access index can be used.
/// `position` is `None` (no index was consulted).
///
/// Fails closed (roborev finding, issue #4222) rather than fabricate
/// metadata when `reader.compaction_stream_loses_cell_metadata()` — see
/// `scan.rs`'s identical check for the full-scan producer's fuller doc.
///
/// Pushes each matching row into `collector` DIRECTLY FROM THE STREAMING
/// CALLBACK (roborev finding, issue #4222 — round 6, correcting an earlier
/// version of this function that buffered every match into an intermediate
/// `Vec<CompactionRow>` and only pushed them into the budget-aware
/// `PointCollector` once the ENTIRE stream had already been consumed —
/// defeating both `PointCollector`'s and `raw_view_point_rows`'s own
/// documented "never fully materialized before the budget is checked"
/// contract, and meaning `stop_after` could never actually stop the walk
/// early under `feature = "tombstones"`, where this is the ONLY point-key
/// path). Returns `ControlFlow::Break` the moment the collector reports
/// "stop", so the underlying reader genuinely stops streaming.
///
/// Returns `true` when the caller should stop (the `stop_after` cap was
/// reached mid-stream).
async fn scan_and_filter_one_reader(
    reader: &SSTableReader,
    schema: &TableSchema,
    pk_bytes: &[u8],
    always_predicates: &[&SSTablePredicate],
    data_predicates: &[&SSTablePredicate],
    scan_cancel: &ScanCancel,
    collector: &mut PointCollector,
) -> Result<bool> {
    if reader.compaction_stream_loses_cell_metadata() {
        return Err(Error::unsupported_query(format!(
            "raw SSTable view: '{}' is a non-'nb'-format BIG SSTable whose compaction stream \
             does not preserve per-cell write metadata — this view cannot surface fabricated \
             timestamps/TTLs as authoritative facts (issue #4222)",
            reader.file_path().display()
        )));
    }
    let source = RawViewSource::from_reader(reader, None);
    let mut stopped = false;
    reader
        .stream_all_partitions_for_compaction(Some(schema), scan_cancel, |crow| {
            if crow.key.as_bytes() != pk_bytes {
                return Ok(ControlFlow::Continue(()));
            }
            if push_mapped_rows(
                crow,
                schema,
                &source,
                always_predicates,
                data_predicates,
                collector,
            )? {
                stopped = true;
                return Ok(ControlFlow::Break(()));
            }
            Ok(ControlFlow::Continue(()))
        })
        .await?;
    Ok(stopped)
}

/// Probe every candidate generation for one partition key, pushing every
/// physical row found (unreconciled) into `collector`. Returns `true` when
/// the caller should stop (the `stop_after` cap was reached).
#[cfg(not(feature = "tombstones"))]
async fn point_rows_for_key(
    readers: &[Arc<SSTableReader>],
    schema: &TableSchema,
    pk_bytes: &[u8],
    always_predicates: &[&SSTablePredicate],
    data_predicates: &[&SSTablePredicate],
    scan_cancel: &ScanCancel,
    collector: &mut PointCollector,
) -> Result<bool> {
    use crate::storage::sstable::reader::SinglePartitionCompaction;

    for reader in readers {
        scan_cancel.check()?;
        match reader
            .read_single_partition_for_compaction(pk_bytes, Some(schema), scan_cancel)
            .await?
        {
            SinglePartitionCompaction::DefinitelyAbsent => continue,
            SinglePartitionCompaction::Rows(rows) => {
                if rows.is_empty() {
                    continue;
                }
                let position = resolve_position(reader, pk_bytes).await;
                let source = RawViewSource::from_reader(reader, position);
                for row in rows {
                    if push_mapped_rows(
                        row,
                        schema,
                        &source,
                        always_predicates,
                        data_predicates,
                        collector,
                    )? {
                        return Ok(true);
                    }
                }
            }
            SinglePartitionCompaction::IndexUnavailable => {
                if scan_and_filter_one_reader(
                    reader,
                    schema,
                    pk_bytes,
                    always_predicates,
                    data_predicates,
                    scan_cancel,
                    collector,
                )
                .await?
                {
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}

/// The `tombstones`-build counterpart (epic #951 "honest paths"): that build
/// compiles out the seek machinery entirely, so every candidate is scanned
/// and filtered — the SAME fail-safe the default build's `IndexUnavailable`
/// arm uses, applied to every reader. Rows are byte-identical either way;
/// only the access path (never reported as targeted) differs.
#[cfg(feature = "tombstones")]
async fn point_rows_for_key(
    readers: &[Arc<SSTableReader>],
    schema: &TableSchema,
    pk_bytes: &[u8],
    always_predicates: &[&SSTablePredicate],
    data_predicates: &[&SSTablePredicate],
    scan_cancel: &ScanCancel,
    collector: &mut PointCollector,
) -> Result<bool> {
    for reader in readers {
        scan_cancel.check()?;
        if scan_and_filter_one_reader(
            reader,
            schema,
            pk_bytes,
            always_predicates,
            data_predicates,
            scan_cancel,
            collector,
        )
        .await?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Row producer entry point: probe every requested partition key against
/// every candidate generation, concatenating every physical row found. Never
/// routes through `KWayMerger`/`StorageEngine::scan*` (design.md D5/D6) — the
/// output is the deliberately UNRECONCILED per-generation contribution.
/// `always_predicates`/`data_predicates` (roborev finding, issue #4222) are the
/// SAME split the full-scan producer applies — see `row_passes_predicates`.
/// `max_result_bytes`/`max_result_rows`/`stop_after` bound accumulation
/// INCREMENTALLY (a second roborev finding, issue #4222): a wide targeted
/// partition across many generations is no longer fully materialized before
/// the budget is checked.
#[allow(clippy::too_many_arguments)]
pub(in crate::query::select_executor) async fn raw_view_point_rows(
    readers: &[Arc<SSTableReader>],
    schema: &TableSchema,
    keys: &[Vec<u8>],
    always_predicates: &[&SSTablePredicate],
    data_predicates: &[&SSTablePredicate],
    max_result_bytes: usize,
    max_result_rows: usize,
    stop_after: Option<usize>,
) -> Result<Vec<QueryRow>> {
    let scan_cancel = ScanCancel::new();
    let mut collector = PointCollector::new(max_result_bytes, max_result_rows, stop_after);
    for key in keys {
        if point_rows_for_key(
            readers,
            schema,
            key,
            always_predicates,
            data_predicates,
            &scan_cancel,
            &mut collector,
        )
        .await?
        {
            break;
        }
    }
    Ok(collector.out)
}
