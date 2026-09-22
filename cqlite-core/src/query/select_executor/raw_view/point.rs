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
use crate::query::result::QueryRow;
use crate::schema::TableSchema;
use crate::storage::scan_cancel::ScanCancel;
use crate::storage::sstable::reader::{CompactionRow, SSTableReader};
use crate::Result;
use std::ops::ControlFlow;
use std::sync::Arc;

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

/// Scan `reader`'s WHOLE compaction stream, forwarding only rows whose
/// partition key equals `pk_bytes` — the fail-safe every point-key path
/// (targeted or not) falls back to when no random-access index can be used.
/// `position` is `None` (no index was consulted).
async fn scan_and_filter_one_reader(
    reader: &SSTableReader,
    schema: &TableSchema,
    pk_bytes: &[u8],
    scan_cancel: &ScanCancel,
    out: &mut Vec<QueryRow>,
) -> Result<()> {
    let source = RawViewSource::from_reader(reader, None);
    let mut matched: Vec<CompactionRow> = Vec::new();
    reader
        .stream_all_partitions_for_compaction(Some(schema), scan_cancel, |crow| {
            if crow.key.as_bytes() == pk_bytes {
                matched.push(crow);
            }
            Ok(ControlFlow::Continue(()))
        })
        .await?;
    for row in matched {
        out.extend(map_compaction_row(row, schema, &source)?);
    }
    Ok(())
}

/// Probe every candidate generation for one partition key, returning every
/// physical row found (unreconciled).
#[cfg(not(feature = "tombstones"))]
async fn point_rows_for_key(
    readers: &[Arc<SSTableReader>],
    schema: &TableSchema,
    pk_bytes: &[u8],
    scan_cancel: &ScanCancel,
) -> Result<Vec<QueryRow>> {
    use crate::storage::sstable::reader::SinglePartitionCompaction;

    let mut out = Vec::new();
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
                    out.extend(map_compaction_row(row, schema, &source)?);
                }
            }
            SinglePartitionCompaction::IndexUnavailable => {
                scan_and_filter_one_reader(reader, schema, pk_bytes, scan_cancel, &mut out).await?;
            }
        }
    }
    Ok(out)
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
    scan_cancel: &ScanCancel,
) -> Result<Vec<QueryRow>> {
    let mut out = Vec::new();
    for reader in readers {
        scan_cancel.check()?;
        scan_and_filter_one_reader(reader, schema, pk_bytes, scan_cancel, &mut out).await?;
    }
    Ok(out)
}

/// Row producer entry point: probe every requested partition key against
/// every candidate generation, concatenating every physical row found. Never
/// routes through `KWayMerger`/`StorageEngine::scan*` (design.md D5/D6) — the
/// output is the deliberately UNRECONCILED per-generation contribution.
pub(in crate::query::select_executor) async fn raw_view_point_rows(
    readers: &[Arc<SSTableReader>],
    schema: &TableSchema,
    keys: &[Vec<u8>],
) -> Result<Vec<QueryRow>> {
    let scan_cancel = ScanCancel::new();
    let mut out = Vec::new();
    for key in keys {
        out.extend(point_rows_for_key(readers, schema, key, &scan_cancel).await?);
    }
    Ok(out)
}
