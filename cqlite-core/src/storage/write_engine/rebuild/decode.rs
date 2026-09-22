//! Decode + reconcile ONE partition for rebuild (design D1).
//!
//! Reuses the SAME decode primitive `salvage` builds its recovery loop on —
//! [`SSTableReader::decode_partition_at_offset_for_salvage`] — with a
//! simpler failure contract: rebuild refuses the WHOLE run on ANY decode
//! failure (design D3) rather than tracking per-partition losses the way
//! salvage does, so this returns a plain [`Result`] rather than salvage's
//! `(LossClass, String)` pair. Every `Err` here is the R5 "Data.db cannot be
//! trusted" signal.

use crate::error::{Error, Result};
use crate::schema::TableSchema;
use crate::storage::scan_cancel::ScanCancel;
use crate::storage::sstable::reader::{PartitionAtOffsetOutcome, SSTableReader};
use crate::storage::write_engine::merge::{
    KWayMerger, MergeEntry, MergeStep, SSTableRowIterator, SSTableRowIteratorAdapter,
};
use crate::storage::write_engine::mutation::{DecoratedKey, Mutation};
use std::collections::VecDeque;

/// A run backed by a single already-decoded partition's `MergeEntry`s — the
/// same shape the point-read path feeds [`KWayMerger::from_row_iterators`],
/// so a healthy partition reconciles through the IDENTICAL machinery
/// `compact_sstables`/`salvage_sstable` use (design D1).
struct SinglePartitionRun {
    entries: VecDeque<MergeEntry>,
}

impl SSTableRowIterator for SinglePartitionRun {
    fn next(&mut self) -> Option<Result<MergeEntry>> {
        self.entries.pop_front().map(Ok)
    }
}

/// Decode + reconcile ONE partition at `offset` (already-authoritative,
/// enumerated by [`super::boundaries::enumerate_partitions`]).
///
/// `Ok(None)` means the partition decoded but reconciled to nothing to write
/// (e.g. a lone shadowed range tombstone) — not an error, and not counted as
/// a partition for any component. `Err` means `Data.db` cannot be trusted at
/// this offset; the caller wraps it into a
/// [`RefusalReason::DataCorrupt`](super::RefusalReason::DataCorrupt) refusal.
pub(super) async fn decode_one_partition(
    reader: &SSTableReader,
    offset: u64,
    end_bound: Option<u64>,
    expected_key: &[u8],
    schema: &TableSchema,
    scan_cancel: &ScanCancel,
) -> Result<Option<(DecoratedKey, Vec<Mutation>)>> {
    let outcome = reader
        .decode_partition_at_offset_for_salvage(
            offset,
            end_bound,
            Some(expected_key),
            Some(schema),
            scan_cancel,
        )
        .await?;

    let rows = match outcome {
        PartitionAtOffsetOutcome::Rows(rows) => rows,
        PartitionAtOffsetOutcome::KeyMismatch => {
            return Err(Error::corruption(format!(
                "decoded key at offset {offset} does not match the boundary walk's own key for \
                 this slot"
            )));
        }
        PartitionAtOffsetOutcome::DecodeError { error } => {
            return Err(Error::corruption(format!(
                "partition at offset {offset} failed to decode: {error}"
            )));
        }
        PartitionAtOffsetOutcome::Truncated => {
            return Err(Error::corruption(format!(
                "partition at offset {offset} extends past Data.db's actual end (truncated)"
            )));
        }
        PartitionAtOffsetOutcome::SpanTooWide { span_bytes } => {
            return Err(Error::corruption(format!(
                "partition at offset {offset} is {span_bytes} bytes wide, exceeding this tool's \
                 plausible-partition-span ceiling"
            )));
        }
    };

    let mut merge_entries = Vec::with_capacity(rows.len());
    for row in rows {
        merge_entries.push(SSTableRowIteratorAdapter::build_merge_entry(0, row, schema)?);
    }
    let run = SinglePartitionRun {
        entries: merge_entries.into(),
    };
    let mut merger = KWayMerger::from_row_iterators(vec![Box::new(run)], schema)?;
    let (key, entries) = match merger.step()? {
        MergeStep::Partition { key, rows } => (key, rows),
        MergeStep::Complete => return Ok(None),
    };
    // One `decode_partition_at_offset_for_salvage` call names exactly one
    // partition slot, so a second `Partition` step would mean the decoder
    // fabricated rows spanning a boundary — refuse rather than silently
    // drop the second partition's rows (mirrors salvage's own guard).
    match merger.step()? {
        MergeStep::Complete => {}
        MergeStep::Partition { .. } => {
            return Err(Error::corruption(format!(
                "partition at offset {offset} decoded rows spanning more than one partition key"
            )));
        }
    }
    if entries.is_empty() {
        return Ok(None);
    }
    let mut mutations = Vec::with_capacity(entries.len());
    for e in entries {
        mutations.push(KWayMerger::merge_entry_to_mutation(e, schema)?);
    }
    Ok(Some((key, mutations)))
}
