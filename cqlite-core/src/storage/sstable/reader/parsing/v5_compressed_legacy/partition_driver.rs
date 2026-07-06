//! Issue #1640 (Epic K, finding **K1**): the single V5CompressedLegacy
//! partition/row emit driver.
//!
//! The parser performance audit (`docs/reports/parser-performance-audit-2026-07-01.md`)
//! found the partition/row emit skeleton — partition-header parse →
//! END_OF_PARTITION / range-tombstone-marker checks → row-body decode →
//! boundary peek → `ParseStep` advance / `flush_and_emitted!` — duplicated
//! across five emit functions, with the subtle issue-#932 row-write-timestamp
//! coexistence decision hand-copied into the two sliding-window loops. "This is
//! how parity regressions are manufactured": a future tombstone fix must land in
//! N places or the paths silently diverge.
//!
//! This module owns:
//! - [`row_write_timestamp`] — the ONE issue-#932 row-write-timestamp decision.
//! - [`SlidingPartitionPolicy`] / [`MarkerOutcome`] /
//!   [`V5CompressedLegacyParser::drive_partition_sliding`] — the ONE bounded
//!   sliding-window partition skeleton, driven by policy hooks that capture the
//!   per-consumer differences (streaming-scan timestamps vs per-element
//!   compaction rows).
//!
//! K1 is the HEAD of the K-emit chain: K2 (#1641 non-allocating boundary peek),
//! K3 (#1642 positional row emit), K4 (#1643 `Arc` key handles) are separate
//! lanes and are NOT folded in here.

use super::*;

/// Issue #932 (K1): the single row-write-timestamp coexistence decision.
///
/// A `HAS_DELETION` row may ALSO carry a liveness timestamp (surviving cells
/// written strictly after the row deletion). Prefer that liveness timestamp as
/// the row timestamp so those cells inherit it and are NOT shadowed by the older
/// row deletion during reconcile; fall back to `markedForDeleteAt` only for a
/// PURE row tombstone (which has no `HAS_TIMESTAMP`). `0` when the row carries
/// neither a liveness timestamp nor a deletion.
///
/// Formerly hand-copied into `parse_one_partition_with_timestamps`
/// (`block_emit_windowed.rs`) and `parse_one_partition_for_compaction`
/// (`compaction.rs`); centralized here so the decision lives in exactly one
/// place (audit finding K1).
pub(super) fn row_write_timestamp(row_header_opt: &Option<RowHeader>) -> i64 {
    let row_tombstone = row_header_opt.as_ref().filter(|h| h.is_row_tombstone());
    row_header_opt
        .as_ref()
        .and_then(|h| h.timestamp)
        .or_else(|| row_tombstone.map(|h| h.row_tombstone_deletion_time()))
        .unwrap_or(0)
}

/// How the driver should advance after a policy handled a range-tombstone marker.
pub(super) enum MarkerOutcome {
    /// The marker was consumed; continue the row loop at this offset.
    Advanced(usize),
    /// The marker could not be represented/parsed faithfully — terminate the
    /// partition (the driver flushes buffered rows on the final chunk, else
    /// returns `NeedMore`), mirroring the pre-K1 `break`/`NeedMore` behaviour.
    Stop,
}

/// Per-consumer policy for the bounded sliding-window partition skeleton
/// (issue #1640, K1). The driver owns the framing; each policy owns exactly the
/// three behaviours that differ between the streaming-scan timestamps path and
/// the per-element compaction path.
///
/// Both policies buffer emitted rows into the driver-owned `pending` vec (never
/// forwarding directly), so a mid-partition `NeedMore` re-parse cannot double-
/// emit (issue #827): `pending` is flushed only when the partition is confirmed
/// complete.
pub(super) trait SlidingPartitionPolicy {
    /// The row carrier this policy emits.
    type Row;

    /// Called once, after the partition header is parsed, before the row loop.
    /// `partition_deletion` is the header's `Option<(markedForDeleteAt µs,
    /// localDeletionTime s)>`. The timestamps policy opens a read-side shadow
    /// here (when shadowing); the compaction policy pushes a synthetic
    /// partition-delete row for a partition tombstone (issue #1072).
    fn on_partition_open(
        &mut self,
        partition_key: RowKey,
        partition_deletion: Option<(i64, i32)>,
        schema: &TableSchema,
        pending: &mut Vec<Self::Row>,
    );

    /// Handle a range-tombstone marker at `offset`. The timestamps policy feeds
    /// the range-tombstone FSM (or skips on a physical read); the compaction
    /// policy pairs bound markers into `RangeMarker` rows (issue #933).
    fn on_range_marker(
        &mut self,
        data: &[u8],
        offset: usize,
        schema: &TableSchema,
        pending: &mut Vec<Self::Row>,
    ) -> MarkerOutcome;

    /// Decode and handle one data row at `offset`, pushing any emitted row into
    /// `pending`. Returns `Some(next_offset)` on success, or `None` when the row
    /// could not be parsed (the driver treats that as end-of-partition on the
    /// final chunk, else `NeedMore`).
    fn on_data_row(
        &mut self,
        data: &[u8],
        offset: usize,
        schema: &TableSchema,
        reader: &crate::storage::sstable::reader::types::SSTableReader,
        resolution: &RowColumnResolution,
        pending: &mut Vec<Self::Row>,
    ) -> Option<usize>;
}

impl V5CompressedLegacyParser {
    /// The single bounded sliding-window partition skeleton (issue #1640, K1).
    ///
    /// Parses exactly ONE partition from the front of `data`, driving the
    /// per-consumer `policy` hooks, and reports how the parse terminated via
    /// [`ParseStep`] — identical `Emitted`/`NeedMore`/`Done` semantics to the
    /// pre-K1 `parse_one_partition_with_timestamps` /
    /// `parse_one_partition_for_compaction` bodies this replaces.
    ///
    /// `at_final_chunk` flips a mid-partition parse failure between a refill
    /// request (`NeedMore`) and a terminal flush, exactly as before.
    pub(super) fn drive_partition_sliding<P, F>(
        &self,
        data: &[u8],
        schema: &TableSchema,
        reader: &crate::storage::sstable::reader::types::SSTableReader,
        at_final_chunk: bool,
        policy: &mut P,
        mut emit: F,
    ) -> Result<ParseStep>
    where
        P: SlidingPartitionPolicy,
        F: FnMut(P::Row) -> Result<std::ops::ControlFlow<()>>,
    {
        if data.is_empty() {
            return Ok(ParseStep::Done);
        }

        // #1741 (roborev HIGH): size the header need-more decision correctly for
        // the oa/da DeletionTime form via the authoritative discriminator peek,
        // so a deleted header split across a NON-FINAL chunk returns `NeedMore`
        // instead of being mis-parsed and skipped (which desynced the scan and,
        // on compaction, dropped a partition tombstone).
        match self.partition_header_readiness(data) {
            PartitionHeaderReadiness::Malformed => return Ok(ParseStep::Emitted(1)),
            PartitionHeaderReadiness::Incomplete => {
                return Ok(if at_final_chunk {
                    ParseStep::Done
                } else {
                    ParseStep::NeedMore
                });
            }
            PartitionHeaderReadiness::Ready => {}
        }

        let (partition_key, mut offset, partition_deletion) =
            match self.parse_partition_header_full(data, 0) {
                Ok(v) => v,
                // Defense-in-depth: `Ready` guarantees the DeletionTime is fully
                // present, so a parse failure here cannot be truncation. On a
                // non-final chunk only re-request bytes if the header is still
                // incomplete; otherwise skip a byte to resynchronise (NeedMore on
                // a complete buffer would loop forever). Under `Ready` this stays
                // the legacy skip-a-byte resync.
                Err(_) => {
                    if !at_final_chunk
                        && self.partition_header_readiness(data)
                            == PartitionHeaderReadiness::Incomplete
                    {
                        return Ok(ParseStep::NeedMore);
                    }
                    return Ok(ParseStep::Emitted(1));
                }
            };

        // Issue #1046: per-PARTITION resolution build (this driver is re-entered
        // once per partition by the sliding-window caller; allocations scale with
        // partition count, not row count). Borrows header strings + schema.
        let resolution = RowColumnResolution::build(schema, reader);

        // Finding 1 (#827): buffer this partition's rows locally and forward them
        // to the external `emit` only once the partition is CONFIRMED complete (an
        // `Emitted` return). A mid-partition `NeedMore` discards `pending` so the
        // caller can refill and re-parse from the partition start without
        // double-emitting already-buffered rows. Bounded by ONE partition's rows.
        let mut pending: Vec<P::Row> = Vec::new();

        policy.on_partition_open(partition_key, partition_deletion, schema, &mut pending);

        // Flush the buffered rows, honouring an early `Break`, and report the
        // bytes consumed for this (complete) partition so the caller drains
        // correctly.
        macro_rules! flush_and_emitted {
            ($consumed:expr) => {{
                for row in pending.drain(..) {
                    match emit(row)? {
                        std::ops::ControlFlow::Continue(()) => {}
                        std::ops::ControlFlow::Break(()) => break,
                    }
                }
                Ok(ParseStep::Emitted($consumed))
            }};
        }

        loop {
            // END_OF_PARTITION (0x01): partition complete, consume the marker.
            if offset < data.len() && Self::is_end_of_partition(data[offset]) {
                offset += 1;
                return flush_and_emitted!(offset);
            }

            // Consumed everything but never saw END_OF_PARTITION: the partition
            // may continue in the next chunk. On NeedMore flush NOTHING (drop the
            // buffered rows) so the caller can refill and re-parse from the start.
            if offset >= data.len() {
                if at_final_chunk {
                    return flush_and_emitted!(offset);
                }
                return Ok(ParseStep::NeedMore);
            }

            if Self::is_range_tombstone_marker(data[offset]) {
                match policy.on_range_marker(data, offset, schema, &mut pending) {
                    MarkerOutcome::Advanced(next_offset) => {
                        offset = next_offset;
                        continue;
                    }
                    MarkerOutcome::Stop => {
                        if at_final_chunk {
                            return flush_and_emitted!(offset);
                        }
                        return Ok(ParseStep::NeedMore);
                    }
                }
            }

            match policy.on_data_row(data, offset, schema, reader, &resolution, &mut pending) {
                Some(next_offset) => {
                    offset = next_offset;
                    if offset >= data.len() {
                        // End of the buffer without an explicit END_OF_PARTITION:
                        // the partition may continue in the next chunk.
                        if at_final_chunk {
                            return flush_and_emitted!(offset);
                        }
                        return Ok(ParseStep::NeedMore);
                    }
                    if self.peek_is_partition_header(data, offset) {
                        // Next partition starts here — current one is complete.
                        return flush_and_emitted!(offset);
                    }
                }
                None => {
                    // A row failed to parse. Mid-stream that may be a row
                    // straddling the chunk boundary, so request more bytes unless
                    // this is the final chunk (where it is end-of-partition).
                    if at_final_chunk {
                        return flush_and_emitted!(offset);
                    }
                    return Ok(ParseStep::NeedMore);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a `RowHeader` fixture with only the fields the #932 row-timestamp
    /// rule reads populated; everything else is a benign default.
    fn hdr(
        timestamp: Option<i64>,
        local_deletion_time: Option<i32>,
        marked_for_delete_at: Option<i64>,
    ) -> RowHeader {
        RowHeader {
            timestamp,
            ttl: None,
            liveness_expires_at_seconds: None,
            local_deletion_time,
            marked_for_delete_at,
            header_size: 0,
            row_size_vint_len: 0,
            missing_columns_bitmap: None,
            max_data_cell_timestamp: None,
            max_data_cell_expires_at: None,
            has_live_forever_data_cell: false,
        }
    }

    /// Issue #1640 / #932 lockstep guard.
    ///
    /// Both sliding-window emit paths (streaming-scan timestamps and per-element
    /// compaction) now resolve a row's write timestamp through the single
    /// [`row_write_timestamp`] helper, so this asserts THE one decision site.
    /// On `main` the rule was hand-copied into both loops, making this
    /// un-writable as a single-site assertion (the point of K1).
    #[test]
    fn row_write_timestamp_coexistence_prefers_liveness() {
        // A HAS_DELETION row that ALSO carries a liveness timestamp (surviving
        // cells written strictly AFTER the row deletion): the row timestamp is
        // the liveness ts (2000), NOT the older markedForDeleteAt (1000). Both
        // the user-scan and compaction paths call the same helper, so they agree
        // by construction — a future divergence would fail HERE.
        let coexistence = Some(hdr(Some(2000), Some(5), Some(1000)));
        assert_eq!(row_write_timestamp(&coexistence), 2000);
    }

    #[test]
    fn row_write_timestamp_pure_tombstone_uses_marked_for_delete_at() {
        // A PURE row tombstone (HAS_DELETION, no HAS_TIMESTAMP): fall back to
        // markedForDeleteAt (1000), never epoch 0 (which would lose LWW ordering).
        let pure_tombstone = Some(hdr(None, Some(5), Some(1000)));
        assert_eq!(row_write_timestamp(&pure_tombstone), 1000);
    }

    #[test]
    fn row_write_timestamp_live_row_uses_liveness() {
        // A live row with no deletion: the liveness timestamp.
        let live = Some(hdr(Some(3000), None, None));
        assert_eq!(row_write_timestamp(&live), 3000);
    }

    #[test]
    fn row_write_timestamp_absent_header_is_zero() {
        assert_eq!(row_write_timestamp(&None), 0);
    }

    #[test]
    fn row_write_timestamp_pure_tombstone_without_mfda_promotes_seconds() {
        // Defensive: a deletion recorded with only localDeletionTime (seconds) —
        // promoted to microseconds so ordering stays non-zero and monotonic.
        let odd = Some(hdr(None, Some(7), None));
        assert_eq!(row_write_timestamp(&odd), 7 * 1_000_000);
    }
}
