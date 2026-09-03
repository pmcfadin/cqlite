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

/// Issue #3809 (Finding 1) — the DISCRIMINATED outcome of a data-row policy hook.
///
/// # Why one `Err` channel was not enough
///
/// Issue #3782 gave [`SlidingPartitionPolicy::on_data_row`] an error channel
/// meaning *the row FAILED TO DECODE*, and made the DRIVER — never the policy —
/// decide tolerance from its authoritative `at_final_chunk`: at the final chunk no
/// further bytes can arrive, so the failure is truncation/corruption and is
/// refused; mid-window it is the ordinary straddling-row case and is answered with
/// `NeedMore`. That is exactly right for its subject, which is a
/// BYTES-AVAILABILITY question.
///
/// Issue #3809 needs the opposite disposition for a different subject. Its
/// clustering-identity check
/// (`CompactionRowData::require_tombstone_clustering_identity`) fires only AFTER
/// the row DECODED successfully: a row deletion reached the builder having
/// recovered fewer clustering values than its table declares, so emitting it would
/// hand the merge a deletion that identifies no row. **No amount of refilling can
/// make a short clustering become full-arity**, so "maybe more bytes will arrive"
/// is not merely unhelpful there, it is semantically inapplicable — and on every
/// entry point that declares [`BufferExtent::Window`](super::BufferExtent::Window)
/// (the point and promoted readers, the block-by-block scans, the windowed emit)
/// `at_final_chunk` is `false`, so a single undiscriminated `Err` channel would
/// convert the refusal into a refill request and silently truncate the read: the
/// very tolerant-tail data loss both issues exist to remove.
///
/// # Why a distinct OUTCOME rather than a kind test at the driver
///
/// Two shapes were available. A kind test at the driver (`if err.is_refusal()`)
/// would need a discriminable marker on [`crate::error::Error`], i.e. either a new
/// public variant — a public-surface change nothing in this repo detects the drift
/// of (#3366), and one that would retitle #3809's deliberately-chosen
/// `Corruption` telemetry bucket — or a message-text test, which the no-heuristics
/// mandate (#28) forbids outright.
///
/// A distinct outcome fits this trait instead, for three reasons:
///
/// * This module ALREADY owns a policy-outcome enum of exactly this shape
///   ([`MarkerOutcome`]), where the policy reports what it found and the driver
///   decides how to advance.
/// * It makes the data-losing default UNREPRESENTABLE, which is the argument
///   [`BufferExtent`](super::BufferExtent) itself is built on. Had the hook kept a
///   `Result`, a future `?` inside a policy body — the most natural thing to write
///   — would route a refusal into the TOLERATED channel silently, and that is the
///   defaulted-flag defect #3782 removed, reintroduced one layer up. With no
///   `Result` in the signature, `?` does not compile and every failure site must
///   NAME which of the two it is.
/// * Both dispositions stay literally true and are readable side by side, so the
///   trait no longer carries two contracts on one channel.
///
/// The cost is stated rather than hidden: a policy that acquires a genuine
/// plumbing error (neither a decode failure nor a refusal) has no `Err` to return
/// and must classify it. No policy has one today — all three route a
/// `parse_row_data_*` `Result` and nothing else — and being forced to choose is
/// the point.
/// What a [`SlidingPartitionPolicy::on_data_row`]
/// call found. FOUR outcomes on ONE channel: the two failures are separate
/// variants because the driver must treat them differently and cannot tell them
/// apart from an [`Error`] value.
#[derive(Debug)]
pub(super) enum DataRowOutcome {
    /// The row decoded and was handled; continue the row loop at this offset.
    Decoded(usize),
    /// The policy DECLINES the row with no error to report. Unchanged pre-#3782
    /// behaviour: end-of-partition on the final chunk, else `NeedMore`.
    ///
    /// `dead_code`-allowed, and the reason is worth recording rather than
    /// silencing: NO production policy declines TODAY. Both classify their one
    /// failure since #3782 (`DecodeFailed`), so the decline path is reached only by
    /// the driver's `StubPolicy` test harness. That was
    /// already true on `main` and merely INVISIBLE there, because the hook returned
    /// `Option<usize>` and `None` is a std variant no lint can call unconstructed.
    /// It is kept because the DRIVER's disposition of it is load-bearing contract —
    /// distinct from both failures, pinned by test (f) — and deleting the variant
    /// would delete that behaviour along with its test.
    #[allow(dead_code)]
    Declined,
    /// The row FAILED TO DECODE, with the decoder's error preserved (issue #3782).
    /// A BYTES-AVAILABILITY answer: the policy does NOT decide tolerance, the
    /// driver does, from `at_final_chunk` — refused at a proven-complete buffer,
    /// tolerated as a straddling row mid-window.
    DecodeFailed(Error),
    /// The row DECODED but MUST NOT be emitted (issue #3809): a semantic refusal.
    /// The driver propagates it UNCONDITIONALLY — `at_final_chunk` is not
    /// consulted, because the question is not about bytes and refilling the window
    /// cannot change the answer.
    Refused(Error),
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
    /// `pending`. Reports which of FOUR things happened via [`DataRowOutcome`];
    /// the distinction between the last two is load-bearing and is the ONE place
    /// issue #3782's contract and issue #3809's are reconciled.
    ///
    /// * [`DataRowOutcome::Decoded(next_offset)`](DataRowOutcome::Decoded) — the
    ///   row decoded; continue at `next_offset`.
    /// * [`DataRowOutcome::Declined`] — the policy declines the row with no error
    ///   to report. The driver treats it exactly as it always has:
    ///   end-of-partition on the final chunk, else `NeedMore`.
    /// * [`DataRowOutcome::DecodeFailed(e)`](DataRowOutcome::DecodeFailed) — the
    ///   row FAILED TO DECODE, with `e` preserved (issue #3782). The policy does
    ///   NOT decide tolerance; the DRIVER does, from `at_final_chunk`.
    /// * [`DataRowOutcome::Refused(e)`](DataRowOutcome::Refused) — the row DECODED
    ///   and must not be emitted (issue #3809). The driver propagates `e`
    ///   UNCONDITIONALLY and never consults `at_final_chunk`.
    ///
    /// # Which failures the driver may tolerate under a [`BufferExtent::Window`],
    /// and why the line is drawn at SEMANTICS rather than byte availability
    ///
    /// A `Window` caller (the point and promoted readers, the block-by-block
    /// scans, the windowed emit) drives this loop with `at_final_chunk == false`,
    /// where a `DecodeFailed` becomes `ParseStep::NeedMore` — the straddling-row
    /// refill protocol those readers depend on. That is CORRECT for `DecodeFailed`
    /// and ONLY for it: the question a decode failure asks is *were all this row's
    /// bytes present?*, more bytes can still arrive, and the answer may genuinely
    /// change on the next chunk. Measured over 42 well-formed corpus tables
    /// (10913 rows) that path fires 614 times, ALL with
    /// `at_final_chunk == false`.
    ///
    /// A `Refused` outcome asks nothing about bytes. The decode already returned
    /// `Ok`; the row is fully framed and structurally complete, and the policy has
    /// judged its CONTENT unrepresentable — a row deletion that recovered fewer
    /// clustering values than its table declares, and so identifies no row
    /// (`CompactionRowData::require_tombstone_clustering_identity`, #3809). No
    /// amount of refilling can make a short clustering become full-arity, so
    /// "maybe more bytes will arrive" is not a weaker answer there, it is an
    /// INAPPLICABLE one — and answering it that way would turn the refusal into a
    /// refill request on every `Window` entry point, silently truncating the read.
    /// A refusal is therefore NEVER tolerated, at any extent.
    ///
    /// Before #3782 this returned `Option<usize>`, so a decode error and "no row
    /// here" were the same value and every error was silently swallowed as
    /// end-of-partition. Before #3809 the two FAILURES were the same value, so a
    /// refusal inherited the decode failure's tolerance. There is deliberately no
    /// `Result` in the signature: a `?` inside a policy body is the most natural
    /// thing to write and would route a refusal into the tolerated channel
    /// silently — the data-losing default [`BufferExtent`] exists to make
    /// unrepresentable. Each failure site must NAME which one it is.
    fn on_data_row(
        &mut self,
        data: &[u8],
        offset: usize,
        schema: &TableSchema,
        reader: &crate::storage::sstable::reader::types::SSTableReader,
        resolution: &RowColumnResolution,
        pending: &mut Vec<Self::Row>,
    ) -> DataRowOutcome;

    /// Called once per partition, AFTER its last row, on every `Emitted` return —
    /// immediately before `pending` is flushed to the external emit. A
    /// mid-partition `NeedMore` discards `pending` and never calls this, so a row
    /// pushed here can never be emitted twice.
    ///
    /// `complete` distinguishes the two ways a partition parse ends (issue #3095,
    /// reconciling this hook with `block_emit*`'s own `partition_complete` guard):
    /// * `true` — the parse observed a STRUCTURAL end: the `END_OF_PARTITION`
    ///   marker, or the next partition's header. A well-formed SSTable always ends
    ///   a partition body this way.
    /// * `false` — the parse ran out of buffer on the FINAL chunk, hit an
    ///   unrepresentable range marker, or a policy declined a row. The partition
    ///   body was only partially observed (truncated/corrupt), so "this partition
    ///   yielded no clustering row" is NOT knowable.
    ///
    /// Since #3782 a row DECODE ERROR at the final chunk no longer reaches here at
    /// all, and since #3809 nor does a row REFUSAL at any extent: the driver
    /// returns that error instead of flushing a partial partition. The three
    /// `complete == false` cases above are unchanged.
    ///
    /// Exists for Cassandra's static-content-on-an-empty-partition rule
    /// (`SelectStatement.processPartition()`, issue #3095): a partition whose
    /// static row is live but which yielded NO clustering row returns exactly one
    /// result row, and "yielded no clustering row" is only knowable at a
    /// structurally-confirmed partition end. Defaults to a no-op so a policy with
    /// no partition-level output (the compaction policy) is unaffected.
    fn on_partition_close(
        &mut self,
        _schema: &TableSchema,
        _pending: &mut Vec<Self::Row>,
        _complete: bool,
    ) {
    }
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
    /// request (`NeedMore`) and a terminal flush, exactly as before — with two
    /// exceptions, both of which return the error to the caller:
    /// * a row DECODE ERROR at the FINAL chunk (issue #3782: no further bytes can
    ///   arrive, so it is data loss, not framing);
    /// * a row REFUSAL at ANY extent (issue #3809: the row decoded and its content
    ///   is unrepresentable, so `at_final_chunk` is not consulted at all).
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

        let (partition_key, mut offset, partition_deletion) = match self
            .parse_partition_header_full(data, 0)
        {
            Ok(v) => v,
            // Defense-in-depth: `Ready` guarantees the DeletionTime is fully
            // present, so a parse failure here cannot be truncation. On a
            // non-final chunk only re-request bytes if the header is still
            // incomplete; otherwise skip a byte to resynchronise (NeedMore on
            // a complete buffer would loop forever). Under `Ready` this stays
            // the legacy skip-a-byte resync.
            Err(_) => {
                if !at_final_chunk
                    && self.partition_header_readiness(data) == PartitionHeaderReadiness::Incomplete
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
        // `$complete` records whether the parse observed a STRUCTURAL partition end
        // (END_OF_PARTITION / the next partition header) rather than simply running
        // out of buffer or failing to parse — see `on_partition_close`. Never called
        // on `NeedMore`, so a row a policy appends here cannot be emitted twice.
        macro_rules! flush_and_emitted {
            ($consumed:expr, $complete:expr) => {{
                policy.on_partition_close(schema, &mut pending, $complete);
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
                // STRUCTURAL end: the END_OF_PARTITION marker.
                return flush_and_emitted!(offset, true);
            }

            // Consumed everything but never saw END_OF_PARTITION: the partition
            // may continue in the next chunk. On NeedMore flush NOTHING (drop the
            // buffered rows) so the caller can refill and re-parse from the start.
            if offset >= data.len() {
                if at_final_chunk {
                    // Buffer exhausted with NO END_OF_PARTITION: truncated body.
                    return flush_and_emitted!(offset, false);
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
                            // Unrepresentable marker: body only partly observed.
                            return flush_and_emitted!(offset, false);
                        }
                        return Ok(ParseStep::NeedMore);
                    }
                }
            }

            match policy.on_data_row(data, offset, schema, reader, &resolution, &mut pending) {
                // Issue #3809: a REFUSAL bypasses `at_final_chunk` entirely. The
                // row DECODED; the policy judged its CONTENT unrepresentable, which
                // is not a question about byte availability, so no refill can
                // change the answer. Tolerating it here would convert the refusal
                // into `NeedMore` on every `BufferExtent::Window` entry point —
                // the tolerant-tail truncation both #3782 and #3809 remove.
                DataRowOutcome::Refused(e) => return Err(e),
                // Issue #3782: for a DECODE FAILURE the DRIVER decides tolerance,
                // never the policy, and `at_final_chunk` is the discriminator — an
                // authoritative property of the sliding window, not a guess about
                // the bytes.
                //
                // At the final chunk NO FURTHER BYTES CAN ARRIVE, so a decode
                // error can never be a row straddling a chunk boundary: it is
                // truncation or corruption, and both are DATA LOSS. Swallowing
                // it made a corrupt clustering value read 23 of 100 rows and
                // made compaction emit 102 rows while LOSING 2 real partitions
                // and FABRICATING 3 — a loss compaction would then write back
                // to disk, invisible to any count-based check.
                //
                // Mid-stream the SAME error is the ordinary straddling-row case
                // and stays tolerant. Measured over 42 well-formed corpus
                // tables (10913 rows) the tolerant path fires 614 times, ALL of
                // them with `at_final_chunk == false` and ZERO with `true`, so
                // refusing here costs no well-formed read.
                DataRowOutcome::DecodeFailed(e) => {
                    if at_final_chunk {
                        return Err(e);
                    }
                    return Ok(ParseStep::NeedMore);
                }
                DataRowOutcome::Decoded(next_offset) => {
                    offset = next_offset;
                    if offset >= data.len() {
                        // End of the buffer without an explicit END_OF_PARTITION:
                        // the partition may continue in the next chunk.
                        if at_final_chunk {
                            return flush_and_emitted!(offset, false);
                        }
                        return Ok(ParseStep::NeedMore);
                    }
                    if self.peek_is_partition_header(data, offset) {
                        // STRUCTURAL end: the next partition's header.
                        return flush_and_emitted!(offset, true);
                    }
                }
                DataRowOutcome::Declined => {
                    // The policy DECLINED the row with no error to report (#3782:
                    // an actual decode error takes the `DecodeFailed` arm above,
                    // #3809: a refusal the `Refused` arm). Mid-stream that may be a
                    // row straddling the chunk boundary, so request more bytes
                    // unless this is the final chunk (where it is end-of-partition).
                    if at_final_chunk {
                        // Body only partly observed.
                        return flush_and_emitted!(offset, false);
                    }
                    return Ok(ParseStep::NeedMore);
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "partition_driver_tests.rs"]
mod tests;
