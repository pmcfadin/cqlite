//! Row-granular resumable within-partition compaction streaming (issue #2299).
//!
//! Split out of the sibling `compaction` module (campsite rule, epic #1116):
//! [`V5CompressedLegacyParser::stream_partition_body_incremental`] and its
//! cross-call [`CompactionPartitionState`] / per-structure [`PartitionStreamStep`]
//! types. Reuses the SAME `CompactionPolicy` decode as the buffered
//! `parse_one_partition_for_compaction`, so tombstone / timestamp / range-marker
//! semantics are byte-identical — only the buffering GRANULARITY differs (this
//! path drains one confirmed structure at a time so the sliding-window compaction
//! driver never has to keep a WIDE partition fully resident).

use super::compaction::CompactionPolicy;
use super::partition_driver::{MarkerOutcome, SlidingPartitionPolicy};
use super::row_framing::PartitionHeaderReadiness;
use super::{RowColumnResolution, V5CompressedLegacyParser};
use crate::schema::TableSchema;
use crate::types::RowKey;
use crate::{Error, Result};

impl V5CompressedLegacyParser {
    /// Row-granular resumable partition streaming for the compaction read path
    /// (issue #2299).
    ///
    /// [`Self::parse_one_partition_for_compaction`] buffers a WHOLE partition
    /// into the driver-owned `pending` vec and only reports its consumed byte
    /// count (and forwards its rows) once the partition is CONFIRMED complete —
    /// i.e. at the END_OF_PARTITION marker or the next partition header. For a
    /// WIDE partition (a single partition holding the bulk of an SSTable, e.g. a
    /// time-series row split by clustering-key range across the merge inputs)
    /// that means the sliding-window driver in
    /// [`SSTableReader::stream_all_partitions_for_compaction`] cannot advance its
    /// window cursor until the whole partition is resident — so peak memory
    /// scales with `max_partition_size`, not `one row + one chunk`. With four
    /// concurrent producers each holding a ~40 MiB wide partition (window bytes
    /// PLUS the buffered rows), the compaction blows the 128 MiB budget.
    ///
    /// This method streams the partition BODY one confirmed structure at a time,
    /// invoking `emit` per row/marker and reporting via [`PartitionStreamStep`]
    /// exactly how many bytes of `data` are now safe to drain from the FRONT of
    /// the caller's sliding window — WITHOUT waiting for END_OF_PARTITION. The
    /// caller drives it as a resumable cursor: it calls this repeatedly on the
    /// (growing/shrinking) window, advancing the front cursor by the returned
    /// `consumed` after every call.
    ///
    /// `state` carries the cross-call partition context (the partition key, the
    /// header-parsed flag, and the in-flight range-tombstone start bound) so a
    /// partition that straddles a chunk boundary resumes correctly after a
    /// refill. Tombstone / timestamp / range-marker semantics are byte-identical
    /// to [`Self::parse_one_partition_for_compaction`] — this is the SAME
    /// `CompactionPolicy` decode, only the buffering granularity differs.
    ///
    /// `at_final_chunk` flips a mid-structure end-of-buffer between a refill
    /// request ([`PartitionStreamStep::NeedMore`]) and terminal completion
    /// ([`PartitionStreamStep::PartitionDone`]), exactly as the sliding driver's
    /// `NeedMore`/`Done` distinction does.
    ///
    /// `resolution` is the column resolution built ONCE per scan by the caller
    /// ([`SSTableReader::stream_all_partitions_for_compaction`]) and threaded in
    /// (the same way `schema` / `state` are). It is derived purely from the
    /// SSTable serialization header (`reader.header`) + `schema`, both INVARIANT
    /// across every partition of a single SSTable, so a single build is
    /// semantically identical to the buffered `drive_partition_sliding`'s
    /// per-partition build (`partition_driver.rs:179`) — only without that path's
    /// per-partition (and, for this per-structure driver, once-PER-ROW) allocation
    /// churn: `RowColumnResolution::build` allocates a `HashMap` over
    /// `schema.columns` plus a fresh `Arc<str>` per header/clustering column, so
    /// rebuilding it per drain step turned an O(partitions) cost into
    /// O(rows × header_cols) on exactly the wide-partition workload issue #2299
    /// optimizes. Threading the once-built resolution restores per-scan
    /// (≤ per-partition) allocation. `None` only when `schema` is `None`, which
    /// this method rejects before any row decode.
    ///
    /// [`SSTableReader::stream_all_partitions_for_compaction`]: crate::storage::sstable::SSTableReader
    pub fn stream_partition_body_incremental<F>(
        &self,
        data: &[u8],
        schema: Option<&TableSchema>,
        reader: &crate::storage::sstable::reader::types::SSTableReader,
        resolution: Option<&RowColumnResolution>,
        at_final_chunk: bool,
        state: &mut CompactionPartitionState,
        emit: &mut F,
    ) -> Result<PartitionStreamStep>
    where
        F: FnMut(
            crate::storage::sstable::reader::compaction_row::CompactionRow,
        ) -> Result<std::ops::ControlFlow<()>>,
    {
        if data.is_empty() {
            return Ok(if at_final_chunk {
                PartitionStreamStep::AllDone
            } else {
                PartitionStreamStep::NeedMore
            });
        }

        let schema = schema.ok_or_else(|| {
            Error::schema(format!(
                "V5CompressedLegacy (compaction) format requires schema for {}.{}",
                self.keyspace, self.table_name
            ))
        })?;

        // The column resolution is built ONCE per scan by the caller and threaded
        // in (see the doc comment). It is required whenever `schema` is present
        // (the caller builds it from the same schema); a missing resolution here
        // is an internal wiring bug, not a data condition.
        let resolution = resolution.ok_or_else(|| {
            Error::schema(format!(
                "V5CompressedLegacy (compaction) streaming requires a prebuilt column \
                 resolution for {}.{}",
                self.keyspace, self.table_name
            ))
        })?;

        // (1) Parse the partition header ONCE per partition. When the header has
        //     already been parsed for this partition (a resumed body call after a
        //     refill) the window front is the body start (offset 0).
        if !state.header_parsed {
            match self.partition_header_readiness(data) {
                PartitionHeaderReadiness::Malformed => {
                    // Skip one byte to resynchronise (matches the buffered driver's
                    // `Emitted(1)` malformed-header behaviour).
                    return Ok(PartitionStreamStep::Consumed(1));
                }
                PartitionHeaderReadiness::Incomplete => {
                    return Ok(if at_final_chunk {
                        PartitionStreamStep::AllDone
                    } else {
                        PartitionStreamStep::NeedMore
                    });
                }
                PartitionHeaderReadiness::Ready => {}
            }

            let (partition_key, after_header, partition_deletion) =
                match self.parse_partition_header_full(data, 0) {
                    Ok(v) => v,
                    Err(_) => {
                        if !at_final_chunk
                            && self.partition_header_readiness(data)
                                == PartitionHeaderReadiness::Incomplete
                        {
                            return Ok(PartitionStreamStep::NeedMore);
                        }
                        return Ok(PartitionStreamStep::Consumed(1));
                    }
                };

            // Emit a partition-level tombstone carrier immediately (issue #1072),
            // via the shared CompactionPolicy hook — same synthetic carrier the
            // buffered path pushes into `pending`. The policy's transient state
            // (partition key + in-flight range-start bound) is written back into
            // `state` so a resumed body call rebuilds an equivalent policy.
            let mut policy = CompactionPolicy::new(self);
            let mut carriers: Vec<crate::storage::sstable::reader::compaction_row::CompactionRow> =
                Vec::new();
            policy.on_partition_open(partition_key, partition_deletion, schema, &mut carriers);
            state.partition_key = policy.partition_key().clone();
            state.pending_range_start = policy.pending_range_start().clone();
            state.header_parsed = true;
            // The header bytes are confirmed; drain them from the window front so a
            // refill re-parse never re-reads the header. The next call sees the body
            // at offset 0.
            for row in carriers {
                match emit(row)? {
                    std::ops::ControlFlow::Continue(()) => {}
                    std::ops::ControlFlow::Break(()) => {
                        return Ok(PartitionStreamStep::Break(after_header));
                    }
                }
            }
            return Ok(PartitionStreamStep::Consumed(after_header));
        }

        // (2) Body: parse ONE confirmed structure (row / range marker /
        //     end-of-partition) from the FRONT of the window (offset 0 — this
        //     driver resumes from 0 each call, reporting its consumed byte count so
        //     the caller advances the window cursor per structure), emit it, and
        //     report its consumed bytes. Rebuild the policy from `state` (it only
        //     carries owned pieces: the partition key and the in-flight
        //     range-tombstone start bound). `data` is non-empty here (early return
        //     at the top), so the front byte `data[0]` is always in bounds.

        // END_OF_PARTITION (0x01): partition complete; consume the marker byte and
        // signal the caller to reset for the next partition.
        if Self::is_end_of_partition(data[0]) {
            state.reset();
            return Ok(PartitionStreamStep::PartitionDone(1));
        }

        let mut policy = CompactionPolicy::new(self);
        policy.set_partition_key(state.partition_key.clone());
        policy.set_pending_range_start(state.pending_range_start.clone());

        // A range-tombstone marker: pair it via the policy (the surviving marker
        // row, if any, is emitted here). A truncated marker on a non-final chunk
        // requests more bytes.
        if Self::is_range_tombstone_marker(data[0]) {
            let mut emitted: Vec<crate::storage::sstable::reader::compaction_row::CompactionRow> =
                Vec::new();
            match policy.on_range_marker(data, 0, schema, &mut emitted) {
                MarkerOutcome::Advanced(next_offset) => {
                    // Confirm the marker is fully framed within the window.
                    if next_offset > data.len() {
                        return Ok(if at_final_chunk {
                            PartitionStreamStep::AllDone
                        } else {
                            PartitionStreamStep::NeedMore
                        });
                    }
                    state.pending_range_start = policy.pending_range_start().clone();
                    for row in emitted {
                        match emit(row)? {
                            std::ops::ControlFlow::Continue(()) => {}
                            std::ops::ControlFlow::Break(()) => {
                                return Ok(PartitionStreamStep::Break(next_offset));
                            }
                        }
                    }
                    return Ok(PartitionStreamStep::Consumed(next_offset));
                }
                // Issue #3721 (roborev job 16): the marker could not be PARSED.
                // On a NON-final chunk that may be nothing worse than a marker
                // body straddling the window boundary, which is exactly what
                // `NeedMore` is for. On the FINAL chunk no refill is coming, so
                // reporting `PartitionDone` — a SUCCESSFUL partition completion —
                // silently dropped a corrupt or truncated tombstone from output
                // that is WRITTEN, resurrecting the rows it shadowed. Propagate
                // the preserved parse error instead. `at_final_chunk` is the
                // caller's own chunking state; no bytes are inspected to guess
                // whether more data exists (issue #28).
                MarkerOutcome::Unparseable(cause) => {
                    if at_final_chunk {
                        return Err(
                            super::range_marker_error::unparseable_marker_at_final_chunk(cause),
                        );
                    }
                    return Ok(PartitionStreamStep::NeedMore);
                }
                // Issue #3721/#3808: a marker that PARSED but cannot be
                // represented is corruption at a known resume point — no refill
                // fixes it, and ending the partition here would report `Ok` with
                // rows missing. `CompactionPolicy::on_range_marker` produces this
                // for an unrecognised bound kind (#3808): the kind byte is real
                // in-window on-disk data whenever the marker parsed at all, so a
                // larger window cannot change it, and this policy's rows are
                // WRITTEN — skipping the marker resurrects what it shadowed.
                MarkerOutcome::Refused(e) => return Err(e),
            }
        }

        // A data row: decode exactly one, emit it, and report its consumed bytes.
        let mut emitted: Vec<crate::storage::sstable::reader::compaction_row::CompactionRow> =
            Vec::new();
        // Issue #3721: `?` — a per-column decode failure must reach the streaming
        // caller, never be folded into the `None` (end-of-partition) arm below.
        match policy.on_data_row(data, 0, schema, reader, resolution, &mut emitted)? {
            Some(next_offset) => {
                // Confirm the row is fully framed WITHIN the window: a next_offset
                // STRICTLY PAST the buffer end means we decoded a truncated row on a
                // non-final chunk — request more bytes rather than emit a partial
                // row. NB: `>` not `>=` (the buffered `drive_partition_sliding` uses
                // `>=` on line 240 of partition_driver.rs). The difference is
                // intentional and load-bearing for this resume-from-0 driver: this
                // path parses ONE structure from the FRONT (offset 0) and reports
                // its consumed byte count so the caller advances the window cursor
                // per-structure, so a row that ends EXACTLY at `data.len()`
                // (`next_offset == data.len()`) is a fully-framed row whose bytes we
                // must consume and drain — treating `==` as truncation would stall
                // the cursor forever (re-parsing the same trailing row every refill).
                // The buffered driver's `>=` is correct for ITS loop (it keeps
                // advancing `offset` within one call and uses `offset >= data.len()`
                // to decide whether to look for the next structure), not for a
                // per-structure resume cursor. Do not "simplify" `>` to `>=`.
                if next_offset > data.len() {
                    return Ok(if at_final_chunk {
                        PartitionStreamStep::AllDone
                    } else {
                        PartitionStreamStep::NeedMore
                    });
                }
                state.pending_range_start = policy.pending_range_start().clone();
                for row in emitted {
                    match emit(row)? {
                        std::ops::ControlFlow::Continue(()) => {}
                        std::ops::ControlFlow::Break(()) => {
                            return Ok(PartitionStreamStep::Break(next_offset));
                        }
                    }
                }
                // Partition-boundary defense-in-depth (roborev): mirror the buffered
                // `drive_partition_sliding` EXACTLY. If the very next bytes are a
                // partition header (a body NOT terminated by an explicit
                // END_OF_PARTITION 0x01 — e.g. malformed/corrupt input, or a
                // producer that elides the marker), the current partition is
                // complete. Feeding the next partition's header into `on_data_row`
                // would mis-attribute/desync it. Split here (reset for the next
                // partition) so the streaming path and the buffered path can never
                // diverge. Only meaningful when the header is fully framed within
                // the window; a truncated header on a non-final chunk falls through
                // to the normal per-structure refill on the next call.
                if next_offset < data.len() && self.peek_is_partition_header(data, next_offset) {
                    state.reset();
                    return Ok(PartitionStreamStep::PartitionDone(next_offset));
                }
                Ok(PartitionStreamStep::Consumed(next_offset))
            }
            None => {
                // A row failed to parse. Mid-stream that may be a row straddling the
                // chunk boundary, so request more bytes unless this is the final
                // chunk (where it is end-of-partition).
                if at_final_chunk {
                    state.reset();
                    Ok(PartitionStreamStep::PartitionDone(0))
                } else {
                    Ok(PartitionStreamStep::NeedMore)
                }
            }
        }
    }
}

/// Cross-call state for [`V5CompressedLegacyParser::stream_partition_body_incremental`]
/// (issue #2299). Carries the in-flight partition's decode context so a partition
/// that straddles a sliding-window chunk boundary resumes correctly after a refill.
pub struct CompactionPartitionState {
    /// Whether the partition header (key + partition-level deletion) has been
    /// parsed for the CURRENT partition. Reset to `false` at each partition
    /// boundary so the next partition re-parses its own header.
    header_parsed: bool,
    /// The current partition's key, decoded once from the header and reused for
    /// every row this partition emits across resumed calls.
    partition_key: RowKey,
    /// Issue #933: the in-flight range-tombstone start bound
    /// `(bound, markedForDeleteAt µs, localDeletionTime s)`, carried across
    /// resumed calls so a range whose CLOSE bound arrives in a later chunk still
    /// pairs correctly (the incremental driver never re-parses from the partition
    /// start, so this cross-call carry replaces the buffered driver's re-derive).
    pending_range_start: Option<(
        crate::storage::sstable::reader::compaction_row::CompactionBound,
        i64,
        i32,
    )>,
}

impl Default for CompactionPartitionState {
    fn default() -> Self {
        Self::new()
    }
}

impl CompactionPartitionState {
    /// A fresh state positioned at the start of a (not-yet-parsed) partition.
    pub fn new() -> Self {
        Self {
            header_parsed: false,
            partition_key: RowKey::new(Vec::new()),
            pending_range_start: None,
        }
    }

    /// Reset to await the next partition header (after an END_OF_PARTITION or a
    /// terminal trailing partition).
    fn reset(&mut self) {
        self.header_parsed = false;
        self.partition_key = RowKey::new(Vec::new());
        self.pending_range_start = None;
    }
}

/// Outcome of one [`V5CompressedLegacyParser::stream_partition_body_incremental`]
/// call (issue #2299). Reports how many bytes of the window front are now safe to
/// drain, WITHOUT waiting for a whole partition.
#[derive(Debug, PartialEq, Eq)]
pub enum PartitionStreamStep {
    /// One structure (header prefix, row, or range marker) was confirmed and its
    /// rows emitted; `usize` bytes may be drained from the window front. The
    /// current partition CONTINUES — call again.
    Consumed(usize),
    /// The partition ended (END_OF_PARTITION, or a terminal trailing partition on
    /// the final chunk); `usize` bytes may be drained. State is reset for the next
    /// partition.
    PartitionDone(usize),
    /// The window is (possibly) truncated mid-structure and `!at_final_chunk`:
    /// the caller must append the next chunk and retry (nothing to drain). Never
    /// returned when `at_final_chunk`.
    NeedMore,
    /// The window is empty at the final chunk — genuine end of data. Terminal.
    AllDone,
    /// `emit` returned `Break` (consumer dropped); `usize` bytes were consumed up
    /// to and including the breaking structure. The caller stops the scan.
    Break(usize),
}
