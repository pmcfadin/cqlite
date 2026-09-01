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
///
/// `Stop` and `Refused` are the two halves of what one signal used to conflate
/// (issue #3721), and the distinction is a FRAMING one, not a severity judgement:
///
/// * `Stop` — the marker could not be PARSED, so there is no resume offset. That is
///   a genuine framing terminator: on a non-final chunk it means "the marker body
///   is truncated here, refill", and on the final chunk it means the body is only
///   partly observed. `compaction::CompactionPolicy::on_range_marker` produces
///   exactly this case and MUST keep its meaning.
/// * `Refused` — the marker WAS parsed (a resume offset exists and the partition
///   body continues there) and the policy cannot represent it. Corruption with a
///   valid resume point, which no refill can fix; reporting it as `Stop` truncated
///   the partition and returned `Ok`.
pub(super) enum MarkerOutcome {
    /// The marker was consumed; continue the row loop at this offset.
    Advanced(usize),
    /// The marker could not be PARSED — no resume offset exists. Terminate the
    /// partition (the driver flushes buffered rows on the final chunk, else
    /// returns `NeedMore`), mirroring the pre-K1 `break`/`NeedMore` behaviour.
    Stop,
    /// The marker was PARSED but cannot be represented faithfully (issue #3721):
    /// corruption at a known resume point. Propagated to the caller of the read —
    /// see [`super::range_marker_error::range_marker_refused`].
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
    /// `pending`.
    ///
    /// Three outcomes, and the distinction between the last two is issue #3721:
    ///
    /// * `Ok(Some(next_offset))` — the row was decoded and consumed those bytes.
    /// * `Ok(None)` — the row FRAMING could not be parsed here. This is the
    ///   ordinary end-of-partition-body signal (a well-formed partition's last row
    ///   is followed by bytes that are not a row), so the driver treats it as
    ///   end-of-partition on the final chunk and `NeedMore` otherwise.
    /// * `Err(e)` — the row was framed but a COLUMN inside it could not be
    ///   decoded ([`crate::Error::ColumnDecode`]). Serving the row without that
    ///   column, or ending the partition early, would both be silent data loss, so
    ///   the driver propagates it to its caller.
    fn on_data_row(
        &mut self,
        data: &[u8],
        offset: usize,
        schema: &TableSchema,
        reader: &crate::storage::sstable::reader::types::SSTableReader,
        resolution: &RowColumnResolution,
        pending: &mut Vec<Self::Row>,
    ) -> Result<Option<usize>>;

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
    ///   unrepresentable range marker, or failed to parse a row. The partition body
    ///   was only partially observed (truncated/corrupt), so "this partition
    ///   yielded no clustering row" is NOT knowable.
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
                            // Unparseable marker: body only partly observed.
                            return flush_and_emitted!(offset, false);
                        }
                        return Ok(ParseStep::NeedMore);
                    }
                    // Issue #3721: the marker parsed and the body continues past
                    // it, so no refill can help and truncating the partition would
                    // report `Ok` with rows missing. Propagate.
                    MarkerOutcome::Refused(e) => return Err(e),
                }
            }

            // Issue #3721: `?` — a per-column decode failure is NOT the
            // end-of-partition signal and must reach the caller, never be folded
            // into the `None` arm below (which would truncate the partition and
            // report success).
            match policy.on_data_row(data, offset, schema, reader, &resolution, &mut pending)? {
                Some(next_offset) => {
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
                None => {
                    // A row failed to parse. Mid-stream that may be a row
                    // straddling the chunk boundary, so request more bytes unless
                    // this is the final chunk (where it is end-of-partition).
                    if at_final_chunk {
                        // Row framing unparseable: body only partly observed.
                        return flush_and_emitted!(offset, false);
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
            has_deleted_data_cell: false,
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

    // -----------------------------------------------------------------------
    // Driver-level framing tests (issue #1640, roborev K1 test-depth finding).
    //
    // The correctness-critical logic this refactor centralizes is
    // `drive_partition_sliding`'s framing skeleton — the previously-duplicated
    // loop whose divergence "manufactures parity regressions". These tests drive
    // that skeleton directly with a STUB `SlidingPartitionPolicy` over a SYNTHETIC
    // byte buffer, so the framing contract (buffer-then-flush, no-double-emit on a
    // mid-partition `NeedMore`, marker-Stop termination) is pinned independently of
    // any real row decode, schema, or on-disk fixture.
    //
    // `write-support` is a DEFAULT feature; the gate is only so the minimal
    // `--no-default-features` build (no synthetic-reader writer) still compiles.
    // -----------------------------------------------------------------------

    /// A synthetic byte the stub treats as exactly ONE data row: it carries
    /// neither the END_OF_PARTITION bit (0x01) nor the IS_MARKER bit (0x02), so
    /// the driver routes it to `on_data_row`.
    #[cfg(feature = "write-support")]
    const STUB_ROW_BYTE: u8 = 0xa0;

    /// The IS_MARKER flag byte (0x02, END_OF_PARTITION bit clear): the driver
    /// routes it to `on_range_marker`.
    #[cfg(feature = "write-support")]
    const STUB_MARKER_BYTE: u8 = 0x02;

    /// A carrier row the stub policy buffers into the driver-owned `pending` vec.
    #[cfg(feature = "write-support")]
    #[derive(Debug, PartialEq, Eq)]
    struct StubRow(u8);

    /// Test-only [`SlidingPartitionPolicy`] over a synthetic buffer. It exercises
    /// the driver's framing skeleton WITHOUT any real row decode: each
    /// [`STUB_ROW_BYTE`] is one row (buffered into `pending`, consuming 1 byte),
    /// any range-tombstone marker is answered with [`MarkerOutcome::Stop`], and
    /// `buffered` records how many rows were pushed into `pending` — so a test can
    /// prove a row WAS buffered even when the driver forwards ZERO rows.
    #[cfg(feature = "write-support")]
    struct StubPolicy {
        /// Count of rows the policy pushed into the driver-owned `pending` vec.
        buffered: usize,
    }

    #[cfg(feature = "write-support")]
    impl SlidingPartitionPolicy for StubPolicy {
        type Row = StubRow;

        fn on_partition_open(
            &mut self,
            _partition_key: RowKey,
            _partition_deletion: Option<(i64, i32)>,
            _schema: &TableSchema,
            _pending: &mut Vec<Self::Row>,
        ) {
            // No synthetic partition-delete row for these framing tests.
        }

        fn on_range_marker(
            &mut self,
            _data: &[u8],
            _offset: usize,
            _schema: &TableSchema,
            _pending: &mut Vec<Self::Row>,
        ) -> MarkerOutcome {
            // Mirror the pre-K1 `break`/`NeedMore` behaviour: a marker the policy
            // cannot represent faithfully terminates the partition.
            MarkerOutcome::Stop
        }

        fn on_data_row(
            &mut self,
            data: &[u8],
            offset: usize,
            _schema: &TableSchema,
            _reader: &crate::storage::sstable::reader::types::SSTableReader,
            _resolution: &RowColumnResolution,
            pending: &mut Vec<Self::Row>,
        ) -> Result<Option<usize>> {
            match data.get(offset) {
                Some(&b) if b == STUB_ROW_BYTE => {
                    pending.push(StubRow(b));
                    self.buffered += 1;
                    Ok(Some(offset + 1))
                }
                // Anything else: "row framing failed to parse" — the driver treats
                // this as end-of-partition on the final chunk, else `NeedMore`.
                _ => Ok(None),
            }
        }
    }

    /// A minimal single-partition-key schema `t(pk int, v text)`. The concrete
    /// columns are irrelevant to the framing under test (the stub never consults
    /// the schema, reader, or resolution), but a valid schema is required to build
    /// [`RowColumnResolution`].
    #[cfg(feature = "write-support")]
    fn stub_schema() -> crate::schema::TableSchema {
        use crate::schema::{Column, KeyColumn, TableSchema};
        let col = |name: &str, ty: &str, nullable: bool| Column {
            name: name.to_string(),
            data_type: ty.to_string(),
            nullable,
            default: None,
            is_static: false,
        };
        TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_tbl".to_string(),
            partition_keys: vec![KeyColumn {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![col("pk", "int", false), col("v", "text", true)],
            comments: std::collections::HashMap::new(),
            dropped_columns: std::collections::HashMap::new(),
        }
    }

    /// Build a synthetic **nb**-format partition: a LIVE header (no partition
    /// tombstone) followed by `body` bytes. The parser built by
    /// [`V5CompressedLegacyParser::new`] uses the nb-compatible gates
    /// (`has_uint_deletion_time == false`), so the header is:
    /// `flags(1) + key_len(1) + key(1) + nb DeletionTime` where the nb
    /// DeletionTime is a 4-byte localDeletionTime (`i32::MAX` == LIVE sentinel) +
    /// 8-byte markedForDeleteAt (0). Fixed bytes only — no wall-clock input.
    #[cfg(feature = "write-support")]
    fn synthetic_partition(body: &[u8]) -> Vec<u8> {
        let mut buf = vec![0x00, 0x01, 0x42]; // flags, key_len=1, key=[0x42]
        buf.extend_from_slice(&i32::MAX.to_be_bytes()); // LIVE localDeletionTime
        buf.extend_from_slice(&0i64.to_be_bytes()); // markedForDeleteAt
        buf.extend_from_slice(body);
        buf
    }

    /// Drive one synthetic partition through the real `drive_partition_sliding`
    /// skeleton with the [`StubPolicy`]. Returns the [`ParseStep`], the number of
    /// rows the policy buffered into `pending`, and the rows the driver actually
    /// forwarded to the external `emit` closure.
    ///
    /// The `&SSTableReader` is a genuine (dataset-independent) synthetic handle
    /// reused from the decoder-lockstep net; its bytes are never consulted here —
    /// the stub ignores the reader and resolution entirely.
    #[cfg(feature = "write-support")]
    async fn drive(data: &[u8], at_final_chunk: bool) -> (ParseStep, usize, Vec<StubRow>) {
        let reader = super::super::decoder_lockstep_tests::open_reader()
            .await
            .expect("write-support synthetic reader is always available");
        let parser = V5CompressedLegacyParser::new(
            "test_ks".to_string(),
            "test_tbl".to_string(),
            0,
            0,
            None,
        );
        let schema = stub_schema();
        let mut policy = StubPolicy { buffered: 0 };
        let mut collected: Vec<StubRow> = Vec::new();
        let step = parser
            .drive_partition_sliding(data, &schema, &reader, at_final_chunk, &mut policy, |row| {
                collected.push(row);
                Ok(std::ops::ControlFlow::Continue(()))
            })
            .expect("drive_partition_sliding should not error on a well-formed header");
        (step, policy.buffered, collected)
    }

    /// (a) Issue-#827 no-double-emit invariant: a truncated partition on a
    /// NON-final chunk returns `NeedMore` and forwards ZERO rows — *even though*
    /// the policy already buffered a row into `pending`. Discarding `pending` on a
    /// mid-partition `NeedMore` is what makes a refill-and-re-parse from the
    /// partition start safe (a forwarded row here would be duplicated on re-parse).
    #[cfg(feature = "write-support")]
    #[tokio::test]
    async fn truncated_non_final_chunk_buffers_but_emits_zero() {
        // Header + one row byte, no END_OF_PARTITION: the buffer ends mid-partition.
        let data = synthetic_partition(&[STUB_ROW_BYTE]);
        let (step, buffered, collected) = drive(&data, false).await;
        assert_eq!(
            step,
            ParseStep::NeedMore,
            "a mid-partition end-of-buffer on a non-final chunk must request more bytes"
        );
        assert_eq!(
            buffered, 1,
            "the row WAS buffered into the driver-owned pending vec"
        );
        assert!(
            collected.is_empty(),
            "NeedMore must DISCARD pending and forward zero rows so a re-parse cannot \
             double-emit (issue #827)"
        );
    }

    /// (b) The SAME buffer with `at_final_chunk = true` flushes the buffered row:
    /// on the final chunk an end-of-buffer is end-of-partition, so `pending` is
    /// forwarded exactly once.
    #[cfg(feature = "write-support")]
    #[tokio::test]
    async fn same_buffer_final_chunk_flushes_pending() {
        let data = synthetic_partition(&[STUB_ROW_BYTE]);
        let (step, buffered, collected) = drive(&data, true).await;
        assert!(
            matches!(step, ParseStep::Emitted(_)),
            "the final chunk treats end-of-buffer as end-of-partition and flushes"
        );
        assert_eq!(buffered, 1, "the same single row is buffered");
        assert_eq!(
            collected,
            vec![StubRow(STUB_ROW_BYTE)],
            "the buffered row is forwarded exactly once on the final chunk"
        );
    }

    /// (c) A range-tombstone marker the policy answers with `MarkerOutcome::Stop`
    /// on a NON-final chunk yields `NeedMore` with NO emission — mirroring the
    /// pre-K1 `break`/`NeedMore` terminate-partition behaviour — and discards any
    /// rows already buffered before the marker.
    #[cfg(feature = "write-support")]
    #[tokio::test]
    async fn marker_stop_non_final_chunk_needmore_no_emit() {
        // One row, then a marker byte (IS_MARKER set, END_OF_PARTITION clear).
        let data = synthetic_partition(&[STUB_ROW_BYTE, STUB_MARKER_BYTE]);
        let (step, buffered, collected) = drive(&data, false).await;
        assert_eq!(
            step,
            ParseStep::NeedMore,
            "on_range_marker -> Stop on a non-final chunk requests more bytes"
        );
        assert_eq!(buffered, 1, "the pre-marker row was buffered into pending");
        assert!(
            collected.is_empty(),
            "a marker Stop discards pending and forwards nothing"
        );
    }
}
