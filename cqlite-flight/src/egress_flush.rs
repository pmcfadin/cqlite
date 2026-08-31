//! The single owning `reserve → build → true-up → emit` batch-boundary helper
//! (issue #2821).
//!
//! Both streaming drive loops (`MergeProducer::drive_merge`'s
//! partition-at-a-time loop and `MergeProducer::drive_merge_streaming`'s
//! row-granular one) finish a batch at three points each: the byte-cap cut, the
//! row-cap cut, and the end-of-merge tail. All SIX call [`MergeProducer::flush_credited`]
//! rather than `sink.emit(self.flush_buffer(..)?)?`, so a build site CANNOT
//! materialize a `RecordBatch` without first holding credit for it — the
//! ordering is owned by this helper rather than left as an unenforced calling
//! convention a future build site could forget.
//!
//! Lives in its own module because `producer.rs` (~3.4k lines) and
//! `producer_stream.rs` are already at/over the campsite source threshold
//! (epic #1116).

use cqlite_core::export::{build_arrow_schema, ArrowRowAccumulator};

use crate::batch_bytes::{worst_case_batch_capacity_bytes, BatchByteCap};
use crate::egress_credit::{count_arrow_array_nodes, CreditedBatch};
use crate::producer::{BatchSink, MergeProducer, ProducerError};

impl MergeProducer {
    /// Arrow array NODES over this producer's projected output schema, counted
    /// ONCE per merge and threaded into every reservation.
    ///
    /// Nodes, not columns: `crate::batch_bytes::BATCH_BYTES_PER_COLUMN_SLACK` is
    /// a per-array-node allowance, so a `map<text,text>` column contributes four.
    pub(crate) fn egress_array_nodes(&self) -> Result<usize, ProducerError> {
        let schema = build_arrow_schema(self.output_columns())?;
        Ok(count_arrow_array_nodes(&schema))
    }

    /// Convert `buffer`'s committed rows into an Arrow batch and clear it.
    ///
    /// This is the `do_get` row route's ONLY batch-materialization point — all six
    /// flush sites reach it through [`Self::flush_credited`].
    ///
    /// The schema is derived from `self.columns` per batch by the accumulator's
    /// `to_record_batch`, which builds it with `build_arrow_schema` and hands it
    /// straight to `RecordBatch::try_new`, so it is built once and never
    /// revalidated (the same non-revalidating path `rows_to_record_batch` takes).
    /// Hoisting that build
    /// to once per merge was measured twice on the WS0 corpus and delivered nothing
    /// (issue #3096: +0.30%, 95% CI covering zero, 4.5x below this box's ~1.4%
    /// between-binary code-layout noise floor; the removed per-batch work is
    /// 1.53 cycles/row of 23,940), so it is not done.
    ///
    /// Lives here rather than in `producer.rs` (~3.2k lines, far over the campsite
    /// source threshold, epic #1116) beside its only caller,
    /// [`Self::flush_credited`].
    ///
    /// Issue #3552: the cells are ALREADY transposed — the accumulator resolved
    /// each of them once, at push time, when it charged the row's payload width —
    /// so this runs the builders only, never a second pass over the rows. A
    /// STAGED-but-uncommitted row (the byte-cap's crossing row) survives the
    /// clear and opens the next batch, exactly as it used to be pushed into the
    /// freshly emptied `Vec<QueryRow>`.
    fn flush_buffer(
        &self,
        buffer: &mut ArrowRowAccumulator<'_>,
    ) -> Result<arrow::record_batch::RecordBatch, ProducerError> {
        let batch = buffer.to_record_batch()?;
        buffer.clear();
        Ok(batch)
    }

    /// Reserve egress credit for the buffered rows, materialize them under that
    /// credit, true the reservation down to the realized capacity, and emit.
    ///
    /// `byte_cap.accumulated()` is, at every one of the six call sites, EXACTLY
    /// the payload estimate for the rows currently in `buffer` (the byte-cap cut
    /// fires before the crossing row is pushed and before `reset()`; the row-cap
    /// cut fires after the pushed row was accumulated; the tail has not been
    /// reset since the previous flush). The accumulator is reset here, so the
    /// caller never has to.
    ///
    /// # Cross-issue invariant (issue #2821 ⇄ issue #2825) — do not weaken
    ///
    /// This reservation is a true upper bound on the realized capacity only
    /// because BOTH of these hold:
    ///
    /// 1. `Σ estimate_arrow_row_bytes(columns, row) >= arrow_payload_bytes(batch)`
    ///    — `cqlite-core/src/export/arrow_size.rs`, "Conservatism is a contract,
    ///    not an aspiration", enforced by the property test in
    ///    `cqlite-core/src/export/arrow_size_tests.rs` over fixed-width, text,
    ///    blob, list/set, map, tuple/UDT, JSON, nested-empty, all-null and
    ///    empty-string shapes.
    /// 2. `get_array_memory_size() <= worst_case_batch_capacity_bytes(payload,
    ///    n_array_nodes, 0)` — `crate::batch_bytes`, from `MutableBuffer`'s
    ///    power-of-two growth.
    ///
    /// Weakening either leg silently voids the per-stream memory bound this
    /// reservation publishes. `arrow_size.rs`'s conservatism section names THIS
    /// ceiling as a dependent consumer for the same reason.
    ///
    /// The conversion uses the FULL published worst case, not a bare
    /// `estimate × BATCH_BYTES_CAPACITY_FACTOR`: the factor alone under-states
    /// capacity by `BATCH_BYTES_PER_COLUMN_SLACK × n_array_nodes`, which on a
    /// wide/nested schema is exactly what would trip the fail-closed path.
    pub(crate) fn flush_credited(
        &self,
        sink: &mut dyn BatchSink,
        buffer: &mut ArrowRowAccumulator<'_>,
        byte_cap: &mut BatchByteCap,
        n_array_nodes: usize,
    ) -> Result<(), ProducerError> {
        #[cfg(debug_assertions)]
        {
            // Re-derived from the buffer's STORED cells (issue #3552), row by
            // row, through the same charging core the running accumulator was
            // advanced by — the O(rows) recomputation the incremental
            // accumulator exists to avoid, kept so this invariant is enforced
            // rather than merely documented.
            assert_eq!(
                byte_cap.accumulated(),
                buffer.recomputed_payload(),
                "flush_credited: `byte_cap.accumulated()` must describe EXACTLY the rows in \
                 `buffer` — a call site that flushed without resetting (or reset without \
                 flushing) would under-reserve here and trip the fail-closed path (issue #2821)"
            );
            assert!(
                !buffer.is_empty() || byte_cap.accumulated() == 0,
                "flush_credited: an empty buffer must carry a zero accumulator"
            );
        }
        // Payload estimate → CAPACITY reservation. Two currencies meet here and
        // only here on the producer side (design D0): `accumulated()` is PAYLOAD
        // bytes, everything downstream of this line is CAPACITY bytes.
        let reserve_capacity_bytes =
            worst_case_batch_capacity_bytes(byte_cap.accumulated(), n_array_nodes, 0);
        // Parks here on an exhausted pool, with ONLY the row buffer resident —
        // nothing is materialized while a reservation is pending.
        let reservation = sink.reserve(reserve_capacity_bytes)?;
        // Issue #2819: time ONLY the Arrow build as `stream_encode` — NOT the
        // reserve park above (egress-credit backpressure, client-paced) nor the
        // emit below (`stream_grpc_write`). Runs on the merge-consumer thread once
        // per batch; a no-op with no flight sink installed (non-flight callers).
        let batch = cqlite_core::observability::stream_subphase::timed(
            cqlite_core::observability::StreamSubPhase::Encode,
            || self.flush_buffer(buffer),
        )?;
        let actual_capacity_bytes = batch.get_array_memory_size();
        // Trues up DOWNWARD; an `actual > reserved` fails closed (never upward).
        let permit = reservation.materialize(actual_capacity_bytes)?;
        byte_cap.reset();
        sink.emit(CreditedBatch::new(batch, permit))
    }
}
