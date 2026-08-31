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

use std::sync::Arc;
use std::time::Instant;

use cqlite_core::export::{build_arrow_schema, ArrowRowAccumulator};
use cqlite_core::observability::stream_subphase;
use cqlite_core::observability::{StreamSubPhase, StreamSubPhaseTimings};

use crate::batch_bytes::{worst_case_batch_capacity_bytes, BatchByteCap};
use crate::egress_credit::{count_arrow_array_nodes, CreditedBatch};
use crate::producer::{BatchSink, MergeProducer, ProducerError};

/// Local accumulator for the PUSH-time half of `stream_encode` (issue #3552).
///
/// # Why this exists
///
/// Before issue #3552 the row→column transpose ran inside `flush_buffer`, i.e.
/// inside the `StreamSubPhase::Encode` region. The fold moved it to PUSH time
/// (`ArrowRowAccumulator::stage` for the cell resolution and width charge, then
/// `ArrowRowAccumulator::commit` for the move into column-major storage — BOTH
/// halves are timed; an earlier revision of this doc named only `stage`, and only
/// `stage` was wrapped, so the counter under-reported wide projections by exactly
/// the commit walk, issue #3552 roborev round 7), so without this accumulator the
/// have left the measured window WITHOUT LEAVING THE PROGRAM: `stream_encode`
/// would read LOWER with no work removed, and whoever took issue #3552's own
/// before/after measurement would read a phantom improvement in the very
/// instrument the change is judged by (CLAUDE.md's #2877 shape — a share shift
/// with unmoved rows/s is a FAIL, and an instrument that shifts by itself cannot
/// tell you which happened).
///
/// # Why it is not an atomic per row
///
/// The sink `Arc` is resolved ONCE at construction (one thread-local read, not one
/// per row — the issue #2819 B3 rule), each staged row's elapsed nanos fold into a
/// plain `u64` in the drive loop's own frame, and the total reaches the shared
/// `AtomicU64` in ONE `add_nanos` on [`Drop`]. So a full scan makes exactly one
/// atomic write regardless of row count, and an early return (a cooperative
/// cancel, a `LIMIT` break, a `?`-propagated error) still records the work already
/// done. `Drop` runs when the drive loop returns, which is strictly before the
/// teardown emitter and the `df_spike` harness read the counters, so the emitted
/// per-RPC total is the same as folding per flush would give.
///
/// Modelled on `producer_stream::RowSubPhaseAccum`, which does exactly this for
/// `stream_merge`.
///
/// # Inert with no flight sink
///
/// Every non-flight caller (compaction, CLI, point reads outside `do_get`)
/// installs no sink, so [`Self::timed`] is one `Option` check on a local plus the
/// bare closure — no `Instant::now()`, no thread-local read, no atomic — and
/// `Drop` records nothing.
pub(crate) struct StageEncodeAccum {
    sink: Option<Arc<StreamSubPhaseTimings>>,
    nanos: u64,
}

impl StageEncodeAccum {
    /// Resolve the per-request sub-phase sink ONCE, before the drive loop.
    pub(crate) fn new() -> Self {
        Self {
            sink: stream_subphase::current(),
            nanos: 0,
        }
    }

    /// Run `f` — the push-time cell resolution + width charge — and fold its
    /// elapsed wall time into the local `stream_encode` total.
    ///
    /// Exactly one `Instant::now()` pair PER CALL when instrumented — two per row,
    /// since both halves of the push-time transpose (`stage`, then `commit`) are
    /// timed — and NO clock at all when not.
    #[inline]
    pub(crate) fn timed<T>(&mut self, f: impl FnOnce() -> T) -> T {
        if self.sink.is_none() {
            return f();
        }
        let start = Instant::now();
        let out = f();
        self.nanos = self
            .nanos
            .saturating_add(stream_subphase::elapsed_nanos(start));
        out
    }
}

impl Drop for StageEncodeAccum {
    fn drop(&mut self) {
        if let Some(sink) = &self.sink {
            sink.add_nanos(StreamSubPhase::Encode, self.nanos);
        }
    }
}

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
    /// 1. `Σ width(columns, row) >= arrow_payload_bytes(batch)` —
    ///    `cqlite-core/src/export/arrow_size.rs`, "Conservatism is a contract, not
    ///    an aspiration", enforced by the property test in
    ///    `cqlite-core/src/export/arrow_size_tests.rs` over fixed-width, text,
    ///    blob, list/set, map, tuple/UDT, JSON, nested-empty, all-null and
    ///    empty-string shapes.
    ///
    ///    `width` on BOTH row routes is now `ArrowRowAccumulator::stage`'s return
    ///    value, NOT a call to `estimate_arrow_row_bytes` — issue #3552 folded the
    ///    charge into the build pass's cell resolution, so neither row route calls
    ///    the standalone estimator at all (the aggregate route still does, through
    ///    `BatchByteCap::row_width`). The two are the same number by construction
    ///    (one shared charging core, `arrow_size::charge_row`, differing only in
    ///    how a cell is resolved) and BY TEST: the equivalence is pinned per row
    ///    over the shared shape corpus by
    ///    `export::arrow_row_accumulator::arrow_row_accumulator_tests::fused_width_equals_the_standalone_estimate_over_the_shape_corpus`,
    ///    which is what carries the conservatism property above onto this path.
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
        //
        // This is now the FLUSH-TIME half of `stream_encode`. Issue #3552 moved the
        // row→column transpose to push time, where [`StageEncodeAccum`] times it
        // into the SAME bucket — so the bucket still spans the whole
        // resolve-then-build cost, just recorded from two places. See
        // `StageEncodeAccum` for what that changed about the bucket's SCOPE.
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

#[cfg(test)]
mod stage_encode_scope_tests {
    /// `stream_encode` must cover BOTH halves of the push-time transpose.
    ///
    /// STRUCTURAL rather than timing-based **on purpose**. What is at risk here is the
    /// SCOPE — which calls sit inside the timed window — and that is a source property,
    /// so it is asserted against the source. A timing-based version (compare a wide
    /// projection's `stream_encode` against a narrow one's) would be a wall-clock
    /// threshold assert in a correctness test path, which is a MECHANIZED
    /// `roborev-lints` failure (#2642) — so the test roborev asked for cannot be
    /// written that way. Precedent for asserting on source:
    /// `cqlite-core/src/observability/error_schema_tests.rs`.
    ///
    /// The defect this pins (issue #3552, roborev round 7): `commit()` sat OUTSIDE the
    /// window. It moves staged cells into column-major storage — work the fold MOVED
    /// there from the formerly-timed transpose (and since round 14 it walks only the
    /// slots the row filled, not every projected column) — so
    /// `stream_encode` under-reported wide projections by exactly that walk, while the
    /// docs named only `stage` and so read as if nothing was lost.
    #[test]
    fn both_push_time_transpose_halves_are_inside_the_timed_window() {
        for (name, src) in [
            ("producer_drive.rs", include_str!("producer_drive.rs")),
            ("producer_stream.rs", include_str!("producer_stream.rs")),
        ] {
            assert!(
                src.contains("stage_encode.timed(|| buffer.stage(row))"),
                "{name}: `stage` is not inside a StageEncodeAccum::timed window"
            );
            assert!(
                src.contains("stage_encode.timed(|| buffer.commit())"),
                "{name}: `commit` is NOT inside a StageEncodeAccum::timed window — the \
                 push-time transpose's second half goes uncounted and `stream_encode` \
                 under-reports wide projections (issue #3552, roborev round 7)"
            );
            // An UNTIMED `buffer.commit();` statement at either loop's indentation would
            // mean a call escaped the window even if a timed one exists elsewhere.
            for indent in [
                "\n            buffer.commit();",
                "\n                    buffer.commit();",
            ] {
                assert!(
                    !src.contains(indent),
                    "{name}: an UNTIMED `buffer.commit();` call remains"
                );
            }
        }
    }
}
