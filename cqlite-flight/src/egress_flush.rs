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

use arrow::datatypes::Schema as ArrowSchema;
use cqlite_core::export::{build_arrow_schema, rows_to_record_batch_with_schema};
use cqlite_core::query::QueryRow;

use crate::batch_bytes::{worst_case_batch_capacity_bytes, BatchByteCap};
use crate::egress_credit::{count_arrow_array_nodes, CreditedBatch};
use crate::producer::{BatchSink, MergeProducer, ProducerError};

/// The per-merge, batch-invariant Arrow facts both drive loops need at every
/// flush point: the output [`ArrowSchema`] and its array-node count.
///
/// Built ONCE per merge (issue #3096, lever 6) and threaded through
/// [`MergeProducer::flush_credited`] in place of the bare `n_array_nodes` this
/// replaced. Neither field can change between batches of one merge — the output
/// column set is fixed when the merge starts — so rebuilding either per batch was
/// pure repeat work: `build_arrow_schema` allocates a `Vec<Field>` with an owned
/// `String` name per column (plus an extension-metadata `HashMap` per
/// uuid/timeuuid column), and `count_arrow_array_nodes` walks it.
pub(crate) struct EgressBatchPlan {
    /// The batch schema every flush labels its arrays with.
    ///
    /// Derived from `self.columns` — the SAME slice [`MergeProducer::flush_buffer`]
    /// builds the arrays from, which is what `rows_to_record_batch(&self.columns,
    /// ..)` derived per batch before. It is deliberately NOT `output_columns()`:
    /// `RecordBatch::try_new` validates the arrays against this schema, so the two
    /// must come from one slice or a projection/aggregation shape would fail
    /// closed here instead of building.
    schema: Arc<ArrowSchema>,
    /// Arrow array NODES over the OUTPUT schema (`output_columns()`), which is
    /// the slice the reservation has always been sized from.
    ///
    /// Nodes, not columns: `crate::batch_bytes::BATCH_BYTES_PER_COLUMN_SLACK` is
    /// a per-array-node allowance, so a `map<text,text>` column contributes four.
    array_nodes: usize,
}

impl MergeProducer {
    /// Build the per-merge [`EgressBatchPlan`] — once, before the drive loop.
    ///
    /// The two derivations below keep the pre-change slices EXACTLY: the node
    /// count from `output_columns()` (what `egress_array_nodes` used) and the
    /// batch schema from `self.columns` (what `flush_buffer`'s
    /// `rows_to_record_batch` used). On the row drive path the two coincide —
    /// `output_columns()` returns `self.columns` unless `partial_columns` is set,
    /// which only the aggregate route (a different, materializing path that never
    /// reaches `flush_credited`) sets. Deriving both from one slice would still be
    /// correct today and would silently change behaviour the day that stops
    /// holding, so it is not done.
    pub(crate) fn egress_batch_plan(&self) -> Result<EgressBatchPlan, ProducerError> {
        let array_nodes = count_arrow_array_nodes(&build_arrow_schema(self.output_columns())?);
        let schema = Arc::new(build_arrow_schema(&self.columns)?);
        Ok(EgressBatchPlan {
            schema,
            array_nodes,
        })
    }

    /// Convert `buffer`'s rows into an Arrow batch over the plan's SHARED schema
    /// and clear it.
    ///
    /// Lives here rather than in `producer.rs` (already far over the campsite
    /// source threshold, epic #1116) beside its only caller,
    /// [`Self::flush_credited`].
    fn flush_buffer(
        &self,
        plan: &EgressBatchPlan,
        buffer: &mut Vec<QueryRow>,
    ) -> Result<arrow::record_batch::RecordBatch, ProducerError> {
        let batch =
            rows_to_record_batch_with_schema(Arc::clone(&plan.schema), &self.columns, buffer)?;
        buffer.clear();
        Ok(batch)
    }

    /// Re-derive the payload estimate of exactly the rows in `buffer`, the way
    /// the running accumulator was built (one `BatchByteCap::row_width` per row,
    /// saturating).
    ///
    /// Debug-only: this is the O(rows) recomputation the incremental accumulator
    /// exists to avoid, used solely to keep the accumulator ⇄ buffer invariant in
    /// [`Self::flush_credited`] enforced rather than merely documented.
    #[cfg(debug_assertions)]
    fn recomputed_buffer_payload(&self, buffer: &[QueryRow]) -> usize {
        let columns = self.output_columns();
        buffer.iter().fold(0usize, |acc, row| {
            acc.saturating_add(BatchByteCap::row_width(columns, row))
        })
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
        buffer: &mut Vec<QueryRow>,
        byte_cap: &mut BatchByteCap,
        plan: &EgressBatchPlan,
    ) -> Result<(), ProducerError> {
        #[cfg(debug_assertions)]
        {
            assert_eq!(
                byte_cap.accumulated(),
                self.recomputed_buffer_payload(buffer),
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
            worst_case_batch_capacity_bytes(byte_cap.accumulated(), plan.array_nodes, 0);
        // Parks here on an exhausted pool, with ONLY the row buffer resident —
        // nothing is materialized while a reservation is pending.
        let reservation = sink.reserve(reserve_capacity_bytes)?;
        // Issue #2819: time ONLY the Arrow build as `stream_encode` — NOT the
        // reserve park above (egress-credit backpressure, client-paced) nor the
        // emit below (`stream_grpc_write`). Runs on the merge-consumer thread once
        // per batch; a no-op with no flight sink installed (non-flight callers).
        let batch = cqlite_core::observability::stream_subphase::timed(
            cqlite_core::observability::StreamSubPhase::Encode,
            || self.flush_buffer(plan, buffer),
        )?;
        let actual_capacity_bytes = batch.get_array_memory_size();
        // Trues up DOWNWARD; an `actual > reserved` fails closed (never upward).
        let permit = reservation.materialize(actual_capacity_bytes)?;
        byte_cap.reset();
        sink.emit(CreditedBatch::new(batch, permit))
    }
}

#[cfg(test)]
#[path = "egress_flush_tests.rs"]
mod egress_flush_tests;
