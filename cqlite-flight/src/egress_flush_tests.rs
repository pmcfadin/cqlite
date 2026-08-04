//! The per-merge [`EgressBatchPlan`] (issue #3096, lever 6).
//!
//! `flush_buffer` rebuilt the whole Arrow `Schema` — a `Vec<Field>` with an owned
//! `String` name per column — on EVERY batch of a scan, then dropped it. The plan
//! builds it once per merge and every batch shares the `Arc`.
//!
//! "Shares the `Arc`" is the property, so the assertion is `Arc::ptr_eq` across
//! the batches of one merge: a value-equality assertion would pass on the
//! pre-change binary (the rebuilt schemas were equal, just not shared) and prove
//! nothing. The output-equality half — that sharing did not change what is
//! emitted — is asserted here against a batch built the old way, and end to end
//! by the Arrow-buffer digest oracle (`tests/issue_3096_arrow_buffer_digest.rs`).
//!
//! No wall-clock threshold is asserted here (#2642).

use super::*;
use crate::producer::CollectSink;
use crate::testutil::{build_sstables, simple_schema, write_row};
use arrow::record_batch::RecordBatch;
use cqlite_core::export::{
    prevalidated_batch_builds_on_this_thread, rows_to_record_batch,
    schema_validations_on_this_thread,
};
use std::sync::Arc;

/// A fixture whose merge emits SEVERAL batches at `batch_size = 1`, so the
/// per-batch sharing has something to be observed across.
const ROWS: i32 = 8;

/// Drive the row path over a small fixture and return every emitted batch.
fn produced_batches(batch_size: usize) -> Vec<RecordBatch> {
    let schema = simple_schema();
    let rows: Vec<_> = (1..=ROWS).map(|i| write_row(i, "n", i, 100)).collect();
    let (_temp, _data, dir) = build_sstables(&schema, vec![rows]);
    let producer = MergeProducer::new(schema, batch_size).expect("producer");
    let paths = producer
        .resolve_paths(&crate::producer::DirSource::new(&dir))
        .expect("resolve");
    let mut out: Vec<RecordBatch> = Vec::new();
    let mut sink = CollectSink(&mut out);
    producer
        .produce_streaming(
            paths,
            &crate::cancel::CancelFlag::new(),
            &mut sink,
            &crate::scan_progress::ScanProgress::default(),
            || {},
        )
        .expect("produce_streaming");
    out
}

#[test]
fn every_batch_of_one_merge_shares_one_schema_arc() {
    let batches = produced_batches(1);
    assert!(
        batches.len() >= 2,
        "the fixture must emit at least two batches or the sharing assertion is \
         vacuous; got {}",
        batches.len()
    );
    let first = batches[0].schema();
    for (i, b) in batches.iter().enumerate().skip(1) {
        assert!(
            Arc::ptr_eq(&first, &b.schema()),
            "batch {i} carries a DIFFERENT schema Arc from batch 0 — the merge is \
             rebuilding the Arrow schema per batch (issue #3096, lever 6). Note a \
             value-equality check would pass here on the pre-change binary; only \
             pointer equality proves the schema is shared rather than rebuilt."
        );
    }
}

#[test]
fn the_shared_schema_equals_the_per_batch_schema_it_replaced() {
    // Output invariance for the cache itself: the shared schema must be exactly
    // what `rows_to_record_batch(&self.columns, ..)` derived per batch.
    let schema = simple_schema();
    let producer = MergeProducer::new(schema.clone(), 8).expect("producer");
    let plan = producer.egress_batch_plan().expect("plan");
    let reference = rows_to_record_batch(&producer.columns, &[]).expect("reference batch");
    assert_eq!(
        plan.schema.schema().as_ref(),
        reference.schema().as_ref(),
        "the cached schema diverged from the one the per-batch build produced"
    );
    assert_eq!(
        plan.schema.columns().len(),
        producer.columns.len(),
        "the plan's schema must be bound to the producer's own columns — that \
         binding is what lets the flush skip revalidation"
    );
}

/// **The finding, pinned on the surface that ships.** The roborev finding said the
/// redundant validation "leaves the Flight path rebuilding fields per batch", and
/// the first fix reached only `rows_to_record_batch` — the AGGREGATE route
/// (`producer.rs`), not `do_get`'s row route. `do_get` flushes through
/// `flush_credited` → `flush_buffer`, which passed a bare `Arc<Schema>` to
/// `rows_to_record_batch_with_schema` and so paid `column_to_field` per column per
/// batch for the whole scan, defeating lever 6 on the only path a client sees.
///
/// Both halves are asserted, because the negative one alone would be vacuous:
///
/// 1. **Positive control** — the flush path built its batches through the
///    PREVALIDATED entry point, on THIS thread, at least twice. Without this, "zero
///    validations on this thread" would also hold for a thread that built nothing
///    (e.g. if the flush ever moved to a worker thread), and the test would go
///    quietly green while measuring nothing.
/// 2. **The property** — across all of those batches the schema was validated ZERO
///    times.
///
/// A counter is the only way to see this: a schema `build_arrow_schema` just
/// produced can never FAIL validation, so "validated and passed" and "not
/// validated" produce the identical batch. An output-equality test therefore passes
/// before and after the fix and guards nothing.
#[test]
fn the_do_get_flush_path_builds_every_batch_prevalidated_and_never_revalidates() {
    let validations_before = schema_validations_on_this_thread();
    let builds_before = prevalidated_batch_builds_on_this_thread();

    let batches = produced_batches(1);

    assert!(
        batches.len() >= 2,
        "the fixture must emit several batches or the per-batch claim is vacuous; \
         got {}",
        batches.len()
    );
    let prevalidated_builds = prevalidated_batch_builds_on_this_thread() - builds_before;
    assert_eq!(
        prevalidated_builds,
        batches.len(),
        "every emitted batch must have been built through \
         `rows_to_record_batch_prevalidated` ON THIS THREAD — a count of 0 means the \
         flush is routed through the validating entry point (or ran elsewhere), and \
         the zero-validation assertion below would be vacuous"
    );
    assert_eq!(
        schema_validations_on_this_thread() - validations_before,
        0,
        "the `do_get` flush path revalidated the shared schema — that reconstructs a \
         `Field` per column per batch for the whole scan and cancels lever 6 on the \
         shipping surface (issue #3096, fourth review)"
    );
}

#[test]
fn the_plan_sizes_the_reservation_from_the_output_columns() {
    // The node count keeps its pre-change slice (`output_columns()`), which the
    // egress reservation is sized from; folding it onto `self.columns` would
    // silently change the reservation on an aggregate shape.
    let producer = MergeProducer::new(simple_schema(), 8).expect("producer");
    let plan = producer.egress_batch_plan().expect("plan");
    let expected = crate::egress_credit::count_arrow_array_nodes(
        &cqlite_core::export::build_arrow_schema(producer.output_columns()).expect("schema"),
    );
    assert_eq!(
        plan.array_nodes, expected,
        "the plan's array-node count must equal the pre-change \
         `count_arrow_array_nodes(build_arrow_schema(output_columns()))`"
    );
}
