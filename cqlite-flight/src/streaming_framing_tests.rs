//! The arrow-flight encoder's wire-side re-slicing target (issue #3096, lever 4).
//!
//! [`crate::batch_bytes::FLIGHT_DATA_SIZE_TARGET_BYTES`] is only worth stating if
//! it actually reaches `FlightDataEncoderBuilder`. These tests drive the REAL
//! [`super::encode_do_get`] — the exact function the `do_get` chain calls
//! (`service.rs do_get` → `spawn_streaming` → `encode_do_get`) — and count the
//! `FlightData` messages one `RecordBatch` of a KNOWN buffer capacity is framed
//! into.
//!
//! The batch capacities below are chosen so the pre-change (arrow-flight default
//! 2 MiB) and post-change (4 MiB) targets give DIFFERENT message counts, so these
//! tests fail on a binary that dropped the builder call rather than passing
//! vacuously:
//!
//! | batch buffer capacity | @2 MiB (arrow default) | @4 MiB (this tree) |
//! |---|--:|--:|
//! | 3 MiB | 2 messages | **1** |
//! | 6 MiB | 3 messages | **2** |
//!
//! No wall-clock threshold is asserted anywhere here (#2642): every assertion is
//! a message count or a byte size.

use super::*;
use crate::batch_bytes::FLIGHT_DATA_SIZE_TARGET_BYTES;
use arrow::array::{Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

/// arrow-flight 53.4.1's `GRPC_TARGET_MAX_FLIGHT_SIZE_BYTES`, restated locally so
/// a change in the dependency's default shows up as a failure here rather than
/// silently re-aligning the two values.
const ARROW_FLIGHT_DEFAULT_TARGET: usize = 2 * 1024 * 1024;

/// A single-`Int64` batch whose value buffer holds EXACTLY `mib` MiB.
///
/// `Int64Array::from(Vec<i64>)` takes the vector's allocation as its buffer, so
/// `get_buffer_memory_size()` is `len * 8` with no validity bitmap — an exact,
/// non-power-of-two-rounded capacity, which is what makes the message counts in
/// the module table deterministic rather than allocator-dependent.
fn batch_of_capacity_mib(mib: usize) -> (RecordBatch, Arc<ArrowSchema>) {
    let n = mib * 1024 * 1024 / 8;
    let array = Int64Array::from((0..n as i64).collect::<Vec<i64>>());
    assert_eq!(
        array.to_data().get_buffer_memory_size(),
        mib * 1024 * 1024,
        "the fixture must have EXACTLY {mib} MiB of buffer capacity, or the \
         expected split factors below are meaningless"
    );
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "v",
        DataType::Int64,
        false,
    )]));
    let batch =
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array)]).expect("record batch");
    (batch, schema)
}

/// Drive `encode_do_get` over one batch and return every emitted `FlightData`
/// body length, EXCLUDING the leading schema message (which carries no body).
async fn body_sizes(batch: RecordBatch, schema: Arc<ArrowSchema>) -> Vec<usize> {
    let rows = batch.num_rows();
    let stream = futures::stream::iter(vec![Ok::<_, FlightError>(batch)]);
    let mut encoded = encode_do_get(stream, schema, StreamProbe::default());
    let mut bodies = Vec::new();
    while let Some(msg) = encoded.next().await {
        let data = msg.expect("encoder message");
        if !data.data_body.is_empty() {
            bodies.push(data.data_body.len());
        }
    }
    assert!(
        !bodies.is_empty(),
        "a {rows}-row batch must produce at least one data message — an empty \
         result would make every count below vacuous"
    );
    bodies
}

#[tokio::test]
async fn a_batch_under_the_target_is_framed_as_one_message() {
    // 3 MiB of capacity: above arrow-flight's own 2 MiB default (which would cut
    // it in two), below this tree's 4 MiB target (which must not cut it at all).
    let (batch, schema) = batch_of_capacity_mib(3);
    let bodies = body_sizes(batch, schema).await;
    assert_eq!(
        bodies.len(),
        1,
        "a 3 MiB batch must be framed as ONE FlightData message at a \
         {FLIGHT_DATA_SIZE_TARGET_BYTES}-byte target; {} messages means the \
         encoder is still running on arrow-flight's {ARROW_FLIGHT_DEFAULT_TARGET}-byte \
         default (sizes: {bodies:?})",
        bodies.len()
    );
}

#[tokio::test]
async fn a_batch_over_the_target_still_splits_for_the_wire() {
    // The target is a wire-safety backstop, not a suggestion: a batch the
    // producer cap did not govern must STILL be cut for the gRPC message limit.
    let (batch, schema) = batch_of_capacity_mib(6);
    let bodies = body_sizes(batch, schema).await;
    assert_eq!(
        bodies.len(),
        2,
        "a 6 MiB batch must still be split at the {FLIGHT_DATA_SIZE_TARGET_BYTES}-byte \
         target (sizes: {bodies:?})"
    );
    for (i, size) in bodies.iter().enumerate() {
        assert!(
            *size <= FLIGHT_DATA_SIZE_TARGET_BYTES,
            "message {i} is {size} B, over the {FLIGHT_DATA_SIZE_TARGET_BYTES}-byte \
             wire target — a message above the typical 4 MiB gRPC ceiling is an \
             interop break, not an optimization"
        );
    }
}

/// The 8 MiB rejection, pinned against the CONSTANTS that record it
/// (`batch_bytes.rs`, issue #3096) rather than against a report.
///
/// Two independent things are checked, because either alone is weak:
///
/// 1. **The interop ceiling relations.** The shipped target is inside
///    `GRPC_DEFAULT_MAX_MESSAGE_BYTES`; the rejected one is outside it and its
///    measured bodies sat on it. (Also guarded at compile time in
///    `batch_bytes.rs` — a raised target fails the BUILD. This test is what says
///    *why* when someone reads the failure.)
/// 2. **Internal consistency of the recorded measurements.** The mean body size
///    is `rows x payload_bytes_per_row / messages`, so the recorded message
///    counts, the recorded corpus width and the recorded body sizes cannot be
///    edited independently. This is what stops the numbers rotting into
///    plausible-looking fiction.
#[test]
fn the_rejected_8mib_target_is_recorded_as_a_grpc_interop_break() {
    use crate::batch_bytes::{
        GRPC_DEFAULT_MAX_MESSAGE_BYTES, MEASURED_ARROW_PAYLOAD_BYTES_PER_ROW,
        MEASURED_DATA_BODY_BYTES_AT_REJECTED_TARGET, MEASURED_DATA_BODY_BYTES_AT_SHIPPED_TARGET,
        MEASURED_FLIGHT_DATA_MESSAGES, MEASURED_FRAMING_ROWS,
        REJECTED_FLIGHT_DATA_SIZE_TARGET_BYTES,
    };

    assert_eq!(
        GRPC_DEFAULT_MAX_MESSAGE_BYTES,
        4 * 1024 * 1024,
        "the recorded interop ceiling is tonic's and grpc-java's 4 MiB default \
         max inbound message size; changing it re-opens the whole rejection"
    );
    assert!(
        FLIGHT_DATA_SIZE_TARGET_BYTES <= GRPC_DEFAULT_MAX_MESSAGE_BYTES,
        "the shipped wire target ({FLIGHT_DATA_SIZE_TARGET_BYTES} B) must stay inside \
         the {GRPC_DEFAULT_MAX_MESSAGE_BYTES} B gRPC ceiling"
    );
    assert!(
        REJECTED_FLIGHT_DATA_SIZE_TARGET_BYTES > GRPC_DEFAULT_MAX_MESSAGE_BYTES,
        "the rejected target must be recorded as ABOVE the ceiling — that is the \
         reason it was rejected"
    );
    // 3.90 MB of body, plus IPC metadata and protobuf framing, on a 4 MiB ceiling.
    assert!(
        MEASURED_DATA_BODY_BYTES_AT_REJECTED_TARGET * 10 > GRPC_DEFAULT_MAX_MESSAGE_BYTES * 9,
        "the measured body at the rejected target ({MEASURED_DATA_BODY_BYTES_AT_REJECTED_TARGET} B) \
         must be recorded as sitting ON the {GRPC_DEFAULT_MAX_MESSAGE_BYTES} B ceiling"
    );
    assert!(
        MEASURED_DATA_BODY_BYTES_AT_SHIPPED_TARGET * 2 < GRPC_DEFAULT_MAX_MESSAGE_BYTES,
        "the measured body at the shipped target must be recorded as comfortably \
         inside the ceiling"
    );

    // The recorded measurements must be mutually consistent at the recorded width.
    let total_payload = MEASURED_FRAMING_ROWS * MEASURED_ARROW_PAYLOAD_BYTES_PER_ROW;
    for (target, messages, recorded_body) in [
        (
            MEASURED_FLIGHT_DATA_MESSAGES[1].0,
            MEASURED_FLIGHT_DATA_MESSAGES[1].1,
            MEASURED_DATA_BODY_BYTES_AT_SHIPPED_TARGET,
        ),
        (
            MEASURED_FLIGHT_DATA_MESSAGES[2].0,
            MEASURED_FLIGHT_DATA_MESSAGES[2].1,
            MEASURED_DATA_BODY_BYTES_AT_REJECTED_TARGET,
        ),
    ] {
        let derived = total_payload / messages;
        let delta_pct =
            (derived as f64 - recorded_body as f64).abs() / recorded_body as f64 * 100.0;
        assert!(
            delta_pct < 5.0,
            "at a {target}-byte target the recorded {messages} messages over \
             {MEASURED_FRAMING_ROWS} rows x {MEASURED_ARROW_PAYLOAD_BYTES_PER_ROW} B/row imply \
             {derived} B/message, {delta_pct:.1}% from the recorded {recorded_body} B — one of \
             these figures was edited without re-measuring the others"
        );
    }

    // Raising the target buys fewer messages; that is never in dispute, and is not
    // the reason 8 MiB was rejected.
    assert!(
        MEASURED_FLIGHT_DATA_MESSAGES[0].1 > MEASURED_FLIGHT_DATA_MESSAGES[1].1
            && MEASURED_FLIGHT_DATA_MESSAGES[1].1 > MEASURED_FLIGHT_DATA_MESSAGES[2].1,
        "the recorded framing table must show message counts falling as the target \
         rises: {MEASURED_FLIGHT_DATA_MESSAGES:?}"
    );
    assert_eq!(
        MEASURED_FLIGHT_DATA_MESSAGES[0].0, ARROW_FLIGHT_DEFAULT_TARGET,
        "the framing table's first row is arrow-flight's own default target"
    );
}

/// The relationship the constant's doc rests on, asserted rather than narrated:
/// the target sits ABOVE arrow-flight's default (so a producer-capped batch stops
/// being re-sliced) and NOT above the producer's own payload cap (so the wire
/// message can never exceed the typical 4 MiB gRPC limit).
#[test]
fn the_target_sits_between_the_arrow_default_and_the_producer_payload_cap() {
    assert!(
        FLIGHT_DATA_SIZE_TARGET_BYTES > ARROW_FLIGHT_DEFAULT_TARGET,
        "the whole point of stating the target is that it is above arrow-flight's \
         {ARROW_FLIGHT_DEFAULT_TARGET}-byte default"
    );
    assert!(
        FLIGHT_DATA_SIZE_TARGET_BYTES <= crate::batch_bytes::DEFAULT_MAX_BATCH_BYTES,
        "raising the target above the producer's payload cap would let a single \
         FlightData message carry a full-cap batch (measured 3.90 MB bodies on the \
         issue #3096 corpus), on the 4 MiB gRPC message ceiling"
    );
}
