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
//! 2 MiB) and post-change (3.875 MiB) targets give DIFFERENT message counts, so
//! these tests fail on a binary that dropped the builder call rather than passing
//! vacuously:
//!
//! | batch buffer capacity | @2 MiB (arrow default) | @3.875 MiB (this tree) |
//! |---|--:|--:|
//! | 3 MiB | 2 messages | **1** |
//! | 6 MiB | 3 messages | **2** |
//!
//! # The wire-safety half (added in the #3096 review)
//!
//! The target used to EQUAL `GRPC_DEFAULT_MAX_MESSAGE_BYTES`, and the guard that
//! was supposed to forbid that was `<=` — so it admitted the one value the module
//! declared unsafe. Two things close that here:
//!
//! * every assertion against the target's relations is now STRICT, and
//! * [`a_ratio_one_binary_batch_frames_every_body_under_the_reserved_ceiling`]
//!   drives the real encoder over a `Binary` batch whose capacity/payload ratio
//!   is ~1.0 (so the target's capacity currency buys no hidden slack) at a payload
//!   just under the producer cap, and asserts EVERY emitted body stays under
//!   `FLIGHT_DATA_RESERVED_CEILING_BYTES`. That fixture is a genuine
//!   discriminator, verified by perturbation: with the target back at 4 MiB it
//!   frames ONE body of **4,188,224 B** — over the reserved ceiling, and just
//!   6,080 B under tonic's hard 4,194,304 refusal threshold before the IPC
//!   `data_header` is even added.
//!
//! No wall-clock threshold is asserted anywhere here (#2642): every assertion is
//! a message count or a byte size.

use super::*;
use crate::batch_bytes::{
    FLIGHT_DATA_RESERVED_CEILING_BYTES, FLIGHT_DATA_SIZE_TARGET_BYTES,
    FLIGHT_FRAMING_OVERHEAD_BYTES, GRPC_DEFAULT_MAX_MESSAGE_BYTES,
    SUPERSEDED_FLIGHT_DATA_SIZE_TARGET_BYTES,
};
use arrow::array::{Array, BinaryArray, Int64Array};
use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
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

/// A single-`Binary` batch of `n_values` values of `value_len` bytes whose buffer
/// **CAPACITY equals its payload** — capacity/payload ratio ~1.0.
///
/// This is the shape that defeats a target denominated in capacity. Both buffers
/// are built from exact-capacity `Vec`s (`Buffer::from_vec` / `ScalarBuffer::from`
/// adopt the vector's allocation), so `get_buffer_memory_size()` is the payload
/// itself — no power-of-two doubling, no slack. `BATCH_BYTES_CAPACITY_FACTOR`'s
/// doc records 1.001x measured on exactly this 8 KiB-`Binary` shape, so this is a
/// realistic blob/`Binary` table, not a synthetic edge.
fn ratio_one_binary_batch(n_values: usize, value_len: usize) -> (RecordBatch, Arc<ArrowSchema>) {
    let values = Buffer::from_vec(vec![0x5au8; n_values * value_len]);
    let offsets: Vec<i32> = (0..=n_values)
        .map(|i| i32::try_from(i * value_len).expect("offsets fit i32"))
        .collect();
    let array = BinaryArray::try_new(OffsetBuffer::new(ScalarBuffer::from(offsets)), values, None)
        .expect("binary array");
    let capacity = array.to_data().get_buffer_memory_size();
    let payload = n_values * value_len + (n_values + 1) * 4;
    assert!(
        capacity * 100 <= payload * 101,
        "the fixture must have a capacity/payload ratio of ~1.0 (got {capacity} B \
         capacity over {payload} B payload) — at a larger ratio the encoder's \
         capacity-denominated target would provide slack of its own and this test \
         would not exercise the hazard it exists for"
    );
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "b",
        DataType::Binary,
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

/// **The wire-safety regression test of the #3096 review.**
///
/// A `Binary` batch at capacity/payload ratio ~1.0 whose payload is just under the
/// producer's `DEFAULT_MAX_BATCH_BYTES` cap — the exact shape a blob-heavy table
/// produces — must be framed into messages that EVERY fit inside
/// `FLIGHT_DATA_RESERVED_CEILING_BYTES` (the gRPC ceiling less the reserved
/// per-message framing overhead).
///
/// **Non-vacuity is asserted, not assumed**: the fixture's payload is checked to
/// be ABOVE the reserved ceiling, so a single unsplit message would fail this test
/// — which is precisely what the superseded 4 MiB target produced.
#[tokio::test]
async fn a_ratio_one_binary_batch_frames_every_body_under_the_reserved_ceiling() {
    // 511 x 8 KiB = 4,186,112 B of values + 2,048 B of offsets = 4,188,160 B:
    // under the 4 MiB producer payload cap (4,194,304), over the reserved
    // ceiling (4,128,768).
    let (batch, schema) = ratio_one_binary_batch(511, 8 * 1024);
    let payload = 511 * 8 * 1024 + 512 * 4;
    assert!(
        payload > FLIGHT_DATA_RESERVED_CEILING_BYTES
            && payload < crate::batch_bytes::DEFAULT_MAX_BATCH_BYTES,
        "the fixture must sit BETWEEN the reserved ceiling and the producer payload \
         cap ({FLIGHT_DATA_RESERVED_CEILING_BYTES} < {payload} < {}), or the assertion \
         below could pass without the encoder splitting anything",
        crate::batch_bytes::DEFAULT_MAX_BATCH_BYTES
    );
    assert!(
        payload <= SUPERSEDED_FLIGHT_DATA_SIZE_TARGET_BYTES,
        "at the superseded {SUPERSEDED_FLIGHT_DATA_SIZE_TARGET_BYTES}-byte target this \
         fixture is framed as ONE body (measured 4,188,224 B) — over the reserved \
         ceiling and 6,080 B under the raw gRPC ceiling before any header. That is the \
         regression this test pins"
    );

    let bodies = body_sizes(batch, schema).await;
    assert!(
        bodies.len() > 1,
        "a {payload}-byte ratio-1.0 batch must be SPLIT at the \
         {FLIGHT_DATA_SIZE_TARGET_BYTES}-byte target; one message means the target is \
         back at (or above) the gRPC ceiling (sizes: {bodies:?})"
    );
    for (i, size) in bodies.iter().enumerate() {
        assert!(
            *size < FLIGHT_DATA_RESERVED_CEILING_BYTES,
            "message {i} is {size} B, at or over the {FLIGHT_DATA_RESERVED_CEILING_BYTES}-byte \
             reserved ceiling ({GRPC_DEFAULT_MAX_MESSAGE_BYTES} B gRPC default less \
             {FLIGHT_FRAMING_OVERHEAD_BYTES} B of IPC/protobuf framing). A tonic consumer \
             refuses that message (sizes: {bodies:?})"
        );
    }
}

/// The reviewer's constructed failure, verbatim: a raised `--max-batch-bytes`
/// (16 MiB) over a ratio-~1.0 `Binary` table.
///
/// At the superseded 4 MiB target this framed FOUR bodies of ~4.19 MB — every one
/// of them over the gRPC ceiling, so the stream died at the client. The producer
/// cap is caller-configurable by contract, so the wire-side target must hold for a
/// batch far larger than the default cap.
#[tokio::test]
async fn a_raised_batch_cap_still_frames_every_body_under_the_reserved_ceiling() {
    // 2,048 x 8 KiB = 16,777,216 B of values: a batch cut to `--max-batch-bytes 16MiB`.
    let (batch, schema) = ratio_one_binary_batch(2048, 8 * 1024);
    let bodies = body_sizes(batch, schema).await;
    assert!(
        bodies.len() >= 5,
        "a ~16 MiB ratio-1.0 batch must be cut into at least \
         ceil(16MiB / {FLIGHT_DATA_SIZE_TARGET_BYTES}) = 5 messages (sizes: {bodies:?})"
    );
    for (i, size) in bodies.iter().enumerate() {
        assert!(
            *size < FLIGHT_DATA_RESERVED_CEILING_BYTES,
            "message {i} is {size} B, at or over the {FLIGHT_DATA_RESERVED_CEILING_BYTES}-byte \
             reserved ceiling — a caller who raises --max-batch-bytes must not be able to \
             push this server into emitting illegal gRPC messages (sizes: {bodies:?})"
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
        MEASURED_ARROW_PAYLOAD_BYTES_PER_ROW, MEASURED_DATA_BODY_BYTES_AT_REJECTED_TARGET,
        MEASURED_DATA_BODY_BYTES_AT_SUPERSEDED_TARGET, MEASURED_FLIGHT_DATA_MESSAGES,
        MEASURED_FRAMING_ROWS, REJECTED_FLIGHT_DATA_SIZE_TARGET_BYTES,
    };

    assert_eq!(
        GRPC_DEFAULT_MAX_MESSAGE_BYTES,
        4 * 1024 * 1024,
        "the recorded interop ceiling is tonic's DEFAULT_MAX_RECV_MESSAGE_SIZE \
         (codec/mod.rs:100) and raw grpc-java's GrpcUtil.DEFAULT_MAX_MESSAGE_SIZE \
         (GrpcUtil.java:212); changing it re-opens the whole rejection"
    );
    // STRICT, and against the RESERVED ceiling: the body is not the whole message.
    assert!(
        FLIGHT_DATA_SIZE_TARGET_BYTES + FLIGHT_FRAMING_OVERHEAD_BYTES
            < GRPC_DEFAULT_MAX_MESSAGE_BYTES,
        "the shipped wire target ({FLIGHT_DATA_SIZE_TARGET_BYTES} B) plus the reserved \
         framing overhead ({FLIGHT_FRAMING_OVERHEAD_BYTES} B) must stay STRICTLY inside \
         the {GRPC_DEFAULT_MAX_MESSAGE_BYTES} B gRPC ceiling — a `<=` here is what \
         admitted the superseded {SUPERSEDED_FLIGHT_DATA_SIZE_TARGET_BYTES} B target"
    );
    assert!(
        SUPERSEDED_FLIGHT_DATA_SIZE_TARGET_BYTES + FLIGHT_FRAMING_OVERHEAD_BYTES
            > GRPC_DEFAULT_MAX_MESSAGE_BYTES,
        "the superseded target must remain recorded as one the strict guard forbids"
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
        MEASURED_DATA_BODY_BYTES_AT_SUPERSEDED_TARGET * 2 < GRPC_DEFAULT_MAX_MESSAGE_BYTES,
        "the measured body at the superseded target must be recorded as comfortably \
         inside the ceiling — which is exactly why a corpus MEAN could not carry the \
         wire-safety claim; the ratio-~1.0 shape is what the framing tests above assert"
    );

    // The recorded measurements must be mutually consistent at the recorded width.
    let total_payload = MEASURED_FRAMING_ROWS * MEASURED_ARROW_PAYLOAD_BYTES_PER_ROW;
    for (target, messages, recorded_body) in [
        (
            MEASURED_FLIGHT_DATA_MESSAGES[1].0,
            MEASURED_FLIGHT_DATA_MESSAGES[1].1,
            MEASURED_DATA_BODY_BYTES_AT_SUPERSEDED_TARGET,
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
/// being re-sliced) and **STRICTLY BELOW** the producer's own payload cap.
///
/// The `<=` this assertion used to carry was defeated by the value it admitted:
/// at equality a single `FlightData` message *does* carry a full-cap batch, which
/// is the very thing the message says must not happen. It is `<` now, and the
/// reserved-ceiling relation is asserted alongside it.
#[test]
fn the_target_sits_between_the_arrow_default_and_the_producer_payload_cap() {
    assert!(
        FLIGHT_DATA_SIZE_TARGET_BYTES > ARROW_FLIGHT_DEFAULT_TARGET,
        "the whole point of stating the target is that it is above arrow-flight's \
         {ARROW_FLIGHT_DEFAULT_TARGET}-byte default"
    );
    assert!(
        FLIGHT_DATA_SIZE_TARGET_BYTES < crate::batch_bytes::DEFAULT_MAX_BATCH_BYTES,
        "the target must be STRICTLY below the producer's payload cap ({} B): at \
         equality a single FlightData message carries a full-cap batch as its body \
         (measured 3.90 MB bodies at 8 MiB on the issue #3096 corpus), on the 4 MiB \
         gRPC message ceiling",
        crate::batch_bytes::DEFAULT_MAX_BATCH_BYTES
    );
    assert_eq!(
        FLIGHT_DATA_SIZE_TARGET_BYTES,
        GRPC_DEFAULT_MAX_MESSAGE_BYTES - 2 * FLIGHT_FRAMING_OVERHEAD_BYTES,
        "the target is DERIVED from the ceiling minus the framing reserve minus the \
         encoder's inexactness margin (both {FLIGHT_FRAMING_OVERHEAD_BYTES} B), not \
         chosen — if that derivation changes, the reserve's justification has to be \
         re-derived with it"
    );
    assert!(
        FLIGHT_DATA_RESERVED_CEILING_BYTES < GRPC_DEFAULT_MAX_MESSAGE_BYTES,
        "the reserved ceiling must sit strictly under the gRPC ceiling"
    );
}
