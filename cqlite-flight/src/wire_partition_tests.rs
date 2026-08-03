//! Unit tests for the wire-side byte partitioner (issue #3096 review).
//!
//! These drive [`super::frame_for_wire_bounded`] at SMALL explicit bounds so the
//! intervention path can be exercised on kilobyte fixtures. The end-to-end half —
//! the real `encode_do_get`, at the SHIPPED bounds, asserting the SERIALIZED
//! `FlightData` body of a width-SKEWED batch — lives in
//! `streaming_framing_tests.rs`; both are needed and neither substitutes for the
//! other.
//!
//! No wall-clock assertion appears here (#2642): every assertion is a byte size, a
//! row count, or a slice count.

use super::*;
use arrow::array::{ArrayRef, BinaryArray, Int64Array};
use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{Field, Schema as ArrowSchema};
use std::sync::Arc;

/// A one-column `Binary` batch with the EXACT per-row widths given, whose buffer
/// capacity equals its payload (`Buffer::from_vec` adopts the vector's
/// allocation), so nothing here depends on allocator rounding.
fn skewed_binary_batch(widths: &[usize]) -> RecordBatch {
    let total: usize = widths.iter().sum();
    let values = Buffer::from_vec(vec![0x5au8; total]);
    let mut offsets: Vec<i32> = Vec::with_capacity(widths.len() + 1);
    let mut acc = 0i32;
    offsets.push(0);
    for w in widths {
        acc += i32::try_from(*w).expect("width fits i32");
        offsets.push(acc);
    }
    let array = BinaryArray::try_new(OffsetBuffer::new(ScalarBuffer::from(offsets)), values, None)
        .expect("binary array");
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "b",
        arrow::datatypes::DataType::Binary,
        false,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(array) as ArrayRef]).expect("record batch")
}

fn int64_batch(n: usize) -> RecordBatch {
    let array = Int64Array::from((0..n as i64).collect::<Vec<i64>>());
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "v",
        arrow::datatypes::DataType::Int64,
        false,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(array) as ArrayRef]).expect("record batch")
}

/// The measurement the whole module rests on: a slice's payload must reflect the
/// slice, not the shared buffer behind it.
///
/// The contrast is the point — `get_buffer_memory_size()` (what arrow-flight sizes
/// with) reports the WHOLE batch for a one-row slice, which is exactly why the
/// encoder cannot size a sub-range and why this module measures instead.
#[test]
fn a_slice_is_measured_by_its_own_bytes_not_the_shared_buffer() {
    let batch = skewed_binary_batch(&[10, 10, 4_000]);
    let whole = slice_payload_bytes(&batch, 0, 3);
    let narrow_pair = slice_payload_bytes(&batch, 0, 2);
    let fat_row = slice_payload_bytes(&batch, 2, 1);

    assert!(
        narrow_pair < 100,
        "the two narrow rows must measure ~their own bytes, got {narrow_pair}"
    );
    assert!(
        (4_000..4_100).contains(&fat_row),
        "the fat row must measure ~4,000 B, got {fat_row}"
    );
    assert!(
        whole >= 4_020 && whole <= narrow_pair + fat_row + 8,
        "the whole batch must be ~the sum of its rows, got {whole}"
    );

    // The contrast: Arrow's capacity measure, on the SAME one-row slice.
    let sliced = batch.slice(2, 1);
    let capacity: usize = sliced
        .columns()
        .iter()
        .map(|c| c.get_buffer_memory_size())
        .sum();
    assert!(
        capacity >= whole,
        "get_buffer_memory_size() on a one-row slice reports the whole shared buffer \
         ({capacity} B) — the reason a capacity-denominated target cannot bound a slice"
    );
}

/// **The finding, at unit level.** A width-skewed batch whose bytes are
/// concentrated in the FIRST rows: the encoder's uniform-by-row-count split leaves
/// them together, and the partitioner separates them.
#[test]
fn width_skew_defeats_the_uniform_row_split_and_the_partitioner_fixes_it() {
    // 4 fat rows then 96 narrow ones. Total ~4,096 B + 960 B.
    let mut widths = vec![1_024usize; 4];
    widths.extend(std::iter::repeat_n(10usize, 96));
    let batch = skewed_binary_batch(&widths);
    let target = 2_048usize;

    // (1) What the encoder would do, replicated: capacity ~5 KB / 2,048 → 3 slices
    // of 33 rows, and slice 0 holds all four fat rows.
    let predicted = slice_payload_bytes(&batch, 0, 33);
    assert!(
        predicted > target,
        "the fixture must be skewed enough that the encoder's first uniform slice \
         ({predicted} B over 33 rows) exceeds the {target}-byte target — otherwise \
         this test proves nothing"
    );
    assert!(
        encoder_split_would_exceed(&batch, target),
        "the intervention predicate must fire on this fixture"
    );

    // (2) With the partitioner, EVERY slice is inside the target.
    let slices = frame_for_wire_bounded(batch.clone(), target, 1 << 20).expect("no oversized row");
    assert!(
        slices.len() > 1,
        "a skewed batch over the target must be split, got {} slice(s)",
        slices.len()
    );
    for (i, s) in slices.iter().enumerate() {
        let bytes = slice_payload_bytes(s, 0, s.num_rows());
        assert!(
            bytes <= target,
            "slice {i} is {bytes} B over {} rows, above the {target}-byte target",
            s.num_rows()
        );
    }
    // Nothing is lost or reordered.
    assert_eq!(
        slices.iter().map(|s| s.num_rows()).sum::<usize>(),
        batch.num_rows(),
        "the partition must cover every row exactly once"
    );
}

/// A uniform batch that the encoder already frames safely passes through
/// UNTOUCHED — one slice, same rows, no copy. This is what keeps the shipped
/// framing (and the exact message counts asserted in `streaming_framing_tests.rs`)
/// unchanged.
#[test]
fn a_batch_the_encoder_frames_safely_passes_through_untouched() {
    let batch = int64_batch(4_096); // 32 KB, uniform rows
    let slices = frame_for_wire_bounded(batch.clone(), 64 * 1024, 1 << 20).expect("safe");
    assert_eq!(slices.len(), 1, "no split was needed");
    assert_eq!(slices[0].num_rows(), batch.num_rows());

    // Even when the batch is larger than the target, a UNIFORM batch is left to the
    // encoder: its row-uniform split is already byte-uniform here.
    let big = int64_batch(65_536); // 512 KB
    let slices = frame_for_wire_bounded(big.clone(), 64 * 1024, 1 << 20).expect("safe");
    assert_eq!(
        slices.len(),
        1,
        "a uniform batch must be left for the encoder to slice, got {} slice(s)",
        slices.len()
    );
}

/// An individually oversized row is REJECTED fail-closed, not framed.
///
/// A single row is indivisible (`rows_per_batch` is `.max(1)`), so no framing makes
/// it legal; the error names the row, its size and the ceiling.
#[test]
fn a_single_row_over_the_ceiling_is_rejected_with_a_clear_error() {
    let ceiling = 4_096usize;
    let batch = skewed_binary_batch(&[10, 10, 8_192, 10]);
    let err = frame_for_wire_bounded(batch, 2_048, ceiling)
        .expect_err("a row over the ceiling must be rejected");
    assert_eq!(err.row, 2, "the error must name the offending row");
    assert!(
        err.bytes > ceiling,
        "the recorded size ({}) must be the reason it was rejected",
        err.bytes
    );
    assert_eq!(err.ceiling, ceiling);
    let msg = err.to_string();
    for expected in ["row 2", "cannot be split", "narrow the projection"] {
        assert!(
            msg.contains(expected),
            "the operator-facing message must contain {expected:?}, got: {msg}"
        );
    }
}

/// A row BETWEEN the target and the ceiling is legal on its own and gets its own
/// slice — it is neither rejected nor merged with a neighbour.
#[test]
fn a_row_between_the_target_and_the_ceiling_gets_its_own_slice() {
    let batch = skewed_binary_batch(&[10, 3_000, 10]);
    let slices = frame_for_wire_bounded(batch, 2_048, 8_192).expect("legal on its own");
    let rows: Vec<usize> = slices.iter().map(|s| s.num_rows()).collect();
    assert_eq!(
        rows,
        vec![1, 1, 1],
        "the fat row must be isolated (its neighbours cannot join it without \
         exceeding the target)"
    );
}

/// An empty batch is passed through, so the schema-only response shape is
/// untouched.
#[test]
fn an_empty_batch_is_passed_through() {
    let batch = int64_batch(0);
    let slices = frame_for_wire_bounded(batch, 1_024, 4_096).expect("empty is safe");
    assert_eq!(slices.len(), 1);
    assert_eq!(slices[0].num_rows(), 0);
}

/// A `List` column is measured on its SLICE's child range, not on the whole shared
/// child array — the over-report that would otherwise reject a perfectly legal
/// collection batch as an "oversized row".
#[test]
fn a_sliced_list_column_is_not_over_measured_by_its_whole_child() {
    use arrow::array::{Int64Builder, ListBuilder};
    let mut builder = ListBuilder::new(Int64Builder::new());
    for _ in 0..1_000 {
        for v in 0..64i64 {
            builder.values().append_value(v);
        }
        builder.append(true);
    }
    let list = builder.finish();
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "l",
        list.data_type().clone(),
        true,
    )]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(list) as ArrayRef]).expect("record batch");

    let whole = slice_payload_bytes(&batch, 0, 1_000);
    let one_row = slice_payload_bytes(&batch, 0, 1);
    assert!(
        one_row < whole / 100,
        "one row of a 1,000-row list column must measure ~1/1000 of the batch, not \
         the whole child array (one_row={one_row}, whole={whole})"
    );
    // ~64 x 8 B of values plus offsets/validity — the real per-row cost.
    assert!(
        (512..640).contains(&one_row),
        "expected ~512-640 B for a 64-element i64 list row, got {one_row}"
    );
}

// ---------------------------------------------------------------------------
// The ceiling guard measures the FULL SERIALIZED MESSAGE (issue #3096, second
// review of lever 4)
// ---------------------------------------------------------------------------
//
// `guard_message_within_ceiling` used to measure `data_body` alone and lean on
// `FLIGHT_FRAMING_OVERHEAD_BYTES` as a RESERVE for everything else, which made it
// conditional on that reserve while claiming to be unconditional. These tests pin
// the two message shapes a body-only check cannot see at all: a header-only
// message (body length 0) and non-body bytes (`app_metadata`) pushing an otherwise
// legal body over the limit.
//
// No wall-clock assertion appears here (#2642): every assertion is a byte size.

/// A `FlightData` with the EXACT field lengths given, so each test's arithmetic is
/// its own.
fn flight_data(header_len: usize, metadata_len: usize, body_len: usize) -> FlightData {
    FlightData {
        data_header: prost::bytes::Bytes::from(vec![0xa5u8; header_len]),
        app_metadata: prost::bytes::Bytes::from(vec![0x5au8; metadata_len]),
        data_body: prost::bytes::Bytes::from(vec![0x11u8; body_len]),
        ..Default::default()
    }
}

/// The measured quantity is the message, and it is STRICTLY larger than the body:
/// protobuf tags and length varints are bytes on the wire too.
#[test]
fn the_measured_size_is_the_whole_message_not_the_body() {
    let data = flight_data(4_096, 128, 65_536);
    let message = serialized_message_bytes(&data);
    let parts = data.data_header.len() + data.app_metadata.len() + data.data_body.len();
    assert!(
        message > parts,
        "the encoded message ({message} B) must exceed the sum of its payload fields \
         ({parts} B) by the field tags and length varints"
    );
    assert!(
        message < parts + 64,
        "…and by no more than a few bytes of framing, got {message} B over {parts} B"
    );
}

/// **A header-only message is MEASURED, not trivially passed.**
///
/// The schema `FlightData` (and every dictionary message) has an empty body, so the
/// old body-only check compared 0 against the ceiling and accepted it whatever the
/// `data_header` weighed. Non-vacuity is the body length itself: it is ZERO here,
/// which is what the superseded check would have measured.
#[test]
fn a_header_only_message_over_the_ceiling_is_rejected_rather_than_trivially_passing() {
    let data = flight_data(FLIGHT_DATA_RESERVED_CEILING_BYTES + 4_096, 0, 0);
    assert!(
        data.data_body.is_empty(),
        "NON-VACUITY: the body must be EMPTY, so the body-only check this replaces \
         measured 0 B and accepted this message"
    );
    let probe = StreamProbe::default();
    let status = guard_message_within_ceiling(data, &probe)
        .expect_err("a header over the ceiling must be refused");
    assert_eq!(status.code(), tonic::Code::OutOfRange, "code: {status:?}");
    let msg = status.message();
    assert!(
        msg.contains("data_body 0") && msg.contains("header-only"),
        "the refusal must show the empty body and name the header-only case, got: {msg}"
    );
}

/// **Non-body bytes count.** A body comfortably inside the ceiling plus
/// `app_metadata` that carries the message over it is refused.
///
/// Non-vacuity is asserted in the guard's own currency: the BODY alone is under the
/// ceiling, so the superseded body-only check accepted exactly this message.
#[test]
fn app_metadata_that_carries_the_message_over_the_ceiling_is_rejected() {
    let body = FLIGHT_DATA_RESERVED_CEILING_BYTES - 1_024;
    let data = flight_data(512, 8_192, body);
    assert!(
        data.data_body.len() <= FLIGHT_DATA_RESERVED_CEILING_BYTES,
        "NON-VACUITY: the body alone ({} B) must be legal, so only the non-body bytes \
         can be what makes this message illegal",
        data.data_body.len()
    );
    let message = serialized_message_bytes(&data);
    assert!(
        message > FLIGHT_DATA_RESERVED_CEILING_BYTES,
        "the fixture must serialize OVER the ceiling ({message} B vs \
         {FLIGHT_DATA_RESERVED_CEILING_BYTES} B)"
    );

    let probe = StreamProbe::default();
    let status = guard_message_within_ceiling(data, &probe)
        .expect_err("a message over the ceiling must be refused");
    assert_eq!(status.code(), tonic::Code::OutOfRange, "code: {status:?}");
    assert!(
        status.message().contains("app_metadata 8192"),
        "the refusal must account for the non-body bytes, got: {}",
        status.message()
    );
}

/// **No false rejection.** The shapes the production path actually emits — a body
/// at the encoder's target with a realistic header, and a small header-only schema
/// message — pass unchanged.
///
/// The 65,536-byte gap between the target and the ceiling is what makes the first
/// of these safe; this test is what would fail if the guard were tightened onto the
/// target instead.
#[test]
fn a_message_at_the_encoder_target_with_a_realistic_header_still_passes() {
    let probe = StreamProbe::default();
    for (header, metadata, body) in [
        (1_024usize, 0usize, FLIGHT_DATA_SIZE_TARGET_BYTES),
        (512, 0, 0),
        (0, 0, 0),
    ] {
        let data = flight_data(header, metadata, body);
        let message = serialized_message_bytes(&data);
        let passed = guard_message_within_ceiling(data, &probe)
            .expect("a message inside the ceiling must pass");
        assert_eq!(
            serialized_message_bytes(&passed),
            message,
            "the guard must return the message unchanged"
        );
        assert!(
            message <= FLIGHT_DATA_RESERVED_CEILING_BYTES,
            "header {header} B + body {body} B serializes to {message} B, which must \
             stay inside the {FLIGHT_DATA_RESERVED_CEILING_BYTES}-byte ceiling — the \
             {}-byte gap from the target is what reserves room for the header",
            FLIGHT_DATA_RESERVED_CEILING_BYTES - FLIGHT_DATA_SIZE_TARGET_BYTES
        );
    }
}
