//! Wire-side **byte** partitioning: what makes the framing target a BOUND rather
//! than a hope (issue #3096, lever 4 — added in that lever's second review).
//!
//! # The hole this closes
//!
//! [`crate::flight_data_size::FLIGHT_DATA_SIZE_TARGET_BYTES`] is handed to
//! `FlightDataEncoderBuilder::with_max_flight_data_size`, and that is a **target,
//! not a cap**. Read what arrow-flight 53.4.1 actually does with it
//! (`arrow-flight-53.4.1/src/encode.rs:613-637`, `split_batch_for_grpc_response`,
//! quoted verbatim):
//!
//! ```text
//! let size = batch.columns().iter().map(|col| col.get_buffer_memory_size()).sum::<usize>();
//! let n_batches =
//!     (size / max_flight_data_size + usize::from(size % max_flight_data_size != 0)).max(1);
//! let rows_per_batch = (batch.num_rows() / n_batches).max(1);
//! // ... then: out.push(batch.slice(offset, rows_per_batch.min(num_rows - offset)))
//! ```
//!
//! Three properties follow, and the third is a wire-safety defect:
//!
//! 1. the size it divides by is the batch's total buffer **CAPACITY**, summed once
//!    over the WHOLE batch;
//! 2. the split is **UNIFORM BY ROW COUNT** — `rows_per_batch` is the same for
//!    every slice; and therefore
//! 3. **a batch whose rows differ in WIDTH can put far more than
//!    `size / n_batches` bytes into one slice.** In the limit, all of a batch's
//!    bytes can land in slice 0.
//!
//! So at a raised `--max-batch-bytes` a width-skewed table can still frame a
//! single `FlightData` message **over the 4 MiB gRPC ceiling**, no matter what the
//! target is set to. `FLIGHT_DATA_SIZE_INEXACTNESS_MARGIN_BYTES`'s doc already
//! STATED this residual ("a caller that raises `--max-batch-bytes` far above the
//! default and feeds it width-skewed rows can still frame a slice above this
//! ceiling, and no value of the target prevents that") — and the framing tests
//! could not see it, because every fixture they used had UNIFORM row widths.
//!
//! Lever 4 delivers **zero** measured throughput (see the constant's own doc); its
//! sole remaining justification is wire safety. A hole in that safety therefore
//! removes its entire justification, which is why this is fixed rather than
//! documented.
//!
//! # The fix: partition on BYTES, then verify the SERIALIZED bytes
//!
//! Two mechanisms, in series, because neither alone is sufficient:
//!
//! * [`partition_for_wire`] cuts each batch on **measured payload bytes plus this
//!   batch's measured per-message framing cost** before the encoder sees it, so no
//!   slice reaching the encoder can be serialized into a message over the ceiling.
//!   Any further row-uniform split the encoder applies to those slices can only
//!   make bodies SMALLER, so the bound survives it.
//! * [`guard_message_within_ceiling`] checks every emitted `FlightData`'s
//!   **actually serialized SIZE — the whole message, not just its body** — against
//!   [`FLIGHT_DATA_RESERVED_CEILING_BYTES`] and fails the stream closed if one is
//!   over it. It is measured on the real encoded bytes, so it holds even where the
//!   pre-encode estimate does not (a single row wider than the ceiling, or an
//!   Arrow layout this module measures conservatively), and it does not depend on
//!   [`crate::flight_data_size::FLIGHT_FRAMING_OVERHEAD_BYTES`] being an accurate
//!   reserve for the non-body bytes, because those bytes are ON the scale.
//!
//! An individually oversized row — one whose own payload exceeds the reserved
//! ceiling — cannot be split at all (`rows_per_batch` is `.max(1)`, and one row is
//! indivisible). It is **rejected fail-closed** with a message naming the row, its
//! size, the ceiling and the remedy, instead of emitting a message the client will
//! reject with an opaque `Failed to read message`.
//!
//! # Why this is not a throughput regression
//!
//! [`partition_for_wire`] is a **no-op unless the encoder's own split would be
//! unsafe**: it first predicts the encoder's row-uniform slices and measures them,
//! and passes the batch through UNTOUCHED (one `Arc` clone, no data copy) when
//! every predicted slice fits. The default path — `DEFAULT_MAX_BATCH_BYTES` over
//! any table whose rows are of similar width — takes that branch, which is why the
//! framing tests' exact message counts are unchanged.
//!
//! The prediction duplicates arrow-flight's private formula, which is coupling we
//! accept knowingly: it is pinned by `Cargo.lock`, it is asserted against the real
//! encoder in `streaming_framing_tests.rs` (exact message counts), and if the
//! dependency ever changes it the only consequence is that we under-intervene —
//! [`guard_message_within_ceiling`] still refuses to put an illegal message on the
//! wire.
//!
//! # The partitioner budgets the SAME quantity the guard measures (third review)
//!
//! The first version of this module budgeted the **Arrow payload alone** against
//! the target while the guard measured the **whole serialized message**. Those are
//! two different quantities, so on a wide-enough schema the two disagreed in the
//! dangerous direction: `data_header` is a `RecordBatch` flatbuffer of ~16 B per
//! Arrow buffer plus ~16 B per field node, so a few thousand columns carry a
//! header of a hundred kilobytes or more — past the
//! [`crate::flight_data_size::FLIGHT_DATA_SIZE_INEXACTNESS_MARGIN_BYTES`] gap
//! between the target and the ceiling. A batch whose payload sat just under the
//! target was therefore passed through as legal by the partitioner and then
//! **REJECTED by the guard**, failing the whole stream closed even though smaller
//! slices would have been perfectly legal. A weak guard had been turned into a hard
//! stream failure.
//!
//! [`per_message_overhead_bytes`] closes that. It MEASURES this batch's per-message
//! non-payload cost — the real IPC header (arrow's own generator, over a one-row
//! probe), the per-buffer body ALIGNMENT PADDING, and protobuf field framing — and
//! the bounds become:
//!
//! * `payload_ceiling = ceiling - overhead`: what a slice's payload may be and still
//!   serialize inside the ceiling, which is also the bound an indivisible row is
//!   rejected against; and
//! * `payload_target = min(target, payload_ceiling)`: the partition budget. The
//!   `min` is what makes this a TIGHTENING ONLY — for every shape in this tree's
//!   corpora the overhead is a few kilobytes against a ~3.9 MB target, the target is
//!   the smaller of the two, and the shipped framing (including the exact message
//!   counts `streaming_framing_tests.rs` pins) is unchanged.
//!
//! The padding term is not a rounding detail: a 2,048-column batch has 4,096 IPC
//! buffers, each padded up to [`IPC_BUFFER_ALIGNMENT_BYTES`], so its body can exceed
//! the measured payload by ~256 KB — measured at 655,385 B of body over 450,560 B of
//! payload on the wide fixture in `wire_partition_tests.rs`. Budgeting the header
//! alone would have left that hole open.

use arrow::array::{Array, LargeListArray, ListArray, MapArray, StructArray};
use arrow::datatypes::DataType;
// Arrow's OWN IPC message generator, so this module measures the `data_header` the
// encoder will really emit for a batch instead of modelling it.
use arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use arrow::record_batch::RecordBatch;
use arrow_flight::error::FlightError;
use arrow_flight::FlightData;
use futures::{Stream, StreamExt};
// `FlightData`'s own serializer, so the ceiling guard measures the bytes tonic will
// actually put on the wire rather than a model of them.
use prost::Message as _;
use tonic::Status;

use crate::flight_data_size::{
    FLIGHT_DATA_RESERVED_CEILING_BYTES, FLIGHT_DATA_SIZE_TARGET_BYTES,
    FLIGHT_FRAMING_OVERHEAD_BYTES,
};
use crate::streaming::{record_encoder_error, StreamProbe};

/// Upper bound on the protobuf FIELD FRAMING of one `FlightData` message — the
/// bytes outside `data_header`/`app_metadata`/`data_body` themselves.
///
/// Derived, not guessed: `FlightData` has four fields, each a `bytes`/message field
/// costing a tag (at most 2 B for these field numbers) plus a length varint (at
/// most 5 B, since a length under 4 GiB encodes in five groups of seven bits) —
/// `4 * (2 + 5) = 28` B. 64 B is that bound rounded up, and it is only ever ADDED
/// to a reserve, so over-stating it can cost a little extra splitting and can never
/// admit an illegal message. Cross-checked by
/// `wire_partition_tests::the_measured_size_is_the_whole_message_not_the_body`,
/// which asserts a real message exceeds the sum of its payload fields by less than
/// this.
const FLIGHT_DATA_PROTOBUF_FRAMING_BYTES: usize = 64;

/// Alignment arrow-rs pads every IPC body buffer to (`IpcWriteOptions::default()`).
///
/// The IPC body is not the concatenation of the buffer LENGTHS this module measures:
/// each buffer is written padded up to this alignment, so a body can exceed the
/// measured payload by up to `alignment - 1` bytes PER BUFFER. On a narrow schema
/// that is tens of bytes; on a few thousand buffers it is hundreds of kilobytes,
/// which is why it is budgeted rather than left to the inexactness margin.
///
/// The value is arrow's, not ours, so it is cross-checked against arrow's real
/// encoder output by
/// `wire_partition_tests::the_body_bound_holds_and_a_smaller_alignment_would_under_budget`
/// — which also shows an 8-byte assumption UNDER-budgets a real body.
const IPC_BUFFER_ALIGNMENT_BYTES: usize = 64;

/// A single row whose own Arrow payload exceeds the largest legal `data_body`.
///
/// Indivisible by construction, so there is no framing that makes it legal — the
/// stream fails closed with this instead of emitting a message a default tonic or
/// grpc-java client refuses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RowTooWide {
    /// Row index WITHIN the batch (not the scan).
    pub(crate) row: usize,
    /// Measured Arrow payload bytes for that one row.
    pub(crate) bytes: usize,
    /// The bound it exceeded: the largest legal serialized message LESS this
    /// batch's measured per-message framing cost
    /// ([`per_message_overhead_bytes`]) — i.e. the payload a single-row message
    /// can actually carry, which is strictly less than the message ceiling.
    pub(crate) ceiling: usize,
}

impl std::fmt::Display for RowTooWide {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "row {} of this batch is {} bytes of Arrow payload on its own, over the \
             {}-byte maximum FlightData payload (the 4 MiB default gRPC message limit less \
             the reserved IPC/protobuf framing overhead, less this batch's own measured \
             IPC data_header). A single row cannot be split \
             across FlightData messages, so there is no framing that makes it legal: \
             narrow the projection to drop the oversized column, or raise the client's \
             max_decoding_message_size and the server's framing reserve together",
            self.row, self.bytes, self.ceiling
        )
    }
}

/// Byte-partition every batch of `batch_stream` before it reaches the arrow-flight
/// encoder (issue #3096 review).
///
/// A batch the encoder would frame safely on its own passes through untouched. One
/// it would not is cut on measured bytes; a row too wide to frame at all fails the
/// stream closed, recorded through the shared encoder-error hook so the failure is
/// visible server-side rather than surfacing only as the client's read error.
pub(crate) fn partition_for_wire(
    batch_stream: impl Stream<Item = Result<RecordBatch, FlightError>> + Send + 'static,
    probe: StreamProbe,
) -> impl Stream<Item = Result<RecordBatch, FlightError>> + Send + 'static {
    batch_stream.flat_map(move |res| {
        let items: Vec<Result<RecordBatch, FlightError>> = match res {
            Ok(batch) => match frame_for_wire(batch) {
                Ok(slices) => slices.into_iter().map(Ok).collect(),
                Err(too_wide) => {
                    // Recorded HERE (log + error signal + probe) and then carried as
                    // an `ExternalError(Status)`, which is the encoder-input contract
                    // for "already observed upstream" — `flight_error_to_status`
                    // recovers it without double-counting.
                    let status =
                        record_encoder_error(Status::out_of_range(too_wide.to_string()), &probe);
                    vec![Err(FlightError::ExternalError(Box::new(status)))]
                }
            },
            Err(e) => vec![Err(e)],
        };
        futures::stream::iter(items)
    })
}

/// Refuse to put a `FlightData` whose **FULL SERIALIZED SIZE** is over
/// [`FLIGHT_DATA_RESERVED_CEILING_BYTES`] on the wire (issue #3096, second
/// review).
///
/// # What is measured
///
/// The protobuf-encoded length of the WHOLE message —
/// [`serialized_message_bytes`]: `data_body`, `data_header`, `app_metadata`, any
/// `flight_descriptor`, and every field tag and length varint. That is the
/// quantity a peer's limit applies to: tonic compares its
/// `max_decoding_message_size` against the length in the gRPC length-prefixed
/// frame, and grpc-java's `maxInboundMessageSize` does the same. The only bytes
/// outside this number are that 5-byte frame prefix itself, four orders of
/// magnitude inside the 65,536-byte gap between this ceiling and the raw 4 MiB
/// limit.
///
/// # Why it is not `data_body` alone
///
/// It was, and that made the check **conditional on a reserve** while its own doc
/// called it unconditional: the non-body bytes were assumed to fit inside
/// [`crate::flight_data_size::FLIGHT_FRAMING_OVERHEAD_BYTES`] (plus a second,
/// equal inexactness margin), so what the guard actually proved was
/// "body ≤ ceiling", not "message ≤ gRPC limit". Those two constants keep their
/// other job — they are how
/// [`crate::flight_data_size::FLIGHT_DATA_SIZE_TARGET_BYTES`] is derived, which is
/// what leaves the pre-encode partition room for a header — but this guard no
/// longer depends on either being an accurate estimate of anything: whatever the
/// header, metadata and framing really cost, they are ON the scale here.
///
/// A **header-only** message — the schema `FlightData` the encoder emits first,
/// and any dictionary message — carries an empty body and so passed the old
/// body-only check trivially, no matter how large its `data_header` flatbuffer
/// was; nothing else on this path bounds a wide-enough schema's header. It is now
/// measured like every other message.
// The `Err` is `tonic::Status` because this function's output IS the `DoGetStream`
// item type mandated by the arrow-flight `FlightService` trait; boxing it (clippy's
// suggestion) would violate that contract — same rationale as
// `streaming::encode_do_get` (#2856).
#[allow(clippy::result_large_err)]
pub(crate) fn guard_message_within_ceiling(
    data: FlightData,
    probe: &StreamProbe,
) -> Result<FlightData, Status> {
    let message = serialized_message_bytes(&data);
    if message > FLIGHT_DATA_RESERVED_CEILING_BYTES {
        let body = data.data_body.len();
        let header = data.data_header.len();
        let metadata = data.app_metadata.len();
        return Err(record_encoder_error(
            Status::out_of_range(format!(
                "refusing to send a FlightData that serializes to {message} bytes \
                 (data_body {body} + data_header {header} + app_metadata {metadata}, plus \
                 protobuf field framing): the largest legal message is \
                 {FLIGHT_DATA_RESERVED_CEILING_BYTES} bytes (the 4 MiB default gRPC message \
                 limit less the reserved IPC/protobuf framing overhead), and a default tonic \
                 or grpc-java client refuses the message rather than reporting this. The \
                 encoder's max_flight_data_size governs the BODY only and is a target met by \
                 slicing uniformly by ROW COUNT, so a width-skewed batch — or a header-only \
                 message over a very wide schema — can exceed it",
            )),
            probe,
        ));
    }
    Ok(data)
}

/// The exact number of bytes `data` occupies as a gRPC message payload.
///
/// `prost::Message::encoded_len` is the serializer's own length calculation, so
/// this is the realized size, not a model of it — and it is what the guard above
/// compares, which is why that guard no longer depends on a framing reserve. Cheap
/// enough to call per message: the `bytes` fields report their own lengths and the
/// rest is a handful of varint width computations.
///
/// Exposed to the tests so the wire-safety assertions can be made in the same
/// currency the guard enforces.
pub(crate) fn serialized_message_bytes(data: &FlightData) -> usize {
    data.encoded_len()
}

/// [`partition_for_wire`]'s per-batch decision, at the shipped bounds.
fn frame_for_wire(batch: RecordBatch) -> Result<Vec<RecordBatch>, RowTooWide> {
    frame_for_wire_bounded(
        batch,
        FLIGHT_DATA_SIZE_TARGET_BYTES,
        FLIGHT_DATA_RESERVED_CEILING_BYTES,
    )
}

/// [`frame_for_wire`] with explicit MESSAGE bounds, so tests can drive the
/// intervention path on small fixtures instead of allocating multi-megabyte
/// batches.
///
/// `target` and `ceiling` are stated in the currency
/// [`guard_message_within_ceiling`] enforces — the FULL serialized message — and
/// this function converts them into payload budgets by subtracting the batch's own
/// [`per_message_overhead_bytes`]. That subtraction is the fix of the third #3096
/// review (see the module header): budgeting the payload against a message bound
/// let a wide-schema batch be passed through here and then refused by the guard.
pub(crate) fn frame_for_wire_bounded(
    batch: RecordBatch,
    target: usize,
    ceiling: usize,
) -> Result<Vec<RecordBatch>, RowTooWide> {
    if batch.num_rows() == 0 {
        return Ok(vec![batch]);
    }
    let overhead = per_message_overhead_bytes(&batch);
    // What a slice's payload may be and still serialize inside the ceiling.
    let payload_ceiling = ceiling.saturating_sub(overhead);
    // The partition budget stays the encoder's target — the shipped contract, and
    // what keeps the framing tests' exact message counts intact — EXCEPT where that
    // target would not leave room for this batch's own per-message cost, which is the
    // case the third review found. `min` is what makes the fix a tightening only: for
    // every shape in this tree's corpora the target is the smaller of the two and
    // nothing changes.
    let payload_target = target.min(payload_ceiling);
    frame_for_wire_payload_budget(batch, target, payload_target, payload_ceiling)
}

/// The partition decision in PAYLOAD currency.
///
/// `encoder_target` is what arrow-flight's own builder is configured with (it is
/// what decides how many row-uniform slices the encoder will cut), while
/// `payload_target`/`payload_ceiling` are what each resulting slice must fit. The
/// two are DIFFERENT numbers by design: the encoder is configured once with the
/// shipped constant and knows nothing about a given batch's header, so its split
/// must be predicted at its own target and then measured against ours.
///
/// Exposed to the tests with the two currencies separated so the pre-fix behavior
/// (payload budgeted against the message bound, i.e. `payload_target == ceiling`'s
/// sibling with no overhead subtracted) can be reproduced and shown to be unsafe.
pub(crate) fn frame_for_wire_payload_budget(
    batch: RecordBatch,
    encoder_target: usize,
    payload_target: usize,
    payload_ceiling: usize,
) -> Result<Vec<RecordBatch>, RowTooWide> {
    if batch.num_rows() == 0 || !encoder_split_would_exceed(&batch, encoder_target, payload_target)
    {
        return Ok(vec![batch]);
    }
    byte_partition(batch, payload_target, payload_ceiling)
}

/// This batch's per-message NON-PAYLOAD cost — everything a serialized
/// `FlightData` carries beyond the Arrow buffer bytes [`slice_payload_bytes`]
/// measures. Three components, none of them a guess:
///
/// 1. **The IPC `data_header`**, MEASURED with arrow's own [`IpcDataGenerator`] —
///    the same code path the flight encoder uses — over a ONE-ROW slice. The
///    `RecordBatch` header flatbuffer holds a fixed-size `FieldNode` (16 B) per
///    array node and a fixed-size `Buffer` (16 B) per buffer, in fixed-width
///    vectors, so its SIZE follows the schema shape and not the row count: a
///    one-row probe measures the same header the full batch (and every slice of
///    it) will carry. Asserted, not assumed —
///    `wire_partition_tests::a_one_row_probe_measures_the_same_header_as_the_whole_batch`.
/// 2. **Body alignment padding**, bounded at
///    `(IPC_BUFFER_ALIGNMENT_BYTES - 1)` per IPC buffer ([`ipc_buffer_count`]).
///    This is the term a header-only reserve missed: a 2,048-column batch has
///    4,096 buffers, so its body can exceed the measured payload by ~256 KB.
/// 3. **Protobuf field framing**, [`FLIGHT_DATA_PROTOBUF_FRAMING_BYTES`].
///
/// Cost: one IPC encode of a single row plus a walk of the schema, per batch —
/// O(schema width), independent of the row count, and negligible against the
/// megabytes of payload a batch carries.
///
/// If arrow declines to encode the probe (a layout its IPC writer rejects), the
/// header term falls back to [`FLIGHT_FRAMING_OVERHEAD_BYTES`], the module's
/// documented upper bound on non-body bytes. A larger reserve can only cause extra
/// splitting, which is the safe direction.
fn per_message_overhead_bytes(batch: &RecordBatch) -> usize {
    let header = match ipc_header_bytes(&batch.slice(0, 1.min(batch.num_rows()))) {
        Some(bytes) => bytes,
        None => FLIGHT_FRAMING_OVERHEAD_BYTES,
    };
    let buffers: usize = batch
        .columns()
        .iter()
        .map(|c| ipc_buffer_count(&c.to_data()))
        .sum();
    let padding = buffers.saturating_mul(IPC_BUFFER_ALIGNMENT_BYTES.saturating_sub(1));
    header
        .saturating_add(padding)
        .saturating_add(FLIGHT_DATA_PROTOBUF_FRAMING_BYTES)
}

/// The number of buffers arrow's IPC writer emits for one array node and its
/// children: the validity bitmap entry it writes for EVERY node, plus the node's own
/// buffers, recursively.
///
/// Over-counting is the safe direction and is deliberate for a `Dictionary` array,
/// whose values travel in a separate dictionary message rather than in this batch's
/// body: counting them here only enlarges the reserve.
fn ipc_buffer_count(data: &arrow::array::ArrayData) -> usize {
    1 + data.buffers().len()
        + data
            .child_data()
            .iter()
            .map(ipc_buffer_count)
            .sum::<usize>()
}

/// The exact `data_header` length arrow's IPC generator produces for `batch`, or
/// `None` if it declines to encode it.
///
/// A fresh [`DictionaryTracker`] per call (`error_on_replacement = false`) keeps
/// this a pure measurement with no state shared with the real encoder; any
/// dictionary batches it would emit are separate `FlightData` messages, guarded on
/// their own by [`guard_message_within_ceiling`], and are deliberately not folded
/// into this batch's reserve.
fn ipc_header_bytes(batch: &RecordBatch) -> Option<usize> {
    let generator = IpcDataGenerator::default();
    let mut dictionaries = DictionaryTracker::new(false);
    let options = IpcWriteOptions::default();
    match generator.encoded_batch(batch, &mut dictionaries, &options) {
        Ok((_dictionaries, encoded)) => Some(encoded.ipc_message.len()),
        Err(_) => None,
    }
}

/// Would the encoder's OWN row-uniform split put more than `payload_target` payload
/// bytes into some slice? (arrow-flight 53.4.1 `split_batch_for_grpc_response`,
/// replicated — see this module's header for the quoted source.)
///
/// The number of slices is predicted at the encoder's OWN `encoder_target`, because
/// that is the value it is configured with; each predicted slice is then measured
/// against `payload_target`, the budget that leaves room for this batch's header.
///
/// Short-circuits on the first violation, and on the whole-batch check first: if
/// the entire batch fits the budget, no slice of it can fail.
fn encoder_split_would_exceed(
    batch: &RecordBatch,
    encoder_target: usize,
    payload_target: usize,
) -> bool {
    let rows = batch.num_rows();
    if rows == 0 {
        return false;
    }
    if slice_payload_bytes(batch, 0, rows) <= payload_target {
        return false;
    }
    // `max(1)` mirrors the encoder AND makes the division safe for a nonsense
    // target supplied by a test.
    let encoder_target = encoder_target.max(1);
    let capacity: usize = batch
        .columns()
        .iter()
        .map(|c| c.get_buffer_memory_size())
        .sum();
    let n_batches =
        (capacity / encoder_target + usize::from(capacity % encoder_target != 0)).max(1);
    let rows_per_batch = (rows / n_batches).max(1);
    let mut offset = 0;
    while offset < rows {
        let length = rows_per_batch.min(rows - offset);
        if slice_payload_bytes(batch, offset, length) > payload_target {
            return true;
        }
        offset += length;
    }
    false
}

/// Cut `batch` into contiguous slices whose measured payload is at most `target`,
/// rejecting any single row over `ceiling`.
///
/// The row boundary for each slice is found by binary search over the slice
/// length — [`slice_payload_bytes`] is monotonically non-decreasing in the length,
/// so this is O(log rows) measurements per slice instead of one per row.
fn byte_partition(
    batch: RecordBatch,
    target: usize,
    ceiling: usize,
) -> Result<Vec<RecordBatch>, RowTooWide> {
    let rows = batch.num_rows();
    let mut out = Vec::new();
    let mut offset = 0;
    while offset < rows {
        let remaining = rows - offset;
        let one_row = slice_payload_bytes(&batch, offset, 1);
        if one_row > ceiling {
            return Err(RowTooWide {
                row: offset,
                bytes: one_row,
                ceiling,
            });
        }
        let len = if one_row > target {
            // Between the target and the ceiling: legal as its own message, and
            // indivisible anyway.
            1
        } else if slice_payload_bytes(&batch, offset, remaining) <= target {
            remaining
        } else {
            // Invariant: payload(lo) <= target < payload(hi).
            let mut lo = 1usize;
            let mut hi = remaining;
            while lo + 1 < hi {
                let mid = lo + (hi - lo) / 2;
                if slice_payload_bytes(&batch, offset, mid) <= target {
                    lo = mid;
                } else {
                    hi = mid;
                }
            }
            lo
        };
        out.push(batch.slice(offset, len));
        offset += len;
    }
    Ok(out)
}

/// Arrow **payload** bytes (buffer lengths, not capacities) of rows
/// `[offset, offset + len)` of `batch`.
///
/// This is the currency an IPC `data_body` is denominated in, and the reason the
/// encoder's own capacity-denominated measure cannot be reused: slicing an Arrow
/// array is zero-copy, so a slice's `get_buffer_memory_size()` still reports the
/// WHOLE shared buffer's capacity and is useless for sizing a sub-range.
pub(crate) fn slice_payload_bytes(batch: &RecordBatch, offset: usize, len: usize) -> usize {
    batch
        .columns()
        .iter()
        .map(|c| array_payload_bytes(c.slice(offset, len).as_ref()))
        .sum()
}

/// Payload bytes of an already-sliced array.
///
/// The container arms slice their CHILD data themselves, because Arrow does not:
/// `ListArray::slice`/`MapArray::slice` clone the values/entries array whole and
/// only slice the offsets, so `ArrayData::get_slice_memory_size()` — which recurses
/// into `child_data` as-is — over-reports a sliced list by the entire child. (For
/// `StructArray::slice` Arrow DOES slice the fields, so struct children need no
/// special handling beyond recursion.)
///
/// Anything else falls back to `get_slice_memory_size()`, which is exact for the
/// leaf layouts CQLite emits (primitives, `Boolean`, `Utf8`/`Binary`,
/// `FixedSizeBinary`, `Decimal128`, `Timestamp`/`Date32`/`Time64`) and
/// conservative — never an under-estimate — for anything else. Conservative is the
/// safe direction here: it can only cause extra splitting, never an over-ceiling
/// message.
fn array_payload_bytes(array: &dyn Array) -> usize {
    let len = array.len();
    if len == 0 {
        return 0;
    }
    match array.data_type() {
        DataType::List(_) => match array.as_any().downcast_ref::<ListArray>() {
            Some(list) => {
                let offsets = list.value_offsets();
                match (offsets.first(), offsets.get(len)) {
                    (Some(&start), Some(&end)) => {
                        let start = usize::try_from(start).unwrap_or(0);
                        let end = usize::try_from(end).unwrap_or(0);
                        let child = list.values().slice(start, end.saturating_sub(start));
                        validity_bytes(array)
                            + (len + 1) * size_of::<i32>()
                            + array_payload_bytes(child.as_ref())
                    }
                    _ => conservative_bytes(array),
                }
            }
            None => conservative_bytes(array),
        },
        DataType::LargeList(_) => match array.as_any().downcast_ref::<LargeListArray>() {
            Some(list) => {
                let offsets = list.value_offsets();
                match (offsets.first(), offsets.get(len)) {
                    (Some(&start), Some(&end)) => {
                        let start = usize::try_from(start).unwrap_or(0);
                        let end = usize::try_from(end).unwrap_or(0);
                        let child = list.values().slice(start, end.saturating_sub(start));
                        validity_bytes(array)
                            + (len + 1) * size_of::<i64>()
                            + array_payload_bytes(child.as_ref())
                    }
                    _ => conservative_bytes(array),
                }
            }
            None => conservative_bytes(array),
        },
        DataType::Map(_, _) => match array.as_any().downcast_ref::<MapArray>() {
            Some(map) => {
                let offsets = map.value_offsets();
                match (offsets.first(), offsets.get(len)) {
                    (Some(&start), Some(&end)) => {
                        let start = usize::try_from(start).unwrap_or(0);
                        let end = usize::try_from(end).unwrap_or(0);
                        let entries = map.entries().slice(start, end.saturating_sub(start));
                        validity_bytes(array)
                            + (len + 1) * size_of::<i32>()
                            + array_payload_bytes(&entries)
                    }
                    _ => conservative_bytes(array),
                }
            }
            None => conservative_bytes(array),
        },
        DataType::Struct(_) => match array.as_any().downcast_ref::<StructArray>() {
            // `StructArray::slice` slices its fields, so the children are already
            // the right range.
            Some(s) => {
                validity_bytes(array)
                    + s.columns()
                        .iter()
                        .map(|c| array_payload_bytes(c.as_ref()))
                        .sum::<usize>()
            }
            None => conservative_bytes(array),
        },
        _ => conservative_bytes(array),
    }
}

/// Validity bitmap bytes for an array that has one (the container arms add this
/// themselves; `get_slice_memory_size()` already includes it for leaves).
fn validity_bytes(array: &dyn Array) -> usize {
    if array.nulls().is_some() {
        array.len().div_ceil(8)
    } else {
        0
    }
}

/// Arrow's own slice-scoped size, falling back to whole-buffer capacity for a
/// layout it declines to size (`NotYetImplemented`). Both are upper bounds on the
/// realized body for the layouts that reach here, and an upper bound is the safe
/// direction.
fn conservative_bytes(array: &dyn Array) -> usize {
    let data = array.to_data();
    data.get_slice_memory_size()
        .unwrap_or_else(|_| data.get_buffer_memory_size())
}

#[cfg(test)]
#[path = "wire_partition_tests.rs"]
mod wire_partition_tests;
