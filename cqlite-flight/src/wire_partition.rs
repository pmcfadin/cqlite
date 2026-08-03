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
//! * [`partition_for_wire`] cuts each batch on **measured payload bytes** before
//!   the encoder sees it, so no slice reaching the encoder exceeds the target.
//!   Any further row-uniform split the encoder applies to those slices can only
//!   make bodies SMALLER, so the bound survives it.
//! * [`guard_body_within_ceiling`] checks every emitted `FlightData`'s
//!   **actually serialized** `data_body` against
//!   [`FLIGHT_DATA_RESERVED_CEILING_BYTES`] and fails the stream closed if one is
//!   over it. This is the unconditional guarantee: it is measured on the real
//!   bytes, so it holds even where the pre-encode estimate does not (a single row
//!   wider than the ceiling, or an Arrow layout this module measures
//!   conservatively).
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
//! [`guard_body_within_ceiling`] still refuses to put an illegal message on the
//! wire.

use arrow::array::{Array, LargeListArray, ListArray, MapArray, StructArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use arrow_flight::error::FlightError;
use arrow_flight::FlightData;
use futures::{Stream, StreamExt};
use tonic::Status;

use crate::flight_data_size::{FLIGHT_DATA_RESERVED_CEILING_BYTES, FLIGHT_DATA_SIZE_TARGET_BYTES};
use crate::streaming::{record_encoder_error, StreamProbe};

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
    /// The bound it exceeded.
    pub(crate) ceiling: usize,
}

impl std::fmt::Display for RowTooWide {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "row {} of this batch is {} bytes of Arrow payload on its own, over the \
             {}-byte maximum FlightData body (the 4 MiB default gRPC message limit less \
             the reserved IPC/protobuf framing overhead). A single row cannot be split \
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

/// Refuse to put a `FlightData` whose SERIALIZED body is over
/// [`FLIGHT_DATA_RESERVED_CEILING_BYTES`] on the wire (issue #3096 review).
///
/// The unconditional half of the guarantee: it measures the encoded body, not an
/// estimate of it, so it holds regardless of what the encoder did with the target
/// or how conservatively [`slice_payload_bytes`] measured the input.
// The `Err` is `tonic::Status` because this function's output IS the `DoGetStream`
// item type mandated by the arrow-flight `FlightService` trait; boxing it (clippy's
// suggestion) would violate that contract — same rationale as
// `streaming::encode_do_get` (#2856).
#[allow(clippy::result_large_err)]
pub(crate) fn guard_body_within_ceiling(
    data: FlightData,
    probe: &StreamProbe,
) -> Result<FlightData, Status> {
    let body = data.data_body.len();
    if body > FLIGHT_DATA_RESERVED_CEILING_BYTES {
        return Err(record_encoder_error(
            Status::out_of_range(format!(
                "refusing to send a FlightData with a {body}-byte data_body: the largest \
                 legal body is {FLIGHT_DATA_RESERVED_CEILING_BYTES} bytes (the 4 MiB default \
                 gRPC message limit less the reserved IPC/protobuf framing overhead), and a \
                 default tonic or grpc-java client refuses the message rather than reporting \
                 this. The encoder's max_flight_data_size is a target met by slicing \
                 uniformly by ROW COUNT, so a width-skewed batch can exceed it",
            )),
            probe,
        ));
    }
    Ok(data)
}

/// [`partition_for_wire`]'s per-batch decision, at the shipped bounds.
fn frame_for_wire(batch: RecordBatch) -> Result<Vec<RecordBatch>, RowTooWide> {
    frame_for_wire_bounded(
        batch,
        FLIGHT_DATA_SIZE_TARGET_BYTES,
        FLIGHT_DATA_RESERVED_CEILING_BYTES,
    )
}

/// [`frame_for_wire`] with explicit bounds, so tests can drive the intervention
/// path on small fixtures instead of allocating multi-megabyte batches.
pub(crate) fn frame_for_wire_bounded(
    batch: RecordBatch,
    target: usize,
    ceiling: usize,
) -> Result<Vec<RecordBatch>, RowTooWide> {
    if batch.num_rows() == 0 || !encoder_split_would_exceed(&batch, target) {
        return Ok(vec![batch]);
    }
    byte_partition(batch, target, ceiling)
}

/// Would the encoder's OWN row-uniform split put more than `target` payload bytes
/// into some slice? (arrow-flight 53.4.1 `split_batch_for_grpc_response`,
/// replicated — see this module's header for the quoted source.)
///
/// Short-circuits on the first violation, and on the whole-batch check first: if
/// the entire batch fits, no slice of it can fail.
fn encoder_split_would_exceed(batch: &RecordBatch, target: usize) -> bool {
    let rows = batch.num_rows();
    if rows == 0 {
        return false;
    }
    if slice_payload_bytes(batch, 0, rows) <= target {
        return false;
    }
    // `max(1)` mirrors the encoder AND makes the division safe for a nonsense
    // target supplied by a test.
    let target = target.max(1);
    let capacity: usize = batch
        .columns()
        .iter()
        .map(|c| c.get_buffer_memory_size())
        .sum();
    let n_batches = (capacity / target + usize::from(capacity % target != 0)).max(1);
    let rows_per_batch = (rows / n_batches).max(1);
    let mut offset = 0;
    while offset < rows {
        let length = rows_per_batch.min(rows - offset);
        if slice_payload_bytes(batch, offset, length) > target {
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
