//! Byte-bounded Arrow egress batches — the dual row-cap / byte-cap batch
//! boundary (issue #2825, T4/M11).
//!
//! # The problem
//!
//! Before this module both egress build sites finished a record batch on **row
//! count** alone (`buffer.len() >= batch_size`). A batch's byte size was
//! therefore `batch_size × row_width` — an *unbounded* function of schema shape:
//! the same code path that produces a ~192 KiB batch for a two-column keyvalue
//! table produces a 512 MiB batch for a table with a 64 KiB blob column. The
//! ratified B4 budget (≤16Mi per-query working set at concurrency 1) cannot be
//! held by a bound stated in rows.
//!
//! # The mechanism
//!
//! [`BatchByteCap`] is a running accumulator: each buffered row's
//! [`estimate_arrow_row_bytes`] width is [`push`](BatchByteCap::push)ed as the
//! row enters the buffer, and the producer flushes when EITHER the row-cap or
//! this byte-cap trips — whichever comes first. The decision is made **before**
//! `rows_to_record_batch` allocates anything: building a batch to discover it is
//! oversized is a report, not a cap, and `RecordBatch::get_array_memory_size()`
//! is only readable after every value has been copied.
//!
//! # Currency: payload bytes, and the published capacity conversion
//!
//! The cap is normatively denominated in Arrow **payload** bytes (the sum of
//! buffer lengths — `cqlite_core::export::arrow_payload_bytes`). It is NOT
//! denominated in `get_array_memory_size()`, which reports buffer **capacity**:
//! the construction path (`StringArray::from` / `BinaryArray::from`) grows
//! `MutableBuffer` by power-of-two doubling from zero, so reported memory runs
//! up to ~2× payload (measured 1.72–1.80× on realistic shapes against arrow 53).
//! Capacity is a property of an allocator's growth policy, not of the data: it
//! is not computable before the batch exists and it is non-monotonic in row
//! count, so it cannot be the trigger.
//!
//! Consumers that must budget in capacity currency convert with the published
//! constants:
//!
//! ```text
//! worst_case_get_array_memory_size
//!     <= BATCH_BYTES_CAPACITY_FACTOR * cap
//!        + BATCH_BYTES_PER_COLUMN_SLACK * n_columns
//! ```
//!
//! # Composition with the per-stream egress ceiling (issue #2821)
//!
//! At the 4 MiB default that worst case is `2 × 4 MiB = 8 MiB` of capacity per
//! resident batch. Issue #2821's per-stream in-flight ceiling must therefore be
//! budgeted in **capacity** currency too — `streaming.rs` already meters
//! `get_array_memory_size()` — giving a guaranteed bound of
//! `ceiling + one maximum batch`. With a 6 MiB ceiling that is
//! `6 + 8 = 14 MiB < 16Mi`, inside B4 at concurrency 1. (The naive
//! `4 + 8 = 12 MiB` reading of the task framing mixes payload and capacity: a
//! 4 MiB *payload* cap is an 8 MiB *capacity* batch, so an 8 MiB ceiling would
//! land at exactly 16 MiB with zero headroom.)
//!
//! # Liveness
//!
//! Push-then-test: a batch is cut only when the buffer is **non-empty** and the
//! accumulated estimate has reached the cap. A single row wider than the whole
//! cap is delivered as a one-row batch — never dropped, never a stall. Caps of
//! `0` and `1` degrade to one row per batch rather than hanging, the same clamp
//! posture as `batch_size.max(1)`.

use cqlite_core::export::estimate_arrow_row_bytes;
use cqlite_core::query::{ColumnInfo, QueryRow};

/// Default per-batch Arrow **payload** byte cap: 4 MiB.
///
/// Chosen so the row-cap still trips first on every narrow shape measured in
/// this tree, i.e. `batch_size × narrow_row_bytes < cap`:
///
/// | narrow shape | bytes/row | full 8192-row batch | headroom |
/// |---|---:|---:|---:|
/// | `issue_1494` fixture (`k{i:06}`/`v{i}`) | ~20 | ~192 KiB | ~22× |
/// | `many_partition_fixture` (`int`/`text`/`int`) | ~13 | ~107 KiB | ~39× |
/// | field model (`phase1-5-transport-ingest.md:195`) | ~180 | 1.47 MB | ~2.9× |
/// | the contested 300 B/row figure | 300 | 2.34 MiB | 1.7× |
///
/// So the byte-cap is a no-op on the narrow path — no throughput regression —
/// and binds only where a batch would otherwise be unbounded. Note that at the
/// pessimistic 300 B/row the *capacity* reading of a full narrow batch is
/// already 4,227,256 B, above 4 MiB: precisely why the cap must be
/// payload-denominated.
pub const DEFAULT_MAX_BATCH_BYTES: usize = 4 * 1024 * 1024;

/// Environment variable backing `--max-batch-bytes`.
pub const ENV_MAX_BATCH_BYTES: &str = "CQLITE_MAX_BATCH_BYTES";

/// Worst-case ratio of a batch's `get_array_memory_size()` (buffer **capacity**)
/// to its payload bytes (buffer **lengths**).
///
/// `MutableBuffer::reserve` grows to `max(round_upto_multiple_of_64(required),
/// capacity * 2)` — power-of-two doubling from zero — so a payload landing just
/// past a power of two reports up to ~2× that payload. Measured against this
/// tree's arrow 53: 1.001× (512 × 8 KiB binary), 1.280× (100 × 64 KiB binary),
/// 1.445× (8192 × 180 B binary), 1.720× (8192 × 300 B binary), 1.779× (8192 ×
/// 290 B string), 1.801× (8192 × 20 B string). `2` is the bound, not the typical.
///
/// Published so a consumer — notably issue #2821's per-stream in-flight ceiling
/// — can convert this change's payload guarantee into the capacity currency it
/// meters, with no undocumented fudge factor.
pub const BATCH_BYTES_CAPACITY_FACTOR: usize = 2;

/// Fixed capacity slack allowed **per output column**, on top of
/// [`BATCH_BYTES_CAPACITY_FACTOR`] × cap.
///
/// Every Arrow array carries small fixed allocations that do not scale with the
/// payload (a 64-byte-aligned minimum allocation per buffer, the validity
/// buffer, an empty-array's offsets buffer). On a batch that is mostly one wide
/// column these round to nothing; on a wide-schema batch of tiny rows they are
/// the whole reported size, so a capacity bound stated purely as a multiple of
/// the payload would be wrong for that shape.
pub const BATCH_BYTES_PER_COLUMN_SLACK: usize = 1024;

/// Whether the caller should finish the current batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShouldFlush {
    /// Keep accumulating: neither cap has been reached.
    No,
    /// The byte-cap has been reached — finish this batch now.
    Yes,
}

impl ShouldFlush {
    /// `true` when the batch must be finished.
    #[inline]
    pub fn is_yes(self) -> bool {
        matches!(self, ShouldFlush::Yes)
    }
}

/// Running per-batch payload-byte accumulator implementing the byte half of the
/// dual row-cap / byte-cap boundary.
///
/// Shared by BOTH egress build sites (`producer.rs`'s partition-at-a-time merge
/// loop and `producer_stream.rs`'s row-granular loop) so the boundary rule is
/// defined once. A cap wired into only one path would leave the other unbounded.
#[derive(Debug, Clone)]
pub struct BatchByteCap {
    /// Configured payload-byte ceiling for one batch.
    cap: usize,
    /// Estimated payload bytes accumulated since the last [`Self::reset`].
    accumulated: usize,
    /// Rows accumulated since the last [`Self::reset`] — the liveness guard that
    /// makes the one-row floor unconditional.
    rows: usize,
}

impl BatchByteCap {
    /// Build an accumulator enforcing `cap` payload bytes per batch.
    ///
    /// `cap` is used exactly as given, including `0` and `1`: the push-then-test
    /// rule makes those degrade to one row per batch rather than hang, so no
    /// clamp is needed (and clamping would silently misreport the operator's
    /// configuration). `usize::MAX` effectively disables the byte-cap, leaving
    /// the row-cap as the sole boundary.
    pub fn new(cap: usize) -> Self {
        Self {
            cap,
            accumulated: 0,
            rows: 0,
        }
    }

    /// The configured payload-byte ceiling.
    pub fn cap(&self) -> usize {
        self.cap
    }

    /// Estimated payload bytes accumulated in the current batch.
    pub fn accumulated(&self) -> usize {
        self.accumulated
    }

    /// Account for one row that has just been pushed into the buffer, and report
    /// whether the batch must now be finished.
    ///
    /// **Push-then-test** (the one-row floor): the row is always counted first,
    /// so the answer can only be [`ShouldFlush::Yes`] with at least one row in
    /// the buffer. A row whose estimate exceeds the entire cap therefore leaves
    /// as a one-row batch instead of triggering a flush of an empty buffer,
    /// which would loop without progress.
    ///
    /// `width` is saturating-added, so a fail-closed `usize::MAX` estimate
    /// (a pathological value, see `estimate_arrow_row_bytes`) pins the
    /// accumulator at the ceiling and cuts the batch rather than wrapping.
    pub fn push_width(&mut self, width: usize) -> ShouldFlush {
        self.accumulated = self.accumulated.saturating_add(width);
        self.rows = self.rows.saturating_add(1);
        if self.rows > 0 && self.accumulated >= self.cap {
            ShouldFlush::Yes
        } else {
            ShouldFlush::No
        }
    }

    /// Estimate `row`'s Arrow payload width for the projected `columns` and
    /// account for it — the form both producers use.
    pub fn push_row(&mut self, columns: &[ColumnInfo], row: &QueryRow) -> ShouldFlush {
        self.push_width(estimate_arrow_row_bytes(columns, row))
    }

    /// Clear the accumulator for the next batch. Called wherever the buffer is
    /// flushed, so the running estimate always describes exactly the rows
    /// currently buffered — the whole buffer is never re-measured per push.
    pub fn reset(&mut self) {
        self.accumulated = 0;
        self.rows = 0;
    }
}

/// Split an already-materialized row slice into contiguous groups, each ending
/// where the dual row-cap / byte-cap boundary falls.
///
/// Used by the aggregate route (issue #841), which folds rows into accumulator
/// state and then materializes one PARTIAL row per `GROUP BY` group in one go —
/// it never passes through the incremental buffer, so it needs the boundary
/// applied after the fact. The row path uses [`BatchByteCap`] directly.
///
/// Never yields an empty group: the same push-then-test rule applies, so a
/// single over-cap row becomes a one-row group. An empty input yields no groups.
pub fn split_rows_into_batches<'a>(
    columns: &[ColumnInfo],
    rows: &'a [QueryRow],
    max_rows: usize,
    cap: usize,
) -> Vec<&'a [QueryRow]> {
    let max_rows = max_rows.max(1);
    let mut groups = Vec::new();
    let mut byte_cap = BatchByteCap::new(cap);
    let mut start = 0usize;
    for (i, row) in rows.iter().enumerate() {
        let byte_full = byte_cap.push_row(columns, row).is_yes();
        let len = i + 1 - start;
        if len >= max_rows || byte_full {
            groups.push(&rows[start..=i]);
            start = i + 1;
            byte_cap.reset();
        }
    }
    if start < rows.len() {
        groups.push(&rows[start..]);
    }
    groups
}

/// Worst-case resident size, in `get_array_memory_size()` (capacity) bytes, of
/// ONE emitted batch produced under `cap` with `n_columns` output columns.
///
/// Derived from the published constants alone:
/// `BATCH_BYTES_CAPACITY_FACTOR * cap + BATCH_BYTES_PER_COLUMN_SLACK * n_columns`.
/// This is the quantity issue #2821's per-stream ceiling composes with to state
/// its `ceiling + one maximum batch` bound against B4's ≤16Mi.
///
/// Saturating: an operator-configured `usize::MAX` cap reports `usize::MAX`
/// rather than wrapping.
pub fn worst_case_batch_capacity_bytes(cap: usize, n_columns: usize) -> usize {
    cap.saturating_mul(BATCH_BYTES_CAPACITY_FACTOR)
        .saturating_add(BATCH_BYTES_PER_COLUMN_SLACK.saturating_mul(n_columns))
}

#[cfg(test)]
#[path = "batch_bytes_tests.rs"]
mod batch_bytes_tests;
