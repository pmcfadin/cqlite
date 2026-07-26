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
//! [`BatchByteCap`] is a running accumulator: each candidate row's
//! [`estimate_arrow_row_bytes`] width is tested against the accumulator with
//! [`cut_before`](BatchByteCap::cut_before) **before** the row joins the buffer,
//! and the producer flushes when EITHER the row-cap or this byte-cap trips —
//! whichever comes first. The decision is made before `rows_to_record_batch`
//! allocates anything: building a batch to discover it is oversized is a report,
//! not a cap, and `RecordBatch::get_array_memory_size()` is only readable after
//! every value has been copied.
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
//!     <= BATCH_BYTES_CAPACITY_FACTOR * max(cap, widest_row_payload)
//!        + BATCH_BYTES_PER_COLUMN_SLACK * n_array_nodes
//! ```
//!
//! # The `max(cap, widest_row_payload)` term is not a fudge
//!
//! One row cannot be split across two Arrow batches, so a schema whose single
//! widest row exceeds the whole cap has an **inherent, unbounded** overshoot:
//! that row leaves as a one-row batch of its own natural size (the alternative
//! is dropping it or stalling). The cap therefore bounds a batch at
//! `max(cap, widest_row_payload)` payload bytes — which reduces to plain `cap`
//! for every schema whose widest row fits, and the overshoot is a property of
//! the DATA, not slack in the mechanism.
//!
//! Nothing in the conversion path imposes a smaller per-cell ceiling that would
//! bound the term for us: `arrow_convert.rs`'s `checked_value_bytes` guard
//! rejects only a *cumulative* `Utf8`/`Binary` column length above
//! `i32::MAX` (2 GiB — the 32-bit Arrow offset limit) and returns an error
//! rather than clamping, so the honest per-row ceiling is that same ~2 GiB.
//!
//! The mechanism itself never overshoots: the boundary is **test-then-push**
//! (below), so a batch whose rows all fit is cut BEFORE the row that would
//! cross, never after it.
//!
//! # What this change guarantees, and what it does NOT (issue #2821)
//!
//! **Guaranteed here, today: a bound on ONE batch.** At the 4 MiB default, over
//! a schema whose rows fit the cap, an emitted batch is ≤4 MiB of payload and
//! therefore ≤`2 × 4 MiB = 8 MiB` of capacity — see
//! [`worst_case_batch_capacity_bytes`], and add the wider row's bytes for a
//! deployment whose rows can individually exceed the cap.
//!
//! **NOT guaranteed here: per-stream egress residency.** The `do_get` path is
//! still **count**-bounded, not byte-bounded: `streaming.rs`'s
//! `DO_GET_CHANNEL_CAPACITY` is 4 batches plus up to ~3 more in flight
//! (`IN_FLIGHT_ALLOWANCE`), so worst-case resident egress is
//! `~7 × 8 MiB ≈ 56 MiB` per stream, NOT 14 MiB. The
//! `get_array_memory_size()` reading that `streaming.rs` takes per batch is fed
//! to **metrics only** — no admission or backpressure decision consumes it — so
//! it does not bound residency. What this change does for that number is make it
//! *finite and stated*: before it, one batch was `batch_size × row_width`, so the
//! product was unbounded in schema shape.
//!
//! **The composition becomes true only once #2821 lands.** When #2821 enforces a
//! per-stream in-flight ceiling denominated in **capacity** currency, its
//! guaranteed bound is `ceiling + one maximum batch`; with a 6 MiB ceiling that
//! is `6 + 8 = 14 MiB < 16Mi`, inside B4 at concurrency 1. (The naive
//! `4 + 8 = 12 MiB` reading of the task framing mixes payload and capacity: a
//! 4 MiB *payload* cap is an 8 MiB *capacity* batch, so an 8 MiB ceiling would
//! land at exactly 16 MiB with zero headroom.) Until that ceiling exists and
//! actually gates production, the 14 MiB figure is a TARGET for the dependent
//! issue, not a property of this tree.
//!
//! # Liveness
//!
//! Test-then-push: the crossing row's width is tested against the accumulator
//! FIRST, and the batch is cut only when the buffer is **non-empty** and adding
//! the row would take it past the cap. An empty buffer always accepts the row,
//! however wide — so a single row wider than the whole cap is delivered as a
//! one-row batch, never dropped and never a stall. Caps of `0` and `1`
//! therefore degrade to one row per batch rather than hanging — the same
//! *outcome* `batch_size.max(1)` gives the row-cap, reached by the ordering rule
//! instead of by clamping the operator's configured value.

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

/// Fixed capacity slack allowed **per Arrow array node**, on top of
/// [`BATCH_BYTES_CAPACITY_FACTOR`] × cap.
///
/// Every Arrow array carries small fixed allocations that do not scale with the
/// payload (a 64-byte-aligned minimum allocation per buffer, the validity
/// buffer, an empty-array's offsets buffer). On a batch that is mostly one wide
/// column these round to nothing; on a wide-schema batch of tiny rows they are
/// the whole reported size, so a capacity bound stated purely as a multiple of
/// the payload would be wrong for that shape.
///
/// Denominated in array NODES, not output columns: a flat scalar column is one
/// node, but a `list<text>` column is two (the `ListArray` and its `Utf8`
/// child) and a `map<text,text>` column is four (map, entries struct, key
/// `Utf8`, value `Utf8`). Callers with a flat schema pass the column count;
/// callers with nested columns must count the child arrays too, or the slack
/// term under-states their fixed allocations. (At the 4 MiB default the
/// `2 × cap` term dominates by three orders of magnitude either way; the
/// distinction bites only for a tiny cap over a deeply nested schema.)
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
    /// `cap` is used exactly as given, including `0` and `1`: the test-then-push
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

    /// Rows accumulated into the current batch since the last [`Self::reset`].
    /// A [`ShouldFlush::Yes`] can only ever be reported with this at 1 or more —
    /// the one-row floor.
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// **Test-then-push**: must the currently buffered rows be finished BEFORE a
    /// row of `width` payload bytes is appended?
    ///
    /// Answering before the row joins the buffer is what bounds a batch at `cap`
    /// rather than at `cap - 1 + width_of_crossing_row`: the batch is cut on the
    /// row that WOULD cross, so the crossing row starts the next batch instead
    /// of overshooting this one (issue #2825 review B1).
    ///
    /// The one-row floor is the `self.rows > 0` conjunct: an **empty buffer
    /// always accepts the row**, however wide, so a row wider than the entire
    /// cap leaves as a one-row batch and can never trigger a flush of nothing
    /// (which would loop without progress). Caps of `0` and `1` therefore
    /// degrade to one row per batch rather than hanging.
    ///
    /// Saturating: a fail-closed `usize::MAX` width (a pathological value, see
    /// `estimate_arrow_row_bytes`) compares at the ceiling instead of wrapping.
    pub fn cut_before(&self, width: usize) -> ShouldFlush {
        if self.rows > 0 && self.accumulated.saturating_add(width) > self.cap {
            ShouldFlush::Yes
        } else {
            ShouldFlush::No
        }
    }

    /// Account for one row of `width` payload bytes that has just been appended
    /// to the buffer. Call AFTER [`Self::cut_before`] has been honoured.
    ///
    /// `width` is saturating-added, so a `usize::MAX` estimate pins the
    /// accumulator at the ceiling rather than wrapping.
    pub fn accumulate(&mut self, width: usize) {
        self.accumulated = self.accumulated.saturating_add(width);
        self.rows = self.rows.saturating_add(1);
    }

    /// Estimate `row`'s Arrow payload width for the projected `columns` — the
    /// quantity both [`Self::cut_before`] and [`Self::accumulate`] take, computed
    /// once per row by the caller so it is never estimated twice.
    pub fn row_width(columns: &[ColumnInfo], row: &QueryRow) -> usize {
        estimate_arrow_row_bytes(columns, row)
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
/// Never yields an empty group: the same test-then-push rule applies, so a
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
        let width = BatchByteCap::row_width(columns, row);
        // Cut BEFORE the crossing row, so the group that ends here holds only
        // rows that fit — the same rule the two incremental producers apply.
        // `cut_before` is `No` while the group is empty, so `start < i` holds
        // here and the pushed group is never empty.
        if byte_cap.cut_before(width).is_yes() {
            groups.push(&rows[start..i]);
            start = i;
            byte_cap.reset();
        }
        byte_cap.accumulate(width);
        if i + 1 - start >= max_rows {
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
/// ONE emitted batch produced under `cap` over a schema whose widest single row
/// contributes `widest_row_payload` payload bytes, with `n_array_nodes` Arrow
/// array nodes (see [`BATCH_BYTES_PER_COLUMN_SLACK`]).
///
/// Derived from the published constants alone:
///
/// ```text
/// BATCH_BYTES_CAPACITY_FACTOR * max(cap, widest_row_payload)
///     + BATCH_BYTES_PER_COLUMN_SLACK * n_array_nodes
/// ```
///
/// This is the quantity issue #2821's per-stream ceiling will compose with to
/// state its `ceiling + one maximum batch` bound against B4's ≤16Mi. Until that
/// ceiling lands, egress residency is count-bounded, not byte-bounded — see the
/// module documentation.
///
/// The `max(..)` term is honest, not slack. The boundary is test-then-push, so a
/// batch is cut BEFORE the row that would cross the cap — but a row cannot be
/// split across Arrow batches, so a row wider than the whole cap is emitted
/// alone at its own natural width. Callers whose rows are known to fit the cap
/// pass `0` (or any value ≤ `cap`) and get the familiar
/// `FACTOR * cap + slack` bound; callers that cannot rule out a wider row must
/// state that row's payload here. Nothing downstream clamps it —
/// `arrow_convert.rs`'s `checked_value_bytes` guard only *rejects* a cumulative
/// column length above `i32::MAX`, so ~2 GiB is the only structural ceiling.
///
/// Saturating: an operator-configured `usize::MAX` cap reports `usize::MAX`
/// rather than wrapping.
pub fn worst_case_batch_capacity_bytes(
    cap: usize,
    n_array_nodes: usize,
    widest_row_payload: usize,
) -> usize {
    cap.max(widest_row_payload)
        .saturating_mul(BATCH_BYTES_CAPACITY_FACTOR)
        .saturating_add(BATCH_BYTES_PER_COLUMN_SLACK.saturating_mul(n_array_nodes))
}

#[cfg(test)]
#[path = "batch_bytes_tests.rs"]
mod batch_bytes_tests;
