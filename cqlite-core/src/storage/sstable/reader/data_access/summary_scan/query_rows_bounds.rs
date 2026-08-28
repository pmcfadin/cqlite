//! Sizing constants for [`QueryRowStream`](super::QueryRowStream) and the
//! read-ahead bounds derived from them (issue #3384).
//!
//! Split out of `query_rows.rs` under the campsite rule (epic #1116): the
//! derivation below is mostly PROSE — four buffer terms, a precondition, and the
//! record of how the bound was once wrong — and it pushed the driver file over
//! the 800-line threshold. Keeping it beside the driver would mean either
//! deleting the reasoning or growing an over-threshold file, and the reasoning is
//! the part that stops the bound silently going wrong again.
//!
//! Everything here is `const`; there is no behaviour in this file.

use super::super::super::scan_stream_windowed::{BATCH_EMIT_ROWS, MAX_INFLIGHT_BATCH_ROWS};
use super::super::sequential::batched_scan_stream::batched_channel_capacity;

/// Rows accumulated before a batch is handed to the consumer. Matches the
/// batched scan surface's emit granularity (issue #1592): one cross-thread
/// handoff per batch instead of per row.
pub(crate) const QUERY_ROWS_PER_BATCH: usize = 128;

/// Batches the bounded handoff channel may hold. Resident rows on this channel
/// are therefore bounded by [`QUERY_ROWS_MAX_RESIDENT_ROWS`] plus the partition
/// currently being decoded — independent of table size.
pub(crate) const QUERY_ROWS_CHANNEL_BATCHES: usize = 4;

/// Rows the query-row handoff channel can hold in flight ahead of a consumer that
/// has stopped reading — [`QUERY_ROWS_CHANNEL_BATCHES`] channel-resident batches
/// plus the ONE batch a producer parked in `SyncSender::send` still owns (a
/// blocked send moves the value only on success), each of at most
/// [`QUERY_ROWS_MAX_HANDOFF_BATCH_ROWS`] rows. The batch currently being
/// accumulated is not a further term: once it reaches that size it BECOMES the
/// parked send, and a parked producer accumulates no further rows.
///
/// Crate-internal (`pub` for its doc links, not re-exported to the crate root):
/// the bound OUTSIDE callers need is [`QUERY_ROWS_MAX_READ_AHEAD`].
///
/// This bounds the HANDOFF CHANNEL ALONE. It is NOT the walk's read-ahead: the
/// producer thread pulls from an inner batched scan stream that has bounded
/// buffers of its own, so a cancelled or abandoned walk can decode up to
/// [`QUERY_ROWS_MAX_READ_AHEAD`] rows — not this many — after its consumer
/// stops. Use that constant for "how far can an abandoned walk run"; use this one
/// only when reasoning about the channel itself.
pub const QUERY_ROWS_MAX_RESIDENT_ROWS: usize =
    QUERY_ROWS_MAX_HANDOFF_BATCH_ROWS * (QUERY_ROWS_CHANNEL_BATCHES + 1);

/// The largest batch either arm can put on the handoff channel.
///
/// The two arms size their batches DIFFERENTLY, and taking the smaller of the two
/// is how the exported read-ahead bound came to understate the full-ring arm by
/// a factor of two (roborev, issue #3384):
///
/// * the token-bounded arm accumulates through [`BatchSink`] and emits at
///   [`QUERY_ROWS_PER_BATCH`] (128) rows;
/// * the full-ring arm ([`drive_full_scan_rows`]) does NOT re-chunk — `emit_rows`
///   forwards the inner batched scan stream's batch VERBATIM, and those are capped
///   at [`BATCH_EMIT_ROWS`] (256).
///
/// A bound that must hold on BOTH arms therefore has to use the MAXIMUM, not
/// either arm's own figure.
pub(crate) const QUERY_ROWS_MAX_HANDOFF_BATCH_ROWS: usize =
    if BATCH_EMIT_ROWS > QUERY_ROWS_PER_BATCH {
        BATCH_EMIT_ROWS
    } else {
        QUERY_ROWS_PER_BATCH
    };

/// `buffer_size` the full-ring arm ([`drive_full_scan_rows`]) hands to the inner
/// batched scan stream. Named so [`QUERY_ROWS_MAX_READ_AHEAD`] is derived
/// from the SAME value the call site passes, never a restated copy of it.
pub(crate) const QUERY_ROWS_FULL_SCAN_BUFFER_ROWS: usize =
    QUERY_ROWS_PER_BATCH * QUERY_ROWS_CHANNEL_BATCHES;

/// Upper bound, IN ROWS, on what a [`QueryRowStream`] producer can decode AHEAD of its
/// consumer — and therefore on how much work an ABANDONED or CANCELLED walk can
/// still do after the consumer stops reading. A CONSTANT: it does not scale with
/// the table, so a walk over a million partitions abandoned after one row decodes
/// at most this many more rows, not the rest of the table.
///
/// Every term is a bounded buffer between the disk and the consumer, on the
/// full-ring arm ([`drive_full_scan_rows`], the deepest of the two arms):
///   1. [`QUERY_ROWS_MAX_RESIDENT_ROWS`] — this stream's handoff channel plus the
///      producer's parked send.
///   2. The inner batched scan stream's public channel:
///      [`batched_channel_capacity`] batches of up to
///      [`BATCH_EMIT_ROWS`] rows.
///   3. The one inner batch [`drive_full_scan_rows`] has `recv`'d and still owns
///      while parked handing it on (up to [`BATCH_EMIT_ROWS`] rows).
///   4. [`MAX_INFLIGHT_BATCH_ROWS`] — the windowed scan's own batching subsystem
///      (pending batch + bounded batch channel + producer parked in `send`).
///
/// # What it does NOT cover
///
/// * The ONE partition currently being decoded: the windowed scan materializes a
///   confirmed partition in `scratch` before batching it, so a table with wide
///   partitions adds a `max_partition_size` term on top (the pre-existing #1156
///   term, see [`MAX_INFLIGHT_BATCH_ROWS`]'s docs).
/// * Anything resident in the CONSUMER downstream of this stream — a Flight
///   producer's own Arrow batches, for instance, are bounded by that consumer's
///   own budget, not by this constant.
/// * **Regions that decode to NO emitted rows.** This is a PRECONDITION, not a
///   footnote (roborev, issue #3384). The full-ring arm observes cancellation
///   only BETWEEN batches it receives from the inner scan, because
///   `scan_stream_batched_admitted` is not handed the child cancel flag and does
///   not poll one internally. So the bound holds while decoded partitions keep
///   YIELDING rows — every decoded partition then pushes the channel toward the
///   parked state in which cancellation is observed. Over a stretch that survives
///   nothing (all tombstoned, or all TTL-expired at the read clock) the inner scan
///   emits no batch, the producer never parks, and it can decode past this bound —
///   in the limit, to the end of the table. Tracked as a real cancellation gap in
///   the read path, NOT merely a documentation caveat: see issue #3428.
/// * **Eager materialization inside a single decode step.** The bound counts rows
///   RESIDENT IN THE PIPELINE'S BOUNDED BUFFERS, and the work probe it is asserted
///   against (`work_counters::stream_walk_partitions_parsed`) advances as entries
///   are ITERATED — which is backpressured, hence bounded. It does NOT bound work
///   done eagerly *inside* one step before any iteration begins (roborev, issue
///   #3384): `parse_batched_block` materializes a whole block's entries at once,
///   and for a format whose data section is one block that is the whole table;
///   `bti_scan_with_metadata` on the `da` path is materializing too. So read this
///   constant as *"rows that can be handed onward after the consumer leaves"*, not
///   as *"CPU and memory an abandoned walk can still spend"*. The two coincide on
///   the incremental block-iteration path and diverge on the materializing ones.
pub const QUERY_ROWS_MAX_READ_AHEAD: usize = QUERY_ROWS_MAX_RESIDENT_ROWS
    + batched_channel_capacity(QUERY_ROWS_FULL_SCAN_BUFFER_ROWS) * BATCH_EMIT_ROWS
    + BATCH_EMIT_ROWS
    + MAX_INFLIGHT_BATCH_ROWS;
