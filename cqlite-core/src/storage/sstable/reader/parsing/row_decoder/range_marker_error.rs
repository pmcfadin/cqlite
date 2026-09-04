//! What a partition row loop does when a range-tombstone MARKER cannot be turned
//! into output faithfully (issue #3721) — either because it is FRAMED and
//! unrepresentable ([`range_marker_refused`]) or because it cannot be PARSED at
//! all on the final chunk ([`range_marker_unparseable`] +
//! [`unparseable_marker_at_final_chunk`]).
//!
//! # The defect this module replaces
//!
//! [`super::column_decode_error`] removed the same shape one structural level
//! down — a per-COLUMN decode failure handled with a bare `break`, so the row was
//! assembled from the cells gathered so far and returned as `Ok`. The MARKER sites
//! were the same construct:
//!
//! ```text
//! block_emit.rs           if sh.feed_range_marker(..).is_err() { break; }
//! block_emit_windowed.rs  if let Err(e) = sh.feed_range_marker(..) { .. break; }
//! timestamp_policy.rs     if sh.feed_range_marker(..).is_err() { MarkerOutcome::Stop }
//! compaction.rs           _ => { /* Unknown bound kind: skip it */ }   (#3808)
//! ```
//!
//! # The SECOND census, one arm over — the marker-PARSE arms (roborev job 78)
//!
//! Fixing the arms above left their SIBLING arm — the one taken when the marker
//! cannot be PARSED at all — answering with the framing terminator:
//!
//! ```text
//! timestamp_policy.rs:148/156   Err(_) => MarkerOutcome::Stop
//! block_emit.rs:149/157         Err(_) => break
//! block_emit_windowed.rs:385/399 Err(e) => { tracing::debug!(..); break; }
//! ```
//!
//! Every one of those is a SUCCESSFUL, incomplete partition on the user-facing
//! read path: the marker and every later row of the partition vanish and `SELECT`
//! returns `Ok`. There is no downstream guard —
//! [`super::block_emit`]'s `partition_complete` flag gates only the #3095
//! static-only emission and never rejects an incomplete partition.
//!
//! The rationale that had been written beside them ("a genuine framing
//! terminator") is the SAME one this issue already disproved for
//! `compaction.rs`. Two facts settle it, and neither is a byte pattern:
//!
//! 1. the branch is entered only when `is_range_tombstone_marker(data[offset])`,
//!    with `is_end_of_partition` handled SEPARATELY one branch above — so a parse
//!    failure here is abnormal data, never an end-of-partition signal. Authority:
//!    `UnfilteredSerializer.deserializeOne` (cassandra-5.0.8:458-483) ends a
//!    partition on `isEndOfPartition(flags)` ALONE (returning `null`); the marker
//!    kind then deserializes the bound + `deserializeMarkerBody`, whose failure
//!    throws `IOException` and propagates — Cassandra never converts it into
//!    "the partition ended", and throws explicitly on corrupt flags;
//! 2. in `block_emit` the ROW `Err` arm already routes through
//!    [`super::column_decode_error::end_of_partition_or_bail`] while the MARKER
//!    arm 120 lines above it just `break`s — rows protected, markers not, inside
//!    one function.
//!
//! Fix 1 of this issue (a marker body must be consumed EXACTLY,
//! `row_framing::parse_range_tombstone_marker_with_ldt`) makes that hole MORE
//! reachable, since a body-size mismatch now returns `Err`.
//!
//! ## What replaces it, by path kind
//!
//! * **buffered** (`parse_block_emit_with_metadata`, `parse_block_emit_windowed`)
//!   — the whole block is already materialised, so NO refill is possible and the
//!   failure is unambiguously corruption:
//!   [`unparseable_marker_in_buffered_block`];
//! * **streaming policy** ([`super::timestamp_policy`], driven by
//!   `drive_partition_sliding`, which holds `at_final_chunk`) — the cause is
//!   carried in [`super::partition_driver::MarkerOutcome::Unparseable`] and the
//!   DRIVER decides: `NeedMore` on a non-final chunk (unchanged behaviour, since a
//!   marker body may simply straddle the window boundary) and the preserved cause
//!   propagated on the final chunk, via
//!   [`unparseable_marker_at_final_chunk`].
//!
//! `MarkerOutcome::Stop` — the variant that expressed "this parse failure IS a
//! framing terminator" — is GONE with those arms rather than left available: it
//! had exactly one meaning, that meaning was wrong for every real policy, and a
//! variant a future policy could reach for is how this defect was written twice.
//! There is now ONE answer to a marker parse failure: carry the cause and let
//! whoever holds the finality decide.
//!
//! ## Two sites deliberately UNCHANGED, and why
//!
//! * `block_emit`'s DELTA-SCAN loop (`:462`) already propagates with `?`,
//!   wrapping (not discarding) the cause;
//! * `block_emit_windowed::prime_shadow_before_window` (`:750`) returns `false`,
//!   which means "cannot prime, decode the FULL partition instead" — a fail-safe
//!   fallback that drops no rows. The same marker is re-encountered by the full
//!   decode, where the arms above now refuse it. Turning it into an error would
//!   remove a legitimate fallback and change nothing about the outcome.
//!
//! The fourth site is the COMPACTION policy, and it is the most consequential of
//! the four (issue #3808): its rows are WRITTEN, so omitting an unrepresentable
//! deletion marker resurrects the rows that marker shadowed DURABLY, on disk —
//! and it was the lone fail-open reader of that byte, since
//! [`super::partition_shadow::PartitionShadow::feed_range_marker`] and delta-scan
//! `block_emit` already refuse it.
//!
//! Each `break`/`Stop` left the partition's row loop and the read then reported
//! `Ok` — so the marker AND every remaining row of that partition silently
//! vanished from a successful `SELECT`. `block_emit_windowed`'s instance is on the
//! point/slice read path, so a clustering-slice query was the worst-affected
//! surface.
//!
//! # Why this is corruption and NOT a boundary — the discriminator
//!
//! Two facts, both established at the call site, neither inferred from bytes:
//!
//! 1. **The marker is already FRAMED.** These handlers run only inside the `Ok`
//!    arm of `parse_range_tombstone_marker_full`, which has bound a valid
//!    `next_offset`. The partition body demonstrably CONTINUES there; the loop
//!    threw that binding away. So this is not end-of-data, and it is not the
//!    "no further row parses here" signal the loops use to detect the end of a
//!    partition body (that signal is a PARSE failure, which yields no resume
//!    point — see [`super::column_decode_error::end_of_partition_or_bail`]).
//! 2. **Each site fails on exactly ONE condition** — a bound kind the
//!    read-side shadow FSM has no faithful representation for
//!    ([`super::partition_shadow::PartitionShadow::feed_range_marker`]'s
//!    `unknown =>` arm, the only `Err` it returns; the compaction policy's own
//!    `unknown =>` arm is the same predicate over the same byte). Every kind a
//!    well-formed marker can carry is represented; a kind outside that set is
//!    evidence the cursor or the data is wrong. `row_framing` returns the bound
//!    kind UNVALIDATED, so an arbitrary byte reaches these matches and giving it a
//!    permissive default meaning is the inference issue #28 forbids.
//!
//! # Why it is not skippable either
//!
//! Advancing to `next_offset` and continuing is worse than either alternative: an
//! unrepresentable bound may OPEN a deletion range covering the rows that follow,
//! so a read that ignores it emits rows a Cassandra `SELECT` hides — deleted data
//! resurrected and reported as success. Guessing what the bound covers is exactly
//! the byte-pattern inference issue #28 forbids. So the condition is FATAL and is
//! propagated to the caller of the read.
//!
//! # The second condition: a marker that cannot be PARSED (roborev job 16)
//!
//! The discriminator above turns on the marker being FRAMED. A marker that does
//! not parse has no resume offset, so it is not that case — and it was handled by
//! the (now REMOVED) `MarkerOutcome::Stop`, which the drivers converted on the
//! FINAL chunk into a successful partition completion. For the COMPACTION
//! policy that is the same durable harm wearing different clothes: a corrupt or
//! truncated tombstone is dropped and the output SSTable is still written, so the
//! rows it shadowed come back.
//!
//! The discriminator there is the CHUNKING STATE the driver already holds, never a
//! byte pattern (issue #28):
//!
//! * a **non-final** chunk may legitimately have cut the marker body in half, so
//!   the answer is `NeedMore` — the one explanation a refill can fix;
//! * on the **final** chunk no further bytes can arrive, so the parse failure is
//!   corrupt/truncated data and is PROPAGATED, carrying the parser's own cause.
//!
//! # Not `Error::ColumnDecode`
//!
//! [`crate::error::Error::ColumnDecode`] exists so the row loops can tell a
//! per-column failure apart from end-of-partition, and the windowed callers
//! (`column_decode_error::indexed_walk_falls_back`) retract the index optimization
//! on it. Neither applies here: the failure is not a column, and it is propagated
//! from the marker arm DIRECTLY rather than through the end-of-partition decision,
//! so no variant match is needed to disambiguate it. It is reported as
//! [`crate::error::Error::Corruption`], wrapping the shadow FSM's own message.

use crate::error::Error;

/// Build the error a partition row loop propagates when a FRAMED range-tombstone
/// marker cannot be represented faithfully (issue #3721), and record the condition
/// at `warn!` — mirroring
/// [`super::column_decode_error::column_decode_failure`], so an operator sees the
/// partition, the marker's offset and the resume point that was discarded even
/// when the caller only surfaces the top-level message.
///
/// `offset` is the marker's own offset; `resume_offset` is the `next_offset` the
/// enclosing `Ok` arm bound — reported precisely because its presence is what
/// proves this is not the end of the partition body.
///
/// `partition` names the partition however the calling loop can: the block loops
/// count partitions within the block and pass an INDEX, while the sliding driver
/// decodes one partition per call and has only its KEY. A `&dyn Display` keeps one
/// helper (and so one message) for both rather than an index the driver would have
/// to invent.
pub(super) fn range_marker_refused(
    cause: Error,
    partition: &dyn std::fmt::Display,
    offset: usize,
    resume_offset: usize,
) -> Error {
    tracing::warn!(
        "V5CompressedLegacy: partition {} range-tombstone marker at offset {} is FRAMED (body \
         continues at offset {}) but cannot be represented: {}",
        partition,
        offset,
        resume_offset,
        cause
    );
    Error::corruption(format!(
        "range-tombstone marker at offset {offset} of partition {partition} could not be \
         represented faithfully; the marker is framed and the partition body continues at offset \
         {resume_offset}, so this is corrupt data and not the end of the partition — truncating \
         here would drop the tombstone AND every later row of the partition from a successful \
         read (issue #3721): {cause}"
    ))
}

/// Build the error a policy PRESERVES in [`super::partition_driver::MarkerOutcome::Unparseable`]
/// when a range-tombstone marker cannot be PARSED at all (issue #3721, roborev
/// job 16) — the sibling of [`range_marker_refused`] for the case where no resume
/// offset exists.
///
/// Deliberately does NOT log: at this point the failure may still be an ordinary
/// window boundary (the marker body straddling a non-final chunk), which is the
/// hot path of a chunk-stitched compaction scan. Only the driver knows whether more
/// data is coming, so the report is emitted there — see
/// [`unparseable_marker_at_final_chunk`]. Constructing the error eagerly is what
/// keeps the parser's own diagnostic (`cause`) available to that decision instead of
/// being discarded and re-synthesised.
pub(super) fn range_marker_unparseable(
    cause: Error,
    partition: &dyn std::fmt::Display,
    offset: usize,
) -> Error {
    Error::corruption(format!(
        "range-tombstone marker at offset {offset} of partition {partition} could not be PARSED, \
         so no resume point exists (issue #3721): {cause}"
    ))
}

/// The buffered counterpart of [`unparseable_marker_at_final_chunk`] (issue
/// #3721, roborev job 78): the whole block is ALREADY materialised in `data`, so
/// finality is established by the BUFFER rather than by a driver's chunking state
/// and no refill can ever complete this marker.
///
/// Composes [`range_marker_unparseable`] so the cause chain and its wording live
/// in one place, and logs at `warn!` — unlike the streaming builder, where the
/// condition may still be an ordinary window boundary, here it is a finding the
/// moment it is constructed.
pub(super) fn unparseable_marker_in_buffered_block(
    cause: Error,
    partition: &dyn std::fmt::Display,
    offset: usize,
) -> Error {
    let inner = range_marker_unparseable(cause, partition, offset);
    tracing::warn!(
        "V5CompressedLegacy: range-tombstone marker is unparseable in a FULLY BUFFERED block, \
         so it cannot be a window boundary: {}",
        inner
    );
    Error::corruption(format!(
        "the whole block is already buffered, so no further data can complete this \
         range-tombstone marker: it is corrupt or truncated, not a chunk boundary. Ending the \
         partition here would report SUCCESS while dropping the tombstone AND every later row \
         of the partition from the read (issue #3721): {inner}"
    ))
}

/// Finish the [`range_marker_unparseable`] error at the point the driver has
/// established that the marker's bytes are ALL the bytes there are: the scan is on
/// its FINAL chunk, so no refill can complete the marker and "unparseable" is
/// corrupt or truncated data rather than a window boundary.
///
/// This is where the condition is reported (`warn!`), mirroring
/// [`range_marker_refused`] — the non-final path builds the same error and drops
/// it, because there it is not yet a finding.
///
/// BOTH streaming policies reach it (roborev job 78), so the message names both
/// harms and asserts neither exclusively: a silently truncated `SELECT` on the
/// read path, and a tombstone missing from WRITTEN output on the compaction path.
///
/// The distinction is taken from the driver's own `at_final_chunk` flag — the
/// chunking state the caller already holds — and never from inspecting bytes to
/// guess whether more data exists (issue #28).
pub(super) fn unparseable_marker_at_final_chunk(cause: Error) -> Error {
    tracing::warn!(
        "V5CompressedLegacy: range-tombstone marker is unparseable on the FINAL chunk of the \
         scan, so it cannot be a window boundary: {}",
        cause
    );
    Error::corruption(format!(
        "the scan is at its FINAL chunk, so no further data can complete this range-tombstone \
         marker: it is corrupt or truncated, not a chunk boundary. Completing the partition here \
         would report SUCCESS while dropping the tombstone AND every later row of the partition — \
         from a `SELECT` on the read path, and from WRITTEN output on the compaction path, where \
         the rows it shadows come back durably, on disk (issue #3721): {cause}"
    ))
}
