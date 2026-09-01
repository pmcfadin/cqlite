//! What a partition row loop does when a range-tombstone MARKER is framed but
//! cannot be represented faithfully (issue #3721).
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
