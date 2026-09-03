//! Issue #3928 — the ONE partition-HEADER arm of the block-emit walk.
//!
//! Split out of `block_emit_windowed.rs` (campsite rule, epic #1116) when that
//! arm gained a tolerance DECISION: it now answers three different things
//! (a decoded header, the end of a window, a resynchronisation) and the choice
//! between the last two rests on the caller's [`BufferExtent`], so it is a
//! responsibility rather than a few inline bounds checks.
//!
//! # What changed, and why the tolerant answer was unsound
//!
//! Both arms used to `tracing::warn!` and then `offset += 1` unconditionally.
//! On a buffer the caller has PROVEN complete that has two consequences, and the
//! second is the bad one:
//!
//! 1. the malformed partition is silently DROPPED, and
//! 2. the one-byte resync can land on MISALIGNED bytes that parse as a plausible
//!    header — so the walk INVENTS a partition that does not exist.
//!
//! Measured on a real Cassandra 5.0 `da` fixture with ONE compressed byte
//! flipped so the first partition's `DeletionTime` discriminator reads `0xFF`
//! (which Cassandra's own `DeletionTime.Serializer.deserialize` throws on,
//! cassandra-5.0.8 `DeletionTime.java:222-230`): `distinct_partition_keys`
//! answered `Ok` with **5** partition keys where the pristine fixture has **3**
//! — 2 FABRICATED — and `get_all_entries` answered `Ok` with **0** of 468 rows.
//! Both are reported as success. The count moves in BOTH directions, which is
//! why no count-based check and no total-comparing parity oracle can see this.
//!
//! `BufferExtent::Complete` is the authoritative discriminator, exactly as it is
//! for the row arm #3782 fixed: no further bytes can arrive, so a header that
//! does not decode is truncation or corruption and both are DATA LOSS. On a
//! `Window` the SAME condition is the ordinary straddle — a header split across
//! the chunk boundary — and stays tolerant.

use super::super::*;

/// What the block-emit walk should do with the bytes at a candidate partition
/// start.
pub(super) enum HeaderStep {
    /// A structurally-valid header: the partition key, the offset of the first
    /// row/marker, and the partition-level deletion.
    Parsed(RowKey, usize, Option<(i64, i32)>),
    /// Fewer bytes remain than even a header needs. Only reachable on a
    /// [`BufferExtent::Window`] (a `Complete` buffer refuses instead), where it
    /// is the ordinary end of the window.
    EndOfBlock,
    /// The header is undecodable and more bytes may still arrive, so the caller
    /// skips ONE byte and re-tries. Only reachable on a
    /// [`BufferExtent::Window`].
    Resync,
}

impl V5CompressedLegacyParser {
    /// Decode the partition header at `offset`, or say what the walk should do
    /// instead.
    ///
    /// Tolerance is decided by TWO caller-stated facts and nothing else:
    ///
    /// * `extent` — can more bytes still arrive? The parse cannot know, so the
    ///   caller states it (see [`BufferExtent`]'s contract, which forbids a
    ///   default).
    /// * `bounded` — was this walk asked to stop SHORT of the extent? A #954
    ///   clustering-slice walk (`row_body_window`) stops its row loop at
    ///   `body_end`, mid-partition by construction, and the outer partition loop
    ///   then re-enters here at bytes that were never claimed to begin a
    ///   partition. Only an UNBOUNDED walk over a complete buffer can hold the
    ///   bytes at `offset` to that promise, so only there is an undecodable
    ///   header proof of corruption.
    ///
    /// Every production caller that passes a `row_body_window` today also passes
    /// [`BufferExtent::Window`] (`big_promoted.rs`, `bti_point.rs`), so `bounded`
    /// changes nothing for them; it is what keeps the `Complete`-plus-window
    /// combination — which nothing forbids, and which
    /// `regression_1741c_tests.rs` uses — from being refused on correct input.
    pub(super) fn block_partition_header(
        &self,
        data: &[u8],
        offset: usize,
        extent: BufferExtent,
        bounded: bool,
        partition_index: usize,
    ) -> Result<HeaderStep> {
        // The ONE tolerance predicate, named once and read twice below.
        let refuse = extent.is_complete() && !bounded;
        // Cassandra partition key size limits (used in header validation)
        // - CASSANDRA_MAX_KEY_SIZE: 64KB limit per Apache Cassandra specification
        // - FORMAT_MAX_KEY_SIZE: u8 max value - V5CompressedLegacy format limitation
        const CASSANDRA_MAX_KEY_SIZE: usize = 65536; // 64KB per Cassandra spec
        const FORMAT_MAX_KEY_SIZE: usize = 255; // u8 max value - format limitation

        // CRITICAL FIX (Issue #164): validate the partition header format before
        // attempting the parse.
        //
        // Most compressed blocks contain EXACTLY ONE partition. After parsing the
        // first partition's row data and trailing VInt we must NOT assume there is
        // another partition just because `offset < data.len()`.
        //
        // Partition header format:
        // - Byte 0: flags (typically 0x00, sometimes partition-level flags)
        // - Byte 1: partition key length (u8, NOT a VInt)
        // - Bytes 2+: partition key data
        //
        // NOTE: no heuristic validation of `flags` (issues #258, #28).
        // `remaining` rather than `offset + N > data.len()`: the caller's loop
        // guarantees `offset < data.len()`, but a saturating subtraction states
        // each bound below without an addition a reader has to check.
        let remaining = data.len().saturating_sub(offset);
        if remaining < 2 {
            // Fewer bytes remain than ANY header needs. Whether that is an
            // ordinary end of walk or DATA LOSS is decided by the same `refuse`
            // predicate as every other answer here — and it must be, because
            // this arm's two tolerant readings are both about the CALLER's
            // situation and neither is about the byte count:
            //
            // * on a `Window` the header continues in the next chunk;
            // * on a BOUNDED walk (`row_body_window`) the walk was asked to stop
            //   short of the extent, so these bytes were never claimed to begin
            //   a partition.
            //
            // On an UNBOUNDED, proven-complete walk neither reading is
            // available: the file ends here, so a surviving header stub is the
            // evidence that the partition Cassandra wrote was truncated away —
            // exactly as a truncated ROW's stub is under #3782. Reporting `Ok`
            // there answered a query with one fewer partition than the file was
            // written with, and it did so INCONSISTENTLY: the sibling
            // cell-metadata walk (`block_emit.rs`) has no such carve-out, so
            // `SELECT *` answered `Ok` and `SELECT *, WRITETIME(c)` answered
            // `Err` on the same bytes (`data_access/mod.rs:249` vs `:288` hand
            // both walks the SAME stitched `Complete` buffer). Pinned by
            // `issue_3928_corrupt_header_refusal.rs`'s
            // `both_stitched_walks_agree_and_refuse_a_truncated_final_header`.
            if refuse {
                return Err(undecodable_partition_header(
                    offset,
                    &format!(
                        "only {remaining} byte(s) remain, fewer than the 2 any header needs \
                         (partition={partition_index}){}",
                        structural_note(data[offset])
                    ),
                ));
            }
            tracing::debug!(
                "V5CompressedLegacy: Not enough bytes for partition header at offset \
                 {offset} (need 2, have {remaining}), stopping"
            );
            return Ok(HeaderStep::EndOfBlock);
        }

        let flags = data[offset];
        let key_len = data[offset + 1] as usize;

        // VG3: oa format (hasUIntDeletionTime) uses a compact DeletionTime:
        //   LIVE = 1 byte; DELETED = 12 bytes. The minimum is therefore 1 byte.
        // nb format always uses 12 bytes (4 + 8).
        let deletion_time_min = if self.has_uint_deletion_time() { 1 } else { 12 };
        let header_min_size = 1 + 1 + key_len + deletion_time_min;
        if key_len == 0
            || key_len > FORMAT_MAX_KEY_SIZE.min(CASSANDRA_MAX_KEY_SIZE)
            || header_min_size > remaining
        {
            let detail = format!(
                "CQLite READ flags=0x{flags:02x} and a key length of {key_len} here, needing \
                 {header_min_size} bytes with {remaining} available (partition={partition_index})\
                 {}",
                structural_note(flags)
            );
            if refuse {
                return Err(undecodable_partition_header(offset, &detail));
            }
            tracing::warn!(
                "V5CompressedLegacy: Skipping malformed partition header at offset {offset} \
                 ({detail})"
            );
            return Ok(HeaderStep::Resync);
        }

        match self.parse_partition_header_full(data, offset) {
            Ok((key, next_offset, deletion)) => Ok(HeaderStep::Parsed(key, next_offset, deletion)),
            Err(e) => {
                // Issue #3928: the error's own kind is preserved and returned on
                // a proven-complete buffer — the same rule, and the same reason,
                // as the row arm's `return Err(e)` (#3782).
                //
                // This arm composes NO message: `e` is `scan_partition_header`'s
                // own, and under the validation above it can realistically only
                // be the `oa`/`da` invalid-IS_LIVE-byte rejection, which does
                // not rest on the key-length model. Its two key-length messages
                // are already excluded here (a zero or over-long length, and a
                // key running past the buffer, are what the validation just
                // checked), and rewording THEM belongs to **#3999** with the
                // model they assert — see `undecodable_partition_header`.
                if refuse {
                    return Err(e);
                }
                tracing::warn!(
                    "V5CompressedLegacy: Failed to parse partition header at offset {offset} \
                     (partition={partition_index}): {e}. Attempting to continue to next partition."
                );
                Ok(HeaderStep::Resync)
            }
        }
    }
}

/// The refusal a partition header raises where the caller has PROVEN no further
/// bytes can arrive.
///
/// Deliberately NOT shared with `partition_driver.rs` /
/// `compaction_stream.rs`, which reach the same decision from `at_final_chunk`
/// and own their own `ParseStep`/`PartitionStreamStep` vocabularies — the same
/// call the #3782 row arm made when it spelled the rule out in each driver
/// rather than inventing one abstraction over three different advance
/// protocols. What must not diverge is the DECISION, and that is one predicate
/// per driver over its own authoritative signal.
fn undecodable_partition_header(offset: usize, detail: &str) -> Error {
    Error::corruption(format!(
        "V5CompressedLegacy: refusing an undecodable partition header at offset {offset} \
         ({detail}); this walk is UNBOUNDED over a PROVEN-COMPLETE buffer, so no further \
         bytes can arrive to finish this header. Skipping a byte to resynchronise would \
         DROP this partition and can land on misaligned bytes that parse as a plausible \
         header, INVENTING a partition that does not exist (issue #3928). \
         NOTE (#3999): CQLite reads the partition key length as ONE byte after a 'flags' \
         byte, while Cassandra writes an unsigned 2-byte big-endian length and no flags \
         byte (SortedTablePartitionWriter.start -> ByteBufferUtil.writeWithShortLength), \
         so the two models agree only for keys under 256 bytes; for a longer key this \
         refusal may be CQLite's model and not corruption."
    ))
}

/// Issue #3928 fix round 1, I2 — name the leading byte's STRUCTURAL class when it
/// is one this arm should never have been handed.
///
/// The row loop breaks with `offset` still ON a range-tombstone marker it could
/// not parse or feed (`block_emit_windowed.rs`'s three marker breaks), and the
/// outer partition loop then re-enters HERE. Without this the diagnostic reads
/// "undecodable partition header" for a MARKER failure and sends the next reader
/// to the wrong arm.
///
/// Diagnostics only: the refusal itself is unchanged, and moving the marker
/// breaks to report their own failure is a different arm and a different issue.
fn structural_note(leading: u8) -> &'static str {
    if V5CompressedLegacyParser::is_end_of_partition(leading) {
        " — NOTE: this leading byte is an END_OF_PARTITION marker (0x01), so the walk \
         re-entered the header arm at a marker rather than at a partition start"
    } else if V5CompressedLegacyParser::is_range_tombstone_marker(leading) {
        " — NOTE: this leading byte carries IS_MARKER, i.e. it is a RANGE-TOMBSTONE \
         marker and not a partition header. The previous partition's row loop ended at a \
         marker it could not parse or feed, leaving the cursor on it; the failure to \
         investigate is in the MARKER arm, not here"
    } else {
        ""
    }
}
