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
    /// `extent` decides the tolerance and NOTHING else does: the parse cannot
    /// know whether more bytes can arrive, so the caller states it (see
    /// [`BufferExtent`]'s contract, which forbids a default).
    pub(super) fn block_partition_header(
        &self,
        data: &[u8],
        offset: usize,
        extent: BufferExtent,
        partition_index: usize,
    ) -> Result<HeaderStep> {
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
        if offset + 2 > data.len() {
            // Issue #3928: on a WINDOW this is the tail of the chunk and the
            // header continues in the next one. On a PROVEN-COMPLETE buffer
            // there is no next one, so trailing bytes that cannot even begin a
            // header are corruption — a Cassandra `Data.db` is exactly a
            // concatenation of partitions with no padding
            // (`SortedTableWriter.append`, cassandra-5.0.8), so `offset` here is
            // either `data.len()` (handled by the caller's loop condition) or a
            // partial header nothing can complete.
            return if extent.is_complete() {
                Err(undecodable_partition_header(
                    offset,
                    &format!(
                        "only {} byte(s) remain, fewer than the 2 a header needs \
                         (partition={partition_index})",
                        data.len() - offset
                    ),
                ))
            } else {
                tracing::debug!(
                    "V5CompressedLegacy: Not enough bytes for partition header at offset {} \
                     (need 2, have {}), stopping",
                    offset,
                    data.len() - offset
                );
                Ok(HeaderStep::EndOfBlock)
            };
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
            || offset + header_min_size > data.len()
        {
            let detail = format!(
                "flags=0x{flags:02x}, key_len={key_len}, need {header_min_size} bytes, \
                 have {}, partition={partition_index}: header validation failed",
                data.len() - offset
            );
            if extent.is_complete() {
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
                if extent.is_complete() {
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
        "V5CompressedLegacy: undecodable partition header at offset {offset} ({detail}); \
         the buffer is PROVEN COMPLETE, so no further bytes can arrive to finish this \
         header. Skipping a byte to resynchronise would DROP this partition and can land \
         on misaligned bytes that parse as a plausible header, INVENTING a partition that \
         does not exist (issue #3928)"
    ))
}
