//! What row assembly does when ONE column fails to decode (issue #3721).
//!
//! # The defect this module replaces
//!
//! [`super::row_data`]'s per-column loop had exactly one error handler per column
//! path, and both were a bare `break`:
//!
//! * the COMPLEX arm — shared by `parse_complex_column_inner` (the compaction /
//!   elements-out read, under `if compaction_complex_out.is_some()`) and
//!   `parse_complex_column` (the user-facing read), which are merely the two arms
//!   producing one `parse_result` binding; and
//! * the SIMPLE arm, whose comment (`// CRITICAL FIX: Stop parsing remaining
//!   columns …`) was honest about the mechanism and silent about the consequence.
//!
//! `break` leaves the loop, and the row is then assembled from the cells gathered
//! SO FAR and returned as `Ok`. So the failing column **and every later on-disk
//! column** silently vanished while the read reported SUCCESS. Measured at the
//! public surface over committed Cassandra fixtures before the fix:
//!
//! ```text
//! test_da.simple_table,  `name` read as INET     -> Ok, 3 rows, each missing
//!                                                   `name` AND `salary`
//! test_signed_coll.signed_int_collections,
//!                        `m` key read as BIGINT  -> Ok, ZERO rows
//! ```
//!
//! A successful `SELECT` with missing columns is the worst shape a read defect can
//! take: it is indistinguishable from a row that legitimately has no value there,
//! so nothing downstream can defend against it. It also silenced every corruption
//! check the decoders make on Cassandra's authority (a non-4/16-byte `inet`, a
//! wrong fixed width, a composite whose components do not consume the slice): the
//! check fired, the row truncated, and the query reported success.
//!
//! # The rule: PROPAGATE. Which class goes where, and why nothing is skippable
//!
//! `break` conflated two different facts — "this column's value failed to decode,
//! but the offset is known and the rest of the row is intact" (which could in
//! principle be skipped) and "we cannot advance the offset safely" (which cannot).
//! At these call sites the SECOND is the only one that can be established, for
//! both paths and for every error class:
//!
//! * **Complex columns.** Neither `parse_complex_column` nor
//!   `parse_complex_column_inner` returns an offset alongside its `Err`, so a
//!   framing failure (a bad vint, an implausible cell count, a truncated cell) and
//!   a value failure (a cell-path key or element that does not decode as its
//!   declared type) are indistinguishable from here, and neither yields a resume
//!   point.
//! * **Simple columns.** Sharper still: a simple cell's FRAMING is
//!   type-dependent — a fixed-width type consumes its width with no length prefix,
//!   a variable-width type reads a vint length first — so when the decode fails
//!   the number of bytes the cell occupies is exactly what is not known.
//!
//! Cassandra's own serializer offers no way to derive a resume offset from a
//! failed decode, and this repository does not invent one (no-heuristics, issue
//! #28). Cassandra does not serve a short row either: a cell it cannot read raises
//! out of `UnfilteredSerializer` and the read fails. So every class here is FATAL
//! and is reported as [`Error::ColumnDecode`].
//!
//! Marking the column and continuing — the other outcome issue #3721 allows — is
//! not available without a public `Value` marker variant for "present on disk,
//! undecodable", which is a public-type change well outside a read-path fix. If
//! one is ever added, the complex arm is where a skippable class could be carved:
//! its framing (`skip_complex_cell`) is type-INDEPENDENT and derived from
//! `UnfilteredSerializer`, so a value-only failure there does have a recoverable
//! resume point. The simple arm still would not.
//!
//! # Why the row/partition loops must MATCH, not read messages
//!
//! The loops above the decoder ([`super::block_emit`],
//! [`super::block_emit_windowed`], and the [`super::partition_driver`] policies)
//! deliberately treat an ordinary row-parse `Err` as *end of the partition body*:
//! a well-formed partition's last row is followed by bytes that do not parse as
//! another row, and that is how those loops detect the end. A per-column decode
//! failure is NOT that condition. The only sound way to tell the two apart is a
//! match on the dedicated [`Error::ColumnDecode`] variant — inspecting message
//! text would be exactly the byte/string-pattern inference issue #28 forbids.
//! [`end_of_partition_or_bail`] is that single decision, written once.

use crate::error::{Error, Result};

/// Build the [`Error::ColumnDecode`] a row-assembly column handler propagates,
/// and record the condition at `warn!` — so a `tracing::debug!` is no longer the
/// only trace of it, and an operator sees the column, its type and the offset even
/// when the caller only surfaces the top-level message.
///
/// `column_type` is the type the decode was driven from: the AUTHORITATIVE on-disk
/// SerializationHeader marshal type where the header carries one, else the
/// supplied schema's declared type (issue #1081) — both authoritative metadata,
/// never guessed from bytes.
pub(super) fn column_decode_failure(
    column_name: &str,
    column_type: &str,
    offset: usize,
    cause: Error,
) -> Error {
    tracing::warn!(
        "V5CompressedLegacy: column '{}' ({}) at offset {} FAILED to decode: {}",
        column_name,
        column_type,
        offset,
        cause
    );
    Error::column_decode(column_name, column_type, offset, cause)
}

/// Is the byte at a simple column's cell position NOT a cell flags byte — i.e. is
/// there no cell to decode here at all?
///
/// **This is the one class that is FRAMING rather than a decode failure, and it is
/// therefore the one class that keeps the historical `break`.** Cassandra's cell
/// flags occupy the low 5 bits (`Cell.Serializer` in `UnfilteredSerializer`, read
/// at the pinned `cassandra-5.0.8` tag); `0x20`/`0x40`/`0x80` are ROW flags, so a
/// byte carrying any of them cannot be a cell — the statement is "there is no cell
/// here", not "this column's value is undecodable". It is the row walk's END-OF-
/// CELLS terminator, and CQLite's SPECULATIVE reads depend on it: the promoted-
/// index reverse read and the clustering-window read
/// (`data_access::big_promoted`) hand `parse_block_emit_windowed` a block extent
/// and let it parse rows from it, and a walk that runs past real row data lands in
/// cell values and is rejected exactly here. Measured: making this class fatal
/// turned a CLEAN read of the committed `test_comp.uncompressed_table` fixture
/// into a hard error (`invalid cell flags 0x61` — an ASCII byte from a `text`
/// value, inside a row body the header claimed was 97 bytes long).
///
/// That speculative walk over-running into a value IS a latent defect of those
/// read paths, but it is NOT this issue's, and converting it into a user-visible
/// read failure would be a large availability regression for wide-partition reads.
/// It stays the terminator; it is reported at `debug!` because it fires on healthy
/// reads of those paths, so a higher level would be pure noise.
///
/// Everything AFTER a valid flags byte — a wrong fixed width, a non-4/16-byte
/// `inet`, invalid UTF-8, a short value, an undecodable cell-path key — is a
/// genuine per-column decode failure and propagates as [`Error::ColumnDecode`].
pub(super) fn not_a_cell(column_name: &str, offset: usize, flags: u8) -> bool {
    let not_a_cell = flags > 0x1F;
    if not_a_cell {
        tracing::debug!(
            "V5CompressedLegacy: no cell at offset {} for column '{}': byte 0x{:02x} \
             carries a ROW flag bit (0x20/0x40/0x80), so the row body's cells end here",
            offset,
            column_name,
            flags
        );
    }
    not_a_cell
}

/// Is `e` the per-column decode failure that must never be mistaken for the end of
/// a partition body? A MATCH on the variant, never a message-text test (issue #28).
pub(crate) fn is_column_decode(e: &Error) -> bool {
    matches!(e, Error::ColumnDecode { .. })
}

/// The ONE place a block/partition row loop decides what a row-parse `Err` means.
///
/// * [`Error::ColumnDecode`] — a framed row whose column would have been dropped.
///   Returned as `Err` so it reaches the caller of the read instead of silently
///   ending the partition (issue #3721).
/// * anything else — the ordinary "no further row parses here" signal that marks
///   the end of the partition body. Logged exactly as before and reported as
///   `Ok(())`, so the caller continues to `break` out of its row loop.
pub(super) fn end_of_partition_or_bail(
    e: Error,
    partition_index: usize,
    row_count: usize,
    offset: usize,
) -> Result<()> {
    if is_column_decode(&e) {
        return Err(e);
    }
    tracing::debug!(
        "V5CompressedLegacy: Partition {} ended after {} rows at offset {}: {}",
        partition_index,
        row_count,
        offset,
        e
    );
    if row_count == 0 {
        // Not one row parsed: worth an error-level line, as before.
        tracing::error!(
            "V5CompressedLegacy: Partition {} - Failed to parse first row at offset {}: {}",
            partition_index,
            offset,
            e
        );
    }
    Ok(())
}
