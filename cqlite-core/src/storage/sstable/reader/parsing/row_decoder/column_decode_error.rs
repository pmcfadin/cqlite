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
//!
//! # The cell-flags byte: NOT a terminator (issue #3721, roborev blocker 1)
//!
//! An earlier revision of this module kept a `not_a_cell` predicate that treated a
//! simple column's flags byte `> 0x1F` as an END-OF-CELLS marker and `break`ed the
//! column loop — i.e. reported a short row as a SUCCESSFUL read, the very defect
//! this module exists to remove. **No such marker exists in the format.** At
//! `cassandra-5.0.8`, `db/rows/UnfilteredSerializer.deserializeRowBody` fixes the
//! column set BEFORE any cell is read (`hasAllColumns ? headerColumns :
//! Columns.serializer.deserializeSubset(headerColumns, in)`) and then iterates
//! exactly that set; cell reading is bounded by the columns bitmap / subset
//! encoding, never by a sentinel flags value. `db/rows/Cell.Serializer` defines
//! five bits only (`0x01|0x02|0x04|0x08|0x10` = `0x1F`).
//!
//! So a `> 0x1F` byte at a cell position is evidence the CURSOR is wrong, and the
//! cell decoder rejects it as corruption (`cell_value.rs`, issue #191) — which
//! becomes an [`Error::ColumnDecode`] like every other per-column failure. The
//! class that used to hide behind the terminator (a SPECULATIVE, index-positioned
//! walk over-running a real row boundary) is handled where it belongs, at the
//! callers that chose the optimization: see [`end_of_partition_or_bail`].

//! # Where an INDEX-POSITIONED walk must stop, and why (issue #3721)
//!
//! `block_emit_windowed`'s `row_body_window` is a byte extent inside the FIRST
//! partition's row body, resolved from that partition's authoritative row index,
//! and it is applied to `partition_index == 0` alone. A windowed walk therefore
//! covers that partition and STOPS.
//!
//! Continuing costs CORRECTNESS, not just work. Stopping the row loop at
//! `body_end` leaves the cursor in the MIDDLE of a row — a row-index block extent
//! is a byte bound, not a row bound — and the try-parse partition detector (issue
//! #166, deliberately no byte-pattern heuristics) then reads a "partition header"
//! out of a cell value and decodes a phantom partition from it. MEASURED on the
//! #1184 fixture: the promoted-index reverse block walk stopped at `body_end`
//! 461014, peeked a partition header there, and decoded a row whose `payload` cell
//! flags byte was `0x70` — the ASCII `p` of the text value it was standing in
//! (`partition_index = 1`, `row_count = 0`).
//!
//! That phantom parse was harmless only while its failure was swallowed. Once a
//! per-column decode failure is reported, as it must be, an over-run reads as a
//! corrupt SSTable. Not entering the phantom partition is the fix;
//! [`indexed_walk_falls_back`] remains as defence in depth for any over-run this
//! does not prevent. Every windowed caller keeps only the target partition's rows,
//! so nothing is lost.

use crate::error::{Error, Result};

/// Build the [`Error::ColumnDecode`] a row-assembly column handler propagates,
/// and record the condition at `warn!` — so a `tracing::debug!` is no longer the
/// only trace of it, and an operator sees the column, its type and the offset even
/// when the caller only surfaces the top-level message.
///
/// `column_type` comes from [`dispatch_type`] and is the type the decode was
/// actually DRIVEN FROM — see there for why that is not always the on-disk header
/// marshal type.
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

/// The row body ran out of BYTES while the row's own column set still names a
/// column as PRESENT (issue #3721). Reported as [`Error::ColumnDecode`] like every
/// other per-column failure, so the row/partition loops route it through
/// [`end_of_partition_or_bail`] rather than mistaking it for end-of-partition.
///
/// # Why this is truncation and not the legitimate end of the row
///
/// Cassandra basis, `cassandra-5.0.8`
/// `db/rows/UnfilteredSerializer.deserializeRowBody`: the column set is fixed
/// BEFORE a single cell is read —
///
/// ```java
/// Columns columns = hasAllColumns ? headerColumns
///                                 : Columns.serializer.deserializeSubset(headerColumns, in);
/// columns.apply(column -> { ... readSimpleColumn / readComplexColumn ... });
/// ```
///
/// — and iteration runs over exactly that set. Exhausting the input mid-set raises
/// out of the serializer; there is no short-row form and no end-of-cells sentinel
/// (see the module note above). So this condition is corrupt or truncated data.
///
/// # The legitimate end of data is DISTINGUISHABLE here, structurally
///
/// [`super::row_data`]'s loop evaluates this condition only AFTER its
/// `if !is_present(col_idx) { continue; }` skip, so every column reaching it is one
/// the row's missing-columns bitmap declares has cell bytes on disk — and a cell is
/// never zero bytes (it always carries at least its flags byte, `db/rows/Cell`'s
/// five-bit `Serializer`). A row whose column set IS fully consumed leaves the
/// `for` loop normally and never evaluates this condition at all. "No more columns
/// expected" and "bytes exhausted with columns outstanding" are therefore different
/// control-flow paths, not two readings of one predicate — nothing is guessed.
///
/// The `break` this replaces logged `parsed {}/{} on-disk cells`, i.e. ADMITTED
/// decoding fewer cells than the column set names, and then let row assembly return
/// `Ok` with this column and every successor silently absent.
///
/// # The bound is the ROW's end, not the parse unit's (issue #3721 / #3809)
///
/// `row_end` is this row's authoritative end — `(row_metadata_offset +
/// row_size_vint_len) + row_size`, Cassandra's `getFilePointer()` arithmetic (issue
/// #237) — and NOT `data.len()`. The parse unit holds every following row and
/// partition, so a corrupt row declaring a SHORT `row_size` leaves outstanding
/// columns plenty of readable bytes: bounding on `data.len()` let them decode out of
/// the NEXT row and return plausible garbage as this row's values. `row_end` is
/// proven `<= data.len()` before the column loop (the #2481 `row_size > available`
/// rejection), so this bound is strictly tighter and can never admit a read the
/// looser one refused. `data_len` is still REPORTED, because "the row claimed bytes
/// it does not own" and "the file ends here" are different operator situations and
/// the pair distinguishes them.
///
/// `progress` is `(col_idx, on_disk_column_count, cells_decoded)` and `bounds` is
/// `(offset, row_end, data_len)`; all are reported so the failure names where the
/// body stopped, where the row ends, and how much of the column set was consumed.
pub(super) fn row_body_exhausted(
    column: &crate::schema::Column,
    header_type: Option<&str>,
    bounds: (usize, usize, usize),
    progress: (usize, usize, usize),
) -> Error {
    let (offset, row_end, data_len) = bounds;
    let (col_idx, column_count, cells_decoded) = progress;
    let ty = dispatch_type(&column.data_type, header_type);
    let cause = Error::corruption(format!(
        "row body exhausted at offset {offset} (this row's body ends at {row_end}; the parse \
         unit is {data_len} bytes) with on-disk column {col_idx} of {column_count} ('{}') still \
         declared PRESENT by the row's column set; {cells_decoded} cell(s) decoded — truncated \
         or corrupt row body",
        column.name
    ));
    column_decode_failure(&column.name, &ty, offset, cause)
}

/// A column's decoder returned an offset PAST the row's authoritative end — so it
/// read bytes belonging to the NEXT row or partition, and the value it produced is
/// not this row's data (issue #3721 / #3809).
///
/// # Why this is the dangerous shape, and why it is FATAL
///
/// A corrupt row that declares a SHORT `row_size` and is followed by more data
/// leaves the cell decoders with plenty of readable bytes: they are handed the
/// whole materialised parse unit, so a cell whose length prefix or fixed width
/// runs past the row boundary decodes SUCCESSFULLY out of the following row's
/// bytes. Framing stays intact — row assembly's `next_offset` derives from the
/// authoritative `row_size`, never from where the columns stopped — so the walk
/// does not desynchronise and nothing downstream looks wrong. The read returns a
/// plausible garbage value and reports SUCCESS, which is strictly worse than a
/// failure: a wrong answer is indistinguishable from a right one.
///
/// The row's authoritative end is `(row_metadata_offset + row_size_vint_len) +
/// row_size` — Cassandra's own `getFilePointer()` arithmetic (issue #237), the same
/// value row assembly returns as `next_offset`. A cell may not cross it: at
/// `cassandra-5.0.8`, `db/rows/UnfilteredSerializer.deserializeRowBody` reads the
/// row body under that bound, so a cell extending past its own row's extent cannot
/// occur in well-formed data.
///
/// Nothing here can recover: the bytes consumed past the boundary belong to another
/// row, so neither the value nor a resume point inside this row is knowable.
/// Reported as [`Error::ColumnDecode`] like every other per-column failure, so the
/// row/partition loops route it through [`end_of_partition_or_bail`] instead of
/// mistaking it for end-of-partition.
///
/// `bounds` is `(column_start, returned_offset, row_end)`; `progress` is
/// `(col_idx, on_disk_column_count)`.
pub(super) fn column_escaped_row_bound(
    column: &crate::schema::Column,
    header_type: Option<&str>,
    bounds: (usize, usize, usize),
    progress: (usize, usize),
) -> Error {
    let (column_start, returned_offset, row_end) = bounds;
    let (col_idx, column_count) = progress;
    let ty = dispatch_type(&column.data_type, header_type);
    let cause = Error::corruption(format!(
        "on-disk column {col_idx} of {column_count} ('{}') decoded from offset {column_start} \
         and returned offset {returned_offset}, PAST the end of its own row body at {row_end} \
         (from the row header's authoritative row_size) — the decoder consumed bytes belonging \
         to the next row or partition, so the value it produced is not this row's data; \
         truncated or corrupt row body",
        column.name
    ));
    column_decode_failure(&column.name, &ty, column_start, cause)
}

/// The per-column walk finished SHORT of the row's authoritative end: bytes inside
/// this row body were accounted for by no column the row declares present (issue
/// #3721 / #3809). The exact converse of [`row_body_exhausted`], and the other half
/// of [`column_escaped_row_bound`].
///
/// # Why a deficit cannot be tolerated
///
/// `row_size` is authoritative and the row loops advance by it, so an unconsumed
/// remainder never desynchronises the walk — which is exactly why it is dangerous:
/// the row is returned as `Ok`, with cells decoded from a cursor that was
/// misaligned somewhere inside the body, and nothing says so. In well-formed data
/// the columns TILE the body exactly: at `cassandra-5.0.8`,
/// `db/rows/UnfilteredSerializer.deserializeRowBody` fixes the column set before
/// the first cell and writes nothing after the last one — there is no trailing
/// field and no padding, the next row starts immediately (issue #237).
///
/// # Every legitimately non-advancing path is accounted for, structurally
///
/// * a column the row's missing-columns bitmap declares ABSENT is `continue`d
///   before any decode and has NO bytes on disk (the bitmap IS Cassandra's
///   `Columns.serializer` subset encoding; an absent column is not written);
/// * a DROPPED column (issue #1080) — on disk, absent from the supplied schema — IS
///   decoded and its offset IS advanced; only the emit is suppressed;
/// * the read-side shadow / TTL drop (issue #1741) and the `collection_absent`
///   emptied-collection case both advance the offset first and suppress the emit
///   only;
/// * a pure row tombstone with no cell bytes returns long before this check.
///
/// So no legitimate path leaves the cursor short, and a deficit is corrupt or
/// truncated data.
///
/// # Attribution: the column SET, not one column
///
/// WHICH column under-consumed is not knowable here — consumption is reported only
/// by each decoder, and every one of them reported success. `last_column` is
/// therefore the column the walk ENDED on (the last one decoded), named so an
/// operator has a starting point, and the message says outright that the deficit
/// belongs to the row's column set as a whole. Nothing is inferred from the bytes
/// (issue #28).
///
/// Reported as [`Error::ColumnDecode`] because that is the variant the row and
/// partition loops match on to tell a row-body failure from end-of-partition
/// ([`end_of_partition_or_bail`]); an `Error::Corruption` here would be folded into
/// "end of partition" and would silently truncate the partition — the very defect
/// this module exists to remove.
pub(super) fn row_body_under_consumed(
    last_column: Option<(&str, &str)>,
    bounds: (usize, usize),
    progress: (usize, usize),
) -> Error {
    let (offset, row_end) = bounds;
    let (column_count, cells_decoded) = progress;
    let (name, ty) = last_column.unwrap_or(("<no column decoded>", "<row body>"));
    let cause = Error::corruption(format!(
        "row body under-consumed: the {column_count} on-disk column(s) this row declares left \
         the cursor at offset {offset} but the row body ends at {row_end} (from the row header's \
         authoritative row_size), leaving {} byte(s) inside this row accounted for by no column; \
         {cells_decoded} cell(s) decoded, the walk ended on column '{name}' — the deficit \
         belongs to the row's column set as a whole, and which column under-consumed is not \
         knowable here; truncated or corrupt row body",
        row_end.saturating_sub(offset)
    ));
    column_decode_failure(name, ty, offset, cause)
}

/// Is `e` the per-column decode failure that must never be mistaken for the end of
/// a partition body? A MATCH on the variant, never a message-text test (issue #28).
pub(crate) fn is_column_decode(e: &Error) -> bool {
    matches!(e, Error::ColumnDecode { .. })
}

/// The type string [`Error::ColumnDecode`] reports: the type the failed decode was
/// DISPATCHED ON, naming the on-disk header marshal type alongside it whenever the
/// two differ (issue #3721, roborev blocker 3).
///
/// Reporting the header type alone MISIDENTIFIES the cause, and the misreport is
/// worst exactly where the error matters most — a mis-declared schema. Both column
/// paths dispatch on the SUPPLIED schema type for a matched column:
///
/// * **simple** — `parse_cell_value_schema_order` decodes on `ColumnToParse.kind`,
///   which `RowColumnResolution::build` resolves from
///   `schema.map(data_type).unwrap_or(header_type)`, i.e. the supplied type when
///   the column matched and the header marshal type only for a DROPPED column. In
///   the row loop that value IS `column.data_type` in both cases (a dropped
///   column's synthetic `Column` is built FROM the header type). So a `text` column
///   read as a supplied `INET` used to be reported as its on-disk `UTF8Type`, which
///   points a reader at the data rather than at the declaration that is wrong.
/// * **complex** — the CONTAINER is decoded from the header marshal type
///   (`complex_type`, authoritative for `UserType(...)`, issue #1081) but the
///   cell-path KEY and the ELEMENT are decoded from the supplied schema type
///   (`parse_complex_column_inner`'s `column`). A map whose key is mis-declared
///   `BIGINT` therefore fails on the SUPPLIED type while the header describes the
///   real on-disk `int` key, and the element/key type the caller has to fix appears
///   only in the former (measured: `map<BIGINT, TEXT> (on-disk map<int, text>)`).
///
/// Naming both when they differ is what makes the report complete: the declared
/// type identifies the dispatch that failed (including its key/element parameters),
/// the header type identifies the bytes it was pointed at. Both are authoritative
/// metadata; neither is inferred from the bytes (issue #28).
pub(super) fn dispatch_type(declared: &str, header: Option<&str>) -> String {
    match header {
        Some(h) if h != declared => format!("{declared} (on-disk {h})"),
        _ => declared.to_string(),
    }
}

/// The ONE response an INDEX-POSITIONED read may make to [`Error::ColumnDecode`]:
/// **abandon the optimization and re-read without the window** (issue #3721,
/// roborev blocker 2). Never "return what we have" — that is the partial-results
/// defect this issue exists to remove.
///
/// A windowed walk (`parse_block_emit_windowed` with a `row_body_window`, driven by
/// `data_access::big_promoted` / `bti_point`) positions its cursor from the
/// promoted/BTI row index, so a failure there is ambiguous between a misaligned
/// cursor and a genuinely undecodable column, and NOTHING at the failure site can
/// tell them apart. Re-reading through the full-partition path — which parses
/// forward from the partition header and is not resynchronising — resolves the
/// ambiguity by measurement: a misalignment artifact disappears, a real decode
/// failure surfaces from a cursor known to be at a row boundary and propagates to
/// the caller of the read. Only the fast path is lost.
///
/// Callers must DISCARD any rows the failed windowed attempt buffered before
/// retrying; every one of them collects into a local `Vec` rather than emitting
/// incrementally, so there is nothing already handed to a consumer.
///
/// # Where each windowed caller retracts, and why not lower down
///
/// * `data_access::clustering_seek_decode` — the forward clustering-slice seek. It
///   holds BOTH index narrowings (the decompressed `decode_end_bound` AND
///   `row_body_window`) and so is the only level that can drop both. Retrying
///   further down, inside `big_decode_clustering_window` or
///   `bti_collect_partition_rows`, drops only the row-body window and still reads
///   the NARROWED extent, which ends mid-partition — MEASURED to fail a clean read
///   of the committed `test_big.wide_partition` fixture on the retry as well.
/// * `big_promoted::big_reverse_partition_rows` — the reverse
///   block walk. It has no unwindowed form to retry, so it abandons the
///   optimization through `Ok(None)`, the documented channel every other "cannot
///   use the fast path" branch of that function already takes; the caller then
///   re-reads via the in-memory-sort full scan.
///
/// Gated `not(tombstones)` like its ONLY caller, `data_access::clustering_seek_decode`
/// (whose seek path is itself `not(tombstones)`): under `tombstones` no windowed
/// read path is compiled, so there is no walk to fall back.
#[cfg(not(feature = "tombstones"))]
pub(in crate::storage::sstable::reader) fn indexed_walk_falls_back(e: &Error) -> bool {
    let fall_back = is_column_decode(e);
    if fall_back {
        tracing::warn!(
            "index-positioned read hit a per-column decode failure ({}); abandoning the \
             windowed optimization and re-reading the full partition (issue #3721)",
            e
        );
    }
    fall_back
}

/// The ONE place a block/partition row loop decides what a row-parse `Err` means.
///
/// * [`Error::ColumnDecode`] — a framed row whose column would have been dropped.
///   Returned as `Err` so it reaches the caller of the read instead of silently
///   ending the partition (issue #3721).
/// * anything else — the ordinary "no further row parses here" signal that marks
///   the end of the partition body. Logged exactly as before and reported as
///   `Ok(())`, so the caller continues to `break` out of its row loop.
///
/// # No caller is exempt (issue #3721, roborev blocker 2)
///
/// An earlier revision took a `resynchronising_walk` flag and, when it was set,
/// folded [`Error::ColumnDecode`] back into the `Ok(())` end-of-partition signal —
/// so every index-positioned read (the promoted-index reverse walk and the
/// clustering-slice window read, `data_access::big_promoted` / `bti_point`, issues
/// #954/#1184) returned PARTIAL RESULTS SILENTLY. That is the original defect
/// surviving on exactly the public read paths a wide-partition `SELECT … ORDER BY
/// … DESC` or a clustering slice takes.
///
/// The two facts it conflated are real and stay distinguished — but NOT here:
///
/// * **cursor misalignment.** A walk positioned from an index can start, or run
///   past, a byte that is not a row boundary and then "decode" a row out of the
///   middle of a cell value. That is evidence about the CURSOR, not about the data.
/// * **decode failure.** The row was framed and one column's value is genuinely
///   undecodable.
///
/// Nothing at THIS call site can tell them apart, and a two-valued predicate that
/// cannot tell would have to pick one — which is how the permissive answer (accept
/// partial output) got chosen. The decision belongs to the caller that CHOSE the
/// index optimization, because only it can retract that choice: on
/// [`Error::ColumnDecode`] it abandons the windowed/indexed walk and re-reads
/// through the FULL-PARTITION path, which is not resynchronising. A misalignment
/// artifact then disappears (the full walk parses forward from the partition
/// header) and a genuine failure surfaces from a cursor known to be at a real row
/// boundary. Either way NO path returns partial output. This mirrors the fallback
/// `prime_shadow_before_window` already uses in `block_emit_windowed` when it
/// cannot faithfully reconstruct shadow state: lose the fast path, never the rows.
///
/// So this function propagates [`Error::ColumnDecode`] unconditionally, and the
/// fallback lives at the TWO windowed callers named in the section above — the
/// only two call sites of [`indexed_walk_falls_back`]:
/// `bti::clustering_seek_decode::decode_clustering_seek_target` and
/// `big_promoted::big_reverse_partition_rows`.
///
/// Measured, because an earlier revision of this paragraph named THREE and named
/// the wrong functions: `big_decode_clustering_window`
/// (`bti/clustering_seek_decode.rs`) and `bti_point::bti_collect_partition_rows`
/// both propagate their windowed walk's `Err` with `?` and retract nothing — the
/// retraction is one level up, for the reason the section above gives (they hold
/// only the row-body window, not the narrowed extent).
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
