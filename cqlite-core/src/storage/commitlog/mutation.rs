//! Decoded CommitLog mutation representation + the mutation-body decoder
//! (issue #2389).
//!
//! A record body (validated by [`super::frame`]) is a serialized Cassandra
//! `Mutation` in **messaging** form (`Mutation.serializer` /
//! `UnfilteredRowIteratorSerializer` with `isForSSTable() == false`). The
//! grammar below is verified byte-for-byte against real Cassandra 5.0.2 and
//! 5.0.8 segments:
//!
//! ```text
//! mutation:  uvint numUpdates, then numUpdates × partition-update
//! update:    tableId(16), UnfilteredRowIterator
//! iterator:  writeWithVIntLength(partitionKey), iterFlags(1),
//!            EncodingStats(3 uvints), [static columns], regular columns,
//!            [partition deletion], [static row], [row estimate uvint],
//!            unfiltereds… END_OF_PARTITION(0x01)
//! row:       flags(1) [ext flags], clustering values, [liveness], [deletion],
//!            [column subset], cells…
//! cell:      flags(1), [ts delta], [localDeletion], [ttl], [path], [value]
//! ```
//!
//! This module fully decodes the common insert path (simple columns, all
//! columns present, no clustering/static/complex/subset/deletion). Constructs
//! it does not fully model (clustering columns — the `ClusteringPrefix`
//! presence-header is not decoded, see `decode_rows`'s bail — static rows,
//! complex/collection columns, column subsets, range tombstone markers,
//! row/partition deletions) are surfaced honestly: the affected partition's
//! [`PartitionUpdate::rows_decoded`] is set `false` rather than guessing byte
//! offsets — each mutation record is independently CRC-framed and
//! length-delimited, so bailing on one body never disturbs the others.

use crate::storage::commitlog::schema::{
    cql_fixed_len, format_table_id, is_simple_scalar_type, CommitLogSchema, SchemaSet,
};
use crate::{Error, Result};

// ---- UnfilteredRowIterator flags -----------------------------------------
const ITER_IS_EMPTY: u8 = 0x01;
const ITER_HAS_PARTITION_DELETION: u8 = 0x04;
const ITER_HAS_STATIC_ROW: u8 = 0x08;
const ITER_HAS_ROW_ESTIMATE: u8 = 0x10;

// ---- Unfiltered (row / marker) flags -------------------------------------
const ROW_END_OF_PARTITION: u8 = 0x01;
const ROW_IS_MARKER: u8 = 0x02;
const ROW_HAS_TIMESTAMP: u8 = 0x04;
const ROW_HAS_TTL: u8 = 0x08;
const ROW_HAS_DELETION: u8 = 0x10;
const ROW_HAS_ALL_COLUMNS: u8 = 0x20;
const ROW_HAS_COMPLEX_DELETION: u8 = 0x40;
const ROW_EXTENSION_FLAG: u8 = 0x80;

// ---- Cell flags -----------------------------------------------------------
const CELL_IS_DELETED: u8 = 0x01;
const CELL_IS_EXPIRING: u8 = 0x02;
const CELL_HAS_EMPTY_VALUE: u8 = 0x04;
const CELL_USE_ROW_TIMESTAMP: u8 = 0x08;
const CELL_USE_ROW_TTL: u8 = 0x10;

/// A decoded CommitLog mutation: one or more partition updates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mutation {
    /// The partition updates carried by this mutation (one per table touched).
    /// May be a PREFIX of the mutation's true update count — see
    /// [`Mutation::updates_complete`].
    pub updates: Vec<PartitionUpdate>,
    /// `false` when fewer updates were represented than the mutation declared.
    /// A final update whose body is only partially decoded still counts as
    /// represented; an unmodeled partial update makes this false only when
    /// additional declared updates remain hidden after it.
    pub updates_complete: bool,
}

/// A single-table partition update within a mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionUpdate {
    /// 16-byte table id (Cassandra `TableId`).
    pub table_id: [u8; 16],
    /// Raw partition-key bytes (schema-typed decoding is the caller's choice).
    pub partition_key: Vec<u8>,
    /// Whether the regular column-name block was successfully parsed.
    /// `true` includes a parsed zero-name block; `false` means no such block
    /// was read (including the empty-partition fast path and static-row bailout).
    pub columns_read: bool,
    /// Regular column names from the messaging header, in wire order.
    pub column_names: Vec<String>,
    /// Whether the partition carries a partition-level deletion.
    pub has_partition_deletion: bool,
    /// Fully-decoded rows. Empty when `rows_decoded` is `false`.
    pub rows: Vec<DecodedRow>,
    /// `true` when every row/cell was decoded; `false` when a schema was
    /// unavailable or the partition used a construct this decoder does not
    /// model (surfaced honestly rather than guessed).
    pub rows_decoded: bool,
    /// The partition's `EncodingStats.minTimestamp` (microseconds): the base every row/cell
    /// timestamp delta is added to. `None` when the update carries no `EncodingStats`
    /// (an empty-partition carrier) — never a fabricated 0.
    pub min_timestamp: Option<i64>,
    /// Why `rows` could not be decoded (`rows_decoded == false`), as a stable human string
    /// naming the unmodeled construct; `None` when rows were decoded.
    pub undecoded_cause: Option<&'static str>,
}

impl PartitionUpdate {
    /// The table id formatted as a canonical UUID string.
    pub fn table_id_uuid(&self) -> String {
        format_table_id(&self.table_id)
    }
}

/// A decoded row within a partition update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedRow {
    /// Clustering-column values in order (raw bytes; empty for a no-clustering
    /// table).
    pub clustering: Vec<Vec<u8>>,
    /// Decoded cells for the row's present columns.
    pub cells: Vec<DecodedCell>,
    /// Whether the row itself is a row deletion (tombstone).
    pub is_row_deletion: bool,
    /// Row liveness timestamp (`min_timestamp + delta`), `None` when the row carries no
    /// row-level timestamp (`ROW_HAS_TIMESTAMP` unset).
    pub writetime: Option<i64>,
    /// The row deletion's `markedForDeleteAt` (`min_timestamp + delta`) when the row is a
    /// deletion, else `None`.
    pub deletion_timestamp: Option<i64>,
}

/// A decoded cell (one column value within a row).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedCell {
    /// Column name.
    pub column: String,
    /// Raw value bytes, or `None` for a tombstone / empty-value cell.
    pub value: Option<Vec<u8>>,
    /// Whether the cell is a tombstone (deleted).
    pub deleted: bool,
    /// Cell timestamp (`min_timestamp + delta`, or the row's when `CELL_USE_ROW_TIMESTAMP`);
    /// `None` only when the cell inherits from a row that has no timestamp.
    pub writetime: Option<i64>,
}

/// Decode a mutation record body into a [`Mutation`].
///
/// `schemas` supplies per-table-id schemas for full clustering/cell decode; a
/// table with no schema decodes structurally (table id + partition key + column
/// names) with `rows_decoded == false`.
///
/// # Errors
/// [`Error::CorruptCommitLogFrame`] if the body runs out of bytes mid-field
/// (should not happen for a CRC-validated record, but is handled defensively —
/// never panics).
pub fn decode_mutation(body: &[u8], schemas: &SchemaSet) -> Result<Mutation> {
    let mut c = Cursor::new(body);
    let num_updates = c.uvint()?;
    // Guard against a maliciously huge declared count spinning the loop below:
    // each update needs at least a few bytes (a 16-byte table id alone), so a
    // count exceeding the body length is already provably impossible — reject
    // it outright rather than relying on each iteration's short-read failure
    // to terminate a `for _ in 0..num_updates` driven by an untrusted u64
    // (roborev finding — a hang risk even though it fails fast today, review-
    // first pass).
    if num_updates > body.len() as u64 {
        return Err(Error::CorruptCommitLogFrame(format!(
            "mutation declares {num_updates} updates, impossible for a {}-byte body",
            body.len()
        )));
    }
    let mut updates = Vec::with_capacity(num_updates.min(1024) as usize);
    let mut updates_complete = true;
    for update_index in 0..num_updates {
        // Each update is decoded either fully (cursor left at the next update) or
        // partially (structural fields only — no schema, or an unmodeled
        // construct). A partial decode cannot locate a following update's
        // offset, so stop. The current update is still represented in `updates`;
        // completeness is false only if declared updates remain hidden after it.
        let (update, consumed_fully) = decode_partition_update(&mut c, schemas)?;
        updates.push(update);
        if !consumed_fully {
            updates_complete = update_index + 1 == num_updates;
            break;
        }
    }
    Ok(Mutation {
        updates,
        updates_complete,
    })
}

fn decode_partition_update(
    c: &mut Cursor<'_>,
    schemas: &SchemaSet,
) -> Result<(PartitionUpdate, bool)> {
    let table_id = c.take_array16()?;
    let pk_len = c.uvint()? as usize;
    let partition_key = c.take(pk_len)?.to_vec();
    let iter_flags = c.u8()?;

    let mut update = PartitionUpdate {
        table_id,
        partition_key,
        column_names: Vec::new(),
        columns_read: false,
        // Set from the authoritative iter_flags bit immediately, before any
        // early return — has_partition_deletion is a structural fact readable
        // right here, independent of rows_decoded, so it must not silently
        // default to false on the static-row/empty-partition paths below.
        // Those early returns previously left it false even when the
        // partition genuinely carried a deletion, reporting an
        // authoritative-looking "not deleted" to callers (CLI JSON, library
        // consumers) that was simply never checked (roborev finding, review-
        // first pass).
        has_partition_deletion: iter_flags & ITER_HAS_PARTITION_DELETION != 0,
        rows: Vec::new(),
        rows_decoded: false,
        min_timestamp: None,
        undecoded_cause: Some("rows not decoded"),
    };

    if iter_flags & ITER_IS_EMPTY != 0 {
        // Empty partition (e.g. a partition-only delete carrier). The update body
        // is exactly tableId + pk + flags — fully consumed.
        update.rows_decoded = true;
        update.undecoded_cause = None;
        return Ok((update, true));
    }

    // EncodingStats: minTimestamp, minLocalDeletionTime, minTTL (all uvints).
    // Verified against cassandra-5.0.8 `EncodingStats.Serializer`: the wire value is
    // `minTimestamp - TIMESTAMP_EPOCH` (2015-09-22T00:00:00Z in microseconds).
    let min_ts_raw = c.uvint()?;
    let _min_ldt = c.uvint()?;
    let _min_ttl = c.uvint()?;
    let min_timestamp = i64::try_from(min_ts_raw)
        .ok()
        .and_then(|d| d.checked_add(TIMESTAMP_EPOCH_MICROS));
    update.min_timestamp = min_timestamp;

    let has_static = iter_flags & ITER_HAS_STATIC_ROW != 0;
    if has_static {
        // Static rows: NOT modeled. Whether Cassandra's
        // UnfilteredRowIteratorSerializer writes a static-columns block gated
        // on this per-partition iterator flag (the prior assumption here), or
        // unconditionally based on the table's static-column set regardless
        // of whether THIS iterator instance carries a static row, is
        // unverified against real Cassandra 5.0 source (unavailable in this
        // environment — see the appendix-h doc). Bail BEFORE reading any
        // columns block at all: under either assumption, guessing wrong here
        // would misalign the cursor and — worse — would leave
        // update.column_names populated with misparsed bytes that then get
        // surfaced to callers (including the CLI's structural-only output)
        // as if authoritative, even though rows_decoded is false (roborev
        // finding, review-first pass).
        update.undecoded_cause = Some("static row not decoded");
        return Ok((update, false));
    }
    update.column_names = read_column_names(c)?;
    update.columns_read = true;

    // Constructs we do not fully model: bail structurally (honest, no guessing).
    // The cursor is left mid-body, so this update is NOT fully consumed.
    if update.has_partition_deletion {
        update.undecoded_cause = Some("partition-deletion timestamp not decoded");
        return Ok((update, false));
    }

    if iter_flags & ITER_HAS_ROW_ESTIMATE != 0 {
        let _row_estimate = c.uvint()?;
    }

    let schema = match schemas.get(&table_id) {
        Some(s) => s,
        None => {
            update.undecoded_cause = Some("table schema not supplied");
            return Ok((update, false)); // structural-only decode
        }
    };

    let Some(base_ts) = min_timestamp else {
        update.undecoded_cause = Some("EncodingStats minTimestamp out of range");
        return Ok((update, false));
    };
    match decode_rows(c, schema, &update.column_names, base_ts) {
        Ok(rows) => {
            update.rows = rows;
            update.rows_decoded = true;
            update.undecoded_cause = None;
            // decode_rows consumed through END_OF_PARTITION.
            Ok((update, true))
        }
        Err(RowBail::Malformed(cause)) => Err(Error::CorruptCommitLogFrame(format!(
            "malformed mutation body for table {}: {cause}",
            update.table_id_uuid()
        ))),
        Err(RowBail::Unsupported(cause)) => {
            // A construct beyond this decoder's model was hit; keep the
            // structural fields and report rows as not decoded.
            update.rows.clear();
            update.rows_decoded = false;
            update.undecoded_cause = Some(cause);
            Ok((update, false))
        }
    }
}

/// Why the row decode stopped: a shape the decoder deliberately does not model (surfaced as an
/// incomplete, `unmeasured` update) versus a structurally MALFORMED body (truncated mid-row, a
/// timestamp delta out of range) that must be a commit-log error, never a quiet success.
enum RowBail {
    Unsupported(&'static str),
    Malformed(&'static str),
}
type RowResult<T> = std::result::Result<T, RowBail>;

fn decode_rows(
    c: &mut Cursor<'_>,
    schema: &CommitLogSchema,
    column_names: &[String],
    base_ts: i64,
) -> RowResult<Vec<DecodedRow>> {
    // Clustered tables: NOT modeled. Cassandra's `ClusteringPrefix.Serializer`
    // writes a `writeUnsignedVInt` PRESENCE HEADER (2 bits per clustering
    // column, batched per 32 columns: 0=present, 1=empty, 2=null) before the
    // present/non-empty values — see the SSTable reader's
    // `row_decoder/row_framing.rs` (issue #213) for the canonical decode of
    // this exact header. A naive per-column value loop (no header) misreads
    // the header bytes as the first value's bytes, silently misaligning every
    // subsequent field. Bail honestly rather than guess, matching this
    // module's existing posture for other unmodeled constructs (range
    // tombstone markers, partition/static-row deletions) — decoding it is a
    // follow-up, not a v1 requirement.
    if !schema.clustering.is_empty() {
        return Err(RowBail::Unsupported("clustered table not decoded"));
    }
    // Complex columns (collection/tuple/UDT/vector): NOT modeled. Cassandra
    // serializes a complex column as an entirely different wire shape (an
    // optional complex-deletion time, then a count-prefixed set of
    // (cell-path, cell) pairs) — not the single simple Cell this module's
    // read_cell decodes. The module doc previously claimed this was
    // "surfaced honestly", but nothing actually checked for it: a schema
    // with e.g. a `list<text>` column would silently misalign every
    // subsequent row while still reporting rows_decoded == true (roborev
    // finding, review-first pass). Checked once here (a schema-level
    // property, not a per-row one) rather than per column per row.
    if column_names
        .iter()
        .any(|name| !schema.column_type(name).is_some_and(is_simple_scalar_type))
    {
        return Err(RowBail::Unsupported("complex column not decoded"));
    }
    let mut rows = Vec::new();
    loop {
        let flags = c.u8().map_err(|_| short_row())?;
        if flags & ROW_END_OF_PARTITION != 0 {
            break;
        }
        if flags & ROW_IS_MARKER != 0 {
            // Range tombstone marker — not modeled.
            return Err(RowBail::Unsupported("range tombstone marker not decoded"));
        }
        if flags & ROW_EXTENSION_FLAG != 0 {
            let _ext = c.u8().map_err(|_| short_row())?;
        }

        // Clustering values: schema.clustering is guaranteed empty at this
        // point (decode_rows bails to structural-only above whenever it
        // isn't), so there is nothing to read here. No dead read-loop is kept
        // around it — re-enabling clustering support means deliberately
        // writing the ClusteringPrefix presence-header decode (see the bail
        // site's comment), not just deleting the guard (roborev finding,
        // review-first pass).
        let clustering = Vec::new();

        // Liveness.
        let mut row_writetime = None;
        if flags & ROW_HAS_TIMESTAMP != 0 {
            row_writetime = Some(add_delta(base_ts, c.uvint().map_err(|_| short_row())?)?);
        }
        if flags & ROW_HAS_TTL != 0 {
            let _ttl = c.uvint().map_err(|_| short_row())?;
            let _ldt = c.uvint().map_err(|_| short_row())?;
        }
        let mut is_row_deletion = false;
        let mut deletion_timestamp = None;
        if flags & ROW_HAS_DELETION != 0 {
            // Row deletion time (two uvints in the messaging delta encoding).
            let mfda = c.uvint().map_err(|_| short_row())?;
            let _ldt = c.uvint().map_err(|_| short_row())?;
            deletion_timestamp = Some(add_delta(base_ts, mfda)?);
            is_row_deletion = true;
        }
        if flags & ROW_HAS_COMPLEX_DELETION != 0 {
            return Err(RowBail::Unsupported("complex column not decoded"));
        }

        // Column subset — not modeled; only the all-columns case is decoded.
        if flags & ROW_HAS_ALL_COLUMNS == 0 {
            return Err(RowBail::Unsupported("column-subset row not decoded"));
        }

        // One simple cell per regular column, in header order.
        let mut cells = Vec::with_capacity(column_names.len());
        for name in column_names {
            let type_name = schema
                .column_type(name)
                .ok_or(RowBail::Unsupported("column type not in schema"))?;
            let cell = read_cell(c, name, type_name, base_ts, row_writetime)?;
            cells.push(cell);
        }

        rows.push(DecodedRow {
            clustering,
            cells,
            is_row_deletion,
            writetime: row_writetime,
            deletion_timestamp,
        });
    }
    Ok(rows)
}

fn read_cell(
    c: &mut Cursor<'_>,
    column: &str,
    type_name: &str,
    base_ts: i64,
    row_writetime: Option<i64>,
) -> RowResult<DecodedCell> {
    let flags = c.u8().map_err(|_| short_row())?;
    let deleted = flags & CELL_IS_DELETED != 0;
    let expiring = flags & CELL_IS_EXPIRING != 0;
    let empty = flags & CELL_HAS_EMPTY_VALUE != 0;

    let writetime = if flags & CELL_USE_ROW_TIMESTAMP == 0 {
        Some(add_delta(base_ts, c.uvint().map_err(|_| short_row())?)?)
    } else {
        row_writetime
    };
    let use_row_ttl = flags & CELL_USE_ROW_TTL != 0;
    if (deleted || expiring) && !use_row_ttl {
        let _ldt = c.uvint().map_err(|_| short_row())?;
    }
    if expiring && !use_row_ttl {
        let _ttl = c.uvint().map_err(|_| short_row())?;
    }

    // A value is present unless the cell is a tombstone or explicitly empty.
    let value = if deleted || empty {
        None
    } else {
        Some(read_typed_value(c, type_name)?)
    };

    Ok(DecodedCell {
        column: column.to_string(),
        value,
        deleted,
        writetime,
    })
}

/// `EncodingStats.TIMESTAMP_EPOCH` (cassandra-5.0.8 `EncodingStats.java`): 2015-09-22T00:00:00Z
/// in microseconds. The serialized `minTimestamp` is relative to it.
pub(crate) const TIMESTAMP_EPOCH_MICROS: i64 = 1_442_880_000_000_000;

/// `base + delta` with overflow treated as an unmodeled construct (never a wrapped value).
fn add_delta(base: i64, delta: u64) -> RowResult<i64> {
    i64::try_from(delta)
        .ok()
        .and_then(|d| base.checked_add(d))
        .ok_or(RowBail::Malformed("timestamp delta overflows i64"))
}

fn short_row() -> RowBail {
    RowBail::Malformed("mutation body ended mid-row")
}

/// Read one CQL value: fixed-length types have no length prefix; variable
/// types are unsigned-vint-length-prefixed (`AbstractType.writeValue`).
fn read_typed_value(c: &mut Cursor<'_>, type_name: &str) -> RowResult<Vec<u8>> {
    match cql_fixed_len(type_name) {
        Some(n) => c.take(n).map(|s| s.to_vec()).map_err(|_| short_row()),
        None => {
            let len = c.uvint().map_err(|_| short_row())? as usize;
            c.take(len).map(|s| s.to_vec()).map_err(|_| short_row())
        }
    }
}

/// Read a `Columns` block: `uvint count`, then `count` × vint-length-prefixed
/// column name bytes.
fn read_column_names(c: &mut Cursor<'_>) -> Result<Vec<String>> {
    let count = c.uvint()?;
    // Guard the declared count against the cursor's remaining bytes before
    // spinning the loop below `count` times on an untrusted u64: each column
    // name needs at least its own 1-byte length vint, so a count exceeding the
    // remaining bytes is provably impossible. Without this, a CRC-valid body of
    // mostly 0x00 bytes (each decoding as a zero-length name) could turn a
    // ~128MB body into ~128M Vec<String> entries (~3GB), defeating the <128MB
    // memory target — the same class as decode_mutation's num_updates guard.
    let remaining = c.remaining() as u64;
    if count > remaining {
        return Err(Error::CorruptCommitLogFrame(format!(
            "column-names block declares {count} names, impossible for {remaining} remaining bytes"
        )));
    }
    let mut names = Vec::with_capacity(count.min(4096) as usize);
    for _ in 0..count {
        let len = c.uvint()? as usize;
        let bytes = c.take(len)?;
        names.push(String::from_utf8_lossy(bytes).into_owned());
    }
    Ok(names)
}

/// A minimal forward-only byte cursor with unsigned-vint support.
struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    /// Bytes not yet consumed — an upper bound for any count-prefixed block, so
    /// a hostile declared count can be rejected before it drives a loop.
    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.pos)
    }

    fn u8(&mut self) -> Result<u8> {
        let b = *self.bytes.get(self.pos).ok_or_else(short)?;
        self.pos += 1;
        Ok(b)
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.pos.checked_add(n).ok_or_else(short)?;
        if end > self.bytes.len() {
            return Err(short());
        }
        let out = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    fn take_array16(&mut self) -> Result<[u8; 16]> {
        let s = self.take(16)?;
        let mut a = [0u8; 16];
        a.copy_from_slice(s);
        Ok(a)
    }

    /// Decode a Cassandra unsigned VInt (`writeUnsignedVInt`).
    ///
    /// Delegates to [`crate::parser::vint::decode_unsigned`] — the ONE canonical
    /// read-side VInt decoder (Issue #1638, Epic J/J4), already used by the
    /// SSTable reader and already fuzzed (`fuzz/fuzz_targets/fuzz_vint.rs`) —
    /// rather than maintaining a second, independent bit-assembly here. (An
    /// earlier local reimplementation had a 9-byte shift-overflow bug that the
    /// canonical decoder already special-cases correctly.)
    fn uvint(&mut self) -> Result<u64> {
        let (value, consumed) =
            crate::parser::vint::decode_unsigned(&self.bytes[self.pos..]).map_err(|_| short())?;
        self.pos += consumed;
        Ok(value)
    }
}

fn short() -> Error {
    Error::CorruptCommitLogFrame("mutation body ended mid-field".to_string())
}

#[cfg(test)]
#[path = "mutation_completeness_tests.rs"]
mod completeness_tests;

#[cfg(test)]
#[path = "mutation_tests.rs"]
mod tests;
