//! Decoded CommitLog mutation representation + the mutation-body decoder
//! (issue #2389).
//!
//! A record body (validated by [`super::frame`]) is a serialized Cassandra
//! `Mutation` in **messaging** form (`Mutation.serializer` /
//! `UnfilteredRowIteratorSerializer` with `isForSSTable() == false`). The
//! grammar below is verified byte-for-byte against a real Cassandra 5.0.2
//! segment:
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
    /// `false` when a batch mutation declared more partition updates than
    /// `updates` contains — the loop stops at the first update whose body
    /// isn't fully consumed (no schema, or an unmodeled construct), since a
    /// partial decode can't locate the next update's offset without the
    /// schema. This is the common case for `open()` with no schemas: a
    /// multi-table batch silently reported only its first update with no
    /// signal that more existed, until this field was added (roborev
    /// finding, review-first pass).
    pub updates_complete: bool,
}

/// A single-table partition update within a mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionUpdate {
    /// 16-byte table id (Cassandra `TableId`).
    pub table_id: [u8; 16],
    /// Raw partition-key bytes (schema-typed decoding is the caller's choice).
    pub partition_key: Vec<u8>,
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
    for _ in 0..num_updates {
        // Each update is decoded either fully (cursor left at the next update) or
        // partially (structural fields only — no schema, or an unmodeled
        // construct). A partial decode cannot locate the following update's
        // offset without the schema, so we stop the update loop and return what
        // we have. The record's frame CRC already proved the bytes are intact;
        // an under-reported batch is honest, not corruption — updates_complete
        // makes that honesty visible to the caller instead of a silent
        // truncation (roborev finding, review-first pass).
        let (update, consumed_fully) = decode_partition_update(&mut c, schemas)?;
        updates.push(update);
        if !consumed_fully {
            updates_complete = false;
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
    };

    if iter_flags & ITER_IS_EMPTY != 0 {
        // Empty partition (e.g. a partition-only delete carrier). The update body
        // is exactly tableId + pk + flags — fully consumed.
        update.rows_decoded = true;
        return Ok((update, true));
    }

    // EncodingStats: minTimestamp, minLocalDeletionTime, minTTL (all uvints).
    let _min_timestamp = c.uvint()?;
    let _min_ldt = c.uvint()?;
    let _min_ttl = c.uvint()?;

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
        return Ok((update, false));
    }
    update.column_names = read_column_names(c)?;

    // Constructs we do not fully model: bail structurally (honest, no guessing).
    // The cursor is left mid-body, so this update is NOT fully consumed.
    if update.has_partition_deletion {
        return Ok((update, false));
    }

    if iter_flags & ITER_HAS_ROW_ESTIMATE != 0 {
        let _row_estimate = c.uvint()?;
    }

    let schema = match schemas.get(&table_id) {
        Some(s) => s,
        None => return Ok((update, false)), // structural-only decode
    };

    match decode_rows(c, schema, &update.column_names) {
        Ok(rows) => {
            update.rows = rows;
            update.rows_decoded = true;
            // decode_rows consumed through END_OF_PARTITION.
            Ok((update, true))
        }
        Err(_) => {
            // A construct beyond this decoder's model was hit; keep the
            // structural fields and report rows as not decoded.
            update.rows.clear();
            update.rows_decoded = false;
            Ok((update, false))
        }
    }
}

/// A sentinel error used internally to bail out of the full row decode into the
/// structural-only path without treating it as a hard corruption error.
struct Unsupported;
type RowResult<T> = std::result::Result<T, Unsupported>;

fn decode_rows(
    c: &mut Cursor<'_>,
    schema: &CommitLogSchema,
    column_names: &[String],
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
        return Err(Unsupported);
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
        return Err(Unsupported);
    }
    let mut rows = Vec::new();
    loop {
        let flags = c.u8().map_err(|_| Unsupported)?;
        if flags & ROW_END_OF_PARTITION != 0 {
            break;
        }
        if flags & ROW_IS_MARKER != 0 {
            // Range tombstone marker — not modeled.
            return Err(Unsupported);
        }
        if flags & ROW_EXTENSION_FLAG != 0 {
            let _ext = c.u8().map_err(|_| Unsupported)?;
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
        if flags & ROW_HAS_TIMESTAMP != 0 {
            let _ts_delta = c.uvint().map_err(|_| Unsupported)?;
        }
        if flags & ROW_HAS_TTL != 0 {
            let _ttl = c.uvint().map_err(|_| Unsupported)?;
            let _ldt = c.uvint().map_err(|_| Unsupported)?;
        }
        let mut is_row_deletion = false;
        if flags & ROW_HAS_DELETION != 0 {
            // Row deletion time (two uvints in the messaging delta encoding).
            let _mfda = c.uvint().map_err(|_| Unsupported)?;
            let _ldt = c.uvint().map_err(|_| Unsupported)?;
            is_row_deletion = true;
        }
        if flags & ROW_HAS_COMPLEX_DELETION != 0 {
            return Err(Unsupported);
        }

        // Column subset — not modeled; only the all-columns case is decoded.
        if flags & ROW_HAS_ALL_COLUMNS == 0 {
            return Err(Unsupported);
        }

        // One simple cell per regular column, in header order.
        let mut cells = Vec::with_capacity(column_names.len());
        for name in column_names {
            let type_name = schema.column_type(name).ok_or(Unsupported)?;
            let cell = read_cell(c, name, type_name)?;
            cells.push(cell);
        }

        rows.push(DecodedRow {
            clustering,
            cells,
            is_row_deletion,
        });
    }
    Ok(rows)
}

fn read_cell(c: &mut Cursor<'_>, column: &str, type_name: &str) -> RowResult<DecodedCell> {
    let flags = c.u8().map_err(|_| Unsupported)?;
    let deleted = flags & CELL_IS_DELETED != 0;
    let expiring = flags & CELL_IS_EXPIRING != 0;
    let empty = flags & CELL_HAS_EMPTY_VALUE != 0;

    if flags & CELL_USE_ROW_TIMESTAMP == 0 {
        let _ts_delta = c.uvint().map_err(|_| Unsupported)?;
    }
    let use_row_ttl = flags & CELL_USE_ROW_TTL != 0;
    if (deleted || expiring) && !use_row_ttl {
        let _ldt = c.uvint().map_err(|_| Unsupported)?;
    }
    if expiring && !use_row_ttl {
        let _ttl = c.uvint().map_err(|_| Unsupported)?;
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
    })
}

/// Read one CQL value: fixed-length types have no length prefix; variable
/// types are unsigned-vint-length-prefixed (`AbstractType.writeValue`).
fn read_typed_value(c: &mut Cursor<'_>, type_name: &str) -> RowResult<Vec<u8>> {
    match cql_fixed_len(type_name) {
        Some(n) => c.take(n).map(|s| s.to_vec()).map_err(|_| Unsupported),
        None => {
            let len = c.uvint().map_err(|_| Unsupported)? as usize;
            c.take(len).map(|s| s.to_vec()).map_err(|_| Unsupported)
        }
    }
}

/// Read a `Columns` block: `uvint count`, then `count` × vint-length-prefixed
/// column name bytes.
fn read_column_names(c: &mut Cursor<'_>) -> Result<Vec<String>> {
    let count = c.uvint()?;
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
mod tests {
    use super::*;
    use crate::storage::commitlog::schema::{parse_table_id, ColumnSpec};
    use std::collections::HashMap;

    // The first user mutation body captured from the real Cassandra 5.0.2
    // segment (commitlog_test.users, id=1 => alice/30). Verified end-to-end.
    const ALICE_BODY: &str = "01d6de7150844811f1bf5abf4cbc47cb4a040000000110fd36c1327cb48c00000203616765046e616d65012400080000001e0805616c69636501";

    fn users_schema() -> CommitLogSchema {
        CommitLogSchema {
            keyspace: "commitlog_test".into(),
            table: "users".into(),
            partition_key: vec![ColumnSpec::new("id", "int")],
            clustering: vec![],
            columns: vec![
                ColumnSpec::new("age", "int"),
                ColumnSpec::new("name", "text"),
            ],
        }
    }

    #[test]
    fn decodes_structural_fields_without_schema() {
        let body = hex(ALICE_BODY);
        let m = decode_mutation(&body, &HashMap::new()).expect("decode");
        assert_eq!(m.updates.len(), 1);
        let u = &m.updates[0];
        assert_eq!(u.table_id_uuid(), "d6de7150-8448-11f1-bf5a-bf4cbc47cb4a");
        assert_eq!(u.partition_key, vec![0, 0, 0, 1]);
        assert_eq!(u.column_names, vec!["age", "name"]);
        assert!(!u.rows_decoded);
    }

    #[test]
    fn decodes_cell_values_with_schema() {
        let body = hex(ALICE_BODY);
        let id = parse_table_id("d6de7150-8448-11f1-bf5a-bf4cbc47cb4a").unwrap();
        let mut schemas: SchemaSet = HashMap::new();
        schemas.insert(id, users_schema());
        let m = decode_mutation(&body, &schemas).expect("decode");
        let u = &m.updates[0];
        assert!(u.rows_decoded);
        assert_eq!(u.rows.len(), 1);
        let row = &u.rows[0];
        assert!(row.clustering.is_empty());
        // Cells in header order: age (int 30), name (text "alice").
        let age = row.cells.iter().find(|c| c.column == "age").unwrap();
        assert_eq!(age.value, Some(vec![0, 0, 0, 30]));
        let name = row.cells.iter().find(|c| c.column == "name").unwrap();
        assert_eq!(name.value.as_deref(), Some(b"alice".as_ref()));
    }

    #[test]
    fn clustered_schema_bails_honestly_instead_of_misaligning() {
        // A schema.clustering entry is enough to trip the bail (decode_rows
        // checks the schema, not the wire bytes) — regression for the
        // clustering-prefix-header gap: decoding a clustered table's rows
        // without reading Cassandra's ClusteringPrefix presence-header first
        // silently misaligns every field after it. Rather than decode wrong
        // values, the schema-declares-clustering case must bail to the
        // honest structural-only path, exactly like partition deletions and
        // static rows already do.
        let body = hex(ALICE_BODY);
        let id = parse_table_id("d6de7150-8448-11f1-bf5a-bf4cbc47cb4a").unwrap();
        let mut schema = users_schema();
        schema.clustering = vec![ColumnSpec::new("ck", "int")];
        let mut schemas: SchemaSet = HashMap::new();
        schemas.insert(id, schema);
        let m = decode_mutation(&body, &schemas).expect("decode");
        let u = &m.updates[0];
        assert!(
            !u.rows_decoded,
            "clustered schema must bail to structural-only, never emit misaligned rows"
        );
        assert!(u.rows.is_empty());
        // Structural fields (table id, partition key) still surface correctly
        // — only the row body is honestly withheld.
        assert_eq!(u.partition_key, vec![0, 0, 0, 1]);
    }

    #[test]
    fn collection_column_schema_bails_honestly_instead_of_misaligning() {
        // Regression: a schema with a collection/tuple/UDT column must bail
        // to structural-only, never attempt to decode it as a simple Cell —
        // Cassandra's complex-column wire shape is categorically different
        // (roborev finding, review-first pass; the module doc previously
        // claimed this was already handled and it was not).
        //
        // Calls decode_rows() directly rather than through decode_mutation():
        // the bail is purely schema-driven (checked before any cursor read),
        // but the only real fixture's wire bytes declare just age/name as
        // column_names — no fixture exists with an actual collection column
        // on the wire, and hand-crafting one would just be guessing bytes,
        // exactly what this module avoids. Testing decode_rows in isolation
        // proves the guard fires without needing to fabricate wire bytes it
        // never even reads.
        let mut schema = users_schema();
        schema.columns.push(ColumnSpec::new("tags", "list<text>"));
        let column_names = vec!["age".to_string(), "name".to_string(), "tags".to_string()];
        let mut c = Cursor::new(&[]);
        let result = decode_rows(&mut c, &schema, &column_names);
        assert!(
            result.is_err(),
            "a collection-typed column in column_names must bail, never emit misaligned rows"
        );
    }

    #[test]
    fn counter_column_no_longer_misreads_as_fixed_length() {
        // Regression: cql_fixed_len("counter") used to return Some(8),
        // silently consuming 8 raw bytes where a vint-length-prefixed
        // CounterContext blob actually sits. This doesn't assert a specific
        // decoded value (a CounterContext isn't a plain scalar this module
        // interprets further) — it asserts the schema-driven bail no longer
        // treats a counter column as fixed-8, i.e. it's on the
        // simple-scalar allowlist via the vint-length path, not excluded
        // from decode entirely (roborev finding, review-first pass).
        use crate::storage::commitlog::schema::is_simple_scalar_type;
        assert!(is_simple_scalar_type("counter"));
        assert_eq!(cql_fixed_len("counter"), None);
    }

    #[test]
    fn tinyint_cell_stays_aligned_into_following_int_cell() {
        // Regression (PR #2797 blocker): tinyint is vint-length-prefixed, not
        // fixed-1. Before the fix, cql_fixed_len("tinyint") == Some(1) read the
        // uvint LENGTH byte (0x01) as the value and left the cursor one byte
        // behind, corrupting the following int cell. Hand-encode one row with a
        // tinyint cell (value 5) followed by an int cell (value 42) and prove
        // both decode with the cursor staying aligned across the tinyint.
        let schema = CommitLogSchema {
            keyspace: "commitlog_test".into(),
            table: "gauges".into(),
            partition_key: vec![ColumnSpec::new("id", "int")],
            clustering: vec![],
            columns: vec![
                ColumnSpec::new("flag", "tinyint"),
                ColumnSpec::new("count", "int"),
            ],
        };
        let column_names = vec!["flag".to_string(), "count".to_string()];

        let bytes: Vec<u8> = [
            // Row flags: all columns present, nothing else.
            ROW_HAS_ALL_COLUMNS,
            // Cell 1 ("flag", tinyint): use-row-timestamp, then vint-length(1) + 0x05.
            CELL_USE_ROW_TIMESTAMP,
            0x01, // uvint length = 1
            0x05, // the tinyint byte value 5
            // Cell 2 ("count", int): use-row-timestamp, then fixed-4 big-endian 42.
            CELL_USE_ROW_TIMESTAMP,
            0x00,
            0x00,
            0x00,
            0x2A,
            // Terminator.
            ROW_END_OF_PARTITION,
        ]
        .to_vec();

        let mut c = Cursor::new(&bytes);
        let rows = match decode_rows(&mut c, &schema, &column_names) {
            Ok(rows) => rows,
            Err(Unsupported) => panic!("row with a variable-length tinyint cell must decode"),
        };
        assert_eq!(rows.len(), 1);
        let cells = &rows[0].cells;
        let flag = cells.iter().find(|c| c.column == "flag").unwrap();
        assert_eq!(
            flag.value.as_deref(),
            Some([0x05].as_ref()),
            "tinyint value must be the single byte 5, not the vint length"
        );
        let count = cells.iter().find(|c| c.column == "count").unwrap();
        assert_eq!(
            count.value.as_deref(),
            Some([0x00, 0x00, 0x00, 0x2A].as_ref()),
            "int cell must stay aligned after the variable-length tinyint"
        );
    }

    fn hex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }
}
