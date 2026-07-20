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
    cql_fixed_len, format_table_id, CommitLogSchema, SchemaSet,
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
    pub updates: Vec<PartitionUpdate>,
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
    let mut updates = Vec::with_capacity(num_updates.min(1024) as usize);
    for _ in 0..num_updates {
        // Each update is decoded either fully (cursor left at the next update) or
        // partially (structural fields only — no schema, or an unmodeled
        // construct). A partial decode cannot locate the following update's
        // offset without the schema, so we stop the update loop and return what
        // we have. The record's frame CRC already proved the bytes are intact;
        // an under-reported batch is honest, not corruption.
        let (update, consumed_fully) = decode_partition_update(&mut c, schemas)?;
        updates.push(update);
        if !consumed_fully {
            break;
        }
    }
    Ok(Mutation { updates })
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
        has_partition_deletion: false,
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
    // Static columns block precedes regular columns only when static is present.
    if has_static {
        let _static_cols = read_column_names(c)?;
    }
    update.column_names = read_column_names(c)?;

    update.has_partition_deletion = iter_flags & ITER_HAS_PARTITION_DELETION != 0;

    // Constructs we do not fully model: bail structurally (honest, no guessing).
    // The cursor is left mid-body, so this update is NOT fully consumed.
    if update.has_partition_deletion || has_static {
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

        // Clustering values: schema.clustering is guaranteed empty here (see
        // the bail above), so this never executes — kept for signature/shape
        // stability until clustering decode is implemented as a follow-up.
        let mut clustering = Vec::with_capacity(schema.clustering.len());
        for col in &schema.clustering {
            let v = read_typed_value(c, &col.type_name)?;
            clustering.push(v);
        }

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

    fn hex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }
}
