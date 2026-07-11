//! Read-shape reassembly of a merged row's per-column cells (issue #2324).
//!
//! The k-way merger emits a live row as a flat `Vec<CellData>` where every
//! element of a non-frozen collection (`list` / `set` / `map`) is its OWN
//! [`CellData`] — one cell per element, all sharing the column name, each keyed
//! by its authoritative `cell_path` (epic #899, byte-parity write substrate).
//! That per-element shape is correct for the compaction WRITE path but WRONG for
//! any read consumer that keys cells by column name: keying the elements by name
//! (e.g. the Arrow Flight producer's `build_row_from_scan`, whose `values` is a
//! `HashMap<column, Value>`) keeps only the LAST element and silently drops the
//! rest of the collection.
//!
//! [`assemble_read_cells`] collapses those per-element cells back into a single
//! `Value::List` / `Value::Set` / `Value::Map` per column, so a read that goes
//! through the merger (the Flight `do_get` path) sees the SAME whole collection a
//! plain single-generation `SELECT` sees.
//!
//! Semantics are taken from authoritative metadata only (issue #28): elements are
//! ordered by their authoritative `cell_path` with the collection-appropriate
//! comparator (list = position-timeuuid byte order; set/map = the declared
//! element/key comparator), the set/list member is the reader-decoded
//! `CellData.value`, and the map key is decoded from the element's `cell_path`
//! with the schema's declared key type via the canonical
//! [`deserialize_value_bytes`] codec (never guessed).
//!
//! Non-scalar set elements / map keys (`frozen<tuple>`, `frozen<udt>`, nested
//! `frozen<collection>`) are NOT decodable by the scalar `deserialize_value_bytes`
//! codec. Rather than fail the whole query — a regression from the canonical
//! single-generation reader, which serves such keys as opaque bytes — this mirrors
//! that reader's `parse_cell_path_key` (complex_column.rs): the key/element is
//! served as a raw `Value::Blob(cell_path)` and ordered by raw `cell_path` byte
//! comparison. Within one SSTable the elements are already comparator-sorted on
//! disk, and a byte-comparable serialized form makes byte order == comparator
//! order; cross-SSTable ordering of a non-byte-comparable composite is a documented
//! limitation shared with (and no worse than) the single-generation read path. The
//! opaque/scalar choice branches on the DECLARED schema type only (no guessing).
//!
//! Tombstone handling follows Cassandra SELECT semantics (issue #1742: SELECT
//! output is the read-path authority), NOT the single-generation reader's
//! physical `collapsed_value`: a deleted set/list member or deleted MAP entry is
//! OMITTED from the reassembled collection. This intentionally DIVERGES from
//! `complex_column.rs`'s collapsed_value, which surfaces a deleted map entry as a
//! physical `(key, Null)` pair; that quirk is the physical-dump side and is
//! tracked separately (issue #2336 family). A `SELECT` never returns a deleted
//! map key, so the merger read-shape must not either.

#[cfg(feature = "write-support")]
use std::cmp::Ordering;
#[cfg(feature = "write-support")]
use std::collections::HashMap;
#[cfg(feature = "write-support")]
use std::sync::Arc;

#[cfg(feature = "write-support")]
use crate::schema::{CqlType, TableSchema};
#[cfg(feature = "write-support")]
use crate::storage::partition_key_codec::deserialize_value_bytes;
#[cfg(feature = "write-support")]
use crate::types::{ComparatorType, RowCells, Value};
#[cfg(feature = "write-support")]
use crate::{Error, Result};

#[cfg(feature = "write-support")]
use super::model::CellData;

/// One column's accumulated cells while grouping a row's flat cell list.
///
/// The two shapes are mutually exclusive for any one column: in Cassandra 5.0
/// (na+) a column's frozen-ness is a FIXED schema property (you cannot `ALTER`
/// a column between frozen and non-frozen), so a single consistent row can never
/// carry BOTH a whole-value (simple) cell AND per-element (complex) cells for the
/// same column. If both nonetheless arrive during grouping the row is
/// inconsistent/corrupt, so [`assemble_read_cells`] fails closed (issue #28)
/// rather than silently keep whichever shape arrived first and drop the other.
#[cfg(feature = "write-support")]
enum ColumnAccum {
    /// A simple (single-cell) column: its one decoded value.
    Simple(Value),
    /// A complex (multi-cell) column: its surviving live elements in arrival
    /// order (re-sorted by `cell_path` at collapse time). Element tombstones are
    /// already dropped.
    Complex(Vec<CellData>),
}

/// Fail-closed error for a column that arrives with BOTH simple and complex
/// cells in one merged row (see [`ColumnAccum`] — impossible for a consistent
/// na+ schema). Names the offending column; never silently drops a shape.
#[cfg(feature = "write-support")]
fn mixed_shape_error(column: &str) -> Error {
    Error::corruption(format!(
        "column '{column}': merged row mixes a whole-value (simple) cell and \
         per-element (complex) cells for one column — impossible for a consistent \
         Cassandra 5.0 (na+) schema (frozen-ness is fixed, un-ALTERable); failing \
         closed rather than silently dropping either shape (issues #28/#2324)"
    ))
}

/// Reassemble a merged live row's per-column cells into user-facing
/// [`RowCells`], collapsing multi-cell collection columns into a single
/// `Value::List` / `Value::Set` / `Value::Map` (issue #2324).
///
/// Cell tombstones (`Value::Tombstone` on a simple cell) and deleted complex
/// elements (`is_deleted`) are dropped, so a column reads as its surviving live
/// content — an absent simple column becomes null downstream, an all-deleted
/// collection becomes the empty collection.
///
/// A non-collection complex column (e.g. a non-frozen top-level UDT) is left as
/// its individual cells — this reassembles collections only, the columns the
/// issue names; other multi-cell shapes are unchanged (no worse than before).
#[cfg(feature = "write-support")]
pub fn assemble_read_cells(cells: Vec<CellData>, schema: &TableSchema) -> Result<RowCells> {
    // Group cells by column, preserving first-seen order so a stable, schema-
    // independent ordering results (downstream keys by name, so order is not
    // load-bearing — but a deterministic order keeps output reproducible).
    let mut order: Vec<Arc<str>> = Vec::new();
    let mut index: HashMap<Arc<str>, usize> = HashMap::new();
    let mut accums: Vec<ColumnAccum> = Vec::new();

    for cell in cells {
        let name: Arc<str> = Arc::from(cell.column.as_str());
        if cell.is_complex_element {
            // Drop element tombstones (a deleted set/list member OR map entry):
            // the whole element is removed. A collection with NO surviving live
            // element is left absent (reads null downstream) — an empty non-frozen
            // collection IS null in Cassandra, and this preserves the pre-fix
            // behaviour for that edge (the fix only recovers dropped LIVE elements).
            //
            // ADJUDICATION (issue #2324, roborev 1628): dropping a deleted MAP
            // entry — rather than surfacing it as `(key, Null)` — is DELIBERATE and
            // matches real Cassandra SELECT output, the read-path authority per the
            // #1742 query-semantics-oracle doctrine: a `SELECT` never returns a
            // deleted map key. This DIVERGES from the single-generation reader's
            // physical `collapsed_value` (complex_column.rs ~L663-733), which keeps
            // the `(key, Null)` pair for the physical-dump oracle; that quirk is the
            // divergent physical side, tracked separately (issue #2336 family). Do
            // NOT "fix" this to preserve `(key, Null)` here — that would corrupt the
            // merger read-shape a `do_get` returns.
            if cell.is_deleted || matches!(cell.value, Value::Tombstone(_)) {
                continue;
            }
            // Fail closed if this column already accumulated a simple cell (mixed
            // shape — impossible for a consistent na+ schema; see `ColumnAccum`).
            let elems = register_complex(&mut order, &mut index, &mut accums, name)?;
            elems.push(cell);
        } else {
            // Simple cell: a tombstoned simple cell leaves the column absent
            // (reads null downstream) — mirror the prior producer filter.
            if matches!(cell.value, Value::Tombstone(_)) {
                continue;
            }
            match index.get(name.as_ref()) {
                Some(&i) => match &mut accums[i] {
                    ColumnAccum::Simple(v) => *v = cell.value,
                    // Mixed shape: complex elements already present for this column.
                    ColumnAccum::Complex(_) => return Err(mixed_shape_error(&name)),
                },
                None => {
                    index.insert(Arc::clone(&name), accums.len());
                    order.push(name);
                    accums.push(ColumnAccum::Simple(cell.value));
                }
            }
        }
    }

    let mut out: RowCells = Vec::with_capacity(order.len());
    for (name, accum) in order.into_iter().zip(accums.into_iter()) {
        match accum {
            ColumnAccum::Simple(value) => out.push((name, value)),
            ColumnAccum::Complex(elements) => {
                let value = assemble_complex(&name, elements, schema)?;
                out.push((name, value));
            }
        }
    }
    Ok(out)
}

/// Fetch (creating if absent) the [`ColumnAccum::Complex`] slot for `name`,
/// returning a mutable reference to its element vec. Fails closed with
/// [`mixed_shape_error`] if the column already holds a simple cell (see
/// [`ColumnAccum`]).
#[cfg(feature = "write-support")]
fn register_complex<'a>(
    order: &mut Vec<Arc<str>>,
    index: &mut HashMap<Arc<str>, usize>,
    accums: &'a mut Vec<ColumnAccum>,
    name: Arc<str>,
) -> Result<&'a mut Vec<CellData>> {
    let i = match index.get(name.as_ref()) {
        Some(&i) => i,
        None => {
            let i = accums.len();
            index.insert(Arc::clone(&name), i);
            order.push(name);
            accums.push(ColumnAccum::Complex(Vec::new()));
            i
        }
    };
    // `order[i]` is the column name for `accums[i]` (both pushed together), and
    // borrows a different vec than `accums`, so it stays available for the error.
    match &mut accums[i] {
        ColumnAccum::Complex(elems) => Ok(elems),
        ColumnAccum::Simple(_) => Err(mixed_shape_error(&order[i])),
    }
}

/// Collapse the surviving live elements of one complex column into a single
/// `Value`, using the column's declared collection type from `schema`.
#[cfg(feature = "write-support")]
fn assemble_complex(
    name: &str,
    mut elements: Vec<CellData>,
    schema: &TableSchema,
) -> Result<Value> {
    // Resolve the declared collection type. An undeclared column (the Flight
    // producer builds cells for every on-disk column, even ones the caller did
    // not declare) has no type here; such columns are never emitted to Arrow, so
    // fall back to the last live element's value rather than fail the whole row.
    let declared = schema
        .columns
        .iter()
        .find(|c| c.name == name)
        .map(|c| c.data_type.as_str());

    let cql_type = match declared {
        Some(dt) => CqlType::parse(dt)?,
        None => return Ok(last_value(elements)),
    };
    let cql_type = unwrap_frozen(&cql_type);

    match cql_type {
        // A set's element identity IS its `cell_path`; order by the declared
        // element comparator so a set spanning multiple SSTables (whose elements
        // arrive in run-encounter, not disk, order) reconstructs in the SAME order
        // a single-generation `SELECT` returns (issue #2324, roborev 1628).
        CqlType::Set(inner) => {
            let elem_cmp = ComparatorType::from_cql_type(inner)?;
            sort_elements_by_cell_path(&mut elements, &elem_cmp)?;
            Ok(Value::Set(elements.into_iter().map(|e| e.value).collect()))
        }
        // A list element's `cell_path` is its position timeuuid, which orders by
        // raw byte comparison — sort by `cell_path` bytes so multi-SSTable list
        // elements land in authoritative position order, not arrival order.
        CqlType::List(_) => {
            elements.sort_by(|a, b| cell_path_bytes(a).cmp(cell_path_bytes(b)));
            Ok(Value::List(elements.into_iter().map(|e| e.value).collect()))
        }
        CqlType::Map(key_type, _) => {
            let key_cmp = ComparatorType::from_cql_type(key_type)?;
            // Order entries by the declared key comparator (scalar) or raw
            // cell_path bytes (opaque composite) so a map spanning multiple
            // SSTables reconstructs in authoritative key order — done on the cells
            // up front so the key is decoded once, at build time, below.
            sort_elements_by_cell_path(&mut elements, &key_cmp)?;
            let opaque = key_is_opaque_composite(&key_cmp);
            let mut entries = Vec::with_capacity(elements.len());
            for e in elements {
                // The map key is the element's cell_path (raw key bytes, no
                // length prefix). Decode a scalar key with the declared type; an
                // opaque composite key (frozen tuple/udt/collection) is served as
                // raw Blob bytes, mirroring the single-generation reader (module
                // doc + `key_is_opaque_composite`).
                let key_bytes = e.cell_path.as_deref().unwrap_or(&[]);
                let key = if opaque {
                    Value::Blob(key_bytes.to_vec())
                } else {
                    deserialize_value_bytes(key_bytes, &key_cmp)?
                };
                entries.push((key, e.value));
            }
            Ok(Value::Map(entries))
        }
        // A non-collection complex column (e.g. non-frozen top-level UDT):
        // reassembly of those shapes is out of this issue's scope. Keep the last
        // element's value (prior behaviour) so nothing regresses.
        _ => Ok(last_value(elements)),
    }
}

/// The element's authoritative `cell_path` bytes (empty when absent).
#[cfg(feature = "write-support")]
fn cell_path_bytes(cell: &CellData) -> &[u8] {
    cell.cell_path.as_deref().unwrap_or(&[])
}

/// Order collection cells by their `cell_path` (the element/key identity), in
/// place, using the declared element/key comparator.
///
/// A SCALAR comparator decodes each `cell_path` to a `Value` and orders by the
/// type comparator (e.g. signed-int order != raw byte order), surfacing any
/// genuine decode error (wrong-width scalar) rather than masking it. An OPAQUE
/// COMPOSITE comparator (frozen tuple/udt/collection — undecodable by the scalar
/// codec) orders by raw `cell_path` byte comparison, mirroring the
/// single-generation reader's opaque-bytes handling (see module doc +
/// [`key_is_opaque_composite`]).
#[cfg(feature = "write-support")]
fn sort_elements_by_cell_path(elements: &mut Vec<CellData>, cmp: &ComparatorType) -> Result<()> {
    if key_is_opaque_composite(cmp) {
        elements.sort_by(|a, b| cell_path_bytes(a).cmp(cell_path_bytes(b)));
        return Ok(());
    }
    // Decode each `cell_path` once up front (fallible → propagated) so the sort
    // itself is total and infallible.
    let mut keyed: Vec<(Value, CellData)> = Vec::with_capacity(elements.len());
    for cell in std::mem::take(elements) {
        let key = deserialize_value_bytes(cell_path_bytes(&cell), cmp)?;
        keyed.push((key, cell));
    }
    sort_by_comparator(&mut keyed, cmp)?;
    elements.extend(keyed.into_iter().map(|(_, cell)| cell));
    Ok(())
}

/// True when `cmp` names an element/key type the scalar [`deserialize_value_bytes`]
/// codec CANNOT decode — a frozen tuple / UDT / nested collection (or any other
/// non-scalar `Custom`). Such a key/element is served as an opaque
/// `Value::Blob(cell_path)` in raw-byte order, mirroring the canonical
/// single-generation reader's `parse_cell_path_key` (complex_column.rs) rather
/// than failing the whole query. The set of decodable scalars is kept in lockstep
/// with `deserialize_value_bytes`; branching on the DECLARED type only, never a
/// byte pattern (no-heuristics, issue #28).
#[cfg(feature = "write-support")]
fn key_is_opaque_composite(cmp: &ComparatorType) -> bool {
    match cmp {
        // `frozen` only ever wraps a composite in a map key / collection element,
        // but recurse defensively so a hypothetical frozen scalar still decodes.
        ComparatorType::Frozen(inner) => key_is_opaque_composite(inner),
        ComparatorType::Boolean
        | ComparatorType::TinyInt
        | ComparatorType::SmallInt
        | ComparatorType::Int
        | ComparatorType::BigInt
        | ComparatorType::Counter
        | ComparatorType::Float32
        | ComparatorType::Float
        | ComparatorType::Text
        | ComparatorType::Blob
        | ComparatorType::Timestamp
        | ComparatorType::Date
        | ComparatorType::Uuid
        | ComparatorType::Varint
        | ComparatorType::Decimal
        | ComparatorType::Duration => false,
        // `deserialize_value_bytes` decodes only these two Custom names.
        ComparatorType::Custom(name) => !(name == "time" || name == "inet"),
        // Tuple / Udt / Set / List / Map: undecodable by the scalar codec.
        _ => true,
    }
}

/// Sort `items` by the first tuple element with the fallible [`ComparatorType`],
/// capturing the first comparison error (a type mismatch the declared metadata
/// should never produce) and surfacing it rather than silently mis-ordering.
#[cfg(feature = "write-support")]
fn sort_by_comparator<T>(items: &mut [(Value, T)], cmp: &ComparatorType) -> Result<()> {
    let mut first_err: Option<Error> = None;
    items.sort_by(|a, b| match cmp.compare(&a.0, &b.0) {
        Ok(ord) => ord,
        Err(e) => {
            if first_err.is_none() {
                first_err = Some(e);
            }
            Ordering::Equal
        }
    });
    match first_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Transparently unwrap `Frozen(inner)` so a `frozen<set<...>>` (single-cell,
/// never reached as complex) and a bare `set<...>` resolve identically.
#[cfg(feature = "write-support")]
fn unwrap_frozen(cql: &CqlType) -> &CqlType {
    match cql {
        CqlType::Frozen(inner) => unwrap_frozen(inner),
        other => other,
    }
}

/// The last element's value, or `Value::Null` when there are none — the safe
/// fallback for a column whose collection type cannot be resolved.
#[cfg(feature = "write-support")]
fn last_value(elements: Vec<CellData>) -> Value {
    elements
        .into_iter()
        .last()
        .map(|e| e.value)
        .unwrap_or(Value::Null)
}

#[cfg(all(test, feature = "write-support"))]
mod tests {
    use super::*;
    use crate::schema::{Column, KeyColumn, TableSchema};
    use crate::types::{TombstoneInfo, TombstoneType};
    use std::collections::HashMap;

    fn col(name: &str, ty: &str) -> Column {
        Column {
            name: name.into(),
            data_type: ty.into(),
            nullable: true,
            default: None,
            is_static: false,
        }
    }

    fn schema() -> TableSchema {
        TableSchema {
            keyspace: "ks".into(),
            table: "t".into(),
            partition_keys: vec![KeyColumn {
                name: "id".into(),
                data_type: "int".into(),
                position: 0,
            }],
            clustering_keys: Vec::new(),
            columns: vec![
                col("id", "int"),
                col("s", "int"),
                col("nums", "set<int>"),
                col("items", "list<int>"),
                col("m", "map<text, bigint>"),
                // Non-scalar element/key columns (roborev 1629 F2): the scalar
                // codec cannot decode these, so they exercise the opaque-composite
                // Blob + raw-byte-order path.
                col("fset", "set<frozen<addr_type>>"),
                col("ftk", "map<frozen<tuple<int, text>>, bigint>"),
            ],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }

    /// One complex ELEMENT cell (a set/list member, or a map entry keyed by
    /// `cell_path`).
    fn elem(column: &str, value: Value, cell_path: Vec<u8>) -> CellData {
        CellData {
            column: column.into(),
            value,
            timestamp: 1,
            ttl: None,
            cell_path: Some(cell_path),
            local_deletion_time: None,
            is_complex_element: true,
            is_deleted: false,
            has_empty_value: false,
        }
    }

    fn get<'a>(cells: &'a RowCells, name: &str) -> Option<&'a Value> {
        cells
            .iter()
            .find(|(n, _)| n.as_ref() == name)
            .map(|(_, v)| v)
    }

    #[test]
    fn simple_column_passes_through() {
        let cells = vec![CellData::new("s".into(), Value::Integer(7), 1)];
        let out = assemble_read_cells(cells, &schema()).unwrap();
        assert_eq!(get(&out, "s"), Some(&Value::Integer(7)));
    }

    #[test]
    fn simple_tombstone_reads_absent() {
        let tomb = Value::Tombstone(Box::new(TombstoneInfo {
            deletion_time: 1,
            tombstone_type: TombstoneType::CellTombstone,
            local_deletion_time: 0,
            ttl: None,
            range_start: None,
            range_end: None,
        }));
        let cells = vec![CellData::new("s".into(), tomb, 1)];
        let out = assemble_read_cells(cells, &schema()).unwrap();
        assert_eq!(
            get(&out, "s"),
            None,
            "a tombstoned simple cell is absent (null)"
        );
    }

    #[test]
    fn set_and_list_reassemble_all_members_not_last_cell() {
        // Two set members + three list members, each its own cell.
        let cells = vec![
            elem("nums", Value::Integer(10), vec![0, 0, 0, 10]),
            elem("nums", Value::Integer(20), vec![0, 0, 0, 20]),
            elem("items", Value::Integer(1), vec![0]),
            elem("items", Value::Integer(2), vec![1]),
            elem("items", Value::Integer(3), vec![2]),
        ];
        let out = assemble_read_cells(cells, &schema()).unwrap();
        assert_eq!(
            get(&out, "nums"),
            Some(&Value::Set(vec![Value::Integer(10), Value::Integer(20)])),
            "SET must keep ALL members, not last-cell-wins"
        );
        assert_eq!(
            get(&out, "items"),
            Some(&Value::List(vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3)
            ])),
            "LIST must keep all members in on-disk order"
        );
    }

    #[test]
    fn map_reassembles_entries_decoding_key_from_cell_path() {
        // MAP<TEXT,BIGINT>: cell_path is the raw utf8 key; value is the bigint.
        let cells = vec![
            elem("m", Value::BigInt(100), b"alpha".to_vec()),
            elem("m", Value::BigInt(200), b"beta".to_vec()),
        ];
        let out = assemble_read_cells(cells, &schema()).unwrap();
        assert_eq!(
            get(&out, "m"),
            Some(&Value::Map(vec![
                (Value::Text("alpha".into()), Value::BigInt(100)),
                (Value::Text("beta".into()), Value::BigInt(200)),
            ])),
            "MAP must reassemble every entry with the key decoded from cell_path"
        );
    }

    #[test]
    fn deleted_elements_are_dropped() {
        let mut deleted = elem("nums", Value::Integer(99), vec![0, 0, 0, 99]);
        deleted.is_deleted = true;
        let cells = vec![elem("nums", Value::Integer(10), vec![0, 0, 0, 10]), deleted];
        let out = assemble_read_cells(cells, &schema()).unwrap();
        assert_eq!(
            get(&out, "nums"),
            Some(&Value::Set(vec![Value::Integer(10)])),
            "a deleted set member must be dropped from the reassembled collection"
        );
    }

    #[test]
    fn elements_reassemble_in_cell_path_order_not_arrival_order() {
        // Simulate multi-SSTable merge arrival: elements registered OUT of
        // cell_path order (a newer run's members encountered first). The
        // reassembled collection must land in authoritative cell_path order, not
        // arrival order (issue #2324, roborev 1628).
        let cells = vec![
            // set<int>: cell_path is the 4-byte big-endian member; arrive 30,10,20.
            elem("nums", Value::Integer(30), vec![0, 0, 0, 30]),
            elem("nums", Value::Integer(10), vec![0, 0, 0, 10]),
            elem("nums", Value::Integer(20), vec![0, 0, 0, 20]),
            // list<int>: cell_path is the position (single byte here); arrive 2,0,1.
            elem("items", Value::Integer(200), vec![2]),
            elem("items", Value::Integer(0), vec![0]),
            elem("items", Value::Integer(100), vec![1]),
            // map<text,bigint>: cell_path is the key; arrive gamma,alpha,beta.
            elem("m", Value::BigInt(3), b"gamma".to_vec()),
            elem("m", Value::BigInt(1), b"alpha".to_vec()),
            elem("m", Value::BigInt(2), b"beta".to_vec()),
        ];
        let out = assemble_read_cells(cells, &schema()).unwrap();
        assert_eq!(
            get(&out, "nums"),
            Some(&Value::Set(vec![
                Value::Integer(10),
                Value::Integer(20),
                Value::Integer(30),
            ])),
            "SET must reassemble in element (cell_path) order, not arrival order"
        );
        assert_eq!(
            get(&out, "items"),
            Some(&Value::List(vec![
                Value::Integer(0),
                Value::Integer(100),
                Value::Integer(200),
            ])),
            "LIST must reassemble in position (cell_path) order, not arrival order"
        );
        assert_eq!(
            get(&out, "m"),
            Some(&Value::Map(vec![
                (Value::Text("alpha".into()), Value::BigInt(1)),
                (Value::Text("beta".into()), Value::BigInt(2)),
                (Value::Text("gamma".into()), Value::BigInt(3)),
            ])),
            "MAP must reassemble in key (cell_path) order, not arrival order"
        );
    }

    #[test]
    fn map_deleted_entry_omitted_per_cassandra_select_semantics() {
        // A deleted MAP entry is OMITTED from the reassembled map — matching real
        // Cassandra SELECT output (the read-path authority, issue #1742). This
        // intentionally diverges from the single-generation reader's physical
        // collapsed_value, which keeps a (key, Null) pair (issue #2324, roborev
        // 1628 adjudication; see the module doc + drop-site comment).
        let mut deleted = elem("m", Value::BigInt(999), b"gone".to_vec());
        deleted.is_deleted = true;
        let cells = vec![
            elem("m", Value::BigInt(1), b"alpha".to_vec()),
            deleted,
            elem("m", Value::BigInt(2), b"beta".to_vec()),
        ];
        let out = assemble_read_cells(cells, &schema()).unwrap();
        assert_eq!(
            get(&out, "m"),
            Some(&Value::Map(vec![
                (Value::Text("alpha".into()), Value::BigInt(1)),
                (Value::Text("beta".into()), Value::BigInt(2)),
            ])),
            "a deleted map entry must be omitted (no (key, Null)) per Cassandra SELECT semantics"
        );
    }

    #[test]
    fn simple_then_complex_same_column_fails_closed() {
        // A simple (whole-value) cell then a per-element (complex) cell for the
        // SAME column: impossible for a consistent na+ schema (roborev 1629 F1).
        // Pre-fix the element was SILENTLY DROPPED (register_complex's `if let`
        // fell through); now it fails closed naming the column.
        let simple = CellData::new("nums".into(), Value::Integer(5), 1);
        let complex = elem("nums", Value::Integer(10), vec![0, 0, 0, 10]);
        let err = assemble_read_cells(vec![simple, complex], &schema()).unwrap_err();
        assert!(
            err.to_string().contains("nums"),
            "mixed-shape error must name the column, got: {err}"
        );
    }

    #[test]
    fn complex_then_simple_same_column_fails_closed() {
        // The reverse arrival order: a complex element then a simple cell for the
        // SAME column. Pre-fix the simple cell OVERWROTE (dropped) the whole
        // collection; now it fails closed naming the column (roborev 1629 F1).
        let complex = elem("nums", Value::Integer(10), vec![0, 0, 0, 10]);
        let simple = CellData::new("nums".into(), Value::Integer(5), 1);
        let err = assemble_read_cells(vec![complex, simple], &schema()).unwrap_err();
        assert!(
            err.to_string().contains("nums"),
            "mixed-shape error must name the column, got: {err}"
        );
    }

    #[test]
    fn map_with_frozen_tuple_key_served_as_blob_in_byte_order() {
        // frozen<tuple> map key: undecodable by the scalar codec. Pre-fix
        // `deserialize_value_bytes(Frozen(Tuple))` hit the `_ => Err` arm and
        // failed the WHOLE row — a regression from the single-generation reader,
        // which serves an undecodable composite key as opaque bytes. Now the key
        // is a raw Value::Blob(cell_path), ordered by raw cell_path bytes, and the
        // row succeeds (roborev 1629 F2).
        let cells = vec![
            elem("ftk", Value::BigInt(2), vec![0x00, 0x00, 0x00, 0x09, 0x62]),
            elem("ftk", Value::BigInt(1), vec![0x00, 0x00, 0x00, 0x01, 0x61]),
        ];
        let out = assemble_read_cells(cells, &schema()).unwrap();
        assert_eq!(
            get(&out, "ftk"),
            Some(&Value::Map(vec![
                (
                    Value::Blob(vec![0x00, 0x00, 0x00, 0x01, 0x61]),
                    Value::BigInt(1)
                ),
                (
                    Value::Blob(vec![0x00, 0x00, 0x00, 0x09, 0x62]),
                    Value::BigInt(2)
                ),
            ])),
            "frozen<tuple> map key served as opaque Blob(cell_path) in byte order"
        );
    }

    #[test]
    fn set_of_frozen_udt_orders_by_cell_path_bytes() {
        // frozen<udt> set element: undecodable by the scalar codec. The reader
        // already decoded the member into e.value (kept as-is); cross-SSTable
        // ordering falls back to raw cell_path byte order. Pre-fix the sort's
        // `deserialize_value_bytes(Frozen(..))` errored the whole row; now it
        // succeeds (roborev 1629 F2). Elements arrive OUT of cell_path order.
        let cells = vec![
            elem("fset", Value::Blob(vec![0xBB]), vec![0x02]),
            elem("fset", Value::Blob(vec![0xAA]), vec![0x01]),
        ];
        let out = assemble_read_cells(cells, &schema()).unwrap();
        assert_eq!(
            get(&out, "fset"),
            Some(&Value::Set(vec![
                Value::Blob(vec![0xAA]),
                Value::Blob(vec![0xBB]),
            ])),
            "frozen<udt> set members order by raw cell_path bytes, not arrival order"
        );
    }

    #[test]
    fn all_deleted_collection_is_absent() {
        let mut deleted = elem("nums", Value::Integer(99), vec![0, 0, 0, 99]);
        deleted.is_deleted = true;
        let out = assemble_read_cells(vec![deleted], &schema()).unwrap();
        assert_eq!(
            get(&out, "nums"),
            None,
            "an all-deleted collection reads absent (empty non-frozen collection == null)"
        );
    }
}
