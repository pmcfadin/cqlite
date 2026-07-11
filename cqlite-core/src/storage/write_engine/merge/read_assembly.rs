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
#[cfg(feature = "write-support")]
enum ColumnAccum {
    /// A simple (single-cell) column: its one decoded value.
    Simple(Value),
    /// A complex (multi-cell) column: its surviving live elements in arrival
    /// order (re-sorted by `cell_path` at collapse time). Element tombstones are
    /// already dropped.
    Complex(Vec<CellData>),
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
            let slot = register_complex(&mut order, &mut index, &mut accums, name);
            if let ColumnAccum::Complex(elems) = slot {
                elems.push(cell);
            }
        } else {
            // Simple cell: a tombstoned simple cell leaves the column absent
            // (reads null downstream) — mirror the prior producer filter.
            if matches!(cell.value, Value::Tombstone(_)) {
                continue;
            }
            match index.get(name.as_ref()) {
                Some(&i) => accums[i] = ColumnAccum::Simple(cell.value),
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
/// returning a mutable reference to it.
#[cfg(feature = "write-support")]
fn register_complex<'a>(
    order: &mut Vec<Arc<str>>,
    index: &mut HashMap<Arc<str>, usize>,
    accums: &'a mut Vec<ColumnAccum>,
    name: Arc<str>,
) -> &'a mut ColumnAccum {
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
    &mut accums[i]
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
            sort_elements_by_cell_path_value(&mut elements, &elem_cmp)?;
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
            let mut entries = Vec::with_capacity(elements.len());
            for e in elements {
                // The map key is the element's cell_path (raw key bytes, no
                // length prefix) — decode it with the declared key type.
                let key_bytes = e.cell_path.as_deref().unwrap_or(&[]);
                let key = deserialize_value_bytes(key_bytes, &key_cmp)?;
                entries.push((key, e.value));
            }
            // Order entries by the declared key comparator so a map spanning
            // multiple SSTables reconstructs in authoritative key order.
            sort_entries_by_key(&mut entries, &key_cmp)?;
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

/// Order set elements by their `cell_path` (the element identity) using the
/// declared element comparator, in place. Decodes each `cell_path` once up front
/// (fallible → propagated) so the sort itself is total and infallible.
#[cfg(feature = "write-support")]
fn sort_elements_by_cell_path_value(
    elements: &mut Vec<CellData>,
    cmp: &ComparatorType,
) -> Result<()> {
    let mut keyed: Vec<(Value, CellData)> = Vec::with_capacity(elements.len());
    for cell in std::mem::take(elements) {
        let key = deserialize_value_bytes(cell_path_bytes(&cell), cmp)?;
        keyed.push((key, cell));
    }
    sort_by_comparator(&mut keyed, cmp)?;
    elements.extend(keyed.into_iter().map(|(_, cell)| cell));
    Ok(())
}

/// Order `(key, value)` map entries by their key using the declared key
/// comparator, in place.
#[cfg(feature = "write-support")]
fn sort_entries_by_key(entries: &mut [(Value, Value)], cmp: &ComparatorType) -> Result<()> {
    sort_by_comparator(entries, cmp)
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
