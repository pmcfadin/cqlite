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
//! codec. Serving them as an opaque `Value::Blob(cell_path)` does not actually
//! reassemble them — the typed Arrow builder downstream expects a tuple/struct
//! value for the declared type and BREAKS on raw bytes, the same failure the
//! whole-row error produced, one layer deeper. So this path FAILS CLOSED with a
//! clear error naming the column + declared key/element type
//! ([`composite_collection_unsupported`]); full composite key/element decode via
//! the value deserializer is follow-up #2339. The opaque/scalar choice branches
//! on the DECLARED schema type only (no guessing).
//!
//! Two decodable scalars — `inet` (`InetAddressType`) and `time` (`TimeType`) —
//! have serialized `cell_path` forms that ARE unsigned-byte-comparable, so they
//! are ordered by raw `cell_path` bytes rather than decoded and routed through
//! the scalar comparator ([`comparator_orders_by_raw_cell_path_bytes`], roborev
//! 1631/1632). The scalar comparator used to order them by FORMATTED string;
//! since #3790 it agrees, so raw-byte order stays equivalent and cheaper.
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
use std::collections::{HashMap, HashSet};
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
///
/// `needed` is the projection-aware set of column names this scan will actually
/// read (the output projection ∪ predicate-referenced ∪ aggregation-referenced
/// columns); `None` means "every column" (a plain `SELECT *`). Cells of a column
/// NOT in `needed` are dropped BEFORE reassembly — they are never emitted
/// downstream (projection removes them from Arrow output, the filter and
/// aggregation don't reference them). This scopes the composite-keyed-collection
/// fail-closed error (a frozen tuple/UDT/nested-collection key/element the scalar
/// codec cannot materialize, #2339) to the columns a query actually reads: an
/// unrelated `SELECT` of scalar columns from a row that merely COEXISTS with an
/// unsupported composite-keyed collection column SUCCEEDS — matching the
/// observable pre-#2324 behaviour — while the clean error still fires when that
/// composite column IS projected or referenced (issue #2324, roborev 1633). It
/// is also a small perf win (unprojected collections are never reassembled).
#[cfg(feature = "write-support")]
pub fn assemble_read_cells(
    cells: Vec<CellData>,
    schema: &TableSchema,
    needed: Option<&HashSet<String>>,
) -> Result<RowCells> {
    // Group cells by column, preserving first-seen order so a stable, schema-
    // independent ordering results (downstream keys by name, so order is not
    // load-bearing — but a deterministic order keeps output reproducible).
    let mut order: Vec<Arc<str>> = Vec::new();
    let mut index: HashMap<Arc<str>, usize> = HashMap::new();
    let mut accums: Vec<ColumnAccum> = Vec::new();

    for cell in cells {
        // Projection-aware assembly (issue #2324, roborev 1633): drop cells of a
        // column this scan never reads before touching the (fallible) reassembly.
        // `None` = all columns → nothing dropped. This keeps the composite-keyed
        // collection fail-closed error (#2339) from firing on a column an unrelated
        // query does not project or reference (see the fn-level doc).
        if let Some(needed) = needed {
            if !needed.contains(cell.column.as_str()) {
                continue;
            }
        }
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
    for (name, accum) in order.into_iter().zip(accums) {
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
            // A frozen tuple/UDT/nested-collection element cannot be materialized
            // as a typed Arrow value here — fail closed (see #2339) rather than
            // emit opaque bytes that break the typed builder downstream.
            if key_is_opaque_composite(&elem_cmp) {
                return Err(composite_collection_unsupported(name, "set element", inner));
            }
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
            // A frozen tuple/UDT/nested-collection KEY cannot be materialized as a
            // typed Arrow value here — fail closed (see #2339) rather than serve an
            // opaque Value::Blob(cell_path) that breaks the typed builder one layer
            // deeper. Only scalar-keyed maps reassemble on this path.
            if key_is_opaque_composite(&key_cmp) {
                return Err(composite_collection_unsupported(name, "map key", key_type));
            }
            // Order entries by the declared (scalar) key comparator so a map
            // spanning multiple SSTables reconstructs in authoritative key order —
            // done on the cells up front so the key is decoded once, at build time.
            sort_elements_by_cell_path(&mut elements, &key_cmp)?;
            let mut entries = Vec::with_capacity(elements.len());
            for e in elements {
                // The map key is the element's cell_path (raw key bytes, no length
                // prefix), decoded with the declared scalar type.
                let key_bytes = e.cell_path.as_deref().unwrap_or(&[]);
                let key = deserialize_value_bytes(key_bytes, &key_cmp)?;
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
/// The Set/Map callers now FAIL CLOSED on an opaque composite element/key before
/// reaching here (see [`composite_collection_unsupported`], #2339), so in practice
/// `cmp` is always a scalar. Two scalar shapes:
///   * A `inet`/`time` comparator ([`comparator_orders_by_raw_cell_path_bytes`])
///     orders by raw `cell_path` byte comparison — its serialized form's unsigned
///     byte order IS its Cassandra order (roborev 1631/1632; the scalar comparator
///     mis-ordered these by formatted string until #3790).
///   * Any other scalar decodes each `cell_path` to a `Value` and orders by the
///     type comparator (e.g. signed-int order != raw byte order), surfacing any
///     genuine decode error (wrong-width scalar) rather than masking it.
///
/// The [`key_is_opaque_composite`] arm is retained as defensive belt-and-suspenders
/// (raw-byte order) should the helper ever be reused without the upstream guard.
#[cfg(feature = "write-support")]
fn sort_elements_by_cell_path(elements: &mut Vec<CellData>, cmp: &ComparatorType) -> Result<()> {
    if key_is_opaque_composite(cmp) || comparator_orders_by_raw_cell_path_bytes(cmp) {
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

/// True when `cmp` names an element/key type [`deserialize_value_bytes`] CANNOT decode — a
/// frozen tuple / UDT / nested collection / non-scalar `Custom`. It is THIS PATH'S FAIL-CLOSED
/// PREDICATE: both guard call sites (set element, map key) return
/// [`composite_collection_unsupported`], whose doc records why the opaque-blob route was
/// abandoned, so the merge path ERRORS; #2339 closes THIS side. Its only other consumer,
/// [`sort_elements_by_cell_path`]'s arm (the `Frozen` arm recurses), is DEFENSIVE: the guard
/// fires first. Lockstep scalars; DECLARED type only (#28). The SINGLE-generation reader's
/// behaviour per type is stated ONCE in `cell_path_key.rs`'s asymmetry section — cite, never restate.
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

/// True when the element/key type's Cassandra ordering IS unsigned raw-byte
/// comparison of the `cell_path`, so the raw bytes can be compared directly with
/// no decode round-trip.
///
/// The two decodable `Custom` names — `inet` and `time` — are the ONLY declared
/// scalar types in this sort path without a dedicated [`ComparatorType`] arm.
/// Both have a canonical serialized form whose UNSIGNED byte order equals their
/// Cassandra order, and the element/key `cell_path` IS that serialized form, so
/// ordering by raw `cell_path` bytes matches Cassandra exactly and needs no
/// decode round-trip (roborev 1631/1632). Until #3790 the scalar `Custom` arm
/// ordered both by FORMATTED string; correct either way here, but only `inet`
/// actually diverged (roborev job 67):
///   * `inet` (`InetAddressType`): raw address bytes — `9.0.0.1` = `[9,0,0,1]`
///     precedes `10.0.0.1`, the REVERSE of string order: a real misordering.
///   * `time` (`TimeType`): big-endian nanos-of-day, always non-negative, so byte
///     order == numeric; and `fmt_time` zero-pads, so over the VALID range string
///     order EQUALS it — the old comparator was already right (no counterexample).
///
/// Branches on the DECLARED type only (no-heuristics, issue #28); recurses through
/// `Frozen` defensively though neither inet nor time is ever frozen here.
#[cfg(feature = "write-support")]
fn comparator_orders_by_raw_cell_path_bytes(cmp: &ComparatorType) -> bool {
    match cmp {
        ComparatorType::Frozen(inner) => comparator_orders_by_raw_cell_path_bytes(inner),
        ComparatorType::Custom(name) => name == "inet" || name == "time",
        _ => false,
    }
}

/// Fail closed when a collection's key/element is an OPAQUE COMPOSITE — a frozen
/// tuple / UDT / nested collection the scalar codec cannot decode — on the
/// merged-read (Flight) assembly path.
///
/// Serving such a key/element as an opaque `Value::Blob(cell_path)` (the round-2
/// route) does not actually reassemble it: the typed Arrow builder downstream
/// expects a tuple/struct value for the declared type and BREAKS on raw bytes —
/// the same failure the pre-#2324 whole-row error produced, only one layer
/// deeper. A clear error naming the column + declared key/element type is
/// strictly better than either, and composite decode was NEVER functional on
/// this path (it collapsed to a scalar and errored in Arrow conversion). Full
/// composite key/element decode via the value deserializer is follow-up #2339
/// (roborev 1632).
#[cfg(feature = "write-support")]
fn composite_collection_unsupported(column: &str, kind: &str, declared: &CqlType) -> Error {
    Error::unsupported_format(format!(
        "column '{column}': composite-keyed collection decode unsupported on this \
         path — {kind} type {declared:?} is a frozen tuple/UDT/nested collection the \
         merged-read assembler cannot materialize as a typed Arrow value; failing \
         closed rather than emitting opaque bytes (roborev 1632, follow-up #2339)"
    ))
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
                // inet/time element ordering (roborev 1631/1632): InetAddressType /
                // TimeType order by raw serialized bytes, which the scalar comparator
                // contradicted with a formatted-string order until #3790.
                col("iset", "set<inet>"),
                col("tset", "set<time>"),
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
        let out = assemble_read_cells(cells, &schema(), None).unwrap();
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
        let out = assemble_read_cells(cells, &schema(), None).unwrap();
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
        let out = assemble_read_cells(cells, &schema(), None).unwrap();
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
        let out = assemble_read_cells(cells, &schema(), None).unwrap();
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
    fn set_of_inet_orders_by_raw_bytes_not_string() {
        // set<inet>: cell_path (and value) is the raw 4-byte address. Cassandra's
        // InetAddressType orders by UNSIGNED address bytes, so 9.0.0.1 (bytes
        // [9,0,0,1]) precedes 10.0.0.1 (bytes [10,0,0,1]). The formatted-string
        // order the scalar `Custom("inet")` comparator would use is the REVERSE
        // ("10.0.0.1" < "9.0.0.1"), which would mis-order a multi-SSTable set.
        // Arrive out of order to prove the sort actually runs (roborev 1631).
        let ip_9 = vec![9u8, 0, 0, 1];
        let ip_10 = vec![10u8, 0, 0, 1];
        let cells = vec![
            elem("iset", Value::inet(ip_10.clone()), ip_10.clone()),
            elem("iset", Value::inet(ip_9.clone()), ip_9.clone()),
        ];
        let out = assemble_read_cells(cells, &schema(), None).unwrap();
        assert_eq!(
            get(&out, "iset"),
            Some(&Value::Set(vec![
                Value::Inet(ip_9.into()),
                Value::Inet(ip_10.into()),
            ])),
            "set<inet> must order by unsigned address bytes (9.0.0.1 before 10.0.0.1), \
             not the reversed formatted-string order"
        );
    }

    #[test]
    fn set_of_time_orders_by_raw_bytes_not_formatted_string() {
        // set<time>: cell_path is the 8-byte big-endian nanoseconds-of-day; Cassandra's
        // TimeType orders by that raw long (non-negative → byte order == numeric order).
        // Until #3790 the scalar Custom("time") comparator instead used a
        // FORMATTED-string order ("TIME(HH:MM:SS.nnn)"), which — because the hours
        // field is only zero-padded to two digits — diverges from numeric order once
        // the hours magnitude changes digit-width. 10h vs 100h: the string
        // "TIME(100:..." sorts BEFORE "TIME(10:..." ('0' < ':'), the REVERSE of numeric
        // order, so a multi-SSTable set would mis-order pre-fix. (Valid times-of-day
        // happen to coincide under the current Display; ordering by the raw cell_path
        // bytes is the robust, parity-correct rule and closes that class here,
        // roborev 1632.) Arrive out of order to prove the sort runs.
        let t_small = 36_000_000_000_000i64; // 10h in ns
        let t_big = 360_000_000_000_000i64; // 100h in ns
        let cells = vec![
            elem("tset", Value::Time(t_big), t_big.to_be_bytes().to_vec()),
            elem("tset", Value::Time(t_small), t_small.to_be_bytes().to_vec()),
        ];
        let out = assemble_read_cells(cells, &schema(), None).unwrap();
        assert_eq!(
            get(&out, "tset"),
            Some(&Value::Set(vec![Value::Time(t_small), Value::Time(t_big)])),
            "set<time> must order by the raw big-endian long (10h before 100h), \
             not the reversed formatted-string order"
        );
    }

    #[test]
    fn deleted_elements_are_dropped() {
        let mut deleted = elem("nums", Value::Integer(99), vec![0, 0, 0, 99]);
        deleted.is_deleted = true;
        let cells = vec![elem("nums", Value::Integer(10), vec![0, 0, 0, 10]), deleted];
        let out = assemble_read_cells(cells, &schema(), None).unwrap();
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
        let out = assemble_read_cells(cells, &schema(), None).unwrap();
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
        let out = assemble_read_cells(cells, &schema(), None).unwrap();
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
        let err = assemble_read_cells(vec![simple, complex], &schema(), None).unwrap_err();
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
        let err = assemble_read_cells(vec![complex, simple], &schema(), None).unwrap_err();
        assert!(
            err.to_string().contains("nums"),
            "mixed-shape error must name the column, got: {err}"
        );
    }

    #[test]
    fn map_with_frozen_tuple_key_fails_closed() {
        // frozen<tuple> map key: an opaque composite the scalar codec cannot decode.
        // The round-2 route served it as a raw Value::Blob(cell_path), but that Blob
        // breaks the typed Arrow builder downstream (which expects a tuple/struct for
        // the declared key type) — the same failure the pre-#2324 whole-row error
        // produced, one layer deeper. So this path now FAILS CLOSED with a clear
        // error naming the column + declared key type (roborev 1632; full composite
        // key decode is follow-up #2339).
        let cells = vec![
            elem("ftk", Value::BigInt(2), vec![0x00, 0x00, 0x00, 0x09, 0x62]),
            elem("ftk", Value::BigInt(1), vec![0x00, 0x00, 0x00, 0x01, 0x61]),
        ];
        let err = assemble_read_cells(cells, &schema(), None).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("ftk") && msg.contains("map key") && msg.contains("#2339"),
            "composite map-key error must name the column, the kind, and the follow-up, got: {msg}"
        );
    }

    #[test]
    fn set_of_frozen_udt_fails_closed() {
        // frozen<udt> set element: an opaque composite the scalar codec cannot decode.
        // The round-2 route kept the reader's e.value (an opaque Blob) and ordered by
        // raw cell_path bytes, but that Blob likewise breaks the typed Arrow builder
        // for the declared element type. This path now FAILS CLOSED with a clear error
        // naming the column + declared element type (roborev 1632; full composite
        // element decode is follow-up #2339).
        let cells = vec![
            elem("fset", Value::blob(vec![0xBB]), vec![0x02]),
            elem("fset", Value::blob(vec![0xAA]), vec![0x01]),
        ];
        let err = assemble_read_cells(cells, &schema(), None).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("fset") && msg.contains("set element") && msg.contains("#2339"),
            "composite set-element error must name the column, the kind, and the follow-up, got: {msg}"
        );
    }

    #[test]
    fn all_deleted_collection_is_absent() {
        let mut deleted = elem("nums", Value::Integer(99), vec![0, 0, 0, 99]);
        deleted.is_deleted = true;
        let out = assemble_read_cells(vec![deleted], &schema(), None).unwrap();
        assert_eq!(
            get(&out, "nums"),
            None,
            "an all-deleted collection reads absent (empty non-frozen collection == null)"
        );
    }

    #[test]
    fn unprojected_composite_collection_column_is_dropped_not_errored() {
        // Projection-aware assembly (issue #2324, roborev 1633): a row carrying a
        // scalar column `s` AND an unsupported composite-keyed collection column
        // `ftk` (frozen<tuple> map key). A query that projects ONLY `s` (so `ftk`
        // is NOT in `needed`) must SUCCEED, dropping `ftk` entirely — matching the
        // observable pre-#2324 behaviour where an unrelated SELECT never touched
        // this column. Pre-fix (round-3, no `needed` filter — i.e. `None` here)
        // this same row HARD-ERRORS the whole do_get; the guard closes that
        // regression. (`fails_closed_without_projection_filter` below pins the red.)
        let cells = vec![
            CellData::new("s".into(), Value::Integer(7), 1),
            elem("ftk", Value::BigInt(1), vec![0x00, 0x00, 0x00, 0x01, 0x61]),
        ];
        let needed: HashSet<String> = ["s".to_string()].into_iter().collect();
        let out = assemble_read_cells(cells, &schema(), Some(&needed)).unwrap();
        assert_eq!(get(&out, "s"), Some(&Value::Integer(7)));
        assert_eq!(
            get(&out, "ftk"),
            None,
            "an unprojected composite-keyed collection column is dropped, not assembled/errored"
        );
    }

    #[test]
    fn fails_closed_without_projection_filter() {
        // The RED anchor for the fix above: the SAME row, but with `needed = None`
        // (the round-3 behaviour, and a plain `SELECT *`). Because every column is
        // read, the composite-keyed `ftk` IS assembled and fails closed — proving
        // the projection filter, not some incidental change, is what rescues the
        // unrelated-projection case (roborev 1633).
        let cells = vec![
            CellData::new("s".into(), Value::Integer(7), 1),
            elem("ftk", Value::BigInt(1), vec![0x00, 0x00, 0x00, 0x01, 0x61]),
        ];
        let err = assemble_read_cells(cells, &schema(), None).unwrap_err();
        assert!(
            err.to_string().contains("ftk"),
            "with no projection filter the composite column still fails closed, got: {err}"
        );
    }

    #[test]
    fn projected_composite_collection_column_still_fails_closed() {
        // The complement: when the composite column IS projected/referenced (it is
        // in `needed`), the round-3 clean fail-closed error still fires — the fix
        // scopes the error, it does not suppress it (roborev 1632/1633; #2339).
        let cells = vec![elem(
            "ftk",
            Value::BigInt(1),
            vec![0x00, 0x00, 0x00, 0x01, 0x61],
        )];
        let needed: HashSet<String> = ["ftk".to_string()].into_iter().collect();
        let err = assemble_read_cells(cells, &schema(), Some(&needed)).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("ftk") && msg.contains("map key") && msg.contains("#2339"),
            "a projected composite map-key column still fails closed, got: {msg}"
        );
    }
}
