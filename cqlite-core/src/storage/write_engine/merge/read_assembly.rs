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
//! `frozen<collection>`) are not decodable by the scalar `deserialize_value_bytes`
//! codec, and serving them as an opaque `Value::Blob(cell_path)` does not actually
//! reassemble them — the typed Arrow builder downstream expects a tuple/struct
//! value for the declared type and BREAKS on raw bytes. Issue #2339 therefore
//! decodes them STRUCTURALLY from their authoritative `cell_path` identity bytes
//! with the canonical value deserializer
//! (`comparator_value_parsing::parse_value_with_comparator` — the SAME decoder the
//! single-generation/bypass arm uses, never a second structural decoder), so a
//! `set<frozen<udt>>` / `map<frozen<tuple>, V>` reads IDENTICALLY however many
//! SSTable generations the table has. Before #2339 this path failed closed, which
//! made a correctness outcome flip on generation count.
//!
//! Composite decode needs the table's [`UdtRegistry`]: an all-lowercase UDT name
//! parses to a bare `CqlType::Custom` with no field list, so without the registry
//! there is no structure to decode into and the path STILL fails closed with a
//! clear error naming the column + declared type (never opaque bytes, never a
//! guess). The composite/scalar choice branches on the DECLARED schema type only
//! (no-heuristics, issue #28).
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
use crate::schema::{CqlType, TableSchema, UdtRegistry};
#[cfg(feature = "write-support")]
use crate::storage::partition_key_codec::deserialize_value_bytes;
#[cfg(feature = "write-support")]
use crate::types::{ComparatorType, RowCells, Value};
#[cfg(feature = "write-support")]
use crate::{Error, Result};
#[cfg(feature = "write-support")]
use composite::{compare_composite, decode_composite};

#[cfg(feature = "write-support")]
use super::model::CellData;

// Composite (frozen tuple / UDT / nested frozen collection) key/element decode +
// Cassandra-faithful ordering (issue #2339). A CHILD module rather than a sibling
// so the campsite-rule split adds nothing to `merge/mod.rs`, which is itself far
// over the size threshold (epic #1116).
#[path = "read_assembly_composite.rs"]
mod composite;

// Re-exported through `merge/mod.rs` as the ONE authority the bypass arm asks
// "can the merged arm order this composite?" (#4063, roborev job 116 F1).
//
// Gated to match `composite`'s own inner `#![cfg(feature = "write-support")]`, so the
// re-export cannot outlive the items it names.
#[cfg(feature = "write-support")]
pub use composite::first_unorderable_leaf;

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

/// Which [`UdtRegistry`], and under WHICH KEYSPACE, an UNQUALIFIED UDT reference
/// in a declared type resolves for merged-read reassembly (issue #2339).
///
/// The two facts travel together because neither is usable alone: the registry is
/// keyed by keyspace, and a caller's `TableSchema.keyspace` is NOT always the one
/// the registry was built under — `schema::parse_cql_schema` yields the literal
/// placeholder `"default"` for an unqualified `CREATE TABLE`, which is what a
/// Flight ticket DDL is, while its registry is keyed by the ticket's real
/// keyspace. Passing the keyspace explicitly makes that mismatch impossible to
/// re-introduce silently: a missed lookup leaves a UDT reference an opaque
/// `Custom` and (correctly) fails the composite decode closed, which reads as
/// "unsupported" rather than "mis-wired".
///
/// `None` at the call site means NO registry: a composite whose element/key is a
/// bare UDT reference then has no field list to decode into and fails closed
/// (never a guess — no-heuristics, issue #28).
#[cfg(feature = "write-support")]
#[derive(Debug, Clone, Copy)]
pub struct UdtScope<'a> {
    /// The authoritative UDT definitions.
    pub registry: &'a UdtRegistry,
    /// The keyspace an unqualified UDT reference resolves under.
    pub keyspace: &'a str,
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
/// The BACK-COMPATIBLE entry point, at this function's pre-#2339 signature.
///
/// `assemble_read_cells` is public API of `cqlite-core` (re-exported through
/// `storage::write_engine::merge`), so #2339 may not change its arity: adding a
/// required parameter would break every external consumer at compile time
/// (roborev job 110 F2). It therefore keeps its three arguments and delegates
/// with NO registry.
///
/// Composite/UDT-aware callers want [`assemble_read_cells_with_udts`]: with no
/// registry, a composite whose element/key is a bare UDT reference has no field
/// list to decode into and fails closed (never a guess — no-heuristics, #28).
/// Behaviour here is otherwise IDENTICAL, because this is a delegation and not a
/// second implementation — there is exactly one body.
///
/// Deliberately NOT `#[deprecated]`: this is the correct entry point for a caller
/// with no UDT registry, and the attribute would red `-D warnings` on every
/// in-repo use of it.
#[cfg(feature = "write-support")]
pub fn assemble_read_cells(
    cells: Vec<CellData>,
    schema: &TableSchema,
    needed: Option<&HashSet<String>>,
) -> Result<RowCells> {
    assemble_read_cells_with_udts(cells, schema, needed, None)
}

/// As [`assemble_read_cells`], plus the [`UdtScope`] an unqualified UDT reference
/// in a declared element/key type resolves against (issue #2339).
///
/// This holds the implementation; the three-argument form delegates here with
/// `udts: None`.
#[cfg(feature = "write-support")]
pub fn assemble_read_cells_with_udts(
    cells: Vec<CellData>,
    schema: &TableSchema,
    needed: Option<&HashSet<String>>,
    udts: Option<UdtScope<'_>>,
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
                let value = assemble_complex(&name, elements, schema, udts)?;
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
    udts: Option<UdtScope<'_>>,
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
            let elem_cmp = element_comparator(inner, udts)?;
            if key_is_opaque_composite(&elem_cmp) {
                // A composite (frozen tuple / UDT / nested collection) set element
                // IS its `cell_path` — `e.value` is EMPTY for a set cell, as the
                // sstabledump golden confirms (`"value":""`). Decode the identity
                // bytes structurally with the declared element type, then order the
                // DECODED values with Cassandra's own type comparator (issue #2339;
                // see the `composite` module for why raw `cell_path` byte order
                // is NOT Cassandra's order for a composite).
                let keyed = decode_composite_elements(name, "set element", elements, &elem_cmp)?;
                return Ok(Value::Set(
                    sort_composite(name, keyed, &elem_cmp)?
                        .into_iter()
                        .map(|(v, _)| v)
                        .collect(),
                ));
            }
            // A set's element identity IS its `cell_path`; order by the declared
            // element comparator so a set spanning multiple SSTables reconstructs
            // in the SAME order a single-generation `SELECT` returns.
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
            let key_cmp = element_comparator(key_type, udts)?;
            if key_is_opaque_composite(&key_cmp) {
                // A composite map KEY is the element's `cell_path`: decode it
                // structurally, then order entries with Cassandra's own type
                // comparator over the DECODED keys (issue #2339).
                let keyed = decode_composite_elements(name, "map key", elements, &key_cmp)?;
                return Ok(Value::Map(
                    sort_composite(name, keyed, &key_cmp)?
                        .into_iter()
                        .map(|(key, cell)| (key, cell.value))
                        .collect(),
                ));
            }
            // Order entries by the declared scalar key comparator so a map spanning
            // multiple SSTables reconstructs in authoritative key order — done on
            // the cells up front so the key is decoded once, at build time.
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
/// Reached for SCALAR element/key types only: an OPAQUE COMPOSITE
/// ([`key_is_opaque_composite`]) is decoded and ordered by [`sort_composite`]
/// before this is called, because Cassandra orders a composite by its TYPE
/// comparator and not by raw `cell_path` bytes (issue #2339). Should a composite
/// nonetheless reach here, `deserialize_value_bytes` fails closed rather than
/// silently mis-ordering it. Two scalar shapes:
///   * A `inet`/`time` comparator ([`comparator_orders_by_raw_cell_path_bytes`])
///     orders by raw `cell_path` byte comparison — its serialized form's unsigned
///     byte order IS its Cassandra order (roborev 1631/1632; the scalar comparator
///     mis-ordered these by formatted string until #3790).
///   * Any other scalar decodes each `cell_path` to a `Value` and orders by the
///     type comparator (e.g. signed-int order != raw byte order), surfacing any
///     genuine decode error (wrong-width scalar) rather than masking it.
#[cfg(feature = "write-support")]
fn sort_elements_by_cell_path(elements: &mut Vec<CellData>, cmp: &ComparatorType) -> Result<()> {
    if comparator_orders_by_raw_cell_path_bytes(cmp) {
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
/// non-scalar `Custom`).
///
/// Such a key/element is decoded STRUCTURALLY from its `cell_path` by
/// `composite::decode_composite` and ORDERED by `composite::compare_composite`,
/// which implements Cassandra's own type comparators (issue #2339). Decoding and
/// ordering are deliberately separate concerns.
///
/// This predicate is therefore no longer a fail-closed guard: it SELECTS the
/// structural path. It still fails closed for one case only — a UDT reference
/// that the table's `UdtRegistry` cannot resolve has no field list to decode
/// into, and is refused by name (see [`composite_collection_unsupported`], whose
/// doc records why the opaque-blob route was abandoned).
///
/// The set of decodable scalars is kept in lockstep with
/// `deserialize_value_bytes`; branching on the DECLARED type only, never a byte
/// pattern (no-heuristics, issue #28).
///
/// The SINGLE-generation reader's behaviour per type is stated ONCE in
/// `cell_path_key.rs`'s asymmetry section (issue #3612) — cite, never restate.
/// That asymmetry is now CLOSED for the shapes #2339 covers: both arms decode a
/// composite cell-path key/element structurally, so the outcome no longer depends
/// on SSTable generation count.
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
///   * `time` (`TimeType`): nanos-of-day, BYTE_ORDER — raw `cell_path` bytes ARE
///     Cassandra's order for EVERY 8-byte value, negatives included (#3935 refuted
///     the "always non-negative" premise; citations in `custom::compare_time`).
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

/// Resolve a collection's declared element/key type to a [`ComparatorType`],
/// resolving UDT REFERENCES through `udts` when a scope is available (issue
/// #2339).
///
/// The registry is what makes a composite element/key decodable at all:
/// `CqlType::parse("set<frozen<contact_info>>")` yields
/// `Set(Frozen(Custom("contact_info")))` — an all-lowercase UDT name parses to a
/// bare `Custom` carrying NO field list — so without the registry there is no
/// field structure to decode INTO and the type stays an opaque `Custom`
/// ([`key_is_opaque_composite`] still names it, so the path fails closed with a
/// clear error rather than guessing).
///
/// UDT references are resolved by [`UdtRegistry::resolve_type`] — the SINGLE
/// shared resolver, not a second implementation (roborev F2). This matters for
/// correctness, not just tidiness: `from_cql_type_with_registry`'s own `Custom`
/// arm looks a reference up with `registry.get_udt(keyspace, name)`, which is NOT
/// qualifier-aware, so a Cassandra-style QUALIFIED reference (`ks.contact_info`,
/// which the CQL parser retains) missed the registry and stayed an opaque
/// `Custom` — while the Flight bypass predicate answers the same question with
/// `resolve_type`, which IS qualifier-aware (`split_qualified_udt`). The two
/// therefore DISAGREED: the predicate called the type resolvable and selected the
/// single-generation arm, while a MULTI-generation merged read of the same table
/// failed closed — reintroducing exactly the generation-dependent correctness
/// outcome this issue exists to remove. Resolving here first makes both arms agree
/// BY CONSTRUCTION: one resolver, one answer.
///
/// Resolution is fail-open by design (an unknown reference is left UNCHANGED,
/// never fabricated — no-heuristics, issue #28), so a genuinely unresolvable UDT
/// still arrives as a bare `Custom` and `decode_composite` still fails closed
/// naming the column and the declared type.
#[cfg(feature = "write-support")]
fn element_comparator(declared: &CqlType, udts: Option<UdtScope<'_>>) -> Result<ComparatorType> {
    match udts {
        Some(udts) => {
            let resolved = udts.registry.resolve_type(declared, udts.keyspace);
            ComparatorType::from_cql_type_with_registry(&resolved, udts.registry, udts.keyspace)
        }
        None => ComparatorType::from_cql_type(declared),
    }
}

/// Decode every element's composite `cell_path` identity once, pairing each
/// decoded key/element with its cell (issue #2339).
#[cfg(feature = "write-support")]
fn decode_composite_elements(
    column: &str,
    kind: &str,
    elements: Vec<CellData>,
    cmp: &ComparatorType,
) -> Result<Vec<(Value, CellData)>> {
    let mut keyed = Vec::with_capacity(elements.len());
    for cell in elements {
        let value = decode_composite(column, kind, cell_path_bytes(&cell), cmp)?;
        keyed.push((value, cell));
    }
    Ok(keyed)
}

/// Order decoded composite keys/elements with Cassandra's own type comparator.
///
/// NOT raw `cell_path` byte order: Cassandra writes a collection's cells in
/// `cellPathComparator()` order, which for a composite is the declared type's
/// component-wise comparator, and the two orders genuinely DISAGREE on
/// Cassandra-written bytes (see [`composite`] for the
/// measured cases). The decode is fallible and already done, so the sort itself is
/// total; a comparison error is captured and surfaced rather than silently
/// mis-ordering. Two shapes reach here: a decoded shape the declared type
/// contradicts, and a leaf type whose central comparator is known to diverge from
/// Cassandra's and is therefore REFUSED (`varint`/`decimal`/`uuid`, issue #4063 —
/// see `composite::divergent_leaf`). `column` is carried in only so those refusals
/// can name it.
#[cfg(feature = "write-support")]
fn sort_composite(
    column: &str,
    mut keyed: Vec<(Value, CellData)>,
    cmp: &ComparatorType,
) -> Result<Vec<(Value, CellData)>> {
    let mut first_err: Option<Error> = None;
    keyed.sort_by(|a, b| match compare_composite(column, &a.0, &b.0, cmp) {
        Ok(ord) => ord,
        Err(e) => {
            if first_err.is_none() {
                first_err = Some(e);
            }
            Ordering::Equal
        }
    });
    if let Some(e) = first_err {
        return Err(e);
    }
    coalesce_comparator_equal(column, keyed, cmp)
}

/// Collapse runs of comparator-EQUAL cell paths to ONE cell, by the shared
/// reconciliation rule (issue #2339, roborev job 117).
///
/// **The defect this closes, reproduced before it was written.** Two encodings can be
/// DIFFERENT BYTES and comparator-EQUAL — an omitted trailing tuple component versus
/// an explicit null one, which `TupleType.compareCustom` returns 0 for and which
/// `an_omitted_tuple_suffix_compares_equal_to_an_explicit_all_null_suffix` already
/// pins. But `reconcile.rs` keys cells by `(column, RAW cell_path)`, so two
/// generations carrying the two encodings both SURVIVE reconciliation and assembly
/// emitted BOTH:
///
/// ```text
/// Map([(Frozen(Tuple([Integer(1)])),       BigInt(1)),
///      (Frozen(Tuple([Integer(1), Null])), BigInt(2))])
/// ```
///
/// That is not a valid CQL map — one logical key, two entries — and Cassandra would
/// return the later-timestamped winner alone.
///
/// **Why HERE and not in `reconcile.rs`.** Making `CellKey` comparator-aware would
/// change the identity of every multi-cell column in the engine, needs the declared
/// type at a layer that deliberately does not have it, and is far outside this
/// issue. This runs where the comparator IS known and the run is already SORTED, so
/// equal keys are adjacent and one linear pass suffices.
///
/// **The winner rule is NOT reimplemented**: `reconcile_rules::cell_wins` is the
/// SHARED predicate already used by both the merge path and the flush/write path
/// (issue #947) — higher timestamp, then a tombstone beats a live/expiring cell at
/// equal timestamp, then first-seen. A second copy here would be a second
/// reconciliation authority, which is the divergence class #2339 exists to remove.
///
/// The WINNER's own key encoding is the one kept, matching Cassandra keeping the
/// winning cell rather than synthesising a canonical form.
#[cfg(feature = "write-support")]
fn coalesce_comparator_equal(
    column: &str,
    keyed: Vec<(Value, CellData)>,
    cmp: &ComparatorType,
) -> Result<Vec<(Value, CellData)>> {
    let mut out: Vec<(Value, CellData)> = Vec::with_capacity(keyed.len());
    for (key, cell) in keyed {
        let collapses = match out.last() {
            // The run is sorted, so a comparator-equal peer can only be the LAST
            // pushed entry. Errors propagate rather than being swallowed into
            // "not equal", which would silently re-admit the duplicate.
            Some((prev_key, _)) => {
                compare_composite(column, prev_key, &key, cmp)? == Ordering::Equal
            }
            None => false,
        };
        if collapses {
            if let Some((prev_key, prev_cell)) = out.last_mut() {
                if crate::storage::write_engine::reconcile_rules::cell_wins(&cell, prev_cell) {
                    *prev_key = key;
                    *prev_cell = cell;
                }
            }
            continue;
        }
        out.push((key, cell));
    }
    Ok(out)
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

// Unit tests live in their own file under the campsite rule (epic #1116/#1135);
// `#[path]` keeps them a CHILD of this module, so `use super::*` still reaches
// this module's private helpers exactly as an inline `mod tests` would.
#[cfg(all(test, feature = "write-support"))]
#[path = "read_assembly_tests.rs"]
mod tests;
