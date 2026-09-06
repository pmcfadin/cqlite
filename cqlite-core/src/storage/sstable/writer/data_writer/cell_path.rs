//! CELL-PATH serialization for a MULTICELL collection — a map's KEY and a set's
//! ELEMENT (issues #3805, #4106).
//!
//! # Why the cell path is the one write position that may emit the SENTINEL
//! [`crate::types::Value::Empty`]'s zero-byte form is legal here and nowhere
//! else, because this is the only position supplying BOTH halves of the
//! admission:
//!
//!  * the **FRAMING** half — the length is carried by the ENCLOSING framing, so a
//!    zero-length buffer is expressible and MEANS "empty". One
//!    `CollectionType.cellPathSerializer` serializes every collection's cell path
//!    (`db/marshal/CollectionType.java:55`, `:361-382` at `cassandra-5.0.8`) and
//!    its whole body is `ByteBufferUtil.writeWithVIntLength(path.get(0))`. This
//!    half is supplied by the POSITION and by nothing else, which is why
//!    [`super::serialize_value_into`] — type-blind, and reached from every generic
//!    write context — refuses the sentinel outright, and why the type-AWARE
//!    `storage/serialization/types.rs` refuses it too: it has the type and not the
//!    framing (roborev job 452);
//!  * the **TYPE** half — the DECLARED component type is known, so the sentinel's
//!    tag can be validated against it. That check is
//!    [`crate::types::EmptyValueType::check_admits`], which lives beside the tag
//!    table it is derived from, which is derived from Cassandra's `validate()`. It
//!    is NOT written here: a copy would be a second opinion able to drift from
//!    that table (roborev job 449 finding D asked for exactly this reuse).
//!
//! Cassandra decides both collection kinds with ONE rule: `validateCellPath`
//! (`schema/ColumnMetadata.java:457-467`) validates
//! `((CollectionType)type).nameComparator().validate(path.get(0))`, and
//! `nameComparator()` is the KEYS type of a `MapType` and the ELEMENTS type of a
//! `SetType` (`db/marshal/SetType.java:101-104`). So the two functions below are
//! the same shape by construction, and the READ side mirrors them from one shared
//! gate (`row_decoder::complex_column::cell_path_empty`).
//!
//! Both dispositions are pinned by the write-surface census in
//! `crate::types::empty_value`'s `write_surface_census_tests`, which requires
//! every admitting position in the crate to be enumerated there.
//!
//! # BOTH declared SPELLINGS are recognised; an UNRESOLVABLE one is a REFUSAL,
//! # never a guess (#28)
//! A column's declared type reaches the writer as a CQL `map<K,V>` / `set<T>`
//! when it came from a CQL schema, and as Cassandra's MARSHAL form
//! (`org.apache.cassandra.db.marshal.MapType(Int32Type,Int32Type)`,
//! `SetType(Int32Type)`) when it came from a `SerializationHeader` — which is why
//! `write_complex_column` dispatches on both forms and why the READ path decodes
//! both. A gate that understood only the CQL spelling refused a sentinel that a
//! schema-less read had legitimately DECODED, on a rewrite (compaction) of the
//! very SSTable it came from (roborev job 453). Recognising the second spelling
//! widens what the gate can SEE; the admission itself is unchanged.
//!
//! Where the declared type resolves to no collection type at all, the sentinel is
//! REFUSED — there is then no declared component type to validate the tag
//! against, and refusing beats writing bytes that read back as something else.
//! Every NON-sentinel component is unaffected and goes straight to the ordinary
//! serializer, so an unresolved declared type costs nothing on any path that does
//! not carry a sentinel.

use super::*;

/// Which component of a multicell collection's declared type a cell path holds.
///
/// The two are the same question — Cassandra answers both with
/// `nameComparator()` (see the module header) — so they share ONE resolver rather
/// than two near-identical ones able to drift.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CellPathComponent {
    /// A `map<K,V>`'s KEY (`MapType.nameComparator()` == the keys type).
    MapKey,
    /// A `set<T>`'s ELEMENT (`SetType.nameComparator()` == the elements type).
    SetElement,
}

impl CellPathComponent {
    /// The declared-type spelling this component expects, for a diagnostic.
    fn declared_shapes(self) -> &'static str {
        match self {
            CellPathComponent::MapKey => {
                "`map<K,V>` nor the Cassandra marshal one \
                 (`org.apache.cassandra.db.marshal.MapType(K,V)`)"
            }
            CellPathComponent::SetElement => {
                "`set<T>` nor the Cassandra marshal one \
                 (`org.apache.cassandra.db.marshal.SetType(T)`)"
            }
        }
    }
}

/// The DECLARED type of the cell-path component of a multicell collection
/// column, from its declared type string in EITHER spelling — `None` when the
/// string denotes no collection of that kind at all.
///
/// # NEITHER resolution is written here (one fact, one parser)
///  * the CQL spelling is [`CqlType::parse`];
///  * the marshal spelling is
///    [`V5CompressedLegacyParser::parse_cassandra_type`], whose name table is
///    derived arm-by-arm from `cql3/CQL3Type.java`'s `Native` enum at
///    `cassandra-5.0.8` and which enforces the marshal PACKAGE rule (a
///    third-party `com.acme.Int32Type` is refused, not read as `int`).
///
/// The string->string `convert_marshal_type_to_cql` in
/// `parser::enhanced_statistics_parser` is deliberately NOT used: it maps
/// `IntegerType` to `int` where Cassandra binds it to `varint`, and a one-argument
/// `MapType(V)` to `map<text, V>`, so reusing it would decide an admission
/// against a component type Cassandra does not agree with.
///
/// # What stays refused, on purpose
/// A `FrozenType(MapType(K,V))` / `FrozenType(SetType(T))` resolves to
/// [`CqlType::Frozen`] and NOT to [`CqlType::Map`] / [`CqlType::Set`], so it is
/// refused here: a frozen collection is ONE inline length-prefixed cell with no
/// CellPath at all, so its empty component is the inline-element case owned by
/// `require_fixed_width` (#3847/#4071), not this one. A non-collection
/// declaration, a foreign-package class, and a spelling neither parser models
/// are all refused for the reason this function exists.
fn resolve_declared_cell_path_type(
    declared: &str,
    component: CellPathComponent,
) -> Option<CqlType> {
    fn pick(ty: CqlType, component: CellPathComponent) -> Option<CqlType> {
        match (ty, component) {
            (CqlType::Map(key, _), CellPathComponent::MapKey) => Some(*key),
            (CqlType::Set(element), CellPathComponent::SetElement) => Some(*element),
            _ => None,
        }
    }
    if let Ok(ty) = CqlType::parse(declared) {
        if let Some(resolved) = pick(ty, component) {
            return Some(resolved);
        }
    }
    match crate::storage::sstable::reader::parsing::row_decoder::V5CompressedLegacyParser::parse_cassandra_type(
        declared,
    ) {
        Ok(ty) => pick(ty, component),
        Err(_) => None,
    }
}

/// The shared body of the two entry points below: admit a sentinel against the
/// DECLARED component type, or hand a non-sentinel to `ordinary`.
///
/// `ordinary` is the serializer the caller would have used anyway — the type-blind
/// [`super::serialize_value_into`] for a map key, and
/// [`super::serialize_collection_element_into`] for a set element (which also
/// keeps that path's pre-existing "SET elements cannot be null" refusal). Taking
/// it as a parameter is what lets ONE admission live here without changing what
/// either caller does with every other value.
fn serialize_cell_path_component_into(
    value: &Value,
    declared: &str,
    component: CellPathComponent,
    out: &mut Vec<u8>,
    ordinary: impl Fn(&Value, &mut Vec<u8>) -> Result<()>,
) -> Result<()> {
    let Value::Empty(tag) = value else {
        return ordinary(value, out);
    };
    let Some(component_type) = resolve_declared_cell_path_type(declared, component) else {
        return Err(Error::InvalidInput(format!(
            "an empty-buffer sentinel (`{}`, issue #3805) needs the DECLARED component \
             type to be validated against, and `{declared}` resolves to a type in \
             neither the CQL spelling ({}); refusing rather than guessing (issue #28)",
            tag.cql_name(),
            component.declared_shapes()
        )));
    };
    tag.check_admits(&component_type, declared)?;
    // The whole encoding: NOTHING. `out` is deliberately left untouched — the
    // length lives in the caller's unsigned VInt, so a zero-length cell path IS
    // the empty component.
    Ok(())
}

/// Serialize a MULTICELL map's CELL PATH (its serialized KEY) into `out`.
///
/// `map_data_type` is the COLUMN's declared type, in either spelling. See the
/// module header for the admission's two halves, why the check is not written
/// twice, and why an unresolvable declaration is a refusal.
pub(crate) fn serialize_map_cell_path_key_into(
    key: &Value,
    map_data_type: &str,
    out: &mut Vec<u8>,
) -> Result<()> {
    serialize_cell_path_component_into(
        key,
        map_data_type,
        CellPathComponent::MapKey,
        out,
        |v, o| serialize_value_into(v, o),
    )
}

/// Serialize a MULTICELL set's CELL PATH (its serialized ELEMENT) into `out`.
///
/// ISSUE #4106. A non-frozen set stores its element IN the cell path and writes
/// the EMPTY buffer as the cell value (`cql3/Sets.java:407`:
/// `params.addCell(column, CellPath.create(bb), ByteBufferUtil.EMPTY_BYTE_BUFFER)`),
/// exactly as a map stores its key there — so the sentinel is legal here for the
/// same two reasons, decided by the same `validateCellPath` line (module header).
///
/// Before this existed, `write_set_complex_cells` routed every element through
/// the type-blind [`super::serialize_value_into`], whose sentinel refusal is
/// CORRECT and deliberate (it has no declared type, so it cannot tell a legal
/// empty `text` from the corruption Cassandra's `validate` throws on for
/// `tinyint`). The consequence was that a set CQLite had legitimately DECODED
/// from Cassandra-written bytes could not be written back — a compaction of that
/// SSTable failed. The refusal is untouched; this adds the schema-aware position
/// that has what it lacks.
///
/// A NON-sentinel element still goes through
/// [`super::serialize_collection_element_into`], so its bytes and its
/// "SET elements cannot be null" refusal are unchanged.
pub(crate) fn serialize_set_cell_path_element_into(
    element: &Value,
    set_data_type: &str,
    out: &mut Vec<u8>,
) -> Result<()> {
    serialize_cell_path_component_into(
        element,
        set_data_type,
        CellPathComponent::SetElement,
        out,
        |v, o| serialize_collection_element_into(v, "SET", o),
    )
}
