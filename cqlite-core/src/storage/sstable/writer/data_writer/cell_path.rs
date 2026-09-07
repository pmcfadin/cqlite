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
use crate::storage::sstable::cell_path_component::{
    resolve_declared_cell_path_type, CellPathComponent,
};
use crate::types::EmptyValueType;

// THE COMPONENT RESOLVER IS SHARED WITH THE READ SIDE and lives in
// `crate::storage::sstable::cell_path_component` — see that module's header, and
// in particular `resolve_declared_cell_path_type`'s "THE SEAM" section.
//
// It used to be private HERE, and that is precisely what #4106 roborev job 449
// finding B1 found: the writer resolved the component from the COMPLETE declared
// type (so `org.apache.cassandra.db.marshal.SetType(Int32Type)` — package on the
// outer name, BARE inner element name — worked), while the reader split the
// component name out first and then asked a classifier that requires the package
// on the name it is HANDED, so the same declaration decoded the empty cell path
// as an opaque blob. One resolver, one opinion.

/// The ADMISSION, shared by the two entry points below.
///
/// Takes no [`Value`] and writes no bytes — deliberately, and not only for
/// tidiness: the write-surface census (`crate::types::empty_value`'s
/// `write_surface_census_tests`) derives its subject set from any function
/// taking `&Value` AND producing bytes, and its rule is that a function whose
/// disposition is a claim about the SENTINEL must carry its own `Value::Empty`
/// arm. Factoring the arm itself out would leave both entry points looking
/// arm-free while their delegate admitted, i.e. the census would report a
/// refusal where there is an admission. So the arm stays at each entry point and
/// only what happens AFTER it is shared.
///
/// Two refusals, both a caller bug rather than something to paper over
/// (no-heuristics, #28): a declared type that resolves to no collection of this
/// kind (nothing to validate the tag against), and — inside
/// [`EmptyValueType::check_admits`] — a declared component type that does not
/// admit an empty buffer, or one that admits a DIFFERENT family than the tag
/// names.
fn admit_empty_cell_path(
    tag: EmptyValueType,
    declared: &str,
    component: CellPathComponent,
) -> Result<()> {
    let Some(component_type) = resolve_declared_cell_path_type(declared, component) else {
        return Err(Error::InvalidInput(format!(
            "an empty-buffer sentinel (`{}`, issue #3805) needs the DECLARED component \
             type to be validated against, and `{declared}` resolves to neither {}; \
             refusing rather than guessing (issue #28)",
            tag.cql_name(),
            component.declared_shapes()
        )));
    };
    tag.check_admits(&component_type, declared)
    // The whole encoding: NOTHING. The caller's `out` is deliberately left
    // untouched — the length lives in its unsigned VInt, so a zero-length cell
    // path IS the empty component.
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
    let Value::Empty(tag) = key else {
        return serialize_value_into(key, out);
    };
    admit_empty_cell_path(*tag, map_data_type, CellPathComponent::MapKey)
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
    let Value::Empty(tag) = element else {
        return serialize_collection_element_into(element, "SET", out);
    };
    admit_empty_cell_path(*tag, set_data_type, CellPathComponent::SetElement)
}
