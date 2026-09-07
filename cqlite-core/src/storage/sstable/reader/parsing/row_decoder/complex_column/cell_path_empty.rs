//! The EMPTY-BUFFER ADMISSION GATE for a multicell collection's CELL PATH, and
//! the SET path's decode entry point (issues #3747, #3805, #4106).
//!
//! # ONE gate, because Cassandra asks ONE question
//! A zero-length cell path is the EMPTY component, never "no component". Whether
//! that component is LEGAL DATA or CORRUPTION is decided by `validateCellPath`,
//! and at `cassandra-5.0.8` that is a single line for every collection kind:
//! `schema/ColumnMetadata.java:457-467` calls
//! `((CollectionType)type).nameComparator().validate(path.get(0))`, where
//! `nameComparator()` is the KEYS type of a `MapType`
//! (`db/marshal/MapType.java`) and the ELEMENTS type of a `SetType`
//! (`db/marshal/SetType.java:101-104`). The FRAMING half is the same object too:
//! one `CollectionType.cellPathSerializer` (`db/marshal/CollectionType.java:55`,
//! `:361-382`) serializes every collection's cell path as
//! `ByteBufferUtil.writeWithVIntLength(path.get(0))`, so a zero-length path is
//! expressible and MEANS empty. Hence one gate here rather than a per-collection
//! opinion able to drift.
//!
//! # THE GATE IS THE TAG TABLE, NOT A WIDTH TABLE
//! The admission is [`EmptyValueType::for_cql_type`] — the ONE place the
//! legal/corruption line is drawn, on Cassandra's `validate()` rather than on
//! decodability. A `Some` tag means that family's serializer accepts the empty
//! buffer AND maps it to null (see that function's membership rule), so the
//! component is PRESENT, TYPED and MEANINGLESS-VALUED. Because the gate is drawn
//! on `validate()` and not on decodability it is CORRECT to consult it without
//! regard to what the decoder would have done, which is why it is consulted for
//! EVERY empty cell path and BEFORE the decode.
//!
//! That placement is the whole content of roborev job 449 finding C (#4079): a
//! gate consulted only in the decoder's `Err` arm cannot reach a family whose
//! decoder ACCEPTS the empty buffer, so `varint` and `inet` — both ADMITTED by
//! the tag table — kept a SECOND spelling of the empty buffer (`Varint(b"")` /
//! `Inet(b"")`) beside the canonical `Value::Empty(tag)`.
//!
//! An earlier revision of the map gate asked the WIDTH table instead
//! (`allowed.contains(&0)`). Those are two authorities answering two DIFFERENT
//! questions and they disagree on exactly one family: `decimal` is
//! VARIABLE-width (Cassandra accepts `0` or `>= 4`), a width table cannot
//! express `{0} ∪ [4, ∞)`, so `allowed.contains(&0)` was FALSE and a LEGAL empty
//! `decimal` component was refused — while `for_cql_type` admits it from
//! `DecimalSerializer.java:31-34,58-63`, whose own message reads *"Expected 0 or
//! at least 4 bytes"* (#3805 REQ-3805-02).
//!
//! # WHAT THE GATE DOES *NOT* REACH — the bound, MEASURED not reasoned
//!  * `text`/`ascii`/`varchar`/`blob`: `for_cql_type` is `None`, because an empty
//!    buffer is a legal, MEANINGFUL value there
//!    (`serializers/AbstractTextSerializer.java:72-77`,
//!    `serializers/BytesSerializer.java:57-62` override `isNull` to say so). They
//!    keep `Text(b"")`/`Blob(b"")`.
//!  * `tinyint`/`smallint`/`date`/`time`: `for_cql_type` is `None` (bare `!= N`
//!    validate — corruption on Cassandra's own terms), so the gate declines and
//!    the refusal is the width check's (map) or the decoder's own (set).
//!  * `duration`, every composite (`list`/`set`/`map`/`tuple`/UDT/
//!    `frozen<collection>`) and `custom`: `for_cql_type` is `None`, so they fall
//!    through to the decode and keep their existing outcome — an `Err` for
//!    `duration` and the collections, a structural decode for `tuple`, an opaque
//!    blob for an unresolvable UDT name.
//!
//! Both halves — the table's answer AND the decode result — are asserted by
//! `regression_3747_empty_map_key_tests` and
//! `regression_4106_empty_set_member_tests`, so a widening of the table cannot
//! silently widen either decoder.
//!
//! # ONE ADMISSION, TWO RESOLUTIONS — THE SEAM, stated (#4106, roborev job 449 B1)
//! Two reviewers reported adjacent facts on this file that only LOOK
//! contradictory. Both are right, about different things:
//!
//!  * the marshal TABLE is shared, so "the gate's classifier and the decoder
//!    cannot disagree on the marshal spelling" holds:
//!    [`Self::native_marshal_to_cql_type`] is the crate's one marshal-name
//!    authority and `primitive_marshal_to_cql_short` (which the decoder consults)
//!    is a projection of it;
//!  * the ENTRY CONDITION differs, and THAT is where read and write diverged.
//!    Every consumer handed an ISOLATED COMPONENT NAME consults the table only
//!    when the string already `contains("org.apache.cassandra.db.marshal.")` —
//!    [`Self::cell_path_key_cql_type`] and the value decoder
//!    (`raw_value/reporting.rs`) both spell it that way, and they must, because a
//!    bare name out of context is ambiguous (`#3612` round 9 finding 2 removed
//!    the package-synthesising alternative: it made a foreign
//!    `com.acme.CustomBytesType` match `BytesType`, which is name-pattern
//!    inference #28 forbids).
//!
//! So the ADMISSION is one function ([`Self::cell_path_empty_admits`]) and the
//! RESOLUTION of the component TYPE is per-route — because Cassandra's own type
//! SELECTION is per-route:
//!
//!  * a SET's element type is derived from the column's declared type and from
//!    nothing else, so the set route resolves it from the COMPLETE declared type
//!    via the shared `cell_path_component::resolve_declared_cell_path_type` — the
//!    same function the WRITER uses, so the two agree by construction. This is
//!    finding B1's fix: `org.apache.cassandra.db.marshal.SetType(Int32Type)`
//!    (package on the OUTER name, bare inner element name — a legal `TypeParser`
//!    spelling, `TypeParser.java:450`) was ACCEPTED by the writer and refused by
//!    the reader, whose string split had discarded the marshal context before the
//!    classifier ever saw it;
//!  * a MAP's key type has a committed marshal-over-schema PRECEDENCE rule
//!    (`map_key_type_for_decode`, #3612 R7/R8: the marshal form wins when it is
//!    UDT-bearing, and Cassandra's `SerializationHeader.getType` is the authority
//!    for that), so the string the decoder uses is NOT always derived from
//!    `column.data_type` and resolving from `column.data_type` would OVERRIDE the
//!    header in exactly the case that rule resolves in the header's favour. The
//!    map route therefore keeps classifying the string the precedence rule
//!    picked. **The same bare-inner-name asymmetry is therefore still reachable
//!    on the MAP route** — `org.apache.cassandra.db.marshal.MapType(Int32Type,
//!    Int32Type)` — and it is NOT silently fixed here; it is named, and it needs
//!    the precedence rule to report WHICH container it picked from before it can
//!    be closed the same way. Every real `SerializationHeader` writes fully
//!    qualified names, so the reachable spelling is the hand-written/
//!    `TypeParser`-legal one.

use super::*;
use crate::storage::sstable::cell_path_component::{
    resolve_declared_cell_path_type, CellPathComponent,
};
use crate::types::EmptyValueType;

impl V5CompressedLegacyParser {
    /// **THE ADMISSION** — the ONE place the legal/corruption line is drawn for a
    /// zero-length cell path, on an already-RESOLVED component type.
    ///
    /// Both routes below end here, so a widening of the tag table widens both at
    /// once and neither can hold a per-family opinion of its own. What differs
    /// between the routes is only how the component type is NAMED; see the
    /// module header's "ONE ADMISSION, TWO RESOLUTIONS" section for why that
    /// half cannot be shared.
    pub(super) fn cell_path_empty_admits(component_type: &CqlType) -> Option<EmptyValueType> {
        EmptyValueType::for_cql_type(component_type)
    }

    /// The typed sentinel a zero-length cell path denotes for a MAP KEY of
    /// DECLARED type `type_str`, or `None` when no authority admits an empty
    /// buffer there.
    ///
    /// `type_str` is the string `map_key_type_for_decode` picked (schema short
    /// form, or the marshal form when it is UDT-bearing), so it is an ISOLATED
    /// COMPONENT NAME and is normalized by [`Self::cell_path_key_cql_type`] —
    /// the classifier for that shape. The admission is
    /// [`Self::cell_path_empty_admits`]. Nothing per-family is decided here.
    ///
    /// See the module header for why this route cannot resolve from the COMPLETE
    /// declared type the way the set route does, and for the residual
    /// bare-inner-marshal-name asymmetry that leaves.
    pub(super) fn cell_path_empty_sentinel(&self, type_str: &str) -> Option<EmptyValueType> {
        self.cell_path_key_cql_type(type_str)
            .as_ref()
            .and_then(Self::cell_path_empty_admits)
    }

    /// The typed sentinel a zero-length cell path denotes for the ELEMENT of a
    /// multicell set whose COLUMN is declared `set_data_type`, or `None` when no
    /// authority admits an empty buffer there.
    ///
    /// **Resolved from the COMPLETE declared type, never from the split-out
    /// element name** (#4106, roborev job 449 finding B1). The set branch derives
    /// its element type from `column.data_type` and from nothing else, so
    /// resolving the element from `column.data_type` is resolving THE SAME
    /// DECLARATION — it restores the marshal context that
    /// `extract_collection_element_type`'s string split discards, and it does so
    /// through `cell_path_component::resolve_declared_cell_path_type`, the very
    /// function the WRITER's admission uses. Read and write therefore agree on
    /// every declared string by construction rather than by care.
    ///
    /// There is deliberately NO fallback to [`Self::cell_path_key_cql_type`] on
    /// the split-out name. A fallback would re-admit exactly the disagreements
    /// this shares a resolver to prevent: `set<frozen<int>>` (not legal CQL)
    /// resolves to `Frozen(Int)` here and is REFUSED, which is what the writer
    /// does with it — a fallback would peel the `frozen<>` and admit `Int` on the
    /// read side only.
    pub(super) fn set_element_empty_sentinel(&self, set_data_type: &str) -> Option<EmptyValueType> {
        resolve_declared_cell_path_type(set_data_type, CellPathComponent::SetElement)
            .as_ref()
            .and_then(Self::cell_path_empty_admits)
    }

    /// Decode a MULTICELL SET's cell path — which IS the member (`cql3/Sets.java:407`
    /// writes the element as the cell path and `ByteBufferUtil.EMPTY_BYTE_BUFFER`
    /// as the cell value).
    ///
    /// ISSUE #4106. A ZERO-LENGTH path is the EMPTY MEMBER, so it goes through the
    /// shared admission gate above; everything else takes the SAME
    /// [`Self::parse_value_from_raw_bytes`] it always did. The guard this replaces
    /// (`!path_bytes.is_empty()`) produced `None` and DROPPED the member, so a
    /// `SELECT` returned a set one member short with no error and no log line —
    /// the identical defect #3747 removed for map keys, and the reason the gate
    /// lives in one place for both.
    ///
    /// # SCOPE: the fixed-width / full-consumption checks are NOT extended here
    /// The MAP cell path additionally validates widths and full consumption
    /// ([`Self::parse_cell_path_key_reporting`], issue #3612), and that module's
    /// header records that widening those to the set/frozen routes was out of
    /// #3612's scope. It stays out of #4106's: a NON-EMPTY member keeps its exact
    /// pre-existing decode, and only the empty buffer — which the guard meant
    /// reached NO authority at all — is newly admitted.
    ///
    /// Errors PROPAGATE (#3811, roborev F1): mapping them to a dropped member is
    /// strictly worse than a refusal, because a dropped member leaves no trace.
    ///
    /// # A CELL-PATH COMPONENT IS NEVER `null`, so a `Null` decode is a REFUSAL
    /// The shared value decoder's oracle for an empty buffer is Cassandra's
    /// `deserialize()`, which maps it to `null` UNIFORMLY for all twelve
    /// fixed-width scalars (issue #3847 — see
    /// [`super::super::raw_value::fixed_width`]'s header and its pinned oracle).
    /// That is the right answer for a VALUE and an impossible one for a cell-path
    /// COMPONENT: Cassandra has no null map key and no null set member —
    /// `cql3/Sets.java:407` writes the element INTO the path and cannot express
    /// one, and `validateCellPath` (`schema/ColumnMetadata.java:457-467`)
    /// validates the component with the element type's `validate()`, which for
    /// `tinyint`/`smallint`/`date`/`time` is a bare `!= N` that THROWS on the
    /// empty buffer. So when the admission gate declines AND the decoder answers
    /// `Null`, no authority admits these bytes and the member is REFUSED.
    ///
    /// This is the same composition `cell_path_key.rs` uses one route over (it
    /// substitutes its own answer keyed on THIS DECODER'S ANSWER, never on a
    /// width table), and it lands on the same OUTCOME the map route already has
    /// for the same declared type: an empty `tinyint` map key is refused there by
    /// the width check, on `cell_path_key`'s committed error-budget rule — `Err`
    /// only where Cassandra's own `validate`/`split` throws.
    ///
    /// MEASURED, not derived: the families this branch actually reaches are
    /// exactly those four (`regression_4106_empty_set_member_tests`'s refusal and
    /// trichotomy cases pin it). Nothing per-family is decided here — the branch
    /// is keyed on the decoder's answer, so it cannot drift from the tag table.
    ///
    /// # TWO type strings, and each is used for the ONE thing it can answer
    /// `set_data_type` is the COLUMN's complete declared type and is what the
    /// admission is resolved from ([`Self::set_element_empty_sentinel`]) — see
    /// the module header's "ONE ADMISSION, TWO RESOLUTIONS" section for why the
    /// split-out element name cannot answer that question for the marshal
    /// spelling. `element_type` is that split-out name and is what the VALUE
    /// DECODER is handed, unchanged: a NON-EMPTY member keeps its exact
    /// pre-existing decode, and widening the decoder's own entry condition is
    /// out of #4106's scope (a bare-inner-name marshal element still decodes to
    /// an opaque blob there, exactly as it did before this change — that is the
    /// decoder's pre-existing `contains(package)` guard in
    /// `raw_value/reporting.rs`, not something introduced or fixed here).
    pub(super) fn decode_set_cell_path_member(
        &self,
        path_bytes: &[u8],
        set_data_type: &str,
        element_type: &str,
        column_name: &str,
    ) -> Result<Value> {
        if path_bytes.is_empty() {
            if let Some(tag) = self.set_element_empty_sentinel(set_data_type) {
                return Ok(Value::Empty(tag));
            }
        }
        let decoded = self.parse_value_from_raw_bytes(path_bytes, element_type, column_name, 0)?;
        if path_bytes.is_empty() && matches!(Self::peeled_for_inspection(&decoded), Value::Null) {
            return Err(Error::corruption(format!(
                "Set member '{column_name}' of type '{element_type}': a zero-length \
                 cell path decodes to no value for this type, and Cassandra's own \
                 `validateCellPath` rejects it (issue #4106)"
            )));
        }
        Ok(decoded)
    }
}
