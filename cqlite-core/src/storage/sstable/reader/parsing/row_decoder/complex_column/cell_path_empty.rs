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

use super::*;
use crate::types::EmptyValueType;

impl V5CompressedLegacyParser {
    /// The typed sentinel a zero-length cell path denotes for a component of
    /// DECLARED type `type_str`, or `None` when no authority admits an empty
    /// buffer there.
    ///
    /// Delegates entirely: the type spelling is normalized by
    /// [`Self::cell_path_key_cql_type`] (the module's ONE classifier, CQL short
    /// form and marshal alike) and the admission is
    /// [`EmptyValueType::for_cql_type`]. Nothing per-family is decided here — see
    /// the module header for why a second opinion at this level is the drift this
    /// file exists to prevent.
    pub(super) fn cell_path_empty_sentinel(&self, type_str: &str) -> Option<EmptyValueType> {
        self.cell_path_key_cql_type(type_str)
            .as_ref()
            .and_then(EmptyValueType::for_cql_type)
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
    pub(super) fn decode_set_cell_path_member(
        &self,
        path_bytes: &[u8],
        element_type: &str,
        column_name: &str,
    ) -> Result<Value> {
        if path_bytes.is_empty() {
            if let Some(tag) = self.cell_path_empty_sentinel(element_type) {
                return Ok(Value::Empty(tag));
            }
        }
        let decoded = self.parse_value_from_raw_bytes(path_bytes, element_type, column_name, 0)?;
        if path_bytes.is_empty() && matches!(Self::peeled_for_inspection(&decoded), Value::Null) {
            return Err(Error::corruption(format!(
                "Set member '{column_name}' of type '{element_type}': a zero-length cell                  path decodes to no value for this type, and Cassandra's own                  `validateCellPath` rejects it (issue #4106)"
            )));
        }
        Ok(decoded)
    }
}
