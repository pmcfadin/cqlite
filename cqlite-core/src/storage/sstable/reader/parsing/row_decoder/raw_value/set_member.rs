//! Decode ONE member of a multicell `set<T>` from its cell path (issue #3723).
//!
//! In the Cassandra 5.0 complex-cell layout a live `set` element carries
//! `HAS_EMPTY_VALUE` (0x04) and stores the element IN THE CELL PATH, so the
//! member value is the path bytes decoded as the element type — see
//! `db/rows/BufferCell.java` / `db/marshal/SetType.java` (`valueComparator()` is
//! `EmptyType`; the element lives in the `CellPath`) at the pinned
//! `cassandra-5.0.8` tag.
//!
//! This module exists to hold THREE decisions that were previously inline in the
//! set branch of `complex_column.rs`, all of them silent-data-loss holes closed
//! by issue #3723:
//!
//! 1. **An EMPTY path is decoded, not skipped.** The old guard was
//!    `else if !cell.path_bytes.is_empty()`, so a zero-length path bypassed the
//!    element decoder entirely and the member was silently OMITTED from the set
//!    — never reaching the fixed-width length guard that this issue's AC2 says
//!    must refuse a zero-length fixed-width element (rationale, including why
//!    Cassandra's own `Int32Serializer.validate` admits `4 or 0` while this
//!    decoder does not, is in `fixed_width.rs`).
//! 2. **A fatal decode error propagates.** A `FixedWidthLengthMismatch` is input
//!    Cassandra REFUSES outright (`SetSerializer.validate` lets the element-level
//!    `MarshalException` escape and additionally throws on extraneous bytes), so
//!    surfacing it as a set that is quietly missing a member is not an option.
//!    Every OTHER decode error keeps its pre-#3723 tolerant `None` — see
//!    [`super::is_fatal_decode_error`] for why the fatal set is exactly one
//!    variant.
//! 3. **The path is decoded BEFORE the legacy cell-value fallback.** The first
//!    #3723 revision returned a non-empty cell value early, so the two guards
//!    above were reachable only when the cell carried no value — a correctly
//!    sized cell value let a malformed member path through untouched. Cassandra
//!    validates the path unconditionally and its read path never reads a set
//!    cell's value at all; the authority, and why the RETURNED value still
//!    stays pre-#3723, are recorded at the ordering itself in
//!    [`V5CompressedLegacyParser::decode_set_member`].

use super::*;

impl V5CompressedLegacyParser {
    /// Decode one multicell-`set` member.
    ///
    /// * `cell_value` — the cell's VALUE, when the cell unusually carries one
    ///   (no `HAS_EMPTY_VALUE`). It still wins as the returned value, exactly as
    ///   before issue #3723, but only AFTER the path has been decoded and
    ///   accepted — never instead of decoding it.
    /// * `path_bytes` — the cell PATH, which for a set IS the element value.
    ///
    /// Returns `Ok(None)` when the member could not be decoded by a TOLERATED
    /// error class (the pre-#3723 behaviour: omit the member, keep the set), and
    /// `Err` when the failure is one this decoder must not tolerate.
    pub(in crate::storage::sstable::reader::parsing::row_decoder) fn decode_set_member(
        &self,
        cell_value: Option<Value>,
        path_bytes: &[u8],
        element_type: &str,
        column_name: &str,
        element_index: u64,
    ) -> Result<Option<Value>> {
        // The PATH is decoded FIRST, UNCONDITIONALLY — before the legacy
        // cell-value fallback below (issue #3723, review round 2). An early
        // `return` on `cell_value` meant a set cell carrying a non-empty value
        // never decoded its path at all, so a malformed fixed-width member path
        // bypassed the very width guard this branch exists to apply.
        //
        // NOTE (#3723): NO `is_empty()` pre-filter here, deliberately — a
        // zero-length path must reach the element decoder so the width guard,
        // not a silent omission, decides it.
        let path_member =
            match self.parse_value_from_raw_bytes(path_bytes, element_type, column_name, 0) {
                Ok(val) => Some(val),
                Err(e) if is_fatal_decode_error(&e) => return Err(e),
                Err(e) => {
                    tracing::debug!(
                        "V5CompressedLegacy: set element {} parse failed (type={}): {}",
                        element_index,
                        element_type,
                        e
                    );
                    None
                }
            };
        // Ordering authority — pinned `cassandra-5.0.8`:
        //
        // * `schema/ColumnMetadata.java` `validateCell(...)`: for a LIVE
        //   (non-tombstone), non-UDT cell it runs
        //   `type.validateCellValue(cell.value(), ...)` and then, with NO
        //   condition on the value, `validateCellPath(cell.path())` →
        //   `((CollectionType) type).nameComparator().validate(path.get(0))`,
        //   i.e. the ELEMENT type's own `validate`. The path check is therefore
        //   NOT gated on the cell value being absent/empty: a wrong-width member
        //   path is refused whether or not a value is present. That is exactly
        //   the property the early return broke.
        // * `db/marshal/CollectionType.java` `serializeForNativeProtocol(...)` →
        //   `SetType.serializedValues(...)`: the READ path builds the set from
        //   `cells.next().path().get(0)` and never reads the cell value at all.
        //
        // Of the two options considered this is (a) — always decode the path,
        // keep the cell value only as the returned `Value` once the path has
        // been accepted — and NOT (b) "a non-empty value on a set cell is
        // invalid outright". (b) is what `SetType.valueComparator()` implies
        // (`EmptyType`, whose `serializers/EmptySerializer.java` `validate`
        // throws `"EmptyType only accept empty values"`), but that refusal lives
        // in `validateCell`, which the read path does NOT run — so making it
        // fatal HERE would be a new refusal class under no read-path oracle,
        // outside this issue's one-variant fatal set.
        //
        // Which value is RETURNED for such an unusual cell also stays exactly
        // pre-#3723 (the cell value wins over the path). `serializedValues`
        // above says Cassandra's read path would use the PATH, so that is a
        // known divergence in a case no Cassandra writer emits (a live set
        // element always carries `HAS_EMPTY_VALUE`); changing the returned data
        // is a LOOSENING/behaviour change this issue's strict-rejection scope
        // does not cover, and it needs its own oracle-backed change.
        if let Some(val) = cell_value {
            return Ok(Some(val));
        }
        Ok(path_member)
    }
}
