//! Decode ONE member of a multicell `set<T>` from its cell path (issue #3723).
//!
//! In the Cassandra 5.0 complex-cell layout a live `set` element carries
//! `HAS_EMPTY_VALUE` (0x04) and stores the element IN THE CELL PATH, so the
//! member value is the path bytes decoded as the element type — see
//! `db/rows/BufferCell.java` / `db/marshal/SetType.java` (`valueComparator()` is
//! `EmptyType`; the element lives in the `CellPath`) at the pinned
//! `cassandra-5.0.8` tag.
//!
//! This module exists to hold TWO decisions that were previously inline in the
//! set branch of `complex_column.rs`, both of them silent-data-loss holes closed
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

use super::*;

impl V5CompressedLegacyParser {
    /// Decode one multicell-`set` member.
    ///
    /// * `cell_value` — the cell's VALUE, when the cell unusually carries one
    ///   (no `HAS_EMPTY_VALUE`); it wins, exactly as before issue #3723.
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
        if let Some(val) = cell_value {
            return Ok(Some(val));
        }
        // NOTE (#3723): NO `is_empty()` pre-filter here, deliberately — a
        // zero-length path must reach the element decoder so the width guard,
        // not a silent omission, decides it.
        match self.parse_value_from_raw_bytes(path_bytes, element_type, column_name, 0) {
            Ok(val) => Ok(Some(val)),
            Err(e) if is_fatal_decode_error(&e) => Err(e),
            Err(e) => {
                tracing::debug!(
                    "V5CompressedLegacy: set element {} parse failed (type={}): {}",
                    element_index,
                    element_type,
                    e
                );
                Ok(None)
            }
        }
    }
}
