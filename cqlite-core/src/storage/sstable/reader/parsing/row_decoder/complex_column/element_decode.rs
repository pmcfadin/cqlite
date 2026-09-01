//! ONE decoder per multicell branch for a MAP entry's KEY and a top-level
//! non-frozen UDT FIELD's value — shared by the live path and by the #1741
//! shadow/TTL DROPPED path (issue #3723, review round 5).
//!
//! ## Why this module exists
//!
//! Round 3 moved the fixed-width guard AHEAD of the `dropped` early return for
//! multicell SETS (`raw_value/set_member.rs`, "the member is validated even when
//! it is DROPPED"). The map and UDT branches of `complex_column` still
//! `continue`d before their own decode, so malformed fixed-width bytes returned
//! SUCCESS whenever the affected entry/field happened to be shadowed by a
//! covering deletion or expired at the read clock. `element_dropped` is a
//! read-clock / covering-deletion decision and the width guard is a statement
//! about the BYTES, so one must not be conditioned on the other — a contract
//! that holds only for unshadowed elements is not a contract.
//!
//! Authority (pinned `cassandra-5.0.8`, never CQLite's prior output):
//! `schema/ColumnMetadata.java` `validateCell(...)` validates the cell value and
//! then, with no condition, `validateCellPath(cell.path())` for EVERY live cell
//! it is handed; nothing in that validation consults a covering deletion or the
//! read clock (reconciliation, `db/rows/Cells.java`, is a separate and LATER
//! concern). The element-level refusal itself is
//! `serializers/Int32Serializer.java` `validate(...)` — `"Expected 4 or 0 byte
//! int (%d)"` — which `ListSerializer`/`SetSerializer.validate` let escape for
//! the whole value.
//!
//! ## The dropped path is more TOLERANT than the live path, deliberately
//!
//! The two live paths propagate EVERY decode error (`?`), and that is unchanged.
//! On the DROPPED path only [`is_fatal_decode_error`] — the ONE variant issue
//! #3723 added, a WRONG-width [`Error::FixedWidthLengthMismatch`] — propagates;
//! every other failure keeps the pre-#3723 outcome (the element is filtered, the
//! read succeeds). That asymmetry is the point rather than an oversight: this
//! round's subject is the WIDTH GUARD escaping observability behind an unrelated
//! shadow, and promoting the other classes for dropped elements would make
//! today's successful reads fail on data the read path never returns — a new
//! refusal surface with no oracle behind it, outside the one-variant fatal set
//! (`raw_value/fatal_decode_error.rs`). No new fatal class is introduced and no
//! tolerated error becomes fatal.
//!
//! For the same reason a DROPPED UDT field whose declared field INDEX is out of
//! range is left alone: that refusal is `Error::Corruption`, not the fatal
//! variant, so validation is skipped rather than promoted (the live path's
//! refusal is untouched).
//!
//! Decoding a value that is then discarded is deliberate wasted work; the
//! cheaper shapes were rejected for the reasons recorded in
//! `raw_value/set_member.rs` (a second width table that can drift, and a
//! validate-only mode that would change `parse_value_from_raw_bytes`'s
//! signature).

use super::super::raw_value::is_fatal_decode_error;
use super::*;

impl V5CompressedLegacyParser {
    /// Decode a multicell MAP entry's key from its cell path, for an entry that
    /// SURVIVES the #1741 filter. Every decode error propagates, exactly as
    /// before issue #3723 round 5.
    ///
    /// `Ok(None)` only for an EMPTY cell path, whose pre-existing disposition
    /// (the entry is omitted from the collapsed map) this round does not change.
    pub(super) fn decode_map_entry_key(
        &self,
        path_bytes: &[u8],
        key_type: &str,
        column_name: &str,
        opaque_out: &mut bool,
    ) -> Result<Option<Value>> {
        self.decode_map_entry_key_inner(path_bytes, key_type, column_name, opaque_out, false)
    }

    /// Width-validate the key of a MAP entry the #1741 filter is DROPPING,
    /// discarding the decoded key.
    ///
    /// The undecodable-key signal is deliberately NOT aggregated: a dropped
    /// entry is never surfaced, so counting it would change the operator-facing
    /// `opaque_key_entries` warning for a filtered entry.
    pub(super) fn validate_dropped_map_key(
        &self,
        path_bytes: &[u8],
        key_type: &str,
        column_name: &str,
    ) -> Result<()> {
        let mut ignored = false;
        self.decode_map_entry_key_inner(path_bytes, key_type, column_name, &mut ignored, true)
            .map(|_| ())
    }

    fn decode_map_entry_key_inner(
        &self,
        path_bytes: &[u8],
        key_type: &str,
        column_name: &str,
        opaque_out: &mut bool,
        dropped: bool,
    ) -> Result<Option<Value>> {
        if path_bytes.is_empty() {
            return Ok(None);
        }
        tracing::debug!(
            "V5CompressedLegacy: Parsing map key for column '{}', key_type='{}', path_len={}, dropped={}",
            column_name,
            key_type,
            path_bytes.len(),
            dropped
        );
        // Cell path keys carry NO length prefix — the whole slice is the key.
        match self.parse_cell_path_key_reporting(path_bytes, key_type, column_name, opaque_out) {
            Ok(decoded) => Ok(Some(decoded)),
            // A LIVE entry propagates everything (`!dropped`), unchanged. A
            // DROPPED entry propagates ONLY the one fatal variant — see the
            // module header.
            Err(e) if !dropped || is_fatal_decode_error(&e) => Err(e),
            Err(e) => {
                tracing::debug!(
                    "V5CompressedLegacy: dropped map entry key parse failed (type={}): {} \
                     — tolerated class, entry filtered (issue #3723)",
                    key_type,
                    e
                );
                Ok(None)
            }
        }
    }

    /// Decode ONE top-level non-frozen UDT field's value with its DECLARED type,
    /// for a field that SURVIVES the #1741 filter. Every decode error
    /// propagates, exactly as before issue #3723 round 5.
    ///
    /// `cell_value` is the cell value captured as raw bytes (`BytesType` is
    /// identity); a non-`Blob` value (including `None`) is returned as-is.
    pub(super) fn decode_udt_field_value(
        &self,
        cell_value: &Option<Value>,
        field_defs: &[(String, String)],
        field_index: i32,
        column_name: &str,
        cell_index: u64,
    ) -> Result<Option<Value>> {
        self.decode_udt_field_value_inner(
            cell_value,
            field_defs,
            field_index,
            column_name,
            cell_index,
            false,
        )
    }

    /// Width-validate the declared value of a UDT field the #1741 filter is
    /// DROPPING, discarding the decoded value.
    pub(super) fn validate_dropped_udt_field(
        &self,
        cell_value: &Option<Value>,
        field_defs: &[(String, String)],
        field_index: i32,
        column_name: &str,
        cell_index: u64,
    ) -> Result<()> {
        self.decode_udt_field_value_inner(
            cell_value,
            field_defs,
            field_index,
            column_name,
            cell_index,
            true,
        )
        .map(|_| ())
    }

    fn decode_udt_field_value_inner(
        &self,
        cell_value: &Option<Value>,
        field_defs: &[(String, String)],
        field_index: i32,
        column_name: &str,
        cell_index: u64,
        dropped: bool,
    ) -> Result<Option<Value>> {
        let Some(Value::Blob(raw)) = cell_value else {
            return Ok(cell_value.clone());
        };
        let resolved = field_defs
            .get(field_index as usize)
            .filter(|_| field_index >= 0);
        let field_type = match resolved {
            Some((_name, field_type)) => field_type,
            // A DROPPED field with an unresolvable index carries no declared
            // type to validate against, and the live path's refusal here is
            // `Error::Corruption` rather than the fatal variant — so it is left
            // exactly as it was (module header).
            None if dropped => return Ok(None),
            None => {
                return Err(Error::corruption(format!(
                    "UDT column '{}' cell {}: field index {} out of range (0..{})",
                    column_name,
                    cell_index,
                    field_index,
                    field_defs.len()
                )))
            }
        };
        match self.parse_value_from_raw_bytes(raw, field_type, column_name, 0) {
            Ok(decoded) => Ok(Some(decoded)),
            // Same discrimination as the map key above.
            Err(e) if !dropped || is_fatal_decode_error(&e) => Err(e),
            Err(e) => {
                tracing::debug!(
                    "V5CompressedLegacy: dropped UDT field {} value parse failed (type={}): {} \
                     — tolerated class, field filtered (issue #3723)",
                    field_index,
                    field_type,
                    e
                );
                Ok(None)
            }
        }
    }
}
