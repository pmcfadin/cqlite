//! The frozen-collection PREAMBLE: the VUInt outer blob length and the i32 BE
//! element count that precede every frozen collection body.
//!
//! Split out of `frozen.rs` under the campsite rule (epic #1116) alongside the
//! issue #3848 fix. The `impl` block is unchanged, so both helpers keep their
//! path and visibility; `read_frozen_preamble` is `pub(super)` so the #3848
//! regression module can call it directly with no reader/dataset dependency.

use super::*;

impl V5CompressedLegacyParser {
    /// Read i32 BE element/entry count from a frozen collection blob.
    ///
    /// `bound` is the exclusive upper byte index for the collection data (either
    /// `data.len()` for raw variants or `blob_end` for cell-level variants).
    pub(super) fn read_frozen_count(
        data: &[u8],
        offset: &mut usize,
        bound: usize,
        collection_kind: &str,
        column_name: &str,
    ) -> Result<usize> {
        if *offset + 4 > bound {
            return Err(Error::corruption(format!(
                "Frozen {} '{}': not enough bytes for element count",
                collection_kind, column_name
            )));
        }
        let count = i32::from_be_bytes([
            data[*offset],
            data[*offset + 1],
            data[*offset + 2],
            data[*offset + 3],
        ]);
        *offset += 4;

        if count < 0 {
            return Err(Error::corruption(format!(
                "Frozen {} '{}': negative element count {}",
                collection_kind, column_name, count
            )));
        }
        let count = count as usize;
        if count > MAX_FROZEN_COLLECTION_SIZE as usize {
            return Err(Error::corruption(format!(
                "Frozen {} '{}': element count {} exceeds maximum {}",
                collection_kind, column_name, count, MAX_FROZEN_COLLECTION_SIZE
            )));
        }
        Ok(count)
    }

    /// Read the frozen collection preamble: VUInt blob_len + i32 BE element count.
    ///
    /// Returns `(count, blob_end)` with `offset` advanced past the preamble.
    pub(super) fn read_frozen_preamble(
        data: &[u8],
        offset: &mut usize,
        collection_kind: &str,
        column_name: &str,
    ) -> Result<(usize, usize)> {
        let (remaining, blob_len_raw) = parse_vuint(&data[*offset..]).map_err(|e| {
            Error::corruption(format!(
                "Frozen {} '{}': failed to parse blob length: {:?}",
                collection_kind, column_name, e
            ))
        })?;
        // Issue #3848: cap BEFORE the `as usize` cast. `parse_vuint` yields up to
        // `u64::MAX` from a 9-byte encoding, so an adversarial `Data.db` length
        // prefix must be rejected here rather than reaching the bounds add below
        // (which, unchecked, panics in an overflow-checked build and wraps in
        // release). Same guard as `parse_tuple_value` on the identical framing.
        if blob_len_raw > MAX_CELL_VALUE_LENGTH {
            return Err(Error::corruption(format!(
                "Frozen {} '{}': blob_len {} exceeds maximum {}",
                collection_kind, column_name, blob_len_raw, MAX_CELL_VALUE_LENGTH
            )));
        }
        let blob_len = blob_len_raw as usize;
        let bytes_consumed = data[*offset..].len() - remaining.len();
        *offset += bytes_consumed;

        // Issue #3848, second axis: the saturating form cannot overflow for ANY
        // `blob_len`, so it holds even if the cap above is ever removed.
        if blob_len > data.len().saturating_sub(*offset) {
            return Err(Error::corruption(format!(
                "Frozen {} '{}': blob_len {} exceeds available data {}",
                collection_kind,
                column_name,
                blob_len,
                data.len().saturating_sub(*offset)
            )));
        }

        let blob_end = *offset + blob_len;
        let count = Self::read_frozen_count(data, offset, blob_end, collection_kind, column_name)?;
        Ok((count, blob_end))
    }
}
