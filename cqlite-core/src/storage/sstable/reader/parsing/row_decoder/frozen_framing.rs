//! Frozen-collection and tuple FRAMING, parameterized by the element decoder
//! (issue #3722).
//!
//! # Why this module exists
//!
//! The `*_raw` framing loops in [`super::frozen`] hardcoded their element decode
//! as `parse_value_from_raw_bytes(elem_data, element_type: &str, ...)`. A UDT
//! field typed `list<frozen<inner_u>>` cannot use that: rendering the element
//! `CqlType::Udt(name, inline_fields)` to a string yields a BARE NAME and DROPS
//! the inline field definitions, which nothing downstream can recover — with no
//! `UdtRegistry` (a schema-less read) the element then falls back to
//! `Value::Blob`. That is the same reason `udt_field.rs`'s `Udt` FIELD arm
//! recurses structurally, applied one level down.
//!
//! So the framing takes the element decode as a CALLBACK: the byte-level
//! framing (`[i32 BE count]`, then per element `[i32 BE len][bytes]`), the
//! bounds checks, the error messages and the offset arithmetic exist ONCE, and
//! each caller supplies how an element's bytes become a `Value`:
//!
//! * [`super::frozen`]'s `element_type: &str` entry points pass a closure
//!   calling `parse_value_from_raw_bytes` — so every pre-#3722 caller of those
//!   functions is BIT-IDENTICAL to before (same bounds checks, same messages,
//!   same offsets, same element decoder, same depth).
//! * `udt_field.rs` passes a closure calling `parse_udt_field_value` with the
//!   STRUCTURED `&CqlType` element, keeping the inline field defs.
//!
//! A second, `CqlType`-keyed copy of these loops would be ~100 lines of
//! duplicated bounds checking — precisely the two-divergent-implementations
//! defect class issue #3722 exists to remove. Hence a callback, not a copy.
//!
//! The bodies below were MOVED verbatim from `frozen.rs` (only the element
//! decode line is parameterized); the move also keeps that file under the
//! campsite file-size ratchet (epic #1116) instead of growing it.

use super::*;

impl V5CompressedLegacyParser {
    /// Frozen list/set framing, raw (no VUInt cell-value-length prefix — the
    /// caller has already bounded `data`). `as_set = true` produces
    /// `Value::Set`.
    ///
    /// Returns the decoded value and the offset ONE PAST the last consumed byte,
    /// so a caller that knows the exact extent of its value can require full
    /// consumption.
    ///
    /// `decode_element` receives one element's exact bytes.
    pub(super) fn parse_frozen_sequence_raw_with(
        &self,
        data: &[u8],
        mut offset: usize,
        column_name: &str,
        as_set: bool,
        decode_element: &dyn Fn(&[u8]) -> Result<Value>,
    ) -> Result<(Value, usize)> {
        let kind = if as_set { "set" } else { "list" };
        let count = Self::read_frozen_count(data, &mut offset, data.len(), kind, column_name)?;

        tracing::debug!(
            "V5CompressedLegacy: Parsing frozen {} '{}' with {} elements (raw)",
            kind,
            column_name,
            count
        );

        let mut elements = Vec::with_capacity(count);
        for i in 0..count {
            // Each element in a frozen collection: [i32 BE len][element bytes]
            if offset + 4 > data.len() {
                return Err(Error::corruption(format!(
                    "Frozen {} '{}': not enough bytes for element {} length",
                    kind, column_name, i
                )));
            }
            let elem_len_i32 = i32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]);
            if elem_len_i32 < 0 {
                return Err(Error::corruption(format!(
                    "Frozen {} '{}': negative element {} length {}",
                    kind, column_name, i, elem_len_i32
                )));
            }
            let elem_len = elem_len_i32 as usize;
            offset += 4;

            if offset + elem_len > data.len() {
                return Err(Error::corruption(format!(
                    "Frozen {} '{}': element {} needs {} bytes but only {} available",
                    kind,
                    column_name,
                    i,
                    elem_len,
                    data.len() - offset
                )));
            }

            let elem_data = &data[offset..offset + elem_len];
            let elem_value = decode_element(elem_data)?;
            elements.push(elem_value);
            offset += elem_len;
        }

        if as_set {
            Ok((Value::Set(elements), offset))
        } else {
            Ok((Value::List(elements), offset))
        }
    }

    /// Frozen map framing, raw. `decode_key`/`decode_value` receive one key's /
    /// one value's exact bytes; they are separate closures because a map's two
    /// element types are decoded independently.
    pub(super) fn parse_frozen_map_raw_with(
        &self,
        data: &[u8],
        mut offset: usize,
        column_name: &str,
        decode_key: &dyn Fn(&[u8]) -> Result<Value>,
        decode_value: &dyn Fn(&[u8]) -> Result<Value>,
    ) -> Result<(Value, usize)> {
        let count = Self::read_frozen_count(data, &mut offset, data.len(), "map", column_name)?;

        tracing::debug!(
            "V5CompressedLegacy: Parsing frozen map '{}' with {} entries (raw)",
            column_name,
            count
        );

        let mut entries = Vec::with_capacity(count);
        for i in 0..count {
            // Key: [i32 BE len][key bytes]
            if offset + 4 > data.len() {
                return Err(Error::corruption(format!(
                    "Frozen map '{}': not enough bytes for key {} length",
                    column_name, i
                )));
            }
            let key_len_i32 = i32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]);
            if key_len_i32 < 0 {
                return Err(Error::corruption(format!(
                    "Frozen map '{}': negative key {} length {}",
                    column_name, i, key_len_i32
                )));
            }
            let key_len = key_len_i32 as usize;
            offset += 4;

            if offset + key_len > data.len() {
                return Err(Error::corruption(format!(
                    "Frozen map '{}': key {} needs {} bytes but only {} available",
                    column_name,
                    i,
                    key_len,
                    data.len() - offset
                )));
            }
            let key_data = &data[offset..offset + key_len];
            let key_value = decode_key(key_data)?;
            offset += key_len;

            // Value: [i32 BE len][value bytes]
            if offset + 4 > data.len() {
                return Err(Error::corruption(format!(
                    "Frozen map '{}': not enough bytes for value {} length",
                    column_name, i
                )));
            }
            let val_len_i32 = i32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]);
            if val_len_i32 < 0 {
                return Err(Error::corruption(format!(
                    "Frozen map '{}': negative value {} length {}",
                    column_name, i, val_len_i32
                )));
            }
            let val_len = val_len_i32 as usize;
            offset += 4;

            if offset + val_len > data.len() {
                return Err(Error::corruption(format!(
                    "Frozen map '{}': value {} needs {} bytes but only {} available",
                    column_name,
                    i,
                    val_len,
                    data.len() - offset
                )));
            }
            let val_data = &data[offset..offset + val_len];
            let val_value = decode_value(val_data)?;
            offset += val_len;

            entries.push((key_value, val_value));
        }

        Ok((Value::Map(entries), offset))
    }

    /// Tuple-element framing over an already-bounded slice: `element_count`
    /// elements of `[i32 BE len][bytes]`, `-1` meaning null.
    ///
    /// `decode_element` receives `(index, element bytes, element description)` —
    /// the index selects the element's type in the caller's own representation
    /// (a `&[String]` or a `&[CqlType]`), and the description is the string this
    /// framing puts in element-level error messages.
    pub(super) fn parse_tuple_elements_raw_with(
        &self,
        data: &[u8],
        offset: &mut usize,
        blob_end: usize,
        element_count: usize,
        column_name: &str,
        decode_element: &dyn Fn(usize, &[u8], &str) -> Result<Value>,
    ) -> Result<Vec<Value>> {
        let mut elements = Vec::with_capacity(element_count);

        for idx in 0..element_count {
            let elem_desc = format!("tuple '{}' element {}", column_name, idx);

            // Need at least 4 bytes for the element length
            if *offset + 4 > blob_end {
                // Trailing elements are implicitly null (matches UDT behaviour)
                tracing::debug!(
                    "Tuple '{}': element {} beyond blob_end, treating as null",
                    column_name,
                    idx
                );
                elements.push(Value::Null);
                continue;
            }

            // Read element length (4-byte big-endian i32)
            let elem_len_i32 = i32::from_be_bytes([
                data[*offset],
                data[*offset + 1],
                data[*offset + 2],
                data[*offset + 3],
            ]);
            *offset += 4;

            if elem_len_i32 == -1 {
                // Null element
                elements.push(Value::Null);
                continue;
            }

            if elem_len_i32 < -1 {
                return Err(Error::corruption(format!(
                    "{}: invalid negative element length {}",
                    elem_desc, elem_len_i32
                )));
            }

            let elem_len = elem_len_i32 as usize;

            if *offset + elem_len > blob_end {
                return Err(Error::corruption(format!(
                    "{}: needs {} bytes but only {} available in blob",
                    elem_desc,
                    elem_len,
                    blob_end - *offset
                )));
            }

            let elem_data = &data[*offset..*offset + elem_len];
            let value = decode_element(idx, elem_data, &elem_desc)?;
            *offset += elem_len;

            elements.push(value);
        }

        Ok(elements)
    }
}
