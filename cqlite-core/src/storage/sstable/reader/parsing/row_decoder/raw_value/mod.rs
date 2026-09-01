//! The bounded raw-value decoder: dispatch from a CQL/marshal type name to the
//! arm that decodes a fully bounded value slice.
//!
//! Campsite split (epic #1116 / issue #3723): the marshal→short-form
//! normalization lives in [`marshal_short`], the fixed-width scalar arms (and
//! their width guards) in [`fixed_width`], and the tests in `tests.rs` +
//! `nested_fixed_width_length_tests.rs`.

use super::*;

// Issue #3723: the fatal-vs-tolerated decode-error discrimination, shared by
// the complex-column loop (`row_data`) and the multicell-set member decode
// (`set_member`). Read its module header before widening the set.
mod fatal_decode_error;
pub(super) use fatal_decode_error::is_fatal_decode_error;
mod fixed_width;
mod marshal_short;
// Issue #3723: ONE multicell-set member decoded from its cell path.
mod set_member;
#[cfg(test)]
mod set_member_tests;

#[cfg(test)]
mod tests;

// Issue #3723: a nested fixed-width collection/tuple element whose `[i32 BE len]`
// prefix declares a WRONG length must be REFUSED.
#[cfg(test)]
mod nested_fixed_width_length_tests;

impl V5CompressedLegacyParser {
    /// Parse a value from a complete, bounded byte slice.
    ///
    /// This is used when the outer Cassandra collection format already provides
    /// explicit `[i32 BE len][raw bytes]` boundaries and we have extracted exactly
    /// the bytes that constitute the value. The entire `data` slice IS the value.
    ///
    /// - Variable-width types (text, blob, varint, decimal, inet): consume the full slice
    /// - Fixed-width types (int, bigint, uuid, etc.): read from offset 0
    /// - Nested collections: use the bounded sub-format `[i32 BE count][i32 BE len][bytes]...`
    pub(super) fn parse_value_from_raw_bytes(
        &self,
        data: &[u8],
        type_str: &str,
        column_name: &str,
        depth: usize,
    ) -> Result<Value> {
        if depth > MAX_TYPE_NESTING_DEPTH {
            return Err(Error::corruption(format!(
                "Frozen element '{}': recursion depth {} exceeds maximum {}",
                column_name, depth, MAX_TYPE_NESTING_DEPTH
            )));
        }
        // Issue #1081: scalar marshal forms (e.g.
        // `org.apache.cassandra.db.marshal.Int32Type` / `BooleanType`) reach this
        // function for multicell-UDT field values, which resolve their field
        // types from the authoritative on-disk `UserType(...)` marshal string.
        // The match below only enumerates short forms plus a handful of text
        // marshal aliases, so a bare scalar marshal type would otherwise fall
        // through to the blob default. Normalize a primitive marshal type to its
        // canonical CQL short form (via the existing authoritative marshal→CqlType
        // mapping, no heuristics) and re-dispatch. Composite/UDT marshal forms
        // (UserType/ListType/MapType/SetType/etc.) are left untouched here — they
        // are handled by the dedicated arms below — so this only rewrites scalars.
        if type_str.contains("org.apache.cassandra.db.marshal.") {
            if let Some(short) = Self::primitive_marshal_to_cql_short(type_str) {
                return self.parse_value_from_raw_bytes(data, short, column_name, depth);
            }
        }

        // Preserve the ORIGINAL-CASE type string. Below, the `match` scrutinee is
        // `type_str.to_lowercase()` and each `type_str if ...` arm binding SHADOWS
        // the function parameter with the lowercased string. The collection/tuple/
        // frozen extraction helpers slice their element/inner types out of the
        // string they are handed, so if we passed the lowercased binding the nested
        // element marshal type would come back lowercased (e.g. `...int32type`) and
        // would NOT re-normalize via the CASE-SENSITIVE `primitive_marshal_to_cql_short`
        // suffix match, wrongly falling through to blob. The marshal-form arms below
        // therefore extract from `raw_type_str` (original case) so nested element
        // marshal types keep their case. The CQL-short-form arms are unaffected
        // because their inner types are already canonical lowercase.
        let raw_type_str = type_str;
        let normalized_type = type_str.to_lowercase();
        match normalized_type.as_str() {
            "text"
            | "varchar"
            | "ascii"
            | "org.apache.cassandra.db.marshal.utf8type"
            | "org.apache.cassandra.db.marshal.asciitype"
            | "org.apache.cassandra.db.marshal.varchartype" => {
                // Issue #1644 (K5 stage 2): validate in place, borrow if possible.
                std::str::from_utf8(data).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen element '{}': invalid UTF-8 in text value: {}",
                        column_name, e
                    ))
                })?;
                Ok(Value::Text(
                    crate::storage::sstable::reader::value_borrow::borrow_active(data),
                ))
            }
            "blob" | "bytes" => Ok(Value::Blob(
                crate::storage::sstable::reader::value_borrow::borrow_active(data),
            )),
            // Fixed-width scalar arms (issue #3723). The slice reaching this
            // function is already bounded by its own `[i32 BE len]` element /
            // field prefix, so `data.len()` IS the declared length and the width
            // guard belongs in the arm itself — see
            // [`fixed_width`].
            short if Self::fixed_width_admissible_width(short).is_some() => {
                Self::decode_fixed_width_raw(short, data, column_name)
            }
            "duration" => {
                // Issue #1081: in this function the entire `data` slice IS the value
                // (the element/cell length prefix already bounded it) — there is NO
                // outer `[VInt len]` prefix. Decode three consecutive SIGNED VInts
                // directly over `data`: months, days, nanos (Cassandra
                // DurationSerializer). Contrast `parse_raw_type_value`'s duration arm,
                // which reads an outer `[VInt len]` first because its framing differs.
                let (remaining, months) = parse_vint(data).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen element '{}': failed to parse duration months: {:?}",
                        column_name, e
                    ))
                })?;
                let pos = data.len() - remaining.len();

                let (remaining, days) = parse_vint(&data[pos..]).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen element '{}': failed to parse duration days: {:?}",
                        column_name, e
                    ))
                })?;
                let pos = data.len() - remaining.len();

                let (_remaining, nanos) = parse_vint(&data[pos..]).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen element '{}': failed to parse duration nanos: {:?}",
                        column_name, e
                    ))
                })?;

                // months/days are i32 in Cassandra's DurationType. Reject
                // (rather than silently truncate via `as i32`) any encoded value
                // outside the i32 range so a corrupt encoding errors instead of
                // wrapping (issue #1632, item b).
                let months = i32::try_from(months).map_err(|_| {
                    Error::corruption(format!(
                        "Frozen element '{}': duration months out of i32 range",
                        column_name
                    ))
                })?;
                let days = i32::try_from(days).map_err(|_| {
                    Error::corruption(format!(
                        "Frozen element '{}': duration days out of i32 range",
                        column_name
                    ))
                })?;
                Ok(Value::Duration {
                    months,
                    days,
                    nanos,
                })
            }
            "varint" => Ok(Value::Varint(
                crate::storage::sstable::reader::value_borrow::borrow_active(data),
            )),
            "decimal" => {
                if data.len() < 4 {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': decimal too short ({} bytes)",
                        column_name,
                        data.len()
                    )));
                }
                let scale = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                let unscaled = data[4..].to_vec();
                Ok(Value::Decimal { scale, unscaled })
            }
            "inet" => Ok(Value::Inet(
                crate::storage::sstable::reader::value_borrow::borrow_active(data),
            )),
            // Nested list/set/map inside a bounded element (e.g. map<text, list<int>>).
            //
            // Issue #1081: the guards accept BOTH the CQL short form (`list<...>`)
            // and the authoritative Cassandra marshal form
            // (`org.apache.cassandra.db.marshal.ListType(...)`). Multicell-UDT field
            // values resolve their field types from the on-disk `UserType(...)`
            // marshal string, so a collection-typed UDT field arrives here in marshal
            // form and would otherwise fall through to the blob default. The
            // extraction helpers (`extract_collection_element_type` / `extract_map_types`)
            // already accept marshal forms; we extract from `raw_type_str`
            // (original case) so the returned nested element marshal type keeps its
            // case and re-normalizes correctly on recursion (see note above).
            type_str
                if type_str.starts_with("list<")
                    || type_str.starts_with("org.apache.cassandra.db.marshal.listtype(") =>
            {
                let element_type = self.extract_collection_element_type(raw_type_str, "list")?;
                let (val, _) = self.parse_frozen_list_value_raw(
                    data,
                    0,
                    &element_type,
                    column_name,
                    depth + 1,
                )?;
                Ok(val)
            }
            type_str
                if type_str.starts_with("set<")
                    || type_str.starts_with("org.apache.cassandra.db.marshal.settype(") =>
            {
                let element_type = self.extract_collection_element_type(raw_type_str, "set")?;
                let (val, _) = self.parse_frozen_set_value_raw(
                    data,
                    0,
                    &element_type,
                    column_name,
                    depth + 1,
                )?;
                Ok(val)
            }
            type_str
                if type_str.starts_with("map<")
                    || type_str.starts_with("org.apache.cassandra.db.marshal.maptype(") =>
            {
                let (key_type, value_type) = self.extract_map_types(raw_type_str)?;
                let (val, _) = self.parse_frozen_map_value_raw(
                    data,
                    0,
                    &key_type,
                    &value_type,
                    column_name,
                    depth + 1,
                )?;
                Ok(val)
            }
            // Nested tuple inside a frozen collection element.
            // The caller (read_frozen_element) has already extracted the raw element bytes
            // into `data`, so there is no outer VUInt length here — just the sequence of
            // [i32 BE len][bytes] fields as written by serialize_value for Value::Tuple.
            // Issue #1081: also accept the marshal form `TupleType(...)`, extracting
            // element types from the original-case `raw_type_str`.
            type_str
                if type_str.starts_with("tuple<")
                    || type_str.starts_with("org.apache.cassandra.db.marshal.tupletype(") =>
            {
                let element_types = self.extract_tuple_element_types(raw_type_str)?;
                if element_types.is_empty() {
                    return Err(Error::schema(format!(
                        "Nested tuple element '{}': empty tuple type",
                        column_name
                    )));
                }
                let mut off = 0usize;
                let blob_end = data.len();
                let elements = self.parse_tuple_elements_raw(
                    data,
                    &mut off,
                    blob_end,
                    &element_types,
                    column_name,
                    depth + 1,
                )?;
                Ok(Value::Tuple(elements))
            }
            // Issue #1081: accept BOTH the CQL short form (`frozen<...>`) and the
            // authoritative Cassandra marshal form
            // (`org.apache.cassandra.db.marshal.FrozenType(...)`). Collection/UDT
            // fields inside a multicell UDT must be frozen, and their field types
            // resolve from the on-disk `UserType(...)` marshal string where a frozen
            // field is spelled `FrozenType(...)` — e.g. `frozen<list<int>>` arrives
            // as `FrozenType(ListType(Int32Type))` and `frozen<some_udt>` as
            // `FrozenType(UserType(...))`. Without this arm those bypass the frozen
            // handling and fall through to the blob default. `extract_frozen_inner_type`
            // accepts both forms; we extract from `raw_type_str` (original case) so the
            // inner marshal type keeps its case and re-routes to the marshal
            // collection/UDT/scalar arms above on recursion.
            type_str
                if type_str.starts_with("frozen<")
                    || type_str.starts_with("org.apache.cassandra.db.marshal.frozentype(") =>
            {
                let inner_type = self.extract_frozen_inner_type(raw_type_str)?;
                let inner =
                    self.parse_value_from_raw_bytes(data, &inner_type, column_name, depth + 1)?;
                Ok(Value::Frozen(Box::new(inner)))
            }
            // UDT (User-Defined Type): delegate to parse_raw_type_value which has the full
            // UDT parsing logic including field count validation and nested type resolution.
            // The raw bytes representation is identical between the two function conventions.
            other if Self::is_udt_type(other) => {
                let (val, _offset) =
                    self.parse_raw_type_value(data, 0, type_str, column_name, depth)?;
                Ok(val)
            }
            other => {
                // Check if it's a short UDT name in the registry (e.g., "address_type").
                // This handles the case where parse_value_from_raw_bytes is called recursively
                // from the frozen<> arm with the stripped inner type (e.g., frozen<address_type>
                // → "address_type"). Since parse_raw_type_value already has a registry-lookup
                // fallback that correctly handles bare UDT names, we delegate there.
                // The byte-level encoding is identical: UDT fields use 4-byte i32 length prefixes
                // with no overall cell-level length prefix, so parse_raw_type_value offset=0 is
                // correct for already-extracted cell value bytes.
                // See Issue #481 regression fix.
                if let Some(ref registry) = self.udt_registry {
                    if registry.get_udt_qualified(&self.keyspace, other).is_some() {
                        tracing::debug!(
                            "parse_value_from_raw_bytes: type '{}' for '{}' resolved as UDT via registry, delegating to parse_raw_type_value",
                            other,
                            column_name,
                        );
                        let (val, _offset) =
                            self.parse_raw_type_value(data, 0, type_str, column_name, depth)?;
                        return Ok(val);
                    }
                }
                // Truly unknown type: fall back to blob.
                tracing::debug!(
                    "parse_value_from_raw_bytes: unknown type '{}' for '{}', treating as blob ({} bytes)",
                    other,
                    column_name,
                    data.len()
                );
                Ok(Value::Blob(
                    crate::storage::sstable::reader::value_borrow::borrow_active(data),
                ))
            }
        }
    }
}
