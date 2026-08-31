use super::*;

impl V5CompressedLegacyParser {
    /// Map a PRIMITIVE Cassandra marshal type (e.g.
    /// `org.apache.cassandra.db.marshal.Int32Type`) to the canonical CQL short
    /// form (`"int"`) understood by [`parse_value_from_raw_bytes`]'s match
    /// (issue #1081). Returns `None` for any non-primitive marshal form
    /// (UserType / collection / tuple / reversed / frozen / custom), so the
    /// caller leaves those to the dedicated arms. The suffix set is a *superset*
    /// of the authoritative marshal→`CqlType` mapping in
    /// [`parse_cassandra_type_with_depth`] (no heuristics — issue #28): in
    /// addition to the scalars that mapping enumerates, this also normalizes a
    /// few marshal forms that `parse_cassandra_type_with_depth` routes to
    /// `Custom` (`VarcharType`, `CounterColumnType`, `LexicalUUIDType`,
    /// `ShortType`, `ByteType`). Those extra mappings are required so we can
    /// decode the corresponding scalar UDT field values — e.g. `ShortType`/
    /// `ByteType` are needed to read `smallint`/`tinyint` UDT fields, which
    /// otherwise fall through to the blob default.
    pub(super) fn primitive_marshal_to_cql_short(marshal_type: &str) -> Option<&'static str> {
        // Composite marshal forms carry a `(` after the type name; primitives do
        // not. Reject anything parameterised so we never misread a collection /
        // UDT as a scalar.
        if marshal_type.contains('(') {
            return None;
        }
        let s = marshal_type;
        let short = if s.ends_with("UTF8Type") || s.ends_with("VarcharType") {
            "text"
        } else if s.ends_with("AsciiType") {
            "ascii"
        } else if s.ends_with("Int32Type") {
            "int"
        } else if s.ends_with("LongType") || s.ends_with("CounterColumnType") {
            "bigint"
        } else if s.ends_with("FloatType") {
            "float"
        } else if s.ends_with("DoubleType") {
            "double"
        } else if s.ends_with("BooleanType") {
            "boolean"
        } else if s.ends_with("TimeUUIDType") {
            "timeuuid"
        } else if s.ends_with("UUIDType") || s.ends_with("LexicalUUIDType") {
            "uuid"
        } else if s.ends_with("SimpleDateType") {
            // CQL `date` (`SimpleDateType`) is a 4-byte unsigned days-since-epoch
            // value. This is distinct from the legacy `DateType` handled below.
            "date"
        } else if s.ends_with("DateType") {
            // Legacy Cassandra `DateType` is an 8-byte millis-since-epoch value —
            // the same wire format as `TimestampType`. Mapping it to `date` would
            // wrongly decode only the first 4 bytes, so route it to `timestamp`.
            // NOTE: this `ends_with` arm must follow the `SimpleDateType` arm above
            // because `SimpleDateType` also ends with `DateType`.
            "timestamp"
        } else if s.ends_with("TimestampType") {
            "timestamp"
        } else if s.ends_with("TimeType") {
            "time"
        } else if s.ends_with("DecimalType") {
            "decimal"
        } else if s.ends_with("IntegerType") {
            "varint"
        } else if s.ends_with("DurationType") {
            "duration"
        } else if s.ends_with("ShortType") {
            "smallint"
        } else if s.ends_with("ByteType") {
            "tinyint"
        } else if s.ends_with("InetAddressType") {
            "inet"
        } else if s.ends_with("BytesType") {
            "blob"
        } else {
            return None;
        };
        Some(short)
    }

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
            "int" => {
                if data.len() < 4 {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 4 bytes for int, got {}",
                        column_name,
                        data.len()
                    )));
                }
                Ok(Value::Integer(i32::from_be_bytes([
                    data[0], data[1], data[2], data[3],
                ])))
            }
            "bigint" | "counter" => {
                if data.len() < 8 {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 8 bytes for bigint, got {}",
                        column_name,
                        data.len()
                    )));
                }
                Ok(Value::BigInt(i64::from_be_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ])))
            }
            "boolean" => {
                if data.is_empty() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 1 byte for boolean",
                        column_name
                    )));
                }
                Ok(Value::Boolean(data[0] != 0))
            }
            "uuid" | "timeuuid" => {
                if data.len() < 16 {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 16 bytes for UUID, got {}",
                        column_name,
                        data.len()
                    )));
                }
                let uuid: [u8; 16] = data[..16]
                    .try_into()
                    .map_err(|_| Error::corruption("UUID byte conversion failed"))?;
                Ok(Value::Uuid(uuid))
            }
            "float" => {
                if data.len() < 4 {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 4 bytes for float, got {}",
                        column_name,
                        data.len()
                    )));
                }
                // CQL `float` is `Value::Float32`, not the f64 `Value::Float`; the column
                // path and both UDT field decoders already agree (roborev round 10 F1).
                let f = f32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                Ok(Value::Float32(f))
            }
            "double" => {
                if data.len() < 8 {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 8 bytes for double, got {}",
                        column_name,
                        data.len()
                    )));
                }
                Ok(Value::Float(f64::from_be_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ])))
            }
            "smallint" | "short" => {
                if data.len() < 2 {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 2 bytes for smallint, got {}",
                        column_name,
                        data.len()
                    )));
                }
                Ok(Value::SmallInt(i16::from_be_bytes([data[0], data[1]])))
            }
            "tinyint" | "byte" => {
                if data.is_empty() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 1 byte for tinyint",
                        column_name
                    )));
                }
                Ok(Value::TinyInt(data[0] as i8))
            }
            "timestamp" => {
                if data.len() < 8 {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 8 bytes for timestamp, got {}",
                        column_name,
                        data.len()
                    )));
                }
                Ok(Value::Timestamp(i64::from_be_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ])))
            }
            "date" => {
                if data.len() < 4 {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 4 bytes for date, got {}",
                        column_name,
                        data.len()
                    )));
                }
                let stored = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                let days_since_epoch = stored.wrapping_add(i32::MIN as u32) as i32;
                Ok(Value::Date(days_since_epoch))
            }
            "time" => {
                if data.len() < 8 {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 8 bytes for time, got {}",
                        column_name,
                        data.len()
                    )));
                }
                Ok(Value::Time(i64::from_be_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ])))
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

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::super::test_support::helpers::*;
    #[allow(unused_imports)]
    use super::*;

    /// Issue #1081: `primitive_marshal_to_cql_short` must normalize every
    /// PRIMITIVE Cassandra marshal type (fully-qualified) to its canonical CQL
    /// short form, and must reject any parameterised/composite marshal form
    /// (anything containing `(` — UDT / collection / reversed) so the
    /// no-heuristics `(`-rejection guard never misreads a composite as a scalar.
    #[test]
    fn primitive_marshal_to_cql_short_maps_scalars_and_rejects_composites() {
        const P: &str = "org.apache.cassandra.db.marshal.";

        // (marshal type name, expected canonical CQL short form)
        let cases: &[(&str, &str)] = &[
            ("UTF8Type", "text"),
            ("AsciiType", "ascii"),
            ("Int32Type", "int"),
            ("LongType", "bigint"),
            ("FloatType", "float"),
            ("DoubleType", "double"),
            ("BooleanType", "boolean"),
            ("UUIDType", "uuid"),
            ("TimeUUIDType", "timeuuid"),
            ("TimestampType", "timestamp"),
            ("SimpleDateType", "date"),
            // Legacy `DateType` is an 8-byte millis-since-epoch value (same wire
            // format as `TimestampType`), NOT the 4-byte CQL `date`
            // (`SimpleDateType`). It must normalize to `timestamp`.
            ("DateType", "timestamp"),
            ("TimeType", "time"),
            ("DecimalType", "decimal"),
            ("IntegerType", "varint"),
            ("DurationType", "duration"),
            ("ShortType", "smallint"),
            ("ByteType", "tinyint"),
            ("InetAddressType", "inet"),
            ("BytesType", "blob"),
        ];

        for (marshal, expected) in cases {
            let full = format!("{}{}", P, marshal);
            assert_eq!(
                V5CompressedLegacyParser::primitive_marshal_to_cql_short(&full),
                Some(*expected),
                "primitive marshal {} should map to {}",
                full,
                expected
            );
        }

        // Parameterised / composite marshal forms must be rejected (return None)
        // by the `(`-guard, leaving them to the dedicated composite arms.
        let composites = [
            "org.apache.cassandra.db.marshal.UserType(...)",
            "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.UTF8Type)",
            "org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.Int32Type)",
        ];
        for composite in composites {
            assert_eq!(
                V5CompressedLegacyParser::primitive_marshal_to_cql_short(composite),
                None,
                "composite marshal {} must be rejected by the `(`-guard",
                composite
            );
        }
    }

    #[test]
    fn test_parse_value_from_raw_bytes_primitives() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        // int
        let data = 42i32.to_be_bytes();
        let val = parser
            .parse_value_from_raw_bytes(&data, "int", "col", 0)
            .unwrap();
        assert_eq!(val, Value::Integer(42));

        // bigint
        let data = 123456789i64.to_be_bytes();
        let val = parser
            .parse_value_from_raw_bytes(&data, "bigint", "col", 0)
            .unwrap();
        assert_eq!(val, Value::BigInt(123456789));

        // text
        let data = b"hello";
        let val = parser
            .parse_value_from_raw_bytes(data, "text", "col", 0)
            .unwrap();
        assert_eq!(val, Value::text("hello".to_string()));

        // boolean true
        let val = parser
            .parse_value_from_raw_bytes(&[1], "boolean", "col", 0)
            .unwrap();
        assert_eq!(val, Value::Boolean(true));

        // boolean false
        let val = parser
            .parse_value_from_raw_bytes(&[0], "boolean", "col", 0)
            .unwrap();
        assert_eq!(val, Value::Boolean(false));

        // float -> `Value::Float32`; used to pin the f32->f64 widening (round 10).
        let data = 1.5f32.to_be_bytes();
        let val = parser
            .parse_value_from_raw_bytes(&data, "float", "col", 0)
            .unwrap();
        assert_eq!(val, Value::Float32(1.5), "float is Float32");

        // double
        let data = 9.876f64.to_be_bytes();
        let val = parser
            .parse_value_from_raw_bytes(&data, "double", "col", 0)
            .unwrap();
        assert_eq!(val, Value::Float(9.876));

        // uuid (16 bytes)
        let uuid_bytes: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let val = parser
            .parse_value_from_raw_bytes(&uuid_bytes, "uuid", "col", 0)
            .unwrap();
        assert_eq!(val, Value::Uuid(uuid_bytes));

        // smallint
        let data = 1234i16.to_be_bytes();
        let val = parser
            .parse_value_from_raw_bytes(&data, "smallint", "col", 0)
            .unwrap();
        assert_eq!(val, Value::SmallInt(1234));

        // tinyint
        let val = parser
            .parse_value_from_raw_bytes(&[42], "tinyint", "col", 0)
            .unwrap();
        assert_eq!(val, Value::TinyInt(42));

        // blob
        let data = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let val = parser
            .parse_value_from_raw_bytes(&data, "blob", "col", 0)
            .unwrap();
        assert_eq!(val, Value::Blob(data.into()));

        // varint
        let data = vec![0x01, 0x00];
        let val = parser
            .parse_value_from_raw_bytes(&data, "varint", "col", 0)
            .unwrap();
        assert_eq!(val, Value::varint(vec![0x01, 0x00]));

        // inet (IPv4)
        let data = vec![127, 0, 0, 1];
        let val = parser
            .parse_value_from_raw_bytes(&data, "inet", "col", 0)
            .unwrap();
        assert_eq!(val, Value::inet(vec![127, 0, 0, 1]));

        // timestamp
        let data = 1704067200000i64.to_be_bytes();
        let val = parser
            .parse_value_from_raw_bytes(&data, "timestamp", "col", 0)
            .unwrap();
        assert_eq!(val, Value::Timestamp(1704067200000));

        // decimal
        let mut data = Vec::new();
        data.extend_from_slice(&2i32.to_be_bytes()); // scale
        data.extend_from_slice(&[0x01, 0xC8]); // unscaled = 456
        let val = parser
            .parse_value_from_raw_bytes(&data, "decimal", "col", 0)
            .unwrap();
        assert_eq!(
            val,
            Value::Decimal {
                scale: 2,
                unscaled: vec![0x01, 0xC8]
            }
        );
    }

    /// Issue #1081: a multicell-UDT field declared `duration` resolves its
    /// field type from the authoritative on-disk `UserType(...)` marshal string
    /// as `org.apache.cassandra.db.marshal.DurationType`. That bare scalar
    /// marshal form must normalize to `"duration"` and decode the three
    /// consecutive SIGNED VInts (months, days, nanos) that constitute the value
    /// — NOT fall through to `Value::Blob`. In `parse_value_from_raw_bytes` the
    /// entire slice IS the value (no outer `[VInt len]` prefix).
    #[test]
    fn test_parse_value_from_raw_bytes_duration_marshal() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        // Build three SIGNED VInts (months=1, days=2, nanos=3) using the same
        // ZigZag-over-unsigned-VInt scheme `parse_vint` decodes. ZigZag:
        // 1 -> 2, 2 -> 4, 3 -> 6.
        let zigzag = |v: i64| ((v << 1) ^ (v >> 63)) as u64;
        let mut data = Vec::new();
        encode_unsigned(zigzag(1), &mut data); // months
        encode_unsigned(zigzag(2), &mut data); // days
        encode_unsigned(zigzag(3), &mut data); // nanos

        // Round-trip via the signed VInt parser to confirm the encoding before
        // exercising the decode arm under test.
        let (rem, m) = parse_vint(&data).unwrap();
        let consumed = data.len() - rem.len();
        let (rem2, d) = parse_vint(&data[consumed..]).unwrap();
        let consumed2 = data.len() - rem2.len();
        let (_rem3, n) = parse_vint(&data[consumed2..]).unwrap();
        assert_eq!(
            (m, d, n),
            (1, 2, 3),
            "hand-encoded duration vints round-trip"
        );

        // The bare scalar marshal form must normalize and decode to Duration,
        // NOT fall through to Blob.
        let val = parser
            .parse_value_from_raw_bytes(
                &data,
                "org.apache.cassandra.db.marshal.DurationType",
                "col",
                0,
            )
            .unwrap();
        assert_eq!(
            val,
            Value::Duration {
                months: 1,
                days: 2,
                nanos: 3
            }
        );

        // The canonical CQL short form must decode identically.
        let val_short = parser
            .parse_value_from_raw_bytes(&data, "duration", "col", 0)
            .unwrap();
        assert_eq!(val_short, val);
    }

    /// Issue #1632 (item b): the raw-value frozen `duration` arm must REJECT a
    /// months/days VInt outside the i32 range instead of silently wrapping via
    /// `as i32`. On pre-fix code `months as i32` wraps `i32::MAX + 1` to a
    /// negative value and returns Ok; the guard turns it into an error.
    #[test]
    fn test_parse_value_from_raw_bytes_duration_months_out_of_i32_range_errors() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);
        let zigzag = |v: i64| ((v << 1) ^ (v >> 63)) as u64;

        // months = i32::MAX + 1 (overflows i32), days = 0, nanos = 0.
        let mut over = Vec::new();
        encode_unsigned(zigzag(i32::MAX as i64 + 1), &mut over);
        encode_unsigned(zigzag(0), &mut over);
        encode_unsigned(zigzag(0), &mut over);
        assert!(
            parser
                .parse_value_from_raw_bytes(&over, "duration", "col", 0)
                .is_err(),
            "months > i32::MAX must error, not wrap via `as i32`"
        );

        // days = i32::MIN - 1 (underflows i32), months = 0, nanos = 0.
        let mut under = Vec::new();
        encode_unsigned(zigzag(0), &mut under);
        encode_unsigned(zigzag(i32::MIN as i64 - 1), &mut under);
        encode_unsigned(zigzag(0), &mut under);
        assert!(
            parser
                .parse_value_from_raw_bytes(&under, "duration", "col", 0)
                .is_err(),
            "days < i32::MIN must error, not wrap via `as i32`"
        );
    }

    #[test]
    fn test_parse_value_from_raw_bytes_nested_list() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        let data = build_frozen_list_int(&[10, 20, 30]);
        let val = parser
            .parse_value_from_raw_bytes(&data, "list<int>", "col", 0)
            .unwrap();
        assert_eq!(
            val,
            Value::List(vec![
                Value::Integer(10),
                Value::Integer(20),
                Value::Integer(30)
            ])
        );
    }

    #[test]
    fn test_parse_value_from_raw_bytes_nested_set() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        let data = build_frozen_list_int(&[5, 15]);
        let val = parser
            .parse_value_from_raw_bytes(&data, "set<int>", "col", 0)
            .unwrap();
        assert_eq!(val, Value::Set(vec![Value::Integer(5), Value::Integer(15)]));
    }

    #[test]
    fn test_parse_value_from_raw_bytes_nested_map() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        let data = build_frozen_map_text_int(&[("alice", 1), ("bob", 2)]);
        let val = parser
            .parse_value_from_raw_bytes(&data, "map<text,int>", "col", 0)
            .unwrap();
        assert_eq!(
            val,
            Value::Map(vec![
                (Value::text("alice".to_string()), Value::Integer(1)),
                (Value::text("bob".to_string()), Value::Integer(2)),
            ])
        );
    }

    #[test]
    fn test_parse_value_from_raw_bytes_frozen_wrapper() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        let data = build_frozen_list_int(&[100, 200]);
        let val = parser
            .parse_value_from_raw_bytes(&data, "frozen<list<int>>", "col", 0)
            .unwrap();
        assert_eq!(
            val,
            Value::Frozen(Box::new(Value::List(vec![
                Value::Integer(100),
                Value::Integer(200)
            ])))
        );
    }

    /// Issue #1081 (FINDING 1): a multicell-UDT field whose declared type arrives
    /// in Cassandra MARSHAL form for a COLLECTION — here
    /// `ListType(UTF8Type)` — must decode to a real `Value::List`, NOT fall
    /// through to the blob default. This also exercises the case-sensitivity
    /// path: the lowercased match binding must NOT corrupt the original-case
    /// nested element marshal type (`UTF8Type`), which would otherwise fail to
    /// re-normalize and blob.
    #[test]
    fn test_parse_value_from_raw_bytes_marshal_list_utf8() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        let data = build_frozen_list_text(&["alpha", "beta"]);
        let val = parser
            .parse_value_from_raw_bytes(
                &data,
                "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.UTF8Type)",
                "udt_field",
                0,
            )
            .expect("marshal ListType(UTF8Type) must decode, not blob");
        assert_eq!(
            val,
            Value::List(vec![
                Value::text("alpha".to_string()),
                Value::text("beta".to_string()),
            ]),
            "marshal-form list field must produce a List of Text (not a Blob)"
        );
    }

    /// Issue #1081 (FINDING 1): a multicell-UDT field declared as a marshal-form
    /// `MapType(UTF8Type, Int32Type)` must decode to a `Value::Map`. The
    /// Int32Type VALUE proves the nested element marshal type keeps its original
    /// case through the lowercased match arm (a lowercased `...int32type` would
    /// fail the case-sensitive primitive normalizer and blob).
    #[test]
    fn test_parse_value_from_raw_bytes_marshal_map_utf8_int32() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        // Same [count][i32 key_len][key][i32 val_len][i32 value] framing the
        // frozen map raw parser expects; the int value is a 4-byte i32.
        let data = build_frozen_map_text_int(&[("alice", 1), ("bob", 2)]);
        let val = parser
            .parse_value_from_raw_bytes(
                &data,
                "org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.Int32Type)",
                "udt_field",
                0,
            )
            .expect("marshal MapType(UTF8Type, Int32Type) must decode, not blob");
        assert_eq!(
            val,
            Value::Map(vec![
                (Value::text("alice".to_string()), Value::Integer(1)),
                (Value::text("bob".to_string()), Value::Integer(2)),
            ]),
            "marshal-form map field must produce a Map of (Text, Integer) (not a Blob)"
        );
    }

    /// Issue #1081 (FINDING 1): a marshal-form `SetType(Int32Type)` field must
    /// decode to a `Value::Set` rather than a blob.
    #[test]
    fn test_parse_value_from_raw_bytes_marshal_set_int32() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        let data = build_frozen_list_int(&[7, 9]);
        let val = parser
            .parse_value_from_raw_bytes(
                &data,
                "org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.Int32Type)",
                "udt_field",
                0,
            )
            .expect("marshal SetType(Int32Type) must decode, not blob");
        assert_eq!(
            val,
            Value::Set(vec![Value::Integer(7), Value::Integer(9)]),
            "marshal-form set field must produce a Set of Integer (not a Blob)"
        );
    }

    /// Issue #1081 (FINDING — last type-tree gap): a multicell-UDT field declared
    /// `frozen<list<text>>` resolves from the on-disk `UserType(...)` marshal string
    /// as `FrozenType(ListType(UTF8Type))`. Before the fix the frozen arm in
    /// `parse_value_from_raw_bytes` only matched the CQL `frozen<...>` form, so this
    /// marshal form bypassed it and fell through to `Value::Blob`. It must now decode
    /// to `Value::Frozen(List([Text, ...]))`. (Collection/UDT fields inside a UDT must
    /// be frozen, so this marshal form is the common real case.)
    #[test]
    fn test_parse_value_from_raw_bytes_marshal_frozen_list_utf8() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        let data = build_frozen_list_text(&["gamma", "delta"]);
        let val = parser
            .parse_value_from_raw_bytes(
                &data,
                "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.UTF8Type))",
                "udt_field",
                0,
            )
            .expect("marshal FrozenType(ListType(UTF8Type)) must decode, not blob");
        assert_eq!(
            val,
            Value::Frozen(Box::new(Value::List(vec![
                Value::text("gamma".to_string()),
                Value::text("delta".to_string()),
            ]))),
            "marshal-form frozen-list field must produce Frozen(List(Text)) (not a Blob)"
        );
    }

    #[test]
    fn test_parse_raw_type_value_depth_guard() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        // Directly calling with depth at limit should fail
        let data = 42i32.to_be_bytes();
        let result =
            parser.parse_raw_type_value(&data, 0, "int", "col", MAX_TYPE_NESTING_DEPTH + 1);
        assert!(result.is_err());
    }
}
