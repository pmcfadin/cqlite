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
    fn primitive_marshal_to_cql_short(marshal_type: &str) -> Option<&'static str> {
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
                let text = String::from_utf8(data.to_vec()).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen element '{}': invalid UTF-8 in text value: {}",
                        column_name, e
                    ))
                })?;
                Ok(Value::Text(text))
            }
            "blob" | "bytes" => Ok(Value::Blob(data.to_vec())),
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
                let f = f32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                Ok(Value::Float(f as f64))
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

                Ok(Value::Duration {
                    months: months as i32,
                    days: days as i32,
                    nanos,
                })
            }
            "varint" => Ok(Value::Varint(data.to_vec())),
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
            "inet" => Ok(Value::Inet(data.to_vec())),
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
                    if registry.get_udt(&self.keyspace, other).is_some() {
                        log::debug!(
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
                log::debug!(
                    "parse_value_from_raw_bytes: unknown type '{}' for '{}', treating as blob ({} bytes)",
                    other,
                    column_name,
                    data.len()
                );
                Ok(Value::Blob(data.to_vec()))
            }
        }
    }

    /// Parse a raw type value WITHOUT cell flags (for frozen collection elements)
    ///
    /// Unlike `parse_cell_value_schema_order`, this function does NOT expect cell flags
    /// or timestamps at the start of the data. Frozen collection elements are stored
    /// as raw type values directly:
    /// - Fixed-width types (int, uuid, bigint, float, double): direct bytes, no length prefix
    /// - Variable-width types (text, blob): VInt length prefix + bytes
    ///
    /// This is the correct format for elements inside frozen collections:
    /// frozen<list<int>> -> [VInt count][int1][int2]...  (each int is 4 bytes, no flags)
    /// frozen<map<text, text>> -> [VInt count][VInt key_len][key][VInt val_len][val]...
    pub(super) fn parse_raw_type_value(
        &self,
        data: &[u8],
        mut offset: usize,
        type_str: &str,
        column_name: &str,
        depth: usize,
    ) -> Result<(Value, usize)> {
        if depth > MAX_TYPE_NESTING_DEPTH {
            return Err(Error::corruption(format!(
                "Frozen element '{}': recursion depth {} exceeds maximum {}",
                column_name, depth, MAX_TYPE_NESTING_DEPTH
            )));
        }
        // Normalize type name for case-insensitive matching
        let normalized_type = type_str.to_lowercase();

        let value = match normalized_type.as_str() {
            // Cassandra internal type names (full package paths)
            "org.apache.cassandra.db.marshal.utf8type"
            | "org.apache.cassandra.db.marshal.asciitype"
            | "org.apache.cassandra.db.marshal.varchartype" => {
                // Text: [VInt len][text bytes]
                let (remaining, text_len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen element '{}': failed to parse text length as VInt: {:?}",
                        column_name, e
                    ))
                })?;
                let text_len = text_len as usize;
                let bytes_consumed = data[offset..].len() - remaining.len();
                offset += bytes_consumed;

                if offset + text_len > data.len() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need {} bytes for text, only {} available",
                        column_name,
                        text_len,
                        data.len() - offset
                    )));
                }

                let text_bytes = &data[offset..offset + text_len];
                let text = String::from_utf8(text_bytes.to_vec())
                    .map_err(|e| Error::corruption(format!("Invalid UTF-8 in text: {}", e)))?;
                offset += text_len;
                Value::Text(text)
            }

            "boolean" => {
                // Boolean: 1 byte
                if offset >= data.len() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': unexpected end at boolean value",
                        column_name
                    )));
                }
                let bool_byte = data[offset];
                offset += 1;
                Value::Boolean(bool_byte != 0)
            }

            "int" => {
                // Integer (i32): fixed-width 4 bytes
                if offset + 4 > data.len() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 4 bytes for int, only {} available",
                        column_name,
                        data.len() - offset
                    )));
                }
                let int_val = i32::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]);
                offset += 4;
                Value::Integer(int_val)
            }

            "text" | "varchar" | "ascii" => {
                // Text: [VInt len][text bytes]
                let (remaining, text_len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen element '{}': failed to parse text length as VInt: {:?}",
                        column_name, e
                    ))
                })?;
                let text_len = text_len as usize;
                let bytes_consumed = data[offset..].len() - remaining.len();
                offset += bytes_consumed;

                if offset + text_len > data.len() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need {} bytes for text, only {} available",
                        column_name,
                        text_len,
                        data.len() - offset
                    )));
                }

                let text_bytes = &data[offset..offset + text_len];
                let text = String::from_utf8(text_bytes.to_vec()).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen element '{}': invalid UTF-8 in text value: {}",
                        column_name, e
                    ))
                })?;

                offset += text_len;
                Value::Text(text)
            }

            "uuid" | "timeuuid" => {
                // UUID/TimeUUID: fixed-width 16 bytes
                if offset + 16 > data.len() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 16 bytes for UUID, only {} available",
                        column_name,
                        data.len() - offset
                    )));
                }

                let uuid_bytes: [u8; 16] = data[offset..offset + 16]
                    .try_into()
                    .map_err(|_| Error::corruption("UUID byte conversion failed"))?;

                offset += 16;
                Value::Uuid(uuid_bytes)
            }

            "bigint" | "counter" => {
                // BigInt/Counter: fixed-width 8 bytes
                if offset + 8 > data.len() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 8 bytes for bigint, only {} available",
                        column_name,
                        data.len() - offset
                    )));
                }
                let val = i64::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                    data[offset + 4],
                    data[offset + 5],
                    data[offset + 6],
                    data[offset + 7],
                ]);
                offset += 8;
                Value::BigInt(val)
            }

            "float" => {
                // Float: 4 bytes
                if offset + 4 > data.len() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 4 bytes for float, only {} available",
                        column_name,
                        data.len() - offset
                    )));
                }

                let val = f32::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]);
                offset += 4;
                Value::Float(val as f64)
            }

            "double" => {
                // Double: 8 bytes
                if offset + 8 > data.len() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 8 bytes for double, only {} available",
                        column_name,
                        data.len() - offset
                    )));
                }
                let val = f64::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                    data[offset + 4],
                    data[offset + 5],
                    data[offset + 6],
                    data[offset + 7],
                ]);
                offset += 8;
                Value::Float(val) // Note: Value::Float holds f64 for both float and double
            }

            "timestamp" => {
                // Timestamp: 8 bytes (milliseconds since epoch)
                if offset + 8 > data.len() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 8 bytes for timestamp, only {} available",
                        column_name,
                        data.len() - offset
                    )));
                }
                let ts = i64::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                    data[offset + 4],
                    data[offset + 5],
                    data[offset + 6],
                    data[offset + 7],
                ]);
                offset += 8;
                Value::Timestamp(ts)
            }

            "date" => {
                // Date: [VInt len=4][u32 BE days since epoch]
                let (remaining, date_len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen element '{}': failed to parse date length as VInt: {:?}",
                        column_name, e
                    ))
                })?;
                let date_len = date_len as usize;
                let bytes_consumed = data[offset..].len() - remaining.len();
                offset += bytes_consumed;

                if date_len != 4 {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': expected date length 4, got {}",
                        column_name, date_len
                    )));
                }

                if offset + 4 > data.len() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 4 bytes for date, only {} available",
                        column_name,
                        data.len() - offset
                    )));
                }

                let stored = u32::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]);
                offset += 4;
                // Cassandra DATE: 4-byte unsigned int with Integer.MIN_VALUE offset
                let days_since_epoch = stored.wrapping_add(i32::MIN as u32) as i32;
                Value::Date(days_since_epoch)
            }

            "time" => {
                // Time: [VInt len=8][i64 BE nanoseconds since midnight]
                let (remaining, time_len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen element '{}': failed to parse time length as VInt: {:?}",
                        column_name, e
                    ))
                })?;
                let time_len = time_len as usize;
                let bytes_consumed = data[offset..].len() - remaining.len();
                offset += bytes_consumed;

                if time_len != 8 {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': expected time length 8, got {}",
                        column_name, time_len
                    )));
                }

                if offset + 8 > data.len() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 8 bytes for time, only {} available",
                        column_name,
                        data.len() - offset
                    )));
                }

                let nanos = i64::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                    data[offset + 4],
                    data[offset + 5],
                    data[offset + 6],
                    data[offset + 7],
                ]);
                offset += 8;
                Value::Time(nanos)
            }

            "duration" => {
                // Duration: [VInt len][months VInt][days VInt][nanos VInt]
                let (remaining, duration_len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen element '{}': failed to parse duration length as VInt: {:?}",
                        column_name, e
                    ))
                })?;
                let duration_len = duration_len as usize;
                let bytes_consumed = data[offset..].len() - remaining.len();
                offset += bytes_consumed;

                if offset + duration_len > data.len() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need {} bytes for duration, only {} available",
                        column_name,
                        duration_len,
                        data.len() - offset
                    )));
                }

                // Parse three VInt components from the duration_len bytes
                let duration_bytes = &data[offset..offset + duration_len];

                // Parse months (signed VInt)
                let (remaining, months) = parse_vint(duration_bytes).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen element '{}': failed to parse duration months: {:?}",
                        column_name, e
                    ))
                })?;
                let pos = duration_bytes.len() - remaining.len();

                // Parse days (signed VInt)
                let (remaining, days) = parse_vint(&duration_bytes[pos..]).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen element '{}': failed to parse duration days: {:?}",
                        column_name, e
                    ))
                })?;
                let pos = duration_bytes.len() - remaining.len();

                // Parse nanoseconds (signed VInt)
                let (_remaining, nanos) = parse_vint(&duration_bytes[pos..]).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen element '{}': failed to parse duration nanos: {:?}",
                        column_name, e
                    ))
                })?;

                offset += duration_len;
                Value::Duration {
                    months: months as i32,
                    days: days as i32,
                    nanos,
                }
            }

            "inet" => {
                // Inet: [VInt len][address bytes] (len is 4 for IPv4, 16 for IPv6)
                let (remaining, len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen element '{}': failed to parse inet length as VInt: {:?}",
                        column_name, e
                    ))
                })?;
                let len = len as usize;
                let bytes_consumed = data[offset..].len() - remaining.len();
                offset += bytes_consumed;

                if len != 4 && len != 16 {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': invalid inet length {}, expected 4 or 16",
                        column_name, len
                    )));
                }

                if offset + len > data.len() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need {} bytes for inet, only {} available",
                        column_name,
                        len,
                        data.len() - offset
                    )));
                }

                let bytes = data[offset..offset + len].to_vec();
                offset += len;
                Value::Inet(bytes)
            }

            "blob" | "bytes" => {
                // Blob: [VInt len][bytes]
                let (remaining, blob_len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen element '{}': failed to parse blob length as VInt: {:?}",
                        column_name, e
                    ))
                })?;
                let blob_len = blob_len as usize;
                let bytes_consumed = data[offset..].len() - remaining.len();
                offset += bytes_consumed;

                if offset + blob_len > data.len() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need {} bytes for blob, only {} available",
                        column_name,
                        blob_len,
                        data.len() - offset
                    )));
                }

                let blob_bytes = data[offset..offset + blob_len].to_vec();
                offset += blob_len;
                Value::Blob(blob_bytes)
            }

            "smallint" | "short" => {
                // SmallInt: 2 bytes
                if offset + 2 > data.len() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 2 bytes for smallint, only {} available",
                        column_name,
                        data.len() - offset
                    )));
                }
                let val = i16::from_be_bytes([data[offset], data[offset + 1]]);
                offset += 2;
                Value::SmallInt(val)
            }

            "tinyint" | "byte" => {
                // TinyInt: 1 byte
                if offset >= data.len() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need 1 byte for tinyint, only {} available",
                        column_name,
                        data.len() - offset
                    )));
                }
                let val = data[offset] as i8;
                offset += 1;
                Value::TinyInt(val)
            }

            "varint" => {
                // VarInt: [VInt len][bytes]
                let (remaining, varint_len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen element '{}': failed to parse varint length as VInt: {:?}",
                        column_name, e
                    ))
                })?;
                let varint_len = varint_len as usize;
                let bytes_consumed = data[offset..].len() - remaining.len();
                offset += bytes_consumed;

                if offset + varint_len > data.len() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need {} bytes for varint, only {} available",
                        column_name,
                        varint_len,
                        data.len() - offset
                    )));
                }

                let varint_bytes = data[offset..offset + varint_len].to_vec();
                offset += varint_len;
                Value::Varint(varint_bytes)
            }

            "decimal" => {
                // Decimal: [VInt total_len][i32 scale][unscaled bytes]
                let (remaining, total_len) = parse_vuint(&data[offset..]).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen element '{}': failed to parse decimal length as VInt: {:?}",
                        column_name, e
                    ))
                })?;
                let total_len = total_len as usize;
                let bytes_consumed = data[offset..].len() - remaining.len();
                offset += bytes_consumed;

                if offset + total_len > data.len() {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': need {} bytes for decimal, only {} available",
                        column_name,
                        total_len,
                        data.len() - offset
                    )));
                }

                if total_len < 4 {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': decimal length {} too small for scale",
                        column_name, total_len
                    )));
                }

                let scale = i32::from_be_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]);
                let unscaled = data[offset + 4..offset + total_len].to_vec();
                offset += total_len;

                Value::Decimal { scale, unscaled }
            }

            // Handle nested tuple types inside a frozen context.
            // In parse_raw_type_value the data slice is the full (unbounded) row buffer, so
            // `offset` marks where the tuple blob starts.  The tuple's per-element length
            // uses the [i32 BE len][bytes] wire format; the count comes from the type string.
            // There is NO outer VUInt blob-length prefix here because parse_raw_type_value is
            // called element-by-element from the frozen-collection parsers which have already
            // consumed the VUInt length for each element (via read_frozen_element).
            //
            // Safety invariant: every caller of parse_raw_type_value for a tuple element
            // pre-slices `data` to the exact element bytes (via read_frozen_element or
            // parse_frozen_sequence_value_raw), so `data.len()` is the true tuple extent.
            // parse_tuple_elements_raw iterates only over schema-derived element_types, so it
            // stops at the schema arity regardless of wire arity, and the returned `offset`
            // is the position after the last schema-specified element's bytes — which is
            // correct because the caller already holds the bounded slice.
            type_str if type_str.starts_with("tuple<") => {
                let element_types = self.extract_tuple_element_types(type_str)?;
                if element_types.is_empty() {
                    return Err(Error::schema(format!(
                        "Frozen element '{}': empty tuple type",
                        column_name
                    )));
                }
                // blob_end = data.len() is correct: callers pre-slice data to the tuple extent.
                let blob_end = data.len();
                let mut off = offset;
                let elements = self.parse_tuple_elements_raw(
                    data,
                    &mut off,
                    blob_end,
                    &element_types,
                    column_name,
                    depth + 1,
                )?;
                offset = off;
                Value::Tuple(elements)
            }

            // Handle nested frozen types
            type_str if type_str.starts_with("frozen<") => {
                let inner_type = self.extract_frozen_inner_type(type_str)?;
                let (inner_value, new_offset) =
                    self.parse_raw_type_value(data, offset, &inner_type, column_name, depth + 1)?;
                offset = new_offset;
                Value::Frozen(Box::new(inner_value))
            }

            // Handle nested collections inside frozen context
            type_str if type_str.starts_with("list<") => {
                let element_type = self.extract_collection_element_type(type_str, "list")?;
                let (list_value, new_offset) = self.parse_frozen_list_value_raw(
                    data,
                    offset,
                    &element_type,
                    column_name,
                    depth + 1,
                )?;
                offset = new_offset;
                list_value
            }

            type_str if type_str.starts_with("set<") => {
                let element_type = self.extract_collection_element_type(type_str, "set")?;
                let (set_value, new_offset) = self.parse_frozen_set_value_raw(
                    data,
                    offset,
                    &element_type,
                    column_name,
                    depth + 1,
                )?;
                offset = new_offset;
                set_value
            }

            type_str if type_str.starts_with("map<") => {
                let (key_type, value_type) = self.extract_map_types(type_str)?;
                let (map_value, new_offset) = self.parse_frozen_map_value_raw(
                    data,
                    offset,
                    &key_type,
                    &value_type,
                    column_name,
                    depth + 1,
                )?;
                offset = new_offset;
                map_value
            }

            // Handle UDT (User-Defined Type) inside frozen collections
            // Note: We match against normalized (lowercased) but need original case for parsing
            normalized if Self::is_udt_type(normalized) => {
                log::debug!(
                    "Frozen element '{}': parsing UDT type '{}'",
                    column_name,
                    type_str
                );

                // Parse UDT definition from the ORIGINAL type string (not lowercased)
                // because UserType parsing expects exact case "UserType"
                let udt_def = Self::parse_udt_type_definition(type_str)?;

                // UDT data: The VInt length prefix has already been consumed by the caller
                // (either complex cell parser or frozen collection element parser).
                // The data slice passed to parse_raw_type_value is already the raw UDT bytes.
                let udt_data = &data[offset..];

                if log::log_enabled!(log::Level::Debug) {
                    let hex: String = udt_data
                        .iter()
                        .take(64)
                        .map(|b| format!("{:02x}", b))
                        .collect::<Vec<_>>()
                        .join(" ");
                    log::debug!(
                        "Frozen UDT '{}': data_len={}, hex dump: {}",
                        column_name,
                        udt_data.len(),
                        hex
                    );
                }

                // TODO(Issue #220): Full UDT parsing requires SSTableReader for nested types.
                // parse_raw_type_value is called in frozen collection contexts where we don't
                // have access to reader. For now, parse simple fields and return blob for
                // complex nested types.
                //
                // Temporary solution: Parse UDT with limited nested type support

                // Validate field count to prevent memory exhaustion
                if udt_def.fields.len() > MAX_UDT_FIELD_COUNT {
                    return Err(Error::schema(format!(
                        "UDT '{}' has {} fields, exceeds maximum {}",
                        udt_def.name,
                        udt_def.fields.len(),
                        MAX_UDT_FIELD_COUNT
                    )));
                }

                let mut current_offset = 0;
                let mut fields = Vec::with_capacity(udt_def.fields.len());

                for field_def in &udt_def.fields {
                    // Check bounds for field length (4 bytes BE i32)
                    if current_offset + 4 > udt_data.len() {
                        // Trailing fields can be omitted (implicit null)
                        log::debug!(
                            "Frozen UDT field '{}' omitted (implicit null)",
                            field_def.name
                        );
                        while fields.len() < udt_def.fields.len() {
                            let remaining_field = &udt_def.fields[fields.len()];
                            fields.push(UdtField {
                                name: remaining_field.name.clone(),
                                value: None,
                            });
                        }
                        break;
                    }

                    // Read field length (4 bytes big-endian i32)
                    let field_len = i32::from_be_bytes([
                        udt_data[current_offset],
                        udt_data[current_offset + 1],
                        udt_data[current_offset + 2],
                        udt_data[current_offset + 3],
                    ]);
                    log::debug!(
                        "Frozen UDT field '{}' at offset {}: length bytes={:02x} {:02x} {:02x} {:02x}, parsed length={}",
                        field_def.name,
                        current_offset,
                        udt_data[current_offset],
                        udt_data[current_offset + 1],
                        udt_data[current_offset + 2],
                        udt_data[current_offset + 3],
                        field_len
                    );
                    current_offset += 4;

                    let field_value = if field_len == -1 {
                        // Null field
                        log::debug!("Frozen UDT field '{}' is null", field_def.name);
                        None
                    } else if field_len == 0 {
                        // Empty field
                        log::debug!("Frozen UDT field '{}' is empty", field_def.name);
                        Some(Self::create_empty_value_for_type(&field_def.field_type))
                    } else if field_len < 0 {
                        // Validation: reject other negative values
                        return Err(Error::corruption(format!(
                            "Frozen UDT field '{}': invalid negative field length {}",
                            field_def.name, field_len
                        )));
                    } else {
                        // Field with data
                        let field_len = field_len as usize;
                        if current_offset + field_len > udt_data.len() {
                            return Err(Error::corruption(format!(
                                "Frozen UDT field '{}': need {} bytes but only {} available",
                                field_def.name,
                                field_len,
                                udt_data.len() - current_offset
                            )));
                        }

                        let field_data = &udt_data[current_offset..current_offset + field_len];
                        current_offset += field_len;

                        log::debug!(
                            "Frozen UDT field '{}' has {} bytes of data, type: {:?}",
                            field_def.name,
                            field_len,
                            field_def.field_type
                        );

                        // Parse field value - handle nested UDTs specially (Issue #238)
                        let value = if let Some(ref registry) = self.udt_registry {
                            match &field_def.field_type {
                                CqlType::Custom(nested_type_name) => {
                                    // Issue #239: Handle "udt:" prefix from schema parsing
                                    let lookup_name = nested_type_name
                                        .strip_prefix("udt:")
                                        .unwrap_or(nested_type_name);
                                    if let Some(nested_udt) =
                                        registry.get_udt(&self.keyspace, lookup_name)
                                    {
                                        self.parse_nested_udt_from_registry(
                                            field_data, nested_udt, registry,
                                        )?
                                    } else {
                                        Self::parse_simple_udt_field_value(
                                            field_data,
                                            &field_def.field_type,
                                        )?
                                    }
                                }
                                CqlType::Udt(udt_name, inline_fields) => {
                                    // Prefer registry, fall back to inline fields (Issue #239)
                                    if let Some(nested_udt) =
                                        registry.get_udt(&self.keyspace, udt_name)
                                    {
                                        self.parse_nested_udt_from_registry(
                                            field_data, nested_udt, registry,
                                        )?
                                    } else if !inline_fields.is_empty() {
                                        self.parse_inline_udt_value(
                                            field_data,
                                            udt_name,
                                            inline_fields,
                                            1,
                                        )?
                                    } else {
                                        Self::parse_simple_udt_field_value(
                                            field_data,
                                            &field_def.field_type,
                                        )?
                                    }
                                }
                                CqlType::Frozen(inner) => match inner.as_ref() {
                                    CqlType::Custom(nested_type_name) => {
                                        // Issue #239: Handle "udt:" prefix from schema parsing
                                        let lookup_name = nested_type_name
                                            .strip_prefix("udt:")
                                            .unwrap_or(nested_type_name);
                                        if let Some(nested_udt) =
                                            registry.get_udt(&self.keyspace, lookup_name)
                                        {
                                            let inner_value = self.parse_nested_udt_from_registry(
                                                field_data, nested_udt, registry,
                                            )?;
                                            Value::Frozen(Box::new(inner_value))
                                        } else {
                                            Self::parse_simple_udt_field_value(
                                                field_data,
                                                &field_def.field_type,
                                            )?
                                        }
                                    }
                                    CqlType::Udt(udt_name, inline_fields) => {
                                        // Prefer registry, fall back to inline fields (Issue #239)
                                        if let Some(nested_udt) =
                                            registry.get_udt(&self.keyspace, udt_name)
                                        {
                                            let inner_value = self.parse_nested_udt_from_registry(
                                                field_data, nested_udt, registry,
                                            )?;
                                            Value::Frozen(Box::new(inner_value))
                                        } else if !inline_fields.is_empty() {
                                            let inner_value = self.parse_inline_udt_value(
                                                field_data,
                                                udt_name,
                                                inline_fields,
                                                1,
                                            )?;
                                            Value::Frozen(Box::new(inner_value))
                                        } else {
                                            Self::parse_simple_udt_field_value(
                                                field_data,
                                                &field_def.field_type,
                                            )?
                                        }
                                    }
                                    _ => Self::parse_simple_udt_field_value(
                                        field_data,
                                        &field_def.field_type,
                                    )?,
                                },
                                _ => Self::parse_simple_udt_field_value(
                                    field_data,
                                    &field_def.field_type,
                                )?,
                            }
                        } else {
                            // No registry - check for inline UDT definitions (Issue #239)
                            match &field_def.field_type {
                                CqlType::Udt(udt_name, inline_fields)
                                    if !inline_fields.is_empty() =>
                                {
                                    self.parse_inline_udt_value(
                                        field_data,
                                        udt_name,
                                        inline_fields,
                                        1,
                                    )?
                                }
                                CqlType::Frozen(inner) => match inner.as_ref() {
                                    CqlType::Udt(udt_name, inline_fields)
                                        if !inline_fields.is_empty() =>
                                    {
                                        let inner_value = self.parse_inline_udt_value(
                                            field_data,
                                            udt_name,
                                            inline_fields,
                                            1,
                                        )?;
                                        Value::Frozen(Box::new(inner_value))
                                    }
                                    _ => Self::parse_simple_udt_field_value(
                                        field_data,
                                        &field_def.field_type,
                                    )?,
                                },
                                _ => Self::parse_simple_udt_field_value(
                                    field_data,
                                    &field_def.field_type,
                                )?,
                            }
                        };
                        Some(value)
                    };

                    fields.push(UdtField {
                        name: field_def.name.clone(),
                        value: field_value,
                    });
                }

                let udt_value = UdtValue {
                    type_name: udt_def.name.clone(),
                    keyspace: udt_def.keyspace.clone(),
                    fields,
                };

                // Update offset to point after the UDT data we consumed
                offset += current_offset;

                Value::Udt(udt_value)
            }

            // Default: check if it's a short UDT name in the registry, otherwise treat as blob
            _ => {
                // Try to look up as UDT in registry by short name (Issue #238)
                // This handles cases like "address_type" which aren't in full marshal format
                if let Some(ref registry) = self.udt_registry {
                    if let Some(udt_def) = registry.get_udt(&self.keyspace, type_str) {
                        log::debug!(
                            "Frozen element '{}': found UDT '{}' in registry, parsing {} fields",
                            column_name,
                            type_str,
                            udt_def.fields.len()
                        );

                        // Parse UDT fields using the registry definition
                        // UDT data in frozen context has 4-byte big-endian i32 length prefixes for each field
                        // (-1 means null, 0 means empty, positive means field data length)
                        let udt_data = &data[offset..];
                        let mut current_offset = 0;
                        let mut fields = Vec::with_capacity(udt_def.fields.len());

                        for field_def in &udt_def.fields {
                            // Check bounds for field length (4 bytes BE i32)
                            if current_offset + 4 > udt_data.len() {
                                // Trailing fields can be omitted (implicit null)
                                log::debug!(
                                    "Frozen UDT field '{}' omitted (implicit null)",
                                    field_def.name
                                );
                                while fields.len() < udt_def.fields.len() {
                                    let remaining_field = &udt_def.fields[fields.len()];
                                    fields.push(UdtField {
                                        name: remaining_field.name.clone(),
                                        value: None,
                                    });
                                }
                                break;
                            }

                            // Read field length (4 bytes big-endian i32)
                            let field_len = i32::from_be_bytes([
                                udt_data[current_offset],
                                udt_data[current_offset + 1],
                                udt_data[current_offset + 2],
                                udt_data[current_offset + 3],
                            ]);
                            current_offset += 4;

                            let field_value = if field_len == -1 {
                                // Null field
                                None
                            } else if field_len == 0 {
                                // Empty field - parse with empty data
                                let value =
                                    Self::parse_simple_udt_field_value(&[], &field_def.field_type)?;
                                Some(value)
                            } else {
                                let field_len = field_len as usize;
                                if current_offset + field_len > udt_data.len() {
                                    return Err(Error::corruption(format!(
                                        "Frozen UDT field '{}' extends beyond data (need {}, have {})",
                                        field_def.name,
                                        field_len,
                                        udt_data.len() - current_offset
                                    )));
                                }

                                let field_data =
                                    &udt_data[current_offset..current_offset + field_len];
                                current_offset += field_len;

                                // Parse field value - handle nested UDTs specially (including FROZEN<udt>)
                                let value = match &field_def.field_type {
                                    CqlType::Custom(nested_type_name) => {
                                        // Issue #239: Handle "udt:" prefix from schema parsing
                                        let lookup_name = nested_type_name
                                            .strip_prefix("udt:")
                                            .unwrap_or(nested_type_name);
                                        // Check if this is a nested UDT
                                        if let Some(nested_udt) =
                                            registry.get_udt(&self.keyspace, lookup_name)
                                        {
                                            // Recursively parse nested UDT
                                            self.parse_nested_udt_from_registry(
                                                field_data, nested_udt, registry,
                                            )?
                                        } else {
                                            // Unknown custom type - parse as blob
                                            Value::Blob(field_data.to_vec())
                                        }
                                    }
                                    CqlType::Udt(udt_name, inline_fields) => {
                                        // Prefer registry, fall back to inline fields (Issue #239)
                                        if let Some(nested_udt) =
                                            registry.get_udt(&self.keyspace, udt_name)
                                        {
                                            self.parse_nested_udt_from_registry(
                                                field_data, nested_udt, registry,
                                            )?
                                        } else if !inline_fields.is_empty() {
                                            self.parse_inline_udt_value(
                                                field_data,
                                                udt_name,
                                                inline_fields,
                                                1,
                                            )?
                                        } else {
                                            Value::Blob(field_data.to_vec())
                                        }
                                    }
                                    CqlType::Frozen(inner) => {
                                        // Handle FROZEN<udt_type> - the inner type may be a UDT
                                        match inner.as_ref() {
                                            CqlType::Custom(nested_type_name) => {
                                                // Issue #239: Handle "udt:" prefix from schema parsing
                                                let lookup_name = nested_type_name
                                                    .strip_prefix("udt:")
                                                    .unwrap_or(nested_type_name);
                                                if let Some(nested_udt) =
                                                    registry.get_udt(&self.keyspace, lookup_name)
                                                {
                                                    let inner_value = self
                                                        .parse_nested_udt_from_registry(
                                                            field_data, nested_udt, registry,
                                                        )?;
                                                    Value::Frozen(Box::new(inner_value))
                                                } else {
                                                    Value::Frozen(Box::new(Value::Blob(
                                                        field_data.to_vec(),
                                                    )))
                                                }
                                            }
                                            CqlType::Udt(udt_name, inline_fields) => {
                                                // Prefer registry, fall back to inline fields (Issue #239)
                                                if let Some(nested_udt) =
                                                    registry.get_udt(&self.keyspace, udt_name)
                                                {
                                                    let inner_value = self
                                                        .parse_nested_udt_from_registry(
                                                            field_data, nested_udt, registry,
                                                        )?;
                                                    Value::Frozen(Box::new(inner_value))
                                                } else if !inline_fields.is_empty() {
                                                    let inner_value = self.parse_inline_udt_value(
                                                        field_data,
                                                        udt_name,
                                                        inline_fields,
                                                        1,
                                                    )?;
                                                    Value::Frozen(Box::new(inner_value))
                                                } else {
                                                    Value::Frozen(Box::new(Value::Blob(
                                                        field_data.to_vec(),
                                                    )))
                                                }
                                            }
                                            _ => {
                                                // Other frozen types - parse as simple value
                                                let inner_value =
                                                    Self::parse_simple_udt_field_value(
                                                        field_data, inner,
                                                    )?;
                                                Value::Frozen(Box::new(inner_value))
                                            }
                                        }
                                    }
                                    _ => Self::parse_simple_udt_field_value(
                                        field_data,
                                        &field_def.field_type,
                                    )?,
                                };
                                Some(value)
                            };

                            fields.push(UdtField {
                                name: field_def.name.clone(),
                                value: field_value,
                            });
                        }

                        let udt_value = UdtValue {
                            type_name: udt_def.name.clone(),
                            keyspace: udt_def.keyspace.clone(),
                            fields,
                        };

                        offset += current_offset;
                        Value::Udt(udt_value)
                    } else {
                        // Not found in registry - parse as blob
                        log::debug!(
                            "Frozen element '{}': unknown type '{}', parsing as blob",
                            column_name,
                            type_str
                        );

                        let (remaining, blob_len) = parse_vuint(&data[offset..]).map_err(|e| {
                            Error::corruption(format!(
                                "Frozen element '{}': failed to parse unknown type length as VInt: {:?}",
                                column_name, e
                            ))
                        })?;
                        let blob_len = blob_len as usize;
                        let bytes_consumed = data[offset..].len() - remaining.len();
                        offset += bytes_consumed;

                        if offset + blob_len > data.len() {
                            return Err(Error::corruption(format!(
                                "Frozen element '{}': need {} bytes for unknown type, only {} available",
                                column_name,
                                blob_len,
                                data.len() - offset
                            )));
                        }

                        let blob_bytes = data[offset..offset + blob_len].to_vec();
                        offset += blob_len;
                        Value::Blob(blob_bytes)
                    }
                } else {
                    // No registry available - parse as blob
                    log::debug!(
                        "Frozen element '{}': unknown type '{}', no UDT registry available, parsing as blob",
                        column_name,
                        type_str
                    );

                    let (remaining, blob_len) = parse_vuint(&data[offset..]).map_err(|e| {
                        Error::corruption(format!(
                            "Frozen element '{}': failed to parse unknown type length as VInt: {:?}",
                            column_name, e
                        ))
                    })?;
                    let blob_len = blob_len as usize;
                    let bytes_consumed = data[offset..].len() - remaining.len();
                    offset += bytes_consumed;

                    if offset + blob_len > data.len() {
                        return Err(Error::corruption(format!(
                            "Frozen element '{}': need {} bytes for unknown type, only {} available",
                            column_name,
                            blob_len,
                            data.len() - offset
                        )));
                    }

                    let blob_bytes = data[offset..offset + blob_len].to_vec();
                    offset += blob_len;
                    Value::Blob(blob_bytes)
                }
            }
        };

        Ok((value, offset))
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
        assert_eq!(val, Value::Text("hello".to_string()));

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

        // float (parse_value_from_raw_bytes promotes f32 to f64 via Float)
        let data = 1.5f32.to_be_bytes();
        let val = parser
            .parse_value_from_raw_bytes(&data, "float", "col", 0)
            .unwrap();
        match val {
            Value::Float(f) => assert!((f - 1.5).abs() < 0.001),
            other => panic!("Expected Float, got {:?}", other),
        }

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
        assert_eq!(val, Value::Blob(data));

        // varint
        let data = vec![0x01, 0x00];
        let val = parser
            .parse_value_from_raw_bytes(&data, "varint", "col", 0)
            .unwrap();
        assert_eq!(val, Value::Varint(vec![0x01, 0x00]));

        // inet (IPv4)
        let data = vec![127, 0, 0, 1];
        let val = parser
            .parse_value_from_raw_bytes(&data, "inet", "col", 0)
            .unwrap();
        assert_eq!(val, Value::Inet(vec![127, 0, 0, 1]));

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
                (Value::Text("alice".to_string()), Value::Integer(1)),
                (Value::Text("bob".to_string()), Value::Integer(2)),
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
                Value::Text("alpha".to_string()),
                Value::Text("beta".to_string()),
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
                (Value::Text("alice".to_string()), Value::Integer(1)),
                (Value::Text("bob".to_string()), Value::Integer(2)),
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
                Value::Text("gamma".to_string()),
                Value::Text("delta".to_string()),
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
