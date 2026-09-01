use super::*;

impl V5CompressedLegacyParser {
    /// Issue #1080: is this on-disk SerializationHeader marshal type a TOP-LEVEL
    /// frozen (or bare) UDT — `FrozenType(UserType(...))` or `UserType(...)` — that
    /// may be decoded as a single UDT blob via `decode_frozen_udt_from_header_type`?
    ///
    /// Returns FALSE for a frozen COLLECTION that merely CONTAINS a UDT, e.g.
    /// `FrozenType(ListType(UserType(...)))` or `FrozenType(MapType(...UserType...))`
    /// — those must go through the collection decoders, NOT the scalar UDT path; a
    /// substring `UserType(` test would misroute them and corrupt the value or break
    /// the row loop, dropping trailing columns (roborev job 1365).
    ///
    /// We match ONLY the fully-qualified marshal form (the single shape real
    /// SerializationHeaders carry, preserved verbatim by `convert_marshal_type_to_cql`)
    /// and require `UserType(` to be the IMMEDIATE inner type, after an optional
    /// single `FrozenType(` wrapper. The whole decode chain
    /// (`parse_udt_type_definition` + `parse_cassandra_type_with_depth`) keys on the
    /// same qualified marker at every nesting level (roborev jobs 1359/1361).
    pub(super) fn marshal_is_top_level_frozen_udt(marshal_type: &str) -> bool {
        const FROZEN: &str = "org.apache.cassandra.db.marshal.frozentype(";
        const USERTYPE: &str = "org.apache.cassandra.db.marshal.usertype(";
        let lower = marshal_type.trim().to_lowercase();
        // Strip at most one leading FrozenType( wrapper; then UserType( must be the
        // immediate inner type (rejecting FrozenType(ListType(UserType(...))) etc.).
        let inner = lower.strip_prefix(FROZEN).unwrap_or(&lower);
        inner.starts_with(USERTYPE)
    }

    /// Issue #1080: decode a single-cell frozen UDT value using the AUTHORITATIVE
    /// on-disk SerializationHeader marshal type (the fully-qualified
    /// `FrozenType(UserType(...))` marshal string), used when no `UdtRegistry` is
    /// wired and the supplied schema short form (`frozen<person_type>`) carries no
    /// field defs.
    ///
    /// `parse_udt_type_definition` does a case-insensitive find for the qualified
    /// `UserType(` marker, so
    /// the `FrozenType(...)` wrapper is transparently handled and nested
    /// `FrozenType(UserType(...))` fields resolve to `CqlType::Frozen(CqlType::Udt)`
    /// (decoded recursively by `parse_udt_field_value`). Returns the decoded UDT
    /// value (UNWRAPPED — the caller wraps it in `Value::Frozen`) and the new offset
    /// AFTER the VInt-prefixed blob, so trailing columns stay byte-aligned.
    pub(super) fn decode_frozen_udt_from_header_type(
        &self,
        data: &[u8],
        mut offset: usize,
        header_type: &str,
        column: &crate::schema::Column,
    ) -> Result<(Value, usize)> {
        let udt_def = Self::parse_udt_type_definition(header_type)?;

        // Read the VInt-prefixed blob length (same framing as the registry /
        // marshal-format UDT blocks in the frozen< arm).
        let (remaining, blob_len_raw) = parse_vuint(&data[offset..]).map_err(|e| {
            Error::corruption(format!(
                "Frozen UDT (column '{}', on-disk header type): failed to parse blob length: {:?}",
                column.name, e
            ))
        })?;
        if blob_len_raw > MAX_CELL_VALUE_LENGTH {
            return Err(Error::corruption(format!(
                "Frozen UDT (column '{}', on-disk header type): blob_len {} exceeds maximum {}",
                column.name, blob_len_raw, MAX_CELL_VALUE_LENGTH
            )));
        }
        let blob_len = blob_len_raw as usize;
        let len_bytes_consumed = data[offset..].len() - remaining.len();
        offset += len_bytes_consumed;

        if offset + blob_len > data.len() {
            return Err(Error::corruption(format!(
                "Frozen UDT (column '{}', on-disk header type): need {} bytes but only {} available",
                column.name,
                blob_len,
                data.len() - offset
            )));
        }

        let udt_data = &data[offset..offset + blob_len];
        let (udt_value, _) = self.parse_udt_value(udt_data, 0, &udt_def, column)?;
        offset += blob_len;

        Ok((udt_value, offset))
    }

    /// Extract inner type from a frozen type string (CQL or Cassandra internal format).
    ///
    /// Accepts both the CQL short form `frozen<T>` and the authoritative on-disk
    /// marshal form `org.apache.cassandra.db.marshal.FrozenType(T)`. Multicell-UDT
    /// field types resolve from the `UserType(...)` marshal string, where frozen
    /// fields are spelled `FrozenType(...)` (e.g. `frozen<list<int>>` →
    /// `FrozenType(ListType(Int32Type))`), so we mirror how
    /// `extract_collection_element_type` supports both forms. In both cases the
    /// inner substring is sliced from the ORIGINAL-CASE input so nested marshal
    /// types keep their case and re-normalize correctly on recursion.
    pub(super) fn extract_frozen_inner_type(&self, type_str: &str) -> Result<String> {
        let type_lower = type_str.to_lowercase();

        // Cassandra internal format: org.apache.cassandra.db.marshal.FrozenType(T)
        let internal_prefix_lower = "org.apache.cassandra.db.marshal.frozentype(";
        if type_lower.starts_with(internal_prefix_lower) && type_lower.ends_with(')') {
            let inner = &type_str[internal_prefix_lower.len()..type_str.len() - 1];
            if inner.is_empty() {
                return Err(Error::schema(format!("Empty frozen type: {}", type_str)));
            }
            return Ok(inner.to_string());
        }

        // CQL format: frozen<T> — match the prefix/suffix case-insensitively
        // (parse_value_from_raw_bytes routes here off a lowercased guard but
        // passes the ORIGINAL-CASE string, so a mixed-case `Frozen<...>` reaches
        // this branch and must still be accepted) while slicing from the
        // original string to preserve nested-type case.
        if type_lower.starts_with("frozen<") && type_lower.ends_with('>') {
            let inner = &type_str[7..type_str.len() - 1];
            if inner.is_empty() {
                return Err(Error::schema(format!("Empty frozen type: {}", type_str)));
            }
            return Ok(inner.to_string());
        }

        Err(Error::schema(format!(
            "Invalid frozen type format: {}",
            type_str
        )))
    }

    /// Check if a type string represents a UDT (User-Defined Type).
    /// Detects Cassandra's internal format: org.apache.cassandra.db.marshal.UserType(...)
    pub(super) fn is_udt_type(type_str: &str) -> bool {
        // ASCII case-insensitive substring match without allocating a lowercased
        // copy. The marshal name is pure ASCII so byte-window comparison is safe.
        const TARGET: &[u8] = b"org.apache.cassandra.db.marshal.usertype";
        let bytes = type_str.as_bytes();
        if bytes.len() < TARGET.len() {
            return false;
        }
        bytes
            .windows(TARGET.len())
            .any(|w| w.iter().zip(TARGET).all(|(a, b)| a.eq_ignore_ascii_case(b)))
    }

    /// Parse a UDT type string to extract the UDT definition.
    /// Cassandra encodes UDTs as:
    /// `UserType(keyspace,hex_name,field1_hex:type1,field2_hex:type2,...)`
    ///
    /// Example:
    /// ```text
    /// org.apache.cassandra.db.marshal.UserType(
    ///   test_collections,
    ///   616464726573735f74797065,    // hex("address_type")
    ///   737472656574:UTF8Type,        // street:UTF8Type
    ///   63697479:UTF8Type,            // city:UTF8Type
    ///   ...
    /// )
    /// ```
    pub(super) fn parse_udt_type_definition(type_str: &str) -> Result<UdtTypeDef> {
        Self::parse_udt_type_definition_with_depth(type_str, 0)
    }

    /// Internal implementation of parse_udt_type_definition with recursion depth tracking.
    fn parse_udt_type_definition_with_depth(type_str: &str, depth: usize) -> Result<UdtTypeDef> {
        // Check recursion depth to prevent stack overflow
        if depth > MAX_TYPE_NESTING_DEPTH {
            return Err(Error::schema(format!(
                "UDT nesting depth {} exceeds maximum {}. Type string: {}",
                depth,
                MAX_TYPE_NESTING_DEPTH,
                type_str.chars().take(100).collect::<String>()
            )));
        }

        // Find the UserType(...) portion (case-insensitive). Match ONLY the
        // fully-qualified marshal marker — the single shape real SerializationHeaders
        // carry, and the same marker the nested-field decoder
        // (`parse_cassandra_type_with_depth`) keys on. Keeping top-level and nested
        // parsing on the same qualified marker avoids the partial-bare-support
        // inconsistency that would blob nested UDT fields (roborev jobs 1359/1361).
        let start_marker = "org.apache.cassandra.db.marshal.UserType(";
        let type_lower = type_str.to_lowercase();
        let start_marker_lower = start_marker.to_lowercase();
        let start_idx = type_lower
            .find(&start_marker_lower)
            .ok_or_else(|| Error::schema(format!("Not a UserType: {}", type_str)))?;

        // Find the matching close paren (handling nested types)
        let inner_start = start_idx + start_marker.len();
        let mut paren_depth = 1;
        let mut end_idx = inner_start;
        let chars: Vec<char> = type_str[inner_start..].chars().collect();

        for (i, c) in chars.iter().enumerate() {
            match c {
                '(' => paren_depth += 1,
                ')' => {
                    paren_depth -= 1;
                    if paren_depth == 0 {
                        end_idx = inner_start + i;
                        break;
                    }
                }
                _ => {}
            }
        }

        if paren_depth != 0 {
            return Err(Error::schema(format!(
                "Unbalanced parentheses in UserType: {}",
                type_str
            )));
        }

        let inner = &type_str[inner_start..end_idx];

        // Split by comma, but respect nested parentheses
        let parts = Self::split_type_args(inner)?;
        if parts.len() < 2 {
            return Err(Error::schema(format!(
                "UserType requires at least keyspace and name: {}",
                inner
            )));
        }

        // First part is keyspace
        let keyspace = parts[0].trim();
        if keyspace.is_empty() {
            return Err(Error::schema("UDT keyspace cannot be empty"));
        }
        let keyspace = keyspace.to_string();

        // Second part is hex-encoded type name
        let udt_name = Self::decode_hex_name(parts[1].trim())?;

        // Remaining parts are field definitions: hex_name:type
        let mut udt_def = UdtTypeDef::new(keyspace, udt_name);
        for field_def in parts.iter().skip(2) {
            let field_def = field_def.trim();
            if field_def.is_empty() {
                continue;
            }

            // Split on first colon (field name is before, type is after)
            if let Some(colon_idx) = field_def.find(':') {
                let field_name_hex = &field_def[..colon_idx];
                let field_type_str = &field_def[colon_idx + 1..];

                let field_name = Self::decode_hex_name(field_name_hex)?;
                // Use depth-aware version to track recursion through UDT fields
                let field_type = Self::parse_cassandra_type_with_depth(field_type_str, depth)?;

                udt_def = udt_def.with_field(field_name, field_type, true);
            } else {
                return Err(Error::schema(format!(
                    "Invalid UDT field definition (missing colon): {}",
                    field_def
                )));
            }
        }

        Ok(udt_def)
    }

    /// Split type arguments by comma, respecting nested parentheses.
    fn split_type_args(s: &str) -> Result<Vec<String>> {
        let mut parts = Vec::new();
        let mut current = String::new();
        let mut depth = 0;

        for c in s.chars() {
            match c {
                '(' => {
                    depth += 1;
                    current.push(c);
                }
                ')' => {
                    depth -= 1;
                    current.push(c);
                }
                ',' if depth == 0 => {
                    parts.push(current.clone());
                    current.clear();
                }
                _ => current.push(c),
            }
        }

        if !current.is_empty() {
            parts.push(current);
        }

        Ok(parts)
    }

    /// Decode a hex-encoded name (e.g., "616464726573735f74797065" -> "address_type")
    fn decode_hex_name(hex: &str) -> Result<String> {
        let bytes = hex::decode(hex)
            .map_err(|e| Error::schema(format!("Invalid hex-encoded UDT name '{}': {}", hex, e)))?;
        String::from_utf8(bytes)
            .map_err(|e| Error::schema(format!("Invalid UTF-8 in UDT name '{}': {}", hex, e)))
    }

    /// Parse a Cassandra type string into a CqlType.
    /// Handles: UTF8Type, Int32Type, ListType(...), SetType(...), MapType(...), UserType(...), FrozenType(...)
    #[allow(dead_code)]
    pub(super) fn parse_cassandra_type(type_str: &str) -> Result<CqlType> {
        Self::parse_cassandra_type_with_depth(type_str, 0)
    }

    /// Internal implementation of parse_cassandra_type with recursion depth tracking.
    fn parse_cassandra_type_with_depth(type_str: &str, depth: usize) -> Result<CqlType> {
        // Check recursion depth to prevent stack overflow
        if depth > MAX_TYPE_NESTING_DEPTH {
            return Err(Error::schema(format!(
                "Type nesting depth {} exceeds maximum {}. Type string: {}",
                depth,
                MAX_TYPE_NESTING_DEPTH,
                type_str.chars().take(100).collect::<String>()
            )));
        }

        let type_str = type_str.trim();

        // Handle FrozenType wrapper
        if type_str.starts_with("org.apache.cassandra.db.marshal.FrozenType(") {
            let inner_start = "org.apache.cassandra.db.marshal.FrozenType(".len();
            let inner = Self::extract_inner_parens(&type_str[inner_start..])?;
            let inner_type = Self::parse_cassandra_type_with_depth(&inner, depth + 1)?;
            return Ok(CqlType::Frozen(Box::new(inner_type)));
        }

        // Handle UserType (nested UDT)
        if type_str.starts_with("org.apache.cassandra.db.marshal.UserType(") {
            let udt_def = Self::parse_udt_type_definition_with_depth(type_str, depth + 1)?;
            let fields: Vec<(String, CqlType)> = udt_def
                .fields
                .into_iter()
                .map(|f| (f.name, f.field_type))
                .collect();
            return Ok(CqlType::Udt(udt_def.name, fields));
        }

        // Handle collection types
        if type_str.starts_with("org.apache.cassandra.db.marshal.ListType(") {
            let inner_start = "org.apache.cassandra.db.marshal.ListType(".len();
            let inner = Self::extract_inner_parens(&type_str[inner_start..])?;
            let elem_type = Self::parse_cassandra_type_with_depth(&inner, depth + 1)?;
            return Ok(CqlType::List(Box::new(elem_type)));
        }

        if type_str.starts_with("org.apache.cassandra.db.marshal.SetType(") {
            let inner_start = "org.apache.cassandra.db.marshal.SetType(".len();
            let inner = Self::extract_inner_parens(&type_str[inner_start..])?;
            let elem_type = Self::parse_cassandra_type_with_depth(&inner, depth + 1)?;
            return Ok(CqlType::Set(Box::new(elem_type)));
        }

        if type_str.starts_with("org.apache.cassandra.db.marshal.MapType(") {
            let inner_start = "org.apache.cassandra.db.marshal.MapType(".len();
            let inner = Self::extract_inner_parens(&type_str[inner_start..])?;
            let parts = Self::split_type_args(&inner)?;
            if parts.len() != 2 {
                return Err(Error::schema(format!(
                    "MapType requires exactly 2 type arguments: {}",
                    type_str
                )));
            }
            let key_type = Self::parse_cassandra_type_with_depth(&parts[0], depth + 1)?;
            let val_type = Self::parse_cassandra_type_with_depth(&parts[1], depth + 1)?;
            return Ok(CqlType::Map(Box::new(key_type), Box::new(val_type)));
        }

        // Handle primitive types
        Ok(match type_str {
            s if s.ends_with("UTF8Type") => CqlType::Text,
            s if s.ends_with("AsciiType") => CqlType::Ascii,
            s if s.ends_with("Int32Type") => CqlType::Int,
            s if s.ends_with("LongType") => CqlType::BigInt,
            s if s.ends_with("FloatType") => CqlType::Float,
            s if s.ends_with("DoubleType") => CqlType::Double,
            s if s.ends_with("BooleanType") => CqlType::Boolean,
            s if s.ends_with("UUIDType") || s.ends_with("TimeUUIDType") => CqlType::Uuid,
            s if s.ends_with("TimestampType") => CqlType::Timestamp,
            s if s.ends_with("DateType") || s.ends_with("SimpleDateType") => CqlType::Date,
            s if s.ends_with("TimeType") => CqlType::Time,
            s if s.ends_with("DecimalType") => CqlType::Decimal,
            s if s.ends_with("IntegerType") => CqlType::Varint,
            s if s.ends_with("BytesType") => CqlType::Blob,
            s if s.ends_with("InetAddressType") => CqlType::Inet,
            // Issue #3722: these four resolved to `Custom` and were the ROOT of a
            // recurring defect family, not a cosmetic gap.
            //
            // On a schema-less read the field types come from the marshal header,
            // so a `smallint` field's type was `Custom("…ShortType")` while under
            // the CQL-short spelling it was `CqlType::SmallInt`. The two SPELLINGS
            // therefore became DIFFERENT `CqlType`s before any decoder ran, and
            // every decode site had to re-normalize — which is where three
            // separate roborev findings landed across two review rounds (the empty
            // path diverging, the non-empty path taking the lenient decoder, and a
            // suffix-normalization workaround misclassifying a registry UDT named
            // `udt:ShortType`). Naming the types here makes both spellings the SAME
            // `CqlType` before dispatch and removes the need for any normalization
            // at a decode site, so the family is eliminated rather than narrowed.
            //
            // `BytesType` does NOT shadow `ByteType` and vice versa (neither string
            // ends with the other), so arm order is not load-bearing here.
            s if s.ends_with("ShortType") => CqlType::SmallInt,
            s if s.ends_with("ByteType") => CqlType::TinyInt,
            s if s.ends_with("VarcharType") => CqlType::Text,
            s if s.ends_with("DurationType") => CqlType::Duration,
            // DELIBERATELY NOT `CounterColumnType`, which also falls through to
            // `Custom`. A counter cell stores a CounterContext, not a raw i64 (see
            // `parse_counter_context`), so resolving it to `CqlType::Counter` here
            // could make a schema-less counter COLUMN read 8 raw bytes instead of
            // parsing the context — a behaviour change on a path this issue does
            // not touch, for no benefit: Cassandra refuses `counter` as a UDT
            // field outright ("A user type cannot contain counters", measured
            // against 5.0.2), so it is unreachable as a UDT field type.
            //
            // `LexicalUUIDType` is likewise absent because it is NOT missing: it
            // ends with `UUIDType` and so already resolves to `CqlType::Uuid` on
            // the arm above. Stated because an earlier count of this gap said
            // "six" and the real number is four.
            _ => CqlType::Custom(type_str.to_string()),
        })
    }

    /// Extract the contents inside parentheses, respecting nesting.
    fn extract_inner_parens(s: &str) -> Result<String> {
        let mut depth = 1;
        let mut end_idx = 0;
        let chars: Vec<char> = s.chars().collect();

        for (i, c) in chars.iter().enumerate() {
            match c {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        end_idx = i;
                        break;
                    }
                }
                _ => {}
            }
        }

        if depth != 0 {
            return Err(Error::schema(format!(
                "Unbalanced parentheses in type: {}",
                s
            )));
        }

        Ok(s[..end_idx].to_string())
    }

    /// Parse a UDT value from binary data using the given UDT definition.
    /// UDT binary format (frozen):
    /// - For each field in schema order:
    ///   - [4 bytes BE i32]: field length (-1 = null, 0 = empty, >0 = data length)
    ///   - [N bytes]: field data (if length > 0)
    // NOTE: this UDT decoder is purely structural — it reads the i32-length-prefixed
    // field layout using only the [`UdtTypeDef`] field types. It does NOT need an
    // [`SSTableReader`] (the previous `reader` param was threaded through but never
    // dereferenced), so it is reader-free and unit-testable in isolation (issue #1080).
    pub(super) fn parse_udt_value(
        &self,
        data: &[u8],
        offset: usize,
        udt_def: &UdtTypeDef,
        _column: &crate::schema::Column,
    ) -> Result<(Value, usize)> {
        // Validate field count to prevent memory exhaustion
        if udt_def.fields.len() > MAX_UDT_FIELD_COUNT {
            return Err(Error::schema(format!(
                "UDT '{}' has {} fields, exceeds maximum {}",
                udt_def.name,
                udt_def.fields.len(),
                MAX_UDT_FIELD_COUNT
            )));
        }

        let mut current_offset = offset;
        let mut fields = Vec::with_capacity(udt_def.fields.len());

        tracing::debug!(
            "V5CompressedLegacy: Parsing UDT '{}' with {} fields at offset {}",
            udt_def.name,
            udt_def.fields.len(),
            offset
        );

        for field_def in &udt_def.fields {
            // Check bounds for field length (4 bytes)
            if current_offset + 4 > data.len() {
                // Trailing fields can be omitted (implicit null)
                tracing::debug!(
                    "V5CompressedLegacy: UDT field '{}' omitted (implicit null), remaining fields omitted",
                    field_def.name
                );
                // Fill remaining fields with null
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
                data[current_offset],
                data[current_offset + 1],
                data[current_offset + 2],
                data[current_offset + 3],
            ]);
            current_offset += 4;

            let field_value = if field_len == -1 {
                // Null field
                tracing::debug!("V5CompressedLegacy: UDT field '{}' is null", field_def.name);
                None
            } else if field_len == 0 {
                // Empty field: through THE decoder, at length 0 (issue #3722).
                // It used to call a `create_empty_value_for_type` helper whose
                // fallback arm was `Value::Blob`, so an empty `varint`,
                // `decimal`, `time`, `inet`, `tuple` or nested `udt` field came
                // back an opaque blob. See `udt_field_empty` for the semantics
                // and their Cassandra sources.
                tracing::debug!(
                    "V5CompressedLegacy: UDT field '{}' is empty",
                    field_def.name
                );
                Some(self.parse_udt_field_value(&[], &field_def.field_type, 0)?)
            } else {
                // Field with data. `checked_component_len` owns BOTH the negative
                // rejection and the bounds test, so no loop can have one without
                // the other (issue #3612, R3-F1).
                let field_len = Self::checked_component_len(
                    field_len,
                    &field_def.name,
                    current_offset,
                    data.len(),
                )?;

                let field_data = &data[current_offset..current_offset + field_len];
                current_offset += field_len;

                tracing::debug!(
                    "V5CompressedLegacy: UDT field '{}' has {} bytes of data",
                    field_def.name,
                    field_len
                );

                // Parse field value based on its type
                let value = self.parse_udt_field_value(field_data, &field_def.field_type, 0)?;
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

        Ok((Value::Udt(Box::new(udt_value)), current_offset))
    }

    /// Parse a CounterContext structure and return the total counter value.
    ///
    /// Counter cells in Cassandra store a CounterContext, not a raw i64 value.
    /// The CounterContext tracks counter updates across multiple replicas (shards).
    ///
    /// Format (from Cassandra's CounterContext.java):
    /// ```text
    /// [header_size: 2-byte BE signed short]    <- Number of shards (negative if cleanup needed)
    /// [indices: 2 bytes * |header_size|]       <- Shard type indicators (negative = global)
    /// [shards: 32 bytes each]:
    ///     [counter_id: 16 bytes UUID]          <- Replica's CounterId
    ///     [clock: 8-byte BE unsigned long]     <- Logical clock
    ///     [count: 8-byte BE signed long]       <- The actual counter value for this shard
    /// ```
    ///
    /// The counter value is the sum of all shard counts, matching Cassandra's `total()` function.
    ///
    /// Returns (total_value, bytes_consumed)
    pub(super) fn parse_counter_context(
        data: &[u8],
        offset: usize,
        column_name: &str,
    ) -> Result<(i64, usize)> {
        // Constants from CounterContext.java
        const HEADER_SIZE_LENGTH: usize = 2;
        const HEADER_ELT_LENGTH: usize = 2;
        const COUNTER_ID_LENGTH: usize = 16;
        const CLOCK_LENGTH: usize = 8;
        const COUNT_LENGTH: usize = 8;
        const STEP_LENGTH: usize = COUNTER_ID_LENGTH + CLOCK_LENGTH + COUNT_LENGTH; // 32

        // Maximum reasonable shard count to prevent DoS from corrupted data
        // A typical Cassandra cluster has at most 100-500 nodes, so 1024 is generous
        const MAX_COUNTER_SHARDS: usize = 1024;

        let mut pos = offset;

        // Read header_size (2-byte BE signed short)
        if pos + HEADER_SIZE_LENGTH > data.len() {
            return Err(Error::corruption(format!(
                "Counter '{}': need {} bytes for header_size at offset {}, only {} available",
                column_name,
                HEADER_SIZE_LENGTH,
                pos,
                data.len() - pos
            )));
        }
        let header_size_raw = i16::from_be_bytes([data[pos], data[pos + 1]]);
        // Negative header_size indicates local shards need cleanup (CASSANDRA-1938).
        // The absolute value gives the actual shard count.
        let shard_count = header_size_raw.unsigned_abs() as usize;
        pos += HEADER_SIZE_LENGTH;

        // Validate shard count to prevent DoS from corrupted data
        if shard_count > MAX_COUNTER_SHARDS {
            return Err(Error::corruption(format!(
                "Counter '{}': unreasonable shard count {} (max {})",
                column_name, shard_count, MAX_COUNTER_SHARDS
            )));
        }

        tracing::debug!(
            "V5CompressedLegacy: Counter '{}' header_size={}, shard_count={}",
            column_name,
            header_size_raw,
            shard_count
        );

        // Handle empty counter context (0 shards = counter value of 0)
        if shard_count == 0 {
            return Ok((0, HEADER_SIZE_LENGTH));
        }

        // Skip header indices (2 bytes per shard)
        let indices_size = HEADER_ELT_LENGTH * shard_count;
        if pos + indices_size > data.len() {
            return Err(Error::corruption(format!(
                "Counter '{}': need {} bytes for indices at offset {}, only {} available",
                column_name,
                indices_size,
                pos,
                data.len() - pos
            )));
        }
        pos += indices_size;

        // Calculate expected body size
        let body_size = STEP_LENGTH * shard_count;
        if pos + body_size > data.len() {
            return Err(Error::corruption(format!(
                "Counter '{}': need {} bytes for {} shards at offset {}, only {} available",
                column_name,
                body_size,
                shard_count,
                pos,
                data.len() - pos
            )));
        }

        // Sum count values from all shards (matching Cassandra's total() function)
        let mut total: i64 = 0;
        for shard_idx in 0..shard_count {
            // Skip counter_id (16 bytes) and clock (8 bytes), read count (8 bytes)
            let count_offset = pos + (shard_idx * STEP_LENGTH) + COUNTER_ID_LENGTH + CLOCK_LENGTH;
            let count = i64::from_be_bytes([
                data[count_offset],
                data[count_offset + 1],
                data[count_offset + 2],
                data[count_offset + 3],
                data[count_offset + 4],
                data[count_offset + 5],
                data[count_offset + 6],
                data[count_offset + 7],
            ]);
            // Use checked_add to detect overflow (unlike Java which silently wraps)
            total = total.checked_add(count).ok_or_else(|| {
                Error::corruption(format!(
                    "Counter '{}': integer overflow when summing shard {} (total={}, count={})",
                    column_name, shard_idx, total, count
                ))
            })?;

            tracing::trace!(
                "V5CompressedLegacy: Counter '{}' shard {} count={}",
                column_name,
                shard_idx,
                count
            );
        }

        // Total bytes consumed
        let consumed = HEADER_SIZE_LENGTH + indices_size + body_size;

        Ok((total, consumed))
    }

    /// Parse a nested UDT from registry definition (Issue #238)
    /// Used when parsing UDT fields that are themselves UDTs
    /// `depth` counts CQL type nesting and is checked on ENTRY, because this
    /// function recurses into itself for a registry-resolved nested UDT. Without
    /// it a chain of registry types recursed until stack exhaustion — reachable
    /// from a schema-less read of hostile bytes, since the chain comes from the
    /// marshal header (roborev round 4 on #3722; the same class as the reset-to-0
    /// defect round 2 found in the inline-UDT arm).
    pub(super) fn parse_nested_udt_from_registry(
        &self,
        data: &[u8],
        udt_def: &crate::types::UdtTypeDef,
        registry: &UdtRegistry,
        depth: usize,
    ) -> Result<Value> {
        if depth > MAX_TYPE_NESTING_DEPTH {
            return Err(Error::corruption(format!(
                "Nested UDT '{}': type nesting depth {} exceeds maximum {}",
                udt_def.name, depth, MAX_TYPE_NESTING_DEPTH
            )));
        }
        let mut current_offset = 0;
        let mut fields = Vec::with_capacity(udt_def.fields.len());

        for field_def in &udt_def.fields {
            // Check bounds for field length (4 bytes BE i32)
            if current_offset + 4 > data.len() {
                // Trailing fields are implicit null
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
                data[current_offset],
                data[current_offset + 1],
                data[current_offset + 2],
                data[current_offset + 3],
            ]);
            current_offset += 4;

            let field_value = if field_len == -1 {
                None
            } else if field_len == 0 {
                // `depth`, NOT 0: a literal here restarted the nesting budget, so a
                // chain UDT -> collection<UDT> -> collection<UDT> recursed without
                // limit even with this function's own entry guard in place
                // (roborev round 5 on #3722 — the THIRD round to find an
                // un-threaded site in this family, which is why the guard is now a
                // behavioural deep-chain test and not per-site trust).
                let value = self.parse_udt_field_value(&[], &field_def.field_type, depth)?;
                Some(value)
            } else {
                let field_len = Self::checked_component_len(
                    field_len,
                    &field_def.name,
                    current_offset,
                    data.len(),
                )?;

                let field_data = &data[current_offset..current_offset + field_len];
                current_offset += field_len;

                // Handle deeply nested UDTs (including FROZEN<udt> types)
                let value = match &field_def.field_type {
                    CqlType::Custom(nested_type_name) => {
                        // `get_udt_qualified` owns "udt:" + keyspace-qualifier
                        // normalization (Issue #239 / #2807).
                        if let Some(nested_udt) =
                            registry.get_udt_qualified(&self.keyspace, nested_type_name)
                        {
                            self.parse_nested_udt_from_registry(
                                field_data,
                                nested_udt,
                                registry,
                                depth + 1,
                            )?
                        } else {
                            Value::Blob(
                                crate::storage::sstable::reader::value_borrow::borrow_active(
                                    field_data,
                                ),
                            )
                        }
                    }
                    CqlType::Udt(udt_name, inline_fields) => {
                        // Inline UDT type - prefer registry, fall back to inline fields (Issue #239)
                        if let Some(nested_udt) =
                            registry.get_udt_qualified(&self.keyspace, udt_name)
                        {
                            self.parse_nested_udt_from_registry(
                                field_data,
                                nested_udt,
                                registry,
                                depth + 1,
                            )?
                        } else if !inline_fields.is_empty() {
                            // Issue #239: Use inline field definitions for nested UDTs
                            self.parse_inline_udt_value(
                                field_data,
                                udt_name,
                                inline_fields,
                                depth + 1,
                            )?
                        } else {
                            Value::Blob(
                                crate::storage::sstable::reader::value_borrow::borrow_active(
                                    field_data,
                                ),
                            )
                        }
                    }
                    CqlType::Frozen(inner) => {
                        // Handle FROZEN<udt_type> - the inner type may be a UDT
                        match inner.as_ref() {
                            CqlType::Custom(nested_type_name) => {
                                // `get_udt_qualified` owns "udt:" + keyspace-qualifier
                                // normalization (Issue #239 / #2807).
                                if let Some(nested_udt) =
                                    registry.get_udt_qualified(&self.keyspace, nested_type_name)
                                {
                                    let inner_value = self.parse_nested_udt_from_registry(
                                        field_data,
                                        nested_udt,
                                        registry,
                                        depth + 1,
                                    )?;
                                    Value::Frozen(Box::new(inner_value))
                                } else {
                                    Value::Frozen(Box::new(Value::Blob(crate::storage::sstable::reader::value_borrow::borrow_active(field_data))))
                                }
                            }
                            CqlType::Udt(udt_name, inline_fields) => {
                                // Prefer registry, fall back to inline fields (Issue #239)
                                if let Some(nested_udt) =
                                    registry.get_udt_qualified(&self.keyspace, udt_name)
                                {
                                    let inner_value = self.parse_nested_udt_from_registry(
                                        field_data,
                                        nested_udt,
                                        registry,
                                        depth + 1,
                                    )?;
                                    Value::Frozen(Box::new(inner_value))
                                } else if !inline_fields.is_empty() {
                                    // Issue #239: Use inline field definitions
                                    let inner_value = self.parse_inline_udt_value(
                                        field_data,
                                        udt_name,
                                        inline_fields,
                                        1,
                                    )?;
                                    Value::Frozen(Box::new(inner_value))
                                } else {
                                    Value::Frozen(Box::new(Value::Blob(crate::storage::sstable::reader::value_borrow::borrow_active(field_data))))
                                }
                            }
                            _ => {
                                // Other frozen types - parse as simple value
                                let inner_value =
                                    self.parse_udt_field_value(field_data, inner, depth + 1)?;
                                Value::Frozen(Box::new(inner_value))
                            }
                        }
                    }
                    // `depth`, NOT 0. This fall-through is the arm a COLLECTION field
                    // type takes, and it was the last reset in this family: a chain
                    // `UDT -> frozen<list<frozen<UDT>>> -> ...` decoded 30 levels deep
                    // against a budget of 10 until this literal was replaced. Caught by
                    // `a_collection_mediated_udt_chain_deeper_than_the_budget_errors`,
                    // which is why that guard is behavioural rather than per-site.
                    _ => self.parse_udt_field_value(field_data, &field_def.field_type, depth)?,
                };
                Some(value)
            };

            fields.push(UdtField {
                name: field_def.name.clone(),
                value: field_value,
            });
        }

        Ok(Value::Udt(Box::new(UdtValue {
            type_name: udt_def.name.clone(),
            keyspace: udt_def.keyspace.clone(),
            fields,
        })))
    }

    /// Parse a UDT using inline field definitions from CqlType::Udt
    /// Used when we have inline type info but no registry entry (Issue #239)
    ///
    /// This handles the case where a UDT contains a nested UDT field, and the
    /// nested UDT's field definitions are available inline in the CqlType structure
    /// (parsed from the Statistics.db type string) rather than from the UdtRegistry.
    pub(super) fn parse_inline_udt_value(
        &self,
        data: &[u8],
        type_name: &str,
        inline_fields: &[(String, CqlType)],
        depth: usize,
    ) -> Result<Value> {
        if depth > MAX_TYPE_NESTING_DEPTH {
            return Err(Error::corruption(format!(
                "UDT nesting depth {} exceeds maximum {}",
                depth, MAX_TYPE_NESTING_DEPTH
            )));
        }

        let mut current_offset = 0;
        let mut fields = Vec::with_capacity(inline_fields.len());

        for (field_name, field_type) in inline_fields {
            // Check bounds for field length (4 bytes BE i32)
            if current_offset + 4 > data.len() {
                // Trailing fields are implicit null
                while fields.len() < inline_fields.len() {
                    let remaining_field = &inline_fields[fields.len()];
                    fields.push(UdtField {
                        name: remaining_field.0.clone(),
                        value: None,
                    });
                }
                break;
            }

            // Read field length (4 bytes big-endian i32)
            let field_len = i32::from_be_bytes([
                data[current_offset],
                data[current_offset + 1],
                data[current_offset + 2],
                data[current_offset + 3],
            ]);
            current_offset += 4;

            let field_value = if field_len == -1 {
                // Null field
                None
            } else if field_len == 0 {
                // Empty value
                let value = self.parse_udt_field_value(&[], field_type, depth)?;
                Some(value)
            } else {
                let field_len =
                    Self::checked_component_len(field_len, field_name, current_offset, data.len())?;

                let field_data = &data[current_offset..current_offset + field_len];
                current_offset += field_len;

                // Handle nested UDTs using inline field definitions (Issue #239)
                let value = match field_type {
                    CqlType::Udt(nested_name, nested_fields) if !nested_fields.is_empty() => {
                        // Recursively parse nested UDT using its inline fields
                        self.parse_inline_udt_value(
                            field_data,
                            nested_name,
                            nested_fields,
                            depth + 1,
                        )?
                    }
                    CqlType::Frozen(inner) => match inner.as_ref() {
                        CqlType::Udt(nested_name, nested_fields) if !nested_fields.is_empty() => {
                            // Frozen nested UDT - unwrap and parse
                            let inner_value = self.parse_inline_udt_value(
                                field_data,
                                nested_name,
                                nested_fields,
                                depth + 1,
                            )?;
                            Value::Frozen(Box::new(inner_value))
                        }
                        _ => {
                            // Other frozen types - parse as simple value
                            let inner_value =
                                self.parse_udt_field_value(field_data, inner, depth + 1)?;
                            Value::Frozen(Box::new(inner_value))
                        }
                    },
                    _ => self.parse_udt_field_value(field_data, field_type, depth)?,
                };
                Some(value)
            };

            fields.push(UdtField {
                name: field_name.clone(),
                value: field_value,
            });
        }

        Ok(Value::Udt(Box::new(UdtValue {
            type_name: type_name.to_string(),
            keyspace: self.keyspace.clone(),
            fields,
        })))
    }

    /// Returns true if the column type is a complex column (non-frozen collection).
    /// Complex columns are stored as multiple cells with cell paths, unlike
    /// frozen collections which are stored as a single cell with blob value.
    ///
    /// Issue #221: This is critical for proper parsing - complex columns have
    /// a different format: [complex_deletion_time?] [cell_count] [cells...]
    pub(super) fn is_complex_column(data_type: &str) -> bool {
        // J1 (issue #1635): this is now called ONCE per column at
        // `RowColumnResolution::build` time (its result cached on
        // `ColumnToParse.is_complex`), NOT per cell. The per-cell-loop
        // `TYPE_NORMALIZE_CALLS` gauge is therefore no longer recorded here — the
        // per-cell decode path performs zero type normalizations after J1.
        let dt = data_type.to_lowercase();
        // Non-frozen collections start directly with list/set/map (CQL syntax)
        // or org.apache.cassandra.db.marshal.ListType/SetType/MapType (internal syntax)
        // Collections containing frozen element types (e.g., list<frozen<...>>) are still complex
        // collections because the outer collection is not frozen - only the elements are.
        // Only frozen<list<...>> etc. are not complex (they're single-cell frozen types)

        // Check for frozen collections (which are NOT complex)
        if dt.starts_with("frozen<")
            || dt.starts_with("org.apache.cassandra.db.marshal.frozentype(")
        {
            return false;
        }

        // Check for CQL-style collection types
        if dt.starts_with("list<") || dt.starts_with("set<") || dt.starts_with("map<") {
            return true;
        }

        // Check for Cassandra internal collection types
        if dt.starts_with("org.apache.cassandra.db.marshal.listtype(")
            || dt.starts_with("org.apache.cassandra.db.marshal.settype(")
            || dt.starts_with("org.apache.cassandra.db.marshal.maptype(")
        {
            return true;
        }

        // Issue #927: a TOP-LEVEL non-frozen UDT is a first-class multi-cell
        // complex column — each field is stored as its own cell whose cell_path
        // is the 2-byte (signed ShortType) field index. Frozen UDTs were already
        // excluded above (frozen<...> / FrozenType(...)), and a UDT nested inside
        // a collection (e.g. ListType(UserType(...))) is matched by its outer
        // list/set/map branch, so a bare `usertype(` prefix here is unambiguous.
        if dt.starts_with("org.apache.cassandra.db.marshal.usertype(") {
            return true;
        }

        false
    }

    /// Parse the OUTERMOST `UserType(ks,hexname,hexfield:type,...)` marshal string
    /// into its DECLARED field order, returning `(field_name, field_marshal_type)`
    /// pairs (issue #927). The field type is kept as its raw marshal string so the
    /// per-field value bytes can be decoded with [`parse_value_from_raw_bytes`].
    ///
    /// Reuses the same paren-matching / hex-name decoding as
    /// [`parse_udt_type_definition`]; the two differ only in that this preserves
    /// the raw type string instead of converting it to a `CqlType`.
    pub(super) fn udt_field_marshal_types(type_str: &str) -> Result<Vec<(String, String)>> {
        let start_marker = "org.apache.cassandra.db.marshal.UserType(";
        let type_lower = type_str.to_lowercase();
        let start_marker_lower = start_marker.to_lowercase();
        let start_idx = type_lower
            .find(&start_marker_lower)
            .ok_or_else(|| Error::schema(format!("Not a UserType: {}", type_str)))?;

        let inner_start = start_idx + start_marker.len();
        let mut paren_depth = 1;
        let mut end_idx = inner_start;
        let chars: Vec<char> = type_str[inner_start..].chars().collect();
        for (i, c) in chars.iter().enumerate() {
            match c {
                '(' => paren_depth += 1,
                ')' => {
                    paren_depth -= 1;
                    if paren_depth == 0 {
                        end_idx = inner_start + i;
                        break;
                    }
                }
                _ => {}
            }
        }
        if paren_depth != 0 {
            return Err(Error::schema(format!(
                "Unbalanced parentheses in UserType: {}",
                type_str
            )));
        }

        let inner = &type_str[inner_start..end_idx];
        let parts = Self::split_type_args(inner)?;
        if parts.len() < 2 {
            return Err(Error::schema(format!(
                "UserType requires at least keyspace and name: {}",
                inner
            )));
        }

        let mut fields = Vec::with_capacity(parts.len().saturating_sub(2));
        for field_def in parts.iter().skip(2) {
            let field_def = field_def.trim();
            if field_def.is_empty() {
                continue;
            }
            let colon_idx = field_def.find(':').ok_or_else(|| {
                Error::schema(format!(
                    "Invalid UDT field definition (missing colon): {}",
                    field_def
                ))
            })?;
            let field_name = Self::decode_hex_name(&field_def[..colon_idx])?;
            let field_type = field_def[colon_idx + 1..].trim().to_string();
            fields.push((field_name, field_type));
        }
        Ok(fields)
    }

    /// Extract `(keyspace, type_name)` from the outermost `UserType(...)` marshal
    /// string (issue #927). The keyspace is the first arg; the name is the
    /// hex-decoded second arg.
    pub(super) fn udt_keyspace_and_name(type_str: &str) -> Result<(String, String)> {
        let udt_def = Self::parse_udt_type_definition(type_str)?;
        Ok((udt_def.keyspace, udt_def.name))
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::super::test_support::helpers::*;
    #[allow(unused_imports)]
    use super::*;

    #[test]
    fn test_udt_field_count_limit() {
        // Test parse_udt_type_definition with excessive fields
        // Build a UDT type string with MAX_UDT_FIELD_COUNT + 1 fields
        let mut field_defs = Vec::new();
        for i in 0..=MAX_UDT_FIELD_COUNT {
            let field_name_hex = hex::encode(format!("field_{}", i));
            field_defs.push(format!(
                "{}:org.apache.cassandra.db.marshal.Int32Type",
                field_name_hex
            ));
        }

        let type_str = format!(
            "org.apache.cassandra.db.marshal.UserType(test_ks,{},{})",
            hex::encode("test_udt"),
            field_defs.join(",")
        );

        // Parse the UDT definition (this will succeed - we only validate field count during value parsing)
        let udt_def = V5CompressedLegacyParser::parse_udt_type_definition(&type_str).unwrap();
        assert_eq!(udt_def.fields.len(), MAX_UDT_FIELD_COUNT + 1);

        // Create a parser
        let parser = V5CompressedLegacyParser::new(
            "test_ks".to_string(),
            "test_table".to_string(),
            0,
            0,
            None,
        );

        // When parsing a value with too many fields, it should fail validation
        // The validation check in parse_raw_type_value at line 4182 will catch this
        let data = vec![0u8; 4 * (MAX_UDT_FIELD_COUNT + 1)]; // Minimal data (all nulls)

        // Test through parse_raw_type_value which has the validation
        // Signature: parse_raw_type_value(data, offset, type_str, column_name, depth)
        let result = parser.parse_raw_type_value(&data, 0, &type_str, "test_col", 0);
        assert!(
            result.is_err(),
            "Should reject UDT with more than MAX_UDT_FIELD_COUNT fields"
        );
        assert!(
            result.unwrap_err().to_string().contains("exceeds maximum"),
            "Error should mention exceeding maximum"
        );
    }

    #[test]
    fn test_type_nesting_depth_limit() {
        // Build a deeply nested type string that exceeds MAX_TYPE_NESTING_DEPTH
        let mut type_str = "org.apache.cassandra.db.marshal.UTF8Type".to_string();

        // Wrap it in ListType MAX_TYPE_NESTING_DEPTH + 1 times
        for _ in 0..=MAX_TYPE_NESTING_DEPTH {
            type_str = format!("org.apache.cassandra.db.marshal.ListType({})", type_str);
        }

        // This should fail due to depth limit
        let result = V5CompressedLegacyParser::parse_cassandra_type_with_depth(&type_str, 0);
        assert!(
            result.is_err(),
            "Should reject type with nesting depth > MAX_TYPE_NESTING_DEPTH"
        );
        assert!(
            result.unwrap_err().to_string().contains("nesting depth"),
            "Error should mention nesting depth"
        );

        // Build a type string with exactly MAX_TYPE_NESTING_DEPTH levels
        let mut ok_type_str = "org.apache.cassandra.db.marshal.UTF8Type".to_string();
        for _ in 0..MAX_TYPE_NESTING_DEPTH {
            ok_type_str = format!("org.apache.cassandra.db.marshal.ListType({})", ok_type_str);
        }

        // This should succeed
        let result = V5CompressedLegacyParser::parse_cassandra_type_with_depth(&ok_type_str, 0);
        assert!(
            result.is_ok(),
            "Should accept type with nesting depth == MAX_TYPE_NESTING_DEPTH"
        );
    }

    #[test]
    fn test_nested_udt_depth_limit() {
        // Build a deeply nested UDT type string
        // Inner UDT: UserType(ks,hex(inner),field1:UTF8Type)
        let inner_udt = "org.apache.cassandra.db.marshal.UserType(ks,696e6e6572,666965746431:org.apache.cassandra.db.marshal.UTF8Type)";

        // Wrap it recursively
        let mut type_str = inner_udt.to_string();
        for i in 0..=MAX_TYPE_NESTING_DEPTH {
            let hex_name = hex::encode(format!("nested_{}", i));
            let hex_field = hex::encode("field");
            type_str = format!(
                "org.apache.cassandra.db.marshal.UserType(ks,{},{}:{})",
                hex_name, hex_field, type_str
            );
        }

        // This should fail due to depth limit
        let result = V5CompressedLegacyParser::parse_udt_type_definition_with_depth(&type_str, 0);
        assert!(
            result.is_err(),
            "Should reject UDT with nesting depth > MAX_TYPE_NESTING_DEPTH"
        );
        assert!(
            result.unwrap_err().to_string().contains("nesting depth"),
            "Error should mention nesting depth"
        );
    }

    /// Regression test for Issue #1080: a `frozen<udt>` column whose supplied
    /// schema short form (`frozen<person_type>`) carries NO field defs, with NO
    /// `UdtRegistry` wired, must decode STRUCTURALLY from the AUTHORITATIVE on-disk
    /// SerializationHeader marshal type (`FrozenType(UserType(...))`) — not drop to
    /// a blob or go MISSING.
    ///
    /// **Before the fix** the `frozen<` arm errored in exactly this configuration
    /// (no registry, no field defs), and the row-decode loop turned that `Err` into
    /// a `break`, silently dropping the failing column AND every trailing column.
    ///
    /// This test drives the exact fix path (`decode_frozen_udt_from_header_type`),
    /// asserts it yields `Value::Udt{...}` with the right fields, AND proves NO
    /// trailing-column loss: the returned offset lands exactly at the start of an
    /// appended trailing-column cell whose bytes are byte-for-byte intact (the
    /// frozen-UDT decode consumed exactly its own VInt-prefixed blob, no more).
    #[test]
    fn test_regression_1080_frozen_udt_decodes_from_header_type_no_registry() {
        // NO UdtRegistry wired; keyspace matches the header type's keyspace.
        let parser = V5CompressedLegacyParser::new(
            "test_types".to_string(),
            "cx_frozen_udt".to_string(),
            0,
            0,
            None,
        );

        // Supplied schema short form: `frozen<person_type>` — NO field defs.
        let column = crate::schema::Column {
            name: "p".to_string(),
            data_type: "frozen<person_type>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        };

        // AUTHORITATIVE on-disk SerializationHeader marshal type for this column:
        //   FrozenType(UserType(test_types, hex("person_type"),
        //                       hex("name"):UTF8Type, hex("age"):Int32Type))
        let hex = |s: &str| -> String { hex::encode(s.as_bytes()) };
        let header_type = format!(
            "org.apache.cassandra.db.marshal.FrozenType(\
             org.apache.cassandra.db.marshal.UserType(test_types,{},{}:org.apache.cassandra.db.marshal.UTF8Type,{}:org.apache.cassandra.db.marshal.Int32Type))",
            hex("person_type"),
            hex("name"),
            hex("age"),
        );
        assert!(
            V5CompressedLegacyParser::marshal_is_top_level_frozen_udt(&header_type),
            "header type must be recognized as a UserType"
        );

        // Build the serialized UDT blob: each field is [i32 BE length][bytes].
        //   name = "Ada" (3 bytes), age = 42 (i32, 4 bytes).
        let mut udt_blob: Vec<u8> = Vec::new();
        let name = b"Ada";
        udt_blob.extend_from_slice(&(name.len() as i32).to_be_bytes());
        udt_blob.extend_from_slice(name);
        udt_blob.extend_from_slice(&4i32.to_be_bytes());
        udt_blob.extend_from_slice(&42i32.to_be_bytes());

        // Cell layout at the decode entry point (after flags/timestamp, which the
        // caller already consumed): [blob_len:VUInt][udt_blob]. Then append a
        // sentinel for the FOLLOWING column to prove no trailing-column loss.
        assert!(udt_blob.len() < 0x80, "test assumes single-byte VUInt");
        let mut buf: Vec<u8> = vec![udt_blob.len() as u8];
        let blob_start = buf.len();
        buf.extend_from_slice(&udt_blob);
        // Trailing column's bytes — must remain addressable & untouched.
        let trailing_marker: &[u8] = &[0xDE, 0xAD, 0xBE, 0xEF];
        let trailing_offset = buf.len();
        buf.extend_from_slice(trailing_marker);

        let (udt_value, new_offset) = parser
            .decode_frozen_udt_from_header_type(&buf, 0, &header_type, &column)
            .expect("frozen<udt> must decode structurally from the on-disk header type");

        // 1) Structural UDT, NOT a blob.
        let udt = match &udt_value {
            Value::Udt(u) => u,
            other => panic!(
                "expected Value::Udt from header-type fallback, got {other:?} \
                 (regression #1080: frozen<udt> must not blob/miss)"
            ),
        };
        assert_eq!(udt.type_name, "person_type");
        let field_names: Vec<&str> = udt.fields.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(
            field_names,
            vec!["name", "age"],
            "UDT field ORDER preserved"
        );
        assert_eq!(
            udt.fields[0].value,
            Some(Value::text("Ada".to_string())),
            "name field decodes to Text"
        );
        assert_eq!(
            udt.fields[1].value,
            Some(Value::Integer(42)),
            "age field decodes to int"
        );

        // 2) NO trailing-column loss: the decode consumed exactly the VInt-prefixed
        //    blob (blob_len bytes after the length prefix), leaving the following
        //    column's bytes intact and addressable at `new_offset`.
        assert_eq!(
            new_offset,
            blob_start + udt_blob.len(),
            "frozen UDT decode must consume exactly its own blob"
        );
        assert_eq!(
            new_offset, trailing_offset,
            "offset must land at the START of the trailing column"
        );
        assert_eq!(
            &buf[new_offset..],
            trailing_marker,
            "the trailing column's bytes must be byte-for-byte intact \
             (proves the Err->break trailing-column loss is gone)"
        );
    }

    /// Issue #1080 / roborev jobs 1359/1361/1365: `marshal_is_top_level_frozen_udt`
    /// gates whether the scalar frozen-UDT decode fires. It must accept ONLY a
    /// top-level `FrozenType(UserType(...))` / `UserType(...)`, and REJECT frozen
    /// collections that merely contain a UDT (which must use the collection
    /// decoders), bare (unqualified) forms, and primitives.
    #[test]
    fn test_regression_1080_marshal_is_top_level_frozen_udt_predicate() {
        let q = "org.apache.cassandra.db.marshal";
        // Positive: top-level qualified UDT and FrozenType(UserType(...)).
        assert!(V5CompressedLegacyParser::marshal_is_top_level_frozen_udt(
            &format!("{q}.UserType(ks,abcd)")
        ));
        assert!(V5CompressedLegacyParser::marshal_is_top_level_frozen_udt(
            &format!("{q}.FrozenType({q}.UserType(ks,abcd))")
        ));
        // Positive: case-insensitive.
        assert!(V5CompressedLegacyParser::marshal_is_top_level_frozen_udt(
            "ORG.APACHE.CASSANDRA.DB.MARSHAL.USERTYPE(ks,abcd)"
        ));
        // Negative (roborev 1365): frozen COLLECTIONS containing a UDT must NOT
        // match — they must go through the collection decoders, not the scalar path.
        assert!(!V5CompressedLegacyParser::marshal_is_top_level_frozen_udt(
            &format!("{q}.FrozenType({q}.ListType({q}.UserType(ks,abcd)))")
        ));
        assert!(!V5CompressedLegacyParser::marshal_is_top_level_frozen_udt(
            &format!("{q}.FrozenType({q}.MapType({q}.UTF8Type,{q}.UserType(ks,abcd)))")
        ));
        assert!(!V5CompressedLegacyParser::marshal_is_top_level_frozen_udt(
            &format!("{q}.SetType({q}.UserType(ks,abcd))")
        ));
        // Negative: bare (unqualified) forms — real headers are always qualified.
        assert!(!V5CompressedLegacyParser::marshal_is_top_level_frozen_udt(
            "UserType(ks,abcd)"
        ));
        assert!(!V5CompressedLegacyParser::marshal_is_top_level_frozen_udt(
            "FrozenType(UserType(ks,abcd))"
        ));
        // Negative: primitives / non-UDT.
        assert!(!V5CompressedLegacyParser::marshal_is_top_level_frozen_udt(
            &format!("{q}.Int32Type")
        ));
        assert!(!V5CompressedLegacyParser::marshal_is_top_level_frozen_udt(
            "user"
        ));
        assert!(!V5CompressedLegacyParser::marshal_is_top_level_frozen_udt(
            ""
        ));
    }

    /// Issue #1080: `decode_frozen_udt_from_header_type` must return a clean
    /// `Error` (never panic / silently truncate) on a malformed blob — a length
    /// prefix that runs past the end of the available data.
    #[test]
    fn test_regression_1080_frozen_udt_truncated_blob_errors() {
        let parser = V5CompressedLegacyParser::new(
            "test_types".to_string(),
            "cx_frozen_udt".to_string(),
            0,
            0,
            None,
        );
        let column = crate::schema::Column {
            name: "p".to_string(),
            data_type: "frozen<person_type>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        };
        let hex = |s: &str| -> String { hex::encode(s.as_bytes()) };
        let header_type = format!(
            "org.apache.cassandra.db.marshal.FrozenType(\
             org.apache.cassandra.db.marshal.UserType(test_types,{},{}:org.apache.cassandra.db.marshal.UTF8Type))",
            hex("person_type"),
            hex("name"),
        );

        // VUInt length prefix claims 50 bytes but only 2 follow → must Err, not panic.
        let buf: Vec<u8> = vec![50u8, 0x00, 0x01];
        let res = parser.decode_frozen_udt_from_header_type(&buf, 0, &header_type, &column);
        assert!(
            res.is_err(),
            "truncated frozen-UDT blob must yield Err, not panic/silent-truncate"
        );
    }

    /// Issue #1080 / roborev jobs 1359+1361: predicate and decoder must agree on
    /// the SAME marshal form. We key everything on the fully-qualified
    /// `org.apache.cassandra.db.marshal.UserType(` marker (the only shape real
    /// SerializationHeaders carry, and the one the nested-field decoder resolves).
    /// This proves a NESTED qualified frozen UDT field decodes structurally — the
    /// exact case partial bare support would have blobbed.
    #[test]
    fn test_regression_1080_nested_qualified_frozen_udt_field_decodes() {
        let parser = V5CompressedLegacyParser::new(
            "test_types".to_string(),
            "cx_frozen_udt".to_string(),
            0,
            0,
            None,
        );
        let column = crate::schema::Column {
            name: "e".to_string(),
            data_type: "frozen<employee_type>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        };
        let hex = |s: &str| -> String { hex::encode(s.as_bytes()) };
        let q = "org.apache.cassandra.db.marshal";
        // Fully-qualified NESTED frozen UDT: employee { name text, home frozen<addr> }
        // where addr = { city text }. Exercises the nested-field decode path.
        let addr = format!(
            "{q}.FrozenType({q}.UserType(test_types,{},{}:{q}.UTF8Type))",
            hex("addr_type"),
            hex("city"),
        );
        let header_type = format!(
            "{q}.FrozenType({q}.UserType(test_types,{},{}:{q}.UTF8Type,{}:{addr}))",
            hex("employee_type"),
            hex("name"),
            hex("home"),
        );
        assert!(
            V5CompressedLegacyParser::marshal_is_top_level_frozen_udt(&header_type),
            "qualified nested frozen UDT header must be recognized"
        );

        // Blob: name="Bo" (2 bytes); home = frozen<addr> blob with city="NYC".
        let mut addr_blob: Vec<u8> = Vec::new();
        let city = b"NYC";
        addr_blob.extend_from_slice(&(city.len() as i32).to_be_bytes());
        addr_blob.extend_from_slice(city);

        let mut udt_blob: Vec<u8> = Vec::new();
        let name = b"Bo";
        udt_blob.extend_from_slice(&(name.len() as i32).to_be_bytes());
        udt_blob.extend_from_slice(name);
        udt_blob.extend_from_slice(&(addr_blob.len() as i32).to_be_bytes());
        udt_blob.extend_from_slice(&addr_blob);

        assert!(udt_blob.len() < 0x80);
        let mut buf: Vec<u8> = vec![udt_blob.len() as u8];
        buf.extend_from_slice(&udt_blob);

        let (value, _off) = parser
            .decode_frozen_udt_from_header_type(&buf, 0, &header_type, &column)
            .expect("nested qualified frozen UDT must decode structurally (not blob)");
        match value {
            Value::Udt(u) => {
                assert_eq!(u.type_name, "employee_type");
                assert_eq!(u.fields.len(), 2);
                // The NESTED `home` field must itself be a structured UDT, NOT a blob.
                let home = u
                    .fields
                    .iter()
                    .find(|f| f.name == "home")
                    .and_then(|f| f.value.as_ref())
                    .expect("home field present");
                let inner = match home {
                    Value::Frozen(b) => b.as_ref(),
                    other => other,
                };
                assert!(
                    matches!(inner, Value::Udt(_)),
                    "nested frozen UDT field must decode structurally, got {inner:?}"
                );
            }
            other => panic!("expected Value::Udt, got {other:?}"),
        }
    }

    // Issue #927: reader-side classification + declared-field parsing for
    // top-level non-frozen UDT complex columns.
    #[test]
    fn test_is_complex_column_udt() {
        // Top-level non-frozen UDT IS complex.
        assert!(V5CompressedLegacyParser::is_complex_column(
            "org.apache.cassandra.db.marshal.UserType(ks,61,62:org.apache.cassandra.db.marshal.UTF8Type)"
        ));
        // Frozen UDT is NOT complex.
        assert!(!V5CompressedLegacyParser::is_complex_column(
            "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.UserType(ks,61,62:org.apache.cassandra.db.marshal.UTF8Type))"
        ));
    }

    #[test]
    fn test_udt_field_marshal_types_declared_order() {
        let marshal = "org.apache.cassandra.db.marshal.UserType(\
            test_ks,706572736f6e,\
            6e616d65:org.apache.cassandra.db.marshal.UTF8Type,\
            616765:org.apache.cassandra.db.marshal.Int32Type)";
        let fields = V5CompressedLegacyParser::udt_field_marshal_types(marshal).unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].0, "name");
        assert_eq!(fields[0].1, "org.apache.cassandra.db.marshal.UTF8Type");
        assert_eq!(fields[1].0, "age");
        assert_eq!(fields[1].1, "org.apache.cassandra.db.marshal.Int32Type");

        let (ks, name) = V5CompressedLegacyParser::udt_keyspace_and_name(marshal).unwrap();
        assert_eq!(ks, "test_ks");
        assert_eq!(name, "person");
    }

    /// A `UserType(test_ks, wide_u, i int, vi varint)` marshal string, plus the
    /// bytes of a value whose `i` is 7 and whose `vi` is ZERO LENGTH.
    fn udt_with_an_empty_field() -> (String, Vec<u8>) {
        let type_str = format!(
            "org.apache.cassandra.db.marshal.UserType(test_ks,{},{}:org.apache.cassandra.db.marshal.Int32Type,{}:org.apache.cassandra.db.marshal.IntegerType)",
            hex::encode("wide_u"),
            hex::encode("i"),
            hex::encode("vi")
        );
        let mut data = 4i32.to_be_bytes().to_vec();
        data.extend_from_slice(&7i32.to_be_bytes());
        data.extend_from_slice(&0i32.to_be_bytes());
        (type_str, data)
    }

    fn assert_empty_varint_field_is_null(value: &Value, ctx: &str) {
        match value {
            Value::Udt(udt) => {
                assert_eq!(udt.fields[0].value, Some(Value::Integer(7)), "{ctx}: i");
                assert_eq!(
                    udt.fields[1].value,
                    Some(Value::Null),
                    "{ctx}: an EMPTY varint field is null (IntegerSerializer.java:33), \
                     and never an opaque Value::Blob (#3722 AC1)"
                );
            }
            other => panic!("{ctx}: expected a Udt, got {other:?}"),
        }
    }

    /// BLOCKER B (roborev, #3722): a field of length 0 bypassed THE decoder at
    /// BOTH UDT call sites, answering from a `create_empty_value_for_type`
    /// helper whose fallback arm was `Value::Blob`. These two cases pin the CALL
    /// SITES (the helper's own per-type semantics are pinned in
    /// `udt_field_empty`), because a total empty-value arm nothing routes to
    /// would leave the defect exactly where it was.
    #[test]
    fn empty_fields_route_through_the_decoder_at_both_udt_call_sites() {
        let parser = V5CompressedLegacyParser::new(
            "test_ks".to_string(),
            "test_table".to_string(),
            0,
            0,
            None,
        );
        let (type_str, data) = udt_with_an_empty_field();
        let udt_def = V5CompressedLegacyParser::parse_udt_type_definition(&type_str)
            .expect("UserType marshal string must parse");

        // Site 1 — `udt.rs::parse_udt_value` (the frozen-UDT column reader).
        let column = crate::schema::Column {
            name: "c".to_string(),
            data_type: "udt".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        };
        let (value, _) = parser
            .parse_udt_value(&data, 0, &udt_def, &column)
            .expect("UDT with an empty field must decode");
        assert_empty_varint_field_is_null(&value, "parse_udt_value");

        // Site 2 — `raw_type_value.rs`'s frozen-UDT arm.
        let (value, _) = parser
            .parse_raw_type_value(&data, 0, &type_str, "c", 0)
            .expect("UDT with an empty field must decode");
        assert_empty_varint_field_is_null(&value, "parse_raw_type_value");
    }

    /// `udt{f: udt{f: ...}}` nested `levels` deep, with the matching bytes:
    /// each level frames its single field as `[i32 BE len][bytes]`.
    fn nested_udt_type_and_bytes(levels: usize) -> (CqlType, Vec<u8>) {
        let mut ty = CqlType::Int;
        let mut data = 7i32.to_be_bytes().to_vec();
        for _ in 0..levels {
            ty = CqlType::Udt("u".to_string(), vec![("f".to_string(), ty)]);
            let mut framed = (data.len() as i32).to_be_bytes().to_vec();
            framed.extend_from_slice(&data);
            data = framed;
        }
        (ty, data)
    }

    /// BLOCKER A (roborev, #3722): the `CqlType::Udt` field arm re-entered the
    /// UDT reader at depth **0**, so a UDT nested inside a UDT restarted the
    /// nesting budget and the guard never fired however deep the nesting went.
    /// Past the bound this must ERROR, not recurse.
    #[test]
    fn nested_udt_field_recursion_is_depth_bounded() {
        let p = V5CompressedLegacyParser::new(
            "test_ks".to_string(),
            "test_table".to_string(),
            0,
            0,
            None,
        );
        // Well WITHIN the budget: decodes, so the error below is the BOUND
        // firing and not nested UDTs failing in general.
        let (ok_ty, ok_data) = nested_udt_type_and_bytes(MAX_TYPE_NESTING_DEPTH - 2);
        assert!(p.parse_udt_field_value(&ok_data, &ok_ty, 0).is_ok());

        let (ty, data) = nested_udt_type_and_bytes(MAX_TYPE_NESTING_DEPTH + 2);
        let err = V5CompressedLegacyParser::new(
            "test_ks".to_string(),
            "test_table".to_string(),
            0,
            0,
            None,
        )
        .parse_udt_field_value(&data, &ty, 0)
        .expect_err("nesting past the bound must error, not recurse");
        assert!(err.to_string().contains("depth"), "got: {err}");
    }

    /// The same arm built its `Value::Udt` with an EMPTY keyspace, so a UDT
    /// reached through a UDT field had a different public identity (`_keyspace`
    /// in the bindings; part of `Udt` equality and hashing, #3504) from the same
    /// UDT nested directly. It must carry the reader's keyspace.
    #[test]
    fn nested_udt_field_carries_the_real_keyspace() {
        let (ty, data) = nested_udt_type_and_bytes(1);
        let value = V5CompressedLegacyParser::new(
            "real_ks".to_string(),
            "test_table".to_string(),
            0,
            0,
            None,
        )
        .parse_udt_field_value(&data, &ty, 0)
        .expect("one level of nesting must decode");
        match value {
            Value::Udt(udt) => {
                assert_eq!(udt.keyspace, "real_ks", "nested UDT keyspace");
                assert_eq!(udt.type_name, "u");
                assert_eq!(udt.fields[0].value, Some(Value::Integer(7)));
            }
            other => panic!("expected a Udt, got {other:?}"),
        }
    }
}
