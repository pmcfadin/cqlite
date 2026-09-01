use super::*;

impl V5CompressedLegacyParser {
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
        // Issue #3722: every UDT-field decode in this function goes through the
        // one consolidated decoder (`row_decoder::udt_field`) at this call's own
        // `depth`. Two adapters, because the field-type sites and the
        // inner-of-frozen sites differ only in which type they pass and by one
        // nesting level; they keep the deeply-nested call sites on one line.
        let decode_udt_field =
            |data: &[u8], field_type: &CqlType| self.parse_udt_field_value(data, field_type, depth);
        let decode_udt_field_inner = |data: &[u8], field_type: &CqlType| {
            self.parse_udt_field_value(data, field_type, depth + 1)
        };

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
                std::str::from_utf8(text_bytes)
                    .map_err(|e| Error::corruption(format!("Invalid UTF-8 in text: {}", e)))?;
                offset += text_len;
                Value::Text(
                    crate::storage::sstable::reader::value_borrow::borrow_active(text_bytes),
                )
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
                std::str::from_utf8(text_bytes).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen element '{}': invalid UTF-8 in text value: {}",
                        column_name, e
                    ))
                })?;

                offset += text_len;
                Value::Text(
                    crate::storage::sstable::reader::value_borrow::borrow_active(text_bytes),
                )
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

                offset += duration_len;
                Value::Duration {
                    months,
                    days,
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

                let bytes = crate::storage::sstable::reader::value_borrow::borrow_active(
                    &data[offset..offset + len],
                );
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

                let blob_bytes = crate::storage::sstable::reader::value_borrow::borrow_active(
                    &data[offset..offset + blob_len],
                );
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

                let varint_bytes = crate::storage::sstable::reader::value_borrow::borrow_active(
                    &data[offset..offset + varint_len],
                );
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
                tracing::debug!(
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

                if tracing::enabled!(tracing::Level::DEBUG) {
                    let hex: String = udt_data
                        .iter()
                        .take(64)
                        .map(|b| format!("{:02x}", b))
                        .collect::<Vec<_>>()
                        .join(" ");
                    tracing::debug!(
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
                        tracing::debug!(
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
                    tracing::debug!(
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
                        tracing::debug!("Frozen UDT field '{}' is null", field_def.name);
                        None
                    } else if field_len == 0 {
                        // Empty field: through THE UDT-field decoder at length 0
                        // (issue #3722) — the helper this used to call answered
                        // most types with an opaque `Value::Blob`.
                        tracing::debug!("Frozen UDT field '{}' is empty", field_def.name);
                        Self::udt_field_value(self.parse_udt_field_value(
                            &[],
                            &field_def.field_type,
                            depth,
                        )?)
                    } else {
                        // Field with data. Routed through the shared guard (issue
                        // #3612, R3-F1/N1) so this loop cannot drift from the other
                        // three: it owns BOTH the negative rejection and the
                        // `checked_add` bounds test.
                        let field_len = Self::checked_component_len(
                            field_len,
                            &field_def.name,
                            current_offset,
                            udt_data.len(),
                        )?;

                        let field_data = &udt_data[current_offset..current_offset + field_len];
                        current_offset += field_len;

                        tracing::debug!(
                            "Frozen UDT field '{}' has {} bytes of data, type: {:?}",
                            field_def.name,
                            field_len,
                            field_def.field_type
                        );

                        // Parse field value - handle nested UDTs specially (Issue #238)
                        let value = if let Some(ref registry) = self.udt_registry {
                            // Issue #3722: one adapter so threading `depth` does not reflow 8
                            // deeply-nested call sites (this file is over the campsite target).
                            let decode_registry_udt =
                                |data: &[u8], def: &crate::types::UdtTypeDef| {
                                    self.parse_nested_udt_from_registry(
                                        data,
                                        def,
                                        registry,
                                        depth + 1,
                                    )
                                };
                            match &field_def.field_type {
                                CqlType::Custom(nested_type_name) => {
                                    // `get_udt_qualified` owns "udt:" + keyspace-
                                    // qualifier normalization (Issue #239 / #2807).
                                    if let Some(nested_udt) =
                                        registry.get_udt_qualified(&self.keyspace, nested_type_name)
                                    {
                                        decode_registry_udt(field_data, nested_udt)?
                                    } else {
                                        decode_udt_field(field_data, &field_def.field_type)?
                                    }
                                }
                                CqlType::Udt(udt_name, inline_fields) => {
                                    // Prefer registry, fall back to inline fields (Issue #239)
                                    if let Some(nested_udt) =
                                        registry.get_udt_qualified(&self.keyspace, udt_name)
                                    {
                                        decode_registry_udt(field_data, nested_udt)?
                                    } else if !inline_fields.is_empty() {
                                        self.parse_inline_udt_value(
                                            field_data,
                                            udt_name,
                                            inline_fields,
                                            depth + 1,
                                        )?
                                    } else {
                                        decode_udt_field(field_data, &field_def.field_type)?
                                    }
                                }
                                CqlType::Frozen(inner) => match inner.as_ref() {
                                    CqlType::Custom(nested_type_name) => {
                                        // `get_udt_qualified` owns "udt:" + keyspace-
                                        // qualifier normalization (Issue #239 / #2807).
                                        if let Some(nested_udt) = registry
                                            .get_udt_qualified(&self.keyspace, nested_type_name)
                                        {
                                            let inner_value =
                                                decode_registry_udt(field_data, nested_udt)?;
                                            Value::Frozen(Box::new(inner_value))
                                        } else {
                                            decode_udt_field(field_data, &field_def.field_type)?
                                        }
                                    }
                                    CqlType::Udt(udt_name, inline_fields) => {
                                        // Prefer registry, fall back to inline fields (Issue #239)
                                        if let Some(nested_udt) =
                                            registry.get_udt_qualified(&self.keyspace, udt_name)
                                        {
                                            let inner_value =
                                                decode_registry_udt(field_data, nested_udt)?;
                                            Value::Frozen(Box::new(inner_value))
                                        } else if !inline_fields.is_empty() {
                                            let inner_value = self.parse_inline_udt_value(
                                                field_data,
                                                udt_name,
                                                inline_fields,
                                                depth + 2,
                                            )?;
                                            Value::Frozen(Box::new(inner_value))
                                        } else {
                                            decode_udt_field(field_data, &field_def.field_type)?
                                        }
                                    }
                                    _ => decode_udt_field(field_data, &field_def.field_type)?,
                                },
                                _ => decode_udt_field(field_data, &field_def.field_type)?,
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
                                        depth + 2,
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
                                            depth + 2,
                                        )?;
                                        Value::Frozen(Box::new(inner_value))
                                    }
                                    _ => decode_udt_field(field_data, &field_def.field_type)?,
                                },
                                _ => decode_udt_field(field_data, &field_def.field_type)?,
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

                Value::Udt(Box::new(udt_value))
            }

            // Default: check if it's a short UDT name in the registry, otherwise treat as blob
            _ => {
                // Try to look up as UDT in registry by short name (Issue #238)
                // This handles cases like "address_type" which aren't in full marshal format
                if let Some(ref registry) = self.udt_registry {
                    // Issue #3722: one adapter so threading `depth` does not reflow 8
                    // deeply-nested call sites (this file is over the campsite target).
                    let decode_registry_udt = |data: &[u8], def: &crate::types::UdtTypeDef| {
                        self.parse_nested_udt_from_registry(data, def, registry, depth + 1)
                    };
                    if let Some(udt_def) = registry.get_udt_qualified(&self.keyspace, type_str) {
                        tracing::debug!(
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
                                tracing::debug!(
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
                                let value = decode_udt_field(&[], &field_def.field_type)?;
                                Some(value)
                            } else {
                                let field_len = Self::checked_component_len(
                                    field_len,
                                    &field_def.name,
                                    current_offset,
                                    udt_data.len(),
                                )?;

                                let field_data =
                                    &udt_data[current_offset..current_offset + field_len];
                                current_offset += field_len;

                                // Parse field value - handle nested UDTs specially (including FROZEN<udt>)
                                let value = match &field_def.field_type {
                                    CqlType::Custom(nested_type_name) => {
                                        // `get_udt_qualified` owns "udt:" + keyspace-
                                        // qualifier normalization (Issue #239 / #2807).
                                        if let Some(nested_udt) = registry
                                            .get_udt_qualified(&self.keyspace, nested_type_name)
                                        {
                                            // Recursively parse nested UDT
                                            decode_registry_udt(field_data, nested_udt)?
                                        } else {
                                            // Unknown custom type - parse as blob
                                            Value::Blob(crate::storage::sstable::reader::value_borrow::borrow_active(field_data))
                                        }
                                    }
                                    CqlType::Udt(udt_name, inline_fields) => {
                                        // Prefer registry, fall back to inline fields (Issue #239)
                                        if let Some(nested_udt) =
                                            registry.get_udt_qualified(&self.keyspace, udt_name)
                                        {
                                            decode_registry_udt(field_data, nested_udt)?
                                        } else if !inline_fields.is_empty() {
                                            self.parse_inline_udt_value(
                                                field_data,
                                                udt_name,
                                                inline_fields,
                                                depth + 1,
                                            )?
                                        } else {
                                            Value::Blob(crate::storage::sstable::reader::value_borrow::borrow_active(field_data))
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
                                                if let Some(nested_udt) = registry
                                                    .get_udt_qualified(&self.keyspace, lookup_name)
                                                {
                                                    let inner_value = self
                                                        .parse_nested_udt_from_registry(
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
                                                if let Some(nested_udt) = registry
                                                    .get_udt_qualified(&self.keyspace, udt_name)
                                                {
                                                    let inner_value = self
                                                        .parse_nested_udt_from_registry(
                                                            field_data,
                                                            nested_udt,
                                                            registry,
                                                            depth + 1,
                                                        )?;
                                                    Value::Frozen(Box::new(inner_value))
                                                } else if !inline_fields.is_empty() {
                                                    let inner_value = self.parse_inline_udt_value(
                                                        field_data,
                                                        udt_name,
                                                        inline_fields,
                                                        depth + 1,
                                                    )?;
                                                    Value::Frozen(Box::new(inner_value))
                                                } else {
                                                    Value::Frozen(Box::new(Value::Blob(crate::storage::sstable::reader::value_borrow::borrow_active(field_data))))
                                                }
                                            }
                                            _ => {
                                                // Other frozen types - parse as simple value
                                                let inner_value =
                                                    decode_udt_field_inner(field_data, inner)?;
                                                Value::Frozen(Box::new(inner_value))
                                            }
                                        }
                                    }
                                    _ => decode_udt_field(field_data, &field_def.field_type)?,
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
                        Value::Udt(Box::new(udt_value))
                    } else {
                        // Not found in registry - parse as blob
                        tracing::debug!(
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

                        let blob_bytes =
                            crate::storage::sstable::reader::value_borrow::borrow_active(
                                &data[offset..offset + blob_len],
                            );
                        offset += blob_len;
                        Value::Blob(blob_bytes)
                    }
                } else {
                    // No registry available - parse as blob
                    tracing::debug!(
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

                    let blob_bytes = crate::storage::sstable::reader::value_borrow::borrow_active(
                        &data[offset..offset + blob_len],
                    );
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
    use super::super::test_support::helpers::encode_unsigned;
    use super::*;
    use crate::parser::vint::encode_vuint;

    fn zigzag(v: i64) -> u64 {
        ((v << 1) ^ (v >> 63)) as u64
    }

    /// Build a raw-type-value duration body: `[VInt len][months][days][nanos]`.
    fn duration_bytes(months: i64, days: i64, nanos: i64) -> Vec<u8> {
        let mut body = Vec::new();
        encode_unsigned(zigzag(months), &mut body);
        encode_unsigned(zigzag(days), &mut body);
        encode_unsigned(zigzag(nanos), &mut body);
        let mut out = encode_vuint(body.len() as u64);
        out.extend_from_slice(&body);
        out
    }

    /// Sanity: an in-range duration still decodes through `parse_raw_type_value`.
    #[test]
    fn test_parse_raw_type_value_duration_in_range_ok() {
        let parser = V5CompressedLegacyParser::new("ks".to_string(), "tbl".to_string(), 0, 0, None);
        let data = duration_bytes(1, 2, 3);
        let (value, _off) = parser
            .parse_raw_type_value(&data, 0, "duration", "col", 0)
            .expect("in-range duration should decode");
        assert_eq!(
            value,
            Value::Duration {
                months: 1,
                days: 2,
                nanos: 3
            }
        );
    }

    /// Issue #1632 (item b): the frozen-element duration arm must REJECT a
    /// months/days VInt outside the i32 range instead of wrapping via `as i32`.
    #[test]
    fn test_parse_raw_type_value_duration_out_of_i32_range_errors() {
        let parser = V5CompressedLegacyParser::new("ks".to_string(), "tbl".to_string(), 0, 0, None);

        let over = duration_bytes(i32::MAX as i64 + 1, 0, 0);
        assert!(
            parser
                .parse_raw_type_value(&over, 0, "duration", "col", 0)
                .is_err(),
            "months > i32::MAX must error, not wrap via `as i32`"
        );

        let under = duration_bytes(0, i32::MIN as i64 - 1, 0);
        assert!(
            parser
                .parse_raw_type_value(&under, 0, "duration", "col", 0)
                .is_err(),
            "days < i32::MIN must error, not wrap via `as i32`"
        );
    }
}
