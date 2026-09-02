use super::*;

impl V5CompressedLegacyParser {
    /// Read i32 BE element/entry count from a frozen collection blob.
    ///
    /// `bound` is the exclusive upper byte index for the collection data (either
    /// `data.len()` for raw variants or `blob_end` for cell-level variants).
    fn read_frozen_count(
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

    /// Read a single length-prefixed element from a frozen collection blob.
    ///
    /// `blob_end` is the exclusive upper byte index bounding the collection.
    /// `element_desc` appears in error messages (e.g. `"list 'col' element 3"`).
    fn read_frozen_element(
        &self,
        data: &[u8],
        offset: &mut usize,
        blob_end: usize,
        type_str: &str,
        element_desc: &str,
        depth: usize,
    ) -> Result<Value> {
        if *offset + 4 > blob_end {
            return Err(Error::corruption(format!(
                "Frozen {}: not enough bytes for length",
                element_desc
            )));
        }
        let len_i32 = i32::from_be_bytes([
            data[*offset],
            data[*offset + 1],
            data[*offset + 2],
            data[*offset + 3],
        ]);
        if len_i32 < 0 {
            return Err(Error::corruption(format!(
                "Frozen {}: negative length {}",
                element_desc, len_i32
            )));
        }
        let len = len_i32 as usize;
        *offset += 4;

        if *offset + len > blob_end {
            return Err(Error::corruption(format!(
                "Frozen {}: needs {} bytes but only {} available",
                element_desc,
                len,
                blob_end - *offset
            )));
        }

        let elem_data = &data[*offset..*offset + len];
        let value = self.parse_value_from_raw_bytes(elem_data, type_str, element_desc, depth)?;
        *offset += len;
        Ok(value)
    }

    /// Parse a frozen list or set (cell-level, with VUInt blob_len prefix).
    ///
    /// The cell layout on disk is:
    ///   [VUInt blob_len][i32 BE element_count][i32 BE elem_len][elem_bytes]...
    ///
    /// `as_set = true` wraps the result in `Value::Set`; otherwise `Value::List`.
    fn parse_frozen_sequence_value(
        &self,
        data: &[u8],
        mut offset: usize,
        element_type: &str,
        column: &crate::schema::Column,
        as_set: bool,
    ) -> Result<(Value, usize)> {
        let kind = if as_set { "set" } else { "list" };
        let (count, blob_end) = Self::read_frozen_preamble(data, &mut offset, kind, &column.name)?;

        tracing::debug!(
            "V5CompressedLegacy: Frozen {} '{}' with {} elements, element_type='{}'",
            kind,
            column.name,
            count,
            element_type
        );

        let mut elements = Vec::with_capacity(count);
        for i in 0..count {
            let desc = format!("{} '{}' element {}", kind, column.name, i);
            let value =
                self.read_frozen_element(data, &mut offset, blob_end, element_type, &desc, 0)?;
            tracing::debug!(
                "V5CompressedLegacy: Frozen {} element {}: {:?}",
                kind,
                i,
                value
            );
            elements.push(value);
        }

        if as_set {
            Ok((Value::Set(elements), blob_end))
        } else {
            Ok((Value::List(elements), blob_end))
        }
    }

    /// Parse frozen list value (thin wrapper around `parse_frozen_sequence_value`).
    pub(super) fn parse_frozen_list_value(
        &self,
        data: &[u8],
        offset: usize,
        element_type: &str,
        column: &crate::schema::Column,
        _reader: &crate::storage::sstable::reader::types::SSTableReader,
    ) -> Result<(Value, usize)> {
        self.parse_frozen_sequence_value(data, offset, element_type, column, false)
    }

    /// Parse frozen set value (thin wrapper around `parse_frozen_sequence_value`).
    ///
    /// Frozen sets have the same binary format as frozen lists; the distinction
    /// is semantic (sets are sorted/unique at the CQL level).
    pub(super) fn parse_frozen_set_value(
        &self,
        data: &[u8],
        offset: usize,
        element_type: &str,
        column: &crate::schema::Column,
        _reader: &crate::storage::sstable::reader::types::SSTableReader,
    ) -> Result<(Value, usize)> {
        self.parse_frozen_sequence_value(data, offset, element_type, column, true)
    }

    /// Parse frozen map value.
    ///
    /// The cell layout on disk is:
    ///   [VUInt blob_len][i32 BE entry_count][i32 BE key_len][key_bytes][i32 BE val_len][val_bytes]...
    pub(super) fn parse_frozen_map_value(
        &self,
        data: &[u8],
        mut offset: usize,
        key_type: &str,
        value_type: &str,
        column: &crate::schema::Column,
        _reader: &crate::storage::sstable::reader::types::SSTableReader,
    ) -> Result<(Value, usize)> {
        let (count, blob_end) = Self::read_frozen_preamble(data, &mut offset, "map", &column.name)?;

        tracing::debug!(
            "V5CompressedLegacy: Frozen map '{}' with {} entries, key_type='{}', value_type='{}'",
            column.name,
            count,
            key_type,
            value_type
        );

        let mut entries = Vec::with_capacity(count);
        for i in 0..count {
            let key_desc = format!("map '{}' key {}", column.name, i);
            let key_value =
                self.read_frozen_element(data, &mut offset, blob_end, key_type, &key_desc, 0)?;

            let val_desc = format!("map '{}' value {}", column.name, i);
            let val_value =
                self.read_frozen_element(data, &mut offset, blob_end, value_type, &val_desc, 0)?;

            tracing::debug!(
                "V5CompressedLegacy: Frozen map entry {}: {:?} -> {:?}",
                i,
                key_value,
                val_value
            );
            entries.push((key_value, val_value));
        }

        Ok((Value::Map(entries), blob_end))
    }

    /// Parse a frozen list or set (raw, nested inside an already-bounded blob).
    ///
    /// Called when parsing nested collections inside an already-bounded frozen
    /// blob.  There is NO VUInt cell-value-length prefix — the caller has
    /// already bounded the data slice.  `as_set = true` produces `Value::Set`.
    fn parse_frozen_sequence_value_raw(
        &self,
        data: &[u8],
        mut offset: usize,
        element_type: &str,
        column_name: &str,
        as_set: bool,
        depth: usize,
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
            let elem_value =
                self.parse_value_from_raw_bytes(elem_data, element_type, column_name, depth)?;
            elements.push(elem_value);
            offset += elem_len;
        }

        if as_set {
            Ok((Value::Set(elements), offset))
        } else {
            Ok((Value::List(elements), offset))
        }
    }

    /// Parse frozen list value (raw version without Column parameter).
    pub(super) fn parse_frozen_list_value_raw(
        &self,
        data: &[u8],
        offset: usize,
        element_type: &str,
        column_name: &str,
        depth: usize,
    ) -> Result<(Value, usize)> {
        self.parse_frozen_sequence_value_raw(data, offset, element_type, column_name, false, depth)
    }

    /// Parse frozen set value (raw version without Column parameter).
    pub(super) fn parse_frozen_set_value_raw(
        &self,
        data: &[u8],
        offset: usize,
        element_type: &str,
        column_name: &str,
        depth: usize,
    ) -> Result<(Value, usize)> {
        self.parse_frozen_sequence_value_raw(data, offset, element_type, column_name, true, depth)
    }

    /// Parse frozen map value (raw version without Column parameter).
    pub(super) fn parse_frozen_map_value_raw(
        &self,
        data: &[u8],
        mut offset: usize,
        key_type: &str,
        value_type: &str,
        column_name: &str,
        depth: usize,
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
            let key_value =
                self.parse_value_from_raw_bytes(key_data, key_type, column_name, depth)?;
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
            let val_value =
                self.parse_value_from_raw_bytes(val_data, value_type, column_name, depth)?;
            offset += val_len;

            entries.push((key_value, val_value));
        }

        Ok((Value::Map(entries), offset))
    }

    /// Parse tuple value from binary data at the cell level.
    ///
    /// Cell-level layout (written by `write_cell`):
    /// ```text
    /// [VUInt blob_len]
    /// for each element (schema-ordered, from type string):
    ///   [i32 BE element_len]  (-1 = null, 0 = empty, >0 = byte count)
    ///   [element_len bytes]   (only present when element_len > 0)
    /// ```
    ///
    /// Element count and types are derived exclusively from the schema type string
    /// (no-heuristics mandate, Issue #28).
    pub(super) fn parse_tuple_value(
        &self,
        data: &[u8],
        offset: &mut usize,
        type_str: &str,
        column: &crate::schema::Column,
        _reader: &crate::storage::sstable::reader::types::SSTableReader,
    ) -> Result<Value> {
        // Extract element types from schema (schema-aware, no heuristics)
        let element_types = self.extract_tuple_element_types(type_str)?;

        if element_types.is_empty() {
            return Err(Error::schema(format!("Empty tuple type: {}", type_str)));
        }

        // Read the VUInt outer blob length to bound the tuple bytes
        let (remaining, blob_len_raw) = parse_vuint(&data[*offset..]).map_err(|e| {
            Error::corruption(format!(
                "Tuple '{}': failed to parse outer blob length as VUInt: {:?}",
                column.name, e
            ))
        })?;
        if blob_len_raw > MAX_CELL_VALUE_LENGTH {
            return Err(Error::corruption(format!(
                "Tuple '{}': blob_len {} exceeds maximum {}",
                column.name, blob_len_raw, MAX_CELL_VALUE_LENGTH
            )));
        }
        let blob_len = blob_len_raw as usize;
        let len_bytes_consumed = data[*offset..].len() - remaining.len();
        *offset += len_bytes_consumed;

        if *offset + blob_len > data.len() {
            return Err(Error::corruption(format!(
                "Tuple '{}': blob_len {} exceeds available data {}",
                column.name,
                blob_len,
                data.len() - *offset
            )));
        }

        let blob_end = *offset + blob_len;

        // Parse each element using the schema-derived element type and the
        // [i32 BE len][bytes] wire format (same as UDT fields and frozen
        // collection elements — see type-mapping-complex.md).
        let elements =
            self.parse_tuple_elements_raw(data, offset, blob_end, &element_types, &column.name, 0)?;

        // Advance offset to end of blob regardless of how many elements were consumed
        // (protects against trailing bytes / schema drift).
        *offset = blob_end;

        Ok(Value::Tuple(elements))
    }

    /// Parse tuple elements from an already-bounded raw byte slice.
    ///
    /// Each element is encoded as `[i32 BE len][bytes]` with -1 meaning null.
    /// Element types are taken from `element_types` in order (schema-aware).
    ///
    /// `blob_end` is the exclusive upper byte index bounding the tuple data.
    pub(super) fn parse_tuple_elements_raw(
        &self,
        data: &[u8],
        offset: &mut usize,
        blob_end: usize,
        element_types: &[String],
        column_name: &str,
        depth: usize,
    ) -> Result<Vec<Value>> {
        let mut elements = Vec::with_capacity(element_types.len());

        for (idx, elem_type) in element_types.iter().enumerate() {
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
            let value =
                self.parse_value_from_raw_bytes(elem_data, elem_type, &elem_desc, depth + 1)?;
            *offset += elem_len;

            elements.push(value);
        }

        Ok(elements)
    }

    /// Extract tuple element types from a `tuple<T1, T2, ...>` (CQL short) or
    /// `org.apache.cassandra.db.marshal.TupleType(T1, T2, ...)` (Cassandra marshal)
    /// type string. Marshal support (issue #1081) is required so a tuple-typed
    /// multicell-UDT field, whose type arrives in marshal form, decodes its
    /// elements instead of falling through to the blob default.
    pub(super) fn extract_tuple_element_types(&self, type_str: &str) -> Result<Vec<String>> {
        let type_lower = type_str.to_lowercase();

        // Determine the inner element-list content based on format. Slice from the
        // ORIGINAL-CASE `type_str` so nested element marshal types keep their case.
        let inner = if type_lower.starts_with("org.apache.cassandra.db.marshal.tupletype(")
            && type_str.ends_with(')')
        {
            let prefix = "org.apache.cassandra.db.marshal.TupleType(";
            &type_str[prefix.len()..type_str.len() - 1]
        } else if type_lower.starts_with("tuple<") && type_str.ends_with('>') {
            &type_str[6..type_str.len() - 1]
        } else {
            return Err(Error::schema(format!(
                "Invalid tuple type format: {}",
                type_str
            )));
        };

        if inner.is_empty() {
            return Ok(Vec::new());
        }

        // Split by top-level comma, handling both CQL angle brackets (`<`/`>`)
        // and marshal parentheses (`(`/`)`) so nested composite element types
        // (e.g. `ListType(UTF8Type)`) are not split internally.
        let mut types = Vec::new();
        let mut current = String::new();
        let mut depth = 0i32;

        for ch in inner.chars() {
            match ch {
                '<' | '(' => {
                    depth += 1;
                    current.push(ch);
                }
                '>' | ')' => {
                    if depth == 0 {
                        return Err(Error::schema(format!(
                            "Unmatched '{}' in tuple type: {}",
                            ch, type_str
                        )));
                    }
                    depth -= 1;
                    current.push(ch);
                }
                ',' if depth == 0 => {
                    types.push(current.trim().to_string());
                    current.clear();
                }
                _ => {
                    current.push(ch);
                }
            }
        }

        if !current.is_empty() {
            types.push(current.trim().to_string());
        }

        Ok(types)
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::super::test_support::helpers::*;
    #[allow(unused_imports)]
    use super::*;

    #[test]
    fn test_extract_frozen_inner_type() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        // Test basic frozen type
        assert_eq!(
            parser
                .extract_frozen_inner_type("frozen<list<int>>")
                .unwrap(),
            "list<int>"
        );

        // Test nested frozen
        assert_eq!(
            parser
                .extract_frozen_inner_type("frozen<map<text,frozen<set<int>>>>")
                .unwrap(),
            "map<text,frozen<set<int>>>"
        );

        // Test error cases
        assert!(parser.extract_frozen_inner_type("frozen<>").is_err());
        assert!(parser.extract_frozen_inner_type("frozen").is_err());
        assert!(parser.extract_frozen_inner_type("list<int>").is_err());
    }

    #[test]
    fn test_extract_tuple_element_types() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        // Test simple tuple
        let types = parser
            .extract_tuple_element_types("tuple<int,text,bigint>")
            .unwrap();
        assert_eq!(types, vec!["int", "text", "bigint"]);

        // Test tuple with nested collections
        let types = parser
            .extract_tuple_element_types("tuple<int,list<text>,map<text,int>>")
            .unwrap();
        assert_eq!(types, vec!["int", "list<text>", "map<text,int>"]);

        // Test tuple with frozen
        let types = parser
            .extract_tuple_element_types("tuple<int,frozen<list<int>>>")
            .unwrap();
        assert_eq!(types, vec!["int", "frozen<list<int>>"]);

        // Test empty tuple
        let types = parser.extract_tuple_element_types("tuple<>").unwrap();
        assert!(types.is_empty());

        // Test error cases
        assert!(parser.extract_tuple_element_types("tuple").is_err());
        assert!(parser.extract_tuple_element_types("int").is_err());
    }

    #[test]
    fn test_extract_tuple_element_types_unmatched_angle_bracket() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        // Unmatched '>' inside inner content must return Err, not panic.
        // "tuple<int>>" — the outer '>' is consumed by starts_with/ends_with stripping,
        // leaving "int>" as the inner string; the extra '>' hits depth == 0 and must error.
        let result = parser.extract_tuple_element_types("tuple<int>>");
        assert!(
            result.is_err(),
            "Expected Err for unmatched '>' but got: {:?}",
            result
        );
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("Unmatched '>'"),
            "Error message should mention unmatched '>': {}",
            err_msg
        );

        // A second variant: extra '>' after a nested type.
        let result2 = parser.extract_tuple_element_types("tuple<list<int>>>");
        assert!(
            result2.is_err(),
            "Expected Err for extra '>' but got: {:?}",
            result2
        );
    }

    /// Issue #1081: `extract_frozen_inner_type` must accept BOTH the CQL short form
    /// (`frozen<list<int>>`) and the authoritative marshal form
    /// (`FrozenType(ListType(Int32Type))`), returning the inner type with its
    /// ORIGINAL case preserved so nested marshal types re-normalize on recursion.
    #[test]
    fn test_extract_frozen_inner_type_cql_and_marshal() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        // CQL short form
        let cql_inner = parser
            .extract_frozen_inner_type("frozen<list<int>>")
            .expect("CQL frozen<...> must parse");
        assert_eq!(cql_inner, "list<int>");

        // Marshal form — original case (Int32Type) must be preserved.
        let marshal_inner = parser
            .extract_frozen_inner_type(
                "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type))",
            )
            .expect("marshal FrozenType(...) must parse");
        assert_eq!(
            marshal_inner,
            "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)",
            "marshal frozen inner type must be original-case (Int32Type preserved)"
        );

        // Mixed-case CQL spelling must also parse (the prefix/suffix check is
        // case-insensitive) while the sliced inner keeps its original case.
        let mixed_inner = parser
            .extract_frozen_inner_type("Frozen<List<Int>>")
            .expect("mixed-case Frozen<...> must parse");
        assert_eq!(mixed_inner, "List<Int>");
    }

    #[test]
    fn test_frozen_sequence_value_raw_list() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        let data = build_frozen_list_int(&[10, 20]);
        let (val, end_offset) = parser
            .parse_frozen_list_value_raw(&data, 0, "int", "col", 0)
            .unwrap();
        assert_eq!(
            val,
            Value::List(vec![Value::Integer(10), Value::Integer(20)])
        );
        assert_eq!(end_offset, data.len());
    }

    #[test]
    fn test_frozen_sequence_value_raw_set() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        let data = build_frozen_list_int(&[5, 15]);
        let (val, _) = parser
            .parse_frozen_set_value_raw(&data, 0, "int", "col", 0)
            .unwrap();
        assert_eq!(val, Value::Set(vec![Value::Integer(5), Value::Integer(15)]));
    }

    #[test]
    fn test_frozen_sequence_value_raw_empty() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        let data = 0i32.to_be_bytes().to_vec(); // count = 0
        let (val, _) = parser
            .parse_frozen_list_value_raw(&data, 0, "int", "col", 0)
            .unwrap();
        assert_eq!(val, Value::List(vec![]));

        let (val, _) = parser
            .parse_frozen_set_value_raw(&data, 0, "int", "col", 0)
            .unwrap();
        assert_eq!(val, Value::Set(vec![]));
    }

    #[test]
    fn test_frozen_map_value_raw() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        let data = build_frozen_map_text_int(&[("x", 42)]);
        let (val, end_offset) = parser
            .parse_frozen_map_value_raw(&data, 0, "text", "int", "col", 0)
            .unwrap();
        assert_eq!(
            val,
            Value::Map(vec![(Value::text("x".to_string()), Value::Integer(42))])
        );
        assert_eq!(end_offset, data.len());
    }

    #[test]
    fn test_frozen_parse_error_truncated_data() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        // Truncated: claims 2 elements but only has space for count header
        let data = 2i32.to_be_bytes().to_vec();
        let result = parser.parse_frozen_list_value_raw(&data, 0, "int", "col", 0);
        assert!(result.is_err());

        // Negative element length
        let mut data = Vec::new();
        data.extend_from_slice(&1i32.to_be_bytes()); // count = 1
        data.extend_from_slice(&(-1i32).to_be_bytes()); // elem_len = -1
        let result = parser.parse_frozen_list_value_raw(&data, 0, "int", "col", 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_frozen_recursion_depth_exceeded() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        // Build a type string with 12 levels of nesting (exceeds MAX_TYPE_NESTING_DEPTH=10)
        let mut type_str = "int".to_string();
        for _ in 0..12 {
            type_str = format!("frozen<{}>", type_str);
        }

        let data = 42i32.to_be_bytes();
        let result = parser.parse_value_from_raw_bytes(&data, &type_str, "col", 0);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("recursion depth"),
            "Error should mention recursion depth: {}",
            err_msg
        );
    }

    #[test]
    fn test_tuple_int_text_parsing() {
        // Test parse_tuple_elements_raw with constructed binary data.
        //
        // Wire format for each tuple element: [i32 BE elem_len][elem_bytes]
        // Null element: [i32 BE -1] (no following bytes)
        //
        // Tuple: (int=42, text="hi")
        //   element 0: [0x00, 0x00, 0x00, 0x04][42 as i32 BE] -> [0,0,0,4][0,0,0,42]
        //   element 1: [0x00, 0x00, 0x00, 0x02]["hi"] -> [0,0,0,2][0x68,0x69]
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        let mut data = Vec::new();
        // element 0: int 42
        data.extend_from_slice(&4i32.to_be_bytes()); // length
        data.extend_from_slice(&42i32.to_be_bytes()); // value
                                                      // element 1: text "hi"
        let hi = b"hi";
        data.extend_from_slice(&(hi.len() as i32).to_be_bytes()); // length
        data.extend_from_slice(hi); // value

        let element_types = vec!["int".to_string(), "text".to_string()];
        let mut offset = 0usize;
        let blob_end = data.len();
        let elements = parser
            .parse_tuple_elements_raw(&data, &mut offset, blob_end, &element_types, "col", 0)
            .unwrap();

        assert_eq!(elements.len(), 2);
        assert_eq!(elements[0], Value::Integer(42));
        assert_eq!(elements[1], Value::text("hi".to_string()));
        assert_eq!(
            offset, blob_end,
            "offset should reach blob_end after parsing all elements"
        );

        // Also test null element: (int=null, text="ok")
        let mut data2 = Vec::new();
        data2.extend_from_slice(&(-1i32).to_be_bytes()); // null element 0
        let ok = b"ok";
        data2.extend_from_slice(&(ok.len() as i32).to_be_bytes());
        data2.extend_from_slice(ok);

        let mut offset2 = 0usize;
        let blob_end2 = data2.len();
        let elements2 = parser
            .parse_tuple_elements_raw(&data2, &mut offset2, blob_end2, &element_types, "col", 0)
            .unwrap();

        assert_eq!(elements2.len(), 2);
        assert_eq!(elements2[0], Value::Null);
        assert_eq!(elements2[1], Value::text("ok".to_string()));
    }

    #[test]
    fn test_frozen_list_int_parsing() {
        // Test type extraction for frozen<list<int>>
        // Note: Full parsing tests require a reader, done via integration tests.
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        // Verify element type extraction works
        let inner_type = parser
            .extract_frozen_inner_type("frozen<list<int>>")
            .unwrap();
        assert_eq!(inner_type, "list<int>");

        let element_type = parser
            .extract_collection_element_type(&inner_type, "list")
            .unwrap();
        assert_eq!(element_type, "int");
    }

    #[test]
    fn test_frozen_set_text_parsing() {
        // Test type extraction for frozen<set<text>>
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        let inner_type = parser
            .extract_frozen_inner_type("frozen<set<text>>")
            .unwrap();
        assert_eq!(inner_type, "set<text>");

        let element_type = parser
            .extract_collection_element_type(&inner_type, "set")
            .unwrap();
        assert_eq!(element_type, "text");
    }

    #[test]
    fn test_frozen_map_text_text_parsing() {
        // Test type extraction for frozen<map<text,text>>
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        let inner_type = parser
            .extract_frozen_inner_type("frozen<map<text,text>>")
            .unwrap();
        assert_eq!(inner_type, "map<text,text>");

        let (key_type, value_type) = parser.extract_map_types(&inner_type).unwrap();
        assert_eq!(key_type, "text");
        assert_eq!(value_type, "text");
    }

    #[test]
    fn test_nested_frozen_map_parsing() {
        // Test type extraction for nested frozen: frozen<map<text, frozen<set<uuid>>>>
        // This is the structure used in chat_messages.reactions
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        let inner_type = parser
            .extract_frozen_inner_type("frozen<map<text,frozen<set<uuid>>>>")
            .unwrap();
        assert_eq!(inner_type, "map<text,frozen<set<uuid>>>");

        let (key_type, value_type) = parser.extract_map_types(&inner_type).unwrap();
        assert_eq!(key_type, "text");
        assert_eq!(value_type, "frozen<set<uuid>>");

        // Further extraction of the nested frozen type
        let inner_set = parser.extract_frozen_inner_type(&value_type).unwrap();
        assert_eq!(inner_set, "set<uuid>");

        let element_type = parser
            .extract_collection_element_type(&inner_set, "set")
            .unwrap();
        assert_eq!(element_type, "uuid");
    }
}
