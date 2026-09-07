//! Value parsing using schema-driven approach
//!
//! This module handles parsing of column values using exact schema types,
//! including collection types (list, set, map), tuples, and UDTs.

use crate::{
    types::{ComparatorType, TableId, UdtField, UdtValue},
    Error, Result, RowKey, Value,
};

use super::super::types::SSTableReader;
use super::comparator_value_parsing::{
    parse_value_with_comparator as decode_scalar_comparator, MAX_VALUE_NESTING_DEPTH,
};

/// Upper bound on collection capacity pre-allocated from a declared element/entry
/// count. A corrupt or adversarial huge count (e.g. `2^30`) must not let us
/// pre-allocate gigabytes up front; we reserve at most this many slots and grow
/// on demand as real elements are decoded (issue #1632). Guard-only: the decoded
/// value is unchanged, only the initial allocation is bounded.
const REASONABLE_COLLECTION_CAPACITY: usize = 4096;

// ============================================================================
// Scalar decode shims (issue #1636 / J2)
//
// Test-only thin delegations to the ONE scalar decode body in
// `comparator_value_parsing::parse_value_with_comparator` (the single owner of
// per-type scalar decoding). Production code routes scalars there directly (the
// `_ => decode_scalar_comparator(..)` arms below), so these carry NO decode logic
// and exist only as the `parse_*_value` unit-test surface — hence `#[cfg(test)]`.
// ============================================================================

/// Decode a scalar value through the single scalar decode body (test surface).
#[cfg(test)]
pub(crate) fn parse_boolean_value(data: &[u8]) -> Result<Value> {
    decode_scalar_comparator(data, &ComparatorType::Boolean)
}

/// Decode a scalar value through the single scalar decode body (test surface).
#[cfg(test)]
pub(crate) fn parse_tinyint_value(data: &[u8]) -> Result<Value> {
    decode_scalar_comparator(data, &ComparatorType::TinyInt)
}

/// Decode a scalar value through the single scalar decode body (test surface).
#[cfg(test)]
pub(crate) fn parse_smallint_value(data: &[u8]) -> Result<Value> {
    decode_scalar_comparator(data, &ComparatorType::SmallInt)
}

/// Decode a scalar value through the single scalar decode body (test surface).
#[cfg(test)]
pub(crate) fn parse_int_value(data: &[u8]) -> Result<Value> {
    decode_scalar_comparator(data, &ComparatorType::Int)
}

/// Decode a scalar value through the single scalar decode body (test surface).
#[cfg(test)]
pub(crate) fn parse_bigint_value(data: &[u8]) -> Result<Value> {
    decode_scalar_comparator(data, &ComparatorType::BigInt)
}

/// Decode a scalar value through the single scalar decode body (test surface).
#[cfg(test)]
pub(crate) fn parse_counter_value(data: &[u8]) -> Result<Value> {
    decode_scalar_comparator(data, &ComparatorType::Counter)
}

/// Decode a scalar value through the single scalar decode body (test surface).
#[cfg(test)]
pub(crate) fn parse_text_value(data: &[u8]) -> Result<Value> {
    decode_scalar_comparator(data, &ComparatorType::Text)
}

/// Decode a scalar value through the single scalar decode body (test surface).
#[cfg(test)]
pub(crate) fn parse_blob_value(data: &[u8]) -> Result<Value> {
    decode_scalar_comparator(data, &ComparatorType::Blob)
}

/// Decode a scalar value through the single scalar decode body (test surface).
#[cfg(test)]
pub(crate) fn parse_uuid_value(data: &[u8]) -> Result<Value> {
    decode_scalar_comparator(data, &ComparatorType::Uuid)
}

/// Decode a scalar value through the single scalar decode body (test surface).
#[cfg(test)]
pub(crate) fn parse_date_value(data: &[u8]) -> Result<Value> {
    decode_scalar_comparator(data, &ComparatorType::Date)
}

/// Parse list value with recursive element parsing via closure
///
/// This allows testing the list parsing logic independently of SSTableReader.
pub(crate) fn parse_list_value_with<F>(
    data: &[u8],
    element_comparator: &ComparatorType,
    parse_element: F,
) -> Result<Value>
where
    F: Fn(&[u8], &ComparatorType) -> Result<Value>,
{
    use crate::parser::vint::parse_vint_length;

    let mut offset = 0;

    // Parse element count
    let (remaining, element_count) = parse_vint_length(&data[offset..])
        .map_err(|_| Error::corruption("Failed to parse list element count"))?;
    offset = data.len() - remaining.len();

    // Clamp pre-allocation: a corrupt huge count must not pre-allocate GBs (#1632).
    let mut elements = Vec::with_capacity(element_count.min(REASONABLE_COLLECTION_CAPACITY));

    // Decode EXACTLY `element_count` elements. A valid collection cell holds
    // exactly `count` fully-encoded elements, so a buffer that runs dry before
    // `count` elements are decoded is corrupt/truncated and must Err — silently
    // returning a short partial list would accept a truncated cell (#1632).
    for _ in 0..element_count {
        if offset >= data.len() {
            return Err(Error::corruption(
                "List declared more elements than present in buffer (truncated)",
            ));
        }

        // Parse element length
        let (remaining, element_len) = parse_vint_length(&data[offset..])
            .map_err(|_| Error::corruption("Failed to parse list element length"))?;
        offset = data.len() - remaining.len();

        if element_len > remaining.len() {
            return Err(Error::corruption(
                "List element length exceeds available data",
            ));
        }

        // Parse element value using provided closure
        let element_data = &remaining[..element_len];
        let element_value = parse_element(element_data, element_comparator)?;
        elements.push(element_value);
        offset += element_len;
    }

    Ok(Value::List(elements))
}

/// Parse set value with recursive element parsing via closure
pub(crate) fn parse_set_value_with<F>(
    data: &[u8],
    element_comparator: &ComparatorType,
    parse_element: F,
) -> Result<Value>
where
    F: Fn(&[u8], &ComparatorType) -> Result<Value>,
{
    // Sets are parsed similarly to lists
    let list_value = parse_list_value_with(data, element_comparator, parse_element)?;
    if let Value::List(elements) = list_value {
        Ok(Value::Set(elements))
    } else {
        Err(Error::corruption("Failed to parse set value"))
    }
}

/// Parse map value with recursive key/value parsing via closure
pub(crate) fn parse_map_value_with<F>(
    data: &[u8],
    key_comparator: &ComparatorType,
    value_comparator: &ComparatorType,
    parse_element: F,
) -> Result<Value>
where
    F: Fn(&[u8], &ComparatorType) -> Result<Value>,
{
    use crate::parser::vint::parse_vint_length;

    let mut offset = 0;

    // Parse entry count
    let (remaining, entry_count) = parse_vint_length(&data[offset..])
        .map_err(|_| Error::corruption("Failed to parse map entry count"))?;
    offset = data.len() - remaining.len();

    // Clamp pre-allocation: a corrupt huge count must not pre-allocate GBs (#1632).
    let mut entries = Vec::with_capacity(entry_count.min(REASONABLE_COLLECTION_CAPACITY));

    // Decode EXACTLY `entry_count` key/value pairs. A valid map cell holds
    // exactly `count` fully-encoded entries, so a buffer that runs dry before
    // `count` entries are decoded is corrupt/truncated and must Err — silently
    // returning a short partial map would accept a truncated cell (#1632).
    for _ in 0..entry_count {
        if offset >= data.len() {
            return Err(Error::corruption(
                "Map declared more entries than present in buffer (truncated)",
            ));
        }

        // Parse key length and data
        let (remaining, key_len) = parse_vint_length(&data[offset..])
            .map_err(|_| Error::corruption("Failed to parse map key length"))?;
        offset = data.len() - remaining.len();

        if key_len > remaining.len() {
            return Err(Error::corruption("Map key length exceeds available data"));
        }

        let key_data = &remaining[..key_len];
        let key_value = parse_element(key_data, key_comparator)?;
        offset += key_len;

        // Parse value length and data
        let (remaining, value_len) = parse_vint_length(&data[offset..])
            .map_err(|_| Error::corruption("Failed to parse map value length"))?;
        offset = data.len() - remaining.len();

        if value_len > remaining.len() {
            return Err(Error::corruption("Map value length exceeds available data"));
        }

        let val_data = &remaining[..value_len];
        let val_value = parse_element(val_data, value_comparator)?;
        entries.push((key_value, val_value));
        offset += value_len;
    }

    Ok(Value::Map(entries))
}

/// Parse tuple value with recursive field parsing via closure
///
/// Cassandra tuple field lengths are encoded as 4-byte big-endian signed int32
/// (TupleType.java uses `accessor.putInt`), with -1 meaning null.
/// This is NOT VInt encoding.
pub(crate) fn parse_tuple_value_with<F>(
    data: &[u8],
    field_comparators: &[ComparatorType],
    parse_element: F,
) -> Result<Value>
where
    F: Fn(&[u8], &ComparatorType) -> Result<Value>,
{
    let mut offset = 0;
    let mut fields = Vec::new();

    // Parse each field
    for (i, field_comparator) in field_comparators.iter().enumerate() {
        if offset >= data.len() {
            break;
        }

        // Parse field length as 4-byte big-endian signed int32 (Cassandra specification)
        // -1 = null field, 0 = empty field, >0 = byte count
        if offset + 4 > data.len() {
            return Err(Error::corruption(format!(
                "Tuple field {} length prefix truncated",
                i
            )));
        }
        let field_len_i32 =
            i32::from_be_bytes(data[offset..offset + 4].try_into().map_err(|_| {
                Error::corruption(format!("Tuple field {} length bytes invalid", i))
            })?);
        offset += 4;

        if field_len_i32 == -1 {
            // Null field
            fields.push(Value::Null);
            continue;
        }
        if field_len_i32 < 0 {
            return Err(Error::corruption(format!(
                "Tuple field {} has invalid negative length {}",
                i, field_len_i32
            )));
        }
        let field_len = field_len_i32 as usize;

        if offset + field_len > data.len() {
            return Err(Error::corruption(format!(
                "Tuple field {} length {} exceeds available data",
                i, field_len
            )));
        }

        // Parse field value using provided closure
        let field_data = &data[offset..offset + field_len];
        let field_value = parse_element(field_data, field_comparator)?;
        fields.push(field_value);
        offset += field_len;
    }

    Ok(Value::Tuple(fields))
}

/// Parse UDT value with recursive field parsing via closure
///
/// Cassandra UDT field lengths are encoded as 4-byte big-endian signed int32
/// (TupleType.java / UserType.java use `accessor.putInt`), with -1 meaning null.
/// This is NOT VInt encoding.
pub(crate) fn parse_udt_value_with<F>(
    data: &[u8],
    field_comparators: &[(String, ComparatorType)],
    parse_element: F,
) -> Result<UdtValue>
where
    F: Fn(&[u8], &ComparatorType) -> Result<Value>,
{
    let mut offset = 0;
    let mut fields = Vec::new();

    // Parse each field
    for (field_name, field_comparator) in field_comparators.iter() {
        if offset >= data.len() {
            break;
        }

        // Parse field length as 4-byte big-endian signed int32 (Cassandra specification)
        // -1 = null field, 0 = empty field, >0 = byte count
        if offset + 4 > data.len() {
            return Err(Error::corruption(format!(
                "UDT field {} length prefix truncated",
                field_name
            )));
        }
        let field_len_i32 =
            i32::from_be_bytes(data[offset..offset + 4].try_into().map_err(|_| {
                Error::corruption(format!("UDT field {} length bytes invalid", field_name))
            })?);
        offset += 4;

        if field_len_i32 == -1 {
            // Null field
            fields.push(UdtField {
                name: field_name.clone(),
                value: None,
            });
            continue;
        }
        if field_len_i32 < 0 {
            return Err(Error::corruption(format!(
                "UDT field {} has invalid negative length {}",
                field_name, field_len_i32
            )));
        }
        let field_len = field_len_i32 as usize;

        if offset + field_len > data.len() {
            return Err(Error::corruption(format!(
                "UDT field {} length {} exceeds available data",
                field_name, field_len
            )));
        }

        // Parse field value using provided closure
        let field_data = &data[offset..offset + field_len];
        let field_value = parse_element(field_data, field_comparator)?;

        fields.push(UdtField {
            name: field_name.clone(),
            value: Some(field_value),
        });
        offset += field_len;
    }

    Ok(UdtValue {
        keyspace: "unknown".to_string(),
        type_name: "unknown".to_string(),
        fields,
    })
}

// ============================================================================
// SSTableReader Methods (Wrappers around pure functions)
// ============================================================================

impl SSTableReader {
    /// Parse column value using schema-driven approach (no heuristics)
    pub(in crate::storage::sstable::reader) fn parse_column_value_enhanced(
        &self,
        value_data: &[u8],
        table_id: &TableId,
        key: &RowKey,
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<Value> {
        if value_data.is_empty() {
            return Ok(Value::Null);
        }

        // Use schema information to determine exact type - NO GUESSING
        if let Some(schema) = self.get_table_schema(schema) {
            // Extract column name from key context if possible
            if let Some(column_name) = self.extract_column_name_from_context(table_id, key) {
                // Find column in schema
                if let Some(column) = schema.columns.iter().find(|c| c.name == column_name) {
                    // Parse using exact type from schema
                    return self.parse_value_with_schema_type(value_data, &column.data_type);
                }
            }
        }

        // Modern formats should never use blob fallback without schema
        match self.header.cassandra_version {
            crate::parser::header::CassandraVersion::V5_0NewBig
            | crate::parser::header::CassandraVersion::V5_0Bti => Err(Error::Schema(format!(
                "Blob fallback not allowed for value parsing in modern format {:?}. \
                     Use schema-aware decode via SSTableReader with complete schema information.",
                self.header.cassandra_version
            ))),
            _ => {
                // Legacy formats can use blob fallback as last resort
                #[cfg(feature = "legacy-heuristics")]
                {
                    Ok(Value::blob(value_data.to_vec()))
                }
                #[cfg(not(feature = "legacy-heuristics"))]
                {
                    Err(Error::Schema(
                        "Blob fallback requires legacy-heuristics feature for legacy compatibility.".to_string()
                    ))
                }
            }
        }
    }

    /// Parse value using exact schema type information
    pub(in crate::storage::sstable::reader) fn parse_value_with_schema_type(
        &self,
        value_data: &[u8],
        data_type: &str,
    ) -> Result<Value> {
        // Convert data type string directly to ComparatorType for decoding
        let comparator = ComparatorType::from_data_type(data_type)?;

        // Structural types keep this reader's recursion + modern-format guards;
        // every scalar (incl. schema-derived Custom) routes through the ONE scalar
        // decode body in `comparator_value_parsing` (issue #1636 / J2), so a scalar
        // type fix lands in exactly one place.
        // Top-level column value enters at depth 0; nested elements/fields/frozen
        // inner types accumulate depth and are capped by MAX_VALUE_NESTING_DEPTH
        // (issue #1632) so a corrupt/adversarial deeply-nested type errors rather
        // than overflowing the stack.
        match &comparator {
            ComparatorType::List(element_comparator) => {
                self.parse_list_value(value_data, element_comparator, 0)
            }
            ComparatorType::Set(element_comparator) => {
                self.parse_set_value(value_data, element_comparator, 0)
            }
            ComparatorType::Map(key_comparator, value_comparator) => {
                self.parse_map_value(value_data, key_comparator, value_comparator, 0)
            }
            ComparatorType::Tuple(field_comparators) => {
                self.parse_tuple_value(value_data, field_comparators, 0)
            }
            ComparatorType::Udt {
                field_comparators, ..
            } => self.parse_udt_value(value_data, field_comparators, 0),
            ComparatorType::Frozen(inner_comparator) => {
                // Issue #2339 (roborev job 124, High): route through the SAME frozen
                // dispatcher the comparator decoder uses. A frozen COLLECTION body is
                // i32-BE element-framed, NOT VInt-framed like a non-frozen collection
                // cell, so unwrapping `Frozen` and recursing here decoded real frozen
                // lists/sets/maps with the wrong framing — and this is the PRIMARY
                // `SSTableReader` path, i.e. ordinary single-generation reads. Fixing
                // only the comparator decoder left two framing authorities for one
                // on-disk shape, which is the divergence class #2339 exists to remove.
                //
                // The outer `frozen<...>` layer costs one nesting level, so the inner
                // comparator is entered at depth 1 — symmetric with the block path
                // below. Entering at depth 0 would silently allow one extra nested
                // level past MAX_VALUE_NESTING_DEPTH (#1632, guard-only).
                let inner_value = super::frozen_value_parsing::parse_frozen_inner_with(
                    value_data,
                    inner_comparator,
                    1,
                    MAX_VALUE_NESTING_DEPTH,
                    &|d, c, dep| self.parse_value_with_comparator_at_depth(d, c, dep),
                )?;
                Ok(Value::Frozen(Box::new(inner_value)))
            }
            _ => decode_scalar_comparator(value_data, &comparator),
        }
    }

    /// Parse value directly using ComparatorType (helper for nested collection elements).
    ///
    /// Provides complete recursive type parsing for collection elements, including
    /// UDTs, tuples, nested collections, and frozen types. Entry callers pass
    /// `depth = 0`; every recursive descent into a nested element/field/frozen inner type
    /// increments `depth`; exceeding [`MAX_VALUE_NESTING_DEPTH`] returns `Err`
    /// instead of recursing to a stack overflow (issue #1632). Guard-only:
    /// successful decoding within the depth budget is byte-identical to before.
    pub(in crate::storage::sstable::reader) fn parse_value_with_comparator_at_depth(
        &self,
        value_data: &[u8],
        comparator: &ComparatorType,
        depth: usize,
    ) -> Result<Value> {
        if depth > MAX_VALUE_NESTING_DEPTH {
            return Err(Error::corruption(format!(
                "Value decode recursion depth {} exceeds maximum {}",
                depth, MAX_VALUE_NESTING_DEPTH
            )));
        }
        match comparator {
            ComparatorType::List(element_comparator) => {
                self.parse_list_value(value_data, element_comparator, depth)
            }
            ComparatorType::Set(element_comparator) => {
                self.parse_set_value(value_data, element_comparator, depth)
            }
            ComparatorType::Map(key_comparator, value_comparator) => {
                self.parse_map_value(value_data, key_comparator, value_comparator, depth)
            }
            ComparatorType::Tuple(field_comparators) => {
                self.parse_tuple_value(value_data, field_comparators, depth)
            }
            ComparatorType::Udt {
                keyspace,
                type_name,
                field_comparators,
            } => {
                // Parse UDT fields inline with full type info (Issue #238 fix)
                // This avoids the V5 format check in parse_udt_value() which incorrectly
                // returns an error even when we have complete schema information.
                // Field lengths are 4-byte big-endian signed int32 per Cassandra specification.
                let mut offset = 0;
                let mut fields = Vec::new();

                for (field_name, field_comparator) in field_comparators.iter() {
                    if offset >= value_data.len() {
                        break;
                    }
                    // Parse field length as 4-byte big-endian signed int32 (Cassandra spec)
                    if offset + 4 > value_data.len() {
                        return Err(Error::corruption(format!(
                            "UDT field {} length prefix truncated",
                            field_name
                        )));
                    }
                    let field_len_i32 = i32::from_be_bytes(
                        value_data[offset..offset + 4].try_into().map_err(|_| {
                            Error::corruption(format!("UDT field {} length invalid", field_name))
                        })?,
                    );
                    offset += 4;

                    if field_len_i32 == -1 {
                        fields.push(UdtField {
                            name: field_name.clone(),
                            value: None,
                        });
                        continue;
                    }
                    if field_len_i32 < 0 {
                        return Err(Error::corruption(format!(
                            "UDT field {} has invalid negative length {}",
                            field_name, field_len_i32
                        )));
                    }
                    let field_len = field_len_i32 as usize;

                    if offset + field_len > value_data.len() {
                        return Err(Error::corruption(format!(
                            "UDT field {} length {} exceeds available data",
                            field_name, field_len
                        )));
                    }

                    let field_data = &value_data[offset..offset + field_len];
                    let field_value = self.parse_value_with_comparator_at_depth(
                        field_data,
                        field_comparator,
                        depth + 1,
                    )?;

                    fields.push(UdtField {
                        name: field_name.clone(),
                        value: Some(field_value),
                    });
                    offset += field_len;
                }

                Ok(Value::Udt(Box::new(UdtValue {
                    keyspace: keyspace.clone().unwrap_or_else(|| "unknown".to_string()),
                    type_name: type_name.clone(),
                    fields,
                })))
            }
            ComparatorType::Frozen(inner_comparator) => {
                // Same frozen dispatcher as the entry arm above and as the comparator
                // decoder (issue #2339, roborev job 124). This is the DEPTH-AWARE
                // recursion, so the caller's `depth` is carried in and incremented for
                // the inner type exactly as before — the framing changes, the
                // recursion-depth accounting does not.
                let inner_value = super::frozen_value_parsing::parse_frozen_inner_with(
                    value_data,
                    inner_comparator,
                    depth + 1,
                    MAX_VALUE_NESTING_DEPTH,
                    &|d, c, dep| self.parse_value_with_comparator_at_depth(d, c, dep),
                )?;
                Ok(Value::Frozen(Box::new(inner_value)))
            }
            // Every scalar (incl. schema-derived Custom) routes through the ONE
            // scalar decode body in `comparator_value_parsing` (issue #1636 / J2).
            _ => decode_scalar_comparator(value_data, comparator),
        }
    }

    /// Parse list value using element comparator
    pub(in crate::storage::sstable::reader) fn parse_list_value(
        &self,
        value_data: &[u8],
        element_comparator: &ComparatorType,
        depth: usize,
    ) -> Result<Value> {
        parse_list_value_with(value_data, element_comparator, |data, comp| {
            self.parse_value_with_comparator_at_depth(data, comp, depth + 1)
        })
    }

    /// Parse set value using element comparator
    pub(in crate::storage::sstable::reader) fn parse_set_value(
        &self,
        value_data: &[u8],
        element_comparator: &ComparatorType,
        depth: usize,
    ) -> Result<Value> {
        parse_set_value_with(value_data, element_comparator, |data, comp| {
            self.parse_value_with_comparator_at_depth(data, comp, depth + 1)
        })
    }

    /// Parse map value using key and value comparators
    pub(in crate::storage::sstable::reader) fn parse_map_value(
        &self,
        value_data: &[u8],
        key_comparator: &ComparatorType,
        value_comparator: &ComparatorType,
        depth: usize,
    ) -> Result<Value> {
        parse_map_value_with(
            value_data,
            key_comparator,
            value_comparator,
            |data, comp| self.parse_value_with_comparator_at_depth(data, comp, depth + 1),
        )
    }

    /// Parse tuple value using field comparators
    pub(in crate::storage::sstable::reader) fn parse_tuple_value(
        &self,
        value_data: &[u8],
        field_comparators: &[ComparatorType],
        depth: usize,
    ) -> Result<Value> {
        parse_tuple_value_with(value_data, field_comparators, |data, comp| {
            self.parse_value_with_comparator_at_depth(data, comp, depth + 1)
        })
    }

    /// Parse UDT value using field comparators
    ///
    /// Cassandra UDT field lengths are 4-byte big-endian signed int32 (not VInt).
    pub(in crate::storage::sstable::reader) fn parse_udt_value(
        &self,
        value_data: &[u8],
        field_comparators: &[(String, ComparatorType)],
        depth: usize,
    ) -> Result<Value> {
        let mut offset = 0;
        let mut fields = Vec::new();

        // Parse each field
        for (field_name, field_comparator) in field_comparators.iter() {
            if offset >= value_data.len() {
                break;
            }

            // Parse field length as 4-byte big-endian signed int32 (Cassandra specification)
            if offset + 4 > value_data.len() {
                return Err(Error::corruption(format!(
                    "UDT field {} length prefix truncated",
                    field_name
                )));
            }
            let field_len_i32 =
                i32::from_be_bytes(value_data[offset..offset + 4].try_into().map_err(|_| {
                    Error::corruption(format!("UDT field {} length invalid", field_name))
                })?);
            offset += 4;

            if field_len_i32 == -1 {
                fields.push(UdtField {
                    name: field_name.clone(),
                    value: None,
                });
                continue;
            }
            if field_len_i32 < 0 {
                return Err(Error::corruption(format!(
                    "UDT field {} has invalid negative length {}",
                    field_name, field_len_i32
                )));
            }
            let field_len = field_len_i32 as usize;

            if offset + field_len > value_data.len() {
                return Err(Error::corruption(format!(
                    "UDT field {} length {} exceeds available data",
                    field_name, field_len
                )));
            }

            // Parse field value using field comparator
            let field_data = &value_data[offset..offset + field_len];
            let field_value =
                self.parse_value_with_comparator_at_depth(field_data, field_comparator, depth + 1)?;
            offset += field_len;

            fields.push(UdtField {
                name: field_name.clone(),
                value: Some(field_value),
            });
        }

        // Modern formats should never use generic UDT fabrication without schema
        match self.header.cassandra_version {
            crate::parser::header::CassandraVersion::V5_0NewBig
            | crate::parser::header::CassandraVersion::V5_0Bti => Err(Error::Schema(format!(
                "Generic UDT fabrication not allowed for modern format {:?}. \
                     Use schema-aware decode via SSTableReader with complete UDT schema information.",
                self.header.cassandra_version
            ))),
            _ => {
                // Legacy formats can use generic UDT fabrication as last resort
                #[cfg(feature = "legacy-heuristics")]
                {
                    Ok(Value::Udt(Box::new(UdtValue {
                        keyspace: "unknown".to_string(),
                        type_name: "unknown".to_string(),
                        fields,
                    })))
                }
                #[cfg(not(feature = "legacy-heuristics"))]
                {
                    Err(Error::Schema(
                        "Generic UDT fabrication requires legacy-heuristics feature for legacy compatibility.".to_string()
                    ))
                }
            }
        }
    }

    /// Extract column name from key context (schema-aware implementation)
    pub(in crate::storage::sstable::reader) fn extract_column_name_from_context(
        &self,
        _table_id: &TableId,
        _key: &RowKey,
    ) -> Option<String> {
        // Modern formats require schema-aware decode (registered schema) for proper column name extraction
        match self.header.cassandra_version {
            crate::parser::header::CassandraVersion::V5_0NewBig
            | crate::parser::header::CassandraVersion::V5_0Bti => {
                // Modern formats should not use this placeholder implementation
                tracing::error!(
                    "Column name extraction from key context requires schema-aware decode (registered schema) for modern format {:?}",
                    self.header.cassandra_version
                );
                None
            }
            _ => {
                // Legacy formats return None (placeholder behavior)
                #[cfg(feature = "legacy-heuristics")]
                {
                    None // Placeholder implementation for legacy compatibility
                }
                #[cfg(not(feature = "legacy-heuristics"))]
                {
                    None // Column name extraction not supported without legacy features
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "value_parsing_tests.rs"]
mod tests;
