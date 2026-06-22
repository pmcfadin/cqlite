//! Canonical partition-key (de)serialization codec.
//!
//! This module is the single source of truth for turning the raw partition-key
//! bytes stored in SSTables (and produced by the write engine) back into typed
//! [`Value`]s. Both the read path (scan-built rows in the query engine) and the
//! write path ([`crate::storage::write_engine::mutation::PartitionKey`]) decode
//! through here so the two never drift apart.
//!
//! ## Wire layout (matches Cassandra and `PartitionKey::to_bytes`)
//!
//! - **Single-component keys** — raw value bytes, *no* length prefix
//!   (UUID = 16 bytes, int = 4 BE bytes, text = raw UTF-8, …).
//! - **Multi-component (composite) keys** — `[len: u16 BE][value bytes][0x00]`
//!   per component, including a trailing `0x00` after the final component.
//!
//! Historically the query engine carried its own ad-hoc decoder that assumed a
//! `u16` length prefix for *every* TEXT key. That was correct for composite
//! components but wrong for single-component text partition keys (whose bytes
//! are raw), so reconstructing a TEXT single-PK column silently failed and the
//! column was dropped from scan-built rows (Issue #586). Routing both paths
//! through this module fixes that and prevents a recurrence.

use crate::schema::TableSchema;
use crate::types::ComparatorType;
use crate::{Error, Result, Value};

/// Decode the partition-key columns from their raw on-disk bytes.
///
/// Returns the `(column_name, value)` pairs in schema-declared partition-key
/// order. `data` must be the raw partition-key bytes (single-component: raw
/// value; multi-component: `[len:u16 BE][value][0x00]` framing).
pub fn decode_partition_key_columns(
    data: &[u8],
    schema: &TableSchema,
) -> Result<Vec<(String, Value)>> {
    if schema.partition_keys.is_empty() {
        return Err(Error::InvalidInput(
            "Schema has no partition keys".to_string(),
        ));
    }

    if data.is_empty() {
        return Err(Error::InvalidInput("Empty partition key bytes".to_string()));
    }

    let mut columns = Vec::with_capacity(schema.partition_keys.len());

    if schema.partition_keys.len() == 1 {
        // Single-component: the whole buffer is the raw value (no length prefix).
        let key_col = &schema.partition_keys[0];
        let comparator = ComparatorType::from_data_type(&key_col.data_type)?;
        let value = deserialize_value_bytes(data, &comparator)?;
        columns.push((key_col.name.clone(), value));
    } else {
        // Multi-component: [len:u16 BE][value bytes][0x00] per component.
        let mut offset = 0;
        for key_col in &schema.partition_keys {
            if offset + 2 > data.len() {
                return Err(Error::InvalidInput(format!(
                    "Truncated multi-component partition key at offset {}",
                    offset
                )));
            }
            let len = u16::from_be_bytes([data[offset], data[offset + 1]]) as usize;
            offset += 2;

            if offset + len > data.len() {
                return Err(Error::InvalidInput(format!(
                    "Partition key component extends beyond data: offset={}, len={}, data_len={}",
                    offset,
                    len,
                    data.len()
                )));
            }

            let comparator = ComparatorType::from_data_type(&key_col.data_type)?;
            let value = deserialize_value_bytes(&data[offset..offset + len], &comparator)?;
            columns.push((key_col.name.clone(), value));
            offset += len;

            // Skip the 0x00 end-of-component marker.
            if offset < data.len() {
                offset += 1;
            }
        }
    }

    Ok(columns)
}

/// Encode partition-key column values into the raw on-disk partition-key bytes.
///
/// This is the inverse of [`decode_partition_key_columns`] and produces byte-for-byte
/// the same layout the write path emits (`PartitionKey::to_bytes`):
///
/// - **Single-component keys** — raw value bytes, no length prefix.
/// - **Multi-component (composite) keys** — `[len:u16 BE][value bytes][0x00]` per
///   component, including a trailing `0x00` after the final component.
///
/// `values` must be in schema-declared partition-key order and have exactly one
/// entry per partition-key column. Because the encoding reproduces the on-disk raw
/// key bytes exactly, the result can be matched directly against (a) the partition
/// RowKeys returned by a scan and (b) the bloom filter / BTI trie, which are keyed
/// on the same raw bytes. Used by the query engine to turn a fully-constrained
/// `WHERE pk = ?` into a partition-targeted lookup.
///
/// Returns an error (so callers can fall back to a full scan) when the column count
/// disagrees with the schema or a value cannot be serialized for its declared type.
pub fn encode_partition_key_columns(values: &[Value], schema: &TableSchema) -> Result<Vec<u8>> {
    if schema.partition_keys.is_empty() {
        return Err(Error::InvalidInput(
            "Schema has no partition keys".to_string(),
        ));
    }
    if values.len() != schema.partition_keys.len() {
        return Err(Error::InvalidInput(format!(
            "Partition key column count mismatch: expected {}, got {}",
            schema.partition_keys.len(),
            values.len()
        )));
    }

    // Single-component key: raw value bytes, no length prefix.
    if schema.partition_keys.len() == 1 {
        let comparator = ComparatorType::from_data_type(&schema.partition_keys[0].data_type)?;
        let coerced = coerce_value_for_comparator(&values[0], &comparator);
        return serialize_value_bytes(&coerced, &comparator);
    }

    // Multi-component: [len:u16 BE][value bytes][0x00] per component.
    let mut result = Vec::new();
    for (key_col, value) in schema.partition_keys.iter().zip(values.iter()) {
        let comparator = ComparatorType::from_data_type(&key_col.data_type)?;
        let coerced = coerce_value_for_comparator(value, &comparator);
        let value_bytes = serialize_value_bytes(&coerced, &comparator)?;
        if value_bytes.len() > u16::MAX as usize {
            return Err(Error::InvalidInput(format!(
                "Partition key component too large: {} bytes",
                value_bytes.len()
            )));
        }
        result.extend_from_slice(&(value_bytes.len() as u16).to_be_bytes());
        result.extend_from_slice(&value_bytes);
        result.push(0x00);
    }
    Ok(result)
}

/// Best-effort coercion of a parsed CQL literal `Value` to the variant the
/// partition-key serializer expects for `comparator`.
///
/// The SELECT parser emits every integer literal as [`Value::BigInt`], so a
/// `WHERE id = 5` against an `int`/`smallint`/`tinyint`/`timestamp` partition key
/// would otherwise fail to serialize. This narrows those numeric literals to the
/// declared type (only when the value fits); every other case is returned
/// unchanged so an unrepresentable value falls through to a serialization error
/// and the caller reverts to a full scan.
fn coerce_value_for_comparator(value: &Value, comparator: &ComparatorType) -> Value {
    match (value, comparator) {
        (Value::BigInt(n), ComparatorType::Int)
            if *n >= i32::MIN as i64 && *n <= i32::MAX as i64 =>
        {
            Value::Integer(*n as i32)
        }
        (Value::BigInt(n), ComparatorType::SmallInt)
            if *n >= i16::MIN as i64 && *n <= i16::MAX as i64 =>
        {
            Value::SmallInt(*n as i16)
        }
        (Value::BigInt(n), ComparatorType::TinyInt)
            if *n >= i8::MIN as i64 && *n <= i8::MAX as i64 =>
        {
            Value::TinyInt(*n as i8)
        }
        (Value::BigInt(n), ComparatorType::Timestamp) => Value::Timestamp(*n),
        (Value::Integer(n), ComparatorType::BigInt) => Value::BigInt(*n as i64),
        _ => value.clone(),
    }
}

/// Serialize a single partition-key value to its raw on-disk bytes.
///
/// Mirrors the write engine's `serialize_value_bytes`
/// ([`crate::storage::write_engine::mutation`]); kept here as the inverse of
/// [`deserialize_value_bytes`] so the read-side encoder has no dependency on the
/// (feature-gated) write engine.
fn serialize_value_bytes(value: &Value, comparator: &ComparatorType) -> Result<Vec<u8>> {
    match (value, comparator) {
        (Value::Boolean(b), ComparatorType::Boolean) => Ok(vec![u8::from(*b)]),
        (Value::TinyInt(n), ComparatorType::TinyInt) => Ok(vec![*n as u8]),
        (Value::SmallInt(n), ComparatorType::SmallInt) => Ok(n.to_be_bytes().to_vec()),
        (Value::Integer(n), ComparatorType::Int) => Ok(n.to_be_bytes().to_vec()),
        (Value::BigInt(n), ComparatorType::BigInt) => Ok(n.to_be_bytes().to_vec()),
        (Value::Counter(n), ComparatorType::Counter) => Ok(n.to_be_bytes().to_vec()),
        (Value::Float32(f), ComparatorType::Float32) => Ok(f.to_bits().to_be_bytes().to_vec()),
        (Value::Float(f), ComparatorType::Float) => Ok(f.to_bits().to_be_bytes().to_vec()),
        (Value::Text(s), ComparatorType::Text) => Ok(s.as_bytes().to_vec()),
        (Value::Blob(bytes), ComparatorType::Blob) => Ok(bytes.clone()),
        (Value::Timestamp(millis), ComparatorType::Timestamp) => Ok(millis.to_be_bytes().to_vec()),
        (Value::Date(days), ComparatorType::Date) => {
            let stored = days.wrapping_sub(i32::MIN) as u32;
            Ok(stored.to_be_bytes().to_vec())
        }
        (Value::Uuid(bytes), ComparatorType::Uuid) => Ok(bytes.to_vec()),
        (Value::Time(nanos), ComparatorType::Custom(name)) if name == "time" => {
            Ok(nanos.to_be_bytes().to_vec())
        }
        (Value::Inet(bytes), ComparatorType::Custom(name)) if name == "inet" => Ok(bytes.clone()),
        (Value::Varint(bytes), ComparatorType::Varint) => Ok(bytes.clone()),
        (Value::Decimal { scale, unscaled }, ComparatorType::Decimal) => {
            let mut result = Vec::with_capacity(4 + unscaled.len());
            result.extend_from_slice(&scale.to_be_bytes());
            result.extend_from_slice(unscaled);
            Ok(result)
        }
        (
            Value::Duration {
                months,
                days,
                nanos,
            },
            ComparatorType::Duration,
        ) => {
            let mut result = Vec::with_capacity(16);
            result.extend_from_slice(&months.to_be_bytes());
            result.extend_from_slice(&days.to_be_bytes());
            result.extend_from_slice(&nanos.to_be_bytes());
            Ok(result)
        }
        _ => Err(Error::InvalidInput(format!(
            "Type mismatch encoding partition key: value {:?} does not match comparator {:?}",
            value, comparator
        ))),
    }
}

/// Deserialize a single raw value from its byte-comparable key encoding.
///
/// `data` is the raw value bytes for one component (no length prefix). This is
/// the inverse of the write engine's `serialize_value_bytes`.
pub fn deserialize_value_bytes(data: &[u8], comparator: &ComparatorType) -> Result<Value> {
    match comparator {
        ComparatorType::Boolean => {
            if data.is_empty() {
                return Err(Error::InvalidInput("Empty boolean value".to_string()));
            }
            Ok(Value::Boolean(data[0] != 0))
        }
        ComparatorType::TinyInt => {
            if data.is_empty() {
                return Err(Error::InvalidInput("Empty tinyint value".to_string()));
            }
            Ok(Value::TinyInt(data[0] as i8))
        }
        ComparatorType::SmallInt => {
            let bytes: [u8; 2] = data.try_into().map_err(|_| {
                Error::InvalidInput(format!("SmallInt requires 2 bytes, got {}", data.len()))
            })?;
            Ok(Value::SmallInt(i16::from_be_bytes(bytes)))
        }
        ComparatorType::Int => {
            let bytes: [u8; 4] = data.try_into().map_err(|_| {
                Error::InvalidInput(format!("Int requires 4 bytes, got {}", data.len()))
            })?;
            Ok(Value::Integer(i32::from_be_bytes(bytes)))
        }
        ComparatorType::BigInt => {
            let bytes: [u8; 8] = data.try_into().map_err(|_| {
                Error::InvalidInput(format!("BigInt requires 8 bytes, got {}", data.len()))
            })?;
            Ok(Value::BigInt(i64::from_be_bytes(bytes)))
        }
        ComparatorType::Counter => {
            let bytes: [u8; 8] = data.try_into().map_err(|_| {
                Error::InvalidInput(format!("Counter requires 8 bytes, got {}", data.len()))
            })?;
            Ok(Value::Counter(i64::from_be_bytes(bytes)))
        }
        ComparatorType::Float32 => {
            let bytes: [u8; 4] = data.try_into().map_err(|_| {
                Error::InvalidInput(format!("Float32 requires 4 bytes, got {}", data.len()))
            })?;
            Ok(Value::Float32(f32::from_bits(u32::from_be_bytes(bytes))))
        }
        ComparatorType::Float => {
            let bytes: [u8; 8] = data.try_into().map_err(|_| {
                Error::InvalidInput(format!("Float requires 8 bytes, got {}", data.len()))
            })?;
            Ok(Value::Float(f64::from_bits(u64::from_be_bytes(bytes))))
        }
        ComparatorType::Text => {
            let s = String::from_utf8(data.to_vec())
                .map_err(|e| Error::InvalidInput(format!("Invalid UTF-8 in text value: {}", e)))?;
            Ok(Value::Text(s))
        }
        ComparatorType::Blob => Ok(Value::Blob(data.to_vec())),
        ComparatorType::Timestamp => {
            let bytes: [u8; 8] = data.try_into().map_err(|_| {
                Error::InvalidInput(format!("Timestamp requires 8 bytes, got {}", data.len()))
            })?;
            Ok(Value::Timestamp(i64::from_be_bytes(bytes)))
        }
        ComparatorType::Date => {
            let bytes: [u8; 4] = data.try_into().map_err(|_| {
                Error::InvalidInput(format!("Date requires 4 bytes, got {}", data.len()))
            })?;
            let stored = u32::from_be_bytes(bytes);
            Ok(Value::Date((stored as i32).wrapping_add(i32::MIN)))
        }
        ComparatorType::Uuid => {
            let bytes: [u8; 16] = data.try_into().map_err(|_| {
                Error::InvalidInput(format!("UUID requires 16 bytes, got {}", data.len()))
            })?;
            Ok(Value::Uuid(bytes))
        }
        ComparatorType::Custom(name) if name == "time" => {
            let bytes: [u8; 8] = data.try_into().map_err(|_| {
                Error::InvalidInput(format!("Time requires 8 bytes, got {}", data.len()))
            })?;
            Ok(Value::Time(i64::from_be_bytes(bytes)))
        }
        ComparatorType::Custom(name) if name == "inet" => Ok(Value::Inet(data.to_vec())),
        ComparatorType::Varint => Ok(Value::Varint(data.to_vec())),
        ComparatorType::Decimal => {
            if data.len() < 4 {
                return Err(Error::InvalidInput(format!(
                    "Decimal requires at least 4 bytes, got {}",
                    data.len()
                )));
            }
            // Length already validated above
            let scale_bytes: [u8; 4] = data[..4]
                .try_into()
                .map_err(|_| Error::InvalidInput("Decimal scale conversion failed".to_string()))?;
            let scale = i32::from_be_bytes(scale_bytes);
            let unscaled = data[4..].to_vec();
            Ok(Value::Decimal { scale, unscaled })
        }
        ComparatorType::Duration => {
            if data.len() < 16 {
                return Err(Error::InvalidInput(format!(
                    "Duration requires 16 bytes, got {}",
                    data.len()
                )));
            }
            let months = i32::from_be_bytes(data[..4].try_into().map_err(|_| {
                Error::InvalidInput("Duration months conversion failed".to_string())
            })?);
            let days =
                i32::from_be_bytes(data[4..8].try_into().map_err(|_| {
                    Error::InvalidInput("Duration days conversion failed".to_string())
                })?);
            let nanos = i64::from_be_bytes(data[8..16].try_into().map_err(|_| {
                Error::InvalidInput("Duration nanos conversion failed".to_string())
            })?);
            Ok(Value::Duration {
                months,
                days,
                nanos,
            })
        }
        _ => Err(Error::InvalidInput(format!(
            "Unsupported comparator for deserialization: {:?}",
            comparator
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::KeyColumn;
    use std::collections::HashMap;

    fn schema_with_pks(pks: &[(&str, &str)]) -> TableSchema {
        TableSchema {
            keyspace: "ks".to_string(),
            table: "t".to_string(),
            partition_keys: pks
                .iter()
                .enumerate()
                .map(|(i, (name, ty))| KeyColumn {
                    name: name.to_string(),
                    data_type: ty.to_string(),
                    position: i,
                })
                .collect(),
            clustering_keys: vec![],
            columns: vec![],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }

    #[test]
    fn single_text_pk_is_raw_bytes() {
        // Regression for #586: a single TEXT partition key is stored as raw
        // UTF-8 with NO u16 length prefix. The old decoder read a phantom
        // prefix and failed.
        let schema = schema_with_pks(&[("id", "text")]);
        let cols = decode_partition_key_columns(b"k0000000000000000", &schema).unwrap();
        assert_eq!(
            cols,
            vec![(
                "id".to_string(),
                Value::Text("k0000000000000000".to_string())
            )]
        );
    }

    #[test]
    fn single_uuid_pk_is_raw_16_bytes() {
        let schema = schema_with_pks(&[("id", "uuid")]);
        let raw = [
            0u8, 35, 236, 231, 124, 78, 71, 5, 144, 104, 209, 165, 158, 197, 254, 25,
        ];
        let cols = decode_partition_key_columns(&raw, &schema).unwrap();
        assert_eq!(cols, vec![("id".to_string(), Value::Uuid(raw))]);
    }

    #[test]
    fn composite_text_pk_uses_framing_per_component() {
        // [len=1]['a'][0x00][len=4]["view"][0x00] — both components decode
        // independently (the old decoder returned component[0] for both).
        let schema = schema_with_pks(&[("application_id", "text"), ("metric_name", "text")]);
        let data = [0u8, 1, b'a', 0, 0, 4, b'v', b'i', b'e', b'w', 0];
        let cols = decode_partition_key_columns(&data, &schema).unwrap();
        assert_eq!(
            cols,
            vec![
                ("application_id".to_string(), Value::Text("a".to_string())),
                ("metric_name".to_string(), Value::Text("view".to_string())),
            ]
        );
    }

    #[test]
    fn composite_text_and_date_decode_by_type() {
        // symbol=text "AAPL", trading_day=date — old code returned a debug
        // string for the date component.
        let schema = schema_with_pks(&[("symbol", "text"), ("trading_day", "date")]);
        let data = [0u8, 4, b'A', b'A', b'P', b'L', 0, 0, 4, 128, 0, 79, 136, 0];
        let cols = decode_partition_key_columns(&data, &schema).unwrap();
        assert_eq!(
            cols[0],
            ("symbol".to_string(), Value::Text("AAPL".to_string()))
        );
        assert_eq!(cols[1].0, "trading_day");
        assert!(
            matches!(cols[1].1, Value::Date(_)),
            "date component must decode to Value::Date, got {:?}",
            cols[1].1
        );
    }

    #[test]
    fn empty_bytes_error() {
        let schema = schema_with_pks(&[("id", "text")]);
        assert!(decode_partition_key_columns(&[], &schema).is_err());
    }

    #[test]
    fn encode_single_text_pk_is_raw_bytes() {
        let schema = schema_with_pks(&[("id", "text")]);
        let bytes =
            encode_partition_key_columns(&[Value::Text("k0000000000000000".to_string())], &schema)
                .unwrap();
        assert_eq!(bytes, b"k0000000000000000");
    }

    #[test]
    fn encode_decode_roundtrip_single_uuid() {
        let schema = schema_with_pks(&[("id", "uuid")]);
        let raw = [
            0u8, 35, 236, 231, 124, 78, 71, 5, 144, 104, 209, 165, 158, 197, 254, 25,
        ];
        let bytes = encode_partition_key_columns(&[Value::Uuid(raw)], &schema).unwrap();
        assert_eq!(bytes, raw);
        let decoded = decode_partition_key_columns(&bytes, &schema).unwrap();
        assert_eq!(decoded, vec![("id".to_string(), Value::Uuid(raw))]);
    }

    #[test]
    fn encode_decode_roundtrip_composite() {
        let schema = schema_with_pks(&[("application_id", "text"), ("metric_name", "text")]);
        let values = vec![
            Value::Text("a".to_string()),
            Value::Text("view".to_string()),
        ];
        let bytes = encode_partition_key_columns(&values, &schema).unwrap();
        // [len=1]['a'][0x00][len=4]["view"][0x00]
        assert_eq!(
            bytes,
            vec![0u8, 1, b'a', 0, 0, 4, b'v', b'i', b'e', b'w', 0]
        );
        let decoded = decode_partition_key_columns(&bytes, &schema).unwrap();
        assert_eq!(
            decoded,
            vec![
                ("application_id".to_string(), Value::Text("a".to_string())),
                ("metric_name".to_string(), Value::Text("view".to_string())),
            ]
        );
    }

    #[test]
    fn encode_coerces_bigint_literal_to_int_column() {
        // The SELECT parser emits integer literals as Value::BigInt; an `int`
        // partition key must still encode to the canonical 4-byte form.
        let schema = schema_with_pks(&[("id", "int")]);
        let bytes = encode_partition_key_columns(&[Value::BigInt(5)], &schema).unwrap();
        assert_eq!(bytes, 5i32.to_be_bytes().to_vec());
        let decoded = decode_partition_key_columns(&bytes, &schema).unwrap();
        assert_eq!(decoded, vec![("id".to_string(), Value::Integer(5))]);
    }

    #[test]
    fn encode_column_count_mismatch_errors() {
        let schema = schema_with_pks(&[("a", "text"), ("b", "text")]);
        assert!(encode_partition_key_columns(&[Value::Text("a".to_string())], &schema).is_err());
    }

    #[test]
    fn encode_out_of_range_bigint_for_int_column_errors() {
        // Too large to narrow to i32 -> serialization fails -> caller falls back to scan.
        let schema = schema_with_pks(&[("id", "int")]);
        assert!(encode_partition_key_columns(&[Value::BigInt(i64::MAX)], &schema).is_err());
    }

    /// Drift guard (review follow-up): this module's encoder must stay
    /// byte-for-byte identical to the write engine's `PartitionKey::to_bytes`,
    /// because a write persists keys with one and the read-side bloom/BTI prune
    /// matches with the other. If they ever diverge, a `WHERE pk = ?` could prune
    /// away the very SSTable holding the row. Values are pre-typed so no coercion
    /// is involved — we're comparing the raw serializers and framing directly.
    #[cfg(feature = "write-support")]
    #[test]
    fn encoder_matches_write_engine_partition_key_to_bytes() {
        use crate::storage::write_engine::mutation::PartitionKey;

        let uuid = [
            0u8, 35, 236, 231, 124, 78, 71, 5, 144, 104, 209, 165, 158, 197, 254, 25,
        ];
        let cases: Vec<(Vec<(&str, &str)>, Vec<(&str, Value)>)> = vec![
            (
                vec![("id", "text")],
                vec![("id", Value::Text("k123".to_string()))],
            ),
            (vec![("id", "int")], vec![("id", Value::Integer(42))]),
            (vec![("id", "bigint")], vec![("id", Value::BigInt(-7))]),
            (vec![("id", "uuid")], vec![("id", Value::Uuid(uuid))]),
            (
                vec![("app", "text"), ("metric", "text")],
                vec![
                    ("app", Value::Text("a".to_string())),
                    ("metric", Value::Text("view".to_string())),
                ],
            ),
            (
                vec![("app", "text"), ("part", "int")],
                vec![
                    ("app", Value::Text("svc".to_string())),
                    ("part", Value::Integer(3)),
                ],
            ),
        ];

        for (pks, values) in cases {
            let schema = schema_with_pks(&pks);
            let value_only: Vec<Value> = values.iter().map(|(_, v)| v.clone()).collect();
            let columns: Vec<(String, Value)> = values
                .iter()
                .map(|(n, v)| (n.to_string(), v.clone()))
                .collect();

            let codec_bytes = encode_partition_key_columns(&value_only, &schema).unwrap();
            let write_bytes = PartitionKey::new(columns).to_bytes(&schema).unwrap();
            assert_eq!(
                codec_bytes, write_bytes,
                "codec encoder and write-engine PartitionKey::to_bytes disagree for {pks:?}",
            );
        }
    }
}
