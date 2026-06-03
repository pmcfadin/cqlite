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
}
