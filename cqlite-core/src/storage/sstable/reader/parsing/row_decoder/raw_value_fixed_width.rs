//! Fixed-width scalar arms of the bounded raw-value decoder (campsite split of
//! `raw_value.rs`, epic #1116 / issue #3723).
//!
//! Every value here is decoded from a slice that a caller has already bounded
//! by an explicit `[i32 BE len]` element/field prefix, so `data.len()` IS the
//! value's declared length. The width guards therefore belong in these arms —
//! never in a call-site framing walk over element lengths (issue #3612 removed
//! exactly that shape; see the warning in issue #3723).

use super::*;

impl V5CompressedLegacyParser {
    /// The EXACT byte width this decoder admits for a fixed-width CQL short
    /// form in the bounded element/field position, or `None` if `cql_short` is
    /// not a fixed-width type (issue #3723).
    ///
    /// The set of names here is the closed set
    /// [`Self::decode_fixed_width_raw`] decodes, and a drift test in
    /// `raw_value_tests.rs` pins the two together.
    pub(super) fn fixed_width_admissible_width(cql_short: &str) -> Option<usize> {
        let width = match cql_short {
            "int" => 4,
            "bigint" | "counter" => 8,
            "boolean" => 1,
            "uuid" | "timeuuid" => 16,
            "float" => 4,
            "double" => 8,
            "smallint" | "short" => 2,
            "tinyint" | "byte" => 1,
            "timestamp" => 8,
            "date" => 4,
            "time" => 8,
            _ => return None,
        };
        Some(width)
    }

    /// Decode one fixed-width scalar from a fully bounded slice.
    ///
    /// `cql_short` is the canonical lowercase CQL short form (already
    /// normalized from any marshal form by
    /// [`Self::primitive_marshal_to_cql_short`]).
    pub(super) fn decode_fixed_width_raw(
        cql_short: &str,
        data: &[u8],
        column_name: &str,
    ) -> Result<Value> {
        match cql_short {
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
            other => Err(Error::internal(format!(
                "decode_fixed_width_raw called with non-fixed-width type '{}' for '{}'",
                other, column_name
            ))),
        }
    }
}
