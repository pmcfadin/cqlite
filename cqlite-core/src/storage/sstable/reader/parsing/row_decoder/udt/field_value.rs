//! UDT FIELD-VALUE decoding — the two scalar field decoders, the width rule they
//! share, and the zero-length router that feeds them.
//!
//! Split out of `udt.rs` under the campsite rule (epic #1116): that file is well
//! over the 800-line source target, and issue #3847 rewrote every one of these
//! decoders' fixed-width arms, so this is the responsibility boundary the change
//! already had to touch. `udt.rs` keeps UDT FRAMING (walking the `[i32 BE len]`
//! component headers, nested/registry/inline dispatch); this file owns what a
//! single field's bytes MEAN.
//!
//! # Issue #3847
//!
//! All three items below admit the accepted set `{n, 0}` for a fixed-width
//! scalar, the empty buffer meaning `null`. The rule, its `cassandra-5.0.8`
//! oracle and the census of the five UDT framing sites that must agree on it are
//! stated once in [`super::super::raw_value::fixed_width`]; do not restate them
//! here.

use super::super::*;
// Issue #3847: the ONE statement of the fixed-width read-path width rule; the two
// field decoders below are reconciled TO it.
use super::super::raw_value::fixed_width::{self, FixedWidthCell};

#[cfg(test)]
#[path = "issue_3847_empty_fixed_width_tests.rs"]
mod issue_3847_empty_fixed_width_tests;

impl V5CompressedLegacyParser {
    /// The width rule for a fixed-width scalar UDT FIELD (issue #3847): `{n, 0}`,
    /// checked here IN FULL because a field's slice is already exactly bounded by
    /// its `[i32 BE len]` header and neither field decoder reports consumption for
    /// a later assert to catch an over-width tail.
    ///
    /// A [`FixedWidthCell::Null`] MUST be decoded to [`Value::Null`]. The rule,
    /// its oracle and the five UDT framing sites it reconciles are stated once in
    /// [`super::raw_value::fixed_width`] — read that, not this. `what` and the
    /// singular/plural of `byte` reproduce the pre-#3847 message wording exactly.
    fn require_udt_field_width(data: &[u8], n: usize, what: &str) -> Result<FixedWidthCell> {
        fixed_width::admissible_exactly(data, n).ok_or_else(|| {
            Error::corruption(format!(
                "{} field requires {} byte{}, got {}",
                what,
                n,
                if n == 1 { "" } else { "s" },
                data.len()
            ))
        })
    }

    /// Parse a UDT field value based on its CqlType.
    pub(super) fn parse_udt_field_value(&self, data: &[u8], field_type: &CqlType) -> Result<Value> {
        // #3847: a 0-length field header means "present and EMPTY", which Cassandra's
        // READ path deserializes to null for every fixed-width scalar (oracle:
        // docs/round-artifacts/issue-3847-cassandra-oracle.md). Answer it HERE, from
        // the one type-keyed oracle, so this decoder agrees with the other framing
        // sites for EVERY member of the set — including the types with no per-type arm
        // below, which would otherwise reach `_ =>` and yield an EMPTY BLOB rather than
        // null (roborev job 94: SmallInt, TinyInt, Time and TimeUuid). The per-type arms keep their
        // own `FixedWidthCell::Null` branch: it is the same answer by a second path, it
        // is what the width-classifier contract tests assert, and it must stay correct
        // for any caller reaching an arm directly.
        //
        // NON-EMPTY decoding is deliberately UNTOUCHED. Those same types degrading to
        // `Value::Blob` when non-empty is a real defect, but it is #3631's (PR #3820),
        // not this issue's, and widening it here would change behaviour this issue's
        // corpus census did not measure.
        if data.is_empty() && fixed_width::width_of(field_type).is_some() {
            return Ok(Value::Null);
        }
        match field_type {
            CqlType::Text | CqlType::Ascii => {
                std::str::from_utf8(data)
                    .map_err(|e| Error::corruption(format!("Invalid UTF-8 in UDT field: {}", e)))?;
                Ok(Value::Text(
                    crate::storage::sstable::reader::value_borrow::borrow_active(data),
                ))
            }
            CqlType::Int => match Self::require_udt_field_width(data, 4, "Int")? {
                FixedWidthCell::Null => Ok(Value::Null),
                FixedWidthCell::Bytes => {
                    let v = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                    Ok(Value::Integer(v))
                }
            },
            CqlType::BigInt => match Self::require_udt_field_width(data, 8, "BigInt")? {
                FixedWidthCell::Null => Ok(Value::Null),
                FixedWidthCell::Bytes => {
                    let v = i64::from_be_bytes([
                        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                    ]);
                    Ok(Value::BigInt(v))
                }
            },
            CqlType::Float => match Self::require_udt_field_width(data, 4, "Float")? {
                FixedWidthCell::Null => Ok(Value::Null),
                FixedWidthCell::Bytes => {
                    let bits = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                    Ok(Value::Float32(f32::from_bits(bits)))
                }
            },
            CqlType::Double => match Self::require_udt_field_width(data, 8, "Double")? {
                FixedWidthCell::Null => Ok(Value::Null),
                FixedWidthCell::Bytes => {
                    let bits = u64::from_be_bytes([
                        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                    ]);
                    Ok(Value::Float(f64::from_bits(bits)))
                }
            },
            CqlType::Boolean => match Self::require_udt_field_width(data, 1, "Boolean")? {
                FixedWidthCell::Null => Ok(Value::Null),
                FixedWidthCell::Bytes => Ok(Value::Boolean(data[0] != 0)),
            },
            CqlType::Uuid => match Self::require_udt_field_width(data, 16, "UUID")? {
                FixedWidthCell::Null => Ok(Value::Null),
                FixedWidthCell::Bytes => {
                    let uuid_bytes: [u8; 16] = data[0..16]
                        .try_into()
                        .map_err(|_| Error::corruption("UUID byte conversion failed"))?;
                    Ok(Value::Uuid(uuid_bytes))
                }
            },
            CqlType::Timestamp => match Self::require_udt_field_width(data, 8, "Timestamp")? {
                FixedWidthCell::Null => Ok(Value::Null),
                FixedWidthCell::Bytes => {
                    let millis = i64::from_be_bytes([
                        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                    ]);
                    Ok(Value::Timestamp(millis))
                }
            },
            CqlType::Date => match Self::require_udt_field_width(data, 4, "Date")? {
                FixedWidthCell::Null => Ok(Value::Null),
                FixedWidthCell::Bytes => {
                    let days = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                    Ok(Value::Date(days as i32))
                }
            },
            CqlType::Blob => Ok(Value::Blob(
                crate::storage::sstable::reader::value_borrow::borrow_active(data),
            )),
            CqlType::Inet => Ok(Value::Inet(
                crate::storage::sstable::reader::value_borrow::borrow_active(data),
            )),
            CqlType::Frozen(inner) => {
                // Parse the inner type and wrap in Frozen
                let inner_value = self.parse_udt_field_value(data, inner)?;
                Ok(Value::Frozen(Box::new(inner_value)))
            }
            CqlType::Udt(name, field_defs) => {
                // Nested UDT - recursively parse
                let mut nested_def = UdtTypeDef::new("".to_string(), name.clone());
                for (field_name, field_type) in field_defs {
                    nested_def =
                        nested_def.with_field(field_name.clone(), field_type.clone(), true);
                }
                let dummy_column = crate::schema::Column {
                    name: name.clone(),
                    data_type: "udt".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                };
                let (value, n) = self.parse_udt_value(data, 0, &nested_def, &dummy_column)?;
                // #3811 (finding C): the 4th discarding bounded caller of the pair
                // roborev named; `data` here is one exactly-bounded UDT field.
                Self::require_fully_consumed_raw(n, data.len(), &nested_def.name, "nested UDT")?;
                Ok(value)
            }
            _ => {
                // For other types, return as blob
                tracing::debug!(
                    "V5CompressedLegacy: UDT field type {:?} parsed as blob ({} bytes)",
                    field_type,
                    data.len()
                );
                Ok(Value::Blob(
                    crate::storage::sstable::reader::value_borrow::borrow_active(data),
                ))
            }
        }
    }

    /// What a ZERO-LENGTH UDT field (`[i32 BE len] == 0`) decodes to, per type.
    ///
    /// Issue #3847: for a FIXED-WIDTH scalar that is [`Value::Null`], not an empty
    /// blob — this is the zero-length ROUTER for two of the five UDT framing
    /// sites, and all five must agree; see [`super::raw_value::fixed_width`] for
    /// the rule, its oracle and the site census. The non-scalar rows are
    /// UNCHANGED and are not #3847's subject: an empty `text` is the empty STRING
    /// (Cassandra distinguishes `''` from `null` for a variable-width type), an
    /// empty `blob` the empty blob, an empty collection the empty collection.
    pub(in crate::storage::sstable::reader::parsing::row_decoder) fn create_empty_value_for_type(
        cql_type: &CqlType,
    ) -> Value {
        match cql_type {
            CqlType::Text | CqlType::Ascii | CqlType::Varchar => Value::text(String::new()),
            CqlType::Blob => Value::blob(Vec::new()),
            CqlType::List(_) => Value::List(Vec::new()),
            CqlType::Set(_) => Value::Set(Vec::new()),
            CqlType::Map(_, _) => Value::Map(Vec::new()),
            // The twelve fixed-width scalars, per the oracle above.
            CqlType::Boolean
            | CqlType::TinyInt
            | CqlType::SmallInt
            | CqlType::Int
            | CqlType::BigInt
            | CqlType::Counter
            | CqlType::Float
            | CqlType::Double
            | CqlType::Timestamp
            | CqlType::Date
            | CqlType::Time
            | CqlType::Uuid
            | CqlType::TimeUuid => Value::Null,
            _ => Value::blob(Vec::new()),
        }
    }

    /// Parse a UDT field value without requiring SSTableReader.
    /// This is a simplified version of parse_udt_field_value for use in frozen collection contexts.
    ///
    /// Limitation: Complex nested types (nested UDTs, nested collections) are returned as blobs.
    /// For full UDT support with nested types, use parse_udt_field_value with a reader.
    pub(in crate::storage::sstable::reader::parsing::row_decoder) fn parse_simple_udt_field_value(
        data: &[u8],
        field_type: &CqlType,
    ) -> Result<Value> {
        // #3847: a 0-length field header means "present and EMPTY", which Cassandra's
        // READ path deserializes to null for every fixed-width scalar (oracle:
        // docs/round-artifacts/issue-3847-cassandra-oracle.md). Answer it HERE, from
        // the one type-keyed oracle, so this decoder agrees with the other framing
        // sites for EVERY member of the set — including the types with no per-type arm
        // below, which would otherwise reach `_ =>` and yield an EMPTY BLOB rather than
        // null (roborev job 94: SmallInt, TinyInt, Time and Date). The per-type arms keep their
        // own `FixedWidthCell::Null` branch: it is the same answer by a second path, it
        // is what the width-classifier contract tests assert, and it must stay correct
        // for any caller reaching an arm directly.
        //
        // NON-EMPTY decoding is deliberately UNTOUCHED. Those same types degrading to
        // `Value::Blob` when non-empty is a real defect, but it is #3631's (PR #3820),
        // not this issue's, and widening it here would change behaviour this issue's
        // corpus census did not measure.
        if data.is_empty() && fixed_width::width_of(field_type).is_some() {
            return Ok(Value::Null);
        }
        match field_type {
            CqlType::Text | CqlType::Ascii => {
                std::str::from_utf8(data)
                    .map_err(|e| Error::corruption(format!("Invalid UTF-8 in UDT field: {}", e)))?;
                Ok(Value::Text(
                    crate::storage::sstable::reader::value_borrow::borrow_active(data),
                ))
            }
            CqlType::Int => match Self::require_udt_field_width(data, 4, "Int")? {
                FixedWidthCell::Null => Ok(Value::Null),
                FixedWidthCell::Bytes => {
                    let v = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                    Ok(Value::Integer(v))
                }
            },
            CqlType::BigInt => match Self::require_udt_field_width(data, 8, "BigInt")? {
                FixedWidthCell::Null => Ok(Value::Null),
                FixedWidthCell::Bytes => {
                    let v = i64::from_be_bytes([
                        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                    ]);
                    Ok(Value::BigInt(v))
                }
            },
            CqlType::Boolean => match Self::require_udt_field_width(data, 1, "Boolean")? {
                FixedWidthCell::Null => Ok(Value::Null),
                FixedWidthCell::Bytes => Ok(Value::Boolean(data[0] != 0)),
            },
            CqlType::Float => match Self::require_udt_field_width(data, 4, "Float")? {
                FixedWidthCell::Null => Ok(Value::Null),
                FixedWidthCell::Bytes => {
                    let bits = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                    Ok(Value::Float32(f32::from_bits(bits)))
                }
            },
            CqlType::Double => match Self::require_udt_field_width(data, 8, "Double")? {
                FixedWidthCell::Null => Ok(Value::Null),
                FixedWidthCell::Bytes => {
                    let bits = u64::from_be_bytes([
                        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                    ]);
                    Ok(Value::Float(f64::from_bits(bits)))
                }
            },
            CqlType::Uuid | CqlType::TimeUuid => {
                match Self::require_udt_field_width(data, 16, "UUID")? {
                    FixedWidthCell::Null => Ok(Value::Null),
                    FixedWidthCell::Bytes => {
                        let uuid_bytes: [u8; 16] = data[0..16]
                            .try_into()
                            .map_err(|_| Error::corruption("UUID byte conversion failed"))?;
                        Ok(Value::Uuid(uuid_bytes))
                    }
                }
            }
            CqlType::Timestamp => match Self::require_udt_field_width(data, 8, "Timestamp")? {
                FixedWidthCell::Null => Ok(Value::Null),
                FixedWidthCell::Bytes => {
                    let millis = i64::from_be_bytes([
                        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                    ]);
                    Ok(Value::Timestamp(millis))
                }
            },
            CqlType::Blob => Ok(Value::Blob(
                crate::storage::sstable::reader::value_borrow::borrow_active(data),
            )),
            _ => {
                // For complex types (nested UDTs, collections, etc.), return as blob
                // These require SSTableReader for full parsing
                tracing::debug!(
                    "UDT field type {:?} in frozen context parsed as blob ({} bytes)",
                    field_type,
                    data.len()
                );
                Ok(Value::Blob(
                    crate::storage::sstable::reader::value_borrow::borrow_active(data),
                ))
            }
        }
    }
}
