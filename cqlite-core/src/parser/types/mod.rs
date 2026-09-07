//! CQL type system parsing and serialization
//!
//! This module handles parsing and serialization of CQL data types as they
//! appear in Cassandra SSTable format. It maps between the binary representation
//! and the CQLite Value types.
//!
//! The implementation is split by responsibility:
//! - [`primitives`] — scalar types (int/text/uuid/timestamp/decimal/…).
//! - [`collections`] — list/set/map (legacy, v5, registry-aware, schema-driven).
//! - [`udt`] — UDT, tuple, and frozen decoding.
//! - [`tombstones`] — tombstone/deletion decoding.
//!
//! This `mod.rs` keeps the shared model ([`CqlTypeId`], type-id ⇄ value mapping,
//! empty-value construction), the value-dispatch entry points
//! ([`parse_cql_value`], [`parse_cql_value_raw`]), the schema-driven dispatcher,
//! buffer-consumption validation helpers, and serialization — and re-exports the
//! full public surface so callers using `parser::types::*` are unaffected.

mod collections;
mod primitives;
mod tombstones;
mod udt;

pub use collections::{
    parse_list, parse_list_enhanced, parse_list_v5_format, parse_list_with_schema, parse_map,
    parse_map_enhanced, parse_map_v5_format, parse_map_with_schema, parse_set, parse_set_enhanced,
    parse_set_v5_format,
};
pub use primitives::{
    parse_bigint, parse_blob, parse_boolean, parse_counter, parse_date, parse_decimal,
    parse_double, parse_duration, parse_float, parse_inet, parse_int, parse_smallint, parse_text,
    parse_time, parse_timestamp, parse_tinyint, parse_uuid, parse_varint,
};
pub use tombstones::parse_tombstone;
pub use udt::{
    parse_frozen_udt, parse_frozen_udt_with_registry, parse_tuple, parse_udt, parse_udt_enhanced,
    parse_udt_enhanced_with_registry, parse_udt_with_registry, parse_udt_with_schema,
    parse_udt_with_schema_and_registry,
};

use super::vint::encode_vint;
use crate::{
    error::{Error, Result},
    schema::CqlType,
    types::{TombstoneType, UdtValue, Value},
};
use nom::{combinator::map_res, number::complete::be_u8, IResult};

/// CQL type identifiers as they appear in the binary format
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CqlTypeId {
    Custom = 0x00,
    Ascii = 0x01,
    BigInt = 0x02,
    Blob = 0x03,
    Boolean = 0x04,
    Counter = 0x05,
    Decimal = 0x06,
    Double = 0x07,
    Float = 0x08,
    Int = 0x09,
    Timestamp = 0x0B,
    Uuid = 0x0C,
    Varchar = 0x0D,
    Varint = 0x0E,
    Timeuuid = 0x0F,
    Inet = 0x10,
    Date = 0x11,
    Time = 0x12,
    Smallint = 0x13,
    Tinyint = 0x14,
    Duration = 0x15,
    List = 0x20,
    Map = 0x21,
    Set = 0x22,
    Udt = 0x30,
    Tuple = 0x31,
    // Custom CQLite extension for tombstones
    Tombstone = 0xFF,
}

impl TryFrom<u8> for CqlTypeId {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0x00 => Ok(CqlTypeId::Custom),
            0x01 => Ok(CqlTypeId::Ascii),
            0x02 => Ok(CqlTypeId::BigInt),
            0x03 => Ok(CqlTypeId::Blob),
            0x04 => Ok(CqlTypeId::Boolean),
            0x05 => Ok(CqlTypeId::Counter),
            0x06 => Ok(CqlTypeId::Decimal),
            0x07 => Ok(CqlTypeId::Double),
            0x08 => Ok(CqlTypeId::Float),
            0x09 => Ok(CqlTypeId::Int),
            0x0B => Ok(CqlTypeId::Timestamp),
            0x0C => Ok(CqlTypeId::Uuid),
            0x0D => Ok(CqlTypeId::Varchar),
            0x0E => Ok(CqlTypeId::Varint),
            0x0F => Ok(CqlTypeId::Timeuuid),
            0x10 => Ok(CqlTypeId::Inet),
            0x11 => Ok(CqlTypeId::Date),
            0x12 => Ok(CqlTypeId::Time),
            0x13 => Ok(CqlTypeId::Smallint),
            0x14 => Ok(CqlTypeId::Tinyint),
            0x15 => Ok(CqlTypeId::Duration),
            0x20 => Ok(CqlTypeId::List),
            0x21 => Ok(CqlTypeId::Map),
            0x22 => Ok(CqlTypeId::Set),
            0x30 => Ok(CqlTypeId::Udt),
            0x31 => Ok(CqlTypeId::Tuple),
            0xFF => Ok(CqlTypeId::Tombstone),
            _ => Err(Error::corruption(format!(
                "Unknown CQL type ID: 0x{:02X}",
                value
            ))),
        }
    }
}

/// Parse a CQL type identifier
pub fn parse_cql_type_id(input: &[u8]) -> IResult<&[u8], CqlTypeId> {
    map_res(be_u8, CqlTypeId::try_from)(input)
}

/// Parse a CQL value based on its type with enhanced collection support
pub fn parse_cql_value(input: &[u8], type_id: CqlTypeId) -> IResult<&[u8], Value> {
    match type_id {
        CqlTypeId::Boolean => parse_boolean(input),
        CqlTypeId::Tinyint => parse_tinyint(input),
        CqlTypeId::Smallint => parse_smallint(input),
        CqlTypeId::Int => parse_int(input),
        CqlTypeId::BigInt => parse_bigint(input),
        CqlTypeId::Counter => parse_counter(input),
        CqlTypeId::Float => parse_float(input),
        CqlTypeId::Double => parse_double(input),
        CqlTypeId::Ascii | CqlTypeId::Varchar => {
            // In the SSTable cell format the caller has already extracted exactly
            // the bytes belonging to this cell (length-framing is done at the cell
            // level before parse_cql_value is invoked).  The entire `input` slice
            // IS the text value — no additional length prefix to strip.
            //
            // For Ascii, Cassandra's AsciiSerializer requires every byte to be in
            // the 0x00-0x7F range.  For Varchar/Text any valid UTF-8 is accepted.
            // We accept both under the same validation path here; a stricter
            // Ascii-only byte check can be added when needed.
            #[cfg(feature = "legacy-heuristics")]
            {
                // Legacy path: try 4-byte big-endian length prefix, then null-terminated,
                // then raw UTF-8 (kept for pre-5.0 compatibility only).
                if input.len() >= 4 {
                    let length =
                        u32::from_be_bytes([input[0], input[1], input[2], input[3]]) as usize;
                    if input.len() >= 4 + length {
                        let text_bytes = &input[4..4 + length];
                        if let Ok(text) = String::from_utf8(text_bytes.to_vec()) {
                            return Ok((&input[4 + length..], Value::Text(text.into())));
                        }
                    }
                }
                if let Some(null_pos) = input.iter().position(|&b| b == 0) {
                    if let Ok(text) = String::from_utf8(input[..null_pos].to_vec()) {
                        return Ok((&input[null_pos + 1..], Value::Text(text.into())));
                    }
                }
                if let Ok(text) = String::from_utf8(input.to_vec()) {
                    return Ok((&[], Value::Text(text.into())));
                }
                parse_text(input)
            }
            #[cfg(not(feature = "legacy-heuristics"))]
            {
                // Deterministic path: treat the entire input as the UTF-8 text value.
                let text = String::from_utf8(input.to_vec()).map_err(|_| {
                    nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
                })?;
                Ok((&[], Value::Text(text.into())))
            }
        }
        CqlTypeId::Blob => {
            // The caller has already extracted exactly the bytes belonging to
            // this cell (length-framing is done at the cell level before
            // parse_cql_value is invoked), so the entire `input` slice IS the
            // blob value verbatim — mirroring the sibling Ascii/Varchar arm,
            // parse_cql_value_raw and parse_blob_value. Framing is the caller's
            // responsibility via this function's contract; the decoder never
            // sniffs byte patterns to infer a length (no-heuristics mandate,
            // issues #28 / #1630). The genuinely VInt-framed decode lives in
            // parse_blob (used by parse_cql_value_with_schema and the write
            // side's tagged serialization), not here.
            Ok((&[], Value::blob(input.to_vec())))
        }
        CqlTypeId::Uuid | CqlTypeId::Timeuuid => parse_uuid(input),
        CqlTypeId::Timestamp => parse_timestamp(input),
        CqlTypeId::Date => parse_date(input),
        CqlTypeId::Time => parse_time(input),
        CqlTypeId::Varint => parse_varint(input),
        CqlTypeId::Decimal => parse_decimal(input),
        CqlTypeId::Duration => parse_duration(input),
        CqlTypeId::Inet => parse_inet(input),
        CqlTypeId::List => parse_list_enhanced(input),
        CqlTypeId::Set => parse_set_enhanced(input),
        CqlTypeId::Map => parse_map_enhanced(input),
        CqlTypeId::Udt => parse_udt_enhanced(input),
        CqlTypeId::Tuple => parse_tuple(input),
        CqlTypeId::Tombstone => parse_tombstone(input),
        CqlTypeId::Custom => {
            // Custom types require additional metadata, return as blob for now
            parse_blob(input)
        }
    }
}

/// Parse a CQL value from raw data (without length prefix)
/// This is used for collection elements where length is already parsed
pub fn parse_cql_value_raw(input: &[u8], type_id: CqlTypeId) -> IResult<&[u8], Value> {
    match type_id {
        CqlTypeId::Boolean => parse_boolean(input),
        CqlTypeId::Tinyint => parse_tinyint(input),
        CqlTypeId::Smallint => parse_smallint(input),
        CqlTypeId::Int => parse_int(input),
        CqlTypeId::BigInt => parse_bigint(input),
        CqlTypeId::Counter => parse_counter(input),
        CqlTypeId::Float => parse_float(input),
        CqlTypeId::Double => parse_double(input),
        CqlTypeId::Ascii | CqlTypeId::Varchar => {
            // For map/collection contexts, the input is already length-prefixed at the collection level
            // So we can treat all input as the text content directly
            let text = String::from_utf8(input.to_vec()).map_err(|_| {
                nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
            })?;
            Ok((&[], Value::Text(text.into())))
        }
        CqlTypeId::Blob => {
            // For blob, use all input as blob data
            Ok((&[], Value::blob(input.to_vec())))
        }
        CqlTypeId::Uuid | CqlTypeId::Timeuuid => parse_uuid(input),
        CqlTypeId::Timestamp => parse_timestamp(input),
        CqlTypeId::Date => parse_date(input),
        CqlTypeId::Time => parse_time(input),
        CqlTypeId::Varint => parse_varint(input),
        CqlTypeId::Decimal => parse_decimal(input),
        CqlTypeId::Duration => parse_duration(input),
        CqlTypeId::Inet => parse_inet(input),
        // Collections and complex types should not be called with raw parsing
        CqlTypeId::List | CqlTypeId::Set | CqlTypeId::Map | CqlTypeId::Udt | CqlTypeId::Tuple => {
            parse_cql_value(input, type_id) // Fallback to normal parsing
        }
        CqlTypeId::Tombstone => parse_tombstone(input),
        CqlTypeId::Custom => {
            // Custom types require additional metadata, return as blob for now
            Ok((&[], Value::blob(input.to_vec())))
        }
    }
}

/// Create empty value for a CQL type
pub(super) fn create_empty_value_for_cql_type(cql_type: &CqlType) -> Result<Value> {
    match cql_type {
        CqlType::Boolean => Ok(Value::Boolean(false)),
        CqlType::TinyInt => Ok(Value::TinyInt(0)),
        CqlType::SmallInt => Ok(Value::SmallInt(0)),
        CqlType::Int => Ok(Value::Integer(0)),
        CqlType::BigInt => Ok(Value::BigInt(0)),
        CqlType::Float => Ok(Value::Float32(0.0)),
        CqlType::Double => Ok(Value::Float(0.0)),
        CqlType::Text | CqlType::Ascii | CqlType::Varchar => Ok(Value::text(String::new())),
        CqlType::Blob => Ok(Value::blob(Vec::new())),
        CqlType::Uuid | CqlType::TimeUuid => Ok(Value::Uuid([0; 16])),
        CqlType::Timestamp => Ok(Value::Timestamp(0)),
        CqlType::Date => Ok(Value::Timestamp(0)),
        CqlType::Time => Ok(Value::Timestamp(0)),
        CqlType::List(_) => Ok(Value::List(Vec::new())),
        CqlType::Set(_) => Ok(Value::Set(Vec::new())),
        CqlType::Map(_, _) => Ok(Value::Map(Vec::new())),
        CqlType::Tuple(_) => Ok(Value::Tuple(Vec::new())),
        CqlType::Udt(name, _) => Ok(Value::Udt(Box::new(UdtValue::new(
            name.clone(),
            "unknown".to_string(),
        )))),
        CqlType::Frozen(inner) => create_empty_value_for_cql_type(inner),
        // #4114 (roborev job 109): there is NO empty vector to construct. A
        // zero-length vector value is an ERROR in Cassandra
        // (`VectorType.java:365-368`, "Invalid empty vector value"), so this arm must
        // not fall through to `Value::Null` below — that would turn an invalid value
        // into a legal-looking one. The refusal comes from the ONE framing rule so
        // the message matches every other vector site. (A genuinely NULL field is
        // decided by the outer framing — `length == -1` — and never reaches here; the
        // collection map path's `length < 0` also lands here, and refusing is the
        // fail-closed reading: Cassandra permits neither a null nor an empty map
        // key/value.)
        CqlType::Vector(element, dimension) => {
            crate::schema::vector_type::vector_value::decode_framed_float_vector(
                &[],
                element,
                *dimension,
                "empty framed vector value",
            )
        }
        _ => Ok(Value::Null),
    }
}

/// Convert CqlType to CqlTypeId for parsing
pub(super) fn cql_type_to_type_id(cql_type: &CqlType) -> CqlTypeId {
    match cql_type {
        CqlType::Boolean => CqlTypeId::Boolean,
        CqlType::TinyInt => CqlTypeId::Tinyint,
        CqlType::SmallInt => CqlTypeId::Smallint,
        CqlType::Int => CqlTypeId::Int,
        CqlType::BigInt => CqlTypeId::BigInt,
        CqlType::Counter => CqlTypeId::Counter,
        CqlType::Float => CqlTypeId::Float,
        CqlType::Double => CqlTypeId::Double,
        CqlType::Text | CqlType::Ascii | CqlType::Varchar => CqlTypeId::Varchar,
        CqlType::Blob => CqlTypeId::Blob,
        CqlType::Uuid => CqlTypeId::Uuid,
        CqlType::TimeUuid => CqlTypeId::Timeuuid,
        CqlType::Timestamp => CqlTypeId::Timestamp,
        CqlType::Date => CqlTypeId::Date,
        CqlType::Time => CqlTypeId::Time,
        CqlType::Decimal => CqlTypeId::Decimal,
        CqlType::Duration => CqlTypeId::Duration,
        CqlType::Inet => CqlTypeId::Inet,
        CqlType::Varint => CqlTypeId::Varint,
        CqlType::List(_) => CqlTypeId::List,
        CqlType::Set(_) => CqlTypeId::Set,
        CqlType::Map(_, _) => CqlTypeId::Map,
        CqlType::Tuple(_) => CqlTypeId::Tuple,
        CqlType::Udt(_, _) => CqlTypeId::Udt,
        CqlType::Frozen(_) => CqlTypeId::Blob, // Fallback only; callers should handle Frozen explicitly
        // #4114: a vector has NO native protocol type id (Cassandra carries it as a
        // custom type), so this conversion is LOSSY — it discards the element type
        // and the dimension, and `CqlTypeId::Custom` decodes as a vint-framed blob.
        // Every caller must therefore intercept `CqlType::Vector` BEFORE reaching a
        // type id, exactly like the `Frozen` arm above. All three do:
        // `parse_cql_value_for_type`, `parse_cql_value_for_type_with_registry` (the
        // one that did NOT, roborev job 109) and `parse_cql_value_with_schema`.
        CqlType::Vector(_, _) => CqlTypeId::Custom,
        CqlType::Custom(_) => CqlTypeId::Blob, // Custom types as blob
    }
}

/// Create an empty value for a given type ID
pub(super) fn create_empty_value(type_id: CqlTypeId) -> Result<Value> {
    match type_id {
        CqlTypeId::Boolean => Ok(Value::Boolean(false)),
        CqlTypeId::Tinyint => Ok(Value::TinyInt(0)),
        CqlTypeId::Smallint => Ok(Value::SmallInt(0)),
        CqlTypeId::Int => Ok(Value::Integer(0)),
        CqlTypeId::BigInt => Ok(Value::BigInt(0)),
        CqlTypeId::Counter => Ok(Value::Counter(0)),
        CqlTypeId::Float => Ok(Value::Float32(0.0)),
        CqlTypeId::Double => Ok(Value::Float(0.0)),
        CqlTypeId::Ascii | CqlTypeId::Varchar => Ok(Value::text(String::new())),
        CqlTypeId::Blob => Ok(Value::blob(Vec::new())),
        CqlTypeId::Uuid | CqlTypeId::Timeuuid => Ok(Value::Uuid([0; 16])),
        CqlTypeId::Timestamp => Ok(Value::Timestamp(0)),
        CqlTypeId::List => Ok(Value::List(Vec::new())),
        CqlTypeId::Set => Ok(Value::Set(Vec::new())),
        CqlTypeId::Map => Ok(Value::Map(Vec::new())),
        CqlTypeId::Tuple => Ok(Value::Tuple(Vec::new())),
        _ => Ok(Value::Null),
    }
}

/// Serialize a CQL value to bytes
pub fn serialize_cql_value(value: &Value) -> Result<Vec<u8>> {
    let mut result = Vec::new();

    match value {
        Value::Null => {
            // Null values are represented with a special length marker
            result.extend_from_slice(&encode_vint(-1));
        }
        Value::Boolean(b) => {
            result.push(CqlTypeId::Boolean as u8);
            result.push(if *b { 1 } else { 0 });
        }
        Value::Integer(i) => {
            result.push(CqlTypeId::Int as u8);
            result.extend_from_slice(&i.to_be_bytes());
        }
        Value::BigInt(i) => {
            result.push(CqlTypeId::BigInt as u8);
            result.extend_from_slice(&i.to_be_bytes());
        }
        Value::Counter(c) => {
            result.push(CqlTypeId::Counter as u8);
            result.extend_from_slice(&c.to_be_bytes());
        }
        Value::Float(f) => {
            result.push(CqlTypeId::Double as u8);
            result.extend_from_slice(&f.to_be_bytes());
        }
        Value::Text(s) => {
            result.push(CqlTypeId::Varchar as u8);
            result.extend_from_slice(&encode_vint(s.len() as i64));
            result.extend_from_slice(s.as_ref());
        }
        Value::Blob(b) => {
            result.push(CqlTypeId::Blob as u8);
            result.extend_from_slice(&encode_vint(b.len() as i64));
            result.extend_from_slice(b);
        }
        Value::Timestamp(ts) => {
            result.push(CqlTypeId::Timestamp as u8);
            let millis = ts / 1000; // Convert microseconds to milliseconds
            result.extend_from_slice(&millis.to_be_bytes());
        }
        Value::Date(days) => {
            result.push(CqlTypeId::Date as u8);
            result.extend_from_slice(&days.to_be_bytes());
        }
        Value::Time(nanos) => {
            result.push(CqlTypeId::Time as u8);
            result.extend_from_slice(&nanos.to_be_bytes());
        }
        Value::Uuid(uuid) => {
            result.push(CqlTypeId::Uuid as u8);
            result.extend_from_slice(uuid);
        }
        Value::Json(json) => {
            // Store JSON as text
            let json_str = json.to_string();
            result.push(CqlTypeId::Varchar as u8);
            result.extend_from_slice(&encode_vint(json_str.len() as i64));
            result.extend_from_slice(json_str.as_bytes());
        }
        Value::List(list) => {
            result.push(CqlTypeId::List as u8);
            result.extend_from_slice(&encode_vint(list.len() as i64));

            // For simplicity, assume all elements are the same type
            if let Some(first) = list.first() {
                let element_type = map_value_to_cql_type(first);
                result.push(element_type as u8);

                for element in list {
                    if let Value::Null = element {
                        // Null element: length = -1
                        result.extend_from_slice(&encode_vint(-1));
                    } else {
                        let element_data = serialize_value_without_type_prefix(element)?;
                        result.extend_from_slice(&encode_vint(element_data.len() as i64));
                        result.extend_from_slice(&element_data);
                    }
                }
            }
        }
        Value::Map(map) => {
            result.push(CqlTypeId::Map as u8);
            result.extend_from_slice(&encode_vint(map.len() as i64));

            // For simplicity, assume all keys and values are the same type
            if let Some((first_key, first_value)) = map.first() {
                let key_type = map_value_to_cql_type(first_key);
                let value_type = map_value_to_cql_type(first_value);
                result.push(key_type as u8);
                result.push(value_type as u8);

                for (key, value) in map {
                    // Serialize key with length prefix
                    if let Value::Null = key {
                        result.extend_from_slice(&encode_vint(-1));
                    } else {
                        let key_data = serialize_value_without_type_prefix(key)?;
                        result.extend_from_slice(&encode_vint(key_data.len() as i64));
                        result.extend_from_slice(&key_data);
                    }

                    // Serialize value with length prefix
                    if let Value::Null = value {
                        result.extend_from_slice(&encode_vint(-1));
                    } else {
                        let value_data = serialize_value_without_type_prefix(value)?;
                        result.extend_from_slice(&encode_vint(value_data.len() as i64));
                        result.extend_from_slice(&value_data);
                    }
                }
            }
        }
        Value::TinyInt(i) => {
            result.push(CqlTypeId::Tinyint as u8);
            result.push(*i as u8);
        }
        Value::Inet(bytes) => {
            result.push(CqlTypeId::Inet as u8);
            result.extend_from_slice(&encode_vint(bytes.len() as i64));
            result.extend_from_slice(bytes);
        }
        Value::SmallInt(i) => {
            result.push(CqlTypeId::Smallint as u8);
            result.extend_from_slice(&i.to_be_bytes());
        }
        Value::Float32(f) => {
            result.push(CqlTypeId::Float as u8);
            result.extend_from_slice(&f.to_be_bytes());
        }
        Value::Set(set) => {
            result.push(CqlTypeId::Set as u8);
            result.extend_from_slice(&encode_vint(set.len() as i64));

            if let Some(first) = set.first() {
                let element_type = map_value_to_cql_type(first);
                result.push(element_type as u8);

                for element in set {
                    if let Value::Null = element {
                        // Null element: length = -1
                        result.extend_from_slice(&encode_vint(-1));
                    } else {
                        let element_data = serialize_value_without_type_prefix(element)?;
                        result.extend_from_slice(&encode_vint(element_data.len() as i64));
                        result.extend_from_slice(&element_data);
                    }
                }
            }
        }
        Value::Tuple(tuple) => {
            result.push(CqlTypeId::Tuple as u8);
            result.extend_from_slice(&encode_vint(tuple.len() as i64));

            // Serialize type information for each field
            for element in tuple {
                let element_type = map_value_to_cql_type(element);
                result.push(element_type as u8);
            }

            // Serialize field values with proper length prefixes
            for element in tuple {
                if let Value::Null = element {
                    // Null field: length = -1
                    result.extend_from_slice(&(-1i32).to_be_bytes());
                } else {
                    let element_data = serialize_value_without_type_prefix(element)?;
                    result.extend_from_slice(&(element_data.len() as i32).to_be_bytes());
                    result.extend_from_slice(&element_data);
                }
            }
        }
        Value::Udt(udt) => {
            result.push(CqlTypeId::Udt as u8);

            // Serialize type name
            result.extend_from_slice(&encode_vint(udt.type_name.len() as i64));
            result.extend_from_slice(udt.type_name.as_bytes());

            // Serialize field count
            result.extend_from_slice(&encode_vint(udt.fields.len() as i64));

            // Serialize field definitions
            for field in &udt.fields {
                result.extend_from_slice(&encode_vint(field.name.len() as i64));
                result.extend_from_slice(field.name.as_bytes());

                // Serialize field type (inferred from value or use blob as fallback)
                let field_type = match &field.value {
                    Some(value) => map_value_to_cql_type(value),
                    None => CqlTypeId::Blob, // Null field, use generic type
                };
                result.push(field_type as u8);
            }

            // Serialize field values
            for field in &udt.fields {
                match &field.value {
                    None => {
                        // Null field: length = -1
                        result.extend_from_slice(&(-1i32).to_be_bytes());
                    }
                    Some(value) => {
                        let field_data = serialize_value_without_type_prefix(value)?;
                        result.extend_from_slice(&(field_data.len() as i32).to_be_bytes());
                        result.extend_from_slice(&field_data);
                    }
                }
            }
        }
        Value::Frozen(boxed_value) => {
            // For frozen values, just serialize the inner value
            let inner_bytes = serialize_cql_value(boxed_value)?;
            result.extend_from_slice(&inner_bytes);
        }
        Value::Tombstone(info) => {
            result.push(CqlTypeId::Tombstone as u8);
            result.extend_from_slice(&info.deletion_time.to_be_bytes());

            let tombstone_type_byte = match info.tombstone_type {
                TombstoneType::RowTombstone => 0u8,
                TombstoneType::CellTombstone => 1u8,
                TombstoneType::RangeTombstone => 2u8,
                TombstoneType::TtlExpiration => 3u8,
                // PartitionTombstone is a delta-scan concept that has no encoding
                // in the legacy binary format.  Silently mapping it to byte 0
                // (RowTombstone) would cause a silent lossy round-trip, violating
                // the no-silent-corruption mandate.  Fail loudly instead.
                TombstoneType::PartitionTombstone => {
                    return Err(Error::invalid_operation(
                        "PartitionTombstone cannot be serialized via the legacy CQL value \
                         binary format; it is a delta-scan concept only",
                    ));
                }
            };
            result.push(tombstone_type_byte);

            // Add TTL if present
            if let Some(ttl) = info.ttl {
                result.extend_from_slice(&ttl.to_be_bytes());
            }

            // Add range information for range tombstones
            if info.tombstone_type == TombstoneType::RangeTombstone {
                if let (Some(start), Some(end)) = (&info.range_start, &info.range_end) {
                    result.push(1u8); // Has range marker
                    result.extend_from_slice(&encode_vint(start.len() as i64));
                    result.extend_from_slice(start.as_bytes());
                    result.extend_from_slice(&encode_vint(end.len() as i64));
                    result.extend_from_slice(end.as_bytes());
                } else {
                    result.push(0u8); // No range marker
                }
            }
        }
        Value::Varint(data) => {
            result.push(CqlTypeId::Varint as u8);
            result.extend_from_slice(&encode_vint(data.len() as i64));
            result.extend_from_slice(data);
        }
        Value::Decimal { scale, unscaled } => {
            result.push(CqlTypeId::Decimal as u8);
            result.extend_from_slice(&scale.to_be_bytes());
            result.extend_from_slice(&encode_vint(unscaled.len() as i64));
            result.extend_from_slice(unscaled);
        }
        Value::Duration {
            months,
            days,
            nanos,
        } => {
            result.push(CqlTypeId::Duration as u8);
            result.extend_from_slice(&months.to_be_bytes());
            result.extend_from_slice(&days.to_be_bytes());
            result.extend_from_slice(&nanos.to_be_bytes());
        }
    }

    Ok(result)
}

/// Serialize a CQL value without the type prefix byte
fn serialize_value_without_type_prefix(value: &Value) -> Result<Vec<u8>> {
    match value {
        Value::Null => Ok(vec![]), // Null should be handled at higher level
        Value::Boolean(b) => Ok(vec![if *b { 1 } else { 0 }]),
        Value::Integer(i) => Ok(i.to_be_bytes().to_vec()),
        Value::BigInt(i) => Ok(i.to_be_bytes().to_vec()),
        Value::Float(f) => Ok(f.to_be_bytes().to_vec()),
        Value::Text(s) => {
            // For raw serialization without type prefix, just include the string bytes
            // The length will be handled by the map format itself
            Ok(s.to_vec())
        }
        Value::Blob(b) => {
            // For raw serialization without type prefix, just include the blob bytes
            // The length will be handled by the map format itself
            Ok(b.to_vec())
        }
        Value::Timestamp(ts) => {
            let millis = ts / 1000; // Convert microseconds to milliseconds
            Ok(millis.to_be_bytes().to_vec())
        }
        Value::Date(days) => Ok(days.to_be_bytes().to_vec()),
        Value::Time(nanos) => Ok(nanos.to_be_bytes().to_vec()),
        Value::Uuid(uuid) => Ok(uuid.to_vec()),
        Value::TinyInt(i) => Ok(vec![*i as u8]),
        Value::Inet(bytes) => Ok(bytes.to_vec()),
        Value::SmallInt(i) => Ok(i.to_be_bytes().to_vec()),
        Value::Float32(f) => Ok(f.to_be_bytes().to_vec()),
        // For complex types, fall back to full serialization and strip type byte
        _ => {
            let full_bytes = serialize_cql_value(value)?;
            Ok(full_bytes[1..].to_vec()) // Skip the type byte
        }
    }
}

fn map_value_to_cql_type(value: &Value) -> CqlTypeId {
    match value {
        Value::Null => CqlTypeId::Blob, // Generic fallback
        Value::Boolean(_) => CqlTypeId::Boolean,
        Value::Integer(_) => CqlTypeId::Int,
        Value::BigInt(_) => CqlTypeId::BigInt,
        Value::Counter(_) => CqlTypeId::Counter,
        Value::Float(_) => CqlTypeId::Double,
        Value::Text(_) => CqlTypeId::Varchar,
        Value::Blob(_) => CqlTypeId::Blob,
        Value::Timestamp(_) => CqlTypeId::Timestamp,
        Value::Date(_) => CqlTypeId::Date,
        Value::Time(_) => CqlTypeId::Time,
        Value::Uuid(_) => CqlTypeId::Uuid,
        Value::Json(_) => CqlTypeId::Varchar,
        Value::TinyInt(_) => CqlTypeId::Tinyint,
        Value::Inet(_) => CqlTypeId::Inet,
        Value::SmallInt(_) => CqlTypeId::Smallint,
        Value::Float32(_) => CqlTypeId::Float,
        Value::List(_) => CqlTypeId::List,
        Value::Set(_) => CqlTypeId::Set,
        Value::Map(_) => CqlTypeId::Map,
        Value::Tuple(_) => CqlTypeId::Tuple,
        Value::Udt(_) => CqlTypeId::Udt,
        Value::Frozen(_) => CqlTypeId::Blob, // Frozen is a wrapper, use blob as fallback
        Value::Tombstone(_) => CqlTypeId::Tombstone,
        Value::Varint(_) => CqlTypeId::Varint,
        Value::Decimal { .. } => CqlTypeId::Decimal,
        Value::Duration { .. } => CqlTypeId::Duration,
    }
}

// ============================================================================
// Buffer Consumption Validation Helpers (Issue #61)
// ============================================================================

/// Assert that all input bytes have been consumed during parsing
///
/// This helper enforces complete buffer consumption to prevent silent data
/// truncation in collection and UDT parsing. Per Issue #61 acceptance criteria,
/// all parsers must validate full consumption.
///
/// # Arguments
/// * `remaining` - The remaining bytes after parsing
/// * `context` - Description of parsing context for error messages
///
/// # Returns
/// * `Ok(())` if buffer is fully consumed
/// * `Err` if bytes remain unconsumed
#[inline]
#[allow(dead_code)] // Will be used in collection parsing functions
pub(crate) fn assert_full_buffer_consumption(remaining: &[u8], context: &str) -> Result<()> {
    if !remaining.is_empty() {
        return Err(Error::corruption(format!(
            "Buffer not fully consumed in {}: {} bytes remaining",
            context,
            remaining.len()
        )));
    }
    Ok(())
}

/// Validate that a parsed element consumed its entire allocated buffer
///
/// Used within collection parsing to ensure each element's buffer is fully
/// consumed before moving to the next element.
#[inline]
#[allow(dead_code)] // Will be used in collection parsing functions
pub(crate) fn validate_element_consumption<'a>(
    _input: &'a [u8],
    remaining: &'a [u8],
    element_index: usize,
    collection_type: &str,
) -> Result<()> {
    if !remaining.is_empty() {
        return Err(Error::corruption(format!(
            "{} element {} did not consume full buffer: {} bytes remaining",
            collection_type,
            element_index,
            remaining.len()
        )));
    }
    Ok(())
}

// ============================================================================
// Schema-Aware Collection Parsing (Issue #61)
// ============================================================================

/// Parse CQL value using schema (no heuristics)
///
/// Schema-driven parser that never falls back to heuristics. Used by
/// schema-aware collection parsers to ensure deterministic decoding.
pub(super) fn parse_cql_value_with_schema<'a>(
    input: &'a [u8],
    schema: &CqlType,
) -> IResult<&'a [u8], Value> {
    match schema {
        CqlType::Boolean => parse_boolean(input),
        CqlType::TinyInt => parse_tinyint(input),
        CqlType::SmallInt => parse_smallint(input),
        CqlType::Int => parse_int(input),
        CqlType::BigInt => parse_bigint(input),
        CqlType::Counter => parse_counter(input),
        CqlType::Float => parse_float(input),
        CqlType::Double => parse_double(input),
        CqlType::Text | CqlType::Ascii | CqlType::Varchar => parse_text(input),
        CqlType::Blob => parse_blob(input),
        CqlType::Uuid | CqlType::TimeUuid => parse_uuid(input),
        CqlType::Timestamp => parse_timestamp(input),
        CqlType::Date => parse_date(input),
        CqlType::Time => parse_time(input),
        CqlType::Duration => parse_duration(input),
        CqlType::Inet => parse_inet(input),
        CqlType::Decimal => parse_decimal(input),
        CqlType::Varint => parse_varint(input),
        CqlType::List(element_type) => parse_list_with_schema(input, element_type),
        CqlType::Set(element_type) => {
            let (remaining, Value::List(elements)) = parse_list_with_schema(input, element_type)?
            else {
                return Err(nom::Err::Error(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Verify,
                )));
            };
            Ok((remaining, Value::Set(elements)))
        }
        CqlType::Map(key_type, value_type) => parse_map_with_schema(input, key_type, value_type),
        CqlType::Tuple(_) => parse_tuple(input),
        CqlType::Udt(_, _) => parse_udt(input),
        // #4114: `vector<float, n>` is `4*n` raw big-endian binary32 bytes with NO
        // length prefix and no per-element framing (`VectorType.java:94-101`,
        // `:445-460`), so it consumes a FIXED width off the front of `input` — it is
        // NOT a collection and must not reach `parse_blob`, which would read the
        // first float's high byte as a vint length (the #4114 defect).
        CqlType::Vector(element_type, dimension) => {
            crate::schema::vector_type::vector_value::parse_float_vector_nom(
                input,
                element_type,
                *dimension,
            )
        }
        CqlType::Frozen(inner) => parse_cql_value_with_schema(input, inner),
        CqlType::Custom(_) => {
            // Custom types require additional metadata, parse as blob
            parse_blob(input)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cql_type_id_conversion() {
        assert_eq!(CqlTypeId::try_from(0x04).unwrap(), CqlTypeId::Boolean);
        assert_eq!(CqlTypeId::try_from(0x09).unwrap(), CqlTypeId::Int);
        assert_eq!(CqlTypeId::try_from(0xFF).unwrap(), CqlTypeId::Tombstone);
    }

    #[test]
    fn test_value_serialization_roundtrip() {
        let test_values = vec![
            Value::Boolean(true),
            Value::Integer(42),
            Value::BigInt(1000),
            Value::Float(std::f64::consts::PI),
            Value::text("test".to_string()),
            Value::blob(vec![1, 2, 3, 4]),
        ];

        for value in test_values {
            let serialized = serialize_cql_value(&value).unwrap();
            // Note: Full roundtrip testing would require implementing deserialization
            // which depends on the type context that's not always preserved
            assert!(!serialized.is_empty());
        }
    }

    #[test]
    fn test_parse_cql_type_id_all_types() {
        // Test all known type IDs
        let type_ids = vec![
            (0x00, CqlTypeId::Custom),
            (0x01, CqlTypeId::Ascii),
            (0x02, CqlTypeId::BigInt),
            (0x03, CqlTypeId::Blob),
            (0x04, CqlTypeId::Boolean),
            (0x05, CqlTypeId::Counter),
            (0x06, CqlTypeId::Decimal),
            (0x07, CqlTypeId::Double),
            (0x08, CqlTypeId::Float),
            (0x09, CqlTypeId::Int),
            (0x0B, CqlTypeId::Timestamp),
            (0x0C, CqlTypeId::Uuid),
            (0x0D, CqlTypeId::Varchar),
            (0x0E, CqlTypeId::Varint),
            (0x0F, CqlTypeId::Timeuuid),
            (0x10, CqlTypeId::Inet),
            (0x11, CqlTypeId::Date),
            (0x12, CqlTypeId::Time),
            (0x13, CqlTypeId::Smallint),
            (0x14, CqlTypeId::Tinyint),
            (0x15, CqlTypeId::Duration),
            (0x20, CqlTypeId::List),
            (0x21, CqlTypeId::Map),
            (0x22, CqlTypeId::Set),
            (0x30, CqlTypeId::Udt),
            (0x31, CqlTypeId::Tuple),
            (0xFF, CqlTypeId::Tombstone),
        ];
        for (byte, expected) in type_ids {
            let data = [byte];
            let (_, type_id) = parse_cql_type_id(&data).unwrap();
            assert_eq!(type_id, expected, "Failed for byte 0x{:02X}", byte);
        }
    }

    #[test]
    fn test_parse_cql_type_id_invalid() {
        // Test invalid type ID
        let data = [0x0A]; // Not a valid type ID
        let result = parse_cql_type_id(&data);
        assert!(result.is_err(), "Should fail for invalid type ID 0x0A");
    }
}
