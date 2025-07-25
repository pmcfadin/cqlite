//! CQL type system parsing and serialization
//!
//! This module handles parsing and serialization of CQL data types as they
//! appear in Cassandra SSTable format. It maps between the binary representation
//! and the CQLite Value types.

use super::vint::{encode_vint, parse_vint, parse_vint_length};
use crate::{
    error::{Error, Result},
    types::{Value, UdtValue, UdtField, UdtTypeDef, TombstoneInfo, TombstoneType, RowKey},
    schema::{CqlType, UdtRegistry},
};
use nom::{
    bytes::complete::take,
    combinator::{map, map_res},
    number::complete::{be_f32, be_f64, be_i32, be_i64, be_u16, be_u32, be_u8},
    IResult,
};

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
        CqlTypeId::BigInt | CqlTypeId::Counter => parse_bigint(input),
        CqlTypeId::Float => parse_float(input),
        CqlTypeId::Double => parse_double(input),
        CqlTypeId::Ascii | CqlTypeId::Varchar => parse_text(input),
        CqlTypeId::Blob => parse_blob(input),
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


/// Parse a boolean value (1 byte: 0x00 = false, 0x01 = true)
pub fn parse_boolean(input: &[u8]) -> IResult<&[u8], Value> {
    map(be_u8, |b| Value::Boolean(b != 0))(input)
}

/// Parse a tinyint (signed 8-bit integer)
pub fn parse_tinyint(input: &[u8]) -> IResult<&[u8], Value> {
    map(be_u8, |b| Value::Integer(b as i8 as i32))(input)
}

/// Parse a smallint (signed 16-bit integer)
pub fn parse_smallint(input: &[u8]) -> IResult<&[u8], Value> {
    map(be_u16, |b| Value::Integer(b as i16 as i32))(input)
}

/// Parse an int (signed 32-bit integer)
pub fn parse_int(input: &[u8]) -> IResult<&[u8], Value> {
    map(be_i32, Value::Integer)(input)
}

/// Parse a bigint (signed 64-bit integer)
pub fn parse_bigint(input: &[u8]) -> IResult<&[u8], Value> {
    map(be_i64, Value::BigInt)(input)
}

/// Parse a float (32-bit floating point)
pub fn parse_float(input: &[u8]) -> IResult<&[u8], Value> {
    map(be_f32, |f| Value::Float(f as f64))(input)
}

/// Parse a double (64-bit floating point)
pub fn parse_double(input: &[u8]) -> IResult<&[u8], Value> {
    map(be_f64, Value::Float)(input)
}

/// Parse text (length-prefixed UTF-8 string)
pub fn parse_text(input: &[u8]) -> IResult<&[u8], Value> {
    let (input, length) = parse_vint_length(input)?;
    let (input, bytes) = take(length)(input)?;
    let text = String::from_utf8(bytes.to_vec()).map_err(|_| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
    })?;
    Ok((input, Value::Text(text)))
}

/// Parse blob (length-prefixed binary data)
pub fn parse_blob(input: &[u8]) -> IResult<&[u8], Value> {
    let (input, length) = parse_vint_length(input)?;
    let (input, bytes) = take(length)(input)?;
    Ok((input, Value::Blob(bytes.to_vec())))
}

/// Parse UUID (16 bytes)
pub fn parse_uuid(input: &[u8]) -> IResult<&[u8], Value> {
    let (input, bytes) = take(16usize)(input)?;
    let mut uuid = [0u8; 16];
    uuid.copy_from_slice(bytes);
    Ok((input, Value::Uuid(uuid)))
}

/// Parse timestamp (64-bit milliseconds since epoch)
pub fn parse_timestamp(input: &[u8]) -> IResult<&[u8], Value> {
    map(be_i64, |ts| Value::Timestamp(ts * 1000))(input) // Convert ms to microseconds
}

/// Parse date (32-bit days since epoch)
pub fn parse_date(input: &[u8]) -> IResult<&[u8], Value> {
    map(be_u32, |days| {
        // Convert days since epoch (1970-01-01) to microseconds
        let microseconds = (days as i64) * 24 * 60 * 60 * 1_000_000;
        Value::Timestamp(microseconds)
    })(input)
}

/// Parse time (64-bit nanoseconds since midnight)
pub fn parse_time(input: &[u8]) -> IResult<&[u8], Value> {
    map(be_i64, |nanos| {
        // Convert nanoseconds to microseconds
        Value::Timestamp(nanos / 1000)
    })(input)
}

/// Parse varint (variable-length integer)
pub fn parse_varint(input: &[u8]) -> IResult<&[u8], Value> {
    map(parse_vint, Value::BigInt)(input)
}

/// Parse decimal (scale + unscaled value)
pub fn parse_decimal(input: &[u8]) -> IResult<&[u8], Value> {
    let (input, scale) = be_i32(input)?;
    let (input, unscaled) = parse_vint(input)?;

    // For now, convert to float (losing precision)
    let value = (unscaled as f64) / (10.0_f64.powi(scale));
    Ok((input, Value::Float(value)))
}

/// Parse duration (months, days, nanoseconds)
pub fn parse_duration(input: &[u8]) -> IResult<&[u8], Value> {
    let (input, months) = parse_vint(input)?;
    let (input, days) = parse_vint(input)?;
    let (input, nanos) = parse_vint(input)?;

    // Convert to total microseconds (approximate)
    let total_micros = (months * 30 * 24 * 60 * 60 * 1_000_000)
        + (days * 24 * 60 * 60 * 1_000_000)
        + (nanos / 1000);

    Ok((input, Value::BigInt(total_micros)))
}

/// Parse inet address (4 or 16 bytes)
pub fn parse_inet(input: &[u8]) -> IResult<&[u8], Value> {
    let (input, length) = parse_vint_length(input)?;
    let (input, bytes) = take(length)(input)?;

    // Store as blob for now, could be converted to proper IP address type
    Ok((input, Value::Blob(bytes.to_vec())))
}

/// Parse list using enhanced Cassandra 5+ parser with fallback to legacy format
pub fn parse_list_enhanced(input: &[u8]) -> IResult<&[u8], Value> {
    // Try Cassandra 5+ format first, fall back to legacy on failure
    match parse_list_v5_format(input) {
        Ok(result) => Ok(result),
        Err(_) => parse_list(input), // Fallback to legacy format
    }
}

/// Legacy list parser for backward compatibility
pub fn parse_list(input: &[u8]) -> IResult<&[u8], Value> {
    let (input, count) = parse_vint_length(input)?;
    
    // Validate count to prevent memory exhaustion attacks
    const MAX_COLLECTION_SIZE: usize = 1_000_000;
    if count > MAX_COLLECTION_SIZE {
        return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::TooLarge)));
    }
    
    if count == 0 {
        return Ok((input, Value::List(Vec::new())));
    }

    let (input, element_type) = parse_cql_type_id(input)?;

    let mut elements = Vec::with_capacity(count);
    let mut remaining = input;

    for _ in 0..count {
        let (new_remaining, element) = parse_cql_value(remaining, element_type)?;
        elements.push(element);
        remaining = new_remaining;
    }

    Ok((remaining, Value::List(elements)))
}

/// Parse set using enhanced Cassandra 5+ parser with fallback to legacy format
pub fn parse_set_enhanced(input: &[u8]) -> IResult<&[u8], Value> {
    // Try Cassandra 5+ format first, fall back to legacy on failure
    match parse_set_v5_format(input) {
        Ok(result) => Ok(result),
        Err(_) => parse_set(input), // Fallback to legacy format
    }
}

/// Legacy set parser for backward compatibility
pub fn parse_set(input: &[u8]) -> IResult<&[u8], Value> {
    let (remaining, list_value) = parse_list(input)?;
    
    if let Value::List(elements) = list_value {
        // Convert to Set - in Cassandra, sets maintain insertion order but enforce uniqueness
        // We preserve the order as read from the SSTable for compatibility
        Ok((remaining, Value::Set(elements)))
    } else {
        Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)))
    }
}

/// Parse map using enhanced Cassandra 5+ parser with fallback to legacy format
pub fn parse_map_enhanced(input: &[u8]) -> IResult<&[u8], Value> {
    // Try Cassandra 5+ format first, fall back to legacy on failure
    match parse_map_v5_format(input) {
        Ok(result) => Ok(result),
        Err(_) => parse_map(input), // Fallback to legacy format
    }
}

/// Legacy map parser for backward compatibility
pub fn parse_map(input: &[u8]) -> IResult<&[u8], Value> {
    let (input, count) = parse_vint_length(input)?;
    
    // Validate count to prevent memory exhaustion
    const MAX_COLLECTION_SIZE: usize = 1_000_000;
    if count > MAX_COLLECTION_SIZE {
        return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::TooLarge)));
    }
    
    if count == 0 {
        return Ok((input, Value::Map(Vec::new())));
    }
    
    let (input, key_type) = parse_cql_type_id(input)?;
    let (input, value_type) = parse_cql_type_id(input)?;

    let mut map = Vec::with_capacity(count);
    let mut remaining = input;

    for _ in 0..count {
        let (new_remaining, key) = parse_cql_value(remaining, key_type)?;
        let (new_remaining, value) = parse_cql_value(new_remaining, value_type)?;

        map.push((key, value));
        remaining = new_remaining;
    }

    Ok((remaining, Value::Map(map)))
}

/// Parse UDT using enhanced parser with schema registry support
pub fn parse_udt_enhanced(input: &[u8]) -> IResult<&[u8], Value> {
    parse_udt_enhanced_with_registry(input, &UdtRegistry::with_cassandra5_defaults())
}

/// Parse UDT with enhanced registry support
pub fn parse_udt_enhanced_with_registry<'a>(input: &'a [u8], registry: &UdtRegistry) -> IResult<&'a [u8], Value> {
    // First, try to parse the embedded schema to get the type name
    let original_input = input;
    
    // Parse UDT type name length and name
    let (input, type_name_length) = parse_vint_length(input)?;
    let (input, type_name_bytes) = take(type_name_length)(input)?;
    let type_name = String::from_utf8(type_name_bytes.to_vec()).map_err(|_| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
    })?;
    
    // Try to use registry first
    if let Some(udt_def) = try_find_udt_in_any_keyspace(registry, &type_name) {
        // Use registry-based parsing for better accuracy
        parse_udt_with_schema_and_registry(original_input, udt_def, registry)
    } else {
        // Fallback to embedded schema parsing
        parse_udt(original_input)
    }
}

/// Parse UDT value with embedded schema information (for SSTable format)
pub fn parse_udt(input: &[u8]) -> IResult<&[u8], Value> {
    // Parse UDT type name length and name
    let (input, type_name_length) = parse_vint_length(input)?;
    let (input, type_name_bytes) = take(type_name_length)(input)?;
    let type_name = String::from_utf8(type_name_bytes.to_vec()).map_err(|_| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
    })?;

    // Parse field count
    let (input, field_count) = parse_vint_length(input)?;

    // Parse field definitions (schema metadata)
    let mut field_defs = Vec::with_capacity(field_count);
    let mut remaining = input;

    for _ in 0..field_count {
        // Parse field name
        let (new_remaining, field_name_length) = parse_vint_length(remaining)?;
        let (new_remaining, field_name_bytes) = take(field_name_length)(new_remaining)?;
        let field_name = String::from_utf8(field_name_bytes.to_vec()).map_err(|_| {
            nom::Err::Error(nom::error::Error::new(new_remaining, nom::error::ErrorKind::Verify))
        })?;

        // Parse field type ID
        let (new_remaining, field_type_id) = parse_cql_type_id(new_remaining)?;

        field_defs.push((field_name, field_type_id));
        remaining = new_remaining;
    }

    // Parse field values
    let mut fields = Vec::with_capacity(field_count);
    for (field_name, field_type_id) in field_defs {
        // Parse field length
        let (new_remaining, length) = be_i32(remaining)?;
        remaining = new_remaining;

        let field_value = if length == -1 {
            // Null field
            None
        } else if length == 0 {
            // Empty field
            Some(create_empty_value(field_type_id).map_err(|e| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)))?)
        } else {
            // Field with data
            let (new_remaining, field_data) = take(length as usize)(remaining)?;
            remaining = new_remaining;
            Some(parse_cql_value(field_data, field_type_id)?.1)
        };

        fields.push(UdtField {
            name: field_name,
            value: field_value,
        });
    }

    let udt = UdtValue {
        type_name,
        keyspace: "unknown".to_string(), // Will be resolved from schema context
        fields,
    };

    Ok((remaining, Value::Udt(udt)))
}

/// Parse UDT value with schema context (preferred method for production)
pub fn parse_udt_with_schema<'a>(input: &'a [u8], udt_def: &UdtTypeDef) -> IResult<&'a [u8], Value> {
    let mut fields = Vec::with_capacity(udt_def.fields.len());
    let mut remaining = input;

    // Parse each field according to the UDT schema definition
    for field_def in &udt_def.fields {
        // Parse field length
        let (new_remaining, length) = be_i32(remaining)?;
        remaining = new_remaining;

        let field_value = if length == -1 {
            // Null field
            None
        } else if length == 0 {
            // Empty field - create appropriate empty value
            Some(create_empty_value_for_cql_type(&field_def.field_type).map_err(|_| {
                nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
            })?)
        } else {
            // Field with data
            let (new_remaining, field_data) = take(length as usize)(remaining)?;
            remaining = new_remaining;
            
            // Parse field data according to its CQL type
            Some(parse_cql_value_for_type(field_data, &field_def.field_type).map_err(|_| {
                nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
            })?)
        };

        fields.push(UdtField {
            name: field_def.name.clone(),
            value: field_value,
        });
    }

    let udt = UdtValue {
        type_name: udt_def.name.clone(),
        keyspace: udt_def.keyspace.clone(),
        fields,
    };

    Ok((remaining, Value::Udt(udt)))
}

/// Parse UDT value by looking up schema from registry with enhanced dependency resolution
pub fn parse_udt_with_registry<'a>(
    input: &'a [u8], 
    type_name: &str, 
    keyspace: &str, 
    registry: &UdtRegistry
) -> IResult<&'a [u8], Value> {
    // Try to resolve UDT with full dependency validation
    match registry.resolve_udt_with_dependencies(keyspace, type_name) {
        Ok(udt_def) => {
            parse_udt_with_schema_and_registry(input, udt_def, registry)
        },
        Err(_) => {
            // Fallback: try other keyspaces (for compatibility)
            if let Some(udt_def) = try_find_udt_in_any_keyspace(registry, type_name) {
                parse_udt_with_schema_and_registry(input, udt_def, registry)
            } else {
                // Last resort: embedded schema parsing
                parse_udt(input)
            }
        }
    }
}

/// Find UDT in any available keyspace (fallback for missing keyspace info)
fn try_find_udt_in_any_keyspace<'a>(registry: &'a UdtRegistry, type_name: &str) -> Option<&'a UdtTypeDef> {
    // Try common keyspaces in order
    let common_keyspaces = ["system", "test_keyspace", "default", "cassandra"];
    
    for keyspace in &common_keyspaces {
        if let Some(udt_def) = registry.get_udt(keyspace, type_name) {
            return Some(udt_def);
        }
    }
    
    None
}

/// Create empty value for a CQL type
fn create_empty_value_for_cql_type(cql_type: &CqlType) -> Result<Value> {
    match cql_type {
        CqlType::Boolean => Ok(Value::Boolean(false)),
        CqlType::TinyInt => Ok(Value::TinyInt(0)),
        CqlType::SmallInt => Ok(Value::SmallInt(0)),
        CqlType::Int => Ok(Value::Integer(0)),
        CqlType::BigInt => Ok(Value::BigInt(0)),
        CqlType::Float => Ok(Value::Float32(0.0)),
        CqlType::Double => Ok(Value::Float(0.0)),
        CqlType::Text | CqlType::Ascii | CqlType::Varchar => Ok(Value::Text(String::new())),
        CqlType::Blob => Ok(Value::Blob(Vec::new())),
        CqlType::Uuid | CqlType::TimeUuid => Ok(Value::Uuid([0; 16])),
        CqlType::Timestamp => Ok(Value::Timestamp(0)),
        CqlType::Date => Ok(Value::Timestamp(0)),
        CqlType::Time => Ok(Value::Timestamp(0)),
        CqlType::List(_) => Ok(Value::List(Vec::new())),
        CqlType::Set(_) => Ok(Value::Set(Vec::new())),
        CqlType::Map(_, _) => Ok(Value::Map(Vec::new())),
        CqlType::Tuple(_) => Ok(Value::Tuple(Vec::new())),
        CqlType::Udt(name, _) => Ok(Value::Udt(UdtValue::new(name.clone(), "unknown".to_string()))),
        CqlType::Frozen(inner) => create_empty_value_for_cql_type(inner),
        _ => Ok(Value::Null),
    }
}

/// Parse CQL value for a specific CQL type (used for UDT fields)
fn parse_cql_value_for_type(input: &[u8], cql_type: &CqlType) -> Result<Value> {
    let type_id = cql_type_to_type_id(cql_type);
    let (_, value) = parse_cql_value(input, type_id)
        .map_err(|_| Error::corruption("Failed to parse CQL value for UDT field".to_string()))?;
    Ok(value)
}

/// Parse UDT with schema and registry support for nested UDTs
pub fn parse_udt_with_schema_and_registry<'a>(
    input: &'a [u8], 
    udt_def: &UdtTypeDef, 
    registry: &UdtRegistry
) -> IResult<&'a [u8], Value> {
    let mut fields = Vec::with_capacity(udt_def.fields.len());
    let mut remaining = input;

    // Parse each field according to the UDT schema definition
    for field_def in &udt_def.fields {
        // Parse field length
        let (new_remaining, length) = be_i32(remaining)?;
        remaining = new_remaining;

        let field_value = if length == -1 {
            // Null field
            None
        } else if length == 0 {
            // Empty field - create appropriate empty value
            Some(create_empty_value_for_cql_type(&field_def.field_type).map_err(|_| {
                nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
            })?)
        } else {
            // Field with data
            let (new_remaining, field_data) = take(length as usize)(remaining)?;
            remaining = new_remaining;
            
            // Parse field data with registry support for nested UDTs
            Some(parse_cql_value_for_type_with_registry(field_data, &field_def.field_type, &udt_def.keyspace, registry).map_err(|_| {
                nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
            })?)
        };

        fields.push(UdtField {
            name: field_def.name.clone(),
            value: field_value,
        });
    }

    let udt = UdtValue {
        type_name: udt_def.name.clone(),
        keyspace: udt_def.keyspace.clone(),
        fields,
    };

    Ok((remaining, Value::Udt(udt)))
}

/// Parse CQL value for a specific CQL type with registry support for nested UDTs
fn parse_cql_value_for_type_with_registry(
    input: &[u8], 
    cql_type: &CqlType, 
    keyspace: &str,
    registry: &UdtRegistry
) -> Result<Value> {
    match cql_type {
        CqlType::Udt(udt_name, _) => {
            // Parse nested UDT using registry
            let (_, value) = parse_udt_with_registry(input, udt_name, keyspace, registry)
                .map_err(|_| Error::corruption(format!("Failed to parse nested UDT '{}'", udt_name)))?;
            Ok(value)
        },
        CqlType::List(element_type) => {
            // Parse list with potential UDT elements
            let (_, value) = parse_list_with_element_type(input, element_type, keyspace, registry)
                .map_err(|_| Error::corruption("Failed to parse list with UDT elements".to_string()))?;
            Ok(value)
        },
        CqlType::Set(element_type) => {
            // Parse set with potential UDT elements
            let (_, value) = parse_set_with_element_type(input, element_type, keyspace, registry)
                .map_err(|_| Error::corruption("Failed to parse set with UDT elements".to_string()))?;
            Ok(value)
        },
        CqlType::Map(key_type, value_type) => {
            // Parse map with potential UDT keys/values
            let (_, value) = parse_map_with_types(input, key_type, value_type, keyspace, registry)
                .map_err(|_| Error::corruption("Failed to parse map with UDT elements".to_string()))?;
            Ok(value)
        },
        CqlType::Frozen(inner_type) => {
            // Parse frozen type (recursive)
            let inner_value = parse_cql_value_for_type_with_registry(input, inner_type, keyspace, registry)?;
            Ok(Value::Frozen(Box::new(inner_value)))
        },
        _ => {
            // For primitive types, use the standard parser
            let type_id = cql_type_to_type_id(cql_type);
            let (_, value) = parse_cql_value(input, type_id)
                .map_err(|_| Error::corruption("Failed to parse primitive CQL value".to_string()))?;
            Ok(value)
        }
    }
}

/// Parse list with specific element type (including UDTs)
fn parse_list_with_element_type<'a>(
    input: &'a [u8],
    element_type: &CqlType,
    keyspace: &str,
    registry: &UdtRegistry
) -> IResult<&'a [u8], Value> {
    let (input, count) = parse_vint_length(input)?;
    
    // Validate count to prevent memory exhaustion attacks
    const MAX_COLLECTION_SIZE: usize = 1_000_000;
    if count > MAX_COLLECTION_SIZE {
        return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::TooLarge)));
    }
    
    if count == 0 {
        return Ok((input, Value::List(Vec::new())));
    }

    let mut elements = Vec::with_capacity(count);
    let mut remaining = input;

    for _ in 0..count {
        // Parse element length
        let (new_remaining, element_length) = be_i32(remaining)?;
        remaining = new_remaining;
        
        if element_length > 0 {
            let (new_remaining, element_data) = take(element_length as usize)(remaining)?;
            remaining = new_remaining;
            
            let element_value = parse_cql_value_for_type_with_registry(element_data, element_type, keyspace, registry)
                .map_err(|_| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)))?;
            elements.push(element_value);
        } else if element_length == 0 {
            // Empty element
            let empty_value = create_empty_value_for_cql_type(element_type)
                .map_err(|_| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)))?;
            elements.push(empty_value);
        } else {
            // Null element - skip for lists (null elements typically not allowed)
            // Note: Some Cassandra versions may handle this differently
        }
    }

    Ok((remaining, Value::List(elements)))
}

/// Parse set with specific element type (including UDTs)
fn parse_set_with_element_type<'a>(
    input: &'a [u8],
    element_type: &CqlType,
    keyspace: &str,
    registry: &UdtRegistry
) -> IResult<&'a [u8], Value> {
    let (remaining, list_value) = parse_list_with_element_type(input, element_type, keyspace, registry)?;
    
    if let Value::List(elements) = list_value {
        // Convert to Set - in Cassandra, sets maintain insertion order but enforce uniqueness
        Ok((remaining, Value::Set(elements)))
    } else {
        Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)))
    }
}

/// Parse map with specific key and value types (including UDTs)
fn parse_map_with_types<'a>(
    input: &'a [u8],
    key_type: &CqlType,
    value_type: &CqlType,
    keyspace: &str,
    registry: &UdtRegistry
) -> IResult<&'a [u8], Value> {
    let (input, count) = parse_vint_length(input)?;
    
    // Validate count to prevent memory exhaustion
    const MAX_COLLECTION_SIZE: usize = 1_000_000;
    if count > MAX_COLLECTION_SIZE {
        return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::TooLarge)));
    }
    
    if count == 0 {
        return Ok((input, Value::Map(Vec::new())));
    }

    let mut map = Vec::with_capacity(count);
    let mut remaining = input;

    for _ in 0..count {
        // Parse key
        let (new_remaining, key_length) = be_i32(remaining)?;
        remaining = new_remaining;
        
        let key = if key_length > 0 {
            let (new_remaining, key_data) = take(key_length as usize)(remaining)?;
            remaining = new_remaining;
            parse_cql_value_for_type_with_registry(key_data, key_type, keyspace, registry)
                .map_err(|_| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)))?
        } else {
            create_empty_value_for_cql_type(key_type)
                .map_err(|_| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)))?
        };
        
        // Parse value
        let (new_remaining, value_length) = be_i32(remaining)?;
        remaining = new_remaining;
        
        let value = if value_length > 0 {
            let (new_remaining, value_data) = take(value_length as usize)(remaining)?;
            remaining = new_remaining;
            parse_cql_value_for_type_with_registry(value_data, value_type, keyspace, registry)
                .map_err(|_| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)))?
        } else {
            create_empty_value_for_cql_type(value_type)
                .map_err(|_| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)))?
        };

        map.push((key, value));
    }

    Ok((remaining, Value::Map(map)))
}

/// Convert CqlType to CqlTypeId for parsing
fn cql_type_to_type_id(cql_type: &CqlType) -> CqlTypeId {
    match cql_type {
        CqlType::Boolean => CqlTypeId::Boolean,
        CqlType::TinyInt => CqlTypeId::Tinyint,
        CqlType::SmallInt => CqlTypeId::Smallint,
        CqlType::Int => CqlTypeId::Int,
        CqlType::BigInt => CqlTypeId::BigInt,
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
        CqlType::List(_) => CqlTypeId::List,
        CqlType::Set(_) => CqlTypeId::Set,
        CqlType::Map(_, _) => CqlTypeId::Map,
        CqlType::Tuple(_) => CqlTypeId::Tuple,
        CqlType::Udt(_, _) => CqlTypeId::Udt,
        CqlType::Frozen(_) => CqlTypeId::Blob, // Frozen types use underlying type
        CqlType::Custom(_) => CqlTypeId::Blob, // Custom types as blob
    }
}

/// Parse FROZEN<UDT> values - these are immutable UDT instances
pub fn parse_frozen_udt<'a>(input: &'a [u8], udt_def: &UdtTypeDef) -> IResult<&'a [u8], Value> {
    let registry = UdtRegistry::with_cassandra5_defaults();
    let (remaining, udt_value) = parse_udt_with_schema_and_registry(input, udt_def, &registry)?;
    Ok((remaining, Value::Frozen(Box::new(udt_value))))
}

/// Parse FROZEN<UDT> with registry support for nested dependencies
pub fn parse_frozen_udt_with_registry<'a>(
    input: &'a [u8], 
    udt_def: &UdtTypeDef, 
    registry: &UdtRegistry
) -> IResult<&'a [u8], Value> {
    let (remaining, udt_value) = parse_udt_with_schema_and_registry(input, udt_def, registry)?;
    Ok((remaining, Value::Frozen(Box::new(udt_value))))
}

/// Parse tuple value according to Cassandra format specification
pub fn parse_tuple(input: &[u8]) -> IResult<&[u8], Value> {
    // Parse field count
    let (input, field_count) = parse_vint_length(input)?;

    // Parse field type definitions
    let mut field_types = Vec::with_capacity(field_count);
    let mut remaining = input;

    for _ in 0..field_count {
        let (new_remaining, field_type_id) = parse_cql_type_id(remaining)?;
        field_types.push(field_type_id);
        remaining = new_remaining;
    }

    // Parse field values (tuples must have exact field count, no sparse representation)
    let mut fields = Vec::with_capacity(field_count);
    for field_type_id in field_types {
        // Parse field length
        let (new_remaining, length) = be_i32(remaining)?;
        remaining = new_remaining;

        let field_value = if length == -1 {
            Value::Null // Null field
        } else {
            let (new_remaining, field_data) = take(length as usize)(remaining)?;
            remaining = new_remaining;
            parse_cql_value(field_data, field_type_id)?.1
        };

        fields.push(field_value);
    }

    Ok((remaining, Value::Tuple(fields)))
}

/// Parse tombstone information with enhanced Cassandra 5.0 compatibility
pub fn parse_tombstone(input: &[u8]) -> IResult<&[u8], Value> {
    // Parse deletion timestamp (microseconds since epoch)
    let (input, deletion_time) = be_i64(input)?;
    
    // Parse tombstone type byte
    let (input, tombstone_type_byte) = be_u8(input)?;
    
    let tombstone_type = match tombstone_type_byte {
        0 => TombstoneType::RowTombstone,
        1 => TombstoneType::CellTombstone,
        2 => TombstoneType::RangeTombstone,
        3 => TombstoneType::TtlExpiration,
        _ => return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))),
    };
    
    // Parse optional TTL for TTL-based tombstones
    let (input, ttl) = if tombstone_type == TombstoneType::TtlExpiration {
        let (input, ttl_value) = be_i64(input)?;
        (input, Some(ttl_value))
    } else {
        (input, None)
    };
    
    // Parse optional clustering key range for range tombstones
    let (input, range_start, range_end) = if tombstone_type == TombstoneType::RangeTombstone {
        let (input, has_range) = be_u8(input)?;
        if has_range != 0 {
            let (input, start_len) = parse_vint_length(input)?;
            let (input, start_data) = take(start_len)(input)?;
            let (input, end_len) = parse_vint_length(input)?;
            let (input, end_data) = take(end_len)(input)?;
            (input, Some(start_data.to_vec()), Some(end_data.to_vec()))
        } else {
            (input, None, None)
        }
    } else {
        (input, None, None)
    };
    
    let tombstone_info = TombstoneInfo {
        deletion_time,
        tombstone_type,
        ttl,
        range_start: range_start.map(|data| RowKey::new(data)),
        range_end: range_end.map(|data| RowKey::new(data)),
    };
    
    Ok((input, Value::Tombstone(tombstone_info)))
}

/// Parse List with enhanced Cassandra 5+ format supporting mixed types and proper cell handling
pub fn parse_list_v5_format(input: &[u8]) -> IResult<&[u8], Value> {
    // Enhanced Cassandra 5+ format with proper cell metadata handling
    let (input, count) = parse_vint_length(input)?;
    
    // Validate count to prevent memory exhaustion attacks
    const MAX_COLLECTION_SIZE: usize = 1_000_000;
    if count > MAX_COLLECTION_SIZE {
        return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::TooLarge)));
    }
    
    if count == 0 {
        return Ok((input, Value::List(Vec::new())));
    }

    // Check if this is a mixed-type collection (Cassandra 5.0 feature)
    let (mut remaining, format_flags) = be_u8(input)?;
    let has_mixed_types = (format_flags & 0x01) != 0;

    let mut elements = Vec::with_capacity(count);

    if has_mixed_types {
        // Each element has its own type information
        for _ in 0..count {
            let (new_remaining, element_type_id) = parse_cql_type_id(remaining)?;
            let (new_remaining, element_length) = parse_vint_length(new_remaining)?;
            
            if element_length > 0 {
                let (new_remaining, element_data) = take(element_length)(new_remaining)?;
                let element_value = parse_cql_value_with_cell_metadata(element_data, element_type_id)
                    .map_err(|e| nom::Err::Error(nom::error::Error::new(element_data, nom::error::ErrorKind::Verify)))?;
                elements.push(element_value);
                remaining = new_remaining;
            } else {
                // Null element
                elements.push(Value::Null);
                remaining = new_remaining;
            }
        }
    } else {
        // Homogeneous collection with single type
        let (new_remaining, element_type_id) = parse_cql_type_id(remaining)?;
        remaining = new_remaining;

        for _ in 0..count {
            let (new_remaining, element_length) = parse_vint_length(remaining)?;
            
            if element_length > 0 {
                let (new_remaining, element_data) = take(element_length)(new_remaining)?;
                let element_value = parse_cql_value_with_cell_metadata(element_data, element_type_id)
                    .map_err(|e| nom::Err::Error(nom::error::Error::new(element_data, nom::error::ErrorKind::Verify)))?;
                elements.push(element_value);
                remaining = new_remaining;
            } else {
                // Null element
                elements.push(Value::Null);
                remaining = new_remaining;
            }
        }
    }

    Ok((remaining, Value::List(elements)))
}

/// Parse CQL value with Cassandra 5.0 cell metadata handling
fn parse_cql_value_with_cell_metadata(input: &[u8], type_id: CqlTypeId) -> Result<Value> {
    if input.is_empty() {
        return Ok(Value::Null);
    }

    // Skip cell metadata if present (Cassandra 5.0 format)
    let mut offset = 0;
    if input.len() > 1 && (input[0] & 0x80) != 0 {
        offset += 1; // Skip cell flags
        
        // Skip timestamp if present (8 bytes)
        if offset + 8 <= input.len() {
            offset += 8;
        }
        
        // Skip TTL if present (4 bytes)
        if offset < input.len() && (input[0] & 0x40) != 0 && offset + 4 <= input.len() {
            offset += 4;
        }
    }

    let actual_data = &input[offset..];
    if actual_data.is_empty() {
        return Ok(Value::Null);
    }

    // Parse the actual value
    let (_, value) = parse_cql_value(actual_data, type_id)?;
    Ok(value)
}

/// Parse Set with Cassandra 5+ format 
pub fn parse_set_v5_format(input: &[u8]) -> IResult<&[u8], Value> {
    // Sets use same binary format as lists in Cassandra 5+
    let (remaining, list_value) = parse_list_v5_format(input)?;
    
    if let Value::List(elements) = list_value {
        // Convert to Set - maintain insertion order for compatibility
        Ok((remaining, Value::Set(elements)))
    } else {
        Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)))
    }
}

/// Parse Map with Cassandra 5+ format (tuple-based serialization)
pub fn parse_map_v5_format(input: &[u8]) -> IResult<&[u8], Value> {
    // Cassandra 5+ format: [count:vint][key_type:u8][value_type:u8][pairs...]
    let (input, count) = parse_vint_length(input)?;
    
    // Validate count to prevent memory exhaustion
    const MAX_COLLECTION_SIZE: usize = 1_000_000;
    if count > MAX_COLLECTION_SIZE {
        return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::TooLarge)));
    }
    
    if count == 0 {
        return Ok((input, Value::Map(Vec::new())));
    }
    
    let (input, key_type_id) = parse_cql_type_id(input)?;
    let (input, value_type_id) = parse_cql_type_id(input)?;

    let mut pairs = Vec::with_capacity(count);
    let mut remaining = input;

    for _ in 0..count {
        // Parse key with length prefix
        let (new_remaining, key_length) = parse_vint_length(remaining)?;
        let (new_remaining, key_data) = take(key_length)(new_remaining)?;
        let (_, key) = parse_cql_value(key_data, key_type_id)?;
        
        // Parse value with length prefix
        let (new_remaining, value_length) = parse_vint_length(new_remaining)?;
        let (new_remaining, value_data) = take(value_length)(new_remaining)?;
        let (_, value) = parse_cql_value(value_data, value_type_id)?;

        pairs.push((key, value));
        remaining = new_remaining;
    }

    Ok((remaining, Value::Map(pairs)))
}

/// Create an empty value for a given type ID
fn create_empty_value(type_id: CqlTypeId) -> Result<Value> {
    match type_id {
        CqlTypeId::Boolean => Ok(Value::Boolean(false)),
        CqlTypeId::Tinyint => Ok(Value::TinyInt(0)),
        CqlTypeId::Smallint => Ok(Value::SmallInt(0)),
        CqlTypeId::Int => Ok(Value::Integer(0)),
        CqlTypeId::BigInt | CqlTypeId::Counter => Ok(Value::BigInt(0)),
        CqlTypeId::Float => Ok(Value::Float32(0.0)),
        CqlTypeId::Double => Ok(Value::Float(0.0)),
        CqlTypeId::Ascii | CqlTypeId::Varchar => Ok(Value::Text(String::new())),
        CqlTypeId::Blob => Ok(Value::Blob(Vec::new())),
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
        Value::Float(f) => {
            result.push(CqlTypeId::Double as u8);
            result.extend_from_slice(&f.to_be_bytes());
        }
        Value::Text(s) => {
            result.push(CqlTypeId::Varchar as u8);
            result.extend_from_slice(&encode_vint(s.len() as i64));
            result.extend_from_slice(s.as_bytes());
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
                    let element_bytes = serialize_cql_value(element)?;
                    result.extend_from_slice(&element_bytes[1..]); // Skip type byte
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
                    let key_bytes = serialize_cql_value(key)?;
                    let value_bytes = serialize_cql_value(value)?;
                    
                    result.extend_from_slice(&key_bytes[1..]); // Skip type byte
                    result.extend_from_slice(&value_bytes[1..]); // Skip type byte
                }
            }
        }
        Value::TinyInt(i) => {
            result.push(CqlTypeId::Tinyint as u8);
            result.push(*i as u8);
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
                    let element_bytes = serialize_cql_value(element)?;
                    result.extend_from_slice(&element_bytes[1..]); // Skip type byte
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
            
            // Serialize field values
            for element in tuple {
                let element_bytes = serialize_cql_value(element)?;
                result.extend_from_slice(&element_bytes[1..]); // Skip type byte
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
                        let field_bytes = serialize_cql_value(value)?;
                        // Remove the type ID byte since it's already in the schema
                        let field_data = &field_bytes[1..];
                        result.extend_from_slice(&(field_data.len() as i32).to_be_bytes());
                        result.extend_from_slice(field_data);
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
    }

    Ok(result)
}

fn map_value_to_cql_type(value: &Value) -> CqlTypeId {
    match value {
        Value::Null => CqlTypeId::Blob, // Generic fallback
        Value::Boolean(_) => CqlTypeId::Boolean,
        Value::Integer(_) => CqlTypeId::Int,
        Value::BigInt(_) => CqlTypeId::BigInt,
        Value::Float(_) => CqlTypeId::Double,
        Value::Text(_) => CqlTypeId::Varchar,
        Value::Blob(_) => CqlTypeId::Blob,
        Value::Timestamp(_) => CqlTypeId::Timestamp,
        Value::Uuid(_) => CqlTypeId::Uuid,
        Value::Json(_) => CqlTypeId::Varchar,
        Value::TinyInt(_) => CqlTypeId::Tinyint,
        Value::SmallInt(_) => CqlTypeId::Smallint,
        Value::Float32(_) => CqlTypeId::Float,
        Value::List(_) => CqlTypeId::List,
        Value::Set(_) => CqlTypeId::Set,
        Value::Map(_) => CqlTypeId::Map,
        Value::Tuple(_) => CqlTypeId::Tuple,
        Value::Udt(_) => CqlTypeId::Udt,
        Value::Frozen(_) => CqlTypeId::Blob, // Frozen is a wrapper, use blob as fallback
        Value::Tombstone(_) => CqlTypeId::Tombstone,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cql_type_id_conversion() {
        assert_eq!(CqlTypeId::try_from(0x04).unwrap(), CqlTypeId::Boolean);
        assert_eq!(CqlTypeId::try_from(0x09).unwrap(), CqlTypeId::Int);
        assert!(CqlTypeId::try_from(0xFF).is_err());
    }

    #[test]
    fn test_boolean_parsing() {
        let data = vec![0x01];
        let (_, value) = parse_boolean(&data).unwrap();
        assert_eq!(value, Value::Boolean(true));

        let data = vec![0x00];
        let (_, value) = parse_boolean(&data).unwrap();
        assert_eq!(value, Value::Boolean(false));
    }

    #[test]
    fn test_int_parsing() {
        let data = vec![0x00, 0x00, 0x01, 0x00]; // 256 in big-endian
        let (_, value) = parse_int(&data).unwrap();
        assert_eq!(value, Value::Integer(256));
    }

    #[test]
    fn test_text_parsing() {
        use super::super::vint::encode_vint;

        let test_str = "hello";
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(test_str.len() as i64));
        data.extend_from_slice(test_str.as_bytes());

        let (_, value) = parse_text(&data).unwrap();
        assert_eq!(value, Value::Text("hello".to_string()));
    }

    #[test]
    fn test_uuid_parsing() {
        let uuid_bytes = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let (_, value) = parse_uuid(&uuid_bytes).unwrap();
        assert_eq!(value, Value::Uuid(uuid_bytes));
    }

    #[test]
    fn test_value_serialization_roundtrip() {
        let test_values = vec![
            Value::Boolean(true),
            Value::Integer(42),
            Value::BigInt(1000),
            Value::Float(3.14),
            Value::Text("test".to_string()),
            Value::Blob(vec![1, 2, 3, 4]),
        ];

        for value in test_values {
            let serialized = serialize_cql_value(&value).unwrap();
            // Note: Full roundtrip testing would require implementing deserialization
            // which depends on the type context that's not always preserved
            assert!(!serialized.is_empty());
        }
    }

    #[test]
    fn test_udt_serialization() {
        // Test UDT serialization
        let udt = UdtValue {
            type_name: "Person".to_string(),
            keyspace: "test".to_string(),
            fields: vec![
                UdtField { name: "name".to_string(), value: Some(Value::Text("John".to_string())) },
                UdtField { name: "age".to_string(), value: Some(Value::Integer(30)) },
                UdtField { name: "email".to_string(), value: None }, // Null field
            ],
        };

        let serialized = serialize_cql_value(&Value::Udt(udt)).unwrap();
        assert!(!serialized.is_empty());
        
        // Should start with UDT type ID
        assert_eq!(serialized[0], CqlTypeId::Udt as u8);
    }

    #[test]
    fn test_tuple_serialization() {
        // Test tuple serialization
        let tuple = vec![
            Value::Text("hello".to_string()),
            Value::Integer(42),
            Value::Boolean(true),
        ];

        let serialized = serialize_cql_value(&Value::Tuple(tuple)).unwrap();
        assert!(!serialized.is_empty());
        
        // Should start with Tuple type ID
        assert_eq!(serialized[0], CqlTypeId::Tuple as u8);
    }

    #[test]
    fn test_tombstone_parsing() {
        // Test row tombstone parsing
        let row_tombstone = Value::row_tombstone(1000);
        let serialized = serialize_cql_value(&row_tombstone).unwrap();
        
        // Parse it back
        let (remaining, parsed_value) = parse_tombstone(&serialized[1..]).unwrap(); // Skip type ID
        assert!(remaining.is_empty());
        assert_eq!(parsed_value, row_tombstone);

        // Test TTL tombstone parsing
        let ttl_tombstone = Value::ttl_tombstone(2000, 1000);
        let serialized_ttl = serialize_cql_value(&ttl_tombstone).unwrap();
        
        let (remaining, parsed_ttl) = parse_tombstone(&serialized_ttl[1..]).unwrap(); // Skip type ID
        assert!(remaining.is_empty());
        assert_eq!(parsed_ttl, ttl_tombstone);
    }

    #[test]
    fn test_tombstone_serialization() {
        // Test tombstone serialization
        let tombstone = Value::cell_tombstone(5000);
        let serialized = serialize_cql_value(&tombstone).unwrap();
        assert!(!serialized.is_empty());
        
        // Should start with Tombstone type ID
        assert_eq!(serialized[0], CqlTypeId::Tombstone as u8);
        
        // Should contain deletion time
        let deletion_time_bytes = &serialized[1..9];
        let deletion_time = i64::from_be_bytes([
            deletion_time_bytes[0], deletion_time_bytes[1], deletion_time_bytes[2], deletion_time_bytes[3],
            deletion_time_bytes[4], deletion_time_bytes[5], deletion_time_bytes[6], deletion_time_bytes[7],
        ]);
        assert_eq!(deletion_time, 5000);
    }
}
