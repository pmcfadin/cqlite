//! UDT, tuple, and frozen CQL type decoding.
//!
//! Handles user-defined types (embedded-schema and registry-driven), tuples,
//! and frozen wrappers, including registry-aware decoding of nested UDTs.

// Issue #1623: UDT/tuple schema + field lengths round-trip with CQLite's own
// ZigZag serializer (serialize_cql_value), a self-consistent internal pair, so
// these length/count reads use the signed helper (not unsigned parse_vint_length).
use super::super::vint::parse_vint_length_signed;
use super::collections::{
    parse_list_with_element_type, parse_map_with_types, parse_set_with_element_type,
};
use super::{
    cql_type_to_type_id, create_empty_value, create_empty_value_for_cql_type, parse_cql_value,
    parse_cql_value_raw,
};
use crate::{
    error::{Error, Result},
    schema::{CqlType, UdtRegistry},
    types::{UdtField, UdtTypeDef, UdtValue, Value},
};
use nom::{bytes::complete::take, number::complete::be_i32, IResult};

/// Parse UDT using enhanced parser with schema registry support
pub fn parse_udt_enhanced(input: &[u8]) -> IResult<&[u8], Value> {
    parse_udt_enhanced_with_registry(input, &UdtRegistry::with_cassandra5_defaults())
}

/// Parse UDT with enhanced registry support
pub fn parse_udt_enhanced_with_registry<'a>(
    input: &'a [u8],
    registry: &UdtRegistry,
) -> IResult<&'a [u8], Value> {
    // First, always try embedded schema parsing (most common in SSTable format)
    match parse_udt(input) {
        Ok((remaining, udt_value)) => {
            // If we parsed successfully and have registry info, enhance with keyspace info
            if let Value::Udt(ref udt) = udt_value {
                if let Some(udt_def) = try_find_udt_in_any_keyspace(registry, &udt.type_name) {
                    let mut enhanced_udt = udt.clone();
                    enhanced_udt.keyspace = udt_def.keyspace.clone();
                    return Ok((remaining, Value::Udt(enhanced_udt)));
                }
            }
            Ok((remaining, udt_value))
        }
        Err(embedded_error) => {
            // Embedded parsing failed, try to extract type name and use registry-based parsing
            if let Ok((after_type_name_len, type_name_length)) = parse_vint_length_signed(input) {
                if let Ok((after_type_name, type_name_bytes)) =
                    take::<_, _, nom::error::Error<&[u8]>>(type_name_length)(after_type_name_len)
                {
                    if let Ok(type_name) = String::from_utf8(type_name_bytes.to_vec()) {
                        if let Some(udt_def) = try_find_udt_in_any_keyspace(registry, &type_name) {
                            // Skip embedded schema and parse field values with registry definition
                            if let Ok((after_schema, _)) = skip_embedded_udt_schema(after_type_name)
                            {
                                return parse_udt_with_schema_and_registry(
                                    after_schema,
                                    udt_def,
                                    registry,
                                );
                            }
                        }
                    }
                }
            }

            // All advanced parsing failed, return original error
            Err(embedded_error)
        }
    }
}

/// Parse UDT value with embedded schema information (for SSTable format)
pub fn parse_udt(input: &[u8]) -> IResult<&[u8], Value> {
    // Parse UDT type name length and name
    let (input, type_name_length) = parse_vint_length_signed(input)?;
    let (input, type_name_bytes) = take(type_name_length)(input)?;
    let type_name = String::from_utf8(type_name_bytes.to_vec()).map_err(|_| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
    })?;

    // Parse field count
    let (input, field_count) = parse_vint_length_signed(input)?;

    // Parse field definitions (schema metadata)
    let mut field_defs = Vec::with_capacity(field_count);
    let mut remaining = input;

    for _ in 0..field_count {
        // Parse field name
        let (new_remaining, field_name_length) = parse_vint_length_signed(remaining)?;
        let (new_remaining, field_name_bytes) = take(field_name_length)(new_remaining)?;
        let field_name = String::from_utf8(field_name_bytes.to_vec()).map_err(|_| {
            nom::Err::Error(nom::error::Error::new(
                new_remaining,
                nom::error::ErrorKind::Verify,
            ))
        })?;

        // Parse field type ID
        let (new_remaining, field_type_id) = super::parse_cql_type_id(new_remaining)?;

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
            Some(create_empty_value(field_type_id).map_err(|_e| {
                nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
            })?)
        } else {
            // Field with data
            let (new_remaining, field_data) = take(length as usize)(remaining)?;
            remaining = new_remaining;
            Some(parse_cql_value_raw(field_data, field_type_id)?.1)
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

    Ok((remaining, Value::Udt(Box::new(udt))))
}

/// Parse UDT value with schema context (preferred method for production)
pub fn parse_udt_with_schema<'a>(
    input: &'a [u8],
    udt_def: &UdtTypeDef,
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
            Some(
                create_empty_value_for_cql_type(&field_def.field_type).map_err(|_| {
                    nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
                })?,
            )
        } else {
            // Field with data
            let (new_remaining, field_data) = take(length as usize)(remaining)?;
            remaining = new_remaining;

            // Parse field data according to its CQL type
            Some(
                parse_cql_value_for_type(field_data, &field_def.field_type).map_err(|_| {
                    nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
                })?,
            )
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

    Ok((remaining, Value::Udt(Box::new(udt))))
}

/// Parse UDT value by looking up schema from registry with enhanced dependency resolution
pub fn parse_udt_with_registry<'a>(
    input: &'a [u8],
    type_name: &str,
    keyspace: &str,
    registry: &UdtRegistry,
) -> IResult<&'a [u8], Value> {
    // First, always try embedded schema parsing (which is the most common format in SSTable data)
    match parse_udt(input) {
        Ok((remaining, udt_value)) => {
            // Successful embedded parsing - check if the type name matches
            if let Value::Udt(ref udt) = udt_value {
                if udt.type_name == type_name {
                    // If we have registry info, update the keyspace if needed
                    if registry.contains_udt(keyspace, type_name) {
                        let mut updated_udt = udt.clone();
                        updated_udt.keyspace = keyspace.to_string();
                        return Ok((remaining, Value::Udt(updated_udt)));
                    }
                }
            }
            Ok((remaining, udt_value))
        }
        Err(_) => {
            // Embedded parsing failed, try registry-based parsing (raw field values)
            match registry.resolve_udt_with_dependencies(keyspace, type_name) {
                Ok(udt_def) => parse_udt_with_schema_and_registry(input, udt_def, registry),
                Err(_) => {
                    // Fallback: try other keyspaces (for compatibility)
                    if let Some(udt_def) = try_find_udt_in_any_keyspace(registry, type_name) {
                        parse_udt_with_schema_and_registry(input, udt_def, registry)
                    } else {
                        // Unable to parse - return the original embedded parsing error
                        parse_udt(input)
                    }
                }
            }
        }
    }
}

/// Find UDT in any available keyspace (fallback for missing keyspace info)
fn try_find_udt_in_any_keyspace<'a>(
    registry: &'a UdtRegistry,
    type_name: &str,
) -> Option<&'a UdtTypeDef> {
    // Try common keyspaces in order
    let common_keyspaces = ["system", "test_keyspace", "default", "cassandra"];

    for keyspace in &common_keyspaces {
        if let Some(udt_def) = registry.get_udt(keyspace, type_name) {
            return Some(udt_def);
        }
    }

    None
}

/// Skip over embedded UDT schema to get to the field values
fn skip_embedded_udt_schema(input: &[u8]) -> IResult<&[u8], ()> {
    // Parse field count
    let (mut remaining, field_count) = parse_vint_length_signed(input)?;

    // Skip over field definitions (name + type for each field)
    for _ in 0..field_count {
        // Skip field name
        let (new_remaining, field_name_length) = parse_vint_length_signed(remaining)?;
        let (new_remaining, _) = take(field_name_length)(new_remaining)?;

        // Skip field type
        let (new_remaining, _) = take(1usize)(new_remaining)?; // Type ID is 1 byte

        remaining = new_remaining;
    }

    Ok((remaining, ()))
}

/// Parse CQL value for a specific CQL type (used for UDT fields)
pub(super) fn parse_cql_value_for_type(input: &[u8], cql_type: &CqlType) -> Result<Value> {
    match cql_type {
        CqlType::Frozen(inner) => {
            let inner_value = parse_cql_value_for_type(input, inner)?;
            Ok(Value::Frozen(Box::new(inner_value)))
        }
        // #4114: intercepted BEFORE `cql_type_to_type_id`, which has no vector id —
        // a vector reaching a type id would be decoded as a `Custom`, i.e. a blob,
        // silently discarding the declared element type and dimension (#28). The
        // field bytes are exactly the value here, so the exact-width rule applies.
        CqlType::Vector(element, dimension) => {
            crate::schema::vector_type::vector_value::require_float_element(element, *dimension)?;
            crate::schema::vector_type::vector_value::decode_float_vector_exact(
                input,
                "UDT field",
                *dimension,
            )
            .map(|(value, _consumed)| value)
        }
        _ => {
            let type_id = cql_type_to_type_id(cql_type);
            let (_, value) = parse_cql_value(input, type_id).map_err(|_| {
                Error::corruption("Failed to parse CQL value for UDT field".to_string())
            })?;
            Ok(value)
        }
    }
}

/// Parse UDT with schema and registry support for nested UDTs
pub fn parse_udt_with_schema_and_registry<'a>(
    input: &'a [u8],
    udt_def: &UdtTypeDef,
    registry: &UdtRegistry,
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
            Some(
                create_empty_value_for_cql_type(&field_def.field_type).map_err(|_| {
                    nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
                })?,
            )
        } else {
            // Field with data
            let (new_remaining, field_data) = take(length as usize)(remaining)?;
            remaining = new_remaining;

            // Parse field data with registry support for nested UDTs
            Some(
                parse_cql_value_for_type_with_registry(
                    field_data,
                    &field_def.field_type,
                    &udt_def.keyspace,
                    registry,
                )
                .map_err(|_| {
                    nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
                })?,
            )
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

    Ok((remaining, Value::Udt(Box::new(udt))))
}

/// Parse CQL value for a specific CQL type with registry support for nested UDTs
pub(super) fn parse_cql_value_for_type_with_registry(
    input: &[u8],
    cql_type: &CqlType,
    keyspace: &str,
    registry: &UdtRegistry,
) -> Result<Value> {
    match cql_type {
        CqlType::Udt(udt_name, _) => {
            // Parse nested UDT using registry
            let (_, value) =
                parse_udt_with_registry(input, udt_name, keyspace, registry).map_err(|_| {
                    Error::corruption(format!("Failed to parse nested UDT '{}'", udt_name))
                })?;
            Ok(value)
        }
        CqlType::List(element_type) => {
            // Parse list with potential UDT elements
            let (_, value) = parse_list_with_element_type(input, element_type, keyspace, registry)
                .map_err(|_| {
                    Error::corruption("Failed to parse list with UDT elements".to_string())
                })?;
            Ok(value)
        }
        CqlType::Set(element_type) => {
            // Parse set with potential UDT elements
            let (_, value) = parse_set_with_element_type(input, element_type, keyspace, registry)
                .map_err(|_| {
                Error::corruption("Failed to parse set with UDT elements".to_string())
            })?;
            Ok(value)
        }
        CqlType::Map(key_type, value_type) => {
            // Parse map with potential UDT keys/values
            let (_, value) = parse_map_with_types(input, key_type, value_type, keyspace, registry)
                .map_err(|_| {
                    Error::corruption("Failed to parse map with UDT elements".to_string())
                })?;
            Ok(value)
        }
        CqlType::Frozen(inner_type) => {
            // Parse frozen type (recursive)
            let inner_value =
                parse_cql_value_for_type_with_registry(input, inner_type, keyspace, registry)?;
            Ok(Value::Frozen(Box::new(inner_value)))
        }
        _ => {
            // For primitive types, use the standard parser
            let type_id = cql_type_to_type_id(cql_type);
            let (_, value) = parse_cql_value(input, type_id).map_err(|_| {
                Error::corruption("Failed to parse primitive CQL value".to_string())
            })?;
            Ok(value)
        }
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
    registry: &UdtRegistry,
) -> IResult<&'a [u8], Value> {
    // First try to parse with embedded schema (most common case)
    if let Ok((remaining, Value::Udt(udt_value))) = parse_udt(input) {
        // Verify the type matches what we expect
        if udt_value.type_name == udt_def.name {
            let mut updated_udt = udt_value;
            updated_udt.keyspace = udt_def.keyspace.clone();
            return Ok((remaining, Value::Frozen(Box::new(Value::Udt(updated_udt)))));
        }
    }

    // Fallback: try to skip embedded schema and parse with registry definition
    if let Ok((after_type_name_len, type_name_length)) = parse_vint_length_signed(input) {
        if let Ok((after_type_name, _type_name_bytes)) =
            take::<_, _, nom::error::Error<&[u8]>>(type_name_length)(after_type_name_len)
        {
            if let Ok((after_schema, _)) = skip_embedded_udt_schema(after_type_name) {
                let (remaining, udt_value) =
                    parse_udt_with_schema_and_registry(after_schema, udt_def, registry)?;
                return Ok((remaining, Value::Frozen(Box::new(udt_value))));
            }
        }
    }

    // All parsing attempts failed
    Err(nom::Err::Error(nom::error::Error::new(
        input,
        nom::error::ErrorKind::Verify,
    )))
}

/// Parse tuple value according to Cassandra format specification
pub fn parse_tuple(input: &[u8]) -> IResult<&[u8], Value> {
    // Parse field count
    let (input, field_count) = parse_vint_length_signed(input)?;

    // Parse field type definitions
    let mut field_types = Vec::with_capacity(field_count);
    let mut remaining = input;

    for _ in 0..field_count {
        let (new_remaining, field_type_id) = super::parse_cql_type_id(remaining)?;
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
            parse_cql_value_raw(field_data, field_type_id)?.1
        };

        fields.push(field_value);
    }

    Ok((remaining, Value::Tuple(fields)))
}

#[cfg(test)]
mod tests {
    use super::super::{serialize_cql_value, CqlTypeId};
    use super::*;

    #[test]
    fn test_udt_serialization() {
        // Test UDT serialization
        let udt = UdtValue {
            type_name: "Person".to_string(),
            keyspace: "test".to_string(),
            fields: vec![
                UdtField {
                    name: "name".to_string(),
                    value: Some(Value::text("John".to_string())),
                },
                UdtField {
                    name: "age".to_string(),
                    value: Some(Value::Integer(30)),
                },
                UdtField {
                    name: "email".to_string(),
                    value: None,
                }, // Null field
            ],
        };

        let serialized = serialize_cql_value(&Value::Udt(Box::new(udt))).unwrap();
        assert!(!serialized.is_empty());

        // Should start with UDT type ID
        assert_eq!(serialized[0], CqlTypeId::Udt as u8);
    }

    #[test]
    fn test_tuple_serialization() {
        // Test tuple serialization
        let tuple = vec![
            Value::text("hello".to_string()),
            Value::Integer(42),
            Value::Boolean(true),
        ];

        let serialized = serialize_cql_value(&Value::Tuple(tuple)).unwrap();
        assert!(!serialized.is_empty());

        // Should start with Tuple type ID
        assert_eq!(serialized[0], CqlTypeId::Tuple as u8);
    }

    #[test]
    fn test_parse_cql_value_for_type_frozen_int() {
        // 4 bytes of i32 BE representing the value 42
        let data: &[u8] = &[0x00, 0x00, 0x00, 0x2A];
        let cql_type = CqlType::Frozen(Box::new(CqlType::Int));
        let result = parse_cql_value_for_type(data, &cql_type)
            .expect("parse_cql_value_for_type should succeed for Frozen<Int>");
        assert_eq!(result, Value::Frozen(Box::new(Value::Integer(42))));
    }
}
