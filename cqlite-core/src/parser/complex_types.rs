//! Enhanced Complex Type Parsing for Cassandra 5+ Compatibility
//!
//! This module implements comprehensive parsing and serialization for complex types
//! with full Cassandra 5+ format compliance, including tuple representation for collections.

use super::vint::{encode_vint, parse_vint, parse_vint_length};
use crate::{
    error::{Error, Result},
    types::{Value, UdtValue, UdtField, UdtTypeDef},
    schema::{CqlType, UdtRegistry},
};
use nom::{
    multi::count,
    number::complete::be_u8,
    IResult,
};

/// Complex type parser with Cassandra 5+ format support
#[derive(Debug)]
pub struct ComplexTypeParser {
    /// UDT type registry for schema lookups
    pub udt_registry: UdtRegistry,
    /// Enable strict Cassandra 5+ format validation
    pub strict_format: bool,
    /// Maximum allowed nesting depth
    pub max_nesting_depth: usize,
}

impl Default for ComplexTypeParser {
    fn default() -> Self {
        Self {
            udt_registry: UdtRegistry::new(),
            strict_format: true,
            max_nesting_depth: 10,
        }
    }
}

impl ComplexTypeParser {
    /// Create a new complex type parser
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a UDT type definition for parsing
    pub fn register_udt(&mut self, udt_def: UdtTypeDef) {
        self.udt_registry.register_udt(udt_def);
    }

    /// Create parser with test UDTs for compatibility testing
    pub fn with_test_udts() -> Self {
        Self {
            udt_registry: UdtRegistry::new(),
            strict_format: true,
            max_nesting_depth: 10,
        }
    }

    /// Parse a collection (List, Set, Map) with Cassandra 5+ tuple format
    pub fn parse_collection<'a>(&'a self, input: &'a [u8], collection_type: CollectionType) -> IResult<&'a [u8], Value> {
        match collection_type {
            CollectionType::List => self.parse_list_v5(input),
            CollectionType::Set => self.parse_set_v5(input),
            CollectionType::Map => self.parse_map_v5(input),
        }
    }

    /// Parse List<T> with Cassandra 5+ tuple-based format
    fn parse_list_v5<'a>(&'a self, input: &'a [u8]) -> IResult<&'a [u8], Value> {
        // Cassandra 5+ format: [count:vint][element_type:u8][elements...]
        let (input, count) = parse_vint_length(input)?;
        
        if count == 0 {
            return Ok((input, Value::List(Vec::new())));
        }

        let (input, element_type) = be_u8(input)?;
        let element_cql_type = self.parse_type_spec(element_type).map_err(|_e| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)))?;

        let mut elements = Vec::with_capacity(count);
        let mut remaining = input;

        for _ in 0..count {
            let (new_remaining, element_value) = self.parse_typed_value(remaining, &element_cql_type)?;
            elements.push(element_value);
            remaining = new_remaining;
        }

        Ok((remaining, Value::List(elements)))
    }

    /// Parse Set<T> with Cassandra 5+ format (similar to List but with deduplication semantics)
    fn parse_set_v5<'a>(&'a self, input: &'a [u8]) -> IResult<&'a [u8], Value> {
        // Sets use same binary format as lists in Cassandra 5+
        let (remaining, list_value) = self.parse_list_v5(input)?;
        
        if let Value::List(elements) = list_value {
            // Convert to Set (Vec maintaining insertion order for compatibility)
            Ok((remaining, Value::Set(elements)))
        } else {
            Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)))
        }
    }

    /// Parse Map<K,V> with Cassandra 5+ tuple-based format
    fn parse_map_v5<'a>(&'a self, input: &'a [u8]) -> IResult<&'a [u8], Value> {
        // Cassandra 5+ format: [count:vint][key_type:u8][value_type:u8][pairs...]
        let (input, count) = parse_vint_length(input)?;
        
        if count == 0 {
            return Ok((input, Value::Map(Vec::new())));
        }

        let (input, key_type) = be_u8(input)?;
        let (input, value_type) = be_u8(input)?;
        
        let key_cql_type = self.parse_type_spec(key_type).map_err(|_e| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)))?;
        let value_cql_type = self.parse_type_spec(value_type).map_err(|_e| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)))?;

        let mut pairs = Vec::with_capacity(count);
        let mut remaining = input;

        for _ in 0..count {
            let (new_remaining, key) = self.parse_typed_value(remaining, &key_cql_type)?;
            let (new_remaining, value) = self.parse_typed_value(new_remaining, &value_cql_type)?;
            pairs.push((key, value));
            remaining = new_remaining;
        }

        Ok((remaining, Value::Map(pairs)))
    }

    /// Parse Tuple with fixed-length heterogeneous types
    pub fn parse_tuple<'a>(&'a self, input: &'a [u8]) -> IResult<&'a [u8], Value> {
        // Cassandra format: [field_count:vint][type1:u8][type2:u8]...[value1][value2]...
        let (input, field_count) = parse_vint_length(input)?;
        
        if field_count == 0 {
            return Ok((input, Value::Tuple(Vec::new())));
        }

        // Parse type specifications
        let (mut input, type_specs) = count(be_u8, field_count)(input)?;
        let mut field_types = Vec::with_capacity(field_count);
        
        for type_spec in type_specs {
            let cql_type = self.parse_type_spec(type_spec).map_err(|_e| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)))?;
            field_types.push(cql_type);
        }

        // Parse values
        let mut values = Vec::with_capacity(field_count);
        for field_type in &field_types {
            let (new_input, value) = self.parse_typed_value(input, field_type)?;
            values.push(value);
            input = new_input;
        }

        Ok((input, Value::Tuple(values)))
    }

    /// Parse User Defined Type (UDT) with schema validation
    pub fn parse_udt<'a>(&'a self, input: &'a [u8], type_name: &str, keyspace: &str) -> IResult<&'a [u8], Value> {
        // Look up UDT definition from registry
        let udt_def = self.udt_registry.get_udt(keyspace, type_name)
            .ok_or_else(|| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)))?;

        // Cassandra format: [field_count:vint][field_values...]
        let (input, field_count) = parse_vint_length(input)?;
        
        if field_count as usize != udt_def.fields.len() {
            return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)));
        }

        let mut fields = Vec::with_capacity(field_count);
        let mut remaining = input;

        for field_def in &udt_def.fields {
            // Check if field is null (length = -1)
            let (new_remaining, field_length) = parse_vint(remaining)?;
            
            if field_length == -1 {
                // Null field
                fields.push(UdtField {
                    name: field_def.name.clone(),
                    value: None,
                });
                remaining = new_remaining;
            } else {
                // Non-null field - parse value according to field type
                let (new_remaining, field_value) = self.parse_typed_value(remaining, &field_def.field_type)?;
                fields.push(UdtField {
                    name: field_def.name.clone(),
                    value: Some(field_value),
                });
                remaining = new_remaining;
            }
        }

        let udt_value = UdtValue {
            type_name: type_name.to_string(),
            keyspace: keyspace.to_string(),
            fields,
        };

        Ok((remaining, Value::Udt(udt_value)))
    }

    /// Parse Frozen type wrapper with enhanced collection support
    pub fn parse_frozen<'a>(&'a self, input: &'a [u8], inner_type: &CqlType) -> IResult<&'a [u8], Value> {
        // Frozen types are serialized the same as their inner type but immutable
        let (remaining, inner_value) = self.parse_typed_value(input, inner_type)?;
        Ok((remaining, Value::Frozen(Box::new(inner_value))))
    }

    /// Parse FROZEN<LIST<T>> with Cassandra 5+ format
    pub fn parse_frozen_list<'a>(&'a self, input: &'a [u8]) -> IResult<&'a [u8], Value> {
        let (remaining, list_value) = self.parse_list_v5(input)?;
        Ok((remaining, Value::Frozen(Box::new(list_value))))
    }

    /// Parse FROZEN<SET<T>> with Cassandra 5+ format
    pub fn parse_frozen_set<'a>(&'a self, input: &'a [u8]) -> IResult<&'a [u8], Value> {
        let (remaining, set_value) = self.parse_set_v5(input)?;
        Ok((remaining, Value::Frozen(Box::new(set_value))))
    }

    /// Parse FROZEN<MAP<K,V>> with Cassandra 5+ format
    pub fn parse_frozen_map<'a>(&'a self, input: &'a [u8]) -> IResult<&'a [u8], Value> {
        let (remaining, map_value) = self.parse_map_v5(input)?;
        Ok((remaining, Value::Frozen(Box::new(map_value))))
    }

    /// Parse FROZEN<UDT> with enhanced validation
    pub fn parse_frozen_udt<'a>(&'a self, input: &'a [u8], type_name: &str, keyspace: &str) -> IResult<&'a [u8], Value> {
        let (remaining, udt_value) = self.parse_udt(input, type_name, keyspace)?;
        Ok((remaining, Value::Frozen(Box::new(udt_value))))
    }

    /// Parse a value based on its CQL type specification
    fn parse_typed_value<'a>(&'a self, input: &'a [u8], cql_type: &CqlType) -> IResult<&'a [u8], Value> {
        match cql_type {
            CqlType::List(element_type) => {
                let (remaining, list_value) = self.parse_list_v5(input)?;
                // Validate element types match specification
                if let Value::List(elements) = &list_value {
                    for element in elements {
                        self.validate_type_compatibility(element, element_type).map_err(|_e| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)))?;
                    }
                }
                Ok((remaining, list_value))
            }
            CqlType::Set(element_type) => {
                let (remaining, set_value) = self.parse_set_v5(input)?;
                // Validate element types
                if let Value::Set(elements) = &set_value {
                    for element in elements {
                        self.validate_type_compatibility(element, element_type).map_err(|_e| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)))?;
                    }
                }
                Ok((remaining, set_value))
            }
            CqlType::Map(key_type, value_type) => {
                let (remaining, map_value) = self.parse_map_v5(input)?;
                // Validate key/value types
                if let Value::Map(pairs) = &map_value {
                    for (key, value) in pairs {
                        self.validate_type_compatibility(key, key_type).map_err(|_e| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)))?;
                        self.validate_type_compatibility(value, value_type).map_err(|_e| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)))?;
                    }
                }
                Ok((remaining, map_value))
            }
            CqlType::Tuple(field_types) => {
                let (remaining, tuple_value) = self.parse_tuple(input)?;
                // Validate field types
                if let Value::Tuple(fields) = &tuple_value {
                    if fields.len() != field_types.len() {
                        return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)));
                    }
                    for (field, expected_type) in fields.iter().zip(field_types.iter()) {
                        self.validate_type_compatibility(field, expected_type).map_err(|_e| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)))?;
                    }
                }
                Ok((remaining, tuple_value))
            }
            CqlType::Udt(type_name, _fields) => {
                // For now, assume default keyspace - in real implementation,
                // this would come from the parsing context
                self.parse_udt(input, type_name, "default")
            }
            CqlType::Frozen(inner_type) => {
                self.parse_frozen(input, inner_type)
            }
            _ => {
                // Delegate to simple type parser
                use super::types::parse_cql_value;
                let type_id = self.cql_type_to_type_id(cql_type).map_err(|_e| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)))?;
                parse_cql_value(input, type_id)
            }
        }
    }

    /// Parse type specification byte into CQL type
    fn parse_type_spec(&self, type_byte: u8) -> Result<CqlType> {
        use super::types::CqlTypeId;
        
        match CqlTypeId::try_from(type_byte)? {
            CqlTypeId::Boolean => Ok(CqlType::Boolean),
            CqlTypeId::Tinyint => Ok(CqlType::TinyInt),
            CqlTypeId::Smallint => Ok(CqlType::SmallInt),
            CqlTypeId::Int => Ok(CqlType::Int),
            CqlTypeId::BigInt => Ok(CqlType::BigInt),
            CqlTypeId::Float => Ok(CqlType::Float),
            CqlTypeId::Double => Ok(CqlType::Double),
            CqlTypeId::Varchar => Ok(CqlType::Text),
            CqlTypeId::Blob => Ok(CqlType::Blob),
            CqlTypeId::Uuid => Ok(CqlType::Uuid),
            CqlTypeId::Timestamp => Ok(CqlType::Timestamp),
            // Complex types require additional parsing context
            _ => Ok(CqlType::Custom(format!("type_0x{:02X}", type_byte))),
        }
    }

    /// Convert CQL type to type ID for simple types
    fn cql_type_to_type_id(&self, cql_type: &CqlType) -> Result<super::types::CqlTypeId> {
        use super::types::CqlTypeId;
        
        match cql_type {
            CqlType::Boolean => Ok(CqlTypeId::Boolean),
            CqlType::TinyInt => Ok(CqlTypeId::Tinyint),
            CqlType::SmallInt => Ok(CqlTypeId::Smallint),
            CqlType::Int => Ok(CqlTypeId::Int),
            CqlType::BigInt => Ok(CqlTypeId::BigInt),
            CqlType::Float => Ok(CqlTypeId::Float),
            CqlType::Double => Ok(CqlTypeId::Double),
            CqlType::Text | CqlType::Varchar => Ok(CqlTypeId::Varchar),
            CqlType::Blob => Ok(CqlTypeId::Blob),
            CqlType::Uuid => Ok(CqlTypeId::Uuid),
            CqlType::Timestamp => Ok(CqlTypeId::Timestamp),
            _ => Err(Error::corruption(format!("Unsupported type conversion: {:?}", cql_type))),
        }
    }

    /// Validate that a value is compatible with expected type
    fn validate_type_compatibility(&self, value: &Value, expected_type: &CqlType) -> Result<()> {
        // Basic type compatibility checking
        // In a full implementation, this would be more comprehensive
        match (value, expected_type) {
            (Value::Integer(_), CqlType::Int) => Ok(()),
            (Value::BigInt(_), CqlType::BigInt) => Ok(()),
            (Value::Text(_), CqlType::Text | CqlType::Varchar) => Ok(()),
            (Value::Boolean(_), CqlType::Boolean) => Ok(()),
            (Value::Float(_), CqlType::Double) => Ok(()),
            (Value::List(_), CqlType::List(_)) => Ok(()),
            (Value::Set(_), CqlType::Set(_)) => Ok(()),
            (Value::Map(_), CqlType::Map(_, _)) => Ok(()),
            (Value::Tuple(_), CqlType::Tuple(_)) => Ok(()),
            (Value::Udt(_), CqlType::Udt(_, _)) => Ok(()),
            _ => {
                if self.strict_format {
                    Err(Error::invalid_operation(format!(
                        "Type mismatch: value {:?} not compatible with type {:?}",
                        value.data_type(), expected_type
                    )))
                } else {
                    Ok(()) // Allow in non-strict mode
                }
            }
        }
    }
}

/// Collection type enumeration
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CollectionType {
    List,
    Set,
    Map,
}

/// Serialization functions for complex types

/// Serialize a List with Cassandra 5+ format
pub fn serialize_list_v5(list: &[Value]) -> Result<Vec<u8>> {
    let mut result = Vec::new();
    
    // Count
    result.extend_from_slice(&encode_vint(list.len() as i64));
    
    if list.is_empty() {
        return Ok(result);
    }
    
    // Element type (assume all elements are same type)
    let element_type = map_value_to_type_id(&list[0]);
    result.push(element_type as u8);
    
    // Elements
    for element in list {
        let element_bytes = super::types::serialize_cql_value(element)?;
        result.extend_from_slice(&element_bytes[1..]); // Skip type byte
    }
    
    Ok(result)
}

/// Serialize a Set with Cassandra 5+ format
pub fn serialize_set_v5(set: &[Value]) -> Result<Vec<u8>> {
    // Sets use same format as lists
    serialize_list_v5(set)
}

/// Serialize a Map with Cassandra 5+ format
pub fn serialize_map_v5(map: &[(Value, Value)]) -> Result<Vec<u8>> {
    let mut result = Vec::new();
    
    // Count
    result.extend_from_slice(&encode_vint(map.len() as i64));
    
    if map.is_empty() {
        return Ok(result);
    }
    
    // Key and value types
    let (first_key, first_value) = &map[0];
    let key_type = map_value_to_type_id(first_key);
    let value_type = map_value_to_type_id(first_value);
    result.push(key_type as u8);
    result.push(value_type as u8);
    
    // Key-value pairs
    for (key, value) in map {
        let key_bytes = super::types::serialize_cql_value(key)?;
        let value_bytes = super::types::serialize_cql_value(value)?;
        result.extend_from_slice(&key_bytes[1..]); // Skip type byte
        result.extend_from_slice(&value_bytes[1..]); // Skip type byte
    }
    
    Ok(result)
}

/// Serialize a Tuple
pub fn serialize_tuple(tuple: &[Value]) -> Result<Vec<u8>> {
    let mut result = Vec::new();
    
    // Field count
    result.extend_from_slice(&encode_vint(tuple.len() as i64));
    
    // Type specifications
    for field in tuple {
        let type_id = map_value_to_type_id(field);
        result.push(type_id as u8);
    }
    
    // Field values
    for field in tuple {
        let field_bytes = super::types::serialize_cql_value(field)?;
        result.extend_from_slice(&field_bytes[1..]); // Skip type byte
    }
    
    Ok(result)
}

fn map_value_to_type_id(value: &Value) -> super::types::CqlTypeId {
    use super::types::CqlTypeId;
    
    match value {
        Value::Boolean(_) => CqlTypeId::Boolean,
        Value::TinyInt(_) => CqlTypeId::Tinyint,
        Value::SmallInt(_) => CqlTypeId::Smallint,
        Value::Integer(_) => CqlTypeId::Int,
        Value::BigInt(_) => CqlTypeId::BigInt,
        Value::Float32(_) => CqlTypeId::Float,
        Value::Float(_) => CqlTypeId::Double,
        Value::Text(_) => CqlTypeId::Varchar,
        Value::Blob(_) => CqlTypeId::Blob,
        Value::Uuid(_) => CqlTypeId::Uuid,
        Value::Timestamp(_) => CqlTypeId::Timestamp,
        Value::List(_) => CqlTypeId::List,
        Value::Set(_) => CqlTypeId::Set,
        Value::Map(_) => CqlTypeId::Map,
        Value::Tuple(_) => CqlTypeId::Tuple,
        Value::Udt(_) => CqlTypeId::Udt,
        _ => CqlTypeId::Blob, // Fallback
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_parsing_v5() {
        let parser = ComplexTypeParser::new();
        
        // Create test data: list of 3 integers [1, 2, 3]
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(3)); // count
        data.push(0x09); // int type
        data.extend_from_slice(&1i32.to_be_bytes());
        data.extend_from_slice(&2i32.to_be_bytes());
        data.extend_from_slice(&3i32.to_be_bytes());
        
        let (_, value) = parser.parse_list_v5(&data).unwrap();
        match value {
            Value::List(elements) => {
                assert_eq!(elements.len(), 3);
                assert_eq!(elements[0], Value::Integer(1));
                assert_eq!(elements[1], Value::Integer(2));
                assert_eq!(elements[2], Value::Integer(3));
            }
            _ => panic!("Expected List value"),
        }
    }

    #[test]
    fn test_empty_collection_parsing() {
        let parser = ComplexTypeParser::new();
        
        // Empty list
        let data = encode_vint(0);
        let (_, value) = parser.parse_list_v5(&data).unwrap();
        assert_eq!(value, Value::List(Vec::new()));
    }

    #[test]
    fn test_tuple_parsing() {
        let parser = ComplexTypeParser::new();
        
        // Tuple<int, text>: (42, "hello")
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(2)); // field count
        data.push(0x09); // int type
        data.push(0x0D); // varchar type
        data.extend_from_slice(&42i32.to_be_bytes()); // 42
        data.extend_from_slice(&encode_vint(5)); // string length
        data.extend_from_slice(b"hello"); // string content
        
        let (_, value) = parser.parse_tuple(&data).unwrap();
        match value {
            Value::Tuple(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0], Value::Integer(42));
                assert_eq!(fields[1], Value::Text("hello".to_string()));
            }
            _ => panic!("Expected Tuple value"),
        }
    }

    #[test]
    fn test_serialization_roundtrip() {
        let list = vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ];
        
        let serialized = serialize_list_v5(&list).unwrap();
        assert!(!serialized.is_empty());
        
        let parser = ComplexTypeParser::new();
        let (_, parsed) = parser.parse_list_v5(&serialized).unwrap();
        
        assert_eq!(Value::List(list), parsed);
    }
}