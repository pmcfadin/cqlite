# Cassandra User Defined Types (UDT) Binary Format Specification

## 🎯 Critical Compatibility Document

> This specification documents the exact byte-level format for Apache Cassandra User Defined Types (UDTs) in SSTable format. Every detail must be implemented exactly as specified to ensure 100% compatibility.

## Table of Contents

1. [UDT Overview](#udt-overview)
2. [Binary Format Specification](#binary-format-specification)
3. [Frozen vs Non-Frozen UDTs](#frozen-vs-non-frozen-udts)
4. [Field Serialization](#field-serialization)
5. [Null Field Handling](#null-field-handling)
6. [Nested UDT Support](#nested-udt-support)
7. [Schema Metadata](#schema-metadata)
8. [Tuple Type Format](#tuple-type-format)
9. [Implementation Examples](#implementation-examples)
10. [Test Cases](#test-cases)
11. [Integration with Value Enum](#integration-with-value-enum)

---

## UDT Overview

### Definition
A User Defined Type (UDT) is a named collection of fields, where each field has a name and a type. UDTs enable complex data structures within Cassandra tables.

### Key Characteristics
- **Field Ordering**: Fields are serialized in the order defined in the type schema
- **Sparse Support**: UDTs can have fewer values than defined fields (missing fields)
- **Type Safety**: Each field must conform to its declared type
- **Nesting**: UDTs can contain other UDTs, collections, and primitive types

### Example UDT Schema
```cql
CREATE TYPE address (
  street TEXT,
  city TEXT,
  state TEXT,
  zip_code TEXT
);

CREATE TYPE person (
  name TEXT,
  age INT,
  address FROZEN<address>
);
```

---

## Binary Format Specification

### UDT Type Identifier
```
Type ID: 0x30 (CqlTypeId::Udt)
```

### Complete UDT Binary Format
```
┌─────────────────────────────────────────────────────────────┐
│                    UDT Binary Format                       │
├─────────────────────────────────────────────────────────────┤
│ Header                                                      │
│ ┌─────────────┬─────────────┬─────────────┬─────────────┐   │
│ │Type ID(1)   │UDT Name Len │UDT Name     │Field Count  │   │
│ │0x30         │(VInt)       │(UTF-8)      │(VInt)       │   │
│ └─────────────┴─────────────┴─────────────┴─────────────┘   │
├─────────────────────────────────────────────────────────────┤
│ Field Definitions (Schema Metadata)                        │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ Field 1:                                                │ │
│ │ ├─ Name Length (VInt)                                   │ │
│ │ ├─ Name (UTF-8)                                         │ │
│ │ ├─ Type ID (1 byte)                                     │ │
│ │ └─ Type Info (Variable, depends on type)               │ │
│ │                                                         │ │
│ │ Field 2...N: (Same structure)                          │ │
│ └─────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│ Field Values (Actual Data)                                 │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ Field 1 Value:                                          │ │
│ │ ├─ Length (4 bytes, big-endian)                        │ │
│ │ │   -1 = NULL, 0 = empty, >0 = data length             │ │
│ │ └─ Data (Variable, encoded per field type)             │ │
│ │                                                         │ │
│ │ Field 2...N Values: (Same structure)                   │ │
│ └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### Critical Format Requirements

#### 1. Endianness
- **All multi-byte integers**: Big-endian (network byte order)
- **Length fields**: 4-byte signed integers (i32)
- **VInt encoding**: Cassandra-specific variable-length integers

#### 2. Field Length Encoding
```rust
// Field length meanings
match length {
    -1 => FieldValue::Null,           // NULL field
    0  => FieldValue::Empty,          // Empty but present field
    n  => FieldValue::Data(n bytes),  // n bytes of actual data
}
```

#### 3. UTF-8 String Encoding
- **Type names**: Valid UTF-8, length-prefixed with VInt
- **Field names**: Valid UTF-8, length-prefixed with VInt
- **Text field values**: Valid UTF-8 without additional length prefix (length already in field header)

---

## Frozen vs Non-Frozen UDTs

### Frozen UDT Characteristics
- **Single Blob**: Entire UDT serialized as one unit
- **No Field Updates**: Must replace entire UDT to modify
- **Type Wrapper**: Enclosed in `FrozenType` wrapper in schema
- **Common Usage**: Recommended for most scenarios

### Non-Frozen UDT Characteristics
- **Individual Fields**: Each field can be updated separately
- **Complex Storage**: Stored as multiple cells in Cassandra
- **Type Handling**: Direct UDT type without wrapper
- **Limited Usage**: Rarely used, complex to handle

### Serialization Header Differences
```rust
// Frozen UDT in serialization header
"org.apache.cassandra.db.marshal.FrozenType(
    org.apache.cassandra.db.marshal.UserType(
        keyspace_name, type_name, field1_type, field2_type, ...
    )
)"

// Non-frozen UDT in serialization header  
"org.apache.cassandra.db.marshal.UserType(
    keyspace_name, type_name, field1_type, field2_type, ...
)"
```

---

## Field Serialization

### Primitive Field Types
```rust
impl FieldSerialization {
    fn serialize_field(value: &Value, field_type: &CqlType) -> Result<Vec<u8>> {
        match field_type {
            CqlType::Text => {
                // UTF-8 string, no additional length prefix
                Ok(value.as_str().unwrap().as_bytes().to_vec())
            }
            CqlType::Int => {
                // 4-byte big-endian signed integer
                Ok(value.as_i32().unwrap().to_be_bytes().to_vec())
            }
            CqlType::BigInt => {
                // 8-byte big-endian signed integer
                Ok(value.as_i64().unwrap().to_be_bytes().to_vec())
            }
            CqlType::Boolean => {
                // 1 byte: 0x00 = false, 0x01 = true
                Ok(vec![if value.as_bool().unwrap() { 1 } else { 0 }])
            }
            CqlType::Uuid => {
                // 16 bytes in network byte order
                Ok(value.as_uuid().unwrap().to_vec())
            }
            CqlType::Timestamp => {
                // 8-byte big-endian milliseconds since epoch
                let millis = value.as_timestamp().unwrap() / 1000; // Convert µs to ms
                Ok(millis.to_be_bytes().to_vec())
            }
            // ... other primitive types
        }
    }
}
```

### Collection Field Types
```rust
impl FieldSerialization {
    fn serialize_list_field(list: &[Value], element_type: &CqlType) -> Result<Vec<u8>> {
        let mut result = Vec::new();
        
        // Element count (4 bytes big-endian)
        result.extend_from_slice(&(list.len() as i32).to_be_bytes());
        
        // Each element with length prefix
        for element in list {
            let element_bytes = Self::serialize_field(element, element_type)?;
            result.extend_from_slice(&(element_bytes.len() as i32).to_be_bytes());
            result.extend_from_slice(&element_bytes);
        }
        
        Ok(result)
    }
    
    fn serialize_map_field(
        map: &[(Value, Value)], 
        key_type: &CqlType, 
        value_type: &CqlType
    ) -> Result<Vec<u8>> {
        let mut result = Vec::new();
        
        // Pair count (4 bytes big-endian)
        result.extend_from_slice(&(map.len() as i32).to_be_bytes());
        
        // Each key-value pair with length prefixes
        for (key, value) in map {
            // Key with length
            let key_bytes = Self::serialize_field(key, key_type)?;
            result.extend_from_slice(&(key_bytes.len() as i32).to_be_bytes());
            result.extend_from_slice(&key_bytes);
            
            // Value with length
            let value_bytes = Self::serialize_field(value, value_type)?;
            result.extend_from_slice(&(value_bytes.len() as i32).to_be_bytes());
            result.extend_from_slice(&value_bytes);
        }
        
        Ok(result)
    }
}
```

---

## Null Field Handling

### Null Representation
```
Field Length: -1 (0xFFFFFFFF in big-endian)
Field Data: (none - zero bytes)
```

### Sparse UDT Support
UDTs support sparse representation where trailing fields can be omitted:

```rust
// UDT with 4 defined fields but only 2 values present
struct SparsePerson {
    name: Some("John"),     // Present
    age: Some(30),          // Present  
    email: None,            // Omitted (implicit null)
    phone: None,            // Omitted (implicit null)
}

// Binary representation only includes first 2 fields
// Trailing omitted fields are implicitly null
```

### Null Handling Rules
1. **Explicit Null**: Field present with length -1
2. **Implicit Null**: Field omitted entirely (sparse representation)
3. **Empty Value**: Field present with length 0
4. **Present Value**: Field present with length > 0

---

## Nested UDT Support

### Nested UDT Serialization
```rust
// Example: Person UDT containing Address UDT
struct Person {
    name: String,
    age: i32,
    address: Address,  // Nested UDT
}

struct Address {
    street: String,
    city: String,
    state: String,
    zip_code: String,
}
```

### Nested Serialization Format
```
Person UDT:
├─ name: [length][data]
├─ age: [length][data]  
└─ address: [length][nested_udt_data]
   └─ nested_udt_data:
      ├─ street: [length][data]
      ├─ city: [length][data]
      ├─ state: [length][data]
      └─ zip_code: [length][data]
```

### Frozen Nested UDTs
```rust
// When address is FROZEN<address>
fn serialize_nested_frozen_udt(address: &Address) -> Result<Vec<u8>> {
    let mut nested_data = Vec::new();
    
    // Serialize all fields of nested UDT
    for field in &address.fields {
        let field_bytes = serialize_field(field)?;
        nested_data.extend_from_slice(&(field_bytes.len() as i32).to_be_bytes());
        nested_data.extend_from_slice(&field_bytes);
    }
    
    // Return as single blob for parent UDT field
    Ok(nested_data)
}
```

---

## Schema Metadata

### UDT Schema Storage
UDT metadata is stored in the SSTable serialization header:

```
Schema Information:
├─ Keyspace: test_keyspace
├─ Type Name: person
├─ Fields:
│  ├─ name: TEXT
│  ├─ age: INT  
│  └─ address: FROZEN<address>
└─ Nested Types:
   └─ address:
      ├─ street: TEXT
      ├─ city: TEXT
      ├─ state: TEXT
      └─ zip_code: TEXT
```

### Type Name Encoding
```rust
// In serialization header
fn encode_udt_type_name(keyspace: &str, type_name: &str, fields: &[FieldDef]) -> String {
    let field_types: Vec<String> = fields.iter()
        .map(|f| f.type_name.clone())
        .collect();
    
    format!(
        "org.apache.cassandra.db.marshal.UserType({},{},{})",
        keyspace,
        type_name,
        field_types.join(",")
    )
}
```

---

## Tuple Type Format

### Tuple vs UDT Differences
```
Tuple Characteristics:
├─ Always frozen (implicit)
├─ Positional fields (no names)
├─ Fixed field count
└─ Type ID: 0x31

UDT Characteristics:
├─ Can be frozen or non-frozen
├─ Named fields
├─ Variable field count (sparse)
└─ Type ID: 0x30
```

### Tuple Binary Format
```
┌─────────────────────────────────────────────────────────────┐
│                   Tuple Binary Format                      │
├─────────────────────────────────────────────────────────────┤
│ Header                                                      │
│ ┌─────────────┬─────────────┬─────────────┐                 │
│ │Type ID(1)   │Field Count  │Field Types  │                 │
│ │0x31         │(VInt)       │(Array)      │                 │
│ └─────────────┴─────────────┴─────────────┘                 │
├─────────────────────────────────────────────────────────────┤
│ Field Values                                                │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ Field 1: [Length][Data]                                 │ │
│ │ Field 2: [Length][Data]                                 │ │
│ │ ...                                                     │ │
│ │ Field N: [Length][Data]                                 │ │
│ └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### Tuple vs UDT Type Names
```rust
// Tuple type name (never wrapped in FrozenType)
"org.apache.cassandra.db.marshal.TupleType(text,int,boolean)"

// UDT type name (wrapped in FrozenType when frozen)
"org.apache.cassandra.db.marshal.FrozenType(
    org.apache.cassandra.db.marshal.UserType(keyspace,type_name,text,int)
)"
```

---

## Implementation Examples

### UDT Parsing Implementation
```rust
use nom::{
    bytes::complete::take,
    number::complete::be_i32,
    IResult,
};

#[derive(Debug, Clone)]
pub struct UdtValue {
    pub type_name: String,
    pub fields: Vec<UdtField>,
}

#[derive(Debug, Clone)]
pub struct UdtField {
    pub name: String,
    pub value: Option<Value>,
}

pub fn parse_udt_value(input: &[u8], type_def: &UdtTypeDef) -> IResult<&[u8], UdtValue> {
    let mut remaining = input;
    let mut fields = Vec::new();
    
    // Parse each field according to schema order
    for (i, field_def) in type_def.fields.iter().enumerate() {
        if remaining.is_empty() {
            // Sparse UDT - remaining fields are implicitly null
            fields.push(UdtField {
                name: field_def.name.clone(),
                value: None,
            });
            continue;
        }
        
        // Parse field length
        let (new_remaining, length) = be_i32(remaining)?;
        remaining = new_remaining;
        
        let field_value = if length == -1 {
            // Explicit null field
            None
        } else if length == 0 {
            // Empty field
            Some(Value::from_empty(&field_def.field_type)?)
        } else {
            // Field with data
            let (new_remaining, field_data) = take(length as usize)(remaining)?;
            remaining = new_remaining;
            Some(parse_field_value(field_data, &field_def.field_type)?)
        };
        
        fields.push(UdtField {
            name: field_def.name.clone(),
            value: field_value,
        });
    }
    
    Ok((remaining, UdtValue {
        type_name: type_def.name.clone(),
        fields,
    }))
}

pub fn serialize_udt_value(udt: &UdtValue, type_def: &UdtTypeDef) -> Result<Vec<u8>> {
    let mut result = Vec::new();
    
    // Serialize fields in schema order
    for field_def in &type_def.fields {
        let field_value = udt.fields.iter()
            .find(|f| f.name == field_def.name)
            .and_then(|f| f.value.as_ref());
        
        match field_value {
            None => {
                // Null field
                result.extend_from_slice(&(-1i32).to_be_bytes());
            }
            Some(value) => {
                let field_bytes = serialize_field_value(value, &field_def.field_type)?;
                result.extend_from_slice(&(field_bytes.len() as i32).to_be_bytes());
                result.extend_from_slice(&field_bytes);
            }
        }
    }
    
    Ok(result)
}

fn parse_field_value(data: &[u8], field_type: &CqlType) -> Result<Value> {
    match field_type {
        CqlType::Text => {
            let text = String::from_utf8(data.to_vec())?;
            Ok(Value::Text(text))
        }
        CqlType::Int => {
            if data.len() != 4 {
                return Err(Error::InvalidLength);
            }
            let value = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
            Ok(Value::Integer(value))
        }
        CqlType::BigInt => {
            if data.len() != 8 {
                return Err(Error::InvalidLength);
            }
            let value = i64::from_be_bytes([
                data[0], data[1], data[2], data[3],
                data[4], data[5], data[6], data[7]
            ]);
            Ok(Value::BigInt(value))
        }
        CqlType::Boolean => {
            if data.len() != 1 {
                return Err(Error::InvalidLength);
            }
            Ok(Value::Boolean(data[0] != 0))
        }
        CqlType::Uuid => {
            if data.len() != 16 {
                return Err(Error::InvalidLength);
            }
            let mut uuid = [0u8; 16];
            uuid.copy_from_slice(data);
            Ok(Value::Uuid(uuid))
        }
        CqlType::Udt(nested_type_def) => {
            let (_, nested_udt) = parse_udt_value(data, nested_type_def)?;
            Ok(Value::Udt(nested_udt))
        }
        // ... handle other types
        _ => Err(Error::UnsupportedType),
    }
}
```

### Tuple Parsing Implementation
```rust
pub fn parse_tuple_value(input: &[u8], field_types: &[CqlType]) -> IResult<&[u8], TupleValue> {
    let mut remaining = input;
    let mut fields = Vec::new();
    
    // Tuples must have exact field count (no sparse representation)
    for field_type in field_types {
        let (new_remaining, length) = be_i32(remaining)?;
        remaining = new_remaining;
        
        let field_value = if length == -1 {
            None // Null field
        } else {
            let (new_remaining, field_data) = take(length as usize)(remaining)?;
            remaining = new_remaining;
            Some(parse_field_value(field_data, field_type)?)
        };
        
        fields.push(field_value);
    }
    
    Ok((remaining, TupleValue { fields }))
}
```

---

## Test Cases

### Basic UDT Test Case
```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_simple_udt_serialization() {
        // Define address UDT type
        let address_type = UdtTypeDef {
            keyspace: "test".to_string(),
            name: "address".to_string(),
            fields: vec![
                FieldDef { name: "street".to_string(), field_type: CqlType::Text },
                FieldDef { name: "city".to_string(), field_type: CqlType::Text },
                FieldDef { name: "state".to_string(), field_type: CqlType::Text },
                FieldDef { name: "zip_code".to_string(), field_type: CqlType::Text },
            ],
        };
        
        // Create address value
        let address = UdtValue {
            type_name: "address".to_string(),
            fields: vec![
                UdtField { name: "street".to_string(), value: Some(Value::Text("123 Main St".to_string())) },
                UdtField { name: "city".to_string(), value: Some(Value::Text("Anytown".to_string())) },
                UdtField { name: "state".to_string(), value: Some(Value::Text("CA".to_string())) },
                UdtField { name: "zip_code".to_string(), value: Some(Value::Text("12345".to_string())) },
            ],
        };
        
        // Test serialization
        let serialized = serialize_udt_value(&address, &address_type).unwrap();
        
        // Test deserialization
        let (remaining, deserialized) = parse_udt_value(&serialized, &address_type).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(deserialized.type_name, address.type_name);
        assert_eq!(deserialized.fields.len(), address.fields.len());
        
        // Verify each field
        for (original, parsed) in address.fields.iter().zip(deserialized.fields.iter()) {
            assert_eq!(original.name, parsed.name);
            assert_eq!(original.value, parsed.value);
        }
    }
    
    #[test]
    fn test_sparse_udt() {
        let person_type = UdtTypeDef {
            keyspace: "test".to_string(),
            name: "person".to_string(),
            fields: vec![
                FieldDef { name: "name".to_string(), field_type: CqlType::Text },
                FieldDef { name: "age".to_string(), field_type: CqlType::Int },
                FieldDef { name: "email".to_string(), field_type: CqlType::Text },
                FieldDef { name: "phone".to_string(), field_type: CqlType::Text },
            ],
        };
        
        // Sparse person with only name and age
        let person = UdtValue {
            type_name: "person".to_string(),
            fields: vec![
                UdtField { name: "name".to_string(), value: Some(Value::Text("John".to_string())) },
                UdtField { name: "age".to_string(), value: Some(Value::Integer(30)) },
                // email and phone are omitted (implicit null)
            ],
        };
        
        let serialized = serialize_udt_value(&person, &person_type).unwrap();
        let (_, deserialized) = parse_udt_value(&serialized, &person_type).unwrap();
        
        // Should have all 4 fields, last 2 are null
        assert_eq!(deserialized.fields.len(), 4);
        assert!(deserialized.fields[0].value.is_some());
        assert!(deserialized.fields[1].value.is_some());
        assert!(deserialized.fields[2].value.is_none());
        assert!(deserialized.fields[3].value.is_none());
    }
    
    #[test]
    fn test_nested_udt() {
        // Define nested UDT structure
        let address_type = UdtTypeDef {
            keyspace: "test".to_string(),
            name: "address".to_string(),
            fields: vec![
                FieldDef { name: "street".to_string(), field_type: CqlType::Text },
                FieldDef { name: "city".to_string(), field_type: CqlType::Text },
            ],
        };
        
        let person_type = UdtTypeDef {
            keyspace: "test".to_string(),
            name: "person".to_string(),
            fields: vec![
                FieldDef { name: "name".to_string(), field_type: CqlType::Text },
                FieldDef { name: "address".to_string(), field_type: CqlType::Udt(address_type.clone()) },
            ],
        };
        
        // Create nested UDT value
        let address = UdtValue {
            type_name: "address".to_string(),
            fields: vec![
                UdtField { name: "street".to_string(), value: Some(Value::Text("123 Main St".to_string())) },
                UdtField { name: "city".to_string(), value: Some(Value::Text("Anytown".to_string())) },
            ],
        };
        
        let person = UdtValue {
            type_name: "person".to_string(),
            fields: vec![
                UdtField { name: "name".to_string(), value: Some(Value::Text("John".to_string())) },
                UdtField { name: "address".to_string(), value: Some(Value::Udt(address)) },
            ],
        };
        
        // Test nested serialization/deserialization
        let serialized = serialize_udt_value(&person, &person_type).unwrap();
        let (_, deserialized) = parse_udt_value(&serialized, &person_type).unwrap();
        
        assert_eq!(deserialized.fields.len(), 2);
        assert!(deserialized.fields[1].value.is_some());
        
        if let Some(Value::Udt(nested_address)) = &deserialized.fields[1].value {
            assert_eq!(nested_address.fields.len(), 2);
            assert_eq!(nested_address.fields[0].name, "street");
            assert_eq!(nested_address.fields[1].name, "city");
        } else {
            panic!("Expected nested UDT");
        }
    }
    
    #[test]
    fn test_null_fields() {
        let person_type = UdtTypeDef {
            keyspace: "test".to_string(),
            name: "person".to_string(),
            fields: vec![
                FieldDef { name: "name".to_string(), field_type: CqlType::Text },
                FieldDef { name: "age".to_string(), field_type: CqlType::Int },
            ],
        };
        
        // Person with explicit null age
        let person = UdtValue {
            type_name: "person".to_string(),
            fields: vec![
                UdtField { name: "name".to_string(), value: Some(Value::Text("John".to_string())) },
                UdtField { name: "age".to_string(), value: None },
            ],
        };
        
        let serialized = serialize_udt_value(&person, &person_type).unwrap();
        
        // Check that null field is encoded as -1 length
        let age_length_offset = 4 + "John".len() + 4; // name length + name + age length
        let age_length = i32::from_be_bytes([
            serialized[age_length_offset],
            serialized[age_length_offset + 1], 
            serialized[age_length_offset + 2],
            serialized[age_length_offset + 3]
        ]);
        assert_eq!(age_length, -1);
    }
    
    #[test]
    fn test_tuple_parsing() {
        let field_types = vec![CqlType::Text, CqlType::Int, CqlType::Boolean];
        
        // Create tuple binary data
        let mut data = Vec::new();
        
        // Field 1: "hello" (length 5 + data)
        data.extend_from_slice(&5i32.to_be_bytes());
        data.extend_from_slice(b"hello");
        
        // Field 2: 42 (length 4 + data)
        data.extend_from_slice(&4i32.to_be_bytes());
        data.extend_from_slice(&42i32.to_be_bytes());
        
        // Field 3: null (length -1)
        data.extend_from_slice(&(-1i32).to_be_bytes());
        
        let (remaining, tuple) = parse_tuple_value(&data, &field_types).unwrap();
        assert!(remaining.is_empty());
        assert_eq!(tuple.fields.len(), 3);
        
        assert_eq!(tuple.fields[0], Some(Value::Text("hello".to_string())));
        assert_eq!(tuple.fields[1], Some(Value::Integer(42)));
        assert_eq!(tuple.fields[2], None);
    }
}
```

### Real-World Test Case (Cassandra Generated Data)
```rust
#[test] 
fn test_cassandra_compatibility() {
    // Test data generated by real Cassandra instance
    // Based on schema from create-keyspaces-fixed.cql
    
    // person UDT from Cassandra test data:
    // { name: 'John Doe', age: 30, address: { street: '123 Main St', city: 'Anytown', state: 'CA', zip_code: '12345' } }
    
    let cassandra_generated_bytes = vec![
        // This would be actual bytes from SSTable
        // Format: [name_len][name_data][age_len][age_data][addr_len][addr_data]
        // where addr_data is nested UDT with same format
    ];
    
    let person_type = load_schema_from_cassandra("test_keyspace", "person");
    let (_, parsed_person) = parse_udt_value(&cassandra_generated_bytes, &person_type).unwrap();
    
    // Verify parsed data matches expected values
    assert_eq!(get_field_value(&parsed_person, "name"), Some(Value::Text("John Doe".to_string())));
    assert_eq!(get_field_value(&parsed_person, "age"), Some(Value::Integer(30)));
    
    // Verify nested address UDT
    if let Some(Value::Udt(address)) = get_field_value(&parsed_person, "address") {
        assert_eq!(get_field_value(&address, "street"), Some(Value::Text("123 Main St".to_string())));
        assert_eq!(get_field_value(&address, "city"), Some(Value::Text("Anytown".to_string())));
        assert_eq!(get_field_value(&address, "state"), Some(Value::Text("CA".to_string())));
        assert_eq!(get_field_value(&address, "zip_code"), Some(Value::Text("12345".to_string())));
    } else {
        panic!("Expected nested address UDT");
    }
}
```

---

## Integration with Value Enum

### Extended Value Enum
```rust
use std::collections::HashMap;

/// Enhanced Value enum with UDT and Tuple support
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    // Existing types...
    Null,
    Boolean(bool),
    Integer(i32),
    BigInt(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
    Timestamp(i64),
    Uuid([u8; 16]),
    Json(serde_json::Value),
    TinyInt(i8),
    SmallInt(i16),
    Float32(f32),
    List(Vec<Value>),
    Set(Vec<Value>),
    Map(Vec<(Value, Value)>),
    
    // New UDT and Tuple support
    Udt(UdtValue),
    Tuple(TupleValue),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UdtValue {
    pub type_name: String,
    pub keyspace: String,
    pub fields: Vec<UdtField>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UdtField {
    pub name: String,
    pub value: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TupleValue {
    pub fields: Vec<Option<Value>>,
}

impl Value {
    /// Get the data type of this value
    pub fn data_type(&self) -> DataType {
        match self {
            // Existing mappings...
            Value::Udt(_) => DataType::Udt,
            Value::Tuple(_) => DataType::Tuple,
            // ... other types
        }
    }
    
    /// Try to convert this value to a UDT
    pub fn as_udt(&self) -> Option<&UdtValue> {
        match self {
            Value::Udt(udt) => Some(udt),
            _ => None,
        }
    }
    
    /// Try to convert this value to a tuple
    pub fn as_tuple(&self) -> Option<&TupleValue> {
        match self {
            Value::Tuple(tuple) => Some(tuple),
            _ => None,
        }
    }
}

/// Extended DataType enum
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    // Existing types...
    Null,
    Boolean,
    TinyInt,
    SmallInt,
    Integer,
    BigInt,
    Float32,
    Float,
    Text,
    Blob,
    Timestamp,
    Uuid,
    Json,
    List,
    Set,
    Map,
    
    // New complex types
    Udt,
    Tuple,
}

impl DataType {
    /// Check if this type is a complex type (UDT or tuple)
    pub fn is_complex(&self) -> bool {
        matches!(self, DataType::Udt | DataType::Tuple)
    }
    
    /// Check if this type is a collection
    pub fn is_collection(&self) -> bool {
        matches!(self, DataType::List | DataType::Set | DataType::Map)
    }
}
```

### UDT Helper Functions
```rust
impl UdtValue {
    /// Create a new UDT value
    pub fn new(type_name: String, keyspace: String) -> Self {
        Self {
            type_name,
            keyspace,
            fields: Vec::new(),
        }
    }
    
    /// Add a field to the UDT
    pub fn with_field(mut self, name: String, value: Option<Value>) -> Self {
        self.fields.push(UdtField { name, value });
        self
    }
    
    /// Get a field value by name
    pub fn get_field(&self, name: &str) -> Option<&Value> {
        self.fields.iter()
            .find(|f| f.name == name)
            .and_then(|f| f.value.as_ref())
    }
    
    /// Set a field value
    pub fn set_field(&mut self, name: String, value: Option<Value>) {
        if let Some(field) = self.fields.iter_mut().find(|f| f.name == name) {
            field.value = value;
        } else {
            self.fields.push(UdtField { name, value });
        }
    }
    
    /// Get all field names
    pub fn field_names(&self) -> Vec<&str> {
        self.fields.iter().map(|f| f.name.as_str()).collect()
    }
}

impl TupleValue {
    /// Create a new tuple value
    pub fn new(fields: Vec<Option<Value>>) -> Self {
        Self { fields }
    }
    
    /// Get field by position
    pub fn get_field(&self, index: usize) -> Option<&Value> {
        self.fields.get(index).and_then(|f| f.as_ref())
    }
    
    /// Set field by position
    pub fn set_field(&mut self, index: usize, value: Option<Value>) {
        if index < self.fields.len() {
            self.fields[index] = value;
        }
    }
    
    /// Get field count
    pub fn field_count(&self) -> usize {
        self.fields.len()
    }
}
```

---

## Critical Implementation Notes

### ⚠️ Zero-Tolerance Requirements

1. **Field Ordering**: Must match exact schema definition order
2. **Length Encoding**: 4-byte signed big-endian integers for all field lengths
3. **Null Handling**: -1 length for null, 0 for empty, >0 for data
4. **UTF-8 Validation**: All text fields must be valid UTF-8
5. **Sparse Support**: Handle UDTs with fewer fields than schema defines
6. **Nested Serialization**: Proper recursive handling of nested UDTs
7. **Type Consistency**: Field values must match declared types exactly
8. **Endianness**: All multi-byte values in big-endian format

### 🔧 Performance Considerations

1. **Memory Allocation**: Pre-allocate vectors based on field counts
2. **Zero-Copy**: Use references where possible during parsing
3. **Schema Caching**: Cache UDT type definitions for reuse
4. **Lazy Parsing**: Consider lazy field parsing for large UDTs
5. **Field Access**: Use HashMap for O(1) field lookup by name

### 📊 Compatibility Testing

1. **Cassandra Integration**: Test against real Cassandra-generated SSTables
2. **Version Testing**: Verify compatibility across Cassandra versions 3.0-5.x
3. **Schema Evolution**: Test adding/removing UDT fields
4. **Edge Cases**: Empty UDTs, deeply nested structures, large field counts
5. **Error Handling**: Proper error messages for format violations

---

**End of Specification**

*This document provides the complete binary format specification for Cassandra UDTs. All implementations must adhere to these exact requirements to ensure 100% compatibility with Apache Cassandra.*