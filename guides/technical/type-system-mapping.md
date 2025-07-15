# CQL to Rust Type System Mapping

## 🎯 Complete Type System Implementation Guide

This guide provides comprehensive mapping from CQL data types to Rust implementations, ensuring perfect serialization compatibility with Cassandra.

## 🏗️ Core Type Architecture

### **CQL Value Enum Design**
```rust
use std::collections::{HashMap, BTreeSet};
use chrono::{DateTime, Utc, NaiveDate, NaiveTime, Duration};
use uuid::Uuid;
use bigdecimal::BigDecimal;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CQLValue {
    // Primitive Types
    Null,
    Boolean(bool),
    TinyInt(i8),
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    VarInt(BigInt),
    Float(OrderedFloat<f32>),
    Double(OrderedFloat<f64>),
    Decimal(BigDecimal),
    
    // String Types
    Ascii(String),
    Text(String),
    Varchar(String),
    
    // Binary Types
    Blob(Vec<u8>),
    
    // Time Types  
    Timestamp(DateTime<Utc>),
    Date(NaiveDate),
    Time(NaiveTime),
    Duration(Duration),
    
    // UUID Types
    Uuid(Uuid),
    TimeUuid(Uuid),
    
    // Network Types
    Inet(IpAddr),
    
    // Collection Types
    List(Vec<CQLValue>),
    Set(BTreeSet<CQLValue>),
    Map(HashMap<CQLValue, CQLValue>),
    
    // Complex Types
    Tuple(Vec<CQLValue>),
    Udt(UserDefinedType),
    
    // Special Types
    Counter(i64),
    Frozen(Box<CQLValue>),
}
```

## 📊 Primitive Type Serialization

### **Integer Types**
```rust
impl CQLValue {
    // TinyInt: 1 byte signed
    fn serialize_tinyint(value: i8) -> Vec<u8> {
        vec![value as u8]
    }
    
    fn deserialize_tinyint(bytes: &[u8]) -> Result<i8> {
        if bytes.len() != 1 {
            return Err(CQLError::InvalidLength);
        }
        Ok(bytes[0] as i8)
    }
    
    // SmallInt: 2 bytes big-endian signed
    fn serialize_smallint(value: i16) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }
    
    fn deserialize_smallint(bytes: &[u8]) -> Result<i16> {
        if bytes.len() != 2 {
            return Err(CQLError::InvalidLength);
        }
        Ok(i16::from_be_bytes([bytes[0], bytes[1]]))
    }
    
    // Int: 4 bytes big-endian signed
    fn serialize_int(value: i32) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }
    
    // BigInt: 8 bytes big-endian signed
    fn serialize_bigint(value: i64) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }
    
    // VarInt: Variable length integer (arbitrary precision)
    fn serialize_varint(value: &BigInt) -> Vec<u8> {
        let bytes = value.to_signed_bytes_be();
        bytes
    }
}
```

### **Floating Point Types**
```rust
use ordered_float::OrderedFloat;

impl CQLValue {
    // Float: 4 bytes IEEE 754 big-endian
    fn serialize_float(value: f32) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }
    
    fn deserialize_float(bytes: &[u8]) -> Result<f32> {
        if bytes.len() != 4 {
            return Err(CQLError::InvalidLength);
        }
        let array = [bytes[0], bytes[1], bytes[2], bytes[3]];
        Ok(f32::from_be_bytes(array))
    }
    
    // Double: 8 bytes IEEE 754 big-endian  
    fn serialize_double(value: f64) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }
    
    // Decimal: [scale: 4 bytes][unscaled_value: variable]
    fn serialize_decimal(value: &BigDecimal) -> Vec<u8> {
        let scale = value.fractional_digit_count() as i32;
        let unscaled = value.as_bigint_and_exponent().0;
        
        let mut result = scale.to_be_bytes().to_vec();
        result.extend_from_slice(&unscaled.to_signed_bytes_be());
        result
    }
}
```

## 📝 String and Binary Types

### **String Serialization**
```rust
impl CQLValue {
    // All string types use UTF-8 encoding
    fn serialize_text(value: &str) -> Vec<u8> {
        value.as_bytes().to_vec()
    }
    
    fn deserialize_text(bytes: &[u8]) -> Result<String> {
        String::from_utf8(bytes.to_vec())
            .map_err(|_| CQLError::InvalidUtf8)
    }
    
    // ASCII is validated to contain only ASCII characters
    fn serialize_ascii(value: &str) -> Result<Vec<u8>> {
        if !value.is_ascii() {
            return Err(CQLError::NonAsciiCharacter);
        }
        Ok(value.as_bytes().to_vec())
    }
    
    // Blob is raw binary data
    fn serialize_blob(value: &[u8]) -> Vec<u8> {
        value.to_vec()
    }
}
```

## ⏰ Temporal Types

### **Date and Time Implementation**
```rust
use chrono::{DateTime, Utc, NaiveDate, NaiveTime, Duration};

impl CQLValue {
    // Timestamp: milliseconds since Unix epoch
    fn serialize_timestamp(value: &DateTime<Utc>) -> Vec<u8> {
        let millis = value.timestamp_millis();
        millis.to_be_bytes().to_vec()
    }
    
    fn deserialize_timestamp(bytes: &[u8]) -> Result<DateTime<Utc>> {
        if bytes.len() != 8 {
            return Err(CQLError::InvalidLength);
        }
        let millis = i64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3],
            bytes[4], bytes[5], bytes[6], bytes[7]
        ]);
        DateTime::from_timestamp_millis(millis)
            .ok_or(CQLError::InvalidTimestamp)
    }
    
    // Date: days since Unix epoch (1970-01-01)
    fn serialize_date(value: &NaiveDate) -> Vec<u8> {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let days = value.signed_duration_since(epoch).num_days() as u32;
        days.to_be_bytes().to_vec()
    }
    
    // Time: nanoseconds since midnight
    fn serialize_time(value: &NaiveTime) -> Vec<u8> {
        let nanos = value.num_seconds_from_midnight() as u64 * 1_000_000_000
                  + value.nanosecond() as u64;
        nanos.to_be_bytes().to_vec()
    }
    
    // Duration: months, days, nanoseconds
    fn serialize_duration(value: &Duration) -> Vec<u8> {
        let months = 0i32; // Duration doesn't track months
        let days = value.num_days() as i32;
        let nanos = (value.num_nanoseconds().unwrap_or(0) % (24 * 60 * 60 * 1_000_000_000)) as i64;
        
        let mut result = Vec::new();
        result.extend_from_slice(&months.to_be_bytes());
        result.extend_from_slice(&days.to_be_bytes());
        result.extend_from_slice(&nanos.to_be_bytes());
        result
    }
}
```

## 🆔 UUID Types

### **UUID Serialization**
```rust
use uuid::Uuid;

impl CQLValue {
    // UUID: 16 bytes in standard format
    fn serialize_uuid(value: &Uuid) -> Vec<u8> {
        value.as_bytes().to_vec()
    }
    
    fn deserialize_uuid(bytes: &[u8]) -> Result<Uuid> {
        if bytes.len() != 16 {
            return Err(CQLError::InvalidLength);
        }
        let mut array = [0u8; 16];
        array.copy_from_slice(bytes);
        Ok(Uuid::from_bytes(array))
    }
    
    // TimeUUID: UUID v1 with timestamp ordering
    fn validate_timeuuid(uuid: &Uuid) -> Result<()> {
        if uuid.get_version() != Some(uuid::Version::Mac) {
            return Err(CQLError::InvalidTimeUuid);
        }
        Ok(())
    }
}
```

## 📦 Collection Types

### **List Implementation**
```rust
impl CQLValue {
    // List: [length: 4 bytes][element1][element2]...
    fn serialize_list(values: &[CQLValue], element_type: &CQLType) -> Result<Vec<u8>> {
        let mut result = Vec::new();
        
        // Write number of elements
        result.extend_from_slice(&(values.len() as i32).to_be_bytes());
        
        // Write each element with length prefix
        for value in values {
            let element_bytes = value.serialize(element_type)?;
            result.extend_from_slice(&(element_bytes.len() as i32).to_be_bytes());
            result.extend_from_slice(&element_bytes);
        }
        
        Ok(result)
    }
    
    fn deserialize_list(bytes: &[u8], element_type: &CQLType) -> Result<Vec<CQLValue>> {
        let mut cursor = 0;
        
        // Read number of elements
        if bytes.len() < 4 {
            return Err(CQLError::InvalidLength);
        }
        let count = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        cursor += 4;
        
        let mut result = Vec::with_capacity(count);
        
        // Read each element
        for _ in 0..count {
            // Read element length
            if cursor + 4 > bytes.len() {
                return Err(CQLError::InvalidLength);
            }
            let length = i32::from_be_bytes([
                bytes[cursor], bytes[cursor+1], bytes[cursor+2], bytes[cursor+3]
            ]) as usize;
            cursor += 4;
            
            // Read element data
            if cursor + length > bytes.len() {
                return Err(CQLError::InvalidLength);
            }
            let element_bytes = &bytes[cursor..cursor + length];
            let element = CQLValue::deserialize(element_bytes, element_type)?;
            result.push(element);
            cursor += length;
        }
        
        Ok(result)
    }
}
```

### **Set Implementation**
```rust
use std::collections::BTreeSet;

impl CQLValue {
    // Set: same format as List but with uniqueness constraint
    fn serialize_set(values: &BTreeSet<CQLValue>, element_type: &CQLType) -> Result<Vec<u8>> {
        let values_vec: Vec<_> = values.iter().cloned().collect();
        Self::serialize_list(&values_vec, element_type)
    }
    
    fn deserialize_set(bytes: &[u8], element_type: &CQLType) -> Result<BTreeSet<CQLValue>> {
        let values = Self::deserialize_list(bytes, element_type)?;
        Ok(values.into_iter().collect())
    }
}
```

### **Map Implementation**
```rust
use std::collections::HashMap;

impl CQLValue {
    // Map: [length: 4 bytes][key1][value1][key2][value2]...
    fn serialize_map(
        map: &HashMap<CQLValue, CQLValue>, 
        key_type: &CQLType, 
        value_type: &CQLType
    ) -> Result<Vec<u8>> {
        let mut result = Vec::new();
        
        // Write number of pairs
        result.extend_from_slice(&(map.len() as i32).to_be_bytes());
        
        // Write each key-value pair
        for (key, value) in map {
            // Serialize key
            let key_bytes = key.serialize(key_type)?;
            result.extend_from_slice(&(key_bytes.len() as i32).to_be_bytes());
            result.extend_from_slice(&key_bytes);
            
            // Serialize value
            let value_bytes = value.serialize(value_type)?;
            result.extend_from_slice(&(value_bytes.len() as i32).to_be_bytes());
            result.extend_from_slice(&value_bytes);
        }
        
        Ok(result)
    }
}
```

## 🏗️ Complex Types

### **User Defined Types (UDT)**
```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UserDefinedType {
    pub type_name: String,
    pub fields: HashMap<String, CQLValue>,
}

impl UserDefinedType {
    fn serialize(&self, type_def: &UDTDefinition) -> Result<Vec<u8>> {
        let mut result = Vec::new();
        
        // Serialize fields in schema order
        for field_def in &type_def.fields {
            if let Some(value) = self.fields.get(&field_def.name) {
                let field_bytes = value.serialize(&field_def.type_)?;
                result.extend_from_slice(&(field_bytes.len() as i32).to_be_bytes());
                result.extend_from_slice(&field_bytes);
            } else {
                // Null field
                result.extend_from_slice(&(-1i32).to_be_bytes());
            }
        }
        
        Ok(result)
    }
}
```

### **Tuple Types**
```rust
impl CQLValue {
    // Tuple: [field1][field2]... (fixed number of fields)
    fn serialize_tuple(values: &[CQLValue], field_types: &[CQLType]) -> Result<Vec<u8>> {
        if values.len() != field_types.len() {
            return Err(CQLError::TupleSizeMismatch);
        }
        
        let mut result = Vec::new();
        
        for (value, field_type) in values.iter().zip(field_types.iter()) {
            match value {
                CQLValue::Null => {
                    result.extend_from_slice(&(-1i32).to_be_bytes());
                }
                _ => {
                    let field_bytes = value.serialize(field_type)?;
                    result.extend_from_slice(&(field_bytes.len() as i32).to_be_bytes());
                    result.extend_from_slice(&field_bytes);
                }
            }
        }
        
        Ok(result)
    }
}
```

## 🎯 Type Conversion Traits

### **Safe Type Conversions**
```rust
impl TryFrom<CQLValue> for i32 {
    type Error = CQLError;
    
    fn try_from(value: CQLValue) -> Result<i32> {
        match value {
            CQLValue::Int(i) => Ok(i),
            CQLValue::SmallInt(i) => Ok(i as i32),
            CQLValue::TinyInt(i) => Ok(i as i32),
            _ => Err(CQLError::TypeMismatch),
        }
    }
}

impl TryFrom<CQLValue> for String {
    type Error = CQLError;
    
    fn try_from(value: CQLValue) -> Result<String> {
        match value {
            CQLValue::Text(s) | CQLValue::Varchar(s) | CQLValue::Ascii(s) => Ok(s),
            _ => Err(CQLError::TypeMismatch),
        }
    }
}

// Implement for all supported types...
```

### **Ergonomic Value Construction**
```rust
impl From<i32> for CQLValue {
    fn from(value: i32) -> Self {
        CQLValue::Int(value)
    }
}

impl From<String> for CQLValue {
    fn from(value: String) -> Self {
        CQLValue::Text(value)
    }
}

impl From<Vec<u8>> for CQLValue {
    fn from(value: Vec<u8>) -> Self {
        CQLValue::Blob(value)
    }
}

// Convenient constructors
impl CQLValue {
    pub fn list<T: Into<CQLValue>>(values: Vec<T>) -> Self {
        CQLValue::List(values.into_iter().map(Into::into).collect())
    }
    
    pub fn map<K: Into<CQLValue>, V: Into<CQLValue>>(
        pairs: Vec<(K, V)>
    ) -> Self {
        let map = pairs.into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        CQLValue::Map(map)
    }
}
```

## ⚠️ Critical Implementation Notes

### **1. Null Handling**
- Null values are represented as `CQLValue::Null`
- In collections, null elements have length -1
- UDT fields can be null (omitted or explicit null)

### **2. Endianness**
- All multi-byte integers use big-endian (network byte order)
- IEEE 754 floats also use big-endian representation
- Critical for Cassandra compatibility

### **3. Collection Ordering**
- Lists preserve insertion order
- Sets use BTreeSet for consistent ordering (required for serialization)
- Maps use HashMap (order not guaranteed, but deterministic serialization needed)

### **4. Performance Considerations**
- Use `Cow<str>` for strings when possible to avoid allocations
- Consider `SmallVec` for small collections
- Implement zero-copy deserialization where beneficial

---

*This type system mapping ensures perfect compatibility with Cassandra's serialization formats while leveraging Rust's type safety and performance advantages.*