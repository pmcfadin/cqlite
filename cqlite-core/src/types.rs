//! Core data types for CQLite

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use crate::schema::CqlType;

/// Database value type that can hold any supported data type
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    /// Null value
    Null,
    /// Boolean value
    Boolean(bool),
    /// 32-bit signed integer
    Integer(i32),
    /// 64-bit signed integer  
    BigInt(i64),
    /// 64-bit floating point number
    Float(f64),
    /// UTF-8 string
    Text(String),
    /// Binary data
    Blob(Vec<u8>),
    /// Timestamp (microseconds since Unix epoch)
    Timestamp(i64),
    /// UUID as 16 bytes
    Uuid([u8; 16]),
    /// JSON value
    Json(serde_json::Value),
    /// 8-bit signed integer (for exact Cassandra compatibility)
    TinyInt(i8),
    /// 16-bit signed integer (for exact Cassandra compatibility)
    SmallInt(i16),
    /// 32-bit floating point (for exact Cassandra compatibility)
    Float32(f32),
    /// List of values
    List(Vec<Value>),
    /// Set of values (implemented as Vec for ordering preservation)
    Set(Vec<Value>),
    /// Map of key-value pairs (Vec of tuples for exact Cassandra format)
    Map(Vec<(Value, Value)>),
    /// Tuple with fixed-size heterogeneous types
    Tuple(Vec<Value>),
    /// User defined type with structured fields
    Udt(UdtValue),
    /// Frozen wrapper for collections (immutable)
    Frozen(Box<Value>),
    /// Tombstone marker indicating deleted data
    Tombstone(TombstoneInfo),
}

/// User Defined Type value with structured field access
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UdtValue {
    /// UDT type name
    pub type_name: String,
    /// Keyspace where the UDT is defined
    pub keyspace: String,
    /// Ordered list of fields (matches schema definition order)
    pub fields: Vec<UdtField>,
}

/// UDT field with name and optional value
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UdtField {
    /// Field name
    pub name: String,
    /// Field value (None represents null)
    pub value: Option<Value>,
}

/// UDT type definition for schema management
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UdtTypeDef {
    /// Keyspace name
    pub keyspace: String,
    /// UDT type name
    pub name: String,
    /// Field definitions in schema order
    pub fields: Vec<UdtFieldDef>,
}

/// UDT field definition in schema
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UdtFieldDef {
    /// Field name
    pub name: String,
    /// Field data type
    pub field_type: CqlType,
    /// Whether the field can be null (default: true)
    #[serde(default = "default_nullable")]
    pub nullable: bool,
}

/// Tuple value with positional fields
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TupleValue {
    /// Positional fields (None represents null)
    pub fields: Vec<Option<Value>>,
}

fn default_nullable() -> bool {
    true
}

/// Tombstone information for tracking deletions
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TombstoneInfo {
    /// Deletion timestamp in microseconds since Unix epoch
    pub deletion_time: i64,
    /// Type of tombstone
    pub tombstone_type: TombstoneType,
    /// TTL if applicable (for TTL-based expiration)
    pub ttl: Option<i64>,
    /// Range start key for range tombstones
    pub range_start: Option<RowKey>,
    /// Range end key for range tombstones  
    pub range_end: Option<RowKey>,
}

/// Types of tombstones in Cassandra
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TombstoneType {
    /// Row-level deletion (entire row is deleted)
    RowTombstone,
    /// Cell-level deletion (specific column is deleted)
    CellTombstone,
    /// Range tombstone (a range of columns/rows is deleted)
    RangeTombstone,
    /// TTL expiration (data expired due to TTL)
    TtlExpiration,
}

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

    /// Get number of fields
    pub fn field_count(&self) -> usize {
        self.fields.len()
    }
}

impl UdtTypeDef {
    /// Create a new UDT type definition
    pub fn new(keyspace: String, name: String) -> Self {
        Self {
            keyspace,
            name,
            fields: Vec::new(),
        }
    }

    /// Add a field definition
    pub fn with_field(mut self, name: String, field_type: CqlType, nullable: bool) -> Self {
        self.fields.push(UdtFieldDef {
            name,
            field_type,
            nullable,
        });
        self
    }

    /// Get field definition by name
    pub fn get_field(&self, name: &str) -> Option<&UdtFieldDef> {
        self.fields.iter().find(|f| f.name == name)
    }

    /// Validate UDT value against this type definition
    pub fn validate_value(&self, value: &UdtValue) -> crate::Result<()> {
        // Check that type names match
        if value.type_name != self.name {
            return Err(crate::Error::schema(format!(
                "UDT type name mismatch: expected '{}', found '{}'",
                self.name, value.type_name
            )));
        }

        // Check that keyspace matches
        if value.keyspace != self.keyspace {
            return Err(crate::Error::schema(format!(
                "UDT keyspace mismatch: expected '{}', found '{}'",
                self.keyspace, value.keyspace
            )));
        }

        // Validate each field
        for field_def in &self.fields {
            if let Some(field_value) = value.get_field(&field_def.name) {
                // Field is present, check type compatibility
                if !Self::is_compatible_type(&field_value.data_type(), &field_def.field_type) {
                    return Err(crate::Error::schema(format!(
                        "Field '{}' type mismatch: expected {:?}, found {:?}",
                        field_def.name, field_def.field_type, field_value.data_type()
                    )));
                }
            } else if !field_def.nullable {
                // Field is missing but not nullable
                return Err(crate::Error::schema(format!(
                    "Non-nullable field '{}' is missing",
                    field_def.name
                )));
            }
        }

        Ok(())
    }

    fn is_compatible_type(value_type: &CqlType, expected_type: &CqlType) -> bool {
        // For now, require exact match - could be extended for type coercion
        value_type == expected_type
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

impl Value {
    /// Get the data type of this value
    pub fn data_type(&self) -> CqlType {
        match self {
            Value::Null => CqlType::Text, // Default type for null
            Value::Boolean(_) => CqlType::Boolean,
            Value::Integer(_) => CqlType::Int,
            Value::BigInt(_) => CqlType::BigInt,
            Value::Float(_) => CqlType::Double,
            Value::Text(_) => CqlType::Text,
            Value::Blob(_) => CqlType::Blob,
            Value::Timestamp(_) => CqlType::Timestamp,
            Value::Uuid(_) => CqlType::Uuid,
            Value::Json(_) => CqlType::Text, // JSON stored as text
            Value::TinyInt(_) => CqlType::TinyInt,
            Value::SmallInt(_) => CqlType::SmallInt,
            Value::Float32(_) => CqlType::Float,
            Value::List(elements) => {
                let element_type = if elements.is_empty() {
                    CqlType::Text
                } else {
                    elements[0].data_type()
                };
                CqlType::List(Box::new(element_type))
            },
            Value::Set(elements) => {
                let element_type = if elements.is_empty() {
                    CqlType::Text
                } else {
                    elements[0].data_type()
                };
                CqlType::Set(Box::new(element_type))
            },
            Value::Map(pairs) => {
                let (key_type, value_type) = if pairs.is_empty() {
                    (CqlType::Text, CqlType::Text)
                } else {
                    (pairs[0].0.data_type(), pairs[0].1.data_type())
                };
                CqlType::Map(Box::new(key_type), Box::new(value_type))
            },
            Value::Tuple(fields) => {
                let field_types = fields.iter().map(|f| f.data_type()).collect();
                CqlType::Tuple(field_types)
            },
            Value::Udt(udt) => {
                let fields = udt.fields.iter().map(|f| {
                    let field_type = if let Some(ref value) = f.value {
                        value.data_type()
                    } else {
                        CqlType::Text // Default for null fields
                    };
                    (f.name.clone(), field_type)
                }).collect();
                CqlType::Udt(udt.type_name.clone(), fields)
            },
            Value::Frozen(inner) => CqlType::Frozen(Box::new(inner.data_type())),
            Value::Tombstone(_) => CqlType::Text, // Tombstones don't have a specific type
        }
    }

    /// Check if this value is null
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Check if this value is a tombstone (deleted)
    pub fn is_tombstone(&self) -> bool {
        matches!(self, Value::Tombstone(_))
    }

    /// Check if this value is deleted (null or tombstone)
    pub fn is_deleted(&self) -> bool {
        self.is_null() || self.is_tombstone()
    }

    /// Check if this value has expired based on TTL
    pub fn is_expired(&self, current_time: i64) -> bool {
        match self {
            Value::Tombstone(info) => {
                // Check if TTL has expired
                if let Some(ttl) = info.ttl {
                    current_time > info.deletion_time + ttl
                } else {
                    false
                }
            },
            _ => false,
        }
    }

    /// Get the deletion timestamp if this is a tombstone
    pub fn deletion_time(&self) -> Option<i64> {
        match self {
            Value::Tombstone(info) => Some(info.deletion_time),
            _ => None,
        }
    }

    /// Create a row tombstone with the given timestamp
    pub fn row_tombstone(deletion_time: i64) -> Self {
        Value::Tombstone(TombstoneInfo {
            deletion_time,
            tombstone_type: TombstoneType::RowTombstone,
            ttl: None,
            range_start: None,
            range_end: None,
        })
    }

    /// Create a cell tombstone with the given timestamp
    pub fn cell_tombstone(deletion_time: i64) -> Self {
        Value::Tombstone(TombstoneInfo {
            deletion_time,
            tombstone_type: TombstoneType::CellTombstone,
            ttl: None,
            range_start: None,
            range_end: None,
        })
    }

    /// Create a TTL expiration tombstone
    pub fn ttl_tombstone(deletion_time: i64, ttl: i64) -> Self {
        Value::Tombstone(TombstoneInfo {
            deletion_time,
            tombstone_type: TombstoneType::TtlExpiration,
            ttl: Some(ttl),
            range_start: None,
            range_end: None,
        })
    }
    
    /// Create a range tombstone for clustering key ranges
    pub fn range_tombstone(deletion_time: i64, start_key: RowKey, end_key: RowKey) -> Self {
        Value::Tombstone(TombstoneInfo {
            deletion_time,
            tombstone_type: TombstoneType::RangeTombstone,
            ttl: None,
            range_start: Some(start_key),
            range_end: Some(end_key),
        })
    }
    
    /// Create a range tombstone with TTL for clustering key ranges
    pub fn range_tombstone_with_ttl(deletion_time: i64, start_key: RowKey, end_key: RowKey, ttl: i64) -> Self {
        Value::Tombstone(TombstoneInfo {
            deletion_time,
            tombstone_type: TombstoneType::RangeTombstone,
            ttl: Some(ttl),
            range_start: Some(start_key),
            range_end: Some(end_key),
        })
    }
    
    /// Get the tombstone type if this is a tombstone
    pub fn tombstone_type(&self) -> Option<TombstoneType> {
        match self {
            Value::Tombstone(info) => Some(info.tombstone_type),
            _ => None,
        }
    }
    
    /// Check if this tombstone covers a specific key (for range tombstones)
    pub fn tombstone_covers_key(&self, key: &RowKey) -> bool {
        match self {
            Value::Tombstone(info) if info.tombstone_type == TombstoneType::RangeTombstone => {
                match (&info.range_start, &info.range_end) {
                    (Some(start), Some(end)) => key >= start && key <= end,
                    (Some(start), None) => key >= start,
                    (None, Some(end)) => key <= end,
                    (None, None) => false,
                }
            },
            Value::Tombstone(_) => true, // Row and cell tombstones cover their specific key
            _ => false,
        }
    }
    
    /// Get TTL information from tombstone
    pub fn tombstone_ttl(&self) -> Option<i64> {
        match self {
            Value::Tombstone(info) => info.ttl,
            _ => None,
        }
    }
    
    /// Check if this is a specific type of tombstone
    pub fn is_tombstone_type(&self, tombstone_type: TombstoneType) -> bool {
        match self {
            Value::Tombstone(info) => info.tombstone_type == tombstone_type,
            _ => false,
        }
    }
    
    /// Get range information for range tombstones
    pub fn tombstone_range(&self) -> Option<(Option<&RowKey>, Option<&RowKey>)> {
        match self {
            Value::Tombstone(info) if info.tombstone_type == TombstoneType::RangeTombstone => {
                Some((info.range_start.as_ref(), info.range_end.as_ref()))
            },
            _ => None,
        }
    }

    /// Try to convert this value to a boolean
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    /// Try to convert this value to an integer
    pub fn as_i32(&self) -> Option<i32> {
        match self {
            Value::Integer(i) => Some(*i),
            Value::TinyInt(i) => Some(*i as i32),
            Value::SmallInt(i) => Some(*i as i32),
            _ => None,
        }
    }

    /// Try to convert this value to a big integer
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::BigInt(i) => Some(*i),
            Value::Integer(i) => Some(*i as i64),
            Value::TinyInt(i) => Some(*i as i64),
            Value::SmallInt(i) => Some(*i as i64),
            _ => None,
        }
    }

    /// Try to convert this value to a float
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float(f) => Some(*f),
            Value::Float32(f) => Some(*f as f64),
            Value::Integer(i) => Some(*i as f64),
            Value::BigInt(i) => Some(*i as f64),
            Value::TinyInt(i) => Some(*i as f64),
            Value::SmallInt(i) => Some(*i as f64),
            _ => None,
        }
    }

    /// Try to convert this value to a string
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Text(s) => Some(s),
            _ => None,
        }
    }

    /// Try to convert this value to bytes
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Blob(b) => Some(b),
            Value::Text(s) => Some(s.as_bytes()),
            _ => None,
        }
    }

    /// Get the size in bytes for this value when serialized
    pub fn size_estimate(&self) -> usize {
        match self {
            Value::Null => 1,
            Value::Boolean(_) => 1,
            Value::TinyInt(_) => 1,
            Value::SmallInt(_) => 2,
            Value::Integer(_) => 4,
            Value::BigInt(_) => 8,
            Value::Float32(_) => 4,
            Value::Float(_) => 8,
            Value::Text(s) => 4 + s.len(), // VInt length + content
            Value::Blob(b) => 4 + b.len(), // VInt length + content
            Value::Timestamp(_) => 8,
            Value::Uuid(_) => 16,
            Value::Json(j) => {
                let json_str = j.to_string();
                4 + json_str.len()
            },
            Value::List(list) => {
                let mut size = 4 + 1; // count + element type
                for item in list {
                    size += item.size_estimate();
                }
                size
            },
            Value::Set(set) => {
                let mut size = 4 + 1; // count + element type
                for item in set {
                    size += item.size_estimate();
                }
                size
            },
            Value::Map(map) => {
                let mut size = 4 + 2; // count + key_type + value_type
                for (key, value) in map {
                    size += key.size_estimate() + value.size_estimate();
                }
                size
            },
            Value::Tuple(tuple) => {
                let mut size = 4; // count
                for item in tuple {
                    size += 1 + item.size_estimate(); // type + value
                }
                size
            },
            Value::Udt(udt_value) => {
                let mut size = 4 + udt_value.type_name.len() + 4 + udt_value.keyspace.len() + 4; // type name + keyspace + field count
                for field in &udt_value.fields {
                    size += 4 + field.name.len(); // field name length + field name
                    if let Some(ref field_value) = field.value {
                        size += 1 + field_value.size_estimate(); // type + value
                    } else {
                        size += 1; // null marker
                    }
                }
                size
            },
            Value::Frozen(inner) => inner.size_estimate(),
            Value::Tombstone(_) => 16, // timestamp + type + optional TTL
        }
    }

    /// Check if this value represents an empty collection
    pub fn is_empty_collection(&self) -> bool {
        match self {
            Value::List(list) => list.is_empty(),
            Value::Set(set) => set.is_empty(),
            Value::Map(map) => map.is_empty(),
            Value::Tuple(tuple) => tuple.is_empty(),
            _ => false,
        }
    }

    /// Get the element count for collections
    pub fn collection_len(&self) -> Option<usize> {
        match self {
            Value::List(list) => Some(list.len()),
            Value::Set(set) => Some(set.len()),
            Value::Map(map) => Some(map.len()),
            Value::Tuple(tuple) => Some(tuple.len()),
            _ => None,
        }
    }

    /// Check if this value can be used as a collection element
    pub fn is_valid_collection_element(&self) -> bool {
        match self {
            Value::Null => false, // Null elements typically not allowed in collections
            _ => true,
        }
    }

    /// Validate collection type consistency
    pub fn validate_collection_types(&self) -> crate::Result<()> {
        match self {
            Value::List(list) => {
                if list.is_empty() {
                    return Ok(());
                }
                let first_type = list[0].data_type();
                for item in list.iter().skip(1) {
                    if item.data_type() != first_type {
                        return Err(crate::Error::schema(
                            format!("List contains mixed types: {:?} and {:?}", first_type, item.data_type())
                        ));
                    }
                }
                Ok(())
            },
            Value::Set(set) => {
                if set.is_empty() {
                    return Ok(());
                }
                let first_type = set[0].data_type();
                for item in set.iter().skip(1) {
                    if item.data_type() != first_type {
                        return Err(crate::Error::schema(
                            format!("Set contains mixed types: {:?} and {:?}", first_type, item.data_type())
                        ));
                    }
                }
                // Check for duplicates in set
                let mut seen = std::collections::HashSet::new();
                for item in set {
                    let item_str = format!("{}", item);
                    if !seen.insert(item_str.clone()) {
                        return Err(crate::Error::schema(
                            format!("Set contains duplicate value: {}", item_str)
                        ));
                    }
                }
                Ok(())
            },
            Value::Map(map) => {
                if map.is_empty() {
                    return Ok(());
                }
                let (first_key, first_value) = &map[0];
                let key_type = first_key.data_type();
                let value_type = first_value.data_type();
                
                for (key, value) in map.iter().skip(1) {
                    if key.data_type() != key_type {
                        return Err(crate::Error::schema(
                            format!("Map contains mixed key types: {:?} and {:?}", key_type, key.data_type())
                        ));
                    }
                    if value.data_type() != value_type {
                        return Err(crate::Error::schema(
                            format!("Map contains mixed value types: {:?} and {:?}", value_type, value.data_type())
                        ));
                    }
                }
                
                // Check for duplicate keys
                let mut seen_keys = std::collections::HashSet::new();
                for (key, _) in map {
                    let key_str = format!("{}", key);
                    if !seen_keys.insert(key_str.clone()) {
                        return Err(crate::Error::schema(
                            format!("Map contains duplicate key: {}", key_str)
                        ));
                    }
                }
                Ok(())
            },
            _ => Ok(()), // Non-collections are always valid
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Boolean(b) => write!(f, "{}", b),
            Value::Integer(i) => write!(f, "{}", i),
            Value::BigInt(i) => write!(f, "{}", i),
            Value::Float(fl) => write!(f, "{}", fl),
            Value::Text(s) => write!(f, "'{}'", s),
            Value::Blob(b) => write!(f, "BLOB({} bytes)", b.len()),
            Value::Timestamp(ts) => write!(f, "TIMESTAMP({})", ts),
            Value::Uuid(uuid) => {
                write!(f, "UUID({})", hex::encode(uuid))
            }
            Value::Json(json) => write!(f, "JSON({})", json),
            Value::TinyInt(i) => write!(f, "{}", i),
            Value::SmallInt(i) => write!(f, "{}", i),
            Value::Float32(fl) => write!(f, "{}", fl),
            Value::Set(set) => {
                write!(f, "{{")?;
                for (i, item) in set.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", item)?;
                }
                write!(f, "}}")
            }
            Value::List(list) => {
                write!(f, "[")?;
                for (i, item) in list.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", item)?;
                }
                write!(f, "]")
            }
            Value::Map(map) => {
                write!(f, "{{")?;
                for (i, (key, value)) in map.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", key, value)?;
                }
                write!(f, "}}")
            }
            Value::Tuple(tuple) => {
                write!(f, "(")?;
                for (i, item) in tuple.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", item)?;
                }
                write!(f, ")")
            }
            Value::Udt(udt) => {
                write!(f, "{}{{", udt.type_name)?;
                for (i, field) in udt.fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    match &field.value {
                        Some(value) => write!(f, "{}: {}", field.name, value)?,
                        None => write!(f, "{}: NULL", field.name)?,
                    }
                }
                write!(f, "}}")
            }
            Value::Frozen(inner) => {
                write!(f, "FROZEN({})", inner)
            }
            Value::Tombstone(info) => {
                match info.tombstone_type {
                    TombstoneType::RowTombstone => write!(f, "TOMBSTONE(ROW@{})", info.deletion_time),
                    TombstoneType::CellTombstone => write!(f, "TOMBSTONE(CELL@{})", info.deletion_time),
                    TombstoneType::RangeTombstone => write!(f, "TOMBSTONE(RANGE@{})", info.deletion_time),
                    TombstoneType::TtlExpiration => {
                        if let Some(ttl) = info.ttl {
                            write!(f, "TOMBSTONE(TTL@{}+{})", info.deletion_time, ttl)
                        } else {
                            write!(f, "TOMBSTONE(TTL@{})", info.deletion_time)
                        }
                    }
                }
            }
        }
    }
}

/// Data type enumeration
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    /// Null type
    Null,
    /// Boolean type
    Boolean,
    /// 8-bit signed integer
    TinyInt,
    /// 16-bit signed integer
    SmallInt,
    /// 32-bit signed integer
    Integer,
    /// 64-bit signed integer
    BigInt,
    /// 32-bit floating point
    Float32,
    /// 64-bit floating point
    Float,
    /// Variable-length text
    Text,
    /// Variable-length binary data
    Blob,
    /// Timestamp with microsecond precision
    Timestamp,
    /// UUID type
    Uuid,
    /// JSON document
    Json,
    /// List of values
    List,
    /// Set of values
    Set,
    /// Map of key-value pairs
    Map,
    /// Tuple type with heterogeneous fields
    Tuple,
    /// User defined type
    Udt,
    /// Frozen type wrapper
    Frozen,
    /// Tombstone marker
    Tombstone,
}

impl DataType {
    /// Check if this type is numeric
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            DataType::TinyInt
                | DataType::SmallInt
                | DataType::Integer
                | DataType::BigInt
                | DataType::Float32
                | DataType::Float
        )
    }

    /// Check if this type is textual
    pub fn is_textual(&self) -> bool {
        matches!(self, DataType::Text)
    }

    /// Check if this type is binary
    pub fn is_binary(&self) -> bool {
        matches!(self, DataType::Blob)
    }

    /// Get the default value for this type
    pub fn default_value(&self) -> Value {
        match self {
            DataType::Null => Value::Null,
            DataType::Boolean => Value::Boolean(false),
            DataType::TinyInt => Value::TinyInt(0),
            DataType::SmallInt => Value::SmallInt(0),
            DataType::Integer => Value::Integer(0),
            DataType::BigInt => Value::BigInt(0),
            DataType::Float32 => Value::Float32(0.0),
            DataType::Float => Value::Float(0.0),
            DataType::Text => Value::Text(String::new()),
            DataType::Blob => Value::Blob(Vec::new()),
            DataType::Timestamp => Value::Timestamp(0),
            DataType::Uuid => Value::Uuid([0; 16]),
            DataType::Json => Value::Json(serde_json::Value::Null),
            DataType::List => Value::List(Vec::new()),
            DataType::Set => Value::Set(Vec::new()),
            DataType::Map => Value::Map(Vec::new()),
            DataType::Tuple => Value::Tuple(Vec::new()),
            DataType::Udt => Value::Udt(UdtValue {
                type_name: String::new(),
                keyspace: String::new(),
                fields: Vec::new(),
            }),
            DataType::Frozen => Value::Frozen(Box::new(Value::Null)),
            DataType::Tombstone => Value::Tombstone(TombstoneInfo {
                deletion_time: 0,
                tombstone_type: TombstoneType::RowTombstone,
                ttl: None,
                range_start: None,
                range_end: None,
            }),
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            DataType::Null => "NULL",
            DataType::Boolean => "BOOLEAN",
            DataType::TinyInt => "TINYINT",
            DataType::SmallInt => "SMALLINT",
            DataType::Integer => "INTEGER",
            DataType::BigInt => "BIGINT",
            DataType::Float32 => "FLOAT32",
            DataType::Float => "FLOAT",
            DataType::Text => "TEXT",
            DataType::Blob => "BLOB",
            DataType::Timestamp => "TIMESTAMP",
            DataType::Uuid => "UUID",
            DataType::Json => "JSON",
            DataType::List => "LIST",
            DataType::Set => "SET",
            DataType::Map => "MAP",
            DataType::Tuple => "TUPLE",
            DataType::Udt => "UDT",
            DataType::Frozen => "FROZEN",
            DataType::Tombstone => "TOMBSTONE",
        };
        write!(f, "{}", name)
    }
}

/// Row key type - used for indexing and sorting
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RowKey(pub Vec<u8>);

impl RowKey {
    /// Create a new row key from bytes
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// Create a row key from a value
    pub fn from_value(value: &Value) -> crate::Result<Self> {
        let bytes =
            bincode::serialize(value).map_err(|e| crate::Error::Serialization(e.to_string()))?;
        Ok(Self(bytes))
    }

    /// Get the byte representation
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Get the length in bytes
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if the key is empty
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<Vec<u8>> for RowKey {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
}

impl From<&[u8]> for RowKey {
    fn from(bytes: &[u8]) -> Self {
        Self(bytes.to_vec())
    }
}

impl From<String> for RowKey {
    fn from(s: String) -> Self {
        Self(s.into_bytes())
    }
}

impl From<&str> for RowKey {
    fn from(s: &str) -> Self {
        Self(s.as_bytes().to_vec())
    }
}

/// Table identifier
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TableId(pub String);

impl TableId {
    /// Create a new table ID
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// Get the table name
    pub fn name(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TableId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for TableId {
    fn from(name: String) -> Self {
        Self(name)
    }
}

impl From<&str> for TableId {
    fn from(name: &str) -> Self {
        Self(name.to_string())
    }
}

/// Column identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnId(pub String);

impl ColumnId {
    /// Create a new column ID
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// Get the column name
    pub fn name(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ColumnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for ColumnId {
    fn from(name: String) -> Self {
        Self(name)
    }
}

impl From<&str> for ColumnId {
    fn from(name: &str) -> Self {
        Self(name.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_types() {
        assert_eq!(Value::Integer(42).data_type(), CqlType::Int);
        assert_eq!(Value::Text("hello".to_string()).data_type(), CqlType::Text);
        assert_eq!(Value::Boolean(true).data_type(), CqlType::Boolean);
    }

    #[test]
    fn test_value_conversions() {
        let val = Value::Integer(42);
        assert_eq!(val.as_i32(), Some(42));
        assert_eq!(val.as_i64(), Some(42));
        assert_eq!(val.as_f64(), Some(42.0));
        assert_eq!(val.as_bool(), None);
    }

    #[test]
    fn test_row_key_creation() {
        let key1 = RowKey::from("test");
        let key2 = RowKey::from(b"test".to_vec());
        assert_eq!(key1.as_bytes(), key2.as_bytes());
    }

    #[test]
    fn test_value_display() {
        assert_eq!(Value::Null.to_string(), "NULL");
        assert_eq!(Value::Integer(42).to_string(), "42");
        assert_eq!(Value::Text("hello".to_string()).to_string(), "'hello'");
    }

    #[test]
    fn test_new_value_types() {
        // Test Tuple
        let tuple = Value::Tuple(vec![
            Value::Integer(42),
            Value::Text("hello".to_string()),
            Value::Boolean(true),
        ]);
        assert!(matches!(tuple.data_type(), CqlType::Tuple(_)));
        assert_eq!(tuple.to_string(), "(42, 'hello', true)");

        // Test UDT
        let udt = Value::Udt(UdtValue {
            type_name: "Person".to_string(),
            keyspace: "test".to_string(),
            fields: vec![
                UdtField { name: "name".to_string(), value: Some(Value::Text("John".to_string())) },
                UdtField { name: "age".to_string(), value: Some(Value::Integer(30)) },
            ],
        });
        assert!(matches!(udt.data_type(), CqlType::Udt(_, _)));
        assert!(udt.to_string().contains("Person{"));

        // Test Frozen
        let frozen_list = Value::Frozen(Box::new(Value::List(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ])));
        assert!(matches!(frozen_list.data_type(), CqlType::Frozen(..)));
        assert_eq!(frozen_list.to_string(), "FROZEN([1, 2, 3])");
    }

    #[test]
    fn test_new_data_types() {
        assert_eq!(DataType::Tuple.to_string(), "TUPLE");
        assert_eq!(DataType::Udt.to_string(), "UDT");
        assert_eq!(DataType::Frozen.to_string(), "FROZEN");
        assert_eq!(DataType::Tombstone.to_string(), "TOMBSTONE");

        // Test default values
        assert_eq!(DataType::Tuple.default_value(), Value::Tuple(Vec::new()));
        assert_eq!(
            DataType::Udt.default_value(),
            Value::Udt(UdtValue::new(String::new(), String::new()))
        );
        assert_eq!(
            DataType::Frozen.default_value(),
            Value::Frozen(Box::new(Value::Null))
        );
        assert!(matches!(
            DataType::Tombstone.default_value(),
            Value::Tombstone(_)
        ));
    }

    #[test]
    fn test_tombstone_functionality() {
        // Test row tombstone creation
        let row_tombstone = Value::row_tombstone(1000);
        assert!(row_tombstone.is_tombstone());
        assert!(row_tombstone.is_deleted());
        assert_eq!(row_tombstone.deletion_time(), Some(1000));
        assert!(!row_tombstone.is_expired(500)); // before deletion
        assert!(!row_tombstone.is_expired(1500)); // TTL tombstones only expire

        // Test cell tombstone creation
        let cell_tombstone = Value::cell_tombstone(2000);
        assert!(cell_tombstone.is_tombstone());
        assert_eq!(cell_tombstone.deletion_time(), Some(2000));

        // Test TTL tombstone creation
        let ttl_tombstone = Value::ttl_tombstone(3000, 1000);
        assert!(ttl_tombstone.is_tombstone());
        assert_eq!(ttl_tombstone.deletion_time(), Some(3000));
        assert!(!ttl_tombstone.is_expired(3500)); // within TTL
        assert!(ttl_tombstone.is_expired(5000)); // past TTL

        // Test regular values
        let regular_value = Value::Integer(42);
        assert!(!regular_value.is_tombstone());
        assert!(!regular_value.is_deleted());
        assert_eq!(regular_value.deletion_time(), None);
        assert!(!regular_value.is_expired(1000));

        // Test null values
        let null_value = Value::Null;
        assert!(!null_value.is_tombstone());
        assert!(null_value.is_deleted()); // null is considered deleted
    }

    #[test]
    fn test_tombstone_display() {
        let row_tombstone = Value::row_tombstone(1000);
        assert_eq!(row_tombstone.to_string(), "TOMBSTONE(ROW@1000)");

        let cell_tombstone = Value::cell_tombstone(2000);
        assert_eq!(cell_tombstone.to_string(), "TOMBSTONE(CELL@2000)");

        let ttl_tombstone = Value::ttl_tombstone(3000, 1000);
        assert_eq!(ttl_tombstone.to_string(), "TOMBSTONE(TTL@3000+1000)");
    }
}
