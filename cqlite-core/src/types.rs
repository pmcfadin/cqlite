//! Core data types for CQLite

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

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
    /// User defined type with type name and fields
    Udt(String, HashMap<String, Value>),
    /// Frozen wrapper for collections (immutable)
    Frozen(Box<Value>),
}

impl Value {
    /// Get the data type of this value
    pub fn data_type(&self) -> DataType {
        match self {
            Value::Null => DataType::Null,
            Value::Boolean(_) => DataType::Boolean,
            Value::Integer(_) => DataType::Integer,
            Value::BigInt(_) => DataType::BigInt,
            Value::Float(_) => DataType::Float,
            Value::Text(_) => DataType::Text,
            Value::Blob(_) => DataType::Blob,
            Value::Timestamp(_) => DataType::Timestamp,
            Value::Uuid(_) => DataType::Uuid,
            Value::Json(_) => DataType::Json,
            Value::TinyInt(_) => DataType::TinyInt,
            Value::SmallInt(_) => DataType::SmallInt,
            Value::Float32(_) => DataType::Float32,
            Value::List(_) => DataType::List,
            Value::Set(_) => DataType::Set,
            Value::Map(_) => DataType::Map,
            Value::Tuple(_) => DataType::Tuple,
            Value::Udt(_, _) => DataType::Udt,
            Value::Frozen(_) => DataType::Frozen,
        }
    }

    /// Check if this value is null
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
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
            Value::Udt(type_name, fields) => {
                write!(f, "{}{{", type_name)?;
                for (i, (field_name, field_value)) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", field_name, field_value)?;
                }
                write!(f, "}}")
            }
            Value::Frozen(inner) => {
                write!(f, "FROZEN({})", inner)
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
            DataType::Udt => Value::Udt(String::new(), HashMap::new()),
            DataType::Frozen => Value::Frozen(Box::new(Value::Null)),
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
        assert_eq!(Value::Integer(42).data_type(), DataType::Integer);
        assert_eq!(Value::Text("hello".to_string()).data_type(), DataType::Text);
        assert_eq!(Value::Boolean(true).data_type(), DataType::Boolean);
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
        assert_eq!(tuple.data_type(), DataType::Tuple);
        assert_eq!(tuple.to_string(), "(42, 'hello', true)");

        // Test UDT
        let mut fields = HashMap::new();
        fields.insert("name".to_string(), Value::Text("John".to_string()));
        fields.insert("age".to_string(), Value::Integer(30));
        let udt = Value::Udt("Person".to_string(), fields);
        assert_eq!(udt.data_type(), DataType::Udt);
        assert!(udt.to_string().contains("Person{"));

        // Test Frozen
        let frozen_list = Value::Frozen(Box::new(Value::List(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ])));
        assert_eq!(frozen_list.data_type(), DataType::Frozen);
        assert_eq!(frozen_list.to_string(), "FROZEN([1, 2, 3])");
    }

    #[test]
    fn test_new_data_types() {
        assert_eq!(DataType::Tuple.to_string(), "TUPLE");
        assert_eq!(DataType::Udt.to_string(), "UDT");
        assert_eq!(DataType::Frozen.to_string(), "FROZEN");

        // Test default values
        assert_eq!(DataType::Tuple.default_value(), Value::Tuple(Vec::new()));
        assert_eq!(
            DataType::Udt.default_value(),
            Value::Udt(String::new(), HashMap::new())
        );
        assert_eq!(
            DataType::Frozen.default_value(),
            Value::Frozen(Box::new(Value::Null))
        );
    }
}
