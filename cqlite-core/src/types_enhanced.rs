//! Enhanced core data types for CQLite with full Cassandra complex type support
//!
//! This module provides comprehensive type system support for Cassandra 5+ complex types:
//! - Collections (List, Set, Map) with proper type metadata
//! - User Defined Types (UDT) with field ordering and type specs
//! - Tuples with heterogeneous field types
//! - Frozen types with inner type preservation
//! - All Cassandra primitive types with exact binary compatibility

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

/// Database value type that can hold any supported data type with exact Cassandra compatibility
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
    /// JSON value (stored as text in Cassandra)
    Json(serde_json::Value),
    /// 8-bit signed integer (for exact Cassandra compatibility)
    TinyInt(i8),
    /// 16-bit signed integer (for exact Cassandra compatibility)
    SmallInt(i16),
    /// 32-bit floating point (for exact Cassandra compatibility)
    Float32(f32),
    
    // Enhanced complex types with proper metadata
    /// List of values with element type metadata
    List(CollectionValue),
    /// Set of values with element type metadata (implemented as Vec for ordering preservation)
    Set(CollectionValue),
    /// Map of key-value pairs with type metadata
    Map(MapValue),
    /// Tuple with fixed-size heterogeneous types and type metadata
    Tuple(TupleValue),
    /// User defined type with type metadata and ordered fields
    Udt(UdtValue),
    /// Frozen wrapper for collections (immutable) with inner type info
    Frozen(FrozenValue),
    
    // Additional Cassandra types
    /// Variable-length integer (Cassandra varint type)
    Varint(Vec<u8>),
    /// Decimal with scale and unscaled value
    Decimal { scale: i32, unscaled: Vec<u8> },
    /// Duration with months, days, and nanoseconds
    Duration { months: i32, days: i32, nanoseconds: i64 },
    /// Date as days since epoch (1970-01-01)
    Date(i32),
    /// Time as nanoseconds since midnight
    Time(i64),
    /// Counter value (64-bit signed integer)
    Counter(i64),
    /// Inet address (IPv4 or IPv6)
    Inet(Vec<u8>),
}

/// Complex type value containers for exact Cassandra format compatibility

/// Collection value with element type metadata
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CollectionValue {
    pub element_type: CqlTypeSpec,
    pub values: Vec<Value>,
}

/// Map value with key and value type metadata
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MapValue {
    pub key_type: CqlTypeSpec,
    pub value_type: CqlTypeSpec,
    pub entries: Vec<(Value, Value)>,
}

/// Tuple value with field types metadata
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TupleValue {
    pub field_types: Vec<CqlTypeSpec>,
    pub values: Vec<Value>,
}

/// UDT value with type metadata and ordered fields
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UdtValue {
    pub type_name: String,
    pub keyspace: Option<String>,
    pub field_specs: Vec<(String, CqlTypeSpec)>,
    pub field_values: HashMap<String, Value>,
}

/// Frozen value with inner type specification
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FrozenValue {
    pub inner_type: CqlTypeSpec,
    pub value: Box<Value>,
}

/// CQL type specification for complex type metadata
/// This provides the type information needed for proper binary serialization/deserialization
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CqlTypeSpec {
    // Primitive types
    Boolean,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Varint,
    Float,
    Double,
    Decimal,
    Text,
    Ascii,
    Varchar,
    Blob,
    Timestamp,
    Date,
    Time,
    Uuid,
    TimeUuid,
    Inet,
    Duration,
    Counter,
    
    // Collection types with element type info
    List(Box<CqlTypeSpec>),
    Set(Box<CqlTypeSpec>),
    Map(Box<CqlTypeSpec>, Box<CqlTypeSpec>),
    
    // Complex types
    Tuple(Vec<CqlTypeSpec>),
    Udt {
        keyspace: Option<String>,
        name: String,
        fields: Vec<(String, CqlTypeSpec)>,
    },
    Frozen(Box<CqlTypeSpec>),
    
    // Custom/Unknown
    Custom(String),
}

impl CqlTypeSpec {
    /// Get the expected byte size for fixed-size types
    pub fn fixed_size(&self) -> Option<usize> {
        match self {
            CqlTypeSpec::Boolean => Some(1),
            CqlTypeSpec::TinyInt => Some(1),
            CqlTypeSpec::SmallInt => Some(2),
            CqlTypeSpec::Int => Some(4),
            CqlTypeSpec::BigInt => Some(8),
            CqlTypeSpec::Float => Some(4),
            CqlTypeSpec::Double => Some(8),
            CqlTypeSpec::Timestamp => Some(8),
            CqlTypeSpec::Date => Some(4),
            CqlTypeSpec::Time => Some(8),
            CqlTypeSpec::Uuid | CqlTypeSpec::TimeUuid => Some(16),
            CqlTypeSpec::Counter => Some(8),
            
            // Variable size types
            CqlTypeSpec::Text
            | CqlTypeSpec::Ascii
            | CqlTypeSpec::Varchar
            | CqlTypeSpec::Blob
            | CqlTypeSpec::Decimal
            | CqlTypeSpec::Duration
            | CqlTypeSpec::Varint
            | CqlTypeSpec::Inet => None,
            
            // Collections and complex types are variable
            CqlTypeSpec::List(_)
            | CqlTypeSpec::Set(_)
            | CqlTypeSpec::Map(_, _)
            | CqlTypeSpec::Tuple(_)
            | CqlTypeSpec::Udt { .. } => None,
            
            CqlTypeSpec::Frozen(inner) => inner.fixed_size(),
            CqlTypeSpec::Custom(_) => None,
        }
    }

    /// Check if this type is a collection
    pub fn is_collection(&self) -> bool {
        matches!(
            self,
            CqlTypeSpec::List(_) | CqlTypeSpec::Set(_) | CqlTypeSpec::Map(_, _)
        )
    }

    /// Check if this type is a complex type (collection, tuple, or UDT)
    pub fn is_complex(&self) -> bool {
        matches!(
            self,
            CqlTypeSpec::List(_) 
            | CqlTypeSpec::Set(_) 
            | CqlTypeSpec::Map(_, _)
            | CqlTypeSpec::Tuple(_)
            | CqlTypeSpec::Udt { .. }
        )
    }

    /// Convert to Cassandra type ID for binary serialization
    pub fn to_type_id(&self) -> u8 {
        match self {
            CqlTypeSpec::Boolean => 0x04,
            CqlTypeSpec::TinyInt => 0x14,
            CqlTypeSpec::SmallInt => 0x13,
            CqlTypeSpec::Int => 0x09,
            CqlTypeSpec::BigInt => 0x02,
            CqlTypeSpec::Varint => 0x0E,
            CqlTypeSpec::Float => 0x08,
            CqlTypeSpec::Double => 0x07,
            CqlTypeSpec::Decimal => 0x06,
            CqlTypeSpec::Text | CqlTypeSpec::Varchar => 0x0D,
            CqlTypeSpec::Ascii => 0x01,
            CqlTypeSpec::Blob => 0x03,
            CqlTypeSpec::Timestamp => 0x0B,
            CqlTypeSpec::Date => 0x11,
            CqlTypeSpec::Time => 0x12,
            CqlTypeSpec::Uuid => 0x0C,
            CqlTypeSpec::TimeUuid => 0x0F,
            CqlTypeSpec::Inet => 0x10,
            CqlTypeSpec::Duration => 0x15,
            CqlTypeSpec::Counter => 0x05,
            CqlTypeSpec::List(_) => 0x20,
            CqlTypeSpec::Set(_) => 0x22,
            CqlTypeSpec::Map(_, _) => 0x21,
            CqlTypeSpec::Tuple(_) => 0x31,
            CqlTypeSpec::Udt { .. } => 0x30,
            CqlTypeSpec::Frozen(_) => 0xFF, // Special handling needed
            CqlTypeSpec::Custom(_) => 0x00,
        }
    }
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
            Value::Udt(_) => DataType::Udt,
            Value::Frozen(_) => DataType::Frozen,
            Value::Varint(_) => DataType::Varint,
            Value::Decimal { .. } => DataType::Decimal,
            Value::Duration { .. } => DataType::Duration,
            Value::Date(_) => DataType::Date,
            Value::Time(_) => DataType::Time,
            Value::Counter(_) => DataType::Counter,
            Value::Inet(_) => DataType::Inet,
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
            Value::Counter(i) => Some(*i),
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
            Value::Varint(b) => Some(b),
            Value::Inet(b) => Some(b),
            Value::Decimal { unscaled, .. } => Some(unscaled),
            _ => None,
        }
    }

    /// Get the type specification for this value
    pub fn type_spec(&self) -> CqlTypeSpec {
        match self {
            Value::Null => CqlTypeSpec::Blob, // Fallback
            Value::Boolean(_) => CqlTypeSpec::Boolean,
            Value::Integer(_) => CqlTypeSpec::Int,
            Value::BigInt(_) => CqlTypeSpec::BigInt,
            Value::Float(_) => CqlTypeSpec::Double,
            Value::Text(_) => CqlTypeSpec::Text,
            Value::Blob(_) => CqlTypeSpec::Blob,
            Value::Timestamp(_) => CqlTypeSpec::Timestamp,
            Value::Uuid(_) => CqlTypeSpec::Uuid,
            Value::Json(_) => CqlTypeSpec::Text, // JSON stored as text
            Value::TinyInt(_) => CqlTypeSpec::TinyInt,
            Value::SmallInt(_) => CqlTypeSpec::SmallInt,
            Value::Float32(_) => CqlTypeSpec::Float,
            Value::List(list) => CqlTypeSpec::List(Box::new(list.element_type.clone())),
            Value::Set(set) => CqlTypeSpec::Set(Box::new(set.element_type.clone())),
            Value::Map(map) => CqlTypeSpec::Map(
                Box::new(map.key_type.clone()),
                Box::new(map.value_type.clone()),
            ),
            Value::Tuple(tuple) => CqlTypeSpec::Tuple(tuple.field_types.clone()),
            Value::Udt(udt) => CqlTypeSpec::Udt {
                keyspace: udt.keyspace.clone(),
                name: udt.type_name.clone(),
                fields: udt.field_specs.clone(),
            },
            Value::Frozen(frozen) => CqlTypeSpec::Frozen(Box::new(frozen.inner_type.clone())),
            Value::Varint(_) => CqlTypeSpec::Varint,
            Value::Decimal { .. } => CqlTypeSpec::Decimal,
            Value::Duration { .. } => CqlTypeSpec::Duration,
            Value::Date(_) => CqlTypeSpec::Date,
            Value::Time(_) => CqlTypeSpec::Time,
            Value::Counter(_) => CqlTypeSpec::Counter,
            Value::Inet(_) => CqlTypeSpec::Inet,
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
                for (i, item) in set.values.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", item)?;
                }
                write!(f, "}}")
            }
            Value::List(list) => {
                write!(f, "[")?;
                for (i, item) in list.values.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", item)?;
                }
                write!(f, "]")
            }
            Value::Map(map) => {
                write!(f, "{{")?;
                for (i, (key, value)) in map.entries.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", key, value)?;
                }
                write!(f, "}}")
            }
            Value::Tuple(tuple) => {
                write!(f, "(")?;
                for (i, item) in tuple.values.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", item)?;
                }
                write!(f, ")")
            }
            Value::Udt(udt) => {
                write!(f, "{}{{" , udt.type_name)?;
                let mut field_iter = udt.field_specs.iter()
                    .filter_map(|(name, _)| {
                        udt.field_values.get(name).map(|value| (name, value))
                    }).enumerate();
                
                while let Some((i, (field_name, field_value))) = field_iter.next() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", field_name, field_value)?;
                }
                write!(f, "}}")
            }
            Value::Frozen(frozen) => {
                write!(f, "FROZEN({})", frozen.value)
            }
            Value::Varint(bytes) => {
                write!(f, "VARINT({})", hex::encode(bytes))
            }
            Value::Decimal { scale, unscaled } => {
                write!(f, "DECIMAL(scale={}, unscaled={})", scale, hex::encode(unscaled))
            }
            Value::Duration { months, days, nanoseconds } => {
                write!(f, "DURATION({}mo {}d {}ns)", months, days, nanoseconds)
            }
            Value::Date(days) => {
                write!(f, "DATE({})", days)
            }
            Value::Time(nanos) => {
                write!(f, "TIME({})", nanos)
            }
            Value::Counter(count) => {
                write!(f, "COUNTER({})", count)
            }
            Value::Inet(bytes) => {
                write!(f, "INET({})", hex::encode(bytes))
            }
        }
    }
}

/// Data type enumeration with complete Cassandra type support
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
    /// Variable-length integer
    Varint,
    /// 32-bit floating point
    Float32,
    /// 64-bit floating point
    Float,
    /// Decimal with arbitrary precision
    Decimal,
    /// Variable-length text
    Text,
    /// Variable-length binary data
    Blob,
    /// Timestamp with microsecond precision
    Timestamp,
    /// Date as days since epoch
    Date,
    /// Time as nanoseconds since midnight
    Time,
    /// UUID type
    Uuid,
    /// Time-based UUID
    TimeUuid,
    /// Counter type
    Counter,
    /// Duration type
    Duration,
    /// Inet address type
    Inet,
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
                | DataType::Varint
                | DataType::Float32
                | DataType::Float
                | DataType::Decimal
                | DataType::Counter
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

    /// Check if this type is temporal
    pub fn is_temporal(&self) -> bool {
        matches!(self, DataType::Timestamp | DataType::Date | DataType::Time | DataType::Duration)
    }

    /// Check if this type is a collection
    pub fn is_collection(&self) -> bool {
        matches!(self, DataType::List | DataType::Set | DataType::Map)
    }

    /// Check if this type is complex (collection, tuple, or UDT)
    pub fn is_complex(&self) -> bool {
        matches!(self, DataType::List | DataType::Set | DataType::Map | DataType::Tuple | DataType::Udt)
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
            DataType::Varint => Value::Varint(vec![0]),
            DataType::Float32 => Value::Float32(0.0),
            DataType::Float => Value::Float(0.0),
            DataType::Decimal => Value::Decimal { scale: 0, unscaled: vec![0] },
            DataType::Text => Value::Text(String::new()),
            DataType::Blob => Value::Blob(Vec::new()),
            DataType::Timestamp => Value::Timestamp(0),
            DataType::Date => Value::Date(0),
            DataType::Time => Value::Time(0),
            DataType::Uuid => Value::Uuid([0; 16]),
            DataType::TimeUuid => Value::Uuid([0; 16]),
            DataType::Counter => Value::Counter(0),
            DataType::Duration => Value::Duration { months: 0, days: 0, nanoseconds: 0 },
            DataType::Inet => Value::Inet(vec![0, 0, 0, 0]),
            DataType::Json => Value::Json(serde_json::Value::Null),
            DataType::List => Value::List(CollectionValue {
                element_type: CqlTypeSpec::Blob,
                values: Vec::new(),
            }),
            DataType::Set => Value::Set(CollectionValue {
                element_type: CqlTypeSpec::Blob,
                values: Vec::new(),
            }),
            DataType::Map => Value::Map(MapValue {
                key_type: CqlTypeSpec::Blob,
                value_type: CqlTypeSpec::Blob,
                entries: Vec::new(),
            }),
            DataType::Tuple => Value::Tuple(TupleValue {
                field_types: Vec::new(),
                values: Vec::new(),
            }),
            DataType::Udt => Value::Udt(UdtValue {
                type_name: String::new(),
                keyspace: None,
                field_specs: Vec::new(),
                field_values: HashMap::new(),
            }),
            DataType::Frozen => Value::Frozen(FrozenValue {
                inner_type: CqlTypeSpec::Blob,
                value: Box::new(Value::Null),
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
            DataType::Varint => "VARINT",
            DataType::Float32 => "FLOAT32",
            DataType::Float => "FLOAT",
            DataType::Decimal => "DECIMAL",
            DataType::Text => "TEXT",
            DataType::Blob => "BLOB",
            DataType::Timestamp => "TIMESTAMP",
            DataType::Date => "DATE",
            DataType::Time => "TIME",
            DataType::Uuid => "UUID",
            DataType::TimeUuid => "TIMEUUID",
            DataType::Counter => "COUNTER",
            DataType::Duration => "DURATION",
            DataType::Inet => "INET",
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

// Helper constructors for complex types
impl CollectionValue {
    /// Create a new list/set collection value
    pub fn new(element_type: CqlTypeSpec, values: Vec<Value>) -> Self {
        Self { element_type, values }
    }

    /// Create an empty collection with the given element type
    pub fn empty(element_type: CqlTypeSpec) -> Self {
        Self::new(element_type, Vec::new())
    }
}

impl MapValue {
    /// Create a new map value
    pub fn new(key_type: CqlTypeSpec, value_type: CqlTypeSpec, entries: Vec<(Value, Value)>) -> Self {
        Self { key_type, value_type, entries }
    }

    /// Create an empty map with the given key and value types
    pub fn empty(key_type: CqlTypeSpec, value_type: CqlTypeSpec) -> Self {
        Self::new(key_type, value_type, Vec::new())
    }
}

impl TupleValue {
    /// Create a new tuple value
    pub fn new(field_types: Vec<CqlTypeSpec>, values: Vec<Value>) -> Self {
        Self { field_types, values }
    }

    /// Create an empty tuple with the given field types
    pub fn empty(field_types: Vec<CqlTypeSpec>) -> Self {
        let values = vec![Value::Null; field_types.len()];
        Self::new(field_types, values)
    }
}

impl UdtValue {
    /// Create a new UDT value
    pub fn new(
        type_name: String,
        keyspace: Option<String>,
        field_specs: Vec<(String, CqlTypeSpec)>,
        field_values: HashMap<String, Value>,
    ) -> Self {
        Self {
            type_name,
            keyspace,
            field_specs,
            field_values,
        }
    }

    /// Create an empty UDT with the given type definition
    pub fn empty(
        type_name: String,
        keyspace: Option<String>,
        field_specs: Vec<(String, CqlTypeSpec)>,
    ) -> Self {
        Self::new(type_name, keyspace, field_specs, HashMap::new())
    }

    /// Get a field value by name
    pub fn get_field(&self, name: &str) -> Option<&Value> {
        self.field_values.get(name)
    }

    /// Set a field value
    pub fn set_field(&mut self, name: String, value: Value) {
        self.field_values.insert(name, value);
    }
}

impl FrozenValue {
    /// Create a new frozen value
    pub fn new(inner_type: CqlTypeSpec, value: Value) -> Self {
        Self {
            inner_type,
            value: Box::new(value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enhanced_value_types() {
        // Test collection value
        let list = Value::List(CollectionValue::new(
            CqlTypeSpec::Int,
            vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)],
        ));
        assert_eq!(list.data_type(), DataType::List);

        // Test map value
        let map = Value::Map(MapValue::new(
            CqlTypeSpec::Text,
            CqlTypeSpec::Int,
            vec![
                (Value::Text("key1".to_string()), Value::Integer(1)),
                (Value::Text("key2".to_string()), Value::Integer(2)),
            ],
        ));
        assert_eq!(map.data_type(), DataType::Map);

        // Test UDT value
        let mut udt = UdtValue::empty(
            "Person".to_string(),
            Some("test".to_string()),
            vec![
                ("name".to_string(), CqlTypeSpec::Text),
                ("age".to_string(), CqlTypeSpec::Int),
            ],
        );
        udt.set_field("name".to_string(), Value::Text("John".to_string()));
        udt.set_field("age".to_string(), Value::Integer(30));
        
        let udt_value = Value::Udt(udt);
        assert_eq!(udt_value.data_type(), DataType::Udt);
    }

    #[test]
    fn test_type_spec_metadata() {
        let list_spec = CqlTypeSpec::List(Box::new(CqlTypeSpec::Int));
        assert!(list_spec.is_collection());
        assert!(list_spec.is_complex());
        assert_eq!(list_spec.to_type_id(), 0x20);

        let map_spec = CqlTypeSpec::Map(Box::new(CqlTypeSpec::Text), Box::new(CqlTypeSpec::Int));
        assert!(map_spec.is_collection());
        assert!(map_spec.is_complex());
        assert_eq!(map_spec.to_type_id(), 0x21);
    }

    #[test]
    fn test_cassandra_types() {
        // Test Varint
        let varint = Value::Varint(vec![0x01, 0x02, 0x03]);
        assert_eq!(varint.data_type(), DataType::Varint);

        // Test Decimal
        let decimal = Value::Decimal {
            scale: 2,
            unscaled: vec![0x01, 0x23],
        };
        assert_eq!(decimal.data_type(), DataType::Decimal);

        // Test Duration
        let duration = Value::Duration {
            months: 1,
            days: 15,
            nanoseconds: 123456789,
        };
        assert_eq!(duration.data_type(), DataType::Duration);
    }

    #[test]
    fn test_display_formatting() {
        // Test collection display
        let list = Value::List(CollectionValue::new(
            CqlTypeSpec::Int,
            vec![Value::Integer(1), Value::Integer(2)],
        ));
        assert_eq!(list.to_string(), "[1, 2]");

        // Test UDT display
        let mut udt = UdtValue::empty(
            "Person".to_string(),
            None,
            vec![("name".to_string(), CqlTypeSpec::Text)],
        );
        udt.set_field("name".to_string(), Value::Text("John".to_string()));
        let udt_value = Value::Udt(udt);
        assert!(udt_value.to_string().contains("Person{"));
        assert!(udt_value.to_string().contains("name: 'John'"));
    }
}