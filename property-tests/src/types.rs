//! CQL Type definitions for property testing

use serde::{Serialize, Deserialize};
use std::collections::HashMap;

/// CQL Value type that mirrors cqlite_core::types::Value
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash, Eq)]
pub enum CqlValue {
    Null,
    Boolean(bool),
    Integer(i32),
    BigInt(i64),
    Float(OrderedFloat),
    Float32(OrderedFloat32),
    TinyInt(i8),
    SmallInt(i16),
    Text(String),
    Blob(Vec<u8>),
    Timestamp(i64),
    Uuid([u8; 16]),
    Varint(Vec<u8>),
    Decimal { scale: i32, unscaled: Vec<u8> },
    Duration { months: i32, days: i32, nanos: i64 },
    Json(serde_json::Value),
    List(Vec<CqlValue>),
    Set(Vec<CqlValue>),
    Map(Vec<(CqlValue, CqlValue)>),
    Tuple(Vec<CqlValue>),
    Udt(UdtValue),
    Frozen(Box<CqlValue>),
    Tombstone(TombstoneInfo),
}

/// User-defined type value
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash, Eq)]
pub struct UdtValue {
    pub type_name: String,
    pub keyspace: String,
    pub fields: Vec<UdtField>,
}

/// UDT field
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash, Eq)]
pub struct UdtField {
    pub name: String,
    pub value: Option<CqlValue>,
}

/// Tombstone information
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash, Eq)]
pub struct TombstoneInfo {
    pub deletion_time: i64,
    pub local_deletion_time: i32,
}

/// Ordered wrapper for f64 to make it hashable and comparable
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct OrderedFloat(pub f64);

impl PartialEq for OrderedFloat {
    fn eq(&self, other: &Self) -> bool {
        if self.0.is_nan() && other.0.is_nan() {
            true
        } else {
            self.0 == other.0
        }
    }
}

impl Eq for OrderedFloat {}

impl std::hash::Hash for OrderedFloat {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        if self.0.is_nan() {
            0u64.hash(state);
        } else {
            self.0.to_bits().hash(state);
        }
    }
}

/// Ordered wrapper for f32
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct OrderedFloat32(pub f32);

impl PartialEq for OrderedFloat32 {
    fn eq(&self, other: &Self) -> bool {
        if self.0.is_nan() && other.0.is_nan() {
            true
        } else {
            self.0 == other.0
        }
    }
}

impl Eq for OrderedFloat32 {}

impl std::hash::Hash for OrderedFloat32 {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        if self.0.is_nan() {
            0u32.hash(state);
        } else {
            self.0.to_bits().hash(state);
        }
    }
}

/// Schema information for testing
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Schema {
    pub keyspace: String,
    pub table: String,
    pub partition_keys: Vec<KeyColumn>,
    pub clustering_keys: Vec<KeyColumn>,
    pub columns: Vec<Column>,
    pub comments: HashMap<String, String>,
}

/// Key column definition
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeyColumn {
    pub name: String,
    pub data_type: String,
    pub position: usize,
}

/// Regular column definition
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: String,
    pub is_static: bool,
}

impl CqlValue {
    /// Get the type name of this value
    pub fn type_name(&self) -> &'static str {
        match self {
            CqlValue::Null => "null",
            CqlValue::Boolean(_) => "boolean",
            CqlValue::Integer(_) => "int",
            CqlValue::BigInt(_) => "bigint",
            CqlValue::Float(_) => "double",
            CqlValue::Float32(_) => "float",
            CqlValue::TinyInt(_) => "tinyint",
            CqlValue::SmallInt(_) => "smallint",
            CqlValue::Text(_) => "text",
            CqlValue::Blob(_) => "blob",
            CqlValue::Timestamp(_) => "timestamp",
            CqlValue::Uuid(_) => "uuid",
            CqlValue::Varint(_) => "varint",
            CqlValue::Decimal { .. } => "decimal",
            CqlValue::Duration { .. } => "duration",
            CqlValue::Json(_) => "json",
            CqlValue::List(_) => "list",
            CqlValue::Set(_) => "set",
            CqlValue::Map(_) => "map",
            CqlValue::Tuple(_) => "tuple",
            CqlValue::Udt(_) => "udt",
            CqlValue::Frozen(_) => "frozen",
            CqlValue::Tombstone(_) => "tombstone",
        }
    }

    /// Check if this value is null
    pub fn is_null(&self) -> bool {
        matches!(self, CqlValue::Null)
    }

    /// Estimate the memory size of this value
    pub fn estimate_size(&self) -> usize {
        match self {
            CqlValue::Null => 1,
            CqlValue::Boolean(_) => 1,
            CqlValue::Integer(_) => 4,
            CqlValue::BigInt(_) => 8,
            CqlValue::Float(_) => 8,
            CqlValue::Float32(_) => 4,
            CqlValue::TinyInt(_) => 1,
            CqlValue::SmallInt(_) => 2,
            CqlValue::Text(s) => s.len(),
            CqlValue::Blob(b) => b.len(),
            CqlValue::Timestamp(_) => 8,
            CqlValue::Uuid(_) => 16,
            CqlValue::Varint(v) => v.len(),
            CqlValue::Decimal { unscaled, .. } => 4 + unscaled.len(),
            CqlValue::Duration { .. } => 16,
            CqlValue::Json(j) => j.to_string().len(),
            CqlValue::List(items) => items.iter().map(|v| v.estimate_size()).sum::<usize>() + 8,
            CqlValue::Set(items) => items.iter().map(|v| v.estimate_size()).sum::<usize>() + 8,
            CqlValue::Map(items) => items.iter()
                .map(|(k, v)| k.estimate_size() + v.estimate_size())
                .sum::<usize>() + 8,
            CqlValue::Tuple(items) => items.iter().map(|v| v.estimate_size()).sum::<usize>() + 8,
            CqlValue::Udt(udt) => {
                udt.type_name.len() + udt.keyspace.len() +
                udt.fields.iter().map(|f| {
                    f.name.len() + f.value.as_ref().map(|v| v.estimate_size()).unwrap_or(0)
                }).sum::<usize>() + 16
            },
            CqlValue::Frozen(boxed) => boxed.estimate_size() + 8,
            CqlValue::Tombstone(_) => 16,
        }
    }
}