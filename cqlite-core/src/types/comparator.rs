//! ComparatorType system for type comparison logic in CQLite
//!
//! This module provides a comprehensive comparison system for all CQL data types,
//! including primitive types, collections, UDTs, and frozen types. The comparator
//! system ensures proper ordering and equality comparison that matches Cassandra's
//! comparison semantics.

use crate::schema::{CqlType, UdtRegistry};
use crate::types::Value;
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

/// ComparatorType enum that handles comparison logic for different CQL types
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ComparatorType {
    /// Boolean type comparator
    Boolean,
    /// 8-bit signed integer comparator
    TinyInt,
    /// 16-bit signed integer comparator
    SmallInt,
    /// 32-bit signed integer comparator
    Int,
    /// 64-bit signed integer comparator
    BigInt,
    /// Counter type comparator (stored as 64-bit signed integer)
    Counter,
    /// 32-bit floating point comparator
    Float32,
    /// 64-bit floating point comparator
    Float,
    /// UTF-8 text comparator (lexicographic)
    Text,
    /// Binary data comparator (byte-wise)
    Blob,
    /// Timestamp comparator (chronological)
    Timestamp,
    /// UUID comparator (byte-wise)
    Uuid,
    /// Variable-length integer comparator
    Varint,
    /// Decimal type comparator
    Decimal,
    /// Duration type comparator
    Duration,
    /// Date type comparator (days since epoch)
    Date,
    /// JSON document comparator
    Json,
    /// List comparator with element comparator
    List(Box<ComparatorType>),
    /// Set comparator with element comparator
    Set(Box<ComparatorType>),
    /// Map comparator with key and value comparators
    Map(Box<ComparatorType>, Box<ComparatorType>),
    /// Tuple comparator with field comparators
    Tuple(Vec<ComparatorType>),
    /// UDT comparator with field definitions
    Udt {
        type_name: String,
        keyspace: Option<String>,
        field_comparators: Vec<(String, ComparatorType)>,
    },
    /// Frozen type comparator (byte-wise comparison of serialized form)
    Frozen(Box<ComparatorType>),
    /// Custom type comparator (fallback)
    Custom(String),
}

// Struct-size regression guard (issue #1616, Epic H/H3; see
// docs/reports/parser-performance-audit-2026-07-01.md §Epic H (finding H3)).
// `ComparatorType` is constructed per column during schema-aware decode and
// clustering-key comparison on the read hot path; its widest variant (`Udt`)
// sets the layout. Measured 72 bytes today on 64-bit targets. Update this pin
// DELIBERATELY, never silently: any change — growth or shrink — must be a
// reviewed edit here.
#[cfg(target_pointer_width = "64")]
const _: () = assert!(std::mem::size_of::<ComparatorType>() == 72);

impl ComparatorType {
    /// Create a ComparatorType from a CqlType
    pub fn from_cql_type(cql_type: &CqlType) -> Result<Self> {
        let comparator = match cql_type {
            CqlType::Boolean => ComparatorType::Boolean,
            CqlType::TinyInt => ComparatorType::TinyInt,
            CqlType::SmallInt => ComparatorType::SmallInt,
            CqlType::Int => ComparatorType::Int,
            CqlType::BigInt => ComparatorType::BigInt,
            CqlType::Counter => ComparatorType::Counter,
            CqlType::Float => ComparatorType::Float32,
            CqlType::Double => ComparatorType::Float,
            CqlType::Text | CqlType::Varchar => ComparatorType::Text,
            CqlType::Ascii => ComparatorType::Text, // Map ASCII to Text for simplicity
            CqlType::Blob => ComparatorType::Blob,
            CqlType::Timestamp => ComparatorType::Timestamp,
            CqlType::Uuid => ComparatorType::Uuid,
            CqlType::TimeUuid => ComparatorType::Uuid, // Map TimeUuid to Uuid for simplicity
            CqlType::Varint => ComparatorType::Varint,
            CqlType::List(element_type) => {
                let element_comparator = Self::from_cql_type(element_type)?;
                ComparatorType::List(Box::new(element_comparator))
            }
            CqlType::Set(element_type) => {
                let element_comparator = Self::from_cql_type(element_type)?;
                ComparatorType::Set(Box::new(element_comparator))
            }
            CqlType::Map(key_type, value_type) => {
                let key_comparator = Self::from_cql_type(key_type)?;
                let value_comparator = Self::from_cql_type(value_type)?;
                ComparatorType::Map(Box::new(key_comparator), Box::new(value_comparator))
            }
            CqlType::Tuple(field_types) => {
                let mut field_comparators = Vec::new();
                for field_type in field_types {
                    field_comparators.push(Self::from_cql_type(field_type)?);
                }
                ComparatorType::Tuple(field_comparators)
            }
            CqlType::Udt(type_name, fields) => {
                let mut field_comparators = Vec::new();
                for (field_name, field_type) in fields {
                    let field_comparator = Self::from_cql_type(field_type)?;
                    field_comparators.push((field_name.clone(), field_comparator));
                }
                ComparatorType::Udt {
                    type_name: type_name.clone(),
                    keyspace: None, // Default keyspace, should be set by caller
                    field_comparators,
                }
            }
            CqlType::Frozen(inner_type) => {
                let inner_comparator = Self::from_cql_type(inner_type)?;
                ComparatorType::Frozen(Box::new(inner_comparator))
            }
            CqlType::Custom(type_name) => ComparatorType::Custom(type_name.clone()),
            // Map newly supported types
            CqlType::Decimal => ComparatorType::Decimal,
            CqlType::Duration => ComparatorType::Duration,
            CqlType::Date => ComparatorType::Date,
            // Map remaining unsupported types to custom for now
            CqlType::Time => ComparatorType::Custom("time".to_string()),
            CqlType::Inet => ComparatorType::Custom("inet".to_string()),
        };

        Ok(comparator)
    }

    /// Create a ComparatorType from a string representation
    pub fn from_type_string(type_str: &str) -> Result<Self> {
        let cql_type = CqlType::parse(type_str)?;
        Self::from_cql_type(&cql_type)
    }

    /// Create a ComparatorType from a data type string (alias for from_type_string)
    pub fn from_data_type(data_type: &str) -> Result<Self> {
        Self::from_type_string(data_type)
    }

    /// Create a ComparatorType from a data type string with UDT registry resolution
    ///
    /// This method resolves UDT type references (e.g., "address_type") to their full
    /// field definitions using the provided UDT registry. This is essential for
    /// properly parsing UDTs inside collections like `list<frozen<address_type>>`.
    ///
    /// # Arguments
    /// * `data_type` - The data type string (e.g., "list<frozen<address_type>>")
    /// * `registry` - The UDT registry containing type definitions
    /// * `keyspace` - The keyspace to use for UDT lookup
    pub fn from_data_type_with_registry(
        data_type: &str,
        registry: &UdtRegistry,
        keyspace: &str,
    ) -> Result<Self> {
        let cql_type = CqlType::parse(data_type)?;
        Self::from_cql_type_with_registry(&cql_type, registry, keyspace)
    }

    /// Create a ComparatorType from a CqlType with UDT registry resolution
    fn from_cql_type_with_registry(
        cql_type: &CqlType,
        registry: &UdtRegistry,
        keyspace: &str,
    ) -> Result<Self> {
        let comparator = match cql_type {
            CqlType::Boolean => ComparatorType::Boolean,
            CqlType::TinyInt => ComparatorType::TinyInt,
            CqlType::SmallInt => ComparatorType::SmallInt,
            CqlType::Int => ComparatorType::Int,
            CqlType::BigInt => ComparatorType::BigInt,
            CqlType::Counter => ComparatorType::Counter,
            CqlType::Float => ComparatorType::Float32,
            CqlType::Double => ComparatorType::Float,
            CqlType::Text | CqlType::Varchar => ComparatorType::Text,
            CqlType::Ascii => ComparatorType::Text,
            CqlType::Blob => ComparatorType::Blob,
            CqlType::Timestamp => ComparatorType::Timestamp,
            CqlType::Uuid => ComparatorType::Uuid,
            CqlType::TimeUuid => ComparatorType::Uuid,
            CqlType::Varint => ComparatorType::Varint,
            CqlType::Decimal => ComparatorType::Decimal,
            CqlType::Duration => ComparatorType::Duration,
            CqlType::Date => ComparatorType::Date,
            CqlType::Time => ComparatorType::Custom("time".to_string()),
            CqlType::Inet => ComparatorType::Custom("inet".to_string()),
            CqlType::List(element_type) => {
                let element_comparator =
                    Self::from_cql_type_with_registry(element_type, registry, keyspace)?;
                ComparatorType::List(Box::new(element_comparator))
            }
            CqlType::Set(element_type) => {
                let element_comparator =
                    Self::from_cql_type_with_registry(element_type, registry, keyspace)?;
                ComparatorType::Set(Box::new(element_comparator))
            }
            CqlType::Map(key_type, value_type) => {
                let key_comparator =
                    Self::from_cql_type_with_registry(key_type, registry, keyspace)?;
                let value_comparator =
                    Self::from_cql_type_with_registry(value_type, registry, keyspace)?;
                ComparatorType::Map(Box::new(key_comparator), Box::new(value_comparator))
            }
            CqlType::Tuple(field_types) => {
                let mut field_comparators = Vec::new();
                for field_type in field_types {
                    field_comparators.push(Self::from_cql_type_with_registry(
                        field_type, registry, keyspace,
                    )?);
                }
                ComparatorType::Tuple(field_comparators)
            }
            CqlType::Udt(type_name, fields) => {
                let mut field_comparators = Vec::new();
                for (field_name, field_type) in fields {
                    let field_comparator =
                        Self::from_cql_type_with_registry(field_type, registry, keyspace)?;
                    field_comparators.push((field_name.clone(), field_comparator));
                }
                ComparatorType::Udt {
                    type_name: type_name.clone(),
                    keyspace: Some(keyspace.to_string()),
                    field_comparators,
                }
            }
            CqlType::Frozen(inner_type) => {
                let inner_comparator =
                    Self::from_cql_type_with_registry(inner_type, registry, keyspace)?;
                ComparatorType::Frozen(Box::new(inner_comparator))
            }
            CqlType::Custom(type_name) => {
                // Check if this is a UDT reference (format: "udt:type_name" or just "type_name")
                let udt_name = type_name.strip_prefix("udt:").unwrap_or(type_name);

                // Look up UDT in registry - try with and without keyspace
                let udt_def = registry
                    .get_udt(keyspace, udt_name)
                    .or_else(|| registry.get_udt(keyspace, type_name));

                if let Some(udt_def) = udt_def {
                    let mut field_comparators = Vec::new();
                    for field in &udt_def.fields {
                        // Recursively resolve field types (field_type is already CqlType)
                        let field_comparator = Self::from_cql_type_with_registry(
                            &field.field_type,
                            registry,
                            keyspace,
                        )?;
                        field_comparators.push((field.name.clone(), field_comparator));
                    }
                    ComparatorType::Udt {
                        type_name: udt_def.name.clone(),
                        keyspace: Some(keyspace.to_string()),
                        field_comparators,
                    }
                } else {
                    // UDT not found in registry - return as custom
                    ComparatorType::Custom(type_name.clone())
                }
            }
        };

        Ok(comparator)
    }

    /// Compare two values using this comparator
    pub fn compare(&self, left: &Value, right: &Value) -> Result<Ordering> {
        // Handle null values first (nulls are always considered less than non-nulls)
        match (left.is_null(), right.is_null()) {
            (true, true) => return Ok(Ordering::Equal),
            (true, false) => return Ok(Ordering::Less),
            (false, true) => return Ok(Ordering::Greater),
            (false, false) => {} // Both non-null, continue with type-specific comparison
        }

        match self {
            ComparatorType::Boolean => self.compare_boolean(left, right),
            ComparatorType::TinyInt => self.compare_tinyint(left, right),
            ComparatorType::SmallInt => self.compare_smallint(left, right),
            ComparatorType::Int => self.compare_int(left, right),
            ComparatorType::BigInt => self.compare_bigint(left, right),
            ComparatorType::Counter => self.compare_bigint(left, right), // Counter uses same comparison as bigint
            ComparatorType::Float32 => self.compare_float32(left, right),
            ComparatorType::Float => self.compare_float(left, right),
            ComparatorType::Text => self.compare_text(left, right),
            ComparatorType::Blob => self.compare_blob(left, right),
            ComparatorType::Timestamp => self.compare_timestamp(left, right),
            ComparatorType::Uuid => self.compare_uuid(left, right),
            ComparatorType::Varint => self.compare_varint(left, right),
            ComparatorType::Decimal => self.compare_decimal(left, right),
            ComparatorType::Duration => self.compare_duration(left, right),
            ComparatorType::Date => self.compare_date(left, right),
            ComparatorType::Json => self.compare_json(left, right),
            ComparatorType::List(element_comparator) => {
                self.compare_list(left, right, element_comparator)
            }
            ComparatorType::Set(element_comparator) => {
                self.compare_set(left, right, element_comparator)
            }
            ComparatorType::Map(key_comparator, value_comparator) => {
                self.compare_map(left, right, key_comparator, value_comparator)
            }
            ComparatorType::Tuple(field_comparators) => {
                self.compare_tuple(left, right, field_comparators)
            }
            ComparatorType::Udt {
                field_comparators, ..
            } => self.compare_udt(left, right, field_comparators),
            ComparatorType::Frozen(inner_comparator) => {
                self.compare_frozen(left, right, inner_comparator)
            }
            ComparatorType::Custom(name) => super::comparator_custom::compare(name, left, right),
        }
    }

    /// Check if two values are equal using this comparator
    pub fn equals(&self, left: &Value, right: &Value) -> Result<bool> {
        Ok(self.compare(left, right)? == Ordering::Equal)
    }

    /// Check if left value is less than right value
    pub fn less_than(&self, left: &Value, right: &Value) -> Result<bool> {
        Ok(self.compare(left, right)? == Ordering::Less)
    }

    /// Check if left value is greater than right value
    pub fn greater_than(&self, left: &Value, right: &Value) -> Result<bool> {
        Ok(self.compare(left, right)? == Ordering::Greater)
    }

    /// Check if left value is less than or equal to right value
    pub fn less_than_or_equal(&self, left: &Value, right: &Value) -> Result<bool> {
        let ordering = self.compare(left, right)?;
        Ok(ordering == Ordering::Less || ordering == Ordering::Equal)
    }

    /// Check if left value is greater than or equal to right value
    pub fn greater_than_or_equal(&self, left: &Value, right: &Value) -> Result<bool> {
        let ordering = self.compare(left, right)?;
        Ok(ordering == Ordering::Greater || ordering == Ordering::Equal)
    }

    /// Get the type name for this comparator
    pub fn type_name(&self) -> &str {
        match self {
            ComparatorType::Boolean => "boolean",
            ComparatorType::TinyInt => "tinyint",
            ComparatorType::SmallInt => "smallint",
            ComparatorType::Int => "int",
            ComparatorType::BigInt => "bigint",
            ComparatorType::Counter => "counter",
            ComparatorType::Float32 => "float",
            ComparatorType::Float => "double",
            ComparatorType::Text => "text",
            ComparatorType::Blob => "blob",
            ComparatorType::Timestamp => "timestamp",
            ComparatorType::Uuid => "uuid",
            ComparatorType::Varint => "varint",
            ComparatorType::Decimal => "decimal",
            ComparatorType::Duration => "duration",
            ComparatorType::Date => "date",
            ComparatorType::Json => "json",
            ComparatorType::List(_) => "list",
            ComparatorType::Set(_) => "set",
            ComparatorType::Map(_, _) => "map",
            ComparatorType::Tuple(_) => "tuple",
            ComparatorType::Udt { .. } => "udt",
            ComparatorType::Frozen(_) => "frozen",
            ComparatorType::Custom(name) => name,
        }
    }

    /// Check if this comparator supports ordering (some types only support equality)
    pub fn supports_ordering(&self) -> bool {
        match self {
            ComparatorType::Boolean
            | ComparatorType::TinyInt
            | ComparatorType::SmallInt
            | ComparatorType::Int
            | ComparatorType::BigInt
            | ComparatorType::Counter
            | ComparatorType::Float32
            | ComparatorType::Float
            | ComparatorType::Text
            | ComparatorType::Blob
            | ComparatorType::Timestamp
            | ComparatorType::Uuid
            | ComparatorType::Varint
            | ComparatorType::Decimal
            | ComparatorType::Duration
            | ComparatorType::Date
            | ComparatorType::Json => true,
            ComparatorType::List(element_comparator) => element_comparator.supports_ordering(),
            ComparatorType::Set(_) => false, // Sets only support equality
            ComparatorType::Map(_, _) => false, // Maps only support equality
            ComparatorType::Tuple(field_comparators) => {
                field_comparators.iter().all(|c| c.supports_ordering())
            }
            ComparatorType::Udt {
                field_comparators, ..
            } => field_comparators.iter().all(|(_, c)| c.supports_ordering()),
            ComparatorType::Frozen(inner_comparator) => inner_comparator.supports_ordering(),
            // `inet` / `time` order by value (issue #3790); the residual
            // unresolved-UDT / unknown name does not.
            ComparatorType::Custom(name) => super::comparator_custom::supports_ordering(name),
        }
    }

    // Private comparison methods for each type

    fn compare_boolean(&self, left: &Value, right: &Value) -> Result<Ordering> {
        match (left.as_bool(), right.as_bool()) {
            (Some(l), Some(r)) => Ok(l.cmp(&r)),
            _ => Err(Error::Schema(
                "Type mismatch: expected boolean values".to_string(),
            )),
        }
    }

    fn compare_tinyint(&self, left: &Value, right: &Value) -> Result<Ordering> {
        match (left, right) {
            (Value::TinyInt(l), Value::TinyInt(r)) => Ok(l.cmp(r)),
            _ => Err(Error::Schema(
                "Type mismatch: expected tinyint values".to_string(),
            )),
        }
    }

    fn compare_smallint(&self, left: &Value, right: &Value) -> Result<Ordering> {
        match (left, right) {
            (Value::SmallInt(l), Value::SmallInt(r)) => Ok(l.cmp(r)),
            _ => Err(Error::Schema(
                "Type mismatch: expected smallint values".to_string(),
            )),
        }
    }

    fn compare_int(&self, left: &Value, right: &Value) -> Result<Ordering> {
        match (left.as_i32(), right.as_i32()) {
            (Some(l), Some(r)) => Ok(l.cmp(&r)),
            _ => Err(Error::Schema(
                "Type mismatch: expected int values".to_string(),
            )),
        }
    }

    fn compare_bigint(&self, left: &Value, right: &Value) -> Result<Ordering> {
        match (left.as_i64(), right.as_i64()) {
            (Some(l), Some(r)) => Ok(l.cmp(&r)),
            _ => Err(Error::Schema(
                "Type mismatch: expected bigint values".to_string(),
            )),
        }
    }

    fn compare_float32(&self, left: &Value, right: &Value) -> Result<Ordering> {
        match (left, right) {
            // Cassandra/Java total order (NaN last, -0.0 < +0.0), NOT IEEE
            // partial_cmp — this is the schema-aware clustering-key read hot
            // path and must agree with the writer's ClusteringKey ordering.
            // See float_cmp.rs and issues #1870/#2010.
            (Value::Float32(l), Value::Float32(r)) => {
                Ok(crate::float_cmp::cassandra_float_cmp(*l, *r))
            }
            _ => Err(Error::Schema(
                "Type mismatch: expected float32 values".to_string(),
            )),
        }
    }

    fn compare_float(&self, left: &Value, right: &Value) -> Result<Ordering> {
        match (left.as_f64(), right.as_f64()) {
            // Cassandra/Java total order (NaN last, -0.0 < +0.0), NOT IEEE
            // partial_cmp. See compare_float32 / float_cmp.rs (#1870/#2010).
            (Some(l), Some(r)) => Ok(crate::float_cmp::cassandra_double_cmp(l, r)),
            _ => Err(Error::Schema(
                "Type mismatch: expected float values".to_string(),
            )),
        }
    }

    fn compare_text(&self, left: &Value, right: &Value) -> Result<Ordering> {
        match (left.as_str(), right.as_str()) {
            (Some(l), Some(r)) => Ok(l.cmp(r)),
            _ => Err(Error::Schema(
                "Type mismatch: expected text values".to_string(),
            )),
        }
    }

    fn compare_json(&self, left: &Value, right: &Value) -> Result<Ordering> {
        match (left, right) {
            (Value::Json(l), Value::Json(r)) => {
                // Compare JSON by string representation for now
                let l_str = serde_json::to_string(l).unwrap_or_default();
                let r_str = serde_json::to_string(r).unwrap_or_default();
                Ok(l_str.cmp(&r_str))
            }
            _ => Err(Error::Schema(
                "Type mismatch: expected JSON values".to_string(),
            )),
        }
    }

    fn compare_blob(&self, left: &Value, right: &Value) -> Result<Ordering> {
        match (left.as_bytes(), right.as_bytes()) {
            (Some(l), Some(r)) => Ok(l.cmp(r)),
            _ => Err(Error::Schema(
                "Type mismatch: expected blob values".to_string(),
            )),
        }
    }

    fn compare_timestamp(&self, left: &Value, right: &Value) -> Result<Ordering> {
        match (left, right) {
            (Value::Timestamp(l), Value::Timestamp(r)) => Ok(l.cmp(r)),
            _ => Err(Error::Schema(
                "Type mismatch: expected timestamp values".to_string(),
            )),
        }
    }

    fn compare_date(&self, left: &Value, right: &Value) -> Result<Ordering> {
        match (left, right) {
            (Value::Date(l), Value::Date(r)) => Ok(l.cmp(r)),
            _ => Err(Error::Schema(
                "Type mismatch: expected date values".to_string(),
            )),
        }
    }

    fn compare_uuid(&self, left: &Value, right: &Value) -> Result<Ordering> {
        match (left, right) {
            (Value::Uuid(l), Value::Uuid(r)) => Ok(l.cmp(r)),
            _ => Err(Error::Schema(
                "Type mismatch: expected uuid values".to_string(),
            )),
        }
    }

    fn compare_varint(&self, left: &Value, right: &Value) -> Result<Ordering> {
        match (left, right) {
            (Value::Varint(l), Value::Varint(r)) => Ok(l.cmp(r)),
            _ => Err(Error::Schema(
                "Type mismatch: expected varint values".to_string(),
            )),
        }
    }

    fn compare_decimal(&self, left: &Value, right: &Value) -> Result<Ordering> {
        match (left, right) {
            (
                Value::Decimal {
                    scale: l_scale,
                    unscaled: l_unscaled,
                },
                Value::Decimal {
                    scale: r_scale,
                    unscaled: r_unscaled,
                },
            ) => {
                // Compare by normalizing to same scale if needed
                if l_scale == r_scale {
                    Ok(l_unscaled.cmp(r_unscaled))
                } else {
                    // For now, simple string comparison
                    // A full implementation would normalize scales
                    let l_str = format!("{:?}.{}", l_unscaled, l_scale);
                    let r_str = format!("{:?}.{}", r_unscaled, r_scale);
                    Ok(l_str.cmp(&r_str))
                }
            }
            _ => Err(Error::Schema(
                "Type mismatch: expected decimal values".to_string(),
            )),
        }
    }

    fn compare_duration(&self, left: &Value, right: &Value) -> Result<Ordering> {
        match (left, right) {
            (
                Value::Duration {
                    months: l_months,
                    days: l_days,
                    nanos: l_nanos,
                },
                Value::Duration {
                    months: r_months,
                    days: r_days,
                    nanos: r_nanos,
                },
            ) => {
                // Compare months first
                match l_months.cmp(r_months) {
                    Ordering::Equal => {
                        // Compare days next
                        match l_days.cmp(r_days) {
                            Ordering::Equal => {
                                // Compare nanoseconds last
                                Ok(l_nanos.cmp(r_nanos))
                            }
                            other => Ok(other),
                        }
                    }
                    other => Ok(other),
                }
            }
            _ => Err(Error::Schema(
                "Type mismatch: expected duration values".to_string(),
            )),
        }
    }

    fn compare_list(
        &self,
        left: &Value,
        right: &Value,
        element_comparator: &ComparatorType,
    ) -> Result<Ordering> {
        match (left, right) {
            (Value::List(l), Value::List(r)) => self.compare_list_values(l, r, element_comparator),
            _ => Err(Error::Schema(
                "Type mismatch: expected list values".to_string(),
            )),
        }
    }

    fn compare_list_values(
        &self,
        left: &[Value],
        right: &[Value],
        element_comparator: &ComparatorType,
    ) -> Result<Ordering> {
        let min_len = left.len().min(right.len());

        // Compare elements pairwise
        for i in 0..min_len {
            let cmp = element_comparator.compare(&left[i], &right[i])?;
            if cmp != Ordering::Equal {
                return Ok(cmp);
            }
        }

        // If all compared elements are equal, compare lengths
        Ok(left.len().cmp(&right.len()))
    }

    fn compare_set(
        &self,
        left: &Value,
        right: &Value,
        _element_comparator: &ComparatorType,
    ) -> Result<Ordering> {
        match (left, right) {
            (Value::Set(l), Value::Set(r)) => {
                // Sets only support equality comparison
                if l.len() != r.len() {
                    return Ok(Ordering::Greater); // Arbitrary but consistent
                }

                // Check if all elements in left set are in right set
                // This is a simplified implementation; a full implementation would need proper set logic
                for l_val in l {
                    let mut found = false;
                    for r_val in r {
                        if l_val == r_val {
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        return Ok(Ordering::Greater); // Arbitrary but consistent
                    }
                }

                Ok(Ordering::Equal)
            }
            _ => Err(Error::Schema(
                "Type mismatch: expected set values".to_string(),
            )),
        }
    }

    fn compare_map(
        &self,
        left: &Value,
        right: &Value,
        _key_comparator: &ComparatorType,
        _value_comparator: &ComparatorType,
    ) -> Result<Ordering> {
        match (left, right) {
            (Value::Map(l), Value::Map(r)) => {
                // Maps only support equality comparison
                if l.len() != r.len() {
                    return Ok(Ordering::Greater); // Arbitrary but consistent
                }

                // Simple equality check
                Ok(if l == r {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                })
            }
            _ => Err(Error::Schema(
                "Type mismatch: expected map values".to_string(),
            )),
        }
    }

    fn compare_tuple(
        &self,
        left: &Value,
        right: &Value,
        field_comparators: &[ComparatorType],
    ) -> Result<Ordering> {
        match (left, right) {
            (Value::Tuple(l), Value::Tuple(r)) => {
                let min_len = l.len().min(r.len()).min(field_comparators.len());

                // Compare fields pairwise using their respective comparators
                for i in 0..min_len {
                    let cmp = field_comparators[i].compare(&l[i], &r[i])?;
                    if cmp != Ordering::Equal {
                        return Ok(cmp);
                    }
                }

                // If all compared fields are equal, compare tuple lengths
                Ok(l.len().cmp(&r.len()))
            }
            _ => Err(Error::Schema(
                "Type mismatch: expected tuple values".to_string(),
            )),
        }
    }

    fn compare_udt(
        &self,
        left: &Value,
        right: &Value,
        field_comparators: &[(String, ComparatorType)],
    ) -> Result<Ordering> {
        match (left, right) {
            (Value::Udt(l), Value::Udt(r)) => {
                // Compare UDT type names first
                if l.type_name != r.type_name {
                    return Ok(l.type_name.cmp(&r.type_name));
                }

                // Compare fields in definition order
                for (field_name, field_comparator) in field_comparators {
                    // Find field in UDT fields
                    let l_val = l
                        .fields
                        .iter()
                        .find(|f| f.name == *field_name)
                        .and_then(|f| f.value.as_ref())
                        .unwrap_or(&Value::Null);

                    let r_val = r
                        .fields
                        .iter()
                        .find(|f| f.name == *field_name)
                        .and_then(|f| f.value.as_ref())
                        .unwrap_or(&Value::Null);

                    let cmp = field_comparator.compare(l_val, r_val)?;
                    if cmp != Ordering::Equal {
                        return Ok(cmp);
                    }
                }

                Ok(Ordering::Equal)
            }
            _ => Err(Error::Schema(
                "Type mismatch: expected UDT values".to_string(),
            )),
        }
    }

    fn compare_frozen(
        &self,
        left: &Value,
        right: &Value,
        inner_comparator: &ComparatorType,
    ) -> Result<Ordering> {
        match (left, right) {
            (Value::Frozen(l), Value::Frozen(r)) => {
                // Compare the inner values using the inner comparator
                inner_comparator.compare(l, r)
            }
            _ => Err(Error::Schema(
                "Type mismatch: expected frozen values".to_string(),
            )),
        }
    }
}

impl std::fmt::Display for ComparatorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ComparatorType::List(element_type) => write!(f, "list<{}>", element_type),
            ComparatorType::Set(element_type) => write!(f, "set<{}>", element_type),
            ComparatorType::Map(key_type, value_type) => {
                write!(f, "map<{}, {}>", key_type, value_type)
            }
            ComparatorType::Tuple(field_types) => {
                write!(f, "tuple<")?;
                for (i, field_type) in field_types.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", field_type)?;
                }
                write!(f, ">")
            }
            ComparatorType::Udt {
                type_name,
                keyspace,
                ..
            } => {
                if let Some(ks) = keyspace {
                    write!(f, "{}.{}", ks, type_name)
                } else {
                    write!(f, "{}", type_name)
                }
            }
            ComparatorType::Frozen(inner_type) => write!(f, "frozen<{}>", inner_type),
            _ => write!(f, "{}", self.type_name()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_primitive_comparisons() {
        let int_comparator = ComparatorType::Int;

        // Test integer comparison
        let left = Value::Integer(10);
        let right = Value::Integer(20);
        assert_eq!(
            int_comparator.compare(&left, &right).unwrap(),
            Ordering::Less
        );
        assert_eq!(
            int_comparator.compare(&right, &left).unwrap(),
            Ordering::Greater
        );
        assert_eq!(
            int_comparator.compare(&left, &left).unwrap(),
            Ordering::Equal
        );

        // Test text comparison
        let text_comparator = ComparatorType::Text;
        let left_text = Value::text("apple".to_string());
        let right_text = Value::text("banana".to_string());
        assert_eq!(
            text_comparator.compare(&left_text, &right_text).unwrap(),
            Ordering::Less
        );
    }

    #[test]
    fn test_null_comparisons() {
        let int_comparator = ComparatorType::Int;
        let null_val = Value::Null;
        let int_val = Value::Integer(42);

        assert_eq!(
            int_comparator.compare(&null_val, &null_val).unwrap(),
            Ordering::Equal
        );
        assert_eq!(
            int_comparator.compare(&null_val, &int_val).unwrap(),
            Ordering::Less
        );
        assert_eq!(
            int_comparator.compare(&int_val, &null_val).unwrap(),
            Ordering::Greater
        );
    }

    #[test]
    fn test_supports_ordering() {
        assert!(ComparatorType::Int.supports_ordering());
        assert!(ComparatorType::Text.supports_ordering());
        assert!(!ComparatorType::Set(Box::new(ComparatorType::Int)).supports_ordering());
        assert!(!ComparatorType::Map(
            Box::new(ComparatorType::Text),
            Box::new(ComparatorType::Int)
        )
        .supports_ordering());
    }
}
