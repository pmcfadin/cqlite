//! CQL type serializers
//!
//! Provides byte-correct encoding for all CQL types:
//! - Primitives: boolean, int, bigint, float, double, timestamp, uuid, timeuuid
//! - Text: text (UTF-8), blob
//! - Numeric: varint, decimal
//! - Temporal: date, time, duration
//! - Collections: list, set, map, tuple
//! - UDT: user-defined types (DANGEROUS - 4-byte prefixes)
//!
//! Complexity ranking (from M5 Council Recommendation):
//! - Trivial: boolean, int, bigint, float, double, timestamp, uuid (1-2 days)
//! - Moderate: text, blob, inet, date, time (2-3 days)
//! - Complex: varint, decimal, duration, list, set, map, tuple (4-5 days)
//! - Dangerous: UDT (schema-ordered, 4-byte prefixes, 3-4 days)

// Note: write-support feature gate is applied at module level in mod.rs

use crate::error::{Error, Result};
use crate::schema::CqlType;
use crate::storage::serialization::vint;
use crate::types::{UdtTypeDef, Value};

/// CQL type serializer for write operations
///
/// Serializes Value instances to byte arrays using Cassandra's binary format.
/// All serialization methods follow the exact encoding documented in
/// `docs/sstables-definitive-guide/chapters/05-data-db-format.md`.
///
/// # Critical UDT Encoding Rules (Issue #385)
///
/// - Field lengths are 4-byte big-endian i32, **NOT VInt**!
/// - Fields MUST be in schema definition order, **NOT alphabetical**!
/// - NULL fields are encoded as -1 (0xFFFFFFFF as i32)
///
/// # Examples
///
/// ```
/// # use cqlite_core::storage::serialization::types::TypeSerializer;
/// # use cqlite_core::types::Value;
/// let serializer = TypeSerializer::new();
///
/// // Simple integer
/// let bytes = serializer.serialize_value(&Value::Integer(42), "int").unwrap();
/// assert_eq!(bytes, vec![0x00, 0x00, 0x00, 0x2A]);
///
/// // Text (no length prefix for cell values)
/// let bytes = serializer.serialize_value(&Value::text("hello".to_string()), "text").unwrap();
/// assert_eq!(bytes, b"hello");
/// ```
#[derive(Debug, Clone, Default)]
pub struct TypeSerializer {
    // Stateless - no fields needed
}

impl TypeSerializer {
    /// Create a new type serializer
    pub fn new() -> Self {
        Self {}
    }

    /// Main entry point: serialize a Value to bytes
    ///
    /// # Arguments
    ///
    /// * `value` - The value to serialize
    /// * `data_type` - CQL type string (e.g., "int", "text", "list<int>")
    ///
    /// # Returns
    ///
    /// Byte representation suitable for SSTable cell values (no length prefix for top-level value)
    pub fn serialize_value(&self, value: &Value, data_type: &str) -> Result<Vec<u8>> {
        match value {
            Value::Null => Ok(Vec::new()), // NULL cells have no data
            // EMPTY-BUFFER SENTINEL (issue #3805): REFUSED HERE (roborev job
            // 452). This is the GENERAL CELL-VALUE API — its return is
            // documented as "suitable for SSTable cell values" — and a declared
            // TYPE is not what the sentinel needs. It needs a FRAMING context in
            // which a ZERO-LENGTH buffer is both expressible and MEANS "empty",
            // and no position this serializer writes is one:
            //
            //  * as a regular CELL VALUE, zero bytes plus `HAS_EMPTY_VALUE_MASK`
            //    (`db/rows/Cell.java:264` at `cassandra-5.0.8`) read back as
            //    `Value::Null`, so the value would silently CHANGE TYPE across
            //    the round trip;
            //  * inside a length-prefixed COLLECTION element, TUPLE field or UDT
            //    field, a zero-length component is the EMPTY VALUE of that
            //    component's own declared type — which nothing in this crate
            //    reads back as the sentinel.
            //
            // Knowing the declared type says only that an empty buffer would be
            // LEGAL for that type; it does not say that this position means an
            // empty collection COMPONENT. That is the whole gap: this writer has
            // the type and not the framing context. The write positions where
            // both are present — the length carried by the enclosing framing (an
            // unsigned VInt, `db/marshal/CollectionType.java:361-382`) and the
            // declared component type available to validate the tag — are a
            // MULTICELL collection's CELL PATH (a map's KEY, #3805; a set's
            // ELEMENT, #4106), which have their own schema-aware entry points in
            // `storage::sstable::writer::data_writer::cell_path`. Refusing beats
            // writing bytes that read back as something else (#28).
            Value::Empty(tag) => Err(refuse_empty_sentinel_cell_value(*tag)),
            _ => {
                let cql_type = CqlType::parse(data_type)?;
                self.serialize_typed_value(value, &cql_type)
            }
        }
    }

    /// Serialize a value with a parsed CqlType
    fn serialize_typed_value(&self, value: &Value, cql_type: &CqlType) -> Result<Vec<u8>> {
        // The sentinel is refused at EVERY position this serializer writes, not
        // only the top-level cell value: this function is also the RECURSION
        // POINT for collection elements, tuple fields and UDT fields. Those
        // nested positions already refused, but by falling through to a
        // per-type "Cannot serialize Empty(int) as Int" mismatch that names
        // neither the reason nor the one legal position — see
        // [`refuse_empty_sentinel_cell_value`], which is the single wording both
        // arms return.
        if let Value::Empty(tag) = value {
            return Err(refuse_empty_sentinel_cell_value(*tag));
        }
        match cql_type {
            // Primitive types
            CqlType::Boolean
            | CqlType::TinyInt
            | CqlType::SmallInt
            | CqlType::Int
            | CqlType::BigInt
            | CqlType::Counter
            | CqlType::Float
            | CqlType::Double
            | CqlType::Uuid
            | CqlType::TimeUuid => self.serialize_primitive(value, cql_type),

            // Text types
            CqlType::Text | CqlType::Ascii | CqlType::Varchar => self.serialize_text(value),

            // Binary types
            CqlType::Blob => self.serialize_blob(value),

            // Temporal types
            CqlType::Timestamp | CqlType::Date | CqlType::Time | CqlType::Duration => {
                self.serialize_temporal(value, cql_type)
            }

            // Numeric types
            CqlType::Varint | CqlType::Decimal => self.serialize_numeric(value, cql_type),

            // Network types
            CqlType::Inet => self.serialize_inet(value),

            // Collection types
            CqlType::List(elem_type) => self.serialize_list(value, elem_type),
            CqlType::Set(elem_type) => self.serialize_set(value, elem_type),
            CqlType::Map(key_type, val_type) => self.serialize_map(value, key_type, val_type),
            CqlType::Tuple(field_types) => self.serialize_tuple(value, field_types),

            // UDT - build schema from value and serialize
            CqlType::Udt(_, _) => {
                // Unwrap Frozen wrapper(s) to get the raw UDT value
                let mut unwrapped = value;
                while let Value::Frozen(inner) = unwrapped {
                    unwrapped = inner.as_ref();
                }
                let udt_value = match unwrapped {
                    Value::Udt(u) => u,
                    _ => {
                        return Err(Error::type_conversion(format!(
                            "Expected UDT value for UDT type, got {:?}",
                            value
                        )))
                    }
                };
                let mut schema =
                    UdtTypeDef::new(udt_value.keyspace.clone(), udt_value.type_name.clone());
                for field in &udt_value.fields {
                    let field_type = Self::infer_cql_type(field.value.as_ref());
                    schema = schema.with_field(field.name.clone(), field_type, true);
                }
                self.serialize_udt(unwrapped, &schema)
            }

            // Frozen wrapper
            CqlType::Frozen(inner_type) => self.serialize_typed_value(value, inner_type),

            // Custom types not supported
            CqlType::Custom(name) => Err(Error::unsupported_format(format!(
                "Custom type not supported: {}",
                name
            ))),
        }
    }

    /// Serialize primitive types (trivial - fixed size)
    fn serialize_primitive(&self, value: &Value, cql_type: &CqlType) -> Result<Vec<u8>> {
        match (value, cql_type) {
            // Boolean: 1 byte (0x00 or 0x01)
            (Value::Boolean(b), CqlType::Boolean) => Ok(vec![if *b { 0x01 } else { 0x00 }]),

            // TinyInt: 1 byte signed
            (Value::TinyInt(n), CqlType::TinyInt) => Ok(vec![*n as u8]),

            // SmallInt: 2 bytes big-endian signed
            (Value::SmallInt(n), CqlType::SmallInt) => Ok(n.to_be_bytes().to_vec()),

            // Int: 4 bytes big-endian signed
            (Value::Integer(n), CqlType::Int) => Ok(n.to_be_bytes().to_vec()),

            // BigInt/Counter: 8 bytes big-endian signed
            (Value::BigInt(n), CqlType::BigInt | CqlType::Counter) => Ok(n.to_be_bytes().to_vec()),
            (Value::Counter(n), CqlType::Counter | CqlType::BigInt) => Ok(n.to_be_bytes().to_vec()),

            // Float: 4 bytes IEEE 754 big-endian
            (Value::Float32(f), CqlType::Float) => Ok(f.to_be_bytes().to_vec()),

            // Double: 8 bytes IEEE 754 big-endian
            (Value::Float(f), CqlType::Double) => Ok(f.to_be_bytes().to_vec()),

            // UUID/TimeUuid: 16 bytes raw
            (Value::Uuid(uuid), CqlType::Uuid | CqlType::TimeUuid) => Ok(uuid.to_vec()),

            // Type mismatch
            _ => Err(Error::type_conversion(format!(
                "Cannot serialize {:?} as {:?}",
                value, cql_type
            ))),
        }
    }

    /// Serialize text types (no length prefix for cell values)
    fn serialize_text(&self, value: &Value) -> Result<Vec<u8>> {
        match value {
            Value::Text(s) => Ok(s.to_vec()),
            _ => Err(Error::type_conversion(format!(
                "Expected Text value, got {:?}",
                value
            ))),
        }
    }

    /// Serialize blob (no length prefix for cell values)
    fn serialize_blob(&self, value: &Value) -> Result<Vec<u8>> {
        match value {
            Value::Blob(bytes) => Ok(bytes.to_vec()),
            _ => Err(Error::type_conversion(format!(
                "Expected Blob value, got {:?}",
                value
            ))),
        }
    }

    /// Serialize temporal types
    fn serialize_temporal(&self, value: &Value, cql_type: &CqlType) -> Result<Vec<u8>> {
        match (value, cql_type) {
            // Timestamp: 8 bytes big-endian (millis since epoch)
            (Value::Timestamp(ts), CqlType::Timestamp) => Ok(ts.to_be_bytes().to_vec()),

            // Date: 4 bytes unsigned int with Integer.MIN_VALUE offset
            // Cassandra stores as unsigned days from epoch (1970-01-01) offset by Integer.MIN_VALUE
            (Value::Date(days), CqlType::Date) => {
                let encoded = (*days as u32).wrapping_add(0x80000000); // Add Integer.MIN_VALUE
                Ok(encoded.to_be_bytes().to_vec())
            }

            // Time: 8 bytes big-endian (nanoseconds since midnight)
            (Value::Time(nanos), CqlType::Time) => Ok(nanos.to_be_bytes().to_vec()),

            // Duration: VInt months + VInt days + VInt nanoseconds
            (
                Value::Duration {
                    months,
                    days,
                    nanos,
                },
                CqlType::Duration,
            ) => {
                let mut buf = Vec::new();
                vint::encode_signed(*months as i64, &mut buf);
                vint::encode_signed(*days as i64, &mut buf);
                vint::encode_signed(*nanos, &mut buf);
                Ok(buf)
            }

            _ => Err(Error::type_conversion(format!(
                "Cannot serialize {:?} as {:?}",
                value, cql_type
            ))),
        }
    }

    /// Serialize numeric types (complex)
    fn serialize_numeric(&self, value: &Value, cql_type: &CqlType) -> Result<Vec<u8>> {
        match (value, cql_type) {
            // Varint: minimal two's complement big-endian
            (Value::Varint(bytes), CqlType::Varint) => Ok(bytes.to_vec()),

            // Decimal: 4-byte BE scale + varint unscaled value
            (Value::Decimal { scale, unscaled }, CqlType::Decimal) => {
                let mut buf = scale.to_be_bytes().to_vec();
                buf.extend_from_slice(unscaled);
                Ok(buf)
            }

            _ => Err(Error::type_conversion(format!(
                "Cannot serialize {:?} as {:?}",
                value, cql_type
            ))),
        }
    }

    /// Serialize inet address
    fn serialize_inet(&self, value: &Value) -> Result<Vec<u8>> {
        match value {
            Value::Inet(bytes) => Ok(bytes.to_vec()),
            _ => Err(Error::type_conversion(format!(
                "Expected Inet value, got {:?}",
                value
            ))),
        }
    }

    /// Serialize list collection
    ///
    /// Format: [4-byte count][elements...]
    /// Each element: [4-byte length][bytes]
    fn serialize_list(&self, value: &Value, elem_type: &CqlType) -> Result<Vec<u8>> {
        // Unwrap Frozen wrapper(s) to get the raw List value
        let mut unwrapped = value;
        while let Value::Frozen(inner) = unwrapped {
            unwrapped = inner.as_ref();
        }
        match unwrapped {
            Value::List(elements) => self.serialize_collection_elements(elements, elem_type),
            _ => Err(Error::type_conversion(format!(
                "Expected List value, got {:?}",
                value
            ))),
        }
    }

    /// Serialize set collection
    ///
    /// Format: [4-byte count][elements...]
    /// Each element: [4-byte length][bytes]
    fn serialize_set(&self, value: &Value, elem_type: &CqlType) -> Result<Vec<u8>> {
        // Unwrap Frozen wrapper(s) to get the raw Set value
        let mut unwrapped = value;
        while let Value::Frozen(inner) = unwrapped {
            unwrapped = inner.as_ref();
        }
        match unwrapped {
            Value::Set(elements) => self.serialize_collection_elements(elements, elem_type),
            _ => Err(Error::type_conversion(format!(
                "Expected Set value, got {:?}",
                value
            ))),
        }
    }

    /// Serialize map collection
    ///
    /// Format: [4-byte count][key-value pairs...]
    /// Each pair: [4-byte key_length][key_bytes][4-byte val_length][val_bytes]
    fn serialize_map(
        &self,
        value: &Value,
        key_type: &CqlType,
        val_type: &CqlType,
    ) -> Result<Vec<u8>> {
        // Unwrap Frozen wrapper(s) to get the raw Map value
        let mut unwrapped = value;
        while let Value::Frozen(inner) = unwrapped {
            unwrapped = inner.as_ref();
        }
        match unwrapped {
            Value::Map(pairs) => {
                let mut buf = Vec::new();

                // Write count as 4-byte big-endian
                buf.extend_from_slice(&(pairs.len() as i32).to_be_bytes());

                // Write each key-value pair
                for (key, val) in pairs {
                    // Serialize key
                    let key_bytes = self.serialize_typed_value(key, key_type)?;
                    buf.extend_from_slice(&(key_bytes.len() as i32).to_be_bytes());
                    buf.extend_from_slice(&key_bytes);

                    // Serialize value
                    let val_bytes = self.serialize_typed_value(val, val_type)?;
                    buf.extend_from_slice(&(val_bytes.len() as i32).to_be_bytes());
                    buf.extend_from_slice(&val_bytes);
                }

                Ok(buf)
            }
            _ => Err(Error::type_conversion(format!(
                "Expected Map value, got {:?}",
                value
            ))),
        }
    }

    /// Serialize tuple
    ///
    /// Format: [elements...]
    /// Each element: [4-byte length][bytes] (length is -1 for NULL)
    fn serialize_tuple(&self, value: &Value, field_types: &[CqlType]) -> Result<Vec<u8>> {
        // Unwrap Frozen wrapper(s) to get the raw Tuple value
        let mut unwrapped = value;
        while let Value::Frozen(inner) = unwrapped {
            unwrapped = inner.as_ref();
        }
        match unwrapped {
            Value::Tuple(fields) => {
                if fields.len() != field_types.len() {
                    return Err(Error::type_conversion(format!(
                        "Tuple field count mismatch: expected {}, got {}",
                        field_types.len(),
                        fields.len()
                    )));
                }

                let mut buf = Vec::new();

                for (field_val, field_type) in fields.iter().zip(field_types.iter()) {
                    if field_val.is_null() {
                        // NULL field: -1 as 4-byte signed
                        buf.extend_from_slice(&(-1i32).to_be_bytes());
                    } else {
                        let field_bytes = self.serialize_typed_value(field_val, field_type)?;
                        buf.extend_from_slice(&(field_bytes.len() as i32).to_be_bytes());
                        buf.extend_from_slice(&field_bytes);
                    }
                }

                Ok(buf)
            }
            _ => Err(Error::type_conversion(format!(
                "Expected Tuple value, got {:?}",
                value
            ))),
        }
    }

    /// Helper: serialize collection elements with 4-byte count and lengths
    fn serialize_collection_elements(
        &self,
        elements: &[Value],
        elem_type: &CqlType,
    ) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        // Write count as 4-byte big-endian
        buf.extend_from_slice(&(elements.len() as i32).to_be_bytes());

        // Write each element with 4-byte length prefix
        for elem in elements {
            let elem_bytes = self.serialize_typed_value(elem, elem_type)?;
            buf.extend_from_slice(&(elem_bytes.len() as i32).to_be_bytes());
            buf.extend_from_slice(&elem_bytes);
        }

        Ok(buf)
    }

    /// Infer CqlType from a Value (used for nested UDT field type inference).
    ///
    /// Empty collections still fall back to `text` because there is no element
    /// value available to inspect.
    fn infer_cql_type(value: Option<&Value>) -> CqlType {
        match value {
            None | Some(Value::Null) => CqlType::Text,
            // The sentinel CARRIES its declared type, so inference is exact
            // here rather than a `text` fallback (issue #3805).
            Some(Value::Empty(ty)) => ty.cql_type(),
            Some(Value::Boolean(_)) => CqlType::Boolean,
            Some(Value::TinyInt(_)) => CqlType::TinyInt,
            Some(Value::SmallInt(_)) => CqlType::SmallInt,
            Some(Value::Integer(_)) => CqlType::Int,
            Some(Value::BigInt(_)) => CqlType::BigInt,
            Some(Value::Float32(_)) => CqlType::Float,
            Some(Value::Float(_)) => CqlType::Double,
            Some(Value::Text(_)) => CqlType::Text,
            Some(Value::Blob(_)) => CqlType::Blob,
            Some(Value::Timestamp(_)) => CqlType::Timestamp,
            Some(Value::Date(_)) => CqlType::Date,
            Some(Value::Time(_)) => CqlType::Time,
            Some(Value::Uuid(_)) => CqlType::Uuid,
            Some(Value::Inet(_)) => CqlType::Inet,
            Some(Value::Varint(_)) => CqlType::Varint,
            Some(Value::Decimal { .. }) => CqlType::Decimal,
            Some(Value::Duration { .. }) => CqlType::Duration,
            Some(Value::Counter(_)) => CqlType::Counter,
            Some(Value::List(elements)) => CqlType::List(Box::new(
                elements
                    .first()
                    .map(|elem| Self::infer_cql_type(Some(elem)))
                    .unwrap_or(CqlType::Text),
            )),
            Some(Value::Set(elements)) => CqlType::Set(Box::new(
                elements
                    .first()
                    .map(|elem| Self::infer_cql_type(Some(elem)))
                    .unwrap_or(CqlType::Text),
            )),
            Some(Value::Map(entries)) => {
                let (key_type, value_type) = entries
                    .first()
                    .map(|(key, value)| {
                        (
                            Self::infer_cql_type(Some(key)),
                            Self::infer_cql_type(Some(value)),
                        )
                    })
                    .unwrap_or((CqlType::Text, CqlType::Text));
                CqlType::Map(Box::new(key_type), Box::new(value_type))
            }
            Some(Value::Tuple(fields)) => CqlType::Tuple(
                fields
                    .iter()
                    .map(|field| Self::infer_cql_type(Some(field)))
                    .collect(),
            ),
            Some(Value::Udt(udt)) => CqlType::Udt(
                udt.type_name.clone(),
                udt.fields
                    .iter()
                    .map(|field| {
                        (
                            field.name.clone(),
                            Self::infer_cql_type(field.value.as_ref()),
                        )
                    })
                    .collect(),
            ),
            Some(Value::Frozen(inner)) => {
                CqlType::Frozen(Box::new(Self::infer_cql_type(Some(inner))))
            }
            Some(Value::Tombstone(_)) | Some(Value::Json(_)) => CqlType::Text,
        }
    }

    /// Serialize UDT with schema awareness (DANGEROUS)
    ///
    /// # Critical Requirements
    ///
    /// 1. Field lengths are 4-byte big-endian i32, **NOT VInt**!
    /// 2. Fields MUST be in schema definition order, **NOT alphabetical**!
    /// 3. NULL fields are encoded as -1 (0xFFFFFFFF as i32)
    ///
    /// # Arguments
    ///
    /// * `value` - UDT value to serialize
    /// * `schema` - UDT type definition with field order
    ///
    /// # Format
    ///
    /// ```text
    /// [field1_length:4 bytes BE i32][field1_bytes]
    /// [field2_length:4 bytes BE i32][field2_bytes]
    /// ...
    /// ```
    ///
    /// # Examples
    ///
    /// ```
    /// # use cqlite_core::storage::serialization::types::TypeSerializer;
    /// # use cqlite_core::types::{UdtValue, UdtTypeDef, Value};
    /// # use cqlite_core::schema::CqlType;
    /// let serializer = TypeSerializer::new();
    ///
    /// // Define schema
    /// let schema = UdtTypeDef::new("ks".to_string(), "address".to_string())
    ///     .with_field("street".to_string(), CqlType::Text, true)
    ///     .with_field("city".to_string(), CqlType::Text, true);
    ///
    /// // Create UDT value
    /// let udt = UdtValue::new("address".to_string(), "ks".to_string())
    ///     .with_field("street".to_string(), Some(Value::text("Main St".to_string())))
    ///     .with_field("city".to_string(), Some(Value::text("NYC".to_string())));
    ///
    /// let bytes = serializer.serialize_udt(&Value::Udt(Box::new(udt)), &schema).unwrap();
    /// ```
    pub fn serialize_udt(&self, value: &Value, schema: &UdtTypeDef) -> Result<Vec<u8>> {
        let udt = match value {
            Value::Udt(udt) => udt,
            _ => {
                return Err(Error::type_conversion(format!(
                    "Expected UDT value, got {:?}",
                    value
                )))
            }
        };

        // Validate type name matches
        if udt.type_name != schema.name {
            return Err(Error::type_conversion(format!(
                "UDT type mismatch: expected '{}', got '{}'",
                schema.name, udt.type_name
            )));
        }

        let mut buf = Vec::new();

        // CRITICAL: Process fields in schema definition order
        for field_def in &schema.fields {
            // Find field value by name
            let field_value = udt.get_field(&field_def.name);

            match field_value {
                Some(val) if !val.is_null() => {
                    // Serialize field value
                    let field_bytes = self.serialize_typed_value(val, &field_def.field_type)?;

                    // CRITICAL: 4-byte BE i32 length, NOT VInt!
                    buf.extend_from_slice(&(field_bytes.len() as i32).to_be_bytes());
                    buf.extend_from_slice(&field_bytes);
                }
                _ => {
                    // NULL field: -1 (0xFFFFFFFF)
                    buf.extend_from_slice(&(-1i32).to_be_bytes());
                }
            }
        }

        Ok(buf)
    }
}

/// The ONE refusal [`TypeSerializer`] returns for the empty-buffer sentinel
/// (issue #3805, roborev job 452), shared by the top-level cell-value entry
/// point and the nested recursion point so the two cannot drift into two
/// different explanations of one rule.
///
/// # Why this writer refuses even though it KNOWS the declared type
/// The sentinel needs two things to be serializable, and this writer supplies
/// only the first: a declared type that ADMITS an empty buffer, and a FRAMING
/// context in which a zero-length buffer both is expressible and MEANS "empty".
/// Every position reachable from here fails the second — a cell value's zero
/// bytes read back as `null` (`db/rows/Cell.java:264` at `cassandra-5.0.8`), and
/// a length-prefixed collection/tuple/UDT component's zero length is that
/// component's own empty value. The single position that supplies both is a
/// MULTICELL collection's CELL PATH, whose schema-aware entry points are
/// [`crate::storage::sstable::writer::data_writer::cell_path`]'s
/// `serialize_map_cell_path_key_into` (a map's KEY, #3805) and
/// `serialize_set_cell_path_element_into` (a set's ELEMENT, #4106) — the ONLY
/// two licensed to admit it, pinned BY NAME by that census.
fn refuse_empty_sentinel_cell_value(tag: crate::types::EmptyValueType) -> Error {
    Error::InvalidInput(format!(
        "an empty-buffer sentinel (`{}`, issue #3805) has no cell-value \
         serialization: a declared type says only that an empty buffer would be \
         LEGAL for it, never that this position means an empty collection \
         COMPONENT — as a cell value zero bytes read back as `null` \
         (`db/rows/Cell.java:264`), and inside a length-prefixed \
         collection/tuple/UDT component they read back as that component's own \
         empty value. It is legal ONLY on a multicell collection's cell path, via \
         `data_writer::cell_path`'s `serialize_map_cell_path_key_into` or \
         `serialize_set_cell_path_element_into`, where the length is carried by the \
         enclosing framing and the declared component type validates the tag (#28)",
        tag.cql_name()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::UdtValue;

    /// Helper to format bytes as hex string
    fn hex(bytes: &[u8]) -> String {
        bytes
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<Vec<_>>()
            .join(" ")
    }

    #[test]
    fn test_serialize_primitive_boolean() {
        let ser = TypeSerializer::new();

        let bytes = ser
            .serialize_value(&Value::Boolean(true), "boolean")
            .unwrap();
        assert_eq!(bytes, vec![0x01]);

        let bytes = ser
            .serialize_value(&Value::Boolean(false), "boolean")
            .unwrap();
        assert_eq!(bytes, vec![0x00]);
    }

    #[test]
    fn test_serialize_primitive_integers() {
        let ser = TypeSerializer::new();

        // TinyInt
        let bytes = ser.serialize_value(&Value::TinyInt(42), "tinyint").unwrap();
        assert_eq!(bytes, vec![42]);

        let bytes = ser.serialize_value(&Value::TinyInt(-1), "tinyint").unwrap();
        assert_eq!(bytes, vec![0xFF]);

        // SmallInt
        let bytes = ser
            .serialize_value(&Value::SmallInt(1000), "smallint")
            .unwrap();
        assert_eq!(bytes, vec![0x03, 0xE8]);

        // Int
        let bytes = ser.serialize_value(&Value::Integer(42), "int").unwrap();
        assert_eq!(bytes, vec![0x00, 0x00, 0x00, 0x2A]);

        let bytes = ser.serialize_value(&Value::Integer(-1), "int").unwrap();
        assert_eq!(bytes, vec![0xFF, 0xFF, 0xFF, 0xFF]);

        // BigInt
        let bytes = ser
            .serialize_value(&Value::BigInt(0x0102030405060708), "bigint")
            .unwrap();
        assert_eq!(bytes, vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
    }

    #[test]
    fn test_serialize_primitive_floats() {
        let ser = TypeSerializer::new();

        // Float32 - use a value that's not PI to avoid clippy::approx_constant
        let bytes = ser.serialize_value(&Value::Float32(1.5), "float").unwrap();
        assert_eq!(bytes, 1.5f32.to_be_bytes().to_vec());

        // Double - use a value that's not E to avoid clippy::approx_constant
        let bytes = ser.serialize_value(&Value::Float(1.234), "double").unwrap();
        assert_eq!(bytes, 1.234f64.to_be_bytes().to_vec());
    }

    #[test]
    fn test_serialize_text() {
        let ser = TypeSerializer::new();

        let bytes = ser
            .serialize_value(&Value::text("hello".to_string()), "text")
            .unwrap();
        assert_eq!(bytes, b"hello");

        // UTF-8
        let bytes = ser
            .serialize_value(&Value::text("日本語".to_string()), "text")
            .unwrap();
        assert_eq!(bytes, "日本語".as_bytes());
    }

    #[test]
    fn test_serialize_blob() {
        let ser = TypeSerializer::new();

        let bytes = ser
            .serialize_value(&Value::blob(vec![0x01, 0x02, 0x03]), "blob")
            .unwrap();
        assert_eq!(bytes, vec![0x01, 0x02, 0x03]);
    }

    #[test]
    fn test_serialize_uuid() {
        let ser = TypeSerializer::new();

        let uuid_bytes = [
            0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC,
            0xDE, 0xF0,
        ];

        let bytes = ser
            .serialize_value(&Value::Uuid(uuid_bytes), "uuid")
            .unwrap();
        assert_eq!(bytes, uuid_bytes.to_vec());
    }

    #[test]
    fn test_serialize_temporal_timestamp() {
        let ser = TypeSerializer::new();

        let bytes = ser
            .serialize_value(&Value::Timestamp(1640000000000), "timestamp")
            .unwrap();
        assert_eq!(bytes, 1640000000000i64.to_be_bytes().to_vec());
    }

    #[test]
    fn test_serialize_temporal_date() {
        let ser = TypeSerializer::new();

        // Date 0 (1970-01-01) encodes as Integer.MIN_VALUE + 0 = 0x80000000
        let bytes = ser.serialize_value(&Value::Date(0), "date").unwrap();
        assert_eq!(bytes, vec![0x80, 0x00, 0x00, 0x00]);

        // Date -1 encodes as Integer.MIN_VALUE - 1 = 0x7FFFFFFF
        let bytes = ser.serialize_value(&Value::Date(-1), "date").unwrap();
        assert_eq!(bytes, vec![0x7F, 0xFF, 0xFF, 0xFF]);
    }

    #[test]
    fn test_serialize_temporal_time() {
        let ser = TypeSerializer::new();

        let nanos = 12_345_678_900_000_000i64; // ~3:25 PM
        let bytes = ser.serialize_value(&Value::Time(nanos), "time").unwrap();
        assert_eq!(bytes, nanos.to_be_bytes().to_vec());
    }

    #[test]
    fn test_serialize_temporal_duration() {
        let ser = TypeSerializer::new();

        let bytes = ser
            .serialize_value(
                &Value::Duration {
                    months: 1,
                    days: 30,
                    nanos: 3_600_000_000_000,
                },
                "duration",
            )
            .unwrap();

        // Decode and verify (VInt encoded)
        let mut expected = Vec::new();
        vint::encode_signed(1, &mut expected);
        vint::encode_signed(30, &mut expected);
        vint::encode_signed(3_600_000_000_000, &mut expected);
        assert_eq!(bytes, expected);
    }

    #[test]
    fn test_serialize_numeric_varint() {
        let ser = TypeSerializer::new();

        let bytes = ser
            .serialize_value(&Value::varint(vec![0x01, 0x02, 0x03]), "varint")
            .unwrap();
        assert_eq!(bytes, vec![0x01, 0x02, 0x03]);
    }

    #[test]
    fn test_serialize_numeric_decimal() {
        let ser = TypeSerializer::new();

        let bytes = ser
            .serialize_value(
                &Value::Decimal {
                    scale: 2,
                    unscaled: vec![0x01, 0x02, 0x03],
                },
                "decimal",
            )
            .unwrap();
        assert_eq!(bytes, vec![0x00, 0x00, 0x00, 0x02, 0x01, 0x02, 0x03]);
    }

    #[test]
    fn test_serialize_inet() {
        let ser = TypeSerializer::new();

        // IPv4
        let bytes = ser
            .serialize_value(&Value::inet(vec![192, 168, 1, 1]), "inet")
            .unwrap();
        assert_eq!(bytes, vec![192, 168, 1, 1]);

        // IPv6
        let ipv6 = vec![
            0x20, 0x01, 0x0d, 0xb8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x01,
        ];
        let bytes = ser
            .serialize_value(&Value::inet(ipv6.clone()), "inet")
            .unwrap();
        assert_eq!(bytes, ipv6);
    }

    #[test]
    fn test_serialize_list() {
        let ser = TypeSerializer::new();

        let list = Value::List(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ]);

        let bytes = ser.serialize_value(&list, "list<int>").unwrap();

        // Expected: [count:4][len1:4][val1:4][len2:4][val2:4][len3:4][val3:4]
        let expected = vec![
            0x00, 0x00, 0x00, 0x03, // count = 3
            0x00, 0x00, 0x00, 0x04, // len1 = 4
            0x00, 0x00, 0x00, 0x01, // val1 = 1
            0x00, 0x00, 0x00, 0x04, // len2 = 4
            0x00, 0x00, 0x00, 0x02, // val2 = 2
            0x00, 0x00, 0x00, 0x04, // len3 = 4
            0x00, 0x00, 0x00, 0x03, // val3 = 3
        ];

        assert_eq!(hex(&bytes), hex(&expected));
    }

    #[test]
    fn test_serialize_set() {
        let ser = TypeSerializer::new();

        let set = Value::Set(vec![
            Value::text("a".to_string()),
            Value::text("b".to_string()),
        ]);

        let bytes = ser.serialize_value(&set, "set<text>").unwrap();

        let expected = vec![
            0x00, 0x00, 0x00, 0x02, // count = 2
            0x00, 0x00, 0x00, 0x01, // len1 = 1
            b'a', // val1
            0x00, 0x00, 0x00, 0x01, // len2 = 1
            b'b', // val2
        ];

        assert_eq!(hex(&bytes), hex(&expected));
    }

    #[test]
    fn test_serialize_map() {
        let ser = TypeSerializer::new();

        let map = Value::Map(vec![
            (Value::text("key1".to_string()), Value::Integer(100)),
            (Value::text("key2".to_string()), Value::Integer(200)),
        ]);

        let bytes = ser.serialize_value(&map, "map<text, int>").unwrap();

        let expected = vec![
            0x00, 0x00, 0x00, 0x02, // count = 2
            // Pair 1
            0x00, 0x00, 0x00, 0x04, // key1 len = 4
            b'k', b'e', b'y', b'1', // key1
            0x00, 0x00, 0x00, 0x04, // val1 len = 4
            0x00, 0x00, 0x00, 0x64, // val1 = 100
            // Pair 2
            0x00, 0x00, 0x00, 0x04, // key2 len = 4
            b'k', b'e', b'y', b'2', // key2
            0x00, 0x00, 0x00, 0x04, // val2 len = 4
            0x00, 0x00, 0x00, 0xC8, // val2 = 200
        ];

        assert_eq!(hex(&bytes), hex(&expected));
    }

    #[test]
    fn test_serialize_tuple() {
        let ser = TypeSerializer::new();

        let tuple = Value::Tuple(vec![
            Value::Integer(42),
            Value::text("hello".to_string()),
            Value::Null,
        ]);

        let bytes = ser
            .serialize_value(&tuple, "tuple<int, text, text>")
            .unwrap();

        let expected = vec![
            0x00, 0x00, 0x00, 0x04, // field1 len = 4
            0x00, 0x00, 0x00, 0x2A, // field1 = 42
            0x00, 0x00, 0x00, 0x05, // field2 len = 5
            b'h', b'e', b'l', b'l', b'o', // field2
            0xFF, 0xFF, 0xFF, 0xFF, // field3 = NULL (-1)
        ];

        assert_eq!(hex(&bytes), hex(&expected));
    }

    #[test]
    fn test_serialize_udt_simple() {
        let ser = TypeSerializer::new();

        // Define schema
        let schema = UdtTypeDef::new("test_ks".to_string(), "address".to_string())
            .with_field("street".to_string(), CqlType::Text, true)
            .with_field("city".to_string(), CqlType::Text, true);

        // Create UDT value (fields in different order than schema)
        let udt = UdtValue::new("address".to_string(), "test_ks".to_string())
            .with_field("city".to_string(), Some(Value::text("NYC".to_string())))
            .with_field(
                "street".to_string(),
                Some(Value::text("Main St".to_string())),
            );

        let bytes = ser
            .serialize_udt(&Value::Udt(Box::new(udt)), &schema)
            .unwrap();

        // CRITICAL: Must be in schema order (street, city), NOT value order (city, street)
        let expected = vec![
            0x00, 0x00, 0x00, 0x07, // street len = 7
            b'M', b'a', b'i', b'n', b' ', b'S', b't', // "Main St"
            0x00, 0x00, 0x00, 0x03, // city len = 3
            b'N', b'Y', b'C', // "NYC"
        ];

        assert_eq!(hex(&bytes), hex(&expected));
    }

    #[test]
    fn test_serialize_udt_with_nulls() {
        let ser = TypeSerializer::new();

        let schema = UdtTypeDef::new("test_ks".to_string(), "person".to_string())
            .with_field("name".to_string(), CqlType::Text, false)
            .with_field("age".to_string(), CqlType::Int, true)
            .with_field("email".to_string(), CqlType::Text, true);

        let udt = UdtValue::new("person".to_string(), "test_ks".to_string())
            .with_field("name".to_string(), Some(Value::text("John".to_string())))
            .with_field("age".to_string(), None) // NULL
            .with_field(
                "email".to_string(),
                Some(Value::text("john@example.com".to_string())),
            );

        let bytes = ser
            .serialize_udt(&Value::Udt(Box::new(udt)), &schema)
            .unwrap();

        let expected = vec![
            0x00, 0x00, 0x00, 0x04, // name len = 4
            b'J', b'o', b'h', b'n', // "John"
            0xFF, 0xFF, 0xFF, 0xFF, // age = NULL (-1)
            0x00, 0x00, 0x00, 0x10, // email len = 16
            b'j', b'o', b'h', b'n', b'@', b'e', b'x', b'a', b'm', b'p', b'l', b'e', b'.', b'c',
            b'o', b'm', // "john@example.com"
        ];

        assert_eq!(hex(&bytes), hex(&expected));
    }

    #[test]
    fn test_serialize_udt_nested() {
        let ser = TypeSerializer::new();

        // Inner UDT schema
        let address_schema = UdtTypeDef::new("test_ks".to_string(), "address".to_string())
            .with_field("street".to_string(), CqlType::Text, true)
            .with_field("city".to_string(), CqlType::Text, true);

        // Outer UDT schema (note: nested UDT serialization requires recursive schema lookup)
        let _person_schema = UdtTypeDef::new("test_ks".to_string(), "person".to_string())
            .with_field("name".to_string(), CqlType::Text, true)
            .with_field(
                "address".to_string(),
                CqlType::Udt("address".to_string(), vec![]),
                true,
            );

        // Create nested UDT
        let address = UdtValue::new("address".to_string(), "test_ks".to_string())
            .with_field(
                "street".to_string(),
                Some(Value::text("Main St".to_string())),
            )
            .with_field("city".to_string(), Some(Value::text("NYC".to_string())));

        let _person = UdtValue::new("person".to_string(), "test_ks".to_string())
            .with_field("name".to_string(), Some(Value::text("John".to_string())))
            .with_field(
                "address".to_string(),
                Some(Value::Udt(Box::new(address.clone()))),
            );

        // Serialize inner UDT first
        let address_bytes = ser
            .serialize_udt(&Value::Udt(Box::new(address)), &address_schema)
            .unwrap();

        // Serialize outer UDT manually
        let mut expected = Vec::new();
        // name field
        expected.extend_from_slice(&4i32.to_be_bytes()); // len = 4
        expected.extend_from_slice(b"John");
        // address field
        expected.extend_from_slice(&(address_bytes.len() as i32).to_be_bytes());
        expected.extend_from_slice(&address_bytes);

        // Note: This test demonstrates the pattern, but actual nested UDT
        // serialization requires recursive schema lookup which isn't
        // implemented in this basic version
    }

    #[test]
    fn test_serialize_udt_field_ordering() {
        let ser = TypeSerializer::new();

        // Schema with specific field order
        let schema = UdtTypeDef::new("test_ks".to_string(), "test_type".to_string())
            .with_field("field_a".to_string(), CqlType::Int, true)
            .with_field("field_b".to_string(), CqlType::Int, true)
            .with_field("field_c".to_string(), CqlType::Int, true);

        // Create UDT with fields in REVERSE order
        let udt = UdtValue::new("test_type".to_string(), "test_ks".to_string())
            .with_field("field_c".to_string(), Some(Value::Integer(3)))
            .with_field("field_b".to_string(), Some(Value::Integer(2)))
            .with_field("field_a".to_string(), Some(Value::Integer(1)));

        let bytes = ser
            .serialize_udt(&Value::Udt(Box::new(udt)), &schema)
            .unwrap();

        // CRITICAL: Must serialize in schema order (a, b, c), not value order (c, b, a)
        let expected = vec![
            0x00, 0x00, 0x00, 0x04, // field_a len = 4
            0x00, 0x00, 0x00, 0x01, // field_a = 1
            0x00, 0x00, 0x00, 0x04, // field_b len = 4
            0x00, 0x00, 0x00, 0x02, // field_b = 2
            0x00, 0x00, 0x00, 0x04, // field_c len = 4
            0x00, 0x00, 0x00, 0x03, // field_c = 3
        ];

        assert_eq!(hex(&bytes), hex(&expected));
    }

    #[test]
    fn test_serialize_udt_with_collection() {
        let ser = TypeSerializer::new();

        let schema = UdtTypeDef::new("test_ks".to_string(), "user".to_string())
            .with_field("name".to_string(), CqlType::Text, true)
            .with_field(
                "tags".to_string(),
                CqlType::List(Box::new(CqlType::Text)),
                true,
            );

        let udt = UdtValue::new("user".to_string(), "test_ks".to_string())
            .with_field("name".to_string(), Some(Value::text("Alice".to_string())))
            .with_field(
                "tags".to_string(),
                Some(Value::List(vec![
                    Value::text("admin".to_string()),
                    Value::text("user".to_string()),
                ])),
            );

        let bytes = ser
            .serialize_udt(&Value::Udt(Box::new(udt)), &schema)
            .unwrap();

        // Serialize list manually for expected
        let mut list_bytes = Vec::new();
        list_bytes.extend_from_slice(&2i32.to_be_bytes()); // count = 2
        list_bytes.extend_from_slice(&5i32.to_be_bytes()); // len1 = 5
        list_bytes.extend_from_slice(b"admin");
        list_bytes.extend_from_slice(&4i32.to_be_bytes()); // len2 = 4
        list_bytes.extend_from_slice(b"user");

        let mut expected = Vec::new();
        expected.extend_from_slice(&5i32.to_be_bytes()); // name len = 5
        expected.extend_from_slice(b"Alice");
        expected.extend_from_slice(&(list_bytes.len() as i32).to_be_bytes()); // tags len
        expected.extend_from_slice(&list_bytes);

        assert_eq!(hex(&bytes), hex(&expected));
    }

    #[test]
    fn test_serialize_udt_with_nested_collection_fields() {
        let ser = TypeSerializer::new();

        let address_schema = UdtTypeDef::new("test_ks".to_string(), "address".to_string())
            .with_field("street".to_string(), CqlType::Text, true)
            .with_field("city".to_string(), CqlType::Text, true);
        let phone_schema = UdtTypeDef::new("test_ks".to_string(), "phone_number".to_string())
            .with_field("label".to_string(), CqlType::Text, true)
            .with_field("number".to_string(), CqlType::Text, true);
        let person_schema = UdtTypeDef::new("test_ks".to_string(), "person".to_string())
            .with_field("name".to_string(), CqlType::Text, true)
            .with_field(
                "phone_numbers".to_string(),
                CqlType::List(Box::new(CqlType::Frozen(Box::new(CqlType::Udt(
                    "phone_number".to_string(),
                    vec![],
                ))))),
                true,
            )
            .with_field(
                "home_address".to_string(),
                CqlType::Frozen(Box::new(CqlType::Udt("address".to_string(), vec![]))),
                true,
            );
        let company_schema = UdtTypeDef::new("test_ks".to_string(), "company".to_string())
            .with_field("name".to_string(), CqlType::Text, true)
            .with_field(
                "employees".to_string(),
                CqlType::List(Box::new(CqlType::Frozen(Box::new(CqlType::Udt(
                    "person".to_string(),
                    vec![],
                ))))),
                true,
            )
            .with_field(
                "departments".to_string(),
                CqlType::Map(
                    Box::new(CqlType::Text),
                    Box::new(CqlType::Frozen(Box::new(CqlType::List(Box::new(
                        CqlType::Frozen(Box::new(CqlType::Udt("person".to_string(), vec![]))),
                    ))))),
                ),
                true,
            );

        let phone = UdtValue::new("phone_number".to_string(), "test_ks".to_string())
            .with_field("label".to_string(), Some(Value::text("mobile".to_string())))
            .with_field(
                "number".to_string(),
                Some(Value::text("+1-555-0101".to_string())),
            );
        let address = UdtValue::new("address".to_string(), "test_ks".to_string())
            .with_field(
                "street".to_string(),
                Some(Value::text("Main St".to_string())),
            )
            .with_field("city".to_string(), Some(Value::text("Seattle".to_string())));
        let person = UdtValue::new("person".to_string(), "test_ks".to_string())
            .with_field("name".to_string(), Some(Value::text("Alice".to_string())))
            .with_field(
                "phone_numbers".to_string(),
                Some(Value::List(vec![Value::Frozen(Box::new(Value::Udt(
                    Box::new(phone.clone()),
                )))])),
            )
            .with_field(
                "home_address".to_string(),
                Some(Value::Frozen(Box::new(Value::Udt(Box::new(
                    address.clone(),
                ))))),
            );
        let company = UdtValue::new("company".to_string(), "test_ks".to_string())
            .with_field("name".to_string(), Some(Value::text("Acme".to_string())))
            .with_field(
                "employees".to_string(),
                Some(Value::List(vec![Value::Frozen(Box::new(Value::Udt(
                    Box::new(person.clone()),
                )))])),
            )
            .with_field(
                "departments".to_string(),
                Some(Value::Map(vec![(
                    Value::text("platform".to_string()),
                    Value::Frozen(Box::new(Value::List(vec![Value::Frozen(Box::new(
                        Value::Udt(Box::new(person.clone())),
                    ))]))),
                )])),
            );

        let bytes = ser
            .serialize_udt(&Value::Udt(Box::new(company.clone())), &company_schema)
            .unwrap();

        let person_bytes = ser
            .serialize_udt(&Value::Udt(Box::new(person.clone())), &person_schema)
            .unwrap();
        let employees_bytes = ser
            .serialize_typed_value(
                &Value::List(vec![Value::Frozen(Box::new(Value::Udt(Box::new(
                    person.clone(),
                ))))]),
                &CqlType::List(Box::new(CqlType::Frozen(Box::new(CqlType::Udt(
                    "person".to_string(),
                    vec![],
                ))))),
            )
            .unwrap();
        let departments_bytes = ser
            .serialize_typed_value(
                &Value::Map(vec![(
                    Value::text("platform".to_string()),
                    Value::Frozen(Box::new(Value::List(vec![Value::Frozen(Box::new(
                        Value::Udt(Box::new(person)),
                    ))]))),
                )]),
                &CqlType::Map(
                    Box::new(CqlType::Text),
                    Box::new(CqlType::Frozen(Box::new(CqlType::List(Box::new(
                        CqlType::Frozen(Box::new(CqlType::Udt("person".to_string(), vec![]))),
                    ))))),
                ),
            )
            .unwrap();

        let mut expected = Vec::new();
        expected.extend_from_slice(&4i32.to_be_bytes());
        expected.extend_from_slice(b"Acme");
        expected.extend_from_slice(&(employees_bytes.len() as i32).to_be_bytes());
        expected.extend_from_slice(&employees_bytes);
        expected.extend_from_slice(&(departments_bytes.len() as i32).to_be_bytes());
        expected.extend_from_slice(&departments_bytes);

        assert_eq!(hex(&bytes), hex(&expected));
        assert!(!person_bytes.is_empty());
        assert!(!ser
            .serialize_udt(&Value::Udt(Box::new(address)), &address_schema)
            .unwrap()
            .is_empty());
        assert!(!ser
            .serialize_udt(&Value::Udt(Box::new(phone)), &phone_schema)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn test_serialize_collection_containing_udts_with_unicode_keys() {
        let ser = TypeSerializer::new();

        let address_schema = UdtTypeDef::new("test_ks".to_string(), "address".to_string())
            .with_field("street".to_string(), CqlType::Text, true)
            .with_field("city".to_string(), CqlType::Text, true);
        let address = UdtValue::new("address".to_string(), "test_ks".to_string())
            .with_field(
                "street".to_string(),
                Some(Value::text("Rua Sao Joao".to_string())),
            )
            .with_field(
                "city".to_string(),
                Some(Value::text("Sao Paulo".to_string())),
            );
        let address_bytes = ser
            .serialize_udt(&Value::Udt(Box::new(address)), &address_schema)
            .unwrap();

        let value = Value::Map(vec![(
            Value::text("cidade_日本".to_string()),
            Value::Frozen(Box::new(Value::Udt(Box::new(
                UdtValue::new("address".to_string(), "test_ks".to_string())
                    .with_field(
                        "street".to_string(),
                        Some(Value::text("Rua Sao Joao".to_string())),
                    )
                    .with_field(
                        "city".to_string(),
                        Some(Value::text("Sao Paulo".to_string())),
                    ),
            )))),
        )]);

        let bytes = ser
            .serialize_typed_value(
                &value,
                &CqlType::Map(
                    Box::new(CqlType::Text),
                    Box::new(CqlType::Frozen(Box::new(CqlType::Udt(
                        "address".to_string(),
                        vec![],
                    )))),
                ),
            )
            .unwrap();

        let key = "cidade_日本".as_bytes();
        let mut expected = Vec::new();
        expected.extend_from_slice(&1i32.to_be_bytes());
        expected.extend_from_slice(&(key.len() as i32).to_be_bytes());
        expected.extend_from_slice(key);
        expected.extend_from_slice(&(address_bytes.len() as i32).to_be_bytes());
        expected.extend_from_slice(&address_bytes);

        assert_eq!(hex(&bytes), hex(&expected));
    }

    #[test]
    fn test_serialize_tuple_with_collection_fields() {
        let ser = TypeSerializer::new();

        let value = Value::Tuple(vec![
            Value::text("phase3".to_string()),
            Value::List(vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3),
            ]),
            Value::Map(vec![
                (
                    Value::text("emoji".to_string()),
                    Value::text("snowman".to_string()),
                ),
                (
                    Value::text("plain".to_string()),
                    Value::text("ascii".to_string()),
                ),
            ]),
        ]);

        let bytes = ser
            .serialize_value(&value, "tuple<text, list<int>, map<text, text>>")
            .unwrap();

        let list_bytes = ser
            .serialize_value(
                &Value::List(vec![
                    Value::Integer(1),
                    Value::Integer(2),
                    Value::Integer(3),
                ]),
                "list<int>",
            )
            .unwrap();
        let map_bytes = ser
            .serialize_value(
                &Value::Map(vec![
                    (
                        Value::text("emoji".to_string()),
                        Value::text("snowman".to_string()),
                    ),
                    (
                        Value::text("plain".to_string()),
                        Value::text("ascii".to_string()),
                    ),
                ]),
                "map<text, text>",
            )
            .unwrap();

        let mut expected = Vec::new();
        expected.extend_from_slice(&6i32.to_be_bytes());
        expected.extend_from_slice(b"phase3");
        expected.extend_from_slice(&(list_bytes.len() as i32).to_be_bytes());
        expected.extend_from_slice(&list_bytes);
        expected.extend_from_slice(&(map_bytes.len() as i32).to_be_bytes());
        expected.extend_from_slice(&map_bytes);

        assert_eq!(hex(&bytes), hex(&expected));
    }

    #[test]
    fn test_serialize_frozen_collection() {
        let ser = TypeSerializer::new();

        let frozen_list = Value::List(vec![Value::Integer(1), Value::Integer(2)]);

        let bytes = ser
            .serialize_value(&frozen_list, "frozen<list<int>>")
            .unwrap();

        // Frozen wrapper doesn't change encoding
        let expected = vec![
            0x00, 0x00, 0x00, 0x02, // count = 2
            0x00, 0x00, 0x00, 0x04, // len1 = 4
            0x00, 0x00, 0x00, 0x01, // val1 = 1
            0x00, 0x00, 0x00, 0x04, // len2 = 4
            0x00, 0x00, 0x00, 0x02, // val2 = 2
        ];

        assert_eq!(hex(&bytes), hex(&expected));
    }

    #[test]
    fn test_serialize_null() {
        let ser = TypeSerializer::new();

        // NULL values serialize to empty byte array
        let bytes = ser.serialize_value(&Value::Null, "int").unwrap();
        assert_eq!(bytes, Vec::<u8>::new());
    }

    #[test]
    fn test_type_mismatch_errors() {
        let ser = TypeSerializer::new();

        // Wrong type for int
        assert!(ser
            .serialize_value(&Value::text("hello".to_string()), "int")
            .is_err());

        // Wrong type for boolean
        assert!(ser.serialize_value(&Value::Integer(42), "boolean").is_err());

        // Wrong type for list
        assert!(ser
            .serialize_value(&Value::Integer(42), "list<int>")
            .is_err());
    }

    #[test]
    fn test_udt_type_name_mismatch() {
        let ser = TypeSerializer::new();

        let schema = UdtTypeDef::new("test_ks".to_string(), "address".to_string()).with_field(
            "street".to_string(),
            CqlType::Text,
            true,
        );

        let udt = UdtValue::new("person".to_string(), "test_ks".to_string()).with_field(
            "street".to_string(),
            Some(Value::text("Main St".to_string())),
        );

        let result = ser.serialize_udt(&Value::Udt(Box::new(udt)), &schema);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("UDT type mismatch"));
    }
}
