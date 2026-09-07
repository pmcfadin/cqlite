//! CEP-25 Compliant Byte-comparable key encoding for BTI format
//!
//! Converts CQL keys to byte sequences where lexicographic comparison
//! of unsigned bytes produces the same result as typed comparison.
//!
//! This implementation follows the CEP-25 specification for Cassandra 5.0
//! byte-comparable key encoding used in trie-indexed SSTables.
//!
//! Key Requirements:
//! - Lexicographic byte comparison must match typed comparison
//! - Support for all Cassandra 5.0 data types  
//! - Proper null value handling with type prefixes
//! - Variable-length encoding for efficiency
//! - Nested type support (collections, UDTs, tuples)
//! - Escape sequences for proper ordering

use super::BtiError;
use crate::error::Result;
use crate::types::{UdtValue, Value};

/// CEP-25 compliant byte-comparable key encoder
///
/// Implements the byte-comparable key encoding as specified in CEP-25.
/// The encoding ensures that lexicographic comparison of the encoded bytes
/// produces the same ordering as the typed comparison of the original values.
pub struct ByteComparableEncoder {
    /// Buffer for building encoded keys
    buffer: Vec<u8>,
    /// Configuration for encoding behavior
    config: EncoderConfig,
}

/// Configuration for the byte-comparable encoder
#[derive(Debug, Clone)]
pub struct EncoderConfig {
    /// Enable variable-length integer encoding
    pub use_varint_encoding: bool,
    /// Maximum depth for nested types to prevent infinite recursion
    pub max_nesting_depth: usize,
    /// Enable prefix compression for collections
    pub enable_prefix_compression: bool,
    /// Strict CEP-25 compliance mode
    pub strict_compliance: bool,
}

impl Default for EncoderConfig {
    fn default() -> Self {
        Self {
            use_varint_encoding: true,
            max_nesting_depth: 32,
            enable_prefix_compression: true,
            strict_compliance: true,
        }
    }
}

/// Type prefixes for byte-comparable encoding as per CEP-25
mod type_prefixes {
    pub const NULL: u8 = 0x00;
    pub const BOOLEAN_FALSE: u8 = 0x01;
    pub const BOOLEAN_TRUE: u8 = 0x02;
    pub const TINYINT: u8 = 0x10;
    pub const SMALLINT: u8 = 0x11;
    pub const INTEGER: u8 = 0x12;
    pub const BIGINT: u8 = 0x13;
    pub const FLOAT: u8 = 0x20;
    pub const DOUBLE: u8 = 0x21;
    #[allow(dead_code)]
    pub const DECIMAL: u8 = 0x22;
    #[allow(dead_code)]
    pub const VARINT: u8 = 0x23;
    pub const TEXT: u8 = 0x30;
    pub const BLOB: u8 = 0x31;
    pub const UUID: u8 = 0x40;
    pub const TIMESTAMP: u8 = 0x41;
    #[allow(dead_code)]
    pub const DATE: u8 = 0x42;
    #[allow(dead_code)]
    pub const TIME: u8 = 0x43;
    #[allow(dead_code)]
    pub const DURATION: u8 = 0x44;
    pub const LIST: u8 = 0x50;
    pub const SET: u8 = 0x51;
    pub const MAP: u8 = 0x52;
    pub const TUPLE: u8 = 0x60;
    pub const UDT: u8 = 0x61;
    pub const FROZEN: u8 = 0x70;
    #[allow(dead_code)]
    pub const TOMBSTONE: u8 = 0x80;
    #[allow(dead_code)]
    pub const ESCAPE: u8 = 0xFF;
    pub const SEPARATOR: u8 = 0x00;
    pub const TERMINATOR: u8 = 0x01;
}

/// Escape sequences for special bytes in values
mod escape_sequences {
    #[allow(dead_code)]
    pub const ESCAPE_BYTE: u8 = 0xFF;
    #[allow(dead_code)]
    pub const ESCAPED_NULL: &[u8] = &[0xFF, 0x00];
    #[allow(dead_code)]
    pub const ESCAPED_ESCAPE: &[u8] = &[0xFF, 0xFF];
    #[allow(dead_code)]
    pub const ESCAPED_SEPARATOR: &[u8] = &[0xFF, 0x01];
}

impl Default for ByteComparableEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl ByteComparableEncoder {
    /// Create new encoder with default configuration
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            config: EncoderConfig::default(),
        }
    }

    /// Create new encoder with custom configuration
    pub fn with_config(config: EncoderConfig) -> Self {
        Self {
            buffer: Vec::new(),
            config,
        }
    }

    /// Get current configuration
    pub fn config(&self) -> &EncoderConfig {
        &self.config
    }

    /// Set new configuration
    pub fn set_config(&mut self, config: EncoderConfig) {
        self.config = config;
    }

    /// Encode a single value to byte-comparable format
    pub fn encode_value(&mut self, value: &Value) -> Result<Vec<u8>> {
        self.buffer.clear();
        self.encode_value_to_buffer(value)?;
        Ok(self.buffer.clone())
    }

    /// Encode a composite key (multiple values) to byte-comparable format
    pub fn encode_composite_key(&mut self, values: &[Value]) -> Result<Vec<u8>> {
        self.buffer.clear();

        for (i, value) in values.iter().enumerate() {
            if i > 0 {
                // Add separator byte between key components
                self.buffer.push(0x00);
            }
            self.encode_value_to_buffer(value)?;
        }

        Ok(self.buffer.clone())
    }

    /// Encode value directly to internal buffer with depth tracking
    fn encode_value_to_buffer(&mut self, value: &Value) -> Result<()> {
        self.encode_value_to_buffer_with_depth(value, 0)
    }

    /// Encode value with nesting depth tracking
    fn encode_value_to_buffer_with_depth(&mut self, value: &Value, depth: usize) -> Result<()> {
        if depth > self.config.max_nesting_depth {
            return Err(BtiError::InvalidByteComparableKey(format!(
                "Maximum nesting depth {} exceeded",
                self.config.max_nesting_depth
            ))
            .into());
        }

        match value {
            Value::Null => self.encode_null(),
            // EMPTY-BUFFER SENTINEL (issue #3805) — REFUSED here, deliberately
            // and fail-closed. This encoder builds BTI byte-comparable KEYS
            // (partition/clustering components); the sentinel arises from a
            // multicell collection's CELL PATH
            // (`db/marshal/CollectionType.java:361-382` at `cassandra-5.0.8`),
            // which is not a byte-comparable key component and never reaches
            // this encoder. Cassandra's byte-comparable encoding of an empty
            // value is a separate, unaudited question, so this refuses rather
            // than inventing a prefix — inferring one would be a heuristic
            // (issue #28).
            Value::Empty(ty) => Err(BtiError::InvalidByteComparableKey(format!(
                "empty-buffer sentinel (declared type `{}`) is not a byte-comparable \
                 key component (issue #3805)",
                ty.cql_name()
            ))
            .into()),
            Value::Boolean(b) => self.encode_boolean(*b),
            Value::TinyInt(i) => self.encode_tinyint(*i),
            Value::SmallInt(i) => self.encode_smallint(*i),
            Value::Integer(i) => self.encode_int(*i),
            Value::BigInt(i) => self.encode_bigint(*i),
            Value::Counter(c) => self.encode_bigint(*c), // Counter encoded as bigint
            Value::Float32(f) => self.encode_float32(*f),
            Value::Float(f) => self.encode_double(*f),
            Value::Text(s) => self
                .encode_text(std::str::from_utf8(s).map_err(|e| {
                    crate::Error::corruption(format!("invalid UTF-8 in text: {e}"))
                })?),
            Value::Blob(bytes) => self.encode_blob(bytes),
            Value::Uuid(uuid) => self.encode_uuid_bytes(uuid),
            Value::Timestamp(ts) => self.encode_timestamp(*ts),
            Value::Json(json) => self.encode_json(json),
            Value::List(items) => self.encode_list_with_depth(items, depth + 1),
            Value::Set(items) => self.encode_set_with_depth(items, depth + 1),
            Value::Map(map) => self.encode_map_with_depth(map, depth + 1),
            Value::Tuple(items) => self.encode_tuple_with_depth(items, depth + 1),
            Value::Udt(udt) => self.encode_udt_with_depth(udt, depth + 1),
            Value::Frozen(inner) => {
                self.buffer.push(type_prefixes::FROZEN);
                self.encode_value_to_buffer_with_depth(inner, depth + 1)
            }
            Value::Varint(data) => {
                // For BTI encoding, treat varint as blob since we don't have the original value
                self.encode_blob(data)
            }
            Value::Decimal { scale: _, unscaled } => self.encode_blob(unscaled), // Treat decimal as blob for BTI
            Value::Duration {
                months,
                days,
                nanos,
            } => {
                // Encode duration as a composite key
                self.buffer.push(type_prefixes::DURATION);
                self.encode_int(*months)?;
                self.encode_int(*days)?;
                self.encode_bigint(*nanos)?;
                Ok(())
            }
            Value::Tombstone(_) => {
                // Tombstones are encoded as null with special marker
                self.buffer.push(type_prefixes::NULL);
                self.buffer.push(0xFF); // Special tombstone marker
                Ok(())
            }
            Value::Date(d) => {
                self.buffer.push(type_prefixes::DATE);
                self.encode_int(*d)
            }
            Value::Time(t) => {
                self.buffer.push(type_prefixes::TIME);
                self.encode_bigint(*t)
            }
            Value::Inet(bytes) => self.encode_blob(bytes),
        }
    }

    /// Encode null value with proper type prefix
    fn encode_null(&mut self) -> Result<()> {
        self.buffer.push(type_prefixes::NULL);
        Ok(())
    }

    /// Encode text/varchar with proper UTF-8 ordering and escape sequences
    fn encode_text(&mut self, text: &str) -> Result<()> {
        self.buffer.push(type_prefixes::TEXT);

        // Encode UTF-8 bytes with proper escaping
        for &byte in text.as_bytes() {
            match byte {
                0x00 => self
                    .buffer
                    .extend_from_slice(escape_sequences::ESCAPED_NULL),
                0xFF => self
                    .buffer
                    .extend_from_slice(escape_sequences::ESCAPED_ESCAPE),
                _ => self.buffer.push(byte),
            }
        }

        // Add terminator for proper ordering
        self.buffer.push(type_prefixes::TERMINATOR);
        Ok(())
    }

    /// Encode JSON as escaped text
    fn encode_json(&mut self, json: &serde_json::Value) -> Result<()> {
        let json_str = json.to_string();
        self.encode_text(&json_str)
    }

    /// Encode tinyint (i8) with proper ordering
    fn encode_tinyint(&mut self, value: i8) -> Result<()> {
        self.buffer.push(type_prefixes::TINYINT);
        // Transform to unsigned for proper lexicographic ordering
        let unsigned = (value as i16 + 128) as u8;
        self.buffer.push(unsigned);
        Ok(())
    }

    /// Encode smallint (i16) with proper ordering
    fn encode_smallint(&mut self, value: i16) -> Result<()> {
        self.buffer.push(type_prefixes::SMALLINT);
        // Transform to unsigned for proper lexicographic ordering
        let unsigned = (value as i32 + 32768) as u16;
        self.buffer.extend_from_slice(&unsigned.to_be_bytes());
        Ok(())
    }

    /// Encode integer (i32) with sign-magnitude encoding
    fn encode_int(&mut self, value: i32) -> Result<()> {
        self.buffer.push(type_prefixes::INTEGER);

        // Use two's complement transformation for proper ordering
        // Transform signed to unsigned preserving order: flip sign bit
        let unsigned = (value as u32) ^ 0x8000_0000;

        self.buffer.extend_from_slice(&unsigned.to_be_bytes());
        Ok(())
    }

    /// Encode bigint (i64) with sign-magnitude encoding
    fn encode_bigint(&mut self, value: i64) -> Result<()> {
        self.buffer.push(type_prefixes::BIGINT);

        // Use two's complement transformation for proper ordering
        let unsigned = if value >= 0 {
            (value as u64) + 0x8000_0000_0000_0000
        } else {
            (value as u64) ^ 0xFFFF_FFFF_FFFF_FFFF
        };

        self.buffer.extend_from_slice(&unsigned.to_be_bytes());
        Ok(())
    }

    /// Encode UUID bytes with proper byte ordering
    fn encode_uuid_bytes(&mut self, uuid: &[u8; 16]) -> Result<()> {
        self.buffer.push(type_prefixes::UUID);
        // UUID bytes in network byte order are naturally comparable
        self.buffer.extend_from_slice(uuid);
        Ok(())
    }

    /// Encode timestamp (microseconds since epoch)
    fn encode_timestamp(&mut self, timestamp: i64) -> Result<()> {
        self.buffer.push(type_prefixes::TIMESTAMP);

        // Transform for proper ordering (timestamps can be negative)
        let unsigned = if timestamp >= 0 {
            (timestamp as u64) + 0x8000_0000_0000_0000
        } else {
            (timestamp as u64) ^ 0xFFFF_FFFF_FFFF_FFFF
        };

        self.buffer.extend_from_slice(&unsigned.to_be_bytes());
        Ok(())
    }

    /// Encode boolean with proper type prefixes
    fn encode_boolean(&mut self, value: bool) -> Result<()> {
        if value {
            self.buffer.push(type_prefixes::BOOLEAN_TRUE);
        } else {
            self.buffer.push(type_prefixes::BOOLEAN_FALSE);
        }
        Ok(())
    }

    /// Encode float32 with IEEE 754 ordering adjustment
    fn encode_float32(&mut self, value: f32) -> Result<()> {
        self.buffer.push(type_prefixes::FLOAT);

        // Handle special values first
        if value.is_nan() {
            // NaN sorts after all other values
            self.buffer.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF]);
            return Ok(());
        }

        let bits = value.to_bits();

        // Adjust for proper ordering of IEEE 754 floats
        let adjusted = if (bits & 0x8000_0000) == 0 {
            // Positive: add sign bit offset
            bits | 0x8000_0000
        } else {
            // Negative: flip all bits
            !bits
        };

        self.buffer.extend_from_slice(&adjusted.to_be_bytes());
        Ok(())
    }

    /// Encode double (f64) with IEEE 754 ordering adjustment
    fn encode_double(&mut self, value: f64) -> Result<()> {
        self.buffer.push(type_prefixes::DOUBLE);

        // Handle special values first
        if value.is_nan() {
            // NaN sorts after all other values
            self.buffer
                .extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
            return Ok(());
        }

        let bits = value.to_bits();

        // Adjust for proper ordering of IEEE 754 floats
        let adjusted = if (bits & 0x8000_0000_0000_0000) == 0 {
            // Positive: add sign bit offset
            bits | 0x8000_0000_0000_0000
        } else {
            // Negative: flip all bits
            !bits
        };

        self.buffer.extend_from_slice(&adjusted.to_be_bytes());
        Ok(())
    }

    /// Encode blob (binary data) with proper escaping
    fn encode_blob(&mut self, bytes: &[u8]) -> Result<()> {
        self.buffer.push(type_prefixes::BLOB);

        // Encode raw bytes with proper escaping
        for &byte in bytes {
            match byte {
                0x00 => self
                    .buffer
                    .extend_from_slice(escape_sequences::ESCAPED_NULL),
                0xFF => self
                    .buffer
                    .extend_from_slice(escape_sequences::ESCAPED_ESCAPE),
                _ => self.buffer.push(byte),
            }
        }

        // Add terminator
        self.buffer.push(type_prefixes::TERMINATOR);
        Ok(())
    }

    /// Encode list with element-by-element encoding and depth tracking
    fn encode_list_with_depth(&mut self, items: &[Value], depth: usize) -> Result<()> {
        self.buffer.push(type_prefixes::LIST);

        // Encode length as varint if configured
        if self.config.use_varint_encoding {
            self.encode_varint(items.len() as u64)?;
        } else {
            self.buffer
                .extend_from_slice(&(items.len() as u32).to_be_bytes());
        }

        // Encode each element with separator
        for (i, item) in items.iter().enumerate() {
            if i > 0 {
                self.buffer.push(type_prefixes::SEPARATOR);
            }
            self.encode_value_to_buffer_with_depth(item, depth)?;
        }

        // Add terminator
        self.buffer.push(type_prefixes::TERMINATOR);
        Ok(())
    }

    /// Backward compatibility wrapper
    #[allow(dead_code)]
    fn encode_list(&mut self, items: &[Value]) -> Result<()> {
        self.encode_list_with_depth(items, 1)
    }

    /// Encode set (sorted for deterministic ordering) with depth tracking
    fn encode_set_with_depth(&mut self, items: &[Value], _depth: usize) -> Result<()> {
        self.buffer.push(type_prefixes::SET);

        // For byte-comparable encoding, we need to sort the encoded items
        let mut encoded_items = Vec::new();

        for item in items {
            let mut encoder = ByteComparableEncoder::with_config(self.config.clone());
            let encoded = encoder.encode_value(item)?;
            encoded_items.push(encoded);
        }

        // Sort encoded items lexicographically for deterministic ordering
        encoded_items.sort();

        // Encode length
        if self.config.use_varint_encoding {
            self.encode_varint(encoded_items.len() as u64)?;
        } else {
            self.buffer
                .extend_from_slice(&(encoded_items.len() as u32).to_be_bytes());
        }

        // Add sorted encoded items with separators
        for (i, encoded_item) in encoded_items.iter().enumerate() {
            if i > 0 {
                self.buffer.push(type_prefixes::SEPARATOR);
            }
            self.buffer.extend_from_slice(encoded_item);
        }

        // Add terminator
        self.buffer.push(type_prefixes::TERMINATOR);
        Ok(())
    }

    /// Backward compatibility wrapper
    #[allow(dead_code)]
    fn encode_set(&mut self, items: &[Value]) -> Result<()> {
        self.encode_set_with_depth(items, 1)
    }

    /// Encode map from Vec of tuples with sorted key-value pairs and depth tracking
    fn encode_map_with_depth(&mut self, map: &Vec<(Value, Value)>, _depth: usize) -> Result<()> {
        self.buffer.push(type_prefixes::MAP);

        // Encode key-value pairs and sort by encoded keys
        let mut encoded_pairs = Vec::new();

        for (key, value) in map {
            let mut key_encoder = ByteComparableEncoder::with_config(self.config.clone());
            let encoded_key = key_encoder.encode_value(key)?;

            let mut value_encoder = ByteComparableEncoder::with_config(self.config.clone());
            let encoded_value = value_encoder.encode_value(value)?;

            encoded_pairs.push((encoded_key, encoded_value));
        }

        // Sort by encoded keys for deterministic ordering
        encoded_pairs.sort_by(|a, b| a.0.cmp(&b.0));

        // Encode length
        if self.config.use_varint_encoding {
            self.encode_varint(encoded_pairs.len() as u64)?;
        } else {
            self.buffer
                .extend_from_slice(&(encoded_pairs.len() as u32).to_be_bytes());
        }

        // Add sorted pairs with separators
        for (i, (encoded_key, encoded_value)) in encoded_pairs.iter().enumerate() {
            if i > 0 {
                self.buffer.push(type_prefixes::SEPARATOR);
            }

            // Encode key-value pair
            self.buffer.extend_from_slice(encoded_key);
            self.buffer.push(type_prefixes::SEPARATOR);
            self.buffer.extend_from_slice(encoded_value);
        }

        // Add terminator
        self.buffer.push(type_prefixes::TERMINATOR);
        Ok(())
    }

    /// Backward compatibility wrapper
    #[allow(dead_code)]
    fn encode_map_vec(&mut self, map: &Vec<(Value, Value)>) -> Result<()> {
        self.encode_map_with_depth(map, 1)
    }

    /// Encode tuple with positional fields and depth tracking
    fn encode_tuple_with_depth(&mut self, items: &[Value], depth: usize) -> Result<()> {
        self.buffer.push(type_prefixes::TUPLE);

        // Encode length
        if self.config.use_varint_encoding {
            self.encode_varint(items.len() as u64)?;
        } else {
            self.buffer
                .extend_from_slice(&(items.len() as u32).to_be_bytes());
        }

        // Encode each field with separator (order is significant for tuples)
        for (i, item) in items.iter().enumerate() {
            if i > 0 {
                self.buffer.push(type_prefixes::SEPARATOR);
            }
            self.encode_value_to_buffer_with_depth(item, depth)?;
        }

        // Add terminator
        self.buffer.push(type_prefixes::TERMINATOR);
        Ok(())
    }

    /// Encode UDT (User Defined Type) with field ordering and depth tracking
    fn encode_udt_with_depth(&mut self, udt: &UdtValue, depth: usize) -> Result<()> {
        self.buffer.push(type_prefixes::UDT);

        // Encode type name and keyspace for disambiguation
        self.encode_text(&udt.keyspace)?;
        self.encode_text(&udt.type_name)?;

        // Encode field count
        if self.config.use_varint_encoding {
            self.encode_varint(udt.fields.len() as u64)?;
        } else {
            self.buffer
                .extend_from_slice(&(udt.fields.len() as u32).to_be_bytes());
        }

        // Encode fields in schema order (important for UDTs)
        for (i, field) in udt.fields.iter().enumerate() {
            if i > 0 {
                self.buffer.push(type_prefixes::SEPARATOR);
            }

            // Encode field name
            self.encode_text(&field.name)?;
            self.buffer.push(type_prefixes::SEPARATOR);

            // Encode field value (null if None)
            match &field.value {
                Some(value) => self.encode_value_to_buffer_with_depth(value, depth)?,
                None => self.encode_null()?,
            }
        }

        // Add terminator
        self.buffer.push(type_prefixes::TERMINATOR);
        Ok(())
    }

    /// Encode variable-length integer (varint)
    fn encode_varint(&mut self, mut value: u64) -> Result<()> {
        while value >= 0x80 {
            self.buffer.push((value & 0xFF) as u8 | 0x80);
            value >>= 7;
        }
        self.buffer.push(value as u8);
        Ok(())
    }
}

/// Byte-comparable key decoder (for debugging/testing)
pub struct ByteComparableDecoder;

impl ByteComparableDecoder {
    /// Decode a byte-comparable key back to readable format (best effort)
    pub fn decode_key_debug(encoded: &[u8]) -> String {
        if encoded.is_empty() {
            return "<empty>".to_string();
        }

        // Simple hex representation for debugging
        let hex: String = encoded
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<Vec<_>>()
            .join(" ");

        // Try to detect if it looks like text (allow null bytes)
        if let Ok(text) = std::str::from_utf8(encoded) {
            // Check if most characters are printable (allow some control chars like null)
            let printable_count = text
                .chars()
                .filter(|c| c.is_ascii_graphic() || c.is_ascii_whitespace())
                .count();
            let total_count = text.chars().count();

            if total_count > 0 && (printable_count as f32 / total_count as f32) >= 0.7 {
                let clean_text = text.trim_end_matches('\0').trim();
                if !clean_text.is_empty() {
                    return format!("\"{}\" ({})", clean_text, hex);
                }
            }
        }

        // Try to extract text even from binary data that might contain readable strings
        let mut text_parts = Vec::new();
        let mut current_text = String::new();

        for &byte in encoded {
            if byte.is_ascii_graphic() || byte == b' ' {
                current_text.push(byte as char);
            } else if !current_text.is_empty() {
                text_parts.push(current_text.clone());
                current_text.clear();
            }
        }

        if !current_text.is_empty() {
            text_parts.push(current_text);
        }

        // If we found any readable text parts, include them
        if !text_parts.is_empty() {
            let text_content = text_parts.join(" ");
            return format!("\"{}\" ({})", text_content, hex);
        }

        format!("0x{}", hex)
    }
}

/// Batch encoder for efficient encoding of multiple values
pub struct BatchEncoder {
    encoder: ByteComparableEncoder,
    batch_buffer: Vec<Vec<u8>>,
}

impl Default for BatchEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl BatchEncoder {
    /// Create new batch encoder
    pub fn new() -> Self {
        Self {
            encoder: ByteComparableEncoder::new(),
            batch_buffer: Vec::new(),
        }
    }

    /// Encode a batch of values efficiently
    pub fn encode_batch(&mut self, values: &[Value]) -> Result<Vec<Vec<u8>>> {
        self.batch_buffer.clear();
        self.batch_buffer.reserve(values.len());

        for value in values {
            let encoded = self.encoder.encode_value(value)?;
            self.batch_buffer.push(encoded);
        }

        Ok(self.batch_buffer.clone())
    }

    /// Clear the batch buffer
    pub fn clear(&mut self) {
        self.batch_buffer.clear();
    }
}

/// Performance statistics for the encoder
#[derive(Debug, Clone, Default)]
pub struct EncoderStats {
    /// Current buffer capacity
    pub buffer_capacity: usize,
    /// Current buffer size
    pub buffer_size: usize,
    /// Number of encodings performed
    pub encodings_performed: u64,
    /// Total bytes encoded
    pub total_bytes_encoded: u64,
}

impl ByteComparableEncoder {
    /// Reserve capacity in the internal buffer
    pub fn reserve(&mut self, additional: usize) {
        self.buffer.reserve(additional);
    }

    /// Get performance statistics
    pub fn get_stats(&self) -> EncoderStats {
        EncoderStats {
            buffer_capacity: self.buffer.capacity(),
            buffer_size: self.buffer.len(),
            encodings_performed: 0, // Would need to track this in practice
            total_bytes_encoded: self.buffer.len() as u64,
        }
    }

    /// Validate an encoded key for correctness
    pub fn validate_encoded_key(&self, encoded: &[u8]) -> Result<()> {
        if encoded.is_empty() {
            return Err(BtiError::InvalidByteComparableKey("Empty encoded key".to_string()).into());
        }

        let type_prefix = encoded[0];

        // Validate type prefix
        match type_prefix {
            type_prefixes::NULL => {
                if encoded.len() > 2 {
                    return Err(BtiError::InvalidByteComparableKey(
                        "Null value too long".to_string(),
                    )
                    .into());
                }
            }
            type_prefixes::BOOLEAN_FALSE | type_prefixes::BOOLEAN_TRUE => {
                if encoded.len() != 1 {
                    return Err(BtiError::InvalidByteComparableKey(
                        "Boolean value should be exactly 1 byte".to_string(),
                    )
                    .into());
                }
            }
            type_prefixes::TINYINT => {
                if encoded.len() != 2 {
                    return Err(BtiError::InvalidByteComparableKey(
                        "TinyInt should be exactly 2 bytes".to_string(),
                    )
                    .into());
                }
            }
            type_prefixes::SMALLINT => {
                if encoded.len() != 3 {
                    return Err(BtiError::InvalidByteComparableKey(
                        "SmallInt should be exactly 3 bytes".to_string(),
                    )
                    .into());
                }
            }
            type_prefixes::INTEGER => {
                if encoded.len() != 5 {
                    return Err(BtiError::InvalidByteComparableKey(
                        "Integer should be exactly 5 bytes".to_string(),
                    )
                    .into());
                }
            }
            type_prefixes::BIGINT | type_prefixes::TIMESTAMP => {
                if encoded.len() != 9 {
                    return Err(BtiError::InvalidByteComparableKey(
                        "BigInt/Timestamp should be exactly 9 bytes".to_string(),
                    )
                    .into());
                }
            }
            type_prefixes::FLOAT => {
                if encoded.len() != 5 {
                    return Err(BtiError::InvalidByteComparableKey(
                        "Float should be exactly 5 bytes".to_string(),
                    )
                    .into());
                }
            }
            type_prefixes::DOUBLE => {
                if encoded.len() != 9 {
                    return Err(BtiError::InvalidByteComparableKey(
                        "Double should be exactly 9 bytes".to_string(),
                    )
                    .into());
                }
            }
            type_prefixes::UUID => {
                if encoded.len() != 17 {
                    return Err(BtiError::InvalidByteComparableKey(
                        "UUID should be exactly 17 bytes".to_string(),
                    )
                    .into());
                }
            }
            type_prefixes::TEXT | type_prefixes::BLOB => {
                if encoded.len() < 2 || encoded[encoded.len() - 1] != type_prefixes::TERMINATOR {
                    return Err(BtiError::InvalidByteComparableKey(
                        "Text/Blob should end with terminator".to_string(),
                    )
                    .into());
                }
            }
            type_prefixes::LIST
            | type_prefixes::SET
            | type_prefixes::MAP
            | type_prefixes::TUPLE
            | type_prefixes::UDT => {
                if encoded.len() < 2 || encoded[encoded.len() - 1] != type_prefixes::TERMINATOR {
                    return Err(BtiError::InvalidByteComparableKey(
                        "Collection/Complex type should end with terminator".to_string(),
                    )
                    .into());
                }
            }
            _ => {
                return Err(BtiError::InvalidByteComparableKey(format!(
                    "Unknown type prefix: 0x{:02x}",
                    type_prefix
                ))
                .into());
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn test_text_encoding() {
        let mut encoder = ByteComparableEncoder::new();

        let encoded_a = encoder.encode_value(&Value::text("a".to_string())).unwrap();
        let encoded_b = encoder.encode_value(&Value::text("b".to_string())).unwrap();
        let encoded_aa = encoder
            .encode_value(&Value::text("aa".to_string()))
            .unwrap();

        // Lexicographic comparison should match string comparison
        assert!(encoded_a < encoded_b);
        assert!(encoded_a < encoded_aa);
        assert!(encoded_aa < encoded_b);
    }

    #[test]
    fn test_integer_encoding() {
        let mut encoder = ByteComparableEncoder::new();

        let encoded_neg = encoder.encode_value(&Value::Integer(-100)).unwrap();
        let encoded_zero = encoder.encode_value(&Value::Integer(0)).unwrap();
        let encoded_pos = encoder.encode_value(&Value::Integer(100)).unwrap();

        // Proper numeric ordering
        assert!(encoded_neg < encoded_zero);
        assert!(encoded_zero < encoded_pos);
    }

    #[test]
    fn test_boolean_encoding() {
        let mut encoder = ByteComparableEncoder::new();

        let encoded_false = encoder.encode_value(&Value::Boolean(false)).unwrap();
        let encoded_true = encoder.encode_value(&Value::Boolean(true)).unwrap();

        // false < true
        assert!(encoded_false < encoded_true);
    }

    #[test]
    fn test_uuid_encoding() {
        let mut encoder = ByteComparableEncoder::new();

        let uuid1 = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let uuid2 = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();

        let encoded1 = encoder
            .encode_value(&Value::Uuid(*uuid1.as_bytes()))
            .unwrap();
        let encoded2 = encoder
            .encode_value(&Value::Uuid(*uuid2.as_bytes()))
            .unwrap();

        assert!(encoded1 < encoded2);
    }

    #[test]
    fn test_composite_key_encoding() {
        let mut encoder = ByteComparableEncoder::new();

        let key1 = vec![Value::text("partition1".to_string()), Value::Integer(1)];
        let key2 = vec![Value::text("partition1".to_string()), Value::Integer(2)];
        let key3 = vec![Value::text("partition2".to_string()), Value::Integer(1)];

        let encoded1 = encoder.encode_composite_key(&key1).unwrap();
        let encoded2 = encoder.encode_composite_key(&key2).unwrap();
        let encoded3 = encoder.encode_composite_key(&key3).unwrap();

        // Proper composite key ordering
        assert!(encoded1 < encoded2); // Same partition, different clustering
        assert!(encoded2 < encoded3); // Different partition
    }

    #[test]
    fn test_list_encoding() {
        let mut encoder = ByteComparableEncoder::new();

        let list1 = Value::List(vec![Value::Integer(1), Value::Integer(2)]);
        let list2 = Value::List(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ]);

        let encoded1 = encoder.encode_value(&list1).unwrap();
        let encoded2 = encoder.encode_value(&list2).unwrap();

        // Shorter list should come first
        assert!(encoded1 < encoded2);
    }

    #[test]
    fn test_float_special_values() {
        let mut encoder = ByteComparableEncoder::new();

        let neg_inf = encoder
            .encode_value(&Value::Float(f64::NEG_INFINITY))
            .unwrap();
        let neg_one = encoder.encode_value(&Value::Float(-1.0)).unwrap();
        let zero = encoder.encode_value(&Value::Float(0.0)).unwrap();
        let one = encoder.encode_value(&Value::Float(1.0)).unwrap();
        let pos_inf = encoder.encode_value(&Value::Float(f64::INFINITY)).unwrap();

        // Proper float ordering
        assert!(neg_inf < neg_one);
        assert!(neg_one < zero);
        assert!(zero < one);
        assert!(one < pos_inf);
    }

    #[test]
    fn test_decode_key_debug() {
        let text_bytes = b"hello\0";
        let decoded = ByteComparableDecoder::decode_key_debug(text_bytes);
        assert!(decoded.contains("hello"));

        let binary_bytes = &[0xFF, 0xFE, 0xFD];
        let decoded = ByteComparableDecoder::decode_key_debug(binary_bytes);
        assert!(decoded.starts_with("0x"));
    }

    #[test]
    fn test_encoder_reuse() {
        let mut encoder = ByteComparableEncoder::new();

        let encoded1 = encoder
            .encode_value(&Value::text("test1".to_string()))
            .unwrap();
        let encoded2 = encoder
            .encode_value(&Value::text("test2".to_string()))
            .unwrap();

        // Each encoding should be independent
        assert_ne!(encoded1, encoded2);
        assert!(encoded1 < encoded2);
    }

    #[test]
    fn test_encoder_config() {
        let config = EncoderConfig {
            use_varint_encoding: false,
            max_nesting_depth: 16,
            enable_prefix_compression: false,
            strict_compliance: false,
        };

        let encoder = ByteComparableEncoder::with_config(config);
        assert!(!encoder.config().use_varint_encoding);
        assert_eq!(encoder.config().max_nesting_depth, 16);
    }

    #[test]
    fn test_max_nesting_depth() {
        let config = EncoderConfig {
            max_nesting_depth: 2,
            ..Default::default()
        };

        let mut encoder = ByteComparableEncoder::with_config(config);

        // Create deeply nested structure
        let deep_nested = Value::List(vec![Value::List(vec![Value::List(vec![Value::Integer(
            1,
        )])])]);

        // Should fail due to depth limit
        let result = encoder.encode_value(&deep_nested);
        assert!(result.is_err());
    }

    #[test]
    fn test_batch_encoder() {
        let mut batch_encoder = BatchEncoder::new();

        let values = vec![
            Value::Integer(1),
            Value::text("hello".to_string()),
            Value::Boolean(true),
        ];

        let encoded_batch = batch_encoder.encode_batch(&values).unwrap();
        assert_eq!(encoded_batch.len(), 3);

        // Verify individual encodings
        let mut single_encoder = ByteComparableEncoder::new();
        for (i, value) in values.iter().enumerate() {
            let single_encoded = single_encoder.encode_value(value).unwrap();
            assert_eq!(encoded_batch[i], single_encoded);
        }
    }

    #[test]
    fn test_ordering_across_types() {
        let mut encoder = ByteComparableEncoder::new();

        // Different types should have deterministic ordering based on type prefixes
        let null_val = encoder.encode_value(&Value::Null).unwrap();
        let bool_val = encoder.encode_value(&Value::Boolean(false)).unwrap();
        let int_val = encoder.encode_value(&Value::Integer(0)).unwrap();
        let text_val = encoder.encode_value(&Value::text("".to_string())).unwrap();

        // Null should come first, then booleans, then numbers, then text
        assert!(null_val < bool_val);
        assert!(bool_val < int_val);
        assert!(int_val < text_val);
    }

    #[test]
    fn test_validation() {
        let encoder = ByteComparableEncoder::new();

        // Test valid encodings
        assert!(encoder.validate_encoded_key(&[type_prefixes::NULL]).is_ok());
        assert!(encoder
            .validate_encoded_key(&[type_prefixes::BOOLEAN_TRUE])
            .is_ok());

        // Test invalid encodings
        assert!(encoder.validate_encoded_key(&[]).is_err()); // Empty
        assert!(encoder
            .validate_encoded_key(&[type_prefixes::NULL, 0x00, 0x00, 0x00])
            .is_err()); // Null too long
    }

    #[test]
    fn test_performance_stats() {
        let mut encoder = ByteComparableEncoder::new();
        encoder.reserve(1024);

        let stats = encoder.get_stats();
        assert!(stats.buffer_capacity >= 1024);
        assert_eq!(stats.buffer_size, 0);

        // Encode something
        encoder
            .encode_value(&Value::text("test".to_string()))
            .unwrap();
        let stats_after = encoder.get_stats();
        assert!(stats_after.buffer_size > 0);
    }

    #[test]
    fn test_timestamp_encoding() {
        let mut encoder = ByteComparableEncoder::new();

        let past = encoder.encode_value(&Value::Timestamp(-1000)).unwrap();
        let epoch = encoder.encode_value(&Value::Timestamp(0)).unwrap();
        let future = encoder.encode_value(&Value::Timestamp(1000)).unwrap();

        // All should start with timestamp prefix
        assert_eq!(past[0], type_prefixes::TIMESTAMP);
        assert_eq!(epoch[0], type_prefixes::TIMESTAMP);
        assert_eq!(future[0], type_prefixes::TIMESTAMP);

        // Proper temporal ordering
        assert!(past < epoch);
        assert!(epoch < future);
    }

    #[test]
    fn test_blob_encoding() {
        let mut encoder = ByteComparableEncoder::new();

        let blob1 = encoder
            .encode_value(&Value::blob(vec![0x01, 0x02]))
            .unwrap();
        let blob2 = encoder
            .encode_value(&Value::blob(vec![0x01, 0x03]))
            .unwrap();
        let blob_with_null = encoder
            .encode_value(&Value::blob(vec![0x01, 0x00, 0x02]))
            .unwrap();

        // Should start with blob prefix
        assert_eq!(blob1[0], type_prefixes::BLOB);
        assert_eq!(blob2[0], type_prefixes::BLOB);

        // Proper lexicographic ordering
        assert!(blob1 < blob2);

        // Should contain escaped null sequence
        assert!(blob_with_null
            .windows(2)
            .any(|w| w == escape_sequences::ESCAPED_NULL));
    }

    #[test]
    fn test_comprehensive_ordering() {
        let mut encoder = ByteComparableEncoder::new();

        // Test a comprehensive set of values for ordering consistency
        let values = vec![
            Value::Null,
            Value::Boolean(false),
            Value::Boolean(true),
            Value::TinyInt(-1),
            Value::TinyInt(0),
            Value::TinyInt(1),
            Value::SmallInt(-100),
            Value::SmallInt(100),
            Value::Integer(-1000),
            Value::Integer(1000),
            Value::BigInt(-10000),
            Value::BigInt(10000),
            Value::Float32(-1.0),
            Value::Float32(1.0),
            Value::Float(-1.0),
            Value::Float(1.0),
            Value::text("a".to_string()),
            Value::text("z".to_string()),
            Value::blob(vec![0x01]),
            Value::blob(vec![0xFF]),
            Value::Uuid([0u8; 16]),
            Value::Uuid([0xFFu8; 16]),
            Value::Timestamp(-1000),
            Value::Timestamp(1000),
        ];

        let encoded_values: Vec<_> = values
            .iter()
            .map(|v| encoder.encode_value(v).unwrap())
            .collect();

        // Verify that lexicographic ordering of encoded values matches the input ordering
        for i in 0..encoded_values.len() - 1 {
            assert!(
                encoded_values[i] <= encoded_values[i + 1],
                "Ordering violation at index {} and {}",
                i,
                i + 1
            );
        }
    }
}
