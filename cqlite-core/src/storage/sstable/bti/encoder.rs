//! Byte-comparable key encoding for BTI format
//!
//! Converts CQL keys to byte sequences where lexicographic comparison
//! of unsigned bytes produces the same result as typed comparison.

use crate::error::Result;
use crate::types::Value;
use crate::parser::CqlTypeId;
use super::BtiError;
use uuid::Uuid;
use std::collections::HashMap;

/// Byte-comparable key encoder
pub struct ByteComparableEncoder {
    /// Buffer for building encoded keys
    buffer: Vec<u8>,
}

impl ByteComparableEncoder {
    /// Create new encoder
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
        }
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

    /// Encode value directly to internal buffer
    fn encode_value_to_buffer(&mut self, value: &Value) -> Result<()> {
        match value {
            Value::Text(s) => self.encode_text(s),
            Value::Integer(i) => self.encode_int(*i),
            Value::BigInt(i) => self.encode_bigint(*i),
            Value::Uuid(uuid) => self.encode_uuid_bytes(uuid),
            Value::Timestamp(ts) => self.encode_timestamp(*ts),
            Value::Boolean(b) => self.encode_boolean(*b),
            Value::Float(f) => self.encode_float(*f as f32),
            Value::Float32(f) => self.encode_float(*f),
            Value::Blob(bytes) => self.encode_blob(bytes),
            Value::List(items) => self.encode_list(items),
            Value::Set(items) => self.encode_set(items),
            Value::Map(map) => self.encode_map_vec(map),
            Value::Frozen(inner) => self.encode_value_to_buffer(inner),
            _ => {
                return Err(BtiError::InvalidByteComparableKey(
                    format!("Unsupported value type for byte-comparable encoding: {:?}", value)
                ).into());
            }
        }
    }

    /// Encode text/varchar with proper UTF-8 ordering
    fn encode_text(&mut self, text: &str) -> Result<()> {
        // UTF-8 bytes are naturally byte-comparable for text
        self.buffer.extend_from_slice(text.as_bytes());
        // Add null terminator to handle prefix matching
        self.buffer.push(0x00);
        Ok(())
    }

    /// Encode integer with sign-magnitude encoding
    fn encode_int(&mut self, value: i32) -> Result<()> {
        if value >= 0 {
            // Positive: 0x80 prefix + big-endian bytes
            self.buffer.push(0x80);
            self.buffer.extend_from_slice(&(value as u32).to_be_bytes());
        } else {
            // Negative: 0x7F prefix + inverted big-endian bytes
            self.buffer.push(0x7F);
            let inverted = !((-value) as u32);
            self.buffer.extend_from_slice(&inverted.to_be_bytes());
        }
        Ok(())
    }

    /// Encode bigint with sign-magnitude encoding
    fn encode_bigint(&mut self, value: i64) -> Result<()> {
        if value >= 0 {
            // Positive: 0x80 prefix + big-endian bytes
            self.buffer.push(0x80);
            self.buffer.extend_from_slice(&(value as u64).to_be_bytes());
        } else {
            // Negative: 0x7F prefix + inverted big-endian bytes
            self.buffer.push(0x7F);
            let inverted = !((-value) as u64);
            self.buffer.extend_from_slice(&inverted.to_be_bytes());
        }
        Ok(())
    }

    /// Encode UUID with proper byte ordering
    fn encode_uuid(&mut self, uuid: &Uuid) -> Result<()> {
        // UUID bytes are naturally comparable
        self.buffer.extend_from_slice(uuid.as_bytes());
        Ok(())
    }

    /// Encode UUID bytes with proper byte ordering
    fn encode_uuid_bytes(&mut self, uuid: &[u8; 16]) -> Result<()> {
        // UUID bytes are naturally comparable
        self.buffer.extend_from_slice(uuid);
        Ok(())
    }

    /// Encode TimeUUID with timestamp-first ordering
    fn encode_timeuuid(&mut self, uuid: &Uuid) -> Result<()> {
        let bytes = uuid.as_bytes();
        
        // TimeUUID has timestamp in specific byte positions
        // Rearrange for time-based comparison
        // Time-high: bytes 6-7, time-mid: bytes 4-5, time-low: bytes 0-3
        self.buffer.extend_from_slice(&bytes[6..8]); // time-high
        self.buffer.extend_from_slice(&bytes[4..6]); // time-mid  
        self.buffer.extend_from_slice(&bytes[0..4]); // time-low
        self.buffer.extend_from_slice(&bytes[8..]);  // rest
        Ok(())
    }

    /// Encode timestamp (microseconds since epoch)
    fn encode_timestamp(&mut self, timestamp: i64) -> Result<()> {
        // Encode as signed 64-bit integer
        self.encode_bigint(timestamp)
    }

    /// Encode boolean
    fn encode_boolean(&mut self, value: bool) -> Result<()> {
        self.buffer.push(if value { 0x01 } else { 0x00 });
        Ok(())
    }

    /// Encode float with IEEE 754 ordering adjustment
    fn encode_float(&mut self, value: f32) -> Result<()> {
        let bits = value.to_bits();
        
        // Adjust for proper ordering of IEEE 754 floats
        let adjusted = if (bits & 0x80000000) == 0 {
            // Positive: flip sign bit
            bits ^ 0x80000000
        } else {
            // Negative: flip all bits
            !bits
        };
        
        self.buffer.extend_from_slice(&adjusted.to_be_bytes());
        Ok(())
    }

    /// Encode double with IEEE 754 ordering adjustment
    fn encode_double(&mut self, value: f64) -> Result<()> {
        let bits = value.to_bits();
        
        // Adjust for proper ordering of IEEE 754 doubles
        let adjusted = if (bits & 0x8000000000000000) == 0 {
            // Positive: flip sign bit
            bits ^ 0x8000000000000000
        } else {
            // Negative: flip all bits
            !bits
        };
        
        self.buffer.extend_from_slice(&adjusted.to_be_bytes());
        Ok(())
    }

    /// Encode blob (binary data)
    fn encode_blob(&mut self, bytes: &[u8]) -> Result<()> {
        // Raw bytes with length prefix for proper comparison
        self.buffer.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
        self.buffer.extend_from_slice(bytes);
        Ok(())
    }

    /// Encode list with element-by-element encoding
    fn encode_list(&mut self, items: &[Value]) -> Result<()> {
        // Length prefix
        self.buffer.extend_from_slice(&(items.len() as u32).to_be_bytes());
        
        // Encode each element
        for item in items {
            self.encode_value_to_buffer(item)?;
        }
        Ok(())
    }

    /// Encode set (similar to list but sorted)
    fn encode_set(&mut self, items: &[Value]) -> Result<()> {
        // For byte-comparable encoding, we need to sort the encoded items
        let mut encoded_items = Vec::new();
        
        for item in items {
            let mut encoder = ByteComparableEncoder::new();
            let encoded = encoder.encode_value(item)?;
            encoded_items.push(encoded);
        }
        
        // Sort encoded items lexicographically
        encoded_items.sort();
        
        // Length prefix
        self.buffer.extend_from_slice(&(encoded_items.len() as u32).to_be_bytes());
        
        // Add sorted encoded items
        for encoded_item in encoded_items {
            self.buffer.extend_from_slice(&(encoded_item.len() as u32).to_be_bytes());
            self.buffer.extend_from_slice(&encoded_item);
        }
        Ok(())
    }

    /// Encode map with sorted key-value pairs
    fn encode_map(&mut self, map: &HashMap<Value, Value>) -> Result<()> {
        // Encode key-value pairs and sort by encoded keys
        let mut encoded_pairs = Vec::new();
        
        for (key, value) in map {
            let mut key_encoder = ByteComparableEncoder::new();
            let encoded_key = key_encoder.encode_value(key)?;
            
            let mut value_encoder = ByteComparableEncoder::new();
            let encoded_value = value_encoder.encode_value(value)?;
            
            encoded_pairs.push((encoded_key, encoded_value));
        }
        
        // Sort by encoded keys
        encoded_pairs.sort_by(|a, b| a.0.cmp(&b.0));
        
        // Length prefix
        self.buffer.extend_from_slice(&(encoded_pairs.len() as u32).to_be_bytes());
        
        // Add sorted pairs
        for (encoded_key, encoded_value) in encoded_pairs {
            self.buffer.extend_from_slice(&(encoded_key.len() as u32).to_be_bytes());
            self.buffer.extend_from_slice(&encoded_key);
            self.buffer.extend_from_slice(&(encoded_value.len() as u32).to_be_bytes());
            self.buffer.extend_from_slice(&encoded_value);
        }
        Ok(())
    }

    /// Encode map from Vec of tuples with sorted key-value pairs
    fn encode_map_vec(&mut self, map: &Vec<(Value, Value)>) -> Result<()> {
        // Encode key-value pairs and sort by encoded keys
        let mut encoded_pairs = Vec::new();
        
        for (key, value) in map {
            let mut key_encoder = ByteComparableEncoder::new();
            let encoded_key = key_encoder.encode_value(key)?;
            
            let mut value_encoder = ByteComparableEncoder::new();
            let encoded_value = value_encoder.encode_value(value)?;
            
            encoded_pairs.push((encoded_key, encoded_value));
        }
        
        // Sort by encoded keys
        encoded_pairs.sort_by(|a, b| a.0.cmp(&b.0));
        
        // Length prefix
        self.buffer.extend_from_slice(&(encoded_pairs.len() as u32).to_be_bytes());
        
        // Add sorted pairs
        for (encoded_key, encoded_value) in encoded_pairs {
            self.buffer.extend_from_slice(&(encoded_key.len() as u32).to_be_bytes());
            self.buffer.extend_from_slice(&encoded_key);
            self.buffer.extend_from_slice(&(encoded_value.len() as u32).to_be_bytes());
            self.buffer.extend_from_slice(&encoded_value);
        }
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
        let hex: String = encoded.iter()
            .map(|b| format!("{:02x}", b))
            .collect::<Vec<_>>()
            .join(" ");
        
        // Try to detect if it looks like text
        if let Ok(text) = std::str::from_utf8(encoded) {
            if text.chars().all(|c| c.is_ascii_graphic() || c.is_ascii_whitespace()) {
                return format!("\"{}\" ({})", text.trim_end_matches('\0'), hex);
            }
        }
        
        format!("0x{}", hex)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;
    use std::collections::HashMap;

    #[test]
    fn test_text_encoding() {
        let mut encoder = ByteComparableEncoder::new();
        
        let encoded_a = encoder.encode_value(&Value::Text("a".to_string())).unwrap();
        let encoded_b = encoder.encode_value(&Value::Text("b".to_string())).unwrap();
        let encoded_aa = encoder.encode_value(&Value::Text("aa".to_string())).unwrap();
        
        // Lexicographic comparison should match string comparison
        assert!(encoded_a < encoded_b);
        assert!(encoded_a < encoded_aa);
        assert!(encoded_aa < encoded_b);
    }

    #[test]
    fn test_integer_encoding() {
        let mut encoder = ByteComparableEncoder::new();
        
        let encoded_neg = encoder.encode_value(&Value::Int(-100)).unwrap();
        let encoded_zero = encoder.encode_value(&Value::Int(0)).unwrap();
        let encoded_pos = encoder.encode_value(&Value::Int(100)).unwrap();
        
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
        
        let encoded1 = encoder.encode_value(&Value::Uuid(uuid1)).unwrap();
        let encoded2 = encoder.encode_value(&Value::Uuid(uuid2)).unwrap();
        
        assert!(encoded1 < encoded2);
    }

    #[test]
    fn test_composite_key_encoding() {
        let mut encoder = ByteComparableEncoder::new();
        
        let key1 = vec![
            Value::Text("partition1".to_string()),
            Value::Int(1),
        ];
        let key2 = vec![
            Value::Text("partition1".to_string()),
            Value::Int(2),
        ];
        let key3 = vec![
            Value::Text("partition2".to_string()),
            Value::Int(1),
        ];
        
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
        
        let list1 = Value::List(vec![Value::Int(1), Value::Int(2)]);
        let list2 = Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
        
        let encoded1 = encoder.encode_value(&list1).unwrap();
        let encoded2 = encoder.encode_value(&list2).unwrap();
        
        // Shorter list should come first
        assert!(encoded1 < encoded2);
    }

    #[test]
    fn test_float_special_values() {
        let mut encoder = ByteComparableEncoder::new();
        
        let neg_inf = encoder.encode_value(&Value::Float(f32::NEG_INFINITY)).unwrap();
        let neg_one = encoder.encode_value(&Value::Float(-1.0)).unwrap();
        let zero = encoder.encode_value(&Value::Float(0.0)).unwrap();
        let one = encoder.encode_value(&Value::Float(1.0)).unwrap();
        let pos_inf = encoder.encode_value(&Value::Float(f32::INFINITY)).unwrap();
        
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
        
        let encoded1 = encoder.encode_value(&Value::Text("test1".to_string())).unwrap();
        let encoded2 = encoder.encode_value(&Value::Text("test2".to_string())).unwrap();
        
        // Each encoding should be independent
        assert_ne!(encoded1, encoded2);
        assert!(encoded1 < encoded2);
    }
}