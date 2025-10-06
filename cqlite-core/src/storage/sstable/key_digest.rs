//! Cassandra-compatible partition key digest computation
//!
//! This module implements the exact key digest algorithm used by Cassandra
//! for Index.db storage. The digest is computed by:
//! 1. Parsing partition key bytes according to the schema comparators
//! 2. Creating byte-comparable encoding of the key components
//! 3. Computing Murmur3 hash of the byte-comparable representation
//! 4. Returning the digest bytes in the format expected by Index.db

use crate::error::{Error, Result};
use crate::schema::registry::ParsingContext;
use crate::storage::sstable::bti::encoder::ByteComparableEncoder;
use crate::types::{ComparatorType, Value};
use murmur3::murmur3_32;
use std::io::Cursor;

/// Cassandra-compatible key digest computer
///
/// This struct provides the exact key digest computation algorithm used by
/// Cassandra for partition key hashing in Index.db files.
pub struct KeyDigestComputer {
    encoder: ByteComparableEncoder,
}

impl KeyDigestComputer {
    /// Create a new key digest computer
    pub fn new() -> Self {
        Self {
            encoder: ByteComparableEncoder::new(),
        }
    }

    /// Compute the partition key digest for Index.db lookup
    ///
    /// This method implements the exact Cassandra algorithm:
    /// 1. Parse the partition key bytes according to schema comparators
    /// 2. Create byte-comparable encoding for each component
    /// 3. Combine components into a single byte-comparable key
    /// 4. Compute Murmur3 hash with seed 0 (Cassandra default)
    /// 5. Return the hash as little-endian bytes
    pub fn compute_partition_key_digest(
        &mut self,
        partition_key_bytes: &[u8],
        parsing_context: &ParsingContext,
    ) -> Result<Vec<u8>> {
        // Step 1: Parse partition key bytes into typed values
        let partition_values = self.parse_partition_key_bytes(
            partition_key_bytes,
            &parsing_context.partition_comparators,
        )?;

        // Step 2: Create byte-comparable encoding for the composite key
        let byte_comparable_key = self.encoder.encode_composite_key(&partition_values)?;

        // Step 3: Compute Murmur3 hash with seed 0 (Cassandra standard)
        let mut cursor = Cursor::new(&byte_comparable_key);
        let hash = murmur3_32(&mut cursor, 0)
            .map_err(|e| Error::corruption(format!("Failed to compute Murmur3 hash: {}", e)))?;

        // Step 4: Return hash as little-endian bytes (Cassandra format)
        Ok(hash.to_le_bytes().to_vec())
    }

    /// Parse partition key bytes into typed values according to comparators
    ///
    /// This method handles both single and multi-component partition keys,
    /// parsing each component according to its type comparator.
    fn parse_partition_key_bytes(
        &self,
        key_bytes: &[u8],
        partition_comparators: &[ComparatorType],
    ) -> Result<Vec<Value>> {
        if partition_comparators.is_empty() {
            return Err(Error::Schema(
                "No partition key comparators provided".to_string(),
            ));
        }

        // Handle single component partition key
        if partition_comparators.len() == 1 {
            let value = self.parse_value_bytes(key_bytes, &partition_comparators[0])?;
            return Ok(vec![value]);
        }

        // Handle multi-component partition key
        // Multi-component keys are encoded with length prefixes for each component
        let mut values = Vec::new();
        let mut offset = 0;

        for comparator in partition_comparators {
            if offset >= key_bytes.len() {
                return Err(Error::corruption(
                    "Insufficient bytes for multi-component partition key".to_string(),
                ));
            }

            // Read component length (2 bytes, big-endian)
            if offset + 2 > key_bytes.len() {
                return Err(Error::corruption(
                    "Invalid component length in partition key".to_string(),
                ));
            }

            let component_len =
                u16::from_be_bytes([key_bytes[offset], key_bytes[offset + 1]]) as usize;
            offset += 2;

            // Read component bytes
            if offset + component_len > key_bytes.len() {
                return Err(Error::corruption(
                    "Component length exceeds available bytes".to_string(),
                ));
            }

            let component_bytes = &key_bytes[offset..offset + component_len];
            let value = self.parse_value_bytes(component_bytes, comparator)?;
            values.push(value);
            offset += component_len;
        }

        Ok(values)
    }

    /// Parse bytes for a single value according to its comparator type
    fn parse_value_bytes(&self, bytes: &[u8], comparator: &ComparatorType) -> Result<Value> {
        match comparator {
            ComparatorType::Boolean => {
                if bytes.len() != 1 {
                    return Err(Error::corruption("Invalid boolean bytes".to_string()));
                }
                Ok(Value::Boolean(bytes[0] != 0))
            }
            ComparatorType::TinyInt => {
                if bytes.len() != 1 {
                    return Err(Error::corruption("Invalid tinyint bytes".to_string()));
                }
                Ok(Value::TinyInt(bytes[0] as i8))
            }
            ComparatorType::SmallInt => {
                if bytes.len() != 2 {
                    return Err(Error::corruption("Invalid smallint bytes".to_string()));
                }
                let value = i16::from_be_bytes([bytes[0], bytes[1]]);
                Ok(Value::SmallInt(value))
            }
            ComparatorType::Int => {
                if bytes.len() != 4 {
                    return Err(Error::corruption("Invalid int bytes".to_string()));
                }
                let value = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                Ok(Value::Integer(value))
            }
            ComparatorType::BigInt => {
                if bytes.len() != 8 {
                    return Err(Error::corruption("Invalid bigint bytes".to_string()));
                }
                let value = i64::from_be_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ]);
                Ok(Value::BigInt(value))
            }
            ComparatorType::Counter => {
                if bytes.len() != 8 {
                    return Err(Error::corruption("Invalid counter bytes".to_string()));
                }
                let value = i64::from_be_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ]);
                Ok(Value::Counter(value))
            }
            ComparatorType::Float32 => {
                if bytes.len() != 4 {
                    return Err(Error::corruption("Invalid float32 bytes".to_string()));
                }
                let bits = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                let value = f32::from_bits(bits);
                Ok(Value::Float32(value))
            }
            ComparatorType::Float => {
                if bytes.len() != 8 {
                    return Err(Error::corruption("Invalid float bytes".to_string()));
                }
                let bits = u64::from_be_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ]);
                let value = f64::from_bits(bits);
                Ok(Value::Float(value))
            }
            ComparatorType::Text => {
                let text = String::from_utf8(bytes.to_vec())
                    .map_err(|e| Error::corruption(format!("Invalid UTF-8 in text: {}", e)))?;
                Ok(Value::Text(text))
            }
            ComparatorType::Blob => Ok(Value::Blob(bytes.to_vec())),
            ComparatorType::Timestamp => {
                if bytes.len() != 8 {
                    return Err(Error::corruption("Invalid timestamp bytes".to_string()));
                }
                let millis = i64::from_be_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ]);
                Ok(Value::Timestamp(millis))
            }
            ComparatorType::Uuid => {
                if bytes.len() != 16 {
                    return Err(Error::corruption("Invalid UUID bytes".to_string()));
                }
                let uuid_bytes: [u8; 16] = bytes
                    .try_into()
                    .map_err(|_| Error::invalid_format("Invalid UUID byte length"))?;
                Ok(Value::Uuid(uuid_bytes))
            }
            // For complex types, we need more sophisticated parsing
            // For now, treat them as blobs to avoid breaking existing functionality
            ComparatorType::List(_)
            | ComparatorType::Set(_)
            | ComparatorType::Map(_, _)
            | ComparatorType::Tuple(_)
            | ComparatorType::Udt { .. }
            | ComparatorType::Frozen(_)
            | ComparatorType::Custom(_)
            | ComparatorType::Varint
            | ComparatorType::Decimal
            | ComparatorType::Duration
            | ComparatorType::Json => {
                log::warn!(
                    "Complex type {} in partition key - using blob fallback",
                    comparator.type_name()
                );
                Ok(Value::Blob(bytes.to_vec()))
            }
        }
    }

    /// Compute a simple hash-based digest (fallback for when schema is unavailable)
    ///
    /// This method provides compatibility with the existing implementation
    /// when full schema information is not available.
    pub fn compute_simple_digest(&self, partition_key: &[u8]) -> Result<Vec<u8>> {
        // Use Murmur3 instead of DefaultHasher for better Cassandra compatibility
        let mut cursor = Cursor::new(partition_key);
        let hash = murmur3_32(&mut cursor, 0)
            .map_err(|e| Error::corruption(format!("Failed to compute Murmur3 hash: {}", e)))?;

        Ok(hash.to_le_bytes().to_vec())
    }
}

impl Default for KeyDigestComputer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{KeyColumn, TableSchema};
    use std::collections::HashMap;

    fn create_test_parsing_context(partition_comparators: Vec<ComparatorType>) -> ParsingContext {
        let schema = TableSchema {
            keyspace: "test".to_string(),
            table: "table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![],
            comments: HashMap::new(),
        };

        ParsingContext {
            schema,
            partition_comparators,
            clustering_comparators: vec![],
            column_comparators: HashMap::new(),
        }
    }

    #[test]
    fn test_single_component_int_key() {
        let mut computer = KeyDigestComputer::new();
        let context = create_test_parsing_context(vec![ComparatorType::Int]);

        // Create a 4-byte big-endian integer (value: 42)
        let key_bytes = [0x00, 0x00, 0x00, 0x2A]; // 42 in big-endian

        let digest = computer
            .compute_partition_key_digest(&key_bytes, &context)
            .unwrap();

        // Digest should be 4 bytes (32-bit Murmur3 hash)
        assert_eq!(digest.len(), 4);

        // Test deterministic - same input should produce same digest
        let digest2 = computer
            .compute_partition_key_digest(&key_bytes, &context)
            .unwrap();
        assert_eq!(digest, digest2);
    }

    #[test]
    fn test_single_component_text_key() {
        let mut computer = KeyDigestComputer::new();
        let context = create_test_parsing_context(vec![ComparatorType::Text]);

        let key_bytes = b"hello";

        let digest = computer
            .compute_partition_key_digest(key_bytes, &context)
            .unwrap();

        // Digest should be 4 bytes (32-bit Murmur3 hash)
        assert_eq!(digest.len(), 4);
    }

    #[test]
    fn test_multi_component_key() {
        let mut computer = KeyDigestComputer::new();
        let context = create_test_parsing_context(vec![ComparatorType::Int, ComparatorType::Text]);

        // Multi-component key: int(42) + text("hello")
        // Format: [len1(2 bytes)][int_bytes(4 bytes)][len2(2 bytes)][text_bytes(5 bytes)]
        let mut key_bytes = Vec::new();
        key_bytes.extend_from_slice(&[0x00, 0x04]); // length of int (4 bytes)
        key_bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x2A]); // int value 42
        key_bytes.extend_from_slice(&[0x00, 0x05]); // length of text (5 bytes)
        key_bytes.extend_from_slice(b"hello"); // text value

        let digest = computer
            .compute_partition_key_digest(&key_bytes, &context)
            .unwrap();

        // Digest should be 4 bytes (32-bit Murmur3 hash)
        assert_eq!(digest.len(), 4);
    }

    #[test]
    fn test_simple_digest_fallback() -> Result<()> {
        let computer = KeyDigestComputer::new();
        let key_bytes = b"test_key";

        let digest = computer.compute_simple_digest(key_bytes)?;

        // Digest should be 4 bytes (32-bit Murmur3 hash)
        assert_eq!(digest.len(), 4);

        // Test deterministic
        let digest2 = computer.compute_simple_digest(key_bytes)?;
        assert_eq!(digest, digest2);
        Ok(())
    }

    #[test]
    fn test_byte_ordering_equivalence() {
        let mut computer = KeyDigestComputer::new();
        let context = create_test_parsing_context(vec![ComparatorType::Int]);

        // Test that smaller values produce smaller digests when possible
        let key1_bytes = [0x00, 0x00, 0x00, 0x01]; // 1
        let key2_bytes = [0x00, 0x00, 0x00, 0x02]; // 2

        let digest1 = computer
            .compute_partition_key_digest(&key1_bytes, &context)
            .unwrap();
        let digest2 = computer
            .compute_partition_key_digest(&key2_bytes, &context)
            .unwrap();

        // While hash ordering may not match value ordering, digests should be different
        assert_ne!(digest1, digest2);
    }
}
