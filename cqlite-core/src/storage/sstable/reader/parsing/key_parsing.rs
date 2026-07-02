//! Key parsing functions for composite and clustering keys
//!
//! This module handles parsing of composite keys in various Cassandra formats,
//! including partition keys, clustering keys, and multi-component keys.

use crate::{
    parser::{header::CassandraVersion, vint::parse_vint_length},
    schema::TableSchema,
    types::ComparatorType,
    Error, Result, RowKey,
};

use super::super::types::SSTableReader;

impl SSTableReader {
    /// Enhanced composite key parsing for Cassandra 5.0 multi-component keys with improved format detection
    pub(in crate::storage::sstable::reader) fn parse_composite_key(
        &self,
        key_data: &[u8],
        schema: Option<&crate::schema::TableSchema>,
    ) -> Result<RowKey> {
        if key_data.is_empty() {
            return Ok(RowKey::new(Vec::new()));
        }

        // SCHEMA-DRIVEN KEY PARSING: Use exact comparator types when available
        if let Some(schema) = self.get_table_schema(schema) {
            return self.parse_key_with_schema(key_data, &schema);
        }

        // Modern formats should never reach this non-schema fallback path
        match self.header.cassandra_version {
            crate::parser::header::CassandraVersion::V5_0NewBig
            | crate::parser::header::CassandraVersion::V5_0Bti
            | crate::parser::header::CassandraVersion::V5_0NewBigFormat
            | crate::parser::header::CassandraVersion::V5_0Uncompressed
            | crate::parser::header::CassandraVersion::V5_0ComplexTypes
            | crate::parser::header::CassandraVersion::V5_0TypedCollections
            | crate::parser::header::CassandraVersion::V5_0WideRows
            | crate::parser::header::CassandraVersion::V5_0FormatG => Err(Error::Schema(format!(
                "Non-schema key parsing fallback not allowed for modern format {:?}. \
                     Use SchemaAwareReader with proper schema registry.",
                self.header.cassandra_version
            ))),
            _ => {
                // Legacy formats can return raw key data as last resort
                #[cfg(feature = "legacy-heuristics")]
                {
                    log::warn!(
                        "No schema available - returning raw key data for key of length {} (use SchemaAwareReader)",
                        key_data.len()
                    );
                    Ok(RowKey::new(key_data.to_vec()))
                }
                #[cfg(not(feature = "legacy-heuristics"))]
                {
                    Err(Error::Schema(
                        "Non-schema key parsing requires legacy-heuristics feature for legacy compatibility.".to_string()
                    ))
                }
            }
        }
    }

    /// Parse key using exact schema information (NO HEURISTICS)
    pub(in crate::storage::sstable::reader) fn parse_key_with_schema(
        &self,
        key_data: &[u8],
        schema: &TableSchema,
    ) -> Result<RowKey> {
        parse_key_with_schema_impl(key_data, schema, self.header.cassandra_version)
    }

    /// Decode key component using exact comparator type
    #[allow(dead_code)]
    pub(in crate::storage::sstable::reader) fn decode_key_component(
        &self,
        component_data: &[u8],
        comparator: &ComparatorType,
    ) -> Result<Vec<u8>> {
        decode_key_component_impl(component_data, comparator)
    }

    /// Parse composite key using Cassandra 5.0+ vint-based format
    #[allow(dead_code)]
    pub(in crate::storage::sstable::reader) fn parse_composite_key_v5_format(
        &self,
        key_data: &[u8],
    ) -> Result<RowKey> {
        parse_composite_key_v5_format_impl(key_data)
    }

    /// Parse composite key using legacy u16-length prefixed format
    #[allow(dead_code)]
    pub(in crate::storage::sstable::reader) fn parse_composite_key_legacy_format(
        &self,
        key_data: &[u8],
    ) -> Result<RowKey> {
        parse_composite_key_legacy_format_impl(key_data)
    }

    /// Parse clustering key format (simpler format for clustering columns)
    #[allow(dead_code)]
    pub(in crate::storage::sstable::reader) fn parse_clustering_key_format(
        &self,
        key_data: &[u8],
    ) -> Result<RowKey> {
        parse_clustering_key_format_impl(key_data)
    }
}

// ============================================================================
// Pure Functions for Testing (Issue #260)
// ============================================================================

/// Decode key component using exact comparator type (pure function for testing)
pub(crate) fn decode_key_component_impl(
    component_data: &[u8],
    comparator: &ComparatorType,
) -> Result<Vec<u8>> {
    // For key components, we typically preserve the byte-comparable encoding
    // but can validate format based on comparator type

    match comparator {
        ComparatorType::Uuid => {
            if component_data.len() != 16 {
                return Err(Error::corruption("Invalid UUID key component length"));
            }
        }
        ComparatorType::Int => {
            if component_data.len() != 4 {
                return Err(Error::corruption("Invalid Int key component length"));
            }
        }
        ComparatorType::BigInt => {
            if component_data.len() != 8 {
                return Err(Error::corruption("Invalid BigInt key component length"));
            }
        }
        ComparatorType::Text => {
            // Validate UTF-8 for text keys
            if std::str::from_utf8(component_data).is_err() {
                return Err(Error::corruption("Invalid UTF-8 in text key component"));
            }
        }
        _ => {
            // For other types, accept as-is for now
        }
    }

    // Return the byte-comparable encoding as-is
    // The comparator validation ensures format correctness
    Ok(component_data.to_vec())
}

/// Parse composite key using Cassandra 5.0+ vint-based format (pure function for testing)
pub(crate) fn parse_composite_key_v5_format_impl(key_data: &[u8]) -> Result<RowKey> {
    if key_data.len() < 2 {
        return Err(Error::corruption("Key too short for v5 format".to_string()));
    }

    let mut components = Vec::new();

    // Parse component count (vint)
    let (remaining, component_count) = parse_vint_length(key_data)
        .map_err(|_| Error::corruption("Failed to parse component count".to_string()))?;
    let mut offset = key_data.len() - remaining.len();

    if component_count == 0 || component_count > 256 {
        return Err(Error::corruption(format!(
            "Invalid component count: {}",
            component_count
        )));
    }

    log::debug!(
        "Parsing v5 composite key with {} components",
        component_count
    );

    // Parse each component
    for i in 0..component_count {
        if offset >= key_data.len() {
            break;
        }

        // Parse component length (vint)
        let (remaining, component_len) = parse_vint_length(&key_data[offset..])
            .map_err(|_| Error::corruption(format!("Failed to parse component {} length", i)))?;
        offset = key_data.len() - remaining.len();

        if component_len > 0 && offset + component_len <= key_data.len() {
            components.extend_from_slice(&key_data[offset..offset + component_len]);
            offset += component_len;

            // Add component separator (except for last component)
            if i < component_count - 1 {
                components.push(0x00);
            }
        }
    }

    log::debug!("Parsed v5 composite key: {} total bytes", components.len());
    Ok(RowKey::new(components))
}

/// Parse composite key using legacy u16-length prefixed format (pure function for testing)
pub(crate) fn parse_composite_key_legacy_format_impl(key_data: &[u8]) -> Result<RowKey> {
    if key_data.len() < 3 || key_data[0] != 0x00 {
        return Err(Error::corruption(
            "Not legacy composite key format".to_string(),
        ));
    }

    let mut offset = 0;
    let mut components = Vec::new();

    while offset < key_data.len() {
        if offset + 2 > key_data.len() {
            break;
        }

        // Read component length (big-endian u16)
        let component_len = u16::from_be_bytes([key_data[offset], key_data[offset + 1]]) as usize;
        offset += 2;

        if offset + component_len > key_data.len() {
            break;
        }

        components.extend_from_slice(&key_data[offset..offset + component_len]);
        components.push(0x00); // Component separator
        offset += component_len;

        // Check for end-of-components marker
        if offset < key_data.len() && key_data[offset] == 0x00 {
            break;
        }
    }

    // Remove trailing separator if present
    if components.last() == Some(&0x00) {
        components.pop();
    }

    log::debug!(
        "Parsed legacy composite key: {} total bytes",
        components.len()
    );
    Ok(RowKey::new(components))
}

/// Parse clustering key format (pure function for testing)
pub(crate) fn parse_clustering_key_format_impl(key_data: &[u8]) -> Result<RowKey> {
    // Clustering keys in Cassandra 5.0 might use a different format
    // Check for clustering key markers or patterns

    if key_data.len() < 4 {
        return Err(Error::corruption(
            "Too short for clustering key".to_string(),
        ));
    }

    // Check if this looks like a clustering key by analyzing the structure
    // Clustering keys often have type info followed by the actual key data
    if key_data[0] <= 0x1F {
        // Potential type marker
        let mut offset = 1;

        // Skip type information
        while offset < key_data.len() && key_data[offset] <= 0x1F {
            offset += 1;
        }

        if offset < key_data.len() {
            let clustering_data = &key_data[offset..];
            log::debug!(
                "Parsed clustering key: {} bytes after {} byte type prefix",
                clustering_data.len(),
                offset
            );
            return Ok(RowKey::new(clustering_data.to_vec()));
        }
    }

    Err(Error::corruption("Not clustering key format".to_string()))
}

/// Parse key using exact schema information (pure function for testing)
pub(crate) fn parse_key_with_schema_impl(
    key_data: &[u8],
    schema: &TableSchema,
    cassandra_version: CassandraVersion,
) -> Result<RowKey> {
    use crate::parser::vint::parse_vint_length;

    // Check if this format uses byte-comparable encoding
    let is_byte_comparable = matches!(cassandra_version, CassandraVersion::V5_0NewBigFormat);

    if is_byte_comparable {
        // Use byte-comparable decoder
        let (_, components) = super::byte_comparable::decode_byte_comparable_key(key_data)
            .map_err(|_| Error::corruption("Failed to decode byte-comparable partition key"))?;

        log::debug!(
            "parse_key_with_schema: Decoded {} byte-comparable components",
            components.len()
        );

        // Build compound key from decoded components
        let total_size: usize = components.iter().map(|c| c.len()).sum();
        let mut compound_key_data = Vec::with_capacity(total_size);
        for component in components {
            compound_key_data.extend_from_slice(&component);
        }

        return Ok(RowKey::new(compound_key_data));
    }

    // Handle single-component partition keys (no VInt prefix - raw bytes)
    // Multi-component (composite) partition keys have VInt length prefixes per component
    if schema.partition_keys.len() == 1 {
        // Single partition key column: key_data is the raw value (no length prefix)
        let partition_column = &schema.partition_keys[0];
        let comparator =
            ComparatorType::from_data_type(&partition_column.data_type).map_err(|e| {
                Error::Schema(format!(
                    "Invalid partition key type '{}': {}",
                    partition_column.data_type, e
                ))
            })?;

        log::debug!(
            "parse_key_with_schema: Single partition key column '{}' ({}), raw key length: {}",
            partition_column.name,
            partition_column.data_type,
            key_data.len()
        );

        // Decode and return the single component directly
        let decoded_component = decode_key_component_impl(key_data, &comparator)?;
        return Ok(RowKey::new(decoded_component));
    }

    // Multi-component (composite) partition keys: VInt-based parsing
    let mut offset = 0;
    let mut key_components = Vec::new();

    log::debug!(
        "parse_key_with_schema: Composite partition key with {} components, key length: {}",
        schema.partition_keys.len(),
        key_data.len()
    );

    // Parse partition key components using exact comparator types
    for partition_column in &schema.partition_keys {
        if offset >= key_data.len() {
            break;
        }

        // Parse component length (vint) - only for composite keys
        let (remaining, component_len) = parse_vint_length(&key_data[offset..])
            .map_err(|_| Error::corruption("Failed to parse partition key component length"))?;
        offset = key_data.len() - remaining.len();

        if component_len > remaining.len() {
            return Err(Error::corruption(
                "Partition key component length exceeds available data",
            ));
        }

        // Extract component data
        let component_data = &remaining[..component_len];

        let comparator =
            ComparatorType::from_data_type(&partition_column.data_type).map_err(|e| {
                Error::Schema(format!(
                    "Invalid partition key type '{}': {}",
                    partition_column.data_type, e
                ))
            })?;

        // Decode component using exact comparator type
        let decoded_component = decode_key_component_impl(component_data, &comparator)?;
        key_components.push(decoded_component);

        offset += component_len;
    }

    // Parse clustering key components if present
    if offset < key_data.len() {
        for clustering_column in &schema.clustering_keys {
            if offset >= key_data.len() {
                break;
            }

            // Parse component length (vint)
            let (remaining, component_len) =
                parse_vint_length(&key_data[offset..]).map_err(|_| {
                    Error::corruption("Failed to parse clustering key component length")
                })?;
            offset = key_data.len() - remaining.len();

            if component_len > remaining.len() {
                return Err(Error::corruption(
                    "Clustering key component length exceeds available data",
                ));
            }

            // Extract component data
            let component_data = &remaining[..component_len];

            // DEPRECATED: This should use SchemaAwareReader with proper comparators
            let comparator =
                ComparatorType::from_data_type(&clustering_column.data_type).map_err(|e| {
                    Error::Schema(format!(
                        "Invalid clustering key type '{}' - use SchemaAwareReader: {}",
                        clustering_column.data_type, e
                    ))
                })?;

            // Decode component using exact comparator type
            let decoded_component = decode_key_component_impl(component_data, &comparator)?;
            key_components.push(decoded_component);

            offset += component_len;
        }
    }

    // Create compound key from decoded components
    let total_size: usize = key_components.iter().map(|c| c.len()).sum();
    let mut compound_key_data = Vec::with_capacity(total_size);
    for component in key_components {
        compound_key_data.extend_from_slice(&component);
    }

    Ok(RowKey::new(compound_key_data))
}

// ============================================================================
// Unit Tests (Issue #260)
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::vint::encode_vuint;
    use crate::schema::KeyColumn;
    use std::collections::HashMap;

    // ========================================================================
    // Test Helper Functions
    // ========================================================================

    /// Create a test schema with a single partition key
    fn create_test_schema(partition_key_type: &str) -> TableSchema {
        TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: partition_key_type.to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }

    /// Create a test schema with multiple partition key components
    fn create_composite_key_schema(types: &[&str]) -> TableSchema {
        TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: types
                .iter()
                .enumerate()
                .map(|(i, t)| KeyColumn {
                    name: format!("key{}", i),
                    data_type: t.to_string(),
                    position: i,
                })
                .collect(),
            clustering_keys: vec![],
            columns: vec![],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }

    /// Build a V5 composite key with VInt-encoded component count and lengths
    fn build_v5_composite_key(components: &[&[u8]]) -> Vec<u8> {
        let mut key = Vec::new();

        // Encode component count
        key.extend_from_slice(&encode_vuint(components.len() as u64));

        // Encode each component with its length
        for component in components {
            key.extend_from_slice(&encode_vuint(component.len() as u64));
            key.extend_from_slice(component);
        }

        key
    }

    // ========================================================================
    // Single-Component Key Tests
    // ========================================================================

    #[test]
    fn test_parse_composite_key_single_uuid() {
        let uuid_bytes = [
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
            0x77, 0x88,
        ];
        let schema = create_test_schema("uuid");

        let result = parse_key_with_schema_impl(&uuid_bytes, &schema, CassandraVersion::V5_0NewBig);
        assert!(result.is_ok());
        let key = result.unwrap();
        assert_eq!(key.as_bytes(), &uuid_bytes);
    }

    #[test]
    fn test_parse_composite_key_single_int() {
        let int_bytes = [0x00, 0x00, 0x00, 0x42]; // Big-endian 66
        let schema = create_test_schema("int");

        let result = parse_key_with_schema_impl(&int_bytes, &schema, CassandraVersion::V5_0NewBig);
        assert!(result.is_ok());
        let key = result.unwrap();
        assert_eq!(key.as_bytes(), &int_bytes);
    }

    #[test]
    fn test_parse_composite_key_single_text() {
        let text_bytes = b"hello";
        let schema = create_test_schema("text");

        let result = parse_key_with_schema_impl(text_bytes, &schema, CassandraVersion::V5_0NewBig);
        assert!(result.is_ok());
        let key = result.unwrap();
        assert_eq!(key.as_bytes(), text_bytes);
    }

    // ========================================================================
    // Multi-Component Key Tests
    // ========================================================================

    #[test]
    fn test_parse_composite_key_multi_component() {
        let uuid_bytes = [
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
            0x77, 0x88,
        ];
        let int_bytes = [0x00, 0x00, 0x00, 0x42];

        let mut key_data = Vec::new();
        // Add UUID component with VInt length
        key_data.extend_from_slice(&encode_vuint(uuid_bytes.len() as u64));
        key_data.extend_from_slice(&uuid_bytes);
        // Add Int component with VInt length
        key_data.extend_from_slice(&encode_vuint(int_bytes.len() as u64));
        key_data.extend_from_slice(&int_bytes);

        let schema = create_composite_key_schema(&["uuid", "int"]);

        let result = parse_key_with_schema_impl(&key_data, &schema, CassandraVersion::V5_0NewBig);
        assert!(result.is_ok());
        let key = result.unwrap();

        // Should contain both components concatenated
        let mut expected = Vec::new();
        expected.extend_from_slice(&uuid_bytes);
        expected.extend_from_slice(&int_bytes);
        assert_eq!(key.as_bytes(), &expected);
    }

    #[test]
    fn test_parse_key_with_schema_composite() {
        let text_bytes = b"key1";
        let bigint_bytes = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x23];

        let mut key_data = Vec::new();
        key_data.extend_from_slice(&encode_vuint(text_bytes.len() as u64));
        key_data.extend_from_slice(text_bytes);
        key_data.extend_from_slice(&encode_vuint(bigint_bytes.len() as u64));
        key_data.extend_from_slice(&bigint_bytes);

        let schema = create_composite_key_schema(&["text", "bigint"]);

        let result = parse_key_with_schema_impl(&key_data, &schema, CassandraVersion::V5_0NewBig);
        assert!(result.is_ok());
        let key = result.unwrap();

        let mut expected = Vec::new();
        expected.extend_from_slice(text_bytes);
        expected.extend_from_slice(&bigint_bytes);
        assert_eq!(key.as_bytes(), &expected);
    }

    // ========================================================================
    // Component Validation Tests
    // ========================================================================

    #[test]
    fn test_decode_key_component_uuid_valid() {
        let uuid_bytes = [
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
            0x77, 0x88,
        ];

        let result = decode_key_component_impl(&uuid_bytes, &ComparatorType::Uuid);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), uuid_bytes);
    }

    #[test]
    fn test_decode_key_component_uuid_invalid_length() {
        let invalid_uuid = [0x12, 0x34, 0x56, 0x78]; // Only 4 bytes

        let result = decode_key_component_impl(&invalid_uuid, &ComparatorType::Uuid);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("UUID"));
    }

    #[test]
    fn test_decode_key_component_int_valid() {
        let int_bytes = [0x00, 0x00, 0x00, 0x42];

        let result = decode_key_component_impl(&int_bytes, &ComparatorType::Int);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), int_bytes);
    }

    #[test]
    fn test_decode_key_component_bigint_valid() {
        let bigint_bytes = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x23];

        let result = decode_key_component_impl(&bigint_bytes, &ComparatorType::BigInt);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), bigint_bytes);
    }

    #[test]
    fn test_decode_key_component_text_valid_utf8() {
        let text_bytes = b"hello world";

        let result = decode_key_component_impl(text_bytes, &ComparatorType::Text);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), text_bytes);
    }

    #[test]
    fn test_decode_key_component_text_invalid_utf8() {
        let invalid_utf8 = [0xFF, 0xFE, 0xFD]; // Invalid UTF-8 sequence

        let result = decode_key_component_impl(&invalid_utf8, &ComparatorType::Text);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("UTF-8"));
    }

    // ========================================================================
    // Format-Specific Tests
    // ========================================================================

    #[test]
    fn test_parse_composite_key_v5_format_single() {
        let component = b"test";
        let key_data = build_v5_composite_key(&[component]);

        let result = parse_composite_key_v5_format_impl(&key_data);
        assert!(result.is_ok());
        let key = result.unwrap();
        assert_eq!(key.as_bytes(), component);
    }

    #[test]
    fn test_parse_composite_key_v5_format_multiple() {
        let comp1 = b"first";
        let comp2 = b"second";
        let comp3 = b"third";
        let key_data = build_v5_composite_key(&[comp1, comp2, comp3]);

        let result = parse_composite_key_v5_format_impl(&key_data);
        assert!(result.is_ok());
        let key = result.unwrap();

        // Components are separated by 0x00
        let expected = b"first\x00second\x00third";
        assert_eq!(key.as_bytes(), expected);
    }

    #[test]
    fn test_parse_composite_key_legacy_format() {
        // Legacy format: requires first byte to be 0x00
        // Then u16 BE length prefixes for each component
        let comp1 = b"hi"; // Short component so length fits in one byte

        let mut key_data = Vec::new();
        // First component: length 0x0002 (u16 BE, first byte 0x00)
        key_data.extend_from_slice(&[0x00, 0x02]); // Length = 2
        key_data.extend_from_slice(comp1); // "hi"
                                           // End marker
        key_data.push(0x00);

        let result = parse_composite_key_legacy_format_impl(&key_data);
        assert!(result.is_ok(), "Failed to parse: {:?}", result);
        let key = result.unwrap();
        // Should just be the component data (separator added then removed)
        assert_eq!(key.as_bytes(), comp1);
    }

    #[test]
    fn test_parse_clustering_key_format() {
        let key_data = [
            0x01, // Type marker
            0x02, // Type info
            0x68, 0x65, 0x6c, 0x6c, 0x6f, // "hello"
        ];

        let result = parse_clustering_key_format_impl(&key_data);
        assert!(result.is_ok());
        let key = result.unwrap();
        assert_eq!(key.as_bytes(), b"hello");
    }

    // ========================================================================
    // Error Case Tests
    // ========================================================================

    #[test]
    fn test_parse_composite_key_empty() {
        let empty_data: &[u8] = &[];
        let schema = create_test_schema("uuid");

        let result = parse_key_with_schema_impl(empty_data, &schema, CassandraVersion::V5_0NewBig);
        // Empty key should fail validation
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_composite_key_too_large() {
        // Create a key with 300 components (>256 limit)
        let mut key_data = Vec::new();
        key_data.extend_from_slice(&encode_vuint(300));

        let result = parse_composite_key_v5_format_impl(&key_data);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("component count"));
    }

    #[test]
    fn test_parse_v5_format_key_too_short() {
        let short_data = [0x01]; // Only 1 byte

        let result = parse_composite_key_v5_format_impl(&short_data);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too short"));
    }

    #[test]
    fn test_decode_key_component_int_wrong_length() {
        let wrong_length = [0x00, 0x00, 0x42]; // 3 bytes instead of 4

        let result = decode_key_component_impl(&wrong_length, &ComparatorType::Int);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Int"));
    }

    #[test]
    fn test_decode_key_component_bigint_wrong_length() {
        let wrong_length = [0x00, 0x00, 0x00, 0x00, 0x01, 0x23]; // 6 bytes instead of 8

        let result = decode_key_component_impl(&wrong_length, &ComparatorType::BigInt);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("BigInt"));
    }

    // ========================================================================
    // Additional Edge Case Tests
    // ========================================================================

    #[test]
    fn test_parse_composite_key_v5_format_zero_components() {
        // Build proper v5 format with zero components
        // encode_vuint(0) = [0x00] (1 byte), but we need at least 2 bytes
        // So let's add a dummy byte to pass length check
        let mut key_data = encode_vuint(0); // Zero components
        key_data.push(0x00); // Add extra byte to pass length check

        let result = parse_composite_key_v5_format_impl(&key_data);
        assert!(result.is_err());
        let err = result.unwrap_err();
        // Error should be about invalid component count
        assert!(
            err.to_string().contains("component count") || err.to_string().contains("Invalid"),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_parse_clustering_key_format_too_short() {
        let short_data = [0x01, 0x02]; // Less than 4 bytes

        let result = parse_clustering_key_format_impl(&short_data);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Too short"));
    }

    #[test]
    fn test_parse_clustering_key_format_invalid() {
        let invalid_data = [0xFF, 0xFF, 0xFF, 0xFF]; // No valid type marker

        let result = parse_clustering_key_format_impl(&invalid_data);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Not clustering key format"));
    }

    #[test]
    fn test_decode_key_component_other_types() {
        // Test that other comparator types are accepted
        let arbitrary_data = [0x01, 0x02, 0x03, 0x04, 0x05];

        let result = decode_key_component_impl(&arbitrary_data, &ComparatorType::Blob);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), arbitrary_data);
    }

    #[test]
    fn test_parse_composite_key_v5_format_component_length_overflow() {
        let mut key_data = Vec::new();
        key_data.extend_from_slice(&encode_vuint(1)); // 1 component
        key_data.extend_from_slice(&encode_vuint(1000)); // Component length > remaining data
        key_data.extend_from_slice(b"short"); // Only 5 bytes

        let result = parse_composite_key_v5_format_impl(&key_data);
        // Should parse but get truncated/empty result
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_legacy_format_not_legacy() {
        // First byte is not 0x00
        let not_legacy = [0x01, 0x00, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f];

        let result = parse_composite_key_legacy_format_impl(&not_legacy);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Not legacy"));
    }

    // Issue #264: Schema-required error path test
    #[test]
    fn test_key_parsing_schema_required_error() {
        // Test that modern formats correctly validate schema requirements
        // UUID type requires exactly 16 bytes - empty data should fail with schema error

        let schema = create_test_schema("uuid");
        let empty_data: &[u8] = &[];

        // Empty key with UUID schema should fail
        let result = parse_key_with_schema_impl(empty_data, &schema, CassandraVersion::V5_0NewBig);
        assert!(result.is_err(), "Empty key with UUID schema should fail");

        let err = result.unwrap_err();
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("Invalid") || err_msg.contains("UUID") || err_msg.contains("length"),
            "Error should be descriptive about UUID validation, got: {}",
            err_msg
        );

        // Also test with wrong-sized data (15 bytes instead of 16)
        let short_uuid = [
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
            0x77,
        ];
        let result = parse_key_with_schema_impl(&short_uuid, &schema, CassandraVersion::V5_0NewBig);
        assert!(result.is_err(), "Short UUID should fail validation");

        // Test with int type requiring exactly 4 bytes but given 2
        let int_schema = create_test_schema("int");
        let short_int = [0x00, 0x42];
        let result =
            parse_key_with_schema_impl(&short_int, &int_schema, CassandraVersion::V5_0NewBig);
        assert!(result.is_err(), "Short int should fail validation");
    }
}
