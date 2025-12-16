//! Key parsing functions for composite and clustering keys
//!
//! This module handles parsing of composite keys in various Cassandra formats,
//! including partition keys, clustering keys, and multi-component keys.

use crate::{
    parser::vint::parse_vint_length, schema::TableSchema, types::ComparatorType, Error, Result,
    RowKey,
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
        use crate::parser::vint::parse_vint_length;

        // Check if this format uses byte-comparable encoding
        let is_byte_comparable = matches!(
            self.header.cassandra_version,
            crate::parser::header::CassandraVersion::V5_0NewBigFormat
        );

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
            let decoded_component = self.decode_key_component(key_data, &comparator)?;
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
            let decoded_component = self.decode_key_component(component_data, &comparator)?;
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
                let comparator = ComparatorType::from_data_type(&clustering_column.data_type)
                    .map_err(|e| {
                        Error::Schema(format!(
                            "Invalid clustering key type '{}' - use SchemaAwareReader: {}",
                            clustering_column.data_type, e
                        ))
                    })?;

                // Decode component using exact comparator type
                let decoded_component = self.decode_key_component(component_data, &comparator)?;
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

    /// Decode key component using exact comparator type
    pub(in crate::storage::sstable::reader) fn decode_key_component(
        &self,
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

    /// Parse composite key using Cassandra 5.0+ vint-based format
    #[allow(dead_code)]
    pub(in crate::storage::sstable::reader) fn parse_composite_key_v5_format(
        &self,
        key_data: &[u8],
    ) -> Result<RowKey> {
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
            let (remaining, component_len) =
                parse_vint_length(&key_data[offset..]).map_err(|_| {
                    Error::corruption(format!("Failed to parse component {} length", i))
                })?;
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

    /// Parse composite key using legacy u16-length prefixed format
    #[allow(dead_code)]
    pub(in crate::storage::sstable::reader) fn parse_composite_key_legacy_format(
        &self,
        key_data: &[u8],
    ) -> Result<RowKey> {
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
            let component_len =
                u16::from_be_bytes([key_data[offset], key_data[offset + 1]]) as usize;
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

    /// Parse clustering key format (simpler format for clustering columns)
    #[allow(dead_code)]
    pub(in crate::storage::sstable::reader) fn parse_clustering_key_format(
        &self,
        key_data: &[u8],
    ) -> Result<RowKey> {
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
}
