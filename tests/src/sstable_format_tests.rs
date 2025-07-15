//! SSTable Format Compatibility Tests
//!
//! Comprehensive tests for Cassandra 5+ SSTable format compatibility,
//! including header parsing, data blocks, and metadata validation.

use cqlite_core::parser::header::{
    parse_sstable_header, serialize_sstable_header, ColumnInfo, CompressionInfo, SSTableHeader,
    SSTableStats, SSTABLE_MAGIC, SUPPORTED_VERSION,
};
use cqlite_core::parser::types::{parse_cql_value, serialize_cql_value};
use cqlite_core::parser::vint::{encode_vint, parse_vint};
use cqlite_core::parser::{CqlTypeId, SSTableParser};
use cqlite_core::{error::Result, Value};
use std::collections::HashMap;
use std::io::{Cursor, Read, Write};

/// Comprehensive SSTable format test suite
pub struct SSTableFormatTests {
    parser: SSTableParser,
}

impl SSTableFormatTests {
    pub fn new() -> Self {
        Self {
            parser: SSTableParser::new(),
        }
    }

    /// Run all SSTable format tests
    pub fn run_all_tests(&self) -> Result<()> {
        println!("🔧 Running SSTable Format Compatibility Tests");

        self.test_header_magic_and_version()?;
        self.test_header_serialization_roundtrip()?;
        self.test_compression_info_parsing()?;
        self.test_stats_parsing()?;
        self.test_column_info_parsing()?;
        self.test_properties_parsing()?;
        self.test_malformed_header_handling()?;
        self.test_version_compatibility()?;
        self.test_large_header_handling()?;

        println!("✅ All SSTable format tests completed");
        Ok(())
    }

    /// Test magic number and version parsing
    fn test_header_magic_and_version(&self) -> Result<()> {
        println!("  Testing magic number and version parsing...");

        // Valid magic and version
        let mut valid_data = Vec::new();
        valid_data.extend_from_slice(&SSTABLE_MAGIC.to_be_bytes());
        valid_data.extend_from_slice(&SUPPORTED_VERSION.to_be_bytes());

        let mut cursor = Cursor::new(&valid_data);
        let mut magic_bytes = [0u8; 4];
        let mut version_bytes = [0u8; 2];

        cursor.read_exact(&mut magic_bytes)?;
        cursor.read_exact(&mut version_bytes)?;

        let magic = u32::from_be_bytes(magic_bytes);
        let version = u16::from_be_bytes(version_bytes);

        assert_eq!(magic, SSTABLE_MAGIC, "Magic number mismatch");
        assert_eq!(version, SUPPORTED_VERSION, "Version mismatch");

        // Invalid magic number
        let mut invalid_magic = Vec::new();
        invalid_magic.extend_from_slice(&0xDEADBEEFu32.to_be_bytes());
        invalid_magic.extend_from_slice(&SUPPORTED_VERSION.to_be_bytes());

        // Should fail parsing with invalid magic
        let parse_result = parse_sstable_header(&invalid_magic);
        assert!(parse_result.is_err(), "Should fail with invalid magic");

        println!("    ✓ Magic number and version validation");
        Ok(())
    }

    /// Test header serialization and deserialization roundtrip
    fn test_header_serialization_roundtrip(&self) -> Result<()> {
        println!("  Testing header serialization roundtrip...");

        let original_header = self.create_test_header();

        // Serialize
        let serialized = serialize_sstable_header(&original_header)?;
        assert!(
            !serialized.is_empty(),
            "Serialized data should not be empty"
        );

        // Deserialize
        let (_, parsed_header) = parse_sstable_header(&serialized)
            .map_err(|e| cqlite_core::error::Error::corruption(format!("Parse error: {:?}", e)))?;

        // Validate roundtrip
        assert_eq!(original_header.version, parsed_header.version);
        assert_eq!(original_header.table_id, parsed_header.table_id);
        assert_eq!(original_header.keyspace, parsed_header.keyspace);
        assert_eq!(original_header.table_name, parsed_header.table_name);
        assert_eq!(original_header.generation, parsed_header.generation);
        assert_eq!(
            original_header.compression.algorithm,
            parsed_header.compression.algorithm
        );
        assert_eq!(
            original_header.compression.chunk_size,
            parsed_header.compression.chunk_size
        );
        assert_eq!(
            original_header.stats.row_count,
            parsed_header.stats.row_count
        );
        assert_eq!(original_header.columns.len(), parsed_header.columns.len());

        println!("    ✓ Header serialization roundtrip successful");
        Ok(())
    }

    /// Test compression info parsing
    fn test_compression_info_parsing(&self) -> Result<()> {
        println!("  Testing compression info parsing...");

        // Test different compression algorithms
        let compression_tests = vec![
            ("LZ4", 4096, HashMap::new()),
            ("SNAPPY", 8192, {
                let mut params = HashMap::new();
                params.insert("level".to_string(), "6".to_string());
                params
            }),
            ("NONE", 0, HashMap::new()),
        ];

        for (algorithm, chunk_size, parameters) in compression_tests {
            let compression_info = CompressionInfo {
                algorithm: algorithm.to_string(),
                chunk_size,
                parameters,
            };

            // Test that compression info can be included in header
            let header = SSTableHeader {
                version: SUPPORTED_VERSION,
                table_id: [0u8; 16],
                keyspace: "test".to_string(),
                table_name: "compression_test".to_string(),
                generation: 1,
                compression: compression_info,
                stats: SSTableStats {
                    row_count: 0,
                    min_timestamp: 0,
                    max_timestamp: 0,
                    max_deletion_time: 0,
                    compression_ratio: 0.0,
                    row_size_histogram: Vec::new(),
                },
                columns: Vec::new(),
                properties: HashMap::new(),
            };

            let serialized = serialize_sstable_header(&header)?;
            let (_, parsed) = parse_sstable_header(&serialized).map_err(|e| {
                cqlite_core::error::Error::corruption(format!("Parse error: {:?}", e))
            })?;

            assert_eq!(parsed.compression.algorithm, algorithm);
            assert_eq!(parsed.compression.chunk_size, chunk_size);

            println!("    ✓ Compression algorithm: {}", algorithm);
        }

        Ok(())
    }

    /// Test statistics parsing
    fn test_stats_parsing(&self) -> Result<()> {
        println!("  Testing statistics parsing...");

        let stats = SSTableStats {
            row_count: 1_000_000,
            min_timestamp: 1640995200000000, // 2022-01-01
            max_timestamp: 1672531200000000, // 2023-01-01
            max_deletion_time: 1640995200000000,
            compression_ratio: 0.33,
            row_size_histogram: vec![100, 500, 1000, 2000, 5000],
        };

        let header = SSTableHeader {
            version: SUPPORTED_VERSION,
            table_id: [0u8; 16],
            keyspace: "test".to_string(),
            table_name: "stats_test".to_string(),
            generation: 1,
            compression: CompressionInfo {
                algorithm: "LZ4".to_string(),
                chunk_size: 4096,
                parameters: HashMap::new(),
            },
            stats,
            columns: Vec::new(),
            properties: HashMap::new(),
        };

        let serialized = serialize_sstable_header(&header)?;
        let (_, parsed) = parse_sstable_header(&serialized)
            .map_err(|e| cqlite_core::error::Error::corruption(format!("Parse error: {:?}", e)))?;

        assert_eq!(parsed.stats.row_count, 1_000_000);
        assert_eq!(parsed.stats.min_timestamp, 1640995200000000);
        assert_eq!(parsed.stats.max_timestamp, 1672531200000000);
        assert!((parsed.stats.compression_ratio - 0.33).abs() < 0.001);
        assert_eq!(parsed.stats.row_size_histogram.len(), 5);

        println!("    ✓ Statistics parsing successful");
        Ok(())
    }

    /// Test column info parsing
    fn test_column_info_parsing(&self) -> Result<()> {
        println!("  Testing column info parsing...");

        let columns = vec![
            ColumnInfo {
                name: "id".to_string(),
                column_type: "uuid".to_string(),
                is_primary_key: true,
                key_position: Some(0),
                is_static: false,
                is_clustering: false,
            },
            ColumnInfo {
                name: "timestamp".to_string(),
                column_type: "timestamp".to_string(),
                is_primary_key: true,
                key_position: Some(1),
                is_static: false,
                is_clustering: true,
            },
            ColumnInfo {
                name: "data".to_string(),
                column_type: "text".to_string(),
                is_primary_key: false,
                key_position: None,
                is_static: false,
                is_clustering: false,
            },
            ColumnInfo {
                name: "metadata".to_string(),
                column_type: "map<text, text>".to_string(),
                is_primary_key: false,
                key_position: None,
                is_static: true,
                is_clustering: false,
            },
        ];

        let header = SSTableHeader {
            version: SUPPORTED_VERSION,
            table_id: [0u8; 16],
            keyspace: "test".to_string(),
            table_name: "column_test".to_string(),
            generation: 1,
            compression: CompressionInfo {
                algorithm: "LZ4".to_string(),
                chunk_size: 4096,
                parameters: HashMap::new(),
            },
            stats: SSTableStats {
                row_count: 0,
                min_timestamp: 0,
                max_timestamp: 0,
                max_deletion_time: 0,
                compression_ratio: 0.0,
                row_size_histogram: Vec::new(),
            },
            columns,
            properties: HashMap::new(),
        };

        let serialized = serialize_sstable_header(&header)?;
        let (_, parsed) = parse_sstable_header(&serialized)
            .map_err(|e| cqlite_core::error::Error::corruption(format!("Parse error: {:?}", e)))?;

        assert_eq!(parsed.columns.len(), 4);

        // Validate primary key column
        let pk_column = &parsed.columns[0];
        assert_eq!(pk_column.name, "id");
        assert_eq!(pk_column.column_type, "uuid");
        assert!(pk_column.is_primary_key);
        assert_eq!(pk_column.key_position, Some(0));

        // Validate clustering column
        let clustering_column = &parsed.columns[1];
        assert_eq!(clustering_column.name, "timestamp");
        assert!(clustering_column.is_clustering);
        assert_eq!(clustering_column.key_position, Some(1));

        // Validate static column
        let static_column = &parsed.columns[3];
        assert_eq!(static_column.name, "metadata");
        assert!(static_column.is_static);

        println!("    ✓ Column info parsing successful");
        Ok(())
    }

    /// Test properties parsing
    fn test_properties_parsing(&self) -> Result<()> {
        println!("  Testing properties parsing...");

        let mut properties = HashMap::new();
        properties.insert("bloom_filter_fp_chance".to_string(), "0.01".to_string());
        properties.insert("caching".to_string(), "ALL".to_string());
        properties.insert(
            "comment".to_string(),
            "Test table for compatibility".to_string(),
        );
        properties.insert(
            "compaction".to_string(),
            "SizeTieredCompactionStrategy".to_string(),
        );
        properties.insert("compression".to_string(), "LZ4Compressor".to_string());

        let header = SSTableHeader {
            version: SUPPORTED_VERSION,
            table_id: [0u8; 16],
            keyspace: "test".to_string(),
            table_name: "properties_test".to_string(),
            generation: 1,
            compression: CompressionInfo {
                algorithm: "LZ4".to_string(),
                chunk_size: 4096,
                parameters: HashMap::new(),
            },
            stats: SSTableStats {
                row_count: 0,
                min_timestamp: 0,
                max_timestamp: 0,
                max_deletion_time: 0,
                compression_ratio: 0.0,
                row_size_histogram: Vec::new(),
            },
            columns: Vec::new(),
            properties,
        };

        let serialized = serialize_sstable_header(&header)?;
        let (_, parsed) = parse_sstable_header(&serialized)
            .map_err(|e| cqlite_core::error::Error::corruption(format!("Parse error: {:?}", e)))?;

        assert_eq!(parsed.properties.len(), 5);
        assert_eq!(
            parsed.properties.get("bloom_filter_fp_chance"),
            Some(&"0.01".to_string())
        );
        assert_eq!(parsed.properties.get("caching"), Some(&"ALL".to_string()));
        assert_eq!(
            parsed.properties.get("compaction"),
            Some(&"SizeTieredCompactionStrategy".to_string())
        );

        println!("    ✓ Properties parsing successful");
        Ok(())
    }

    /// Test malformed header handling
    fn test_malformed_header_handling(&self) -> Result<()> {
        println!("  Testing malformed header handling...");

        // Test incomplete header
        let incomplete_data = vec![0x6F, 0x61, 0x00, 0x00]; // Just magic, no version
        let result = parse_sstable_header(&incomplete_data);
        assert!(result.is_err(), "Should fail with incomplete data");

        // Test invalid version
        let mut invalid_version_data = Vec::new();
        invalid_version_data.extend_from_slice(&SSTABLE_MAGIC.to_be_bytes());
        invalid_version_data.extend_from_slice(&0xFFFFu16.to_be_bytes()); // Invalid version

        let result = parse_sstable_header(&invalid_version_data);
        assert!(result.is_err(), "Should fail with invalid version");

        // Test truncated data
        let truncated_data = vec![0x6F, 0x61, 0x00, 0x00, 0x00, 0x01]; // Valid header start but truncated
        let result = parse_sstable_header(&truncated_data);
        assert!(result.is_err(), "Should fail with truncated data");

        println!("    ✓ Malformed header handling successful");
        Ok(())
    }

    /// Test version compatibility
    fn test_version_compatibility(&self) -> Result<()> {
        println!("  Testing version compatibility...");

        // Current implementation only supports version 1
        // Future versions should be handled gracefully

        let mut future_version_data = Vec::new();
        future_version_data.extend_from_slice(&SSTABLE_MAGIC.to_be_bytes());
        future_version_data.extend_from_slice(&0x0002u16.to_be_bytes()); // Future version

        let result = parse_sstable_header(&future_version_data);
        // Should fail for now, but this shows where compatibility handling would go
        assert!(result.is_err(), "Future versions not yet supported");

        println!("    ✓ Version compatibility handling ready");
        Ok(())
    }

    /// Test large header handling
    fn test_large_header_handling(&self) -> Result<()> {
        println!("  Testing large header handling...");

        // Create header with many columns and properties
        let mut columns = Vec::new();
        let mut properties = HashMap::new();

        // Add 100 columns
        for i in 0..100 {
            columns.push(ColumnInfo {
                name: format!("column_{:03}", i),
                column_type: "text".to_string(),
                is_primary_key: i == 0,
                key_position: if i == 0 { Some(0) } else { None },
                is_static: false,
                is_clustering: false,
            });
        }

        // Add 50 properties
        for i in 0..50 {
            properties.insert(format!("property_{:03}", i), format!("value_{:03}", i));
        }

        let large_header = SSTableHeader {
            version: SUPPORTED_VERSION,
            table_id: [0u8; 16],
            keyspace: "test_large".to_string(),
            table_name: "large_header_test".to_string(),
            generation: 1,
            compression: CompressionInfo {
                algorithm: "LZ4".to_string(),
                chunk_size: 4096,
                parameters: HashMap::new(),
            },
            stats: SSTableStats {
                row_count: 0,
                min_timestamp: 0,
                max_timestamp: 0,
                max_deletion_time: 0,
                compression_ratio: 0.0,
                row_size_histogram: Vec::new(),
            },
            columns,
            properties,
        };

        let serialized = serialize_sstable_header(&large_header)?;
        let (_, parsed) = parse_sstable_header(&serialized)
            .map_err(|e| cqlite_core::error::Error::corruption(format!("Parse error: {:?}", e)))?;

        assert_eq!(parsed.columns.len(), 100);
        assert_eq!(parsed.properties.len(), 50);

        println!(
            "    ✓ Large header handling successful ({} bytes)",
            serialized.len()
        );
        Ok(())
    }

    /// Create a test header for testing
    fn create_test_header(&self) -> SSTableHeader {
        let mut properties = HashMap::new();
        properties.insert("test_property".to_string(), "test_value".to_string());

        SSTableHeader {
            version: SUPPORTED_VERSION,
            table_id: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            keyspace: "test_keyspace".to_string(),
            table_name: "test_table".to_string(),
            generation: 42,
            compression: CompressionInfo {
                algorithm: "LZ4".to_string(),
                chunk_size: 4096,
                parameters: HashMap::new(),
            },
            stats: SSTableStats {
                row_count: 1000,
                min_timestamp: 1640995200000000,
                max_timestamp: 1672531200000000,
                max_deletion_time: 0,
                compression_ratio: 0.33,
                row_size_histogram: vec![100, 200, 300, 400, 500],
            },
            columns: vec![
                ColumnInfo {
                    name: "id".to_string(),
                    column_type: "uuid".to_string(),
                    is_primary_key: true,
                    key_position: Some(0),
                    is_static: false,
                    is_clustering: false,
                },
                ColumnInfo {
                    name: "name".to_string(),
                    column_type: "text".to_string(),
                    is_primary_key: false,
                    key_position: None,
                    is_static: false,
                    is_clustering: false,
                },
            ],
            properties,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sstable_format_suite() {
        let tests = SSTableFormatTests::new();
        let result = tests.run_all_tests();
        assert!(result.is_ok(), "SSTable format tests should pass");
    }

    #[test]
    fn test_header_roundtrip() {
        let tests = SSTableFormatTests::new();
        let result = tests.test_header_serialization_roundtrip();
        assert!(result.is_ok(), "Header roundtrip should work");
    }

    #[test]
    fn test_malformed_data() {
        let tests = SSTableFormatTests::new();
        let result = tests.test_malformed_header_handling();
        assert!(result.is_ok(), "Should handle malformed data gracefully");
    }
}
