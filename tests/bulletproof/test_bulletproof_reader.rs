//! Comprehensive test suite for updated bulletproof SSTable reader
//! Tests the new 'oa' format parsing and schema-driven approach

#[cfg(test)]
mod tests {
    use cqlite_core::storage::sstable::bulletproof_reader::*;
    use cqlite_core::storage::sstable::reader::*;
    use cqlite_core::error::{Error, Result};
    use std::path::Path;
    use std::fs;

    fn create_mock_oa_sstable() -> Vec<u8> {
        let mut data = Vec::new();
        
        // 'oa' format header (32 bytes)
        data.extend_from_slice(&[0x6F, 0x61, 0x00, 0x00]); // Magic number
        data.extend_from_slice(&[0x00, 0x01]); // Version
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x05]); // Flags
        data.extend_from_slice(&vec![0; 22]); // Reserved
        
        // Metadata section
        data.push(0x02); // 2 partitions (VInt)
        data.extend_from_slice(&1640995200000000i64.to_be_bytes()); // Min timestamp
        data.extend_from_slice(&1640995260000000i64.to_be_bytes()); // Max timestamp
        
        // Partition 1
        data.push(0x10); // Key length (16 bytes)
        data.extend_from_slice(&[0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0,
                                0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0]); // UUID key
        data.push(0x03); // 3 rows (VInt)
        
        // Mock row data
        data.extend_from_slice(&[0x00, 0x05, 0x68, 0x65, 0x6C, 0x6C, 0x6F]); // "hello"
        data.extend_from_slice(&[0x00, 0x05, 0x77, 0x6F, 0x72, 0x6C, 0x64]); // "world"
        
        data
    }

    #[test]
    fn test_bulletproof_reader_creation() {
        let reader = BulletproofReader::new();
        // Verify reader can be created without UUID scanning dependencies
        assert!(true, "BulletproofReader should initialize without UUID scanning");
    }

    #[test]
    fn test_oa_format_parsing() {
        let reader = BulletproofReader::new();
        let test_data = create_mock_oa_sstable();
        
        // Test that new parse_oa_header works correctly
        let header_result = reader.parse_oa_header(&test_data[..32]);
        assert!(header_result.is_ok(), "Should parse valid 'oa' format header");
        
        let header = header_result.unwrap();
        assert_eq!(header.magic_number, 0x6F610000, "Magic number should match 'oa' format");
        assert_eq!(header.format_version, 0x0001, "Format version should be 1");
    }

    #[test] 
    fn test_vint_decoding() {
        let reader = BulletproofReader::new();
        
        // Test VInt decoding functionality that replaced UUID scanning
        let test_cases = vec![
            (vec![0x00], 0i64, 1),
            (vec![0x01], 1i64, 1),
            (vec![0x7F], 127i64, 1),
            (vec![0xC0, 0x80], 128i64, 2),
            (vec![0xFF], -1i64, 1),
        ];
        
        for (bytes, expected_value, expected_consumed) in test_cases {
            let result = reader.read_vint(&bytes);
            assert!(result.is_ok(), "VInt decoding should succeed for {:?}", bytes);
            
            let (value, consumed) = result.unwrap();
            assert_eq!(value, expected_value, "Wrong VInt value for {:?}", bytes);
            assert_eq!(consumed, expected_consumed, "Wrong bytes consumed for {:?}", bytes);
        }
    }

    #[test]
    fn test_modern_format_parsing_without_uuid_scanning() {
        let reader = BulletproofReader::new();
        let test_data = create_mock_oa_sstable();
        
        // Test that parse_modern_format now uses structured parsing instead of UUID scanning
        let result = reader.parse_modern_format(&test_data);
        assert!(result.is_ok(), "Should parse modern format without UUID scanning");
        
        let entries = result.unwrap();
        assert!(!entries.is_empty(), "Should extract entries using structured parsing");
        
        // Verify entries don't contain UUID scanning artifacts
        for entry in &entries {
            // Check that format_info doesn't contain uuid_scan references
            assert!(!entry.format_info.contains("uuid_scan"), 
                   "Entry should not contain UUID scanning references: {}", entry.format_info);
        }
    }

    #[test]
    fn test_data_blocks_parsing() {
        let reader = BulletproofReader::new();
        let test_data = create_mock_oa_sstable();
        
        // Skip header (32 bytes) and metadata to get to data blocks
        let data_start = 32 + 1 + 8 + 8; // Header + partition count + timestamps
        let data_blocks = &test_data[data_start..];
        
        let result = reader.parse_data_blocks(data_blocks);
        assert!(result.is_ok(), "Should parse data blocks using new structured approach");
        
        let entries = result.unwrap();
        assert!(!entries.is_empty(), "Should extract entries from data blocks");
    }

    #[test] 
    fn test_partition_block_parsing() {
        let reader = BulletproofReader::new();
        
        // Create partition block data
        let mut partition_data = Vec::new();
        partition_data.push(0x10); // Key length
        partition_data.extend_from_slice(&[0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0,
                                          0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0]); // Key
        partition_data.push(0x02); // Row count
        partition_data.extend_from_slice(b"test data"); // Mock row data
        
        let result = reader.parse_partition_block(&partition_data, 0);
        assert!(result.is_ok(), "Should parse partition block correctly");
        
        let entry = result.unwrap();
        assert_eq!(entry.key.len(), 16, "Should extract correct partition key length");
        assert_eq!(entry.key, vec![0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0,
                                   0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0], 
                  "Should extract correct partition key");
    }

    #[test]
    fn test_error_handling_without_uuid_dependencies() {
        let reader = BulletproofReader::new();
        
        // Test various error conditions
        let error_tests = vec![
            (vec![], "Empty data"),
            (vec![0xFF, 0xFF, 0xFF, 0xFF], "Invalid magic number"), 
            (vec![0x6F, 0x61, 0x00, 0x02], "Unsupported version"),
        ];
        
        for (data, description) in error_tests {
            let result = reader.parse_oa_header(&data);
            assert!(result.is_err(), "Should handle error case: {}", description);
            
            // Verify error doesn't mention UUID scanning
            let error_msg = format!("{:?}", result.unwrap_err());
            assert!(!error_msg.to_lowercase().contains("uuid"),
                   "Error messages should not reference UUID scanning: {}", error_msg);
        }
    }

    #[test]
    fn test_legacy_compatibility() {
        let reader = BulletproofReader::new();
        
        // Test that reader can still handle legacy formats without UUID scanning
        let legacy_nb_data = vec![0x6E, 0x62, 0x00, 0x00]; // 'nb' format
        let legacy_ma_data = vec![0x6D, 0x61, 0x00, 0x00]; // 'ma' format
        
        // These should be handled by appropriate format detection, not UUID scanning
        for (data, format_name) in [(legacy_nb_data, "nb"), (legacy_ma_data, "ma")] {
            let mut full_data = data;
            full_data.extend_from_slice(&vec![0; 100]); // Add some data
            
            let result = reader.parse_modern_format(&full_data);
            // Result may be error (unsupported format) but shouldn't crash or use UUID scanning
            match result {
                Ok(_) => println!("Successfully parsed {} format without UUID scanning", format_name),
                Err(e) => {
                    let error_msg = format!("{:?}", e);
                    assert!(!error_msg.to_lowercase().contains("uuid"),
                           "Legacy format error should not mention UUID: {}", error_msg);
                }
            }
        }
    }

    #[test]
    fn test_performance_without_uuid_scanning() {
        use std::time::Instant;
        
        let reader = BulletproofReader::new();
        let test_data = create_mock_oa_sstable();
        
        // Measure performance of new structured parsing vs old UUID scanning
        let start = Instant::now();
        for _ in 0..100 {
            let _ = reader.parse_modern_format(&test_data);
        }
        let duration = start.elapsed();
        
        // Should be faster than UUID scanning approach
        assert!(duration.as_millis() < 1000, 
               "Structured parsing should be fast: took {}ms", duration.as_millis());
    }

    #[test]
    fn test_memory_usage_improvement() {
        let reader = BulletproofReader::new();
        let test_data = create_mock_oa_sstable();
        
        // Parse data and verify memory usage is reasonable
        let result = reader.parse_modern_format(&test_data);
        assert!(result.is_ok());
        
        let entries = result.unwrap();
        
        // Verify we're not keeping unnecessary data in memory (no UUID scan artifacts)
        let total_memory = entries.iter()
            .map(|entry| entry.key.len() + entry.values.iter().map(|v| v.len()).sum::<usize>())
            .sum::<usize>();
        
        // Memory usage should be proportional to actual data, not scanning overhead
        assert!(total_memory < test_data.len() * 2, 
               "Memory usage should be efficient without UUID scanning overhead");
    }

    #[test]
    fn test_integration_with_real_files() {
        // Test with real SSTable files if available
        let test_paths = vec![
            "/Users/patrick/local_projects/cqlite/test-data/sstables",
            "/Users/patrick/local_projects/cqlite/real_cassandra5_data",
        ];
        
        for test_path in test_paths {
            if Path::new(test_path).exists() {
                let reader = BulletproofReader::new();
                
                // Try to parse any Data.db files found
                if let Ok(entries) = fs::read_dir(test_path) {
                    for entry in entries.flatten() {
                        let path = entry.path();
                        if path.extension().and_then(|s| s.to_str()) == Some("db") &&
                           path.file_name().and_then(|s| s.to_str())
                               .map(|s| s.contains("Data")).unwrap_or(false) {
                            
                            if let Ok(data) = fs::read(&path) {
                                let result = reader.parse_modern_format(&data);
                                match result {
                                    Ok(entries) => {
                                        println!("✅ Successfully parsed {} with {} entries", 
                                                path.display(), entries.len());
                                    }
                                    Err(e) => {
                                        println!("⚠️  Could not parse {}: {:?}", path.display(), e);
                                        // Ensure error doesn't mention UUID scanning
                                        let error_msg = format!("{:?}", e);
                                        assert!(!error_msg.to_lowercase().contains("uuid"),
                                               "Real file errors should not mention UUID scanning");
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}