//! Unit tests for individual header parsing functions
//!
//! This module provides focused unit tests for specific header parsing
//! functions with detailed validation of parsing logic.

use cqlite_core::{
    parser::header::{
        CassandraVersion, ColumnInfo, CompressionInfo, SSTABLE_MAGIC, SSTableHeader, SSTableStats,
        SUPPORTED_VERSION, SUPPORTED_MAGIC_NUMBERS, parse_magic_and_version, parse_vstring,
        parse_compression_info, parse_sstable_stats, parse_column_info, serialize_sstable_header,
    },
    parser::vint::{encode_vint, parse_vint, parse_vint_length},
};
use std::collections::HashMap;

/// Unit tests for magic number and version parsing
#[cfg(test)]
mod magic_version_unit_tests {
    use super::*;

    #[test]
    fn test_parse_magic_and_version_standard_formats() {
        let standard_formats = vec![
            CassandraVersion::Legacy,
            CassandraVersion::V5_0Alpha,
            CassandraVersion::V5_0Beta,
            CassandraVersion::V5_0Release,
            CassandraVersion::V5_0Bti,
        ];

        for version in standard_formats {
            let mut data = Vec::new();
            data.extend_from_slice(&version.magic_number().to_be_bytes());
            data.extend_from_slice(&SUPPORTED_VERSION.to_be_bytes());
            data.extend_from_slice(&[0xAA; 50]); // Test data following header

            let (remaining, (parsed_version, parsed_format_version)) =
                parse_magic_and_version(&data).unwrap();

            assert_eq!(parsed_version, version);
            assert_eq!(parsed_format_version, SUPPORTED_VERSION);
            assert_eq!(remaining.len(), 50, "Should consume exactly 6 bytes");
            assert_eq!(remaining[0], 0xAA, "Remaining data should be untouched");

            println!("✅ Standard format parsing: {:?}", version);
        }
    }

    #[test]
    fn test_parse_magic_and_version_newbig_format() {
        let version = CassandraVersion::V5_0NewBig;
        let mut data = Vec::new();
        data.extend_from_slice(&version.magic_number().to_be_bytes()); // 4 bytes
        data.extend_from_slice(&[0x00; 25]); // 25 bytes padding
        data.extend_from_slice(&SUPPORTED_VERSION.to_be_bytes()); // 2 bytes
        data.extend_from_slice(&[0xBB; 50]); // Test data

        let (remaining, (parsed_version, parsed_format_version)) =
            parse_magic_and_version(&data).unwrap();

        assert_eq!(parsed_version, version);
        assert_eq!(parsed_format_version, SUPPORTED_VERSION);
        assert_eq!(remaining.len(), 50, "Should consume exactly 31 bytes");
        assert_eq!(remaining[0], 0xBB, "Remaining data should be untouched");

        println!("✅ NewBig format parsing successful");
    }

    #[test]
    fn test_parse_magic_and_version_error_cases() {
        let error_cases = vec![
            // Invalid magic numbers
            (vec![0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x01], "Invalid magic number"),
            (vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x01], "Zero magic number"),
            // Truncated data
            (vec![0x6F, 0x61], "Truncated magic"),
            (vec![0x6F, 0x61, 0x00, 0x00], "Magic only"),
            (vec![0x6F, 0x61, 0x00, 0x00, 0x00], "Partial version"),
            // Invalid version
            (vec![0x6F, 0x61, 0x00, 0x00, 0xFF, 0xFF], "Invalid version"),
        ];

        for (data, description) in error_cases {
            let result = parse_magic_and_version(&data);
            assert!(result.is_err(), "Should fail for: {}", description);
            println!("✅ Correctly rejected: {}", description);
        }
    }

    #[test]
    fn test_parse_magic_and_version_exact_boundaries() {
        // Test parsing at exact byte boundaries
        let test_data = vec![
            (6, "Exact standard header size"),
            (31, "Exact NewBig header size"),
        ];

        for (size, description) in test_data {
            let mut data = vec![0x00; size];

            if size >= 6 {
                // Standard format
                data[0..4].copy_from_slice(&SSTABLE_MAGIC.to_be_bytes());
                data[4..6].copy_from_slice(&SUPPORTED_VERSION.to_be_bytes());

                let (remaining, (version, format_version)) =
                    parse_magic_and_version(&data).unwrap();

                assert_eq!(version, CassandraVersion::Legacy);
                assert_eq!(format_version, SUPPORTED_VERSION);
                assert!(remaining.is_empty(), "Should consume all data for {}", description);
            }

            if size >= 31 {
                // NewBig format
                let mut newbig_data = vec![0x00; 31];
                newbig_data[0..4].copy_from_slice(&CassandraVersion::V5_0NewBig.magic_number().to_be_bytes());
                newbig_data[29..31].copy_from_slice(&SUPPORTED_VERSION.to_be_bytes());

                let (remaining, (version, format_version)) =
                    parse_magic_and_version(&newbig_data).unwrap();

                assert_eq!(version, CassandraVersion::V5_0NewBig);
                assert_eq!(format_version, SUPPORTED_VERSION);
                assert!(remaining.is_empty(), "Should consume all NewBig data");
            }

            println!("✅ Boundary test passed: {}", description);
        }
    }
}

/// Unit tests for VString parsing
#[cfg(test)]
mod vstring_unit_tests {
    use super::*;

    #[test]
    fn test_parse_vstring_basic() {
        let test_strings = vec![
            "",
            "a",
            "hello",
            "Hello, World!",
            "🚀 Unicode test ñáéíóú",
            "A".repeat(1000),
        ];

        for test_str in test_strings {
            let mut data = Vec::new();
            data.extend_from_slice(&encode_vint(test_str.len() as i64));
            data.extend_from_slice(test_str.as_bytes());

            let (remaining, parsed) = parse_vstring(&data).unwrap();
            assert_eq!(parsed, test_str);
            assert!(remaining.is_empty());

            println!("✅ VString test passed: '{}' ({} bytes)",
                     if test_str.len() > 20 { &test_str[..20] } else { &test_str },
                     test_str.len());
        }
    }

    #[test]
    fn test_parse_vstring_with_remaining_data() {
        let test_str = "test_string";
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(test_str.len() as i64));
        data.extend_from_slice(test_str.as_bytes());
        data.extend_from_slice(&[0xCC; 20]); // Extra data

        let (remaining, parsed) = parse_vstring(&data).unwrap();
        assert_eq!(parsed, test_str);
        assert_eq!(remaining.len(), 20);
        assert_eq!(remaining[0], 0xCC);

        println!("✅ VString with remaining data test passed");
    }

    #[test]
    fn test_parse_vstring_error_cases() {
        let error_cases = vec![
            // Invalid length
            (vec![], "Empty data"),
            (vec![0xFF], "Invalid VInt"),
            // Length mismatch
            (vec![0x05, b'h', b'i'], "Length too long"),
            // Invalid UTF-8
            (vec![0x02, 0xFF, 0xFE], "Invalid UTF-8"),
        ];

        for (data, description) in error_cases {
            let result = parse_vstring(&data);
            assert!(result.is_err(), "Should fail for: {}", description);
            println!("✅ VString correctly rejected: {}", description);
        }
    }

    #[test]
    fn test_vstring_edge_lengths() {
        let edge_lengths = vec![0, 1, 127, 128, 255, 256, 16383, 16384];

        for length in edge_lengths {
            let test_str = "x".repeat(length);
            let mut data = Vec::new();
            data.extend_from_slice(&encode_vint(length as i64));
            data.extend_from_slice(test_str.as_bytes());

            let (remaining, parsed) = parse_vstring(&data).unwrap();
            assert_eq!(parsed.len(), length);
            assert_eq!(parsed, test_str);
            assert!(remaining.is_empty());

            println!("✅ VString edge length test passed: {} bytes", length);
        }
    }
}

/// Unit tests for compression info parsing
#[cfg(test)]
mod compression_info_unit_tests {
    use super::*;

    #[test]
    fn test_parse_compression_info_basic() {
        let algorithm = "LZ4Compressor";
        let chunk_size = 65536u32;
        let mut params = HashMap::new();
        params.insert("level".to_string(), "9".to_string());
        params.insert("window_size".to_string(), "32768".to_string());

        let mut data = Vec::new();
        // Algorithm name
        data.extend_from_slice(&encode_vint(algorithm.len() as i64));
        data.extend_from_slice(algorithm.as_bytes());
        // Chunk size
        data.extend_from_slice(&chunk_size.to_be_bytes());
        // Parameters count
        data.extend_from_slice(&encode_vint(params.len() as i64));
        // Parameters
        for (key, value) in &params {
            data.extend_from_slice(&encode_vint(key.len() as i64));
            data.extend_from_slice(key.as_bytes());
            data.extend_from_slice(&encode_vint(value.len() as i64));
            data.extend_from_slice(value.as_bytes());
        }

        let (remaining, parsed) = parse_compression_info(&data).unwrap();
        assert_eq!(parsed.algorithm, algorithm);
        assert_eq!(parsed.chunk_size, chunk_size);
        assert_eq!(parsed.parameters.len(), params.len());
        for (key, value) in &params {
            assert_eq!(parsed.parameters.get(key), Some(value));
        }
        assert!(remaining.is_empty());

        println!("✅ Basic compression info parsing passed");
    }

    #[test]
    fn test_parse_compression_info_no_parameters() {
        let algorithm = "NoCompression";
        let chunk_size = 0u32;

        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(algorithm.len() as i64));
        data.extend_from_slice(algorithm.as_bytes());
        data.extend_from_slice(&chunk_size.to_be_bytes());
        data.extend_from_slice(&encode_vint(0i64)); // No parameters

        let (remaining, parsed) = parse_compression_info(&data).unwrap();
        assert_eq!(parsed.algorithm, algorithm);
        assert_eq!(parsed.chunk_size, chunk_size);
        assert!(parsed.parameters.is_empty());
        assert!(remaining.is_empty());

        println!("✅ Compression info with no parameters passed");
    }

    #[test]
    fn test_parse_compression_info_many_parameters() {
        let algorithm = "CustomCompressor";
        let chunk_size = 4096u32;
        let mut params = HashMap::new();

        // Add many parameters
        for i in 0..100 {
            params.insert(format!("param_{}", i), format!("value_{}", i));
        }

        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(algorithm.len() as i64));
        data.extend_from_slice(algorithm.as_bytes());
        data.extend_from_slice(&chunk_size.to_be_bytes());
        data.extend_from_slice(&encode_vint(params.len() as i64));

        for (key, value) in &params {
            data.extend_from_slice(&encode_vint(key.len() as i64));
            data.extend_from_slice(key.as_bytes());
            data.extend_from_slice(&encode_vint(value.len() as i64));
            data.extend_from_slice(value.as_bytes());
        }

        let (remaining, parsed) = parse_compression_info(&data).unwrap();
        assert_eq!(parsed.algorithm, algorithm);
        assert_eq!(parsed.chunk_size, chunk_size);
        assert_eq!(parsed.parameters.len(), 100);
        assert!(remaining.is_empty());

        println!("✅ Compression info with many parameters passed");
    }
}

/// Unit tests for SSTable statistics parsing
#[cfg(test)]
mod sstable_stats_unit_tests {
    use super::*;

    #[test]
    fn test_parse_sstable_stats_basic() {
        let row_count = 1000u64;
        let min_timestamp = -5000i64;
        let max_timestamp = 5000i64;
        let max_deletion_time = 2500i64;
        let compression_ratio = 0.75f64;
        let histogram = vec![10u64, 20, 30, 40, 50];

        let mut data = Vec::new();
        data.extend_from_slice(&row_count.to_be_bytes());
        data.extend_from_slice(&encode_vint(min_timestamp));
        data.extend_from_slice(&encode_vint(max_timestamp));
        data.extend_from_slice(&encode_vint(max_deletion_time));
        data.extend_from_slice(&compression_ratio.to_bits().to_be_bytes());
        data.extend_from_slice(&encode_vint(histogram.len() as i64));
        for &value in &histogram {
            data.extend_from_slice(&value.to_be_bytes());
        }

        let (remaining, parsed) = parse_sstable_stats(&data).unwrap();
        assert_eq!(parsed.row_count, row_count);
        assert_eq!(parsed.min_timestamp, min_timestamp);
        assert_eq!(parsed.max_timestamp, max_timestamp);
        assert_eq!(parsed.max_deletion_time, max_deletion_time);
        assert_eq!(parsed.compression_ratio, compression_ratio);
        assert_eq!(parsed.row_size_histogram, histogram);
        assert!(remaining.is_empty());

        println!("✅ Basic SSTable stats parsing passed");
    }

    #[test]
    fn test_parse_sstable_stats_extreme_values() {
        let extreme_cases = vec![
            (0u64, i64::MIN, i64::MAX, i64::MAX, 0.0f64, vec![]),
            (u64::MAX, 0i64, 0i64, 0i64, 1.0f64, vec![u64::MAX]),
        ];

        for (i, (row_count, min_ts, max_ts, max_del, ratio, histogram)) in extreme_cases.iter().enumerate() {
            let mut data = Vec::new();
            data.extend_from_slice(&row_count.to_be_bytes());
            data.extend_from_slice(&encode_vint(*min_ts));
            data.extend_from_slice(&encode_vint(*max_ts));
            data.extend_from_slice(&encode_vint(*max_del));
            data.extend_from_slice(&ratio.to_bits().to_be_bytes());
            data.extend_from_slice(&encode_vint(histogram.len() as i64));
            for &value in histogram {
                data.extend_from_slice(&value.to_be_bytes());
            }

            let (remaining, parsed) = parse_sstable_stats(&data).unwrap();
            assert_eq!(parsed.row_count, *row_count);
            assert_eq!(parsed.min_timestamp, *min_ts);
            assert_eq!(parsed.max_timestamp, *max_ts);
            assert_eq!(parsed.max_deletion_time, *max_del);
            assert_eq!(parsed.compression_ratio, *ratio);
            assert_eq!(parsed.row_size_histogram, *histogram);
            assert!(remaining.is_empty());

            println!("✅ Extreme values test case {} passed", i);
        }
    }

    #[test]
    fn test_parse_sstable_stats_large_histogram() {
        let row_count = 500u64;
        let min_timestamp = 1000i64;
        let max_timestamp = 2000i64;
        let max_deletion_time = 1500i64;
        let compression_ratio = 0.5f64;
        let histogram: Vec<u64> = (0..10000).collect(); // Large histogram

        let mut data = Vec::new();
        data.extend_from_slice(&row_count.to_be_bytes());
        data.extend_from_slice(&encode_vint(min_timestamp));
        data.extend_from_slice(&encode_vint(max_timestamp));
        data.extend_from_slice(&encode_vint(max_deletion_time));
        data.extend_from_slice(&compression_ratio.to_bits().to_be_bytes());
        data.extend_from_slice(&encode_vint(histogram.len() as i64));
        for &value in &histogram {
            data.extend_from_slice(&value.to_be_bytes());
        }

        let (remaining, parsed) = parse_sstable_stats(&data).unwrap();
        assert_eq!(parsed.row_size_histogram, histogram);
        assert!(remaining.is_empty());

        println!("✅ Large histogram parsing passed ({} entries)", histogram.len());
    }
}

/// Unit tests for column info parsing
#[cfg(test)]
mod column_info_unit_tests {
    use super::*;

    #[test]
    fn test_parse_column_info_primary_key() {
        let name = "id";
        let column_type = "uuid";
        let is_primary_key = true;
        let key_position = 0u16;
        let is_static = false;
        let is_clustering = false;

        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(name.len() as i64));
        data.extend_from_slice(name.as_bytes());
        data.extend_from_slice(&encode_vint(column_type.len() as i64));
        data.extend_from_slice(column_type.as_bytes());

        let flags = 0x01; // is_primary_key
        data.push(flags);
        data.extend_from_slice(&key_position.to_be_bytes());

        let (remaining, parsed) = parse_column_info(&data).unwrap();
        assert_eq!(parsed.name, name);
        assert_eq!(parsed.column_type, column_type);
        assert_eq!(parsed.is_primary_key, is_primary_key);
        assert_eq!(parsed.key_position, Some(key_position));
        assert_eq!(parsed.is_static, is_static);
        assert_eq!(parsed.is_clustering, is_clustering);
        assert!(remaining.is_empty());

        println!("✅ Primary key column parsing passed");
    }

    #[test]
    fn test_parse_column_info_regular_column() {
        let name = "data";
        let column_type = "text";
        let is_primary_key = false;
        let is_static = false;
        let is_clustering = false;

        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(name.len() as i64));
        data.extend_from_slice(name.as_bytes());
        data.extend_from_slice(&encode_vint(column_type.len() as i64));
        data.extend_from_slice(column_type.as_bytes());

        let flags = 0x00; // No flags set
        data.push(flags);

        let (remaining, parsed) = parse_column_info(&data).unwrap();
        assert_eq!(parsed.name, name);
        assert_eq!(parsed.column_type, column_type);
        assert_eq!(parsed.is_primary_key, is_primary_key);
        assert_eq!(parsed.key_position, None);
        assert_eq!(parsed.is_static, is_static);
        assert_eq!(parsed.is_clustering, is_clustering);
        assert!(remaining.is_empty());

        println!("✅ Regular column parsing passed");
    }

    #[test]
    fn test_parse_column_info_all_flags() {
        let name = "special_column";
        let column_type = "custom_type";
        let key_position = 5u16;

        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(name.len() as i64));
        data.extend_from_slice(name.as_bytes());
        data.extend_from_slice(&encode_vint(column_type.len() as i64));
        data.extend_from_slice(column_type.as_bytes());

        let flags = 0x01 | 0x02 | 0x04; // All flags set
        data.push(flags);
        data.extend_from_slice(&key_position.to_be_bytes());

        let (remaining, parsed) = parse_column_info(&data).unwrap();
        assert_eq!(parsed.name, name);
        assert_eq!(parsed.column_type, column_type);
        assert_eq!(parsed.is_primary_key, true);
        assert_eq!(parsed.key_position, Some(key_position));
        assert_eq!(parsed.is_static, true);
        assert_eq!(parsed.is_clustering, true);
        assert!(remaining.is_empty());

        println!("✅ All flags column parsing passed");
    }

    #[test]
    fn test_parse_column_info_unicode_names() {
        let name = "столбец_🚀";
        let column_type = "тип_данных";

        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(name.len() as i64));
        data.extend_from_slice(name.as_bytes());
        data.extend_from_slice(&encode_vint(column_type.len() as i64));
        data.extend_from_slice(column_type.as_bytes());
        data.push(0x00); // No flags

        let (remaining, parsed) = parse_column_info(&data).unwrap();
        assert_eq!(parsed.name, name);
        assert_eq!(parsed.column_type, column_type);
        assert!(remaining.is_empty());

        println!("✅ Unicode column names parsing passed");
    }
}

/// Unit tests for full header parsing functions
#[cfg(test)]
mod full_header_unit_tests {
    use super::*;

    #[test]
    fn test_parse_complete_header_minimal() {
        let header = SSTableHeader {
            cassandra_version: CassandraVersion::Legacy,
            version: SUPPORTED_VERSION,
            table_id: [0; 16],
            keyspace: "ks".to_string(),
            table_name: "tbl".to_string(),
            generation: 1,
            compression: CompressionInfo {
                algorithm: "NONE".to_string(),
                chunk_size: 0,
                parameters: HashMap::new(),
            },
            stats: SSTableStats {
                row_count: 0,
                min_timestamp: 0,
                max_timestamp: 0,
                max_deletion_time: 0,
                compression_ratio: 1.0,
                row_size_histogram: vec![],
            },
            columns: vec![],
            properties: HashMap::new(),
        };

        let serialized = serialize_sstable_header(&header).unwrap();
        let (remaining, parsed) = cqlite_core::parser::header::parse_sstable_header(&serialized).unwrap();

        assert_eq!(parsed.cassandra_version, header.cassandra_version);
        assert_eq!(parsed.version, header.version);
        assert_eq!(parsed.table_id, header.table_id);
        assert_eq!(parsed.keyspace, header.keyspace);
        assert_eq!(parsed.table_name, header.table_name);
        assert_eq!(parsed.generation, header.generation);
        assert!(remaining.is_empty());

        println!("✅ Minimal header round-trip parsing passed");
    }

    #[test]
    fn test_parse_complete_header_maximal() {
        let mut properties = HashMap::new();
        for i in 0..50 {
            properties.insert(format!("prop_{}", i), format!("value_{}", i));
        }

        let mut columns = Vec::new();
        for i in 0..20 {
            columns.push(ColumnInfo {
                name: format!("col_{i}"),
                column_type: format!("type_{}", i),
                is_primary_key: i < 3,
                key_position: if i < 3 { Some(i as u16) } else { None },
                is_static: i % 5 == 0,
                is_clustering: i % 3 == 0,
                clustering_reversed: false,            });
        }

        let header = SSTableHeader {
            cassandra_version: CassandraVersion::V5_0Beta,
            version: SUPPORTED_VERSION,
            table_id: [0xFF; 16],
            keyspace: "test_keyspace_with_unicode_🔥".to_string(),
            table_name: "test_table_αβγδ".to_string(),
            generation: u64::MAX,
            compression: CompressionInfo {
                algorithm: "LZ4Compressor".to_string(),
                chunk_size: 65536,
                parameters: {
                    let mut params = HashMap::new();
                    params.insert("level".to_string(), "9".to_string());
                    params.insert("window_size".to_string(), "32768".to_string());
                    params
                },
            },
            stats: SSTableStats {
                row_count: u64::MAX,
                min_timestamp: i64::MIN,
                max_timestamp: i64::MAX,
                max_deletion_time: i64::MAX,
                compression_ratio: 0.123456789,
                row_size_histogram: (0..1000).collect(),
            },
            columns,
            properties,
        };

        let serialized = serialize_sstable_header(&header).unwrap();
        let (remaining, parsed) = cqlite_core::parser::header::parse_sstable_header(&serialized).unwrap();

        // Verify all fields
        assert_eq!(parsed.cassandra_version, header.cassandra_version);
        assert_eq!(parsed.version, header.version);
        assert_eq!(parsed.table_id, header.table_id);
        assert_eq!(parsed.keyspace, header.keyspace);
        assert_eq!(parsed.table_name, header.table_name);
        assert_eq!(parsed.generation, header.generation);
        assert_eq!(parsed.compression.algorithm, header.compression.algorithm);
        assert_eq!(parsed.stats.row_count, header.stats.row_count);
        assert_eq!(parsed.columns.len(), header.columns.len());
        assert_eq!(parsed.properties.len(), header.properties.len());
        assert!(remaining.is_empty());

        println!("✅ Maximal header round-trip parsing passed");
        println!("   Header size: {} bytes", serialized.len());
        println!("   Columns: {}", parsed.columns.len());
        println!("   Properties: {}", parsed.properties.len());
    }

    #[test]
    fn test_parse_header_with_remaining_data() {
        let header = SSTableHeader {
            cassandra_version: CassandraVersion::Legacy,
            version: SUPPORTED_VERSION,
            table_id: [1; 16],
            keyspace: "test_ks".to_string(),
            table_name: "test_table".to_string(),
            generation: 42,
            compression: CompressionInfo {
                algorithm: "LZ4".to_string(),
                chunk_size: 4096,
                parameters: HashMap::new(),
            },
            stats: SSTableStats::default(),
            columns: vec![],
            properties: HashMap::new(),
        };

        let mut data = serialize_sstable_header(&header).unwrap();
        let trailing_data = vec![0xDD; 500];
        data.extend_from_slice(&trailing_data);

        let (remaining, parsed) = cqlite_core::parser::header::parse_sstable_header(&data).unwrap();

        assert_eq!(parsed.keyspace, header.keyspace);
        assert_eq!(remaining.len(), 500);
        assert_eq!(remaining[0], 0xDD);

        println!("✅ Header parsing with remaining data passed");
    }
}

/// Property-based unit tests
#[cfg(test)]
mod property_unit_tests {
    use super::*;

    #[test]
    fn test_vint_roundtrip_property() {
        let test_values = vec![
            0i64, 1, -1, 127, -127, 128, -128,
            16383, -16383, 16384, -16384,
            i64::MAX, i64::MIN,
        ];

        for value in test_values {
            let encoded = encode_vint(value);
            let (remaining, decoded) = parse_vint(&encoded).unwrap();

            assert_eq!(decoded, value, "VInt round-trip failed for {}", value);
            assert!(remaining.is_empty(), "VInt should consume all data");

            println!("✅ VInt round-trip: {} -> {} bytes -> {}", value, encoded.len(), decoded);
        }
    }

    #[test]
    fn test_magic_number_bijection_property() {
        // Property: magic_number(from_magic_number(x)) == x for all valid x
        for &magic in SUPPORTED_MAGIC_NUMBERS {
            let version = CassandraVersion::from_magic_number(magic).unwrap();
            let round_trip_magic = version.magic_number();

            assert_eq!(round_trip_magic, magic,
                      "Magic number bijection failed for 0x{:08X}", magic);
        }

        println!("✅ Magic number bijection property verified");
    }

    #[test]
    fn test_header_serialization_size_property() {
        // Property: Header size should be predictable based on content
        let base_header = SSTableHeader {
            cassandra_version: CassandraVersion::Legacy,
            version: SUPPORTED_VERSION,
            table_id: [0; 16],
            keyspace: "".to_string(),
            table_name: "".to_string(),
            generation: 0,
            compression: CompressionInfo {
                algorithm: "".to_string(),
                chunk_size: 0,
                parameters: HashMap::new(),
            },
            stats: SSTableStats::default(),
            columns: vec![],
            properties: HashMap::new(),
        };

        let base_size = serialize_sstable_header(&base_header).unwrap().len();
        println!("Base header size: {} bytes", base_size);

        // Test size growth with content
        let size_tests = vec![
            ("keyspace", 10),
            ("table_name", 20),
            ("compression_algorithm", 30),
        ];

        for (field, extra_chars) in size_tests {
            let mut test_header = base_header.clone();
            let extra_string = "x".repeat(extra_chars);

            match field {
                "keyspace" => test_header.keyspace = extra_string,
                "table_name" => test_header.table_name = extra_string,
                "compression_algorithm" => test_header.compression.algorithm = extra_string,
                _ => unreachable!(),
            }

            let test_size = serialize_sstable_header(&test_header).unwrap().len();
            let growth = test_size - base_size;

            println!("✅ Size growth for {} (+{} chars): +{} bytes",
                     field, extra_chars, growth);

            // Size should grow reasonably (at least the string length + VInt overhead)
            assert!(growth >= extra_chars,
                   "Size should grow by at least the string length");
            assert!(growth <= extra_chars + 10,
                   "Size growth should not be excessive");
        }
    }

    #[test]
    fn test_parse_stability_property() {
        // Property: parse(serialize(parse(data))) should equal parse(data)
        let original_header = SSTableHeader {
            cassandra_version: CassandraVersion::V5_0Release,
            version: SUPPORTED_VERSION,
            table_id: [42; 16],
            keyspace: "stability_test".to_string(),
            table_name: "stability_table".to_string(),
            generation: 12345,
            compression: CompressionInfo {
                algorithm: "TestCompressor".to_string(),
                chunk_size: 8192,
                parameters: {
                    let mut params = HashMap::new();
                    params.insert("test_param".to_string(), "test_value".to_string());
                    params
                },
            },
            stats: SSTableStats {
                row_count: 100,
                min_timestamp: 1000,
                max_timestamp: 2000,
                max_deletion_time: 1500,
                compression_ratio: 0.8,
                row_size_histogram: vec![1, 2, 3, 4, 5],
            },
            columns: vec![
                ColumnInfo {
                    name: "test_col".to_string(),
                    column_type: "test_type".to_string(),
                    is_primary_key: true,
                    key_position: Some(0),
                    is_static: false,
                    is_clustering: false,
                    clustering_reversed: false,                }
            ],
            properties: {
                let mut props = HashMap::new();
                props.insert("test_prop".to_string(), "test_prop_value".to_string());
                props
            },
        };

        // Serialize -> Parse -> Serialize -> Parse
        let serialized1 = serialize_sstable_header(&original_header).unwrap();
        let (_, parsed1) = cqlite_core::parser::header::parse_sstable_header(&serialized1).unwrap();
        let serialized2 = serialize_sstable_header(&parsed1).unwrap();
        let (_, parsed2) = cqlite_core::parser::header::parse_sstable_header(&serialized2).unwrap();

        // Key fields should be stable
        assert_eq!(parsed1.cassandra_version, parsed2.cassandra_version);
        assert_eq!(parsed1.version, parsed2.version);
        assert_eq!(parsed1.table_id, parsed2.table_id);
        assert_eq!(parsed1.keyspace, parsed2.keyspace);
        assert_eq!(parsed1.table_name, parsed2.table_name);
        assert_eq!(parsed1.generation, parsed2.generation);

        println!("✅ Parse stability property verified");
    }
}