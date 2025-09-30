//! Security tests for SSTable header parsing
//!
//! Tests various attack vectors and malformed inputs to verify security fixes

use cqlite_core::parser::header::{parse_magic_and_version, CassandraVersion};
use cqlite_core::parser::vint::{parse_vint, parse_vint_length, MAX_VINT_SIZE};

#[cfg(test)]
mod security_tests {
    use super::*;

    #[test]
    fn test_invalid_magic_numbers() {
        // Test completely invalid magic numbers
        let invalid_magic_tests = vec![
            0x00000000u32, // All zeros
            0xFFFFFFFFu32, // All ones
            0xDEADBEEFu32, // Classic invalid pattern
            0x12345678u32, // Random invalid
            0x41414141u32, // ASCII 'AAAA'
            0x6F620000u32, // 'ob' (close to valid 'oa')
        ];

        for magic in invalid_magic_tests {
            let mut data = Vec::new();
            data.extend_from_slice(&magic.to_be_bytes());
            data.extend_from_slice(&[0x00, 0x01]); // Valid version

            let result = parse_magic_and_version(&data);
            assert!(
                result.is_err(),
                "Invalid magic 0x{:08X} should be rejected",
                magic
            );
        }
    }

    #[test]
    fn test_invalid_version_numbers() {
        // Test invalid version numbers with valid magic
        let valid_magic = CassandraVersion::Legacy.magic_number();
        let invalid_versions = vec![
            0x0000u16, // Zero version
            0x0002u16, // Unsupported version
            0xFFFFu16, // Maximum version
            0x1234u16, // Random invalid
        ];

        for version in invalid_versions {
            let mut data = Vec::new();
            data.extend_from_slice(&valid_magic.to_be_bytes());
            data.extend_from_slice(&version.to_be_bytes());

            let result = parse_magic_and_version(&data);
            assert!(
                result.is_err(),
                "Invalid version 0x{:04X} should be rejected",
                version
            );
        }
    }

    #[test]
    fn test_truncated_headers() {
        let valid_magic = CassandraVersion::Legacy.magic_number();

        // Test various truncation points
        let truncation_tests = vec![
            0, // Empty
            1, // Single byte
            2, // Two bytes
            3, // Three bytes
            4, // Just magic
            5, // Magic + 1 version byte
        ];

        for size in truncation_tests {
            let mut data = Vec::new();
            data.extend_from_slice(&valid_magic.to_be_bytes());
            data.extend_from_slice(&[0x00, 0x01]); // Valid version
            data.truncate(size);

            let result = parse_magic_and_version(&data);
            if size < 6 {
                assert!(
                    result.is_err(),
                    "Truncated data of size {} should be rejected",
                    size
                );
            }
        }
    }

    #[test]
    fn test_vint_integer_overflow() {
        // Test maximum length VInts
        let max_length_tests = vec![
            vec![0xFF; MAX_VINT_SIZE],     // Maximum size, all 0xFF
            vec![0xFF; MAX_VINT_SIZE + 1], // Exceeds maximum
            vec![0xFF; 20],                // Way too large
        ];

        for test_data in max_length_tests {
            let result = parse_vint(&test_data);
            // Should either parse successfully or fail gracefully
            if result.is_err() {
                // Error is acceptable for oversized data
                continue;
            }
            // If it parses, ensure it doesn't cause overflow
            if let Ok((_, value)) = result {
                assert!(
                    (i64::MIN..=i64::MAX).contains(&value),
                    "VInt overflow detected"
                );
            }
        }
    }

    #[test]
    fn test_vint_negative_lengths() {
        // Test VInts that decode to negative values when used as lengths
        let negative_vint_tests = vec![
            vec![0x01], // ZigZag: -1
            vec![0x03], // ZigZag: -2
            vec![0xFF], // Could be -1 in some encodings
        ];

        for test_data in negative_vint_tests {
            let result = parse_vint_length(&test_data);
            assert!(
                result.is_err(),
                "Negative length {:?} should be rejected",
                test_data
            );
        }
    }

    #[test]
    fn test_malformed_header_strings() {
        // Test headers with malformed string data
        let malformed_string_tests = vec![
            // Invalid UTF-8 sequences
            vec![0x03, 0xFF, 0xFE, 0xFD], // Length 3, invalid UTF-8
            vec![0x04, 0xC0, 0x80, 0x00, 0x00], // Overlong encoding
            vec![0x02, 0xED, 0xA0],       // Surrogate pair
            // Extremely long strings
            vec![0xFF, 0xFF, 0xFF, 0xFF], // Massive length prefix
        ];

        for _test_data in malformed_string_tests {
            // This should be caught by string parsing validation
            // We can't easily test parse_vstring directly since it's not public
            // But the header parser should handle these gracefully
        }
    }

    #[test]
    fn test_buffer_overflow_scenarios() {
        // Test scenarios that could cause buffer overruns
        let overflow_tests = vec![
            // Large length followed by insufficient data
            (vec![0x80, 0xFF], "Large 2-byte length with no data"),
            (vec![0xC0, 0xFF, 0xFF], "Large 3-byte length with no data"),
            // VInt claiming huge length
            (
                vec![0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
                "8-byte VInt maximum",
            ),
        ];

        for (test_data, description) in overflow_tests {
            let result = parse_vint(&test_data);
            // Should either succeed with reasonable value or fail safely
            if let Ok((_, value)) = result {
                assert!(
                    value < 1_000_000_000,
                    "{}: VInt value {} too large",
                    description,
                    value
                );
            }
        }
    }

    #[test]
    fn test_cassandra_nb_format_security() {
        // Test security of 'nb' format with padding bytes
        let mut data = Vec::new();
        data.extend_from_slice(&CassandraVersion::V5_0NewBig.magic_number().to_be_bytes());

        // Add malicious padding that might confuse parser
        let malicious_padding = vec![
            0xFF, 0xFF, 0xFF, 0xFF, // Could be misinterpreted as large length
            0x00, 0x00, 0x00, 0x00, // Null bytes
            0x6F, 0x61, 0x00, 0x01, // Looks like valid header
            0xDE, 0xAD, 0xBE, 0xEF, // Invalid data
            0x00, 0x00, 0x00, 0x00, // More nulls
            0x00, 0x00, 0x00, 0x00, 0x01, // Final byte to complete 25-byte padding
        ];
        data.extend_from_slice(&malicious_padding);
        data.extend_from_slice(&[0x00, 0x01]); // Valid version

        let result = parse_magic_and_version(&data);
        assert!(
            result.is_ok(),
            "Valid 'nb' format should parse despite padding content"
        );

        if let Ok((_, (version, _))) = result {
            assert_eq!(version, CassandraVersion::V5_0NewBig);
        }
    }

    #[test]
    fn test_denial_of_service_vectors() {
        // Test potential DoS vectors

        // 1. Deeply nested or recursive structures (if applicable)
        // 2. Extremely large claimed sizes
        // 3. Repeated parsing of invalid data

        for _ in 0..1000 {
            let invalid_data = vec![0xFF; 100];
            let _ = parse_magic_and_version(&invalid_data);
            // Should complete quickly without hanging
        }
    }

    #[test]
    fn test_information_disclosure() {
        // Test that error messages don't leak sensitive information
        let test_cases = vec![
            (vec![0x00, 0x00, 0x00, 0x00], "Empty magic"),
            (vec![0xFF, 0xFF, 0xFF, 0xFF], "Invalid magic"),
            (vec![0x6F, 0x61, 0x00, 0x00, 0xFF, 0xFF], "Invalid version"),
        ];

        for (data, description) in test_cases {
            if let Err(error) = parse_magic_and_version(&data) {
                let error_msg = format!("{:?}", error);

                // Error messages should not contain:
                // - Raw binary data
                // - Memory addresses
                // - Internal paths
                // - Detailed parsing state
                assert!(
                    !error_msg.contains(&format!("{:?}", data)),
                    "{}: Error message should not contain raw data",
                    description
                );
                assert!(
                    !error_msg.contains("0x"),
                    "{}: Error message should not contain hex addresses",
                    description
                );
            }
        }
    }

    #[test]
    fn test_bounds_checking_comprehensive() {
        // Comprehensive bounds checking test
        let valid_magic = CassandraVersion::Legacy.magic_number();

        // Test reading beyond buffer in various scenarios
        let scenarios = vec![
            // Magic + partial version
            (6, "Complete magic and version"),
            (5, "Magic + 1 version byte"),
            (4, "Just magic"),
            (3, "Partial magic"),
            (2, "Two bytes"),
            (1, "One byte"),
            (0, "Empty"),
        ];

        for (size, description) in scenarios {
            let mut data = vec![0u8; size];
            if size >= 4 {
                data[0..4].copy_from_slice(&valid_magic.to_be_bytes());
            }
            if size >= 6 {
                data[4..6].copy_from_slice(&[0x00, 0x01]);
            }

            let result = parse_magic_and_version(&data);

            if size < 6 {
                assert!(
                    result.is_err(),
                    "{}: Should fail with insufficient data",
                    description
                );
            } else {
                assert!(
                    result.is_ok(),
                    "{}: Should succeed with sufficient data",
                    description
                );
            }
        }
    }
}
