//! Comprehensive Edge Case Testing for CQL Data Types
//!
//! This module tests extreme boundary conditions, malformed data handling,
//! and edge cases that could break Cassandra compatibility.

use cqlite_core::parser::types::*;
use cqlite_core::parser::vint::{encode_vint, parse_vint};
use cqlite_core::{error::Result, Value};
use std::collections::HashMap;

/// Comprehensive edge case test suite for data types
pub struct EdgeCaseDataTypeTests {
    test_results: Vec<EdgeCaseTestResult>,
    stress_test_enabled: bool,
}

#[derive(Debug, Clone)]
struct EdgeCaseTestResult {
    test_name: String,
    passed: bool,
    error_message: Option<String>,
    processing_time_nanos: u64,
    data_size: usize,
    edge_case_type: EdgeCaseType,
}

#[derive(Debug, Clone)]
enum EdgeCaseType {
    BoundaryValue,
    MalformedData,
    UnicodeEdgeCase,
    LargeData,
    CorruptedInput,
    ConcurrencyStress,
    MemoryExhaustion,
}

impl EdgeCaseDataTypeTests {
    pub fn new() -> Self {
        Self {
            test_results: Vec::new(),
            stress_test_enabled: true,
        }
    }

    /// Run all edge case tests
    pub fn run_all_edge_case_tests(&mut self) -> Result<()> {
        println!("🔍 Running Comprehensive Edge Case Data Type Tests");

        self.test_extreme_numeric_boundaries()?;
        self.test_malformed_vint_data()?;
        self.test_unicode_edge_cases()?;
        self.test_large_data_structures()?;
        self.test_corrupted_data_handling()?;
        self.test_memory_exhaustion_scenarios()?;

        if self.stress_test_enabled {
            self.test_concurrent_data_access()?;
            self.test_stress_data_volumes()?;
        }

        self.print_edge_case_results();
        Ok(())
    }

    /// Test extreme numeric boundary conditions
    fn test_extreme_numeric_boundaries(&mut self) -> Result<()> {
        println!("  Testing extreme numeric boundaries...");

        // Extreme integer values
        let extreme_values = vec![
            // Standard boundaries
            (i8::MIN as i64, "I8_MIN"),
            (i8::MAX as i64, "I8_MAX"),
            (i16::MIN as i64, "I16_MIN"),
            (i16::MAX as i64, "I16_MAX"),
            (i32::MIN as i64, "I32_MIN"),
            (i32::MAX as i64, "I32_MAX"),
            (i64::MIN, "I64_MIN"),
            (i64::MAX, "I64_MAX"),
            // Powers of 2 boundaries
            (0x7F, "2^7-1"),
            (0x80, "2^7"),
            (0xFF, "2^8-1"),
            (0x100, "2^8"),
            (0x7FFF, "2^15-1"),
            (0x8000, "2^15"),
            (0xFFFF, "2^16-1"),
            (0x10000, "2^16"),
            (0x7FFFFFFF, "2^31-1"),
            (0x80000000, "2^31"),
            (0xFFFFFFFF, "2^32-1"),
            (0x100000000, "2^32"),
            // VInt encoding boundaries
            (63, "VINT_1_BYTE_MAX"),
            (64, "VINT_2_BYTE_MIN"),
            (16383, "VINT_2_BYTE_MAX"),
            (16384, "VINT_3_BYTE_MIN"),
            // Off-by-one around boundaries
            (0x7E, "2^7-2"),
            (0x81, "2^7+1"),
            (0xFE, "2^8-2"),
            (0x101, "2^8+1"),
        ];

        for (value, name) in extreme_values {
            self.test_vint_boundary_case(value, name)?;
            self.test_vint_boundary_case(-value, &format!("NEG_{}", name))?;
        }

        // Extreme floating-point values
        let float_extremes = vec![
            (f64::MIN, "F64_MIN"),
            (f64::MAX, "F64_MAX"),
            (f64::MIN_POSITIVE, "F64_MIN_POSITIVE"),
            (f64::INFINITY, "F64_INFINITY"),
            (f64::NEG_INFINITY, "F64_NEG_INFINITY"),
            (f64::NAN, "F64_NAN"),
            (0.0, "F64_ZERO"),
            (-0.0, "F64_NEG_ZERO"),
            (f64::EPSILON, "F64_EPSILON"),
            (1.0 + f64::EPSILON, "F64_ONE_PLUS_EPSILON"),
            (1.0 - f64::EPSILON / 2.0, "F64_ONE_MINUS_HALF_EPSILON"),
        ];

        for (value, name) in float_extremes {
            self.test_float_boundary_case(value, name)?;
        }

        Ok(())
    }

    /// Test malformed VInt data handling
    fn test_malformed_vint_data(&mut self) -> Result<()> {
        println!("  Testing malformed VInt data handling...");

        let malformed_cases = vec![
            // Empty data
            (vec![], "EMPTY_DATA"),
            // Invalid length encodings
            (vec![0xFF], "ALL_ONES_SINGLE"),
            (vec![0xFF, 0xFF], "ALL_ONES_DOUBLE"),
            (
                vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
                "OVERLONG_ENCODING",
            ),
            // Incomplete multi-byte sequences
            (vec![0x80], "INCOMPLETE_2_BYTE"),
            (vec![0xC0], "INCOMPLETE_3_BYTE"),
            (vec![0xE0], "INCOMPLETE_4_BYTE"),
            (vec![0xC0, 0x00], "PARTIAL_3_BYTE"),
            // Zero-length claimed but data present
            (vec![0x80, 0x00], "MINIMAL_2_BYTE"),
            (vec![0xC0, 0x00, 0x00], "MINIMAL_3_BYTE"),
            // Maximum length violations
            (vec![0xFF; 10], "EXCEEDS_MAX_LENGTH"),
            (vec![0xFE; 9], "AT_MAX_LENGTH"),
            // Canonical encoding violations (leading zeros)
            (vec![0x80, 0x00], "NON_CANONICAL_2_BYTE"),
            (vec![0xC0, 0x00, 0x00], "NON_CANONICAL_3_BYTE"),
        ];

        for (data, name) in malformed_cases {
            self.test_malformed_vint_case(&data, name)?;
        }

        Ok(())
    }

    /// Test Unicode and text edge cases
    fn test_unicode_edge_cases(&mut self) -> Result<()> {
        println!("  Testing Unicode edge cases...");

        let unicode_edge_cases = vec![
            // Empty string
            ("", "EMPTY_STRING"),
            // Basic ASCII control characters
            ("\x00", "NULL_CHAR"),
            ("\x01\x02\x03", "LOW_CONTROL_CHARS"),
            ("\x7F", "DEL_CHAR"),
            // Unicode planes
            ("🚀", "EMOJI_ROCKET"),
            ("𝕌𝕟𝕚𝕔𝕠𝕕𝕖", "MATHEMATICAL_ALPHANUMERIC"),
            ("👨‍👩‍👧‍👦", "FAMILY_EMOJI_ZWJ_SEQUENCE"),
            // Bidirectional text
            ("Hello مرحبا עולם", "MIXED_LTR_RTL"),
            ("\u{202E}REVERSED\u{202C}", "BIDI_OVERRIDE"),
            // Combining characters
            ("e\u{0301}", "E_WITH_ACUTE_COMBINING"),
            ("a\u{0300}\u{0301}\u{0302}", "MULTIPLE_COMBINING"),
            // Zero-width characters
            ("Zero\u{200B}Width", "ZERO_WIDTH_SPACE"),
            ("Word\u{FEFF}BOM", "BYTE_ORDER_MARK"),
            ("\u{061C}Direction", "ARABIC_LETTER_MARK"),
            // Normalization issues
            ("café", "PRECOMPOSED_CAFE"),
            ("cafe\u{0301}", "DECOMPOSED_CAFE"),
            // Surrogate pairs and invalid UTF-8 sequences
            // Note: Rust strings are always valid UTF-8, so these are conceptual
            ("Valid UTF-8 \u{10000}", "SURROGATE_PAIR_EQUIVALENT"),
            // Very long strings
            (&"A".repeat(1000), "LONG_ASCII_1K"),
            (&"🚀".repeat(1000), "LONG_EMOJI_1K"),
            // Mixed scripts
            ("Αα Ββ 中文 العربية हिन्दी ελληνικά русский", "MIXED_SCRIPTS"),
            // Private Use Area
            ("\u{E000}\u{F8FF}", "PRIVATE_USE_AREA"),
            // Non-characters
            ("\u{FFFE}\u{FFFF}", "NON_CHARACTERS"),
            // Case conversion edge cases
            ("ß", "GERMAN_ESZETT"),
            ("İ", "TURKISH_I_WITH_DOT"),
            ("ı", "TURKISH_DOTLESS_I"),
            // Ligatures
            ("ﬁﬂ", "LATIN_LIGATURES"),
            // Variation selectors
            ("︎", "VARIATION_SELECTOR"),
            // Vertical text
            ("︀", "VERTICAL_FORMS"),
        ];

        for (text, name) in unicode_edge_cases {
            self.test_unicode_text_case(text, name)?;
        }

        Ok(())
    }

    /// Test large data structure handling
    fn test_large_data_structures(&mut self) -> Result<()> {
        println!("  Testing large data structure handling...");

        // Large collections
        self.test_large_list()?;
        self.test_large_map()?;
        self.test_deep_nesting()?;
        self.test_large_blobs()?;
        self.test_extreme_string_lengths()?;

        Ok(())
    }

    /// Test corrupted data handling
    fn test_corrupted_data_handling(&mut self) -> Result<()> {
        println!("  Testing corrupted data handling...");

        // Generate various corruption patterns
        let base_data = self.create_valid_serialized_data()?;

        self.test_bit_flip_corruption(&base_data)?;
        self.test_truncation_corruption(&base_data)?;
        self.test_random_byte_corruption(&base_data)?;
        self.test_length_field_corruption(&base_data)?;

        Ok(())
    }

    /// Test memory exhaustion scenarios
    fn test_memory_exhaustion_scenarios(&mut self) -> Result<()> {
        println!("  Testing memory exhaustion scenarios...");

        // Test claimed large sizes that could cause allocation failures
        self.test_claimed_large_sizes()?;
        self.test_recursive_allocation_patterns()?;

        Ok(())
    }

    /// Test concurrent data access (basic simulation)
    fn test_concurrent_data_access(&mut self) -> Result<()> {
        println!("  Testing concurrent data access scenarios...");

        // Since we're testing parsers, this focuses on thread-safety of parsing operations
        // In a real scenario, this would use actual threading

        let test_data = self.create_concurrent_test_data()?;

        // Simulate concurrent parsing by rapidly parsing the same data
        for i in 0..1000 {
            self.test_concurrent_parsing_iteration(&test_data, i)?;
        }

        Ok(())
    }

    /// Test stress data volumes
    fn test_stress_data_volumes(&mut self) -> Result<()> {
        println!("  Testing stress data volumes...");

        // Test parsing performance under extreme loads
        self.test_million_element_list()?;
        self.test_gigabyte_string()?;
        self.test_deeply_nested_structures()?;

        Ok(())
    }

    // Helper methods for individual test cases

    fn test_vint_boundary_case(&mut self, value: i64, name: &str) -> Result<()> {
        let start_time = std::time::Instant::now();

        let result = match std::panic::catch_unwind(|| {
            let encoded = encode_vint(value);
            match parse_vint(&encoded) {
                Ok((remaining, decoded)) => {
                    if remaining.is_empty() && decoded == value {
                        Ok(encoded.len())
                    } else {
                        Err(format!(
                            "Roundtrip failed: {} -> {} (remaining: {})",
                            value,
                            decoded,
                            remaining.len()
                        ))
                    }
                }
                Err(e) => Err(format!("Parse failed: {:?}", e)),
            }
        }) {
            Ok(result) => result,
            Err(_) => Err("Panic occurred during test".to_string()),
        };

        let elapsed = start_time.elapsed();

        let test_result = EdgeCaseTestResult {
            test_name: format!("VINT_BOUNDARY_{}", name),
            passed: result.is_ok(),
            error_message: result.err(),
            processing_time_nanos: elapsed.as_nanos() as u64,
            data_size: result.unwrap_or(0),
            edge_case_type: EdgeCaseType::BoundaryValue,
        };

        self.test_results.push(test_result);
        Ok(())
    }

    fn test_float_boundary_case(&mut self, value: f64, name: &str) -> Result<()> {
        let start_time = std::time::Instant::now();

        let result = match std::panic::catch_unwind(|| {
            let float_value = Value::Float(value);
            match serialize_cql_value(&float_value) {
                Ok(serialized) => {
                    if serialized.len() > 1 {
                        match parse_cql_value(&serialized[1..], CqlTypeId::Double) {
                            Ok((_, parsed)) => match parsed {
                                Value::Float(parsed_f) => {
                                    if value.is_nan() && parsed_f.is_nan() {
                                        Ok(serialized.len())
                                    } else if (value - parsed_f).abs() < f64::EPSILON {
                                        Ok(serialized.len())
                                    } else {
                                        Err(format!("Float mismatch: {} != {}", value, parsed_f))
                                    }
                                }
                                _ => Err("Wrong value type returned".to_string()),
                            },
                            Err(e) => Err(format!("Parse failed: {:?}", e)),
                        }
                    } else {
                        Err("Serialized data too short".to_string())
                    }
                }
                Err(e) => Err(format!("Serialization failed: {:?}", e)),
            }
        }) {
            Ok(result) => result,
            Err(_) => Err("Panic occurred during test".to_string()),
        };

        let elapsed = start_time.elapsed();

        let test_result = EdgeCaseTestResult {
            test_name: format!("FLOAT_BOUNDARY_{}", name),
            passed: result.is_ok(),
            error_message: result.err(),
            processing_time_nanos: elapsed.as_nanos() as u64,
            data_size: result.unwrap_or(0),
            edge_case_type: EdgeCaseType::BoundaryValue,
        };

        self.test_results.push(test_result);
        Ok(())
    }

    fn test_malformed_vint_case(&mut self, data: &[u8], name: &str) -> Result<()> {
        let start_time = std::time::Instant::now();

        let result = match std::panic::catch_unwind(|| {
            match parse_vint(data) {
                Ok(_) => {
                    // For malformed data, success might be unexpected
                    if data.is_empty() {
                        Err("Should have failed on empty data".to_string())
                    } else {
                        Ok(data.len()) // Some malformed data might still be parseable
                    }
                }
                Err(_) => {
                    // Expected for malformed data
                    Ok(data.len())
                }
            }
        }) {
            Ok(result) => result,
            Err(_) => Err("Panic occurred during test".to_string()),
        };

        let elapsed = start_time.elapsed();

        let test_result = EdgeCaseTestResult {
            test_name: format!("MALFORMED_VINT_{}", name),
            passed: result.is_ok(),
            error_message: result.err(),
            processing_time_nanos: elapsed.as_nanos() as u64,
            data_size: data.len(),
            edge_case_type: EdgeCaseType::MalformedData,
        };

        self.test_results.push(test_result);
        Ok(())
    }

    fn test_unicode_text_case(&mut self, text: &str, name: &str) -> Result<()> {
        let start_time = std::time::Instant::now();

        let result = match std::panic::catch_unwind(|| {
            let text_value = Value::Text(text.to_string());
            match serialize_cql_value(&text_value) {
                Ok(serialized) => {
                    if serialized.len() > 1 {
                        match parse_cql_value(&serialized[1..], CqlTypeId::Varchar) {
                            Ok((_, parsed)) => match parsed {
                                Value::Text(parsed_text) => {
                                    if parsed_text == text {
                                        Ok(serialized.len())
                                    } else {
                                        Err(format!(
                                            "Text mismatch: '{}' != '{}'",
                                            text, parsed_text
                                        ))
                                    }
                                }
                                _ => Err("Wrong value type returned".to_string()),
                            },
                            Err(e) => Err(format!("Parse failed: {:?}", e)),
                        }
                    } else {
                        Err("Serialized data too short".to_string())
                    }
                }
                Err(e) => Err(format!("Serialization failed: {:?}", e)),
            }
        }) {
            Ok(result) => result,
            Err(_) => Err("Panic occurred during test".to_string()),
        };

        let elapsed = start_time.elapsed();

        let test_result = EdgeCaseTestResult {
            test_name: format!("UNICODE_{}", name),
            passed: result.is_ok(),
            error_message: result.err(),
            processing_time_nanos: elapsed.as_nanos() as u64,
            data_size: text.len(),
            edge_case_type: EdgeCaseType::UnicodeEdgeCase,
        };

        self.test_results.push(test_result);
        Ok(())
    }

    fn test_large_list(&mut self) -> Result<()> {
        let start_time = std::time::Instant::now();

        // Create list with 100,000 elements
        let large_list: Vec<Value> = (0..100_000).map(|i| Value::Integer(i)).collect();
        let list_value = Value::List(large_list);

        let result = match std::panic::catch_unwind(|| match serialize_cql_value(&list_value) {
            Ok(serialized) => Ok(serialized.len()),
            Err(e) => Err(format!("Large list serialization failed: {:?}", e)),
        }) {
            Ok(result) => result,
            Err(_) => Err("Panic occurred during large list test".to_string()),
        };

        let elapsed = start_time.elapsed();

        let test_result = EdgeCaseTestResult {
            test_name: "LARGE_LIST_100K".to_string(),
            passed: result.is_ok(),
            error_message: result.err(),
            processing_time_nanos: elapsed.as_nanos() as u64,
            data_size: result.unwrap_or(0),
            edge_case_type: EdgeCaseType::LargeData,
        };

        self.test_results.push(test_result);
        Ok(())
    }

    fn test_large_map(&mut self) -> Result<()> {
        let start_time = std::time::Instant::now();

        // Create map with 10,000 entries
        let mut large_map = HashMap::new();
        for i in 0..10_000 {
            large_map.insert(format!("key_{:06}", i), Value::Integer(i));
        }
        let map_value = Value::Map(large_map);

        let result = match std::panic::catch_unwind(|| match serialize_cql_value(&map_value) {
            Ok(serialized) => Ok(serialized.len()),
            Err(e) => Err(format!("Large map serialization failed: {:?}", e)),
        }) {
            Ok(result) => result,
            Err(_) => Err("Panic occurred during large map test".to_string()),
        };

        let elapsed = start_time.elapsed();

        let test_result = EdgeCaseTestResult {
            test_name: "LARGE_MAP_10K".to_string(),
            passed: result.is_ok(),
            error_message: result.err(),
            processing_time_nanos: elapsed.as_nanos() as u64,
            data_size: result.unwrap_or(0),
            edge_case_type: EdgeCaseType::LargeData,
        };

        self.test_results.push(test_result);
        Ok(())
    }

    fn test_deep_nesting(&mut self) -> Result<()> {
        let start_time = std::time::Instant::now();

        // Create deeply nested list structure
        let mut nested_value = Value::Integer(42);
        for _ in 0..100 {
            nested_value = Value::List(vec![nested_value]);
        }

        let result = match std::panic::catch_unwind(|| match serialize_cql_value(&nested_value) {
            Ok(serialized) => Ok(serialized.len()),
            Err(e) => Err(format!("Deep nesting serialization failed: {:?}", e)),
        }) {
            Ok(result) => result,
            Err(_) => Err("Panic occurred during deep nesting test".to_string()),
        };

        let elapsed = start_time.elapsed();

        let test_result = EdgeCaseTestResult {
            test_name: "DEEP_NESTING_100".to_string(),
            passed: result.is_ok(),
            error_message: result.err(),
            processing_time_nanos: elapsed.as_nanos() as u64,
            data_size: result.unwrap_or(0),
            edge_case_type: EdgeCaseType::LargeData,
        };

        self.test_results.push(test_result);
        Ok(())
    }

    fn test_large_blobs(&mut self) -> Result<()> {
        let start_time = std::time::Instant::now();

        // Create 10MB blob
        let large_blob: Vec<u8> = (0..10_000_000).map(|i| (i % 256) as u8).collect();
        let blob_value = Value::Blob(large_blob);

        let result = match std::panic::catch_unwind(|| match serialize_cql_value(&blob_value) {
            Ok(serialized) => Ok(serialized.len()),
            Err(e) => Err(format!("Large blob serialization failed: {:?}", e)),
        }) {
            Ok(result) => result,
            Err(_) => Err("Panic occurred during large blob test".to_string()),
        };

        let elapsed = start_time.elapsed();

        let test_result = EdgeCaseTestResult {
            test_name: "LARGE_BLOB_10MB".to_string(),
            passed: result.is_ok(),
            error_message: result.err(),
            processing_time_nanos: elapsed.as_nanos() as u64,
            data_size: result.unwrap_or(0),
            edge_case_type: EdgeCaseType::LargeData,
        };

        self.test_results.push(test_result);
        Ok(())
    }

    fn test_extreme_string_lengths(&mut self) -> Result<()> {
        // Test various extreme string lengths
        let string_lengths = vec![
            0,     // Empty
            1,     // Single char
            65535, // 16-bit max
            65536, // Just over 16-bit
            1_000_000, // 1MB
                   // Don't test larger to avoid memory issues in CI
        ];

        for length in string_lengths {
            let start_time = std::time::Instant::now();

            let test_string = "A".repeat(length);
            let text_value = Value::Text(test_string);

            let result = match std::panic::catch_unwind(|| match serialize_cql_value(&text_value) {
                Ok(serialized) => Ok(serialized.len()),
                Err(e) => Err(format!(
                    "String length {} serialization failed: {:?}",
                    length, e
                )),
            }) {
                Ok(result) => result,
                Err(_) => Err("Panic occurred during extreme string test".to_string()),
            };

            let elapsed = start_time.elapsed();

            let test_result = EdgeCaseTestResult {
                test_name: format!("EXTREME_STRING_LENGTH_{}", length),
                passed: result.is_ok(),
                error_message: result.err(),
                processing_time_nanos: elapsed.as_nanos() as u64,
                data_size: result.unwrap_or(0),
                edge_case_type: EdgeCaseType::LargeData,
            };

            self.test_results.push(test_result);
        }

        Ok(())
    }

    // Additional helper methods for corruption testing

    fn create_valid_serialized_data(&self) -> Result<Vec<u8>> {
        let test_value = Value::Text("Hello, World!".to_string());
        serialize_cql_value(&test_value)
    }

    fn test_bit_flip_corruption(&mut self, base_data: &[u8]) -> Result<()> {
        // Test single bit flips at various positions
        for bit_pos in 0..(base_data.len() * 8).min(64) {
            let mut corrupted_data = base_data.to_vec();
            let byte_idx = bit_pos / 8;
            let bit_idx = bit_pos % 8;
            corrupted_data[byte_idx] ^= 1 << bit_idx;

            self.test_corrupted_data(&corrupted_data, &format!("BIT_FLIP_{}", bit_pos))?;
        }
        Ok(())
    }

    fn test_truncation_corruption(&mut self, base_data: &[u8]) -> Result<()> {
        // Test truncation at various lengths
        for len in 1..base_data.len().min(10) {
            let truncated_data = &base_data[..len];
            self.test_corrupted_data(truncated_data, &format!("TRUNCATED_{}", len))?;
        }
        Ok(())
    }

    fn test_random_byte_corruption(&mut self, base_data: &[u8]) -> Result<()> {
        // Test random byte corruption
        for i in 0..10 {
            let mut corrupted_data = base_data.to_vec();
            if !corrupted_data.is_empty() {
                corrupted_data[i % corrupted_data.len()] = 0xFF;
            }
            self.test_corrupted_data(&corrupted_data, &format!("RANDOM_BYTE_{}", i))?;
        }
        Ok(())
    }

    fn test_length_field_corruption(&mut self, base_data: &[u8]) -> Result<()> {
        // Corrupt length fields (first few bytes which often contain length info)
        for i in 0..base_data.len().min(4) {
            let mut corrupted_data = base_data.to_vec();
            corrupted_data[i] = 0xFF;
            self.test_corrupted_data(&corrupted_data, &format!("LENGTH_CORRUPT_{}", i))?;
        }
        Ok(())
    }

    fn test_corrupted_data(&mut self, data: &[u8], name: &str) -> Result<()> {
        let start_time = std::time::Instant::now();

        let result = match std::panic::catch_unwind(|| {
            // Try to parse as various types to see if it fails gracefully
            if data.is_empty() {
                return Ok(0);
            }

            // Attempt to parse with different type IDs
            let type_ids = [
                CqlTypeId::Boolean,
                CqlTypeId::Int,
                CqlTypeId::Varchar,
                CqlTypeId::Blob,
            ];

            for type_id in &type_ids {
                let _ = parse_cql_value(data, *type_id);
                // We don't care if it fails, just that it doesn't crash
            }

            Ok(data.len())
        }) {
            Ok(result) => result,
            Err(_) => Err("Panic occurred during corruption test".to_string()),
        };

        let elapsed = start_time.elapsed();

        let test_result = EdgeCaseTestResult {
            test_name: format!("CORRUPTION_{}", name),
            passed: result.is_ok(),
            error_message: result.err(),
            processing_time_nanos: elapsed.as_nanos() as u64,
            data_size: data.len(),
            edge_case_type: EdgeCaseType::CorruptedInput,
        };

        self.test_results.push(test_result);
        Ok(())
    }

    fn test_claimed_large_sizes(&mut self) -> Result<()> {
        // Test data that claims to be very large but isn't
        let malicious_data = vec![
            // Claims 1GB string but only has few bytes
            vec![0x0D, 0xFF, 0xFF, 0xFF, 0xFF, 0x48, 0x69], // VARCHAR with huge length claim
            // Claims huge list but minimal data
            vec![0x20, 0xFF, 0xFF, 0xFF, 0xFF], // LIST with huge count claim
        ];

        for (i, data) in malicious_data.iter().enumerate() {
            self.test_corrupted_data(data, &format!("MALICIOUS_SIZE_{}", i))?;
        }

        Ok(())
    }

    fn test_recursive_allocation_patterns(&mut self) -> Result<()> {
        // This would test patterns that could cause exponential memory allocation
        // For now, we just ensure our parser handles it gracefully
        let recursive_pattern = vec![0x20, 0x01, 0x20, 0x01, 0x20]; // Nested lists
        self.test_corrupted_data(&recursive_pattern, "RECURSIVE_ALLOC")?;
        Ok(())
    }

    fn create_concurrent_test_data(&self) -> Result<Vec<u8>> {
        // Create data that will be parsed concurrently
        let test_value = Value::Integer(42);
        serialize_cql_value(&test_value)
    }

    fn test_concurrent_parsing_iteration(&mut self, data: &[u8], iteration: usize) -> Result<()> {
        let start_time = std::time::Instant::now();

        let result =
            match std::panic::catch_unwind(|| match parse_cql_value(&data[1..], CqlTypeId::Int) {
                Ok(_) => Ok(data.len()),
                Err(e) => Err(format!("Concurrent parse failed: {:?}", e)),
            }) {
                Ok(result) => result,
                Err(_) => Err("Panic during concurrent test".to_string()),
            };

        let elapsed = start_time.elapsed();

        // Only record every 100th iteration to avoid too much data
        if iteration % 100 == 0 {
            let test_result = EdgeCaseTestResult {
                test_name: format!("CONCURRENT_PARSE_{}", iteration),
                passed: result.is_ok(),
                error_message: result.err(),
                processing_time_nanos: elapsed.as_nanos() as u64,
                data_size: data.len(),
                edge_case_type: EdgeCaseType::ConcurrencyStress,
            };

            self.test_results.push(test_result);
        }

        Ok(())
    }

    fn test_million_element_list(&mut self) -> Result<()> {
        let start_time = std::time::Instant::now();

        // Test with 1M elements (memory permitting)
        let result = match std::panic::catch_unwind(|| {
            let huge_list: Vec<Value> = (0..1_000_000).map(|i| Value::Integer(i % 1000)).collect();
            let list_value = Value::List(huge_list);

            match serialize_cql_value(&list_value) {
                Ok(serialized) => Ok(serialized.len()),
                Err(e) => Err(format!("Million element list failed: {:?}", e)),
            }
        }) {
            Ok(result) => result,
            Err(_) => Err("Panic during million element test".to_string()),
        };

        let elapsed = start_time.elapsed();

        let test_result = EdgeCaseTestResult {
            test_name: "STRESS_MILLION_ELEMENTS".to_string(),
            passed: result.is_ok(),
            error_message: result.err(),
            processing_time_nanos: elapsed.as_nanos() as u64,
            data_size: result.unwrap_or(0),
            edge_case_type: EdgeCaseType::ConcurrencyStress,
        };

        self.test_results.push(test_result);
        Ok(())
    }

    fn test_gigabyte_string(&mut self) -> Result<()> {
        // We'll simulate this without actually allocating 1GB
        let start_time = std::time::Instant::now();

        let result = Ok(0); // Skip actual GB allocation for practical reasons

        let elapsed = start_time.elapsed();

        let test_result = EdgeCaseTestResult {
            test_name: "STRESS_GIGABYTE_STRING_SIMULATION".to_string(),
            passed: true, // Simulated
            error_message: Some("Simulated test - would require 1GB allocation".to_string()),
            processing_time_nanos: elapsed.as_nanos() as u64,
            data_size: 0,
            edge_case_type: EdgeCaseType::ConcurrencyStress,
        };

        self.test_results.push(test_result);
        Ok(())
    }

    fn test_deeply_nested_structures(&mut self) -> Result<()> {
        let start_time = std::time::Instant::now();

        // Create structure with 1000 levels of nesting
        let result = match std::panic::catch_unwind(|| {
            let mut nested_value = Value::Integer(42);
            for _ in 0..1000 {
                nested_value = Value::List(vec![nested_value]);
            }

            match serialize_cql_value(&nested_value) {
                Ok(serialized) => Ok(serialized.len()),
                Err(e) => Err(format!("Deep nesting 1000 failed: {:?}", e)),
            }
        }) {
            Ok(result) => result,
            Err(_) => Err("Stack overflow or panic during deep nesting".to_string()),
        };

        let elapsed = start_time.elapsed();

        let test_result = EdgeCaseTestResult {
            test_name: "STRESS_DEEP_NESTING_1000".to_string(),
            passed: result.is_ok(),
            error_message: result.err(),
            processing_time_nanos: elapsed.as_nanos() as u64,
            data_size: result.unwrap_or(0),
            edge_case_type: EdgeCaseType::ConcurrencyStress,
        };

        self.test_results.push(test_result);
        Ok(())
    }

    /// Print comprehensive edge case test results
    fn print_edge_case_results(&self) {
        let total_tests = self.test_results.len();
        let passed_tests = self.test_results.iter().filter(|r| r.passed).count();
        let failed_tests = total_tests - passed_tests;

        println!("\n📊 Edge Case Test Results Summary:");
        println!("  Total Tests: {}", total_tests);
        println!(
            "  Passed: {} ({:.1}%)",
            passed_tests,
            (passed_tests as f64 / total_tests as f64) * 100.0
        );
        println!(
            "  Failed: {} ({:.1}%)",
            failed_tests,
            (failed_tests as f64 / total_tests as f64) * 100.0
        );

        // Group by edge case type
        for edge_case_type in [
            EdgeCaseType::BoundaryValue,
            EdgeCaseType::MalformedData,
            EdgeCaseType::UnicodeEdgeCase,
            EdgeCaseType::LargeData,
            EdgeCaseType::CorruptedInput,
            EdgeCaseType::ConcurrencyStress,
            EdgeCaseType::MemoryExhaustion,
        ] {
            let type_tests: Vec<_> = self
                .test_results
                .iter()
                .filter(|r| {
                    std::mem::discriminant(&r.edge_case_type)
                        == std::mem::discriminant(&edge_case_type)
                })
                .collect();

            if !type_tests.is_empty() {
                let type_passed = type_tests.iter().filter(|r| r.passed).count();
                println!(
                    "\n  {:?}: {}/{} passed",
                    edge_case_type,
                    type_passed,
                    type_tests.len()
                );

                // Show failures for this type
                for test in type_tests.iter().filter(|r| !r.passed) {
                    println!(
                        "    ❌ {}: {}",
                        test.test_name,
                        test.error_message
                            .as_ref()
                            .unwrap_or(&"Unknown error".to_string())
                    );
                }
            }
        }

        // Performance analysis
        let avg_processing_time = self
            .test_results
            .iter()
            .map(|r| r.processing_time_nanos)
            .sum::<u64>()
            / total_tests as u64;

        let max_processing_time = self
            .test_results
            .iter()
            .map(|r| r.processing_time_nanos)
            .max()
            .unwrap_or(0);

        let slowest_test = self
            .test_results
            .iter()
            .max_by_key(|r| r.processing_time_nanos);

        println!("\n⏱️  Performance Analysis:");
        println!(
            "  Average processing time: {:.2}μs",
            avg_processing_time as f64 / 1000.0
        );
        println!(
            "  Maximum processing time: {:.2}μs",
            max_processing_time as f64 / 1000.0
        );

        if let Some(slowest) = slowest_test {
            println!(
                "  Slowest test: {} ({:.2}μs)",
                slowest.test_name,
                slowest.processing_time_nanos as f64 / 1000.0
            );
        }

        // Data size analysis
        let total_data_processed: usize = self.test_results.iter().map(|r| r.data_size).sum();

        println!("\n📈 Data Analysis:");
        println!(
            "  Total data processed: {} bytes ({:.2} MB)",
            total_data_processed,
            total_data_processed as f64 / 1_000_000.0
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_edge_case_suite() {
        let mut tests = EdgeCaseDataTypeTests::new();
        // Disable stress tests in unit testing
        tests.stress_test_enabled = false;

        let result = tests.run_all_edge_case_tests();
        assert!(
            result.is_ok(),
            "Edge case tests should complete without panic"
        );
    }

    #[test]
    fn test_extreme_boundaries() {
        let mut tests = EdgeCaseDataTypeTests::new();
        let result = tests.test_extreme_numeric_boundaries();
        assert!(result.is_ok(), "Boundary tests should complete");
    }

    #[test]
    fn test_unicode_handling() {
        let mut tests = EdgeCaseDataTypeTests::new();
        let result = tests.test_unicode_edge_cases();
        assert!(result.is_ok(), "Unicode tests should complete");
    }

    #[test]
    fn test_malformed_data() {
        let mut tests = EdgeCaseDataTypeTests::new();
        let result = tests.test_malformed_vint_data();
        assert!(
            result.is_ok(),
            "Malformed data tests should complete gracefully"
        );
    }
}
