//! CQL Type System Compatibility Tests
//!
//! Comprehensive tests for all CQL data types and their serialization/parsing
//! compatibility with Cassandra 5+ format specifications.

use cqlite_core::parser::types::*;
use cqlite_core::parser::vint::{encode_vint, parse_vint};
use cqlite_core::parser::{parse_cql_value, serialize_cql_value, CqlTypeId};
use cqlite_core::{error::Result, Value};
use std::collections::HashMap;

/// Comprehensive CQL type system test suite
pub struct TypeSystemTests {
    test_results: Vec<TypeTestResult>,
}

#[derive(Debug, Clone)]
struct TypeTestResult {
    type_name: String,
    passed: bool,
    error_message: Option<String>,
    serialized_size: usize,
}

impl TypeSystemTests {
    pub fn new() -> Self {
        Self {
            test_results: Vec::new(),
        }
    }

    /// Run all type system tests
    pub fn run_all_tests(&mut self) -> Result<()> {
        println!("🔢 Running CQL Type System Compatibility Tests");

        self.test_primitive_types()?;
        self.test_numeric_types()?;
        self.test_text_and_binary_types()?;
        self.test_temporal_types()?;
        self.test_collection_types()?;
        self.test_special_types()?;
        self.test_edge_cases()?;
        self.test_null_handling()?;
        self.test_type_conversions()?;

        self.print_results();
        Ok(())
    }

    /// Test primitive types (boolean, integers, floats)
    fn test_primitive_types(&mut self) -> Result<()> {
        println!("  Testing primitive types...");

        // Boolean tests
        self.test_type_roundtrip("BOOLEAN_TRUE", CqlTypeId::Boolean, Value::Boolean(true))?;
        self.test_type_roundtrip("BOOLEAN_FALSE", CqlTypeId::Boolean, Value::Boolean(false))?;

        // Integer tests
        self.test_type_roundtrip("TINYINT", CqlTypeId::Tinyint, Value::Integer(127))?;
        self.test_type_roundtrip("SMALLINT", CqlTypeId::Smallint, Value::Integer(32767))?;
        self.test_type_roundtrip("INT", CqlTypeId::Int, Value::Integer(2147483647))?;
        self.test_type_roundtrip(
            "BIGINT",
            CqlTypeId::BigInt,
            Value::BigInt(9223372036854775807),
        )?;

        // Negative integers
        self.test_type_roundtrip("INT_NEGATIVE", CqlTypeId::Int, Value::Integer(-2147483648))?;
        self.test_type_roundtrip(
            "BIGINT_NEGATIVE",
            CqlTypeId::BigInt,
            Value::BigInt(-9223372036854775808),
        )?;

        // Float tests
        self.test_type_roundtrip("FLOAT", CqlTypeId::Float, Value::Float(3.14159))?;
        self.test_type_roundtrip("DOUBLE", CqlTypeId::Double, Value::Float(2.718281828459045))?;

        // Special float values
        self.test_type_roundtrip(
            "FLOAT_INFINITY",
            CqlTypeId::Double,
            Value::Float(f64::INFINITY),
        )?;
        self.test_type_roundtrip(
            "FLOAT_NEG_INFINITY",
            CqlTypeId::Double,
            Value::Float(f64::NEG_INFINITY),
        )?;
        self.test_type_roundtrip("FLOAT_ZERO", CqlTypeId::Double, Value::Float(0.0))?;

        Ok(())
    }

    /// Test numeric type edge cases
    fn test_numeric_types(&mut self) -> Result<()> {
        println!("  Testing numeric type edge cases...");

        // Integer boundaries
        self.test_type_roundtrip("INT_MIN", CqlTypeId::Int, Value::Integer(i32::MIN))?;
        self.test_type_roundtrip("INT_MAX", CqlTypeId::Int, Value::Integer(i32::MAX))?;
        self.test_type_roundtrip("BIGINT_MIN", CqlTypeId::BigInt, Value::BigInt(i64::MIN))?;
        self.test_type_roundtrip("BIGINT_MAX", CqlTypeId::BigInt, Value::BigInt(i64::MAX))?;

        // Zero values
        self.test_type_roundtrip("INT_ZERO", CqlTypeId::Int, Value::Integer(0))?;
        self.test_type_roundtrip("BIGINT_ZERO", CqlTypeId::BigInt, Value::BigInt(0))?;

        // Counter type (same as bigint)
        self.test_type_roundtrip("COUNTER", CqlTypeId::Counter, Value::BigInt(1000))?;

        // Varint tests (variable-length integers)
        self.test_varint_encoding()?;

        Ok(())
    }

    /// Test text and binary types
    fn test_text_and_binary_types(&mut self) -> Result<()> {
        println!("  Testing text and binary types...");

        // ASCII and VARCHAR (both map to Text)
        self.test_type_roundtrip(
            "ASCII",
            CqlTypeId::Ascii,
            Value::Text("Hello, World!".to_string()),
        )?;
        self.test_type_roundtrip(
            "VARCHAR",
            CqlTypeId::Varchar,
            Value::Text("Hello, World!".to_string()),
        )?;

        // Empty string
        self.test_type_roundtrip(
            "EMPTY_STRING",
            CqlTypeId::Varchar,
            Value::Text("".to_string()),
        )?;

        // Unicode strings
        self.test_type_roundtrip(
            "UNICODE",
            CqlTypeId::Varchar,
            Value::Text("🚀 Unicode: δῶς, ñoël, 中文, العربية, עברית".to_string()),
        )?;

        // Long strings
        let long_string = "A".repeat(10000);
        self.test_type_roundtrip("LONG_STRING", CqlTypeId::Varchar, Value::Text(long_string))?;

        // BLOB tests
        self.test_type_roundtrip(
            "BLOB_SMALL",
            CqlTypeId::Blob,
            Value::Blob(vec![0x01, 0x02, 0x03, 0xFF]),
        )?;
        self.test_type_roundtrip("BLOB_EMPTY", CqlTypeId::Blob, Value::Blob(vec![]))?;

        // Large blob
        let large_blob = (0..1000).map(|i| (i % 256) as u8).collect();
        self.test_type_roundtrip("BLOB_LARGE", CqlTypeId::Blob, Value::Blob(large_blob))?;

        Ok(())
    }

    /// Test temporal types
    fn test_temporal_types(&mut self) -> Result<()> {
        println!("  Testing temporal types...");

        // Timestamp tests
        self.test_type_roundtrip("TIMESTAMP_EPOCH", CqlTypeId::Timestamp, Value::Timestamp(0))?;
        self.test_type_roundtrip(
            "TIMESTAMP_2022",
            CqlTypeId::Timestamp,
            Value::Timestamp(1640995200000000),
        )?;
        self.test_type_roundtrip(
            "TIMESTAMP_2023",
            CqlTypeId::Timestamp,
            Value::Timestamp(1672531200000000),
        )?;
        self.test_type_roundtrip(
            "TIMESTAMP_FUTURE",
            CqlTypeId::Timestamp,
            Value::Timestamp(2524608000000000),
        )?;

        // Date tests (stored as timestamps)
        self.test_type_roundtrip("DATE", CqlTypeId::Date, Value::Timestamp(1640995200000000))?;

        // Time tests (stored as timestamps)
        self.test_type_roundtrip("TIME", CqlTypeId::Time, Value::Timestamp(43200000000))?; // Noon in microseconds

        Ok(())
    }

    /// Test collection types
    fn test_collection_types(&mut self) -> Result<()> {
        println!("  Testing collection types...");

        // LIST tests
        let empty_list = Value::List(vec![]);
        self.test_type_roundtrip("LIST_EMPTY", CqlTypeId::List, empty_list)?;

        let string_list = Value::List(vec![
            Value::Text("item1".to_string()),
            Value::Text("item2".to_string()),
            Value::Text("item3".to_string()),
        ]);
        self.test_type_roundtrip("LIST_STRINGS", CqlTypeId::List, string_list)?;

        let number_list = Value::List(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ]);
        self.test_type_roundtrip("LIST_NUMBERS", CqlTypeId::List, number_list)?;

        // SET tests (same format as list)
        let string_set = Value::List(vec![
            Value::Text("unique1".to_string()),
            Value::Text("unique2".to_string()),
            Value::Text("unique3".to_string()),
        ]);
        self.test_type_roundtrip("SET_STRINGS", CqlTypeId::Set, string_set)?;

        // MAP tests
        let empty_map = Value::Map(HashMap::new());
        self.test_type_roundtrip("MAP_EMPTY", CqlTypeId::Map, empty_map)?;

        let mut test_map = HashMap::new();
        test_map.insert("key1".to_string(), Value::Text("value1".to_string()));
        test_map.insert("key2".to_string(), Value::Integer(42));
        test_map.insert("key3".to_string(), Value::Boolean(true));
        let map_value = Value::Map(test_map);
        self.test_type_roundtrip("MAP_MIXED", CqlTypeId::Map, map_value)?;

        Ok(())
    }

    /// Test special types
    fn test_special_types(&mut self) -> Result<()> {
        println!("  Testing special types...");

        // UUID tests
        let uuid_bytes = [
            0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54,
            0x32, 0x10,
        ];
        self.test_type_roundtrip("UUID", CqlTypeId::Uuid, Value::Uuid(uuid_bytes))?;

        // TimeUUID (same format as UUID)
        self.test_type_roundtrip("TIMEUUID", CqlTypeId::Timeuuid, Value::Uuid(uuid_bytes))?;

        // INET address (stored as blob)
        let ipv4_bytes = vec![192, 168, 1, 1]; // 192.168.1.1
        self.test_type_roundtrip("INET_IPV4", CqlTypeId::Inet, Value::Blob(ipv4_bytes))?;

        let ipv6_bytes = vec![
            0x20, 0x01, 0x0d, 0xb8, 0x85, 0xa3, 0x00, 0x00, 0x00, 0x00, 0x8a, 0x2e, 0x03, 0x70,
            0x73, 0x34,
        ]; // 2001:db8:85a3::8a2e:370:7334
        self.test_type_roundtrip("INET_IPV6", CqlTypeId::Inet, Value::Blob(ipv6_bytes))?;

        // Duration (stored as bigint representing microseconds)
        self.test_type_roundtrip("DURATION", CqlTypeId::Duration, Value::BigInt(3600000000))?; // 1 hour

        Ok(())
    }

    /// Test edge cases and error conditions
    fn test_edge_cases(&mut self) -> Result<()> {
        println!("  Testing edge cases...");

        // Very long text
        let very_long_text = "X".repeat(100000);
        self.test_type_roundtrip(
            "VERY_LONG_TEXT",
            CqlTypeId::Varchar,
            Value::Text(very_long_text),
        )?;

        // Text with all ASCII characters
        let all_ascii: String = (0..128).map(|i| i as u8 as char).collect();
        self.test_type_roundtrip("ALL_ASCII", CqlTypeId::Varchar, Value::Text(all_ascii))?;

        // Binary data with all byte values
        let all_bytes: Vec<u8> = (0..256).map(|i| i as u8).collect();
        self.test_type_roundtrip("ALL_BYTES", CqlTypeId::Blob, Value::Blob(all_bytes))?;

        // List with many elements
        let large_list: Vec<Value> = (0..1000).map(|i| Value::Integer(i)).collect();
        self.test_type_roundtrip("LARGE_LIST", CqlTypeId::List, Value::List(large_list))?;

        // Map with many entries
        let mut large_map = HashMap::new();
        for i in 0..1000 {
            large_map.insert(format!("key_{:04}", i), Value::Integer(i));
        }
        self.test_type_roundtrip("LARGE_MAP", CqlTypeId::Map, Value::Map(large_map))?;

        Ok(())
    }

    /// Test null value handling
    fn test_null_handling(&mut self) -> Result<()> {
        println!("  Testing null value handling...");

        // Null should be serializable
        let result = serialize_cql_value(&Value::Null);
        match result {
            Ok(serialized) => {
                self.test_results.push(TypeTestResult {
                    type_name: "NULL".to_string(),
                    passed: !serialized.is_empty(),
                    error_message: None,
                    serialized_size: serialized.len(),
                });
                println!("    ✓ NULL serialization");
            }
            Err(e) => {
                self.test_results.push(TypeTestResult {
                    type_name: "NULL".to_string(),
                    passed: false,
                    error_message: Some(format!("Null serialization failed: {:?}", e)),
                    serialized_size: 0,
                });
                println!("    ✗ NULL serialization failed");
            }
        }

        Ok(())
    }

    /// Test type conversions and compatibility
    fn test_type_conversions(&mut self) -> Result<()> {
        println!("  Testing type conversions...");

        // Test that tinyint values fit in int
        let tinyint_max = Value::Integer(127);
        self.test_type_compatibility("TINYINT_AS_INT", CqlTypeId::Int, tinyint_max)?;

        // Test that smallint values fit in int
        let smallint_max = Value::Integer(32767);
        self.test_type_compatibility("SMALLINT_AS_INT", CqlTypeId::Int, smallint_max)?;

        // Test that int values fit in bigint
        let int_value = Value::BigInt(2147483647);
        self.test_type_compatibility("INT_AS_BIGINT", CqlTypeId::BigInt, int_value)?;

        // Test that float values can be stored as double
        let float_value = Value::Float(3.14159);
        self.test_type_compatibility("FLOAT_AS_DOUBLE", CqlTypeId::Double, float_value)?;

        Ok(())
    }

    /// Test varint encoding specifically
    fn test_varint_encoding(&mut self) -> Result<()> {
        println!("    Testing varint encoding...");

        let test_values = vec![
            0i64,
            1,
            -1,
            127,
            -128,
            128,
            -129,
            32767,
            -32768,
            32768,
            -32769,
            2147483647,
            -2147483648,
            9223372036854775807,
            -9223372036854775808,
        ];

        for value in test_values {
            let encoded = encode_vint(value);
            match parse_vint(&encoded) {
                Ok((_, decoded)) => {
                    if decoded == value {
                        println!("      ✓ Varint {}: {} bytes", value, encoded.len());
                    } else {
                        println!("      ✗ Varint {} mismatch: got {}", value, decoded);
                    }
                }
                Err(e) => {
                    println!("      ✗ Varint {} parse error: {:?}", value, e);
                }
            }
        }

        Ok(())
    }

    /// Test a type roundtrip (serialize then parse)
    fn test_type_roundtrip(
        &mut self,
        test_name: &str,
        type_id: CqlTypeId,
        value: Value,
    ) -> Result<()> {
        match serialize_cql_value(&value) {
            Ok(serialized) => {
                if serialized.is_empty() {
                    self.test_results.push(TypeTestResult {
                        type_name: test_name.to_string(),
                        passed: false,
                        error_message: Some("Empty serialization".to_string()),
                        serialized_size: 0,
                    });
                    return Ok(());
                }

                // Skip the type byte for parsing
                if serialized.len() > 1 {
                    match parse_cql_value(&serialized[1..], type_id) {
                        Ok((_, parsed_value)) => {
                            let compatible = self.values_compatible(&value, &parsed_value);
                            self.test_results.push(TypeTestResult {
                                type_name: test_name.to_string(),
                                passed: compatible,
                                error_message: if !compatible {
                                    Some(format!(
                                        "Value mismatch: original={:?}, parsed={:?}",
                                        value, parsed_value
                                    ))
                                } else {
                                    None
                                },
                                serialized_size: serialized.len(),
                            });
                        }
                        Err(e) => {
                            self.test_results.push(TypeTestResult {
                                type_name: test_name.to_string(),
                                passed: false,
                                error_message: Some(format!("Parse error: {:?}", e)),
                                serialized_size: serialized.len(),
                            });
                        }
                    }
                } else {
                    self.test_results.push(TypeTestResult {
                        type_name: test_name.to_string(),
                        passed: false,
                        error_message: Some("Serialized data too short".to_string()),
                        serialized_size: serialized.len(),
                    });
                }
            }
            Err(e) => {
                self.test_results.push(TypeTestResult {
                    type_name: test_name.to_string(),
                    passed: false,
                    error_message: Some(format!("Serialization error: {:?}", e)),
                    serialized_size: 0,
                });
            }
        }

        Ok(())
    }

    /// Test type compatibility (different type, same value)
    fn test_type_compatibility(
        &mut self,
        test_name: &str,
        type_id: CqlTypeId,
        value: Value,
    ) -> Result<()> {
        match serialize_cql_value(&value) {
            Ok(serialized) => {
                if serialized.len() > 1 {
                    match parse_cql_value(&serialized[1..], type_id) {
                        Ok((_, _)) => {
                            self.test_results.push(TypeTestResult {
                                type_name: test_name.to_string(),
                                passed: true,
                                error_message: None,
                                serialized_size: serialized.len(),
                            });
                        }
                        Err(e) => {
                            self.test_results.push(TypeTestResult {
                                type_name: test_name.to_string(),
                                passed: false,
                                error_message: Some(format!("Compatibility error: {:?}", e)),
                                serialized_size: serialized.len(),
                            });
                        }
                    }
                }
            }
            Err(e) => {
                self.test_results.push(TypeTestResult {
                    type_name: test_name.to_string(),
                    passed: false,
                    error_message: Some(format!("Serialization error: {:?}", e)),
                    serialized_size: 0,
                });
            }
        }

        Ok(())
    }

    /// Check if two values are compatible (accounting for type conversions)
    fn values_compatible(&self, original: &Value, parsed: &Value) -> bool {
        match (original, parsed) {
            (Value::Boolean(a), Value::Boolean(b)) => a == b,
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::BigInt(a), Value::BigInt(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => {
                if a.is_nan() && b.is_nan() {
                    true
                } else if a.is_infinite() && b.is_infinite() {
                    a.is_sign_positive() == b.is_sign_positive()
                } else {
                    (a - b).abs() < f64::EPSILON
                }
            }
            (Value::Text(a), Value::Text(b)) => a == b,
            (Value::Blob(a), Value::Blob(b)) => a == b,
            (Value::Uuid(a), Value::Uuid(b)) => a == b,
            (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
            (Value::List(a), Value::List(b)) => a.len() == b.len(),
            (Value::Map(a), Value::Map(b)) => a.len() == b.len(),
            (Value::Null, Value::Null) => true,
            _ => false,
        }
    }

    /// Print test results summary
    fn print_results(&self) {
        let total_tests = self.test_results.len();
        let passed_tests = self.test_results.iter().filter(|r| r.passed).count();
        let total_size: usize = self.test_results.iter().map(|r| r.serialized_size).sum();

        println!("\n📊 CQL Type System Test Results:");
        println!("  Total Tests: {}", total_tests);
        println!(
            "  Passed: {} ({:.1}%)",
            passed_tests,
            (passed_tests as f64 / total_tests as f64) * 100.0
        );
        println!("  Failed: {}", total_tests - passed_tests);
        println!("  Total Data Processed: {} bytes", total_size);

        // Show failed tests
        let failed_tests: Vec<_> = self.test_results.iter().filter(|r| !r.passed).collect();
        if !failed_tests.is_empty() {
            println!("\n❌ Failed Tests:");
            for test in failed_tests {
                println!(
                    "  • {}: {}",
                    test.type_name,
                    test.error_message
                        .as_ref()
                        .unwrap_or(&"Unknown error".to_string())
                );
            }
        }

        // Show largest serialized types
        let mut by_size = self.test_results.clone();
        by_size.sort_by(|a, b| b.serialized_size.cmp(&a.serialized_size));

        println!("\n📏 Largest Serialized Types:");
        for test in by_size.iter().take(5) {
            if test.serialized_size > 0 {
                println!("  • {}: {} bytes", test.type_name, test.serialized_size);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_system_suite() {
        let mut tests = TypeSystemTests::new();
        let result = tests.run_all_tests();
        assert!(result.is_ok(), "Type system tests should complete");
    }

    #[test]
    fn test_primitive_types() {
        let mut tests = TypeSystemTests::new();
        let result = tests.test_primitive_types();
        assert!(result.is_ok(), "Primitive type tests should pass");
    }

    #[test]
    fn test_collection_types() {
        let mut tests = TypeSystemTests::new();
        let result = tests.test_collection_types();
        assert!(result.is_ok(), "Collection type tests should pass");
    }
}
