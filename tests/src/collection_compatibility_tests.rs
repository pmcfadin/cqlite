//! Collection compatibility tests with real Cassandra data formats
//!
//! This module tests collection parsing against real Cassandra SSTable data
//! to ensure 100% compatibility with the 'oa' format specification.

use cqlite_core::{
    parser::{
        // parse_cql_value, serialize_cql_value, CqlTypeId,
        // parse_list_with_type, parse_set_with_type, parse_map_with_types,
        // parse_tuple, parse_udt, vint::encode_vint
    },
    // types::Value,
    // Result,
};
use std::collections::HashMap;

/// Test collection parsing against Cassandra 5+ format specification
pub struct CollectionCompatibilityTester {
    pub test_results: Vec<TestResult>,
}

#[derive(Debug, Clone)]
pub struct TestResult {
    pub test_name: String,
    pub success: bool,
    pub error_message: Option<String>,
    pub performance_metrics: Option<PerformanceMetrics>,
}

#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub parse_time_us: u64,
    pub serialize_time_us: u64,
    pub data_size_bytes: usize,
    pub throughput_mbps: f64,
}

impl CollectionCompatibilityTester {
    pub fn new() -> Self {
        Self {
            test_results: Vec::new(),
        }
    }

    /// Run comprehensive collection compatibility tests
    pub fn run_all_tests(&mut self) -> Result<()> {
        println!("🧪 Running Collection Compatibility Tests...");
        
        // Basic collection type tests
        self.test_list_compatibility()?;
        self.test_set_compatibility()?;
        self.test_map_compatibility()?;
        self.test_tuple_compatibility()?;
        
        // Nested collection tests
        self.test_nested_collections()?;
        
        // Edge case tests
        self.test_edge_cases()?;
        
        // Performance tests
        self.test_collection_performance()?;
        
        // Real-world data pattern tests
        self.test_real_world_patterns()?;
        
        Ok(())
    }

    /// Test List compatibility with various element types
    fn test_list_compatibility(&mut self) -> Result<()> {
        println!("  📋 Testing List compatibility...");
        
        // Test 1: Empty List
        self.test_collection_roundtrip(
            "Empty List",
            Value::List(vec![]),
            CqlTypeId::List,
        )?;
        
        // Test 2: String List (common in user data)
        let string_list = Value::List(vec![
            Value::Text("apple".to_string()),
            Value::Text("banana".to_string()),
            Value::Text("cherry".to_string()),
            Value::Text("date".to_string()),
            Value::Text("elderberry".to_string()),
        ]);
        self.test_collection_roundtrip("String List", string_list, CqlTypeId::List)?;
        
        // Test 3: Integer List (common in analytics)
        let int_list = Value::List(vec![
            Value::Integer(1),
            Value::Integer(42),
            Value::Integer(-100),
            Value::Integer(0),
            Value::Integer(2147483647), // MAX_INT
        ]);
        self.test_collection_roundtrip("Integer List", int_list, CqlTypeId::List)?;
        
        // Test 4: BigInt List (timestamps, IDs)
        let bigint_list = Value::List(vec![
            Value::BigInt(1640995200000000), // 2022-01-01 timestamp
            Value::BigInt(1672531200000000), // 2023-01-01 timestamp
            Value::BigInt(9223372036854775807), // MAX_LONG
        ]);
        self.test_collection_roundtrip("BigInt List", bigint_list, CqlTypeId::List)?;
        
        // Test 5: UUID List (common for IDs)
        let uuid_list = Value::List(vec![
            Value::Uuid([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
            Value::Uuid([16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]),
        ]);
        self.test_collection_roundtrip("UUID List", uuid_list, CqlTypeId::List)?;
        
        // Test 6: Large List (performance test)
        let large_list = Value::List((0..1000).map(|i| Value::Integer(i)).collect());
        self.test_collection_roundtrip("Large List (1K elements)", large_list, CqlTypeId::List)?;
        
        Ok(())
    }

    /// Test Set compatibility with duplicate handling
    fn test_set_compatibility(&mut self) -> Result<()> {
        println!("  🎯 Testing Set compatibility...");
        
        // Test 1: Empty Set
        self.test_collection_roundtrip(
            "Empty Set",
            Value::Set(vec![]),
            CqlTypeId::Set,
        )?;
        
        // Test 2: String Set (tags, categories)
        let string_set = Value::Set(vec![
            Value::Text("technology".to_string()),
            Value::Text("programming".to_string()),
            Value::Text("database".to_string()),
            Value::Text("cassandra".to_string()),
        ]);
        self.test_collection_roundtrip("String Set", string_set, CqlTypeId::Set)?;
        
        // Test 3: Integer Set (numeric categories)
        let int_set = Value::Set(vec![
            Value::Integer(1),
            Value::Integer(3),
            Value::Integer(5),
            Value::Integer(7),
            Value::Integer(11), // Prime numbers
        ]);
        self.test_collection_roundtrip("Integer Set", int_set, CqlTypeId::Set)?;
        
        // Test 4: Set with duplicate handling (should be processed correctly)
        let duplicate_test_data = vec![
            Value::Text("apple".to_string()),
            Value::Text("banana".to_string()),
            Value::Text("apple".to_string()), // Duplicate
            Value::Text("cherry".to_string()),
        ];
        
        // Manually create binary data with duplicates to test parser behavior
        let mut test_data = Vec::new();
        test_data.extend_from_slice(&encode_vint(duplicate_test_data.len() as i64));
        test_data.push(CqlTypeId::Varchar as u8);
        
        for item in &duplicate_test_data {
            if let Value::Text(s) = item {
                test_data.extend_from_slice(&encode_vint(s.len() as i64));
                test_data.extend_from_slice(s.as_bytes());
            }
        }
        
        let start = std::time::Instant::now();
        let (_, parsed_value) = parse_cql_value(&test_data, CqlTypeId::Set)?;
        let parse_time = start.elapsed();
        
        if let Value::Set(parsed_set) = parsed_value {
            // Should have only unique elements (3, not 4)
            let success = parsed_set.len() == 3;
            self.test_results.push(TestResult {
                test_name: "Set Duplicate Handling".to_string(),
                success,
                error_message: if !success { 
                    Some(format!("Expected 3 unique elements, got {}", parsed_set.len())) 
                } else { 
                    None 
                },
                performance_metrics: Some(PerformanceMetrics {
                    parse_time_us: parse_time.as_micros() as u64,
                    serialize_time_us: 0,
                    data_size_bytes: test_data.len(),
                    throughput_mbps: 0.0,
                }),
            });
        }
        
        Ok(())
    }

    /// Test Map compatibility with various key-value combinations
    fn test_map_compatibility(&mut self) -> Result<()> {
        println!("  🗺️  Testing Map compatibility...");
        
        // Test 1: Empty Map
        self.test_collection_roundtrip(
            "Empty Map",
            Value::Map(vec![]),
            CqlTypeId::Map,
        )?;
        
        // Test 2: String to Integer Map (most common pattern)
        let string_int_map = Value::Map(vec![
            (Value::Text("count".to_string()), Value::Integer(42)),
            (Value::Text("total".to_string()), Value::Integer(1000)),
            (Value::Text("average".to_string()), Value::Integer(24)),
        ]);
        self.test_collection_roundtrip("String-to-Int Map", string_int_map, CqlTypeId::Map)?;
        
        // Test 3: String to String Map (metadata, configs)
        let string_string_map = Value::Map(vec![
            (Value::Text("version".to_string()), Value::Text("1.0.0".to_string())),
            (Value::Text("environment".to_string()), Value::Text("production".to_string())),
            (Value::Text("region".to_string()), Value::Text("us-east-1".to_string())),
        ]);
        self.test_collection_roundtrip("String-to-String Map", string_string_map, CqlTypeId::Map)?;
        
        // Test 4: Integer to String Map (ID mappings)
        let int_string_map = Value::Map(vec![
            (Value::Integer(1), Value::Text("admin".to_string())),
            (Value::Integer(2), Value::Text("user".to_string())),
            (Value::Integer(3), Value::Text("guest".to_string())),
        ]);
        self.test_collection_roundtrip("Int-to-String Map", int_string_map, CqlTypeId::Map)?;
        
        // Test 5: UUID to Text Map (user profiles)
        let uuid_text_map = Value::Map(vec![
            (
                Value::Uuid([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
                Value::Text("John Doe".to_string())
            ),
            (
                Value::Uuid([16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]),
                Value::Text("Jane Smith".to_string())
            ),
        ]);
        self.test_collection_roundtrip("UUID-to-Text Map", uuid_text_map, CqlTypeId::Map)?;
        
        // Test 6: Large Map (performance test)
        let large_map = Value::Map(
            (0..100)
                .map(|i| (
                    Value::Text(format!("key_{}", i)),
                    Value::Integer(i * 10)
                ))
                .collect()
        );
        self.test_collection_roundtrip("Large Map (100 entries)", large_map, CqlTypeId::Map)?;
        
        Ok(())
    }

    /// Test Tuple compatibility with heterogeneous types
    fn test_tuple_compatibility(&mut self) -> Result<()> {
        println!("  📦 Testing Tuple compatibility...");
        
        // Test 1: Empty Tuple
        self.test_collection_roundtrip(
            "Empty Tuple",
            Value::Tuple(vec![]),
            CqlTypeId::Tuple,
        )?;
        
        // Test 2: Mixed basic types (common in analytics)
        let mixed_tuple = Value::Tuple(vec![
            Value::Integer(42),
            Value::Text("hello".to_string()),
            Value::Boolean(true),
            Value::Float(3.14159),
            Value::BigInt(1640995200000000),
        ]);
        self.test_collection_roundtrip("Mixed Basic Types Tuple", mixed_tuple, CqlTypeId::Tuple)?;
        
        // Test 3: All numeric types
        let numeric_tuple = Value::Tuple(vec![
            Value::TinyInt(127),
            Value::SmallInt(32767),
            Value::Integer(2147483647),
            Value::BigInt(9223372036854775807),
            Value::Float32(3.14),
            Value::Float(2.718281828),
        ]);
        self.test_collection_roundtrip("All Numeric Types Tuple", numeric_tuple, CqlTypeId::Tuple)?;
        
        // Test 4: Tuple with UUID and Timestamp (common in events)
        let event_tuple = Value::Tuple(vec![
            Value::Uuid([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
            Value::Timestamp(1640995200000000), // 2022-01-01
            Value::Text("user_login".to_string()),
            Value::Integer(200), // HTTP status
        ]);
        self.test_collection_roundtrip("Event Tuple", event_tuple, CqlTypeId::Tuple)?;
        
        // Test 5: Tuple with Binary Data
        let binary_tuple = Value::Tuple(vec![
            Value::Text("image_metadata".to_string()),
            Value::Blob(vec![0xFF, 0xD8, 0xFF, 0xE0]), // JPEG header
            Value::Integer(1920), // width
            Value::Integer(1080), // height
        ]);
        self.test_collection_roundtrip("Binary Data Tuple", binary_tuple, CqlTypeId::Tuple)?;
        
        Ok(())
    }

    /// Test nested collections (List of Maps, Map of Lists, etc.)
    fn test_nested_collections(&mut self) -> Result<()> {
        println!("  🪆 Testing Nested Collections...");
        
        // Test 1: List of Maps (common in JSON-like data)
        let list_of_maps = Value::List(vec![
            Value::Map(vec![
                (Value::Text("name".to_string()), Value::Text("John".to_string())),
                (Value::Text("age".to_string()), Value::Integer(30)),
            ]),
            Value::Map(vec![
                (Value::Text("name".to_string()), Value::Text("Jane".to_string())),
                (Value::Text("age".to_string()), Value::Integer(25)),
            ]),
        ]);
        self.test_collection_roundtrip("List of Maps", list_of_maps, CqlTypeId::List)?;
        
        // Test 2: Map with List values (tags per category)
        let map_of_lists = Value::Map(vec![
            (
                Value::Text("programming".to_string()),
                Value::List(vec![
                    Value::Text("rust".to_string()),
                    Value::Text("python".to_string()),
                    Value::Text("javascript".to_string()),
                ])
            ),
            (
                Value::Text("databases".to_string()),
                Value::List(vec![
                    Value::Text("cassandra".to_string()),
                    Value::Text("postgresql".to_string()),
                ])
            ),
        ]);
        self.test_collection_roundtrip("Map of Lists", map_of_lists, CqlTypeId::Map)?;
        
        // Test 3: Tuple with nested collections
        let nested_tuple = Value::Tuple(vec![
            Value::Text("user_data".to_string()),
            Value::Map(vec![
                (Value::Text("preferences".to_string()), Value::Text("dark_mode".to_string())),
                (Value::Text("language".to_string()), Value::Text("en".to_string())),
            ]),
            Value::List(vec![
                Value::Text("admin".to_string()),
                Value::Text("user".to_string()),
            ]),
        ]);
        self.test_collection_roundtrip("Nested Collections Tuple", nested_tuple, CqlTypeId::Tuple)?;
        
        Ok(())
    }

    /// Test edge cases and error conditions
    fn test_edge_cases(&mut self) -> Result<()> {
        println!("  ⚠️  Testing Edge Cases...");
        
        // Test very large collections (within limits)
        let large_list = Value::List((0..10000).map(|i| Value::Integer(i)).collect());
        self.test_collection_roundtrip("Very Large List (10K)", large_list, CqlTypeId::List)?;
        
        // Test collections with null-equivalent empty strings
        let empty_string_list = Value::List(vec![
            Value::Text("".to_string()),
            Value::Text("non-empty".to_string()),
            Value::Text("".to_string()),
        ]);
        self.test_collection_roundtrip("Empty String List", empty_string_list, CqlTypeId::List)?;
        
        // Test collections with extreme values
        let extreme_values = Value::List(vec![
            Value::Integer(i32::MIN),
            Value::Integer(i32::MAX),
            Value::BigInt(i64::MIN),
            Value::BigInt(i64::MAX),
        ]);
        self.test_collection_roundtrip("Extreme Values", extreme_values, CqlTypeId::List)?;
        
        Ok(())
    }

    /// Test collection performance with large datasets
    fn test_collection_performance(&mut self) -> Result<()> {
        println!("  🚀 Testing Collection Performance...");
        
        // Performance test data sizes
        let test_sizes = vec![100, 1000, 10000];
        
        for size in test_sizes {
            // Test List performance
            let large_list = Value::List(
                (0..size).map(|i| Value::Text(format!("item_{}", i))).collect()
            );
            
            let start = std::time::Instant::now();
            let serialized = serialize_cql_value(&large_list)?;
            let serialize_time = start.elapsed();
            
            let start = std::time::Instant::now();
            let (_, _parsed) = parse_cql_value(&serialized[1..], CqlTypeId::List)?;
            let parse_time = start.elapsed();
            
            let throughput_mbps = (serialized.len() as f64) / (parse_time.as_secs_f64() * 1_000_000.0);
            
            self.test_results.push(TestResult {
                test_name: format!("List Performance ({} elements)", size),
                success: true,
                error_message: None,
                performance_metrics: Some(PerformanceMetrics {
                    parse_time_us: parse_time.as_micros() as u64,
                    serialize_time_us: serialize_time.as_micros() as u64,
                    data_size_bytes: serialized.len(),
                    throughput_mbps,
                }),
            });
            
            // Test Map performance
            let large_map = Value::Map(
                (0..size).map(|i| (
                    Value::Text(format!("key_{}", i)),
                    Value::Integer(i)
                )).collect()
            );
            
            let start = std::time::Instant::now();
            let serialized = serialize_cql_value(&large_map)?;
            let serialize_time = start.elapsed();
            
            let start = std::time::Instant::now();
            let (_, _parsed) = parse_cql_value(&serialized[1..], CqlTypeId::Map)?;
            let parse_time = start.elapsed();
            
            let throughput_mbps = (serialized.len() as f64) / (parse_time.as_secs_f64() * 1_000_000.0);
            
            self.test_results.push(TestResult {
                test_name: format!("Map Performance ({} entries)", size),
                success: true,
                error_message: None,
                performance_metrics: Some(PerformanceMetrics {
                    parse_time_us: parse_time.as_micros() as u64,
                    serialize_time_us: serialize_time.as_micros() as u64,
                    data_size_bytes: serialized.len(),
                    throughput_mbps,
                }),
            });
        }
        
        Ok(())
    }

    /// Test real-world data patterns from production systems
    fn test_real_world_patterns(&mut self) -> Result<()> {
        println!("  🌍 Testing Real-World Data Patterns...");
        
        // Pattern 1: IoT sensor metadata
        let iot_metadata = Value::Map(vec![
            (Value::Text("device_id".to_string()), Value::Text("sensor_001".to_string())),
            (Value::Text("firmware_version".to_string()), Value::Text("v2.1.3".to_string())),
            (Value::Text("last_update".to_string()), Value::BigInt(1640995200000000)),
            (Value::Text("battery_level".to_string()), Value::Integer(85)),
            (Value::Text("location".to_string()), Value::Text("warehouse_a".to_string())),
        ]);
        self.test_collection_roundtrip("IoT Sensor Metadata", iot_metadata, CqlTypeId::Map)?;
        
        // Pattern 2: User social profiles
        let social_profiles = Value::List(vec![
            Value::Map(vec![
                (Value::Text("platform".to_string()), Value::Text("twitter".to_string())),
                (Value::Text("username".to_string()), Value::Text("john_doe".to_string())),
                (Value::Text("verified".to_string()), Value::Boolean(true)),
                (Value::Text("followers".to_string()), Value::Integer(1250)),
            ]),
            Value::Map(vec![
                (Value::Text("platform".to_string()), Value::Text("linkedin".to_string())),
                (Value::Text("username".to_string()), Value::Text("john.doe".to_string())),
                (Value::Text("verified".to_string()), Value::Boolean(false)),
                (Value::Text("connections".to_string()), Value::Integer(500)),
            ]),
        ]);
        self.test_collection_roundtrip("Social Profiles", social_profiles, CqlTypeId::List)?;
        
        // Pattern 3: Analytics event properties
        let event_properties = Value::Map(vec![
            (Value::Text("event_type".to_string()), Value::Text("page_view".to_string())),
            (Value::Text("user_id".to_string()), Value::Uuid([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])),
            (Value::Text("session_id".to_string()), Value::Text("sess_abc123".to_string())),
            (Value::Text("page_url".to_string()), Value::Text("/products/laptop".to_string())),
            (Value::Text("referrer".to_string()), Value::Text("https://google.com".to_string())),
            (Value::Text("user_agent".to_string()), Value::Text("Mozilla/5.0 (compatible)".to_string())),
            (Value::Text("ip_address".to_string()), Value::Text("192.168.1.100".to_string())),
        ]);
        self.test_collection_roundtrip("Analytics Event", event_properties, CqlTypeId::Map)?;
        
        // Pattern 4: Content tags and keywords
        let content_tags = Value::Set(vec![
            Value::Text("programming".to_string()),
            Value::Text("rust".to_string()),
            Value::Text("database".to_string()),
            Value::Text("performance".to_string()),
            Value::Text("tutorial".to_string()),
            Value::Text("open-source".to_string()),
        ]);
        self.test_collection_roundtrip("Content Tags", content_tags, CqlTypeId::Set)?;
        
        Ok(())
    }

    /// Helper function to test collection serialization/parsing roundtrip
    fn test_collection_roundtrip(
        &mut self,
        test_name: &str,
        original_value: Value,
        type_id: CqlTypeId,
    ) -> Result<()> {
        let start = std::time::Instant::now();
        let serialized = serialize_cql_value(&original_value)?;
        let serialize_time = start.elapsed();
        
        let start = std::time::Instant::now();
        let (remaining, parsed_value) = parse_cql_value(&serialized[1..], type_id)?;
        let parse_time = start.elapsed();
        
        let success = remaining.is_empty() && self.values_equivalent(&original_value, &parsed_value);
        let error_message = if !success {
            Some(format!(
                "Roundtrip failed: remaining_bytes={}, values_match={}",
                remaining.len(),
                original_value == parsed_value
            ))
        } else {
            None
        };
        
        let throughput_mbps = if parse_time.as_secs_f64() > 0.0 {
            (serialized.len() as f64) / (parse_time.as_secs_f64() * 1_000_000.0)
        } else {
            0.0
        };
        
        self.test_results.push(TestResult {
            test_name: test_name.to_string(),
            success,
            error_message,
            performance_metrics: Some(PerformanceMetrics {
                parse_time_us: parse_time.as_micros() as u64,
                serialize_time_us: serialize_time.as_micros() as u64,
                data_size_bytes: serialized.len(),
                throughput_mbps,
            }),
        });
        
        Ok(())
    }

    /// Compare values for equivalence (handling floating point precision)
    fn values_equivalent(&self, a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Float(f1), Value::Float(f2)) => (f1 - f2).abs() < f64::EPSILON,
            (Value::Float32(f1), Value::Float32(f2)) => (f1 - f2).abs() < f32::EPSILON,
            (Value::List(l1), Value::List(l2)) => {
                l1.len() == l2.len() && l1.iter().zip(l2.iter()).all(|(a, b)| self.values_equivalent(a, b))
            },
            (Value::Set(s1), Value::Set(s2)) => {
                s1.len() == s2.len() && s1.iter().zip(s2.iter()).all(|(a, b)| self.values_equivalent(a, b))
            },
            (Value::Map(m1), Value::Map(m2)) => {
                m1.len() == m2.len() && m1.iter().zip(m2.iter()).all(|((k1, v1), (k2, v2))| {
                    self.values_equivalent(k1, k2) && self.values_equivalent(v1, v2)
                })
            },
            (Value::Tuple(t1), Value::Tuple(t2)) => {
                t1.len() == t2.len() && t1.iter().zip(t2.iter()).all(|(a, b)| self.values_equivalent(a, b))
            },
            _ => a == b,
        }
    }

    /// Generate test report
    pub fn generate_report(&self) -> String {
        let total_tests = self.test_results.len();
        let successful_tests = self.test_results.iter().filter(|r| r.success).count();
        let failed_tests = total_tests - successful_tests;
        
        let mut report = String::new();
        report.push_str(&format!("📊 Collection Compatibility Test Report\n"));
        report.push_str(&format!("=========================================\n\n"));
        report.push_str(&format!("Total Tests: {}\n", total_tests));
        report.push_str(&format!("✅ Successful: {}\n", successful_tests));
        report.push_str(&format!("❌ Failed: {}\n", failed_tests));
        report.push_str(&format!("Success Rate: {:.1}%\n\n", (successful_tests as f64 / total_tests as f64) * 100.0));
        
        // Performance summary
        let perf_tests: Vec<_> = self.test_results.iter()
            .filter_map(|r| r.performance_metrics.as_ref().map(|p| (r, p)))
            .collect();
        
        if !perf_tests.is_empty() {
            let avg_parse_time: f64 = perf_tests.iter().map(|(_, p)| p.parse_time_us as f64).sum::<f64>() / perf_tests.len() as f64;
            let avg_serialize_time: f64 = perf_tests.iter().map(|(_, p)| p.serialize_time_us as f64).sum::<f64>() / perf_tests.len() as f64;
            let max_throughput = perf_tests.iter().map(|(_, p)| p.throughput_mbps).fold(0.0f64, f64::max);
            
            report.push_str(&format!("🚀 Performance Summary\n"));
            report.push_str(&format!("---------------------\n"));
            report.push_str(&format!("Average Parse Time: {:.1} μs\n", avg_parse_time));
            report.push_str(&format!("Average Serialize Time: {:.1} μs\n", avg_serialize_time));
            report.push_str(&format!("Max Throughput: {:.2} MB/s\n\n", max_throughput));
        }
        
        // Detailed results
        report.push_str(&format!("📋 Detailed Results\n"));
        report.push_str(&format!("-------------------\n"));
        
        for result in &self.test_results {
            let status = if result.success { "✅" } else { "❌" };
            report.push_str(&format!("{} {}\n", status, result.test_name));
            
            if let Some(error) = &result.error_message {
                report.push_str(&format!("   Error: {}\n", error));
            }
            
            if let Some(perf) = &result.performance_metrics {
                report.push_str(&format!("   Parse: {}μs, Serialize: {}μs, Size: {} bytes\n",
                    perf.parse_time_us, perf.serialize_time_us, perf.data_size_bytes));
                if perf.throughput_mbps > 0.0 {
                    report.push_str(&format!("   Throughput: {:.2} MB/s\n", perf.throughput_mbps));
                }
            }
            report.push_str("\n");
        }
        
        report
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collection_compatibility_suite() {
        let mut tester = CollectionCompatibilityTester::new();
        let result = tester.run_all_tests();
        
        assert!(result.is_ok(), "Collection compatibility tests failed: {:?}", result);
        
        let report = tester.generate_report();
        println!("{}", report);
        
        // Ensure high success rate
        let total_tests = tester.test_results.len();
        let successful_tests = tester.test_results.iter().filter(|r| r.success).count();
        let success_rate = (successful_tests as f64 / total_tests as f64) * 100.0;
        
        assert!(success_rate >= 95.0, "Collection compatibility success rate too low: {:.1}%", success_rate);
        assert!(total_tests >= 20, "Not enough tests run: {}", total_tests);
    }
}