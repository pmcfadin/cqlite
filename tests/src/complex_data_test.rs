//! Complex data type testing for SSTable operations
//! Tests serialization and deserialization of various Cassandra data types

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use cqlite_core::{
    platform::Platform,
    storage::sstable::{
        reader::SSTableReader,
        writer::SSTableWriter,
    },
    types::TableId,
    Config, Result, RowKey, Value,
};

use tempfile::TempDir;

/// Complex data type test suite
pub struct ComplexDataTestSuite {
    platform: Arc<Platform>,
    config: Config,
    test_dir: TempDir,
}

impl ComplexDataTestSuite {
    /// Create a new complex data test suite
    pub async fn new() -> Result<Self> {
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await?);
        let test_dir = TempDir::new().map_err(|e| {
            cqlite_core::error::Error::storage(format!("Failed to create temp dir: {}", e))
        })?;

        Ok(Self {
            platform,
            config,
            test_dir,
        })
    }

    /// Run comprehensive complex data type tests
    pub async fn run_tests(&self) -> Result<ComplexDataTestResults> {
        println!("🧪 Running complex data type tests...");

        let mut results = ComplexDataTestResults {
            tests_passed: 0,
            tests_failed: 0,
            test_details: HashMap::new(),
        };

        // Test 1: Basic data types
        println!("📊 Testing basic data types...");
        match self.test_basic_data_types().await {
            Ok(_) => {
                results.tests_passed += 1;
                results.test_details.insert("basic_data_types".to_string(), "PASSED".to_string());
            }
            Err(e) => {
                results.tests_failed += 1;
                results.test_details.insert("basic_data_types".to_string(), format!("FAILED: {}", e));
            }
        }

        // Test 2: Unicode and special characters
        println!("🌍 Testing Unicode and special characters...");
        match self.test_unicode_data().await {
            Ok(_) => {
                results.tests_passed += 1;
                results.test_details.insert("unicode_data".to_string(), "PASSED".to_string());
            }
            Err(e) => {
                results.tests_failed += 1;
                results.test_details.insert("unicode_data".to_string(), format!("FAILED: {}", e));
            }
        }

        // Test 3: Binary data
        println!("💾 Testing binary data...");
        match self.test_binary_data().await {
            Ok(_) => {
                results.tests_passed += 1;
                results.test_details.insert("binary_data".to_string(), "PASSED".to_string());
            }
            Err(e) => {
                results.tests_failed += 1;
                results.test_details.insert("binary_data".to_string(), format!("FAILED: {}", e));
            }
        }

        // Test 4: Large values
        println!("📏 Testing large values...");
        match self.test_large_values().await {
            Ok(_) => {
                results.tests_passed += 1;
                results.test_details.insert("large_values".to_string(), "PASSED".to_string());
            }
            Err(e) => {
                results.tests_failed += 1;
                results.test_details.insert("large_values".to_string(), format!("FAILED: {}", e));
            }
        }

        // Test 5: Edge cases
        println!("⚠️ Testing edge cases...");
        match self.test_edge_cases().await {
            Ok(_) => {
                results.tests_passed += 1;
                results.test_details.insert("edge_cases".to_string(), "PASSED".to_string());
            }
            Err(e) => {
                results.tests_failed += 1;
                results.test_details.insert("edge_cases".to_string(), format!("FAILED: {}", e));
            }
        }

        // Test 6: Timestamp precision
        println!("⏰ Testing timestamp precision...");
        match self.test_timestamp_precision().await {
            Ok(_) => {
                results.tests_passed += 1;
                results.test_details.insert("timestamp_precision".to_string(), "PASSED".to_string());
            }
            Err(e) => {
                results.tests_failed += 1;
                results.test_details.insert("timestamp_precision".to_string(), format!("FAILED: {}", e));
            }
        }

        Ok(results)
    }

    /// Test basic data types
    async fn test_basic_data_types(&self) -> Result<()> {
        let test_path = self.test_dir.path().join("basic_types.sst");
        let mut writer = SSTableWriter::create(&test_path, &self.config, self.platform.clone()).await?;

        let table_id = TableId::new("basic_types_test");
        
        // Test all basic types
        let test_data = vec![
            ("null_value", Value::Null),
            ("boolean_true", Value::Boolean(true)),
            ("boolean_false", Value::Boolean(false)),
            ("integer_positive", Value::Integer(42)),
            ("integer_negative", Value::Integer(-42)),
            ("integer_zero", Value::Integer(0)),
            ("integer_max", Value::Integer(i32::MAX)),
            ("integer_min", Value::Integer(i32::MIN)),
            ("bigint_positive", Value::BigInt(9223372036854775807)),
            ("bigint_negative", Value::BigInt(-9223372036854775808)),
            ("float_positive", Value::Float(3.14159)),
            ("float_negative", Value::Float(-3.14159)),
            ("float_zero", Value::Float(0.0)),
            ("float_infinity", Value::Float(f64::INFINITY)),
            ("float_neg_infinity", Value::Float(f64::NEG_INFINITY)),
            ("text_simple", Value::Text("Hello, World!".to_string())),
            ("text_empty", Value::Text("".to_string())),
            ("blob_simple", Value::Blob(vec![1, 2, 3, 4, 5])),
            ("blob_empty", Value::Blob(vec![])),
            ("timestamp", Value::Timestamp(
                SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as u64
            )),
            ("uuid", Value::Uuid(vec![
                0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0,
                0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0
            ])),
        ];

        // Write all test data
        for (key, value) in &test_data {
            writer.add_entry(&table_id, RowKey::from(*key), value.clone()).await?;
        }

        writer.finish().await?;

        // Read back and verify
        let reader = SSTableReader::open(&test_path, &self.config, self.platform.clone()).await?;

        for (key, expected_value) in &test_data {
            match reader.get(&table_id, &RowKey::from(*key)).await? {
                Some(actual_value) => {
                    if actual_value != *expected_value {
                        return Err(cqlite_core::error::Error::storage(format!(
                            "Value mismatch for key '{}': expected {:?}, got {:?}",
                            key, expected_value, actual_value
                        )));
                    }
                }
                None => {
                    return Err(cqlite_core::error::Error::storage(format!(
                        "Could not read value for key '{}'", key
                    )));
                }
            }
        }

        println!("✅ All basic data types passed round-trip test");
        Ok(())
    }

    /// Test Unicode and special characters
    async fn test_unicode_data(&self) -> Result<()> {
        let test_path = self.test_dir.path().join("unicode_test.sst");
        let mut writer = SSTableWriter::create(&test_path, &self.config, self.platform.clone()).await?;

        let table_id = TableId::new("unicode_test");

        let unicode_test_data = vec![
            ("emoji", Value::Text("🦀🚀🌟💾🔥".to_string())),
            ("chinese", Value::Text("你好世界".to_string())),
            ("arabic", Value::Text("مرحبا بالعالم".to_string())),
            ("hebrew", Value::Text("שלום עולם".to_string())),
            ("russian", Value::Text("Привет мир".to_string())),
            ("japanese", Value::Text("こんにちは世界".to_string())),
            ("korean", Value::Text("안녕하세요 세계".to_string())),
            ("mixed", Value::Text("Hello 世界 🌍 مرحبا שלום".to_string())),
            ("special_chars", Value::Text("!@#$%^&*()[]{}|\\:;\"'<>,.?/~`".to_string())),
            ("control_chars", Value::Text("\t\n\r".to_string())),
            ("zero_width", Value::Text("a\u{200B}b\u{FEFF}c".to_string())), // Zero-width chars
        ];

        for (key, value) in &unicode_test_data {
            writer.add_entry(&table_id, RowKey::from(*key), value.clone()).await?;
        }

        writer.finish().await?;

        // Verify
        let reader = SSTableReader::open(&test_path, &self.config, self.platform.clone()).await?;

        for (key, expected_value) in &unicode_test_data {
            match reader.get(&table_id, &RowKey::from(*key)).await? {
                Some(actual_value) => {
                    if actual_value != *expected_value {
                        return Err(cqlite_core::error::Error::storage(format!(
                            "Unicode test failed for key '{}': expected {:?}, got {:?}",
                            key, expected_value, actual_value
                        )));
                    }
                }
                None => {
                    return Err(cqlite_core::error::Error::storage(format!(
                        "Could not read Unicode value for key '{}'", key
                    )));
                }
            }
        }

        println!("✅ All Unicode data passed round-trip test");
        Ok(())
    }

    /// Test binary data with various patterns
    async fn test_binary_data(&self) -> Result<()> {
        let test_path = self.test_dir.path().join("binary_test.sst");
        let mut writer = SSTableWriter::create(&test_path, &self.config, self.platform.clone()).await?;

        let table_id = TableId::new("binary_test");

        let binary_test_data = vec![
            ("all_zeros", Value::Blob(vec![0; 100])),
            ("all_ones", Value::Blob(vec![255; 100])),
            ("alternating", Value::Blob((0..100).map(|i| if i % 2 == 0 { 0x55 } else { 0xAA }).collect())),
            ("sequential", Value::Blob((0..256).map(|i| i as u8).collect())),
            ("random_pattern", Value::Blob(vec![
                0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0,
                0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54, 0x32, 0x10
            ])),
            ("large_binary", Value::Blob((0..10000).map(|i| (i % 256) as u8).collect())),
        ];

        for (key, value) in &binary_test_data {
            writer.add_entry(&table_id, RowKey::from(*key), value.clone()).await?;
        }

        writer.finish().await?;

        // Verify
        let reader = SSTableReader::open(&test_path, &self.config, self.platform.clone()).await?;

        for (key, expected_value) in &binary_test_data {
            match reader.get(&table_id, &RowKey::from(*key)).await? {
                Some(actual_value) => {
                    if actual_value != *expected_value {
                        return Err(cqlite_core::error::Error::storage(format!(
                            "Binary test failed for key '{}'", key
                        )));
                    }
                }
                None => {
                    return Err(cqlite_core::error::Error::storage(format!(
                        "Could not read binary value for key '{}'", key
                    )));
                }
            }
        }

        println!("✅ All binary data passed round-trip test");
        Ok(())
    }

    /// Test large values
    async fn test_large_values(&self) -> Result<()> {
        let test_path = self.test_dir.path().join("large_values_test.sst");
        let mut writer = SSTableWriter::create(&test_path, &self.config, self.platform.clone()).await?;

        let table_id = TableId::new("large_values_test");

        // Test progressively larger values
        let sizes = vec![1024, 10240, 102400, 1048576]; // 1KB, 10KB, 100KB, 1MB

        for size in sizes {
            let key = RowKey::from(format!("large_{}", size));
            
            // Create large text value with pattern
            let pattern = format!("This is a test pattern for size {} bytes. ", size);
            let repetitions = size / pattern.len() + 1;
            let large_text = pattern.repeat(repetitions);
            let truncated_text = large_text.chars().take(size).collect::<String>();
            
            let value = Value::Text(truncated_text.clone());
            writer.add_entry(&table_id, key.clone(), value.clone()).await?;
            
            println!("✍️ Wrote large value of size: {} bytes", truncated_text.len());
        }

        writer.finish().await?;

        // Verify
        let reader = SSTableReader::open(&test_path, &self.config, self.platform.clone()).await?;

        for size in &[1024, 10240, 102400, 1048576] {
            let key = RowKey::from(format!("large_{}", size));
            
            match reader.get(&table_id, &key).await? {
                Some(Value::Text(text)) => {
                    if text.len() != *size {
                        return Err(cqlite_core::error::Error::storage(format!(
                            "Large value size mismatch: expected {}, got {}", size, text.len()
                        )));
                    }
                    println!("✅ Verified large value of size: {} bytes", text.len());
                }
                Some(_) => {
                    return Err(cqlite_core::error::Error::storage(
                        "Large value type mismatch".to_string()
                    ));
                }
                None => {
                    return Err(cqlite_core::error::Error::storage(format!(
                        "Could not read large value of size {}", size
                    )));
                }
            }
        }

        println!("✅ All large values passed round-trip test");
        Ok(())
    }

    /// Test edge cases
    async fn test_edge_cases(&self) -> Result<()> {
        let test_path = self.test_dir.path().join("edge_cases_test.sst");
        let mut writer = SSTableWriter::create(&test_path, &self.config, self.platform.clone()).await?;

        let table_id = TableId::new("edge_cases_test");

        let edge_cases = vec![
            // Numeric edge cases
            ("float_nan", Value::Float(f64::NAN)),
            ("zero_float", Value::Float(0.0)),
            ("negative_zero_float", Value::Float(-0.0)),
            ("min_positive_float", Value::Float(f64::MIN_POSITIVE)),
            ("max_float", Value::Float(f64::MAX)),
            ("min_float", Value::Float(f64::MIN)),
            
            // String edge cases
            ("single_char", Value::Text("a".to_string())),
            ("newline_only", Value::Text("\n".to_string())),
            ("tab_only", Value::Text("\t".to_string())),
            ("space_only", Value::Text(" ".to_string())),
            ("null_char", Value::Text("\0".to_string())),
            
            // Binary edge cases
            ("single_byte", Value::Blob(vec![42])),
            ("null_byte", Value::Blob(vec![0])),
            ("max_byte", Value::Blob(vec![255])),
        ];

        for (key, value) in &edge_cases {
            writer.add_entry(&table_id, RowKey::from(*key), value.clone()).await?;
        }

        writer.finish().await?;

        // Verify (special handling for NaN)
        let reader = SSTableReader::open(&test_path, &self.config, self.platform.clone()).await?;

        for (key, expected_value) in &edge_cases {
            match reader.get(&table_id, &RowKey::from(*key)).await? {
                Some(actual_value) => {
                    // Special case for NaN comparison
                    if let (Value::Float(expected), Value::Float(actual)) = (expected_value, &actual_value) {
                        if expected.is_nan() && actual.is_nan() {
                            continue; // Both NaN, consider equal
                        }
                    }
                    
                    if actual_value != *expected_value {
                        return Err(cqlite_core::error::Error::storage(format!(
                            "Edge case test failed for key '{}': expected {:?}, got {:?}",
                            key, expected_value, actual_value
                        )));
                    }
                }
                None => {
                    return Err(cqlite_core::error::Error::storage(format!(
                        "Could not read edge case value for key '{}'", key
                    )));
                }
            }
        }

        println!("✅ All edge cases passed round-trip test");
        Ok(())
    }

    /// Test timestamp precision
    async fn test_timestamp_precision(&self) -> Result<()> {
        let test_path = self.test_dir.path().join("timestamp_test.sst");
        let mut writer = SSTableWriter::create(&test_path, &self.config, self.platform.clone()).await?;

        let table_id = TableId::new("timestamp_test");

        // Test various timestamp values
        let base_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as u64;
        
        let timestamp_tests = vec![
            ("current_time", base_time),
            ("epoch", 0),
            ("year_2000", 946684800_000_000), // Jan 1, 2000 in microseconds
            ("year_2038", 2147483647_000_000), // Y2K38 problem timestamp
            ("future", base_time + 365 * 24 * 60 * 60 * 1_000_000), // One year from now
            ("microsecond_precision", base_time + 123), // Test microsecond precision
        ];

        for (key, timestamp) in &timestamp_tests {
            let value = Value::Timestamp(*timestamp);
            writer.add_entry(&table_id, RowKey::from(*key), value).await?;
        }

        writer.finish().await?;

        // Verify
        let reader = SSTableReader::open(&test_path, &self.config, self.platform.clone()).await?;

        for (key, expected_timestamp) in &timestamp_tests {
            match reader.get(&table_id, &RowKey::from(*key)).await? {
                Some(Value::Timestamp(actual_timestamp)) => {
                    if actual_timestamp != *expected_timestamp {
                        return Err(cqlite_core::error::Error::storage(format!(
                            "Timestamp precision test failed for key '{}': expected {}, got {}",
                            key, expected_timestamp, actual_timestamp
                        )));
                    }
                }
                Some(_) => {
                    return Err(cqlite_core::error::Error::storage(
                        "Timestamp type mismatch".to_string()
                    ));
                }
                None => {
                    return Err(cqlite_core::error::Error::storage(format!(
                        "Could not read timestamp value for key '{}'", key
                    )));
                }
            }
        }

        println!("✅ All timestamp precision tests passed");
        Ok(())
    }

    /// Get test directory path
    pub fn test_dir_path(&self) -> &Path {
        self.test_dir.path()
    }
}

/// Test results for complex data types
#[derive(Debug)]
pub struct ComplexDataTestResults {
    pub tests_passed: u32,
    pub tests_failed: u32,
    pub test_details: HashMap<String, String>,
}

impl ComplexDataTestResults {
    /// Print results summary
    pub fn print_summary(&self) {
        println!("\n📊 Complex Data Type Test Results");
        println!("=================================");
        println!("Tests passed: {}", self.tests_passed);
        println!("Tests failed: {}", self.tests_failed);
        println!("Success rate: {:.1}%", 
            self.tests_passed as f64 / (self.tests_passed + self.tests_failed) as f64 * 100.0);
        
        println!("\nDetailed Results:");
        for (test_name, result) in &self.test_details {
            let status_icon = if result == "PASSED" { "✅" } else { "❌" };
            println!("  {} {}: {}", status_icon, test_name, result);
        }
        
        if self.tests_failed == 0 {
            println!("\n🎉 All complex data type tests passed!");
        } else {
            println!("\n⚠️ Some tests failed. See details above.");
        }
    }

    /// Check if all tests passed
    pub fn all_passed(&self) -> bool {
        self.tests_failed == 0
    }
}

/// Run comprehensive complex data type tests
pub async fn run_complex_data_tests() -> Result<()> {
    println!("🚀 Starting Complex Data Type Test Suite");
    println!("========================================");

    let test_suite = ComplexDataTestSuite::new().await?;
    let results = test_suite.run_tests().await?;
    
    results.print_summary();
    
    if results.all_passed() {
        Ok(())
    } else {
        Err(cqlite_core::error::Error::storage(
            "Complex data type tests failed".to_string()
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_suite_creation() {
        let suite = ComplexDataTestSuite::new().await.unwrap();
        assert!(suite.test_dir_path().exists());
    }

    #[tokio::test]
    async fn test_basic_data_types() {
        let suite = ComplexDataTestSuite::new().await.unwrap();
        suite.test_basic_data_types().await.unwrap();
    }

    #[tokio::test]
    async fn test_unicode_data() {
        let suite = ComplexDataTestSuite::new().await.unwrap();
        suite.test_unicode_data().await.unwrap();
    }
}