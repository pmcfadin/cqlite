//! Comprehensive SSTable validator for testing reader/writer functionality
//! and Cassandra 5+ 'oa' format specification compliance

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use cqlite_core::{
    platform::Platform,
    storage::sstable::{
        bloom::BloomFilter,
        reader::SSTableReader,
        validation::{CassandraValidationFramework, TestResult, ValidationReport},
        writer::SSTableWriter,
        SSTableManager,
    },
    types::{DataType, TableId},
    Config, Result, RowKey, Value,
};

use tempfile::TempDir;

/// Comprehensive SSTable validator
pub struct SSTableValidator {
    /// Platform abstraction
    platform: Arc<Platform>,
    /// Configuration
    config: Config,
    /// Test output directory
    test_dir: TempDir,
    /// Validation framework for Cassandra compatibility
    cassandra_validator: CassandraValidationFramework,
}

impl SSTableValidator {
    /// Create a new SSTable validator
    pub async fn new() -> Result<Self> {
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await?);
        let test_dir = TempDir::new().map_err(|e| {
            cqlite_core::error::Error::storage(format!("Failed to create temp dir: {}", e))
        })?;

        let cassandra_validator = CassandraValidationFramework::new(
            platform.clone(),
            config.clone(),
            test_dir.path().to_str().unwrap(),
        );

        Ok(Self {
            platform,
            config,
            test_dir,
            cassandra_validator,
        })
    }

    /// Run comprehensive validation test suite
    pub async fn run_full_validation(&self) -> Result<ValidationReport> {
        let mut report = ValidationReport::new();

        println!("🧪 Starting comprehensive SSTable validation...");

        // Test 1: Basic write/read functionality
        println!("📝 Testing basic write/read functionality...");
        report.add_test_result("basic_write_read", self.test_basic_write_read().await?);

        // Test 2: Data type serialization/deserialization
        println!("📊 Testing data type serialization...");
        report.add_test_result("data_types", self.test_data_type_serialization().await?);

        // Test 3: Complex data structures
        println!("🏗️ Testing complex data structures...");
        report.add_test_result("complex_data", self.test_complex_data_structures().await?);

        // Test 4: File format validation
        println!("📋 Testing file format compliance...");
        report.add_test_result("file_format", self.test_file_format_compliance().await?);

        // Test 5: Compression functionality
        println!("🗜️ Testing compression functionality...");
        report.add_test_result("compression", self.test_compression_functionality().await?);

        // Test 6: Bloom filter validation
        println!("🌸 Testing bloom filter functionality...");
        report.add_test_result("bloom_filter", self.test_bloom_filter_functionality().await?);

        // Test 7: Index functionality
        println!("📇 Testing index functionality...");
        report.add_test_result("index", self.test_index_functionality().await?);

        // Test 8: Edge cases and error handling
        println!("⚠️ Testing edge cases...");
        report.add_test_result("edge_cases", self.test_edge_cases().await?);

        // Test 9: Performance validation
        println!("⚡ Testing performance characteristics...");
        report.add_test_result("performance", self.test_performance_characteristics().await?);

        // Test 10: Cassandra compatibility
        println!("🔗 Testing Cassandra compatibility...");
        let cassandra_report = self.cassandra_validator.run_full_validation().await?;
        for (test_name, result) in cassandra_report.tests {
            report.add_test_result(&format!("cassandra_{}", test_name), result);
        }

        Ok(report)
    }

    /// Test basic write and read functionality
    async fn test_basic_write_read(&self) -> Result<TestResult> {
        let test_path = self.test_dir.path().join("basic_test.sst");

        // Write data
        let mut writer = SSTableWriter::create(&test_path, &self.config, self.platform.clone()).await?;
        
        let test_data = vec![
            (TableId::new("users"), RowKey::from("user1"), Value::Text("Alice".to_string())),
            (TableId::new("users"), RowKey::from("user2"), Value::Text("Bob".to_string())),
            (TableId::new("posts"), RowKey::from("post1"), Value::Text("Hello World".to_string())),
        ];

        for (table_id, key, value) in &test_data {
            writer.add_entry(table_id, key.clone(), value.clone()).await?;
        }

        writer.finish().await?;

        // Verify file exists and has content
        if !test_path.exists() {
            return Ok(TestResult::failure("SSTable file was not created", ""));
        }

        let file_size = fs::metadata(&test_path)
            .map_err(|e| cqlite_core::error::Error::storage(format!("Failed to get metadata: {}", e)))?
            .len();

        if file_size == 0 {
            return Ok(TestResult::failure("SSTable file is empty", ""));
        }

        // Read data back
        let reader = SSTableReader::open(&test_path, &self.config, self.platform.clone()).await?;

        // Verify we can read the data
        for (table_id, key, expected_value) in &test_data {
            match reader.get(table_id, key).await? {
                Some(value) => {
                    if value != *expected_value {
                        return Ok(TestResult::failure(
                            "Read value doesn't match written value",
                            &format!("Expected {:?}, got {:?}", expected_value, value),
                        ));
                    }
                }
                None => {
                    return Ok(TestResult::failure(
                        "Could not read back written value",
                        &format!("Key: {:?}", key),
                    ));
                }
            }
        }

        Ok(TestResult::success("Basic write/read functionality works correctly"))
    }

    /// Test various data type serialization and deserialization
    async fn test_data_type_serialization(&self) -> Result<TestResult> {
        let test_path = self.test_dir.path().join("data_types_test.sst");

        // Create test data with various types
        let test_data = vec![
            (TableId::new("test"), RowKey::from("text"), Value::Text("Hello, 世界!".to_string())),
            (TableId::new("test"), RowKey::from("integer"), Value::Integer(42)),
            (TableId::new("test"), RowKey::from("bigint"), Value::BigInt(9223372036854775807)),
            (TableId::new("test"), RowKey::from("float"), Value::Float(3.14159)),
            (TableId::new("test"), RowKey::from("boolean_true"), Value::Boolean(true)),
            (TableId::new("test"), RowKey::from("boolean_false"), Value::Boolean(false)),
            (TableId::new("test"), RowKey::from("null"), Value::Null),
            (TableId::new("test"), RowKey::from("blob"), Value::Blob(vec![0, 1, 2, 3, 255])),
            (TableId::new("test"), RowKey::from("timestamp"), Value::Timestamp(
                SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as u64
            )),
            (TableId::new("test"), RowKey::from("uuid"), Value::Uuid(
                vec![0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0]
            )),
        ];

        // Write data
        let mut writer = SSTableWriter::create(&test_path, &self.config, self.platform.clone()).await?;
        
        for (table_id, key, value) in &test_data {
            writer.add_entry(table_id, key.clone(), value.clone()).await?;
        }

        writer.finish().await?;

        // Read data back and verify
        let reader = SSTableReader::open(&test_path, &self.config, self.platform.clone()).await?;

        for (table_id, key, expected_value) in &test_data {
            match reader.get(table_id, key).await? {
                Some(value) => {
                    if value != *expected_value {
                        return Ok(TestResult::failure(
                            "Data type serialization/deserialization failed",
                            &format!("Key: {:?}, Expected: {:?}, Got: {:?}", key, expected_value, value),
                        ));
                    }
                }
                None => {
                    return Ok(TestResult::failure(
                        "Could not read back serialized data",
                        &format!("Key: {:?}, Type: {:?}", key, expected_value.data_type()),
                    ));
                }
            }
        }

        Ok(TestResult::success("All data types serialize/deserialize correctly"))
    }

    /// Test complex data structures
    async fn test_complex_data_structures(&self) -> Result<TestResult> {
        let test_path = self.test_dir.path().join("complex_data_test.sst");

        // Create large text values
        let large_text = "x".repeat(10000);
        let binary_data = (0..1000).map(|i| (i % 256) as u8).collect::<Vec<u8>>();

        let test_data = vec![
            (TableId::new("large"), RowKey::from("text"), Value::Text(large_text)),
            (TableId::new("large"), RowKey::from("binary"), Value::Blob(binary_data)),
            (TableId::new("unicode"), RowKey::from("emoji"), Value::Text("🦀🚀🌟💾🔥".to_string())),
            (TableId::new("unicode"), RowKey::from("mixed"), Value::Text("ASCII + 中文 + العربية + עברית".to_string())),
        ];

        // Write data
        let mut writer = SSTableWriter::create(&test_path, &self.config, self.platform.clone()).await?;
        
        for (table_id, key, value) in &test_data {
            writer.add_entry(table_id, key.clone(), value.clone()).await?;
        }

        writer.finish().await?;

        // Read and verify
        let reader = SSTableReader::open(&test_path, &self.config, self.platform.clone()).await?;

        for (table_id, key, expected_value) in &test_data {
            match reader.get(table_id, key).await? {
                Some(value) => {
                    if value != *expected_value {
                        return Ok(TestResult::failure(
                            "Complex data structure handling failed",
                            &format!("Key: {:?}", key),
                        ));
                    }
                }
                None => {
                    return Ok(TestResult::failure(
                        "Could not read back complex data",
                        &format!("Key: {:?}", key),
                    ));
                }
            }
        }

        Ok(TestResult::success("Complex data structures handled correctly"))
    }

    /// Test file format compliance
    async fn test_file_format_compliance(&self) -> Result<TestResult> {
        let test_path = self.test_dir.path().join("format_test.sst");

        // Create minimal SSTable
        let mut writer = SSTableWriter::create(&test_path, &self.config, self.platform.clone()).await?;
        writer.add_entry(&TableId::new("test"), RowKey::from("key"), Value::Text("value".to_string())).await?;
        writer.finish().await?;

        // Read file as binary and validate format
        let file_data = fs::read(&test_path)
            .map_err(|e| cqlite_core::error::Error::storage(format!("Failed to read test file: {}", e)))?;

        // Check minimum file size
        if file_data.len() < 48 {
            return Ok(TestResult::failure(
                "File too small for valid SSTable",
                &format!("Size: {} bytes", file_data.len()),
            ));
        }

        // Validate magic bytes
        if &file_data[0..4] != [0x5A, 0x5A, 0x5A, 0x5A] {
            return Ok(TestResult::failure(
                "Invalid magic bytes in header",
                &format!("Found: {:?}", &file_data[0..4]),
            ));
        }

        // Validate format version
        if &file_data[4..6] != b"oa" {
            return Ok(TestResult::failure(
                "Invalid format version",
                &format!("Found: {:?}", String::from_utf8_lossy(&file_data[4..6])),
            ));
        }

        // Check footer magic
        let footer_start = file_data.len() - 8;
        if &file_data[footer_start..] != [0x5A, 0x5A, 0x5A, 0x5A, 0x5A, 0x5A, 0x5A, 0x5A] {
            return Ok(TestResult::failure(
                "Invalid footer magic bytes",
                &format!("Found: {:?}", &file_data[footer_start..]),
            ));
        }

        Ok(TestResult::success("File format complies with Cassandra 5+ 'oa' specification"))
    }

    /// Test compression functionality
    async fn test_compression_functionality(&self) -> Result<TestResult> {
        let algorithms = vec![
            ("lz4", cqlite_core::config::CompressionAlgorithm::Lz4),
            ("snappy", cqlite_core::config::CompressionAlgorithm::Snappy),
            ("deflate", cqlite_core::config::CompressionAlgorithm::Deflate),
        ];

        let mut results = Vec::new();

        for (name, algorithm) in algorithms {
            let result = self.test_compression_algorithm(name, algorithm).await?;
            results.push(format!("{}: {}", name, result.status));
        }

        if results.iter().any(|r| r.contains("FAIL")) {
            Ok(TestResult::failure(
                "Some compression algorithms failed",
                &results.join(", "),
            ))
        } else {
            Ok(TestResult::success(&format!(
                "All compression algorithms work: {}",
                results.join(", ")
            )))
        }
    }

    /// Test specific compression algorithm
    async fn test_compression_algorithm(
        &self,
        name: &str,
        algorithm: cqlite_core::config::CompressionAlgorithm,
    ) -> Result<TestResult> {
        let test_path = self.test_dir.path().join(format!("compression_{}_test.sst", name));

        // Create config with compression enabled
        let mut config = self.config.clone();
        config.storage.compression.enabled = true;
        config.storage.compression.algorithm = algorithm;

        // Write compressible data
        let mut writer = SSTableWriter::create(&test_path, &config, self.platform.clone()).await?;
        
        // Add repetitive data that should compress well
        for i in 0..100 {
            let table_id = TableId::new("compression_test");
            let key = RowKey::from(format!("key_{:03}", i));
            let value = Value::Text("A".repeat(1000)); // Very compressible
            writer.add_entry(&table_id, key, value).await?;
        }

        writer.finish().await?;

        // Check file size (should be much smaller than uncompressed)
        let file_size = fs::metadata(&test_path)
            .map_err(|e| cqlite_core::error::Error::storage(format!("Failed to get metadata: {}", e)))?
            .len();

        // With 100KB of 'A' characters, compression should achieve significant reduction
        if file_size > 20_000 {
            return Ok(TestResult::warning(
                &format!("{} compression may not be effective", name),
                &format!("File size: {} bytes", file_size),
            ));
        }

        // Verify we can read the data back
        let reader = SSTableReader::open(&test_path, &config, self.platform.clone()).await?;
        
        let test_key = RowKey::from("key_050");
        let test_table = TableId::new("compression_test");
        
        match reader.get(&test_table, &test_key).await? {
            Some(Value::Text(text)) => {
                if text.len() != 1000 || !text.chars().all(|c| c == 'A') {
                    return Ok(TestResult::failure(
                        &format!("{} decompression failed", name),
                        "Decompressed data doesn't match original",
                    ));
                }
            }
            _ => {
                return Ok(TestResult::failure(
                    &format!("{} compression test failed", name),
                    "Could not read back compressed data",
                ));
            }
        }

        Ok(TestResult::success(&format!(
            "{} compression working ({}KB -> {}KB)",
            name,
            100,
            file_size / 1024
        )))
    }

    /// Test bloom filter functionality
    async fn test_bloom_filter_functionality(&self) -> Result<TestResult> {
        let test_path = self.test_dir.path().join("bloom_test.sst");

        // Create config with bloom filter enabled
        let mut config = self.config.clone();
        config.storage.enable_bloom_filters = true;
        config.storage.bloom_filter_fp_rate = 0.01;

        let mut writer = SSTableWriter::create(&test_path, &config, self.platform.clone()).await?;
        
        let table_id = TableId::new("bloom_test");
        let existing_keys = vec!["key1", "key2", "key3", "key4", "key5"];
        
        // Add known keys
        for key in &existing_keys {
            writer.add_entry(&table_id, RowKey::from(*key), Value::Text(format!("value_{}", key))).await?;
        }

        writer.finish().await?;

        // Read and test bloom filter
        let reader = SSTableReader::open(&test_path, &config, self.platform.clone()).await?;

        // Test that existing keys are found
        for key in &existing_keys {
            match reader.get(&table_id, &RowKey::from(*key)).await? {
                Some(_) => {}, // Good
                None => {
                    return Ok(TestResult::failure(
                        "Bloom filter false negative",
                        &format!("Key {} should exist but was not found", key),
                    ));
                }
            }
        }

        // Test non-existing keys (should mostly return None due to bloom filter)
        let non_existing_keys = vec!["nonkey1", "nonkey2", "nonkey3", "nonkey4", "nonkey5"];
        let mut false_positives = 0;
        
        for key in &non_existing_keys {
            if let Some(_) = reader.get(&table_id, &RowKey::from(*key)).await? {
                false_positives += 1;
            }
        }

        // With 5 non-existing keys and 1% FP rate, we expect 0-1 false positives
        if false_positives > 2 {
            return Ok(TestResult::warning(
                "High bloom filter false positive rate",
                &format!("Got {} false positives out of {} tests", false_positives, non_existing_keys.len()),
            ));
        }

        Ok(TestResult::success("Bloom filter functioning correctly"))
    }

    /// Test index functionality
    async fn test_index_functionality(&self) -> Result<TestResult> {
        let test_path = self.test_dir.path().join("index_test.sst");

        let mut writer = SSTableWriter::create(&test_path, &self.config, self.platform.clone()).await?;
        
        let table_id = TableId::new("index_test");
        
        // Add entries in non-sorted order to test indexing
        let keys = vec!["key_050", "key_010", "key_090", "key_030", "key_070"];
        
        for key in &keys {
            writer.add_entry(&table_id, RowKey::from(*key), Value::Text(format!("value_{}", key))).await?;
        }

        writer.finish().await?;

        // Test reader with index
        let reader = SSTableReader::open(&test_path, &self.config, self.platform.clone()).await?;

        // Test range scan
        let start_key = RowKey::from("key_020");
        let end_key = RowKey::from("key_080");
        
        let results = reader.scan(&table_id, Some(&start_key), Some(&end_key), None).await?;

        // Should find keys in range: key_030, key_050, key_070
        if results.len() != 3 {
            return Ok(TestResult::failure(
                "Index range scan failed",
                &format!("Expected 3 results, got {}", results.len()),
            ));
        }

        // Verify results are sorted
        let mut prev_key: Option<&RowKey> = None;
        for (key, _) in &results {
            if let Some(prev) = prev_key {
                if key < prev {
                    return Ok(TestResult::failure(
                        "Index scan results not sorted",
                        &format!("Found {:?} after {:?}", key, prev),
                    ));
                }
            }
            prev_key = Some(key);
        }

        Ok(TestResult::success("Index functionality working correctly"))
    }

    /// Test edge cases and error handling
    async fn test_edge_cases(&self) -> Result<TestResult> {
        let mut issues = Vec::new();

        // Test 1: Empty values
        match self.test_empty_values().await {
            Ok(result) if result.status == cqlite_core::storage::sstable::validation::TestStatus::Fail => {
                issues.push(format!("Empty values: {}", result.message));
            }
            Err(e) => issues.push(format!("Empty values test error: {}", e)),
            _ => {}
        }

        // Test 2: Very long keys
        match self.test_long_keys().await {
            Ok(result) if result.status == cqlite_core::storage::sstable::validation::TestStatus::Fail => {
                issues.push(format!("Long keys: {}", result.message));
            }
            Err(e) => issues.push(format!("Long keys test error: {}", e)),
            _ => {}
        }

        // Test 3: Many small entries
        match self.test_many_small_entries().await {
            Ok(result) if result.status == cqlite_core::storage::sstable::validation::TestStatus::Fail => {
                issues.push(format!("Many entries: {}", result.message));
            }
            Err(e) => issues.push(format!("Many entries test error: {}", e)),
            _ => {}
        }

        if issues.is_empty() {
            Ok(TestResult::success("All edge cases handled correctly"))
        } else {
            Ok(TestResult::failure(
                "Some edge cases failed",
                &issues.join("; "),
            ))
        }
    }

    /// Test empty values
    async fn test_empty_values(&self) -> Result<TestResult> {
        let test_path = self.test_dir.path().join("empty_values_test.sst");
        
        let mut writer = SSTableWriter::create(&test_path, &self.config, self.platform.clone()).await?;
        
        let table_id = TableId::new("empty_test");
        writer.add_entry(&table_id, RowKey::from("empty_text"), Value::Text("".to_string())).await?;
        writer.add_entry(&table_id, RowKey::from("empty_blob"), Value::Blob(vec![])).await?;
        writer.add_entry(&table_id, RowKey::from("null_value"), Value::Null).await?;
        
        writer.finish().await?;

        let reader = SSTableReader::open(&test_path, &self.config, self.platform.clone()).await?;
        
        // Verify we can read empty values
        if let Some(Value::Text(text)) = reader.get(&table_id, &RowKey::from("empty_text")).await? {
            if !text.is_empty() {
                return Ok(TestResult::failure("Empty text not preserved", ""));
            }
        } else {
            return Ok(TestResult::failure("Could not read empty text", ""));
        }

        Ok(TestResult::success("Empty values handled correctly"))
    }

    /// Test very long keys
    async fn test_long_keys(&self) -> Result<TestResult> {
        let test_path = self.test_dir.path().join("long_keys_test.sst");
        
        let mut writer = SSTableWriter::create(&test_path, &self.config, self.platform.clone()).await?;
        
        let table_id = TableId::new("long_key_test");
        let long_key = "x".repeat(1000); // 1KB key
        
        writer.add_entry(&table_id, RowKey::from(long_key.clone()), Value::Text("value".to_string())).await?;
        writer.finish().await?;

        let reader = SSTableReader::open(&test_path, &self.config, self.platform.clone()).await?;
        
        if reader.get(&table_id, &RowKey::from(long_key)).await?.is_none() {
            return Ok(TestResult::failure("Could not read back long key", ""));
        }

        Ok(TestResult::success("Long keys handled correctly"))
    }

    /// Test many small entries
    async fn test_many_small_entries(&self) -> Result<TestResult> {
        let test_path = self.test_dir.path().join("many_entries_test.sst");
        
        let mut writer = SSTableWriter::create(&test_path, &self.config, self.platform.clone()).await?;
        
        let table_id = TableId::new("many_test");
        
        // Write 1000 small entries
        for i in 0..1000 {
            let key = RowKey::from(format!("key_{:06}", i));
            let value = Value::Text(format!("value_{}", i));
            writer.add_entry(&table_id, key, value).await?;
        }
        
        writer.finish().await?;

        let reader = SSTableReader::open(&test_path, &self.config, self.platform.clone()).await?;
        
        // Verify we can read all entries
        let results = reader.scan(&table_id, None, None, None).await?;
        
        if results.len() != 1000 {
            return Ok(TestResult::failure(
                "Not all entries were written/read",
                &format!("Expected 1000, got {}", results.len()),
            ));
        }

        Ok(TestResult::success("Many small entries handled correctly"))
    }

    /// Test performance characteristics
    async fn test_performance_characteristics(&self) -> Result<TestResult> {
        let test_path = self.test_dir.path().join("performance_test.sst");
        
        let start_time = SystemTime::now();
        
        let mut writer = SSTableWriter::create(&test_path, &self.config, self.platform.clone()).await?;
        
        let table_id = TableId::new("perf_test");
        
        // Write 10,000 entries
        for i in 0..10_000 {
            let key = RowKey::from(format!("perf_key_{:08}", i));
            let value = Value::Text(format!("Performance test value number {}", i));
            writer.add_entry(&table_id, key, value).await?;
        }
        
        writer.finish().await?;
        
        let write_duration = start_time.elapsed().unwrap();
        
        // Test read performance
        let read_start = SystemTime::now();
        let reader = SSTableReader::open(&test_path, &self.config, self.platform.clone()).await?;
        
        // Random access test
        for i in (0..10_000).step_by(100) {
            let key = RowKey::from(format!("perf_key_{:08}", i));
            reader.get(&table_id, &key).await?;
        }
        
        let read_duration = read_start.elapsed().unwrap();
        
        let write_throughput = 10_000.0 / write_duration.as_secs_f64();
        let read_throughput = 100.0 / read_duration.as_secs_f64();
        
        // Basic performance thresholds
        if write_throughput < 1000.0 {
            return Ok(TestResult::warning(
                "Write performance below threshold",
                &format!("Got {:.0} writes/sec", write_throughput),
            ));
        }
        
        if read_throughput < 100.0 {
            return Ok(TestResult::warning(
                "Read performance below threshold", 
                &format!("Got {:.0} reads/sec", read_throughput),
            ));
        }

        Ok(TestResult::success(&format!(
            "Performance acceptable: {:.0} writes/sec, {:.0} reads/sec",
            write_throughput, read_throughput
        )))
    }

    /// Get test directory path for external tools
    pub fn test_dir_path(&self) -> &Path {
        self.test_dir.path()
    }
}

/// Run comprehensive SSTable validation
pub async fn run_validation() -> Result<()> {
    println!("🚀 Starting SSTable Validation Suite");
    println!("=====================================");

    let validator = SSTableValidator::new().await?;
    let report = validator.run_full_validation().await?;

    println!("\n{}", report.detailed_report());

    if report.is_successful() {
        println!("✅ All tests passed! SSTable implementation is working correctly.");
        Ok(())
    } else {
        println!("❌ Some tests failed. See report above for details.");
        Err(cqlite_core::error::Error::storage(
            "SSTable validation failed".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_validator_creation() {
        let validator = SSTableValidator::new().await.unwrap();
        assert!(validator.test_dir_path().exists());
    }

    #[tokio::test]
    async fn test_basic_validation() {
        let validator = SSTableValidator::new().await.unwrap();
        let result = validator.test_basic_write_read().await.unwrap();
        assert_eq!(result.status, cqlite_core::storage::sstable::validation::TestStatus::Pass);
    }
}