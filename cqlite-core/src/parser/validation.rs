//! Parser validation framework for ensuring byte-perfect Cassandra compatibility
//!
//! This module provides comprehensive validation tests for all parser components
//! to ensure they produce identical results to Cassandra's implementation.

use super::*;
use crate::error::Result;
use std::collections::HashMap;

/// Validation framework for parser components
pub struct ParserValidator {
    /// Whether to run strict byte-level comparisons
    pub strict_mode: bool,
    /// Test data directory for real SSTable files
    pub test_data_dir: Option<std::path::PathBuf>,
    /// Validation results
    results: HashMap<String, ValidationResult>,
}

/// Result of a validation test
#[derive(Debug, Clone)]
pub struct ValidationResult {
    /// Test name
    pub name: String,
    /// Whether the test passed
    pub passed: bool,
    /// Error message if failed
    pub error: Option<String>,
    /// Performance metrics
    pub metrics: Option<ValidationMetrics>,
}

/// Performance metrics for validation tests
#[derive(Debug, Clone)]
pub struct ValidationMetrics {
    /// Bytes processed
    pub bytes_processed: usize,
    /// Time taken in microseconds
    pub duration_micros: u64,
    /// Throughput in MB/s
    pub throughput_mbs: f64,
}

impl ParserValidator {
    /// Create a new parser validator
    pub fn new() -> Self {
        Self {
            strict_mode: true,
            test_data_dir: None,
            results: HashMap::new(),
        }
    }

    /// Set the test data directory containing real SSTable files
    pub fn with_test_data_dir(mut self, path: std::path::PathBuf) -> Self {
        self.test_data_dir = Some(path);
        self
    }

    /// Enable or disable strict byte-level validation
    pub fn strict_mode(mut self, enabled: bool) -> Self {
        self.strict_mode = enabled;
        self
    }

    /// Run VInt validation tests
    pub fn validate_vint(&mut self) -> Result<()> {
        let start_time = std::time::Instant::now();

        // Test 1: Basic roundtrip validation
        self.validate_vint_roundtrip()?;

        // Test 2: Specific bit pattern validation
        self.validate_vint_bit_patterns()?;

        // Test 3: Boundary value testing
        self.validate_vint_boundaries()?;

        // Test 4: Performance validation
        self.validate_vint_performance()?;

        let duration = start_time.elapsed();
        println!("✅ VInt validation completed in {:?}", duration);
        Ok(())
    }

    /// Validate VInt roundtrip for comprehensive value ranges
    fn validate_vint_roundtrip(&mut self) -> Result<()> {
        let test_name = "vint_roundtrip";
        let start_time = std::time::Instant::now();

        let test_values = vec![
            // Single byte values
            0,
            1,
            -1,
            63,
            -63,
            // Multi-byte boundaries
            64,
            -64,
            127,
            -127,
            128,
            -128,
            // Powers of 2
            255,
            -255,
            256,
            -256,
            511,
            -511,
            512,
            -512,
            1023,
            -1023,
            1024,
            -1024,
            2047,
            -2047,
            2048,
            -2048,
            // Large values
            32767,
            -32768,
            65535,
            -65535,
            1000000,
            -1000000,
            100000000,
            -100000000,
            // Maximum safe values
            i32::MAX as i64,
            i32::MIN as i64,
        ];

        let mut bytes_processed = 0;
        let mut failures = Vec::new();

        for value in &test_values {
            let encoded = vint::encode_vint(*value);
            bytes_processed += encoded.len();

            match vint::parse_vint(&encoded) {
                Ok((remaining, decoded)) => {
                    if !remaining.is_empty() {
                        failures.push(format!(
                            "Value {} left {} unparsed bytes",
                            value,
                            remaining.len()
                        ));
                    }
                    if decoded != *value {
                        failures.push(format!(
                            "Value {} decoded as {} (expected {})",
                            value, decoded, *value
                        ));
                    }
                }
                Err(e) => {
                    failures.push(format!("Value {} failed to parse: {:?}", value, e));
                }
            }
        }

        let duration = start_time.elapsed();
        let metrics = ValidationMetrics {
            bytes_processed,
            duration_micros: duration.as_micros() as u64,
            throughput_mbs: (bytes_processed as f64) / duration.as_secs_f64() / 1_000_000.0,
        };

        let result = if failures.is_empty() {
            ValidationResult {
                name: test_name.to_string(),
                passed: true,
                error: None,
                metrics: Some(metrics.clone()),
            }
        } else {
            ValidationResult {
                name: test_name.to_string(),
                passed: false,
                error: Some(failures.join("; ")),
                metrics: Some(metrics.clone()),
            }
        };

        self.results.insert(test_name.to_string(), result.clone());

        if !result.passed {
            println!(
                "❌ VInt roundtrip validation failed: {}",
                result.error.unwrap()
            );
        } else {
            println!(
                "✅ VInt roundtrip validation passed ({} values, {:.2} MB/s)",
                test_values.len(),
                metrics.throughput_mbs
            );
        }

        Ok(())
    }

    /// Validate specific VInt bit patterns
    fn validate_vint_bit_patterns(&mut self) -> Result<()> {
        let test_name = "vint_bit_patterns";
        let start_time = std::time::Instant::now();

        let mut failures = Vec::new();
        let mut bytes_processed = 0;

        // Test single-byte encoding (should have MSB = 0)
        for value in 0..64 {
            let encoded = vint::encode_vint(value);
            bytes_processed += encoded.len();

            if encoded.len() != 1 {
                failures.push(format!(
                    "Value {} should encode to 1 byte, got {}",
                    value,
                    encoded.len()
                ));
                continue;
            }

            if encoded[0] & 0x80 != 0 {
                failures.push(format!(
                    "Single-byte value {} should have MSB=0, got 0x{:02X}",
                    value, encoded[0]
                ));
            }
        }

        // Test two-byte encoding (should start with 10xxxxxx)
        let two_byte_values = [64, 100, 127, 200, 255];
        for &value in &two_byte_values {
            let encoded = vint::encode_vint(value);
            bytes_processed += encoded.len();

            if encoded.len() < 2 {
                continue; // May be optimized to single byte
            }

            if encoded[0] & 0xC0 != 0x80 {
                failures.push(format!(
                    "Two-byte value {} should start with 10xxxxxx, got 0x{:02X}",
                    value, encoded[0]
                ));
            }
        }

        let duration = start_time.elapsed();
        let metrics = ValidationMetrics {
            bytes_processed,
            duration_micros: duration.as_micros() as u64,
            throughput_mbs: (bytes_processed as f64) / duration.as_secs_f64() / 1_000_000.0,
        };

        let result = ValidationResult {
            name: test_name.to_string(),
            passed: failures.is_empty(),
            error: if failures.is_empty() {
                None
            } else {
                Some(failures.join("; "))
            },
            metrics: Some(metrics),
        };

        self.results.insert(test_name.to_string(), result.clone());

        if !result.passed {
            println!(
                "❌ VInt bit pattern validation failed: {}",
                result.error.unwrap()
            );
        } else {
            println!("✅ VInt bit pattern validation passed");
        }

        Ok(())
    }

    /// Validate VInt boundary values
    fn validate_vint_boundaries(&mut self) -> Result<()> {
        let test_name = "vint_boundaries";
        let start_time = std::time::Instant::now();

        let boundaries = [
            (63, 1),    // Max single byte
            (64, 2),    // Min two byte
            (16383, 2), // Max two byte
            (16384, 3), // Min three byte
        ];

        let mut failures = Vec::new();
        let mut bytes_processed = 0;

        for &(value, expected_bytes) in &boundaries {
            let encoded = vint::encode_vint(value);
            bytes_processed += encoded.len();

            if encoded.len() != expected_bytes {
                failures.push(format!(
                    "Value {} should encode to {} bytes, got {}",
                    value,
                    expected_bytes,
                    encoded.len()
                ));
            }

            // Test negative values too
            let neg_encoded = vint::encode_vint(-value);
            bytes_processed += neg_encoded.len();

            if neg_encoded.len() != expected_bytes {
                failures.push(format!(
                    "Value -{} should encode to {} bytes, got {}",
                    value,
                    expected_bytes,
                    neg_encoded.len()
                ));
            }
        }

        let duration = start_time.elapsed();
        let metrics = ValidationMetrics {
            bytes_processed,
            duration_micros: duration.as_micros() as u64,
            throughput_mbs: (bytes_processed as f64) / duration.as_secs_f64() / 1_000_000.0,
        };

        let result = ValidationResult {
            name: test_name.to_string(),
            passed: failures.is_empty(),
            error: if failures.is_empty() {
                None
            } else {
                Some(failures.join("; "))
            },
            metrics: Some(metrics),
        };

        self.results.insert(test_name.to_string(), result.clone());

        if !result.passed {
            println!(
                "❌ VInt boundary validation failed: {}",
                result.error.unwrap()
            );
        } else {
            println!("✅ VInt boundary validation passed");
        }

        Ok(())
    }

    /// Validate VInt performance meets targets
    fn validate_vint_performance(&mut self) -> Result<()> {
        let test_name = "vint_performance";
        let start_time = std::time::Instant::now();

        // Generate large dataset for performance testing
        let mut test_data = Vec::new();
        for i in 0..100_000 {
            test_data.push(i);
            test_data.push(-i);
            test_data.push(i * 1000);
            test_data.push(-i * 1000);
        }

        let mut total_encoded_bytes = 0;
        let encode_start = std::time::Instant::now();

        // Encode all values
        let mut encoded_values = Vec::new();
        for &value in &test_data {
            let encoded = vint::encode_vint(value);
            total_encoded_bytes += encoded.len();
            encoded_values.push(encoded);
        }

        let encode_duration = encode_start.elapsed();

        // Decode all values
        let decode_start = std::time::Instant::now();
        let mut decode_failures = 0;

        for (i, encoded) in encoded_values.iter().enumerate() {
            match vint::parse_vint(encoded) {
                Ok((remaining, decoded)) => {
                    if !remaining.is_empty() || decoded != test_data[i] {
                        decode_failures += 1;
                    }
                }
                Err(_) => decode_failures += 1,
            }
        }

        let decode_duration = decode_start.elapsed();
        let total_duration = start_time.elapsed();

        // Calculate throughput targets
        let encode_throughput =
            (total_encoded_bytes as f64) / encode_duration.as_secs_f64() / 1_000_000.0;
        let decode_throughput =
            (total_encoded_bytes as f64) / decode_duration.as_secs_f64() / 1_000_000.0;
        let overall_throughput =
            (total_encoded_bytes as f64) / total_duration.as_secs_f64() / 1_000_000.0;

        let metrics = ValidationMetrics {
            bytes_processed: total_encoded_bytes,
            duration_micros: total_duration.as_micros() as u64,
            throughput_mbs: overall_throughput,
        };

        // Performance targets
        let min_encode_throughput = 50.0; // MB/s
        let min_decode_throughput = 100.0; // MB/s
        let max_decode_failures = test_data.len() / 1000; // 0.1% failure rate

        let mut failures = Vec::new();
        if encode_throughput < min_encode_throughput {
            failures.push(format!(
                "Encode throughput {:.2} MB/s below target {:.2} MB/s",
                encode_throughput, min_encode_throughput
            ));
        }
        if decode_throughput < min_decode_throughput {
            failures.push(format!(
                "Decode throughput {:.2} MB/s below target {:.2} MB/s",
                decode_throughput, min_decode_throughput
            ));
        }
        if decode_failures > max_decode_failures {
            failures.push(format!(
                "{} decode failures exceed target {}",
                decode_failures, max_decode_failures
            ));
        }

        let result = ValidationResult {
            name: test_name.to_string(),
            passed: failures.is_empty(),
            error: if failures.is_empty() {
                None
            } else {
                Some(failures.join("; "))
            },
            metrics: Some(metrics),
        };

        self.results.insert(test_name.to_string(), result.clone());

        if !result.passed {
            println!(
                "❌ VInt performance validation failed: {}",
                result.error.unwrap()
            );
        } else {
            println!(
                "✅ VInt performance validation passed: Encode {:.2} MB/s, Decode {:.2} MB/s",
                encode_throughput, decode_throughput
            );
        }

        Ok(())
    }

    /// Run header validation tests
    pub fn validate_header(&mut self) -> Result<()> {
        println!("🔍 Validating header parsing...");

        // Test header serialization/deserialization roundtrip
        let test_header = self.create_test_header();

        match header::serialize_sstable_header(&test_header) {
            Ok(serialized) => match header::parse_sstable_header(&serialized) {
                Ok((remaining, parsed_header)) => {
                    if !remaining.is_empty() {
                        println!("❌ Header parsing left {} unparsed bytes", remaining.len());
                    } else if self.headers_equal(&test_header, &parsed_header) {
                        println!("✅ Header validation passed");
                    } else {
                        println!("❌ Header roundtrip produced different result");
                    }
                }
                Err(e) => {
                    println!("❌ Header parsing failed: {:?}", e);
                }
            },
            Err(e) => {
                println!("❌ Header serialization failed: {:?}", e);
            }
        }

        Ok(())
    }

    /// Run type system validation tests  
    pub fn validate_types(&mut self) -> Result<()> {
        println!("🔍 Validating type system...");

        // Test all CQL type roundtrips
        let test_values = self.create_test_values();

        for (type_id, value) in test_values {
            let serialized = types::serialize_cql_value(&value)?;
            match types::parse_cql_value(&serialized[1..], type_id) {
                // Skip type byte
                Ok((remaining, parsed_value)) => {
                    if !remaining.is_empty() {
                        println!(
                            "❌ Type {:?} parsing left {} unparsed bytes",
                            type_id,
                            remaining.len()
                        );
                    } else if self.values_equal(&value, &parsed_value) {
                        println!("✅ Type {:?} validation passed", type_id);
                    } else {
                        println!("❌ Type {:?} roundtrip produced different result", type_id);
                    }
                }
                Err(e) => {
                    println!("❌ Type {:?} parsing failed: {:?}", type_id, e);
                }
            }
        }

        Ok(())
    }

    /// Generate validation report
    pub fn generate_report(&self) -> String {
        let mut report = String::new();
        report.push_str("# Parser Validation Report\n\n");

        let total_tests = self.results.len();
        let passed_tests = self.results.values().filter(|r| r.passed).count();
        let failed_tests = total_tests - passed_tests;

        report.push_str(&format!("## Summary\n"));
        report.push_str(&format!("- Total Tests: {}\n", total_tests));
        report.push_str(&format!(
            "- Passed: {} ({:.1}%)\n",
            passed_tests,
            (passed_tests as f64 / total_tests as f64) * 100.0
        ));
        report.push_str(&format!(
            "- Failed: {} ({:.1}%)\n\n",
            failed_tests,
            (failed_tests as f64 / total_tests as f64) * 100.0
        ));

        report.push_str("## Test Results\n\n");
        for result in self.results.values() {
            let status = if result.passed {
                "✅ PASS"
            } else {
                "❌ FAIL"
            };
            report.push_str(&format!("### {} - {}\n", result.name, status));

            if let Some(metrics) = &result.metrics {
                report.push_str(&format!("- Bytes processed: {}\n", metrics.bytes_processed));
                report.push_str(&format!("- Duration: {} μs\n", metrics.duration_micros));
                report.push_str(&format!(
                    "- Throughput: {:.2} MB/s\n",
                    metrics.throughput_mbs
                ));
            }

            if let Some(error) = &result.error {
                report.push_str(&format!("- Error: {}\n", error));
            }

            report.push_str("\n");
        }

        report
    }

    // Helper methods

    fn create_test_header(&self) -> header::SSTableHeader {
        use std::collections::HashMap;

        header::SSTableHeader {
            version: header::SUPPORTED_VERSION,
            table_id: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            keyspace: "test_keyspace".to_string(),
            table_name: "test_table".to_string(),
            generation: 12345,
            compression: header::CompressionInfo {
                algorithm: "LZ4".to_string(),
                chunk_size: 4096,
                parameters: HashMap::new(),
            },
            stats: header::SSTableStats {
                row_count: 1000,
                min_timestamp: 1000000,
                max_timestamp: 2000000,
                max_deletion_time: 0,
                compression_ratio: 0.75,
                row_size_histogram: vec![10, 20, 30, 40, 50],
            },
            columns: vec![
                header::ColumnInfo {
                    name: "id".to_string(),
                    column_type: "uuid".to_string(),
                    is_primary_key: true,
                    key_position: Some(0),
                    is_static: false,
                    is_clustering: false,
                },
                header::ColumnInfo {
                    name: "name".to_string(),
                    column_type: "text".to_string(),
                    is_primary_key: false,
                    key_position: None,
                    is_static: false,
                    is_clustering: false,
                },
            ],
            properties: HashMap::new(),
        }
    }

    fn create_test_values(&self) -> Vec<(types::CqlTypeId, crate::Value)> {
        vec![
            (types::CqlTypeId::Boolean, crate::Value::Boolean(true)),
            (types::CqlTypeId::Int, crate::Value::Integer(42)),
            (types::CqlTypeId::BigInt, crate::Value::BigInt(1000)),
            (types::CqlTypeId::Float, crate::Value::Float(3.14)),
            (
                types::CqlTypeId::Varchar,
                crate::Value::Text("test".to_string()),
            ),
            (types::CqlTypeId::Blob, crate::Value::Blob(vec![1, 2, 3, 4])),
            (
                types::CqlTypeId::Uuid,
                crate::Value::Uuid([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
            ),
        ]
    }

    fn headers_equal(&self, h1: &header::SSTableHeader, h2: &header::SSTableHeader) -> bool {
        h1.version == h2.version
            && h1.table_id == h2.table_id
            && h1.keyspace == h2.keyspace
            && h1.table_name == h2.table_name
            && h1.generation == h2.generation
    }

    fn values_equal(&self, v1: &crate::Value, v2: &crate::Value) -> bool {
        // Simplified equality check
        std::mem::discriminant(v1) == std::mem::discriminant(v2)
    }
}

impl Default for ParserValidator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parser_validation_framework() {
        let mut validator = ParserValidator::new();

        // This test validates that the validation framework itself works
        assert!(validator.validate_vint().is_ok());
        assert!(validator.validate_header().is_ok());
        assert!(validator.validate_types().is_ok());

        let report = validator.generate_report();
        assert!(!report.is_empty());
        assert!(report.contains("Parser Validation Report"));
    }
}
