//! Parser performance benchmarks
//!
//! This module provides comprehensive benchmarking for all parser components
//! to ensure they meet performance targets for large-scale production use.

use super::*;
use crate::error::Result;
use std::time::{Duration, Instant};

/// Benchmark suite for parser performance
pub struct ParserBenchmarks {
    /// Minimum required throughput in MB/s
    pub min_throughput_mbs: f64,
    /// Target file size for benchmarks (in bytes)
    pub target_file_size: usize,
    /// Number of iterations for each benchmark
    pub iterations: usize,
    /// Results from benchmark runs
    results: Vec<BenchmarkResult>,
}

/// Result from a single benchmark run
#[derive(Debug, Clone)]
pub struct BenchmarkResult {
    /// Benchmark name
    pub name: String,
    /// Bytes processed
    pub bytes_processed: usize,
    /// Total duration
    pub duration: Duration,
    /// Throughput in MB/s
    pub throughput_mbs: f64,
    /// Memory usage in bytes
    pub memory_usage: Option<usize>,
    /// Whether benchmark met targets
    pub meets_targets: bool,
}

impl ParserBenchmarks {
    /// Create new benchmark suite with default targets
    pub fn new() -> Self {
        Self {
            min_throughput_mbs: 100.0, // 100 MB/s minimum for 1GB files in 10s
            target_file_size: 1024 * 1024 * 1024, // 1GB
            iterations: 3,
            results: Vec::new(),
        }
    }

    /// Set minimum throughput target
    pub fn with_min_throughput(mut self, mbs: f64) -> Self {
        self.min_throughput_mbs = mbs;
        self
    }

    /// Set target file size for benchmarks
    pub fn with_target_file_size(mut self, bytes: usize) -> Self {
        self.target_file_size = bytes;
        self
    }

    /// Run VInt encoding/decoding benchmarks
    pub fn benchmark_vint(&mut self) -> Result<()> {
        println!("🚀 Running VInt performance benchmarks...");

        // Generate test data
        let test_data = self.generate_vint_test_data();

        // Benchmark encoding
        let encode_result = self.benchmark_vint_encoding(&test_data)?;
        self.results.push(encode_result.clone());

        // Benchmark decoding
        let decode_result = self.benchmark_vint_decoding(&test_data)?;
        self.results.push(decode_result.clone());

        println!("✅ VInt encoding: {:.2} MB/s", encode_result.throughput_mbs);
        println!("✅ VInt decoding: {:.2} MB/s", decode_result.throughput_mbs);

        Ok(())
    }

    /// Benchmark VInt encoding performance
    fn benchmark_vint_encoding(&self, test_data: &[i64]) -> Result<BenchmarkResult> {
        let mut best_duration = Duration::MAX;
        let mut total_bytes = 0;

        for _ in 0..self.iterations {
            let start = Instant::now();

            for &value in test_data {
                let encoded = vint::encode_vint(value);
                total_bytes += encoded.len();
            }

            let duration = start.elapsed();
            if duration < best_duration {
                best_duration = duration;
            }
        }

        let throughput_mbs = (total_bytes as f64) / best_duration.as_secs_f64() / 1_000_000.0;
        let meets_targets = throughput_mbs >= self.min_throughput_mbs;

        Ok(BenchmarkResult {
            name: "vint_encoding".to_string(),
            bytes_processed: total_bytes,
            duration: best_duration,
            throughput_mbs,
            memory_usage: None,
            meets_targets,
        })
    }

    /// Benchmark VInt decoding performance
    fn benchmark_vint_decoding(&self, test_data: &[i64]) -> Result<BenchmarkResult> {
        // Pre-encode all test data
        let mut encoded_data = Vec::new();
        let mut total_bytes = 0;

        for &value in test_data {
            let encoded = vint::encode_vint(value);
            total_bytes += encoded.len();
            encoded_data.push(encoded);
        }

        let mut best_duration = Duration::MAX;

        for _ in 0..self.iterations {
            let start = Instant::now();

            for encoded in &encoded_data {
                match vint::parse_vint(encoded) {
                    Ok(_) => {}
                    Err(_) => {
                        return Err(crate::Error::corruption(
                            "VInt decode failed during benchmark",
                        ))
                    }
                }
            }

            let duration = start.elapsed();
            if duration < best_duration {
                best_duration = duration;
            }
        }

        let throughput_mbs = (total_bytes as f64) / best_duration.as_secs_f64() / 1_000_000.0;
        let meets_targets = throughput_mbs >= self.min_throughput_mbs;

        Ok(BenchmarkResult {
            name: "vint_decoding".to_string(),
            bytes_processed: total_bytes,
            duration: best_duration,
            throughput_mbs,
            memory_usage: None,
            meets_targets,
        })
    }

    /// Run header parsing benchmarks
    pub fn benchmark_header(&mut self) -> Result<()> {
        println!("🚀 Running header parsing benchmarks...");

        // Create test header
        let test_header = self.create_benchmark_header();
        let serialized = header::serialize_sstable_header(&test_header)?;

        let mut best_duration = Duration::MAX;
        let iterations = self.target_file_size / serialized.len(); // Simulate many headers

        for _ in 0..self.iterations {
            let start = Instant::now();

            for _ in 0..iterations {
                match header::parse_sstable_header(&serialized) {
                    Ok(_) => {}
                    Err(_) => {
                        return Err(crate::Error::corruption(
                            "Header parse failed during benchmark",
                        ))
                    }
                }
            }

            let duration = start.elapsed();
            if duration < best_duration {
                best_duration = duration;
            }
        }

        let total_bytes = serialized.len() * iterations;
        let throughput_mbs = (total_bytes as f64) / best_duration.as_secs_f64() / 1_000_000.0;
        let meets_targets = throughput_mbs >= self.min_throughput_mbs;

        let result = BenchmarkResult {
            name: "header_parsing".to_string(),
            bytes_processed: total_bytes,
            duration: best_duration,
            throughput_mbs,
            memory_usage: None,
            meets_targets,
        };

        self.results.push(result.clone());
        println!("✅ Header parsing: {:.2} MB/s", result.throughput_mbs);

        Ok(())
    }

    /// Run type system benchmarks
    pub fn benchmark_types(&mut self) -> Result<()> {
        println!("🚀 Running type system benchmarks...");

        let test_values = self.create_benchmark_values();
        let mut total_bytes = 0;
        let mut best_duration = Duration::MAX;

        // Calculate iterations to reach target file size
        let mut sample_size = 0;
        for (_, value) in &test_values {
            if let Ok(serialized) = types::serialize_cql_value(value) {
                sample_size += serialized.len();
            }
        }

        let iterations = if sample_size > 0 {
            self.target_file_size / sample_size
        } else {
            1000
        };

        for _ in 0..self.iterations {
            let start = Instant::now();

            for _ in 0..iterations {
                for (type_id, value) in &test_values {
                    if let Ok(serialized) = types::serialize_cql_value(value) {
                        total_bytes += serialized.len();

                        // Test parsing (skip type byte for parse_cql_value)
                        if serialized.len() > 1 {
                            match types::parse_cql_value(&serialized[1..], *type_id) {
                                Ok(_) => {}
                                Err(_) => {
                                    return Err(crate::Error::corruption(
                                        "Type parse failed during benchmark",
                                    ))
                                }
                            }
                        }
                    }
                }
            }

            let duration = start.elapsed();
            if duration < best_duration {
                best_duration = duration;
            }
        }

        let throughput_mbs = (total_bytes as f64) / best_duration.as_secs_f64() / 1_000_000.0;
        let meets_targets = throughput_mbs >= self.min_throughput_mbs;

        let result = BenchmarkResult {
            name: "type_system".to_string(),
            bytes_processed: total_bytes,
            duration: best_duration,
            throughput_mbs,
            memory_usage: None,
            meets_targets,
        };

        self.results.push(result.clone());
        println!("✅ Type system: {:.2} MB/s", result.throughput_mbs);

        Ok(())
    }

    /// Run streaming parser benchmark (simulated large file)
    pub fn benchmark_streaming(&mut self) -> Result<()> {
        println!("🚀 Running streaming parser benchmarks...");

        // Simulate streaming by parsing chunks
        let chunk_size = 64 * 1024; // 64KB chunks
        let num_chunks = self.target_file_size / chunk_size;

        let mut total_bytes = 0;
        let mut best_duration = Duration::MAX;

        for _ in 0..self.iterations {
            let start = Instant::now();

            for _ in 0..num_chunks {
                // Simulate parsing a chunk of VInt data
                let chunk_data = self.generate_chunk_data(chunk_size);
                total_bytes += chunk_data.len();

                // Parse the chunk
                self.parse_chunk(&chunk_data)?;
            }

            let duration = start.elapsed();
            if duration < best_duration {
                best_duration = duration;
            }
        }

        let throughput_mbs = (total_bytes as f64) / best_duration.as_secs_f64() / 1_000_000.0;
        let meets_targets = throughput_mbs >= self.min_throughput_mbs;

        let result = BenchmarkResult {
            name: "streaming_parser".to_string(),
            bytes_processed: total_bytes,
            duration: best_duration,
            throughput_mbs,
            memory_usage: None,
            meets_targets,
        };

        self.results.push(result.clone());
        println!("✅ Streaming parser: {:.2} MB/s", result.throughput_mbs);

        Ok(())
    }

    /// Generate comprehensive performance report
    pub fn generate_report(&self) -> String {
        let mut report = String::new();
        report.push_str("# Parser Performance Benchmark Report\n\n");

        let total_benchmarks = self.results.len();
        let passed_benchmarks = self.results.iter().filter(|r| r.meets_targets).count();
        let failed_benchmarks = total_benchmarks - passed_benchmarks;

        report.push_str(&format!("## Summary\n"));
        report.push_str(&format!(
            "- Target Throughput: {:.1} MB/s\n",
            self.min_throughput_mbs
        ));
        report.push_str(&format!(
            "- Target File Size: {} MB\n",
            self.target_file_size / 1_000_000
        ));
        report.push_str(&format!("- Total Benchmarks: {}\n", total_benchmarks));
        report.push_str(&format!(
            "- Met Targets: {} ({:.1}%)\n",
            passed_benchmarks,
            (passed_benchmarks as f64 / total_benchmarks as f64) * 100.0
        ));
        report.push_str(&format!(
            "- Failed Targets: {} ({:.1}%)\n\n",
            failed_benchmarks,
            (failed_benchmarks as f64 / total_benchmarks as f64) * 100.0
        ));

        report.push_str("## Benchmark Results\n\n");
        for result in &self.results {
            let status = if result.meets_targets {
                "✅ PASS"
            } else {
                "❌ FAIL"
            };
            report.push_str(&format!("### {} - {}\n", result.name, status));
            report.push_str(&format!(
                "- Throughput: {:.2} MB/s\n",
                result.throughput_mbs
            ));
            report.push_str(&format!(
                "- Bytes Processed: {} MB\n",
                result.bytes_processed / 1_000_000
            ));
            report.push_str(&format!(
                "- Duration: {:.2} ms\n",
                result.duration.as_millis()
            ));

            if let Some(memory) = result.memory_usage {
                report.push_str(&format!("- Memory Usage: {} MB\n", memory / 1_000_000));
            }

            report.push_str("\n");
        }

        // Performance analysis
        report.push_str("## Performance Analysis\n\n");

        if let Some(slowest) = self
            .results
            .iter()
            .min_by(|a, b| a.throughput_mbs.partial_cmp(&b.throughput_mbs).unwrap())
        {
            report.push_str(&format!(
                "- Slowest component: {} ({:.2} MB/s)\n",
                slowest.name, slowest.throughput_mbs
            ));
        }

        if let Some(fastest) = self
            .results
            .iter()
            .max_by(|a, b| a.throughput_mbs.partial_cmp(&b.throughput_mbs).unwrap())
        {
            report.push_str(&format!(
                "- Fastest component: {} ({:.2} MB/s)\n",
                fastest.name, fastest.throughput_mbs
            ));
        }

        let avg_throughput: f64 =
            self.results.iter().map(|r| r.throughput_mbs).sum::<f64>() / self.results.len() as f64;
        report.push_str(&format!(
            "- Average throughput: {:.2} MB/s\n",
            avg_throughput
        ));

        if avg_throughput >= self.min_throughput_mbs {
            report.push_str("- Overall performance: **MEETS TARGETS** ✅\n");
        } else {
            report.push_str("- Overall performance: **BELOW TARGETS** ❌\n");
        }

        report
    }

    // Helper methods

    fn generate_vint_test_data(&self) -> Vec<i64> {
        let count = self.target_file_size / 8; // Assume average 8 bytes per VInt
        let mut data = Vec::with_capacity(count);

        for i in 0..count {
            // Mix of different value ranges to test various VInt sizes
            match i % 4 {
                0 => data.push((i % 128) as i64),   // Single byte values
                1 => data.push((i % 16384) as i64), // Two byte values
                2 => data.push((i as i64) * 1000),  // Larger values
                _ => data.push(-(i as i64)),        // Negative values
            }
        }

        data
    }

    fn create_benchmark_header(&self) -> header::SSTableHeader {
        use std::collections::HashMap;

        header::SSTableHeader {
            version: header::SUPPORTED_VERSION,
            table_id: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            keyspace: "benchmark_keyspace".to_string(),
            table_name: "benchmark_table".to_string(),
            generation: 12345,
            compression: header::CompressionInfo {
                algorithm: "LZ4".to_string(),
                chunk_size: 4096,
                parameters: HashMap::new(),
            },
            stats: header::SSTableStats {
                row_count: 1000000,
                min_timestamp: 1000000,
                max_timestamp: 2000000,
                max_deletion_time: 0,
                compression_ratio: 0.75,
                row_size_histogram: vec![10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
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
                    name: "timestamp".to_string(),
                    column_type: "timestamp".to_string(),
                    is_primary_key: false,
                    key_position: None,
                    is_static: false,
                    is_clustering: true,
                },
                header::ColumnInfo {
                    name: "data".to_string(),
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

    fn create_benchmark_values(&self) -> Vec<(types::CqlTypeId, crate::Value)> {
        vec![
            (types::CqlTypeId::Boolean, crate::Value::Boolean(true)),
            (types::CqlTypeId::Boolean, crate::Value::Boolean(false)),
            (types::CqlTypeId::Int, crate::Value::Integer(42)),
            (types::CqlTypeId::Int, crate::Value::Integer(-42)),
            (types::CqlTypeId::BigInt, crate::Value::BigInt(1000000)),
            (types::CqlTypeId::BigInt, crate::Value::BigInt(-1000000)),
            (types::CqlTypeId::Float, crate::Value::Float(3.14159)),
            (types::CqlTypeId::Float, crate::Value::Float(-2.71828)),
            (
                types::CqlTypeId::Varchar,
                crate::Value::Text("benchmark_string".to_string()),
            ),
            (
                types::CqlTypeId::Varchar,
                crate::Value::Text("".to_string()),
            ),
            (
                types::CqlTypeId::Blob,
                crate::Value::Blob(vec![1, 2, 3, 4, 5, 6, 7, 8]),
            ),
            (types::CqlTypeId::Blob, crate::Value::Blob(vec![])),
            (
                types::CqlTypeId::Uuid,
                crate::Value::Uuid([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
            ),
            (
                types::CqlTypeId::Timestamp,
                crate::Value::Timestamp(1640995200000000),
            ), // 2022-01-01
        ]
    }

    fn generate_chunk_data(&self, size: usize) -> Vec<u8> {
        let mut data = Vec::with_capacity(size);
        let mut current_pos = 0;

        // Fill chunk with VInt-encoded values
        while current_pos < size {
            let value = (current_pos as i64) % 10000;
            let encoded = vint::encode_vint(value);

            if current_pos + encoded.len() <= size {
                data.extend_from_slice(&encoded);
                current_pos += encoded.len();
            } else {
                break;
            }
        }

        data
    }

    fn parse_chunk(&self, data: &[u8]) -> Result<()> {
        let mut remaining = data;

        while !remaining.is_empty() {
            match vint::parse_vint(remaining) {
                Ok((new_remaining, _value)) => {
                    remaining = new_remaining;
                }
                Err(_) => break, // End of valid data
            }
        }

        Ok(())
    }
}

impl Default for ParserBenchmarks {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_benchmark_framework() {
        let mut benchmarks = ParserBenchmarks::new()
            .with_min_throughput(10.0) // Lower target for tests
            .with_target_file_size(1024); // Small size for tests

        assert!(benchmarks.benchmark_vint().is_ok());
        assert!(benchmarks.benchmark_header().is_ok());
        assert!(benchmarks.benchmark_types().is_ok());

        let report = benchmarks.generate_report();
        assert!(!report.is_empty());
        assert!(report.contains("Parser Performance Benchmark Report"));
    }
}
