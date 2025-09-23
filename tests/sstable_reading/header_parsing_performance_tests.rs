//! Performance benchmark tests for SSTable header parsing
//!
//! This module provides comprehensive performance testing for header parsing
//! operations, including throughput benchmarks, memory usage analysis,
//! and scalability testing.

use cqlite_core::{
    parser::header::{
        CassandraVersion, ColumnInfo, CompressionInfo, SSTABLE_MAGIC, SSTableHeader, SSTableStats,
        SUPPORTED_VERSION, SUPPORTED_MAGIC_NUMBERS, parse_sstable_header, serialize_sstable_header,
        parse_magic_and_version,
    },
    Config,
    platform::Platform,
    storage::sstable::reader::SSTableReader,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

/// Performance benchmarks for core parsing operations
#[cfg(test)]
mod core_parsing_benchmarks {
    use super::*;

    #[test]
    fn benchmark_magic_number_detection() {
        println!("🚀 Magic Number Detection Benchmark");

        let iterations = 1_000_000;
        let test_magics: Vec<u32> = SUPPORTED_MAGIC_NUMBERS
            .iter()
            .cycle()
            .take(iterations)
            .cloned()
            .collect();

        // Warm up
        for _ in 0..1000 {
            let _ = CassandraVersion::from_magic_number(SSTABLE_MAGIC);
        }

        let start = Instant::now();
        let mut valid_count = 0;

        for &magic in &test_magics {
            if CassandraVersion::from_magic_number(magic).is_some() {
                valid_count += 1;
            }
        }

        let duration = start.elapsed();
        let rate = iterations as f64 / duration.as_secs_f64();

        println!("  Iterations: {}", iterations);
        println!("  Valid detections: {}", valid_count);
        println!("  Total time: {:?}", duration);
        println!("  Rate: {:.0} detections/sec", rate);
        println!("  Average time: {:.2} ns/detection", duration.as_nanos() as f64 / iterations as f64);

        // Performance assertion
        assert!(rate > 1_000_000.0, "Magic detection should exceed 1M ops/sec");
        println!("  ✅ Performance target met\n");
    }

    #[test]
    fn benchmark_header_parsing_throughput() {
        println!("🚀 Header Parsing Throughput Benchmark");

        let test_headers = create_benchmark_headers();
        let iterations = 10_000;

        println!("  Test headers: {}", test_headers.len());
        println!("  Iterations per header: {}", iterations);

        for (name, header) in test_headers {
            let serialized = serialize_sstable_header(&header).unwrap();

            // Warm up
            for _ in 0..100 {
                let _ = parse_sstable_header(&serialized);
            }

            let start = Instant::now();
            for _ in 0..iterations {
                let result = parse_sstable_header(&serialized);
                assert!(result.is_ok());
            }
            let duration = start.elapsed();

            let rate = iterations as f64 / duration.as_secs_f64();
            let throughput_mbps = (serialized.len() * iterations) as f64 / (1024.0 * 1024.0) / duration.as_secs_f64();

            println!("  {} ({} bytes):", name, serialized.len());
            println!("    Rate: {:.0} parses/sec", rate);
            println!("    Throughput: {:.2} MB/s", throughput_mbps);
            println!("    Avg time: {:.2} μs", duration.as_micros() as f64 / iterations as f64);

            // Performance assertions
            assert!(rate > 10_000.0, "Should parse at least 10K headers/sec for {}", name);
            assert!(duration.as_micros() / iterations < 500, "Should parse in under 500μs for {}", name);
        }
        println!("  ✅ All throughput targets met\n");
    }

    #[test]
    fn benchmark_header_serialization_throughput() {
        println!("🚀 Header Serialization Throughput Benchmark");

        let test_headers = create_benchmark_headers();
        let iterations = 5_000;

        for (name, header) in test_headers {
            // Warm up
            for _ in 0..100 {
                let _ = serialize_sstable_header(&header);
            }

            let start = Instant::now();
            let mut total_size = 0;

            for _ in 0..iterations {
                let serialized = serialize_sstable_header(&header).unwrap();
                total_size += serialized.len();
            }

            let duration = start.elapsed();
            let rate = iterations as f64 / duration.as_secs_f64();
            let throughput_mbps = total_size as f64 / (1024.0 * 1024.0) / duration.as_secs_f64();

            println!("  {}:", name);
            println!("    Rate: {:.0} serializations/sec", rate);
            println!("    Throughput: {:.2} MB/s", throughput_mbps);
            println!("    Avg time: {:.2} μs", duration.as_micros() as f64 / iterations as f64);

            assert!(rate > 5_000.0, "Should serialize at least 5K headers/sec for {}", name);
        }
        println!("  ✅ All serialization targets met\n");
    }

    #[test]
    fn benchmark_roundtrip_performance() {
        println!("🚀 Round-trip Performance Benchmark");

        let test_headers = create_benchmark_headers();
        let iterations = 1_000;

        for (name, header) in test_headers {
            // Warm up
            for _ in 0..50 {
                let serialized = serialize_sstable_header(&header).unwrap();
                let _ = parse_sstable_header(&serialized);
            }

            let start = Instant::now();

            for _ in 0..iterations {
                let serialized = serialize_sstable_header(&header).unwrap();
                let (_, parsed) = parse_sstable_header(&serialized).unwrap();
                // Use parsed to prevent optimization
                assert_eq!(parsed.keyspace, header.keyspace);
            }

            let duration = start.elapsed();
            let rate = iterations as f64 / duration.as_secs_f64();

            println!("  {}:", name);
            println!("    Rate: {:.0} round-trips/sec", rate);
            println!("    Avg time: {:.2} μs", duration.as_micros() as f64 / iterations as f64);

            assert!(rate > 1_000.0, "Should complete 1K round-trips/sec for {}", name);
        }
        println!("  ✅ All round-trip targets met\n");
    }
}

/// Memory usage and efficiency benchmarks
#[cfg(test)]
mod memory_benchmarks {
    use super::*;

    #[test]
    fn benchmark_memory_usage_during_parsing() {
        println!("🚀 Memory Usage Benchmark");

        let headers = create_varied_size_headers();

        for (name, header) in headers {
            let serialized = serialize_sstable_header(&header).unwrap();

            // Measure memory usage during parsing
            let initial_memory = get_memory_usage();

            let iterations = 1000;
            let mut parsed_headers = Vec::new();

            for _ in 0..iterations {
                let (_, parsed) = parse_sstable_header(&serialized).unwrap();
                parsed_headers.push(parsed);
            }

            let peak_memory = get_memory_usage();
            let memory_growth = peak_memory.saturating_sub(initial_memory);

            // Clean up
            drop(parsed_headers);

            let memory_per_header = memory_growth / iterations;
            let efficiency_ratio = serialized.len() as f64 / memory_per_header as f64;

            println!("  {} (serialized: {} bytes):", name, serialized.len());
            println!("    Memory per parse: {} bytes", memory_per_header);
            println!("    Efficiency ratio: {:.2}x", efficiency_ratio);
            println!("    Total memory growth: {} bytes", memory_growth);

            // Memory efficiency assertions
            assert!(memory_per_header < serialized.len() * 5,
                   "Memory usage should not exceed 5x serialized size for {}", name);

            println!("    ✅ Memory efficiency acceptable");
        }
        println!("  ✅ All memory targets met\n");
    }

    #[test]
    fn benchmark_memory_fragmentation() {
        println!("🚀 Memory Fragmentation Benchmark");

        let header = create_complex_header();
        let serialized = serialize_sstable_header(&header).unwrap();

        // Test repeated allocation/deallocation patterns
        let cycles = 100;
        let allocations_per_cycle = 1000;

        let initial_memory = get_memory_usage();

        for cycle in 0..cycles {
            let mut temp_headers = Vec::new();

            // Allocate many headers
            for _ in 0..allocations_per_cycle {
                let (_, parsed) = parse_sstable_header(&serialized).unwrap();
                temp_headers.push(parsed);
            }

            // Deallocate half randomly
            for i in (0..temp_headers.len()).step_by(2) {
                if i < temp_headers.len() {
                    temp_headers.remove(i);
                }
            }

            if cycle % 10 == 0 {
                let current_memory = get_memory_usage();
                println!("  Cycle {}: Memory usage {} bytes", cycle, current_memory);
            }
        }

        let final_memory = get_memory_usage();
        let memory_growth = final_memory.saturating_sub(initial_memory);

        println!("  Final memory growth: {} bytes", memory_growth);
        println!("  Memory growth per cycle: {} bytes", memory_growth / cycles);

        // Fragmentation should be reasonable
        assert!(memory_growth < serialized.len() * 1000,
               "Memory fragmentation should be manageable");

        println!("  ✅ Memory fragmentation acceptable\n");
    }

    fn get_memory_usage() -> usize {
        // Simplified memory usage estimation
        // In practice, you might use more sophisticated memory profiling
        let process = std::process::Command::new("ps")
            .args(&["-o", "rss=", "-p", &std::process::id().to_string()])
            .output();

        if let Ok(output) = process {
            if let Ok(rss_str) = String::from_utf8(output.stdout) {
                if let Ok(rss_kb) = rss_str.trim().parse::<usize>() {
                    return rss_kb * 1024; // Convert to bytes
                }
            }
        }

        // Fallback: return 0 if we can't measure
        0
    }
}

/// Scalability and stress testing
#[cfg(test)]
mod scalability_benchmarks {
    use super::*;

    #[test]
    fn benchmark_parsing_scalability() {
        println!("🚀 Parsing Scalability Benchmark");

        let header_sizes = vec![
            ("tiny", 1),
            ("small", 10),
            ("medium", 100),
            ("large", 1000),
            ("huge", 10000),
        ];

        for (size_name, column_count) in header_sizes {
            let header = create_header_with_columns(column_count);
            let serialized = serialize_sstable_header(&header).unwrap();

            let iterations = std::cmp::max(1, 10000 / column_count);

            // Warm up
            for _ in 0..10 {
                let _ = parse_sstable_header(&serialized);
            }

            let start = Instant::now();
            for _ in 0..iterations {
                let result = parse_sstable_header(&serialized);
                assert!(result.is_ok());
            }
            let duration = start.elapsed();

            let rate = iterations as f64 / duration.as_secs_f64();
            let time_per_column = duration.as_nanos() as f64 / (iterations * column_count) as f64;

            println!("  {} ({} columns, {} bytes):", size_name, column_count, serialized.len());
            println!("    Rate: {:.0} parses/sec", rate);
            println!("    Time per column: {:.2} ns", time_per_column);
            println!("    Avg parse time: {:.2} μs", duration.as_micros() as f64 / iterations as f64);

            // Scalability should be roughly linear
            assert!(time_per_column < 10000.0, "Time per column should be under 10μs");
        }
        println!("  ✅ Scalability targets met\n");
    }

    #[test]
    fn benchmark_concurrent_parsing() {
        println!("🚀 Concurrent Parsing Benchmark");

        let header = create_complex_header();
        let serialized = serialize_sstable_header(&header).unwrap();
        let serialized = Arc::new(serialized);

        let thread_counts = vec![1, 2, 4, 8, 16];
        let iterations_per_thread = 1000;

        for thread_count in thread_counts {
            let start = Instant::now();
            let mut handles = Vec::new();

            for _ in 0..thread_count {
                let serialized_clone = serialized.clone();
                let handle = std::thread::spawn(move || {
                    for _ in 0..iterations_per_thread {
                        let result = parse_sstable_header(&serialized_clone);
                        assert!(result.is_ok());
                    }
                });
                handles.push(handle);
            }

            for handle in handles {
                handle.join().unwrap();
            }

            let duration = start.elapsed();
            let total_operations = thread_count * iterations_per_thread;
            let rate = total_operations as f64 / duration.as_secs_f64();

            println!("  {} threads:", thread_count);
            println!("    Total operations: {}", total_operations);
            println!("    Rate: {:.0} parses/sec", rate);
            println!("    Time: {:?}", duration);

            if thread_count > 1 {
                // Should show some speedup with more threads
                println!("    Parallel efficiency: estimated");
            }
        }
        println!("  ✅ Concurrent parsing successful\n");
    }

    #[tokio::test]
    async fn benchmark_async_file_parsing() {
        println!("🚀 Async File Parsing Benchmark");

        let temp_dir = TempDir::new().unwrap();
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());

        // Create test files with different header complexities
        let file_configs = vec![
            ("simple", create_simple_header()),
            ("complex", create_complex_header()),
            ("large", create_large_header()),
        ];

        let file_count = 10;
        let mut file_paths = Vec::new();

        // Create multiple files of each type
        for (config_name, header) in &file_configs {
            for i in 0..file_count {
                let filename = format!("{}-{}.sst", config_name, i);
                let file_path = temp_dir.path().join(&filename);

                let mut file_data = serialize_sstable_header(header).unwrap();
                file_data.extend_from_slice(&[0x00; 1000]); // Body content

                tokio::fs::write(&file_path, &file_data).await.unwrap();
                file_paths.push((config_name.to_string(), file_path));
            }
        }

        println!("  Created {} test files", file_paths.len());

        // Benchmark sequential file opening
        let start = Instant::now();
        let mut successful_opens = 0;

        for (config_name, file_path) in &file_paths {
            let result = SSTableReader::open(file_path, &config, platform.clone()).await;
            if result.is_ok() {
                successful_opens += 1;
            } else {
                println!("    Failed to open {}: {:?}", config_name, result.err());
            }
        }

        let sequential_duration = start.elapsed();
        let sequential_rate = successful_opens as f64 / sequential_duration.as_secs_f64();

        println!("  Sequential opening:");
        println!("    Successful opens: {}/{}", successful_opens, file_paths.len());
        println!("    Rate: {:.1} files/sec", sequential_rate);
        println!("    Total time: {:?}", sequential_duration);

        // Benchmark concurrent file opening
        let start = Instant::now();
        let mut tasks = Vec::new();

        for (config_name, file_path) in file_paths {
            let config_clone = config.clone();
            let platform_clone = platform.clone();
            let task_name = config_name.clone();

            let task = tokio::spawn(async move {
                let result = SSTableReader::open(&file_path, &config_clone, platform_clone).await;
                (task_name, result.is_ok())
            });
            tasks.push(task);
        }

        let mut concurrent_successful = 0;
        for task in tasks {
            let (config_name, success) = task.await.unwrap();
            if success {
                concurrent_successful += 1;
            }
        }

        let concurrent_duration = start.elapsed();
        let concurrent_rate = concurrent_successful as f64 / concurrent_duration.as_secs_f64();

        println!("  Concurrent opening:");
        println!("    Successful opens: {}", concurrent_successful);
        println!("    Rate: {:.1} files/sec", concurrent_rate);
        println!("    Total time: {:?}", concurrent_duration);
        println!("    Speedup: {:.2}x", concurrent_rate / sequential_rate);

        assert!(concurrent_successful > 0, "Should successfully open some files");
        println!("  ✅ Async file parsing completed\n");
    }
}

/// Stress testing and edge case performance
#[cfg(test)]
mod stress_tests {
    use super::*;

    #[test]
    fn stress_test_repeated_parsing() {
        println!("🚀 Repeated Parsing Stress Test");

        let header = create_complex_header();
        let serialized = serialize_sstable_header(&header).unwrap();

        let total_iterations = 1_000_000;
        let check_interval = 100_000;

        println!("  Target iterations: {}", total_iterations);
        println!("  Check interval: {}", check_interval);

        let start = Instant::now();
        let mut last_check = start;

        for i in 1..=total_iterations {
            let result = parse_sstable_header(&serialized);
            assert!(result.is_ok(), "Parse failed at iteration {}", i);

            if i % check_interval == 0 {
                let now = Instant::now();
                let interval_duration = now.duration_since(last_check);
                let interval_rate = check_interval as f64 / interval_duration.as_secs_f64();

                println!("    {} iterations: {:.0} parses/sec", i, interval_rate);
                last_check = now;
            }
        }

        let total_duration = start.elapsed();
        let overall_rate = total_iterations as f64 / total_duration.as_secs_f64();

        println!("  Total time: {:?}", total_duration);
        println!("  Overall rate: {:.0} parses/sec", overall_rate);
        println!("  ✅ Stress test completed successfully\n");
    }

    #[test]
    fn stress_test_memory_pressure() {
        println!("🚀 Memory Pressure Stress Test");

        let header = create_large_header();
        let serialized = serialize_sstable_header(&header).unwrap();

        let batch_size = 10_000;
        let batch_count = 100;

        println!("  Batch size: {}", batch_size);
        println!("  Batch count: {}", batch_count);

        for batch in 0..batch_count {
            let start = Instant::now();
            let mut parsed_headers = Vec::with_capacity(batch_size);

            // Parse many headers in memory
            for _ in 0..batch_size {
                let (_, parsed) = parse_sstable_header(&serialized).unwrap();
                parsed_headers.push(parsed);
            }

            let parse_duration = start.elapsed();

            // Verify all headers are valid
            for (i, header) in parsed_headers.iter().enumerate() {
                assert!(!header.keyspace.is_empty(), "Invalid header at index {}", i);
            }

            let verify_duration = start.elapsed() - parse_duration;

            // Clean up batch
            drop(parsed_headers);

            if batch % 10 == 0 {
                println!("    Batch {}: parse={:?}, verify={:?}",
                        batch, parse_duration, verify_duration);
            }
        }

        println!("  ✅ Memory pressure test completed\n");
    }

    #[test]
    fn stress_test_error_handling_performance() {
        println!("🚀 Error Handling Performance Stress Test");

        let corrupted_data_sets = create_corrupted_test_data();
        let iterations_per_dataset = 10_000;

        for (corruption_type, corrupted_data) in corrupted_data_sets {
            let start = Instant::now();
            let mut error_count = 0;

            for _ in 0..iterations_per_dataset {
                let result = parse_sstable_header(&corrupted_data);
                if result.is_err() {
                    error_count += 1;
                }
            }

            let duration = start.elapsed();
            let rate = iterations_per_dataset as f64 / duration.as_secs_f64();

            println!("  {} corruption:", corruption_type);
            println!("    Error rate: {}/{}", error_count, iterations_per_dataset);
            println!("    Processing rate: {:.0} attempts/sec", rate);
            println!("    Avg time: {:.2} μs", duration.as_micros() as f64 / iterations_per_dataset as f64);

            // Error handling should still be fast
            assert!(rate > 50_000.0, "Error handling should be fast for {}", corruption_type);
        }

        println!("  ✅ Error handling performance acceptable\n");
    }
}

// Helper functions for creating test data

fn create_benchmark_headers() -> Vec<(String, SSTableHeader)> {
    vec![
        ("minimal", create_minimal_header()),
        ("typical", create_typical_header()),
        ("complex", create_complex_header()),
        ("large", create_large_header()),
    ]
}

fn create_minimal_header() -> SSTableHeader {
    SSTableHeader {
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
        stats: SSTableStats::default(),
        columns: vec![],
        properties: HashMap::new(),
    }
}

fn create_simple_header() -> SSTableHeader {
    SSTableHeader {
        cassandra_version: CassandraVersion::Legacy,
        version: SUPPORTED_VERSION,
        table_id: [1; 16],
        keyspace: "simple_ks".to_string(),
        table_name: "simple_table".to_string(),
        generation: 1,
        compression: CompressionInfo {
            algorithm: "LZ4".to_string(),
            chunk_size: 4096,
            parameters: HashMap::new(),
        },
        stats: SSTableStats::default(),
        columns: vec![
            ColumnInfo {
                name: "id".to_string(),
                column_type: "uuid".to_string(),
                is_primary_key: true,
                key_position: Some(0),
                is_static: false,
                is_clustering: false,
            }
        ],
        properties: HashMap::new(),
    }
}

fn create_typical_header() -> SSTableHeader {
    let mut header = create_simple_header();
    header.keyspace = "typical_keyspace".to_string();
    header.table_name = "typical_table".to_string();

    // Add typical columns
    header.columns = vec![
        ColumnInfo {
            name: "id".to_string(),
            column_type: "uuid".to_string(),
            is_primary_key: true,
            key_position: Some(0),
            is_static: false,
            is_clustering: false,
        },
        ColumnInfo {
            name: "created_at".to_string(),
            column_type: "timestamp".to_string(),
            is_primary_key: false,
            key_position: None,
            is_static: false,
            is_clustering: true,
        },
        ColumnInfo {
            name: "data".to_string(),
            column_type: "text".to_string(),
            is_primary_key: false,
            key_position: None,
            is_static: false,
            is_clustering: false,
        },
    ];

    // Add typical properties
    header.properties.insert("compaction_class".to_string(), "LeveledCompactionStrategy".to_string());
    header.properties.insert("sstable_size_in_mb".to_string(), "160".to_string());

    header
}

fn create_complex_header() -> SSTableHeader {
    let mut header = create_typical_header();
    header.cassandra_version = CassandraVersion::V5_0Release;

    // Complex compression with parameters
    header.compression.parameters.insert("level".to_string(), "9".to_string());
    header.compression.parameters.insert("window_size".to_string(), "32768".to_string());
    header.compression.parameters.insert("strategy".to_string(), "fast".to_string());

    // Complex statistics
    header.stats = SSTableStats {
        row_count: 1_000_000,
        min_timestamp: 1_600_000_000_000,
        max_timestamp: 1_700_000_000_000,
        max_deletion_time: 1_650_000_000_000,
        compression_ratio: 0.65,
        row_size_histogram: (0..100).map(|i| i * i).collect(),
    };

    // Add more columns
    for i in 0..20 {
        header.columns.push(ColumnInfo {
            name: format!("column_{}", i),
            column_type: format!("type_{}", i % 5),
            is_primary_key: i < 3,
            key_position: if i < 3 { Some(i as u16) } else { None },
            is_static: i % 7 == 0,
            is_clustering: i % 3 == 0,
        });
    }

    // Add more properties
    for i in 0..50 {
        header.properties.insert(
            format!("custom_property_{}", i),
            format!("custom_value_{}_with_some_longer_content", i),
        );
    }

    header
}

fn create_large_header() -> SSTableHeader {
    let mut header = create_complex_header();

    // Very large column set
    header.columns.clear();
    for i in 0..1000 {
        header.columns.push(ColumnInfo {
            name: format!("large_column_name_with_prefix_{}_and_suffix", i),
            column_type: format!("complex_type_definition_{}", i % 20),
            is_primary_key: i < 10,
            key_position: if i < 10 { Some(i as u16) } else { None },
            is_static: i % 13 == 0,
            is_clustering: i % 17 == 0,
        });
    }

    // Large property set
    header.properties.clear();
    for i in 0..500 {
        header.properties.insert(
            format!("large_property_name_with_very_long_prefix_{}_and_descriptive_suffix", i),
            format!("large_property_value_containing_lots_of_text_and_configuration_data_item_{}", i),
        );
    }

    // Large histogram
    header.stats.row_size_histogram = (0..10000).map(|i| i as u64).collect();

    header
}

fn create_varied_size_headers() -> Vec<(String, SSTableHeader)> {
    vec![
        ("minimal", create_minimal_header()),
        ("simple", create_simple_header()),
        ("typical", create_typical_header()),
        ("complex", create_complex_header()),
        ("large", create_large_header()),
    ]
}

fn create_header_with_columns(column_count: usize) -> SSTableHeader {
    let mut header = create_simple_header();

    header.columns.clear();
    for i in 0..column_count {
        header.columns.push(ColumnInfo {
            name: format!("col_{}", i),
            column_type: format!("type_{}", i % 10),
            is_primary_key: i < 5,
            key_position: if i < 5 { Some(i as u16) } else { None },
            is_static: i % 10 == 0,
            is_clustering: i % 7 == 0,
        });
    }

    header
}

fn create_corrupted_test_data() -> Vec<(String, Vec<u8>)> {
    let valid_header = create_typical_header();
    let valid_data = serialize_sstable_header(&valid_header).unwrap();

    vec![
        ("invalid_magic", {
            let mut data = valid_data.clone();
            data[0] = 0xFF;
            data
        }),
        ("truncated", valid_data[..20].to_vec()),
        ("invalid_version", {
            let mut data = valid_data.clone();
            if data.len() > 5 {
                data[4] = 0xFF;
                data[5] = 0xFF;
            }
            data
        }),
        ("random_corruption", {
            let mut data = valid_data.clone();
            for i in (0..data.len()).step_by(10) {
                if i < data.len() {
                    data[i] ^= 0xFF;
                }
            }
            data
        }),
    ]
}