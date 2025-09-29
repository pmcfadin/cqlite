//! Property-based tests for compression algorithms and data integrity
//!
//! This module specifically tests compression edge cases, algorithm-specific
//! behaviors, and ensures data integrity across all supported compression types.

use crate::storage::sstable::compression::{CompressionCodec, CompressionType};
use proptest::prelude::*;
use std::collections::HashMap;

// ============================================================================
// Compression-Specific Data Generators
// ============================================================================

/// Generates data patterns that test compression algorithm edge cases
fn arb_compression_test_data() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        // Empty data
        Just(vec![]),
        // Single byte
        any::<u8>().prop_map(|b| vec![b]),
        // Highly compressible - repeated byte patterns
        (any::<u8>(), 1..100000usize).prop_map(|(byte, len)| vec![byte; len]),
        // Highly compressible - simple patterns
        (1..1000usize).prop_map(|len| { (0..len).map(|i| (i % 256) as u8).collect() }),
        // Incompressible - random data
        prop::collection::vec(any::<u8>(), 1..100000),
        // Sparse data - mostly zeros with some random bytes
        prop::collection::vec(any::<u8>(), 100..10000).prop_map(|mut data| {
            for i in (0..data.len()).step_by(10) {
                if i < data.len() {
                    data[i] = 0;
                }
            }
            data
        }),
        // Structured data - simulating SSTable blocks
        arb_structured_sstable_data(),
        // Binary data with specific patterns
        arb_binary_patterns(),
        // Text-like data
        arb_text_like_data(),
        // Extreme cases
        arb_extreme_compression_cases(),
    ]
}

/// Generates structured data similar to SSTable blocks
fn arb_structured_sstable_data() -> impl Strategy<Value = Vec<u8>> {
    (
        prop::collection::vec(any::<u32>(), 10..1000), // Row offsets
        prop::collection::vec(any::<u16>(), 10..1000), // Column counts
        prop::collection::vec(any::<u8>(), 100..10000), // Data payload
    )
        .prop_map(|(offsets, counts, payload)| {
            let mut result = Vec::new();

            // Serialize offsets (little-endian)
            for offset in offsets {
                result.extend_from_slice(&offset.to_le_bytes());
            }

            // Serialize counts
            for count in counts {
                result.extend_from_slice(&count.to_le_bytes());
            }

            // Add payload
            result.extend_from_slice(&payload);

            result
        })
}

/// Generates various binary patterns
fn arb_binary_patterns() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        // Alternating patterns
        (any::<u8>(), any::<u8>(), 100..10000usize)
            .prop_map(|(a, b, len)| { (0..len).map(|i| if i % 2 == 0 { a } else { b }).collect() }),
        // Graduated patterns
        (100..10000usize).prop_map(|len| { (0..len).map(|i| ((i * 255) / len) as u8).collect() }),
        // Block patterns - large chunks of same value
        prop::collection::vec((any::<u8>(), 10..1000usize), 1..100).prop_map(|blocks| {
            let mut result = Vec::new();
            for (byte, count) in blocks {
                result.extend(vec![byte; count]);
            }
            result
        }),
        // Random walks
        (any::<u8>(), 1000..50000usize).prop_map(|(start, len)| {
            let mut result = vec![start];
            let mut current = start;
            for _ in 1..len {
                let delta = (fastrand::u32(0..3) as i32 - 1) as i8;
                current = current.wrapping_add(delta as u8);
                result.push(current);
            }
            result
        }),
    ]
}

/// Generates text-like data for UTF-8 compression testing
fn arb_text_like_data() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        // ASCII text
        prop::string::string_regex("[a-zA-Z0-9 .,!?\\n]{100,10000}")
            .unwrap()
            .prop_map(|s| s.into_bytes()),
        // Repeated words
        (
            prop::collection::vec("[a-zA-Z]{3,15}", 10..100),
            100..1000usize
        )
            .prop_map(|(words, repeat_count)| {
                let text = words.join(" ").repeat(repeat_count / words.len() + 1);
                text.into_bytes()
            }),
        // JSON-like structure
        prop::collection::vec(
            (
                prop::string::string_regex("[a-zA-Z]{5,20}").unwrap(),
                any::<i32>()
            ),
            10..100
        )
        .prop_map(|pairs| {
            let json = pairs
                .iter()
                .map(|(k, v)| format!("\"{}\":{}", k, v))
                .collect::<Vec<_>>()
                .join(",");
            format!("{{{}}}", json).into_bytes()
        }),
    ]
}

/// Generates extreme cases that might break compression algorithms
fn arb_extreme_compression_cases() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        // Maximum single-byte repetition
        any::<u8>().prop_map(|b| vec![b; 1024 * 1024]),
        // Pathological LZ77 cases - very long matches
        (1..10000usize).prop_map(|pattern_len| {
            let pattern: Vec<u8> = (0..pattern_len).map(|i| (i % 256) as u8).collect();
            pattern.repeat(100)
        }),
        // Anti-compression patterns
        (1..100000usize).prop_map(|len| {
            (0..len)
                .map(|i| {
                    // Create a pattern that resists compression
                    let mut hasher = std::collections::hash_map::DefaultHasher::new();
                    std::hash::Hasher::write_usize(&mut hasher, i);
                    std::hash::Hasher::finish(&hasher) as u8
                })
                .collect()
        }),
        // Huffman pathological cases - very skewed frequency distribution
        (any::<u8>(), 1..50000usize).prop_map(|(rare_byte, len)| {
            let mut result = vec![0u8; len];
            // Insert rare byte at specific positions to create skewed distribution
            for i in (0..len).step_by(1000) {
                if i < len {
                    result[i] = rare_byte;
                }
            }
            result
        }),
        // Dictionary exhaustion cases
        prop::collection::vec(any::<[u8; 32]>(), 1000..10000)
            .prop_map(|chunks| chunks.into_iter().flatten().collect()),
    ]
}

// ============================================================================
// Property Tests for Compression
// ============================================================================

proptest! {
    /// Test that all compression algorithms preserve data integrity
    #[test]
    fn prop_compression_data_integrity(
        data in arb_compression_test_data(),
        compression_type in prop_oneof![
            Just(CompressionType::None),
            Just(CompressionType::Lz4),
            Just(CompressionType::Snappy),
            Just(CompressionType::Deflate),
            Just(CompressionType::Zstd),
        ]
    ) {
        let codec = match CompressionCodec::new(compression_type) {
            Ok(codec) => codec,
            Err(_) => return Ok(()), // Skip if compression type not available
        };

        // Compress the data
        let compressed_result = codec.compress(&data);
        prop_assert!(compressed_result.is_ok(),
            "Compression should succeed for {:?}", compression_type);

        let compressed = compressed_result.unwrap();

        // Decompress back
        let decompressed_result = codec.decompress(&compressed, data.len());
        prop_assert!(decompressed_result.is_ok(),
            "Decompression should succeed for {:?}", compression_type);

        let decompressed = decompressed_result.unwrap();

        // Data must be identical
        prop_assert_eq!(data, decompressed,
            "Data corruption in {:?} compression", compression_type);

        // Additional integrity checks
        prop_assert_eq!(data.len(), decompressed.len(),
            "Length mismatch after compression roundtrip");

        if !data.is_empty() {
            prop_assert_eq!(data[0], decompressed[0],
                "First byte corrupted");
            prop_assert_eq!(data[data.len()-1], decompressed[decompressed.len()-1],
                "Last byte corrupted");
        }
    }

    /// Test compression ratio properties for different data patterns
    #[test]
    fn prop_compression_ratio_properties(
        data in arb_compression_test_data(),
        compression_type in prop_oneof![
            Just(CompressionType::Lz4),
            Just(CompressionType::Snappy),
            Just(CompressionType::Deflate),
            Just(CompressionType::Zstd),
        ]
    ) {
        if data.is_empty() {
            return Ok(());
        }

        let codec = match CompressionCodec::new(compression_type) {
            Ok(codec) => codec,
            Err(_) => return Ok(()),
        };

        let compressed = codec.compress(&data)?;
        let compression_ratio = compressed.len() as f64 / data.len() as f64;

        // Compression ratio bounds based on data characteristics
        let expected_bounds = analyze_data_compressibility(&data);

        match compression_type {
            CompressionType::Lz4 => {
                // LZ4 prioritizes speed over compression ratio
                prop_assert!(compression_ratio <= 1.1 || data.len() < 100,
                    "LZ4 ratio {:.3} too high for compressible data", compression_ratio);

                if expected_bounds.highly_compressible {
                    prop_assert!(compression_ratio <= 0.1,
                        "LZ4 should achieve good compression on highly compressible data");
                }
            },
            CompressionType::Snappy => {
                // Snappy also prioritizes speed
                prop_assert!(compression_ratio <= 1.1 || data.len() < 100,
                    "Snappy ratio {:.3} too high", compression_ratio);
            },
            CompressionType::Deflate => {
                // Deflate should achieve better compression ratios
                if expected_bounds.highly_compressible {
                    prop_assert!(compression_ratio <= 0.05,
                        "Deflate should achieve excellent compression on highly compressible data");
                }
            },
            CompressionType::Zstd => {
                // Zstd should balance speed and compression
                if expected_bounds.highly_compressible {
                    prop_assert!(compression_ratio <= 0.1,
                        "Zstd should achieve good compression on highly compressible data");
                }
            },
            _ => {}
        }

        // Universal bounds
        prop_assert!(compression_ratio > 0.0, "Compression ratio must be positive");

        // For truly random data, compression might make it larger (worst case ~1.125x for LZ77)
        if expected_bounds.incompressible {
            prop_assert!(compression_ratio <= 1.15,
                "Compression ratio {:.3} too high even for incompressible data", compression_ratio);
        }
    }

    /// Test compression algorithm-specific edge cases
    #[test]
    fn prop_algorithm_specific_edge_cases(
        data in arb_extreme_compression_cases(),
        compression_type in prop_oneof![
            Just(CompressionType::Lz4),
            Just(CompressionType::Snappy),
            Just(CompressionType::Deflate),
            Just(CompressionType::Zstd),
        ]
    ) {
        let codec = match CompressionCodec::new(compression_type) {
            Ok(codec) => codec,
            Err(_) => return Ok(()),
        };

        // Should handle extreme cases without panic or corruption
        let compress_result = std::panic::catch_unwind(|| {
            codec.compress(&data)
        });

        prop_assert!(compress_result.is_ok(),
            "Compression should not panic on extreme data");

        if let Ok(Ok(compressed)) = compress_result {
            let decompress_result = std::panic::catch_unwind(|| {
                codec.decompress(&compressed, data.len())
            });

            prop_assert!(decompress_result.is_ok(),
                "Decompression should not panic");

            if let Ok(Ok(decompressed)) = decompress_result {
                prop_assert_eq!(data, decompressed,
                    "Data integrity must be preserved even for extreme cases");
            }
        }

        // Algorithm-specific edge case validations
        match compression_type {
            CompressionType::Lz4 => {
                // LZ4 should handle very large repetitions
                if data.len() > 1000 && data.iter().all(|&b| b == data[0]) {
                    let compressed = codec.compress(&data)?;
                    let ratio = compressed.len() as f64 / data.len() as f64;
                    prop_assert!(ratio < 0.01,
                        "LZ4 should achieve excellent compression on repetitive data");
                }
            },
            CompressionType::Snappy => {
                // Snappy should handle long matches efficiently
                if data.len() > 10000 {
                    let compressed = codec.compress(&data)?;
                    prop_assert!(compressed.len() <= data.len(),
                        "Snappy should not expand data significantly");
                }
            },
            CompressionType::Deflate => {
                // Deflate should handle dictionary cases well
                let compressed = codec.compress(&data)?;
                prop_assert!(compressed.len() <= data.len() + data.len() / 8,
                    "Deflate overhead should be bounded");
            },
            CompressionType::Zstd => {
                // Zstd should be robust across all patterns
                let compressed = codec.compress(&data)?;
                prop_assert!(compressed.len() <= data.len() * 2,
                    "Zstd should not excessively expand any data");
            },
            _ => {}
        }
    }

    /// Test compression performance characteristics
    #[test]
    fn prop_compression_performance_characteristics(
        data in prop::collection::vec(any::<u8>(), 1000..100000),
        compression_type in prop_oneof![
            Just(CompressionType::Lz4),
            Just(CompressionType::Snappy),
            Just(CompressionType::Deflate),
            Just(CompressionType::Zstd),
        ]
    ) {
        let codec = match CompressionCodec::new(compression_type) {
            Ok(codec) => codec,
            Err(_) => return Ok(()),
        };

        use std::time::Instant;

        // Measure compression time
        let compress_start = Instant::now();
        let compressed = codec.compress(&data)?;
        let compress_time = compress_start.elapsed();

        // Measure decompression time
        let decompress_start = Instant::now();
        let _decompressed = codec.decompress(&compressed, data.len())?;
        let decompress_time = decompress_start.elapsed();

        let data_mb = data.len() as f64 / (1024.0 * 1024.0);

        // Performance expectations based on algorithm characteristics
        let (max_compress_mb_per_sec, max_decompress_mb_per_sec) = match compression_type {
            CompressionType::Lz4 => (200.0, 800.0),    // Very fast
            CompressionType::Snappy => (150.0, 600.0), // Fast
            CompressionType::Deflate => (20.0, 300.0), // Slower, better compression
            CompressionType::Zstd => (50.0, 400.0),    // Balanced
            _ => (10.0, 100.0), // Conservative defaults
        };

        let compress_mb_per_sec = data_mb / compress_time.as_secs_f64();
        let decompress_mb_per_sec = data_mb / decompress_time.as_secs_f64();

        // Allow for some variance due to system load, but check rough performance
        prop_assert!(compress_mb_per_sec >= max_compress_mb_per_sec / 10.0,
            "{:?} compression too slow: {:.1} MB/s (expected >= {:.1})",
            compression_type, compress_mb_per_sec, max_compress_mb_per_sec / 10.0);

        prop_assert!(decompress_mb_per_sec >= max_decompress_mb_per_sec / 10.0,
            "{:?} decompression too slow: {:.1} MB/s (expected >= {:.1})",
            compression_type, decompress_mb_per_sec, max_decompress_mb_per_sec / 10.0);

        // Decompression should generally be faster than compression
        if compress_time.as_millis() > 10 { // Only check for non-trivial times
            prop_assert!(decompress_time <= compress_time * 2,
                "Decompression significantly slower than compression");
        }
    }

    /// Test concurrent compression operations
    #[test]
    fn prop_concurrent_compression_safety(
        data_sets in prop::collection::vec(
            prop::collection::vec(any::<u8>(), 1000..10000),
            2..10
        ),
        compression_type in prop_oneof![
            Just(CompressionType::Lz4),
            Just(CompressionType::Snappy),
        ] // Use faster algorithms for concurrency testing
    ) {
        use std::sync::Arc;
        use std::thread;

        let codec = match CompressionCodec::new(compression_type) {
            Ok(codec) => Arc::new(codec),
            Err(_) => return Ok(()),
        };

        let shared_data = Arc::new(data_sets);

        // Spawn multiple threads doing compression
        let handles: Vec<_> = (0..shared_data.len()).map(|i| {
            let codec = Arc::clone(&codec);
            let data = shared_data[i].clone();

            thread::spawn(move || {
                let compressed = codec.compress(&data)?;
                let decompressed = codec.decompress(&compressed, data.len())?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>((data, decompressed))
            })
        }).collect();

        // Collect results
        for (i, handle) in handles.into_iter().enumerate() {
            let result = handle.join()
                .expect("Thread should not panic")
                .expect("Compression should succeed");

            let (original, decompressed) = result;
            prop_assert_eq!(original, decompressed,
                "Thread {} data corruption in concurrent compression", i);
        }
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

#[derive(Debug)]
struct CompressionBounds {
    highly_compressible: bool,
    incompressible: bool,
    entropy_estimate: f64,
}

/// Analyzes data to predict compression characteristics
fn analyze_data_compressibility(data: &[u8]) -> CompressionBounds {
    if data.is_empty() {
        return CompressionBounds {
            highly_compressible: false,
            incompressible: false,
            entropy_estimate: 0.0,
        };
    }

    // Count byte frequencies
    let mut freq = [0u32; 256];
    for &byte in data {
        freq[byte as usize] += 1;
    }

    // Calculate entropy
    let len = data.len() as f64;
    let entropy = freq
        .iter()
        .filter(|&&count| count > 0)
        .map(|&count| {
            let p = count as f64 / len;
            -p * p.log2()
        })
        .sum::<f64>();

    // Check for highly repetitive patterns
    let unique_bytes = freq.iter().filter(|&&count| count > 0).count();
    let max_freq = freq.iter().max().unwrap();
    let max_freq_ratio = *max_freq as f64 / len;

    let highly_compressible = entropy < 2.0 || unique_bytes <= 16 || max_freq_ratio > 0.8;
    let incompressible = entropy > 7.5 && unique_bytes > 200;

    CompressionBounds {
        highly_compressible,
        incompressible,
        entropy_estimate: entropy,
    }
}

#[cfg(test)]
mod compression_integration_tests {
    use super::*;

    #[test]
    fn test_data_analysis_works() {
        // Highly compressible data
        let repetitive = vec![42u8; 1000];
        let bounds = analyze_data_compressibility(&repetitive);
        assert!(bounds.highly_compressible);
        assert!(!bounds.incompressible);

        // Random data
        let random: Vec<u8> = (0..1000).map(|i| (i * 17 + 42) as u8).collect();
        let bounds = analyze_data_compressibility(&random);
        // This specific pattern might not be truly incompressible, so just check it doesn't crash
        assert!(bounds.entropy_estimate > 0.0);
    }

    #[test]
    fn test_compression_generators() {
        use proptest::test_runner::TestRunner;

        let mut runner = TestRunner::default();
        let strategy = arb_compression_test_data();

        // Generate a few samples to ensure generators work
        for _ in 0..10 {
            let data = strategy.new_tree(&mut runner).unwrap().current();
            // Should generate various data sizes
            assert!(data.len() <= 1024 * 1024); // Reasonable upper bound
        }
    }
}
