//! Integration tests for ChunkedDataReader with real compressed SSTables
//!
//! Tests the chunked streaming reader against canonical Cassandra 5.0 datasets
//! covering LZ4, Snappy, and Deflate compression algorithms.

use cqlite_core::{
    storage::sstable::{chunked_data_reader::ChunkedDataReader, compression_info::CompressionInfo},
    testing::{list_tables, resolve_table_to_sstable_path},
};
use std::collections::HashMap;
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

/// Find CompressionInfo.db files in a table directory
fn find_compressioninfo_files(table_dir: &Path) -> Vec<std::path::PathBuf> {
    if let Ok(dir) = fs::read_dir(table_dir) {
        dir.filter_map(|entry| entry.ok())
            .map(|e| e.path())
            .filter(|p| p.is_file())
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.ends_with("-CompressionInfo.db"))
                    .unwrap_or(false)
            })
            .collect()
    } else {
        Vec::new()
    }
}

/// Find corresponding Data.db file for a CompressionInfo.db file
fn find_data_file(compression_info_path: &Path) -> Option<std::path::PathBuf> {
    let stem = compression_info_path
        .file_name()?
        .to_str()?
        .strip_suffix("-CompressionInfo.db")?;

    let data_path = compression_info_path.with_file_name(format!("{}-Data.db", stem));

    if data_path.exists() {
        Some(data_path)
    } else {
        None
    }
}

#[test]
fn test_chunked_reader_with_real_lz4() {
    // Find LZ4-compressed tables from canonical datasets
    let mut found_lz4 = false;

    for table in list_tables(None).unwrap_or_default() {
        let table_dir = match resolve_table_to_sstable_path(&table.keyspace, &table.table) {
            Ok(p) => p,
            Err(_) => continue,
        };

        for ci_path in find_compressioninfo_files(&table_dir) {
            // Parse CompressionInfo to check algorithm
            let ci_data = match fs::read(&ci_path) {
                Ok(d) => d,
                Err(_) => continue,
            };

            let compression_info = match CompressionInfo::parse(&ci_data) {
                Ok(info) => info,
                Err(_) => continue,
            };

            // Only test LZ4 in this test
            if !compression_info.algorithm.to_uppercase().contains("LZ4") {
                continue;
            }

            // Find corresponding Data.db
            let data_path = match find_data_file(&ci_path) {
                Some(p) => p,
                None => continue,
            };

            println!("Testing LZ4 ChunkedDataReader with: {:?}", ci_path);
            println!("  Algorithm: {}", compression_info.algorithm);
            println!("  Chunk length: {} bytes", compression_info.chunk_length);
            println!("  Total chunks: {}", compression_info.chunk_offsets.len());

            // Open Data.db file
            let data_file = fs::File::open(&data_path).expect("Failed to open Data.db");
            let file_size = data_file.metadata().expect("Failed to get metadata").len();

            // Create ChunkedDataReader
            let compression_info_arc = Arc::new(compression_info);
            let mut reader =
                ChunkedDataReader::new(data_file, file_size, compression_info_arc.clone())
                    .expect("Failed to create ChunkedDataReader");

            // Test basic read
            let mut buffer = vec![0u8; 1024];
            let bytes_read = reader
                .read(&mut buffer)
                .expect("Failed to read from ChunkedDataReader");

            assert!(bytes_read > 0, "Should read at least some data");
            assert!(bytes_read <= 1024, "Should not read more than buffer size");

            println!("  ✅ Read {} bytes successfully", bytes_read);

            // Test position tracking
            assert_eq!(reader.position(), bytes_read as u64);

            // Test seeking
            reader
                .seek(SeekFrom::Start(0))
                .expect("Failed to seek to start");
            assert_eq!(reader.position(), 0);

            // Test multi-chunk read
            let mut large_buffer = vec![0u8; (compression_info_arc.chunk_length as usize) * 2];
            let bytes_read = reader
                .read(&mut large_buffer)
                .expect("Failed to read large buffer");
            assert!(bytes_read > 0);

            println!("  ✅ Multi-chunk read: {} bytes", bytes_read);

            found_lz4 = true;
            break;
        }

        if found_lz4 {
            break;
        }
    }

    if !found_lz4 {
        println!("⚠️ No LZ4-compressed tables found - skipping test");
    }
}

#[test]
fn test_chunked_reader_with_real_snappy() {
    let mut found_snappy = false;

    for table in list_tables(None).unwrap_or_default() {
        let table_dir = match resolve_table_to_sstable_path(&table.keyspace, &table.table) {
            Ok(p) => p,
            Err(_) => continue,
        };

        for ci_path in find_compressioninfo_files(&table_dir) {
            let ci_data = match fs::read(&ci_path) {
                Ok(d) => d,
                Err(_) => continue,
            };

            let compression_info = match CompressionInfo::parse(&ci_data) {
                Ok(info) => info,
                Err(_) => continue,
            };

            if !compression_info.algorithm.to_uppercase().contains("SNAPPY") {
                continue;
            }

            let data_path = match find_data_file(&ci_path) {
                Some(p) => p,
                None => continue,
            };

            println!("Testing Snappy ChunkedDataReader with: {:?}", ci_path);

            let data_file = fs::File::open(&data_path).expect("Failed to open Data.db");
            let file_size = data_file.metadata().expect("Failed to get metadata").len();

            let compression_info_arc = Arc::new(compression_info);
            let mut reader = ChunkedDataReader::new(data_file, file_size, compression_info_arc)
                .expect("Failed to create ChunkedDataReader");

            let mut buffer = vec![0u8; 1024];
            let bytes_read = reader.read(&mut buffer).expect("Failed to read");

            assert!(bytes_read > 0);
            println!("  ✅ Snappy read {} bytes successfully", bytes_read);

            found_snappy = true;
            break;
        }

        if found_snappy {
            break;
        }
    }

    if !found_snappy {
        println!("⚠️ No Snappy-compressed tables found - skipping test");
    }
}

#[test]
fn test_chunked_reader_with_real_deflate() {
    let mut found_deflate = false;

    for table in list_tables(None).unwrap_or_default() {
        let table_dir = match resolve_table_to_sstable_path(&table.keyspace, &table.table) {
            Ok(p) => p,
            Err(_) => continue,
        };

        for ci_path in find_compressioninfo_files(&table_dir) {
            let ci_data = match fs::read(&ci_path) {
                Ok(d) => d,
                Err(_) => continue,
            };

            let compression_info = match CompressionInfo::parse(&ci_data) {
                Ok(info) => info,
                Err(_) => continue,
            };

            if !compression_info
                .algorithm
                .to_uppercase()
                .contains("DEFLATE")
            {
                continue;
            }

            let data_path = match find_data_file(&ci_path) {
                Some(p) => p,
                None => continue,
            };

            println!("Testing Deflate ChunkedDataReader with: {:?}", ci_path);

            let data_file = fs::File::open(&data_path).expect("Failed to open Data.db");
            let file_size = data_file.metadata().expect("Failed to get metadata").len();

            let compression_info_arc = Arc::new(compression_info);
            let mut reader = ChunkedDataReader::new(data_file, file_size, compression_info_arc)
                .expect("Failed to create ChunkedDataReader");

            let mut buffer = vec![0u8; 1024];
            let bytes_read = reader.read(&mut buffer).expect("Failed to read");

            assert!(bytes_read > 0);
            println!("  ✅ Deflate read {} bytes successfully", bytes_read);

            found_deflate = true;
            break;
        }

        if found_deflate {
            break;
        }
    }

    if !found_deflate {
        println!("⚠️ No Deflate-compressed tables found - skipping test");
    }
}

#[test]
fn test_chunked_reader_seeks_and_reads() {
    // Find any compressed table for seeking tests
    let mut tested = false;

    for table in list_tables(None).unwrap_or_default() {
        let table_dir = match resolve_table_to_sstable_path(&table.keyspace, &table.table) {
            Ok(p) => p,
            Err(_) => continue,
        };

        for ci_path in find_compressioninfo_files(&table_dir) {
            let ci_data = match fs::read(&ci_path) {
                Ok(d) => d,
                Err(_) => continue,
            };

            let compression_info = match CompressionInfo::parse(&ci_data) {
                Ok(info) => info,
                Err(_) => continue,
            };

            let data_path = match find_data_file(&ci_path) {
                Some(p) => p,
                None => continue,
            };

            println!("Testing seek operations with: {:?}", ci_path);

            let data_file = fs::File::open(&data_path).expect("Failed to open Data.db");
            let file_size = data_file.metadata().expect("Failed to get metadata").len();

            let compression_info_arc = Arc::new(compression_info.clone());
            let mut reader = ChunkedDataReader::new(data_file, file_size, compression_info_arc)
                .expect("Failed to create ChunkedDataReader");

            // Test 1: Read from start
            let mut buf1 = vec![0u8; 100];
            let _bytes_read = reader.read(&mut buf1).expect("Failed to read buf1");

            // Test 2: Seek to middle of second chunk (if available)
            if compression_info.chunk_offsets.len() > 1 {
                let second_chunk_offset = compression_info.chunk_length as u64 + 50;
                reader
                    .seek(SeekFrom::Start(second_chunk_offset))
                    .expect("Failed to seek");
                assert_eq!(reader.position(), second_chunk_offset);

                let mut buf2 = vec![0u8; 100];
                let bytes_read = reader.read(&mut buf2).expect("Failed to read buf2");
                assert!(bytes_read > 0);

                println!("  ✅ Seek to chunk boundary + offset works");
            }

            // Test 3: Seek back to start
            reader
                .seek(SeekFrom::Start(0))
                .expect("Failed to seek to start");
            assert_eq!(reader.position(), 0);

            let mut buf3 = vec![0u8; 100];
            let _bytes_read = reader.read(&mut buf3).expect("Failed to read buf3");

            // buf1 and buf3 should match (same position)
            assert_eq!(&buf1[..], &buf3[..]);

            println!("  ✅ Seek back to start works");

            tested = true;
            break;
        }

        if tested {
            break;
        }
    }

    if !tested {
        println!("⚠️ No compressed tables found for seek tests - skipping");
    }
}

#[test]
fn test_chunked_reader_chunk_boundary_spanning() {
    // Test reading across chunk boundaries
    let mut tested = false;

    for table in list_tables(None).unwrap_or_default() {
        let table_dir = match resolve_table_to_sstable_path(&table.keyspace, &table.table) {
            Ok(p) => p,
            Err(_) => continue,
        };

        for ci_path in find_compressioninfo_files(&table_dir) {
            let ci_data = match fs::read(&ci_path) {
                Ok(d) => d,
                Err(_) => continue,
            };

            let compression_info = match CompressionInfo::parse(&ci_data) {
                Ok(info) => info,
                Err(_) => continue,
            };

            // Need at least 2 chunks for this test
            if compression_info.chunk_offsets.len() < 2 {
                continue;
            }

            let data_path = match find_data_file(&ci_path) {
                Some(p) => p,
                None => continue,
            };

            println!("Testing chunk boundary spanning with: {:?}", ci_path);
            println!("  Chunks: {}", compression_info.chunk_offsets.len());

            let data_file = fs::File::open(&data_path).expect("Failed to open Data.db");
            let file_size = data_file.metadata().expect("Failed to get metadata").len();

            let compression_info_arc = Arc::new(compression_info.clone());
            let mut reader = ChunkedDataReader::new(data_file, file_size, compression_info_arc)
                .expect("Failed to create ChunkedDataReader");

            // Seek to near end of first chunk
            let chunk_len = compression_info.chunk_length as u64;
            let near_boundary = chunk_len - 50;
            reader
                .seek(SeekFrom::Start(near_boundary))
                .expect("Failed to seek");

            // Read buffer that spans chunk boundary
            let mut spanning_buffer = vec![0u8; 200];
            let bytes_read = reader
                .read(&mut spanning_buffer)
                .expect("Failed to read spanning buffer");

            // Should read data from both chunks
            assert!(bytes_read > 0);
            assert!(bytes_read <= 200);

            // Verify position advanced correctly
            assert_eq!(reader.position(), near_boundary + bytes_read as u64);

            println!("  ✅ Read {} bytes spanning chunk boundary", bytes_read);

            tested = true;
            break;
        }

        if tested {
            break;
        }
    }

    if !tested {
        println!("⚠️ No multi-chunk compressed tables found - skipping boundary test");
    }
}

#[test]
fn test_chunked_reader_all_algorithms() {
    // Discover and test one table per algorithm
    let mut by_algo: HashMap<String, std::path::PathBuf> = HashMap::new();

    for table in list_tables(None).unwrap_or_default() {
        let table_dir = match resolve_table_to_sstable_path(&table.keyspace, &table.table) {
            Ok(p) => p,
            Err(_) => continue,
        };

        for ci_path in find_compressioninfo_files(&table_dir) {
            if let Ok(ci_data) = fs::read(&ci_path) {
                if let Ok(info) = CompressionInfo::parse(&ci_data) {
                    let algo_key = info.algorithm.to_uppercase();
                    by_algo.entry(algo_key).or_insert_with(|| ci_path.clone());
                    // Stop when we have LZ4, Snappy, and Deflate
                    if by_algo.len() >= 3 {
                        break;
                    }
                }
            }
        }

        if by_algo.len() >= 3 {
            break;
        }
    }

    if by_algo.is_empty() {
        println!("⚠️ No compressed tables found - skipping algorithm coverage test");
        return;
    }

    println!(
        "Testing ChunkedDataReader with {} algorithms",
        by_algo.len()
    );

    for (algo, ci_path) in by_algo {
        println!("\nTesting algorithm: {}", algo);

        let ci_data = fs::read(&ci_path).expect("Failed to read CompressionInfo.db");
        let compression_info =
            CompressionInfo::parse(&ci_data).expect("Failed to parse CompressionInfo.db");

        let data_path = find_data_file(&ci_path).expect("Failed to find Data.db");
        let data_file = fs::File::open(&data_path).expect("Failed to open Data.db");
        let file_size = data_file.metadata().expect("Failed to get metadata").len();

        let compression_info_arc = Arc::new(compression_info);
        let mut reader = ChunkedDataReader::new(data_file, file_size, compression_info_arc)
            .expect("Failed to create ChunkedDataReader");

        // Perform basic read test
        let mut buffer = vec![0u8; 512];
        let bytes_read = reader.read(&mut buffer).expect("Failed to read");

        assert!(bytes_read > 0, "Should read data for algorithm: {}", algo);
        println!("  ✅ {} read {} bytes successfully", algo, bytes_read);
    }
}
