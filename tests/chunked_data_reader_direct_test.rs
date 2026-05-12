//! Direct tests for ChunkedDataReader using known test data paths
//!
//! This test file directly references known compressed SSTable locations
//! to ensure reliable test coverage without relying on discovery mechanisms.

use cqlite_core::storage::sstable::{
    chunked_data_reader::ChunkedDataReader, compression_info::CompressionInfo,
};
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::Arc;

/// Get the datasets root from environment or use default
fn datasets_root() -> PathBuf {
    std::env::var("CQLITE_DATASETS_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("test-data/datasets"))
}

/// Test ChunkedDataReader with real LZ4-compressed SSTable
#[test]
fn test_chunked_reader_lz4_direct() {
    let datasets = datasets_root();

    // Direct path to known LZ4-compressed table
    let table_dir =
        datasets.join("sstables/test_timeseries/tick_data-706fe650934a11f08d448925b7a9e804");

    let ci_path = table_dir.join("nb-1-big-CompressionInfo.db");
    let data_path = table_dir.join("nb-1-big-Data.db");

    // Skip if test data not available
    if !ci_path.exists() || !data_path.exists() {
        println!(
            "⚠️  LZ4 test data not available at {table_dir:?} - skipping"
        );
        return;
    }

    println!("✅ Testing LZ4 ChunkedDataReader with: {ci_path:?}");

    // Parse CompressionInfo
    let ci_data = fs::read(&ci_path).expect("Failed to read CompressionInfo.db");
    let compression_info =
        CompressionInfo::parse(&ci_data).expect("Failed to parse CompressionInfo");

    println!("  Algorithm: {}", compression_info.algorithm);
    println!("  Chunk length: {} bytes", compression_info.chunk_length);
    println!("  Total chunks: {}", compression_info.chunk_offsets.len());

    assert!(compression_info.algorithm.to_uppercase().contains("LZ4"));

    // Open Data.db
    let data_file = fs::File::open(&data_path).expect("Failed to open Data.db");
    let file_size = data_file.metadata().expect("Failed to get metadata").len();

    // Create ChunkedDataReader
    let compression_info_arc = Arc::new(compression_info);
    let mut reader = ChunkedDataReader::new(data_file, file_size, compression_info_arc.clone())
        .expect("Failed to create ChunkedDataReader");

    // Test 1: Basic read
    let mut buffer = vec![0u8; 1024];
    let bytes_read = reader.read(&mut buffer).expect("Failed to read");

    assert!(bytes_read > 0, "Should read at least some data");
    println!("  ✅ Read {bytes_read} bytes successfully");

    // Test 2: Position tracking
    assert_eq!(reader.position(), bytes_read as u64);

    // Test 3: Seek back to start
    reader.seek(SeekFrom::Start(0)).expect("Failed to seek");
    assert_eq!(reader.position(), 0);

    // Test 4: Multi-chunk read (if multiple chunks exist)
    if compression_info_arc.chunk_offsets.len() > 1 {
        let large_size = (compression_info_arc.chunk_length as usize) * 2;
        let mut large_buffer = vec![0u8; large_size];
        let bytes_read = reader
            .read(&mut large_buffer)
            .expect("Failed to read large buffer");
        assert!(bytes_read > 0);
        println!("  ✅ Multi-chunk read: {bytes_read} bytes");
    }
}

/// Test ChunkedDataReader with real Snappy-compressed SSTable
#[test]
fn test_chunked_reader_snappy_direct() {
    let datasets = datasets_root();

    // Direct path to known Snappy-compressed table
    let table_dir =
        datasets.join("sstables/test_timeseries/user_sessions-7063d860934a11f08d448925b7a9e804");

    let ci_path = table_dir.join("nb-1-big-CompressionInfo.db");
    let data_path = table_dir.join("nb-1-big-Data.db");

    if !ci_path.exists() || !data_path.exists() {
        println!(
            "⚠️  Snappy test data not available at {table_dir:?} - skipping"
        );
        return;
    }

    println!("✅ Testing Snappy ChunkedDataReader with: {ci_path:?}");

    let ci_data = fs::read(&ci_path).expect("Failed to read CompressionInfo.db");
    let compression_info =
        CompressionInfo::parse(&ci_data).expect("Failed to parse CompressionInfo");

    println!("  Algorithm: {}", compression_info.algorithm);
    assert!(compression_info.algorithm.to_uppercase().contains("SNAPPY"));

    let data_file = fs::File::open(&data_path).expect("Failed to open Data.db");
    let file_size = data_file.metadata().expect("Failed to get metadata").len();

    let compression_info_arc = Arc::new(compression_info);
    let mut reader = ChunkedDataReader::new(data_file, file_size, compression_info_arc)
        .expect("Failed to create ChunkedDataReader");

    let mut buffer = vec![0u8; 512];
    let bytes_read = reader.read(&mut buffer).expect("Failed to read");

    assert!(bytes_read > 0);
    println!("  ✅ Snappy read {bytes_read} bytes successfully");
}

/// Test Seek trait implementation across chunk boundaries
#[test]
fn test_seek_trait_implementation() {
    let datasets = datasets_root();
    let table_dir =
        datasets.join("sstables/test_timeseries/sensor_data-701e1cd0934a11f08d448925b7a9e804");

    let ci_path = table_dir.join("nb-1-big-CompressionInfo.db");
    let data_path = table_dir.join("nb-1-big-Data.db");

    if !ci_path.exists() || !data_path.exists() {
        println!("⚠️  Test data not available - skipping seek test");
        return;
    }

    println!("✅ Testing Seek trait implementation");

    let ci_data = fs::read(&ci_path).expect("Failed to read CompressionInfo.db");
    let compression_info =
        CompressionInfo::parse(&ci_data).expect("Failed to parse CompressionInfo");

    let data_file = fs::File::open(&data_path).expect("Failed to open Data.db");
    let file_size = data_file.metadata().expect("Failed to get metadata").len();

    let compression_info_arc = Arc::new(compression_info.clone());
    let mut reader = ChunkedDataReader::new(data_file, file_size, compression_info_arc)
        .expect("Failed to create ChunkedDataReader");

    // Test SeekFrom::Start
    let pos = reader.seek(SeekFrom::Start(100)).expect("Failed to seek");
    assert_eq!(pos, 100);
    assert_eq!(reader.position(), 100);
    println!("  ✅ SeekFrom::Start works");

    // Test SeekFrom::Current
    let pos = reader.seek(SeekFrom::Current(50)).expect("Failed to seek");
    assert_eq!(pos, 150);
    println!("  ✅ SeekFrom::Current works");

    // Test SeekFrom::End
    let total_len = compression_info.data_length;
    let pos = reader.seek(SeekFrom::End(-100)).expect("Failed to seek");
    assert_eq!(pos, total_len - 100);
    println!("  ✅ SeekFrom::End works");

    // Test seeking across chunk boundary (if multi-chunk)
    if compression_info.chunk_offsets.len() > 1 {
        let chunk_boundary = compression_info.chunk_length as u64;
        let pos = reader
            .seek(SeekFrom::Start(chunk_boundary + 10))
            .expect("Failed to seek");
        assert_eq!(pos, chunk_boundary + 10);

        let mut buf = vec![0u8; 20];
        let bytes_read = reader.read(&mut buf).expect("Failed to read after seek");
        assert!(bytes_read > 0);
        println!("  ✅ Seek across chunk boundary works");
    }
}

/// Test that ChunkedDataReader correctly handles rows spanning chunk boundaries
#[test]
fn test_row_assembly_across_chunks() {
    let datasets = datasets_root();
    let table_dir =
        datasets.join("sstables/test_timeseries/log_entries-7046da80934a11f08d448925b7a9e804");

    let ci_path = table_dir.join("nb-1-big-CompressionInfo.db");
    let data_path = table_dir.join("nb-1-big-Data.db");

    if !ci_path.exists() || !data_path.exists() {
        println!("⚠️  Test data not available - skipping row assembly test");
        return;
    }

    println!("✅ Testing row assembly across chunk boundaries");

    let ci_data = fs::read(&ci_path).expect("Failed to read CompressionInfo.db");
    let compression_info =
        CompressionInfo::parse(&ci_data).expect("Failed to parse CompressionInfo");

    // Only test if multi-chunk
    if compression_info.chunk_offsets.len() < 2 {
        println!("  ⚠️  Only 1 chunk - skipping boundary test");
        return;
    }

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

    // Read across chunk boundary
    let mut spanning_buffer = vec![0u8; 200];
    let bytes_read = reader
        .read(&mut spanning_buffer)
        .expect("Failed to read spanning buffer");

    assert!(bytes_read > 0);
    assert_eq!(reader.position(), near_boundary + bytes_read as u64);

    println!("  ✅ Read {bytes_read} bytes spanning chunk boundary");
    println!("  ✅ Position correctly updated: {}", reader.position());
}
