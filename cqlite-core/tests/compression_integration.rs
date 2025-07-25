//! Integration tests for compression parsing and SSTable format detection
//!
//! These tests validate compression-related functionality using real SSTable files
//! from the test-env directory, ensuring compatibility with Cassandra formats.

use std::fs;

#[test]
fn test_compression_info_parsing() -> Result<(), Box<dyn std::error::Error>> {
    // Test parsing the real CompressionInfo.db file
    let compression_data = fs::read("../test-env/cassandra5/sstables/all_types-46200090673711f0b2cf19d64e7cbecb/nb-1-big-CompressionInfo.db")?;
    
    println!("CompressionInfo.db size: {} bytes", compression_data.len());
    
    // Parse the binary format based on hex dump analysis
    if compression_data.len() < 20 {
        return Err("File too short".into());
    }
    
    let mut offset = 0;
    
    // Read algorithm name length (4 bytes big-endian)
    let algo_len = u32::from_be_bytes([
        compression_data[offset], 
        compression_data[offset + 1], 
        compression_data[offset + 2], 
        compression_data[offset + 3]
    ]) as usize;
    offset += 4;
    
    println!("Algorithm name length: {}", algo_len);
    
    // Read algorithm name
    if offset + algo_len > compression_data.len() {
        return Err("Invalid algorithm length".into());
    }
    
    let algorithm = String::from_utf8(compression_data[offset..offset + algo_len].to_vec())?;
    offset += algo_len;
    
    println!("Algorithm: {}", algorithm);
    
    // Read chunk length (4 bytes big-endian)
    if offset + 4 > compression_data.len() {
        return Err("Missing chunk length".into());
    }
    
    let chunk_length = u32::from_be_bytes([
        compression_data[offset], 
        compression_data[offset + 1], 
        compression_data[offset + 2], 
        compression_data[offset + 3]
    ]);
    offset += 4;
    
    println!("Chunk length: {} bytes", chunk_length);
    
    // Read total data length (8 bytes big-endian)
    if offset + 8 > compression_data.len() {
        return Err("Missing data length".into());
    }
    
    let data_length = u64::from_be_bytes([
        compression_data[offset], compression_data[offset + 1], compression_data[offset + 2], compression_data[offset + 3],
        compression_data[offset + 4], compression_data[offset + 5], compression_data[offset + 6], compression_data[offset + 7]
    ]);
    offset += 8;
    
    println!("Total data length: {} bytes", data_length);
    
    // Read number of chunks (4 bytes big-endian)
    if offset + 4 > compression_data.len() {
        return Err("Missing chunk count".into());
    }
    
    let chunk_count = u32::from_be_bytes([
        compression_data[offset], 
        compression_data[offset + 1], 
        compression_data[offset + 2], 
        compression_data[offset + 3]
    ]) as usize;
    offset += 4;
    
    println!("Number of chunks: {}", chunk_count);
    
    // Read first few chunks as examples
    for i in 0..std::cmp::min(5, chunk_count) {
        if offset + 16 > compression_data.len() {
            break;
        }
        
        // Read chunk offset (8 bytes big-endian)
        let chunk_offset = u64::from_be_bytes([
            compression_data[offset], compression_data[offset + 1], compression_data[offset + 2], compression_data[offset + 3],
            compression_data[offset + 4], compression_data[offset + 5], compression_data[offset + 6], compression_data[offset + 7]
        ]);
        offset += 8;
        
        // Read compressed size (4 bytes big-endian)
        let compressed_size = u32::from_be_bytes([
            compression_data[offset], 
            compression_data[offset + 1], 
            compression_data[offset + 2], 
            compression_data[offset + 3]
        ]);
        offset += 4;
        
        // Skip uncompressed size (4 bytes)
        offset += 4;
        
        println!("Chunk {}: offset={}, compressed_size={}", i, chunk_offset, compressed_size);
    }
    
    // Validate basic parsing worked
    assert!(chunk_count > 0, "Should have at least one chunk");
    assert!(data_length > 0, "Should have non-zero data length");
    assert!(!algorithm.is_empty(), "Algorithm should not be empty");
    
    Ok(())
}

#[test]
fn test_compression_info_format_parsing() -> Result<(), Box<dyn std::error::Error>> {
    // Read the actual CompressionInfo.db file
    let data = fs::read("../test-env/cassandra5/sstables/all_types-46200090673711f0b2cf19d64e7cbecb/nb-1-big-CompressionInfo.db")?;
    
    // Parse the first few bytes to understand the format
    if data.len() >= 20 {
        // Read algorithm name length (first 4 bytes)
        let algo_len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        println!("Algorithm name length: {}", algo_len);
        
        if data.len() >= 4 + algo_len {
            let algorithm = String::from_utf8_lossy(&data[4..4 + algo_len]);
            println!("Algorithm: {}", algorithm);
            
            let mut offset = 4 + algo_len;
            
            // Read chunk length (next 4 bytes)
            if data.len() >= offset + 4 {
                let chunk_length = u32::from_be_bytes([
                    data[offset], data[offset + 1], data[offset + 2], data[offset + 3]
                ]);
                println!("Chunk length: {}", chunk_length);
                offset += 4;
                
                // Read data length (next 8 bytes)
                if data.len() >= offset + 8 {
                    let data_length = u64::from_be_bytes([
                        data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
                        data[offset + 4], data[offset + 5], data[offset + 6], data[offset + 7]
                    ]);
                    println!("Data length: {}", data_length);
                    offset += 8;
                    
                    // Read chunk count (next 4 bytes)
                    if data.len() >= offset + 4 {
                        let chunk_count = u32::from_be_bytes([
                            data[offset], data[offset + 1], data[offset + 2], data[offset + 3]
                        ]);
                        println!("Chunk count: {}", chunk_count);
                        
                        // Validate parsing results
                        assert!(chunk_count > 0, "Should have at least one chunk");
                        assert!(data_length > 0, "Should have non-zero data length");
                        assert!(!algorithm.is_empty(), "Algorithm should not be empty");
                        assert!(chunk_length > 0, "Chunk length should be positive");
                    }
                }
            }
        }
    }
    
    Ok(())
}
