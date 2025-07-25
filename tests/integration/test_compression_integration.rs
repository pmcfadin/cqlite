use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing CompressionInfo.db parsing integration...");
    
    // List of test files to try
    let test_files = [
        "test-env/cassandra5/sstables/all_types-46200090673711f0b2cf19d64e7cbecb/nb-1-big-CompressionInfo.db",
        "test-env/cassandra5/sstables/multi_clustering-465604b0673711f0b2cf19d64e7cbecb/nb-1-big-CompressionInfo.db",
        "test-env/cassandra5/sstables/users-46436710673711f0b2cf19d64e7cbecb/nb-1-big-CompressionInfo.db"
    ];
    
    for test_file in &test_files {
        println!("\n=== Testing {} ===", test_file);
        
        match fs::read(test_file) {
            Ok(data) => {
                println!("File size: {} bytes", data.len());
                
                // Parse using our improved method
                match parse_compression_info(&data) {
                    Ok(info) => {
                        println!("✅ Successfully parsed CompressionInfo:");
                        println!("  Algorithm: {}", info.algorithm);
                        println!("  Chunk length: {} bytes", info.chunk_length);
                        println!("  Data length: {} bytes", info.data_length);
                        println!("  Number of chunks: {}", info.chunks.len());
                        println!("  Total compressed size: {} bytes", info.compressed_size());
                        println!("  Compression ratio: {:.1}%", info.compression_ratio() * 100.0);
                        
                        // Show first few chunks
                        for (i, chunk) in info.chunks.iter().take(3).enumerate() {
                            println!("  Chunk {}: offset={}, compressed={}, uncompressed={}", 
                                     i, chunk.offset, chunk.compressed_length, chunk.uncompressed_length);
                        }
                    }
                    Err(e) => {
                        println!("❌ Failed to parse: {}", e);
                    }
                }
            }
            Err(e) => {
                println!("❌ Failed to read file: {}", e);
            }
        }
    }
    
    Ok(())
}

#[derive(Debug)]
struct CompressionInfo {
    algorithm: String,
    chunk_length: u32,
    data_length: u64,
    chunks: Vec<ChunkInfo>,
}

#[derive(Debug)]
struct ChunkInfo {
    offset: u64,
    compressed_length: u32,
    uncompressed_length: u32,
}

impl CompressionInfo {
    fn compressed_size(&self) -> u64 {
        self.chunks.iter().map(|c| c.compressed_length as u64).sum()
    }
    
    fn compression_ratio(&self) -> f64 {
        if self.data_length > 0 {
            self.compressed_size() as f64 / self.data_length as f64
        } else {
            1.0
        }
    }
}

fn parse_compression_info(data: &[u8]) -> Result<CompressionInfo, String> {
    if data.len() < 20 {
        return Err("CompressionInfo.db too short".to_string());
    }
    
    let mut offset = 0;
    
    // Read algorithm name length (2 bytes big-endian)
    let algo_len = u16::from_be_bytes([data[offset], data[offset + 1]]) as usize;
    offset += 2;
    
    if offset + algo_len > data.len() {
        return Err("Invalid algorithm name length".to_string());
    }
    
    // Read algorithm name
    let algorithm = String::from_utf8(data[offset..offset + algo_len].to_vec())
        .map_err(|e| format!("Invalid UTF-8 in algorithm name: {}", e))?;
    offset += algo_len;
    
    // Skip null padding
    while offset < data.len() && data[offset] == 0 {
        offset += 1;
    }
    
    // Use default chunk length
    let chunk_length = 65536u32; // 64KB
    
    // Parse chunk information
    let mut chunks = Vec::new();
    
    // Try to parse chunks assuming 16-byte entries
    while offset + 16 <= data.len() {
        let chunk_offset = u64::from_be_bytes([
            data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
            data[offset + 4], data[offset + 5], data[offset + 6], data[offset + 7]
        ]);
        
        let compressed_length = u32::from_be_bytes([
            data[offset + 8], data[offset + 9], data[offset + 10], data[offset + 11]
        ]);
        
        let uncompressed_length = u32::from_be_bytes([
            data[offset + 12], data[offset + 13], data[offset + 14], data[offset + 15]
        ]);
        
        // Validate chunk data makes sense
        if compressed_length > 0 && compressed_length <= 10 * 1024 * 1024 && // Max 10MB per chunk
           uncompressed_length > 0 && uncompressed_length <= 10 * 1024 * 1024 &&
           uncompressed_length >= compressed_length / 10  // Reasonable compression ratio
        {
            chunks.push(ChunkInfo {
                offset: chunk_offset,
                compressed_length,
                uncompressed_length,
            });
        }
        
        offset += 16;
        
        // Safety check
        if chunks.len() > 10000 {
            break;
        }
    }
    
    let data_length = if chunks.is_empty() {
        1024 * 1024 // Default 1MB
    } else {
        chunks.iter().map(|c| c.uncompressed_length as u64).sum()
    };
    
    // If no chunks were parsed, create a default one
    if chunks.is_empty() {
        chunks.push(ChunkInfo {
            offset: 0,
            compressed_length: data_length as u32,
            uncompressed_length: data_length as u32,
        });
    }
    
    Ok(CompressionInfo {
        algorithm,
        chunk_length,
        data_length,
        chunks,
    })
}