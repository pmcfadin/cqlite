use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Test parsing the real CompressionInfo.db file
    let compression_data = fs::read("test-env/cassandra5/sstables/all_types-46200090673711f0b2cf19d64e7cbecb/nb-1-big-CompressionInfo.db")?;
    
    println!("CompressionInfo.db size: {} bytes", compression_data.len());
    println!("First 32 bytes as hex:");
    for i in 0..32.min(compression_data.len()) {
        print!("{:02x} ", compression_data[i]);
        if (i + 1) % 16 == 0 {
            println!();
        }
    }
    println!();
    
    // Based on hex dump: 00 0d 4c 5a 34 43 6f 6d 70 72 65 73 73 6f 72
    // This looks like: 2-byte length (00 0d = 13), followed by "LZ4Compressor"
    
    if compression_data.len() < 4 {
        return Err("File too short".into());
    }
    
    let mut offset = 0;
    
    // Try different interpretation: first 2 bytes might be length
    let algo_len = u16::from_be_bytes([compression_data[0], compression_data[1]]) as usize;
    offset += 2;
    
    println!("Algorithm name length (2-byte): {}", algo_len);
    
    if offset + algo_len > compression_data.len() {
        return Err(format!("Invalid algorithm length: {} bytes needed, {} available", 
                          algo_len, compression_data.len() - offset).into());
    }
    
    // Read algorithm name
    let algorithm = String::from_utf8(compression_data[offset..offset + algo_len].to_vec())?;
    offset += algo_len;
    
    println!("Algorithm: {}", algorithm);
    
    // Now look at the remaining data structure
    println!("Remaining bytes: {}", compression_data.len() - offset);
    
    if offset + 4 <= compression_data.len() {
        let next_4_bytes = u32::from_be_bytes([
            compression_data[offset], 
            compression_data[offset + 1], 
            compression_data[offset + 2], 
            compression_data[offset + 3]
        ]);
        println!("Next 4 bytes as u32: {} (0x{:08x})", next_4_bytes, next_4_bytes);
        
        // Check if this could be chunk length - common values are 64KB (65536) or 4KB (4096)
        if next_4_bytes == 65536 || next_4_bytes == 4096 || next_4_bytes < 1024*1024 {
            println!("This looks like chunk length: {} bytes", next_4_bytes);
            let chunk_length = next_4_bytes;
            offset += 4;
            
            // Check next 8 bytes for data length
            if offset + 8 <= compression_data.len() {
                let data_length = u64::from_be_bytes([
                    compression_data[offset], compression_data[offset + 1], compression_data[offset + 2], compression_data[offset + 3],
                    compression_data[offset + 4], compression_data[offset + 5], compression_data[offset + 6], compression_data[offset + 7]
                ]);
                println!("Data length: {} bytes", data_length);
                offset += 8;
                
                // Check next 4 bytes for chunk count
                if offset + 4 <= compression_data.len() {
                    let chunk_count = u32::from_be_bytes([
                        compression_data[offset], 
                        compression_data[offset + 1], 
                        compression_data[offset + 2], 
                        compression_data[offset + 3]
                    ]);
                    println!("Chunk count: {}", chunk_count);
                    offset += 4;
                    
                    // Validate chunk count is reasonable
                    if chunk_count > 0 && chunk_count < 10000 {
                        let expected_chunk_data = chunk_count as usize * 16; // 8 + 4 + 4 bytes per chunk
                        let remaining = compression_data.len() - offset;
                        println!("Expected chunk data: {} bytes, remaining: {} bytes", 
                                expected_chunk_data, remaining);
                        
                        if remaining >= expected_chunk_data {
                            // Parse first few chunks
                            for i in 0..std::cmp::min(5, chunk_count) {
                                if offset + 16 <= compression_data.len() {
                                    let chunk_offset = u64::from_be_bytes([
                                        compression_data[offset], compression_data[offset + 1], 
                                        compression_data[offset + 2], compression_data[offset + 3],
                                        compression_data[offset + 4], compression_data[offset + 5], 
                                        compression_data[offset + 6], compression_data[offset + 7]
                                    ]);
                                    let compressed_size = u32::from_be_bytes([
                                        compression_data[offset + 8], compression_data[offset + 9], 
                                        compression_data[offset + 10], compression_data[offset + 11]
                                    ]);
                                    let uncompressed_size = u32::from_be_bytes([
                                        compression_data[offset + 12], compression_data[offset + 13], 
                                        compression_data[offset + 14], compression_data[offset + 15]
                                    ]);
                                    
                                    println!("Chunk {}: offset={}, compressed={}, uncompressed={}, ratio={:.1}%", 
                                             i, chunk_offset, compressed_size, uncompressed_size,
                                             (compressed_size as f64 / uncompressed_size as f64) * 100.0);
                                    offset += 16;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    
    Ok(())
}