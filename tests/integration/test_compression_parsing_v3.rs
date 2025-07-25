use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Test parsing the real CompressionInfo.db file
    let compression_data = fs::read("test-env/cassandra5/sstables/all_types-46200090673711f0b2cf19d64e7cbecb/nb-1-big-CompressionInfo.db")?;
    
    println!("CompressionInfo.db size: {} bytes", compression_data.len());
    println!("First 64 bytes as hex:");
    for (i, &byte) in compression_data.iter().take(64).enumerate() {
        print!("{:02x} ", byte);
        if (i + 1) % 16 == 0 {
            println!();
        }
    }
    println!();
    
    // From hex: 00 0d 4c 5a 34 43 6f 6d 70 72 65 73 73 6f 72 00 00 00 00 00 00 40 00
    //           ^len  "LZ4Compressor"                           00000000   0x4000
    // Looks like: 2-byte len + string + padding + chunk_length
    
    let mut offset = 0;
    
    // Read algorithm name length (2 bytes big-endian)
    let algo_len = u16::from_be_bytes([compression_data[0], compression_data[1]]) as usize;
    offset += 2;
    println!("Algorithm name length: {}", algo_len);
    
    // Read algorithm name
    let algorithm = String::from_utf8(compression_data[offset..offset + algo_len].to_vec())?;
    offset += algo_len;
    println!("Algorithm: {}", algorithm);
    
    // Looking at hex pattern, after "LZ4Compressor" there are several zero bytes, then 0x4000
    // Let's skip to where we see the 0x4000 pattern which is 16384 in decimal (could be chunk size)
    
    // Skip any zero padding to find the actual data
    while offset < compression_data.len() && compression_data[offset] == 0 {
        offset += 1;
    }
    
    println!("After skipping zeros, offset: {}", offset);
    
    if offset + 4 <= compression_data.len() {
        // Try reading as chunk length
        let chunk_length = u32::from_be_bytes([
            compression_data[offset], 
            compression_data[offset + 1], 
            compression_data[offset + 2], 
            compression_data[offset + 3]
        ]);
        println!("Potential chunk length: {} (0x{:x})", chunk_length, chunk_length);
        
        // 0x4000 = 16384, which could be a reasonable chunk size
        if chunk_length == 16384 || chunk_length == 65536 || chunk_length == 4096 {
            offset += 4;
            
            // Look for data length and chunk count
            // Based on the pattern, let's try different interpretations
            
            // Skip more padding if needed
            let mut next_non_zero = offset;
            while next_non_zero < compression_data.len() && compression_data[next_non_zero] == 0 {
                next_non_zero += 1;
            }
            
            println!("Next non-zero byte at offset: {}", next_non_zero);
            
            // Look for a pattern that makes sense
            // In the hex, after the zeros we see: 7f ff ff ff which could be a large number
            if next_non_zero + 8 <= compression_data.len() {
                let potential_data_len = u64::from_be_bytes([
                    compression_data[next_non_zero], compression_data[next_non_zero + 1],
                    compression_data[next_non_zero + 2], compression_data[next_non_zero + 3],
                    compression_data[next_non_zero + 4], compression_data[next_non_zero + 5],
                    compression_data[next_non_zero + 6], compression_data[next_non_zero + 7]
                ]);
                println!("Potential data length: {} (0x{:x})", potential_data_len, potential_data_len);
                
                next_non_zero += 8;
                
                if next_non_zero + 4 <= compression_data.len() {
                    let potential_chunk_count = u32::from_be_bytes([
                        compression_data[next_non_zero], compression_data[next_non_zero + 1],
                        compression_data[next_non_zero + 2], compression_data[next_non_zero + 3]
                    ]);
                    println!("Potential chunk count: {}", potential_chunk_count);
                    
                    // Try to calculate expected file size
                    if potential_chunk_count > 0 && potential_chunk_count < 1000 {
                        let header_size = next_non_zero + 4;
                        let chunk_data_size = potential_chunk_count as usize * 16; // Each chunk = 8+4+4 bytes
                        let expected_size = header_size + chunk_data_size;
                        println!("Expected file size: {} bytes, actual: {} bytes", expected_size, compression_data.len());
                        
                        if expected_size <= compression_data.len() {
                            println!("This looks like a valid interpretation!");
                            
                            // Parse first few chunks
                            offset = next_non_zero + 4;
                            for i in 0..std::cmp::min(3, potential_chunk_count) {
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
                                    
                                    println!("Chunk {}: offset={}, compressed={}, uncompressed={}", 
                                             i, chunk_offset, compressed_size, uncompressed_size);
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