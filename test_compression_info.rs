use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Read the actual CompressionInfo.db file
    let data = fs::read("/Users/patrick/local_projects/cqlite/test-env/cassandra5/sstables/all_types-46200090673711f0b2cf19d64e7cbecb/nb-1-big-CompressionInfo.db")?;
    
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
                        
                        println!("First few chunks:");
                        offset += 4;
                        for i in 0..std::cmp::min(chunk_count, 5) {
                            if data.len() >= offset + 16 {
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
                                
                                println!("  Chunk {}: offset={}, compressed={}, uncompressed={}", 
                                    i, chunk_offset, compressed_length, uncompressed_length);
                                offset += 16;
                            }
                        }
                    }
                }
            }
        }
    }
    
    println!("Total file size: {} bytes", data.len());
    Ok(())
}