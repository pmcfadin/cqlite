use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Detailed CompressionInfo.db analysis...");
    
    let test_file = "test-env/cassandra5/sstables/multi_clustering-465604b0673711f0b2cf19d64e7cbecb/nb-1-big-CompressionInfo.db";
    let data = fs::read(test_file)?;
    
    println!("File: {}", test_file);
    println!("Size: {} bytes", data.len());
    
    // Parse algorithm
    let algo_len = u16::from_be_bytes([data[0], data[1]]) as usize;
    let algorithm = String::from_utf8(data[2..2 + algo_len].to_vec())?;
    println!("Algorithm: {} (length: {})", algorithm, algo_len);
    
    let mut offset = 2 + algo_len;
    println!("After algorithm, offset: {}", offset);
    
    // Skip zeros and show hex pattern
    while offset < data.len() && data[offset] == 0 {
        offset += 1;
    }
    println!("After skipping zeros, offset: {}", offset);
    
    // Show remaining data as potential chunks
    println!("Remaining {} bytes:", data.len() - offset);
    
    // Let's examine the hex pattern more systematically
    let remaining = &data[offset..];
    
    // Look for patterns that might indicate chunk count or data length
    if remaining.len() >= 16 {
        // Check if the first few bytes could be chunk count (usually small number)
        for i in 0..8 {
            if offset + i + 4 <= data.len() {
                let potential_count = u32::from_be_bytes([
                    data[offset + i], data[offset + i + 1], data[offset + i + 2], data[offset + i + 3]
                ]);
                
                if potential_count > 0 && potential_count < 1000 {
                    println!("Potential chunk count at offset {}: {} (0x{:x})", 
                             offset + i, potential_count, potential_count);
                    
                    // If this is chunk count, the next data should be chunk entries
                    let chunks_start = offset + i + 4;
                    let expected_chunk_data = potential_count as usize * 16;
                    
                    if chunks_start + expected_chunk_data <= data.len() {
                        println!("✅ This could be chunk count! Expected {} bytes, available {}",
                                expected_chunk_data, data.len() - chunks_start);
                        
                        // Try to parse chunks
                        parse_chunks(&data, chunks_start, potential_count as usize);
                    }
                }
            }
        }
    }
    
    // Also try parsing from different offsets as chunks directly
    println!("\nTrying direct chunk parsing from different offsets:");
    for start_offset in (offset..offset + 32).step_by(4) {
        if start_offset + 16 <= data.len() {
            let chunk_offset = u64::from_be_bytes([
                data[start_offset], data[start_offset + 1], data[start_offset + 2], data[start_offset + 3],
                data[start_offset + 4], data[start_offset + 5], data[start_offset + 6], data[start_offset + 7]
            ]);
            
            let compressed_length = u32::from_be_bytes([
                data[start_offset + 8], data[start_offset + 9], data[start_offset + 10], data[start_offset + 11]
            ]);
            
            let uncompressed_length = u32::from_be_bytes([
                data[start_offset + 12], data[start_offset + 13], data[start_offset + 14], data[start_offset + 15]
            ]);
            
            // Check if this looks like valid chunk data
            if compressed_length > 0 && compressed_length < 1024*1024 &&
               uncompressed_length > 0 && uncompressed_length <= 1024*1024 &&
               compressed_length <= uncompressed_length
            {
                println!("Potential chunk at offset {}: offset={}, compressed={}, uncompressed={}, ratio={:.1}%",
                         start_offset, chunk_offset, compressed_length, uncompressed_length,
                         (compressed_length as f64 / uncompressed_length as f64) * 100.0);
            }
        }
    }
    
    Ok(())
}

fn parse_chunks(data: &[u8], start_offset: usize, chunk_count: usize) {
    println!("Parsing {} chunks starting at offset {}:", chunk_count, start_offset);
    
    let mut offset = start_offset;
    for i in 0..chunk_count {
        if offset + 16 <= data.len() {
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
            
            println!("  Chunk {}: offset={}, compressed={}, uncompressed={}, ratio={:.1}%",
                     i, chunk_offset, compressed_length, uncompressed_length,
                     if uncompressed_length > 0 { 
                         (compressed_length as f64 / uncompressed_length as f64) * 100.0
                     } else { 0.0 });
            
            offset += 16;
        }
    }
}