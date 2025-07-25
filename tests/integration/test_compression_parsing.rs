use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Test parsing the real CompressionInfo.db file
    let compression_data = fs::read("test-env/cassandra5/sstables/all_types-46200090673711f0b2cf19d64e7cbecb/nb-1-big-CompressionInfo.db")?;
    
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
        
        // Read uncompressed size (4 bytes big-endian)
        let uncompressed_size = u32::from_be_bytes([
            compression_data[offset], 
            compression_data[offset + 1], 
            compression_data[offset + 2], 
            compression_data[offset + 3]
        ]);
        offset += 4;
        
        println!("Chunk {}: offset={}, compressed={}, uncompressed={}, ratio={:.2}%", 
                 i, chunk_offset, compressed_size, uncompressed_size,
                 (compressed_size as f64 / uncompressed_size as f64) * 100.0);
    }
    
    Ok(())
}