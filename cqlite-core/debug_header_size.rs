fn main() {
    let mut header = Vec::new();
    
    // Cassandra magic bytes (4 bytes)
    let magic = [0x5A, 0x5A, 0x5A, 0x5A];
    header.extend_from_slice(&magic);
    println!("After magic: {} bytes", header.len());
    
    // Cassandra format version ('oa' = 2 bytes)  
    let format_version = b"oa";
    header.extend_from_slice(format_version);
    println!("After format version: {} bytes", header.len());
    
    // Flags (4 bytes, big-endian)
    let flags = 0u32;
    header.extend_from_slice(&flags.to_be_bytes());
    println!("After flags: {} bytes", header.len());
    
    // Partition count (8 bytes, big-endian)
    header.extend_from_slice(&0u64.to_be_bytes());
    println!("After partition count: {} bytes", header.len());
    
    // Timestamp range (16 bytes, big-endian) - should be 2 x u64 = 16 bytes
    let created_at = 1234567890u64;
    header.extend_from_slice(&created_at.to_be_bytes()); // 8 bytes
    header.extend_from_slice(&created_at.to_be_bytes()); // 8 bytes  
    println!("After timestamp range: {} bytes", header.len());
    
    // Reserved bytes (should be enough to reach 32)
    let needed = 32 - header.len();
    println!("Need {} more bytes to reach 32", needed);
    header.extend_from_slice(&vec![0u8; needed]);
    println!("Final size: {} bytes", header.len());
}
