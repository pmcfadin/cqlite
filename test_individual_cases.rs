fn main() {
    // Test individual cases to understand the pattern
    let test_cases = vec![
        (0, vec![0x80]),
        (1, vec![0x81]),
        (-1, vec![0xFF]),
        (63, vec![0xBF]),
        (-64, vec![0xC0]),
        (127, vec![0xC0, 0x7F]),
        (-128, vec![0xC0, 0x80]),
    ];

    println!("Analyzing individual test cases:");
    
    for (value, expected_bytes) in test_cases {
        println!("\nValue: {}", value);
        println!("Expected bytes: {:?}", expected_bytes);
        
        if expected_bytes.len() == 1 {
            let byte = expected_bytes[0];
            println!("  Single byte: 0x{:02X} (0b{:08b})", byte, byte);
            
            if byte >= 0x80 && byte <= 0xBF {
                // Range 0x80-0xBF: values 0 to 63
                let decoded = byte & 0x3F;
                println!("  Extracted value from 0x80-0xBF range: {}", decoded);
                println!("  Matches expected: {}", decoded as i64 == value);
            } else if byte >= 0xC0 && byte <= 0xFF {
                // Range 0xC0-0xFF: maybe negative values?
                if byte == 0xFF {
                    println!("  Special case 0xFF = -1");
                    println!("  Matches expected: {}", value == -1);
                } else {
                    let decoded = (byte as i64) - 0x100; // or some other transformation
                    println!("  Trying as negative: {}", decoded);
                    println!("  Alternative: {}", (byte as i64) - 192); // 0xC0 = 192
                    
                    if value == -64 && byte == 0xC0 {
                        println!("  Maybe 0xC0-0xFF maps to -64 to -1");
                        let neg_value = -64 + (byte - 0xC0) as i64;
                        println!("  Calculated: {}", neg_value);
                        println!("  Matches: {}", neg_value == value);
                    }
                }
            }
        } else {
            println!("  Multi-byte case:");
            let first_byte = expected_bytes[0];
            let second_byte = expected_bytes[1];
            println!("  First: 0x{:02X}, Second: 0x{:02X}", first_byte, second_byte);
            
            // Try different interpretations
            let combined = ((first_byte as u16) << 8) | (second_byte as u16);
            println!("  Combined as u16: 0x{:04X} = {}", combined, combined);
            
            // Try extracting from 0xC0 prefix (5 bits + 8 bits)
            if first_byte >= 0xC0 {
                let high_bits = (first_byte & 0x1F) as i64; // 5 bits
                let low_bits = second_byte as i64; // 8 bits
                let unsigned_val = (high_bits << 8) | low_bits;
                println!("  As 13-bit value: {}", unsigned_val);
                
                // Try sign extension for 13 bits
                let signed_val = if unsigned_val >= 4096 {
                    unsigned_val - 8192
                } else {
                    unsigned_val
                };
                println!("  Sign-extended: {}", signed_val);
                println!("  Matches expected: {}", signed_val == value);
            }
        }
    }
}