use cqlite_core::parser::vint::*;

fn main() {
    println!("🔍 Debugging VInt Encoding Issues");
    
    // Test the specific failing values
    let test_values = vec![
        256,    // Failing in Cassandra compatibility test  
        1048576, // Failing in comprehensive test
        64,      // Two-byte encoding issue
        127,     // Boundary test
        -1,      // Negative test
    ];
    
    for value in test_values {
        println!("\n--- Testing value: {} ---", value);
        
        let encoded = encode_vint(value);
        println!("Encoded bytes: {:?}", encoded);
        println!("Encoded hex: {:02X?}", encoded);
        println!("Length: {}", encoded.len());
        
        // Analyze bit patterns
        if !encoded.is_empty() {
            let first_byte = encoded[0];
            let leading_ones = first_byte.leading_ones();
            let bit_pattern = format!("{:08b}", first_byte);
            
            println!("First byte: 0x{:02X} ({})", first_byte, bit_pattern);
            println!("Leading ones: {}", leading_ones);
            
            // Expected leading ones for multi-byte should be (length - 1)
            if encoded.len() > 1 {
                println!("Expected leading ones: {}", encoded.len() - 1);
                if leading_ones as usize != (encoded.len() - 1) {
                    println!("❌ MISMATCH: Expected {} leading ones, got {}", 
                           encoded.len() - 1, leading_ones);
                }
            }
        }
        
        // Test decoding
        match parse_vint(&encoded) {
            Ok((remaining, decoded)) => {
                println!("✅ Decoded: {} (remaining: {} bytes)", decoded, remaining.len());
                if decoded != value {
                    println!("❌ Roundtrip FAILED: {} != {}", value, decoded);
                }
            }
            Err(e) => {
                println!("❌ Decode FAILED: {:?}", e);
            }
        }
    }
    
    // Test specific Cassandra expected bytes
    println!("\n--- Testing Cassandra Expected Bytes ---");
    let cassandra_tests = vec![
        (256, vec![0xE0, 0x01, 0x00]), // This is what [224, 1, 0] should decode to
        (65536, vec![0xF0, 0x01, 0x00, 0x00]),
        (127, vec![0xC0, 0x7F]),
    ];
    
    for (expected_value, bytes) in cassandra_tests {
        println!("\nTesting bytes {:?} -> should be {}", bytes, expected_value);
        match parse_vint(&bytes) {
            Ok((_, decoded)) => {
                println!("✅ Decoded: {}", decoded);
                if decoded != expected_value {
                    println!("❌ MISMATCH: Expected {}, got {}", expected_value, decoded);
                }
            }
            Err(e) => {
                println!("❌ Parse FAILED: {:?}", e);
            }
        }
    }
    
    println!("\n🔬 Bit Analysis Complete");
}