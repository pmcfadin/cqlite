// Debug script to understand Cassandra VInt format
fn main() {
    println!("Analyzing Cassandra VInt format...");
    
    // Based on the test case, Cassandra VInt format seems to be:
    // - NOT the same as our current implementation
    // - Value 0 -> [0x80] (not [0x00])
    // - Value 1 -> [0x81] (not [0x02])
    
    // Let's analyze the pattern from test cases:
    let test_cases = vec![
        (0, vec![0x80]),     // 0 -> 0b10000000
        (1, vec![0x81]),     // 1 -> 0b10000001
        (-1, vec![0xFF]),    // -1 -> 0b11111111
        (63, vec![0xBF]),    // 63 -> 0b10111111
        (-64, vec![0xC0]),   // -64 -> 0b11000000
        (127, vec![0xC0, 0x7F]),  // 127 -> 0b11000000 0b01111111
        (-128, vec![0xC0, 0x80]), // -128 -> 0b11000000 0b10000000
    ];
    
    println!("Test cases analysis:");
    for (value, bytes) in &test_cases {
        let first_byte: u8 = bytes[0];
        let high_bits = first_byte >> 6;
        let leading_ones = first_byte.leading_ones();
        
        println!("Value {}: {:?}", value, bytes);
        println!("  First byte: 0x{:02X} (0b{:08b})", first_byte, first_byte);
        println!("  High 2 bits: 0b{:02b}", high_bits);
        println!("  Leading 1s: {}", leading_ones);
        println!("  Length: {} bytes", bytes.len());
        println!();
    }
    
    // Analysis suggests:
    // - Single byte values: 1xxxxxxx (high bit = 1, NOT 0)
    // - Two byte values: 11xxxxxx xxxxxxxx (two high bits = 11)
    // This is different from our current implementation which uses:
    // - Single byte: 0xxxxxxx (high bit = 0)
    // - Two byte: 10xxxxxx xxxxxxxx (high bits = 10)
    
    println!("HYPOTHESIS: Cassandra VInt format:");
    println!("- Single byte: 1xxxxxxx (NOT 0xxxxxxx)");
    println!("- Two bytes: 11xxxxxx xxxxxxxx");
    println!("- Three bytes: 111xxxxx xxxxxxxx xxxxxxxx");
    println!("- etc.");
}
