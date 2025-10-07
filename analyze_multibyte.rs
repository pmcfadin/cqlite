fn main() {
    println!("Analyzing multi-byte cases:");

    let cases = vec![(127, vec![0xC0, 0x7F]), (-128, vec![0xC0, 0x80])];

    for (expected_value, bytes) in cases {
        println!(
            "\nExpected: {}, Bytes: {:02X} {:02X}",
            expected_value, bytes[0], bytes[1]
        );

        let first = bytes[0];
        let second = bytes[1];

        // Method 1: Extract 5 bits from first + 8 from second
        let high_5 = (first & 0x1F) as u16; // 5 bits
        let low_8 = second as u16; // 8 bits
        let unsigned_13 = (high_5 << 8) | low_8;
        println!("Method 1 (5+8 bits): {}", unsigned_13);

        // Method 2: Treat second byte as signed
        let signed_second = second as i8 as i64;
        println!("Method 2 (signed second): {}", signed_second);

        // Method 3: Different bit interpretation
        if expected_value == -128 && second == 0x80 {
            println!("Method 3: 0x80 = -128 in 8-bit two's complement");
        }

        // Method 4: Try different sign extension
        let sign_extended = if unsigned_13 >= 0x800 {
            // Sign bit for 12 bits?
            (unsigned_13 as i64) - 0x1000
        } else {
            unsigned_13 as i64
        };
        println!("Method 4 (12-bit sign extend): {}", sign_extended);

        // Method 5: Maybe the format is different
        // First byte 0xC0 indicates "two-byte format"; second is signed value
        let direct_signed = second as i8 as i64;
        println!("Method 5 (direct signed): {}", direct_signed);

        println!("Expected value: {}", expected_value);
    }
}