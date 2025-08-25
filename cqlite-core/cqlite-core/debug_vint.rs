use crate::parser::vint::{encode_vint, parse_vint};

#[test]
fn debug_vint_128() {
    let value = 128i64;
    let encoded = encode_vint(value);
    println!("Value {} encoded to {:?} (len={})", value, encoded, encoded.len());
    for (i, &byte) in encoded.iter().enumerate() {
        println!("  encoded[{}] = 0x{:02X} ({})", i, byte, byte);
    }
    
    let (remaining, decoded) = parse_vint(&encoded).unwrap();
    println!("Decoded: {} (remaining: {} bytes)", decoded, remaining.len());
}
