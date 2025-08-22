use crate::parser::vint::parse_vint_length;

fn main() {
    // Test the values used in the test
    let test_bytes = [0x04]; // Column count: 2
    match parse_vint_length(&test_bytes) {
        Ok((_, value)) => println!("0x04 decodes to: {}", value),
        Err(e) => println!("Error decoding 0x04: {:?}", e),
    }
    
    let test_bytes = [0x10]; // name length: 8
    match parse_vint_length(&test_bytes) {
        Ok((_, value)) => println!("0x10 decodes to: {}", value),
        Err(e) => println!("Error decoding 0x10: {:?}", e),
    }
    
    let test_bytes = [0x0C]; // value length: 6
    match parse_vint_length(&test_bytes) {
        Ok((_, value)) => println!("0x0C decodes to: {}", value),
        Err(e) => println!("Error decoding 0x0C: {:?}", e),
    }
}
