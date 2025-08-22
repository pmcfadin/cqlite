// Simple test to debug VInt encoding
fn main() {
    // Test the VInt parsing that our test is using
    let test_data = vec![0x40, 0x01, 0x0C];
    
    println!("Test data: {:?}", test_data);
    
    // Try to parse at offset 1 (after the 0x40 flag)
    let data_at_offset_1 = &test_data[1..];
    println!("Data at offset 1: {:?}", data_at_offset_1);
    
    // Check what parse_vint_length would do with 0x01
    let first_byte = 0x01u8;
    let leading_ones = first_byte.leading_ones();
    println!("First byte: 0x{:02x}, leading ones: {}", first_byte, leading_ones);
    
    if leading_ones == 0 {
        let value = first_byte & 0x7F;
        println!("Single byte VInt value: {}", value);
    }
}