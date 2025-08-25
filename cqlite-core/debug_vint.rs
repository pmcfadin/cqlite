use crate::parser::vint::{encode_vint, parse_vint};

fn main() {
    println!("encode_vint(128) = {:?}", encode_vint(128));
    println!("encode_vint(5) = {:?}", encode_vint(5));
    
    // Test parse_vint on [0x81, 0x00]
    let (_, value) = parse_vint(&[0x81, 0x00]).unwrap();
    println!("parse_vint([0x81, 0x00]) = {}", value);
    
    // Test parse_vint on [0x05]
    let (_, value) = parse_vint(&[0x05]).unwrap();
    println!("parse_vint([0x05]) = {}", value);
}
