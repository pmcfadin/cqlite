//! Standalone VInt test to verify implementation works correctly

#[allow(dead_code)]
use super::vint::*;

#[test]
fn test_vint_basic_functionality() {
    // Test basic encoding/decoding
    let test_values = vec![0, 1, -1, 42, -42, 127, -128];
    
    for value in test_values {
        let encoded = encode_vint(value);
        let (remaining, decoded) = parse_vint(&encoded).unwrap();
        assert!(remaining.is_empty(), "Should consume all bytes");
        assert_eq!(decoded, value, "Roundtrip failed for {}", value);
        println!("✓ Value {} roundtrip: {} bytes", value, encoded.len());
    }
}

#[test]
fn test_vint_edge_cases() {
    // Test boundary conditions
    let boundaries = vec![63, 64, 127, 128, 255, 256, 16383, 16384];
    
    for value in boundaries {
        let pos_encoded = encode_vint(value);
        let neg_encoded = encode_vint(-value);
        
        let (_, pos_decoded) = parse_vint(&pos_encoded).unwrap();
        let (_, neg_decoded) = parse_vint(&neg_encoded).unwrap();
        
        assert_eq!(pos_decoded, value);
        assert_eq!(neg_decoded, -value);
        println!("✓ Boundary {} (+/-): {} / {} bytes", value, pos_encoded.len(), neg_encoded.len());
    }
}

#[test]
fn test_vint_format_structure() {
    // Test specific bit patterns
    
    // Single byte values (0xxxxxxx pattern)
    let encoded_0 = encode_vint(0);
    assert_eq!(encoded_0, vec![0x00]);
    assert_eq!(encoded_0.len(), 1);
    assert_eq!(encoded_0[0] & 0x80, 0); // MSB should be 0
    
    let encoded_1 = encode_vint(1);
    assert_eq!(encoded_1.len(), 1);
    assert_eq!(encoded_1[0] & 0x80, 0); // MSB should be 0
    
    // Multi-byte values should have MSB pattern 10xxxxxx, 110xxxxx, etc.
    let encoded_large = encode_vint(1000);
    assert!(encoded_large.len() >= 2);
    assert_eq!(encoded_large[0] & 0x80, 0x80); // Should have leading 1
    
    println!("✓ Format structure tests passed");
}

#[test]
fn test_parse_errors() {
    // Test error conditions
    assert!(parse_vint(&[]).is_err()); // Empty input
    assert!(parse_vint(&[0x80]).is_err()); // Incomplete multi-byte
    assert!(parse_vint(&[0xC0]).is_err()); // Missing second byte
    
    println!("✓ Error handling tests passed");
}

#[test]
fn run_all_vint_tests() {
    test_vint_basic_functionality();
    test_vint_edge_cases();
    test_vint_format_structure();
    test_parse_errors();
    println!("🎉 All VInt tests passed!");
}

fn main() {
    run_all_vint_tests();
}