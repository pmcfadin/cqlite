//! Byte-comparable key decoding for Cassandra 5.0+ (CEP-25)
//!
//! This module implements decoding of byte-comparable encoded keys used in
//! Cassandra 5.0's 'newbig' format. Byte-comparable encoding preserves the
//! sort order of typed values when compared lexicographically as bytes.
//!
//! ## Format Overview
//!
//! Keys are encoded as a sequence of components separated by markers:
//! - `0x40` (NEXT_COMPONENT): Separates components
//! - `0x38` (TERMINATOR): Marks end of key
//! - `0x3E` (NULL_MARKER): Represents null component
//! - `0x00 0xFE`: Escape sequence for literal zero byte
//! - `0x00 0xFF`: End-of-variable-length-data marker
//!
//! ## Type-Specific Encodings
//!
//! - **Fixed signed integers** (int, bigint): Sign bit flipped for ordering
//! - **Text/Blob**: Escape sequences with `0x00 0xFF` terminator
//! - **UUID**: Direct 16-byte encoding
//!
//! ## References
//!
//! - CEP-25: https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-25
//! - ByteComparable.md: https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/utils/bytecomparable/ByteComparable.md

use crate::error::{Error, Result};
use nom::IResult;

/// Byte-comparable format constants (from CEP-25)
pub const NEXT_COMPONENT: u8 = 0x40;
pub const TERMINATOR: u8 = 0x38;
pub const NULL_MARKER: u8 = 0x3E;
pub const ESCAPE_00: u8 = 0x00;
pub const ESCAPE_FE: u8 = 0xFE; // Literal zero
pub const ESCAPE_FF: u8 = 0xFF; // End of variable-length data

/// Decode a byte-comparable encoded composite key
///
/// Returns a vector of component byte arrays, one for each key component.
/// Each component is already decoded (unescaped, with markers removed).
///
/// # Arguments
/// * `input` - Raw bytes containing byte-comparable encoded key
///
/// # Returns
/// * `Ok((remaining, components))` - Remaining data and decoded components
/// * `Err(_)` - Parse error with corruption details
///
/// # Format
/// ```text
/// [component_1_data] 0x40 [component_2_data] 0x40 ... 0x38
/// ```
pub fn decode_byte_comparable_key(input: &[u8]) -> IResult<&[u8], Vec<Vec<u8>>> {
    let mut components = Vec::new();
    let mut offset = 0;

    loop {
        if offset >= input.len() {
            // Reached end without terminator - return what we have
            break;
        }

        // Check for terminator first
        if input[offset] == TERMINATOR {
            offset += 1;
            break;
        }

        // Extract next component
        match extract_component(&input[offset..]) {
            Ok((component_data, consumed)) => {
                components.push(component_data);
                offset += consumed;

                // Check what comes after component
                if offset < input.len() {
                    match input[offset] {
                        NEXT_COMPONENT => {
                            // More components follow
                            offset += 1;
                        }
                        TERMINATOR => {
                            // End of key
                            offset += 1;
                            break;
                        }
                        _ => {
                            // Unexpected byte - may be start of next component
                            // Some formats may not have explicit separators
                            continue;
                        }
                    }
                }
            }
            Err(_e) => {
                return Err(nom::Err::Error(nom::error::Error::new(
                    &input[offset..],
                    nom::error::ErrorKind::Fail,
                )));
            }
        }
    }

    Ok((&input[offset..], components))
}

/// Extract a single component up to separator (0x40) or terminator (0x38)
///
/// Handles escape sequences for variable-length data.
///
/// # Returns
/// * `Ok((data, consumed))` - Decoded component data and bytes consumed
/// * `Err(_)` - Parse error
fn extract_component(input: &[u8]) -> Result<(Vec<u8>, usize)> {
    let mut data = Vec::new();
    let mut i = 0;

    while i < input.len() {
        match input[i] {
            NEXT_COMPONENT => {
                // Component separator - end of component
                return Ok((data, i));
            }
            TERMINATOR => {
                // End of key - end of component
                return Ok((data, i));
            }
            NULL_MARKER => {
                // Null component
                return Ok((vec![], i + 1));
            }
            ESCAPE_00 => {
                // Escape sequence
                if i + 1 >= input.len() {
                    return Err(Error::corruption("Incomplete escape sequence"));
                }
                match input[i + 1] {
                    ESCAPE_FE => {
                        // Literal 0x00
                        data.push(0x00);
                        i += 2;
                    }
                    ESCAPE_FF => {
                        // End of variable-length data (acts as component terminator)
                        return Ok((data, i + 2));
                    }
                    _ => {
                        // Unknown escape - treat as regular byte
                        data.push(input[i]);
                        i += 1;
                    }
                }
            }
            byte => {
                data.push(byte);
                i += 1;
            }
        }
    }

    // Reached end of input - return what we collected
    Ok((data, i))
}

/// Decode a signed 32-bit integer component
///
/// Fixed-size encoding (4 bytes) with sign bit flipped for ordering.
/// Negative numbers have high bit = 0, positive have high bit = 1.
///
/// **NOTE**: This function is used for schema-aware key decoding when type
/// information is available from the schema. In schemaless parsing, keys are
/// returned as raw bytes. The `#[allow(dead_code)]` annotation is present
/// because this function is called conditionally based on schema availability.
///
/// # Arguments
/// * `data` - 4-byte component data
///
/// # Returns
/// * `Ok(value)` - Decoded i32 value
/// * `Err(_)` - Invalid length or parse error
#[allow(dead_code)] // Used for type-specific decoding when schema is available
pub fn decode_int32_component(data: &[u8]) -> Result<i32> {
    if data.len() != 4 {
        return Err(Error::corruption(format!(
            "Invalid int32 component length: {} (expected 4)",
            data.len()
        )));
    }

    // Read big-endian and un-flip sign bit
    let encoded = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    let value = (encoded ^ 0x8000_0000) as i32;
    Ok(value)
}

/// Decode a signed 64-bit integer component (bigint or timestamp)
///
/// Fixed-size encoding (8 bytes) with sign bit flipped for ordering.
///
/// **NOTE**: This function is used for schema-aware key decoding when type
/// information is available. The `#[allow(dead_code)]` annotation is present
/// because usage is conditional on schema availability.
///
/// # Arguments
/// * `data` - 8-byte component data
///
/// # Returns
/// * `Ok(value)` - Decoded i64 value
/// * `Err(_)` - Invalid length or parse error
#[allow(dead_code)] // Used for type-specific decoding when schema is available
pub fn decode_bigint_component(data: &[u8]) -> Result<i64> {
    if data.len() != 8 {
        return Err(Error::corruption(format!(
            "Invalid bigint component length: {} (expected 8)",
            data.len()
        )));
    }

    // Read big-endian and un-flip sign bit
    let encoded = u64::from_be_bytes([
        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
    ]);
    let value = (encoded ^ 0x8000_0000_0000_0000) as i64;
    Ok(value)
}

/// Decode a text component with escape sequences
///
/// Variable-length text with `0x00 0xFF` terminator and `0x00 0xFE` escaping.
/// This function validates UTF-8 encoding.
///
/// **NOTE**: This function is used for schema-aware key decoding when type
/// information is available. The `#[allow(dead_code)]` annotation is present
/// because usage is conditional on schema availability.
///
/// # Arguments
/// * `data` - Already-decoded component bytes (escape sequences removed)
///
/// # Returns
/// * `Ok(text)` - Decoded UTF-8 string
/// * `Err(_)` - Invalid UTF-8
#[allow(dead_code)] // Used for type-specific decoding when schema is available
pub fn decode_text_component(data: &[u8]) -> Result<String> {
    String::from_utf8(data.to_vec())
        .map_err(|e| Error::corruption(format!("Invalid UTF-8 in text component: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_simple_key() {
        // Example: "AMZN" (4 bytes) + separator + date bytes + terminator
        let input = &[
            0x41, 0x4D, 0x5A, 0x4E, // "AMZN"
            0x40, // NEXT_COMPONENT
            0x80, 0x00, 0x00, 0x00, 0x00, 0x4F, 0x88, 0x00, // Encoded date (8 bytes)
            0x38, // TERMINATOR
        ];

        let (remaining, components) = decode_byte_comparable_key(input).unwrap();
        assert_eq!(components.len(), 2);
        assert_eq!(components[0], b"AMZN");
        assert_eq!(
            components[1],
            &[0x80, 0x00, 0x00, 0x00, 0x00, 0x4F, 0x88, 0x00]
        );
        assert_eq!(remaining.len(), 0);
    }

    #[test]
    fn test_decode_with_escapes() {
        // Variable-length data with literal zero: "A\x00B" + separator
        let input = &[
            0x41, // 'A'
            0x00, 0xFE, // Escaped zero
            0x42, // 'B'
            0x00, 0xFF, // End of variable-length
        ];

        let (_remaining, components) = decode_byte_comparable_key(input).unwrap();
        assert_eq!(components.len(), 1);
        assert_eq!(components[0], b"A\x00B");
    }

    #[test]
    fn test_decode_null_component() {
        // Key with null component: component1 + separator + null + terminator
        let input = &[
            0x41, 0x42, 0x43, // "ABC"
            0x40, // NEXT_COMPONENT
            0x3E, // NULL_MARKER (this also consumes itself, so next byte is terminator)
            0x38, // TERMINATOR
        ];

        let (remaining, components) = decode_byte_comparable_key(input).unwrap();
        assert_eq!(components.len(), 2);
        assert_eq!(components[0], b"ABC");
        assert_eq!(components[1], b""); // Null represented as empty vec
        assert_eq!(remaining.len(), 0);
    }

    #[test]
    fn test_decode_int32() {
        // Positive number: +1234 = 0x000004D2
        // Encoded: 0x000004D2 ^ 0x80000000 = 0x800004D2
        let encoded = &[0x80, 0x00, 0x04, 0xD2];
        let value = decode_int32_component(encoded).unwrap();
        assert_eq!(value, 1234);

        // Negative number: -1234 = 0xFFFFFB2E
        // Encoded: 0xFFFFFB2E ^ 0x80000000 = 0x7FFFFB2E
        let encoded = &[0x7F, 0xFF, 0xFB, 0x2E];
        let value = decode_int32_component(encoded).unwrap();
        assert_eq!(value, -1234);
    }

    #[test]
    fn test_decode_bigint() {
        // Positive: +123456789 = 0x00000000075BCD15
        // Encoded: 0x00000000075BCD15 ^ 0x8000000000000000 = 0x8000000007BDCD15
        let encoded = &[0x80, 0x00, 0x00, 0x00, 0x07, 0x5B, 0xCD, 0x15];
        let value = decode_bigint_component(encoded).unwrap();
        assert_eq!(value, 123456789);
    }

    #[test]
    fn test_decode_text() {
        let data = b"Hello, World!";
        let text = decode_text_component(data).unwrap();
        assert_eq!(text, "Hello, World!");
    }

    #[test]
    fn test_decode_text_invalid_utf8() {
        let data = &[0xFF, 0xFE, 0xFD]; // Invalid UTF-8
        let result = decode_text_component(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_component_with_separator() {
        let input = &[0x41, 0x42, 0x43, 0x40]; // "ABC" + NEXT_COMPONENT
        let (data, consumed) = extract_component(input).unwrap();
        assert_eq!(data, b"ABC");
        assert_eq!(consumed, 3); // Should NOT include the separator
    }

    #[test]
    fn test_extract_component_with_terminator() {
        let input = &[0x41, 0x42, 0x43, 0x38]; // "ABC" + TERMINATOR
        let (data, consumed) = extract_component(input).unwrap();
        assert_eq!(data, b"ABC");
        assert_eq!(consumed, 3); // Should NOT include the terminator
    }

    #[test]
    fn test_multiple_components() {
        // Three components: "A" + "B" + "C"
        let input = &[
            0x41, 0x40, // "A" + NEXT_COMPONENT
            0x42, 0x40, // "B" + NEXT_COMPONENT
            0x43, 0x38, // "C" + TERMINATOR
        ];

        let (remaining, components) = decode_byte_comparable_key(input).unwrap();
        assert_eq!(components.len(), 3);
        assert_eq!(components[0], b"A");
        assert_eq!(components[1], b"B");
        assert_eq!(components[2], b"C");
        assert_eq!(remaining.len(), 0);
    }

    #[test]
    fn test_incomplete_escape_sequence() {
        // Escape start (0x00) but buffer ends before escape code
        let input = &[0x41, 0x00]; // 'A' + escape start, but truncated
        let result = extract_component(input);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Incomplete escape sequence"));
    }

    #[test]
    fn test_empty_key() {
        // Empty input should return empty components
        let input = &[];
        let (remaining, components) = decode_byte_comparable_key(input).unwrap();
        assert_eq!(components.len(), 0);
        assert_eq!(remaining.len(), 0);
    }

    #[test]
    fn test_key_without_terminator() {
        // Key data without 0x38 terminator (EOF ends key)
        let input = &[0x41, 0x42, 0x43]; // "ABC" without terminator
        let (remaining, components) = decode_byte_comparable_key(input).unwrap();
        assert_eq!(components.len(), 1);
        assert_eq!(components[0], b"ABC");
        assert_eq!(remaining.len(), 0);
    }

    #[test]
    fn test_int32_invalid_length() {
        // int32 requires exactly 4 bytes
        let data = &[0x80, 0x00, 0x04]; // Only 3 bytes
        let result = decode_int32_component(data);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid int32 component length: 3"));
    }

    #[test]
    fn test_bigint_invalid_length() {
        // bigint requires exactly 8 bytes
        let data = &[0x80, 0x00, 0x00, 0x00, 0x07, 0x5B]; // Only 6 bytes
        let result = decode_bigint_component(data);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid bigint component length: 6"));
    }
}
