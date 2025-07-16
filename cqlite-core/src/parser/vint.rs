//! Variable-length integer encoding/decoding for Cassandra SSTable format
//!
//! Cassandra uses a variable-length integer encoding scheme to save space.
//! This module implements VInt encoding compatible with Cassandra 5+ format.

use nom::{bytes::complete::take, IResult};

/// Maximum bytes a VInt can occupy (for safety)
pub const MAX_VINT_SIZE: usize = 9;

/// Decode a variable-length signed integer from bytes
///
/// VInt encoding in Cassandra:
/// - First byte contains length information in the most significant bits
/// - Remaining bits and subsequent bytes contain the value
/// - Supports both positive and negative numbers using two's complement
///
/// # Arguments
///
/// * `input` - Input byte slice
///
/// # Returns
///
/// Tuple of (remaining_bytes, decoded_value)
pub fn parse_vint(input: &[u8]) -> IResult<&[u8], i64> {
    if input.is_empty() {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Eof,
        )));
    }

    let first_byte = input[0];

    // Determine the number of bytes to read based on leading zeros
    let leading_zeros = first_byte.leading_zeros() as usize;
    let length = if leading_zeros >= 8 {
        1
    } else {
        leading_zeros + 1
    };

    if length > MAX_VINT_SIZE {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::TooLarge,
        )));
    }

    if input.len() < length {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Eof,
        )));
    }

    let (input, bytes) = take(length)(input)?;

    let mut value: i64;

    if length == 1 {
        // Single byte case - value is in lower 7 bits
        value = (first_byte & 0x7F) as i64;
        if first_byte & 0x80 != 0 {
            value |= !0x7F; // Sign extend for negative numbers
        }
    } else {
        // Multi-byte case
        let mask = (1u8 << (8 - leading_zeros)) - 1;
        value = (first_byte & mask) as i64;

        for &byte in &bytes[1..] {
            value = (value << 8) | (byte as i64);
        }

        // Check if this should be a negative number
        let sign_bit_pos = (length * 8) - leading_zeros - 1;
        if sign_bit_pos < 64 && (value >> sign_bit_pos) & 1 != 0 {
            let sign_extend_mask = !((1i64 << (sign_bit_pos + 1)) - 1);
            value |= sign_extend_mask;
        }
    }

    Ok((input, value))
}

/// Encode a signed integer as a variable-length integer
///
/// # Arguments
///
/// * `value` - The integer value to encode
///
/// # Returns
///
/// Vector of bytes representing the VInt-encoded value
pub fn encode_vint(value: i64) -> Vec<u8> {
    if value >= -64 && value < 64 {
        // Single byte encoding for small values
        return vec![(value as u8) & 0xFF];
    }

    // Determine minimum number of bytes needed
    let mut bytes_needed = 1;
    let mut test_value = if value < 0 { !value } else { value };

    while test_value > 0 {
        test_value >>= 8;
        bytes_needed += 1;
        if bytes_needed > MAX_VINT_SIZE {
            break;
        }
    }

    // Cap at maximum safe size
    bytes_needed = bytes_needed.min(MAX_VINT_SIZE);

    let mut result = Vec::with_capacity(bytes_needed);

    // Encode length in the first byte's leading bits
    let leading_zeros = bytes_needed - 1;
    let length_mask = if leading_zeros == 0 {
        0x80u8
    } else {
        0xFFu8 << (8 - leading_zeros)
    };

    // Extract bytes from the value
    let mut bytes = Vec::new();
    let mut temp_value = value as u64;
    for _ in 0..bytes_needed {
        bytes.push((temp_value & 0xFF) as u8);
        temp_value >>= 8;
    }
    bytes.reverse();

    // Combine length encoding with value
    bytes[0] = (bytes[0] & !length_mask) | length_mask;

    result.extend_from_slice(&bytes);
    result
}

/// Decode a variable-length unsigned integer from bytes
///
/// Similar to VInt but treats the value as unsigned
pub fn parse_vuint(input: &[u8]) -> IResult<&[u8], u64> {
    let (remaining, signed_value) = parse_vint(input)?;
    Ok((remaining, signed_value as u64))
}

/// Encode an unsigned integer as a variable-length integer
pub fn encode_vuint(value: u64) -> Vec<u8> {
    encode_vint(value as i64)
}

/// Parse a VInt and convert to usize for length fields
pub fn parse_vint_length(input: &[u8]) -> IResult<&[u8], usize> {
    let (remaining, value) = parse_vint(input)?;
    if value < 0 {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }
    Ok((remaining, value as usize))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vint_single_byte() {
        // Test small positive values
        assert_eq!(parse_vint(&[0x05]).unwrap().1, 5);
        assert_eq!(parse_vint(&[0x3F]).unwrap().1, 63);

        // Test small negative values (sign bit set)
        assert_eq!(parse_vint(&[0x80]).unwrap().1, -128);
        assert_eq!(parse_vint(&[0xFF]).unwrap().1, -1);
    }

    #[test]
    fn test_vint_multi_byte() {
        // Test two-byte encoding
        let bytes = vec![0xC0, 0x80]; // Should decode to 128
        assert_eq!(parse_vint(&bytes).unwrap().1, 128);

        // Test larger values
        let bytes = vec![0xE0, 0x01, 0x00]; // 256 in 3-byte encoding
        assert_eq!(parse_vint(&bytes).unwrap().1, 256);
    }

    #[test]
    fn test_vint_roundtrip() {
        let test_values = vec![0, 1, -1, 63, -64, 127, -128, 255, -256, 1000, -1000];

        for value in test_values {
            let encoded = encode_vint(value);
            let (_, decoded) = parse_vint(&encoded).unwrap();
            assert_eq!(decoded, value, "Failed roundtrip for value {}", value);
        }
    }

    #[test]
    fn test_vuint_positive() {
        let value = 1000u64;
        let encoded = encode_vuint(value);
        let (_, decoded) = parse_vuint(&encoded).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn test_vint_length() {
        let bytes = encode_vint(42);
        let (_, length) = parse_vint_length(&bytes).unwrap();
        assert_eq!(length, 42);
    }

    #[test]
    fn test_vint_errors() {
        // Test empty input
        assert!(parse_vint(&[]).is_err());

        // Test negative length
        let negative_bytes = encode_vint(-10);
        assert!(parse_vint_length(&negative_bytes).is_err());
    }
}
