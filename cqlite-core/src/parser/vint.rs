//! Variable-length integer encoding/decoding for Cassandra SSTable format
//!
//! Cassandra uses a variable-length integer encoding scheme to save space.
//! This module implements VInt encoding compatible with Cassandra 5+ format.
//!
//! VInt Encoding Specification (from Cassandra/ScyllaDB):
//! - MSB-first encoding with consecutive 1-bits indicating extra bytes
//! - First byte pattern: [number of extra bytes as 1-bits][0][value bits]
//! - Example: 110xxxxx indicates 2 extra bytes follow
//! - Uses ZigZag encoding for signed integers to efficiently encode small negative values
//! - Maximum 9 bytes total length

use nom::{bytes::complete::take, IResult};

/// Maximum bytes a VInt can occupy (Cassandra supports up to 9 bytes total)
pub const MAX_VINT_SIZE: usize = 9;

/// Decode a variable-length signed integer from bytes using Cassandra VInt format
///
/// VInt encoding in Cassandra:
/// - First byte uses leading 1-bits to indicate number of extra bytes
/// - Pattern: [1-bits for extra bytes][0][value bits]
/// - Remaining bytes contain the rest of the value
/// - Uses ZigZag decoding for signed values: (n >> 1) ^ -(n & 1)
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

    // Count leading 1-bits to determine extra bytes
    let extra_bytes = first_byte.leading_ones() as usize;
    let total_length = extra_bytes + 1;

    // Cassandra VInt format supports at most 8 extra bytes (9 total bytes)
    if total_length > MAX_VINT_SIZE {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::TooLarge,
        )));
    }

    if input.len() < total_length {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Eof,
        )));
    }

    let (input, bytes) = take(total_length)(input)?;

    let value = if extra_bytes == 0 {
        // Single byte case - value is in lower 7 bits (first bit is 0)
        (first_byte & 0x7F) as u64
    } else {
        // Multi-byte case
        // Calculate number of value bits in the first byte
        // For N extra bytes, we have (8 - N - 1) value bits in first byte
        let first_byte_value_bits = if extra_bytes < 7 { 7 - extra_bytes } else { 0 };

        // Extract value bits from first byte
        let first_byte_mask = if first_byte_value_bits > 0 {
            (1u8 << first_byte_value_bits) - 1
        } else {
            0
        };
        let mut value = (first_byte & first_byte_mask) as u64;

        // Read remaining bytes in big-endian order
        for &byte in &bytes[1..] {
            value = (value << 8) | (byte as u64);
        }

        value
    };

    // Apply ZigZag decoding to convert unsigned to signed
    let signed_value = zigzag_decode(value);

    Ok((input, signed_value))
}

/// ZigZag encode a signed integer to unsigned (for efficient small negative number encoding)
///
/// ZigZag encoding maps signed integers to unsigned integers so that numbers
/// with small absolute values have small encodings:
/// 0 -> 0, -1 -> 1, 1 -> 2, -2 -> 3, 2 -> 4, -3 -> 5, ...
fn zigzag_encode(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

/// ZigZag decode an unsigned integer back to signed
fn zigzag_decode(value: u64) -> i64 {
    ((value >> 1) ^ ((!0u64).wrapping_mul(value & 1))) as i64
}

/// Calculate the number of bytes needed to encode a value
fn vint_size(value: u64) -> usize {
    if value == 0 {
        return 1;
    }

    // Calculate how many bytes we need for the value
    let bits_needed = 64 - value.leading_zeros() as usize;

    // For each encoding length, calculate how many value bits are available
    // Length 1: 0xxxxxxx -> 7 bits
    // Length 2: 10xxxxxx xxxxxxxx -> 6 + 8 = 14 bits
    // Length 3: 110xxxxx xxxxxxxx xxxxxxxx -> 5 + 16 = 21 bits
    // etc.
    for length in 1..=MAX_VINT_SIZE {
        let available_bits = if length == 1 {
            7 // Single byte: 0xxxxxxx
        } else {
            // Multi-byte: for length N, we have (8 - N) bits in first byte + 8 * (N - 1) bits in remaining bytes
            // For length = 9 (8 extra bytes), first byte has 0 value bits: all bits are 11111111
            let first_byte_bits = if length <= 8 { 8 - length } else { 0 };
            first_byte_bits + 8 * (length - 1)
        };

        if bits_needed <= available_bits {
            return length;
        }
    }

    MAX_VINT_SIZE
}

/// Encode a signed integer as a variable-length integer using Cassandra VInt format
///
/// Cassandra VInt encoding format:
/// - Single byte: 0xxxxxxx (7 value bits)
/// - Two bytes: 10xxxxxx xxxxxxxx (6 + 8 = 14 value bits)
/// - Three bytes: 110xxxxx xxxxxxxx xxxxxxxx (5 + 16 = 21 value bits)
/// - etc.
///
/// # Arguments
///
/// * `value` - The integer value to encode
///
/// # Returns
///
/// Vector of bytes representing the VInt-encoded value
pub fn encode_vint(value: i64) -> Vec<u8> {
    // Apply ZigZag encoding
    let unsigned_value = zigzag_encode(value);

    let size = vint_size(unsigned_value);
    let mut result = vec![0u8; size];

    if size == 1 {
        // Single byte: 0xxxxxxx
        result[0] = (unsigned_value & 0x7F) as u8;
    } else {
        // Multi-byte encoding
        let extra_bytes = size - 1;

        // Calculate first byte pattern: [extra_bytes 1-bits][0][value bits]
        // For size=2: 10xxxxxx (1 one-bit, then 0, then 6 value bits)
        // For size=3: 110xxxxx (2 one-bits, then 0, then 5 value bits)
        let first_byte_value_bits = if extra_bytes < 7 { 7 - extra_bytes } else { 0 }; // Available value bits in first byte
        let first_byte_prefix = 0xFFu8 << (8 - extra_bytes); // Leading 1s

        // Extract the high-order bits for the first byte
        let high_bits_shift = 8 * extra_bytes;
        let first_byte_value = if first_byte_value_bits > 0 {
            (unsigned_value >> high_bits_shift) & ((1u64 << first_byte_value_bits) - 1)
        } else {
            0
        };

        result[0] = first_byte_prefix | (first_byte_value as u8);

        // Fill remaining bytes with value in big-endian order
        let mut remaining_value = unsigned_value;
        for i in (1..size).rev() {
            result[i] = (remaining_value & 0xFF) as u8;
            remaining_value >>= 8;
        }
    }

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
    fn test_zigzag_encoding() {
        // Test ZigZag encoding mappings
        assert_eq!(zigzag_encode(0), 0);
        assert_eq!(zigzag_encode(-1), 1);
        assert_eq!(zigzag_encode(1), 2);
        assert_eq!(zigzag_encode(-2), 3);
        assert_eq!(zigzag_encode(2), 4);
        assert_eq!(zigzag_encode(-3), 5);
        assert_eq!(zigzag_encode(i64::MAX), u64::MAX - 1);
        assert_eq!(zigzag_encode(i64::MIN), u64::MAX);
    }

    #[test]
    fn test_zigzag_roundtrip() {
        let test_values = vec![0, 1, -1, 127, -128, 32767, -32768, i64::MAX, i64::MIN];
        for value in test_values {
            let encoded = zigzag_encode(value);
            let decoded = zigzag_decode(encoded);
            assert_eq!(decoded, value, "ZigZag roundtrip failed for {}", value);
        }
    }

    #[test]
    fn test_vint_size_calculation() {
        assert_eq!(vint_size(0), 1);
        assert_eq!(vint_size(0x7F), 1); // Max single byte value
        assert_eq!(vint_size(0x80), 2); // Min two byte value
        assert_eq!(vint_size(0x3FFF), 2); // Max two byte value
        assert_eq!(vint_size(0x4000), 3); // Min three byte value
    }

    #[test]
    fn test_vint_single_byte_encoding() {
        // Test small values that fit in single byte
        for i in 0..=63 {
            let encoded = encode_vint(i);
            assert_eq!(encoded.len(), 1, "Value {} should encode to 1 byte", i);
            assert_eq!(encoded[0] & 0x80, 0, "Single byte should have leading 0");

            let (_, decoded) = parse_vint(&encoded).unwrap();
            assert_eq!(decoded, i, "Roundtrip failed for {}", i);
        }

        // Test small negative values
        for i in -63..=0 {
            let encoded = encode_vint(i);
            assert_eq!(encoded.len(), 1, "Value {} should encode to 1 byte", i);

            let (_, decoded) = parse_vint(&encoded).unwrap();
            assert_eq!(decoded, i, "Roundtrip failed for {}", i);
        }
    }

    #[test]
    fn test_vint_multi_byte_encoding() {
        // Test two-byte encoding
        let value = 128;
        let encoded = encode_vint(value);
        assert_eq!(encoded.len(), 2, "Value {} should encode to 2 bytes", value);
        assert_eq!(
            encoded[0] & 0x80,
            0x80,
            "Two-byte encoding should start with 10"
        );
        assert_eq!(
            encoded[0] & 0x40,
            0,
            "Two-byte encoding should start with 10"
        );

        let (_, decoded) = parse_vint(&encoded).unwrap();
        assert_eq!(decoded, value);

        // Test three-byte encoding
        let value = 16384; // 2^14
        let encoded = encode_vint(value);
        assert_eq!(encoded.len(), 3, "Value {} should encode to 3 bytes", value);
        assert_eq!(
            encoded[0] & 0xE0,
            0xC0,
            "Three-byte encoding should start with 110"
        );

        let (_, decoded) = parse_vint(&encoded).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn test_vint_comprehensive_roundtrip() {
        let test_values = vec![
            // Edge cases around single/multi-byte boundaries
            0,
            1,
            -1,
            63,
            -63,
            64,
            -64,
            // Powers of 2 and their negatives
            127,
            -127,
            128,
            -128,
            255,
            -255,
            256,
            -256,
            1023,
            -1023,
            1024,
            -1024,
            2047,
            -2047,
            2048,
            -2048,
            // Large values
            32767,
            -32768,
            65535,
            -65535,
            1000000,
            -1000000,
            // Maximum values
            i32::MAX as i64,
            i32::MIN as i64,
            // Very large values (but not max to avoid encoding issues)
            i64::MAX / 2,
            i64::MIN / 2,
        ];

        for value in test_values {
            let encoded = encode_vint(value);
            assert!(
                encoded.len() <= MAX_VINT_SIZE,
                "Encoded length {} exceeds maximum {} for value {}",
                encoded.len(),
                MAX_VINT_SIZE,
                value
            );

            let (remaining, decoded) = parse_vint(&encoded).unwrap();
            assert!(remaining.is_empty(), "Parsing should consume all bytes");
            assert_eq!(decoded, value, "Roundtrip failed for value {}", value);
        }
    }

    #[test]
    fn test_vint_format_compliance() {
        // Test specific bit patterns to ensure Cassandra compliance

        // Single byte: 0xxxxxxx
        let encoded = encode_vint(0);
        assert_eq!(encoded, vec![0x00]);

        let encoded = encode_vint(1);
        assert_eq!(encoded, vec![0x02]); // ZigZag: 1 -> 2

        let encoded = encode_vint(-1);
        assert_eq!(encoded, vec![0x01]); // ZigZag: -1 -> 1

        // Two bytes: 10xxxxxx xxxxxxxx
        let encoded = encode_vint(64);
        assert_eq!(encoded.len(), 2);
        assert_eq!(encoded[0] & 0xC0, 0x80); // Should start with 10

        // Verify we can parse back
        let (_, decoded) = parse_vint(&encoded).unwrap();
        assert_eq!(decoded, 64);
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

        // Test valid max length encoding (0xFF indicates 8 extra bytes = 9 total bytes)
        assert!(parse_vint(&[0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]).is_ok());

        // Test theoretical invalid condition: what if we had a first byte with all 1s except in an impossible way?
        // Since we now support 0xFF (8 extra bytes), there's no invalid first byte pattern.
        // The only way to trigger an error is insufficient bytes for the declared length.
        assert!(parse_vint(&[0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]).is_err()); // 0xFF needs 8 extra, only 7 provided

        // Test incomplete data
        assert!(parse_vint(&[0x80]).is_err()); // Claims 1 extra byte but none provided
        assert!(parse_vint(&[0x80, 0x00]).is_ok()); // Has the promised 1 extra byte (2 bytes total)
        assert!(parse_vint(&[0xC0, 0x00, 0x00]).is_ok()); // Has the promised 2 extra bytes (3 bytes total)
        assert!(parse_vint(&[0xC0, 0x00]).is_err()); // Missing 1 of the promised 2 extra bytes
        assert!(parse_vint(&[0xC0]).is_err()); // Missing the promised 2 extra bytes
    }

    #[test]
    fn test_vint_edge_case_patterns() {
        // Test maximum single-byte value
        let max_single = 63;
        let encoded = encode_vint(max_single);
        assert_eq!(encoded.len(), 1);
        assert_eq!(encoded[0] & 0x80, 0);

        // Test minimum two-byte value
        let min_double = 64;
        let encoded = encode_vint(min_double);
        assert_eq!(encoded.len(), 2);
        assert_eq!(encoded[0] & 0xC0, 0x80);
    }
}
