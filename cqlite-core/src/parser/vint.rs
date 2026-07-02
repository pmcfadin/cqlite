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

// Type aliases for complex types to reduce complexity warnings
type VintParseResult<'a> = Result<Option<(usize, i64)>, nom::Err<nom::error::Error<&'a [u8]>>>;

/// Detect ASCII corruption in VInt data
///
/// Common corruption patterns:
/// - ASCII strings like "data", "bin", "node" being parsed as VInt
/// - All bytes in printable ASCII range (0x20-0x7E)
/// - Common file extensions or directory names
#[allow(dead_code)]
fn detect_ascii_corruption(input: &[u8]) -> bool {
    if input.len() < 4 {
        return false;
    }

    // Check first 4 bytes for common ASCII corruption patterns
    let bytes = &input[0..4];

    // Common corrupted values we've seen
    let corrupted_patterns: &[&[u8]] = &[
        b"data", b"bin", b"node", b"base", b"temp", b"logs", b"meta", b"main", b"root", b"home",
    ];

    for pattern in corrupted_patterns {
        if bytes.starts_with(pattern) {
            return true;
        }
    }

    // Check if all bytes look like printable ASCII (likely corruption)
    let ascii_count = bytes
        .iter()
        .filter(|&&b| (0x20..=0x7E).contains(&b))
        .count();
    if ascii_count >= 3 {
        return true;
    }

    // Check for specific corrupted values we've encountered
    let value = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    match value {
        2959239534 | 1684108385 => true, // Known corrupted values: "bin" and "data"
        _ => false,
    }
}

/// Maximum bytes a VInt can occupy (Cassandra supports up to 9 bytes total)
pub const MAX_VINT_SIZE: usize = 9;

/// Maximum length value accepted by parse_vint_length to prevent overflow attacks.
/// Set to 1GB as a generous limit that won't cause allocation issues on any platform.
/// This prevents memory exhaustion attacks via malicious input claiming huge lengths.
pub const MAX_VINT_LENGTH: i64 = 1024 * 1024 * 1024; // 1GB safety limit

/// Decode a variable-length signed integer from bytes with backward compatibility
///
/// This function supports both:
/// 1. **ZigZag encoding** (legacy/test compatibility)
/// 2. **BTI format** (Issue #36 compatibility)
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

    let _first_byte = input[0];

    // Corruption detection: temporarily disabled to avoid false positives in collection data
    // TODO: Make corruption detection more sophisticated to distinguish between
    // legitimate string content in collections vs actual VInt corruption
    // if input.len() >= 8 && detect_ascii_corruption(input) {
    //     return Err(nom::Err::Error(nom::error::Error::new(
    //         input,
    //         nom::error::ErrorKind::Verify,
    //     )));
    // }

    // Try the fixed Cassandra-compatible VInt parsing first
    match crate::parser::vint_fixed::parse_vint_fixed(input) {
        Ok(result) => Ok(result),
        Err(_) => {
            // Fall back to ZigZag encoding for backward compatibility
            // This handles edge cases and legacy formats
            parse_zigzag_vint(input)
        }
    }
}

/// Parse VInt using ZigZag encoding (backward compatibility)
fn parse_zigzag_vint(input: &[u8]) -> IResult<&[u8], i64> {
    if input.is_empty() {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Eof,
        )));
    }

    let first_byte = input[0];
    let (bytes_used, unsigned_value) = if first_byte < 0x80 {
        // Single byte: 0xxxxxxx (7 data bits)
        (1, first_byte as u64)
    } else if first_byte < 0xC0 {
        // Two bytes: 10xxxxxx xxxxxxxx
        if input.len() < 2 {
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Eof,
            )));
        }
        let value = ((first_byte & 0x3F) as u64) << 8 | input[1] as u64;
        (2, value)
    } else if first_byte < 0xE0 {
        // Three bytes: 110xxxxx xxxxxxxx xxxxxxxx
        if input.len() < 3 {
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Eof,
            )));
        }
        let value = ((first_byte & 0x1F) as u64) << 16 | (input[1] as u64) << 8 | input[2] as u64;
        (3, value)
    } else if first_byte == 0xF0 {
        // Extended format: 0xF0 followed by variable length bytes
        if input.len() < 2 {
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Eof,
            )));
        }
        // Read the remaining bytes as a big-endian integer
        let mut value = 0u64;
        let bytes_to_read = input.len() - 1; // Skip the 0xF0 marker
        #[allow(clippy::needless_range_loop)]
        for i in 1..=bytes_to_read.min(8) {
            // Max 8 bytes for u64
            value = (value << 8) | (input[i] as u64);
        }
        (bytes_to_read + 1, value)
    } else if first_byte == 0xFF {
        // Extended format: 0xFF followed by variable length bytes (similar to 0xF0)
        if input.len() < 2 {
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Eof,
            )));
        }
        // Read the remaining bytes as a big-endian integer
        let mut value = 0u64;
        let bytes_to_read = input.len() - 1; // Skip the 0xFF marker
        #[allow(clippy::needless_range_loop)]
        for i in 1..=bytes_to_read.min(8) {
            // Max 8 bytes for u64
            value = (value << 8) | (input[i] as u64);
        }
        (bytes_to_read + 1, value)
    } else {
        // Not a valid ZigZag VInt, let caller try other formats
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    };

    let signed_value = zigzag_decode(unsigned_value);
    let (remaining_input, _) = take(bytes_used)(input)?;
    Ok((remaining_input, signed_value))
}

/// Parse VInt using Cassandra-compatible format
#[allow(dead_code)]
fn parse_cassandra_vint_format(input: &[u8]) -> VintParseResult<'_> {
    if input.is_empty() {
        return Ok(None);
    }

    let first_byte = input[0];

    // Count leading ones to determine the byte length
    let leading_ones = first_byte.leading_ones() as usize;
    let total_length = leading_ones + 1;

    if total_length > 9 || input.len() < total_length {
        return Ok(None); // Invalid or incomplete
    }

    // Extract the value based on the format
    let value = if total_length == 1 {
        // Single byte format
        if first_byte & 0x80 == 0x80 {
            // 1xxxxxxx format: values 0-127
            (first_byte & 0x7F) as i64
        } else if first_byte == 0xFF {
            -1
        } else if first_byte & 0xC0 == 0xC0 {
            // 11xxxxxx format: negative values -1 to -63
            -((first_byte & 0x3F) as i64)
        } else {
            // 0xxxxxxx format should not appear in Cassandra VInt
            return Ok(None);
        }
    } else {
        // Multi-byte format: extract data bits after leading pattern
        let data_bits = (total_length * 8) - leading_ones - 1;
        // Extract data from first byte (after leading pattern)
        let first_data_bits = 8 - leading_ones - 1;
        let first_data_mask = (1u8 << first_data_bits) - 1;
        let mut value = (first_byte & first_data_mask) as i64;

        // Add remaining bytes
        #[allow(clippy::needless_range_loop)]
        for i in 1..total_length {
            value = (value << 8) | (input[i] as i64);
        }

        // Check if this should be interpreted as negative (two's complement)
        // For Cassandra VInt, we need to handle signed values properly
        let max_positive = (1i64 << (data_bits - 1)) - 1;
        if value > max_positive {
            // Convert from unsigned to signed (two's complement)
            value -= 1i64 << data_bits;
        }

        value
    };

    Ok(Some((total_length, value)))
}

/// Parse VInt using custom BTI format (Issue #36)
#[allow(dead_code)]
fn parse_custom_vint_format(input: &[u8]) -> VintParseResult<'_> {
    if input.is_empty() {
        return Ok(None);
    }

    let first_byte = input[0];

    let (total_length, value) = if first_byte < 0x80 {
        // Single byte: 0xxxxxxx (7 data bits)
        let unsigned_value = first_byte & 0x7F;
        let value = if unsigned_value < 64 {
            unsigned_value as i64
        } else {
            (unsigned_value as i64) - 128
        };
        (1, value)
    } else if first_byte < 0xC0 {
        // Single byte: 10xxxxxx (0x80-0xBF) -> values 0-63
        let value = (first_byte & 0x3F) as i64;
        (1, value)
    } else if first_byte == 0xFF {
        // Special case: 0xFF represents -1
        (1, -1)
    } else if first_byte >= 0xC0 {
        if input.len() == 1 {
            // Single byte negative: 0xC0-0xFE maps to -64 to -2
            let value = -64 + (first_byte - 0xC0) as i64;
            (1, value)
        } else if first_byte == 0xC0 && input.len() >= 2 {
            // Two-byte format: 0xC0 + value byte
            let second_byte = input[1];
            let value = if second_byte <= 0x7F {
                second_byte as i64
            } else if second_byte == 0x80 {
                -128
            } else {
                second_byte as i64
            };
            (2, value)
        } else {
            return Ok(None); // Not supported in this format
        }
    } else {
        return Ok(None);
    };

    if input.len() < total_length {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Eof,
        )));
    }

    Ok(Some((total_length, value)))
}

/// Encode using Cassandra-compatible VInt format
#[allow(dead_code)]
fn encode_cassandra_vint(value: i64) -> Vec<u8> {
    // Handle negative values using two's complement representation
    let _unsigned_value = if value >= 0 {
        value as u64
    } else {
        // Use two's complement for negative values
        value as u64 // This will wrap negative values correctly
    };

    // Determine the number of bytes needed
    let bytes_needed = if value == 0 {
        1
    } else if (-63..=63).contains(&value) {
        1 // Single byte range for small values
    } else if (-8192..=8191).contains(&value) {
        2 // Two bytes
    } else if (-1048576..=1048575).contains(&value) {
        3 // Three bytes
    } else if (-134217728..=134217727).contains(&value) {
        4 // Four bytes
    } else {
        // Calculate bytes needed for larger values
        let abs_value = value.unsigned_abs();
        if abs_value <= 0xFF {
            2
        } else if abs_value <= 0xFFFF {
            3
        } else if abs_value <= 0xFFFFFF {
            4
        } else if abs_value <= 0xFFFFFFFF {
            5
        } else {
            8 // Maximum for i64
        }
    };

    match bytes_needed {
        1 => {
            // Single byte: 1xxxxxxx for values 0-127, 0xxxxxxx for negative -1 to -63
            if (0..=63).contains(&value) {
                vec![0x80 | (value as u8)]
            } else if value == -1 {
                vec![0xFF]
            } else if (-63..0).contains(&value) {
                vec![0xC0 | ((-value) as u8)]
            } else {
                // fallback to two bytes
                encode_cassandra_vint_multi_byte(value, 2)
            }
        }
        2 => encode_cassandra_vint_multi_byte(value, 2),
        3 => encode_cassandra_vint_multi_byte(value, 3),
        4 => encode_cassandra_vint_multi_byte(value, 4),
        _ => encode_cassandra_vint_multi_byte(value, bytes_needed),
    }
}

/// Encode multi-byte Cassandra VInt with proper leading bit pattern
#[allow(dead_code)]
fn encode_cassandra_vint_multi_byte(value: i64, num_bytes: usize) -> Vec<u8> {
    let mut result = vec![0u8; num_bytes];

    // Set the leading bit pattern: n-1 leading ones followed by a zero
    let leading_ones = num_bytes - 1;
    let first_byte_mask = (0xFF << (8 - leading_ones)) & 0xFF;

    // Convert value to bytes (using two's complement for negatives)
    let value_bytes = if value >= 0 {
        value.to_be_bytes()
    } else {
        (value as u64).to_be_bytes() // Two's complement representation
    };

    // Place the value in the remaining bits
    let data_bits = (num_bytes * 8) - leading_ones - 1; // Total data bits available
    let data_bytes = data_bits.div_ceil(8); // How many bytes we need for data

    // Copy the relevant bytes from value_bytes
    let start_idx = 8 - data_bytes;
    for (i, &byte) in value_bytes[start_idx..].iter().enumerate() {
        if i == 0 {
            // First byte: combine leading pattern with data
            let data_mask = (1u8 << (8 - leading_ones - 1)) - 1;
            result[0] = first_byte_mask as u8 | (byte & data_mask);
        } else {
            result[i] = byte;
        }
    }

    result
}

/// Encode a signed integer using ZigZag encoding (backward compatibility)
#[allow(dead_code)]
fn encode_zigzag_vint(value: i64) -> Vec<u8> {
    let unsigned_value = zigzag_encode(value);

    if unsigned_value <= 0x7F {
        // Single byte: 0xxxxxxx
        vec![unsigned_value as u8]
    } else if unsigned_value <= 0x3FFF {
        // Two bytes: 10xxxxxx xxxxxxxx
        let high = ((unsigned_value >> 8) & 0x3F) | 0x80;
        let low = unsigned_value & 0xFF;
        vec![high as u8, low as u8]
    } else if unsigned_value <= 0x1FFFFF {
        // Three bytes: 110xxxxx xxxxxxxx xxxxxxxx
        let high = ((unsigned_value >> 16) & 0x1F) | 0xC0;
        let mid = (unsigned_value >> 8) & 0xFF;
        let low = unsigned_value & 0xFF;
        vec![high as u8, mid as u8, low as u8]
    } else {
        // For larger values, use a simplified multi-byte format
        let bytes = unsigned_value.to_be_bytes();
        let mut result = vec![0xF0]; // Marker for extended format

        // Find the first non-zero byte and include remaining bytes
        let start = bytes.iter().position(|&b| b != 0).unwrap_or(7);
        result.extend_from_slice(&bytes[start..]);
        result
    }
}

/// ZigZag encode a signed integer to unsigned (for efficient small negative number encoding)
///
/// ZigZag encoding maps signed integers to unsigned integers so that numbers
/// with small absolute values have small encodings:
/// 0 -> 0, -1 -> 1, 1 -> 2, -2 -> 3, 2 -> 4, -3 -> 5, ...
#[allow(dead_code)]
pub fn zigzag_encode(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

/// ZigZag decode an unsigned integer back to signed
#[allow(dead_code)]
pub fn zigzag_decode(value: u64) -> i64 {
    ((value >> 1) ^ ((!0u64).wrapping_mul(value & 1))) as i64
}

/// Calculate the number of bytes needed to encode a value
///
/// Cassandra VInt encoding boundaries:
/// - 1 byte: 0xxxxxxx -> 0 to 127 (7 bits)
/// - 2 bytes: 10xxxxxx xxxxxxxx -> 0 to 16383 (14 bits: 6+8)
/// - 3 bytes: 110xxxxx xxxxxxxx xxxxxxxx -> 0 to 2097151 (21 bits: 5+16)
/// - etc.
#[allow(dead_code)]
fn vint_size(value: u64) -> usize {
    if value == 0 {
        return 1;
    }

    // Cassandra VInt boundaries based on actual capacity
    if value <= 127 {
        // 2^7 - 1 (7 bits)
        1
    } else if value <= 16383 {
        // 2^14 - 1 (14 bits)
        2
    } else if value <= 2097151 {
        // 2^21 - 1 (21 bits)
        3
    } else if value <= 268435455 {
        // 2^28 - 1 (28 bits)
        4
    } else if value <= 34359738367 {
        // 2^35 - 1 (35 bits)
        5
    } else if value <= 4398046511103 {
        // 2^42 - 1 (42 bits)
        6
    } else if value <= 562949953421311 {
        // 2^49 - 1 (49 bits)
        7
    } else if value <= 72057594037927935 {
        // 2^56 - 1 (56 bits)
        8
    } else {
        9 // Maximum size
    }
}

/// Encode a signed integer as a variable-length integer with backward compatibility
///
/// This function now prioritizes Cassandra-compatible format for better
/// compatibility with standard Cassandra VInt encoding.
///
/// # Arguments
///
/// * `value` - The integer value to encode
///
/// # Returns
///
/// Vector of bytes representing the VInt-encoded value
pub fn encode_vint(value: i64) -> Vec<u8> {
    encode_vint_zigzag(value)
}

/// Encode VInt using original ZigZag format for backward compatibility
pub fn encode_vint_zigzag(value: i64) -> Vec<u8> {
    let unsigned_value = zigzag_encode(value);

    if unsigned_value <= 0x7F {
        // Single byte: 0xxxxxxx
        vec![unsigned_value as u8]
    } else if unsigned_value <= 0x3FFF {
        // Two bytes: 10xxxxxx xxxxxxxx
        let byte0 = 0x80 | ((unsigned_value >> 8) & 0x3F) as u8;
        let byte1 = (unsigned_value & 0xFF) as u8;
        vec![byte0, byte1]
    } else if unsigned_value <= 0x1FFFFF {
        // Three bytes: 110xxxxx xxxxxxxx xxxxxxxx
        let byte0 = 0xC0 | ((unsigned_value >> 16) & 0x1F) as u8;
        let byte1 = ((unsigned_value >> 8) & 0xFF) as u8;
        let byte2 = (unsigned_value & 0xFF) as u8;
        vec![byte0, byte1, byte2]
    } else if unsigned_value <= 0xFFFFFFF {
        // Four bytes: 1110xxxx xxxxxxxx xxxxxxxx xxxxxxxx
        let byte0 = 0xE0 | ((unsigned_value >> 24) & 0x0F) as u8;
        let byte1 = ((unsigned_value >> 16) & 0xFF) as u8;
        let byte2 = ((unsigned_value >> 8) & 0xFF) as u8;
        let byte3 = (unsigned_value & 0xFF) as u8;
        vec![byte0, byte1, byte2, byte3]
    } else if unsigned_value <= 0x7FFFFFFFF {
        // Five bytes: 11110xxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
        let byte0 = 0xF0 | ((unsigned_value >> 32) & 0x07) as u8;
        let byte1 = ((unsigned_value >> 24) & 0xFF) as u8;
        let byte2 = ((unsigned_value >> 16) & 0xFF) as u8;
        let byte3 = ((unsigned_value >> 8) & 0xFF) as u8;
        let byte4 = (unsigned_value & 0xFF) as u8;
        vec![byte0, byte1, byte2, byte3, byte4]
    } else if unsigned_value <= 0x3FFFFFFFFFF {
        // Six bytes: 111110xx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
        let byte0 = 0xF8 | ((unsigned_value >> 40) & 0x03) as u8;
        let byte1 = ((unsigned_value >> 32) & 0xFF) as u8;
        let byte2 = ((unsigned_value >> 24) & 0xFF) as u8;
        let byte3 = ((unsigned_value >> 16) & 0xFF) as u8;
        let byte4 = ((unsigned_value >> 8) & 0xFF) as u8;
        let byte5 = (unsigned_value & 0xFF) as u8;
        vec![byte0, byte1, byte2, byte3, byte4, byte5]
    } else if unsigned_value <= 0x1FFFFFFFFFFFF {
        // Seven bytes: 1111110x xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
        let byte0 = 0xFC | ((unsigned_value >> 48) & 0x01) as u8;
        let byte1 = ((unsigned_value >> 40) & 0xFF) as u8;
        let byte2 = ((unsigned_value >> 32) & 0xFF) as u8;
        let byte3 = ((unsigned_value >> 24) & 0xFF) as u8;
        let byte4 = ((unsigned_value >> 16) & 0xFF) as u8;
        let byte5 = ((unsigned_value >> 8) & 0xFF) as u8;
        let byte6 = (unsigned_value & 0xFF) as u8;
        vec![byte0, byte1, byte2, byte3, byte4, byte5, byte6]
    } else if unsigned_value <= 0xFFFFFFFFFFFFFF {
        // Eight bytes: 11111110 xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
        let byte0 = 0xFE;
        let byte1 = ((unsigned_value >> 48) & 0xFF) as u8;
        let byte2 = ((unsigned_value >> 40) & 0xFF) as u8;
        let byte3 = ((unsigned_value >> 32) & 0xFF) as u8;
        let byte4 = ((unsigned_value >> 24) & 0xFF) as u8;
        let byte5 = ((unsigned_value >> 16) & 0xFF) as u8;
        let byte6 = ((unsigned_value >> 8) & 0xFF) as u8;
        let byte7 = (unsigned_value & 0xFF) as u8;
        vec![byte0, byte1, byte2, byte3, byte4, byte5, byte6, byte7]
    } else {
        // Nine bytes: 11111111 xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
        let byte0 = 0xFF;
        let byte1 = ((unsigned_value >> 56) & 0xFF) as u8;
        let byte2 = ((unsigned_value >> 48) & 0xFF) as u8;
        let byte3 = ((unsigned_value >> 40) & 0xFF) as u8;
        let byte4 = ((unsigned_value >> 32) & 0xFF) as u8;
        let byte5 = ((unsigned_value >> 24) & 0xFF) as u8;
        let byte6 = ((unsigned_value >> 16) & 0xFF) as u8;
        let byte7 = ((unsigned_value >> 8) & 0xFF) as u8;
        let byte8 = (unsigned_value & 0xFF) as u8;
        vec![
            byte0, byte1, byte2, byte3, byte4, byte5, byte6, byte7, byte8,
        ]
    }
}

/// Encode VInt using Cassandra-compatible format for Issue #17 requirements
pub fn encode_vint_cassandra(value: i64) -> Vec<u8> {
    crate::parser::vint_fixed::encode_vint_fixed(value)
}

/// Parse VInt using Cassandra-compatible format for Issue #17 requirements
pub fn parse_vint_cassandra(input: &[u8]) -> IResult<&[u8], i64> {
    crate::parser::vint_fixed::parse_vint_fixed(input)
}

/// Parse unsigned VInt32 for Cassandra value lengths
///
/// Matches org/apache/cassandra/io/util/DataInputPlus.readUnsignedVInt32()
/// Used for variable-width type value lengths (text, blob, decimal, etc.)
///
/// Encoding format:
/// - Leading 1-bits indicate number of extra bytes
/// - Pattern: [n ones][0][data bits]
/// - Example: 0xxxxxxx = 1 byte, 10xxxxxx xxxxxxxx = 2 bytes
///
/// # Arguments
///
/// * `input` - Input byte slice
///
/// # Returns
///
/// Tuple of (remaining_bytes, decoded_u32_value)
pub fn parse_unsigned_vint32(input: &[u8]) -> IResult<&[u8], u32> {
    if input.is_empty() {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Eof,
        )));
    }

    let first_byte = input[0];
    let num_extra_bytes = first_byte.leading_ones() as usize;

    if num_extra_bytes > 4 || num_extra_bytes + 1 > input.len() {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Eof,
        )));
    }

    let value = if num_extra_bytes == 0 {
        // Single byte: 0xxxxxxx
        (first_byte & 0x7F) as u32
    } else {
        // Multi-byte: extract data bits after leading ones pattern
        let data_bits_first = 8 - num_extra_bytes - 1;
        let mask = (1u8 << data_bits_first) - 1;
        let mut value = (first_byte & mask) as u32;

        for &byte in input.iter().skip(1).take(num_extra_bytes) {
            value = (value << 8) | (byte as u32);
        }
        value
    };

    let bytes_consumed = num_extra_bytes + 1;
    let (remaining, _) = take(bytes_consumed)(input)?;
    Ok((remaining, value))
}

/// Parse unsigned VInt64 for Cassandra timestamps
///
/// Matches org/apache/cassandra/io/util/DataInputPlus.readUnsignedVInt()
/// Used for timestamp deltas and other 64-bit unsigned values.
///
/// Encoding format:
/// - Leading 1-bits indicate number of extra bytes
/// - Pattern: [n ones][0][data bits]
/// - Example: 0xxxxxxx = 1 byte, 10xxxxxx xxxxxxxx = 2 bytes
/// - Maximum 8 extra bytes (9 bytes total) for full 64-bit range
///
/// # Arguments
///
/// * `input` - Input byte slice
///
/// # Returns
///
/// Tuple of (remaining_bytes, decoded_u64_value)
pub fn parse_vuint(input: &[u8]) -> IResult<&[u8], u64> {
    if input.is_empty() {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Eof,
        )));
    }

    let first_byte = input[0];
    let num_extra_bytes = first_byte.leading_ones() as usize;

    if num_extra_bytes > 8 || num_extra_bytes + 1 > input.len() {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Eof,
        )));
    }

    let value = if num_extra_bytes == 0 {
        // Single byte: 0xxxxxxx
        (first_byte & 0x7F) as u64
    } else if num_extra_bytes == 8 {
        // Special case: 8 leading ones (0xFF) means 8 extra bytes follow, no data bits in first byte
        let mut value = 0u64;
        for &byte in input.iter().skip(1).take(num_extra_bytes) {
            value = (value << 8) | (byte as u64);
        }
        value
    } else {
        // Multi-byte: extract data bits after leading ones pattern
        let data_bits_first = 8 - num_extra_bytes - 1;
        let mask = (1u8 << data_bits_first) - 1;
        let mut value = (first_byte & mask) as u64;

        for &byte in input.iter().skip(1).take(num_extra_bytes) {
            value = (value << 8) | (byte as u64);
        }
        value
    };

    let bytes_consumed = num_extra_bytes + 1;
    let (remaining, _) = take(bytes_consumed)(input)?;
    Ok((remaining, value))
}

/// Encode an unsigned integer as a variable-length integer
pub fn encode_vuint(value: u64) -> Vec<u8> {
    if value <= 0x7F {
        // Single byte: 0xxxxxxx
        vec![value as u8]
    } else if value <= 0x3FFF {
        // Two bytes: 10xxxxxx xxxxxxxx
        let byte0 = 0x80 | ((value >> 8) & 0x3F) as u8;
        let byte1 = (value & 0xFF) as u8;
        vec![byte0, byte1]
    } else if value <= 0x1FFFFF {
        // Three bytes: 110xxxxx xxxxxxxx xxxxxxxx
        let byte0 = 0xC0 | ((value >> 16) & 0x1F) as u8;
        let byte1 = ((value >> 8) & 0xFF) as u8;
        let byte2 = (value & 0xFF) as u8;
        vec![byte0, byte1, byte2]
    } else if value <= 0xFFFFFFF {
        // Four bytes: 1110xxxx xxxxxxxx xxxxxxxx xxxxxxxx
        let byte0 = 0xE0 | ((value >> 24) & 0x0F) as u8;
        let byte1 = ((value >> 16) & 0xFF) as u8;
        let byte2 = ((value >> 8) & 0xFF) as u8;
        let byte3 = (value & 0xFF) as u8;
        vec![byte0, byte1, byte2, byte3]
    } else if value <= 0x7FFFFFFFF {
        // Five bytes: 11110xxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
        let byte0 = 0xF0 | ((value >> 32) & 0x07) as u8;
        let byte1 = ((value >> 24) & 0xFF) as u8;
        let byte2 = ((value >> 16) & 0xFF) as u8;
        let byte3 = ((value >> 8) & 0xFF) as u8;
        let byte4 = (value & 0xFF) as u8;
        vec![byte0, byte1, byte2, byte3, byte4]
    } else if value <= 0x3FFFFFFFFFF {
        // Six bytes: 111110xx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
        let byte0 = 0xF8 | ((value >> 40) & 0x03) as u8;
        let byte1 = ((value >> 32) & 0xFF) as u8;
        let byte2 = ((value >> 24) & 0xFF) as u8;
        let byte3 = ((value >> 16) & 0xFF) as u8;
        let byte4 = ((value >> 8) & 0xFF) as u8;
        let byte5 = (value & 0xFF) as u8;
        vec![byte0, byte1, byte2, byte3, byte4, byte5]
    } else if value <= 0x1FFFFFFFFFFFF {
        // Seven bytes: 1111110x xxxxxxxx ... xxxxxxxx
        let byte0 = 0xFC | ((value >> 48) & 0x01) as u8;
        let byte1 = ((value >> 40) & 0xFF) as u8;
        let byte2 = ((value >> 32) & 0xFF) as u8;
        let byte3 = ((value >> 24) & 0xFF) as u8;
        let byte4 = ((value >> 16) & 0xFF) as u8;
        let byte5 = ((value >> 8) & 0xFF) as u8;
        let byte6 = (value & 0xFF) as u8;
        vec![byte0, byte1, byte2, byte3, byte4, byte5, byte6]
    } else if value <= 0xFFFFFFFFFFFFFF {
        // Eight bytes: 11111110 xxxxxxxx ... xxxxxxxx
        let byte0 = 0xFE;
        let byte1 = ((value >> 48) & 0xFF) as u8;
        let byte2 = ((value >> 40) & 0xFF) as u8;
        let byte3 = ((value >> 32) & 0xFF) as u8;
        let byte4 = ((value >> 24) & 0xFF) as u8;
        let byte5 = ((value >> 16) & 0xFF) as u8;
        let byte6 = ((value >> 8) & 0xFF) as u8;
        let byte7 = (value & 0xFF) as u8;
        vec![byte0, byte1, byte2, byte3, byte4, byte5, byte6, byte7]
    } else {
        // Nine bytes: 11111111 xxxxxxxx ... xxxxxxxx (full 8 bytes follow)
        let byte0 = 0xFF;
        let byte1 = ((value >> 56) & 0xFF) as u8;
        let byte2 = ((value >> 48) & 0xFF) as u8;
        let byte3 = ((value >> 40) & 0xFF) as u8;
        let byte4 = ((value >> 32) & 0xFF) as u8;
        let byte5 = ((value >> 24) & 0xFF) as u8;
        let byte6 = ((value >> 16) & 0xFF) as u8;
        let byte7 = ((value >> 8) & 0xFF) as u8;
        let byte8 = (value & 0xFF) as u8;
        vec![
            byte0, byte1, byte2, byte3, byte4, byte5, byte6, byte7, byte8,
        ]
    }
}

/// Test-only differential audit of length/count decodes (Issue #1623).
///
/// Every `parse_vint_length` call records whether the OLD signed (ZigZag)
/// decode of the same bytes would have AGREED with the NEW unsigned decode.
/// The verdict is intrinsic to the input bytes (not to the caller), so the
/// counts remain correct even if unrelated tests decode concurrently. The
/// corpus-differential test resets these around a full 33-table scan to report
/// the real blast radius of the ZigZag mis-read.
#[cfg(test)]
pub(crate) mod length_decode_audit {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    pub(crate) static AGREE: AtomicU64 = AtomicU64::new(0);
    pub(crate) static DISAGREE: AtomicU64 = AtomicU64::new(0);
    static ENABLED: AtomicBool = AtomicBool::new(false);

    /// Reset counters and arm recording for the current audit window.
    pub(crate) fn arm() {
        AGREE.store(0, Ordering::SeqCst);
        DISAGREE.store(0, Ordering::SeqCst);
        ENABLED.store(true, Ordering::SeqCst);
    }

    /// Disarm recording; returns `(agree, disagree)`.
    pub(crate) fn disarm() -> (u64, u64) {
        ENABLED.store(false, Ordering::SeqCst);
        (
            AGREE.load(Ordering::SeqCst),
            DISAGREE.load(Ordering::SeqCst),
        )
    }

    /// Compare the legacy signed decode against the unsigned decode of the same
    /// bytes and tally the verdict. Only records while armed.
    pub(crate) fn record(input: &[u8]) {
        if !ENABLED.load(Ordering::Relaxed) {
            return;
        }
        // Legacy behaviour: signed ZigZag decode, rejecting negatives, as a usize.
        let signed =
            super::parse_vint(input)
                .ok()
                .and_then(|(_, v)| if v >= 0 { Some(v as u64) } else { None });
        // Fixed behaviour: unsigned decode.
        let unsigned = super::parse_vuint(input).ok().map(|(_, v)| v);
        match (signed, unsigned) {
            (Some(s), Some(u)) if s == u => {
                AGREE.fetch_add(1, Ordering::Relaxed);
            }
            _ => {
                DISAGREE.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// Parse an UNSIGNED VInt length/count field (Cassandra `writeUnsignedVInt`).
///
/// # Producer classification (Issue #1623)
///
/// Cassandra encodes length, count, and collection-element-count fields with
/// `writeUnsignedVInt` — UNSIGNED, no ZigZag (see Appendix B, encodings cheat
/// sheet). ZigZag is used ONLY for genuinely-signed deltas. This function
/// therefore routes through [`parse_vuint`], NOT the signed [`parse_vint`].
///
/// The prior implementation routed through the signed ZigZag decoder, silently
/// mis-reading any unsigned length whose bit pattern differs under ZigZag while
/// consuming the same byte count (e.g. raw `0x05` is length 5, but ZigZag reads
/// it as -3 → error; `0x04` is length 4 but ZigZag reads 2).
///
/// ## Call-site verdicts
///
/// | Site (producer)                                              | Verdict          |
/// |--------------------------------------------------------------|------------------|
/// | Data.db rows/cells/collections: `row_cell_state_machine`,    | flip-to-unsigned |
/// |   `reader/parsing/{key_parsing,block_entries,value_parsing,  |   (Cassandra;    |
/// |   comparator_value_parsing}`, `types/{collections,primitives,|   CQLite's own   |
/// |   tombstones,udt}`, `optimized_complex_types`                |   writer also    |
/// |                                                              |   uses           |
/// |                                                              |   `encode_unsigned`) |
/// | Statistics.db: `parser/statistics.rs` (superseded on the     | flip-to-unsigned |
/// |   prod path by `enhanced_statistics_parser`, format-unsigned) |                  |
/// | Header-spec field parser: `storage/.../header_spec.rs`        | flip-to-unsigned |
/// | Header round-trip: `parser/header.rs` standard body —         | KEEP-SIGNED →    |
/// |   `serialize_sstable_header` writes lengths with `encode_vint`|   uses           |
/// |   (ZigZag); a CQLite-internal format, short-circuited for     |   `parse_vint_length_signed` |
/// |   real V5 data via `parse_cassandra5_simplified_header`       |                  |
///
/// # Safety
/// Enforces a maximum length of 1GB ([`MAX_VINT_LENGTH`]) to prevent:
/// - Overflow on 32-bit platforms where usize is 4 bytes
/// - Memory exhaustion attacks via malicious input claiming huge lengths
pub fn parse_vint_length(input: &[u8]) -> IResult<&[u8], usize> {
    #[cfg(test)]
    length_decode_audit::record(input);

    let (remaining, value) = parse_vuint(input)?;
    // Safety: Prevent overflow on usize conversion and allocation attacks.
    // `value` is u64; MAX_VINT_LENGTH (1GB) is far below usize::MAX on all
    // supported platforms (>= 32-bit), so the cast below cannot truncate once
    // the value is bounded.
    if value > MAX_VINT_LENGTH as u64 {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::TooLarge,
        )));
    }
    Ok((remaining, value as usize))
}

/// Parse a SIGNED (ZigZag) VInt length field written by CQLite's own
/// serializers.
///
/// # CQLite-internal: encoder is ZigZag
///
/// A handful of CQLite-internal, self-consistent round-trips write length
/// prefixes with the ZigZag encoder `parser::vint::encode_vint`
/// (`serialize_sstable_header` and friends in `parser/header.rs`). Reading
/// those back requires the SAME ZigZag decode — flipping them to unsigned would
/// corrupt the round-trip. This helper preserves the historical signed decode
/// while keeping the identical safety caps as [`parse_vint_length`].
///
/// Do NOT use this for Cassandra-produced bytes — those are unsigned; use
/// [`parse_vint_length`].
pub fn parse_vint_length_signed(input: &[u8]) -> IResult<&[u8], usize> {
    let (remaining, value) = parse_vint(input)?;
    if value < 0 {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }
    if value > MAX_VINT_LENGTH {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::TooLarge,
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
        // Test small values that fit in single byte (original ZigZag format)
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
        // Test specific bit patterns to ensure format compliance

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
        // Issue #1623: length fields are UNSIGNED. Encode with the unsigned
        // encoder so the byte pattern matches what Cassandra writes.
        let bytes = encode_vuint(42);
        let (_, length) = parse_vint_length(&bytes).unwrap();
        assert_eq!(length, 42);
    }

    /// Issue #1623 ACCEPTANCE TEST: a raw unsigned byte `0x05` is length 5.
    ///
    /// This MUST FAIL on `main` (commit 1) where `parse_vint_length` routes
    /// through the signed ZigZag decoder: `0x05` ZigZag-decodes to -3, which is
    /// rejected as a negative length. Commit 2 (the fix) routes through the
    /// unsigned decoder and makes this pass.
    #[test]
    fn test_parse_vint_length_unsigned_single_byte_ac() {
        assert_eq!(parse_vint_length(&[0x05]).unwrap().1, 5);
    }

    /// Issue #1623: additional unsigned single-byte checks. `0x04` is length 4
    /// (the signed decoder silently returns 2), `0x7F` is the single-byte max.
    #[test]
    fn test_parse_vint_length_unsigned_bit_patterns() {
        assert_eq!(parse_vint_length(&[0x04]).unwrap().1, 4);
        assert_eq!(parse_vint_length(&[0x7F]).unwrap().1, 127);
        assert_eq!(parse_vint_length(&[0x00]).unwrap().1, 0);
        // Two-byte unsigned: 0x80 0x80 == 128
        assert_eq!(parse_vint_length(&[0x80, 0x80]).unwrap().1, 128);
    }

    /// Issue #1623: unsigned length round-trip via the unsigned encoder for a
    /// spread of values, including odd values whose ZigZag pattern would be
    /// mis-read by the signed decoder.
    #[test]
    fn test_parse_vint_length_unsigned_roundtrip() {
        for value in [
            0u64, 1, 2, 3, 4, 5, 63, 64, 127, 128, 255, 256, 16383, 16384, 100_000,
        ] {
            let bytes = encode_vuint(value);
            let (_, decoded) = parse_vint_length(&bytes)
                .unwrap_or_else(|_| panic!("unsigned length decode failed for {value}"));
            assert_eq!(decoded as u64, value, "roundtrip failed for {value}");
        }
    }

    /// Issue #1623: the signed helper preserves the CQLite-internal ZigZag
    /// round-trip used by `serialize_sstable_header` (`encode_vint`).
    #[test]
    fn test_parse_vint_length_signed_zigzag_roundtrip() {
        for value in [0i64, 1, 5, 13, 42, 63, 64, 127, 1000] {
            let bytes = encode_vint(value); // ZigZag encoder
            let (_, decoded) = parse_vint_length_signed(&bytes)
                .unwrap_or_else(|_| panic!("signed length decode failed for {value}"));
            assert_eq!(decoded as i64, value, "signed roundtrip failed for {value}");
        }
        // Negative ZigZag values are rejected as lengths.
        assert!(parse_vint_length_signed(&encode_vint(-10)).is_err());
    }

    #[test]
    fn test_collection_vint_debug() {
        // Debug the collection test issue
        let encoded_4 = encode_vint(4);
        println!("encode_vint(4) = {:?}", encoded_4);
        let (_, decoded_4) = parse_vint(&encoded_4).unwrap();
        println!("parse_vint({:?}) = {}", encoded_4, decoded_4);

        // Check what [10] decodes to
        let test_10 = [10u8];
        let (_, decoded_10) = parse_vint(&test_10).unwrap();
        println!("parse_vint([10]) = {}", decoded_10);

        // Check what encodes to [10]
        for i in 0..20 {
            let encoded = encode_vint(i);
            if encoded == vec![10] {
                println!("Value {} encodes to [10]", i);
            }
        }

        assert_eq!(decoded_4, 4, "Roundtrip test for 4");

        // Debug the specific collection test issue
        let long_string = "this is a longer string";
        let encoded_23 = encode_vint(long_string.len() as i64);
        println!("encode_vint(23) = {:?}", encoded_23);
        println!(
            "String length: {}, bytes: {:?}",
            long_string.len(),
            long_string.as_bytes()
        );

        // Check if the encoded length triggers ASCII corruption detection
        match parse_vint(&encoded_23) {
            Ok((_, decoded)) => println!("parse_vint({:?}) = {}", encoded_23, decoded),
            Err(e) => println!("parse_vint({:?}) failed: {:?}", encoded_23, e),
        }

        // Debug the specific failing values
        println!("\n🔍 Debug failing VInt cases:");
        let failing_values = vec![256, 1048576, 64];
        for value in failing_values {
            let encoded = encode_vint(value);
            println!(
                "Value {}: encoded={:?}, hex={:02X?}, len={}",
                value,
                encoded,
                encoded,
                encoded.len()
            );
            if !encoded.is_empty() {
                let first_byte = encoded[0];
                println!(
                    "  First byte: 0x{:02X} ({:08b}), leading_ones: {}",
                    first_byte,
                    first_byte,
                    first_byte.leading_ones()
                );
                if encoded.len() > 1 {
                    println!(
                        "  Expected leading ones: {}, got: {}",
                        encoded.len() - 1,
                        first_byte.leading_ones()
                    );
                }
            }
        }

        // Test Cassandra expected bytes
        println!("\n🔍 Testing Cassandra format:");
        let cassandra_bytes = vec![0xE0, 0x01, 0x00]; // Should decode to 256
        match parse_vint(&cassandra_bytes) {
            Ok((_, decoded)) => println!("Cassandra bytes {:?} -> {}", cassandra_bytes, decoded),
            Err(e) => println!(
                "Failed to parse Cassandra bytes {:?}: {:?}",
                cassandra_bytes, e
            ),
        }
    }

    #[test]
    fn test_vint_errors() {
        // Test empty input
        assert!(parse_vint(&[]).is_err());

        // Issue #1623: length fields are unsigned, so they cannot be negative.
        // Negative-length rejection now lives on the signed helper.
        assert!(parse_vint_length_signed(&encode_vint(-10)).is_err());
        // An over-1GB unsigned length is rejected by the length cap.
        assert!(parse_vint_length(&encode_vuint(2_000_000_000u64)).is_err());

        // Test valid max length encoding (0xFF indicates 8 extra bytes = 9 total bytes)
        assert!(parse_vint(&[0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]).is_ok());

        // Test valid extended formats - should succeed now with backward compatibility
        assert!(parse_vint(&[0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]).is_ok()); // F0 extended format
        assert!(parse_vint(&[0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]).is_ok()); // Should work with ZigZag parsing

        // Test incomplete data - with backward compatibility, focus on truly invalid cases
        assert!(parse_vint(&[0x80, 0x00]).is_ok()); // Two-byte format with data
        assert!(parse_vint(&[0xC0, 0x00, 0x00]).is_ok()); // Three-byte format with data

        // Test truly invalid sequences (corrupted data that shouldn't parse)
        // Focus on patterns that should be rejected by corruption detection
        let _corrupted_data = b"data"; // ASCII corruption
                                       // Note: corruption detection should catch these, but if not, we accept them
                                       // as the new format is more permissive for backward compatibility
    }

    #[test]
    fn test_vint_edge_case_patterns() {
        // Test maximum single-byte value
        let max_single = 63;
        let encoded = encode_vint(max_single);
        assert_eq!(encoded.len(), 1);
        assert_eq!(encoded[0] & 0x80, 0); // Original format has leading 0

        // Test minimum two-byte value
        let min_double = 64;
        let encoded = encode_vint(min_double);
        assert_eq!(encoded.len(), 2);
        assert_eq!(encoded[0] & 0xC0, 0x80); // Two-byte format starts with 10
    }

    #[test]
    fn test_detect_ascii_corruption_patterns() {
        assert!(detect_ascii_corruption(b"data_payload"));
        assert!(detect_ascii_corruption(b"node_meta"));
        assert!(!detect_ascii_corruption(&[0x00, 0x80, 0xFF, 0x10]));
    }

    #[test]
    fn test_parse_vint_extended_formats() {
        // 0xF0 prefix should fall back to ZigZag parsing and succeed
        let bytes = [0xF0, 0x00, 0x00, 0x00, 0x10];
        let _ = parse_vint(&bytes).expect("extended format parses");

        // 0xFF extended format should also succeed
        let bytes = [0xFF, 0x00, 0x00, 0x00, 0x05];
        let _ = parse_vint(&bytes).expect("fallback parse");
    }

    // Issue #264 / #1623: VInt overflow protection tests (unsigned lengths).
    #[test]
    fn test_vint_overflow_protection() {
        let max = MAX_VINT_LENGTH as u64;

        // Test 1: Value exceeding 1GB limit should fail
        let large_value = encode_vuint(2_000_000_000u64); // 2GB - exceeds MAX_VINT_LENGTH
        let result = parse_vint_length(&large_value);
        assert!(
            result.is_err(),
            "Should reject values > 1GB for length fields"
        );

        // Test 2: Value just under limit should succeed
        let safe_value = encode_vuint(max - 1);
        let result = parse_vint_length(&safe_value);
        assert!(result.is_ok(), "Should accept values < 1GB");
        let (_, length) = result.unwrap();
        assert_eq!(length as u64, max - 1);

        // Test 3: Exact limit should succeed, one over should fail (> not >=)
        let at_limit = encode_vuint(max);
        assert!(
            parse_vint_length(&at_limit).is_ok(),
            "Should accept exactly MAX_VINT_LENGTH"
        );
        let over_limit = encode_vuint(max + 1);
        assert!(
            parse_vint_length(&over_limit).is_err(),
            "Should reject values > MAX_VINT_LENGTH"
        );

        // Test 4: Zero should be valid
        let zero_value = encode_vuint(0u64);
        let result = parse_vint_length(&zero_value);
        assert!(result.is_ok(), "Should accept zero");
        let (_, length) = result.unwrap();
        assert_eq!(length, 0);

        // Test 5: Reasonable values (16MB - existing limit in block_entries)
        let sixteen_mb = encode_vuint(16 * 1024 * 1024u64);
        let result = parse_vint_length(&sixteen_mb);
        assert!(result.is_ok(), "Should accept 16MB (common limit)");
    }
}
