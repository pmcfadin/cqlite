//! VInt (Variable-length integer) encoder for Cassandra compatibility
//!
//! Implements byte-identical encoding to Cassandra's VIntCoding.java.
//! All SSTable component writers depend on this producing correct output.
//!
//! Encoding format:
//! - Single byte: values 0-127 (unsigned) or -64 to 63 (signed)
//! - Multi-byte: first byte encodes length via leading 1-bits, remaining bytes are big-endian
//! - Signed values use ZigZag encoding: 0→0, -1→1, 1→2, -2→3, etc.
//!
//! References:
//! - Cassandra VIntCoding.java: org.apache.cassandra.utils.vint.VIntCoding
//! - Appendix B: docs/sstables-definitive-guide/chapters/appendix-b-encodings-cheat-sheet.md

/// ZigZag encode a signed integer to unsigned
///
/// Maps signed integers to unsigned so that small absolute values
/// have small encodings: 0→0, -1→1, 1→2, -2→3, 2→4, -3→5, ...
///
/// Formula: (n << 1) ^ (n >> 63)
/// - For positive n: left shift gives 2n, right shift gives 0, so result is 2n
/// - For negative n: left shift gives 2n, right shift gives -1 (all 1s), so XOR inverts
#[inline]
fn zigzag_encode(n: i64) -> u64 {
    ((n << 1) ^ (n >> 63)) as u64
}

/// Encode a signed i64 to VInt bytes (Cassandra-compatible)
///
/// Uses ZigZag encoding to efficiently encode small negative numbers.
///
/// # Arguments
///
/// * `value` - Signed 64-bit integer to encode
/// * `buf` - Target buffer to write VInt bytes
///
/// # Examples
///
/// ```
/// # use cqlite_core::storage::serialization::vint::encode_signed;
/// let mut buf = Vec::new();
/// encode_signed(0, &mut buf);
/// assert_eq!(buf, vec![0x00]); // Single-byte zero
///
/// buf.clear();
/// encode_signed(-1, &mut buf);
/// assert_eq!(buf, vec![0x01]); // ZigZag encoded -1
///
/// buf.clear();
/// encode_signed(64, &mut buf);
/// assert_eq!(buf, vec![0x80, 0x80]); // Two-byte format
/// ```
pub fn encode_signed(value: i64, buf: &mut Vec<u8>) {
    let unsigned_value = zigzag_encode(value);
    encode_unsigned(unsigned_value, buf);
}

/// Encode an unsigned u64 to VInt bytes
///
/// Encoding format (standard Cassandra unsigned VInt):
/// - 1 byte: 0xxxxxxx (values 0-127)
/// - 2 bytes: 10xxxxxx xxxxxxxx (values 128-16383, 14 bits total: 6+8)
/// - 3 bytes: 110xxxxx xxxxxxxx xxxxxxxx (values 16384-2097151, 21 bits: 5+8+8)
/// - ... up to 9 bytes total
///
/// The first byte encodes the number of extra bytes via leading 1-bits,
/// followed by a 0 separator bit, then data bits.
///
/// # Arguments
///
/// * `value` - Unsigned 64-bit integer to encode
/// * `buf` - Target buffer to write VInt bytes
///
/// # Examples
///
/// ```
/// # use cqlite_core::storage::serialization::vint::encode_unsigned;
/// let mut buf = Vec::new();
/// encode_unsigned(0, &mut buf);
/// assert_eq!(buf, vec![0x00]);
///
/// buf.clear();
/// encode_unsigned(127, &mut buf);
/// assert_eq!(buf, vec![0x7F]);
///
/// buf.clear();
/// encode_unsigned(128, &mut buf);
/// assert_eq!(buf, vec![0x80, 0x80]); // 10xxxxxx xxxxxxxx
/// ```
pub fn encode_unsigned(value: u64, buf: &mut Vec<u8>) {
    let size = unsigned_len_value(value);

    if size == 1 {
        // Single byte: 0xxxxxxx (values 0-127)
        buf.push(value as u8);
    } else if size == 9 {
        // 9 bytes: 0xFF followed by full 8-byte long
        buf.push(0xFF);
        buf.extend_from_slice(&value.to_be_bytes());
    } else {
        // Multi-byte (2-8 bytes): [leading 1s][0][data bits][remaining bytes]
        let extra_bytes = size - 1;

        // Create first byte with leading 1-bits pattern
        // For 2 bytes (1 extra): 0x80 | data_bits (10xxxxxx)
        // For 3 bytes (2 extra): 0xC0 | data_bits (110xxxxx)
        let mask = encode_extra_bytes_to_read(extra_bytes);

        // Calculate how many data bits fit in the first byte
        let first_byte_data_bits = 8 - extra_bytes - 1;

        // Extract data bits for first byte
        let shift = extra_bytes * 8;
        let first_byte_data = ((value >> shift) & ((1 << first_byte_data_bits) - 1)) as u8;
        buf.push(mask | first_byte_data);

        // Add remaining bytes in big-endian order
        for i in (0..extra_bytes).rev() {
            buf.push(((value >> (i * 8)) & 0xFF) as u8);
        }
    }
}

/// Calculate the encoded length of a signed i64
///
/// Returns the number of bytes that would be needed to encode this value.
///
/// # Examples
///
/// ```
/// # use cqlite_core::storage::serialization::vint::signed_len;
/// assert_eq!(signed_len(0), 1);
/// assert_eq!(signed_len(63), 1);
/// assert_eq!(signed_len(-64), 1);
/// assert_eq!(signed_len(64), 2);
/// assert_eq!(signed_len(-65), 2);
/// ```
#[inline]
pub fn signed_len(value: i64) -> usize {
    let unsigned_value = zigzag_encode(value);
    unsigned_len_value(unsigned_value)
}

/// Calculate the encoded length of an unsigned u64
///
/// Returns the number of bytes that would be needed to encode this value.
///
/// # Examples
///
/// ```
/// # use cqlite_core::storage::serialization::vint::unsigned_len;
/// assert_eq!(unsigned_len(0), 1);
/// assert_eq!(unsigned_len(127), 1);
/// assert_eq!(unsigned_len(128), 2);
/// assert_eq!(unsigned_len(16384), 3);
/// ```
#[inline]
pub fn unsigned_len(value: u64) -> usize {
    unsigned_len_value(value)
}

/// Compute the number of bytes needed to encode an unsigned VInt
///
/// Matches Cassandra's computeUnsignedVIntSize algorithm:
/// Uses bit manipulation to calculate size based on the number of leading zeros.
///
/// Formula: (639 - magnitude * 9) >> 6
/// where magnitude = numberOfLeadingZeros(value | 1)
#[inline]
fn unsigned_len_value(value: u64) -> usize {
    // | with 1 ensures magnitude <= 63, so (63 - 1) / 7 <= 8
    let magnitude = (value | 1).leading_zeros();
    // Hand-picked formula from Cassandra that matches: 9 - ((magnitude - 1) / 7)
    ((639 - (magnitude * 9)) >> 6) as usize
}

/// Encode the number of extra bytes to read in the first byte
///
/// For a VInt with N extra bytes, this returns the bit pattern
/// with N leading 1-bits followed by a 0 separator.
///
/// Examples:
/// - 0 extra bytes: 0x00 (00000000)
/// - 1 extra byte:  0x80 (10000000)
/// - 2 extra bytes: 0xC0 (11000000)
/// - 3 extra bytes: 0xE0 (11100000)
#[inline]
fn encode_extra_bytes_to_read(extra_bytes: usize) -> u8 {
    if extra_bytes == 0 {
        0x00
    } else if extra_bytes >= 8 {
        0xFF
    } else {
        // Generate mask with N leading 1-bits followed by 0s
        // For 1 extra byte: 0x80 (10000000)
        // For 2 extra bytes: 0xC0 (11000000)
        // Formula: ~((1 << (8 - extra_bytes)) - 1)
        let shift = 8 - extra_bytes;
        0xFF_u8 << shift
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zigzag_encode() {
        assert_eq!(zigzag_encode(0), 0);
        assert_eq!(zigzag_encode(-1), 1);
        assert_eq!(zigzag_encode(1), 2);
        assert_eq!(zigzag_encode(-2), 3);
        assert_eq!(zigzag_encode(2), 4);
        assert_eq!(zigzag_encode(-3), 5);
        assert_eq!(zigzag_encode(63), 126);
        assert_eq!(zigzag_encode(-64), 127);
        assert_eq!(zigzag_encode(64), 128);
    }

    #[test]
    fn test_encode_signed_test_vectors() {
        // Test vectors from Issue #363
        let test_cases = vec![
            (0i64, vec![0x00]),
            (1i64, vec![0x02]),
            (-1i64, vec![0x01]),
            (63i64, vec![0x7E]),
            (-64i64, vec![0x7F]),
            (64i64, vec![0x80, 0x80]),
        ];

        for (value, expected) in test_cases {
            let mut buf = Vec::new();
            encode_signed(value, &mut buf);
            assert_eq!(
                buf, expected,
                "encode_signed({}) failed: expected {:?}, got {:?}",
                value, expected, buf
            );
        }
    }

    #[test]
    fn test_encode_unsigned_boundaries() {
        let test_cases = vec![
            (0u64, vec![0x00]),
            (127u64, vec![0x7F]),               // Max single byte
            (128u64, vec![0x80, 0x80]),         // Min two bytes
            (16383u64, vec![0xBF, 0xFF]),       // Max two bytes (14 bits: 6+8)
            (16384u64, vec![0xC0, 0x40, 0x00]), // Min three bytes
        ];

        for (value, expected) in test_cases {
            let mut buf = Vec::new();
            encode_unsigned(value, &mut buf);
            assert_eq!(
                buf, expected,
                "encode_unsigned({}) failed: expected {:?}, got {:?}",
                value, expected, buf
            );
        }
    }

    #[test]
    fn test_signed_len() {
        assert_eq!(signed_len(0), 1);
        assert_eq!(signed_len(1), 1);
        assert_eq!(signed_len(-1), 1);
        assert_eq!(signed_len(63), 1);
        assert_eq!(signed_len(-64), 1);
        assert_eq!(signed_len(64), 2);
        assert_eq!(signed_len(-65), 2);
        assert_eq!(signed_len(127), 2);
        assert_eq!(signed_len(-128), 2);
    }

    #[test]
    fn test_unsigned_len() {
        assert_eq!(unsigned_len(0), 1);
        assert_eq!(unsigned_len(127), 1);
        assert_eq!(unsigned_len(128), 2);
        assert_eq!(unsigned_len(16383), 2);
        assert_eq!(unsigned_len(16384), 3);
        assert_eq!(unsigned_len(2097151), 3);
        assert_eq!(unsigned_len(2097152), 4);
    }

    #[test]
    fn test_encode_extra_bytes() {
        assert_eq!(encode_extra_bytes_to_read(0), 0x00);
        assert_eq!(encode_extra_bytes_to_read(1), 0x80);
        assert_eq!(encode_extra_bytes_to_read(2), 0xC0);
        assert_eq!(encode_extra_bytes_to_read(3), 0xE0);
        assert_eq!(encode_extra_bytes_to_read(4), 0xF0);
        assert_eq!(encode_extra_bytes_to_read(5), 0xF8);
        assert_eq!(encode_extra_bytes_to_read(6), 0xFC);
        assert_eq!(encode_extra_bytes_to_read(7), 0xFE);
        assert_eq!(encode_extra_bytes_to_read(8), 0xFF);
    }

    #[test]
    fn test_roundtrip_with_decoder() {
        // Test roundtrip encode → decode using standard Cassandra VInt format
        use crate::parser::vint::parse_vint;

        let test_values = vec![
            0,
            1,
            -1,
            63,
            -64,
            64,
            -65,
            127,
            -128,
            255,
            -255,
            1000,
            -1000,
            32767,
            -32768,
            1048576,
            -1048576,
            i32::MAX as i64,
            i32::MIN as i64,
        ];

        for value in test_values {
            // Test our encoder
            let mut buf = Vec::new();
            encode_signed(value, &mut buf);

            // Test decoder can parse it
            let (remaining, decoded) = parse_vint(&buf).unwrap();
            assert!(
                remaining.is_empty(),
                "Decoder should consume all bytes for value {}",
                value
            );
            assert_eq!(
                decoded, value,
                "Roundtrip failed for value {}: encoded as {:?}",
                value, buf
            );
        }
    }

    #[test]
    fn test_large_values() {
        // Test values requiring different byte sizes
        let test_cases = vec![
            (1u64 << 7, 2),  // 128 - 2 bytes
            (1u64 << 14, 3), // 16384 - 3 bytes
            (1u64 << 21, 4), // 2097152 - 4 bytes
            (1u64 << 28, 5), // 268435456 - 5 bytes
            (1u64 << 35, 6), // 6 bytes
            (1u64 << 42, 7), // 7 bytes
            (1u64 << 49, 8), // 8 bytes
            (1u64 << 56, 9), // 9 bytes
        ];

        for (value, expected_size) in test_cases {
            assert_eq!(
                unsigned_len(value),
                expected_size,
                "Value {} should encode to {} bytes",
                value,
                expected_size
            );

            let mut buf = Vec::new();
            encode_unsigned(value, &mut buf);
            assert_eq!(
                buf.len(),
                expected_size,
                "Encoded {} to {:?}, expected {} bytes",
                value,
                buf,
                expected_size
            );
        }
    }

    #[test]
    fn test_performance_target() {
        // Performance target: <100ns per encode
        // This is a rough check that we're not doing anything obviously slow
        use std::time::Instant;

        let iterations = 10_000;
        let mut buf = Vec::with_capacity(9);

        let start = Instant::now();
        for i in 0..iterations {
            buf.clear();
            encode_signed(i, &mut buf);
        }
        let elapsed = start.elapsed();

        let avg_ns = elapsed.as_nanos() / iterations as u128;
        println!("Average encode time: {} ns", avg_ns);

        // Sanity check: should be much faster than 1µs per encode
        assert!(
            avg_ns < 1000,
            "Encoding is too slow: {} ns per operation",
            avg_ns
        );
    }

    #[test]
    fn test_cassandra_compatibility() {
        // Test specific patterns from standard Cassandra VInt encoding
        // These use ZigZag encoding: signed → unsigned → VInt bytes
        let test_cases = vec![
            (0i64, vec![0x00], "Zero value"), // zigzag(0)=0, vint(0)=[0x00]
            (1i64, vec![0x02], "Single byte positive"), // zigzag(1)=2, vint(2)=[0x02]
            (63i64, vec![0x7E], "Maximum single byte positive"), // zigzag(63)=126, vint(126)=[0x7E]
            (64i64, vec![0x80, 0x80], "Two byte encoding start"), // zigzag(64)=128, vint(128)=[0x80,0x80]
            (127i64, vec![0x80, 0xFE], "Two byte positive"), // zigzag(127)=254, vint(254)=[0x80,0xFE]
            (-1i64, vec![0x01], "Single byte negative"),     // zigzag(-1)=1, vint(1)=[0x01]
            (-64i64, vec![0x7F], "Single byte negative boundary"), // zigzag(-64)=127, vint(127)=[0x7F]
            (-65i64, vec![0x80, 0x81], "Two byte negative"), // zigzag(-65)=129, vint(129)=[0x80,0x81]
        ];

        for (value, expected_bytes, description) in test_cases {
            let mut buf = Vec::new();
            encode_signed(value, &mut buf);
            assert_eq!(
                buf, expected_bytes,
                "{}: encode_signed({}) failed: expected {:?}, got {:?}",
                description, value, expected_bytes, buf
            );
        }
    }
}
