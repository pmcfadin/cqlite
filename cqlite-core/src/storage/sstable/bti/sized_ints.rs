//! SizedInts - Variable-length integer encoding for BTI payloads
//!
//! This module implements Cassandra's SizedInts encoding, used for storing
//! partition offsets in BTI index payloads. The encoding uses 1-8 bytes
//! in big-endian format, with the size determined by the magnitude of the value.
//!
//! Reference: `org.apache.cassandra.io.util.SizedInts` in Cassandra 5.0.0

use crate::error::Error;
use std::io::Read;

/// Calculate the number of bytes needed to store a value
///
/// Matches `SizedInts.nonZeroSize()` from Cassandra:
/// ```java
/// public static int nonZeroSize(long value) {
///     if (value < 0)
///         value = ~value;
///     int lz = Long.numberOfLeadingZeros(value);
///     return (64 - lz + 1 + 7) / 8;  // At least 1, at most 8
/// }
/// ```
pub fn non_zero_size(value: i64) -> usize {
    let abs_value = if value < 0 { !value } else { value } as u64;

    if abs_value == 0 {
        return 1;
    }

    let leading_zeros = abs_value.leading_zeros();
    let significant_bits = 64 - leading_zeros as usize;

    // +1 for sign bit, round up to bytes
    (significant_bits + 1).div_ceil(8)
}

/// Read a variable-length signed integer
///
/// Matches `SizedInts.read()` from Cassandra.
/// All values are stored in big-endian format.
///
/// # Arguments
/// * `reader` - Input reader positioned at the start of the integer
/// * `bytes` - Number of bytes to read (1-8)
///
/// # Returns
/// Signed 64-bit integer value
pub fn read<R: Read>(reader: &mut R, bytes: usize) -> Result<i64, Error> {
    match bytes {
        0 => Ok(0),
        1 => {
            let mut buf = [0u8; 1];
            reader.read_exact(&mut buf)?;
            Ok(buf[0] as i8 as i64)
        }
        2 => {
            let mut buf = [0u8; 2];
            reader.read_exact(&mut buf)?;
            Ok(i16::from_be_bytes(buf) as i64)
        }
        3 => {
            let mut buf = [0u8; 3];
            reader.read_exact(&mut buf)?;
            let high = buf[0] as i8 as i64;
            let low = u16::from_be_bytes([buf[1], buf[2]]) as i64;
            Ok((high << 16) | low)
        }
        4 => {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf)?;
            Ok(i32::from_be_bytes(buf) as i64)
        }
        5 => {
            let mut buf = [0u8; 5];
            reader.read_exact(&mut buf)?;
            let high = buf[0] as i8 as i64;
            let low = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as i64;
            Ok((high << 32) | low)
        }
        6 => {
            let mut buf = [0u8; 6];
            reader.read_exact(&mut buf)?;
            let high = i16::from_be_bytes([buf[0], buf[1]]) as i64;
            let low = u32::from_be_bytes([buf[2], buf[3], buf[4], buf[5]]) as i64;
            Ok((high << 32) | low)
        }
        7 => {
            let mut buf = [0u8; 7];
            reader.read_exact(&mut buf)?;
            let high1 = buf[0] as i8 as i64;
            let high2 = u16::from_be_bytes([buf[1], buf[2]]) as i64;
            let low = u32::from_be_bytes([buf[3], buf[4], buf[5], buf[6]]) as i64;
            Ok((high1 << 48) | (high2 << 32) | low)
        }
        8 => {
            let mut buf = [0u8; 8];
            reader.read_exact(&mut buf)?;
            Ok(i64::from_be_bytes(buf))
        }
        _ => Err(Error::Parse(format!(
            "Invalid SizedInts byte count: {} (expected 0-8)",
            bytes
        ))),
    }
}

/// Read an unsigned variable-length integer
///
/// Matches `SizedInts.readUnsigned()` from Cassandra.
pub fn read_unsigned<R: Read>(reader: &mut R, bytes: usize) -> Result<u64, Error> {
    if bytes == 8 {
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf)?;
        Ok(u64::from_be_bytes(buf))
    } else {
        let signed = read(reader, bytes)?;
        let mask = if bytes == 0 {
            0
        } else {
            (1u64 << (bytes * 8)) - 1
        };
        Ok(signed as u64 & mask)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_non_zero_size() {
        assert_eq!(non_zero_size(0), 1);
        assert_eq!(non_zero_size(127), 1);
        assert_eq!(non_zero_size(128), 2);
        assert_eq!(non_zero_size(255), 2);
        assert_eq!(non_zero_size(256), 2);
        assert_eq!(non_zero_size(32767), 2);
        assert_eq!(non_zero_size(32768), 3);
        assert_eq!(non_zero_size(0x7FFFFFFF), 4);
        assert_eq!(non_zero_size(0x80000000), 5);
        assert_eq!(non_zero_size(0x7FFFFFFFFFFF), 6);
        assert_eq!(non_zero_size(0x7FFFFFFFFFFFFF), 7);
        assert_eq!(non_zero_size(i64::MAX), 8);
    }

    #[test]
    fn test_read_1_byte() {
        let data = vec![0x7F];
        let mut cursor = Cursor::new(data);
        assert_eq!(read(&mut cursor, 1).unwrap(), 127);
    }

    #[test]
    fn test_read_2_bytes() {
        let data = vec![0x01, 0x00];
        let mut cursor = Cursor::new(data);
        assert_eq!(read(&mut cursor, 2).unwrap(), 256);
    }

    #[test]
    fn test_read_3_bytes() {
        let data = vec![0x00, 0x04, 0x80];
        let mut cursor = Cursor::new(data);
        assert_eq!(read(&mut cursor, 3).unwrap(), 0x0480);
    }

    #[test]
    fn test_read_4_bytes() {
        // Example from BTI payload: 0x00048000 = 294,912
        let data = vec![0x00, 0x04, 0x80, 0x00];
        let mut cursor = Cursor::new(data);
        assert_eq!(read(&mut cursor, 4).unwrap(), 294912);
    }

    #[test]
    fn test_read_5_bytes() {
        let data = vec![0x01, 0x00, 0x00, 0x00, 0x00];
        let mut cursor = Cursor::new(data);
        assert_eq!(read(&mut cursor, 5).unwrap(), 0x0100000000);
    }

    #[test]
    fn test_read_6_bytes() {
        // 6 bytes big-endian: [0x00, 0x01, 0x00, 0x00, 0x00, 0x00]
        // = 0x000100000000 = 4,294,967,296
        let data = vec![0x00, 0x01, 0x00, 0x00, 0x00, 0x00];
        let mut cursor = Cursor::new(data);
        assert_eq!(read(&mut cursor, 6).unwrap(), 0x000100000000);
    }

    #[test]
    fn test_read_7_bytes() {
        // 7 bytes big-endian: [0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00]
        // = 0x00000100000000 = 4,294,967,296
        let data = vec![0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00];
        let mut cursor = Cursor::new(data);
        assert_eq!(read(&mut cursor, 7).unwrap(), 0x00000100000000);
    }

    #[test]
    fn test_read_8_bytes() {
        let data = vec![0x00, 0x00, 0x00, 0x00, 0x7F, 0xFF, 0xFF, 0xFF];
        let mut cursor = Cursor::new(data);
        assert_eq!(read(&mut cursor, 8).unwrap(), 0x7FFFFFFF);
    }

    #[test]
    fn test_read_negative() {
        // -1 in two's complement (i16)
        let data = vec![0xFF, 0xFF];
        let mut cursor = Cursor::new(data);
        assert_eq!(read(&mut cursor, 2).unwrap(), -1);
    }

    #[test]
    fn test_read_unsigned() {
        let data = vec![0xFF, 0xFF];
        let mut cursor = Cursor::new(data);
        assert_eq!(read_unsigned(&mut cursor, 2).unwrap(), 0xFFFF);
    }

    #[test]
    fn test_read_zero_bytes() {
        let data = vec![];
        let mut cursor = Cursor::new(data);
        assert_eq!(read(&mut cursor, 0).unwrap(), 0);
    }

    #[test]
    fn test_invalid_byte_count() {
        let data = vec![0xFF; 9];
        let mut cursor = Cursor::new(data);
        assert!(read(&mut cursor, 9).is_err());
    }

    #[test]
    fn test_real_world_example() {
        // From BTI payload analysis: payloadBits=11, size=4
        // Hash byte: 0x00
        // Position bytes: 0x00 0x04 0x80 0x00
        let payload_data = vec![0x00, 0x00, 0x04, 0x80, 0x00];
        let mut cursor = Cursor::new(payload_data);

        let mut hash_buf = [0u8; 1];
        cursor.read_exact(&mut hash_buf).unwrap();
        assert_eq!(hash_buf[0], 0x00);

        let position = read(&mut cursor, 4).unwrap();
        assert_eq!(position, 294912); // ~295 KB
    }
}
