//! Tombstone (deletion) value decoding.
//!
//! Decodes the legacy CQL binary tombstone encoding into `Value::Tombstone`.

use super::super::vint::parse_vint_length;
use crate::types::{RowKey, TombstoneInfo, TombstoneType, Value};
use nom::{
    bytes::complete::take,
    number::complete::{be_i64, be_u8},
    IResult,
};

/// Parse tombstone information with enhanced Cassandra 5.0 compatibility
pub fn parse_tombstone(input: &[u8]) -> IResult<&[u8], Value> {
    // Parse deletion timestamp (microseconds since epoch)
    let (input, deletion_time) = be_i64(input)?;

    // Parse tombstone type byte
    let (input, tombstone_type_byte) = be_u8(input)?;

    let tombstone_type = match tombstone_type_byte {
        0 => TombstoneType::RowTombstone,
        1 => TombstoneType::CellTombstone,
        2 => TombstoneType::RangeTombstone,
        3 => TombstoneType::TtlExpiration,
        _ => {
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
        }
    };

    // Parse optional TTL for TTL-based tombstones
    let (input, ttl) = if tombstone_type == TombstoneType::TtlExpiration {
        let (input, ttl_value) = be_i64(input)?;
        (input, Some(ttl_value))
    } else {
        (input, None)
    };

    // Parse optional clustering key range for range tombstones
    let (input, range_start, range_end) = if tombstone_type == TombstoneType::RangeTombstone {
        let (input, has_range) = be_u8(input)?;
        if has_range != 0 {
            let (input, start_len) = parse_vint_length(input)?;
            let (input, start_data) = take(start_len)(input)?;
            let (input, end_len) = parse_vint_length(input)?;
            let (input, end_data) = take(end_len)(input)?;
            (input, Some(start_data.to_vec()), Some(end_data.to_vec()))
        } else {
            (input, None, None)
        }
    } else {
        (input, None, None)
    };

    let tombstone_info = TombstoneInfo {
        deletion_time,
        tombstone_type,
        // The legacy CQL binary tombstone encoding carries no localDeletionTime
        // (only deletion_time); default to 0 (#873).
        local_deletion_time: 0,
        ttl,
        range_start: range_start.map(RowKey::new),
        range_end: range_end.map(RowKey::new),
    };

    Ok((input, Value::Tombstone(tombstone_info)))
}

#[cfg(test)]
mod tests {
    use super::super::{serialize_cql_value, CqlTypeId};
    use super::*;

    #[test]
    fn test_tombstone_parsing() {
        // Test row tombstone parsing
        let row_tombstone = Value::row_tombstone(1000);
        let serialized = serialize_cql_value(&row_tombstone).unwrap();

        // Parse it back
        let (remaining, parsed_value) = parse_tombstone(&serialized[1..]).unwrap(); // Skip type ID
        assert!(remaining.is_empty());
        assert_eq!(parsed_value, row_tombstone);

        // Test TTL tombstone parsing
        let ttl_tombstone = Value::ttl_tombstone(2000, 1000);
        let serialized_ttl = serialize_cql_value(&ttl_tombstone).unwrap();

        let (remaining, parsed_ttl) = parse_tombstone(&serialized_ttl[1..]).unwrap(); // Skip type ID
        assert!(remaining.is_empty());
        assert_eq!(parsed_ttl, ttl_tombstone);
    }

    #[test]
    fn test_tombstone_serialization() {
        // Test tombstone serialization
        let tombstone = Value::cell_tombstone(5000);
        let serialized = serialize_cql_value(&tombstone).unwrap();
        assert!(!serialized.is_empty());

        // Should start with Tombstone type ID
        assert_eq!(serialized[0], CqlTypeId::Tombstone as u8);

        // Should contain deletion time
        let deletion_time_bytes = &serialized[1..9];
        let deletion_time = i64::from_be_bytes([
            deletion_time_bytes[0],
            deletion_time_bytes[1],
            deletion_time_bytes[2],
            deletion_time_bytes[3],
            deletion_time_bytes[4],
            deletion_time_bytes[5],
            deletion_time_bytes[6],
            deletion_time_bytes[7],
        ]);
        assert_eq!(deletion_time, 5000);
    }

    #[test]
    fn test_partition_tombstone_serialize_errors() {
        // PartitionTombstone has no encoding in the legacy CQL binary format.
        // Serializing one must return an explicit error, not silently alias to
        // RowTombstone (byte 0) — that would be a silent lossy round-trip.
        use crate::types::{TombstoneInfo, TombstoneType};
        let partition_tombstone = Value::Tombstone(TombstoneInfo {
            deletion_time: 9999,
            tombstone_type: TombstoneType::PartitionTombstone,
            local_deletion_time: 0,
            ttl: None,
            range_start: None,
            range_end: None,
        });
        let result = serialize_cql_value(&partition_tombstone);
        assert!(
            result.is_err(),
            "serializing PartitionTombstone must return Err, not Ok"
        );
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("PartitionTombstone"),
            "error message should mention PartitionTombstone, got: {err_msg}"
        );
    }
}
