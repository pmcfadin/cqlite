//! EncodingStats decoding from the SERIALIZATION_HEADER component of Statistics.db.
//!
//! EncodingStats are three epoch-relative unsigned VInt deltas (minTimestamp,
//! minLocalDeletionTime, minTTL) that prefix the SerializationHeader. They are
//! the baseline values V5CompressedLegacy needs for delta-coded timestamps/TTLs.
//! This module decodes those three fields and then defers to
//! [`super::serialization_header`] for the schema that follows.

use super::super::vint::parse_vuint;
use super::super::vint_narrow::take_vuint_length;
use super::marshal_type::build_column_infos;
use super::serialization_header::{parse_serialization_header, parse_serialization_header_schema};
use super::EncodingStatsResult;
use crate::storage::sstable::version_gate::VersionGates;
use nom::{number::complete::be_u32, IResult};

/// Epoch constants matching Cassandra's EncodingStats.java (EncodingStats.Serializer)
/// Used for delta-encoding/decoding EncodingStats fields in Statistics.db SERIALIZATION_HEADER.
/// Cassandra serializes: writeUnsignedVInt(value - EPOCH)
/// Cassandra deserializes: readUnsignedVInt() + EPOCH
const TIMESTAMP_EPOCH: i64 = 1_442_880_000_000_000; // Sept 22, 2015 00:00:00 UTC in microseconds
const DELETION_TIME_EPOCH: i64 = 1_442_880_000; // Sept 22, 2015 00:00:00 UTC in seconds
                                                // TTL epoch is 0 in Cassandra, but kept for consistency with the delta-encoding pattern
const TTL_EPOCH: i64 = 0;

/// Parse minimal EncodingStats section from nb-format Statistics.db
///
/// Returns: (min_timestamp, min_deletion_time, min_ttl, partition_keys, clustering_keys, columns)
///
/// # Arguments
/// * `input` - The data starting at the STATS component
/// * `full_input` - The complete Statistics.db content (needed for TOC-based HEADER lookup)
/// * `header_offset` - Optional offset to SerializationHeader from TOC (Issue #216)
/// * `gates` - Optional VersionGates for VG3 version-sensitive decoding decisions.
///   Pass `None` from standalone tools/tests to use nb-compatible defaults.
pub(super) fn parse_minimal_encoding_stats<'a>(
    input: &'a [u8],
    full_input: &'a [u8],
    header_offset: Option<usize>,
    gates: Option<&VersionGates>,
) -> IResult<&'a [u8], EncodingStatsResult> {
    // The SERIALIZATION_HEADER component (type 3) starts with EncodingStats:
    //   [vuint minTimestamp_delta] [vuint minLocalDeletionTime_delta] [vuint minTTL_delta]
    // These are unsigned VInt deltas from epoch constants (see EncodingStats.Serializer).
    // Use the TOC-based offset to read from the correct location.

    let Some(offset) = header_offset else {
        tracing::debug!("No HEADER TOC offset, using fallback EncodingStats parsing");
        return parse_encoding_stats_fallback(input, gates);
    };

    if offset >= full_input.len() {
        tracing::warn!(
            "TOC offset 0x{:x} exceeds input length {}, using fallback",
            offset,
            full_input.len()
        );
        return parse_encoding_stats_fallback(input, gates);
    }

    let header_data = &full_input[offset..];
    tracing::debug!(
        "Parsing EncodingStats + SerializationHeader at TOC offset 0x{:x} ({} bytes available)",
        offset,
        header_data.len()
    );

    // Parse EncodingStats (3 unsigned VInts at start of SERIALIZATION_HEADER)
    let (rest, (min_timestamp, min_deletion_time, min_ttl)) =
        parse_encoding_stats_vuints(header_data, gates)?;

    tracing::debug!(
        "EncodingStats from HEADER: min_timestamp={}, min_deletion_time={}, min_ttl={:?}",
        min_timestamp,
        min_deletion_time,
        min_ttl
    );

    // Parse the rest of the SerializationHeader (schema info)
    let (partition_types, clustering_types, columns) = match parse_serialization_header_schema(rest)
    {
        Ok((_, result)) => result,
        Err(e) => {
            tracing::warn!(
                "Schema parsing after EncodingStats failed: {:?}, falling back to marker search",
                e
            );
            parse_serialization_header(input)?.1
        }
    };

    // Gate 2 of #4104: the key comparators reach `build_column_infos` as RAW
    // marshal strings (the DESC `ReversedType(..)` signal is derived there), so
    // this is where a frozen-scalar KEY type is refused. Fail-closed.
    let (partition_key_columns, clustering_key_columns) =
        match build_column_infos(&partition_types, &clustering_types) {
            Ok(cols) => cols,
            Err(e) => {
                tracing::error!("Refusing SerializationHeader key types: {e}");
                return Err(nom::Err::Error(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Verify,
                )));
            }
        };

    Ok((
        input,
        (
            min_timestamp,
            min_deletion_time,
            min_ttl,
            partition_key_columns,
            clustering_key_columns,
            columns,
        ),
    ))
}

/// Parse 3 EncodingStats unsigned VInt deltas and convert to absolute values by adding epochs.
/// Returns (min_timestamp, min_deletion_time, min_ttl).
///
/// # VG3 authority note
///
/// The `EncodingStats.Serializer` (EncodingStats.java:274-276) uses the SAME unsigned-VInt
/// + epoch-offset format for **both** `nb` and `oa`:
///
/// ```text
/// out.writeUnsignedVInt(stats.minTimestamp - TIMESTAMP_EPOCH)
/// out.writeUnsignedVInt32((int)(stats.minLocalDeletionTime - DELETION_TIME_EPOCH))
/// out.writeUnsignedVInt32(stats.minTTL - TTL_EPOCH)
/// ```
///
/// The `hasUIntDeletionTime` gate (BigFormat.java:409) affects only the **StatsMetadata**
/// (STATS component in Statistics.db), not the SerializationHeader component where
/// EncodingStats lives.  The epoch-relative decoding here is correct for both nb and oa.
/// `gates` is accepted (not consumed) for API completeness; `None` is fine too.
fn parse_encoding_stats_vuints<'a>(
    input: &'a [u8],
    // VG3: gates threaded here for authority completeness.
    // Authority investigation: the EncodingStats.Serializer (EncodingStats.java:274-276)
    // uses the SAME unsigned VInt + epoch format for both nb and oa:
    //   out.writeUnsignedVInt(stats.minTimestamp - TIMESTAMP_EPOCH)
    //   out.writeUnsignedVInt32((int)(stats.minLocalDeletionTime - DELETION_TIME_EPOCH))
    //   out.writeUnsignedVInt32(stats.minTTL - TTL_EPOCH)
    // The `hasUIntDeletionTime` gate (BigFormat.java:409) affects ONLY the
    // StatsMetadata section (Statistics.db STATS component), NOT the
    // SerializationHeader component where EncodingStats lives.  No decode
    // difference applies here.  Gates accepted but not consumed.
    _gates: Option<&VersionGates>,
) -> IResult<&'a [u8], (i64, i64, Option<i64>)> {
    let (rest, min_ts_delta) = parse_vuint(input)?;
    let (rest, min_ldt_delta) = parse_vuint(rest)?;
    let (rest, min_ttl_delta) = parse_vuint(rest)?;

    Ok((
        rest,
        (
            min_ts_delta as i64 + TIMESTAMP_EPOCH,
            // EncodingStats.java:289: `long minLocalDeletionTime = in.readUnsignedVInt32() + DELETION_TIME_EPOCH`
            // Same formula for nb and oa — DELETION_TIME_EPOCH is always added back.
            min_ldt_delta as i64 + DELETION_TIME_EPOCH,
            Some(min_ttl_delta as i64 + TTL_EPOCH),
        ),
    ))
}

/// Fallback EncodingStats parser for when no TOC HEADER offset is available.
/// Uses ad-hoc parsing from the data following the file header.
fn parse_encoding_stats_fallback<'a>(
    input: &'a [u8],
    gates: Option<&VersionGates>,
) -> IResult<&'a [u8], EncodingStatsResult> {
    // Skip metadata_type (u32 BE) at start of data section
    let (rest, _metadata_type) = be_u32(input)?;

    // Parse data section length (VInt)
    let (rest, _data_length) = parse_vuint(rest)?;

    // Parse partitioner string length (VInt)
    let (rest, partitioner_len) = parse_vuint(rest)?;

    // Skip partitioner string. #3848: `take_vuint_length` narrows the raw `u64`
    // with a checked conversion — `take(partitioner_len as usize)` would accept a
    // declared 4 GiB length as an empty run on a 32-bit target.
    let (rest, _) = take_vuint_length(partitioner_len)(rest)?;

    // Skip additional metadata (observed: ~2 VInts before timestamp fields)
    let (rest, _metadata1) = parse_vuint(rest)?;
    let (rest, _metadata2) = parse_vuint(rest)?;

    // Parse EncodingStats fields (unsigned VInt deltas from epoch)
    let (rest, (min_timestamp, min_deletion_time, min_ttl)) =
        parse_encoding_stats_vuints(rest, gates)?;

    // Fall back to marker-based header search for schema
    let (_, (partition_types, clustering_types, columns)) = parse_serialization_header(rest)?;

    // Gate 2 of #4104: the key comparators reach `build_column_infos` as RAW
    // marshal strings (the DESC `ReversedType(..)` signal is derived there), so
    // this is where a frozen-scalar KEY type is refused. Fail-closed.
    let (partition_key_columns, clustering_key_columns) =
        match build_column_infos(&partition_types, &clustering_types) {
            Ok(cols) => cols,
            Err(e) => {
                tracing::error!("Refusing SerializationHeader key types: {e}");
                return Err(nom::Err::Error(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Verify,
                )));
            }
        };

    Ok((
        input,
        (
            min_timestamp,
            min_deletion_time,
            min_ttl,
            partition_key_columns,
            clustering_key_columns,
            columns,
        ),
    ))
}
