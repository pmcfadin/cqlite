//! EncodingStats decoding from the SERIALIZATION_HEADER component of Statistics.db.
//!
//! EncodingStats are three epoch-relative unsigned VInt deltas (minTimestamp,
//! minLocalDeletionTime, minTTL) that prefix the SerializationHeader. They are
//! the baseline values V5CompressedLegacy needs for delta-coded timestamps/TTLs.
//! This module decodes those three fields and then defers to
//! [`super::serialization_header`] for the schema that follows.
//!
//! # There is exactly ONE way to reach the SerializationHeader (issues #4159, #28)
//!
//! The TOC's `SERIALIZATION_HEADER` offset. It is authoritative metadata, and this
//! module now REFUSES rather than substituting a guess when it cannot be used:
//!
//! * no `HEADER` entry in the TOC — the component the reader needs is not declared;
//! * a `HEADER` offset at or past end of file — a corrupt TOC;
//! * a `HEADER` offset whose EncodingStats/schema walk fails — corrupt content.
//!
//! Each of those used to fall back to a MARKER SEARCH
//! (`serialization_header::parse_serialization_header`) that scanned the first 8 KiB
//! for the literal `org.apache.cassandra.db.marshal` and worked BACKWARDS through a
//! 15-byte look-back for something VInt-shaped. That is type guessing from byte
//! patterns — the no-heuristics mandate's central prohibition — and it was not even
//! behind the `legacy-heuristics` feature. Worse, for issue #4159 it meant a
//! legitimately-refused serialization header was **not observable at any layer
//! above this one**: the refusal was replaced by a plausible-looking column list
//! decoded against guessed offsets, so a scan returned rows built from the wrong
//! columns instead of reporting that the file could not be read.
//!
//! Measured before removal: with the marker search in place, a `Statistics.db`
//! truncated to half its length AND one whose partition-key marshal type was made
//! invalid UTF-8 both still parsed SUCCESSFULLY
//! (`issue_4159_unreadable_sstable_scan_refusal.rs` asserts the post-removal
//! behaviour). Both now refuse.

#[cfg(test)]
mod encoding_stats_tests;

use super::super::vint::parse_vuint;
use super::marshal_type::build_column_infos;
use super::serialization_header::parse_serialization_header_schema;
use super::EncodingStatsResult;
use crate::storage::sstable::version_gate::VersionGates;
use nom::IResult;

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
///
/// # The error is TYPED, and that is the point (#4104, roborev job 119)
///
/// A semantic refusal (`frozen<scalar>`) and a structural failure (truncated,
/// mispositioned) are two different answers, and a `nom::Err` can carry neither
/// the distinction nor the refusal's message. Both used to leave here as the same
/// bare `ErrorKind::Verify`, so a user opening an SSTable whose header spells
/// `FrozenType(Int32Type)` was told `Corruption: … code: Verify` — indistinguishable
/// from a garbled file, while the precise column-naming, citation-carrying message
/// the gate had already built survived only if `RUST_LOG` happened to be on.
/// [`HeaderSchemaError::Refused`] carries that message to the caller, which turns it
/// into the user-visible error.
pub(super) fn parse_minimal_encoding_stats<'a>(
    input: &'a [u8],
    full_input: &'a [u8],
    header_offset: Option<usize>,
    gates: Option<&VersionGates>,
) -> Result<(&'a [u8], EncodingStatsResult), HeaderSchemaError<'a>> {
    // The SERIALIZATION_HEADER component (type 3) starts with EncodingStats:
    //   [vuint minTimestamp_delta] [vuint minLocalDeletionTime_delta] [vuint minTTL_delta]
    // These are unsigned VInt deltas from epoch constants (see EncodingStats.Serializer).
    // Use the TOC-based offset to read from the correct location.

    // #4159/#28: the TOC offset is the ONLY authoritative route to the
    // SERIALIZATION_HEADER. Its absence is a refusal, not an invitation to guess.
    let Some(offset) = header_offset else {
        tracing::debug!(
            "Statistics.db declares no SERIALIZATION_HEADER (HEADER) TOC entry; refusing \
             rather than searching the buffer for a marshal-type marker"
        );
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    };

    if offset >= full_input.len() {
        tracing::debug!(
            "Statistics.db HEADER TOC offset 0x{:x} is at or past end of file ({} bytes); \
             refusing — a TOC that points outside the file is corrupt",
            offset,
            full_input.len()
        );
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
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

    // Parse the rest of the SerializationHeader (schema info).
    //
    // #4159/#28: a refusal here PROPAGATES. It used to fall back to the marker
    // search, which is what made "the serialization header refused" unobservable
    // above this layer — the very condition issue #4159 is about.
    let (_, (partition_types, clustering_types, columns)) =
        parse_serialization_header_schema(rest)?;

    // Gate 2 of #4104: the key comparators reach `build_column_infos` as RAW
    // marshal strings (the DESC `ReversedType(..)` signal is derived there), so
    // this is where a frozen-scalar KEY type is refused. Fail-closed.
    let (partition_key_columns, clustering_key_columns) =
        match build_column_infos(&partition_types, &clustering_types) {
            Ok(cols) => cols,
            // Semantic, exactly like the column gate above: the refusal message
            // (which names the key column and its type) is carried to the caller
            // rather than logged and dropped (#4104 blocker A).
            Err(e) => return Err(HeaderSchemaError::Refused(e)),
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

