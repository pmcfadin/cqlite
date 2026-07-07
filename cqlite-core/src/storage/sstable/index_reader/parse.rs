//! Index.db nom parse tree (issue #1599 / G3 split of `index_reader.rs`, campsite #1116).
//!
//! The BIG/NB `Index.db` on-disk layout and its `nom` combinators, split out of
//! `index_reader/mod.rs` (which keeps the `IndexReader` struct, its `open`/lookup
//! API, and the header/entry types). Behavior is unchanged; the parsers are
//! `pub(crate)`-re-exported from `mod.rs` so existing call paths
//! (`index_reader::parse_big_index_entry`, `::parse_all_partition_keys`) are
//! preserved verbatim.

use super::{IndexData, IndexHeader, PartitionIndexEntry, PromotedIndexData};
use crate::parser::vint::parse_vuint;
use crate::storage::sstable::header_spec::get_global_registry;
use crate::storage::sstable::summary_reader::SummaryReader;
use nom::{bytes::complete::take, number::complete::be_u16, IResult};
use std::collections::HashMap;
use std::sync::Arc;

/// Parse Index.db file data with optional Summary.db correlation using spec-driven approach
pub(super) fn parse_index_data_with_summary<'a>(
    input: &'a [u8],
    summary_reader: Option<&SummaryReader>,
) -> IResult<&'a [u8], IndexData> {
    use nom::error::{Error as NomError, ErrorKind};

    // First try spec-driven header parsing
    let registry = get_global_registry();
    let (remaining, header) = match registry.parse_index_header(input) {
        Ok(parsed_header) => {
            log::debug!("Successfully parsed Index.db header using spec-driven approach");

            // Convert ParsedHeader to IndexHeader
            let header = IndexHeader {
                version: parsed_header
                    .fields
                    .get("version")
                    .and_then(|v| v.as_u32().ok())
                    .unwrap_or(1),
                entry_count: parsed_header
                    .fields
                    .get("entry_count")
                    .and_then(|v| v.as_u32().ok())
                    .unwrap_or(0),
                data_size: parsed_header
                    .fields
                    .get("data_size")
                    .and_then(|v| v.as_u64().ok())
                    .unwrap_or(input.len() as u64),
                checksum: parsed_header
                    .fields
                    .get("checksum")
                    .and_then(|v| v.as_u32().ok())
                    .unwrap_or(0),
            };

            // Skip header bytes for data parsing
            let header_size = parsed_header.header_size;
            if input.len() < header_size {
                return Err(nom::Err::Error(NomError::new(input, ErrorKind::Eof)));
            }
            (&input[header_size..], header)
        }
        Err(_) => {
            log::debug!("Spec-driven header parsing failed, assuming headerless format");

            // Parse all partition key digests - no header in some formats
            let header = IndexHeader {
                version: 1,
                entry_count: 0, // Will be updated after parsing entries
                data_size: input.len() as u64,
                checksum: 0,
            };
            (input, header)
        }
    };

    // Parse partition entries from remaining data
    let (remaining, partition_entries) =
        parse_all_partition_keys_with_summary(remaining, summary_reader)?;

    // Build lookup table with zero-copy approach using Arc::clone (reference counting only)
    // This eliminates the memory explosion from cloning Vec<u8> key digests
    let mut key_lookup = HashMap::new();
    for (index, entry) in partition_entries.iter().enumerate() {
        key_lookup.insert(Arc::clone(&entry.key_digest), index);
    }

    // Update header with actual entry count
    let header = IndexHeader {
        entry_count: partition_entries.len() as u32,
        ..header
    };

    Ok((
        remaining,
        IndexData {
            header,
            partition_entries,
            key_lookup,
        },
    ))
}

/// Parse all partition entries from the Index.db file.
///
/// ## Authoritative format (Issue #552, Cassandra 5.0 NB / BIG Index.db)
///
/// Index.db is ALWAYS the BIG-format partition index. Each entry is:
///
/// ```text
/// [key_len: u16 BE]                    ← length of the raw partition key
/// [raw partition key bytes: key_len]   ← the partition key exactly as in Data.db
/// [data_offset: unsigned vint]         ← byte offset into the Data.db data section
/// [promoted_index_len: unsigned vint]  ← byte length of the promoted index (0 = none)
/// [promoted_index_data: promoted_index_len bytes]
/// ```
///
/// The leading u16 is the partition key LENGTH, not a `0x0010` marker, and there is no
/// MD5 digest on disk (verified against real Cassandra Index.db files: single-UUID keys
/// start `0x0010`, the composite-key `multi_partition_table` starts `0x0026` = 38 bytes).
///
/// There is no separate "BTI" Index.db format: a BTI-indexed SSTable uses Partitions.db /
/// Rows.db trie structures and does not produce an Index.db at all (see guide Ch.17). So the
/// previous `detect_index_format` heuristic was entirely spurious (Issue #28 mandate) and has
/// been removed in favour of this single, spec-accurate parser that works for ANY key length.
///
/// The `summary_reader` argument is retained for API compatibility; offsets are now stored
/// inline so Summary.db correlation is no longer needed for parsing.
pub(super) fn parse_all_partition_keys_with_summary<'a>(
    input: &'a [u8],
    _summary_reader: Option<&SummaryReader>,
) -> IResult<&'a [u8], Vec<PartitionIndexEntry>> {
    let mut entries = Vec::new();
    let mut remaining = input;

    let mut entry_index = 0;
    while !remaining.is_empty() {
        match parse_big_index_entry(remaining) {
            Ok((rest, entry)) => {
                debug_assert!(
                    rest.len() < remaining.len(),
                    "BIG Index.db parser must make forward progress"
                );
                entries.push(entry);
                remaining = rest;
                entry_index += 1;
            }
            Err(_e) => {
                log::debug!(
                    "Stopped parsing Index.db at entry {} with {} bytes remaining",
                    entry_index,
                    remaining.len()
                );
                break;
            }
        }
    }

    log::debug!("Parsed {} partition entries from Index.db", entries.len());
    Ok((remaining, entries))
}

/// Parse a single BIG-format Index.db entry.
///
/// Layout: `[key_len: u16 BE][raw key][data_offset: vint][promoted_len: vint][promoted...]`.
/// Works for any key length (int, text, UUID, composite). The raw partition key is stored
/// directly in `key_digest` / `raw_key` (no MD5, no marker).
pub(crate) fn parse_big_index_entry(input: &[u8]) -> IResult<&[u8], PartitionIndexEntry> {
    // Read partition key length (u16 big-endian).
    let (input, key_len) = be_u16(input)?;

    // Read the raw partition key bytes.
    let (input, key_bytes) = take(key_len)(input)?;

    // Read unsigned VInt data offset (relative to the Data.db data section start;
    // SSTableReader adds the header size when seeking).
    let (input, data_offset) = parse_vuint(input)?;

    // Read promoted-index length (unsigned VInt). When > 0, CAPTURE the promoted
    // payload (Issue #993) instead of discarding it; structural decode is deferred
    // to PromotedIndexData::decode (needs schema-driven clustering-prefix lengths).
    let (input, promoted_len) = parse_vuint(input)?;
    // Saturating cast: on a 32-bit target `promoted_len as usize` could truncate and
    // misalign subsequent entries. `usize::MAX` makes `take` return an Eof error on a
    // short buffer instead, which is the safe failure mode for a corrupt Index.db.
    let promoted_len = usize::try_from(promoted_len).unwrap_or(usize::MAX);
    let (input, promoted_data) = take(promoted_len)(input)?;

    log::trace!(
        "Index.db BIG entry: key_len={}, data_offset={}, promoted_len={}",
        key_len,
        data_offset,
        promoted_len
    );

    // promoted_len == 0 → no promoted index (None). Otherwise wrap the raw payload.
    let promoted_index = if promoted_len > 0 {
        Some(PromotedIndexData::from_raw(promoted_data.to_vec()))
    } else {
        None
    };

    let raw_key: Arc<[u8]> = Arc::from(key_bytes);

    Ok((
        input,
        PartitionIndexEntry {
            key_digest: Arc::clone(&raw_key),
            raw_key: Some(raw_key),
            // Size is not stored in Index.db; determined during the Data.db read.
            data_offset,
            data_size: 0,
            promoted_index,
        },
    ))
}

// REMOVED: Old heuristic functions that violated Issue #28 no-heuristics mandate
// - calculate_data_offset_from_summary: Summary.db correlation (now obsolete with inline offsets)
// - interpolate_data_offset_from_summary_position: Used arbitrary estimates
// - estimate_data_offset_from_index_position: Used hardcoded partition size guesses
//
// Modern Cassandra 5+ Index.db format includes unsigned VInt offsets inline,
// eliminating the need for Summary.db correlation. See parse_vuint() in parser/vint.rs.

/// Parse Index.db file data - Legacy API for backward compatibility
#[allow(dead_code)]
pub(super) fn parse_index_data(input: &[u8]) -> IResult<&[u8], IndexData> {
    parse_index_data_with_summary(input, None)
}

/// Parse all partition key digests from the Index.db file - Legacy API
#[allow(dead_code)]
pub(crate) fn parse_all_partition_keys(input: &[u8]) -> IResult<&[u8], Vec<PartitionIndexEntry>> {
    parse_all_partition_keys_with_summary(input, None)
}

/// Parse a single BIG-format Index.db partition entry - Legacy API
#[allow(dead_code)]
pub(super) fn parse_simple_partition_key(input: &[u8]) -> IResult<&[u8], PartitionIndexEntry> {
    parse_big_index_entry(input)
}

// Note: Promoted index parsing removed as it's not present in the simple Index.db format
// Real Cassandra 5 Index.db files only contain partition key digests
