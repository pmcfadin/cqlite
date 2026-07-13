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
use crate::storage::scan_cancel::ScanCancel;
use crate::storage::sstable::header_spec::get_global_registry;
use crate::storage::sstable::summary_reader::SummaryReader;
use crate::Result;
use nom::{bytes::complete::take, number::complete::be_u16, IResult};
use std::collections::HashMap;
use std::sync::Arc;

/// How often the O(entries) `Index.db` parse loop polls the cancel flag
/// (issue #2383 fix C): once per this many entries, NOT per entry — a single
/// relaxed atomic load amortised over ~64k `memcmp`/vint decodes is free, yet a
/// client-disconnect cancel still aborts a 1.58M-entry parse within one interval
/// instead of pinning a worker to completion.
const CANCEL_POLL_INTERVAL: usize = 1 << 16;

/// Parse Index.db file data with optional Summary.db correlation using spec-driven approach.
///
/// Non-cancellable façade over [`parse_index_data_cancellable`] (issue #2383): the
/// default [`ScanCancel`] never trips, so the cancel arm is unreachable here.
pub(super) fn parse_index_data_with_summary<'a>(
    input: &'a [u8],
    summary_reader: Option<&SummaryReader>,
) -> IResult<&'a [u8], IndexData> {
    use nom::error::{Error as NomError, ErrorKind};
    match parse_index_data_cancellable(input, summary_reader, &ScanCancel::default()) {
        Ok(pair) => Ok(pair),
        // A default (never-cancel) flag cannot yield `Cancelled`; any error here is
        // a header/structural failure mapped to a nom error for the IResult callers.
        Err(_) => Err(nom::Err::Error(NomError::new(input, ErrorKind::Fail))),
    }
}

/// Cancel-aware Index.db parse (issue #2383 fix C).
///
/// Polls `cancel` before the header and roughly every [`CANCEL_POLL_INTERVAL`]
/// entries inside the O(entries) loop, returning [`crate::Error::Cancelled`]
/// promptly instead of running a 1.58M-entry parse to completion after a client
/// disconnect. This is the SINGLE site that emits `index_parses_total`, so both
/// the cancellable and non-cancellable façades count a full parse exactly once.
pub(super) fn parse_index_data_cancellable<'a>(
    input: &'a [u8],
    summary_reader: Option<&SummaryReader>,
    cancel: &ScanCancel,
) -> Result<(&'a [u8], IndexData)> {
    // Coarse pre-parse cancel check (covers a pre-cancelled open).
    cancel.check()?;

    let (remaining, header) = parse_index_header_prefix(input);

    // Parse partition entries (cancel-polled) from the post-header data.
    let (remaining, partition_entries) =
        parse_all_partition_keys_cancellable(remaining, summary_reader, cancel)?;

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

/// Parse (or synthesize, for the headerless format) the `Index.db` header prefix,
/// returning the post-header remainder. Infallible: an unparseable header falls
/// back to the headerless layout exactly as before (issue #28, no heuristics —
/// the spec-driven registry is the authority, the headerless case is the
/// documented Cassandra 5.0 shape).
fn parse_index_header_prefix(input: &[u8]) -> (&[u8], IndexHeader) {
    let registry = get_global_registry();
    match registry.parse_index_header(input) {
        Ok(parsed_header) => {
            tracing::debug!("Successfully parsed Index.db header using spec-driven approach");
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
            let header_size = parsed_header.header_size;
            if input.len() < header_size {
                // Truncated below the declared header: no post-header data.
                (&[], header)
            } else {
                (&input[header_size..], header)
            }
        }
        Err(_) => {
            tracing::debug!("Spec-driven header parsing failed, assuming headerless format");
            let header = IndexHeader {
                version: 1,
                entry_count: 0, // Will be updated after parsing entries
                data_size: input.len() as u64,
                checksum: 0,
            };
            (input, header)
        }
    }
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
    summary_reader: Option<&SummaryReader>,
) -> IResult<&'a [u8], Vec<PartitionIndexEntry>> {
    use nom::error::{Error as NomError, ErrorKind};
    // Non-cancellable façade: a default flag never trips, so the cancel arm is
    // unreachable; map it defensively to a nom error for the IResult callers.
    match parse_all_partition_keys_cancellable(input, summary_reader, &ScanCancel::default()) {
        Ok(pair) => Ok(pair),
        Err(_) => Err(nom::Err::Error(NomError::new(input, ErrorKind::Fail))),
    }
}

/// Cancel-aware entry loop (issue #2383 fix C). Polls `cancel` once per
/// [`CANCEL_POLL_INTERVAL`] entries; on a trip returns [`crate::Error::Cancelled`]
/// promptly. Emits the `index_parses_total` counter exactly once on a full pass —
/// the SINGLE full-parse site (both façades route here).
pub(super) fn parse_all_partition_keys_cancellable<'a>(
    input: &'a [u8],
    _summary_reader: Option<&SummaryReader>,
    cancel: &ScanCancel,
) -> Result<(&'a [u8], Vec<PartitionIndexEntry>)> {
    let mut entries = Vec::new();
    let mut remaining = input;

    let mut entry_index = 0usize;
    while !remaining.is_empty() {
        // #2383 fix C: bounded-interval cooperative cancel (not per entry).
        if entry_index % CANCEL_POLL_INTERVAL == 0 {
            cancel.check()?;
        }
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
                tracing::debug!(
                    "Stopped parsing Index.db at entry {} with {} bytes remaining",
                    entry_index,
                    remaining.len()
                );
                break;
            }
        }
    }

    tracing::debug!("Parsed {} partition entries from Index.db", entries.len());
    // Issue #2383: count every full Index.db parse so a redundant re-parse of the
    // SAME generation (the resolve-phase CPU spin) is observable in metrics — a
    // correct read path parses each generation's index at most once per query.
    crate::observability::add_counter(crate::observability::catalog::INDEX_PARSES_TOTAL, 1, &[]);
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

    tracing::trace!(
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
