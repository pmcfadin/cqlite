#![allow(dead_code)] // MEASUREMENT SCAFFOLD (#4159): removed below if the corpus stays green
//! SerializationHeader schema parsing (partition/clustering/static/regular columns).
//!
//! The SerializationHeader component of Statistics.db describes the table schema.
//! This module holds the marker-search dispatcher plus the backtracking / ASCII
//! fallbacks (used when the schema offset is unknown). The offset-anchored
//! decoders live in submodules:
//! - [`sequential`] — parse from a known start (legacy marker or post-EncodingStats).
//! - [`schema`] — the authoritative post-EncodingStats sequential decoder.

mod candidate_identity;
mod regular_columns;
mod schema;
mod sequential;

#[cfg(test)]
mod serialization_header_tests;

pub(in crate::parser::enhanced_statistics_parser) use schema::parse_serialization_header_schema;

use super::super::header::ColumnInfo;
use super::marshal_type::convert_marshal_type_to_cql_checked;
use super::schema_refusal::HeaderSchemaError;
use super::SerializationHeaderResult;
use candidate_identity::candidate_confirmed_as_header;
use regular_columns::parse_regular_columns;
use sequential::{
    parse_serialization_header_at_offset, parse_serialization_header_sequential, SemanticGate,
};

/// Parse SerializationHeader from Statistics.db (Issue #163)
///
/// This function locates and parses the complete SerializationHeader section including:
/// 1. Partition key types
/// 2. Clustering key types
/// 3. Regular column definitions
///
/// Returns: (partition_key_types, clustering_key_types, regular_columns)
///
/// # A SEMANTIC REFUSAL STOPS THE SEARCH — ONCE THE CANDIDATE IS CONFIRMED (#4104)
///
/// The search tries many candidate offsets, so a failed candidate is normally just
/// "not here — try the next one", and that is right for every STRUCTURAL failure:
/// where the header lives is exactly what is in doubt.
///
/// A semantic refusal (`FrozenType(<scalar>)`, which no Cassandra writer can have
/// recorded) is a different answer, and it needed THREE attempts to place
/// correctly:
///
/// 1. Originally it was demoted to an ordinary failed candidate, so the search
///    continued and ended at this function's own
///    `Ok((input, (empty, empty, empty)))` no-header success, which the caller
///    accepts as a schema-less header. FAIL-OPEN — every individual gate refused
///    and the net outcome was still `Ok` (roborev job 119).
/// 2. Then it was propagated immediately from wherever it arose. OVER-REFUSAL —
///    this search walks ARBITRARY bytes and the decoders accept any UTF-8 string
///    as the key type, so an earlier FALSE candidate carrying a plausible
///    `FrozenType(<scalar>)` run aborted an otherwise VALID SSTable before the
///    real header was reached (roborev job 120).
/// 3. Now: a refusal is authoritative only once the candidate is CONFIRMED to be
///    a SerializationHeader — a complete decode of its own declared column lists
///    plus marshal class SPELLINGS for its key and clustering types, judged by
///    [`candidate_confirmed_as_header`] from Cassandra's writer. Confirmed ⇒
///    propagate (never fall through to another candidate or to the no-header
///    success). Unconfirmed ⇒ it was a guess that did not pan out, so the search
///    continues, exactly as for a structural failure.
///
/// On the TOC-ANCHORED path the location is known, so the refusal is authoritative
/// on arrival and is propagated there without any of this — see
/// `parse_minimal_encoding_stats`.
pub(super) fn parse_serialization_header(
    input: &[u8],
) -> Result<(&[u8], SerializationHeaderResult), HeaderSchemaError<'_>> {
    tracing::debug!(
        "Searching for SerializationHeader in {} bytes (max search: 8KB)",
        input.len()
    );

    // Log input buffer state at function entry
    let preview_len = std::cmp::min(64, input.len());
    let preview_hex: String = input[..preview_len]
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<Vec<_>>()
        .join(" ");
    tracing::debug!(
        "Input buffer size: {} bytes, first 64 bytes: {}",
        input.len(),
        preview_hex
    );

    // Search for SerializationHeader start marker: VInt followed by 0x00 0x00 and '(' character
    // This marks the beginning of the partition key type descriptor
    let mut search_offset = 0;

    // Search for SerializationHeader by finding "org.apache.cassandra.db.marshal" string
    // and working backwards to find the 0x00 0x00 marker
    // Format: [VInt unknown] [0x00 0x00] [VInt partition_type_len] [partition_type_string]
    let marshal_pattern = b"org.apache.cassandra.db.marshal";

    while search_offset + marshal_pattern.len() < input.len() && search_offset < 8192 {
        if &input[search_offset..search_offset + marshal_pattern.len()] == marshal_pattern {
            let context_start = search_offset.saturating_sub(10);
            let context_end = (search_offset + 50).min(input.len());
            tracing::debug!(
                "Found 'org.apache.cassandra.db.marshal' at offset {}, context (offset-10 to offset+50): {:02x?}",
                search_offset,
                &input[context_start..context_end]
            );

            // Issue #216 fix: Look for the pattern [prev_zero] [pk_type_len] "org.apache..."
            // where pk_type_len is a valid VInt length (0x01-0x7F for single byte, or multi-byte VInt)
            // The prev_zero is typically the last byte of EncodingStats (minTTL=0) or another zero field.
            //
            // We need to find the START of the partition key type length, which is:
            // - 1 byte before "org.apache..." for single-byte lengths (0x28 = 40 bytes for UUIDType)
            // - 2 bytes before for two-byte VInt lengths (0x80 0xXX)

            for lookback in 1..=15 {
                if search_offset < lookback {
                    break;
                }
                let type_len_offset = search_offset - lookback;

                // Check if this could be a valid pk_type_len
                // For single-byte VInt: values 0x01-0x7F
                // For two-byte VInt: first byte has high bit set (0x80-0xFF)
                let first_byte = input[type_len_offset];

                // Common partition key type lengths:
                // - UUIDType: 40 bytes (0x28)
                // - UTF8Type: 40 bytes (0x28)
                // - Int32Type: 41 bytes (0x29)
                // - TimestampType: 45 bytes (0x2D)
                // - CompositeType: ~80-150 bytes (0x50-0x96 or multi-byte VInt)

                // Single-byte VInt: 0x20-0x7F are reasonable pk_type lengths (32-127 bytes)
                let is_valid_single_byte_len = (0x20..=0x7F).contains(&first_byte);

                // Two-byte VInt: 0x80-0xBF with continuation
                let is_multi_byte_vint = first_byte >= 0x80;

                if is_valid_single_byte_len || is_multi_byte_vint {
                    // Try parsing from this offset using sequential parser
                    let candidate = &input[type_len_offset..];
                    match parse_serialization_header_sequential(candidate, SemanticGate::Enforce) {
                        // A semantic refusal fails CLOSED — but only once these
                        // bytes are CONFIRMED to be a header rather than a
                        // candidate that briefly looked like one. The confirmation
                        // re-decodes the SAME bytes with the refusal deferred, so
                        // the completeness the refusal cut short can be judged
                        // (#4104, roborev job 120). Unconfirmed falls through to
                        // the next candidate offset, like a structural failure.
                        Err(refused @ HeaderSchemaError::Refused(_)) => {
                            if candidate_confirmed_as_header(parse_serialization_header_sequential(
                                candidate,
                                SemanticGate::Survey,
                            )) {
                                return Err(refused);
                            }
                        }
                        Ok((remaining, (pk_types, ck_types, cols))) => {
                            // Validate: partition key type should contain expected substring
                            if !pk_types.is_empty()
                                && pk_types[0].contains("org.apache.cassandra.db.marshal")
                            {
                                tracing::debug!(
                                    "Successfully parsed SerializationHeader at offset {} (lookback: {}): pk_type={}",
                                    type_len_offset,
                                    lookback,
                                    pk_types[0]
                                );
                                return Ok((remaining, (pk_types, ck_types, cols)));
                            }
                        }
                        Err(HeaderSchemaError::Structural(_)) => {}
                    }
                }

                // Also try the legacy 0x00 0x00 marker for backward compatibility
                if type_len_offset > 0 {
                    let prev_offset = type_len_offset - 1;
                    if input[prev_offset] == 0x00 && input[type_len_offset] == 0x00 {
                        let candidate = &input[prev_offset..];
                        match parse_serialization_header_at_offset(candidate, SemanticGate::Enforce)
                        {
                            // Same confirm-then-fail-closed rule as the sequential
                            // candidate above: this offset is a guess too.
                            Err(refused @ HeaderSchemaError::Refused(_)) => {
                                if candidate_confirmed_as_header(
                                    parse_serialization_header_at_offset(
                                        candidate,
                                        SemanticGate::Survey,
                                    ),
                                ) {
                                    return Err(refused);
                                }
                            }
                            Ok(parsed) => {
                                tracing::debug!(
                                    "Successfully parsed SerializationHeader at legacy marker offset {}",
                                    prev_offset
                                );
                                return Ok(parsed);
                            }
                            Err(HeaderSchemaError::Structural(_)) => {}
                        }
                    }
                }
            }
        }
        search_offset += 1;
    }

    tracing::debug!(
        "Search completed: searched {} bytes, no partition key type found",
        search_offset
    );

    // Partition key type not found - try to find regular columns directly
    // This handles files where SerializationHeader contains only regular columns
    tracing::debug!("Attempting to parse regular columns without partition key metadata");
    let (remaining, (partition_keys, columns)) = parse_regular_columns(input)?;

    if !columns.is_empty() {
        tracing::debug!(
            "Successfully parsed {} regular columns, {} partition keys via backtracking",
            columns.len(),
            partition_keys.len()
        );
        return Ok((remaining, (partition_keys, Vec::new(), columns)));
    }

    // Nothing found - return empty results
    tracing::warn!(
        "Failed to locate SerializationHeader or regular columns: searched {} bytes",
        search_offset
    );

    if let Some((pk_types, ck_types, cols)) = fallback_parse_serialization_header_ascii(input)? {
        tracing::debug!(
            "ASCII fallback extracted SerializationHeader: {} partition keys, {} clustering keys, {} regular columns",
            pk_types.len(),
            ck_types.len(),
            cols.len()
        );
        return Ok((input, (pk_types, ck_types, cols)));
    }

    Ok((input, (Vec::new(), Vec::new(), Vec::new())))
}

/// ASCII fallback parser for SerializationHeader when structured parsing fails.
///
/// `Ok(None)` = nothing recognisable here (the caller falls through to its
/// no-header success). `Err(Refused)` = a type Cassandra cannot have written was
/// found, which fails CLOSED — it must not degrade to `Ok(None)` and thence to an
/// accepted empty schema (#4104, roborev job 119).
fn fallback_parse_serialization_header_ascii(
    input: &[u8],
) -> Result<Option<SerializationHeaderResult>, HeaderSchemaError<'_>> {
    // Helper to find subsequence
    fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack
            .windows(needle.len())
            .position(|window| window == needle)
    }

    let mut partition_types = Vec::new();
    let mut clustering_types = Vec::new();
    let mut columns = Vec::new();

    // Extract partition key types from CompositeType(...)
    if let Some(comp_idx) = find_subsequence(input, b"CompositeType(") {
        let start = comp_idx + "CompositeType(".len();
        let mut end = start;
        while end < input.len() && input[end] != b')' {
            end += 1;
        }
        if end <= input.len() {
            if let Ok(inner) = std::str::from_utf8(&input[start..end]) {
                partition_types = inner
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
            }

            // Attempt to extract clustering types immediately after the composite type string
            let mut cursor = end + 1;
            while cursor < input.len() && input[cursor] < 0x20 {
                cursor += 1;
            }
            if cursor < input.len() && input[cursor] == b'(' {
                cursor += 1;
                let mut cluster_end = cursor;
                while cluster_end < input.len() && input[cluster_end] >= 0x20 {
                    cluster_end += 1;
                }
                if cluster_end > cursor {
                    if let Ok(cluster_str) = std::str::from_utf8(&input[cursor..cluster_end]) {
                        if cluster_str.contains("org.apache.cassandra.db.marshal") {
                            clustering_types = cluster_str
                                .split(',')
                                .map(|s| s.trim().to_string())
                                .filter(|s| !s.is_empty())
                                .collect();
                        }
                    }
                }
                // Set scan position for column parsing after clustering types/control bytes
                let mut scan_start = cluster_end;
                while scan_start < input.len() && input[scan_start] < 0x20 {
                    scan_start += 1;
                }

                // Parse regular columns using [len][name][type] pattern with control-byte delimiters
                let mut idx = scan_start;
                while idx < input.len() {
                    let name_len = input[idx] as usize;
                    if name_len == 0 || name_len > 64 {
                        idx += 1;
                        continue;
                    }

                    let name_start = idx + 1;
                    let name_end = name_start + name_len;
                    if name_end > input.len() {
                        break;
                    }

                    let name_bytes = &input[name_start..name_end];
                    if !name_bytes
                        .iter()
                        .all(|b| b.is_ascii_alphanumeric() || *b == b'_')
                    {
                        idx += 1;
                        continue;
                    }

                    if name_end >= input.len() || input[name_end] != b'(' {
                        idx += 1;
                        continue;
                    }

                    let type_start = name_end + 1;
                    let mut type_end = type_start;
                    while type_end < input.len() && input[type_end] >= 0x20 {
                        type_end += 1;
                    }

                    if type_end == type_start {
                        idx += 1;
                        continue;
                    }

                    let type_bytes = &input[type_start..type_end];
                    if !type_bytes.windows(10).any(|w| w == b"org.apach") {
                        idx += 1;
                        continue;
                    }

                    let column_name = match std::str::from_utf8(name_bytes) {
                        Ok(s) => s.to_string(),
                        Err(_) => {
                            idx += 1;
                            continue;
                        }
                    };

                    let internal_type = match std::str::from_utf8(type_bytes) {
                        Ok(s) => s.trim().to_string(),
                        Err(_) => {
                            idx += 1;
                            continue;
                        }
                    };

                    // Gate 2 of #4104: a refused type fails the WHOLE header rather
                    // than silently dropping the column, which would hide the refusal
                    // behind a schema that merely looks short — and it propagates as
                    // a REFUSAL, not as `None`, which the caller would accept.
                    let cql_type = convert_marshal_type_to_cql_checked(&internal_type)
                        .map_err(HeaderSchemaError::Refused)?;
                    columns.push(ColumnInfo {
                        name: column_name,
                        column_type: cql_type,
                        is_primary_key: false,
                        key_position: None,
                        is_static: false,
                        is_clustering: false,
                        clustering_reversed: false,
                    });

                    // Advance past control bytes to next potential column entry
                    idx = type_end;
                    while idx < input.len() && input[idx] < 0x20 {
                        idx += 1;
                    }
                }
            }
        }
    }

    if partition_types.is_empty() && columns.is_empty() {
        return Ok(None);
    }

    Ok(Some((partition_types, clustering_types, columns)))
}
