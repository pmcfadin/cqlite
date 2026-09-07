//! SerializationHeader schema parsing (partition/clustering/static/regular columns).
//!
//! The SerializationHeader component of Statistics.db describes the table schema.
//! This module holds the marker-search dispatcher plus the backtracking / ASCII
//! fallbacks (used when the schema offset is unknown). The offset-anchored
//! decoders live in submodules:
//! - [`sequential`] — parse from a known start (legacy marker or post-EncodingStats).
//! - [`schema`] — the authoritative post-EncodingStats sequential decoder.

mod schema;
mod sequential;

#[cfg(test)]
mod serialization_header_tests;

pub(in crate::parser::enhanced_statistics_parser) use schema::parse_serialization_header_schema;

use super::super::header::ColumnInfo;
use super::super::vint::parse_vuint;
use super::marshal_type::convert_marshal_type_to_cql_checked;
use super::schema_refusal::HeaderSchemaError;
use super::SerializationHeaderResult;
use sequential::{parse_serialization_header_at_offset, parse_serialization_header_sequential};

/// `(partition_key_types, regular_columns)` — what the backtracking column
/// scanner recovers when no partition-key/clustering section could be located.
/// Named so its `Result` stays inside clippy's `type_complexity` budget.
type RegularColumns = (Vec<String>, Vec<ColumnInfo>);

/// Parse SerializationHeader from Statistics.db (Issue #163)
///
/// This function locates and parses the complete SerializationHeader section including:
/// 1. Partition key types
/// 2. Clustering key types
/// 3. Regular column definitions
///
/// Returns: (partition_key_types, clustering_key_types, regular_columns)
///
/// # A SEMANTIC REFUSAL STOPS THE SEARCH (#4104, roborev job 119)
///
/// The search tries many candidate offsets, so a failed candidate is normally just
/// "not here — try the next one", and that is right for every STRUCTURAL failure:
/// where the header lives is exactly what is in doubt. It is WRONG for a semantic
/// refusal. A candidate that decoded a column name and a marshal type and then
/// found `FrozenType(<scalar>)` DID find a header; continuing the search can only
/// replace a correct refusal with a different heuristic match — or, as measured,
/// with this function's own `Ok((input, (empty, empty, empty)))` no-header success,
/// which the caller accepts as a schema-less header. That was fail-open even though
/// every individual gate had refused, which is why the two reviewers of job 119
/// disagreed: the per-candidate gates DO fire, and the net outcome was still `Ok`.
///
/// So [`HeaderSchemaError::Refused`] is propagated immediately from wherever it
/// arises, and only `Structural` continues the search.
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
                    let result = parse_serialization_header_sequential(&input[type_len_offset..]);
                    // FAIL-CLOSED on a semantic refusal: this offset DID hold a
                    // header, so no other offset can produce a better answer (#4104).
                    if let Err(refused @ HeaderSchemaError::Refused(_)) = result {
                        return Err(refused);
                    }
                    if let Ok((remaining, (pk_types, ck_types, cols))) = result {
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
                }

                // Also try the legacy 0x00 0x00 marker for backward compatibility
                if type_len_offset > 0 {
                    let prev_offset = type_len_offset - 1;
                    if input[prev_offset] == 0x00 && input[type_len_offset] == 0x00 {
                        let result = parse_serialization_header_at_offset(&input[prev_offset..]);
                        // Same fail-closed rule as the sequential candidate above.
                        if let Err(refused @ HeaderSchemaError::Refused(_)) = result {
                            return Err(refused);
                        }
                        if result.is_ok() {
                            tracing::debug!(
                                "Successfully parsed SerializationHeader at legacy marker offset {}",
                                prev_offset
                            );
                            return result;
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

/// Extract partition key type by backtracking from the `0x00 0x00` marker
///
/// The partition key type descriptor ends immediately before the marker.
/// We try parsing VInt lengths at different offsets before the marker to find
/// a valid type string that matches Cassandra marshal type patterns.
fn extract_partition_key_before_marker(input: &[u8], marker_offset: usize) -> Option<String> {
    if marker_offset < 3 {
        return None;
    }

    tracing::debug!(
        "Backtracking from marker at offset {} (input len: {})",
        marker_offset,
        input.len()
    );

    // Try parsing VInt lengths at different positions before the marker
    // Type strings can be up to 200 bytes, and VInts can be 1-9 bytes,
    // so we need to search back at least 209 bytes (200 + 9)
    let max_lookback = 210;
    let search_start = marker_offset.saturating_sub(max_lookback);
    tracing::debug!(
        "Searching for VInt from offset {} to {} ({} positions)",
        search_start,
        marker_offset,
        marker_offset - search_start
    );

    for vint_start in (search_start..marker_offset).rev() {
        // Try to parse VInt at this position
        match parse_vuint(&input[vint_start..marker_offset]) {
            Ok((remaining, type_len)) => {
                // Validate type length is reasonable first (before any arithmetic)
                if !(10..200).contains(&type_len) {
                    continue;
                }

                // Calculate how many bytes the VInt consumed
                let vint_len = marker_offset - vint_start - remaining.len();
                let type_start = vint_start + vint_len;

                // Bounds check before addition to prevent overflow
                let type_len_usize = type_len as usize;
                if type_start > input.len() || type_len_usize > input.len() - type_start {
                    continue;
                }

                let type_end = type_start + type_len_usize;

                // Validate:
                // 1. The type string ends exactly at the marker
                // 2. The type string is valid UTF-8
                // 3. It matches Cassandra marshal type patterns
                if type_end == marker_offset {
                    if let Ok(type_str) = std::str::from_utf8(&input[type_start..type_end]) {
                        tracing::debug!(
                            "Candidate at vint_start={}: type_len={}, type_start={}, type_end={}, str={}",
                            vint_start, type_len, type_start, type_end, type_str
                        );
                        // Validate it's a Cassandra marshal type
                        // Note: Partition key types may or may not start with '('
                        // Both "(org.apache.cassandra..." and "org.apache.cassandra..." are valid
                        if type_str.contains("org.apache.cassandra") {
                            tracing::debug!(
                                "Found partition key type at offset {}: length={}, type={}",
                                vint_start,
                                type_len,
                                type_str
                            );
                            return Some(type_str.to_string());
                        } else {
                            tracing::debug!(
                                "Rejected candidate (starts_with='(': {}, contains 'org.apache.cassandra': {})",
                                type_str.starts_with('('),
                                type_str.contains("org.apache.cassandra")
                            );
                        }
                    } else {
                        tracing::debug!(
                            "Rejected candidate at vint_start={}: not valid UTF-8",
                            vint_start
                        );
                    }
                }
            }
            Err(_) => continue, // Try next offset
        }
    }

    None
}

/// Parse regular columns section from SerializationHeader
///
/// Returns: (partition_key_types, regular_columns)
/// Partition key types are extracted via backtracking when found before the column section marker.
fn parse_regular_columns(input: &[u8]) -> Result<(&[u8], RegularColumns), HeaderSchemaError<'_>> {
    let mut search_offset = 0;
    let mut partition_key_types = Vec::new();

    while search_offset + 2 < input.len() && search_offset < 8192 {
        if input[search_offset] == 0x00 {
            let (marker_offset, count_offset) =
                if search_offset + 1 < input.len() && input[search_offset + 1] == 0x00 {
                    (search_offset, search_offset + 2)
                } else {
                    (search_offset, search_offset + 1)
                };

            if count_offset >= input.len() {
                break;
            }

            let column_count = input[count_offset] as usize;
            if column_count == 0 || column_count > 50 {
                search_offset += 1;
                continue;
            }

            tracing::debug!(
                "Attempting to extract partition key by backtracking from marker at offset {}",
                marker_offset
            );
            if let Some(pk_type) = extract_partition_key_before_marker(input, marker_offset) {
                tracing::debug!("Found partition key type before marker: {}", pk_type);
                partition_key_types.push(pk_type);
            } else {
                tracing::debug!(
                    "No partition key type found via backtracking at offset {}",
                    marker_offset
                );
            }

            let mut pos = count_offset + 1;

            let context_len = std::cmp::min(128, input.len() - marker_offset);
            let context_hex: String = input[marker_offset..marker_offset + context_len]
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<Vec<_>>()
                .join(" ");
            tracing::debug!(
                "Pattern found at offset {}: count={}, next 128 bytes: {}",
                marker_offset,
                column_count,
                context_hex
            );

            // Try to parse all columns - if successful, we found the right section
            let mut parsed_columns = Vec::with_capacity(column_count);
            let mut parse_success = true;

            for col_idx in 0..column_count {
                if pos >= input.len() {
                    tracing::debug!(
                        "Column {} parsing failed at offset {}: position {} exceeds buffer length {}",
                        col_idx,
                        marker_offset,
                        pos,
                        input.len()
                    );
                    parse_success = false;
                    break;
                }

                if pos >= input.len() {
                    tracing::debug!(
                        "Column {} parsing failed at offset {}: no data available for name length byte (pos={}, len={})",
                        col_idx,
                        marker_offset,
                        pos,
                        input.len()
                    );
                    parse_success = false;
                    break;
                }

                let name_len = input[pos] as usize;
                pos += 1;

                if name_len == 0 || name_len > 200 || pos + name_len > input.len() {
                    tracing::debug!(
                        "Column {} parsing failed at offset {}: name_len sanity check failed (name_len={}, pos={}, buffer_len={})",
                        col_idx,
                        marker_offset,
                        name_len,
                        pos,
                        input.len()
                    );
                    parse_success = false;
                    break;
                }

                // Column name (UTF-8 string)
                let name_bytes = &input[pos..pos + name_len];
                let column_name = match std::str::from_utf8(name_bytes) {
                    Ok(s) => s.to_string(),
                    Err(e) => {
                        let name_hex: String = name_bytes
                            .iter()
                            .map(|b| format!("{:02x}", b))
                            .collect::<Vec<_>>()
                            .join(" ");
                        tracing::debug!(
                            "Column {} parsing failed at offset {}: UTF-8 decode error for column name at pos {} (len={}): {:?}, bytes: {}",
                            col_idx,
                            marker_offset,
                            pos,
                            name_len,
                            e,
                            name_hex
                        );
                        parse_success = false;
                        break;
                    }
                };
                pos += name_len;

                if pos >= input.len() {
                    tracing::debug!(
                        "Column {} ('{}') parsing failed at offset {}: no data available for type length byte (pos={}, len={})",
                        col_idx,
                        column_name,
                        marker_offset,
                        pos,
                        input.len()
                    );
                    parse_success = false;
                    break;
                }

                // Parse type length as VInt (can exceed 127 for collection types)
                let type_len_result = parse_vuint(&input[pos..]);
                let (type_remaining, type_len_u64) = match type_len_result {
                    Ok(r) => r,
                    Err(_) => {
                        tracing::debug!(
                            "Column {} ('{}') parsing failed at offset {}: VInt parse error at pos {}",
                            col_idx,
                            column_name,
                            marker_offset,
                            pos
                        );
                        parse_success = false;
                        break;
                    }
                };
                let type_len = type_len_u64 as usize; // #3848: raw `u64` bounded on the `if` below
                pos = input.len() - type_remaining.len();

                if type_len_u64 == 0 || type_len_u64 > 5000 || pos + type_len > input.len() {
                    tracing::debug!(
                        "Column {} ('{}') parsing failed at offset {}: type_len sanity check failed (type_len={}, pos={}, buffer_len={})",
                        col_idx,
                        column_name,
                        marker_offset,
                        type_len,
                        pos,
                        input.len()
                    );
                    parse_success = false;
                    break;
                }

                // Column type: decoded and CQL-converted in one step. The two causes
                // no longer share one arm (#4104, roborev job 119) — they are
                // DIFFERENT answers. Bytes that are not UTF-8 mean "this marker
                // offset holds no readable header", so the search continues. A type
                // no Cassandra writer can have recorded means the header IS here and
                // is unwritable, so it fails CLOSED and carries its message out.
                let type_bytes = &input[pos..pos + type_len];
                let Ok(internal_type) = std::str::from_utf8(type_bytes) else {
                    tracing::debug!(
                        "Column {} ('{}') parsing failed at offset {}: column type at pos {} \
                         (len={}) is not valid UTF-8; bytes: {:02x?}",
                        col_idx,
                        column_name,
                        marker_offset,
                        pos,
                        type_len,
                        type_bytes
                    );
                    parse_success = false;
                    break;
                };
                let cql_type = convert_marshal_type_to_cql_checked(internal_type)
                    .map_err(HeaderSchemaError::Refused)?;
                pos += type_len;

                parsed_columns.push(ColumnInfo {
                    name: column_name,
                    column_type: cql_type,
                    is_primary_key: false, // Will be determined from partition/clustering info
                    key_position: None,
                    is_static: false,
                    is_clustering: false,
                    clustering_reversed: false,
                });
            }

            if parse_success && parsed_columns.len() == column_count {
                // Successfully parsed all columns
                let column_names: Vec<&str> =
                    parsed_columns.iter().map(|c| c.name.as_str()).collect();
                tracing::debug!(
                    "Successfully parsed {} columns at offset {}: {:?}",
                    parsed_columns.len(),
                    marker_offset,
                    column_names
                );
                if !partition_key_types.is_empty() {
                    tracing::debug!(
                        "Extracted {} partition key types via backtracking: {:?}",
                        partition_key_types.len(),
                        partition_key_types
                    );
                }

                let remaining = &input[pos..];
                return Ok((remaining, (partition_key_types, parsed_columns)));
            }
        }

        search_offset += 1;
    }

    // Column section not found - return empty vecs (not an error, some files may have no regular columns)
    tracing::debug!(
        "Regular column section not found: searched {} bytes",
        search_offset
    );
    Ok((input, (Vec::new(), Vec::new())))
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
