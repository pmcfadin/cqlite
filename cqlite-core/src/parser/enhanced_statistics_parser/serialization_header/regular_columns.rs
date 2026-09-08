//! The LAST-RESORT regular-column scanner: `[0x00]`-anchored `[name][type]`
//! lists, for files where the partition-key/clustering sections could not be
//! located at all.
//!
//! Extracted from `serialization_header/mod.rs` under the campsite rule
//! (#1116): that file was 747 lines against an 800-line target, and the
//! confirm-then-refuse rule below could not be documented there without
//! tripping the `file-size` ratchet.
//!
//! # WHY A REFUSAL HERE IS NOT AUTHORITATIVE ON ARRIVAL (#4104, roborev job 121)
//!
//! This scanner runs only after the marker search has failed to locate a header,
//! and it anchors on a single `0x00` byte. So its candidates are the WEAKEST
//! guesses in the module: any `0x00` followed by a small count and a plausible
//! `[len][name][VInt len][type]` run reaches the semantic gate. Propagating a
//! `FrozenType(<scalar>)` refusal from such a run — which the first #4104 fix
//! did — lets unrelated bytes make an otherwise VALID SSTable unopenable, the
//! same over-refusal `candidate_identity::candidate_confirmed_as_header` fixed
//! on the sequential paths (job 120).
//!
//! There is no `keyType` FIELD here to confirm against, so that helper does not
//! apply. The rule used instead is the one property this site can decide:
//!
//! **A refusal is enforced exactly where the scanner would otherwise have
//! ACCEPTED, and only on evidence that what it accepted is a header.**
//!
//! Concretely, a deferred refusal becomes authoritative only when all three
//! hold:
//!
//! 1. the candidate's column list is COMPLETE and TERMINATED — every one of its
//!    own declared `column_count` entries decoded, which is the same condition
//!    the accept path requires;
//! 2. every column type in it is a fully-qualified marshal class SPELLING, not
//!    merely a string that contains the package (see
//!    [`super::candidate_identity::is_marshal_type_spelling`]); and
//! 3. the backtracking recovered a partition-key type that is also such a
//!    spelling and that ends EXACTLY at the anchor, i.e. the `[VInt len][class
//!    name]` field Cassandra's `SerializationHeader.writeType` writes
//!    immediately before the counts.
//!
//! ## What that confirmation does and does not establish
//!
//! It establishes that these bytes carry a length-framed marshal class spelling
//! ending exactly where a count section begins, followed by a fully consumed
//! `count × ([len][name][VInt len][marshal spelling])` list — the byte shape and
//! the type-string spellings `AbstractType::toString()` produces and therefore
//! the only ones `writeType` can have written (`cassandra-5.0.8:src/java/org/
//! apache/cassandra/db/SerializationHeader.java`; framing tabulated in
//! `docs/sstables-definitive-guide/chapters/08-statistics-db.md`).
//!
//! It does NOT establish that this offset IS the header: no `keyType` field, no
//! clustering or static section and no EncodingStats anchor are decoded here, so
//! the evidence is strictly weaker than the sequential paths'. That is inherent
//! to a scanner whose premise is that the header could not be located — it is
//! the strongest evidence available at this site, not a proof of identity.
//!
//! ## It does not reopen the fail-open
//!
//! An UNCONFIRMED candidate carrying a deferred refusal is SKIPPED, never
//! returned: its surveyed columns hold a raw marshal spelling in place of a CQL
//! type, and returning those would be both a wrong schema and the #28
//! heuristic-fallback the deferral exists to avoid. A CONFIRMED one propagates
//! `Refused`, so it can reach neither another candidate nor the dispatcher's
//! empty-schema `Ok` (job 119's fail-open).

use super::super::super::header::ColumnInfo;
use super::super::super::vint::parse_vuint;
use super::super::marshal_type::convert_marshal_type_to_cql_checked;
use super::super::schema_refusal::HeaderSchemaError;
use super::candidate_identity::is_marshal_type_spelling;

/// `(partition_key_types, regular_columns)` — what the backtracking column
/// scanner recovers when no partition-key/clustering section could be located.
/// Named so its `Result` stays inside clippy's `type_complexity` budget.
pub(super) type RegularColumns = (Vec<String>, Vec<ColumnInfo>);

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

/// Whether a COMPLETE candidate's own recovered strings confirm that this anchor
/// holds a SerializationHeader, to the extent this site can decide.
///
/// `candidate_key_type` is what the backtracking recovered immediately before the
/// anchor (`None` = nothing did), and `declared_types` are the RAW type spellings
/// of the columns the candidate declared. Both must be fully-qualified marshal
/// class spellings, which is what `SerializationHeader.writeType` writes and what
/// arbitrary bytes quoting a type name generally are not. The module header
/// states precisely what this establishes and what it does not.
fn scanned_candidate_confirmed(candidate_key_type: Option<&str>, declared_types: &[&str]) -> bool {
    let Some(key_type) = candidate_key_type else {
        tracing::debug!(
            "Column-section candidate refused a type but no partition-key type ends at its \
             anchor, so nothing identifies it as a header; continuing the scan"
        );
        return false;
    };
    if !is_marshal_type_spelling(key_type) {
        tracing::debug!(
            "Column-section candidate refused a type but the string before its anchor is not \
             a marshal class spelling ({:?}); continuing the scan",
            key_type
        );
        return false;
    }
    // Empty is unconfirmable rather than vacuously confirmed: a candidate that
    // declared no column cannot have refused one. (The scanner already rejects
    // `column_count == 0`, so this is a guard against a future caller, not a
    // reachable state.)
    !declared_types.is_empty()
        && declared_types.iter().all(|declared| {
            let spelled = is_marshal_type_spelling(declared);
            if !spelled {
                tracing::debug!(
                    "Column-section candidate refused a type but one of its column types is \
                     not a marshal class spelling ({:?}); continuing the scan",
                    declared
                );
            }
            spelled
        })
}

/// Parse regular columns section from SerializationHeader
///
/// Returns: (partition_key_types, regular_columns)
/// Partition key types are extracted via backtracking when found before the column section marker.
///
/// A `FrozenType(<scalar>)` refusal raised inside a candidate is DEFERRED to that
/// candidate's acceptance point and enforced only if
/// [`scanned_candidate_confirmed`] holds — this module's header states the rule
/// and what it does and does not establish (#4104, roborev job 121).
pub(super) fn parse_regular_columns(
    input: &[u8],
) -> Result<(&[u8], RegularColumns), HeaderSchemaError<'_>> {
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
            // Kept per-candidate as well as pushed: `partition_key_types`
            // accumulates across candidates, so it cannot answer "what did the
            // backtracking recover at THIS anchor?", which is what the
            // confirmation below asks.
            let mut candidate_key_type = None;
            if let Some(pk_type) = extract_partition_key_before_marker(input, marker_offset) {
                tracing::debug!("Found partition key type before marker: {}", pk_type);
                candidate_key_type = Some(pk_type.clone());
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
            // The refused type, held until this candidate is either accepted or
            // skipped, plus the RAW spellings the candidate declared (the
            // confirmation judges spellings, and `ColumnInfo::column_type` holds
            // the CONVERTED CQL name).
            let mut deferred_refusal = None;
            let mut declared_types: Vec<&str> = Vec::with_capacity(column_count);

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
                declared_types.push(internal_type);
                let cql_type = match convert_marshal_type_to_cql_checked(internal_type) {
                    Ok(cql_type) => cql_type,
                    Err(refusal) => {
                        // DEFERRED, not propagated: this anchor is a `0x00` guess
                        // and an unrelated byte run must not make a valid SSTable
                        // unopenable (module header). The raw spelling stands in
                        // for the CQL name so the list can decode to its declared
                        // end — the candidate is SKIPPED rather than returned if
                        // the refusal turns out to be unconfirmed, so this
                        // spelling never reaches a caller as a schema.
                        deferred_refusal.get_or_insert(refusal);
                        internal_type.to_string()
                    }
                };
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
                // The candidate is COMPLETE, so this is the point at which it
                // would be accepted as the header — and therefore the point at
                // which a deferred refusal is decided.
                if let Some(refusal) = deferred_refusal {
                    if scanned_candidate_confirmed(candidate_key_type.as_deref(), &declared_types) {
                        return Err(HeaderSchemaError::Refused(refusal));
                    }
                    tracing::debug!(
                        "Column section at offset {} refused a type but is not confirmed as a \
                         header; skipping this candidate and continuing the scan",
                        marker_offset
                    );
                    search_offset += 1;
                    continue;
                }

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

