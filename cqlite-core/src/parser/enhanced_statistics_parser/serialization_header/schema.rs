//! Post-EncodingStats SerializationHeader schema parser.
//!
//! `parse_serialization_header_schema` is the authoritative sequential decoder
//! invoked once the TOC offset has positioned us immediately after the three
//! EncodingStats VInts. It walks keyType, clusteringTypes, staticColumns, and
//! regularColumns in their on-disk order (see SerializationHeader.java).

use super::super::super::header::ColumnInfo;
use super::super::super::vint::parse_vuint;
use super::super::marshal_type::convert_marshal_type_to_cql_checked;
use super::super::schema_refusal::HeaderSchemaError;
use super::super::SerializationHeaderResult;
use nom::bytes::complete::take;

/// Parse the schema portion of a SerializationHeader (after EncodingStats have been consumed).
///
/// Format:
/// 1. keyType (VInt length + UTF-8 type string)
/// 2. clusteringTypes (VInt count + [VInt type_len + type]*)
/// 3. staticColumns (VInt count + [VInt name_len + name + VInt type_len + type]*)
/// 4. regularColumns (VInt count + [VInt name_len + name + VInt type_len + type]*)
///
/// # Failure kinds are NOT interchangeable
///
/// The error channel is [`HeaderSchemaError`], not `nom::Err`, precisely so the
/// caller can tell the two apart (issue #4104, roborev job 116):
///
/// * [`HeaderSchemaError::Structural`] — the bytes here are not a readable
///   header (truncated, implausible declared length, non-UTF-8 name/type). WHERE
///   the header is, is in doubt, so retrying with the marker-search decoder is
///   legitimate.
/// * [`HeaderSchemaError::Refused`] — the header decoded and DECLARES a type
///   Cassandra cannot have written (`frozen<scalar>`). Position is not in doubt,
///   so no other decoder can improve the answer; the caller MUST fail closed and
///   must never substitute a marker-search guess (#28).
pub(in crate::parser::enhanced_statistics_parser) fn parse_serialization_header_schema<'a>(
    input: &'a [u8],
) -> Result<(&'a [u8], SerializationHeaderResult), HeaderSchemaError<'a>> {
    // Parse keyType (partition key type)
    let (input, pk_type_len) = parse_vuint(input)?;
    if pk_type_len == 0 || pk_type_len > 5000 {
        tracing::debug!("Invalid pk_type_len: {}", pk_type_len);
        return Err(HeaderSchemaError::structural_at(input));
    }
    if pk_type_len > 1000 {
        tracing::warn!(
            "Unusually long partition key type string: {} bytes (typical <1000)",
            pk_type_len
        );
    }

    let (input, pk_type_bytes) = take(pk_type_len as usize)(input)?;
    let partition_key_type = match std::str::from_utf8(pk_type_bytes) {
        // Gate 2 of #4104: a header type that freezes a scalar is not writable by
        // Cassandra, so refuse the HEADER rather than read a type it cannot have
        // recorded. This is a SEMANTIC refusal — the bytes decoded, and what they
        // declare cannot exist — so it leaves this function as
        // `HeaderSchemaError::Refused`, carrying the message (with its
        // `CQL3Type.java:647-651` citation) verbatim. The caller must NOT retry it
        // through the marker-search fallback (roborev job 116).
        Ok(s) => match convert_marshal_type_to_cql_checked(s) {
            Ok(t) => t,
            Err(e) => {
                return Err(HeaderSchemaError::refused(format!(
                    "SerializationHeader partition key type is not writable by Cassandra: {e}"
                )));
            }
        },
        Err(_) => {
            tracing::debug!("Invalid UTF-8 in partition key type");
            return Err(HeaderSchemaError::structural_at(input));
        }
    };

    tracing::debug!(
        "HEADER: Partition key type: {} ({} bytes)",
        partition_key_type,
        pk_type_len
    );

    // Step 3: Parse clusteringTypes
    let (input, clustering_count) = parse_vuint(input)?;
    // Sanity check: Cassandra tables rarely have >100 clustering keys
    if clustering_count > 1000 {
        tracing::warn!(
            "Suspicious clustering_count={} in SerializationHeader (expected <100)",
            clustering_count
        );
        return Err(HeaderSchemaError::structural_at(input));
    }
    tracing::debug!("HEADER: {} clustering key types", clustering_count);

    let mut input = input;
    let mut clustering_key_types = Vec::with_capacity(clustering_count as usize);

    for i in 0..clustering_count {
        let (remaining, ck_type_len) = parse_vuint(input)?;
        if ck_type_len == 0 || ck_type_len > 5000 {
            tracing::debug!("Invalid clustering key type length: {}", ck_type_len);
            return Err(HeaderSchemaError::structural_at(input));
        }
        if ck_type_len > 1000 {
            tracing::warn!(
                "Unusually long clustering key type string: {} bytes (typical <1000)",
                ck_type_len
            );
        }

        let (remaining, ck_type_bytes) = take(ck_type_len as usize)(remaining)?;
        // Issue #759: preserve the RAW comparator class name (including any
        // `ReversedType(...)` wrapper) here. `build_clustering_key_columns` is
        // the single place that converts to a CQL type AND derives clustering
        // order from the wrapper, so converting eagerly would discard the DESC
        // signal. Conversion in `build_clustering_key_columns` is idempotent for
        // already-CQL strings, keeping the other parse paths correct.
        let ck_type = match std::str::from_utf8(ck_type_bytes) {
            Ok(s) => s.to_string(),
            Err(_) => {
                tracing::debug!("Invalid UTF-8 in clustering key type {}", i);
                return Err(HeaderSchemaError::structural_at(input));
            }
        };

        tracing::debug!(
            "HEADER: Clustering key {}: {} ({} bytes)",
            i,
            ck_type,
            ck_type_len
        );
        clustering_key_types.push(ck_type);
        input = remaining;
    }

    // Step 4: Parse staticColumns
    let (input, static_count) = parse_vuint(input)?;
    // Sanity check: Cassandra tables rarely have >1000 static columns
    if static_count > 10000 {
        tracing::warn!(
            "Suspicious static_count={} in SerializationHeader (expected <1000)",
            static_count
        );
        return Err(HeaderSchemaError::structural_at(input));
    }
    tracing::debug!("HEADER: {} static columns", static_count);

    let mut input = input;
    let mut static_columns = Vec::with_capacity(static_count as usize);

    for i in 0..static_count {
        // Column name
        let (remaining, name_len) = parse_vuint(input)?;
        if name_len == 0 || name_len > 200 {
            tracing::debug!("Invalid static column name length: {}", name_len);
            return Err(HeaderSchemaError::structural_at(input));
        }

        let (remaining, name_bytes) = take(name_len as usize)(remaining)?;
        let column_name = match std::str::from_utf8(name_bytes) {
            Ok(s) => s.to_string(),
            Err(_) => {
                tracing::debug!("Invalid UTF-8 in static column name {}", i);
                return Err(HeaderSchemaError::structural_at(input));
            }
        };

        // Column type
        let (remaining, type_len) = parse_vuint(remaining)?;
        if type_len == 0 || type_len > 5000 {
            tracing::debug!("Invalid static column type length: {}", type_len);
            return Err(HeaderSchemaError::structural_at(input));
        }
        if type_len > 1000 {
            tracing::warn!(
                "Unusually long static column type string: {} bytes (typical <1000)",
                type_len
            );
        }

        let (remaining, type_bytes) = take(type_len as usize)(remaining)?;
        let cql_type = match std::str::from_utf8(type_bytes) {
            // Gate 2 of #4104 — see the partition-key site for the reasoning.
            // SEMANTIC: propagated as `Refused`, never retried heuristically.
            Ok(s) => match convert_marshal_type_to_cql_checked(s) {
                Ok(t) => t,
                Err(e) => {
                    return Err(HeaderSchemaError::refused(format!(
                        "SerializationHeader static column {i} ('{column_name}') type is not \
                         writable by Cassandra: {e}"
                    )));
                }
            },
            Err(_) => {
                tracing::debug!("Invalid UTF-8 in static column type {}", i);
                return Err(HeaderSchemaError::structural_at(input));
            }
        };

        tracing::debug!(
            "HEADER: Static column '{}': {} ({} bytes)",
            column_name,
            cql_type,
            type_len
        );

        static_columns.push(ColumnInfo {
            name: column_name,
            column_type: cql_type,
            is_primary_key: false,
            key_position: None,
            is_static: true,
            is_clustering: false,
            clustering_reversed: false,
        });

        input = remaining;
    }

    // Step 5: Parse regularColumns
    let (input, regular_count) = parse_vuint(input)?;
    // Sanity check: Cassandra tables rarely have >1000 regular columns
    if regular_count > 10000 {
        tracing::warn!(
            "Suspicious regular_count={} in SerializationHeader (expected <1000)",
            regular_count
        );
        return Err(HeaderSchemaError::structural_at(input));
    }
    tracing::debug!("HEADER: {} regular columns", regular_count);

    let mut input = input;
    let mut regular_columns = Vec::with_capacity(regular_count as usize);

    for i in 0..regular_count {
        // Column name
        let (remaining, name_len) = parse_vuint(input)?;
        if name_len == 0 || name_len > 200 {
            tracing::debug!("Invalid regular column name length: {}", name_len);
            return Err(HeaderSchemaError::structural_at(input));
        }

        let (remaining, name_bytes) = take(name_len as usize)(remaining)?;
        let column_name = match std::str::from_utf8(name_bytes) {
            Ok(s) => s.to_string(),
            Err(_) => {
                tracing::debug!("Invalid UTF-8 in regular column name {}", i);
                return Err(HeaderSchemaError::structural_at(input));
            }
        };

        // Column type
        let (remaining, type_len) = parse_vuint(remaining)?;
        if type_len == 0 || type_len > 5000 {
            tracing::debug!("Invalid regular column type length: {}", type_len);
            return Err(HeaderSchemaError::structural_at(input));
        }
        if type_len > 1000 {
            tracing::warn!(
                "Unusually long regular column type string: {} bytes (typical <1000)",
                type_len
            );
        }

        let (remaining, type_bytes) = take(type_len as usize)(remaining)?;
        let cql_type = match std::str::from_utf8(type_bytes) {
            // Gate 2 of #4104 — see the partition-key site for the reasoning.
            // SEMANTIC: propagated as `Refused`, never retried heuristically.
            Ok(s) => match convert_marshal_type_to_cql_checked(s) {
                Ok(t) => t,
                Err(e) => {
                    return Err(HeaderSchemaError::refused(format!(
                        "SerializationHeader regular column {i} ('{column_name}') type is not \
                         writable by Cassandra: {e}"
                    )));
                }
            },
            Err(_) => {
                tracing::debug!("Invalid UTF-8 in regular column type {}", i);
                return Err(HeaderSchemaError::structural_at(input));
            }
        };

        tracing::debug!(
            "HEADER: Regular column '{}': {} ({} bytes)",
            column_name,
            cql_type,
            type_len
        );

        regular_columns.push(ColumnInfo {
            name: column_name,
            column_type: cql_type,
            is_primary_key: false,
            key_position: None,
            is_static: false,
            is_clustering: false,
            clustering_reversed: false,
        });

        input = remaining;
    }

    // Combine static and regular columns
    let mut all_columns = static_columns;
    all_columns.extend(regular_columns);

    tracing::debug!(
        "HEADER parsing complete: partition_key='{}', {} clustering keys, {} total columns",
        partition_key_type,
        clustering_key_types.len(),
        all_columns.len()
    );

    Ok((
        input,
        (vec![partition_key_type], clustering_key_types, all_columns),
    ))
}
