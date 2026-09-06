//! Post-EncodingStats SerializationHeader schema parser.
//!
//! `parse_serialization_header_schema` is the authoritative sequential decoder
//! invoked once the TOC offset has positioned us immediately after the three
//! EncodingStats VInts. It walks keyType, clusteringTypes, staticColumns, and
//! regularColumns in their on-disk order (see SerializationHeader.java).

use super::super::super::header::ColumnInfo;
use super::super::super::vint::parse_vuint;
use super::super::marshal_type::convert_marshal_type_to_cql_checked;
use super::super::SerializationHeaderResult;
use nom::{bytes::complete::take, IResult};

/// Parse the schema portion of a SerializationHeader (after EncodingStats have been consumed).
///
/// Format:
/// 1. keyType (VInt length + UTF-8 type string)
/// 2. clusteringTypes (VInt count + [VInt type_len + type]*)
/// 3. staticColumns (VInt count + [VInt name_len + name + VInt type_len + type]*)
/// 4. regularColumns (VInt count + [VInt name_len + name + VInt type_len + type]*)
pub(in crate::parser::enhanced_statistics_parser) fn parse_serialization_header_schema(
    input: &[u8],
) -> IResult<&[u8], SerializationHeaderResult> {
    // Parse keyType (partition key type)
    let (input, pk_type_len) = parse_vuint(input)?;
    if pk_type_len == 0 || pk_type_len > 5000 {
        tracing::debug!("Invalid pk_type_len: {}", pk_type_len);
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
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
        // recorded. The nom channel carries no message, so the refusal — which does
        // carry the `CQL3Type.java:647-651` citation — is logged before it is
        // discarded.
        Ok(s) => match convert_marshal_type_to_cql_checked(s) {
            Ok(t) => t,
            Err(e) => {
                tracing::error!("Refusing SerializationHeader partition key type: {e}");
                return Err(nom::Err::Error(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Verify,
                )));
            }
        },
        Err(_) => {
            tracing::debug!("Invalid UTF-8 in partition key type");
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
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
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }
    tracing::debug!("HEADER: {} clustering key types", clustering_count);

    let mut input = input;
    let mut clustering_key_types = Vec::with_capacity(clustering_count as usize);

    for i in 0..clustering_count {
        let (remaining, ck_type_len) = parse_vuint(input)?;
        if ck_type_len == 0 || ck_type_len > 5000 {
            tracing::debug!("Invalid clustering key type length: {}", ck_type_len);
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
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
                return Err(nom::Err::Error(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Verify,
                )));
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
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }
    tracing::debug!("HEADER: {} static columns", static_count);

    let mut input = input;
    let mut static_columns = Vec::with_capacity(static_count as usize);

    for i in 0..static_count {
        // Column name
        let (remaining, name_len) = parse_vuint(input)?;
        if name_len == 0 || name_len > 200 {
            tracing::debug!("Invalid static column name length: {}", name_len);
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
        }

        let (remaining, name_bytes) = take(name_len as usize)(remaining)?;
        let column_name = match std::str::from_utf8(name_bytes) {
            Ok(s) => s.to_string(),
            Err(_) => {
                tracing::debug!("Invalid UTF-8 in static column name {}", i);
                return Err(nom::Err::Error(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Verify,
                )));
            }
        };

        // Column type
        let (remaining, type_len) = parse_vuint(remaining)?;
        if type_len == 0 || type_len > 5000 {
            tracing::debug!("Invalid static column type length: {}", type_len);
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
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
            Ok(s) => match convert_marshal_type_to_cql_checked(s) {
                Ok(t) => t,
                Err(e) => {
                    tracing::error!("Refusing SerializationHeader static column {i} type: {e}");
                    return Err(nom::Err::Error(nom::error::Error::new(
                        input,
                        nom::error::ErrorKind::Verify,
                    )));
                }
            },
            Err(_) => {
                tracing::debug!("Invalid UTF-8 in static column type {}", i);
                return Err(nom::Err::Error(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Verify,
                )));
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
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }
    tracing::debug!("HEADER: {} regular columns", regular_count);

    let mut input = input;
    let mut regular_columns = Vec::with_capacity(regular_count as usize);

    for i in 0..regular_count {
        // Column name
        let (remaining, name_len) = parse_vuint(input)?;
        if name_len == 0 || name_len > 200 {
            tracing::debug!("Invalid regular column name length: {}", name_len);
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
        }

        let (remaining, name_bytes) = take(name_len as usize)(remaining)?;
        let column_name = match std::str::from_utf8(name_bytes) {
            Ok(s) => s.to_string(),
            Err(_) => {
                tracing::debug!("Invalid UTF-8 in regular column name {}", i);
                return Err(nom::Err::Error(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Verify,
                )));
            }
        };

        // Column type
        let (remaining, type_len) = parse_vuint(remaining)?;
        if type_len == 0 || type_len > 5000 {
            tracing::debug!("Invalid regular column type length: {}", type_len);
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
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
            Ok(s) => match convert_marshal_type_to_cql_checked(s) {
                Ok(t) => t,
                Err(e) => {
                    tracing::error!("Refusing SerializationHeader regular column {i} type: {e}");
                    return Err(nom::Err::Error(nom::error::Error::new(
                        input,
                        nom::error::ErrorKind::Verify,
                    )));
                }
            },
            Err(_) => {
                tracing::debug!("Invalid UTF-8 in regular column type {}", i);
                return Err(nom::Err::Error(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Verify,
                )));
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
