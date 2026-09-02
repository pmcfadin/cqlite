//! Direct (offset-anchored) SerializationHeader parsers.
//!
//! Both routines here assume the caller already knows where the schema starts —
//! either at a legacy `0x00 0x00` marker (`parse_serialization_header_at_offset`)
//! or exactly at the partition-key type length VInt
//! (`parse_serialization_header_sequential`, used after EncodingStats). The
//! marker-search dispatcher in the parent module picks which to call.

use super::super::super::header::ColumnInfo;
use super::super::super::vint::parse_vuint;
use super::super::marshal_type::convert_marshal_type_to_cql;
use super::super::SerializationHeaderResult;
use nom::IResult;

/// Parse SerializationHeader structure starting at a known offset
pub(super) fn parse_serialization_header_at_offset(
    input: &[u8],
) -> IResult<&[u8], SerializationHeaderResult> {
    use nom::bytes::complete::tag;
    use nom::number::complete::u8 as parse_u8;

    let _original_input = input;

    // Step 1: Expect 0x00 0x00 marker
    let (input, _) = tag(b"\x00\x00")(input)?;
    tracing::debug!("Found 0x00 0x00 marker");

    // Step 2: Parse partition key type (single byte length + string)
    let (input, partition_type_len) = parse_u8(input)?;
    tracing::debug!("Partition key type length: {} bytes", partition_type_len);

    let (input, partition_type_bytes) =
        nom::bytes::complete::take(partition_type_len as usize)(input)?;
    let partition_key_type = std::str::from_utf8(partition_type_bytes)
        .map_err(|_| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)))?
        .to_string();

    tracing::debug!("Partition key type: {}", partition_key_type);

    // Step 3: Parse clustering key count (single byte)
    let (input, clustering_count) = parse_u8(input)?;
    tracing::debug!("Clustering key count: {}", clustering_count);

    // Step 4: Parse clustering key types
    let mut clustering_key_types = Vec::with_capacity(clustering_count as usize);
    let mut input = input;

    for idx in 0..clustering_count {
        // Parse clustering type length (single byte)
        let (remaining, type_len) = parse_u8(input)?;
        tracing::debug!("Clustering key {} type length: {} bytes", idx, type_len);

        let (remaining, type_bytes) = nom::bytes::complete::take(type_len as usize)(remaining)?;
        let clustering_type = std::str::from_utf8(type_bytes)
            .map_err(|_| {
                nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
            })?
            .to_string();

        tracing::debug!("Clustering key {} type: {}", idx, clustering_type);

        clustering_key_types.push(clustering_type);
        input = remaining;
    }

    // Step 5: Parse static column count (NOT a separator - this was the bug!)
    // When static_count = 0, this byte is 0x00 which made simple tables work.
    // But when static_count > 0, parsing failed.
    let (input, static_count) = parse_u8(input)?;
    tracing::debug!("Static column count: {}", static_count);

    // Step 5a: Parse static columns
    let mut static_columns = Vec::with_capacity(static_count as usize);
    let mut input = input;

    for static_idx in 0..static_count {
        // Static column name length (single byte)
        let (remaining, name_len) = parse_u8(input)?;
        tracing::debug!(
            "Static column {} name length: {} bytes",
            static_idx,
            name_len
        );

        // Validate name length (match validation in parse_regular_columns)
        if name_len == 0 || name_len > 200 {
            tracing::debug!(
                "Static column {} name_len sanity check failed: {}",
                static_idx,
                name_len
            );
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
        }

        // Static column name (UTF-8 string)
        let (remaining, name_bytes) = nom::bytes::complete::take(name_len as usize)(remaining)?;
        let column_name = std::str::from_utf8(name_bytes)
            .map_err(|_| {
                nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
            })?
            .to_string();

        // Static column type length (VInt - can exceed 127 for collection types)
        let (remaining, type_len_u64) = parse_vuint(remaining)?;
        tracing::debug!(
            "Static column {} ('{}') type length: {} bytes",
            static_idx,
            column_name,
            type_len_u64
        );

        // Validate type length (match validation in parse_regular_columns)
        if type_len_u64 == 0 || type_len_u64 > 5000 {
            tracing::debug!(
                "Static column {} ('{}') type_len sanity check failed: {}",
                static_idx,
                column_name,
                type_len_u64
            );
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
        }
        if type_len_u64 > 1000 {
            tracing::warn!(
                "Unusually long static column type string: {} bytes (typical <1000)",
                type_len_u64
            );
        }

        // Static column type (UTF-8 string)
        let (remaining, type_bytes) = nom::bytes::complete::take(type_len_u64 as usize)(remaining)?;
        let internal_type = std::str::from_utf8(type_bytes)
            .map_err(|_| {
                nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
            })?
            .to_string();

        let cql_type = convert_marshal_type_to_cql(&internal_type);

        tracing::debug!(
            "Static column {}: name='{}', type='{}' (CQL: '{}')",
            static_idx,
            column_name,
            internal_type,
            cql_type
        );

        static_columns.push(ColumnInfo {
            name: column_name,
            column_type: cql_type,
            is_primary_key: false,
            key_position: None,
            is_static: true, // Mark as static column!
            is_clustering: false,
            clustering_reversed: false,
        });

        input = remaining;
    }

    tracing::debug!("Parsed {} static columns", static_columns.len());

    // Step 6: Parse regular column count (single byte)
    let (mut input, column_count) = parse_u8(input)?;
    tracing::debug!("Regular column count: {}", column_count);

    // Step 7: Parse each regular column
    let mut columns = Vec::with_capacity(column_count as usize + static_columns.len());

    for col_idx in 0..column_count {
        // Column name length (single byte)
        let (remaining, name_len) = parse_u8(input)?;
        tracing::debug!("Column {} name length: {} bytes", col_idx, name_len);

        // Column name (UTF-8 string)
        let (remaining, name_bytes) = nom::bytes::complete::take(name_len as usize)(remaining)?;
        let column_name = std::str::from_utf8(name_bytes)
            .map_err(|_| {
                nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
            })?
            .to_string();

        // Column type length (VInt - can exceed 127 for collection types)
        let (remaining, type_len_u64) = parse_vuint(remaining)?;
        tracing::debug!(
            "Column {} ('{}') type length: {} bytes",
            col_idx,
            column_name,
            type_len_u64
        );

        // Validate type length (consistent with parse_regular_columns and static columns)
        if type_len_u64 == 0 || type_len_u64 > 5000 {
            tracing::debug!(
                "Column {} ('{}') type_len validation failed: {}",
                col_idx,
                column_name,
                type_len_u64
            );
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
        }
        if type_len_u64 > 1000 {
            tracing::warn!(
                "Unusually long column type string: {} bytes (typical <1000)",
                type_len_u64
            );
        }

        // Column type (UTF-8 string)
        let (remaining, type_bytes) = nom::bytes::complete::take(type_len_u64 as usize)(remaining)?;
        let internal_type = std::str::from_utf8(type_bytes)
            .map_err(|_| {
                nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
            })?
            .to_string();

        input = remaining;

        // Convert to CQL type
        let cql_type = convert_marshal_type_to_cql(&internal_type);

        tracing::debug!(
            "Column {}: name='{}', type='{}' (CQL: '{}')",
            col_idx,
            column_name,
            internal_type,
            cql_type
        );

        columns.push(ColumnInfo {
            name: column_name,
            column_type: cql_type,
            is_primary_key: false,
            key_position: None,
            is_static: false,
            is_clustering: false,
            clustering_reversed: false,
        });
    }

    // Merge static columns (first) with regular columns
    // Static columns come before regular columns in the combined list
    let mut all_columns = static_columns;
    all_columns.append(&mut columns);

    tracing::debug!(
        "Successfully parsed SerializationHeader: {} partition keys, {} clustering keys, {} static columns, {} regular columns ({} total)",
        1, // Always 1 partition key in current implementation
        clustering_key_types.len(),
        all_columns.iter().filter(|c| c.is_static).count(),
        all_columns.iter().filter(|c| !c.is_static).count(),
        all_columns.len()
    );

    Ok((
        input,
        (vec![partition_key_type], clustering_key_types, all_columns),
    ))
}

/// Parse SerializationHeader using sequential VInt parsing (Issue #216)
///
/// This function assumes the input starts EXACTLY at the SerializationHeader
/// (immediately after EncodingStats). It does NOT search for markers.
///
/// Format (from SerializationHeader.java):
/// [VInt pk_type_len] [pk_type_string]
/// [VInt ck_count] [for each: VInt ck_type_len, ck_type_string]
/// [VInt static_count] [for each: VInt name_len, name, VInt type_len, type]
/// [VInt regular_count] [for each: VInt name_len, name, VInt type_len, type]
pub(super) fn parse_serialization_header_sequential(
    input: &[u8],
) -> IResult<&[u8], SerializationHeaderResult> {
    // Step 1: Parse partition key type (VInt length + string)
    let (input, pk_type_len) = parse_vuint(input)?;

    // Validate partition key type length
    if pk_type_len == 0 || pk_type_len > 5000 {
        tracing::debug!(
            "Invalid partition key type length: {} (expected 1-2000)",
            pk_type_len
        );
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }

    let (input, pk_type_bytes) = nom::bytes::complete::take(pk_type_len as usize)(input)?;
    let partition_key_type = std::str::from_utf8(pk_type_bytes)
        .map_err(|_| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)))?
        .to_string();

    tracing::debug!(
        "Sequential parser: partition key type (len={}): {}",
        pk_type_len,
        partition_key_type
    );

    // Step 2: Parse clustering key count and types
    let (input, clustering_count) = parse_vuint(input)?;

    if clustering_count > 100 {
        tracing::debug!(
            "Invalid clustering key count: {} (expected 0-100)",
            clustering_count
        );
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }

    // #3848: narrow only AFTER the bound above, which compares the RAW `u64`.
    // Casting first let `(1 << 32) + n` pass the bound as `n` on a 32-bit target.
    let clustering_count = clustering_count as usize;

    tracing::debug!(
        "Sequential parser: clustering key count: {}",
        clustering_count
    );

    let mut clustering_key_types = Vec::with_capacity(clustering_count);
    let mut input = input;

    for idx in 0..clustering_count {
        let (remaining, type_len) = parse_vuint(input)?;

        if type_len == 0 || type_len > 5000 {
            tracing::debug!("Invalid clustering key {} type length: {}", idx, type_len);
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
        }

        let (remaining, type_bytes) = nom::bytes::complete::take(type_len as usize)(remaining)?;
        let clustering_type = std::str::from_utf8(type_bytes)
            .map_err(|_| {
                nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
            })?
            .to_string();

        tracing::debug!(
            "Sequential parser: clustering key {} type (len={}): {}",
            idx,
            type_len,
            clustering_type
        );

        clustering_key_types.push(clustering_type);
        input = remaining;
    }

    // Step 3: Parse static columns
    let (input, static_count) = parse_vuint(input)?;

    if static_count > 200 {
        tracing::debug!(
            "Invalid static column count: {} (expected 0-200)",
            static_count
        );
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }

    // #3848: narrow only AFTER the bound above, which compares the RAW `u64`.
    // Casting first let `(1 << 32) + n` pass the bound as `n` on a 32-bit target.
    let static_count = static_count as usize;

    tracing::debug!("Sequential parser: static column count: {}", static_count);

    let mut static_columns = Vec::with_capacity(static_count);
    let mut input = input;

    for idx in 0..static_count {
        // Column name (VInt length + UTF-8)
        let (remaining, name_len) = parse_vuint(input)?;

        if name_len == 0 || name_len > 200 {
            tracing::debug!("Invalid static column {} name length: {}", idx, name_len);
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
        }

        let (remaining, name_bytes) = nom::bytes::complete::take(name_len as usize)(remaining)?;
        let column_name = std::str::from_utf8(name_bytes)
            .map_err(|_| {
                nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
            })?
            .to_string();

        // Column type (VInt length + UTF-8)
        let (remaining, type_len) = parse_vuint(remaining)?;

        if type_len == 0 || type_len > 5000 {
            tracing::debug!(
                "Invalid static column '{}' type length: {}",
                column_name,
                type_len
            );
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
        }

        let (remaining, type_bytes) = nom::bytes::complete::take(type_len as usize)(remaining)?;
        let internal_type = std::str::from_utf8(type_bytes)
            .map_err(|_| {
                nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
            })?
            .to_string();

        let cql_type = convert_marshal_type_to_cql(&internal_type);

        tracing::debug!(
            "Sequential parser: static column {}: name='{}', type='{}'",
            idx,
            column_name,
            cql_type
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

    // Step 4: Parse regular columns
    let (input, regular_count) = parse_vuint(input)?;

    if regular_count > 500 {
        tracing::debug!(
            "Invalid regular column count: {} (expected 0-500)",
            regular_count
        );
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }

    // #3848: narrow only AFTER the bound above, which compares the RAW `u64`.
    // Casting first let `(1 << 32) + n` pass the bound as `n` on a 32-bit target.
    let regular_count = regular_count as usize;

    tracing::debug!("Sequential parser: regular column count: {}", regular_count);

    let mut regular_columns = Vec::with_capacity(regular_count);
    let mut input = input;

    for idx in 0..regular_count {
        // Column name (VInt length + UTF-8)
        let (remaining, name_len) = parse_vuint(input)?;

        if name_len == 0 || name_len > 200 {
            tracing::debug!("Invalid regular column {} name length: {}", idx, name_len);
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
        }

        let (remaining, name_bytes) = nom::bytes::complete::take(name_len as usize)(remaining)?;
        let column_name = std::str::from_utf8(name_bytes)
            .map_err(|_| {
                nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
            })?
            .to_string();

        // Column type (VInt length + UTF-8)
        let (remaining, type_len) = parse_vuint(remaining)?;

        if type_len == 0 || type_len > 5000 {
            tracing::debug!(
                "Invalid regular column '{}' type length: {}",
                column_name,
                type_len
            );
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
        }

        let (remaining, type_bytes) = nom::bytes::complete::take(type_len as usize)(remaining)?;
        let internal_type = std::str::from_utf8(type_bytes)
            .map_err(|_| {
                nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
            })?
            .to_string();

        let cql_type = convert_marshal_type_to_cql(&internal_type);

        tracing::debug!(
            "Sequential parser: regular column {}: name='{}', type='{}'",
            idx,
            column_name,
            cql_type
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

    // Combine static and regular columns (static columns first)
    let mut all_columns = static_columns;
    all_columns.extend(regular_columns);

    tracing::debug!(
        "Sequential parser complete: partition_key='{}', {} clustering keys, {} total columns",
        partition_key_type,
        clustering_key_types.len(),
        all_columns.len()
    );

    Ok((
        input,
        (vec![partition_key_type], clustering_key_types, all_columns),
    ))
}
