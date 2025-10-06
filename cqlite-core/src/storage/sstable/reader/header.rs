//! SSTable header parsing and detection
//!
//! This module handles header parsing, version detection, and conversion
//! for different Cassandra SSTable formats.

use std::path::Path;

use crate::{
    parser::header::{
        parse_sstable_header, CassandraVersion, ColumnInfo, CompressionInfo, SSTableHeader,
        SSTableStats, SUPPORTED_MAGIC_NUMBERS,
    },
    Error, Result,
};

use super::super::header_spec::{get_global_registry, ParsedHeader};

// Re-export helper functions from header_helpers module
pub(crate) use super::header_helpers::{
    calculate_actual_header_size, extract_generation_from_path,
};

/// Check if a value appears to be ASCII corruption
pub(crate) fn is_ascii_corruption_value(value: u32) -> bool {
    // Check for known corrupted values
    match value {
        2959239534 | 1684108385 => return true, // "bin" and "data"
        _ => {}
    }

    // Convert to bytes and check if they look like ASCII text
    let bytes = value.to_be_bytes();
    let ascii_count = bytes
        .iter()
        .filter(|&&b| (0x20..=0x7E).contains(&b))
        .count();

    // If 3 or more bytes are printable ASCII, likely corruption
    ascii_count >= 3
}

/// Detect ASCII corruption in header buffer
pub(crate) fn detect_ascii_header_corruption(header: &[u8]) -> bool {
    if header.len() < 4 {
        return false;
    }

    // Check for common ASCII corruption patterns in header
    let chunk = &header[0..4];
    let ascii_patterns = [
        b"data", b"node", b"temp", b"logs", b"meta", b"home", b"root",
    ];

    for pattern in &ascii_patterns {
        if chunk == *pattern {
            return true;
        }
    }

    // Check if all 4 bytes are printable ASCII
    let ascii_count = chunk
        .iter()
        .filter(|&&b| (0x20..=0x7E).contains(&b))
        .count();
    ascii_count >= 3
}

/// Enhanced header parsing with version detection using spec-driven approach
pub(crate) async fn parse_header_with_version_detection(
    header_buffer: &[u8],
    path: &Path,
) -> Result<SSTableHeader> {
    // Validate minimum header size
    if header_buffer.len() < 8 {
        return Err(Error::corruption(format!(
            "Header buffer too small for parsing: {} bytes (minimum 8 bytes required). \
             File: {}",
            header_buffer.len(),
            path.display()
        )));
    }

    // First try spec-driven parsing for Data.db component
    let registry = get_global_registry();
    match registry.parse_data_header(header_buffer) {
        Ok(parsed_header) => {
            log::debug!(
                "Successfully parsed Data.db header using spec-driven approach for file '{}' \
                 with version: {:?}",
                path.display(),
                parsed_header.cassandra_version
            );

            // Convert ParsedHeader to SSTableHeader for compatibility
            return convert_parsed_header_to_sstable_header(parsed_header, header_buffer);
        }
        Err(spec_error) => {
            log::debug!(
                "Spec-driven parsing failed for file '{}', falling back to legacy parser: {}",
                path.display(),
                spec_error
            );
        }
    }

    // Fallback to legacy parsing approach
    // Extract and validate magic number
    let magic_bytes = &header_buffer[0..4];
    let magic = u32::from_be_bytes([
        magic_bytes[0],
        magic_bytes[1],
        magic_bytes[2],
        magic_bytes[3],
    ]);

    // Validate magic number against supported formats
    if !SUPPORTED_MAGIC_NUMBERS.contains(&magic) {
        return Err(Error::unsupported_format(format!(
            "Unsupported SSTable format: magic number 0x{:08x} not recognized. \
             Supported formats: {:?}. File: {}. \
             This may indicate file corruption or an unsupported Cassandra version.",
            magic,
            SUPPORTED_MAGIC_NUMBERS
                .iter()
                .map(|m| format!("0x{:08x}", m))
                .collect::<Vec<_>>(),
            path.display()
        )));
    }

    // Detect Cassandra version from magic number
    let cassandra_version = CassandraVersion::from_magic_number(magic).ok_or_else(|| {
        Error::corruption(format!(
            "Failed to map magic number 0x{:08x} to Cassandra version. File: {}",
            magic,
            path.display()
        ))
    })?;

    // Try to parse using the existing header parser
    match parse_sstable_header(header_buffer) {
        Ok((_, header)) => {
            log::debug!(
                "Successfully parsed header for file '{}' with version: {:?}",
                path.display(),
                header.cassandra_version
            );
            Ok(header)
        }
        Err(parse_error) => {
            // For legacy formats, allow minimal header parsing if feature is enabled
            if cassandra_version == CassandraVersion::Legacy {
                #[cfg(feature = "legacy-heuristics")]
                {
                    log::warn!(
                        "Failed to parse full header for legacy format file '{}', \
                         attempting minimal legacy header parsing: {:?}",
                        path.display(),
                        parse_error
                    );

                    // Only create minimal header for verified legacy format
                    parse_minimal_legacy_header(header_buffer, path, cassandra_version)
                }
                #[cfg(not(feature = "legacy-heuristics"))]
                {
                    Err(Error::unsupported_format(format!(
                        "Legacy SSTable format detected but legacy-heuristics feature is disabled. \
                         Enable feature for backward compatibility. File: {}. Parse error: {:?}",
                        path.display(),
                        parse_error
                    )))
                }
            } else {
                // For modern formats, strict parsing is required
                Err(Error::corruption(format!(
                    "Failed to parse header for modern format {:?} file '{}': {:?}. \
                     This indicates file corruption or format incompatibility.",
                    cassandra_version,
                    path.display(),
                    parse_error
                )))
            }
        }
    }
}

/// Convert ParsedHeader from spec-driven parsing to SSTableHeader for compatibility
pub(crate) fn convert_parsed_header_to_sstable_header(
    parsed_header: ParsedHeader,
    _header_buffer: &[u8],
) -> Result<SSTableHeader> {
    use std::collections::HashMap;

    // Extract required fields with proper error handling
    let table_id = parsed_header
        .fields
        .get("table_id")
        .and_then(|v| v.as_bytes().ok())
        .and_then(|bytes| {
            if bytes.len() == 16 {
                let mut id = [0u8; 16];
                id.copy_from_slice(bytes);
                Some(id)
            } else {
                None
            }
        })
        .unwrap_or([0u8; 16]);

    let keyspace = parsed_header
        .fields
        .get("keyspace")
        .and_then(|v| v.as_string().ok())
        .unwrap_or("unknown")
        .to_string();

    let table_name = parsed_header
        .fields
        .get("table_name")
        .and_then(|v| v.as_string().ok())
        .unwrap_or("unknown")
        .to_string();

    let generation = parsed_header
        .fields
        .get("generation")
        .and_then(|v| v.as_u64().ok())
        .unwrap_or(0);

    // Create default compression info (would be enhanced with actual compression parsing)
    let compression = CompressionInfo {
        algorithm: "NONE".to_string(),
        chunk_size: 4096,
        parameters: HashMap::new(),
    };

    // Create default stats (would be enhanced with actual stats parsing)
    let stats = SSTableStats {
        row_count: 0,
        min_timestamp: 0,
        max_timestamp: 0,
        max_deletion_time: 0,
        compression_ratio: 1.0,
        row_size_histogram: Vec::new(),
    };

    // Create default columns (would be enhanced with actual column parsing)
    let columns = Vec::<ColumnInfo>::new();

    // Create default properties
    let properties = HashMap::new();

    Ok(SSTableHeader {
        cassandra_version: parsed_header.cassandra_version,
        version: parsed_header.format_version as u16,
        table_id,
        keyspace,
        table_name,
        generation,
        compression,
        stats,
        columns,
        properties,
    })
}

/// Parse minimal legacy header with strict validation (feature-gated)
#[cfg(feature = "legacy-heuristics")]
pub(crate) fn parse_minimal_legacy_header(
    header_buffer: &[u8],
    path: &Path,
    cassandra_version: CassandraVersion,
) -> Result<SSTableHeader> {
    use crate::parser::header::SUPPORTED_VERSION;
    // Extract version if available
    let version = if header_buffer.len() >= 6 {
        u16::from_be_bytes([header_buffer[4], header_buffer[5]])
    } else {
        log::warn!(
            "Legacy header too short for version extraction, using default version. File: {}",
            path.display()
        );
        SUPPORTED_VERSION
    };

    // Validate version is reasonable
    if version > 100 {
        // Sanity check for version
        return Err(Error::corruption(format!(
            "Invalid version {} in legacy header. File: {}",
            version,
            path.display()
        )));
    }

    log::info!(
        "Creating minimal legacy header for file '{}' with version {}",
        path.display(),
        version
    );

    Ok(SSTableHeader {
        cassandra_version,
        version,
        table_id: [0; 16], // Zero-filled for legacy compatibility
        keyspace: path
            .parent()
            .and_then(|p| p.file_name())
            .and_then(|n| n.to_str())
            .map(|s| s.split('-').next().unwrap_or("unknown").to_string())
            .unwrap_or_else(|| "unknown".to_string()),
        table_name: path
            .file_stem()
            .and_then(|n| n.to_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| "unknown".to_string()),
        generation: extract_generation_from_path(path),
        compression: CompressionInfo {
            algorithm: "NONE".to_string(),
            chunk_size: 0,
            parameters: std::collections::HashMap::new(),
        },
        stats: SSTableStats {
            row_count: 0,
            min_timestamp: 0,
            max_timestamp: 0,
            max_deletion_time: 0,
            compression_ratio: 1.0,
            row_size_histogram: vec![],
        },
        columns: vec![],
        properties: std::collections::HashMap::new(),
    })
}
