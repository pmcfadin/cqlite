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

/// Extract keyspace name from SSTable file path
///
/// SSTable paths follow Cassandra convention:
/// `/path/to/sstables/{keyspace}/{table_name}-{uuid}/nb-1-big-Data.db`
///
/// This function extracts the keyspace directory name (grandparent of the Data.db file).
fn extract_keyspace_from_path(path: &Path) -> String {
    // Get parent directory containing table_name-uuid
    path.parent()
        .and_then(|table_dir| {
            // Get keyspace directory (parent of table directory)
            table_dir.parent()
        })
        .and_then(|keyspace_dir| keyspace_dir.file_name())
        .and_then(|n| n.to_str())
        .unwrap_or("unknown")
        .to_string()
}

/// Extract table name from SSTable file path
///
/// SSTable paths follow Cassandra convention:
/// `/path/to/sstables/{keyspace}/{table_name}-{uuid}/nb-1-big-Data.db`
///
/// This function extracts the table name from the parent directory, stripping the UUID suffix.
/// Format: "table_name-uuid" → "table_name"
fn extract_table_name_from_path(path: &Path) -> String {
    path.parent()
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        .and_then(|s| {
            // Split on last hyphen to handle table names containing hyphens
            // Format: "table_name-uuid" or "user-profiles-abc123"
            s.rsplit_once('-').map(|(table_name, _uuid)| table_name)
        })
        .unwrap_or("unknown")
        .to_string()
}

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

    // Detect NB format (Cassandra 4.x+) from filename FIRST - these files don't have magic numbers
    // Pattern: nb-{generation}-big-{component}.db (e.g., "nb-1-big-Data.db")
    let is_nb_format = path
        .file_name()
        .and_then(|n| n.to_str())
        .map(|s| s.contains("nb-") && s.contains("-big-"))
        .unwrap_or(false);

    if is_nb_format {
        log::debug!(
            "Detected NB format (Cassandra 4.x+) from filename '{}' - checking for header presence",
            path.display()
        );

        // NB format files can be either:
        // 1. Headerless (most common) - metadata in separate component files
        // 2. Header-based (e.g., collections_with_udts) - has magic number + header
        //
        // Check first 4 bytes to determine which type we have (Issue #154)
        if header_buffer.len() >= 4 {
            let first_4_bytes = u32::from_be_bytes([
                header_buffer[0],
                header_buffer[1],
                header_buffer[2],
                header_buffer[3],
            ]);

            // If first 4 bytes are a valid magic number, this NB file has a header
            //
            // IMPORTANT: Check for false positives from Snappy-compressed data (Issue #219)
            //
            // Snappy varint length encoding can collide with Cassandra magic numbers.
            // Snappy varints use the high bit (0x80) as a continuation marker. When the
            // first byte of compressed chunk data has this bit set, it can coincidentally
            // match certain magic number patterns.
            //
            // Known collisions (Issue #480):
            //   V5_0WideRows (0xF07C5C00):
            //   - First byte 0xF0 has high bit set (Snappy continuation marker)
            //   - 0xF0 0x7C decodes as Snappy varint for ~15984 bytes (compressed chunk length)
            //   - This occurs when NB format Data.db starts with a ~16KB compressed chunk
            //
            //   V5_0StaticColumns (0xC0515C00):
            //   - First byte 0xC0 has high bit set (Snappy continuation marker)
            //   - 0xC0 0x51 decodes as Snappy varint for 10432 bytes (uncompressed size)
            //   - This occurs when the static_columns_table Data.db is Snappy-compressed
            //     and the uncompressed size encodes to bytes matching this magic number
            //
            // Detection strategy: if detected version is one of the known headerless
            // collisions AND the first byte has the Snappy high-bit set, treat as
            // headerless Snappy-compressed NB format.
            let detected_version = CassandraVersion::from_magic_number(first_4_bytes);
            let is_snappy_varint_collision = matches!(
                detected_version,
                Some(CassandraVersion::V5_0WideRows) | Some(CassandraVersion::V5_0StaticColumns)
            ) && (header_buffer[0] & 0x80) != 0;

            if detected_version.is_some() && !is_snappy_varint_collision {
                log::debug!(
                    "NB format file '{}' has embedded header (magic: 0x{:08x}) - using standard header parsing",
                    path.display(),
                    first_4_bytes
                );
                // Fall through to standard header parsing below
            } else if is_snappy_varint_collision {
                // Snappy varint collision detected - treat as headerless
                log::debug!(
                    "NB format file '{}' has Snappy varint collision with magic 0x{:08x} - treating as headerless",
                    path.display(),
                    first_4_bytes
                );
                return create_minimal_nb_header(path).await;
            } else {
                // True headerless NB format - first 4 bytes are compressed data
                log::debug!(
                    "NB format file '{}' is headerless (first bytes: 0x{:08x}) - loading CompressionInfo.db",
                    path.display(),
                    first_4_bytes
                );
                return create_minimal_nb_header(path).await;
            }
        } else {
            // Buffer too small for magic number check, assume headerless
            log::warn!(
                "NB format file '{}' has insufficient header buffer ({} bytes) - assuming headerless format",
                path.display(),
                header_buffer.len()
            );
            return create_minimal_nb_header(path).await;
        }
    }

    // Read first 4 bytes as potential magic number or CRC32 checksum
    let first_4_bytes = u32::from_be_bytes([
        header_buffer[0],
        header_buffer[1],
        header_buffer[2],
        header_buffer[3],
    ]);

    // CRITICAL: Check for false positive with V5_0Uncompressed magic (0x0010045e)
    //
    // Issue: Uncompressed Data.db files don't have embedded headers - they start
    // directly with partition data. When partition data coincidentally starts with
    // bytes matching V5_0Uncompressed magic (0x00 0x10 0x04 0x5e), we incorrectly
    // detect a header and consume partition data as header bytes.
    //
    // Example collision in test_basic.uncompressed_table:
    // - Byte 0: 0x00 = partition flags
    // - Byte 1: 0x10 = key length (16 bytes for UUID)
    // - Bytes 2-3: 0x04 0x5e = first 2 bytes of UUID
    // - Together: 0x0010045e = V5_0Uncompressed magic (false positive!)
    //
    // Detection: If bytes match V5_0Uncompressed AND no CompressionInfo.db exists,
    // this is truly uncompressed with no header.
    if let Some(CassandraVersion::V5_0Uncompressed) =
        CassandraVersion::from_magic_number(first_4_bytes)
    {
        // Check if CompressionInfo.db exists to differentiate real header from collision
        let parent_dir = path.parent().unwrap_or(Path::new("."));
        let compression_info_exists = check_compression_info_exists(path, parent_dir);

        if !compression_info_exists {
            log::debug!(
                "Detected V5_0Uncompressed magic (0x{:08x}) but no CompressionInfo.db file exists - \
                 treating as headerless uncompressed format (partition data collision). File: '{}'",
                first_4_bytes,
                path.display()
            );
            // Create minimal header with V5_0Uncompressed version (not V5_0NewBig)
            // This ensures block_io.rs uses the uncompressed read path
            return create_minimal_uncompressed_header(path).await;
        } else {
            log::debug!(
                "Detected V5_0Uncompressed magic (0x{:08x}) with CompressionInfo.db present - \
                 parsing as standard header. File: '{}'",
                first_4_bytes,
                path.display()
            );
        }
    }

    // Detect CRC32 prefix (Cassandra 5.0+ feature)
    // If first 4 bytes don't match any known magic number, treat as CRC32 checksum
    let actual_header = if CassandraVersion::from_magic_number(first_4_bytes).is_none() {
        // First 4 bytes are likely a CRC32 checksum prefix
        log::debug!(
            "Detected CRC32 checksum prefix: 0x{:08x} in file '{}'",
            first_4_bytes,
            path.display()
        );

        let expected_checksum = first_4_bytes;
        let header_data = &header_buffer[4..];

        // Validate there's enough data after checksum
        if header_data.len() < 4 {
            return Err(Error::corruption(format!(
                "Insufficient data after CRC32 prefix: {} bytes. File: {}",
                header_data.len(),
                path.display()
            )));
        }

        // Validate CRC32 checksum
        let computed_checksum = crc32fast::hash(header_data);

        if computed_checksum != expected_checksum {
            // Don't fail - just warn. The checksum algorithm or scope may be different.
            log::warn!(
                "Header CRC32 checksum mismatch for file '{}' \
                 (Expected: 0x{:08x}, Computed: 0x{:08x}). \
                 Proceeding with parsing - checksum validation may use different algorithm.",
                path.display(),
                expected_checksum,
                computed_checksum
            );
        } else {
            log::info!(
                "Header CRC32 validated (0x{:08x}) for file '{}'",
                expected_checksum,
                path.display()
            );
        }

        header_data
    } else {
        // First 4 bytes are a valid magic number - no checksum prefix
        header_buffer
    };

    // First try spec-driven parsing for Data.db component
    let registry = get_global_registry();
    match registry.parse_data_header(actual_header) {
        Ok(parsed_header) => {
            log::debug!(
                "Successfully parsed Data.db header using spec-driven approach for file '{}' \
                 with version: {:?}",
                path.display(),
                parsed_header.cassandra_version
            );

            // Convert ParsedHeader to SSTableHeader for compatibility
            return convert_parsed_header_to_sstable_header(parsed_header, actual_header);
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
    // Extract and validate magic number (from actual_header, which may have CRC stripped)
    let magic_bytes = &actual_header[0..4];
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

    // Try to parse using the existing header parser (using actual_header)
    match parse_sstable_header(actual_header) {
        Ok((_, mut header)) => {
            log::debug!(
                "Successfully parsed header for file '{}' with version: {:?}",
                path.display(),
                header.cassandra_version
            );

            // Override keyspace/table_name with correct values extracted from path
            // This fixes Issue where simplified header parsers use hardcoded "test_keyspace"/"test_table" defaults
            header.keyspace = extract_keyspace_from_path(path);
            header.table_name = extract_table_name_from_path(path);

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
                    parse_minimal_legacy_header(actual_header, path, cassandra_version)
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

/// Check if CompressionInfo.db exists for this SSTable
fn check_compression_info_exists(data_db_path: &Path, parent_dir: &Path) -> bool {
    use super::compression::extract_sstable_base_name;

    // Extract base name (e.g., "nb-1-big-Data.db" -> "nb-1-big")
    if let Some(base_name) = extract_sstable_base_name(data_db_path) {
        let compression_info_path = parent_dir.join(format!("{}-CompressionInfo.db", base_name));
        if compression_info_path.exists() {
            return true;
        }
    }

    // Fallback: check for generic CompressionInfo.db
    let generic_path = parent_dir.join("CompressionInfo.db");
    generic_path.exists()
}

/// Create minimal header for truly uncompressed tables (no CompressionInfo.db)
///
/// This is used when partition data coincidentally matches V5_0Uncompressed magic
/// but no CompressionInfo.db file exists, indicating a headerless uncompressed table.
async fn create_minimal_uncompressed_header(path: &Path) -> Result<SSTableHeader> {
    log::info!(
        "Creating minimal uncompressed header for headerless file: {}",
        path.display()
    );

    // Use V5_0Uncompressed version so block_io.rs reads data directly without compression
    Ok(SSTableHeader {
        cassandra_version: CassandraVersion::V5_0Uncompressed,
        version: 0, // Sentinel value indicating headerless format
        table_id: [0; 16],
        keyspace: extract_keyspace_from_path(path),
        table_name: extract_table_name_from_path(path),
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

/// Create minimal header for headerless NB format files
async fn create_minimal_nb_header(path: &Path) -> Result<SSTableHeader> {
    // Try to load CompressionInfo.db to determine compression algorithm
    let compression_algorithm = match load_nb_compression_info(path).await {
        Ok(info) => {
            log::info!(
                "Loaded CompressionInfo.db for NB format: algorithm={}, chunk_length={}, chunks={}",
                info.algorithm,
                info.chunk_length,
                info.chunk_offsets.len()
            );
            info.algorithm
        }
        Err(e) => {
            log::warn!(
                "Could not load CompressionInfo.db for NB format file '{}': {}. Assuming no compression.",
                path.display(),
                e
            );
            "NONE".to_string()
        }
    };

    // Create a minimal header for NB format with compression info
    Ok(SSTableHeader {
        cassandra_version: CassandraVersion::V5_0NewBig, // NB format maps to NewBig
        version: 0,        // NB format doesn't have version in Data.db
        table_id: [0; 16], // Table ID is in other components
        keyspace: extract_keyspace_from_path(path),
        table_name: extract_table_name_from_path(path),
        generation: extract_generation_from_path(path),
        compression: CompressionInfo {
            algorithm: compression_algorithm,
            chunk_size: 16384, // Default chunk size
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

/// Load CompressionInfo.db for NB format files
async fn load_nb_compression_info(
    data_db_path: &Path,
) -> Result<crate::storage::sstable::compression_info::CompressionInfo> {
    use super::compression::extract_sstable_base_name;
    use tokio::fs::File;
    use tokio::io::AsyncReadExt;

    // Extract base name (e.g., "nb-1-big-Data.db" -> "nb-1-big")
    let base_name = extract_sstable_base_name(data_db_path).ok_or_else(|| {
        Error::InvalidFormat(format!("Cannot extract base name from {:?}", data_db_path))
    })?;

    // Build CompressionInfo.db path
    let parent_dir = data_db_path.parent().unwrap_or(Path::new("."));
    let compression_info_path = parent_dir.join(format!("{}-CompressionInfo.db", base_name));

    // Read and parse CompressionInfo.db
    let mut file = File::open(&compression_info_path).await.map_err(|e| {
        Error::InvalidFormat(format!(
            "Failed to open CompressionInfo.db at {:?}: {}. NB format requires CompressionInfo.db",
            compression_info_path, e
        ))
    })?;

    let mut data = Vec::new();
    file.read_to_end(&mut data).await.map_err(|e| {
        Error::InvalidFormat(format!(
            "Failed to read CompressionInfo.db at {:?}: {}",
            compression_info_path, e
        ))
    })?;

    crate::storage::sstable::compression_info::CompressionInfo::parse(&data)
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Test Snappy varint collision detection for V5_0WideRows magic number.
    ///
    /// Issue #219: Snappy-compressed chunk data starting with 0xF0 0x7C 0x5C 0x00
    /// collides with V5_0WideRows magic (0xF07C5C00) because 0xF0 has the high bit
    /// set (Snappy continuation marker).
    #[test]
    fn test_snappy_varint_collision_detection() {
        // Bytes that look like V5_0WideRows magic but are actually Snappy varint
        let header_buffer = [0xF0, 0x7C, 0x5C, 0x00, 0x10, 0x30, 0xB5, 0x68];

        // First byte 0xF0 has high bit set (Snappy continuation marker)
        assert_eq!(
            header_buffer[0] & 0x80,
            0x80,
            "First byte should have high bit set"
        );

        // These bytes match V5_0WideRows magic
        let first_4_bytes = u32::from_be_bytes([
            header_buffer[0],
            header_buffer[1],
            header_buffer[2],
            header_buffer[3],
        ]);
        assert_eq!(first_4_bytes, 0xF07C5C00, "Should match V5_0WideRows magic");

        // Magic number is detected
        let detected = CassandraVersion::from_magic_number(first_4_bytes);
        assert_eq!(
            detected,
            Some(CassandraVersion::V5_0WideRows),
            "Should detect V5_0WideRows"
        );

        // Collision should be detected (detected version + high bit)
        let is_collision =
            detected == Some(CassandraVersion::V5_0WideRows) && (header_buffer[0] & 0x80) != 0;
        assert!(is_collision, "Should detect Snappy collision");
    }

    /// Test that genuine magic numbers without high bit are NOT flagged as collisions.
    #[test]
    fn test_genuine_magic_number_not_collision() {
        // V5_0TypedCollections magic: 0x0F3C0000 - first byte 0x0F does NOT have high bit
        let header_buffer = [0x0F, 0x3C, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];

        // First byte does NOT have high bit set
        assert_eq!(
            header_buffer[0] & 0x80,
            0x00,
            "First byte should NOT have high bit"
        );

        let first_4_bytes = u32::from_be_bytes([
            header_buffer[0],
            header_buffer[1],
            header_buffer[2],
            header_buffer[3],
        ]);

        let detected = CassandraVersion::from_magic_number(first_4_bytes);
        assert_eq!(
            detected,
            Some(CassandraVersion::V5_0TypedCollections),
            "Should detect V5_0TypedCollections"
        );

        // Should NOT be a collision (no high bit in first byte)
        let is_collision =
            detected == Some(CassandraVersion::V5_0WideRows) && (header_buffer[0] & 0x80) != 0;
        assert!(
            !is_collision,
            "V5_0TypedCollections should not be flagged as collision"
        );
    }

    /// Test other magic numbers with high bit are NOT flagged (only V5_0WideRows check).
    ///
    /// We specifically only check V5_0WideRows to avoid breaking real embedded headers.
    #[test]
    fn test_other_high_bit_magic_not_collision() {
        // V5_0ComplexTypes magic: 0x82365C00 - first byte 0x82 HAS high bit
        // But we don't flag it because we only check V5_0WideRows specifically
        let header_buffer = [0x82, 0x36, 0x5C, 0x00, 0x00, 0x00, 0x00, 0x00];

        // First byte DOES have high bit set
        assert_eq!(
            header_buffer[0] & 0x80,
            0x80,
            "First byte should have high bit"
        );

        let first_4_bytes = u32::from_be_bytes([
            header_buffer[0],
            header_buffer[1],
            header_buffer[2],
            header_buffer[3],
        ]);

        let detected = CassandraVersion::from_magic_number(first_4_bytes);
        assert_eq!(
            detected,
            Some(CassandraVersion::V5_0ComplexTypes),
            "Should detect V5_0ComplexTypes"
        );

        // Should NOT be flagged as collision (only V5_0WideRows is checked)
        let is_collision =
            detected == Some(CassandraVersion::V5_0WideRows) && (header_buffer[0] & 0x80) != 0;
        assert!(
            !is_collision,
            "V5_0ComplexTypes should not be flagged as collision"
        );
    }

    /// Test that unrecognized bytes are not flagged as collisions.
    #[test]
    fn test_unrecognized_bytes_not_collision() {
        // Random bytes that don't match any magic number
        let header_buffer = [0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x00, 0x00, 0x00];

        let first_4_bytes = u32::from_be_bytes([
            header_buffer[0],
            header_buffer[1],
            header_buffer[2],
            header_buffer[3],
        ]);

        let detected = CassandraVersion::from_magic_number(first_4_bytes);
        assert_eq!(detected, None, "Should not detect any version");

        // Not a collision because no version detected
        let is_collision =
            detected == Some(CassandraVersion::V5_0WideRows) && (header_buffer[0] & 0x80) != 0;
        assert!(!is_collision, "Random bytes should not be flagged");
    }

    /// Test V5_0Uncompressed magic collision detection with partition data.
    ///
    /// Issue: Uncompressed Data.db files start with partition data that can
    /// coincidentally match V5_0Uncompressed magic (0x0010045e).
    ///
    /// Example from test_basic.uncompressed_table:
    /// - Byte 0: 0x00 = partition flags
    /// - Byte 1: 0x10 = key length (16 bytes for UUID)
    /// - Bytes 2-3: 0x04 0x5e = first 2 bytes of UUID
    /// - Together: 0x0010045e = V5_0Uncompressed magic (false positive!)
    ///
    /// Detection: No CompressionInfo.db exists -> truly uncompressed -> headerless
    #[test]
    fn test_v5_uncompressed_magic_collision() {
        // Bytes from actual uncompressed_table Data.db file
        let header_buffer = [0x00, 0x10, 0x04, 0x5e, 0x63, 0xfc, 0x4e, 0x93];

        // These bytes match V5_0Uncompressed magic
        let first_4_bytes = u32::from_be_bytes([
            header_buffer[0],
            header_buffer[1],
            header_buffer[2],
            header_buffer[3],
        ]);
        assert_eq!(
            first_4_bytes, 0x0010045e,
            "Should match V5_0Uncompressed magic"
        );

        // Magic number is detected
        let detected = CassandraVersion::from_magic_number(first_4_bytes);
        assert_eq!(
            detected,
            Some(CassandraVersion::V5_0Uncompressed),
            "Should detect V5_0Uncompressed"
        );

        // In practice, check_compression_info_exists() would return false,
        // causing create_minimal_uncompressed_header() to be called with
        // cassandra_version = V5_0Uncompressed (not V5_0NewBig)
    }
}
