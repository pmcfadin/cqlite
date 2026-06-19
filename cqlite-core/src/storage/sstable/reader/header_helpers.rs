//! SSTable header helper functions
//!
//! This module contains utility functions for header size calculation,
//! generation extraction, and legacy format handling.

use crate::{
    parser::header::{parse_sstable_header, CassandraVersion, SSTableHeader},
    Result,
};
use log::{debug, warn};
use std::path::Path;

/// Extract generation number from SSTable file path
pub(crate) fn extract_generation_from_path(path: &Path) -> u64 {
    let filename = path.file_name().and_then(|n| n.to_str()).unwrap_or("");

    // Common Cassandra SSTable filename patterns:
    // nb-1-big-Data.db -> generation 1
    // mc-1-big-Data.db -> generation 1
    // la-123-big-Data.db -> generation 123
    // keyspace-table-nb-456-big-Data.db -> generation 456

    // Try to find generation number in different patterns
    let parts: Vec<&str> = filename.split('-').collect();

    // Pattern 1: nb-{generation}-big-Data.db
    if parts.len() >= 3 && (parts[0] == "nb" || parts[0] == "mc" || parts[0] == "la") {
        if let Ok(generation) = parts[1].parse::<u64>() {
            debug!(
                "Extracted generation {} from pattern 1: {}",
                generation, filename
            );
            return generation;
        }
    }

    // Pattern 2: keyspace-table-nb-{generation}-big-Data.db
    if parts.len() >= 5 {
        for i in 0..parts.len() - 2 {
            if (parts[i] == "nb" || parts[i] == "mc" || parts[i] == "la") && i + 1 < parts.len() {
                if let Ok(generation) = parts[i + 1].parse::<u64>() {
                    log::debug!(
                        "Extracted generation {} from pattern 2: {}",
                        generation,
                        filename
                    );
                    return generation;
                }
            }
        }
    }

    // Pattern 3: Look for any numeric part that could be generation
    for part in &parts {
        if let Ok(generation) = part.parse::<u64>() {
            // Skip obviously wrong numbers (like version numbers)
            if generation > 0 && generation < 1_000_000 {
                debug!(
                    "Extracted generation {} from numeric part: {}",
                    generation, filename
                );
                return generation;
            }
        }
    }

    // Default generation if parsing fails
    debug!("Using default generation 0 for: {}", filename);
    0
}

/// Calculate actual header size based on header content and buffer
pub(crate) fn calculate_actual_header_size(
    header: &SSTableHeader,
    header_buffer: &[u8],
) -> Result<usize> {
    // Check for headerless NB format files FIRST (Issue #211)
    // NB format Data.db files are headerless - metadata is in separate component files.
    // When create_minimal_nb_header() is used, it sets version=0 as a sentinel value.
    // In this case, the header_buffer contains compressed row data, not a header.
    if header.cassandra_version == CassandraVersion::V5_0NewBig && header.version == 0 {
        debug!("Headerless NB format detected (version=0) - Data.db starts at offset 0");
        return Ok(0);
    }

    // Check for truly uncompressed format (no header)
    // When partition data coincidentally matches V5_0Uncompressed magic but no
    // CompressionInfo.db exists, we create a minimal header with version=0.
    // The Data.db file starts at offset 0 with raw partition data.
    if header.cassandra_version == CassandraVersion::V5_0Uncompressed && header.version == 0 {
        debug!("Headerless uncompressed format detected (version=0) - Data.db starts at offset 0");
        return Ok(0);
    }

    // Issue #831: BTI ("da") Data.db is headerless — create_minimal_bti_header()
    // sets version=0 as the headerless sentinel. The Data.db buffer is compressed
    // chunk data starting at offset 0, not an embedded header.
    if header.cassandra_version == CassandraVersion::V5_0Bti && header.version == 0 {
        debug!("Headerless BTI (da) format detected (version=0) - Data.db starts at offset 0");
        return Ok(0);
    }

    // Use proper structured parsing to find the end of the header
    match header.cassandra_version {
        CassandraVersion::V5_0NewBig => {
            // Modern BIG v5 format - use nom parser to find exact header end
            parse_exact_header_size_nb(header, header_buffer)
        }
        CassandraVersion::V5_0Bti => {
            // Modern BTI format - use nom parser to find exact header end
            parse_exact_header_size_bti(header, header_buffer)
        }
        CassandraVersion::Legacy => {
            #[cfg(feature = "legacy-heuristics")]
            {
                // Legacy format with heuristics enabled
                find_data_start_legacy_format(header_buffer)
            }
            #[cfg(not(feature = "legacy-heuristics"))]
            {
                // Legacy format without heuristics - use conservative fixed size
                Ok(512.min(header_buffer.len()))
            }
        }
        _ => {
            // For other Cassandra versions, try to parse with known format
            parse_exact_header_size_nb(header, header_buffer)
        }
    }
}

/// Parse exact header size for BIG v5 format using nom parser
pub(crate) fn parse_exact_header_size_nb(
    _header: &SSTableHeader,
    header_buffer: &[u8],
) -> Result<usize> {
    // Use the actual nom parser to determine where the header ends
    match parse_sstable_header(header_buffer) {
        Ok((remaining, _parsed_header)) => {
            // The difference between original buffer and remaining is the exact header size
            let header_size = header_buffer.len() - remaining.len();
            debug!(
                "Parsed exact header size {} for BIG v5 format using nom parser",
                header_size
            );

            // Verify we have a reasonable header size
            if header_size < 32 {
                return Err(crate::error::Error::InvalidFormat(
                    "Header size too small - possible corruption".to_string(),
                ));
            }
            if header_size > header_buffer.len() {
                return Err(crate::error::Error::InvalidFormat(
                    "Header size exceeds buffer - possible corruption".to_string(),
                ));
            }

            Ok(header_size)
        }
        Err(err) => {
            warn!("Failed to parse header with nom: {:?}", err);
            // Fallback to scanning for data start markers
            #[cfg(feature = "legacy-heuristics")]
            {
                find_data_start_legacy_format(header_buffer)
            }
            #[cfg(not(feature = "legacy-heuristics"))]
            {
                Ok(512.min(header_buffer.len()))
            }
        }
    }
}

/// Parse exact header size for BTI format using nom parser
pub(crate) fn parse_exact_header_size_bti(
    _header: &SSTableHeader,
    header_buffer: &[u8],
) -> Result<usize> {
    // Use the actual nom parser to determine where the header ends
    match parse_sstable_header(header_buffer) {
        Ok((remaining, _parsed_header)) => {
            // The difference between original buffer and remaining is the exact header size
            let header_size = header_buffer.len() - remaining.len();
            debug!(
                "Parsed exact header size {} for BTI format using nom parser",
                header_size
            );

            // Verify we have a reasonable header size
            if header_size < 32 {
                return Err(crate::error::Error::InvalidFormat(
                    "Header size too small - possible corruption".to_string(),
                ));
            }
            if header_size > header_buffer.len() {
                return Err(crate::error::Error::InvalidFormat(
                    "Header size exceeds buffer - possible corruption".to_string(),
                ));
            }

            Ok(header_size)
        }
        Err(err) => {
            warn!("Failed to parse header with nom: {:?}", err);
            // Fallback to scanning for data start markers
            #[cfg(feature = "legacy-heuristics")]
            {
                find_data_start_legacy_format(header_buffer)
            }
            #[cfg(not(feature = "legacy-heuristics"))]
            {
                Ok(512.min(header_buffer.len()))
            }
        }
    }
}

/// Find data start for legacy format files (legacy heuristics)
#[cfg(feature = "legacy-heuristics")]
pub(crate) fn find_data_start_legacy_format(header_buffer: &[u8]) -> Result<usize> {
    // Legacy format is more predictable - usually 512 bytes or less
    let fallback_size = 512.min(header_buffer.len());
    debug!(
        "Using standard header size {} for legacy format",
        fallback_size
    );
    Ok(fallback_size)
}

/// Estimate header size using heuristics when version is unknown (legacy only)
#[cfg(feature = "legacy-heuristics")]
#[allow(dead_code)]
pub(crate) fn estimate_header_size_heuristic(header_buffer: &[u8]) -> Result<usize> {
    // DEPRECATED: This function uses heuristics and should only be used for legacy support
    // Modern formats (BIG v5, BTI) should use structured parsing instead

    // Use heuristics to estimate where header ends and data begins
    // Look for patterns that indicate start of data section

    for i in (64..header_buffer.len().min(1024)).step_by(64) {
        if i + 16 < header_buffer.len() {
            // Check if this position has characteristics of data vs. header
            let slice = &header_buffer[i..i + 16];

            // Data sections often have more entropy than headers
            let non_zero_bytes = slice.iter().filter(|&&b| b != 0).count();
            let entropy_score = non_zero_bytes as f32 / 16.0;

            // If we find a region with high entropy, it might be start of data
            if entropy_score > 0.7 {
                debug!(
                    "[LEGACY HEURISTIC] Detected potential data start at offset {} (entropy: {:.2})",
                    i, entropy_score
                );
                return Ok(i);
            }
        }
    }

    // Conservative fallback
    let fallback_size = 768.min(header_buffer.len());
    debug!(
        "[LEGACY HEURISTIC] Using heuristic header size {}",
        fallback_size
    );
    Ok(fallback_size)
}
