/// Additional functions for fixing header size calculation in SSTable reader

use crate::error::Result;
use crate::parser::header::SSTableHeader;

/// Parse exact header size for standard Cassandra versions using nom parser
pub fn parse_exact_header_size_standard(
    _header: &SSTableHeader,
    header_buffer: &[u8],
) -> Result<usize> {
    use crate::parser::header::parse_sstable_header;

    // Use the actual nom parser to determine where the header ends
    match parse_sstable_header(header_buffer) {
        Ok((remaining, _parsed_header)) => {
            // The difference between original buffer and remaining is the exact header size
            let header_size = header_buffer.len() - remaining.len();
            log::debug!(
                "Parsed exact header size {} for standard format using nom parser",
                header_size
            );

            // Verify we have a reasonable header size
            if header_size < 32 {
                return Err(crate::error::Error::InvalidFormat(
                    "Header size too small - possible corruption".to_string()
                ));
            }
            if header_size > header_buffer.len() {
                return Err(crate::error::Error::InvalidFormat(
                    "Header size exceeds buffer - possible corruption".to_string()
                ));
            }

            Ok(header_size)
        }
        Err(err) => {
            log::warn!("Failed to parse header with nom: {:?}", err);
            // Fallback to scanning for data start markers
            find_data_start_by_heuristic_scanning(header_buffer)
        }
    }
}

/// Find data start by scanning for block start markers (heuristic fallback method)
pub fn find_data_start_by_heuristic_scanning(header_buffer: &[u8]) -> Result<usize> {
    // Look for common data block start patterns in Cassandra SSTables
    const BLOCK_MARKERS: &[&[u8]] = &[
        // Common compression block headers
        b"\x00\x00\x00", // Uncompressed block marker
        b"\x78\x9c",      // Zlib header
        b"\x1f\x8b",      // Gzip header
        b"LZ4",          // LZ4 block header
        // Cassandra-specific markers
        b"\x5a\x5a\x5a\x5a", // Another magic pattern
    ];

    // Start scanning after minimum header size
    let start_scan = 64.min(header_buffer.len());

    for offset in start_scan..header_buffer.len().saturating_sub(4) {
        let window = &header_buffer[offset..offset + 4];

        // Check for known block markers
        for marker in BLOCK_MARKERS {
            if window.starts_with(marker) {
                log::debug!(
                    "Found potential data start at offset {} (marker: {:02x?})",
                    offset, marker
                );
                return Ok(offset);
            }
        }

        // Look for null byte patterns that might indicate end of header
        if offset > 128 && window == [0, 0, 0, 0] {
            // Check if this is followed by non-null data (potential block start)
            if offset + 8 < header_buffer.len() {
                let next_bytes = &header_buffer[offset + 4..offset + 8];
                if next_bytes.iter().any(|&b| b != 0) {
                    log::debug!(
                        "Found potential data start after null padding at offset {}",
                        offset + 4
                    );
                    return Ok(offset + 4);
                }
            }
        }
    }

    // If no markers found, use a conservative fallback
    let fallback_size = if header_buffer.len() > 2048 {
        1024 // Use 1KB for larger files
    } else {
        512  // Use 512B for smaller files
    };

    log::warn!(
        "No data start markers found, using fallback size {}",
        fallback_size
    );
    Ok(fallback_size.min(header_buffer.len()))
}