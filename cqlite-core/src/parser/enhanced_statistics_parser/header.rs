//! Statistics.db file header (32-byte fixed prefix) and Table-of-Contents parsing.
//!
//! The 32-byte header carries the version/kind markers and data length. The TOC
//! that follows maps each Cassandra `MetadataType` component to its byte offset;
//! we use it to locate the `HEADER` (SerializationHeader) component.

use super::super::statistics::StatisticsHeader;
use nom::{number::complete::be_u32, IResult};

/// Cassandra MetadataType enum ordinals (from MetadataType.java)
/// Used to identify component types in Statistics.db TOC
#[allow(dead_code)]
const METADATA_TYPE_VALIDATION: u32 = 0;
#[allow(dead_code)]
const METADATA_TYPE_COMPACTION: u32 = 1;
#[allow(dead_code)]
const METADATA_TYPE_STATS: u32 = 2;
const METADATA_TYPE_HEADER: u32 = 3; // SerializationHeader

/// Enhanced Statistics.db header parser for real 'nb' format
///
/// This function parses the actual 32-byte binary header structure from
/// Cassandra 5.0 Statistics.db files. Based on hex analysis of real files:
///
/// ```text
/// 00000000  00 00 00 04 26 29 1b 05  00 00 00 00 00 00 00 2c
/// 00000010  00 00 00 01 00 00 00 65  00 00 00 02 00 00 14 d4
/// ```
///
/// # Binary Format (32 bytes)
///
/// - Bytes 0-3:   `version_type` (u32 BE) - Format version identifier (e.g., 0x00000004)
/// - Bytes 4-7:   `statistics_kind` (u32 BE) - Statistics type marker (e.g., 0x26291b05)
/// - Bytes 8-11:  `reserved1` (u32 BE) - Reserved field (typically 0x00000000)
/// - Bytes 12-15: `data_length` (u32 BE) - Length of variable-length data section
/// - Bytes 16-19: `metadata1` (u32 BE) - Metadata field (purpose TBD in M2)
/// - Bytes 20-23: `metadata2` (u32 BE) - Metadata field (purpose TBD in M2)
/// - Bytes 24-27: `metadata3` (u32 BE) - Metadata field (purpose TBD in M2)
/// - Bytes 28-31: `checksum_or_more` (u32 BE) - Checksum or additional metadata
///
/// # Returns
///
/// `Ok((remaining_input, StatisticsHeader))` on successful parse of 32-byte header.
///
/// # Note
///
/// This is the ONLY function in this module that reads actual binary data.
/// All other parsing functions have been removed per Issue #28 mandate.
pub fn parse_nb_format_header(input: &[u8]) -> IResult<&[u8], StatisticsHeader> {
    let (input, version_type) = be_u32(input)?;
    let (input, statistics_kind) = be_u32(input)?;
    let (input, _reserved1) = be_u32(input)?;
    let (input, data_length) = be_u32(input)?;
    let (input, metadata1) = be_u32(input)?;
    let (input, metadata2) = be_u32(input)?;
    let (input, metadata3) = be_u32(input)?;
    let (input, checksum_or_more) = be_u32(input)?;

    Ok((
        input,
        StatisticsHeader {
            version: version_type,
            statistics_kind,
            data_length,
            metadata1,
            metadata2,
            metadata3,
            checksum: checksum_or_more,
            table_id: None,
        },
    ))
}

/// Parse Statistics.db Table of Contents to get component offsets (Issue #216)
///
/// Statistics.db format (from Cassandra MetadataSerializer.java):
/// - [4 bytes] number_of_components (u32 BE)
/// - [4 bytes] checksum (u32 BE)
/// - [TOC] component_type (u32) | offset (u32) for each component
/// - [Component data...]
///
/// MetadataType enum ordinals:
/// - 0 = VALIDATION
/// - 1 = COMPACTION
/// - 2 = STATS
/// - 3 = HEADER (SerializationHeader)
///
/// Returns the offset to the HEADER component (SerializationHeader), or None if not found.
pub(super) fn parse_statistics_toc_for_header_offset(input: &[u8]) -> Option<usize> {
    if input.len() < 8 {
        log::debug!("Statistics.db too small for TOC: {} bytes", input.len());
        return None;
    }

    // Parse number of components
    let num_components = u32::from_be_bytes([input[0], input[1], input[2], input[3]]);
    log::debug!("Statistics.db TOC: {} components", num_components);

    // Sanity check: Cassandra has exactly 4 MetadataType enum values
    // (VALIDATION=0, COMPACTION=1, STATS=2, HEADER=3)
    // A value > 100 indicates corrupted or malicious data
    if num_components > 100 {
        log::warn!(
            "Suspicious num_components={} in Statistics.db TOC (expected <=4)",
            num_components
        );
        return None;
    }

    // Skip checksum (bytes 4-7)
    // TOC starts at byte 8

    let toc_start: usize = 8;
    let toc_entry_size: usize = 8; // 4 bytes type + 4 bytes offset

    // Use checked_mul to prevent integer overflow on multiplication
    let toc_size = (num_components as usize)
        .checked_mul(toc_entry_size)
        .and_then(|size| size.checked_add(toc_start))?;

    if input.len() < toc_size {
        log::debug!(
            "Statistics.db too small for {} TOC entries: {} bytes (need {})",
            num_components,
            input.len(),
            toc_size
        );
        return None;
    }

    // Search for HEADER component (type 3)
    for i in 0..num_components as usize {
        // Use checked arithmetic to prevent overflow in entry offset calculation
        let entry_offset = i
            .checked_mul(toc_entry_size)
            .and_then(|offset| offset.checked_add(toc_start))?;
        let component_type = u32::from_be_bytes([
            input[entry_offset],
            input[entry_offset + 1],
            input[entry_offset + 2],
            input[entry_offset + 3],
        ]);
        let component_offset = u32::from_be_bytes([
            input[entry_offset + 4],
            input[entry_offset + 5],
            input[entry_offset + 6],
            input[entry_offset + 7],
        ]) as usize;

        log::debug!(
            "TOC entry {}: type={} offset=0x{:x}",
            i,
            component_type,
            component_offset
        );

        if component_type == METADATA_TYPE_HEADER {
            log::debug!(
                "Found HEADER component at offset 0x{:x} ({})",
                component_offset,
                component_offset
            );
            return Some(component_offset);
        }
    }

    log::debug!("HEADER component not found in Statistics.db TOC");
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nb_format_header_parsing() {
        // Test data based on real file hex dump
        let test_data = vec![
            0x00, 0x00, 0x00, 0x04, // version_type = 4
            0x26, 0x29, 0x1b, 0x05, // statistics_kind
            0x00, 0x00, 0x00, 0x00, // reserved
            0x00, 0x00, 0x00, 0x2c, // data_length = 44
            0x00, 0x00, 0x00, 0x01, // metadata1 = 1
            0x00, 0x00, 0x00, 0x65, // metadata2 = 101
            0x00, 0x00, 0x00, 0x02, // metadata3 = 2
            0x00, 0x00, 0x14, 0xd4, // checksum/more = 5332
        ];

        let result = parse_nb_format_header(&test_data);
        assert!(result.is_ok());

        let (_, header) = result.unwrap();
        assert_eq!(header.version, 4);
        assert_eq!(header.statistics_kind, 0x2629_1b05);
        assert_eq!(header.data_length, 44);
        assert_eq!(header.metadata1, 1);
        assert_eq!(header.metadata2, 101);
        assert_eq!(header.metadata3, 2);
        assert_eq!(header.checksum, 0x14d4);
    }
}
