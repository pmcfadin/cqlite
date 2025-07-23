//! SSTable header parsing for Cassandra 5+ 'oa' format
//!
//! This module handles parsing of SSTable headers which contain metadata
//! about the table structure, compression, and other essential information.

use super::vint::{parse_vint, parse_vint_length};
use crate::error::Result;
use nom::{
    bytes::complete::take,
    multi::count,
    number::complete::{be_u16, be_u32, be_u64, be_u8},
    IResult,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Cassandra version enum mapping magic numbers to versions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CassandraVersion {
    /// Legacy 'oa' format (backward compatibility)
    Legacy,
    /// Cassandra 5.0 Alpha
    V5_0_Alpha,
    /// Cassandra 5.0 Beta
    V5_0_Beta,
    /// Cassandra 5.0 Release
    V5_0_Release,
    /// Cassandra 5.0 'nb' (new big) format
    V5_0_NewBig,
    /// Cassandra 5.0 BTI (Big Trie-Indexed) format
    V5_0_Bti,
}

impl CassandraVersion {
    /// Get the magic number for this version
    pub fn magic_number(&self) -> u32 {
        match self {
            CassandraVersion::Legacy => 0x6F61_0000,      // 'oa' format
            CassandraVersion::V5_0_Alpha => 0xAD01_0000,   // Cassandra 5.0 Alpha
            CassandraVersion::V5_0_Beta => 0xA007_0000,    // Cassandra 5.0 Beta
            CassandraVersion::V5_0_Release => 0x4316_0000, // Cassandra 5.0 Release
            CassandraVersion::V5_0_NewBig => 0x0040_0000,  // Cassandra 5.0 'nb' (new big) format
            CassandraVersion::V5_0_Bti => 0x6461_0000,     // Cassandra 5.0 BTI (Big Trie-Indexed) format
        }
    }
    
    /// Parse magic number to version
    pub fn from_magic_number(magic: u32) -> Option<CassandraVersion> {
        match magic {
            0x6F61_0000 => Some(CassandraVersion::Legacy),
            0xAD01_0000 => Some(CassandraVersion::V5_0_Alpha),
            0xA007_0000 => Some(CassandraVersion::V5_0_Beta),
            0x4316_0000 => Some(CassandraVersion::V5_0_Release),
            0x0040_0000 => Some(CassandraVersion::V5_0_NewBig),
            0x6461_0000 => Some(CassandraVersion::V5_0_Bti),
            _ => None,
        }
    }
    
    /// Get human-readable version string
    pub fn version_string(&self) -> &'static str {
        match self {
            CassandraVersion::Legacy => "Legacy 'oa' format",
            CassandraVersion::V5_0_Alpha => "Cassandra 5.0 Alpha",
            CassandraVersion::V5_0_Beta => "Cassandra 5.0 Beta",
            CassandraVersion::V5_0_Release => "Cassandra 5.0 Release",
            CassandraVersion::V5_0_NewBig => "Cassandra 5.0 'nb' (new big) format",
            CassandraVersion::V5_0_Bti => "Cassandra 5.0 BTI (Big Trie-Indexed) format",
        }
    }
}

/// Legacy magic number for backward compatibility
pub const SSTABLE_MAGIC: u32 = 0x6F61_0000; // 'oa' followed by version bytes

/// All supported magic numbers
pub const SUPPORTED_MAGIC_NUMBERS: &[u32] = &[
    0x6F61_0000, // Legacy 'oa' format
    0xAD01_0000, // Cassandra 5.0 Alpha
    0xA007_0000, // Cassandra 5.0 Beta
    0x4316_0000, // Cassandra 5.0 Release
    0x0040_0000, // Cassandra 5.0 'nb' (new big) format
    0x6461_0000, // Cassandra 5.0 BTI (Big Trie-Indexed) format
];

/// Current supported format version
pub const SUPPORTED_VERSION: u16 = 0x0001;

/// SSTable header containing metadata about the table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSTableHeader {
    /// Cassandra version detected from magic number
    pub cassandra_version: CassandraVersion,
    /// Format version
    pub version: u16,
    /// Table UUID
    pub table_id: [u8; 16],
    /// Keyspace name
    pub keyspace: String,
    /// Table name
    pub table_name: String,
    /// Generation number
    pub generation: u64,
    /// Compression information
    pub compression: CompressionInfo,
    /// Statistics about the SSTable
    pub stats: SSTableStats,
    /// Column metadata
    pub columns: Vec<ColumnInfo>,
    /// Custom properties
    pub properties: HashMap<String, String>,
}

/// Compression configuration for the SSTable
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionInfo {
    /// Compression algorithm name (e.g., "LZ4", "SNAPPY", "NONE")
    pub algorithm: String,
    /// Compression chunk size in bytes
    pub chunk_size: u32,
    /// Additional compression parameters
    pub parameters: HashMap<String, String>,
}

/// Statistics about the SSTable content
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSTableStats {
    /// Total number of rows
    pub row_count: u64,
    /// Minimum timestamp
    pub min_timestamp: i64,
    /// Maximum timestamp
    pub max_timestamp: i64,
    /// Maximum deletion time
    pub max_deletion_time: i64,
    /// Compression ratio (0.0 to 1.0)
    pub compression_ratio: f64,
    /// Estimated row size distribution
    pub row_size_histogram: Vec<u64>,
}

/// Information about a column in the table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    /// Column name
    pub name: String,
    /// Column type (CQL type name)
    pub column_type: String,
    /// Whether the column is part of the primary key
    pub is_primary_key: bool,
    /// Column position in the primary key (if applicable)
    pub key_position: Option<u16>,
    /// Whether the column is static
    pub is_static: bool,
    /// Whether the column is clustering
    pub is_clustering: bool,
}

/// Parse the SSTable magic number and version, supporting multiple Cassandra versions
pub fn parse_magic_and_version(input: &[u8]) -> IResult<&[u8], (CassandraVersion, u16)> {
    let (input, magic) = be_u32(input)?;
    
    // Detect Cassandra version from magic number
    let cassandra_version = CassandraVersion::from_magic_number(magic)
        .ok_or_else(|| {
            nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            ))
        })?;

    // Handle different format versions - 'nb' format has different header structure
    let (input, version) = match cassandra_version {
        CassandraVersion::V5_0_NewBig => {
            // For 'nb' format, skip the next bytes and look for version later
            // Based on analysis of real data, version appears at offset 29 (25 bytes after magic number)
            let (input, _skip_bytes) = take(25usize)(input)?; // Skip 25 bytes after magic number
            let (input, version) = be_u16(input)?;
            (input, version)
        }
        _ => {
            // Standard format: version immediately follows magic number
            let (input, version) = be_u16(input)?;
            (input, version)
        }
    };
    
    // For now, we support version 0x0001 across all Cassandra versions
    // This can be extended in the future for version-specific handling
    if version != SUPPORTED_VERSION {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }

    Ok((input, (cassandra_version, version)))
}

/// Legacy function for backward compatibility
pub fn parse_magic_and_version_legacy(input: &[u8]) -> IResult<&[u8], u16> {
    let (input, (_, version)) = parse_magic_and_version(input)?;
    Ok((input, version))
}

/// Parse a length-prefixed string using VInt encoding
pub fn parse_vstring(input: &[u8]) -> IResult<&[u8], String> {
    let (input, length) = parse_vint_length(input)?;
    let (input, bytes) = take(length)(input)?;
    let string = String::from_utf8(bytes.to_vec()).map_err(|_| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
    })?;
    Ok((input, string))
}

/// Parse compression information
pub fn parse_compression_info(input: &[u8]) -> IResult<&[u8], CompressionInfo> {
    let (input, algorithm) = parse_vstring(input)?;
    let (input, chunk_size) = be_u32(input)?;
    let (input, param_count) = parse_vint_length(input)?;

    let mut parameters = HashMap::new();
    let mut remaining = input;

    for _ in 0..param_count {
        let (new_remaining, key) = parse_vstring(remaining)?;
        let (new_remaining, value) = parse_vstring(new_remaining)?;
        parameters.insert(key, value);
        remaining = new_remaining;
    }

    Ok((
        remaining,
        CompressionInfo {
            algorithm,
            chunk_size,
            parameters,
        },
    ))
}

/// Parse SSTable statistics
pub fn parse_sstable_stats(input: &[u8]) -> IResult<&[u8], SSTableStats> {
    let (input, row_count) = be_u64(input)?;
    let (input, min_timestamp) = parse_vint(input)?;
    let (input, max_timestamp) = parse_vint(input)?;
    let (input, max_deletion_time) = parse_vint(input)?;
    let (input, compression_ratio_bits) = be_u64(input)?;
    let compression_ratio = f64::from_bits(compression_ratio_bits);

    let (input, histogram_size) = parse_vint_length(input)?;
    let (input, row_size_histogram) = count(be_u64, histogram_size)(input)?;

    Ok((
        input,
        SSTableStats {
            row_count,
            min_timestamp,
            max_timestamp,
            max_deletion_time,
            compression_ratio,
            row_size_histogram,
        },
    ))
}

/// Parse column information
pub fn parse_column_info(input: &[u8]) -> IResult<&[u8], ColumnInfo> {
    let (input, name) = parse_vstring(input)?;
    let (input, column_type) = parse_vstring(input)?;
    let (input, flags) = be_u8(input)?;

    let is_primary_key = (flags & 0x01) != 0;
    let is_static = (flags & 0x02) != 0;
    let is_clustering = (flags & 0x04) != 0;

    let (input, key_position) = if is_primary_key {
        let (input, pos) = be_u16(input)?;
        (input, Some(pos))
    } else {
        (input, None)
    };

    Ok((
        input,
        ColumnInfo {
            name,
            column_type,
            is_primary_key,
            key_position,
            is_static,
            is_clustering,
        },
    ))
}

/// Parse the complete SSTable header
pub fn parse_sstable_header(input: &[u8]) -> IResult<&[u8], SSTableHeader> {
    let (input, (cassandra_version, version)) = parse_magic_and_version(input)?;
    let (input, table_id) = take(16usize)(input)?;
    let table_id = {
        let mut id = [0u8; 16];
        id.copy_from_slice(table_id);
        id
    };

    let (input, keyspace) = parse_vstring(input)?;
    let (input, table_name) = parse_vstring(input)?;
    let (input, generation) = be_u64(input)?;
    let (input, compression) = parse_compression_info(input)?;
    let (input, stats) = parse_sstable_stats(input)?;

    let (input, column_count) = parse_vint_length(input)?;
    let (input, columns) = count(parse_column_info, column_count)(input)?;

    let (input, prop_count) = parse_vint_length(input)?;
    let mut properties = HashMap::new();
    let mut remaining = input;

    for _ in 0..prop_count {
        let (new_remaining, key) = parse_vstring(remaining)?;
        let (new_remaining, value) = parse_vstring(new_remaining)?;
        properties.insert(key, value);
        remaining = new_remaining;
    }

    Ok((
        remaining,
        SSTableHeader {
            cassandra_version,
            version,
            table_id,
            keyspace,
            table_name,
            generation,
            compression,
            stats,
            columns,
            properties,
        },
    ))
}

/// Serialize an SSTable header to bytes
pub fn serialize_sstable_header(header: &SSTableHeader) -> Result<Vec<u8>> {
    let mut result = Vec::new();

    // Magic and version - use the magic number for the detected Cassandra version
    result.extend_from_slice(&header.cassandra_version.magic_number().to_be_bytes());
    result.extend_from_slice(&header.version.to_be_bytes());

    // Table ID
    result.extend_from_slice(&header.table_id);

    // Keyspace and table name
    serialize_vstring(&mut result, &header.keyspace)?;
    serialize_vstring(&mut result, &header.table_name)?;

    // Generation
    result.extend_from_slice(&header.generation.to_be_bytes());

    // Compression info
    serialize_compression_info(&mut result, &header.compression)?;

    // Stats
    serialize_sstable_stats(&mut result, &header.stats)?;

    // Columns
    serialize_vint_length(&mut result, header.columns.len())?;
    for column in &header.columns {
        serialize_column_info(&mut result, column)?;
    }

    // Properties
    serialize_vint_length(&mut result, header.properties.len())?;
    for (key, value) in &header.properties {
        serialize_vstring(&mut result, key)?;
        serialize_vstring(&mut result, value)?;
    }

    Ok(result)
}

fn serialize_vstring(output: &mut Vec<u8>, s: &str) -> Result<()> {
    use super::vint::encode_vint;
    output.extend_from_slice(&encode_vint(s.len() as i64));
    output.extend_from_slice(s.as_bytes());
    Ok(())
}

fn serialize_vint_length(output: &mut Vec<u8>, len: usize) -> Result<()> {
    use super::vint::encode_vint;
    output.extend_from_slice(&encode_vint(len as i64));
    Ok(())
}

fn serialize_compression_info(output: &mut Vec<u8>, info: &CompressionInfo) -> Result<()> {
    serialize_vstring(output, &info.algorithm)?;
    output.extend_from_slice(&info.chunk_size.to_be_bytes());
    serialize_vint_length(output, info.parameters.len())?;

    for (key, value) in &info.parameters {
        serialize_vstring(output, key)?;
        serialize_vstring(output, value)?;
    }

    Ok(())
}

fn serialize_sstable_stats(output: &mut Vec<u8>, stats: &SSTableStats) -> Result<()> {
    use super::vint::encode_vint;

    output.extend_from_slice(&stats.row_count.to_be_bytes());
    output.extend_from_slice(&encode_vint(stats.min_timestamp));
    output.extend_from_slice(&encode_vint(stats.max_timestamp));
    output.extend_from_slice(&encode_vint(stats.max_deletion_time));
    output.extend_from_slice(&stats.compression_ratio.to_bits().to_be_bytes());

    serialize_vint_length(output, stats.row_size_histogram.len())?;
    for &size in &stats.row_size_histogram {
        output.extend_from_slice(&size.to_be_bytes());
    }

    Ok(())
}

fn serialize_column_info(output: &mut Vec<u8>, column: &ColumnInfo) -> Result<()> {
    serialize_vstring(output, &column.name)?;
    serialize_vstring(output, &column.column_type)?;

    let mut flags = 0u8;
    if column.is_primary_key {
        flags |= 0x01;
    }
    if column.is_static {
        flags |= 0x02;
    }
    if column.is_clustering {
        flags |= 0x04;
    }
    output.push(flags);

    if let Some(position) = column.key_position {
        output.extend_from_slice(&position.to_be_bytes());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_magic_and_version_legacy() {
        let mut data = Vec::new();
        data.extend_from_slice(&SSTABLE_MAGIC.to_be_bytes());
        data.extend_from_slice(&SUPPORTED_VERSION.to_be_bytes());

        let (_, (cassandra_version, version)) = parse_magic_and_version(&data).unwrap();
        assert_eq!(cassandra_version, CassandraVersion::Legacy);
        assert_eq!(version, SUPPORTED_VERSION);
    }

    #[test]
    fn test_magic_and_version_cassandra_5_alpha() {
        let mut data = Vec::new();
        data.extend_from_slice(&CassandraVersion::V5_0_Alpha.magic_number().to_be_bytes());
        data.extend_from_slice(&SUPPORTED_VERSION.to_be_bytes());

        let (_, (cassandra_version, version)) = parse_magic_and_version(&data).unwrap();
        assert_eq!(cassandra_version, CassandraVersion::V5_0_Alpha);
        assert_eq!(version, SUPPORTED_VERSION);
    }

    #[test]
    fn test_magic_and_version_cassandra_5_beta() {
        let mut data = Vec::new();
        data.extend_from_slice(&CassandraVersion::V5_0_Beta.magic_number().to_be_bytes());
        data.extend_from_slice(&SUPPORTED_VERSION.to_be_bytes());

        let (_, (cassandra_version, version)) = parse_magic_and_version(&data).unwrap();
        assert_eq!(cassandra_version, CassandraVersion::V5_0_Beta);
        assert_eq!(version, SUPPORTED_VERSION);
    }

    #[test]
    fn test_magic_and_version_cassandra_5_release() {
        let mut data = Vec::new();
        data.extend_from_slice(&CassandraVersion::V5_0_Release.magic_number().to_be_bytes());
        data.extend_from_slice(&SUPPORTED_VERSION.to_be_bytes());

        let (_, (cassandra_version, version)) = parse_magic_and_version(&data).unwrap();
        assert_eq!(cassandra_version, CassandraVersion::V5_0_Release);
        assert_eq!(version, SUPPORTED_VERSION);
    }

    #[test]
    fn test_magic_and_version_cassandra_5_newbig() {
        let mut data = Vec::new();
        data.extend_from_slice(&CassandraVersion::V5_0_NewBig.magic_number().to_be_bytes());
        data.extend_from_slice(&SUPPORTED_VERSION.to_be_bytes());

        let (_, (cassandra_version, version)) = parse_magic_and_version(&data).unwrap();
        assert_eq!(cassandra_version, CassandraVersion::V5_0_NewBig);
        assert_eq!(version, SUPPORTED_VERSION);
    }

    #[test]
    fn test_magic_and_version_invalid() {
        let mut data = Vec::new();
        data.extend_from_slice(&0xDEADBEEFu32.to_be_bytes()); // Invalid magic number
        data.extend_from_slice(&SUPPORTED_VERSION.to_be_bytes());

        let result = parse_magic_and_version(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_cassandra_version_from_magic() {
        assert_eq!(CassandraVersion::from_magic_number(0x6F61_0000), Some(CassandraVersion::Legacy));
        assert_eq!(CassandraVersion::from_magic_number(0xAD01_0000), Some(CassandraVersion::V5_0_Alpha));
        assert_eq!(CassandraVersion::from_magic_number(0xA007_0000), Some(CassandraVersion::V5_0_Beta));
        assert_eq!(CassandraVersion::from_magic_number(0x4316_0000), Some(CassandraVersion::V5_0_Release));
        assert_eq!(CassandraVersion::from_magic_number(0x0040_0000), Some(CassandraVersion::V5_0_NewBig));
        assert_eq!(CassandraVersion::from_magic_number(0xDEADBEEF), None);
    }

    #[test]
    fn test_cassandra_version_strings() {
        assert_eq!(CassandraVersion::Legacy.version_string(), "Legacy 'oa' format");
        assert_eq!(CassandraVersion::V5_0_Alpha.version_string(), "Cassandra 5.0 Alpha");
        assert_eq!(CassandraVersion::V5_0_Beta.version_string(), "Cassandra 5.0 Beta");
        assert_eq!(CassandraVersion::V5_0_Release.version_string(), "Cassandra 5.0 Release");
        assert_eq!(CassandraVersion::V5_0_NewBig.version_string(), "Cassandra 5.0 'nb' (new big) format");
    }

    #[test]
    fn test_vstring_parsing() {
        use super::super::vint::encode_vint;

        let test_str = "test_string";
        let mut data = Vec::new();
        data.extend_from_slice(&encode_vint(test_str.len() as i64));
        data.extend_from_slice(test_str.as_bytes());

        let (_, parsed) = parse_vstring(&data).unwrap();
        assert_eq!(parsed, test_str);
    }

    #[test]
    fn test_column_info_roundtrip() {
        let column = ColumnInfo {
            name: "test_column".to_string(),
            column_type: "text".to_string(),
            is_primary_key: true,
            key_position: Some(0),
            is_static: false,
            is_clustering: false,
        };

        let mut serialized = Vec::new();
        serialize_column_info(&mut serialized, &column).unwrap();

        let (_, parsed) = parse_column_info(&serialized).unwrap();
        assert_eq!(parsed.name, column.name);
        assert_eq!(parsed.column_type, column.column_type);
        assert_eq!(parsed.is_primary_key, column.is_primary_key);
        assert_eq!(parsed.key_position, column.key_position);
    }

    #[test]
    fn test_compression_info_roundtrip() {
        let mut params = HashMap::new();
        params.insert("level".to_string(), "6".to_string());

        let compression = CompressionInfo {
            algorithm: "LZ4".to_string(),
            chunk_size: 4096,
            parameters: params,
        };

        let mut serialized = Vec::new();
        serialize_compression_info(&mut serialized, &compression).unwrap();

        let (_, parsed) = parse_compression_info(&serialized).unwrap();
        assert_eq!(parsed.algorithm, compression.algorithm);
        assert_eq!(parsed.chunk_size, compression.chunk_size);
        assert_eq!(parsed.parameters, compression.parameters);
    }

    #[test]
    fn test_header_serialization_roundtrip() {
        use std::collections::HashMap;
        
        let mut properties = HashMap::new();
        properties.insert("test_key".to_string(), "test_value".to_string());
        
        let mut compression_params = HashMap::new();
        compression_params.insert("level".to_string(), "6".to_string());
        
        let header = SSTableHeader {
            cassandra_version: CassandraVersion::V5_0_NewBig,
            version: SUPPORTED_VERSION,
            table_id: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            keyspace: "test_keyspace".to_string(),
            table_name: "test_table".to_string(),
            generation: 12345,
            compression: CompressionInfo {
                algorithm: "LZ4".to_string(),
                chunk_size: 4096,
                parameters: compression_params,
            },
            stats: SSTableStats {
                row_count: 1000,
                min_timestamp: -1000,
                max_timestamp: 1000,
                max_deletion_time: 500,
                compression_ratio: 0.75,
                row_size_histogram: vec![10, 20, 30],
            },
            columns: vec![ColumnInfo {
                name: "test_column".to_string(),
                column_type: "text".to_string(),
                is_primary_key: true,
                key_position: Some(0),
                is_static: false,
                is_clustering: false,
            }],
            properties,
        };

        // Serialize the header
        let serialized = serialize_sstable_header(&header).unwrap();
        
        // Parse it back
        let (_, parsed_header) = parse_sstable_header(&serialized).unwrap();
        
        // Verify all fields match
        assert_eq!(parsed_header.cassandra_version, header.cassandra_version);
        assert_eq!(parsed_header.version, header.version);
        assert_eq!(parsed_header.table_id, header.table_id);
        assert_eq!(parsed_header.keyspace, header.keyspace);
        assert_eq!(parsed_header.table_name, header.table_name);
        assert_eq!(parsed_header.generation, header.generation);
        assert_eq!(parsed_header.compression.algorithm, header.compression.algorithm);
        assert_eq!(parsed_header.stats.row_count, header.stats.row_count);
        assert_eq!(parsed_header.columns.len(), header.columns.len());
        assert_eq!(parsed_header.properties, header.properties);
    }
}
