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

/// Magic number for Cassandra 5+ SSTable format ('oa' format)
pub const SSTABLE_MAGIC: u32 = 0x6F61_0000; // 'oa' followed by version bytes

/// Current supported format version
pub const SUPPORTED_VERSION: u16 = 0x0001;

/// SSTable header containing metadata about the table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSTableHeader {
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

/// Parse the SSTable magic number and version
pub fn parse_magic_and_version(input: &[u8]) -> IResult<&[u8], u16> {
    let (input, magic) = be_u32(input)?;
    if magic != SSTABLE_MAGIC {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Tag,
        )));
    }

    let (input, version) = be_u16(input)?;
    if version != SUPPORTED_VERSION {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }

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
    let (input, version) = parse_magic_and_version(input)?;
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

    // Magic and version
    result.extend_from_slice(&SSTABLE_MAGIC.to_be_bytes());
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
    fn test_magic_and_version() {
        let mut data = Vec::new();
        data.extend_from_slice(&SSTABLE_MAGIC.to_be_bytes());
        data.extend_from_slice(&SUPPORTED_VERSION.to_be_bytes());

        let (_, version) = parse_magic_and_version(&data).unwrap();
        assert_eq!(version, SUPPORTED_VERSION);
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
}
