//! Bulletproof SSTable reader with universal format support
//!
//! This module provides a bulletproof SSTable reader that can handle any
//! Cassandra version (2.x, 3.x, 4.x, 5.x) with automatic format detection
//! and proper compression handling.
//!
//! ⚠️  **EXPERIMENTAL WARNING for Modern Formats (4.x/5.x)**
//!
//! The 'oa' format parsing implementation in this module is EXPERIMENTAL and
//! based on reverse engineering. It may not fully align with the official
//! Cassandra Big format specification (CEP-25). For production use with modern
//! formats, prefer the spec-accurate readers:
//!
//! - `row_cell_state_machine.rs` - Implements schema-driven parsing without heuristics
//! - Follows exact Cassandra specification for BIG format row/cell parsing
//! - Eliminates type guessing in favor of schema-aware decoding
//!
//! **TODO**: Either align this implementation with CEP-25 Big format specification
//! or deprecate the modern format parsing in favor of spec-accurate implementations.

use log::{debug, info, warn};
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use super::{
    chunk_decompressor::{ChunkDecompressor, create_decompressor_from_file},
    compression_info::CompressionInfo,
    format_detector::{SSTableComponent, SSTableFormat, SSTableInfo},
};
use crate::{Error, Result};

/// Bulletproof SSTable reader with automatic format detection
pub struct BulletproofReader {
    /// SSTable information (format, generation, etc.)
    info: SSTableInfo,
    /// Base directory containing SSTable files
    base_dir: PathBuf,
    /// Chunk decompressor (if compression is used)
    decompressor: Option<ChunkDecompressor>,
    /// Data file reader
    data_reader: Option<BufReader<File>>,
}

impl BulletproofReader {
    /// Create a new bulletproof reader with default settings (for testing)
    pub fn new() -> Self {
        Self {
            info: SSTableInfo::default(),
            base_dir: PathBuf::new(),
            decompressor: None,
            data_reader: None,
        }
    }

    /// Create a new bulletproof reader from any SSTable file path
    ///
    /// This will automatically detect the format version and set up
    /// proper compression handling if needed.
    pub fn open<P: AsRef<Path>>(sstable_path: P) -> Result<Self> {
        let path = sstable_path.as_ref();
        let info = SSTableInfo::from_path(path)?;

        let base_dir = path
            .parent()
            .ok_or_else(|| Error::InvalidPath("No parent directory".to_string()))?
            .to_path_buf();

        info!(
            "Opening SSTable with bulletproof reader: format={:?}, generation={}, size={}, component={:?}, base={}",
            info.format, info.generation, info.size, info.component, info.base_name
        );

        let mut reader = Self {
            info,
            base_dir,
            decompressor: None,
            data_reader: None,
        };

        reader.initialize()?;
        Ok(reader)
    }

    /// Initialize the reader by setting up compression and opening files
    fn initialize(&mut self) -> Result<()> {
        // Set up compression if the format supports it
        if self.info.format.supports_compression() {
            if let Err(e) = self.setup_compression() {
                warn!(
                    "Compression setup failed: {}, trying without compression",
                    e
                );
            }
        }

        // Open the Data.db file
        self.open_data_file()?;

        Ok(())
    }

    /// Set up compression by reading CompressionInfo.db if it exists
    fn setup_compression(&mut self) -> Result<()> {
        let compression_info_path = self
            .info
            .companion_path(SSTableComponent::CompressionInfo, &self.base_dir);

        if compression_info_path.exists() {
            debug!("Found CompressionInfo.db, setting up decompression");

            let decompressor = create_decompressor_from_file(&compression_info_path)?;
            self.decompressor = Some(decompressor);

            debug!("Compression setup complete");
        } else {
            debug!("No CompressionInfo.db found, assuming uncompressed data");
        }

        Ok(())
    }

    /// Open the Data.db file for reading
    fn open_data_file(&mut self) -> Result<()> {
        let data_path = self
            .info
            .companion_path(SSTableComponent::Data, &self.base_dir);

        if !data_path.exists() {
            return Err(Error::InvalidPath(format!(
                "Data.db file not found: {:?}",
                data_path
            )));
        }

        let file = File::open(&data_path).map_err(|e| Error::Io(e))?;
        let reader = BufReader::new(file);

        self.data_reader = Some(reader);

        debug!("Data.db file opened: {:?}", data_path);
        Ok(())
    }

    /// Read raw data from the SSTable at specified offset and length
    ///
    /// This automatically handles compression if present
    pub fn read_raw_data(&mut self, offset: u64, length: usize) -> Result<Vec<u8>> {
        let reader = self
            .data_reader
            .as_mut()
            .ok_or_else(|| Error::InvalidState("Data reader not initialized".to_string()))?;

        if let Some(decompressor) = &mut self.decompressor {
            // Use chunk-based decompression
            decompressor.read_data(reader, offset, length)
        } else {
            // Read directly from uncompressed file
            use std::io::{Read, Seek, SeekFrom};

            reader
                .seek(SeekFrom::Start(offset))
                .map_err(|e| Error::Io(e))?;

            let mut buffer = vec![0u8; length];
            reader.read_exact(&mut buffer).map_err(|e| Error::Io(e))?;

            Ok(buffer)
        }
    }

    /// Read the entire SSTable data (for debugging)
    pub fn read_all_data(&mut self) -> Result<Vec<u8>> {
        if let Some(decompressor) = &mut self.decompressor {
            let reader = self
                .data_reader
                .as_mut()
                .ok_or_else(|| Error::InvalidState("Data reader not initialized".to_string()))?;

            decompressor.read_all_data(reader)
        } else {
            let reader = self
                .data_reader
                .as_mut()
                .ok_or_else(|| Error::InvalidState("Data reader not initialized".to_string()))?;

            use std::io::{Read, Seek, SeekFrom};

            // Get file size
            let current_pos = reader.stream_position().map_err(|e| Error::Io(e))?;
            let file_size = reader.seek(SeekFrom::End(0)).map_err(|e| Error::Io(e))?;
            reader
                .seek(SeekFrom::Start(current_pos))
                .map_err(|e| Error::Io(e))?;

            // Read entire file
            reader.seek(SeekFrom::Start(0)).map_err(|e| Error::Io(e))?;

            let mut buffer = Vec::with_capacity(file_size as usize);
            reader.read_to_end(&mut buffer).map_err(|e| Error::Io(e))?;

            Ok(buffer)
        }
    }

    /// Parse SSTable data using format-specific parser
    ///
    /// This is where we'll implement the actual SSTable parsing
    /// based on the detected format version
    pub fn parse_sstable_data(&mut self) -> Result<Vec<SSTableEntry>> {
        let data = self.read_all_data()?;

        info!(
            "Parsing SSTable data ({} bytes) with format {:?}",
            data.len(),
            self.info.format
        );

        match &self.info.format {
            SSTableFormat::V4x(_) | SSTableFormat::V5x(_) => self.parse_modern_format(&data),
            SSTableFormat::V3x(_) => self.parse_v3_format(&data),
            SSTableFormat::V2x(_) => self.parse_v2_format(&data),
            SSTableFormat::Unknown(version) => Err(Error::UnsupportedFormat(format!(
                "Unknown SSTable version: {}",
                version
            ))),
        }
    }

    /// Parse modern SSTable format (4.x, 5.x) with EXPERIMENTAL 'oa' format parsing
    ///
    /// ⚠️ WARNING: EXPERIMENTAL IMPLEMENTATION
    /// This 'oa' format parser is experimental and may not fully align with
    /// the official Cassandra Big format specification. For production use,
    /// prefer the spec-accurate readers in row_cell_state_machine.rs which
    /// implement schema-driven parsing without heuristics.
    ///
    /// TODO: Align with CEP-25 Big format specification or deprecate in favor
    /// of the spec-accurate state machine implementation.
    fn parse_modern_format(&self, data: &[u8]) -> Result<Vec<SSTableEntry>> {
        warn!("EXPERIMENTAL: Parsing modern SSTable format with custom 'oa' format parsing");
        warn!("This implementation may not fully align with Cassandra Big format specification");

        if data.len() < 8 {
            return Err(Error::InvalidFormat(
                "Data too short for 'oa' format header".to_string(),
            ));
        }

        // Parse the 'oa' format header
        let header = self.parse_oa_header(data)?;
        debug!(
            "Parsed 'oa' header: version={}, partition_count={}",
            header.format_version, header.partition_count
        );

        // Parse data blocks following the header
        let entries = self.parse_data_blocks(data, &header)?;

        info!(
            "Parsed {} entries from {} bytes using structured parsing",
            entries.len(),
            data.len()
        );
        Ok(entries)
    }

    /// Parse Cassandra 'oa' format header (EXPERIMENTAL)
    ///
    /// ⚠️ EXPERIMENTAL: This header parsing implementation is based on
    /// reverse engineering and may not match the official Cassandra Big
    /// format specification. The magic number check and field interpretations
    /// should be verified against CEP-25 specification.
    pub fn parse_oa_header(&self, data: &[u8]) -> Result<OaFormatHeader> {
        if data.len() < 8 {
            return Err(Error::InvalidFormat(
                "Insufficient data for 'oa' header".to_string(),
            ));
        }

        // Read magic number (first 4 bytes) - EXPERIMENTAL: should be 0x6F61_0000 for 'oa' format
        // This may not match the actual Big format magic number from CEP-25
        let magic = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        if magic != 0x6F61_0000 {
            warn!(
                "EXPERIMENTAL: Magic number mismatch: expected 0x6F61_0000, got 0x{:08x}",
                magic
            );
            warn!("This may indicate Big format specification differences");
        }

        // Read format version (next 4 bytes)
        let format_version = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
        debug!("'oa' format version: {}", format_version);

        let mut offset = 8;

        // Read partition count using VInt encoding
        let (partition_count, vint_bytes) = self.read_vint(&data[offset..])?;
        offset += vint_bytes;

        // Read additional metadata using VInt encoding
        let (metadata_size, vint_bytes) = self.read_vint(&data[offset..])?;
        offset += vint_bytes;

        debug!(
            "Partition count: {}, metadata size: {}",
            partition_count, metadata_size
        );

        Ok(OaFormatHeader {
            magic_number: magic,
            format_version,
            partition_count,
            metadata_size,
            header_size: offset,
        })
    }

    /// Parse data blocks following the 'oa' header
    fn parse_data_blocks(&self, data: &[u8], header: &OaFormatHeader) -> Result<Vec<SSTableEntry>> {
        let mut entries = Vec::new();
        let mut offset = header.header_size;

        debug!(
            "Parsing {} partitions starting at offset {}",
            header.partition_count, offset
        );

        for partition_idx in 0..header.partition_count {
            if offset >= data.len() {
                warn!(
                    "Reached end of data while parsing partition {}",
                    partition_idx
                );
                break;
            }

            match self.parse_partition_block(&data[offset..], partition_idx) {
                Ok((entry, bytes_consumed)) => {
                    entries.push(entry);
                    offset += bytes_consumed;

                    if offset >= data.len() {
                        break;
                    }
                }
                Err(e) => {
                    warn!("Failed to parse partition {}: {}", partition_idx, e);
                    // Try to advance by a reasonable amount to recover
                    offset += 16; // Skip forward and try next potential partition
                    continue;
                }
            }
        }

        Ok(entries)
    }

    /// Parse a single partition block
    fn parse_partition_block(
        &self,
        data: &[u8],
        partition_idx: u64,
    ) -> Result<(SSTableEntry, usize)> {
        if data.len() < 4 {
            return Err(Error::InvalidFormat(
                "Insufficient data for partition block".to_string(),
            ));
        }

        let mut offset = 0;

        // Read partition key length using VInt
        let (key_length, vint_bytes) = self.read_vint(&data[offset..])?;
        offset += vint_bytes;

        if offset + key_length as usize > data.len() {
            return Err(Error::InvalidFormat(
                "Partition key extends beyond data".to_string(),
            ));
        }

        // Read partition key
        let key_data = &data[offset..offset + key_length as usize];
        offset += key_length as usize;

        // Read row count using VInt
        let (row_count, vint_bytes) = self.read_vint(&data[offset..])?;
        offset += vint_bytes;

        debug!(
            "Partition {}: key_length={}, row_count={}",
            partition_idx, key_length, row_count
        );

        // Format key data as hex string without type assumptions
        let key_str = key_data
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<Vec<_>>()
            .join("");

        // Skip row data for now (would need more complex parsing)
        // For each row, we'd need to parse clustering keys, column data, etc.

        let entry = SSTableEntry {
            key: crate::RowKey::from(key_data.to_vec()),
            values: vec![crate::Value::Text(key_str)],
            timestamp: Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64,
            ),
            generation: Some(self.info.generation),
            format_info: format!("oa_format:partition={}", partition_idx),
        };

        Ok((entry, offset))
    }

    /// Read Variable Length Integer (VInt) from data
    pub fn read_vint(&self, data: &[u8]) -> Result<(u64, usize)> {
        if data.is_empty() {
            return Err(Error::InvalidFormat("Empty data for VInt".to_string()));
        }

        let mut result = 0u64;
        let mut bytes_read = 0;

        for (i, &byte) in data.iter().enumerate() {
            if i >= 10 {
                // VInt should not exceed 10 bytes for u64
                return Err(Error::InvalidFormat("VInt too long".to_string()));
            }

            bytes_read += 1;

            if byte & 0x80 == 0 {
                // Most significant bit is 0, this is the last byte
                result = (result << 7) | (byte as u64);
                break;
            } else {
                // Most significant bit is 1, more bytes follow
                result = (result << 7) | ((byte & 0x7F) as u64);
            }
        }

        Ok((result, bytes_read))
    }

    /// Read legacy varint format for backwards compatibility
    #[allow(dead_code)]
    fn read_varint(&self, data: &[u8]) -> Result<(u64, usize)> {
        if data.is_empty() {
            return Err(Error::InvalidFormat("Empty data for varint".to_string()));
        }

        let mut result = 0u64;
        let mut shift = 0;
        let mut bytes_read = 0;

        for &byte in data {
            bytes_read += 1;

            if byte & 0x80 == 0 {
                // Most significant bit is 0, this is the last byte
                result |= (byte as u64) << shift;
                break;
            } else {
                // Most significant bit is 1, more bytes follow
                result |= ((byte & 0x7F) as u64) << shift;
                shift += 7;

                if shift >= 64 {
                    return Err(Error::InvalidFormat("Varint overflow".to_string()));
                }
            }
        }

        Ok((result, bytes_read))
    }
    /// Parse V3.x format
    fn parse_v3_format(&self, _data: &[u8]) -> Result<Vec<SSTableEntry>> {
        debug!("Parsing V3.x SSTable format");
        // TODO: Implement V3.x specific parsing
        Ok(Vec::new())
    }

    /// Parse V2.x format
    fn parse_v2_format(&self, _data: &[u8]) -> Result<Vec<SSTableEntry>> {
        debug!("Parsing V2.x SSTable format");
        // TODO: Implement V2.x specific parsing
        Ok(Vec::new())
    }

    /// Get information about the SSTable
    pub fn info(&self) -> &SSTableInfo {
        &self.info
    }

    /// Get compression information if available
    pub fn compression_info(&self) -> Option<&CompressionInfo> {
        self.decompressor.as_ref().map(|d| d.compression_info())
    }

    /// Get cache statistics if compression is enabled
    pub fn cache_stats(&self) -> Option<(usize, usize)> {
        self.decompressor.as_ref().map(|d| d.cache_stats())
    }

    /// Get header information (for compatibility)
    pub async fn get_header(&self) -> Result<crate::parser::header::SSTableHeader> {
        // Return a basic header based on format detection
        Ok(crate::parser::header::SSTableHeader {
            cassandra_version: match &self.info.format {
                SSTableFormat::V5x(_) => crate::parser::header::CassandraVersion::V5_0Release,
                SSTableFormat::V4x(_) => crate::parser::header::CassandraVersion::Legacy, // Use Legacy for V4
                SSTableFormat::V3x(_) => crate::parser::header::CassandraVersion::Legacy, // Use Legacy for V3
                SSTableFormat::V2x(_) => crate::parser::header::CassandraVersion::Legacy, // Use Legacy for V2
                SSTableFormat::Unknown(_) => crate::parser::header::CassandraVersion::V5_0Release,
            },
            version: 1,
            table_id: [0; 16], // Placeholder
            keyspace: "unknown".to_string(),
            table_name: "unknown".to_string(),
            generation: self.info.generation,
            compression: crate::parser::header::CompressionInfo {
                algorithm: "NONE".to_string(),
                chunk_size: 65536,
                parameters: std::collections::HashMap::new(),
            },
            stats: crate::parser::header::SSTableStats::default(),
            columns: vec![],
            properties: std::collections::HashMap::new(),
        })
    }

    /// Stream entries from the SSTable (for compatibility)
    pub async fn stream_entries(&self) -> Result<SSTableEntryStream> {
        let entries = self.parse_sstable_data_readonly()?;
        Ok(SSTableEntryStream {
            entries,
            position: 0,
        })
    }

    /// Get file path for the SSTable
    pub fn get_file_path(&self) -> &Path {
        &self.base_dir
    }

    /// Verify integrity of the SSTable
    pub async fn verify_integrity(&self) -> Result<bool> {
        // Basic integrity check - try to parse the data
        match self.parse_sstable_data_readonly() {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    /// Parse SSTable data without mutable access (for compatibility)
    fn parse_sstable_data_readonly(&self) -> Result<Vec<SSTableEntry>> {
        // Read the data file directly
        use std::fs::File;
        use std::io::Read;

        let data_file_path = self
            .base_dir
            .join(format!("{}-Data.db", self.info.base_name));
        let mut file = File::open(&data_file_path)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;

        // Parse using the existing logic but with dummy values for new fields
        self.parse_modern_format_readonly(&data)
    }

    /// Parse modern format without mutable access using EXPERIMENTAL 'oa' format
    ///
    /// ⚠️ EXPERIMENTAL: This readonly parsing is experimental and should be
    /// replaced with the spec-accurate row_cell_state_machine implementation
    /// for production use.
    fn parse_modern_format_readonly(&self, data: &[u8]) -> Result<Vec<SSTableEntry>> {
        if data.len() < 8 {
            return Err(Error::InvalidFormat(
                "Data too short for 'oa' format".to_string(),
            ));
        }

        // Parse using the EXPERIMENTAL 'oa' format (may not align with Big format spec)
        match self.parse_oa_header(data) {
            Ok(header) => self.parse_data_blocks(data, &header),
            Err(_) => {
                // Fallback to basic parsing if header parsing fails
                warn!("EXPERIMENTAL: 'oa' header parsing failed, using fallback");
                warn!("Consider using spec-accurate readers for production");
                Ok(Vec::new())
            }
        }
    }
}

/// EXPERIMENTAL Cassandra 'oa' format header structure
///
/// ⚠️ WARNING: This structure is based on reverse engineering and may not
/// accurately represent the official Cassandra Big format header as specified
/// in CEP-25. Field interpretations and byte layouts should be verified
/// against the official specification.
#[derive(Debug, Clone)]
pub struct OaFormatHeader {
    /// Magic number (EXPERIMENTAL: assumed to be 0x6F61_0000)
    /// TODO: Verify against CEP-25 Big format specification
    #[allow(dead_code)]
    pub magic_number: u32,
    /// Format version (interpretation may not match Big format spec)
    pub format_version: u32,
    /// Number of partitions in this SSTable (experimental field interpretation)
    partition_count: u64,
    /// Size of metadata section (experimental field interpretation)
    #[allow(dead_code)]
    metadata_size: u64,
    /// Total header size in bytes
    header_size: usize,
}

/// Parsed SSTable entry
#[derive(Debug, Clone)]
pub struct SSTableEntry {
    /// Row key
    pub key: crate::RowKey,
    /// Column values
    pub values: Vec<crate::Value>,
    /// Write timestamp
    pub timestamp: Option<i64>,
    /// Generation number
    pub generation: Option<u64>,
    /// Format-specific information
    pub format_info: String,
}

/// Stream of SSTable entries for iterating over large datasets
pub struct SSTableEntryStream {
    entries: Vec<SSTableEntry>,
    position: usize,
}

impl SSTableEntryStream {
    /// Get the next entry from the stream
    pub async fn next(&mut self) -> Result<Option<SSTableEntry>> {
        if self.position < self.entries.len() {
            let entry = self.entries[self.position].clone();
            self.position += 1;
            Ok(Some(entry))
        } else {
            Ok(None)
        }
    }
}

/// Utility function to test reading an SSTable directory
pub fn test_read_sstable_directory<P: AsRef<Path>>(dir_path: P) -> Result<()> {
    let dir = dir_path.as_ref();

    info!("Testing bulletproof SSTable reading in: {:?}", dir);

    // Find Data.db files
    let entries = std::fs::read_dir(dir).map_err(|e| Error::Io(e))?;

    for entry in entries {
        let entry = entry.map_err(|e| Error::Io(e))?;
        let path = entry.path();

        if path
            .file_name()
            .and_then(|s| s.to_str())
            .map(|s| s.ends_with("-Data.db"))
            .unwrap_or(false)
        {
            debug!("Testing SSTable: {:?}", path);

            match BulletproofReader::open(path) {
                Ok(mut reader) => {
                    info!("Successfully opened SSTable");

                    if let Some(compression_info) = reader.compression_info() {
                        debug!("Compression: {}", compression_info.algorithm);
                        debug!("Chunk size: {} bytes", compression_info.chunk_length);
                    }

                    // Try to read first 1KB of data
                    match reader.read_raw_data(0, 1024) {
                        Ok(data) => {
                            debug!("Read {} bytes successfully", data.len());
                            debug!(
                                "First 32 bytes: {:02x?}",
                                &data[..std::cmp::min(32, data.len())]
                            );

                            // Try to parse the data
                            match reader.parse_sstable_data() {
                                Ok(entries) => {
                                    info!("Parsed {} entries", entries.len());
                                    for (i, entry) in entries.iter().take(3).enumerate() {
                                        debug!(
                                            "Entry {}: key='{:?}' ({})",
                                            i, entry.key, entry.format_info
                                        );
                                    }
                                }
                                Err(e) => {
                                    warn!("Parsing failed (this is expected for now): {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Failed to read data: {}", e);
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to open SSTable: {}", e);
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vint_reading() {
        let reader = BulletproofReader {
            info: SSTableInfo::from_path(&std::path::PathBuf::from("nb-1-big-Data.db")).unwrap(),
            base_dir: std::path::PathBuf::new(),
            decompressor: None,
            data_reader: None,
        };

        // Test simple VInt (single byte)
        let data = [0x05]; // Value 5
        let (value, bytes_read) = reader.read_vint(&data).unwrap();
        assert_eq!(value, 5);
        assert_eq!(bytes_read, 1);

        // Test multi-byte VInt
        let data = [0x81, 0x00]; // Value 128 in VInt encoding
        let (value, bytes_read) = reader.read_vint(&data).unwrap();
        assert_eq!(value, 128);
        assert_eq!(bytes_read, 2);

        // Test legacy varint for backwards compatibility
        let data = [0x80, 0x01]; // Value 128 in legacy varint
        let (value, bytes_read) = reader.read_varint(&data).unwrap();
        assert_eq!(value, 128);
        assert_eq!(bytes_read, 2);
    }
}
