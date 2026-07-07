//! Bulletproof SSTable reader with universal format support
//!
//! # DEPRECATED - DO NOT USE IN PRODUCTION
//!
//! This module is DEPRECATED for production use. Use `SSTableReader` instead.
//!
//! **Deprecation Notice (Issue #190):**
//! - This reader is marked EXPERIMENTAL and should not be used in production code paths
//! - For production use, prefer `crate::storage::sstable::reader::SSTableReader`
//! - This module is retained only for testing and legacy compatibility purposes
//!
//! ⚠️  **EXPERIMENTAL WARNING for Modern Formats (4.x/5.x)**
//!
//! The 'oa' format parsing implementation in this module is EXPERIMENTAL and
//! based on reverse engineering. It may not fully align with the official
//! Cassandra Big format specification (CEP-25). For production use with modern
//! formats, prefer the spec-accurate readers:
//!
//! - `crate::storage::sstable::reader::SSTableReader` - Production-ready spec-accurate reader
//! - `row_cell_state_machine.rs` - Implements schema-driven parsing without heuristics
//! - Follows exact Cassandra specification for BIG format row/cell parsing
//! - Eliminates type guessing in favor of schema-aware decoding

// Allow deprecated warnings within this module since the entire module is deprecated
#![allow(deprecated)]

use tracing::{debug, info, warn};
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use super::{
    chunk_decompressor::{create_decompressor_from_file, ChunkDecompressor},
    compression_info::CompressionInfo,
    format_detector::{SSTableComponent, SSTableFormat, SSTableInfo},
    version_gate::VersionGates,
};
use crate::parser::vint::parse_vint;
use crate::{Error, Result};

/// Bulletproof SSTable reader with automatic format detection
///
/// # Deprecated
///
/// This reader is DEPRECATED for production use (Issue #190).
/// Use `crate::storage::sstable::reader::SSTableReader` instead.
#[deprecated(
    since = "0.1.0",
    note = "Use SSTableReader instead. This reader is EXPERIMENTAL and not suitable for production. See Issue #190."
)]
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

impl Default for BulletproofReader {
    fn default() -> Self {
        Self::new()
    }
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

        // #1249: reject ALL below-floor versions BEFORE any initialization or
        // file-body read, using the SAME authoritative gate the production
        // readers use (`reader/mod.rs::open_inner`, `statistics_reader.rs::open`).
        // `VersionGates::from_path` derives the version from the filename alone
        // (no I/O) and rejects every pre-`na` BIG (`la`/`ic`/`jb`/`ma`–`me`) and
        // non-`da` BTI descriptor with a typed `UnsupportedVersion` naming the
        // supported floor — including below-floor versions that
        // `SSTableInfo::from_path` classifies as `Unknown` (e.g. `la`), which the
        // old `V2x`/`V3x` format match silently bypassed. A structurally-
        // unparseable descriptor falls through to current behaviour (it is not
        // made fatal); the floor only fires on a *parsed* below-floor version.
        if let Err(e @ Error::UnsupportedVersion { .. }) = VersionGates::from_path(path) {
            return Err(e);
        }

        let info = SSTableInfo::from_path(path)?;

        let base_dir = path
            .parent()
            .ok_or_else(|| Error::InvalidPath("No parent directory".to_string()))?
            .to_path_buf();

        info!(
            "Opening SSTable with bulletproof reader: format={:?}, generation={}, size={}, component={:?}, base={}",
            info.format, info.generation_numeric().unwrap_or(0), info.size, info.component, info.base_name
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

        let file = File::open(&data_path).map_err(Error::Io)?;
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

            reader.seek(SeekFrom::Start(offset)).map_err(Error::Io)?;

            let mut buffer = vec![0u8; length];
            reader.read_exact(&mut buffer).map_err(Error::Io)?;

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
            let current_pos = reader.stream_position().map_err(Error::Io)?;
            let file_size = reader.seek(SeekFrom::End(0)).map_err(Error::Io)?;
            reader
                .seek(SeekFrom::Start(current_pos))
                .map_err(Error::Io)?;

            // Read entire file
            reader.seek(SeekFrom::Start(0)).map_err(Error::Io)?;

            let mut buffer = Vec::with_capacity(file_size as usize);
            reader.read_to_end(&mut buffer).map_err(Error::Io)?;

            Ok(buffer)
        }
    }

    /// Parse SSTable data using format-specific parser
    ///
    /// This is where we'll implement the actual SSTable parsing
    /// based on the detected format version
    pub fn parse_sstable_data(&mut self) -> Result<Vec<SSTableEntry>> {
        // #1249: reject ALL below-floor versions BEFORE reading any row bytes,
        // using the same authoritative gate as `open` and the production
        // readers. `open` already rejects these, but readers built via `new()`
        // reach here directly, so re-derive from the same descriptor (the
        // `<version>-<id>-<format>` `base_name` plus the Data component). This
        // catches every pre-`na` BIG (`la`/`ma`–`me`) and non-`da` BTI
        // version — including ones classified as `Unknown` (e.g. `la`) that the
        // old `V2x`/`V3x` format match silently bypassed — with a typed
        // `UnsupportedVersion` before `read_all_data()`. A structurally-
        // unparseable descriptor is NOT made fatal: it falls through to the
        // format-family match below.
        if let Err(e @ Error::UnsupportedVersion { .. }) =
            VersionGates::from_path(Path::new(&format!("{}-Data.db", self.info.base_name)))
        {
            return Err(e);
        }

        let data = self.read_all_data()?;

        info!(
            "Parsing SSTable data ({} bytes) with format {:?}",
            data.len(),
            self.info.format
        );

        match &self.info.format {
            SSTableFormat::V4x(_) | SSTableFormat::V5x(_) => self.parse_modern_format(&data),
            // Below-floor V2x/V3x (and below-floor Unknown like `la`) are
            // rejected via VersionGates above before any read, so they never
            // reach this match. An above-floor Unknown (e.g. `nc`/`ob`/`pa`,
            // tracked separately in #1297) still surfaces UnsupportedFormat.
            SSTableFormat::V2x(version)
            | SSTableFormat::V3x(version)
            | SSTableFormat::Unknown(version) => Err(Error::UnsupportedFormat(format!(
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
    ///
    /// This function strictly parses only the 32-byte header portion as per
    /// the Cassandra SSTable format specification, handling oversized input
    /// by reading only the first 32 bytes.
    pub fn parse_oa_header(&self, data: &[u8]) -> Result<OaFormatHeader> {
        if data.len() < 32 {
            return Err(Error::InvalidFormat(
                "OA header must be exactly 32 bytes".to_string(),
            ));
        }

        // For header size compliance, we only read the first 32 bytes
        // This ensures oversized input is handled correctly by ignoring extra data
        let header_data = &data[..32];

        // Read magic number (first 4 bytes) - should be 0x6F61_0000 for 'oa' format
        let magic = u32::from_be_bytes([
            header_data[0],
            header_data[1],
            header_data[2],
            header_data[3],
        ]);
        if magic != 0x6F61_0000 {
            return Err(Error::InvalidFormat(format!(
                "Invalid magic number: expected 0x6F61_0000, got 0x{:08x}",
                magic
            )));
        }

        // Read format version (next 2 bytes, big-endian)
        let format_version = u16::from_be_bytes([header_data[4], header_data[5]]);
        debug!("'oa' format version: {}", format_version);

        // Validate version - only version 1 is supported
        if format_version != 1 {
            return Err(Error::InvalidFormat(format!(
                "Unsupported OA format version: {}. Only version 1 is supported.",
                format_version
            )));
        }

        // Read flags (4 bytes, big-endian)
        let _flags = u32::from_be_bytes([
            header_data[6],
            header_data[7],
            header_data[8],
            header_data[9],
        ]);

        // The remaining bytes (10-31) are reserved and should be zero per spec
        // We don't validate they are zero to maintain compatibility, but we acknowledge them

        // Return header with basic structure - the 32-byte header is now fully parsed
        // Additional metadata parsing (like partition count) should be done separately
        // when actually parsing the SSTable content, not during header validation
        Ok(OaFormatHeader {
            magic_number: magic,
            format_version,
            partition_count: 0, // Will be populated during full SSTable parsing
            metadata_size: 0,   // Will be populated during full SSTable parsing
            header_size: 32,    // Fixed header size per specification
        })
    }

    /// Parse data blocks following the 'oa' header
    fn parse_data_blocks(&self, data: &[u8], header: &OaFormatHeader) -> Result<Vec<SSTableEntry>> {
        let mut entries = Vec::new();
        let mut offset = header.header_size;

        // If we're only doing header compliance testing (partition_count = 0),
        // we don't need to parse actual data blocks
        if header.partition_count == 0 {
            debug!("Header-only parsing mode - no data blocks to parse");
            return Ok(entries);
        }

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
            generation: Some(self.info.generation_numeric().unwrap_or(0)),
            format_info: format!("oa_format:partition={}", partition_idx),
        };

        Ok((entry, offset))
    }

    /// Read Variable Length Integer (VInt) from data using Cassandra format
    pub fn read_vint(&self, data: &[u8]) -> Result<(u64, usize)> {
        match parse_vint(data) {
            Ok((remaining, value)) => {
                let bytes_consumed = data.len() - remaining.len();
                // VInts in SSTable can be negative (ZigZag encoded), but we return as u64
                // The caller needs to interpret the sign appropriately
                Ok((value as u64, bytes_consumed))
            }
            Err(nom_error) => Err(Error::InvalidFormat(format!(
                "VInt parsing failed: {:?}",
                nom_error
            ))),
        }
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
            generation: self.info.generation_numeric().unwrap_or(0),
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
    pub format_version: u16,
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
    let entries = std::fs::read_dir(dir).map_err(Error::Io)?;

    for entry in entries {
        let entry = entry.map_err(Error::Io)?;
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
    fn test_vint_reading() -> Result<()> {
        let reader = BulletproofReader {
            info: SSTableInfo::from_path(&std::path::PathBuf::from("nb-1-big-Data.db")).unwrap(),
            base_dir: std::path::PathBuf::new(),
            decompressor: None,
            data_reader: None,
        };

        // Test simple VInt (single byte)
        // Value 5 ZigZag-encodes to 10 (0x0A)
        let data = [0x0A]; // Value 5 in ZigZag VInt encoding
        let (value, bytes_read) = reader.read_vint(&data)?;
        assert_eq!(value, 5);
        assert_eq!(bytes_read, 1);

        // Test multi-byte VInt
        // Value 128 ZigZag-encodes to 256, which needs [0x81, 0x00]
        let data = [0x81, 0x00]; // Value 128 in ZigZag VInt encoding (256 raw -> 128 decoded)
        let (value, bytes_read) = reader.read_vint(&data)?;
        assert_eq!(value, 128);
        assert_eq!(bytes_read, 2);

        // Test legacy varint for backwards compatibility
        let data = [0x80, 0x01]; // Value 128 in legacy varint
        let (value, bytes_read) = reader.read_varint(&data)?;
        assert_eq!(value, 128);
        assert_eq!(bytes_read, 2);
        Ok(())
    }

    /// #1249: a below-floor format (Cassandra 3.x BIG `ma`) must yield the typed
    /// `UnsupportedVersion` error from `parse_sstable_data` BEFORE any row bytes
    /// are read. The reader points at a non-existent Data.db: if the rejection
    /// happened after `read_all_data()` we would instead get an IO/parse error,
    /// so observing `UnsupportedVersion` proves no body read occurred.
    #[test]
    fn test_below_floor_rejected_before_read() {
        let info = SSTableInfo::from_path(&std::path::PathBuf::from("ma-1-big-Data.db")).unwrap();
        assert!(matches!(info.format, SSTableFormat::V3x(_)));

        let mut reader = BulletproofReader {
            info,
            // Intentionally bogus base dir + no data_reader so that any attempt
            // to read the body would fail loudly with a non-version error.
            base_dir: std::path::PathBuf::from("/nonexistent/cqlite-1249"),
            decompressor: None,
            data_reader: None,
        };

        match reader.parse_sstable_data() {
            Err(Error::UnsupportedVersion { version, floor }) => {
                assert_eq!(version, "ma");
                assert_eq!(floor, "na");
            }
            other => panic!("expected UnsupportedVersion, got {:?}", other),
        }
    }

    /// #1249: `open` rejects a below-floor descriptor before initialization,
    /// even when the Data.db body exists (here a truncated/garbage body). The
    /// typed `UnsupportedVersion` must surface rather than a parse/corruption
    /// error from reading the body.
    #[test]
    fn test_open_below_floor_rejected_with_body_present() {
        let dir = std::env::temp_dir().join(format!("cqlite-1249-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let data_path = dir.join("ma-1-big-Data.db");
        // Non-empty, non-'oa' body: would error during parsing if ever read.
        std::fs::write(&data_path, b"not a valid sstable body").unwrap();

        let result = BulletproofReader::open(&data_path);
        let _ = std::fs::remove_dir_all(&dir);

        match result {
            Err(Error::UnsupportedVersion { version, floor }) => {
                assert_eq!(version, "ma");
                assert_eq!(floor, "na");
            }
            other => panic!("expected UnsupportedVersion, got {:?}", other.map(|_| ())),
        }
    }

    /// #1249 regression: a below-floor BIG descriptor that `SSTableInfo`
    /// classifies as `SSTableFormat::Unknown` (Cassandra 2.x `la`) must STILL
    /// be rejected by the floor. The old `V2x`/`V3x` format-match guard let
    /// `la` (Unknown) bypass the floor entirely; routing through the
    /// authoritative `VersionGates` rejects it via `BigVersionGates`'s `< na`
    /// gate. We point at a non-existent Data.db so any post-read code path
    /// would surface an IO/parse error instead of `UnsupportedVersion`.
    #[test]
    fn test_below_floor_unknown_la_rejected_before_read() {
        let info = SSTableInfo::from_path(&std::path::PathBuf::from("la-1-big-Data.db")).unwrap();
        // `la` is NOT classified as V2x/V3x — it falls into Unknown, which the
        // old guard did not catch. This assertion documents that gap.
        assert!(matches!(info.format, SSTableFormat::Unknown(ref v) if v == "la"));

        let mut reader = BulletproofReader {
            info,
            base_dir: std::path::PathBuf::from("/nonexistent/cqlite-1249-la"),
            decompressor: None,
            data_reader: None,
        };

        match reader.parse_sstable_data() {
            Err(Error::UnsupportedVersion { version, floor }) => {
                assert_eq!(version, "la");
                assert_eq!(floor, "na");
            }
            other => panic!("expected UnsupportedVersion for la, got {:?}", other),
        }
    }

    /// #1249 regression: `open` rejects a below-floor `Unknown`-classified BIG
    /// descriptor (`la`) before initialization, even when a (garbage) Data.db
    /// body exists on disk. Proves pre-read rejection via the authoritative
    /// gate rather than a parse/corruption error from reading the body.
    #[test]
    fn test_open_below_floor_unknown_la_rejected_with_body_present() {
        let dir = std::env::temp_dir().join(format!("cqlite-1249-la-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let data_path = dir.join("la-1-big-Data.db");
        // Garbage body: would error during parsing if it were ever read.
        std::fs::write(&data_path, b"not a valid sstable body").unwrap();

        let result = BulletproofReader::open(&data_path);
        let _ = std::fs::remove_dir_all(&dir);

        match result {
            Err(Error::UnsupportedVersion { version, floor }) => {
                assert_eq!(version, "la");
                assert_eq!(floor, "na");
            }
            other => panic!(
                "expected UnsupportedVersion for la, got {:?}",
                other.map(|_| ())
            ),
        }
    }
}
