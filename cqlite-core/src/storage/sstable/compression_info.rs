//! CompressionInfo.db file parser for SSTable compression metadata
//!
//! This module provides deterministic parsing of Cassandra's CompressionInfo.db files
//! which contain the metadata needed to decompress chunks from compressed Data.db files.
//!
//! ## Binary Format (Cassandra NB / 5.0, CompressionMetadata.java:375-392)
//!
//! ```text
//! writeUTF(compressor_simple_name)     // 2-byte BE length + UTF-8 bytes
//! writeInt(option_count)               // 4 bytes BE
//! for each option:
//!     writeUTF(key)                    // 2-byte BE length + UTF-8 bytes
//!     writeUTF(value)                  // 2-byte BE length + UTF-8 bytes
//! writeInt(chunk_length)               // 4 bytes BE — uncompressed chunk size
//! writeInt(max_compressed_length)      // 4 bytes BE — present when version >= "na" (all 5.0 files)
//! writeLong(data_length)               // 8 bytes BE — total uncompressed data length
//! writeInt(chunk_count)                // 4 bytes BE
//! for each chunk:
//!     writeLong(chunk_offset)          // 8 bytes BE — byte offset in Data.db
//! ```
//!
//! ## Note on CRCs
//!
//! Per-chunk CRC32 checksums are stored INLINE in Data.db, not in CompressionInfo.db.
//! Each compressed chunk in Data.db is followed by a 4-byte little-endian CRC32 of the
//! compressed bytes. See: CompressedSequentialWriter.java:192.
//! There is NO trailing metadata CRC in CompressionInfo.db.

use crate::{Error, Result};
use std::io::{Cursor, Read};

/// Cassandra compressor simple names that CQLite can decompress.
///
/// These are the only `org.apache.cassandra.io.compress.ICompressor` implementations
/// CQLite supports. The decompression paths in `chunk_decompressor.rs` and
/// `chunked_data_reader.rs` are keyed on exactly these names. Any other name in a
/// `CompressionInfo.db` is rejected fail-fast at metadata-parse time rather than
/// silently treated as uncompressed (issue #1001). No content-based guessing is ever
/// performed (no-heuristics mandate, issue #28).
pub const SUPPORTED_COMPRESSOR_NAMES: &[&str] = &[
    "LZ4Compressor",
    "SnappyCompressor",
    "DeflateCompressor",
    "ZstdCompressor",
    // NoopCompressor is the explicit "no compression" marker; chunks are stored raw.
    "NoopCompressor",
];

/// Returns true if `name` is a Cassandra compressor simple name CQLite can honor.
///
/// Accepts both the simple name (e.g. `"LZ4Compressor"`) and the fully-qualified
/// class name (e.g. `"org.apache.cassandra.io.compress.LZ4Compressor"`) that Cassandra
/// may write into `CompressionInfo.db`.
pub fn is_supported_compressor_name(name: &str) -> bool {
    let simple = name.rsplit('.').next().unwrap_or(name);
    SUPPORTED_COMPRESSOR_NAMES.contains(&simple)
}

/// CompressionInfo.db file content parsed from binary format
#[derive(Debug, Clone)]
pub struct CompressionInfo {
    /// Compression algorithm simple name (e.g., "LZ4Compressor", "SnappyCompressor")
    pub algorithm: String,
    /// Optional compression parameters from CompressionInfo.db
    /// Key-value pairs as written by CompressionMetadata.writeHeader()
    pub option_pairs: Vec<(String, String)>,
    /// Size of uncompressed data chunks (bytes)
    pub chunk_length: u32,
    /// Maximum compressed chunk length; if a compressed chunk reaches this size, the chunk
    /// was stored uncompressed in Data.db instead. Equals i32::MAX when minCompressRatio=0
    /// (the default). Source: CompressionParams.java:186-189.
    pub max_compressed_length: u32,
    /// Total uncompressed data length (bytes)
    pub data_length: u64,
    /// List of compressed chunk offsets in Data.db file
    ///
    /// Each offset points to the start of a compressed-chunk record. The record consists of:
    ///   [compressed_bytes][4-byte CRC32 of compressed_bytes]
    /// The delta between consecutive offsets therefore includes the trailing 4-byte CRC.
    /// See: CompressedSequentialWriter.java:203 `chunkOffset += compressedLength + 4`
    pub chunk_offsets: Vec<u64>,
    // NOTE: per-chunk CRC32 values are NOT stored in CompressionInfo.db.
    // They are written inline in Data.db after each compressed chunk.
    // CompressedSequentialWriter.java:192: crcMetadata.appendDirect(toWrite, true)
}

/// Read a Java-style writeUTF string: 2-byte BE length followed by UTF-8 bytes
fn read_utf(cursor: &mut Cursor<&[u8]>) -> Result<String> {
    let mut len_bytes = [0u8; 2];
    cursor
        .read_exact(&mut len_bytes)
        .map_err(|e| Error::InvalidFormat(format!("Failed to read writeUTF length: {}", e)))?;
    let len = u16::from_be_bytes(len_bytes) as usize;

    let mut string_bytes = vec![0u8; len];
    cursor.read_exact(&mut string_bytes).map_err(|e| {
        Error::InvalidFormat(format!(
            "Failed to read writeUTF bytes (len={}): {}",
            len, e
        ))
    })?;

    String::from_utf8(string_bytes)
        .map_err(|e| Error::InvalidFormat(format!("Invalid UTF-8 in writeUTF string: {}", e)))
}

/// Read a 4-byte big-endian u32
fn read_u32(cursor: &mut Cursor<&[u8]>, field: &str) -> Result<u32> {
    let mut bytes = [0u8; 4];
    cursor
        .read_exact(&mut bytes)
        .map_err(|e| Error::InvalidFormat(format!("Failed to read {} (u32 BE): {}", field, e)))?;
    Ok(u32::from_be_bytes(bytes))
}

/// Read an 8-byte big-endian u64
fn read_u64(cursor: &mut Cursor<&[u8]>, field: &str) -> Result<u64> {
    let mut bytes = [0u8; 8];
    cursor
        .read_exact(&mut bytes)
        .map_err(|e| Error::InvalidFormat(format!("Failed to read {} (u64 BE): {}", field, e)))?;
    Ok(u64::from_be_bytes(bytes))
}

impl CompressionInfo {
    /// Parse CompressionInfo.db file from binary data.
    ///
    /// Implements the deterministic layout from CompressionMetadata.java:375-392.
    /// No heuristics — every field is read at its authoritative position.
    pub fn parse(data: &[u8]) -> Result<Self> {
        if data.is_empty() {
            return Err(Error::InvalidFormat(
                "Empty compression info data".to_string(),
            ));
        }

        let mut cursor = Cursor::new(data);

        // 1. writeUTF(compressor simple name)
        let algorithm = read_utf(&mut cursor)?;
        if algorithm.is_empty() {
            return Err(Error::InvalidFormat(
                "Compressor simple name is empty".to_string(),
            ));
        }

        // Fail-fast on unknown/unsupported compressors (issue #1001). A CompressionInfo.db
        // that names a compressor CQLite cannot decompress must error here, at metadata-parse
        // time, BEFORE any Data.db chunk is read — never silently fall back to uncompressed.
        // No content-based guessing is performed (no-heuristics mandate, issue #28).
        if !is_supported_compressor_name(&algorithm) {
            return Err(Error::UnsupportedFormat(format!(
                "Unsupported compression algorithm '{}' in CompressionInfo.db. \
                 CQLite only supports: {}. Cannot decompress this SSTable.",
                algorithm,
                SUPPORTED_COMPRESSOR_NAMES.join(", ")
            )));
        }

        // 2. writeInt(option_count)
        let option_count = read_u32(&mut cursor, "option_count")?;
        if option_count > 256 {
            return Err(Error::InvalidFormat(format!(
                "Unreasonably large option_count: {} (max 256)",
                option_count
            )));
        }

        // 3. option_count × (writeUTF key + writeUTF value)
        let mut option_pairs = Vec::with_capacity(option_count as usize);
        for i in 0..option_count {
            let key = read_utf(&mut cursor).map_err(|e| {
                Error::InvalidFormat(format!("Failed to read option key {}: {}", i, e))
            })?;
            let value = read_utf(&mut cursor).map_err(|e| {
                Error::InvalidFormat(format!("Failed to read option value {}: {}", i, e))
            })?;
            option_pairs.push((key, value));
        }

        // 4. writeInt(chunk_length)
        let chunk_length = read_u32(&mut cursor, "chunk_length")?;
        if chunk_length == 0 {
            return Err(Error::InvalidFormat(
                "chunk_length cannot be zero".to_string(),
            ));
        }
        if chunk_length > 256 * 1024 * 1024 {
            return Err(Error::InvalidFormat(format!(
                "chunk_length too large: {} bytes (max 256 MiB)",
                chunk_length
            )));
        }

        // 5. writeInt(max_compressed_length) — present for all Cassandra 5.0 (version >= "na") files
        let max_compressed_length = read_u32(&mut cursor, "max_compressed_length")?;

        // 6. writeLong(data_length)
        let data_length = read_u64(&mut cursor, "data_length")?;

        // 7. writeInt(chunk_count)
        let chunk_count = read_u32(&mut cursor, "chunk_count")? as usize;
        if chunk_count == 0 {
            return Err(Error::InvalidFormat(
                "chunk_count cannot be zero".to_string(),
            ));
        }
        if chunk_count > 1_000_000 {
            return Err(Error::InvalidFormat(format!(
                "chunk_count too large: {} (max 1,000,000)",
                chunk_count
            )));
        }

        // 8. chunk_count × writeLong(chunk_offset)
        let mut chunk_offsets = Vec::with_capacity(chunk_count);
        for i in 0..chunk_count {
            let offset = read_u64(&mut cursor, &format!("chunk_offset[{}]", i))?;
            chunk_offsets.push(offset);
        }

        let info = CompressionInfo {
            algorithm,
            option_pairs,
            chunk_length,
            max_compressed_length,
            data_length,
            chunk_offsets,
        };

        info.validate()?;
        Ok(info)
    }

    /// Resolve the compression algorithm to the runtime `CompressionAlgorithm` enum,
    /// failing fast on any name CQLite cannot decompress (issue #1001).
    ///
    /// `parse()` already rejects unknown names, so for any `CompressionInfo` produced by
    /// `parse()` this is infallible in practice; the `Result` guards `CompressionInfo`
    /// values constructed directly (e.g. in tests) and keeps the no-fallback contract
    /// at the point of use.
    pub fn algorithm_enum(
        &self,
    ) -> Result<crate::storage::sstable::compression::CompressionAlgorithm> {
        crate::storage::sstable::compression::CompressionAlgorithm::parse(&self.algorithm)
    }

    /// Get the chunk index for a given offset in the uncompressed data
    pub fn chunk_for_offset(&self, offset: u64) -> usize {
        (offset / self.chunk_length as u64) as usize
    }

    /// Get the offset within a chunk for a given global offset
    pub fn offset_within_chunk(&self, offset: u64) -> u64 {
        offset % self.chunk_length as u64
    }

    /// Get the compressed chunk offset for a given chunk index
    pub fn compressed_chunk_offset(&self, chunk_index: usize) -> Option<u64> {
        self.chunk_offsets.get(chunk_index).copied()
    }

    /// Get the size of a compressed-chunk record (delta between consecutive offsets or
    /// end-of-file), INCLUDING the 4-byte trailing CRC appended inline in Data.db.
    ///
    /// To get the actual compressed payload size, subtract 4 from the returned value.
    /// See: CompressedSequentialWriter.java:203 `chunkOffset += compressedLength + 4`
    pub fn compressed_chunk_size(
        &self,
        chunk_index: usize,
        total_compressed_size: u64,
    ) -> Option<u64> {
        let start_offset = self.compressed_chunk_offset(chunk_index)?;

        if chunk_index + 1 < self.chunk_offsets.len() {
            let next_offset = self.chunk_offsets[chunk_index + 1];
            Some(next_offset - start_offset)
        } else {
            Some(total_compressed_size - start_offset)
        }
    }

    /// Validate the compression info structure
    pub fn validate(&self) -> Result<()> {
        if self.algorithm.is_empty() {
            return Err(Error::InvalidFormat(
                "Empty compression algorithm".to_string(),
            ));
        }
        if self.chunk_length == 0 {
            return Err(Error::InvalidFormat("Zero chunk length".to_string()));
        }
        if self.chunk_offsets.is_empty() {
            return Err(Error::InvalidFormat("No chunk offsets".to_string()));
        }
        // Offsets must be strictly ascending
        for i in 1..self.chunk_offsets.len() {
            if self.chunk_offsets[i] <= self.chunk_offsets[i - 1] {
                return Err(Error::InvalidFormat(format!(
                    "Chunk offsets not in ascending order: offsets[{}]={} <= offsets[{}]={}",
                    i,
                    self.chunk_offsets[i],
                    i - 1,
                    self.chunk_offsets[i - 1]
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal valid Cassandra CompressionInfo.db blob with no options.
    fn make_compression_info_blob(
        algorithm: &str,
        options: &[(&str, &str)],
        chunk_length: u32,
        max_compressed_length: u32,
        data_length: u64,
        offsets: &[u64],
    ) -> Vec<u8> {
        let mut data = Vec::new();

        // writeUTF(algorithm)
        let name_bytes = algorithm.as_bytes();
        data.extend_from_slice(&(name_bytes.len() as u16).to_be_bytes());
        data.extend_from_slice(name_bytes);

        // writeInt(option_count)
        data.extend_from_slice(&(options.len() as u32).to_be_bytes());

        // option key-value pairs
        for (k, v) in options {
            let kb = k.as_bytes();
            data.extend_from_slice(&(kb.len() as u16).to_be_bytes());
            data.extend_from_slice(kb);
            let vb = v.as_bytes();
            data.extend_from_slice(&(vb.len() as u16).to_be_bytes());
            data.extend_from_slice(vb);
        }

        // writeInt(chunk_length)
        data.extend_from_slice(&chunk_length.to_be_bytes());

        // writeInt(max_compressed_length)
        data.extend_from_slice(&max_compressed_length.to_be_bytes());

        // writeLong(data_length)
        data.extend_from_slice(&data_length.to_be_bytes());

        // writeInt(chunk_count)
        data.extend_from_slice(&(offsets.len() as u32).to_be_bytes());

        // chunk offsets
        for &off in offsets {
            data.extend_from_slice(&off.to_be_bytes());
        }

        data
    }

    #[test]
    fn test_parse_no_options() {
        let blob = make_compression_info_blob(
            "LZ4Compressor",
            &[],
            16384,
            i32::MAX as u32,
            32768,
            &[0, 8200],
        );

        let info = CompressionInfo::parse(&blob).expect("parse should succeed");
        assert_eq!(info.algorithm, "LZ4Compressor");
        assert!(info.option_pairs.is_empty());
        assert_eq!(info.chunk_length, 16384);
        assert_eq!(info.max_compressed_length, i32::MAX as u32);
        assert_eq!(info.data_length, 32768);
        assert_eq!(info.chunk_offsets, vec![0, 8200]);
    }

    #[test]
    fn test_parse_with_options() {
        // Regression test for Bug #638: old heuristic parser skipped option_count bytes
        // as "padding" and misread chunk_length when options were present.
        let blob = make_compression_info_blob(
            "LZ4Compressor",
            &[("compression_level", "9")],
            16384,
            i32::MAX as u32,
            16384,
            &[0],
        );

        let info = CompressionInfo::parse(&blob).expect(
            "Bug #638 repro: parser must handle option_count > 0 deterministically, \
             not skip 4 bytes as padding",
        );
        assert_eq!(info.algorithm, "LZ4Compressor");
        assert_eq!(info.option_pairs.len(), 1);
        assert_eq!(info.option_pairs[0].0, "compression_level");
        assert_eq!(info.option_pairs[0].1, "9");
        assert_eq!(info.chunk_length, 16384);
        assert_eq!(info.max_compressed_length, i32::MAX as u32);
    }

    #[test]
    fn test_parse_exposes_max_compressed_length() {
        // Regression test for Bug #638: old struct had no max_compressed_length field,
        // making the incompressible-chunk fallback impossible to implement.
        let blob = make_compression_info_blob(
            "SnappyCompressor",
            &[],
            16384,
            i32::MAX as u32,
            16384,
            &[0],
        );

        let info = CompressionInfo::parse(&blob).expect("parse should succeed");
        // max_compressed_length must be accessible on the parsed struct
        assert_eq!(
            info.max_compressed_length,
            i32::MAX as u32,
            "Bug #638: max_compressed_length field must be exposed for incompressible-chunk fallback"
        );
    }

    #[test]
    fn test_parse_real_snappy_fixture() {
        // Parse a real CompressionInfo.db from test fixtures and verify sensible values.
        // This fixture has 0 options (SnappyCompressor with default settings).
        let fixture_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("test-data/datasets/sstables/test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-CompressionInfo.db");

        if !fixture_path.exists() {
            println!("Skipping real-fixture test: {}", fixture_path.display());
            return;
        }

        let data = std::fs::read(&fixture_path).expect("read fixture");
        let info = CompressionInfo::parse(&data).expect("parse real CompressionInfo.db");

        assert_eq!(info.algorithm, "SnappyCompressor");
        assert_eq!(info.chunk_length, 16384);
        assert_eq!(info.max_compressed_length, i32::MAX as u32);
        assert!(!info.chunk_offsets.is_empty());
        // Offsets must be strictly increasing
        for i in 1..info.chunk_offsets.len() {
            assert!(
                info.chunk_offsets[i] > info.chunk_offsets[i - 1],
                "offsets must be increasing"
            );
        }
        // No CRC bytes in CompressionInfo.db: file ends exactly after offsets
        let expected_size: usize = 2
            + info.algorithm.len()
            + 4 // option_count
            + 4 // chunk_length
            + 4 // max_compressed_length
            + 8 // data_length
            + 4 // chunk_count
            + info.chunk_offsets.len() * 8;
        assert_eq!(
            data.len(),
            expected_size,
            "CompressionInfo.db must end immediately after offsets — no CRC bytes appended"
        );
    }

    #[test]
    fn test_chunk_calculations() {
        let info = CompressionInfo {
            algorithm: "LZ4Compressor".to_string(),
            option_pairs: vec![],
            chunk_length: 16384,
            max_compressed_length: i32::MAX as u32,
            data_length: 32768,
            chunk_offsets: vec![0, 8192],
        };

        assert_eq!(info.chunk_for_offset(0), 0);
        assert_eq!(info.chunk_for_offset(16384), 1);
        assert_eq!(info.offset_within_chunk(100), 100);
        assert_eq!(info.offset_within_chunk(16484), 100);

        assert_eq!(info.compressed_chunk_offset(0), Some(0));
        assert_eq!(info.compressed_chunk_offset(1), Some(8192));

        assert_eq!(info.compressed_chunk_size(0, 20000), Some(8192));
        assert_eq!(info.compressed_chunk_size(1, 20000), Some(11808));
    }

    #[test]
    fn test_validation() {
        let valid_info = CompressionInfo {
            algorithm: "LZ4Compressor".to_string(),
            option_pairs: vec![],
            chunk_length: 16384,
            max_compressed_length: i32::MAX as u32,
            data_length: 32768,
            chunk_offsets: vec![0, 8192],
        };
        assert!(valid_info.validate().is_ok());

        // Empty algorithm
        let invalid = CompressionInfo {
            algorithm: "".to_string(),
            option_pairs: vec![],
            chunk_length: 16384,
            max_compressed_length: i32::MAX as u32,
            data_length: 0,
            chunk_offsets: vec![0],
        };
        assert!(invalid.validate().is_err());

        // Non-ascending offsets
        let invalid2 = CompressionInfo {
            algorithm: "LZ4Compressor".to_string(),
            option_pairs: vec![],
            chunk_length: 16384,
            max_compressed_length: i32::MAX as u32,
            data_length: 32768,
            chunk_offsets: vec![8192, 0],
        };
        assert!(invalid2.validate().is_err());
    }
}
