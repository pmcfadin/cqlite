//! CompressionInfo.db writer - writes compression metadata
//!
//! Generates the CompressionInfo.db component that describes how Data.db chunks
//! are compressed. This file is required for readers to decompress Data.db.
//!
//! # Binary Format (Cassandra NB / 5.0, CompressionMetadata.java:375-392)
//!
//! ```text
//! [u16 BE: name_length]              ← Java writeUTF length prefix
//! [bytes: algorithm_name]            ← "LZ4Compressor", "SnappyCompressor", etc.
//! [u32 BE: option_count]             ← Number of key-value option pairs (usually 0)
//! for each option:
//!     [u16 BE: key_length][key_bytes]
//!     [u16 BE: val_length][val_bytes]
//! [u32 BE: chunk_length]             ← Uncompressed chunk size (default 16384)
//! [u32 BE: max_compressed_length]    ← INT_MAX when minCompressRatio=0 (default)
//! [u64 BE: data_length]              ← Total uncompressed Data.db size
//! [u32 BE: chunk_count]              ← Number of chunks
//! [u64 BE * chunk_count: offsets]    ← Byte offset of each chunk record in Data.db
//! ```
//!
//! **Note on CRCs**: Per-chunk CRC32 checksums are stored INLINE in Data.db after each
//! compressed chunk — they are NOT written to CompressionInfo.db.
//! See: CompressedSequentialWriter.java:192.
//! There is also NO trailing metadata CRC in CompressionInfo.db.
//!
//! References:
//! - Parser: `cqlite-core/src/storage/sstable/compression_info.rs`
//! - Cassandra source: `CompressionMetadata.java:375-392`
//!
//! # Claim boundary — BUILT-BUT-UNWIRED (issue #1406, posture b)
//!
//! CQLite's production write surface (flush + compaction via `SSTableWriter`)
//! emits **uncompressed** SSTables only, and therefore never emits a
//! CompressionInfo.db. This writer and its sibling `CompressedDataWriter` are
//! wired ONLY into read-path fixtures: they let tests synthesize compressed
//! SSTables so the *reader/decompressor* can be exercised. They are NOT wired
//! into any production write path, and there is ZERO Cassandra-side byte-parity
//! coverage for a CQLite-emitted CompressionInfo.db.
//!
//! To keep that boundary honest and fail-closed, any code that would emit a
//! CompressionInfo.db as part of a real (claimed) SSTable must first pass
//! [`CompressionInfoWriter::guard_unsupported_production_write`], which errors
//! for every real compression algorithm rather than emitting a false/partial
//! artifact or making an unearned parity claim. Wiring real compressed writes
//! (posture a) is tracked in issue #1406.

use crate::error::{Error, Result};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

/// Compression algorithm identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionAlgorithm {
    /// LZ4 compression (fast, moderate ratio)
    Lz4,
    /// Snappy compression (very fast, lower ratio)
    Snappy,
    /// Deflate/zlib compression (slower, better ratio)
    Deflate,
    /// Zstd compression (balanced speed/ratio)
    Zstd,
    /// No compression (passthrough)
    None,
}

impl CompressionAlgorithm {
    /// Get the Cassandra algorithm name string
    pub fn cassandra_name(&self) -> &'static str {
        match self {
            CompressionAlgorithm::Lz4 => "LZ4Compressor",
            CompressionAlgorithm::Snappy => "SnappyCompressor",
            CompressionAlgorithm::Deflate => "DeflateCompressor",
            CompressionAlgorithm::Zstd => "ZstdCompressor",
            CompressionAlgorithm::None => "NoopCompressor",
        }
    }

    /// Parse from Cassandra algorithm name
    pub fn from_cassandra_name(name: &str) -> Option<Self> {
        match name {
            "LZ4Compressor" | "org.apache.cassandra.io.compress.LZ4Compressor" => {
                Some(CompressionAlgorithm::Lz4)
            }
            "SnappyCompressor" | "org.apache.cassandra.io.compress.SnappyCompressor" => {
                Some(CompressionAlgorithm::Snappy)
            }
            "DeflateCompressor" | "org.apache.cassandra.io.compress.DeflateCompressor" => {
                Some(CompressionAlgorithm::Deflate)
            }
            "ZstdCompressor" | "org.apache.cassandra.io.compress.ZstdCompressor" => {
                Some(CompressionAlgorithm::Zstd)
            }
            "NoopCompressor" | "org.apache.cassandra.io.compress.NoopCompressor" => {
                Some(CompressionAlgorithm::None)
            }
            _ => None,
        }
    }
}

/// Metadata about compressed Data.db, collected during compression.
///
/// Written to CompressionInfo.db by `CompressionInfoWriter`.
#[derive(Debug, Clone)]
pub struct CompressionMetadata {
    /// Compression algorithm used
    pub algorithm: CompressionAlgorithm,
    /// Uncompressed chunk size in bytes (typically 16384 for Cassandra 5.0)
    pub chunk_length: u32,
    /// Maximum compressed chunk length. When a compressed chunk reaches or exceeds this
    /// size, Cassandra stores the chunk uncompressed instead.
    /// Set to `i32::MAX` (default) when `minCompressRatio=0` (the Cassandra default).
    /// Source: CompressionParams.java:186-189.
    pub max_compressed_length: u32,
    /// Total uncompressed data length (Data.db file, excluding the inline CRC bytes)
    pub data_length: u64,
    /// Byte offset of each compressed-chunk record in Data.db.
    /// Each record is: [compressed_bytes][4-byte inline CRC32].
    /// The delta between consecutive offsets therefore includes the 4-byte CRC.
    pub chunk_offsets: Vec<u64>,
    /// Optional compression parameter key-value pairs (usually empty for default settings)
    pub option_pairs: Vec<(String, String)>,
}

impl CompressionMetadata {
    /// Create new compression metadata with default settings
    ///
    /// Sets `max_compressed_length` to `i32::MAX` (the default when `minCompressRatio=0`).
    pub fn new(algorithm: CompressionAlgorithm, chunk_length: u32) -> Self {
        Self {
            algorithm,
            chunk_length,
            max_compressed_length: i32::MAX as u32,
            data_length: 0,
            chunk_offsets: Vec::new(),
            option_pairs: Vec::new(),
        }
    }

    /// Add a new chunk offset.
    ///
    /// The offset must point to the start of the compressed-chunk record (before the
    /// compressed bytes), not after the inline CRC.
    pub fn add_chunk(&mut self, offset: u64) {
        self.chunk_offsets.push(offset);
    }

    /// Set the total uncompressed data length
    pub fn set_data_length(&mut self, length: u64) {
        self.data_length = length;
    }

    /// Get the number of chunks
    pub fn chunk_count(&self) -> usize {
        self.chunk_offsets.len()
    }
}

/// CompressionInfo.db file writer
///
/// Writes compression metadata to disk in Cassandra's binary format.
/// Authority: CompressionMetadata.java:375-392.
#[derive(Debug)]
pub struct CompressionInfoWriter {
    /// Output file path
    path: PathBuf,
}

impl CompressionInfoWriter {
    /// Create a new CompressionInfo.db writer
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// Fail-closed guard for PRODUCTION CompressionInfo.db emission (issue #1406,
    /// posture b — "guard now, wire compression later").
    ///
    /// CQLite does not have a wired compressed-write path: flush and compaction
    /// emit uncompressed SSTables (no CompressionInfo.db), and no Cassandra-side
    /// byte-parity coverage exists for a CQLite-emitted CompressionInfo.db. Any
    /// code that intends to produce a CompressionInfo.db as part of a real,
    /// parity-claimed SSTable MUST call this first so an unwired compression
    /// request errors clearly instead of silently emitting an uncompressed
    /// SSTable (a false claim) or a partial/unvalidated CompressionInfo.db.
    ///
    /// [`CompressionAlgorithm::None`] (uncompressed) is permitted; every real
    /// compression algorithm returns [`Error::UnsupportedFormat`]. This is the
    /// enforced claim boundary — see the module docs and issue #1406.
    ///
    /// Note: this does NOT restrict [`Self::build_to_vec`] / [`Self::write`],
    /// which read-path fixtures legitimately use to synthesize compressed
    /// SSTables for exercising the decompressing reader.
    pub fn guard_unsupported_production_write(algorithm: CompressionAlgorithm) -> Result<()> {
        match algorithm {
            CompressionAlgorithm::None => Ok(()),
            other => Err(Error::UnsupportedFormat(format!(
                "compressed SSTable writing is not supported: CQLite emits \
                 uncompressed SSTables only (requested {}). The CompressionInfo.db \
                 write path is built but unwired and unvalidated against Cassandra \
                 (see issue #1406).",
                other.cassandra_name()
            ))),
        }
    }

    /// Write compression metadata to file in Cassandra's binary format.
    ///
    /// This writes the exact layout produced by CompressionMetadata.writeHeader():
    ///   writeUTF(name) + option_count + options + chunk_length + max_compressed_length
    ///   + data_length + chunk_count + offsets
    ///
    /// No trailing metadata CRC is written — Cassandra's CompressionInfo.db ends after offsets.
    pub fn write(&self, metadata: &CompressionMetadata) -> Result<()> {
        let file = File::create(&self.path).map_err(|e| {
            Error::Storage(format!(
                "Failed to create CompressionInfo.db at {}: {}",
                self.path.display(),
                e
            ))
        })?;
        let mut writer = BufWriter::new(file);

        let content = self.build_content(metadata)?;
        writer.write_all(&content).map_err(|e| {
            Error::Storage(format!("Failed to write CompressionInfo.db content: {}", e))
        })?;

        writer
            .flush()
            .map_err(|e| Error::Storage(format!("Failed to flush CompressionInfo.db: {}", e)))?;

        Ok(())
    }

    /// Build the binary content matching Cassandra's CompressionMetadata.writeHeader() layout.
    fn build_content(&self, metadata: &CompressionMetadata) -> Result<Vec<u8>> {
        let mut content = Vec::new();

        // 1. writeUTF(algorithm_name): 2-byte BE length + UTF-8 bytes
        let algorithm_name = metadata.algorithm.cassandra_name();
        let name_bytes = algorithm_name.as_bytes();
        if name_bytes.len() > u16::MAX as usize {
            return Err(Error::InvalidInput(format!(
                "Algorithm name too long: {} bytes (max {})",
                name_bytes.len(),
                u16::MAX
            )));
        }
        content.extend_from_slice(&(name_bytes.len() as u16).to_be_bytes());
        content.extend_from_slice(name_bytes);

        // 2. writeInt(option_count)
        let option_count = metadata.option_pairs.len();
        if option_count > u32::MAX as usize {
            return Err(Error::InvalidInput("Too many option pairs".to_string()));
        }
        content.extend_from_slice(&(option_count as u32).to_be_bytes());

        // 3. option key-value pairs (each a writeUTF)
        for (key, value) in &metadata.option_pairs {
            let kb = key.as_bytes();
            let vb = value.as_bytes();
            if kb.len() > u16::MAX as usize || vb.len() > u16::MAX as usize {
                return Err(Error::InvalidInput(format!(
                    "Option key/value too long: key={} bytes, value={} bytes",
                    kb.len(),
                    vb.len()
                )));
            }
            content.extend_from_slice(&(kb.len() as u16).to_be_bytes());
            content.extend_from_slice(kb);
            content.extend_from_slice(&(vb.len() as u16).to_be_bytes());
            content.extend_from_slice(vb);
        }

        // 4. writeInt(chunk_length)
        content.extend_from_slice(&metadata.chunk_length.to_be_bytes());

        // 5. writeInt(max_compressed_length)
        content.extend_from_slice(&metadata.max_compressed_length.to_be_bytes());

        // 6. writeLong(data_length)
        content.extend_from_slice(&metadata.data_length.to_be_bytes());

        // 7. writeInt(chunk_count)
        if metadata.chunk_offsets.len() > u32::MAX as usize {
            return Err(Error::InvalidInput(format!(
                "Too many chunks: {} (max {})",
                metadata.chunk_offsets.len(),
                u32::MAX
            )));
        }
        content.extend_from_slice(&(metadata.chunk_offsets.len() as u32).to_be_bytes());

        // 8. chunk_count × writeLong(chunk_offset)
        for offset in &metadata.chunk_offsets {
            content.extend_from_slice(&offset.to_be_bytes());
        }

        // NOTE: No trailing metadata CRC — Cassandra's CompressionInfo.db ends after offsets.
        // Per-chunk CRCs are stored INLINE in Data.db, not here.

        Ok(content)
    }

    /// Build content to a buffer instead of writing to file (for testing/round-trip checks)
    pub fn build_to_vec(&self, metadata: &CompressionMetadata) -> Result<Vec<u8>> {
        self.build_content(metadata)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_compression_algorithm_names() {
        assert_eq!(CompressionAlgorithm::Lz4.cassandra_name(), "LZ4Compressor");
        assert_eq!(
            CompressionAlgorithm::Snappy.cassandra_name(),
            "SnappyCompressor"
        );
        assert_eq!(
            CompressionAlgorithm::Deflate.cassandra_name(),
            "DeflateCompressor"
        );
        assert_eq!(
            CompressionAlgorithm::Zstd.cassandra_name(),
            "ZstdCompressor"
        );
        assert_eq!(
            CompressionAlgorithm::None.cassandra_name(),
            "NoopCompressor"
        );
    }

    #[test]
    fn test_compression_algorithm_from_name() {
        assert_eq!(
            CompressionAlgorithm::from_cassandra_name("LZ4Compressor"),
            Some(CompressionAlgorithm::Lz4)
        );
        assert_eq!(
            CompressionAlgorithm::from_cassandra_name(
                "org.apache.cassandra.io.compress.LZ4Compressor"
            ),
            Some(CompressionAlgorithm::Lz4)
        );
        assert_eq!(
            CompressionAlgorithm::from_cassandra_name("UnknownCompressor"),
            None
        );
    }

    #[test]
    fn test_compression_metadata_new() {
        let metadata = CompressionMetadata::new(CompressionAlgorithm::Lz4, 16384);
        assert_eq!(metadata.algorithm, CompressionAlgorithm::Lz4);
        assert_eq!(metadata.chunk_length, 16384);
        assert_eq!(metadata.max_compressed_length, i32::MAX as u32);
        assert_eq!(metadata.data_length, 0);
        assert!(metadata.chunk_offsets.is_empty());
        assert!(metadata.option_pairs.is_empty());
    }

    #[test]
    fn test_compression_metadata_add_chunk() {
        let mut metadata = CompressionMetadata::new(CompressionAlgorithm::Lz4, 16384);

        metadata.add_chunk(0);
        metadata.add_chunk(8200); // 8196 compressed bytes + 4 CRC

        assert_eq!(metadata.chunk_count(), 2);
        assert_eq!(metadata.chunk_offsets, vec![0, 8200]);
    }

    /// Regression test for Bug #638 writer side:
    /// Verify build_content() matches the exact binary layout Cassandra expects.
    /// No trailing CRC, no chunk CRC section — just the 8-field header + offsets.
    #[test]
    fn test_build_content_matches_cassandra_format() {
        let mut metadata = CompressionMetadata::new(CompressionAlgorithm::Lz4, 16384);
        metadata.add_chunk(0);
        metadata.add_chunk(8200);
        metadata.set_data_length(32768);

        let writer = CompressionInfoWriter::new(PathBuf::from("/tmp/test"));
        let content = writer.build_content(&metadata).unwrap();

        // Expected layout:
        //   [0..2]   u16 BE name_len = 13 (LZ4Compressor)
        //   [2..15]  "LZ4Compressor"
        //   [15..19] u32 BE option_count = 0
        //   [19..23] u32 BE chunk_length = 16384
        //   [23..27] u32 BE max_compressed_length = INT_MAX
        //   [27..35] u64 BE data_length = 32768
        //   [35..39] u32 BE chunk_count = 2
        //   [39..47] u64 BE offset[0] = 0
        //   [47..55] u64 BE offset[1] = 8200
        //   Total: 55 bytes — NO trailing CRC section

        assert_eq!(
            content.len(),
            55,
            "Bug #638 writer: content must be exactly 55 bytes (no trailing CRC)"
        );

        // name_len = 13
        assert_eq!(&content[0..2], &[0x00, 0x0D]);
        // algorithm name
        assert_eq!(&content[2..15], b"LZ4Compressor");
        // option_count = 0
        assert_eq!(&content[15..19], &[0x00, 0x00, 0x00, 0x00]);
        // chunk_length = 16384 = 0x00004000
        assert_eq!(&content[19..23], &[0x00, 0x00, 0x40, 0x00]);
        // max_compressed_length = i32::MAX = 0x7FFFFFFF
        assert_eq!(&content[23..27], &[0x7F, 0xFF, 0xFF, 0xFF]);
        // data_length = 32768 = 0x0000000000008000
        assert_eq!(
            &content[27..35],
            &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x00]
        );
        // chunk_count = 2
        assert_eq!(&content[35..39], &[0x00, 0x00, 0x00, 0x02]);
        // offset[0] = 0
        assert_eq!(
            &content[39..47],
            &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
        );
        // offset[1] = 8200 = 0x0000000000002008
        assert_eq!(
            &content[47..55],
            &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x08]
        );
    }

    #[test]
    fn test_build_content_with_options() {
        let mut metadata = CompressionMetadata::new(CompressionAlgorithm::Lz4, 16384);
        metadata.option_pairs = vec![("compression_level".to_string(), "9".to_string())];
        metadata.add_chunk(0);
        metadata.set_data_length(16384);

        let writer = CompressionInfoWriter::new(PathBuf::from("/tmp/test"));
        let content = writer.build_content(&metadata).unwrap();

        // option_count at offset 15 should be 1
        let option_count = u32::from_be_bytes([content[15], content[16], content[17], content[18]]);
        assert_eq!(option_count, 1, "option_count must be 1");

        // Parse the option key at offset 19
        let key_len = u16::from_be_bytes([content[19], content[20]]) as usize;
        let key = std::str::from_utf8(&content[21..21 + key_len]).unwrap();
        assert_eq!(key, "compression_level");
    }

    /// Verify round-trip: writer produces output that the reader can parse back.
    #[test]
    fn test_writer_reader_round_trip() {
        use crate::storage::sstable::compression_info::CompressionInfo;

        let mut metadata = CompressionMetadata::new(CompressionAlgorithm::Snappy, 16384);
        metadata.add_chunk(0);
        metadata.add_chunk(9876);
        metadata.set_data_length(32000);

        let writer = CompressionInfoWriter::new(PathBuf::from("/tmp/test"));
        let bytes = writer.build_to_vec(&metadata).unwrap();

        let info = CompressionInfo::parse(&bytes)
            .expect("Writer output must be parseable by CompressionInfo::parse");

        assert_eq!(info.algorithm, "SnappyCompressor");
        assert_eq!(info.chunk_length, 16384);
        assert_eq!(info.max_compressed_length, i32::MAX as u32);
        assert_eq!(info.data_length, 32000);
        assert_eq!(info.chunk_offsets, vec![0, 9876]);
        assert!(info.option_pairs.is_empty());
    }

    /// Issue #1406 (posture b): the production-emission guard fails closed for
    /// every real compression algorithm and permits only the uncompressed case.
    #[test]
    fn test_guard_unsupported_production_write_fails_closed() {
        // Uncompressed is the only permitted production write.
        assert!(
            CompressionInfoWriter::guard_unsupported_production_write(CompressionAlgorithm::None)
                .is_ok(),
            "uncompressed (None) production writes must be permitted"
        );

        for algo in [
            CompressionAlgorithm::Lz4,
            CompressionAlgorithm::Snappy,
            CompressionAlgorithm::Deflate,
            CompressionAlgorithm::Zstd,
        ] {
            let err = CompressionInfoWriter::guard_unsupported_production_write(algo)
                .expect_err("real compression must be rejected by the fail-closed guard");
            match err {
                Error::UnsupportedFormat(msg) => {
                    assert!(
                        msg.contains(algo.cassandra_name()),
                        "error must name the requested algorithm, got: {msg}"
                    );
                    assert!(
                        msg.contains("1406"),
                        "error must cite the claim-boundary issue, got: {msg}"
                    );
                }
                other => panic!("expected UnsupportedFormat, got {other:?}"),
            }
        }
    }

    #[test]
    fn test_compression_info_writer_write_file() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("nb-1-big-CompressionInfo.db");

        let mut metadata = CompressionMetadata::new(CompressionAlgorithm::Lz4, 16384);
        metadata.add_chunk(0);
        metadata.set_data_length(16384);

        let writer = CompressionInfoWriter::new(path.clone());
        writer.write(&metadata).unwrap();

        // Verify file was created
        assert!(path.exists());

        // Verify the file is parseable
        let bytes = std::fs::read(&path).unwrap();
        let info = crate::storage::sstable::compression_info::CompressionInfo::parse(&bytes)
            .expect("Written file must be parseable");
        assert_eq!(info.algorithm, "LZ4Compressor");
        assert_eq!(info.chunk_length, 16384);
    }
}
