//! CRC validation tests for the compression pipeline (updated for Bug #638/#639 fix)
//!
//! Key facts (from Cassandra authority sources):
//!   - CRCs are NOT stored in CompressionInfo.db.  They are 4-byte values
//!     appended INLINE after each compressed chunk in Data.db.
//!   - The new CompressionInfo struct has no `crc32` or `chunk_crcs` fields.
//!   - ChunkDecompressor reads (offset_delta - 4) bytes as compressed data,
//!     then reads 4 bytes as CRC32, validates, and decompresses.

use cqlite_core::storage::sstable::chunk_decompressor::ChunkDecompressor;
use cqlite_core::storage::sstable::compression_info::CompressionInfo;
use std::io::Cursor;

#[cfg(test)]
mod tests {
    use super::*;
    use cqlite_core::parser::CassandraVersion;

    /// Build a minimal CompressionInfo with the given algorithm and 1 chunk.
    fn one_chunk_info(algorithm: &str) -> CompressionInfo {
        CompressionInfo {
            algorithm: algorithm.to_string(),
            option_pairs: vec![],
            chunk_length: 16384,
            max_compressed_length: i32::MAX as u32,
            data_length: 16384,
            chunk_offsets: vec![0],
        }
    }

    /// Test that CompressionInfo can be created without CRC fields (Bug #638).
    #[test]
    fn test_compression_info_no_crc_fields() {
        let info = one_chunk_info("LZ4Compressor");
        // These fields must NOT exist; if they did, this test would fail to compile.
        // Instead we verify the struct has exactly the fields the spec demands.
        assert_eq!(info.algorithm, "LZ4Compressor");
        assert_eq!(info.chunk_length, 16384);
        assert_eq!(info.data_length, 16384);
        assert_eq!(info.chunk_offsets, vec![0u64]);
        assert_eq!(info.option_pairs.len(), 0);
        assert_eq!(info.max_compressed_length, i32::MAX as u32);
    }

    /// Test that validate() succeeds when no CRCs are present (correct behaviour).
    #[test]
    fn test_validate_without_crcs_succeeds() {
        let info = one_chunk_info("SnappyCompressor");
        assert!(info.validate().is_ok());
    }

    /// Test that ChunkDecompressor creation succeeds without CRCs in CompressionInfo.
    #[test]
    fn test_decompressor_creation_no_crcs() {
        for algo in &[
            "LZ4Compressor",
            "SnappyCompressor",
            "ZstdCompressor",
            "DeflateCompressor",
        ] {
            let info = one_chunk_info(algo);
            assert!(
                ChunkDecompressor::new(info, CassandraVersion::V5_0Release).is_ok(),
                "decompressor creation failed for {algo}"
            );
        }
    }

    /// Parsing bytes that are exactly the correct length (no extra CRC at end) must succeed.
    #[test]
    fn test_parse_no_trailing_crc_required() {
        // Hand-craft a minimal valid CompressionInfo.db:
        //   writeUTF("LZ4Compressor")  = [0x00, 0x0d, ...13 bytes...]
        //   option_count = 0           = [0x00, 0x00, 0x00, 0x00]
        //   chunk_length = 16384       = [0x00, 0x00, 0x40, 0x00]
        //   max_compressed_length = INT_MAX = [0x7F, 0xFF, 0xFF, 0xFF]
        //   data_length = 16384        = [0x00,0x00,0x00,0x00,0x00,0x00,0x40,0x00]
        //   chunk_count = 1            = [0x00, 0x00, 0x00, 0x01]
        //   offset[0] = 0             = [0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00]
        let data: Vec<u8> = vec![
            // writeUTF("LZ4Compressor")
            0x00, 0x0d, 0x4c, 0x5a, 0x34, 0x43, 0x6f, 0x6d, 0x70, 0x72, 0x65, 0x73, 0x73, 0x6f,
            0x72, // option_count = 0
            0x00, 0x00, 0x00, 0x00, // chunk_length = 16384
            0x00, 0x00, 0x40, 0x00, // max_compressed_length = INT_MAX
            0x7F, 0xFF, 0xFF, 0xFF, // data_length = 16384
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, // chunk_count = 1
            0x00, 0x00, 0x00, 0x01, // offset[0] = 0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];

        let result = CompressionInfo::parse(&data);
        assert!(result.is_ok(), "parse failed: {:?}", result.err());

        let info = result.unwrap();
        assert_eq!(info.algorithm, "LZ4Compressor");
        assert_eq!(info.chunk_length, 16384);
        assert_eq!(info.max_compressed_length, i32::MAX as u32);
        assert_eq!(info.data_length, 16384);
        assert_eq!(info.chunk_offsets, vec![0u64]);
    }

    /// Test that each algorithm's CompressionInfo passes validate().
    #[test]
    fn test_compression_algorithm_validation() {
        let algorithms = [
            "LZ4Compressor",
            "SnappyCompressor",
            "ZstdCompressor",
            "DeflateCompressor",
        ];

        for algo in &algorithms {
            let info = CompressionInfo {
                algorithm: algo.to_string(),
                option_pairs: vec![],
                chunk_length: 16384,
                max_compressed_length: i32::MAX as u32,
                data_length: 16384,
                chunk_offsets: vec![0],
            };
            assert!(info.validate().is_ok(), "validate() failed for {algo}");
        }
    }

    /// Test chunk size matrix — different chunk sizes must produce valid offsets.
    #[test]
    fn test_chunk_size_matrix() {
        let chunk_sizes: [u32; 3] = [4096, 16384, 65536];
        let algorithms = ["LZ4Compressor", "SnappyCompressor"];

        for algo in &algorithms {
            for &chunk_size in &chunk_sizes {
                let info = CompressionInfo {
                    algorithm: algo.to_string(),
                    option_pairs: vec![],
                    chunk_length: chunk_size,
                    max_compressed_length: i32::MAX as u32,
                    data_length: chunk_size as u64 * 4,
                    chunk_offsets: (0..4).map(|i| i * chunk_size as u64).collect(),
                };
                assert!(info.validate().is_ok());
                assert_eq!(info.chunk_length, chunk_size);
                assert_eq!(info.chunk_offsets.len(), 4);
                assert_eq!(info.chunk_for_offset(0), 0);
                assert_eq!(info.chunk_for_offset(chunk_size as u64), 1);
            }
        }
    }

    /// Verify that corrupt inline CRC bytes in a fake Data.db record cause an error
    /// that references CRC/checksum/mismatch — not a vague decompression guess.
    #[test]
    fn test_corrupt_data_db_inline_crc_rejected() {
        // 8-byte fake "compressed" payload + 4-byte wrong CRC
        let mut fake_data_db: Vec<u8> = vec![0xFF; 8];
        fake_data_db.extend_from_slice(&0xDEADBEEFu32.to_be_bytes());

        // Two offsets so delta = 12 → compressed_len = 12 - 4 = 8
        let info = CompressionInfo {
            algorithm: "LZ4Compressor".to_string(),
            option_pairs: vec![],
            chunk_length: 65536,
            max_compressed_length: i32::MAX as u32,
            data_length: 65536,
            chunk_offsets: vec![0, 12],
        };

        let mut decomp = ChunkDecompressor::new(info, CassandraVersion::V5_0Release).unwrap();
        let mut reader = Cursor::new(fake_data_db);

        let result = decomp.read_data(&mut reader, 0, 4);
        assert!(result.is_err(), "Expected CRC error, got Ok");

        let msg = result.unwrap_err().to_string().to_lowercase();
        assert!(
            msg.contains("crc") || msg.contains("checksum") || msg.contains("mismatch"),
            "Error should reference CRC, got: {msg}"
        );
    }
}
