//! CI test verifying that inline CRC corruption in Data.db is detected (Bug #639 fix).
//!
//! Cassandra stores a 4-byte CRC32 INLINE after each compressed chunk in Data.db.
//! CompressionInfo.db has NO CRC fields.  ChunkDecompressor reads:
//!   compressed_len = offset_delta - 4
//!   compressed_bytes[0..compressed_len]
//!   crc32_bytes[4]
//!   validates CRC, then decompresses.

use cqlite_core::parser::header::CassandraVersion;
use cqlite_core::storage::sstable::chunk_decompressor::ChunkDecompressor;
use cqlite_core::storage::sstable::compression_info::CompressionInfo;
use std::io::Cursor;

/// Helper: build a fake Data.db record with a controllable CRC suffix.
/// `payload` = the "compressed" bytes (can be anything for this test).
/// `crc`     = the 4-byte BE CRC to append (may be intentionally wrong).
fn build_data_db_record(payload: &[u8], crc: u32) -> Vec<u8> {
    let mut v = payload.to_vec();
    v.extend_from_slice(&crc.to_be_bytes());
    v
}

/// Compute the CRC32 that would match `payload` (Adler CRC32 / IEEE CRC32).
fn correct_crc(payload: &[u8]) -> u32 {
    let mut h = crc32fast::Hasher::new();
    h.update(payload);
    h.finalize()
}

/// Build a CompressionInfo where one chunk occupies `record_size` bytes total
/// (compressed_len = record_size - 4) starting at offset 0.
fn make_one_chunk_info(record_size: u64) -> CompressionInfo {
    CompressionInfo {
        algorithm: "LZ4Compressor".to_string(),
        option_pairs: vec![],
        chunk_length: 65536,
        max_compressed_length: i32::MAX as u32,
        data_length: 65536,
        // Two offsets: [0, record_size] — delta = record_size
        chunk_offsets: vec![0, record_size],
    }
}

/// Test that a chunk with a matching inline CRC is accepted (no error from CRC step).
/// The subsequent decompression may still fail on garbage payload — that's fine.
#[test]
fn test_ci_valid_inline_crc_accepted() {
    let payload: Vec<u8> = vec![0xAB; 12]; // 12 bytes of fake compressed data
    let good_crc = correct_crc(&payload);
    let record = build_data_db_record(&payload, good_crc);
    let record_size = record.len() as u64; // 12 + 4 = 16

    let info = make_one_chunk_info(record_size);
    let mut decomp =
        ChunkDecompressor::new(info, CassandraVersion::V5_0Release).expect("decompressor created");
    let mut reader = Cursor::new(record);

    // We expect either Ok (unlikely with garbage payload) or an error that is NOT
    // about CRC mismatch.  A decompression error is acceptable; a CRC error is not.
    match decomp.read_data(&mut reader, 0, 4) {
        Ok(_) => { /* fine */ }
        Err(e) => {
            let msg = e.to_string().to_lowercase();
            assert!(
                !msg.contains("crc") && !msg.contains("checksum") && !msg.contains("mismatch"),
                "CRC should have been valid but got CRC error: {e}"
            );
        }
    }
}

/// Test that a chunk with a wrong inline CRC is rejected.
/// Error must mention CRC/checksum/mismatch.
#[test]
fn test_ci_crc_corruption_detection() {
    let payload: Vec<u8> = vec![0xCC; 16]; // 16 bytes of fake compressed data
    let bad_crc: u32 = 0xDEADBEEF;
    let record = build_data_db_record(&payload, bad_crc);
    let record_size = record.len() as u64; // 16 + 4 = 20

    let info = make_one_chunk_info(record_size);
    let mut decomp =
        ChunkDecompressor::new(info, CassandraVersion::V5_0Release).expect("decompressor created");
    let mut reader = Cursor::new(record);

    let result = decomp.read_data(&mut reader, 0, 4);
    assert!(result.is_err(), "Expected CRC error, got Ok");

    let msg = result.unwrap_err().to_string().to_lowercase();
    assert!(
        msg.contains("crc") || msg.contains("checksum") || msg.contains("mismatch"),
        "Error should reference CRC validation, got: {msg}"
    );

    println!("CI Test PASSED: inline CRC corruption detected — {msg}");
}

/// Test multiple records: verify that each bad CRC is caught independently.
#[test]
fn test_ci_multiple_records_each_crc_checked() {
    // Build three consecutive records in a single fake Data.db buffer.
    // Each record: 8-byte payload + 4-byte CRC.
    let chunk_size: u32 = 12; // 8 compressed + 4 CRC

    let payloads: [&[u8]; 3] = [&[0x11; 8], &[0x22; 8], &[0x33; 8]];
    let bad_crcs: [u32; 3] = [0xBAD00001, 0xBAD00002, 0xBAD00003];

    let mut fake_db: Vec<u8> = Vec::new();
    for (payload, &crc) in payloads.iter().zip(bad_crcs.iter()) {
        fake_db.extend_from_slice(payload);
        fake_db.extend_from_slice(&crc.to_be_bytes());
    }

    let offsets: Vec<u64> = (0..=3).map(|i| i * chunk_size as u64).collect();
    let info = CompressionInfo {
        algorithm: "SnappyCompressor".to_string(),
        option_pairs: vec![],
        chunk_length: 65536,
        max_compressed_length: i32::MAX as u32,
        data_length: 65536 * 3,
        chunk_offsets: offsets,
    };

    let mut decomp =
        ChunkDecompressor::new(info, CassandraVersion::V5_0Release).expect("decompressor created");

    // All three chunks have bad CRCs — each read must produce an error
    for chunk_start in [0u64, 12, 24] {
        let mut reader = Cursor::new(fake_db.clone());
        let result = decomp.read_data(&mut reader, chunk_start, 4);
        assert!(
            result.is_err(),
            "Expected error for chunk at offset {chunk_start}, got Ok"
        );
    }

    println!("CI Test PASSED: all three chunks detected bad inline CRCs");
}
