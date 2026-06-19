//! End-to-end integration test for Issue #755: BTI trie point lookup
//!
//! # What this proves
//!
//! The BTI Partitions.db trie lookup (`lookup_raw_key_in_bti_partitions_db`)
//! resolves the exact Data.db byte offset for each UUID partition **before**
//! reading any Data.db bytes.  This is point-lookup via trie — not sequential
//! scan.
//!
//! # Validation chain
//!
//! 1. Open `da-2-bti-Partitions.db` from the real `test_da/simple_table` fixture.
//! 2. Call `lookup_raw_key_in_bti_partitions_db` with raw UUID bytes for all
//!    three partitions.
//! 3. Assert the returned `BtiPartitionLocation::DataOffset` values match the
//!    "position" fields in the JSONL sstabledump golden:
//!    - `[0x22]*16` → offset 0
//!    - `[0x11]*16` → offset 63
//!    - `[0x33]*16` → offset 125
//! 4. Open `da-2-bti-CompressionInfo.db` and `da-2-bti-Data.db`.
//! 5. Decompress the LZ4 chunk using `ChunkReader` + `Compression::decompress`.
//! 6. At each resolved offset in the decompressed buffer, parse the partition
//!    header and read the 16-byte UUID key.
//! 7. Assert the UUID bytes match the partition key we originally looked up
//!    (proves trie resolved the right offset, not a lucky scan result).
//!
//! # Architecture note
//!
//! This test exercises the low-level trie + compression primitives directly.
//! As of issue #831, `SSTableReader::open` now SUCCEEDS for BTI Data.db and wires
//! these same primitives into the public open + get path (see
//! `issue_831_bti_reader_point_lookup.rs` for the end-to-end reader test).
//!
//! # Test data requirement
//!
//! The `CQLITE_DATASETS_ROOT` environment variable must point to the
//! `test-data/datasets` directory and the `test_da` binary SSTables must have
//! been fetched:
//!
//! ```bash
//! bash test-data/scripts/fetch-datasets.sh
//! ```
//!
//! Tests skip gracefully when binary files are absent.

use cqlite_core::storage::sstable::bti::{
    lookup_raw_key_in_bti_partitions_db, BtiPartitionLocation,
};
use cqlite_core::storage::sstable::chunk_reader::ChunkReader;
use cqlite_core::storage::sstable::compression::{Compression, CompressionAlgorithm};
use cqlite_core::storage::sstable::compression_info::CompressionInfo;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// The three test UUID partitions in the `test_da/simple_table` fixture.
///
/// Golden "position" values come from `da-2-bti-Data.db.jsonl` (sstabledump
/// output).
struct Partition {
    /// Raw 16-byte UUID (all bytes identical for these test UUIDs)
    uuid_byte: u8,
    /// Expected Data.db uncompressed byte offset (from JSONL golden)
    expected_offset: u64,
    /// Canonical display UUID string (for diagnostics)
    label: &'static str,
}

const TEST_PARTITIONS: &[Partition] = &[
    Partition {
        uuid_byte: 0x22,
        expected_offset: 0,
        label: "22222222-2222-2222-2222-222222222222",
    },
    Partition {
        uuid_byte: 0x11,
        expected_offset: 63,
        label: "11111111-1111-1111-1111-111111111111",
    },
    Partition {
        uuid_byte: 0x33,
        expected_offset: 125,
        label: "33333333-3333-3333-3333-333333333333",
    },
];

/// Return the path to the `test_da/simple_table-*` SSTable directory, or `None`
/// if the binary Partitions.db sentinel is absent.
fn da_simple_table_dir() -> Option<PathBuf> {
    let root = std::env::var("CQLITE_DATASETS_ROOT").ok()?;
    let base = PathBuf::from(root).join("sstables").join("test_da");

    let table_dir = std::fs::read_dir(&base)
        .ok()?
        .flatten()
        .find(|e| e.file_name().to_string_lossy().starts_with("simple_table-"))
        .map(|e| e.path())?;

    // Guard: binary files must be present (CI may have goldens-only checkout)
    let has_partitions_db = std::fs::read_dir(&table_dir).ok()?.flatten().any(|e| {
        let s = e.file_name();
        let s = s.to_string_lossy();
        s.starts_with("da-") && s.ends_with("-bti-Partitions.db")
    });

    if !has_partitions_db {
        eprintln!(
            "SKIP: da-*-bti-Partitions.db not present in {:?}. \
             Run `bash test-data/scripts/fetch-datasets.sh` to download.",
            table_dir
        );
        return None;
    }

    Some(table_dir)
}

/// Find the first file in `dir` whose name matches `starts_with` + `ends_with`.
fn find_file(dir: &PathBuf, starts_with: &str, ends_with: &str) -> Option<PathBuf> {
    std::fs::read_dir(dir)
        .ok()?
        .flatten()
        .find(|e| {
            let n = e.file_name();
            let s = n.to_string_lossy();
            s.starts_with(starts_with) && s.ends_with(ends_with)
        })
        .map(|e| e.path())
}

// ---------------------------------------------------------------------------
// Core test: trie resolves correct Data.db offsets
// ---------------------------------------------------------------------------

/// Phase 1: Verify that the BTI trie lookup returns the golden Data.db offsets.
///
/// The resolved offsets come ENTIRELY from Partitions.db — Data.db is NOT
/// opened yet in this phase.  That is the definition of "trie, not scan".
#[test]
fn bti_trie_resolves_data_db_offsets() {
    let Some(dir) = da_simple_table_dir() else {
        eprintln!("SKIP: test_da/simple_table binary SSTables not available");
        return;
    };

    let partitions_db = match find_file(&dir, "da-", "-bti-Partitions.db") {
        Some(p) => p,
        None => {
            eprintln!("SKIP: da-*-bti-Partitions.db not found in {:?}", dir);
            return;
        }
    };

    let file = File::open(&partitions_db)
        .unwrap_or_else(|e| panic!("Failed to open {:?}: {}", partitions_db, e));
    let mut reader = BufReader::new(file);

    for p in TEST_PARTITIONS {
        let raw_uuid: [u8; 16] = [p.uuid_byte; 16];

        let result = lookup_raw_key_in_bti_partitions_db(&mut reader, &raw_uuid)
            .unwrap_or_else(|e| panic!("Trie lookup failed for UUID {}: {}", p.label, e));

        match result {
            Some(BtiPartitionLocation::DataOffset(offset)) => {
                assert_eq!(
                    offset, p.expected_offset,
                    "UUID {} trie lookup: expected DataOffset({}) but got DataOffset({})",
                    p.label, p.expected_offset, offset
                );
            }
            Some(BtiPartitionLocation::RowsOffset(rows_off)) => {
                panic!(
                    "UUID {} trie lookup returned RowsOffset({}) but expected DataOffset({}). \
                     simple_table is a narrow-partition table so Rows.db offsets are not expected.",
                    p.label, rows_off, p.expected_offset
                );
            }
            None => {
                panic!(
                    "UUID {} not found in Partitions.db trie (lookup returned None). \
                     Expected DataOffset({}).",
                    p.label, p.expected_offset
                );
            }
        }
    }

    eprintln!(
        "bti_trie_resolves_data_db_offsets PASSED: all three UUID trie lookups \
         returned the golden Data.db offsets (0, 63, 125)"
    );
}

// ---------------------------------------------------------------------------
// Core test: decompressed Data.db contains the correct partition key bytes
//            at the trie-resolved offsets
// ---------------------------------------------------------------------------

/// Phase 2: Decompress Data.db chunk and verify UUID bytes at each trie-resolved
/// offset.
///
/// This test COMBINES the trie lookup with Data.db decompression to provide full
/// end-to-end parity proof:
///  - Trie resolves offset O for UUID key K.
///  - Decompressed Data.db[O] == partition-header for K.
///
/// Partition header format (da / oa format, V5CompressedLegacy with
/// `hasUIntDeletionTime`):
///   byte 0:     flags          (0x00 for simple partitions)
///   byte 1:     key_len (u8)   (0x10 = 16 for UUID)
///   bytes 2-17: raw UUID bytes
///   byte 18:    deletion-time byte (0x80 = LIVE)
#[test]
fn bti_trie_offset_points_to_correct_uuid_in_data_db() {
    let Some(dir) = da_simple_table_dir() else {
        eprintln!("SKIP: test_da/simple_table binary SSTables not available");
        return;
    };

    // ---- 1. Locate the three SSTable components ----
    let partitions_db = match find_file(&dir, "da-", "-bti-Partitions.db") {
        Some(p) => p,
        None => {
            eprintln!("SKIP: da-*-bti-Partitions.db not found");
            return;
        }
    };
    let compression_info_path = match find_file(&dir, "da-", "-bti-CompressionInfo.db") {
        Some(p) => p,
        None => {
            eprintln!("SKIP: da-*-bti-CompressionInfo.db not found");
            return;
        }
    };
    let data_db_path = match find_file(&dir, "da-", "-bti-Data.db") {
        Some(p) => p,
        None => {
            eprintln!("SKIP: da-*-bti-Data.db not found");
            return;
        }
    };

    // ---- 2. Phase 1: trie lookups (Data.db not yet opened) ----
    let mut partitions_file = BufReader::new(
        File::open(&partitions_db).unwrap_or_else(|e| panic!("open {:?}: {}", partitions_db, e)),
    );

    let mut resolved_offsets: Vec<(u8, u64)> = Vec::new();
    for p in TEST_PARTITIONS {
        let raw_uuid: [u8; 16] = [p.uuid_byte; 16];
        let loc = lookup_raw_key_in_bti_partitions_db(&mut partitions_file, &raw_uuid)
            .unwrap_or_else(|e| panic!("trie lookup for UUID {}: {}", p.label, e))
            .unwrap_or_else(|| panic!("UUID {} not found in trie", p.label));

        let data_offset = match loc {
            BtiPartitionLocation::DataOffset(off) => off,
            BtiPartitionLocation::RowsOffset(off) => {
                panic!(
                    "UUID {} returned RowsOffset({}); expected DataOffset",
                    p.label, off
                )
            }
        };

        assert_eq!(
            data_offset, p.expected_offset,
            "Phase-1 assertion: UUID {} expected offset {} got {}",
            p.label, p.expected_offset, data_offset
        );
        resolved_offsets.push((p.uuid_byte, data_offset));
    }

    // ---- 3. Decompress Data.db ----
    let compression_raw = std::fs::read(&compression_info_path)
        .unwrap_or_else(|e| panic!("read {:?}: {}", compression_info_path, e));
    let compression_info = CompressionInfo::parse(&compression_raw)
        .unwrap_or_else(|e| panic!("parse CompressionInfo: {}", e));

    // Detect compression algorithm from the parsed name
    let algorithm = CompressionAlgorithm::from(compression_info.algorithm.as_str());

    let data_file =
        File::open(&data_db_path).unwrap_or_else(|e| panic!("open {:?}: {}", data_db_path, e));
    let data_file_size = data_file
        .metadata()
        .unwrap_or_else(|e| panic!("metadata {:?}: {}", data_db_path, e))
        .len();

    let mut chunk_reader =
        ChunkReader::new(BufReader::new(data_file), compression_info, data_file_size);

    // All three partitions are in chunk 0 (data_length = 191, single 16 KiB chunk)
    let compressed_chunk = chunk_reader
        .read_chunk(0)
        .unwrap_or_else(|e| panic!("read_chunk(0): {}", e));

    let compressor =
        Compression::new(algorithm).unwrap_or_else(|e| panic!("Compression::new: {}", e));
    let decompressed = compressor
        .decompress(&compressed_chunk)
        .unwrap_or_else(|e| panic!("decompress chunk 0: {}", e));

    assert!(
        !decompressed.is_empty(),
        "Decompressed Data.db chunk 0 must not be empty"
    );

    // ---- 4. Phase 2: verify UUID bytes at trie-resolved offsets ----
    //
    // Partition header layout (da / oa, V5CompressedLegacy hasUIntDeletionTime):
    //   [0]      flags     (0x00)
    //   [1]      key_len   (0x10 = 16)
    //   [2..18]  UUID bytes (16 bytes)
    //   [18]     deletion-time byte (0x80 = LIVE)
    const FLAGS_OFFSET: usize = 0;
    const KEY_LEN_OFFSET: usize = 1;
    const KEY_DATA_OFFSET: usize = 2;
    const UUID_LEN: usize = 16;

    for (uuid_byte, data_offset) in &resolved_offsets {
        let off = *data_offset as usize;

        // Guard: bounds check before indexing
        assert!(
            off + KEY_DATA_OFFSET + UUID_LEN <= decompressed.len(),
            "Partition at offset {} extends past decompressed buffer len {}",
            off,
            decompressed.len()
        );

        let flags = decompressed[off + FLAGS_OFFSET];
        let key_len = decompressed[off + KEY_LEN_OFFSET] as usize;

        assert_eq!(
            key_len, UUID_LEN,
            "Partition at offset {}: expected key_len=16 for UUID, got {}. flags=0x{:02x}",
            off, key_len, flags
        );

        let uuid_bytes = &decompressed[off + KEY_DATA_OFFSET..off + KEY_DATA_OFFSET + UUID_LEN];
        let expected_uuid: [u8; UUID_LEN] = [*uuid_byte; UUID_LEN];

        assert_eq!(
            uuid_bytes,
            &expected_uuid[..],
            "Partition at offset {} (uuid_byte=0x{:02x}): UUID bytes in Data.db do not match. \
             Got: {:?}, Expected: {:?}",
            off,
            uuid_byte,
            uuid_bytes,
            expected_uuid
        );
    }

    eprintln!(
        "bti_trie_offset_points_to_correct_uuid_in_data_db PASSED: \
         trie-resolved offsets (0, 63, 125) each point to the correct UUID bytes \
         in the decompressed Data.db"
    );
}

// ---------------------------------------------------------------------------
// Issue #831: SSTableReader::open now SUCCEEDS for BTI Data.db (the pre-#831
// UnsupportedFormat gate has been lifted). The end-to-end reader assertions
// live in issue_831_bti_reader_point_lookup.rs; this is a light guard that the
// gate is gone.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bti_sstable_reader_open_now_succeeds() {
    let Some(dir) = da_simple_table_dir() else {
        eprintln!("SKIP: test_da/simple_table binary SSTables not available");
        return;
    };

    let data_db = match find_file(&dir, "da-", "-bti-Data.db") {
        Some(p) => p,
        None => {
            eprintln!("SKIP: da-*-bti-Data.db not found");
            return;
        }
    };

    use cqlite_core::{storage::sstable::reader::SSTableReader, Config};
    use std::sync::Arc;

    let config = Config::default();
    let platform = Arc::new(
        cqlite_core::platform::Platform::new(&config)
            .await
            .expect("Platform::new"),
    );

    let result = SSTableReader::open(&data_db, &config, platform).await;
    assert!(
        result.is_ok(),
        "SSTableReader::open on BTI Data.db must now succeed (#831), got: {:?}",
        result.err()
    );

    eprintln!("bti_sstable_reader_open_now_succeeds PASSED: BTI open gate lifted");
}
