//! Authoritative PARTITION START offsets for tests (issue #4501, part of #4482).
//!
//! `sstabledump`'s `partition.position` is NOT a partition start: it is the scan
//! reader's file pointer when the partition is serialized. Cassandra 5.0.2 printed
//! the start (first partition 0); 5.0.8 prints the pointer AFTER the partition header
//! (`2 + key_len + 12` bytes later: first int-keyed partition 18). A test that needs a
//! partition's start offset must therefore get it from authoritative on-disk metadata:
//!
//!   * BIG (`nb`/`oa`): the partition's `Index.db` entry (definitive guide Ch.6),
//!     read with CQLite's own `IndexReader`;
//!   * BTI (`da`): the `Partitions.db` trie payload (Ch.17), see the BTI helpers in the
//!     tests that need them.
//!
//! No byte-pattern inference (#28): the key is an explicit argument and the offset is
//! cross-checked against the two-byte key length that `Data.db` itself carries there.

#![allow(dead_code)]

use std::path::Path;
use std::sync::Arc;

use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::index_reader::IndexReader;
use cqlite_core::Config;

/// Every `(raw partition key bytes, Data.db start offset)` entry of a BIG `Index.db`,
/// in file order.
pub fn big_partition_starts(index_db: &Path) -> Vec<(Vec<u8>, u64)> {
    let path = index_db.to_path_buf();
    // A fresh OS thread owns its own runtime, so this is callable from sync tests AND
    // from inside a `#[tokio::test]` (where `block_on` on the caller's thread panics).
    let entries = std::thread::scope(|scope| {
        scope
            .spawn(|| {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("tokio runtime");
                rt.block_on(async {
                    let config = Config::default();
                    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
                    let reader = IndexReader::open(&path, platform)
                        .await
                        .unwrap_or_else(|e| panic!("open {path:?}: {e}"));
                    reader
                        .get_partition_entries()
                        .iter()
                        .map(|e| {
                            let key = e
                                .raw_key
                                .clone()
                                .unwrap_or_else(|| e.key_digest.clone())
                                .to_vec();
                            (key, e.data_offset)
                        })
                        .collect::<Vec<_>>()
                })
            })
            .join()
            .expect("index reader thread")
    });
    assert!(!entries.is_empty(), "{index_db:?} declares no partitions");
    entries
}

/// The Data.db start offset of the partition whose raw key bytes are `raw_key`, from
/// `index_db`. Panics (naming the file and key) when the key is absent or duplicated.
pub fn big_partition_start(index_db: &Path, raw_key: &[u8]) -> u64 {
    let mut hits = big_partition_starts(index_db)
        .into_iter()
        .filter(|(k, _)| k.as_slice() == raw_key);
    let first = hits
        .next()
        .unwrap_or_else(|| panic!("{index_db:?}: no Index.db entry for key {raw_key:02x?}"));
    assert!(
        hits.next().is_none(),
        "{index_db:?}: key {raw_key:02x?} appears more than once"
    );
    first.1
}

/// [`big_partition_start`] cross-checked against the (decompressed) `Data.db` bytes:
/// the partition header there begins with the two-byte big-endian key length and the
/// key itself, so an `Index.db` mis-read fails by name instead of pointing a test at
/// the wrong byte.
pub fn big_partition_start_checked(index_db: &Path, data: &[u8], raw_key: &[u8]) -> usize {
    let start = big_partition_start(index_db, raw_key) as usize;
    let key_len = u16::from_be_bytes([data[start], data[start + 1]]) as usize;
    assert_eq!(key_len, raw_key.len(), "{index_db:?}: key length at {start}");
    assert_eq!(
        &data[start + 2..start + 2 + key_len],
        raw_key,
        "{index_db:?}: key bytes at {start}"
    );
    start
}

/// Bytes of the partition header that `sstabledump` 5.0.8 has consumed when it reports
/// `partition.position`, for an `nb` partition: `u16 key_len + key + i32
/// localDeletionTime + i64 markedForDeleteAt` (12 bytes of `DeletionTime`).
pub fn header_len_nb(key_len: usize) -> u64 {
    (2 + key_len + 4 + 8) as u64
}

/// The same for a LIVE `oa`/`da` partition: the modern `DeletionTime` serializer writes
/// the single `0x80` live sentinel byte (so `test_da` int-keyed partitions: 2 + 4 + 1 = 7,
/// which is why the first row there sits at `partition.position`).
pub fn header_len_modern_live(key_len: usize) -> u64 {
    (2 + key_len + 1) as u64
}

/// What a golden's `partition.position` means, per generator release.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DumpPositionConvention {
    /// The partition's start offset (the committed 5.0.2 goldens).
    PartitionStart,
    /// The scanner pointer after the partition header (`start + header_len_*`,
    /// Cassandra >= 5.0.5: `JsonTransformer` reads `SSTableSimpleScanner.getCurrentPosition()`
    /// = `dfile.getFilePointer()` once the partition header has been consumed).
    AfterHeader,
}

/// Classify a whole golden: for every `(index_start, header_len, dump_position)` the dump
/// position must be EXACTLY the start or EXACTLY start + header, and one convention
/// must hold for every partition of the file. Any other value (or a mix) panics, so a
/// drifted golden can never pass by accident.
pub fn dump_position_convention(
    what: &str,
    partitions: &[(u64, u64, u64)],
) -> DumpPositionConvention {
    assert!(!partitions.is_empty(), "{what}: no partitions to classify");
    let mut seen: Option<DumpPositionConvention> = None;
    for &(start, header_len, dump) in partitions {
        let this = if dump == start {
            DumpPositionConvention::PartitionStart
        } else if dump == start + header_len {
            DumpPositionConvention::AfterHeader
        } else {
            panic!(
                "{what}: dump position {dump} is neither the index-derived start {start} nor \
                 start + header ({})",
                start + header_len
            );
        };
        match seen {
            None => seen = Some(this),
            Some(prev) => assert_eq!(prev, this, "{what}: mixed dump-position conventions"),
        }
    }
    seen.expect("non-empty")
}

/// Data.db START offset of the BTI partition whose raw key bytes are `raw_key`, from
/// `Partitions.db` (+ `Rows.db` for wide partitions), definitive guide Ch.17:
/// a narrow partition's trie payload is the `Data.db` offset itself
/// (`BtiPartitionLocation::DataOffset`); a wide one points at a `Rows.db`
/// `TrieIndexEntry` whose first field is the partition's `Data.db` position.
pub fn bti_partition_start(partitions_db: &[u8], rows_db: &[u8], raw_key: &[u8]) -> u64 {
    use cqlite_core::storage::sstable::bti::{
        lookup_raw_key_in_bti_partitions_db, resolve_rows_db_entry, BtiPartitionLocation,
    };
    let mut cur = std::io::Cursor::new(partitions_db.to_vec());
    match lookup_raw_key_in_bti_partitions_db(&mut cur, raw_key)
        .expect("Partitions.db lookup must succeed")
    {
        Some(BtiPartitionLocation::DataOffset(o)) => o,
        Some(BtiPartitionLocation::RowsOffset(o)) => {
            resolve_rows_db_entry(rows_db, o as usize)
                .expect("Rows.db entry must deserialize")
                .data_position
        }
        None => panic!("Partitions.db has no entry for key {raw_key:02x?}"),
    }
}
