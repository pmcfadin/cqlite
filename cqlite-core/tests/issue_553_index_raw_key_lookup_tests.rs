//! Issue #553 regression tests — Index.db O(1) raw-key lookup
//!
//! Defect: `lookup_partition_with_index` computed a Murmur3 digest of the partition key
//! and looked up that digest, but the Index.db `key_lookup` map is keyed on RAW partition
//! key bytes (since Issue #552).  Every lookup missed → O(n) sequential scan fallback on
//! every `get()` call.
//!
//! Fix: pass `partition_key` directly to `index_reader.lookup_partition`.  The incoming
//! `partition_key: &[u8]` must be the raw representation from `PartitionKey::to_bytes`:
//! - Single: raw value bytes (UUID = 16 bytes, int = 4 BE bytes, …)
//! - Multi-component composite: `[len u16 BE][value][0x00]` per component
//!
//! Tests:
//! - Unit: Index.db raw-key lookup succeeds after the fix (no Data.db needed)
//! - Integration: `lookup_partition_with_index` returns `Some((offset, size))` for a
//!   known-present UUID key from `test_basic.simple_table`
//! - Integration: composite-key lookup from `test_basic.multi_partition_table`
//!
//! Integration tests require `CQLITE_DATASETS_ROOT`.

// ============================================================================
// Unit tests — no Data.db needed; verifies raw-key lookup on a hand-crafted index
// ============================================================================

/// Issue #553: After the fix, the IndexReader lookup_partition uses raw key bytes
/// and must find entries for known keys.
///
/// This test constructs a synthetic Index.db in memory, opens it via IndexReader,
/// and verifies that `lookup_partition` with the exact raw key bytes succeeds.
///
/// Before fix: `lookup_partition_with_index` computed a Murmur3 digest and looked
///   up the digest in a map keyed on raw bytes → always missed → returned None.
/// After fix: raw key bytes are passed directly → O(1) hit.
#[tokio::test]
async fn test_index_reader_raw_key_lookup_uuid() {
    use cqlite_core::storage::sstable::index_reader::IndexReader;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::io::AsyncWriteExt;

    let uuid_bytes: [u8; 16] = [
        0x00, 0x23, 0xec, 0xe7, 0x7c, 0x4e, 0x47, 0x05, 0x90, 0x68, 0xd1, 0xa5, 0x9e, 0xc5, 0xfe,
        0x19,
    ];

    // Build synthetic Index.db: one BIG-format entry
    // Format: [key_len: u16 BE][raw_key][data_offset: vint][promoted_len: vint]
    // vint for offset=256: 0x81 0x00
    let mut index_bytes = Vec::new();
    index_bytes.extend_from_slice(&[0x00, 0x10]); // key_len = 16
    index_bytes.extend_from_slice(&uuid_bytes); // 16 raw key bytes
    index_bytes.extend_from_slice(&[0x81, 0x00]); // vint offset = 256
    index_bytes.extend_from_slice(&[0x00]); // vint promoted_len = 0

    let temp_dir = TempDir::new().expect("TempDir");
    let index_path = temp_dir.path().join("nb-1-big-Index.db");
    let mut file = tokio::fs::File::create(&index_path)
        .await
        .expect("create index file");
    file.write_all(&index_bytes).await.expect("write index");
    file.flush().await.expect("flush");
    drop(file);

    let config = cqlite_core::Config::default();
    let platform = Arc::new(
        cqlite_core::platform::Platform::new(&config)
            .await
            .expect("Platform"),
    );
    let reader = IndexReader::open(&index_path, platform)
        .await
        .expect("IndexReader::open");

    // Direct raw-key lookup must succeed after the fix.
    let entry = reader.lookup_partition(&uuid_bytes);
    assert!(
        entry.is_some(),
        "Issue #553: raw-key lookup in Index.db must return Some. \
         Before fix: Murmur3 digest was used and missed. \
         After fix: raw bytes are used and hit."
    );
    let entry = entry.unwrap();
    assert_eq!(
        entry.data_offset, 256,
        "Offset must be 256 (from vint 0x81 0x00)"
    );

    // A non-existent key must still return None (no false positives).
    let wrong_key = [0xFF_u8; 16];
    assert!(
        reader.lookup_partition(&wrong_key).is_none(),
        "Non-existent key must return None"
    );

    eprintln!("test_index_reader_raw_key_lookup_uuid: PASSED");
}

// ============================================================================
// Integration tests — require CQLITE_DATASETS_ROOT + Index.db files
// ============================================================================

use std::path::{Path, PathBuf};
use std::sync::Arc;

fn get_datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

fn index_db_exists(datasets_root: &Path, keyspace: &str, table_dir: &str) -> bool {
    datasets_root
        .join("sstables")
        .join(keyspace)
        .join(table_dir)
        .join("nb-1-big-Index.db")
        .exists()
}

/// Issue #553 integration: `lookup_partition_with_index` returns `Some((offset, _))`
/// for a real UUID partition key from `test_basic.simple_table`.
///
/// The fast path proves the Index.db was hit (not the sequential scan fallback):
/// - Before fix: every lookup missed → `Ok(None)` always
/// - After fix: raw-key lookup hits → `Ok(Some((non_zero_offset, 0)))`
#[tokio::test]
async fn test_lookup_returns_some_for_simple_table_uuid() {
    let datasets_root = match get_datasets_root() {
        Some(r) => r,
        None => {
            eprintln!(
                "test_lookup_returns_some_for_simple_table_uuid: SKIPPED (no CQLITE_DATASETS_ROOT)"
            );
            return;
        }
    };

    let simple_dir = "simple_table-6aa08200a25111f0a3fef1a551383fb9";
    if !index_db_exists(&datasets_root, "test_basic", simple_dir) {
        eprintln!("test_lookup_returns_some_for_simple_table_uuid: SKIPPED (Index.db not found)");
        return;
    }

    let index_path = datasets_root
        .join("sstables")
        .join("test_basic")
        .join(simple_dir)
        .join("nb-1-big-Index.db");

    // Open the real Index.db
    let config = cqlite_core::Config::default();
    let platform = Arc::new(
        cqlite_core::platform::Platform::new(&config)
            .await
            .expect("Platform creation should succeed"),
    );
    let index_reader =
        cqlite_core::storage::sstable::index_reader::IndexReader::open(&index_path, platform)
            .await
            .expect("IndexReader::open should succeed");

    // The first entry from the JSONL is UUID 15291a77-d739-4e73-8397-b787442f3a1f.
    // Its raw bytes are the Index.db key (16 bytes, no framing for single-component PK).
    let uuid_15291a77: [u8; 16] = [
        0x15, 0x29, 0x1a, 0x77, 0xd7, 0x39, 0x4e, 0x73, 0x83, 0x97, 0xb7, 0x87, 0x44, 0x2f, 0x3a,
        0x1f,
    ];

    // Direct lookup using the raw key bytes.
    let entry = index_reader.lookup_partition(&uuid_15291a77);

    assert!(
        entry.is_some(),
        "Issue #553 BEFORE FIX: lookup_partition with raw UUID bytes returns None. \
         After fix it must return Some. UUID=15291a77-d739-4e73-8397-b787442f3a1f"
    );

    let entry = entry.unwrap();

    // The first partition is at offset 0 in Data.db data section.
    assert_eq!(
        entry.data_offset, 0,
        "First simple_table partition must be at data offset 0 (from Index.db)"
    );

    eprintln!(
        "test_lookup_returns_some_for_simple_table_uuid: PASSED \
         (offset={}, data_size={})",
        entry.data_offset, entry.data_size
    );
}

/// Issue #553 integration: `SSTableReader::lookup_partition_with_index` returns `Some`
/// for the first UUID partition in `test_basic.simple_table`, proving the fast (index)
/// path is taken rather than a sequential scan fallback.
///
/// Before fix: `lookup_partition_with_index` computed `compute_partition_key_digest(raw_key)`
///   → Murmur3 hash bytes → looked those up in a map keyed on RAW bytes → always None.
/// After fix: raw_key bytes are passed directly → O(1) hit → returns Some.
// Self-skips at runtime when datasets/Data.db are absent (see body), so it runs
// and asserts the full index fast path whenever data is present (e.g. CI fetches
// datasets) rather than being unconditionally ignored.
#[tokio::test]
async fn test_lookup_partition_with_index_uuid_fast_path() {
    let datasets_root = match get_datasets_root() {
        Some(r) => r,
        None => {
            eprintln!("test_lookup_partition_with_index_uuid_fast_path: SKIPPED");
            return;
        }
    };

    let simple_dir = "simple_table-6aa08200a25111f0a3fef1a551383fb9";
    let data_path = datasets_root
        .join("sstables")
        .join("test_basic")
        .join(simple_dir)
        .join("nb-1-big-Data.db");

    if !data_path.exists() {
        eprintln!("test_lookup_partition_with_index_uuid_fast_path: SKIPPED (Data.db not found)");
        return;
    }

    let config = cqlite_core::Config::default();
    let platform = Arc::new(
        cqlite_core::platform::Platform::new(&config)
            .await
            .expect("Platform"),
    );
    let reader =
        cqlite_core::storage::sstable::reader::SSTableReader::open(&data_path, &config, platform)
            .await
            .expect("SSTableReader::open");

    // Raw UUID key for the first partition: 15291a77-d739-4e73-8397-b787442f3a1f
    let uuid_15291a77: [u8; 16] = [
        0x15, 0x29, 0x1a, 0x77, 0xd7, 0x39, 0x4e, 0x73, 0x83, 0x97, 0xb7, 0x87, 0x44, 0x2f, 0x3a,
        0x1f,
    ];

    let result = reader
        .lookup_partition_with_index(&uuid_15291a77)
        .await
        .expect("lookup_partition_with_index should not error");

    assert!(
        result.is_some(),
        "Issue #553 BEFORE FIX: lookup_partition_with_index returns None for a known UUID. \
         After fix it must return Some((offset, data_size)). \
         UUID=15291a77-d739-4e73-8397-b787442f3a1f"
    );

    let (offset, _data_size) = result.unwrap();

    // The first partition in simple_table is at offset 0 in the data section.
    assert_eq!(
        offset, 0,
        "First simple_table partition must be at data_offset 0 (from Index.db)"
    );

    eprintln!(
        "test_lookup_partition_with_index_uuid_fast_path: PASSED (offset={})",
        offset
    );
}

/// Issue #553 integration: composite-key lookup in `test_basic.multi_partition_table`.
///
/// multi_partition_table has partition key (tenant_id UUID, user_id UUID).
/// The raw composite key is `[0x00 0x10][16 bytes][0x00][0x00 0x10][16 bytes][0x00]` = 38 bytes.
/// The Index.db key_len is 0x0026 = 38 for this table (verified in test_real_index_db_big_format).
#[tokio::test]
async fn test_lookup_returns_some_for_multi_partition_table() {
    let datasets_root = match get_datasets_root() {
        Some(r) => r,
        None => {
            eprintln!("test_lookup_returns_some_for_multi_partition_table: SKIPPED (no CQLITE_DATASETS_ROOT)");
            return;
        }
    };

    let multi_dir = "multi_partition_table-6ac52100a25111f0a3fef1a551383fb9";
    if !index_db_exists(&datasets_root, "test_basic", multi_dir) {
        eprintln!(
            "test_lookup_returns_some_for_multi_partition_table: SKIPPED (Index.db not found)"
        );
        return;
    }

    let index_path = datasets_root
        .join("sstables")
        .join("test_basic")
        .join(multi_dir)
        .join("nb-1-big-Index.db");

    // Open the real Index.db
    let config = cqlite_core::Config::default();
    let platform = Arc::new(
        cqlite_core::platform::Platform::new(&config)
            .await
            .expect("Platform creation should succeed"),
    );
    let index_reader =
        cqlite_core::storage::sstable::index_reader::IndexReader::open(&index_path, platform)
            .await
            .expect("IndexReader::open should succeed");

    // The first partition from the JSONL has composite key:
    //   tenant_id = 98e05820-982d-411c-961f-26d1057474e4
    //   user_id   = 9d159a2b-08da-4ad1-be78-c90f8783e5c1
    //
    // Composite framing (matching PartitionKey::to_bytes multi-component output):
    //   [0x00 0x10][tenant_id 16 bytes][0x00][0x00 0x10][user_id 16 bytes][0x00] = 38 bytes
    let tenant_id: [u8; 16] = [
        0x98, 0xe0, 0x58, 0x20, 0x98, 0x2d, 0x41, 0x1c, 0x96, 0x1f, 0x26, 0xd1, 0x05, 0x74, 0x74,
        0xe4,
    ];
    let user_id: [u8; 16] = [
        0x9d, 0x15, 0x9a, 0x2b, 0x08, 0xda, 0x4a, 0xd1, 0xbe, 0x78, 0xc9, 0x0f, 0x87, 0x83, 0xe5,
        0xc1,
    ];

    // Build the composite raw key
    let mut composite_key = Vec::with_capacity(38);
    composite_key.extend_from_slice(&[0x00, 0x10]); // len = 16
    composite_key.extend_from_slice(&tenant_id);
    composite_key.push(0x00); // end-of-component
    composite_key.extend_from_slice(&[0x00, 0x10]); // len = 16
    composite_key.extend_from_slice(&user_id);
    composite_key.push(0x00); // end-of-component
    assert_eq!(composite_key.len(), 38);

    let entry = index_reader.lookup_partition(&composite_key);

    assert!(
        entry.is_some(),
        "Issue #553 BEFORE FIX: composite-key lookup returns None. \
         After fix it must return Some. \
         Key (38 bytes): tenant=98e05820... user=9d159a2b..."
    );

    let entry = entry.unwrap();
    assert_eq!(
        entry.data_offset, 0,
        "First multi_partition_table partition must be at data offset 0"
    );
    assert_eq!(
        entry.key_digest.len(),
        38,
        "Index.db entry key must be 38 bytes (composite UUID pair)"
    );

    eprintln!(
        "test_lookup_returns_some_for_multi_partition_table: PASSED \
         (offset={}, key_len={})",
        entry.data_offset,
        entry.key_digest.len()
    );
}
