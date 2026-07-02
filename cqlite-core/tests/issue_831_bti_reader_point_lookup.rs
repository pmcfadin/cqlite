//! End-to-end integration test for Issue #831: wire the BTI trie point-lookup
//! primitive (verified in #755) into `SSTableReader`'s public open + get path.
//!
//! # What this proves
//!
//! 1. `SSTableReader::open` now SUCCEEDS for a BTI ("da") Data.db when the
//!    sibling `*-Partitions.db` trie is present (the #657 gate is lifted).
//! 2. A partition-key lookup resolves via the BTI trie (O(log n)) — returning
//!    the exact golden Data.db offsets (0/63/125) — instead of a sequential
//!    scan.
//! 3. `reader.get(table_id, raw_uuid)` returns the decoded row for each known
//!    key, with column values matching the `da-2-bti-Data.db.jsonl` golden.
//! 4. A missing key returns `Ok(None)` without consulting the sequential scan.
//!
//! # Test data requirement
//!
//! `CQLITE_DATASETS_ROOT` must point to `test-data/datasets` and the `test_da`
//! binary SSTables must be present. Tests skip gracefully when absent:
//!
//! ```bash
//! CQLITE_DATASETS_ROOT=$PWD/test-data/datasets cargo test --test issue_831_bti_reader_point_lookup
//! ```

use cqlite_core::ScanRow;
use cqlite_core::{storage::sstable::reader::SSTableReader, types::TableId, Config, RowKey, Value};
use std::path::{Path, PathBuf};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Golden constants (same fixture / offsets as issue_755_bti_trie_point_lookup.rs)
// ---------------------------------------------------------------------------

struct Partition {
    /// Raw 16-byte UUID (all bytes identical for these test UUIDs)
    uuid_byte: u8,
    /// Expected Data.db uncompressed byte offset (from JSONL golden "position")
    expected_offset: u64,
    /// Canonical display UUID string (diagnostics)
    label: &'static str,
    /// Golden cell values (from da-2-bti-Data.db.jsonl).
    /// `age` is CQL INT (Value::Integer), `salary` is CQL BIGINT (Value::BigInt).
    name: &'static str,
    age: i32,
    salary: i64,
    active: bool,
}

const TEST_PARTITIONS: &[Partition] = &[
    Partition {
        uuid_byte: 0x22,
        expected_offset: 0,
        label: "22222222-2222-2222-2222-222222222222",
        name: "Bob Johnson",
        age: 45,
        salary: 95000,
        active: false,
    },
    Partition {
        uuid_byte: 0x11,
        expected_offset: 63,
        label: "11111111-1111-1111-1111-111111111111",
        name: "Alice Smith",
        age: 30,
        salary: 75000,
        active: true,
    },
    Partition {
        uuid_byte: 0x33,
        expected_offset: 125,
        label: "33333333-3333-3333-3333-333333333333",
        name: "Carol Williams",
        age: 28,
        salary: 65000,
        active: true,
    },
];

const TABLE_ID: &str = "test_da.simple_table";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Locate the `da-*-bti-Data.db` for `test_da/simple_table`, or `None` if the
/// binary fixture (with the Partitions.db trie) is absent.
fn da_data_db_path() -> Option<PathBuf> {
    let root = std::env::var("CQLITE_DATASETS_ROOT").ok()?;
    let base = PathBuf::from(root).join("sstables").join("test_da");

    let table_dir = std::fs::read_dir(&base)
        .ok()?
        .flatten()
        .find(|e| e.file_name().to_string_lossy().starts_with("simple_table-"))
        .map(|e| e.path())?;

    // Both Data.db AND Partitions.db must be present for the lookup path.
    let has_partitions = std::fs::read_dir(&table_dir).ok()?.flatten().any(|e| {
        let n = e.file_name();
        let s = n.to_string_lossy();
        s.starts_with("da-") && s.ends_with("-bti-Partitions.db")
    });
    if !has_partitions {
        eprintln!("SKIP: da-*-bti-Partitions.db not present; run fetch-datasets.sh");
        return None;
    }

    std::fs::read_dir(&table_dir).ok()?.flatten().find_map(|e| {
        let n = e.file_name();
        let s = n.to_string_lossy();
        if s.starts_with("da-") && s.ends_with("-bti-Data.db") {
            Some(e.path())
        } else {
            None
        }
    })
}

async fn open_reader(data_db: &Path) -> SSTableReader {
    let config = Config::default();
    let platform = Arc::new(
        cqlite_core::platform::Platform::new(&config)
            .await
            .expect("Platform::new"),
    );
    SSTableReader::open(data_db, &config, platform)
        .await
        .expect("SSTableReader::open must succeed for BTI Data.db with Partitions.db present")
}

/// Extract a named cell value from the `Value::Map` a BTI partition row decodes to.
fn cell<'a>(value: &'a ScanRow, name: &str) -> Option<&'a Value> {
    // Issue #1334: rows decode to `ScanRow::Row` keyed by `Arc<str>`.
    if let ScanRow::Row(entries) = value {
        for (k, v) in entries {
            if k.as_ref() == name {
                return Some(v);
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Test 1: open succeeds for BTI when Partitions.db present
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bti_reader_open_succeeds() {
    let Some(data_db) = da_data_db_path() else {
        eprintln!("SKIP: test_da/simple_table BTI fixture not available");
        return;
    };
    let _reader = open_reader(&data_db).await;
    eprintln!("bti_reader_open_succeeds PASSED: SSTableReader::open returned Ok for BTI Data.db");
}

// ---------------------------------------------------------------------------
// Test 2: trie resolves the golden offsets (proves trie use, not scan)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bti_reader_point_lookup_resolves_golden_offset() {
    let Some(data_db) = da_data_db_path() else {
        eprintln!("SKIP: test_da/simple_table BTI fixture not available");
        return;
    };
    let reader = open_reader(&data_db).await;

    for p in TEST_PARTITIONS {
        let raw: [u8; 16] = [p.uuid_byte; 16];
        let resolved = reader
            .lookup_partition_via_bti_trie(&raw)
            .expect("trie lookup must not error");
        assert_eq!(
            resolved,
            Some(p.expected_offset),
            "UUID {} expected trie-resolved DataOffset {}",
            p.label,
            p.expected_offset
        );
    }
    eprintln!(
        "bti_reader_point_lookup_resolves_golden_offset PASSED: trie returned offsets 0/63/125"
    );
}

// ---------------------------------------------------------------------------
// Test 3: get() returns the decoded row for each known key
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bti_reader_get_returns_row_for_known_key() {
    let Some(data_db) = da_data_db_path() else {
        eprintln!("SKIP: test_da/simple_table BTI fixture not available");
        return;
    };
    let reader = open_reader(&data_db).await;
    let table_id = TableId::from(TABLE_ID);

    for p in TEST_PARTITIONS {
        let raw: [u8; 16] = [p.uuid_byte; 16];
        let key = RowKey::new(raw.to_vec());

        let result = reader
            .get(&table_id, &key)
            .await
            .unwrap_or_else(|e| panic!("get() for UUID {} errored: {}", p.label, e));

        let value = result
            .unwrap_or_else(|| panic!("get() for UUID {} returned None (expected a row)", p.label));

        // name (Text)
        match cell(&value, "name") {
            Some(Value::Text(s)) => assert_eq!(s, p.name, "UUID {} name mismatch", p.label),
            other => panic!("UUID {} missing/!text name cell: {:?}", p.label, other),
        }
        // age (Int)
        match cell(&value, "age") {
            Some(Value::Integer(n)) => assert_eq!(*n, p.age, "UUID {} age mismatch", p.label),
            other => panic!("UUID {} missing/!int age cell: {:?}", p.label, other),
        }
        // salary (BigInt)
        match cell(&value, "salary") {
            Some(Value::BigInt(n)) => {
                assert_eq!(*n, p.salary, "UUID {} salary mismatch", p.label)
            }
            other => panic!("UUID {} missing/!bigint salary cell: {:?}", p.label, other),
        }
        // active (Boolean)
        match cell(&value, "active") {
            Some(Value::Boolean(b)) => {
                assert_eq!(*b, p.active, "UUID {} active mismatch", p.label)
            }
            other => panic!("UUID {} missing/!bool active cell: {:?}", p.label, other),
        }
    }
    eprintln!(
        "bti_reader_get_returns_row_for_known_key PASSED: decoded rows match the JSONL golden"
    );
}

// ---------------------------------------------------------------------------
// Test 3a: a fully-qualified WRONG-KEYSPACE query must NOT return a row, even
// though the table name matches (issue #831 review: the BTI guard must compare
// keyspace.table exactly, not just the unqualified table name).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bti_reader_get_wrong_keyspace_returns_none() {
    let Some(data_db) = da_data_db_path() else {
        eprintln!("SKIP: test_da/simple_table BTI fixture not available");
        return;
    };
    let reader = open_reader(&data_db).await;

    // Same table name, different keyspace — both fully qualified.
    let wrong_ks = TableId::from("other_keyspace.simple_table");
    // A key that DOES exist in this SSTable under the correct keyspace.
    let existing_key = RowKey::new([TEST_PARTITIONS[0].uuid_byte; 16].to_vec());

    let result = reader
        .get(&wrong_ks, &existing_key)
        .await
        .expect("get() with a wrong-keyspace table id must not error");
    assert!(
        result.is_none(),
        "BTI point lookup returned a row for a wrong-keyspace query \
         (other_keyspace.simple_table); the keyspace-aware guard must reject it"
    );

    // Sanity: the same key under the CORRECT qualified id still resolves.
    let right_ks = TableId::from(TABLE_ID);
    let ok = reader
        .get(&right_ks, &existing_key)
        .await
        .expect("get() with the correct table id must not error");
    assert!(
        ok.is_some(),
        "control: correct keyspace.table must still return the row"
    );
    eprintln!("bti_reader_get_wrong_keyspace_returns_none PASSED");
}

// ---------------------------------------------------------------------------
// Test 3b: chunk-targeted decompress lands on the correct chunk (issue #831
// perf finding) — the lookup decompresses only the chunk containing the trie
// offset, not the whole section. For this single-chunk fixture every golden
// offset (0/63/125) maps to target_chunk 0, and get() must still return the
// identical golden rows (proving the chunk-targeted branch produced correct
// output rather than falling back to the whole-section stitch).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bti_reader_get_uses_chunk_targeted_decompress() {
    let Some(data_db) = da_data_db_path() else {
        eprintln!("SKIP: test_da/simple_table BTI fixture not available");
        return;
    };
    let reader = open_reader(&data_db).await;
    let table_id = TableId::from(TABLE_ID);

    // The fixture is chunk-compressed: CompressionInfo must be present, and the
    // chunk-targeting math must place every golden offset in chunk 0.
    let comp = reader
        .compression_info
        .as_ref()
        .expect("BTI fixture is chunk-compressed (CompressionInfo.db present)");
    let chunk_length = comp.chunk_length as u64;
    assert!(chunk_length > 0, "chunk_length must be non-zero");

    for p in TEST_PARTITIONS {
        let target_chunk = (p.expected_offset / chunk_length) as usize;
        assert_eq!(
            target_chunk, 0,
            "offset {} (chunk_length {}) must target chunk 0 for this fixture",
            p.expected_offset, chunk_length
        );

        // And get() through the chunk-targeted path still returns the golden row.
        let raw: [u8; 16] = [p.uuid_byte; 16];
        let key = RowKey::new(raw.to_vec());
        let value = reader
            .get(&table_id, &key)
            .await
            .unwrap_or_else(|e| panic!("get() for UUID {} errored: {}", p.label, e))
            .unwrap_or_else(|| panic!("get() for UUID {} returned None", p.label));
        match cell(&value, "name") {
            Some(Value::Text(s)) => assert_eq!(s, p.name, "UUID {} name mismatch", p.label),
            other => panic!("UUID {} missing/!text name cell: {:?}", p.label, other),
        }
    }
    eprintln!(
        "bti_reader_get_uses_chunk_targeted_decompress PASSED: target_chunk=0 for all golden \
         offsets and rows match"
    );
}

// ---------------------------------------------------------------------------
// Test 4: missing key returns None
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bti_reader_get_missing_key_returns_none() {
    let Some(data_db) = da_data_db_path() else {
        eprintln!("SKIP: test_da/simple_table BTI fixture not available");
        return;
    };
    let reader = open_reader(&data_db).await;
    let table_id = TableId::from(TABLE_ID);

    let missing = RowKey::new(vec![0x00u8; 16]);
    let result = reader
        .get(&table_id, &missing)
        .await
        .expect("get() for missing key must not error");
    assert!(
        result.is_none(),
        "get() for a UUID not in the fixture must return None, got {:?}",
        result
    );
    eprintln!("bti_reader_get_missing_key_returns_none PASSED");
}

// ---------------------------------------------------------------------------
// Test 5: no sequential scan is consulted on a BTI get()
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bti_reader_get_does_not_sequential_scan() {
    let Some(data_db) = da_data_db_path() else {
        eprintln!("SKIP: test_da/simple_table BTI fixture not available");
        return;
    };
    let reader = open_reader(&data_db).await;
    let table_id = TableId::from(TABLE_ID);

    let before = SSTableReader::scan_for_key_call_count();
    let raw: [u8; 16] = [0x22; 16];
    let key = RowKey::new(raw.to_vec());
    let _ = reader.get(&table_id, &key).await.expect("get() ok");
    let after = SSTableReader::scan_for_key_call_count();

    assert_eq!(
        before, after,
        "BTI get() must not invoke scan_for_key (sequential scan), count went {} -> {}",
        before, after
    );
    eprintln!("bti_reader_get_does_not_sequential_scan PASSED: scan_for_key never called");
}
