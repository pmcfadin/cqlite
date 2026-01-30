//! Collection serialization roundtrip tests for M5.1
//!
//! Tests frozen and non-frozen collection serialization.
//! Validates list, set, and map encoding matches Cassandra format.

#![cfg(feature = "write-support")]

use cqlite_core::storage::sstable::writer::data_writer::{
    encode_collection_cell, serialize_collection_or_value, serialize_frozen_list,
    serialize_frozen_map, serialize_frozen_set, serialize_nonfrozen_list, serialize_nonfrozen_map,
    serialize_nonfrozen_set, CollectionCell,
};
use cqlite_core::types::Value;

// =============================================================================
// Frozen Collection Tests
// =============================================================================

#[test]
fn test_frozen_list_format() {
    // Test the binary format matches Cassandra's frozen list encoding
    let elements = vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)];
    let bytes = serialize_frozen_list(&elements).unwrap();

    // Parse back to verify format
    let count = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    assert_eq!(count, 3, "Element count should be 3");

    // First element: length + value
    let len1 = i32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    assert_eq!(len1, 4, "First element length should be 4 (int)");
    let val1 = i32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
    assert_eq!(val1, 1, "First element value should be 1");
}

#[test]
fn test_frozen_set_uniqueness_not_enforced() {
    // Frozen sets don't enforce uniqueness at serialization level
    // That's the caller's responsibility
    let elements = vec![
        Value::Text("duplicate".to_string()),
        Value::Text("duplicate".to_string()),
    ];
    let bytes = serialize_frozen_set(&elements).unwrap();

    // Should serialize both elements (caller enforces uniqueness)
    let count = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    assert_eq!(count, 2, "Should have 2 elements even if duplicates");
}

#[test]
fn test_frozen_map_format() {
    let pairs = vec![
        (Value::Text("a".to_string()), Value::Integer(1)),
        (Value::Text("b".to_string()), Value::Integer(2)),
    ];
    let bytes = serialize_frozen_map(&pairs).unwrap();

    // Parse entry count
    let count = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    assert_eq!(count, 2, "Entry count should be 2");

    // First entry: key "a" (len=1), value 1 (len=4)
    let key1_len = i32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    assert_eq!(key1_len, 1, "First key length should be 1");
    assert_eq!(bytes[8], b'a', "First key should be 'a'");
}

#[test]
fn test_frozen_collection_empty() {
    // Empty collections should just have count=0
    let empty_list = serialize_frozen_list(&[]).unwrap();
    let empty_set = serialize_frozen_set(&[]).unwrap();
    let empty_map = serialize_frozen_map(&[]).unwrap();

    // All should be exactly 4 bytes (i32 count = 0)
    assert_eq!(empty_list, vec![0, 0, 0, 0]);
    assert_eq!(empty_set, vec![0, 0, 0, 0]);
    assert_eq!(empty_map, vec![0, 0, 0, 0]);
}

#[test]
fn test_frozen_list_various_types() {
    // Test with different element types
    let text_list = vec![
        Value::Text("hello".to_string()),
        Value::Text("world".to_string()),
    ];
    let bytes = serialize_frozen_list(&text_list).unwrap();
    assert!(bytes.len() > 4, "Should have content beyond count");

    let bigint_list = vec![Value::BigInt(i64::MAX), Value::BigInt(i64::MIN)];
    let bytes = serialize_frozen_list(&bigint_list).unwrap();
    let count = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    assert_eq!(count, 2);
}

#[test]
fn test_serialize_collection_or_value_dispatches() {
    // Test that serialize_collection_or_value correctly dispatches
    let list = Value::List(vec![Value::Integer(42)]);
    let list_bytes = serialize_collection_or_value(&list).unwrap();
    let direct_bytes = serialize_frozen_list(&[Value::Integer(42)]).unwrap();
    assert_eq!(list_bytes, direct_bytes, "Should use frozen list format");

    let set = Value::Set(vec![Value::Integer(42)]);
    let set_bytes = serialize_collection_or_value(&set).unwrap();
    assert_eq!(set_bytes, direct_bytes, "Set format matches list format");

    let map = Value::Map(vec![(Value::Text("k".to_string()), Value::Integer(42))]);
    let map_bytes = serialize_collection_or_value(&map).unwrap();
    assert!(map_bytes.len() > 4, "Map should have content");
}

// =============================================================================
// Non-Frozen Collection Tests
// =============================================================================

#[test]
fn test_nonfrozen_list_timeuuid_paths() {
    let elements = vec![Value::Integer(100), Value::Integer(200)];
    let cells = serialize_nonfrozen_list(&elements).unwrap();

    assert_eq!(cells.len(), 2, "Should have 2 cells");

    // Each cell path should be a 16-byte TimeUUID
    assert_eq!(
        cells[0].path.len(),
        16,
        "Path should be TimeUUID (16 bytes)"
    );
    assert_eq!(
        cells[1].path.len(),
        16,
        "Path should be TimeUUID (16 bytes)"
    );

    // Paths should be different (unique TimeUUIDs via atomic counter)
    assert_ne!(cells[0].path, cells[1].path, "TimeUUIDs should be unique");

    // Values should be the serialized integers
    assert_eq!(cells[0].value, vec![0, 0, 0, 100]);
    assert_eq!(cells[1].value, vec![0, 0, 0, 200]);
}

#[test]
fn test_nonfrozen_set_element_as_path() {
    let elements = vec![
        Value::Text("alpha".to_string()),
        Value::Text("beta".to_string()),
    ];
    let cells = serialize_nonfrozen_set(&elements).unwrap();

    assert_eq!(cells.len(), 2);

    // For sets: path = element bytes, value = empty
    assert_eq!(cells[0].path, b"alpha");
    assert!(cells[0].value.is_empty());

    assert_eq!(cells[1].path, b"beta");
    assert!(cells[1].value.is_empty());
}

#[test]
fn test_nonfrozen_map_key_value_split() {
    let pairs = vec![
        (Value::Integer(1), Value::Text("one".to_string())),
        (Value::Integer(2), Value::Text("two".to_string())),
    ];
    let cells = serialize_nonfrozen_map(&pairs).unwrap();

    assert_eq!(cells.len(), 2);

    // For maps: path = key bytes, value = value bytes
    assert_eq!(cells[0].path, vec![0, 0, 0, 1]); // int 1
    assert_eq!(cells[0].value, b"one");

    assert_eq!(cells[1].path, vec![0, 0, 0, 2]); // int 2
    assert_eq!(cells[1].value, b"two");
}

#[test]
fn test_encode_collection_cell_format() {
    let cell = CollectionCell {
        path: vec![0x01, 0x02, 0x03, 0x04],
        value: vec![0xAA, 0xBB],
    };

    let encoded = encode_collection_cell(&cell);

    // Format: [VInt path_len][path][VInt value_len][value]
    // path_len = 4 (VInt = 0x04)
    // value_len = 2 (VInt = 0x02)
    assert_eq!(
        encoded,
        vec![
            0x04, // path length
            0x01, 0x02, 0x03, 0x04, // path
            0x02, // value length
            0xAA, 0xBB // value
        ]
    );
}

#[test]
fn test_nonfrozen_empty_collections() {
    let empty_list = serialize_nonfrozen_list(&[]).unwrap();
    let empty_set = serialize_nonfrozen_set(&[]).unwrap();
    let empty_map = serialize_nonfrozen_map(&[]).unwrap();

    // Empty non-frozen collections have no cells
    assert!(empty_list.is_empty());
    assert!(empty_set.is_empty());
    assert!(empty_map.is_empty());
}

// =============================================================================
// Cross-Type Tests
// =============================================================================

#[test]
fn test_nested_frozen_collection() {
    // Nested collection: list of lists
    let inner1 = Value::List(vec![Value::Integer(1), Value::Integer(2)]);
    let inner2 = Value::List(vec![Value::Integer(3), Value::Integer(4)]);

    // Serialize the inner lists first
    let inner1_bytes = serialize_collection_or_value(&inner1).unwrap();
    let _inner2_bytes = serialize_collection_or_value(&inner2).unwrap();

    // The bytes should be valid frozen list format
    let count1 = i32::from_be_bytes([
        inner1_bytes[0],
        inner1_bytes[1],
        inner1_bytes[2],
        inner1_bytes[3],
    ]);
    assert_eq!(count1, 2);
}

#[test]
fn test_blob_in_collection() {
    let elements = vec![
        Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]),
        Value::Blob(vec![0xCA, 0xFE]),
    ];

    let bytes = serialize_frozen_list(&elements).unwrap();

    // Verify count
    let count = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    assert_eq!(count, 2);

    // First blob: len=4, data=[0xDE, 0xAD, 0xBE, 0xEF]
    let len1 = i32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]) as usize;
    assert_eq!(len1, 4);
    assert_eq!(&bytes[8..12], &[0xDE, 0xAD, 0xBE, 0xEF]);
}

#[test]
fn test_timestamp_in_collection() {
    let elements = vec![
        Value::Timestamp(1704067200000), // 2024-01-01 00:00:00 UTC
        Value::Timestamp(1735689600000), // 2025-01-01 00:00:00 UTC
    ];

    let cells = serialize_nonfrozen_list(&elements).unwrap();

    // Timestamps are 8 bytes each
    assert_eq!(cells[0].value.len(), 8);
    assert_eq!(cells[1].value.len(), 8);

    // Verify first timestamp
    let ts1 = i64::from_be_bytes([
        cells[0].value[0],
        cells[0].value[1],
        cells[0].value[2],
        cells[0].value[3],
        cells[0].value[4],
        cells[0].value[5],
        cells[0].value[6],
        cells[0].value[7],
    ]);
    assert_eq!(ts1, 1704067200000);
}
