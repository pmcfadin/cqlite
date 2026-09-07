//! Unit tests for [`super`] — the marker-search SerializationHeader dispatcher,
//! its backtracking regular-column scanner and its ASCII fallback.
//!
//! Extracted from `serialization_header/mod.rs` under the campsite rule
//! (#1116/#1135): that file was already ~1060 lines, well over the 800-line
//! source target, so #4104's fail-closed change to the marker search could not
//! land there without tripping the `file-size` ratchet.

use super::*;

#[test]
fn test_serialization_header_with_no_clustering_keys() {
    // Test SerializationHeader with partition key and regular columns, no clustering keys
    // Format: [VInt partition_type_len] [0x00 0x00] [partition_type] [clustering_count=0] [0x00 0x00 column_count] [columns...]

    let mut test_data = vec![];

    // Partition key type: 41 bytes "(org.apache.cassandra.db.marshal.UUIDType"
    let partition_type = b"(org.apache.cassandra.db.marshal.UUIDType";
    test_data.extend_from_slice(&[0x00, 0x00]); // Marker
    test_data.push(partition_type.len() as u8);
    test_data.extend_from_slice(partition_type);

    // Clustering key count = 0
    test_data.push(0x00);

    // Regular columns section: separator (0x00) + count
    test_data.push(0x00); // section separator
    test_data.push(0x02); // column count

    // Column 1: "id" (UUID)
    test_data.push(0x02); // name length = 2
    test_data.extend_from_slice(b"id");
    test_data.push(0x28); // type length = 40
    test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.UUIDType");

    // Column 2: "name" (UTF8/text)
    test_data.push(0x04); // name length = 4
    test_data.extend_from_slice(b"name");
    test_data.push(0x28); // type length = 40
    test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.UTF8Type");

    // Add some garbage data before the SerializationHeader
    let mut full_data = vec![0xFF; 100];
    full_data.extend_from_slice(&test_data);

    let result = parse_serialization_header(&full_data);
    assert!(
        result.is_ok(),
        "Failed to parse SerializationHeader: {:?}",
        result.as_ref().err()
    );

    let (_remaining, (partition_types, clustering_types, columns)) = result.unwrap();

    // Verify partition key
    assert_eq!(partition_types.len(), 1, "Expected 1 partition key");
    assert!(partition_types[0].contains("UUIDType"));

    // Verify clustering keys (should be none)
    assert_eq!(clustering_types.len(), 0, "Expected 0 clustering keys");

    // Verify regular columns
    assert_eq!(columns.len(), 2, "Expected 2 columns");
    assert_eq!(columns[0].name, "id");
    assert_eq!(columns[0].column_type, "uuid");
    assert_eq!(columns[1].name, "name");
    assert_eq!(columns[1].column_type, "text");
}

#[test]
fn test_serialization_header_with_clustering_keys() {
    // Test SerializationHeader with partition key, 2 clustering keys, and regular columns

    let mut test_data = vec![];

    // Partition key type: 41 bytes
    let partition_type = b"(org.apache.cassandra.db.marshal.UUIDType";
    test_data.extend_from_slice(&[0x00, 0x00]); // Marker
    test_data.push(partition_type.len() as u8);
    test_data.extend_from_slice(partition_type);

    // Clustering key count = 2
    test_data.push(0x02);

    // Clustering key 1: ReversedType(TimestampType)
    let ck1 =
        b"[org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.TimestampType)";
    test_data.push(ck1.len() as u8);
    test_data.extend_from_slice(ck1);

    // Clustering key 2: UTF8Type
    let ck2 = b"(org.apache.cassandra.db.marshal.UTF8Type)";
    test_data.push(ck2.len() as u8);
    test_data.extend_from_slice(ck2);

    // Regular columns section
    test_data.push(0x00); // separator
    test_data.push(0x02); // count

    // Column 1: "data" (UTF8)
    test_data.push(0x04); // name length
    test_data.extend_from_slice(b"data");
    test_data.push(0x28); // type length
    test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.UTF8Type");

    // Column 2: "value" (Int32)
    test_data.push(0x05); // name length
    test_data.extend_from_slice(b"value");
    test_data.push(0x29); // type length
    test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.Int32Type");

    // Add garbage data before SerializationHeader
    let mut full_data = vec![0xFF; 100];
    full_data.extend_from_slice(&test_data);

    let result = parse_serialization_header(&full_data);
    assert!(
        result.is_ok(),
        "Failed to parse SerializationHeader with clustering keys: {:?}",
        result.err()
    );

    let (_remaining, (partition_types, clustering_types, columns)) = result.unwrap();

    // Verify partition key
    assert_eq!(partition_types.len(), 1);
    assert!(partition_types[0].contains("UUIDType"));

    // Verify clustering keys
    assert_eq!(clustering_types.len(), 2, "Expected 2 clustering keys");
    assert!(clustering_types[0].contains("ReversedType"));
    assert!(clustering_types[0].contains("TimestampType"));
    assert!(clustering_types[1].contains("UTF8Type"));

    // Verify regular columns
    assert_eq!(columns.len(), 2);
    assert_eq!(columns[0].name, "data");
    assert_eq!(columns[0].column_type, "text");
    assert_eq!(columns[1].name, "value");
    assert_eq!(columns[1].column_type, "int");
}

#[test]
fn test_serialization_header_with_static_columns() {
    // Test SerializationHeader with static columns (Issue #210)
    // Schema: partition key (uuid), clustering key (timestamp),
    //         static column (text), regular columns (text, int)

    let mut test_data = vec![];

    // Marker
    test_data.extend_from_slice(&[0x00, 0x00]);

    // Partition key type: UUIDType (40 bytes)
    let partition_type = b"org.apache.cassandra.db.marshal.UUIDType";
    test_data.push(partition_type.len() as u8);
    test_data.extend_from_slice(partition_type);

    // Clustering key count = 1
    test_data.push(0x01);

    // Clustering key 1: TimestampType (45 bytes)
    let ck1 = b"org.apache.cassandra.db.marshal.TimestampType";
    test_data.push(ck1.len() as u8);
    test_data.extend_from_slice(ck1);

    // Static column count = 1 (NOT a separator - this is the key fix!)
    test_data.push(0x01);

    // Static column 1: "static_data" (UTF8Type)
    test_data.push(0x0b); // name length = 11
    test_data.extend_from_slice(b"static_data");
    test_data.push(0x28); // type length = 40
    test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.UTF8Type");

    // Regular column count = 2
    test_data.push(0x02);

    // Regular column 1: "row_data" (UTF8)
    test_data.push(0x08); // name length
    test_data.extend_from_slice(b"row_data");
    test_data.push(0x28); // type length = 40
    test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.UTF8Type");

    // Regular column 2: "row_value" (Int32)
    test_data.push(0x09); // name length
    test_data.extend_from_slice(b"row_value");
    test_data.push(0x29); // type length = 41
    test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.Int32Type");

    // Add garbage data before SerializationHeader
    let mut full_data = vec![0xFF; 100];
    full_data.extend_from_slice(&test_data);

    let result = parse_serialization_header(&full_data);
    assert!(
        result.is_ok(),
        "Failed to parse SerializationHeader with static columns: {:?}",
        result.err()
    );

    let (_remaining, (partition_types, clustering_types, columns)) = result.unwrap();

    // Verify partition key
    assert_eq!(partition_types.len(), 1);
    assert!(partition_types[0].contains("UUIDType"));

    // Verify clustering keys
    assert_eq!(clustering_types.len(), 1);
    assert!(clustering_types[0].contains("TimestampType"));

    // Verify columns (static + regular = 3 total)
    assert_eq!(
        columns.len(),
        3,
        "Expected 3 columns (1 static + 2 regular)"
    );

    // Static column should be first and marked as static
    assert_eq!(columns[0].name, "static_data");
    assert_eq!(columns[0].column_type, "text");
    assert!(
        columns[0].is_static,
        "static_data should be marked as static"
    );

    // Regular columns should NOT be static
    assert_eq!(columns[1].name, "row_data");
    assert_eq!(columns[1].column_type, "text");
    assert!(
        !columns[1].is_static,
        "row_data should NOT be marked as static"
    );

    assert_eq!(columns[2].name, "row_value");
    assert_eq!(columns[2].column_type, "int");
    assert!(
        !columns[2].is_static,
        "row_value should NOT be marked as static"
    );
}

#[test]
fn test_partition_key_extraction_via_backtracking() {
    // Test the backtracking logic to extract partition key type before the column marker
    // This simulates the real ttl_test_table case where we have:
    // VInt(40) + "org.apache.cassandra.db.marshal.UUIDType" + 0x00 0x00 + [count]
    // Note: Real files use 2-byte VInt: 0x80 0x28 for length 40

    let mut test_data = vec![];

    // Add some garbage data before the partition key
    test_data.extend_from_slice(&[0xFF; 50]);

    // Partition key type: 40 bytes "org.apache.cassandra.db.marshal.UUIDType"
    test_data.extend_from_slice(&[0x80, 0x28]); // VInt: 40 (2-byte encoding)
    test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.UUIDType");

    // Marker: 0x00 0x00 followed by column count
    // NOTE: In SerializationHeader, partition keys are NOT in the regular columns section
    // Only regular (non-key) columns are listed here
    test_data.push(0x00); // separator
    test_data.push(0x02); // 2 regular columns

    // Regular Column 1: "expiring_value" (Int32)
    test_data.push(0x0E); // name length = 14
    test_data.extend_from_slice(b"expiring_value");
    test_data.push(0x29); // type length = 41
    test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.Int32Type");

    // Regular Column 2: "session_info" (UTF8)
    test_data.push(0x0C); // name length = 12
    test_data.extend_from_slice(b"session_info");
    test_data.push(0x28); // type length = 40
    test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.UTF8Type");

    // Parse the regular columns section which should extract partition key via backtracking
    let result = parse_regular_columns(&test_data);
    assert!(
        result.is_ok(),
        "Failed to parse columns with backtracking: {:?}",
        result.err()
    );

    let (_remaining, (partition_keys, columns)) = result.unwrap();

    // Verify partition key was extracted
    assert_eq!(
        partition_keys.len(),
        1,
        "Expected 1 partition key via backtracking"
    );
    assert_eq!(
        partition_keys[0],
        "org.apache.cassandra.db.marshal.UUIDType"
    );

    // Verify regular columns
    assert_eq!(columns.len(), 2, "Expected 2 regular columns");
    assert_eq!(columns[0].name, "expiring_value");
    assert_eq!(columns[0].column_type, "int");
    assert!(!columns[0].is_primary_key);
    assert_eq!(columns[1].name, "session_info");
    assert_eq!(columns[1].column_type, "text");
    assert!(!columns[1].is_primary_key);
}

#[test]
fn test_partition_key_extraction_with_longer_type() {
    // Test with a composite partition key type (longer type string)
    let mut test_data = vec![0xFF; 100]; // Garbage prefix

    // CompositeType with multiple components: 75 bytes
    let composite_type =
        "(org.apache.cassandra.db.marshal.CompositeType(UTF8Type,Int32Type,UUIDType)";
    let type_len = composite_type.len() as u8;

    // VInt encode the length (75 = 0x4B, fits in single byte)
    test_data.push(type_len);
    test_data.extend_from_slice(composite_type.as_bytes());

    // Marker + column count
    test_data.push(0x00); // separator
    test_data.push(0x01); // column count

    // Single column: "data" (UTF8)
    test_data.push(0x04);
    test_data.extend_from_slice(b"data");
    test_data.push(0x28);
    test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.UTF8Type");

    let result = parse_regular_columns(&test_data);
    assert!(result.is_ok(), "Failed to parse: {:?}", result.err());

    let (_remaining, (partition_keys, columns)) = result.unwrap();

    assert_eq!(partition_keys.len(), 1);
    assert_eq!(partition_keys[0], composite_type);

    // Expect 1 regular column
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0].name, "data");
    assert!(!columns[0].is_primary_key);
}

#[test]
fn test_backtracking_with_no_partition_key() {
    // Test case where there's no partition key before the marker
    // This should still parse columns successfully but return empty partition key list

    let mut test_data = vec![];

    // Just the marker and columns, no partition key type before
    test_data.push(0x00); // separator
    test_data.push(0x01); // count

    // Column: "name" (UTF8)
    test_data.push(0x04);
    test_data.extend_from_slice(b"name");
    test_data.push(0x28);
    test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.UTF8Type");

    let result = parse_regular_columns(&test_data);
    assert!(result.is_ok());

    let (_remaining, (partition_keys, columns)) = result.unwrap();

    assert_eq!(partition_keys.len(), 0, "Should have no partition keys");
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0].name, "name");
}

#[test]
fn test_backtracking_rejects_invalid_types() {
    // Test that backtracking rejects strings that don't match Cassandra type patterns
    let mut test_data = vec![0xFF; 50];

    // Invalid type: doesn't start with '(' and doesn't contain "org.apache.cassandra"
    test_data.push(0x15); // VInt: 21 bytes
    test_data.extend_from_slice(b"InvalidTypeDescriptor");

    // Marker + column count
    test_data.extend_from_slice(&[0x00, 0x00, 0x01]);

    // Column
    test_data.push(0x04);
    test_data.extend_from_slice(b"test");
    test_data.push(0x28);
    test_data.extend_from_slice(b"org.apache.cassandra.db.marshal.UTF8Type");

    let result = parse_regular_columns(&test_data);
    assert!(result.is_ok());

    let (_remaining, (partition_keys, _columns)) = result.unwrap();

    // Should not extract the invalid type
    assert_eq!(
        partition_keys.len(),
        0,
        "Should reject invalid type pattern"
    );
}
