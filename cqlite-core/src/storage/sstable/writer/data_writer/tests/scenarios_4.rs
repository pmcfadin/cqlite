//! data_writer tests, group 4/6 (issue #1118 split).
//! Relocated verbatim from the original inline `mod tests`.

#![allow(unused_imports)]

use super::super::*;
use super::support::*;
use crate::schema::{ClusteringColumn, ClusteringOrder, Column, CqlType, KeyColumn, TableSchema};
use crate::storage::serialization::types::TypeSerializer;
use crate::storage::write_engine::mutation::{CellOperation, ClusteringKey, PartitionKey, TableId};
use crate::types::UdtValue;
use std::collections::HashMap;

#[test]
fn test_is_complex_column() {
    // Non-frozen collections ARE complex (CQL syntax)
    assert!(is_complex_column("set<int>"));
    assert!(is_complex_column("list<text>"));
    assert!(is_complex_column("map<text, int>"));
    assert!(is_complex_column("SET<INT>"));
    assert!(is_complex_column("List<Text>"));
    assert!(is_complex_column("Map<Text, Int>"));

    // Non-frozen collections ARE complex (Cassandra internal syntax)
    assert!(is_complex_column(
        "org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.Int32Type)"
    ));
    assert!(is_complex_column(
        "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.UTF8Type)"
    ));
    assert!(is_complex_column(
            "org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.Int32Type)"
        ));

    // Frozen collections are NOT complex (CQL syntax)
    assert!(!is_complex_column("frozen<set<int>>"));
    assert!(!is_complex_column("frozen<list<text>>"));
    assert!(!is_complex_column("frozen<map<text, int>>"));
    assert!(!is_complex_column("FROZEN<SET<INT>>"));

    // Frozen collections are NOT complex (Cassandra internal syntax)
    assert!(!is_complex_column(
            "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.Int32Type))"
        ));

    // Primitives are NOT complex
    assert!(!is_complex_column("int"));
    assert!(!is_complex_column("text"));
    assert!(!is_complex_column("uuid"));
    assert!(!is_complex_column("timestamp"));

    // Issue #927: a TOP-LEVEL non-frozen UDT IS complex.
    assert!(is_complex_column(
            "org.apache.cassandra.db.marshal.UserType(ks,61,62:org.apache.cassandra.db.marshal.UTF8Type)"
        ));
    // A frozen UDT is NOT complex (single-cell).
    assert!(!is_complex_column(
            "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.UserType(ks,61,62:org.apache.cassandra.db.marshal.UTF8Type))"
        ));
    // A bare CQL UDT name is NOT detected here (needs a UdtRegistry — issue
    // #927 item 4); `frozen<addr>` is likewise not complex.
    assert!(!is_complex_column("address_type"));
    assert!(!is_complex_column("frozen<address_type>"));
}

#[test]
fn test_generate_list_cell_path_timeuuid() {
    let ts = 1_704_067_200_000_000i64; // 2024-01-01 00:00:00 UTC

    let uuid0 = generate_list_cell_path_timeuuid(ts, 0);
    let uuid1 = generate_list_cell_path_timeuuid(ts, 1);
    let uuid2 = generate_list_cell_path_timeuuid(ts, 2);

    // All should be 16 bytes
    assert_eq!(uuid0.len(), 16);
    assert_eq!(uuid1.len(), 16);

    // Version bits should be 1 (0x1X in byte 6)
    assert_eq!(uuid0[6] & 0xF0, 0x10, "Should be UUID version 1");
    assert_eq!(uuid1[6] & 0xF0, 0x10, "Should be UUID version 1");

    // UUIDs should be monotonically increasing (as byte arrays)
    assert!(uuid0 < uuid1, "UUID0 should be less than UUID1");
    assert!(uuid1 < uuid2, "UUID1 should be less than UUID2");
}

#[test]
fn test_write_set_complex_column() {
    let stats = create_test_stats();
    let writer = DataWriter::new(stats);

    let column = Column {
        name: "tags".to_string(),
        data_type: "set<text>".to_string(),
        nullable: true,
        default: None,
        is_static: false,
    };

    let value = Value::Set(vec![
        Value::Text("alpha".to_string()),
        Value::Text("beta".to_string()),
    ]);

    let mut buf = Vec::new();
    writer
        .write_complex_column(&mut buf, &column, &value, 1001000, None)
        .unwrap();

    assert!(!buf.is_empty());

    // Structurally parse cell flags so DeletionTime.LIVE header bytes
    // (which can coincide with flag values) are not misidentified.
    let expected_cell_flags = CELL_USE_ROW_TIMESTAMP | CELL_HAS_EMPTY_VALUE;
    let cell_flags = parse_complex_cell_flags(&buf);
    assert_eq!(cell_flags.len(), 2, "Should have 2 SET cells");
    assert!(
        cell_flags.iter().all(|&f| f == expected_cell_flags),
        "Should have 2 SET cells with USE_ROW_TIMESTAMP | HAS_EMPTY_VALUE flags, got: {:?}",
        cell_flags
    );
}

#[test]
fn test_write_map_complex_column() {
    let stats = create_test_stats();
    let writer = DataWriter::new(stats);

    let column = Column {
        name: "props".to_string(),
        data_type: "map<text, int>".to_string(),
        nullable: true,
        default: None,
        is_static: false,
    };

    let value = Value::Map(vec![
        (Value::Text("key1".to_string()), Value::Integer(100)),
        (Value::Text("key2".to_string()), Value::Integer(200)),
    ]);

    let mut buf = Vec::new();
    writer
        .write_complex_column(&mut buf, &column, &value, 1001000, None)
        .unwrap();

    assert!(!buf.is_empty());

    // MAP cells have USE_ROW_TIMESTAMP (0x08) but NOT HAS_EMPTY_VALUE.
    // Use structural parse so DeletionTime.LIVE header bytes are not misidentified.
    let cell_flags = parse_complex_cell_flags(&buf);
    assert_eq!(cell_flags.len(), 2, "Should have 2 MAP cells");
    assert!(
        cell_flags.iter().all(|&f| f == CELL_USE_ROW_TIMESTAMP),
        "Should have 2 MAP cells with USE_ROW_TIMESTAMP flags, got: {:?}",
        cell_flags
    );
}

#[test]
fn test_write_list_complex_column() {
    let stats = create_test_stats();
    let writer = DataWriter::new(stats);

    let column = Column {
        name: "items".to_string(),
        data_type: "list<int>".to_string(),
        nullable: true,
        default: None,
        is_static: false,
    };

    let value = Value::List(vec![Value::Integer(10), Value::Integer(20)]);

    let mut buf = Vec::new();
    writer
        .write_complex_column(&mut buf, &column, &value, 1001000, None)
        .unwrap();

    assert!(!buf.is_empty());

    // LIST cells have USE_ROW_TIMESTAMP (0x08) and 16-byte TimeUUID paths.
    // Use structural parse so DeletionTime.LIVE header bytes are not misidentified.
    let cell_flags = parse_complex_cell_flags(&buf);
    assert_eq!(cell_flags.len(), 2, "Should have 2 LIST cells");
    assert!(
        cell_flags.iter().all(|&f| f == CELL_USE_ROW_TIMESTAMP),
        "Should have 2 LIST cells with USE_ROW_TIMESTAMP flags, got: {:?}",
        cell_flags
    );
    // The TimeUUID path length (16) is structurally verified by parse_complex_cell_flags
    // successfully parsing each cell's path — if path_len were wrong, parsing would
    // overshoot or the cell count would be wrong.
}

#[test]
fn test_frozen_collection_not_complex() {
    // Frozen collections should still use simple cell (serialize_value), not complex column
    let schema = TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![Column {
            name: "frozen_tags".to_string(),
            data_type: "frozen<set<text>>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        }],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let stats = create_test_stats();
    let mut writer = DataWriter::new(stats);

    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(1));
    let mutation = Mutation::new(
        table_id,
        pk,
        None,
        vec![CellOperation::Write {
            column: "frozen_tags".to_string(),
            value: Value::Frozen(Box::new(Value::Set(vec![
                Value::Text("a".to_string()),
                Value::Text("b".to_string()),
            ]))),
        }],
        1001000,
        None,
    );

    writer.write_row(&mutation, &schema).unwrap();

    let bytes = writer.finish().unwrap();
    assert!(!bytes.is_empty());

    // Frozen collection should NOT have HAS_COMPLEX_DELETION flag
    let flags = bytes[0];
    assert_eq!(
        flags & ROW_HAS_COMPLEX_DELETION,
        0,
        "Frozen collection should NOT have HAS_COMPLEX_DELETION flag"
    );
}

#[test]
fn test_frozen_set_sorted_by_serialized_element_bytes() {
    // Issue #1254: Cassandra SetType is a sorted collection. A frozen<set<int>>
    // written with REVERSED elements must serialize in unsigned serialized-byte
    // order. serialize_value is the exact path a frozen collection cell takes
    // (cells.rs -> serialize_value). Assert the FULL serialized blob, not a count.
    let value = Value::Frozen(Box::new(Value::Set(vec![
        Value::Integer(3),
        Value::Integer(2),
        Value::Integer(1),
    ])));

    let bytes = serialize_value(&value).unwrap();

    // Wire format: [count i32][len i32][value]... — int values are 4 big-endian bytes.
    let mut expected = Vec::new();
    expected.extend_from_slice(&3i32.to_be_bytes()); // count
    for n in [1i32, 2, 3] {
        // sorted ascending
        expected.extend_from_slice(&4i32.to_be_bytes());
        expected.extend_from_slice(&n.to_be_bytes());
    }
    assert_eq!(
        bytes, expected,
        "frozen<set<int>> must serialize elements in sorted-byte order"
    );
}

#[test]
fn test_frozen_map_sorted_by_serialized_key_bytes() {
    // Issue #1254: Cassandra MapType is a sorted collection. A frozen<map<text,int>>
    // written with REVERSED keys must serialize entries in unsigned serialized
    // key-byte order. Assert the FULL serialized blob.
    let value = Value::Frozen(Box::new(Value::Map(vec![
        (Value::Text("c".to_string()), Value::Integer(30)),
        (Value::Text("b".to_string()), Value::Integer(20)),
        (Value::Text("a".to_string()), Value::Integer(10)),
    ])));

    let bytes = serialize_value(&value).unwrap();

    // Wire format: [count i32]([klen i32][key][vlen i32][val])...
    // Text key bytes are raw UTF-8; int values are 4 big-endian bytes.
    let mut expected = Vec::new();
    expected.extend_from_slice(&3i32.to_be_bytes());
    for (k, v) in [("a", 10i32), ("b", 20), ("c", 30)] {
        // sorted ascending by key
        let kb = k.as_bytes();
        expected.extend_from_slice(&(kb.len() as i32).to_be_bytes());
        expected.extend_from_slice(kb);
        expected.extend_from_slice(&4i32.to_be_bytes());
        expected.extend_from_slice(&v.to_be_bytes());
    }
    assert_eq!(
        bytes, expected,
        "frozen<map<text,int>> must serialize entries in sorted key-byte order"
    );
}

#[test]
fn test_frozen_map_row_emits_sorted_bytes_end_to_end() {
    // Issue #1254 AC#3: byte-for-byte check that a full written row for a
    // multi-entry non-UDT frozen<map<text,int>> contains the map blob in sorted
    // key order — the same bytes Cassandra's MapType produces — when fed reversed.
    let schema = TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![Column {
            name: "fm".to_string(),
            data_type: "frozen<map<text, int>>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        }],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let stats = create_test_stats();
    let mut writer = DataWriter::new(stats);

    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(1));
    let mutation = Mutation::new(
        table_id,
        pk,
        None,
        vec![CellOperation::Write {
            column: "fm".to_string(),
            value: Value::Frozen(Box::new(Value::Map(vec![
                (Value::Text("c".to_string()), Value::Integer(30)),
                (Value::Text("b".to_string()), Value::Integer(20)),
                (Value::Text("a".to_string()), Value::Integer(10)),
            ]))),
        }],
        1001000,
        None,
    );

    writer.write_row(&mutation, &schema).unwrap();
    let bytes = writer.finish().unwrap();

    // The exact frozen-map blob the cell must carry (sorted a,b,c).
    let mut sorted_blob = Vec::new();
    sorted_blob.extend_from_slice(&3i32.to_be_bytes());
    for (k, v) in [("a", 10i32), ("b", 20), ("c", 30)] {
        let kb = k.as_bytes();
        sorted_blob.extend_from_slice(&(kb.len() as i32).to_be_bytes());
        sorted_blob.extend_from_slice(kb);
        sorted_blob.extend_from_slice(&4i32.to_be_bytes());
        sorted_blob.extend_from_slice(&v.to_be_bytes());
    }
    let reversed_blob = {
        let mut b = Vec::new();
        b.extend_from_slice(&3i32.to_be_bytes());
        for (k, v) in [("c", 30i32), ("b", 20), ("a", 10)] {
            let kb = k.as_bytes();
            b.extend_from_slice(&(kb.len() as i32).to_be_bytes());
            b.extend_from_slice(kb);
            b.extend_from_slice(&4i32.to_be_bytes());
            b.extend_from_slice(&v.to_be_bytes());
        }
        b
    };

    let contains = |hay: &[u8], needle: &[u8]| hay.windows(needle.len()).any(|w| w == needle);
    assert!(
        contains(&bytes, &sorted_blob),
        "written row must contain the frozen map in sorted key-byte order"
    );
    assert!(
        !contains(&bytes, &reversed_blob),
        "written row must NOT contain the frozen map in input (reversed) order"
    );
}

#[test]
fn test_mixed_simple_and_complex_columns() {
    let schema = TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "tags".to_string(),
                data_type: "set<text>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let stats = create_test_stats();
    let mut writer = DataWriter::new(stats);

    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(1));
    let mutation = Mutation::new(
        table_id,
        pk,
        None,
        vec![
            CellOperation::Write {
                column: "name".to_string(),
                value: Value::Text("Alice".to_string()),
            },
            CellOperation::Write {
                column: "tags".to_string(),
                value: Value::Set(vec![
                    Value::Text("admin".to_string()),
                    Value::Text("user".to_string()),
                ]),
            },
        ],
        1001000,
        None,
    );

    writer.write_row(&mutation, &schema).unwrap();

    let bytes = writer.finish().unwrap();
    assert!(!bytes.is_empty());

    // Row should have HAS_COMPLEX_DELETION flag because of the SET column
    let flags = bytes[0];
    assert_eq!(
        flags & ROW_HAS_COMPLEX_DELETION,
        ROW_HAS_COMPLEX_DELETION,
        "Row with non-frozen SET should have HAS_COMPLEX_DELETION flag"
    );
    assert_eq!(
        flags & ROW_HAS_TIMESTAMP,
        ROW_HAS_TIMESTAMP,
        "Should have timestamp"
    );
    assert_eq!(
        flags & ROW_HAS_ALL_COLUMNS,
        ROW_HAS_ALL_COLUMNS,
        "Should have all columns"
    );
}

#[test]
fn test_set_canonical_ordering() {
    // Elements provided out of order should be sorted by serialized bytes
    let stats = create_test_stats();
    let writer = DataWriter::new(stats);

    let column = Column {
        name: "tags".to_string(),
        data_type: "set<text>".to_string(),
        nullable: true,
        default: None,
        is_static: false,
    };

    // Input: zebra, alpha, mango (unsorted)
    let value = Value::Set(vec![
        Value::Text("zebra".to_string()),
        Value::Text("alpha".to_string()),
        Value::Text("mango".to_string()),
    ]);

    let mut buf = Vec::new();
    writer
        .write_complex_column(&mut buf, &column, &value, 1001000, None)
        .unwrap();

    // Extract cell paths from the binary output.
    // After complex deletion (2 VInts) and cell count (1 VInt), each cell is:
    //   flags(1) + path_len(VInt) + path_bytes
    // Find the text values in order by scanning for ASCII strings.
    let buf_str = String::from_utf8_lossy(&buf);
    let alpha_pos = buf_str.find("alpha").expect("alpha should be in output");
    let mango_pos = buf_str.find("mango").expect("mango should be in output");
    let zebra_pos = buf_str.find("zebra").expect("zebra should be in output");

    assert!(
        alpha_pos < mango_pos && mango_pos < zebra_pos,
        "SET elements should be in sorted order: alpha({}) < mango({}) < zebra({})",
        alpha_pos,
        mango_pos,
        zebra_pos
    );
}

#[test]
fn test_map_canonical_ordering() {
    let stats = create_test_stats();
    let writer = DataWriter::new(stats);

    let column = Column {
        name: "props".to_string(),
        data_type: "map<text, int>".to_string(),
        nullable: true,
        default: None,
        is_static: false,
    };

    // Input: keys out of order (z_key, a_key)
    let value = Value::Map(vec![
        (Value::Text("z_key".to_string()), Value::Integer(1)),
        (Value::Text("a_key".to_string()), Value::Integer(2)),
    ]);

    let mut buf = Vec::new();
    writer
        .write_complex_column(&mut buf, &column, &value, 1001000, None)
        .unwrap();

    let buf_str = String::from_utf8_lossy(&buf);
    let a_pos = buf_str.find("a_key").expect("a_key should be in output");
    let z_pos = buf_str.find("z_key").expect("z_key should be in output");

    assert!(
        a_pos < z_pos,
        "MAP entries should be sorted by key: a_key({}) < z_key({})",
        a_pos,
        z_pos
    );
}

#[test]
fn test_set_rejects_list_value() {
    let stats = create_test_stats();
    let writer = DataWriter::new(stats);

    let column = Column {
        name: "tags".to_string(),
        data_type: "set<text>".to_string(),
        nullable: true,
        default: None,
        is_static: false,
    };

    // Pass a List value to a SET column — should be rejected
    let value = Value::List(vec![Value::Text("x".to_string())]);
    let mut buf = Vec::new();
    let result = writer.write_complex_column(&mut buf, &column, &value, 1001000, None);
    assert!(result.is_err(), "SET column should reject Value::List");
}

#[test]
fn test_list_rejects_set_value() {
    let stats = create_test_stats();
    let writer = DataWriter::new(stats);

    let column = Column {
        name: "items".to_string(),
        data_type: "list<text>".to_string(),
        nullable: true,
        default: None,
        is_static: false,
    };

    // Pass a Set value to a LIST column — should be rejected
    let value = Value::Set(vec![Value::Text("x".to_string())]);
    let mut buf = Vec::new();
    let result = writer.write_complex_column(&mut buf, &column, &value, 1001000, None);
    assert!(result.is_err(), "LIST column should reject Value::Set");
}

#[test]
fn test_complex_column_deletion() {
    let stats = create_test_stats();
    let writer = DataWriter::new(stats);

    let mut buf = Vec::new();
    // Issue #764: the caller now supplies the local_deletion_time explicitly.
    writer
        .write_complex_column_deletion(&mut buf, 1001000, 42)
        .unwrap();

    assert!(!buf.is_empty());

    // Should contain: marked_for_delete_at delta + local_deletion_time delta + cell_count(0)
    // The last byte should be 0x00 (cell_count = 0 encoded as unsigned VInt)
    assert_eq!(
        buf[buf.len() - 1],
        0x00,
        "Last byte should be cell_count = 0"
    );
}

#[test]
fn test_complex_column_deletion_rejects_ldt_below_baseline() {
    // Issue #764: an explicit local_deletion_time below min_local_deletion_time
    // must be rejected, not silently wrapped into a corrupt unsigned VInt.
    let mut stats = create_test_stats();
    stats.min_local_deletion_time = 100;
    let writer = DataWriter::new(stats);

    let mut buf = Vec::new();
    let result = writer.write_complex_column_deletion(&mut buf, 1001000, 50);
    assert!(
        result.is_err(),
        "LDT below baseline must be rejected to avoid VInt wrap corruption"
    );
}

/// Issue #873: a ROW tombstone whose explicit localDeletionTime is below the
/// Statistics baseline (in normal, non-negative i32 time space) must be
/// rejected loudly rather than silently wrapping the unsigned LDT delta into
/// a huge VInt — which would over-count the row body and corrupt Data.db.
/// This mirrors the complex-deletion guard above.
#[test]
fn test_row_tombstone_rejects_ldt_below_baseline() {
    use crate::storage::write_engine::mutation::{CellOperation, Mutation, PartitionKey, TableId};

    let schema = create_test_schema();
    let mut stats = create_test_stats();
    stats.min_timestamp = 0;
    stats.min_local_deletion_time = 1_700_000_000; // baseline well above the delete's LDT
    let mut writer = DataWriter::new(stats);

    // A row tombstone (DeleteRow) whose explicit LDT (50s) is far below the
    // baseline. `effective_local_deletion_time()` honors the explicit value.
    let mutation = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![CellOperation::DeleteRow],
        1_000_000, // deletion timestamp (micros)
        None,
    )
    .with_local_deletion_time(50);

    let key = mutation
        .decorated_key(&schema)
        .expect("decorated key must build");
    let result = writer.write_partition(&key, &[mutation], &schema, None, &[]);
    assert!(
        result.is_err(),
        "a below-baseline row-tombstone LDT must be rejected to avoid VInt wrap corruption"
    );
}

/// Companion to the guard test: a row tombstone with a far-future LDT in
/// [2^31, 2^32) (negative i32 bit pattern) is a LEGITIMATE value and must NOT
/// be rejected — the wrapping i32 arithmetic is intended there (#853/#873).
#[test]
fn test_row_tombstone_far_future_ldt_is_accepted() {
    use crate::storage::write_engine::mutation::{CellOperation, Mutation, PartitionKey, TableId};

    let schema = create_test_schema();
    let mut stats = create_test_stats();
    stats.min_timestamp = 0;
    stats.min_local_deletion_time = 0; // common DeletionTime.LIVE-derived baseline
    let mut writer = DataWriter::new(stats);

    let far_future_ldt = (1u32 << 31) as i32; // negative i32, value 2^31
    let mutation = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(2)),
        None,
        vec![CellOperation::DeleteRow],
        1_000_000,
        None,
    )
    .with_local_deletion_time(far_future_ldt);

    let key = mutation
        .decorated_key(&schema)
        .expect("decorated key must build");
    writer
        .write_partition(&key, &[mutation], &schema, None, &[])
        .expect("a far-future row-tombstone LDT must be accepted, not rejected");
}

/// Issue #853: a complex-deletion marker whose localDeletionTime lands in
/// [2^31, 2^32) (far future, ~2038-2106) must encode the LDT delta with the
/// SAME i32 cast + wrapping that Cassandra's DeletionTime.serialize uses, so the
/// number of bytes written equals the size the row-size vint accounts for. The
/// previous i64-widened path both rejected these values and would have produced
/// a divergent byte count.
#[test]
fn test_complex_column_deletion_far_future_ldt_size_matches_written() {
    use crate::parser::vint::parse_vuint;

    // min baseline of 0 (DeletionTime.LIVE-derived stats min), the common case.
    let stats = create_test_stats();
    assert_eq!(stats.min_local_deletion_time, 0);
    let writer = DataWriter::new(stats);

    // Boundary 2^31 and a high value near 2^32 - 1, both representable only as
    // negative i32 bit patterns.
    let far_future: [u32; 3] = [1u32 << 31, (1u32 << 31) + 12345, u32::MAX - 1];

    for raw in far_future {
        let ldt = raw as i32; // negative i32 bit pattern for [2^31, 2^32)
        assert!(
            ldt < 0,
            "value {raw} must be a negative i32 in [2^31, 2^32)"
        );

        let mut buf = Vec::new();
        writer
            .write_complex_column_deletion(&mut buf, 1_001_000, ldt)
            .expect("far-future complex deletion must be accepted, not rejected");

        // Skip the markedForDeleteAt VInt (timestamp delta) to reach the LDT delta.
        // parse_vuint is a nom parser: Ok((remaining, value)).
        let (ldt_bytes, _ts_delta) = parse_vuint(&buf).expect("markedForDeleteAt VInt must decode");

        // The encoded LDT delta must equal the i32-wrapping u32 value Cassandra
        // would write: localDeletionTime - minLocalDeletionTime in 32-bit space.
        let expected_delta = ldt.wrapping_sub(0) as u32; // min = 0
        assert_eq!(
            expected_delta, raw,
            "delta must equal the raw far-future value"
        );

        let (rest, decoded_delta) = parse_vuint(ldt_bytes).expect("LDT delta VInt must decode");
        assert_eq!(
            decoded_delta, expected_delta as u64,
            "round-tripped LDT delta must match the i32-wrapping value for raw={raw}"
        );

        // SIZE == WRITTEN: the bytes consumed by the LDT delta VInt must equal
        // the canonical unsigned_len of that delta (no over/under-count), and the
        // only remaining byte is the cell_count(0).
        let ldt_vint_len = ldt_bytes.len() - rest.len();
        assert_eq!(
            ldt_vint_len,
            unsigned_len(expected_delta as u64),
            "encoded LDT delta size must equal bytes written for raw={raw}"
        );
        assert_eq!(
            rest,
            &[0u8],
            "trailing byte must be cell_count = 0 for raw={raw}"
        );
    }
}

/// Branch-review (#853/#889): a range-tombstone marker whose localDeletionTime
/// lands in [2^31, 2^32) (far future, ~2038-2106) must encode the LDT delta with
/// the SAME i32 cast + wrapping that Cassandra's DeletionTime.serialize uses, so
/// the bytes written equal the size the marker_body_size vint accounts for. The
/// previous i64-widened path produced a 64-bit wrapped delta with a divergent
/// byte count (and a corrupted body_size vint).
#[test]
fn test_range_tombstone_far_future_ldt_size_matches_written() {
    use crate::parser::vint::parse_vuint;

    // min baseline of 0 (DeletionTime.LIVE-derived stats min), the common case.
    let mut stats = create_test_stats();
    stats.min_timestamp = 0;
    stats.min_local_deletion_time = 0;
    assert_eq!(stats.min_local_deletion_time, 0);
    let schema = create_test_schema();

    // Boundary 2^31 and a high value near 2^32 - 1, both representable only as
    // negative i32 bit patterns.
    let far_future: [u32; 3] = [1u32 << 31, (1u32 << 31) + 12345, u32::MAX - 1];

    for raw in far_future {
        let ldt = raw as i32; // negative i32 bit pattern for [2^31, 2^32)
        assert!(
            ldt < 0,
            "value {raw} must be a negative i32 in [2^31, 2^32)"
        );

        let mut writer = DataWriter::new(stats.clone());
        // Bottom bound: an inclusive start with zero clustering values, keeping
        // the marker framing minimal.
        let prev_size = 0u64;
        let written = writer
            .write_range_bound(
                &ClusteringBound::Bottom,
                /* is_open */ true,
                /* deletion_time */ 1_001_000,
                ldt,
                &schema,
                prev_size,
            )
            .expect("far-future range tombstone must be accepted, not rejected");

        // SIZE == WRITTEN: the returned marker size must equal the buffer growth.
        assert_eq!(
            written,
            writer.buffer.len(),
            "returned marker size must equal bytes written for raw={raw}"
        );

        // Walk the marker layout to reach the deletion-time VInts:
        //   [IS_MARKER][bound_kind][cluster_count u16=0][body_size vuint]
        //   [prev_size vuint][ts_delta vuint][ldt_delta vuint]
        let buf = &writer.buffer;
        assert_eq!(buf[0], IS_MARKER, "first byte must be IS_MARKER");
        assert_eq!(buf[1], INCL_START_BOUND, "Bottom open bound kind");
        assert_eq!(&buf[2..4], &[0u8, 0u8], "cluster_count u16 = 0 for Bottom");

        let after_count = &buf[4..];
        let (after_body_size, body_size) =
            parse_vuint(after_count).expect("body_size VInt must decode");
        let body_start_remaining = after_body_size.len();
        let (after_prev, _prev) = parse_vuint(after_body_size).expect("prev_size VInt must decode");
        let (after_ts, _ts_delta) =
            parse_vuint(after_prev).expect("markedForDeleteAt VInt must decode");
        let (rest, decoded_ldt) = parse_vuint(after_ts).expect("LDT delta VInt must decode");

        // The encoded LDT delta must equal the i32-wrapping u32 value Cassandra
        // would write: localDeletionTime - minLocalDeletionTime in 32-bit space.
        let expected_delta = ldt.wrapping_sub(0) as u32; // min = 0
        assert_eq!(
            expected_delta, raw,
            "delta must equal the raw far-future value for raw={raw}"
        );
        assert_eq!(
            decoded_ldt, expected_delta as u64,
            "round-tripped LDT delta must match the i32-wrapping value for raw={raw}"
        );

        // body_size must exactly account for prev_size + ts_delta + ldt_delta:
        // the bytes from the start of prev_size to the end of the marker.
        assert!(
            rest.is_empty(),
            "marker must end after LDT delta for raw={raw}"
        );
        assert_eq!(
            body_size as usize, body_start_remaining,
            "body_size vint must equal bytes of (prev_size + deletion times) for raw={raw}"
        );
    }
}

/// Issue #853: the same far-future marker, written inside a full row, must keep
/// the row-size vint exactly equal to the row-body bytes that follow it. A
/// schema with no clustering key keeps the framing simple: after the row-flags
/// byte the next bytes are the row-size vint itself.
#[test]
fn test_complex_deletion_far_future_row_size_vint_matches_body() {
    use crate::parser::vint::parse_vuint;

    let schema = TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "tags".to_string(),
                data_type: "set<text>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    // Boundary 2^31 and a high value near 2^32 - 1, both negative i32 patterns.
    for raw in [1u32 << 31, u32::MAX - 1] {
        let ldt = raw as i32;
        let mut writer = DataWriter::new(create_test_stats());

        let table_id = TableId::new("test_ks", "test_table");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let mutation = Mutation::new(
            table_id,
            pk,
            None,
            vec![CellOperation::Delete {
                column: "tags".to_string(),
                local_deletion_time: None,
            }],
            2_000_000,
            None,
        )
        .with_local_deletion_time(ldt);

        writer
            .write_row(&mutation, &schema)
            .expect("far-future complex-deletion row must write, not error");
        let out = writer.finish().expect("finish");

        // out = [row_flags u8][row_size vint][prev_size vint][body...].
        // (no clustering key, so nothing between flags and row_size.)
        assert!(!out.is_empty(), "row must be written for raw={raw}");
        let after_flags = &out[1..];
        let (body_after_size, row_size) =
            parse_vuint(after_flags).expect("row-size vint must decode");

        // Size == written: the row-size vint must exactly account for the body
        // bytes that follow it (a divergent far-future LDT byte count would make
        // this mismatch and corrupt the row framing).
        assert_eq!(
            row_size as usize,
            body_after_size.len(),
            "row-size vint must equal the row-body bytes written for raw={raw}"
        );
    }
}

#[test]
fn test_write_with_ttl_complex_column() {
    // WriteWithTtl on a complex column should use complex format, not simple cell
    let schema = TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![Column {
            name: "tags".to_string(),
            data_type: "set<text>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        }],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let stats = create_test_stats();
    let mut writer = DataWriter::new(stats);

    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(1));
    let mutation = Mutation::new(
        table_id,
        pk,
        None,
        vec![CellOperation::WriteWithTtl {
            column: "tags".to_string(),
            value: Value::Set(vec![
                Value::Text("a".to_string()),
                Value::Text("b".to_string()),
            ]),
            ttl_seconds: 3600,
            local_deletion_time: None,
        }],
        1001000,
        None,
    );

    // Should succeed without error — complex format should be used
    writer.write_row(&mutation, &schema).unwrap();

    let bytes = writer.finish().unwrap();
    assert!(!bytes.is_empty());

    // Should have HAS_COMPLEX_DELETION flag
    let flags = bytes[0];
    assert_eq!(
        flags & ROW_HAS_COMPLEX_DELETION,
        ROW_HAS_COMPLEX_DELETION,
        "WriteWithTtl on SET should set HAS_COMPLEX_DELETION"
    );
}

#[test]
fn test_delete_complex_column() {
    // Delete on a complex column should write complex deletion, not simple tombstone
    let schema = TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![Column {
            name: "tags".to_string(),
            data_type: "set<text>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        }],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let stats = create_test_stats();
    let mut writer = DataWriter::new(stats);

    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(1));
    let mutation = Mutation::new(
        table_id,
        pk,
        None,
        vec![CellOperation::Delete {
            column: "tags".to_string(),
            local_deletion_time: None,
        }],
        1001000,
        None,
    );

    // Should succeed — uses complex deletion format
    writer.write_row(&mutation, &schema).unwrap();

    let bytes = writer.finish().unwrap();
    assert!(!bytes.is_empty());

    // Should have HAS_COMPLEX_DELETION flag
    let flags = bytes[0];
    assert_eq!(
        flags & ROW_HAS_COMPLEX_DELETION,
        ROW_HAS_COMPLEX_DELETION,
        "Delete on SET should set HAS_COMPLEX_DELETION"
    );
}

#[test]
fn test_internal_type_string_complex_column() {
    // Cassandra internal type strings should be recognized as complex
    let schema = TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![Column {
            name: "tags".to_string(),
            data_type:
                "org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UTF8Type)"
                    .to_string(),
            nullable: true,
            default: None,
            is_static: false,
        }],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let stats = create_test_stats();
    let mut writer = DataWriter::new(stats);

    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(1));
    let mutation = Mutation::new(
        table_id,
        pk,
        None,
        vec![CellOperation::Write {
            column: "tags".to_string(),
            value: Value::Set(vec![Value::Text("test".to_string())]),
        }],
        1001000,
        None,
    );

    writer.write_row(&mutation, &schema).unwrap();

    let bytes = writer.finish().unwrap();
    let flags = bytes[0];
    assert_eq!(
        flags & ROW_HAS_COMPLEX_DELETION,
        ROW_HAS_COMPLEX_DELETION,
        "Internal type string should be recognized as complex column"
    );
}

#[test]
fn test_set_complex_column_with_ttl() {
    // SET with TTL should write IS_EXPIRING flag per cell, not USE_ROW_TIMESTAMP.
    // Uses structural parsing to read cell flags at their exact byte positions,
    // avoiding false positives from time-derived LDT bytes that can equal 0x02.
    let stats = create_test_stats();
    let writer = DataWriter::new(stats);

    let column = Column {
        name: "tags".to_string(),
        data_type: "set<text>".to_string(),
        nullable: true,
        default: None,
        is_static: false,
    };

    let value = Value::Set(vec![
        Value::Text("alpha".to_string()),
        Value::Text("beta".to_string()),
    ]);

    let mut buf = Vec::new();
    writer
        .write_complex_column(&mut buf, &column, &value, 1001000, Some(3600))
        .unwrap();

    // Parse cell flags structurally so wall-clock LDT bytes in the header and
    // per-cell TTL fields cannot be misidentified as flag bytes.
    let cell_flags = parse_complex_cell_flags(&buf);
    let expected_flags = CELL_IS_EXPIRING | CELL_HAS_EMPTY_VALUE; // 0x06

    assert_eq!(
        cell_flags.len(),
        2,
        "SET with 2 elements should produce 2 cells"
    );
    assert!(
        cell_flags.iter().all(|&f| f == expected_flags),
        "SET with TTL: all cells should have IS_EXPIRING | HAS_EMPTY_VALUE (0x06), got: {:?}",
        cell_flags
    );

    // Confirm absence of USE_ROW_TIMESTAMP on all cells
    assert!(
        cell_flags
            .iter()
            .all(|&f| (f & CELL_USE_ROW_TIMESTAMP) == 0),
        "SET with TTL should NOT have USE_ROW_TIMESTAMP on any cell, got: {:?}",
        cell_flags
    );
}

// ---------------------------------------------------------------------------
// Issue #1275 / #1295: type-aware SIGNED ordering of SET elements / MAP keys.
//
// Cassandra `SetType`/`MapType` sort by the element/key type's own
// `AbstractType.compare`. For `Int32Type`(int) that is SIGNED, so a collection
// containing NEGATIVE values must serialize -1 BEFORE 0/1 — the opposite of the
// raw big-endian two's-complement (unsigned) byte order, where -1 (0xFFFF_FFFF)
// sorts LAST. The ordering ORACLE asserted below is Cassandra's signed
// `Int32Type` comparator.
//
// Issue #1295 commissioned a REAL Cassandra 5.0.2 golden with these exact
// negative-element shapes (`test_signed_coll`, generated by
// `test-data/scripts/generate-signed-collection-parity.sh`). The frozen
// blobs the two writer tests below assert are now CONFIRMED byte-for-byte
// against the on-disk frozen `fs` / `fm` cell blobs Cassandra wrote — see
// `expected_frozen_*` (which slice the committed golden Data.db when present and
// fall back to the documented wire-format layout when the gitignored binary is
// absent), and the read-path companion
// `cqlite-core/tests/issue_1295_signed_collection_order_parity.rs`.
// ---------------------------------------------------------------------------

/// Locate the committed Cassandra 5.0.2 golden `Data.db` for a `test_signed_coll`
/// table (issue #1295). Returns `None` when `CQLITE_DATASETS_ROOT` is unset or
/// the gitignored binary is absent (a fresh checkout / CI without datasets), so
/// callers fall back to the documented wire-format expectation.
fn signed_coll_golden_data_db(table: &str) -> Option<Vec<u8>> {
    let root = std::path::PathBuf::from(std::env::var("CQLITE_DATASETS_ROOT").ok()?);
    let ks = root.join("sstables").join("test_signed_coll");
    let dir = std::fs::read_dir(&ks).ok()?.flatten().find_map(|e| {
        let p = e.path();
        let n = e.file_name();
        if p.is_dir()
            && n.to_str()
                .is_some_and(|s| s.starts_with(&format!("{table}-")))
        {
            Some(p)
        } else {
            None
        }
    })?;
    let data_db = dir.join("nb-1-big-Data.db");
    std::fs::read(data_db).ok()
}

/// Build the canonical frozen<set<int>> blob `[count i32]([4 i32][value])...` for
/// the signed-sorted elements, and assert the committed Cassandra golden's
/// `frozen_int_collections.fs` cell carries EXACTLY this blob when the golden is
/// present (byte-for-byte oracle upgrade, issue #1295).
fn expected_frozen_set_int_blob(sorted: &[i32]) -> Vec<u8> {
    let mut blob = Vec::new();
    blob.extend_from_slice(&(sorted.len() as i32).to_be_bytes());
    for &n in sorted {
        blob.extend_from_slice(&4i32.to_be_bytes());
        blob.extend_from_slice(&n.to_be_bytes());
    }
    if let Some(golden) = signed_coll_golden_data_db("frozen_int_collections") {
        assert!(
            golden.windows(blob.len()).any(|w| w == blob),
            "committed Cassandra golden frozen_int_collections.fs must contain the signed \
             frozen<set<int>> blob {blob:02x?} — derived layout diverges from real bytes"
        );
    }
    blob
}

/// Build the canonical frozen<map<int,text>> blob and assert the committed
/// Cassandra golden's `frozen_int_collections.fm` cell carries EXACTLY this blob
/// when the golden is present (byte-for-byte oracle upgrade, issue #1295).
fn expected_frozen_map_int_text_blob(sorted: &[(i32, &str)]) -> Vec<u8> {
    let mut blob = Vec::new();
    blob.extend_from_slice(&(sorted.len() as i32).to_be_bytes());
    for &(k, v) in sorted {
        blob.extend_from_slice(&4i32.to_be_bytes());
        blob.extend_from_slice(&k.to_be_bytes());
        let vb = v.as_bytes();
        blob.extend_from_slice(&(vb.len() as i32).to_be_bytes());
        blob.extend_from_slice(vb);
    }
    if let Some(golden) = signed_coll_golden_data_db("frozen_int_collections") {
        assert!(
            golden.windows(blob.len()).any(|w| w == blob),
            "committed Cassandra golden frozen_int_collections.fm must contain the signed \
             frozen<map<int,text>> blob {blob:02x?} — derived layout diverges from real bytes"
        );
    }
    blob
}

/// frozen<set<int>> of {3, -1, 1, 0, -2} fed UNSORTED must serialize its elements
/// in SIGNED order (-2, -1, 0, 1, 3). Asserts the FULL serialized blob byte-for-
/// byte against the layout derived from Cassandra's Int32Type comparator.
#[test]
fn test_frozen_set_int_negative_sorts_signed() {
    let value = Value::Frozen(Box::new(Value::Set(vec![
        Value::Integer(3),
        Value::Integer(-1),
        Value::Integer(1),
        Value::Integer(0),
        Value::Integer(-2),
    ])));

    let bytes = serialize_value(&value).unwrap();

    // Wire format: [count i32]([len i32][value])... — int = 4 big-endian bytes.
    // Oracle: Int32Type signed order -> -2, -1, 0, 1, 3. The expected blob is now
    // CONFIRMED byte-for-byte against the committed Cassandra 5.0.2 golden
    // `frozen_int_collections.fs` (issue #1295) when the dataset is present.
    let expected = expected_frozen_set_int_blob(&[-2, -1, 0, 1, 3]);
    assert_eq!(
        bytes, expected,
        "frozen<set<int>> with negatives must serialize in SIGNED (Int32Type) order, \
         not raw unsigned byte order"
    );
}

/// frozen<map<int,text>> with negative keys fed UNSORTED must serialize its
/// entries in SIGNED KEY order (-5, -1, 0, 2). Asserts the FULL serialized blob.
#[test]
fn test_frozen_map_int_key_negative_sorts_signed() {
    let value = Value::Frozen(Box::new(Value::Map(vec![
        (Value::Integer(2), Value::Text("two".to_string())),
        (Value::Integer(-1), Value::Text("neg-one".to_string())),
        (Value::Integer(0), Value::Text("zero".to_string())),
        (Value::Integer(-5), Value::Text("neg-five".to_string())),
    ])));

    let bytes = serialize_value(&value).unwrap();

    // Wire format: [count i32]([klen i32][key][vlen i32][val])...
    // Oracle: Int32Type signed KEY order -> -5, -1, 0, 2. The expected blob is now
    // CONFIRMED byte-for-byte against the committed Cassandra 5.0.2 golden
    // `frozen_int_collections.fm` (issue #1295) when the dataset is present.
    let expected = expected_frozen_map_int_text_blob(&[
        (-5, "neg-five"),
        (-1, "neg-one"),
        (0, "zero"),
        (2, "two"),
    ]);
    assert_eq!(
        bytes, expected,
        "frozen<map<int,text>> with negative keys must serialize entries in SIGNED \
         (Int32Type) key order"
    );
}

/// Non-frozen set<int> (multicell complex column) of {3, -1, 1, 0, -2} fed
/// UNSORTED must emit its cells with cell_paths in SIGNED order. The cell_path of
/// a SET element is its serialized element (4 big-endian int bytes). Asserts the
/// exact cell_path byte sequence decoded from the on-disk complex column.
#[test]
fn test_nonfrozen_set_int_negative_sorts_signed() {
    let stats = create_test_stats();
    let writer = DataWriter::new(stats);

    let column = Column {
        name: "s".to_string(),
        data_type: "set<int>".to_string(),
        nullable: true,
        default: None,
        is_static: false,
    };
    let value = Value::Set(vec![
        Value::Integer(3),
        Value::Integer(-1),
        Value::Integer(1),
        Value::Integer(0),
        Value::Integer(-2),
    ]);

    let mut buf = Vec::new();
    writer
        .write_complex_column(&mut buf, &column, &value, 1_001_000, None)
        .unwrap();

    let (_, _, cells) = decode_complex_column(&buf);
    let paths: Vec<Vec<u8>> = cells.iter().map(|c| c.cell_path.clone()).collect();

    // Oracle: Int32Type signed order -> -2, -1, 0, 1, 3.
    let expected: Vec<Vec<u8>> = [-2i32, -1, 0, 1, 3]
        .iter()
        .map(|n| n.to_be_bytes().to_vec())
        .collect();
    assert_eq!(
        paths, expected,
        "non-frozen set<int> with negatives must order cells in SIGNED (Int32Type) order"
    );
}

/// Non-frozen map<int,text> (multicell complex column) with negative keys fed
/// UNSORTED must emit cells with cell_paths (= serialized keys) in SIGNED order.
#[test]
fn test_nonfrozen_map_int_key_negative_sorts_signed() {
    let stats = create_test_stats();
    let writer = DataWriter::new(stats);

    let column = Column {
        name: "m".to_string(),
        data_type: "map<int, text>".to_string(),
        nullable: true,
        default: None,
        is_static: false,
    };
    let value = Value::Map(vec![
        (Value::Integer(2), Value::Text("two".to_string())),
        (Value::Integer(-1), Value::Text("neg-one".to_string())),
        (Value::Integer(0), Value::Text("zero".to_string())),
        (Value::Integer(-5), Value::Text("neg-five".to_string())),
    ]);

    let mut buf = Vec::new();
    writer
        .write_complex_column(&mut buf, &column, &value, 1_001_000, None)
        .unwrap();

    let (_, _, cells) = decode_complex_column(&buf);
    let paths: Vec<Vec<u8>> = cells.iter().map(|c| c.cell_path.clone()).collect();

    // Oracle: Int32Type signed KEY order -> -5, -1, 0, 2.
    let expected: Vec<Vec<u8>> = [-5i32, -1, 0, 2]
        .iter()
        .map(|n| n.to_be_bytes().to_vec())
        .collect();
    assert_eq!(
        paths, expected,
        "non-frozen map<int,text> with negative keys must order cells in SIGNED key order"
    );
}
