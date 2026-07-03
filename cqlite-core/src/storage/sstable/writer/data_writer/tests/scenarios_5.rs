//! data_writer tests, group 5/6 (issue #1118 split).
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
fn test_map_complex_column_with_ttl() {
    // MAP with TTL should write IS_EXPIRING flag per cell.
    // Uses structural parsing to read cell flags at their exact byte positions,
    // avoiding false positives from time-derived LDT bytes that can equal 0x02.
    let stats = create_test_stats();
    let writer = DataWriter::new(stats);

    let column = Column {
        name: "props".to_string(),
        data_type: "map<text, int>".to_string(),
        nullable: true,
        default: None,
        is_static: false,
    };

    let value = Value::Map(vec![(Value::Text("key1".to_string()), Value::Integer(100))]);

    let mut buf = Vec::new();
    writer
        .write_complex_column(&mut buf, &column, &value, 1001000, Some(7200))
        .unwrap();

    // Parse cell flags structurally so wall-clock LDT bytes cannot be
    // misidentified as IS_EXPIRING (0x02) flag bytes.
    let cell_flags = parse_complex_cell_flags(&buf);

    assert_eq!(
        cell_flags.len(),
        1,
        "MAP with 1 entry should produce 1 cell"
    );
    assert_eq!(
        cell_flags[0] & CELL_IS_EXPIRING,
        CELL_IS_EXPIRING,
        "MAP with TTL: cell should have IS_EXPIRING flag set, got flags byte: 0x{:02X}",
        cell_flags[0]
    );
    assert_eq!(
        cell_flags[0] & CELL_HAS_EMPTY_VALUE,
        0,
        "MAP with TTL: cell should NOT have HAS_EMPTY_VALUE, got flags byte: 0x{:02X}",
        cell_flags[0]
    );
}

#[test]
fn test_list_complex_column_with_ttl() {
    // LIST with TTL should write IS_EXPIRING per cell, producing a larger
    // output than without TTL (extra timestamp/LDT/TTL delta fields).
    // Uses structural parsing to read cell flags at their exact byte positions,
    // avoiding false positives from time-derived LDT bytes.
    let stats = create_test_stats();
    let writer_ttl = DataWriter::new(stats.clone());
    let writer_no_ttl = DataWriter::new(stats);

    let column = Column {
        name: "items".to_string(),
        data_type: "list<int>".to_string(),
        nullable: true,
        default: None,
        is_static: false,
    };

    let value = Value::List(vec![
        Value::Integer(1),
        Value::Integer(2),
        Value::Integer(3),
    ]);

    let mut buf_ttl = Vec::new();
    writer_ttl
        .write_complex_column(&mut buf_ttl, &column, &value, 1001000, Some(1800))
        .unwrap();

    let mut buf_no_ttl = Vec::new();
    writer_no_ttl
        .write_complex_column(&mut buf_no_ttl, &column, &value, 1001000, None)
        .unwrap();

    // TTL version must be larger: each cell gets timestamp + LDT + TTL deltas
    // instead of just USE_ROW_TIMESTAMP flag.
    assert!(
        buf_ttl.len() > buf_no_ttl.len(),
        "LIST with TTL ({} bytes) should be larger than without TTL ({} bytes)",
        buf_ttl.len(),
        buf_no_ttl.len()
    );

    // Structurally verify IS_EXPIRING is set on every cell in the TTL version.
    let cell_flags_ttl = parse_complex_cell_flags(&buf_ttl);
    assert_eq!(
        cell_flags_ttl.len(),
        3,
        "LIST with 3 elements should produce 3 cells"
    );
    assert!(
        cell_flags_ttl.iter().all(|&f| (f & CELL_IS_EXPIRING) != 0),
        "LIST with TTL: all cells should have IS_EXPIRING flag set, got: {:?}",
        cell_flags_ttl
    );

    // Verify the no-TTL version uses USE_ROW_TIMESTAMP instead.
    let cell_flags_no_ttl = parse_complex_cell_flags(&buf_no_ttl);
    assert_eq!(cell_flags_no_ttl.len(), 3);
    assert!(
        cell_flags_no_ttl
            .iter()
            .all(|&f| (f & CELL_IS_EXPIRING) == 0),
        "LIST without TTL: no cells should have IS_EXPIRING flag, got: {:?}",
        cell_flags_no_ttl
    );
}

#[test]
fn test_complex_column_no_ttl_uses_row_timestamp() {
    // Regression: without TTL, cells should still use USE_ROW_TIMESTAMP
    let stats = create_test_stats();
    let writer = DataWriter::new(stats);

    let column = Column {
        name: "tags".to_string(),
        data_type: "set<text>".to_string(),
        nullable: true,
        default: None,
        is_static: false,
    };

    let value = Value::Set(vec![Value::Text("x".to_string())]);

    let mut buf = Vec::new();
    writer
        .write_complex_column(&mut buf, &column, &value, 1001000, None)
        .unwrap();

    // Without TTL: USE_ROW_TIMESTAMP | HAS_EMPTY_VALUE = 0x0C.
    // Use structural parse so DeletionTime.LIVE header bytes are not misidentified.
    let expected_flags = CELL_USE_ROW_TIMESTAMP | CELL_HAS_EMPTY_VALUE;
    let cell_flags = parse_complex_cell_flags(&buf);
    assert_eq!(
        cell_flags.len(),
        1,
        "SET with 1 element should produce 1 cell"
    );
    assert_eq!(
        cell_flags[0], expected_flags,
        "Without TTL, SET cells should use USE_ROW_TIMESTAMP | HAS_EMPTY_VALUE, got: 0x{:02X}",
        cell_flags[0]
    );
}

#[test]
fn test_bitmap_includes_deleted_columns() {
    // Delete operations should mark columns as present in the bitmap
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
                name: "age".to_string(),
                data_type: "int".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let stats = create_test_stats();
    let writer = DataWriter::new(stats);

    // Write "name" and delete "age"
    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(1));
    let mutation = Mutation::new(
        table_id,
        pk,
        None,
        vec![
            CellOperation::Delete {
                column: "age".to_string(),
                local_deletion_time: None,
            },
            CellOperation::Write {
                column: "name".to_string(),
                value: Value::Text("Alice".to_string()),
            },
        ],
        1001000,
        None,
    );

    // Write bitmap — both columns should be present (bitmap = 0)
    let mut buf = Vec::new();
    writer
        .write_column_bitmap(&mut buf, &mutation, &schema)
        .unwrap();

    // bitmap = 0 means all columns present (no MISSING bits set)
    // Since we have 2 regular columns and both are in operations,
    // all should be marked present
    assert_eq!(buf.len(), 1, "Bitmap should be a single byte");
    assert_eq!(
        buf[0], 0,
        "Bitmap should be 0 (all columns present) when both write and delete cover all columns"
    );
}

#[test]
fn test_bitmap_delete_only_column_is_present() {
    // A column that ONLY has a Delete should still be marked present
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
                name: "age".to_string(),
                data_type: "int".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let stats = create_test_stats();
    let writer = DataWriter::new(stats);

    // Only delete "age", don't write "name"
    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(1));
    let mutation = Mutation::new(
        table_id,
        pk,
        None,
        vec![CellOperation::Delete {
            column: "age".to_string(),
            local_deletion_time: None,
        }],
        1001000,
        None,
    );

    let mut buf = Vec::new();
    writer
        .write_column_bitmap(&mut buf, &mutation, &schema)
        .unwrap();

    // Regular columns sorted alphabetically: [age, name]
    // age (idx 0) = present (Delete), name (idx 1) = missing
    // bitmap bit 1 = 1, bit 0 = 0 → bitmap = 0b10 = 2
    assert_eq!(buf.len(), 1);
    assert_eq!(
        buf[0], 2,
        "Bitmap should mark 'name' as missing (bit 1) but 'age' as present (bit 0)"
    );
}

/// Byte-identical guard (Issue #492): the streaming writer (flushing each
/// partition to a file) must produce a Data.db byte sequence that is
/// identical to the legacy in-memory writer, and the returned partition
/// offsets must match exactly. Anything else breaks Index.db offsets.
#[test]
fn test_streaming_writer_byte_identical_to_in_memory() {
    let schema = create_test_schema();
    let partitions = streaming_test_partitions();

    // In-memory reference: accumulate every partition in `buffer`.
    let mut mem_writer = DataWriter::new(create_test_stats());
    let mut mem_offsets = Vec::new();
    for (key, mutations) in &partitions {
        mem_offsets.push(
            mem_writer
                .write_partition(key, mutations, &schema, None, &[])
                .unwrap(),
        );
    }
    let expected_bytes = mem_writer.finish().unwrap();

    // Streaming: flush each partition to a temp Data.db file.
    let dir = tempfile::tempdir().unwrap();
    let data_path = dir.path().join("nb-1-big-Data.db");
    let mut stream_writer = DataWriter::with_sink(create_test_stats(), data_path.clone());
    let mut stream_offsets = Vec::new();
    for (key, mutations) in &partitions {
        stream_offsets.push(
            stream_writer
                .write_partition(key, mutations, &schema, None, &[])
                .unwrap(),
        );
    }
    let data_size = stream_writer.finish_streaming().unwrap().data_size;

    // Offsets returned to the caller (fed to Index.db) must be identical.
    assert_eq!(
        stream_offsets, mem_offsets,
        "streaming partition offsets must equal in-memory offsets"
    );

    // The on-disk Data.db must be byte-for-byte identical to the in-memory
    // bytes, and the reported data_size must match the file length.
    let on_disk = std::fs::read(&data_path).unwrap();
    assert_eq!(
        on_disk, expected_bytes,
        "streamed Data.db must be byte-identical to in-memory Data.db"
    );
    assert_eq!(
        data_size as usize,
        expected_bytes.len(),
        "finish_streaming() data_size must equal file length"
    );

    // Every returned offset must point at the actual start byte in the file:
    // a partition starts with its 2-byte key length, here always 0x0004.
    for &off in &stream_offsets {
        assert_eq!(
            &on_disk[off as usize..off as usize + 2],
            &[0x00, 0x04],
            "offset {off} must land on a partition's key-length prefix"
        );
    }
}

/// Bounded-memory evidence (Issue #492): after each `write_partition` the
/// scratch buffer must hold only the most recent partition, while the
/// flushed `position` grows monotonically. This is the proof that peak heap
/// is O(largest partition) rather than O(file).
#[test]
fn test_streaming_writer_bounds_memory_to_one_partition() {
    let schema = create_test_schema();
    let partitions = streaming_test_partitions();

    let dir = tempfile::tempdir().unwrap();
    let data_path = dir.path().join("nb-1-big-Data.db");
    let mut writer = DataWriter::with_sink(create_test_stats(), data_path);

    let mut prev_flushed = 0u64;
    // Tracks the largest single-partition flushed size. Because the scratch is
    // cleared after every partition (asserted below), peak resident Data.db
    // bytes are bounded by this value, not the whole file.
    let mut max_partition_size = 0usize;
    for (i, (key, mutations)) in partitions.iter().enumerate() {
        let flushed_before = writer.flushed_position();
        writer
            .write_partition(key, mutations, &schema, None, &[])
            .unwrap();

        // After a partition is written it has been flushed and the scratch
        // cleared: the scratch must be empty, never accumulating prior
        // partitions.
        assert_eq!(
            writer.scratch_len(),
            0,
            "scratch must be cleared after partition {i} (bounded memory)"
        );

        // Flushed bytes must strictly increase by this partition's size.
        let flushed_after = writer.flushed_position();
        assert!(
            flushed_after > flushed_before,
            "flushed position must grow after writing partition {i}"
        );
        let this_partition_size = (flushed_after - flushed_before) as usize;
        max_partition_size = max_partition_size.max(this_partition_size);
        assert!(flushed_after > prev_flushed);
        prev_flushed = flushed_after;
    }

    let total = writer.finish_streaming().unwrap().data_size;
    assert_eq!(
        total, prev_flushed,
        "total size must equal last flushed pos"
    );

    // Peak resident bytes were bounded by the largest single partition,
    // which is far smaller than the whole file for many partitions.
    assert!(
            (max_partition_size as u64) < total,
            "largest single partition ({max_partition_size}) must be smaller than the full file ({total})"
        );
}

/// Issue #1392: `finish_streaming` must fsync the Data.db contents (not merely
/// `flush()` them to the page cache) so the bytes are durable before the flush
/// handoff fsyncs the directory and truncates the WAL. fsync is not directly
/// observable, but the durable contract is: after `finish_streaming` returns,
/// the on-disk file exists with the full reported length and the exact bytes,
/// with no writer handle still buffering. Regression guard for the previous
/// flush-only path.
#[test]
fn finish_streaming_persists_data_db_contents() {
    let schema = create_test_schema();
    let partitions = streaming_test_partitions();

    let dir = tempfile::tempdir().unwrap();
    let data_path = dir.path().join("nb-1-big-Data.db");
    let mut writer = DataWriter::with_sink(create_test_stats(), data_path.clone());
    for (key, mutations) in &partitions {
        writer
            .write_partition(key, mutations, &schema, None, &[])
            .unwrap();
    }
    let data_size = writer.finish_streaming().unwrap().data_size;

    // The file length on disk equals the reported size (all bytes durable).
    let meta = std::fs::metadata(&data_path).unwrap();
    assert_eq!(
        meta.len(),
        data_size,
        "on-disk Data.db length must equal finish_streaming()'s reported size"
    );
    assert!(data_size > 0, "streamed Data.db must be non-empty");
    // And the content is fully readable (writer handle released, bytes flushed).
    let on_disk = std::fs::read(&data_path).unwrap();
    assert_eq!(on_disk.len() as u64, data_size);
}

/// (a) Two elements at DIFFERENT per-element timestamps must produce two
/// cells, each carrying its OWN explicit timestamp delta (NOT
/// USE_ROW_TIMESTAMP, NOT a single promoted row timestamp).
#[test]
fn per_element_distinct_timestamps_emit_explicit_deltas() {
    let mut stats = StatisticsMetadata::new();
    stats.min_timestamp = 1_000_000;
    stats.min_ttl = 0;
    stats.min_local_deletion_time = 0;
    let writer = DataWriter::new(stats);

    let column = set_column("tags");
    // Row liveness timestamp differs from BOTH element timestamps, so neither
    // element may use USE_ROW_TIMESTAMP.
    let row_ts = 1_000_000i64;
    let elem_a = ComplexElementWrite {
        cell_path: serialize_collection_element(&Value::Integer(10), "SET").unwrap(),
        value: None, // SET element: empty value
        timestamp_micros: 1_005_000,
        ttl_seconds: None,
        local_deletion_time: None,
        is_deleted: false,
    };
    let elem_b = ComplexElementWrite {
        cell_path: serialize_collection_element(&Value::Integer(20), "SET").unwrap(),
        value: None,
        timestamp_micros: 1_009_000,
        ttl_seconds: None,
        local_deletion_time: None,
        is_deleted: false,
    };

    let mut buf = Vec::new();
    writer
        .write_complex_column_per_element(&mut buf, &column, None, &[elem_a, elem_b], row_ts)
        .unwrap();

    let (_del_ts, _del_ldt, cells) = decode_complex_column(&buf);
    assert_eq!(cells.len(), 2, "two SET elements => two cells");
    for c in &cells {
        assert_eq!(
            c.flags & CELL_USE_ROW_TIMESTAMP,
            0,
            "element ts differs from row ts => USE_ROW_TIMESTAMP must be CLEARED, flags=0x{:02x}",
            c.flags
        );
        assert!(
            c.ts_delta.is_some(),
            "an explicit per-element timestamp delta must be written"
        );
    }
    // The two distinct timestamps must survive as two DISTINCT deltas — not
    // collapsed/promoted to one.
    assert_eq!(cells[0].ts_delta, Some(5_000));
    assert_eq!(cells[1].ts_delta, Some(9_000));
    assert_ne!(
        cells[0].ts_delta, cells[1].ts_delta,
        "disjoint per-element timestamps must NOT be promoted to one"
    );
}

/// An element whose per-element timestamp EQUALS the row timestamp keeps
/// USE_ROW_TIMESTAMP (0x08) and writes no explicit delta; a sibling at a
/// different timestamp clears it. (Mixed case in one column.)
#[test]
fn per_element_row_timestamp_kept_only_when_equal() {
    let mut stats = StatisticsMetadata::new();
    stats.min_timestamp = 1_000_000;
    stats.min_ttl = 0;
    stats.min_local_deletion_time = 0;
    let writer = DataWriter::new(stats);

    let column = set_column("tags");
    let row_ts = 1_007_000i64;
    let same = ComplexElementWrite {
        cell_path: serialize_collection_element(&Value::Integer(10), "SET").unwrap(),
        value: None,
        timestamp_micros: row_ts, // equal to row ts
        ttl_seconds: None,
        local_deletion_time: None,
        is_deleted: false,
    };
    let diff = ComplexElementWrite {
        cell_path: serialize_collection_element(&Value::Integer(20), "SET").unwrap(),
        value: None,
        timestamp_micros: 1_009_000, // != row ts
        ttl_seconds: None,
        local_deletion_time: None,
        is_deleted: false,
    };

    let mut buf = Vec::new();
    writer
        .write_complex_column_per_element(&mut buf, &column, None, &[same, diff], row_ts)
        .unwrap();

    let (_del_ts, _del_ldt, cells) = decode_complex_column(&buf);
    assert_eq!(cells.len(), 2);
    // Cell for element 10 (path-sorted first): equals row ts → USE_ROW_TIMESTAMP.
    assert_ne!(cells[0].flags & CELL_USE_ROW_TIMESTAMP, 0);
    assert_eq!(cells[0].ts_delta, None);
    // Cell for element 20: differs → explicit delta.
    assert_eq!(cells[1].flags & CELL_USE_ROW_TIMESTAMP, 0);
    assert_eq!(cells[1].ts_delta, Some(9_000));
}

/// (b) A REAL complex deletion marker (markedForDeleteAt + localDeletionTime,
/// NOT the LIVE sentinel) must be written, followed by surviving cells.
#[test]
fn per_element_real_complex_deletion_then_surviving_cells() {
    let mut stats = StatisticsMetadata::new();
    stats.min_timestamp = 1_000_000;
    stats.min_ttl = 0;
    stats.min_local_deletion_time = 1_700_000_000;
    let writer = DataWriter::new(stats);

    let column = set_column("tags");
    let row_ts = 1_012_000i64;
    let mfda = 1_010_000i64; // markedForDeleteAt
    let ldt = 1_700_000_005i32; // localDeletionTime (seconds)

    // One element survives the complex deletion (written after mfda).
    let survivor = ComplexElementWrite {
        cell_path: serialize_collection_element(&Value::Integer(30), "SET").unwrap(),
        value: None,
        timestamp_micros: row_ts,
        ttl_seconds: None,
        local_deletion_time: None,
        is_deleted: false,
    };

    let mut buf = Vec::new();
    writer
        .write_complex_column_per_element(&mut buf, &column, Some((mfda, ldt)), &[survivor], row_ts)
        .unwrap();

    let (del_ts, del_ldt, cells) = decode_complex_column(&buf);

    // LIVE sentinel deltas (what the old hardcoded path wrote):
    let live_ts_delta = i64::MIN.wrapping_sub(1_000_000) as u64;
    let live_ldt_delta = i32::MAX.wrapping_sub(1_700_000_000) as u32 as u64;
    assert_ne!(
        del_ts, live_ts_delta,
        "must NOT be the LIVE markedForDeleteAt sentinel"
    );
    assert_ne!(
        del_ldt, live_ldt_delta,
        "must NOT be the LIVE localDeletionTime sentinel"
    );
    // Real deletion deltas (unsigned VInt against seeded baselines).
    assert_eq!(del_ts, (mfda - 1_000_000) as u64);
    assert_eq!(del_ldt, (ldt - 1_700_000_000) as u64);
    // The surviving element is still emitted after the marker.
    assert_eq!(
        cells.len(),
        1,
        "the surviving element must follow the marker"
    );
}

/// (c) A LIST element's source 16-byte cell path must round-trip byte-for-byte
/// (it is the preserved TimeUUID, NOT a freshly generated one).
#[test]
fn per_element_list_cell_path_roundtrips_byte_for_byte() {
    let mut stats = StatisticsMetadata::new();
    stats.min_timestamp = 1_000_000;
    stats.min_ttl = 0;
    stats.min_local_deletion_time = 0;
    let writer = DataWriter::new(stats);

    let column = list_column("items");
    let row_ts = 1_003_000i64;

    // A specific, recognizable 16-byte TimeUUID we must NOT regenerate.
    let source_path: Vec<u8> = vec![
        0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF,
        0x01,
    ];
    let elem = ComplexElementWrite {
        cell_path: source_path.clone(),
        value: Some(Value::Integer(42)),
        timestamp_micros: row_ts,
        ttl_seconds: None,
        local_deletion_time: None,
        is_deleted: false,
    };

    let mut buf = Vec::new();
    writer
        .write_complex_column_per_element(&mut buf, &column, None, &[elem], row_ts)
        .unwrap();

    let (_del_ts, _del_ldt, cells) = decode_complex_column(&buf);
    assert_eq!(cells.len(), 1);
    assert_eq!(
        cells[0].cell_path, source_path,
        "the source 16-byte LIST cell path must round-trip byte-for-byte (not regenerated)"
    );
    assert_eq!(
        cells[0].value,
        Some(serialize_value(&Value::Integer(42)).unwrap()),
        "LIST element value must be serialized after the preserved path"
    );
}

/// An element-level tombstone (`value == None`, `is_deleted`) writes
/// IS_DELETED (0x01), an explicit ts (when != row ts) and an LDT, and no
/// value bytes.
#[test]
fn per_element_element_tombstone_writes_is_deleted_and_ldt() {
    let mut stats = StatisticsMetadata::new();
    stats.min_timestamp = 1_000_000;
    stats.min_ttl = 0;
    stats.min_local_deletion_time = 1_700_000_000;
    let writer = DataWriter::new(stats);

    let column = list_column("items");
    let row_ts = 1_000_000i64;
    let elem = ComplexElementWrite {
        cell_path: vec![0xAB; 16],
        value: None,
        timestamp_micros: 1_004_000,
        ttl_seconds: None,
        local_deletion_time: Some(1_700_000_009),
        is_deleted: true,
    };

    let mut buf = Vec::new();
    writer
        .write_complex_column_per_element(&mut buf, &column, None, &[elem], row_ts)
        .unwrap();

    let (_del_ts, _del_ldt, cells) = decode_complex_column(&buf);
    assert_eq!(cells.len(), 1);
    assert_ne!(
        cells[0].flags & CELL_IS_DELETED,
        0,
        "IS_DELETED must be set"
    );
    // roborev #897: a tombstone carries no value bytes, so it MUST also set
    // HAS_EMPTY_VALUE (0x04). Cassandra's Cell.Serializer derives value
    // presence from that bit alone.
    assert_ne!(
        cells[0].flags & CELL_HAS_EMPTY_VALUE,
        0,
        "tombstone must set HAS_EMPTY_VALUE so strict readers read no value length"
    );
    assert_eq!(cells[0].ts_delta, Some(4_000));
    assert_eq!(
        cells[0].ldt_delta,
        Some((1_700_000_009 - 1_700_000_000) as u64)
    );
    assert_eq!(cells[0].ttl_delta, None, "a tombstone is not expiring");
    assert!(cells[0].value.is_none(), "tombstone writes no value bytes");
    assert_eq!(cells[0].cell_path, vec![0xAB; 16]);
}

/// An expiring per-element write emits IS_EXPIRING with explicit ts + ldt +
/// ttl deltas (against the seeded baselines).
#[test]
fn per_element_expiring_writes_ts_ldt_ttl_deltas() {
    let mut stats = StatisticsMetadata::new();
    stats.min_timestamp = 1_000_000;
    stats.min_ttl = 100;
    stats.min_local_deletion_time = 1_700_000_000;
    let writer = DataWriter::new(stats);

    let column = list_column("items");
    let row_ts = 1_000_000i64;
    let elem = ComplexElementWrite {
        cell_path: vec![0xCD; 16],
        value: Some(Value::Integer(7)),
        timestamp_micros: 1_006_000,
        ttl_seconds: Some(3_600),
        local_deletion_time: Some(1_700_003_600),
        is_deleted: false,
    };

    let mut buf = Vec::new();
    writer
        .write_complex_column_per_element(&mut buf, &column, None, &[elem], row_ts)
        .unwrap();

    let (_del_ts, _del_ldt, cells) = decode_complex_column(&buf);
    assert_eq!(cells.len(), 1);
    assert_ne!(
        cells[0].flags & CELL_IS_EXPIRING,
        0,
        "IS_EXPIRING must be set"
    );
    assert_eq!(cells[0].ts_delta, Some(6_000));
    assert_eq!(
        cells[0].ldt_delta,
        Some((1_700_003_600 - 1_700_000_000) as u64)
    );
    assert_eq!(cells[0].ttl_delta, Some((3_600 - 100) as u64));
    assert_eq!(
        cells[0].value,
        Some(serialize_value(&Value::Integer(7)).unwrap())
    );
}

/// roborev #897 — BYTE-LEVEL regression on the HAS_EMPTY_VALUE (0x04) bit of a
/// per-element complex cell. Cassandra's Cell.Serializer derives value presence
/// from 0x04 alone (`hasValue = !flag(HAS_EMPTY_VALUE_MASK)`); a cell with no
/// value bytes MUST set 0x04 or a strict reader desynchronizes trying to read a
/// value length that is not there.
///
/// Asserts the EXACT flags byte for three element shapes, that no value bytes
/// follow whenever 0x04 is set, and that the bytes round-trip through the wire
/// walk. The assertions read the raw flags byte directly (not via the
/// `is_deleted || has_empty_value` value gate), so the test FAILS if 0x04 is
/// dropped for the tombstone — the exact regression being guarded.
#[test]
fn per_element_tombstone_sets_has_empty_value_byte_level() {
    let mut stats = StatisticsMetadata::new();
    stats.min_timestamp = 1_000_000;
    stats.min_ttl = 0;
    stats.min_local_deletion_time = 1_700_000_000;
    let writer = DataWriter::new(stats);
    let row_ts = 1_000_000i64;

    // Case 1: deleted element (tombstone) — flags MUST be IS_DELETED |
    // HAS_EMPTY_VALUE | USE_ROW_TIMESTAMP. (ts == row_ts so USE_ROW_TIMESTAMP is
    // set; ts_delta is therefore absent. The deletion bits are what matters.)
    let tombstone = ComplexElementWrite {
        cell_path: vec![0x01; 16],
        value: None,
        timestamp_micros: row_ts,
        ttl_seconds: None,
        local_deletion_time: Some(1_700_000_005),
        is_deleted: true,
    };
    let mut buf = Vec::new();
    writer
        .write_complex_column_per_element(
            &mut buf,
            &list_column("items"),
            None,
            &[tombstone],
            row_ts,
        )
        .unwrap();
    let (_d0, _d1, cells) = decode_complex_column(&buf);
    assert_eq!(cells.len(), 1);
    let expected_tombstone_flags = CELL_IS_DELETED | CELL_HAS_EMPTY_VALUE | CELL_USE_ROW_TIMESTAMP;
    assert_eq!(
            cells[0].flags, expected_tombstone_flags,
            "tombstone flags must be 0x{:02x} (IS_DELETED|HAS_EMPTY_VALUE|USE_ROW_TIMESTAMP); got 0x{:02x}",
            expected_tombstone_flags, cells[0].flags
        );
    // Sensitivity anchors: the two bits the finding is about.
    assert_ne!(
        cells[0].flags & CELL_HAS_EMPTY_VALUE,
        0,
        "tombstone MUST set HAS_EMPTY_VALUE (0x04)"
    );
    assert_eq!(
        cells[0].flags & (CELL_IS_DELETED | CELL_HAS_EMPTY_VALUE),
        0x05,
        "deleted complex element must serialize with IS_DELETED|HAS_EMPTY_VALUE == 0x05"
    );
    assert!(
        cells[0].value.is_none(),
        "tombstone carries zero value bytes"
    );

    // Case 2: live SET member (value None, not deleted) — flags MUST set
    // HAS_EMPTY_VALUE (+ USE_ROW_TIMESTAMP here) and carry no value bytes; the
    // datum lives in the cell_path.
    let set_member = ComplexElementWrite {
        cell_path: vec![0x02; 4],
        value: None,
        timestamp_micros: row_ts,
        ttl_seconds: None,
        local_deletion_time: None,
        is_deleted: false,
    };
    let mut buf = Vec::new();
    writer
        .write_complex_column_per_element(
            &mut buf,
            &set_column("tags"),
            None,
            &[set_member],
            row_ts,
        )
        .unwrap();
    let (_d0, _d1, cells) = decode_complex_column(&buf);
    assert_eq!(cells.len(), 1);
    assert_ne!(
        cells[0].flags & CELL_HAS_EMPTY_VALUE,
        0,
        "live SET member MUST set HAS_EMPTY_VALUE (0x04)"
    );
    assert_eq!(
        cells[0].flags & CELL_IS_DELETED,
        0,
        "a live SET member is NOT a tombstone"
    );
    assert!(
        cells[0].value.is_none(),
        "SET member carries no value bytes (HAS_EMPTY_VALUE set)"
    );

    // Case 3: live MAP/LIST element (value Some, not deleted) — flags MUST NOT
    // set HAS_EMPTY_VALUE; a value-length VInt and value bytes follow.
    let list_elem = ComplexElementWrite {
        cell_path: vec![0x03; 16],
        value: Some(Value::Integer(99)),
        timestamp_micros: row_ts,
        ttl_seconds: None,
        local_deletion_time: None,
        is_deleted: false,
    };
    let mut buf = Vec::new();
    writer
        .write_complex_column_per_element(
            &mut buf,
            &list_column("items"),
            None,
            &[list_elem],
            row_ts,
        )
        .unwrap();
    let (_d0, _d1, cells) = decode_complex_column(&buf);
    assert_eq!(cells.len(), 1);
    assert_eq!(
        cells[0].flags & CELL_HAS_EMPTY_VALUE,
        0,
        "a live MAP/LIST element with a value MUST NOT set HAS_EMPTY_VALUE"
    );
    assert_eq!(
        cells[0].flags & CELL_IS_DELETED,
        0,
        "a live MAP/LIST element is NOT a tombstone"
    );
    assert_eq!(
        cells[0].value,
        Some(serialize_value(&Value::Integer(99)).unwrap()),
        "live MAP/LIST element round-trips its value bytes"
    );
}

#[test]
fn row_path_complex_element_only_mutation_round_trips() {
    let schema = complex_only_schema();

    let mut stats = StatisticsMetadata::new();
    stats.min_timestamp = 1_000_000;
    stats.min_ttl = 0;
    stats.min_local_deletion_time = 1_700_000_000;

    let row_ts = 2_000_000i64;

    // cell paths for two distinct SET members (serialized int).
    let path_keep = serialize_collection_element(&Value::Integer(10), "SET").unwrap();
    let path_dead = serialize_collection_element(&Value::Integer(20), "SET").unwrap();

    // The mutation's ONLY ops are per-element complex ops:
    //   - a real per-column complex deletion marker,
    //   - an EXPIRING SET member (value None, ttl Some, ldt Some, NOT deleted),
    //   - a genuine element tombstone (value None, is_deleted true).
    let mutation = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![
            CellOperation::ComplexDeletion {
                column: "tags".to_string(),
                marked_for_delete_at: 1_500_000,
                local_deletion_time: 1_700_000_005,
            },
            CellOperation::WriteComplexElement {
                column: "tags".to_string(),
                cell_path: path_keep.clone(),
                value: None,
                timestamp_micros: 2_000_000,
                ttl_seconds: Some(3_600),
                local_deletion_time: Some(1_700_003_600),
                // EXPIRING set member — authoritatively NOT a tombstone.
                is_deleted: false,
            },
            CellOperation::WriteComplexElement {
                column: "tags".to_string(),
                cell_path: path_dead.clone(),
                value: None,
                timestamp_micros: 2_000_000,
                ttl_seconds: None,
                local_deletion_time: Some(1_700_000_009),
                // Genuine element tombstone.
                is_deleted: true,
            },
        ],
        row_ts,
        None,
    );

    let row = DataWriter::merge_row_group(&[&mutation], &schema, false, None)
        .expect("merge_row_group should produce a row for complex_element_ops");
    assert!(
        row.ops.is_empty(),
        "all ops are per-element complex ops; row.ops (whole-column) must be empty"
    );
    assert_eq!(
        row.complex_element_ops.len(),
        3,
        "the deletion marker + two element writes survive as complex_element_ops"
    );

    let mut writer = DataWriter::new(stats.clone());
    let (_bytes, cells_written) = writer
        .write_merged_row_with_prev_size(&row, &schema, 0)
        .expect("row path write should succeed");
    assert_eq!(
        cells_written, 1,
        "one complex column emitted => one column counted"
    );

    // ---- Strip the row prefix to reach the body the parser walks.
    // Layout: [row_flags u8][row_size vint][prev_size vint][body...].
    let out = writer.buffer.clone();
    let mut pos = 0usize;
    let row_flags = out[pos];
    pos += 1;

    // FINDING 1: a row whose only ops are complex element ops MUST set
    // ROW_HAS_COMPLEX_DELETION (computed from complex_element_ops, not ops).
    assert_ne!(
        row_flags & ROW_HAS_COMPLEX_DELETION,
        0,
        "ROW_HAS_COMPLEX_DELETION must be set for a complex-element-only row, flags=0x{:02x}",
        row_flags
    );
    assert_eq!(
        row_flags & ROW_HAS_ALL_COLUMNS,
        0,
        "a single present complex column is a subset, not HAS_ALL_COLUMNS"
    );

    // Skip row_size + prev_size vints.
    fn skip_uvint(buf: &[u8], pos: &mut usize) {
        let first = buf[*pos];
        let extra = if first == 0xFF {
            8
        } else {
            first.leading_ones() as usize
        };
        *pos += 1 + extra;
    }
    skip_uvint(&out, &mut pos); // row_size
    skip_uvint(&out, &mut pos); // prev_size

    let body = &out[pos..];
    let (column_present, complex_deletion, cells) = parse_complex_only_row(body, row_flags, &stats);

    // FINDING 1: the column present only via complex_element_ops must be
    // marked PRESENT in the bitmap (bit 0 == 0).
    assert!(
        column_present,
        "the complex column present only via complex_element_ops must be marked present"
    );

    // FINDING 1: the real complex deletion marker must decode (NOT the LIVE
    // sentinel) — markedForDeleteAt delta = 1_500_000 - 1_000_000 = 500_000;
    // localDeletionTime delta = 1_700_000_005 - 1_700_000_000 = 5.
    let (del_ts, del_ldt) = complex_deletion.expect("complex deletion header present");
    assert_eq!(
        del_ts, 500_000,
        "real complex-deletion markedForDeleteAt delta"
    );
    assert_eq!(del_ldt, 5, "real complex-deletion localDeletionTime delta");

    assert_eq!(cells.len(), 2, "two surviving element cells");
    // SET cells are emitted sorted by cell_path bytes; locate by path.
    let keep = cells
        .iter()
        .find(|c| c.cell_path == path_keep)
        .expect("expiring element cell present");
    let dead = cells
        .iter()
        .find(|c| c.cell_path == path_dead)
        .expect("tombstone element cell present");

    // FINDING 2: the EXPIRING SET member round-trips as IS_EXPIRING set,
    // IS_DELETED CLEAR, no value bytes, ts/ldt/ttl deltas present.
    assert_ne!(
        keep.flags & CELL_IS_EXPIRING,
        0,
        "expiring set member must set IS_EXPIRING, flags=0x{:02x}",
        keep.flags
    );
    assert_eq!(
        keep.flags & CELL_IS_DELETED,
        0,
        "expiring set member must NOT be classified as a tombstone, flags=0x{:02x}",
        keep.flags
    );
    assert!(
        keep.value.is_none(),
        "set member carries the element in the path, no value bytes"
    );
    assert_eq!(keep.ttl_delta, Some(3_600));
    assert_eq!(keep.ldt_delta, Some((1_700_003_600 - 1_700_000_000) as u64));

    // FINDING 2: the genuine tombstone round-trips as IS_DELETED.
    assert_ne!(
        dead.flags & CELL_IS_DELETED,
        0,
        "genuine element tombstone must set IS_DELETED, flags=0x{:02x}",
        dead.flags
    );
    assert_eq!(
        dead.flags & CELL_IS_EXPIRING,
        0,
        "a tombstone is not expiring, flags=0x{:02x}",
        dead.flags
    );
    assert!(dead.value.is_none(), "tombstone writes no value bytes");
    assert_eq!(dead.ldt_delta, Some((1_700_000_009 - 1_700_000_000) as u64));
}

#[test]
fn mixed_whole_column_and_per_element_emit_in_schema_order() {
    let schema = two_complex_columns_schema();

    let mut stats = StatisticsMetadata::new();
    stats.min_timestamp = 1_000_000;
    stats.min_ttl = 0;
    stats.min_local_deletion_time = 1_700_000_000;

    let row_ts = 2_000_000i64;

    // EARLIER-sorting column gets a PER-ELEMENT op (member int 10).
    let aaa_path = serialize_collection_element(&Value::Integer(10), "SET").unwrap();
    // LATER-sorting column gets a WHOLE-COLUMN complex write ({99}).
    let zzz_path = serialize_collection_element(&Value::Integer(99), "SET").unwrap();

    let mutation = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![
            // Whole-column complex write → lands in row.ops.
            CellOperation::Write {
                column: "zzz_set".to_string(),
                value: Value::Set(vec![Value::Integer(99)]),
            },
            // Per-element complex op → lands in row.complex_element_ops.
            CellOperation::WriteComplexElement {
                column: "aaa_set".to_string(),
                cell_path: aaa_path.clone(),
                value: None,
                timestamp_micros: row_ts,
                ttl_seconds: None,
                local_deletion_time: None,
                is_deleted: false,
            },
        ],
        row_ts,
        None,
    );

    let row =
        DataWriter::merge_row_group(&[&mutation], &schema, false, None).expect("row should merge");
    assert_eq!(
        row.ops.len(),
        1,
        "zzz_set whole-column write lands in row.ops"
    );
    assert_eq!(
        row.complex_element_ops.len(),
        1,
        "aaa_set per-element op lands in row.complex_element_ops"
    );

    let mut writer = DataWriter::new(stats.clone());
    let (_bytes, cells_written) = writer
        .write_merged_row_with_prev_size(&row, &schema, 0)
        .expect("row path write should succeed");
    assert_eq!(
        cells_written, 2,
        "two complex columns emitted => two columns counted"
    );

    // ---- Strip [row_flags][row_size][prev_size] to reach the body.
    fn read_uvint(buf: &[u8], pos: &mut usize) -> u64 {
        let first = buf[*pos];
        *pos += 1;
        if first == 0xFF {
            let mut v = 0u64;
            for _ in 0..8 {
                v = (v << 8) | buf[*pos] as u64;
                *pos += 1;
            }
            return v;
        }
        let extra = first.leading_ones() as usize;
        let mask = 0xFF_u8.wrapping_shr((extra + 1) as u32);
        let mut v = (first & mask) as u64;
        for _ in 0..extra {
            v = (v << 8) | buf[*pos] as u64;
            *pos += 1;
        }
        v
    }
    fn skip_uvint(buf: &[u8], pos: &mut usize) {
        let first = buf[*pos];
        let extra = if first == 0xFF {
            8
        } else {
            first.leading_ones() as usize
        };
        *pos += 1 + extra;
    }
    /// Walk one complex column (deletion header + cells) and return its cell
    /// paths, advancing `pos` past the column.
    fn walk_complex_column(buf: &[u8], pos: &mut usize) -> Vec<Vec<u8>> {
        let _del_ts = read_uvint(buf, pos);
        let _del_ldt = read_uvint(buf, pos);
        let cell_count = read_uvint(buf, pos) as usize;
        let mut paths = Vec::with_capacity(cell_count);
        for _ in 0..cell_count {
            let flags = buf[*pos];
            *pos += 1;
            let is_deleted = (flags & CELL_IS_DELETED) != 0;
            let is_expiring = (flags & CELL_IS_EXPIRING) != 0;
            let has_empty_value = (flags & CELL_HAS_EMPTY_VALUE) != 0;
            let use_row_ts = (flags & CELL_USE_ROW_TIMESTAMP) != 0;
            let use_row_ttl = (flags & CELL_USE_ROW_TTL) != 0;
            if !use_row_ts {
                read_uvint(buf, pos);
            }
            if !use_row_ttl && (is_deleted || is_expiring) {
                read_uvint(buf, pos);
            }
            if !use_row_ttl && is_expiring {
                read_uvint(buf, pos);
            }
            let path_len = read_uvint(buf, pos) as usize;
            paths.push(buf[*pos..*pos + path_len].to_vec());
            *pos += path_len;
            if !(is_deleted || has_empty_value) {
                let value_len = read_uvint(buf, pos) as usize;
                *pos += value_len;
            }
        }
        paths
    }

    let out = writer.buffer.clone();
    let mut pos = 0usize;
    let row_flags = out[pos];
    pos += 1;
    skip_uvint(&out, &mut pos); // row_size
    skip_uvint(&out, &mut pos); // prev_size

    let body = &out[pos..];
    let mut bpos = 0usize;
    if (row_flags & ROW_HAS_TIMESTAMP) != 0 {
        read_uvint(body, &mut bpos);
    }
    if (row_flags & ROW_HAS_TTL) != 0 {
        read_uvint(body, &mut bpos);
        read_uvint(body, &mut bpos);
    }
    if (row_flags & ROW_HAS_DELETION) != 0 {
        read_uvint(body, &mut bpos);
        read_uvint(body, &mut bpos);
    }
    if (row_flags & ROW_HAS_ALL_COLUMNS) == 0 {
        read_uvint(body, &mut bpos); // column bitmap (both present)
    }

    // The body must lay the complex columns out in schema order: the
    // earlier-sorting per-element column (aaa_set) FIRST, then the
    // later-sorting whole-column write (zzz_set).
    let col1 = walk_complex_column(body, &mut bpos);
    let col2 = walk_complex_column(body, &mut bpos);
    assert_eq!(
        col1,
        vec![aaa_path.clone()],
        "first complex column in the body must be the earlier-sorting \
             per-element column aaa_set, not the whole-column zzz_set"
    );
    assert_eq!(
        col2,
        vec![zzz_path.clone()],
        "second complex column in the body must be the whole-column write zzz_set"
    );
}

/// A sparse, out-of-order whole-`Value::Udt` write must round-trip through
/// the reader with each field landing at its DECLARED index (issue #927 item
/// 3: field index comes from declared order, never the literal's position).
#[test]
fn udt_whole_write_roundtrips_sparse_out_of_order() {
    let writer = DataWriter::new(create_test_stats());
    let col = udt_column("addr", &person_udt_marshal());
    // Literal lists email THEN name (out of order) and OMITS age (sparse).
    let udt = Value::Udt(crate::types::UdtValue {
        type_name: "person".to_string(),
        keyspace: "test_ks".to_string(),
        fields: vec![
            udt_field("email", Some(Value::Text("a@b.com".to_string()))),
            udt_field("name", Some(Value::Text("Alice".to_string()))),
        ],
    });

    let row_ts = 1_005_000i64;
    let mut buf = Vec::new();
    writer
        .write_complex_column(&mut buf, &col, &udt, row_ts, None)
        .unwrap();

    // Decode the raw bytes: two cells (name idx 0, email idx 2), ascending
    // signed-short field index, age (idx 1) absent.
    let (_del_ts, _del_ldt, cells) = decode_complex_column(&buf);
    assert_eq!(cells.len(), 2, "two non-null fields => two cells");
    assert_eq!(
        cells[0].cell_path,
        0u16.to_be_bytes().to_vec(),
        "name idx 0 first"
    );
    assert_eq!(
        cells[1].cell_path,
        2u16.to_be_bytes().to_vec(),
        "email idx 2 next"
    );

    // True round-trip through the reader.
    let parser = person_reader();
    let (value, _off, _meta) = parser
        .parse_complex_column_inner(&buf, 0, &col, &col.data_type, true, row_ts, None)
        .expect("reader must parse the UDT complex column");

    match value {
        Value::Udt(out) => {
            assert_eq!(out.type_name, "person");
            assert_eq!(out.keyspace, "test_ks");
            assert_eq!(out.fields.len(), 3, "all DECLARED fields present");
            assert_eq!(out.fields[0].name, "name");
            assert_eq!(out.fields[0].value, Some(Value::Text("Alice".to_string())));
            assert_eq!(out.fields[1].name, "age");
            assert_eq!(out.fields[1].value, None, "omitted field stays null");
            assert_eq!(out.fields[2].name, "email");
            assert_eq!(
                out.fields[2].value,
                Some(Value::Text("a@b.com".to_string()))
            );
        }
        other => panic!("expected Value::Udt, got {:?}", other),
    }
}

/// Row TTL on a whole-UDT write must propagate to every emitted field cell as
/// an expiring cell (issue #927 item 4 / roborev job 929).
#[test]
fn udt_whole_write_propagates_row_ttl() {
    let writer = DataWriter::new(create_test_stats());
    let col = udt_column("addr", &person_udt_marshal());
    let udt = Value::Udt(crate::types::UdtValue {
        type_name: "person".to_string(),
        keyspace: "test_ks".to_string(),
        fields: vec![
            udt_field("name", Some(Value::Text("Bob".to_string()))),
            udt_field("age", Some(Value::Integer(42))),
        ],
    });

    let mut buf = Vec::new();
    writer
        .write_complex_column(&mut buf, &col, &udt, 1_005_000, Some(3_600))
        .unwrap();

    let (_d, _l, cells) = decode_complex_column(&buf);
    assert_eq!(cells.len(), 2);
    for c in &cells {
        assert_ne!(
            c.flags & CELL_IS_EXPIRING,
            0,
            "TTL must make every field cell expiring, flags=0x{:02x}",
            c.flags
        );
        assert!(c.ttl_delta.is_some(), "expiring cell carries a TTL delta");
        assert!(c.ldt_delta.is_some(), "expiring cell carries an LDT delta");
    }
}

/// A whole-UDT literal naming a field that is not declared is authoritative
/// corruption — reject it (no-heuristics mandate, issue #927 item 3).
#[test]
fn udt_whole_write_rejects_unknown_field() {
    let writer = DataWriter::new(create_test_stats());
    let col = udt_column("addr", &person_udt_marshal());
    let udt = Value::Udt(crate::types::UdtValue {
        type_name: "person".to_string(),
        keyspace: "test_ks".to_string(),
        fields: vec![udt_field("nope", Some(Value::Text("x".to_string())))],
    });
    let mut buf = Vec::new();
    let err = writer
        .write_complex_column(&mut buf, &col, &udt, 1_005_000, None)
        .unwrap_err();
    assert!(
        matches!(err, Error::InvalidInput(_)),
        "unknown UDT field must be InvalidInput, got {:?}",
        err
    );
}

/// UDT per-element cell paths must sort by SIGNED ShortType, so a field index
/// in `[32768, 65535]` (negative as i16) sorts BEFORE the positive indices
/// (issue #927, parity Cassandra `d14c96b8`). A plain byte-lexicographic sort
/// would put 0x8000 last.
#[test]
fn udt_per_element_signed_short_ordering() {
    let writer = DataWriter::new(create_test_stats());
    let col = udt_column("addr", &person_udt_marshal());
    let mk = |idx: u16| ComplexElementWrite {
        cell_path: idx.to_be_bytes().to_vec(),
        value: Some(Value::Integer(idx as i32)),
        timestamp_micros: 1_000_000,
        ttl_seconds: None,
        local_deletion_time: None,
        is_deleted: false,
    };
    // Supplied out of order; 0x8000 == -32768 (signed), 0x7FFF == 32767.
    let elements = vec![mk(0x7FFF), mk(0x8000), mk(0x0001)];
    let mut buf = Vec::new();
    writer
        .write_complex_column_per_element(&mut buf, &col, None, &elements, 1_000_000)
        .unwrap();

    let (_d, _l, cells) = decode_complex_column(&buf);
    let paths: Vec<u16> = cells
        .iter()
        .map(|c| u16::from_be_bytes([c.cell_path[0], c.cell_path[1]]))
        .collect();
    assert_eq!(
        paths,
        vec![0x8000, 0x0001, 0x7FFF],
        "signed-short order: -32768, 1, 32767"
    );
}

/// Mixed-stream reconciliation (issue #927 item 6): a column carrying BOTH a
/// whole-column op and per-element ops keeps the newer stream by timestamp,
/// rather than emitting both (which would double-write and desync the reader).
#[test]
fn udt_mixed_stream_reconciliation_newer_wins() {
    let writer = DataWriter::new(create_test_stats());
    let mut schema = create_test_schema();
    schema
        .columns
        .push(udt_column("addr", &person_udt_marshal()));

    let table_id = TableId::new("test_ks", "test_table");
    let pk = PartitionKey::single("id", Value::Integer(7));

    // Older whole-column UDT write (ts 1_000_000) ...
    let whole = Mutation::new(
        table_id.clone(),
        pk.clone(),
        None,
        vec![CellOperation::Write {
            column: "addr".to_string(),
            value: Value::Udt(crate::types::UdtValue {
                type_name: "person".to_string(),
                keyspace: "test_ks".to_string(),
                fields: vec![udt_field("name", Some(Value::Text("Old".to_string())))],
            }),
        }],
        1_000_000,
        None,
    );
    // ... shadowed by a NEWER per-element edit (ts 2_000_000).
    let per_elem = Mutation::new(
        table_id,
        pk,
        None,
        vec![CellOperation::WriteComplexElement {
            column: "addr".to_string(),
            cell_path: 0u16.to_be_bytes().to_vec(),
            value: Some(Value::Text("New".to_string())),
            timestamp_micros: 2_000_000,
            ttl_seconds: None,
            local_deletion_time: None,
            is_deleted: false,
        }],
        2_000_000,
        None,
    );

    let _ = &writer; // writer not needed: merge_row_group is associated
    let row = DataWriter::merge_row_group(&[&whole, &per_elem], &schema, false, None)
        .expect("merge must produce a row");

    // The per-element stream is newer, so the whole-column op is dropped and
    // the per-element op survives — no double-write.
    assert!(
        !row.ops
            .iter()
            .any(|m| merged_op_column(m.op) == Some("addr")),
        "older whole-column UDT op must be shadowed by newer per-element edit"
    );
    assert_eq!(
        row.complex_element_ops.len(),
        1,
        "newer per-element edit survives"
    );
}

/// Issue #887: a row-tombstone mutation that ALSO carries a complex-deletion
/// marker whose `marked_for_delete_at` STRICTLY exceeds the row-tombstone time
/// must NOT have that marker shadowed out of `merge_row_group`. The row tombstone
/// covers only `timestamp <= row_del`; the marker covers `(row_del, mfda]` — so
/// it must survive into the written row alongside the row deletion.
#[test]
fn merge_row_group_keeps_strictly_newer_complex_deletion_on_row_tombstone() {
    let schema = complex_column_schema();
    const ROW_DEL: i64 = 100;
    const MFDA: i64 = 300; // strictly greater than ROW_DEL

    let mutation = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![
            CellOperation::DeleteRow,
            CellOperation::ComplexDeletion {
                column: "tags".to_string(),
                marked_for_delete_at: MFDA,
                local_deletion_time: 1_700_000_000,
            },
        ],
        ROW_DEL,
        None,
    );

    let row = DataWriter::merge_row_group(&[&mutation], &schema, false, None)
        .expect("a row tombstone + surviving complex deletion must produce a row");

    assert!(
        row.row_deletion.is_some(),
        "the row tombstone must be preserved"
    );
    assert_eq!(row.row_deletion.map(|(ts, _)| ts), Some(ROW_DEL));

    let kept_marker = row.complex_element_ops.iter().any(|mop| {
        matches!(
            mop.op,
            CellOperation::ComplexDeletion {
                column,
                marked_for_delete_at,
                ..
            } if column == "tags" && *marked_for_delete_at == MFDA
        )
    });
    assert!(
        kept_marker,
        "the strictly-newer (mfda > row_del) complex deletion marker must survive \
             into the written row (else (row_del, mfda] elements resurrect)"
    );
}
