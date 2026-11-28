//! `MergeEntry` ordering and variant tests (split out of `merge/mod.rs` `tests`, #4658).

use super::*;
use crate::storage::write_engine::mutation::DecoratedKey;

#[test]
fn test_merge_entry_ordering_by_token() {
    let entry1 = MergeEntry::new(
        0,
        DecoratedKey::new(100, vec![1, 2, 3]),
        None,
        1000,
        RowData::Live { cells: vec![] },
    );

    let entry2 = MergeEntry::new(
        0,
        DecoratedKey::new(200, vec![1, 2, 3]),
        None,
        1000,
        RowData::Live { cells: vec![] },
    );

    // Entry with lower token should come first
    assert!(entry1 < entry2);
    assert!(entry2 > entry1);
}

#[test]
fn test_merge_entry_ordering_by_key_bytes() {
    // Same token, different key bytes (hash collision)
    let entry1 = MergeEntry::new(
        0,
        DecoratedKey::new(100, vec![1, 2, 3]),
        None,
        1000,
        RowData::Live { cells: vec![] },
    );

    let entry2 = MergeEntry::new(
        0,
        DecoratedKey::new(100, vec![1, 2, 4]),
        None,
        1000,
        RowData::Live { cells: vec![] },
    );

    // Entry with smaller key bytes should come first
    assert!(entry1 < entry2);
    assert!(entry2 > entry1);
}

#[test]
fn test_merge_entry_ordering_by_run_index() {
    // Same token and key, different run indices
    let entry1 = MergeEntry::new(
        0,
        DecoratedKey::new(100, vec![1, 2, 3]),
        None,
        1000,
        RowData::Live { cells: vec![] },
    );

    let entry2 = MergeEntry::new(
        1,
        DecoratedKey::new(100, vec![1, 2, 3]),
        None,
        1000,
        RowData::Live { cells: vec![] },
    );

    // Entry with lower run_index should come first (newer file wins)
    assert!(entry1 < entry2);
    assert!(entry2 > entry1);
}

#[test]
fn test_merge_entry_min_heap() {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    let mut heap: BinaryHeap<Reverse<MergeEntry>> = BinaryHeap::new();

    // Insert in reverse order
    let entry3 = MergeEntry::new(
        0,
        DecoratedKey::new(300, vec![3]),
        None,
        1000,
        RowData::Live { cells: vec![] },
    );
    let entry1 = MergeEntry::new(
        0,
        DecoratedKey::new(100, vec![1]),
        None,
        1000,
        RowData::Live { cells: vec![] },
    );
    let entry2 = MergeEntry::new(
        0,
        DecoratedKey::new(200, vec![2]),
        None,
        1000,
        RowData::Live { cells: vec![] },
    );

    heap.push(Reverse(entry3.clone()));
    heap.push(Reverse(entry1.clone()));
    heap.push(Reverse(entry2.clone()));

    // Should pop in ascending order
    assert_eq!(heap.pop().unwrap().0.key.token, 100);
    assert_eq!(heap.pop().unwrap().0.key.token, 200);
    assert_eq!(heap.pop().unwrap().0.key.token, 300);
}

#[test]
fn test_row_data_variants() {
    let live = RowData::Live {
        cells: vec![CellData {
            column: "name".to_string(),
            value: Value::text("Alice".to_string()),
            timestamp: 1000,
            ttl: None,
            cell_path: None,
            local_deletion_time: None,
            is_complex_element: false,
            is_deleted: false,
            has_empty_value: false,
        }],
    };

    match live {
        RowData::Live { cells } => {
            assert_eq!(cells.len(), 1);
            assert_eq!(cells[0].column, "name");
        }
        _ => panic!("Expected Live variant"),
    }

    let tombstone = RowData::Tombstone {
        deletion_time: 2000,
        local_deletion_time: 1000,
    };

    match tombstone {
        RowData::Tombstone {
            deletion_time,
            local_deletion_time,
        } => {
            assert_eq!(deletion_time, 2000);
            assert_eq!(local_deletion_time, 1000);
        }
        _ => panic!("Expected Tombstone variant"),
    }
}

#[test]
fn test_cell_data_creation() {
    let cell = CellData {
        column: "age".to_string(),
        value: Value::Integer(30),
        timestamp: 1234567890,
        ttl: Some(3600),
        cell_path: None,
        local_deletion_time: None,
        is_complex_element: false,
        is_deleted: false,
        has_empty_value: false,
    };

    assert_eq!(cell.column, "age");
    assert_eq!(cell.value, Value::Integer(30));
    assert_eq!(cell.timestamp, 1234567890);
    assert_eq!(cell.ttl, Some(3600));
}

#[test]
fn test_merge_stats_creation() {
    let stats = MergeStats {
        input_files: 5,
        output_partitions: 1000,
        output_rows: 5000,
        bytes_written: 1024 * 1024,
        elapsed: Duration::from_secs(10),
        dropped_whole: Vec::new(),
    };

    assert_eq!(stats.input_files, 5);
    assert_eq!(stats.output_partitions, 1000);
    assert_eq!(stats.output_rows, 5000);
    assert_eq!(stats.bytes_written, 1024 * 1024);
    assert_eq!(stats.elapsed.as_secs(), 10);
}

#[test]
fn test_run_reader_estimate_entry_size() {
    let entry = MergeEntry::new(
        0,
        DecoratedKey::new(100, vec![1, 2, 3, 4]),
        None,
        1000,
        RowData::Live {
            cells: vec![CellData {
                column: "name".to_string(),
                value: Value::text("Alice".to_string()),
                timestamp: 1000,
                ttl: None,
                cell_path: None,
                local_deletion_time: None,
                is_complex_element: false,
                is_deleted: false,
                has_empty_value: false,
            }],
        },
    );

    let size = RunReader::estimate_entry_size(&entry);

    // Size should be at least the base struct size plus key bytes
    let expected_min_size = std::mem::size_of::<MergeEntry>() + 4;
    assert!(size >= expected_min_size);
}

#[test]
fn test_kway_merger_empty_input() {
    use crate::schema::{KeyColumn, TableSchema};
    use std::collections::HashMap;

    let schema = TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let result = KWayMerger::new(vec![], &schema);
    assert!(result.is_err());

    if let Err(Error::InvalidInput(msg)) = result {
        assert!(msg.contains("at least one input file"));
    } else {
        panic!("Expected InvalidInput error");
    }
}

#[test]
fn test_merge_entry_equal_timestamps_prefer_lower_run_index() {
    // Same partition, same clustering, same timestamp
    // Lower run_index should win (newer file)
    let entry_run0 = MergeEntry::new(
        0, // run_index 0 (newer)
        DecoratedKey::new(100, vec![1, 2, 3]),
        None,
        1000, // same timestamp
        RowData::Live {
            cells: vec![CellData {
                column: "name".to_string(),
                value: Value::text("Newer".to_string()),
                timestamp: 1000,
                ttl: None,
                cell_path: None,
                local_deletion_time: None,
                is_complex_element: false,
                is_deleted: false,
                has_empty_value: false,
            }],
        },
    );

    let entry_run1 = MergeEntry::new(
        1, // run_index 1 (older)
        DecoratedKey::new(100, vec![1, 2, 3]),
        None,
        1000, // same timestamp
        RowData::Live {
            cells: vec![CellData {
                column: "name".to_string(),
                value: Value::text("Older".to_string()),
                timestamp: 1000,
                ttl: None,
                cell_path: None,
                local_deletion_time: None,
                is_complex_element: false,
                is_deleted: false,
                has_empty_value: false,
            }],
        },
    );

    // Entry from run 0 should come first in ordering
    assert!(entry_run0 < entry_run1);
}

#[test]
fn test_merge_entry_tombstone() {
    let tombstone_entry = MergeEntry::new(
        0,
        DecoratedKey::new(100, vec![1, 2, 3]),
        None,
        2000,
        RowData::Tombstone {
            deletion_time: 2000,
            local_deletion_time: 1000,
        },
    );

    match tombstone_entry.row_data {
        RowData::Tombstone {
            deletion_time,
            local_deletion_time,
        } => {
            assert_eq!(deletion_time, 2000);
            assert_eq!(local_deletion_time, 1000);
        }
        _ => panic!("Expected Tombstone"),
    }
}
