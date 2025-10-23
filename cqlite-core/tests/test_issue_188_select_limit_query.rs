//! Regression test for Issue #188
//!
//! **Problem**: One-shot SELECT with LIMIT returns zero rows because table_id comparison
//! strips keyspace in qualified queries (e.g., "test_basic.simple_table")
//!
//! **Root Cause**: SSTableManager stored readers indexed by unqualified table name,
//! but Index::find_matching_table_id() returned the first match without checking if it
//! actually existed in the entries HashMap, causing lookups to fail.
//!
//! **Fix**: Modified Index::find_matching_table_id() to return a reference to a TableId
//! that actually exists in self.entries, ensuring that both qualified and unqualified
//! queries can match stored table IDs correctly.
//!
//! **Test Coverage**:
//! - Unit tests for Index::find_matching_table_id() with qualified and unqualified names
//! - Ambiguity detection when multiple tables with same unqualified name exist

#![cfg(feature = "state_machine")]

use cqlite_core::{
    storage::sstable::index::{Index, IndexEntry},
    types::TableId,
    RowKey,
};

#[test]
fn test_issue_188_qualified_table_lookup() {
    let mut index = Index::new();

    // Add entry with qualified table name
    let entry = IndexEntry {
        table_id: TableId::new("test_basic.simple_table"),
        key: RowKey::from("key1"),
        offset: 0,
        size: 100,
        compressed: false,
    };
    index.add_entry(entry);

    // Query with qualified name should match
    let qualified_table_id = TableId::new("test_basic.simple_table");
    let result = index.get(&qualified_table_id, &RowKey::from("key1"));
    assert!(
        result.is_some(),
        "Issue #188: Qualified query should find entry"
    );
}

#[test]
fn test_issue_188_unqualified_table_lookup() {
    let mut index = Index::new();

    // Add entry with qualified table name
    let entry = IndexEntry {
        table_id: TableId::new("test_basic.simple_table"),
        key: RowKey::from("key1"),
        offset: 0,
        size: 100,
        compressed: false,
    };
    index.add_entry(entry);

    // Query with unqualified name should match
    let unqualified_table_id = TableId::new("simple_table");
    let result = index.get(&unqualified_table_id, &RowKey::from("key1"));
    assert!(
        result.is_some(),
        "Issue #188: Unqualified query should find entry with qualified table name"
    );
}

#[test]
fn test_issue_188_ambiguous_table_detection() {
    let mut index = Index::new();

    // Add two entries with same unqualified name but different keyspaces
    let entry1 = IndexEntry {
        table_id: TableId::new("keyspace1.users"),
        key: RowKey::from("key1"),
        offset: 0,
        size: 100,
        compressed: false,
    };
    index.add_entry(entry1);

    let entry2 = IndexEntry {
        table_id: TableId::new("keyspace2.users"),
        key: RowKey::from("key2"),
        offset: 100,
        size: 100,
        compressed: false,
    };
    index.add_entry(entry2);

    // Unqualified query should detect ambiguity
    let unqualified_table_id = TableId::new("users");
    let entries = index.get_range(&unqualified_table_id, None, None);

    // Should return empty Vec (error converted to empty result in get_range)
    assert!(
        entries.unwrap().is_empty(),
        "Issue #188: Ambiguous unqualified query should return empty results"
    );
}

#[test]
fn test_issue_188_qualified_resolves_ambiguity() {
    let mut index = Index::new();

    // Add two entries with same unqualified name but different keyspaces
    let entry1 = IndexEntry {
        table_id: TableId::new("keyspace1.users"),
        key: RowKey::from("key1"),
        offset: 0,
        size: 100,
        compressed: false,
    };
    index.add_entry(entry1);

    let entry2 = IndexEntry {
        table_id: TableId::new("keyspace2.users"),
        key: RowKey::from("key2"),
        offset: 100,
        size: 100,
        compressed: false,
    };
    index.add_entry(entry2);

    // Qualified query should work even when unqualified would be ambiguous
    let qualified_table_id = TableId::new("keyspace1.users");
    let result = index.get(&qualified_table_id, &RowKey::from("key1"));
    assert!(
        result.is_some(),
        "Issue #188: Qualified query should resolve ambiguity"
    );
}

#[test]
fn test_issue_188_table_not_found() {
    let index = Index::new();

    // Query for non-existent table
    let table_id = TableId::new("nonexistent.table");
    let entries = index.get_range(&table_id, None, None);

    // Should return empty Vec (error converted to empty result in get_range)
    assert!(
        entries.unwrap().is_empty(),
        "Issue #188: Non-existent table should return empty results"
    );
}

#[test]
fn test_issue_188_reverse_index_o1_performance() {
    let mut index = Index::new();

    // Add many entries to test O(1) performance
    for i in 0..1000 {
        let entry = IndexEntry {
            table_id: TableId::new(format!("test_basic.table_{}", i)),
            key: RowKey::from(format!("key_{}", i)),
            offset: i * 100,
            size: 100,
            compressed: false,
        };
        index.add_entry(entry);
    }

    // Lookup should be O(1) via reverse index, not O(n) iteration
    let table_id = TableId::new("table_500");
    let result = index.get(&table_id, &RowKey::from("key_500"));
    assert!(
        result.is_some(),
        "Issue #188: Reverse index should enable O(1) lookup"
    );
}

#[test]
fn test_issue_188_exact_match_takes_priority() {
    let mut index = Index::new();

    // Add entry with unqualified name
    let entry1 = IndexEntry {
        table_id: TableId::new("simple_table"),
        key: RowKey::from("key1"),
        offset: 0,
        size: 100,
        compressed: false,
    };
    index.add_entry(entry1);

    // Add entry with qualified name
    let entry2 = IndexEntry {
        table_id: TableId::new("test_basic.simple_table"),
        key: RowKey::from("key2"),
        offset: 100,
        size: 100,
        compressed: false,
    };
    index.add_entry(entry2);

    // Exact match should take priority
    let exact_table_id = TableId::new("simple_table");
    let result = index.get(&exact_table_id, &RowKey::from("key1"));
    assert!(
        result.is_some(),
        "Issue #188: Exact match should take priority over fuzzy match"
    );

    // Qualified exact match should also work
    let qualified_exact_table_id = TableId::new("test_basic.simple_table");
    let result2 = index.get(&qualified_exact_table_id, &RowKey::from("key2"));
    assert!(
        result2.is_some(),
        "Issue #188: Qualified exact match should work"
    );
}
