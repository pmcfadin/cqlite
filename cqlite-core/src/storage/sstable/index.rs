//! Index implementation for SSTable fast lookups

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::{types::TableId, Result, RowKey};

/// Index entry for fast key lookups
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexEntry {
    /// Table identifier
    pub table_id: TableId,

    /// Row key
    pub key: RowKey,

    /// Offset in the SSTable file
    pub offset: u64,

    /// Size of the entry in bytes
    pub size: u32,

    /// Whether the entry is compressed
    pub compressed: bool,
}

/// In-memory index for fast lookups
#[derive(Debug, Clone)]
pub struct Index {
    /// Entries organized by table and key
    entries: HashMap<TableId, HashMap<RowKey, IndexEntry>>,

    /// Sorted keys for range queries
    sorted_keys: HashMap<TableId, Vec<RowKey>>,

    /// Total number of entries
    total_entries: usize,
}

impl Index {
    /// Create a new empty index
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            sorted_keys: HashMap::new(),
            total_entries: 0,
        }
    }

    /// Create index from a vector of entries
    pub fn from_entries(entries: Vec<IndexEntry>) -> Self {
        let mut index = Self::new();

        for entry in entries {
            index.add_entry(entry);
        }

        index
    }

    /// Add an entry to the index
    pub fn add_entry(&mut self, entry: IndexEntry) {
        let table_id = entry.table_id.clone();
        let key = entry.key.clone();

        // Add to entries map
        self.entries
            .entry(table_id.clone())
            .or_insert_with(HashMap::new)
            .insert(key.clone(), entry);

        // Add to sorted keys
        let sorted_keys = self.sorted_keys.entry(table_id).or_insert_with(Vec::new);

        // Insert in sorted order
        match sorted_keys.binary_search(&key) {
            Ok(_) => {
                // Key already exists, replace it
                // This shouldn't happen in normal operation
            }
            Err(pos) => {
                sorted_keys.insert(pos, key);
            }
        }

        self.total_entries += 1;
    }

    /// Get an entry by table and key
    pub fn get(&self, table_id: &TableId, key: &RowKey) -> Option<&IndexEntry> {
        self.entries.get(table_id)?.get(key)
    }

    /// Get entries for a key range
    pub fn get_range(
        &self,
        table_id: &TableId,
        start_key: Option<&RowKey>,
        end_key: Option<&RowKey>,
    ) -> Result<Vec<&IndexEntry>> {
        let table_entries = match self.entries.get(table_id) {
            Some(entries) => entries,
            None => return Ok(Vec::new()),
        };

        let sorted_keys = match self.sorted_keys.get(table_id) {
            Some(keys) => keys,
            None => return Ok(Vec::new()),
        };

        let mut result = Vec::new();

        // Find start position
        let start_pos = match start_key {
            Some(key) => match sorted_keys.binary_search(key) {
                Ok(pos) => pos,
                Err(pos) => pos,
            },
            None => 0,
        };

        // Find end position
        let end_pos = match end_key {
            Some(key) => match sorted_keys.binary_search(key) {
                Ok(pos) => pos + 1,
                Err(pos) => pos,
            },
            None => sorted_keys.len(),
        };

        // Collect entries in range
        for i in start_pos..end_pos {
            if let Some(key) = sorted_keys.get(i) {
                if let Some(entry) = table_entries.get(key) {
                    result.push(entry);
                }
            }
        }

        Ok(result)
    }

    /// Get all entries for a table
    pub fn get_table_entries(&self, table_id: &TableId) -> Option<&HashMap<RowKey, IndexEntry>> {
        self.entries.get(table_id)
    }

    /// Get all table IDs
    pub fn get_table_ids(&self) -> Vec<&TableId> {
        self.entries.keys().collect()
    }

    /// Get statistics
    pub fn stats(&self) -> IndexStats {
        let table_count = self.entries.len();
        let mut total_entries = 0;
        let mut avg_entries_per_table = 0.0;

        for table_entries in self.entries.values() {
            total_entries += table_entries.len();
        }

        if table_count > 0 {
            avg_entries_per_table = total_entries as f64 / table_count as f64;
        }

        IndexStats {
            table_count,
            total_entries,
            avg_entries_per_table,
        }
    }

    /// Clear all entries
    pub fn clear(&mut self) {
        self.entries.clear();
        self.sorted_keys.clear();
        self.total_entries = 0;
    }

    /// Check if index is empty
    pub fn is_empty(&self) -> bool {
        self.total_entries == 0
    }

    /// Get total number of entries
    pub fn len(&self) -> usize {
        self.total_entries
    }
}

/// Type alias for SSTableIndex
pub type SSTableIndex = Index;

/// Index statistics
#[derive(Debug, Clone)]
pub struct IndexStats {
    /// Number of tables in the index
    pub table_count: usize,

    /// Total number of entries
    pub total_entries: usize,

    /// Average entries per table
    pub avg_entries_per_table: f64,
}

impl Default for Index {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TableId;

    #[test]
    fn test_index_creation() {
        let index = Index::new();
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);
    }

    #[test]
    fn test_index_add_entry() {
        let mut index = Index::new();

        let entry = IndexEntry {
            table_id: TableId::new("test_table"),
            key: RowKey::from("test_key"),
            offset: 0,
            size: 100,
            compressed: false,
        };

        index.add_entry(entry.clone());

        assert_eq!(index.len(), 1);
        assert!(!index.is_empty());

        let retrieved = index.get(&entry.table_id, &entry.key);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().offset, 0);
        assert_eq!(retrieved.unwrap().size, 100);
    }

    #[test]
    fn test_index_range_query() {
        let mut index = Index::new();
        let table_id = TableId::new("test_table");

        // Add entries
        for i in 0..10 {
            let entry = IndexEntry {
                table_id: table_id.clone(),
                key: RowKey::from(format!("key_{:03}", i)),
                offset: i * 100,
                size: 100,
                compressed: false,
            };
            index.add_entry(entry);
        }

        // Test range query
        let start_key = RowKey::from("key_003");
        let end_key = RowKey::from("key_007");

        let range_entries = index
            .get_range(&table_id, Some(&start_key), Some(&end_key))
            .unwrap();
        assert_eq!(range_entries.len(), 4); // key_003, key_004, key_005, key_006

        // Test open-ended range
        let range_entries = index.get_range(&table_id, Some(&start_key), None).unwrap();
        assert_eq!(range_entries.len(), 7); // key_003 to key_009
    }

    #[test]
    fn test_index_stats() {
        let mut index = Index::new();
        let table_id = TableId::new("test_table");

        // Add entries
        for i in 0..5 {
            let entry = IndexEntry {
                table_id: table_id.clone(),
                key: RowKey::from(format!("key_{}", i)),
                offset: i * 100,
                size: 100,
                compressed: false,
            };
            index.add_entry(entry);
        }

        let stats = index.stats();
        assert_eq!(stats.table_count, 1);
        assert_eq!(stats.total_entries, 5);
        assert_eq!(stats.avg_entries_per_table, 5.0);
    }
}
