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

    /// Reverse index: unqualified table name -> list of qualified TableIds
    /// Enables O(1) unqualified name resolution instead of O(n) HashMap iteration
    unqualified_to_qualified: HashMap<String, Vec<TableId>>,

    /// Total number of entries
    total_entries: usize,
}

impl Index {
    /// Create a new empty index
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            sorted_keys: HashMap::new(),
            unqualified_to_qualified: HashMap::new(),
            total_entries: 0,
        }
    }

    /// Create index from a vector of entries.
    ///
    /// Issue #2385: builds the per-table `sorted_keys` by collecting each table's
    /// unique keys and sorting ONCE (O(N log N) total) rather than the per-entry
    /// `binary_search` + `Vec::insert` of [`Self::add_entry`], which is O(N²) when
    /// keys do not arrive in ascending byte order (real `Index.db` arrives in token
    /// order, but `RowKey` sorts by raw bytes, so every insert lands mid-vector).
    /// The result is byte-for-byte identical to feeding the same entries through
    /// `add_entry` one at a time: last write wins per `(table_id, key)`, duplicate
    /// keys collapse in `sorted_keys`, and `total_entries` counts input entries.
    pub fn from_entries(entries: Vec<IndexEntry>) -> Self {
        let mut index = Self::new();
        // `total_entries` matches `add_entry`'s call-count semantics (incremented
        // unconditionally, including for a duplicate key).
        index.total_entries = entries.len();

        for entry in entries {
            let table_id = entry.table_id.clone();

            // Reverse index: unqualified name -> qualified TableId (dedup).
            let table_name = table_id.name();
            let unqualified = if let Some(dot_pos) = table_name.rfind('.') {
                table_name[dot_pos + 1..].to_string()
            } else {
                table_name.to_string()
            };
            let qualified_list = index
                .unqualified_to_qualified
                .entry(unqualified)
                .or_default();
            if !qualified_list.contains(&table_id) {
                qualified_list.push(table_id.clone());
            }

            // Entries map: last write wins per key (matches add_entry).
            index
                .entries
                .entry(table_id)
                .or_default()
                .insert(entry.key.clone(), entry);
        }

        // Sort each table's keys ONCE. The keys come from the entries map, so they
        // are already unique (duplicates collapsed) — sorting yields the same
        // ascending byte order add_entry maintained incrementally.
        for (table_id, table_entries) in &index.entries {
            let mut keys: Vec<RowKey> = table_entries.keys().cloned().collect();
            keys.sort();
            index.sorted_keys.insert(table_id.clone(), keys);
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
            .or_default()
            .insert(key.clone(), entry);

        // Add to sorted keys
        let sorted_keys = self.sorted_keys.entry(table_id.clone()).or_default();

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

        // Update reverse index: unqualified name -> qualified TableId
        let table_name = table_id.name();
        let unqualified = if let Some(dot_pos) = table_name.rfind('.') {
            table_name[dot_pos + 1..].to_string()
        } else {
            table_name.to_string()
        };

        let qualified_list = self
            .unqualified_to_qualified
            .entry(unqualified)
            .or_default();

        // Only add if not already present (avoid duplicates)
        if !qualified_list.contains(&table_id) {
            qualified_list.push(table_id);
        }

        self.total_entries += 1;
    }

    /// Get an entry by table and key
    pub fn get(&self, table_id: &TableId, key: &RowKey) -> Option<&IndexEntry> {
        // Find matching table_id (handles qualified/unqualified matching)
        let actual_table_id = self.find_matching_table_id(table_id).ok()?;
        self.entries.get(actual_table_id)?.get(key)
    }

    /// Find an entry by table and key (alias for get)
    pub async fn find_entry(
        &self,
        table_id: &TableId,
        key: &RowKey,
    ) -> Result<Option<&IndexEntry>> {
        Ok(self.get(table_id, key))
    }

    /// Find the actual table_id stored in the index that matches the query table_id.
    ///
    /// This handles flexible matching between qualified (keyspace.table) and unqualified (table) formats:
    /// - Exact match has priority (O(1))
    /// - Falls back to matching unqualified table names (O(1) via reverse index)
    /// - Detects and returns error for ambiguous unqualified names
    ///
    /// # Examples
    /// - Query "test_basic.simple_table" matches stored "test_basic.simple_table" (exact)
    /// - Query "simple_table" matches stored "test_basic.simple_table" (unqualified → qualified)
    /// - Query "test_basic.simple_table" matches stored "simple_table" (qualified → unqualified)
    ///
    /// # Errors
    /// - Returns `Error::table_not_found()` if no matching table exists
    /// - Returns `Error::ambiguous_table()` if unqualified name matches multiple tables
    ///
    /// Note: This always returns a reference to a TableId stored in self.entries, never the query_table_id parameter.
    fn find_matching_table_id(&self, query_table_id: &TableId) -> Result<&TableId> {
        let query_name = query_table_id.name();

        // Fast path: exact match - return the key from self.entries, not query_table_id
        if let Some((stored_key, _)) = self.entries.get_key_value(query_table_id) {
            return Ok(stored_key);
        }

        // Extract unqualified query name
        let query_unqualified = if let Some(dot_pos) = query_name.rfind('.') {
            &query_name[dot_pos + 1..]
        } else {
            query_name
        };

        // O(1) reverse index lookup (not O(n) iteration!)
        let matches = self
            .unqualified_to_qualified
            .get(query_unqualified)
            .ok_or_else(|| crate::Error::table_not_found(query_name))?;

        match matches.len() {
            0 => Err(crate::Error::table_not_found(query_name)),
            1 => Ok(&matches[0]),
            _ => {
                // Multiple tables with same unqualified name - ambiguous
                let table_names: Vec<String> =
                    matches.iter().map(|t| t.name().to_string()).collect();
                Err(crate::Error::ambiguous_table(format!(
                    "Table '{}' is ambiguous. Found in: {}. Use qualified name (keyspace.table).",
                    query_unqualified,
                    table_names.join(", ")
                )))
            }
        }
    }

    /// Get entries for a key range
    pub fn get_range(
        &self,
        table_id: &TableId,
        start_key: Option<&RowKey>,
        end_key: Option<&RowKey>,
    ) -> Result<Vec<&IndexEntry>> {
        // Find the actual table_id stored in the index that matches the query table_id
        // This handles both qualified (keyspace.table) and unqualified (table) formats
        // Returns error for not found or ambiguous tables
        let actual_table_id = match self.find_matching_table_id(table_id) {
            Ok(tid) => tid,
            Err(_) => return Ok(Vec::new()), // Table not found, return empty results
        };

        let table_entries = match self.entries.get(actual_table_id) {
            Some(entries) => entries,
            None => return Ok(Vec::new()),
        };

        let sorted_keys = match self.sorted_keys.get(actual_table_id) {
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

        // Find end position (exclusive)
        let end_pos = match end_key {
            Some(key) => match sorted_keys.binary_search(key) {
                Ok(pos) => pos,  // End key found - exclude it from results
                Err(pos) => pos, // End key not found - use insertion point
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
        self.unqualified_to_qualified.clear();
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

    /// Load index from a file/reader
    pub async fn load<R: tokio::io::AsyncRead + Unpin>(_reader: &mut R) -> Result<Self> {
        // For now, return an empty index as a placeholder
        // In a real implementation, this would deserialize the index from the reader
        Ok(Self::new())
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
