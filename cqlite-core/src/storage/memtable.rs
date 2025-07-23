//! In-memory write buffer implementation
//!
//! MemTable provides fast in-memory storage for recent writes before they are
//! flushed to SSTables. It uses a BTreeMap for efficient key-value storage
//! and range queries.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{types::TableId, Config, Result, RowKey, Value};

/// Entry in the MemTable with metadata
#[derive(Debug, Clone)]
pub struct MemTableEntry {
    /// The actual value (or None for tombstones)
    pub value: Option<Value>,
    /// Timestamp when this entry was created (microseconds since Unix epoch)
    pub timestamp: u64,
    /// Sequence number for ordering
    pub sequence: u64,
}

impl MemTableEntry {
    /// Create a new entry with current timestamp
    pub fn new(value: Option<Value>, sequence: u64) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        Self {
            value,
            timestamp,
            sequence,
        }
    }

    /// Check if this entry is a tombstone (deleted)
    pub fn is_tombstone(&self) -> bool {
        self.value.is_none()
    }
}

/// In-memory write buffer for fast writes
#[derive(Debug)]
pub struct MemTable {
    /// Map from (table_id, row_key) to entry
    data: BTreeMap<(TableId, RowKey), MemTableEntry>,
    /// Approximate size in bytes
    size: AtomicU64,
    /// Global sequence number counter
    sequence: AtomicU64,
    /// Configuration
    config: Config,
}

impl MemTable {
    /// Create a new empty MemTable
    pub fn new(config: &Config) -> Result<Self> {
        Ok(Self {
            data: BTreeMap::new(),
            size: AtomicU64::new(0),
            sequence: AtomicU64::new(0),
            config: config.clone(),
        })
    }

    /// Insert a key-value pair
    pub fn put(&mut self, table_id: &TableId, key: RowKey, value: Value) -> Result<()> {
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);
        let entry = MemTableEntry::new(Some(value), sequence);

        // Calculate size increase
        let key_size = table_id.name().len() + key.len();
        let value_size = self.estimate_value_size(&entry.value);
        let entry_size = key_size + value_size + 24; // 24 bytes for metadata

        // Insert or update entry
        let composite_key = (table_id.clone(), key);
        let size_delta = if let Some(old_entry) = self.data.insert(composite_key, entry) {
            // Replace existing entry - calculate size difference
            let old_size = key_size + self.estimate_value_size(&old_entry.value) + 24;
            entry_size as i64 - old_size as i64
        } else {
            // New entry
            entry_size as i64
        };

        // Update size
        if size_delta > 0 {
            self.size.fetch_add(size_delta as u64, Ordering::SeqCst);
        } else {
            self.size.fetch_sub((-size_delta) as u64, Ordering::SeqCst);
        }

        Ok(())
    }

    /// Get a value by key
    pub fn get(&self, table_id: &TableId, key: &RowKey) -> Result<Option<Value>> {
        let composite_key = (table_id.clone(), key.clone());

        if let Some(entry) = self.data.get(&composite_key) {
            if entry.is_tombstone() {
                // Return None for tombstones (deleted entries)
                Ok(None)
            } else {
                Ok(entry.value.clone())
            }
        } else {
            Ok(None)
        }
    }

    /// Delete a key (insert tombstone)
    pub fn delete(&mut self, table_id: &TableId, key: RowKey) -> Result<()> {
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst);
        let entry = MemTableEntry::new(None, sequence);

        // Calculate size for tombstone
        let key_size = table_id.name().len() + key.len();
        let entry_size = key_size + 24; // 24 bytes for metadata, no value

        // Insert tombstone
        let composite_key = (table_id.clone(), key);
        let size_delta = if let Some(old_entry) = self.data.insert(composite_key, entry) {
            // Replace existing entry with tombstone
            let old_size = key_size + self.estimate_value_size(&old_entry.value) + 24;
            entry_size as i64 - old_size as i64
        } else {
            // New tombstone
            entry_size as i64
        };

        // Update size
        if size_delta > 0 {
            self.size.fetch_add(size_delta as u64, Ordering::SeqCst);
        } else {
            self.size.fetch_sub((-size_delta) as u64, Ordering::SeqCst);
        }

        Ok(())
    }

    /// Scan a range of keys
    pub fn scan(
        &self,
        table_id: &TableId,
        start_key: Option<&RowKey>,
        end_key: Option<&RowKey>,
        limit: Option<usize>,
    ) -> Result<Vec<(RowKey, Value)>> {
        let mut results = Vec::new();
        let mut count = 0;

        // Create range bounds
        let start_bound = start_key.map(|k| (table_id.clone(), k.clone()));
        let end_bound = end_key.map(|k| (table_id.clone(), k.clone()));

        // Iterate through matching entries
        for ((tid, key), entry) in &self.data {
            // Check table match
            if tid != table_id {
                continue;
            }

            // Check range bounds
            if let Some((ref start_tid, ref start_key)) = start_bound {
                if (tid, key) < (start_tid, start_key) {
                    continue;
                }
            }

            if let Some((ref end_tid, ref end_key)) = end_bound {
                if (tid, key) >= (end_tid, end_key) {
                    break;
                }
            }

            // Skip tombstones
            if entry.is_tombstone() {
                continue;
            }

            // Add to results
            if let Some(ref value) = entry.value {
                results.push((key.clone(), value.clone()));
                count += 1;

                // Check limit
                if let Some(limit) = limit {
                    if count >= limit {
                        break;
                    }
                }
            }
        }

        Ok(results)
    }

    /// Get current size in bytes
    pub fn size(&self) -> u64 {
        self.size.load(Ordering::SeqCst)
    }

    /// Get number of entries
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the MemTable is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Flush all data and return it
    pub fn flush(&mut self) -> Result<Vec<(TableId, RowKey, Value)>> {
        let mut result = Vec::new();

        // Collect all non-tombstone entries
        for ((table_id, key), entry) in &self.data {
            if !entry.is_tombstone() {
                if let Some(ref value) = entry.value {
                    result.push((table_id.clone(), key.clone(), value.clone()));
                }
            }
        }

        // Sort by table_id, then by key for consistent ordering
        result.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

        Ok(result)
    }

    /// Get statistics about the MemTable
    pub fn stats(&self) -> MemTableStats {
        let mut table_counts = std::collections::HashMap::new();
        let mut tombstone_count = 0;

        for ((table_id, _), entry) in &self.data {
            if entry.is_tombstone() {
                tombstone_count += 1;
            } else {
                *table_counts.entry(table_id.clone()).or_insert(0) += 1;
            }
        }

        MemTableStats {
            entry_count: self.data.len(),
            size_bytes: self.size.load(Ordering::SeqCst),
            table_count: table_counts.len(),
            tombstone_count,
            sequence_number: self.sequence.load(Ordering::SeqCst),
        }
    }

    /// Estimate the size of a value in bytes
    fn estimate_value_size(&self, value: &Option<Value>) -> usize {
        match value {
            None => 0,
            Some(Value::Null) => 1,
            Some(Value::Boolean(_)) => 1,
            Some(Value::Integer(_)) => 4,
            Some(Value::BigInt(_)) => 8,
            Some(Value::Float(_)) => 8,
            Some(Value::Text(s)) => s.len(),
            Some(Value::Blob(b)) => b.len(),
            Some(Value::Timestamp(_)) => 8,
            Some(Value::Uuid(_)) => 16,
            Some(Value::Json(j)) => j.to_string().len(),
            Some(Value::List(l)) => l
                .iter()
                .map(|v| self.estimate_value_size(&Some(v.clone())))
                .sum(),
            Some(Value::Map(m)) => m
                .iter()
                .map(|(k, v)| {
                    self.estimate_value_size(&Some(k.clone()))
                        + self.estimate_value_size(&Some(v.clone()))
                })
                .sum(),
            Some(Value::TinyInt(_)) => 1,
            Some(Value::SmallInt(_)) => 2,
            Some(Value::Float32(_)) => 4,
            Some(Value::Set(s)) => s
                .iter()
                .map(|v| self.estimate_value_size(&Some(v.clone())))
                .sum(),
            Some(Value::Tuple(t)) => t
                .iter()
                .map(|v| self.estimate_value_size(&Some(v.clone())))
                .sum(),
            Some(Value::Udt(udt)) => udt.fields
                .iter()
                .map(|f| self.estimate_value_size(&f.value))
                .sum(),
            Some(Value::Frozen(boxed_val)) => self.estimate_value_size(&Some((**boxed_val).clone())),
            Some(Value::Tombstone(_)) => 16, // timestamp + type + optional TTL
        }
    }
}

/// Statistics about a MemTable
#[derive(Debug, Clone)]
pub struct MemTableStats {
    /// Number of entries (including tombstones)
    pub entry_count: usize,
    /// Size in bytes
    pub size_bytes: u64,
    /// Number of tables
    pub table_count: usize,
    /// Number of tombstones
    pub tombstone_count: usize,
    /// Current sequence number
    pub sequence_number: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TableId;

    #[test]
    fn test_memtable_basic_operations() {
        let config = Config::default();
        let mut memtable = MemTable::new(&config).unwrap();

        let table_id = TableId::new("test_table");
        let key = RowKey::from("test_key");
        let value = Value::Text("test_value".to_string());

        // Test put
        memtable.put(&table_id, key.clone(), value.clone()).unwrap();
        assert_eq!(memtable.len(), 1);
        assert!(memtable.size() > 0);

        // Test get
        let retrieved = memtable.get(&table_id, &key).unwrap();
        assert_eq!(retrieved, Some(value));

        // Test delete
        memtable.delete(&table_id, key.clone()).unwrap();
        let retrieved = memtable.get(&table_id, &key).unwrap();
        assert_eq!(retrieved, None);

        // Entry should still exist as tombstone
        assert_eq!(memtable.len(), 1);
    }

    #[test]
    fn test_memtable_scan() {
        let config = Config::default();
        let mut memtable = MemTable::new(&config).unwrap();

        let table_id = TableId::new("test_table");

        // Insert test data
        for i in 0..10 {
            let key = RowKey::from(format!("key_{:02}", i));
            let value = Value::Integer(i as i32);
            memtable.put(&table_id, key, value).unwrap();
        }

        // Test scan all
        let results = memtable.scan(&table_id, None, None, None).unwrap();
        assert_eq!(results.len(), 10);

        // Test scan with limit
        let results = memtable.scan(&table_id, None, None, Some(5)).unwrap();
        assert_eq!(results.len(), 5);

        // Test scan with range
        let start_key = RowKey::from("key_03");
        let end_key = RowKey::from("key_07");
        let results = memtable
            .scan(&table_id, Some(&start_key), Some(&end_key), None)
            .unwrap();
        assert_eq!(results.len(), 4); // key_03, key_04, key_05, key_06
    }

    #[test]
    fn test_memtable_flush() {
        let config = Config::default();
        let mut memtable = MemTable::new(&config).unwrap();

        let table_id = TableId::new("test_table");
        let key1 = RowKey::from("key1");
        let key2 = RowKey::from("key2");
        let value1 = Value::Text("value1".to_string());
        let value2 = Value::Text("value2".to_string());

        // Insert data
        memtable
            .put(&table_id, key1.clone(), value1.clone())
            .unwrap();
        memtable
            .put(&table_id, key2.clone(), value2.clone())
            .unwrap();

        // Delete one key
        memtable.delete(&table_id, key1.clone()).unwrap();

        // Flush should only return non-tombstone entries
        let flushed = memtable.flush().unwrap();
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0], (table_id, key2, value2));
    }

    #[test]
    fn test_memtable_stats() {
        let config = Config::default();
        let mut memtable = MemTable::new(&config).unwrap();

        let table_id = TableId::new("test_table");
        let key = RowKey::from("test_key");
        let value = Value::Text("test_value".to_string());

        // Insert and delete
        memtable.put(&table_id, key.clone(), value).unwrap();
        memtable.delete(&table_id, key).unwrap();

        let stats = memtable.stats();
        assert_eq!(stats.entry_count, 1);
        assert_eq!(stats.table_count, 0); // No non-tombstone entries
        assert_eq!(stats.tombstone_count, 1);
        assert!(stats.size_bytes > 0);
        assert!(stats.sequence_number > 0);
    }
}
