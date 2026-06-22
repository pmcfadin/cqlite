//! In-memory write buffer (memtable)
//!
//! Stores mutations in memory using a BTreeMap for partition and clustering ordering.
//! Flushes to L0 SSTable when size threshold is reached.
//!
//! The memtable maintains mutations in token-sorted order (via DecoratedKey) and tracks
//! approximate memory usage to trigger flushes at a configurable threshold.

use crate::error::Result;
use crate::storage::write_engine::mutation::{DecoratedKey, Mutation};
use std::collections::BTreeMap;

/// In-memory write buffer
///
/// Stores mutations in memory with token-based ordering. Each partition can have
/// multiple mutations (e.g., multiple rows with different clustering keys).
#[derive(Debug)]
pub struct Memtable {
    /// Partition-level storage: token-ordered map of mutations
    data: BTreeMap<DecoratedKey, Vec<Mutation>>,
    /// Approximate size in bytes
    size_bytes: usize,
    /// Approximate row count (total mutations across all partitions)
    row_count: usize,
    /// Creation timestamp (Unix epoch microseconds)
    created_at: i64,
}

impl Memtable {
    /// Create a new memtable
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
            size_bytes: 0,
            row_count: 0,
            created_at: Self::current_timestamp_micros(),
        }
    }

    /// Insert a mutation into the memtable
    ///
    /// Mutations are grouped by partition key (DecoratedKey). Multiple mutations
    /// for the same partition are stored as a vector.
    pub fn insert(&mut self, _mutation: Mutation) -> Result<()> {
        // Calculate decorated key from partition key
        // Note: This requires schema, but mutation doesn't store it.
        // For now, we expect the mutation to be pre-validated and the key
        // to be extractable. In practice, the caller will need to provide
        // the decorated key or schema context.
        //
        // WORKAROUND: Since Mutation doesn't store DecoratedKey directly,
        // and calculating it requires schema, we need to pass the key separately.
        // For Issue #362, we'll implement a public API that accepts DecoratedKey.
        //
        // This is a design limitation that will be addressed in the full WriteEngine.
        // For now, insert_with_key() is the primary API.

        // This method is kept for API compatibility but requires rethinking.
        // We'll implement the core logic in insert_with_key() below.
        Err(crate::error::Error::InvalidInput(
            "Use insert_with_key() - decorated key must be provided with mutation".to_string(),
        ))
    }

    /// Insert a mutation with an explicit decorated key
    ///
    /// This is the primary insertion API. The caller is responsible for computing
    /// the decorated key from the partition key using the table schema.
    pub fn insert_with_key(&mut self, key: DecoratedKey, mutation: Mutation) -> Result<()> {
        // Calculate mutation size (conservative estimate)
        let mutation_size = Self::estimate_mutation_size(&mutation);

        // Get or create mutation list for this partition
        let mutations = self.data.entry(key).or_default();

        // Add mutation
        mutations.push(mutation);
        self.row_count += 1;
        self.size_bytes += mutation_size;

        Ok(())
    }

    /// Get all mutations for a given partition key
    pub fn get(&self, key: &DecoratedKey) -> Option<&[Mutation]> {
        self.data.get(key).map(|v| v.as_slice())
    }

    /// Check if memtable is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Get current size in bytes (approximate)
    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    /// Get approximate row count
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Check if memtable should be flushed
    pub fn should_flush(&self, threshold_bytes: usize) -> bool {
        self.size_bytes >= threshold_bytes
    }

    /// Get creation timestamp (microseconds since Unix epoch)
    pub fn created_at(&self) -> i64 {
        self.created_at
    }

    /// Iterate over all partitions and their mutations
    ///
    /// Returns an iterator over (DecoratedKey, mutations) pairs in token order.
    pub fn iter(&self) -> impl Iterator<Item = (&DecoratedKey, &[Mutation])> {
        self.data.iter().map(|(k, v)| (k, v.as_slice()))
    }

    /// Clear all data from the memtable
    ///
    /// Used after successful flush to SSTable.
    pub fn clear(&mut self) {
        self.data.clear();
        self.size_bytes = 0;
        self.row_count = 0;
        // Keep created_at unchanged - represents original creation time
    }

    /// Maximum nesting depth for collection size estimation (prevents stack overflow)
    const MAX_NESTING_DEPTH: usize = 32;

    /// Estimate the size of a mutation in bytes
    ///
    /// Conservative estimate includes:
    /// - Fixed overhead per mutation (48 bytes for struct fields)
    /// - Partition key size (key bytes)
    /// - Clustering key size (if present)
    /// - Cell operation sizes (column names + values)
    fn estimate_mutation_size(mutation: &Mutation) -> usize {
        let mut size = 48; // Base struct overhead

        // Partition key size
        for (col_name, value) in &mutation.partition_key.columns {
            size += col_name.len();
            size += Self::estimate_value_size_with_depth(value, 0);
        }

        // Clustering key size
        if let Some(ref clustering_key) = mutation.clustering_key {
            for (col_name, value) in &clustering_key.columns {
                size += col_name.len();
                size += Self::estimate_value_size_with_depth(value, 0);
            }
        }

        // Cell operations
        for op in &mutation.operations {
            size += Self::estimate_operation_size(op);
        }

        size
    }

    /// Estimate the size of a CQL value in bytes
    fn estimate_value_size(value: &crate::types::Value) -> usize {
        Self::estimate_value_size_with_depth(value, 0)
    }

    /// Estimate the size of a CQL value in bytes with depth tracking
    ///
    /// Limits recursion depth to prevent stack overflow from deeply nested collections.
    /// When max depth is reached, returns a conservative fixed estimate.
    fn estimate_value_size_with_depth(value: &crate::types::Value, depth: usize) -> usize {
        use crate::types::Value;

        // Prevent excessive recursion for deeply nested structures
        if depth >= Self::MAX_NESTING_DEPTH {
            // Return conservative estimate: assume 1KB for deeply nested value
            return 1024;
        }

        match value {
            Value::Null => 0,
            Value::Boolean(_) => 1,
            Value::TinyInt(_) => 1,
            Value::SmallInt(_) => 2,
            Value::Integer(_) => 4,
            Value::BigInt(_) | Value::Counter(_) | Value::Timestamp(_) | Value::Time(_) => 8,
            Value::Float32(_) => 4,
            Value::Float(_) => 8,
            Value::Text(s) => s.len(),
            Value::Blob(bytes) | Value::Varint(bytes) | Value::Inet(bytes) => bytes.len(),
            Value::Uuid(_) => 16,
            Value::Date(_) => 4,
            Value::Decimal { scale: _, unscaled } => 4 + unscaled.len(),
            Value::Duration { .. } => 16,
            Value::List(items) => {
                items
                    .iter()
                    .map(|item| Self::estimate_value_size_with_depth(item, depth + 1))
                    .sum::<usize>()
                    + 16
            }
            Value::Set(items) => {
                items
                    .iter()
                    .map(|item| Self::estimate_value_size_with_depth(item, depth + 1))
                    .sum::<usize>()
                    + 16
            }
            Value::Map(entries) => {
                entries
                    .iter()
                    .map(|(k, v)| {
                        Self::estimate_value_size_with_depth(k, depth + 1)
                            + Self::estimate_value_size_with_depth(v, depth + 1)
                    })
                    .sum::<usize>()
                    + 16
            }
            Value::Udt(udt) => {
                udt.fields
                    .iter()
                    .map(|field| {
                        field.name.len()
                            + field
                                .value
                                .as_ref()
                                .map_or(0, |v| Self::estimate_value_size_with_depth(v, depth + 1))
                    })
                    .sum::<usize>()
                    + 16
            }
            Value::Tuple(items) => {
                items
                    .iter()
                    .map(|item| Self::estimate_value_size_with_depth(item, depth + 1))
                    .sum::<usize>()
                    + 16
            }
            Value::Json(json) => json.to_string().len(),
            Value::Frozen(inner) => Self::estimate_value_size_with_depth(inner, depth + 1) + 8,
            Value::Tombstone(_) => 24, // timestamp + type + ttl overhead
        }
    }

    /// Estimate the size of a cell operation
    fn estimate_operation_size(
        op: &crate::storage::write_engine::mutation::CellOperation,
    ) -> usize {
        use crate::storage::write_engine::mutation::CellOperation;

        match op {
            CellOperation::Write { column, value } => {
                column.len() + Self::estimate_value_size(value) + 8 // +8 for overhead
            }
            CellOperation::WriteWithTtl {
                column,
                value,
                ttl_seconds: _,
            } => {
                // TTL cells: same as Write + 4 bytes for TTL + 4 bytes for local_deletion_time
                column.len() + Self::estimate_value_size(value) + 16
            }
            CellOperation::Delete { column, .. } => column.len() + 8,
            CellOperation::DeleteRow => 8,
            // Epic #899: per-element complex ops. Each carries a column name,
            // the preserved cell path, an optional value, and temporal metadata.
            CellOperation::WriteComplexElement {
                column,
                cell_path,
                value,
                ..
            } => {
                column.len()
                    + cell_path.len()
                    + value.as_ref().map(Self::estimate_value_size).unwrap_or(0)
                    + 16 // flags + ts/ldt/ttl deltas + length prefixes overhead
            }
            CellOperation::ComplexDeletion { column, .. } => column.len() + 16,
        }
    }

    /// Get current timestamp in microseconds since Unix epoch
    fn current_timestamp_micros() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as i64
    }
}

impl Default for Memtable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::write_engine::mutation::{
        CellOperation, ClusteringKey, PartitionKey, TableId,
    };
    use crate::types::Value;

    fn create_test_mutation(
        id: i32,
        name: &str,
        clustering_val: Option<i64>,
    ) -> (DecoratedKey, Mutation) {
        let table_id = TableId::new("test_ks", "test_table");
        let partition_key = PartitionKey::single("id", Value::Integer(id));

        // Calculate decorated key from partition key bytes
        let key_bytes = id.to_be_bytes().to_vec();
        let decorated_key = DecoratedKey::from_key_bytes(key_bytes).unwrap();

        let clustering_key =
            clustering_val.map(|val| ClusteringKey::single("ts", Value::BigInt(val)));

        let operations = vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text(name.to_string()),
        }];

        let mutation = Mutation::new(
            table_id,
            partition_key,
            clustering_key,
            operations,
            1234567890,
            None,
        );

        (decorated_key, mutation)
    }

    #[test]
    fn test_memtable_new() {
        let memtable = Memtable::new();
        assert!(memtable.is_empty());
        assert_eq!(memtable.size_bytes(), 0);
        assert_eq!(memtable.row_count(), 0);
        assert!(memtable.created_at() > 0);
    }

    #[test]
    fn test_memtable_insert_and_get() {
        let mut memtable = Memtable::new();

        let (key, mutation) = create_test_mutation(1, "Alice", None);
        memtable.insert_with_key(key.clone(), mutation).unwrap();

        assert!(!memtable.is_empty());
        assert_eq!(memtable.row_count(), 1);
        assert!(memtable.size_bytes() > 0);

        // Retrieve mutation
        let mutations = memtable.get(&key).unwrap();
        assert_eq!(mutations.len(), 1);
        assert_eq!(mutations[0].table.table, "test_table");
    }

    #[test]
    fn test_memtable_multiple_mutations_same_partition() {
        let mut memtable = Memtable::new();

        // Insert multiple mutations for same partition (different clustering keys)
        let (key, mutation1) = create_test_mutation(1, "Alice", Some(1000));
        let (_, mutation2) = create_test_mutation(1, "Alice Updated", Some(2000));

        memtable.insert_with_key(key.clone(), mutation1).unwrap();
        memtable.insert_with_key(key.clone(), mutation2).unwrap();

        assert_eq!(memtable.row_count(), 2);

        // Both mutations should be stored for this partition
        let mutations = memtable.get(&key).unwrap();
        assert_eq!(mutations.len(), 2);
    }

    #[test]
    fn test_memtable_multiple_partitions() {
        let mut memtable = Memtable::new();

        let (key1, mutation1) = create_test_mutation(1, "Alice", None);
        let (key2, mutation2) = create_test_mutation(2, "Bob", None);
        let (key3, mutation3) = create_test_mutation(3, "Charlie", None);

        memtable.insert_with_key(key1, mutation1).unwrap();
        memtable.insert_with_key(key2, mutation2).unwrap();
        memtable.insert_with_key(key3, mutation3).unwrap();

        assert_eq!(memtable.row_count(), 3);
        assert!(!memtable.is_empty());
    }

    #[test]
    fn test_memtable_token_ordering() {
        let mut memtable = Memtable::new();

        // Insert in non-sorted order
        let (key3, mutation3) = create_test_mutation(300, "Charlie", None);
        let (key1, mutation1) = create_test_mutation(100, "Alice", None);
        let (key2, mutation2) = create_test_mutation(200, "Bob", None);

        memtable.insert_with_key(key3.clone(), mutation3).unwrap();
        memtable.insert_with_key(key1.clone(), mutation1).unwrap();
        memtable.insert_with_key(key2.clone(), mutation2).unwrap();

        // Verify iteration returns partitions in token order
        let keys: Vec<_> = memtable.iter().map(|(k, _)| k.token).collect();
        assert_eq!(keys.len(), 3);

        // Keys should be sorted by token
        assert!(keys.windows(2).all(|w| w[0] <= w[1]));
    }

    #[test]
    fn test_memtable_size_tracking() {
        let mut memtable = Memtable::new();

        let initial_size = memtable.size_bytes();
        assert_eq!(initial_size, 0);

        // Insert mutation
        let (key, mutation) = create_test_mutation(1, "Alice", None);
        memtable.insert_with_key(key, mutation).unwrap();

        // Size should increase
        assert!(memtable.size_bytes() > initial_size);
        let size_after_insert = memtable.size_bytes();

        // Insert another mutation - size should increase more
        let (key2, mutation2) = create_test_mutation(2, "Bob with a longer name", None);
        memtable.insert_with_key(key2, mutation2).unwrap();

        assert!(memtable.size_bytes() > size_after_insert);
    }

    #[test]
    fn test_memtable_should_flush() {
        let mut memtable = Memtable::new();

        // Should not flush when empty
        assert!(!memtable.should_flush(1024));

        // Insert mutations until threshold
        for i in 0..100 {
            let (key, mutation) = create_test_mutation(i, "Test data", None);
            memtable.insert_with_key(key, mutation).unwrap();
        }

        // Should flush if size exceeds threshold
        let current_size = memtable.size_bytes();
        assert!(memtable.should_flush(current_size - 1));
        assert!(!memtable.should_flush(current_size + 1000));
    }

    #[test]
    fn test_memtable_clear() {
        let mut memtable = Memtable::new();

        let created_at = memtable.created_at();

        // Insert some data
        let (key, mutation) = create_test_mutation(1, "Alice", None);
        memtable.insert_with_key(key, mutation).unwrap();

        assert!(!memtable.is_empty());
        assert!(memtable.size_bytes() > 0);
        assert!(memtable.row_count() > 0);

        // Clear
        memtable.clear();

        assert!(memtable.is_empty());
        assert_eq!(memtable.size_bytes(), 0);
        assert_eq!(memtable.row_count(), 0);
        assert_eq!(memtable.created_at(), created_at); // Timestamp unchanged
    }

    #[test]
    fn test_memtable_iterator() {
        let mut memtable = Memtable::new();

        // Insert multiple partitions
        let (key1, mutation1) = create_test_mutation(1, "Alice", None);
        let (key2, mutation2) = create_test_mutation(2, "Bob", None);

        memtable.insert_with_key(key1.clone(), mutation1).unwrap();
        memtable.insert_with_key(key2.clone(), mutation2).unwrap();

        // Iterate and verify
        let mut count = 0;
        for (key, mutations) in memtable.iter() {
            assert!(!mutations.is_empty());
            assert!([key1.token, key2.token].contains(&key.token));
            count += 1;
        }

        assert_eq!(count, 2);
    }

    #[test]
    fn test_memtable_empty_check() {
        let mut memtable = Memtable::new();
        assert!(memtable.is_empty());

        let (key, mutation) = create_test_mutation(1, "Alice", None);
        memtable.insert_with_key(key, mutation).unwrap();
        assert!(!memtable.is_empty());

        memtable.clear();
        assert!(memtable.is_empty());
    }

    #[test]
    fn test_memtable_size_estimates() {
        // Test size estimation for different value types
        let small_text = Value::Text("hi".to_string());
        let large_text = Value::Text("a".repeat(1000));
        let integer = Value::Integer(42);
        let uuid = Value::Uuid([0u8; 16]);

        assert_eq!(Memtable::estimate_value_size(&small_text), 2);
        assert_eq!(Memtable::estimate_value_size(&large_text), 1000);
        assert_eq!(Memtable::estimate_value_size(&integer), 4);
        assert_eq!(Memtable::estimate_value_size(&uuid), 16);
    }

    #[test]
    fn test_memtable_collection_size_estimates() {
        // List
        let list = Value::List(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ]);
        let size = Memtable::estimate_value_size(&list);
        assert!(size >= 12); // 3 * 4 bytes + overhead

        // Set
        let set = Value::Set(vec![
            Value::Text("a".to_string()),
            Value::Text("b".to_string()),
        ]);
        let size = Memtable::estimate_value_size(&set);
        assert!(size >= 2); // 2 * 1 byte + overhead

        // Map
        let map = Value::Map(vec![
            (Value::Integer(1), Value::Text("one".to_string())),
            (Value::Integer(2), Value::Text("two".to_string())),
        ]);
        let size = Memtable::estimate_value_size(&map);
        assert!(size >= 11); // 2 * (4 + 3) bytes + overhead
    }

    #[test]
    fn test_memtable_realistic_flush_threshold() {
        let mut memtable = Memtable::new();

        // Target: ~10K mutations before 64MB flush (conservative estimate)
        // Average mutation size should be < 6.4KB
        let flush_threshold = 64 * 1024 * 1024; // 64MB

        // Insert 10K typical mutations
        for i in 0..10_000 {
            let (key, mutation) = create_test_mutation(
                i,
                "Typical user data with moderate length name",
                Some(i as i64),
            );
            memtable.insert_with_key(key, mutation).unwrap();
        }

        let final_size = memtable.size_bytes();
        println!(
            "10K mutations size: {} bytes ({} KB)",
            final_size,
            final_size / 1024
        );

        // Should be well under 64MB for 10K mutations
        assert!(final_size < flush_threshold);

        // Verify avg size per mutation is reasonable
        let avg_size = final_size / 10_000;
        println!("Average mutation size: {} bytes", avg_size);
        assert!(avg_size > 0);
        assert!(avg_size < 10_000); // Should be less than 10KB per mutation
    }

    #[test]
    fn test_memtable_get_nonexistent_key() {
        let memtable = Memtable::new();
        let key = DecoratedKey::new(12345, vec![0, 0, 0, 99]);

        assert!(memtable.get(&key).is_none());
    }

    #[test]
    fn test_memtable_insert_deprecated_api() {
        let mut memtable = Memtable::new();

        let table_id = TableId::new("test_ks", "test_table");
        let partition_key = PartitionKey::single("id", Value::Integer(1));
        let operations = vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text("Alice".to_string()),
        }];

        let mutation = Mutation::new(table_id, partition_key, None, operations, 1234567890, None);

        // Deprecated insert() should return error
        let result = memtable.insert(mutation);
        assert!(result.is_err());
    }

    #[test]
    fn test_memtable_nested_collection_depth_limit() {
        // Create a deeply nested list structure (40 levels deep)
        let mut nested_value = Value::Integer(42);
        for _ in 0..40 {
            nested_value = Value::List(vec![nested_value]);
        }

        // Should not panic and should return conservative estimate
        let size = Memtable::estimate_value_size(&nested_value);

        // At depth 32, should return 1024 for each remaining level
        // First 32 levels recurse normally, remaining 8 levels return 1024 each
        assert!(size > 0);
        assert!(
            size >= 1024,
            "Should use conservative estimate at max depth"
        );
    }

    #[test]
    fn test_memtable_nested_map_depth_limit() {
        // Create deeply nested map structure
        let mut nested_value = Value::Text("bottom".to_string());
        for _ in 0..35 {
            nested_value = Value::Map(vec![(Value::Integer(1), nested_value)]);
        }

        // Should not panic
        let size = Memtable::estimate_value_size(&nested_value);
        assert!(size > 0);
        assert!(
            size >= 1024,
            "Should use conservative estimate for deep nesting"
        );
    }

    #[test]
    fn test_memtable_nested_udt_depth_limit() {
        use crate::types::{UdtField, UdtValue};

        // Create deeply nested UDT structure
        let mut nested_value = Value::Integer(1);
        for i in 0..35 {
            let udt = UdtValue {
                type_name: format!("type_{}", i),
                keyspace: "test_ks".to_string(),
                fields: vec![UdtField {
                    name: "field".to_string(),
                    value: Some(nested_value),
                }],
            };
            nested_value = Value::Udt(udt);
        }

        // Should not panic
        let size = Memtable::estimate_value_size(&nested_value);
        assert!(size > 0);
        assert!(size >= 1024);
    }

    #[test]
    fn test_memtable_frozen_nested_depth_limit() {
        // Create deeply nested frozen values
        let mut nested_value = Value::Integer(99);
        for _ in 0..40 {
            nested_value = Value::Frozen(Box::new(nested_value));
        }

        // Should not panic or cause stack overflow
        let size = Memtable::estimate_value_size(&nested_value);
        assert!(size > 0);
        assert!(size >= 1024);
    }

    #[test]
    fn test_memtable_mixed_nested_collections() {
        use crate::types::{UdtField, UdtValue};

        // Create a complex nested structure mixing different types
        let mut nested_value = Value::Text("base".to_string());

        // Alternate between different collection types
        for i in 0..50 {
            nested_value = match i % 5 {
                0 => Value::List(vec![nested_value]),
                1 => Value::Set(vec![nested_value]),
                2 => Value::Map(vec![(Value::Integer(i), nested_value)]),
                3 => Value::Tuple(vec![nested_value]),
                4 => Value::Udt(UdtValue {
                    type_name: format!("type_{}", i),
                    keyspace: "test_ks".to_string(),
                    fields: vec![UdtField {
                        name: "f".to_string(),
                        value: Some(nested_value),
                    }],
                }),
                _ => unreachable!(),
            };
        }

        // Should handle mixed nesting without panic
        let size = Memtable::estimate_value_size(&nested_value);
        assert!(size > 0);
    }

    #[test]
    fn test_memtable_depth_limit_exact_boundary() {
        // Test at exactly the max depth (32 levels)
        let mut nested_value = Value::Integer(1);
        for _ in 0..32 {
            nested_value = Value::List(vec![nested_value]);
        }

        // Should handle max depth normally
        let size = Memtable::estimate_value_size(&nested_value);
        assert!(size > 0);

        // Add one more level - should hit depth limit on deepest value
        nested_value = Value::List(vec![nested_value]);
        let size_over = Memtable::estimate_value_size(&nested_value);

        // Size with depth limit should be >= 1024 due to conservative estimate
        assert!(size_over >= 1024);
    }

    #[test]
    fn test_memtable_shallow_collections_unaffected() {
        // Verify shallow collections are not affected by depth limit

        // Simple list
        let simple_list = Value::List(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ]);
        let size = Memtable::estimate_value_size(&simple_list);
        assert_eq!(size, 12 + 16); // 3 * 4 bytes + overhead

        // Nested but shallow (3 levels)
        let shallow_nested =
            Value::List(vec![Value::List(vec![Value::List(vec![Value::Integer(
                1,
            )])])]);
        let size = Memtable::estimate_value_size(&shallow_nested);
        assert!(size > 0);
        assert!(size < 1024); // Should not use conservative estimate
    }
}
