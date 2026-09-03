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
    #[tracing::instrument(name = "memtable.insert", level = "debug", skip(self, key, mutation))]
    pub fn insert_with_key(&mut self, key: DecoratedKey, mutation: Mutation) -> Result<()> {
        // Calculate mutation size (conservative estimate). This is the SAME
        // computation the admission gate consults via `estimate_mutation_size`,
        // so accounting and admission can never drift (issue #1625).
        let mutation_size = Self::mutation_size(&mutation);

        // Get or create mutation list for this partition
        let mutations = self.data.entry(key).or_default();

        // Add mutation
        mutations.push(mutation);
        self.row_count = self.row_count.saturating_add(1);
        // `saturating_add`: `mutation_size` can legitimately be `usize::MAX`
        // (the estimator fails closed at the node cap, issue #1625). Admission
        // rejects over-limit mutations, but a direct `Memtable` user (bypassing
        // admission) or a `WriteEngine` with `memtable_hard_limit == usize::MAX`
        // could otherwise panic in debug (overflow check) or wrap in release —
        // the ledger update MUST be self-safe.
        self.size_bytes = self.size_bytes.saturating_add(mutation_size);

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

    /// Upper bound on the number of value nodes the ITERATIVE size estimator
    /// visits before it fails closed (issue #1625).
    ///
    /// The estimator walks a value with an explicit heap worklist (no recursion,
    /// so no stack-overflow risk and no depth cap collapsing deep children to a
    /// floor). To bound worst-case work on a pathologically huge/deep value, the
    /// traversal stops after visiting this many nodes and returns a
    /// CONSERVATIVE-LARGE estimate (`usize::MAX`) that is GUARANTEED to exceed any
    /// hard limit, so admission REJECTS the value rather than under-counting it.
    /// Failing closed on the pathological case is the correct behavior for a DoS
    /// guard. `1_000_000` is far beyond any legitimate mutation's node count yet
    /// keeps the traversal cheap.
    const MAX_ESTIMATE_NODES: usize = 1_000_000;

    /// Estimate the number of bytes a mutation would add to this memtable.
    ///
    /// Returns the SAME value that [`Memtable::insert_with_key`] adds to
    /// `size_bytes`, so the write-engine admission gate (issue #1625) and the
    /// running size accounting agree by construction — both funnel through the
    /// private [`Memtable::mutation_size`] computation, so there is no drift.
    pub(crate) fn estimate_mutation_size(&self, m: &Mutation) -> usize {
        Self::mutation_size(m)
    }

    /// Estimate the size of a mutation in bytes
    ///
    /// Conservative estimate includes:
    /// - Fixed overhead per mutation (48 bytes for struct fields)
    /// - Partition key size (key bytes)
    /// - Clustering key size (if present)
    /// - Cell operation sizes (column names + values)
    fn mutation_size(mutation: &Mutation) -> usize {
        // Base struct overhead. All accumulation uses `saturating_add` because
        // `estimate_value_size` may return `usize::MAX` (fail-closed) for a
        // pathological value (issue #1625); a plain `+` would panic under debug
        // overflow checks.
        let mut size: usize = 48;

        // Partition key size
        for (col_name, value) in &mutation.partition_key.columns {
            size = size.saturating_add(col_name.len());
            size = size.saturating_add(Self::estimate_value_size(value));
        }

        // Clustering key size
        if let Some(ref clustering_key) = mutation.clustering_key {
            for (col_name, value) in &clustering_key.columns {
                size = size.saturating_add(col_name.len());
                size = size.saturating_add(Self::estimate_value_size(value));
            }
        }

        // Cell operations
        for op in &mutation.operations {
            size = size.saturating_add(Self::estimate_operation_size(op));
        }

        size
    }

    /// Estimate the size in bytes a CQL value would add to the memtable.
    ///
    /// Implemented as a BOUNDED ITERATIVE traversal (issue #1625): an explicit
    /// worklist of `&Value` replaces the previous recursive estimator. Because
    /// traversal state is not on the call stack there is NO stack-overflow risk,
    /// so there is NO depth cap and therefore NO collapsing of deeply nested
    /// children to a conservative floor — a large scalar buried arbitrarily deep
    /// (e.g. `List([List([List([Text(128KB)])])])`) is counted at its real heap
    /// size, closing the hard-limit bypass.
    ///
    /// The worklist is a stack-backed [`SmallVec`] with 32 inline slots, so the
    /// common case (shallow/normal mutations — what the #1660 write-path
    /// allocation budget test exercises) performs the whole traversal with ZERO
    /// heap allocation on the admission/accounting hot path. Only pathologically
    /// deep/wide values spill the worklist to the heap.
    ///
    /// Scalars contribute their real heap size (`Text`/`Blob`/`Varint`/`Inet`/
    /// `Json`/`Decimal` byte length; fixed widths otherwise). Collections, maps,
    /// UDTs, tuples and frozen values contribute their per-container overhead and
    /// push their children (maps push keys AND values; UDTs count field names and
    /// push field values) onto the worklist.
    ///
    /// To bound worst-case work on a pathologically huge/deep value, traversal
    /// stops after visiting [`MAX_ESTIMATE_NODES`](Self::MAX_ESTIMATE_NODES) and
    /// FAILS CLOSED, returning `usize::MAX` so admission REJECTS the value rather
    /// than under-counting it. All accumulation uses `saturating_add`, so the
    /// estimate can never wrap around a small value.
    fn estimate_value_size(value: &crate::types::Value) -> usize {
        use crate::types::Value;
        use smallvec::SmallVec;

        let mut total: usize = 0;
        let mut visited: usize = 0;
        // Stack-backed worklist of borrowed values still to be measured. The 32
        // inline slots cover normal nesting/width, so shallow/normal mutations
        // (the #1660 write-path allocation budget case) traverse with ZERO heap
        // allocation; only pathological deep/wide values spill to the heap.
        let mut worklist: SmallVec<[&Value; 32]> = SmallVec::new();
        worklist.push(value);

        // Would scheduling `incoming` more children push the total node count
        // past the cap? `visited` (popped so far) + `pending` (already queued) +
        // `incoming` is the upper bound on nodes this traversal will touch. This
        // is checked BEFORE enqueuing so a single flat collection with far more
        // than `MAX_ESTIMATE_NODES` elements can never grow the worklist
        // proportional to its element count — the DoS guard fails closed WITHOUT
        // the huge allocation (issue #1625).
        let would_exceed_cap = |visited: usize, pending: usize, incoming: usize| -> bool {
            visited.saturating_add(pending).saturating_add(incoming) > Self::MAX_ESTIMATE_NODES
        };

        while let Some(v) = worklist.pop() {
            visited += 1;
            if visited > Self::MAX_ESTIMATE_NODES {
                // Pathological value: fail closed so admission rejects it.
                return usize::MAX;
            }

            match v {
                Value::Null => {}
                // Zero payload bytes: the sentinel's serialized form is the
                // EMPTY buffer, and its 1-byte type tag lives inline in the
                // `Value` slot, not on the heap (issue #3805).
                Value::Empty(_) => {}
                Value::Boolean(_) | Value::TinyInt(_) => total = total.saturating_add(1),
                Value::SmallInt(_) => total = total.saturating_add(2),
                Value::Integer(_) | Value::Float32(_) | Value::Date(_) => {
                    total = total.saturating_add(4)
                }
                Value::BigInt(_)
                | Value::Counter(_)
                | Value::Timestamp(_)
                | Value::Time(_)
                | Value::Float(_) => total = total.saturating_add(8),
                Value::Uuid(_) | Value::Duration { .. } => total = total.saturating_add(16),
                Value::Text(s) => total = total.saturating_add(s.len()),
                Value::Blob(bytes) | Value::Varint(bytes) | Value::Inet(bytes) => {
                    total = total.saturating_add(bytes.len())
                }
                Value::Decimal { scale: _, unscaled } => {
                    total = total.saturating_add(4).saturating_add(unscaled.len())
                }
                Value::Json(json) => total = total.saturating_add(json.to_string().len()),
                Value::Tombstone(_) => total = total.saturating_add(24),
                Value::List(items) | Value::Set(items) | Value::Tuple(items) => {
                    total = total.saturating_add(16);
                    if would_exceed_cap(visited, worklist.len(), items.len()) {
                        return usize::MAX;
                    }
                    worklist.extend(items.iter());
                }
                Value::Map(entries) => {
                    total = total.saturating_add(16);
                    // Each entry enqueues both a key and a value.
                    let incoming = entries.len().saturating_mul(2);
                    if would_exceed_cap(visited, worklist.len(), incoming) {
                        return usize::MAX;
                    }
                    for (k, val) in entries {
                        worklist.push(k);
                        worklist.push(val);
                    }
                }
                Value::Udt(udt) => {
                    total = total.saturating_add(16);
                    // Upper bound: at most one child per field (fields with a
                    // value); check before touching any field so a wide UDT
                    // cannot balloon the worklist.
                    if would_exceed_cap(visited, worklist.len(), udt.fields.len()) {
                        return usize::MAX;
                    }
                    for field in &udt.fields {
                        total = total.saturating_add(field.name.len());
                        if let Some(fv) = field.value.as_ref() {
                            worklist.push(fv);
                        }
                    }
                }
                Value::Frozen(inner) => {
                    total = total.saturating_add(8);
                    if would_exceed_cap(visited, worklist.len(), 1) {
                        return usize::MAX;
                    }
                    worklist.push(inner);
                }
            }
        }

        total
    }

    /// Estimate the size of a cell operation
    fn estimate_operation_size(
        op: &crate::storage::write_engine::mutation::CellOperation,
    ) -> usize {
        use crate::storage::write_engine::mutation::CellOperation;

        // `saturating_add` throughout: `estimate_value_size` may return
        // `usize::MAX` (fail-closed) for a pathological value (issue #1625).
        match op {
            CellOperation::Write { column, value } => column
                .len()
                .saturating_add(Self::estimate_value_size(value))
                .saturating_add(8), // +8 for overhead
            CellOperation::WriteWithTtl { column, value, .. } => {
                // TTL cells: same as Write + 4 bytes for TTL + 4 bytes for local_deletion_time
                column
                    .len()
                    .saturating_add(Self::estimate_value_size(value))
                    .saturating_add(16)
            }
            CellOperation::Delete { column, .. } => column.len().saturating_add(8),
            CellOperation::DeleteRow => 8,
            // Epic #899: per-element complex ops. Each carries a column name,
            // the preserved cell path, an optional value, and temporal metadata.
            CellOperation::WriteComplexElement {
                column,
                cell_path,
                value,
                ..
            } => column
                .len()
                .saturating_add(cell_path.len())
                .saturating_add(value.as_ref().map(Self::estimate_value_size).unwrap_or(0))
                .saturating_add(16), // flags + ts/ldt/ttl deltas + length prefixes overhead
            CellOperation::ComplexDeletion { column, .. } => column.len().saturating_add(16),
        }
    }

    /// Test-only: force the tracked approximate size, used to exercise the
    /// admission gate's `saturating_add` overflow guard near `usize::MAX`
    /// (issue #1625) — a size unreachable through real inserts.
    #[cfg(test)]
    pub(crate) fn set_size_bytes_for_test(&mut self, size: usize) {
        self.size_bytes = size;
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
            value: Value::text(name.to_string()),
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
        let small_text = Value::text("hi".to_string());
        let large_text = Value::text("a".repeat(1000));
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
            Value::text("a".to_string()),
            Value::text("b".to_string()),
        ]);
        let size = Memtable::estimate_value_size(&set);
        assert!(size >= 2); // 2 * 1 byte + overhead

        // Map
        let map = Value::Map(vec![
            (Value::Integer(1), Value::text("one".to_string())),
            (Value::Integer(2), Value::text("two".to_string())),
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
            value: Value::text("Alice".to_string()),
        }];

        let mutation = Mutation::new(table_id, partition_key, None, operations, 1234567890, None);

        // Deprecated insert() should return error
        let result = memtable.insert(mutation);
        assert!(result.is_err());
    }

    #[test]
    fn test_memtable_nested_collection_depth_limit() {
        // Issue #1625: a deeply nested list wrapping a tiny scalar is handled by
        // the ITERATIVE estimator without stack overflow and counted ACCURATELY
        // (no depth cap, no floor). 40 lists (16 each) around Integer(42) (4).
        let mut nested_value = Value::Integer(42);
        for _ in 0..40 {
            nested_value = Value::List(vec![nested_value]);
        }

        let size = Memtable::estimate_value_size(&nested_value);
        assert_eq!(
            size,
            40 * 16 + 4,
            "deep list of a tiny scalar must be counted accurately, not floored"
        );
    }

    #[test]
    fn test_memtable_nested_map_depth_limit() {
        // Issue #1625: deep map nesting counted accurately. 35 maps, each +16
        // overhead + Integer key (4), innermost Text("bottom") (6). No floor.
        let mut nested_value = Value::text("bottom".to_string());
        for _ in 0..35 {
            nested_value = Value::Map(vec![(Value::Integer(1), nested_value)]);
        }

        let size = Memtable::estimate_value_size(&nested_value);
        assert_eq!(
            size,
            35 * (16 + 4) + 6,
            "deep map of a tiny scalar must be counted accurately, not floored"
        );
    }

    #[test]
    fn test_memtable_nested_udt_depth_limit() {
        use crate::types::{UdtField, UdtValue};

        // Issue #1625: deep UDT nesting counted accurately. 35 UDTs, each +16
        // overhead + field name "field" (5), innermost Integer(1) (4). No floor.
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
            nested_value = Value::Udt(Box::new(udt));
        }

        let size = Memtable::estimate_value_size(&nested_value);
        assert_eq!(
            size,
            35 * (16 + 5) + 4,
            "deep UDT of a tiny scalar must be counted accurately, not floored"
        );
    }

    #[test]
    fn test_memtable_frozen_nested_depth_limit() {
        // Issue #1625: deep Frozen nesting must NOT stack-overflow (iterative)
        // and is counted accurately. 40 frozen (8 each) around Integer(99) (4).
        let mut nested_value = Value::Integer(99);
        for _ in 0..40 {
            nested_value = Value::Frozen(Box::new(nested_value));
        }

        let size = Memtable::estimate_value_size(&nested_value);
        assert_eq!(
            size,
            40 * 8 + 4,
            "deep frozen of a tiny scalar must be counted accurately, not floored"
        );
    }

    #[test]
    fn test_memtable_mixed_nested_collections() {
        use crate::types::{UdtField, UdtValue};

        // Create a complex nested structure mixing different types
        let mut nested_value = Value::text("base".to_string());

        // Alternate between different collection types
        for i in 0..50 {
            nested_value = match i % 5 {
                0 => Value::List(vec![nested_value]),
                1 => Value::Set(vec![nested_value]),
                2 => Value::Map(vec![(Value::Integer(i), nested_value)]),
                3 => Value::Tuple(vec![nested_value]),
                4 => Value::Udt(Box::new(UdtValue {
                    type_name: format!("type_{}", i),
                    keyspace: "test_ks".to_string(),
                    fields: vec![UdtField {
                        name: "f".to_string(),
                        value: Some(nested_value),
                    }],
                })),
                _ => unreachable!(),
            };
        }

        // Should handle mixed nesting without panic
        let size = Memtable::estimate_value_size(&nested_value);
        assert!(size > 0);
    }

    #[test]
    fn test_memtable_depth_limit_exact_boundary() {
        // Issue #1625: with the iterative estimator there is no depth cap, so
        // adding a wrapper level increases the estimate by exactly one List's
        // overhead (16) — no discontinuity/jump to a floor at any depth.
        let mut nested_value = Value::Integer(1);
        for _ in 0..32 {
            nested_value = Value::List(vec![nested_value]);
        }

        let size = Memtable::estimate_value_size(&nested_value);
        assert_eq!(size, 32 * 16 + 4);

        // One more wrapper: exactly +16, not a jump to any conservative floor.
        nested_value = Value::List(vec![nested_value]);
        let size_over = Memtable::estimate_value_size(&nested_value);
        assert_eq!(size_over, size + 16);
    }

    #[test]
    fn test_estimate_mutation_size_matches_insert_accounting() {
        // Issue #1625: the admission gate and the running size accounting must
        // agree by construction — `estimate_mutation_size(&m)` must equal the
        // delta `size_bytes()` gains from inserting the same mutation.
        let mut memtable = Memtable::new();
        let (key, mutation) = create_test_mutation(7, "some data here", Some(42));

        let before = memtable.size_bytes();
        let predicted = memtable.estimate_mutation_size(&mutation);
        memtable.insert_with_key(key, mutation).unwrap();
        let actual_delta = memtable.size_bytes() - before;

        assert_eq!(
            predicted, actual_delta,
            "estimate_mutation_size must equal the size delta insert applies"
        );
        assert!(predicted > 0);
    }

    #[test]
    fn test_deep_wide_collection_counted_accurately() {
        // Issue #1625: a WIDE collection buried under many wrapper lists must be
        // counted at its REAL byte size regardless of depth. The iterative
        // estimator has no depth cap, so the 500 × 200-byte strings (~100KB) at
        // the bottom are summed exactly — not collapsed to any floor.
        let wide = Value::List((0..500).map(|_| Value::text("y".repeat(200))).collect());
        let mut nested = wide;
        for _ in 0..32 {
            nested = Value::List(vec![nested]);
        }

        let size = Memtable::estimate_value_size(&nested);
        let real_payload = 500 * 200; // 100_000 bytes of text
        assert!(
            size >= real_payload,
            "wide collection must count real bytes at any depth (got {size})"
        );
        // Accurate, not wildly inflated: real payload + bounded per-container
        // overhead only.
        assert!(
            size < real_payload + 10 * 1024,
            "estimate must be accurate, not floor-inflated (got {size})"
        );
    }

    #[test]
    fn test_deep_narrow_collection_large_scalar_not_undercounted() {
        // Issue #1625 (roborev finding): a deep NARROW collection wrapping a
        // large DIRECT scalar must not be systematically under-counted. Wrap a
        // `List([Text(128KB)])` in 32 single-element lists.
        //
        // Pre-fix: any node past the depth cap collapsed to a ~1KB floor — small
        // enough to slip a 64KB gate. Post-fix: the iterative estimator counts
        // the 128KB scalar at its real heap size regardless of depth.
        let big = 128 * 1024;
        let mut nested = Value::List(vec![Value::text("x".repeat(big))]);
        for _ in 0..32 {
            nested = Value::List(vec![nested]);
        }

        let size = Memtable::estimate_value_size(&nested);
        assert!(
            size >= big,
            "deep narrow collection with a large scalar must count the scalar's \
             real heap size, not the old ~1KB floor (got {size}, expected >= {big})"
        );
    }

    #[test]
    fn test_large_scalar_buried_below_old_cap_counted() {
        // Issue #1625 (3rd iteration): a large scalar buried MANY levels below
        // where the old depth cap (32) sat must be counted at real size. Wrap a
        // 128KB text in 40 single-element lists — well past the old cap — and
        // confirm the iterative estimator still sees the full 128KB.
        let big = 128 * 1024;
        let mut nested = Value::text("z".repeat(big));
        for _ in 0..40 {
            nested = Value::List(vec![nested]);
        }

        let size = Memtable::estimate_value_size(&nested);
        assert!(
            size >= big,
            "scalar buried below the old depth cap must be counted at real size \
             (got {size}, expected >= {big})"
        );
    }

    #[test]
    fn test_pathological_node_cap_returns_usize_max() {
        // Issue #1625: exceeding MAX_ESTIMATE_NODES must FAIL CLOSED (usize::MAX),
        // never under-count — and never overflow or hang. A flat list of
        // 1_000_001 integers is a single fast allocation that trips the cap.
        let value = Value::List((0..1_000_001i32).map(Value::Integer).collect());
        let size = Memtable::estimate_value_size(&value);
        assert_eq!(
            size,
            usize::MAX,
            "hitting the node cap must fail closed with usize::MAX (got {size})"
        );
    }

    #[test]
    fn test_insert_with_pathological_value_saturates_ledger_no_panic() {
        // Issue #1625 (roborev finding 1): a direct `Memtable` user bypasses the
        // WriteEngine admission gate, so `insert_with_key` MUST be self-safe when
        // the estimator returns `usize::MAX` (node cap fail-closed). The ledger
        // update must saturate — never panic (debug overflow) or wrap (release).
        let mut memtable = Memtable::new();

        // A flat list past the node cap makes `mutation_size` == usize::MAX.
        let pathological = Value::List(
            (0..(Memtable::MAX_ESTIMATE_NODES as i32 + 5))
                .map(Value::Integer)
                .collect(),
        );
        let table_id = TableId::new("test_ks", "test_table");
        let partition_key = PartitionKey::single("id", Value::Integer(1));
        let key = DecoratedKey::from_key_bytes(1i32.to_be_bytes().to_vec()).unwrap();
        let operations = vec![CellOperation::Write {
            column: "big".to_string(),
            value: pathological,
        }];
        let mutation = Mutation::new(table_id, partition_key, None, operations, 1, None);

        // Sanity: the estimate really is usize::MAX for this mutation.
        assert_eq!(memtable.estimate_mutation_size(&mutation), usize::MAX);

        // Must not panic; ledger saturates at usize::MAX (no wrap to a small value).
        memtable.insert_with_key(key, mutation).unwrap();
        assert_eq!(memtable.size_bytes(), usize::MAX);
    }

    #[test]
    fn test_insert_saturates_when_ledger_already_near_max() {
        // Issue #1625 (roborev finding 1): incrementing an already-huge ledger by
        // a normal mutation size must saturate, not wrap around to a small value.
        let mut memtable = Memtable::new();
        memtable.set_size_bytes_for_test(usize::MAX - 3);

        let (key, mutation) = create_test_mutation(1, "Alice", None);
        memtable.insert_with_key(key, mutation).unwrap();

        assert_eq!(
            memtable.size_bytes(),
            usize::MAX,
            "ledger must saturate at usize::MAX, never wrap"
        );
    }

    #[test]
    fn test_wide_collection_fails_closed_before_enqueuing_children() {
        // Issue #1625 (roborev finding 2): a single flat collection whose element
        // count exceeds MAX_ESTIMATE_NODES must fail closed (usize::MAX) at the
        // ENQUEUE check — the worklist is never grown proportional to the element
        // count. Verified for every enqueue site (list, map, UDT, frozen).
        use crate::types::{UdtField, UdtValue};

        let over = Memtable::MAX_ESTIMATE_NODES + 5;

        // List / Set / Tuple enqueue site.
        let list = Value::List((0..over as i32).map(Value::Integer).collect());
        assert_eq!(Memtable::estimate_value_size(&list), usize::MAX);

        // Map enqueue site (keys + values).
        let map = Value::Map(
            (0..over as i32)
                .map(|i| (Value::Integer(i), Value::Integer(i)))
                .collect(),
        );
        assert_eq!(Memtable::estimate_value_size(&map), usize::MAX);

        // UDT enqueue site (field values).
        let udt = Value::Udt(Box::new(UdtValue {
            type_name: "t".to_string(),
            keyspace: "ks".to_string(),
            fields: (0..over)
                .map(|i| UdtField {
                    name: String::new(),
                    value: Some(Value::Integer(i as i32)),
                })
                .collect(),
        }));
        assert_eq!(Memtable::estimate_value_size(&udt), usize::MAX);
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
