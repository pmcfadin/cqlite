# Iterator Patterns for CQLite SSTable Reading

## 🔄 Iterator-Based vs Alternative Approaches

### **Iterator-Based Approach**
```rust
// Iterator pattern - lazy evaluation, memory efficient
let rows = sstable.rows()
    .filter(|row| row.partition_key == target_key)
    .map(|row| decode_row(row, &schema))
    .take(100);

for row in rows {
    println!("{:?}", row);
}
```

**Pros:**
- **Memory efficient**: Only loads data as needed
- **Composable**: Can chain filters, maps, takes easily
- **Rust idiomatic**: Follows Rust ecosystem patterns
- **Lazy evaluation**: Doesn't process until consumed
- **Short-circuits**: Can stop early on first match

**Cons:**
- **Sequential access**: Not ideal for random key lookups
- **No seeking**: Hard to jump to specific positions
- **State management**: Complex for stateful operations

### **Direct Access Approach**
```rust
// Direct access - immediate, explicit control
let row = sstable.get_partition(&partition_key)?;
let range = sstable.get_range(&start_key, &end_key)?;
let all_rows = sstable.scan_all()?;
```

**Pros:**
- **Explicit control**: Clear what operation you're doing
- **Random access**: Can seek directly to keys
- **Performance predictable**: Know exactly what's loaded
- **Index-friendly**: Works well with Index.db lookups

**Cons:**
- **Memory usage**: May load more than needed
- **Less composable**: Harder to chain operations
- **More verbose**: Requires explicit iteration

### **Hybrid Approach** (Recommended)
```rust
// Best of both worlds
impl SSTableReader {
    // Direct access for key lookups
    fn get_partition(&self, key: &PartitionKey) -> Result<Vec<Row>>;
    
    // Iterator for scanning
    fn scan_partition(&self, key: &PartitionKey) -> impl Iterator<Item = Row>;
    fn scan_range(&self, start: &Key, end: &Key) -> impl Iterator<Item = Row>;
    
    // Memory-mapped access for bulk operations
    fn memory_map(&self) -> MemoryMappedView;
}
```

## 🎯 Recommendation for CQLite

**Use Hybrid Approach with Iterator support:**

1. **Primary Operations** - Direct access methods
2. **Scanning Operations** - Iterator-based
3. **Bulk Operations** - Memory-mapped access
4. **Filtering/Processing** - Iterator chains

This gives us:
- Efficient key lookups (direct access)
- Memory-efficient scanning (iterators)  
- High-performance bulk access (memory mapping)
- Composable data processing (iterator chains)