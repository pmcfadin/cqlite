# Issue #447: K-way Merge & Compaction Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace three interconnected stubs to make the M5.2 compaction pipeline functional — SSTable reader integration, merge policy activation, and merge entry conversion.

**Architecture:** Wrap the existing async `SSTableReader` in a sync `SSTableRowIteratorAdapter` using tokio `block_on` (matching existing patterns in `flush_internal` and `finalize_merge_blocking`). Deserialize `DecoratedKey` bytes back to `PartitionKey` using schema comparators. Remove the guard on `set_merge_policy` to enable STCS activation.

**Tech Stack:** Rust, tokio (async runtime bridging), cqlite-core with `write-support` feature flag

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `cqlite-core/src/storage/write_engine/merge.rs` | Modify | Add `SSTableRowIteratorAdapter`, fix `KWayMerger::new()`, fix `merge_entry_to_mutation()` |
| `cqlite-core/src/storage/write_engine/mod.rs` | Modify | Fix `set_merge_policy()`, fix `merge_entry_to_mutation()` |
| `cqlite-core/src/storage/write_engine/mutation.rs` | Modify | Add `PartitionKey::from_bytes()` for key deserialization |

---

### Task 1: Add `PartitionKey::from_bytes()` deserialization

**Files:**
- Modify: `cqlite-core/src/storage/write_engine/mutation.rs`

This is the inverse of `PartitionKey::to_bytes()`. Single-component keys are raw value bytes; multi-component keys use `[len:u16 BE][value bytes][0x00]` per component.

- [ ] **Step 1: Add `deserialize_value_bytes` helper function**

Add after `serialize_value_bytes` (around line 600). This is the inverse — takes raw bytes + comparator, returns a `Value`:

```rust
/// Deserialize raw bytes to a Value according to its CQL comparator type
///
/// This is the inverse of `serialize_value_bytes`.
fn deserialize_value_bytes(data: &[u8], comparator: &ComparatorType) -> Result<Value> {
    match comparator {
        ComparatorType::Boolean => {
            if data.is_empty() {
                return Err(Error::InvalidInput("Empty boolean value".to_string()));
            }
            Ok(Value::Boolean(data[0] != 0))
        }
        ComparatorType::TinyInt => {
            if data.is_empty() {
                return Err(Error::InvalidInput("Empty tinyint value".to_string()));
            }
            Ok(Value::TinyInt(data[0] as i8))
        }
        ComparatorType::SmallInt => {
            let bytes: [u8; 2] = data.try_into().map_err(|_| {
                Error::InvalidInput(format!("SmallInt requires 2 bytes, got {}", data.len()))
            })?;
            Ok(Value::SmallInt(i16::from_be_bytes(bytes)))
        }
        ComparatorType::Int => {
            let bytes: [u8; 4] = data.try_into().map_err(|_| {
                Error::InvalidInput(format!("Int requires 4 bytes, got {}", data.len()))
            })?;
            Ok(Value::Integer(i32::from_be_bytes(bytes)))
        }
        ComparatorType::BigInt => {
            let bytes: [u8; 8] = data.try_into().map_err(|_| {
                Error::InvalidInput(format!("BigInt requires 8 bytes, got {}", data.len()))
            })?;
            Ok(Value::BigInt(i64::from_be_bytes(bytes)))
        }
        ComparatorType::Counter => {
            let bytes: [u8; 8] = data.try_into().map_err(|_| {
                Error::InvalidInput(format!("Counter requires 8 bytes, got {}", data.len()))
            })?;
            Ok(Value::Counter(i64::from_be_bytes(bytes)))
        }
        ComparatorType::Float32 => {
            let bytes: [u8; 4] = data.try_into().map_err(|_| {
                Error::InvalidInput(format!("Float32 requires 4 bytes, got {}", data.len()))
            })?;
            Ok(Value::Float32(f32::from_bits(u32::from_be_bytes(bytes))))
        }
        ComparatorType::Float => {
            let bytes: [u8; 8] = data.try_into().map_err(|_| {
                Error::InvalidInput(format!("Float requires 8 bytes, got {}", data.len()))
            })?;
            Ok(Value::Float(f64::from_bits(u64::from_be_bytes(bytes))))
        }
        ComparatorType::Text => {
            let s = String::from_utf8(data.to_vec()).map_err(|e| {
                Error::InvalidInput(format!("Invalid UTF-8 in text value: {}", e))
            })?;
            Ok(Value::Text(s))
        }
        ComparatorType::Blob => Ok(Value::Blob(data.to_vec())),
        ComparatorType::Timestamp => {
            let bytes: [u8; 8] = data.try_into().map_err(|_| {
                Error::InvalidInput(format!("Timestamp requires 8 bytes, got {}", data.len()))
            })?;
            Ok(Value::Timestamp(i64::from_be_bytes(bytes)))
        }
        ComparatorType::Date => {
            let bytes: [u8; 4] = data.try_into().map_err(|_| {
                Error::InvalidInput(format!("Date requires 4 bytes, got {}", data.len()))
            })?;
            let stored = u32::from_be_bytes(bytes);
            Ok(Value::Date((stored as i32).wrapping_add(i32::MIN)))
        }
        ComparatorType::Uuid => {
            let bytes: [u8; 16] = data.try_into().map_err(|_| {
                Error::InvalidInput(format!("UUID requires 16 bytes, got {}", data.len()))
            })?;
            Ok(Value::Uuid(bytes))
        }
        ComparatorType::Custom(name) if name == "time" => {
            let bytes: [u8; 8] = data.try_into().map_err(|_| {
                Error::InvalidInput(format!("Time requires 8 bytes, got {}", data.len()))
            })?;
            Ok(Value::Time(i64::from_be_bytes(bytes)))
        }
        ComparatorType::Custom(name) if name == "inet" => Ok(Value::Inet(data.to_vec())),
        ComparatorType::Varint => Ok(Value::Varint(data.to_vec())),
        ComparatorType::Decimal => {
            if data.len() < 4 {
                return Err(Error::InvalidInput(format!(
                    "Decimal requires at least 4 bytes, got {}",
                    data.len()
                )));
            }
            let scale = i32::from_be_bytes(data[..4].try_into().unwrap());
            let unscaled = data[4..].to_vec();
            Ok(Value::Decimal { scale, unscaled })
        }
        _ => Err(Error::InvalidInput(format!(
            "Unsupported comparator for deserialization: {:?}",
            comparator
        ))),
    }
}
```

- [ ] **Step 2: Add `PartitionKey::from_bytes()` method**

Add to the `impl PartitionKey` block, after `to_decorated_key()`:

```rust
/// Deserialize partition key from raw bytes (inverse of `to_bytes`)
///
/// Single-component keys are raw value bytes.
/// Multi-component keys use `[len:u16 BE][value bytes][0x00]` per component.
pub fn from_bytes(data: &[u8], schema: &TableSchema) -> Result<Self> {
    if schema.partition_keys.is_empty() {
        return Err(Error::InvalidInput(
            "Schema has no partition keys".to_string(),
        ));
    }

    if data.is_empty() {
        return Err(Error::InvalidInput(
            "Empty partition key bytes".to_string(),
        ));
    }

    let mut columns = Vec::with_capacity(schema.partition_keys.len());

    if schema.partition_keys.len() == 1 {
        // Single-component: raw value bytes
        let key_col = &schema.partition_keys[0];
        let comparator = ComparatorType::from_data_type(&key_col.data_type)?;
        let value = deserialize_value_bytes(data, &comparator)?;
        columns.push((key_col.name.clone(), value));
    } else {
        // Multi-component: [len:u16 BE][value bytes][0x00] per component
        let mut offset = 0;
        for key_col in &schema.partition_keys {
            if offset + 2 > data.len() {
                return Err(Error::InvalidInput(format!(
                    "Truncated multi-component partition key at offset {}",
                    offset
                )));
            }
            let len =
                u16::from_be_bytes([data[offset], data[offset + 1]]) as usize;
            offset += 2;

            if offset + len > data.len() {
                return Err(Error::InvalidInput(format!(
                    "Partition key component extends beyond data: offset={}, len={}, data_len={}",
                    offset, len, data.len()
                )));
            }

            let comparator = ComparatorType::from_data_type(&key_col.data_type)?;
            let value = deserialize_value_bytes(&data[offset..offset + len], &comparator)?;
            columns.push((key_col.name.clone(), value));
            offset += len;

            // Skip 0x00 end-of-component marker
            if offset < data.len() {
                offset += 1;
            }
        }
    }

    Ok(PartitionKey { columns })
}
```

- [ ] **Step 3: Add round-trip tests**

Add to the existing `#[cfg(test)] mod tests` in mutation.rs:

```rust
#[test]
fn test_partition_key_from_bytes_single_int() {
    let schema = TableSchema {
        keyspace: "ks".to_string(),
        table: "tbl".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![],
        comments: std::collections::HashMap::new(),
    };

    let original = PartitionKey::single("id", Value::Integer(42));
    let bytes = original.to_bytes(&schema).unwrap();
    let decoded = PartitionKey::from_bytes(&bytes, &schema).unwrap();
    assert_eq!(original, decoded);
}

#[test]
fn test_partition_key_from_bytes_single_uuid() {
    let schema = TableSchema {
        keyspace: "ks".to_string(),
        table: "tbl".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "uuid".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![],
        comments: std::collections::HashMap::new(),
    };

    let uuid_bytes = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let original = PartitionKey::single("id", Value::Uuid(uuid_bytes));
    let bytes = original.to_bytes(&schema).unwrap();
    let decoded = PartitionKey::from_bytes(&bytes, &schema).unwrap();
    assert_eq!(original, decoded);
}

#[test]
fn test_partition_key_from_bytes_single_text() {
    let schema = TableSchema {
        keyspace: "ks".to_string(),
        table: "tbl".to_string(),
        partition_keys: vec![KeyColumn {
            name: "name".to_string(),
            data_type: "text".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![],
        comments: std::collections::HashMap::new(),
    };

    let original = PartitionKey::single("name", Value::Text("hello".to_string()));
    let bytes = original.to_bytes(&schema).unwrap();
    let decoded = PartitionKey::from_bytes(&bytes, &schema).unwrap();
    assert_eq!(original, decoded);
}

#[test]
fn test_partition_key_from_bytes_multi_component() {
    let schema = TableSchema {
        keyspace: "ks".to_string(),
        table: "tbl".to_string(),
        partition_keys: vec![
            KeyColumn {
                name: "tenant".to_string(),
                data_type: "text".to_string(),
                position: 0,
            },
            KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 1,
            },
        ],
        clustering_keys: vec![],
        columns: vec![],
        comments: std::collections::HashMap::new(),
    };

    let original = PartitionKey::new(vec![
        ("tenant".to_string(), Value::Text("acme".to_string())),
        ("id".to_string(), Value::Integer(99)),
    ]);
    let bytes = original.to_bytes(&schema).unwrap();
    let decoded = PartitionKey::from_bytes(&bytes, &schema).unwrap();
    assert_eq!(original, decoded);
}

#[test]
fn test_partition_key_from_bytes_empty_errors() {
    let schema = TableSchema {
        keyspace: "ks".to_string(),
        table: "tbl".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![],
        comments: std::collections::HashMap::new(),
    };

    assert!(PartitionKey::from_bytes(&[], &schema).is_err());
}
```

- [ ] **Step 4: Run tests to verify**

Run: `env RUSTFLAGS="-D warnings" cargo test --package cqlite-core --features write-support -- mutation::tests::test_partition_key_from_bytes`
Expected: All 5 tests pass

- [ ] **Step 5: Commit**

```bash
git add cqlite-core/src/storage/write_engine/mutation.rs
git commit -m "feat(#447): add PartitionKey::from_bytes() for key deserialization"
```

---

### Task 2: Implement `SSTableRowIteratorAdapter` and fix `KWayMerger::new()`

**Files:**
- Modify: `cqlite-core/src/storage/write_engine/merge.rs`

The adapter pre-loads all entries from an SSTable (via `iterate_all_partitions()`) into a `Vec<MergeEntry>`, then yields them one by one. This is simpler than streaming and matches the existing pattern where the reader loads all partitions into memory.

- [ ] **Step 1: Add SSTableRowIteratorAdapter struct and imports**

Add after the `SSTableRowIterator` trait definition (after line 368), before `KWayMerger`:

```rust
/// Adapter that wraps async SSTableReader into sync SSTableRowIterator
///
/// Pre-loads all entries from an SSTable into memory, converting
/// `(RowKey, Value)` pairs into `MergeEntry` format. Uses tokio
/// `block_on` for async-to-sync bridging (same pattern as
/// `flush_internal` and `finalize_merge_blocking`).
struct SSTableRowIteratorAdapter {
    /// Pre-loaded entries
    entries: std::vec::IntoIter<MergeEntry>,
}

impl SSTableRowIteratorAdapter {
    /// Open an SSTable and load all entries as MergeEntry
    ///
    /// # Arguments
    /// * `path` - Path to the Data.db file
    /// * `schema` - Table schema for key/value deserialization
    /// * `run_index` - Index of this run in the merge (0 = newest)
    fn open(path: &Path, schema: &TableSchema, run_index: usize) -> Result<Self> {
        use crate::platform::Platform;
        use crate::storage::write_engine::mutation::PartitionKey;
        use crate::Config;
        use std::sync::Arc;

        let platform = Arc::new(Platform::default());
        let config = Config::default();

        // Open SSTable reader using async runtime (same pattern as finalize_merge_blocking)
        let reader = match tokio::runtime::Handle::try_current() {
            Ok(handle) => handle.block_on(
                crate::storage::sstable::reader::SSTableReader::open(
                    path, &config, platform,
                ),
            )?,
            Err(_) => {
                let rt = tokio::runtime::Runtime::new().map_err(|e| {
                    Error::Storage(format!("Failed to create tokio runtime: {}", e))
                })?;
                rt.block_on(
                    crate::storage::sstable::reader::SSTableReader::open(
                        path, &config, platform,
                    ),
                )?
            }
        };

        // Load all partitions
        let raw_entries = match tokio::runtime::Handle::try_current() {
            Ok(handle) => handle.block_on(reader.iterate_all_partitions())?,
            Err(_) => {
                let rt = tokio::runtime::Runtime::new().map_err(|e| {
                    Error::Storage(format!("Failed to create tokio runtime: {}", e))
                })?;
                rt.block_on(reader.iterate_all_partitions())?
            }
        };

        // Convert (RowKey, Value) pairs to MergeEntry
        let mut entries = Vec::with_capacity(raw_entries.len());
        for (row_key, value) in raw_entries {
            let key_bytes = row_key.0;
            let decorated_key = DecoratedKey::from_key_bytes(key_bytes)?;

            // Extract timestamp from the value
            let timestamp = reader.extract_write_time_from_entry(
                &crate::types::RowKey::new(decorated_key.key.clone()),
                &value,
            );

            // Convert Value to RowData
            let row_data = Self::value_to_row_data(&value, schema)?;

            entries.push(MergeEntry::new(
                run_index,
                decorated_key,
                None, // Clustering key extraction deferred - handled by merge_partition_rows
                timestamp,
                row_data,
            ));
        }

        // Sort by token for correct merge ordering
        entries.sort();

        Ok(Self {
            entries: entries.into_iter(),
        })
    }

    /// Convert a reader Value to RowData
    fn value_to_row_data(value: &crate::types::Value, _schema: &TableSchema) -> Result<RowData> {
        match value {
            crate::types::Value::Tombstone(info) => Ok(RowData::Tombstone {
                deletion_time: info.deletion_time,
                local_deletion_time: info.local_deletion_time,
            }),
            crate::types::Value::Map(entries) => {
                let mut cells = Vec::with_capacity(entries.len());
                for (key, val) in entries {
                    let column = match key {
                        crate::types::Value::Text(s) => s.clone(),
                        other => format!("{:?}", other),
                    };
                    cells.push(CellData {
                        column,
                        value: val.clone(),
                        timestamp: 0, // Per-cell timestamps not available from reader
                        ttl: None,
                    });
                }
                Ok(RowData::Live { cells })
            }
            // Single value or other formats - wrap as a single cell
            other => Ok(RowData::Live {
                cells: vec![CellData {
                    column: "value".to_string(),
                    value: other.clone(),
                    timestamp: 0,
                    ttl: None,
                }],
            }),
        }
    }
}

impl SSTableRowIterator for SSTableRowIteratorAdapter {
    fn next(&mut self) -> Option<Result<MergeEntry>> {
        self.entries.next().map(Ok)
    }
}
```

- [ ] **Step 2: Replace KWayMerger::new() stub**

Replace the body of `KWayMerger::new()` (lines 433-460):

```rust
pub fn new(input_paths: Vec<PathBuf>, schema: &TableSchema) -> Result<Self> {
    if input_paths.is_empty() {
        return Err(Error::InvalidInput(
            "K-way merge requires at least one input file".to_string(),
        ));
    }

    // Create run readers for each input SSTable (ordered newest to oldest)
    let mut runs = Vec::with_capacity(input_paths.len());
    for (run_index, path) in input_paths.iter().enumerate() {
        let adapter = SSTableRowIteratorAdapter::open(path, schema, run_index)?;
        runs.push(RunReader::new(Box::new(adapter)));
    }

    // Initialize heap (will be populated on first step)
    let heap = BinaryHeap::new();

    Ok(Self {
        runs,
        heap,
        current_partition: None,
        schema: schema.clone(),
    })
}
```

- [ ] **Step 3: Remove `#[allow(dead_code)]` from RunReader methods**

Remove the two `#[allow(dead_code)]` annotations on `DEFAULT_BUFFER_SIZE` (line 234) and `fn new` (line 238) since they're now used.

- [ ] **Step 4: Add `use std::path::Path;` import**

Add to the `#[cfg(feature = "write-support")]` imports section at top of merge.rs if not already present.

- [ ] **Step 5: Run build to verify compilation**

Run: `cargo build --package cqlite-core --features write-support`
Expected: Compiles successfully

- [ ] **Step 6: Commit**

```bash
git add cqlite-core/src/storage/write_engine/merge.rs
git commit -m "feat(#447): implement SSTableRowIteratorAdapter and wire KWayMerger::new()"
```

---

### Task 3: Implement `merge_entry_to_mutation()` (both copies)

**Files:**
- Modify: `cqlite-core/src/storage/write_engine/merge.rs` (KWayMerger version)
- Modify: `cqlite-core/src/storage/write_engine/mod.rs` (WriteEngine version)

- [ ] **Step 1: Implement KWayMerger::merge_entry_to_mutation in merge.rs**

Replace the stub at lines 657-673:

```rust
/// Convert a MergeEntry back to Mutation for writing
fn merge_entry_to_mutation(
    entry: MergeEntry,
    schema: &TableSchema,
) -> Result<crate::storage::write_engine::mutation::Mutation> {
    use crate::storage::write_engine::mutation::{
        CellOperation, Mutation, PartitionKey, TableId,
    };

    // Reconstruct PartitionKey from DecoratedKey bytes
    let partition_key = PartitionKey::from_bytes(&entry.key.key, schema)?;

    let table_id = TableId::new(&schema.keyspace, &schema.table);

    // Convert row data to cell operations
    let operations = match entry.row_data {
        RowData::Live { cells } => cells
            .into_iter()
            .map(|cell| {
                if let Some(ttl) = cell.ttl {
                    CellOperation::WriteWithTtl {
                        column: cell.column,
                        value: cell.value,
                        ttl_seconds: ttl,
                    }
                } else {
                    CellOperation::Write {
                        column: cell.column,
                        value: cell.value,
                    }
                }
            })
            .collect(),
        RowData::Tombstone { .. } => vec![CellOperation::DeleteRow],
    };

    Ok(Mutation::new(
        table_id,
        partition_key,
        entry.clustering_key,
        operations,
        entry.timestamp,
        None,
    ))
}
```

- [ ] **Step 2: Implement WriteEngine::merge_entry_to_mutation in mod.rs**

Replace the stub at lines 1028-1039:

```rust
/// Convert MergeEntry to Mutation (M5.2 helper)
fn merge_entry_to_mutation(
    &self,
    entry: merge::MergeEntry,
) -> Result<crate::storage::write_engine::mutation::Mutation> {
    use crate::storage::write_engine::mutation::{
        CellOperation, Mutation, PartitionKey, TableId,
    };

    // Reconstruct PartitionKey from DecoratedKey bytes
    let partition_key = PartitionKey::from_bytes(&entry.key.key, &self.config.schema)?;

    let table_id = TableId::new(&self.config.schema.keyspace, &self.config.schema.table);

    // Convert row data to cell operations
    let operations = match entry.row_data {
        merge::RowData::Live { cells } => cells
            .into_iter()
            .map(|cell| {
                if let Some(ttl) = cell.ttl {
                    CellOperation::WriteWithTtl {
                        column: cell.column,
                        value: cell.value,
                        ttl_seconds: ttl,
                    }
                } else {
                    CellOperation::Write {
                        column: cell.column,
                        value: cell.value,
                    }
                }
            })
            .collect(),
        merge::RowData::Tombstone { .. } => vec![CellOperation::DeleteRow],
    };

    Ok(Mutation::new(
        table_id,
        partition_key,
        entry.clustering_key,
        operations,
        entry.timestamp,
        None,
    ))
}
```

- [ ] **Step 3: Add unit tests for merge_entry_to_mutation in merge.rs**

Add to existing `mod tests` in merge.rs:

```rust
#[test]
fn test_merge_entry_to_mutation_live_cells() {
    use crate::schema::{KeyColumn, TableSchema};
    use crate::storage::write_engine::mutation::DecoratedKey;
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
    };

    // Create a MergeEntry with live cells
    // Int value 42 = 0x0000002A in big-endian
    let key_bytes = 42i32.to_be_bytes().to_vec();
    let token = -1; // Token doesn't matter for conversion
    let entry = MergeEntry::new(
        0,
        DecoratedKey::new(token, key_bytes),
        None,
        1000000,
        RowData::Live {
            cells: vec![
                CellData {
                    column: "name".to_string(),
                    value: Value::Text("Alice".to_string()),
                    timestamp: 1000000,
                    ttl: None,
                },
                CellData {
                    column: "age".to_string(),
                    value: Value::Integer(30),
                    timestamp: 1000000,
                    ttl: Some(3600),
                },
            ],
        },
    );

    let mutation = KWayMerger::merge_entry_to_mutation(entry, &schema).unwrap();
    assert_eq!(mutation.table.keyspace, "test_ks");
    assert_eq!(mutation.table.table, "test_table");
    assert_eq!(mutation.partition_key.columns[0].0, "id");
    assert_eq!(mutation.partition_key.columns[0].1, Value::Integer(42));
    assert_eq!(mutation.operations.len(), 2);
    assert_eq!(mutation.timestamp_micros, 1000000);
}

#[test]
fn test_merge_entry_to_mutation_tombstone() {
    use crate::schema::{KeyColumn, TableSchema};
    use crate::storage::write_engine::mutation::{CellOperation, DecoratedKey};
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
    };

    let key_bytes = 99i32.to_be_bytes().to_vec();
    let entry = MergeEntry::new(
        0,
        DecoratedKey::new(-1, key_bytes),
        None,
        2000000,
        RowData::Tombstone {
            deletion_time: 2000000,
            local_deletion_time: 1000,
        },
    );

    let mutation = KWayMerger::merge_entry_to_mutation(entry, &schema).unwrap();
    assert_eq!(mutation.operations.len(), 1);
    assert!(matches!(mutation.operations[0], CellOperation::DeleteRow));
}
```

- [ ] **Step 4: Run tests**

Run: `env RUSTFLAGS="-D warnings" cargo test --package cqlite-core --features write-support -- merge::tests::test_merge_entry_to_mutation`
Expected: Both tests pass

- [ ] **Step 5: Commit**

```bash
git add cqlite-core/src/storage/write_engine/merge.rs cqlite-core/src/storage/write_engine/mod.rs
git commit -m "feat(#447): implement merge_entry_to_mutation for compaction pipeline"
```

---

### Task 4: Enable `set_merge_policy()`

**Files:**
- Modify: `cqlite-core/src/storage/write_engine/mod.rs`

- [ ] **Step 1: Replace set_merge_policy stub**

Replace lines 700-723:

```rust
/// Set the merge policy for background compaction (M5.2, Issue #383)
///
/// # Arguments
///
/// * `policy` - Merge policy implementation (e.g., STCS, LCS, TWCS)
///
/// # Example
///
/// ```rust,ignore
/// use cqlite_core::storage::write_engine::STCSPolicy;
/// engine.set_merge_policy(Box::new(STCSPolicy::default()))?;
/// ```
pub fn set_merge_policy(&mut self, policy: Box<dyn MergePolicy>) -> Result<()> {
    self.merge_policy = Some(policy);
    Ok(())
}
```

- [ ] **Step 2: Add test for set_merge_policy**

Add to existing `mod tests` in mod.rs:

```rust
#[test]
fn test_set_merge_policy() {
    let temp_dir = TempDir::new().unwrap();
    let schema = create_test_schema();

    let config = WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        schema,
    );

    let mut engine = WriteEngine::new(config).unwrap();

    // Should succeed now (was previously returning error)
    let policy = Box::new(crate::storage::write_engine::STCSPolicy::default());
    engine.set_merge_policy(policy).unwrap();

    // Verify policy is set by checking maintenance_step behavior
    // With policy set but no SSTables, should return quickly with no work
    let report = engine
        .maintenance_step(std::time::Duration::from_millis(100))
        .unwrap();
    assert!(!report.pending_compaction);
    assert_eq!(report.rows_merged, 0);
}
```

- [ ] **Step 3: Run tests**

Run: `env RUSTFLAGS="-D warnings" cargo test --package cqlite-core --features write-support -- tests::test_set_merge_policy`
Expected: Pass

- [ ] **Step 4: Commit**

```bash
git add cqlite-core/src/storage/write_engine/mod.rs
git commit -m "feat(#447): enable set_merge_policy by removing M5.3 guard"
```

---

### Task 5: Build, test, and clippy validation

- [ ] **Step 1: Run full build**

Run: `cargo build --package cqlite-core --features write-support`
Expected: Compiles successfully

- [ ] **Step 2: Run all write-engine tests**

Run: `env RUSTFLAGS="-D warnings" cargo test --package cqlite-core --features write-support -- storage::write_engine`
Expected: All tests pass

- [ ] **Step 3: Run clippy**

Run: `env RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features`
Expected: No warnings or errors

- [ ] **Step 4: Run cargo fmt**

Run: `cargo fmt --check`
Expected: No formatting issues

- [ ] **Step 5: Fix any issues found in steps 1-4**

Address compiler errors, test failures, clippy warnings, or format issues.

---

### Task 6: Commit and push

- [ ] **Step 1: Create final commit if needed**

If fixes from Task 5 required changes, commit them.

- [ ] **Step 2: Push to remote**

Run: `git push`
Expected: Push succeeds, CI triggered
