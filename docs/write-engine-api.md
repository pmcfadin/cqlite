# WriteEngine API Documentation

## Overview

The `WriteEngine` is the primary public API for write operations in CQLite. It coordinates the Write-Ahead Log (WAL), in-memory Memtable, and SSTable generation to provide durable, crash-safe write operations.

**Implementation**: Issue #371 (M5.0-6)

## Architecture

```
┌─────────────┐
│   Client    │
└─────┬───────┘
      │ write(mutation) / execute(cql)
      ▼
┌─────────────────────────────────┐
│       WriteEngine               │
│  ┌──────────┐  ┌──────────┐   │
│  │   WAL    │  │ Memtable │   │
│  └──────────┘  └──────────┘   │
└─────────┬───────────────────────┘
          │ flush()
          ▼
    ┌──────────────┐
    │  SSTableWriter│
    └──────────────┘
          │
          ▼
    ┌──────────────┐
    │  Data.db     │
    │  Index.db    │
    │  Filter.db   │
    │  Summary.db  │
    │  Statistics.db│
    │  TOC.txt     │
    └──────────────┘
```

## Write Flow

1. User calls `write(mutation)` or `execute(cql_statement)`
2. WriteEngine appends mutation to WAL (durability)
3. WriteEngine inserts mutation into Memtable
4. If Memtable size exceeds threshold → automatic flush to SSTable
5. After successful flush → WAL is truncated

## Core Types

### WriteEngineConfig

Configuration for the WriteEngine.

```rust
pub struct WriteEngineConfig {
    /// Directory for SSTable data files
    pub data_dir: PathBuf,
    /// Directory for WAL files
    pub wal_dir: PathBuf,
    /// Memtable flush threshold in bytes (default: 64MB)
    pub memtable_flush_threshold: usize,
    /// Table schema for column metadata
    pub schema: TableSchema,
}
```

**Constructor**:
```rust
pub fn new(data_dir: PathBuf, wal_dir: PathBuf, schema: TableSchema) -> Self
```

**Methods**:
```rust
pub fn with_flush_threshold(self, threshold: usize) -> Self
```

**Constants**:
- `DEFAULT_FLUSH_THRESHOLD`: 64 MB (67,108,864 bytes)

### WriteEngine

Main coordinator for write operations.

```rust
pub struct WriteEngine {
    config: WriteEngineConfig,
    wal: WriteAheadLog,
    memtable: Memtable,
    generation: u32,
    closed: bool,
}
```

## API Methods

### Constructor

```rust
pub fn new(config: WriteEngineConfig) -> Result<Self>
```

Creates a new WriteEngine instance.

**Behavior**:
- Ensures `data_dir` and `wal_dir` exist
- If WAL exists, replays all entries into memtable (crash recovery)
- Scans `data_dir` to determine next SSTable generation number

**Returns**: `Result<WriteEngine>`

**Errors**:
- Directory creation fails
- WAL replay fails
- Invalid schema

**Example**:
```rust
use cqlite_core::storage::write_engine::{WriteEngine, WriteEngineConfig};
use std::path::PathBuf;

let config = WriteEngineConfig::new(
    PathBuf::from("data/test_ks/users"),
    PathBuf::from("wal"),
    schema
);

let mut engine = WriteEngine::new(config)?;
```

### write()

```rust
pub fn write(&mut self, mutation: Mutation) -> Result<()>
```

Writes a mutation to the engine.

**Steps**:
1. Appends mutation to WAL (with fsync)
2. Inserts mutation into memtable
3. Checks memtable size against flush threshold
4. If threshold exceeded → triggers automatic flush

**Returns**: `Result<()>`

**Errors**:
- Engine has been closed
- WAL append fails
- Memtable insert fails
- Automatic flush fails

**Example**:
```rust
use cqlite_core::storage::write_engine::{Mutation, PartitionKey, TableId, CellOperation};
use cqlite_core::types::Value;

let table_id = TableId::new("test_ks", "users");
let pk = PartitionKey::single("id", Value::Integer(1));
let ops = vec![CellOperation::Write {
    column: "name".to_string(),
    value: Value::Text("Alice".to_string()),
}];

let mutation = Mutation::new(table_id, pk, None, ops, 1234567890, None);
engine.write(mutation)?;
```

### execute()

```rust
pub fn execute(&mut self, statement: &str) -> Result<()>
```

Executes a CQL statement (INSERT, UPDATE, DELETE).

**Note**: CQL parsing is not yet implemented in M5.0-6. This method currently returns an error.

**Planned support** (M5.0-8):
- `INSERT INTO table (col1, col2) VALUES (?, ?)`
- `UPDATE table SET col = ? WHERE id = ?`
- `DELETE FROM table WHERE id = ?`

**Returns**: `Result<()>`

**Errors**:
- CQL parsing fails
- Statement is not a mutation
- Write fails

### flush()

```rust
pub async fn flush(&mut self) -> Result<Option<SSTableInfo>>
```

Forces a flush of the memtable to SSTable.

**Behavior**:
1. If memtable is empty → returns `None`
2. Creates SSTableWriter with current generation number
3. Writes all partitions from memtable (in token order)
4. Finalizes all SSTable components (Data.db, Index.db, Filter.db, Summary.db, Statistics.db, Digest.crc32, TOC.txt)
5. Truncates WAL (data now persisted)
6. Clears memtable
7. Increments generation counter

**Returns**: `Result<Option<SSTableInfo>>`
- `Some(SSTableInfo)` if data was flushed
- `None` if memtable was empty

**Errors**:
- Engine has been closed
- SSTable write fails
- WAL truncate fails

**Example**:
```rust
let info = engine.flush().await?;

if let Some(info) = info {
    println!("Flushed {} partitions to {}",
             info.partition_count,
             info.data_path.display());
}
```

### close()

```rust
pub async fn close(self) -> Result<()>
```

Closes the write engine cleanly.

**Behavior**:
- If memtable is non-empty → flushes to SSTable
- Marks engine as closed
- After close, engine cannot be used for further writes

**Returns**: `Result<()>`

**Errors**:
- Final flush fails

**Example**:
```rust
// Consume engine and close
engine.close().await?;
```

### Inspection Methods

```rust
pub fn memtable_size(&self) -> usize
pub fn memtable_row_count(&self) -> usize
pub fn wal_size(&self) -> u64
pub fn generation(&self) -> u32
```

Provides read-only access to internal state for monitoring.

## SSTableInfo

Information about a written SSTable, returned by `flush()`.

```rust
pub struct SSTableInfo {
    pub data_path: PathBuf,
    pub index_path: PathBuf,
    pub filter_path: PathBuf,
    pub summary_path: PathBuf,
    pub stats_path: PathBuf,
    pub toc_path: PathBuf,
    pub digest_path: PathBuf,
    pub partition_count: usize,
    pub data_size: u64,
}
```

All paths follow the naming convention: `nb-{generation}-big-{Component}.db`

Example: `nb-1-big-Data.db`, `nb-1-big-Index.db`

## Error Handling

The WriteEngine uses `cqlite_core::error::Error` for all error conditions:

| Error Type | Cause |
|------------|-------|
| `InvalidInput` | Engine closed, invalid mutation |
| `Storage` | I/O failure, WAL/SSTable write failure |
| `Schema` | Schema validation failure |

## Thread Safety

**WriteEngine is NOT thread-safe**. It follows a single-writer model.

If concurrent writes are needed, protect the engine with external synchronization:

```rust
use std::sync::{Arc, Mutex};

let engine = Arc::new(Mutex::new(WriteEngine::new(config)?));

// Thread 1
{
    let mut engine = engine.lock().unwrap();
    engine.write(mutation1)?;
}

// Thread 2
{
    let mut engine = engine.lock().unwrap();
    engine.write(mutation2)?;
}
```

## Crash Recovery

On startup, WriteEngine automatically replays all valid entries from the WAL:

1. Opens existing WAL (if present)
2. Reads all entries sequentially
3. Validates CRC32 for each entry
4. Skips corrupted entries (logs warning)
5. Stops at truncated entries
6. Inserts valid mutations into memtable

**Example recovery scenario**:

```rust
// Session 1: Write 100 mutations, crash before flush
{
    let mut engine = WriteEngine::new(config.clone())?;
    for i in 0..100 {
        engine.write(mutation)?;
    }
    // CRASH (no flush)
}

// Session 2: Automatic recovery
{
    let engine = WriteEngine::new(config)?;
    assert_eq!(engine.memtable_row_count(), 100); // All recovered
}
```

## Flush Triggers

Flush can be triggered in three ways:

1. **Automatic**: When memtable size exceeds `memtable_flush_threshold`
2. **Manual**: By calling `flush()` explicitly
3. **Close**: When `close()` is called with non-empty memtable

## Generation Management

Each SSTable flush creates a new generation:

- Generation starts at 1 (or max existing + 1)
- Increments after each successful flush
- Persists across restarts (scanned from data directory)

File naming: `nb-{generation}-big-{Component}.db`

**Example**:
```
Generation 1: nb-1-big-Data.db, nb-1-big-Index.db, ...
Generation 2: nb-2-big-Data.db, nb-2-big-Index.db, ...
```

## Best Practices

### 1. Flush Threshold Tuning

**Default (64 MB)**: Good for most use cases
- ~100K-500K mutations per flush
- Reasonable memory overhead
- Moderate SSTable count

**Low threshold (1-8 MB)**: Many small SSTables
- Lower memory usage
- More frequent I/O
- Higher compaction overhead

**High threshold (128-512 MB)**: Fewer large SSTables
- Higher memory usage
- Less frequent I/O
- Lower compaction overhead

### 2. Directory Structure

Recommended layout:

```
/data/{keyspace}/{table}/
  ├── nb-1-big-Data.db
  ├── nb-1-big-Index.db
  ├── nb-1-big-Filter.db
  └── ...
/wal/
  └── commitlog.wal
```

### 3. Monitoring

Track these metrics:

```rust
// Memory pressure
if engine.memtable_size() > threshold * 0.8 {
    log::warn!("Memtable approaching flush threshold");
}

// WAL growth
if engine.wal_size() > 100 * 1024 * 1024 {
    log::warn!("WAL exceeds 100MB - consider flush");
}

// Generation count (indicates compaction needs)
if engine.generation() > 100 {
    log::info!("100+ generations - compaction recommended");
}
```

### 4. Graceful Shutdown

Always call `close()` to ensure data durability:

```rust
// Good
engine.close().await?;

// Bad (data loss risk)
drop(engine); // WAL not flushed
```

### 5. Error Recovery

Handle write errors appropriately:

```rust
match engine.write(mutation) {
    Ok(()) => {
        // Success
    }
    Err(Error::Storage(msg)) => {
        log::error!("Storage failure: {}", msg);
        // Retry or escalate
    }
    Err(e) => {
        log::error!("Write failed: {}", e);
        // Handle error
    }
}
```

## Limitations (M5.0-6)

1. **CQL Parsing**: Not yet implemented. Use `write(mutation)` directly.
2. **Single Writer**: No concurrent write support.
3. **No Compaction**: SSTables accumulate, compaction in M5.0-9.
4. **No Tombstone GC**: Delete markers persist until compaction.
5. **Fixed Murmur3**: Only Murmur3Partitioner supported.

## Future Enhancements

- **M5.0-8**: CQL INSERT/UPDATE/DELETE parsing
- **M5.0-9**: K-way merge compaction
- **M6**: Multi-table support
- **M7**: Concurrent writes with locking

## See Also

- [WAL API Documentation](./wal-api.md)
- [Memtable API Documentation](./memtable-api.md)
- [SSTableWriter API Documentation](./sstable-writer-api.md)
- [Mutation Types](./mutation-types.md)
