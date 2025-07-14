# CQLite Simplified Architecture Diagram

## Simplified System Architecture (Single SSTable + CQL)

```
                    ┌─────────────────────────────────────────────┐
                    │            Language Bindings               │
                    │  ┌─────────┐  ┌─────────┐  ┌─────────┐     │
                    │  │ Python  │  │ Node.js │  │   Go    │     │
                    │  │ (PyO3)  │  │ (N-API) │  │ (CGO)   │     │
                    │  └─────────┘  └─────────┘  └─────────┘     │
                    └─────────────┬───────────────────────────────┘
                                  │
    ┌─────────────────────────────┼─────────────────────────────┐
    │                             │                             │
    │          C FFI Layer        │        WASM Bindings       │
    │  ┌─────────────────────────┐ │ ┌─────────────────────────┐ │
    │  │ - Safe abstractions    │ │ │ - JavaScript API        │ │
    │  │ - Error handling       │ │ │ - IndexedDB integration │ │
    │  │ - Memory management    │ │ │ - Web Workers support   │ │
    │  │ - Thread safety        │ │ │ - SIMD optimizations   │ │
    │  └─────────────────────────┘ │ └─────────────────────────┘ │
    └─────────────────────────────┼─────────────────────────────┘
                                  │
            ┌─────────────────────┴─────────────────────┐
            │                Core Engine                │
            │              (cqlite-core)                │
            └─────────────────────┬─────────────────────┘
                                  │
    ┌─────────────────────────────┼─────────────────────────────┐
    │                             │                             │
    │        CQL Query Engine     │       Schema Manager        │
    │  ┌─────────────────────────┐ │ ┌─────────────────────────┐ │
    │  │ CQL Parser (Antlr4)    │ │ │ Type System            │ │
    │  │ Simple Query Planner   │ │ │ Schema Evolution       │ │
    │  │ Execution Engine       │ │ │ Constraint Validation  │ │
    │  │ Expression Evaluator   │ │ │ Catalog Management     │ │
    │  │ CQL AST Processing     │ │ │ DDL Operations         │ │
    │  └─────────────────────────┘ │ └─────────────────────────┘ │
    └─────────────────────────────┼─────────────────────────────┘
                                  │
            ┌─────────────────────┴─────────────────────┐
            │               Storage Engine              │
            └─────────────────────┬─────────────────────┘
                                  │
    ┌─────────────────────────────┼─────────────────────────────┐
    │                             │                             │
    │       MemTable              │      Single SSTable         │
    │  ┌─────────────────────────┐ │ ┌─────────────────────────┐ │
    │  │ Skip List Structure    │ │ │ One Immutable File     │ │
    │  │ In-Memory Buffer       │ │ │ Simple Block Storage   │ │
    │  │ Write Operations       │ │ │ Basic Index Only       │ │
    │  │ Flush Management       │ │ │ Optional Compression   │ │
    │  └─────────────────────────┘ │ │ Cassandra 5 Layout     │ │
    │                             │ └─────────────────────────┘ │
    └─────────────────────────────┼─────────────────────────────┘
                                  │
    ┌─────────────────────────────┼─────────────────────────────┐
    │                             │                             │
    │     Write-Ahead Log         │    Directory Layout         │
    │  ┌─────────────────────────┐ │ ┌─────────────────────────┐ │
    │  │ Durability Guarantee   │ │ │ Cassandra 5 Structure  │ │
    │  │ Sequential Writes      │ │ │ Data.db + Index.db     │ │
    │  │ Recovery Support       │ │ │ Statistics.db          │ │
    │  │ Configurable Sync      │ │ │ Summary.db + TOC.txt   │ │
    │  └─────────────────────────┘ │ └─────────────────────────┘ │
    └─────────────────────────────┼─────────────────────────────┘
                                  │
            ┌─────────────────────┴─────────────────────┐
            │         Simplified Memory Management      │
            │  ┌─────────────────────────────────────┐  │
            │  │ Simple Block Cache (LRU)           │  │
            │  │ Basic Buffer Pool                  │  │
            │  │ No Complex Cache Hierarchy         │  │
            │  │ Minimal Memory Footprint           │  │
            │  └─────────────────────────────────────┘  │
            └─────────────────────┬─────────────────────┘
                                  │
            ┌─────────────────────┴─────────────────────┐
            │           Platform Abstraction            │
            │  ┌─────────────────────────────────────┐  │
            │  │ File System (Native/WASM)          │  │
            │  │ Threading (Tokio/Web Workers)      │  │
            │  │ Time Utilities                     │  │
            │  │ Synchronization Primitives         │  │
            │  └─────────────────────────────────────┘  │
            └───────────────────────────────────────────┘
```

## Data Flow Architecture

### Simplified Write Path
```
CQL INSERT
     │
     ▼
┌────────────┐     ┌──────────────┐     ┌─────────────┐
│ CQL Parser │────▶│Simple Planner│────▶│ Validator   │
└────────────┘     └──────────────┘     └─────────────┘
                                              │
                                              ▼
                                        ┌─────────────┐
                                        │ MemTable    │◀── In-memory
                                        └─────────────┘    write buffer
                                              │
                                              ▼
                                        ┌─────────────┐
                                        │ WAL Write   │◀── Durability
                                        └─────────────┘    guarantee
                                              │
                                        (Async Flush)
                                              ▼
                                        ┌─────────────┐
                                        │Single SSTable│◀── Persistent
                                        │ Creation     │    storage
                                        └─────────────┘
                                              │
                                              ▼
                                        ┌─────────────┐
                                        │ Directory   │◀── Cassandra 5
                                        │ Layout      │    file structure
                                        └─────────────┘
```

### Simplified Read Path
```
CQL SELECT
     │
     ▼
┌────────────┐     ┌──────────────┐     ┌─────────────┐
│ CQL Parser │────▶│Simple Planner│────▶│ Executor    │
└────────────┘     └──────────────┘     └─────────────┘
                                              │
                                              ▼
                                        ┌─────────────┐
                                        │ Executor    │
                                        └─────────────┘
                                              │
                              ┌───────────────┼───────────────┐
                              ▼               ▼               ▼
                        ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
                        │ Simple Cache│ │ MemTable    │ │Single SSTable│
                        │ Lookup      │ │ Search      │ │ Search       │
                        └─────────────┘ └─────────────┘ └─────────────┘
                              │               │               │
                              └───────────────┼───────────────┘
                                              ▼
                                        ┌─────────────┐
                                        │ Result      │
                                        │ Merge &     │
                                        │ Dedup       │
                                        └─────────────┘
                                              │
                                              ▼
                                        ┌─────────────┐
                                        │ Result Set  │
                                        └─────────────┘
```

## Simplified SSTable File Format (No Bloom Filters)

```
┌─────────────────────────────────────────────────────────────┐
│                        File Header                          │
│  ┌─────────────┬─────────────┬─────────────┬─────────────┐  │
│  │ Magic (4B)  │ Version (4B)│ Flags (4B)  │ Reserved    │  │
│  └─────────────┴─────────────┴─────────────┴─────────────┘  │
├─────────────────────────────────────────────────────────────┤
│                       Data Blocks                          │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ Block 0: [Entry][Entry][Entry]...[Optional Comp]   │   │
│  │ Block 1: [Entry][Entry][Entry]...[Optional Comp]   │   │
│  │ Block N: [Entry][Entry][Entry]...[Optional Comp]   │   │
│  └─────────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────────┤
│                      Index Block                           │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ Simple Index: [Offset][Size][First Key][Last Key]  │   │
│  │ Entry Count: [Total Entries][Block Count]          │   │
│  └─────────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────────┤
│                        Footer                              │
│  ┌─────────────┬─────────────┬─────────────┬─────────────┐  │
│  │Index Offset │ Data Size   │ Checksum    │ Magic (4B)  │  │
│  │    (8B)     │    (8B)     │   (4B)      │             │  │
│  └─────────────┴─────────────┴─────────────┴─────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Cassandra 5 Directory Layout (Per Table)

```
/keyspace_name/table_name/
├── na-1-big-Data.db          # Main data file (SSTable data)
├── na-1-big-Index.db         # Index file (partition/clustering keys)
├── na-1-big-Statistics.db    # Table statistics and metadata
├── na-1-big-Summary.db       # Partition summary for quick lookups
└── na-1-big-TOC.txt         # Table of contents (file manifest)

Optional files (not implemented initially):
├── na-1-big-Filter.db        # (Skip - No bloom filters)
├── na-1-big-CompressionInfo.db # Compression metadata
└── na-1-big-Digest.crc32     # File integrity checksum
```

## Simplified Memory Layout

```
┌─────────────────────────────────────────────────────────────┐
│                       Process Memory                       │
├─────────────────────────────────────────────────────────────┤
│ ┌─────────────────────────────────────────────────────────┐ │
│ │                 Application Memory                      │ │
│ │ ┌─────────────┬─────────────┬─────────────┬─────────────┐ │ │
│ │ │ User Code   │ FFI/WASM    │ Result Sets │ Temp Data   │ │ │
│ │ └─────────────┴─────────────┴─────────────┴─────────────┘ │ │
│ └─────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│ ┌─────────────────────────────────────────────────────────┐ │
│ │                CQLite Memory (Simplified)               │ │
│ │ ┌─────────────┬─────────────┬─────────────┬─────────────┐ │ │
│ │ │Simple Cache │             │ Working Set │ CQL Parser  │ │ │
│ │ │   (32MB)    │  (Unused)   │ (Variable)  │   (8MB)     │ │ │
│ │ └─────────────┴─────────────┴─────────────┴─────────────┘ │ │
│ │ ┌─────────────┬─────────────┬─────────────┬─────────────┐ │ │
│ │ │ MemTable    │ WAL Buffer  │ I/O Buffers │ Metadata    │ │ │
│ │ │   (16MB)    │   (4MB)     │   (16MB)    │   (4MB)     │ │ │
│ │ └─────────────┴─────────────┴─────────────┴─────────────┘ │ │
│ └─────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│ ┌─────────────────────────────────────────────────────────┐ │
│ │                  System Memory                          │ │
│ │ ┌─────────────┬─────────────┬─────────────┬─────────────┐ │ │
│ │ │ OS Buffers  │ Network     │ File System │ Overhead    │ │ │
│ │ └─────────────┴─────────────┴─────────────┴─────────────┘ │ │
│ └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘

Total Simplified Configuration: ~128MB (75% reduction)
WASM Optimized: ~64MB (50% reduction) 
Memory Optimized: ~80MB (70% reduction)
```

## Thread Architecture

### Simplified Native Deployment
```
┌─────────────────────────────────────────────────────────────┐
│                      Main Thread                           │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ CQL Processing │ Simple Query Exec │ API Handling       │ │
│ └─────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│               Minimal Background Threads                   │
│ ┌─────────────┬─────────────┬─────────────┬─────────────────┐ │
│ │ (No         │ I/O Thread  │ WAL Sync    │ Basic Stats     │ │
│ │ Compaction) │ Pool        │ Thread      │ (Optional)      │ │
│ └─────────────┴─────────────┴─────────────┴─────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### WASM Deployment
```
┌─────────────────────────────────────────────────────────────┐
│                      Main Thread                           │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ All Operations (Single Threaded + Async)               │ │
│ └─────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│                   Web Workers (Optional)                   │
│ ┌─────────────┬─────────────┬─────────────┬─────────────────┐ │
│ │ Worker 1    │ Worker 2    │ Worker 3    │ Worker 4        │ │
│ │ Compaction  │ I/O Ops     │ Heavy Comp  │ Background      │ │
│ └─────────────┴─────────────┴─────────────┴─────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## Error Handling Strategy

```
┌─────────────────────────────────────────────────────────────┐
│                    Error Categories                        │
├─────────────────────────────────────────────────────────────┤
│ System Errors                                              │
│ ├── I/O Errors (recoverable with retry)                    │
│ ├── Memory Errors (recoverable with cleanup)               │
│ └── Platform Errors (context dependent)                    │
├─────────────────────────────────────────────────────────────┤
│ Data Errors                                                │
│ ├── Corruption Errors (non-recoverable)                    │
│ ├── Serialization Errors (non-recoverable)                 │
│ └── Type Conversion Errors (non-recoverable)               │
├─────────────────────────────────────────────────────────────┤
│ Logic Errors                                               │
│ ├── SQL Parse Errors (non-recoverable)                     │
│ ├── Schema Errors (non-recoverable)                        │
│ └── Constraint Violations (non-recoverable)                │
├─────────────────────────────────────────────────────────────┤
│ Concurrency Errors                                         │
│ ├── Lock Timeouts (recoverable with retry)                 │
│ ├── Transaction Conflicts (recoverable)                    │
│ └── Resource Contention (recoverable)                      │
└─────────────────────────────────────────────────────────────┘

Error Propagation:
Rust Core ─→ FFI Layer ─→ Language Bindings
   │              │             │
   │              │             └─→ Language-specific exceptions
   │              └─→ C error codes + message buffer
   └─→ Result<T, Error> types
```

## Simplified Core Components Design

### CQL Parser Module
```rust
// Based on Antlr4 grammar from Patrick's Cassandra grammar
pub struct CQLParser {
    lexer: CQLLexer,
    ast_builder: ASTBuilder,
}

// No complex optimization - simple, direct parsing
pub enum CQLStatement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    CreateTable(CreateTableStatement),
    CreateKeyspace(CreateKeyspaceStatement),
}
```

### Single SSTable Storage
```rust
// One SSTable per table - no compaction complexity
pub struct SingleSSTable {
    data_file: File,      // na-1-big-Data.db
    index_file: File,     // na-1-big-Index.db  
    summary_file: File,   // na-1-big-Summary.db
    stats_file: File,     // na-1-big-Statistics.db
    toc_file: File,       // na-1-big-TOC.txt
}

// No bloom filters - simple direct reads
impl SingleSSTable {
    pub fn read(&self, key: &[u8]) -> Result<Option<Row>>;
    pub fn write_batch(&mut self, rows: Vec<Row>) -> Result<()>;
    // No compaction methods needed
}
```

### Memory Management (Simplified)
```rust
pub struct SimpleMemoryManager {
    block_cache: LRUCache<BlockId, Block>,  // 32MB max
    buffer_pool: BufferPool,                // 16MB max
    // No complex cache hierarchy
}
```

## Key Simplifications Summary

1. **Single SSTable per table** - Eliminates compaction complexity entirely
2. **No bloom filters** - Direct reads, simpler code, smaller memory footprint  
3. **CQL parser only** - No SQL parsing, use proven Antlr4 grammar
4. **Cassandra 5 file layout** - Mirror real Cassandra structure for compatibility
5. **Minimal memory usage** - ~128MB total vs ~1GB in original design
6. **Reduced thread complexity** - No compaction threads needed
7. **Simple caching** - Single LRU cache instead of complex hierarchy

This simplified architecture provides a focused, maintainable foundation for CQLite with 75% less complexity while maintaining core Cassandra compatibility.