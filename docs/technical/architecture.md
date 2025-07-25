# CQLite Architecture Design

## Overview

CQLite is a simplified, embeddable database engine built in Rust with multi-language bindings. It implements a radically simplified version of Cassandra's storage model with **single SSTable per table** design, CQL parsing, and no compaction complexity. Supports both native and WASM deployments.

## Core Design Principles

1. **Radical Simplicity**: Single SSTable per table, no compaction, no bloom filters
2. **CQL-First**: Native Cassandra Query Language parsing using Antlr4 grammar
3. **Cassandra 5 Compatible**: Mirror Cassandra file directory structure
4. **Cross-Platform**: Native (Linux/macOS/Windows) and WASM support
5. **FFI-First**: Clean C API for language bindings
6. **Embedded-Friendly**: No external dependencies, single binary
7. **Memory-Safe**: Leverage Rust's safety guarantees

## Architecture Layers

```
┌─────────────────────────────────────────────┐
│             Language Bindings               │
│     (Python, NodeJS, Go, C++)              │
├─────────────────────────────────────────────┤
│               C FFI Layer                   │
│          (Safe Abstractions)               │
├─────────────────────────────────────────────┤
│              Query Engine                   │
│    (Parser, Planner, Executor)             │
├─────────────────────────────────────────────┤
│            Schema Management                │
│     (Types, Validation, Evolution)         │
├─────────────────────────────────────────────┤
│             Storage Engine                  │
│   (SSTable, MemTable, WAL, Compaction)     │
├─────────────────────────────────────────────┤
│          Memory Management                  │
│     (Cache, Buffer Pool, Allocators)       │
├─────────────────────────────────────────────┤
│         Platform Abstraction               │
│    (FS, Threading, WASM Compatibility)     │
└─────────────────────────────────────────────┘
```

## Module Structure

### 1. Core Library (`cqlite-core`)

```
src/
├── lib.rs                    # Public API and re-exports
├── error.rs                  # Error types and handling
├── config.rs                 # Configuration management
├── types.rs                  # Core data types
│
├── storage/                  # Storage Engine (Simplified)
│   ├── mod.rs               # Storage module interface
│   ├── sstable/             # Single SSTable per table
│   │   ├── mod.rs
│   │   ├── reader.rs        # SSTable reader
│   │   ├── writer.rs        # SSTable writer
│   │   ├── index.rs         # Simple index management
│   │   └── compression.rs   # Optional compression
│   ├── memtable.rs          # In-memory write buffer
│   ├── wal.rs               # Write-ahead log
│   └── directory.rs         # Cassandra 5 directory layout
│
├── schema/                   # Schema Management
│   ├── mod.rs               # Schema module interface
│   ├── types.rs             # Data type definitions
│   ├── validation.rs        # Schema validation
│   ├── evolution.rs         # Schema evolution
│   └── catalog.rs           # Schema catalog
│
├── query/                    # CQL Query Engine
│   ├── mod.rs               # Query module interface
│   ├── cql_parser.rs        # CQL parser (Antlr4-based)
│   ├── ast.rs               # CQL AST definitions
│   ├── planner.rs           # Simple query planner
│   ├── executor.rs          # Query executor
│   └── expression.rs        # Expression evaluation
│
├── memory/                   # Memory Management
│   ├── mod.rs               # Memory module interface
│   ├── cache.rs             # LRU and adaptive caches
│   ├── buffer_pool.rs       # Buffer pool management
│   ├── allocator.rs         # Custom allocators
│   └── metrics.rs           # Memory usage metrics
│
└── platform/                # Platform Abstraction
    ├── mod.rs               # Platform module interface
    ├── fs.rs                # File system abstraction
    ├── threading.rs         # Threading utilities
    ├── time.rs              # Time utilities
    └── wasm.rs              # WASM-specific implementations
```

### 2. FFI Layer (`cqlite-ffi`)

```
src/
├── lib.rs                    # C API exports
├── types.rs                  # C-compatible types
├── error.rs                  # Error code mappings
├── database.rs               # Database handle management
├── query.rs                  # Query execution API
├── schema.rs                 # Schema management API
├── iterator.rs               # Result iteration API
└── utils.rs                  # Utility functions
```

### 3. WASM Bindings (`cqlite-wasm`)

```
src/
├── lib.rs                    # WASM exports
├── database.rs               # Database interface
├── query.rs                  # Query interface
├── schema.rs                 # Schema interface
├── types.rs                  # JS-compatible types
└── utils.rs                  # WASM utilities
```

## Data Flow Architecture

### Write Path (Simplified)
```
CQL INSERT → CQL Parser → Schema Validator → MemTable → WAL → Single SSTable (when full)
```

### Read Path (Simplified)
```
CQL SELECT → CQL Parser → Simple Planner → Executor → MemTable + Single SSTable → Result Set
```

### No Compaction Path
```
(Eliminated - Single SSTable per table means no compaction needed)
```

## Memory Architecture

### Buffer Management (Simplified)
- **Buffer Pool**: Simple pool for single SSTable blocks
- **MemTable**: Skip-list based in-memory structure  
- **Simple Cache**: Basic block cache only (no complex hierarchy)

### WASM Considerations
- **Memory Limits**: 4GB max, careful allocation strategy
- **Linear Memory**: Single contiguous address space
- **GC Integration**: Proper cleanup for JS integration

## Storage Format

### Simplified SSTable Structure (Cassandra 5 Layout)
```
┌─────────────────┐
│   File Header   │  Magic + Version + Metadata
├─────────────────┤
│   Data Blocks   │  Row data with optional compression
├─────────────────┤
│   Index Block   │  Simple block index entries
├─────────────────┤
│   Footer        │  Checksum + Block pointers
└─────────────────┘

Directory Structure (mirrors Cassandra 5):
/table_name/
  ├── na-1-big-Data.db           # Main data file
  ├── na-1-big-Index.db          # Index file
  ├── na-1-big-Statistics.db     # Statistics
  ├── na-1-big-Summary.db        # Summary
  └── na-1-big-TOC.txt          # Table of contents
```

### Row Format
```
┌──────┬─────────┬──────────┬─────────────┐
│ Key  │ Version │ Metadata │ Column Data │
└──────┴─────────┴──────────┴─────────────┘
```

## API Design

### Core Rust API
```rust
pub struct Database {
    storage: StorageEngine,
    schema: SchemaManager,
    query_engine: QueryEngine,
    config: Config,
}

impl Database {
    pub fn open(path: &Path, config: Config) -> Result<Self>;
    pub fn execute_cql(&self, cql: &str) -> Result<QueryResult>;
    pub fn prepare_cql(&self, cql: &str) -> Result<PreparedStatement>;
    pub fn close(&mut self) -> Result<()>;
}
```

### C FFI API
```c
typedef struct cqlite_db cqlite_db_t;
typedef struct cqlite_result cqlite_result_t;

int cqlite_open(const char* path, cqlite_db_t** db);
int cqlite_execute_cql(cqlite_db_t* db, const char* cql_query, cqlite_result_t** result);
int cqlite_close(cqlite_db_t* db);
void cqlite_result_free(cqlite_result_t* result);
```

### WASM API
```javascript
class CQLiteDB {
    constructor(config)
    async open(path)
    async executeCQL(cqlQuery)
    async prepareCQL(cqlQuery)
    close()
}
```

## Performance Targets (Simplified Architecture)

- **Insert Throughput**: 50K ops/sec (single thread, no compaction overhead)
- **Query Latency**: <2ms for simple lookups (no bloom filter complexity)
- **Memory Usage**: <50MB for 1M rows (single SSTable)
- **Startup Time**: <50ms cold start (simplified startup)
- **WASM Size**: <1MB compressed (reduced complexity)

## CQL Parser Implementation

### Antlr4 Grammar Integration
- **Reference**: Patrick's Cassandra Antlr4 grammar (https://github.com/pmcfadin/cassandra-antlr4-grammar)
- **Implementation**: Rust-native parser based on CQL.g4 grammar
- **AST Structure**: Direct mapping from CQL grammar to Rust enums/structs

### Supported CQL Features (Initial)
- **DDL**: CREATE/ALTER/DROP TABLE, CREATE/DROP KEYSPACE
- **DML**: SELECT, INSERT, UPDATE, DELETE
- **Data Types**: TEXT, INT, BIGINT, UUID, TIMESTAMP, BLOB
- **Operators**: Basic comparison, IN, LIKE (simplified)
- **Functions**: Basic aggregates (COUNT, SUM, AVG)

### CQL Parser Architecture
```rust
pub enum CQLStatement {
    Select(SelectStatement),
    Insert(InsertStatement), 
    Update(UpdateStatement),
    Delete(DeleteStatement),
    CreateTable(CreateTableStatement),
    CreateKeyspace(CreateKeyspaceStatement),
    // ... other statements
}

pub struct CQLParser {
    lexer: CQLLexer,
}

impl CQLParser {
    pub fn parse(&mut self, cql: &str) -> Result<CQLStatement, ParseError>;
    pub fn parse_expression(&mut self, expr: &str) -> Result<Expression, ParseError>;
}
```

## Error Handling Strategy

### Error Types
1. **Storage Errors**: I/O, corruption, space
2. **Schema Errors**: Type mismatches, constraints
3. **Query Errors**: Syntax, semantic validation
4. **System Errors**: OOM, threading, platform

### Error Propagation
- **Rust**: Result types with custom error enums
- **C FFI**: Integer error codes with detailed messages
- **WASM**: JavaScript Error objects with stack traces

## Thread Safety

### Concurrent Access
- **Reads**: Multiple concurrent readers
- **Writes**: Single writer with reader isolation
- **Background Tasks**: Separate threads for compaction, metrics

### Synchronization
- **RwLock**: For schema and metadata
- **Arc**: For shared immutable data
- **Channels**: For background task communication

## Testing Strategy

### Unit Tests
- Per-module comprehensive coverage
- Property-based testing for storage formats
- Fuzzing for parser and serialization

### Integration Tests
- Cross-platform compatibility
- FFI boundary testing
- WASM runtime validation

### Performance Tests
- Benchmarks for all operations
- Memory usage profiling
- Concurrency stress testing

## Security Considerations

### Memory Safety
- No unsafe code in public API
- Careful FFI boundary validation
- Buffer overflow prevention

### Input Validation
- SQL injection prevention
- Schema constraint enforcement
- File path sanitization

## Deployment Configurations

### Native Binary
- Single executable with static linking
- Optional dynamic linking for system libraries
- Cross-compilation support

### WASM Module
- ES6 module exports
- Web Workers compatibility
- Node.js integration

### Language Bindings
- Python wheel packages
- npm packages for Node.js
- Go module distribution