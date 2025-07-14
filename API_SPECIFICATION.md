# CQLite API Specification

## Overview

CQLite provides multiple API layers for different use cases:
- **Native Rust API**: Core library interface
- **C API**: FFI layer for language bindings
- **Python API**: Pythonic interface with type hints
- **NodeJS API**: Modern JavaScript with TypeScript support
- **WASM API**: Browser-compatible interface

---

## Rust Native API

### Core Database Interface

```rust
use cqlite::{Database, Config, Result, Error};
use std::path::Path;

// Database connection and management
pub struct Database {
    // Internal state
}

impl Database {
    /// Open a database at the specified path
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self>;
    
    /// Open with custom configuration
    pub fn open_with_config<P: AsRef<Path>>(path: P, config: Config) -> Result<Self>;
    
    /// Close the database (automatic on drop)
    pub fn close(self) -> Result<()>;
    
    /// Get database statistics
    pub fn stats(&self) -> DatabaseStats;
}

// Configuration options
#[derive(Debug, Clone)]
pub struct Config {
    pub cache_size: usize,           // Block cache size in bytes
    pub bloom_filter_bits: u32,      // Bits per bloom filter entry
    pub compression: Compression,     // Compression algorithm
    pub read_only: bool,             // Read-only mode
    pub verify_checksums: bool,      // Enable checksum verification
}

impl Default for Config {
    fn default() -> Self {
        Self {
            cache_size: 64 * 1024 * 1024,  // 64MB
            bloom_filter_bits: 10,
            compression: Compression::Lz4,
            read_only: false,
            verify_checksums: true,
        }
    }
}
```

### Schema Management

```rust
use cqlite::schema::{Schema, Table, Column, DataType};

// Schema definition and management
pub struct Schema {
    pub keyspace: String,
    pub tables: Vec<Table>,
}

pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub partition_keys: Vec<String>,
    pub clustering_keys: Vec<String>,
}

pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    // Primitive types
    Boolean,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Float,
    Double,
    Decimal,
    Ascii,
    Text,
    Varchar,
    Blob,
    Uuid,
    TimeUuid,
    Timestamp,
    Date,
    Time,
    Duration,
    Inet,
    
    // Collection types
    List(Box<DataType>),
    Set(Box<DataType>),
    Map(Box<DataType>, Box<DataType>),
    
    // Complex types
    Tuple(Vec<DataType>),
    Udt(String),
    Frozen(Box<DataType>),
}

impl Database {
    /// Create a new table with schema
    pub fn create_table(&mut self, table: &Table) -> Result<()>;
    
    /// Get table schema
    pub fn get_schema(&self, table_name: &str) -> Result<Schema>;
    
    /// List all tables
    pub fn list_tables(&self) -> Result<Vec<String>>;
}
```

### Data Operations

```rust
use cqlite::data::{Row, Value, QueryResult};

// Data manipulation
impl Database {
    /// Insert a row into a table
    pub fn insert(&mut self, table: &str, row: Row) -> Result<()>;
    
    /// Insert multiple rows efficiently
    pub fn insert_batch(&mut self, table: &str, rows: Vec<Row>) -> Result<()>;
    
    /// Execute a SELECT query
    pub fn select(&self, query: &str) -> Result<QueryResult>;
    
    /// Execute a SELECT query with parameters
    pub fn select_with_params(&self, query: &str, params: &[Value]) -> Result<QueryResult>;
    
    /// Get a specific row by partition key
    pub fn get(&self, table: &str, partition_key: &[Value]) -> Result<Option<Row>>;
    
    /// Get rows in a partition with clustering key range
    pub fn get_range(&self, table: &str, partition_key: &[Value], 
                     start: Option<&[Value]>, end: Option<&[Value]>) -> Result<QueryResult>;
}

// Row representation
pub struct Row {
    pub columns: HashMap<String, Value>,
}

// CQL value types
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Boolean(bool),
    TinyInt(i8),
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    Float(f32),
    Double(f64),
    Decimal(BigDecimal),
    Text(String),
    Blob(Vec<u8>),
    Uuid(uuid::Uuid),
    Timestamp(chrono::DateTime<chrono::Utc>),
    List(Vec<Value>),
    Set(HashSet<Value>),
    Map(HashMap<Value, Value>),
    Tuple(Vec<Value>),
    Udt(HashMap<String, Value>),
}

// Query results
pub struct QueryResult {
    pub rows: Vec<Row>,
    pub schema: Schema,
    pub count: usize,
}

impl QueryResult {
    /// Iterate over rows
    pub fn iter(&self) -> impl Iterator<Item = &Row>;
    
    /// Convert to JSON
    pub fn to_json(&self) -> Result<String>;
    
    /// Export to CSV
    pub fn to_csv(&self) -> Result<String>;
}
```

### Async API

```rust
use tokio;

// Async operations for better performance
impl Database {
    /// Async version of select
    pub async fn select_async(&self, query: &str) -> Result<QueryResult>;
    
    /// Async batch insert
    pub async fn insert_batch_async(&mut self, table: &str, rows: Vec<Row>) -> Result<()>;
    
    /// Stream large result sets
    pub fn select_stream(&self, query: &str) -> impl Stream<Item = Result<Row>>;
}
```

---

## C API (FFI Layer)

### Core Functions

```c
#include <stdint.h>
#include <stdbool.h>

// Opaque types
typedef struct cqlite_database cqlite_database_t;
typedef struct cqlite_result cqlite_result_t;
typedef struct cqlite_row cqlite_row_t;
typedef struct cqlite_schema cqlite_schema_t;

// Error codes
typedef enum {
    CQLITE_OK = 0,
    CQLITE_ERROR = 1,
    CQLITE_INVALID_SCHEMA = 2,
    CQLITE_IO_ERROR = 3,
    CQLITE_CORRUPTION = 4,
    CQLITE_NOT_FOUND = 5,
    CQLITE_INVALID_QUERY = 6,
    CQLITE_MEMORY_ERROR = 7,
} cqlite_error_t;

// Database operations
cqlite_error_t cqlite_open(const char* path, cqlite_database_t** db);
cqlite_error_t cqlite_open_with_config(const char* path, const char* config_json, cqlite_database_t** db);
cqlite_error_t cqlite_close(cqlite_database_t* db);

// Schema operations
cqlite_error_t cqlite_create_table(cqlite_database_t* db, const char* schema_json);
cqlite_error_t cqlite_get_schema(cqlite_database_t* db, const char* table_name, cqlite_schema_t** schema);
cqlite_error_t cqlite_list_tables(cqlite_database_t* db, char*** tables, size_t* count);

// Data operations
cqlite_error_t cqlite_insert(cqlite_database_t* db, const char* table, const char* row_json);
cqlite_error_t cqlite_insert_batch(cqlite_database_t* db, const char* table, const char* rows_json);
cqlite_error_t cqlite_select(cqlite_database_t* db, const char* query, cqlite_result_t** result);
cqlite_error_t cqlite_select_with_params(cqlite_database_t* db, const char* query, 
                                        const char* params_json, cqlite_result_t** result);

// Result handling
size_t cqlite_result_row_count(cqlite_result_t* result);
cqlite_error_t cqlite_result_get_row(cqlite_result_t* result, size_t index, cqlite_row_t** row);
cqlite_error_t cqlite_result_to_json(cqlite_result_t* result, char** json);
void cqlite_result_free(cqlite_result_t* result);

// Row handling
cqlite_error_t cqlite_row_get_column(cqlite_row_t* row, const char* column, char** value_json);
void cqlite_row_free(cqlite_row_t* row);

// Memory management
void cqlite_free_string(char* str);
void cqlite_free_string_array(char** array, size_t count);

// Error handling
const char* cqlite_error_message(cqlite_error_t error);
```

---

## Python API

### Installation and Basic Usage

```python
import cqlite
from typing import List, Dict, Any, Optional
import asyncio

# Database connection
class Database:
    def __init__(self, path: str, config: Optional[Dict[str, Any]] = None):
        """Open a CQLite database at the specified path."""
        pass
    
    def __enter__(self) -> 'Database':
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def close(self) -> None:
        """Close the database connection."""
        pass
    
    @property
    def stats(self) -> Dict[str, Any]:
        """Get database statistics."""
        pass

# Schema management
    def create_table(self, schema: Dict[str, Any]) -> None:
        """Create a new table with the given schema."""
        pass
    
    def get_schema(self, table_name: str) -> Dict[str, Any]:
        """Get the schema for a table."""
        pass
    
    def list_tables(self) -> List[str]:
        """List all tables in the database."""
        pass

# Data operations
    def insert(self, table: str, row: Dict[str, Any]) -> None:
        """Insert a single row into a table."""
        pass
    
    def insert_batch(self, table: str, rows: List[Dict[str, Any]]) -> None:
        """Insert multiple rows efficiently."""
        pass
    
    def select(self, query: str, params: Optional[List[Any]] = None) -> 'QueryResult':
        """Execute a SELECT query and return results."""
        pass
    
    def get(self, table: str, partition_key: List[Any]) -> Optional[Dict[str, Any]]:
        """Get a specific row by partition key."""
        pass
    
    def get_range(self, table: str, partition_key: List[Any], 
                  start: Optional[List[Any]] = None, 
                  end: Optional[List[Any]] = None) -> 'QueryResult':
        """Get rows in a partition with clustering key range."""
        pass

# Async operations
    async def select_async(self, query: str, params: Optional[List[Any]] = None) -> 'QueryResult':
        """Async version of select."""
        pass
    
    async def insert_batch_async(self, table: str, rows: List[Dict[str, Any]]) -> None:
        """Async batch insert."""
        pass
    
    def select_stream(self, query: str, params: Optional[List[Any]] = None):
        """Stream large result sets."""
        pass

# Query results
class QueryResult:
    @property
    def rows(self) -> List[Dict[str, Any]]:
        """Get all rows as list of dictionaries."""
        pass
    
    @property
    def count(self) -> int:
        """Get number of rows."""
        pass
    
    def __iter__(self):
        """Iterate over rows."""
        pass
    
    def __len__(self) -> int:
        return self.count
    
    def to_pandas(self) -> 'pandas.DataFrame':
        """Convert to pandas DataFrame (optional dependency)."""
        pass
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        pass
    
    def to_csv(self) -> str:
        """Convert to CSV string."""
        pass

# Example usage
def main():
    with cqlite.Database("/path/to/data") as db:
        # Create table
        schema = {
            "name": "users",
            "columns": [
                {"name": "user_id", "type": "uuid", "partition_key": True},
                {"name": "name", "type": "text"},
                {"name": "email", "type": "text"},
                {"name": "created_at", "type": "timestamp"}
            ]
        }
        db.create_table(schema)
        
        # Insert data
        db.insert("users", {
            "user_id": "550e8400-e29b-41d4-a716-446655440000",
            "name": "John Doe",
            "email": "john@example.com",
            "created_at": "2024-01-01T00:00:00Z"
        })
        
        # Query data
        results = db.select("SELECT * FROM users WHERE name = ?", ["John Doe"])
        for row in results:
            print(f"User: {row['name']} ({row['email']})")

if __name__ == "__main__":
    main()
```

---

## NodeJS API

### Installation and Basic Usage

```typescript
import { Database, QueryResult, Schema, Config } from 'cqlite';

// Database interface
export class Database {
    constructor(path: string, config?: Config);
    
    // Schema operations
    async createTable(schema: Schema): Promise<void>;
    async getSchema(tableName: string): Promise<Schema>;
    async listTables(): Promise<string[]>;
    
    // Data operations
    async insert(table: string, row: Record<string, any>): Promise<void>;
    async insertBatch(table: string, rows: Record<string, any>[]): Promise<void>;
    async select(query: string, params?: any[]): Promise<QueryResult>;
    async get(table: string, partitionKey: any[]): Promise<Record<string, any> | null>;
    async getRange(table: string, partitionKey: any[], 
                   start?: any[], end?: any[]): Promise<QueryResult>;
    
    // Streaming
    selectStream(query: string, params?: any[]): AsyncIterable<Record<string, any>>;
    
    // Management
    async close(): Promise<void>;
    get stats(): DatabaseStats;
}

// Configuration interface
export interface Config {
    cacheSize?: number;
    bloomFilterBits?: number;
    compression?: 'lz4' | 'snappy' | 'deflate' | 'none';
    readOnly?: boolean;
    verifyChecksums?: boolean;
}

// Schema definitions
export interface Schema {
    name: string;
    columns: Column[];
    partitionKeys: string[];
    clusteringKeys: string[];
}

export interface Column {
    name: string;
    type: DataType;
    nullable?: boolean;
}

export type DataType = 
    | 'boolean' | 'tinyint' | 'smallint' | 'int' | 'bigint'
    | 'float' | 'double' | 'decimal' | 'ascii' | 'text' | 'varchar'
    | 'blob' | 'uuid' | 'timeuuid' | 'timestamp' | 'date' | 'time'
    | 'duration' | 'inet'
    | { list: DataType }
    | { set: DataType }
    | { map: [DataType, DataType] }
    | { tuple: DataType[] }
    | { udt: string }
    | { frozen: DataType };

// Query results
export class QueryResult {
    readonly rows: Record<string, any>[];
    readonly count: number;
    readonly schema: Schema;
    
    [Symbol.iterator](): Iterator<Record<string, any>>;
    
    toJSON(): string;
    toCSV(): string;
}

// Database statistics
export interface DatabaseStats {
    tableCount: number;
    totalRows: number;
    totalSize: number;
    cacheHitRate: number;
    compressionRatio: number;
}

// Example usage
async function main() {
    const db = new Database('/path/to/data', {
        cacheSize: 64 * 1024 * 1024, // 64MB
        compression: 'lz4'
    });
    
    try {
        // Create table
        await db.createTable({
            name: 'users',
            columns: [
                { name: 'user_id', type: 'uuid' },
                { name: 'name', type: 'text' },
                { name: 'email', type: 'text' },
                { name: 'created_at', type: 'timestamp' }
            ],
            partitionKeys: ['user_id'],
            clusteringKeys: []
        });
        
        // Insert data
        await db.insert('users', {
            user_id: '550e8400-e29b-41d4-a716-446655440000',
            name: 'John Doe',
            email: 'john@example.com',
            created_at: new Date()
        });
        
        // Query data
        const results = await db.select('SELECT * FROM users WHERE name = ?', ['John Doe']);
        for (const row of results) {
            console.log(`User: ${row.name} (${row.email})`);
        }
        
        // Stream large results
        for await (const row of db.selectStream('SELECT * FROM large_table')) {
            console.log(row);
        }
        
    } finally {
        await db.close();
    }
}

main().catch(console.error);
```

---

## WASM API

### Browser Integration

```javascript
import init, { Database } from 'cqlite-wasm';

// Initialize WASM module
await init();

// Database interface optimized for browser constraints
class Database {
    constructor(name, config = {}) {
        // Uses IndexedDB for persistence
        this.name = name;
        this.config = {
            maxMemory: 50 * 1024 * 1024, // 50MB default
            cacheSize: 10 * 1024 * 1024, // 10MB cache
            useWebWorker: false,         // Optional Web Worker
            ...config
        };
    }
    
    async open() {
        // Initialize IndexedDB storage
    }
    
    async close() {
        // Clean up resources
    }
    
    // Schema operations (same interface as NodeJS)
    async createTable(schema) { /* ... */ }
    async getSchema(tableName) { /* ... */ }
    async listTables() { /* ... */ }
    
    // Data operations with memory awareness
    async insert(table, row) { /* ... */ }
    async insertBatch(table, rows, batchSize = 1000) { /* ... */ }
    async select(query, params) { /* ... */ }
    
    // Streaming with pagination for memory efficiency
    async* selectStream(query, params, pageSize = 100) {
        // Yield rows in batches to manage memory
    }
    
    // Browser-specific utilities
    async exportToFile(table, format = 'json') {
        // Export data as downloadable file
    }
    
    async importFromFile(file, table) {
        // Import from File API
    }
    
    // Memory management
    get memoryUsage() {
        return {
            used: /* current usage */,
            limit: this.config.maxMemory,
            percentage: /* used/limit * 100 */
        };
    }
    
    async gc() {
        // Force garbage collection
    }
}

// Example browser usage
async function main() {
    const db = new Database('my-app-data', {
        maxMemory: 100 * 1024 * 1024, // 100MB
        useWebWorker: true
    });
    
    await db.open();
    
    try {
        // Create table
        await db.createTable({
            name: 'events',
            columns: [
                { name: 'id', type: 'uuid' },
                { name: 'timestamp', type: 'timestamp' },
                { name: 'data', type: 'text' }
            ],
            partitionKeys: ['id'],
            clusteringKeys: ['timestamp']
        });
        
        // Insert events
        const events = Array.from({length: 1000}, (_, i) => ({
            id: crypto.randomUUID(),
            timestamp: new Date(),
            data: `Event ${i}`
        }));
        
        await db.insertBatch('events', events);
        
        // Query with streaming for large results
        const results = [];
        for await (const row of db.selectStream('SELECT * FROM events ORDER BY timestamp')) {
            results.push(row);
            
            // Check memory usage
            if (db.memoryUsage.percentage > 80) {
                console.warn('High memory usage, consider reducing query scope');
                break;
            }
        }
        
        console.log(`Processed ${results.length} events`);
        
    } finally {
        await db.close();
    }
}

// Web Worker integration (optional)
if (typeof WorkerGlobalScope !== 'undefined') {
    // Running in Web Worker
    self.onmessage = async function(e) {
        const { operation, data } = e.data;
        
        try {
            const result = await handleDatabaseOperation(operation, data);
            self.postMessage({ success: true, result });
        } catch (error) {
            self.postMessage({ success: false, error: error.message });
        }
    };
}
```

---

## Error Handling

### Common Error Types

```rust
// Rust error types
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Invalid schema: {0}")]
    InvalidSchema(String),
    
    #[error("Corruption detected: {0}")]
    Corruption(String),
    
    #[error("Query error: {0}")]
    Query(String),
    
    #[error("Not found: {0}")]
    NotFound(String),
    
    #[error("Type error: expected {expected}, got {actual}")]
    Type { expected: String, actual: String },
    
    #[error("Memory limit exceeded: {limit} bytes")]
    MemoryLimit { limit: usize },
}

// Result type alias
pub type Result<T> = std::result::Result<T, Error>;
```

```python
# Python exceptions
class CQLiteError(Exception):
    """Base exception for all CQLite errors."""
    pass

class SchemaError(CQLiteError):
    """Schema-related errors."""
    pass

class QueryError(CQLiteError):
    """Query parsing or execution errors."""
    pass

class CorruptionError(CQLiteError):
    """Data corruption detected."""
    pass

class NotFoundError(CQLiteError):
    """Requested data not found."""
    pass

class MemoryError(CQLiteError):
    """Memory limit exceeded."""
    pass
```

```typescript
// TypeScript error classes
export class CQLiteError extends Error {
    constructor(message: string, public code: string) {
        super(message);
        this.name = 'CQLiteError';
    }
}

export class SchemaError extends CQLiteError {
    constructor(message: string) {
        super(message, 'SCHEMA_ERROR');
        this.name = 'SchemaError';
    }
}

export class QueryError extends CQLiteError {
    constructor(message: string) {
        super(message, 'QUERY_ERROR');
        this.name = 'QueryError';
    }
}

export class CorruptionError extends CQLiteError {
    constructor(message: string) {
        super(message, 'CORRUPTION_ERROR');
        this.name = 'CorruptionError';
    }
}
```

This comprehensive API specification provides type-safe, ergonomic interfaces across all target platforms while maintaining consistent functionality and error handling patterns.