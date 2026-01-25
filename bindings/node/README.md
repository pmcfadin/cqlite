# @cqlite/node

Node.js bindings for [CQLite](https://github.com/pmcfadin/cqlite) - a high-performance library for reading Apache Cassandra 5.0 SSTable files locally, without requiring a running Cassandra cluster.

## Installation

```bash
npm install @cqlite/node
```

## Quick Start

```typescript
import { Database } from '@cqlite/node';

// Open a database with schema
const db = await Database.open('path/to/sstables', { schema: 'schema.cql' });

// Execute queries
const result = await db.execute('SELECT * FROM keyspace.table LIMIT 10');
for (const row of result.rows) {
  console.log(row.name);
}

await db.close();
```

## Features

- **Zero cluster dependency** - Read SSTable files directly from disk
- **Full CQL type support** - All primitive types, collections, UDTs, and frozen types
- **Native JavaScript types** - BigInt, Date, Buffer, Set, Map via `executeNative()`
- **Memory-efficient streaming** - Configure buffer sizes for large datasets
- **Thread-safe** - Safe concurrent access from multiple workers
- **Cross-platform** - Linux (x86_64, ARM64), macOS (Intel, Apple Silicon), Windows

## Supported Platforms

| Platform | Architecture | Status |
|----------|--------------|--------|
| Linux | x86_64 | ✅ |
| Linux | ARM64 | ✅ |
| macOS | Intel (x86_64) | ✅ |
| macOS | Apple Silicon | ✅ |
| Windows | x64 | ✅ |

## Requirements

- Node.js 18+
- Cassandra 5.0 SSTable files

## API Reference

### Opening a Database

```typescript
import { Database } from '@cqlite/node';

// With schema file
const db = await Database.open('/path/to/sstables', {
  schema: '/path/to/schema.cql',
});

// Always close when done
await db.close();
```

The `close()` method is idempotent - safe to call multiple times.

### Executing Queries

```typescript
// Simple query - returns JSON-serializable values
const result = await db.execute('SELECT * FROM keyspace.table');
for (const row of result.rows) {
  console.log(row);
}

// With LIMIT
const limited = await db.execute('SELECT name, age FROM users LIMIT 100');

// Access query metadata
console.log(`Rows returned: ${result.rowCount}`);
console.log(`Execution time: ${result.executionTimeMs}ms`);
console.log(`Columns: ${result.columns.map(c => c.name).join(', ')}`);
```

### Native Types with executeNative()

Use `executeNative()` to get native JavaScript types instead of JSON-serializable values:

```typescript
const result = await db.executeNative('SELECT * FROM keyspace.table');
for (const row of result.rows) {
  // BigInt for CQL bigint/varint
  const balance: bigint = row.balance;

  // Date for CQL timestamp
  const created: Date = row.created;

  // Buffer for CQL blob
  const data: Buffer = row.blob_data;

  // Set for CQL set
  const tags: Set<string> = row.tags;

  // Map for CQL map
  const metadata: Map<string, string> = row.metadata;
}
```

### Column Metadata

Each query result includes column information:

```typescript
const result = await db.execute('SELECT * FROM keyspace.table');

for (const col of result.columns) {
  console.log(`${col.name}: ${col.dataType}`);
  console.log(`  nullable: ${col.nullable}`);
  console.log(`  position: ${col.position}`);
}
```

### Database Statistics

```typescript
const stats = await db.getStats();
console.log(`SSTables: ${stats.totalSstables}`);
console.log(`Total rows: ${stats.totalRows}`);
console.log(`Memory: ${stats.memoryUsedBytes} bytes`);
```

### Error Handling

All errors include structured metadata for programmatic handling:

```typescript
import { Database } from '@cqlite/node';

try {
  const db = await Database.open('/path/to/data');
  const result = await db.execute('SELECT * FROM keyspace.table');
} catch (e) {
  // Error code for programmatic handling
  console.log(`Code: ${e.code}`);        // 'IO', 'SCHEMA', 'QUERY', 'PARSE', etc.

  // Error category
  console.log(`Category: ${e.category}`); // 'System', 'Schema', 'Query', etc.

  // Whether the operation can be retried
  console.log(`Recoverable: ${e.isRecoverable}`);

  // Original error message
  console.log(`Message: ${e.message}`);
}
```

**Error Codes:**

| Code | Category | Description | Recoverable |
|------|----------|-------------|-------------|
| `IO` | System | File system errors | Yes |
| `SCHEMA` | Schema | Schema parsing/validation | No |
| `QUERY` | Query | Query execution failures | No |
| `PARSE` | Data | CQL syntax errors | No |
| `CONFIG` | Configuration | Invalid configuration | No |
| `STORAGE` | Storage | Storage engine errors | No |
| `NOT_FOUND` | NotFound | Table/resource not found | No |
| `INVALID_INPUT` | Logic | Invalid operation (e.g., closed db) | No |

## Type Conversions

CQL types are automatically converted to JavaScript types:

| CQL Type | JavaScript Type | Notes |
|----------|-----------------|-------|
| `text`, `varchar`, `ascii` | `string` | |
| `int`, `smallint`, `tinyint` | `number` | |
| `bigint`, `varint`, `counter` | `bigint` | via `executeNative()` |
| `float`, `double` | `number` | |
| `decimal` | `string` | Preserves precision |
| `boolean` | `boolean` | |
| `blob` | `Buffer` | via `executeNative()` |
| `timestamp` | `Date` | via `executeNative()` |
| `date` | `Date` | via `executeNative()` |
| `time` | `bigint` | Nanoseconds since midnight, via `executeNative()` |
| `duration` | `object` | `{ months, days, nanos }` |
| `uuid`, `timeuuid` | `string` | Lowercase formatted |
| `inet` | `string` | IP address string |
| `list<T>` | `T[]` | |
| `set<T>` | `Set<T>` | via `executeNative()` |
| `map<K,V>` | `Map<K,V>` | via `executeNative()` |
| `tuple<...>` | `[...]` | Array |
| `frozen<T>` | Inner type | Unwrapped |
| UDT | `object` | With `_type` and `_keyspace` fields |

## Examples

See the [examples/](examples/) directory for complete working examples:

- [basic-query.ts](examples/basic-query.ts) - Simple SELECT queries
- [type-handling.ts](examples/type-handling.ts) - Working with CQL types
- [error-handling.ts](examples/error-handling.ts) - Error handling patterns
- [streaming.ts](examples/streaming.ts) - Large result handling
- [performance.ts](examples/performance.ts) - Memory-optimized usage

## Resources

- [TypeScript Definitions](lib/index.d.ts) - Complete API type hints
- [Main Project README](../../README.md) - CQLite project overview
- [Issue Tracker](https://github.com/pmcfadin/cqlite/issues) - Report bugs or request features

## License

MIT OR Apache-2.0

## Links

- [GitHub Repository](https://github.com/pmcfadin/cqlite)
- [npm Package](https://www.npmjs.com/package/@cqlite/node)
- [Documentation](https://github.com/pmcfadin/cqlite/tree/main/docs)
