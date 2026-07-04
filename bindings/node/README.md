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

// Execute queries (executeNative() returns native JS types with full precision)
const result = await db.executeNative('SELECT * FROM keyspace.table LIMIT 10');
for (const row of result.rows) {
  console.log(row.name);
}

await db.close();
```

> **⚠️ Warning: `execute()` is deprecated and will be removed in the next major.**
> Prefer `executeNative()`. `execute()` returns **lossy** legacy JSON encodings:
> - `blob` → base64 **string** (not a `Buffer`)
> - `timestamp` → ISO-8601 **string** (not a `Date`)
> - `varint` → `"0x{hex}"` **string**
> - `decimal` → `"decimal:{scale}:0x{hex}"` **string**
> - `date`/`time` → **number** (days-since-epoch / nanoseconds-since-midnight)
>
> It is also slower (JSON off-loop, then JS on-loop — a double conversion).
> Calling `execute()` emits a one-time `DeprecationWarning`. Use `executeNative()`
> for native types (`BigInt`, `Buffer`, `Date`, `Set`, `Map`) with full fidelity.
> (`bigint`/`counter` currently come back as an exact `BigInt` on this napi build,
> so they are not presently rounded — but `execute()` is unsupported regardless.)

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
// Simple query - executeNative() returns native JS types with full precision
const result = await db.executeNative('SELECT * FROM keyspace.table');
for (const row of result.rows) {
  console.log(row);
}

// With LIMIT
const limited = await db.executeNative('SELECT name, age FROM users LIMIT 100');

// Access query metadata
console.log(`Rows returned: ${result.rowCount}`);
console.log(`Execution time: ${result.executionTimeMs}ms`);
console.log(`Columns: ${result.columns.map(c => c.name).join(', ')}`);
```

> Prefer `executeNative()` over the deprecated `execute()` — see the warning at
> the top of this README for the precision/encoding hazards of `execute()`.

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

### JSON Encoding (deprecated `execute()` method)

> **⚠️ Deprecated — removed in the next major.** `execute()` returns lossy legacy
> JSON encodings and is slower than `executeNative()`. In particular blob comes
> back as a base64 string, timestamp as an ISO-8601 string, and varint/decimal
> as bespoke non-round-trippable strings. This section documents the encoding for
> the few callers that still depend on it; new code should use `executeNative()`.

The `execute()` method returns JSON-serializable values. For most types this works intuitively,
but `varint` and `decimal` types use a hex-based encoding to preserve arbitrary precision:

```typescript
// Using execute() - hex encoding (deprecated)
const result = await db.execute('SELECT amount FROM transactions');
console.log(result.rows[0].amount);
// Varint: "0x7f" (127), "0xff" (-1), "0x0100" (256)
// Decimal: "decimal:2:0x7b" (1.23), "decimal:2:0xee29" (-45.67)

// Using executeNative() - proper types (recommended)
const native = await db.executeNative('SELECT amount FROM transactions');
console.log(native.rows[0].amount);
// Varint: 127n (BigInt)
// Decimal: "1.23" (human-readable string)
```

**Hex Format Details:**
- `varint`: `"0x{hex}"` - Two's complement big-endian hex encoding
- `decimal`: `"decimal:{scale}:0x{hex}"` - Scale (decimal places) + hex-encoded unscaled value

**Recommendation:** Use `executeNative()`. The `execute()` method is deprecated
(removed in the next major); its JSON encoding is lossy (blob/timestamp/varint/
decimal come back as bespoke strings) and it is slower than `executeNative()`.

### Column Metadata

Each query result includes column information:

```typescript
const result = await db.executeNative('SELECT * FROM keyspace.table');

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
  const result = await db.executeNative('SELECT * FROM keyspace.table');
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
| `bigint`, `varint`*, `counter` | `bigint` | via `executeNative()` |
| `float`, `double` | `number` | |
| `decimal`* | `string` | Preserves precision |
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

\* **Note:** With `execute()`, `varint` returns `"0x{hex}"` and `decimal` returns `"decimal:{scale}:0x{hex}"`.
Use `executeNative()` for human-readable formats.

## Write Operations

CQLite v0.9.0 adds write support to the Node.js bindings. Open the database with
`writable: true` and a `writeDir` to enable write operations.

```javascript
const { Database } = require('@cqlite/node');

const db = await Database.open('path/to/sstables', {
  schema: 'schema.cql',
  writable: true,
  writeDir: '/tmp/my-writes',
});

// Write rows via CQL INSERT, UPDATE, or DELETE
await db.executeNative(
  "INSERT INTO test_basic.simple_table (id, name, age) " +
  "VALUES (22222222-2222-2222-2222-222222222222, 'Bob', 25)"
);
await db.executeNative(
  "UPDATE test_basic.simple_table SET age = 26 " +
  "WHERE id = 22222222-2222-2222-2222-222222222222"
);

// Flush the in-memory write buffer (memtable) to an SSTable on disk.
// Returns the path to the flushed Data.db file, or "" if memtable was empty.
const path = await db.flushRun();
console.log('Flushed to:', path);

// Run background compaction within a time budget
const report = await db.maintenanceStep({ budgetMs: 100 });
console.log(`Merged ${report.rowsMerged} rows in ${report.timeSpentMs}ms`);
if (report.pendingCompaction) {
  console.log('More compaction work available');
}

// Inspect write statistics (synchronous getter)
const stats = db.writeStats;
console.log('Memtable size:', stats.memtableSizeBytes, 'bytes');
console.log('Total flushed:', stats.totalWrittenBytes, 'bytes');

await db.close();
```

### Write API

| Method / Property | Description |
|-------------------|-------------|
| `db.executeNative(cql)` | Execute a CQL INSERT, UPDATE, or DELETE statement (recommended) |
| `db.execute(cql)` | **Deprecated** (removed next major; emits a `DeprecationWarning`). Same DML behavior as `executeNative()`, but lossy for SELECT — use `executeNative()` |
| `db.flushRun()` | Flush memtable to SSTable; returns the Data.db path or `""` if memtable was empty |
| `db.maintenanceStep(options?)` | Run STCS compaction for up to `options.budgetMs` ms (default: 100); returns `MaintenanceReport` |
| `db.writeStats` | Synchronous getter: `memtableSizeBytes`, `memtableRowCount`, `totalWrittenBytes`, `l0SstableCount` |

### Known Limitations

- Counter columns cannot be written — `execute()` throws `CqliteError` for
  counter mutations.
- BTI-format index files are not produced; the writer emits BIG format.

See [docs/write-support-limitations.md](../../docs/write-support-limitations.md)
for the full limitations reference.

## Examples

See the [examples/](examples/) directory for complete working examples:

- [basic-query.ts](examples/basic-query.ts) - Simple SELECT queries
- [type-handling.ts](examples/type-handling.ts) - Working with CQL types
- [error-handling.ts](examples/error-handling.ts) - Error handling patterns
- [streaming.ts](examples/streaming.ts) - Large result handling
- [performance.ts](examples/performance.ts) - Memory-optimized usage

> **Streaming concurrency caveat:** `executeStreaming()` fetches each batch of
> `K = bufferSize` rows on a libuv threadpool thread, so N concurrent streams can
> each occupy a libuv threadpool thread for the duration of a batch fetch. Heavy
> concurrent `fs`/`crypto` work in the same process may see added latency until
> the follow-up ([#1901](https://github.com/pmcfadin/cqlite/issues/1901)) moves
> streaming off the libuv pool onto the tokio runtime.

## Resources

- [TypeScript Definitions](lib/index.d.ts) - Complete API type hints
- [Main Project README](../../README.md) - CQLite project overview
- [Write Support Guide](../../docs/write-support.md) - Detailed write documentation
- [Issue Tracker](https://github.com/pmcfadin/cqlite/issues) - Report bugs or request features

## License

MIT OR Apache-2.0

## Links

- [GitHub Repository](https://github.com/pmcfadin/cqlite)
- [npm Package](https://www.npmjs.com/package/@cqlite/node)
- [Documentation](https://github.com/pmcfadin/cqlite/tree/main/docs)
