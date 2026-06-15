---
title: Node.js Bindings
description: Install and use the @cqlite/node package to query Cassandra 5.0 SSTables from JavaScript and TypeScript.
sidebar:
  label: Node.js Bindings
  order: 6
---

The `@cqlite/node` package provides direct access to Cassandra 5.0 SSTable files
from JavaScript and TypeScript, without requiring a running Cassandra cluster.
It ships pre-built native binaries for all major platforms.

## Installation

```bash
npm install @cqlite/node
```

Requires Node.js 18+. Pre-built binaries are available for Linux (x86_64,
ARM64), macOS (Intel and Apple Silicon), and Windows (x64).

## Quick start

```javascript
const { Database } = require('@cqlite/node');

(async () => {
  const db = await Database.open('path/to/sstables', {
    schema: 'path/to/schema.cql',
  });

  const result = await db.executeNative(
    'SELECT id, name, age FROM test_basic.simple_table LIMIT 5'
  );
  console.log(`Rows: ${result.rowCount}`);
  console.log(`Time: ${result.executionTimeMs}ms`);

  for (const row of result.rows) {
    console.log(`${row.name}: age ${row.age}`);
  }

  await db.close();
})();
```

Running against the test datasets produces:

```
Rows: 5
Time: 8ms
Debbie Soto: age 79
Richard Parker: age 58
Andrew Meyers: age 47
Jacqueline Davis: age 27
Aaron Moore: age 68
```

## Opening a database

`Database.open()` is the async factory method. Always call `close()` when done
to release file handles.

```javascript
// With schema
const db = await Database.open('/path/to/sstables', {
  schema: '/path/to/schema.cql',
});

// With memory limit (bytes)
const db = await Database.open('/path/to/sstables', {
  schema: '/path/to/schema.cql',
  memoryLimit: 256 * 1024 * 1024, // 256 MB
});

// Always close when done — close() is idempotent
await db.close();
await db.close(); // safe to call again
```

**`DatabaseOptions` fields**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `schema` | `string` (optional) | — | Path to a `.cql` schema file |
| `memoryLimit` | `number` (optional) | 1 GB | Maximum memory budget in bytes |
| `cacheEnabled` | `boolean` (optional) | `true` | Enable block, row, and query caches |
| `writable` | `boolean` (optional) | `false` | Enable INSERT/UPDATE/DELETE |
| `writeDir` | `string` (optional) | — | Directory for WAL and flushed SSTables; required when `writable: true` |

## executeNative() — recommended

Use `executeNative()` to get native JavaScript types. It maps CQL types to
`bigint`, `Date`, `Buffer`, `Set`, and `Map` instead of JSON-serializable values.

```javascript
const result = await db.executeNative(
  'SELECT id, name, age, account_balance FROM test_basic.simple_table LIMIT 5'
);

for (const row of result.rows) {
  // row.id          — string (UUID)
  // row.name        — string
  // row.age         — number
  // row.account_balance — string (decimal, precision preserved)
  console.log(row);
}
```

## execute() and the hex-encoding caveat

The older `execute()` method returns JSON-serializable values. It works well for
most types, but encodes `varint` and `decimal` using a hex format to preserve
arbitrary precision — output that is not human-readable:

```javascript
// Using execute() — hex encoding for varint/decimal
const result = await db.execute('SELECT amount FROM transactions');
console.log(result.rows[0].amount);
// varint:  "0x7f"            (127 in two's-complement big-endian hex)
// decimal: "decimal:2:0x7b"  (1.23 — scale 2, unscaled value 123)

// Using executeNative() — human-readable (recommended)
const native = await db.executeNative('SELECT amount FROM transactions');
console.log(native.rows[0].amount);
// varint:  127n    (BigInt)
// decimal: "1.23"  (string preserving precision)
```

**Use `executeNative()` for all new code.** The `execute()` method is kept for
backwards compatibility and JSON-serialization scenarios where you need raw,
reversible encoding. See [issue #343](https://github.com/pmcfadin/cqlite/issues/343)
for full details.

## Type conversions

| CQL Type | JavaScript Type (`executeNative`) | Notes |
|----------|----------------------------------|-------|
| `text`, `varchar`, `ascii` | `string` | |
| `int`, `smallint`, `tinyint` | `number` | |
| `float`, `double` | `number` | |
| `bigint`, `counter` | `bigint` | Full 64-bit precision |
| `varint` | `bigint` | Arbitrary precision |
| `decimal` | `string` | Precision-preserving string |
| `boolean` | `boolean` | |
| `blob` | `Buffer` | |
| `timestamp` | `Date` | |
| `date` | `Date` | |
| `time` | `bigint` | Nanoseconds since midnight |
| `duration` | `{ months, days, nanos }` | `nanos` is `bigint` |
| `uuid`, `timeuuid` | `string` | Lowercase formatted UUID |
| `inet` | `string` | IP address string |
| `list<T>` | `T[]` | |
| `set<T>` | `Set<T>` | |
| `map<K,V>` | `Map<K,V>` | |
| `tuple<...>` | `[...]` | Array |
| `frozen<T>` | Inner type | Unwrapped |
| UDT | `object` | Includes `_type` and `_keyspace` fields |
| `null` | `null` | |

## Streaming large result sets

`executeStreaming()` returns an `AsyncIterable<Row>` for memory-bounded
iteration. Use it with `for await...of`.

```javascript
// Basic streaming
for await (const row of db.executeStreaming('SELECT * FROM large_table')) {
  console.log(row.name);
}

// With custom buffer sizes
const config = { bufferSize: 256, chunkSize: 2500 };
for await (const row of db.executeStreaming('SELECT * FROM large_table', config)) {
  process(row);
}

// Early termination — resources cleaned up automatically
for await (const row of db.executeStreaming('SELECT * FROM huge_table')) {
  if (row.id === targetId) {
    break;
  }
}
```

**Default memory budget (~11 MB peak):**

| Option | Default | Description |
|--------|---------|-------------|
| `bufferSize` | `1024` | Rows to hold in-flight |
| `chunkSize` | `10000` | Rows fetched per storage read |

The `StreamingResult` object also exposes `rowsReceived` and `columns` for
progress tracking after the first row is yielded.

## Exporting to Parquet

`db.exportParquet()` writes query results straight to a Parquet file using
the embeddable core writer.
The query streams, so arbitrarily large result sets export within bounded
memory, and the export runs as an async task off the JavaScript main thread.

```javascript
const rows = await db.exportParquet(
  'SELECT * FROM test_basic.simple_table',
  '/tmp/simple_table.parquet',
  { rowGroupSize: 10000, compression: 'snappy' } // or 'zstd' / 'none'
);
console.log(`Exported ${rows} rows`);
```

The output uses the high-fidelity Arrow type mapping — typed lists, maps,
structs for UDTs/tuples, `Decimal128`, `Date32`, `Time64`, UUID extension —
see [Output Formats](/cqlite/user-docs/output-formats/) for the full table.

Errors carry the standard `code` / `category` / `isRecoverable` properties:
invalid options surface as `CONFIG`, file and encoding failures as `IO`, and
query failures through the usual `QUERY` / `PARSE` codes.

## Error handling

All CQLite errors include structured metadata for programmatic handling:

```javascript
const { Database } = require('@cqlite/node');

try {
  const db = await Database.open('/path/to/data', {
    schema: '/path/to/schema.cql',
  });
  const result = await db.executeNative('SELECT * FROM keyspace.table');
  await db.close();
} catch (e) {
  console.log(`Code: ${e.code}`);          // 'IO', 'SCHEMA', 'QUERY', etc.
  console.log(`Category: ${e.category}`);  // 'System', 'Schema', 'Query', etc.
  console.log(`Recoverable: ${e.isRecoverable}`);
  console.log(`Message: ${e.message}`);
}
```

**Error code reference**

| Code | Category | Description | Recoverable |
|------|----------|-------------|-------------|
| `IO` | `System` | File system errors (access, memory, timeout) | Yes |
| `SCHEMA` | `Schema` | Schema parsing or validation failures | No |
| `QUERY` | `Query` | Query execution failures | No |
| `PARSE` | `Data` | CQL syntax errors, type conversion failures | No |
| `CONFIG` | `Configuration` | Invalid configuration | No |
| `STORAGE` | `Storage` | Storage engine errors | No |
| `NOT_FOUND` | `NotFound` | Table or resource not found | No |
| `INVALID_INPUT` | `Logic` | Invalid operation (e.g. closed database) | No |

## Column metadata

Each query result includes `columns: ColumnInfo[]`:

```javascript
const result = await db.executeNative('SELECT * FROM test_basic.simple_table LIMIT 1');

for (const col of result.columns) {
  console.log(`${col.name}: ${col.dataType} (nullable: ${col.nullable}, pos: ${col.position})`);
}
// id: Text (nullable: false, pos: 0)
// name: Text (nullable: true, pos: 1)
// ...
```

**`ColumnInfo` fields**: `name`, `dataType`, `nullable`, `position`, `tableName`.

## Database statistics

```javascript
const stats = await db.getStats();
console.log(`SSTables: ${stats.totalSstables}`);
console.log(`Total rows: ${stats.totalRows}`);      // bigint
console.log(`Memory: ${stats.memoryUsedBytes}`);    // bigint
```

## Write support

Open the database with `writable: true` to enable INSERT, UPDATE, and DELETE.

```javascript
const db = await Database.open('path/to/sstables', {
  schema: 'schema.cql',
  writable: true,
  writeDir: '/tmp/my-writes',
});

await db.execute(
  "INSERT INTO test_basic.simple_table (id, name, age) " +
  "VALUES (22222222-2222-2222-2222-222222222222, 'Bob', 25)"
);

// Flush the memtable to an SSTable on disk
const path = await db.flushRun();
console.log('Flushed to:', path);

// Incremental compaction
let report;
do {
  report = await db.maintenanceStep({ budgetMs: 100 });
  console.log(`Merged ${report.rowsMerged} rows`);
} while (report.pendingCompaction);

// Synchronous write stats getter
const stats = db.writeStats;
console.log(`Memtable: ${stats.memtableSize} bytes, ${stats.memtableRows} rows`);

await db.close();
```

**Known write limitations**

- Counter columns cannot be written; `execute()` throws `CqliteError` for counter mutations.
- The writer produces BIG-format index files, not BTI format.

## TypeScript support

Complete TypeScript definitions are included in `lib/index.d.ts`:

```typescript
import { Database, DatabaseOptions, NativeQueryResult, CqliteError } from '@cqlite/node';

async function query(): Promise<void> {
  const db = await Database.open('/path/to/sstables', {
    schema: '/path/to/schema.cql',
  } satisfies DatabaseOptions);

  const result: NativeQueryResult = await db.executeNative(
    'SELECT * FROM test_basic.simple_table LIMIT 10'
  );

  for (const row of result.rows) {
    const name = row.name as string;
    console.log(name);
  }

  await db.close();
}
```

## API reference summary

| Method / property | Returns | Description |
|-------------------|---------|-------------|
| `Database.open(dir, options?)` | `Promise<Database>` | Open a database |
| `db.execute(query)` | `Promise<QueryResult>` | Execute query; hex-encoded varint/decimal |
| `db.executeNative(query)` | `Promise<NativeQueryResult>` | Execute query with native JS types (recommended) |
| `db.executeStreaming(query, config?)` | `StreamingResult` | Memory-bounded async iteration |
| `db.exportParquet(query, path, options?)` | `Promise<number>` | Stream query results to a Parquet file |
| `db.getStats()` | `Promise<DatabaseStats>` | Storage and memory metrics |
| `db.prepare(query)` | `Promise<PreparedStatement>` | Parse and plan a query |
| `db.flushRun()` | `Promise<string>` | Flush memtable; returns Data.db path |
| `db.maintenanceStep(options?)` | `Promise<MaintenanceReport>` | Incremental compaction |
| `db.writeStats` | `WriteStats` | Synchronous write engine snapshot |
| `db.close()` | `Promise<void>` | Release resources (idempotent) |
| `db.isClosed` | `boolean` | True if the connection is closed |

Full TypeScript definitions are in
[`bindings/node/lib/index.d.ts`](https://github.com/pmcfadin/cqlite/blob/main/bindings/node/lib/index.d.ts).

## Resources

- [GitHub Repository](https://github.com/pmcfadin/cqlite)
- [npm Package](https://www.npmjs.com/package/@cqlite/node)
- [TypeScript Definitions](https://github.com/pmcfadin/cqlite/blob/main/bindings/node/lib/index.d.ts)
- [Query from Node.js recipe](/cqlite/agents-using/query-nodejs/) — copy-pasteable agent recipe with expected output shapes
