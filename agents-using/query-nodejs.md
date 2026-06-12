---
title: Query from Node.js
description: Open a CQLite database and run SELECT queries from JavaScript or TypeScript using @cqlite/node.
sidebar:
  label: Query from Node.js
  order: 3
---

# Query from Node.js

**Task**: Open a Cassandra 5.0 SSTable directory and query it from JavaScript/TypeScript.

## Install

```bash
npm install @cqlite/node
```

Requires Node.js 18+. Pre-built binaries for Linux (x86_64, ARM64), macOS (Intel and Apple Silicon), and Windows (x64). See [Node.js Bindings](/cqlite/user-docs/nodejs/) for full install details.

## Basic query

<!-- SMOKE:NODE -->
```javascript
const { Database } = require('@cqlite/node');

(async () => {
  const db = await Database.open('test-data/datasets/sstables', {
    schema: 'test-data/schemas/basic-types.cql',
  });

  const result = await db.executeNative(
    'SELECT id, name, age FROM test_basic.simple_table LIMIT 3'
  );

  console.log('Rows:', result.rowCount);
  for (const row of result.rows) {
    console.log(JSON.stringify(row));
  }

  await db.close();
})();
```
<!-- /SMOKE:NODE -->

**Expected output** (real output):

```
Rows: 3
{"name":"Debbie Soto","age":79,"id":"0023ece7-7c4e-4705-9068-d1a59ec5fe19"}
{"age":58,"name":"Richard Parker","id":"009fb913-7173-40df-b4ea-67ed6834cfe5"}
{"age":47,"name":"Andrew Meyers","id":"00a74226-9bde-4259-9ba0-d74359e8013e"}
```

Note: key order in row objects may vary; do not rely on property order.

## executeNative() vs execute()

Use `executeNative()`. The older `execute()` method encodes `varint` and `decimal` as hex strings; `executeNative()` returns `BigInt`, `Date`, `Buffer`, `Set`, and `Map` directly.

```javascript
// executeNative — recommended
const result = await db.executeNative('SELECT id, name FROM test_basic.simple_table LIMIT 1');
// result.rows[0].id is a string (UUID)
// result.rows[0].name is a string

// execute — deprecated
const result2 = await db.execute('SELECT id, name FROM test_basic.simple_table LIMIT 1');
// varint fields would appear as "0x..." hex strings
```

## Type mapping (executeNative)

| CQL type | JavaScript type |
|----------|----------------|
| `text`, `varchar`, `ascii` | `string` |
| `int`, `smallint`, `tinyint` | `number` |
| `bigint`, `counter` | `BigInt` |
| `float`, `double` | `number` |
| `boolean` | `boolean` |
| `uuid`, `timeuuid` | `string` (standard UUID format) |
| `timestamp` | `Date` |
| `date` | `string` (YYYY-MM-DD) |
| `time` | `BigInt` (nanoseconds since midnight) |
| `blob` | `Buffer` |
| `inet` | `string` (dotted decimal or IPv6) |
| `decimal` | `string` (exact decimal string) |
| `varint` | `BigInt` |
| `list<T>` | `Array` |
| `set<T>` | `Set` |
| `map<K,V>` | `Map` |

## QueryResult fields

```typescript
interface QueryResult {
  rows: object[];            // one object per row
  rowCount: number;          // number of rows returned
  executionTimeMs: number;   // wall-clock time in ms
  columns: ColumnInfo[];     // column metadata
}

interface ColumnInfo {
  name: string;
  dataType: string;          // e.g. "Text", "Integer", "List"
  nullable: boolean;
  position: number;          // 0-indexed
  tableName: string | null;
}
```

## Filtering with WHERE

```javascript
const result = await db.executeNative(
  'SELECT id, name, age FROM test_basic.simple_table WHERE age > 70 LIMIT 3'
);

for (const row of result.rows) {
  console.log(row.name, row.age);
}
```

**Expected** (first 3 rows where age > 70):

```
Debbie Soto 79
Angela Davis 72
Sabrina Rodriguez 78
```

## Streaming large result sets

For large tables, use `executeStreaming()` to avoid loading the full result into memory:

```javascript
const stream = await db.executeStreaming(
  'SELECT id, name FROM test_basic.simple_table'
);

let count = 0;
for await (const row of stream) {
  count++;
}
console.log('Total rows:', count); // 1000
```

## Always close the database

```javascript
const db = await Database.open(dataDir, { schema });
try {
  // ... queries ...
} finally {
  await db.close(); // idempotent — safe to call multiple times
}
```

## Failure modes

| Symptom | Error `code` | Fix |
|---------|--------------|-----|
| No `schema` option | `"SCHEMA"` | Pass `schema: '/path/to/schema.cql'` in options |
| Path does not exist | `"IO"` | Check the data directory path |
| Table not in schema | `"SCHEMA"` | Verify the table name in the CQL |
| BTI-format SSTables | `0` rows returned | BTI format not yet supported |
