---
title: "Node.js: Services and Tooling"
description: How to use the CQLite Node.js bindings to build data inspection services, ops dashboards, and CLI tools that read Cassandra SSTables without a live cluster.
sidebar:
  label: Node.js / Services
  order: 3
---

# Node.js: Services and Tooling

The CQLite Node.js bindings (`@cqlite/node`, built with napi-rs) let you read
Cassandra 5.0 SSTables from Node.js — no cluster, no JVM, no Cassandra driver.

This page covers the services and tooling use case: data inspection APIs, operational
dashboards, async streaming for large result sets, and native JavaScript type handling.

<!-- TODO(W5): link to full Node.js bindings API reference when merged -->

## Installation

```bash
npm install @cqlite/node
```

Or build from source:

```bash
git clone https://github.com/pmcfadin/cqlite
cd cqlite/bindings/node
npm install
npm run build      # requires Rust 1.85+
```

## The core pattern

```javascript
const { Database } = require("@cqlite/node");

const db = await Database.open("/path/to/sstables", {
  schema: "/path/to/schema.cql",
});

const result = await db.executeNative(
  "SELECT * FROM my_keyspace.my_table LIMIT 100"
);
console.log(`${result.rowCount} rows in ${result.executionTimeMs}ms`);

for (const row of result.rows) {
  console.log(row);
}

await db.close();
```

Use `executeNative()` rather than the deprecated `execute()`. It returns native
JavaScript types: `BigInt` for 64-bit integers, `Date` for timestamps, `Buffer` for
blobs, `Set` and `Map` for collection types. `execute()` uses hex-encoded strings for
`varint` and `decimal`, which is harder to work with.

## Complete example: data inspection service

This example was run against the real CQLite test datasets and its output is shown below.

```javascript
const { Database } = require("@cqlite/node");
const path = require("path");

const DATASETS = path.resolve("test-data/datasets/sstables");
const SCHEMA   = path.resolve("test-data/schemas/basic-types.cql");

async function inspectTable() {
  const db = await Database.open(DATASETS, { schema: SCHEMA });

  try {
    const result = await db.executeNative(
      "SELECT id, name, age, active, account_balance " +
      "FROM test_basic.simple_table LIMIT 3"
    );

    console.log(`rowCount: ${result.rowCount}`);
    console.log(`executionTimeMs: ${result.executionTimeMs}`);
    console.log("columns:", result.columns.map(c => `${c.name}:${c.dataType}`));
    console.log();

    for (const row of result.rows) {
      // account_balance is returned as a string for decimal types
      console.log(JSON.stringify(row));
    }
  } finally {
    await db.close();
  }
}

inspectTable().catch(console.error);
```

**Actual output** (run against `test_basic.simple_table`):

```
rowCount: 3
executionTimeMs: 9
columns: [ 'name:Text', 'age:Integer', 'account_balance:Decimal', 'id:Uuid', 'active:Boolean' ]

{"name":"Debbie Soto","age":79,"account_balance":"69799.73","id":"0023ece7-7c4e-4705-9068-d1a59ec5fe19","active":true}
{"active":false,"name":"Richard Parker","id":"009fb913-7173-40df-b4ea-67ed6834cfe5","age":58,"account_balance":"74196.78"}
{"active":false,"name":"Andrew Meyers","age":47,"account_balance":"47110.15","id":"00a74226-9bde-4259-9ba0-d74359e8013e"}
```

## CQL → JavaScript type mapping (`executeNative`)

| CQL type | JavaScript type |
|----------|----------------|
| boolean | `boolean` |
| tinyint, smallint, int | `number` |
| bigint, counter | `BigInt` |
| float, double | `number` |
| decimal, varint | `string` (decimal string representation) |
| text, varchar, ascii | `string` |
| blob | `Buffer` |
| timestamp | `Date` |
| date | `string` (`"YYYY-MM-DD"`) |
| time | `string` (`"HH:MM:SS.nnnnnnnnn"`) |
| uuid, timeuuid | `string` (standard hyphenated UUID format) |
| duration | `object` `{ months, days, nanoseconds: BigInt }` |
| inet | `string` |
| list, set | `Array` |
| map | `Map` |
| tuple | `Array` |
| UDT | `object` (field name → value) |

## Streaming for memory-bounded iteration

For large tables, `executeStreaming()` returns an async iterable. Rows are streamed
from the SSTable rather than loaded all at once:

```javascript
const { Database } = require("@cqlite/node");
const path = require("path");

const DATASETS = path.resolve("test-data/datasets/sstables");
const SCHEMA   = path.resolve("test-data/schemas/basic-types.cql");

async function streamRows() {
  const db = await Database.open(DATASETS, { schema: SCHEMA });

  try {
    let count = 0;
    const stream = await db.executeStreaming(
      "SELECT name, age, salary FROM test_basic.simple_table"
    );

    for await (const row of stream) {
      count++;
      // process each row without accumulating all in memory
    }

    console.log(`Streamed ${count} rows`);
  } finally {
    await db.close();
  }
}

streamRows().catch(console.error);
```

**Actual output** (run against `test_basic.simple_table`, 100 rows):

```
Streamed 100 rows
```

Default buffer size is 1,024 rows (~11 MB peak). Pass a `StreamingConfig` to tune:

```javascript
const stream = await db.executeStreaming(query, {
  chunkSize: 500,   // rows per internal chunk
  bufferSize: 2048, // max buffered rows
});
```

## Error handling

All errors thrown by `@cqlite/node` include `code`, `category`, and `isRecoverable`
properties:

```javascript
try {
  const result = await db.executeNative("SELECT * FROM nonexistent.table");
} catch (err) {
  console.error(err.message);      // human-readable message
  console.error(err.code);         // e.g. "NOT_FOUND", "SCHEMA", "IO"
  console.error(err.category);     // e.g. "NotFound", "Schema"
  console.error(err.isRecoverable); // boolean: retry vs. fix the input
}
```

| Code | Category | When it fires |
|------|----------|---------------|
| `IO` | System | File access, memory, timeout |
| `SCHEMA` | Schema | Table not found, schema mismatch |
| `QUERY` | Query | CQL syntax error |
| `PARSE` | Data | Binary format parsing error |
| `NOT_FOUND` | NotFound | SSTable or keyspace not found |
| `INVALID_INPUT` | Logic | Invalid operation or state |

## Building an Express inspection API

```javascript
const express = require("express");
const { Database } = require("@cqlite/node");
const path = require("path");

const app = express();
let db;

async function start() {
  db = await Database.open(process.env.SSTABLE_DIR, {
    schema: process.env.SCHEMA_FILE,
  });

  app.get("/tables/:keyspace/:table", async (req, res) => {
    const { keyspace, table } = req.params;
    const limit = Math.min(parseInt(req.query.limit ?? "100"), 1000);

    try {
      const result = await db.executeNative(
        `SELECT * FROM ${keyspace}.${table} LIMIT ${limit}`
      );
      res.json({
        rowCount: result.rowCount,
        executionTimeMs: result.executionTimeMs,
        columns: result.columns,
        rows: result.rows,
      });
    } catch (err) {
      const status = err.code === "NOT_FOUND" ? 404 : 500;
      res.status(status).json({ error: err.message, code: err.code });
    }
  });

  app.listen(3000, () => console.log("Listening on :3000"));
}

start().catch(err => { console.error(err); process.exit(1); });
```

## TypeScript support

`@cqlite/node` ships complete TypeScript definitions in `lib/index.d.ts`:

```typescript
import { Database, QueryResult, ColumnInfo, StreamingConfig } from "@cqlite/node";

const db: Database = await Database.open("/path/to/sstables", {
  schema: "/path/to/schema.cql",
});

const result: QueryResult = await db.executeNative(
  "SELECT * FROM my_ks.my_table LIMIT 10"
);

for (const col of result.columns as ColumnInfo[]) {
  console.log(`${col.name}: ${col.dataType} (nullable: ${col.nullable})`);
}
```

## What you cannot do (yet)

- **Write back to SSTables from Node.js.** The write API is in the core library but not
  yet exposed in the Node.js bindings.
- **Export Parquet directly from Node.js.** Use the CLI `--out parquet` flag for now.
  [Epic #682](https://github.com/pmcfadin/cqlite/issues/682) tracks this *(in progress)*.
- **Collection type fidelity in Parquet export.** Lists and maps are stringified.
  [Epic #673](https://github.com/pmcfadin/cqlite/issues/673) tracks this *(in progress)*.

## Further reading

- [GitHub repository](https://github.com/pmcfadin/cqlite)
- [API reference](/cqlite/api/latest/)
<!-- TODO(W5): link to Node.js bindings reference page when merged -->
