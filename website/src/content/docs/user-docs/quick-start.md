---
title: Quick Start
description: Get CQLite running and query a Cassandra 5.0 SSTable in under five minutes.
sidebar:
  label: Quick Start
  order: 1
---

# Quick Start

This guide gets you from nothing to a working CQLite query in under five minutes.
CQLite reads Cassandra 5.0 SSTables directly from disk — no cluster, no JVM, no daemon.

## What you need

- A CQLite binary (see [Installation](/cqlite/user-docs/installation/))
- A Cassandra 5.0 SSTable directory (`nb-1-big-Data.db` and siblings), **or** the
  bundled test data (option A below)

## Option A — Use the bundled test data (fastest)

If you cloned the repository you can fetch the real Cassandra 5.0 test datasets and
run the one-shot CLI immediately.

```bash
# Fetch test datasets (~50 MB tarball, real Cassandra 5.0 SSTables)
bash test-data/scripts/fetch-datasets.sh

# Run a query — JSON output to stdout
cargo run --package cqlite-cli -- \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  --query "SELECT * FROM test_basic.simple_table LIMIT 5" \
  --out json
```

Expected output (abbreviated):

```json
[
  {"id":"…","name":"Alice","age":30},
  {"id":"…","name":"Bob","age":25}
]
```

## Option B — Use your own Cassandra 5.0 data

### 1. Locate your SSTables

Cassandra 5.0 stores SSTables under the data directory, one subdirectory per table:

```
/var/lib/cassandra/data/
└── my_keyspace/
    └── my_table-{uuid}/
        ├── nb-1-big-Data.db
        ├── nb-1-big-Index.db
        ├── nb-1-big-Summary.db
        ├── nb-1-big-Statistics.db
        ├── nb-1-big-CompressionInfo.db
        ├── nb-1-big-Filter.db
        └── nb-1-big-TOC.txt
```

### 2. Provide a schema file

CQLite needs a `.cql` schema file describing the table structure.
Create one from your existing Cassandra keyspace:

```cql
-- my_keyspace.cql
CREATE KEYSPACE my_keyspace
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE my_keyspace;

CREATE TABLE my_table (
    id   UUID    PRIMARY KEY,
    name TEXT,
    age  INT
);
```

You can export the schema directly from `cqlsh`:

```bash
cqlsh -e "DESCRIBE KEYSPACE my_keyspace" > my_keyspace.cql
```

### 3. Run a query

```bash
cqlite \
  --schema my_keyspace.cql \
  --data-dir /var/lib/cassandra/data/my_keyspace \
  --query "SELECT * FROM my_keyspace.my_table LIMIT 10" \
  --out json
```

## Python

Install the Python binding, then open a dataset and run a query:

```bash
pip install cqlite-py
```

```python
import cqlite

with cqlite.open('test-data/datasets/sstables',
                 schema='test-data/schemas/basic-types.cql') as db:
    for row in db.execute('SELECT * FROM test_basic.simple_table LIMIT 5'):
        print(row.to_dict())
```

## Node.js

```bash
npm install @cqlite/node
```

```javascript
import { Database } from '@cqlite/node';

const db = await Database.open('test-data/datasets/sstables', {
  schema: 'test-data/schemas/basic-types.cql',
});
const result = await db.executeNative(
  'SELECT * FROM test_basic.simple_table LIMIT 5'
);
for (const row of result.rows) {
  console.log(row.name);
}
await db.close();
```

## CLI modes

Beyond one-shot mode shown above, the CLI offers an interactive REPL and a TUI:

```bash
# Interactive REPL
cqlite --schema my_keyspace.cql --data-dir /path/to/sstables repl

# Full terminal UI
cqlite --schema my_keyspace.cql --data-dir /path/to/sstables tui
```

In REPL mode:

```
[OK] Mem: 24.5 MB | Data: 1.2 GB
cqlite> SELECT * FROM my_keyspace.my_table LIMIT 5;
```

## What CQLite cannot do (yet)

- Query Cassandra 3.x or 4.x SSTables (Cassandra 5.0 `nb-*-big-*` format only)
- Read BTI-format SSTables (the opt-in `bti` index format; see [Limitations](/cqlite/user-docs/limitations/))
- Run against a live Cassandra cluster — CQLite works on local files only

For a full list, see [Limitations](/cqlite/user-docs/limitations/).

## Next steps

- [Installation](/cqlite/user-docs/installation/) — prebuilt binaries, cargo, pip, npm
- [Troubleshooting](/cqlite/user-docs/troubleshooting/) — common issues and fixes
<!-- TODO(W8): link CLI reference, Output Formats, Python, Node.js when merged -->
