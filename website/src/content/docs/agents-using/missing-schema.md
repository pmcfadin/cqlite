---
title: Handle missing-schema errors
description: Diagnose and fix the most common CQLite failure — schema not found — including exact error text and recovery steps.
sidebar:
  label: Handle schema errors
  order: 9
---

# Handle missing-schema errors

**Task**: Understand and recover from the `Schema not found for table '...'` error.

## The error

Running a query without providing a schema file, or with the wrong schema:

<!-- SMOKE:CLI:exit=3 -->
```bash
cqlite \
  --data-dir test-data/datasets/sstables \
  --query "SELECT id, name FROM test_basic.simple_table LIMIT 1" \
  --out json
```
<!-- /SMOKE:CLI:exit=3 -->

**Exact error output** (stderr):

```
Error: Schema not found for table 'simple_table'

Cause: Table schema not found: simple_table

Troubleshooting:
1. Verify table name matches schema definition
2. Check that schema file was loaded correctly
3. Use 'read-sstable' command to inspect SSTable contents directly

Hint: Use ':schema load <file>' or '--schema <path>' to provide schema
```

**Exit code**: `3`

## Fix: add --schema

```bash
cqlite \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  --query "SELECT id, name FROM test_basic.simple_table LIMIT 1" \
  --out json
```

**Expected output** (error resolved):

```json
[{"id": "0023ece7-7c4e-4705-9068-d1a59ec5fe19", "name": "Debbie Soto"}]
```

## Schema file selection

| Keyspace prefix | Schema file |
|-----------------|-------------|
| `test_basic.*` | `test-data/schemas/basic-types.cql` |
| `test_collections.*` | `test-data/schemas/collections.cql` |
| `test_timeseries.*` | `test-data/schemas/time-series.cql` |
| `test_wide_rows.*` | `test-data/schemas/wide-rows.cql` |

## Wrong table name

Providing a schema but misspelling the table name produces the same error:

```bash
cqlite \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  --query "SELECT id FROM test_basic.simpel_table LIMIT 1"
```

**Error**:

```
Error: Schema not found for table 'simpel_table'
```

**Exit code**: `3`

**Fix**: correct the table name to match the `CREATE TABLE` in the schema file.

## Python

```python
import cqlite

try:
    with cqlite.open(
        "test-data/datasets/sstables"
        # schema= intentionally omitted
    ) as db:
        for row in db.execute("SELECT id FROM test_basic.simple_table LIMIT 1"):
            print(row.to_dict())
except cqlite.CqliteError as e:
    print("Error:", e)
    # Error: Schema not found for table 'simple_table'
```

## Node.js

```javascript
const { Database } = require('@cqlite/node');

(async () => {
  // schema option intentionally omitted
  const db = await Database.open('test-data/datasets/sstables');
  try {
    await db.executeNative('SELECT id FROM test_basic.simple_table LIMIT 1');
  } catch (err) {
    console.log('code:', err.code);      // "SCHEMA"
    console.log('message:', err.message); // "Schema not found..."
  } finally {
    await db.close();
  }
})();
```

## Other common schema errors

| Error | Cause | Fix |
|-------|-------|-----|
| `Schema not found for table '...'` | Missing `--schema` or wrong file | Add `--schema` with the right `.cql` file |
| `Table schema not found: ...` | Table name in query doesn't match schema | Check CREATE TABLE names in the schema file |
| `Partition key column count mismatch` | Wrong schema for table (different column count) | Use the schema file that matches the SSTable |
| Zero rows, no error | Data.db files not fetched | Run `bash test-data/scripts/fetch-datasets.sh` |
