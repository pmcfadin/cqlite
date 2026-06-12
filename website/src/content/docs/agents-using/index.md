---
title: "For Agents: Using CQLite"
description: Task-oriented recipes for AI agents integrating with or automating CQLite. Every command was run against the real cassandra5-small-full-v3.1 test datasets.
sidebar:
  label: Overview
  order: 0
---

# For Agents: Using CQLite

Terse, copy-pasteable, machine-verifiable recipes for agents that integrate with CQLite as a CLI tool, Python package, or Node.js module.

**All commands on these pages were run against the real `cassandra5-small-full-v3.1` test datasets.** Expected outputs are trimmed but structurally accurate.

## Recipes

| Page | Task | Interface |
|------|------|-----------|
| [SSTable to JSON one-liner](/cqlite/agents-using/sstable-to-json/) | Dump a table as a JSON array | CLI |
| [Query from Python](/cqlite/agents-using/query-python/) | Open a database and run SELECT from Python | Python |
| [Query from Node.js](/cqlite/agents-using/query-nodejs/) | Open a database and run SELECT from Node.js | Node.js |
| [Export to Parquet](/cqlite/agents-using/export-parquet/) | Write query results to a Parquet file | CLI |
| [Export to CSV](/cqlite/agents-using/export-csv/) | Write query results to a CSV file | CLI |
| [Inspect schema](/cqlite/agents-using/inspect-schema/) | Discover which tables and columns are available | CLI |
| [Count rows](/cqlite/agents-using/count-rows/) | Count rows in a table with and without filters | CLI |
| [Read collections](/cqlite/agents-using/read-collections/) | Query LIST, SET, and MAP columns | CLI + Python |
| [Handle missing-schema errors](/cqlite/agents-using/missing-schema/) | Diagnose and fix schema-not-found failures | CLI |
| [Write a mutation and flush](/cqlite/agents-using/write-mutation/) | Insert a row and flush the memtable to SSTable | CLI |
| [Export SSTable for Cassandra import](/cqlite/agents-using/export-sstable/) | Re-export SSTables in Cassandra-compatible format | CLI |

## Setup

Every recipe assumes:

```bash
export CQLITE_DATASETS_ROOT=/path/to/test-data/datasets
```

Schemas are in `test-data/schemas/`. The write-support recipes require the CLI built with `--features write-support`:

```bash
cargo build --package cqlite-cli --features write-support
```

## Design principles

1. **Real output** — every example was executed; no invented data.
2. **Exit codes documented** — 0 = success; non-zero = failure.
3. **Failure modes explicit** — each recipe lists the exact error text for the most common failures.
4. **Terse** — code first.

## Error codes

Error codes used by the CLI (exit code 0 = success; all errors print to stderr) and thrown as JavaScript/Python exceptions by the bindings.

| Code | Category | Description | Typical cause |
|------|----------|-------------|---------------|
| `IO` | System | I/O errors — file read/write, file not found | Missing Data.db, wrong path |
| `SCHEMA` | Schema | Schema or table lookup failure | `--schema` not provided, or table name typo |
| `QUERY` | Query | Query execution or CQL syntax error | Unsupported CQL, bad column name |
| `PARSE` | Data | Binary format parsing or type conversion error | Corrupt SSTable, unsupported format |
| `CONFIG` | Configuration | Configuration validation error | Missing required option, bad flag combination |
| `STORAGE` | Storage | Storage engine error | WriteEngine misconfiguration |
| `NOT_FOUND` | NotFound | Resource does not exist | Table has no SSTables on disk |
| `INVALID_INPUT` | Logic | Invalid operation or state | Type mismatch in mutation, bad JSON mutation format |

### CLI exit codes

| Exit code | Meaning |
|-----------|---------|
| `0` | Success |
| `1` | Unhandled / internal error |
| `2` | Invalid CLI arguments |
| `3` | Schema or query error (SCHEMA / QUERY category) |
| `5` | Parse error (PARSE category) |
| `6` | File already exists (use `--overwrite` to force) |

### Node.js error properties

```javascript
try {
  await db.executeNative('SELECT * FROM unknown.table');
} catch (err) {
  console.log(err.code);          // e.g. "SCHEMA"
  console.log(err.category);      // e.g. "Schema"
  console.log(err.isRecoverable); // false for most schema/config errors
}
```

### Python exception type

```python
import cqlite
try:
    with cqlite.open('/no/such/path', schema='schema.cql') as db:
        pass
except cqlite.CqliteError as e:
    print(e)  # human-readable message
```
