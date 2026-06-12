---
title: SSTable to JSON one-liner
description: Dump a Cassandra 5.0 SSTable table to a JSON array on stdout using one cqlite command.
sidebar:
  label: SSTable to JSON
  order: 1
---

# SSTable to JSON one-liner

**Task**: Dump all rows from a table to a JSON array on stdout.

<!-- SMOKE:CLI -->
```bash
cqlite \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  --query "SELECT id, name, age FROM test_basic.simple_table LIMIT 3" \
  --out json
```
<!-- /SMOKE:CLI -->

**Expected output shape** (real output, trimmed to 3 rows):

```json
[
  {
    "id": "0023ece7-7c4e-4705-9068-d1a59ec5fe19",
    "name": "Debbie Soto",
    "age": 79
  },
  {
    "id": "009fb913-7173-40df-b4ea-67ed6834cfe5",
    "name": "Richard Parker",
    "age": 58
  },
  {
    "id": "00a74226-9bde-4259-9ba0-d74359e8013e",
    "name": "Andrew Meyers",
    "age": 47
  }
]
```

**Exit code**: `0` on success.

**Output format**:
- Array of objects, one object per row.
- Written to stdout; redirect with `--output /path/to/out.json`.
- INFO/WARN log lines go to stderr; stdout is clean JSON.

## Dump all columns

Omit the column list in the SELECT to retrieve all columns:

<!-- SMOKE:CLI -->
```bash
cqlite \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  --query "SELECT * FROM test_basic.simple_table LIMIT 1" \
  --out json
```
<!-- /SMOKE:CLI -->

**Expected keys** (19 columns in `simple_table`; null columns omitted from objects):

```
id, name, age, salary, height, weight, active, created, birth_date,
work_time, description, account_balance, session_id, ip_address,
small_number, medium_number, duration_val, varchar_field, large_int
```

## Write to a file

```bash
cqlite \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  --query "SELECT * FROM test_basic.simple_table" \
  --out json \
  --output /tmp/simple_table.json
```

Use `--overwrite` to replace an existing file (exit code `6` if the file exists and `--overwrite` is not set).

## Failure modes

| Symptom | Error text | Fix |
|---------|-----------|-----|
| No `--schema` flag | `Error: Schema not found for table 'simple_table'` | Add `--schema path/to/schema.cql` |
| Wrong table name | `Error: Schema not found for table 'bad_name'` | Check table name matches schema |
| Output file exists | exit code `6` | Add `--overwrite` |
| BTI-format SSTable | WARN on stderr, 0 rows returned | BTI (`da-` prefix) not supported; use `nb-` SSTables |

## Type mapping

See the full type map in [Output Formats](/cqlite/user-docs/output-formats/).

Key rules:
- UUIDs → string (`"0023ece7-..."`)
- Timestamps → string (`"2025-10-06 01:00:30.616+0000"`)
- Booleans → `true` / `false`
- Integers → JSON number
- Floats → JSON number (may include rounding artifacts)
- `null` columns are omitted from the JSON object (not rendered as `null`)
