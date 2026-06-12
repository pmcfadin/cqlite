---
title: Count rows
description: Count rows in a CQLite table with and without filters using SELECT and JSON output.
sidebar:
  label: Count rows
  order: 7
---

# Count rows

**Task**: Determine how many rows a table contains, with or without filters.

## Workaround: total row count via LIMIT + row counting

`COUNT(*)` is not fully supported in the current release (it returns a null aggregate). The recommended workaround is to fetch all rows with `SELECT` and count the result set in a post-processing step.

<!-- SMOKE:CLI -->
```bash
cqlite \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  --query "SELECT id FROM test_basic.simple_table" \
  --out json \
| python3 -c "import json,sys; rows=json.load(sys.stdin); print(len(rows))"
```
<!-- /SMOKE:CLI -->

**Expected output**:

```
1000
```

## Count filtered rows

```bash
cqlite \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  --query "SELECT id FROM test_basic.simple_table WHERE age > 70" \
  --out json \
| python3 -c "import json,sys; rows=json.load(sys.stdin); print(len(rows))"
```

**Expected output** (count varies by dataset):

```
148
```

## Count via jq (alternative)

```bash
cqlite \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  --query "SELECT id FROM test_basic.simple_table" \
  --out json \
| jq 'length'
```

**Expected**:

```
1000
```

## Count rows in a collection table

```bash
cqlite \
  --schema test-data/schemas/collections.cql \
  --data-dir test-data/datasets/sstables \
  --query "SELECT id FROM test_collections.collection_table" \
  --out json \
| python3 -c "import json,sys; rows=json.load(sys.stdin); print(len(rows))"
```

**Expected**:

```
1000
```

## Known limitation: COUNT(*) aggregate

`SELECT COUNT(*) FROM table` currently returns a row with a null value:

```bash
cqlite \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  --query "SELECT COUNT(*) FROM test_basic.simple_table" \
  --out json
```

Output (aggregate not implemented):

```json
[{"col_0": null}]
```

Use the `SELECT id ... | count` pattern above until aggregate functions are implemented (planned for a future milestone).

## Row count per table (script)

```bash
for table in simple_table typed_table; do
  count=$(cqlite \
    --schema test-data/schemas/basic-types.cql \
    --data-dir test-data/datasets/sstables \
    --query "SELECT id FROM test_basic.${table}" \
    --out json 2>/dev/null \
  | python3 -c "import json,sys; print(len(json.load(sys.stdin)))")
  echo "${table}: ${count}"
done
```

**Expected output**:

```
simple_table: 1000
typed_table: 1000
```
