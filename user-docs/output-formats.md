---
title: Output Formats
description: JSON, CSV, and Parquet output format behavior, options, and type-fidelity caveats.
sidebar:
  label: Output Formats
  order: 4
---

# Output Formats

CQLite query results can be written in four formats: `table`, `json`, `csv`, and `parquet`. This page covers the behavior and type mapping for each, with verified examples run against the real test datasets (`cassandra5-small-full-v3.1`).

## Selecting a format

Use `--out` (or its env var `CQLITE_OUT`) to select the format for one-shot and query-subcommand output:

```bash
cqlite --schema schema.cql --data-dir ./sstables \
  --query "SELECT id, name FROM ks.tbl LIMIT 5" \
  --out json   # or: csv, table, parquet
```

`--out` takes precedence over the global `--format` flag when both are specified. See [Output format precedence](/cqlite/user-docs/cli-reference/#output-format-precedence) for the full resolution order.

---

## `table` — cqlsh-compatible table format (default)

The default format. Renders results as an ASCII table compatible with `cqlsh` output, with column names in the header row.

**Example:**

```bash
cqlite \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  --query "SELECT id, name, age FROM test_basic.simple_table LIMIT 3"
```

Output (truncated for readability):
```
 id                                   | name           | age
--------------------------------------+----------------+-----
 0023ece7-7c4e-4705-9068-d1a59ec5fe19 | Debbie Soto    |  79
 009fb913-7173-40df-b4ea-67ed6834cfe5 | Richard Parker |  58
 00a74226-9bde-4259-9ba0-d74359e8013e | Andrew Meyers  |  47

(3 rows)
```

The table format is intended for human display, not machine parsing. Use `json` or `csv` for scripting.

---

## `json` — JSON array of objects

Renders each row as a JSON object; the full result set is a JSON array. Written to stdout (or to a file with `--output`).

**Type mapping:**

| CQL type | JSON representation |
|----------|-------------------|
| `text`, `varchar`, `ascii` | `string` |
| `int`, `smallint`, `tinyint` | `number` (integer) |
| `bigint`, `counter` | `number` (integer) |
| `float`, `double` | `number` (float) |
| `boolean` | `true` / `false` |
| `uuid`, `timeuuid` | `string` (standard UUID format) |
| `timestamp` | `string` (ISO 8601 UTC, e.g. `"2025-10-06 01:12:05.926+0000"`) |
| `date` | `string` (YYYY-MM-DD) |
| `time` | `string` (HH:MM:SS.nnnnnnnnn) |
| `blob` | `string` (hex-encoded, `0x...`) |
| `inet` | `string` (dotted decimal or IPv6) |
| `decimal` | `string` (exact decimal string) |
| `varint` | `string` (decimal integer string) |
| `duration` | `string` (e.g. `"3033000000000ns"`) |
| `list<T>` | JSON array |
| `set<T>` | JSON array |
| `map<K,V>` | JSON object (keys coerced to strings) |
| `tuple<...>` | JSON array |
| UDT | JSON object (field names as keys) |
| `null` / tombstone | `null` |

**Verified example:**

```bash
cqlite \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  --query "SELECT id, name, age FROM test_basic.simple_table LIMIT 3" \
  --out json
```

Output:
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

**Writing to a file:**

```bash
cqlite \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  --query "SELECT * FROM test_basic.simple_table LIMIT 100" \
  --out json \
  --output results.json
```

Stdout: `Output written to: results.json`

---

## `csv` — Comma-separated values

The first line is the header row (column names). Values follow standard CSV rules: strings are unquoted unless they contain commas, newlines, or double-quotes.

**Verified example:**

```bash
cqlite \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  --query "SELECT id, name, age FROM test_basic.simple_table LIMIT 3" \
  --out csv
```

Output:
```
id,name,age
0023ece7-7c4e-4705-9068-d1a59ec5fe19,Debbie Soto,79
009fb913-7173-40df-b4ea-67ed6834cfe5,Richard Parker,58
00a74226-9bde-4259-9ba0-d74359e8013e,Andrew Meyers,47
```

**Type mapping:** Same string serialization as JSON, without the JSON quotes. Complex types (lists, maps, UDTs) are serialized as their JSON string representation within a CSV field.

**Using `CQLITE_OUT` for piping:**

```bash
CQLITE_OUT=csv cqlite \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  --query "SELECT id, name, age FROM test_basic.simple_table LIMIT 2"
```

Output:
```
id,name,age
0023ece7-7c4e-4705-9068-d1a59ec5fe19,Debbie Soto,79
009fb913-7173-40df-b4ea-67ed6834cfe5,Richard Parker,58
```

---

## `parquet` — Apache Parquet binary format

Parquet is a columnar binary format for analytics workloads. CQLite writes valid Parquet files (magic bytes `PAR1`) compatible with Apache Arrow, DuckDB, Spark, and other readers.

**Parquet requires a file destination** — it cannot be written to stdout:

```bash
cqlite \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  --query "SELECT id, name, age FROM test_basic.simple_table LIMIT 100" \
  --out parquet \
  --output results.parquet
```

If you specify `--out parquet` without `--output`, CQLite exits with an error.

### Current type-fidelity caveats

**CQLite's Parquet output has known type-mapping limitations tracked in [epic #673](https://github.com/pmcfadin/cqlite/issues/673).** The issues affect complex and high-precision types:

| CQL type | Current Parquet representation | Planned (epic #673) |
|----------|-------------------------------|---------------------|
| `list<T>`, `set<T>` | `List<Utf8>` (string elements) | `List<T>` with typed elements |
| `map<K,V>` | `Map<Utf8, Utf8>` (string keys/values) | `Map<K, V>` with typed keys/values |
| `tuple<...>` | Single JSON string column | Arrow `Struct` with positional fields |
| UDT | Single JSON string column | Arrow `Struct` with named fields |
| `frozen<...>` | Same as non-frozen | Transparent (no change needed) |
| `date` | `Utf8` string | `Date32` |
| `time` | `Utf8` string | `Time64(Nanosecond)` |
| `decimal` | `Utf8` string | `Decimal128` |
| `varint` | `Utf8` string | `LargeBinary` or `Decimal128` |
| `duration` | `Utf8` string | `Interval(MonthDayNano)` |
| `inet` | `Utf8` string | `Utf8` (no change planned) |
| `uuid` | `Utf8` string | `FixedSizeBinary(16)` + UUID annotation |
| `timestamp` | `Utf8` string | `Timestamp(Microsecond, UTC)` |
| `blob` | `Utf8` hex string | `LargeBinary` |

**Scalar scalars that currently work correctly:** `int`, `smallint`, `tinyint` → `Int32`/`Int16`/`Int8`; `bigint`, `counter` → `Int64`; `float` → `Float32`; `double` → `Float64`; `boolean` → `Boolean`; `text`, `varchar`, `ascii` → `Utf8`.

**Consequence for downstream consumers:** Predicate pushdown on date/time/decimal columns, or columnar processing of list/map/UDT columns, requires either the improvements from epic #673 or a post-read cast in the consuming tool (e.g., DuckDB `CAST`).

**Root cause (per epic #673):** `ColumnInfo.data_type` carries an unparameterized flat enum. The fully-parameterized `CqlType` from the schema layer is not yet threaded through to query results, so the Parquet writer cannot construct the proper Arrow schema for parameterized types. Epic #673 tracks the full fix.

### Reading Parquet output

The produced files are standard Parquet and can be read with any Parquet-compatible tool:

```python
# Python (pyarrow)
import pyarrow.parquet as pq
table = pq.read_table("results.parquet")
print(table.schema)
```

```sql
-- DuckDB
SELECT * FROM read_parquet('results.parquet');
```

```bash
# Apache Arrow CLI
parquet-tools show results.parquet
```

---

## Choosing a format

| Use case | Recommended format |
|----------|--------------------|
| Human inspection | `table` (default) |
| Scripting / API integration | `json` |
| Spreadsheet / pandas import | `csv` |
| Analytics / Spark / DuckDB | `parquet` (with awareness of caveats above) |
| Lakehouse / columnar predicates on complex types | Wait for epic #673 or cast in the consumer |

**See also**: [CLI Reference](/cqlite/user-docs/cli-reference/) for `--out`, `--format`, and `CQLITE_OUT` flag details.
For agent recipes that use these formats, see [For Agents: Using CQLite](/cqlite/agents-using/).
