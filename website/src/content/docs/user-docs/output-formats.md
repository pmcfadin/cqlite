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

The writer is embeddable ([epic #682](https://github.com/pmcfadin/cqlite/issues/682)): it lives in `cqlite-core` behind the off-by-default `parquet` cargo feature, and is exposed without the CLI via `db.export_parquet(...)` in the [Python bindings](/cqlite/user-docs/python/) and `db.exportParquet(...)` in the [Node.js bindings](/cqlite/user-docs/nodejs/).

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

### Type mapping

**CQLite's Parquet output preserves nested and high-precision CQL types** ([epic #673](https://github.com/pmcfadin/cqlite/issues/673)). When a schema is provided (the normal case — CQLite requires one to decode SSTables), query results carry the authoritative schema `CqlType` and the writer builds a faithful Arrow schema, recursively:

| CQL type | Arrow/Parquet type | Notes |
|----------|--------------------|-------|
| `boolean` | `Boolean` | |
| `tinyint` / `smallint` / `int` / `bigint` | `Int8` / `Int16` / `Int32` / `Int64` | |
| `float` / `double` | `Float32` / `Float64` | |
| `text` / `varchar` / `ascii` | `Utf8` | |
| `blob` | `Binary` | |
| `timestamp` | `Timestamp(Millisecond, UTC)` | |
| `date` | `Date32` | Signed days since 1970-01-01 |
| `time` | `Time64(Nanosecond)` | Nanoseconds since midnight |
| `decimal` | `Decimal128(38, 9)` | Fixed column scale of 9; values are rescaled with checked arithmetic; overflow or precision > 38 is a deterministic error, never silent truncation |
| `varint` | `Decimal128(38, 0)` | Values longer than 38 digits are a deterministic error |
| `duration` | `Utf8` (CQL text form, e.g. `"1mo2d3ns"`) | The `parquet` crate (v53) cannot write Arrow `Interval(MonthDayNano)`; text fallback until upstream support lands |
| `uuid` / `timeuuid` | `FixedSizeBinary(16)` + `ARROW:extension:name=arrow.uuid` | Carries the canonical Arrow UUID extension annotation |
| `inet` | `Utf8` (canonical text, e.g. `"192.168.1.1"`) | Deliberate: no standard Arrow inet type; text is most portable |
| `counter` | `Int64` | |
| `list<T>` | `List<T>` | Element type mapped recursively |
| `set<T>` | `List<T>` | Arrow has no set type; exported as `List` |
| `map<K,V>` | `Map<K, V>` | Typed keys (non-nullable) and values (nullable), mapped recursively |
| `tuple<A,B,…>` | `Struct(field_0, field_1, …)` | Positional field names, per-position types |
| UDT | `Struct` with the UDT's field names | Missing/unset fields become null |
| `frozen<T>` | Same as `T` | Fully transparent at every nesting level |

Nesting composes: `list<frozen<list<int>>>` exports as `List<List<Int32>>`, `map<text, frozen<list<int>>>` as `Map<Utf8, List<Int32>>`, and a UDT containing a collection field exports as a `Struct` with a typed `List` child.

Null and empty collections are preserved distinctly, as are null elements inside collections. The streaming Parquet writer produces the same schema and values as the batch writer.

**Legacy fallback:** if a column has no schema-sourced `CqlType` (no schema available), the writer falls back to the older flat mapping, where collections and UDTs are stringified.

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
| Analytics / Spark / DuckDB | `parquet` |
| Lakehouse / columnar predicates on complex types | `parquet` (typed lists, maps, and structs — see table above) |

**See also**: [CLI Reference](/cqlite/user-docs/cli-reference/) for `--out`, `--format`, and `CQLITE_OUT` flag details.
For agent recipes that use these formats, see [For Agents: Using CQLite](/cqlite/agents-using/).
