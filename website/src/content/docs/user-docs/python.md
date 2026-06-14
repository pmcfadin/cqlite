---
title: Python Bindings
description: Install and use the cqlite Python package to query Cassandra 5.0 SSTables without a cluster.
sidebar:
  label: Python Bindings
  order: 5
---

The `cqlite` Python package provides direct access to Cassandra 5.0 SSTable files
from Python, without requiring a running Cassandra cluster. It exposes a simple
open/execute/stream API built on native Python types.

## Installation

### From PyPI (recommended)

```bash
pip install cqlite-py
```

Requires Python 3.9+. Pre-built wheels are available for Linux (x86_64, ARM64),
macOS (Intel and Apple Silicon), and Windows (x64).

### From source (development)

```bash
# Install Rust 1.85+ and maturin first
pip install maturin
git clone https://github.com/pmcfadin/cqlite
cd cqlite/bindings/python
maturin develop
```

## Quick start

```python
import cqlite

# Open an SSTable directory with a CQL schema
with cqlite.open("path/to/sstables", schema="path/to/schema.cql") as db:
    result = db.execute("SELECT * FROM test_basic.simple_table LIMIT 5")
    print(f"Rows returned: {len(result)}")
    print(f"Execution time: {result.execution_time_ms}ms")
    for row in result:
        print(row.to_dict())
```

Running against the test datasets produces output like:

```
Rows returned: 5
Execution time: 74ms
{'id': UUID('0023ece7-7c4e-4705-9068-d1a59ec5fe19'), 'name': 'Debbie Soto',
 'age': 79, 'account_balance': Decimal('69799.73'), 'active': True,
 'created': datetime.datetime(2025, 10, 6, 1, 12, 5, 926000, tzinfo=datetime.timezone.utc),
 ...}
```

## Opening a database

`cqlite.open()` is the main entry point. It returns a `Database` that supports
the context manager protocol.

```python
import cqlite

# Context manager — close() called automatically on exit
with cqlite.open("data/sstables", schema="schema.cql") as db:
    ...

# Manual lifecycle
db = cqlite.open("data/sstables", schema="schema.cql")
try:
    ...
finally:
    db.close()  # idempotent; safe to call more than once
```

**Parameters**

| Parameter | Type | Description |
|-----------|------|-------------|
| `path` | `str` or `Path` | Directory containing SSTable files |
| `schema` | `str` or `Path` (optional) | Path to a `.cql` schema file |
| `config` | `dict` or `str` (optional) | Configuration dict or preset name |
| `writable` | `bool` (default `False`) | Enable write support (INSERT/UPDATE/DELETE) |
| `write_dir` | `str` or `Path` | Directory for WAL and flushed SSTables; required when `writable=True` |

## Executing queries

`db.execute()` runs a CQL SELECT (or DML when `writable=True`) and returns a
`QueryResult` containing all rows.

```python
# Iterate directly over the result
for row in db.execute("SELECT id, name, age FROM test_basic.simple_table LIMIT 10"):
    print(f"{row['name']}: age {row['age']}")

# Access metadata
result = db.execute("SELECT * FROM test_basic.simple_table LIMIT 100")
print(f"Rows: {len(result)}")
print(f"Time: {result.execution_time_ms}ms")
print(f"Columns: {[col.name for col in result.columns]}")
```

## Type conversions

CQL types are mapped to native Python types automatically:

| CQL Type | Python Type |
|----------|-------------|
| `text`, `varchar`, `ascii` | `str` |
| `int`, `smallint`, `tinyint` | `int` |
| `bigint`, `varint` | `int` (arbitrary precision) |
| `float` | `float` |
| `double` | `float` |
| `boolean` | `bool` |
| `blob` | `bytes` |
| `timestamp` | `datetime.datetime` (UTC-aware) |
| `date` | `datetime.date` |
| `time` | `datetime.time` |
| `duration` | `datetime.timedelta` |
| `uuid`, `timeuuid` | `uuid.UUID` |
| `inet` | `ipaddress.IPv4Address` or `IPv6Address` |
| `decimal` | `decimal.Decimal` |
| `counter` | `int` |
| `list<T>` | `list` |
| `set<T>` | `frozenset` |
| `map<K,V>` | `dict` |
| `tuple<...>` | `tuple` |
| `frozen<T>` | Unwrapped inner type |
| UDT | `dict` with `_type` and `_keyspace` keys |
| `null` | `None` |

## Streaming large result sets

For tables with many rows, use `db.execute_streaming()` instead of
`db.execute()`. It yields rows one at a time, keeping memory usage bounded
regardless of result size.

```python
import cqlite
from cqlite import StreamingConfig

# Default streaming (buffers up to 1024 rows, ~11 MB peak)
with cqlite.open("data/sstables", schema="schema.cql") as db:
    for row in db.execute_streaming("SELECT * FROM test_basic.simple_table"):
        process(row)

# Custom config for memory-constrained environments
config = StreamingConfig(buffer_size=256, chunk_size=2000)
with cqlite.open("data/sstables", schema="schema.cql") as db:
    iterator = db.execute_streaming(
        "SELECT * FROM test_basic.simple_table", config=config
    )
    for row in iterator:
        if iterator.rows_received % 1000 == 0:
            print(f"Processed {iterator.rows_received} rows")
```

### StreamingConfig presets

`StreamingConfig` takes two parameters controlling memory usage:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `buffer_size` | `1024` | Rows to hold in-flight (~1 MB with typical rows) |
| `chunk_size` | `10000` | Rows fetched per storage read (~10 MB per chunk) |

For rows with large blobs, reduce both values proportionally.

You can also open a database with a built-in memory preset:

```python
# 256 MB memory limit
db = cqlite.open("data/sstables", schema="schema.cql", config="memory_optimized")

# 4 GB memory limit
db = cqlite.open("data/sstables", schema="schema.cql", config="performance_optimized")
```

## Exporting to Parquet

`db.export_parquet()` writes query results straight to a Parquet file using
the embeddable core writer ([epic #682](https://github.com/pmcfadin/cqlite/issues/682)).
The query streams, so arbitrarily large result sets export within bounded
memory, and the GIL is released for the duration of the export.

```python
with cqlite.open("data/sstables", schema="schema.cql") as db:
    rows = db.export_parquet(
        "SELECT * FROM test_basic.simple_table",
        "/tmp/simple_table.parquet",
        row_group_size=10000,    # rows per Parquet row group (default)
        compression="snappy",    # "snappy" (default), "zstd", or "none"
    )
    print(f"Exported {rows} rows")
```

The output uses the high-fidelity Arrow type mapping — typed lists, maps,
structs for UDTs/tuples, `Decimal128`, `Date32`, `Time64`, UUID extension —
see [Output Formats](/cqlite/user-docs/output-formats/) for the full table.

Invalid options raise `ValueError`; file and encoding failures raise
`IOError`; query failures raise the usual `QueryError`/`ParseError`.

## Error handling

```python
import cqlite

try:
    with cqlite.open("data/sstables", schema="schema.cql") as db:
        for row in db.execute("SELECT * FROM test_basic.simple_table"):
            print(row.to_dict())
except cqlite.ParseError as e:
    print(f"CQL syntax error: {e}")
except cqlite.QueryError as e:
    print(f"Query execution failed: {e}")
except cqlite.SchemaError as e:
    print(f"Schema validation failed: {e}")
except IOError as e:
    print(f"File not found: {e}")
except RuntimeError as e:
    print(f"Database already closed: {e}")
```

**Exception hierarchy**

```
CqliteError              # base exception for all CQLite errors
├── SchemaError          # schema parsing or validation failures
├── QueryError           # query execution failures
└── ParseError           # CQL syntax errors

Built-in exceptions also used:
├── IOError              # file system errors
├── ValueError           # invalid configuration
├── RuntimeError         # invalid state (e.g. database closed)
└── MemoryError          # memory allocation failures
```

## Write support

Open the database with `writable=True` and a `write_dir` to enable INSERT,
UPDATE, and DELETE statements.

```python
import cqlite

with cqlite.open(
    "path/to/sstables",
    schema="schema.cql",
    writable=True,
    write_dir="/tmp/my-writes",
) as db:
    db.execute(
        "INSERT INTO test_basic.simple_table (id, name, age) "
        "VALUES (11111111-1111-1111-1111-111111111111, 'Alice', 30)"
    )

    # Flush in-memory writes to an SSTable on disk
    path = db.flush_run()
    print(f"Flushed to: {path}")

    # Run incremental compaction within a time budget
    report = db.maintenance_step(budget_ms=100)
    print(f"Merged {report.rows_merged} rows in {report.time_spent_ms:.1f} ms")
    if report.pending_compaction:
        print("More compaction work available")

    # Inspect write engine state
    stats = db.write_stats
    print(f"Memtable: {stats.memtable_size} bytes, {stats.memtable_rows} rows")
```

**Known write limitations**

- Counter columns cannot be written; `execute()` raises `CqliteError` for counter mutations.
- Concurrent queries on the same handle require a warm-up query first (see issue #311).

## Thread safety

`Database` handles are thread-safe via `Arc<Database>`. All blocking operations
(`open`, `execute`, `execute_streaming`, `close`) release the Python GIL so
other threads can run during I/O.

Each thread should create its own `StreamingIterator` via `execute_streaming()`.
Do not share a single iterator across threads.

## API reference summary

| Method / property | Description |
|-------------------|-------------|
| `cqlite.open(path, ...)` | Open a database; returns `Database` |
| `db.execute(query)` | Run a CQL query; returns `QueryResult` |
| `db.execute_streaming(query, config?)` | Memory-bounded row iteration; returns `StreamingIterator` |
| `db.export_parquet(query, path, *, row_group_size?, compression?)` | Stream query results to a Parquet file; returns row count |
| `db.prepare(query)` | Parse and plan a query; returns `PreparedStatement` |
| `db.stats()` | Storage and memory metrics; returns `DatabaseStats` |
| `db.flush_run()` | Flush memtable to SSTable; returns Data.db path |
| `db.maintenance_step(budget_ms)` | Incremental compaction; returns `MaintenanceReport` |
| `db.write_stats` | Write engine snapshot; returns `WriteStats` |
| `db.close()` | Release resources (idempotent) |
| `db.is_closed` | `True` if the connection is closed |

Full type stubs are in
[`bindings/python/python/cqlite/__init__.pyi`](https://github.com/pmcfadin/cqlite/blob/main/bindings/python/python/cqlite/__init__.pyi).

## Resources

- [GitHub Repository](https://github.com/pmcfadin/cqlite)
- [PyPI Package](https://pypi.org/project/cqlite-py/)
- [Type Stubs](https://github.com/pmcfadin/cqlite/blob/main/bindings/python/python/cqlite/__init__.pyi)
- [Query from Python recipe](/cqlite/agents-using/query-python/) — copy-pasteable agent recipe with expected output shapes
