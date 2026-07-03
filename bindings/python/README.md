# cqlite-py

Python bindings for [CQLite](https://github.com/pmcfadin/cqlite) - a high-performance library for reading Apache Cassandra 5.0 SSTable files locally, without requiring a running Cassandra cluster.

## Installation

```bash
pip install cqlite-py
```

## Quick Start

```python
import cqlite

# Open a database with schema
with cqlite.open('path/to/sstables', schema='schema.cql') as db:
    # Execute queries
    for row in db.execute('SELECT * FROM keyspace.table LIMIT 10'):
        print(row.to_dict())
```

## Features

- **Zero cluster dependency** - Read SSTable files directly from disk
- **Full CQL type support** - All primitive types, collections, UDTs, and frozen types
- **Memory-efficient streaming** - Iterate over large datasets without loading all rows
- **Thread-safe database handles** - Safe concurrent access from multiple threads
- **Cross-platform** - Linux (x86_64, ARM64), macOS (Intel, Apple Silicon), Windows

## Supported Platforms

| Platform | Architecture | Status |
|----------|--------------|--------|
| Linux | x86_64 | ✅ |
| Linux | ARM64 | ✅ |
| macOS | Intel (x86_64) | ✅ |
| macOS | Apple Silicon | ✅ |
| Windows | x64 | ✅ |

## Requirements

- Python 3.9+
- Cassandra 5.0 SSTable files

## API Reference

### Opening a Database

```python
import cqlite

# Context manager (recommended)
with cqlite.open(data_dir, schema=schema_path) as db:
    # use db...

# Manual management
db = cqlite.open(data_dir, schema=schema_path)
# use db...
db.close()
```

### Executing Queries

```python
# Simple query
results = db.execute('SELECT * FROM keyspace.table')
for row in results:
    print(row.to_dict())

# With LIMIT
for row in db.execute('SELECT name, age FROM users LIMIT 100'):
    print(f"{row['name']}: {row['age']}")

# Access query metadata
print(f"Rows returned: {len(results)}")
print(f"Execution time: {results.execution_time_ms}ms")
print(f"Columns: {[col.name for col in results.columns]}")
```

### Streaming Large Results

For memory-efficient iteration over large datasets:

```python
from cqlite import StreamingConfig

# Configure streaming for memory efficiency
config = StreamingConfig(buffer_size=512, chunk_size=1000)
for row in db.execute_streaming('SELECT * FROM large_table', config=config):
    process(row)

# Track progress
iterator = db.execute_streaming('SELECT * FROM large_table')
for row in iterator:
    if iterator.rows_received % 10000 == 0:
        print(f"Processed {iterator.rows_received} rows")
```

### Configuration Presets

```python
import cqlite

# Built-in presets for common use cases
config = cqlite.memory_optimized()      # 256 MB max memory
config = cqlite.performance_optimized() # 4 GB max memory

# Open database with preset configuration
db = cqlite.open('path/to/data', schema='schema.cql', config='memory_optimized')

# Validate custom configuration
custom_config = {'memory': {'max_memory': 536870912}}  # 512 MB
cqlite.validate_config(custom_config)
```

### Error Handling

```python
import cqlite

try:
    with cqlite.open('path/to/data', schema='schema.cql') as db:
        result = db.execute('SELECT * FROM keyspace.table')
        for row in result:
            print(row.to_dict())
except cqlite.ParseError as e:
    print(f"Query syntax error: {e}")
except cqlite.QueryError as e:
    print(f"Query execution failed: {e}")
except cqlite.SchemaError as e:
    print(f"Schema validation failed: {e}")
except IOError as e:
    print(f"File not found: {e}")
except RuntimeError as e:
    print(f"Database already closed: {e}")
```

**Exception Hierarchy:**

```
CqliteError (base exception)
├── SchemaError   - Schema parsing or validation failures
├── QueryError    - Query execution failures
└── ParseError    - CQL syntax errors

Built-in exceptions also used:
├── IOError       - File system errors
├── ValueError    - Invalid configuration
├── RuntimeError  - Invalid state (e.g., database closed)
└── MemoryError   - Memory allocation failures
```

## Type Conversions

CQL types are automatically converted to Python native types:

| CQL Type | Python Type |
|----------|-------------|
| `text`, `varchar` | `str` |
| `int`, `bigint`, `smallint`, `tinyint` | `int` |
| `float`, `double` | `float` |
| `boolean` | `bool` |
| `blob` | `bytes` |
| `timestamp` | `datetime.datetime` |
| `date` | `datetime.date` |
| `time` | `int` (nanoseconds since midnight, lossless) |
| `duration` | `cqlite.Duration` (exact `months` / `days` / `nanos`) |
| `uuid`, `timeuuid` | `uuid.UUID` |
| `inet` | `ipaddress.IPv4Address` or `IPv6Address` |
| `decimal` | `decimal.Decimal` |
| `varint` | `int` (arbitrary precision) |
| `list<T>` | `list` |
| `set<T>` | `frozenset` |
| `map<K,V>` | `dict` |
| `tuple<...>` | `tuple` |
| `frozen<T>` | Unwrapped inner type |
| UDT | `dict` with `_type` and `_keyspace` keys |

## Write Operations

CQLite v0.9.0 adds write support to the Python bindings. Open the database with
`writable=True` and a `write_dir` to enable write operations.

```python
import cqlite

with cqlite.open(
    'path/to/sstables',
    schema='schema.cql',
    writable=True,
    write_dir='/tmp/my-writes',
) as db:
    # Write rows via CQL INSERT, UPDATE, or DELETE
    db.execute(
        "INSERT INTO test_basic.simple_table (id, name, age) "
        "VALUES (11111111-1111-1111-1111-111111111111, 'Alice', 30)"
    )
    db.execute(
        "UPDATE test_basic.simple_table SET age = 31 "
        "WHERE id = 11111111-1111-1111-1111-111111111111"
    )

    # Flush the in-memory write buffer (memtable) to an SSTable on disk.
    # Returns the path to the flushed Data.db file.
    path = db.flush_run()
    print(f'Flushed to: {path}')

    # Run background compaction within a time budget
    report = db.maintenance_step(budget_ms=100)
    print(f'Merged {report.rows_merged} rows in {report.time_spent_ms:.1f} ms')
    if report.pending_compaction:
        print('More compaction work available')

    # Inspect write statistics
    stats = db.write_stats
    print(f'Memtable size: {stats.memtable_size_bytes} bytes')
    print(f'Total flushed: {stats.total_written_bytes} bytes')
```

### Write API

| Method / Property | Description |
|-------------------|-------------|
| `db.execute(cql)` | Execute a CQL INSERT, UPDATE, or DELETE statement |
| `db.flush_run()` | Flush memtable to SSTable; returns the Data.db path or `""` if memtable was empty |
| `db.maintenance_step(budget_ms)` | Run STCS compaction for up to `budget_ms` milliseconds; returns `MaintenanceReport` |
| `db.write_stats` | `WriteStats` property: `memtable_size_bytes`, `memtable_row_count`, `total_written_bytes`, `l0_sstable_count` |

### Known Limitations

- Counter columns cannot be written — `execute()` raises `CqliteError` for
  counter mutations.
- Concurrent queries on the same handle may need a warm-up query first (Issue
  #311).

See [docs/write-support-limitations.md](../../docs/write-support-limitations.md)
for the full limitations reference.

## Resources

- [Acceptance Testing Notebook](notebooks/acceptance-testing.ipynb) - Interactive examples and validation
- [Type Stubs](python/cqlite/__init__.pyi) - Complete API type hints for IDE support
- [Main Project README](../../README.md) - CQLite project overview and documentation
- [Write Support Guide](../../docs/write-support.md) - Detailed write documentation
- [Issue Tracker](https://github.com/pmcfadin/cqlite/issues) - Report bugs or request features

## License

MIT OR Apache-2.0

## Links

- [GitHub Repository](https://github.com/pmcfadin/cqlite)
- [PyPI Package](https://pypi.org/project/cqlite-py/)
- [Documentation](https://github.com/pmcfadin/cqlite/tree/main/docs)
