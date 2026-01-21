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
- **Streaming results** - Memory-efficient iteration over large datasets
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
```

### Streaming Large Results

```python
from cqlite import StreamingConfig

# Configure streaming for memory efficiency
config = StreamingConfig(batch_size=1000)
for row in db.stream('SELECT * FROM large_table', config=config):
    process(row)
```

## License

MIT OR Apache-2.0

## Links

- [GitHub Repository](https://github.com/pmcfadin/cqlite)
- [Documentation](https://github.com/pmcfadin/cqlite/tree/main/bindings/python)
- [Issue Tracker](https://github.com/pmcfadin/cqlite/issues)
