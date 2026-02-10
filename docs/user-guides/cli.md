# CQLite CLI User Guide

## Quick Start with Cassandra 5+ SSTable Files

CQLite is designed to read and query **Cassandra 5.0+ SSTable files** directly from the filesystem. This guide shows you how to use CQLite with real Cassandra data.

> **⚠️ Important**: CQLite currently supports **Cassandra 5.0+ only**. Earlier versions (3.11, 4.0) are not supported in this release.

## Prerequisites

1. **Cassandra 5.0+ SSTable files** in the new format
2. **Schema files** in JSON format for your tables  
3. **CQLite binary** (built from source)

## SSTable File Structure

Cassandra 5 creates SSTable directories with this structure:
```
keyspace/
└── table_name-{uuid}/
    ├── nb-1-big-Data.db
    ├── nb-1-big-Index.db  
    ├── nb-1-big-Summary.db
    ├── nb-1-big-Statistics.db
    ├── nb-1-big-Filter.db
    ├── nb-1-big-CompressionInfo.db
    └── nb-1-big-TOC.txt
```

## Creating Test Data (Cassandra 5 Only)

First, create test data in a Cassandra 5.0+ cluster:

```sql
-- Connect to Cassandra 5.0+
CREATE KEYSPACE test_keyspace 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE test_keyspace;

CREATE TABLE users (
    id UUID PRIMARY KEY,
    name TEXT,
    email TEXT,
    created_at TIMESTAMP
);

INSERT INTO users (id, name, email, created_at) 
VALUES (uuid(), 'John Doe', 'john@example.com', toTimestamp(now()));

INSERT INTO users (id, name, email, created_at)
VALUES (uuid(), 'Jane Smith', 'jane@example.com', toTimestamp(now()));

-- Force SSTable creation
FLUSH;
```

## Schema File Creation

Create a `schema.json` file for your table:

```json
{
  "keyspace": "test_keyspace",
  "table": "users",
  "columns": [
    {
      "name": "id",
      "type": "UUID",
      "kind": "partition_key"
    },
    {
      "name": "name", 
      "type": "TEXT",
      "kind": "regular"
    },
    {
      "name": "email",
      "type": "TEXT", 
      "kind": "regular"
    },
    {
      "name": "created_at",
      "type": "TIMESTAMP",
      "kind": "regular"
    }
  ]
}
```

## Basic Commands

### 1. View SSTable Information

```bash
# Get basic information about an SSTable directory
cqlite info /path/to/cassandra/data/test_keyspace/users-{uuid}

# Get detailed information
cqlite info /path/to/cassandra/data/test_keyspace/users-{uuid} --detailed
```

### 2. Read SSTable Data

```bash
# Read all data from SSTable 
cqlite read /path/to/cassandra/data/test_keyspace/users-{uuid} \
  --schema schema.json

# Limit output to 10 rows
cqlite read /path/to/cassandra/data/test_keyspace/users-{uuid} \
  --schema schema.json \
  --limit 10

# Skip first 5 rows, show next 10
cqlite read /path/to/cassandra/data/test_keyspace/users-{uuid} \
  --schema schema.json \
  --skip 5 \
  --limit 10
```

### 3. Execute CQL Queries

```bash
# Execute SELECT queries against SSTable data
cqlite select /path/to/cassandra/data/test_keyspace/users-{uuid} \
  --schema schema.json \
  "SELECT * FROM users LIMIT 5"

# Query with WHERE clause (partition key only for SSTable)
cqlite select /path/to/cassandra/data/test_keyspace/users-{uuid} \
  --schema schema.json \
  "SELECT name, email FROM users WHERE id = '12345678-1234-5678-9abc-123456789abc'"

# Count rows
cqlite select /path/to/cassandra/data/test_keyspace/users-{uuid} \
  --schema schema.json \
  "SELECT COUNT(*) FROM users"
```

### 4. Interactive REPL Mode

```bash
# Start interactive REPL
cqlite

# Or start REPL with a default database
cqlite --database /path/to/local.db
```

In REPL mode:
```sql
-- Configure data directory for SSTable access
cqlite> :config data-dir /path/to/cassandra/data

-- List available tables (scans filesystem)
cqlite> :tables

-- Show table schema
cqlite> :describe test_keyspace.users

-- Execute queries
cqlite> SELECT * FROM users LIMIT 5;

-- Get help
cqlite> :help

-- Exit
cqlite> :quit
```

## Output Formats

CQLite supports multiple output formats:

```bash
# Table format (default)
cqlite read /path/to/sstable --schema schema.json --format table

# CSV format
cqlite read /path/to/sstable --schema schema.json --format csv

# JSON format  
cqlite read /path/to/sstable --schema schema.json --format json
```

## Performance Options

```bash
# Enable parallel processing for large tables
cqlite read /path/to/sstable --schema schema.json --parallel

# Set page size for large result sets
cqlite read /path/to/sstable --schema schema.json --page-size 100

# Control memory usage (in MB)
cqlite read /path/to/sstable --schema schema.json --max-memory-mb 256

# Increase I/O buffer size
cqlite read /path/to/sstable --schema schema.json --buffer-size 16384
```

## Advanced Features

### Export Data
```bash
# Export SSTable data to CSV
cqlite export "SELECT * FROM users" output.csv \
  --format csv \
  --sstable /path/to/sstable \
  --schema schema.json

# Export to JSON
cqlite export "SELECT * FROM users" output.json \
  --format json \
  --sstable /path/to/sstable \
  --schema schema.json
```

### Batch Operations
```bash
# Execute multiple queries from file
echo "SELECT * FROM users LIMIT 5;" > queries.sql
echo "SELECT COUNT(*) FROM users;" >> queries.sql

# Run in REPL mode
cqlite
cqlite> :source queries.sql
```

## Troubleshooting

### Common Issues

1. **"SSTable not found"**
   - Verify the path points to the SSTable directory (not individual files)
   - Ensure the directory contains `nb-*-big-Data.db` files

2. **"Schema file invalid"**
   - Validate JSON syntax in your schema file
   - Ensure column types match Cassandra 5 data types
   - Verify keyspace and table names match

3. **"Unsupported SSTable format"**
   - CQLite only supports Cassandra 5.0+ format
   - Use `--auto-detect` flag to attempt automatic format detection

4. **"Memory limit exceeded"**
   - Use `--page-size` to limit result set size
   - Increase `--max-memory-mb` if you have available RAM
   - Consider using WHERE clauses to filter data

### Getting Help

```bash
# Command help
cqlite --help
cqlite read --help

# REPL help
cqlite
cqlite> :help
cqlite> :help commands
cqlite> :help troubleshooting
```

## Supported Data Types (Cassandra 5)

| CQL Type | Support | Notes |
|----------|---------|-------|
| UUID | ✅ | Full support |
| TEXT | ✅ | Full support |
| INT | ✅ | Full support |
| BIGINT | ✅ | Full support |
| FLOAT | ✅ | Full support |
| DOUBLE | ✅ | Full support |
| BOOLEAN | ✅ | Full support |
| TIMESTAMP | ✅ | Full support |
| TIMEUUID | ✅ | Full support |
| BLOB | ✅ | Full support |
| DECIMAL | ✅ | Full support |
| VARINT | ✅ | Full support |
| INET | ✅ | Full support |
| LIST | ✅ | Collections supported |
| SET | ✅ | Collections supported |
| MAP | ✅ | Collections supported |
| UDT | ✅ | User-defined types |
| TUPLE | ✅ | Tuple types |
| COUNTER | ✅ | Counter columns |

## Limitations

1. **Single SSTable**: Queries operate on one SSTable directory at a time
2. **Partition key queries**: WHERE clauses work best with partition keys
3. **Cassandra 5+ only**: Earlier versions are not supported
4. **Local files only**: No network connectivity to live Cassandra clusters

## Write Support (M5)

CQLite supports write operations when built with the `write-support` feature flag:

```bash
# Build with write support
cargo build --package cqlite-cli --features write-support

# Enable write mode
cqlite --writable --write-dir /path/to/write-dir \
  --schema schema.json \
  --mutation '{"table":{"keyspace":"ks","table":"tbl"},...}'

# Available write subcommands
cqlite maintenance --budget-ms 100 --writable --write-dir /path  # Run compaction
cqlite write-stats --writable --write-dir /path                   # Show statistics
cqlite export-sstable /output --writable --write-dir /path        # Export SSTables
```

For full write support documentation, see [CLI Usage Examples](../../cqlite-cli/CLI_USAGE_EXAMPLES.md#write-support-m5).

## Next Steps

- Explore the [REPL commands](../cli/repl-commands.md) for interactive data exploration
- Learn about [schema file formats](../cli/schema-formats.md) for complex data types
- Review [performance tuning](../cli/performance.md) for large datasets

---

> **Note**: This guide covers the currently implemented functionality. Some advanced features may require the latest development build.