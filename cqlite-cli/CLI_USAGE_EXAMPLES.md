# CQLite CLI Usage Examples (M2/M3)

This document provides comprehensive usage examples for the CQLite CLI in M2 milestone, featuring interactive REPL and one-shot modes for reading Cassandra 5 SSTables with cqlsh-compatible syntax.

## Table of Contents

- [Introduction](#introduction)
- [Environment Variables](#environment-variables)
- [One-Shot Mode Examples](#one-shot-mode-examples)
- [REPL Mode Examples](#repl-mode-examples)
- [Output Formats](#output-formats)
- [Exporting Data (M3)](#exporting-data-m3)
- [Write Support (M5)](#write-support-m5)
- [Command Reference](#command-reference)

---

## Introduction

CQLite M2 delivers a cqlsh-compatible experience for querying local Cassandra 5 SSTable data. The CLI supports:

- **One-shot mode**: Execute queries or scripts from the command line
- **Interactive REPL**: Explore data with familiar cqlsh-style commands
- **Multiple output formats**: table (cqlsh-compatible), JSON, CSV, and Parquet
- **File export**: Export data to CSV, JSON, Parquet, or CQL INSERT statements
- **Schema-aware reading**: Load CQL or JSON schema definitions
- **Status & health commands**: Monitor schema-data synchronization
- **Ingestion model**: Provide `--schema` and `--data-dir` together to trigger schema loading + dataset discovery for query execution

All examples in this guide use the validated test data paths from the CQLite repository.

**Note**: Timestamps are displayed in UTC for M2 milestone. Local timezone support is planned for M3.

---

## Environment Variables

CQLite supports the following environment variables to simplify configuration (Issue #126):

| Variable | Description | Example |
|----------|-------------|---------|
| `CQLITE_DATA_DIR` | Cassandra data directory root | `/Users/patrick/local_projects/cqlite/test-data/datasets` |
| `CQLITE_SCHEMA` | Schema file path(s), comma-separated | `/Users/patrick/local_projects/cqlite/test-data/schemas` |
| `CQLITE_LIMIT` | Maximum rows for queries | `100` |
| `CQLITE_PAGE_SIZE` | Page size for pagination | `50` |
| `CQLITE_NO_COLOR` | Disable colored output | `1`, `true`, `yes`, `on` |
| `CQLITE_OUT` | Output format | `table`, `json`, `csv` |
| `CQLITE_WRITABLE` | Enable write mode | `1`, `true`, `yes`, `on` |
| `CQLITE_WRITE_DIR` | Directory for write operations | `/path/to/write-dir` |

### Example Usage

```bash
# Set environment variables for all sessions
export CQLITE_DATA_DIR=/Users/patrick/local_projects/cqlite/test-data/datasets
export CQLITE_SCHEMA=/Users/patrick/local_projects/cqlite/test-data/schemas

# Now run queries without specifying paths
cqlite -e "SELECT * FROM ks.users LIMIT 5"

# Override with specific format
CQLITE_OUT=json cqlite -e "SELECT id, name FROM ks.users LIMIT 3"

# Disable color for piping to files
CQLITE_NO_COLOR=1 cqlite -e "SELECT * FROM ks.users LIMIT 10" > output.txt
```

**Precedence**: CLI flags > environment variables > config file > defaults

---

## One-Shot Mode Examples

One-shot mode executes queries or scripts without entering the interactive REPL. This is ideal for automation, scripting, and quick data access.

### Basic Query Execution

```bash
# Execute a simple query with table output (cqlsh-compatible)
cqlite --schema /Users/patrick/local_projects/cqlite/test-data/schemas \
       --data-dir /Users/patrick/local_projects/cqlite/test-data/datasets \
       -e "SELECT * FROM ks.users LIMIT 5" --out table

# Short form with environment variables
export CQLITE_DATA_DIR=/Users/patrick/local_projects/cqlite/test-data/datasets
export CQLITE_SCHEMA=/Users/patrick/local_projects/cqlite/test-data/schemas
cqlite -e "SELECT * FROM ks.users LIMIT 5"
```

### JSON Output

```bash
# Query with JSON output
cqlite --schema /Users/patrick/local_projects/cqlite/test-data/schemas \
       --data-dir /Users/patrick/local_projects/cqlite/test-data/datasets \
       -e "SELECT id, name FROM ks.users LIMIT 3" --out json

# Example output:
# [
#   {"id": "8b6c8a96-5f5a-4f7e-a6a8-2b5a3a3f1c01", "name": "Alice Wong"},
#   {"id": "2a1dc9b7-2f1f-4db2-8d1f-7c0a4d4f9b12", "name": "Bob Smith"},
#   {"id": "4c7e2f90-1b33-4a6a-9e1c-9d4e8a2f3c45", "name": "Carol Chen"}
# ]
```

### CSV Output

```bash
# Query with CSV output
cqlite --schema /Users/patrick/local_projects/cqlite/test-data/schemas \
       --data-dir /Users/patrick/local_projects/cqlite/test-data/datasets \
       -e "SELECT id, email FROM ks.users LIMIT 3" --out csv

# Example output:
# id,email
# 8b6c8a96-5f5a-4f7e-a6a8-2b5a3a3f1c01,alice@example.com
# 2a1dc9b7-2f1f-4db2-8d1f-7c0a4d4f9b12,bob@example.com
# 4c7e2f90-1b33-4a6a-9e1c-9d4e8a2f3c45,carol@example.com
```

### Execute Statements from File

```bash
# Run a script of CQL statements (semicolon-terminated)
cqlite --schema /Users/patrick/local_projects/cqlite/test-data/schemas \
       --data-dir /Users/patrick/local_projects/cqlite/test-data/datasets \
       -f statements.cql --out table

# Example statements.cql:
# USE ks;
# SELECT * FROM users LIMIT 5;
# SELECT * FROM orders WHERE user_id = 8b6c8a96-5f5a-4f7e-a6a8-2b5a3a3f1c01 LIMIT 10;
```

### Loading Multiple Schema Sources

```bash
# Load schemas from multiple files/directories (order defines precedence)
cqlite --schema /Users/patrick/local_projects/cqlite/test-data/schemas/ks.cql \
       --schema /Users/patrick/local_projects/cqlite/test-data/schemas/system.json \
       --data-dir /Users/patrick/local_projects/cqlite/test-data/datasets \
       -e "SELECT * FROM ks.users LIMIT 5"
```

### Pagination and Limits

```bash
# Limit rows returned
cqlite --schema /Users/patrick/local_projects/cqlite/test-data/schemas \
       --data-dir /Users/patrick/local_projects/cqlite/test-data/datasets \
       --limit 10 \
       -e "SELECT * FROM ks.users"

# Set page size for display
cqlite --schema /Users/patrick/local_projects/cqlite/test-data/schemas \
       --data-dir /Users/patrick/local_projects/cqlite/test-data/datasets \
       --page-size 25 \
       -e "SELECT * FROM ks.events LIMIT 100"
```

### Using Configuration Files

```bash
# Load configuration from file (TOML/YAML/JSON)
cqlite --config /Users/patrick/local_projects/cqlite/config.toml \
       -e "SELECT * FROM ks.users LIMIT 5"

# Example config.toml:
# data_directory = "/Users/patrick/local_projects/cqlite/test-data/datasets"
# schema_paths = ["/Users/patrick/local_projects/cqlite/test-data/schemas"]
# default_keyspace = "ks"
#
# [repl]
# page_size = 50
# enable_history = true
#
# [output]
# colors = true
```

---

## REPL Mode Examples

The interactive REPL provides a cqlsh-compatible experience for exploring and querying data. Launch with `cqlite repl` or simply `cqlite` (default).

### Complete REPL Session Example

This example demonstrates a full REPL workflow from configuration to querying:

```text
$ cqlite

cqlite> :config data-dir /var/lib/cassandra/data
Success: Data directory set to: /var/lib/cassandra/data

cqlite> :schema load ./schemas
Loaded 3 schema files (2 CQL, 1 JSON)
Keyspaces: ks
Tables: ks.users, ks.orders, ks.events

cqlite> :status
Data Directory: /var/lib/cassandra/data
Discovery: 2 keyspaces, 7 tables
Schema Coverage:
  - tables with schema: 6
  - tables missing schema: 1  (e.g., ks.audit_logs)
  - schemas without data: 0
Cassandra Version: detected 5.0 (configured: 5.0)
Status: Green (86%+ coverage; no critical errors)

cqlite> :keyspaces
Keyspaces:
  - system (5 tables)
  - ks (2 tables)

cqlite> USE ks;

cqlite> :tables
Tables (ks):
  - users
  - orders

cqlite> DESCRIBE ks.users;
CREATE TABLE ks.users (
    id uuid PRIMARY KEY,
    name text,
    email text,
    created_at timestamp
) WITH compaction = { ... } AND compression = { ... };

cqlite> SELECT id, name, email FROM users LIMIT 5;
 id                                   | name        | email
--------------------------------------+-------------+-----------------------
 8b6c8a96-5f5a-4f7e-a6a8-2b5a3a3f1c01 | Alice Wong  | alice@example.com
 2a1dc9b7-2f1f-4db2-8d1f-7c0a4d4f9b12 | Bob Smith   | bob@example.com
 4c7e2f90-1b33-4a6a-9e1c-9d4e8a2f3c45 | Carol Chen  | carol@example.com
 9f2d1a3b-7c2e-4a5b-8f1e-3d4c5b6a7e89 | Dan Jones   | dan@example.com
 1e2f3a4b-5c6d-7e8f-9012-3456789abcde | Eve Adams   | eve@example.com

(5 rows)

cqlite> :health
Checks:
  - data-dir readable: OK
  - schema parse: OK (3 files)
  - schema/data sync: 6/7 tables covered
  - compression codecs: LZ4, Snappy available
  - config: page-size=50, timing=off
Tips:
  - Missing schema for: ks.audit_logs (use :schema load <file>)

cqlite> :quit
```

### REPL Meta-Commands Reference

#### Session & Help
```text
:help [topic]     # Show help (general or topic-specific)
:quit             # Exit REPL (aliases: :exit, :q)
:clear            # Clear screen (alias: :cls)
:history          # Show command history
```

#### Configuration
```text
:config                        # Show effective configuration
:config data-dir <PATH>        # Set data directory for session
:config page-size <N>          # Set pagination size
:config timing on|off          # Enable/disable query timing
:config save [FILE]            # Save current config to file
```

#### Schema Management
```text
:schema list                   # List loaded schema sources
:schema load <FILE|DIR>        # Load CQL or JSON schemas
:schema unload <NAME>|all      # Unload schema(s)
:schema show <[ks.]table>      # Show effective schema model
:schema refresh                # Re-parse schema files
```

#### Navigation & Introspection
```text
:use <keyspace>                # Set current keyspace
:keyspaces                     # List all keyspaces
:tables                        # List tables (current keyspace or all)
:describe <[ks.]table>         # Show table DDL (alias: :desc)
DESC <[ks.]table>              # cqlsh-compatible DESCRIBE
```

#### Data Discovery & Sync
```text
:discover [--refresh]          # Scan data-dir for keyspaces/tables
:status                        # Show schema-data sync status
:health                        # Show config and environment checks
```

#### Scripting
```text
:source <FILE>                 # Execute commands/CQL from file
```

### Configuration Examples

```text
# Set data directory
cqlite> :config data-dir /Users/patrick/local_projects/cqlite/test-data/datasets
Success: Data directory set to: /Users/patrick/local_projects/cqlite/test-data/datasets

# Set page size
cqlite> :config page-size 25
Success: Page size set to: 25

# Enable timing
cqlite> :config timing on
Success: Timing enabled

# View effective config
cqlite> :config
Data Directory: /Users/patrick/local_projects/cqlite/test-data/datasets
Default Keyspace: ks
Page Size: 25
Timing: on
Colors: enabled
```

### Schema Management Examples

```text
# Load schema from directory
cqlite> :schema load /Users/patrick/local_projects/cqlite/test-data/schemas
Loaded 5 schema files (3 CQL, 2 JSON)
Keyspaces: ks, system
Tables: ks.users, ks.orders, ks.events, system.local, system.peers

# List loaded schemas
cqlite> :schema list
Schema Sources:
  - /Users/patrick/local_projects/cqlite/test-data/schemas/ks.cql (CQL)
  - /Users/patrick/local_projects/cqlite/test-data/schemas/system.json (JSON)
Keyspaces: ks, system
Tables: 5

# Show specific table schema
cqlite> :schema show ks.users
Table: ks.users
Keyspace: ks
Columns:
  - id (uuid) PRIMARY KEY
  - name (text)
  - email (text)
  - created_at (timestamp)
```

### Navigation Examples

```text
# List keyspaces
cqlite> :keyspaces
Keyspaces:
  - system (5 tables)
  - ks (3 tables)

# Switch keyspace
cqlite> USE ks;
Using keyspace: ks

# List tables in current keyspace
cqlite> :tables
Tables (ks):
  - users
  - orders
  - events

# Describe table (cqlsh-compatible)
cqlite> DESCRIBE ks.users;
CREATE TABLE ks.users (
    id uuid PRIMARY KEY,
    name text,
    email text,
    created_at timestamp
) WITH compaction = { ... } AND compression = { ... };

# Alternative describe syntax
cqlite> :describe users
CREATE TABLE ks.users (
    id uuid PRIMARY KEY,
    name text,
    email text,
    created_at timestamp
) WITH compaction = { ... } AND compression = { ... };
```

### Status and Health Commands

```text
# Check schema-data synchronization
cqlite> :status
Data Directory: /Users/patrick/local_projects/cqlite/test-data/datasets
Discovery: 2 keyspaces, 8 tables
Schema Coverage:
  - tables with schema: 7
  - tables missing schema: 1  (e.g., ks.audit_logs)
  - schemas without data: 0
Cassandra Version: detected 5.0 (configured: 5.0)
Status: Green (88% coverage; no critical errors)

# Check environment and configuration health
cqlite> :health
Checks:
  - data-dir readable: OK
  - schema parse: OK (5 files)
  - schema/data sync: 7/8 tables covered
  - compression codecs: LZ4, Snappy, Zstd available
  - config: page-size=50, timing=off, colors=on
Tips:
  - Missing schema for: ks.audit_logs (use :schema load <file>)
  - Consider enabling timing with: :config timing on
```

### Query Execution Examples

```text
# Simple SELECT
cqlite> SELECT * FROM users LIMIT 5;
 id                                   | name        | email                | created_at
--------------------------------------+-------------+----------------------+---------------------------
 8b6c8a96-5f5a-4f7e-a6a8-2b5a3a3f1c01 | Alice Wong  | alice@example.com    | 2024-01-15 10:30:00
 2a1dc9b7-2f1f-4db2-8d1f-7c0a4d4f9b12 | Bob Smith   | bob@example.com      | 2024-01-16 14:22:00
 4c7e2f90-1b33-4a6a-9e1c-9d4e8a2f3c45 | Carol Chen  | carol@example.com    | 2024-01-17 09:15:00
 9f2d1a3b-7c2e-4a5b-8f1e-3d4c5b6a7e89 | Dan Jones   | dan@example.com      | 2024-01-18 16:45:00
 1e2f3a4b-5c6d-7e8f-9012-3456789abcde | Eve Adams   | eve@example.com      | 2024-01-19 11:30:00

(5 rows)

# SELECT specific columns
cqlite> SELECT id, name FROM users LIMIT 3;
 id                                   | name
--------------------------------------+-------------
 8b6c8a96-5f5a-4f7e-a6a8-2b5a3a3f1c01 | Alice Wong
 2a1dc9b7-2f1f-4db2-8d1f-7c0a4d4f9b12 | Bob Smith
 4c7e2f90-1b33-4a6a-9e1c-9d4e8a2f3c45 | Carol Chen

(3 rows)

# WHERE clause on primary key
cqlite> SELECT * FROM users WHERE id = 8b6c8a96-5f5a-4f7e-a6a8-2b5a3a3f1c01;
 id                                   | name        | email                | created_at
--------------------------------------+-------------+----------------------+---------------------------
 8b6c8a96-5f5a-4f7e-a6a8-2b5a3a3f1c01 | Alice Wong  | alice@example.com    | 2024-01-15 10:30:00

(1 row)
```

---

## Output Formats

CQLite supports multiple output formats:

### UTC Timestamp Behavior (M2)

CQLite M2 displays all timestamp values in UTC timezone for consistency:

```text
cqlite> SELECT id, created_at FROM users LIMIT 2;
 id                                   | created_at
--------------------------------------+---------------------------
 8b6c8a96-5f5a-4f7e-a6a8-2b5a3a3f1c01 | 2024-01-15 10:30:00 UTC
 2a1dc9b7-2f1f-4db2-8d1f-7c0a4d4f9b12 | 2024-01-16 14:22:00 UTC
```

**Note**: This behavior ensures consistent timestamp display across different system timezones. Local timezone support is planned for M3.

### Table Format (cqlsh-compatible)

Default format with cqlsh-style rendering:

```text
cqlite> SELECT id, name, email FROM users LIMIT 3;
 id                                   | name        | email
--------------------------------------+-------------+-----------------------
 8b6c8a96-5f5a-4f7e-a6a8-2b5a3a3f1c01 | Alice Wong  | alice@example.com
 2a1dc9b7-2f1f-4db2-8d1f-7c0a4d4f9b12 | Bob Smith   | bob@example.com
 4c7e2f90-1b33-4a6a-9e1c-9d4e8a2f3c45 | Carol Chen  | carol@example.com

(3 rows)
```

Features:
- Column headers with proper alignment
- Row separators
- Row count summary
- Colored output (disable with `--no-color` or `CQLITE_NO_COLOR=1`)

### JSON Format

Array of row objects with stable key ordering:

```bash
cqlite --schema /Users/patrick/local_projects/cqlite/test-data/schemas \
       --data-dir /Users/patrick/local_projects/cqlite/test-data/datasets \
       -e "SELECT id, name, email FROM ks.users LIMIT 3" --out json
```

Output:
```json
[
  {
    "id": "8b6c8a96-5f5a-4f7e-a6a8-2b5a3a3f1c01",
    "name": "Alice Wong",
    "email": "alice@example.com"
  },
  {
    "id": "2a1dc9b7-2f1f-4db2-8d1f-7c0a4d4f9b12",
    "name": "Bob Smith",
    "email": "bob@example.com"
  },
  {
    "id": "4c7e2f90-1b33-4a6a-9e1c-9d4e8a2f3c45",
    "name": "Carol Chen",
    "email": "carol@example.com"
  }
]
```

### CSV Format

Standard CSV with header row:

```bash
cqlite --schema /Users/patrick/local_projects/cqlite/test-data/schemas \
       --data-dir /Users/patrick/local_projects/cqlite/test-data/datasets \
       -e "SELECT id, name, email FROM ks.users LIMIT 3" --out csv
```

Output:
```csv
id,name,email
8b6c8a96-5f5a-4f7e-a6a8-2b5a3a3f1c01,Alice Wong,alice@example.com
2a1dc9b7-2f1f-4db2-8d1f-7c0a4d4f9b12,Bob Smith,bob@example.com
4c7e2f90-1b33-4a6a-9e1c-9d4e8a2f3c45,Carol Chen,carol@example.com
```

---

## Exporting Data (M3)

CQLite M3 adds file-based export functionality for extracting data to various formats.

### Export Command Syntax

```bash
cqlite export <OUTPUT_FILE> --format <FORMAT> --table <KEYSPACE.TABLE> [--query <WHERE_CLAUSE>]
```

### Supported Export Formats

| Format | Extension | Description | Use Case |
|--------|-----------|-------------|----------|
| `csv` | .csv | RFC 4180 CSV | Spreadsheets, data warehouses |
| `json` | .json | JSON array of objects | APIs, web applications |
| `parquet` | .parquet | Apache Parquet (Snappy) | Analytics, big data tools |
| `cql` | .cql | CQL INSERT statements | Cassandra migration, backups |

### Export Examples

#### Export to CSV

```bash
cqlite --schema test-data/schemas/basic-types.cql \
       --data-dir test-data/datasets/sstables \
       export users.csv --format csv --table test_basic.simple_table
```

#### Export to JSON

```bash
cqlite --schema test-data/schemas/basic-types.cql \
       --data-dir test-data/datasets/sstables \
       export users.json --format json --table test_basic.simple_table
```

#### Export to Parquet

```bash
cqlite --schema test-data/schemas/basic-types.cql \
       --data-dir test-data/datasets/sstables \
       export users.parquet --format parquet --table test_basic.simple_table
```

#### Export with WHERE Filter

```bash
cqlite --schema test-data/schemas/basic-types.cql \
       --data-dir test-data/datasets/sstables \
       export active_users.csv --format csv \
       --table test_basic.simple_table --query "active = true"
```

#### Export to CQL INSERT Statements

```bash
cqlite --schema test-data/schemas/basic-types.cql \
       --data-dir test-data/datasets/sstables \
       export backup.cql --format cql --table test_basic.simple_table
```

#### Export with Row Limit

Use `--limit` to restrict the number of rows exported:

```bash
# Export only the first 1000 rows
cqlite --schema test-data/schemas/basic-types.cql \
       --data-dir test-data/datasets/sstables \
       export sample.csv --format csv \
       --table test_basic.simple_table --limit 1000

# Combine limit with filter
cqlite --schema test-data/schemas/basic-types.cql \
       --data-dir test-data/datasets/sstables \
       export top_users.json --format json \
       --table test_basic.simple_table --query "active = true" --limit 100
```

### CQL-to-Parquet Type Mapping

When exporting to Parquet format, CQL types are mapped to Apache Arrow types:

| CQL Type | Arrow Type | Notes |
|----------|------------|-------|
| `int` | Int32 | 32-bit signed integer |
| `smallint` | Int16 | 16-bit signed integer |
| `tinyint` | Int8 | 8-bit signed integer |
| `bigint`, `counter` | Int64 | 64-bit signed integer |
| `float` | Float32 | Single precision |
| `double` | Float64 | Double precision |
| `text`, `varchar`, `ascii` | Utf8 | UTF-8 strings |
| `blob` | Binary | Raw bytes |
| `boolean` | Boolean | True/false |
| `timestamp` | Timestamp(ms, UTC) | Millisecond precision, UTC timezone |
| `uuid`, `timeuuid` | FixedSizeBinary(16) | 16-byte binary |
| `list<T>` | List(Utf8) | List of UTF-8 strings |
| `set<T>` | List(Utf8) | List of UTF-8 strings |
| `map<K,V>` | Map(Utf8, Utf8) | Map with string keys/values |
| `tuple`, `UDT`, `frozen` | Utf8 | JSON serialization |

### Export Progress and Statistics

Exports display progress and statistics (unless `--quiet` is specified):

```
[████████████████████████████████] 100% (10000/10000 rows) ETA: 0s
Export complete: 10000 rows written to users.parquet
File size: 245.3 KB
Duration: 1.2s (8333 rows/sec)
```

### Streaming Export (Memory Efficiency)

CQLite exports use memory-efficient streaming automatically. No special flag is needed.

#### How Streaming Works

- **Data Processing**: Rows are processed in chunks of 1,000 rows
- **Parquet Row Groups**: Parquet files use 10,000-row row groups for optimal compression
- **Memory Budget**: Designed to stay under 128MB memory usage even for large datasets

#### Benefits

- Export datasets larger than available RAM
- Consistent memory footprint regardless of table size
- Automatic chunk-based processing for all formats

#### Format-Specific Behavior

| Format | Streaming Strategy |
|--------|-------------------|
| CSV | Direct write per row (most memory-efficient) |
| JSON | Line-by-line output with deterministic key ordering |
| Parquet | Buffered row groups (10,000 rows) for columnar efficiency |
| CQL | Generates INSERT statements in batches |

#### Example: Large Dataset Export

```bash
# Export a multi-GB table - streaming handles memory automatically
cqlite --schema schema.cql --data-dir ./large-dataset \
       export all_data.parquet --format parquet --table myks.events
```

No configuration needed - CQLite automatically streams the data efficiently.

---

## Write Support (M5)

CQLite M5 adds write operations for creating Cassandra-compatible SSTables. Write support is behind the `write-support` feature flag.

> **Note**: Write support requires building with `--features write-support`.

### Building with Write Support

```bash
# Build CLI with write support enabled
cargo build --package cqlite-cli --features write-support

# Or install directly
cargo install --path cqlite-cli --features write-support
```

### Write Mode Flags

Write mode is enabled with `--writable` and requires `--write-dir` to specify where to store the WAL and SSTable files:

| Flag | Description | Example |
|------|-------------|---------|
| `--writable` | Enable write mode | `--writable` |
| `--write-dir <DIR>` | Directory for WAL and SSTable output | `--write-dir /tmp/cqlite-write` |
| `--mutation <JSON>` | JSON mutation to write (repeatable) | `--mutation '{"table":...}'` |
| `--mutations-file <FILE>` | File containing mutations (JSONL) | `--mutations-file mutations.jsonl` |
| `--flush` | Force flush memtable to SSTable | `--flush` |

### Mutation JSON Format

Mutations use a JSON structure that specifies the table, partition key, and cell operations:

```json
{
  "table": {
    "keyspace": "my_keyspace",
    "table": "my_table"
  },
  "partition_key": [
    {"Uuid": [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]}
  ],
  "clustering_key": [],
  "operations": [
    {
      "Write": {
        "column": "name",
        "value": {"Text": "Alice"}
      }
    },
    {
      "Write": {
        "column": "age",
        "value": {"Int": 30}
      }
    }
  ],
  "timestamp_micros": 1704067200000000
}
```

### Write Examples

#### Single Mutation Write

```bash
# Write a single mutation
cqlite --writable --write-dir /tmp/cqlite-write \
  --schema test-data/schemas/basic-types.cql \
  --mutation '{"table":{"keyspace":"test_basic","table":"simple_table"},"partition_key":[{"Uuid":[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]}],"clustering_key":[],"operations":[{"Write":{"column":"name","value":{"Text":"Test User"}}}],"timestamp_micros":1704067200000000}'

# Output: OK: 1 row(s) affected (Xms)
```

#### Multiple Mutations

```bash
# Write multiple mutations in a single command
cqlite --writable --write-dir /tmp/cqlite-write \
  --schema test-data/schemas/basic-types.cql \
  --mutation '{"table":{"keyspace":"test_basic","table":"simple_table"},...}' \
  --mutation '{"table":{"keyspace":"test_basic","table":"simple_table"},...}'
```

#### Mutations from File (JSONL)

Create a file with one mutation JSON per line:

```bash
# mutations.jsonl
{"table":{"keyspace":"test_basic","table":"simple_table"},"partition_key":[...],"clustering_key":[],"operations":[...],"timestamp_micros":1704067200000000}
{"table":{"keyspace":"test_basic","table":"simple_table"},"partition_key":[...],"clustering_key":[],"operations":[...],"timestamp_micros":1704067200000001}
```

Then execute:

```bash
cqlite --writable --write-dir /tmp/cqlite-write \
  --schema test-data/schemas/basic-types.cql \
  --mutations-file mutations.jsonl

# Output: Batch complete: 2 row(s) affected (2 succeeded, 0 failed) in Xms
```

#### Flush Memtable to SSTable

```bash
# Write mutations and flush to disk
cqlite --writable --write-dir /tmp/cqlite-write \
  --schema test-data/schemas/basic-types.cql \
  --mutation '{"table":...}' \
  --flush

# Output:
# OK: 1 row(s) affected (Xms)
# Flushed: 1 partitions, 256 bytes
#   Output: /tmp/cqlite-write/data/nb-1-big-Data.db
```

### Write Subcommands

#### Maintenance (Compaction)

Run background maintenance to compact L0 SSTables:

```bash
cqlite maintenance --budget-ms 500 \
  --writable --write-dir /tmp/cqlite-write \
  --schema test-data/schemas/basic-types.cql

# Output:
# Maintenance complete:
#   Time spent: 234ms
#   Rows merged: 150
#   Bytes written: 4096 bytes
#   Pending compaction: false
```

#### Write Statistics

Display current write engine statistics:

```bash
cqlite write-stats \
  --writable --write-dir /tmp/cqlite-write \
  --schema test-data/schemas/basic-types.cql

# Output:
# Write Engine Statistics:
#   Memtable size: 1024 bytes
#   Memtable rows: 5
#   WAL size: 2048 bytes
#   Generation: 3
```

#### Package Flushed SSTables for Cassandra Import

After `cqlite` flushes data, the write directory already contains portable Cassandra SSTable components.
Use `export-sstable` when you want those files arranged into a loader-friendly keyspace/table directory for tools like `sstableloader`:

```bash
cqlite export-sstable /tmp/export \
  --keyspace my_keyspace \
  --table my_table \
  --writable --write-dir /tmp/cqlite-write \
  --schema test-data/schemas/basic-types.cql

# Output:
# Export complete:
#   Output: /tmp/export/my_keyspace/my_table-xyz/nb-1-big-Data.db
#   Rows: 100
#   Size: 8192 bytes
#   Time: 45.2ms
```

Packaging options:

| Flag | Description |
|------|-------------|
| `--keyspace <NAME>` | Keyspace name for export (default: `export`) |
| `--table <NAME>` | Table name for export (default: `data`) |
| `--compact` | Run compaction before export to merge multiple SSTables |
| `--skip-validate` | Skip validation after export |

### REPL Write Meta-Commands

When running in REPL mode with write support enabled, additional meta-commands are available:

```text
# In REPL with write mode enabled
cqlite> .flush
Flushed: 5 partitions, 1024 bytes
  Output: /tmp/cqlite-write/data/nb-2-big-Data.db

cqlite> .stats
Write Engine Statistics:
  Memtable size: 0 bytes
  Memtable rows: 0
  WAL size: 512 bytes
  Generation: 2

cqlite> .maintenance 200
Maintenance complete:
  Time spent: 156ms
  Rows merged: 50
  Bytes written: 2048 bytes
  Pending compaction: false
```

Available write meta-commands:

| Command | Aliases | Description |
|---------|---------|-------------|
| `.flush` | | Flush memtable to SSTable |
| `.stats` | `.write-stats`, `.writestats`, `.wstats` | Show write engine statistics |
| `.maintenance [ms]` | `.maint`, `.compact` | Run compaction with optional time budget |

**Note:** Write commands (`.flush`, `.stats`, `.maintenance`) require the REPL to be started with `--writable --write-dir <path>` flags.

### SSTable Output Structure

When flushing or exporting, CQLite creates Cassandra-compatible SSTable files:

```
write-dir/
├── data/                          # SSTable files
│   ├── nb-1-big-Data.db          # Row data
│   ├── nb-1-big-Index.db         # Partition index
│   ├── nb-1-big-Summary.db       # Summary index
│   ├── nb-1-big-Statistics.db    # Table statistics
│   ├── nb-1-big-Filter.db        # Bloom filter
│   └── nb-1-big-TOC.txt          # Table of contents
└── wal/                           # Write-ahead log
    └── wal-{timestamp}.log
```

The packaged keyspace/table directory can be imported into Cassandra using:

```bash
sstableloader -d <cassandra-host> /path/to/export/keyspace/table-uuid/
```

---

## Command Reference

### Top-Level Commands

```bash
# Default: launch REPL
cqlite

# Explicit REPL mode
cqlite repl

# One-shot query
cqlite --schema <PATH> --data-dir <DIR> -e <CQL> [--out <FORMAT>]

# One-shot script
cqlite --schema <PATH> --data-dir <DIR> -f <CQL_FILE> [--out <FORMAT>]

# Low-level SSTable inspection
cqlite read-sstable <sstable_or_dir> --schema <FILE> --format <FORMAT>

# SSTable metadata
cqlite info <sstable_or_dir> [--validate]
```

### Global Flags

| Flag | Description | Example |
|------|-------------|---------|
| `--config <FILE>` | Load config (TOML/YAML/JSON) | `--config config.toml` |
| `--schema <PATH>` | Schema file/directory (repeatable) | `--schema schemas/` |
| `--data-dir <DIR>` | Cassandra data directory | `--data-dir /var/lib/cassandra/data` |
| `-e, --execute <CQL>` | Execute single statement | `-e "SELECT * FROM users"` |
| `-f, --file <FILE>` | Execute statements from file | `-f script.cql` |
| `--out <FORMAT>` | Output format (table/json/csv) | `--out json` |
| `--limit <N>` | Cap rows returned | `--limit 100` |
| `--page-size <N>` | Pagination size | `--page-size 50` |
| `--auto-detect` | Enable auto-detection | `--auto-detect` |
| `--cassandra-version <VER>` | Version hint | `--cassandra-version 5.0` |
| `-v, --verbose` | Increase verbosity | `-vvv` |
| `-q, --quiet` | Suppress output | `--quiet` |
| `--no-color` | Disable colored output | `--no-color` |

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 2 | Invalid CLI arguments |
| 3 | Schema errors |
| 4 | Data directory/discovery errors |
| 5 | Query execution errors |
| 6 | Write operation errors (M5) |

---

## Additional Examples

### Working with Collections

```text
cqlite> SELECT user_id, tags FROM user_tags LIMIT 3;
 user_id                              | tags
--------------------------------------+------------------
 8b6c8a96-5f5a-4f7e-a6a8-2b5a3a3f1c01 | {admin, premium}
 2a1dc9b7-2f1f-4db2-8d1f-7c0a4d4f9b12 | {user}
 4c7e2f90-1b33-4a6a-9e1c-9d4e8a2f3c45 | {user, beta}

(3 rows)
```

### Using Configuration Persistence

```text
# Save current session config
cqlite> :config save ~/.cqlite.toml
Configuration saved to: /Users/patrick/.cqlite.toml

# Load saved config in future sessions
$ cqlite --config ~/.cqlite.toml
```

### Scripting Workflow

```bash
# Create a CQL script
cat > analyze.cql <<'EOF'
USE ks;
SELECT COUNT(*) FROM users;
SELECT * FROM users WHERE created_at > '2024-01-01' LIMIT 10;
SELECT id, name FROM orders LIMIT 5;
EOF

# Execute script
cqlite --schema /Users/patrick/local_projects/cqlite/test-data/schemas \
       --data-dir /Users/patrick/local_projects/cqlite/test-data/datasets \
       -f analyze.cql --out table
```

### Combining Environment Variables and Flags

```bash
# Set defaults via environment
export CQLITE_DATA_DIR=/Users/patrick/local_projects/cqlite/test-data/datasets
export CQLITE_SCHEMA=/Users/patrick/local_projects/cqlite/test-data/schemas
export CQLITE_PAGE_SIZE=25

# Override specific settings with flags
cqlite -e "SELECT * FROM ks.users LIMIT 100" --out json --limit 50
# Uses env for data-dir and schema, but overrides limit with flag
```

---

## Best Practices

1. **Use environment variables** for commonly-used paths to simplify commands
2. **Start with `:status`** in REPL to understand schema-data coverage
3. **Use `:health`** to diagnose configuration issues
4. **Prefer table format** for interactive exploration, JSON/CSV for programmatic use
5. **Save configuration** with `:config save` for consistent sessions
6. **Load schemas early** to enable introspection commands (`:tables`, `:describe`)
7. **Use `--limit`** to prevent overwhelming output from large tables

---

## Troubleshooting

### Missing Schema Errors

```text
Error: No schema found for table ks.users
Tip: Load schema with: :schema load /path/to/schemas
```

Solution:
```text
cqlite> :schema load /Users/patrick/local_projects/cqlite/test-data/schemas
```

### Data Directory Not Set

```text
Error: Data directory not configured
Tip: Set with: :config data-dir <PATH>
```

Solution:
```text
cqlite> :config data-dir /Users/patrick/local_projects/cqlite/test-data/datasets
```

### Unknown Meta-Command

```text
Error: Unknown command ':foo'
Tip: See available commands with :help
```

Solution:
```text
cqlite> :help
```

---

## Milestone Notes

### M2 Features
- **Read-only operations**: SELECT, DESCRIBE, USE (no DML/DDL mutations)
- **Output formats**: Table, JSON, CSV
- **TUI mode**: Experimental

### M3 Features
- **Export command**: `cqlite export` for file-based data export
- **Parquet output**: Apache Parquet with Snappy compression
- **Streaming export**: Memory-efficient export for large datasets
- **Additional formats**: CQL INSERT statement generation

### M5 Features (Issue #392)
- **Write support**: Mutation-based writes to local SSTables
- **Write flags**: `--writable`, `--write-dir`, `--mutation`, `--mutations-file`, `--flush`
- **Write subcommands**: `maintenance`, `write-stats`, `export-sstable`
- **REPL meta-commands**: `.flush`, `.stats`, `.maintenance`
- **SSTable export**: Cassandra-compatible SSTable generation for `sstableloader`
- **Feature flag**: Requires `--features write-support` to build

### Future Enhancements
- **CQL DML parsing**: INSERT/UPDATE/DELETE syntax (blocked on Issue #372)
- **Remote cluster connectivity**: Out of scope

For complete specification details, see `docs/development/M2_CLI_SPEC.md`.
