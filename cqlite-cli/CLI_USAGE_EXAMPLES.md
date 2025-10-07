# CQLite CLI Usage Examples (M2)

This document shows one‑shot and REPL usage aligned with the M2 CLI spec.

## Core Commands

### One‑Shot Queries Against Local Data (recommended during dev)

```bash
# Absolute paths for repo test-data
SCHEMA=/Users/patrick/local_projects/cqlite/test-data/schemas
DATA_DIR=/Users/patrick/local_projects/cqlite/test-data/datasets

# Execute a simple query, cqlsh-style table output
cargo run -p cqlite-cli -- \
  --schema "$SCHEMA" \
  --data-dir "$DATA_DIR" \
  -e "SELECT * FROM ks.users LIMIT 5" --out table

# Output as JSON
cargo run -p cqlite-cli -- \
  --schema "$SCHEMA" \
  --data-dir "$DATA_DIR" \
  -e "SELECT id, name FROM ks.users LIMIT 3" --out json

# Output as CSV
cargo run -p cqlite-cli -- \
  --schema "$SCHEMA" \
  --data-dir "$DATA_DIR" \
  -e "SELECT id, email FROM ks.users LIMIT 3" --out csv

# Run a script of statements
cargo run -p cqlite-cli -- \
  --schema "$SCHEMA" \
  --data-dir "$DATA_DIR" \
  -f statements.cql --out table
```

### REPL (Interactive)

```bash
cargo run -p cqlite-cli -- repl

# In REPL:
:config data-dir /Users/patrick/local_projects/cqlite/test-data/datasets
:schema load /Users/patrick/local_projects/cqlite/test-data/schemas
:status
:keyspaces
:tables
DESCRIBE ks.users;
SELECT id, name FROM users LIMIT 5;
```

### SSTable Information (low-level helpers)

```bash
# Show SSTable metadata and statistics
cargo run -p cqlite-cli -- info /path/to/users.sstable

# Example output:
# SSTable Information
# ==================
# File: /path/to/users.sstable
# Size: 15728640 bytes
# Index entries: 1024
# Compression: snappy
# Format version: 3.11
```

### Export Data (JSON/CSV)

```bash
# Export SSTable data to JSON file
cargo run -p cqlite-cli -- export dummy --sstable /path/to/users.sstable --schema "$SCHEMA" /tmp/output.json --format json

# Export to CSV
cargo run -p cqlite-cli -- export dummy --sstable /path/to/users.sstable --schema "$SCHEMA" /tmp/output.csv --format csv
```

## Key Features Implemented

### 1. Schema-Aware Reading
- Loads table schema from JSON file
- Maps SSTable data to proper CQL types
- Displays column names and types correctly

### 2. Multiple Output Formats
- **Table**: Pretty-printed ASCII tables with borders
- **JSON**: Well-formatted JSON arrays with proper type conversion
- **CSV**: Standard CSV format with headers
- **YAML**: YAML format for configuration-like output

### 3. Progress Indicators
- Real-time spinner showing progress
- Row count tracking
- Elapsed time display
- Final summary with total rows processed

### 4. Data Type Formatting
- **UUID/TimeUUID**: Standard UUID string format
- **Blob**: Hexadecimal representation (0x...)
- **Collections**: Proper List [a,b,c], Set {a,b,c}, Map {k:v} formatting
- **Text**: Direct string output
- **Numbers**: Appropriate precision for floats/doubles
- **Timestamps**: Human-readable formats

### 5. Error Handling
- Descriptive error messages with file paths
- Schema validation with helpful hints
- Graceful handling of corrupt or invalid files
- Context-aware error reporting

### 6. Performance Features
- Streaming data processing (doesn't load all data into memory)
- Configurable limits to prevent overwhelming output
- Skip functionality for pagination
- Progress tracking for large files

## Command Structure (M2)

### Main Commands
- `cqlite --schema <PATH> --data-dir <DIR> -e <CQL> --out <table|json|csv>`
- `cqlite --schema <PATH> --data-dir <DIR> -f <CQL_FILE> --out <table|json|csv>`
- `cqlite repl` (interactive)
- `cqlite info <sstable_or_dir>`
- `cqlite read-sstable <sstable_or_dir> --schema <FILE> --format <table|json|csv>`

### Global Options
- `--format <table|json|csv|yaml>` - Output format (default: table)
- `--verbose` - Increase verbosity (-v, -vv, -vvv)
- `--quiet` - Suppress output
- `--config <file>` - Configuration file path

### One‑Shot Options
- `--schema <FILE|DIR>` - Schema sources (CQL/JSON), repeatable
- `--data-dir <DIR>` - Cassandra data root directory
- `-e/--execute <CQL>` - Execute a single statement
- `-f/--file <CQL_FILE>` - Execute statements from file
- `--out <table|json|csv>` - Output format (default table)
- `--limit <n>` - Maximum number of rows
- `--page-size <n>` - Reader/display pagination size

## Example Schema File (JSON)

```json
{
  "table_name": "users",
  "columns": [
    {
      "name": "id",
      "data_type": "Uuid"
    },
    {
      "name": "name", 
      "data_type": "Text"
    },
    {
      "name": "email",
      "data_type": "Text"
    },
    {
      "name": "created_at",
      "data_type": "Timestamp"
    }
  ],
  "primary_key": ["id"]
}
```

## Environment Variables

```bash
export CQLITE_SCHEMA=/Users/patrick/local_projects/cqlite/test-data/schemas
export CQLITE_DATA_DIR=/Users/patrick/local_projects/cqlite/test-data/datasets
```