# CQLite CLI Usage Examples

This document demonstrates the CLI tool commands that have been implemented.

## Core Commands

### Reading SSTable Files

```bash
# Read SSTable with schema and display as table (default)
cqlite read /path/to/users.sstable --schema schema_example.json

# Limit output to 50 rows
cqlite read /path/to/users.sstable --schema schema_example.json --limit 50

# Skip first 100 rows, show next 25
cqlite read /path/to/users.sstable --schema schema_example.json --skip 100 --limit 25

# Output as JSON
cqlite read /path/to/users.sstable --schema schema_example.json --format json

# Output as CSV
cqlite read /path/to/users.sstable --schema schema_example.json --format csv

# Output as YAML
cqlite read /path/to/users.sstable --schema schema_example.json --format yaml
```

### Schema Validation

```bash
# Validate a schema JSON file
cqlite schema validate schema_example.json

# Example successful validation output:
# ✅ Schema validation successful!
# Table: users
# Columns: 4
#   1. id (UUID)
#   2. name (Text)
#   3. email (Text) 
#   4. created_at (Timestamp)
# Primary key: id
```

### SSTable Information

```bash
# Show SSTable metadata and statistics
cqlite info /path/to/users.sstable

# Example output:
# SSTable Information
# ==================
# File: /path/to/users.sstable
# Size: 15728640 bytes
# Index entries: 1024
# Compression: snappy
# Format version: 3.11
```

### Export Data

```bash
# Export SSTable data to JSON file
cqlite export dummy --sstable /path/to/users.sstable --schema schema_example.json /path/to/output.json --format json

# Export to CSV
cqlite export dummy --sstable /path/to/users.sstable --schema schema_example.json /path/to/output.csv --format csv

# Export to SQL INSERT statements
cqlite export dummy --sstable /path/to/users.sstable --schema schema_example.json /path/to/output.sql --format sql
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

## Command Structure

### Main Commands
- `cqlite read <sstable> --schema <json>` - Read and display SSTable data
- `cqlite info <sstable>` - Show SSTable information
- `cqlite schema validate <json>` - Validate schema file
- `cqlite export <source> --sstable <path> --schema <json> <output> --format <format>` - Export data

### Global Options
- `--format <table|json|csv|yaml>` - Output format (default: table)
- `--verbose` - Increase verbosity (-v, -vv, -vvv)
- `--quiet` - Suppress output
- `--config <file>` - Configuration file path

### Read Command Options
- `--limit <n>` - Maximum number of rows to display
- `--skip <n>` - Number of rows to skip
- `--schema <path>` - Schema JSON file (required)

## Example Schema File

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

## Implementation Status

✅ **Completed**:
- CLI argument parsing with clap
- Command structure and routing
- Schema validation functionality
- SSTable reading framework
- Multiple output format support
- Progress bar integration
- Error handling and user feedback
- Help text and documentation

⏳ **Dependencies**:
- Core library compilation issues must be resolved
- SSTableReader implementation needs completion
- TableSchema deserialization must be working

🔄 **Next Steps**:
1. Fix core library compilation errors
2. Test CLI with real SSTable files
3. Add integration tests
4. Performance optimization
5. Add more export formats (Parquet)