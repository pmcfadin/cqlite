---
title: CLI Reference
description: Complete reference for the cqlite command-line interface — all flags, subcommands, and verified examples.
sidebar:
  label: CLI Reference
  order: 3
---

# CLI Reference

This page is built from the actual `cqlite --help` output (binary version 0.10.0, built with `--features write-support`). Every example on this page was run against the real test datasets (`cassandra5-small-full-v3.1`).

## Synopsis

```
cqlite [OPTIONS] [COMMAND]
```

CQLite provides cqlsh-compatible access to Apache Cassandra 5.0 SSTables locally without cluster dependencies. Supports interactive REPL and one-shot query modes.

**Ingestion model**: provide `--schema` and `--data-dir` together to trigger schema loading and dataset discovery for query execution.

---

## Global options

These options apply at the top level and are available regardless of which subcommand is used.

| Flag | Short | Default | Env var | Description |
|------|-------|---------|---------|-------------|
| `--database <FILE>` | `-d` | `cqlite.db` | — | Database file path |
| `--config <FILE>` | `-c` | — | — | Load config file (TOML/YAML/JSON). Precedence: flags > env > file > defaults |
| `--verbose` | `-v` | — | — | Verbose output; repeat for more detail (`-v`, `-vv`, `-vvv`) |
| `--quiet` | `-q` | — | — | Quiet mode — suppress non-essential output |
| `--format <FORMAT>` | — | `table` | — | Global display format: `table`, `json`, `csv`, `parquet` |
| `--schema <PATH>` | — | — | `CQLITE_SCHEMA` | CQL (`.cql`) or JSON (`.json`) schema file. Required for query execution. Repeatable; order defines precedence |
| `--data-dir <DIR>` | — | — | `CQLITE_DATA_DIR` | Cassandra data directory root. Combined with `--schema`, triggers dataset discovery. Mutually exclusive with `--dataset` |
| `--dataset <DATASET>` | — | — | — | Dataset name shortcut (e.g., `test_basic`). Looks under `CQLITE_DATASETS_ROOT/sstables/{dataset}/`. Mutually exclusive with `--data-dir` |
| `--execute <CQL>` | `-e` | — | — | Execute a single CQL statement (one-shot mode). Alias: `--query` |
| `--query <CQL>` | — | — | — | Alias for `--execute` |
| `--file <CQL_FILE>` | `-f` | — | — | Execute statements from a file (semicolon-terminated) |
| `--out <OUT>` | — | — | `CQLITE_OUT` | Output format for query results. Overrides `--format` when specified. Values: `table`, `json`, `csv`, `parquet` |
| `--output <FILE>` | `-o` | stdout | `CQLITE_OUTPUT` | Output file path. Required when format is `parquet` |
| `--overwrite` | — | — | — | Overwrite output file if it exists (requires `--output`) |
| `--limit <N>` | — | — | `CQLITE_LIMIT` | Cap the number of returned rows |
| `--page-size <N>` | — | — | `CQLITE_PAGE_SIZE` | Reader and display pagination size |
| `--no-color` | — | — | `CQLITE_NO_COLOR` | Disable colored output |
| `--auto-detect` | — | — | — | Enable best-effort format/version auto-detection |
| `--cassandra-version <VER>` | — | — | — | Version hint (e.g., `5.0`) for format compatibility |
| `--writable` | — | — | `CQLITE_WRITABLE` | Enable write mode (requires `--write-dir`) |
| `--write-dir <DIR>` | — | — | `CQLITE_WRITE_DIR` | Directory for write operations (WAL and SSTable output) |
| `--mutation <JSON>` | — | — | — | JSON mutation to write. Repeatable. Requires `--writable` |
| `--mutations-file <FILE>` | — | — | — | JSONL file of mutations (one per line). Requires `--writable` |
| `--flush` | — | — | — | Force flush memtable to SSTable after writes. Requires `--writable` |
| `--enable-select-fallback` | — | — | `CQLITE_ENABLE_SELECT_FALLBACK` | **Experimental**: fallback to `read-sstable` for SELECT when ingestion unavailable. Will be removed in M3 |
| `--help` | `-h` | — | — | Print help (full with `--help`, summary with `-h`) |
| `--version` | `-V` | — | — | Print version |

---

## Output format precedence

When multiple format controls are present, the resolution order is:

1. **`--out` (or `CQLITE_OUT` env var)** — highest priority; overrides `--format`
2. **`--format`** — global display format; used when `--out` is not set
3. **Default**: `table`

Verified behavior:

```bash
# --out json wins over --format csv
cqlite --schema schema.cql --data-dir ./sstables \
  --query "SELECT id FROM test_basic.simple_table LIMIT 1" \
  --out json --format csv
# Output: JSON array

# CQLITE_OUT=csv with --format json: CQLITE_OUT sets --out, which wins
CQLITE_OUT=csv cqlite --schema schema.cql --data-dir ./sstables \
  --query "SELECT id FROM test_basic.simple_table LIMIT 1" \
  --format json
# Output: CSV

# Explicit --out overrides CQLITE_OUT env var
CQLITE_OUT=json cqlite --schema schema.cql --data-dir ./sstables \
  --query "SELECT id FROM test_basic.simple_table LIMIT 1" \
  --out csv
# Output: CSV (explicit flag wins)
```

The `CQLITE_OUT` environment variable sets the default value for `--out`, not for `--format`. An explicit `--out <value>` on the command line overrides the env var.

---

## Subcommands

### `repl` — Interactive REPL

Start an interactive text-based REPL with meta-commands and CQL query support.

```
Usage: cqlite repl
```

**Meta-commands** available inside the REPL:

| Command | Description |
|---------|-------------|
| `:config data-dir <path>` | Set the data directory |
| `:config schema <path>` | Load a schema file |
| `:schema` | Show loaded schema |
| `:status` | Show connection status |
| `:health` | Show health metrics |
| `:help` | Show meta-command help |

**Status line** shown in the prompt:

```
[OK] Mem: 24.5 MB | Data: 1.2 GB
cqlite>
```

Status metrics refresh every 5 seconds. The status line is disabled for piped output.

**Invocation example:**

```bash
cqlite repl \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables
```

For the full-screen terminal UI, use `cqlite tui` instead.

---

### `tui` — Terminal UI

Start a full-screen terminal UI with panels for tables, query results, and history.

```
Usage: cqlite tui
```

**Keyboard shortcuts:**

| Key | Action |
|-----|--------|
| `Tab` | Navigate between panels |
| `F2` | Toggle tables panel |
| `F3` | Toggle query panel |
| `F4` | Toggle history panel |
| `F5` | Reset layout |
| `Ctrl+C` / `Esc` | Exit |

**Status bar** shown at the bottom:

```
Health: OK | Mem: 24.5 MB | Data: 1.2 GB | Status: Ready | Mode: EDIT
```

**Invocation example:**

```bash
cqlite tui \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables
```

For the basic text REPL, use `cqlite repl`.

---

### `query` — One-shot query (subcommand form)

Friendly wrapper for one-shot query execution. The query is the positional argument.

```
Usage: cqlite query [OPTIONS] <QUERY>
```

| Option | Description |
|--------|-------------|
| `--explain` | Show execution plan |
| `--timing` | Show query timing |

**Example:**

```bash
cqlite \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  --out json \
  query "SELECT id, name FROM test_basic.simple_table LIMIT 2"
```

Output:
```json
[
  {
    "id": "0023ece7-7c4e-4705-9068-d1a59ec5fe19",
    "name": "Debbie Soto"
  },
  {
    "id": "009fb913-7173-40df-b4ea-67ed6834cfe5",
    "name": "Richard Parker"
  }
]
```

The top-level `--execute` / `-e` / `--query` flags (not the `query` subcommand) are the more common one-shot interface. See [One-shot mode](#one-shot-mode) below.

---

### `import` — Import data from file

Import data from a file into a table.

```
Usage: cqlite import [OPTIONS] --table <TABLE> <FILE>
```

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `<FILE>` | — | — | Input file path (positional) |
| `--table <TABLE>` | `-t` | required | Target table name |
| `--format <FORMAT>` | `-f` | `csv` | Input format: `csv`, `json`, `parquet` |
| `--mapping <MAPPING>` | `-m` | — | Column mapping (`col1:field1,col2:field2`) |
| `--batch-size <N>` | — | `1000` | Batch size for imports |

---

### `export` — Export data to file

Export data from a table to a file.

```
Usage: cqlite export [OPTIONS] --table <TABLE> <FILE>
```

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `<FILE>` | — | — | Output file path (positional) |
| `--table <TABLE>` | `-t` | required | Source table name |
| `--format <FORMAT>` | `-f` | `csv` | Export format: `csv`, `json`, `parquet`, `cql` |
| `--query <QUERY>` | `-q` | — | Query filter (WHERE clause) |
| `--limit <LIMIT>` | `-l` | — | Maximum rows to export |

---

### `admin` — Administrative commands

```
Usage: cqlite admin <COMMAND>
```

| Subcommand | Description |
|------------|-------------|
| `info` | Display database information |
| `compact` | Compact database files |
| `backup` | Backup database |
| `restore` | Restore database from backup |

---

### `schema` — Schema management

```
Usage: cqlite schema <COMMAND>
```

| Subcommand | Description |
|------------|-------------|
| `list` | List all tables |
| `describe` | Describe table structure |
| `create` | Create table from schema file |
| `drop` | Drop table |
| `load` | Load schemas from files or directories |

---

### `bench` — Performance benchmarks

```
Usage: cqlite bench <COMMAND>
```

| Subcommand | Description |
|------------|-------------|
| `read` | Run read performance benchmark |
| `write` | Run write performance benchmark |
| `mixed` | Run mixed workload benchmark |

---

### `read-sstable` — Low-level SSTable inspection

Direct SSTable inspection, bypassing the query engine and schema. Useful for debugging.

```
Usage: cqlite read-sstable [OPTIONS] <FILE>
```

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `<FILE>` | — | — | SSTable file or directory path (positional) |
| `--format <FORMAT>` | `-f` | `table` | Output format: `table`, `json`, `csv`, `parquet` |
| `--limit <LIMIT>` | `-l` | — | Limit number of rows |
| `--skip <SKIP>` | — | `0` | Skip N rows |
| `--keys-only` | — | — | Show only partition/clustering keys |
| `--raw` | — | — | Show raw binary data |
| `--verbose` | — | — | Enable detailed output |

---

### `info` — File metadata and statistics

Show SSTable or database file metadata, stats, and optional validation.

```
Usage: cqlite info [OPTIONS] [PATH]
```

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `[PATH]` | — | — | Target file or database path (positional, optional) |
| `--format <FORMAT>` | `-f` | `text` | Output format: `text`, `json`, `csv` |
| `--detailed` | `-d` | — | Show detailed information |

---

### `maintenance` — Background compaction (write-support)

Run incremental background compaction within a time budget. Call repeatedly from a background task for continuous compaction. Requires `--writable` mode.

```
Usage: cqlite maintenance [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--budget-ms <N>` | `100` | Time budget in milliseconds for this maintenance step |

**Example:**

```bash
cqlite \
  --writable --write-dir /tmp/cqlite-data \
  --schema test-data/schemas/basic-types.cql \
  maintenance --budget-ms 100
```

Output:
```
Maintenance complete:
  Time spent: 42ns
  Rows merged: 0
  Bytes written: 0 bytes
  Pending compaction: false
```

---

### `write-stats` — Write engine statistics (write-support)

Display current write engine statistics: memtable size, row count, WAL size, and generation number. Requires `--writable` mode.

```
Usage: cqlite write-stats
```

**Example:**

```bash
cqlite \
  --writable --write-dir /tmp/cqlite-data \
  --schema test-data/schemas/basic-types.cql \
  write-stats
```

Output:
```
Write Engine Statistics:
  Memtable size: 0 bytes
  Memtable rows: 0
  WAL size: 0 bytes
  Generation: 1
```

---

### `export-sstable` — Export SSTables (write-support)

Export data from the write engine as Cassandra-compatible SSTables. Requires `--writable` mode.

```
Usage: cqlite export-sstable [OPTIONS] <OUTPUT>
```

| Option | Default | Description |
|--------|---------|-------------|
| `<OUTPUT>` | — | Output directory for exported SSTables (positional) |
| `--keyspace <KEYSPACE>` | `export` | Keyspace name for the exported SSTable |
| `--table <TABLE>` | `data` | Table name for the exported SSTable |
| `--compact` | — | Run compaction before export to merge multiple SSTables |
| `--skip-validate` | — | Skip validation after export |

**Example:**

```bash
cqlite \
  --writable --write-dir /tmp/cqlite-data \
  --schema test-data/schemas/basic-types.cql \
  export-sstable /tmp/exported-sstables \
  --keyspace my_keyspace --table my_table --compact
```

---

## One-shot mode

The top-level `--execute` flag (short: `-e`) and its alias `--query` execute a single CQL statement and exit. This is the most common usage pattern for scripting and automation.

**`--execute` and `--query` are identical aliases** — both are accepted by the parser and produce the same behavior.

**Required flags for one-shot mode:**
- `--schema <path>` — schema file defining the keyspace/table structure
- `--data-dir <dir>` — data directory containing the SSTable files

**Verified example** (run against `cassandra5-small-full-v3.1`):

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

The same query with `-e` (short form of `--execute`):

```bash
cqlite \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  -e "SELECT id, name, age FROM test_basic.simple_table LIMIT 3" \
  --out json
```

---

## CSV output

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

---

## Parquet output

Parquet is a binary format and **cannot be written to stdout**. You must provide `--output <file>`:

```bash
cqlite \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  --query "SELECT id, name, age FROM test_basic.simple_table LIMIT 100" \
  --out parquet \
  --output results.parquet
```

Output (printed to stderr):
```
Output written to: results.parquet
```

For type-fidelity caveats with Parquet output, see [Output Formats: Parquet](/cqlite/user-docs/output-formats/#parquet--apache-parquet-binary-format).

---

## Environment variables

| Variable | Equivalent flag | Description |
|----------|-----------------|-------------|
| `CQLITE_SCHEMA` | `--schema` | Schema file path |
| `CQLITE_DATA_DIR` | `--data-dir` | SSTable data directory |
| `CQLITE_OUT` | `--out` | Query result output format |
| `CQLITE_OUTPUT` | `--output` | Output file path |
| `CQLITE_DATASETS_ROOT` | — | Root directory for `--dataset` shortcut |
| `CQLITE_LIMIT` | `--limit` | Row cap |
| `CQLITE_PAGE_SIZE` | `--page-size` | Pagination size |
| `CQLITE_NO_COLOR` | `--no-color` | Disable color |
| `CQLITE_WRITABLE` | `--writable` | Enable write mode |
| `CQLITE_WRITE_DIR` | `--write-dir` | Write directory |
| `CQLITE_ENABLE_SELECT_FALLBACK` | `--enable-select-fallback` | Enable experimental SELECT fallback |

**`CQLITE_OUT` example** (verified):

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

## Write mode

Write mode enables mutations, memtable management, and SSTable export. All write operations require both `--writable` and `--write-dir`.

**Prerequisites:**
- Build with `--features write-support` (or use a release binary that includes write support)
- Provide `--write-dir` pointing to a writable directory

**Writing a mutation:**

```bash
cqlite \
  --writable \
  --write-dir /tmp/cqlite-write \
  --schema test-data/schemas/basic-types.cql \
  --mutation '{"table":{"keyspace":"test_basic","table":"simple_table"},"partition_key":[{"Uuid":[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]}],"clustering_key":[],"operations":[{"Write":{"column":"name","value":{"Text":"Test"}}}],"timestamp_micros":1704067200000000}'
```

**Flushing memtable to SSTable:**

```bash
cqlite \
  --writable \
  --write-dir /tmp/cqlite-write \
  --schema test-data/schemas/basic-types.cql \
  --flush
```

For write statistics, compaction, and export see the `write-stats`, `maintenance`, and `export-sstable` subcommands above.

---

## Logging

Log output goes to **stderr only**, so it never contaminates JSON/CSV stdout output. Control verbosity with:

- (none) — `INFO` level
- `-v` — `DEBUG` level
- `-vv` or `-vvv` — `TRACE` level
- `-q` / `--quiet` — `ERROR` level only

To suppress all log output when piping query results:

```bash
cqlite --quiet \
  --schema schema.cql \
  --data-dir ./sstables \
  -e "SELECT * FROM ks.tbl LIMIT 10" \
  --out json
```

<!-- TODO(W8): link output-formats page when merged -->

**See also**: [Output Formats](/cqlite/user-docs/output-formats/) for JSON, CSV, and Parquet format details.
