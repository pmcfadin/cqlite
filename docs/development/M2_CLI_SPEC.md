## CQLite CLI M2 Specification (cqlsh‑compatible UX)

### Goals (per PRD M2)

- Interactive REPL and one‑shot modes usable end‑to‑end for reading Cassandra 5 SSTables via provided schema(s).
- Familiar cqlsh‑style syntax and ergonomics where appropriate.
- First‑class configuration for schema sources and Cassandra data directory.
- Status and health commands to show config state and schema↔data synchronization.
- JSON and CSV output in M2; Parquet slated for M3 per PRD.

---

### Operating Modes

- One‑shot (non‑interactive):
  - Execute a query or script against local SSTable data using provided schema(s).
  - Primary flags: `--schema`, `--data-dir`, `--execute|-e`, `--file|-f`, `--out`, `--format`, `--limit`, `--page-size`, `--cassandra-version`.
  - Example:
    - `cqlite --schema schemas/ --data-dir /var/lib/cassandra/data -e "SELECT * FROM ks.users WHERE id = ?" --out json`
    - `cqlite --schema ks.cql --data-dir ./test-data/cassandra5 -f script.cql --out csv`

- Interactive REPL:
  - Starts with `cqlite repl` or `cqlite` (default) and supports meta‑commands with `:` prefix (cqlsh‑like).
  - Schema/data‑dir configuration via `:config` and `:schema` commands.
  - Query execution accepts regular CQL (subset aligned with read‑only operations for M2): `SELECT`, `DESCRIBE`, `USE`, `DESC`, etc.
  - Guideline: Prefer plain CQL keywords (`DESCRIBE`/`USE`/`SELECT`) for queries; reserve `:` for REPL meta‑commands only. Aliases like `:describe` may exist, but keyword forms are canonical for cqlsh parity.

---

### Global CLI Flags (common to all commands)

- `--config <FILE>`: Load config (TOML/YAML/JSON). Precedence: flags > env > file > defaults.
- `--schema <PATH>`: File (.cql or .json) or directory containing schemas. Repeatable; order defines precedence.
- `--data-dir <DIR>`: Cassandra data directory root (e.g., `/var/lib/cassandra/data`).
- `--execute, -e <CQL>`: Execute a single CQL statement in one‑shot mode.
- `--file, -f <CQL_FILE>`: Execute statements from a file (semicolon‑terminated).
- `--out <table|json|csv>`: Output format for query results (table = cqlsh‑compatible). Parquet in M3.
- `--limit <N>`: Cap rows.
- `--page-size <N>`: Reader and display pagination size.
- `--auto-detect`: Enable best‑effort auto detection (format/version) when available.
- `--cassandra-version <VER>`: Hint (e.g., `5.0`) for format compatibility.
- `-v/--verbose`, `-q/--quiet`, `--no-color`.

Notes:
- `--schema` and `--data-dir` populate the REPL session defaults if `repl` is subsequently launched.
- Config file keys align with `cqlite-cli/src/config.rs` (`data_directory`, `default_keyspace`, `repl.*`, etc.).

---

### One‑Shot Commands

- `cqlite query` (friendly wrapper; optional if `-e/--file` is present at top‑level):
  - `cqlite query --schema <PATH> --data-dir <DIR> -e "SELECT ..." --out json`
  - `cqlite query --schema ks.cql --data-dir ./data --file script.cql --out table`

- `cqlite read-sstable` (low‑level, already present):
  - Keep for direct SSTable inspection: `cqlite read-sstable <sstable_or_dir> --schema <FILE> --format <table|json|csv>`.

- `cqlite info` (already present):
  - File metadata, stats, optional `--validate`.

---

### REPL Meta‑Commands (cqlsh‑inspired)

- Session & Help
  - `:help [topic]`
  - `:quit | :exit | :q`
  - `:clear | :cls`
  - `:history`

- Navigation & Introspection
  - `:use <keyspace>`
  - `:keyspaces`
  - `:tables` (uses current keyspace if set; else grouped)
  - `:describe <[keyspace.]table> | :desc <...>` (DDL‑like view)
  - `DESC ...` (cqlsh shorthand supported in parser)

- Configuration
  - `:config` (show effective config)
  - `:config data-dir <PATH>` (persist to session; optional `:config save [FILE]` writes config)
  - `:config page-size <N>`
  - `:config timing on|off`

- Schema Management
  - `:schema list` (loaded schema sources; effective tables/types)
  - `:schema load <FILE|DIR>` (parse CQL or JSON; merge)
  - `:schema unload <NAME>|all`
  - `:schema show <[keyspace.]table>` (effective model)
  - `:schema refresh` (re‑parse files)

- Data Discovery & Sync
  - `:discover [--refresh]` (scan `data-dir` for keyspaces/tables)
  - `:status` (see below): show sync between discovered data and loaded schemas
  - `:health` (see below): config and environment checks

- Scripting
  - `:source <FILE>` execute commands/CQL from file

---

### Status and Health Semantics

- `:status` (schema↔data sync overview)
  - Show: data dir, discovery timestamp, keyspaces, tables found.
  - Schema coverage: counts and sample deltas
    - tables with schema: X
    - tables missing schema: Y (list a few; suggest `:schema load ...`)
    - schemas without data: Z (list a few)
  - Version hints: detected vs configured Cassandra version.
  - State badge: Green (≥95% covered), Yellow (50‑95%), Red (<50% or critical errors).

- `:health` (configuration/environment checks)
  - Readability of `data-dir`; directory layout sanity
  - Schema parse success; invalid files with error counts
  - Config coherence: page size, timing, color; config file path if loaded
  - IO/format compatibility probes: compression codecs available; platform limits
  - Actionable tips with next‑step commands

---

### Output & Formatting

- Table output: cqlsh‑compatible table rendering using `CqlshTableFormatter` (existing module).
- JSON: array of row objects; stable key order where possible.
- CSV: header row + rows; basic type stringification matching cqlsh conventions.
- Parquet: M3 (export writers).

---

### Error Handling & UX

- Follow cqlsh phrasing where reasonable; include concise hints.
- For unknown meta‑commands: suggest `:help`.
- For missing schema: prompt with `:schema load` or `--schema` examples.
- For unset `data-dir`: prompt with `:config data-dir <PATH>`.

---

### Configuration Model

- File keys mirror `cqlite-cli/src/config.rs`:
  - `data_directory`, `default_keyspace`, `repl.enable_history`, `repl.page_size`, `output.colors`, etc.
- Precedence: CLI flags > env vars > config file > defaults.
- `:config save [FILE]` writes current effective config.

---

### Compatibility Notes (cqlsh)

- Support `DESCRIBE`/`DESC`, `USE`, `SELECT` read‑only subset.
- Meta‑commands are `:`‑prefixed (cqlsh uses cql directly; we add `:` for REPL control commands).
- Prefer keyword forms in documentation and examples; use `:` only for non‑CQL meta operations (e.g., `:config`, `:status`, `:schema`).
- Align table formatting, header casing, and value rendering.

---

### Examples

```bash
# One‑shot: query with schema directory and data dir
cqlite --schema ./schemas --data-dir /var/lib/cassandra/data -e "SELECT * FROM ks.users LIMIT 5" --out table

# One‑shot: run script, output as CSV
cqlite --schema ks.cql --data-dir ./data -f statements.cql --out csv

# Interactive: configure and explore
cqlite repl
  :config data-dir ./test-data/cassandra5
  :schema load ./schemas
  :status
  :use ks
  :tables
  SELECT * FROM users LIMIT 10;
```

#### Example REPL session (simulation)

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


---

### Phased Delivery Plan (convertible to issues)

- Phase 1: One‑shot plumbing and global flags
  - Implement top‑level flags: `--schema`, `--data-dir`, `-e/--file`, `--out`, `--limit`, `--page-size`.
  - Wire to existing `read-sstable`/`info` pathways; unify output via formatter.
  - Basic tests: CLI arg parsing, happy‑path JSON/CSV output, error messages.

- Phase 2: REPL config and status
  - Implement `:config` (show/set data‑dir, page‑size, timing) and persistence.
  - Implement discovery API call for data‑dir and `:status` coverage summary.
  - Add `:schema list|load|show|refresh` scaffolding; parse .cql/.json files.
  - Tests: REPL parsing, config mutation, status rendering for synthetic layouts.

- Phase 3: cqlsh‑like introspection and formatting
  - `:keyspaces`, `:tables`, `:describe` using schema catalog; `DESC` support.
  - Integrate `CqlshTableFormatter` for table output consistency.
  - Tests: golden snapshots against representative schemas.

- Phase 4: Health diagnostics and polish
  - Implement `:health` checks and actionable hints.
  - Config precedence, `:config save`, `--no-color` fidelity.
  - CLI help/usage docs; examples; error polish.

---

### Mapping to Code (existing work to leverage)

- REPL command parsing: `cqlite-cli/src/repl/command_parser.rs` (supports `:config`, `:tables`, `:describe`, `:use`, `DESC`).
- Enhanced REPL scaffolding including `:config data-dir` and help text: `cqlite-cli/src/enhanced_interactive.rs`.
- Formatter for cqlsh‑compatible output: `cqlite-cli/src/formatter.rs`.
- Config model and file formats: `cqlite-cli/src/config.rs`.
- Info/read pathways in `cqlite-cli/src/commands/` (e.g., `info.rs`, `mod.rs` stubs) for one‑shot wiring.

---

### Acceptance for M2 (final)

- One‑shot mode
  - Supports `--schema <FILE|DIR>` (CQL/JSON), `--data-dir <DIR>`, `-e/--execute <CQL>` and `-f/--file <CQL_FILE>`.
  - Executes read‑only `SELECT` with `WHERE` on primary/partition key and `LIMIT`; honors `--limit`, `--page-size`, `--no-color`.
  - Output formats: `--out table|json|csv` produce correct rows; table matches cqlsh formatting for headers, separators, and row count.
  - Helpful errors for missing/invalid schema and unset `--data-dir` (with next‑step hints); non‑zero exit codes on failure.

- REPL mode
  - Launch via `cqlite repl` (or default `cqlite`) and accept: `:config`, `:schema list|load|show|refresh`, `:status`, `:health`, `:use`, `:keyspaces`, `:tables`, `DESCRIBE`/`DESC`, `SELECT ... LIMIT`.
  - Maintains session defaults (data‑dir, page‑size, timing, keyspace); history enabled; `:source <FILE>` executes statements.
  - Renders query results with cqlsh‑compatible table formatting; respects `--no-color`.

- Status & Health
  - `:status` scans `data-dir`, shows discovery timestamp, keyspaces/tables, schema coverage summary (tables with schema, tables missing schema, schemas without data), version hints, and Green/Yellow/Red badge per spec.
  - `:health` validates config and environment (data‑dir readability/layout, schema parse success with counts, codec availability, config coherence) and surfaces actionable tips.

- Documentation & Tests
  - CLI usage docs include one‑shot and REPL examples using repository `test-data` paths; CLI help text is accurate.
  - Integration tests cover one‑shot JSON/CSV/table output, REPL essentials (`:config`, `:schema`, `:status`, `:tables`, `DESCRIBE`, `SELECT`), and golden snapshots for table formatting.

- Platforms & Performance
  - Works on macOS/Linux developer environments against local datasets; no explicit performance targets in M2 beyond functional responsiveness.

---

### Prioritized Scope (M2)

- Must‑have
  - One‑shot flags and execution path wired to core query engine.
  - REPL with `:config`, `:schema`, `:status`, `:health`, `:use`, `:keyspaces`, `:tables`, `DESCRIBE`/`DESC`, and `SELECT` subset.
  - cqlsh‑compatible table formatting, plus JSON/CSV writers.
  - Data‑dir discovery and schema coverage reporting.
  - Updated docs and integration tests using `test-data`.

- Should‑have
  - `:config save [FILE]` persistence and env var overrides.
  - `:schema unload <NAME>|all`.

- Out‑of‑scope for M2 (deferred)
  - Parquet output (M3), TUI mode, advanced SELECT optimizer (`state_machine`), DML/DDL mutations, remote cluster connectivity.

---

### Test Data & Fixtures

- Default local paths for development and tests:
  - `--data-dir` = `/Users/patrick/local_projects/cqlite/test-data/datasets`
  - `--schema` = `/Users/patrick/local_projects/cqlite/test-data/schemas`
- Optional environment variables to simplify invocation:
  - `CQLITE_DATA_DIR=/Users/patrick/local_projects/cqlite/test-data/datasets`
  - `CQLITE_SCHEMA=/Users/patrick/local_projects/cqlite/test-data/schemas`
- Regeneration scripts (when needed): `test-data/scripts/start-clean.sh`, `test-data/scripts/export.sh`.

---

### Exit Codes

- `0` success; `2` invalid CLI args; `3` schema errors; `4` data‑dir/discovery errors; `5` query execution errors.

---

### Documentation Deliverables

- Update `cqlite-cli/CLI_USAGE_EXAMPLES.md` with one‑shot and REPL flows using `test-data`.
- Ensure `--help` output matches this spec (flags, subcommands, examples).

