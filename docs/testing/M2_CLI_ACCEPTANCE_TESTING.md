# M2 CLI Acceptance Testing

This document tracks acceptance testing for the CQLite CLI (Milestone 2).

**Reference**: `scripts/validation/validate_cli_acceptance.sh`
**PRD**: `docs/development/PRD.md`
**CLI Guide**: `docs/user-guides/cli.md`

---

## Progress Summary

| # | Test | Status | Issue |
|---|------|--------|-------|
| A | Help and version | :white_check_mark: Passed | |
| B | One-shot table output | :white_check_mark: Passed | |
| C | One-shot JSON output | :white_check_mark: Passed | #227 (Fixed) |
| D | One-shot CSV output | :white_check_mark: Passed | Fixed in-session |
| E | Script execution (`-f` flag) | :x: Failed | #229 |
| F | Limits and pagination | :x: Failed | #228 |
| G | Env vs flag precedence | :white_check_mark: Passed | |
| H | REPL session commands | :x: Failed | #230 |
| I | REPL query execution | :x: Failed | #230 |
| K | Error handling and exit codes | :construction: Partial | #231 |
| L | Info command | :x: Failed | #232 |

**Legend**: :white_check_mark: Passed | :x: Failed | :hourglass: Pending | :construction: In Progress

---

## Test Details

### A. Help and Version

**Command**:
```bash
cargo run --package cqlite-cli -- --help
cargo run --package cqlite-cli -- --version
```

**Acceptance Criteria**:
- [x] `--help` output includes `--schema`
- [x] `--help` output includes `--data-dir`
- [x] `--help` output includes `-e, --execute`
- [x] `--help` output includes `-f, --file`
- [x] `--help` output includes `--out`
- [x] `--help` output includes `--limit`
- [x] `--help` output includes `--page-size`
- [x] `--version` displays version number (`cqlite 0.1.0`)

**Status**: :white_check_mark: Passed

---

### B. One-shot Table Output

**Command**:
```bash
cargo run --package cqlite-cli -- \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  -e "SELECT * FROM test_basic.simple_table LIMIT 3"
```

**Acceptance Criteria**:
- [x] Query executes without error
- [x] Output displays in formatted table
- [x] Column headers present
- [x] Row count footer displayed

**Status**: :white_check_mark: Passed

---

### C. One-shot JSON Output

**Command**:
```bash
cargo run --package cqlite-cli -- \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  -e "SELECT * FROM test_basic.simple_table LIMIT 3" \
  --out json
```

**Acceptance Criteria**:
- [x] Valid JSON output (parseable by `jq`)
- [x] Values properly formatted (UUIDs, dates, etc.)
- [x] Column names as keys

**Status**: :white_check_mark: Passed
**Notes**: Fixed in Issue #227. JSON now uses `ValueFormatter` for human-readable formatting:
- `description`: `0x...` hex format
- `account_balance`: `69799.73` decimal string
- `birth_date`: `2025-02-22` date format
- `created`: `2025-10-06 01:12:05.926+0000` timestamp format
- `duration_val`: `3033000000000ns` duration format
- `work_time`: `01:12:05.926782000` time format

---

### D. One-shot CSV Output

**Command**:
```bash
cargo run --package cqlite-cli -- \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  -e "SELECT * FROM test_basic.simple_table LIMIT 3" \
  --out csv
```

**Acceptance Criteria**:
- [x] Header row with column names
- [x] Values properly formatted (not Debug output)
- [x] Decimal: `69799.73` (not `DECIMAL(scale=2, unscaled=[...])`)
- [x] Date: `2025-02-22` (not `DATE(20141)`)
- [x] UUID: `0023ece7-7c4e-4705-...` (not `UUID(...)`)
- [x] Time: `01:12:05.926782000` (not `TIME(...)`)

**Status**: :white_check_mark: Passed
**Notes**: Fixed `print_csv_format()` to use `ValueFormatter` instead of `Display` trait.

---

### E. Script Execution

**Command**:
```bash
cat > /tmp/test_script.sql << 'EOF'
SELECT * FROM test_basic.simple_table LIMIT 5;
SELECT * FROM test_basic.composite_key_table LIMIT 3;
EOF

cargo run --package cqlite-cli -- \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  -f /tmp/test_script.sql \
  --out table
```

**Acceptance Criteria**:
- [ ] Multiple statements execute in sequence
- [ ] Results displayed for each statement
- [ ] Errors in one statement don't block others (or do they? TBD)

**Status**: :hourglass: Pending

---

### F. Limits and Pagination

**Commands**:
```bash
# Test --limit
cargo run --package cqlite-cli -- \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  --limit 10 \
  -e "SELECT * FROM test_basic.simple_table" \
  --out json

# Test --page-size
cargo run --package cqlite-cli -- \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  --page-size 25 \
  -e "SELECT * FROM test_basic.simple_table LIMIT 50" \
  --out table
```

**Acceptance Criteria**:
- [ ] `--limit 10` returns at most 10 rows
- [ ] `--page-size` affects pagination behavior
- [ ] Query LIMIT and CLI --limit interact correctly

**Status**: :hourglass: Pending

---

### G. Environment vs Flag Precedence

**Command**:
```bash
CQLITE_OUT=json cargo run --package cqlite-cli -- \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  -e "SELECT * FROM test_basic.simple_table LIMIT 1" \
  --out csv
```

**Acceptance Criteria**:
- [ ] `--out` flag takes precedence over `CQLITE_OUT` env var
- [ ] Output is CSV (not JSON)

**Status**: :hourglass: Pending

---

### H. REPL Session Commands

**Commands** (interactive or via script):
```
:status
:keyspaces
:tables
:health
:help
```

**Acceptance Criteria**:
- [ ] `:status` shows connection/ingestion status
- [ ] `:keyspaces` lists available keyspaces
- [ ] `:tables` lists tables in current keyspace
- [ ] `:health` shows system health
- [ ] `:help` displays available commands

**Status**: :hourglass: Pending

---

### I. REPL Query Execution

**Commands** (interactive or via script):
```sql
USE test_basic;
DESCRIBE simple_table;
SELECT id, name FROM simple_table LIMIT 5;
```

**Acceptance Criteria**:
- [ ] `USE keyspace` switches context
- [ ] `DESCRIBE table` shows schema
- [ ] `SELECT` queries execute and display results

**Status**: :hourglass: Pending

---

### K. Error Handling and Exit Codes

**Commands**:
```bash
# Exit code 3: Schema file not found
cargo run --package cqlite-cli -- \
  --schema /does/not/exist \
  --data-dir test-data/datasets/sstables \
  -e "SELECT * FROM test_basic.simple_table LIMIT 1"
echo "Exit code: $?"

# Exit code 4: Data directory missing
cargo run --package cqlite-cli -- \
  --schema test-data/schemas/basic-types.cql \
  -e "SELECT * FROM test_basic.simple_table LIMIT 1"
echo "Exit code: $?"

# Exit code 5: Query error (unsupported operation)
cargo run --package cqlite-cli -- \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  -e "SELECT * FROM test_basic.simple_table ORDER BY name"
echo "Exit code: $?"
```

**Acceptance Criteria**:
- [x] Exit code 3 for schema file errors
- [ ] Exit code 4 for data directory errors
- [x] Exit code 5 for query execution errors
- [x] Error messages are descriptive

**Status**: :construction: Partial Pass
**Notes**: Exit codes 3 and 5 work correctly. Missing `--data-dir` returns exit code 3 instead of 4 with confusing "schema not found" message. ORDER BY is actually implemented and works (not an error case).

---

### L. Info Command

**Command**:
```bash
cargo run --package cqlite-cli -- info test-data/datasets/sstables/test_basic
```

**Acceptance Criteria**:
- [ ] Displays SSTable information
- [ ] Shows file sizes, formats, compression
- [ ] Works with directory path

**Status**: :x: Failed
**Notes**: Info command fails with "Unknown SSTable component: Statistics.db.txt" when directory contains auxiliary test files (`.jsonl`, `.txt`). The scanner should ignore unrecognized file extensions instead of failing.

---

## Issues Found During Testing

| Issue # | Description | Status |
|---------|-------------|--------|
| #226 | BTI Index warnings too noisy (downgrade to debug) | Open |
| #227 | JSON output uses raw values instead of human-readable formatting | **Fixed** |
| #228 | --limit flag ignored when using JSON output format | Open |
| #229 | Script execution (-f) shows wrong columns due to schema lookup failure | Open |
| #230 | REPL fails to start when schema directory contains UDT-only JSON files | Open |
| #231 | Missing --data-dir returns exit code 3 instead of 4 with confusing error message | Open |
| #232 | Info command fails on SSTable directories containing extra files (.jsonl, .txt) | Open |

---

## Test Environment

```
Platform: macOS Darwin 24.6.0
Rust: (run `rustc --version`)
Date: 2025-12-19
```

---

## Running the Automated Suite

```bash
# Full automated acceptance test
./scripts/validation/validate_cli_acceptance.sh

# Individual test with verbose output
CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
  cargo run --package cqlite-cli -- \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  -e "SELECT * FROM test_basic.simple_table LIMIT 5" \
  --out json
```
