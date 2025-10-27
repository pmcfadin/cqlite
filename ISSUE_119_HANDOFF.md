# Issue #119 Handoff Document

**Issue:** [#119 - M2-CLI: Implement QueryResult → JSON/CSV writers (deterministic)](https://github.com/pmcfadin/cqlite/issues/119)
**Status:** Reopened - Table formatting incomplete
**Priority:** P1 (M2-CLI milestone blocker)
**Estimate:** 2-3 days for experienced Rust developer

---

## Executive Summary

Issue #119 was closed prematurely. While JSON/CSV output writers are complete and working, **table formatting is not integrated into the REPL output path**. Currently, queries return correct data but display a placeholder message instead of a formatted table.

### What Works ✅
- Query execution (verified returning 3 rows from `test_basic.simple_table`)
- JSON output format
- CSV output format
- `CqlshTableFormatter` module exists in codebase

### What's Broken ❌
- Table formatter not connected to REPL query display
- Shows placeholder: `"(Table formatting would be implemented here)"`
- UUID type warnings: `"Unsupported partition key type: UUID PRIMARY KEY"`

---

## Quick Start for New Developer

### 1. Reproduce the Issue

```bash
# Build the CLI
cargo build --package cqlite-cli

# Create test config
cat > test.toml << 'EOF'
data_directory = "./test-data/datasets/sstables"
schema_paths = ["./test-data/schemas/basic-types.cql"]
default_keyspace = "test_basic"

[repl]
page_size = 25
show_timing = true
prompt = "test_basic> "
EOF

# Start REPL
./target/debug/cqlite --config test.toml repl

# Query (in REPL)
test_basic> SELECT * FROM simple_table LIMIT 3;
```

**Current Output:**
```
📊 Results: 3 rows returned
(Table formatting would be implemented here)
```

**Expected Output:**
```
 id                                   | name | value
--------------------------------------+------+-------
 00000000-0000-0000-0000-000000000001 | foo  |   100
 00000000-0000-0000-0000-000000000002 | bar  |   200
 00000000-0000-0000-0000-000000000003 | baz  |   300

(3 rows)
Execution time: 95ms
```

### 2. Locate the Integration Point

The placeholder text appears in:
```
cqlite-cli/src/repl/engine.rs (around lines 400-450)
```

Search for: `"(Table formatting would be implemented here)"`

This is where you need to:
1. Import the `CqlshTableFormatter`
2. Convert `QueryResult` to table format
3. Print the formatted table to stdout

### 3. Reference Implementations

**JSON Output Writer:** Shows how to convert `QueryResult` to output format
**CSV Output Writer:** Shows column iteration and value stringification
**CqlshTableFormatter:** Existing module that needs to be wired up

---

## Technical Implementation Guide

### Architecture Overview

```
QueryResult (from core)
    ↓
[OUTPUT FORMAT SELECTION]
    ↓
├─→ JSON Writer ✅ (working)
├─→ CSV Writer ✅ (working)
└─→ Table Formatter ❌ (missing integration)
    ↓
stdout
```

### Key Files to Modify

#### 1. `cqlite-cli/src/repl/engine.rs`
**Current code (approximate location):**
```rust
// After query execution
println!("📊 Results: {} rows returned", result.rows.len());
println!("(Table formatting would be implemented here)");
```

**Needs to become:**
```rust
// After query execution
use crate::formatter::CqlshTableFormatter;

let formatter = CqlshTableFormatter::new(self.config.enable_colors);
let table_output = formatter.format(&result)?;
println!("{}", table_output);

if self.config.show_timing {
    println!("Execution time: {}ms", execution_time_ms);
}
```

#### 2. `cqlite-cli/src/formatter/mod.rs`
Ensure `CqlshTableFormatter` is properly exported:
```rust
pub mod table;
pub use table::CqlshTableFormatter;
```

#### 3. `cqlite-cli/src/output/mod.rs`
Add table output writer to match JSON/CSV pattern:
```rust
pub enum OutputFormat {
    Table,  // Add this
    Json,
    Csv,
}

pub fn write_output(result: &QueryResult, format: OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Table => write_table(result),
        OutputFormat::Json => write_json(result),
        OutputFormat::Csv => write_csv(result),
    }
}
```

#### 4. `cqlite-core/src/types.rs`
Fix UUID display formatting:
```rust
impl Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::Uuid(uuid) => write!(f, "{}", uuid.hyphenated()),
            // ... other types
        }
    }
}
```

### Table Formatter Requirements

**Column Alignment:**
- Numeric types (int, bigint, float, double): right-aligned
- Text types (text, varchar, ascii): left-aligned
- UUID, timestamp: left-aligned
- Boolean: center-aligned

**Border Style (cqlsh-compatible):**
```
 column1 | column2 | column3
---------+---------+---------
 value1  | value2  | value3
```

**Special Cases:**
- NULL values display as `"null"`
- Empty strings display as `""`
- Collections display as `[item1, item2, ...]` for lists/sets
- Maps display as `{key1: val1, key2: val2}`

**Column Width:**
- Auto-size based on content
- Max width: 50 characters (truncate with `...`)
- Min width: column name length

**Footer:**
```
(N rows)
Execution time: XXms  # if timing enabled
```

---

## Acceptance Criteria Checklist

### Functional Requirements

- [ ] **Table rendering works in REPL**
  - Query results display as formatted table (not placeholder)
  - Column headers shown
  - Rows aligned correctly

- [ ] **UUID type support**
  - No warnings about unsupported UUID types
  - UUIDs display in hyphenated format: `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`

- [ ] **All CQL primitive types display correctly**
  - `text`, `varchar`, `ascii`
  - `int`, `bigint`, `varint`
  - `float`, `double`, `decimal`
  - `boolean`
  - `timestamp`
  - `uuid`, `timeuuid`
  - `blob` (display as hex)

- [ ] **Collection types display correctly**
  - `list<type>`
  - `set<type>`
  - `map<key, value>`

- [ ] **NULL handling**
  - NULL values display as `"null"`
  - Not confused with string `"null"`

- [ ] **Output format switching**
  - `--out table` flag works (default)
  - `--out json` flag works
  - `--out csv` flag works
  - `:config out <format>` command works in REPL

- [ ] **Color support**
  - Colors work when enabled (headers, borders)
  - `--no-color` flag disables colors
  - `config.repl.enable_colors = false` respected

### Testing Requirements

- [ ] **Integration tests added**
  - `test_basic.simple_table` table output
  - Collection types table output
  - NULL values table output
  - Wide rows with pagination

- [ ] **Golden snapshots**
  - Snapshot test for table format (no colors)
  - Snapshot test for JSON format
  - Snapshot test for CSV format

- [ ] **All output formats tested**
  - Table format
  - JSON format
  - CSV format

### Documentation Requirements

- [ ] **CLI_USAGE_EXAMPLES.md updated**
  - Table output examples added
  - Output format switching examples
  - Column alignment rules documented

- [ ] **Inline code documentation**
  - Table formatter methods documented
  - Type handling documented
  - Edge cases documented

### CI Requirements

- [ ] `cargo fmt` passes
- [ ] `cargo clippy` passes (no warnings)
- [ ] `cargo test` passes (all tests green)
- [ ] `cargo build --release` succeeds

---

## Testing Strategy

### Manual Testing

```bash
# Test 1: Basic table output
echo "SELECT * FROM simple_table LIMIT 3;" | ./target/debug/cqlite --config test.toml repl

# Test 2: JSON output
echo "SELECT * FROM simple_table LIMIT 3;" | ./target/debug/cqlite --config test.toml --out json repl

# Test 3: CSV output
echo "SELECT * FROM simple_table LIMIT 3;" | ./target/debug/cqlite --config test.toml --out csv repl

# Test 4: Collection types
echo "SELECT * FROM test_collections.collection_table LIMIT 5;" | \
  ./target/debug/cqlite \
  --schema test-data/schemas/collections.cql \
  --data-dir test-data/datasets/sstables \
  --out table repl
```

### Automated Testing

Create integration test in `cqlite-cli/tests/table_output_tests.rs`:

```rust
#[test]
fn test_table_output_simple() {
    let output = run_query_with_config(
        "SELECT * FROM simple_table LIMIT 3",
        "test.toml",
        OutputFormat::Table,
    );

    // Verify table structure
    assert!(output.contains("id"));
    assert!(output.contains("name"));
    assert!(output.contains("value"));
    assert!(output.contains("---")); // Border
    assert!(output.contains("|"));   // Separator
    assert!(output.contains("(3 rows)"));
}

#[test]
fn test_uuid_display() {
    let output = run_query_with_config(
        "SELECT id FROM simple_table LIMIT 1",
        "test.toml",
        OutputFormat::Table,
    );

    // Verify UUID format (hyphenated)
    assert!(output.contains("00000000-0000-0000-0000-000000000001"));

    // Verify no warnings
    assert!(!output.contains("Unsupported partition key type"));
}
```

### Golden Snapshot Testing

Use `insta` crate for snapshot testing:

```rust
#[test]
fn test_table_output_snapshot() {
    let output = run_query_with_config(
        "SELECT * FROM simple_table LIMIT 3",
        "test.toml",
        OutputFormat::Table,
    );

    insta::assert_snapshot!(output);
}
```

---

## Common Pitfalls to Avoid

### 1. **Character Encoding Issues**
- Use UTF-8 for all output
- Handle emoji and special characters correctly
- Test with international characters

### 2. **Column Width Calculation**
- Account for multi-byte characters (UTF-8)
- Handle very long text values (truncate gracefully)
- Test with empty strings and NULL values

### 3. **Type Conversion Errors**
- Don't unwrap/expect in formatter code (return Result)
- Handle unexpected type variants gracefully
- Log warnings for unsupported types, don't panic

### 4. **Color Code Injection**
- Only add color codes when colors enabled
- Strip color codes when measuring string width
- Test with and without `--no-color` flag

### 5. **Performance with Large Results**
- Don't load all rows into memory for formatting
- Stream output for large result sets
- Respect `page_size` setting for pagination

---

## Definition of Done

**This issue can be closed when:**

1. ✅ REPL query results display as formatted tables (no placeholder text)
2. ✅ UUID types display correctly without warnings
3. ✅ All three output formats work: `--out table`, `--out json`, `--out csv`
4. ✅ Integration tests pass for table output
5. ✅ Golden snapshots created for all output formats
6. ✅ Documentation updated in `CLI_USAGE_EXAMPLES.md`
7. ✅ All CI checks pass: format, clippy, tests
8. ✅ Manual acceptance test passes (see Quick Start section)

---

## Resources

### Key Documents
- **M2_CLI_SPEC.md** (lines 122-128): Table formatting specification
- **CLAUDE.md**: Development guidelines and testing commands
- **PRD.md**: M2 milestone requirements

### Test Data
- **Schemas:** `test-data/schemas/basic-types.cql`
- **SSTables:** `test-data/datasets/sstables/test_basic/simple_table/`
- **Config:** `test.toml` (see Quick Start section)
- **Environment:** `CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets`

### Related Issues
- **#117:** Parent epic (M2–CLI)
- **#119:** This issue (table formatting)
- **#122:** One-shot execution (merged, reference implementation)
- **#129:** Deterministic JSON/CSV (merged, reference implementation)
- **#189:** REPL :health diagnostics (merged, shows working REPL integration)

### Code References
- **Query Execution:** `cqlite-core/src/query/executor.rs`
- **Query Results:** `cqlite-core/src/query/result.rs`
- **JSON Writer:** `cqlite-cli/src/output/json.rs` (reference)
- **CSV Writer:** `cqlite-cli/src/output/csv.rs` (reference)
- **REPL Engine:** `cqlite-cli/src/repl/engine.rs` (integration point)

---

## Questions?

For technical questions or clarifications:
1. Review the detailed GitHub issue comments: [#119](https://github.com/pmcfadin/cqlite/issues/119)
2. Check the M2 epic for context: [#117](https://github.com/pmcfadin/cqlite/issues/117)
3. Review existing output writer implementations (JSON/CSV) as reference

**Good luck! The groundwork is solid - this is primarily a wiring task.** 🚀
