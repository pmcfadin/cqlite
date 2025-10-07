# M2 CLI Architecture Review – Action Items

**Review Date**: October 7, 2025  
**Status**: ✅ Approved with Recommendations  
**Tracking**: Use this document to track completion of review recommendations

---

## Critical Actions (Block Phase 1 Kickoff)

**Due Date**: Friday, October 11, 2025 (EOW)  
**Blocker**: Phase 1 implementation cannot begin until these are complete

### Action 1: Document Schema JSON Format
 - **Priority**: 🔴 CRITICAL
 - **Owner**: Lead/Architect
 - **Status**: ✅ DONE (Oct 7)
 - **Due**: Oct 11 (Fri)
 - **Deliverable**: `docs/development/SCHEMA_JSON_FORMAT.md`

**Requirements**:
- Document canonical JSON schema format
- Specify both minimal format (simple tables) and full format (with options, indices, UDTs)
- Show examples for common scenarios (basic table, table with UDTs, table with collections)
- Define validation rules and error messages
- Align with `cqlite-core::schema::json_exporter` structures
- Link from `ARCH_PLAN_M2_CLI.md` section 1.3

**Example Structure**:
```markdown
# Schema JSON Format Specification

## Minimal Format
{
  "keyspace": "ks",
  "table": "users",
  "columns": [...],
  "partition_keys": [...],
  "clustering_keys": [...]
}

## Full Format (with UDTs, indices, options)
{...}

## Validation Rules
- Required fields: keyspace, table, columns, partition_keys
- ...

## Error Messages
- Missing required field → exit 3 with hint
- ...
```

---

### Action 2: Finalize Version Detection Precedence
 - **Priority**: 🔴 CRITICAL
 - **Owner**: Lead/Architect + Core Engineer
 - **Status**: ✅ DONE (Oct 7)
 - **Due**: Oct 11 (Fri)
 - **Deliverable**: Updated `ARCH_PLAN_M2_CLI.md` §2.3

**Requirements**:
1. Investigate if `cqlite-core` SSTable reader exposes format version metadata
2. Define precedence chain (recommended):
   - **Primary**: User flag (`--cassandra-version`) if explicitly set
   - **Secondary**: SSTable metadata if available via core API
   - **Tertiary**: `metadata.yml` if present at `<data-dir>/../metadata.yml`
   - **Fallback**: Display "unknown" (don't guess)
3. Document in architecture plan
4. Add version mismatch warning specification

**Code Investigation**:
```bash
# Check if SSTable reader exposes version
grep -r "version\|format_version" cqlite-core/src/storage/sstable/
```

**Expected Update** (in `ARCH_PLAN_M2_CLI.md`):
```markdown
#### 2.3 Version hints/detection

**Precedence Chain**:
1. User flag: `--cassandra-version=<VER>` (explicit override)
2. SSTable metadata: Read from SSTable file headers via core API [IF AVAILABLE]
3. Dataset metadata: Parse `<data-dir>/metadata.yml` or `<data-dir>/../metadata.yml`
4. Fallback: Display "unknown" (no guessing/heuristics)

**`:status` Display**:
- Show "detected: 5.0" when found via methods 2-3
- Show "configured: 5.0" when set via method 1
- Warn on mismatch: "⚠ Version mismatch: detected v5.0, configured v4.1"
```

---

### Action 3: Define QueryResult Interface Contract
 - **Priority**: 🔴 CRITICAL
 - **Owner**: Core Engineer
 - **Status**: ✅ DONE (Oct 7)
 - **Due**: Oct 11 (Fri)
 - **Deliverable**: `docs/development/QUERY_RESULT_CONTRACT.md` or docstring in code

**Requirements**:
- Document the shape of `cqlite_core::query::QueryResult`
- Specify column ordering guarantees in `QueryResult.metadata.columns`
- Define how `Value` types map to display representations
- Clarify null/empty value handling conventions
- Document any async iteration patterns for large result sets

**Expected Documentation**:
```markdown
# QueryResult Interface Contract

## Structure
```rust
pub struct QueryResult {
    pub metadata: QueryMetadata,
    pub rows: Vec<QueryRow>,
    pub performance: PerformanceMetrics,
}

pub struct QueryMetadata {
    pub columns: Vec<ColumnInfo>,  // ORDERING GUARANTEE: Stable, matches SELECT clause
    pub table: String,
    pub keyspace: Option<String>,
}
```

## Guarantees for CLI Writers
1. **Column Order**: `metadata.columns` order matches SELECT clause or schema definition order
2. **Value Types**: `QueryRow` provides `Value` enum with all CQL types
3. **Null Handling**: Null values represented as `Value::Null`
4. **Empty Collections**: Empty list/set/map as `Value::List([])`, etc.

## Value → String Mapping (for CLI)
- UUID/TimeUUID: `to_string()` → lowercase hyphenated
- Timestamp: Custom formatter needed (see VALUE_FORMATTING_SPEC.md)
- Collections: JSON-like representation
- Blob: Hex encoding (`0x` prefix)
```

---

## High Priority Actions (Implement in M2 Sprint)

**Due Date**: During M2 implementation (Oct 15 - Nov 5)  
**Impact**: Improves robustness and usability

### Action 4: Add `:schema validate` Command
- **Priority**: 🟡 HIGH
- **Owner**: CLI Engineer
- **Status**: ⬜ TODO
- **Phase**: Phase 2 (REPL core)

**Requirements**:
- Add `:schema validate [PATH]` command
- Validate schema files without applying to session
- Report parse errors, UDT dependency issues, circular references
- Output: "✅ Valid" or list of errors with line numbers
- Use case: Pre-flight checks before `:schema load`

**Acceptance**:
```bash
cqlite> :schema validate schemas/users.cql
✅ Valid: 1 table, 2 UDTs, no errors

cqlite> :schema validate schemas/broken.cql
❌ Invalid:
  - Line 10: Unresolved type reference 'address_type'
  - Line 15: Duplicate column name 'id'
```

---

### Action 5: Implement Atomic Schema Loading
- **Priority**: 🟡 HIGH
- **Owner**: CLI Engineer
- **Status**: ⬜ TODO
- **Phase**: Phase 2 (REPL core)

**Requirements**:
- Multi-file schema load is atomic: all files succeed or none applied
- On error, rollback to previous schema state
- Error message shows which file failed and why
- Add `--rollback` flag for manual undo

**Implementation Notes**:
```rust
// Pseudo-code
fn load_schemas(paths: &[Path]) -> Result<()> {
    let checkpoint = self.schema_manager.snapshot();
    
    for path in paths {
        match parse_and_register(path) {
            Ok(_) => continue,
            Err(e) => {
                self.schema_manager.restore(checkpoint);
                return Err(format!("Failed at {}: {}", path, e));
            }
        }
    }
    
    Ok(())
}
```

---

### Action 6: Add Unsupported Query Error Tests
- **Priority**: 🟡 HIGH
- **Owner**: SDET
- **Status**: ⬜ TODO
- **Phase**: Phase 1 (Test infrastructure)

**Requirements**:
- Test each unsupported SELECT form
- Validate exit code 5
- Validate error message contains:
  - What was unsupported
  - What is supported
  - Example correction
- Test cases:
  1. `SELECT * FROM ks.tbl WHERE non_key_col = 'value'` (non-key filter)
  2. `SELECT * FROM ks.tbl WHERE key = 1 ORDER BY col` (ORDER BY)
  3. `SELECT * FROM ks.tbl WHERE key = 1 ALLOW FILTERING` (ALLOW FILTERING)
  4. `SELECT COUNT(*) FROM ks.tbl` (aggregates)
  5. `SELECT * FROM t1 JOIN t2` (joins)

**Test Structure**:
```rust
#[test]
fn test_unsupported_non_key_filter() {
    let result = cli()
        .arg("-e")
        .arg("SELECT * FROM ks.users WHERE status = 'active'")
        .assert()
        .exit_code(5)
        .stderr_contains("Unsupported query form")
        .stderr_contains("primary/partition key equality")
        .stderr_contains("WHERE id = <uuid>");
}
```

---

### Action 7: Document Timestamp Formatting Rules
- **Priority**: 🟡 HIGH
- **Owner**: Lead/Architect
- **Status**: ⬜ TODO
- **Due**: Oct 14 (Mon)
- **Deliverable**: `docs/development/VALUE_FORMATTING_SPEC.md`

**Requirements**:
1. Research exact cqlsh timestamp formatting (check Python `cassandra-driver` source)
2. Document format specification
3. Choose timezone default for M2 (recommend UTC)
4. Specify precision (milliseconds vs microseconds)
5. Plan for future `--timezone` flag (M3+)

**Research Steps**:
```bash
# Install cqlsh
pip install cassandra-driver

# Test timestamp rendering
cqlsh> CREATE TABLE test.timestamps (id uuid PRIMARY KEY, ts timestamp);
cqlsh> INSERT INTO test.timestamps (id, ts) VALUES (..., '2025-10-07 12:34:56.789Z');
cqlsh> SELECT * FROM test.timestamps;

# Observe output format
```

**Expected Spec**:
```markdown
# Value Formatting Specification

## Timestamps
- **Format**: `YYYY-MM-DD HH:MM:SS[.fff][+0000]`
- **Timezone**: UTC (default for M2)
- **Precision**: Milliseconds (3 decimal places)
- **Example**: `2025-10-07 12:34:56.789+0000`
- **Null**: Empty string or literal `null` (TBD based on cqlsh)

## UUIDs
- **Format**: Lowercase hyphenated
- **Example**: `8b6c8a96-5f5a-4f7e-a6a8-2b5a3a3f1c01`

## Blobs
- **Format**: `0x`-prefixed lowercase hex
- **Example**: `0xdeadbeef`

## Collections
- **List**: `[a, b, c]`
- **Set**: `{a, b, c}`  
- **Map**: `{k1: v1, k2: v2}`
```

---

### Action 8: Add `:status --json` Flag
- **Priority**: 🟡 HIGH
- **Owner**: CLI Engineer
- **Status**: ⬜ TODO
- **Phase**: Phase 2 (REPL core)

**Requirements**:
- `:status --json` outputs machine-readable JSON
- Include all `:status` information in structured format
- Enable scripting and CI parsing

**Output Schema**:
```json
{
  "data_directory": "/path/to/data",
  "discovery_timestamp": "2025-10-07T12:34:56Z",
  "keyspaces": ["ks1", "ks2"],
  "tables": {
    "ks1": ["users", "orders"],
    "ks2": ["events"]
  },
  "coverage": {
    "tables_with_schema": 6,
    "tables_missing_schema": ["ks1.audit_logs"],
    "schemas_without_data": [],
    "coverage_ratio": 0.86,
    "badge": "green"
  },
  "version": {
    "detected": "5.0",
    "configured": "5.0",
    "mismatch": false
  }
}
```

---

## Medium Priority Actions (Consider for M2 or M3)

**Timing**: Evaluate during sprint planning

### Action 9: Add `--strict-coverage` Flag
- **Priority**: 🟢 MEDIUM
- **Owner**: CLI Engineer
- **Deliverable**: Flag implementation in one-shot and REPL

**Use Case**: CI validation requiring 100% schema coverage
```bash
cqlite --schema schemas/ --data-dir data/ --strict-coverage -e "SELECT ..."
# Fails with exit 4 if any discovered table lacks schema
```

---

### Action 10: Implement `Handler` Trait for REPL
- **Priority**: 🟢 MEDIUM
- **Owner**: CLI Engineer
- **Deliverable**: Trait definition and handler refactoring

**Design**:
```rust
pub trait ReplHandler {
    async fn execute(&self, context: &mut ReplContext) -> ReplResult<()>;
    fn command_name(&self) -> &str;
    fn help_text(&self) -> &str;
    fn category(&self) -> CommandCategory;
}

// Implement for ConfigHandler, SchemaHandler, DiscoveryHandler, etc.
```

---

### Action 11: Add `:config save --minimal`
- **Priority**: 🟢 MEDIUM
- **Owner**: CLI Engineer
- **Use Case**: Cleaner config files (only non-default values)

---

### Action 12: Add `:session save/load`
- **Priority**: 🟢 MEDIUM
- **Owner**: CLI Engineer
- **Use Case**: Reproducible REPL sessions

---

### Action 13: Add `:health --verbose`
- **Priority**: 🟢 MEDIUM
- **Owner**: CLI Engineer
- **Use Case**: Detailed diagnostics (codec versions, memory, platform info)

---

### Action 14: Add Discovery Caching
- **Priority**: 🟢 MEDIUM
- **Owner**: CLI Engineer
- **Use Case**: Avoid repeated filesystem scans

**Design**: Cache discovery results with 5-minute TTL; `:discover --refresh` invalidates cache

---

### Action 15: Document Circular UDT Reference Handling
- **Priority**: 🟢 MEDIUM
- **Owner**: Lead/Architect
- **Deliverable**: Add to `ARCH_PLAN_M2_CLI.md` §1.3

**Decision**: Reject circular UDT references in M2 with clear error message

---

## Low Priority Actions (Nice to Have)

### Action 16: Add `just check-m2-spec` Recipe
- **Priority**: ⚪ LOW
- **Owner**: SDET

---

### Action 17: Add `--discover-timeout` Flag
- **Priority**: ⚪ LOW
- **Owner**: CLI Engineer

---

### Action 18: Add `:schema rollback`
- **Priority**: ⚪ LOW
- **Owner**: CLI Engineer

---

### Action 19: Document Exit Codes in `--help`
- **Priority**: ⚪ LOW
- **Owner**: CLI Engineer

---

## Progress Tracking

**Last Updated**: October 7, 2025

| Priority | Total | TODO | In Progress | Done |
|----------|-------|------|-------------|------|
| Critical | 3 | 0 | 0 | 3 |
| High | 5 | 5 | 0 | 0 |
| Medium | 7 | 7 | 0 | 0 |
| Low | 4 | 4 | 0 | 0 |
| **TOTAL** | **19** | **19** | **0** | **0** |

---

## Completion Checklist

### Week 1 (Oct 7-11): Critical Actions
- [ ] Action 1: Schema JSON format documented
- [ ] Action 2: Version detection precedence finalized
- [ ] Action 3: QueryResult contract defined
- [ ] Architecture plan updated with findings
- [ ] Review team signoff obtained

### Week 2 (Oct 14-18): Phase 1 + High Priority
- [ ] Phase 1 implementation begins
- [ ] Action 7: Timestamp formatting documented
- [ ] Action 6: Unsupported query tests added
- [ ] Phase 1 deliverables complete

### Week 3-4 (Oct 21-Nov 1): Phase 2-3 + High Priority
- [ ] Action 4: `:schema validate` implemented
- [ ] Action 5: Atomic schema loading implemented
- [ ] Action 8: `:status --json` implemented
- [ ] Phase 2-3 deliverables complete

### Week 5 (Nov 4-8): Polish + Medium Priority
- [ ] Evaluate medium priority actions
- [ ] Phase 4 implementation
- [ ] M2 acceptance criteria validated
- [ ] Documentation complete

---

## Notes

- Update this document as actions are completed
- Move completed actions to "Done" section with completion date
- Add new actions discovered during implementation
- Link to PRs/issues for each action

---

**Document Version**: 1.0  
**Maintained By**: Architecture Team + Product Manager
