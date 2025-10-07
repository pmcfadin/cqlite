# Architecture Review: CQLite M2–CLI

**Review Date**: October 7, 2025  
**Reviewer**: Architecture Team  
**Documents Reviewed**:
- `docs/development/ARCH_PLAN_M2_CLI.md` (Primary)
- `docs/development/M2_CLI_SPEC.md` (Spec)
- `docs/development/EPIC_M2_CLI.md` (Epic)
- `docs/development/PRD.md` (Context)
- `cqlite-cli/CLI_USAGE_EXAMPLES.md` (Examples)

**Status**: ✅ **APPROVED WITH RECOMMENDATIONS**

---

## Executive Summary

The M2 CLI architecture is **sound and ready for implementation** with minor recommendations. The plan demonstrates strong alignment with cqlsh parity goals, clear separation of concerns, and appropriate scope control. The existing codebase provides a solid foundation with `QueryEngine`, `SchemaManager`, and REPL infrastructure already in place.

**Key Strengths:**
- Clear integration surface between CLI and core
- Appropriate SELECT subset with explicit unsupported query handling
- Well-defined schema precedence and two-pass loading
- Practical discovery algorithm and coverage reporting
- Comprehensive REPL command routing architecture
- Strong testing strategy with golden snapshots

**Key Recommendations:**
1. Clarify QueryResult interface consistency
2. Add explicit schema JSON format specification
3. Document version detection precedence more precisely
4. Consider adding `:schema validate` command for pre-execution checks
5. Expand error recovery guidance for multi-file schema loading

---

## 1. Integration Surface Validation

### 1.1 CLI ↔ Core Integration

✅ **APPROVED**: The integration surface is well-defined and leverages existing core infrastructure appropriately.

**Findings:**
- `cqlite-core::query::QueryEngine` exists and provides `execute()` → `QueryResult` interface
- `cqlite-core::schema::SchemaManager` has schema loading and registry capabilities
- Both `SchemaRegistry` and `SchemaDiscoveryEngine` are available in core for CLI to leverage
- REPL command routing via `command_parser.rs` and `engine.rs` is already established

**Verification from Code:**
```rust
// cqlite-core/src/query/mod.rs
pub use result::{
    ColumnInfo, PerformanceMetrics, QueryMetadata, 
    QueryResult, QueryRow, RowMetadata,
};

// cqlite-core/src/schema/mod.rs  
pub use registry::{SchemaRegistry, SchemaSource, SchemaValidator};
pub use discovery::SchemaDiscoveryEngine;
```

**Recommendation**: Document the exact shape of `QueryResult` in the architecture plan to ensure output writers have a stable contract. Specifically:
- Confirm `QueryResult.metadata.columns` ordering guarantees
- Specify how `Value` types map to cqlsh representations
- Define null/empty value handling conventions

### 1.2 Data Flow Clarity

✅ **APPROVED**: Five-step flow is logical and matches existing patterns.

The documented flow (config → schema → discovery → query → output) aligns well with existing CLI patterns in `cqlite-cli/src/repl/engine.rs` and `cqlite-cli/src/interactive.rs`.

---

## 2. SELECT Strategy Review

### 2.1 Query Subset Definition

✅ **APPROVED**: The M2 SELECT subset is appropriately scoped and defensible.

**Supported Subset:**
```sql
SELECT [columns|*] FROM <[keyspace.]table> 
WHERE <partition-key = value> 
  [AND <clustering-key = value>]* 
[LIMIT N]
```

**Analysis:**
- Partition key equality: ✅ Essential for SSTable access
- Optional clustering prefix: ✅ Enables efficient range scans within partition
- `LIMIT`: ✅ Critical for user experience and memory management
- No `ORDER BY`: ✅ Acceptable (clustering order is implicit)
- No `ALLOW FILTERING`: ✅ Correct (would require full scan)

**Code Evidence:**
The existing `QueryEngine` in `cqlite-core/src/query/engine.rs` shows routing logic for SELECT queries:
```rust
if trimmed_sql.starts_with("SELECT") {
    return self.execute_select_query(sql, start_time).await;
}
```

The `state_machine` feature gate is properly used to defer advanced SELECT optimization.

### 2.2 Deferring `state_machine` Feature

✅ **APPROVED**: Keeping `state_machine` OFF for M2 is the right risk mitigation.

**Question 1 Response**: *"Any concerns with deferring the `state_machine` path to post‑M2?"*

**Answer**: No concerns. This is a prudent approach because:
1. The base `QueryEngine` path is mature and tested
2. `state_machine` components (`select_optimizer`, `select_executor`) exist but are experimental
3. M2 acceptance criteria can be fully met without advanced optimization
4. Exit code 5 with clear messaging provides good user feedback path

**Recommendation**: Add a test fixture that explicitly validates the error message for unsupported queries (e.g., `WHERE non_key_column = value`) to ensure messaging is actionable.

### 2.3 Fallback Error Semantics

✅ **APPROVED**: Exit code 5 with hint is appropriate.

Suggested error message template (from spec):
```
Unsupported query form in M2. Supported: SELECT with primary/partition 
key equality and optional LIMIT. Try narrowing WHERE clause.
```

**Recommendation**: Consider adding example correction to error message:
```
Unsupported: SELECT * FROM ks.tbl WHERE status = 'active'
Supported:   SELECT * FROM ks.tbl WHERE id = <uuid> LIMIT 100
```

---

## 3. Schema Ingestion Strategy

### 3.1 Source Handling and Precedence

✅ **APPROVED**: Multi-source schema loading with last-wins precedence is sound.

**Question 2 Response**: *"Are the schema precedence and two‑pass load rules sufficient?"*

**Answer**: Yes, the rules are sufficient and well-designed:

**Precedence Chain:**
1. Multiple `--schema` paths processed in order
2. Within directories: lexical order (deterministic)
3. Last writer wins per fully-qualified name (`keyspace.table`, `keyspace.type`)
4. Two-pass loading: types first, then tables

**Code Evidence:**
`SchemaManager` in `cqlite-core/src/schema/mod.rs` provides:
```rust
pub async fn parse_and_register_cql_schema(&self, cql: &str) -> Result<TableSchema>
pub async fn register_schema(&self, schema: TableSchema, source: SchemaSource) -> Result<()>
```

The `UdtRegistry` supports the two-pass model.

### 3.2 Two-Pass Loading (Types → Tables)

✅ **APPROVED**: Two-pass loading handles UDT dependencies correctly.

**UDT Dependency Resolution:**
- Pass 1: Parse and register all `CREATE TYPE` statements
- Pass 2: Parse `CREATE TABLE` statements (which may reference UDTs)
- Unresolved references → Schema error (exit 3) with hints

**Recommendation**: Clarify behavior for circular UDT references (rare but possible in Cassandra):
- Should the loader detect and reject circular references?
- Or rely on Cassandra's own validation?
- Suggest: Document as "circular UDT references are unsupported in M2" and return schema error

### 3.3 Schema JSON Format

⚠️ **RECOMMENDATION**: Finalize and document the accepted JSON schema format.

**Open Question from Plan**:
> "Schema JSON format: finalize accepted shape(s) and alignment with core `SchemaManager` import."

**Current State:**
The existing `CLI_USAGE_EXAMPLES.md` shows a JSON format:
```json
{
  "table_name": "users",
  "columns": [{"name": "id", "data_type": "Uuid"}, ...],
  "primary_key": ["id"]
}
```

But `cqlite-core/src/schema/json_exporter.rs` defines more comprehensive structures (`JsonSchema`, `JsonTable`, `JsonUDT`).

**Recommendation**: 
1. Document the canonical JSON schema format in `docs/development/SCHEMA_JSON_FORMAT.md`
2. Specify both minimal format (for simple use cases) and full format (with options, indices, UDTs)
3. Add JSON schema validation in `SchemaLoader` with clear error messages
4. Link this spec from `ARCH_PLAN_M2_CLI.md` section 1.3

### 3.4 Error Reporting

✅ **APPROVED**: Exit code 3 for schema errors with actionable hints is appropriate.

Suggested error message structure:
```
Schema Error: Unresolved type reference 'address_type' in table 'ks.users' column 'addr'
Hint: Ensure CREATE TYPE address_type appears before CREATE TABLE users
File: /path/to/schema.cql:42
```

---

## 4. Discovery and Coverage Reporting

### 4.1 Discovery Algorithm

✅ **APPROVED**: The directory scanning algorithm is practical and well-scoped.

**Question 3 Response**: *"Is the discovery algorithm and coverage badge policy acceptable?"*

**Answer**: Yes, both are appropriate for M2.

**Discovery Logic (from plan §2.1):**
```
1. Scan --data-dir for keyspace directories
2. Skip hidden and 'system' keyspaces  
3. Match table directories: tablename-<uuid>
4. Validate SSTable presence via core I/O layer
5. Output: DiscoverySummary with keyspaces, tables, counts, timestamp
```

**Code Evidence:**
`cqlite-core/src/storage/sstable_data_manager.rs` lines 289-400 implement this pattern:
```rust
async fn perform_discovery(&self, data_dir: &Path) -> Result<TableDiscovery> {
    // Scans keyspace directories, skips system, validates SSTables
}
```

**Recommendation**: Consider exposing discovery as a reusable service in `cqlite-cli/src/services/discovery.rs` (as planned) rather than duplicating logic from core. The CLI service can wrap core's `TableDiscovery` and add CLI-specific presentation logic.

### 4.2 Coverage Badge Thresholds

✅ **APPROVED**: Green/Yellow/Red thresholds are reasonable.

**Coverage Formula:**
```
Coverage = |Discovered ∩ HasSchema| / |Discovered|

Green:  ≥ 95%
Yellow: 50% - 95%
Red:    < 50% or critical errors
```

**Rationale:**
- 95% threshold allows for a few system tables without schema (acceptable)
- 50% boundary distinguishes "mostly covered" from "mostly missing"
- Critical errors (e.g., parse failures, directory unreadable) → Red regardless of percentage

**Recommendation**: Add a `--strict-coverage` flag that requires 100% for scripting/CI use cases where any missing schema should fail the run.

### 4.3 `:status` Output Design

✅ **APPROVED**: The mock output in `M2_CLI_SPEC.md` lines 190-199 is clear and actionable.

Example output (from spec):
```
Data Directory: /var/lib/cassandra/data
Discovery: 2 keyspaces, 7 tables
Schema Coverage:
  - tables with schema: 6
  - tables missing schema: 1  (e.g., ks.audit_logs)
  - schemas without data: 0
Cassandra Version: detected 5.0 (configured: 5.0)
Status: Green (86%+ coverage; no critical errors)
```

**Recommendation**: Add a `:status --json` flag for programmatic parsing in scripts/CI.

---

## 5. Version Hints and Detection

### 5.1 Version Detection Strategy

⚠️ **NEEDS CLARIFICATION**: Finalize version detection precedence.

**Question 4 Response**: *"Preferred source of version hints for `:status`?"*

**Current Plan (§2.3):**
> Preferred: read version from SSTable metadata via core (if exposed) or from repository `metadata.yml` at the dataset root when present.
> Fallback: `--cassandra-version` flag or `auto-detect` heuristic.

**Analysis:**
- **SSTable metadata**: Most authoritative if available; check if `cqlite-core` exposes SSTable format version
- **`metadata.yml`**: Good for test datasets (present in `test-data/datasets/metadata.yml`)
- **`--cassandra-version` flag**: User override for edge cases
- **Auto-detect heuristic**: Fragile; discourage in M2

**Recommendation**:
1. **Primary**: Check if SSTable metadata exposes version via core APIs (investigate `cqlite-core` SSTable reader metadata)
2. **Secondary**: Parse `metadata.yml` if present at `<data-dir>/../metadata.yml` or `<data-dir>/metadata.yml`
3. **Tertiary**: Use `--cassandra-version` flag if explicitly set
4. **Fallback**: Display "unknown" rather than guessing
5. **Precedence**: User flag > SSTable metadata > metadata.yml > unknown

Document this precedence in `ARCH_PLAN_M2_CLI.md` §2.3.

### 5.2 Version Mismatch Warnings

✅ **APPROVED**: Warning on detected vs. configured mismatch is good UX.

Example warning:
```
⚠ Version mismatch: SSTables indicate v5.0, but --cassandra-version=4.1 specified
```

---

## 6. REPL Architecture

### 6.1 Command Routing

✅ **APPROVED**: The command routing model is well-designed and already implemented.

**Code Evidence:**
`cqlite-cli/src/repl/command_parser.rs` and `engine.rs` show mature routing:
```rust
pub enum CommandType {
    Config { operation: String },
    Tables,
    Describe { object_name: String },
    Use { keyspace: String },
    CqlQuery { query: String },
    // ... etc
}
```

**Planned Handler Structure (§3.1):**
```
cqlite-cli/src/repl/handlers/
  ├── config.rs       (:config show/set/save)
  ├── schema.rs       (:schema list/load/show/refresh/unload)
  ├── discovery.rs    (:discover, :status)
  ├── health.rs       (:health checks)
  └── introspection.rs (:keyspaces, :tables, DESCRIBE)
```

**Recommendation**: This is a clean separation. Consider adding a `Handler` trait to ensure consistent error handling and output formatting across handlers:

```rust
pub trait ReplHandler {
    async fn execute(&self, context: &mut ReplContext) -> ReplResult<()>;
    fn command_name(&self) -> &str;
    fn help_text(&self) -> &str;
}
```

### 6.2 Session Model

✅ **APPROVED**: Session state design is appropriate.

**Session State (§3.2):**
- `data_dir`: Path
- `default_keyspace`: Option<String>
- `page_size`: usize
- `timing`: bool
- `color`: bool
- `history_path`: PathBuf
- Schema catalog reference

**Code Evidence:**
`cqlite-cli/src/repl/session.rs` already implements session management.

**Recommendation**: Add session export/import for reproducibility:
```
:session save my_session.toml
:session load my_session.toml
```

### 6.3 Config Precedence and `:config save`

✅ **APPROVED**: Precedence chain is standard and correct.

**Question 7 Response**: *"Any changes requested to config precedence or `:config save` behavior?"*

**Answer**: No changes needed. The precedence is industry-standard:

```
flags > env vars > config file > defaults
```

**`:config save` Behavior:**
- Writes effective config (after precedence resolution)
- Default format: TOML
- Optional: `:config save [FILE]` specifies output path
- Include `repl.*` keys for session settings

**Recommendation**: Consider `:config save --minimal` flag that only writes non-default values (cleaner config files).

---

## 7. Output and Formatting

### 7.1 Output Format Mapping

✅ **APPROVED**: The `QueryResult` → writer mapping is well-designed.

**Question 5 Response**: *"Do the output formatting rules meet cqlsh parity expectations?"*

**Answer**: Yes, with minor clarifications needed.

**Format Specifications:**

**Table Output (§4.1):**
- Use `CqlshTableFormatter` (already exists in `cqlite-cli/src/formatter.rs`)
- Headers, separators, right-alignment per cqlsh rules
- Stable column order from `QueryResult.metadata.columns`

**Code Evidence:**
```rust
// cqlite-cli/src/formatter.rs
pub struct CqlshTableFormatter {
    pub column_headers: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub show_row_count: bool,
    pub color_support: bool,
}
```

**JSON Output:**
- Array of row objects
- Deterministic column order (from metadata, not map iteration)
- Example:
  ```json
  [
    {"id": "uuid-1", "name": "Alice", "email": "alice@example.com"},
    {"id": "uuid-2", "name": "Bob", "email": "bob@example.com"}
  ]
  ```

**CSV Output:**
- Header row from `metadata.columns`
- Stringified values per cqlsh conventions
- Example:
  ```csv
  id,name,email
  uuid-1,Alice,alice@example.com
  uuid-2,Bob,bob@example.com
  ```

### 7.2 Value Formatting Conventions

⚠️ **NEEDS CLARIFICATION**: Document exact formatting rules for all types.

**Specified Formats (§4.2):**
- UUID/TimeUUID: lowercase hyphenated (`8b6c8a96-5f5a-4f7e-a6a8-2b5a3a3f1c01`)
- Timestamps: `YYYY-MM-DD HH:MM:SS[.fff][+0000]` *(timezone handling needs clarification)*
- Collections: `[a, b]`, `{a, b}`, `{k: v}` (cqlsh-style)
- Blobs: `0x`-prefixed lowercase hex

**Open Question from Plan:**
> "Timestamp display: confirm cqlsh exact formatting and timezone default for M2."

**Recommendation**:
1. Research cqlsh timestamp rendering (check Python `cassandra-driver` source)
2. Default to UTC for M2 (simpler, deterministic)
3. Add optional `--timezone` flag for future milestones
4. Document in `docs/development/VALUE_FORMATTING_SPEC.md` with examples

### 7.3 Adapter Layer

✅ **APPROVED**: Thin adapters in CLI writer layer is the right approach.

From §4.2:
> Where core `Value` formatting differs from cqlsh, add thin adapters in the CLI writer layer without mutating core types.

This maintains separation of concerns: core provides raw values, CLI adds presentation logic.

---

## 8. Diagnostics and Exit Codes

### 8.1 `:health` Checklist

✅ **APPROVED**: The health check list is comprehensive and actionable.

**Checks (§5.1):**
1. Data-dir readability and layout
2. Schema parse success/failed file counts
3. Schema↔data sync summary (from `:status`)
4. Compression codec availability (LZ4/Snappy/Deflate)
5. Config coherence (page size, timing, color, effective config path)
6. Actionable next steps

**Example Output (from spec):**
```
Checks:
  - data-dir readable: OK
  - schema parse: OK (3 files)
  - schema/data sync: 6/7 tables covered
  - compression codecs: LZ4, Snappy available
  - config: page-size=50, timing=off
Tips:
  - Missing schema for: ks.audit_logs (use :schema load <file>)
```

**Recommendation**: Add a `:health --verbose` mode that shows:
- Detailed codec versions
- Platform capabilities (async I/O, mmap support)
- Memory usage stats

### 8.2 Exit Codes

✅ **APPROVED**: Exit code semantics are appropriate for scripting.

**Question 6 Response**: *"Are the error semantics/exit codes appropriate for scripting and CI?"*

**Answer**: Yes, the five-code scheme is well-designed.

**Exit Code Mapping (§5.2):**
```
0: Success
2: Invalid arguments (CLI or meta-commands)
3: Schema errors
4: Data-dir/discovery errors
5: Query execution errors
```

**Analysis:**
- Distinct codes for different failure categories enable smart retry logic
- Exit 2: User error, don't retry → fix command
- Exit 3: Schema error, fix schema → retry
- Exit 4: Env error, fix paths → retry
- Exit 5: Query error, fix query → retry

**Recommendation**: Add exit code documentation to `--help` output and `CLI_USAGE_EXAMPLES.md`.

**Code Evidence:**
`cqlite-cli/src/repl/mod.rs` shows exit code support:
```rust
pub enum ExecutionResult {
    Continue,
    ExitWithCode(i32),
}
```

---

## 9. Testing Strategy

### 9.1 Test Coverage Plan

✅ **APPROVED**: The testing strategy is comprehensive and appropriate for M2.

**Test Layers (§6):**
1. **Integration tests**: One-shot and REPL against `test-data/datasets`
2. **Golden snapshots**: Table formatting via `insta` crate
3. **Fixture coverage**: Narrow rows, wide rows, collections, timeseries
4. **Help text snapshots**: Guard CLI parity with spec
5. **Environment vars**: `CQLITE_DATA_DIR`, `CQLITE_SCHEMA` for test defaults

**Code Evidence:**
`cqlite-cli/tests/` directory shows existing test infrastructure with `cli_helpers.rs`, `assertions.rs`, and `integration.rs`.

**Recommendation**: Add these specific test scenarios:
1. **Multi-file schema loading**: 3+ files with overlapping definitions (test last-wins)
2. **UDT dependency resolution**: Type defined after table reference (should fail)
3. **Discovery edge cases**: Empty keyspace, no-schema tables, hidden directories
4. **Unsupported query errors**: Each unsupported SELECT form with error message validation
5. **Config precedence**: Flag overriding env overriding file
6. **`:status` coverage badges**: Synthetic scenarios for Green/Yellow/Red thresholds

### 9.2 Snapshot Policy

✅ **APPROVED**: Snapshot policy ensures formatting stability.

From §6:
> Snapshot policy: Any intentional changes to table formatting require updating spec examples and snapshots in the same PR.

This guards against accidental formatting regressions.

---

## 10. Risk Analysis

### 10.1 Identified Risks (§7)

✅ **ACKNOWLEDGED**: Risks are appropriately identified and mitigated.

| Risk | Mitigation |
|------|------------|
| SELECT coverage gaps | Explicit error messages, test unsupported forms |
| Discovery variance | Validate patterns, document assumptions, fallback logic |
| Formatting drift | Golden snapshots, centralized formatter |
| Scope creep | Defer Parquet, TUI, advanced `state_machine` to M3+ |

### 10.2 Additional Risks

⚠️ **ADDITIONAL CONSIDERATIONS**:

1. **Performance Risk**: Large dataset scanning for `:discover`
   - **Mitigation**: Add `--discover-timeout` flag and progress indicator
   - Cache discovery results with TTL in session

2. **Multi-file Schema Loading Risk**: Partial failures leave inconsistent state
   - **Mitigation**: Atomic schema loading—either all files succeed or none are applied
   - Consider `:schema load --dry-run` for validation without applying

3. **REPL Error Recovery**: How do errors in REPL affect session state?
   - **Mitigation**: Document error recovery model—errors should not corrupt session state
   - Add `:schema rollback` to revert last schema load

---

## 11. Work Breakdown Validation

### 11.1 Phase Structure

✅ **APPROVED**: Four-phase breakdown is logical and incremental.

**Phases (§9):**
1. **Phase 1**: One-shot plumbing (flags, schema loader, discovery, output writers)
2. **Phase 2**: REPL core (session, `:config`, `:discover`, `:status`, `:schema`)
3. **Phase 3**: Introspection (`:keyspaces`, `:tables`, `DESCRIBE`, formatting parity)
4. **Phase 4**: Health diagnostics and polish

Each phase delivers testable, demoable functionality.

### 11.2 Owner Alignment

✅ **APPROPRIATE**: Suggested owner roles make sense.

**Owners (§9):**
- Lead/Architect: Integration decisions, interfaces, SELECT subset ADR
- CLI Engineer: Flags wiring, REPL handlers, writers, status/health
- Core Engineer: Confirm `QueryResult` shape, schema/discovery hooks
- SDET: Integration/snapshot tests, fixtures

**Recommendation**: Assign explicit DRI (Directly Responsible Individual) for each phase to clarify accountability.

---

## 12. Open Questions Resolution

### From §10 of Architecture Plan

| Question | Status | Recommendation |
|----------|--------|----------------|
| Schema JSON format | ⚠️ Open | **Action**: Document canonical format, add to spec |
| Timestamp display | ⚠️ Open | **Action**: Research cqlsh, default UTC, document |
| Version detection | ⚠️ Open | **Action**: Finalize precedence, expose SSTable version API |

---

## 13. Validation Plan

### 13.1 Pre-Implementation Validation

✅ **APPROVED**: Validation plan is practical.

**Steps (§11):**
1. Run examples in `CLI_USAGE_EXAMPLES.md` against `test-data` on macOS/Linux
2. Execute `just test` across workspace
3. Confirm golden snapshot stability
4. Verify `--help` parity with `M2_CLI_SPEC.md`

**Recommendation**: Add a `just check-m2-spec` recipe that:
- Runs all M2 acceptance criteria from `M2_CLI_SPEC.md` §302
- Fails if any criterion is not met
- Generates a compliance report

### 13.2 Acceptance Criteria Mapping

✅ **CLEAR**: Acceptance criteria in `M2_CLI_SPEC.md` lines 282-303 are testable.

**Must-Have Criteria:**
- ✅ One-shot mode with `--schema`, `--data-dir`, `-e`/`-f`, `--out`
- ✅ READ-only SELECT subset (partition/primary key + LIMIT)
- ✅ Output formats: table, JSON, CSV
- ✅ REPL with `:config`, `:schema`, `:status`, `:health`, `:use`, `:keyspaces`, `:tables`, `DESCRIBE`, `SELECT`
- ✅ Session defaults, history, `:source`
- ✅ cqlsh-compatible table formatting
- ✅ Discovery, coverage reporting, version hints, badge system
- ✅ Docs and integration tests

**Should-Have Criteria:**
- ✅ `:config save [FILE]`
- ✅ `:schema unload <NAME>|all`

All criteria are implementable within the architecture.

---

## 14. Recommendations Summary

### 14.1 Critical (Must Address Before Implementation)

1. **[CRITICAL]** Document canonical schema JSON format in `docs/development/SCHEMA_JSON_FORMAT.md`
2. **[CRITICAL]** Finalize version detection precedence and document in arch plan
3. **[CRITICAL]** Define and document `QueryResult` interface contract for output writers

### 14.2 High Priority (Should Address in M2)

4. **[HIGH]** Add `:schema validate` command for pre-execution schema checks
5. **[HIGH]** Implement atomic schema loading (all-or-nothing for multi-file loads)
6. **[HIGH]** Add test fixtures for unsupported query error messages
7. **[HIGH]** Document timestamp formatting rules (research cqlsh, default UTC)
8. **[HIGH]** Add `:status --json` for programmatic parsing

### 14.3 Medium Priority (Consider for M2 or M3)

9. **[MEDIUM]** Add `--strict-coverage` flag for CI use cases
10. **[MEDIUM]** Implement `Handler` trait for consistent REPL handler behavior
11. **[MEDIUM]** Add `:config save --minimal` flag (non-defaults only)
12. **[MEDIUM]** Add `:session save/load` for reproducibility
13. **[MEDIUM]** Add `:health --verbose` for detailed diagnostics
14. **[MEDIUM]** Add discovery caching with TTL to avoid repeated scans
15. **[MEDIUM]** Document circular UDT reference handling (reject in M2)

### 14.4 Low Priority (Nice to Have)

16. **[LOW]** Add `just check-m2-spec` recipe for compliance reporting
17. **[LOW]** Add `--discover-timeout` flag for large dataset protection
18. **[LOW]** Add `:schema rollback` for error recovery
19. **[LOW]** Add exit code documentation to `--help` output

---

## 15. Approval Conditions

### This architecture is **APPROVED** contingent on:

1. ✅ **Addressing Critical Recommendations** (items 1-3) within 1 week
2. ✅ **Implementing High Priority Recommendations** (items 4-8) during M2
3. ✅ **Documenting Open Questions** (schema JSON, timestamps, version detection) before Phase 1 begins

### Timeline:

- **Recommendations 1-3**: Due by **Friday, October 11, 2025** (EOW as requested)
- **Architecture plan update**: Due by **Monday, October 14, 2025**
- **Phase 1 kickoff**: Target **Tuesday, October 15, 2025** (contingent on above)

---

## 16. Appendix: Code Quality Observations

### 16.1 Existing Code Strengths

From codebase examination, the following existing components are **production-ready**:

✅ **Strong Foundations:**
- `cqlite-core::query::QueryEngine`: Mature query execution with caching
- `cqlite-core::schema::SchemaManager`: Robust schema management with UDT support
- `cqlite-core::schema::SchemaRegistry`: Comprehensive registry with validation
- `cqlite-cli::formatter::CqlshTableFormatter`: Well-designed table formatting
- `cqlite-cli::repl::CommandParser`: Clean command parsing with metadata

✅ **Test Infrastructure:**
- `cqlite-cli/tests/` has solid test helpers (`cli_helpers.rs`, `assertions.rs`)
- Integration test framework in place
- Golden snapshot support via `insta` (implied by repo conventions)

### 16.2 Areas for Enhancement

⚠️ **Fragmentation Noted:**
- Discovery logic appears in multiple places:
  - `cqlite-core/src/storage/sstable_data_manager.rs`
  - `cqlite-cli/src/repl/session.rs`
  - `cqlite-cli/src/interactive.rs`
  
  **Recommendation**: Consolidate into `cqlite-cli/src/services/discovery.rs` as planned in architecture.

- Multiple schema loading approaches:
  - `SchemaManager::load_schema()`
  - `SchemaRegistry::discover_schema()`
  - `SchemaManager::parse_and_register_cql_schema()`
  
  **Recommendation**: Add façade in `cqlite-cli/src/services/schema_loader.rs` that provides unified interface for CLI use.

---

## 17. Conclusion

The M2 CLI architecture is **well-designed, implementable, and ready for execution** with the minor clarifications outlined above. The plan demonstrates:

- ✅ Clear understanding of cqlsh parity requirements
- ✅ Appropriate scope control (deferring advanced features to M3+)
- ✅ Strong separation of concerns (CLI vs. core)
- ✅ Practical risk mitigation strategies
- ✅ Comprehensive testing approach
- ✅ Realistic work breakdown

**The architecture team recommends proceeding with implementation** after addressing the three critical recommendations.

---

## Sign-Off

**Architecture Review Team**  
October 7, 2025

**Next Steps:**
1. Product team addresses critical recommendations (1-3) by EOW
2. Updated architecture plan circulated for final sign-off
3. Engineering team begins Phase 1 implementation
4. Weekly sync on progress against phase milestones

**Contact for Questions:**
- Architecture questions → Lead/Architect
- Implementation questions → CLI Engineer + Core Engineer  
- Test strategy questions → SDET

---

**Document Version**: 1.0  
**Status**: Final Review  
**Distribution**: Product, Engineering, QA
