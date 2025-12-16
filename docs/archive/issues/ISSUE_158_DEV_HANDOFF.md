# Issue #158: Cassandra 5.0 Parsing Fix - Dev Team Handoff

**GitHub Issue**: https://github.com/pmcfadin/cqlite/issues/158  
**Priority**: P0 - Critical  
**Blocks**: M2 CLI Milestone  
**Status**: Partial fix implemented (format detection), schema wiring needed

---

## Executive Summary for Dev Team

### What We Discovered

**Two-part problem** affecting all M2 CLI queries:

1. ✅ **Format Mismatch (FIXED)**: 
   - V5_0DataFormat was routed to state machine expecting VInt encoding
   - Actually uses compressed blocks with legacy serialization
   - **Fixed**: Added format detection, queries no longer crash

2. ❌ **Schema Not Used (CRITICAL)**: 
   - Schema IS passed from CLI and propagates to SSTableReader
   - But NOT wired into RowCellStateMachine
   - Result: Everything returns `Value::Blob` instead of proper types
   - **Needs Fix**: Wire schema into state machine + fix value extraction

### Current Behavior (After Partial Fix)

```bash
$ CQLITE_SCHEMA=test-data/schemas/basic-types.cql \
  cqlite -e "SELECT id, name, age FROM test_basic.simple_table LIMIT 1" --out json

✅ Exit code: 0  # Doesn't crash (routing fix works)
❌ Output: {"id": [blob], "name": [blob], "age": [blob]}  # Wrong types!
```

### Expected Behavior (After Full Fix)

```bash
✅ Exit code: 0
✅ Output: {"id": "4d432...", "name": "Alice", "age": 25}  # Proper types!
```

---

## Technical Root Cause

### Issue 1: State Machine Created Without Schema ✗

**File**: `cqlite-core/src/storage/sstable/reader/parsing/mod.rs` (line 264)

**Current** (broken):
```rust
pub fn parse_partition_data(&self, data: &[u8]) -> Result<...> {
    let mut state_machine = RowCellStateMachine::new();  // ← No schema!
    // State machine can't emit typed values without schema
}
```

**Needed**:
```rust
pub fn parse_partition_data(&self, data: &[u8], schema: Option<&TableSchema>) -> Result<...> {
    let mut state_machine = if let Some(schema) = schema {
        RowCellStateMachine::with_schema_and_version(
            schema.clone(),
            schema.get_partition_key_comparators()?[0].clone(),
            self.header.cassandra_version
        )
    } else {
        RowCellStateMachine::new()
    };
}
```

### Issue 2: Value Extraction Returns First Cell Only ✗

**File**: `cqlite-core/src/storage/sstable/reader/parsing/mod.rs` (line 320)

**Current** (broken):
```rust
fn extract_value_from_parsed_row(&self, parsed_row: &ParsedRow) -> Result<Value> {
    // Returns first non-null cell as blob
    for cell in &parsed_row.cells {
        if !cell.value_bytes.is_empty() {
            return Ok(Value::Blob(cell.value_bytes.clone()));  // ← Wrong!
        }
    }
    Ok(Value::Text(format!("row_with_{}_cells", ...)))  // ← Fallback
}
```

**Needed**:
```rust
fn extract_value_from_parsed_row_with_schema(
    &self, 
    parsed_row: &ParsedRow, 
    schema: &TableSchema
) -> Result<Value> {
    let mut columns = HashMap::new();
    
    // Process ALL cells, not just first one
    for cell in &parsed_row.cells {
        if let Some(column_def) = schema.columns.iter().find(|c| c.name == cell.name) {
            // Parse with schema type
            let typed_value = self.parse_value_with_schema_type(
                &cell.value_bytes,
                &column_def.data_type
            )?;
            columns.insert(cell.name.clone(), typed_value);
        }
    }
    
    Ok(Value::Row(columns))  // ← Proper multi-column row!
}
```

### Issue 3: Schema Not Threaded Through Call Chain ✗

Schema exists at `SSTableReader::scan()` but doesn't reach state machine:

```rust
// data_access.rs - Schema available here
pub async fn scan(..., schema: Option<&TableSchema>) -> Result<...> {
    // ... processes blocks ...
    self.parse_partition_data(data)  // ← Schema not passed!
}
```

**Fix**: Thread schema through:
- `scan()` → `parse_partition_data(data, schema)`
- `parse_partition_data()` → `RowCellStateMachine::with_schema_and_version()`
- `parse_partition_data()` → `extract_value_from_parsed_row_with_schema()`

---

## Implementation Plan (Corrected)

### ✅ Already Complete

**Format Detection + Routing Fix** (6 hours):
- Added `DataFormat` enum (`header.rs`)
- Fixed routing to avoid state machine for V5CompressedLegacy
- Added unit tests
- Queries no longer crash

**Files Modified**:
- `cqlite-core/src/parser/header.rs` (DataFormat enum, data_format() method)
- `cqlite-core/src/storage/sstable/reader/parsing/block_entries.rs` (routing)

### ❌ Critical Work Remaining

#### Phase 3: Wire Schema Into State Machine (4-6 hours)

**Tasks**:

1. **Update parse_partition_data signature** (`parsing/mod.rs` line 255):
   ```rust
   // Add schema parameter
   pub fn parse_partition_data(&self, data: &[u8], schema: Option<&TableSchema>) -> Result<...>
   ```

2. **Create schema-aware state machine** (`parsing/mod.rs` line 264):
   ```rust
   let mut state_machine = if let Some(schema) = schema {
       // Get comparators from schema
       let comparators = schema.get_partition_key_comparators()
           .map_err(|e| Error::Schema(format!("No partition key comparators: {}", e)))?;
       
       if comparators.is_empty() {
           return Err(Error::Schema("Schema has no partition keys".into()));
       }
       
       // Create with schema, comparator, and version
       RowCellStateMachine::with_schema_and_version(
           schema.clone(),
           comparators[0].clone(),
           self.header.cassandra_version
       )
   } else {
       // Schemaless fallback (for legacy formats only)
       RowCellStateMachine::new()
   };
   ```

3. **Thread schema through call sites**:
   - `data_access.rs` scan() → parse_partition_data(data, schema)
   - `block_entries.rs` → parse_partition_data(data, schema)
   - Any other callers

#### Phase 4: Fix Value Extraction (3-4 hours)

**Tasks**:

1. **Add new schema-aware extractor** (`parsing/mod.rs` after line 320):
   ```rust
   pub fn extract_value_from_parsed_row_with_schema(
       &self,
       parsed_row: &ParsedRow,
       schema: &TableSchema,
   ) -> Result<Value> {
       let mut columns = HashMap::new();
       
       // Parse partition key columns
       for (idx, component) in parsed_row.partition_key.components.iter().enumerate() {
           if let Some(pk_col) = schema.partition_keys.get(idx) {
               let typed = self.parse_value_with_schema_type(component, &pk_col.data_type)?;
               columns.insert(pk_col.name.clone(), typed);
           }
       }
       
       // Parse clustering key columns
       if let Some(ref clustering_key) = parsed_row.clustering_key {
           // Extract clustering components and map to schema
       }
       
       // Parse regular columns from cells
       for cell in &parsed_row.cells {
           if let Some(col) = schema.columns.iter().find(|c| c.name == cell.name) {
               let typed = self.parse_value_with_schema_type(&cell.value_bytes, &col.data_type)?;
               columns.insert(cell.name.clone(), typed);
           }
       }
       
       if columns.is_empty() {
           return Err(Error::Schema("No columns matched schema".into()));
       }
       
       Ok(Value::Row(columns))
   }
   ```

2. **Update parse_partition_data to use new extractor** (line 273):
   ```rust
   for parsed_row in parsed_rows {
       let row_key = self.extract_row_key_from_parsed_row(&parsed_row)?;
       
       let value = if let Some(schema) = schema {
           // Use schema-aware extraction
           self.extract_value_from_parsed_row_with_schema(&parsed_row, schema)?
       } else {
           // Fallback for schemaless (legacy formats)
           self.extract_value_from_parsed_row_fallback(&parsed_row)?
       };
       
       results.push((row_key, value));
   }
   ```

3. **Rename old extractor to _fallback**:
   ```rust
   fn extract_value_from_parsed_row_fallback(&self, parsed_row: &ParsedRow) -> Result<Value> {
       // Keep existing logic for legacy schemaless parsing
   }
   ```

#### Phase 5: Storage-Layer Testing (2-3 hours)

**Critical**: Test at storage layer, not just CLI output

**File**: `cqlite-core/tests/storage/sstable_schema_awareness.rs` (NEW)

```rust
#[tokio::test]
async fn test_v5_compressed_legacy_with_schema_returns_typed_values() {
    // Load schema
    let schema_text = std::fs::read_to_string("test-data/schemas/basic-types.cql")?;
    let schema = parse_schema(&schema_text, "test_basic", "simple_table")?;
    
    // Open SSTable
    let sstable_path = glob("test-data/datasets/sstables/test_basic/simple_table-*/nb-1-big-Data.db")?[0];
    let reader = SSTableReader::open(&sstable_path).await?;
    
    // Scan WITH schema
    let table_id = TableId::new("test_basic.simple_table");
    let results = reader.scan(&table_id, None, None, Some(1), Some(&schema)).await?;
    
    assert_eq!(results.len(), 1, "Should return 1 row");
    
    let (row_key, value) = &results[0];
    
    // CRITICAL: Assert value is Row with proper types
    match value {
        Value::Row(columns) => {
            // Assert UUID type (not blob!)
            assert!(
                matches!(columns.get("id"), Some(Value::UUID(_))),
                "id should be Value::UUID, got: {:?}",
                columns.get("id")
            );
            
            // Assert Text type (not blob!)
            assert!(
                matches!(columns.get("name"), Some(Value::Text(_))),
                "name should be Value::Text, got: {:?}",
                columns.get("name")
            );
            
            // Assert Integer type (not blob!)
            assert!(
                matches!(columns.get("age"), Some(Value::Integer(_))),
                "age should be Value::Integer, got: {:?}",
                columns.get("age")
            );
            
            // Assert Timestamp type (not blob!)
            assert!(
                matches!(columns.get("created_at"), Some(Value::Timestamp(_))),
                "created_at should be Value::Timestamp, got: {:?}",
                columns.get("created_at")
            );
            
            // Verify all expected columns present
            assert!(columns.contains_key("id"));
            assert!(columns.contains_key("name"));
            assert!(columns.contains_key("age"));
            assert!(columns.contains_key("created_at"));
        }
        _ => panic!("Expected Value::Row, got: {:?}", value),
    }
}

#[tokio::test]
async fn test_v5_without_schema_fails_clearly() {
    let sstable_path = glob("test-data/datasets/sstables/test_basic/simple_table-*/nb-1-big-Data.db")?[0];
    let reader = SSTableReader::open(&sstable_path).await?;
    
    // Try to scan V5CompressedLegacy WITHOUT schema
    let result = reader.scan(&table_id, None, None, Some(1), None).await;
    
    // Should fail with clear error message
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("Schema required") || err_msg.contains("schema"),
        "Error should mention schema requirement, got: {}",
        err_msg
    );
}
```

#### Phase 6: Integration Testing (3-4 hours)

After storage-layer tests pass, validate CLI:

```bash
# Test JSON output shows proper types
CQLITE_SCHEMA=test-data/schemas/basic-types.cql \
cqlite -e "SELECT id, name, age FROM test_basic.simple_table LIMIT 1" --out json | \
  jq '.[] | {
    id_type: (.id | type),
    name_type: (.name | type), 
    age_type: (.age | type)
  }'

# Expected: {"id_type": "string", "name_type": "string", "age_type": "number"}
# NOT: {"id_type": "array", ...}  ← Would indicate blobs
```

---

## Critical Code Locations

### Schema Propagation (Working ✓)

**Already implemented in Issue #157**:

```rust
// cqlite-core/src/query/select_executor.rs (lines 198-223)
let schema_opt = self._schema.find_schema_by_table(&keyspace, &table_name).await;
let scan_results = self.storage.scan(table_id, start_key, end_key, limit, schema_opt.as_ref()).await?;

// cqlite-core/src/storage/sstable/mod.rs (line 654)
let results = reader.scan(table_id, start_key, end_key, None, schema).await?;

// cqlite-core/src/storage/sstable/reader/data_access.rs (line 57)
pub async fn scan(..., schema: Option<&TableSchema>) -> Result<...> {
    // Schema is HERE but not used for parsing!
}
```

### Where Schema Needs to Wire In (Broken ✗)

**File**: `cqlite-core/src/storage/sstable/reader/parsing/mod.rs`

**Line 264** - State machine creation:
```rust
// CURRENT (no schema):
let mut state_machine = RowCellStateMachine::new();

// NEEDED (with schema):
let mut state_machine = if let Some(schema) = schema {
    RowCellStateMachine::with_schema_and_version(
        schema.clone(),
        schema.get_partition_key_comparators()?[0].clone(),
        self.header.cassandra_version
    )
} else {
    RowCellStateMachine::new()
};
```

**Line 273** - Value extraction:
```rust
// CURRENT (returns first cell as blob):
let value = self.extract_value_from_parsed_row(&parsed_row)?;

// NEEDED (builds row map with all columns):
let value = if let Some(schema) = schema {
    self.extract_value_from_parsed_row_with_schema(&parsed_row, schema)?
} else {
    self.extract_value_from_parsed_row_fallback(&parsed_row)?
};
```

### State Machine Already Has Schema Support (Unused ✓)

**File**: `cqlite-core/src/storage/sstable/row_cell_state_machine.rs`

**Lines 196-209** - Constructor exists:
```rust
pub fn with_schema_and_version(
    schema: TableSchema,
    comparator: ComparatorType,
    version: CassandraVersion,
) -> Self {
    Self {
        state: State::Header,
        offset: 0,
        parsed_row: None,
        error_message: None,
        schema: Some(schema),  // ← Schema stored here
        comparator: Some(comparator),
        version,
    }
}
```

**This already exists** - we just need to CALL it!

---

## Implementation Checklist

### Phase 3: Wire Schema to State Machine (4-6 hours)

- [ ] Update `parse_partition_data()` signature to accept `schema: Option<&TableSchema>`
- [ ] Create state machine with `with_schema_and_version()` when schema available
- [ ] Thread schema parameter through all call sites:
  - [ ] `data_access.rs` → `parse_partition_data(data, schema)`
  - [ ] `block_entries.rs` → `parse_partition_data(data, schema)`
  - [ ] Any other callers
- [ ] Add error if V5CompressedLegacy called without schema

### Phase 4: Fix Value Extraction (3-4 hours)

- [ ] Create `extract_value_from_parsed_row_with_schema()` 
  - [ ] Build `HashMap<String, Value>` with ALL columns
  - [ ] Map column names from cells to schema definitions
  - [ ] Parse each value with `parse_value_with_schema_type()`
  - [ ] Return `Value::Row(columns)`
- [ ] Rename old extractor to `extract_value_from_parsed_row_fallback()`
- [ ] Update `parse_partition_data()` to call new extractor when schema available

### Phase 5: Storage-Layer Tests (2-3 hours)

- [ ] Create `cqlite-core/tests/storage/sstable_schema_awareness.rs`
- [ ] Add `test_v5_compressed_legacy_with_schema_returns_typed_values()`
  - [ ] Assert `Value::UUID` for UUID columns
  - [ ] Assert `Value::Text` for text columns
  - [ ] Assert `Value::Integer` for int columns
  - [ ] Assert `Value::Timestamp` for timestamp columns
  - [ ] Assert `Value::Row` contains all expected columns
- [ ] Add `test_v5_without_schema_fails_clearly()`
- [ ] Add tests for collections, UDTs, composite keys

### Phase 6: Integration Testing (3-4 hours)

- [ ] Test all table groups with proper type assertions:
  - [ ] test_basic/* (simple types, composite keys, TTL, counters)
  - [ ] test_collections/* (sets, lists, maps, UDTs)
  - [ ] test_timeseries/* (timestamps, time bucketing)
  - [ ] test_wide_rows/* (many columns, large data)
- [ ] Test all output formats:
  - [ ] JSON shows proper types (not arrays)
  - [ ] CSV shows readable values (not hex)
  - [ ] Table shows formatted values
- [ ] Run CI smoke test: `test-data/scripts/ci-one-shot-smoke.sh`
- [ ] Validate REPL mode

### Phase 7: Documentation (1-2 hours)

- [ ] Update `docs/sstables-definitive-guide/chapters/05-data-db-format.md`
- [ ] Add code comments explaining schema wiring
- [ ] Update Issue #158 with final status
- [ ] Document V5CompressedLegacy format characteristics

---

## Common Pitfalls to Avoid

### ❌ Don't: Just Reroute Parsers

Changing routing without wiring schema won't fix the problem - values will still be blobs.

### ❌ Don't: Test Only CLI Output

JSON might look okay but hide blob-to-string conversions. Test `Value` types directly.

### ❌ Don't: Use Non-Existent Helpers

Don't invent `find_partition_boundary()` or manufacture TableIds. Use existing extraction.

### ✅ Do: Wire Schema Through Full Stack

Schema needs to flow: scan() → parse_partition_data() → state machine → cell extraction.

### ✅ Do: Build Complete Row Maps

`extract_value_from_parsed_row_with_schema()` must return ALL columns, not first cell.

### ✅ Do: Test at Storage Layer First

Assert `Value::UUID`, `Value::Text`, etc. at SSTableReader level before testing CLI.

---

## Alternative Approach: SchemaAwareReader

If state machine approach proves difficult, consider:

**SchemaAwareReader** (`cqlite-core/src/storage/sstable/schema_aware_reader.rs`):
- Already designed for schema-first parsing
- Proper row/column structure
- Type-safe throughout

**Trade-offs**:
- ✅ Cleaner architecture
- ✅ Better long-term maintainability
- ❌ Requires chunk/block handling work (8-12 hours)
- ❌ More code churn

**Recommendation**: Try state machine wiring first (simpler). If issues arise, pivot to SchemaAwareReader.

---

## Testing Strategy

### Layer 1: Storage (CRITICAL - Test First)

```rust
// Assert Value types directly
assert!(matches!(value, Value::Row(_)));
assert!(matches!(columns.get("id"), Some(Value::UUID(_))));
```

**Why**: Catches type issues before they propagate to CLI

### Layer 2: CLI Output

```bash
# Verify JSON shows proper types
cqlite -e "SELECT id FROM test_basic.simple_table LIMIT 1" --out json
# Should show: [{"id": "uuid-string"}]
# NOT: [{"id": [98, 76, 45, ...]}]
```

**Why**: Validates end-to-end user experience

### Layer 3: Integration

```bash
# CI smoke test
test-data/scripts/ci-one-shot-smoke.sh
```

**Why**: Validates production readiness

---

## Timeline (Realistic)

| Phase | Task | Hours | Dependencies |
|-------|------|-------|--------------|
| ✅ 1-2 | Format detection + routing | 6 | - |
| 3 | Wire schema to state machine | 4-6 | Phases 1-2 |
| 4 | Fix value extraction | 3-4 | Phase 3 |
| 5 | Storage-layer tests | 2-3 | Phase 4 |
| 6 | Integration tests | 3-4 | Phase 5 |
| 7 | Documentation | 1-2 | Phase 6 |
| **Total** | | **19-25 hours** | |

**With SchemaAwareReader pivot** (if needed): +8-12 hours = **27-37 hours total**

---

## Success Metrics

### Storage Layer
- `SSTableReader::scan()` returns `Value::Row(HashMap)` with proper types
- Zero `Value::Blob` for columns with schema
- All columns present in row map

### CLI Layer
- JSON output: `{"id": "string", "age": number}` (not arrays)
- CSV output: readable values
- Table output: properly formatted

### Integration
- All test table groups pass
- CI smoke test: exit code 0
- Issue #157 validated

---

## Priority & Assignment

**Priority**: P0 - Critical  
**Blocks**: M2 CLI Milestone  
**Estimated Effort**: 13-19 hours remaining (after 6 hours complete)  
**Complexity**: Medium-High (requires threading schema through parser stack)

**Recommended Assignment**: Senior developer familiar with:
- Schema architecture
- State machine parsing
- Storage layer internals

---

## Related Documentation

- **Issue #157**: Schema propagation (complete, needs validation)
- **Issue #156**: Path extraction (complete)
- **M2 CLI Spec**: `docs/development/M2_CLI_SPEC.md`
- **Schema Propagation**: `docs/architecture/SCHEMA_PROPAGATION_DECISION.md`
- **SSTable Guide**: `docs/sstables-definitive-guide/`

---

**Created**: 2025-10-14  
**GitHub Issue**: [#158](https://github.com/pmcfadin/cqlite/issues/158)  
**Status**: Partial fix implemented, schema wiring critical path identified  
**Next Action**: Wire TableSchema into RowCellStateMachine

