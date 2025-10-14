# Cassandra 5.0 Parsing Investigation - Executive Summary

**Date**: October 14, 2025  
**GitHub Issue**: [#158](https://github.com/pmcfadin/cqlite/issues/158)  
**Priority**: P0 - Critical  
**Status**: Root cause identified, partial fix implemented, architecture validated

---

## The Problem in Plain English

After implementing Issue #157 (schema propagation), CI tests started failing with:

```
❌ State machine processing error: Data corruption: 
   Failed to parse partition key component count
```

**Investigation revealed TWO problems**:

1. ✅ **Format mismatch** (FIXED): Wrong parser for V5_0DataFormat
2. ❌ **Schema not used** (CRITICAL): Parser can't map columns to types

---

## What We Fixed (6 hours)

### Part 1: Format Detection ✓

**Problem**: Code treated all V5.0 formats as using "oa" VInt encoding  
**Reality**: V5_0DataFormat uses compressed blocks with legacy serialization  

**Solution**: Added format classification system

```rust
pub enum DataFormat {
    LegacyOA,              // Old uncompressed
    V5CompressedLegacy,    // Real-world Cassandra 5.0 (most common)
    V5UncompressedOA,      // Theoretical true oa format (rare)
}
```

**Files Modified**:
- `cqlite-core/src/parser/header.rs` (DataFormat enum + classification)
- `cqlite-core/src/storage/sstable/reader/parsing/block_entries.rs` (routing fix)

**Result**: Queries no longer crash ✓

### Part 2: Routing Fix ✓

**Problem**: V5_0DataFormat routed to wrong parser (RowCellStateMachine)  
**Solution**: Route based on actual data format, not version number

```rust
// Now correctly routes based on data format
let data_format = self.header.cassandra_version.data_format();
let use_state_machine = matches!(data_format, DataFormat::V5UncompressedOA);
```

**Result**: V5_0DataFormat now uses legacy parser (correct) ✓

---

## What's Still Broken (13-19 hours remaining)

### The Real Issue: Schema Not Wired to State Machine

**Your question was spot-on**: Does the plan account for schema passed via CLI?

**Answer**: Schema PROPAGATES correctly (Issue #157 works!) BUT isn't USED for parsing.

### Why Queries Return Blobs Instead of Proper Types

```rust
// Schema reaches SSTableReader::scan()
pub async fn scan(..., schema: Option<&TableSchema>) -> Result<...> {
    // ... but then parsing calls:
    self.parse_partition_data(data)  // ← Schema not passed!
}

// parse_partition_data creates state machine WITHOUT schema
fn parse_partition_data(&self, data: &[u8]) -> Result<...> {
    let mut state_machine = RowCellStateMachine::new();  // ← No schema!
    
    // Without schema, state machine can't emit typed cells
    // Everything becomes Value::Blob
    
    // And then extractor returns FIRST CELL only:
    for cell in &parsed_row.cells {
        return Ok(Value::Blob(cell.value_bytes));  // ← Wrong!
    }
}
```

### Current vs Expected Output

**Current** (what queries actually return today):
```json
[{
  "id": [77, 67, 33, 226, 102, 43, ...],        // ← Blob array
  "name": [65, 108, 105, 99, 101],              // ← Blob array
  "age": [0, 0, 0, 25]                          // ← Blob array
}]
```

**Expected** (after full fix):
```json
[{
  "id": "4d4321e2-662b-4ba1-b75f-48e080727a52", // ← UUID string
  "name": "Alice",                               // ← Text string
  "age": 25                                      // ← Number
}]
```

---

## The Real Fix Required

### Three Changes Needed

#### 1. Wire Schema Into State Machine

**File**: `cqlite-core/src/storage/sstable/reader/parsing/mod.rs` (line 255)

```rust
// Add schema parameter
pub fn parse_partition_data(&self, data: &[u8], schema: Option<&TableSchema>) -> Result<...>

// Create schema-aware state machine
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

#### 2. Fix Value Extraction to Build Row Maps

**File**: `cqlite-core/src/storage/sstable/reader/parsing/mod.rs` (new method)

```rust
fn extract_value_from_parsed_row_with_schema(
    &self,
    parsed_row: &ParsedRow,
    schema: &TableSchema,
) -> Result<Value> {
    let mut columns = HashMap::new();
    
    // Process ALL cells (not just first one!)
    for cell in &parsed_row.cells {
        if let Some(col) = schema.columns.iter().find(|c| c.name == cell.name) {
            let typed = self.parse_value_with_schema_type(&cell.value_bytes, &col.data_type)?;
            columns.insert(cell.name.clone(), typed);
        }
    }
    
    Ok(Value::Row(columns))  // ← Multi-column row with proper types
}
```

#### 3. Thread Schema Through Call Sites

**Files**: `data_access.rs`, `block_entries.rs`

Ensure schema parameter flows:
- `scan(schema)` → `parse_partition_data(data, schema)`

---

## Why Your Question Was Critical

**You asked**: "Does it account for schema passed via CLI?"

**Initial answer**: "Yes, Issue #157 propagates schema"  
**Real answer**: "Schema propagates BUT isn't wired into state machine"

**The gap**: 
- Schema reaches `SSTableReader::scan()` ✓
- But `parse_partition_data()` doesn't accept schema parameter ✗
- State machine created without schema ✗
- Values extracted as blobs ✗

**This is exactly what you suspected** - the plan didn't fully account for schema USAGE, only schema PROPAGATION.

---

## Technical Debt & Architectural Issues

### Issue 1: extract_column_name_from_context is a Stub

**File**: `cqlite-core/src/storage/sstable/reader/parsing/value_parsing.rs` (line 426)

```rust
pub fn extract_column_name_from_context(...) -> Option<String> {
    None  // ← Always returns None!
}
```

**Why this exists**: Legacy parser tries to use schema but can't extract column names from row context.

**Real fix**: Don't use this approach - use `ParsedRow.cells` which HAS column names!

### Issue 2: Value Extraction is Lossy

**File**: `cqlite-core/src/storage/sstable/reader/parsing/mod.rs` (line 320)

Current extractor:
- Returns first non-null cell
- Falls back to `Value::Text("row_with_X_cells")`
- Doesn't build multi-column rows

**Fix**: Complete rewrite to build `Value::Row(HashMap)`.

### Issue 3: No Storage-Layer Testing

Tests only check CLI output (JSON/CSV). Need to test `Value` types at SSTableReader level.

**Fix**: Add storage-layer tests asserting Value variants.

---

## Deliverables

### ✅ Completed (6 hours)

1. **Root Cause Analysis**
   - Format mismatch identified
   - Schema usage gap identified
   - Architecture validated

2. **Format Detection System**
   - `DataFormat` enum
   - Classification method
   - Unit tests

3. **Routing Fix**
   - V5CompressedLegacy no longer uses state machine
   - Queries don't crash

4. **Documentation**
   - GitHub Issue #158
   - Three plan documents (original, revised, final)
   - Dev handoff document
   - Code comments

### ⏳ Pending (13-19 hours)

1. **Schema Wiring** (4-6 hours)
   - Update parse_partition_data signature
   - Create schema-aware state machine
   - Thread schema through call sites

2. **Value Extraction Fix** (3-4 hours)
   - Rewrite to build row maps
   - Include all columns with proper types

3. **Storage-Layer Tests** (2-3 hours)
   - Assert Value types directly
   - Test V5CompressedLegacy with/without schema

4. **Integration Tests** (3-4 hours)
   - All table groups
   - All output formats
   - CI smoke test

5. **Documentation** (1-2 hours)
   - SSTable guide updates
   - Final status

---

## Recommendations for Dev Team

### Development Order

1. **Start with storage-layer test** (write first, implement after):
   ```rust
   // This test will fail initially - that's expected
   assert!(matches!(columns.get("id"), Some(Value::UUID(_))));
   ```

2. **Wire schema to state machine** (make test pass)

3. **Fix value extraction** (all columns, not just first)

4. **Validate with real data** (all table groups)

### Testing Approach

- **TDD**: Write storage-layer tests first
- **Incremental**: Test each table group separately
- **Debug**: Add instrumentation to see Value types

### If You Hit Issues

**State machine approach too complex?**
- Pivot to SchemaAwareReader
- Cleaner architecture, more work

**Can't extract column names from cells?**
- Inspect `ParsedRow.cells` structure
- May need to enhance state machine to populate cell names

---

## Success Criteria

**Storage Layer**:
- ✅ `Value::Row(HashMap<String, Value>)` returned
- ✅ UUID columns are `Value::UUID`
- ✅ Text columns are `Value::Text`
- ✅ Zero `Value::Blob` for columns with schema

**CLI Layer**:
- ✅ JSON shows proper types
- ✅ CSV shows readable values
- ✅ Table formatter works

**Integration**:
- ✅ All test table groups work
- ✅ CI smoke test passes
- ✅ Issue #157 validated

---

## Files Reference

**Modified** (Phases 1-2):
- `cqlite-core/src/parser/header.rs`
- `cqlite-core/src/storage/sstable/reader/parsing/block_entries.rs`

**Need to Modify** (Phases 3-6):
- `cqlite-core/src/storage/sstable/reader/parsing/mod.rs` (schema wiring)
- `cqlite-core/src/storage/sstable/reader/data_access.rs` (thread schema)
- `cqlite-core/tests/storage/sstable_schema_awareness.rs` (NEW - tests)

**Reference** (existing but unused):
- `cqlite-core/src/storage/sstable/row_cell_state_machine.rs` (has with_schema_and_version)
- `cqlite-core/src/storage/sstable/schema_aware_reader.rs` (alternative approach)

---

**Bottom Line**: 

The format detection work (6 hours) was necessary but **not sufficient**. The real fix is wiring schema into the state machine so it can emit typed values instead of blobs. This requires threading the schema parameter through the parsing stack and rewriting value extraction to build proper row maps.

**Estimated remaining**: 13-19 hours for schema wiring + testing + docs.

---

**Contact**: See Issue #158 for updates and technical discussion  
**Assignee**: TBD - Recommend senior developer  
**Timeline**: 2-3 days for complete implementation and testing

