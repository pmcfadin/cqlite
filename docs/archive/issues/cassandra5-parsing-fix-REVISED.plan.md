# Comprehensive Cassandra 5.0 Parsing & Architecture Validation Plan (REVISED)

## Executive Summary

**Root Cause**: Two-part problem:
1. ✅ **FIXED**: State machine routing - V5_0DataFormat was incorrectly routed to RowCellStateMachine
2. ❌ **CRITICAL**: Schema not being used - Legacy parser has stub column name extraction, returns blobs instead of typed data

**Impact**: **P0 - Blocks M2 CLI** - Queries execute but return incorrect data types (blobs instead of UUIDs, timestamps, etc.)

**Status**: 
- ✅ Phase 1-2 Complete: Format detection and routing fixed
- ❌ Phase 3 Critical: Schema usage not implemented - queries return untyped data

## Problem Analysis (Updated)

### Part 1: Format Mismatch ✅ FIXED

**Original Error**:
```
❌ State machine processing error: Data corruption: Failed to parse partition key component count
```

**Cause**: V5_0DataFormat uses compressed blocks with legacy serialization, not VInt-encoded "oa" format

**Solution Implemented**:
- Added `DataFormat` enum to classify formats correctly
- Fixed routing to only use state machine for true oa formats
- V5CompressedLegacy now routes to legacy parser

**Result**: Queries no longer crash ✓

### Part 2: Schema Not Being Used ❌ CRITICAL

**Current Behavior**:
```bash
$ CQLITE_SCHEMA=test-data/schemas/basic-types.cql cqlite -e "SELECT * FROM test_basic.simple_table LIMIT 1"
✅ Exit code: 0  # Doesn't crash
❌ Returns: Value::Blob(...) for ALL columns
✗ Expected: UUID for id, Text for name, Timestamp for created_at, etc.
```

**Root Cause**:

In `value_parsing.rs` lines 29-38:
```rust
if let Some(schema) = self.get_table_schema(schema) {
    if let Some(column_name) = self.extract_column_name_from_context(table_id, key) {
        // Parse using schema type
        return self.parse_value_with_schema_type(value_data, &column.data_type);
    }
}
// Falls through to blob fallback
```

But `extract_column_name_from_context()` is a **STUB** (lines 426-441):
```rust
pub fn extract_column_name_from_context(&self, _table_id: &TableId, _key: &RowKey) -> Option<String> {
    None  // ← Always returns None!
}
```

**Impact**: Even with schema loaded via CLI, parser can't map data bytes to schema columns, so it returns blob data.

### The Real Problem

The legacy parser (`parse_block_entries`) doesn't understand the **partition structure** needed to extract column names:

```
Partition Block:
  [Partition Key Data]
  [Row Flags]
  [Clustering Key Data]
  [Column Name] [Column Value] ← Need to extract this name!
  [Column Name] [Column Value]
  ...
```

Without column names, we can't look up types in the schema, so everything becomes a blob.

## Solution Design (Revised)

### Option A: Use parse_partition_data (RECOMMENDED)

**Already exists** in `parsing/mod.rs` lines 253-290!

This method:
- ✅ Uses `RowCellStateMachine` to parse partition structure
- ✅ Extracts `ParsedRow` with column data
- ✅ Already called by other code paths
- ✅ Returns proper `(RowKey, Value)` tuples

**Implementation**:
```rust
// In parse_block_entries for V5CompressedLegacy
if matches!(data_format, DataFormat::V5CompressedLegacy) {
    // Use existing parse_partition_data which understands row structure
    return self.parse_partition_data_with_schema(data, schema);
}
```

**Files to modify**:
1. `block_entries.rs` - Route V5CompressedLegacy to partition parser
2. `parsing/mod.rs` - Ensure parse_partition_data uses schema for type extraction

**Pros**:
- ✅ Minimal new code (reuse existing parser)
- ✅ Already handles row structure correctly
- ✅ Can extract column names from parsed rows

**Cons**:
- May need to adapt for compressed legacy format specifics

### Option B: Use SchemaAwareReader

**File**: `cqlite-core/src/storage/sstable/schema_aware_reader.rs`

This is the "gold standard" schema-driven reader:
- ✅ Full schema awareness (partition keys, clustering keys, columns)
- ✅ Type-safe parsing throughout
- ✅ Proper comparator handling

**Implementation**:
```rust
if matches!(data_format, DataFormat::V5CompressedLegacy) {
    // Create SchemaAwareReader for this SSTable
    let schema_reader = SchemaAwareReader::new(self, schema)?;
    return schema_reader.parse_block_with_schema(data)?;
}
```

**Pros**:
- ✅ Cleanest architecture
- ✅ Full type safety
- ✅ Future-proof

**Cons**:
- Requires understanding SchemaAwareReader API
- May need adaptation for block-level parsing

### Option C: Fix Column Name Extraction (NOT RECOMMENDED)

Implement proper column name extraction in legacy parser.

**Why Not**: Most complex, duplicates existing functionality.

### RECOMMENDED: Option A (parse_partition_data)

Use existing partition parser which already understands row structure.

## Implementation Plan (Revised)

### ✅ Phase 1: Format Detection (COMPLETE)

**Status**: Done ✓
- Added `DataFormat` enum
- Added `data_format()` classification method
- Test passes: `test_v5_format_classification()`

### ✅ Phase 2: Routing Fix (COMPLETE)

**Status**: Done ✓
- Fixed state machine routing
- Added debug logging
- Queries no longer crash

### ❌ Phase 3: Schema-Aware Parsing (CRITICAL - 6-10 hours)

**Status**: In Progress - Critical gap identified

**Objective**: Make legacy parser actually USE the schema for type interpretation

#### Step 3.1: Verify Current Behavior (1 hour)

Test actual output to confirm data types:

```bash
# Test with real query
CQLITE_SCHEMA=test-data/schemas/basic-types.cql \
CQLITE_DATA_DIR=test-data/datasets/sstables \
cqlite -e "SELECT id, name, age, created_at FROM test_basic.simple_table LIMIT 1" --out json

# Expected (if working correctly):
# {"id": "4d432...", "name": "Alice", "age": 25, "created_at": "2023-..."}

# Actual (if broken):
# {"id": [blob bytes], "name": [blob bytes], "age": [blob bytes], ...}
```

Add instrumentation:
```rust
eprintln!("[DEBUG] Parsed value type: {:?}", value);
// Should show Value::UUID, Value::Text, Value::Integer, etc.
// NOT Value::Blob for everything
```

#### Step 3.2: Route to Partition Parser (3-4 hours)

**File**: `cqlite-core/src/storage/sstable/reader/parsing/block_entries.rs`

Update V5CompressedLegacy routing:

```rust
if matches!(data_format, DataFormat::V5CompressedLegacy) {
    eprintln!("[DEBUG] Using partition data parser for V5CompressedLegacy");
    
    // parse_partition_data understands row structure and can extract column names
    let mut results = Vec::new();
    let mut offset = 0;
    
    while offset < data.len() {
        // Extract partition data for this entry
        let partition_end = self.find_partition_boundary(&data[offset..])?;
        let partition_data = &data[offset..offset + partition_end];
        
        // Use existing parse_partition_data which handles structure correctly
        if let Some(partition_results) = self.parse_partition_data(partition_data)? {
            results.extend(partition_results);
        }
        
        offset += partition_end;
    }
    
    return Ok(results.into_iter().map(|(key, value)| {
        (TableId::new("table"), key, value)
    }).collect());
}
```

**Key Changes**:
1. Route V5CompressedLegacy to `parse_partition_data` instead of generic legacy parser
2. `parse_partition_data` uses `RowCellStateMachine` which extracts column structure
3. Can then map column names to schema types

#### Step 3.3: Enhance parse_partition_data for Schema (2-3 hours)

**File**: `cqlite-core/src/storage/sstable/reader/parsing/mod.rs`

Update `parse_partition_data` to use schema:

```rust
pub(in crate::storage::sstable::reader) fn parse_partition_data(
    &self,
    data: &[u8],
) -> Result<Option<Vec<(RowKey, Value)>>> {
    // ... existing parsing ...
    
    for parsed_row in parsed_rows {
        let row_key = self.extract_row_key_from_parsed_row(&parsed_row)?;
        
        // NEW: Extract value using schema if available
        let value = if let Some(schema) = self.get_table_schema(None) {
            self.extract_value_from_parsed_row_with_schema(&parsed_row, &schema)?
        } else {
            self.extract_value_from_parsed_row(&parsed_row)?
        };
        
        results.push((row_key, value));
    }
}
```

Add new method:
```rust
fn extract_value_from_parsed_row_with_schema(
    &self,
    parsed_row: &ParsedRow,
    schema: &TableSchema,
) -> Result<Value> {
    let mut columns = HashMap::new();
    
    // parsed_row.cells contains column names and values
    for cell in &parsed_row.cells {
        let column_name = &cell.name;
        
        // Look up column type in schema
        if let Some(column_def) = schema.columns.iter().find(|c| c.name == column_name) {
            // Parse value using schema type
            let typed_value = self.parse_value_with_schema_type(
                &cell.value_bytes,
                &column_def.data_type
            )?;
            columns.insert(column_name.clone(), typed_value);
        }
    }
    
    Ok(Value::Row(columns))
}
```

#### Step 3.4: Test Schema Usage (1-2 hours)

**Tests**:

```rust
#[test]
fn test_v5_compressed_legacy_with_schema() {
    let schema = load_schema("test-data/schemas/basic-types.cql");
    let sstable = load_sstable("test-data/datasets/sstables/test_basic/simple_table-*/");
    
    let results = sstable.scan_with_schema(&schema, None, None, Some(1)).await?;
    
    // Verify proper types
    assert!(matches!(results[0].get("id"), Some(Value::UUID(_))));
    assert!(matches!(results[0].get("name"), Some(Value::Text(_))));
    assert!(matches!(results[0].get("age"), Some(Value::Integer(_))));
    assert!(matches!(results[0].get("created_at"), Some(Value::Timestamp(_))));
    
    // NOT blobs!
    assert!(!matches!(results[0].get("id"), Some(Value::Blob(_))));
}
```

### Phase 4: Validate Schema Propagation (Issue #157) (2-3 hours)

**Objective**: Verify schema flows correctly from CLI → QueryEngine → SSTableReader

**Test E2E**:
```bash
# Should return properly typed JSON
CQLITE_SCHEMA=test-data/schemas/basic-types.cql \
CQLITE_DATA_DIR=test-data/datasets/sstables \
cqlite -e "SELECT id, name, age, created_at FROM test_basic.simple_table LIMIT 1" --out json

# Verify output has proper types, not blobs:
# {"id": "uuid-string", "name": "text", "age": 25, "created_at": "timestamp"}
```

**Verification Points**:
1. ✅ Schema loaded from CLI (CQLITE_SCHEMA env var)
2. ✅ Schema registered in SchemaManager
3. ✅ QueryEngine looks up schema from SchemaManager
4. ✅ Schema passed to storage.scan()
5. ✅ SSTableReader receives schema
6. ✅ Parser uses schema for type interpretation ← **THIS IS NEW**
7. ✅ Results contain properly typed values

### Phase 5: Integration Testing (3-4 hours)

**Test Scenarios**:

1. **Type Accuracy Tests**:
   ```bash
   # UUID columns
   cqlite -e "SELECT id FROM test_basic.simple_table LIMIT 1" --out json
   # Should show: {"id": "4d432..."}  NOT {"id": [98, 76, ...]}
   
   # Timestamp columns  
   cqlite -e "SELECT created_at FROM test_basic.simple_table LIMIT 1"
   # Should show: "2023-10-14T..." NOT blob
   
   # Collections
   cqlite -e "SELECT tags FROM test_collections.collection_table LIMIT 1"
   # Should show: ["tag1", "tag2"] NOT blob
   ```

2. **All Table Groups**:
   - test_basic/* (primitive types, composite keys)
   - test_collections/* (sets, lists, maps, UDTs)
   - test_timeseries/* (time-bucketed data)
   - test_wide_rows/* (wide partitions, many columns)

3. **Output Formats**:
   ```bash
   --out json  # Should show typed values as JSON
   --out csv   # Should show properly formatted values
   --out table # Should show readable values, not hex blobs
   ```

4. **CI Smoke Test**:
   ```bash
   test-data/scripts/ci-one-shot-smoke.sh
   # Must pass with exit code 0
   # Must return properly typed data
   ```

### Phase 6: Documentation (1-2 hours)

**Updates**:

1. **Code Comments** (already done for Phase 1-2):
   - ✅ `header.rs` - DataFormat enum rationale
   - ✅ `block_entries.rs` - Routing logic
   - ⏳ `parsing/mod.rs` - Schema usage in partition parser

2. **User Documentation**:
   - Update `docs/sstables-definitive-guide/chapters/05-data-db-format.md`
   - Add section on V5CompressedLegacy format
   - Explain schema requirement for type interpretation

3. **Architecture Documentation**:
   - Document that V5CompressedLegacy REQUIRES schema
   - Update Issue #158 with findings

## Acceptance Criteria (Revised)

### Must Have

**Phase 1-2** (Complete ✅):
- [x] `DataFormat` enum distinguishes formats correctly
- [x] State machine routing fixed
- [x] Queries execute without crashing
- [x] Code compiles, tests pass

**Phase 3** (Critical - In Progress ❌):
- [ ] Schema is actually USED for type interpretation
- [ ] UUID columns return `Value::UUID`, not `Value::Blob`
- [ ] Timestamp columns return `Value::Timestamp`, not `Value::Blob`
- [ ] Integer columns return `Value::Integer`, not `Value::Blob`
- [ ] Collection columns return proper collection types, not blobs
- [ ] Test query returns properly typed JSON output

**Phase 4** (Schema Propagation):
- [ ] End-to-end test validates schema flow from CLI to parser
- [ ] Debug logs show "Using schema for type interpretation"
- [ ] Issue #157 validated working correctly

**Phase 5** (Integration):
- [ ] All test table groups return properly typed data
- [ ] CI smoke test passes with correct output
- [ ] All output formats (JSON, CSV, table) show proper types

**Phase 6** (Documentation):
- [ ] Code comments explain schema usage
- [ ] User docs updated with format details
- [ ] Issue #158 updated with final status

### Nice to Have

- [ ] Performance comparison with/without schema
- [ ] Support for schemaless parsing (blob fallback) with feature flag
- [ ] Debug mode showing type resolution decisions

## Risk Assessment (Updated)

### Critical Risk ⚠️

**Schema usage not implemented**:
- Queries appear to work but return incorrect data types
- Users may not notice blob data until they try to use it
- **Mitigation**: Comprehensive type testing in Phase 3-5

### High Risk

**parse_partition_data may need adaptation**:
- Currently expects uncompressed data
- May need tweaks for compressed legacy format
- **Mitigation**: Test thoroughly with real V5CompressedLegacy data

### Medium Risk

**Column name extraction complexity**:
- ParsedRow structure may not have column names in expected format
- May need to extract from cell metadata
- **Mitigation**: Fallback to positional mapping if names unavailable

### Low Risk

**Performance of schema lookup**:
- Looking up column types per cell may be slow
- **Mitigation**: Cache schema column lookups per partition

## Timeline Estimate (Revised)

- ✅ **Phase 1** (Format Detection): Complete (4 hours)
- ✅ **Phase 2** (Routing Fix): Complete (2 hours)
- ❌ **Phase 3** (Schema-Aware Parsing): **6-10 hours** ← CRITICAL
- **Phase 4** (Schema Validation): 2-3 hours
- **Phase 5** (Integration Testing): 3-4 hours
- **Phase 6** (Documentation): 1-2 hours
- **Total Remaining**: **12-19 hours** (1.5-2.5 days)

## Priority

**Priority**: **P0 - Critical**

**Rationale**:
- Queries execute but return WRONG data (blobs instead of proper types)
- Blocks M2 CLI with incorrect output
- Users may not notice until data is used
- Must be fixed before any M2 release

## Key Insights (Updated)

1. ✅ **Routing fixed**: V5CompressedLegacy no longer crashes
2. ❌ **Schema not used**: Legacy parser can't extract column names
3. 💡 **Solution exists**: `parse_partition_data` already handles structure
4. 🎯 **Focus**: Route V5CompressedLegacy → partition parser + schema

## Next Steps

1. **Verify current behavior** - Confirm queries return blobs
2. **Implement Option A** - Route to parse_partition_data
3. **Enhance with schema** - Add schema-aware value extraction
4. **Test type accuracy** - Verify UUIDs are UUIDs, not blobs
5. **Validate E2E** - Confirm Issue #157 works with proper types

---

**Status**: Phase 1-2 Complete ✓, Phase 3 Critical Gap Identified ❌  
**Next Action**: Implement schema-aware parsing for V5CompressedLegacy  
**Timeline**: 12-19 hours remaining  
**Blocker Level**: P0 - Must fix before M2 release

