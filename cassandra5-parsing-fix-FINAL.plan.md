# Cassandra 5.0 Parsing Fix - Final Technical Plan

## Critical Gap Analysis

The previous plans identified symptoms but **missed the architectural fix**. Here's what's really broken:

### The Real Problem

1. **Schema IS propagated** (Issue #157) ✓
   - CLI → SchemaManager → QueryEngine → SSTableReader::scan(schema)
   - Schema parameter reaches the parser

2. **But schema is NOT wired into the state machine** ✗
   - `parse_partition_data()` calls `RowCellStateMachine::new()` (no schema)
   - `parse_block_entries()` doesn't pass schema to state machine
   - State machine without schema → can't emit typed cells → everything becomes blob

3. **Value extraction is structurally broken** ✗
   - `extract_value_from_parsed_row()` returns first non-null cell or fallback text
   - Doesn't build `Value::Map` with all columns keyed by name
   - Multi-column rows collapse to single field

4. **Routing changes are superficial** ✗
   - My plan shuffled parsers around without fixing schema plumbing
   - Even with correct routing, no schema in state machine = still blobs

## Root Cause: Schema-to-State-Machine Gap

**Current flow** (broken):
```rust
// SSTableReader::scan receives schema
pub async fn scan(..., schema: Option<&TableSchema>) -> Result<Vec<(RowKey, Value)>> {
    // ... but when parsing blocks:
    self.parse_partition_data(data)  // ← Schema not passed!
}

// parse_partition_data creates state machine without schema
fn parse_partition_data(&self, data: &[u8]) -> Result<...> {
    let mut state_machine = RowCellStateMachine::new();  // ← No schema!
    // ... state machine can't emit typed values
}

// extract_value_from_parsed_row returns first cell
fn extract_value_from_parsed_row(&self, parsed_row: &ParsedRow) -> Result<Value> {
    // Find first non-null cell
    for cell in &parsed_row.cells {
        if !cell.value_bytes.is_empty() {
            return Ok(Value::Blob(cell.value_bytes.clone()));  // ← Blob!
        }
    }
    Ok(Value::Text(format!("row_with_{}_cells", parsed_row.cells.len())))
}
```

**What needs to happen**:
```rust
// Pass schema through to state machine
fn parse_partition_data(&self, data: &[u8], schema: &TableSchema) -> Result<...> {
    // Get partition key comparators from schema
    let comparators = schema.get_partition_key_comparators()?;
    
    // Create schema-aware state machine
    let mut state_machine = RowCellStateMachine::with_schema_and_version(
        schema.clone(),
        comparators[0].clone(),
        self.header.cassandra_version
    );
    
    // State machine can now emit typed values
}

// Build proper row map with all columns
fn extract_value_from_parsed_row(&self, parsed_row: &ParsedRow, schema: &TableSchema) -> Result<Value> {
    let mut columns = HashMap::new();
    
    for cell in &parsed_row.cells {
        // Look up column type in schema
        if let Some(column) = schema.columns.iter().find(|c| c.name == cell.name) {
            // Parse with correct type
            let typed_value = self.parse_value_with_schema_type(&cell.value_bytes, &column.data_type)?;
            columns.insert(cell.name.clone(), typed_value);
        }
    }
    
    Ok(Value::Map(columns))  // ← Proper multi-column row!
}
```

## Correct Solution Architecture

### Phase 1: Wire Schema Into State Machine (4-6 hours)

**Objective**: Make state machine schema-aware for V5CompressedLegacy

#### Step 1.1: Update parse_partition_data Signature

**File**: `cqlite-core/src/storage/sstable/reader/parsing/mod.rs` (line 255)

**Current**:
```rust
pub(in crate::storage::sstable::reader) fn parse_partition_data(
    &self,
    data: &[u8],
) -> Result<Option<Vec<(RowKey, Value)>>> {
```

**Change to**:
```rust
pub(in crate::storage::sstable::reader) fn parse_partition_data(
    &self,
    data: &[u8],
    schema: Option<&TableSchema>,  // ← Add schema parameter
) -> Result<Option<Vec<(RowKey, Value)>>> {
```

#### Step 1.2: Create Schema-Aware State Machine

**File**: `cqlite-core/src/storage/sstable/reader/parsing/mod.rs` (line 264)

**Current**:
```rust
let mut state_machine = super::super::row_cell_state_machine::RowCellStateMachine::new();
```

**Change to**:
```rust
let mut state_machine = if let Some(schema) = schema {
    // For V5 formats, require schema
    match schema.get_partition_key_comparators() {
        Ok(comparators) if !comparators.is_empty() => {
            RowCellStateMachine::with_schema_and_version(
                schema.clone(),
                comparators[0].clone(),
                self.header.cassandra_version
            )
        }
        _ => {
            return Err(Error::Schema(format!(
                "Schema required for V5 format but has no partition key comparators"
            )));
        }
    }
} else {
    // Fallback for legacy formats or schemaless parsing
    RowCellStateMachine::new()
};
```

#### Step 1.3: Thread Schema Through Call Sites

**Files to update**:

1. **`cqlite-core/src/storage/sstable/reader/data_access.rs`** (scan method)
   - Currently has schema parameter but doesn't pass it to parse_partition_data
   - Add schema parameter to parse calls

2. **`cqlite-core/src/storage/sstable/reader/parsing/block_entries.rs`**
   - Pass schema when calling parse_partition_data
   - For V5CompressedLegacy, ensure schema is provided

**Key changes**:
```rust
// In scan() method where blocks are processed
if matches!(self.header.cassandra_version.data_format(), DataFormat::V5CompressedLegacy) {
    // Require schema for V5CompressedLegacy
    if schema.is_none() {
        return Err(Error::Schema("Schema required for V5CompressedLegacy format".into()));
    }
    
    // Parse with schema
    if let Some(partition_results) = self.parse_partition_data(decompressed_data, schema)? {
        results.extend(partition_results);
    }
}
```

### Phase 2: Fix Value Extraction (3-4 hours)

**Objective**: Build proper multi-column row maps instead of single-cell blobs

#### Step 2.1: Rewrite extract_value_from_parsed_row

**File**: `cqlite-core/src/storage/sstable/reader/parsing/mod.rs` (line 293)

**Current** (broken):
```rust
pub(in crate::storage::sstable::reader) fn extract_value_from_parsed_row(
    &self,
    parsed_row: &ParsedRow,
) -> Result<Value> {
    // Find first non-null cell and return it as blob
    for cell in &parsed_row.cells {
        if !cell.value_bytes.is_empty() {
            return Ok(Value::Blob(cell.value_bytes.clone()));  // ← WRONG!
        }
    }
    Ok(Value::Text(format!("row_with_{}_cells", parsed_row.cells.len())))
}
```

**Replace with**:
```rust
pub(in crate::storage::sstable::reader) fn extract_value_from_parsed_row_with_schema(
    &self,
    parsed_row: &ParsedRow,
    schema: &TableSchema,
) -> Result<Value> {
    let mut columns = HashMap::new();
    
    // Process all cells in the parsed row
    for cell in &parsed_row.cells {
        let column_name = &cell.name;
        
        // Look up column definition in schema
        if let Some(column_def) = schema.columns.iter().find(|c| c.name == column_name) {
            // Parse value using exact schema type
            let typed_value = self.parse_value_with_schema_type(
                &cell.value_bytes,
                &column_def.data_type
            )?;
            columns.insert(column_name.clone(), typed_value);
        } else {
            eprintln!("[WARN] Column '{}' not found in schema, skipping", column_name);
        }
    }
    
    if columns.is_empty() {
        return Err(Error::Schema(format!(
            "No columns matched schema for row with {} cells",
            parsed_row.cells.len()
        )));
    }
    
    // Return structured row with all columns
    Ok(Value::Row(columns))  // ← Proper row map!
}
```

#### Step 2.2: Update parse_partition_data to Use New Extractor

**File**: `cqlite-core/src/storage/sstable/reader/parsing/mod.rs` (line 270)

**Current**:
```rust
for parsed_row in parsed_rows {
    let row_key = self.extract_row_key_from_parsed_row(&parsed_row)?;
    let value = self.extract_value_from_parsed_row(&parsed_row)?;  // ← Old
    results.push((row_key, value));
}
```

**Change to**:
```rust
for parsed_row in parsed_rows {
    let row_key = self.extract_row_key_from_parsed_row(&parsed_row)?;
    
    let value = if let Some(schema) = schema {
        // Use schema-aware extraction for typed values
        self.extract_value_from_parsed_row_with_schema(&parsed_row, schema)?
    } else {
        // Fallback for schemaless parsing (legacy formats)
        self.extract_value_from_parsed_row_fallback(&parsed_row)?
    };
    
    results.push((row_key, value));
}
```

### Phase 3: Test at Storage Layer (2-3 hours)

**Objective**: Verify Value types at SSTableReader level, not just CLI output

#### Step 3.1: Add Storage-Layer Tests

**File**: `cqlite-core/tests/sstable_reader_schema_tests.rs` (NEW)

```rust
use cqlite_core::storage::sstable::SSTableReader;
use cqlite_core::schema::TableSchema;
use cqlite_core::types::Value;

#[tokio::test]
async fn test_v5_compressed_legacy_returns_typed_values() {
    // Load schema
    let schema = load_test_schema("test-data/schemas/basic-types.cql", "test_basic", "simple_table");
    
    // Open SSTable
    let reader = SSTableReader::open("test-data/datasets/sstables/test_basic/simple_table-*/nb-1-big-Data.db").await?;
    
    // Scan with schema
    let results = reader.scan(&table_id, None, None, Some(1), Some(&schema)).await?;
    
    assert_eq!(results.len(), 1);
    let (key, value) = &results[0];
    
    // Verify value is a Row map, not a blob
    match value {
        Value::Row(columns) => {
            // Verify column types from schema
            assert!(matches!(columns.get("id"), Some(Value::UUID(_))), 
                "id should be UUID, got {:?}", columns.get("id"));
            
            assert!(matches!(columns.get("name"), Some(Value::Text(_))), 
                "name should be Text, got {:?}", columns.get("name"));
            
            assert!(matches!(columns.get("age"), Some(Value::Integer(_))), 
                "age should be Integer, got {:?}", columns.get("age"));
            
            assert!(matches!(columns.get("created_at"), Some(Value::Timestamp(_))), 
                "created_at should be Timestamp, got {:?}", columns.get("created_at"));
        }
        _ => panic!("Expected Value::Row, got {:?}", value),
    }
}

#[tokio::test]
async fn test_v5_without_schema_fails_gracefully() {
    let reader = SSTableReader::open("test-data/datasets/sstables/test_basic/simple_table-*/nb-1-big-Data.db").await?;
    
    // Try to scan V5CompressedLegacy without schema
    let result = reader.scan(&table_id, None, None, Some(1), None).await;
    
    // Should fail with clear error
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Schema required"));
}
```

#### Step 3.2: Add Debug Instrumentation

**File**: `cqlite-core/src/storage/sstable/reader/parsing/mod.rs`

Add logging to track value types:
```rust
// After extracting value
eprintln!("[DEBUG] Extracted value type: {}", match &value {
    Value::Row(cols) => format!("Row({} columns)", cols.len()),
    Value::UUID(_) => "UUID".to_string(),
    Value::Text(_) => "Text".to_string(),
    Value::Integer(_) => "Integer".to_string(),
    Value::Blob(_) => "Blob".to_string(),  // ← Should NOT see this for V5 with schema!
    _ => format!("{:?}", value),
});
```

### Phase 4: Handle V5CompressedLegacy Routing (2-3 hours)

**Objective**: Ensure V5CompressedLegacy uses the schema-aware path

#### Step 4.1: Update Block Entry Parsing

**File**: `cqlite-core/src/storage/sstable/reader/parsing/block_entries.rs`

For V5CompressedLegacy with schema, route to partition parser:

```rust
let data_format = self.header.cassandra_version.data_format();

if matches!(data_format, DataFormat::V5CompressedLegacy) {
    eprintln!("[DEBUG] V5CompressedLegacy detected");
    
    if schema.is_none() {
        return Err(Error::Schema(
            "Schema required for V5CompressedLegacy format. \
             Use --schema flag to provide schema.".into()
        ));
    }
    
    // Use partition parser which can handle schema-aware state machine
    return self.parse_block_with_partition_parser(data, schema);
}
```

#### Step 4.2: Implement parse_block_with_partition_parser

**File**: `cqlite-core/src/storage/sstable/reader/parsing/block_entries.rs`

```rust
fn parse_block_with_partition_parser(
    &self,
    data: &[u8],
    schema: Option<&TableSchema>,
) -> Result<Vec<(TableId, RowKey, Value)>> {
    // V5CompressedLegacy blocks contain partition data
    // Use existing parse_partition_data which now accepts schema
    
    let table_id = TableId::from_path(&self.file_path)?;  // Extract from SSTable path
    
    if let Some(partition_results) = self.parse_partition_data(data, schema)? {
        Ok(partition_results.into_iter()
            .map(|(row_key, value)| (table_id.clone(), row_key, value))
            .collect())
    } else {
        Ok(Vec::new())
    }
}
```

**Note**: Need helper for TableId extraction:
```rust
impl TableId {
    pub fn from_path(path: &Path) -> Result<Self> {
        // Extract keyspace.table from SSTable path
        // e.g., "test-data/.../test_basic/simple_table-uuid/nb-1-big-Data.db"
        // → "test_basic.simple_table"
        
        let components: Vec<_> = path.components()
            .filter_map(|c| c.as_os_str().to_str())
            .collect();
        
        // Find pattern: keyspace_name/table_name-uuid/
        for i in 0..components.len().saturating_sub(1) {
            if let Some((table, _)) = components[i+1].split_once('-') {
                return Ok(TableId::new(format!("{}.{}", components[i], table)));
            }
        }
        
        Err(Error::Schema("Cannot extract table ID from path".into()))
    }
}
```

### Phase 5: Consider SchemaAwareReader Path (Optional, 8-12 hours)

**Objective**: Evaluate if SchemaAwareReader is better for V5CompressedLegacy

**Current state**:
- SchemaAwareReader is architecturally cleaner (designed for schema-first parsing)
- But requires chunk/block handling rework
- May not be MVP-critical if state machine approach works

**Decision point**: 
- If Phase 1-4 work cleanly → stick with state machine approach
- If Phase 1-4 have issues → pivot to SchemaAwareReader

**SchemaAwareReader approach** (if needed):
```rust
if matches!(data_format, DataFormat::V5CompressedLegacy) {
    let schema = schema.ok_or_else(|| Error::Schema("Schema required".into()))?;
    
    // Create schema-aware reader
    let schema_reader = SchemaAwareReader::new(self, schema)?;
    
    // Parse block with full schema context
    return schema_reader.parse_decompressed_block(data)?;
}
```

**Requires**:
1. SchemaAwareReader::parse_decompressed_block() method
2. Chunk/block boundary detection
3. Row assembly with proper column mapping

## Acceptance Criteria (Revised)

### Storage Layer (Critical)

- [ ] `SSTableReader::scan()` with schema returns `Value::Row(HashMap<String, Value>)`
- [ ] UUID columns return `Value::UUID`, not `Value::Blob`
- [ ] Timestamp columns return `Value::Timestamp`, not `Value::Blob`
- [ ] Integer columns return `Value::Integer`, not `Value::Blob`
- [ ] Storage-layer tests pass asserting Value types
- [ ] V5CompressedLegacy without schema returns clear error

### CLI Layer

- [ ] JSON output shows `{"id": "uuid-string", "name": "text", ...}` not blob arrays
- [ ] CSV output shows readable values, not hex
- [ ] Table output shows proper types, not blobs
- [ ] One-shot queries work: `cqlite --schema X --data-dir Y -e "SELECT ..."`
- [ ] REPL queries work: `:schema load X`, `SELECT ...`

### Integration

- [ ] All test table groups return typed data
- [ ] CI smoke test passes
- [ ] Issue #157 schema propagation validated working

## Timeline (Revised)

- **Phase 1**: Wire schema into state machine (4-6 hours)
- **Phase 2**: Fix value extraction to build row maps (3-4 hours)
- **Phase 3**: Storage-layer tests (2-3 hours)
- **Phase 4**: V5CompressedLegacy routing (2-3 hours)
- **Phase 5**: SchemaAwareReader (optional, 8-12 hours if needed)

**Total**: **11-16 hours** (with schema-aware state machine)  
**Total with SchemaAwareReader**: **19-28 hours** (if pivot needed)

## Risk Assessment (Updated)

### Critical Risks

1. **ParsedRow.cells may not have column names**
   - State machine might not populate cell names
   - Mitigation: Inspect `RowCellStateMachine` output, add cell name extraction if needed

2. **V5CompressedLegacy may have format nuances**
   - Block boundaries, multiple partitions per block
   - Mitigation: Test incrementally, add boundary detection as needed

### Medium Risks

1. **State machine may not work for compressed legacy**
   - May need format-specific tweaks
   - Mitigation: If state machine fails, pivot to SchemaAwareReader (Phase 5)

2. **Performance of schema lookups per cell**
   - HashMap lookups per column
   - Mitigation: Acceptable for correctness; optimize later if needed

## Implementation Order

**Day 1** (6-8 hours):
1. ✅ Phase 1 complete: Wire schema into state machine
2. ✅ Phase 2 complete: Fix value extraction
3. ⏳ Phase 3 partial: Add basic storage-layer test

**Day 2** (5-8 hours):
1. ✅ Phase 3 complete: Full storage-layer tests
2. ✅ Phase 4 complete: V5CompressedLegacy routing
3. ⏳ Integration testing

**Day 3** (optional, if SchemaAwareReader needed):
1. Phase 5: SchemaAwareReader implementation

## Key Insights (Corrected)

1. ✅ Routing fix prevents crashes (Phase 1-2 of previous plan)
2. ❌ But doesn't enable schema usage - need state machine wiring
3. 🎯 Real fix: Thread schema → state machine → typed cells → row maps
4. 🧪 Test at storage layer to catch issues early
5. 🔄 SchemaAwareReader is fallback if state machine approach fails

## Action Items

1. **Immediately**: Wire schema into `RowCellStateMachine::with_schema_and_version`
2. **Next**: Rewrite `extract_value_from_parsed_row` to build row maps
3. **Then**: Add storage-layer tests asserting Value types
4. **Finally**: Route V5CompressedLegacy correctly with schema requirement

---

**Status**: Phases 1-2 (format detection/routing) complete, but **Phase 3 (schema usage) is the real fix**  
**Critical Path**: Schema → State Machine → Typed Cells → Row Maps  
**Timeline**: 11-16 hours for schema-aware approach (correct fix)  
**Priority**: P0 - Required for M2, previous "fix" was incomplete

