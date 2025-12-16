# V5CompressedLegacy Binary Format Research & Implementation Plan

**Issue**: #159  
**Goal**: Native Rust parser for V5CompressedLegacy format  
**Timeline**: 1-2 developer-days (10-18 hours)  
**Priority**: P0 - Blocks M2

---

## Phase 1: Revert + Stabilize (30 minutes)

### Immediate Action: Revert Commit 180329a

```bash
# Revert the partition parser routing
git revert 180329a

# Or manual revert if needed:
git show 180329a:cqlite-core/src/storage/sstable/reader/parsing/block_entries.rs > /tmp/before_159.rs
# Restore the fall-through to legacy parser behavior

# Test CI goes green
git push
```

**Result**: CI green, blob output behavior restored (known state)

---

## Phase 2: Binary Format Research (4-6 hours)

### Step 1: Capture Decompressed Block (1 hour)

**Add debugging to dump raw data**:

```rust
// In block_entries.rs after decompression (line ~91)
if matches!(data_format, DataFormat::V5CompressedLegacy) {
    // Dump first 256 bytes for analysis
    let hex_dump = hex::encode(&data[..std::cmp::min(256, data.len())]);
    eprintln!("[RESEARCH] Decompressed block hex (first 256 bytes):");
    for chunk in hex_dump.as_bytes().chunks(64) {
        eprintln!("[RESEARCH] {}", std::str::from_utf8(chunk).unwrap());
    }
    
    // Dump block metadata
    eprintln!("[RESEARCH] Block size: {}", data.len());
    eprintln!("[RESEARCH] Format: {:?}", self.header.cassandra_version);
    eprintln!("[RESEARCH] Table: {}.{}", self.header.keyspace, self.header.table_name);
}
```

**Run test query**:
```bash
RUST_LOG=debug CQLITE_SCHEMA=test-data/schemas/basic-types.cql \
  cqlite -e "SELECT * FROM test_basic.simple_table LIMIT 1" 2>&1 | \
  grep RESEARCH > decompressed_block_dump.txt
```

**Analyze output**:
- First 16-32 bytes: What's the structure?
- Repeating patterns: What are the boundaries?
- Compare hex positions with known data

### Step 2: Cross-Reference sstabledump (1 hour)

**Get same partition from sstabledump**:
```bash
sstabledump test-data/datasets/sstables/test_basic/simple_table-*/nb-1-big-Data.db | \
  jq '.[0]' > first_partition.json
```

**Compare**:
- Partition key value from JSON → find in hex dump
- First column value from JSON → find in hex dump
- Identify boundaries and structure

### Step 3: Research Cassandra Source (2-3 hours)

**Key classes to study**:
```
org.apache.cassandra.db.rows.UnfilteredRowIteratorSerializer
org.apache.cassandra.db.rows.UnfilteredSerializer
org.apache.cassandra.db.SerializationHeader
org.apache.cassandra.io.sstable.format.big.BigFormat
```

**Focus questions**:
1. What's at offset 0 of decompressed chunk?
2. How are partition keys encoded? (u16 lengths? VInt?)
3. How are row headers structured?
4. How are cells laid out?
5. Where are column names (if present)?

**Document**: Create `V5_COMPRESSED_LEGACY_FORMAT_SPEC.md` with findings

### Step 4: Format Specification (1-2 hours)

**Document the actual format**:

```markdown
# V5CompressedLegacy Decompressed Block Format

## Block Structure

Offset 0x0000: [Partition header]
  - Partition key encoding: ???
  - Flags/metadata: ???

Offset 0x????: [Row data]
  - Row headers: ???
  - Clustering keys: ???
  - Cell structure: ???

## Field Encodings

- Lengths: u16 big-endian or VInt?
- Component boundaries: Fixed or variable?
- Cell format: Name + value or value only?

## Examples

From test_basic.simple_table:
- Hex offset 0x0000-0x0020: [analyzed structure]
- Partition key (UUID): [byte layout]
- First cell (name): [byte layout]
```

---

## Phase 3: Implement Parser (4-8 hours)

### Option 3A: Extend RowCellStateMachine (Simpler)

**If format is close to existing state machine**:

```rust
// In row_cell_state_machine.rs
impl RowCellStateMachine {
    pub fn with_v5_compressed_legacy(
        schema: TableSchema,
        version: CassandraVersion,
    ) -> Self {
        Self {
            state: State::Header,
            schema: Some(schema),
            version,
            encoding: Encoding::U16Lengths,  // NEW: Not VInt
            // ...
        }
    }
    
    fn parse_partition_key_u16(&mut self, data: &[u8]) -> Result<usize> {
        // Use u16 big-endian length prefixes instead of VInt
        let component_count = u16::from_be_bytes([data[0], data[1]]) as usize;
        let mut offset = 2;
        
        for i in 0..component_count {
            let component_len = u16::from_be_bytes([data[offset], data[offset+1]]) as usize;
            offset += 2;
            
            let component_data = &data[offset..offset + component_len];
            // ... extract component
            offset += component_len;
        }
        
        Ok(offset)
    }
}
```

### Option 3B: New V5CompressedLegacyParser (If Very Different)

**If format is completely different**:

```rust
// New file: cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs

pub struct V5CompressedLegacyParser {
    schema: TableSchema,
    header: SSTableHeader,
}

impl V5CompressedLegacyParser {
    pub fn parse_block(
        &self,
        data: &[u8],
    ) -> Result<Vec<(RowKey, Value)>> {
        // Implement based on format research
        // Use u16 lengths, handle clustering correctly
        // Map to schema columns
        // Return typed values
    }
}
```

**Integration**:
```rust
// In block_entries.rs
if matches!(data_format, DataFormat::V5CompressedLegacy) {
    let parser = V5CompressedLegacyParser::new(schema, &self.header);
    return parser.parse_block(&data)?
        .into_iter()
        .map(|(k, v)| (table_id.clone(), k, v))
        .collect();
}
```

### Step-by-Step Implementation

**Day 1 Research Complete → Start Day 2**:

1. **Choose parser approach** (30 min):
   - Extend state machine if format similar
   - New parser if format very different

2. **Implement partition key parsing** (1-2 hours):
   - Based on research findings
   - Handle u16 or identified encoding
   - Test with known partition keys

3. **Implement row/cell parsing** (2-3 hours):
   - Parse row headers
   - Extract clustering keys
   - Parse cells with column names
   - Map to schema columns

4. **Integrate typed value extraction** (1 hour):
   - Use existing `parse_value_with_schema_type()`
   - Build row maps (from Issue #158)
   - Return Value::Udt/Row

5. **Test with real data** (1-2 hours):
   - Test all test_basic tables
   - Verify typed output
   - Check for edge cases

---

## Phase 4: Validation & Integration (2-4 hours)

### Testing Checklist

**Storage Layer**:
- [ ] Parse simple_table successfully
- [ ] Return Value::Row with proper types
- [ ] UUID columns are Value::UUID (not Blob)
- [ ] Text columns are Value::Text (not Blob)
- [ ] Integer columns are Value::Integer (not Blob)

**All Table Groups**:
- [ ] test_basic/* (simple types, composite keys, TTL, counters)
- [ ] test_collections/* (sets, lists, maps, UDTs)
- [ ] test_timeseries/* (timestamps, time buckets)
- [ ] test_wide_rows/* (wide partitions, many columns)

**Integration**:
- [ ] CI smoke test passes
- [ ] JSON output shows proper types
- [ ] CSV output readable
- [ ] Table output formatted

**Add Tests**:
```rust
#[tokio::test]
async fn test_v5_compressed_legacy_native_parsing() {
    let schema = load_schema("test-data/schemas/basic-types.cql", "test_basic", "simple_table");
    let reader = SSTableReader::open("test-data/datasets/.../nb-1-big-Data.db").await?;
    
    let results = reader.scan(&table_id, None, None, Some(1), Some(&schema)).await?;
    
    assert_eq!(results.len(), 1);
    match &results[0].1 {
        Value::Row(cols) => {
            assert!(matches!(cols.get("id"), Some(Value::UUID(_))));
            assert!(matches!(cols.get("name"), Some(Value::Text(_))));
            assert!(matches!(cols.get("age"), Some(Value::Integer(_))));
        }
        _ => panic!("Expected Value::Row"),
    }
}
```

---

## Research Tools & Resources

### Hex Dump Tools

```bash
# Dump decompressed block
hexdump -C decompressed_block.bin | head -40

# Or with annotations
xxd -g 1 -c 16 decompressed_block.bin | head -40
```

### Cassandra Source References

**GitHub**: https://github.com/apache/cassandra/tree/cassandra-5.0.0

**Key files**:
```
src/java/org/apache/cassandra/db/rows/
  - UnfilteredRowIteratorSerializer.java
  - UnfilteredSerializer.java
  - BufferCell.java

src/java/org/apache/cassandra/db/
  - SerializationHeader.java
  - ColumnSerializer.java
  
src/java/org/apache/cassandra/io/sstable/format/big/
  - BigTableWriter.java
  - BigFormat.java
```

### Existing Documentation

**In repo**:
- `docs/sstables-definitive-guide/chapters/05-data-db-format.md`
- `docs/sstables-definitive-guide/ISSUE_149_LEARNINGS.md`
- `docs/sstables-definitive-guide/ISSUE_153_LEARNINGS.md`

**Look for**: Serialization header format, partition layout, cell encoding

---

## Expected Format Structure (Hypothesis)

Based on Cassandra source, V5CompressedLegacy likely uses:

```
Decompressed Chunk:
  [SerializationHeader - if present]
    - Partition key columns (types, counts)
    - Clustering key columns
    - Regular columns
    - Static columns
  
  [Partition 1]
    [Partition Key - u16 lengths?]
      u16 component_count (or single u16 length for single key)
      u16 component_1_length
      bytes component_1_data
      ...
    
    [Unfiltered Row Iterator]
      [Row 1]
        flags (1 byte?)
        timestamp (8 bytes)
        [Clustering Key - if present]
        [Cells]
          [Cell 1]
            column_name_or_id
            value_bytes
          [Cell 2]
            ...
      [Row 2]
        ...
  
  [Partition 2]
    ...
```

**Key unknowns**:
- Exact partition key encoding (u16 vs VInt)
- Presence/absence of serialization header
- Cell structure (name included or ID only?)
- Clustering key format

**Research will clarify** these specifics.

---

## Success Metrics

### Research Phase Complete When:

- [ ] Decompressed block hex captured and analyzed
- [ ] Format structure documented with byte offsets
- [ ] Partition key encoding identified (u16/VInt/other)
- [ ] Row/cell structure understood
- [ ] Column name extraction method known
- [ ] Format spec document written

### Implementation Complete When:

- [ ] Parser handles identified format
- [ ] Extracts all cells correctly (not 0)
- [ ] Maps cells to schema columns
- [ ] Returns properly typed values
- [ ] Tests pass with real data

### M2 Ready When:

- [ ] CI green
- [ ] Queries return typed output
- [ ] All table groups work
- [ ] Integration tests pass
- [ ] No external dependencies

---

## Risk Mitigation

### Risk 1: Format More Complex Than Expected

**Mitigation**: 
- Start with simplest table (simple_table)
- Incremental implementation
- Test each component separately

### Risk 2: Multiple Format Variants

**Mitigation**:
- Test with all V5_0Format variants (C, D, E, F, G)
- Document differences
- Implement variant handling if needed

### Risk 3: Timeline Exceeds Estimate

**Mitigation**:
- Day 1 checkpoint: Format identified?
- If blocked: Escalate, consider alternatives
- Keep Patrick updated on progress

---

## Deliverables

### Day 1 (Research):
1. Decompressed block hex dump
2. Format specification document
3. Comparison with Cassandra source
4. Implementation approach decided

### Day 2 (Implementation):
1. Native V5CompressedLegacy parser
2. Integration with schema wiring
3. Tests with real data
4. CI green

### Final:
1. Working M2 CLI queries
2. Typed output (UUID, Text, Integer)
3. Single binary (no external deps)
4. Production-ready code

---

## Recommended Team Structure

**Senior Developer** (Lead):
- Binary format research
- Cassandra source analysis
- Parser architecture decisions

**Mid-Level Developer** (Implementation):
- Parser coding
- Integration with existing code
- Test implementation

**Total**: 1-2 developers, 1-2 days

---

## Timeline

**Day 1** (Research):
- Morning: Revert + hex dump capture (2-3h)
- Afternoon: Cassandra source research + format spec (2-3h)
- **Deliverable**: Format specification document

**Day 2** (Implementation):
- Morning: Parser implementation (3-4h)
- Afternoon: Testing + integration (2-4h)
- **Deliverable**: Working native parser

**Total**: 10-18 hours across 1-2 days

---

## Communication Plan

**Daily Updates**:
- End of Day 1: Format research findings
- End of Day 2: Implementation status
- Blockers: Immediate escalation

**Checkpoints**:
- After 4 hours: Format identified? (Go/No-go)
- After 8 hours: Parser working for simple_table? (Go/No-go)
- After 12 hours: All table groups? (Go/No-go)

---

## Success Criteria

**M2 Blocker Resolved When**:

```bash
# This command works:
CQLITE_SCHEMA=test-data/schemas/basic-types.cql \
cqlite -e "SELECT id, name, age FROM test_basic.simple_table LIMIT 1" --out json

# Returns:
[{"id": "uuid-string", "name": "Alice", "age": 25}]

# NOT:
[{"id": [blob], "name": [blob], "age": [blob]}]
```

---

**Next Action**: Assign senior developer to format research phase

See Issue #159 for updates and discussion.

