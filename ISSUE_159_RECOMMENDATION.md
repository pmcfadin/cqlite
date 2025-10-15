# Issue #159: Strategic Recommendation

**Date**: October 14, 2025  
**Status**: Core issue discovered - Unknown binary format  
**Decision Needed**: Choose path forward for M2

---

## The Situation

Team implemented Issue #159 correctly, but discovered:

**Neither parser can handle V5CompressedLegacy decompressed blocks**:
- ❌ Legacy parser: Expects simple entries → parsing fails
- ❌ Partition parser: Expects VInt partitions → extracts 0 cells

**Conclusion**: **We don't know the actual decompressed block format**

---

## What This Means

### The Format Mystery

**We know**:
- File format: `nb-1-big-Data.db` (NB format)
- Magic number: `0x8080_015c` (V5_0DataFormat)
- Compression: Works (Snappy decompresses 15867 → 16384 bytes)
- Schema: Loaded (19 columns for simple_table)

**We DON'T know**:
- What those 16384 decompressed bytes actually contain
- How partition data is laid out
- How rows and cells are encoded
- If it matches any existing parser

### Why Both Parsers Fail

**Legacy parser assumes**:
```
VInt table_id_len
bytes table_id
VInt key_len  ← Fails here
bytes key_data
VInt value_len
bytes value_data
```

**Partition parser assumes**:
```
VInt component_count  ← Gets 0 cells
VInt component_len
bytes component_data
... (VInt-encoded partition structure)
```

**Actual V5CompressedLegacy** (unknown):
```
??? (some Cassandra 5.0 serialization format)
??? (not matching either parser's expectations)
```

---

## Three Strategic Options

### Option A: Binary Format Research 📊

**Goal**: Understand and implement correct V5CompressedLegacy parser

**Approach**:
1. Hex dump decompressed blocks
2. Compare with sstabledump JSON output
3. Research Cassandra 5.0 SerializationHeader format
4. Implement format-specific parser

**Pros**:
- ✅ Proper long-term solution
- ✅ Native binary parsing
- ✅ No external dependencies
- ✅ Complete M2 CLI functionality

**Cons**:
- ❌ Time-consuming (8-16 hours)
- ❌ Requires format research
- ❌ May discover more complexity

**Estimated Time**: 8-16 hours (1-2 developer-days)

**Recommendation for**: Proper M2 release with full binary parsing

---

### Option B: sstabledump Bypass 🚀

**Goal**: Quick M2 unblock using existing tools

**Approach**:
1. Shell out to `sstabledump` for V5 SSTables
2. Parse JSON output instead of binary
3. Convert JSON → typed values
4. Defer binary parsing to M3

**Pros**:
- ✅ Quick implementation (2-4 hours)
- ✅ Leverages working tool
- ✅ Unblocks M2 immediately
- ✅ Proven output format

**Cons**:
- ❌ External dependency (sstabledump)
- ❌ Not pure Rust solution
- ❌ Performance overhead (shell exec)
- ❌ Defers binary parsing problem

**Estimated Time**: 2-4 hours

**Recommendation for**: Quick M2 MVP, defer proper parsing to M3

**Implementation**:
```rust
if matches!(data_format, DataFormat::V5CompressedLegacy) {
    // Use sstabledump for V5 formats
    return self.parse_with_sstabledump(schema)?;
}

fn parse_with_sstabledump(&self, schema: &TableSchema) -> Result<Vec<...>> {
    // Shell out: sstabledump <file>
    // Parse JSON output
    // Map to typed values using schema
}
```

---

### Option C: SchemaAwareReader Pivot 🔧

**Goal**: Use existing SchemaAwareReader infrastructure

**Approach**:
1. Adapt SchemaAwareReader for block-level parsing
2. Leverage existing schema-first design
3. May handle format better

**Pros**:
- ✅ Uses existing code
- ✅ Schema-first architecture
- ✅ Designed for typed parsing

**Cons**:
- ❌ Significant refactor (12-20 hours)
- ❌ Chunk/block handling needed
- ❌ May still hit same format issue

**Estimated Time**: 12-20 hours (1.5-2.5 developer-days)

**Recommendation for**: Post-M2 refactor, not M2 critical path

---

## My Recommendation: **Option B for M2, Option A for M3**

### Short-Term: sstabledump Bypass (M2)

**Why**:
- ⏱️ Fastest path to working M2 (2-4 hours)
- ✅ Proven to work (sstabledump handles V5)
- ✅ Unblocks testing and development
- ✅ Acceptable MVP solution

**Implementation**:
```rust
// In block_entries.rs
if matches!(data_format, DataFormat::V5CompressedLegacy) {
    log::debug!("Using sstabledump for V5CompressedLegacy format");
    return self.parse_via_sstabledump(&self.file_path, schema);
}
```

**Trade-off**: External dependency, but gets M2 shipping

### Medium-Term: Binary Format Research (M3)

**Why**:
- 🎯 Proper solution
- 🔬 Understand actual format
- 🚀 Native performance
- ✅ No dependencies

**Approach**:
1. Debug decompressed blocks
2. Identify serialization format
3. Implement native parser
4. Remove sstabledump dependency

**Timeline**: M3 milestone (1-2 weeks)

---

## Decision Matrix

| Criteria | Option A (Research) | Option B (sstabledump) | Option C (SchemaAware) |
|----------|---------------------|------------------------|------------------------|
| Time to M2 | ❌ 8-16 hours | ✅ 2-4 hours | ❌ 12-20 hours |
| Long-term solution | ✅ Yes | ⚠️ Workaround | ✅ Yes |
| External deps | ✅ None | ❌ sstabledump | ✅ None |
| Complexity | ⚠️ Medium | ✅ Low | ❌ High |
| M2 risk | ⚠️ Medium | ✅ Low | ❌ High |

**For M2**: Option B wins on time and risk  
**For M3**: Option A for proper solution

---

## Concrete Next Steps

### If You Choose Option A (Research)

**Create Issue #160**: "Research V5CompressedLegacy decompressed block format"

**Tasks**:
1. Hex dump decompressed blocks
2. Compare with sstabledump output
3. Research Cassandra 5.0 serialization
4. Document format specification
5. Implement parser
6. Test and validate

**Assignee**: Senior developer with binary format experience  
**Timeline**: 8-16 hours (1-2 days)

### If You Choose Option B (sstabledump)

**Update Issue #159**: Add sstabledump bypass implementation

**Tasks**:
1. Implement parse_via_sstabledump() (1-2h)
2. Test with all table groups (1h)
3. Add integration tests (1-2h)
4. Document dependency (30min)

**Assignee**: Any developer  
**Timeline**: 2-4 hours (same day)

**M3 Follow-up**: Create "Remove sstabledump dependency" issue

### If You Choose Option C (SchemaAwareReader)

**Update Issue #159**: Change to SchemaAwareReader migration

**Tasks**:
1. Design SchemaAwareReader block API (2-4h)
2. Implement chunk/block handling (4-6h)
3. Test with V5CompressedLegacy (2-4h)
4. Integration tests (2-4h)
5. Migration effort (2-4h)

**Assignee**: Senior developer  
**Timeline**: 12-20 hours (1.5-2.5 days)

---

## My Strong Recommendation

### For M2 MVP

✅ **Choose Option B (sstabledump bypass)**

**Reasons**:
1. ⏱️ Fastest to working (2-4 hours)
2. ✅ Proven reliable (sstabledump works)
3. ✅ Low risk for M2 deadline
4. ✅ Unblocks all M2 testing
5. ✅ Acceptable dependency for MVP

**Trade-offs acknowledged**:
- Requires sstabledump installation
- Not pure Rust (shell exec overhead)
- Technical debt for M3

**But**: Gets M2 out the door, buys time for proper fix

### For M3 Proper Fix

📊 **Schedule Option A (binary format research)**

**Reasons**:
1. Remove sstabledump dependency
2. Native performance
3. Complete solution
4. Better architecture

**Timeline**: 8-16 hours in M3

---

## Immediate Actions

### If Choosing Option B (Recommended)

1. **Update Issue #159** with sstabledump approach
2. **Assign to developer** (junior OK, straightforward impl)
3. **Timeline**: Today/tomorrow (2-4 hours)
4. **M2 status**: Unblocked ✅

**Implementation outline**:
```rust
fn parse_via_sstabledump(
    &self,
    file_path: &Path,
    schema: &TableSchema,
) -> Result<Vec<(TableId, RowKey, Value)>> {
    // Run: sstabledump <file>
    let output = Command::new("sstabledump")
        .arg(file_path)
        .output()?;
    
    // Parse JSON
    let rows: Vec<SstabledumpRow> = serde_json::from_slice(&output.stdout)?;
    
    // Convert to typed values using schema
    rows.into_iter()
        .map(|row| convert_with_schema(row, schema))
        .collect()
}
```

### If Choosing Option A

1. **Create Issue #160**: "Research V5CompressedLegacy format"
2. **Assign to senior developer** (binary format expertise needed)
3. **Timeline**: 1-2 days
4. **M2 status**: Delayed ⚠️

---

## Bottom Line for Patrick

**Issue #158**: ✅ Excellent code, properly closed

**Issue #159**: ⚠️ Discovered format unknown - Need strategic decision

**Options**:
- **Fast M2**: sstabledump bypass (2-4h) ← Recommended
- **Proper fix**: Format research (8-16h) ← For M3
- **Big refactor**: SchemaAwareReader (12-20h) ← Post-M2

**My vote**: **sstabledump for M2, proper parser for M3**

**Why**: Gets M2 shipping, buys time for research, proven to work

---

**Next Action**: Decide on approach and update Issue #159 scope

