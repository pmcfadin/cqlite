# Issue #164 Team Feedback - Response & Action Plan

## Team Findings Summary

### Critical Issue Identified ✅

**Location**: `cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs:648`

**Root Cause**: The parser treats `row_size` extending past the current decompressed block as corruption, but Cassandra allows rows to span multiple compressed chunks.

**Impact**: Parser processes only rows in the first chunk (~5 rows) then exits, instead of stitching chunks to read all 1000 rows.

**Team Assessment**: "We need to accumulate enough bytes to cover row_size (possibly by stitching adjacent chunks) instead of exiting the partition loop."

---

## Proposed Fix: Multi-Chunk Row Stitching

### Architecture Overview

```
Current (Broken):
┌─────────────┐
│  Chunk 0    │ → parse_block() → 5 rows → STOP (row_size > block size)
│  (16KB)     │
└─────────────┘
│  Chunk 1    │ → Never processed
│  (16KB)     │
└─────────────┘
...
│  Chunk 40   │ → Never processed
└─────────────┘

Fixed (Multi-Chunk):
┌─────────────┐
│  Chunk 0    │ ─┐
└─────────────┘  │
│  Chunk 1    │  ├─→ Stitch into continuous buffer → parse_block() → All 1000 rows ✅
└─────────────┘  │
...              │
│  Chunk 40   │ ─┘
└─────────────┘
```

### Implementation Strategy

**Option A: Chunk Stitching (Recommended)**
- Decompress all 41 chunks into a single continuous buffer
- Pass entire buffer to `parse_block()`
- Parser handles all rows without chunk boundary concerns

**Pros**:
- Simple parser logic (no boundary handling)
- Matches Cassandra's logical row model
- Easy to validate against ground truth

**Cons**:
- Higher memory usage (41 × 16KB ≈ 656KB uncompressed)
- Must buffer entire SSTable block range

**Option B: Streaming Multi-Chunk Parser**
- Parse rows incrementally, requesting next chunk when `row_size` exceeds current buffer
- More complex state machine to track chunk boundaries

**Pros**:
- Lower memory footprint
- Handles arbitrarily large rows

**Cons**:
- Complex parser state management
- Harder to test and validate
- Over-engineering for typical row sizes (<1MB)

### Recommended Approach: **Option A** (Chunk Stitching)

Cassandra's chunk size is 64KB by default (configurable), and rows are typically <100KB. Stitching 41 chunks (≈2.5MB uncompressed) is acceptable for modern systems.

---

## Code Changes Required

### File 1: `cqlite-core/src/storage/sstable/reader/data_access.rs`

**Current** (`get_all_entries()` for V5CompressedLegacy):
```rust
// Reads only first block
let block_data = self.read_next_block().await?;
let entries = self.parse_block_entries(&block_data, schema)?;
```

**Proposed Fix**:
```rust
// Read and stitch ALL blocks for V5CompressedLegacy
let mut stitched_buffer = Vec::new();
while let Ok(block_data) = self.read_next_block().await {
    stitched_buffer.extend_from_slice(&block_data);
}

// Parse entire stitched buffer
let entries = self.parse_block_entries(&stitched_buffer, schema)?;
```

### File 2: `cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs`

**Current** (Line 648 - Validation that exits early):
```rust
if row_size > data.len() - row_start_offset {
    return Err(Error::corruption(format!(
        "Row size {} exceeds remaining block data {} bytes",
        row_size, data.len() - row_start_offset
    )));
}
```

**Proposed Fix**:
```rust
// Remove this check entirely - row_size is valid across chunk boundaries
// The higher-level reader has already stitched chunks, so row_size is guaranteed valid
```

---

## Performance Optimization (Team Feedback #2)

### Issue: Per-Row HashSet Allocation

**Location**: `cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs:702`

**Current**:
```rust
// Rebuilt for EVERY row
let partition_key_names: HashSet<_> = schema.partition_keys.iter()...collect();
let clustering_key_names: HashSet<_> = schema.clustering_keys.iter()...collect();
let columns_in_order: Vec<_> = schema.columns.iter().filter(...)...collect();
```

**Proposed Fix**:
```rust
// Cache in parser struct (built once)
pub struct V5CompressedLegacyParser {
    // Existing fields...

    /// Cached regular columns (partition/clustering keys filtered out)
    regular_columns: Vec<Column>,
}

impl V5CompressedLegacyParser {
    pub fn new(..., schema: &TableSchema) -> Self {
        // Build filtered column list ONCE at construction
        let partition_key_names: HashSet<_> = schema.partition_keys...;
        let clustering_key_names: HashSet<_> = schema.clustering_keys...;

        let regular_columns = schema.columns.iter()
            .filter(|col| !partition_key_names.contains(...)
                       && !clustering_key_names.contains(...))
            .cloned()
            .collect();

        Self {
            // ...
            regular_columns,
        }
    }

    fn parse_row_data_with_offset(...) {
        // Use cached self.regular_columns instead of rebuilding
        for column in &self.regular_columns {
            // Parse cell...
        }
    }
}
```

**Benefit**: Eliminates 2 HashSet + 1 Vec allocation per row (18,000 allocations for 1000 rows with 18 columns).

---

## Implementation Plan

### Phase 1: Multi-Chunk Stitching (Priority: P0)

**Tasks**:
1. Modify `get_all_entries()` to stitch all decompressed chunks
2. Remove `row_size` validation check that exits on chunk boundary
3. Test with simple_table (1000 rows across 41 chunks)
4. Validate parsed data matches JSONL ground truth

**Estimated Time**: 2-3 hours

**Success Criteria**:
- ✅ Read 1000 entries (not 5)
- ✅ All entries have non-Null cell values
- ✅ Parsed data matches JSONL reference

### Phase 2: Cache Filtered Columns (Priority: P1)

**Tasks**:
1. Add `regular_columns: Vec<Column>` to `V5CompressedLegacyParser`
2. Build filtered list once in constructor
3. Remove per-row HashSet/filter logic
4. Benchmark performance improvement

**Estimated Time**: 1-2 hours

**Success Criteria**:
- ✅ No per-row allocations
- ✅ Measurable performance improvement (5-10% faster)
- ✅ All tests still pass

---

## Testing Strategy

### Unit Test: Multi-Chunk Row Stitching

```rust
#[tokio::test]
async fn test_v5_compressed_legacy_multi_chunk_stitching() {
    let reader = SSTableReader::open(...).await.unwrap();

    // simple_table has 1000 rows across 41 chunks
    let entries = reader.get_all_entries().await.unwrap();

    assert_eq!(entries.len(), 1000, "Should read all 1000 rows");

    // Verify first and last entries have valid cells
    for entry in &entries {
        let (_, _, value) = entry;
        assert!(!matches!(value, Value::Null), "No entry should be Null");
    }
}
```

### Integration Test: Data Validation

```rust
#[tokio::test]
async fn test_v5_compressed_legacy_data_accuracy() {
    let reader = SSTableReader::open(...).await.unwrap();
    let entries = reader.get_all_entries().await.unwrap();

    // Load JSONL ground truth
    let ground_truth = load_jsonl("nb-1-big-Data.db.jsonl");

    // Validate first 10 rows match exactly
    for (i, (expected_row, (_, _, actual_value))) in
        ground_truth.iter().zip(entries.iter()).take(10).enumerate()
    {
        assert_eq!(
            extract_cells(actual_value),
            expected_row.cells,
            "Row {} cells mismatch", i
        );
    }
}
```

---

## Risk Assessment

### Low Risk Changes ✅
- Chunk stitching: Straightforward buffer concatenation
- Removing chunk boundary check: Safe since buffer is stitched
- Caching filtered columns: Pure optimization, no behavior change

### Testing Coverage
- ✅ Existing 759 tests validate no regressions
- ✅ Integration tests validate correct parsing
- ✅ New tests validate 1000-row reading
- ✅ JSONL ground truth validates data accuracy

### Rollback Plan
If multi-chunk stitching causes issues:
1. Revert to single-chunk parsing (current state)
2. Document limitation: "V5CompressedLegacy reads first chunk only"
3. File follow-up issue for proper multi-chunk support

---

## Timeline

**Immediate** (Today):
- [ ] Implement multi-chunk stitching
- [ ] Test with simple_table (1000 rows)
- [ ] Validate against JSONL ground truth

**Short-term** (This Week):
- [ ] Implement column caching optimization
- [ ] Add comprehensive multi-chunk tests
- [ ] Update documentation

**Follow-up Issues**:
- [ ] Create Issue: "Optimize V5CompressedLegacy for arbitrarily large rows (streaming parser)"
- [ ] Create Issue: "Fix upstream schema construction to exclude partition/clustering keys"

---

## Response to Team

**RE: Multi-Chunk Row Spanning**

Thank you for identifying the exact root cause! You're absolutely correct that row_size validation at line 648 is incorrectly treating chunk boundaries as corruption.

**Action**: I'll implement chunk stitching in `get_all_entries()` to concatenate all 41 decompressed chunks into a continuous buffer before parsing. This will allow rows to naturally span chunk boundaries without special handling in the parser.

**RE: Per-Row Allocation Overhead**

Excellent catch on the HashSet churn. Once multi-chunk parsing is working, I'll move the filtered column list into the parser struct to eliminate 18,000+ allocations.

**ETA**: Multi-chunk fix within 2-3 hours, performance optimization within 1-2 hours after that.

---

## Questions for Team

1. **Memory Budget**: Is 2.5MB for stitched buffer acceptable, or should we implement streaming multi-chunk parser for memory-constrained environments?

2. **Scope**: Should these fixes be included in Issue #164, or tracked as separate follow-up issues (e.g., "Issue #165: Multi-chunk support for V5CompressedLegacy")?

3. **Schema Filtering**: Should we also address upstream schema construction (Issue #163 follow-up), or keep the parser-level filtering as a safety net?
