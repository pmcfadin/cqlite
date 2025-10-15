# Issue #159: Critical Format Analysis

## The Real Problem

**Neither parser works** because we don't know the actual V5CompressedLegacy format!

### What We Know

1. ✅ Block decompresses correctly (15867 → 16384 bytes)
2. ✅ Routing works (partition parser called)
3. ✅ Schema loaded (19 columns)
4. ❌ State machine extracts 0 cells

### What This Means

**V5CompressedLegacy decompressed blocks** are NOT:
- ❌ Simple entries (legacy parser tried this, failed)
- ❌ VInt-encoded partitions (partition parser tried this, got 0 cells)

**They must be**: Some other Cassandra 5.0 serialization format we haven't identified

## Root Cause

**We've been guessing at the format!**

Need to:
1. Hex dump decompressed block data
2. Compare with Cassandra 5.0 source code
3. Understand actual serialization format
4. Implement correct parser

## Recommendation

**Option A: Format Research** (8-12 hours)
- Dump decompressed block hex
- Research Cassandra 5.0 serialization format
- Implement format-specific parser

**Option B: Use sstabledump** (2-3 hours)
- Parse sstabledump JSON output
- Bypass binary parsing for M2
- Defer binary parser to M3

**Option C: Revert + Block M2** 
- Admit we can't parse V5 SSTables yet
- Focus on 3.x/4.x support for M2

I recommend **Option A** for proper fix or **Option B** for quick M2 unblock.
