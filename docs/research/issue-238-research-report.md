# Issue #238 Research Report: UDTs Inside Collections Test Data and Validation

**Date**: 2026-01-05  
**Issue**: [#238 - UDTs inside collections are not parsed - displayed as empty blob (0x)](https://github.com/pmcfadin/cqlite/issues/238)  
**Status**: OPEN (Bug)  
**Test Table**: `test_collections.collections_with_udts`

---

## Executive Summary

Issue #238 is a **critical bug** where User-Defined Types (UDTs) nested inside collections (List, Set, Map) are incorrectly rendered as empty blobs (`0x`) instead of their actual field values. The table `collections_with_udts` currently **PASSES** smoke tests (reported in validation-matrix.md), but UDT values are not being properly deserialized within collections.

**Root Cause**: The `parse_value_with_comparator()` function (lines 173-198 in `value_parsing.rs`) has a minimal implementation that falls back to Blob for all complex types including UDTs. Collection parsers call this function instead of the full `parse_value_with_schema_type()`.

---

## 1. Schema Definition

**Location**: `/Users/patrick/local_projects/cqlite/test-data/schemas/collections.cql`

### UDT Type Definitions (Lines 47-59)

```sql
-- address_type UDT (5 fields)
CREATE TYPE IF NOT EXISTS address_type (
    street TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    country TEXT
);

-- contact_info UDT (3 fields, nested address)
CREATE TYPE IF NOT EXISTS contact_info (
    email TEXT,
    phone TEXT,
    address FROZEN<address_type>  -- Nested UDT!
);
```

### Table Schema (Lines 61-68)

```sql
CREATE TABLE IF NOT EXISTS collections_with_udts (
    user_id UUID PRIMARY KEY,
    addresses LIST<FROZEN<address_type>>,           -- List of UDTs
    contacts SET<FROZEN<contact_info>>,             -- Set of UDTs (nested)
    locations_visited MAP<DATE, FROZEN<address_type>>, -- Map with UDT values
    emergency_contacts MAP<TEXT, FROZEN<contact_info>> -- Map with nested UDTs
) WITH compression = {'class': 'LZ4Compressor'}
  AND compaction = {'class': 'LeveledCompactionStrategy'};
```

**Key Characteristics**:
- **2 UDT types**: `address_type` (simple), `contact_info` (nested - contains `address_type`)
- **4 collection columns** using UDTs:
  1. `addresses`: List of address UDTs
  2. `contacts`: Set of contact_info UDTs (nested)
  3. `locations_visited`: Map with DATE keys, address UDT values
  4. `emergency_contacts`: Map with TEXT keys, contact_info UDT values
- **Compression**: LZ4
- **Compaction**: Leveled

---

## 2. SSTable Files

**Directory**: `/Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_collections/collections_with_udts-6bc2bae0a25111f0a3fef1a551383fb9/`

### Component Files

| Component | Size | Purpose |
|-----------|------|---------|
| `nb-1-big-Data.db` | 24K | Partition data (50 partitions) |
| `nb-1-big-Index.db` | 1.0K | BTI partition index |
| `nb-1-big-Summary.db` | 92B | Index summary |
| `nb-1-big-CompressionInfo.db` | 63B | LZ4 compression metadata |
| `nb-1-big-Statistics.db` | 7.1K | SSTable statistics |
| `nb-1-big-Filter.db` | 48B | Bloom filter |
| `nb-1-big-Digest.crc32` | 10B | CRC32 digest |
| `nb-1-big-TOC.txt` | 92B | Table of contents |

**Format**: V5CompressedLegacy (`nb-1-big` prefix = NB format)

---

## 3. Statistics.db Metadata

**Source**: `/Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_collections/collections_with_udts-6bc2bae0a25111f0a3fef1a551383fb9/nb-1-big-Statistics.db.txt`

### Table Statistics
- **Total Rows**: 50
- **Total Columns**: 200
- **Partitions**: 50 (1 row per partition)
- **Partition Size**: 770 bytes (median/mode)
- **Columns per Row**: 6 (4 collection columns + metadata)
- **Compression Ratio**: 0.704 (LZ4)

### Type Information (Lines 62-65)

**KeyType**: `org.apache.cassandra.db.marshal.UUIDType`  
**ClusteringTypes**: `[]` (no clustering keys)  
**StaticColumns**: (none)  
**RegularColumns**: 

1. **emergency_contacts**:  
   `MapType(UTF8Type, FrozenType(UserType(test_collections, 636f6e746163745f696e666f, ...)))`
   - Hex keyspace: `test_collections`
   - Hex type name: `636f6e746163745f696e666f` = "contact_info"
   - Fields: email (UTF8), phone (UTF8), address (nested UserType)

2. **addresses**:  
   `ListType(FrozenType(UserType(test_collections, 616464726573735f74797065, ...)))`
   - Hex type name: `616464726573735f74797065` = "address_type"
   - Fields: street, city, state, zip_code, country (all UTF8)

3. **locations_visited**:  
   `MapType(SimpleDateType, FrozenType(UserType(test_collections, 616464726573735f74797065, ...)))`
   - Key: SimpleDateType
   - Value: address_type UDT

4. **contacts**:  
   `SetType(FrozenType(UserType(test_collections, 636f6e746163745f696e666f, ...)))`
   - Element: contact_info UDT (nested)

---

## 4. JSONL Reference Data

**File**: `/Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_collections/collections_with_udts-6bc2bae0a25111f0a3fef1a551383fb9/nb-1-big-Data.db.jsonl`

**Line Count**: 49 (one partition per line)

### Sample Partition (First Entry)

```json
{
  "table kind": "REGULAR",
  "partition": {
    "key": ["e94f10e8-6d74-4da3-ae2f-e3d92cf68976"],
    "position": 30
  },
  "rows": [
    {
      "type": "row",
      "position": 30,
      "liveness_info": {"tstamp": "2025-10-06T01:12:07.375638Z"},
      "cells": [
        {"name": "addresses", "deletion_info": {...}},
        {
          "name": "addresses",
          "path": ["7a24ad00-a251-11f0-a3fe-f1a551383fb9"],
          "value": {
            "street": "07372 Mary Shoals Suite 758",
            "city": "Alyssafurt",
            "state": "IL",
            "zip_code": "79107",
            "country": "British Indian Ocean Territory (Chagos Archipelago)"
          }
        },
        {
          "name": "addresses",
          "path": ["7a24ad0a-a251-11f0-a3fe-f1a551383fb9"],
          "value": {
            "street": "13898 Adam Port Suite 788",
            "city": "East Veronica",
            "state": "IL",
            "zip_code": "30919",
            "country": "Philippines"
          }
        },
        {"name": "contacts", "deletion_info": {...}},
        {
          "name": "contacts",
          "path": ["alyssa23\\@example.com:(223)342-2641:423 Michael View Suite 577\\:Smithfurt\\:CT\\:83376\\:Northern Mariana Islands"],
          "value": ""
        },
        {
          "name": "contacts",
          "path": ["michaelmartinez\\@example.com:542.210.8439:169 Green Meadows\\:Port Stephaniefurt\\:TN\\:96351\\:Moldova"],
          "value": ""
        },
        {"name": "emergency_contacts", "deletion_info": {...}},
        {
          "name": "emergency_contacts",
          "path": ["Mark"],
          "value": {
            "email": "tgray@example.com",
            "phone": "001-926-959-0282",
            "address": {
              "street": "9482 Klein Unions Suite 763",
              "city": "Kennethmouth",
              "state": "TN",
              "zip_code": "73703",
              "country": "Chile"
            }
          }
        },
        {"name": "locations_visited", "deletion_info": {...}},
        {
          "name": "locations_visited",
          "path": ["2025-10-05"],
          "value": {
            "street": "8511 Serrano Manors Suite 124",
            "city": "Fryefurt",
            "state": "PW",
            "zip_code": "84668",
            "country": "Micronesia"
          }
        }
      ]
    }
  ]
}
```

### Data Patterns Observed

1. **addresses** (List): 2 elements per partition, each is a full address_type UDT
2. **contacts** (Set): 2 elements per partition, stored as path with empty value (frozen set encoding)
3. **emergency_contacts** (Map): 1 entry per partition, TEXT key → contact_info UDT (with nested address)
4. **locations_visited** (Map): 1 entry per partition, DATE key → address_type UDT

**Important**: The JSONL data shows full UDT structures are present in the raw data. CQLite is reading the bytes but not deserializing them correctly.

---

## 5. Validation Matrix Status

**Source**: `/Users/patrick/local_projects/cqlite/test-data/validation-matrix.md` (Line 72)

| Table | Rows | Load | Parse | Count | Int Test | Status | Notes |
|-------|------|------|-------|-------|----------|--------|-------|
| collections_with_udts | 49 | ✅ | ✅ | ✅ | ⚠️ (1 test) | **PASS** | Fixed by Issue #220 (UDT support) |

**Current Status**: 
- ✅ **Load**: SSTable opens successfully
- ✅ **Parse**: No crashes, data loads
- ✅ **Count**: 49 partitions match expected count
- ⚠️ **Int Test**: 1 integration test (`issue_154_test.rs`)
- **Status**: PASS (but UDT values not properly displayed)

**Issue #220**: UDT support was marked as "fixed" in December 2025, which allowed the table to pass basic validation. However, **nested UDTs in collections** remain broken.

---

## 6. Existing Tests

### Integration Test: `issue_154_test.rs`

**Location**: `/Users/patrick/local_projects/cqlite/cqlite-core/tests/issue_154_test.rs`

**Purpose**: Basic smoke test to verify the SSTable can be opened without "Verify error"

```rust
#[tokio::test]
async fn test_collections_with_udts_can_open() {
    let data_file_path = test_root.join(
        "sstables/test_collections/collections_with_udts-6bc2bae0a25111f0a3fef1a551383fb9/nb-1-big-Data.db"
    );

    // Try to open the SSTable - this should NOT fail with "Verify error"
    let result = cqlite_core::storage::sstable::reader::SSTableReader::open(
        &data_file_path,
        &config,
        platform,
    ).await;

    result.expect("collections_with_udts table should open successfully (Issue #154)");
}
```

**Coverage**: Only tests that the file opens, does NOT validate UDT parsing

### Unit Tests: `udt_tests.rs`

**Location**: `/Users/patrick/local_projects/cqlite/cqlite-core/src/parser/udt_tests.rs`

**Coverage**: Extensive UDT parsing tests including:
- Basic UDT parsing (line 210)
- UDT with registry lookup (line 233)
- Nested UDT parsing (line 252)
- UDT with collections (line 283)
- Frozen UDT parsing (line 301)
- List with UDT elements (line 360)
- Map with UDT values (line 398)

**Note**: Many tests are marked `#[ignore]` because helper methods are not implemented.

### Unit Tests: `collection_udt_tests.rs`

**Location**: `/Users/patrick/local_projects/cqlite/cqlite-core/src/parser/collection_udt_tests.rs`

**Coverage**: Tests for collections containing UDTs:
- Address UDT parsing (line 124, ignored)
- Person UDT with nested address (line 192, ignored)
- UDT null field handling (line 329, ignored)

**Status**: Most tests are disabled/ignored, indicating incomplete implementation.

---

## 7. Root Cause Analysis

### The Bug: Two-Tiered Value Parsing

**File**: `/Users/patrick/local_projects/cqlite/cqlite-core/src/storage/sstable/reader/parsing/value_parsing.rs`

#### Function 1: `parse_value_with_schema_type()` (Lines 66-169)

**Purpose**: Full value parsing with complete type support  
**Supports**:
- ✅ Primitives (Boolean, Int, BigInt, Text, UUID, etc.)
- ✅ Collections (List, Set, Map)
- ✅ Tuples
- ✅ **UDTs** (line 157-159: calls `parse_udt_value()`)
- ✅ Frozen types

**Used for**: Top-level column values (direct table columns)

#### Function 2: `parse_value_with_comparator()` (Lines 173-198)

**Purpose**: Minimal value parsing for recursion  
**Supports**:
- ✅ Boolean (line 180-185)
- ✅ Text (line 187-190)
- ✅ Blob (line 192)
- ❌ **EVERYTHING ELSE** → Falls back to Blob (line 193-196)

```rust
_ => {
    // For complex types, implement as needed
    Ok(Value::Blob(value_data.to_vec()))
}
```

**Used for**: Element/value parsing inside collections (recursive calls)

### The Problem Flow

1. **Top-level column** (e.g., `addresses LIST<FROZEN<address_type>>`)
   - Calls `parse_value_with_schema_type()` (line 66)
   - Matches `ComparatorType::List(element_comparator)` (line 145)
   - Calls `parse_list_value(value_data, element_comparator)` (line 146)

2. **Inside list parser** (`parse_list_value()`, line 201)
   - Loops through list elements (line 217)
   - Parses element data (line 234)
   - **Calls `parse_value_with_comparator(element_data, element_comparator)`** (line 236) ❌
   - `element_comparator` is `Frozen(Udt {...})`
   - `parse_value_with_comparator()` doesn't handle UDTs → falls back to Blob
   - Returns `Value::Blob([...])` instead of `Value::Udt(...)`

3. **Result**: UDT bytes stored as opaque Blob, displayed as `0x`

### Similar Issues

- **Map parser** (line 305): Calls `parse_value_with_comparator()` for values
- **Set parser** (line 251): Delegates to list parser (same issue)
- **Tuple parser** (line 346): Calls `parse_value_with_comparator()` for fields

---

## 8. Impact Assessment

### Broken Functionality
- ❌ UDTs inside Lists
- ❌ UDTs inside Sets
- ❌ UDTs as Map values (or keys)
- ❌ Nested UDTs (UDT field containing another UDT)
- ❌ Frozen UDTs within collections

### Working Functionality
- ✅ Top-level UDT columns (direct table column)
- ✅ Simple collections (List<Int>, Set<Text>, Map<Text, Int>)
- ✅ SSTable loading and basic parsing
- ✅ Partition counting

### Test Tables Affected

**From validation-matrix.md**:
- `test_collections.collections_with_udts` (49 rows) - **Primary target**
- Potentially other tables with nested UDTs (not currently in test suite)

---

## 9. Suggested Fix Strategy

### Option 1: Expand `parse_value_with_comparator()` (Recommended)

**Approach**: Add full type support to the recursive helper function

**Changes Required**:
1. Add UDT parsing branch (lines 193-196)
2. Add List/Set/Map recursive calls
3. Add Frozen type handling
4. Add Tuple parsing

**Pros**:
- Single unified parsing function
- Consistent behavior across all contexts
- Minimal refactoring

**Cons**:
- Increases function complexity
- May duplicate logic from `parse_value_with_schema_type()`

### Option 2: Unify Parsing Functions

**Approach**: Refactor to use `parse_value_with_schema_type()` everywhere

**Changes Required**:
1. Convert `ComparatorType` → `data_type` string in collection parsers
2. Update all recursive calls to use `parse_value_with_schema_type()`
3. Remove or deprecate `parse_value_with_comparator()`

**Pros**:
- Eliminates code duplication
- Guarantees consistent parsing
- Simpler to maintain

**Cons**:
- More invasive refactoring
- Type conversion overhead

### Option 3: Recursive Delegation

**Approach**: Make `parse_value_with_comparator()` delegate to full parser

**Changes Required**:
1. Add `match` branch for `ComparatorType::Udt`
2. Delegate to `parse_udt_value()`
3. Add similar delegations for List/Set/Map

**Pros**:
- Minimal code changes
- Preserves existing structure
- Low risk

**Cons**:
- Adds indirection layer
- May have performance impact

---

## 10. Validation Plan

### Test Data Available
✅ Schema: `test-data/schemas/collections.cql`  
✅ SSTable files: 8 components (Data.db, Index.db, etc.)  
✅ Reference JSONL: 49 partitions with full UDT structures  
✅ Statistics.db: Type metadata for validation

### Suggested Tests

1. **Unit Test**: Parse single UDT from collection element
   ```rust
   #[test]
   fn test_parse_udt_in_list_element() {
       // Parse addresses[0] -> address_type UDT
       // Verify: street, city, state, zip_code, country fields
   }
   ```

2. **Integration Test**: Full table scan with UDT validation
   ```rust
   #[test]
   async fn test_collections_with_udts_full_scan() {
       // Load all 49 partitions
       // For each partition:
       //   - Verify addresses[0].city is non-empty string
       //   - Verify emergency_contacts["Mark"].email exists
       //   - Verify nested address in contact_info
   }
   ```

3. **JSONL Parity Test**: Compare against sstabledump output
   ```rust
   #[test]
   async fn test_collections_with_udts_jsonl_parity() {
       // Load JSONL reference file
       // For each partition:
       //   - Parse CQLite output
       //   - Compare UDT field values
       //   - Assert exact match
   }
   ```

4. **Regression Test**: Ensure simple collections still work
   ```rust
   #[test]
   fn test_simple_collections_unaffected() {
       // Test List<Int>, Set<Text>, Map<Text, Int>
       // Verify no regression from UDT fixes
   }
   ```

---

## 11. Related Issues and History

### Issue #220: UDT Support (Resolved)
- **Fixed**: December 2025
- **Scope**: Basic UDT parsing for top-level columns
- **Limitation**: Did not address UDTs inside collections

### Issue #154: collections_with_udts Header Parsing Failure
- **Test File**: `issue_154_test.rs`
- **Scope**: SSTable opening without "Verify error"
- **Limitation**: Only tests file loading, not value parsing

### Issue #221: Complex Cell Flag Handling
- **Fixed**: December 2025
- **Impact**: `typed_collections_table`, `frozen_collections_table` now pass
- **Related**: Fixed collection parsing infrastructure

---

## 12. Key File Paths

All paths are absolute from project root:

**Schema**:
- `/Users/patrick/local_projects/cqlite/test-data/schemas/collections.cql`

**SSTable Directory**:
- `/Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_collections/collections_with_udts-6bc2bae0a25111f0a3fef1a551383fb9/`

**SSTable Components**:
- `nb-1-big-Data.db` (24K)
- `nb-1-big-Index.db` (1.0K)
- `nb-1-big-Statistics.db` (7.1K)
- `nb-1-big-CompressionInfo.db` (63B)
- `nb-1-big-Summary.db` (92B)

**Reference Files**:
- `nb-1-big-Data.db.jsonl` (92K, 49 lines)
- `nb-1-big-Statistics.db.txt` (4.7K)

**Source Code**:
- `/Users/patrick/local_projects/cqlite/cqlite-core/src/storage/sstable/reader/parsing/value_parsing.rs` (bug location)
- `/Users/patrick/local_projects/cqlite/cqlite-core/src/parser/udt_tests.rs` (UDT test suite)
- `/Users/patrick/local_projects/cqlite/cqlite-core/src/parser/collection_udt_tests.rs` (collection UDT tests)

**Integration Tests**:
- `/Users/patrick/local_projects/cqlite/cqlite-core/tests/issue_154_test.rs` (current test)

**Validation**:
- `/Users/patrick/local_projects/cqlite/test-data/validation-matrix.md` (status tracking)

---

## 13. Summary and Recommendations

### Current State
- ✅ SSTable loads successfully (passes smoke test)
- ✅ Basic UDT parsing works for top-level columns
- ❌ UDTs inside collections are broken (rendered as `0x`)
- ❌ Nested UDTs (UDT within UDT) likely broken

### Immediate Next Steps

1. **Implement Fix** (Option 1 recommended):
   - Expand `parse_value_with_comparator()` to handle UDT types
   - Add recursive support for Frozen, List, Set, Map
   - Test against `collections_with_udts` table

2. **Add Comprehensive Tests**:
   - Unit test for UDT element parsing
   - Integration test for full table scan
   - JSONL parity test for exact match validation

3. **Update Validation Matrix**:
   - Re-run smoke tests after fix
   - Update `collections_with_udts` status
   - Add note about UDT nesting support

4. **Document Limitations**:
   - Update Appendix F in definitive guide
   - Note any remaining UDT edge cases
   - Add to known limitations list

### Long-Term Improvements

- Unify value parsing architecture (eliminate dual-function pattern)
- Add more UDT test tables to test suite
- Implement serialization functions for roundtrip testing
- Add performance benchmarks for UDT parsing

---

**End of Report**
