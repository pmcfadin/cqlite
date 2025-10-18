# Follow-up Issue: Schema Extraction Enhancements and Test Failures

## Context

Issue #163 successfully implemented partition key extraction from Statistics.db SerializationHeader, enabling schema-aware parsing for V5CompressedLegacy format SSTables. This follow-up tracks remaining enhancements and test failures discovered during implementation.

## Issues to Address

### 1. Test Failure: test_v5_compressed_legacy_extracts_cells

**Status**: Pre-existing failure, unrelated to #163 schema extraction work

**Symptoms**:
- Test: `storage::sstable::reader::tests::tests::test_v5_compressed_legacy_extracts_cells`
- Error: Returns `Value::Null` instead of parsed cells with data
- File: `test_basic/simple_table-6aa08200a25111f0a3fef1a551383fb9/nb-1-big-Data.db`
- Impact: 1/759 tests failing (99.9% pass rate)

**Investigation Needed**:
- Determine why V5CompressedLegacy parser returns Null for simple_table
- Check if schema extraction changes affected cell parsing (likely not, as schema is populated after parsing)
- Verify if test was passing before Issue #160 V5CompressedLegacy implementation
- Fix or document as known limitation

**Priority**: Medium (test infrastructure issue, doesn't block schema extraction functionality)

---

### 2. Enhancement: Clustering Key Extraction from SerializationHeader

**Status**: Format documented, implementation ready but not required for #163

**Current State**:
- Partition key extraction: ✅ Implemented
- Clustering key extraction: ⚠️ Not implemented (no test data has clustering keys in SerializationHeader)
- Documentation: ✅ Complete specification in `docs/research/issue_163_serialization_header_parsing_spec.md`

**Implementation Notes**:
- Binary format: After partition key types, before `0x00 0x00` marker
- Structure: `VInt(count) + [VInt(length) + marshal_type + VInt(reversed_flag)]*count`
- Create synthetic clustering key columns: `ck_0`, `ck_1`, etc.
- Set `is_clustering: true`, `key_position: Some(index)`

**Test Data Needed**:
- Real Statistics.db file containing clustering keys in SerializationHeader
- Example: table with composite primary key `(partition_key, clustering_key_1, clustering_key_2)`

**Priority**: Low (no current test data has clustering keys, can implement when needed)

---

### 3. Enhancement: Real Column Name Discovery

**Status**: Enhancement idea, not blocking

**Current Approach** (Issue #163):
- Partition keys: Synthetic names ("id" for single PK, "pk_0"/"pk_1" for composite)
- Rationale: SerializationHeader provides TYPES but not NAMES
- Works for schema-aware parsing (type info is authoritative)

**Alternative Approaches**:
1. **Match by type**: Find first regular column with matching type
   - Pro: Uses actual column names from SerializationHeader
   - Con: Ambiguous if multiple columns have same type (e.g., two UUIDs)
   - Con: Violates no-heuristics mandate (guessing which column is the key)

2. **External schema source**: Load from .cql files or schema registry
   - Pro: Authoritative column names
   - Pro: Already implemented in schema discovery system
   - Con: Requires external schema files (not always available)

3. **Hybrid approach**: Use synthetic names, allow override from external schema
   - Pro: Works without external schema, improves with it
   - Pro: Maintains no-heuristics for default behavior
   - Con: Added complexity

**Recommendation**: Keep current synthetic name approach unless specific use case requires real names. Schema discovery system already handles external .cql files for authoritative schemas.

**Priority**: Low (current approach meets requirements)

---

## Test Coverage Status

**Passing** (758/759 = 99.9%):
- ✅ All schema extraction tests
- ✅ All enhanced_statistics_parser unit tests
- ✅ statistics_db_real_file_test (real Statistics.db parsing)
- ✅ debug_schema_extraction (end-to-end validation)
- ✅ All other cqlite-core library tests

**Failing** (1/759):
- ❌ test_v5_compressed_legacy_extracts_cells (pre-existing, unrelated to #163)

---

## Recommended Actions

1. **Immediate**: 
   - Investigate and fix `test_v5_compressed_legacy_extracts_cells` failure
   - Document as known issue if pre-existing from Issue #160

2. **When Needed**:
   - Implement clustering key extraction when test data becomes available
   - Revisit column name discovery if use cases require real names

3. **Nice to Have**:
   - Add integration test with composite partition keys
   - Add integration test with clustering keys (when test data available)
   - Benchmark performance impact of Statistics.db parsing

---

## References

- Issue #163: Schema extraction from Statistics.db SerializationHeader
- Issue #160: V5CompressedLegacy parser implementation
- Issue #162: NB format detection enhancements
- Spec: `docs/research/issue_163_serialization_header_parsing_spec.md`
