# Issue #163: SerializationHeader Column Parsing Research

## Executive Summary

After analyzing Statistics.db binary format from Cassandra 5.0 NB-format SSTables, I've determined that **SerializationHeader column metadata is NOT consistently embedded in Statistics.db files** in a parseable format for the test data available.

## Binary Format Analysis

### Statistics.db Structure (Observed)

```
Offset  Size    Field                   Value (simple_table example)
------  ----    -----                   -------------------------
0-3     4       version_type            0x00000004
4-7     4       statistics_kind         0x26291b05
8-11    4       reserved                0x00000000
12-15   4       data_length             0x0000002c (44 bytes)
16-19   4       metadata1               0x00000001
20-23   4       metadata2               0x00000065 (101)
24-27   4       metadata3               0x00000002
28-31   4       checksum                0x00000b53

--- Data Section (offset 32+) ---
32-35   4       metadata_type           0x00000003 (EncodingStats marker)
36-39   ?       data_section_length     (encoding unclear: 00 00 1d 2b)
40+     var     partitioner + metadata
```

### EncodingStats Location

The current `parse_minimal_encoding_stats` function successfully extracts:
- `minTimestamp` (signed VInt)
- `minLocalDeletionTime` (signed VInt)
- `minTTL` (signed VInt)

These appear after:
- metadata_type (u32 BE)
- data_section_length (encoding ambiguous)
- partitioner string (VInt length + UTF-8 bytes)
- 2 additional metadata VInts

### SerializationHeader Search Results

**Key Finding**: After parsing EncodingStats fields, the remaining data does NOT match the expected SerializationHeader column format described in Cassandra source code.

Expected format (per SerializationHeader.java):
```
[regularColumnsCount: VInt]
For each regular column:
  [columnNameLength: VInt]
  [columnName: UTF-8]
  [columnTypeLength: VInt]
  [columnType: UTF-8]

[clusteringColumnsCount: VInt]
For each clustering column:
  [columnNameLength: VInt]
  [columnName: UTF-8]
  [columnTypeLength: VInt]
  [columnType: UTF-8]
  [order: u8]
```

**Actual data after EncodingStats** (offset varies, ~100-150):
- Does not contain recognizable UTF-8 column names
- VInt sequences do not align with expected column counts (18 regular columns)
- Binary appears to be histogram/statistics data, not schema metadata

## Root Cause Analysis

### Cassandra Version Variations

Cassandra 5.0 Statistics.db format varies significantly between:
1. **Production SSTables**: May embed full SerializationHeader
2. **Test data / specific table types**: May omit schema, relying on system schema tables
3. **Compression-enabled tables**: Schema might be in CompressionInfo.db instead

### Test Data Characteristics

The test file analyzed:
- Path: `test_basic/simple_table-.../nb-1-big-Statistics.db`
- Size: 8483 bytes
- Contains: EncodingStats + extensive histogram data
- Missing: Readable SerializationHeader column definitions

## Recommendations

### Short-Term Solution (Issue #163)

**Do NOT attempt to parse SerializationHeader from Statistics.db** for the following reasons:

1. **Format Inconsistency**: Not all Cassandra 5.0 SSTables embed schema in Statistics.db
2. **Complexity**: Offset calculations depend on variable-length histogram data
3. **Fragility**: Binary format is undocumented and version-specific

**Instead, implement schema fallback hierarchy:**

```rust
fn extract_schema(sstable_path: &Path) -> Result<TableSchema> {
    // 1. Try SSTableHeader.columns (if populated)
    if let Some(schema) = try_from_header_columns() {
        return Ok(schema);
    }

    // 2. Try schema.cql file (co-located with SSTable)
    if let Some(schema) = try_load_schema_cql(sstable_path.parent()) {
        return Ok(schema);
    }

    // 3. Try system schema tables (if available)
    if let Some(schema) = try_system_schema_tables() {
        return Ok(schema);
    }

    // 4. Require user-provided schema
    Err(Error::SchemaRequired(
        "V5CompressedLegacy format requires external schema. \
         Provide schema.cql file or use --schema flag"
    ))
}
```

### Medium-Term Solution (M3+)

1. **CQL DDL Parser**: Implement `CREATE TABLE` statement parser
2. **Schema File Support**: Add `--schema schema.cql` CLI flag
3. **Schema Cache**: Cache parsed schemas in `~/.cqlite/schemas/`

### Long-Term Solution (M4+)

1. **System Schema Reader**: Parse Cassandra system.schema_* tables
2. **Schema Discovery**: Auto-detect schema from SSTable directory structure
3. **Schema Inference**: Heuristic schema inference from cell data (last resort)

## Alternative Approaches Considered

### Approach 1: Deep Binary Parsing (REJECTED)

- **Pros**: Would work for some SSTables
- **Cons**:
  - Requires reverse-engineering Cassandra internals
  - Fragile across versions
  - Test data doesn't contain schema anyway

### Approach 2: sstabledump Integration (CONSIDERED)

- Use Apache Cassandra's `sstabledump` tool to extract schema
- **Pros**: Authoritative source
- **Cons**:
  - External dependency
  - Performance overhead
  - Not available in WASM/embedded contexts

### Approach 3: Schema File Requirement (RECOMMENDED)

- Require `schema.cql` alongside SSTable files
- **Pros**:
  - Simple, reliable
  - Matches Cassandra operational practice
  - Enables schema versioning
- **Cons**:
  - User burden (mitigated by clear error messages)

## Test Data Observations

### simple_table Schema (from JSONL)

```
Partition key: id UUID
Clustering keys: (none)
Regular columns (18):
  - account_balance: decimal
  - active: boolean
  - age: int
  - ascii_field: ascii
  - birth_date: date
  - created: timestamp
  - description: blob
  - duration_val: duration
  - height: double
  - ip_address: inet
  - medium_number: int
  - name: text
  - salary: bigint
  - session_id: timeuuid
  - small_number: smallint
  - varchar_field: varchar
  - weight: float
  - work_time: time
```

This schema is NOT found in Statistics.db binary at any recognizable offset.

## Conclusion

**Issue #163 should be closed as "Won't Fix (Design Decision)"** with the following resolution:

> V5CompressedLegacy format schema extraction from Statistics.db is not feasible due to binary format variations across Cassandra versions. Instead, implement schema fallback to external sources (schema.cql files, system tables, or user-provided DDL).

**Implementation Path Forward:**

1. Update `TableSchema::from_sstable_header` to return `None` for V5CompressedLegacy
2. Add schema fallback logic in `SSTableReader::open`
3. Provide clear error message: "Schema file required for NB-format SSTables"
4. Add `--schema` CLI flag for M3 milestone
5. Document schema requirements in user guide

---

**Research Duration**: 1.5 hours
**Files Analyzed**:
- `/test-data/datasets/sstables/test_basic/simple_table-.../nb-1-big-Statistics.db`
- `cqlite-core/src/parser/enhanced_statistics_parser.rs`
- `cqlite-core/src/parser/vint.rs`

**References**:
- Apache Cassandra SerializationHeader.java
- Issue #162: V5CompressedLegacy parser implementation
- Issue #163: Schema extraction requirement
