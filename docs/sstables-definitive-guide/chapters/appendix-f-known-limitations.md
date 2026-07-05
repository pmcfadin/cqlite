# Appendix F — Known Limitations

This appendix documents current capabilities, parsing limitations, validation status, and workarounds in CQLite's SSTable implementation. It serves as a reference to prevent repeated investigation of known issues and provides clear guidance for contributors.

**In this appendix you will learn:**
- M5.1 write support capabilities (NEW)
- Which SSTable formats and table types have parsing issues
- Current validation pass rates across test datasets
- Feature gaps and remaining limitations
- Practical workarounds for common limitations
- Issue tracking references for ongoing fixes

---

## M5.1 Write Support Capabilities

CQLite M5.1 introduces comprehensive SSTable write support with the following capabilities:

### Data.db Writing (V5CompressedLegacy Format)

**Status**: IMPLEMENTED

The `DataWriter` produces valid Cassandra 5.0 BIG format Data.db files with:

- **Partition ordering**: Murmur3 token ordering with collision handling (token, then key bytes)
- **Row format**: V5CompressedLegacy with proper flag handling
- **Delta encoding**: Timestamps, TTL, and local deletion times delta-encoded against Statistics.db baseline
- **Clustering prefixes**: Multi-column clustering keys with state bits (PRESENT/NULL/EMPTY)
- **Cell types**: All primitive CQL types supported (int, bigint, text, timestamp, uuid, etc.)

### CompressionInfo.db Writing

**Status**: BUILDING BLOCKS (test-only)

CQLite's production SSTable writer emits uncompressed Data.db only. Compressed-write infrastructure exists to synthesize fixtures for the read path and is fail-closed for production — see #1406.

The `CompressedDataWriter` and `CompressionInfoWriter` types are UNWIRED building blocks: no path from flush or compaction reaches them, and no Cassandra-side byte-parity coverage exists for a CQLite-emitted CompressionInfo.db. Any attempt to configure compressed production writing returns `Error::UnsupportedFormat`:

- `SSTableWriter::with_compression` and `CompressionInfoWriter::guard_unsupported_production_write` accept only `CompressionAlgorithm::None`; every real algorithm (LZ4, Snappy, Deflate, Zstd) errors.

These building blocks are used solely to synthesize compressed SSTables for exercising the decompressing reader. The CompressionInfo.db binary format CQLite can *parse* on read is:
- Algorithm name with BE u16 length prefix
- Chunk length (default 64KB)
- Chunk offset table (u64 BE per chunk)
- Per-chunk CRC32 checksums
- Trailing metadata CRC32

### Frozen Collection Serialization

**Status**: IMPLEMENTED (Issue #377)

Frozen collections are serialized as single cells:

- **frozen\<list\<T\>\>**: `[i32 count][i32 len][bytes]...`
- **frozen\<set\<T\>\>**: Same format as frozen list
- **frozen\<map\<K,V\>\>**: `[i32 count][i32 key_len][key][i32 val_len][val]...`

### Non-Frozen Collection Serialization

**Status**: IMPLEMENTED (Issue #378)

Non-frozen collections are serialized as multiple cells (complex columns):

- **list\<T\>**: Elements stored with UUID timeuuid paths
- **set\<T\>**: Elements stored with serialized element as path
- **map\<K,V\>**: Entries stored with serialized key as path

Complex columns set the `ROW_HAS_COMPLEX_DELETION` flag (0x40).

### Static Row Support

**Status**: IMPLEMENTED (Issue #379)

Static rows use extended flags format:

- `ROW_HAS_EXTENDED_FLAGS` (0x80) set in row flags
- `EXTENDED_IS_STATIC` (0x01) as extended flags byte
- No clustering prefix (static rows apply to entire partition)
- Written after partition header, before regular rows

### Composite Partition Key Support

**Status**: IMPLEMENTED (Issue #380)

Multi-column partition keys use composite encoding:

- Single component: raw value bytes (no length prefix) — e.g. `text` is raw UTF-8,
  `uuid` is 16 raw bytes, `int` is 4 BE bytes.
- Multi-component: `[u16 BE len][bytes][0x00]` per component, **including a trailing
  `0x00` after the final component** (matches `PartitionKey::to_bytes`/`from_bytes`).

This encoding is decoded by the single canonical codec
`storage::partition_key_codec::decode_partition_key_columns`, shared by the write
engine and the read/scan path so the two cannot drift.

### Partition-key column reconstruction on the scan path

**Status**: RESOLVED (Issue #586, v0.10.1) — was a correctness defect in v0.10.0.

When a `SELECT` falls to the scan + residual-filter path (rather than the Index.db
point-lookup path used for `WHERE pk = <uuid>`, see Issues #548/#553), the partition
key is not present in the cell payload and must be reconstructed from the raw row key.

In v0.10.0 this reconstruction assumed a `u16` length prefix for **every** TEXT key.
That is the composite-component framing, *not* the single-component layout (which is
raw bytes), so:

- A **single-component TEXT partition key** (`id text PRIMARY KEY`) failed to decode;
  the error was silently swallowed and the column was dropped. `SELECT *` was missing
  `id` and `WHERE id = '<literal>'` returned 0 rows.
- A **composite partition key** decoded every column from component[0], so the second+
  columns got the wrong value (and non-text components became debug strings).

Both paths now decode through `partition_key_codec`, and a failed reconstruction is
logged (`log::warn!`) rather than swallowed.

### Delta Encoding with Statistics.db Baseline

**Status**: IMPLEMENTED

All timestamps, TTL values, and local deletion times are delta-encoded:

- `StatisticsWriter` produces baseline values (min_timestamp, min_ttl, min_local_deletion_time)
- `DataWriter` uses baseline for delta encoding in row and cell data
- Reduces SSTable size for tables with similar timestamps

---

## Resolved M5.0 Limitations

The following limitations from M5.0 have been resolved in M5.1:

### CompressionInfo.db Writing

**Status**: RESOLVED (was "NOT IMPLEMENTED" in M5.0)

M5.0 produced only uncompressed SSTables. M5.1 implements full compression support with:
- All four compression algorithms (LZ4, Snappy, Deflate, Zstd)
- Chunk-based compression with configurable chunk size
- Trailing CRC32 checksums per chunk
- CompressionInfo.db metadata file generation

### Collection Serialization

**Status**: RESOLVED (Issues #377, #378)

M5.0 had limited collection support. M5.1 implements:
- Frozen collection serialization (single-cell format)
- Non-frozen collection serialization (multi-cell complex columns)
- Proper flag handling (`ROW_HAS_COMPLEX_DELETION`)

### Static Column Support

**Status**: RESOLVED (Issue #379)

M5.0 did not support static columns. M5.1 implements:
- Static row writing with extended flags
- Proper ordering (static rows before regular rows)
- Correct column bitmap handling for static columns

---

## M5.2 Compaction and Export Capabilities

CQLite M5.2 introduces compaction APIs and SSTable export support. Note that compaction execution is pending M5.3 reader integration.

### K-Way Merge (Issue #382)

**Status**: PARTIALLY IMPLEMENTED

API surface defined; merge execution pending M5.3 SSTable reader integration.

K-way merge infrastructure for combining multiple L0 SSTables:
- Binary heap-based merge with O(log k) per entry
- Last-write-wins semantics by timestamp
- Schema-aware clustering key comparison
- Memory budget: k × 8KB peek buffers

**Current Limitation**: `KWayMerger::new()` returns "pending" error. Merge execution requires M5.3 reader integration to convert SSTable entries back to Mutation format.

**Code Review Fixes Applied**:
- `merge.rs:551`: Replaced `unwrap()` with `ok_or_else()` for proper error handling
- `merge.rs:643`: Added `log::warn` for schema comparison fallback instead of silent error

### STCS Merge Policy (Issue #383)

**Status**: IMPLEMENTED (not yet usable)

Pluggable compaction strategy via `MergePolicy` trait:
- `STCSPolicy`: Size-Tiered Compaction Strategy (Cassandra default)
  - Bucket grouping by size ratio (0.5x - 1.5x)
  - Configurable min/max thresholds (default: 4-32)
- Custom policies via `Box<dyn MergePolicy>`

**Current Limitation**: While the `STCSPolicy` logic is fully implemented and tested, it cannot be used yet because `WriteEngine::set_merge_policy()` returns an error pending M5.3 reader integration.

**Code Review Fixes Applied**:
- `merge_policy.rs:175-177`: Fixed bucket boundary to use inclusive comparisons (`>=`/`<=`)
- `merge_policy.rs:192-193`: Changed to saturating arithmetic for overflow safety

### Maintenance Step API (Issue #384)

**Status**: PARTIALLY IMPLEMENTED

Incremental maintenance via `maintenance_step()`:
- Non-blocking, budget-limited execution
- Returns `MaintenanceReport` with maintenance stats
- Suitable for background thread scheduling

**Current Limitation**: `maintenance_step()` currently performs only flush operations. Compaction steps pending M5.3 reader integration.

### TTL and Expiring Cells (Issue #386)

**Status**: IMPLEMENTED

TTL support for expiring data:
- TTL delta encoding against Statistics.db baseline
- Expiration timestamp tracking
- Tombstone generation for expired cells

**Code Review Fixes Applied**:
- `data_writer.rs:328`: Added negative TTL delta validation with descriptive error

### SSTable Export API (Issue #388)

**Status**: IMPLEMENTED

`export_sstable()` API for distribution:
- Cassandra-compatible naming: `{keyspace}-{table}-nb-{gen}-big-{Component}.db`
- Optional compaction before export
- Component validation (Data.db, Index.db, Statistics.db, etc.)

**Code Review Fixes Applied**:
- `export.rs:220`: Changed `std::fs::create_dir_all` to `tokio::fs::create_dir_all().await`
- `export.rs:343-378`: Made `find_most_recent_sstable()` async with `tokio::fs::read_dir`

---

## Remaining M5.1 Limitations

### Promoted Index (Deferred)

**Status**: DEFERRED (M5.2+ scope)

Index.db entries always write `promoted_index_length = 0`. Wide partitions (10K+ rows) cannot use fast within-partition seeks.

**Impact**:
- Simple/narrow partitions: No impact
- Wide partitions (10K+ rows): Linear scan required for within-partition queries

**Rationale**: M5.1 prioritizes correctness over performance. Promoted index requires complex sampling logic.

### Compaction

**Status**: PARTIALLY IMPLEMENTED (M5.2)

CQLite has defined k-way merge compaction API with STCS (Size-Tiered Compaction Strategy):
- `maintenance_step()` API for incremental maintenance (currently flush-only)
- `set_merge_policy()` for custom compaction strategies (currently returns error)
- API surface complete; execution pending M5.3 reader integration
- See "M5.2 Compaction and Export Capabilities" section above for details

**Current Limitation**: `set_merge_policy()` currently returns an error. Compaction execution requires M5.3 SSTable reader integration to convert entries back to mutations for k-way merge.

### BTI Format Writing

**Status**: NOT IMPLEMENTED

M5.1 produces BIG format SSTables only. BTI (trie-based) index writing is not supported.

**Rationale**: BTI is opt-in/experimental in Cassandra 5.0. BIG format covers >95% of production use cases.

### Index.db/Summary.db Full Format

**Status**: PARTIAL

Current implementation:
- Index.db: MD5 digest format with VInt offsets (no promoted index)
- Summary.db: Sampled entries with correct offset tracking

Not implemented:
- Full promoted index data in Index.db entries
- BTI trie format

### Statistics.db Full TOC Format

**Status**: IMPLEMENTED

The `StatisticsWriter` produces a full Cassandra 5.0 compatible Statistics.db with complete TOC structure:

**Implemented**:
- Full TOC header with component count and CRC32 checksums
- VALIDATION component (partitioner class name, bloom filter FP chance)
- COMPACTION component (minimal HyperLogLogPlus cardinality estimator)
- STATS component (EncodingStats with min/max timestamps, TTL, deletion times, histograms)
- SERIALIZATION_HEADER component (schema-derived or minimal stub)

**Known Limitations**:
- STATS component uses minimal histograms (2 buckets, empty tombstone histogram)
- COMPACTION component uses empty HyperLogLogPlus sketch (no cardinality data)

**Wide tables (>64 columns) — supported (Issue #763)**:
The SERIALIZATION_HEADER column sets are now written exactly as Cassandra's
`SerializationHeader.Serializer.writeColumnsWithTypes`
(cassandra-5.0.0 `SerializationHeader.java` lines 489-497): an unsigned-VInt column
count followed by `count` `(VInt-length name, VInt-length marshal type)` pairs. This
path has **no** 64-column limit. A previous note here claimed a "64-column bitmap"
cap; that was inaccurate — the 64-bit bitmap encoding belongs to
`Columns.serializer.serializeSubset` (`Columns.java` lines 503-531), which serialises
a per-row column *subset* against a pre-shared superset (Data.db rows / inter-node
messaging) and is never used for the SSTable header. Tables with 65+ columns therefore
round-trip losslessly; regression coverage lives in
`stats_writer.rs::tests::test_serialization_header_70_columns_roundtrip` and
`test_serialization_header_200_columns_count_is_vint`.

**Impact**: Statistics.db files are fully compatible with Cassandra 5.0. Schema can be provided explicitly for richer SerializationHeader, or omitted for minimal stub format.

---

## Parsing Limitations

### ~~Static Column Support (Exit Code 3)~~ - FIXED

**Status**: ✅ **FIXED** (Issue #210)
**Impact**: Was 1 table - now 0 (SerializationHeader extraction works for static column tables)
**Resolution**: Fixed SerializationHeader parser to handle static columns section

**Root Cause Found**: The SerializationHeader format includes a static column section between clustering keys and regular columns. The parser was treating the `static_count` byte as a separator (expecting `0x00`), which only worked when there were no static columns.

**Correct Format** (confirmed via `SerializationHeader.java`):
```
[pk_type] [ck_count] [ck_types...] [static_count] [static_columns...] [reg_count] [regular_columns...]
```

When `static_count = 0`, it encodes as `0x00`, making simple tables work. But when `static_count > 0`, parsing would fail.

**Fix**: Modified `parse_serialization_header_at_offset()` in `enhanced_statistics_parser.rs` to:
1. Parse static column count after clustering keys
2. Parse static column definitions when count > 0
3. Mark static columns with `is_static: true` flag

**Tracking**: Issue #210 (CLOSED)

---

### ~~SerializationHeader Marker Search Failures~~ - FIXED

**Status**: ✅ **FIXED** (Issue #216)
**Impact**: Was 5 tables - now 0 (SerializationHeader parsing works for all collection-heavy tables)
**Resolution**: Implemented TOC-based offset lookup and sequential parsing

**Root Cause Found**: The marker-based search (`0x00 0x00` pattern) for SerializationHeader was unreliable because:
- Collection type strings are long (80-200+ bytes) with multi-byte VInt length encoding
- Multiple `0x00 0x00` patterns exist in Statistics.db histogram data
- The parser picked patterns inside column data instead of the actual header start

**Solution Implemented**:
1. **TOC-Based Offset Lookup**: Statistics.db contains a Table of Contents at the start:
   - `[4 bytes num_components] [4 bytes checksum] [TOC entries...]`
   - Each TOC entry: `[4 bytes component_type] [4 bytes offset]`
   - Component type 3 (HEADER) points directly to SerializationHeader
2. **Sequential VInt Parsing**: New `parse_serialization_header_at_toc_offset()` parses:
   - EncodingStats (3 VInts: minTimestamp, minLocalDeletionTime, minTTL)
   - Partition key type (VInt len + string)
   - Clustering types (VInt count + types)
   - Static columns (VInt count + columns)
   - Regular columns (VInt count + columns)
3. **Proper Nested Type Conversion**: `extract_inner_type()` helper uses parenthesis depth tracking instead of `trim_end_matches(')')` to correctly handle nested types like `frozen<map<text, list<int>>>`

**Previously Affected Tables** (now all parsing correctly):
- `frozen_collections_table` - FrozenType(MapType) ✅
- `typed_collections_table` - ListType, SetType, MapType ✅
- `nested_collections_table` - MapType(FrozenType(ListType)) ✅
- `collections_with_udts` - MapType(FrozenType(UserType)) ✅
- `chat_messages` - MapType(FrozenType(SetType)) ✅

**Note**: While SerializationHeader parsing is fixed, these tables still fail smoke tests due to separate Data.db parsing issues (complex cell flags 0xc1-0xcf for collection types). This is a V5CompressedLegacy parser limitation, not a Statistics.db issue.

**Tracking**: Issue #216 (CLOSED)

---

### ~~Summary.db Header Format Mismatch~~ - FIXED

**Status**: ✅ **FIXED** (Issue #218)
**Impact**: Was 5 tables - now 0 (Summary.db parses correctly for all tables)
**Resolution**: Complete rewrite of `summary_reader.rs` with correct Cassandra 5.0 format

**Root Cause Found**: The original parser used a completely incorrect format specification. It expected a "version" field as the first 4 bytes, but Cassandra 5.0 Summary.db starts with `min_index_interval` (e.g., 128).

**Correct Cassandra 5.0 Format** (implemented):
```
Offset  Size  Field                    Description
------  ----  -----------------------  -----------
0x00    4     min_index_interval       e.g., 128 (BE)
0x04    4     entries_count            Number of entries (BE)
0x08    8     summary_entries_size     Offset table + entry data size (BE)
0x10    4     sampling_level           Sampling level 1-128 (BE)
0x14    4     size_at_full_sampling    Entries at full sampling (BE)
        ----  Total header: 24 bytes
0x18    4*N   offset_table[]           LITTLE-ENDIAN offsets!
        var   entries[]                key_data + be_u64 position
        var   first_key                be_u32 size + key data
        var   last_key                 be_u32 size + key data
```

**Critical Implementation Details**:
1. **Offset table is LITTLE-ENDIAN** (not big-endian like everything else!)
2. **No length prefix for entry keys** - key boundaries determined by offset differences
3. **No tokens in summary entries** - only partition key + Index.db position
4. **First/last keys at file end** - serialized with be_u32 length prefix

**API Changes**:
- `SummaryEntry.token` removed (tokens not stored in Summary.db)
- `SummaryEntry.index_offset` renamed to `position`
- `find_entries_in_range()` removed (no token-based queries)
- `find_best_entry_for_token()` replaced with `find_entry_for_position()`
- `get_token_ranges()` removed
- Added `get_first_key()`, `get_last_key()`, `get_header()`
- `iterate_token_range()` deprecated, use `iterate_all_partitions()`
- `get_token_coverage()` deprecated (tokens must be computed from partition keys)

**Tracking**: Issue #218 (CLOSED)

---

### ~~Complex Cell Flags in Data.db~~ - ROOT CAUSE FIXED

**Status**: ✅ **ROOT CAUSE FIXED** (Issue #218)
**Reality**: The "cell flags 0xc1-0xcf" errors were **cascading failures** from Summary.db parsing

With Issue #218 fixed, Summary.db now parses correctly. The remaining collection-heavy table failures are separate Data.db parsing issues with complex cell types (UDTs, frozen collections, nested collections), not cascading from Summary.db.

**Tracking**: Issue #218 (CLOSED)

---

### ~~Clustering Key Row Format Parsing Failures (Exit Code 5)~~ - FIXED

**Status**: ✅ **FIXED** (Issue #213)
**Impact**: Was ~19 tables - now 0 (all clustering key tables parse correctly)
**Resolution**: Corrected field order in V5CompressedLegacy parser

**Root Cause Found**: The clustering prefix comes BEFORE `row_size` in Cassandra's format, not after.

**Correct Format** (confirmed via `UnfilteredSerializer.java`):
```
[row_flags] [extended_flags] [clustering_prefix] [row_size] [prev_size] [row_body]
```

**Previous (Wrong) Format**:
```
[row_flags] [row_size] [prev_size] ... [clustering_prefix]  ← Wrong order!
```

**Fix Details**:
- Split `parse_row_header()` into `parse_row_flags()` + `parse_row_metadata()`
- Parse clustering prefix immediately after flags, before row_size
- File: `cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs`

**Results**:
- Smoke test pass rate improved from 27% (9/33) to 79% (26/33)
- All clustering key tables now pass: sensor_data, wide_partition_table, app_metrics, etc.

**Related Fixes (Issue #211)**:
- ✅ Removed false positive magic number `0x00400000` (was LZ4 chunk length prefix)
- ✅ Fixed NB format headerless detection
- ✅ Corrected V5_0NewBig to use V5CompressedLegacy format

---

### ~~BTI Index Zero Entries (Exit Code 0, Silent Failure)~~ - FIXED

**Status**: ✅ **FIXED** (Issue #212)
**Impact**: Was 1 table - now 0 (BTI index parsing works correctly)
**Resolution**: Fixed V5_0NewBigFormat version variant handling in block_io.rs

**Root Cause Found**: Two issues combined to cause silent data loss:

1. **Missing Version Variant**: `CassandraVersion::V5_0NewBigFormat` was not included in the match statement for NB format chunk reading in `block_io.rs`. This caused the reader to use legacy block header parsing (which returns EOF immediately) instead of the correct NB chunk-based reading.

2. **BTI Inter-Entry Padding**: BTI Index.db entries have variable padding bytes between them (null or non-null). The parser needed enhanced padding skip logic to find valid entry boundaries.

**Correct Flow** (after fix):
```
V5_0NewBigFormat → read_nb_format_chunk_data() → decompress chunk → parse partition data
```

**Previous (Wrong) Flow**:
```
V5_0NewBigFormat → read_legacy_format_block_header() → EOF → 0 entries
```

**Fix Details**:
- File: `cqlite-core/src/storage/sstable/reader/block_io.rs` - Added `V5_0NewBigFormat` to NB chunk reader match
- File: `cqlite-core/src/storage/sstable/index_reader.rs` - Enhanced BTI padding skip logic

**Results**:
- `stock_prices` now returns 231 entries (2 partitions with rows)
- Smoke test pass rate improved from 26/33 (79%) to 28/33 (85%)
- BTI format parsing now works correctly for all tested tables

**Tracking**: Issue #212 (CLOSED)

---

### ~~BTI Empty Index Fallback (Query Returns 0 Rows)~~ - FIXED

**Status**: ✅ **FIXED** (Issue #256)
**Impact**: Was 1 table - now 0 (time_bucketed_counters returns 41 rows correctly)
**Resolution**: Added empty index entries check to trigger sequential scan fallback

**Root Cause Found**: When BTI Index.db parsing is incomplete (returns 0 partition entries), the scan path in `data_access.rs` would:
1. Take the index-based path (since `self.index.is_some()`)
2. Get 0 entries from `get_range()`
3. Check `has_zero_size` which is `false` (no entries to check)
4. Return empty results without falling back to sequential scan

**Symptom**: `SELECT * FROM test_timeseries.time_bucketed_counters` returned 0 rows despite containing 41 rows.

**Fix Details**:
- File: `cqlite-core/src/storage/sstable/reader/data_access.rs`
- Added check for empty entries BEFORE the `has_zero_size` check
- When `entries.is_empty()`, triggers sequential scan fallback
- Sequential scan correctly parses Data.db directly, bypassing index issues

**Code Change**:
```rust
// Issue #256 FIX: Fall back to sequential scan when index returns no entries
if entries.is_empty() {
    return self.sequential_scan(table_id, start_key, end_key, limit, schema).await;
}
```

**Results**:
- `time_bucketed_counters` now returns 41 rows via sequential scan fallback
- No regression on tables using DigestFormat index

**Tracking**: Issue #256 (CLOSED)

---

### BTI End-to-End Validation (Issue #36 - Deferred to Post-M2)

**Status**: 🔄 **DEFERRED** (Issue #36)
**Impact**: No full BTI parity testing against sstabledump
**Decision**: BTI validation deferred to future milestone per team agreement

**Background**: Issue #36 requested comprehensive BTI validation including:
- TDD tests for trie traversal lookups and iteration
- Rows.db decoding tests with range tombstones and complex types
- Round-trip byte-comparable invariants
- Zero-diff vs sstabledump on BTI datasets

**Key Findings**:
1. **BIG format is Cassandra 5.0 default** - BTI requires explicit opt-in via `selected_format: bti` in cassandra.yaml
2. **All test data uses BIG format** - 100% of 354 SSTable files use `nb-` prefix (BIG format)
3. **0% BTI test data exists** - No Partitions.db/Rows.db trie files in test datasets
4. **BTI is experimental** - Cassandra 5.0 marks BTI as opt-in, expected <5% production adoption

**Current Implementation** (~3,200 LOC in `cqlite-core/src/storage/sstable/bti/`):
- ✅ Format detection (magic number `0x6461`)
- ✅ Byte-comparable encoding (CEP-25 compliant)
- ✅ Trie node structures (all 4 types)
- ✅ SizedInts encoding
- ⚠️ Trie traversal (stub implementation)
- ❌ Range queries (not implemented)
- ❌ Full partition iteration (not implemented)

**Decision Rationale**:
- No BTI test data available for validation
- BTI is opt-in/experimental in Cassandra 5.0
- BIG format covers 100% of current test scenarios
- Production BTI code preserved for future validation

**Future Work** (new issue when BTI demand emerges):
1. Configure test cluster with `selected_format: bti`
2. Generate real BTI SSTables (Partitions.db, Rows.db)
3. Validate CQLite BTI parser vs sstabledump output
4. Complete trie traversal implementation

**Reference**: Full status documented in `docs/sstables-definitive-guide/references/bti-v1-status.md`

**Tracking**: Issue #36 (DEFERRED - see issue comments for full discussion)

---

### Table ID Matching in scan_for_key() (Issue #36 Follow-up - FIXED)

**Status**: ✅ **FIXED** (Issue #36 regression fix)
**Impact**: Was causing zero-row results when table names had qualified vs unqualified mismatch
**Resolution**: Updated `scan_for_key()` to use `table_ids_match()` function

**Root Cause Found**: The `scan_for_key()` function in `data_access.rs` used direct equality (`==`) to compare table IDs, which failed when:
- Query used qualified name (e.g., `test_basic.simple_table`)
- SSTable stored unqualified name (e.g., `simple_table`)
- Or vice versa

**Symptom**: CLI queries returned zero rows with debug log showing `table_id mismatch ('test_basic.simple_table' != 'simple_table')`.

**Fix Details**:
- File: `cqlite-core/src/storage/sstable/reader/data_access.rs:504`
- Changed: `entry_table_id == *table_id` → `table_ids_match(&entry_table_id, table_id)`
- The `table_ids_match()` function (lines 26-50) handles qualified/unqualified name comparison correctly

**Note**: The `sequential_scan()` function (line 648) already used `table_ids_match()` correctly. This fix aligns `scan_for_key()` with the same matching logic.

**Tracking**: Issue #36 (comment thread, Jan 2026)

---

### BTI Metadata Offset Extraction (Performance Optimization - M3+ Scope)

**Status**: 🔄 **DEFERRED** (Issue #226)
**Impact**: Performance - sequential scan fallback instead of direct partition lookup
**Current Behavior**: Fully functional with sequential read mode

**Background**: BTI format Index.db entries contain variable-length metadata after the partition key. This metadata encodes the Data.db offset for direct partition seeks, but the exact format was not previously documented.

**Research Findings** (Issue #226):
- BTI payload uses **SizedInts encoding** (not VInt)
- Format: `[hash_byte: 1 byte][position: size bytes]`
- Size determined by `payloadBits` field in trie node header
- Formula: `size = payloadBits - 7`

**Example from stock_prices Index.db**:
```text
00 00 04 80 00 4f 88 00
^  ^-----------^
│  └─ Position bytes (Data.db offset)
└─── Hash byte (filter hash lower 8 bits)
```

**Current Workaround**: Sequential scan with raw_key matching (Issue #212 fix) - functionally correct but O(n) performance.

**Future Optimization** (M3+ scope):
1. Extract `payloadBits` from BTI trie node headers
2. Decode SizedInts to get Data.db offset
3. Enable O(log n) direct partition seeks

**Implementation Status**:
- ✅ SizedInts decoder implemented (`cqlite-core/src/storage/sstable/bti/sized_ints.rs`)
- ✅ Research documented (`docs/research/BTI_PAYLOAD_*.md`)
- ⏳ Trie node header parsing (pending)
- ⏳ Direct offset extraction (pending)

**Tracking**: Issue #226 (log noise fix - CLOSED), Issue #208 C3 (offset extraction - deferred)

---

### Index.db VInt Offset Parsing (DigestFormat - NB Tables)

**Status**: 🐛 **OPEN** (Issue #237)
**Impact**: 83% of partitions skipped in 7 test_timeseries tables (~827 partitions)
**Current Behavior**: Falls back to sequential Data.db scan with "malformed partition" warnings

**Root Cause**: The Index.db parser incorrectly reads VInt offsets as length-prefixed bytes. The current implementation treats the first byte after the digest as an `offset_len` field, then reads that many bytes. This matches older MC/MD SSTable formats, but NB format (Cassandra 5.0) uses VInt encoding directly.

**Affected Format** (DigestFormat with VInt Offsets):
```
Entry: marker(2) + digest(16) + vint_offset(1-9 bytes)

Where:
- marker: 0x0010 (fixed)
- digest: 16-byte MD5 hash of partition key
- vint_offset: Cassandra VInt encoding (NOT length-prefixed)
```

**Bug Location**: `cqlite-core/src/storage/sstable/index_reader.rs`
- Function: `parse_simple_partition_key_with_offset()` (lines ~375-430)

**Current (Wrong)**:
```rust
let (input, offset_len) = nom_u8(input)?;      // Treats VInt byte as length!
let (input, offset_bytes) = take(offset_len)(input)?;
let data_offset = decode_be_offset(offset_bytes);
```

**Evidence from sensor_data Index.db**:
```
0x0000  00 10 02 84 a7 18 be 7b 49 e6 b6 b9 8e 82 f5 ff  .......{I.......
0x0010  16 60 [00] 00 00 10 7d 39 42 8c aa a8 45 1d 84 7f  .`....}9B...E...
        ^^^^^^ Entry 0 VInt (0x00 = 0)
                   ^^^^^ Entry 1 marker
```

Parser reads `0x00` as `offset_len=0`, takes 0 bytes, advances by 19 bytes instead of 20. Next entry parse fails.

**Affected Tables**:
| Table | Partitions | Currently Parsed | Success Rate |
|-------|-----------|------------------|--------------|
| sensor_data | 9 | 1 | 11% |
| app_metrics | 199 | 1 | <1% |
| user_activity | 199 | 1 | <1% |
| log_entries | 199 | 1 | <1% |
| event_store | 199 | 1 | <1% |
| user_sessions | 199 | 1 | <1% |
| tick_data | 23 | 1 | 4% |

**Proposed Fix**:
```rust
// Replace offset parsing with VInt decoding:
let (input, vint_offset) = parse_vint(input)?;
let data_offset = vint_offset;  // SSTableReader adds header_size later
```

**Additional Notes**:
- Index.db offsets are relative to Data.db data section (exclude 30-byte header)
- VInt decoding already exists in `cqlite-core/src/parser/vint.rs`
- Format detection needed to distinguish NB VInt from legacy length-prefixed

**Tracking**: Issue #237

---

## Epic #817 — Compaction-fidelity gaps (verified)

These limitations were surfaced and byte-verified during Epic #817 (compaction
fidelity). Each is grounded in CQLite's own reader/writer or a cited Cassandra
class; where CQLite diverges from Cassandra it is called out explicitly.

### Reader lacks the `≥ 64`-column large-subset decode branch (#12)

**Status**: 🐛 **OPEN**

When `HAS_ALL_COLUMNS` (0x20) is clear, Cassandra's `Columns.Serializer.serializeSubset`
(`Columns.java:503-531`) selects the columns-subset encoding by superset size: a single
unsigned-VInt bitmap for `< 64` regular columns, and a **large-subset** form (VInt count +
smaller-of present/missing **absolute** column indices, each an unsigned VInt — not deltas) for
`≥ 64`. CQLite's reader
(`reader/parsing/v5_compressed_legacy.rs::parse_row_metadata`) always reads a single
`parse_vuint` into a `u64` `missing_columns_bitmap` and has no `≥ 64` branch, so for a
`≥ 64`-column table it consumes only the missing-count VInt and then mis-reads the trailing
index VInts as cell data, corrupting the row stream. The reader also treats any column at
`idx >= 64` as present regardless of the field. The **writer** implements both modes correctly
(`data_writer.rs::write_column_subset`), pinned at the 63/64/65 boundary by
`cqlite-core/tests/issue_824_column_subset_and_filter.rs`. **Workaround**: tables with fewer
than 64 regular columns are unaffected (the common case).

### Complex-column merge is whole-column, not per-cell-path (#14/#17/#18) — RESOLVED in epic #921

**Status**: ✅ **RESOLVED** (#844 / #888 / #927 / #887, epic #921)

**Was** (Epic #817): Cassandra merges complex (multi-cell collection/UDT) columns **per
cell-path** using the column's path comparator — signed `ShortType` for a UDT field index,
`TimeUUIDType` for a list element, the map key type for a map — applying shadow-before-purge per
path. CQLite's merge (`storage/write_engine/merge.rs::reconcile_cluster`) reconciled by **whole
column**: its `CellData` carried no cell-path, so per-path merge of multi-cell collections/UDTs
was not representable.

**Now** (epic #921): `reconcile_cluster` keys per-cell winners by `(column, cell_path)` and the
`merge_entry_to_mutation` rewrite emits one `WriteComplexElement` per surviving element. Disjoint
elements survive; a same-key collision resolves by the higher per-cell timestamp (#844). UDT field
paths compare as **signed** `ShortType` (`compare_cell_paths`, field index `>= 32768` sorts
negative) and complex columns match **by name** across differing source headers
(#888 / #927; parity Cassandra `d14c96b8` / `5e636f9`). Complex deletion markers reconcile with
strict-supersede (equal `markedForDeleteAt` does NOT supersede) and shadow-before-purge (elements
with ts `<= markedForDeleteAt` are shadowed BEFORE the marker is purged) (#887; parity `bd244649`
+ `f66fa14f`). Non-frozen UDT multi-cell data now reads and writes end-to-end (#927). See
Chapter 11 — "Compaction merge semantics". Authority:
`org.apache.cassandra.db.rows.Cells` (per-column complex merge) and the column's `CellPath`
comparator.

### Equal-timestamp live-cell value tie-break diverges from Cassandra (#4/#21)

**Status**: 🐛 **OPEN** (divergence; FIX ruled in #818, follow-up)

At equal timestamp with two **live** cells (neither a tombstone), Cassandra's
`Cells.resolveRegular` keeps the cell with the strictly-greater **raw value bytes** (unsigned
lexicographic over the raw value, skipping the VInt length prefix). CQLite's
`reconcile_cluster` keeps the **first-seen** cell (newest file by `run_index`) and does not
compare value bytes — its `replace` predicate fires only for a higher timestamp or an
equal-timestamp cell tombstone. Result: at an exact timestamp tie between two distinct live
values, CQLite may keep a different value than Cassandra. Only PART of Cassandra's equal-ts
hierarchy matches: a cell tombstone beats both a live and an expiring cell (rules 1–2). CQLite
does NOT implement expiring-beats-pure-live or the `localDeletionTime`/TTL tie-breaks (rules
3–4) — those are additional divergences (see Chapter 11). Authority:
`org.apache.cassandra.db.rows.Cells.resolveRegular`.

### Latent: RT / complex-deletion size VInt width (#25)

**Status**: 🐛 **OPEN** (latent)

Cassandra's `UnfilteredSerializer` writes certain marker/deletion sizes as `long` VInts where
the corresponding read uses an `(int)` VInt (and vice versa) in places; CQLite's
range-tombstone-marker and complex-column-deletion size fields must use the matching width or a
large partition can mis-encode the size field. This is currently latent (small fixtures do not
exceed the narrow width) but is a real width hazard to watch when writing large partitions.
Authority: `org.apache.cassandra.db.rows.UnfilteredSerializer` (marker/row-body size fields).

### AlwaysPresentFilter / absent `Filter.db` — handled (#23)

**Status**: ✅ **HANDLED** (documented for completeness)

A table created with `bloom_filter_fp_chance = 1.0` is backed by Cassandra's
`AlwaysPresentFilter`, which serializes nothing — the SSTable has **no `Filter.db` component**.
CQLite reads such tables correctly via **both** `scan` and `get`: the per-reader bloom gate
(`reader/data_access.rs`) only consults `might_contain` when a filter is present, so an absent
filter ("always maybe") never short-circuits a point lookup to `None`; `get` then falls back to
the same stitched-chunk scan that `scan` uses. Verified by
`cqlite-core/tests/issue_824_column_subset_and_filter.rs` (absent-`Filter.db` scan + get tests,
which also strip the `Filter.db` TOC entry to faithfully reproduce the always-present case).

---

## Epic #921 — Compaction merge semantics (landed) and residual gaps

Epic #921 made CQLite's compaction merge path act on metadata that earlier epics
only carried. The merge behaviors are documented in full in Chapter 11 — "Compaction
merge semantics"; this section records what is now **supported** and the limitations
that **remain** after the epic, each verified against the code on the epic branch.

### Non-frozen UDT multi-cell read+write — SUPPORTED (#927)

**Status**: ✅ **SUPPORTED** end-to-end (was previously called out as unsupported)

A non-frozen UDT is stored as a complex column with one cell per field, keyed by a
2-byte **signed** `ShortType` declared field index. CQLite now reads such columns and
writes them back through compaction: per-element winners reconcile by `(column,
cell_path)`, field paths sort by signed `ShortType` (`compare_cell_paths`), and
complex columns are matched **by name** so two sources with differing serialization
headers merge the same logical column (#888 / #927; parity Cassandra `d14c96b8` /
`5e636f9`). Any stale "non-frozen UDT unsupported" claim elsewhere is superseded by
this entry.

### Row-deletion + live-cells coexistence — NOT represented (#932)

**Status**: 🐛 **OPEN** (limitation)

A Cassandra row can carry a **row deletion** (`HAS_DELETION`) **and** surviving cells
written strictly after the deletion timestamp at the same time. CQLite does not
represent this: the V5CompressedLegacy reader collapses a `HAS_DELETION` row to a
**pure tombstone** (it emits the row tombstone with an empty cell map and detects it
via `RowHeader::is_row_tombstone`), and the merge `RowData` is an enum — `Tombstone`
**xor** `Live`, never both. So a row tombstone that should coexist with newer
surviving cells **loses the row deletion** (the surviving cells win and the deletion
is dropped). Authority: `org.apache.cassandra.db.rows.Row` (a `Row` carries both a
`Row.Deletion` and live cells). Tracked in **#932**.

### Range tombstones during compaction — NOT applied/emitted (#933)

**Status**: 🐛 **OPEN** (limitation)

Range tombstones are not applied or emitted end-to-end in the compaction merge path.
The V5CompressedLegacy reader **skips** range tombstone markers
(`skip_range_tombstone_marker`) on the normal scan/compaction path — it does not
surface a surviving marker to the merger (only the dedicated `delta-scan` path decodes
them via `parse_range_tombstone_marker_full`), and `merge_entry_to_mutation` does not
persist a surviving range marker into the rewritten output. Consequently a range
tombstone is neither used to shadow covered rows during compaction nor re-emitted into
the compacted SSTable. Authority:
`org.apache.cassandra.db.rows.RangeTombstoneMarker` / `UnfilteredSerializer`. Tracked
in **#933**.

### gc_grace purging: overlap-aware in partial compactions (#935)

**Status**: ✅ **RESOLVED** (#935; full-compaction fast path from #845 retained)

gc_grace / `gcBefore` tombstone purging (#845, parity Cassandra `8d47ebb2`) runs on a
**full/major** compaction (which spans every SSTable for the table) **and**, since
**#935**, on a **partial / background** compaction when an overlap check proves the
tombstone is safe to purge. The background path (`WriteEngine::maintenance_step`)
computes a `max_purgeable_timestamp` — the minimum write timestamp
(`markedForDeleteAt`, micros) across the **non-included overlapping** SSTables, read
from their `Statistics.db` min-timestamp bound (`merge::compute_max_purgeable_timestamp`)
— and threads it into the merger (`KWayMerger::with_max_purgeable_timestamp`). In
`reconcile_cluster_with_overlap` a tombstone is purged only when BOTH its gc grace has
elapsed (`localDeletionTime < gcBefore`) AND its own deletion timestamp is **strictly
less** than that bound, so it provably shadows nothing outside the compaction set. A
full compaction uses an `i64::MAX` (+∞) bound, identical to #845; a partial compaction
with no readable overlap bound stays conservative and retains every tombstone (#921).
In the one-shot CLI, purging remains opt-in via `--major` / `--purge-tombstones`
(the explicit input list carries no table-wide overlap context). Authority:
`org.apache.cassandra.db.compaction.CompactionController#maxPurgeableTimestamp` /
`getPurgeEvaluator` (`time -> time < minTimestamp`).

---

## Validation Status

### Overall Pass Rate: 100% (33/33 tables) ✅ COMPLETE (macOS)

As of Issue #220 fix (Updated: 2025-12-18)

**Note**: All SSTable component parsers and cell type handling are now complete. All 33 test tables pass validation on macOS!

### ~~CI Environment Issue (Issue #225)~~ - FIXED

**Status**: ✅ **FIXED** (Issue #225)
**Impact**: Was 2 tables failing on Linux CI - now 0
**Resolution**: Added bounds checks and safe type conversions for complex collection parsing

**Root Cause Found**: Non-frozen complex column parsing lacked the bounds check present in frozen collection parsing. The `parse_complex_column` function used `Vec::with_capacity(cell_count_usize)` without first checking against `MAX_FROZEN_COLLECTION_SIZE`. Additionally, `parse_complex_cell_value` and `skip_complex_cell` used unsafe `as usize` casts on `path_len` and `value_len` (u64 values) which could overflow on large/corrupted values.

**Fix Applied**:
1. Added `MAX_CELL_VALUE_LENGTH` constant (64 MB limit) for path/value length validation
2. Added bounds check in `parse_complex_column` matching frozen collection pattern
3. Replaced `as usize` casts with `try_into()` + limit checks in `parse_complex_cell_value`
4. Applied same safe conversion pattern in `skip_complex_cell`

**Previously Affected Tables** (now parsing correctly on all platforms):
- `test_collections.large_collections_table` ✅
- `test_timeseries.app_metrics` ✅

**Tracking**: Issue #225 (CLOSED)

### Pass Rate by Keyspace

| Keyspace | Passed | Failed | Total | Pass Rate |
|----------|--------|--------|-------|-----------|
| **test_basic** | 8 | 0 | 8 | 100% ✅ |
| **test_collections** | 8 | 0 | 8 | 100% ✅ |
| **test_timeseries** | 9 | 0 | 9 | 100% ✅ |
| **test_wide_rows** | 8 | 0 | 8 | 100% ✅ |

**Note**: All tables now pass after completion of Issues #219, #220, and #221!

### Passing Tables (Production-Ready)

These tables are validated against Apache Cassandra's `sstabledump` output:

**test_basic** (8/8 passing - 100%):
- `simple_table` - Gold standard validation table
- `composite_key_table` - Composite partition keys validated
- `compression_test_table` - LZ4 compression validated
- `multi_partition_table` - Multi-partition scenarios
- `ttl_test_table` - TTL metadata parsing
- `counters` - Counter column type support
- `uncompressed_table` - Now passing after Issue #213 fix
- `static_columns_table` - Static columns now working (Issue #210 fix)

**test_collections** (8/8 passing - 100% ✅):
- `collection_table` - Lists, sets, maps validated
- `collection_clustering_table` - Collections with clustering keys (Issue #213 fix)
- `collections_with_udts` - UDT support (Issue #220 fix) ✅
- `empty_collections_table` - Empty collection handling
- `frozen_collections_table` - Frozen collections (Issues #219, #221 fix) ✅
- `large_collections_table` - Large collection support
- `nested_collections_table` - Nested collections (Issue #218 fix)
- `typed_collections_table` - Complex collection types (Issue #221 fix) ✅

**test_timeseries** (9/9 passing - 100%):
- `sensor_data` - Timestamp clustering (Issue #213 fix, was key test case)
- `app_metrics`, `log_entries`, `tick_data` - All passing
- `time_bucketed_counters`, `user_activity`, `user_sessions`, `event_store`
- `stock_prices` - BTI format now working (Issue #212 fix)

**test_wide_rows** (8/8 passing - 100% ✅):
- `wide_partition_table` - Wide partitions (Issue #213 fix)
- `chat_messages` - Non-frozen collections with frozen values (Issue #221 fix) ✅
- `document_versions`, `large_blob_table`, `many_columns_table`
- `multi_metric_timeseries`, `product_catalog`, `sparse_data_table`

### Remaining Failures

**Status**: ✅ **NO REMAINING FAILURES** - All 33 tables now pass!

Previously blocking issues have been resolved:
- ✅ **Issue #219**: Frozen type support - FIXED
- ✅ **Issue #220**: UDT support - FIXED
- ✅ **Issue #221**: Complex cell flag handling - FIXED

All core SSTable component parsers are working correctly with complete support for all data types and collection formats.

---

## M2+ Deferred Features

The following features are planned but not implemented in the current M2 milestone:

### SSTable Writing (Removed in Issue #175, #176)

**Status**: Removed from codebase
**Rationale**: CQLite is a **read-only library** focused on local SSTable access

**Removed Components**:
- `storage/wal.rs` (Write-Ahead Log)
- `storage/memtable.rs` (In-memory write buffer)
- `storage/compaction.rs` (Background merging)
- `storage/manifest.rs` (Metadata tracking)
- `storage/sstable/writer.rs` (SSTable serialization)
- `storage/sstable/validation.rs` (Write validation)

**Impact**: All `put()`, `delete()`, `flush()`, `compact()` methods return errors with message "removed in Issue #175/176".

**Workaround**: Use Apache Cassandra for writes. CQLite is read-only.

**Future**: Write support may return in M4+ if community demand justifies the complexity.

---

### Experimental Features (Opt-In)

#### Bloom Filter Tests (Issue #65)

**Feature Flag**: `experimental`
**Default**: **Disabled** (not in CI default lane)
**Purpose**: Gate bloom filter unit tests for M3 milestone

Bloom filter implementation is complete with overflow-safe arithmetic (`wrapping_add`/`wrapping_mul`). Tests are gated behind the `experimental` feature to keep them out of the default CI lane per Issue #65 requirements.

**To Run Bloom Filter Tests**:
```bash
cargo test --package cqlite-core --features experimental bloom
```

**Note**: The bloom filter *implementation* is always available; only the *tests* require the feature flag.

---

#### Legacy Heuristics (Pre-5.0 Format Support)

**Feature Flag**: `legacy-heuristics`
**Default**: **Disabled** (not in CI)
**Purpose**: Backward compatibility for Cassandra 3.x/4.x SSTables

CQLite defaults to Cassandra 5.0+ formats using authoritative metadata (no-heuristics mandate, Issue #28). Legacy heuristics enable schema-less blob fallback for older formats.

**To Enable**:
```bash
cargo build --features legacy-heuristics
```

**Note**: Legacy support is **not tested in CI** and may have gaps. Modern Cassandra 5.0 is the supported target.

---

#### ANTLR Parser (Alternative CQL Parser)

**Feature Flag**: `antlr`
**Default**: Disabled
**Purpose**: ANTLR4-based CQL parser as alternative to nom-based parser

M2+ uses nom parser by default. ANTLR integration is experimental and incomplete.

---

#### Tombstone and GC Logic

**Feature Flag**: `tombstones`
**Default**: Disabled
**Purpose**: Tombstone detection and garbage collection semantics

**Status**: Deferred to M3+

Current implementation skips tombstoned rows (Issue #191 fix in select_executor.rs) but does not expose tombstone metadata or perform GC simulation.

---

### Query Engine Limitations (M2+ Scope)

**Current State**: Query engine enabled by default (`state_machine` feature)

**Implemented**:
- SELECT statement parsing and execution
- Prepared statement support
- Query planning and optimization
- Multi-partition query execution
- Schema-aware result formatting

**Not Implemented** (M3+ Scope):
- INSERT/UPDATE/DELETE statement execution (write operations)
- WHERE clause filtering (partial - partition key filtering works)
- ORDER BY clause support
- LIMIT clause support
- Aggregate functions (COUNT, SUM, AVG, etc.)
- GROUP BY clause support

**Workaround**: For unsupported query features, use `read-sstable` command for raw data access or Apache Cassandra tools.

---

## Known Workarounds

### Workaround 1: Using sstabledump for Unsupported Tables

For tables that fail to parse (static columns, frozen types, UDTs), use Apache Cassandra's `sstabledump` tool:

```bash
# Generate JSONL output
sstabledump /path/to/Data.db > output.jsonl

# Human-readable format
sstabledump -d /path/to/Data.db
```

**Note**: sstabledump requires Cassandra installation and Java runtime.

---

### Workaround 2: Direct Component Access

For debugging or advanced use cases, access individual SSTable components directly:

```bash
# Read Statistics.db metadata
cqlite read-sstable --component statistics /path/to/Statistics.db

# Read Index.db entries
cqlite read-sstable --component index /path/to/Index.db

# Read CompressionInfo.db
cqlite read-sstable --component compression-info /path/to/CompressionInfo.db
```

---

### Workaround 3: Entry Count Mismatches (Multi-Row Partitions)

**Observation**: Some passing tables report fewer entries than expected rows:
- `composite_key_table`: 45 entries vs 99 rows
- `multi_partition_table`: 24 entries vs 99 rows
- `ttl_test_table`: 44 entries vs 99 rows

**Explanation**: These tables have clustering keys, creating multi-row partitions. CQLite counts **partition entries** while sstabledump counts **total rows**.

**Action**: This is correct behavior. No workaround needed.

---

### Workaround 4: Minimal Builds (No Query Engine)

For embedded or constrained environments, build without query engine:

```bash
# M1-compatible binary (storage layer only)
cargo build --no-default-features --features all-compression

# Binary size reduced by ~40% (no query planner/executor)
```

**Trade-off**: Lose `execute()`, `prepare()`, `explain()` methods. Only low-level SSTable API available.

---

## Issue References

### Completed Issues (Fixed - Jan 2026)

- **Issue #258**: V5CompressedLegacy Parser Errors for 15/33 Tables - **FIXED**
  - Status: ✅ FIXED - Two root causes identified and resolved
  - Root cause 1: Timestamp units mismatch in `parser/types.rs` - `parse_timestamp()` multiplied milliseconds by 1000 (converting to microseconds) but `Value::Timestamp(i64)` stores milliseconds. This caused overflow → negative values → `<invalid-timestamp:...>` markers.
  - Root cause 2: Partition header flags heuristic in `v5_compressed_legacy.rs` - `flags > 0x20` check rejected valid partition headers with higher flag values, causing single-byte offset skip and cascading misalignment errors. Violated Issue #28 no-heuristics mandate.
  - Fix 1: Removed `* 1000` multiplication in `parser/types.rs:289` - now stores milliseconds directly
  - Fix 2: Removed `flags > 0x20` heuristic check in `v5_compressed_legacy.rs:292` - validation now format-based only
  - Result: All 33 test tables pass comprehensive SELECT tests with no ERROR messages or invalid data markers
  - Files: `cqlite-core/src/parser/types.rs`, `cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs`

- **Issue #240**: DATE Type Values Display as `<invalid-date:...>` - **FIXED**
  - Status: ✅ FIXED - DATE type now parses correctly in all contexts including map keys
  - Root cause: `CqlType::Date` was mapped to `ComparatorType::Custom("date")` in `comparator.rs`, causing DATE values to fall through to blob parsing. Also, multiple parsing paths (parser/types.rs, v5_compressed_legacy.rs) read DATE as raw i32 without Cassandra's Integer.MIN_VALUE offset decoding.
  - Fix:
    1. Added `ComparatorType::Date` variant to comparator.rs with proper comparison support
    2. Updated `from_cql_type()` and `from_cql_type_with_registry()` to map `CqlType::Date` → `ComparatorType::Date`
    3. Added DATE parsing arm to `parse_value_with_schema_type()` and `parse_value_with_comparator()` in value_parsing.rs
    4. Fixed `parse_date()` in parser/types.rs to apply Cassandra DATE encoding: `stored.wrapping_add(i32::MIN as u32) as i32`
    5. Fixed map key DATE parsing in v5_compressed_legacy.rs line 5327
  - Cassandra DATE encoding: 4-byte big-endian unsigned int shifted by Integer.MIN_VALUE (2^31) for byte-order comparability. Decoding adds i32::MIN back.
  - Result: DATE columns and DATE keys in maps now display as `YYYY-MM-DD` format (e.g., `2025-10-05`) instead of `<invalid-date:...>`
  - Files: `comparator.rs`, `value_parsing.rs`, `comparator_value_parsing.rs`, `key_digest.rs`, `parser/types.rs`, `v5_compressed_legacy.rs`

- **Issue #238**: UDTs Inside Collections Not Parsed - **FIXED**
  - Status: ✅ FIXED - Extended `parse_value_with_comparator` for recursive type parsing
  - Root cause: `parse_value_with_comparator` had minimal implementation (only Boolean, Text, Blob) - all other types fell back to Blob, including UDTs nested in List/Set/Map
  - Fix: Added complete type handlers for TinyInt, SmallInt, Int, BigInt, Uuid, List, Set, Map, Tuple, UDT, and Frozen types
  - Result: UDTs inside collections now show actual field values instead of `0x` blobs
  - File: `cqlite-core/src/storage/sstable/reader/parsing/value_parsing.rs` (lines 172-324)

- **Issue #239**: Nested UDTs Inside Collections Display as Hex Blobs - **FIXED**
  - Status: ✅ FIXED - Nested UDTs in collections now parse correctly
  - Root cause: Two issues:
    1. When parsing UDT field types from schema, nested UDT names were stored as `CqlType::Custom("udt:typename")` with a "udt:" prefix, but registry lookups used plain names without the prefix
    2. Inline `CqlType::Udt(name, fields)` definitions were ignored (the `fields` parameter was prefixed with `_`) and code fell back to Blob when registry lookup failed
  - Fix:
    1. Added `strip_prefix("udt:")` normalization at 6 registry lookup sites in `parse_nested_udt_from_registry()` and `parse_raw_type_value()`
    2. Added `parse_inline_udt_value()` function to parse UDTs using inline field definitions when registry lookup fails
    3. Modified all `CqlType::Udt(udt_name, inline_fields)` pattern matches to use `inline_fields` as fallback
  - Result: Nested UDTs like `contact_info.address` now show parsed field values (`{street, city, state, zip_code, country}`) instead of `0x...` blobs
  - File: `cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs`

### Completed Issues (Fixed - Dec 2025)

- **Issue #220**: UDT (User-Defined Type) Support - **FIXED**
  - Status: ✅ FIXED - UDT schema parsing and cell deserialization
  - Impact: `collections_with_udts` now passes
  - Result: All 8 test_collections tables now passing (100%)

- **Issue #221**: Complex Cell Flag Handling (0xC0-0xCF) - **FIXED**
  - Status: ✅ FIXED - Non-frozen collection parsing implemented
  - Root cause: Parser tried to read complex deletion time VInt as cell flags
  - Fix: Added `is_complex_column()` detection, `parse_complex_column()` with proper HAS_COMPLEX_DELETION handling, `skip_complex_cell()` with correct field order (flags→timestamp→deletion→ttl→path→value)
  - Key insight: Cell flags are ONLY 0x00-0x1F (5 bits). The 0xC0+ bytes were VInt data, not flags.
  - Also fixed: Added V5_0TypedCollections to block_io.rs NB format list
  - Result: `typed_collections_table` and `frozen_collections_table` now pass

- **Issue #218**: Summary.db parser format mismatch - **FIXED**
  - Status: ✅ FIXED - Complete rewrite with correct Cassandra 5.0 format
  - Root cause: Parser used wrong format (expected `version`, got `min_index_interval`)
  - Fix: Implemented correct 24-byte header, little-endian offset table, offset-based key parsing
  - Result: Summary.db now parses correctly for all 33 tables
  - Reference: `/docs/sstable-summary-format.md`

- **Issue #215 + #216**: SerializationHeader parsing - **FIXED**
  - Status: ✅ FIXED - TOC-based offset lookup implemented
  - Statistics.db/SerializationHeader now parses correctly for all 33 tables

- **Issue #210**: Static columns in SerializationHeader - FIXED
  - Status: Fixed (VInt + static column section parsing)
  - Result: `static_columns_table` now passing

- **Issue #211**: LZ4 compression chunk format - FIXED
  - Status: Fixed (correct chunk header parsing)
  - Result: 19 tables unblocked

- **Issue #212**: BTI index zero entries - FIXED
  - Status: Fixed (V5_0NewBigFormat variant handling)
  - Result: `stock_prices` now passing

- **Issue #213**: Clustering key parsing order - FIXED
  - Status: Fixed (clustering prefix before row_size)
  - Result: sensor_data, wide_partition_table, and many others now passing

### Completed Issues (Fixed - Earlier)

- **Issue #206**: V5_0FormatG Counter Support
  - Status: Fixed (1-line header routing fix)
  - Result: `counters` table now passing

- **Issue #207**: Byte-Comparable Key Encoding (CEP-25)
  - Status: Completed
  - Result: V5_0NewBigFormat (0xD4645400) now recognized

- **Issue #208**: BTI Index.db Format Support
  - Status: Completed
  - Result: Dual-parser architecture for MD5 digest + BTI formats
  - Impact: +366 LOC, Index.db parsing improved

- **Issue #209**: Component Flattening Pre-allocation
  - Status: Completed
  - Result: 55-75% performance improvement for 2-6 component keys

### Deferred Issues (M3+ Scope)

- **Issue #154**: UDT support (collections_with_udts)
  - Status: Partial implementation, blocked by Issue #210
  - Scope: M3+ feature completeness

- **Issue #162**: Statistics.db EncodingStats parsing
  - Status: Minimal parser implemented
  - Scope: M3+ metadata enhancements

- **Issue #191**: Tombstone row filtering
  - Status: Fixed (skip tombstoned rows in select_executor.rs)
  - Remaining: Expose tombstone metadata (M3+ scope)

### Infrastructure Removed

- **Issue #175**: MemTable and WAL removal
  - Rationale: Read-only library focus
  - Impact: All write operations return errors

- **Issue #176**: Compaction and manifest removal
  - Rationale: Read-only library focus
  - Impact: Compaction methods return errors

---

## M5 Write Support Limitations

CQLite M5 introduces SSTable write support, but with several intentional limitations for the initial release. The write engine produces valid Cassandra 5.0 BIG format SSTables that can be read by both CQLite and Cassandra, but takes simplified approaches in areas where full Cassandra compatibility is not critical.

### Tombstone Local Deletion Time Workaround

**Status**: ⚠️ **WORKAROUND IN PLACE** (Issue #401)
**Impact**: Tombstone cells use derived local_deletion_time instead of explicit value
**Affected Component**: `cqlite-core/src/storage/sstable/writer/data_writer.rs`

**Background**: Cassandra tombstones require two timestamp fields:
1. **Deletion timestamp** (microseconds): When the delete was issued
2. **Local deletion time** (seconds): Local server time for GC eligibility tracking

**Current Implementation**: The `Mutation` struct does not include an explicit `local_deletion_time` field. The writer derives it from the deletion timestamp:

```rust
// data_writer.rs:416
let local_deletion_time = (mutation.timestamp_micros / 1_000_000) as i32;
```

**Rationale**:
- For immediate deletes, local_deletion_time = deletion_time / 1000 is correct
- GC semantics are deferred (M6+ scope)
- SSTable compaction (which uses local_deletion_time) is out of scope for M5

**Future Fix** (M6+):
- Add `local_deletion_time: Option<i32>` to `Mutation` struct
- Allow explicit setting via API: `mutation.with_deletion_time(timestamp, local_deletion_time)`
- Default to derived value if not specified

**Tracking**: Issue #401 (RESOLVED with workaround)

---

### IndexWriter Memory Buffering

**Status**: ⚠️ **ACCEPTABLE TRADE-OFF** (Issue #408)
**Impact**: Index.db entries are buffered in memory (not streamed to disk)
**Affected Component**: `cqlite-core/src/storage/sstable/writer/index_writer.rs`

**Current Implementation**: The `IndexWriter` uses a `Vec<u8>` buffer that holds all index entries in memory until `finish()` is called:

```rust
// index_writer.rs:90-91
buffer: Vec<u8>,  // Serialized index data (written incrementally)
```

**Memory Usage**:
- Each entry: 20-22 bytes (marker + digest + VInt offset + promoted_length)
- 1M partitions: ~20 MB
- 10M partitions: ~200 MB
- Memory grows linearly with partition count

**Why Not True Streaming?**:
1. **Summary.db sampling requires accurate offsets**: Summary.db samples every Nth index entry and needs the exact byte offset in Index.db where each entry was written. This requires knowing Index.db positions before the file is complete.
2. **Offset tracking complexity**: True disk streaming would require:
   - Flushing partial buffers during writes
   - Complex offset calculation across buffer boundaries
   - Synchronous I/O in async context

**Trade-Off Analysis**:
- ✅ **Acceptable**: 200 MB for 10M partitions is reasonable for modern systems
- ✅ **Simplicity**: Vec buffer provides O(1) append and accurate offset tracking
- ❌ **Not suitable for**: Billion-partition SSTables (20 GB+ memory)

**Workaround**: For extremely large SSTables, split writes into multiple generations.

**Future Optimization** (M6+):
- Implement true streaming with buffered I/O
- Add configurable buffer size with periodic flushes
- Track cumulative offsets across buffer boundaries

**Tracking**: Issue #408 (documented limitation)

---

### Promoted Index Deferred

**Status**: ⏳ **DEFERRED** (M5 Stage 0 scope)
**Impact**: Wide partitions (many clustering keys) cannot use fast within-partition seeks
**Affected Component**: `cqlite-core/src/storage/sstable/writer/index_writer.rs`

**Current Implementation**: Index.db entries always write `promoted_index_length = 0`:

```rust
// index_writer.rs:168-169
// Write promoted index length (0 = no promoted index)
encode_unsigned(0, &mut self.buffer);
```

**What Is Promoted Index?**:
Promoted index is Cassandra's optimization for wide partitions (partitions with many clustering keys). It stores sampled clustering key ranges within a partition, enabling O(log n) seeks to specific rows instead of O(n) sequential scans.

**Example Use Case**:
```sql
-- Wide partition: 10,000 rows per user_id
CREATE TABLE user_activity (
    user_id int,
    timestamp timestamp,
    activity text,
    PRIMARY KEY (user_id, timestamp)
);

-- Without promoted index: Must scan all 10K rows
SELECT * FROM user_activity WHERE user_id = 1 AND timestamp > '2025-01-01';

-- With promoted index: Jump directly to 2025-01-01 range
```

**Impact**:
- ✅ **No impact**: Simple tables, narrow partitions (< 100 rows per partition)
- ⚠️ **Minor impact**: Medium partitions (100-1000 rows) - sequential scan still fast
- ❌ **Significant impact**: Wide partitions (10K+ rows) - linear scan required

**Rationale for Deferral**:
- M5 Stage 0 focuses on correctness, not performance optimizations
- Promoted index format is complex (requires sampling logic and offset tracking)
- 95% of use cases have narrow partitions
- Sequential scan is functionally correct, just slower

**Future Implementation** (M5.1+):
1. Detect wide partitions (> 1000 rows) during write
2. Sample clustering keys every N rows (e.g., every 256 rows)
3. Encode promoted index block: `[num_entries][entry_1]...[entry_n]`
4. Each entry: `[clustering_key][row_offset]`
5. Write promoted_index_length and data to Index.db

**Tracking**: M5.0 Stage 0 scope decision (no issue filed)

---

### ~~Statistics.db Minimal Format~~ - RESOLVED

**Status**: ✅ **RESOLVED** (M5.1)
**Resolution**: Full Cassandra 5.0 TOC format implemented

The `StatisticsWriter` now produces complete Cassandra 5.0 compatible Statistics.db files with:

**Implemented**:
- Full TOC header (4 bytes count + CRC32 checksums + component offsets)
- VALIDATION component (partitioner, bloom filter FP chance)
- COMPACTION component (HyperLogLogPlus cardinality estimator)
- STATS component (min/max timestamps, TTL, deletion times, row/column counts, histograms)
- SERIALIZATION_HEADER component (schema-derived partition keys, clustering keys, column names/types)

**Current Limitations**:
- Column bitmap encoding limited to 64 columns (VUInt format)
- STATS histograms use minimal valid values (2 buckets, empty tombstone histogram)
- COMPACTION cardinality estimator uses empty HyperLogLogPlus sketch

**Impact**: Statistics.db files are fully compatible with Cassandra 5.0. When schema is provided via `write()`, SerializationHeader contains full column metadata. When schema is None, uses minimal stub format.

**Files**:
- `cqlite-core/src/storage/sstable/writer/stats_writer.rs` (complete implementation)

**Related Issues**: Issue #425 (Statistics.db checksums and format - FIXED)

---

### ~~CompressionInfo.db Not Implemented~~ - RESOLVED

**Status**: ✅ **RESOLVED** (M5.1)
**Resolution**: Full compression support implemented in M5.1

M5.0 produced only uncompressed SSTables. M5.1 implements full compression support via:
- `CompressedDataWriter`: Chunk-based compression with LZ4/Snappy/Deflate/Zstd
- `CompressionInfoWriter`: Compression metadata file generation
- Trailing CRC32 checksums per chunk
- Feature-gated compression algorithms

See "M5.1 Write Support Capabilities" section at the top of this document for details.

**Files Added**:
- `cqlite-core/src/storage/sstable/writer/compressed_data_writer.rs`
- `cqlite-core/src/storage/sstable/writer/compression_info_writer.rs`

---

## Key Takeaways

- **Pass rate: 100% (33/33 tables)** - COMPLETE! All test tables now passing
- All SSTable component parsers (Data.db, Index.db, Summary.db, Statistics.db) now use correct formats
- All data types fully supported: basic types, collections, UDTs, frozen types, complex cells
- **M5.1 Write Support**: Feature-complete with documented trade-offs
  - ✅ CompressionInfo.db: Full compression support (LZ4, Snappy, Deflate, Zstd)
  - ✅ Collection serialization: Frozen and non-frozen collections
  - ✅ Static columns: Extended flags format with EXTENDED_IS_STATIC
  - ✅ Composite partition keys: Multi-component encoding
  - ✅ Delta encoding: Statistics.db baseline for timestamps/TTL
  - ⚠️ Issue #401: Tombstone local_deletion_time derived from timestamp
  - ⚠️ Issue #408: IndexWriter uses Vec buffer (not true disk streaming)
  - ⏳ Promoted Index deferred (length=0 in all entries)
  - ⚠️ Statistics.db minimal format (hybrid header, not full TOC)
- **All read-side feature gaps closed**:
  - ✅ Issue #219: Frozen type support
  - ✅ Issue #220: UDT (User-Defined Type) support
  - ✅ Issue #221: Complex cell flag handling for non-frozen collections
- **Milestone achieved**: M5.1 completion (write support with compression, collections, static columns)

---

## References

- Validation Matrix: `test-data/validation-matrix.md`
- Smoke Test Script: `test-data/scripts/smoke-test-all-tables.sh`
- Issue Tracker: https://github.com/pmcfadin/cqlite/issues
- Integration Tests: `cqlite-core/tests/*.rs`
- Feature Flags: `cqlite-core/Cargo.toml` [features] section

---

## Cross-Links

- Appendix B — [On-Disk Encodings Cheat Sheet](appendix-b-encodings-cheat-sheet.md) - Binary format details
- Appendix C — [Reference Walkthroughs with Code](appendix-c-walkthroughs.md) - Parsing examples
- Appendix D — [Tools & Workflows](appendix-d-tools-and-workflows.md) - sstabledump usage
- Chapter 8 — [Statistics.db](08-statistics-db.md) - SerializationHeader format
- Chapter 17 — [BTI Formats](17-bti-formats.md) - BTI index structure
