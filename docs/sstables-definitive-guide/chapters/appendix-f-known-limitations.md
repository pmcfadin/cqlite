# Appendix F — Known Limitations

This appendix documents current capabilities, parsing limitations, validation status, and workarounds in CQLite's SSTable implementation. It serves as a reference to prevent repeated investigation of known issues and provides clear guidance for contributors.

**In this appendix you will learn:**
- Write support capabilities and the one hard claim boundary (uncompressed SSTables only, #1406)
- Which SSTable formats and table types have parsing issues
- Current validation pass rates across test datasets
- Feature gaps and remaining limitations
- Practical workarounds for common limitations
- Issue tracking references for ongoing fixes

---

## Write Support Capabilities

CQLite ships SSTable write support (`write-support`, a **default** feature of `cqlite-core`) with the
following capabilities. See "Write support posture (current)" below for the full picture — flush,
STCS compaction, export — and the one hard claim boundary (uncompressed only, #1406).

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

These building blocks are used solely to synthesize compressed SSTables for exercising the decompressing reader. The CompressionInfo.db binary format CQLite actually *parses* on read (see `cqlite-core/src/storage/sstable/compression_info.rs:132-249`, mirroring Cassandra's `CompressionMetadata.java:375-392`) is, in exact on-disk order:
- Compressor simple name — Java `writeUTF`: BE `u16` byte-length prefix followed by the UTF-8 name (e.g. `LZ4Compressor`)
- `option_count` — BE `i32`
- `option_count` × option pairs — each pair is two `writeUTF` strings (key, then value), each a BE `u16` length prefix + UTF-8 bytes
- `chunk_length` — BE `i32`, the uncompressed chunk size (default 64 KB)
- `max_compressed_length` — BE `i32` (present on all Cassandra 5.0 / version ≥ `na` files; equals `i32::MAX` when `minCompressRatio=0`)
- `data_length` — BE `i64`, total uncompressed data length
- `chunk_count` — BE `i32`
- `chunk_count` × chunk offset — BE `i64` per chunk, the byte offset of each compressed chunk record in Data.db

CompressionInfo.db ends immediately after the chunk offset table — it contains **no CRC bytes**. The per-chunk CRC32 checksums live inline in Data.db: each compressed chunk is followed by a 4-byte big-endian CRC32 of its compressed bytes (`CompressedSequentialWriter.java:192`), so consecutive chunk offsets differ by `compressedLength + 4`. There is likewise no trailing metadata CRC32 in CompressionInfo.db.

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

### Zero-length collection element value: `HAS_EMPTY_VALUE` on the whole-column writers

**Status**: KNOWN GAP (Issue #2970)

Cassandra decides the presence of a cell's value length + bytes from the `HAS_EMPTY_VALUE` flag
(`0x04`), which it sets from `cell.valueSize() > 0` — a **size** test, not a type test
(`Cell.java:271`, `:277-278`, `:303-304`; read side `:310`, `:329-339`). A zero-length value is
therefore written as `flags |= 0x04` with **no** length VInt and **no** bytes.

CQLite implements that rule correctly on:

- the reader — `.../reader/parsing/row_decoder/complex_column.rs::parse_complex_cell_value()`
  (`:1012`, `:1128`) reads a value length only when neither `IS_DELETED` nor `HAS_EMPTY_VALUE` is set;
- the per-element writer — `.../writer/data_writer/complex.rs::write_complex_element_cell()`
  (`:884-891`, `:960-969`) derives the flag and emits the length only when it is clear;
- `write_set_complex_cells()` (`:594`), which always sets `CELL_HAS_EMPTY_VALUE` and writes no value.

**The gap**: the *whole-column* writers `write_map_complex_cells()` (`:646`) and
`write_list_complex_cells()` (`:705`) hardcode `flags = 0` (`:683`, `:737`) and call
`encode_unsigned(len)` unconditionally (`:694`, `:747`). An element whose serialized value is
zero-length — a `map<text,text>` entry with value `''`, an empty blob in a `list<blob>` — is emitted as
`flags=0` + `0x00` where Cassandra emits `flags=0x04` + nothing.

**Impact — a byte-parity divergence, not a framing one.** Cassandra **reads these bytes without error**:
its deserializer is symmetric with its serializer, so on `flags=0` it computes `hasValue = true`
(`Cell.java:310`), takes the variable-length branch `int l = in.readUnsignedVInt32()`
(`AbstractType.java:590`) which consumes our `0x00` as `l=0`, and `accessor.read(in, 0)` short-circuits
to `EMPTY_BYTE_BUFFER` before allocating or reading any bytes (`ByteBufferUtil.java:444-448`). The
stream stays byte-aligned and the decoded value is identical (empty). **This is not corruption.**

What it does break is byte-level equality with Cassandra's output. For the same logical row CQLite emits
two divergences: one spurious `0x00` length byte, and a `flags` byte differing by `0x04`. Consequences:

- **byte-for-byte compaction parity** — the v0.12 guarantee does not hold for such a row;
- **digests** — `Digest.crc32` over the cell bytes diverges, so a CQLite-written SSTable will not
  digest-match Cassandra's for identical data;
- **`row_size` / `prev_size` accounting** — the row is one byte longer than Cassandra would write it.

Non-empty values, sets, and the per-element path are unaffected.

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

## Resolved Early-Write-Support Limitations

These were limitations of the first write-support drop (M5.0), resolved in M5.1:

### CompressionInfo.db Writing

**Status**: NOT IMPLEMENTED for production writes (building blocks are test-only, fail-closed — #1406)

CQLite's production SSTable writer emits uncompressed Data.db only and never writes a CompressionInfo.db. The `CompressedDataWriter` / `CompressionInfoWriter` types exist solely to synthesize compressed fixtures for exercising the decompressing reader; they are UNWIRED (no flush/compaction path reaches them) and any attempt to configure compressed production writing returns `Error::UnsupportedFormat`.

The READ/decompression path fully supports all four compression algorithms (LZ4, Snappy, Deflate, Zstd) — CQLite reads compressed Cassandra SSTables end-to-end. See the "CompressionInfo.db Writing" entry under "Write Support Capabilities" above for the exact fail-closed boundary and the parseable on-read format.

### Collection Serialization

**Status**: RESOLVED (Issues #377, #378)

The first drop had limited collection support; now implemented:
- Frozen collection serialization (single-cell format)
- Non-frozen collection serialization (multi-cell complex columns)
- Proper flag handling (`ROW_HAS_COMPLEX_DELETION`)

### Static Column Support

**Status**: RESOLVED (Issue #379)

The first drop did not support static columns; now implemented:
- Static row writing with extended flags
- Proper ordering (static rows before regular rows)
- Correct column bitmap handling for static columns

---

## Compaction and Export Capabilities

Compaction executes end-to-end. v0.12 delivered **byte-for-byte STCS compaction parity** against
Apache Cassandra; the old "execution pending M5.3 reader integration" caveat no longer applies.

### K-Way Merge (Issue #382)

**Status**: ✅ **IMPLEMENTED** — this is the production compaction merger

K-way merge infrastructure for combining multiple SSTables:
- Binary heap-based merge with O(log k) per entry
- Schema-aware clustering key comparison
- Per-cell / per-cell-path reconciliation (epic #921)

`KWayMerger::new` (`cqlite-core/src/storage/write_engine/merge/mod.rs:2530`) delegates to
`new_with_gc` (`:2570`) and returns a working merger; `new_cancellable` (`:2539`) additionally
wires a cooperative `ScanCancel` into every input reader's compaction scan (#2264).

### STCS Merge Policy (Issue #383)

**Status**: ✅ **IMPLEMENTED** and usable

Pluggable compaction strategy via the `MergePolicy` trait
(`cqlite-core/src/storage/write_engine/merge_policy.rs`):
- `STCSPolicy` (`:80`): Size-Tiered Compaction Strategy (Cassandra default)
  - Bucket grouping by size ratio with inclusive `>=`/`<=` bounds (`:191-192`)
  - Configurable min/max thresholds, validated in `STCSPolicy::new` (`:100`)
- Custom policies via `Box<dyn MergePolicy>`

`WriteEngine::set_merge_policy` (`cqlite-core/src/storage/write_engine/maintenance.rs:321`) stores
the policy and returns `Ok(())`.

### Maintenance Step API (Issue #384)

**Status**: ✅ **IMPLEMENTED** — flush **and** compaction

`WriteEngine::maintenance_step` (`cqlite-core/src/storage/write_engine/maintenance.rs:462`,
inner at `:491`) performs background compaction work within a time budget, driving the merge state
machine and the gc_grace / overlap-aware purge logic (#921 / #935 / #1388 / #2299):
- Non-blocking, budget-limited execution (a ~10% budget tolerance is recorded to observability)
- Returns `MaintenanceReport` with maintenance stats
- Suitable for background thread scheduling

### TTL and Expiring Cells (Issue #386)

**Status**: ✅ **IMPLEMENTED**

TTL support for expiring data:
- TTL delta encoding against the Statistics.db baseline
  (`cqlite-core/src/storage/sstable/writer/data_writer/cells.rs:186`), which rejects a negative
  delta with a descriptive error (`:187-193`)
- Expiration-time derivation via `expiring_local_deletion_time` (`cells.rs:261`)

### SSTable Export API (Issue #388)

**Status**: ✅ **IMPLEMENTED**

`WriteEngine::export_sstable` (`cqlite-core/src/storage/write_engine/export.rs:280`) for
distribution:
- Cassandra-compatible naming: `{keyspace}-{table}-nb-{gen}-big-{Component}.db`
- Optional compaction before export
- Component validation (Data.db, Index.db, Statistics.db, etc.)
- Fully async I/O — `tokio::fs::create_dir_all` (`:307`) and an async
  `find_most_recent_sstable` (`:437`)

---

## Remaining Write-Path Limitations

### Promoted Index

**Status**: ✅ **IMPLEMENTED** (Issue #993)

Wide partitions get a real promoted index. `SSTableWriter` collects promoted-index blocks during the
Data.db pass for partitions with **≥ 64 KiB** of row data
(`cqlite-core/src/storage/sstable/writer/mod.rs:687-693`) and passes them to
`IndexWriter::add_partition_with_promoted`
(`cqlite-core/src/storage/sstable/writer/index_writer.rs:380`, called from `writer/mod.rs:725` and
`writer/incremental.rs:168`). The writer gates emission on **≥ 2** blocks, mirroring Cassandra's
`RowIndexEntry.create()` which only builds an indexed entry when `columnIndexCount > 1`
(cassandra-5.0.8 `io/sstable/format/big/RowIndexEntry.java:234`).

The read side consumes it: `PromotedIndexData` (`index_reader/mod.rs:80`) carries the raw payload
and `decode_partition_promoted_index` (`reader/data_access/big_promoted.rs:268`) decodes it for a
within-partition seek.

### BTI Format Writing

**Status**: IMPLEMENTED (canonical `da` BTI write since v0.12 — #872)

CQLite emits canonical BTI (`da`) format SSTables, including trie-based `Partitions.db` and `Rows.db`. BIG (`nb`) remains the DEFAULT write target; BTI (`da`) is an explicit, supported alternative. BTI read is fully supported end-to-end.

**Rationale**: BIG format covers the majority of production use cases and remains the default, while `da` BTI write/read achieves byte-parity with Cassandra 5.0 for callers that select it.

### Index.db/Summary.db Full Format

**Status**: ✅ **IMPLEMENTED**

- Index.db: key + VInt data offset + VInt promoted-index length, with a real promoted-index payload
  for wide partitions (see "Promoted Index" above). Entry framing:
  `cqlite-core/src/storage/sstable/index_reader/parse.rs:161-162`.
- Summary.db: sampled entries with correct offset tracking
  (`cqlite-core/src/storage/sstable/summary_reader/`).

BTI (`da`) trie format write/read IS supported (see "BTI Format Writing" above).

### Statistics.db Full TOC Format

**Status**: ✅ **IMPLEMENTED**

The `StatisticsWriter` (`cqlite-core/src/storage/sstable/writer/stats_writer/`) produces a full
Cassandra 5.0 compatible Statistics.db with complete TOC structure:

**Implemented**:
- Full TOC header with component count and CRC32 checksums
- VALIDATION component (partitioner class name, bloom filter FP chance)
- STATS component (EncodingStats with min/max timestamps, TTL, deletion times, histograms).
  The `EstimatedHistogram`s are Cassandra-canonical, not stubs: `estimatedPartitionSize` uses 156
  buckets and `estimatedCellPerPartitionCount` 119 buckets (issue #1327,
  `stats_writer/components.rs:493-496`; accumulator in `stats_writer/estimated_histogram.rs`), and
  `estimatedTombstoneDropTime` is populated for every tombstone local-deletion-time observed during
  the write (`stats_writer/metadata.rs:257-264`, which correctly excludes the
  `DeletionTime.LIVE` sentinel per #851).
- SERIALIZATION_HEADER component (schema-derived or minimal stub)

**Known Limitation**:
- COMPACTION component writes a hardcoded **empty** HyperLogLogPlus sketch — no cardinality data
  (`stats_writer/components.rs:63-92`, a minimal valid `HyperLogLogPlus(p=11, sp=25)` SPARSE
  sketch).

**Wide tables (>64 columns) — supported (Issue #763)**:
The SERIALIZATION_HEADER column sets are written exactly as Cassandra's
`SerializationHeader.Serializer.writeColumnsWithTypes` (cassandra-5.0.8
`src/java/org/apache/cassandra/db/SerializationHeader.java:489-497`): an unsigned-VInt column count
followed by `count` `(VInt-length name, VInt-length marshal type)` pairs. This path has **no**
64-column limit. A previous note here claimed a "64-column bitmap" cap; that was inaccurate — the
64-bit bitmap encoding belongs to `Columns.Serializer.serializeSubset` (cassandra-5.0.8
`src/java/org/apache/cassandra/db/Columns.java:503-531`), which serialises a per-row column
*subset* against a pre-shared superset (Data.db rows / inter-node messaging) and is never used for
the SSTable header. Tables with 65+ columns therefore round-trip losslessly; regression coverage
lives in `writer/stats_writer/serialization_header.rs::test_serialization_header_70_columns_roundtrip`
(`:445`) and `::test_serialization_header_200_columns_count_is_vint` (`:501`).

**Impact**: Statistics.db files are fully compatible with Cassandra 5.0. Schema can be provided explicitly for richer SerializationHeader, or omitted for minimal stub format.

---

## Parsing Limitations

### Snapshot header-identity extraction — ID-less snapshot dirs

**Status**: 🐛 **OPEN** (limitation) — ID-ful (Cassandra-shaped) snapshots fully resolved (#2384);
ID-less snapshot dirs still misparse
**Impact**: Logging + sync schema-fallback identity only (does NOT affect the ticket-derived warm-cache key)

Snapshot-aware path parsing (`cqlite-core/src/storage/sstable/snapshot_path.rs`) resolves the
real `{keyspace}.{table}` identity from a Data.db path by walking up past a
`snapshots/{tag}/` layer. Detection is guarded by the structural
`{table_name}-{32-hex}` shape (`is_table_id_dir`) so it triggers ONLY for
Cassandra-shaped, ID-ful snapshot directories
(`.../{ks}/{table}-{id}/snapshots/{tag}/...-Data.db`). This correctly:
- resolves the real `{keyspace}.{table}` for ID-ful Cassandra snapshots, and
- avoids the false-trigger for an ordinary table living in a keyspace literally
  named `snapshots` (`.../data/snapshots/{table}-{id}/...-Data.db`).

**Residual limitation**: CQLite's own write engine emits **ID-less** table
directories `{keyspace}/{table}/` (no `-{uuid}` suffix —
`cqlite-core/src/storage/sstable/writer/mod.rs:470`). When
those are snapshotted (e.g. the flight producer's
`reads_from_snapshot_directory` path, `cqlite-flight/src/producer.rs:2391`), the read path is
`{ks}/{table}/snapshots/{tag}/...-Data.db`. Because `is_table_id_dir` requires a
`-{32-hex}` suffix, an ID-less snapshot dir does NOT match the guard, the walk-up
is skipped, and the header-identity misparse persists: keyspace resolves to
`snapshots` and table to `{tag}`.

This is **inherently unresolvable from the path alone** — an ID-less snapshot
`{ks}/{table}/snapshots/{tag}/` and an ordinary table in a keyspace literally
named `snapshots` (`{data}/snapshots/{table}/`) are structurally identical. Only
external authoritative keyspace/table context can disambiguate them.

**Blast radius**: header keyspace/table are used for logging and the sync
schema-fallback path; the ID-less misparse does NOT corrupt the ticket-derived
warm-cache key.

The current (limited) behavior is **deliberately pinned** by
`snapshot_path.rs::idless_snapshot_currently_unresolved_pending_followup` (`:230`), whose doc
comment states the assertion should FLIP when the robust fix (authoritative keyspace/table threaded
into `SSTableReader::open`) lands. That pin, not this appendix, is the authority on whether the
limitation is still live: as of this writing it still asserts the misparse.

**Tracking**: Issue #2384 (structural fix, shipped). The robust-fix follow-up issue is CLOSED, but
the code pin above is unchanged — treat this as an open limitation until that assertion flips.

---

### ~~Static Column Support (Exit Code 3)~~ - FIXED

**Status**: ✅ **FIXED** (Issue #210)
**Impact**: Was 1 table - now 0 (SerializationHeader extraction works for static column tables)
**Resolution**: Fixed SerializationHeader parser to handle static columns section

**Root Cause Found**: The SerializationHeader format includes a static column section between clustering keys and regular columns. The parser was treating the `static_count` byte as a separator (expecting `0x00`), which only worked when there were no static columns.

**Correct Format** (confirmed via cassandra-5.0.8
`src/java/org/apache/cassandra/db/SerializationHeader.java:458-459`):
```
[pk_type] [ck_count] [ck_types...] [static_count] [static_columns...] [reg_count] [regular_columns...]
```

When `static_count = 0`, it encodes as `0x00`, making simple tables work. But when `static_count > 0`, parsing would fail.

**Fix**: Modified the SerializationHeader parse path in
`cqlite-core/src/parser/enhanced_statistics_parser/` (now split into `header.rs`,
`encoding_stats.rs`, `marshal_type.rs`, and `serialization_header/`) to:
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
**Resolution**: Complete rewrite of the Summary.db reader
(`cqlite-core/src/storage/sstable/summary_reader/`) with the correct Cassandra 5.0 format

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
- File: `cqlite-core/src/storage/sstable/reader/parsing/row_decoder/` — the row framing now lives in
  `row_framing.rs` (`parse_row_metadata` at `:394`)

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
**Resolution**: Corrected format-version dispatch on the block-read path plus BTI inter-entry padding handling

**Root Cause Found**: Two issues combined to cause silent data loss:

1. **Format-version dispatch**: the block reader classified the table's version into the legacy
   block-header path (which returns EOF immediately) instead of the chunk-based read path, so the
   index yielded zero entries with no error.
2. **BTI Inter-Entry Padding**: BTI Index.db entries have variable padding bytes between them (null or non-null). The parser needed enhanced padding skip logic to find valid entry boundaries.

> **Implementation note (CQLite-specific).** Both fixes long predate the epic #1116 module splits,
> and the code named in the original writeup has since been rewritten and relocated
> (version classification now lives in `cqlite-core/src/parser/header.rs`; block reads in
> `cqlite-core/src/storage/sstable/reader/block_io.rs`; the index reader in
> `cqlite-core/src/storage/sstable/index_reader/`). The specific match arms and helper functions
> quoted in the original 2025 writeup no longer exist under those names, so they are not reproduced
> here — the durable record is the outcome below.

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

**Root Cause Found**: When BTI Index.db parsing is incomplete (returns 0 partition entries), the scan path in `reader/data_access/` would:
1. Take the index-based path (since `self.index.is_some()`)
2. Get 0 entries from `get_range()`
3. Check `has_zero_size` which is `false` (no entries to check)
4. Return empty results without falling back to sequential scan

**Symptom**: `SELECT * FROM test_timeseries.time_bucketed_counters` returned 0 rows despite containing 41 rows.

**Fix Details**:
- Added a check for empty index entries BEFORE the `has_zero_size` check
- When the index yields no entries, the scan falls back to a sequential Data.db scan
- Sequential scan correctly parses Data.db directly, bypassing index issues

**Current shape of that guard** (`cqlite-core/src/storage/sstable/reader/data_access/full_index_scan.rs:102`):
a zero-entry index is now treated as *structurally unusable*, never as a legitimately empty
SSTable — the full-index path returns `Ok(None)` and the caller falls back with a loud WARN
(`IndexReader::open` already rejects a zero-byte Index.db as corruption, and neither Cassandra
nor `SSTableWriter` emits a zero-partition SSTable). The sequential fallback itself lives in
`data_access/sequential.rs`.

**Results**:
- `time_bucketed_counters` now returns 41 rows via sequential scan fallback
- No regression on tables using DigestFormat index

**Tracking**: Issue #256 (CLOSED)

---

### BTI End-to-End Support (Issue #36 → resolved by v0.12 #872)

**Status**: ✅ **RESOLVED** (canonical `da` BTI write/read since v0.12 — #872)
**Note**: The narrative below is historical (the original Issue #36 deferral). It has since been superseded: BTI read is fully supported end-to-end, and CQLite emits canonical `da`-format SSTables (trie-based `Partitions.db`/`Rows.db`) with byte-parity vs Cassandra 5.0. BIG (`nb`) remains the DEFAULT write target.

**Historical background**: Issue #36 originally requested comprehensive BTI validation including:
- TDD tests for trie traversal lookups and iteration
- Rows.db decoding tests with range tombstones and complex types
- Round-trip byte-comparable invariants
- Zero-diff vs sstabledump on BTI datasets

**Historical findings** (at time of deferral):
1. **BIG format is Cassandra 5.0 default** - BTI requires explicit opt-in via `selected_format: bti` in cassandra.yaml
2. **Test data used BIG format** - the original SSTable corpus used the `nb-` prefix (BIG format)
3. **BTI is opt-in** - Cassandra 5.0 marks BTI as opt-in

**Current implementation** (`cqlite-core/src/storage/sstable/bti/`):
- ✅ Format detection (magic number `0x6461`)
- ✅ Byte-comparable encoding (CEP-25 compliant)
- ✅ Trie node structures (all 4 types)
- ✅ SizedInts encoding
- ✅ Trie traversal (fully implemented)
- ✅ Range queries and full partition iteration
- ✅ Canonical `da` BTI write (`Partitions.db`/`Rows.db`) with Cassandra byte-parity (#872)

**Reference**: `docs/sstables-definitive-guide/references/bti-v1-status.md`

**Tracking**: Issue #36 (resolved by v0.12 #872)

---

### Table ID Matching in scan_for_key() (Issue #36 Follow-up - FIXED)

**Status**: ✅ **FIXED** (Issue #36 regression fix)
**Impact**: Was causing zero-row results when table names had qualified vs unqualified mismatch
**Resolution**: Updated `scan_for_key()` to use `table_ids_match()` function

**Root Cause Found**: The `scan_for_key()` function used direct equality (`==`) to compare table IDs, which failed when:
- Query used qualified name (e.g., `test_basic.simple_table`)
- SSTable stored unqualified name (e.g., `simple_table`)
- Or vice versa

**Symptom**: CLI queries returned zero rows with debug log showing `table_id mismatch ('test_basic.simple_table' != 'simple_table')`.

**Fix Details**:
- Changed: `entry_table_id == *table_id` → `table_ids_match(&entry_table_id, table_id)`
- `table_ids_match()` handles qualified/unqualified name comparison correctly

**Current locations** (post epic #1116 split): `scan_for_key` at
`cqlite-core/src/storage/sstable/reader/data_access/sequential.rs:680` and `sequential_scan` at
`sequential.rs:815`; the comparator `table_ids_match` at `data_access/model.rs:213`. A stricter
sibling, `table_ids_match_strict` (`model.rs:251`), additionally requires the keyspace to match
when both sides carry one (#1284) and is used on the BTI point-lookup path.

**Note**: `sequential_scan()` already used `table_ids_match()` correctly. This fix aligned `scan_for_key()` with the same matching logic.

**Tracking**: Issue #36 (comment thread, Jan 2026)

---

### ~~BTI Metadata Offset Extraction~~ — IMPLEMENTED

**Status**: ✅ **IMPLEMENTED** (Issues #226 and #208 both CLOSED)
**Was**: deferred as an M3+ performance optimization; BTI point lookups fell back to a sequential scan.

**Format** (Cassandra 5.0 BTI, `da`): a `Partitions.db` trie leaf's payload is
`[hash_byte: 1 byte][position: N bytes]` in **SizedInts** encoding (not VInt), where `N` derives
from the `payloadBits` low nibble of the node header. A **negative** encoded position means the
payload is a direct `Data.db` offset (`data_offset = ~position`, a NARROW partition); a
**non-negative** position is an offset into `Rows.db` for that partition's row-level trie index (a
WIDE partition).

**Example from a `stock_prices` `Partitions.db` leaf**:
```text
00 00 04 80 00 4f 88 00
^  ^-----------^
│  └─ Position bytes (SizedInts)
└─── Hash byte (filter hash lower 8 bits)
```

**Implementation** — both formerly-pending pieces now exist:
- ✅ SizedInts decoder (`cqlite-core/src/storage/sstable/bti/sized_ints.rs`)
- ✅ Trie node header parsing — `payload_flags = header_byte & 0x0F`
  (`bti/parser/node_decode.rs:313`) feeds the payload decoders
- ✅ Direct offset extraction — `decode_bti_partition_payload(trie_data, payload_start, payload_bits)`
  (`bti/parser/partitions.rs:78`) returns
  `BtiPartitionLocation::DataOffset(u64)` / `RowsOffset(u64)` (`partitions.rs:47-58`), with
  `payload_bits` range-validated fail-closed (`:81-91`) and
  `position_bytes = payload_bits - FLAG_HAS_HASH_BYTE + 1` (`:97`). The row-level analogue is
  `bti/parser/rows.rs:207`.
- ✅ Consumed on the read path: `reader/data_access/bti.rs:333` uses a `RowsOffset` to seek the wide
  partition's `Rows.db` entry directly; the writer's inverse is
  `writer/partitions_writer.rs:632`.

**Tracking**: Issue #226 (CLOSED), Issue #208 (CLOSED)

---

### ~~Index.db VInt Offset Parsing (NB Tables)~~ - FIXED

**Status**: ✅ **FIXED** (Issue #237, CLOSED 2026-01-06)
**Was**: 83% of partitions skipped in 7 `test_timeseries` tables (~827 partitions), falling back to a
sequential Data.db scan with "malformed partition" warnings.

**Root Cause**: the Index.db parser read the entry's offset as a length-prefixed byte run — treating
the first byte of the offset as an `offset_len` and then taking that many bytes. Cassandra 5.0 NB
encodes the offset as an unsigned VInt directly, so a `0x00` first byte was read as `offset_len=0`,
the cursor advanced one byte short, and every following entry failed to parse.

**Authoritative entry layout** (BIG Index.db; see also Issue #552 and guide Ch.6, and the
`parse_all_partition_entries` doc comment at
`cqlite-core/src/storage/sstable/index_reader/parse.rs:150-175`):
```text
[key_len: u16 BE]                    ← length of the raw partition key
[raw partition key bytes: key_len]   ← the partition key exactly as in Data.db
[data_offset: unsigned vint]         ← byte offset into the Data.db data section
[promoted_index_len: unsigned vint]  ← byte length of the promoted index (0 = none)
[promoted_index_data: promoted_index_len bytes]
```

> **Correction to the original 2026-01 writeup.** That writeup described the entry as
> `marker(0x0010) + 16-byte MD5 digest + vint_offset`. There is no `0x0010` marker and no on-disk
> digest: the leading `u16` is the partition-key LENGTH (single-UUID keys happen to start `0x0010`
> = 16, which is where the "marker" reading came from; the composite-key `multi_partition_table`
> starts `0x0026` = 38). There is likewise no separate "BTI Index.db format" — a BTI-indexed
> SSTable emits `Partitions.db`/`Rows.db` tries and no Index.db at all.

**Fix**: offsets are decoded with the unsigned-VInt reader (`cqlite-core/src/parser/vint.rs`;
`parse_vuint` is used for both the data offset and the promoted-index length at
`index_reader/parse.rs:349-350`, and the framing-only fast path is
`parse_big_index_entry_framing` at `:345`). Index.db offsets are relative to the Data.db data
section, so the reader adds the header size. The inverse encoder (`parser::vint::encode_vuint`) is
used to build fixtures in `index_reader/lazy.rs:385-386`.

**Regression pin**: `cqlite-core/tests/issue_237_row_size_offset_regression_test.rs`.

**Tracking**: Issue #237 (CLOSED)

---

## Epic #817 — Compaction-fidelity gaps (verified)

These limitations were surfaced and byte-verified during Epic #817 (compaction
fidelity). Each is grounded in CQLite's own reader/writer or a cited Cassandra
class; where CQLite diverges from Cassandra it is called out explicitly.

> **Note on the numbering.** The parenthesised numbers in the headings below (`cursor-finding 4`,
> `12`, `14`, … `25`) are the ordinals of the findings in Epic #817's own audit list. They are
> **not** GitHub issue numbers — GitHub issues are always written `#<n>` in prose (e.g. #824, #921).

### Reader lacks the `≥ 64`-column large-subset decode branch (cursor-finding 12)

**Status**: 🐛 **OPEN**

When `HAS_ALL_COLUMNS` (0x20) is clear, Cassandra's `Columns.Serializer.serializeSubset`
(cassandra-5.0.8 `src/java/org/apache/cassandra/db/Columns.java:503-531`) selects the
columns-subset encoding by superset size: a single
unsigned-VInt bitmap of *missing* columns for `< 64` regular columns, and a **large-subset** form
for `≥ 64` — `serializeLargeSubset` (`Columns.java:609-639`) writes
`supersetCount - columnCount` as an unsigned VInt, then the smaller of the present/missing sets as
**absolute** column indices (`iter.indexOfCurrent()`), each an unsigned VInt. (Note: the method's
own doc comment says "deltas"; the code writes absolute indices — the code is authoritative.)
CQLite's reader
(`reader/parsing/row_decoder/row_framing.rs::parse_row_metadata`, `:394`) always reads a single
`parse_vuint` into a `u64` `missing_columns_bitmap` and has no `≥ 64` branch, so for a
`≥ 64`-column table it consumes only the missing-count VInt and then mis-reads the trailing
index VInts as cell data, corrupting the row stream. The reader also treats any column at
`idx >= 64` as present regardless of the field (`row_decoder/row_data.rs:340`). The **writer**
implements both modes correctly
(`writer/data_writer/rows.rs::write_column_subset`, `:1136`), pinned at the 63/64/65 boundary by
`cqlite-core/tests/issue_824_column_subset_and_filter.rs`. **Workaround**: tables with fewer
than 64 regular columns are unaffected (the common case).

### Complex-column merge is whole-column, not per-cell-path (cursor-findings 14/17/18) — RESOLVED in epic #921

**Status**: ✅ **RESOLVED** (#844 / #888 / #927 / #887, epic #921)

**Was** (Epic #817): Cassandra merges complex (multi-cell collection/UDT) columns **per
cell-path** using the column's path comparator — signed `ShortType` for a UDT field index,
`TimeUUIDType` for a list element, the map key type for a map — applying shadow-before-purge per
path. CQLite's merge (`storage/write_engine/merge/mod.rs::reconcile_cluster`, `:4207`, with the
per-cell helpers in `merge/reconcile.rs`) reconciled by **whole
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

### Equal-timestamp live-cell value tie-break diverges from Cassandra (cursor-findings 4/21)

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

### Latent: RT / complex-deletion size VInt width (cursor-finding 25)

**Status**: 🐛 **OPEN** (latent)

Cassandra's `UnfilteredSerializer` writes certain marker/deletion sizes as `long` VInts where
the corresponding read uses an `(int)` VInt (and vice versa) in places; CQLite's
range-tombstone-marker and complex-column-deletion size fields must use the matching width or a
large partition can mis-encode the size field. This is currently latent (small fixtures do not
exceed the narrow width) but is a real width hazard to watch when writing large partitions.
Authority: `org.apache.cassandra.db.rows.UnfilteredSerializer` (marker/row-body size fields).

### AlwaysPresentFilter / absent `Filter.db` — handled (cursor-finding 23)

**Status**: ✅ **HANDLED** (documented for completeness)

A table created with `bloom_filter_fp_chance = 1.0` is backed by Cassandra's
`AlwaysPresentFilter`, which serializes nothing — the SSTable has **no `Filter.db` component**.
CQLite reads such tables correctly via **both** `scan` and `get`: the per-reader bloom gate
(`reader/data_access/big_point.rs:77`) only consults `might_contain` when a filter is present, so an absent
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

### Row-deletion + live-cells coexistence — SUPPORTED (#932)

**Status**: ✅ **RESOLVED** (#932, CLOSED 2026-06-22)

**Was**: a Cassandra row can carry a **row deletion** (`HAS_DELETION`) **and** surviving cells
written strictly after the deletion timestamp at the same time. CQLite could not represent this —
the reader collapsed a `HAS_DELETION` row to a **pure tombstone**, and the merge `RowData` was an
enum, `Tombstone` **xor** `Live`, never both, so a row tombstone that should coexist with newer
surviving cells lost the row deletion.

**Now**: the merge model carries the deletion alongside the surviving cells —
`MergeRow::row_deletion: Option<(i64, i32)>` (`write_engine/merge/model.rs:73`, set via
`with_row_deletion`, `:177`) is `(markedForDeleteAt, localDeletionTime)`, so a live row can also be
row-deleted. The reader's single row-write-timestamp decision for the coexistence case is
`row_write_timestamp` (`reader/parsing/row_decoder/partition_driver.rs:40`), which prefers the row
liveness timestamp when cells survive and falls back to `markedForDeleteAt` for a pure tombstone.
Pinned by `reconcile_cluster_attaches_row_deletion_when_cells_survive`
(`write_engine/merge/mod.rs:11050`) plus the `row_write_timestamp_*` cases at
`partition_driver.rs:301-332`. Authority: `org.apache.cassandra.db.rows.Row` (a `Row` carries both
a `Row.Deletion` and live cells).

### Range tombstones during compaction — SUPPORTED (#933)

**Status**: ✅ **RESOLVED** (#933, CLOSED 2026-06-23)

**Was**: range tombstones were neither applied nor emitted in the compaction merge path — the
reader skipped range-tombstone markers on the normal scan/compaction path (only the dedicated
`delta-scan` path decoded them), and the merge did not persist a surviving marker into the
rewritten output.

**Now**, all three legs are wired:
- **Reader surfacing** — the compaction read path decodes markers via
  `parse_range_tombstone_marker_with_ldt`
  (`reader/parsing/row_decoder/compaction.rs:519`; the decoder itself is
  `row_decoder/row_framing.rs:1108`, also used by the `delta-scan`
  `parse_range_tombstone_marker_full` at `:1000`).
- **Merge shadowing** — `coalesce_range_tombstones` (`write_engine/merge/mod.rs:3465`) folds
  overlapping markers and `apply_range_shadowing` (`:3936`) drops the rows they cover.
- **Writer emission** — surviving markers are re-emitted into the compacted SSTable.

Pinned end-to-end by `cqlite-core/tests/issue_933_range_tombstone_compaction.rs`. Authority:
`org.apache.cassandra.db.rows.RangeTombstoneMarker` / `UnfilteredSerializer`.

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

## Write support posture (current)

### CQLite writes SSTables — uncompressed only

**Status**: ✅ **SUPPORTED** — M5 complete; byte-for-byte STCS compaction parity since v0.12

CQLite is **not** a read-only library. `write-support` is a **default** feature of `cqlite-core`,
and the write engine produces Cassandra 5.0-readable SSTables:

- **Flush**: `WriteEngine::write` (`cqlite-core/src/storage/write_engine/mod.rs`) accepts
  `Mutation`s through a WAL + memtable (`write_engine/wal.rs`, `write_engine/memtable.rs`) and
  flushes them as SSTables via `SSTableWriter` (`storage/sstable/writer/`).
- **Compaction**: STCS runs end-to-end. `WriteEngine::set_merge_policy` and
  `WriteEngine::maintenance_step` (`write_engine/maintenance.rs`) drive real background compaction,
  and `KWayMerger` (`write_engine/merge/mod.rs`) is the production merger.
- **Formats**: BIG (`nb`) is the DEFAULT write target; canonical BTI (`da`) write is a supported
  alternative (#872).
- **Export**: `WriteEngine::export_sstable` (`write_engine/export.rs`) emits a
  Cassandra-named component set.

> **Claim boundary — UNCOMPRESSED SSTable writes only (issue #1406).**
> The production write surface (flush + compaction) emits **uncompressed** SSTables and **never**
> writes a `CompressionInfo.db`. The compressed-write building blocks (`CompressedDataWriter`,
> `CompressionInfoWriter`) exist but are **UNWIRED** — fixture synthesis for the decompressing
> reader only, with zero Cassandra-side byte-parity coverage. Configuring compressed production
> writing returns `Error::UnsupportedFormat`
> (`CompressionInfoWriter::guard_unsupported_production_write`). Do **not** claim CQLite emits
> compressed SSTables. The READ path decompresses all four algorithms (LZ4, Snappy, Deflate, Zstd)
> end-to-end.

**Historically removed components** (Issues #175 / #176): the original `storage/wal.rs`,
`storage/memtable.rs`, `storage/compaction.rs`, `storage/manifest.rs`,
`storage/sstable/writer.rs`, and `storage/sstable/validation.rs` were deleted when CQLite was
briefly read-only. The write path was later rebuilt in a different place — under
`cqlite-core/src/storage/write_engine/` and `cqlite-core/src/storage/sstable/writer/` — so those
paths are gone but the capability is not.

**Residual read-only stubs**: the *legacy* `Storage` trait methods `put()` / `delete()` /
`flush_memtable()` / `flush()` in `cqlite-core/src/storage/mod.rs` still return
"removed in Issue #175" errors — that trait was never rewired to the write engine. Use
`WriteEngine` (or the CLI's write/compact commands) rather than those stubs.
`Database::flush()` / `Database::compact()` (`cqlite-core/src/lib.rs`) are gated behind the
`experimental` feature and delegate to those same legacy stubs.

---

## Feature-Gated and Deferred Features

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
**Purpose**: Opt-in heuristic *fallbacks* (schema-less blob decode) — **not** support for a
pre-Cassandra-5.0 format.

CQLite decodes using authoritative metadata only (no-heuristics mandate, Issue #28); this flag
re-enables the old guessing fallbacks for callers who accept the risk.

> **Version floor (do not misread this flag).** CQLite targets **Cassandra 5.0**: `na`/`nb` BIG and
> `oa`/`da` BTI. Pre-`na` (`ma`–`me`, Cassandra 3.x) is **out of scope** and is rejected in code —
> `BigVersionGates::from_version` rejects `< na` and `BtiVersionGates::from_version` rejects
> non-`da`, both surfacing `Error::UnsupportedVersion` through `SSTableReader::open`. Enabling
> `legacy-heuristics` does **not** make a 3.x SSTable readable.

**To Enable**:
```bash
cargo build --features legacy-heuristics
```

**Note**: These fallbacks are **not tested in CI** and may have gaps.

---

#### ~~ANTLR Parser (Alternative CQL Parser)~~ — REMOVED

**Status**: ✅ **REMOVED** (Issue #1639)

There is no `antlr` feature flag. The ANTLR stub backend was removed in #1639 (every parse through
it failed); nom is the only built-in CQL parser backend. Pinned by
`cqlite-core/tests/parser_factory_tests.rs`.

---

#### Tombstone and GC Logic

**Feature Flag**: `tombstones`
**Default**: Disabled (`cqlite-core/Cargo.toml`)

The default build skips tombstoned rows on the read path. The `tombstones` feature changes
execution behavior in `cqlite-core/src/query/select_executor/execute.rs` (epic #951 "honest
paths": a non-targeted execution fails loudly rather than silently full-scanning). Tombstone
semantics on the **write/compaction** path — gc_grace purging, range-tombstone shadowing and
re-emission — are NOT behind this flag and are supported by default; see
"Epic #921 — Compaction merge semantics" above.

---

### Query Engine Surface

**Current State**: Query engine enabled by default (`state_machine` feature)

**Implemented**:
- SELECT statement parsing and execution
- Prepared statement support
- Query planning and optimization
- Multi-partition query execution
- Schema-aware result formatting
- `WHERE` filtering: partition-key point lookup plus a residual filter step
  (`select_executor/mod.rs::execute_filter`, `select_executor/predicate.rs`)
- `LIMIT` / `OFFSET`, including scan-level limit pushdown
  (`select_executor/mod.rs::execute_limit`, `select_executor/limit_pushdown/`)
- `PER PARTITION LIMIT` (`execute_per_partition_limit`)
- `ORDER BY` (`select_executor/lookup.rs`)
- `DISTINCT` (`select_optimizer.rs`)
- Aggregate functions and `GROUP BY`
  (`select_executor/aggregation/`, `select_executor/stream_agg.rs`, plus the #1578
  GROUP-BY-free global-aggregate fast path in `select_executor/execute.rs`)

**Not Implemented**:
- `UPDATE` / `DELETE` statement execution (no executor exists; the statement types parse only)
- `INSERT` execution requires the non-default `experimental` feature
  (`cqlite-core/src/query/executor.rs` returns `Error::UnsupportedFormat` otherwise)

**Workaround**: For unsupported query features, use the `read-sstable` command for raw data access,
or write through `WriteEngine` / Apache Cassandra tools.

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
  - Root cause 1: Timestamp units mismatch in the type parser - `parse_timestamp()` multiplied milliseconds by 1000 (converting to microseconds) but `Value::Timestamp(i64)` stores milliseconds. This caused overflow → negative values → `<invalid-timestamp:...>` markers.
  - Root cause 2: Partition header flags heuristic in the row decoder - a `flags > 0x20` check rejected valid partition headers with higher flag values, causing single-byte offset skip and cascading misalignment errors. Violated Issue #28 no-heuristics mandate.
  - Fix 1: Removed the `* 1000` multiplication — `parse_timestamp` now stores milliseconds directly (`cqlite-core/src/parser/types/primitives.rs:89`)
  - Fix 2: Removed the `flags > 0x20` heuristic check in the row decoder - validation is now format-based only
  - Result: All 33 test tables pass comprehensive SELECT tests with no ERROR messages or invalid data markers
  - Files: `cqlite-core/src/parser/types/` (split by epic #1116: `primitives.rs`, `collections.rs`, `udt.rs`, `tombstones.rs`), `cqlite-core/src/storage/sstable/reader/parsing/row_decoder/`

- **Issue #240**: DATE Type Values Display as `<invalid-date:...>` - **FIXED**
  - Status: ✅ FIXED - DATE type now parses correctly in all contexts including map keys
  - Root cause: `CqlType::Date` was mapped to `ComparatorType::Custom("date")`, causing DATE values to fall through to blob parsing. Also, multiple parsing paths read DATE as raw i32 without Cassandra's Integer.MIN_VALUE offset decoding.
  - Fix:
    1. Added a `ComparatorType::Date` variant with proper comparison support (`cqlite-core/src/types/comparator.rs:48`)
    2. Updated `from_cql_type()` / `from_cql_type_with_registry()` to map `CqlType::Date` → `ComparatorType::Date` (`comparator.rs:140`)
    3. Added DATE parsing arms to `parse_value_with_schema_type()` and `parse_value_with_comparator()` (`reader/parsing/value_parsing.rs`, `reader/parsing/comparator_value_parsing.rs`)
    4. Fixed `parse_date()` to apply Cassandra DATE encoding: `stored.wrapping_add(i32::MIN as u32) as i32` (`parser/types/primitives.rs:97-100`)
    5. Fixed DATE map-key parsing in the row decoder (now `row_decoder/complex_column.rs:1582`, with the same decode in `cell_value_scalar.rs:332`, `raw_type_value.rs:275`, `raw_value.rs:262`)
    6. Digest-key DATE decoding at `storage/sstable/key_digest.rs:233-240`
  - Cassandra DATE encoding: 4-byte big-endian unsigned int shifted by Integer.MIN_VALUE (2^31) for byte-order comparability. Decoding adds i32::MIN back.
  - Result: DATE columns and DATE keys in maps now display as `YYYY-MM-DD` format (e.g., `2025-10-05`) instead of `<invalid-date:...>`

- **Issue #238**: UDTs Inside Collections Not Parsed - **FIXED**
  - Status: ✅ FIXED - Extended `parse_value_with_comparator` for recursive type parsing
  - Root cause: `parse_value_with_comparator` had minimal implementation (only Boolean, Text, Blob) - all other types fell back to Blob, including UDTs nested in List/Set/Map
  - Fix: Added complete type handlers for TinyInt, SmallInt, Int, BigInt, Uuid, List, Set, Map, Tuple, UDT, and Frozen types
  - Result: UDTs inside collections now show actual field values instead of `0x` blobs
  - File: `cqlite-core/src/storage/sstable/reader/parsing/comparator_value_parsing.rs`
    (`parse_value_with_comparator` at `:44`, recursion via `parse_value_with_comparator_at_depth`
    in `value_parsing.rs:481`)

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
  - File: `cqlite-core/src/storage/sstable/reader/parsing/row_decoder/` (split by epic #1116)

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
  - Also fixed: format-version dispatch on the block-read path (the version variant named in the
    original writeup no longer exists; version classification now lives in
    `cqlite-core/src/parser/header.rs`)
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
  - Status: Fixed (format-version dispatch on the block-read path + BTI inter-entry padding)
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
  - Result: the `0xD4645400` header magic is recognized (`cqlite-core/src/parser/header.rs:171`)

- **Issue #208**: BTI Index.db Format Support
  - Status: Completed
  - Note: the "dual-parser architecture for MD5 digest + BTI Index.db formats" described here was
    subsequently **removed** as a spurious heuristic (Issue #28 mandate) — there is no MD5 digest in
    a BIG Index.db entry, and a BTI-indexed SSTable has no Index.db at all (it uses
    `Partitions.db`/`Rows.db`). See the corrected entry layout under "Index.db VInt Offset Parsing"
    above.

- **Issue #209**: Component Flattening Pre-allocation
  - Status: Completed
  - Result: 55-75% performance improvement for 2-6 component keys

### Formerly Deferred (M3+ Scope) — now closed

- **Issue #154**: UDT support (`collections_with_udts`)
  - Status: ✅ CLOSED (2025-10-11). Its blocker, Issue #210 (static columns in SerializationHeader),
    is also CLOSED; UDT support landed via #220 and `collections_with_udts` passes.

- **Issue #162**: Statistics.db EncodingStats parsing
  - Status: Parser implemented; the EncodingStats decode now lives in
    `cqlite-core/src/parser/enhanced_statistics_parser/encoding_stats.rs`.

- **Issue #191**: Tombstone row filtering
  - Status: ✅ CLOSED (2025-10-24). Tombstoned rows are skipped on the SELECT path
    (`cqlite-core/src/query/select_executor/execute.rs`). Tombstone metadata *is* exposed — behind
    the non-default `tombstones` feature, with epic #951's honest-paths behavior (a non-targeted
    execution fails rather than silently full-scanning); see "Tombstone and GC Logic" above.

### Infrastructure Removed (and later rebuilt elsewhere)

- **Issue #175**: MemTable and WAL removal
  - Rationale at the time: read-only library focus
  - Then: all write operations returned errors
  - **Now**: rebuilt under `cqlite-core/src/storage/write_engine/` (`wal.rs`, `memtable.rs`). Only
    the *legacy* `Storage` trait stubs still return the "removed in Issue #175" error — see
    "Write support posture (current)".

- **Issue #176**: Compaction and manifest removal
  - Rationale at the time: read-only library focus
  - Then: compaction methods returned errors
  - **Now**: STCS compaction runs end-to-end via `WriteEngine::maintenance_step` +
    `KWayMerger`, with byte-for-byte parity vs Cassandra since v0.12 — see "Compaction and Export
    Capabilities".

---

## Historical M5 Write-Support Limitations (mostly closed)

These were the intentional simplifications taken when write support first landed in M5. **Nearly all
have since been fixed** — each entry below records what the limitation was and what closed it, so
that an old reference to "the M5 write limitation" resolves to the current truth rather than the
2026-01 snapshot. The write engine produces valid Cassandra 5.0 BIG format SSTables readable by both
CQLite and Cassandra; the one genuinely open claim boundary is the uncompressed-only write surface
(#1406, last entry in this section).

### ~~Tombstone Local Deletion Time Workaround~~ - FIXED

**Status**: ✅ **FIXED** (Issue #401, CLOSED 2026-01-29; explicit field shipped in #764)

**Background**: Cassandra tombstones require two timestamp fields:
1. **Deletion timestamp** (microseconds): When the delete was issued
2. **Local deletion time** (seconds): Local server time for GC eligibility tracking

**Was**: `Mutation` had no explicit `local_deletion_time`, so the writer derived it as
`timestamp_micros / 1_000_000` — correct for an immediate delete, wrong for a replayed or
back-dated one.

**Now**: `Mutation` carries the field explicitly —
`pub local_deletion_time: Option<i32>` (`cqlite-core/src/storage/write_engine/mutation.rs:97`),
settable via `Mutation::with_local_deletion_time` (`:191`), and a row deletion carries its own
`(deletion_time_micros, local_deletion_time_secs)` pair (`:101`). It still defaults to the derived
value when unset, so existing callers are unaffected. The writer lives under
`cqlite-core/src/storage/sstable/writer/data_writer/` (split by epic #1116); TTL / expiring-cell
handling is in `data_writer/cells.rs`.

**Tracking**: Issue #401 (CLOSED)

---

### ~~IndexWriter Memory Buffering~~ - FIXED (streaming mode)

**Status**: ✅ **FIXED** (Issue #408, CLOSED 2026-01-29; streaming from #753, counting mode from #908)

**Was**: `IndexWriter` held every serialized index entry in a `Vec<u8>` until `finish()`, so peak
heap grew linearly with partition count (~20 MB per 1M partitions, ~200 MB per 10M) and a
billion-partition write was out of reach.

**Now**: `IndexWriter` has **three** modes that produce byte-identical Index.db output
(`cqlite-core/src/storage/sstable/writer/index_writer.rs:130-165`):

| Mode | Constructor | Peak heap | Used by |
|------|-------------|-----------|---------|
| In-memory | `IndexWriter::new` (`:215`) | O(file) | unit tests that inspect produced bytes |
| Streaming | `IndexWriter::with_sink(index_path)` (`:270`) | O(one entry) | the production BIG write path (#753) |
| Counting | `IndexWriter::counting` (`:251`) | O(one entry) | the BTI path, which has no Index.db but still needs per-entry offset/size bookkeeping (#908) |

In streaming mode each entry is serialized into a small scratch `Vec`, written through a
`BufWriter<File>`, and the scratch is cleared — keeping a multi-GB compaction inside the 128 MB
memory target. The file is opened lazily on the first `add_partition`, so the parent directory need
not exist at construction. Summary.db sampling still gets exact offsets: in streaming/counting
modes `index_offset` equals `position` (the scratch is empty at that point), and in in-memory mode
it equals `buffer.len()`.

**Tracking**: Issue #408 (CLOSED)

---

### ~~Promoted Index Deferred~~ - IMPLEMENTED

**Status**: ✅ **IMPLEMENTED** (Issue #993) — the M5 Stage 0 deferral is over.

Promoted index is Cassandra's optimization for wide partitions: it stores sampled clustering-key
ranges *within* a partition, enabling a bounded seek to a specific row range instead of a full
within-partition scan. The old claim here — that Index.db entries always write
`promoted_index_length = 0` — is no longer true.

See **"Remaining Write-Path Limitations → Promoted Index"** earlier in this appendix for the
current write-side and read-side implementation, including the **≥ 64 KiB** collection threshold and
the **≥ 2**-block emission gate that mirrors Cassandra's `RowIndexEntry.create()`
(`columnIndexCount > 1`).

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

**Current Limitations** (one, not three):
- COMPACTION cardinality estimator writes a **minimal valid empty** HyperLogLogPlus sketch
  (`HyperLogLogPlus(p=11, sp=25)`, SPARSE, 15 bytes, both set sizes 0 —
  `writer/stats_writer/components.rs:63-92`). The file parses in Cassandra, but the estimate is 0.

**Corrections to earlier drafts of this section**:
- *"STATS histograms use minimal valid values (2 buckets, empty tombstone histogram)"* was **wrong**.
  The writer emits Cassandra-canonical `EstimatedHistogram` bucket layouts (156 partition-size / 119
  column-count buckets, #1327 — `stats_writer/components.rs:493-496`,
  `stats_writer/estimated_histogram.rs`), and the tombstone-drop histogram is populated at
  `stats_writer/metadata.rs:257-264` (excluding the `DeletionTime.LIVE` sentinel, per #851).
- *"Column bitmap encoding limited to 64 columns"* is a **reader** limitation, not a Statistics.db
  one — see "Reader lacks the `≥ 64`-column large-subset decode branch" above. The writer implements
  both the `< 64` bitmap and the `≥ 64` large-subset forms.

**Impact**: Statistics.db files are fully compatible with Cassandra 5.0. When schema is provided via `write()`, SerializationHeader contains full column metadata. When schema is None, uses minimal stub format.

**Files**:
- `cqlite-core/src/storage/sstable/writer/stats_writer/` (split by epic #1116: `mod.rs`,
  `components.rs`, `metadata.rs`, `estimated_histogram.rs`, `serialization_header.rs`, `marshal.rs`)

**Related Issues**: Issue #425 (Statistics.db checksums and format - FIXED)

---

### CompressionInfo.db Writing — production writes NOT implemented (test-only, fail-closed)

**Status**: ⚠️ Production write NOT implemented; READ/decompression fully supported (#1406)

CQLite's production SSTable writer emits uncompressed Data.db only and never writes a CompressionInfo.db. The `CompressedDataWriter` / `CompressionInfoWriter` types are UNWIRED building blocks that synthesize compressed fixtures for the decompressing reader; configuring compressed production writing returns `Error::UnsupportedFormat`.

The READ path fully supports all four algorithms (LZ4, Snappy, Deflate, Zstd) — CQLite reads compressed Cassandra SSTables end-to-end.

See the "Write Support Capabilities" section at the top of this document for the exact fail-closed boundary.

**Files**:
- `cqlite-core/src/storage/sstable/writer/compressed_data_writer.rs` (test-only building block)
- `cqlite-core/src/storage/sstable/writer/compression_info_writer.rs` (test-only building block)

---

## Key Takeaways

- **Pass rate: 100% (33/33 tables)** - COMPLETE! All test tables now passing
- All SSTable component parsers (Data.db, Index.db, Summary.db, Statistics.db) now use correct formats
- All data types fully supported: basic types, collections, UDTs, frozen types, complex cells
- **CQLite is NOT read-only.** Write support (`write-support`, a **default** feature) and STCS
  compaction ship, with byte-for-byte compaction parity vs Apache Cassandra since v0.12. See
  "Write support posture (current)".
- **Write support**: feature-complete for its claimed surface
  - ⚠️ **The one hard claim boundary**: the production write surface emits **uncompressed** SSTables
    only and **never** a `CompressionInfo.db`; compressed-write building blocks are UNWIRED
    (fixture synthesis only) and configuring compressed production writing returns
    `Error::UnsupportedFormat` (#1406). READ/decompression fully supports LZ4, Snappy, Deflate, Zstd
  - ✅ Collection serialization: Frozen and non-frozen collections
  - ✅ Static columns: Extended flags format with EXTENDED_IS_STATIC
  - ✅ Composite partition keys: Multi-component encoding
  - ✅ Delta encoding: Statistics.db baseline for timestamps/TTL
  - ✅ Explicit tombstone `local_deletion_time` on `Mutation` (#401 fixed, shipped in #764)
  - ✅ Streaming `IndexWriter` — O(one entry) peak heap via `with_sink` (#408 fixed via #753/#908)
  - ✅ Promoted index for ≥ 64 KiB partitions, gated on ≥ 2 blocks like Cassandra (#993)
  - ✅ Statistics.db full Cassandra 5.0 TOC format with canonical histograms (#1327); only the
    HyperLogLogPlus cardinality sketch is an empty stub
  - ✅ Canonical BTI (`da`) write in addition to the default BIG (`nb`) target (#872)
- **All read-side feature gaps closed**:
  - ✅ Issue #219: Frozen type support
  - ✅ Issue #220: UDT (User-Defined Type) support
  - ✅ Issue #221: Complex cell flag handling for non-frozen collections
- **Remaining read-side divergences to know about**: the `≥ 64`-column large-subset decode branch is
  missing, and the equal-timestamp live-cell value tie-break differs from Cassandra's
  `Cells.resolveRegular` — both detailed under "Epic #817".

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
