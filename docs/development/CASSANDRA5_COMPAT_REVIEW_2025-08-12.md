## Cassandra 5 Data Compatibility Review (Uncompromising)

### Author: External Cassandra Reviewer
### Date: 2025-08-12

---

## Executive summary

- **Objective**: “Read any Cassandra 5 data with our library.”
- **Verdict**: **Not currently achievable** with the present approach. The code relies on heuristics, guesses, and incomplete component readers. Several claims in documentation are incorrect or overstated relative to the implementation. Critical format features (especially BTI) are absent.
- **Confidence (success on current trajectory)**: **Low (15–25%)**.
- **Required pivot**: Replace heuristic parsing with spec-accurate, schema/comparator-driven decoding; implement BTI; wire `CompressionInfo.db`/chunking/CRC; implement real `Index.db`/`Summary.db`/`Statistics.db`; integrate validation against Cassandra tools.

---

## Evidence (code and docs)

### Overstated/incorrect documentation

The guide claims “Mission accomplished: 100% compatibility,” and documents an invented header layout that does not correspond to Cassandra’s actual on-disk formats.

```text
docs/technical/CASSANDRA_COMPATIBILITY_GUIDE.md:1-20,39-50
# CQLite Cassandra 5+ Compatibility Guide

## 🎯 **MISSION ACCOMPLISHED: Byte-Perfect Cassandra 5+ Compatibility**
...
### ✅ **100% Compatibility Achieved**
...
#### ✅ **Header Format Compliance**
```
```
Cassandra 'oa' Header (32 bytes):
[Magic: 4 bytes = 0x5A5A5A5A][Version: 2 bytes = "oa"][Flags: 4 bytes]
[Partition Count: 8 bytes][Timestamp Range: 16 bytes][Reserved: 7 bytes]
```
```
```

Simultaneously, the team’s own matrix concedes BTI is not implemented:

```text
docs/technical/CASSANDRA_5_0_COMPATIBILITY_MATRIX.md:14-34
| **BTI Format ('da')** | ✅ New Default | ⚠️ Partial | P0 |
...
| **Partitions.db** | ✅ Specified | ❌ Not Implemented | P0 |
| **Rows.db**       | ✅ Specified | ❌ Not Implemented | P0 |
| **Byte-comparable keys** | ✅ Specified | ❌ Not Implemented | P0 |
```

### Heuristic, non-generalizable parsing in storage layer

- “Modern format” (4.x/5.x) parsing uses UUID scanning across `Data.db` instead of spec-driven decoding.

```rust
cqlite-core/src/storage/sstable/bulletproof_reader.rs:202-216
/// Parse modern SSTable format (4.x, 5.x)
fn parse_modern_format(&self, data: &[u8]) -> Result<Vec<SSTableEntry>> {
    println!("🆕 Parsing modern SSTable format WITH NEW UUID SCANNING!");
    ...
    // For Cassandra 5.0, use UUID scanning approach
    println!("🚀 USING NEW UUID SCANNING APPROACH!");
    let entries = self.scan_for_uuids(data)?;
    ...
}
```

- Column decoding guesses types and sparsely skips “flags” rather than reading the actual cell/row header state machine tied to schema and comparators.

```rust
cqlite-core/src/storage/sstable/reader.rs:1474-1523
fn parse_column_value_enhanced(&self, value_data: &[u8], _table_id: &TableId, _key: &RowKey) -> Result<Value> {
    // Skip cell flags if present (Cassandra 5.0 format)
    if value_data.len() > 1 && (value_data[0] & 0x80) != 0 { ... }
    ...
    match self.detect_value_type(actual_value_data) {
        Some(type_id) => { ... }
        None => {
            // fallback to UTF-8 or blob
        }
    }
}
```

- Hardcoded value parsing as `Varchar` in a path that should be schema-driven:

```rust
cqlite-core/src/storage/sstable/reader.rs:740-742
// Parse the value
let (_, value) = parse_cql_value(&data, CqlTypeId::Varchar) // Type should be determined from context
    .map_err(|e| Error::corruption(format!("Failed to parse value: {:?}", e)))?;
```

- `Frozen<T>` handling incorrectly passes a `Debug` string of the inner type instead of a concrete type spec.

```rust
cqlite-core/src/storage/reader.rs:586-591
CqlType::Frozen(inner_type) => {
    // Frozen types have the same binary format as their inner type
    let (inner_value, consumed) = self.parse_column_value_exact(data, &format!("{:?}", inner_type))?;
    Ok((Value::Frozen(Box::new(inner_value)), consumed))
}
```

### Critical components are stubbed or oversimplified

- `Index.db` parsing reduced to “length + 8-byte offset” iteration. Real index structures (including promoted indexes) are more complex.

```rust
cqlite-core/src/storage/reader.rs:184-221
fn parse_index_file(&self, index_data: &[u8], cache: &mut ReaderMetadata) -> Result<()> {
    // length-prefixed key, then 8-byte data offset
    ...
    cache.partition_index.insert(key_bytes, data_offset);
}
```

- `Statistics.db` checksum/format handling left as TODO, acknowledging uncertainty.

```rust
cqlite-core/src/storage/sstable/statistics_reader.rs:54-61
// Validate checksum if present (for enhanced parser, checksums are handled differently)
// ... Skip validation for now as the format is more complex
// TODO: Add proper checksum validation for nb format
```

- Compression uses trial-and-error fallbacks, not authoritative chunk boundaries from `CompressionInfo.db` with CRC enforcement.

```rust
cqlite-core/src/storage/sstable/compression.rs:147-186
// LZ4: try size-prepended; try big-endian size; try little-endian size; ...
```

- “Schema discovery” fabricates column names from samples; this cannot yield reliable decoding of any table.

```rust
cqlite-core/src/storage/schema_discovery.rs:318-324
for (i, value) in entry.values.iter().enumerate() {
    let column_name = format!("column_{}", i); // Generic name, will be refined
    row_data.insert(column_name, value.clone());
}
```

- Tombstone parsing exists in isolation, but tombstone application and reconciliation are not integrated with accurate row/cell metadata and clustering ranges per spec.

```rust
cqlite-core/src/parser/types.rs:864-913
pub fn parse_tombstone(input: &[u8]) -> IResult<&[u8], Value> { ... }
```

### BTI is missing

No implemented readers for `Partitions.db` (trie), `Rows.db`, or byte-comparable key decoding. The repo acknowledges this gap in the matrix.

---

## Why this will not succeed as-is

- **Heuristics over specification**: UUID scanning, type-guessing based on byte patterns, and ad-hoc flag skipping are not compatible with Cassandra’s precise row/cell serialization. They will fail on many real datasets, especially with collections, static rows, and variable encodings.
- **Schema and comparator blind**: Correct decoding requires the table schema, partition/clustering comparators, and column type info. Current code paths do not consistently thread authoritative schema/comparator data, leading to brittle decoding and wrong results.
- **BTI not supported**: Cassandra 5’s BTI is the default focus. Without `Partitions.db` trie traversal, byte-comparable key handling, and `Rows.db` decoding, “read any Cassandra 5 data” is impossible.
- **Component-level gaps**: Accurate `Index.db`/`Summary.db`/`Statistics.db` reading (including promoted indexes and min/max/token coverage) is necessary both for correctness and performance. Current implementations are stubs or oversimplifications.
- **Compression correctness**: Chunk addressing and CRC via `CompressionInfo.db` is required. Trial-and-error decompression is fragile and risks silent corruption/misparsing.
- **Tombstone semantics**: Range tombstones, row deletions, expirations, and reconciliation rules must be honored at read time with write-time ordering. Current filters are permissive and not spec-accurate.
- **Validation gap**: No rigorous golden comparison against `sstabledump` or Cassandra itself. Local scripts with ad-hoc paths are not sufficient to assert compatibility.
- **Documentation accuracy**: Overclaims of 100% compatibility undermine credibility and can mislead implementation decisions.

---

## What “good” looks like (acceptance criteria)

- **BIG and BTI complete**:
  - BIG (‘na/nb/oa’): spec-accurate partition/row/cell decoding with correct row/cell headers, clustering blocks, static rows, multi-cell collections, UDTs, TTL, localDeletionTime, writeTime.
  - BTI (‘da’): `Partitions.db` trie traversal with byte-comparable keys; `Rows.db` decoding; parity with BIG semantics as per C*.
- **Schema/comparator-driven**: Decoding driven by table schema and comparators (partition/clustering), not guessing.
- **Compression**: Reads are chunked according to `CompressionInfo.db` with checksum validation and exact block boundaries.
- **Index/Summary/Statistics**: Implement real readers supporting lookups, iteration, and metadata (min/max, token coverage), including promoted indexes.
- **Tombstones**: Accurate reconciliation honoring timestamps, ranges, and TTLs.
- **Validation**: CI tests using a corpus of SSTables (BIG/BTI, various compressors, complex types, tombstones) compared against `sstabledump` output or Cassandra query results; zero-diff requirement.

---

## Remediation plan (prioritized, methodical)

### P0 — Replace heuristics with spec-accurate readers

- Remove UUID scanning and type guessing paths. Implement a row decoder that follows Cassandra’s row/cell header state machine bound to the table schema and column mask.
- Implement authoritative `CompressionInfo.db` parser and chunked I/O with per-chunk checksum. The reader must never guess decompression formats.
- Implement `Index.db` and `Summary.db` parsing according to the format version, including promoted index handling. Use these for partition lookup and iteration.

### P0 — BTI first-class support

- Implement `Partitions.db` trie traversal, including byte-comparable comparators.
- Implement `Rows.db` decoding, ensuring parity with BIG’s row/cell semantics.
- Build integration tests across BIG and BTI for the same logical data.

### P0 — Schema and comparator integration

- Require schema input (from `system_schema` export or a manifest) for any read. Add a schema registry mapping tables to partition/clustering comparators and column types.
- Thread comparator-aware decoding through partition key and clustering key parsing.

### P0 — Validation harness

- Add test runner that:
  - Generates test SSTables with Cassandra 5 (BIG and BTI, multiple compressors, complex types, static rows, range tombstones).
  - Runs `sstabledump` on each file.
  - Compares CQLite-decoded rows cell-by-cell to `sstabledump` JSON (values, writeTime, TTL, tombstones).
  - Fails on any discrepancy; report includes first diff and context.

### P1 — Tombstone reconciliation engine

- Implement reconciliation rules (per C*): row vs cell tombstones, range tombstones, write-time ordering, TTL expirations.
- Add targeted tests (conflicting writes, overlapping ranges, expired tombstones) and validate against `sstabledump` and live Cassandra reads.

### P1 — Statistics and Bloom filter

- Implement `Statistics.db` parser with accurate min/max and token coverage extraction; wire Bloom filter for partition existence hints.
- Enforce checksum/CRC validations where applicable.

### P2 — Observability and fuzzing

- Add detailed decode traces (behind a feature flag) that print header/flag decisions and offsets for triage.
- Fuzz vint parsing and row decoder inputs; keep a minimized corpus of real-world failures.

---

## Concrete refactors and deletions

- Delete or quarantine:
  - UUID-scanning code paths in `sstable/bulletproof_reader.rs`.
  - Type-guessing fallbacks in `parse_column_value_enhanced`; replace with schema-driven decoding.
  - Overclaiming and incorrect doc sections (or clearly mark as aspirational).

- Introduce modules:
  - `sstable/format/big/{header.rs,row.rs,cell.rs,index.rs,summary.rs,statistics.rs}`
  - `sstable/format/bti/{partitions.rs,rows.rs,keys.rs}`
  - `sstable/compression/{compression_info.rs,chunk_reader.rs}`
  - `schema/{registry.rs,comparators.rs}`
  - `validation/{sstabledump_diff.rs,corpus.rs}`

Each module must have unit tests with raw-byte fixtures and end-to-end integration tests against real SSTables.

---

## Quality gates (must pass before claiming compatibility)

- Green CI across:
  - BIG and BTI SSTables, compressed (LZ4, Zstd, Snappy, Deflate) and uncompressed.
  - Tables covering: primitives, collections (frozen/non-frozen), UDTs, tuples, counters, static rows, wide partitions, range tombstones.
  - Index/Summary/Statistics parsing verified; compression chunk checksums enforced.
- `sstabledump` parity: 100% cell-level equality across a seeded corpus (no ignored fields).
- No heuristic fallbacks (string/Blob guessing). All values decoded via schema.
- Documentation re-written to reflect reality; include format-versioned behavior and known limitations (if any) until fully closed.

---

## Immediate actions (next 2 weeks)

- Remove UUID scanning and guessing fallbacks.
- Land `CompressionInfo.db` parser and strict chunked reads with CRC.
- Implement BIG row/cell header decoder bound to schema and column masks; add tests vs `sstabledump` for at least 3 representative tables (simple, collections, UDTs).
- Stand up CI job producing SSTables in Docker Cassandra 5 and running parity checks.
- Correct documentation: retract “100%” claims; publish a truthful status page with a short-term roadmap.

---

## Closing note

Ambition is good; accuracy is mandatory. Cassandra’s on-disk formats are unforgiving. With a pivot to spec-accurate, schema-driven readers, proper BTI support, and hard validation against Cassandra outputs, this project can get on track. Without that pivot, “read any Cassandra 5 data” will remain out of reach.

---

## References

- [R1] ScyllaDB Docs: SSTables 3.0 Data File Format (describes Cassandra 3.x row-oriented format, file components, varint/delta, row/header layout). `https://opensource.docs.scylladb.com/stable/architecture/sstable/sstable3/sstables-3-data-file-format.html`
- [R2] Cassandra Docs: Compaction overview (immutability, multiple SSTables, compaction goals/types). `https://cassandra.apache.org/doc/latest/cassandra/managing/operating/compaction/overview.html`
- [R3] Cassandra Docs: Tombstones (gc_grace_seconds, zombie prevention, deletion semantics, fully expired SSTables). `https://cassandra.apache.org/doc/latest/cassandra/managing/operating/compaction/tombstones.html`
- [R4] DataStax Docs: sstabledump (tooling to validate and inspect SSTables). `https://docs.datastax.com/en/archived/cassandra/3.0/cassandra/tools/ToolsSSTabledump.html`
- [R5] Cassandra Blog: Apache Cassandra 4.1 – New SSTable Identifiers (file naming pattern, version slot such as nb/md, component names). `https://cassandra.apache.org/_/blog/Apache-Cassandra-4.1-New-SSTable-Identifiers.html`
- [R6] Scylla Wiki: SSTables Index File (primary index structure, promoted index for wide rows, skipping within partitions). `https://github.com/scylladb/scylladb/wiki/SSTables-Index-File`
- [R7] Scylla Wiki: SSTables Data File (partition/row/cell structures for 2.x-era format; useful for historical contrast). `https://github.com/scylladb/scylladb/wiki/SSTables-Data-File`
- [R8] Scylla Docs: SSTable Compression and CompressionInfo.db (chunked compression, chunk offsets, CRC, LZ4/Snappy/Deflate). `https://opensource.docs.scylladb.com/stable/architecture/sstable/sstable2/sstable-compression.html`
- [R9] PVLDB 2022: Trie Memtables in Cassandra (byte-comparable keys, trie-based structures). `https://www.vldb.org/pvldb/vol15/p3359-lambov.pdf`
- [R10] CEP-19: Trie memtable implementation (adopted in Cassandra 5.0). `https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-19%3A+Trie+memtable+implementation`
- [R11] CEP-25: Trie-indexed SSTable format (new on-disk trie indexing in Cassandra 5.0). `https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-25%3A+Trie-indexed+SSTable+format`
- [R12] CEP-7: Storage Attached Index (SAI) (Cassandra 5.0 secondary index implementation). `https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-7%3A+Storage+Attached+Index`
- [R13] Cassandra CEP index (adopted 5.0 features incl. SSTable format API, SAI, UCS). `https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=95652201`
- [R14] Cassandra 4.1 Features: Pluggable memtables (JIRA CASSANDRA-17034). `https://issues.apache.org/jira/browse/CASSANDRA-17034`
- [R15] StackOverflow: Promoted index explanation (skipping inside wide partitions). `https://stackoverflow.com/questions/42546354/how-does-cassandra-read-specific-rows-with-partition-key-and-clustering-keys`
- [R16] Gist: Cassandra SSTable format version numbers (historical list up through mc/md). `https://gist.github.com/shyamsalimkumar/49a61e5bc6f403d20c55`
- [R17] Anant: Cassandra SSTables Overview (components and read path checkpoints). `https://anant.us/blog/modern-business/cassandra-sstables-overview/`

### Appendix: Cassandra 5.0 source pointers (file headers worth reading)

The following files in the Cassandra 5.0 codebase contain authoritative header comments and implementation details for reading/writing SSTables and related components:

- Core SSTable API and BigTable format
  - SSTable abstraction and reader API: `src/java/org/apache/cassandra/io/sstable/format/SSTableReader.java`
  - BigTable format reader: `src/java/org/apache/cassandra/io/sstable/format/big/BigTableReader.java`
  - BigTable scanner (sequential and range reads): `src/java/org/apache/cassandra/io/sstable/format/big/BigTableScanner.java`
  - Row index entry (promoted index for wide partitions): `src/java/org/apache/cassandra/io/sstable/format/big/RowIndexEntry.java`
  - BigTable writer: `src/java/org/apache/cassandra/io/sstable/format/big/BigTableWriter.java`
  - Path: `https://github.com/apache/cassandra/tree/cassandra-5.0/src/java/org/apache/cassandra/io/sstable/format/big`

- Index summary and primary index
  - Index summary (Summary.db) structures: `src/java/org/apache/cassandra/io/sstable/indexsummary/IndexSummary.java`
  - Primary index readers/writers live alongside BigTable classes; see also `IndexSummaryBuilder`, `IndexFileUtils`.
  - Path: `https://github.com/apache/cassandra/tree/cassandra-5.0/src/java/org/apache/cassandra/io/sstable/indexsummary`

- Statistics and metadata
  - StatsMetadata (Statistics.db fields, encoding stats, histograms): `src/java/org/apache/cassandra/io/sstable/metadata/StatsMetadata.java`
  - SSTableMetadata interfaces: `src/java/org/apache/cassandra/io/sstable/metadata`
  - Path: `https://github.com/apache/cassandra/tree/cassandra-5.0/src/java/org/apache/cassandra/io/sstable/metadata`

- Compression
  - Compression metadata (CompressionInfo.db, chunk length/offsets, CRC): `src/java/org/apache/cassandra/io/compress/CompressionMetadata.java`
  - Compressors (LZ4/Snappy/Deflate): `src/java/org/apache/cassandra/io/compress`
  - Path: `https://github.com/apache/cassandra/tree/cassandra-5.0/src/java/org/apache/cassandra/io/compress`

- Read path iterators
  - Unfiltered row iteration (rows, range tombstones): `src/java/org/apache/cassandra/db/rows/UnfilteredRowIterator.java`
  - On-disk row reading serializers: `src/java/org/apache/cassandra/db/rows`

- ByteComparable utilities (trie/BTI readiness)
  - ByteComparable and comparators used for trie-based data structures: `src/java/org/apache/cassandra/utils/bytecomparable`
  - Path: `https://github.com/apache/cassandra/tree/cassandra-5.0/src/java/org/apache/cassandra/utils/bytecomparable`

- Storage-Attached Index (SAI)
  - On-disk index files, KD-tree postings, trie terms dictionary: `src/java/org/apache/cassandra/index/sai`
  - Path: `https://github.com/apache/cassandra/tree/cassandra-5.0/src/java/org/apache/cassandra/index/sai`

Repository root (5.0 branch): `https://github.com/apache/cassandra/tree/cassandra-5.0`

