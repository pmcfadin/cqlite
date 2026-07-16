## Statistics.db

`Statistics.db` captures table-level metadata such as histograms, min/max timestamps, repair/level flags, compression ratios, and counts that inform compaction and read heuristics.

### In this chapter you will learn
- What `StatsMetadata` contains and how it is used
- How statistics are collected during flush
- How stats influence compaction and read behavior
- How to inspect a tiny subset via tools

## Stats Overview

Trimmed excerpt from `test_basic/simple_table`:

```text
Bloom Filter FP chance: 0.01
Minimum timestamp: 2025-09-16 22:14:23
Maximum timestamp: 2025-09-16 22:14:24
Compressor: org.apache.cassandra.io.compress.SnappyCompressor
Compression ratio: 0.976...
SSTable Level: 0
totalRows: 1000
```

### File Structure and Key Fields

`Statistics.db` serializes `StatsMetadata` alongside related metadata blocks. Important fields (names align with Cassandra classes where applicable):

- Timestamps and Deletions:
  - **min_timestamp / max_timestamp**: microsecond epoch range of writes
  - **min/max local deletion time**: lower/upper bounds for tombstone local deletion time
- Bloom and Compression:
  - **bloom filter fp chance**: build-time target false-positive rate used when constructing `Filter.db`. Runtime observed FPR may diverge as the filter saturates or key distribution shifts; validate empirically and rebuild if drift is unacceptable.
  - **compressor / compression ratio**: algorithm and computed ratio for `Data.db`
- Cardinality and Sizes:
  - **estimated cardinality**: approximate partition count
  - **estimated partition size histogram**: size distribution (bytes) with percentiles
  - **estimated column count histogram**: columns per partition distribution with percentiles
- Topology and Repair:
  - **level**: LCS level (0 for STCS/TWCS)
  - **repaired at / pending repair / originating host id**: repair metadata
  - **covered commit log positions**: replay coverage

These fields drive compaction policies (e.g., tombstone purging thresholds), read heuristics (e.g., read-ahead sizing), and operational insights (e.g., skew from partition histograms).

## Collection and Serialization

Statistics are collected during flush and serialized alongside component files. Readers parse `Statistics.db` to provide summaries and drive decisions (e.g., compaction tuning, bloom FPR reporting).

Pinpoints in Cassandra 5.0.8:
- `MetadataCollector` gathers live stats during flush (row counts, histograms)
- `MetadataSerializer` writes and reads the metadata blocks
- `StatsMetadata` exposes typed accessors for the above

For an implementation walkthrough of parsing and reporting helpers, see Appendix C.

## Operational Implications

- Compaction strategies consider levels, droppable tombstones, and partition histograms.
- Read path can report expected Bloom FPR and compression effectiveness.

#### Performance and Capacity Planning
- Use the partition-size histogram percentiles (P50/P95/P99) to set read-ahead and block cache sizing.
- Droppable tombstone estimates help target compaction to reclaim space.
- Compression ratio trends indicate if chunk sizes or algorithms need tuning (see Ch. 9).

#### Troubleshooting Pointers
- Unexpectedly high `bloom fp chance` often indicates a mis-sized Bloom at write time; verify `bloom_filter_fp_chance` and key cardinality.
- Large gap between min/max timestamp suggests hot + cold data mixing; check compaction strategy alignment (Ch. 15).
- Level > 0 with STCS may indicate previous LCS usage or tooling inconsistencies; confirm table options.

### Key Takeaways
- `Statistics.db` exposes key health and distribution signals for the table.
- Min/max timestamps and histograms drive maintenance and expectations on reads.
- Compression info and bloom FPR here help explain IO and false positives.

### Example Walkthrough (trimmed → interpretation)
- The sample shows P50 partition size ≈ 770 B and totalRows=1000, implying light rows and low IO per partition, favoring read-ahead windows at or near one chunk (see Ch. 9).
- Compression ratio ≈ 0.98 suggests low compressibility (random-looking bytes in `description`), so prioritize CPU over disk savings.

## SerializationHeader Component

Statistics.db in Cassandra 5.0 (nb-format) also contains an embedded **SerializationHeader** component that defines the table schema used when writing the SSTable. This is critical for correctly deserializing Data.db content.

### Binary Format

The SerializationHeader follows this structure (from `SerializationHeader.java`):

```
[UnsignedVInt minTimestamp_delta]              -- 64-bit delta from TIMESTAMP_EPOCH (µs)
[UnsignedVInt32 minLocalDeletionTime_delta]    -- 32-bit delta from DELETION_TIME_EPOCH (s)
[UnsignedVInt32 minTTL_delta]                  -- 32-bit delta from TTL_EPOCH (0)
                                               -- (EncodingStats block: 3–14 bytes total)
[VInt pk_type_len] [pk_type_string]            -- partition key type
[UnsignedVInt32 ck_count]                      -- clustering key count
  for each clustering key:
    [VInt ck_len] [ck_type_string]             -- clustering key type
[UnsignedVInt32 static_count]                  -- static column count (0 if none)
  for each static column:
    [VInt name_len] [name] [VInt type_len] [type]
[UnsignedVInt32 reg_count]                     -- regular column count
  for each regular column:
    [VInt name_len] [name] [VInt type_len] [type]
```

**Key insight**: When `static_count = 0`, the VInt encodes as `0x00`. This can appear to be a separator, but it is actually the static column count. Tables with static columns will have `static_count > 0` and include the static column definitions between clustering keys and regular columns.

### Example: Table with Static Columns

For `static_columns_table` with schema:
- Partition key: `id` (uuid)
- Clustering key: `event_time` (timestamp)
- Static column: `static_data` (text)
- Regular columns: `row_data` (text), `row_value` (int)

The SerializationHeader contains:
```
pk_type: org.apache.cassandra.db.marshal.UUIDType
ck_count: 1
ck_types: [org.apache.cassandra.db.marshal.TimestampType]
static_count: 1
static_columns: [{name: "static_data", type: "UTF8Type"}]
reg_count: 2
regular_columns: [{name: "row_data", type: "UTF8Type"}, {name: "row_value", type: "Int32Type"}]
```

### References
- Cassandra 5.0.8:
  - [`StatsMetadata`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/metadata/StatsMetadata.java)
  - [`MetadataCollector`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/metadata/MetadataCollector.java)
  - [`MetadataSerializer`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/metadata/MetadataSerializer.java)
  - [`SerializationHeader`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/SerializationHeader.java)
  - [`EncodingStats`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/rows/EncodingStats.java)

For implementation details, see Appendix C.

## Writing Statistics.db

CQLite's M5 implementation produces Statistics.db files compatible with Cassandra 5.0's nb-format. The writer generates a **hybrid format** that satisfies both the TOC-based parser and the simplified nb-format header parser.

### Hybrid Format Structure

The Statistics.db file written by CQLite uses a 32-byte header followed by EncodingStats data:

```text
Bytes 0-31: Header (doubles as fake TOC)
  [u32 BE] 4                    - Interpreted as num_components or version
  [u32 BE] 0x26291b05           - Statistics magic number
  [u32 BE] 0                    - Reserved
  [u32 BE] data_length          - Length of EncodingStats data
  [u32 BE] 1, 0x65, 2, 0        - Metadata fields (observed in real files)

Bytes 32+: EncodingStats data
  [u32 BE] 3                    - Metadata type (EncodingStats marker)
  [VUInt]  0                    - Data length placeholder
  [VUInt]  43                   - Partitioner string length
  [bytes]  Murmur3Partitioner   - Partitioner class name
  [VUInt]  0, 0                 - Metadata placeholders
  [UnsignedVInt]   min_timestamp_delta     - Unsigned VInt 64-bit (delta from TIMESTAMP_EPOCH)
  [UnsignedVInt32] min_deletion_time_delta - Unsigned VInt32 (delta from DELETION_TIME_EPOCH)
  [UnsignedVInt32] min_ttl_delta            - Unsigned VInt32 (delta from TTL_EPOCH=0)
```

**Magic Number**: The constant `0x26291b05` appears at bytes 4-7 and serves dual purposes:
- `statistics_kind` when read by `parse_nb_format_header()`
- `checksum` when read by the single-pass `parse_statistics_toc()` TOC walk (issue #2148)

**Version/Components**: The value `4` at bytes 0-3 indicates:
- `version_type = 4` (Cassandra 5.0 format) for nb-format parsers
- `num_components = 4` for TOC-based parsers

This hybrid approach allows a minimal Statistics.db to be readable by multiple parser implementations without requiring the full TOC structure with VALIDATION, COMPACTION, STATS, and HEADER components.

### Delta Encoding Baselines

The primary purpose of Statistics.db is to provide **baseline values for delta encoding** in Data.db. Three critical fields establish these baselines:

#### min_timestamp
- **Purpose**: Baseline for timestamp delta encoding
- **Unit**: Microseconds since Unix epoch
- **Encoding**: `writeUnsignedVInt` (64-bit unsigned VInt, up to 8 bytes; delta from
  `TIMESTAMP_EPOCH` = Sept 22 2015 in µs). **Not** ZigZag/signed.
- **Usage**: Data.db encodes cell timestamps as deltas from this value

#### min_local_deletion_time
- **Purpose**: Baseline for tombstone deletion time encoding
- **Unit**: Seconds since Unix epoch
- **Encoding**: `writeUnsignedVInt32` (32-bit unsigned VInt, up to 5 bytes; delta from
  `DELETION_TIME_EPOCH` = Sept 22 2015 in seconds). **Not** ZigZag/signed.
- **Usage**: Tombstone deletion times in Data.db are encoded as deltas from this value

#### min_ttl
- **Purpose**: Baseline for TTL delta encoding
- **Unit**: Seconds
- **Encoding**: `writeUnsignedVInt32` (32-bit unsigned VInt, up to 5 bytes; delta from
  `TTL_EPOCH` = 0). **Not** ZigZag/signed.
- **Usage**: Cell TTL values in Data.db are encoded as deltas from this value
- **Special case**: If no TTL is used in the SSTable, this value is set to 0

**Critical ordering**: Statistics.db MUST be written BEFORE Data.db, as the Data.db writer requires these baseline values to encode timestamps, deletion times, and TTL values correctly.

### EncodingStats Serialization

The EncodingStats section (starting at byte 32) uses a specific VInt encoding sequence:

1. **Metadata type marker** (`u32 BE` = 3): Identifies the start of EncodingStats data
2. **Data length placeholder** (`VUInt` = 0): Reserved field read and discarded by parser
3. **Partitioner string**:
   - Length as `VUInt` (43 bytes for Murmur3Partitioner)
   - Full class name: `org.apache.cassandra.dht.Murmur3Partitioner`
4. **Metadata placeholders** (2x `VUInt` = 0): Purpose unclear, observed in real files
5. **Delta encoding baselines** (3× unsigned VInt):
   - `min_timestamp` (i64 delta): `writeUnsignedVInt` (64-bit, up to 8 bytes)
   - `min_local_deletion_time` (i32 delta): `writeUnsignedVInt32` (32-bit, up to 5 bytes)
   - `min_ttl` (i32 delta): `writeUnsignedVInt32` (32-bit, up to 5 bytes)

**VInt encoding**: All baseline values use **unsigned** VInt encoding. Deltas are always
non-negative (subtracted from their respective epochs). ZigZag (signed) encoding is NOT
used. Using a 32-bit VInt for `minTimestamp` will corrupt timestamps after 2037. See
[`EncodingStats.java:272–276`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/rows/EncodingStats.java#L272) and Appendix B for VInt encoding details.

### Statistics Metadata Collection

During memtable flush, the writer tracks the following metadata (subset shown):

```rust
pub struct StatisticsMetadata {
    pub min_timestamp: i64,           // Microseconds
    pub max_timestamp: i64,           // Microseconds
    pub min_local_deletion_time: i32, // Seconds
    pub max_local_deletion_time: i32, // Seconds
    pub min_ttl: i32,                 // Seconds
    pub max_ttl: i32,                 // Seconds
    pub partition_count: u64,
    pub row_count: u64,
    pub column_count: u64,
    pub total_rows_size: u64,
    pub tombstone_histogram: TombstoneHistogram, // estimatedTombstoneDropTime
}
```

**Update methods**:
- `update_timestamp(i64)`: Tracks min/max timestamp range
- `update_local_deletion_time(i32)`: Tracks the deletion time range for tombstones **and**
  accumulates the value into the `estimatedTombstoneDropTime` histogram (see below)
- `update_ttl(i32)`: Tracks TTL range (ignores 0 values)
- `increment_partition_count()`: Counts partitions written
- `increment_row_count()`: Counts rows (live + tombstones)

**Finalization**: Before writing, sentinel values (i64::MAX, i32::MAX) are normalized to 0 if no actual values were recorded. This ensures valid baselines even for empty SSTables.

### Implementation Reference

For the complete write-side implementation, see:
- `cqlite-core/src/storage/sstable/writer/stats_writer.rs`: Statistics.db writer
- `cqlite-core/src/storage/sstable/writer/data_writer.rs`: Data.db writer that uses these baselines

### estimatedTombstoneDropTime histogram (STATS component)

The STATS component carries `estimatedTombstoneDropTime`, the histogram of tombstone
local-deletion-times that Cassandra uses to compute `estimatedDroppableTombstoneRatio`
and schedule tombstone compaction. CQLite emits this histogram from its own writer
(verified in `cqlite-core/src/storage/sstable/writer/stats_writer.rs`).

Structure and algorithm:

- **Bins.** A streaming histogram keyed by `local_deletion_time` (seconds since epoch),
  each bin holding a tombstone count. CQLite mirrors Cassandra's
  `StreamingTombstoneHistogramBuilder` with a fixed cap of **100 bins**
  (`TOMBSTONE_HISTOGRAM_MAX_BIN_SIZE`). When the cap is exceeded, the two **nearest**
  bins are merged: the merged key is the count-weighted average of the two points and
  the merged value is the sum of the counts (`merge_closest_bins`, matching Cassandra's
  `mergeNearestBins`).
- **Legacy (nb) serialization** — `write_tombstone_histogram`:

  ```text
  i32 BE  maxBinSize   (100 when non-empty, 0 when empty)
  i32 BE  size         (number of bins)
  for each bin:
    f64 BE  point       (local-deletion-time as a double)
    i64 BE  value       (tombstone count in this bin)
  ```

  An empty histogram serializes as 8 bytes (`maxBinSize = 0, size = 0`).

- **Determinism / byte-stability (scope-limited).** Bins are stored in a
  `BTreeMap<i64, i64>` and serialized in ascending key order, so for a **given final
  bin set** the serialized bytes are order-stable. This is NOT a full
  insertion-order-independence guarantee: the histogram is built by **streaming
  insertion** (`update`), and once the input exceeds the 100-bin cap the builder
  merges the two **nearest** bins on the fly (`merge_closest_bins`). Because the
  merged key is the count-weighted average of whichever two bins happen to be
  adjacent at that moment, the resulting merged bins — and thus the final byte
  output — can differ depending on the **order** in which the same multiset of
  deletion times was inserted. So byte-identity across different insertion orders of
  the same deletion-time multiset is **not guaranteed**, and `stats_writer.rs` does
  **not** assert it. What the tests in `stats_writer.rs` DO assert (issue #730) is
  the framing: a non-empty histogram reports `maxBinSize = 100`, an empty one reports
  `maxBinSize = 0, size = 0`, and below the cap there is one bin per distinct
  deletion time (no byte-stability / insertion-order-independence assertion above the
  cap).

Authority: `org.apache.cassandra.utils.streamhist.StreamingTombstoneHistogramBuilder`
and `org.apache.cassandra.io.sstable.metadata.StatsMetadata` (`estimatedTombstoneDropTime`).

### Full Cassandra TOC Structure (Not Implemented)

For reference, full Cassandra Statistics.db files contain a complete TOC with four components:

1. **TOC**: `num_components` (4) + checksum + 4 component entries (32 bytes total)
2. **VALIDATION component** (offset ~44): Validator class name
3. **COMPACTION component**: Compaction metadata
4. **STATS component**: Contains EncodingStats plus histograms, cardinality estimates
5. **HEADER component**: SerializationHeader with full table schema

CQLite's minimal implementation writes only the EncodingStats baseline values needed for Data.db decoding. Future versions may add support for the complete metadata structure.


