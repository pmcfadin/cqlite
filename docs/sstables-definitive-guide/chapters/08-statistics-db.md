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

Pinpoints in Cassandra 5.0.0:
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

### References
- Cassandra 5.0.0:
  - `StatsMetadata`: [org.apache.cassandra.io.sstable.metadata.StatsMetadata](https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/metadata/StatsMetadata.java)
  - `MetadataCollector`: [org.apache.cassandra.io.sstable.metadata.MetadataCollector](https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/metadata/MetadataCollector.java)
  - `MetadataSerializer`: [org.apache.cassandra.io.sstable.metadata.MetadataSerializer](https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/metadata/MetadataSerializer.java)
  
For implementation details, see Appendix C.


