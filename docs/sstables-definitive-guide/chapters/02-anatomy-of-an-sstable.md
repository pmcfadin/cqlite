## Anatomy of an SSTable

This chapter explains each SSTable component and how they fit together. Cassandra writes a family of files per SSTable generation; `TOC.txt` enumerates the components present and serves as the invariant list for integrity checks and lifecycle tooling.

### In this chapter you will learn
- The role of each component file and how they interact
- Versioning and feature flags across formats
- How schema and CQL types map to on-disk encodings
- How directory layout and TOC establish invariants
- Where BTI (5.0) differs from `big` in component internals

## Components Overview

- `Data.db`: The primary row/partition data, optionally compressed in fixed-size chunks.
- `Index.db`: Per-partition index entries mapping key digests to `Data.db` positions; may include promoted index data for wide partitions.
- `Summary.db`: A sampled (promoted) index to accelerate binary search into `Index.db`.
- `Filter.db` (Bloom): Probabilistic membership filter to skip non-existent partitions.
- `Statistics.db`: SSTable-level metadata (timestamps, histograms, repair/level info, min/max tokens, etc.).
- `CompressionInfo.db`: Algorithm, chunk length, and the map of compressed chunk offsets (and optional per-chunk CRCs) for `Data.db`.
- `Digest.crc32`: Digest file for end-to-end integrity.
- `TOC.txt`: Text file listing the components present; tools use it to validate completeness.

> **Publication barrier = `TOC.txt`**. See “TOC Invariants and Integrity Checks” below and Chapter 16 (“SSTable Lifecycle and Maintenance”) for a practical checklist and tooling pointers.

Diagram: sstable components and relationships
- Alt text: Component diagram showing Data, Index, Summary, Bloom, Stats, CompressionInfo, TOC
![SSTable components and relationships](diagrams/sstable-components)
- Caption: How SSTable components reference each other during reads

## Versioning and Feature Flags

SSTable formats evolved over time. In 5.0, BTI (B-Tree/Trie Indexed) coexists with the long-standing `big` family. The component set is stable, but internal formats and metadata change with version/feature flags. The `Descriptor` defines the `{format}` segment and controls feature availability, while `StatsMetadata` evolves fields used by compaction heuristics and read optimizations. See Chapter 17 for BTI details.

### Format Version Evolution (concise)
| Area | Pre-5.x (big) | 5.0 (BTI/big) |
|---|---|---|
| Index entry | Simple digest list; promoted index optional | BTI adjusts indexing structures; promoted index layout may differ |
| Summary | Sampling rate + offsets | Token-sorted with explicit index offsets |
| CompressionInfo | Offsets only (legacy) | Optional per-chunk CRCs; format detection supported |
| Statistics | Fewer fields | Expanded histograms/repair/level fields |

## Schema and Type Mapping

On-disk encodings derive from the table schema (partition/clustering keys and column types). Cassandra’s `SerializationHeader` computes how values are encoded; in this guide we cross-reference concrete mappings in Appendix A (types) and Appendix B (encodings cheat sheet). For an implementation walkthrough, see Appendix C.

## Directory Listing Example

Trimmed listing from `test_basic/simple_table` (one SSTable generation):

```text
nb-1-big-CompressionInfo.db
nb-1-big-Data.db
nb-1-big-Digest.crc32
nb-1-big-Filter.db
nb-1-big-Index.db
nb-1-big-Statistics.db
nb-1-big-Summary.db
nb-1-big-TOC.txt
```

The `TOC.txt` inside the same directory confirms the components present:

```1:9:/Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_basic/simple_table-6de93b70934a11f08d448925b7a9e804/nb-1-big-TOC.txt
Data.db
Statistics.db
Digest.crc32
TOC.txt
CompressionInfo.db
Filter.db
Index.db
Summary.db
```

### BTI directory example (5.0)

When BTI is enabled, a generation includes BTI-specific components alongside common ones. During upgrades, directories may contain both BIG and BTI generations. Real filenames (trimmed):

```text
na-3-bti-Data.db
na-3-bti-Partitions.db
na-3-bti-Rows.db
na-3-bti-Statistics.db
na-3-bti-TOC.txt
na-3-bti-Digest.crc32
```

See the BTI package for details: `org.apache.cassandra.io.sstable.format.bti`.

## TOC Invariants and Integrity Checks

`TOC.txt` is authoritative: tools validate that every listed component exists and that unexpected files do not appear. Integrity checks commonly include:
- Presence: Each required component listed in `TOC.txt` exists on disk
- Consistency: `Statistics.db` and `CompressionInfo.db` fields align with observed file sizes and counts
- Cross-component alignment: `Index.db` positions must resolve into valid `Data.db` boundaries; `Summary.db` samples must be sorted and within token range
- Digest validation: `Digest.crc32` matches computed digests over the appropriate component payloads

### Sidebar: Version Differences (3.x/4.x)

- File family remains multi-component; feature flags and index internals differ
- `Descriptor` format tags (`big`, `mc/mm`, `bti`) encode capabilities; 5.0 introduces BTI
- Statistics fields expanded over time; tooling output formatting changed subtly

### Key Takeaways
- `TOC.txt` is authoritative for the component set in a given SSTable
- `Summary.db` samples `Index.db` to accelerate seeks; Bloom reduces unnecessary IO
- `CompressionInfo.db` is required to read compressed `Data.db`
- Version/format changes do not remove the core components, but affect internal structure

### References

- Cassandra 5.0.0 (pinned):
  - `Descriptor` — `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/Descriptor.java`
  - `StatsMetadata` — `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/metadata/StatsMetadata.java`
  - `IndexSummary` — `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/IndexSummary.java`
Reference (diagram source):
```1:10:/Users/patrick/local_projects/cqlite/docs/sstables-definitive-guide/diagrams/sstable-components.mmd
%% SSTable component relationship diagram (stub)
flowchart TD
  A[Memtable] -->|Flush| B[Data.db]
  A -->|Flush| C[Index.db]
  A -->|Flush| D[Summary.db]
  A -->|Flush| E[Filter.db]
  A -->|Flush| F[Statistics.db]
  A -->|Flush| G[CompressionInfo.db]
```

For implementation details, see Appendix C.

