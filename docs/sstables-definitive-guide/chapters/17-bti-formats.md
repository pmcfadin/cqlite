## BTI (B-Tree/Trie Indexed) Formats

BTI is the modern SSTable index format introduced to improve lookup efficiency, cache locality, and on-disk structure over the classic `big` family. Instead of a single `Index.db` plus sampled `Summary.db`, BTI splits indexing into trie-structured files that directly encode byte-comparable keys, reducing indirection and making prefix and range navigation more predictable. This chapter contrasts BTI with big/mc/mm and calls out practical impacts on read amplification.

### In this chapter you will learn
- What BTI changes relative to big/mc/mm
- How BTI’s index structures affect read paths
- Where BTI lives in the codebase
- Practical implications for latency

## Motivation and Structure
BTI (B-Tree/Trie Indexed) replaces the classic `big` index structure with trie-based indexes that favor prefix navigation and reduce binary-search hops across large sampled summaries. In Cassandra 5.0, BTI artifacts live alongside the data file and statistics:

- BTI-specific components: `Partitions.db` (partition trie), `Rows.db` (per-partition clustering trie)
- Common components retained: `Data.db`, `Statistics.db`, `TOC.txt`, `Digest.crc32`, `CompressionInfo.db` (when compressed)

Where to look in source:
- Cassandra 5.0.0 (pinned): `org.apache.cassandra.io.sstable.format.bti` — see package directory
  - `https://github.com/apache/cassandra/tree/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/format/bti`
- Classic big format for comparison:
  - Reader: `BigTableReader`
    `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/format/big/BigTableReader.java`

Sidebar: Version Differences
BTI is a Cassandra 5.x format family. Older releases rely on `big` plus `Index.db`/`Summary.db`. Readers should expect co-existence during upgrades; mixed-format directories are normal during transitions.

## Read Amplification and Index Layout
Conceptual contrast (trimmed):

- big: `Index.db` (per-partition entries) + `Summary.db` (sampling) → seek into `Data.db`
- BTI: `Partitions.db` trie → partition payload; then `Rows.db` trie (within-partition) → row payload in `Data.db`

Illustrative bullets:
- Fewer binary-search steps against sampled summaries; trie traversal uses byte-wise transitions
- Better prefix navigation for wide-partition clustering keys
- Similar Bloom filter role for negative lookups; statistics unchanged
- Mixed deployments are supported; compaction/upgrade can rewrite formats

For implementation walkthroughs of BTI headers and trie navigation, see Appendix C.

## Performance Considerations and Benchmark Methodology

Note: Provide methodology and harness only; do not claim specific results here.

- Goals:
  - Compare point lookup and slice traversal costs for BTI vs BIG across key distributions
  - Measure IO and CPU separately where possible (warm vs cold cache runs)
- Dataset: Use `test-data/datasets/test_basic` plus synthetic wide-partition variants
- Metrics:
  - Median/95p latency for partition key lookups and clustering slices
  - Hops/steps: trie transitions vs binary-search steps; bytes read from `Index/Partitions/Rows`
- Procedure:
  - Run N repeated lookups against a fixed corpus; alternate hot/cold cache
  - Record OS-level IO and per-query timings; pin CPU governor when possible
- Harness guidance:
  - Use consistent datasets and key distributions across formats
  - Ensure format detection is bypassed in the hot path to avoid skew
  - Report confidence intervals; avoid extrapolating beyond tested sizes

### Key Takeaways
- BTI uses trie indexes (`Partitions.db`/`Rows.db`) instead of `Index.db`/`Summary.db`.
- Trie traversal replaces some binary searches, improving predictability for wide partitions.
- Bloom filters and statistics continue to guide/guard the read path.
- Mixed-format directories occur during upgrades; readers must detect format.
For implementation details, see Appendix C.

### References
- Cassandra 5.0.0:
  - BTI package: `https://github.com/apache/cassandra/tree/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/format/bti`
  - Big format reader (contrast): `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/format/big/BigTableReader.java`
  
For implementation details, see Appendix C.



