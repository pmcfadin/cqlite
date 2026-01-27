## BTI (B-Tree/Trie Indexed) Formats

BTI is the modern SSTable index format introduced to improve lookup efficiency, cache locality, and on-disk structure over the classic `big` family. Instead of a single `Index.db` plus sampled `Summary.db`, BTI splits indexing into trie-structured files that directly encode byte-comparable keys, reducing indirection and making prefix and range navigation more predictable. This chapter contrasts BTI with big/mc/mm and calls out practical impacts on read amplification.

### In this chapter you will learn
- What BTI changes relative to big/mc/mm
- How BTI’s index structures affect read paths
- Where BTI lives in the codebase
- Practical implications for latency

## Motivation and Structure
BTI (B-Tree/Trie Indexed) replaces the classic `big` index structure with trie-based indexes that favor prefix navigation and reduce binary-search hops across large sampled summaries. In Cassandra 5.0, BTI artifacts live alongside the data file and statistics:

### Connection to In-Memory Tries: The Efficiency Foundation

> **Cross-reference**: This section connects to [Chapter 4: From CQL to Disk](./04-from-cql-to-disk.md), which covers the flush pipeline from memtable to SSTable.

BTI's efficiency is not just an on-disk optimization—it is architecturally aligned with Cassandra 5.0's in-memory `TrieMemtable`. The on-disk BTI structure is essentially a **direct persistence of the efficient in-memory trie concepts**, which dramatically reduces flush complexity and overhead.

**Key alignment points:**

1. **Identical byte-comparable representation**: Both `TrieMemtable` and BTI use `ByteComparable.Version.OSS50` for partition key encoding. This means keys in memory are already in the exact format needed for the on-disk trie—no transformation required during flush.

2. **No sorting pass required**: Traditional memtables (like the older `SkipListMemtable`) stored data in a structure that required iteration to produce sorted output. The `TrieMemtable` stores partition keys in a trie that is inherently sorted by byte-comparable order. The `entryIterator()` method walks the trie and emits partitions in exactly the order BTI expects.

3. **Prefix sharing preserved**: The in-memory trie shares prefixes between partition keys (e.g., keys `user:alice` and `user:bob` share the `user:` prefix). During flush, the `IncrementalTrieWriter` constructs the on-disk trie incrementally and naturally preserves this prefix structure.

4. **Single-pass incremental construction**: The BTI writer receives pre-sorted keys from the memtable trie iterator and builds the `Partitions.db` index in a single pass using `PartitionIndexBuilder`. No buffering, no intermediate structures, no second pass.

**Pseudocode illustrating the alignment:**

```text
// Flush path (simplified)
for entry in memtableTrie.entryIterator():    // Already sorted!
  key = entry.getKey()                         // DecoratedKey implements ByteComparable
  partition = entry.getValue()

  position = dataWriter.write(partition)       // Write to Data.db
  partitionIndexBuilder.addEntry(key, position) // Key treated as ByteComparable (OSS50)

// PartitionIndexBuilder internally:
//   - Computes diff point with previous key (ByteComparable.diffPoint)
//   - Writes only the shortest unique prefix to trie
//   - Uses IncrementalTrieWriter for page-aware output
```

**Position encoding trick:** The partition index uses position sign to distinguish pointer types:
- **Positive position** → points to row index file (`Rows.db`) for wide partitions
- **Negative position (`~dataPosition`)** → bitwise NOT of direct `Data.db` offset (e.g., position 0 → -1, position 1 → -2)

This encoding eliminates the need for a separate flag field and allows the reader to immediately know whether to consult the row index.

**Why this matters for performance:**

| Aspect | Big Format (classic) | BTI with TrieMemtable |
|--------|---------------------|----------------------|
| Key format during flush | May need transformation | Already byte-comparable |
| Sort guarantee | Iterator provides order | Trie iteration is inherently ordered |
| Prefix sharing | None (full keys in Index.db) | Preserved memory→disk |
| Construction passes | Multiple (data, index, summary) | Single incremental pass |

This alignment was intentionally designed as described in the [VLDB 2022 paper](https://www.vldb.org/pvldb/vol15/p3359-lambov.pdf) that introduced `TrieMemtable` to Cassandra.

**Where to look in source:**
- `TrieMemtable`: `org.apache.cassandra.db.memtable.TrieMemtable` — see `getFlushSet()` method (lines 360-493)
  - [https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/db/memtable/TrieMemtable.java](https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/db/memtable/TrieMemtable.java)
- `PartitionIndexBuilder`: `org.apache.cassandra.io.sstable.format.bti.PartitionIndexBuilder` — builds `Partitions.db` from sorted byte-comparable keys
  - [https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/format/bti/PartitionIndexBuilder.java](https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/format/bti/PartitionIndexBuilder.java)
- `IncrementalTrieWriter`: `org.apache.cassandra.io.tries.IncrementalTrieWriter` — incremental trie construction from sorted input
  - [https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/tries/IncrementalTrieWriter.java](https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/tries/IncrementalTrieWriter.java)

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

### Prefix/range navigation (byte-wise example)

Consider a composite clustering key `(user_id uuid, path text)` with UTF-8 collation. BTI’s `Rows.db` encodes a trie over the byte sequence of the clustering prefix, enabling prefix seeks:

Pseudo (simplified):
```text
advance(trie, prefix_bytes):
  node = root
  for b in prefix_bytes:
    if node.has_child(b):
      node = node.child(b)
    else:
      return node.first_ge_branch(b)
  return node
```

Effectively, prefix seek walks byte-by-byte until divergence, then takes the first branch ≥ the requested byte; this contrasts with BIG’s binary search over sampled entries in `Summary.db` followed by scans in `Index.db`.

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



