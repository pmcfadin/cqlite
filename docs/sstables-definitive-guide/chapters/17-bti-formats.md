## BTI (Big Trie-Indexed) Formats

BTI is the modern SSTable index format introduced to improve lookup efficiency, cache locality, and on-disk structure over the classic `big` family. Instead of a single `Index.db` plus sampled `Summary.db`, BTI splits indexing into trie-structured files that directly encode byte-comparable keys, reducing indirection and making prefix and range navigation more predictable. This chapter contrasts BTI with big/mc/mm and calls out practical impacts on read amplification.

### In this chapter you will learn
- What BTI changes relative to big/mc/mm
- How BTI’s index structures affect read paths
- Where BTI lives in the codebase
- Practical implications for latency

## Motivation and Structure
BTI (Big Trie-Indexed) replaces the classic `big` index structure with trie-based indexes that traverse keys byte-by-byte instead of binary-searching a sampled summary—reading less data and requiring less processing per lookup, with equivalent seek counts on a match ([`BtiFormat.md` lines 581–589](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/BtiFormat.md#L581)). In Cassandra 5.0, BTI artifacts live alongside the data file and statistics:

### Connection to In-Memory Tries: The Efficiency Foundation

> **Cross-reference**: This section connects to [Chapter 4: From CQL to Disk](./04-from-cql-to-disk.md), which covers the flush pipeline from memtable to SSTable.

BTI's efficiency is not just an on-disk optimization—it is architecturally aligned with Cassandra 5.0's in-memory `TrieMemtable`. Both use byte-comparable keys and trie organisation, but the flush path constructs a new on-disk trie incrementally from the memtable iterator; it is **not** a raw serialisation of the in-memory trie.

**Key alignment points:**

1. **Identical byte-comparable representation**: Both `TrieMemtable` and BTI use `ByteComparable.Version.OSS50` for partition key encoding. This means keys in memory are already in the exact format needed for the on-disk trie—no transformation required during flush.

2. **No sorting pass required**: `SkipListMemtable` (the compiled-in default factory in Cassandra 5.0 unless overridden in `cassandra.yaml`; see [`MemtableParams.java:99`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/schema/MemtableParams.java#L99)) stored data in a structure that required iteration to produce sorted output. `TrieMemtable` is an opt-in alternative: it stores partition keys in a trie that is inherently sorted by byte-comparable order. The `entryIterator()` method walks the trie and emits partitions in exactly the order BTI expects.

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

**Position encoding trick:** The partition index uses position sign to distinguish pointer types ([`BtiFormat.md` lines 965–971](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/BtiFormat.md#L965)):
- **Positive position** → points to row index file (`Rows.db`) for wide partitions
- **Negative position (`~dataPosition`)** → bitwise NOT of direct `Data.db` offset (e.g., position 0 → -1, position 1 → -2)

This encoding eliminates the need for a separate flag field and allows the reader to immediately know whether to consult the row index.

**Hash byte (Cassandra 5.0 always present):** Every leaf node in the partition trie carries a hash
byte before the position value. This byte holds the lowest 8 bits of the partition key's filter
hash and allows fast mismatch rejection without reading the full key from disk. When
`payloadBits >= 8` (`FLAG_HAS_HASH_BYTE = 8`), byte 0 at the payload position is the hash byte
and bytes 1–(`payloadBits`−8+1) encode the position. In Cassandra 5.0 the hash byte is
**always written** ([`PartitionIndex.java:131–135`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/PartitionIndex.java#L131);
[`BtiFormat.md` lines 946–963](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/BtiFormat.md#L946)).

**Why this matters for performance:**

| Aspect | Big Format (classic) | BTI with TrieMemtable |
|--------|---------------------|----------------------|
| Key format during flush | May need transformation | Already byte-comparable |
| Sort guarantee | Iterator provides order | Trie iteration is inherently ordered |
| Prefix sharing | None (full keys in Index.db) | Preserved memory→disk |
| Construction passes | Multiple (data, index, summary) | Single incremental pass |

This alignment was intentionally designed as described in the [VLDB 2022 paper](https://www.vldb.org/pvldb/vol15/p3359-lambov.pdf) that introduced `TrieMemtable` to Cassandra [external citation — not verified against 5.0.8 source].

**Where to look in source:**
- `TrieMemtable`: `org.apache.cassandra.db.memtable.TrieMemtable` — see `getFlushSet()` method (lines 350–403)
  - [https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/memtable/TrieMemtable.java](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/memtable/TrieMemtable.java)
- `PartitionIndexBuilder`: `org.apache.cassandra.io.sstable.format.bti.PartitionIndexBuilder` — builds `Partitions.db` from sorted byte-comparable keys
  - [https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/PartitionIndexBuilder.java](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/PartitionIndexBuilder.java)
- `IncrementalTrieWriter`: `org.apache.cassandra.io.tries.IncrementalTrieWriter` — incremental trie construction from sorted input
  - [https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/tries/IncrementalTrieWriter.java](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/tries/IncrementalTrieWriter.java)

- BTI-specific components: `Partitions.db` (partition trie), `Rows.db` (per-partition clustering trie)
- Common components retained: `Data.db`, `Statistics.db`, `TOC.txt`, `Digest.crc32`, `CompressionInfo.db` (when compressed)

> **Note**: BTI format SSTables do **not** include `Index.db` or `Summary.db`. These are
> components of the classic `big` format only. The complete required BTI component set is:
> `Data.db`, `Partitions.db`, `Rows.db`, `Statistics.db`, `TOC.txt`, `Digest.crc32`, and
> optionally `CompressionInfo.db`
> ([`BtiFormat.java:83–102`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/BtiFormat.java#L83)).

Where to look in source:
- Cassandra 5.0.8 (pinned): `org.apache.cassandra.io.sstable.format.bti` — see package directory
  - `https://github.com/apache/cassandra/tree/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti`
- Classic big format for comparison:
  - Reader: `BigTableReader`
    `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/big/BigTableReader.java`

Sidebar: Version Differences
BTI is a Cassandra 5.x format family. Older releases rely on `big` plus `Index.db`/`Summary.db`. Readers should expect co-existence during upgrades; mixed-format directories are normal during transitions.

## Read Amplification and Index Layout
Conceptual contrast (trimmed):

- big: `Index.db` (per-partition entries) + `Summary.db` (sampling) → seek into `Data.db`
- BTI: `Partitions.db` trie → partition payload; then `Rows.db` trie (within-partition) → row payload in `Data.db`

Illustrative bullets:
- Trie traversal reads less data and requires less processing per lookup than binary-searching sampled summaries; seek counts on a match are equivalent to the `big` format
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

### Trie node type families

Every trie node starts with one byte: high nibble (bits 7–4) = 4-bit node-type ordinal (0–15);
low nibble (bits 3–0) = 4 payload flag bits (_pb_). Four families
([`BtiFormat.md` lines 806–877](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/BtiFormat.md#L806);
[`TrieNode.java:947–969`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/tries/TrieNode.java#L947)):

| Family | Ordinals | Description |
|--------|----------|-------------|
| `PAYLOAD_ONLY` | 0 | Leaf; no transitions |
| `SINGLE` | 1–4 | One child; 4-/8-/12-/16-bit distance |
| `SPARSE` | 5–9 | Binary-searched byte list; 8- to 40-bit distances |
| `DENSE` | 10–15 | Consecutive byte range; 12- to 64-bit distances (`LONG_DENSE` catch-all) |

All distances are unsigned and subtracted from the current node position (children are earlier
in the file). See Appendix C for complete per-type byte layouts.

### `Rows.db` per-partition footer

Each partition's row index in `Rows.db` is padded to a 4096-byte page boundary. After the trie
pages the footer contains, in order
([`BtiFormat.md` lines 977–1010](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/BtiFormat.md#L977);
[`TrieIndexEntry.java:92–116`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/TrieIndexEntry.java#L92)):

1. Partition key (short-length-prefixed bytes)
2. Data file position of the partition start (unsigned vint)
3. Root node position: signed vint delta relative to the data file position
4. Row count in the partition (unsigned vint)
5. Partition deletion time (12 bytes: local deletion time int + marked-for-delete-at long)

The partition index (`Partitions.db`) points to the position of the partition key bytes (step 1).

### CQLite reader: footer root, position sign, and `Rows.db` entry resolution

These BTI facts are verified against CQLite's own reader
(`cqlite-core/src/storage/sstable/bti/parser.rs`) in addition to the Cassandra spec:

- **No header; root offset is the footer.** `Partitions.db` has no header. CQLite
  reads the trie root's absolute byte offset from the **last 8 bytes** of the file
  (big-endian `u64`), then treats everything before that footer as the trie body
  (`lookup_partition_in_bti_file` / `load_bti_trie_via_footer`). Matches
  `PartitionIndex.java` and `BtiFormat.md`.

- **Leaf payload and position sign.** A partition leaf (PayloadOnly, ordinal 0)
  with `payloadBits >= 8` lays out `[hash byte][SizedInts position]` where the
  `SizedInts` byte count is `payloadBits − 8 + 1`. CQLite decodes the sign exactly
  as `PartitionIndex.java`: a **negative** position yields a direct `Data.db` offset
  `~position` (`BtiPartitionLocation::DataOffset`); a **non-negative** position is a
  `Rows.db` offset (`BtiPartitionLocation::RowsOffset`).

- **`RowsOffset` is a `TrieIndexEntry`, not a trie root.** The non-negative position
  points at the partition's per-partition row-index **entry** in `Rows.db`, which
  CQLite deserializes (`resolve_rows_db_entry`) to recover the real row-index trie
  root before traversing. The entry layout is
  `[u16 key_length][key bytes][data position: unsigned vint][trieRoot − base: SIGNED
  vint][block count: unsigned vint][partition DeletionTime]`, with
  `base = RowsOffset + key_length` and `indexTrieRoot = readVInt() + base` (Cassandra
  `TrieIndexEntry.deserialize`). Feeding `RowsOffset` straight into a trie walker
  parses entry metadata as a node and fails. Verified against the `wide_table`
  fixture in `cqlite-core/tests/issue_832_bti_traversal.rs`; cited to
  `TrieIndexEntry.java` (`serialize`/`deserialize`) and `RowIndexWriter.complete`.

- **Trie distances are backward.** All child distances are unsigned and subtracted
  from the current node position (children are written before parents / at lower
  offsets), consistent with the node-family table above.

> **Scope note (CQLite-specific).** The items here describe CQLite's BTI **reader**.
> CQLite's *write* path emits the classic `big`/`nb` format, not BTI; BTI writing is
> out of scope (epic #762). The byte-invariant test
> `issue_821_writer_byte_invariants.rs::finding16_bti_partition_leaf_data_offset_roundtrips_over_2gib`
> only exercises the leaf `SizedInts` offset encoder in isolation to prove 64-bit
> offset safety — it is not a full BTI writer.

### Row index granularity

The row index does not index every row—it indexes blocks. The default block size is **at least
16 KB** of serialised row data, controlled by the `column_index_size` parameter in
`cassandra.yaml`. Separator keys (the shortest prefix greater than the last key of the prior
block) are stored, not exact start keys, keeping the index compact
([`BtiFormat.md` lines 646–653](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/BtiFormat.md#L646)).

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
- BTI stands for **Big Trie-Indexed**; it uses trie indexes (`Partitions.db`/`Rows.db`) and does not include `Index.db` or `Summary.db`.
- Trie traversal reads less data and requires less CPU per lookup than binary-searching sampled summaries; seek counts on a match are equivalent.
- Every partition-index leaf node carries a hash byte (`FLAG_HAS_HASH_BYTE = 8`) for fast mismatch rejection—always present in Cassandra 5.0.
- `SkipListMemtable` remains the compiled-in default in Cassandra 5.0; `TrieMemtable` is opt-in via `cassandra.yaml`.
- The row index operates on 16 KB blocks (configurable via `column_index_size`), not individual rows.
- Bloom filters and statistics continue to guide/guard the read path.
- Mixed-format directories occur during upgrades; readers must detect format.
For implementation details, see Appendix C.

### References
- Cassandra 5.0.8 (pinned):
  - BTI package: `https://github.com/apache/cassandra/tree/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti`
  - Authoritative in-tree spec: `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/BtiFormat.md`
  - `BtiFormat.java` (component sets): `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/BtiFormat.java#L83`
  - `PartitionIndex.java` (hash byte, position encoding): `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/PartitionIndex.java`
  - `TrieNode.java` (node types): `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/tries/TrieNode.java#L947`
  - `PageAware.java` (4096-byte page constant): `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/util/PageAware.java#L24`
  - `MemtableParams.java` (default factory): `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/schema/MemtableParams.java#L99`
  - Big format reader (contrast): `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/big/BigTableReader.java`

For implementation details, see Appendix C.



