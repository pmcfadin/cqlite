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

Cassandra 5.0.8 declares exactly one BTI version: `BtiVersion.current_version = "da"` and
`earliest_supported_version = "da"`
([`BtiFormat.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/BtiFormat.java)),
so **`da` is the only BTI version that exists on disk**. Everything in this chapter describes `da`.
CQLite matches that floor: `BtiVersionGates::from_version` rejects any non-`da` BTI version with
`Error::UnsupportedVersion`.

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

### Prefix/range navigation: the `separatorFloor` walk

Consider a composite clustering key `(user_id uuid, path text)`. BTI's `Rows.db` encodes a trie over
the byte-comparable (`ByteComparable.Version.OSS50`) encoding of the clustering prefix. Because the
trie stores **separators** rather than block start keys (see "Row index separator semantics" below),
locating the block that could hold a key `K` is a **floor** query — the largest separator `<= K` —
not a "first branch `>=` the requested byte" ceiling query.

Cassandra implements it as `RowIndexReader.separatorFloor(K)`, a single downward walk built on
`Walker.prefixAndNeighbours` that tracks two things as it descends: the closest **prefix payload**
(a separator that is a prefix of `K`) and the closest strictly-**lesser branch**
([`RowIndexReader.java:81–98`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/RowIndexReader.java#L81);
[`Walker.java:318–350`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/tries/Walker.java#L318)):

Pseudo (simplified; mirrors `prefixAndNeighbours` + `goMax`):
```text
separator_floor(root, K):                  // visits O(len(K)) nodes
  prefix_payload = None                    // closest separator that prefixes K
  lesser_branch  = NONE                    // closest strictly-lesser subtree
  node = root
  for b in bytes(K) + [END_OF_STREAM]:     // byte-comparable bytes of K, then -1
    i = node.search(b)                     // >= 0 exact; else -insertion_point - 1
                                           // Sparse: binary search; Dense: index arithmetic
    if i == 0 or i == -1:                  // nothing in this node sorts below b
      if node.has_payload:
        prefix_payload = node.payload      // a longer prefix wins over a shorter one
    else:                                  // a strictly-lesser child exists
      lesser_branch  = node.lesser_child(i)
      prefix_payload = None                // that child's max is a closer floor
    if i < 0: break                        // no exact transition: walk ends
    node = node.child(i)

  if prefix_payload: return prefix_payload
  return go_max(lesser_branch)             // rightmost payload of the lesser subtree
```

The walk visits **O(len(K))** nodes — one per key byte consumed — never O(number of blocks); it
never enumerates the trie. `go_max` then descends `lastTransition` repeatedly
([`Walker.java:164–174`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/tries/Walker.java#L164)).
This contrasts with BIG's binary search over sampled entries in `Summary.db` followed by a scan in
`Index.db`.

> **Note**: on a well-formed `Rows.db` trie `separatorFloor` never returns `null`, because the first
> block is stored under the empty separator at the root (see "Row index separator semantics"
> below). `SSTableIterator.ForwardIndexedReader.setForSlice` relies on this with an explicit
> `assert indexInfo != null`
> ([`SSTableIterator.java:100–113`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/SSTableIterator.java#L100)),
> and Cassandra's own `RowIndexTest` asserts `separatorFloor(ClusteringBound.BOTTOM).offset == 0`.

The complementary walks for the other end of a range are `Walker.followWithGreater` +
`goMin(greaterBranch)` (the smallest separator greater than a key)
([`Walker.java:176–190`, `225–242`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/tries/Walker.java#L176)),
and `RowIndexReader.min()`, which returns the first block by calling `goMin(root)`
([`RowIndexReader.java:100`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/RowIndexReader.java#L100)).

> **CQLite implementation note (not format authority).** CQLite implements both walks in
> `cqlite-core/src/storage/sstable/bti/parser/rows_floor.rs` (`rows_floor_block`,
> `rows_strict_ceiling_block`). See Appendix C for CQLite reader details.

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
| `DENSE` | 10–14 | Consecutive byte range; 12- to 40-bit distances |
| `LONG_DENSE` | 15 | Consecutive byte range; 64-bit distances (catch-all) |

All distances are unsigned and subtracted from the current node position (children are earlier
in the file). See Appendix C for complete per-type byte layouts.

#### Child lookup cost: `SPARSE` searches, `DENSE` indexes

The two multi-child families deliberately trade space for lookup cost, and the difference is a
property of the on-disk layout, not of any particular reader:

- **`SPARSE`** stores an explicit, ascending list of transition bytes followed by the matching
  child-distance array. Finding a transition is a **binary search** over that byte list —
  `O(log n)` in the node's child count — returning `-insertionPoint - 1` on a miss so the caller
  can identify the neighbouring children
  ([`TrieNode.java:513–533`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/tries/TrieNode.java#L513)).
- **`DENSE`/`LONG_DENSE`** store only a start byte and a range length, then one fixed-width
  distance slot per byte in `[start, start + length]`. Finding a transition is therefore **O(1)
  index arithmetic**: the slot for a search byte `b` is `b - start`, with an out-of-range `b`
  rejected by the same comparison. A range with no child at some byte writes a distance of `0`
  (`NULL_VALUE`) into that slot, so a present-but-empty slot is distinguishable from a real child
  without any search
  ([`TrieNode.java:662–702`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/tries/TrieNode.java#L662)).
  The range is serialised as a single `length − 1` byte, so `transitionRange = 1 + thatByte` and a
  dense node covers between 1 and 256 consecutive transition bytes — at most the full byte range,
  never more.

The writer picks the cheaper encoding per node, so a hot upper node with many children is usually
dense (O(1) descent) while a sparse tail node costs a short binary search.

> **CQLite implementation note (not format authority).** CQLite implements both child-lookup paths
> in `cqlite-core/src/storage/sstable/bti/parser/slice_walk.rs` (`sparse_child`, `dense_child`).
> See Appendix C for CQLite reader details.

### `Rows.db` per-partition footer

Trie pages in `Rows.db` are page-aware: the writer pads to the next 4096-byte page boundary only
when a node or a whole branch would otherwise straddle a page, so that no node is ever split
across pages ([`IncrementalTrieWriterPageAware.java`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/tries/IncrementalTrieWriterPageAware.java)).
It is not an unconditional per-partition padding. After the trie
pages the footer contains, in order
([`BtiFormat.md` lines 977–1010](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/BtiFormat.md#L977);
[`TrieIndexEntry.java:92–116`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/TrieIndexEntry.java#L92)):

1. Partition key (short-length-prefixed bytes)
2. Data file position of the partition start (unsigned vint)
3. Root node position: signed vint delta relative to the **entry's own base position** — the file
   offset just past the key bytes, i.e. `entryStart + 2 + keyLength` (see the CQLite reader note
   below for why the `+ 2` matters)
4. Row-index **block** count (unsigned vint) — the number of row-index blocks the trie separates,
   not the number of rows
   ([`TrieIndexEntry.rowIndexBlockCount`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/TrieIndexEntry.java#L92))
5. Partition deletion time — in `da` (`hasUIntDeletionTime()`), either a single `0x80` sentinel
   byte for `DeletionTime.LIVE`, or 12 bytes: 8-byte `markedForDeleteAt` long then a 4-byte
   **unsigned** local-deletion-time int
   ([`DeletionTime.Serializer`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/DeletionTime.java#L205))

The partition index (`Partitions.db`) points to the position of the partition key bytes (step 1).

### CQLite reader: footer root, position sign, and `Rows.db` entry resolution

These BTI facts are verified against CQLite's own reader
(`cqlite-core/src/storage/sstable/bti/parser/`) in addition to the Cassandra spec:

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
  vint][block count: unsigned vint][partition DeletionTime]`.
  In Cassandra the base is the position the writer was at **after** writing the
  short-length-prefixed key, i.e. `base = RowsOffset + 2 + key_length`, and
  `indexTrieRoot = readVInt() + base`
  ([`BtiTableWriter.java:185–213`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/BtiTableWriter.java#L185)
  writes `writeWithShortLength(key)` before calling
  `TrieIndexEntry.serialize(.., rowIndexWriter.position(), ..)`; the same
  `+ 2 + keyLength` base is recomputed on read in
  [`BtiTableReader.java:191`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/BtiTableReader.java#L191)).
  Feeding `RowsOffset` straight into a trie walker
  parses entry metadata as a node and fails. Cited to
  `TrieIndexEntry.java` (`serialize`/`deserialize`) and `RowIndexWriter.complete`.

- **Trie distances are backward.** All child distances are unsigned and subtracted
  from the current node position (children are written before parents / at lower
  offsets), consistent with the node-family table above.

> **Scope note (CQLite-specific).** CQLite has both a BTI **reader** (the items
> above) and, as of epic #872 (issues #908/#910), an opt-in BTI **writer**
> (`SSTableWriter::with_format(.., SSTableFormat::Bti)`). The default write path
> still emits classic `big`/`nb`. The BTI writer emits the canonical `da-*-bti-*`
> set — `Data.db`, `Partitions.db`, `Rows.db`, `Statistics.db`, `Filter.db`,
> `Digest.crc32`, `TOC.txt` — with the round-trip invariant proven against this
> reader: `Partitions.db` leaves store a negative direct `Data.db` offset for
> narrow partitions and a positive `RowsOffset` for wide ones; `Rows.db` holds
> one per-partition `TrieIndexEntry` + row-index trie per wide partition, and is
> emitted as a 0-byte component when no partition is wide (matching the real
> `da-2-bti-Rows.db` fixtures). CQLite's writer treats a partition as wide when it
> spans `>= 2` blocks of its own `COLUMN_INDEX_SIZE_BYTES` (64 KiB — BIG's default,
> not BTI's 16 KiB granularity), so its block boundaries are its own, not
> byte-identical to Cassandra's BTI writer. An
> empty BTI write (zero partitions) is refused, since a `da` SSTable has no
> readable zero-partition `Partitions.db` form. See
> `cqlite-core/src/storage/sstable/writer/partitions_writer.rs` (`RowsTrieWriter`)
> and `tests/issue_908_bti_canonical_write.rs`.

### Row index granularity

The row index does not index every row—it indexes blocks. BTI's default granularity is **at least
16 KiB** of serialised row data
([`BtiFormatPartitionWriter.DEFAULT_GRANULARITY = 16 * 1024`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/BtiFormatPartitionWriter.java#L41)),
overridable with the `column_index_size` parameter in `cassandra.yaml` — whose comment records the
split explicitly: "64 KiB for BIG, 16KiB for BTI". A partition only gets a row-index trie at all
when it spans more than one block; with a single block the writer skips the trie and the
`Partitions.db` leaf carries a direct `Data.db` offset instead
([`BtiFormatPartitionWriter.java:92–119`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/BtiFormatPartitionWriter.java#L92)).

#### Row index separator semantics

The trie stores **separators**, not exact block start keys, keeping the index compact
([`BtiFormat.md` lines 646–653](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/BtiFormat.md#L646)).
`RowIndexWriter.add` builds them like this
([`RowIndexWriter.java:66–75`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/RowIndexWriter.java#L66)):

- The **first** block's separator is `ByteComparable.EMPTY` — the empty byte sequence. Adding an
  empty key to the incremental trie writer sets the payload on the **root node** itself, so block 0
  is reachable as the root's payload rather than via any transition.
- Every **subsequent** block's separator is `ByteComparable.separatorGt(prevMax, firstKey)`: the
  shortest byte sequence that sorts strictly greater than the previous block's last key and no
  greater than this block's first key.
- `RowIndexWriter.complete(endPos)` appends one final "nudged" separator so the last block has an
  upper bound.

Two consequences follow, and they are the format's contract rather than a reader's choice:

1. A separator is generally **not** a key that exists in the partition. Never treat a separator as
   a row key; it is only a boundary usable for `<`/`>=` comparisons.
2. Because block 0 is indexed under the empty separator at the root, a lookup for a key that sorts
   below the first real separator still resolves — `separatorFloor` returns the root's payload
   (block 0). `separatorFloor` returns `null` only when the trie has no lesser branch and no prefix
   payload at all, which a well-formed `Rows.db` trie does not produce; see the note under
   "Prefix/range navigation" above.

> **CQLite note.** CQLite's reader computes the `TrieIndexEntry` base as
> `rows_offset + key_length`, omitting Cassandra's 2-byte short-length prefix
> (`cqlite-core/src/storage/sstable/bti/parser/rows.rs`). Walking from that position lands two bytes
> before the real root, which is why CQLite has historically described the first block as
> "unindexed": from the wrong root, the empty-key root payload is not reachable. Decoding the real
> `da-2-bti-Rows.db` fixture from Cassandra's base yields **39** payload entries (the empty-key
> entry plus 38 separators) where CQLite's base yields 38. This is a CQLite defect, not a property
> of the BTI format.

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
- The row index operates on blocks of at least 16 KiB (BTI's `DEFAULT_GRANULARITY`, configurable via
  `column_index_size`; BIG's default is 64 KiB), not individual rows — and it stores separators
  between blocks, never the block start keys themselves.
- Block 0 is indexed under the **empty** separator, which lands its payload on the row-index trie's
  root node; `separatorFloor` therefore resolves keys below the first real separator instead of
  returning `null`.
- Bloom filters and statistics continue to guide/guard the read path.
- Mixed-format directories occur during upgrades; readers must detect format.
For implementation details, see Appendix C.

### References
- Cassandra 5.0.8 (pinned):
  - BTI package: `https://github.com/apache/cassandra/tree/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti`
  - Authoritative in-tree spec: `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/BtiFormat.md`
  - `BtiFormat.java` (component sets): `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/BtiFormat.java#L83`
  - `PartitionIndex.java` (hash byte, position encoding): `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/PartitionIndex.java`
  - `TrieNode.java` (node types, `Sparse.search`, `Dense.search`): `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/tries/TrieNode.java#L947`
  - `Walker.java` (`prefixAndNeighbours`, `goMax`/`goMin`, `followWithGreater`): `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/tries/Walker.java#L318`
  - `RowIndexReader.java` (`separatorFloor`, payload layout): `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/RowIndexReader.java#L81`
  - `RowIndexWriter.java` (separator construction, `complete`): `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/RowIndexWriter.java#L66`
  - `BtiFormatPartitionWriter.java` (16 KiB `DEFAULT_GRANULARITY`): `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/bti/BtiFormatPartitionWriter.java#L41`
  - `PageAware.java` (4096-byte page constant): `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/util/PageAware.java#L24`
  - `MemtableParams.java` (default factory): `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/schema/MemtableParams.java#L99`
  - Big format reader (contrast): `https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/io/sstable/format/big/BigTableReader.java`

For implementation details, see Appendix C.



