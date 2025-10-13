## Index.db and Summary.db

This chapter explains the partition index (`Index.db`) and the sampled summary (`Summary.db`), and how they guide binary search and seeks into `Data.db`. It also outlines token-range iteration behavior.

### In this chapter you will learn
- The structure of index entries and promoted index behavior
- How summary sampling accelerates lookups
- How binary search is guided from summary to index to data
- How token range iteration interacts with the index

## Partition Index Structure

`Index.db` primarily stores partition key digests and, depending on format, may include offsets and sizes.

Annotated example (BIG, one entry):
```
00000000: 0010 6b88 bf20 a251 11f0 a3fe f1a5 5138  |..k.. .Q......Q8|
00000010: 3fb9 00                                   |?. .             |
```
- `0010` → marker (partition key digest follows)
- `6b88…3fb9` → 16-byte digest
- `00` → start of length/offset field (variable-length; see reader)

Variant gating (BIG):

Pseudo-structs per variant (field order, big-endian for fixed-width):

```
// No length prefix (legacy/BIG variant)
u16 marker = 0x0010
u128 partition_key_digest
varint data_offset
[optional promoted-index payload]

// With 2-byte length prefix (some 5.0 BIG tables)
u16 entry_length
u16 marker = 0x0010
u128 partition_key_digest
varint data_offset
[optional promoted-index payload]
```

Gate detection is handled by the BIG reader; consult `org.apache.cassandra.io.sstable.format.big.BigTableReader` and `RowIndexEntry` for exact parsing. Implementations must handle both variants by detecting an initial length field that precedes the `0x0010` marker.

Promoted index (BIG): emitted for wide partitions to accelerate within-partition seeks. Readers detect presence via entry payload structure and fall back to scan when absent. See `org.apache.cassandra.io.sstable.format.big` reader/writer for details.

## Summary Sampling and Navigation

`Summary.db` samples index entries for faster navigation.

## Token Range Iteration

Token-range iterators advance by consulting sampled tokens in `Summary.db`, then scanning contiguous partitions in `Index.db` over the range.

### BTI Notes
- BTI’s indexing can alter how promoted index information is structured; the high-level flow (Summary → Index → Data) remains intact, but entry payloads differ. Ensure readers gate parsing on `Descriptor` format.

### Key Takeaways
- `Index.db` maps partition keys to positions; `Summary.db` accelerates binary search.
- Sampling reduces memory while preserving fast seeks.
- Token-range iteration combines summary jumps with index scans.

### References
- Cassandra 5.0.0:
  - `IndexSummary`: [org.apache.cassandra.io.sstable.IndexSummary](https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/IndexSummary.java)
  - `SSTableReader`: [org.apache.cassandra.io.sstable.SSTableReader](https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/SSTableReader.java)
  - BIG reader: [org/apache/cassandra/io/sstable/format/big/BigTableReader.java](https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/format/big/BigTableReader.java)
  
For implementation details, see Appendix C.


