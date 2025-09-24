## Index.db and Summary.db

This chapter explains the partition index (`Index.db`) and the sampled summary (`Summary.db`), and how they guide binary search and seeks into `Data.db`. It also outlines token-range iteration behavior.

### In this chapter you will learn
- The structure of index entries and promoted index behavior
- How summary sampling accelerates lookups
- How binary search is guided from summary to index to data
- How token range iteration interacts with the index

## Partition Index Structure

`Index.db` primarily stores partition key digests and, depending on format, may include offsets and sizes.

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
  
For implementation details, see Appendix C.


