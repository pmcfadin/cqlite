## Point Reads and Slices

This chapter details the read flow for point and slice queries: Bloom → Index → Summary → Data, and how slices/ranges behave for narrow vs wide partitions.

### In this chapter you will learn
- Read decision tree from Bloom to disk
- Slice and range behavior across partitions
- When promoted index/summary cuts latency
- Practical impacts on IO patterns

## Read Flow (Decision Tree)

Diagram: read-path decision tree
- Diagram (Mermaid source): `../diagrams/read-path-decision-tree.mmd`
- Alt text: Decision tree from Bloom to Summary/Index to Data
- Caption: Point and slice reads traverse Bloom → Summary/Index → Data

For an implementation walkthrough of the read flow, see Appendix C.

## Slices and Range Reads

- Slices can skip to approximate positions via `Summary.db`, then advance in `Index.db`.
- For wide partitions, promoted indexes improve within-partition seeks.

### Clustering Slice Optimization (wide partitions)
- When partitions are wide, a promoted index enables O(log n) within-partition seeks for clustering slices instead of scanning all rows. Ensure readers check for promoted index presence and fall back correctly when absent.

### Iterators for Range Scans
- Expose an iterator interface that yields `(key, value)` pairs over token ranges: initialize from `Summary.db` token, then advance through `Index.db` entries. Provide `next()`/`peek()` and bounded `limit` support.

### BTI Read Path Notes
- BTI preserves the decision flow but the index entry payload may differ. Gate parsing using the `Descriptor` format; the iterator and seek abstractions remain the same.

Promoted index (BIG):
- Emitted for wide partitions when clustering counts exceed internal thresholds.
- Readers detect presence and use it for O(log m) within-partition seeks; otherwise, fall back to sequential scan within the partition.

### Complexity Notes
- Point read: Bloom O(1), Summary binary search O(log s), Index probe O(log n) or O(1) if direct-offset; fallback scan O(n).
- Slice read: Initialization O(log s) + sequential advancement; with promoted index, within-partition seek O(log m) over clustering entries.

## Minimal Example (trimmed)

From `test_basic/simple_table`:
- Bloom FP chance: 0.01 (from `Statistics.db.txt`)
- A point read on an absent key short-circuits at Bloom.

### Key Takeaways
- Bloom optimizes negative lookups; positives proceed to index/summary.
- Summary sampling accelerates binary search; promoted index helps wide rows.
- Range scans mix summary jumps with sequential index advances.

### References
- Cassandra 5.0.0:
  - `IndexSummary`: https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/IndexSummary.java
  - Read commands: `org.apache.cassandra.db.SinglePartitionReadCommand`
  
For implementation details, see Appendix C.


