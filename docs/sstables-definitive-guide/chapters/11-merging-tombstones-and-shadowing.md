## Merging, Tombstones, and Shadowing

Tombstones mark deletions at partition/row/cell levels (and ranges). This chapter explains how multiple SSTables and generations reconcile (shadowing), TTL expiry, and the effect of range tombstones.

### In this chapter you will learn
- Tombstone types and lifecycles
- Shadowing across SSTables/generations
- TTL expiry and gc_grace interactions
- Practical reconciliation rules

## Tombstone Types

- Partition, Row, Cell tombstones
- Range tombstones spanning clustering key intervals

## Reconciling Multiple Generations

Reconciliation applies Cassandra 5.0 semantics to select visible values.

Row-level handling ensures newer data can supersede older row tombstones when timestamps allow.

### Tombstone Tie-Breaking Hierarchy

When two cells share **equal timestamps**, `Cells.resolveRegular()` applies this precedence
(`Cells.java:79–128`, CASSANDRA-14592):

1. **Tombstone/expiring beats live cell** — any cell with a `localDeletionTime` wins over a live
   cell at the same timestamp.
2. **Pure tombstone beats expiring cell** — a hard delete wins over a TTL-expiring write.
3. **Higher `localDeletionTime` wins** — between two expiring cells or two tombstones.
4. **Lower TTL wins** — between two expiring cells with equal `localDeletionTime`.
5. **Value bytes** — final tiebreaker for live cells with identical timestamps.

## Range Tombstones

Range tombstones delete clustering intervals; readers must compare timestamps against range bounds during reconciliation.

## Tombstone Timeline Diagram

![Tombstone timeline](diagrams/tombstone-timeline)
- Alt text: Timeline showing writes, tombstones, and TTL expiry with shadowing
- Caption: Newer values can shadow older tombstones; TTLs create time-bound deletions

## Key Takeaways
- Newest wins by timestamp; at **equal timestamp**, tombstones (and expiring cells) always beat
  live cells (`Cells.java:94`, CASSANDRA-14592). Within equal-timestamp tombstones: pure
  tombstone beats expiring cell; then higher `localDeletionTime`; then lower TTL.
- Range tombstones apply only within their intervals and while active.
- TTL expiry can surface as synthetic tombstones.

### Complexity Notes
- Merge per row: sorting values is O(k log k) where k is the number of versions; single-pass reconciliation after sort is O(k).
- Range tombstone filtering: O(n × t) worst-case (n entries, t tombstones) but typically reduced by time-sorted early exits.

### References
- Cassandra 5.0.8 (pinned):
  - `Cells.java` (tombstone reconciliation L79–L128) — https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/rows/Cells.java#L79-L128
  - `DeletionTime.supersedes()` (partition/row tombstone precedence L158–L161) — https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/DeletionTime.java#L158-L161
  - Rows/tombstones package — https://github.com/apache/cassandra/tree/cassandra-5.0.8/src/java/org/apache/cassandra/db/rows
  
For implementation details, see Appendix C.


