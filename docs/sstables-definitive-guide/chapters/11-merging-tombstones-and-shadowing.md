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

## Range Tombstones

Range tombstones delete clustering intervals; readers must compare timestamps against range bounds during reconciliation.

## Tombstone Timeline Diagram

- Diagram (Mermaid source): `../diagrams/tombstone-timeline.mmd`
- Alt text: Timeline showing writes, tombstones, and TTL expiry with shadowing
- Caption: Newer values can shadow older tombstones; TTLs create time-bound deletions

## Key Takeaways
- Newest wins unless tombstones at or after write remove visibility.
- Range tombstones apply only within their intervals and while active.
- TTL expiry can surface as synthetic tombstones.

### Complexity Notes
- Merge per row: sorting values is O(k log k) where k is the number of versions; single-pass reconciliation after sort is O(k).
- Range tombstone filtering: O(n × t) worst-case (n entries, t tombstones) but typically reduced by time-sorted early exits.

### References
- Cassandra 5.0.0:
  - Rows/tombstones: [org.apache.cassandra.db.rows](https://github.com/apache/cassandra/tree/cassandra-5.0.0/src/java/org/apache/cassandra/db/rows)
  
For implementation details, see Appendix C.


