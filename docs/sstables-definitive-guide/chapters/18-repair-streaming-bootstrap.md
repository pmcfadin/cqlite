## Repair, Streaming, and Bootstrap (Overview)

These cluster maintenance processes keep replicas consistent and safely move SSTables between nodes. We focus on what artifacts are shipped and how these flows intersect the read/write paths already covered—without turning this into an operations guide.

### In this chapter you will learn
- The purpose of repair, streaming, and bootstrap
- How SSTables move between nodes at a high level
- Where these processes intersect the read/write paths
- When these processes are invoked

## Processes at a Glance
High-level sequences (trimmed):

- Repair (anti-entropy):
  1) Compare data across replicas (Merkle trees) per token ranges
  2) Identify differences → produce segments to sync
  3) Stream SSTable sections (range-based) to reconcile
  4) Post-apply validation and compaction may follow

- Streaming (range movement):
  1) Establish session and negotiate ranges
  2) Sender reads SSTable sections; receiver writes new SSTables
  3) Validate via checksums/TOC; update metadata and mark complete

- Bootstrap (new node join):
  1) Allocate token ranges for the new node
  2) Stream relevant SSTable sections from existing replicas
  3) Build local indexes/stats; participate fully after completion

## Intersections with Read/Write Paths
These flows reuse the same on-disk artifacts and parsers:
- Readers: open `Data.db` through `CompressionInfo.db`, consult Bloom (`Filter.db`) and, depending on format, `Index.db`/`Summary.db` or BTI tries
- Writers: produce valid components and `TOC.txt`, update `Statistics.db`, and maintain checksums (`Digest.crc32`, per-chunk CRCs)

For a streaming reader implementation walkthrough, see Appendix C.

## Concurrency and Coordination Details (Overview)

- Sessions and Streams:
  - Multiple token ranges can stream concurrently; coordination ensures backpressure and ordering per range
  - Retry and resumption logic operates at section granularity, not whole-file
- Integrity and Idempotency:
  - Receivers validate chunks and components atomically; partially received files remain isolated until complete
  - Duplicate section arrivals are ignored or cause idempotent overwrites gated by `TOC` and digest checks
- Resource Management:
  - Concurrency limits cap open files and in-flight buffers; memory pressure triggers throttling
  - Compaction is often deferred or rate-limited during large streaming events
- Failure Handling (High-level):
  - Transient failures trigger re-requests for missing sections; persistent failures abort the session cleanly
  - Cleanups remove orphaned partials; metadata is reconciled before marking ranges consistent

### Key Takeaways
- Repair/streaming/bootstrap move SSTable sections; they don’t invent new file types.
- Integrity relies on the same checksums and `TOC.txt` invariants used during reads.
- Range-based transfers minimize work; follow-up compaction cleans up overlap.
- Operational control and scheduling are out of scope here; see Cassandra docs.

### References
- Cassandra 5.0.0:
  - Streaming package: `https://github.com/apache/cassandra/tree/cassandra-5.0.0/src/java/org/apache/cassandra/streaming`
  - Repair package: `https://github.com/apache/cassandra/tree/cassandra-5.0.0/src/java/org/apache/cassandra/repair`
- Cross-links: see `10-point-reads-and-slices.md`, `04-from-cql-to-disk.md`, and `16-sstable-lifecycle-and-maintenance.md`


