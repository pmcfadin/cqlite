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


## Zero-Copy (Entire-SSTable) Streaming

When certain eligibility conditions are met, Cassandra 5.0 streams an entire SSTable as raw bytes over the wire
instead of rebuilding it section by section. This is the default fast path for repair, bootstrap, and range movement
when the requested token range covers the full SSTable extent.

### Eligibility Conditions

A sender qualifies for zero-copy streaming only if **all four** of the following hold
([`CassandraOutgoingFile.computeShouldStreamEntireSSTables()`, lines 181–201](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/streaming/CassandraOutgoingFile.java#L181-L201)):

1. **`stream_entire_sstables` is enabled** — `DatabaseDescriptor.streamEntireSSTables()` returns `true`.
   This is set via `cassandra.yaml` and is the default for Cassandra 5.0.
2. **No legacy counter shards** — `SSTableMetadata.hasLegacyCounterShards == false`. Pre-3.0 counter SSTables
   carry legacy shard metadata incompatible with the zero-copy wire format.
3. **No old bloom-filter format** — `sstable.descriptor.version.hasOldBfFormat() == false`. The pre-4.0 bloom
   filter encoding is incompatible; affected SSTables fall back to section-based streaming.
4. **Sections fully cover the SSTable** — the sum of all requested `PartitionPositionBounds` lengths equals
   `sstable.uncompressedLength()`. A partial range request disqualifies zero-copy.

If any condition fails the code falls through to `CassandraStreamWriter` (legacy, lines 173–176), which reads
only the requested sections and reconstructs the SSTable on the receiver.

### ComponentManifest

When an SSTable qualifies, the sender creates a
[`ComponentManifest`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/streaming/ComponentManifest.java):
an ordered `LinkedHashMap<Component, Long>` recording each streaming component and its exact on-disk size in bytes.
The manifest is embedded in the `CassandraStreamHeader` transmitted before the data so the receiver knows the
byte length of every component before reading it.

Mutable components (`Statistics.db` and Index Summary) can be written concurrently by Cassandra internals. The sender
therefore re-creates the manifest **inside `sstable.runWithLock()`** at the moment bytes are about to be sent
(`CassandraOutgoingFile.java:157–165`). A `ComponentContext` retains the manifest and any required hard-linked
copies for the duration of the transfer.

### CassandraEntireSSTableStreamWriter Flow

[`CassandraEntireSSTableStreamWriter.write()`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/streaming/CassandraEntireSSTableStreamWriter.java)
iterates `manifest.components()` in order. For each component it:

1. Opens a `FileChannel` via `ComponentContext.channel(descriptor, component, length)`.
2. Calls `out.writeFileToChannel(channel, limiter)` — a rate-limited OS-level transfer backed by
   `FileChannel.transferTo` or equivalent; no row parsing occurs.
3. Records per-component progress to the `StreamSession` via `session.progress()`.

Total bytes transferred equals `manifest.totalSize()` — the sum of all component file sizes.

### CassandraEntireSSTableStreamReader Flow

On the receiver,
[`CassandraEntireSSTableStreamReader.read()`](https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/streaming/CassandraEntireSSTableStreamReader.java)
reads the `ComponentManifest` from the incoming header, constructs a new `SSTableZeroCopyWriter` at a fresh
descriptor, then calls `writer.writeComponent(component, in, length)` for each component in manifest order.
After all components land, it mutates `StatsMetadata` in-place to reflect the receiver's SSTable level and
repair metadata — the only processing step beyond raw I/O.

### Contrast with Section-Based (Legacy) Streaming

The legacy `CassandraStreamWriter` path reads only the partition sections requested
(`PartitionPositionBounds`), passing compressed chunks through `CassandraCompressedStreamWriter` when needed.
The receiver reconstructs a full SSTable through normal writer infrastructure.

| Dimension | Zero-copy path | Section-based path |
|-----------|---------------|-------------------|
| Granularity | Entire file components | Requested partition sections only |
| CPU (sender) | Near-zero (no parsing) | Moderate (section reads, decompress if needed) |
| CPU (receiver) | Near-zero (write raw bytes) | Full SSTable construction |
| Eligibility gate | Full range + no legacy features | Always available as fallback |
| Key classes | `CassandraEntireSSTableStream{Writer,Reader}`, `ComponentManifest` | `CassandraStream{Writer,Reader}`, `CassandraCompressedStreamWriter` |

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
  - Receivers validate chunks and components atomically using `ComponentManifest` (which enumerates each component and its exact byte length); partially received files remain isolated until complete
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
  - Streaming package: `https://github.com/apache/cassandra/tree/cassandra-5.0.8/src/java/org/apache/cassandra/streaming`
  - Repair package: `https://github.com/apache/cassandra/tree/cassandra-5.0.8/src/java/org/apache/cassandra/repair`
- Cross-links: see `10-point-reads-and-slices.md`, `04-from-cql-to-disk.md`, and `16-sstable-lifecycle-and-maintenance.md`


