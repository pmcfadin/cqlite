# read-cache Specification

## Purpose
TBD - created by archiving change decompressed-chunk-cache. Update Purpose after archive.
## Requirements
### Requirement: A shared, bytes-bounded, sharded decompressed-chunk cache exists
The system SHALL provide a decompressed-chunk cache that stores decompressed SSTable compression
chunks keyed by authoritative SSTable identity and chunk index. Cache values SHALL be reference-counted
byte buffers (`Arc<[u8]>` or equivalent) so that a cache hit returns the buffer by refcount bump, never
by copying the chunk bytes. The cache SHALL be bounded by the total decompressed **bytes** it holds (not
by entry count): each entry is weighed by its decompressed length, and when an insert would exceed the
configured byte budget the cache SHALL evict least-recently-used entries until it is within budget. The
cache SHALL be internally **sharded** so that a cache lookup ("hit path") acquires only a per-shard lock
and never a single process-wide lock. The cache key SHALL be derived from authoritative reader
identity (SSTable `file_path`/`generation`/id) and the chunk index computed from authoritative
`CompressionInfo` chunk metadata or promoted-index offsets — never inferred from decompressed byte
content.

#### Scenario: A cache hit returns the same buffer without copying
- **WHEN** a chunk is inserted and then fetched again by the same key
- **THEN** the fetch returns a handle pointing to the *same* underlying buffer as the inserted one (pointer/`Arc`-identity equal), performing no chunk-sized allocation or copy

#### Scenario: Byte-bounded LRU eviction
- **WHEN** a cache is created with a byte budget that holds at most two equally-sized chunks, and chunks are accessed in the order A, B, A, then C is inserted
- **THEN** B is evicted (least recently used) and A survives, because recency is updated on access and eviction is by bytes, not by arbitrary key order

#### Scenario: Eviction keeps the cache within its byte budget
- **WHEN** more distinct chunks are inserted than the byte budget can hold
- **THEN** the cache's total resident bytes never exceed the configured budget after each insert (older entries are evicted to make room)

#### Scenario: The hit path takes no global lock
- **WHEN** multiple threads read chunks whose keys fall in different shards concurrently
- **THEN** their lookups proceed without serializing on a single shared lock (they contend at most per-shard)

### Requirement: Repeated reads of the same chunk skip re-decompression and re-reading
When a decompressed chunk is already resident in the cache, the read path SHALL return it from the cache
without re-reading the compressed bytes from the backing source and without re-invoking the decompressor.
A process-observable decompress-work counter SHALL be incremented exactly once per actual chunk
decompression performed by the wired read sites, so that a test can prove a repeated read performed zero
decompressions.

#### Scenario: Second identical point read does zero decompress work and zero underlying reads
- **WHEN** the same multi-chunk fixture is opened and the identical point read is issued twice, with the decompress counter and a byte-source read counter reset before the second read
- **THEN** the second read returns the same result AND the decompress-counter delta is 0 AND the underlying byte-source performs 0 reads for the cached chunk(s)

#### Scenario: Cold read populates the cache and counts one decompress per chunk
- **WHEN** a chunk not yet resident is read
- **THEN** the compressed bytes are read once, CRC-verified where the site verifies today, decompressed exactly once (counter +1), the decompressed buffer is inserted into the cache, and the buffer is returned

### Requirement: All three decompressed-chunk read sites consult the shared cache
The system SHALL wire the same shared cache into all three decompressed-chunk read sites — the BIG
point-read chunk fetch (`get_cached_data` in `data_access/mod.rs`), the windowed streaming scan chunk
fill (`scan_stream_windowed.rs`), and the BTI target-chunk read (`data_access/bti.rs`) — so each site
consults the cache before reading+decompressing and inserts on miss. Each site's use of
the cache SHALL be provable independently via the decompress-work counter (per-site wiring evidence).
Readers created for the same logical dataset (via the same manager/`Database`) SHALL share one cache
instance.

#### Scenario: BIG point-read path is wired
- **WHEN** the same BIG (`nb`) partition is point-read twice through the public query API
- **THEN** the second read's decompress-counter delta is 0 for the chunk(s) that partition occupies

#### Scenario: BTI point-read path is wired
- **WHEN** the same BTI (`da`) partition is point-read twice through the public query API
- **THEN** the second read's decompress-counter delta is 0 for the target chunk

#### Scenario: Windowed scan path is wired
- **WHEN** a table is scanned, then a partition it already decompressed is read again through the same shared cache
- **THEN** that partition's chunk(s) are served from the cache with a decompress-counter delta of 0

### Requirement: The cache never changes read results and stays within the memory budget
Introducing the cache SHALL NOT change any query result: the 33-table `sstabledump` parity harness SHALL
remain green byte-for-byte. A scan of a table larger than the cache byte budget SHALL complete correctly
with memory bounded by the budget (eviction works under scan pressure), keeping the process within the
<128MB memory target validated by the A4 dhat lane. The cache SHALL cache only decompressed chunks (never
compressed bytes).

#### Scenario: Parity unchanged
- **WHEN** the full parity/smoke read suite runs with the cache enabled
- **THEN** every table's rows match their `sstabledump` JSONL golden exactly, identical to pre-cache behavior

#### Scenario: Scan larger than the cache completes with bounded memory
- **WHEN** a table whose decompressed size exceeds the cache byte budget is fully scanned
- **THEN** the scan returns all rows correctly AND the cache's resident bytes stay within the budget throughout (older chunks are evicted under scan pressure)

### Requirement: Concurrent access to the shared cache is sound
The cache SHALL be safe under concurrent reads and inserts from multiple threads: it SHALL NOT propagate
a poisoned lock as a panic on the read path, SHALL NOT grow without bound, and SHALL NOT permit a data
race or return a torn/partial buffer. Concurrent readers of the same or different chunks SHALL each get a
correct, complete decompressed buffer.

#### Scenario: Concurrent readers get correct buffers
- **WHEN** many threads concurrently read overlapping and disjoint chunks through the shared cache under eviction pressure
- **THEN** every returned buffer is a complete, correct decompressed chunk and the cache's resident bytes stay within budget (no unbounded growth, no panic, no torn read)

