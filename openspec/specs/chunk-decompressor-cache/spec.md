# chunk-decompressor-cache Specification

## Purpose
TBD - created by archiving change chunk-cache-no-clone-evict. Update Purpose after archive.
## Requirements
### Requirement: The CLI-path chunk cache returns hits by refcount bump, never by copying

The `ChunkDecompressor` chunk cache SHALL store each decompressed chunk as a reference-counted byte buffer
(`Arc<[u8]>`). A cache hit SHALL return a handle to the *same* underlying buffer (an `Arc::clone` /
refcount bump), performing no chunk-sized allocation or memcpy of the cached bytes. The decompressed
`Vec<u8>` SHALL be converted to the reference-counted buffer exactly once, on insert. Callers that require
an owned `Vec` SHALL copy explicitly, so the copy is off the hit path.

#### Scenario: A cache hit returns the same buffer without copying

- **WHEN** a chunk is decompressed (populating the cache) and then the same chunk index is fetched again from the cache
- **THEN** the second fetch returns a handle pointing to the same underlying buffer as the first (`Arc`-pointer-identity equal), with no chunk-sized allocation or copy

### Requirement: Repeated reads of the same chunk skip re-decompression

When a decompressed chunk is already resident in the `ChunkDecompressor` cache, the read path SHALL return
it from the cache without re-invoking the decompressor. A decompress-work counter SHALL increment exactly
once per actual chunk decompression (miss path only) and SHALL NOT increment on a cache hit, so a test can
prove a repeated read of the same chunk performed exactly one decompression.

#### Scenario: Reading the same chunk twice decompresses exactly once

- **WHEN** the same byte range (within a single chunk) is read twice from the same `ChunkDecompressor` over a multi-chunk fixture
- **THEN** both reads return the identical bytes AND the decompress-work counter increases by exactly 1 across the two reads (the second read is served from the cache)

#### Scenario: A cold miss counts one decompression and populates the cache

- **WHEN** a chunk not yet resident is read
- **THEN** the compressed bytes are read and CRC-verified as today, the chunk is decompressed exactly once (counter +1), and the resulting buffer is inserted into the cache

### Requirement: Eviction removes the genuinely least-recently-used chunk

When the `ChunkDecompressor` cache is at capacity and a new chunk is inserted, it SHALL evict the entry
that was least recently *accessed* (recency updated on every get and insert), not an arbitrary entry. The
cache SHALL remain bounded by its configured capacity.

#### Scenario: A, B, A, C at capacity two evicts B

- **WHEN** the cache capacity is two and chunks are accessed in the order A, B, A, then C is inserted (exceeding capacity)
- **THEN** B (the least recently used) is evicted while A (recently re-accessed) and C (just inserted) remain resident, so a subsequent read of B re-decompresses (counter increments) while reads of A and C are served from the cache (counter unchanged)

#### Scenario: The cache never exceeds its configured capacity

- **WHEN** more distinct chunks than the capacity are read in sequence
- **THEN** the number of resident cache entries never exceeds the configured capacity

### Requirement: The fix changes no read result and no public method signature

Introducing `Arc<[u8]>` values and real LRU eviction SHALL NOT change any decompressed output or any read
result: the CLI smoke / parity suite SHALL remain green. The public methods `read_data`, `read_all_data`,
and `decompress_chunk_by_index` SHALL keep their existing `Result<Vec<u8>>` return types, and
`cache_stats()` SHALL keep returning `(resident entry count, capacity)`.

#### Scenario: CLI output parity unchanged

- **WHEN** the CLI smoke suite reads every table after the change
- **THEN** every table's output matches its pre-change output, byte-for-byte

#### Scenario: Public signatures unchanged

- **WHEN** an existing caller invokes `read_data`, `read_all_data`, `decompress_chunk_by_index`, or `cache_stats`
- **THEN** it compiles and behaves as before (same return types and semantics), with only faster, non-copying cache hits internally

