## ADDED Requirements

### Requirement: A single chunk decode plane
Chunk read → CRC → decompress → B1-cache SHALL be performed in exactly one module
(`reader/chunk_source.rs`) reached by every query/CLI-query read path. No other non-test
module in the query decode path SHALL call a compression `decompress` primitive or an inline
chunk CRC verification directly.

#### Scenario: Decompress is invoked from exactly one module on the query path
- **WHEN** the workspace source is scanned for calls to `Compression::decompress` (or an inline per-chunk `crc32fast` verify-then-decompress sequence) reachable from `get`, the windowed scan, or the CLI query path
- **THEN** every such call resolves inside `reader/chunk_source.rs`, and the architecture test that counts these call sites reports exactly one module

#### Scenario: A repeat read of a resident chunk decompresses zero times
- **WHEN** the same chunk index is read twice through `ChunkSource::chunk` with a warm `DecompressedChunkCache`
- **THEN** the second call returns an `Arc` clone of the cached bytes, the process-global `DECOMPRESS_CALLS` counter is unchanged across the second call, and no file read is issued

#### Scenario: CRC is verified before decompression
- **WHEN** `ChunkSource::chunk` reads a chunk whose stored inline CRC32 does not match the computed CRC over its compressed bytes
- **THEN** it returns a corruption error and never invokes the decompressor for that chunk

### Requirement: CLI-vs-core query parity on one scan stack
The CLI query path and the core query path SHALL decode through the same `ChunkSource`,
producing identical rows. Neither SHALL reach `BulletproofReader` or `ChunkDecompressor`.

#### Scenario: CLI query and core query return byte-identical results
- **WHEN** the same SELECT is executed against the same SSTable via the CLI query command and via the core `Database` query API
- **THEN** the returned rows are byte-identical, and neither execution reaches `BulletproofReader` or `ChunkDecompressor`

#### Scenario: The query path does not reference the retired readers
- **WHEN** the module graph reachable from `get`, the windowed scan, and the CLI query command is inspected
- **THEN** no path references `BulletproofReader` or `ChunkDecompressor`

### Requirement: Byte-identical results and 33-table parity preserved
Migrating `get`, the scan window fill, and the CLI query path onto `ChunkSource` SHALL NOT
change any decoded bytes; the golden parity harness SHALL remain green.

#### Scenario: 33-table golden parity is unchanged after migration
- **WHEN** the SSTable read/parity harness runs against the real test datasets after the migration
- **THEN** every previously passing table parses to the same rows as before

#### Scenario: get and scan return identical bytes for a shared chunk
- **WHEN** a partition is read once via the `get` point path and once via the windowed scan, both covering the same compression chunk
- **THEN** the decompressed chunk bytes each path observes are identical

### Requirement: Compression-algorithm matrix stays green
`ChunkSource` SHALL decode every supported compression algorithm (LZ4, Snappy, Deflate,
Zstd) and the incompressible raw-passthrough case, matching pre-migration behavior.

#### Scenario: Every compression algorithm decodes through the one plane
- **WHEN** chunks compressed with LZ4, Snappy, Deflate, and Zstd, plus an incompressible raw-stored chunk, are read through `ChunkSource::chunk` under each algorithm's feature
- **THEN** each returns the exact decompressed bytes Cassandra wrote, using the single decode plane

### Requirement: Non-query BulletproofReader consumers are documented as follow-up
Retiring `BulletproofReader` from the query path SHALL NOT delete it while non-query
consumers remain; those consumers SHALL be enumerated as a follow-up.

#### Scenario: Remaining consumers are recorded and still compile
- **WHEN** the change lands with `BulletproofReader` retired from the query path
- **THEN** the CLI inspect/info/read/export/benchmark/support commands, `sstable_data_manager`, and `oa_format_compliance_test` still compile, and the change records them as the deletion follow-up

### Requirement: Library panic-freedom and warning-clean build
The consolidated decode plane SHALL contain no `unwrap`/`expect` in library code, and the
crate SHALL build clean under `RUSTFLAGS=-D warnings`.

#### Scenario: No unwrap/expect on the decode plane
- **WHEN** `reader/chunk_source.rs` and the migrated call sites are inspected
- **THEN** no non-test library line uses `unwrap()` or `expect()`

#### Scenario: Warning-clean build
- **WHEN** the crate is built with `RUSTFLAGS="-D warnings"` across the query-relevant feature combinations
- **THEN** the build succeeds with no warnings
