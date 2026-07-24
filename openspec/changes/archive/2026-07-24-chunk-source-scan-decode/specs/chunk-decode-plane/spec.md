# chunk-decode-plane — delta for chunk-source-scan-decode (issue #2165)

## MODIFIED Requirements

### Requirement: A single chunk decode plane
Chunk read → CRC → decompress → B1-cache SHALL be performed in exactly one module
(`reader/chunk_source.rs`) reached by every query/CLI-query read path — including the sequential
scan and full-partition iteration paths (`sequential_scan`, `iterate_all_partitions`,
`parse_block_entries`). No other non-test module in the query decode path SHALL call a compression
`decompress` primitive or an inline chunk CRC verification directly, and the architecture test
SHALL NOT carry allowlist exclusions for `parsing/` modules.

#### Scenario: Decompress is invoked from exactly one module on the query path
- **WHEN** the workspace source is scanned for calls to `Compression::decompress` (or an inline per-chunk `crc32fast` verify-then-decompress sequence) reachable from `get`, the windowed scan, the sequential scan / full-partition iteration, or the CLI query path
- **THEN** every such call resolves inside `reader/chunk_source.rs`, and the architecture test that counts these call sites reports exactly one module

#### Scenario: The parsing module carries no decompress call sites and no allowlist exclusion
- **WHEN** `cqlite-core/tests/chunk_decode_single_plane.rs` runs after the migration
- **THEN** its source scan covers `parsing/mod.rs` and `parsing/block_entries.rs` (the exclusion entries for both are deleted), and the test passes with `parsing/` fully consolidated

#### Scenario: A repeat read of a resident chunk decompresses zero times
- **WHEN** the same chunk index is read twice through `ChunkSource::chunk` with a warm `DecompressedChunkCache`
- **THEN** the second call returns an `Arc` clone of the cached bytes, the process-global `DECOMPRESS_CALLS` counter is unchanged across the second call, and no file read is issued

#### Scenario: CRC is verified before decompression
- **WHEN** `ChunkSource::chunk` reads a chunk whose stored inline CRC32 does not match the computed CRC over its compressed bytes
- **THEN** it returns a corruption error and never invokes the decompressor for that chunk

## ADDED Requirements

### Requirement: Sequential block decompression fails closed
The reader SHALL surface a corruption error when a compressed block reached via the sequential
scan / full-partition iteration path (`parse_block_entries`) fails decompression, and SHALL NOT
fall back to parsing the raw compressed bytes as row data.

#### Scenario: Decompress failure surfaces an error instead of a raw-bytes parse
- **GIVEN** a reader whose `CompressionInfo` declares a compression algorithm and a block whose bytes fail decompression
- **WHEN** `parse_block_entries` processes that block
- **THEN** it returns a corruption error, no rows are produced from the raw bytes, and the former silent-fallback test (`test_decompression_fallback_on_failure`) asserts this fail-closed behavior

#### Scenario: Uncompressed readers are unaffected
- **GIVEN** a reader with no `CompressionInfo` (`compression_reader == None`), e.g. an uncompressed or BTI no-CompressionInfo SSTable
- **WHEN** the sequential scan parses its blocks
- **THEN** blocks are parsed as raw bytes exactly as before the migration, with no decompression attempted and no error

### Requirement: Scan and iteration parity is unchanged by the migration
Routing the sequential-scan block decompress through `ChunkSource` SHALL NOT change any decoded
bytes or row set on healthy files; the existing parity suites over the public scan/iteration
surfaces SHALL remain green.

#### Scenario: Sequential-scan and full-iteration parity suites stay green on real fixtures
- **WHEN** the compressed-legacy parity tests (`v5_compressed_legacy_parity_test`, `v5_compressed_legacy_row_count_parity`), the tombstone full-scan parity test (`issue_1085`), the index-size-zero sequential-fallback integration test, the point-vs-full differential lane, and the query-semantics oracle run against the real datasets after the migration
- **THEN** every previously passing test passes with identical rows, values, and order
