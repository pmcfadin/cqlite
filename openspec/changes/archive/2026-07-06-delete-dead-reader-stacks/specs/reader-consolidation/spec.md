## ADDED Requirements

### Requirement: CompressionInfo.db is parsed exactly once per reader open
The SSTable reader open path SHALL parse each `CompressionInfo.db` file at most once per open. It
SHALL parse the file with a single parser (`compression_info::CompressionInfo::parse`) and derive the
compression algorithm for the reader's `CompressionReader` from that single parsed result, rather than
parsing the same file a second time with a separate parser solely to learn the algorithm. A
process-observable work counter SHALL increment exactly once per `CompressionInfo.db` parse so a test
can assert the per-open parse count.

#### Scenario: Opening a compressed SSTable parses CompressionInfo.db exactly once
- **WHEN** the CompressionInfo-parse work counter is reset and a reader is opened on a compressed
  Cassandra 5.0 SSTable that ships a `CompressionInfo.db`
- **THEN** the counter delta is exactly 1 (before this change it was 2 — a legacy `parse_binary` plus
  the modern `parse`)

#### Scenario: The derived algorithm matches the CompressionInfo.db contents
- **WHEN** a reader is opened on a compressed SSTable
- **THEN** the reader's compression algorithm equals the algorithm named in the parsed
  `CompressionInfo.db`, and decompressed read results are byte-for-byte identical to before the change

### Requirement: Compression component discovery is O(1) from authoritative metadata
The reader open path SHALL locate the `CompressionInfo.db` component by the name derived
deterministically from `SsTableDescriptor`, issuing at most one `exists()`/`stat` probe for it. It
SHALL NOT probe a fixed list of ~25 speculative generation-numbered filenames. Component and
algorithm identification SHALL use only authoritative metadata (`SsTableDescriptor` and the parsed
`CompressionInfo.db`), never byte-content heuristics.

#### Scenario: Opening probes the single descriptor-derived CompressionInfo name
- **WHEN** a reader is opened on any Cassandra 5.0 SSTable
- **THEN** the open does not iterate the legacy fixed generation-number probe list, and a genuinely
  uncompressed SSTable (no `CompressionInfo.db`) opens with the compression reader absent

### Requirement: Dead reader stacks are removed from the reader surface
The system SHALL NOT retain the dead reader stacks identified by the read-path audit: the
`SchemaAwareReader` type and its module, the `ChunkedDataReader` type and its module, the
`StreamingDecompressor` type and `ChunkedDecompressionConfig`, and the streaming half of
`CompressionReader` (`read_streaming`, `read`, `with_block_size`, `block_size`, and the associated
`buffer`/`block_size` fields). The duplicate legacy `compression::CompressionInfo` parser (and its
`ChunkInfo`, `parse`, `parse_binary`, and `normalize_algorithm_name`) SHALL be removed together with
its only remaining (unwired) consumer. `CompressionReader` SHALL retain only its algorithm field and
the `algorithm()` accessor used by the live decompression sites.

#### Scenario: The deleted symbols are absent from the workspace
- **WHEN** the workspace is searched for `SchemaAwareReader`, `ChunkedDataReader`,
  `StreamingDecompressor`, `read_streaming`, or `compression::CompressionInfo::parse_binary`
- **THEN** no definitions, re-exports, module declarations, or references remain in `src/`, `bindings/`,
  `cqlite-cli/src/`, or the test crates

#### Scenario: The full build and 33-table parity remain green after deletion
- **WHEN** the workspace is built with default features, with the minimal feature set
  (`--no-default-features --features all-compression`), and with the `tombstones` and `write-support`
  features, and the parity suite is run
- **THEN** every build compiles with no dangling references and the 33-table `sstabledump` parity
  harness passes byte-for-byte unchanged
