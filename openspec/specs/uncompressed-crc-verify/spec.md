# uncompressed-crc-verify Specification

## Purpose
TBD - created by archiving change uncompressed-crc-verify. Update Purpose after archive.
## Requirements
### Requirement: CRC.db reader byte-agrees with the Cassandra / #1197 writer format

CQLite SHALL provide a `CRC.db` reader that parses the Cassandra `ChecksumWriter` layout
— a 4-byte big-endian signed `i32` chunk-size header followed by one big-endian `u32`
CRC32 per Data.db chunk — and byte-agrees with both the #1197 write-side emitter
(`writer/crc_writer.rs`, `CRC_CHUNK_SIZE = 64 KiB`) and a committed Cassandra-written
`CRC.db` fixture. The reader SHALL expose, for any Data.db byte offset, the stored CRC32
for that offset's chunk via the mapping `chunk_index = offset / chunk_size`,
`crc_file_pos = chunk_index * 4 + 4`.

#### Scenario: Parses a committed Cassandra-written CRC.db fixture

- **GIVEN** the real Cassandra-written `CRC.db` shipped with an uncompressed BIG fixture
  (e.g. `test_basic/uncompressed_table`)
- **WHEN** the `CRC.db` reader parses it
- **THEN** the parsed chunk-size header equals the Cassandra value (`65536` / `0x00010000`)
- **AND** each parsed per-chunk CRC32 byte-agrees with the CRC32 recomputed over the
  corresponding raw `chunk_size` block of that fixture's `Data.db`.

#### Scenario: Round-trips the #1197 writer output

- **GIVEN** a `CRC.db` produced by the #1197 writer for a multi-chunk uncompressed
  `Data.db`
- **WHEN** the reader parses it
- **THEN** the recovered chunk size and every per-chunk CRC32 equal the values the writer
  emitted (reader and writer agree on the same byte layout).

#### Scenario: Chunk-to-CRC index mapping is correct for a multi-chunk file

- **GIVEN** a parsed `CRC.db` for a Data.db spanning at least three chunks
- **WHEN** the reader is queried for the CRC of a byte offset in chunk *k*
- **THEN** it returns the CRC32 stored at file position `k * 4 + 4`, matching the CRC32
  recomputed over the raw bytes of chunk *k*.

### Requirement: Uncompressed reads are CRC-verified on every read, default-on

`read_uncompressed_data_block` SHALL verify each Data.db chunk it returns against the
stored `CRC.db` CRC32, unconditionally and by default (no config flag). On a mismatch it
SHALL return a typed `Error::Corruption` naming the failing chunk index and its Data.db
byte offset, and SHALL NOT return the corrupt bytes, wrong decoded values, or a silent
empty result. The compressed read path SHALL be unaffected.

#### Scenario: Plain query over a bit-flipped uncompressed fixture fails fast

- **GIVEN** a Cassandra-written uncompressed BIG SSTable with exactly one Data.db byte
  flipped (corruption-corpus style, SHA-bound)
- **WHEN** it is read via the plain public query surface (`Database.execute` /
  `SSTableReader.scan`, not `verify_sstable`)
- **THEN** the read returns `Error::Corruption`
- **AND** the error message names the failing chunk index and its Data.db byte offset
- **AND** the query never returns wrong decoded values and never silently returns 0 rows.

#### Scenario: Clean uncompressed read is verified and succeeds

- **GIVEN** an uncorrupted uncompressed BIG SSTable with a valid `CRC.db`
- **WHEN** it is read via the plain query surface with verification default-on
- **THEN** every chunk passes CRC verification and the query returns the correct rows
  (verification of correct data never rejects it).

#### Scenario: A flip in a later chunk is attributed to that chunk

- **GIVEN** an uncompressed multi-chunk fixture with a single byte flipped inside a
  non-first chunk *k*
- **WHEN** it is read via the plain query surface
- **THEN** the returned `Error::Corruption` names chunk *k* (and its Data.db offset),
  demonstrating correct chunk attribution across the piece/chunk boundary.

### Requirement: verify --mode full validates CRC.db and reports a checksum-mismatch class

`verify_sstable` in `VerifyMode::Full` SHALL read `CRC.db` contents and validate every
uncompressed Data.db chunk against it, reporting a mismatch as a stable, distinct
`VerifyErrorClass` checksum-mismatch variant (the uncompressed analogue of the compressed
path's inline chunk-CRC finding) via a `VerifyFinding` that names the failing chunk and
the `CRC.db`/`Data.db` component. Prior behavior (name-whitelisting `CRC.db` without
reading it) SHALL be replaced. The corruption corpus SHALL gain an
`uncompressed_data_bit_flip` fixture with a manifest entry recording the captured
Cassandra `sstableverify` verdict as oracle.

#### Scenario: verify --mode full reports the failing chunk on the corrupt fixture

- **GIVEN** the `uncompressed_data_bit_flip` fixture
- **WHEN** `cqlite verify --mode full` runs against it
- **THEN** it reports at least one `VerifyFinding` with the stable checksum-mismatch error
  class
- **AND** the finding names the failing chunk and the `CRC.db`/`Data.db` component.

#### Scenario: verify --mode full passes on the clean source fixture

- **GIVEN** the uncorrupted clean-source uncompressed BIG SSTable with a valid `CRC.db`
- **WHEN** `cqlite verify --mode full` runs against it
- **THEN** no checksum-mismatch finding is reported for `CRC.db`.

#### Scenario: corpus manifest carries the Cassandra sstableverify oracle

- **GIVEN** the `uncompressed_data_bit_flip` fixture entry in the corruption manifest
- **WHEN** the fixture is generated by the corpus generator
- **THEN** the entry records the injected mutation (offset, original/mutated bytes), the
  fixture SHA-256 bindings, and the captured Apache Cassandra 5.0.2 `sstableverify`
  verdict as the oracle for CQLite's verdict
- **AND** the CQLite verify verdict matches the captured Cassandra verdict for that class.

### Requirement: Clean-path parity is byte-identical and within the perf budget

Enabling default-on CRC verification SHALL NOT change the results of the uncompressed
parity suite: every uncompressed fixture SHALL return byte-identical query results to the
pre-change baseline, and the change SHALL stay within the agreed perf budget (one CRC32
pass per chunk read; no new file-sized allocations).

#### Scenario: Uncompressed parity suite is unchanged with verification on

- **GIVEN** the full uncompressed-fixture parity suite (sstabledump JSONL goldens)
- **WHEN** it runs with CRC verification default-on
- **THEN** every fixture's query results are byte-identical to the pre-change baseline
  (no rows added, dropped, or altered by verification).

#### Scenario: No memory-budget or unbounded-allocation regression

- **GIVEN** a large uncompressed SSTable read with verification on
- **WHEN** chunks are verified during a full scan
- **THEN** the CRC reader reads only the header plus the per-chunk CRC (O(chunk_size)
  working set), allocating no additional file-sized buffer, keeping the read within the
  <128 MB budget.

### Requirement: The CRC.db reader is panic-free and handles a short CRC.db as a typed error

The `CRC.db` reader SHALL contain no `unwrap`/`expect` in library code and SHALL treat a
truncated or short `CRC.db` (missing header, or fewer CRC entries than the Data.db has
chunks) as a typed error rather than a panic or a silent skip. Behavior when `CRC.db` is
absent on an uncompressed BIG SSTable SHALL be a documented, pinned decision covered by a
test and recorded in the parity manifest scenario.

#### Scenario: Truncated CRC.db surfaces a typed error

- **GIVEN** an uncompressed fixture whose `CRC.db` is truncated (header only, or fewer
  per-chunk CRC entries than the Data.db requires)
- **WHEN** a chunk requiring the missing CRC entry is read or verified
- **THEN** the reader returns a typed error (`Error::Corruption` / `UnexpectedEof`-class)
  naming the `CRC.db` component
- **AND** no panic occurs and no chunk is silently treated as verified.

#### Scenario: Absent CRC.db behavior is pinned

- **GIVEN** an uncompressed BIG SSTable with `CRC.db` removed
- **WHEN** it is read via the plain query surface
- **THEN** the behavior matches the documented pinned decision (the chosen warn-and-proceed
  vs fail-closed outcome), asserted by a test
- **AND** the parity manifest scenario records that behavior so it cannot drift silently.

