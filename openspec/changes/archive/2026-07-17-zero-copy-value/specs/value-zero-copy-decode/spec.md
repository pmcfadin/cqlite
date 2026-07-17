# value-zero-copy-decode

## ADDED Requirements

### Requirement: Scalar byte payloads are extracted without a per-cell copy on the streaming path

The value-decode paths SHALL materialize `Text`, `Blob`, `Varint`, and `Inet` payloads as refcounted
`Bytes` subslices of the decoded chunk (via the scan-window borrow API) on the streaming decode path,
rather than copying with `String::from_utf8(x.to_vec())` / `x.to_vec()`. This covers the decode sites in
`storage/sstable/reader/parsing/row_decoder/` (`raw_value`, `cell_value`), `parsing/raw_type_value.rs`,
`parsing/complex_column.rs`, and `.../udt.rs`. UTF-8 validation for `Text` SHALL be performed in place on
the borrowed slice (`str::from_utf8`) — validation without a copy — before the validated `Bytes` is
stored. The decode tag SHALL continue to come from authoritative column metadata; the value SHALL never
be inferred from byte patterns (no-heuristics, issue #28).

#### Scenario: A text-heavy scan copies ≈ zero payload bytes into values

- **WHEN** the #2075 dhat allocs-per-row lane runs a full scan over a present text-heavy fixture after
  the change
- **THEN** the allocations-per-row and the bytes-copied-into-`Value`-payloads metrics drop measurably
  versus `main` (borrowed / refcounted decode; ~1×-payload-per-cell copy on `main`)
- **AND** the scan returns a non-zero number of rows with byte-identical values.

#### Scenario: A predicate-rejected value is never copied

- **GIVEN** a scan with a predicate that rejects a text/blob cell
- **WHEN** the cell is decoded and evaluated on the streaming path
- **THEN** no owned copy of that cell's payload is allocated (the borrow is dropped after the predicate),
  observable on the allocs-per-row lane.

### Requirement: Comparator and byte-comparable decode borrow without changing ordering

The comparator-driven decode paths SHALL also borrow scalar byte payloads rather than copy —
`parsing/comparator_value_parsing.rs`, `parsing/byte_comparable.rs`, and `parsing/custom_scalar.rs` —
and the resulting comparison/ordering behavior SHALL be byte-identical to `main` across all CQL types.

#### Scenario: Comparator ordering is unchanged after borrowing

- **WHEN** values decoded through the comparator/byte-comparable paths are ordered after the change
- **THEN** the ordering is identical to `main` (including NaN-last and `-0.0 < +0.0`), and the decoded
  bytes match the sstabledump/JSONL golden.

### Requirement: Chunk retention is bounded by a documented force-copy boundary

A documented force-copy (compaction) boundary SHALL bound retained memory, because borrowing a `Bytes`
subslice keeps its whole parent chunk alive by refcount: on the streaming decode path values borrow, but
at every retention boundary — collected/materialized result sets, LIMIT/sort/dedup buffers, core-internal
caches, any `Value` stored in a longer-lived structure, and the FFI/binding boundary — a borrowed
payload whose parent chunk is materially larger than the payload SHALL be compacted into a tight
standalone allocation, releasing the chunk. "Materially larger" SHALL be a documented rule
(`backing.len() > payload.len() + RETENTION_SLACK`, with `RETENTION_SLACK` a documented constant). The
threshold and the long-lived-copy rule SHALL be documented at the extraction/compaction site.

#### Scenario: A tiny long-lived value does not retain its chunk

- **GIVEN** a small value decoded from a large (e.g. 64 KB) chunk that is retained beyond the scan
  window (e.g. collected into a materialized result)
- **WHEN** the scan advances past that chunk
- **THEN** the chunk buffer is released — the retained tiny value holds a tight standalone allocation,
  not a strong reference to the whole chunk — asserted by a retention test.

#### Scenario: A large streaming value borrows without compaction

- **GIVEN** a value whose size is close to its backing chunk and which is consumed within the current
  window's lifetime
- **WHEN** it is materialized on the streaming path
- **THEN** it borrows (no compaction copy), because its backing is not materially larger than the
  payload.

### Requirement: Zero-copy is core-internal — the FFI boundary always copies

Zero-copy extraction SHALL be internal to core decode and the Arrow/Flight append path only. Bindings
SHALL copy at the FFI boundary so that Python `str`/`bytes` and Node `Buffer`/string values own their
own memory and never hold a reference to a decoded chunk; Arrow arrays SHALL own their materialized
columnar buffers. No decoded chunk `Bytes` SHALL escape across the FFI boundary.

#### Scenario: Binding values own their memory

- **WHEN** the Python and Node binding suites convert `Value::Text` and `Value::Blob` results
- **THEN** the produced Python `str`/`bytes` and Node string/`Buffer` own their memory (a copy at the
  boundary) and remain valid after the underlying scan and its chunks are dropped
- **AND** both binding suites pass with byte-identical converted values.

### Requirement: Decoded values remain byte-for-byte unchanged (parity preserved)

Zero-copy extraction SHALL NOT change any decoded value for any CQL type across any compression
algorithm. Scalar, collection, UDT, frozen, and tuple decode SHALL remain byte-identical, and both the
physical-dump parity and the query-semantics oracle SHALL be unchanged (this is a pure representation
change).

#### Scenario: 33-table parity and the query-semantics oracle are unchanged

- **WHEN** the 33-table JSONL/sstabledump parity suite (LZ4/Snappy/Deflate/Zstd) and the
  query-semantics oracle run after the change
- **THEN** every decoded value matches the pre-change / sstabledump golden output
- **AND** the query-semantics oracle result set at its pinned `now` is unchanged.
