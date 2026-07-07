# scan-window-substrate

## ADDED Requirements

### Requirement: The windowed scan carries chunk bytes as a refcounted substrate

The streaming windowed scan SHALL make decompressed chunk bytes available to the parse half as a
refcounted, borrowable substrate (`bytes::Bytes` / the existing B1 `Arc<[u8]>`) rather than only as
bytes copied into an owned `Vec<u8>` window. This applies to `WindowCursor`
(`reader/window_cursor.rs`) and the `scan_stream_windowed.rs` fill path. Decompression SHALL occur such that the window can hand out a refcounted subslice
view of a chunk (a `Bytes` slice) without an intervening full copy on the borrow path. The B1
`DecompressedChunkCache` `Arc<[u8]>` contract SHALL be preserved (the window fill site aligns with the
cached `Arc`; a cache hit remains a refcount bump, never a memcpy).

#### Scenario: Steady-state scan allocates at most one buffer per chunk

- **GIVEN** the dhat allocation lane running a steady-state windowed scan over a present multi-chunk
  fixture
- **WHEN** the scan decompresses and parses K chunks
- **THEN** the allocation count attributable to chunk handling is ≤ 1 per chunk (on `main` it is ≥ 2)
- **AND** the read-op work-counter remains exactly 1 read per chunk (the E3/A5 invariant is not
  regressed).

#### Scenario: The window can hand out a borrowed chunk subslice

- **WHEN** the parse half needs the bytes for a value that lies within the current window
- **THEN** it can obtain a refcounted `Bytes` subslice of the chunk substrate (a view), not only a copy
  into the owned window buffer.

### Requirement: CRC ordering, bounded memory, and byte-parity are preserved

The substrate change SHALL NOT alter correctness. CRC verification SHALL still happen before any
decompressed output is trusted, in the same order as today. The windowed scan's bounded-memory
discipline and window-size semantics SHALL be unchanged. `uncompressed_len` SHALL continue to come from
`CompressionInfo` (authoritative — no guessing / no-heuristics). Decoded bytes SHALL be byte-identical
across all four compression algorithms.

#### Scenario: 33-table byte-parity holds across every compression algorithm

- **WHEN** the 33-table byte-parity harness runs over LZ4, Snappy, Deflate, and Zstd fixtures after the
  substrate change
- **THEN** every decoded value matches the sstabledump/JSONL golden output for all four algorithms
- **AND** the windowed scan's documented worst-case memory bound (window size) is unchanged.

#### Scenario: CRC is verified before decompressed bytes are trusted

- **WHEN** a chunk with a deliberately corrupted CRC is read on the substrate path
- **THEN** the read fails on the CRC check before any decompressed bytes are handed to the parse half
  (the check order matches `main`).
