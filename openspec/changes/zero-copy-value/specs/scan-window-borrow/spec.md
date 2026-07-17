# scan-window-borrow

## ADDED Requirements

### Requirement: The scan window hands out a refcounted `Bytes` subslice view of the chunk

The `WindowCursor` SHALL expose a value-materialization borrow API that returns a refcounted
`bytes::Bytes` view of a requested byte range without copying into the owned window buffer, consuming
the Stage-2 (#1940) per-chunk `Bytes` substrate. When the logical window is backed by exactly one
chunk's `Bytes` (the steady-state case, after the previous chunk was fully consumed), a refill SHALL
replace the backing with the incoming `Bytes` by move + refcount rather than by
`extend_from_slice` copy, and a borrow of a range within it SHALL return a `Bytes::slice`/`slice_ref`
view (no allocation). The parser's scanning/boundary logic SHALL continue to read the window as a
`&[u8]` slice unchanged; only value materialization uses the borrow API.

#### Scenario: A value fully within one chunk is borrowed, not copied

- **GIVEN** a windowed scan whose current window is backed by a single chunk's `Bytes`
- **WHEN** a value's byte range lying entirely within that chunk is materialized through the window
  borrow API
- **THEN** the returned payload is a refcounted `Bytes` view of the chunk (the window-borrow work-probe
  records zero bytes copied for that value), exercised end-to-end through a scan
- **AND** the scan returns a non-zero number of rows with byte-identical values.

### Requirement: A value straddling a chunk boundary copies (correctness over borrow)

The borrow API SHALL return an owned copy (not a borrowed view) when a value's byte range straddles a
decompression-chunk boundary — the window is a stitched owned buffer for that range because the value
has no single parent `Bytes` — and the decoded value SHALL be byte-identical to the non-straddling case.
Correctness SHALL never be sacrificed to avoid a copy.

#### Scenario: A straddling value decodes byte-identically via an owned copy

- **GIVEN** a fixture where a value's bytes cross a decompression-chunk boundary
- **WHEN** the scan materializes that value through the window borrow API
- **THEN** the returned payload is an owned (copied) `Bytes` and the decoded value matches the
  sstabledump/JSONL golden for that cell.

### Requirement: Window byte-movement and bounded-memory contracts are preserved

The borrow API SHALL NOT change the window's cursor/`consume`/`refill` byte-movement contract
(issue #1589) or its bounded-memory / window-size discipline. A `Bytes`-backed window SHALL hold at most
one chunk's worth of bytes for the borrowed backing, the same bound as one stitched chunk. CRC
verification order and the #1940 ≤1-alloc/chunk substrate SHALL be unregressed.

#### Scenario: Byte-movement and memory bound are unchanged

- **WHEN** the issue-#1589 window byte-movement probe and the #1940 allocs/chunk lane run over a
  present multi-chunk fixture after the borrow API is added
- **THEN** the window's total bytes physically moved and its documented worst-case memory bound are
  unchanged from before the borrow API
- **AND** the steady-state scan still allocates at most one buffer per chunk.
