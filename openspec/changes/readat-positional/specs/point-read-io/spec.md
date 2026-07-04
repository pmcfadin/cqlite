## ADDED Requirements

### Requirement: Concurrent point reads do not serialize on a cross-I/O mutex
The point-read path SHALL NOT hold a mutex across disk I/O. Concurrent point reads on a single
SSTable SHALL execute without serializing on a shared file-cursor lock — the `Arc<Mutex<BlockSource>>`
convoy on the point path is removed. Reads SHALL be served via positioned reads (`ReadAt`) on a shared
file descriptor, whose positioned reads are independent of one another.

#### Scenario: 8 concurrent gets do not convoy
- **GIVEN** an SSTable whose read backend is a test double whose `read_at` sleeps 10ms per call
- **WHEN** 8 concurrent point reads (`get()`) are issued against that one SSTable
- **THEN** total wall time is far less than 8×10ms (e.g. under 40ms), proving the reads do not serialize
- **AND** on current `main` the same test fails (~80ms) because the shared-cursor mutex serializes them

### Requirement: BTI lookups do not open the file per lookup
BTI point lookups SHALL open the underlying file descriptor exactly once (at reader open) and issue
positioned reads thereafter. A point lookup SHALL NOT call `File::open` per operation.

#### Scenario: fd high-water stays bounded under load
- **GIVEN** an open BTI reader and the open-time file-descriptor count recorded
- **WHEN** 64 point lookups and 8 scans are issued
- **THEN** the process fd high-water mark is at most open-time fds plus a small constant
- **AND** on current `main` the same test fails on BTI because each lookup opens the file

### Requirement: Positioned reads return correct bytes with no shared mutable cursor
The `ReadAt` trait SHALL expose `read_at(&self, offset, buf) -> Result<usize>` and `read_exact_at`
with no mutable position, no `&mut self`, and no seek. Each backend (buffered/plain via
`FileExt::read_at` on one shared file, mmap via slice indexing, O_DIRECT honoring 4K alignment, and
the cfg-gated Windows `seek_read`) SHALL return the exact bytes at the requested offset independent of
any other in-flight read.

#### Scenario: interleaved positioned reads each return their own bytes
- **GIVEN** a `ReadAt` backend over a file with known content
- **WHEN** two positioned reads at different offsets are issued concurrently (interleaved)
- **THEN** each read returns exactly the bytes at its requested offset, with no cross-contamination from the other read's position
- **AND** the trait methods take `&self` (no `&mut self`, no seek), so no shared mutable cursor exists

### Requirement: CRC is verified before decompression on the chunk path
The BIG chunk-fetch path migrated onto `ReadAt` SHALL preserve CRC-then-decompress ordering: the chunk's
CRC is verified before the payload is decompressed, and error classification is unchanged.

#### Scenario: a corrupt chunk fails CRC before any decompress attempt
- **GIVEN** a chunk whose stored CRC does not match its payload, fetched via `read_at`
- **WHEN** the point-read path fetches that chunk
- **THEN** the CRC check fails and the same corruption error is raised before decompression is attempted
- **AND** the decompressor is never invoked for that chunk

### Requirement: Rows returned via the ReadAt point-read path byte-match the prior path
Point reads served through the `ReadAt` path SHALL be value-identical to the pre-refactor point-read
path over the 33-table corpus (correctness oracle).

#### Scenario: 33-table point-read parity
- **GIVEN** the 33-table test corpus with real SSTable binaries present
- **WHEN** point reads are executed over each table via the `ReadAt` point-read path
- **THEN** every returned row byte-matches the result the pre-refactor path produced (and the sstabledump JSONL goldens)
- **AND** when the binaries are absent the parity test skips clean rather than passing on 0 rows

### Requirement: Scans remain functionally unchanged
This change SHALL NOT migrate or rewrite the windowed streaming scan. Scan output and behavior SHALL be
unchanged; the trait is shaped so scans can adopt `ReadAt` later (F3) without requiring it now.

#### Scenario: scan output is unchanged after the point-read migration
- **GIVEN** the windowed streaming scan pipeline (`scan_stream_windowed.rs`) untouched by this change
- **WHEN** a full scan is executed over a corpus table
- **THEN** the scan returns the same rows in the same order as before the point-read migration
- **AND** the scan continues to use the existing cursor/windowed pipeline (no `scan_stream_windowed.rs` rewrite)
