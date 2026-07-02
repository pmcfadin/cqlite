# Design: uncompressed-crc-verify (core)

## Context

Uncompressed BIG (`nb`) SSTables ship a `CRC.db` component: a per-chunk CRC32 sidecar
Cassandra's `ChecksummedRandomAccessReader` uses to validate every uncompressed read.
CQLite writes this file byte-for-byte (`writer/crc_writer.rs`, issue #1197, byte-parity
tested against the #1190 flush and #1017 compaction goldens) but has **no reader** — the
uncompressed read path (`read_uncompressed_data_block`) returns raw bytes unchecked, and
`verify_sstable` only name-whitelists the component. This change adds the reader and
wires it into the read path, default-on, closing the "silent wrong values on a bit flip"
gap for uncompressed tables so they match the compressed path's fail-fast posture.

The oracle for the `CRC.db` format is twofold and must agree byte-for-byte: (1) our own
#1197 writer, and (2) Cassandra 5.0's `ChecksumWriter` /
`DataIntegrityMetadata.ChecksumValidator`.

## On-disk format (oracle: Cassandra 5.0 `ChecksumWriter`, mirrored by #1197 writer)

```text
[chunk size : 4 bytes, i32, big-endian]   <- SequentialWriter buffer.capacity(), default 65536 (0x00010000)
[CRC32 chunk 0 : 4 bytes, u32, big-endian]
[CRC32 chunk 1 : 4 bytes, u32, big-endian]
...
```

- Chunk size defaults to `64 * 1024` (`CRC_CHUNK_SIZE` in `writer/crc_writer.rs`).
- Each CRC32 covers exactly one `chunk_size` block of the **raw uncompressed** Data.db
  bytes (the final chunk is short). Algorithm: `java.util.zip.CRC32` (IEEE), i.e.
  `crc32fast` — identical to the compressed path's `crc32fast::hash`.
- The **compaction** write path appends one trailing `CRC32 = 0` (`00000000`) empty-final
  chunk (issue #1222); the **flush** path does not. The reader treats a trailing entry
  that maps beyond EOF of Data.db as a non-error (it is never dereferenced by a real
  read) — the read side only ever seeks the CRC for an in-bounds Data.db offset.

## Chunk-to-CRC-index mapping (oracle: `DataIntegrityMetadata.ChecksumValidator`)

For a Data.db byte `offset` and header `chunk_size`:

```text
chunk_index  = offset / chunk_size
crc_file_pos = chunk_index * 4 + 4        // + 4 skips the header
```

`read_uncompressed_data_block` today returns either one bounded piece (piecewise
stitching callers, `UNCOMPRESSED_READ_PIECE_BYTES`) or the whole remaining section
(contiguous callers). To verify, the read path aligns each returned byte range to
`chunk_size` boundaries, and for every fully-covered `chunk_size` block (and the final
short block) recomputes CRC32 and compares to the stored value at `crc_file_pos`. Because
`UNCOMPRESSED_READ_PIECE_BYTES` and `CRC_CHUNK_SIZE` may differ, the implementation
verifies on `chunk_size`-aligned boundaries, buffering at most one chunk's worth beyond
what it already holds — memory stays O(chunk_size), well under the <128 MB budget.

## Decisions

### D1 — Reader byte-agrees with the #1197 writer and a Cassandra fixture
The reader parses the exact layout `writer/crc_writer.rs` emits and Cassandra's
`ChecksumWriter` produces. Acceptance is a round-trip (writer output parses back
identically) **and** a committed Cassandra-written `CRC.db` fixture (from
`test_basic/uncompressed_table`) parses to the expected chunk size + per-chunk values.
This is the no-heuristics anchor: verification consumes authoritative `CRC.db` bytes, not
inferred-from-content values.

### D2 — Verification is default-on and unconditional (posture a), not a knob
Posture (a) — verify on every uncompressed read — is owner-approved and matches the
compressed path's unconditional per-chunk CRC. **What it beat:**
- **Posture (b), verify-only** (`CRC.db` read only inside `verify --mode full`, normal
  reads documented as unverified): rejected because it leaves the plain query surface
  silently returning wrong values on a bit flip — the exact "silent errors" bar the
  epic targets. (We still ADD the `verify --mode full` integration; it is a superset, not
  the ceiling.)
- **Posture (c), config flag defaulting on**: rejected — a toggle invites disabling the
  only integrity check on the uncompressed path, adds surface area, and uncompressed is
  not the perf-critical configuration. No knob is introduced.

### D3 — Failure is a non-recoverable typed corruption error naming chunk + offset
A CRC mismatch on read returns a typed `Error::Corruption` (the uncompressed analogue of
the compressed path's typed chunk-CRC error) whose message names the failing chunk index
and its Data.db byte offset. It is **non-recoverable**: the read aborts; the query does
not fall back to returning the corrupt bytes and does not silently yield 0 rows. In
`verify --mode full`, the same failure is reported as a stable `VerifyErrorClass`
checksum-mismatch variant (the uncompressed analogue of `ChunkDecompressionError`) with a
`VerifyFinding` naming the `CRC.db`/`Data.db` component and the failing chunk — so CI can
match on it. The exact enum variant name is an implementation detail fixed at build time;
the spec requires only that it is stable, distinct, and names the chunk.

### D4 — Missing / short `CRC.db` is a typed, pinned decision
- **Truncated / short `CRC.db`** (header missing, or fewer CRC entries than the Data.db
  has chunks): a typed error (`Error::Corruption` / `UnexpectedEof`-class), never a
  panic and never a silent skip. No `unwrap`/`expect` anywhere in the reader.
- **Absent `CRC.db`** on an uncompressed BIG SSTable where Cassandra would have written
  one: behavior is pinned by a test. Because CQLite targets Cassandra 5.0 (`nb` writes
  `CRC.db`), the default is **fail-closed for the read path is NOT assumed** — the pinned
  decision (warn-and-proceed vs hard-fail) is captured as a spec scenario and a test so
  it cannot drift silently. The parity manifest scenario is updated to record it.

### D5 — Clean-path parity is a hard invariant
The full uncompressed parity suite MUST return byte-identical results with verification
on (a correct `CRC.db` never rejects correct data), and stay within the agreed perf
budget (one CRC32 per chunk; the same primitive the compressed path already pays).

## Interaction with `verify --mode full`

`verify --mode full` already validates inline compressed chunk CRCs and runs a full row
scan. This change adds a `CRC.db` validation step to the full mode for uncompressed
tables: read the header, walk every chunk of `Data.db`, recompute and compare. A mismatch
becomes a `VerifyFinding` with the stable checksum-mismatch class. Quick mode is
unchanged (metadata-only).

## Perf note

Verification adds one `crc32fast` pass over each uncompressed chunk actually read — the
identical cost the compressed path already incurs per chunk. No extra file-sized buffers
are allocated (the reader already streams through a capped scratch buffer, issue #592;
the CRC reader reads only 4 + 4 bytes per verified chunk). The clean-path parity/perf
gate must show no regression beyond the agreed budget.

## Risks / Trade-offs

- **Piece/chunk boundary mismatch.** `UNCOMPRESSED_READ_PIECE_BYTES` ≠ `CRC_CHUNK_SIZE`.
  Mitigation: verify on `chunk_size`-aligned boundaries independent of the read-piece
  size; covered by a multi-chunk fixture scenario.
- **Trailing empty-final-chunk CRC (compaction, #1222).** A compacted `CRC.db` has one
  extra `00000000`. Mitigation: the reader indexes by `crc_file_pos` for in-bounds
  offsets only and never dereferences the trailing entry; covered by a scenario over a
  compaction-written fixture.

## Migration

None. Additive read-path hardening; no on-disk format change, no writer change, no public
API signature change. A previously-silent wrong-value read now becomes a typed error —
the intended behavior change.
