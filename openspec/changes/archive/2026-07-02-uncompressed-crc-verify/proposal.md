# Proposal: uncompressed-crc-verify (core)

> Milestone: Read-path integrity hardening (epic #1380, from the 2026-07-01 Cassandra
> test-parity audit). Issue: #1396.
> Routing: **design-driven** — the deliverable touches the public read path
> (`Database.execute` / `SSTableReader.scan`) and required a product posture decision.
> The posture is **owner-approved (2026-07-01): posture (a) — verify `CRC.db` on every
> uncompressed read, default-on.** This change designs to that posture; it does not
> re-litigate it.

## Why

Compressed BIG reads are genuinely fail-fast: every compressed chunk is CRC32-checked
unconditionally on read (`reader/block_io.rs` ~412-440), and a mismatch surfaces a typed
`Error` naming the chunk/offset. Uncompressed BIG reads have **zero** read-time
integrity. `read_uncompressed_data_block` (`reader/block_io.rs` ~647-740) returns raw
bytes with no checksum, so a single-bit flip inside an uncompressed `Data.db` chunk is
decoded into **wrong values and returned silently** unless it happens to break VInt/flag
parsing.

`CRC.db` — the per-chunk checksum component Cassandra writes for every uncompressed BIG
SSTable — exists in CQLite today only as: a component *name* (`directory/types.rs`), a
byte-parity-tested **write-side** emitter (`writer/crc_writer.rs`, issue #1197), and
export plumbing. **No consumer ever reads its contents** — `verify_sstable` merely
name-whitelists `CRC.db` (`verify.rs` ~497), never validating a byte.

Cassandra verifies uncompressed reads against `CRC.db` chunk checksums
(`ChecksummedRandomAccessReader` / `DataIntegrityMetadata.ChecksumValidator`). This
change closes the gap by giving CQLite a `CRC.db` **reader** that byte-agrees with our
#1197 writer (and Cassandra's `ChecksumWriter`), and wiring it into
`read_uncompressed_data_block` so every uncompressed chunk is verified on read,
default-on, returning a typed corruption error naming the chunk and byte offset.

## What Changes

- **`CRC.db` reader.** A parser for the Cassandra `ChecksumWriter` layout: a 4-byte
  big-endian `i32` chunk-size header followed by one big-endian `u32` CRC32 per chunk.
  It byte-agrees with the #1197 writer (`writer/crc_writer.rs`, `CRC_CHUNK_SIZE = 64 KiB`)
  and with a committed Cassandra-written `CRC.db` fixture. No `unwrap`/`expect`; a
  truncated or short `CRC.db` is a typed error.
- **Read-time verification, default-on.** `read_uncompressed_data_block` maps each byte
  range it returns to its CRC chunk index (`chunk_index = offset / chunk_size`,
  `crc_file_pos = chunk_index * 4 + 4`), recomputes CRC32 over the raw chunk bytes, and
  compares against the stored value. A mismatch returns a typed `Error::Corruption`
  naming the failing chunk index and its Data.db byte offset — never wrong values, never
  a silent 0 rows.
- **`verify --mode full` integration.** The full verifier reads `CRC.db` and reports a
  stable checksum-mismatch error class (the uncompressed analogue of the compressed
  path's chunk-CRC finding) naming the failing chunk, instead of only name-whitelisting
  the component.
- **Corruption corpus.** A new `uncompressed_data_bit_flip` fixture (clean source: the
  Cassandra-written `test_basic/uncompressed_table`, which already ships a real `CRC.db`)
  plus a manifest entry carrying the captured Cassandra `sstableverify` verdict as the
  oracle.

## Non-goals

- **The compressed read path is unchanged.** Compressed tables keep their existing
  unconditional inline per-chunk CRC (`block_io.rs` ~412-440); they do not carry a
  `CRC.db` and are out of scope here.
- **No new config knob / feature flag.** Posture (a) is default-on and unconditional; no
  runtime toggle is introduced (a knob would be posture (c), which was rejected).
- **BTI (`da`) tables are out of scope.** Cassandra does not emit `CRC.db` for BTI; those
  tables are unaffected.
- **Pre-`na` formats are out of scope** (below the supported version floor;
  `BigVersionGates` already rejects `< na`).
- **`Digest.crc32` semantics are unchanged.** This change adds per-chunk `CRC.db`
  verification; it does not alter whole-file digest handling.
- **Not a compaction/writer change.** The #1197 writer is the oracle we read against; it
  is not modified.

## Impact

- **Public surface**: `Database.execute` / `SSTableReader.scan` over an uncompressed BIG
  SSTable now fail-fast with a typed corruption error on a flipped chunk (wiring evidence
  requirement — the plain query surface must exercise the new check, not just a helper
  unit test). CLI `verify --mode full` gains real `CRC.db` validation.
- **No-heuristics mandate**: verification uses **authoritative** `CRC.db` bytes only —
  no inference from Data.db content. When `CRC.db` is present it is read; behavior on a
  missing/short `CRC.db` is a pinned, documented decision (see design D4).
- **Memory budget (<128 MB)**: the reader is streaming — it reads only the 4-byte header
  plus the 4-byte CRC for the chunk under verification; it never buffers the whole
  `CRC.db`.
- **Perf**: one CRC32 hash per uncompressed chunk on read, matching Cassandra's posture.
  The clean-path parity suite must stay byte-identical and within the agreed perf budget.
