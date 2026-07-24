# Proposal: Complete the G2 single decode plane — migrate the last `parsing/` decompress site onto `ChunkSource` (issue #2165)

**Milestone:** 0.17 · **Priority:** P3 · **Routing:** design-driven (I/O-model consolidation with a
behavioral decision) · **Issue:** #2165 (G2 follow-up from #1598)

## Why

Issue #1598 (G2) consolidated the query-path chunk decode (read → CRC → decompress → B1-cache) into
one module, `reader/chunk_source.rs`, and locked the invariant with the architecture test
`cqlite-core/tests/chunk_decode_single_plane.rs`. Two `parsing/` sites were deferred with allowlist
exclusions. Investigation of the current tree (2026-07-23) corrects the issue premise:

- **`parse_partition_at_offset` no longer exists.** `parsing/mod.rs` (now 355 lines, post-#112
  splits and #2302 full-index enumeration) contains zero `.decompress(` calls. Its allowlist
  exclusion (`chunk_decode_single_plane.rs:46`) is vacuous dead config.
- **Exactly ONE query-reachable legacy site remains:** `parse_block_entries`
  (`parsing/block_entries.rs:111`) — inline `Compression::new(...)` + `decompress(block_data)`
  driven by the legacy `self.compression_reader` field, reached from the non-stitching branches of
  `sequential_scan` and `get_all_entries` (public surfaces: `SSTableReader::scan` index-fallback
  paths and `iterate_all_partitions` → `sequential_scan`).
- That site also carries a **silent decompress-failure → parse-raw-bytes fallback**
  (`block_entries.rs:120-125`) — a no-heuristics smell (#28): on corruption it guesses the bytes
  were uncompressed instead of surfacing an error. `ChunkSource` fails closed
  (`Error::corruption`); the legacy site must not keep a divergent semantic.

The archived G2 design's "adaptation layer" framing is heavier than current reality: the in-repo
precedent for exactly this shape — an already-read, already-CRC-validated compressed buffer — is the
stitch path (`data_access/mod.rs:474`), which calls `ChunkSource::decompress_only(...)`.

## What Changes

1. **Migrate `parsing/block_entries.rs:111`** to `ChunkSource::decompress_only(...)` — the same
   pattern as the stitch path. The `compression_reader == None` (uncompressed / no
   `CompressionInfo`) raw passthrough is preserved unchanged.
2. **Drop the silent raw-bytes fallback** on decompress failure — fail closed with a corruption
   error, matching `ChunkSource` semantics and the no-heuristics mandate. The synthetic test
   `test_decompression_fallback_on_failure` flips to assert the fail-closed behavior.
3. **Remove the two allowlist exclusions** (`parsing/mod.rs`, `parsing/block_entries.rs`) from
   `chunk_decode_single_plane.rs` so the architecture test asserts full consolidation of `parsing/`,
   and update its stale doc-comments (the `warm_windowed_scan_skips_decompress` note naming #2165 as
   pending, and the `chunk_source.rs:4-9` module doc naming the deferred sites).
4. **Record the premise correction** on issue #2165 (comment; no scope/title change).

## Non-goals

- No restructuring of the sequential-scan I/O model (`ScanCursor` / `read_next_block` / `block_io`)
  onto `ChunkSource::chunk(index)` — the block read + CRC already happens upstream in the unified C2
  primitive; only the decompress step moves.
- No B1-caching of sequential-scan blocks (a full scan would churn the point-read chunk cache).
- No change to the stitching (`V5CompressedLegacy`/nb) path, BTI scan path, public APIs, or decoded
  bytes on any healthy file.
- No deletion of `parse_block_entries` itself or its callers.

## Doctrine impact

None (no CLAUDE.md / website change). The architecture test and module docs updated in-change are
the record. Behavioral delta on **corrupt** compressed files only: previously a failed block
decompress silently attempted a raw parse; now it errors — the fail-closed direction doctrine
already mandates.
