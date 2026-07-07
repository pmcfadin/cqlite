## Context

Issue #1598 (Epic G #1519, Wave 4) consolidates the read → CRC → decompress → cache decode
logic to ONE plane. This is a CONSOLIDATION, not a rewrite: the surviving implementation is
the current best-of-breed code MOVED into one module.

## Current decode planes (established by worktree inspection)

| Plane | Entry / anchor | Basis | On query path? |
|---|---|---|---|
| Legacy `Read+Seek` | `chunk_decompressor.rs::read_data` (L109), own `LruCache` (L164), `decompress_chunk` (L207) | stateful cursor | No — only `BulletproofReader` |
| C2 read+CRC primitive | `block_io.rs::read_compressed_chunk_at` (L543) | `ReadAt`, CRC-before-use | Yes (shared) |
| Decompress+B1 (duplicated) | `bti.rs` L713-745; `scan_stream_windowed.rs` L784-807; `mod.rs::get_cached_data` L543-590; `big_promoted.rs::pull_reverse_chunk` | `Compression` + `DecompressedChunkCache` | Yes (3-4 copies) |

The read+CRC half is ALREADY unified. The only real duplication is the ~15-line decompress+
cache tail. `BulletproofReader`'s stack is a separate older plane used only off the query
path. The CLI query already rides the core scan (`Database::execute_streaming`), so "retire
from the CLI-query decode plane" means proving the invariant, not moving code.

## The ChunkSource abstraction

Module: `cqlite-core/src/storage/sstable/reader/chunk_source.rs`.

```
pub(crate) struct ChunkSource<'a> {
    source: &'a dyn ReadAt,                         // C2
    comp_info: &'a CompressionInfo,                 // chunk offsets/sizes/algorithm
    compression: Option<&'a Compression>,           // None => raw passthrough
    cache: &'a DecompressedChunkCache,              // B1
    file_size: u64,
    header_offset: u64,                             // always 0 for NB/BTI
    namespace: u64,                                 // NS_* cache-key salt
    cache_id: u64,                                  // reader's stable identity
}

impl ChunkSource<'_> {
    /// Whole-chunk read: read -> CRC -> (raw|decompress) -> B1-cache, exactly once.
    pub(crate) fn chunk(&self, index: usize) -> Result<Option<Arc<[u8]>>>;
    /// Ranged (offset,size) read for the BIG point path (aux-keyed).
    pub(crate) fn range(&self, offset: u64, size: u32) -> Result<Arc<[u8]>>;
}
```

`chunk(index)` composition (exactly the moved best-of-breed sequence):
1. `key = ChunkKey::new(cache_id ^ namespace, index)`; `cache.get(&key)` — hit returns
   `Arc` clone (no read, no decompress).
2. Miss: `read_compressed_chunk_at(source, comp_info, index, file_size, header_offset)` —
   positioned read of payload+CRC, `crc32fast` verify BEFORE returning. `Ok(None)` at EOF.
3. Incompressible-raw passthrough (`compressed_len >= max_compressed_length`) → cache raw
   bytes as decompressed.
4. Else `compression.decompress(&compressed)`, bump `DECOMPRESS_CALLS`.
5. `cache.insert(key, decompressed)` → `Arc<[u8]>` (Vec→Arc conversion happens once, in B1).

`range(offset,size)` mirrors `get_cached_data`: `ChunkKey::with_aux(cache_id ^ NS_BIG_POINT,
offset, size)`, positioned `read_exact_at`, optional decompress, `cache.insert`.

Best-of-breed base being MOVED: read+CRC from `read_compressed_chunk_at`; decompress+cache
tail from the BTI block (`bti.rs` L713-745) — it is the most complete (cache-before-
decompress, `DECOMPRESS_CALLS` counter, raw fallback). `read_compressed_chunk_at` itself may
stay in `block_io.rs` and be called by `ChunkSource`, or be moved adjacent; either keeps ONE
read+CRC implementation.

## Migration plan

1. **get (BTI)** — `bti.rs::bti_decompress_and_parse_target` replaces its inline cache/
   decompress block (L713-745) with `chunk_source.chunk(chunk_index)`.
2. **get (BIG point)** — `mod.rs::get_cached_data` becomes a thin call to
   `chunk_source.range(block_offset, size)`.
3. **scan window fill** — `scan_stream_windowed.rs::drain_scan_window_blocking` replaces its
   inline block (L784-807) with `chunk_source.chunk(chunk_count)`; the incompressible-raw
   branch moves inside `ChunkSource`.
4. **BIG reverse** — `big_promoted.rs::pull_reverse_chunk` routes through `ChunkSource` for
   full coverage.
5. **CLI-query** — no code change; add the CLI-vs-core parity test proving identical rows on
   the one scan stack.
6. **Retire from query path** — confirm (and lock via architecture test) that no query path
   references `BulletproofReader`/`ChunkDecompressor`.

## "Decompress in exactly ONE module" — architecture-test approach

A `cqlite-core` integration test (`tests/chunk_decode_single_plane.rs`), modeled on
`tests/compile_time_heuristic_enforcement.rs` and the `parser_no_unwired_modules` guard:

1. Static scan of `cqlite-core/src` for call sites of `Compression::decompress` and inline
   per-chunk `crc32fast` verify sequences, excluding `#[cfg(test)]` blocks and the retired
   `chunk_decompressor.rs`/`bulletproof_reader.rs` (non-query) modules.
2. Assert every remaining query-path call site is inside `reader/chunk_source.rs` — the
   counter must be exactly one module.
3. Assert no file reachable from `get`/windowed-scan/CLI-query references `BulletproofReader`
   or `ChunkDecompressor`.
4. The counter/`DECOMPRESS_CALLS` runtime check: reading one chunk twice warm increments the
   counter exactly once (proves the single miss path and the cache short-circuit).

Deterministic (static source scan + one runtime counter probe), low false-positive
(path-segment match, not bare-word).

## Retired vs deferred

- **Retired from query path (locked by test):** `BulletproofReader`, `ChunkDecompressor`.
- **Deferred (follow-up issue):** deletion of both, after migrating the 7 non-query
  consumers (CLI inspect/info/read/export/benchmark/support, `sstable_data_manager`,
  `oa_format_compliance_test`). Compaction read side untouched.
