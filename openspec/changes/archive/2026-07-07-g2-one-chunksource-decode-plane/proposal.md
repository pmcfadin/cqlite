## Why

Chunk read → CRC → decompress currently lives in MULTIPLE decode planes. Epic G Wave 4
(issue #1598, under #1519) consolidates them to ONE `ChunkSource`. Investigation of the
worktree establishes the precise state:

- The C2 `ReadAt` read+CRC primitive is already unified in
  `storage/sstable/reader/block_io.rs::read_compressed_chunk_at` (positioned read of
  payload+4-byte trailing CRC in one `read_exact_at`, `crc32fast` CRC **before**
  decompress, returns the compressed bytes).
- The decompress + B1-cache STEP, however, is copy-pasted at three query-path sites, each
  re-doing `cache.get → Compression::new(algorithm) → decompress → DECOMPRESS_CALLS++ →
  cache.insert`: BTI `get` (`data_access/bti.rs` ~713-745, `NS_BTI_CHUNK`), the scan window
  fill (`reader/scan_stream_windowed.rs` ~784-807, `NS_WINDOWED_CHUNK`), and the BIG point
  get (`data_access/mod.rs::get_cached_data` ~543-590, ranged `NS_BIG_POINT` key). A fourth
  (`data_access/big_promoted.rs::pull_reverse_chunk`) decompresses without the cache.
- A SEPARATE legacy `Read+Seek` plane (`storage/sstable/chunk_decompressor.rs`, 997 LOC,
  its own `LruCache`) is consumed only by `BulletproofReader`, which is **not on the query
  path**.
- The CLI query path (`cqlite-cli/src/commands/query.rs`) already routes through
  `Database::execute_streaming` (post-D5, #1581) — the core scan stack — and contains ZERO
  `Bulletproof` references. There is no separate CLI-query decode plane.

Consolidating the duplicated decompress+cache step into one `ChunkSource` removes the
copy-paste, guarantees decode logic lives in exactly one module, and lets a grep/counter
architecture test lock the invariant so the class of drift cannot silently return.

## What Changes

- **Add** one module `storage/sstable/reader/chunk_source.rs` with `ChunkSource`, composing
  `ReadAt` + `CompressionInfo` + `Compression` + `DecompressedChunkCache`. Its operation
  `chunk(index) -> Result<Option<Arc<[u8]>>>` does read → CRC → decompress → B1-cache exactly
  once; a warm hit is an `Arc` clone that skips both read and decompress. A sibling
  `range(offset, size)` variant serves the BIG point-read ranged key.
- **Move** (not rewrite) the read+CRC half from `read_compressed_chunk_at` and the
  best-of-breed decompress+cache tail from the BTI block into that one module.
- **Migrate** BTI `get`, the scan window fill, and the BIG point get onto `ChunkSource`;
  fold `pull_reverse_chunk` in for full coverage. CLI-query needs no change (it already
  rides the core scan).
- **Retire** `BulletproofReader` / `ChunkDecompressor` from the QUERY path and lock it with
  a grep+counter architecture test proving one decode module and no query-path reference to
  the retired readers.
- **Add** the CLI-vs-core query parity test (identical rows, one scan stack).

## Non-goals

- **Do NOT delete `BulletproofReader` or `ChunkDecompressor`.** `rg` confirms non-query
  consumers remain: CLI `inspect`/`info`/`read`/`export_sstable`/`benchmark_sstable`/
  `support`/`mod`, core `sstable_data_manager` (doc TODOs), and test
  `oa_format_compliance_test`. Migrating and deleting them is a documented FOLLOW-UP
  (new issue under Epic G).
- **Compaction read side is out of scope.** Only the query/CLI-query decode plane.
- No format/schema change; no new public API beyond the internal `ChunkSource` and the
  two tests.
