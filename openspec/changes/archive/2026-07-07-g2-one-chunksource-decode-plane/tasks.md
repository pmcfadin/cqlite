## 1. Tests first (TDD red)

- [ ] 1.1 Add `tests/chunk_decode_single_plane.rs`: grep/counter architecture test asserting
      `Compression::decompress` + inline chunk-CRC on the query path resolve to exactly one
      module (`reader/chunk_source.rs`), and no query path references `BulletproofReader`/
      `ChunkDecompressor`. Verify it REDS on pre-migration state (multiple modules).
- [ ] 1.2 Add the CLI-vs-core query parity test: same SELECT via CLI query command and core
      `Database` API returns byte-identical rows on the one scan stack. Verify it compiles
      and expresses the invariant.
- [ ] 1.3 Add a warm-cache `DECOMPRESS_CALLS` probe: reading a chunk twice increments the
      counter exactly once.

## 2. Introduce ChunkSource (move, don't rewrite)

- [ ] 2.1 Create `reader/chunk_source.rs` with `ChunkSource` + `chunk(index)` composing
      `read_compressed_chunk_at` (read+CRC) → raw/decompress → B1 `cache.insert`.
- [ ] 2.2 Add the `range(offset,size)` variant for the BIG-point ranged key.
- [ ] 2.3 Wire `chunk_source.rs` into `reader/mod.rs`; keep one read+CRC impl.

## 3. Migrate the query paths

- [ ] 3.1 Migrate BTI `get` (`bti.rs` L713-745) to `chunk_source.chunk(idx)`.
- [ ] 3.2 Migrate scan window fill (`scan_stream_windowed.rs` L784-807) to `chunk(idx)`;
      move the incompressible-raw branch inside `ChunkSource`.
- [ ] 3.3 Migrate BIG point `get_cached_data` (`mod.rs` L543-590) to `range(offset,size)`.
- [ ] 3.4 Migrate `big_promoted.rs::pull_reverse_chunk` for full coverage.

## 4. Retire from the query path

- [ ] 4.1 Confirm no query path references `BulletproofReader`/`ChunkDecompressor`; make the
      architecture test (1.1) GREEN.
- [ ] 4.2 Make the CLI-vs-core parity test (1.2) GREEN.

## 5. Follow-up scope (documented, NOT implemented here)

- [ ] 5.1 Record the follow-up issue: migrate the 7 non-query `BulletproofReader` consumers
      off it, then delete `bulletproof_reader.rs` + `chunk_decompressor.rs`.

## 6. Validate

- [ ] 6.1 33-table golden parity green after migration.
- [ ] 6.2 Compression matrix (LZ4/Snappy/Deflate/Zstd + raw) green through the one plane.
- [ ] 6.3 No `unwrap`/`expect` in `chunk_source.rs`; `RUSTFLAGS="-D warnings"` clean across
      query-relevant feature combos.
- [ ] 6.4 `openspec validate g2-one-chunksource-decode-plane --strict` clean.
