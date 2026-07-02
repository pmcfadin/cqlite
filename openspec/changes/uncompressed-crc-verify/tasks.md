# Tasks: uncompressed-crc-verify (core)

> All tasks are implementation to be done **after** owner spec approval (Seam 1). This
> change ships spec artifacts only; nothing below is started yet.

## 1. CRC.db reader (surface: `reader/` CRC parser + unit tests)
- [ ] Add a `CRC.db` reader parsing the `ChecksumWriter` layout (4-byte big-endian `i32`
      chunk-size header + one big-endian `u32` CRC32 per chunk), mirroring
      `writer/crc_writer.rs` (`CRC_CHUNK_SIZE`). No `unwrap`/`expect`; return typed errors.
- [ ] Expose `crc_for_offset(offset)` using `chunk_index = offset / chunk_size`,
      `crc_file_pos = chunk_index * 4 + 4`; stream — read only header + the one CRC entry.
- [ ] Unit test: parse the committed Cassandra-written `CRC.db`
      (`test_basic/uncompressed_table`) and assert chunk size `65536` + per-chunk CRC32
      byte-agree with CRC32 recomputed over the raw Data.db chunks.
- [ ] Unit test: round-trip the #1197 writer output through the reader (multi-chunk).
- [ ] Unit test: truncated/short `CRC.db` → typed error, no panic.

## 2. Wire verification into the uncompressed read path (surface: `Database.execute` / `SSTableReader.scan`)
- [ ] In `read_uncompressed_data_block` (`reader/block_io.rs`), verify each returned chunk
      against `CRC.db` on `chunk_size`-aligned boundaries, default-on, unconditional.
- [ ] On mismatch return typed `Error::Corruption` naming the failing chunk index + Data.db
      byte offset; never return corrupt bytes / wrong values / silent 0 rows.
- [ ] Handle absent `CRC.db` per the pinned decision (design D4); leave the compressed
      path (`block_io.rs` ~412-440) untouched.
- [ ] Wiring-evidence e2e test: plain `Database.execute` over the bit-flipped uncompressed
      fixture returns `Error::Corruption` naming chunk/offset; clean fixture returns
      correct rows.
- [ ] e2e test: single flip in a non-first chunk is attributed to chunk *k* (boundary).

## 3. verify --mode full integration (surface: `cqlite verify --mode full`)
- [ ] Add a stable, distinct `VerifyErrorClass` checksum-mismatch variant (uncompressed
      analogue of the inline compressed chunk-CRC finding); replace the `CRC.db`
      name-whitelist (`verify.rs` ~497) with real content validation in `VerifyMode::Full`.
- [ ] Test: `verify --mode full` reports the checksum-mismatch finding naming the chunk on
      the corrupt fixture; reports none on the clean source.

## 4. Corruption corpus + oracle (surface: corpus generator + parity manifest)
- [ ] Add `uncompressed_data_bit_flip` to `generate-corruption-corpus.sh` (clean source:
      Cassandra-written `test_basic/uncompressed_table`; single deterministic byte flip).
- [ ] Add the manifest entry (mutation offset, original/mutated bytes, SHA-256 bindings,
      `verdict_captured_for_dir_sha256`) with the captured Cassandra 5.0.2 `sstableverify`
      verdict as oracle; record the absent-`CRC.db` pinned behavior scenario.
- [ ] Parity test: CQLite verify verdict matches the captured Cassandra verdict for this
      class; fixture is SHA-bound and fails closed when absent.

## 5. Clean-path regression + perf
- [ ] Run the full uncompressed parity suite (sstabledump JSONL goldens) with verification
      on; assert byte-identical results vs baseline.
- [ ] Confirm no memory/perf-budget regression (O(chunk_size) working set; one CRC32 per
      chunk; no file-sized allocations) — perf gate within the agreed budget.

## 6. Validate + review (gate → C → roborev)
- [ ] `bash scripts/agent-gate.sh` (with `CQLITE_DATASETS_ROOT` → datasets) → PASS; paste
      the AGENT-GATE SUMMARY block verbatim.
- [ ] Run clippy `RUSTFLAGS="-D warnings"` workspace + `--all-targets`; zero warnings.
- [ ] spec-auditor (**C**) anchored to
      `openspec/changes/uncompressed-crc-verify/specs/**` → PASS (every requirement
      satisfied with a public-surface test as evidence).
- [ ] roborev `/roborev-review-branch --base origin/main --agent codex --wait` → clean.

## 7. Finalize
- [ ] Merge on green (gate PASS + C PASS + roborev clean); squash + delete branch.
- [ ] `openspec archive uncompressed-crc-verify`; remove worktree/branch; close #1396 with
      a comment linking epic #1380.
