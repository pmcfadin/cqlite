# Tasks — readat-positional

## 1. TDD tests first (write FIRST, must fail on current `main`)
- [ ] 1.1 **Convoy test** (surface: `SSTableReader::get` / point-read path): wrap the source in a
      test double whose `read_at` sleeps 10ms; issue 8 concurrent `get()`s on one SSTable and assert
      total wall time ≪ 8×10ms (e.g. < 40ms). Must fail (~80ms) on current `main`.
- [ ] 1.2 **fd high-water test** (surface: BTI point-lookup path): record open-time fd count, issue
      64 point lookups + 8 scans, assert fd high-water ≤ open-time fds + small constant. Uses A5's fd
      helper. Must fail on current `main` (BTI per-lookup opens).
- [ ] 1.3 **Positional-read correctness test** (surface: `ReadAt` impls): interleaved `read_at` at
      distinct offsets each return exactly the bytes at their offset; assert the trait methods take
      `&self` (no shared mutable cursor). Cover buffered, mmap, and O_DIRECT backends.
- [ ] 1.4 **CRC-before-decompress test** (surface: BIG chunk-fetch path): a chunk with a bad CRC
      fetched via `read_at` raises the same corruption error before decompress; decompressor not invoked.
- [ ] 1.5 **33-table value parity test** (surface: `Database::execute` `WHERE pk = ?` point path):
      point reads via the `ReadAt` path byte-match the pre-refactor results and sstabledump JSONL
      goldens; skip-not-fail when binaries absent.

## 2. `ReadAt` trait + backends (production)
- [ ] 2.1 Define the `ReadAt` trait in the reader I/O layer: `read_at(&self, offset, buf) -> Result<usize>`
      and `read_exact_at` — no mutable position, no `&mut self`, no seek.
- [ ] 2.2 Implement `ReadAt` for the buffered/plain backend via `std::os::unix::fs::FileExt::read_at`
      on ONE shared `File`.
- [ ] 2.3 Implement `ReadAt` for the mmap backend via bounds-checked slice indexing.
- [ ] 2.4 Implement `ReadAt` for the O_DIRECT backend via a positioned read honoring the existing 4K
      alignment logic.
- [ ] 2.5 cfg-gate a Windows `ReadAt` impl via `std::os::windows::fs::FileExt::seek_read`, consistent
      with the existing backend cfg-gating.

## 3. Migrate the point-read path (production)
- [ ] 3.1 BIG chunk fetch (`data_access/mod.rs:428-445`): resolve offset → `read_at` chunk → CRC →
      decompress; remove the `Mutex<BlockSource>` from this path. Preserve CRC-then-decompress ordering
      and error classification.
- [ ] 3.2 BTI lookups (`source.rs:108-130`): open the fd ONCE at reader open and store the `ReadAt`
      backend on the reader; replace per-lookup `File::open` with positioned reads.
- [ ] 3.3 Confirm no `unwrap()`/`expect()` introduced in library code; error paths reuse existing
      classification.

## 4. Guardrails (do-not-touch verification)
- [ ] 4.1 Assert `scan_stream_windowed.rs` is unmodified by this change (scans stay on the existing
      cursor/windowed pipeline; F3 handles scan blocking later).
- [ ] 4.2 Assert `use_mmap` default is unchanged (this change does not flip it).

## 5. Validation
- [ ] 5.1 Run tasks 1.1–1.5 red-then-green (with `CQLITE_DATASETS_ROOT` set for parity).
- [ ] 5.2 `scripts/agent-gate.sh` PASS — paste the AGENT-GATE SUMMARY block verbatim.
- [ ] 5.3 `RUSTFLAGS="-D warnings"` clean; workspace clippy `--all-targets`.
- [ ] 5.4 spec-auditor (**C**) PASS against `openspec/changes/readat-positional/specs/**`.
- [ ] 5.5 roborev clean (`--base origin/main`).
