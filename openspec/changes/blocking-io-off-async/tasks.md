# Tasks — Blocking I/O off async workers (F3)

## 1. TDD: red guard first
- [x] 1.1 Add an I/O-read thread-identity probe hook to the `scan-offload-probe` module
  (`record_io_read_thread` / `recorded_io_read_thread`), mirroring `record_parse_thread`.
- [x] 1.2 Write `tests/issue_1593_io_offload_thread.rs`: open the multi-chunk fixture with
  `use_mmap = true`, run one full streaming scan on a fixed 2-worker runtime, assert the
  recorded I/O read thread is NOT an async worker. Confirm it is RED on `main`.
  *Surface exercised:* `SSTableReader::run_scan_stream_windowed` I/O half via `execute_streaming`.

## 2. Implement the offload
- [x] 2.1 `source.rs`: add `BlockSource::faults_synchronously()` (Mapped/Direct → true,
  Buffered → false); change `ScanCursor::chunk_index` to `Arc<AtomicUsize>` (size pin holds);
  update `ScanCursor::new`.
- [x] 2.2 `data_access/mod.rs`: add `read_next_block_parts(&self, file, chunk_index)` and
  delegate `read_next_block` to it (so the blocking loop can call it with shared Arcs).
- [x] 2.3 `scan_stream_windowed.rs`: determine the actual backend once (lock the cursor);
  for a faulting backend run the I/O feed loop on one `spawn_blocking` task
  (`futures::executor::block_on` + `blocking_send`, `io_failed` store, returns terminal
  `io_err`); leave the buffered path as the existing inline async loop. Fire the
  `record_io_read_thread` probe in the blocking loop. Document the reactor-free soundness
  invariant at the `block_on` call site.

## 3. Correctness + ratio harness
- [x] 3.1 Add a correctness test: mmap-backed scan returns the identical row set as buffered.
- [x] 3.2 Extend the A2 tail-latency harness with a cold-mmap ratio path (`mixed.p99 <= K ×
  scan_free.p99`), skip-not-fail without the fixture; ratio only, never a wall-clock bound.

## 4. Wire the gate
- [x] 4.1 Add the F3 offload guard to `scripts/agent-gate.sh` (component or `--test` on the
  scan-offload component), built with `scan-offload-probe` + `cli-helpers`.

## 5. Validate
- [x] 5.1 `openspec validate blocking-io-off-async --strict` clean.
- [x] 5.2 No `unwrap()`/`expect()` in library code; `RUSTFLAGS="-D warnings"` clean; minimal
  and default and `cli-helpers` builds compile.
- [x] 5.3 `scripts/agent-gate.sh --lite` PASS each fix round; verify the p99 gate once in
  isolation (never repeatedly under load).
