# Design — Blocking I/O off async workers (F3)

## Context

`run_scan_stream_windowed` (`scan_stream_windowed.rs`) is a three-stage bounded pipeline:

1. **Async I/O half** (in `run_scan_stream_windowed`): `loop { read_next_block(cursor).await → raw_tx.send(chunk).await }`.
2. **Blocking parse half** (`drain_scan_window_blocking`, a `spawn_blocking` task, issue #1143): decompress + parse + batch → `batch_tx.blocking_send`.
3. **Async forwarder** (a `tokio::spawn`): flatten batches into the caller's per-item channel.

Stage 2's CPU is already off the async workers. Stage 1's **read** is the F3 gap: for the
`Buffered` backend the read is real async `tokio::fs` I/O (reactor-driven, must stay on the
runtime); for the `Mapped` and `Direct` backends `poll_read` performs the blocking work
**synchronously** (mmap page-fault copy / `O_DIRECT` `pread`) and returns `Poll::Ready`, so
the `await` never yields — it just blocks the polling worker for the duration of the disk
I/O.

## Decision: offload the whole per-scan I/O loop for faulting backends (mechanic (b))

When the cursor's backend faults synchronously (`Mapped` / `Direct`), run the **entire**
I/O feed loop on one dedicated `spawn_blocking` thread that reads chunks and hands them to
the existing bounded `raw` channel via `blocking_send` (backpressure preserved: a full
channel parks the blocking thread; the parse half drains it). The `Buffered` backend keeps
the existing inline async loop unchanged.

**One offload per scan, not per chunk.** Mirrors the parse half's single `spawn_blocking`.
Per-chunk `spawn_blocking` would re-introduce the over-spawn cost F3's guardrail warns
against ("do not degrade warm-path latency by over-spawning"; per-scan admission is F4).

**Why `spawn_blocking` + `futures::executor::block_on`, not `block_in_place` or a nested tokio runtime:**

- `tokio::task::block_in_place` panics on a current-thread runtime, and `scan_stream` is
  reachable from current-thread runtimes (the CLI/bindings/harness build them). Not safe
  universally.
- `tokio::runtime::Handle::current().block_on(...)` and building a nested `Runtime` both
  panic from within a runtime context (a `spawn_blocking` thread has the runtime context
  entered). Not usable there.
- `futures::executor::block_on` (crate already a dependency) is a plain thread-parking
  executor with **no tokio involvement**, so it does not trip the nested-runtime guard and
  drives the read future to completion on the blocking thread.

**Soundness invariant (documented at the call site):** driving `read_next_block` under
`futures::executor::block_on` is sound **only** because the mmap/direct per-chunk read path
touches **no** tokio reactor/timer primitive: the per-scan cursor's `tokio::sync::Mutex` is
executor-agnostic (semaphore + wakers, no reactor); the `MmapCursor`/`DirectCursor`
`poll_read`/`poll_complete` are pure synchronous memory/`pread` ops; the CRC digest is
preloaded into memory at open (`CrcDb` holds a `Vec<u32>`, no per-chunk `tokio::fs`); and
`retry_transient_once` re-seeks + retries with **no** `tokio::time` sleep. This path is
never taken for `Buffered` (which does use `tokio::fs` and stays on the reactor). The
invariant is asserted structurally by the offload decision keying on the **actual** cursor
backend, not the configured intent (a `Direct` request that falls back to `Buffered` at
open is read inline, never under `block_on`).

### Data ownership for the offload

`spawn_blocking` requires `'static`. The loop needs: `Arc<Self>` (reader — clone), the
cursor's `Arc<Mutex<BlockSource>>` (clone), the cursor's chunk index, the `raw_tx` sender
(clone), and the `io_failed` flag (`Arc`). `ScanCursor::chunk_index` becomes
`Arc<AtomicUsize>` so it can be shared into the blocking task while the cursor stays usable;
this keeps `ScanCursor` at 16 bytes on 64-bit (two pointers), so the existing size pin holds
unchanged. The blocking task returns the terminal `io_err` (if any); dropping the moved
`raw_tx` when it returns still signals clean EOF to the parse half exactly as the async loop
does.

## Testing strategy (robust against flakiness — issue guardrail)

The `mixed_p99_bounded_by_k_times_baseline` family is known to flake under CPU
oversubscription, so the **primary gate is deterministic and timing-free**:

- **Worker-starvation / offload thread-identity guard** (primary, deterministic): mirror the
  #1143 parse-offload guard. On a fixed 2-worker runtime, open the real multi-chunk fixture
  with `use_mmap = true`, arm a probe, run one full streaming scan, and assert the recorded
  **I/O read thread** is NOT in the enumerated async-worker set. RED on `main` (read runs on
  a worker); GREEN after the offload. No wall-clock threshold; compiled only under the
  existing non-default `scan-offload-probe` feature so the instrumentation never ships.
- **Correctness**: the mmap-backed streaming scan returns the identical row set as the
  buffered-backed scan (proves the scheduling change did not perturb data).
- **Cold-mixed-load harness ratio** (secondary): reuse the A2 harness with `use_mmap = true`
  and assert `mixed.p99 <= K × scan_free.p99` — a **ratio** to a measured baseline captured
  in the same window, never an absolute. Gated skip-not-fail without the fixture.

The full `agent-gate.sh` runs components **serially** (capped machine-wide concurrency,
issue #1825), so the ratio gate is never measured under a competing gate on the same box.
