## Why

The July 2026 read-path performance audit
(`docs/reports/read-path-performance-audit-2026-07-01.md`, Epic F finding **F4**)
found a priority inversion in the windowed streaming scan's use of tokio's
blocking pool:

- Each windowed scan holds a long-lived `spawn_blocking` PARSE task
  (`drain_scan_window_blocking`, issue #1143) for the whole scan.
- F3 (issue #1593, now merged) additionally moved the raw-chunk I/O of a
  synchronously-faulting backend (mmap page fault / `O_DIRECT` `pread`) onto a
  SECOND long-lived `spawn_blocking` FEED task (`feed_raw_chunks_blocking`), so a
  faulting-backend scan now holds **two** long-lived blocking-pool threads at once.
- The blocking pool defaults to 512 threads and is SHARED with tokio-fs internals.
  `K` concurrent cold faulting-backend scans pin `~2K` blocking threads; at high
  scan concurrency latency-critical point-read file operations queue behind those
  long-lived throughput tasks — the priority inversion the audit named, and the
  `~2K` footprint the flow-lead flagged on issue #1594 when F3 merged (roborev
  deferred it from #1593 to F4).

The fix is ADMISSION CONTROL at the scan layer: a bounded number of windowed
scans are admitted to the blocking pool concurrently; excess scans WAIT
(natural backpressure) rather than oversubscribe the pool, leaving guaranteed
headroom for fs / point-read operations.

**Routing: design-driven.** This is a read-path scheduling/mechanics change (bound
concurrency admitted to a shared thread pool), not an oracle-driven parse-correctness
bug, so it is captured as an OpenSpec change per the spec-driven doctrine. The
design and priority are owner-approved via the read-path performance audit
(standing owner Seam-1 approval, v0.14 perf wave); this change encodes that
decision rather than re-litigating it.

Milestone: **v0.14 performance wave** (Epic F, #1518). The scan output is
UNCHANGED — this is a scheduling change only. Parity is the proof: the windowed
scan emits the identical row set/order/tombstone filtering as before.

## What Changes

- **Add** a process-wide bounded admission mechanism for windowed streaming scans
  (`scan_admission`): a shared `tokio::sync::Semaphore` whose permit count caps the
  number of windowed scans admitted to the blocking pool concurrently.
- **Acquire** one permit at the top of `run_scan_stream_windowed`, BEFORE any
  `spawn_blocking` (parse or faulting-backend feed) is spawned, and hold it for the
  whole scan via an RAII guard. The permit — and therefore the admission slot —
  releases on scan completion, error, OR cancellation/drop (RAII `Drop`), so a
  scan can never leak a slot.
- **Size** the default cap from `available_parallelism` (the parse work is
  CPU-bound, so more concurrent scans than cores yields no throughput). Because a
  faulting-backend scan holds TWO blocking threads per admitted slot (F3's doubled
  footprint), `cap` admitted scans use at most `2 × cap` blocking threads,
  bounding the footprint to a small multiple of the CPU count regardless of `K`
  and leaving the rest of the 512-thread default pool free for fs/point ops.
- **Queue-full behavior:** the `cap + 1`-th scan WAITS on the semaphore (the
  scan's own spawned task blocks at `admit().await`) and proceeds when a permit
  frees. Scans never error from admission.
- **Add** a `scan-offload-probe`-gated test surface (a test-only limit override +
  in-flight/max-in-flight counters) proving the wiring: a full concurrent-scan run
  never exceeds the admission limit, and the semaphore-behavior unit tests prove
  the bound, the wait-then-proceed queueing, and no-permit-leak across many
  acquire/drop cycles (including cancellation/drop). This surface compiles ONLY
  under the non-default feature — zero production/public surface.

## Non-goals

- **No new production config knob.** The cap has a documented, `available_parallelism`-
  derived default; the only override is the `scan-offload-probe`-gated test surface
  (test instrumentation, not a shipped knob), so the guardrail against decorative
  knobs is honored.
- **Do NOT wire or resurrect `platform/threading.rs`** (audit capstone §3 / AK2).
  As built it cannot bound async scans; the admission point is designed fresh at
  the scan layer. If AK2 has not deleted that module yet, it is ignored entirely.
- **No change to scan output.** Row set, order, and tombstone filtering are
  byte-identical; this is a scheduling change only.
- **Not** re-litigating the pre-`na` version floor or the no-heuristics mandate.
