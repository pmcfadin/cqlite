## Why

The July 2026 read-path audit (`docs/reports/read-path-performance-audit-2026-07-01.md`
§Epic F, child **F3** — "Blocking I/O off async workers") found that on the **mmap** and
**O_DIRECT** scan backends the blocking work runs **inline in `poll_read` on tokio worker
threads** (`cqlite-core/src/storage/sstable/reader/source.rs` — the code comments admit
it): an mmap page fault (cold data = a synchronous disk read inside a memory access) and an
`O_DIRECT` `pread` block the async worker that polls them. `K` concurrent cold scans can
pin **all** async workers, stalling every warm point read behind disk I/O — the
p99-diverges-under-mixed-load mechanism the audit's capstone decision #2 turns on.

The windowed streaming scan already offloads its CPU-bound **decompress + parse** half to a
`spawn_blocking` thread (issue #1143), but the **I/O half** still `await`s
`read_next_block` on the async runtime. For the buffered backend that read is genuinely
async (`tokio::fs`, driven by the reactor — correct, leave it). For mmap/direct it is
synchronous blocking work masquerading as an `await` that never yields, so it runs to
completion on — and starves — the worker.

**Routing: design-driven, owner-pre-decided.** The read-path performance audit is the
source of truth and carries a standing owner Seam-1 approval (audit capstone §5 decision
#2, 2026-07-01). This change encodes F3's locked posture with **no new design latitude**.
The audit offered two mechanics: (a) land C2's sync-core `ReadAt` direction for the scan
reads, or (b) route the faulting/blocking section through the blocking half. We take **(b)**
(see `design.md` for why (a) is a larger refactor deliberately out of scope here).

Milestone: **v0.14 perf wave** (Epic F, Wave 3, after C2). No change to any read result —
this is a scheduling change, not a data change.

## What Changes

- The windowed streaming scan's **I/O feed loop** no longer runs the blocking chunk read on
  an async worker when the scan backend faults synchronously (mmap / `O_DIRECT`). For those
  backends the whole per-scan read loop runs on a dedicated `spawn_blocking` thread, feeding
  the same bounded raw-chunk channel the parse half already consumes; the buffered backend's
  genuinely-async read stays inline on the runtime.
- A new **cold-mixed-load / worker-starvation gate** proves the mechanism directly and
  deterministically (thread-identity, not wall-clock): under a real mmap-backed scan the
  blocking chunk read executes on a `spawn_blocking` thread, **not** on any async worker
  thread. This mirrors the existing #1143 parse-offload guard, so it inherits the same
  no-flake, no-timing property.
- The A2 mixed-load tail-latency harness gains a **ratio-bounded** (never wall-clock
  absolute) assertion for the cold mmap backend, wired advisory→enforcing under the same
  policy as the existing convoy bound.
- The new gate component / `--test` target is wired into `scripts/agent-gate.sh` following
  the existing scan-offload guard's pattern.

## Non-goals

- **Do NOT flip the `use_mmap` default.** Decision #2 gates the default flip on this gate
  existing and proving a p99 win first; the buffered-vs-mmap comparison numbers are reported
  to the owner separately. This change only makes the mmap/direct path non-starving.
- **Blocking-pool admission control** (per-scan `spawn_blocking` count / semaphore capping)
  is **F4's** scope, coordinated separately — not changed here.
- **Hardware-sympathy advice** (`madvise`/`fadvise`) is **F6's** scope; the #1143 measured
  "no madvise in Auto for scans" decision is left intact.
- **The C2 sync-core `ReadAt` trait refactor** (mechanic (a)) is a separate architectural
  change; this change does not undertake it.
- No change to scan output: the emitted row set/order/tombstone filtering is byte-identical.
