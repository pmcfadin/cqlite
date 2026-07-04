## Why

Milestone: Epic C #1515 read-path performance audit (2026-07), Wave 3, child **C2**.
Source of truth: `docs/reports/read-path-performance-audit-2026-07-01.md` §Epic C.

Routing: **design-driven** (perf/architecture). The load-bearing architectural call — a
`ReadAt` sync-core refactor — is **owner-endorsed** (Owner Decision #3, 2026-07-01: "the
`ReadAt` sync-core refactor is endorsed as the target read architecture — build it; do not
re-litigate"). This OpenSpec change transcribes that already-decided design; it does not
propose or weigh alternatives.

The point-read path has two pathologies with one root cause — a **stateful file cursor**:

1. **Shared-cursor convoy.** BIG point reads serialize on an `Arc<Mutex<BlockSource>>` that
   is held **across disk I/O** (`types.rs:217`, `data_access/mod.rs:428-445`). N concurrent
   point reads on one SSTable execute one-at-a-time — a pre-#815 convoy that survived on the
   point path. The lock protects a mutable seek position, so it cannot be dropped before the
   read completes.
2. **Per-lookup `open(2)` fd churn.** BTI lookups sidestep the mutex by paying `File::open`
   **per lookup** (`source.rs:108-130`, default `use_mmap: false`) — an open/close syscall
   pair per operation and fd-exhaustion risk under concurrent load.

## What Changes

The endorsed solution is a **`ReadAt` positional-read trait** — `read_at(&self, offset, buf)`
(and `read_exact_at`) with **no mutable position, no `&mut self`, no seek** — so a single
shared file descriptor's positioned reads are independent and require no lock.

- **Introduce `ReadAt`** in the reader I/O layer.
- **Implement it per backend**: buffered/plain via `std::os::unix::fs::FileExt::read_at` on
  ONE shared `File`; mmap via slice indexing into the map; O_DIRECT via a positioned read that
  honors the existing 4K alignment logic; Windows via `std::os::windows::fs::FileExt::seek_read`,
  cfg-gated consistently with the existing backend code.
- **Migrate the point-read path** — BIG chunk fetch + BTI lookups — off the `Mutex<BlockSource>`
  cursor and off per-op `File::open` onto `ReadAt`: resolve offset → `read_at` the chunk → CRC
  check → decompress. The BTI fd is opened **once at reader open**, then only positioned reads.
- **Preserve** CRC-then-decompress ordering and all error classification, unchanged.

## Non-goals

- **Scans stay on the cursor/windowed pipeline for now — do NOT churn it.** The windowed
  streaming scan (`scan_stream_windowed.rs`) is A-grade, protected "already good" machinery.
  F3 routes scan blocking-I/O work properly later; the trait is designed so scans can adopt
  `ReadAt` later, but this change does not migrate them.
- **Do NOT flip `use_mmap`.** The default disk-access mode (Decision #2) and F3's blocking-fault
  gate are separate; this change keeps the current default and only removes the per-op-open and
  cursor-mutex mechanisms.
- **No change to CRC-then-decompress ordering or error classification.**
- **Windows support is cfg-gated consistently** with the existing backend code — not a new
  first-class platform target introduced here.
- **No cache work** (that is Epic B / B1) and **no BIG index digest fix** (that is C1) — this
  change is scoped to the positional-read/fd/convoy mechanisms only.
