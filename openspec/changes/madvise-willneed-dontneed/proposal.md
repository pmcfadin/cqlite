# Proposal: `madvise(WILLNEED)` under Auto-mmap — BUILT, MEASURED, REJECTED (issue #2824)

**Milestone:** 0.17 scan-path throughput program (epic #2817, manifest item M10) · **Priority:** P2 ·
**Routing:** design-driven · **Issue:** #2824 · **Outcome:** **priced re-scope — the lever does not
ship.** Lead ruling on REQ-2824-03, 2026-09-01. · **Refs:** #1143, #2210, #2876, #2412, #2605/PR #3446,
#2818, roborev job 340

## Outcome first

`PrefetchMode::Auto` still maps to **no** `madvise`. This change ships **no behavioural code** — the
`Auto -> WillNeed` flip was implemented, reviewed, measured, and then **reverted**. What ships is the
investigation, the A/B harness, and the recorded measurement, because the finding is that **issue #2824
as filed is not implementable**:

> "Flip `Auto` to issue the **already-built** `madvise(MADV_WILLNEED)` at open … This is a **policy flip
> on built machinery**, not new IO code."

Both halves — `WILLNEED` at open and `MADV_DONTNEED` post-scan-once — depend on scan-lifetime plumbing
that does not exist. Filed as **issue #3853**, targeted at the i4i rig.

## Why the flip cannot ship: advising at open is advising the whole data directory

`SSTableManager::new` calls `load_existing_sstables()`
(`cqlite-core/src/storage/sstable/manager_open.rs:61`), which walks `base_path` recursively to
`MAX_SSTABLE_SCAN_DEPTH = 3` (`cqlite-core/src/storage/sstable/mod.rs:121`) collecting every
`*-Data.db` and **opens all of them in a loop** (`manager_open.rs:300`). That path is reached from
`StorageEngine::open` -> `Database::open` (`cqlite-core/src/lib.rs:240`) — the library, the CLI
interactive and ingest paths, and both the Node and Python bindings.

Defaults are `prefetch = Auto` and `disk_access_mode = Auto`, and `resolve_disk_access_mode` selects
mmap for any file at or above `mmap_min_size_bytes` (4096) under the RAM fraction. So with the flip:

> **`Database::open` issues one whole-file `MADV_WILLNEED` per SSTable, for every SSTable of every table
> of every keyspace under the data directory, before any query is seen.**

A point-lookup-only workload pays that in full, including for tables it never touches. Point lookups
open **zero** additional readers precisely because everything was opened already
(`manager_point_read.rs:135` clones `Arc`s out of the pre-populated map), so bloom and summary gating
cannot help — the read-ahead is queued before any filter runs, and the `MADV_RANDOM` advice on the
later point mapping (#2210) cannot cancel it.

`cqlite-flight` is not a startup storm (it never calls `Database::open`) but has the same shape per
table on first touch: `enumerate_generations` -> `open_added` opens every generation with
`Config::default()`, and token pruning happens *after* the opens. Compaction and merge are unaffected —
they force buffered I/O, so no `madvise` is issued.

Found by roborev job 340 (High) and independently verified against the source before acting.

## Why the obvious fixes are not available

- **Defer the advice to scan start** (roborev's suggestion) is correct in principle and needs the
  scan-lifetime seam: nine scan entry points in three shapes, and no reader-scoped scan-in-flight state
  to hang a guard on. That is the same plumbing AC2 needs.
- **Advise a bounded prefix** would be an unmeasured heuristic (#28), and this lane cannot derive a
  threshold — see below.
- **Leave it to explicit opt-in** is a no-op: `PrefetchMode::WillNeed` already maps to
  `Advice::WillNeed` today. The issue was specifically about the *default*.

## Why the benefit could not be demonstrated either

`docs/reports/issue-2824-artifacts/RESULTS.md`. On this lane the flip measured **no effect and no
regression**: cold major faults 52 -> 50 across ~630,000 file pages, warm unchanged. But the corpus
device is **EBS**, measured at **132 MB/s** with a **128 KiB** read-ahead window, so the default window
already saturates the device and the read is bandwidth-bound. There is **no headroom to detect the
effect in either direction**, and a null result here says nothing about i4i — whose local NVMe at
multiple GB/s is the regime where a 128 KiB window at low queue depth would *not* saturate and
whole-file `WILLNEED` would raise queue depth.

So the position at decision time was a **confirmed cost** against an **unconfirmed benefit**. The lead
ruled that 0.17's criterion is cheap *measured* wins, and that shipping on those terms would be the
anti-pattern the Phase-1 adjudication killed levers for.

## `MADV_DONTNEED` does not do what the issue's AC2 says

AC2's rationale — "so a full-ring scan does not leave the page cache warm" — is not delivered by its
mechanism. Per `madvise(2)`, after `MADV_DONTNEED` on a **file-backed** mapping subsequent accesses
repopulate "from the up-to-date contents of the underlying mapped file"; the call frees the process's
resident PTEs and therefore **RSS**, and the pages remain in page cache. Page-cache eviction is
`posix_fadvise(POSIX_FADV_DONTNEED)`, which #2824 explicitly scopes to the buffered and direct backends.

This is recorded as a standing claim boundary so the successor issue does not inherit the wording
unexamined.

## What ships

- `docs/reports/issue-2824-artifacts/RESULTS.md` — the measurement and its four declared limits.
- `docs/reports/issue-2824-artifacts/ab/construction.md` — the A/B construction assertion: both arms
  built from one tree differing by one match arm, verified by `strace` to differ by exactly one
  `madvise` call, plus the runtime confirmation that `MADV_SEQUENTIAL` is issued by neither arm and the
  #2210 point mapping is untouched.
- `docs/reports/issue-2824-artifacts/cold-warm-ab.sh` — the harness, fail-closed when it cannot drop
  the page cache.
- Corrections to two documents that asserted the old anchors, and this spec delta.

## What does not ship, and where it went

The scan-lifetime advice plumbing — one seam serving `WILLNEED` at scan start *and* `DONTNEED` at scan
end — is filed as **issue #3853**, unmilestoned, cross-referencing roborev job 340 and the #2824 thread.
It should be measured on the i4i rig, where the benefit is demonstrable, and it must re-run
`benches/concurrent_scan.rs`'s `scaling_floors` gate because concurrent scans over one reader are a
supported, tested property (#815).
