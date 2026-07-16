# Extend blocking-pool admission to the eager multi-generation merge path

## Milestone
0.14+ read-path perf wave (Epic F, §F4). Design-driven — this is the deferred second half of #1594
(F4), which added scan-operation admission control to the LAZY/WINDOWED path but explicitly carved out
the EAGER `merge_generations_for_read` branch as out of scope. This change removes that carve-out so
the most common write-support multi-generation read is bounded like the windowed path.

## Why (measured problem)
Source of truth: #1594, the read-path audit §F4, and the `# Scope` doc comment in
`cqlite-core/src/storage/sstable/reader/scan_admission.rs:51-58`.

#1594 fixed a fan-out self-deadlock and bounded concurrent windowed scans against a process-wide
admission semaphore (sized from `available_parallelism()`), acquiring **one permit per scan operation**
with sub-scans marked `Exempt`. But the DEFAULT write-support build's most common multi-generation
read takes the EAGER branch — `merge_generations_for_read` (`generation_merge.rs:238`), chosen when
`reader_list.len() > 1` AND a schema is present — which drains a `KWayMerger` inside a single
`spawn_blocking` and **never passes through the admission semaphore**. So concurrent eager multi-gen
scans (the schema-present common case) remain unthrottled: the priority-inversion / oversubscription
class F4 targets is only *partially* covered, and the admission bound silently does not apply to the
path most production reads actually use.

The eager path's real footprint is `1 spawn_blocking task + M std::thread producer threads` per
operation (M = generation count; KWayMerger, #2316) — so `cap` concurrent eager operations can pin up
to `cap × M` producer OS threads with no ceiling on operation concurrency at all.

## What changes
Acquire the **same operation-level admission permit** at the top of the eager merge path, before its
`spawn_blocking`, holding the RAII `ScanAdmissionPermit` across the whole operation — mirroring the
`ScanAdmission::Acquire` arm the windowed path already uses. Because the eager path is a single
`spawn_blocking` with no async fan-out, **one permit for the whole operation** is the exact fit and
cannot reintroduce the #1594 hold-and-wait deadlock (there is exactly one thing wanting exactly one
permit; the KWayMerger's producer threads are plain OS threads that never call `admit()`).

Concretely:
1. Acquire `scan_admission::admit().await` at the top of ALL THREE eager helpers, each of which has the
   identical single-`spawn_blocking(KWayMerger)` shape and was equally unadmitted:
   - `merge_generations_for_read` — the materializing plain read (`scan` / range / point read).
   - `seek_merge_generations_for_read` — the partition-SEEKING point-read merge (multi-candidate
     `WHERE pk = ?`, via `scan_partition_clustering`); its sole call site is a top-level manager
     operation, never nested under another admitted operation, so admitting it is deadlock-safe.
   - `merge_generations_for_read_with_metadata` — the `WRITETIME`/`TTL` projection sibling.
2. Cancellation safety: the two PURE-blocking helpers (`merge_generations_for_read`,
   `seek_merge_generations_for_read`) MOVE the `OwnedSemaphorePermit` INTO the `spawn_blocking` closure,
   so a cancelled/dropped join holds the slot until the detached blocking work terminates (repeated
   cancels can never exceed the bound). The metadata helper's permit must span an async per-reader loop
   OUTSIDE `spawn_blocking`, so it stays an outer future guard with a documented weaker residual.
3. Update the `scan_admission.rs` `# Scope` doc comment to reflect that the eager path is now admitted
   (naming all three helpers + the cancellation shapes) and fix its stale `storage/sstable/mod.rs`
   reference → `generation_merge.rs`.
4. Add an eager-path admission bound + deadlock-freedom regression guard (a `scan-offload-probe`-gated
   test mirroring `issue_1594_scan_admission_bound.rs`, driving the eager branch via a multi-generation
   fixture WITH a schema present), including end-to-end coverage of the metadata sibling and the seek
   helper. The `scan-offload-guard` gate component runs the new test target.

## Non-goals
- **Not** making the core admission semaphore operator-tunable. It stays auto-sized from
  `available_parallelism()` (status quo). The Flight-layer `--max-concurrent-scans` (#2420) is a
  separate `do_get`-boundary semaphore and is untouched. A future issue may unify/expose these.
- **Not** re-sizing or splitting the semaphore to bound the eager path's OS-thread footprint
  separately — this change unifies *operation* admission under the existing shared bound (consistent
  with #1594's "operation concurrency, not total blocking threads" framing). The OS-thread-footprint
  distinction is documented, not solved here.
- **Not** changing the KWayMerger, the eager-vs-lazy branch selection, or the per-reader-concatenation
  merge-error fallback.

## Doctrine impact
No public API or user-facing change. Internal concurrency behavior only. Updates the `scan_admission`
scope doc comment; no CLAUDE.md / website change required (the admission semaphore is not a documented
user surface).
