# Design — eager multi-generation merge admission

## Context

#1594 (F4) introduced `scan_admission` (`cqlite-core/src/storage/sstable/reader/scan_admission.rs`): a
process-wide `static OnceLock<Arc<Semaphore>>` sized from `available_parallelism().unwrap_or(4).max(1)`,
reached through a free function `admit() -> ScanAdmissionPermit` (RAII; releases on every exit path via
`Drop`; fail-open if the semaphore is closed). The windowed/lazy paths acquire **one permit per scan
operation** (`ScanAdmission::Acquire`) and mark cross-generation sub-scans `Exempt` so a fan-out merge
holds exactly one permit (`mod.rs:2226-2242`, `sequential.rs:377-380`).

The eager path is THREE helpers in `generation_merge.rs`, each explicitly carved out (scope doc
`scan_admission.rs:51-58`), each chosen when `reader_list.len() > 1` (or `candidates.len() > 1` for the
point read) AND a schema is present, and each draining a `KWayMerger` in a **single** `spawn_blocking`:

- `merge_generations_for_read` — the materializing plain read (`mod.rs:1135`, `1646`).
- `seek_merge_generations_for_read` — the partition-SEEKING point-read merge (`mod.rs:1650`, reached
  via `scan_partition_clustering`); `#[cfg(all(write-support, not(tombstones)))]`.
- `merge_generations_for_read_with_metadata` — the WRITETIME/TTL sibling (`mod.rs:1482`, `1854`).

## Goals / constraints
- Bound concurrent eager multi-gen operations under the SAME operation-concurrency semaphore as the
  windowed path (one shared admission bound for all scan operations).
- **Must not** reintroduce the #1594 fan-out hold-and-wait deadlock.
- No `unwrap`/`expect` in library code; permit released on every exit (success/error/cancel).
- No change to eager-vs-lazy branch selection, KWayMerger, or the merge-error concatenation fallback.

## Decision 1 — where to acquire the permit
**Chosen: acquire inside the eager functions, at the top, before `spawn_blocking`, holding the RAII
guard across the join `.await`.**

```rust
// top of merge_generations_for_read (and _with_metadata), before line 255:
let _admission = scan_admission::admit().await;   // one permit for the whole op; RAII release
// ... existing spawn_blocking(KWayMerger drain) ... .await
```

Reachable via `super::reader::scan_stream_windowed::scan_admission::admit()` (the module path the
fan-out site already uses; `generation_merge.rs` already `use super::reader;`).

**Alternatives considered:**
- *Acquire at the `mod.rs` call sites* (like the fan-out merge does at `mod.rs:2226`): rejected — the
  eager function has 4 call sites (2 per variant) and no fan-out, so acquiring inside the function is
  one edit per variant instead of four, and keeps the permit lifetime co-located with the work it
  bounds. The windowed fan-out acquires at the call site only because the permit must span multiple
  `Exempt` sub-scan opens; the eager path has no sub-scans, so that reason does not apply.
- *Add a `ScanAdmission` parameter to the eager functions*: rejected — unnecessary indirection; the
  eager path is never itself a sub-scan of another admitted operation (it is always a top-level
  materialize), so it never needs an `Exempt` mode. (If a future caller ever nests it under an already
  admitted operation, that caller would need the parameter — noted as a latent extension, not built.)

## Decision 2 — deadlock-freedom argument (why one permit is safe here)
The #1594 deadlock required: N concurrent async sub-scans, `cap` winning permits and parking in
consumer backpressure while a priming merge waits on the remaining `N-cap` blocked-on-`admit()`
sub-scans → no permit ever frees. The eager path has **none of that topology**:
- It is a single `spawn_blocking` draining `KWayMerger::step()` **sequentially** — no N concurrent
  async sub-scans, no prime-then-drain, no call to `scan_stream_admitted`.
- KWayMerger's internal producers are plain `std::thread::spawn` OS threads each running a
  `current_thread` runtime (`write_engine/merge/mod.rs:437-447`, #2316) — they **never call
  `admit()`**, so there is no nested permit acquisition and no hold-and-wait.
- No cross-path cycle: neither a windowed operation nor an eager operation, while holding its permit,
  acquires a second permit or depends on *another* operation getting one. So sharing the semaphore
  across both paths preserves the #1594 deadlock-freedom invariant.

Therefore one top-level, once-only `admit()` with no nested acquire is deadlock-safe.

## Decision 3 — scope: ALL THREE eager helpers
**Chosen: admit all three.** Beyond `merge_generations_for_read`, both
`merge_generations_for_read_with_metadata` (the WRITETIME/TTL sibling) and
`seek_merge_generations_for_read` (the partition-SEEKING point read) have the identical
single-`spawn_blocking(KWayMerger)` shape and were equally unadmitted. Fixing only one would leave a
twin/triplet gap that silently defeats the bound on the metadata and point-read paths. All three get the
same operation-level acquire. `seek_merge_generations_for_read`'s only call site
(`SSTableManager::scan_partition_clustering`, `mod.rs:1650`) is a top-level manager operation that holds
no outer permit, so admitting it introduces no cross-path hold-and-wait (verified: no nested acquire).

## Decision 4 — cancellation: hold the permit until the detached blocking work terminates
Dropping a `spawn_blocking` `JoinHandle` DETACHES the closure — the KWayMerger producer OS threads keep
running — while a permit guard bound to the OUTER future would already be released, so repeated cancels
could exceed the bound. **Chosen: for the two PURE-blocking helpers (`merge_generations_for_read`,
`seek_merge_generations_for_read`) MOVE the `OwnedSemaphorePermit` INTO the `spawn_blocking` closure**
(`Send + 'static`), so the permit is released only when the detached blocking work actually finishes.
The METADATA helper cannot do this — its permit must span the async per-reader `scan_with_cell_metadata`
loop that runs OUTSIDE `spawn_blocking` — so it keeps the outer future guard. That is an honestly weaker
cancellation property (a cancelled metadata read releases immediately while a detached in-flight merge's
producer threads run permit-free), documented in the scope doc + Known-limitation rather than
over-engineered away.

## Known limitation (documented, not solved)
The shared semaphore bounds *operation concurrency*. The eager path's real resource footprint is
`M std::thread` producer threads per operation (not tokio blocking-pool threads like the windowed
path). So `cap` admitted eager operations can still spawn up to `cap × M` producer OS threads — the
bound limits how many eager *operations* run at once, not their aggregate thread count. This matches
#1594's stated semantic ("operation concurrency, not total blocking threads") and is called out in the
scope doc; sizing the eager path's thread footprint separately is explicitly a Non-goal / future issue.

Additionally, the METADATA helper (`merge_generations_for_read_with_metadata`) has a weaker CANCELLATION
property than the two pure-blocking helpers (Decision 4): its permit is an outer future guard, so on
cancellation it releases immediately while a detached in-flight merge's producer threads run permit-free.
Repeated mid-merge cancels of the metadata path can transiently exceed the bound. Documented, not solved
— the two-phase (async TTL scan + blocking merge) shape cannot move the permit into a single closure.

## Test / verification plan
- **Bound guard** (new `tests/issue_2063_eager_merge_admission_bound.rs`, `scan-offload-probe`-gated):
  drive N concurrent eager multi-gen scans (multi-generation fixture WITH a schema present so the
  `merge_generations_for_read` branch is taken — contrast the #1594 fan-out test which passes
  `schema=None` to force the lazy branch). Assert `max_in_flight <= LIMIT` (bound covers eager),
  `max_in_flight >= 1` (acquire is wired — non-vacuous), `current_in_flight == 0` after (RAII release
  across the join). Deterministic — asserts the safety bound and level snapshots, never wall-clock.
- **Deadlock-freedom flavor**: N > cap concurrent eager scans complete within a timeout (proves the
  shared semaphore doesn't hang when both paths contend). Trivially true given the single-spawn_blocking
  shape, but pins the no-cross-path-hold-and-wait claim.
- **Non-redundancy check**: confirm which branch the existing `issue_1594_scan_admission_bound.rs`
  fixture hits once the eager path is wired (it uses a schema-carrying `SELECT *`); if it now also
  exercises the eager path, ensure the new guard adds distinct coverage (explicit multi-gen + eager
  assertion).
- **Gate**: the new `scan-offload-probe` test runs in the same gate lane as the #1594 guard; full
  `agent-gate.sh` must PASS. C (spec-auditor) anchored to this change's specs. roborev clean.
