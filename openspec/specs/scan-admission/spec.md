# scan-admission Specification

## Purpose
TBD - created by archiving change eager-merge-admission. Update Purpose after archive.
## Requirements
### Requirement: The eager multi-generation merge path acquires a scan-admission permit

Each of the THREE eager multi-generation merge helpers SHALL acquire exactly one operation-level
`ScanAdmissionPermit` from the same process-wide admission semaphore the windowed/lazy scan path uses
(#1594). The three helpers, in `cqlite-core/src/storage/sstable/generation_merge.rs`, are
`merge_generations_for_read` (the materializing plain read), `seek_merge_generations_for_read` (the
partition-seeking point read), and `merge_generations_for_read_with_metadata` (the WRITETIME/TTL
sibling). The permit SHALL be acquired before the `KWayMerger` `spawn_blocking` work and
held for the full operation (across the join `.await`), and SHALL be released on every exit path —
success, merge error, and cancellation — via the RAII `Drop` on `ScanAdmissionPermit`. Acquisition SHALL
be a single, top-level, once-only `admit()` with no nested permit acquisition inside the merge (the
KWayMerger's producer threads do not acquire admission permits), and the seek helper's sole call site
SHALL be a top-level manager operation that holds no outer permit (no cross-path hold-and-wait).

#### Scenario: Concurrent eager multi-gen scans are bounded by the admission limit

- **WHEN** the admission limit is set to `LIMIT` (via the `scan-offload-probe` test override) and
  `N > LIMIT` concurrent reads are issued against a multi-generation table WITH a schema present, so
  each takes the eager `merge_generations_for_read` branch
- **THEN** the observed maximum number of simultaneously-admitted operations (`max_in_flight` from the
  probe instrumentation) is `<= LIMIT` — the eager path is now covered by the operation-concurrency
  bound
- **AND** `max_in_flight >= 1` — the acquire is actually wired (the assertion is non-vacuous; a
  never-acquiring path would leave `max_in_flight == 0`)
- **AND** after all reads complete, `current_in_flight == 0` — every permit was released across the
  `spawn_blocking` join (no leak on the success path).

#### Scenario: The metadata sibling is bounded end-to-end

- **WHEN** `N > LIMIT` concurrent `scan_with_cell_metadata` reads (WRITETIME/TTL projection) are issued
  against the multi-generation + schema fixture, so each routes through
  `merge_generations_for_read_with_metadata`
- **THEN** `max_in_flight >= 1` (the metadata acquire is wired, non-vacuous), `max_in_flight <= LIMIT`
  (the bound covers the metadata path), and `current_in_flight == 0` after (RAII release).

#### Scenario: A permit is released when the eager merge errors and falls back

- **WHEN** an eager multi-gen merge acquires a permit and its `KWayMerger` drain returns an error
  (triggering the existing per-reader-concatenation fallback at the call site)
- **THEN** the `ScanAdmissionPermit` is dropped as the merge function returns the error, releasing the
  permit before the fallback path runs
- **AND** the probe `current_in_flight` returns to `0` after the operation, so an erroring eager merge
  does not strand a permit.

### Requirement: Sharing the admission semaphore across eager and windowed paths is deadlock-free

Admitting the eager path under the SAME semaphore as the windowed/lazy path SHALL NOT reintroduce the
#1594 fan-out hold-and-wait deadlock. Because the eager path is a single `spawn_blocking` draining a
`KWayMerger` sequentially — with no concurrent async sub-scans, no prime-then-drain, and no nested
`admit()` call — a set of concurrent eager operations exceeding the admission limit SHALL all complete
(each acquiring its one permit in turn), never deadlock.

#### Scenario: More concurrent eager scans than the limit all complete without hanging

- **WHEN** the admission limit is set to `CAP` and `N > CAP` concurrent eager multi-gen scans (schema
  present, multiple generations) are driven at once
- **THEN** all `N` scans complete within a bounded timeout (pre-change there was no bound; the guard
  proves the shared semaphore does not hang under contention)
- **AND** the total rows returned across the scans is `> 0` (the scans did real work, not a vacuous
  early return)
- **AND** the observed `max_in_flight <= CAP` throughout (the bound held while all N eventually
  completed).

### Requirement: All eager helpers hold the permit until the detached blocking work ends

Every eager helper SHALL hold the `ScanAdmissionPermit` until the detached `spawn_blocking` merge work
actually terminates, so that on cancellation (the awaiting future dropped) the admission slot is not
released while the KWayMerger producer threads keep running. The two pure-blocking helpers
(`merge_generations_for_read`, `seek_merge_generations_for_read`) SHALL move the permit INTO the
`spawn_blocking` closure directly. The metadata helper `merge_generations_for_read_with_metadata` SHALL
hold the permit as an outer future guard across its async per-reader `scan_with_cell_metadata` loop —
where cancellation is clean because no detached blocking work exists yet, so early release is harmless —
and SHALL THEN move the permit into its `spawn_blocking` merge closure for the detached phase. No phase
both runs detached blocking work AND has already released the permit; the three helpers are uniformly
cancellation-safe.

#### Scenario: Every eager helper's permit is held into the blocking closure, not released before it ends

- **WHEN** the source of `merge_generations_for_read` / `seek_merge_generations_for_read` /
  `merge_generations_for_read_with_metadata` is inspected
- **THEN** the `ScanAdmissionPermit` is moved into each helper's `spawn_blocking` closure (bound to a
  `let` inside it), so a dropped `JoinHandle` cannot release the permit before the detached blocking
  work ends
- **AND** the metadata helper additionally holds the permit as an outer future guard across its async
  per-reader loop before moving it into the closure, and the module scope doc states that all three
  helpers are uniformly cancellation-safe.

### Requirement: The admission scope documentation reflects eager-path coverage

The `# Scope` doc comment on the admission module SHALL state that the eager multi-generation merge
path is admitted (no longer carved out) and SHALL NOT contain the stale claim that the eager path is
out of scope. The doc lives on `cqlite-core/src/storage/sstable/reader/scan_admission.rs`. Any
file-location reference in that doc SHALL point at the eager path's actual location
(`generation_merge.rs`), not the pre-#1116 `storage/sstable/mod.rs`.

#### Scenario: The scope doc no longer excludes the eager path

- **WHEN** the `scan_admission` module `# Scope` doc comment is read
- **THEN** it does not state that `merge_generations_for_read` is outside admission coverage
- **AND** it names all three eager helpers (`merge_generations_for_read`,
  `seek_merge_generations_for_read`, `merge_generations_for_read_with_metadata`) as admitted
- **AND** it correctly locates the eager path in `generation_merge.rs`
- **AND** it documents the known limitation that the shared bound limits eager *operation* concurrency,
  not the eager path's per-operation producer-thread footprint, and states that all three eager helpers
  are uniformly cancellation-safe.

