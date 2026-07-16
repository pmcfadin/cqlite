# scan-admission

## ADDED Requirements

### Requirement: The eager multi-generation merge path acquires a scan-admission permit

The eager path SHALL acquire exactly one operation-level `ScanAdmissionPermit` from the same
process-wide admission semaphore the windowed/lazy scan path uses (#1594), covering both
`merge_generations_for_read` and `merge_generations_for_read_with_metadata`
(`cqlite-core/src/storage/sstable/generation_merge.rs`). The permit SHALL be acquired before the
`KWayMerger` `spawn_blocking` work and held for the full operation (across the join `.await`), and
SHALL be released on every exit path — success, merge error, and cancellation — via the RAII `Drop` on
`ScanAdmissionPermit`. Acquisition SHALL be a single, top-level, once-only `admit()` with no nested
permit acquisition inside the merge (the KWayMerger's producer threads do not acquire admission
permits).

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

### Requirement: The admission scope documentation reflects eager-path coverage

The `# Scope` doc comment on the admission module SHALL state that the eager multi-generation merge
path is admitted (no longer carved out) and SHALL NOT contain the stale claim that the eager path is
out of scope. The doc lives on `cqlite-core/src/storage/sstable/reader/scan_admission.rs`. Any
file-location reference in that doc SHALL point at the eager path's actual location
(`generation_merge.rs`), not the pre-#1116 `storage/sstable/mod.rs`.

#### Scenario: The scope doc no longer excludes the eager path

- **WHEN** the `scan_admission` module `# Scope` doc comment is read
- **THEN** it does not state that `merge_generations_for_read` is outside admission coverage
- **AND** it correctly locates the eager path in `generation_merge.rs`
- **AND** it documents the known limitation that the shared bound limits eager *operation* concurrency,
  not the eager path's per-operation producer-thread footprint.
