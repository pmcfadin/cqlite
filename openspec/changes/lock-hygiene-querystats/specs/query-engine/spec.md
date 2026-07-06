# query-engine

## ADDED Requirements

### Requirement: Lock-free query statistics

The query engine SHALL maintain per-query statistics using lock-free atomic counters
rather than a shared read/write lock. Incrementing any counter SHALL NOT take a lock, and
concurrent queries SHALL produce exact counter totals (no lost updates). The public
`QueryStats` snapshot returned by `stats()` SHALL keep its existing field set and shapes;
derived values (`avg_execution_time_us`, `cache_hit_ratio`) SHALL be computed at read time
from the atomic counters.

#### Scenario: Concurrent queries produce exact counter totals

- **GIVEN** the engine's atomic statistics
- **WHEN** N threads each record a known number of query, error, cache-hit, and execution
  events concurrently
- **THEN** the resulting `stats()` snapshot reports `total_queries`, `error_queries`, and
  `rows_affected` exactly equal to the summed number of issued events (no lost updates)
- **AND** the assertion compares only counter sums against the issued counts, using no
  wall-clock window, so it cannot flake on a timing boundary.

#### Scenario: QueryStats snapshot keeps its public shape and derived values

- **GIVEN** an engine that has recorded some queries including at least one cache hit and
  one execution with a non-zero duration
- **WHEN** `stats()` is called
- **THEN** the returned `QueryStats` has the same fields as before (`total_queries`,
  `error_queries`, `avg_execution_time_us`, `cache_hit_ratio`, `rows_affected`)
- **AND** `cache_hit_ratio` is in `[0.0, 1.0]` and is `> 0.0` after a cache hit
- **AND** `avg_execution_time_us` is `exec_time_us_sum / total_queries` (0 when no
  queries were recorded).

#### Scenario: Statistics updates take no lock

- **GIVEN** the query engine
- **WHEN** a query increments any statistic (total, error, cache hit, or execution
  time/rows)
- **THEN** the increment is a relaxed atomic operation and no `RwLock`/`Mutex` is acquired
- **AND** `engine.rs` no longer depends on `parking_lot` for statistics.

### Requirement: Plan-cache hit path uses a shared (read) lock

A plan-cache HIT SHALL be served under a shard READ lock (`DashMap::get`), not a shard
WRITE lock (`DashMap::get_mut`). The per-entry hit counter SHALL be an atomic bumped with a
relaxed `fetch_add`, and no shard lock SHALL be held across the query's `.await`.

#### Scenario: Cache hit bumps the counter without a write lock

- **GIVEN** a query whose plan is already cached
- **WHEN** the query is executed and hits the plan cache
- **THEN** the hit is served via `DashMap::get` (shared read lock), the entry's atomic
  `hit_count` is incremented with a relaxed `fetch_add`, and the shard lock is released
  before executing the plan
- **AND** the query returns the same result as a cache miss for the same plan.

#### Scenario: Concurrent hits to the same cached plan do not serialize on a write lock

- **GIVEN** a single cached plan
- **WHEN** multiple queries hit that plan concurrently
- **THEN** they share the shard read lock rather than contending for a shard write lock
- **AND** the entry's `hit_count` reflects every hit exactly (no lost increments).

#### Scenario: A non-reusable placeholder plan is still evicted

- **GIVEN** a cached entry whose plan has no resolved table (a placeholder)
- **WHEN** a SELECT for that key is executed
- **THEN** the entry is removed after the read guard is dropped (never removed while a
  `get` guard on the same shard is held) and the query proceeds via the cold path.

### Requirement: Access-path signal is recorded without a mutex

The process-global "last access path" signal (epic #951) SHALL be stored lock-free via
`arc_swap::ArcSwapOption`, preserving the existing `record`/`last`/`reset` API and its
cross-thread visibility. No `Mutex` and no lock-poisoning handling SHALL remain on this
path.

#### Scenario: record/last/reset round-trip preserved

- **GIVEN** the access-path probe
- **WHEN** `reset()` then `record(path)` then `last()` are called
- **THEN** `last()` returns `None` after `reset()` and `Some(path)` after `record(path)`,
  identical to the prior mutex-backed behavior.

#### Scenario: Signal is visible across threads with no poisoning surface

- **GIVEN** the streaming SELECT path records the access path from a spawned task on a
  different thread than the reader
- **WHEN** a value is recorded on one thread and read on another
- **THEN** the reader observes the recorded value (cross-thread visibility preserved)
- **AND** the implementation contains no `.lock()` and no poisoning/`unwrap` branch.

#### Scenario: Existing epic-#951 access-path tests stay green

- **GIVEN** the existing #951/#960/#962 access-path signal tests
- **WHEN** they run against the `ArcSwapOption`-backed probe
- **THEN** they pass unchanged (the observable signal is identical).
