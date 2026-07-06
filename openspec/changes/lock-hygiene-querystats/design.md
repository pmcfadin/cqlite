# Design — per-query lock hygiene

## Context

`QueryEngine::execute` is the per-query hot path. Three shared cells are touched on every
query for bookkeeping/signaling only, each serializing all queries across all cores:

- `stats: Arc<parking_lot::RwLock<QueryStats>>` — write-locked 2–3×/query.
- `plan_cache: DashMap<String, QueryCacheEntry>` — HIT path uses `get_mut` (shard write
  lock) to bump `hit_count`.
- `access_path::LAST_ACCESS_PATH: Mutex<Option<AccessPath>>` — written on every SELECT.

The audit's locked decision (F5) is: counters → relaxed atomics; plan-cache hit → shard
read lock + atomic; `LAST_ACCESS_PATH` → `ArcSwapOption`; preserve every observable
surface.

## Decision 1 — QueryStats becomes a struct of relaxed atomics

Keep the public `QueryStats` (in `query/mod.rs`) exactly as-is — it is the **read
snapshot** returned by `stats()`, consumed by `db.stats()` tests
(`total_queries`, `error_queries`, `cache_hit_ratio`, `avg_execution_time_us`,
`rows_affected`). Introduce a private `AtomicQueryStats` (new `query/engine_stats.rs`, to
avoid growing the already-over-threshold `engine.rs`) holding:

- `total_queries: AtomicU64`, `error_queries: AtomicU64`, `cache_hits: AtomicU64`,
  `rows_affected: AtomicU64`, `exec_time_us_sum: AtomicU64`.

Methods (`&self`, no lock): `record_query`, `record_error`, `record_cache_hit`,
`record_execution(us, rows)`, and `snapshot() -> QueryStats`.

**Ordering: `Relaxed` for every counter.** Counters are independent totals; there is no
happens-before relationship a reader depends on between two different counters, and the
snapshot is inherently a racy point-in-time read (as it was under the old lock — the lock
only guaranteed a consistent read of the *derived* averages, not linearizability with the
query stream). Relaxed `fetch_add` is exact under concurrency (no lost updates), which is
the only invariant the audit requires ("counts must remain correct").

**Derived stats at read time** (`snapshot`): `avg_execution_time_us = exec_time_us_sum /
total_queries` (0 when no queries), `cache_hit_ratio = cache_hits / total_queries`
(0.0 when no queries). This is a cleaner, monotone definition than the old incremental
running-mean (which drifted because it divided by `total_queries` while only updating on
recorded events); the public field shapes and ranges (ratio in `[0,1]`) are unchanged, and
the only stats test that pins a value asserts `cache_hit_ratio > 0.0` after a hit, which
holds.

`QueryEngine.stats` becomes a plain `AtomicQueryStats` field (atomics are `Sync`; no `Arc`
/ `RwLock` wrapper needed). `parking_lot` is dropped from `engine.rs`.

## Decision 2 — plan-cache hit path takes a shard read lock

Change `QueryCacheEntry.hit_count: u64` → `AtomicU64`. Because `AtomicU64` is not `Clone`,
implement `Clone` for `QueryCacheEntry` by hand (load the counter `Relaxed`) so the public
derive-equivalent surface is preserved.

Hit path (both `execute` non-SELECT and `execute_select_query`): use `DashMap::get`
(shard **read** lock). Inside the guard scope, `hit_count.fetch_add(1, Relaxed)` and clone
the plan out; the guard is dropped at the end of the `match`/expression, **before** the
`self.executor.execute(&plan).await`. This is strictly better than the old code, which held
a `get_mut` **write** guard across the await (a lock-across-await footgun *and* a
serialization point): concurrent hits now share the read lock and hold nothing across the
await. Cloning the plan per hit is the deliberate tradeoff and is negligible next to query
execution.

For `execute_select_query`, the placeholder-plan case (no `table`) must still `remove` the
entry; resolve the three outcomes (reusable / placeholder / miss) inside the guard scope
into a small local enum, drop the guard, then act — so `remove` never runs while a `get`
guard on the same shard is held.

## Decision 3 — LAST_ACCESS_PATH becomes ArcSwapOption

`static LAST_ACCESS_PATH: ArcSwapOption<AccessPath> = ArcSwapOption::const_empty();`
(`const_empty()` is a const fn, valid in a `static`). API unchanged:

- `record(path)` → `store(Some(Arc::new(path)))`
- `last()` → `load_full().map(|a| (*a).clone())`
- `reset()` → `store(None)`

This preserves cross-thread visibility (the streaming SELECT path records from a spawned
task and an integration test reads it from another thread) and **removes the `.lock()`
poisoning branch entirely** — no `unwrap`/poisoning handling remains. `arc-swap` is a tiny,
zero-dependency, widely-used crate added to `[workspace.dependencies]` and `cqlite-core`.

## Alternatives considered

- **`crossbeam_utils::atomic::AtomicCell<Option<AccessPath>>`** for `LAST_ACCESS_PATH`:
  rejected — `AccessPath` (with a data-carrying variant) exceeds native atomic width, so
  `AtomicCell` falls back to an internal seqlock; not truly lock-free and less clear than
  the audit-named `ArcSwapOption`.
- **Hold the plan-cache read guard across the await** (minimal swap of `get_mut`→`get`):
  rejected — keeps a lock across `.await`; clone-and-drop is both correct and faster.
- **`unsafe` `AtomicPtr`** for the access-path cell: rejected — the audit guardrail forbids
  `unsafe`, and `ArcSwapOption` gives the same lock-free swap safely.

## Risks / mitigations

- **Derived-stat value change** (avg/ratio formula): mitigated — public field shapes and
  ranges are unchanged; the only pinned assertion (`cache_hit_ratio > 0.0`) still holds;
  exact counts are preserved and covered by a new concurrent-exactness test.
- **Concurrency test flakiness** (wall-clock races): the correctness test asserts only
  counter *sums* against the number of issued operations (no timing windows), so it cannot
  flake on clock boundaries.
