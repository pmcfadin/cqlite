# Per-query lock hygiene: QueryStats atomics, plan-cache hit path, LAST_ACCESS_PATH

## Why

Milestone: **v0.14 performance wave** (Epic F, #1518 — read-path audit, block 1).
Routing: **design-driven** — this is a concurrency/latency change with real design
latitude in how each shared cell is made lock-free while preserving observable
semantics. Grounded by the read-path performance audit
(`docs/reports/read-path-performance-audit-2026-07-01.md` §Epic F, row F5) and covered
by **standing owner Seam-1 approval (2026-07-06 drain directive)** for the audit's
locked decisions.

The audit found three shared cells taken on **every** query, contending across cores
and putting lock frames in the hot profile with no functional need:

1. **`QueryStats` write lock** — `QueryEngine` holds `Arc<RwLock<QueryStats>>` and takes
   a **write lock 2–3× per query** (`engine.rs`: `inc_total_queries`, `inc_error_queries`,
   `record_cache_hit`, `update_execution_stats`) purely to bump counters. Every query on
   every thread serializes on this one lock.
2. **Plan-cache hit path write lock** — a plan-cache HIT calls `DashMap::get_mut`
   (a shard **write** lock) just to bump `hit_count`, so concurrent hits to the same
   shard contend with each other even though they only read the cached plan.
3. **`LAST_ACCESS_PATH: Mutex`** — a process-global `Mutex<Option<AccessPath>>` written on
   **every** SELECT (`access_path.rs`), a process-wide serialization point (and a
   `.lock()` poisoning surface). The epic-#951 signal is worth keeping; the mutex is not.

## What changes

- **`QueryStats` → lock-free atomics.** Replace the engine's `Arc<RwLock<QueryStats>>`
  with a private struct of `AtomicU64` counters incremented with `Ordering::Relaxed`
  (counters need no cross-field ordering). Derived stats (`avg_execution_time_us`,
  `cache_hit_ratio`) are computed at read time in `stats()`, which returns the existing
  public `QueryStats` snapshot **unchanged in shape**. Exact counts are preserved.
- **Plan-cache hit path → shard READ lock + atomic bump.** Store `hit_count: AtomicU64`
  inside `QueryCacheEntry`; the hit path uses `DashMap::get` (shard **read** lock), bumps
  the counter with a relaxed `fetch_add`, clones the plan out, and **drops the guard
  before the `.await`** so no shard lock is held across execution. Concurrent hits no
  longer contend.
- **`LAST_ACCESS_PATH` → `arc_swap::ArcSwapOption`.** Replace the `Mutex` with
  `ArcSwapOption<AccessPath>`. The existing `record`/`last`/`reset` API and its
  cross-thread visibility (the streaming SELECT path records from a spawned task) are
  preserved; the `.lock()` poisoning branch disappears entirely.

## Non-goals

- **Schema-registry digest-path snapshot (audit F5 item 4).** The per-lookup
  `schema_registry.read().await` the audit flagged (`key_digest.rs`) survives **only in
  the `#[allow(dead_code)]` `compute_partition_key_digest`**, which issue #553 already
  removed from the live `lookup_partition_with_index` path (the live
  `lookup_partition_with_schema_context` takes a resolved `&ParsingContext` parameter and
  holds no registry lock). There is no per-lookup registry lock left on the hot path, so
  this item is already satisfied. The `Arc`-snapshot-at-open work is E5's resolve-once
  territory (#1587) — coordinated there, **not duplicated here**.
- **`CachePadded` on A5 work counters (audit F5 item 5).** Explicitly "measure first" in
  the audit; no false-sharing profile evidence is presented, so it is skipped.
- **Changing the public `QueryStats` field set or the `AccessPath` API.** Shapes are held
  stable; only the internal storage becomes lock-free.
- **The scan-side reader-map/streaming locks (F1/F2/F3/F4).** Separate F-epic children.

## Doctrine impact

- Reinforces the **no `.lock().unwrap()` / handle poisoning** rule — removing the last
  `Mutex` on the SELECT hot path deletes a poisoning surface rather than papering over it.
- No public CLI/binding surface change (the `QueryStats` snapshot and `AccessPath` API are
  unchanged), so no `agents-developing/` site change is required.
