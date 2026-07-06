# Tasks — per-query lock hygiene (F5)

## 1. QueryStats → lock-free atomics

- [ ] 1.1 Add `query/engine_stats.rs` with a private `AtomicQueryStats` (fields:
  `total_queries`, `error_queries`, `cache_hits`, `rows_affected`, `exec_time_us_sum` —
  all `AtomicU64`). Surfaces: `record_query`, `record_error`, `record_cache_hit`,
  `record_execution(us, rows)`, `snapshot() -> QueryStats`. All `Relaxed`.
- [ ] 1.2 Declare `mod engine_stats;` (crate-private) in `query/mod.rs`.
- [ ] 1.3 In `engine.rs`, replace `stats: Arc<parking_lot::RwLock<QueryStats>>` with
  `stats: AtomicQueryStats`; rewrite `inc_total_queries`/`inc_error_queries`/
  `record_cache_hit`/`update_execution_stats`/`stats()` to the atomic surfaces; drop the
  `parking_lot` usage in `engine.rs`.
- [ ] 1.4 Unit test in `engine_stats.rs`: N threads record known event counts →
  `snapshot()` totals equal the issued counts (sum-only asserts, no wall-clock window).
- [ ] 1.5 Unit test: `snapshot()` derived-value shapes (`cache_hit_ratio` in `[0,1]`,
  `> 0.0` after a hit; `avg = sum/total`, 0 when empty).

## 2. Plan-cache hit path → shard read lock + atomic

- [ ] 2.1 `QueryCacheEntry.hit_count: u64` → `AtomicU64`; hand-write `Clone` (load
  `Relaxed`) so the public surface keeps `Clone`.
- [ ] 2.2 `execute` non-SELECT hit path: `DashMap::get` + `fetch_add(1, Relaxed)` + clone
  plan out; drop guard before `.await`.
- [ ] 2.3 `execute_select_query` hit path: resolve reusable/placeholder/miss inside the
  guard scope into a local enum, drop guard, then execute or `remove`.

## 3. LAST_ACCESS_PATH → ArcSwapOption

- [ ] 3.1 Add `arc-swap` to `[workspace.dependencies]` and `cqlite-core/Cargo.toml`.
- [ ] 3.2 In `access_path.rs`, replace `Mutex<Option<AccessPath>>` with
  `ArcSwapOption<AccessPath>` (`const_empty()`); rewrite `record`/`last`/`reset`; remove
  the poisoning branch. Update the module/`static` doc comments.
- [ ] 3.3 Confirm existing `probe_round_trips` test and the tests-dir #951/#960/#962
  access-path tests stay green.

## 4. Gate + review + validate

- [ ] 4.1 `openspec validate lock-hygiene-querystats --strict` clean.
- [ ] 4.2 `cargo +1.88.0 fmt` and `cargo +1.88.0 fmt --check` clean.
- [ ] 4.3 FAST iteration gate (`scripts/agent-gate.sh --lite`) → RESULT: PASS each fix
  round (never the full gate — the orchestrator runs it serially).
- [ ] 4.4 Pre-roborev self-check: atomic ordering correctness (Relaxed, justified); no
  `.lock().unwrap()`; no wall-clock races in the concurrency test; no
  `manual_range_contains`; minimal-feature (`--no-default-features --features
  all-compression`) compiles.
- [ ] 4.5 Intent audit **C** (`spec-auditor`) PASS against
  `specs/query-engine/spec.md` after the gate is green.
- [ ] 4.6 roborev clean (`--base origin/main`).
