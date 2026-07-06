# Tasks — dead-cache-delete (issue #1568, Epic B / B2)

> Depends on B1 (#1567, landed via PR #1831) — it defines the `DecompressedChunkCache`
> that backs the stats and the budget knob. Write the TDD tests RED against current `main`
> first (paste the red run in the PR), then implement.

## 1. TDD tests first (write RED, paste red run in PR)
- [ ] 1.1 Config knob test (public `Config` surface): open with a tiny `block_cache.max_size`
      → assert the live B1 cache's `budget_bytes()` == the configured value AND it evicts
      under the small budget (`resident_bytes()` stays within budget while a fixture is
      scanned). Fails today (knob wired to nothing).
- [ ] 1.2 Config compat test (public `Config` surface): a config naming `row_cache` /
      `query_cache` / `allocator` / `CachePolicy::{Lfu,Arc}` fails closed
      (deserialize error or `Config::validate()` rejection); a config with only
      `max_memory` + `block_cache` deserializes and validates.
- [ ] 1.3 Stats test (public `Database::stats()` surface, real fixture, `CQLITE_DATASETS_ROOT`):
      open a multi-chunk fixture, issue the identical read twice → assert
      `Database::stats().memory_stats.block_cache_hit_rate() > 0.0` after the repeat read.
      Fails today (structural `0.0`). Skip-not-fail when the fixture is absent; never a
      silent 0-row pass.
- [ ] 1.4 Semver-shape test (public surface): `Database::stats().memory_stats` is a
      `MemoryStats` with the expected public field names/types and the `block_cache_hit_rate()`
      accessor; `MemoryManager::stats()` keeps `-> Result<MemoryStats>`.

## 2. Delete the per-reader dead block cache
- [ ] 2.1 Remove `SSTableReader.block_cache`, `block_meta_cache`, `CachedBlock`,
      `cache_hits`, `cache_misses` (`reader/types.rs:220,270-287`) and their init/clear
      sites (`reader/mod.rs:766,1186-1189`).
- [ ] 2.2 Remove `record_cache_hit` / `record_cache_miss` / `get_cache_stats` and the
      always-empty-map summation in `estimate_memory_usage` (`reader/cache.rs:22-52`);
      remove the `record_cache_hit()` / `record_cache_miss()` call sites
      (`data_access/mod.rs:538-541`) and adjust `integrity.rs:30`'s use of
      `estimate_memory_usage`.
- [ ] 2.3 `rg` sweep every read of each removed field → confirm no production consumer;
      build + lint clean under `RUSTFLAGS="-D warnings"` with no retained
      `#[allow(dead_code)]` for these members.

## 3. Delete the MemoryManager dead cache core; keep the stats() shell (public surface: `MemoryManager::stats()` / `Database::stats().memory_stats`)
- [ ] 3.1 Delete the `MemoryManager` block cache / row cache / buffer pool internals and
      `clear_caches` (`memory/mod.rs`) — except any component B1 adopted as its backing
      store (confirm against what B1 shipped).
- [ ] 3.2 Preserve `MemoryManager::stats()` (`memory/mod.rs:295`) and the `MemoryStats`
      shape (`memory/mod.rs:435`) shape-compatibly; keep `Database::stats().memory_stats`
      reachability (`lib.rs:597-600,676`) unchanged.

## 4. Bridge stats() to B1's real numbers (public surface: `Database::stats()`)
- [ ] 4.1 Thread the live `DecompressedChunkCache` (owned by the storage engine) to the
      stats surface so `MemoryStats` block-cache hits/misses/occupancy come from
      `hit_count()` / `miss_count()` / `resident_bytes()`.
- [ ] 4.2 Ensure `block_cache_hit_rate()` is non-zero after a repeated cached read
      (satisfies test 1.3); retained-but-unbacked sub-fields report a fixed `0` (full
      surface is B5).

## 5. Collapse the config to one real knob (public surface: `Config` / `MemoryConfig`)
- [ ] 5.1 Delete `MemoryConfig.row_cache`, `query_cache`, `allocator` and the
      `CachePolicy::{Lfu,Arc}` variants (`config.rs:271-348`); remove them from
      `Config::validate()`. Make removed-knob configs fail closed (test 1.2).
- [ ] 5.2 Wire `block_cache.max_size` as the B1 cache's byte budget at construction so
      `budget_bytes()` == the configured value (test 1.1).
- [ ] 5.3 Changelog: breaking config-schema note for the removed knobs.

## 6. Validation
- [ ] 6.1 33-table parity + smoke green (`env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets`)
      — byte-for-byte unchanged (spec: "changes no read result").
- [ ] 6.2 Minimal-features build passes:
      `cargo build --package cqlite-core --no-default-features --features all-compression`.
- [ ] 6.3 `RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets` clean (sibling
      crates too — public-surface change).
- [ ] 6.4 Run `scripts/agent-gate.sh` — PASS; paste the AGENT-GATE SUMMARY block verbatim.
- [ ] 6.5 `spec-auditor` (C) PASS against `openspec/changes/dead-cache-delete/specs/**`
      (every requirement satisfied with a public-surface test as evidence).
- [ ] 6.6 `roborev review --branch --base origin/main` clean before merge.
