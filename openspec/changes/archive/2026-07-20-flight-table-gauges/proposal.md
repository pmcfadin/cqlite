# Flight table-visibility gauges — tables_discovered + warm_tables

## Milestone
0.16 observability thread (from #2419 saturation gauge family, #2661 soak follow-through). Owner-scoped
2026-07-17. Design-driven (lightweight). Scope is **cqlite-flight** only. No parity oracle.

## Why (operator problem)
Operators have no first-class signal for "what does this cqlite-flight pod actually see?" A pod with a
wrong `--data-dir` mount, bad permissions, or an empty volume fails **lazily** — the first query errors —
instead of visibly at startup. And there is no live view of the warm working set (tables actually opened
and served), even though `WarmTableRegistry` already tracks it internally.

## What changes — two TRUE gauges (bidirectional; values rise AND fall)

1. **`cqlite.flight.tables_discovered`** — the number of `<keyspace>/<table>` SSTable directories currently
   visible under `--data-dir`. Re-sampled on the existing ~2s saturation sampler tick (#2419): a directory
   walk (readdir) only, **zero SSTable I/O, zero opens** (cold-start invariant #2385 preserved). RISES when
   new tables appear on disk (new table, mount fixed), FALLS when tables are dropped/removed. A
   wrong/empty mount reads **0** immediately.

2. **`cqlite.flight.warm_tables`** — the current size of the `WarmTableRegistry` (tables with a live warm
   reader set). Atomic-backed like `blocking_tasks_in_use`, emitting the post-mutation `tables.len()` at the
   registry's insert/evict sites (independent of sampler cadence). RISES on first open of a previously-unseen
   table, FALLS on evict/retire/generation-turnover removal.

Plus one **startup / first-sample `info` log line**: `discovered N tables across M keyspaces under
<data-dir>` — makes the inert-mount failure mode visible in logs even without a metrics stack (same spirit
as the #2128 OTel-inert warn).

### Design calls resolved (see design.md)
- **Labels:** both gauges are **total-only** (`&[]`), matching every existing #2419 saturation gauge (all
  emit with no attributes; catalog docs say "no high-cardinality attributes"). The per-keyspace breakdown
  lives in the one-time startup log line, not as a metric label — a per-keyspace gauge dimension would be
  unbounded-ish (keyspace cardinality) and is explicitly out of scope.
- **Discovery = sampler-driven** for `tables_discovered` (the walk is cheap readdir on the 2s tick);
  **atomic-backed at call sites** for `warm_tables` (the registry already mutates under a lock, so emitting
  `.len()` post-mutation is exact and cadence-independent).
- **What counts as a "table dir":** a `<keyspace>/<table[-uuid]>` directory that directly contains at least
  one `*-Data.db` file (matches the existing `DirSource`/`enumerate_generations` prior art), correct on
  UUID-suffixed dirs, **excluding `snapshots/` and `backups/` subtrees**. Structural (directory layout)
  only — never inferred from file contents (no-heuristics).
- **Readdir-only:** the walk lists directory entries and checks for a `*-Data.db` *name*; it does NOT stat
  generations, open, or parse any SSTable (so `index_parses` sees zero delta from sampling).

## Non-goals
- No per-keyspace metric label dimension (startup log carries the breakdown).
- No new alerting/dashboard rules — the gauges are exported through the same OTLP pipeline as the #2419
  family and become visible in the standing dashboard next field round; the 2s-vs-scrape-interval
  undersampling caveat (#2661) is documented, not fixed here.
- No change to query behavior, the warm eviction policy, or the connector.

## Doctrine impact
- **No-heuristics (#28):** discovery is structural (directory layout: a dir containing `*-Data.db`,
  excluding snapshots/backups) — never inferred from file contents.
- **Cold-start invariant (#2385):** the discovery walk does zero SSTable opens/parses; a test asserts a
  zero `index_parses` delta across sampler ticks.
- **Bounded-cardinality metrics:** both gauges are total-only; no new bounded-attr allowlist entry needed.
- **Wiring-evidence:** `warm_tables` is proven through the public Flight surface — a real `do_get` on a
  previously-unseen table increments it, and eviction/retirement decrements it.
