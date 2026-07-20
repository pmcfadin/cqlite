# Design — Flight table-visibility gauges

## Two gauges, two emission mechanisms (both already precedented in the crate)

### `cqlite.flight.tables_discovered` — sampler-driven observable gauge
Hook: extend the ~2s saturation sampler (`cqlite-flight/src/saturation.rs`, `DEFAULT_SAMPLE_INTERVAL =
2s`, `sample_once()` / `run_sampler()`). Today the sampler reads only `/proc/self/*`; it must gain a
readdir walk of `--data-dir`. `data_dir` is NOT currently threaded to the sampler (it is spawned in
`main.rs` separately from the service), so `run_sampler` gains a `data_dir: PathBuf` parameter, passed at
the `main.rs` spawn site.

The walk each tick:
- `read_dir(data_dir)` → each entry that is a directory is a candidate keyspace.
- Within each keyspace dir, `read_dir` → each entry that is a directory whose name is NOT `snapshots` or
  `backups` is a candidate table dir (covers `<table>` and `<table>-<uuid>` — matches the `DirSource`
  prefix logic).
- A candidate table dir counts iff it **directly contains** a `*-Data.db` entry (readdir name check only —
  NO stat, NO open, NO generation parse). This keeps the cold-start invariant (#2385): zero SSTable I/O.
- Emit `obs::record_gauge(catalog::FLIGHT_TABLES_DISCOVERED, count as i64, &[])` and, for the startup line,
  also compute the keyspace count.

Bidirectionality is intrinsic: the count is recomputed from disk each tick, so a removed table dir lowers
it on the next sample; a new one raises it.

Testability: the in-crate saturation tests already drive `sample_once`/`run_sampler` directly against a
temp dir; a fixture-dir test pins the exact count (including a UUID-suffixed dir + a `snapshots/`/`backups/`
subdir that must be excluded), then removes a table dir and asserts the next sample falls.

Cold-start assertion: with the OTel capture harness, snapshot `counter_sum(INDEX_PARSES_TOTAL)` before/after
N sampler ticks over a populated data-dir and assert **zero delta** (`INDEX_PARSES_TOTAL` is incremented at
exactly one site — a full Index.db parse — so any open would show up).

### `cqlite.flight.warm_tables` — atomic-backed gauge at the registry
Hook: `WarmTableRegistry` (`cqlite-flight/src/warm/registry.rs`), whose `Inner.tables:
HashMap<TableKey, TableWarm>` size IS the metric. Mutation sites:
- `rebuild()` insert (`registry.rs:597`) — the only insert; note it does a remove-then-reinsert of the same
  key earlier (`registry.rs:519`), so a naive inc-on-insert/dec-on-remove would transiently dip.
- `evict_to_budget()` removal (`registry.rs:659`) — the true LRU/generation-turnover removal.

Chosen approach: emit the **post-mutation `inner.tables.len()`** (while the `Inner` lock is held) at the end
of `rebuild` and `evict_to_budget` via `obs::record_gauge(catalog::FLIGHT_WARM_TABLES, len as i64, &[])`.
This avoids the remove-then-reinsert dip and is always exact. Also expose a feature-independent level reader
`warm_table_count() -> i64` (mirroring `saturation::blocking_tasks_in_use_level()`) so up/down tests can
read the current value without an OTel stack.

Bidirectionality: a first `do_get` on a new table triggers `rebuild` insert → count rises; eviction/retire
triggers `evict_to_budget` removal → count falls. The only production caller of `warm_readers` is
`do_get_resolve`'s row/point route (`service.rs`), so the public-surface test drives a real `do_get`.

## Three-layer metric registration (each gauge)
Enforced by existing `catalog.rs` tests, so all three are mandatory:
1. `cqlite-core/src/observability/catalog.rs`: add the `&'static str` constant + a `unit` (use
   `unit::ENTRIES` = `"{entry}"`, or a new `{table}` unit — pick `ENTRIES` to avoid a new unit), add to
   `ALL_METRICS`, and add to `SATURATION_GAUGES` (so the namespaced/unique + dedicated-otel-arm tests cover
   them).
2. `cqlite-core/src/observability/otel.rs`: add a `Gauge<i64>` field to `Instruments`, build it with
   `.i64_gauge(catalog::NAME).with_unit(...).with_description(...)`, and add a dedicated `record_gauge`
   match arm mapping the name → field (the `saturation_gauges_have_dedicated_otel_arms_not_the_adhoc_fallback`
   test source-scans for `catalog::IDENT =>`, so the ad-hoc `_ =>` fallback is disallowed).
3. Emit via `cqlite_core::observability::record_gauge` (no-op when the `observability` feature is off).

## Startup log line
Emit one `info`: `discovered N tables across M keyspaces under <data-dir>` after the first sampler sample.
Precedent: `main.rs` "cqlite-flight starting" info line, and `log_platform_support_once()` (a `Once`-guarded
single info line inside the sampler) — co-locate the "discovered N tables" line with the sampler using the
same `Once` pattern so it fires exactly once after the first walk. Mirrors the #2128 OTel-inert visibility
spirit: an empty/wrong mount logs `discovered 0 tables ...` at startup, surfacing the failure mode without a
metrics stack.

## Alternatives considered
- **Per-keyspace gauge label** — rejected: keyspace cardinality is unbounded-ish and every existing #2419
  gauge is total-only; the startup log carries the breakdown instead.
- **Deriving `tables_discovered` from the warm registry** — rejected: the registry only knows *opened*
  tables, not what's *visible on disk*; the whole point is to catch an inert/empty mount before any query.
- **On-demand (per-request) discovery walk** — rejected: the 2s sampler already exists and amortizes the
  readdir cost; a per-request walk adds latency to the hot path.
- **Counting by statting generations (`enumerate_generations`)** — rejected: it stats/opens; violates the
  readdir-only + cold-start requirement. A `*-Data.db` name check is sufficient and I/O-free.

## Undersampling caveat (documented, #2661)
`tables_discovered` is sampled every 2s; a Prometheus scrape at a longer interval can miss short-lived
transitions. Documented in the metric description / ops notes, not fixed here (same caveat as the rest of
the #2419 family).
