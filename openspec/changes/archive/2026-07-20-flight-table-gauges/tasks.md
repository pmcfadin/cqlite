# Tasks — Flight table-visibility gauges

## Registration (3-layer, both gauges)
- [ ] `catalog.rs`: add `FLIGHT_TABLES_DISCOVERED` + `FLIGHT_WARM_TABLES` constants (unit `ENTRIES`), add
  to `ALL_METRICS` + `SATURATION_GAUGES`.
- [ ] `otel.rs`: add a `Gauge<i64>` `Instruments` field + builder + dedicated `record_gauge` match arm for
  each (no ad-hoc `_ =>` fallback — the source-scan test enforces this).

## tables_discovered (sampler-driven)
- [ ] Thread `data_dir: PathBuf` into `run_sampler`; pass it at the `main.rs` spawn site.
- [ ] Add the readdir walk: keyspace dirs → table dirs (excl. `snapshots/`/`backups/`, incl. UUID-suffixed)
  → count those directly containing a `*-Data.db` name. NO stat/open/parse (surface:
  `saturation::sample_once` / the new walk fn).
- [ ] Emit `record_gauge(FLIGHT_TABLES_DISCOVERED, count, &[])` each tick.

## warm_tables (atomic-backed at the registry)
- [ ] Emit `record_gauge(FLIGHT_WARM_TABLES, inner.tables.len() as i64, &[])` post-mutation under the lock
  at `rebuild()` insert and `evict_to_budget()` removal (`registry.rs`).
- [ ] Add feature-independent `warm_table_count() -> i64` level reader (mirror
  `blocking_tasks_in_use_level`) for up/down tests.

## Startup log
- [ ] Emit one `info` line `discovered N tables across M keyspaces under <data-dir>` after the first sample
  (Once-guarded, co-located with the sampler).

## Wiring evidence + tests
- [ ] Fixture-dir test: pins `tables_discovered` count (UUID dir counted; snapshots/backups/non-table
  excluded); removes a table dir → next sample falls; adds one → rises; empty dir → 0.
- [ ] Cold-start test: N sampler ticks over a populated dir → zero `INDEX_PARSES_TOTAL` delta.
- [ ] Public-surface `warm_tables` test: real `do_get` on a previously-unseen table → gauge increments;
  eviction/retirement → decrements (drive up AND down).
- [ ] Startup-log-line test (or assertion) that the info line is present with N/M/data-dir.

## Docs
- [ ] Note the 2s-vs-scrape-interval undersampling caveat (#2661) in the metric description / ops notes;
  add both gauges to any Flight metrics reference doc.

## Quality gates
- [ ] `scripts/agent-gate.sh --lite` green each round; rust-reviewer + roborev on the lite-green diff.
- [ ] Full `scripts/agent-gate.sh` PASS (gate of record, in flow-closer).
- [ ] C intent audit (spec-auditor) PASS. Final roborev clean. `openspec archive` on merge.
