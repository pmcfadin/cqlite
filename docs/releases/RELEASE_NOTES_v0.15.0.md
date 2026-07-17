# CQLite v0.15.0 — Trino latency, throughput & operations

Released: 2026-07-17

v0.15.0 is the **Trino latency / throughput / operations** release (epic
[#2403](https://github.com/pmcfadin/cqlite/issues/2403)). It builds on the v0.14.0 Flight
field-readiness base and turns the field-validated read path into a fast, observable, and
overload-resilient one. Warm throughput through Trino is up roughly **15×** versus the v0.14
field baseline, the read hot paths were re-cut end to end, saturation is now legible in-process,
and a P0 silent-row-loss class was removed per the no-heuristics mandate.

This release note is factual and cites the PRs/issues that shipped. Release binaries and the
auto-generated GitHub release body (commit/PR changelog + API-docs links) are produced by
`release.yml` on the `v0.15.0` tag.

## Headline — warm Trino throughput up ~15×

Round-11b field validation (tracker [#2367](https://github.com/pmcfadin/cqlite/issues/2367))
measured **~34 qps warm, p50 227ms / p99 366ms through Trino**, with server-side work at
**~2ms** and **zero cold parses on the warm path** — roughly **15×** the v0.14 warm-throughput
field baseline, at 80 concurrent threads with no OOMKills. The levers:

- **Connector-side snapshot lifecycle closure** — per-`(keyspace, table)` reader reuse plus
  warm rebind, so repeat queries no longer re-open from cold
  ([#2356](https://github.com/pmcfadin/cqlite/issues/2356),
  [#2306](https://github.com/pmcfadin/cqlite/issues/2306) via
  [PR #2425](https://github.com/pmcfadin/cqlite/pull/2425)).
- **Lazy Summary-guided BIG index** — `O(summary)` open, bounded point-lookup intervals, and
  streaming scans, with token pushdown so a scan no longer decodes every partition body
  ([#2412](https://github.com/pmcfadin/cqlite/issues/2412),
  [#2413](https://github.com/pmcfadin/cqlite/issues/2413) via
  [PR #2440](https://github.com/pmcfadin/cqlite/pull/2440); warm-scan dominant cost first
  identified in [#2398](https://github.com/pmcfadin/cqlite/issues/2398)).
- **Row-granular point-read streaming** — point reads and cache-warm merges drive the merge
  row-by-row instead of materializing
  ([#2423](https://github.com/pmcfadin/cqlite/issues/2423) via
  [PR #2434](https://github.com/pmcfadin/cqlite/pull/2434)).
- **Multi-node read fan-out unblocked** — split primaries now rotate across replica owners, so
  reads under RF=N are no longer pinned to a single pod (the standing v0.14 limitation)
  ([#2397](https://github.com/pmcfadin/cqlite/issues/2397) via
  [PR #2409](https://github.com/pmcfadin/cqlite/pull/2409)).

## Operations — admission control & saturation observability

- **Flight `do_get` admission control**: bounded scan concurrency (`--max-concurrent-scans`),
  `UNAVAILABLE` load-shedding, and phase-visible queueing keep an overloaded server responsive
  instead of thrashing ([#2420](https://github.com/pmcfadin/cqlite/issues/2420) via
  [PR #2431](https://github.com/pmcfadin/cqlite/pull/2431)); the eager multi-generation merge
  path is admitted through the same scan-admission semaphore
  ([#2063](https://github.com/pmcfadin/cqlite/issues/2063) via
  [PR #2568](https://github.com/pmcfadin/cqlite/pull/2568)).
- **Saturation gauges**: five in-process gauges (blocking-task guard, merge egress depth, and an
  fd / thread / RSS sampler on a 2s tick) make server saturation legible during overload
  ([#2419](https://github.com/pmcfadin/cqlite/issues/2419) via
  [PR #2547](https://github.com/pmcfadin/cqlite/pull/2547)).
- **Metrics surface**: `cqlite.errors.total` is eagerly registered at 0 on startup
  ([#2288](https://github.com/pmcfadin/cqlite/issues/2288)); an operator-facing flight-metrics
  reference is generated from the observability catalog
  ([#2426](https://github.com/pmcfadin/cqlite/issues/2426)); the round-N standard metrics
  template ([#2399](https://github.com/pmcfadin/cqlite/issues/2399)) and a refreshed
  cqlite-flight Grafana dashboard with a catalog-drift guard
  ([#2427](https://github.com/pmcfadin/cqlite/issues/2427)) landed; a static xtask audit enforces
  the no-unbounded-materialization invariant
  ([#2012](https://github.com/pmcfadin/cqlite/issues/2012)) and a nightly FD/RSS resource-leak
  soak guards against handle leaks ([#2013](https://github.com/pmcfadin/cqlite/issues/2013)).

## Correctness / parity

- **P0 — silent row loss on large single-cell values**: rows with a single cell of ≥~1MB were
  silently dropped because a **1MB `row_size` heuristic** rejected legitimate rows as corrupt and
  the driver folded the error into a clean-empty partition. The heuristic is replaced with an
  authoritative remaining-bytes bound, per the no-heuristics mandate
  ([#2436](https://github.com/pmcfadin/cqlite/issues/2436) via
  [PR #2482](https://github.com/pmcfadin/cqlite/pull/2482)).
- **v5 cell parsing hardening**: overflow-safe v5 cell bounds guards, `Float32` / varint parity,
  and a `cell_value` split ([#1795](https://github.com/pmcfadin/cqlite/issues/1795),
  [#1884](https://github.com/pmcfadin/cqlite/issues/1884),
  [#1885](https://github.com/pmcfadin/cqlite/issues/1885) via
  [PR #2467](https://github.com/pmcfadin/cqlite/pull/2467)); the v5 ladder-decoder varint arm now
  decodes a CQL varint as `Value::Varint`, not `Blob`
  ([#1885](https://github.com/pmcfadin/cqlite/issues/1885) via
  [PR #2466](https://github.com/pmcfadin/cqlite/pull/2466)).
- **`GROUP BY` float/double** groups by the Cassandra comparator (NaN → one group, `±0.0`
  distinct) ([#2074](https://github.com/pmcfadin/cqlite/issues/2074) via
  [PR #2488](https://github.com/pmcfadin/cqlite/pull/2488)).
- Complex-cell element TTL is clamped to `i32::MAX`, matching the scalar reader
  ([#2173](https://github.com/pmcfadin/cqlite/issues/2173)); BIG clustering-slice / reverse seek
  is migrated to the `ReadAt` point source
  ([#1869](https://github.com/pmcfadin/cqlite/issues/1869)); snapshot-aware SSTable identity path
  parsing handles ID-ful snapshots and guards the snapshots keyspace
  ([#2384](https://github.com/pmcfadin/cqlite/issues/2384)).
- **`CompressionInfo` fail-closed**: `max_compressed_length == 0` is rejected at parse and in the
  compressed-offset-window read instead of producing garbage
  ([#2529](https://github.com/pmcfadin/cqlite/issues/2529),
  [#2524](https://github.com/pmcfadin/cqlite/issues/2524)); compressed `CHUNK_READ_CALLS`
  accounting restored ([#2167](https://github.com/pmcfadin/cqlite/issues/2167)); decimal
  `max_abs` hoisted to a `LazyLock` with watchdog panic-vs-hang disambiguation
  ([#2145](https://github.com/pmcfadin/cqlite/issues/2145)).

## Performance — read/write hot-path train

- **Zero-copy `Bytes`-backed `Value`** — the final stage of the Value-v2 train removes per-cell
  copies on the read path ([#1644](https://github.com/pmcfadin/cqlite/issues/1644) via
  [PR #2598](https://github.com/pmcfadin/cqlite/pull/2598)).
- **Merge core**: binary-search range shadowing replaces the linear scan
  ([#1669](https://github.com/pmcfadin/cqlite/issues/1669)); the `MergeEntry` double-clone in the
  k-way merge is eliminated ([#1664](https://github.com/pmcfadin/cqlite/issues/1664)); reconcile
  uses the `entry()` API + `mem::take` survivors
  ([#1665](https://github.com/pmcfadin/cqlite/issues/1665)).
- **Point-read I/O**: a dedicated `MADV_RANDOM` point-read mmap for large SSTables
  ([#2210](https://github.com/pmcfadin/cqlite/issues/2210)); a partition-seeking merge-run reader
  for multi-candidate point reads ([#2096](https://github.com/pmcfadin/cqlite/issues/2096)); an
  aligned bounce buffer reused across Direct-I/O windowed-scan chunks
  ([#2319](https://github.com/pmcfadin/cqlite/issues/2319)); BIG point-read repeats served from
  the decompressed-chunk cache ([#1818](https://github.com/pmcfadin/cqlite/issues/1818)); a global
  bounded key→partition-offset cache ([#2059](https://github.com/pmcfadin/cqlite/issues/2059)); an
  `O(depth)` local BTI successor walk replacing whole-trie DFS enumeration
  ([#2058](https://github.com/pmcfadin/cqlite/issues/2058)); the materializing full-index scan
  drops its `O(N)` per-partition re-probe
  ([#2430](https://github.com/pmcfadin/cqlite/issues/2430)); `Statistics.db` TOC parsed once
  instead of three walks ([#2148](https://github.com/pmcfadin/cqlite/issues/2148)).
- **Query metadata**: `WRITETIME` / `TTL` `IN`-list reads fan out to targeted lookups
  ([#1916](https://github.com/pmcfadin/cqlite/issues/1916)).
- **Write path**: cached ordered column lists + per-column `is_complex`
  ([#1674](https://github.com/pmcfadin/cqlite/issues/1674)); cached parsed `CqlType` + skipped
  ordered-key re-sort ([#1677](https://github.com/pmcfadin/cqlite/issues/1677)); one murmur3 per
  partition via an `h1+h2` fold ([#1681](https://github.com/pmcfadin/cqlite/issues/1681)); a
  reusable `row_scratch` with in-place row-size VInt
  ([#1673](https://github.com/pmcfadin/cqlite/issues/1673)); `serialize_value_into(&mut Vec<u8>)`
  kills the fresh-Vec-per-cell double copy
  ([#1672](https://github.com/pmcfadin/cqlite/issues/1672)); a regression lock pins zero
  `StatisticsMetadata` allocations per partition
  ([#1676](https://github.com/pmcfadin/cqlite/issues/1676)); incremental BTI partition-trie
  emission in a single sweep with a depth-≤9 stack
  ([#1679](https://github.com/pmcfadin/cqlite/issues/1679)).
- **Compaction memory**: uncompressed-SSTable compaction peak heap bounded from **410 → 54 MiB**
  via row-granular streaming read + writer/merge direct-stream
  ([#2299](https://github.com/pmcfadin/cqlite/issues/2299) via
  [PR #2421](https://github.com/pmcfadin/cqlite/pull/2421)); non-stitching full scans use a
  sequential windowed `Data.db` pass ([#2366](https://github.com/pmcfadin/cqlite/issues/2366));
  the read-path merge streams per-row via `StreamingMerger`
  ([#2230](https://github.com/pmcfadin/cqlite/issues/2230)).

## Trino connector

The Trino connector (`in.mcfad:cqlite-trino`) is versioned separately from the crate bump. This
cycle it advanced to **0.14.3 / 0.14.4**:

- Snapshot lifecycle closure — per-`(keyspace, table)` reuse + warm rebind
  ([#2356](https://github.com/pmcfadin/cqlite/issues/2356),
  [#2306](https://github.com/pmcfadin/cqlite/issues/2306) via
  [PR #2425](https://github.com/pmcfadin/cqlite/pull/2425)).
- Snapshot retirement hardening — ref-counted rollback retire + a background grace-sweep with a
  quiet-table tick ([#2452](https://github.com/pmcfadin/cqlite/issues/2452) via
  [PR #2579](https://github.com/pmcfadin/cqlite/pull/2579)).

## CI / testing / tools

- **Flight↔Trino docker E2E** auto-runs on integration-surface PRs
  ([#2358](https://github.com/pmcfadin/cqlite/issues/2358)).
- **`tools/flight-loadgen`**: a raw `FlightServiceClient` ramp harness for throughput validation
  (WS1 of the throughput program [#2313](https://github.com/pmcfadin/cqlite/issues/2313) via
  [PR #2575](https://github.com/pmcfadin/cqlite/pull/2575)); a `CQLITE_READ_PATH` forcing knob +
  point-vs-full differential lane ([#1918](https://github.com/pmcfadin/cqlite/issues/1918)); a
  measure-first Stage-0 `DecodePolicy` bench + A/B report
  ([#2211](https://github.com/pmcfadin/cqlite/issues/2211)).
- De-flaked wall-clock / global-counter tests via thread-local scopes and bounded timeouts
  ([#2470](https://github.com/pmcfadin/cqlite/issues/2470),
  [#2369](https://github.com/pmcfadin/cqlite/issues/2369),
  [#1819](https://github.com/pmcfadin/cqlite/issues/1819),
  [#2428](https://github.com/pmcfadin/cqlite/issues/2428)); `dhat` allocs/row + allocs/cell
  budgets for RowCells assembly ([#2075](https://github.com/pmcfadin/cqlite/issues/2075)); span
  demotion pinned across write/select ([#2172](https://github.com/pmcfadin/cqlite/issues/2172));
  DuckDB parquet-validation test misuse fixed
  ([#2491](https://github.com/pmcfadin/cqlite/issues/2491)); `env_logger` moved to dev-dependencies
  ([#2519](https://github.com/pmcfadin/cqlite/issues/2519)); `table_snapshot` goldens re-blessed
  after the v3.5 corpus regen ([#1896](https://github.com/pmcfadin/cqlite/issues/1896)).

## Docs

- Public **Performance** page with the measured round-11b results + the goals ladder headline
  ([#2473](https://github.com/pmcfadin/cqlite/issues/2473) via
  [PR #2475](https://github.com/pmcfadin/cqlite/pull/2475)); a round-12 field-validation report
  with embedded Grafana panels; a website + `CLAUDE.md` sweep for the mid-July ships
  ([#2460](https://github.com/pmcfadin/cqlite/issues/2460)); a DataFusion promotion decision brief
  (Option C — spike first, promote on data)
  ([#941](https://github.com/pmcfadin/cqlite/issues/941)).

## Write-surface claim boundary

Unchanged: CQLite's production write surface (flush + STCS compaction) emits **uncompressed**
SSTables only and never a `CompressionInfo.db`. CQLite does **not** emit compressed SSTables; the
compressed-write building blocks remain unwired (fixture-synthesis only, no Cassandra-side parity
coverage) — tracked as [#1406](https://github.com/pmcfadin/cqlite/issues/1406).

## Known limitations

- **Windows Node CI**: the Windows Node bindings CI lane remains deferred debt
  ([#1979](https://github.com/pmcfadin/cqlite/issues/1979)); Linux and macOS Node bindings are
  unaffected.
- **Quiet-table snapshot grace-sweep**: background retirement of snapshots for tables that have
  gone quiet is tracked as a follow-up ([#2452](https://github.com/pmcfadin/cqlite/issues/2452) —
  connector-side hardening shipped this cycle; further tuning continues).

## Upgrading

Rust crates, the Python (`cqlite-py`) and Node (`@cqlite/node`) bindings all move `0.14.1 →
0.15.0` in lockstep. The Trino connector (`in.mcfad:cqlite-trino`, currently `0.14.4`) is
versioned separately and is not part of this crate bump.
