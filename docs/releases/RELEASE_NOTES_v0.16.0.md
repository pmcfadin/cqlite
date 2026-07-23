# CQLite v0.16.0 — Trino connector completeness & cancellation

Released: 2026-07-22

v0.16.0 builds on the v0.15.0 Trino latency/throughput base and closes two field-surfaced
connector gaps: Cassandra **collection columns are now projected through Trino** (they were
silently dropped), and the **weight-balanced split→pod fan-out** ships with a root-fix for a
`LIMIT`-cancellation hang. It also lands the UDT registry across both Flight read paths,
plan-time split pruning, and read-path parity fixes.

This release note is factual and cites the PRs/issues that shipped. Release binaries and the
auto-generated GitHub release body (commit/PR changelog + API-docs links) are produced by
`release.yml` on the `v0.16.0` tag.

## Headline — Trino collection columns + weight-balanced, hang-free sub-splitting

- **Typed collection columns through Trino** — `list`/`set`/`map` columns (including
  `list<frozen<udt>>`) now project as Trino `array`/`row`/`map` instead of being silently
  dropped from the schema; unmappable columns are surfaced loudly, never hidden without a trace.
  UDT *element-value* decode inside a collection remains tracked to
  [#2349](https://github.com/pmcfadin/cqlite/issues/2349); primitive element types decode fully.
  ([#2815](https://github.com/pmcfadin/cqlite/issues/2815) via
  [PR #2816](https://github.com/pmcfadin/cqlite/pull/2816)).
- **Weight-balanced split→pod assignment** — K-way token-range sub-splitting
  (`cqlite.sub-splits-per-range`, default 4) with span-proportional `SplitWeight`, evening out
  the 2–4× per-pod CPU skew that capped aggregate throughput. Aggregate, pushed-`LIMIT`, and
  fully-bound point reads are exempted to K=1.
  ([#2680](https://github.com/pmcfadin/cqlite/issues/2680) via
  [PR #2833](https://github.com/pmcfadin/cqlite/pull/2833)).
- **P0 `LIMIT`-cancellation hang fixed at root** — a partial-predicate `LIMIT` under sub-splitting
  could hang because the blocking Flight `DoGet` read ran on the Trino driver thread, so early
  operator close could never cancel it. The read now runs off the driver thread
  (`isBlocked()`), letting close cross-cancel the stream; the server egress path also races a
  cancel flag. Guarded by a docker-compose E2E `LIMIT` regression
  ([#2782](https://github.com/pmcfadin/cqlite/issues/2782), fixed in
  [PR #2833](https://github.com/pmcfadin/cqlite/pull/2833)).

## Also in this release

- **UDT registry wired into both Flight read paths** (cold + warm)
  ([#2349](https://github.com/pmcfadin/cqlite/issues/2349) via
  [PR #2761](https://github.com/pmcfadin/cqlite/pull/2761)).
- **Plan-time split pruning for fully-bound partition keys** — a point read prunes to the
  covering split instead of fanning out
  ([#2679](https://github.com/pmcfadin/cqlite/issues/2679) via
  [PR #2774](https://github.com/pmcfadin/cqlite/pull/2774);
  [#2806](https://github.com/pmcfadin/cqlite/issues/2806) via
  [PR #2810](https://github.com/pmcfadin/cqlite/pull/2810)).
- **Keyspace-qualified UDT type names** accepted on both read paths
  ([#2807](https://github.com/pmcfadin/cqlite/issues/2807) via
  [PR #2808](https://github.com/pmcfadin/cqlite/pull/2808)).
- **Read-time TTL/liveness reconciliation** routed through `do_get` + query-semantics oracle
  ([#2374](https://github.com/pmcfadin/cqlite/issues/2374),
  [#2789](https://github.com/pmcfadin/cqlite/issues/2789) via
  [PR #2800](https://github.com/pmcfadin/cqlite/pull/2800)).
- **Recurring roborev blocker classes mechanized as `--lite` lints**
  ([#2656](https://github.com/pmcfadin/cqlite/issues/2656) via
  [PR #2741](https://github.com/pmcfadin/cqlite/pull/2741)).

## Known follow-ups (not blocking)

- UDT element-value string decode inside collections
  ([#2349](https://github.com/pmcfadin/cqlite/issues/2349) — the collection column now reaches
  the read path, unblocking field verification).
- Two non-blocking cancel-path polish nits from the #2680 review
  ([#2834](https://github.com/pmcfadin/cqlite/issues/2834) — observability metric skew + a benign
  pre-existing `toPage`/close window + comment/Javadoc accuracy).

The broader 0.16 milestone (Flight hardening epic [#1466](https://github.com/pmcfadin/cqlite/issues/1466),
streaming-egress epic [#1467](https://github.com/pmcfadin/cqlite/issues/1467), round-trip parity
epic [#1469](https://github.com/pmcfadin/cqlite/issues/1469), and related items) remains open as
backlog — the release version and the milestone are tracked independently.
