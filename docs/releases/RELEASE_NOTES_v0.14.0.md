# CQLite v0.14.0 — Flight field-readiness

Released: 2026-07-13

v0.14.0 is the **Flight field-readiness** release: the Arrow Flight server and Trino
connector read path are now field-validated against a live, at-scale Cassandra deployment.
The build cut here is the **round-9 field-validated build** (validation tracker
[#2367](https://github.com/pmcfadin/cqlite/issues/2367)); the field anchors
[#2264](https://github.com/pmcfadin/cqlite/issues/2264) and
[#2157](https://github.com/pmcfadin/cqlite/issues/2157) were closed on field confirmation.

This release note is factual and cites the issues that shipped. Release binaries and the
auto-generated GitHub release body (commit/PR changelog + API-docs links) are produced by
`release.yml` on the `v0.14.0` tag.

## Headline — field-validated Flight read path

- **Streaming `do_get` scan rewrite** ([#2361](https://github.com/pmcfadin/cqlite/issues/2361)):
  the non-stitching scan path no longer materializes the whole SSTable before the first emit.
  It walks the full index lazily (fail-closed fallback gating), applies `LIMIT` effectively,
  tears producers down on cancel via a Drop-join, and tracks the in-flight scan phase with an
  up/down counter. Bounded memory, effective `LIMIT`/cancel.
- **Resolve-phase parse-once warm registry** ([#2383](https://github.com/pmcfadin/cqlite/issues/2383)):
  the resolve phase no longer CPU-spins re-parsing per request at multi-million-partition scale.
  Single-flight parse, rebind-by-inode, and cancel-aware parse remove the spin that hung
  `LIMIT`, `count(*)`, and point reads.
- **Warm-handles ENOENT fix** ([#2352](https://github.com/pmcfadin/cqlite/issues/2352)):
  streaming merge producers no longer fail with `ENOENT` when a snapshot path goes stale after
  `clearSnapshot` — a path-liveness gate re-opens by live path.
- **Broad-lane CI green** ([#2359](https://github.com/pmcfadin/cqlite/issues/2359)):
  the metadata-driven, feature-aware nextest archive replaces the hand-maintained hardcoded
  test lists that had diverged; first green `main` push since 2026-07-06.

## Flight / Trino read path

- do_get pushes PK-equality predicates toward partition point-read / prune instead of a full
  merge scan ([#2207](https://github.com/pmcfadin/cqlite/issues/2207)).
- do_get snapshot-index re-load/glob loop now honors cancellation — `LIMIT` queries no longer
  hang and the in-flight gauge no longer sticks ([#2264](https://github.com/pmcfadin/cqlite/issues/2264),
  [#2157](https://github.com/pmcfadin/cqlite/issues/2157)).
- Flight producer `entry_to_row` no longer collapses multi-cell / collection columns via
  HashMap overwrite ([#2324](https://github.com/pmcfadin/cqlite/issues/2324)).
- Flight merge no longer builds a full multi-core Tokio runtime per producer thread
  ([#2316](https://github.com/pmcfadin/cqlite/issues/2316)); graceful shutdown +
  cooperatively cancellable merges ([#1473](https://github.com/pmcfadin/cqlite/issues/1473)).
- `stitch_all_chunks` accepts Cassandra-compacted compressed SSTables with a degenerate empty
  trailing chunk ([#2225](https://github.com/pmcfadin/cqlite/issues/2225)).
- Connector deployment hardening: `--add-opens=java.base/java.nio` wired + fail-fast at plugin
  init ([#2290](https://github.com/pmcfadin/cqlite/issues/2290)); netty BOM pin propagation
  ([#2300](https://github.com/pmcfadin/cqlite/issues/2300)); Gradle 9.1 publication validation
  ([#2334](https://github.com/pmcfadin/cqlite/issues/2334)).
- Snapshot-completeness diagnosis (Sidecar snapshots arriving with only `Data.db`)
  ([#2295](https://github.com/pmcfadin/cqlite/issues/2295)); local field-repro harness + round-4
  coordination ([#2289](https://github.com/pmcfadin/cqlite/issues/2289),
  [#2286](https://github.com/pmcfadin/cqlite/issues/2286)).

## Correctness / parity

- Read-time reconciliation: multi-generation SELECT applies read-time TTL / partition /
  range-tombstone visibility ([#1849](https://github.com/pmcfadin/cqlite/issues/1849));
  `scan_stream` single-generation path returns rows on CQLite-written SSTables
  ([#1897](https://github.com/pmcfadin/cqlite/issues/1897)).
- Write path honors `USING TTL` and per-cell expiration; surviving live TTL cells stay
  byte-identical after compaction, including complex / collection / UDT elements
  ([#1743](https://github.com/pmcfadin/cqlite/issues/1743),
  [#2038](https://github.com/pmcfadin/cqlite/issues/2038),
  [#1538](https://github.com/pmcfadin/cqlite/issues/1538),
  [#1537](https://github.com/pmcfadin/cqlite/issues/1537)); non-frozen collection round-trip
  fixed ([#2035](https://github.com/pmcfadin/cqlite/issues/2035)).
- Float/double ordering matches Cassandra (NaN last, `-0.0 < +0.0`) across
  `Value` comparison, `ORDER BY`, `MIN`/`MAX`
  ([#2010](https://github.com/pmcfadin/cqlite/issues/2010),
  [#1870](https://github.com/pmcfadin/cqlite/issues/1870)).
- Aggregate metadata + types: empty-table global aggregate returns one row; result column
  labels / types derive from the expression, not synthetic `col_N`
  ([#2069](https://github.com/pmcfadin/cqlite/issues/2069),
  [#1872](https://github.com/pmcfadin/cqlite/issues/1872),
  [#1871](https://github.com/pmcfadin/cqlite/issues/1871),
  [#1763](https://github.com/pmcfadin/cqlite/issues/1763),
  [#1941](https://github.com/pmcfadin/cqlite/issues/1941)); SUM/AVG result-type divergence
  tracked ([#2202](https://github.com/pmcfadin/cqlite/issues/2202)).
- Point-lookup `WHERE pk = ?` returns typed columns and routes to the fast path instead of the
  legacy column-less heuristic fork ([#2066](https://github.com/pmcfadin/cqlite/issues/2066),
  [#1802](https://github.com/pmcfadin/cqlite/issues/1802),
  [#1750](https://github.com/pmcfadin/cqlite/issues/1750)).
- No-heuristics + parser hardening: Blob decode no longer guesses on a hardcoded byte pattern;
  recursion-depth guards, `duration` `try_from`, clamped capacities; typed Zstd-dictionary
  rejection; `set<int>` no longer perturbs regular-column decode; `inclusive` clustering-name
  collision rejected; checked Arrow collection offsets
  ([#1630](https://github.com/pmcfadin/cqlite/issues/1630),
  [#1632](https://github.com/pmcfadin/cqlite/issues/1632),
  [#1414](https://github.com/pmcfadin/cqlite/issues/1414),
  [#1723](https://github.com/pmcfadin/cqlite/issues/1723),
  [#1488](https://github.com/pmcfadin/cqlite/issues/1488),
  [#1486](https://github.com/pmcfadin/cqlite/issues/1486)).
- Query-semantics oracle added so read-reconciliation bugs no longer pass physical-dump parity
  green ([#1742](https://github.com/pmcfadin/cqlite/issues/1742)); fabricated statistics
  placeholders made honest/optional ([#1653](https://github.com/pmcfadin/cqlite/issues/1653));
  compaction finalize path fsyncs directories ([#1959](https://github.com/pmcfadin/cqlite/issues/1959));
  multi-candidate concat ordering + exhaustive-regen corpus tracked
  ([#1917](https://github.com/pmcfadin/cqlite/issues/1917),
  [#2009](https://github.com/pmcfadin/cqlite/issues/2009)).
- Bindings: integer-exact µs timestamps; typed malformed-inet errors
  ([#1463](https://github.com/pmcfadin/cqlite/issues/1463),
  [#1453](https://github.com/pmcfadin/cqlite/issues/1453)).

## Performance

The read/parse/export hot paths from the audit epics (B–G, J–M, AC–AE) landed: boxed `Value`
variants (88B → ≤40B), byte-bounded result budgets replacing the 1M row-count cliff, `LIMIT`/
`OFFSET` pushed into the scan, streaming multi-generation merge + streaming O(1) aggregates, a
key→partition-offset cache, one-walk zero-copy BTI trie descent, a single read-side VInt
decoder, per-column resolved dispatch, and capacity-hinted Arrow builders. Representative
issues: [#1583](https://github.com/pmcfadin/cqlite/issues/1583),
[#1582](https://github.com/pmcfadin/cqlite/issues/1582),
[#1577](https://github.com/pmcfadin/cqlite/issues/1577),
[#1579](https://github.com/pmcfadin/cqlite/issues/1579),
[#1578](https://github.com/pmcfadin/cqlite/issues/1578),
[#1570](https://github.com/pmcfadin/cqlite/issues/1570),
[#1574](https://github.com/pmcfadin/cqlite/issues/1574),
[#1650](https://github.com/pmcfadin/cqlite/issues/1650),
[#1638](https://github.com/pmcfadin/cqlite/issues/1638),
[#1635](https://github.com/pmcfadin/cqlite/issues/1635),
[#1585](https://github.com/pmcfadin/cqlite/issues/1585),
[#1495](https://github.com/pmcfadin/cqlite/issues/1495),
[#1496](https://github.com/pmcfadin/cqlite/issues/1496),
[#1817](https://github.com/pmcfadin/cqlite/issues/1817).

## CI / testing

- Metadata-driven, feature-aware CI lanes; cli-tests no longer use a hardcoded allowlist
  ([#2359](https://github.com/pmcfadin/cqlite/issues/2359),
  [#2039](https://github.com/pmcfadin/cqlite/issues/2039)).
- Dataset skip-guard checks `Data.db` (not just the table dir) so dataset-dependent tests skip
  rather than panic ([#2065](https://github.com/pmcfadin/cqlite/issues/2065),
  [#1860](https://github.com/pmcfadin/cqlite/issues/1860),
  [#1859](https://github.com/pmcfadin/cqlite/issues/1859),
  [#1843](https://github.com/pmcfadin/cqlite/issues/1843)).
- De-flaked wall-clock / global-counter / RSS-monotonic tests
  ([#1776](https://github.com/pmcfadin/cqlite/issues/1776),
  [#1774](https://github.com/pmcfadin/cqlite/issues/1774),
  [#1539](https://github.com/pmcfadin/cqlite/issues/1539),
  [#1946](https://github.com/pmcfadin/cqlite/issues/1946),
  [#2006](https://github.com/pmcfadin/cqlite/issues/2006)); single-compression-feature and
  all-features build lanes green ([#1873](https://github.com/pmcfadin/cqlite/issues/1873),
  [#1880](https://github.com/pmcfadin/cqlite/issues/1880)); Python/Node binding CI stabilized
  ([#1803](https://github.com/pmcfadin/cqlite/issues/1803),
  [#1928](https://github.com/pmcfadin/cqlite/issues/1928),
  [#1784](https://github.com/pmcfadin/cqlite/issues/1784)).
- Dead-code purges (TrieNavigator, ANTLR stub, dead parser generations) and struct-size /
  work-counter pins ([#1652](https://github.com/pmcfadin/cqlite/issues/1652),
  [#1639](https://github.com/pmcfadin/cqlite/issues/1639),
  [#1637](https://github.com/pmcfadin/cqlite/issues/1637),
  [#1616](https://github.com/pmcfadin/cqlite/issues/1616)).

## Docs

- Published Arrow Flight server + Trino connector user-docs page
  ([#2115](https://github.com/pmcfadin/cqlite/issues/2115)).

## Write-surface claim boundary

CQLite's production write surface (flush + STCS compaction) emits **uncompressed** SSTables
only and never a `CompressionInfo.db`. CQLite does **not** emit compressed SSTables; the
compressed-write building blocks remain unwired (fixture-synthesis only, no Cassandra-side
parity coverage) — tracked as [#1406](https://github.com/pmcfadin/cqlite/issues/1406).

## Known limitations

- **Cold first-query-per-table parse cost**: `SSTableReader::open` parses `Index.db` super-
  linearly and currently parses it twice per open, so the first query against a large table
  pays a cold parse cost at field scale. Fix queued for **0.14.1**
  ([#2385](https://github.com/pmcfadin/cqlite/issues/2385),
  [#2395](https://github.com/pmcfadin/cqlite/issues/2395)).
- **Reads are single-node-bound under RF=N**: under concurrency one pod does the work;
  throughput caps at one node. Fix groomed ([#2397](https://github.com/pmcfadin/cqlite/issues/2397)).
- **Warm scan setup overhead**: warm scans carry a fixed ~4–5s per-query resolve/merge-setup
  cost, so a `LIMIT 5` scan runs comparably to `LIMIT 100`
  ([#2398](https://github.com/pmcfadin/cqlite/issues/2398)).

## Upgrading

Rust crates, the Python (`cqlite-py`) and Node (`@cqlite/node`) bindings all move `0.13.0 →
0.14.0` in lockstep. The Trino connector (`in.mcfad:cqlite-trino`) is versioned separately and
is not part of this crate bump.
