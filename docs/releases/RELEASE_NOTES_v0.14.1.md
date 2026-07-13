# CQLite v0.14.1 — cold-start parse fix

Released: 2026-07-13

v0.14.1 is a patch release on top of the [v0.14.0](RELEASE_NOTES_v0.14.0.md) Flight
field-readiness build. It fixes the cold first-query-per-table parse cost that v0.14.0 called out
as a known limitation. No API or behavior changes — this is a pure performance fix.

Release binaries and the auto-generated GitHub release body (commit/PR changelog + API-docs links)
are produced by `release.yml` on the `v0.14.1` tag.

## Headline — cold-start parse fix

- **Retired the redundant `SSTableIndex` from BIG reader open**
  ([#2385](https://github.com/pmcfadin/cqlite/issues/2385),
  [#2395](https://github.com/pmcfadin/cqlite/issues/2395) via
  [PR #2402](https://github.com/pmcfadin/cqlite/pull/2402)): `SSTableReader::open` no longer builds
  a second, redundant in-memory index. The removal is behavior-identical and consumer-verified —
  `Index.db` is now parsed exactly once per open (parses-per-open **2 → 1**), and the surviving
  index build uses capacity hints so it grows linearithmically instead of quadratically.

### Numbers

- 200k-entry index build: **6.17s → 0.061s** (~**100×**).
- Growth ratio (build time vs entry count): **15.5 → 4.96** — quadratic collapses to linearithmic.
- Parses of `Index.db` per open: **2 → 1**.
- Resident index memory: roughly **halved per generation**.

### Field context

At round-9 field validation, a cold `LIMIT` query took **4m17s at 1.42M partitions**. With this
fix the cold cost is expected to drop to seconds-per-generation; the round-10 field pass will
confirm.

## Known limitations

Unchanged from v0.14.0, minus the cold-start item fixed above:

- **Reads are single-node-bound under RF=N**: under concurrency one pod does the work; throughput
  caps at one node. Connector fix coming
  ([#2397](https://github.com/pmcfadin/cqlite/issues/2397)).
- **Warm scan setup overhead**: warm scans carry a fixed ~4–5s per-query resolve/merge-setup cost,
  so a `LIMIT 5` scan runs comparably to `LIMIT 100`
  ([#2398](https://github.com/pmcfadin/cqlite/issues/2398)).

## Write-surface claim boundary

Unchanged: CQLite's production write surface (flush + STCS compaction) emits **uncompressed**
SSTables only and never a `CompressionInfo.db`. CQLite does **not** emit compressed SSTables; the
compressed-write building blocks remain unwired
([#1406](https://github.com/pmcfadin/cqlite/issues/1406)).

## Upgrading

Rust crates, the Python (`cqlite-py`) and Node (`@cqlite/node`) bindings all move `0.14.0 →
0.14.1` in lockstep. The Trino connector (`in.mcfad:cqlite-trino`) is versioned separately and is
not part of this crate bump.
